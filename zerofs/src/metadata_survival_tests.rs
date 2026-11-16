use crate::block_transformer::ZeroFsBlockTransformer;
use crate::config::Settings;
use crate::db::SlateDbHandle;
use crate::frame_codec::FrameCodec;
use crate::fs::ZeroFS;
use crate::fs::errors::FsError;
use crate::fs::inode::{Inode, InodeAttrs};
use crate::fs::mutation::durability::{DurabilityTarget, ObjectCoverage};
use crate::fs::permissions::Credentials;
use crate::fs::types::{AuthContext, SetAttributes, SetGid, SetMode, SetSize, SetUid};
use crate::key_management::{load_existing_encryption_key, load_or_init_encryption_key};
use crate::pool_object_store::PoolObjectStore;
use crate::segment::{FOOTER_LEN, parse_footer};
use crate::storage_backend::open_pool_storage;
use crate::writeback::config::{AckMode, ShutdownFlush, WritebackSettings};
use crate::writeback::model::JournalIdentity;
use crate::writeback::store::WritebackObjectStore;
use anyhow::{Context, Result, ensure};
use bytes::Bytes;
use object_store::{ObjectStore, prefix::PrefixStore};
use sha2::{Digest, Sha256};
use slatedb::{BlockTransformer, DbBuilder};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

const PASSWORD: &str = "m0-disposable-fixture-password";
const FINAL_SIZE: usize = 64 * 1024 + 17;

struct Fixture {
    root: tempfile::TempDir,
    settings: Settings,
}

impl Fixture {
    fn new() -> Self {
        let root = tempfile::tempdir().unwrap();
        let symlink_free_root = root.path().canonicalize().unwrap();
        for directory in [
            "authority-device/authority",
            "disk-a-device/data",
            "disk-a-device/metadata-replica",
            "disk-b-device/data",
        ] {
            std::fs::create_dir_all(symlink_free_root.join(directory)).unwrap();
        }
        let uri = |name| {
            url::Url::from_directory_path(symlink_free_root.join(name))
                .unwrap()
                .to_string()
        };
        let document = serde_json::json!({
            "cache": {"dir": symlink_free_root.join("cache"), "disk_size_gb": 0.001},
            "storage": {
                "url": uri("authority-device/authority"),
                "encryption_password": PASSWORD,
                "pool": {
                    "catalog_dir": symlink_free_root.join("authority-device/catalog"),
                    "members": [
                        {"id": "disk-a", "url": uri("disk-a-device/data"), "capacity_gb": 0.0002, "reserve_free_gb": 0.0},
                        {"id": "disk-b", "url": uri("disk-b-device/data"), "capacity_gb": 0.008, "reserve_free_gb": 0.0}
                    ]
                }
            },
            "servers": {},
            "telemetry": {"enabled": false}
        });
        let config = root.path().join("fixture.toml");
        std::fs::write(&config, toml::to_string(&document).unwrap()).unwrap();
        Self {
            settings: Settings::from_file(config).unwrap(),
            root,
        }
    }

    fn lose(&self, loss: Loss) {
        let device = self.root.path().join("authority-device");
        let authority = device.join("authority");
        match loss {
            Loss::Authority => {
                std::fs::remove_dir_all(&authority).unwrap();
            }
            Loss::Catalog => std::fs::remove_dir_all(device.join("catalog")).unwrap(),
            Loss::AuthorityDevice => {
                std::fs::remove_dir_all(&device).unwrap();
            }
            Loss::WrappedKey => std::fs::remove_file(authority.join("zerofs.key")).unwrap(),
        }
        std::fs::create_dir_all(&authority).unwrap();
    }
}

#[derive(Clone, Copy, Debug)]
enum Loss {
    Authority,
    Catalog,
    AuthorityDevice,
    WrappedKey,
}

async fn open(settings: &Settings, initialize: bool) -> Result<(Arc<ZeroFS>, PoolObjectStore)> {
    let (fs, pool, _) = open_with_writeback(settings, initialize, None).await?;
    Ok((fs, pool))
}

async fn open_with_writeback(
    settings: &Settings,
    initialize: bool,
    journal_dir: Option<PathBuf>,
) -> Result<(Arc<ZeroFS>, PoolObjectStore, Option<WritebackObjectStore>)> {
    let (opened, pool) = open_pool_storage(settings, initialize)
        .await
        .context("open original pool")?;
    let store: Arc<dyn ObjectStore> = Arc::from(opened.store);
    let path = opened.path;
    let key = if initialize {
        load_or_init_encryption_key(&store, &path, PASSWORD, false).await?
    } else {
        load_existing_encryption_key(&store, &path, PASSWORD)
            .await
            .context("recover original encryption key")?
    };
    let (store, writeback) = if let Some(dir) = journal_dir {
        let identity = JournalIdentity {
            format_version: 1,
            bucket_id: pool.status().await?.pool_id,
            backend_endpoint: settings.storage.url.clone(),
            database_prefix: path.to_string(),
            backend_kind: "file".to_owned(),
            encryption_key_identity_sha256: Sha256::digest(key).into(),
        };
        let attached = crate::writeback::bootstrap::attach(
            store,
            WritebackSettings {
                dir,
                ack_mode: AckMode::Ssd,
                memory_bytes: 16 * 1024 * 1024,
                disk_bytes: 64 * 1024 * 1024,
                min_free_bytes: 1,
                high_watermark_percent: 95,
                resume_percent: 85,
                upload_concurrency: 2,
                local_concurrency: 2,
                shutdown_flush: ShutdownFlush::Local,
            },
            identity,
            "metadata-survival",
        )
        .await?;
        (attached.store, Some(attached.lifecycle))
    } else {
        (store, None)
    };
    let transformer: Arc<dyn BlockTransformer> =
        ZeroFsBlockTransformer::new_arc(&key, settings.compression());
    let db = Arc::new(
        DbBuilder::new(path.clone(), Arc::clone(&store))
            .with_settings(slatedb::config::Settings {
                wal_enabled: false,
                l0_sst_size_bytes: usize::MAX - 1,
                max_unflushed_bytes: usize::MAX,
                flush_interval: None,
                compactor_options: None,
                garbage_collector_options: None,
                ..Default::default()
            })
            .with_block_transformer(transformer)
            .with_filter_policies(crate::fs::filter_policy::filter_policies())
            .with_segment_extractor(Arc::new(crate::segment_extractor::ZeroFsSegmentExtractor))
            .build()
            .await
            .context("open original namespace database")?,
    );
    let codec = FrameCodec::new(&key, crate::segment::SEGMENT_INFO, settings.compression());
    let segment_store: Arc<dyn ObjectStore> = Arc::new(PrefixStore::new(store, path));
    let mut fs = ZeroFS::new_with_slatedb(
        SlateDbHandle::ReadWrite(db),
        u64::MAX,
        None,
        false,
        segment_store,
        codec,
    )
    .await?;
    fs.write_ack = settings
        .filesystem_write_ack_settings(crate::writeback::config::WritebackAccessMode::ReadWrite)?;
    if let Some(writeback) = &writeback {
        let local = writeback.clone();
        fs.flush_coordinator
            .set_local_durability_barrier(Arc::new(move || {
                let local = local.clone();
                Box::pin(async move {
                    local
                        .wait_local_through_accepted()
                        .await
                        .map_err(|_| FsError::IoError)
                })
            }));
        let captured = writeback.clone();
        fs.flush_coordinator
            .set_object_capture(Arc::new(move || captured.object_coverage()));
        let waited = writeback.clone();
        fs.flush_coordinator
            .set_object_wait(Arc::new(move |coverage, target| {
                let waited = waited.clone();
                Box::pin(async move {
                    match coverage {
                        ObjectCoverage::DirectRemote => Ok(()),
                        ObjectCoverage::Writeback {
                            journal_incarnation,
                            sequence,
                        } => waited
                            .wait_coverage(
                                journal_incarnation.as_uuid(),
                                sequence,
                                target == DurabilityTarget::RemoteBackend,
                            )
                            .await
                            .map_err(crate::fs::mutation::durability::DurabilityError::Object),
                    }
                })
            }));
    }
    let fs = Arc::new(fs);
    fs.install_volatile_overlay();
    fs.start_materializer();
    Ok((fs, pool, writeback))
}

async fn close(fs: Arc<ZeroFS>, pool: PoolObjectStore) {
    fs.stop_new_mutation_admission();
    fs.stop_mutation_workers().await.unwrap();
    fs.flush_coordinator.close().await.unwrap();
    let worker = fs.write_coordinator.take_worker_for_tests();
    drop(fs);
    worker
        .await
        .expect("release database/catalog before reopen");
    pool.settle().await;
}

fn payload(seed: u32) -> Bytes {
    (0..4097_u32)
        .flat_map(|index| Sha256::digest([seed.to_le_bytes(), index.to_le_bytes()].concat()))
        .collect::<Vec<_>>()
        .into()
}

#[derive(Clone)]
struct ExpectedNamespace {
    directory: u64,
    file: u64,
    empty_directory: u64,
    empty_file: u64,
    payload: Bytes,
}

async fn populate(fs: &ZeroFS) -> ExpectedNamespace {
    let auth = AuthContext::default();
    let creds = Credentials::from_auth_context(&auth);
    let (directory, _) = fs
        .mkdir(&creds, 0, b"Media", &SetAttributes::default())
        .await
        .unwrap();
    let (empty_directory, _) = fs
        .mkdir(&creds, 0, b"Empty", &SetAttributes::default())
        .await
        .unwrap();
    let (empty_file, _) = fs
        .create(&creds, directory, b"empty", &SetAttributes::default())
        .await
        .unwrap();
    let (file, _) = fs
        .create(&creds, directory, b"staging", &SetAttributes::default())
        .await
        .unwrap();
    fs.write(&auth, file, 0, &payload(1)).await.unwrap();
    let _ = fs.administrative_remote_durability().await.unwrap();
    let current = payload(2);
    fs.write(&auth, file, 0, &current).await.unwrap();
    fs.setattr(
        &creds,
        file,
        &SetAttributes {
            size: SetSize::Set(FINAL_SIZE as u64),
            mode: SetMode::Set(0o640),
            uid: SetUid::Set(1001),
            gid: SetGid::Set(1002),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    fs.link(&auth, file, 0, b"alias").await.unwrap();
    fs.rename(&auth, directory, b"staging", directory, b"movie.bin")
        .await
        .unwrap();
    let (removed, _) = fs
        .create(&creds, directory, b"removed", &SetAttributes::default())
        .await
        .unwrap();
    fs.write(&auth, removed, 0, &Bytes::from_static(b"deleted payload"))
        .await
        .unwrap();
    let _ = fs.administrative_remote_durability().await.unwrap();
    fs.remove(&auth, directory, b"removed").await.unwrap();
    ExpectedNamespace {
        directory,
        file,
        empty_directory,
        empty_file,
        payload: current.slice(..FINAL_SIZE),
    }
}

async fn verify_namespace(fs: &ZeroFS, expected: &ExpectedNamespace) -> Result<()> {
    let auth = AuthContext::default();
    let creds = Credentials::from_auth_context(&auth);
    let directory = fs
        .lookup(&creds, 0, b"Media")
        .await
        .context("lookup /Media")?;
    ensure!(
        directory == expected.directory,
        "directory identity changed"
    );
    let file = fs.lookup(&creds, directory, b"movie.bin").await?;
    ensure!(file == expected.file, "file identity changed");
    ensure!(
        fs.lookup(&creds, 0, b"alias").await? == file,
        "hardlink lost"
    );
    ensure!(fs.lookup(&creds, 0, b"Empty").await? == expected.empty_directory);
    ensure!(fs.lookup(&creds, directory, b"empty").await? == expected.empty_file);
    for name in [b"staging".as_slice(), b"removed"] {
        ensure!(fs.lookup(&creds, directory, name).await == Err(FsError::NotFound));
    }
    let inode = fs.inode_store.get(file).await?;
    ensure!((inode.mode(), inode.uid(), inode.gid(), inode.nlink()) == (0o640, 1001, 1002, 2));
    ensure!(matches!(inode, Inode::File(ref file) if file.size == FINAL_SIZE as u64));
    let (actual, eof) = fs
        .read_file(&auth, file, 0, (FINAL_SIZE + 1) as u32)
        .await?;
    ensure!(
        actual == expected.payload && eof,
        "current payload/truncation mismatch"
    );
    let (empty, eof) = fs.read_file(&auth, expected.empty_file, 0, 1).await?;
    ensure!(empty.is_empty() && eof, "empty file changed");
    for (id, names) in [
        (
            0,
            vec![b"Empty".to_vec(), b"Media".to_vec(), b"alias".to_vec()],
        ),
        (directory, vec![b"empty".to_vec(), b"movie.bin".to_vec()]),
        (expected.empty_directory, vec![]),
    ] {
        let listing = fs.readdir(&auth, id, 0, 32).await?;
        ensure!(listing.end, "incomplete namespace scan");
        let mut actual: Vec<_> = listing
            .entries
            .into_iter()
            .map(|entry| entry.name)
            .filter(|name| name != b"." && name != b"..")
            .collect();
        actual.sort();
        ensure!(actual == names, "directory entries changed");
    }
    Ok(())
}

#[derive(Debug, PartialEq, Eq)]
struct MemberObject {
    path: PathBuf,
    sha256: [u8; 32],
}

async fn member_objects(fixture: &Fixture, pool: &PoolObjectStore) -> Vec<MemberObject> {
    let placements = pool.placements().await.unwrap();
    assert!(!placements.is_empty(), "fixture must publish real segments");
    for member in ["disk-a", "disk-b"] {
        assert!(
            placements
                .iter()
                .any(|placement| placement.member_id == member),
            "fixture must publish on {member}"
        );
    }
    placements
        .into_iter()
        .map(|placement| {
            let path = fixture
                .root
                .path()
                .join(format!("{}-device", placement.member_id))
                .join("data")
                .join(placement.physical_key);
            let body = std::fs::read(&path).unwrap();
            let footer = &body[body.len() - FOOTER_LEN..];
            parse_footer(footer, body.len() as u64).expect("real ZeroFS segment footer");
            MemberObject {
                path,
                sha256: Sha256::digest(body).into(),
            }
        })
        .collect()
}

async fn observe_loss(loss: Loss) -> Result<()> {
    let fixture = Fixture::new();
    let (fs, pool) = open(&fixture.settings, true).await.unwrap();
    let expected = populate(&fs).await;
    let receipt = fs.administrative_remote_durability().await.unwrap();
    assert_eq!(receipt.target, DurabilityTarget::RemoteBackend);
    assert_eq!(receipt.object_coverage, ObjectCoverage::DirectRemote);
    let pool_id = pool.status().await.unwrap().pool_id;
    let objects = member_objects(&fixture, &pool).await;
    close(fs, pool).await;

    let (fs, pool) = open(&fixture.settings, false).await.unwrap();
    verify_namespace(&fs, &expected)
        .await
        .expect("cold-open control before loss");
    assert_eq!(pool.status().await.unwrap().pool_id, pool_id);
    assert_eq!(member_objects(&fixture, &pool).await, objects);
    close(fs, pool).await;
    fixture.lose(loss);

    let recovered = match open(&fixture.settings, false).await {
        Ok((fs, pool)) => {
            let result = verify_namespace(&fs, &expected).await;
            close(fs, pool).await;
            result
        }
        Err(error) => Err(error),
    };
    for object in &objects {
        let body = std::fs::read(&object.path).expect("member payload must survive authority loss");
        assert_eq!(<[u8; 32]>::from(Sha256::digest(body)), object.sha256);
    }
    let error = recovered.as_ref().err().map(|error| format!("{error:#}"));
    fixture
        .root
        .close()
        .expect("remove all owned fixture state before reporting");
    println!(
        "M0_METADATA_LOSS_REPRODUCTION {}",
        serde_json::json!({
            "loss": format!("{loss:?}"),
            "survival_gate": if recovered.is_ok() { "passed" } else { "failed" },
            "cold_control": "passed", "surviving_segment_objects": objects.len(),
            "backend_mutation_cutoff": receipt.mutation_cutoff.sequence,
            "writeback": false, "cleanup": "passed", "error": error,
        })
    );
    recovered
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn baseline_metadata_loss_is_not_recoverable() {
    tokio::time::timeout(Duration::from_secs(120), async {
        for (loss, expected_error) in [
            (Loss::Authority, "authority identity marker is missing"),
            (Loss::Catalog, "pool catalog is missing"),
            (Loss::AuthorityDevice, "pool catalog is missing"),
            (Loss::WrappedKey, "missing existing encryption key"),
        ] {
            let error = observe_loss(loss)
                .await
                .expect_err("baseline survival gate must fail");
            assert!(
                format!("{error:#}").contains(expected_error),
                "unexpected {loss:?} failure: {error:#}"
            );
        }
    })
    .await
    .expect("metadata-loss reproduction exceeded deadline");
}

// These are controls for the existing writeback durability contract, not
// metadata-survival acceptance tests. No replica is configured and a backend
// receipt is not a survival receipt. The strict survival contracts live in the acceptance module below.
struct DurablePrefixFixture {
    fixture: Fixture,
    expected: ExpectedNamespace,
    pool_id: String,
}

impl DurablePrefixFixture {
    async fn new() -> Self {
        let fixture = Fixture::new();
        let (fs, pool) = open(&fixture.settings, true).await.unwrap();
        let expected = populate(&fs).await;
        let _ = fs.administrative_remote_durability().await.unwrap();
        let pool_id = pool.status().await.unwrap().pool_id;
        close(fs, pool).await;
        let control = Self {
            fixture,
            expected,
            pool_id,
        };
        control.verify_cold_prefix(&control.expected).await;
        control
    }

    async fn verify_cold_prefix(&self, expected: &ExpectedNamespace) {
        let (fs, pool) = open(&self.fixture.settings, false).await.unwrap();
        let result = verify_namespace(&fs, expected).await;
        assert_eq!(pool.status().await.unwrap().pool_id, self.pool_id);
        close(fs, pool).await;
        result.expect("cold namespace and payload must match the expected durable prefix");
    }

    fn finish(self, case: &str, result: Result<()>) -> Result<()> {
        let error = result.as_ref().err().map(|error| format!("{error:#}"));
        let root = self.fixture.root.path().to_owned();
        self.fixture
            .root
            .close()
            .expect("remove durability-control fixture");
        assert!(!root.exists(), "durability-control fixture must be absent");
        println!(
            "METADATA_SURVIVAL_DURABILITY_CONTROL {}",
            serde_json::json!({
                "case": case,
                "survival_acceptance": "not exercised",
                "cold_control": "passed",
                "cleanup": "passed",
                "fixture": root,
                "error": error,
            })
        );
        result
    }
}

async fn observe_journal_device_loss(after_backend_cutoff: bool) -> Result<()> {
    let mut control = DurablePrefixFixture::new().await;
    let journal_device = control
        .fixture
        .root
        .path()
        .canonicalize()?
        .join("journal-device");
    let (fs, pool, writeback) = open_with_writeback(
        &control.fixture.settings,
        false,
        Some(journal_device.join("journal")),
    )
    .await?;
    let writeback = writeback.unwrap();
    let mut suffix = control.expected.clone();
    suffix.payload = payload(3).slice(..FINAL_SIZE);
    fs.write(&AuthContext::default(), suffix.file, 0, &suffix.payload)
        .await
        .unwrap();
    let receipt = fs
        .flush_coordinator
        .durable_through(fs.capture_mutation_cutoff(), DurabilityTarget::LocalSsd)
        .await
        .unwrap();
    assert_eq!(receipt.target, DurabilityTarget::LocalSsd);
    assert!(matches!(
        receipt.object_coverage,
        ObjectCoverage::Writeback { .. }
    ));
    verify_namespace(&fs, &suffix).await.unwrap();
    let local = writeback.status().unwrap();
    assert!(local.local_seq > local.remote_seq);
    assert_eq!(local.remote_seq, 0, "remote replay must still be paused");
    if after_backend_cutoff {
        writeback.activate_remote().unwrap();
        let receipt = fs.administrative_remote_durability().await.unwrap();
        assert_eq!(receipt.target, DurabilityTarget::RemoteBackend);
        assert!(matches!(
            receipt.object_coverage,
            ObjectCoverage::Writeback { .. }
        ));
        control.expected = suffix;
    }
    close(fs, pool).await;
    if after_backend_cutoff {
        writeback
            .wait_remote(writeback.accepted_sequence())
            .await
            .unwrap();
    }
    let status = writeback.status().unwrap();
    writeback.shutdown().await.unwrap();
    drop(writeback);
    std::fs::remove_dir_all(&journal_device)?;
    assert!(!journal_device.exists());
    control.verify_cold_prefix(&control.expected).await;
    println!(
        "METADATA_SURVIVAL_JOURNAL_LOSS {}",
        serde_json::json!({
            "after_backend_cutoff": after_backend_cutoff,
            "local_sequence": status.local_seq,
            "remote_sequence": status.remote_seq,
            "journal_absent": true,
            "cold_prefix": "passed",
            "survival_commit": "not implemented",
        })
    );
    control.finish(
        if after_backend_cutoff {
            "journal-device-after-backend-cutoff"
        } else {
            "journal-device-before-backend-cutoff"
        },
        Ok(()),
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn journal_loss_before_backend_cutoff_preserves_only_the_backend_prefix() {
    tokio::time::timeout(Duration::from_secs(120), observe_journal_device_loss(false))
        .await
        .expect("journal-loss control exceeded deadline")
        .expect("The existing backend prefix must remain readable after journal loss");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn journal_loss_after_backend_cutoff_preserves_the_drained_suffix() {
    tokio::time::timeout(Duration::from_secs(120), observe_journal_device_loss(true))
        .await
        .expect("journal-loss control exceeded deadline")
        .expect("The remotely durable suffix must remain readable after journal loss");
}

pub(crate) mod acceptance;
