use crate::writeback::anchored_dir::AnchoredDir;
use crate::writeback::config::WritebackSettings;
use crate::writeback::journal::Journal;
use crate::writeback::model::JournalIdentity;
use crate::writeback::reservation::SsdAdmission;
use crate::writeback::space_sample::PhysicalSpaceSampler;
use crate::writeback::store::WritebackObjectStore;
use object_store::ObjectStore;
use std::sync::Arc;

pub(crate) struct AttachedWriteback {
    pub(crate) store: Arc<dyn ObjectStore>,
    pub(crate) lifecycle: WritebackObjectStore,
    // Landed-but-not-wired: consumers arrive with the tiered admission wiring.
    #[allow(dead_code)]
    pub(crate) space: Arc<PhysicalSpaceSampler>,
    #[allow(dead_code)]
    pub(crate) ssd: Arc<SsdAdmission>,
}

/// Recover the local overlay while leaving remote replay paused.
///
/// The caller must invoke [`WritebackObjectStore::activate_remote`] after it
/// finishes opening the database over `store`. This keeps the remote manifest
/// view from changing underneath database recovery without making startup wait
/// for the entire SSD backlog to reach the backend.
pub(crate) async fn attach(
    remote: Arc<dyn ObjectStore>,
    mut settings: WritebackSettings,
    identity: JournalIdentity,
    namespace: &str,
) -> anyhow::Result<AttachedWriteback> {
    validate_namespace(namespace)?;
    let base = AnchoredDir::open_or_create_absolute(&settings.dir, 0o700)?;
    let journal_dir = base.open_or_create_child(namespace.as_ref(), 0o700)?;
    settings.dir = journal_dir.display().to_path_buf();
    let journal = Arc::new(Journal::open_anchored(journal_dir.clone(), identity)?);
    let space = Arc::new(PhysicalSpaceSampler::new_anchored(journal_dir));
    attach_journal(remote, settings, journal, space).await
}

pub(crate) async fn attach_raw(
    remote: Arc<dyn ObjectStore>,
    settings: WritebackSettings,
    identity: JournalIdentity,
    namespace: &str,
    region: zerofs_raw_device::journal_region::TransactionRegion,
) -> anyhow::Result<AttachedWriteback> {
    validate_raw_namespace(&settings, namespace)?;

    validate_raw_budget(&region, &settings)?;
    let journal = Arc::new(Journal::open_raw(region, identity)?);
    let space = Arc::new(PhysicalSpaceSampler::new_raw(Arc::clone(&journal)));
    tracing::info!("using raw P3 Journal for writeback metadata and payloads");
    attach_journal(remote, settings, journal, space).await
}

/// Apply the same directory-journal migration guard to startup and offline
/// verification before either opens the raw journal database.
pub(crate) fn validate_raw_namespace(
    settings: &WritebackSettings,
    namespace: &str,
) -> anyhow::Result<()> {
    validate_namespace(namespace)?;
    let previous = settings.dir.join(namespace);
    match std::fs::symlink_metadata(&previous) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error.into()),
        Ok(_) => anyhow::bail!(
            "raw P3 writeback refuses an existing directory journal at {}; drain and migrate it first",
            previous.display()
        ),
    }
}

/// Check that the payload log holds the whole dirty budget and the free
/// space SSD admission keeps beside it.
pub(crate) fn validate_raw_budget(
    region: &zerofs_raw_device::journal_region::TransactionRegion,
    settings: &WritebackSettings,
) -> anyhow::Result<()> {
    let log_bytes = Journal::raw_payload_log_bytes(region);
    anyhow::ensure!(
        settings
            .disk_bytes
            .checked_add(settings.min_free_bytes)
            .is_some_and(|needed| needed <= log_bytes),
        "raw writeback disk budget and free-space reserve exceed the {log_bytes}-byte P3 Journal payload log"
    );
    Ok(())
}

async fn attach_journal(
    remote: Arc<dyn ObjectStore>,
    settings: WritebackSettings,
    journal: Arc<Journal>,
    space: Arc<PhysicalSpaceSampler>,
) -> anyhow::Result<AttachedWriteback> {
    let recovery = journal.progress()?;
    let sample = space.sample().await?;
    let pending = journal.pending_ssd_reservations()?;
    let ssd = Arc::new(SsdAdmission::recover(
        settings.disk_bytes,
        journal.max_pending_operations(),
        settings.high_watermark_percent,
        settings.resume_percent,
        settings.min_free_bytes,
        pending,
        Some(sample),
    )?);
    let lifecycle = WritebackObjectStore::open_paused_with_owners(
        remote,
        journal,
        settings,
        Arc::clone(&space),
        Arc::clone(&ssd),
    )
    .await?;
    if recovery.remote_seq < recovery.local_seq {
        tracing::info!(
            remote_sequence = recovery.remote_seq,
            local_sequence = recovery.local_seq,
            pending_operations = recovery.local_seq - recovery.remote_seq,
            "attached a stable recovered writeback overlay; remote replay remains paused until filesystem initialization completes"
        );
    }
    Ok(AttachedWriteback {
        store: Arc::new(lifecycle.clone()),
        lifecycle,
        space,
        ssd,
    })
}

fn validate_namespace(namespace: &str) -> anyhow::Result<()> {
    if namespace.is_empty()
        || namespace == "."
        || namespace == ".."
        || namespace.contains('/')
        || namespace.contains('\\')
    {
        anyhow::bail!("writeback namespace must be one safe path component");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::fault_store::FaultStore;
    use crate::writeback::config::{AckMode, ShutdownFlush, WritebackSettings};
    use crate::writeback::model::JournalIdentity;
    use bytes::Bytes;
    use futures::TryStreamExt;
    use object_store::{ObjectStoreExt, PutMode, PutOptions, memory::InMemory, path::Path};
    use std::sync::Arc;
    #[cfg(unix)]
    use std::{fs, os::unix::fs::PermissionsExt};

    fn physical_child(temp: &tempfile::TempDir, name: &str) -> std::path::PathBuf {
        temp.path().canonicalize().unwrap().join(name)
    }

    #[tokio::test]
    async fn raw_attachment_fences_replays_and_retires_without_a_directory() {
        use zerofs_raw_device::{Identity, Role, journal_region::TransactionRegion};

        const CAPACITY: u64 = 64 << 20;
        const ID: Identity = Identity {
            pool_uuid: [1; 16],
            member_uuid: [2; 16],
            role: Role::Journal,
        };
        let file = tempfile::tempfile().unwrap();
        file.set_len(CAPACITY).unwrap();
        let region =
            TransactionRegion::provision(file.try_clone().unwrap(), CAPACITY, 0, CAPACITY, ID)
                .unwrap();
        let temp = tempfile::tempdir().unwrap();
        let settings = WritebackSettings {
            dir: physical_child(&temp, "unused-directory"),
            ack_mode: AckMode::Memory,
            memory_bytes: 4 << 20,
            disk_bytes: 8 << 20,
            min_free_bytes: 1,
            high_watermark_percent: 95,
            resume_percent: 85,
            upload_concurrency: 2,
            local_concurrency: 2,
            shutdown_flush: ShutdownFlush::Local,
        };
        let identity = JournalIdentity {
            format_version: 1,
            bucket_id: "raw-bucket".to_owned(),
            backend_endpoint: "raw-pool".to_owned(),
            database_prefix: "db".to_owned(),
            backend_kind: "raw".to_owned(),
            encryption_key_identity_sha256: [7; 32],
        };
        let remote = Arc::new(InMemory::new());
        let (partitioned, controls) = FaultStore::new(remote.clone());
        controls.partition_writes(true);
        let attached = super::attach_raw(
            partitioned.clone(),
            settings.clone(),
            identity.clone(),
            "bucket_fixture",
            region,
        )
        .await
        .unwrap();
        let sample = attached.space.sample().await.unwrap();
        assert!(sample.available_bytes < CAPACITY - 2 * zerofs_raw_device::BLOCK_SIZE);
        let path = Path::from("db/segments/1");
        let payload = Bytes::from(vec![0x58; 132 << 10]);
        attached
            .store
            .put(&path, payload.clone().into())
            .await
            .unwrap();
        attached
            .lifecycle
            .wait_local_through_accepted()
            .await
            .unwrap();
        assert_eq!(attached.lifecycle.status().unwrap().local_seq, 1);
        assert!(remote.head(&path).await.is_err());
        attached.lifecycle.shutdown().await.unwrap();
        drop(attached);

        let region =
            TransactionRegion::open(file.try_clone().unwrap(), CAPACITY, 0, CAPACITY, ID).unwrap();
        let attached = super::attach_raw(
            partitioned,
            settings.clone(),
            identity.clone(),
            "bucket_fixture",
            region,
        )
        .await
        .unwrap();
        assert_eq!(
            attached
                .store
                .get(&path)
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap(),
            payload
        );
        assert!(remote.head(&path).await.is_err());
        controls.partition_writes(false);
        attached.lifecycle.activate_remote().unwrap();
        tokio::time::timeout(
            std::time::Duration::from_secs(5),
            attached.lifecycle.wait_remote(1),
        )
        .await
        .unwrap()
        .unwrap();
        attached.lifecycle.shutdown().await.unwrap();
        drop(attached);
        assert_eq!(
            remote.get(&path).await.unwrap().bytes().await.unwrap(),
            payload
        );

        let region = TransactionRegion::open(file, CAPACITY, 0, CAPACITY, ID).unwrap();
        let journal = super::Journal::open_raw(region, identity).unwrap();
        assert_eq!(journal.progress().unwrap().remote_seq, 1);
        assert!(journal.mutation(1).unwrap().is_none());
        assert!(!settings.dir.exists());
    }

    /// What a kill leaves on the P3: the bytes the journal has handed the
    /// device so far, with no clean close.
    #[cfg(unix)]
    fn killed_image(media: &fs::File) -> fs::File {
        use std::os::unix::fs::FileExt;
        let len = media.metadata().unwrap().len();
        let image = tempfile::tempfile().unwrap();
        image.set_len(len).unwrap();
        let mut buffer = vec![0; 1 << 20];
        for offset in (0..len).step_by(buffer.len()) {
            let count = (len - offset).min(buffer.len() as u64) as usize;
            media.read_exact_at(&mut buffer[..count], offset).unwrap();
            image.write_all_at(&buffer[..count], offset).unwrap();
        }
        image
    }

    #[cfg(unix)]
    mod raw_replay {
        use super::super::{AttachedWriteback, attach_raw};
        use super::killed_image;
        use crate::fault_store::{FaultControls, FaultStore};
        use crate::segment::Segid;
        use crate::segment_store::GeneratedSegmentCreate;
        use crate::writeback::config::{AckMode, ShutdownFlush, WritebackSettings};
        use crate::writeback::model::JournalIdentity;
        use bytes::Bytes;
        use object_store::{
            ObjectStore, ObjectStoreExt, PutMode, PutOptions, memory::InMemory, path::Path,
        };
        use std::sync::Arc;
        use std::time::{Duration, Instant};
        use zerofs_raw_device::{Identity, Role, journal_region::TransactionRegion};

        const CAPACITY: u64 = 256 << 20;
        const ID: Identity = Identity {
            pool_uuid: [3; 16],
            member_uuid: [4; 16],
            role: Role::Journal,
        };
        /// The raw journal's payload chunk and the remote reconciliation range.
        const PAYLOAD_CHUNK: usize = 60 << 10;
        const REMOTE_RANGE: usize = 16 << 20;

        struct Fixture {
            remote: Arc<InMemory>,
            store: Arc<FaultStore>,
            controls: Arc<FaultControls>,
            settings: WritebackSettings,
            identity: JournalIdentity,
            _temp: tempfile::TempDir,
        }

        impl Fixture {
            fn new() -> Self {
                let temp = tempfile::tempdir().unwrap();
                let remote = Arc::new(InMemory::new());
                let (store, controls) = FaultStore::new(remote.clone());
                Self {
                    remote,
                    store,
                    controls,
                    settings: WritebackSettings {
                        dir: super::physical_child(&temp, "unused-directory"),
                        ack_mode: AckMode::Memory,
                        memory_bytes: 60 << 20,
                        disk_bytes: 60 << 20,
                        min_free_bytes: 1,
                        high_watermark_percent: 95,
                        resume_percent: 85,
                        upload_concurrency: 4,
                        local_concurrency: 2,
                        shutdown_flush: ShutdownFlush::Local,
                    },
                    identity: JournalIdentity {
                        format_version: 1,
                        bucket_id: "raw-bucket".to_owned(),
                        backend_endpoint: "raw-pool".to_owned(),
                        database_prefix: "db".to_owned(),
                        backend_kind: "raw".to_owned(),
                        encryption_key_identity_sha256: [7; 32],
                    },
                    _temp: temp,
                }
            }

            /// Start the engine on `media`, as a restart does: the overlay
            /// recovers and remote replay stays paused until activated.
            async fn start(&self, media: &std::fs::File) -> AttachedWriteback {
                let region =
                    TransactionRegion::open(media.try_clone().unwrap(), CAPACITY, 0, CAPACITY, ID)
                        .unwrap();
                attach_raw(
                    self.store.clone(),
                    self.settings.clone(),
                    self.identity.clone(),
                    "bucket_fixture",
                    region,
                )
                .await
                .unwrap()
            }

            async fn published(&self, path: &Path) -> Option<Bytes> {
                match self.remote.get(path).await {
                    Ok(result) => Some(result.bytes().await.unwrap()),
                    Err(object_store::Error::NotFound { .. }) => None,
                    Err(error) => panic!("remote read of {path} failed: {error}"),
                }
            }

            /// Wait for `path` to be published, failing as soon as replay
            /// stops for good instead of timing out.
            async fn wait_published(&self, engine: &AttachedWriteback, path: &Path) {
                let deadline = Instant::now() + Duration::from_secs(60);
                while self.published(path).await.is_none() {
                    let status = engine.lifecycle.status().unwrap();
                    assert_eq!(
                        status.terminal_error, None,
                        "replay stopped before publishing {path}"
                    );
                    assert!(Instant::now() < deadline, "{path} was never published");
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            }
        }

        fn segment(counter: u64) -> Path {
            Path::from(format!("db/{}", Segid::new(3, counter).object_key()))
        }

        fn pattern(seed: u32, len: usize) -> Bytes {
            let mut state = seed;
            (0..len)
                .map(|_| {
                    state = state.wrapping_mul(1_664_525).wrapping_add(1_013_904_223);
                    (state >> 24) as u8
                })
                .collect::<Vec<_>>()
                .into()
        }

        async fn seal(engine: &AttachedWriteback, path: &Path, payload: &Bytes) {
            let mut options = PutOptions::from(PutMode::Create);
            options.extensions.insert(GeneratedSegmentCreate);
            engine
                .store
                .put_opts(path, payload.clone().into(), options)
                .await
                .unwrap();
        }

        /// The N5 production wedge of 2026-11-20. Segments are sealed into a
        /// raw P3 journal faster than the HDDs take them. The engine is
        /// killed while the frontier upload is still in flight and segments
        /// behind it are already published, so the journal's remote watermark
        /// does not cover them. Every later start replays those creates
        /// against objects that already hold the same bytes. Replay must
        /// reconcile them and drain; engine 102b0e59 instead reported a
        /// permanent remote divergence for any such segment larger than one
        /// 60 KiB journal payload chunk and refused every write from then on.
        #[tokio::test]
        async fn restarts_reconcile_segments_published_before_a_kill_and_drain() {
            let fixture = Fixture::new();
            let media = tempfile::tempfile().unwrap();
            media.set_len(CAPACITY).unwrap();
            TransactionRegion::provision(media.try_clone().unwrap(), CAPACITY, 0, CAPACITY, ID)
                .unwrap();

            // The backlog: one ordering fence, then five sealed segments.
            let manifest = Path::from("db/manifest/00000000000000000001.manifest");
            let manifest_bytes = pattern(1, 4 << 10);
            let segments = [
                (segment(0xb60), pattern(2, PAYLOAD_CHUNK + 1)),
                (segment(0xb61), pattern(3, 4 << 20)),
                (segment(0xb62), pattern(4, REMOTE_RANGE + PAYLOAD_CHUNK + 1)),
                (segment(0xb63), pattern(5, 200 << 10)),
                (segment(0xb64), pattern(6, 4 << 20)),
            ];

            // First run: the HDDs stall on the fence while two segments
            // behind it finish publishing.
            fixture.controls.block_puts();
            let engine = fixture.start(&media).await;
            engine
                .store
                .put_opts(
                    &manifest,
                    manifest_bytes.clone().into(),
                    PutMode::Create.into(),
                )
                .await
                .unwrap();
            for (path, payload) in &segments {
                seal(&engine, path, payload).await;
            }
            engine
                .lifecycle
                .wait_local_through_accepted()
                .await
                .unwrap();
            engine.lifecycle.activate_remote().unwrap();
            for (path, _) in &segments[1..3] {
                fixture.controls.release_put_path(path.as_ref());
                fixture.wait_published(&engine, path).await;
            }
            let status = engine.lifecycle.status().unwrap();
            assert_eq!((status.remote_seq, status.local_seq), (0, 6));

            // SIGKILL: the P3 keeps what it held at this instant. The dying
            // process publishes nothing more.
            let media = killed_image(&media);
            engine.lifecycle.shutdown().await.unwrap();
            drop(engine);
            assert_eq!(fixture.published(&manifest).await, None);

            // Second run: restart with the backlog. Replay meets the two
            // published segments and must accept them as its own bytes.
            let engine = fixture.start(&media).await;
            let status = engine.lifecycle.status().unwrap();
            assert_eq!((status.remote_seq, status.local_seq), (0, 6));
            engine.lifecycle.activate_remote().unwrap();
            for (path, _) in &segments[1..3] {
                fixture.controls.release_put_path(path.as_ref());
            }
            // The lanes those two free carry the next segment, which then
            // also publishes ahead of the still-stalled fence.
            fixture.controls.release_put_path(segments[3].0.as_ref());
            fixture.wait_published(&engine, &segments[3].0).await;
            let status = engine.lifecycle.status().unwrap();
            assert_eq!((status.remote_seq, status.terminal_error), (0, None));

            // A graceful stop in the middle of that drain, then a third run
            // on healthy HDDs.
            engine.lifecycle.shutdown().await.unwrap();
            drop(engine);
            fixture.controls.release_puts();
            let engine = fixture.start(&media).await;
            engine.lifecycle.activate_remote().unwrap();
            tokio::time::timeout(Duration::from_secs(60), engine.lifecycle.wait_remote(6))
                .await
                .expect("the backlog drains")
                .expect("replay reconciles every published segment");
            let status = engine.lifecycle.status().unwrap();
            assert_eq!(
                (status.remote_seq, status.local_seq, status.terminal_error),
                (6, 6, None)
            );
            assert_eq!(fixture.published(&manifest).await, Some(manifest_bytes));
            for (path, payload) in &segments {
                assert_eq!(fixture.published(path).await.as_ref(), Some(payload));
            }
            engine.lifecycle.shutdown().await.unwrap();
            drop(engine);

            // Fourth run: nothing is pending and new writes drain.
            let engine = fixture.start(&media).await;
            let status = engine.lifecycle.status().unwrap();
            assert_eq!((status.remote_seq, status.local_seq), (6, 6));
            engine.lifecycle.activate_remote().unwrap();
            let (path, payload) = (segment(0xb65), pattern(7, 4 << 20));
            seal(&engine, &path, &payload).await;
            tokio::time::timeout(Duration::from_secs(60), engine.lifecycle.wait_remote(7))
                .await
                .expect("the new segment drains")
                .unwrap();
            assert_eq!(fixture.published(&path).await, Some(payload));
            engine.lifecycle.shutdown().await.unwrap();
        }

        /// The same restart still fails closed when the published object is
        /// not the journal's: replay stops for good and refuses new writes
        /// instead of accepting or overwriting the other bytes.
        #[tokio::test]
        async fn a_restart_still_refuses_a_published_segment_with_other_bytes() {
            let fixture = Fixture::new();
            let media = tempfile::tempfile().unwrap();
            media.set_len(CAPACITY).unwrap();
            TransactionRegion::provision(media.try_clone().unwrap(), CAPACITY, 0, CAPACITY, ID)
                .unwrap();
            let (path, payload) = (segment(0xb64), pattern(8, 4 << 20));
            // Same length, one different byte in the last payload chunk.
            let mut other = payload.to_vec();
            *other.last_mut().unwrap() ^= 1;
            let other = Bytes::from(other);
            fixture
                .remote
                .put(&path, other.clone().into())
                .await
                .unwrap();

            let engine = fixture.start(&media).await;
            seal(&engine, &path, &payload).await;
            engine
                .lifecycle
                .wait_local_through_accepted()
                .await
                .unwrap();
            let media = killed_image(&media);
            engine.lifecycle.shutdown().await.unwrap();
            drop(engine);

            let engine = fixture.start(&media).await;
            engine.lifecycle.activate_remote().unwrap();
            let error =
                tokio::time::timeout(Duration::from_secs(60), engine.lifecycle.wait_remote(1))
                    .await
                    .expect("replay reaches a verdict")
                    .expect_err("other bytes must not reconcile");
            assert!(
                error
                    .to_string()
                    .contains("permanent remote divergence at sequence 1"),
                "{error}"
            );
            assert_eq!(fixture.published(&path).await, Some(other));
            let refused = engine
                .store
                .put(&segment(0xb65), Bytes::from_static(b"later").into())
                .await
                .expect_err("a diverged journal admits no further writes");
            assert!(refused.to_string().contains("poisoned"), "{refused}");
            let _ = engine.lifecycle.shutdown().await;
        }
    }

    #[tokio::test]
    async fn attachment_scopes_the_journal_and_routes_writes_through_the_overlay() {
        let temp = tempfile::tempdir().unwrap();
        let remote = Arc::new(InMemory::new());
        let (partitioned, controls) = FaultStore::new(remote.clone());
        controls.partition_writes(true);
        let base = physical_child(&temp, "dirty");
        let settings = WritebackSettings {
            dir: base.clone(),
            ack_mode: AckMode::Memory,
            memory_bytes: 16_000_000_000,
            disk_bytes: 512_000_000_000,
            min_free_bytes: 1,
            high_watermark_percent: 95,
            resume_percent: 85,
            upload_concurrency: 4,
            local_concurrency: 4,
            shutdown_flush: ShutdownFlush::Local,
        };
        let identity = JournalIdentity {
            format_version: 1,
            bucket_id: "bucket-123".to_owned(),
            backend_endpoint: "sftp://storage.example:23".to_owned(),
            database_prefix: "zerofs/pilot".to_owned(),
            backend_kind: "sftp".to_owned(),
            encryption_key_identity_sha256: [0x55; 32],
        };

        let attached = super::attach(partitioned, settings, identity, "bucket_12345678")
            .await
            .unwrap();
        let location = Path::from("zerofs/pilot/segments/1");
        attached
            .store
            .put(&location, Bytes::from_static(b"payload").into())
            .await
            .unwrap();

        assert_eq!(
            attached
                .store
                .get(&location)
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap(),
            Bytes::from_static(b"payload")
        );
        assert!(remote.head(&location).await.is_err());
        assert!(base.join("bucket_12345678").join("journal.redb").exists());
        attached.lifecycle.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn recovered_journal_is_stable_and_available_before_remote_replay() {
        let temp = tempfile::tempdir().unwrap();
        let remote = Arc::new(InMemory::new());
        let (partitioned, controls) = FaultStore::new(remote.clone());
        controls.partition_writes(true);
        let settings = WritebackSettings {
            dir: physical_child(&temp, "dirty"),
            ack_mode: AckMode::Memory,
            memory_bytes: 1_000_000,
            disk_bytes: 10_000_000,
            min_free_bytes: 1,
            high_watermark_percent: 95,
            resume_percent: 85,
            upload_concurrency: 4,
            local_concurrency: 4,
            shutdown_flush: ShutdownFlush::Local,
        };
        let identity = JournalIdentity {
            format_version: 1,
            bucket_id: "bucket-recovery".to_owned(),
            backend_endpoint: "sftp://storage.example:23".to_owned(),
            database_prefix: "zerofs/recovery".to_owned(),
            backend_kind: "sftp".to_owned(),
            encryption_key_identity_sha256: [0x66; 32],
        };
        let first = super::attach(
            partitioned.clone(),
            settings.clone(),
            identity.clone(),
            "bucket_recovery",
        )
        .await
        .unwrap();
        for sequence in 0..32 {
            first
                .store
                .put(
                    &Path::from(format!("zerofs/recovery/segments/{sequence:02}")),
                    Bytes::from(format!("segment-{sequence:02}")).into(),
                )
                .await
                .unwrap();
        }
        first.lifecycle.wait_local_through_accepted().await.unwrap();
        first.lifecycle.shutdown().await.unwrap();
        drop(first);

        let puts_before_recovery = controls.put_count();
        let recovered = tokio::time::timeout(
            std::time::Duration::from_secs(1),
            super::attach(partitioned, settings, identity, "bucket_recovery"),
        )
        .await
        .expect("an offline remote must not make recovered attachment wait for a full drain")
        .unwrap();
        let visible = recovered
            .store
            .list(Some(&Path::from("zerofs/recovery/segments")))
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert_eq!(visible.len(), 32);
        assert_eq!(
            recovered
                .store
                .get(&Path::from("zerofs/recovery/segments/31"))
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap(),
            Bytes::from_static(b"segment-31")
        );
        tokio::time::sleep(std::time::Duration::from_millis(250)).await;
        assert_eq!(
            controls.put_count(),
            puts_before_recovery,
            "remote replay must remain paused while the database opens over the recovered overlay"
        );

        recovered.lifecycle.activate_remote().unwrap();
        controls.partition_writes(false);
        tokio::time::timeout(
            std::time::Duration::from_secs(5),
            recovered.lifecycle.wait_remote(32),
        )
        .await
        .expect("activated recovery should drain after the backend heals")
        .unwrap();
        recovered.lifecycle.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn cancelling_activated_recovery_stops_workers_and_releases_the_journal() {
        let temp = tempfile::tempdir().unwrap();
        let remote = Arc::new(InMemory::new());
        let (partitioned, controls) = FaultStore::new(remote);
        controls.partition_writes(true);
        let settings = WritebackSettings {
            dir: physical_child(&temp, "dirty"),
            ack_mode: AckMode::Memory,
            memory_bytes: 1_000_000,
            disk_bytes: 10_000_000,
            min_free_bytes: 1,
            high_watermark_percent: 95,
            resume_percent: 85,
            upload_concurrency: 4,
            local_concurrency: 4,
            shutdown_flush: ShutdownFlush::Local,
        };
        let identity = JournalIdentity {
            format_version: 1,
            bucket_id: "bucket-cancel".to_owned(),
            backend_endpoint: "sftp://storage.example:23".to_owned(),
            database_prefix: "zerofs/cancel".to_owned(),
            backend_kind: "sftp".to_owned(),
            encryption_key_identity_sha256: [0x67; 32],
        };
        let first = super::attach(
            partitioned.clone(),
            settings.clone(),
            identity.clone(),
            "bucket_cancel",
        )
        .await
        .unwrap();
        first
            .store
            .put(
                &Path::from("zerofs/cancel/segments/1"),
                Bytes::from_static(b"segment").into(),
            )
            .await
            .unwrap();
        first.lifecycle.wait_local_through_accepted().await.unwrap();
        first.lifecycle.shutdown().await.unwrap();
        drop(first);

        controls.partition_writes(false);
        controls.block_puts();
        let recovered = super::attach(
            partitioned.clone(),
            settings.clone(),
            identity.clone(),
            "bucket_cancel",
        )
        .await
        .unwrap();
        let puts_before_activation = controls.put_count();
        recovered.lifecycle.activate_remote().unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            while controls.put_count() == puts_before_activation {
                controls.put_activity().notified().await;
            }
        })
        .await
        .expect("activated replay should enter the blocked remote operation");
        tokio::time::timeout(
            std::time::Duration::from_secs(1),
            recovered.lifecycle.shutdown(),
        )
        .await
        .expect("shutdown must cancel an in-flight recovery operation")
        .unwrap();
        drop(recovered);
        controls.release_puts();

        let reopened = super::attach(partitioned, settings, identity, "bucket_cancel")
            .await
            .expect("cancelled recovery must release its journal lock");
        reopened.lifecycle.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn recovered_create_with_different_remote_bytes_is_terminal() {
        let temp = tempfile::tempdir().unwrap();
        let remote = Arc::new(InMemory::new());
        let settings = WritebackSettings {
            dir: physical_child(&temp, "dirty"),
            ack_mode: AckMode::Memory,
            memory_bytes: 1_000_000,
            disk_bytes: 10_000_000,
            min_free_bytes: 1,
            high_watermark_percent: 95,
            resume_percent: 85,
            upload_concurrency: 4,
            local_concurrency: 4,
            shutdown_flush: ShutdownFlush::Local,
        };
        let identity = JournalIdentity {
            format_version: 1,
            bucket_id: "bucket-divergence".to_owned(),
            backend_endpoint: "sftp://storage.example:23".to_owned(),
            database_prefix: "zerofs/divergence".to_owned(),
            backend_kind: "sftp".to_owned(),
            encryption_key_identity_sha256: [0x68; 32],
        };
        let location = Path::from("zerofs/divergence/manifest/1");
        let first = super::attach(
            remote.clone(),
            settings.clone(),
            identity.clone(),
            "bucket_divergence",
        )
        .await
        .unwrap();
        first
            .store
            .put_opts(
                &location,
                Bytes::from_static(b"locally-durable").into(),
                PutOptions::from(PutMode::Create),
            )
            .await
            .unwrap();
        first.lifecycle.wait_local_through_accepted().await.unwrap();
        first.lifecycle.shutdown().await.unwrap();
        drop(first);
        remote
            .put(&location, Bytes::from_static(b"remote-diverged").into())
            .await
            .unwrap();

        let recovered = super::attach(remote, settings, identity, "bucket_divergence")
            .await
            .unwrap();
        recovered.lifecycle.activate_remote().unwrap();
        let error = tokio::time::timeout(
            std::time::Duration::from_secs(1),
            recovered.lifecycle.wait_remote(1),
        )
        .await
        .expect("verified content divergence must not retry forever")
        .expect_err("different bytes at a create target must be terminal");
        assert!(error.to_string().contains("different bytes"));
        assert!(
            recovered
                .lifecycle
                .status()
                .unwrap()
                .terminal_error
                .is_some(),
            "terminal divergence must remain observable after waking waiters"
        );
        let shutdown_error = recovered
            .lifecycle
            .shutdown()
            .await
            .expect_err("shutdown must preserve the remote durability failure");
        assert!(shutdown_error.to_string().contains("different bytes"));
    }

    #[tokio::test]
    async fn remote_flush_shutdown_activates_a_paused_attachment() {
        let temp = tempfile::tempdir().unwrap();
        let remote = Arc::new(InMemory::new());
        let settings = WritebackSettings {
            dir: physical_child(&temp, "dirty"),
            ack_mode: AckMode::Memory,
            memory_bytes: 1_000_000,
            disk_bytes: 10_000_000,
            min_free_bytes: 1,
            high_watermark_percent: 95,
            resume_percent: 85,
            upload_concurrency: 4,
            local_concurrency: 4,
            shutdown_flush: ShutdownFlush::Remote,
        };
        let identity = JournalIdentity {
            format_version: 1,
            bucket_id: "bucket-remote-shutdown".to_owned(),
            backend_endpoint: "sftp://storage.example:23".to_owned(),
            database_prefix: "zerofs/remote-shutdown".to_owned(),
            backend_kind: "sftp".to_owned(),
            encryption_key_identity_sha256: [0x69; 32],
        };
        let location = Path::from("zerofs/remote-shutdown/segments/1");
        let attached = super::attach(remote.clone(), settings, identity, "bucket_remote_shutdown")
            .await
            .unwrap();
        attached
            .store
            .put(&location, Bytes::from_static(b"segment").into())
            .await
            .unwrap();

        tokio::time::timeout(
            std::time::Duration::from_secs(2),
            attached.lifecycle.shutdown(),
        )
        .await
        .expect("remote-flush shutdown must not wait forever on a paused scheduler")
        .unwrap();
        assert_eq!(
            remote.get(&location).await.unwrap().bytes().await.unwrap(),
            Bytes::from_static(b"segment")
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn attachment_rejects_an_existing_permissive_base_without_chmodding_it() {
        let temp = tempfile::tempdir().unwrap();
        let base = physical_child(&temp, "dirty");
        fs::create_dir(&base).unwrap();
        fs::set_permissions(&base, fs::Permissions::from_mode(0o755)).unwrap();
        let settings = WritebackSettings {
            dir: base.clone(),
            ack_mode: AckMode::Memory,
            memory_bytes: 1,
            disk_bytes: 1,
            min_free_bytes: 1,
            high_watermark_percent: 95,
            resume_percent: 85,
            upload_concurrency: 1,
            local_concurrency: 4,
            shutdown_flush: ShutdownFlush::Local,
        };
        let identity = JournalIdentity {
            format_version: 1,
            bucket_id: "bucket-123".to_owned(),
            backend_endpoint: "sftp://storage.example:23".to_owned(),
            database_prefix: "zerofs/pilot".to_owned(),
            backend_kind: "sftp".to_owned(),
            encryption_key_identity_sha256: [0x55; 32],
        };

        let error = super::attach(
            Arc::new(InMemory::new()),
            settings,
            identity,
            "bucket_12345678",
        )
        .await
        .err()
        .expect("permissive base should be rejected");

        assert!(format!("{error:#}").contains("mode"));
        assert_eq!(
            fs::metadata(&base).unwrap().permissions().mode() & 0o777,
            0o755
        );
    }

    #[tokio::test]
    async fn attachment_samples_the_namespaced_writeback_filesystem() {
        let temp = tempfile::tempdir().unwrap();
        let base = physical_child(&temp, "dirty");
        let settings = WritebackSettings {
            dir: base.clone(),
            ack_mode: AckMode::Memory,
            memory_bytes: 1,
            disk_bytes: 1,
            min_free_bytes: 1,
            high_watermark_percent: 95,
            resume_percent: 85,
            upload_concurrency: 1,
            local_concurrency: 4,
            shutdown_flush: ShutdownFlush::Local,
        };
        let identity = JournalIdentity {
            format_version: 1,
            bucket_id: "bucket-space".to_owned(),
            backend_endpoint: "sftp://storage.example:23".to_owned(),
            database_prefix: "zerofs/space".to_owned(),
            backend_kind: "sftp".to_owned(),
            encryption_key_identity_sha256: [0x70; 32],
        };

        let attached = super::attach(
            Arc::new(InMemory::new()),
            settings,
            identity,
            "bucket_space",
        )
        .await
        .unwrap();
        let namespaced = base.join("bucket_space");
        assert_eq!(attached.space.writeback_dir(), namespaced.as_path());
        assert_eq!(attached.space.latest_generation(), 1);
        let sample = attached.space.sample().await.unwrap();
        assert_eq!(sample.generation, 2);
        assert_eq!(attached.ssd.used_bytes(), 0);
        assert_eq!(attached.ssd.used_operations(), 0);
        assert!(
            std::sync::Arc::ptr_eq(&attached.space, attached.lifecycle.space_sampler()),
            "store must use the attach-time space sampler, not a shadow owner"
        );
        assert!(
            std::sync::Arc::ptr_eq(&attached.ssd, attached.lifecycle.ssd_admission()),
            "store must use the attach-time SSD admission owner, not a shadow owner"
        );
        attached.lifecycle.shutdown().await.unwrap();
    }
}
