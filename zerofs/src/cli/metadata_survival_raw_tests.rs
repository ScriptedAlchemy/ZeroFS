//! The metadata copy on a raw-device pool: an authority P3 that is lost and
//! two raw HDD members that hold the recovery generations.
//!
//! These tests drive the real configuration, startup, commit and recovery
//! paths. The three devices are private regular files, which test builds
//! accept in place of loop devices, provisioned exactly as real devices are.

use super::init::initialize_filesystem;
use super::server::{DatabaseMode, InitResult};
use crate::config::Settings;
use crate::fs::ZeroFS;
use crate::fs::errors::FsError;
use crate::fs::inode::{Inode, InodeAttrs};
use crate::fs::key_codec::KeyCodec;
use crate::fs::permissions::Credentials;
use crate::fs::types::{AuthContext, SetAttributes, SetGid, SetMode, SetUid};
use crate::metadata_survival::replica::InjectedFault;
use crate::metadata_survival::{SurvivalFailure, SurvivalManager, offline_status, recover};
use crate::pool_object_store::PoolObjectStore;
use crate::segment::FrameLoc;
use crate::storage_backend::raw::{RawBinding, open_recovery_p3};
use anyhow::{Context, Result, ensure};
use bytes::Bytes;
use futures::TryStreamExt;
use object_store::ObjectStoreExt;
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};
use uuid::Uuid;
use zerofs_raw_device::p3::P3Layout;
use zerofs_raw_device::{Identity, RawStore, Role};

const MIB: u64 = 1 << 20;
/// Journal, catalog, authority log, key envelope, scratch and cache.
const P3_LAYOUT_MIB: [u64; 6] = [64, 16, 48, 4, 4, 8];
const HDD_BYTES: u64 = 64 * MIB;
const PASSWORD: &str = "raw-metadata-copy-fixture";
const HDDS: [&str; 2] = ["hdd-a", "hdd-b"];
/// Metadata headroom on each 64 MiB member, far above one generation.
const HEADROOM: u64 = 8 * MIB;

struct Fixture {
    root: tempfile::TempDir,
    pool: Uuid,
    hdds: [Uuid; 2],
    p3: PathBuf,
    p3_member: Uuid,
    p3_count: u32,
    interval_seconds: u64,
    final_pause_milliseconds: u64,
    settings: Settings,
}

impl Fixture {
    /// A pool whose generations are cut only when a test asks.
    async fn new() -> Self {
        Self::with_cadence(3600, 1000).await
    }

    /// Three provisioned devices and an initialized, empty pool.
    async fn with_cadence(interval_seconds: u64, final_pause_milliseconds: u64) -> Self {
        let root = tempfile::tempdir().unwrap();
        let base = root.path().canonicalize().unwrap();
        let pool = Uuid::new_v4();
        let hdds = [Uuid::new_v4(), Uuid::new_v4()];
        for (id, member) in HDDS.iter().zip(hdds) {
            let file = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .create_new(true)
                .open(base.join(format!("{id}.img")))
                .unwrap();
            file.set_len(HDD_BYTES).unwrap();
            RawStore::provision(
                file,
                HDD_BYTES,
                Identity {
                    pool_uuid: *pool.as_bytes(),
                    member_uuid: *member.as_bytes(),
                    role: Role::Data,
                },
            )
            .unwrap();
        }
        let mut fixture = Self {
            p3: PathBuf::new(),
            p3_member: Uuid::nil(),
            p3_count: 0,
            settings: Settings::generate_default(),
            root,
            pool,
            hdds,
            interval_seconds,
            final_pause_milliseconds,
        };
        fixture.provision_blank_p3();
        drop(
            crate::storage_backend::open_pool_storage(&fixture.settings, true)
                .await
                .unwrap(),
        );
        fixture
    }

    fn base(&self) -> PathBuf {
        self.root.path().canonicalize().unwrap()
    }

    fn hdd_path(&self, id: &str) -> PathBuf {
        self.base().join(format!("{id}.img"))
    }

    /// Provision a new, blank P3 under a new device UUID for the same pool
    /// and point the configuration at it.
    fn provision_blank_p3(&mut self) {
        self.p3_count += 1;
        self.p3 = self.base().join(format!("p3-{}.img", self.p3_count));
        self.p3_member = Uuid::new_v4();
        let size = P3_LAYOUT_MIB.iter().sum::<u64>() * MIB;
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(&self.p3)
            .unwrap();
        file.set_len(size).unwrap();
        P3Layout::new(size, P3_LAYOUT_MIB.map(|mib| mib * MIB))
            .unwrap()
            .provision_v2(file, *self.pool.as_bytes(), *self.p3_member.as_bytes())
            .unwrap();
        self.settings = self.build_settings();
    }

    /// Lose the authority device entirely and fit a blank replacement.
    fn replace_lost_p3(&mut self) {
        std::fs::remove_file(&self.p3).unwrap();
        self.provision_blank_p3();
    }

    fn build_settings(&self) -> Settings {
        let base = self.base();
        let layout = P3_LAYOUT_MIB.map(|mib| mib.to_string()).join(",");
        let members: Vec<_> = HDDS
            .iter()
            .zip(self.hdds)
            .map(|(id, member)| {
                serde_json::json!({
                    "id": id,
                    "url": format!(
                        "raw+testfile://{}?pool={}&member={member}",
                        self.hdd_path(id).display(),
                        self.pool
                    ),
                    "capacity_gb": 0.064,
                    "reserve_free_gb": 0.0,
                })
            })
            .collect();
        let document = serde_json::json!({
            "cache": {"dir": base.join("cache"), "disk_size_gb": 0.0, "memory_size_gb": 0.01},
            "storage": {
                "url": format!(
                    "raw+p3-testfile://{}?pool={}&member={}&layout_mib={layout}",
                    self.p3.display(),
                    self.pool,
                    self.p3_member
                ),
                "encryption_password": PASSWORD,
                "pool": {
                    "members": members,
                    "metadata_survival": {
                        "format_version": 1,
                        "headroom_bytes": HEADROOM,
                        "commit_interval_seconds": self.interval_seconds,
                        "final_pause_max_milliseconds": self.final_pause_milliseconds,
                    },
                },
            },
            "writeback": {
                "enabled": true,
                "dir": base.join("writeback"),
                "memory_size_gb": 0.004,
                "disk_size_gb": 0.008,
                "min_free_gb": 0.001,
            },
            "servers": {},
            "telemetry": {"enabled": false},
        });
        let config = base.join("zerofs.toml");
        std::fs::write(&config, toml::to_string(&document).unwrap()).unwrap();
        Settings::from_file(config).unwrap()
    }

    async fn start(&self) -> Result<Running> {
        let write_ack = self
            .settings
            .filesystem_write_ack_settings(DatabaseMode::ReadWrite.into())?;
        let init =
            initialize_filesystem(&self.settings, DatabaseMode::ReadWrite, write_ack).await?;
        Ok(Running { init })
    }

    async fn rebuild(&self, allow_missing: &[&str]) -> Result<recover::RecoveryReport> {
        let allow: Vec<String> = allow_missing.iter().map(|id| (*id).to_owned()).collect();
        recover::rebuild_raw_p3(&self.settings, PASSWORD, &allow).await
    }

    /// True while the configured P3 holds nothing in any of its six roles.
    async fn p3_is_blank(&self) -> bool {
        let binding =
            RawBinding::parse(&url::Url::parse(&self.settings.storage.url).unwrap()).unwrap();
        open_recovery_p3(binding).await.is_ok()
    }
}

struct Running {
    init: InitResult,
}

impl Running {
    fn fs(&self) -> &Arc<ZeroFS> {
        &self.init.fs
    }

    fn pool(&self) -> &PoolObjectStore {
        self.init.pool.as_ref().unwrap()
    }

    fn survival(&self) -> &Arc<SurvivalManager> {
        self.init
            .survival
            .as_ref()
            .expect("the survival owner attaches on a writer")
    }

    /// Stop the server and release every device. No final generation is
    /// written here: a test that wants one calls `shutdown_commit` first, as
    /// the server's clean shutdown does.
    async fn stop(self) {
        let InitResult {
            fs,
            pool,
            writeback,
            survival,
            ..
        } = self.init;
        drop(survival);
        fs.stop_new_mutation_admission();
        fs.stop_mutation_workers().await.unwrap();
        fs.flush_coordinator.close().await.unwrap();
        let worker = fs.write_coordinator.take_worker_for_tests();
        drop(fs);
        worker.await.unwrap();
        if let Some(writeback) = writeback {
            writeback.shutdown().await.unwrap();
        }
        if let Some(pool) = pool {
            pool.settle().await;
        }
    }
}

fn payload(seed: u8, length: usize) -> Bytes {
    let mut bytes = Vec::with_capacity(length + 32);
    let mut block = [seed; 32];
    while bytes.len() < length {
        block = Sha256::digest(block).into();
        bytes.extend_from_slice(&block);
    }
    bytes.truncate(length);
    bytes.into()
}

async fn write_file(
    fs: &ZeroFS,
    parent: u64,
    name: &str,
    (mode, uid, gid): (u32, u32, u32),
    data: &Bytes,
) -> Result<u64> {
    let auth = AuthContext::default();
    let creds = Credentials::from_auth_context(&auth);
    let attributes = SetAttributes {
        mode: SetMode::Set(mode),
        uid: SetUid::Set(uid),
        gid: SetGid::Set(gid),
        ..Default::default()
    };
    let (inode, _) = fs
        .create(&creds, parent, name.as_bytes(), &attributes)
        .await?;
    for (index, chunk) in data.chunks(256 * 1024).enumerate() {
        fs.write(
            &auth,
            inode,
            (index * 256 * 1024) as u64,
            &Bytes::copy_from_slice(chunk),
        )
        .await?;
    }
    // One flush per file, so each file's bytes seal into their own segment
    // and can be placed on a chosen member.
    let _ = fs.administrative_remote_durability().await?;
    Ok(inode)
}

async fn make_directory(fs: &ZeroFS, parent: u64, name: &str, mode: u32) -> Result<u64> {
    let auth = AuthContext::default();
    let creds = Credentials::from_auth_context(&auth);
    let attributes = SetAttributes {
        mode: SetMode::Set(mode),
        ..Default::default()
    };
    Ok(fs
        .mkdir(&creds, parent, name.as_bytes(), &attributes)
        .await?
        .0)
}

/// The placements of a file's current extents.
async fn placements_of(
    fs: &ZeroFS,
    pool: &PoolObjectStore,
    inode: u64,
) -> Result<Vec<crate::pool_object_store::PoolPlacement>> {
    let Inode::File(file) = fs.inode_store.get(inode).await? else {
        anyhow::bail!("{inode} is not a file");
    };
    let placements = pool.placements().await?;
    let mut found = Vec::new();
    let extent = crate::fs::EXTENT_SIZE as u64;
    for index in 0..file.size.div_ceil(extent) {
        let bytes = fs
            .db
            .get_bytes(&KeyCodec::new().extent_key(inode, index))
            .await?
            .context("file extent")?;
        let key = FrameLoc::decode(&bytes)
            .context("frame location")?
            .segid
            .object_key();
        let placement = placements
            .iter()
            .find(|placement| placement.location.as_ref().ends_with(key.as_str()))
            .context("extent placement")?;
        found.push(placement.clone());
    }
    Ok(found)
}

/// The members holding a file's current extents.
async fn members_of(fs: &ZeroFS, pool: &PoolObjectStore, inode: u64) -> Result<BTreeSet<String>> {
    Ok(placements_of(fs, pool, inode)
        .await?
        .into_iter()
        .map(|placement| placement.member_id)
        .collect())
}

/// Move every segment of a file to one member.
async fn place_on(fs: &ZeroFS, pool: &PoolObjectStore, inode: u64, member: &str) -> Result<()> {
    let Inode::File(file) = fs.inode_store.get(inode).await? else {
        anyhow::bail!("{inode} is not a file");
    };
    let extent = crate::fs::EXTENT_SIZE as u64;
    let mut moved = BTreeSet::new();
    for index in 0..file.size.div_ceil(extent) {
        let bytes = fs
            .db
            .get_bytes(&KeyCodec::new().extent_key(inode, index))
            .await?
            .context("file extent")?;
        let key = FrameLoc::decode(&bytes)
            .context("frame location")?
            .segid
            .object_key();
        let placement = pool
            .placements()
            .await?
            .into_iter()
            .find(|placement| placement.location.as_ref().ends_with(key.as_str()))
            .context("extent placement")?;
        if placement.member_id != member && moved.insert(placement.location.clone()) {
            pool.migrate(&placement.location, member).await?;
        }
    }
    ensure!(
        members_of(fs, pool, inode).await? == BTreeSet::from([member.to_owned()]),
        "file {inode} is not wholly on {member}"
    );
    Ok(())
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct Node {
    directory: bool,
    mode: u32,
    uid: u32,
    gid: u32,
    size: u64,
    sha256: [u8; 32],
}

/// Every name in the namespace with its type, mode, owner, size and the
/// SHA-256 of all of its bytes, read through the filesystem.
async fn namespace(fs: &ZeroFS) -> Result<BTreeMap<String, Node>> {
    let auth = AuthContext::default();
    let mut nodes = BTreeMap::new();
    let mut stack = vec![(0_u64, String::new())];
    while let Some((directory, prefix)) = stack.pop() {
        let mut after = 0;
        loop {
            let listing = fs.readdir(&auth, directory, after, 64).await?;
            for entry in &listing.entries {
                after = entry.cookie;
                if entry.name == b"." || entry.name == b".." {
                    continue;
                }
                let path = format!("{prefix}/{}", String::from_utf8_lossy(&entry.name));
                let inode = fs.inode_store.get(entry.fileid).await?;
                let (is_directory, size) = match &inode {
                    Inode::Directory(_) => (true, 0),
                    Inode::File(file) => (false, file.size),
                    other => anyhow::bail!("unexpected inode {other:?} at {path}"),
                };
                let mut hash = Sha256::new();
                let mut offset = 0;
                while !is_directory && offset < size {
                    let (bytes, _) = fs.read_file(&auth, entry.fileid, offset, 1 << 20).await?;
                    ensure!(!bytes.is_empty(), "short read of {path} at {offset}");
                    hash.update(&bytes);
                    offset += bytes.len() as u64;
                }
                nodes.insert(
                    path.clone(),
                    Node {
                        directory: is_directory,
                        mode: inode.mode(),
                        uid: inode.uid(),
                        gid: inode.gid(),
                        size,
                        sha256: hash.finalize().into(),
                    },
                );
                if is_directory {
                    stack.push((entry.fileid, path));
                }
            }
            if listing.end || listing.entries.is_empty() {
                break;
            }
        }
    }
    Ok(nodes)
}

struct Tree {
    movie: u64,
    notes: u64,
    top: u64,
}

/// Directories, an empty file and files of several sizes, modes and owners,
/// with current file data on both HDD members.
async fn write_tree(running: &Running) -> Result<Tree> {
    let fs = running.fs();
    let media = make_directory(fs, 0, "Media", 0o750).await?;
    make_directory(fs, 0, "Empty", 0o700).await?;
    let movie = write_file(
        fs,
        media,
        "movie.bin",
        (0o640, 1001, 1002),
        &payload(1, 1_500_000),
    )
    .await?;
    let notes = write_file(
        fs,
        media,
        "notes.txt",
        (0o600, 1001, 1002),
        &payload(2, 4096),
    )
    .await?;
    write_file(fs, media, "empty", (0o444, 0, 0), &Bytes::new()).await?;
    let top = write_file(fs, 0, "top.bin", (0o644, 0, 0), &payload(3, 300_000)).await?;
    place_on(fs, running.pool(), movie, "hdd-b").await?;
    place_on(fs, running.pool(), notes, "hdd-a").await?;
    place_on(fs, running.pool(), top, "hdd-a").await?;
    Ok(Tree { movie, notes, top })
}

fn failure(error: &anyhow::Error) -> Option<SurvivalFailure> {
    error.downcast_ref::<SurvivalFailure>().copied()
}

#[tokio::test(flavor = "multi_thread")]
async fn a_lost_p3_is_rebuilt_from_the_hdds_and_serves_every_file_byte_for_byte() -> Result<()> {
    let mut fixture = Fixture::new().await;
    let running = fixture.start().await?;
    write_tree(&running).await?;
    let expected = namespace(running.fs()).await?;
    ensure!(expected.len() == 6, "fixture namespace: {expected:?}");

    let started = Instant::now();
    let receipt = running.survival().commit_and_report().await?;
    let first_commit = started.elapsed();
    let status = running
        .pool()
        .status()
        .await?
        .metadata_survival
        .context("pool status carries the metadata copy")?;
    ensure!(status.last_sequence == Some(receipt.sequence));
    ensure!(status.commit_interval_seconds == 3600);
    for (replica, id) in status.replicas.iter().zip(HDDS) {
        ensure!(
            replica.id == id
                && replica.last_sequence == Some(receipt.sequence)
                && replica.generation_age_seconds.is_some_and(|age| age < 60)
                && replica.region_bytes.is_some_and(|bytes| bytes > 0)
                && replica.error.is_none(),
            "member status: {replica:?}"
        );
    }
    // A second commit with nothing new copies no authority object again.
    let started = Instant::now();
    let second = running.survival().commit_and_report().await?;
    let idle_commit = started.elapsed();
    ensure!(second.sequence == receipt.sequence + 1);
    let status = running.survival().status().await;
    let regions: Vec<u64> = status
        .replicas
        .iter()
        .filter_map(|replica| replica.region_bytes)
        .collect();
    println!(
        "checkpoint cost on file-backed fixture devices: first commit {first_commit:?}, \
         idle commit {idle_commit:?} with admission closed for {:?} ms, \
         region bytes per member {regions:?}",
        status.last_pause_millis
    );

    // Acknowledged after the last generation: this write is not on the HDDs'
    // metadata copy and is lost with the P3.
    write_file(
        running.fs(),
        0,
        "after-checkpoint.bin",
        (0o644, 0, 0),
        &payload(9, 70_000),
    )
    .await?;
    running.stop().await;

    fixture.replace_lost_p3();
    // Ordinary startup never recovers on its own, and it leaves the blank
    // device untouched.
    ensure!(
        fixture.start().await.is_err(),
        "startup must refuse a blank P3"
    );
    ensure!(
        fixture.p3_is_blank().await,
        "a refused startup wrote to the P3"
    );
    // Offline status needs only the HDDs.
    let offline = offline_status(&fixture.settings).await?;
    ensure!(offline.last_sequence == Some(second.sequence));
    ensure!(
        offline
            .replicas
            .iter()
            .all(|replica| replica.last_sequence == Some(second.sequence))
    );

    let report = fixture.rebuild(&[]).await?;
    ensure!(report.sequence == second.sequence);
    ensure!(report.unavailable_members.is_empty() && report.unreadable_files.is_empty());
    ensure!(report.writes_after_generation_not_recovered);
    // A second recovery refuses the rebuilt device.
    ensure!(fixture.rebuild(&[]).await.is_err());

    let running = fixture.start().await?;
    let recovered = namespace(running.fs()).await?;
    ensure!(
        recovered == expected,
        "recovered namespace differs:\n{recovered:#?}\nexpected:\n{expected:#?}"
    );
    let auth = AuthContext::default();
    let creds = Credentials::from_auth_context(&auth);
    ensure!(
        running
            .fs()
            .lookup(&creds, 0, b"after-checkpoint.bin")
            .await
            == Err(FsError::NotFound),
        "a write after the last generation must not reappear"
    );

    // The rebuilt pool keeps working: it accepts writes and extends the
    // same generation chain on both members.
    write_file(
        running.fs(),
        0,
        "after-recovery.bin",
        (0o600, 7, 7),
        &payload(4, 90_000),
    )
    .await?;
    let third = running.survival().commit_and_report().await?;
    ensure!(third.sequence == second.sequence + 1);
    let mut expected_after = expected.clone();
    expected_after.insert(
        "/after-recovery.bin".into(),
        Node {
            directory: false,
            mode: 0o600,
            uid: 7,
            gid: 7,
            size: 90_000,
            sha256: Sha256::digest(payload(4, 90_000)).into(),
        },
    );
    running.stop().await;
    let running = fixture.start().await?;
    ensure!(namespace(running.fs()).await? == expected_after);
    running.stop().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn a_torn_generation_falls_back_to_the_previous_one() -> Result<()> {
    let mut fixture = Fixture::new().await;
    let running = fixture.start().await?;
    write_tree(&running).await?;
    let expected = namespace(running.fs()).await?;
    let first = running.survival().commit_and_report().await?;

    write_file(
        running.fs(),
        0,
        "second-generation.bin",
        (0o644, 0, 0),
        &payload(5, 50_000),
    )
    .await?;
    // hdd-a receives every part of the next generation. hdd-b then fails, so
    // no member ever receives the commit object.
    running
        .survival()
        .inject_fault("hdd-b", Some(InjectedFault::Io));
    let error = running
        .survival()
        .commit_and_report()
        .await
        .expect_err("a generation that cannot reach every member is not acknowledged");
    ensure!(
        failure(&error) == Some(SurvivalFailure::Io),
        "unexpected failure: {error:#}"
    );
    let status = running.survival().status().await;
    ensure!(status.last_sequence == Some(first.sequence));
    ensure!(status.failed_replicas.contains_key("hdd-b"));
    running.stop().await;

    // hdd-a really holds parts of the torn generation, without its commit.
    let member = &fixture.settings.storage.pool.as_ref().unwrap().members[0];
    let opened = crate::storage_backend::open_raw_member(&fixture.settings, member).await?;
    let torn = format!(
        ".zerofs-survival/v1/dependencies/{}.json",
        first.sequence + 1
    );
    let committed = format!(".zerofs-survival/v1/commits/1/{}.json", first.sequence + 1);
    ensure!(opened.store.head(&torn.as_str().into()).await.is_ok());
    ensure!(opened.store.head(&committed.as_str().into()).await.is_err());
    drop(opened);

    fixture.replace_lost_p3();
    let report = fixture.rebuild(&[]).await?;
    ensure!(
        report.sequence == first.sequence,
        "recovered {}",
        report.sequence
    );
    let running = fixture.start().await?;
    ensure!(namespace(running.fs()).await? == expected);
    running.stop().await;
    Ok(())
}

/// Replace one stored part with different bytes of the same length.
async fn corrupt_one_part(fixture: &Fixture, member: usize) -> Result<()> {
    let config = &fixture.settings.storage.pool.as_ref().unwrap().members[member];
    let opened = crate::storage_backend::open_raw_member(&fixture.settings, config).await?;
    let parts: Vec<object_store::ObjectMeta> = opened
        .store
        .list(Some(&".zerofs-survival/v1/objects".into()))
        .try_collect()
        .await?;
    // The same part on every member: the largest, then the first by name.
    let part = parts
        .iter()
        .max_by_key(|meta| (meta.size, std::cmp::Reverse(meta.location.to_string())))
        .context("the member holds generation parts")?;
    opened
        .store
        .put(&part.location, vec![0xA5_u8; part.size as usize].into())
        .await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn a_corrupted_part_is_served_from_the_other_member_or_fails_closed() -> Result<()> {
    let mut fixture = Fixture::new().await;
    let running = fixture.start().await?;
    write_tree(&running).await?;
    let expected = namespace(running.fs()).await?;
    let receipt = running.survival().commit_and_report().await?;
    running.stop().await;

    // One damaged copy: the other member still verifies.
    corrupt_one_part(&fixture, 0).await?;
    fixture.replace_lost_p3();
    let report = fixture.rebuild(&[]).await?;
    ensure!(report.sequence == receipt.sequence);
    ensure!(
        report.selected_from.contains("hdd-b"),
        "selected {}",
        report.selected_from
    );
    let running = fixture.start().await?;
    ensure!(namespace(running.fs()).await? == expected);
    running.stop().await;

    // Both copies damaged: recovery refuses and writes nothing.
    corrupt_one_part(&fixture, 1).await?;
    fixture.replace_lost_p3();
    let error = fixture
        .rebuild(&[])
        .await
        .expect_err("no member holds a verifying copy");
    ensure!(
        failure(&error) == Some(SurvivalFailure::Corrupt),
        "unexpected failure: {error:#}"
    );
    ensure!(
        fixture.p3_is_blank().await,
        "a refused recovery wrote to the P3"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn recovery_with_one_hdd_missing_names_exactly_the_unreadable_files() -> Result<()> {
    let mut fixture = Fixture::new().await;
    let running = fixture.start().await?;
    let tree = write_tree(&running).await?;
    ensure!(
        members_of(running.fs(), running.pool(), tree.movie).await?
            == BTreeSet::from(["hdd-b".to_owned()])
    );
    ensure!(
        members_of(running.fs(), running.pool(), tree.top).await?
            == BTreeSet::from(["hdd-a".to_owned()])
    );
    ensure!(
        members_of(running.fs(), running.pool(), tree.notes).await?
            == BTreeSet::from(["hdd-a".to_owned()])
    );
    // The clean-shutdown generation is the one recovered here.
    running.survival().shutdown_commit().await;
    let sequence = running
        .survival()
        .status()
        .await
        .last_sequence
        .context("clean shutdown wrote a generation")?;
    running.stop().await;

    std::fs::remove_file(fixture.hdd_path("hdd-b")).unwrap();
    fixture.replace_lost_p3();

    // A missing member is never assumed away.
    let error = fixture
        .rebuild(&[])
        .await
        .expect_err("a missing member needs an explicit decision");
    ensure!(
        failure(&error) == Some(SurvivalFailure::Lost),
        "unexpected failure: {error:#}"
    );
    ensure!(fixture.p3_is_blank().await);

    let report = fixture.rebuild(&["hdd-b"]).await?;
    ensure!(report.sequence == sequence);
    ensure!(report.unavailable_members == ["hdd-b"]);
    ensure!(
        report.unreadable_files == ["/Media/movie.bin"],
        "reported {:?}",
        report.unreadable_files
    );
    ensure!(
        report
            .lost_ranges
            .iter()
            .all(|range| range.member.as_deref() == Some("hdd-b"))
    );
    let lost_bytes: u64 = report
        .lost_ranges
        .iter()
        .filter(|range| range.names == ["/Media/movie.bin"])
        .map(|range| u64::from(range.length))
        .sum();
    ensure!(
        lost_bytes == 1_500_000,
        "lost {lost_bytes} bytes of the movie"
    );
    Ok(())
}

async fn cleanup_debts(pool: &PoolObjectStore) -> Result<u64> {
    Ok(pool
        .status()
        .await?
        .members
        .iter()
        .map(|member| member.cleanup_debts)
        .sum())
}

#[tokio::test(flavor = "multi_thread")]
async fn payload_a_generation_maps_outlives_its_deletion_and_a_restart() -> Result<()> {
    let mut fixture = Fixture::new().await;
    let running = fixture.start().await?;
    let tree = write_tree(&running).await?;
    let expected = namespace(running.fs()).await?;
    let first = running.survival().commit_and_report().await?;

    // After the generation, the movie is unlinked and its segment reclaimed,
    // as the filesystem's segment collector does for a deleted file.
    let auth = AuthContext::default();
    let creds = Credentials::from_auth_context(&auth);
    let media = running.fs().lookup(&creds, 0, b"Media").await?;
    let extent = running
        .fs()
        .db
        .get_bytes(&KeyCodec::new().extent_key(tree.movie, 0))
        .await?
        .context("movie extent")?;
    let key = FrameLoc::decode(&extent)
        .context("frame location")?
        .segid
        .object_key();
    let segment = running
        .pool()
        .placements()
        .await?
        .into_iter()
        .find(|placement| placement.location.as_ref().ends_with(key.as_str()))
        .context("movie segment")?
        .location;
    running.fs().remove(&auth, media, b"movie.bin").await?;
    let _ = running.fs().administrative_remote_durability().await?;
    running.pool().delete(&segment).await?;
    ensure!(
        cleanup_debts(running.pool()).await? > 0,
        "the reclaimed segment must be held as a debt"
    );
    running.stop().await;

    // A restart loads the hold from the members before it cleans anything.
    let running = fixture.start().await?;
    running
        .pool()
        .cleanup_released_survival_garbage()
        .await
        .map_err(|error| anyhow::anyhow!("{error}"))?;
    ensure!(cleanup_debts(running.pool()).await? > 0);
    running.stop().await;

    fixture.replace_lost_p3();
    let report = fixture.rebuild(&[]).await?;
    ensure!(report.sequence == first.sequence);
    let running = fixture.start().await?;
    ensure!(
        namespace(running.fs()).await? == expected,
        "the generation's movie bytes were reclaimed"
    );

    // Once two later generations retire the first one, the bytes are freed.
    let media = running.fs().lookup(&creds, 0, b"Media").await?;
    running.fs().remove(&auth, media, b"movie.bin").await?;
    let _ = running.fs().administrative_remote_durability().await?;
    running.pool().delete(&segment).await?;
    ensure!(cleanup_debts(running.pool()).await? > 0);
    for _ in 0..2 {
        running.survival().commit_and_report().await?;
    }
    running
        .pool()
        .cleanup_released_survival_garbage()
        .await
        .map_err(|error| anyhow::anyhow!("{error}"))?;
    ensure!(
        cleanup_debts(running.pool()).await? == 0,
        "retired payload must be reclaimed"
    );
    running.stop().await;
    Ok(())
}

/// Segment data fills both members until WRITE answers ENOSPC. The metadata
/// headroom still takes the next generations, so mutations keep working, and
/// a deleted file's room takes writes again once its generations retire.
#[tokio::test(flavor = "multi_thread")]
async fn a_pool_full_of_data_still_commits_generations_and_keeps_mutations_open() -> Result<()> {
    let fixture = Fixture::new().await;
    let running = fixture.start().await?;
    let fs = running.fs();
    let auth = AuthContext::default();
    let creds = Credentials::from_auth_context(&auth);
    let attributes = SetAttributes {
        mode: SetMode::Set(0o644),
        ..Default::default()
    };
    let chunk = payload(9, 256 * 1024);
    let length = chunk.len() as u64;
    let (blocked, _) = fs.create(&creds, 0, b"blocked", &attributes).await?;
    let mut files = Vec::new();
    loop {
        ensure!(files.len() < 64, "WRITE never answered ENOSPC");
        let name = format!("fill-{}", files.len());
        let (inode, _) = fs.create(&creds, 0, name.as_bytes(), &attributes).await?;
        files.push(name);
        for index in 0..12 {
            match fs.write(&auth, inode, index * length, &chunk).await {
                Ok(_) => {}
                Err(FsError::NoSpace) => break,
                Err(error) => return Err(error.into()),
            }
        }
        let _ = fs.administrative_remote_durability().await?;
        // A WRITE claims an upper bound, and a flush returns what its segment
        // did not use. The pool is full once a WRITE is refused after one.
        if matches!(
            fs.write(&auth, blocked, 0, &chunk).await,
            Err(FsError::NoSpace)
        ) {
            break;
        }
    }

    // Generations land in the headroom that WRITE may not use.
    running.survival().commit_and_report().await?;
    make_directory(fs, 0, "after-full", 0o755)
        .await
        .context("mkdir on a full pool")?;
    ensure!(
        matches!(
            fs.write(&auth, blocked, 0, &chunk).await,
            Err(FsError::NoSpace)
        ),
        "a generation must not free room for WRITE"
    );
    for member in running.pool().status().await?.members {
        ensure!(
            member.space.metadata_headroom_used_bytes
                + member.space.metadata_headroom_remaining_bytes
                == HEADROOM
                && member.space.metadata_headroom_remaining_bytes > 0,
            "{} reports its headroom as {:?}",
            member.id,
            member.space
        );
    }

    // Reclaimed room refills the headroom before WRITE may use it, so free
    // more than the headroom on each member: 30 MiB of files.
    let mut reclaimed = BTreeSet::new();
    for name in &files[..10] {
        let inode = fs.lookup(&creds, 0, name.as_bytes()).await?;
        reclaimed.extend(
            placements_of(fs, running.pool(), inode)
                .await?
                .into_iter()
                .map(|placement| placement.location),
        );
        fs.remove(&auth, 0, name.as_bytes()).await?;
    }
    let _ = fs.administrative_remote_durability().await?;
    for segment in &reclaimed {
        running.pool().delete(segment).await?;
    }
    for _ in 0..2 {
        running.survival().commit_and_report().await?;
    }
    running
        .pool()
        .cleanup_released_survival_garbage()
        .await
        .map_err(|error| anyhow::anyhow!("{error}"))?;
    fs.write(&auth, blocked, 0, &chunk)
        .await
        .context("write after reclaim")?;
    let _ = fs
        .administrative_remote_durability()
        .await
        .context("flush after reclaim")?;
    running.stop().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn generations_reach_both_members_at_the_configured_cadence() -> Result<()> {
    // Writers wait out each cut instead of being told to retry.
    let fixture = Fixture::with_cadence(1, 30_000).await;
    let running = fixture.start().await?;
    write_tree(&running).await?;
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        let status = running.pool().status().await?.metadata_survival;
        if let Some(status) = status
            && status.last_sequence.is_some_and(|sequence| sequence >= 2)
            && status
                .replicas
                .iter()
                .all(|replica| replica.last_sequence == status.last_sequence)
        {
            break;
        }
        ensure!(
            Instant::now() < deadline,
            "no generation reached both members on the one-second cadence"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    running.stop().await;
    Ok(())
}

/// Settings for a one-member raw pool with this survival table.
fn raw_survival_settings(root: &std::path::Path, survival: serde_json::Value) -> Result<Settings> {
    let pool = Uuid::new_v4();
    let url = |scheme: &str, name: &str, extra: &str| {
        format!(
            "{scheme}://{}/{name}?pool={pool}&member={}{extra}",
            root.display(),
            Uuid::new_v4()
        )
    };
    let document = serde_json::json!({
        "cache": {"dir": root.join("cache"), "disk_size_gb": 0.0},
        "storage": {
            "url": url("raw+p3-testfile", "p3", "&layout_mib=64,16,48,4,4,8"),
            "encryption_password": PASSWORD,
            "pool": {
                "members": [{
                    "id": "hdd-a",
                    "url": url("raw+testfile", "a", ""),
                    "capacity_gb": 0.064,
                    "reserve_free_gb": 0.001,
                }],
                "metadata_survival": survival,
            },
        },
        "servers": {},
        "telemetry": {"enabled": false},
    });
    let config = root.join("zerofs.toml");
    std::fs::write(&config, toml::to_string(&document).unwrap()).unwrap();
    Settings::from_file(config)
}

#[test]
fn a_raw_pool_refuses_separately_listed_replicas() {
    let root = tempfile::tempdir().unwrap();
    // Every member of a raw pool holds the generations, so the set is not
    // configurable.
    raw_survival_settings(
        root.path(),
        serde_json::json!({"format_version": 1, "headroom_bytes": HEADROOM}),
    )
    .unwrap();
    assert!(
        raw_survival_settings(
            root.path(),
            serde_json::json!({
                "format_version": 1,
                "headroom_bytes": HEADROOM,
                "replicas": [{"id": "extra", "device_id": "x", "dir": "/var/lib/extra"}],
            })
        )
        .is_err()
    );
}

/// The headroom and the member's reserve must fit in its capacity. The
/// default 4 GiB does not fit a 64 MB member.
#[test]
fn a_metadata_headroom_that_does_not_fit_a_member_is_refused() {
    let root = tempfile::tempdir().unwrap();
    let load = |survival: serde_json::Value| raw_survival_settings(root.path(), survival);
    assert!(load(serde_json::json!({"format_version": 1})).is_err());
    // 0.064 GB of capacity less the 0.001 GB reserve.
    let room: u64 = 63_000_000;
    load(serde_json::json!({"format_version": 1, "headroom_bytes": room - 1})).unwrap();
    for headroom in [room, i64::MAX as u64] {
        assert!(
            load(serde_json::json!({"format_version": 1, "headroom_bytes": headroom})).is_err(),
            "{headroom} bytes of headroom were accepted"
        );
    }
}
