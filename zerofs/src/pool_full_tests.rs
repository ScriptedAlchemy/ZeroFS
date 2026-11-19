//! A full pool member must not hang the filesystem.
//!
//! Members are real local directories. Their real free space comes from an
//! injected source with exact numbers. A full member is armed with
//! `fault_store::native_errno`, so its writes fail with a native ENOSPC in
//! the shape `LocalFileSystem` gives them, and a `FaultStore` above it counts
//! every write attempt. The filesystem, the retry layer and the pool are the
//! production types. Every wait is bounded, so a regression fails instead of
//! hanging. Times printed as `FULL_MEMBER_TIMING` are test-harness numbers on
//! a tmp directory, not device performance.

use crate::block_transformer::ZeroFsBlockTransformer;
use crate::config::{CompressionConfig, StoreProfile};
use crate::db::SlateDbHandle;
use crate::fault_store::FaultControls;
use crate::fault_store::native_errno::{self, Armed, Operation};
use crate::frame_codec::FrameCodec;
use crate::fs::ZeroFS;
use crate::fs::errors::FsError;
use crate::fs::permissions::Credentials;
use crate::fs::types::{AuthContext, SetAttributes};
use crate::pool_object_store::{FilesystemSpace, FreeSpace, PoolMember, PoolObjectStore};
use bytes::Bytes;
use object_store::ObjectStore;
use object_store::path::Path;
use slatedb::{BlockTransformer, DbBuilder};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

const MIB: u64 = 1024 * 1024;
const GIB: u64 = 1024 * MIB;
/// `capacity_gb` values from the field report, in decimal gigabytes.
const DECLARED_A: u64 = 6_001_000_000_000;
const DECLARED_B: u64 = 18_000_000_000_000;
const RESERVE: u64 = MIB;
/// The promise under test: a write or fsync on a full pool answers within
/// this bound. Measured answers are milliseconds; the bound leaves room for a
/// loaded CI host.
const BOUND: Duration = Duration::from_secs(5);
/// Every test body runs under this, so a hang fails the test.
const TEST_DEADLINE: Duration = Duration::from_secs(120);

/// Real free space with exact numbers, standing in for `statvfs`.
#[derive(Debug)]
struct InjectedSpace {
    filesystem: u64,
    available: AtomicU64,
    total: u64,
}

impl FreeSpace for InjectedSpace {
    fn observe(&self) -> std::io::Result<FilesystemSpace> {
        Ok(FilesystemSpace {
            filesystem: self.filesystem,
            committed: 0,
            available: self.available.load(Ordering::SeqCst),
            total: self.total,
        })
    }
}

struct Member {
    /// The filesystem path the member's objects live under.
    root: std::path::PathBuf,
    writes: Arc<FaultControls>,
    no_space: std::sync::Mutex<Option<Armed>>,
    space: Option<Arc<InjectedSpace>>,
}

impl Member {
    /// Every write to the member now fails with a native ENOSPC.
    fn fill(&self) {
        *self.no_space.lock().unwrap() = Some(native_errno::arm(
            self.root.clone(),
            Operation::Write,
            "",
            libc::ENOSPC,
            None,
        ));
    }

    fn make_room(&self) {
        self.no_space.lock().unwrap().take();
    }

    /// Writes the pool sent to this member, successful or not, including
    /// the identity marker the pool writes when it initializes the member.
    fn attempts(&self) -> usize {
        self.writes.put_count()
    }

    fn set_available(&self, bytes: u64) {
        self.space
            .as_ref()
            .expect("local member")
            .available
            .store(bytes, Ordering::SeqCst);
    }
}

#[derive(Clone, Copy)]
enum Kind {
    /// A directory on a filesystem with this much free and total space.
    Local { available: u64, total: u64 },
    /// An object store with no filesystem to ask.
    Remote,
}

struct Fixture {
    _root: tempfile::TempDir,
    fs: Arc<ZeroFS>,
    pool: PoolObjectStore,
    a: Member,
    b: Member,
    profile: StoreProfile,
}

fn member(
    root: &std::path::Path,
    id: &str,
    declared: u64,
    kind: Kind,
    filesystem: u64,
) -> (PoolMember, Member) {
    let (inner, prefix, local_path, space): (Box<dyn ObjectStore>, _, _, _) = match kind {
        Kind::Local { available, total } => {
            let directory = root.join(id);
            std::fs::create_dir(&directory).unwrap();
            let directory = directory.canonicalize().unwrap();
            let space = Arc::new(InjectedSpace {
                filesystem,
                available: AtomicU64::new(available),
                total,
            });
            (
                Box::new(crate::local_object_store::LocalConditionalStore::new()),
                Path::from_filesystem_path(&directory).unwrap(),
                Some(directory),
                Some(space),
            )
        }
        Kind::Remote => (
            Box::new(object_store::memory::InMemory::new()),
            Path::from(format!("{}/remote/{id}", root.display())),
            None,
            None,
        ),
    };
    let (counted, writes) =
        crate::fault_store::FaultStore::new(Arc::from(native_errno::wrap(inner)));
    let store: Arc<dyn ObjectStore> = Arc::new(object_store::prefix::PrefixStore::new(
        counted,
        prefix.clone(),
    ));
    let mut config = PoolMember {
        id: id.into(),
        identity: format!("file:///{id}"),
        store,
        capacity_bytes: declared,
        local_path,
        min_free_bytes: RESERVE,
        health_device: None,
        free_space: None,
    };
    if let Some(space) = &space {
        config = config.with_free_space(space.clone());
    }
    let member = Member {
        root: format!("/{prefix}").into(),
        writes,
        no_space: std::sync::Mutex::new(None),
        space,
    };
    (config, member)
}

/// A small seal threshold and two residency permits make the retained-segment
/// bound reachable with a few MiB.
fn small_segments() -> StoreProfile {
    StoreProfile {
        seal_threshold: Some(4 * MIB as usize),
        max_inflight_seals: Some(2),
        ..Default::default()
    }
}

async fn fixture(a: (u64, Kind), b: (u64, Kind), profile: StoreProfile) -> Fixture {
    let root = tempfile::tempdir().unwrap();
    std::fs::create_dir(root.path().join("authority")).unwrap();
    let authority: Arc<dyn ObjectStore> = Arc::new(object_store::prefix::PrefixStore::new(
        crate::local_object_store::LocalConditionalStore::new(),
        Path::from_filesystem_path(root.path().join("authority").canonicalize().unwrap()).unwrap(),
    ));
    let (config_a, a) = member(root.path(), "member-a", a.0, a.1, 1);
    let (config_b, b) = member(root.path(), "member-b", b.0, b.1, 2);
    let pool = PoolObjectStore::initialize(
        authority,
        Path::from("db"),
        root.path().join("catalog"),
        vec![config_a, config_b],
    )
    .await
    .unwrap();
    // The production stack: the retry layer sits above the pool.
    let store: Arc<dyn ObjectStore> = Arc::new(
        crate::retrying_object_store::RetryingObjectStore::new(Arc::new(pool.clone())),
    );
    let key = [0x52; 32];
    let transformer: Arc<dyn BlockTransformer> =
        ZeroFsBlockTransformer::new_arc(&key, CompressionConfig::default());
    let db = Arc::new(
        DbBuilder::new(Path::from("db"), Arc::clone(&store))
            .with_block_transformer(transformer)
            .with_filter_policies(crate::fs::filter_policy::filter_policies())
            .with_segment_extractor(Arc::new(crate::segment_extractor::ZeroFsSegmentExtractor))
            .build()
            .await
            .unwrap(),
    );
    let segment_store: Arc<dyn ObjectStore> = Arc::new(object_store::prefix::PrefixStore::new(
        store,
        Path::from("db"),
    ));
    let fs = ZeroFS::new_with_slatedb_and_lease(
        SlateDbHandle::ReadWrite(db),
        u64::MAX,
        None,
        false,
        false,
        None,
        None,
        Arc::new(crate::dedup::DedupCache::new()),
        None,
        crate::object_trace::ObjectTracer::new(),
        segment_store,
        FrameCodec::new(
            &key,
            crate::segment::SEGMENT_INFO,
            CompressionConfig::default(),
        ),
        None,
        profile,
        crate::fs::name_mode::NameMode::Sensitive,
    )
    .await
    .unwrap();
    let fs = Arc::new(fs);
    fs.install_volatile_overlay();
    fs.start_materializer();
    Fixture {
        _root: root,
        fs,
        pool,
        a,
        b,
        profile,
    }
}

fn local(available: u64) -> Kind {
    Kind::Local {
        available,
        total: 64 * GIB,
    }
}

fn plenty() -> Kind {
    local(64 * GIB)
}

fn root_credentials() -> Credentials {
    Credentials {
        uid: 0,
        gid: 0,
        gid_known: true,
        groups: [0; 16],
        groups_count: 1,
        groups_complete: true,
        groups_truncated: false,
    }
}

fn auth() -> AuthContext {
    AuthContext {
        uid: 0,
        gid: 0,
        ..Default::default()
    }
}

/// Incompressible bytes, so segment sizes follow the payload size.
fn payload(seed: u64, length: u64) -> Bytes {
    let mut state = seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1;
    (0..length)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state as u8
        })
        .collect::<Vec<_>>()
        .into()
}

fn timing(test: &str, step: &str, elapsed: Duration) {
    println!(
        "FULL_MEMBER_TIMING {}",
        serde_json::json!({
            "test": test,
            "step": step,
            "millis": elapsed.as_secs_f64() * 1000.0,
            "kind": "test harness on a tmp directory, not device performance",
        })
    );
}

/// Run one filesystem step that must answer within [`BOUND`].
async fn bounded<T>(test: &str, step: &str, work: impl std::future::Future<Output = T>) -> T {
    let started = Instant::now();
    let result = tokio::time::timeout(BOUND, work)
        .await
        .unwrap_or_else(|_| panic!("{test}: {step} did not answer within {BOUND:?}"));
    timing(test, step, started.elapsed());
    result
}

async fn create(fs: &ZeroFS, name: &str) -> u64 {
    fs.create(
        &root_credentials(),
        0,
        name.as_bytes(),
        &SetAttributes::default(),
    )
    .await
    .unwrap()
    .0
}

/// Write in 1 MiB requests, as the SMB client does. Returns the first error.
async fn write(test: &str, fs: &ZeroFS, inode: u64, data: &Bytes) -> Result<(), FsError> {
    let mut offset = 0;
    while offset < data.len() {
        let end = (offset + MIB as usize).min(data.len());
        bounded(
            test,
            "write 1 MiB",
            fs.write(&auth(), inode, offset as u64, &data.slice(offset..end)),
        )
        .await?;
        offset = end;
    }
    Ok(())
}

async fn fsync(test: &str, fs: &ZeroFS, inode: u64) -> Result<(), FsError> {
    bounded(test, "fsync", fs.wait_inode_durability(inode)).await
}

async fn read(fs: &ZeroFS, inode: u64, length: u64) -> Bytes {
    let mut out = Vec::with_capacity(length as usize);
    while (out.len() as u64) < length {
        let (chunk, eof) = fs
            .read_file(&auth(), inode, out.len() as u64, MIB as u32)
            .await
            .unwrap();
        out.extend_from_slice(&chunk);
        if eof {
            break;
        }
    }
    out.into()
}

async fn status(pool: &PoolObjectStore) -> serde_json::Value {
    serde_json::to_value(pool.status().await.unwrap()).unwrap()
}

async fn members_holding_segments(pool: &PoolObjectStore) -> Vec<String> {
    let mut members: Vec<_> = pool
        .placements()
        .await
        .unwrap()
        .into_iter()
        .map(|placement| placement.member_id)
        .collect();
    members.sort();
    members.dedup();
    members
}

async fn deadline(test: impl std::future::Future<Output = ()>) {
    tokio::time::timeout(TEST_DEADLINE, test)
        .await
        .expect("full-member test exceeded its deadline: something waited without limit");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_member_whose_write_returns_enospc_is_skipped_within_the_same_flush() {
    deadline(async {
        let test = "spill";
        // member-a's filesystem claims room, but its writes return ENOSPC:
        // the race between the free-space check and the write.
        let f = fixture(
            (DECLARED_A, plenty()),
            (DECLARED_B, plenty()),
            StoreProfile::default(),
        )
        .await;
        let attempts = f.a.attempts();
        f.a.fill();
        let inode = create(&f.fs, "spill.bin").await;
        let data = payload(1, 2 * MIB);
        write(test, &f.fs, inode, &data).await.unwrap();
        fsync(test, &f.fs, inode).await.unwrap();
        assert!(f.a.attempts() > attempts, "member-a was never tried");
        assert_eq!(members_holding_segments(&f.pool).await, ["member-b"]);
        let report = status(&f.pool).await;
        let a = &report["members"][0];
        assert_eq!(a["space"]["full"]["reason"], "write_returned_no_space");
        assert!(
            a["health"]["quarantine"].is_null(),
            "full is not a media fault"
        );
        assert_eq!(a["available_bytes"], 0);
        assert_eq!(read(&f.fs, inode, data.len() as u64).await, data);

        // Later placements skip the full member without touching it.
        let attempts = f.a.attempts();
        let second = create(&f.fs, "second.bin").await;
        write(test, &f.fs, second, &payload(2, MIB)).await.unwrap();
        fsync(test, &f.fs, second).await.unwrap();
        assert_eq!(f.a.attempts(), attempts);
    })
    .await;
}

/// What [`run_out_of_room`] left behind.
struct Filled {
    /// A file written while the pool was full, and the bytes it should hold.
    after: u64,
    after_data: Bytes,
    /// Member write attempts when the pool filled.
    attempts: (usize, usize),
}

/// Fill the pool so no member has room, then write and fsync again and again.
async fn run_out_of_room(test: &str, f: &Fixture, how: Fill) -> Filled {
    let before = create(&f.fs, "before.bin").await;
    let before_data = payload(10, 3 * MIB);
    write(test, &f.fs, before, &before_data).await.unwrap();
    fsync(test, &f.fs, before).await.unwrap();

    match how {
        Fill::NativeEnospc => {
            f.a.fill();
            f.b.fill();
        }
        Fill::BelowReserve => {
            f.a.set_available(RESERVE / 2);
            f.b.set_available(RESERVE / 2);
        }
    }
    let attempts = (f.a.attempts(), f.b.attempts());
    let after = create(&f.fs, "after.bin").await;
    let after_data = payload(11, 32 * MIB);
    match write(test, &f.fs, after, &after_data).await {
        Ok(()) | Err(FsError::NoSpace) => {}
        Err(other) => panic!("{test}: a write on a full pool returned {other:?}, not ENOSPC"),
    }
    let max_inflight = f.profile.max_inflight_seals.unwrap_or(1);
    // More fsyncs than residency permits: none may turn into EIO.
    for attempt in 0..max_inflight + 4 {
        assert_eq!(
            fsync(test, &f.fs, after).await,
            Err(FsError::NoSpace),
            "{test}: fsync {attempt} on a full pool"
        );
    }
    // Accepted but undurable bytes stay bounded while the pool is full.
    let threshold = f.profile.seal_threshold.unwrap() as u64;
    let (generations, bytes) = f.fs.extent_store.retained_seals();
    assert!(
        generations <= max_inflight,
        "{test}: {generations} retained generations exceed {max_inflight} residency permits"
    );
    assert!(
        bytes as u64 <= generations as u64 * (threshold + 2 * MIB),
        "{test}: {bytes} retained bytes in {generations} generations"
    );
    assert_eq!(
        read(&f.fs, before, before_data.len() as u64).await,
        before_data
    );
    Filled {
        after,
        after_data,
        attempts,
    }
}

#[derive(Clone, Copy)]
enum Fill {
    /// Writes return ENOSPC while the filesystem still claims room.
    NativeEnospc,
    /// The filesystem reports free space below the member's reserve, as an
    /// 8 MiB dataset behind a 6,001 GB declaration does.
    BelowReserve,
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn when_writes_return_enospc_everywhere_fsync_reports_enospc_within_the_bound() {
    deadline(async {
        let test = "all-enospc";
        let f = fixture(
            (DECLARED_A, plenty()),
            (DECLARED_B, plenty()),
            small_segments(),
        )
        .await;
        run_out_of_room(test, &f, Fill::NativeEnospc).await;
        let report = status(&f.pool).await;
        for member in report["members"].as_array().unwrap() {
            assert_eq!(member["space"]["full"]["reason"], "write_returned_no_space");
            assert!(member["health"]["quarantine"].is_null());
        }
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn when_real_free_space_is_below_every_reserve_fsync_reports_enospc_within_the_bound() {
    deadline(async {
        let test = "all-below-reserve";
        let f = fixture(
            (DECLARED_A, plenty()),
            (DECLARED_B, plenty()),
            small_segments(),
        )
        .await;
        let filled = run_out_of_room(test, &f, Fill::BelowReserve).await;
        // Once the pool filled, placement refused both members without
        // sending either another write.
        assert_eq!((f.a.attempts(), f.b.attempts()), filled.attempts);
        let report = status(&f.pool).await;
        for member in report["members"].as_array().unwrap() {
            assert_eq!(
                member["space"]["full"]["reason"],
                "below_free_space_reserve"
            );
            assert_eq!(member["space"]["filesystem_free_bytes"], RESERVE / 2);
            assert_eq!(member["available_bytes"], 0);
        }
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn writes_resume_without_a_restart_when_space_returns() {
    deadline(async {
        let test = "space-returns";
        let f = fixture(
            (DECLARED_A, plenty()),
            (DECLARED_B, plenty()),
            small_segments(),
        )
        .await;
        let Filled {
            after, after_data, ..
        } = run_out_of_room(test, &f, Fill::NativeEnospc).await;
        // Space returns: the filesystem reports more free bytes than when the
        // writes failed, and writes succeed again.
        f.a.make_room();
        f.b.make_room();
        f.a.set_available(65 * GIB);
        f.b.set_available(65 * GIB);
        write(test, &f.fs, after, &after_data).await.unwrap();
        fsync(test, &f.fs, after).await.unwrap();
        assert_eq!(
            read(&f.fs, after, after_data.len() as u64).await,
            after_data
        );
        assert_eq!(f.fs.extent_store.retained_seals(), (0, 0));
        let report = status(&f.pool).await;
        for member in report["members"].as_array().unwrap() {
            assert!(member["space"]["full"].is_null(), "{member}");
        }
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn placement_and_status_use_the_real_free_space_not_the_declared_capacity() {
    deadline(async {
        let test = "real-free-space";
        // 6,001 GB declared, 100 KiB above the reserve really free.
        let f = fixture(
            (DECLARED_A, local(RESERVE + 100 * 1024)),
            (DECLARED_B, plenty()),
            StoreProfile::default(),
        )
        .await;
        let report = status(&f.pool).await;
        let a = &report["members"][0];
        assert_eq!(a["capacity_bytes"], DECLARED_A);
        assert_eq!(a["available_bytes"], 100 * 1024);
        assert_eq!(a["space"]["filesystem_free_bytes"], RESERVE + 100 * 1024);
        assert!(a["space"]["full"].is_null());

        let attempts = f.a.attempts();
        let inode = create(&f.fs, "placed.bin").await;
        let data = payload(3, 2 * MIB);
        write(test, &f.fs, inode, &data).await.unwrap();
        fsync(test, &f.fs, inode).await.unwrap();
        assert_eq!(members_holding_segments(&f.pool).await, ["member-b"]);
        assert_eq!(
            f.a.attempts(),
            attempts,
            "placement must not try a member without room"
        );

        f.a.set_available(RESERVE - 1);
        let report = status(&f.pool).await;
        let a = &report["members"][0];
        assert_eq!(a["space"]["full"]["reason"], "below_free_space_reserve");
        assert_eq!(a["available_bytes"], 0);
    })
    .await;
}

#[tokio::test]
async fn a_declared_capacity_beyond_the_filesystem_warns_at_startup_and_in_status() {
    deadline(async {
        let small = Kind::Local {
            available: 6 * MIB,
            total: 8 * MIB,
        };
        let f = fixture(
            (DECLARED_A, small),
            (DECLARED_B, small),
            StoreProfile::default(),
        )
        .await;
        // The exact lines the server logs at WARN when the pool opens.
        let warnings = f.pool.declared_beyond_filesystem();
        assert_eq!(warnings.len(), 2, "{warnings:?}");
        for ((id, declared), line) in [("member-a", DECLARED_A), ("member-b", DECLARED_B)]
            .into_iter()
            .zip(&warnings)
        {
            assert!(
                line.contains(&format!("pool member {id} declares")),
                "{line}"
            );
            assert!(line.contains(&declared.to_string()), "{line}");
            assert!(line.contains(&(8 * MIB).to_string()), "{line}");
        }
        let report = status(&f.pool).await;
        for member in report["members"].as_array().unwrap() {
            assert_eq!(member["space"]["declared_exceeds_filesystem"], true);
            assert_eq!(member["space"]["filesystem_total_bytes"], 8 * MIB);
        }
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_remote_member_without_a_free_space_source_keeps_working() {
    deadline(async {
        let test = "remote";
        // member-a is an object store declared at 1 MiB; it has no statvfs.
        let f = fixture(
            (MIB, Kind::Remote),
            (DECLARED_B, plenty()),
            StoreProfile::default(),
        )
        .await;
        let report = status(&f.pool).await;
        let a = &report["members"][0];
        assert_eq!(a["available_bytes"], MIB);
        assert!(a["space"]["filesystem_free_bytes"].is_null());
        assert!(a["space"]["filesystem_total_bytes"].is_null());
        assert_eq!(a["space"]["declared_exceeds_filesystem"], false);
        assert!(a["space"]["full"].is_null());

        // Larger than its declared capacity: lands on member-b.
        let large = create(&f.fs, "large.bin").await;
        write(test, &f.fs, large, &payload(4, 2 * MIB))
            .await
            .unwrap();
        fsync(test, &f.fs, large).await.unwrap();
        assert_eq!(members_holding_segments(&f.pool).await, ["member-b"]);

        // Small enough: lands on member-a.
        let small = create(&f.fs, "small.bin").await;
        write(test, &f.fs, small, &payload(5, 64 * 1024))
            .await
            .unwrap();
        fsync(test, &f.fs, small).await.unwrap();
        assert_eq!(
            members_holding_segments(&f.pool).await,
            ["member-a", "member-b"]
        );

        // A real no-space answer from the remote store spills too.
        let attempts = f.a.attempts();
        f.a.fill();
        let spilled = create(&f.fs, "spilled.bin").await;
        let data = payload(6, 64 * 1024);
        write(test, &f.fs, spilled, &data).await.unwrap();
        fsync(test, &f.fs, spilled).await.unwrap();
        assert!(f.a.attempts() > attempts, "member-a was never tried");
        assert_eq!(read(&f.fs, spilled, data.len() as u64).await, data);
        let report = status(&f.pool).await;
        assert_eq!(
            report["members"][0]["space"]["full"]["reason"],
            "write_returned_no_space"
        );
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ninep_fsync_on_a_full_pool_returns_enospc_not_eio() {
    use ninep_client::{ClientError, NOFID, NinePClient};
    deadline(async {
        let test = "ninep";
        let f = fixture(
            (DECLARED_A, plenty()),
            (DECLARED_B, plenty()),
            StoreProfile::default(),
        )
        .await;
        let socket = f._root.path().join("9p.sock");
        let server = crate::ninep::NinePServer::new_unix(Arc::clone(&f.fs), socket.clone());
        let shutdown = tokio_util::sync::CancellationToken::new();
        let server_shutdown = shutdown.clone();
        tokio::spawn(async move {
            let _ = server.start(server_shutdown).await;
        });
        let mut client = None;
        for _ in 0..100 {
            if let Ok(connected) = NinePClient::connect_unix(&socket, 10240 * 1024).await {
                client = Some(connected);
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        let client = client.expect("9P server did not start");
        let attach = client.alloc_fid();
        client.attach(attach, NOFID, "", "", 0).await.unwrap();
        let open = |name: &'static str| {
            let client = Arc::clone(&client);
            async move {
                let fid = client.alloc_fid();
                client.walk(attach, fid, &[]).await.unwrap();
                client
                    .lcreate(fid, name.as_bytes(), 2, 0o644, 0)
                    .await
                    .unwrap();
                fid
            }
        };
        let before = open("before.bin").await;
        let before_data = payload(7, 2 * MIB);
        bounded(
            test,
            "9P write 2 MiB",
            client.write_bytes(before, 0, before_data.clone()),
        )
        .await
        .unwrap();
        bounded(test, "9P fsync", client.fsync(before, 0))
            .await
            .unwrap();

        // The field report: every member's filesystem is below its reserve.
        f.a.set_available(RESERVE / 2);
        f.b.set_available(RESERVE / 2);
        let after = open("after.bin").await;
        for chunk in 0..32 {
            let data = payload(8 + chunk, MIB);
            bounded(
                test,
                "9P write 1 MiB",
                client.write_bytes(after, chunk * MIB, data),
            )
            .await
            .unwrap();
        }
        for _ in 0..3 {
            match bounded(test, "9P fsync on a full pool", client.fsync(after, 0)).await {
                Err(ClientError::Errno(errno)) => {
                    assert_eq!(errno, libc::ENOSPC as u32, "full must not look broken")
                }
                other => panic!("9P fsync on a full pool returned {other:?}"),
            }
        }
        let read = client.alloc_fid();
        client.walk(attach, read, &[b"before.bin"]).await.unwrap();
        client.lopen(read, 0).await.unwrap();
        assert_eq!(
            client.read(read, 0, 2 * MIB as u32).await.unwrap(),
            before_data
        );
        shutdown.cancel();
    })
    .await;
}
