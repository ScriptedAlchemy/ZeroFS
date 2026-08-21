use super::super::test_util::*;
use super::*;
use crate::config::CompressionConfig;
use slatedb::config::{PutOptions, WriteOptions};

#[tokio::test]
async fn contiguous_multiextent_read_is_one_ranged_get() {
    let (writer, db, object_store) = make_with_compression(CompressionConfig::Lz4).await;
    let mut model = Vec::new();
    // Four extents in ONE write -> one segment, frames contiguous.
    write_and_check(&writer, &db, &mut model, 0, &vec![1u8; 4 * EXTENT_SIZE]).await;
    writer.seal_open().await.unwrap();
    let store = make_store(object_store, db, CompressionConfig::Lz4, 8);

    let before = store.segments.read_calls();
    let got = store.read(1, 0, 4 * EXTENT_SIZE as u64).await.unwrap();
    let gets = store.segments.read_calls() - before;

    assert_eq!(got.len(), 4 * EXTENT_SIZE);
    assert_eq!(got.as_ref(), model.as_slice());
    assert_eq!(
        gets, 1,
        "a contiguous 4-extent read must coalesce into one ranged GET"
    );
}

#[tokio::test]
async fn repeated_contiguous_read_reuses_decoded_extents() {
    let (writer, db, object_store) = make_with_compression(CompressionConfig::Lz4).await;
    for extent in 0..4u64 {
        write_extent(&writer, &db, extent, &[extent as u8 + 1; EXTENT_SIZE]).await;
    }
    writer.seal_open().await.unwrap();
    let store = make_store(object_store, db.clone(), CompressionConfig::Lz4, 8);

    let before = store.segments.read_calls();
    let scans_before = db.scan_call_count();
    let first = store.read(1, 0, 4 * EXTENT_SIZE as u64).await.unwrap();
    let after_first = store.segments.read_calls();
    let scans_after_first = db.scan_call_count();
    let second = store.read(1, 0, 4 * EXTENT_SIZE as u64).await.unwrap();
    let after_second = store.segments.read_calls();
    let scans_after_second = db.scan_call_count();

    assert_eq!(first, second);
    assert_eq!(after_first - before, 1, "the first read fetches the run");
    assert_eq!(
        after_second - after_first,
        0,
        "validated plaintext should be reused without another segment fetch"
    );
    assert_eq!(
        scans_after_first - scans_before,
        1,
        "the first read resolves the logical extent map once"
    );
    assert_eq!(
        scans_after_second - scans_after_first,
        0,
        "a repeated read must reuse committed FrameLocs without another metadata scan"
    );
}

#[tokio::test]
async fn committed_write_is_read_ready_after_seal() {
    let (store, db) = make().await;
    let expected = Bytes::from(vec![0x5a; EXTENT_SIZE]);
    write_extent(&store, &db, 0, &expected).await;
    store.seal_open().await.unwrap();

    let before = store.segments.read_calls();
    let actual = store.read(1, 0, EXTENT_SIZE as u64).await.unwrap();

    assert_eq!(actual, expected);
    assert_eq!(
        store.segments.read_calls() - before,
        0,
        "freshly written plaintext should already be in the clean extent cache"
    );
}

#[tokio::test]
async fn committed_overwrite_and_delete_refresh_the_logical_extent_map() {
    let (store, db) = make().await;
    write_extent(&store, &db, 0, &[0x11; EXTENT_SIZE]).await;
    write_extent(&store, &db, 1, &[0x22; EXTENT_SIZE]).await;

    let mut overwrite = db.new_transaction().unwrap();
    store
        .write(
            &mut overwrite,
            1,
            0,
            &Bytes::from(vec![0x33; EXTENT_SIZE]),
            2 * EXTENT_SIZE as u64,
        )
        .await
        .unwrap();
    commit(&store, overwrite).await;

    let before_overwrite_read = db.scan_call_count();
    let got = store.read(1, 0, 2 * EXTENT_SIZE as u64).await.unwrap();
    assert_eq!(&got[..EXTENT_SIZE], &[0x33; EXTENT_SIZE]);
    assert_eq!(&got[EXTENT_SIZE..], &[0x22; EXTENT_SIZE]);
    assert_eq!(db.scan_call_count(), before_overwrite_read);

    let mut delete = db.new_transaction().unwrap();
    store.delete_range(&mut delete, 1, 1, 2).await.unwrap();
    commit(&store, delete).await;

    let before_delete_read = db.scan_call_count();
    let got = store.read(1, 0, 2 * EXTENT_SIZE as u64).await.unwrap();
    assert_eq!(&got[..EXTENT_SIZE], &[0x33; EXTENT_SIZE]);
    assert_eq!(&got[EXTENT_SIZE..], ZERO_EXTENT);
    assert_eq!(db.scan_call_count(), before_delete_read);
}

#[tokio::test]
async fn failed_commit_never_publishes_its_staged_frameloc() {
    let (store, db) = make().await;
    write_extent(&store, &db, 0, &[0x11; EXTENT_SIZE]).await;
    write_extent(&store, &db, 1, &[0x22; EXTENT_SIZE]).await;
    let old_loc = frameloc_of(&store, &db, 1, 0).await.unwrap();
    let segcount = store
        .key_codec
        .segcount_key(old_loc.segid.epoch, old_loc.segid.counter);
    db.put_with_options(
        &segcount,
        b"bogus",
        &PutOptions::default(),
        &WriteOptions::default(),
    )
    .await
    .unwrap();

    let mut failed = db.new_transaction().unwrap();
    store
        .write(
            &mut failed,
            1,
            0,
            &Bytes::from(vec![0x99; EXTENT_SIZE]),
            2 * EXTENT_SIZE as u64,
        )
        .await
        .unwrap();
    assert!(store.commit_via_coordinator(failed).await.is_err());

    let before = db.scan_call_count();
    let got = store.read(1, 0, 2 * EXTENT_SIZE as u64).await.unwrap();
    assert_eq!(&got[..EXTENT_SIZE], &[0x11; EXTENT_SIZE]);
    assert_eq!(&got[EXTENT_SIZE..], &[0x22; EXTENT_SIZE]);
    assert_eq!(db.scan_call_count(), before);
}

#[tokio::test]
async fn uncommitted_write_through_entry_cannot_shadow_current_extent() {
    let (store, db) = make().await;
    let committed = Bytes::from(vec![0x11; EXTENT_SIZE]);
    write_extent(&store, &db, 0, &committed).await;

    let mut aborted = db.new_transaction().unwrap();
    store
        .write(
            &mut aborted,
            1,
            0,
            &Bytes::from(vec![0x22; EXTENT_SIZE]),
            EXTENT_SIZE as u64,
        )
        .await
        .unwrap();
    drop(aborted);

    assert_eq!(
        store.read(1, 0, EXTENT_SIZE as u64).await.unwrap(),
        committed,
        "only the FrameLoc selected by committed metadata may hit the cache"
    );
}

// A stored FrameLoc whose byte range lies outside the open buffer (a torn or
// corrupt LSM value — any 32-byte value decodes) must surface as EIO, not an
// out-of-range panic that poisons the open-segment lock for every later writer.
#[tokio::test]
async fn corrupt_frameloc_into_open_segment_is_eio_not_panic() {
    let (store, db) = make().await;
    let mut model = Vec::new();
    write_and_check(&store, &db, &mut model, 0, &[1u8; 100]).await;

    // Fabricate a pointer at the current open segment with an impossible range.
    let bogus = FrameLoc {
        segid: store.open_lane(1).open.lock().unwrap().segid,
        frame_index: 0,
        byte_offset: 1 << 40,
        byte_len: 4096,
    };
    let key = store.key_codec.extent_key(1, 5);
    let mut txn = db.new_transaction().unwrap();
    txn.put_bytes(&key, Bytes::copy_from_slice(&bogus.encode()));
    commit(&store, txn).await;

    assert!(matches!(store.get(1, 5).await, Err(FsError::IoError)));
    // The open-segment lock survived (not poisoned): writes still work.
    write_and_check(&store, &db, &mut model, 0, &[2u8; 100]).await;
}

// A corrupt extent value inside a multi-extent scan is the same EIO as the
// single-extent path — skipping it would serve that extent as fabricated
// zeros while a single-extent read of the same key errors.
#[tokio::test]
async fn multi_extent_read_of_a_corrupt_value_is_eio_not_zeros() {
    let (store, db) = make().await;

    // Plant a torn (undecodable) value at extent 1, then read across
    // extents 0..=2 so the ranged-scan path (not `get`) resolves it.
    let key = store.key_codec.extent_key(1, 1);
    let mut txn = db.new_transaction().unwrap();
    txn.put_bytes(&key, Bytes::from_static(&[0u8; FrameLoc::ENCODED_LEN - 1]));
    commit(&store, txn).await;

    let r = store.read_range(1, 0, 3 * EXTENT_SIZE as u64, true).await;
    assert!(matches!(r, Err(FsError::IoError)));
}

// In `get`'s GC-repoint retry, a re-resolved value that fails to decode is the
// same corrupt-value EIO as the primary path — treating it like an absent key
// would fabricate a hole of zeros from a torn LSM value.
#[test]
fn reresolve_distinguishes_a_hole_from_a_corrupt_value() {
    // Absent: a concurrent truncate/unlink made the extent a genuine hole.
    assert!(matches!(
        ExtentStore::decode_reresolved_extent(1, 0, None),
        Ok(None)
    ));
    // Present but undecodable (torn/truncated): EIO, never a hole.
    let torn = Bytes::from_static(&[0u8; FrameLoc::ENCODED_LEN - 1]);
    assert!(matches!(
        ExtentStore::decode_reresolved_extent(1, 0, Some(&torn)),
        Err(FsError::IoError)
    ));
    // A decodable value passes through for the segid comparison.
    let loc = FrameLoc {
        segid: Segid::new(1, 2),
        frame_index: 3,
        byte_offset: 4,
        byte_len: 5,
    };
    let enc = Bytes::copy_from_slice(&loc.encode());
    assert_eq!(
        ExtentStore::decode_reresolved_extent(1, 0, Some(&enc)).unwrap(),
        Some(loc)
    );
}

#[tokio::test]
async fn crossings_count_seams_not_reads_and_holes_break_adjacency() {
    let (writer, db, object_store) = make_with_compression(CompressionConfig::Lz4).await;
    // Segments A (extents 0-1), B (2-3), C (4-5), all sealed.
    for extent in 0..6u64 {
        write_extent(&writer, &db, extent, &[extent as u8 + 1; EXTENT_SIZE]).await;
        if extent % 2 == 1 {
            writer.seal_open().await.unwrap();
        }
    }
    let a = frameloc_of(&writer, &db, 1, 0).await.unwrap().segid;
    let b = frameloc_of(&writer, &db, 1, 2).await.unwrap().segid;
    let c = frameloc_of(&writer, &db, 1, 4).await.unwrap().segid;
    let store = make_store(object_store, db, CompressionConfig::Lz4, 8);
    store.enable_nominations();

    // One pass over A|B|C pays each seam once; re-reading in the same GC
    // round adds nothing (burst reads are not episodes).
    store.read(1, 0, 6 * EXTENT_SIZE as u64).await.unwrap();
    store.read(1, 0, 6 * EXTENT_SIZE as u64).await.unwrap();
    {
        let stats = store.pair_stats.lock().unwrap();
        assert_eq!(stats.map.len(), 2);
        assert_eq!(stats.map[&PairStats::key(a, b)].count, 1);
        assert_eq!(stats.map[&PairStats::key(b, c)].count, 1);
    }

    // A hole between two segments is not a seam: the data isn't adjacent.
    let (writer2, db2, object_store2) = make_with_compression(CompressionConfig::Lz4).await;
    for extent in 0..2u64 {
        write_extent(&writer2, &db2, extent, &[1u8; EXTENT_SIZE]).await;
    }
    writer2.seal_open().await.unwrap();
    for extent in 3..5u64 {
        // Writing extent 3 with the file at 2 extents leaves extent 2 a hole.
        let mut txn = db2.new_transaction().unwrap();
        let tu = writer2
            .write(
                &mut txn,
                1,
                extent * EXTENT_SIZE as u64,
                &Bytes::from(vec![2u8; EXTENT_SIZE]),
                2 * EXTENT_SIZE as u64,
            )
            .await
            .unwrap();
        commit(&writer2, txn).await;
        writer2.apply_tail_update(1, tu);
    }
    writer2.seal_open().await.unwrap();
    let store2 = make_store(object_store2, db2, CompressionConfig::Lz4, 8);
    store2.enable_nominations();
    store2.read(1, 0, 5 * EXTENT_SIZE as u64).await.unwrap();
    assert!(store2.pair_stats.lock().unwrap().map.is_empty());
}

#[tokio::test]
async fn reads_nominate_only_enabled_fanned_out_and_on_store_segments() {
    let (writer, db, object_store) = make_with_compression(CompressionConfig::Lz4).await;
    // Segment A holds extents 0-1, segment B extents 2-3.
    for extent in 0..4u64 {
        write_extent(&writer, &db, extent, &[extent as u8 + 1; EXTENT_SIZE]).await;
        if extent % 2 == 1 {
            writer.seal_open().await.unwrap();
        }
    }
    let seg_a = frameloc_of(&writer, &db, 1, 0).await.unwrap().segid;
    let seg_b = frameloc_of(&writer, &db, 1, 2).await.unwrap().segid;

    // Disabled (replica / pre-GC shape): a fanned-out read tracks nothing.
    let disabled = make_store(object_store.clone(), db.clone(), CompressionConfig::Lz4, 8);
    disabled.read(1, 0, 4 * EXTENT_SIZE as u64).await.unwrap();
    assert!(disabled.nominations.lock().unwrap().set.is_empty());

    // A read served by one segment is below the fan-out floor.
    let below_floor = make_store(object_store.clone(), db.clone(), CompressionConfig::Lz4, 9);
    below_floor.enable_nominations();
    below_floor
        .read(1, 0, 2 * EXTENT_SIZE as u64)
        .await
        .unwrap();
    assert!(below_floor.nominations.lock().unwrap().set.is_empty());

    // A read fanning out across both nominates both; re-reading dedups.
    let store = make_store(object_store.clone(), db.clone(), CompressionConfig::Lz4, 10);
    store.enable_nominations();
    store.read(1, 0, 4 * EXTENT_SIZE as u64).await.unwrap();
    store.read(1, 0, 4 * EXTENT_SIZE as u64).await.unwrap();
    {
        let noms = store.nominations.lock().unwrap();
        assert_eq!(noms.set.len(), 2);
        assert!(noms.set.contains(&seg_a) && noms.set.contains(&seg_b));
    }

    // RAM-served runs never count: extents 4-5 live in the open buffer, so
    // a read across B + open is one on-store segment — below the fan-out
    // floor (RAM runs cost no GETs, so the read isn't suffering).
    let ram_store = make_store(object_store.clone(), db.clone(), CompressionConfig::Lz4, 11);
    ram_store.enable_nominations();
    for extent in 4..6u64 {
        write_extent(&ram_store, &db, extent, &[extent as u8 + 1; EXTENT_SIZE]).await;
    }
    ram_store
        .read(1, 2 * EXTENT_SIZE as u64, 4 * EXTENT_SIZE as u64)
        .await
        .unwrap();
    assert!(ram_store.nominations.lock().unwrap().set.is_empty());

    // A read across A + B + open clears the floor on the two on-store
    // segments and still never nominates the open segid.
    let fanout_store = make_store(object_store, db.clone(), CompressionConfig::Lz4, 12);
    fanout_store.enable_nominations();
    for extent in 4..6u64 {
        write_extent(&fanout_store, &db, extent, &[extent as u8 + 1; EXTENT_SIZE]).await;
    }
    fanout_store
        .read(1, 0, 6 * EXTENT_SIZE as u64)
        .await
        .unwrap();
    {
        let open_segid = fanout_store.open_lane(1).open.lock().unwrap().segid;
        let noms = fanout_store.nominations.lock().unwrap();
        assert_eq!(noms.set.len(), 2);
        assert!(noms.set.contains(&seg_a) && noms.set.contains(&seg_b));
        assert!(!noms.set.contains(&open_segid));
    }
}

#[tokio::test]
async fn per_call_cap_bounds_nominations() {
    let (writer, db, object_store) = make_with_compression(CompressionConfig::Lz4).await;
    // More single-extent segments than the per-call cap.
    let n = NOMINATE_PER_CALL_CAP as u64 + 2;
    for extent in 0..n {
        write_extent(&writer, &db, extent, &[1u8; EXTENT_SIZE]).await;
        writer.seal_open().await.unwrap();
    }
    let store = make_store(object_store, db, CompressionConfig::Lz4, 8);
    store.enable_nominations();
    store.read(1, 0, n * EXTENT_SIZE as u64).await.unwrap();
    assert_eq!(
        store.nominations.lock().unwrap().set.len(),
        NOMINATE_PER_CALL_CAP
    );
}

#[test]
fn read_ahead_planner() {
    let w = READ_AHEAD_WINDOW_BYTES;
    // First read of a stream: unconfirmed, no prefetch.
    let (s, p) = plan_read_ahead((0, 0, 0), 0, 1000);
    assert_eq!(s, (1000, 1000, 1));
    assert_eq!(p, None);
    // Second sequential read: confirmed -> prefetch a window ahead.
    let (s, p) = plan_read_ahead(s, 1000, 1000);
    assert_eq!(s, (2000, 2000 + w, 2));
    assert_eq!(p, Some((2000, 2000 + w)));
    // Still deep in the buffered-ahead: no new prefetch.
    let (s, p) = plan_read_ahead(s, 2000, 1000);
    assert_eq!(s, (3000, 2000 + w, 3));
    assert_eq!(p, None);
    // A non-contiguous jump resets to unconfirmed.
    let (s, p) = plan_read_ahead(s, 5_000_000, 1000);
    assert_eq!(s, (5_001_000, 5_001_000, 1));
    assert_eq!(p, None);
    // Less than half a window buffered ahead -> refill.
    let low = (2000 + w / 2 + 1, 2000 + w, 9);
    let (_, p) = plan_read_ahead(low, 2000 + w / 2 + 1, 100);
    assert!(
        p.is_some(),
        "refills when under half a window remains ahead"
    );
}

#[tokio::test]
async fn read_ahead_spawns_only_on_confirmed_sequential() {
    let (store, _db) = make().await;
    // First read of a stream: nothing spawned (could be a one-off).
    assert!(store.trigger_read_ahead(1, 0, 1000).is_none());
    // Second sequential read: a read-ahead task is spawned.
    let h = store.trigger_read_ahead(1, 1000, 1000);
    assert!(h.is_some(), "confirmed sequential -> read-ahead");
    h.unwrap().await.unwrap();
    // A non-contiguous jump: nothing spawned.
    assert!(store.trigger_read_ahead(1, 9_000_000, 1000).is_none());
}

#[tokio::test]
async fn read_ahead_skip_at_cap_keeps_coverage_honest() {
    let (store, _db) = make().await;
    // Hold every permit so the planned prefetch is skipped at the cap.
    let held: Vec<_> = (0..READ_AHEAD_MAX_CONCURRENT)
        .map(|_| Arc::clone(&store.prefetch_sem).try_acquire_owned().unwrap())
        .collect();
    assert!(store.trigger_read_ahead(1, 0, 1000).is_none());
    assert!(
        store.trigger_read_ahead(1, 1000, 1000).is_none(),
        "at the cap -> skip"
    );
    // The skip must not count the window as covered: `prefetched_to`
    // stays at the read end, so the next read re-plans.
    let (_, prefetched_to, _) = store.read_ahead.get(&1).map(|e| *e).unwrap();
    assert_eq!(prefetched_to, 2000, "skipped window recorded as covered");
    drop(held);
    let h = store.trigger_read_ahead(1, 2000, 1000);
    assert!(h.is_some(), "freed permit -> the next read re-triggers");
    h.unwrap().await.unwrap();
}

use crate::fault_store::FaultStore;
use futures::stream::BoxStream;
use object_store::memory::InMemory;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions as ObjectPutOptions, PutPayload, PutResult, path::Path,
};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;
use tokio::sync::Notify;

#[derive(Debug)]
struct GetGate {
    hold: AtomicBool,
    active: AtomicUsize,
    peak: AtomicUsize,
    started: Notify,
    release: Notify,
}

impl GetGate {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            hold: AtomicBool::new(false),
            active: AtomicUsize::new(0),
            peak: AtomicUsize::new(0),
            started: Notify::new(),
            release: Notify::new(),
        })
    }

    fn hold(&self) {
        self.hold.store(true, Ordering::SeqCst);
    }

    fn release(&self) {
        self.hold.store(false, Ordering::SeqCst);
        self.release.notify_waiters();
    }

    fn peak(&self) -> usize {
        self.peak.load(Ordering::SeqCst)
    }
}

#[derive(Debug)]
struct GatedGetStore {
    inner: Arc<dyn ObjectStore>,
    gate: Arc<GetGate>,
}

impl GatedGetStore {
    fn wrap(inner: Arc<dyn ObjectStore>) -> (Arc<Self>, Arc<GetGate>) {
        let gate = GetGate::new();
        (
            Arc::new(Self {
                inner,
                gate: Arc::clone(&gate),
            }),
            gate,
        )
    }
}

impl std::fmt::Display for GatedGetStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "GatedGetStore({})", self.inner)
    }
}

#[async_trait::async_trait]
impl ObjectStore for GatedGetStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: ObjectPutOptions,
    ) -> object_store::Result<PutResult> {
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        let now = self.gate.active.fetch_add(1, Ordering::SeqCst) + 1;
        self.gate.peak.fetch_max(now, Ordering::SeqCst);
        self.gate.started.notify_waiters();
        while self.gate.hold.load(Ordering::SeqCst) {
            let notified = self.gate.release.notified();
            if !self.gate.hold.load(Ordering::SeqCst) {
                break;
            }
            notified.await;
        }
        let result = self.inner.get_opts(location, options).await;
        self.gate.active.fetch_sub(1, Ordering::SeqCst);
        result
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, object_store::Result<Path>>,
    ) -> BoxStream<'static, object_store::Result<Path>> {
        self.inner.delete_stream(locations)
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        opts: CopyOptions,
    ) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, opts).await
    }
}

async fn sealed_independent_runs(
    run_count: u64,
) -> (ExtentStore, Vec<u8>, Arc<GetGate>, Arc<dyn ObjectStore>) {
    use crate::block_transformer::ZeroFsBlockTransformer;
    use crate::db::Db;
    use slatedb::{BlockTransformer, DbBuilder};

    let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let (gated, gate) = GatedGetStore::wrap(Arc::clone(&inner));
    let object_store: Arc<dyn ObjectStore> = gated;
    let bt: Arc<dyn BlockTransformer> =
        ZeroFsBlockTransformer::new_arc(&[0u8; 32], CompressionConfig::default());
    let slatedb = Arc::new(
        DbBuilder::new(Path::from("t"), object_store.clone())
            .with_block_transformer(bt)
            .with_segment_extractor(Arc::new(crate::segment_extractor::ZeroFsSegmentExtractor))
            .build()
            .await
            .unwrap(),
    );
    let db = Arc::new(Db::new(slatedb, None));
    let writer = make_store(object_store.clone(), db.clone(), CompressionConfig::Lz4, 7);
    let mut model = Vec::new();
    for extent in 0..run_count {
        let byte = (extent as u8).wrapping_add(1);
        write_extent(&writer, &db, extent, &[byte; EXTENT_SIZE]).await;
        writer.seal_open().await.unwrap();
        model.extend(std::iter::repeat_n(byte, EXTENT_SIZE));
    }
    let store = make_store(object_store.clone(), db, CompressionConfig::Lz4, 8);
    (store, model, gate, object_store)
}

#[tokio::test]
async fn one_fragmented_read_fetches_independent_runs_concurrently() {
    let (store, model, gate, _) = sealed_independent_runs(8).await;
    let metrics_store = store.clone();
    gate.hold();
    let reader = tokio::spawn(async move { store.read(1, 0, 8 * EXTENT_SIZE as u64).await });
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if gate.peak() >= 2 {
                break;
            }
            gate.started.notified().await;
        }
    })
    .await
    .expect("independent runs never reached concurrent GETs");
    assert!(
        gate.peak() >= 2,
        "fragmented read stayed sequential: peak {}",
        gate.peak()
    );
    assert!(
        gate.peak() <= PARALLEL_EXTENT_OPS,
        "fragmented read exceeded PARALLEL_EXTENT_OPS: peak {}",
        gate.peak()
    );
    gate.release();
    let got = tokio::time::timeout(Duration::from_secs(5), reader)
        .await
        .expect("fragmented read did not finish")
        .expect("fragmented read task panicked")
        .expect("fragmented read failed");
    assert_eq!(got.as_ref(), model.as_slice());
    let snapshot = metrics_store
        .last_read_metrics()
        .expect("read recorded metrics");
    assert_eq!(snapshot.on_store_runs, 8);
    assert!(snapshot.peak_run_fetches >= 2);
}

#[tokio::test]
async fn fragmented_read_concurrency_is_bounded() {
    let (store, model, gate, _) = sealed_independent_runs(8).await;
    gate.hold();
    let reader = tokio::spawn(async move { store.read(1, 0, 8 * EXTENT_SIZE as u64).await });
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(
        gate.peak() <= PARALLEL_EXTENT_OPS,
        "peak {} exceeded PARALLEL_EXTENT_OPS",
        gate.peak()
    );
    gate.release();
    let got = reader.await.unwrap().unwrap();
    assert_eq!(got.as_ref(), model.as_slice());
}

#[tokio::test]
async fn fragmented_read_preserves_logical_output_order() {
    let (store, model, _, _) = sealed_independent_runs(8).await;
    let got = store.read(1, 0, 8 * EXTENT_SIZE as u64).await.unwrap();
    assert_eq!(got.as_ref(), model.as_slice());
    for extent in 0..8u64 {
        let expected = (extent as u8).wrapping_add(1);
        let start = extent as usize * EXTENT_SIZE;
        assert!(
            got[start..start + EXTENT_SIZE]
                .iter()
                .all(|b| *b == expected),
            "extent {extent} lost logical order"
        );
    }
}

#[tokio::test]
async fn contiguous_control_remains_one_ranged_get() {
    let (writer, db, object_store) = make_with_compression(CompressionConfig::Lz4).await;
    let mut model = Vec::new();
    write_and_check(&writer, &db, &mut model, 0, &vec![9u8; 4 * EXTENT_SIZE]).await;
    writer.seal_open().await.unwrap();
    let store = make_store(object_store, db, CompressionConfig::Lz4, 8);
    let before = store.segments.read_calls();
    let got = store.read(1, 0, 4 * EXTENT_SIZE as u64).await.unwrap();
    assert_eq!(store.segments.read_calls() - before, 1);
    assert_eq!(got.as_ref(), model.as_slice());
}

#[tokio::test]
async fn stale_location_fallback_remains_correct_under_concurrency() {
    let (store, model, _, object_store) = sealed_independent_runs(8).await;
    let (fault, controls) = FaultStore::new(object_store);
    let _ = fault;
    controls.fail_gets(1);
    let got = store.read(1, 0, 8 * EXTENT_SIZE as u64).await.unwrap();
    assert_eq!(got.as_ref(), model.as_slice());
}

#[tokio::test]
async fn failed_fragmented_read_releases_every_fetch_permit() {
    let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let (fault, controls) = FaultStore::new(Arc::clone(&inner));
    let (store, model, _, _) = {
        // Build independent runs on the fault store so later GETs can fail.
        use crate::block_transformer::ZeroFsBlockTransformer;
        use crate::db::Db;
        use slatedb::{BlockTransformer, DbBuilder};

        let object_store: Arc<dyn ObjectStore> = fault;
        let bt: Arc<dyn BlockTransformer> =
            ZeroFsBlockTransformer::new_arc(&[0u8; 32], CompressionConfig::default());
        let slatedb = Arc::new(
            DbBuilder::new(Path::from("t"), object_store.clone())
                .with_block_transformer(bt)
                .with_segment_extractor(Arc::new(crate::segment_extractor::ZeroFsSegmentExtractor))
                .build()
                .await
                .unwrap(),
        );
        let db = Arc::new(Db::new(slatedb, None));
        let writer = make_store(object_store.clone(), db.clone(), CompressionConfig::Lz4, 7);
        let mut model = Vec::new();
        for extent in 0..8u64 {
            let byte = (extent as u8).wrapping_add(3);
            write_extent(&writer, &db, extent, &[byte; EXTENT_SIZE]).await;
            writer.seal_open().await.unwrap();
            model.extend(std::iter::repeat_n(byte, EXTENT_SIZE));
        }
        (
            make_store(object_store, db, CompressionConfig::Lz4, 8),
            model,
            (),
            (),
        )
    };
    controls.fail_gets(64);
    let err = store.read(1, 0, 8 * EXTENT_SIZE as u64).await;
    assert!(err.is_err(), "expected failed fragmented read, got success");
    let snapshot = store
        .last_read_metrics()
        .expect("failed read still records metrics");
    assert!(
        snapshot.peak_run_fetches > 0,
        "failed read never started a run fetch"
    );
    controls.fail_gets(0);
    // Permits must be reusable after failure.
    let got = store.read(1, 0, 8 * EXTENT_SIZE as u64).await.unwrap();
    assert_eq!(got.as_ref(), model.as_slice());
}
