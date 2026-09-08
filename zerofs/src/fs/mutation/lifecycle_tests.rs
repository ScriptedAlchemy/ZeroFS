use super::config::{
    ClientDurabilityTarget, FilesystemWriteAckMode, FilesystemWriteAckSettings,
    FilesystemWriteAckSource,
};
use super::materializer::{ApplyHook, Materializer, MaterializerLifecycleCensus};
use super::overlay::OverlayLifecycleCensus;
use super::types::{MutationCutoff, MutationIncarnation, PreparedBatchResult, PreparedWriteBatch};
use crate::fs::ZeroFS;
use crate::fs::test_util::test_creds;
use crate::fs::types::{AuthContext, FileAttributes};
use bytes::Bytes;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Weak};
use std::time::Duration;
use tokio::sync::{Notify, mpsc};

const HISTORICAL_INODES: usize = 10_000;
const LIVE_CONCURRENCY: usize = 1;
const CONFIGURED_OPERATION_CAPACITY: usize = 32;
// A completed task may remain observable while its destructor settles. This
// bounds that transition without requiring unrelated inode teardown to run in
// the foreground; the post-drain census below still requires exact zero.
const MAX_ACTIVE_PLUS_RETIRING: usize = LIVE_CONCURRENCY + 1;
const OPERATION_TIMEOUT: Duration = Duration::from_secs(10);
const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(120);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct LifecycleCensus {
    overlay: OverlayLifecycleCensus,
    materializer: MaterializerLifecycleCensus,
}

impl LifecycleCensus {
    fn observe_max(&mut self, sample: Self) {
        self.overlay.runtimes = self.overlay.runtimes.max(sample.overlay.runtimes);
        self.overlay.runtime_workers = self
            .overlay
            .runtime_workers
            .max(sample.overlay.runtime_workers);
        self.overlay.pending_keys = self.overlay.pending_keys.max(sample.overlay.pending_keys);
        self.overlay.pending_dispatches = self
            .overlay
            .pending_dispatches
            .max(sample.overlay.pending_dispatches);
        self.overlay.visible_attrs = self.overlay.visible_attrs.max(sample.overlay.visible_attrs);
        self.materializer.lanes = self.materializer.lanes.max(sample.materializer.lanes);
        self.materializer.worker_handles = self
            .materializer
            .worker_handles
            .max(sample.materializer.worker_handles);
        self.materializer.active_workers = self
            .materializer
            .active_workers
            .max(sample.materializer.active_workers);
    }

    fn execution_is_idle(self) -> bool {
        self.overlay.runtimes == 0
            && self.overlay.runtime_workers == 0
            && self.overlay.pending_keys == 0
            && self.overlay.pending_dispatches == 0
            && self.overlay.visible_attrs == 0
            && self.materializer.lanes == 0
            && self.materializer.worker_handles == 0
            && self.materializer.active_workers == 0
    }
}

fn volatile_settings() -> FilesystemWriteAckSettings {
    FilesystemWriteAckSettings {
        mode: FilesystemWriteAckMode::VolatileMemory,
        volatile_memory_bytes: 8 * 1024 * 1024,
        // Workload concurrency, not configured admission capacity, is the
        // independent variable in this lifecycle test. Keep enough headroom
        // for the deterministic hot-enqueue fixture below.
        volatile_max_operations: CONFIGURED_OPERATION_CAPACITY,
        source: FilesystemWriteAckSource::Filesystem,
        client_durability_target: ClientDurabilityTarget::LocalSsd,
    }
}

async fn filesystem() -> (Arc<ZeroFS>, AuthContext) {
    let mut fs = ZeroFS::new_in_memory().await.unwrap();
    fs.write_ack = volatile_settings();
    let fs = Arc::new(fs);
    fs.install_volatile_overlay();
    (fs, AuthContext::from(&test_creds()))
}

fn census(fs: &ZeroFS) -> LifecycleCensus {
    LifecycleCensus {
        overlay: fs
            .volatile_overlay
            .get()
            .expect("volatile overlay installed")
            .lifecycle_census_for_test(),
        materializer: fs
            .materializer
            .get()
            .expect("materializer installed")
            .lifecycle_census_for_test(),
    }
}

async fn wait_for_idle(fs: &ZeroFS) -> bool {
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if census(fs).execution_is_idle() {
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .is_ok()
}

fn cutoff(incarnation: MutationIncarnation, sequence: u64) -> MutationCutoff {
    MutationCutoff {
        mutation_incarnation: incarnation,
        sequence,
    }
}

fn replayed_batch(inode: u64) -> PreparedWriteBatch {
    PreparedWriteBatch::replayed(
        [0; 16],
        PreparedBatchResult {
            members: vec![(inode, FileAttributes::default())],
            cutoff: None,
        },
    )
}

async fn wait_for_enqueued_jobs(materializer: &Materializer, target: usize) {
    tokio::time::timeout(Duration::from_secs(2), async {
        while materializer.lane_jobs_enqueued_for_test() < target {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("materializer job was not enqueued");
}

async fn receive_sequence(receiver: &mut mpsc::UnboundedReceiver<u64>) -> u64 {
    tokio::time::timeout(Duration::from_secs(2), receiver.recv())
        .await
        .expect("materializer apply did not enter")
        .expect("materializer apply observer dropped")
}

async fn stop_filesystem(fs: &ZeroFS) {
    tokio::time::timeout(SHUTDOWN_TIMEOUT, fs.stop_mutation_workers())
        .await
        .expect("mutation worker shutdown timed out")
        .expect("mutation worker shutdown failed");
}

/// Regression for Draft 10: byte/operation budgets bound live dirty work, but
/// must not leave one runtime, two workers, and map entries per historical
/// inode after that work is drained and deleted.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_inode_churn_is_bounded_by_live_concurrency() {
    let (fs, auth) = filesystem().await;
    let mut peak = census(&fs);

    for index in 0..HISTORICAL_INODES {
        let name = format!("idle-lifecycle-{index:05}.bin").into_bytes();
        let inode = tokio::time::timeout(OPERATION_TIMEOUT, fs.create_exclusive(&auth, 0, &name))
            .await
            .expect("create timed out")
            .expect("create real inode");
        let payload = Bytes::copy_from_slice(&(index as u64).to_le_bytes());
        tokio::time::timeout(OPERATION_TIMEOUT, fs.write_ack(&auth, inode, 0, &payload))
            .await
            .expect("write acknowledgement timed out")
            .expect("acknowledge real filesystem write");
        peak.observe_max(census(&fs));

        tokio::time::timeout(OPERATION_TIMEOUT, fs.quiesce_overlay_inode(inode))
            .await
            .expect("inode drain timed out")
            .expect("drain acknowledged inode");
        tokio::time::timeout(OPERATION_TIMEOUT, fs.remove(&auth, 0, &name))
            .await
            .expect("inode delete timed out")
            .expect("delete drained inode");
        peak.observe_max(census(&fs));
    }

    tokio::time::timeout(SHUTDOWN_TIMEOUT, fs.wait_configured_durability())
        .await
        .expect("global durability drain timed out")
        .expect("drain global durability cutoff");
    let became_idle = wait_for_idle(&fs).await;
    let drained = census(&fs);
    stop_filesystem(&fs).await;

    assert!(
        became_idle,
        "drained lifecycle retained historical inode state: {drained:?}"
    );
    assert!(
        peak.overlay.runtimes <= MAX_ACTIVE_PLUS_RETIRING,
        "runtime cardinality followed history rather than live work: {peak:?}"
    );
    assert!(peak.overlay.runtime_workers <= MAX_ACTIVE_PLUS_RETIRING);
    assert!(peak.overlay.pending_keys <= MAX_ACTIVE_PLUS_RETIRING);
    assert!(peak.overlay.pending_dispatches <= MAX_ACTIVE_PLUS_RETIRING);
    assert!(peak.overlay.visible_attrs <= MAX_ACTIVE_PLUS_RETIRING);
    assert!(peak.materializer.lanes <= MAX_ACTIVE_PLUS_RETIRING);
    assert!(peak.materializer.worker_handles <= MAX_ACTIVE_PLUS_RETIRING);
    assert!(peak.materializer.active_workers <= MAX_ACTIVE_PLUS_RETIRING);
    assert!(drained.execution_is_idle(), "final census: {drained:?}");
}

/// The idle worker must either observe a concurrently queued job in its old
/// generation or retire before the enqueue creates a new generation. It must
/// never drop queued work or allow two same-inode apply owners to overlap.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn hot_enqueue_racing_materializer_retirement_keeps_one_fifo_owner() {
    let inode = 41;
    let incarnation = MutationIncarnation::new();
    let active = Arc::new(AtomicUsize::new(0));
    let max_active = Arc::new(AtomicUsize::new(0));
    let release_second = Arc::new(Notify::new());
    let (entered_tx, mut entered_rx) = mpsc::unbounded_channel();
    let hook: ApplyHook = {
        let active = Arc::clone(&active);
        let max_active = Arc::clone(&max_active);
        let release_second = Arc::clone(&release_second);
        Arc::new(move |cutoff, _batch| {
            let active = Arc::clone(&active);
            let max_active = Arc::clone(&max_active);
            let release_second = Arc::clone(&release_second);
            let entered_tx = entered_tx.clone();
            Box::pin(async move {
                let now_active = active.fetch_add(1, Ordering::AcqRel) + 1;
                max_active.fetch_max(now_active, Ordering::AcqRel);
                entered_tx.send(cutoff.sequence).unwrap();
                if cutoff.sequence == 2 {
                    release_second.notified().await;
                }
                active.fetch_sub(1, Ordering::AcqRel);
                Ok(())
            })
        })
    };
    let materializer = Materializer::start_with_hook(incarnation, Weak::new(), None, Some(hook));
    let (retirement_reached, resume_retirement) =
        materializer.pause_next_idle_retirement_for_test(inode);

    tokio::time::timeout(
        OPERATION_TIMEOUT,
        materializer.dispatch_through(cutoff(incarnation, 1), replayed_batch(inode)),
    )
    .await
    .expect("first dispatch timed out")
    .unwrap();
    assert_eq!(receive_sequence(&mut entered_rx).await, 1);
    tokio::time::timeout(Duration::from_secs(2), retirement_reached)
        .await
        .expect("worker did not reach idle retirement")
        .expect("idle retirement pause dropped");

    let second = tokio::spawn({
        let materializer = Arc::clone(&materializer);
        async move {
            materializer
                .dispatch_through(cutoff(incarnation, 2), replayed_batch(inode))
                .await
        }
    });
    wait_for_enqueued_jobs(&materializer, 2).await;
    resume_retirement.send(()).unwrap();
    assert_eq!(receive_sequence(&mut entered_rx).await, 2);

    let third = tokio::spawn({
        let materializer = Arc::clone(&materializer);
        async move {
            materializer
                .dispatch_through(cutoff(incarnation, 3), replayed_batch(inode))
                .await
        }
    });
    wait_for_enqueued_jobs(&materializer, 3).await;
    assert!(
        tokio::time::timeout(Duration::from_millis(100), entered_rx.recv())
            .await
            .is_err(),
        "a replacement lane overtook the blocked owner"
    );

    release_second.notify_one();
    assert_eq!(receive_sequence(&mut entered_rx).await, 3);
    tokio::time::timeout(OPERATION_TIMEOUT, second)
        .await
        .expect("second dispatch join timed out")
        .expect("second dispatch task failed")
        .expect("second dispatch failed");
    tokio::time::timeout(OPERATION_TIMEOUT, third)
        .await
        .expect("third dispatch join timed out")
        .expect("third dispatch task failed")
        .expect("third dispatch failed");
    let became_idle = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let census = materializer.lifecycle_census_for_test();
            if census.lanes == 0 && census.worker_handles == 0 && census.active_workers == 0 {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .is_ok();
    let drained = materializer.lifecycle_census_for_test();
    tokio::time::timeout(SHUTDOWN_TIMEOUT, materializer.stop())
        .await
        .expect("materializer shutdown timed out");

    assert_eq!(max_active.load(Ordering::Acquire), 1);
    assert!(became_idle, "materializer did not retire: {drained:?}");
}

/// Exercise the real overlay acceptance path while a drained runtime is at
/// its retirement edge. The hot write must stay visible, drainable, and owned
/// by a single runtime worker generation.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn hot_write_racing_runtime_retirement_remains_visible_and_drainable() {
    let (fs, auth) = filesystem().await;
    let inode = tokio::time::timeout(
        OPERATION_TIMEOUT,
        fs.create_exclusive(&auth, 0, b"hot-runtime.bin"),
    )
    .await
    .expect("hot inode create timed out")
    .unwrap();
    let overlay = fs.volatile_overlay.get().cloned().unwrap();
    let mut retirement_pause = overlay.pause_next_runtime_idle_retirement_for_test(inode);

    tokio::time::timeout(
        OPERATION_TIMEOUT,
        fs.write_ack(&auth, inode, 0, &Bytes::from_static(b"first")),
    )
    .await
    .expect("first write acknowledgement timed out")
    .unwrap();
    tokio::time::timeout(
        Duration::from_secs(2),
        retirement_pause.wait_until_reached(),
    )
    .await
    .expect("runtime did not reach idle retirement")
    .expect("runtime retirement pause dropped");

    let hot_write = tokio::spawn({
        let fs = Arc::clone(&fs);
        let auth = auth.clone();
        async move {
            fs.write_ack(&auth, inode, 0, &Bytes::from_static(b"second"))
                .await
        }
    });
    tokio::time::timeout(OPERATION_TIMEOUT, hot_write)
        .await
        .expect("hot write join timed out")
        .expect("hot write task failed")
        .expect("hot write failed");
    let raced = census(&fs);
    assert_eq!(raced.overlay.runtimes, 1, "two runtime owners: {raced:?}");
    assert_eq!(
        raced.overlay.runtime_workers, 1,
        "two runtime workers: {raced:?}"
    );

    retirement_pause.resume();
    tokio::time::timeout(OPERATION_TIMEOUT, fs.quiesce_overlay_inode(inode))
        .await
        .expect("hot inode drain timed out")
        .unwrap();
    let (data, _) = tokio::time::timeout(OPERATION_TIMEOUT, fs.read_file(&auth, inode, 0, 16))
        .await
        .expect("hot inode read timed out")
        .unwrap();
    assert_eq!(data, Bytes::from_static(b"second"));
    tokio::time::timeout(OPERATION_TIMEOUT, fs.remove(&auth, 0, b"hot-runtime.bin"))
        .await
        .expect("hot inode delete timed out")
        .unwrap();

    let became_idle = wait_for_idle(&fs).await;
    let drained = census(&fs);
    stop_filesystem(&fs).await;
    assert!(became_idle, "runtime did not retire: {drained:?}");
}
