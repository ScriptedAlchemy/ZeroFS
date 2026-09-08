//! Canonical materialization of accepted write batches.
//!
//! Same-inode work is FIFO. Distinct inodes run concurrently. A striped
//! batch becomes visible to [`MutationProgress`] only after every member
//! has been applied. The first post-ack failure poisons progress and
//! freezes the overlay so clients keep the last coherent view.

use crate::dedup::AcceptedWriteLifecycle;
use crate::fs::ZeroFS;
use crate::fs::mutation::overlay::FilesystemVolatileOverlay;
use crate::fs::mutation::overlay_dispatch::PendingDispatch;
use crate::fs::mutation::progress::MutationProgress;
use crate::fs::mutation::types::{
    MutationCutoff, MutationError, MutationIncarnation, PreparedWriteBatch,
};
use crate::fs::mutation::volatile_overlay::{OverlayError, VolatileWriteRuntime};
use crate::fs::ops::write::apply_prepared_batch;
use futures::FutureExt;
use std::collections::{BTreeSet, HashMap};
use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::pin::Pin;
#[cfg(test)]
use std::sync::Condvar;
#[cfg(test)]
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, Weak};
use tokio::sync::mpsc;
#[cfg(test)]
use tokio::sync::oneshot;
use tokio_util::task::TaskTracker;

pub(crate) type ApplyHook = Arc<
    dyn Fn(
            MutationCutoff,
            PreparedWriteBatch,
        ) -> Pin<Box<dyn Future<Output = Result<(), MutationError>> + Send>>
        + Send
        + Sync,
>;

#[allow(clippy::large_enum_variant)]
enum LaneJob {
    #[cfg(test)]
    Apply {
        cutoff: MutationCutoff,
        batch: PreparedWriteBatch,
        accepted_write: Option<AcceptedWriteLifecycle>,
        reply: oneshot::Sender<Result<(), MutationError>>,
    },
    #[cfg(test)]
    Hold {
        acquired: oneshot::Sender<()>,
        release: oneshot::Receiver<()>,
    },
    VolatileMember {
        fs: Arc<ZeroFS>,
        dispatch: Arc<PendingDispatch>,
        runtime: Arc<VolatileWriteRuntime>,
        sequence: u64,
    },
    #[cfg(test)]
    Probe {
        label: &'static str,
        observed: mpsc::UnboundedSender<&'static str>,
    },
}

struct LaneEntry {
    generation: u64,
    sender: mpsc::UnboundedSender<LaneJob>,
}

struct LaneRegistry {
    closed: bool,
    next_generation: u64,
    entries: HashMap<u64, LaneEntry>,
}

struct MaterializerInner {
    incarnation: MutationIncarnation,
    progress: MutationProgress,
    fs: Weak<ZeroFS>,
    overlay: Weak<FilesystemVolatileOverlay>,
    apply_hook: Option<ApplyHook>,
    lanes: Mutex<LaneRegistry>,
    hold_enqueue: Mutex<()>,
    workers: TaskTracker,
    #[cfg(test)]
    hold_enqueue_interleave: HoldEnqueueInterleave,
    #[cfg(test)]
    idle_retirement_pause: Mutex<Option<IdleRetirementPause>>,
    #[cfg(test)]
    lane_jobs_enqueued: AtomicUsize,
}

#[cfg(test)]
struct HoldEnqueueInterleave {
    enabled: AtomicBool,
    arrivals: AtomicUsize,
    second_attempted: Mutex<bool>,
    changed: Condvar,
}

#[cfg(test)]
struct IdleRetirementPause {
    inode: u64,
    reached: oneshot::Sender<()>,
    resume: oneshot::Receiver<()>,
}

#[cfg(test)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct MaterializerLifecycleCensus {
    pub(crate) lanes: usize,
    pub(crate) worker_handles: usize,
    pub(crate) active_workers: usize,
}

/// Owns per-inode worker loops that apply accepted batches in order.
#[derive(Clone)]
pub(crate) struct Materializer {
    inner: Arc<MaterializerInner>,
}

impl Materializer {
    pub(crate) fn start(
        incarnation: MutationIncarnation,
        fs: Weak<ZeroFS>,
        overlay: Option<Arc<FilesystemVolatileOverlay>>,
    ) -> Arc<Self> {
        Self::start_with_hook(incarnation, fs, overlay, None)
    }

    pub(crate) fn start_with_hook(
        incarnation: MutationIncarnation,
        fs: Weak<ZeroFS>,
        overlay: Option<Arc<FilesystemVolatileOverlay>>,
        apply_hook: Option<ApplyHook>,
    ) -> Arc<Self> {
        Arc::new(Self {
            inner: Arc::new(MaterializerInner {
                incarnation,
                progress: MutationProgress::new(incarnation),
                fs,
                overlay: overlay
                    .map(|overlay| Arc::downgrade(&overlay))
                    .unwrap_or_default(),
                apply_hook,
                lanes: Mutex::new(LaneRegistry {
                    closed: false,
                    next_generation: 1,
                    entries: HashMap::new(),
                }),
                hold_enqueue: Mutex::new(()),
                workers: TaskTracker::new(),
                #[cfg(test)]
                hold_enqueue_interleave: HoldEnqueueInterleave {
                    enabled: AtomicBool::new(false),
                    arrivals: AtomicUsize::new(0),
                    second_attempted: Mutex::new(false),
                    changed: Condvar::new(),
                },
                #[cfg(test)]
                idle_retirement_pause: Mutex::new(None),
                #[cfg(test)]
                lane_jobs_enqueued: AtomicUsize::new(0),
            }),
        })
    }

    pub(crate) fn incarnation(&self) -> MutationIncarnation {
        self.inner.incarnation
    }

    pub(crate) fn progress(&self) -> MutationProgress {
        self.inner.progress.clone()
    }

    #[cfg(test)]
    pub(crate) async fn dispatch_through(
        self: &Arc<Self>,
        cutoff: MutationCutoff,
        batch: PreparedWriteBatch,
    ) -> Result<(), MutationError> {
        self.dispatch_through_owned(cutoff, batch, None).await
    }

    #[cfg(test)]
    pub(crate) async fn dispatch_accepted_through(
        self: &Arc<Self>,
        cutoff: MutationCutoff,
        batch: PreparedWriteBatch,
        accepted_write: AcceptedWriteLifecycle,
    ) -> Result<(), MutationError> {
        self.dispatch_through_owned(cutoff, batch, Some(accepted_write))
            .await
    }

    #[cfg(test)]
    async fn dispatch_through_owned(
        self: &Arc<Self>,
        cutoff: MutationCutoff,
        batch: PreparedWriteBatch,
        accepted_write: Option<AcceptedWriteLifecycle>,
    ) -> Result<(), MutationError> {
        if cutoff.mutation_incarnation != self.inner.incarnation {
            return Err(MutationError::StaleIncarnation);
        }
        if lock(&self.inner.lanes).closed {
            return Err(MutationError::Closed);
        }
        self.inner.progress.check()?;

        let inodes = batch_inodes(&batch);
        if inodes.is_empty() {
            self.record_success(cutoff)?;
            if let Some(accepted_write) = accepted_write {
                accepted_write.finish(true);
            }
            return Ok(());
        }

        if inodes.len() == 1 {
            let inode = *inodes.iter().next().expect("one inode");
            let (reply_tx, reply_rx) = oneshot::channel();
            self.enqueue_lane(
                inode,
                LaneJob::Apply {
                    cutoff,
                    batch,
                    accepted_write,
                    reply: reply_tx,
                },
            )?;
            return reply_rx
                .await
                .unwrap_or_else(|_| Err(self.poison("inode worker dropped")));
        }

        let mut holds = Vec::with_capacity(inodes.len());
        #[cfg(test)]
        let interleave_position = self.hold_enqueue_interleave_position_for_test();
        {
            // All lanes must observe multi-inode batches in one global order.
            // Hold only this synchronous enqueue lock; the potentially slow
            // acquisition and apply phases remain concurrent.
            let _enqueue = lock(&self.inner.hold_enqueue);
            for inode in &inodes {
                #[cfg(test)]
                let is_first = holds.is_empty();
                let (acquired_tx, acquired_rx) = oneshot::channel();
                let (release_tx, release_rx) = oneshot::channel();
                self.enqueue_lane(
                    *inode,
                    LaneJob::Hold {
                        acquired: acquired_tx,
                        release: release_rx,
                    },
                )?;
                holds.push((acquired_rx, release_tx));
                #[cfg(test)]
                if is_first && interleave_position == Some(0) {
                    let attempted = lock(&self.inner.hold_enqueue_interleave.second_attempted);
                    drop(
                        self.inner
                            .hold_enqueue_interleave
                            .changed
                            .wait_while(attempted, |attempted| !*attempted)
                            .unwrap_or_else(|error| error.into_inner()),
                    );
                }
            }
        }
        for (acquired, _) in &mut holds {
            acquired
                .await
                .map_err(|_| self.poison("inode worker dropped"))?;
        }

        let result = self.apply_caught(cutoff, batch, accepted_write).await;
        if result.is_ok() {
            // Attribute previews are part of the lane-owned visibility tail.
            // Retire them before releasing any hold so a replacement lane
            // generation cannot start while the prior preview is still live.
            self.retire_inodes(&inodes);
        }
        for (_, release) in holds {
            let _ = release.send(());
        }
        result
    }

    pub(crate) fn with_volatile_enqueue_gate<T>(&self, operation: impl FnOnce() -> T) -> T {
        // Volatile admission takes this gate, then one runtime state lock, then
        // the lane registry. The closure must remain synchronous so workers
        // never await behind an admission-held lock.
        let _enqueue = lock(&self.inner.hold_enqueue);
        operation()
    }

    pub(crate) fn enqueue_volatile_member(
        self: &Arc<Self>,
        inode: u64,
        fs: Arc<ZeroFS>,
        dispatch: Arc<PendingDispatch>,
        runtime: Arc<VolatileWriteRuntime>,
        sequence: u64,
    ) -> Result<(), MutationError> {
        self.enqueue_lane(
            inode,
            LaneJob::VolatileMember {
                fs,
                dispatch,
                runtime,
                sequence,
            },
        )
    }

    #[cfg(test)]
    fn enqueue_probe(
        self: &Arc<Self>,
        inode: u64,
        label: &'static str,
        observed: mpsc::UnboundedSender<&'static str>,
    ) -> Result<(), MutationError> {
        self.enqueue_lane(inode, LaneJob::Probe { label, observed })
    }

    pub(crate) async fn apply_held(
        &self,
        cutoff: MutationCutoff,
        batch: PreparedWriteBatch,
        accepted_write: Option<AcceptedWriteLifecycle>,
    ) -> Result<(), MutationError> {
        if cutoff.mutation_incarnation != self.inner.incarnation {
            if let Some(accepted_write) = accepted_write {
                accepted_write.finish(false);
            }
            return Err(MutationError::StaleIncarnation);
        }
        if let Err(error) = self.inner.progress.check() {
            if let Some(accepted_write) = accepted_write {
                accepted_write.finish(false);
            }
            return Err(error);
        }
        let inodes = batch_inodes(&batch);
        let result = self.apply_caught(cutoff, batch, accepted_write).await;
        if result.is_ok() {
            self.retire_inodes(&inodes);
        }
        result
    }

    pub(crate) async fn stop(&self) {
        if let Some(overlay) = self.inner.overlay.upgrade() {
            overlay.stop_admission();
            let _ = overlay.shutdown().await;
        }
        let senders = {
            let mut lanes = lock(&self.inner.lanes);
            lanes.closed = true;
            lanes
                .entries
                .drain()
                .map(|(_, lane)| lane.sender)
                .collect::<Vec<_>>()
        };
        drop(senders);
        self.inner.workers.close();
        self.inner.workers.wait().await;
    }

    fn enqueue_lane(self: &Arc<Self>, inode: u64, job: LaneJob) -> Result<(), MutationError> {
        let mut lanes = lock(&self.inner.lanes);
        // TaskTracker::close() does not reject later spawns. This registry bit
        // is therefore the admission authority, checked under the same lock
        // that creates a lane and sends every job.
        if lanes.closed {
            return Err(MutationError::Closed);
        }
        if !lanes.entries.contains_key(&inode) {
            let generation = lanes.next_generation;
            lanes.next_generation = lanes
                .next_generation
                .checked_add(1)
                .ok_or_else(|| self.poison("materializer lane generation exhausted"))?;
            let (sender, receiver) = mpsc::unbounded_channel();
            lanes
                .entries
                .insert(inode, LaneEntry { generation, sender });
            drop(self.inner.workers.spawn(spawn_lane(
                Arc::clone(self),
                inode,
                generation,
                receiver,
            )));
        }
        let send_result = lanes
            .entries
            .get(&inode)
            .expect("lane inserted")
            .sender
            .send(job);
        if send_result.is_err() {
            lanes.entries.remove(&inode);
            drop(lanes);
            return Err(self.poison("inode worker dropped"));
        }
        #[cfg(test)]
        self.inner.lane_jobs_enqueued.fetch_add(1, Ordering::AcqRel);
        Ok(())
    }

    #[cfg(test)]
    fn interleave_next_hold_enqueues_for_test(&self) {
        let interleave = &self.inner.hold_enqueue_interleave;
        *lock(&interleave.second_attempted) = false;
        interleave.arrivals.store(0, Ordering::Release);
        interleave.enabled.store(true, Ordering::Release);
    }

    #[cfg(test)]
    fn hold_enqueue_interleave_position_for_test(&self) -> Option<usize> {
        let interleave = &self.inner.hold_enqueue_interleave;
        if !interleave.enabled.load(Ordering::Acquire) {
            return None;
        }
        let position = interleave.arrivals.fetch_add(1, Ordering::AcqRel);
        if position == 1 {
            interleave.enabled.store(false, Ordering::Release);
            *lock(&interleave.second_attempted) = true;
            interleave.changed.notify_all();
        }
        Some(position)
    }

    #[cfg(test)]
    pub(crate) fn lifecycle_census_for_test(&self) -> MaterializerLifecycleCensus {
        let lanes = lock(&self.inner.lanes).entries.len();
        let workers = self.inner.workers.len();
        MaterializerLifecycleCensus {
            lanes,
            worker_handles: workers,
            active_workers: workers,
        }
    }

    #[cfg(test)]
    pub(crate) fn lane_jobs_enqueued_for_test(&self) -> usize {
        self.inner.lane_jobs_enqueued.load(Ordering::Acquire)
    }

    #[cfg(test)]
    pub(crate) fn pause_next_idle_retirement_for_test(
        &self,
        inode: u64,
    ) -> (oneshot::Receiver<()>, oneshot::Sender<()>) {
        let (reached_tx, reached_rx) = oneshot::channel();
        let (resume_tx, resume_rx) = oneshot::channel();
        *lock(&self.inner.idle_retirement_pause) = Some(IdleRetirementPause {
            inode,
            reached: reached_tx,
            resume: resume_rx,
        });
        (reached_rx, resume_tx)
    }

    #[cfg(test)]
    async fn pause_before_idle_retirement_for_test(&self, inode: u64) {
        let pause = {
            let mut pause = lock(&self.inner.idle_retirement_pause);
            if pause.as_ref().is_some_and(|pause| pause.inode == inode) {
                pause.take()
            } else {
                None
            }
        };
        if let Some(pause) = pause {
            let _ = pause.reached.send(());
            let _ = pause.resume.await;
        }
    }

    async fn apply_caught(
        &self,
        cutoff: MutationCutoff,
        batch: PreparedWriteBatch,
        accepted_write: Option<AcceptedWriteLifecycle>,
    ) -> Result<(), MutationError> {
        let apply = AssertUnwindSafe(self.apply_batch(cutoff, batch)).catch_unwind();
        let result = match apply.await {
            Ok(Ok(())) => self.record_success(cutoff),
            Ok(Err(error)) => {
                let message = error.to_string();
                Err(self.poison(message))
            }
            Err(_) => Err(self.poison("materializer worker panicked")),
        };
        if let Some(accepted_write) = accepted_write {
            accepted_write.finish(result.is_ok());
        }
        result
    }

    async fn apply_batch(
        &self,
        cutoff: MutationCutoff,
        mut batch: PreparedWriteBatch,
    ) -> Result<(), MutationError> {
        if let Some(hook) = &self.inner.apply_hook {
            return hook(cutoff, batch).await;
        }
        let Some(fs) = self.inner.fs.upgrade() else {
            return Err(MutationError::Closed);
        };
        apply_prepared_batch(&fs.write_apply_context(), &mut batch)
            .await
            .map(|_| ())
            .map_err(|error| MutationError::Poisoned(error.to_string()))
    }

    fn record_success(&self, cutoff: MutationCutoff) -> Result<(), MutationError> {
        self.inner.progress.record_materialized(cutoff)
    }

    fn retire_inodes(&self, inodes: &BTreeSet<u64>) {
        for inode in inodes {
            self.retire_inode(*inode);
        }
    }

    fn retire_inode(&self, inode: u64) {
        if let Some(overlay) = self.inner.overlay.upgrade() {
            overlay.retire_inode(inode);
        }
    }

    fn poison(&self, message: impl Into<String>) -> MutationError {
        let message = message.into();
        self.inner.progress.poison(message.clone());
        if let Some(overlay) = self.inner.overlay.upgrade() {
            overlay.freeze_terminal();
        }
        MutationError::Poisoned(message)
    }
}

async fn spawn_lane(
    materializer: Arc<Materializer>,
    inode: u64,
    generation: u64,
    mut receiver: mpsc::UnboundedReceiver<LaneJob>,
) {
    let mut next = receiver.recv().await;
    while let Some(job) = next {
        run_lane_job(&materializer, inode, job).await;
        #[cfg(test)]
        if receiver.is_empty() {
            materializer
                .pause_before_idle_retirement_for_test(inode)
                .await;
        }
        next = next_lane_job_or_retire(&materializer, inode, generation, &mut receiver);
    }
}

async fn run_lane_job(materializer: &Materializer, _inode: u64, job: LaneJob) {
    match job {
        #[cfg(test)]
        LaneJob::Apply {
            cutoff,
            batch,
            accepted_write,
            reply,
        } => {
            let result = materializer
                .apply_caught(cutoff, batch, accepted_write)
                .await;
            if result.is_ok() {
                // Canonical apply, attribute-preview retirement, and the reply
                // form one lane-owned completion. A new generation cannot be
                // installed before this tail finishes.
                materializer.retire_inode(_inode);
            }
            let _ = reply.send(result);
        }
        #[cfg(test)]
        LaneJob::Hold { acquired, release } => {
            let _ = acquired.send(());
            let _ = release.await;
        }
        LaneJob::VolatileMember {
            fs,
            dispatch,
            runtime,
            sequence,
        } => {
            let shutdown = runtime.shutdown_token();
            let lifecycle = AssertUnwindSafe(async {
                tokio::select! {
                    biased;
                    _ = shutdown.cancelled() => {
                        dispatch.fail_terminal();
                        runtime.fail(sequence, OverlayError::IoError);
                        Err(OverlayError::IoError)
                    }
                    result = async {
                        match dispatch.arrive_held(fs, materializer).await {
                            Ok(()) => {
                                runtime.complete(sequence).await;
                                Ok(())
                            }
                            Err(error) => {
                                runtime.fail(sequence, error);
                                Err(error)
                            }
                        }
                    } => result,
                }
            })
            .catch_unwind()
            .await;
            match lifecycle {
                Ok(Ok(())) => {}
                Ok(Err(_)) => {
                    materializer.poison("volatile member materialization failed");
                }
                Err(_) => {
                    dispatch.fail_terminal();
                    let _ = std::panic::catch_unwind(AssertUnwindSafe(|| {
                        runtime.fail(sequence, OverlayError::IoError);
                    }));
                    materializer.poison("volatile member lifecycle panicked");
                }
            }
        }
        #[cfg(test)]
        LaneJob::Probe { label, observed } => {
            let _ = observed.send(label);
        }
    }
}

fn batch_inodes(batch: &PreparedWriteBatch) -> BTreeSet<u64> {
    let mut inodes = batch
        .members
        .iter()
        .map(|member| member.id)
        .collect::<BTreeSet<_>>();
    if let Some(replayed) = &batch.replayed {
        inodes.extend(replayed.members.iter().map(|(id, _)| *id));
    }
    inodes
}

fn next_lane_job_or_retire(
    materializer: &Materializer,
    inode: u64,
    generation: u64,
    receiver: &mut mpsc::UnboundedReceiver<LaneJob>,
) -> Option<LaneJob> {
    let mut lanes = lock(&materializer.inner.lanes);
    match receiver.try_recv() {
        Ok(job) => Some(job),
        Err(mpsc::error::TryRecvError::Empty | mpsc::error::TryRecvError::Disconnected) => {
            if lanes
                .entries
                .get(&inode)
                .is_some_and(|lane| lane.generation == generation)
            {
                lanes.entries.remove(&inode);
            }
            None
        }
    }
}

fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|error| error.into_inner())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fs::mutation::config::{
        ClientDurabilityTarget, FilesystemWriteAckMode, FilesystemWriteAckSettings,
        FilesystemWriteAckSource,
    };
    use crate::fs::mutation::types::{PrepareWriteMember, PrepareWriteRequest};
    use crate::fs::ops::write::prepare_write;
    use crate::fs::test_util::test_creds;
    use crate::fs::types::{AuthContext, SetAttributes};
    use bytes::Bytes;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;
    use tokio::sync::Notify;

    fn cutoff(incarnation: MutationIncarnation, sequence: u64) -> MutationCutoff {
        MutationCutoff {
            mutation_incarnation: incarnation,
            sequence,
        }
    }

    fn volatile_settings() -> FilesystemWriteAckSettings {
        FilesystemWriteAckSettings {
            mode: FilesystemWriteAckMode::VolatileMemory,
            volatile_memory_bytes: 8 * 1024 * 1024,
            volatile_max_operations: 1024,
            source: FilesystemWriteAckSource::Filesystem,
            client_durability_target: ClientDurabilityTarget::LocalSsd,
        }
    }

    async fn receive_probe(receiver: &mut mpsc::UnboundedReceiver<&'static str>) -> &'static str {
        tokio::time::timeout(Duration::from_secs(2), receiver.recv())
            .await
            .expect("probe did not run")
            .expect("probe observer dropped")
    }

    async fn filesystem() -> (Arc<ZeroFS>, AuthContext) {
        let mut fs = ZeroFS::new_in_memory().await.unwrap();
        fs.write_ack = volatile_settings();
        let fs = Arc::new(fs);
        fs.install_volatile_overlay();
        (fs, AuthContext::from(&test_creds()))
    }

    async fn create_file(fs: &ZeroFS, _auth: &AuthContext, name: &[u8]) -> u64 {
        fs.create(&test_creds(), 0, name, &SetAttributes::default())
            .await
            .unwrap()
            .0
    }

    async fn prepare(
        fs: &ZeroFS,
        auth: AuthContext,
        id: u64,
        data: &'static [u8],
    ) -> PreparedWriteBatch {
        let mut batch = prepare_write(
            &fs.write_prepare_context(),
            PrepareWriteRequest {
                members: vec![PrepareWriteMember {
                    id,
                    offset: 0,
                    data: Bytes::from_static(data),
                }],
                auth,
                op_id: [0u8; 16],
                check_permissions: true,
            },
        )
        .await
        .unwrap();
        batch.guards = None;
        batch
    }

    async fn prepare_with_op_id(
        fs: &ZeroFS,
        auth: AuthContext,
        id: u64,
        data: &'static [u8],
        op_id: crate::dedup::OpId,
    ) -> PreparedWriteBatch {
        let mut batch = prepare_write(
            &fs.write_prepare_context(),
            PrepareWriteRequest {
                members: vec![PrepareWriteMember {
                    id,
                    offset: 0,
                    data: Bytes::from_static(data),
                }],
                auth,
                op_id,
                check_permissions: true,
            },
        )
        .await
        .unwrap();
        batch.guards = None;
        batch
    }

    async fn prepare_striped(
        fs: &ZeroFS,
        auth: AuthContext,
        first: u64,
        second: u64,
    ) -> PreparedWriteBatch {
        prepare_write(
            &fs.write_prepare_context(),
            PrepareWriteRequest {
                members: vec![
                    PrepareWriteMember {
                        id: first,
                        offset: 0,
                        data: Bytes::from_static(b"AAAA"),
                    },
                    PrepareWriteMember {
                        id: second,
                        offset: 0,
                        data: Bytes::from_static(b"BBBB"),
                    },
                ],
                auth,
                op_id: [1u8; 16],
                check_permissions: true,
            },
        )
        .await
        .unwrap()
    }

    #[tokio::test]
    async fn same_inode_applies_fifo() {
        let (fs, auth) = filesystem().await;
        let inode = create_file(&fs, &auth, b"fifo.txt").await;
        let order = Arc::new(Mutex::new(Vec::new()));
        let incarnation = MutationIncarnation::new();
        let hook_order = Arc::clone(&order);
        let hook: ApplyHook = Arc::new(move |cutoff, _batch| {
            let order = Arc::clone(&hook_order);
            Box::pin(async move {
                order.lock().unwrap().push(("start", cutoff.sequence));
                tokio::time::sleep(Duration::from_millis(40)).await;
                order.lock().unwrap().push(("end", cutoff.sequence));
                Ok(())
            })
        });
        let materializer = Materializer::start_with_hook(
            incarnation,
            Arc::downgrade(&fs),
            fs.volatile_overlay.get().cloned(),
            Some(hook),
        );

        let first = tokio::spawn({
            let materializer = Arc::clone(&materializer);
            let batch = prepare(&fs, auth.clone(), inode, b"one").await;
            async move {
                materializer
                    .dispatch_through(cutoff(incarnation, 1), batch)
                    .await
            }
        });
        tokio::time::sleep(Duration::from_millis(10)).await;
        let second = tokio::spawn({
            let materializer = Arc::clone(&materializer);
            let batch = prepare(&fs, auth.clone(), inode, b"two").await;
            async move {
                materializer
                    .dispatch_through(cutoff(incarnation, 2), batch)
                    .await
            }
        });

        first.await.unwrap().unwrap();
        second.await.unwrap().unwrap();
        assert_eq!(
            *order.lock().unwrap(),
            vec![("start", 1), ("end", 1), ("start", 2), ("end", 2)]
        );
        materializer.stop().await;
    }

    #[tokio::test]
    async fn cancelled_dispatch_waiter_does_not_abandon_accepted_write_lifecycle() {
        let (fs, auth) = filesystem().await;
        let inode = create_file(&fs, &auth, b"cancelled-waiter.txt").await;
        let op_id = [0x71; 16];
        let fingerprint = [0x19; 32];
        let accepted_write = fs.dedup.begin_accepted_write(
            crate::dedup::DedupEntry {
                op_id,
                result: crate::dedup::DedupResult::Write {
                    attrs: crate::fs::types::FileAttributes::default(),
                },
            },
            fingerprint,
            4,
        );
        let entered = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let hook: ApplyHook = {
            let entered = Arc::clone(&entered);
            let release = Arc::clone(&release);
            Arc::new(move |_cutoff, _batch| {
                let entered = Arc::clone(&entered);
                let release = Arc::clone(&release);
                Box::pin(async move {
                    entered.notify_one();
                    release.notified().await;
                    Ok(())
                })
            })
        };
        let incarnation = MutationIncarnation::new();
        let materializer = Materializer::start_with_hook(
            incarnation,
            Arc::downgrade(&fs),
            fs.volatile_overlay.get().cloned(),
            Some(hook),
        );
        let waiter = tokio::spawn({
            let materializer = Arc::clone(&materializer);
            let batch = prepare_with_op_id(&fs, auth, inode, b"data", op_id).await;
            async move {
                materializer
                    .dispatch_accepted_through(cutoff(incarnation, 1), batch, accepted_write)
                    .await
            }
        });
        tokio::time::timeout(Duration::from_secs(2), entered.notified())
            .await
            .unwrap();
        waiter.abort();
        assert!(waiter.await.unwrap_err().is_cancelled());
        assert!(fs.dedup.accepted_write_pending(&op_id));

        release.notify_one();
        materializer
            .progress()
            .wait_materialized(cutoff(incarnation, 1))
            .await
            .unwrap();
        assert!(!fs.dedup.accepted_write_pending(&op_id));
        assert_eq!(
            fs.dedup.replay_write(&op_id, fingerprint),
            Some(crate::dedup::WriteReplay::Match { count: 4 })
        );
        materializer.stop().await;
    }

    #[tokio::test]
    async fn failed_materializer_job_retracts_accepted_write_result() {
        let (fs, auth) = filesystem().await;
        let inode = create_file(&fs, &auth, b"failed-job.txt").await;
        let op_id = [0x72; 16];
        let fingerprint = [0x29; 32];
        let accepted_write = fs.dedup.begin_accepted_write(
            crate::dedup::DedupEntry {
                op_id,
                result: crate::dedup::DedupResult::Write {
                    attrs: crate::fs::types::FileAttributes::default(),
                },
            },
            fingerprint,
            4,
        );
        let hook: ApplyHook = Arc::new(move |_cutoff, _batch| {
            Box::pin(async move { Err(MutationError::Poisoned("injected failure".into())) })
        });
        let incarnation = MutationIncarnation::new();
        let materializer = Materializer::start_with_hook(
            incarnation,
            Arc::downgrade(&fs),
            fs.volatile_overlay.get().cloned(),
            Some(hook),
        );
        let batch = prepare_with_op_id(&fs, auth, inode, b"data", op_id).await;
        assert!(
            materializer
                .dispatch_accepted_through(cutoff(incarnation, 1), batch, accepted_write)
                .await
                .is_err()
        );
        assert!(!fs.dedup.accepted_write_pending(&op_id));
        assert_eq!(fs.dedup.replay_write(&op_id, fingerprint), None);
        materializer.stop().await;
    }

    #[tokio::test]
    async fn different_inodes_apply_concurrently() {
        let (fs, auth) = filesystem().await;
        let first_id = create_file(&fs, &auth, b"a.txt").await;
        let second_id = create_file(&fs, &auth, b"b.txt").await;
        let entered = Arc::new(AtomicUsize::new(0));
        let release = Arc::new(Notify::new());
        let incarnation = MutationIncarnation::new();
        let hook: ApplyHook = {
            let entered = Arc::clone(&entered);
            let release = Arc::clone(&release);
            Arc::new(move |_cutoff, _batch| {
                let entered = Arc::clone(&entered);
                let release = Arc::clone(&release);
                Box::pin(async move {
                    entered.fetch_add(1, Ordering::SeqCst);
                    release.notified().await;
                    Ok(())
                })
            })
        };
        let materializer =
            Materializer::start_with_hook(incarnation, Arc::downgrade(&fs), None, Some(hook));

        let first = tokio::spawn({
            let materializer = Arc::clone(&materializer);
            let batch = prepare(&fs, auth.clone(), first_id, b"aaa").await;
            async move {
                materializer
                    .dispatch_through(cutoff(incarnation, 1), batch)
                    .await
            }
        });
        let second = tokio::spawn({
            let materializer = Arc::clone(&materializer);
            let batch = prepare(&fs, auth.clone(), second_id, b"bbb").await;
            async move {
                materializer
                    .dispatch_through(cutoff(incarnation, 2), batch)
                    .await
            }
        });

        tokio::time::timeout(Duration::from_secs(2), async {
            while entered.load(Ordering::SeqCst) < 2 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("distinct inodes must be in flight together");
        release.notify_waiters();
        first.await.unwrap().unwrap();
        second.await.unwrap().unwrap();
        materializer.stop().await;
    }

    #[tokio::test]
    async fn held_apply_rejects_stale_incarnation_before_canonical_apply() {
        let (fs, auth) = filesystem().await;
        let inode = create_file(&fs, &auth, b"stale-held.txt").await;
        let apply_calls = Arc::new(AtomicUsize::new(0));
        let incarnation = MutationIncarnation::new();
        let hook: ApplyHook = {
            let apply_calls = Arc::clone(&apply_calls);
            Arc::new(move |_cutoff, _batch| {
                apply_calls.fetch_add(1, Ordering::SeqCst);
                Box::pin(async { Ok(()) })
            })
        };
        let materializer = Materializer::start_with_hook(
            incarnation,
            Arc::downgrade(&fs),
            fs.volatile_overlay.get().cloned(),
            Some(hook),
        );
        let batch = prepare(&fs, auth, inode, b"stale").await;

        let error = materializer
            .apply_held(cutoff(MutationIncarnation::new(), 1), batch, None)
            .await
            .expect_err("stale held apply must be rejected");
        assert!(matches!(error, MutationError::StaleIncarnation));
        assert_eq!(apply_calls.load(Ordering::SeqCst), 0);
        materializer.stop().await;
    }

    #[tokio::test]
    async fn striped_batch_completes_after_all_members() {
        let (fs, auth) = filesystem().await;
        let first_id = create_file(&fs, &auth, b"stripe0.bin").await;
        let second_id = create_file(&fs, &auth, b"stripe1.bin").await;
        let seen = Arc::new(Mutex::new(Vec::new()));
        let incarnation = MutationIncarnation::new();
        let hook: ApplyHook = {
            let seen = Arc::clone(&seen);
            Arc::new(move |_cutoff, batch| {
                let seen = Arc::clone(&seen);
                Box::pin(async move {
                    let members = batch
                        .members
                        .iter()
                        .map(|member| member.id)
                        .collect::<Vec<_>>();
                    seen.lock().unwrap().push(members);
                    Ok(())
                })
            })
        };
        let materializer =
            Materializer::start_with_hook(incarnation, Arc::downgrade(&fs), None, Some(hook));
        let mut batch = prepare_striped(&fs, auth, first_id, second_id).await;
        batch.guards = None;
        materializer
            .dispatch_through(cutoff(incarnation, 1), batch)
            .await
            .unwrap();
        let seen = seen.lock().unwrap().clone();
        assert_eq!(seen.len(), 1, "striped batch is one apply");
        assert_eq!(seen[0].len(), 2);
        assert_eq!(materializer.progress().materialized_through(), 1);
        materializer.stop().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn overlapping_striped_batches_cannot_split_lane_holds() {
        let (fs, auth) = filesystem().await;
        let first_id = create_file(&fs, &auth, b"overlap0.bin").await;
        let second_id = create_file(&fs, &auth, b"overlap1.bin").await;
        let incarnation = MutationIncarnation::new();
        let hook: ApplyHook = Arc::new(|_cutoff, _batch| Box::pin(async { Ok(()) }));
        let materializer =
            Materializer::start_with_hook(incarnation, Arc::downgrade(&fs), None, Some(hook));
        let mut first_batch = prepare_striped(&fs, auth.clone(), first_id, second_id).await;
        first_batch.guards = None;
        let mut second_batch = prepare_striped(&fs, auth, first_id, second_id).await;
        second_batch.guards = None;
        materializer.interleave_next_hold_enqueues_for_test();
        let start = Arc::new(tokio::sync::Barrier::new(3));

        let first = tokio::spawn({
            let materializer = Arc::clone(&materializer);
            let start = Arc::clone(&start);
            async move {
                start.wait().await;
                materializer
                    .dispatch_through(cutoff(incarnation, 1), first_batch)
                    .await
            }
        });
        let second = tokio::spawn({
            let materializer = Arc::clone(&materializer);
            let start = Arc::clone(&start);
            async move {
                start.wait().await;
                materializer
                    .dispatch_through(cutoff(incarnation, 2), second_batch)
                    .await
            }
        });
        start.wait().await;

        tokio::time::timeout(Duration::from_secs(1), async {
            first.await.unwrap().unwrap();
            second.await.unwrap().unwrap();
        })
        .await
        .expect("overlapping striped batches split their inode holds and deadlocked");
        materializer.stop().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn single_enqueue_cannot_split_striped_volatile_group() {
        let incarnation = MutationIncarnation::new();
        let materializer = Materializer::start_with_hook(
            incarnation,
            Weak::new(),
            None,
            Some(Arc::new(|_, _| Box::pin(async { Ok(()) }))),
        );
        let enqueue_order = Arc::new(Mutex::new(Vec::new()));
        let (shared_tx, mut shared_rx) = mpsc::unbounded_channel();
        let (other_tx, mut other_rx) = mpsc::unbounded_channel();
        let (first_member_tx, first_member_rx) = std::sync::mpsc::channel();
        let (single_attempt_tx, single_attempt_rx) = std::sync::mpsc::channel();

        let striped = tokio::task::spawn_blocking({
            let materializer = Arc::clone(&materializer);
            let enqueue_order = Arc::clone(&enqueue_order);
            let shared_tx = shared_tx.clone();
            move || {
                materializer.with_volatile_enqueue_gate(|| {
                    enqueue_order.lock().unwrap().push("striped-shared");
                    materializer
                        .enqueue_probe(71, "striped-shared", shared_tx)
                        .unwrap();
                    first_member_tx.send(()).unwrap();
                    single_attempt_rx
                        .recv_timeout(Duration::from_secs(2))
                        .expect("single enqueue did not race striped group");
                    enqueue_order.lock().unwrap().push("striped-other");
                    materializer
                        .enqueue_probe(72, "striped-other", other_tx)
                        .unwrap();
                });
            }
        });
        let single = tokio::task::spawn_blocking({
            let materializer = Arc::clone(&materializer);
            let enqueue_order = Arc::clone(&enqueue_order);
            move || {
                first_member_rx
                    .recv_timeout(Duration::from_secs(2))
                    .expect("striped enqueue did not start");
                single_attempt_tx.send(()).unwrap();
                materializer.with_volatile_enqueue_gate(|| {
                    enqueue_order.lock().unwrap().push("single-shared");
                    materializer
                        .enqueue_probe(71, "single-shared", shared_tx)
                        .unwrap();
                });
            }
        });

        tokio::time::timeout(Duration::from_secs(2), async {
            striped.await.unwrap();
            single.await.unwrap();
        })
        .await
        .expect("mixed volatile enqueue race deadlocked");
        assert_eq!(
            *enqueue_order.lock().unwrap(),
            ["striped-shared", "striped-other", "single-shared"]
        );
        assert_eq!(receive_probe(&mut shared_rx).await, "striped-shared");
        assert_eq!(receive_probe(&mut shared_rx).await, "single-shared");
        assert_eq!(receive_probe(&mut other_rx).await, "striped-other");
        materializer.stop().await;
    }

    #[tokio::test]
    async fn global_prefix_waits_for_gap() {
        let (fs, auth) = filesystem().await;
        let first_id = create_file(&fs, &auth, b"gap1.txt").await;
        let second_id = create_file(&fs, &auth, b"gap2.txt").await;
        let release_first = Arc::new(Notify::new());
        let second_done = Arc::new(Notify::new());
        let incarnation = MutationIncarnation::new();
        let hook: ApplyHook = {
            let release_first = Arc::clone(&release_first);
            let second_done = Arc::clone(&second_done);
            Arc::new(move |cutoff, _batch| {
                let release_first = Arc::clone(&release_first);
                let second_done = Arc::clone(&second_done);
                Box::pin(async move {
                    if cutoff.sequence == 1 {
                        release_first.notified().await;
                    } else {
                        second_done.notify_waiters();
                    }
                    Ok(())
                })
            })
        };
        let materializer =
            Materializer::start_with_hook(incarnation, Arc::downgrade(&fs), None, Some(hook));

        let later = tokio::spawn({
            let materializer = Arc::clone(&materializer);
            let batch = prepare(&fs, auth.clone(), second_id, b"two").await;
            async move {
                materializer
                    .dispatch_through(cutoff(incarnation, 2), batch)
                    .await
            }
        });
        tokio::time::timeout(Duration::from_secs(2), second_done.notified())
            .await
            .expect("sequence 2 must apply without waiting for 1");
        assert_eq!(materializer.progress().materialized_through(), 0);

        let waiter = tokio::spawn({
            let progress = materializer.progress();
            async move { progress.wait_materialized(cutoff(incarnation, 2)).await }
        });
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(!waiter.is_finished());

        let earlier = tokio::spawn({
            let materializer = Arc::clone(&materializer);
            let batch = prepare(&fs, auth.clone(), first_id, b"one").await;
            async move {
                materializer
                    .dispatch_through(cutoff(incarnation, 1), batch)
                    .await
            }
        });
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(!waiter.is_finished());
        release_first.notify_waiters();
        earlier.await.unwrap().unwrap();
        later.await.unwrap().unwrap();
        tokio::time::timeout(Duration::from_secs(1), waiter)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(materializer.progress().materialized_through(), 2);
        materializer.stop().await;
    }

    #[tokio::test]
    async fn overlay_retires_after_canonical_visibility() {
        let (fs, auth) = filesystem().await;
        let inode = create_file(&fs, &auth, b"retire.txt").await;
        let overlay = fs.volatile_overlay.get().cloned().expect("overlay");
        let batch = prepare_write(
            &fs.write_prepare_context(),
            PrepareWriteRequest {
                members: vec![PrepareWriteMember {
                    id: inode,
                    offset: 0,
                    data: Bytes::from_static(b"visible"),
                }],
                auth: auth.clone(),
                op_id: [2u8; 16],
                check_permissions: true,
            },
        )
        .await
        .unwrap();
        let attrs = batch.members[0].post_attrs.clone();
        overlay.preview_attrs(inode, attrs.clone());
        assert_eq!(overlay.visible_size(inode, 0), attrs.size);

        let incarnation = MutationIncarnation::new();
        let materializer =
            Materializer::start(incarnation, Arc::downgrade(&fs), Some(Arc::clone(&overlay)));
        materializer
            .dispatch_through(cutoff(incarnation, 1), batch)
            .await
            .unwrap();

        let persisted = fs.inode_store.get(inode).await.unwrap();
        let canonical = match persisted {
            crate::fs::inode::Inode::File(file) => file.size,
            other => panic!("expected file, got {other:?}"),
        };
        assert_eq!(canonical, attrs.size);
        assert_eq!(
            overlay.visible_size(inode, canonical),
            canonical,
            "overlay must retire after canonical apply owns the bytes"
        );
        let (data, _) = fs.read_file(&auth, inode, 0, 16).await.unwrap();
        assert_eq!(data.as_ref(), b"visible");
        materializer.stop().await;
    }

    #[tokio::test]
    async fn panic_poison_retains_frozen_view() {
        let (fs, auth) = filesystem().await;
        let inode = create_file(&fs, &auth, b"poison.txt").await;
        let overlay = fs.volatile_overlay.get().cloned().expect("overlay");
        let batch = prepare(&fs, auth.clone(), inode, b"frozen").await;
        let attrs = batch.members[0].post_attrs.clone();
        overlay.preview_attrs(inode, attrs.clone());

        let incarnation = MutationIncarnation::new();
        let hook: ApplyHook = Arc::new(|_cutoff, _batch| {
            Box::pin(async move {
                panic!("canonical apply exploded");
            })
        });
        let materializer = Materializer::start_with_hook(
            incarnation,
            Arc::downgrade(&fs),
            Some(Arc::clone(&overlay)),
            Some(hook),
        );
        let error = materializer
            .dispatch_through(cutoff(incarnation, 1), batch)
            .await
            .expect_err("panic must poison");
        assert!(matches!(error, MutationError::Poisoned(_)));
        assert!(overlay.is_frozen());
        assert_eq!(
            overlay.visible_size(inode, 0),
            attrs.size,
            "poison must keep the last coherent overlay view"
        );
        let later = prepare(&fs, auth, inode, b"later").await;
        let closed = materializer
            .dispatch_through(cutoff(incarnation, 2), later)
            .await
            .expect_err("poisoned materializer rejects later work");
        assert!(matches!(closed, MutationError::Poisoned(_)));
        materializer.stop().await;
    }

    #[tokio::test]
    async fn stop_joins_worker_loops() {
        let (fs, auth) = filesystem().await;
        let inode = create_file(&fs, &auth, b"stop.txt").await;
        let entered = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let incarnation = MutationIncarnation::new();
        let hook: ApplyHook = {
            let entered = Arc::clone(&entered);
            let release = Arc::clone(&release);
            Arc::new(move |_cutoff, _batch| {
                let entered = Arc::clone(&entered);
                let release = Arc::clone(&release);
                Box::pin(async move {
                    entered.notify_waiters();
                    release.notified().await;
                    Ok(())
                })
            })
        };
        let materializer =
            Materializer::start_with_hook(incarnation, Arc::downgrade(&fs), None, Some(hook));
        let dispatched = tokio::spawn({
            let materializer = Arc::clone(&materializer);
            let batch = prepare(&fs, auth, inode, b"stop").await;
            async move {
                materializer
                    .dispatch_through(cutoff(incarnation, 1), batch)
                    .await
            }
        });
        tokio::time::timeout(Duration::from_secs(2), entered.notified())
            .await
            .unwrap();
        let stopping = tokio::spawn({
            let materializer = Arc::clone(&materializer);
            async move { materializer.stop().await }
        });
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(
            !stopping.is_finished(),
            "stop must join the in-flight worker, not detach it"
        );
        release.notify_waiters();
        tokio::time::timeout(Duration::from_secs(2), stopping)
            .await
            .expect("stop must join")
            .unwrap();
        dispatched.await.unwrap().unwrap();
        assert!(
            materializer.inner.workers.is_empty(),
            "stop must take and join every worker handle"
        );
    }
}
