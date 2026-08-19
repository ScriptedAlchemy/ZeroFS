use crate::db::Db;
#[cfg(feature = "failpoints")]
use crate::failpoints::{self as fp, fail_point};
use crate::fs::errors::FsError;
use crate::fs::mutation::durability::{
    DurabilityError, DurabilityReceipt, DurabilityTarget, ObjectCoverage,
};
use crate::fs::mutation::types::MutationCutoff;
use crate::task::spawn_named;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, OnceLock};
use tokio::sync::{Mutex, mpsc, oneshot};
use tokio::task::{AbortHandle, JoinHandle};

/// Pre-flush hook: seals the data-plane open segment (PUT) before the metadata
/// memtable is flushed, so a durable manifest never references an un-PUT segment.
type SealHook =
    Arc<dyn Fn() -> Pin<Box<dyn Future<Output = Result<(), FsError>> + Send>> + Send + Sync>;
type LocalDurabilityHook =
    Arc<dyn Fn() -> Pin<Box<dyn Future<Output = Result<(), FsError>> + Send>> + Send + Sync>;
type MaterializeHook = Arc<
    dyn Fn(MutationCutoff) -> Pin<Box<dyn Future<Output = Result<(), DurabilityError>> + Send>>
        + Send
        + Sync,
>;
type CaptureHook = Arc<dyn Fn() -> ObjectCoverage + Send + Sync>;
type ObjectWaitHook = Arc<
    dyn Fn(
            ObjectCoverage,
            DurabilityTarget,
        ) -> Pin<Box<dyn Future<Output = Result<(), DurabilityError>> + Send>>
        + Send
        + Sync,
>;
type Reply = oneshot::Sender<Result<(), FsError>>;

/// Move-only evidence that the shared flush coordinator completed a durability
/// barrier. The private field keeps callers from manufacturing a receipt and
/// clearing an HA reconnect barrier without performing the flush.
#[must_use = "the receipt must discharge the HA base-flush requirement"]
pub(crate) struct FlushReceipt {
    _private: (),
}

#[cfg(test)]
impl FlushReceipt {
    pub(crate) const fn for_tests() -> Self {
        Self { _private: () }
    }
}

enum Request {
    Flush(Reply),
    Close(Reply),
}

#[derive(Clone)]
pub struct FlushCoordinator {
    sender: mpsc::UnboundedSender<Request>,
    seal_hook: Arc<OnceLock<SealHook>>,
    local_durability_hook: Arc<OnceLock<LocalDurabilityHook>>,
    materialize_hook: Arc<OnceLock<MaterializeHook>>,
    object_capture: Arc<OnceLock<CaptureHook>>,
    object_wait: Arc<OnceLock<ObjectWaitHook>>,
    db: Arc<Db>,
    worker: Arc<Mutex<Option<JoinHandle<()>>>>,
    worker_abort: AbortHandle,
    /// Test-only count of submitted flush requests. Unlike completed cycles,
    /// this advances before the worker can block acquiring the flush barrier.
    #[cfg(test)]
    requested_flushes: Arc<std::sync::atomic::AtomicU64>,
    /// Test-only count of successful coordinator flush cycles.
    #[cfg(test)]
    completed_flushes: Arc<std::sync::atomic::AtomicU64>,
}

impl FlushCoordinator {
    pub fn new(db: Arc<Db>) -> Self {
        let seal_hook: Arc<OnceLock<SealHook>> = Arc::new(OnceLock::new());
        let hook = Arc::clone(&seal_hook);
        let local_durability_hook: Arc<OnceLock<LocalDurabilityHook>> = Arc::new(OnceLock::new());
        let local_durability = Arc::clone(&local_durability_hook);
        let materialize_hook: Arc<OnceLock<MaterializeHook>> = Arc::new(OnceLock::new());
        let object_capture: Arc<OnceLock<CaptureHook>> = Arc::new(OnceLock::new());
        let object_wait: Arc<OnceLock<ObjectWaitHook>> = Arc::new(OnceLock::new());
        let (sender, mut receiver) = mpsc::unbounded_channel::<Request>();
        #[cfg(test)]
        let requested_flushes = Arc::new(std::sync::atomic::AtomicU64::new(0));
        #[cfg(test)]
        let completed_flushes = Arc::new(std::sync::atomic::AtomicU64::new(0));
        #[cfg(test)]
        let flush_counter = Arc::clone(&completed_flushes);

        let worker_db = Arc::clone(&db);
        let worker = spawn_named("flush-coordinator", async move {
            while let Some(request) = receiver.recv().await {
                let mut pending_senders = Vec::new();
                let mut closer = None;
                match request {
                    Request::Flush(sender) => pending_senders.push(sender),
                    Request::Close(sender) => closer = Some(sender),
                }
                while closer.is_none() {
                    match receiver.try_recv() {
                        Ok(Request::Flush(sender)) => pending_senders.push(sender),
                        Ok(Request::Close(sender)) => closer = Some(sender),
                        Err(_) => break,
                    }
                }

                // A close keeps the barrier through db.close(), leaving no gap
                // in which a FrameLoc can commit after the final seal.
                let barrier = worker_db.flush_barrier().write_owned().await;
                let sealed = match hook.get() {
                    Some(seal) => match seal().await {
                        Ok(()) => {
                            #[cfg(feature = "failpoints")]
                            fail_point!(fp::FLUSH_AFTER_SEAL_BEFORE_MANIFEST);
                            worker_db.flush().await.map_err(|_| FsError::IoError)
                        }
                        Err(e) => Err(e),
                    },
                    None => worker_db.flush().await.map_err(|_| FsError::IoError),
                };
                let close_result = if closer.is_some() && sealed.is_ok() {
                    worker_db.mark_closing();
                    worker_db.close().await.map_err(|_| FsError::IoError)
                } else {
                    sealed
                };
                #[cfg(test)]
                if sealed.is_ok() {
                    flush_counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                }
                drop(barrier);
                let should_wait = if closer.is_some() {
                    close_result.is_ok()
                } else {
                    sealed.is_ok()
                };
                let result = if should_wait {
                    match local_durability.get() {
                        Some(wait_local) => wait_local().await,
                        None => Ok(()),
                    }
                } else {
                    sealed
                };
                let close_result = close_result.and(result);

                #[cfg(feature = "failpoints")]
                fail_point!(fp::FLUSH_AFTER_COMPLETE);

                for sender in pending_senders.drain(..) {
                    let _ = sender.send(result);
                }
                if let Some(closer) = closer {
                    let _ = closer.send(close_result);
                    while let Ok(request) = receiver.try_recv() {
                        match request {
                            Request::Flush(sender) | Request::Close(sender) => {
                                let _ = sender.send(Err(FsError::ShuttingDown));
                            }
                        }
                    }
                    return;
                }
            }
        });

        let worker_abort = worker.abort_handle();
        Self {
            sender,
            seal_hook,
            local_durability_hook,
            materialize_hook,
            object_capture,
            object_wait,
            db,
            worker: Arc::new(Mutex::new(Some(worker))),
            worker_abort,
            #[cfg(test)]
            requested_flushes,
            #[cfg(test)]
            completed_flushes,
        }
    }

    /// Install the pre-flush seal hook (first call wins). Set once at bring-up,
    /// after the data plane is constructed.
    pub fn set_sealer(&self, hook: SealHook) {
        let _ = self.seal_hook.set(hook);
    }

    /// Install the post-flush local durability barrier (first call wins).
    ///
    /// The worker invokes it only after the data segment is sealed and SlateDB
    /// flushes its metadata. Writeback uses this point to capture the newest
    /// accepted object mutation and wait until the SSD journal covers it.
    pub fn set_local_durability_barrier(&self, hook: LocalDurabilityHook) {
        let _ = self.local_durability_hook.set(hook);
    }

    pub(crate) fn set_materialize(&self, hook: MaterializeHook) {
        let _ = self.materialize_hook.set(hook);
    }

    pub(crate) fn set_object_capture(&self, hook: CaptureHook) {
        let _ = self.object_capture.set(hook);
    }

    pub(crate) fn set_object_wait(&self, hook: ObjectWaitHook) {
        let _ = self.object_wait.set(hook);
    }

    /// Materialize `cutoff`, then seal+flush+capture under the database
    /// barrier, then wait the requested object target after releasing it.
    pub(crate) async fn durable_through(
        &self,
        cutoff: MutationCutoff,
        target: DurabilityTarget,
    ) -> Result<DurabilityReceipt, DurabilityError> {
        if let Some(materialize) = self.materialize_hook.get() {
            materialize(cutoff).await?;
        }

        let coverage = self.capture_under_barrier().await?;
        self.wait_object(coverage, target).await?;
        Ok(DurabilityReceipt {
            mutation_cutoff: cutoff,
            object_coverage: coverage,
            target,
        })
    }

    async fn capture_under_barrier(&self) -> Result<ObjectCoverage, DurabilityError> {
        let _barrier = self.db.flush_barrier().write_owned().await;
        if let Some(seal) = self.seal_hook.get() {
            seal().await.map_err(DurabilityError::from_flush)?;
        }
        self.db
            .flush()
            .await
            .map_err(DurabilityError::FilesystemFlush)?;
        Ok(self
            .object_capture
            .get()
            .map(|capture| capture())
            .unwrap_or(ObjectCoverage::DirectRemote))
    }

    async fn wait_object(
        &self,
        coverage: ObjectCoverage,
        target: DurabilityTarget,
    ) -> Result<(), DurabilityError> {
        match self.object_wait.get() {
            Some(wait) => wait(coverage, target).await,
            None => {
                // Production writeback installs object_wait. Tests and
                // direct backends share the same local hook the flush
                // worker uses after metadata is durable.
                if let Some(wait_local) = self.local_durability_hook.get() {
                    wait_local().await.map_err(DurabilityError::from_flush)?;
                }
                match coverage {
                    ObjectCoverage::DirectRemote => Ok(()),
                    ObjectCoverage::Writeback { .. } => Err(DurabilityError::Closed),
                }
            }
        }
    }

    pub async fn flush(&self) -> Result<(), FsError> {
        let (tx, rx) = oneshot::channel();

        #[cfg(test)]
        self.requested_flushes
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        self.sender
            .send(Request::Flush(tx))
            .map_err(|_| FsError::ShuttingDown)?;

        rx.await.map_err(|_| FsError::ShuttingDown)?
    }

    /// Flush and return proof suitable for discharging an HA Solo-base barrier.
    pub(crate) async fn flush_with_receipt(&self) -> Result<FlushReceipt, FsError> {
        self.flush().await?;
        Ok(FlushReceipt { _private: () })
    }

    #[cfg(test)]
    pub(crate) fn requested_flush_count(&self) -> u64 {
        self.requested_flushes
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    #[cfg(test)]
    pub(crate) fn completed_flush_count(&self) -> u64 {
        self.completed_flushes
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Seal, flush, and close under one barrier write lock. On error, the
    /// caller must exit without closing the database separately.
    pub async fn close(&self) -> Result<(), FsError> {
        let (tx, rx) = oneshot::channel();

        let reply = match self.sender.send(Request::Close(tx)) {
            Ok(()) => rx.await.unwrap_or(Err(FsError::ShuttingDown)),
            Err(_) => Err(FsError::ShuttingDown),
        };
        let joined = self.join_worker().await;
        joined.and(reply)
    }

    async fn join_worker(&self) -> Result<(), FsError> {
        let mut worker = self.worker.lock().await;
        let Some(handle) = worker.as_mut() else {
            return Ok(());
        };
        let result = handle.await;
        worker.take();
        result.map_err(|_| FsError::IoError)
    }

    /// Abort and join the flush worker without fencing the database.
    ///
    /// Lifecycle close must stop the worker before taking the flush barrier so
    /// seal/flush/close can still run. `abort_close_worker` marks the database
    /// closing first, which makes a later `Db::flush` fail closed.
    pub(crate) async fn stop_worker(&self) -> Result<(), FsError> {
        // Abort before taking the mutex: a canceled close can leave its join
        // future holding that mutex while the worker itself is stuck.
        self.worker_abort.abort();
        let mut worker = self.worker.lock().await;
        let Some(handle) = worker.as_mut() else {
            return Ok(());
        };
        let result = handle.await;
        worker.take();
        match result {
            Ok(()) => Ok(()),
            Err(error) if error.is_cancelled() => Ok(()),
            Err(_) => Err(FsError::IoError),
        }
    }

    /// Stop and join the actual coordinator worker after its final close
    /// deadline expires. This is stronger than dropping the `close()` future,
    /// which only abandons its reply receiver while the worker keeps using the
    /// database and object store.
    pub async fn abort_close_worker(&self) -> Result<(), FsError> {
        self.db.mark_closing();
        self.stop_worker().await
    }
}

#[cfg(test)]
mod tests {
    use super::FlushCoordinator;
    use crate::fs::ZeroFS;
    use crate::fs::errors::FsError;
    use crate::fs::mutation::durability::{DurabilityError, DurabilityTarget, ObjectCoverage};
    use crate::fs::mutation::types::{MutationCutoff, MutationIncarnation};
    use std::sync::{Arc, Mutex};
    use std::time::Duration;
    use tokio::sync::Notify;
    use uuid::Uuid;

    fn cutoff(sequence: u64) -> MutationCutoff {
        MutationCutoff {
            mutation_incarnation: MutationIncarnation::new(),
            sequence,
        }
    }

    async fn isolated_coordinator() -> (ZeroFS, FlushCoordinator) {
        let fs = ZeroFS::new_in_memory().await.unwrap();
        let coordinator = FlushCoordinator::new(Arc::clone(&fs.db));
        (fs, coordinator)
    }

    #[tokio::test]
    async fn seal_and_flush_complete_before_object_capture() {
        let (_fs, coordinator) = isolated_coordinator().await;
        let order = Arc::new(Mutex::new(Vec::new()));
        coordinator.set_sealer({
            let order = Arc::clone(&order);
            Arc::new(move || {
                order.lock().unwrap().push("seal");
                Box::pin(async { Ok(()) })
            })
        });
        coordinator.set_object_capture({
            let order = Arc::clone(&order);
            Arc::new(move || {
                order.lock().unwrap().push("capture");
                ObjectCoverage::DirectRemote
            })
        });

        let _receipt = coordinator
            .durable_through(cutoff(0), DurabilityTarget::RemoteBackend)
            .await
            .unwrap();
        assert_eq!(*order.lock().unwrap(), ["seal", "capture"]);
    }

    #[tokio::test]
    async fn object_capture_holds_the_flush_barrier() {
        let (fs, coordinator) = isolated_coordinator().await;
        let held = Arc::new(Mutex::new(false));
        coordinator.set_object_capture({
            let held = Arc::clone(&held);
            let barrier = fs.db.flush_barrier();
            Arc::new(move || {
                *held.lock().unwrap() = barrier.try_write().is_err();
                ObjectCoverage::DirectRemote
            })
        });
        let _receipt = coordinator
            .durable_through(cutoff(0), DurabilityTarget::RemoteBackend)
            .await
            .unwrap();
        assert!(
            *held.lock().unwrap(),
            "object capture must run while the database flush barrier is held"
        );
    }

    #[tokio::test]
    async fn object_wait_runs_after_barrier_release() {
        let (fs, coordinator) = isolated_coordinator().await;
        let released = Arc::new(Mutex::new(false));
        let entered = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        coordinator.set_object_capture(Arc::new(|| ObjectCoverage::Writeback {
            journal_incarnation: crate::fs::mutation::durability::JournalIncarnation::new(
                Uuid::nil(),
            ),
            sequence: 1,
        }));
        coordinator.set_object_wait({
            let released = Arc::clone(&released);
            let entered = Arc::clone(&entered);
            let release = Arc::clone(&release);
            let barrier = fs.db.flush_barrier();
            Arc::new(move |_, _| {
                let released = Arc::clone(&released);
                let entered = Arc::clone(&entered);
                let release = Arc::clone(&release);
                let barrier = Arc::clone(&barrier);
                Box::pin(async move {
                    *released.lock().unwrap() = barrier.try_write().is_ok();
                    entered.notify_one();
                    release.notified().await;
                    Ok(())
                })
            })
        });

        let mut wait = tokio::spawn({
            let coordinator = coordinator.clone();
            async move {
                coordinator
                    .durable_through(cutoff(1), DurabilityTarget::LocalSsd)
                    .await
            }
        });
        entered.notified().await;
        assert!(
            *released.lock().unwrap(),
            "object wait must run after the database flush barrier is released"
        );
        assert!(
            tokio::time::timeout(Duration::from_millis(25), &mut wait)
                .await
                .is_err(),
            "durable_through returned before the object wait finished"
        );
        release.notify_one();
        let _receipt = wait
            .await
            .expect("durable_through task panicked")
            .expect("durable_through failed");
    }

    #[tokio::test]
    async fn seal_failure_does_not_capture_or_wait() {
        let (_fs, coordinator) = isolated_coordinator().await;
        let captured = Arc::new(Mutex::new(false));
        let waited = Arc::new(Mutex::new(false));
        coordinator.set_sealer(Arc::new(|| Box::pin(async { Err(FsError::IoError) })));
        coordinator.set_object_capture({
            let captured = Arc::clone(&captured);
            Arc::new(move || {
                *captured.lock().unwrap() = true;
                ObjectCoverage::DirectRemote
            })
        });
        coordinator.set_object_wait({
            let waited = Arc::clone(&waited);
            Arc::new(move |_, _| {
                *waited.lock().unwrap() = true;
                Box::pin(async { Ok(()) })
            })
        });
        let result = coordinator
            .durable_through(cutoff(0), DurabilityTarget::RemoteBackend)
            .await;
        assert!(matches!(result, Err(DurabilityError::FilesystemFlush(_))));
        assert!(!*captured.lock().unwrap());
        assert!(!*waited.lock().unwrap());
    }

    #[tokio::test]
    async fn local_hook_waits_after_flush_barrier_release() {
        let (fs, coordinator) = isolated_coordinator().await;
        let released = Arc::new(Mutex::new(false));
        coordinator.set_local_durability_barrier({
            let released = Arc::clone(&released);
            let barrier = fs.db.flush_barrier();
            Arc::new(move || {
                let released = Arc::clone(&released);
                let barrier = Arc::clone(&barrier);
                Box::pin(async move {
                    *released.lock().unwrap() = barrier.try_write().is_ok();
                    Ok(())
                })
            })
        });
        coordinator.flush().await.unwrap();
        assert!(
            *released.lock().unwrap(),
            "the local durability hook must not hold the database flush barrier"
        );
    }
}
