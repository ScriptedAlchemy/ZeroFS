//! Sole owner of bounded unified-writeback shutdown.
//!
//! [`MutationLifecycle::close`] is the only process close path. Cancellation
//! and deadline expiry keep this owner alive and report the incomplete phase.

use crate::fs::mutation::durability::{DurabilityTarget, ObjectCoverage};
use crate::fs::mutation::types::MutationCutoff;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use tokio::sync::Notify;
use tokio::time::Instant;

type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;
type Step = Arc<dyn Fn() -> BoxFuture<'static, Result<(), ShutdownError>> + Send + Sync>;
type CutoffStep =
    Arc<dyn Fn() -> BoxFuture<'static, Result<MutationCutoff, ShutdownError>> + Send + Sync>;
type MaterializeStep =
    Arc<dyn Fn(MutationCutoff) -> BoxFuture<'static, Result<(), ShutdownError>> + Send + Sync>;
type BarrierStep =
    Arc<dyn Fn() -> BoxFuture<'static, Result<BarrierGuard, ShutdownError>> + Send + Sync>;
type CaptureStep = Arc<dyn Fn() -> ObjectCoverage + Send + Sync>;
type WaitStep = Arc<
    dyn Fn(ObjectCoverage, DurabilityTarget) -> BoxFuture<'static, Result<(), ShutdownError>>
        + Send
        + Sync,
>;

/// One phase of the only allowed shutdown order.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ShutdownPhase {
    StopListeners,
    DrainDispatched,
    CloseAdmission,
    Materialize,
    StopMutation,
    AcquireBarrier,
    CloseDatabase,
    CaptureObjects,
    ReleaseBarrier,
    WaitTarget,
    StopWriteback,
    StopSftp,
    Complete,
}

/// Move-only evidence that the lifecycle owner finished every phase.
#[must_use = "a shutdown receipt is proof that close completed"]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ShutdownReceipt {
    pub(crate) mutation_cutoff: MutationCutoff,
    pub(crate) object_coverage: ObjectCoverage,
    pub(crate) target: DurabilityTarget,
}

#[derive(Debug, Clone, thiserror::Error)]
pub(crate) enum ShutdownError {
    #[error("shutdown incomplete at {phase:?}")]
    Incomplete { phase: ShutdownPhase },
    #[error("shutdown failed at {phase:?}: {message}")]
    Failed {
        phase: ShutdownPhase,
        message: String,
    },
}

impl ShutdownError {
    fn failed(phase: ShutdownPhase, error: impl std::fmt::Display) -> Self {
        Self::Failed {
            phase,
            message: error.to_string(),
        }
    }
}

/// Held while the database is sealed, flushed, and closed.
pub(crate) struct BarrierGuard {
    release: Option<Box<dyn FnOnce() + Send>>,
}

impl BarrierGuard {
    pub(crate) fn new(release: impl FnOnce() + Send + 'static) -> Self {
        Self {
            release: Some(Box::new(release)),
        }
    }
}

impl Drop for BarrierGuard {
    fn drop(&mut self) {
        if let Some(release) = self.release.take() {
            release();
        }
    }
}

/// In-flight protocol calls that must drain before the mutation cutoff.
#[derive(Debug, Default)]
pub(crate) struct DispatchedCalls {
    inner: Mutex<DispatchedState>,
    notify: Notify,
}

#[derive(Debug, Default)]
struct DispatchedState {
    inflight: u64,
    closed: bool,
}

impl DispatchedCalls {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    pub(crate) fn begin(&self) -> Result<DispatchedGuard<'_>, ShutdownError> {
        let mut state = self.inner.lock().expect("dispatched calls");
        if state.closed {
            return Err(ShutdownError::failed(
                ShutdownPhase::DrainDispatched,
                "lifecycle already closed admission",
            ));
        }
        state.inflight += 1;
        Ok(DispatchedGuard { calls: self })
    }

    fn finish(&self) {
        {
            let mut state = self.inner.lock().expect("dispatched calls");
            state.inflight = state.inflight.saturating_sub(1);
        }
        self.notify.notify_waiters();
    }

    pub(crate) async fn drain(&self) -> Result<(), ShutdownError> {
        {
            self.inner.lock().expect("dispatched calls").closed = true;
        }
        loop {
            {
                let state = self.inner.lock().expect("dispatched calls");
                if state.inflight == 0 {
                    return Ok(());
                }
            }
            self.notify.notified().await;
        }
    }
}

pub(crate) struct DispatchedGuard<'a> {
    calls: &'a DispatchedCalls,
}

impl Drop for DispatchedGuard<'_> {
    fn drop(&mut self) {
        self.calls.finish();
    }
}

/// Injectable owners consumed by the single close path.
#[derive(Clone)]
pub(crate) struct LifecycleOwners {
    pub(crate) stop_listeners: Step,
    pub(crate) stop_admission: Step,
    pub(crate) drain_dispatched: Step,
    pub(crate) close_admission: CutoffStep,
    pub(crate) materialize: MaterializeStep,
    pub(crate) acquire_barrier: BarrierStep,
    pub(crate) close_database: Step,
    pub(crate) capture_objects: CaptureStep,
    pub(crate) wait_target: WaitStep,
    pub(crate) stop_mutation: Step,
    pub(crate) stop_writeback: Step,
    pub(crate) stop_sftp: Step,
}

/// Process-wide close owner. The first `close` starts the owner; later
/// callers and cancelled waiters join that same owner.
pub(crate) struct MutationLifecycle {
    owners: LifecycleOwners,
    phase: Mutex<ShutdownPhase>,
    started: Mutex<bool>,
    result: Mutex<Option<Result<ShutdownReceipt, ShutdownError>>>,
    done: Notify,
}

impl MutationLifecycle {
    pub(crate) fn new(owners: LifecycleOwners) -> Arc<Self> {
        Arc::new(Self {
            owners,
            phase: Mutex::new(ShutdownPhase::StopListeners),
            started: Mutex::new(false),
            result: Mutex::new(None),
            done: Notify::new(),
        })
    }

    pub(crate) fn phase(&self) -> ShutdownPhase {
        *self.phase.lock().expect("lifecycle phase")
    }

    fn set_phase(&self, phase: ShutdownPhase) {
        *self.phase.lock().expect("lifecycle phase") = phase;
    }

    fn store_result(&self, result: Result<ShutdownReceipt, ShutdownError>) {
        *self.result.lock().expect("lifecycle result") = Some(result);
        self.done.notify_waiters();
    }

    fn result_snapshot(&self) -> Option<Result<ShutdownReceipt, ShutdownError>> {
        self.result.lock().expect("lifecycle result").clone()
    }

    fn ensure_owner(self: &Arc<Self>, target: DurabilityTarget) {
        let mut started = self.started.lock().expect("lifecycle owner");
        if *started {
            return;
        }
        *started = true;
        let owner = Arc::clone(self);
        tokio::task::spawn(async move {
            let result = owner.run_close(target).await;
            owner.store_result(result);
        });
    }

    pub(crate) async fn close(
        self: Arc<Self>,
        deadline: Instant,
        target: DurabilityTarget,
    ) -> Result<ShutdownReceipt, ShutdownError> {
        self.ensure_owner(target);
        loop {
            if let Some(result) = self.result_snapshot() {
                return result;
            }
            tokio::select! {
                _ = self.done.notified() => {}
                _ = tokio::time::sleep_until(deadline) => {
                    if let Some(result) = self.result_snapshot() {
                        return result;
                    }
                    return Err(ShutdownError::Incomplete {
                        phase: self.phase(),
                    });
                }
            }
        }
    }

    async fn run_close(&self, target: DurabilityTarget) -> Result<ShutdownReceipt, ShutdownError> {
        self.run_step(ShutdownPhase::StopListeners, &self.owners.stop_listeners)
            .await?;
        self.run_step(ShutdownPhase::StopListeners, &self.owners.stop_admission)
            .await?;
        self.run_step(
            ShutdownPhase::DrainDispatched,
            &self.owners.drain_dispatched,
        )
        .await?;
        self.set_phase(ShutdownPhase::CloseAdmission);
        let mutation_cutoff = (self.owners.close_admission)()
            .await
            .map_err(|error| annotate(error, ShutdownPhase::CloseAdmission))?;
        self.set_phase(ShutdownPhase::Materialize);
        (self.owners.materialize)(mutation_cutoff)
            .await
            .map_err(|error| annotate(error, ShutdownPhase::Materialize))?;
        self.run_step(ShutdownPhase::StopMutation, &self.owners.stop_mutation)
            .await?;
        self.set_phase(ShutdownPhase::AcquireBarrier);
        let barrier = (self.owners.acquire_barrier)()
            .await
            .map_err(|error| annotate(error, ShutdownPhase::AcquireBarrier))?;
        self.run_step(ShutdownPhase::CloseDatabase, &self.owners.close_database)
            .await?;
        self.set_phase(ShutdownPhase::CaptureObjects);
        let object_coverage = (self.owners.capture_objects)();
        self.set_phase(ShutdownPhase::ReleaseBarrier);
        drop(barrier);
        self.set_phase(ShutdownPhase::WaitTarget);
        (self.owners.wait_target)(object_coverage, target)
            .await
            .map_err(|error| annotate(error, ShutdownPhase::WaitTarget))?;
        self.run_step(ShutdownPhase::StopWriteback, &self.owners.stop_writeback)
            .await?;
        self.run_step(ShutdownPhase::StopSftp, &self.owners.stop_sftp)
            .await?;
        self.set_phase(ShutdownPhase::Complete);
        Ok(ShutdownReceipt {
            mutation_cutoff,
            object_coverage,
            target,
        })
    }

    async fn run_step(&self, phase: ShutdownPhase, step: &Step) -> Result<(), ShutdownError> {
        self.set_phase(phase);
        step().await.map_err(|error| annotate(error, phase))
    }
}

fn annotate(error: ShutdownError, phase: ShutdownPhase) -> ShutdownError {
    match error {
        ShutdownError::Incomplete { .. } => ShutdownError::Incomplete { phase },
        ShutdownError::Failed { message, .. } => ShutdownError::Failed { phase, message },
    }
}

impl LifecycleOwners {
    pub(crate) fn for_process(
        shutdown: tokio_util::sync::CancellationToken,
        dispatched: Arc<DispatchedCalls>,
        fs: Arc<crate::fs::ZeroFS>,
        writeback: Option<crate::writeback::store::WritebackObjectStore>,
        sftp: Option<crate::sftp_transport::SftpSessionPool>,
        read_only: bool,
    ) -> Self {
        let target_fs = Arc::clone(&fs);
        let writeback_for_capture = writeback.clone();
        let writeback_for_wait = writeback.clone();
        Self {
            stop_listeners: {
                let shutdown = shutdown.clone();
                Arc::new(move || {
                    let shutdown = shutdown.clone();
                    Box::pin(async move {
                        shutdown.cancel();
                        Ok(())
                    })
                })
            },
            stop_admission: {
                let fs = Arc::clone(&fs);
                Arc::new(move || {
                    let fs = Arc::clone(&fs);
                    Box::pin(async move {
                        fs.stop_new_mutation_admission();
                        if let Some(overlay) = fs.volatile_overlay.get() {
                            overlay.freeze_terminal();
                        }
                        Ok(())
                    })
                })
            },
            drain_dispatched: {
                let dispatched = Arc::clone(&dispatched);
                Arc::new(move || {
                    let dispatched = Arc::clone(&dispatched);
                    Box::pin(async move { dispatched.drain().await })
                })
            },
            close_admission: {
                let fs = Arc::clone(&fs);
                Arc::new(move || {
                    let fs = Arc::clone(&fs);
                    Box::pin(async move { Ok(crate::fs::mutation::closed_admission_cutoff(&fs)) })
                })
            },
            materialize: {
                let fs = Arc::clone(&fs);
                Arc::new(move |cutoff| {
                    let fs = Arc::clone(&fs);
                    Box::pin(async move {
                        fs.materialize_through_cutoff(cutoff)
                            .await
                            .map_err(|error| {
                                ShutdownError::failed(ShutdownPhase::Materialize, error)
                            })
                    })
                })
            },
            acquire_barrier: {
                let fs = Arc::clone(&fs);
                Arc::new(move || {
                    let fs = Arc::clone(&fs);
                    Box::pin(async move { acquire_flush_barrier(&fs).await })
                })
            },
            close_database: {
                let fs = Arc::clone(&fs);
                let sftp = sftp.clone();
                Arc::new(move || {
                    let fs = Arc::clone(&fs);
                    let sftp = sftp.clone();
                    Box::pin(async move { close_database(&fs, sftp.as_ref(), read_only).await })
                })
            },
            capture_objects: Arc::new(move || match &writeback_for_capture {
                Some(store) => store.object_coverage(),
                None => ObjectCoverage::DirectRemote,
            }),
            wait_target: {
                Arc::new(move |coverage, target| {
                    let writeback = writeback_for_wait.clone();
                    Box::pin(async move {
                        wait_object_target(writeback.as_ref(), coverage, target).await
                    })
                })
            },
            stop_mutation: {
                let fs = Arc::clone(&target_fs);
                Arc::new(move || {
                    let fs = Arc::clone(&fs);
                    Box::pin(async move {
                        fs.stop_mutation_workers().await.map_err(|error| {
                            ShutdownError::failed(ShutdownPhase::StopMutation, error)
                        })
                    })
                })
            },
            stop_writeback: {
                let writeback = writeback.clone();
                Arc::new(move || {
                    let writeback = writeback.clone();
                    Box::pin(async move {
                        if let Some(store) = writeback {
                            store.shutdown().await.map_err(|error| {
                                ShutdownError::failed(ShutdownPhase::StopWriteback, error)
                            })?;
                        }
                        Ok(())
                    })
                })
            },
            stop_sftp: {
                let sftp = sftp.clone();
                Arc::new(move || {
                    let sftp = sftp.clone();
                    Box::pin(async move {
                        if let Some(pool) = sftp {
                            pool.shutdown().await.map_err(|error| {
                                ShutdownError::failed(ShutdownPhase::StopSftp, error)
                            })?;
                        }
                        Ok(())
                    })
                })
            },
        }
    }
}

async fn acquire_flush_barrier(fs: &crate::fs::ZeroFS) -> Result<BarrierGuard, ShutdownError> {
    let _ = fs.flush_coordinator.stop_worker().await;
    let guard = fs.db.flush_barrier().write_owned().await;
    Ok(BarrierGuard::new(move || drop(guard)))
}

async fn close_database(
    fs: &crate::fs::ZeroFS,
    sftp: Option<&crate::sftp_transport::SftpSessionPool>,
    read_only: bool,
) -> Result<(), ShutdownError> {
    let closing = async {
        if read_only {
            fs.db
                .close()
                .await
                .map_err(|_| crate::fs::errors::FsError::IoError)
        } else {
            fs.close_canonical_database().await
        }
    };
    match sftp {
        Some(pool) => close_database_with_sftp(closing, pool).await,
        None => closing
            .await
            .map_err(|error| ShutdownError::failed(ShutdownPhase::CloseDatabase, error)),
    }
}

async fn close_database_with_sftp<F>(
    closing: F,
    pool: &crate::sftp_transport::SftpSessionPool,
) -> Result<(), ShutdownError>
where
    F: Future<Output = Result<(), crate::fs::errors::FsError>> + Send,
{
    let timeout = super::SFTP_FINAL_DATABASE_CLOSE_TIMEOUT;
    let mut closing = Box::pin(closing);
    match tokio::time::timeout(timeout, &mut closing).await {
        Ok(result) => {
            result.map_err(|error| ShutdownError::failed(ShutdownPhase::CloseDatabase, error))
        }
        Err(_) => {
            pool.begin_shutdown();
            match tokio::time::timeout(timeout, closing).await {
                Ok(result) => result
                    .map_err(|error| ShutdownError::failed(ShutdownPhase::CloseDatabase, error)),
                Err(_) => Err(ShutdownError::failed(
                    ShutdownPhase::CloseDatabase,
                    format!(
                        "SFTP-backed database close did not finish within {}s",
                        timeout.as_secs()
                    ),
                )),
            }
        }
    }
}

async fn wait_object_target(
    writeback: Option<&crate::writeback::store::WritebackObjectStore>,
    coverage: ObjectCoverage,
    target: DurabilityTarget,
) -> Result<(), ShutdownError> {
    match (writeback, coverage, target) {
        (
            Some(store),
            ObjectCoverage::Writeback {
                journal_incarnation,
                sequence,
            },
            DurabilityTarget::LocalSsd,
        ) => store
            .wait_local_coverage(journal_incarnation.as_uuid(), sequence)
            .await
            .map_err(|error| ShutdownError::failed(ShutdownPhase::WaitTarget, error)),
        (
            Some(store),
            ObjectCoverage::Writeback {
                journal_incarnation,
                sequence,
            },
            DurabilityTarget::RemoteBackend,
        ) => store
            .wait_remote_coverage(journal_incarnation.as_uuid(), sequence)
            .await
            .map_err(|error| ShutdownError::failed(ShutdownPhase::WaitTarget, error)),
        (_, ObjectCoverage::DirectRemote, _) => Ok(()),
        (None, ObjectCoverage::Writeback { .. }, _) => Err(ShutdownError::failed(
            ShutdownPhase::WaitTarget,
            "writeback coverage without a writeback owner",
        )),
    }
}

fn record_step(order: &Arc<Mutex<Vec<&'static str>>>, name: &'static str) -> Step {
    let order = Arc::clone(order);
    Arc::new(move || {
        let order = Arc::clone(&order);
        Box::pin(async move {
            order.lock().expect("order").push(name);
            Ok(())
        })
    })
}

fn blocked_step(
    order: &Arc<Mutex<Vec<&'static str>>>,
    name: &'static str,
    entered: &Arc<Notify>,
    release: &Arc<Notify>,
) -> Step {
    let order = Arc::clone(order);
    let entered = Arc::clone(entered);
    let release = Arc::clone(release);
    Arc::new(move || {
        let order = Arc::clone(&order);
        let entered = Arc::clone(&entered);
        let release = Arc::clone(&release);
        Box::pin(async move {
            order.lock().expect("order").push(name);
            entered.notify_one();
            release.notified().await;
            Ok(())
        })
    })
}

#[cfg(test)]
#[path = "mutation_lifecycle_materialized_tests.rs"]
mod materialized_tests;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fs::ZeroFS;
    use crate::fs::mutation::durability::JournalIncarnation;
    use crate::fs::mutation::types::MutationIncarnation;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::Duration;
    use uuid::Uuid;

    fn cutoff(sequence: u64) -> MutationCutoff {
        MutationCutoff {
            mutation_incarnation: MutationIncarnation::new(),
            sequence,
        }
    }

    fn coverage(sequence: u64) -> ObjectCoverage {
        ObjectCoverage::Writeback {
            journal_incarnation: JournalIncarnation::new(Uuid::nil()),
            sequence,
        }
    }

    fn owners_with(order: &Arc<Mutex<Vec<&'static str>>>) -> LifecycleOwners {
        let captured = cutoff(7);
        LifecycleOwners {
            stop_listeners: record_step(order, "listeners"),
            stop_admission: record_step(order, "admission_stop"),
            drain_dispatched: record_step(order, "drain"),
            close_admission: {
                let order = Arc::clone(order);
                Arc::new(move || {
                    let order = Arc::clone(&order);
                    Box::pin(async move {
                        order.lock().expect("order").push("cutoff");
                        Ok(captured)
                    })
                })
            },
            materialize: {
                let order = Arc::clone(order);
                Arc::new(move |_| {
                    let order = Arc::clone(&order);
                    Box::pin(async move {
                        order.lock().expect("order").push("materialize");
                        Ok(())
                    })
                })
            },
            acquire_barrier: {
                let order = Arc::clone(order);
                Arc::new(move || {
                    let order = Arc::clone(&order);
                    Box::pin(async move {
                        order.lock().expect("order").push("barrier");
                        Ok(BarrierGuard::new(|| {}))
                    })
                })
            },
            close_database: record_step(order, "db_close"),
            capture_objects: {
                let order = Arc::clone(order);
                Arc::new(move || {
                    order.lock().expect("order").push("capture");
                    coverage(7)
                })
            },
            wait_target: {
                let order = Arc::clone(order);
                Arc::new(move |_, _| {
                    let order = Arc::clone(&order);
                    Box::pin(async move {
                        order.lock().expect("order").push("wait");
                        Ok(())
                    })
                })
            },
            stop_mutation: record_step(order, "mutation"),
            stop_writeback: record_step(order, "writeback"),
            stop_sftp: record_step(order, "sftp"),
        }
    }

    async fn close_ok(owners: LifecycleOwners) -> ShutdownReceipt {
        let lifecycle = MutationLifecycle::new(owners);
        lifecycle
            .close(
                Instant::now() + Duration::from_secs(2),
                DurabilityTarget::LocalSsd,
            )
            .await
            .expect("close")
    }

    #[tokio::test]
    async fn close_stops_listeners_before_admission() {
        let order = Arc::new(Mutex::new(Vec::new()));
        let _receipt = close_ok(owners_with(&order)).await;
        let order = order.lock().expect("order");
        let listeners = pos(&order, "listeners");
        let admission = pos(&order, "admission_stop");
        assert!(
            listeners < admission,
            "listeners must stop before admission: {order:?}"
        );
    }

    #[tokio::test]
    async fn close_drains_dispatched_calls_before_cutoff() {
        let calls = DispatchedCalls::new();
        let entered = Arc::new(Notify::new());
        let _release = Arc::new(Notify::new());
        let order = Arc::new(Mutex::new(Vec::new()));
        let guard = calls.begin().expect("begin dispatched");
        let mut owners = owners_with(&order);
        owners.drain_dispatched = {
            let calls = Arc::clone(&calls);
            let order = Arc::clone(&order);
            let entered = Arc::clone(&entered);
            Arc::new(move || {
                let calls = Arc::clone(&calls);
                let order = Arc::clone(&order);
                let entered = Arc::clone(&entered);
                Box::pin(async move {
                    order.lock().expect("order").push("drain");
                    entered.notify_one();
                    calls.drain().await
                })
            })
        };
        let lifecycle = MutationLifecycle::new(owners);
        let mut closing = tokio::spawn({
            let lifecycle = Arc::clone(&lifecycle);
            async move {
                lifecycle
                    .close(
                        Instant::now() + Duration::from_secs(2),
                        DurabilityTarget::RemoteBackend,
                    )
                    .await
            }
        });
        entered.notified().await;
        assert!(
            tokio::time::timeout(Duration::from_millis(25), &mut closing)
                .await
                .is_err(),
            "cutoff must wait for dispatched drain"
        );
        drop(guard);
        let receipt = closing.await.expect("join").expect("close");
        let order = order.lock().expect("order");
        assert!(pos(&order, "drain") < pos(&order, "cutoff"), "{order:?}");
        assert_eq!(receipt.target, DurabilityTarget::RemoteBackend);
    }

    #[tokio::test]
    async fn close_captures_objects_emitted_by_db_close() {
        let accepted = Arc::new(AtomicU64::new(3));
        let order = Arc::new(Mutex::new(Vec::new()));
        let mut owners = owners_with(&order);
        owners.close_database = {
            let accepted = Arc::clone(&accepted);
            let order = Arc::clone(&order);
            Arc::new(move || {
                let accepted = Arc::clone(&accepted);
                let order = Arc::clone(&order);
                Box::pin(async move {
                    accepted.store(11, Ordering::SeqCst);
                    order.lock().expect("order").push("db_close");
                    Ok(())
                })
            })
        };
        owners.capture_objects = {
            let accepted = Arc::clone(&accepted);
            let order = Arc::clone(&order);
            Arc::new(move || {
                order.lock().expect("order").push("capture");
                coverage(accepted.load(Ordering::SeqCst))
            })
        };
        let receipt = close_ok(owners).await;
        assert_eq!(receipt.object_coverage, coverage(11));
        let order = order.lock().expect("order");
        assert!(
            pos(&order, "db_close") < pos(&order, "capture"),
            "{order:?}"
        );
    }

    #[tokio::test]
    async fn barrier_blocks_crossing_writes_during_db_close() {
        let fs = ZeroFS::new_in_memory().await.unwrap();
        let entered = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let held = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let order = Arc::new(Mutex::new(Vec::new()));
        let mut owners = owners_with(&order);
        owners.acquire_barrier = {
            let barrier = fs.db.flush_barrier();
            let order = Arc::clone(&order);
            Arc::new(move || {
                let barrier = std::sync::Arc::clone(&barrier);
                let order = Arc::clone(&order);
                Box::pin(async move {
                    order.lock().expect("order").push("barrier");
                    let guard = barrier.write_owned().await;
                    Ok(BarrierGuard::new(move || drop(guard)))
                })
            })
        };
        owners.close_database = {
            let barrier = fs.db.flush_barrier();
            let entered = Arc::clone(&entered);
            let release = Arc::clone(&release);
            let held = Arc::clone(&held);
            let order = Arc::clone(&order);
            Arc::new(move || {
                let barrier = std::sync::Arc::clone(&barrier);
                let entered = Arc::clone(&entered);
                let release = Arc::clone(&release);
                let held = Arc::clone(&held);
                let order = Arc::clone(&order);
                Box::pin(async move {
                    order.lock().expect("order").push("db_close");
                    held.store(barrier.try_write().is_err(), Ordering::SeqCst);
                    entered.notify_one();
                    release.notified().await;
                    Ok(())
                })
            })
        };
        let lifecycle = MutationLifecycle::new(owners);
        let closing = tokio::spawn({
            let lifecycle = Arc::clone(&lifecycle);
            async move {
                lifecycle
                    .close(
                        Instant::now() + Duration::from_secs(2),
                        DurabilityTarget::LocalSsd,
                    )
                    .await
            }
        });
        entered.notified().await;
        assert!(
            held.load(Ordering::SeqCst),
            "crossing writes must not take the flush barrier during db close"
        );
        release.notify_one();
        let _receipt = closing.await.expect("join").expect("close");
    }

    #[tokio::test]
    async fn close_stops_mutation_before_writeback_before_sftp() {
        let order = Arc::new(Mutex::new(Vec::new()));
        let _receipt = close_ok(owners_with(&order)).await;
        let order = order.lock().expect("order");
        assert!(
            pos(&order, "mutation") < pos(&order, "writeback"),
            "{order:?}"
        );
        assert!(pos(&order, "writeback") < pos(&order, "sftp"), "{order:?}");
    }

    #[tokio::test]
    async fn close_drains_mutation_workers_before_acquiring_database_barrier() {
        let order = Arc::new(Mutex::new(Vec::new()));
        let _receipt = close_ok(owners_with(&order)).await;
        let order = order.lock().expect("order");
        assert!(
            pos(&order, "mutation") < pos(&order, "barrier"),
            "{order:?}"
        );
    }

    #[tokio::test]
    async fn cancelled_close_retains_owner() {
        let order = Arc::new(Mutex::new(Vec::new()));
        let entered = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let mut owners = owners_with(&order);
        owners.materialize = {
            let order = Arc::clone(&order);
            let entered = Arc::clone(&entered);
            let release = Arc::clone(&release);
            Arc::new(move |_| {
                let order = Arc::clone(&order);
                let entered = Arc::clone(&entered);
                let release = Arc::clone(&release);
                Box::pin(async move {
                    order.lock().expect("order").push("materialize");
                    entered.notify_one();
                    release.notified().await;
                    Ok(())
                })
            })
        };
        let lifecycle = MutationLifecycle::new(owners);
        let first = tokio::spawn({
            let lifecycle = Arc::clone(&lifecycle);
            async move {
                lifecycle
                    .close(
                        Instant::now() + Duration::from_secs(5),
                        DurabilityTarget::LocalSsd,
                    )
                    .await
            }
        });
        entered.notified().await;
        first.abort();
        assert!(first.await.unwrap_err().is_cancelled());
        assert_eq!(lifecycle.phase(), ShutdownPhase::Materialize);
        assert!(
            lifecycle.result_snapshot().is_none(),
            "cancelled waiter must not finish or restart the owner"
        );
        release.notify_one();
        let receipt = Arc::clone(&lifecycle)
            .close(
                Instant::now() + Duration::from_secs(2),
                DurabilityTarget::LocalSsd,
            )
            .await
            .expect("replacement close");
        assert_eq!(receipt.target, DurabilityTarget::LocalSsd);
        assert_eq!(lifecycle.phase(), ShutdownPhase::Complete);
    }

    #[tokio::test]
    async fn timeout_reports_incomplete_phase() {
        let order = Arc::new(Mutex::new(Vec::new()));
        let entered = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let mut owners = owners_with(&order);
        owners.close_database = blocked_step(&order, "db_close", &entered, &release);
        let lifecycle = MutationLifecycle::new(owners);
        let first = tokio::spawn({
            let lifecycle = Arc::clone(&lifecycle);
            async move {
                lifecycle
                    .close(
                        Instant::now() + Duration::from_millis(50),
                        DurabilityTarget::RemoteBackend,
                    )
                    .await
            }
        });
        entered.notified().await;
        let error = first.await.expect("join").expect_err("deadline");
        assert!(
            matches!(
                error,
                ShutdownError::Incomplete {
                    phase: ShutdownPhase::CloseDatabase
                }
            ),
            "timeout must name the incomplete phase, got {error:?}"
        );
        assert_eq!(lifecycle.phase(), ShutdownPhase::CloseDatabase);
        assert!(lifecycle.result_snapshot().is_none());
        release.notify_one();
        let _receipt = Arc::clone(&lifecycle)
            .close(
                Instant::now() + Duration::from_secs(2),
                DurabilityTarget::RemoteBackend,
            )
            .await
            .expect("owner completed after timeout");
    }

    fn pos(order: &[&'static str], name: &'static str) -> usize {
        order
            .iter()
            .position(|step| *step == name)
            .unwrap_or_else(|| panic!("{name} missing from {order:?}"))
    }
}
