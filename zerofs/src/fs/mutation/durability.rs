//! Typed filesystem durability cutoffs and receipts.
//!
//! Protocols convert the resolved [`ClientDurabilityTarget`] with `.into()`
//! and never choose SSD or remote themselves. A receipt is returned only
//! after mutation materialization, conservative object capture, and the
//! requested local or remote wait all succeed.

// WIP on develop: landed but not fully wired into every protocol yet.
#![allow(dead_code)]

use crate::fs::errors::FsError;
use crate::fs::mutation::config::ClientDurabilityTarget;
use crate::fs::mutation::types::MutationCutoff;
use crate::writeback::WritebackError;
use crate::writeback::journaler::LocalBarrierError;
use crate::writeback::model::Sequence;
use crate::writeback::remote::RemoteBarrierError;
use uuid::Uuid;

/// Object-writeback journal boot. Distinct from [`crate::fs::mutation::types::MutationIncarnation`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct JournalIncarnation(Uuid);

impl JournalIncarnation {
    pub(crate) fn new(id: Uuid) -> Self {
        Self(id)
    }

    pub(crate) fn as_uuid(self) -> Uuid {
        self.0
    }
}

/// Where a durability wait must complete.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DurabilityTarget {
    LocalSsd,
    RemoteBackend,
}

impl From<ClientDurabilityTarget> for DurabilityTarget {
    fn from(target: ClientDurabilityTarget) -> Self {
        match target {
            ClientDurabilityTarget::LocalSsd => Self::LocalSsd,
            ClientDurabilityTarget::RemoteBackend => Self::RemoteBackend,
        }
    }
}

/// Conservative object-store coverage captured under the filesystem flush barrier.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ObjectCoverage {
    DirectRemote,
    Writeback {
        journal_incarnation: JournalIncarnation,
        sequence: Sequence,
    },
}

/// Move-only evidence that a cutoff reached its requested durability target.
#[must_use = "a durability receipt is proof that the cutoff completed"]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct DurabilityReceipt {
    pub(crate) mutation_cutoff: MutationCutoff,
    pub(crate) object_coverage: ObjectCoverage,
    pub(crate) target: DurabilityTarget,
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum DurabilityError {
    #[error("stale mutation incarnation")]
    StaleMutationIncarnation,
    #[error("stale journal incarnation")]
    StaleJournalIncarnation,
    #[error("mutation materialization failed: {0}")]
    Materialization(#[source] FsError),
    #[error("filesystem flush failed: {0}")]
    FilesystemFlush(#[source] anyhow::Error),
    #[error("object durability failed: {0}")]
    Object(#[source] WritebackError),
    #[error("durability wait closed before target")]
    Closed,
}

impl DurabilityError {
    pub(crate) fn from_flush(error: FsError) -> Self {
        if error == FsError::ShuttingDown {
            Self::Closed
        } else {
            Self::FilesystemFlush(anyhow::anyhow!("{error}"))
        }
    }

    pub(crate) fn from_local(error: LocalBarrierError) -> Self {
        match error {
            LocalBarrierError::StaleIncarnation => Self::StaleJournalIncarnation,
            LocalBarrierError::Closed => Self::Closed,
            error => Self::Object(WritebackError::Local(error)),
        }
    }

    pub(crate) fn from_remote(error: RemoteBarrierError) -> Self {
        match error {
            RemoteBarrierError::StaleIncarnation => Self::StaleJournalIncarnation,
            RemoteBarrierError::Closed => Self::Closed,
            error => Self::Object(WritebackError::Remote(error)),
        }
    }

    pub(crate) fn from_materialization(error: crate::fs::mutation::types::MutationError) -> Self {
        use crate::fs::mutation::types::MutationError;
        match error {
            MutationError::StaleIncarnation => Self::StaleMutationIncarnation,
            MutationError::Closed => Self::Closed,
            MutationError::TooLarge { .. } => Self::Materialization(FsError::NoSpace),
            MutationError::Backpressure => Self::Materialization(FsError::RetryLater),
            MutationError::Poisoned(_) => Self::Materialization(FsError::IoError),
        }
    }
}

impl From<DurabilityError> for FsError {
    fn from(error: DurabilityError) -> Self {
        match error {
            DurabilityError::StaleMutationIncarnation
            | DurabilityError::StaleJournalIncarnation => FsError::StaleHandle,
            DurabilityError::Closed => FsError::ShuttingDown,
            DurabilityError::Materialization(error) => error,
            DurabilityError::FilesystemFlush(_) | DurabilityError::Object(_) => FsError::IoError,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fs::ZeroFS;
    use crate::fs::flush_coordinator::FlushCoordinator;
    use crate::fs::mutation::types::MutationIncarnation;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};
    use std::time::Duration;
    use tokio::sync::Notify;

    fn cutoff(incarnation: MutationIncarnation, sequence: u64) -> MutationCutoff {
        MutationCutoff {
            mutation_incarnation: incarnation,
            sequence,
        }
    }

    fn writeback_coverage() -> ObjectCoverage {
        ObjectCoverage::Writeback {
            journal_incarnation: JournalIncarnation::new(Uuid::nil()),
            sequence: 1,
        }
    }

    async fn isolated_coordinator() -> (ZeroFS, FlushCoordinator) {
        let fs = ZeroFS::new_in_memory().await.unwrap();
        let coordinator = FlushCoordinator::new(Arc::clone(&fs.db));
        (fs, coordinator)
    }

    #[test]
    fn journal_incarnation_is_distinct_from_mutation_incarnation() {
        let id = Uuid::nil();
        assert_eq!(JournalIncarnation::new(id).as_uuid(), id);
        assert!(matches!(
            DurabilityError::from_materialization(
                crate::fs::mutation::types::MutationError::Poisoned("apply failed".into())
            ),
            DurabilityError::Materialization(FsError::IoError)
        ));
    }

    #[test]
    fn client_target_converts_only_through_from() {
        assert_eq!(
            DurabilityTarget::from(ClientDurabilityTarget::LocalSsd),
            DurabilityTarget::LocalSsd
        );
        assert_eq!(
            DurabilityTarget::from(ClientDurabilityTarget::RemoteBackend),
            DurabilityTarget::RemoteBackend
        );
    }

    #[tokio::test]
    async fn durable_through_direct_backend_coverage() {
        let (_fs, coordinator) = isolated_coordinator().await;
        let incarnation = MutationIncarnation::new();
        let receipt = coordinator
            .durable_through(cutoff(incarnation, 0), DurabilityTarget::RemoteBackend)
            .await
            .expect("direct backend durability should succeed");
        assert_eq!(receipt.mutation_cutoff, cutoff(incarnation, 0));
        assert_eq!(receipt.object_coverage, ObjectCoverage::DirectRemote);
        assert_eq!(receipt.target, DurabilityTarget::RemoteBackend);
    }

    #[tokio::test]
    async fn durable_through_rejects_stale_mutation_incarnation() {
        let (_fs, coordinator) = isolated_coordinator().await;
        coordinator.set_materialize({
            Arc::new(move |_cutoff| {
                Box::pin(async { Err(DurabilityError::StaleMutationIncarnation) })
            })
        });
        let result = coordinator
            .durable_through(
                cutoff(MutationIncarnation::new(), 1),
                DurabilityTarget::LocalSsd,
            )
            .await;
        assert!(matches!(
            result,
            Err(DurabilityError::StaleMutationIncarnation)
        ));
    }

    #[tokio::test]
    async fn durable_through_rejects_stale_journal_incarnation() {
        let (_fs, coordinator) = isolated_coordinator().await;
        coordinator.set_object_capture(Arc::new(writeback_coverage));
        coordinator.set_object_wait(Arc::new(|_, _| {
            Box::pin(async { Err(DurabilityError::StaleJournalIncarnation) })
        }));
        let result = coordinator
            .durable_through(
                cutoff(MutationIncarnation::new(), 1),
                DurabilityTarget::LocalSsd,
            )
            .await;
        assert!(matches!(
            result,
            Err(DurabilityError::StaleJournalIncarnation)
        ));
    }

    #[tokio::test]
    async fn durable_through_distinguishes_local_and_remote_terminal() {
        let (_fs, coordinator) = isolated_coordinator().await;
        coordinator.set_object_capture(Arc::new(writeback_coverage));
        coordinator.set_object_wait(Arc::new(|_, target| {
            Box::pin(async move {
                match target {
                    DurabilityTarget::LocalSsd => Err(DurabilityError::from_local(
                        LocalBarrierError::LocalDurability("ssd journal failed".into()),
                    )),
                    DurabilityTarget::RemoteBackend => Err(DurabilityError::from_remote(
                        RemoteBarrierError::Remote("remote publish failed".into()),
                    )),
                }
            })
        }));

        let local = coordinator
            .durable_through(
                cutoff(MutationIncarnation::new(), 1),
                DurabilityTarget::LocalSsd,
            )
            .await;
        assert!(matches!(
            local,
            Err(DurabilityError::Object(WritebackError::Local(
                LocalBarrierError::LocalDurability(_)
            )))
        ));

        let remote = coordinator
            .durable_through(
                cutoff(MutationIncarnation::new(), 1),
                DurabilityTarget::RemoteBackend,
            )
            .await;
        assert!(matches!(
            remote,
            Err(DurabilityError::Object(WritebackError::Remote(
                RemoteBarrierError::Remote(_)
            )))
        ));
    }

    #[tokio::test]
    async fn durable_through_returns_no_receipt_on_partial_failure() {
        let (_fs, coordinator) = isolated_coordinator().await;
        let failures = Arc::new(AtomicUsize::new(0));
        coordinator.set_sealer({
            let failures = Arc::clone(&failures);
            Arc::new(move || {
                failures.fetch_add(1, Ordering::SeqCst);
                Box::pin(async { Err(FsError::IoError) })
            })
        });
        let result = coordinator
            .durable_through(
                cutoff(MutationIncarnation::new(), 1),
                DurabilityTarget::RemoteBackend,
            )
            .await;
        assert!(result.is_err(), "partial failure must not yield a receipt");
        assert!(matches!(result, Err(DurabilityError::FilesystemFlush(_))));
        assert_eq!(failures.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn durable_through_materializes_before_taking_the_flush_barrier() {
        let (fs, coordinator) = isolated_coordinator().await;
        let order = Arc::new(Mutex::new(Vec::new()));
        let entered = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        coordinator.set_materialize({
            let order = Arc::clone(&order);
            let entered = Arc::clone(&entered);
            let release = Arc::clone(&release);
            let barrier = fs.db.flush_barrier();
            Arc::new(move |_cutoff| {
                let order = Arc::clone(&order);
                let entered = Arc::clone(&entered);
                let release = Arc::clone(&release);
                let barrier = std::sync::Arc::clone(&barrier);
                Box::pin(async move {
                    order.lock().unwrap().push("materialize");
                    assert!(
                        barrier.try_write().is_ok(),
                        "materialization must not hold the database flush barrier"
                    );
                    entered.notify_one();
                    release.notified().await;
                    Ok(())
                })
            })
        });
        coordinator.set_object_capture({
            let order = Arc::clone(&order);
            Arc::new(move || {
                order.lock().unwrap().push("capture");
                ObjectCoverage::DirectRemote
            })
        });

        let mut wait = tokio::spawn({
            let coordinator = coordinator.clone();
            async move {
                coordinator
                    .durable_through(
                        cutoff(MutationIncarnation::new(), 1),
                        DurabilityTarget::RemoteBackend,
                    )
                    .await
            }
        });
        entered.notified().await;
        assert!(
            tokio::time::timeout(Duration::from_millis(25), &mut wait)
                .await
                .is_err(),
            "durable_through returned before materialization finished"
        );
        release.notify_one();
        let receipt = wait
            .await
            .expect("durable_through task panicked")
            .expect("durable_through failed");
        assert_eq!(receipt.object_coverage, ObjectCoverage::DirectRemote);
        assert_eq!(*order.lock().unwrap(), ["materialize", "capture"]);
    }

    #[tokio::test]
    async fn durable_through_does_not_wait_when_materialization_fails() {
        let (_fs, coordinator) = isolated_coordinator().await;
        let waited = Arc::new(AtomicBool::new(false));
        coordinator.set_materialize(Arc::new(move |_| {
            Box::pin(async { Err(DurabilityError::StaleMutationIncarnation) })
        }));
        coordinator.set_object_wait({
            let waited = Arc::clone(&waited);
            Arc::new(move |_, _| {
                waited.store(true, Ordering::SeqCst);
                Box::pin(async { Ok(()) })
            })
        });
        let result = coordinator
            .durable_through(
                cutoff(MutationIncarnation::new(), 1),
                DurabilityTarget::LocalSsd,
            )
            .await;
        assert!(matches!(
            result,
            Err(DurabilityError::StaleMutationIncarnation)
        ));
        assert!(
            !waited.load(Ordering::SeqCst),
            "object wait must not run after materialization failure"
        );
    }
}
