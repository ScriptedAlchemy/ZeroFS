//! Deadlock-safe conflict fences.
//!
//! A fence closes conflicting preparation, waits pre-closure guards to
//! publish or abort, drains the accepted cutoff without holding canonical
//! inode locks, and reopens admission on drop. It promises visibility
//! order only, never SSD durability.

use crate::fs::mutation::admission::PreparationGate;
use crate::fs::mutation::progress::MutationProgress;
use crate::fs::mutation::types::{ConflictScope, MutationCutoff, MutationError};
use std::sync::Arc;

/// Owns preparation quiescence plus gap-free materialization drain.
pub(crate) struct MutationCoordinator {
    gate: Arc<PreparationGate>,
    progress: MutationProgress,
}

impl MutationCoordinator {
    pub(crate) fn new(gate: Arc<PreparationGate>, progress: MutationProgress) -> Arc<Self> {
        Arc::new(Self { gate, progress })
    }

    pub(crate) fn gate(&self) -> Arc<PreparationGate> {
        Arc::clone(&self.gate)
    }

    pub(crate) fn progress(&self) -> MutationProgress {
        self.progress.clone()
    }

    pub(crate) async fn materialization_fence(
        self: &Arc<Self>,
        scope: ConflictScope,
    ) -> Result<MaterializationFence, MutationError> {
        self.gate.begin_close(&scope)?;
        let mut fence = MaterializationFence {
            gate: Arc::clone(&self.gate),
            scope: scope.clone(),
            cutoff: MutationCutoff {
                mutation_incarnation: self.gate.incarnation(),
                sequence: 0,
            },
            armed: true,
        };
        let drain = async {
            self.gate.wait_closed(&scope).await?;
            let cutoff = MutationCutoff {
                mutation_incarnation: self.gate.incarnation(),
                sequence: self.gate.published_through(),
            };
            if cutoff.sequence > 0 {
                self.progress.wait_materialized(cutoff).await?;
            }
            Ok(cutoff)
        }
        .await;
        match drain {
            Ok(cutoff) => {
                fence.cutoff = cutoff;
                Ok(fence)
            }
            Err(error) => Err(error),
        }
    }
}

/// Holds a closed conflict scope until drop reopens preparation admission.
pub(crate) struct MaterializationFence {
    gate: Arc<PreparationGate>,
    scope: ConflictScope,
    cutoff: MutationCutoff,
    armed: bool,
}

impl MaterializationFence {
    pub(crate) fn cutoff(&self) -> MutationCutoff {
        self.cutoff
    }

    pub(crate) fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for MaterializationFence {
    fn drop(&mut self) {
        if self.armed {
            self.armed = false;
            self.gate.reopen_scope(&self.scope);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::MutationCoordinator;
    use crate::fs::mutation::admission::{
        PreparationAbort, PreparationGate, PreparationGuard, RawMutationBudget,
    };
    use crate::fs::mutation::progress::MutationProgress;
    use crate::fs::mutation::request_cache::{RequestCache, RequestLookup};
    use crate::fs::mutation::types::{
        ConflictKey, ConflictScope, MutationError, MutationIncarnation, PreparedBatchResult,
        PreparedWriteBatch, RequestFingerprint, RequestIdentity, RequestLifetime,
    };
    use crate::fs::types::FileAttributes;
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::sync::Mutex;

    fn scope(inode: u64) -> ConflictScope {
        ConflictScope::single(ConflictKey::Inode(inode))
    }

    fn pending(
        cache: &RequestCache,
        handle: u64,
    ) -> crate::fs::mutation::request_cache::PendingRequest {
        match cache
            .lookup_or_reserve(
                RequestIdentity::Nbd {
                    connection_incarnation: 7,
                    handle,
                },
                RequestFingerprint::from_parts(&[&[handle as u8]]),
                RequestLifetime::InFlightOnly,
            )
            .unwrap()
        {
            RequestLookup::Vacant(vacancy) => vacancy.begin_pending(),
            other => panic!("expected vacant lookup, got {other:?}"),
        }
    }

    fn batch() -> PreparedWriteBatch {
        PreparedWriteBatch::replayed(
            [0u8; 16],
            PreparedBatchResult {
                members: vec![(1, FileAttributes::default())],
            },
        )
    }

    fn coordinator() -> (
        Arc<MutationCoordinator>,
        RawMutationBudget,
        RequestCache,
        MutationIncarnation,
    ) {
        let incarnation = MutationIncarnation::new();
        let gate = PreparationGate::new(incarnation);
        let progress = MutationProgress::new(incarnation);
        (
            MutationCoordinator::new(gate, progress),
            RawMutationBudget::new(32, 16),
            RequestCache::new(8),
            incarnation,
        )
    }

    #[tokio::test]
    async fn fence_waits_preclosure_guard_to_publish_or_abort() {
        let (coordinator, budget, cache, _) = coordinator();
        let guard = PreparationGuard::new(
            coordinator.gate(),
            scope(1),
            budget.acquire(4).await.unwrap(),
            pending(&cache, 1),
        )
        .unwrap();

        let fencer = tokio::spawn({
            let coordinator = Arc::clone(&coordinator);
            async move { coordinator.materialization_fence(scope(1)).await }
        });
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(!fencer.is_finished(), "fence must wait for the open guard");

        guard
            .abort(PreparationAbort::TransportCancellation)
            .unwrap();
        let fence = tokio::time::timeout(Duration::from_secs(1), fencer)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        drop(fence);
    }

    #[tokio::test]
    async fn postclosure_preparer_waits() {
        let (coordinator, budget, _cache, _) = coordinator();
        let fence = coordinator.materialization_fence(scope(1)).await.unwrap();

        let late = tokio::spawn({
            let gate = coordinator.gate();
            let budget = budget.clone();
            async move {
                let cache = RequestCache::new(8);
                loop {
                    match PreparationGuard::new(
                        Arc::clone(&gate),
                        scope(1),
                        budget.acquire(4).await.unwrap(),
                        pending(&cache, 2),
                    ) {
                        Ok(guard) => return Ok(guard),
                        Err(MutationError::Closed) => {
                            tokio::task::yield_now().await;
                        }
                        Err(error) => return Err(error),
                    }
                }
            }
        });
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(
            !late.is_finished(),
            "a post-closure preparer must wait for the fence to reopen"
        );
        drop(fence);
        let guard = tokio::time::timeout(Duration::from_secs(1), late)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        guard
            .abort(PreparationAbort::TransportCancellation)
            .unwrap();
    }

    #[tokio::test]
    async fn cutoff_is_captured_after_quiescence() {
        let (coordinator, budget, cache, incarnation) = coordinator();
        let guard = PreparationGuard::new(
            coordinator.gate(),
            scope(1),
            budget.acquire(4).await.unwrap(),
            pending(&cache, 1),
        )
        .unwrap();
        let accepted = guard.publish(batch()).unwrap();
        assert_eq!(accepted.cutoff().sequence, 1);
        coordinator
            .progress()
            .record_materialized(accepted.cutoff())
            .unwrap();

        let fence = coordinator.materialization_fence(scope(1)).await.unwrap();
        assert_eq!(fence.cutoff().mutation_incarnation, incarnation);
        assert_eq!(
            fence.cutoff().sequence,
            1,
            "cutoff must be captured after pre-closure publish"
        );
        drop(fence);
        drop(accepted);
    }

    #[tokio::test]
    async fn drain_holds_no_canonical_lock() {
        let (coordinator, budget, cache, _) = coordinator();
        let guard = PreparationGuard::new(
            coordinator.gate(),
            scope(1),
            budget.acquire(4).await.unwrap(),
            pending(&cache, 1),
        )
        .unwrap();
        let accepted = guard.publish(batch()).unwrap();
        let canonical = Arc::new(Mutex::new(()));

        let fencer = tokio::spawn({
            let coordinator = Arc::clone(&coordinator);
            async move { coordinator.materialization_fence(scope(1)).await }
        });
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(!fencer.is_finished(), "drain waits for materialization");

        let _canonical = tokio::time::timeout(Duration::from_millis(100), canonical.lock())
            .await
            .expect("drain must not hold a canonical lock");

        coordinator
            .progress()
            .record_materialized(accepted.cutoff())
            .unwrap();
        let fence = tokio::time::timeout(Duration::from_secs(1), fencer)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        drop(fence);
        drop(accepted);
    }

    #[tokio::test]
    async fn drop_reopens_scope() {
        let (coordinator, budget, cache, _) = coordinator();
        let fence = coordinator.materialization_fence(scope(1)).await.unwrap();
        let refused = PreparationGuard::new(
            coordinator.gate(),
            scope(1),
            budget.acquire(4).await.unwrap(),
            pending(&cache, 1),
        );
        assert!(matches!(refused, Err(MutationError::Closed)));
        drop(fence);
        let guard = PreparationGuard::new(
            coordinator.gate(),
            scope(1),
            budget.acquire(4).await.unwrap(),
            pending(&cache, 2),
        )
        .unwrap();
        guard
            .abort(PreparationAbort::TransportCancellation)
            .unwrap();
    }

    #[tokio::test]
    async fn cancelled_fence_reopens_scope() {
        let (coordinator, budget, cache, _) = coordinator();
        let guard = PreparationGuard::new(
            coordinator.gate(),
            scope(1),
            budget.acquire(4).await.unwrap(),
            pending(&cache, 1),
        )
        .unwrap();

        let fencer = tokio::spawn({
            let coordinator = Arc::clone(&coordinator);
            async move { coordinator.materialization_fence(scope(1)).await }
        });
        tokio::time::sleep(Duration::from_millis(20)).await;
        fencer.abort();
        assert!(
            fencer.await.is_err(),
            "cancelled fence task must not complete the fence"
        );

        let late = PreparationGuard::new(
            coordinator.gate(),
            scope(1),
            budget.acquire(4).await.unwrap(),
            pending(&cache, 2),
        )
        .unwrap();
        late.abort(PreparationAbort::TransportCancellation).unwrap();
        guard
            .abort(PreparationAbort::TransportCancellation)
            .unwrap();
    }
}
