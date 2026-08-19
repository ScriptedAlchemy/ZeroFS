//! Deadlock-safe conflict fences.
//!
//! A fence closes conflicting preparation, waits pre-closure guards to
//! publish or abort, drains the accepted cutoff without holding canonical
//! inode locks, and reopens admission on drop. It promises visibility
//! order only, never SSD durability.

use crate::fs::ZeroFS;
use crate::fs::errors::FsError;
#[cfg(test)]
use crate::fs::inode::InodeId;
use crate::fs::mutation::admission::{PreparationGate, RawMutationBudget};
use crate::fs::mutation::progress::MutationProgress;
use crate::fs::mutation::request_cache::RequestCache;
use crate::fs::mutation::types::{ConflictKey, ConflictScope, MutationCutoff, MutationError};
use std::collections::BTreeSet;
use std::sync::Arc;

/// Owns preparation quiescence plus gap-free materialization drain.
pub(crate) struct MutationCoordinator {
    gate: Arc<PreparationGate>,
    progress: MutationProgress,
    request_cache: RequestCache,
    raw_budget: RawMutationBudget,
}

impl MutationCoordinator {
    pub(crate) fn new(gate: Arc<PreparationGate>, progress: MutationProgress) -> Arc<Self> {
        Self::new_with_limits(gate, progress, u64::MAX, 1024)
    }

    pub(crate) fn new_with_limits(
        gate: Arc<PreparationGate>,
        progress: MutationProgress,
        max_bytes: u64,
        max_operations: u64,
    ) -> Arc<Self> {
        Arc::new(Self {
            gate,
            progress,
            request_cache: RequestCache::new(max_operations.min(usize::MAX as u64) as usize),
            raw_budget: RawMutationBudget::new(max_bytes, max_operations),
        })
    }

    pub(crate) fn gate(&self) -> Arc<PreparationGate> {
        Arc::clone(&self.gate)
    }

    pub(crate) fn progress(&self) -> MutationProgress {
        self.progress.clone()
    }

    pub(crate) fn request_cache(&self) -> RequestCache {
        self.request_cache.clone()
    }

    pub(crate) fn raw_budget(&self) -> RawMutationBudget {
        self.raw_budget.clone()
    }

    pub(crate) fn poison(&self, message: impl Into<String>) {
        let message = message.into();
        self.gate.poison(message.clone());
        self.progress.poison(message);
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

/// Holds a materialization fence for the duration of one metadata operation.
/// Overlay drain happens before this guard is returned so setattr/io do not
/// grow a second wait beside the fence.
pub(crate) struct MetadataFence {
    _materialization: Option<MaterializationFence>,
}

fn mutation_fs_error(error: MutationError) -> FsError {
    match error {
        MutationError::TooLarge { .. } => FsError::NoSpace,
        MutationError::StaleIncarnation => FsError::StaleHandle,
        MutationError::Closed | MutationError::Poisoned(_) => FsError::IoError,
    }
}

impl ZeroFS {
    /// Close overlapping preparation, drain accepted writes, then drain the
    /// volatile overlay. Callers take canonical locks only after this returns.
    pub(crate) async fn fence_metadata(
        &self,
        scope: ConflictScope,
    ) -> Result<MetadataFence, FsError> {
        let materialization = if let Some(coordinator) = self.mutation_coordinator.get() {
            Some(
                coordinator
                    .materialization_fence(scope.clone())
                    .await
                    .map_err(mutation_fs_error)?,
            )
        } else {
            None
        };
        let mut seen = BTreeSet::new();
        for key in scope.keys() {
            let id = match key {
                ConflictKey::Inode(id) | ConflictKey::Directory(id) => id,
            };
            if seen.insert(id) {
                self.quiesce_overlay_inode(id).await?;
            }
        }
        Ok(MetadataFence {
            _materialization: materialization,
        })
    }
}

#[cfg(test)]
pub(crate) struct HeldPreparation {
    guard: Option<crate::fs::mutation::admission::PreparationGuard>,
    _cache: crate::fs::mutation::request_cache::RequestCache,
    _budget: crate::fs::mutation::admission::RawMutationBudget,
}

#[cfg(test)]
impl HeldPreparation {
    pub(crate) fn abort(mut self) {
        if let Some(guard) = self.guard.take() {
            guard
                .abort(crate::fs::mutation::admission::PreparationAbort::TransportCancellation)
                .unwrap();
        }
    }
}

#[cfg(test)]
impl ZeroFS {
    pub(crate) async fn hold_preparation_for_test(&self, scope: ConflictScope) -> HeldPreparation {
        use crate::fs::mutation::admission::{PreparationGuard, RawMutationBudget};
        use crate::fs::mutation::request_cache::{RequestCache, RequestLookup};
        use crate::fs::mutation::types::{RequestFingerprint, RequestIdentity, RequestLifetime};
        use std::sync::atomic::{AtomicU64, Ordering};

        static HANDLE: AtomicU64 = AtomicU64::new(1);
        let coordinator = self
            .mutation_coordinator
            .get()
            .expect("start_materializer must install the mutation coordinator");
        let cache = RequestCache::new(8);
        let budget = RawMutationBudget::new(1 << 20, 1024);
        let handle = HANDLE.fetch_add(1, Ordering::Relaxed);
        let pending = match cache
            .lookup_or_reserve(
                RequestIdentity::Nbd {
                    connection_incarnation: 7,
                    handle,
                },
                RequestFingerprint::from_parts(&[&handle.to_le_bytes()]),
                RequestLifetime::InFlightOnly,
            )
            .unwrap()
        {
            RequestLookup::Vacant(vacancy) => vacancy.begin_pending(),
            other => panic!("expected vacant lookup, got {other:?}"),
        };
        let guard = PreparationGuard::new(
            coordinator.gate(),
            scope,
            budget.acquire(1).await.unwrap(),
            pending,
        )
        .unwrap();
        HeldPreparation {
            guard: Some(guard),
            _cache: cache,
            _budget: budget,
        }
    }

    pub(crate) async fn assert_pending_write_drains_without_canonical_lock<T>(
        self: &Arc<Self>,
        scope: ConflictScope,
        lock_id: InodeId,
        op: impl std::future::Future<Output = T> + Send + 'static,
    ) -> T
    where
        T: Send + 'static,
    {
        let hold = self.hold_preparation_for_test(scope).await;
        let task = tokio::spawn(op);
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        assert!(
            !task.is_finished(),
            "metadata op must wait for the pending overlapping write"
        );
        let _lock = tokio::time::timeout(
            std::time::Duration::from_millis(100),
            self.lock_manager.acquire(lock_id),
        )
        .await
        .expect("fence must not hold a canonical lock while draining");
        drop(_lock);
        hold.abort();
        tokio::time::timeout(std::time::Duration::from_secs(1), task)
            .await
            .expect("metadata op must finish after the write drains")
            .expect("metadata op task must join")
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
