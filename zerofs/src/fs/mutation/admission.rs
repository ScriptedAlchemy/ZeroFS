//! Raw mutation admission and preparation quiescence.

use crate::fs::errors::FsError;
use crate::fs::mutation::request_cache::{AcceptedRequest, PendingRequest};
use crate::fs::mutation::types::{
    ConflictKey, ConflictScope, MutationCutoff, MutationError, MutationIncarnation,
    PreparedWriteBatch,
};
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::{Arc, Mutex, MutexGuard};
use tokio::sync::{Notify, oneshot};

/// Global raw-mutation byte and operation budget.
#[derive(Debug, Clone)]
pub(crate) struct RawMutationBudget {
    inner: Arc<BudgetInner>,
}

#[derive(Debug)]
struct BudgetInner {
    capacity_bytes: u64,
    max_operations: u64,
    state: Mutex<BudgetState>,
}

#[derive(Debug)]
struct BudgetState {
    used_bytes: u64,
    used_operations: u64,
    next_waiter: u64,
    waiters: VecDeque<BudgetWaiter>,
    terminal: Option<MutationError>,
}

#[derive(Debug)]
struct BudgetWaiter {
    id: u64,
    bytes: u64,
    sender: oneshot::Sender<Result<RawMutationPermit, MutationError>>,
}

/// Move-only raw mutation permit. Drop releases bytes and one operation.
#[derive(Debug)]
pub(crate) struct RawMutationPermit {
    budget: Arc<BudgetInner>,
    bytes: u64,
    active: bool,
}

impl RawMutationPermit {
    pub(crate) fn bytes(&self) -> u64 {
        self.bytes
    }

    fn disarm(&mut self) {
        self.active = false;
    }
}

impl Drop for RawMutationPermit {
    fn drop(&mut self) {
        if !self.active {
            return;
        }
        self.active = false;
        self.budget.release(self.bytes, 1);
    }
}

struct WaitRegistration {
    inner: Arc<BudgetInner>,
    id: u64,
    charged_op: bool,
}

impl Drop for WaitRegistration {
    fn drop(&mut self) {
        if !self.charged_op {
            return;
        }
        {
            let mut state = lock(&self.inner.state);
            state.waiters.retain(|waiter| waiter.id != self.id);
            if state.terminal.is_none() {
                if let Some(operations) = state.used_operations.checked_sub(1) {
                    state.used_operations = operations;
                }
            }
        }
        self.inner.grant_waiters();
    }
}

impl RawMutationBudget {
    pub(crate) fn new(capacity_bytes: u64, max_operations: u64) -> Self {
        Self {
            inner: Arc::new(BudgetInner {
                capacity_bytes,
                max_operations,
                state: Mutex::new(BudgetState {
                    used_bytes: 0,
                    used_operations: 0,
                    next_waiter: 0,
                    waiters: VecDeque::new(),
                    terminal: None,
                }),
            }),
        }
    }

    pub(crate) fn used_bytes(&self) -> u64 {
        lock(&self.inner.state).used_bytes
    }

    pub(crate) fn used_operations(&self) -> u64 {
        lock(&self.inner.state).used_operations
    }

    pub(crate) fn poison(&self, message: impl Into<String>) {
        self.inner
            .terminate(MutationError::Poisoned(message.into()));
    }

    pub(crate) async fn acquire(&self, bytes: u64) -> Result<RawMutationPermit, MutationError> {
        if bytes > self.inner.capacity_bytes {
            return Err(MutationError::TooLarge {
                requested: bytes,
                capacity: self.inner.capacity_bytes,
            });
        }
        let (id, receiver) = {
            let mut state = lock(&self.inner.state);
            if let Some(error) = &state.terminal {
                return Err(error.clone());
            }
            if state.used_operations >= self.inner.max_operations {
                return Err(MutationError::TooLarge {
                    requested: 1,
                    capacity: self.inner.max_operations,
                });
            }
            state.used_operations += 1;
            if state.waiters.is_empty()
                && state.used_bytes.saturating_add(bytes) <= self.inner.capacity_bytes
            {
                state.used_bytes += bytes;
                return Ok(RawMutationPermit {
                    budget: Arc::clone(&self.inner),
                    bytes,
                    active: true,
                });
            }
            let id = state.next_waiter;
            state.next_waiter = state.next_waiter.wrapping_add(1);
            let (sender, receiver) = oneshot::channel();
            state.waiters.push_back(BudgetWaiter { id, bytes, sender });
            (id, receiver)
        };
        let mut registration = WaitRegistration {
            inner: Arc::clone(&self.inner),
            id,
            charged_op: true,
        };
        let result = receiver.await.unwrap_or(Err(MutationError::Closed));
        registration.charged_op = false;
        result
    }
}

impl BudgetInner {
    fn grant_waiters(self: &Arc<Self>) {
        let mut ready = Vec::new();
        {
            let mut state = lock(&self.state);
            if state.terminal.is_some() {
                return;
            }
            while let Some(front) = state.waiters.front() {
                if state.used_bytes.saturating_add(front.bytes) > self.capacity_bytes {
                    break;
                }
                let waiter = state.waiters.pop_front().expect("front exists");
                state.used_bytes += waiter.bytes;
                ready.push(waiter);
            }
        }
        for waiter in ready {
            let _ = waiter.sender.send(Ok(RawMutationPermit {
                budget: Arc::clone(self),
                bytes: waiter.bytes,
                active: true,
            }));
        }
    }

    fn release(self: &Arc<Self>, bytes: u64, operations: u64) {
        {
            let mut state = lock(&self.state);
            if state.terminal.is_some() {
                return;
            }
            if let Some(remaining) = state.used_bytes.checked_sub(bytes) {
                state.used_bytes = remaining;
            }
            if let Some(remaining) = state.used_operations.checked_sub(operations) {
                state.used_operations = remaining;
            }
        }
        self.grant_waiters();
    }

    fn terminate(&self, error: MutationError) {
        let waiters = {
            let mut state = lock(&self.state);
            if state.terminal.is_some() {
                return;
            }
            state.terminal = Some(error.clone());
            state.waiters.drain(..).collect::<Vec<_>>()
        };
        for waiter in waiters {
            let _ = waiter.sender.send(Err(error.clone()));
        }
    }
}

/// Why a preparation guard is being torn down.
#[derive(Debug)]
pub(crate) enum PreparationAbort {
    RequestFailure(FsError),
    TransportCancellation,
}

enum PreparationState {
    Open,
    Consumed,
}

/// Counts in-flight preparations without holding canonical inode locks.
pub(crate) struct PreparationGate {
    incarnation: MutationIncarnation,
    state: Mutex<GateState>,
    notify: Notify,
}

struct GateState {
    active: HashMap<u64, ConflictScope>,
    next_id: u64,
    published_through: u64,
    closing: HashMap<ConflictKey, usize>,
    terminal: Option<MutationError>,
}

impl PreparationGate {
    pub(crate) fn new(incarnation: MutationIncarnation) -> Arc<Self> {
        Arc::new(Self {
            incarnation,
            state: Mutex::new(GateState {
                active: HashMap::new(),
                next_id: 1,
                published_through: 0,
                closing: HashMap::new(),
                terminal: None,
            }),
            notify: Notify::new(),
        })
    }

    pub(crate) fn incarnation(&self) -> MutationIncarnation {
        self.incarnation
    }

    pub(crate) fn active_guards(&self) -> usize {
        lock(&self.state).active.len()
    }

    pub(crate) fn poison(&self, message: impl Into<String>) {
        {
            let mut state = lock(&self.state);
            if state.terminal.is_none() {
                state.terminal = Some(MutationError::Poisoned(message.into()));
            }
        }
        self.notify.notify_waiters();
    }

    pub(crate) fn published_through(&self) -> u64 {
        lock(&self.state).published_through
    }

    pub(crate) fn begin_close(&self, scope: &ConflictScope) -> Result<(), MutationError> {
        let mut state = lock(&self.state);
        if let Some(error) = &state.terminal {
            return Err(error.clone());
        }
        for key in scope.keys() {
            *state.closing.entry(key).or_insert(0) += 1;
        }
        Ok(())
    }

    pub(crate) fn reopen_scope(&self, scope: &ConflictScope) {
        {
            let mut state = lock(&self.state);
            for key in scope.keys() {
                if let Some(count) = state.closing.get_mut(&key) {
                    *count = count.saturating_sub(1);
                    if *count == 0 {
                        state.closing.remove(&key);
                    }
                }
            }
        }
        self.notify.notify_waiters();
    }

    pub(crate) async fn wait_closed(&self, scope: &ConflictScope) -> Result<(), MutationError> {
        loop {
            {
                let state = lock(&self.state);
                if let Some(error) = &state.terminal {
                    return Err(error.clone());
                }
                let overlapping = state.active.values().any(|active| overlaps(active, scope));
                if !overlapping {
                    return Ok(());
                }
            }
            self.notify.notified().await;
        }
    }

    pub(crate) async fn close_scope(&self, scope: &ConflictScope) -> Result<(), MutationError> {
        self.begin_close(scope)?;
        self.wait_closed(scope).await
    }

    fn mark_published(&self, sequence: u64) {
        let mut state = lock(&self.state);
        state.published_through = state.published_through.max(sequence);
    }

    fn register(&self, scope: ConflictScope) -> Result<u64, MutationError> {
        let mut state = lock(&self.state);
        if let Some(error) = &state.terminal {
            return Err(error.clone());
        }
        if scope
            .keys()
            .any(|key| state.closing.get(&key).copied().unwrap_or(0) > 0)
        {
            return Err(MutationError::Closed);
        }
        let id = state.next_id;
        state.next_id = state.next_id.saturating_add(1);
        state.active.insert(id, scope);
        Ok(id)
    }

    fn unregister(&self, id: u64) {
        {
            let mut state = lock(&self.state);
            state.active.remove(&id);
        }
        self.notify.notify_waiters();
    }
}

fn overlaps(left: &ConflictScope, right: &ConflictScope) -> bool {
    let right_keys: HashSet<_> = right.keys().collect();
    left.keys().any(|key| right_keys.contains(&key))
}

/// One accepted preparation, still holding the raw permit.
pub(crate) struct AcceptedMutation {
    request: AcceptedRequest,
    batch: PreparedWriteBatch,
    raw_permit: RawMutationPermit,
    cutoff: MutationCutoff,
}

impl AcceptedMutation {
    pub(crate) fn cutoff(&self) -> MutationCutoff {
        self.cutoff
    }

    pub(crate) fn into_parts(
        self,
    ) -> (
        AcceptedRequest,
        PreparedWriteBatch,
        RawMutationPermit,
        MutationCutoff,
    ) {
        (self.request, self.batch, self.raw_permit, self.cutoff)
    }
}

/// Preparation that must publish or abort exactly once.
pub(crate) struct PreparationGuard {
    gate: Arc<PreparationGate>,
    id: u64,
    raw_permit: Option<RawMutationPermit>,
    request: Option<PendingRequest>,
    state: PreparationState,
}

impl PreparationGuard {
    pub(crate) fn new(
        gate: Arc<PreparationGate>,
        scope: ConflictScope,
        raw_permit: RawMutationPermit,
        request: PendingRequest,
    ) -> Result<Self, MutationError> {
        match gate.register(scope) {
            Ok(id) => Ok(Self {
                gate,
                id,
                raw_permit: Some(raw_permit),
                request: Some(request),
                state: PreparationState::Open,
            }),
            Err(error) => {
                request.cancel();
                drop(raw_permit);
                Err(error)
            }
        }
    }

    pub(crate) fn publish(
        mut self,
        batch: PreparedWriteBatch,
    ) -> Result<AcceptedMutation, MutationError> {
        self.state = PreparationState::Consumed;
        let request = self.request.take().expect("open guard owns a request");
        let raw_permit = self.raw_permit.take().expect("open guard owns a permit");
        let cutoff = MutationCutoff {
            mutation_incarnation: self.gate.incarnation(),
            sequence: self.id,
        };
        self.gate.mark_published(self.id);
        self.gate.unregister(self.id);
        Ok(AcceptedMutation {
            request: request.accept(),
            batch,
            raw_permit,
            cutoff,
        })
    }

    pub(crate) fn abort(mut self, disposition: PreparationAbort) -> Result<(), MutationError> {
        self.state = PreparationState::Consumed;
        let request = self.request.take().expect("open guard owns a request");
        let permit = self.raw_permit.take();
        match disposition {
            PreparationAbort::RequestFailure(error) => {
                let _retained = request.fail(error);
            }
            PreparationAbort::TransportCancellation => request.cancel(),
        }
        drop(permit);
        self.gate.unregister(self.id);
        Ok(())
    }
}

impl Drop for PreparationGuard {
    fn drop(&mut self) {
        if matches!(self.state, PreparationState::Consumed) {
            return;
        }
        if let Some(request) = self.request.take() {
            request.cancel();
        }
        self.raw_permit.take();
        self.gate.unregister(self.id);
    }
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|error| error.into_inner())
}

#[cfg(test)]
mod tests {
    use super::{PreparationAbort, PreparationGate, PreparationGuard, RawMutationBudget};
    use crate::fs::errors::FsError;
    use crate::fs::mutation::request_cache::{PendingRequest, RequestCache, RequestLookup};
    use crate::fs::mutation::types::{
        ConflictKey, ConflictScope, MutationError, MutationIncarnation, PreparedBatchResult,
        PreparedWriteBatch, RequestFingerprint, RequestIdentity, RequestLifetime,
    };
    use crate::fs::types::FileAttributes;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::Duration;

    fn fingerprint(tag: u8) -> RequestFingerprint {
        RequestFingerprint::from_parts(&[&[tag]])
    }

    fn nbd(handle: u64) -> RequestIdentity {
        RequestIdentity::Nbd {
            connection_incarnation: 7,
            handle,
        }
    }

    fn pending(cache: &RequestCache, handle: u64, lifetime: RequestLifetime) -> PendingRequest {
        match cache
            .lookup_or_reserve(nbd(handle), fingerprint(1), lifetime)
            .unwrap()
        {
            RequestLookup::Vacant(vacancy) => vacancy.begin_pending(),
            other => panic!("expected vacant lookup, got {other:?}"),
        }
    }

    fn scope(inode: u64) -> ConflictScope {
        ConflictScope::single(ConflictKey::Inode(inode))
    }

    fn batch() -> PreparedWriteBatch {
        PreparedWriteBatch::replayed(
            [0u8; 16],
            PreparedBatchResult {
                members: vec![(
                    1,
                    FileAttributes {
                        size: 1,
                        ..FileAttributes::default()
                    },
                )],
            },
        )
    }

    #[tokio::test]
    async fn permit_is_acquired_before_payload_copy() {
        let budget = RawMutationBudget::new(8, 4);
        let held = budget.acquire(8).await.unwrap();
        let copied = Arc::new(AtomicBool::new(false));
        let waiter = tokio::spawn({
            let budget = budget.clone();
            let copied = Arc::clone(&copied);
            async move {
                let permit = budget.acquire(4).await.unwrap();
                // The owned frame copy happens only after admission grants
                // byte and operation ownership.
                copied.store(true, Ordering::SeqCst);
                permit
            }
        });

        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(
            !copied.load(Ordering::SeqCst),
            "the payload must not be copied while admission blocks"
        );
        assert_eq!(budget.used_bytes(), 8);

        drop(held);
        let permit = tokio::time::timeout(Duration::from_secs(1), waiter)
            .await
            .unwrap()
            .unwrap();
        assert!(copied.load(Ordering::SeqCst));
        assert_eq!(permit.bytes(), 4);
    }

    #[tokio::test]
    async fn cancelled_waiter_rolls_back_bytes_and_ops() {
        let budget = RawMutationBudget::new(8, 8);
        let held = budget.acquire(8).await.unwrap();
        let blocked = tokio::spawn({
            let budget = budget.clone();
            async move { budget.acquire(4).await }
        });

        tokio::time::sleep(Duration::from_millis(20)).await;
        assert_eq!(
            budget.used_operations(),
            2,
            "a blocked waiter already owns its operation reservation"
        );
        blocked.abort();
        blocked.await.unwrap_err();

        assert_eq!(budget.used_bytes(), 8);
        assert_eq!(budget.used_operations(), 1);
        drop(held);
        assert_eq!(budget.used_bytes(), 0);
        assert_eq!(budget.used_operations(), 0);
    }

    #[tokio::test]
    async fn race_winner_releases_unused_permit() {
        let budget = RawMutationBudget::new(16, 8);
        let cache = RequestCache::new(8);
        let owner = pending(&cache, 1, RequestLifetime::InFlightOnly);
        let owner_permit = budget.acquire(4).await.unwrap();

        // A concurrent identical request acquired its permit, then lost the
        // dedup race: the lookup joins the in-flight entry instead.
        let racer_permit = budget.acquire(4).await.unwrap();
        let joined = match cache
            .lookup_or_reserve(nbd(1), fingerprint(1), RequestLifetime::InFlightOnly)
            .unwrap()
        {
            RequestLookup::Joined(retained) => retained,
            other => panic!("expected joined lookup, got {other:?}"),
        };
        drop(racer_permit);
        assert_eq!(budget.used_bytes(), 4);
        assert_eq!(budget.used_operations(), 1);

        let waiter = tokio::spawn(async move { joined.wait().await });
        cache.complete(
            owner.accept(),
            Ok(PreparedBatchResult {
                members: vec![(1, FileAttributes::default())],
            }),
        );
        waiter.await.unwrap().unwrap();
        drop(owner_permit);
        assert_eq!(budget.used_bytes(), 0);
        assert_eq!(budget.used_operations(), 0);
    }

    #[tokio::test]
    async fn guard_must_publish_or_abort() {
        let budget = RawMutationBudget::new(16, 8);
        let cache = RequestCache::new(8);
        let gate = PreparationGate::new(MutationIncarnation::new());

        // publish consumes the guard and moves permit ownership onward.
        let guard = PreparationGuard::new(
            Arc::clone(&gate),
            scope(1),
            budget.acquire(4).await.unwrap(),
            pending(&cache, 1, RequestLifetime::InFlightOnly),
        )
        .unwrap();
        assert_eq!(gate.active_guards(), 1);
        let accepted = guard.publish(batch()).unwrap();
        assert_eq!(gate.active_guards(), 0);
        assert_eq!(accepted.cutoff().mutation_incarnation, gate.incarnation());
        assert_eq!(accepted.cutoff().sequence, 1);
        assert_eq!(
            budget.used_bytes(),
            4,
            "publish transfers the permit, it does not release it"
        );
        let (request, _batch, permit, _cutoff) = accepted.into_parts();
        cache.complete(
            request,
            Ok(PreparedBatchResult {
                members: vec![(1, FileAttributes::default())],
            }),
        );
        drop(permit);
        assert_eq!(budget.used_bytes(), 0);

        // abort consumes the guard and releases everything.
        let guard = PreparationGuard::new(
            Arc::clone(&gate),
            scope(1),
            budget.acquire(4).await.unwrap(),
            pending(&cache, 2, RequestLifetime::InFlightOnly),
        )
        .unwrap();
        guard
            .abort(PreparationAbort::TransportCancellation)
            .unwrap();
        assert_eq!(gate.active_guards(), 0);
        assert_eq!(budget.used_bytes(), 0);

        // A dropped guard is defect-safe cancellation, never a leak.
        let guard = PreparationGuard::new(
            Arc::clone(&gate),
            scope(1),
            budget.acquire(4).await.unwrap(),
            pending(&cache, 3, RequestLifetime::InFlightOnly),
        )
        .unwrap();
        drop(guard);
        assert_eq!(gate.active_guards(), 0);
        assert_eq!(budget.used_bytes(), 0);
        assert_eq!(budget.used_operations(), 0);
        assert_eq!(cache.used_slots(), 0);
    }

    #[tokio::test]
    async fn guard_failure_retains_result_and_releases_raw_permit() {
        let budget = RawMutationBudget::new(16, 8);
        let cache = RequestCache::new(8);
        let gate = PreparationGate::new(MutationIncarnation::new());
        let guard = PreparationGuard::new(
            Arc::clone(&gate),
            scope(1),
            budget.acquire(4).await.unwrap(),
            pending(&cache, 1, RequestLifetime::CanonicalDedup),
        )
        .unwrap();

        guard
            .abort(PreparationAbort::RequestFailure(FsError::IoError))
            .unwrap();

        assert_eq!(gate.active_guards(), 0);
        assert_eq!(budget.used_bytes(), 0);
        assert_eq!(budget.used_operations(), 0);
        let retained = match cache
            .lookup_or_reserve(nbd(1), fingerprint(1), RequestLifetime::CanonicalDedup)
            .unwrap()
        {
            RequestLookup::Joined(retained) => retained,
            other => panic!("expected retained failure, got {other:?}"),
        };
        assert!(matches!(retained.completed(), Some(Err(FsError::IoError))));
    }

    #[tokio::test]
    async fn guard_cancellation_removes_request_and_releases_slot() {
        let budget = RawMutationBudget::new(16, 8);
        let cache = RequestCache::new(8);
        let gate = PreparationGate::new(MutationIncarnation::new());
        let guard = PreparationGuard::new(
            Arc::clone(&gate),
            scope(1),
            budget.acquire(4).await.unwrap(),
            pending(&cache, 1, RequestLifetime::CanonicalDedup),
        )
        .unwrap();

        guard
            .abort(PreparationAbort::TransportCancellation)
            .unwrap();

        assert_eq!(cache.len(), 0, "cancellation removes the provisional entry");
        assert_eq!(cache.used_slots(), 0);
        assert_eq!(budget.used_bytes(), 0);
        assert_eq!(budget.used_operations(), 0);
        let retry = cache
            .lookup_or_reserve(nbd(1), fingerprint(1), RequestLifetime::CanonicalDedup)
            .unwrap();
        assert!(matches!(retry, RequestLookup::Vacant(_)));
        drop(retry);
    }

    #[tokio::test]
    async fn gate_close_waits_pre_cutoff_guards() {
        let budget = RawMutationBudget::new(16, 8);
        let cache = RequestCache::new(8);
        let gate = PreparationGate::new(MutationIncarnation::new());
        let guard = PreparationGuard::new(
            Arc::clone(&gate),
            scope(1),
            budget.acquire(4).await.unwrap(),
            pending(&cache, 1, RequestLifetime::InFlightOnly),
        )
        .unwrap();

        let closer = tokio::spawn({
            let gate = Arc::clone(&gate);
            let closing = scope(1);
            async move { gate.close_scope(&closing).await }
        });
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(
            !closer.is_finished(),
            "closing must wait for the pre-closure guard"
        );

        // A guard arriving after closure is refused and cleaned up.
        let late = PreparationGuard::new(
            Arc::clone(&gate),
            scope(1),
            budget.acquire(4).await.unwrap(),
            pending(&cache, 2, RequestLifetime::InFlightOnly),
        );
        assert!(matches!(late, Err(MutationError::Closed)));
        assert_eq!(budget.used_bytes(), 4);
        assert_eq!(cache.used_slots(), 1);

        guard
            .abort(PreparationAbort::TransportCancellation)
            .unwrap();
        tokio::time::timeout(Duration::from_secs(1), closer)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
    }

    #[tokio::test]
    async fn guard_holds_no_canonical_lock() {
        let budget = RawMutationBudget::new(16, 8);
        let cache = RequestCache::new(8);
        let gate = PreparationGate::new(MutationIncarnation::new());

        // The gate counts preparations; it never locks the conflict keys, so
        // two concurrent guards over the same inode coexist without blocking.
        let first = PreparationGuard::new(
            Arc::clone(&gate),
            scope(1),
            budget.acquire(4).await.unwrap(),
            pending(&cache, 1, RequestLifetime::InFlightOnly),
        )
        .unwrap();
        let second = PreparationGuard::new(
            Arc::clone(&gate),
            scope(1),
            budget.acquire(4).await.unwrap(),
            pending(&cache, 2, RequestLifetime::InFlightOnly),
        )
        .unwrap();
        assert_eq!(gate.active_guards(), 2);

        first
            .abort(PreparationAbort::TransportCancellation)
            .unwrap();
        second
            .abort(PreparationAbort::TransportCancellation)
            .unwrap();
        assert_eq!(gate.active_guards(), 0);
    }
}
