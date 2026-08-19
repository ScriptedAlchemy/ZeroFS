//! Bounded protocol request replay cache.
//!
//! [`RequestCache::lookup_or_reserve`] is the only entry point. It joins,
//! rejects fingerprint mismatches, and applies cache-pressure backpressure
//! before any raw admission call. A vacant lookup already owns exactly one
//! operation slot; [`RequestVacancy::begin_pending`] moves that same slot.

use crate::fs::errors::FsError;
use crate::fs::mutation::types::{
    PreparedBatchResult, RequestFingerprint, RequestIdentity, RequestLifetime,
};
use std::collections::HashMap;
use std::fmt;
use std::sync::{Arc, Mutex, MutexGuard, Weak};
use std::time::Instant;
use tokio::sync::Notify;

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub(crate) enum RequestCacheError {
    #[error("request cache is closed")]
    Closed,
}

/// Charge against the cache operation cap. Drop releases once while `active`.
/// Transfer moves the charge without disarming; retained completion disarms
/// only after the cache entry assumes the bounded charge.
pub(crate) struct RequestOperationSlot {
    cache: Weak<RequestCacheInner>,
    active: bool,
}

impl RequestOperationSlot {
    fn transfer(&mut self) -> Self {
        let transferred = Self {
            cache: self.cache.clone(),
            active: self.active,
        };
        self.active = false;
        transferred
    }

    fn disarm(&mut self) {
        self.active = false;
    }
}

impl Drop for RequestOperationSlot {
    fn drop(&mut self) {
        if !self.active {
            return;
        }
        self.active = false;
        if let Some(cache) = self.cache.upgrade() {
            cache.release_slot();
        }
    }
}

pub(crate) enum RequestLookup {
    Vacant(RequestVacancy),
    Joined(Arc<RetainedRequest>),
    FingerprintMismatch,
    Backpressured,
}

impl fmt::Debug for RequestLookup {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Vacant(_) => f.write_str("Vacant"),
            Self::Joined(_) => f.write_str("Joined"),
            Self::FingerprintMismatch => f.write_str("FingerprintMismatch"),
            Self::Backpressured => f.write_str("Backpressured"),
        }
    }
}

#[derive(Clone)]
pub(crate) struct RequestCache {
    inner: Arc<RequestCacheInner>,
}

struct RequestCacheInner {
    max_entries: usize,
    state: Mutex<CacheState>,
}

struct CacheState {
    entries: HashMap<RequestIdentity, CacheEntry>,
    used_slots: usize,
    closed: bool,
}

enum CacheEntry {
    Pending {
        fingerprint: RequestFingerprint,
        retained: Arc<RetainedRequest>,
    },
    Retained {
        retained: Arc<RetainedRequest>,
        expires_at: Option<Instant>,
        slot: Option<RequestOperationSlot>,
    },
}

pub(crate) struct RequestVacancy {
    cache: Arc<RequestCacheInner>,
    identity: RequestIdentity,
    fingerprint: RequestFingerprint,
    lifetime: RequestLifetime,
    operation_slot: Option<RequestOperationSlot>,
    consumed: bool,
}

pub(crate) struct PendingRequest {
    cache: Arc<RequestCacheInner>,
    identity: RequestIdentity,
    fingerprint: RequestFingerprint,
    lifetime: RequestLifetime,
    operation_slot: Option<RequestOperationSlot>,
    retained: Arc<RetainedRequest>,
    state: PendingRequestState,
}

enum PendingRequestState {
    Preparing,
    Accepted,
    Terminal,
}

pub(crate) struct AcceptedRequest {
    cache: Arc<RequestCacheInner>,
    identity: RequestIdentity,
    lifetime: RequestLifetime,
    operation_slot: Option<RequestOperationSlot>,
    completed: bool,
}

/// Shared in-flight or completed result. Joiners wait here instead of
/// re-entering raw admission.
pub(crate) struct RetainedRequest {
    identity: RequestIdentity,
    fingerprint: RequestFingerprint,
    outcome: Mutex<Option<Result<PreparedBatchResult, FsError>>>,
    notify: Notify,
}

impl RetainedRequest {
    fn new(identity: RequestIdentity, fingerprint: RequestFingerprint) -> Arc<Self> {
        Arc::new(Self {
            identity,
            fingerprint,
            outcome: Mutex::new(None),
            notify: Notify::new(),
        })
    }

    pub(crate) fn identity(&self) -> &RequestIdentity {
        &self.identity
    }

    pub(crate) fn fingerprint(&self) -> RequestFingerprint {
        self.fingerprint
    }

    pub(crate) fn completed(&self) -> Option<Result<PreparedBatchResult, FsError>> {
        lock(&self.outcome).clone()
    }

    pub(crate) async fn wait(&self) -> Result<PreparedBatchResult, FsError> {
        loop {
            let notified = self.notify.notified();
            if let Some(result) = self.completed() {
                return result;
            }
            notified.await;
        }
    }

    fn publish(&self, result: Result<PreparedBatchResult, FsError>) {
        *lock(&self.outcome) = Some(result);
        self.notify.notify_waiters();
    }
}

impl RequestVacancy {
    /// Consume the vacancy and move its single operation slot into pending.
    pub(crate) fn begin_pending(mut self) -> PendingRequest {
        self.consumed = true;
        let retained = self
            .cache
            .mark_pending(self.identity.clone(), self.fingerprint);
        PendingRequest {
            cache: Arc::clone(&self.cache),
            identity: self.identity.clone(),
            fingerprint: self.fingerprint,
            lifetime: self.lifetime,
            operation_slot: self.operation_slot.take(),
            retained,
            state: PendingRequestState::Preparing,
        }
    }
}

impl Drop for RequestVacancy {
    fn drop(&mut self) {
        if self.consumed {
            return;
        }
        self.consumed = true;
        self.cache.cancel_pending(&self.identity, FsError::IoError);
    }
}

impl PendingRequest {
    pub(crate) fn retained(&self) -> Arc<RetainedRequest> {
        Arc::clone(&self.retained)
    }

    pub(crate) fn accept(mut self) -> AcceptedRequest {
        self.state = PendingRequestState::Accepted;
        AcceptedRequest {
            cache: Arc::clone(&self.cache),
            identity: self.identity.clone(),
            lifetime: self.lifetime,
            operation_slot: self.operation_slot.take(),
            completed: false,
        }
    }

    pub(crate) fn fail(mut self, error: FsError) -> Arc<RetainedRequest> {
        self.state = PendingRequestState::Terminal;
        let slot = self.operation_slot.take();
        self.cache
            .retain_failure(&self.identity, self.lifetime, slot, error)
    }

    pub(crate) fn cancel(mut self) {
        self.state = PendingRequestState::Terminal;
        let _slot = self.operation_slot.take();
        self.cache.cancel_pending(&self.identity, FsError::IoError);
    }
}

impl Drop for PendingRequest {
    fn drop(&mut self) {
        if matches!(
            self.state,
            PendingRequestState::Accepted | PendingRequestState::Terminal
        ) {
            return;
        }
        self.state = PendingRequestState::Terminal;
        let _slot = self.operation_slot.take();
        self.cache.cancel_pending(&self.identity, FsError::IoError);
    }
}

impl Drop for AcceptedRequest {
    fn drop(&mut self) {
        if self.completed {
            return;
        }
        self.cache.cancel_pending(&self.identity, FsError::IoError);
    }
}

impl RequestCache {
    pub(crate) fn new(max_entries: usize) -> Self {
        Self {
            inner: Arc::new(RequestCacheInner {
                max_entries: max_entries.max(1),
                state: Mutex::new(CacheState {
                    entries: HashMap::new(),
                    used_slots: 0,
                    closed: false,
                }),
            }),
        }
    }

    pub(crate) fn used_slots(&self) -> usize {
        lock(&self.inner.state).used_slots
    }

    pub(crate) fn len(&self) -> usize {
        lock(&self.inner.state).entries.len()
    }

    pub(crate) fn lookup_or_reserve(
        &self,
        identity: RequestIdentity,
        fingerprint: RequestFingerprint,
        lifetime: RequestLifetime,
    ) -> Result<RequestLookup, RequestCacheError> {
        self.inner
            .lookup_or_reserve(identity, fingerprint, lifetime)
    }

    pub(crate) fn complete(
        &self,
        accepted: AcceptedRequest,
        result: Result<PreparedBatchResult, FsError>,
    ) -> Arc<RetainedRequest> {
        self.inner.complete(accepted, result)
    }
}

impl RequestCacheInner {
    fn lookup_or_reserve(
        self: &Arc<Self>,
        identity: RequestIdentity,
        fingerprint: RequestFingerprint,
        lifetime: RequestLifetime,
    ) -> Result<RequestLookup, RequestCacheError> {
        let mut state = lock(&self.state);
        if state.closed {
            return Err(RequestCacheError::Closed);
        }
        self.evict_expired(&mut state);
        if let Some(entry) = state.entries.get(&identity) {
            return Ok(if *existingfingerprint(entry) == fingerprint {
                RequestLookup::Joined(Arc::clone(retained_of(entry)))
            } else {
                RequestLookup::FingerprintMismatch
            });
        }
        if state.entries.len() >= self.max_entries || state.used_slots >= self.max_entries {
            return Ok(RequestLookup::Backpressured);
        }
        state.used_slots += 1;
        let slot = RequestOperationSlot {
            cache: Arc::downgrade(self),
            active: true,
        };
        // The vacancy is not yet a named entry; begin_pending inserts pending.
        // Reserve a placeholder so a concurrent lookup cannot steal the identity.
        state.entries.insert(
            identity.clone(),
            CacheEntry::Pending {
                fingerprint,
                retained: RetainedRequest::new(identity.clone(), fingerprint),
            },
        );
        drop(state);
        Ok(RequestLookup::Vacant(RequestVacancy {
            cache: Arc::clone(self),
            identity,
            fingerprint,
            lifetime,
            operation_slot: Some(slot),
            consumed: false,
        }))
    }

    fn mark_pending(
        self: &Arc<Self>,
        identity: RequestIdentity,
        fingerprint: RequestFingerprint,
    ) -> Arc<RetainedRequest> {
        let state = lock(&self.state);
        match state.entries.get(&identity) {
            Some(CacheEntry::Pending { retained, .. }) => Arc::clone(retained),
            _ => {
                // Vacancy drop may have raced; recreate the pending entry.
                drop(state);
                let retained = RetainedRequest::new(identity.clone(), fingerprint);
                let mut state = lock(&self.state);
                state.entries.insert(
                    identity,
                    CacheEntry::Pending {
                        fingerprint,
                        retained: Arc::clone(&retained),
                    },
                );
                retained
            }
        }
    }

    fn cancel_pending(self: &Arc<Self>, identity: &RequestIdentity, error: FsError) {
        let retained = {
            let mut state = lock(&self.state);
            match state.entries.remove(identity) {
                Some(CacheEntry::Pending { retained, .. }) => Some(retained),
                Some(entry) => {
                    state.entries.insert(identity.clone(), entry);
                    None
                }
                None => None,
            }
        };
        if let Some(retained) = retained {
            retained.publish(Err(error));
        }
    }

    fn retain_failure(
        self: &Arc<Self>,
        identity: &RequestIdentity,
        lifetime: RequestLifetime,
        slot: Option<RequestOperationSlot>,
        error: FsError,
    ) -> Arc<RetainedRequest> {
        self.finish(identity, lifetime, slot, Err(error))
    }

    fn complete(
        self: &Arc<Self>,
        mut accepted: AcceptedRequest,
        result: Result<PreparedBatchResult, FsError>,
    ) -> Arc<RetainedRequest> {
        let slot = accepted.operation_slot.take();
        accepted.completed = true;
        self.finish(&accepted.identity, accepted.lifetime, slot, result)
    }

    fn finish(
        self: &Arc<Self>,
        identity: &RequestIdentity,
        lifetime: RequestLifetime,
        mut slot: Option<RequestOperationSlot>,
        result: Result<PreparedBatchResult, FsError>,
    ) -> Arc<RetainedRequest> {
        let retained = {
            let state = lock(&self.state);
            match state.entries.get(identity) {
                Some(CacheEntry::Pending { retained, .. })
                | Some(CacheEntry::Retained { retained, .. }) => Arc::clone(retained),
                None => {
                    RetainedRequest::new(identity.clone(), RequestFingerprint::from_bytes([0; 32]))
                }
            }
        };
        retained.publish(result);
        match lifetime {
            RequestLifetime::OneShot | RequestLifetime::InFlightOnly => {
                {
                    let mut state = lock(&self.state);
                    state.entries.remove(identity);
                }
                drop(slot);
            }
            RequestLifetime::CanonicalDedup | RequestLifetime::ReplayWindow(_) => {
                let expires_at = match lifetime {
                    RequestLifetime::ReplayWindow(window) => Some(Instant::now() + window),
                    RequestLifetime::CanonicalDedup => None,
                    _ => None,
                };
                let assumed = slot.as_mut().map(RequestOperationSlot::transfer);
                if let Some(slot) = slot.as_mut() {
                    slot.disarm();
                }
                let mut state = lock(&self.state);
                state.entries.insert(
                    identity.clone(),
                    CacheEntry::Retained {
                        retained: Arc::clone(&retained),
                        expires_at,
                        slot: assumed,
                    },
                );
            }
        }
        retained
    }

    fn evict_expired(&self, state: &mut CacheState) {
        let now = Instant::now();
        let mut released = Vec::new();
        state.entries.retain(|_, entry| match entry {
            CacheEntry::Pending { .. } => true,
            CacheEntry::Retained {
                expires_at, slot, ..
            } => {
                let expired = expires_at.is_some_and(|deadline| deadline <= now);
                if expired && let Some(mut owned) = slot.take() {
                    if owned.active {
                        state.used_slots = state.used_slots.saturating_sub(1);
                        owned.disarm();
                    }
                    released.push(owned);
                }
                !expired
            }
        });
        drop(released);
    }

    fn release_slot(&self) {
        let mut state = lock(&self.state);
        state.used_slots = state.used_slots.saturating_sub(1);
    }
}

fn existingfingerprint(entry: &CacheEntry) -> &RequestFingerprint {
    match entry {
        CacheEntry::Pending { fingerprint, .. } => fingerprint,
        CacheEntry::Retained { retained, .. } => &retained.fingerprint,
    }
}

fn retained_of(entry: &CacheEntry) -> &Arc<RetainedRequest> {
    match entry {
        CacheEntry::Pending { retained, .. } | CacheEntry::Retained { retained, .. } => retained,
    }
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

#[cfg(test)]
mod tests {
    use super::{RequestCache, RequestLookup};
    use crate::fs::errors::FsError;
    use crate::fs::mutation::types::{
        PreparedBatchResult, RequestFingerprint, RequestIdentity, RequestLifetime,
    };
    use crate::fs::types::FileAttributes;
    use std::sync::Arc;
    use std::time::Duration;
    use uuid::Uuid;

    fn fingerprint(tag: u8) -> RequestFingerprint {
        RequestFingerprint::from_parts(&[&[tag]])
    }

    fn attrs() -> FileAttributes {
        FileAttributes {
            size: 1,
            ..FileAttributes::default()
        }
    }

    fn result() -> PreparedBatchResult {
        PreparedBatchResult {
            members: vec![(1, attrs())],
            cutoff: None,
        }
    }

    fn nbd(handle: u64) -> RequestIdentity {
        RequestIdentity::Nbd {
            connection_incarnation: 7,
            handle,
        }
    }

    fn nfs(connection: u64, xid: u32) -> RequestIdentity {
        RequestIdentity::Nfs {
            server_incarnation: Uuid::nil(),
            connection_incarnation: connection,
            xid,
        }
    }

    fn expect_vacant(lookup: RequestLookup) -> super::RequestVacancy {
        match lookup {
            RequestLookup::Vacant(vacancy) => vacancy,
            other => panic!("expected vacant, got mismatch/join/backpressure: {other:?}"),
        }
    }

    #[tokio::test]
    async fn join_before_admission_shares_one_pending_result() {
        let cache = RequestCache::new(8);
        let identity = nbd(1);
        let lookup = cache
            .lookup_or_reserve(
                identity.clone(),
                fingerprint(1),
                RequestLifetime::InFlightOnly,
            )
            .unwrap();
        let pending = expect_vacant(lookup).begin_pending();
        let joined = match cache
            .lookup_or_reserve(
                identity.clone(),
                fingerprint(1),
                RequestLifetime::InFlightOnly,
            )
            .unwrap()
        {
            RequestLookup::Joined(retained) => retained,
            other => panic!("expected join, got {other:?}"),
        };
        assert_eq!(cache.used_slots(), 1);
        let waiter = tokio::spawn({
            let joined = Arc::clone(&joined);
            async move { joined.wait().await }
        });
        let accepted = pending.accept();
        cache.complete(accepted, Ok(result()));
        assert_eq!(waiter.await.unwrap().unwrap().members[0].0, 1);
    }

    #[test]
    fn fingerprint_auth_or_stability_mismatch_is_rejected() {
        let cache = RequestCache::new(8);
        let identity = RequestIdentity::NineP {
            session_incarnation: 1,
            operation_id: [9; 16],
        };
        let pending = expect_vacant(
            cache
                .lookup_or_reserve(
                    identity.clone(),
                    fingerprint(1),
                    RequestLifetime::InFlightOnly,
                )
                .unwrap(),
        )
        .begin_pending();
        assert!(matches!(
            cache
                .lookup_or_reserve(identity, fingerprint(2), RequestLifetime::InFlightOnly)
                .unwrap(),
            RequestLookup::FingerprintMismatch
        ));
        pending.cancel();
    }

    #[test]
    fn pending_entries_are_not_evicted_under_pressure() {
        let cache = RequestCache::new(1);
        let pending = expect_vacant(
            cache
                .lookup_or_reserve(nbd(1), fingerprint(1), RequestLifetime::InFlightOnly)
                .unwrap(),
        )
        .begin_pending();
        assert!(matches!(
            cache
                .lookup_or_reserve(nbd(2), fingerprint(1), RequestLifetime::InFlightOnly)
                .unwrap(),
            RequestLookup::Backpressured
        ));
        assert_eq!(cache.len(), 1);
        assert_eq!(cache.used_slots(), 1);
        pending.cancel();
        assert_eq!(cache.len(), 0);
        assert_eq!(cache.used_slots(), 0);
    }

    #[tokio::test]
    async fn completed_replay_window_expires() {
        let cache = RequestCache::new(2);
        let pending = expect_vacant(
            cache
                .lookup_or_reserve(
                    nbd(1),
                    fingerprint(1),
                    RequestLifetime::ReplayWindow(Duration::from_millis(10)),
                )
                .unwrap(),
        )
        .begin_pending();
        cache.complete(pending.accept(), Ok(result()));
        assert!(matches!(
            cache
                .lookup_or_reserve(
                    nbd(1),
                    fingerprint(1),
                    RequestLifetime::ReplayWindow(Duration::from_millis(10)),
                )
                .unwrap(),
            RequestLookup::Joined(_)
        ));
        tokio::time::sleep(Duration::from_millis(20)).await;
        let again = cache
            .lookup_or_reserve(
                nbd(1),
                fingerprint(1),
                RequestLifetime::ReplayWindow(Duration::from_millis(10)),
            )
            .unwrap();
        assert!(matches!(again, RequestLookup::Vacant(_)));
        drop(again);
    }

    #[test]
    fn cache_pressure_returns_backpressure() {
        let cache = RequestCache::new(1);
        let pending = expect_vacant(
            cache
                .lookup_or_reserve(
                    nbd(1),
                    fingerprint(1),
                    RequestLifetime::ReplayWindow(Duration::from_secs(60)),
                )
                .unwrap(),
        )
        .begin_pending();
        cache.complete(pending.accept(), Ok(result()));
        assert!(matches!(
            cache
                .lookup_or_reserve(nbd(2), fingerprint(2), RequestLifetime::InFlightOnly)
                .unwrap(),
            RequestLookup::Backpressured
        ));
    }

    #[test]
    fn one_shot_direct_calls_do_not_join_after_completion() {
        let cache = RequestCache::new(4);
        let identity = RequestIdentity::DirectOneShot(Uuid::from_u128(42));
        let pending = expect_vacant(
            cache
                .lookup_or_reserve(identity.clone(), fingerprint(1), RequestLifetime::OneShot)
                .unwrap(),
        )
        .begin_pending();
        cache.complete(pending.accept(), Ok(result()));
        assert_eq!(cache.len(), 0);
        let again = cache
            .lookup_or_reserve(identity, fingerprint(1), RequestLifetime::OneShot)
            .unwrap();
        assert!(matches!(again, RequestLookup::Vacant(_)));
        drop(again);
    }

    #[test]
    fn dropped_accepted_request_releases_pending_entry_and_slot() {
        let cache = RequestCache::new(1);
        let accepted = expect_vacant(
            cache
                .lookup_or_reserve(nbd(1), fingerprint(1), RequestLifetime::InFlightOnly)
                .unwrap(),
        )
        .begin_pending()
        .accept();
        assert_eq!(cache.used_slots(), 1);

        drop(accepted);

        assert_eq!(cache.used_slots(), 0);
        assert_eq!(cache.len(), 0);
    }

    #[tokio::test]
    async fn dropped_accepted_request_wakes_existing_joiner_with_error() {
        let cache = RequestCache::new(1);
        let identity = nbd(1);
        let accepted = expect_vacant(
            cache
                .lookup_or_reserve(
                    identity.clone(),
                    fingerprint(1),
                    RequestLifetime::InFlightOnly,
                )
                .unwrap(),
        )
        .begin_pending()
        .accept();
        let joined = match cache
            .lookup_or_reserve(identity, fingerprint(1), RequestLifetime::InFlightOnly)
            .unwrap()
        {
            RequestLookup::Joined(retained) => retained,
            other => panic!("expected join, got {other:?}"),
        };
        let waiter = tokio::spawn(async move { joined.wait().await });

        drop(accepted);

        assert!(matches!(
            tokio::time::timeout(Duration::from_secs(1), waiter).await,
            Ok(Ok(Err(FsError::IoError)))
        ));
        assert_eq!(cache.used_slots(), 0);
        assert_eq!(cache.len(), 0);
    }

    #[test]
    fn in_flight_nbd_collision_rejectsfingerprint_mismatch() {
        let cache = RequestCache::new(4);
        let pending = expect_vacant(
            cache
                .lookup_or_reserve(nbd(99), fingerprint(1), RequestLifetime::InFlightOnly)
                .unwrap(),
        )
        .begin_pending();
        assert!(matches!(
            cache
                .lookup_or_reserve(nbd(99), fingerprint(9), RequestLifetime::InFlightOnly)
                .unwrap(),
            RequestLookup::FingerprintMismatch
        ));
        pending.cancel();
    }

    #[test]
    fn nfs_reconnect_address_reuse_does_not_join_old_xid() {
        let cache = RequestCache::new(4);
        let first = expect_vacant(
            cache
                .lookup_or_reserve(nfs(1, 7), fingerprint(1), RequestLifetime::InFlightOnly)
                .unwrap(),
        )
        .begin_pending();
        let reused_address = cache
            .lookup_or_reserve(nfs(2, 7), fingerprint(1), RequestLifetime::InFlightOnly)
            .unwrap();
        assert!(matches!(reused_address, RequestLookup::Vacant(_)));
        drop(reused_address);
        first.cancel();
    }

    #[test]
    fn pending_failure_is_retained_for_joiners() {
        let cache = RequestCache::new(4);
        let identity = RequestIdentity::DirectTagged {
            caller_incarnation: Uuid::nil(),
            operation_id: 1,
        };
        let pending = expect_vacant(
            cache
                .lookup_or_reserve(
                    identity.clone(),
                    fingerprint(1),
                    RequestLifetime::CanonicalDedup,
                )
                .unwrap(),
        )
        .begin_pending();
        let retained = pending.fail(FsError::IoError);
        assert_eq!(retained.identity(), &identity);
        assert_eq!(retained.fingerprint(), fingerprint(1));
        assert!(matches!(retained.completed(), Some(Err(FsError::IoError))));
        assert!(matches!(
            cache
                .lookup_or_reserve(identity, fingerprint(1), RequestLifetime::CanonicalDedup,)
                .unwrap(),
            RequestLookup::Joined(_)
        ));
        let _ = pending_retained_slot(&cache);
    }

    fn pending_retained_slot(cache: &RequestCache) -> usize {
        cache.used_slots()
    }
}
