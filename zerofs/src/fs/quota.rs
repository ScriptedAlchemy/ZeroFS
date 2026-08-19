//! Logical filesystem quota with single-owner reservation transfer.
//!
//! Growth is CAS-reserved against committed plus pending visible size.
//! [`ProvisionalQuotaReservation`] drop rolls back only the `Provisional`
//! state. Acceptance, canonical apply, and terminal retention change
//! ownership without add/subtract. Shrink subtracts only after a successful
//! canonical commit.

use crate::fs::errors::FsError;
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::{Arc, Mutex};

const STATE_PROVISIONAL: u8 = 0;
const STATE_ACCEPTED: u8 = 1;
const STATE_CANONICAL: u8 = 2;
const STATE_TERMINAL: u8 = 3;
const STATE_RELEASED: u8 = 4;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum QuotaReservationState {
    Provisional,
    Accepted,
    Canonical,
    TerminalRetained,
    Released,
}

impl QuotaReservationState {
    fn from_u8(value: u8) -> Self {
        match value {
            STATE_PROVISIONAL => Self::Provisional,
            STATE_ACCEPTED => Self::Accepted,
            STATE_CANONICAL => Self::Canonical,
            STATE_TERMINAL => Self::TerminalRetained,
            _ => Self::Released,
        }
    }
}

struct QuotaCounters {
    committed: u64,
    pending: u64,
}

/// Visible logical-size budget shared by every write protocol.
pub(crate) struct LogicalQuota {
    max_bytes: u64,
    inner: Mutex<QuotaCounters>,
}

/// One growth claim. Drop releases only while [`QuotaReservationState::Provisional`].
pub(crate) struct ProvisionalQuotaReservation {
    quota: Arc<LogicalQuota>,
    bytes: u64,
    state: AtomicU8,
}

impl LogicalQuota {
    pub(crate) fn new(max_bytes: u64, committed: u64) -> Arc<Self> {
        Arc::new(Self {
            max_bytes,
            inner: Mutex::new(QuotaCounters {
                committed,
                pending: 0,
            }),
        })
    }

    pub(crate) fn max_bytes(&self) -> u64 {
        self.max_bytes
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, QuotaCounters> {
        self.inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    pub(crate) fn committed_bytes(&self) -> u64 {
        self.lock().committed
    }

    pub(crate) fn pending_bytes(&self) -> u64 {
        self.lock().pending
    }

    pub(crate) fn visible_bytes(&self) -> u64 {
        let inner = self.lock();
        inner.committed.saturating_add(inner.pending)
    }

    /// CAS-reserve `bytes` of growth against committed plus pending visible size.
    pub(crate) fn reserve(
        self: &Arc<Self>,
        bytes: u64,
    ) -> Result<ProvisionalQuotaReservation, FsError> {
        if bytes == 0 {
            return Ok(ProvisionalQuotaReservation {
                quota: Arc::clone(self),
                bytes: 0,
                state: AtomicU8::new(STATE_PROVISIONAL),
            });
        }
        {
            let mut inner = self.lock();
            let visible = inner.committed.saturating_add(inner.pending);
            if visible.saturating_add(bytes) > self.max_bytes {
                return Err(FsError::NoSpace);
            }
            inner.pending = inner.pending.saturating_add(bytes);
        }
        Ok(ProvisionalQuotaReservation {
            quota: Arc::clone(self),
            bytes,
            state: AtomicU8::new(STATE_PROVISIONAL),
        })
    }

    /// Subtract committed bytes after a shrink/reclaim commit succeeds.
    pub(crate) fn release_committed(&self, bytes: u64) {
        if bytes == 0 {
            return;
        }
        let mut inner = self.lock();
        inner.committed = inner.committed.saturating_sub(bytes);
    }

    fn release_pending(&self, bytes: u64) {
        if bytes == 0 {
            return;
        }
        let mut inner = self.lock();
        inner.pending = inner.pending.saturating_sub(bytes);
    }

    fn transfer_to_committed(&self, bytes: u64) {
        if bytes == 0 {
            return;
        }
        let mut inner = self.lock();
        inner.pending = inner.pending.saturating_sub(bytes);
        inner.committed = inner.committed.saturating_add(bytes);
    }
}

impl ProvisionalQuotaReservation {
    pub(crate) fn bytes(&self) -> u64 {
        self.bytes
    }

    pub(crate) fn state(&self) -> QuotaReservationState {
        QuotaReservationState::from_u8(self.state.load(Ordering::Acquire))
    }

    /// Mark the reservation accepted. Visible size is unchanged.
    pub(crate) fn accept(&self) {
        let _ = self.state.compare_exchange(
            STATE_PROVISIONAL,
            STATE_ACCEPTED,
            Ordering::AcqRel,
            Ordering::Acquire,
        );
    }

    /// Transfer accepted/provisional ownership into committed size.
    pub(crate) fn canonical(&self) {
        let previous = self.state.swap(STATE_CANONICAL, Ordering::AcqRel);
        if matches!(previous, STATE_PROVISIONAL | STATE_ACCEPTED) {
            self.quota.transfer_to_committed(self.bytes);
        }
    }

    /// Keep the pending charge after a terminal failure. Drop will not release.
    pub(crate) fn retain_terminal(&self) {
        let previous = self.state.swap(STATE_TERMINAL, Ordering::AcqRel);
        if previous == STATE_PROVISIONAL {
            // Stay charged in pending; do not roll back.
        }
    }
}

impl Drop for ProvisionalQuotaReservation {
    fn drop(&mut self) {
        if self
            .state
            .compare_exchange(
                STATE_PROVISIONAL,
                STATE_RELEASED,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_ok()
        {
            self.quota.release_pending(self.bytes);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{LogicalQuota, QuotaReservationState};
    use crate::fs::errors::FsError;
    use std::sync::Arc;

    #[test]
    fn concurrent_preparations_cannot_oversubscribe() {
        let quota = LogicalQuota::new(100, 0);
        let mut handles = Vec::new();
        for _ in 0..20 {
            let quota = Arc::clone(&quota);
            handles.push(std::thread::spawn(move || quota.reserve(10)));
        }
        let mut won = 0u64;
        let mut held = Vec::new();
        for handle in handles {
            match handle.join().unwrap() {
                Ok(token) => {
                    won += token.bytes();
                    held.push(token);
                }
                Err(FsError::NoSpace) => {}
                Err(error) => panic!("unexpected error: {error:?}"),
            }
        }
        assert_eq!(won, 100);
        assert_eq!(quota.visible_bytes(), 100);
        assert_eq!(quota.pending_bytes(), 100);
        drop(held);
        assert_eq!(quota.visible_bytes(), 0);
    }

    #[test]
    fn provisional_drop_releases_once() {
        let quota = LogicalQuota::new(50, 0);
        let token = quota.reserve(20).unwrap();
        assert_eq!(quota.pending_bytes(), 20);
        drop(token);
        assert_eq!(quota.pending_bytes(), 0);
        assert_eq!(quota.visible_bytes(), 0);
        let again = quota.reserve(50).unwrap();
        assert_eq!(again.bytes(), 50);
    }

    #[test]
    fn acceptance_transfers_without_arithmetic() {
        let quota = LogicalQuota::new(80, 10);
        let token = quota.reserve(30).unwrap();
        assert_eq!(quota.committed_bytes(), 10);
        assert_eq!(quota.pending_bytes(), 30);
        let visible = quota.visible_bytes();
        token.accept();
        assert_eq!(token.state(), QuotaReservationState::Accepted);
        assert_eq!(quota.committed_bytes(), 10);
        assert_eq!(quota.pending_bytes(), 30);
        assert_eq!(quota.visible_bytes(), visible);
    }

    #[test]
    fn canonical_apply_transfers_without_gap() {
        let quota = LogicalQuota::new(80, 10);
        let token = quota.reserve(30).unwrap();
        token.accept();
        let visible = quota.visible_bytes();
        token.canonical();
        assert_eq!(token.state(), QuotaReservationState::Canonical);
        assert_eq!(quota.visible_bytes(), visible);
        assert_eq!(quota.committed_bytes(), 40);
        assert_eq!(quota.pending_bytes(), 0);
        drop(token);
        assert_eq!(quota.committed_bytes(), 40);
        assert_eq!(quota.pending_bytes(), 0);
    }

    #[test]
    fn terminal_retains_pending_charge() {
        let quota = LogicalQuota::new(80, 0);
        let token = quota.reserve(25).unwrap();
        token.retain_terminal();
        assert_eq!(token.state(), QuotaReservationState::TerminalRetained);
        assert_eq!(quota.pending_bytes(), 25);
        drop(token);
        assert_eq!(quota.pending_bytes(), 25);
        assert_eq!(quota.visible_bytes(), 25);
        assert!(quota.reserve(60).is_err());
        assert!(quota.reserve(55).is_ok());
    }

    #[test]
    fn shrink_releases_only_after_commit() {
        let quota = LogicalQuota::new(80, 40);
        assert_eq!(quota.committed_bytes(), 40);
        // A shrink that has not committed must not change the budget.
        let staged = 15u64;
        assert_eq!(quota.visible_bytes(), 40);
        quota.release_committed(0);
        assert_eq!(quota.committed_bytes(), 40);
        quota.release_committed(staged);
        assert_eq!(quota.committed_bytes(), 25);
        assert_eq!(quota.visible_bytes(), 25);
        let _held = quota.reserve(55).unwrap();
        assert_eq!(quota.visible_bytes(), 80);
    }
}
