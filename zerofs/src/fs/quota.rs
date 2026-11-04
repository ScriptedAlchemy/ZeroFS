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
#[allow(dead_code)]
const STATE_TERMINAL: u8 = 3;
const STATE_RELEASED: u8 = 4;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(dead_code)]
pub(crate) enum QuotaReservationState {
    Provisional,
    Accepted,
    Canonical,
    TerminalRetained,
    Released,
}

impl QuotaReservationState {
    #[allow(dead_code)]
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

#[derive(Debug)]
struct QuotaCounters {
    committed: u64,
    pending: u64,
    poisoned: Option<&'static str>,
}

/// Visible logical-size budget shared by every write protocol.
#[derive(Debug)]
pub(crate) struct LogicalQuota {
    max_bytes: u64,
    inner: Mutex<QuotaCounters>,
}

/// One growth claim. Drop releases only while [`QuotaReservationState::Provisional`].
#[derive(Debug)]
pub(crate) struct ProvisionalQuotaReservation {
    quota: Arc<LogicalQuota>,
    bytes: u64,
    state: AtomicU8,
}

/// Metadata quota ownership transferred to the canonical commit worker.
/// Dropping a caller must neither refund queued growth nor lose a reclaim.
pub(crate) enum QuotaSettlement {
    Growth(ProvisionalQuotaReservation),
    Shrink {
        quota: Arc<LogicalQuota>,
        bytes: u64,
    },
}

impl QuotaSettlement {
    pub(crate) fn finish(self, result: Result<(), FsError>) {
        if result.is_ok() {
            match self {
                Self::Growth(reservation) => {
                    reservation.accept();
                    reservation.canonical();
                }
                Self::Shrink { quota, bytes } => quota.release_committed(bytes),
            }
        }
        // Failed growth remains provisional and refunds itself on drop.
    }
}

impl LogicalQuota {
    pub(crate) fn new(max_bytes: u64, committed: u64) -> Arc<Self> {
        Arc::new(Self {
            max_bytes,
            inner: Mutex::new(QuotaCounters {
                committed,
                pending: 0,
                poisoned: None,
            }),
        })
    }

    #[allow(dead_code)]
    pub(crate) fn max_bytes(&self) -> u64 {
        self.max_bytes
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, QuotaCounters> {
        self.inner.lock().expect("logical quota mutex poisoned")
    }

    fn poison(inner: &mut QuotaCounters, reason: &'static str) {
        if inner.poisoned.is_none() {
            inner.poisoned = Some(reason);
        }
    }

    fn ensure_healthy(inner: &QuotaCounters) -> Result<(), FsError> {
        if inner.poisoned.is_some() {
            Err(FsError::IoError)
        } else {
            Ok(())
        }
    }

    #[allow(dead_code)]
    pub(crate) fn is_poisoned(&self) -> bool {
        self.lock().poisoned.is_some()
    }

    #[allow(dead_code)]
    pub(crate) fn committed_bytes(&self) -> u64 {
        self.lock().committed
    }

    #[allow(dead_code)]
    pub(crate) fn pending_bytes(&self) -> u64 {
        self.lock().pending
    }

    #[allow(dead_code)]
    pub(crate) fn visible_bytes(&self) -> u64 {
        let mut inner = self.lock();
        match inner.committed.checked_add(inner.pending) {
            Some(visible) => visible,
            None => {
                Self::poison(&mut inner, "visible size overflow");
                u64::MAX
            }
        }
    }

    /// CAS-reserve `bytes` of growth against committed plus pending visible size.
    pub(crate) fn reserve(
        self: &Arc<Self>,
        bytes: u64,
    ) -> Result<ProvisionalQuotaReservation, FsError> {
        {
            let mut inner = self.lock();
            Self::ensure_healthy(&inner)?;
            if bytes == 0 {
                drop(inner);
                return Ok(ProvisionalQuotaReservation {
                    quota: Arc::clone(self),
                    bytes: 0,
                    state: AtomicU8::new(STATE_PROVISIONAL),
                });
            }
            let visible = match inner.committed.checked_add(inner.pending) {
                Some(visible) => visible,
                None => {
                    Self::poison(&mut inner, "visible size overflow");
                    return Err(FsError::IoError);
                }
            };
            let next = match visible.checked_add(bytes) {
                Some(next) => next,
                None => {
                    Self::poison(&mut inner, "reservation overflow");
                    return Err(FsError::IoError);
                }
            };
            if next > self.max_bytes {
                return Err(FsError::NoSpace);
            }
            inner.pending = match inner.pending.checked_add(bytes) {
                Some(pending) => pending,
                None => {
                    Self::poison(&mut inner, "pending overflow");
                    return Err(FsError::IoError);
                }
            };
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
        if inner.poisoned.is_some() {
            return;
        }
        match inner.committed.checked_sub(bytes) {
            Some(committed) => inner.committed = committed,
            None => Self::poison(&mut inner, "committed underflow"),
        }
    }

    fn release_pending(&self, bytes: u64) {
        if bytes == 0 {
            return;
        }
        let mut inner = self.lock();
        if inner.poisoned.is_some() {
            return;
        }
        match inner.pending.checked_sub(bytes) {
            Some(pending) => inner.pending = pending,
            None => Self::poison(&mut inner, "pending underflow"),
        }
    }

    fn transfer_to_committed(&self, bytes: u64) {
        if bytes == 0 {
            return;
        }
        let mut inner = self.lock();
        if inner.poisoned.is_some() {
            return;
        }
        match (
            inner.pending.checked_sub(bytes),
            inner.committed.checked_add(bytes),
        ) {
            (Some(pending), Some(committed)) => {
                inner.pending = pending;
                inner.committed = committed;
            }
            _ => Self::poison(&mut inner, "canonical transfer overflow or underflow"),
        }
    }
}

impl ProvisionalQuotaReservation {
    #[allow(dead_code)]
    pub(crate) fn bytes(&self) -> u64 {
        self.bytes
    }

    #[allow(dead_code)]
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
    #[allow(dead_code)]
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
    use super::{LogicalQuota, QuotaReservationState, QuotaSettlement};
    use crate::fs::ZeroFS;
    use crate::fs::errors::FsError;
    use crate::fs::test_util::test_creds;
    use crate::fs::types::{AuthContext, FallocateMode, SetAttributes, SetSize};
    use crate::test_helpers::test_helpers_mod::test_auth;
    use bytes::Bytes;
    use std::sync::Arc;

    async fn cancel_at_apply(
        fs: &Arc<ZeroFS>,
        operation: impl std::future::Future<Output = Result<(), FsError>> + Send + 'static,
        expected_pending: u64,
    ) {
        // The worker has accepted the transaction but cannot apply it while
        // this permit is held. No timer or scheduler race decides cancellation.
        let blocked = fs.db.flush_barrier().write_owned().await;
        let reached = fs.write_coordinator.probe_next_apply();
        let task = tokio::spawn(operation);
        tokio::time::timeout(std::time::Duration::from_secs(10), reached)
            .await
            .expect("metadata commit did not reach apply")
            .unwrap();
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        assert_eq!(fs.quota.pending_bytes(), expected_pending);
        drop(blocked);
        tokio::time::timeout(
            std::time::Duration::from_secs(10),
            fs.write_coordinator.barrier(),
        )
        .await
        .expect("cancelled request stranded its queued commit")
        .unwrap();
    }

    #[tokio::test]
    async fn cancelled_metadata_growth_keeps_queued_quota() {
        for operation in 0..3 {
            let mut fs = ZeroFS::new_in_memory().await.unwrap();
            fs.max_bytes = 4096;
            fs.quota = LogicalQuota::new(4096, 0);
            let fs = Arc::new(fs);
            let (id, _) = fs
                .create(&test_creds(), 0, b"grow", &SetAttributes::default())
                .await
                .unwrap();
            let task_fs = Arc::clone(&fs);
            cancel_at_apply(
                &fs,
                async move {
                    if operation == 2 {
                        task_fs
                            .setattr(
                                &test_creds(),
                                id,
                                &SetAttributes {
                                    size: SetSize::Set(4096),
                                    ..Default::default()
                                },
                            )
                            .await?;
                    } else {
                        let mode = if operation == 0 {
                            FallocateMode::Allocate
                        } else {
                            FallocateMode::ZeroRange { keep_size: false }
                        };
                        task_fs
                            .fallocate_opened(&(&test_auth()).into(), id, 0, 4096, mode)
                            .await?;
                    }
                    Ok(())
                },
                4096,
            )
            .await;

            assert_eq!(fs.quota.committed_bytes(), 4096);
            assert_eq!(fs.quota.pending_bytes(), 0);
            assert_eq!(fs.global_stats.get_totals().0, 4096);
            let auth: AuthContext = (&test_auth()).into();
            assert!(matches!(
                fs.write(&auth, id, 4096, &Bytes::from_static(b"x")).await,
                Err(FsError::NoSpace)
            ));
            fs.setattr(
                &test_creds(),
                id,
                &SetAttributes {
                    size: SetSize::Set(0),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
            assert!(!fs.quota.is_poisoned());
            fs.write(&auth, id, 0, &Bytes::from_static(b"after-cancellation"))
                .await
                .unwrap();
        }
    }

    #[tokio::test]
    async fn cancelled_metadata_reclaim_releases_quota() {
        for operation in 0..4 {
            let mut fs = ZeroFS::new_in_memory().await.unwrap();
            fs.max_bytes = 4096;
            fs.quota = LogicalQuota::new(4096, 0);
            let fs = Arc::new(fs);
            let (id, _) = fs
                .create(&test_creds(), 0, b"target", &SetAttributes::default())
                .await
                .unwrap();
            let (replacement, _) = fs
                .create(&test_creds(), 0, b"replacement", &SetAttributes::default())
                .await
                .unwrap();
            let auth: AuthContext = (&test_auth()).into();
            fs.write(&auth, id, 0, &Bytes::from(vec![b'a'; 4096]))
                .await
                .unwrap();
            if operation == 3 {
                fs.open_handle_inc(id);
                fs.remove(&auth, 0, b"target").await.unwrap();
                assert_eq!(fs.quota.committed_bytes(), 4096);
            }
            let task_fs = Arc::clone(&fs);
            cancel_at_apply(
                &fs,
                async move {
                    let auth: AuthContext = (&test_auth()).into();
                    match operation {
                        0 => {
                            task_fs
                                .setattr(
                                    &test_creds(),
                                    id,
                                    &SetAttributes {
                                        size: SetSize::Set(0),
                                        ..Default::default()
                                    },
                                )
                                .await?;
                        }
                        1 => task_fs.remove(&auth, 0, b"target").await?,
                        2 => {
                            task_fs
                                .rename(&auth, 0, b"replacement", 0, b"target")
                                .await?;
                        }
                        _ => task_fs.handle_closed(id).await,
                    }
                    Ok(())
                },
                0,
            )
            .await;

            assert_eq!(fs.quota.committed_bytes(), 0);
            assert_eq!(fs.global_stats.get_totals().0, 0);
            assert!(!fs.quota.is_poisoned());
            fs.write(&auth, replacement, 0, &Bytes::from(vec![b'b'; 4096]))
                .await
                .expect("reclaimed bytes must be available to another file");
        }
    }

    #[tokio::test]
    async fn rename_over_open_target_keeps_quota_until_last_close() {
        let fs = ZeroFS::new_in_memory().await.unwrap();
        let auth: AuthContext = (&test_auth()).into();
        let (source, _) = fs
            .create(&test_creds(), 0, b"source", &SetAttributes::default())
            .await
            .unwrap();
        let (target, _) = fs
            .create(&test_creds(), 0, b"target", &SetAttributes::default())
            .await
            .unwrap();
        fs.write(&auth, source, 0, &Bytes::from_static(b"abc"))
            .await
            .unwrap();
        fs.write(&auth, target, 0, &Bytes::from_static(b"12345"))
            .await
            .unwrap();
        fs.open_handle_inc(target);
        fs.rename(&auth, 0, b"source", 0, b"target").await.unwrap();
        assert_eq!(fs.quota.committed_bytes(), 8);
        fs.handle_closed(target).await;
        assert_eq!(fs.quota.committed_bytes(), 3);
        assert!(!fs.quota.is_poisoned());
    }

    #[tokio::test]
    async fn durability_errors_preserve_applied_quota() {
        for operation in 0..6 {
            let mut fs = ZeroFS::new_in_memory_with_sync_writes(true).await.unwrap();
            fs.max_bytes = 4096;
            fs.quota = LogicalQuota::new(4096, 0);
            let auth: AuthContext = (&test_auth()).into();
            let (target, _) = fs
                .create(&test_creds(), 0, b"target", &SetAttributes::default())
                .await
                .unwrap();
            let (replacement, _) = fs
                .create(&test_creds(), 0, b"replacement", &SetAttributes::default())
                .await
                .unwrap();
            if operation >= 3 {
                fs.write(&auth, target, 0, &Bytes::from(vec![b'a'; 4096]))
                    .await
                    .unwrap();
            }
            let fail_flush = Arc::new(std::sync::atomic::AtomicBool::new(true));
            fs.flush_coordinator.set_local_durability_barrier({
                let fail_flush = Arc::clone(&fail_flush);
                Arc::new(move || {
                    let fail_flush = Arc::clone(&fail_flush);
                    Box::pin(async move {
                        if fail_flush.load(std::sync::atomic::Ordering::SeqCst) {
                            Err(FsError::IoError)
                        } else {
                            Ok(())
                        }
                    })
                })
            });
            let result = match operation {
                0 => fs
                    .fallocate_opened(&auth, target, 0, 4096, FallocateMode::Allocate)
                    .await
                    .map(|_| ()),
                1 | 3 => fs
                    .setattr(
                        &test_creds(),
                        target,
                        &SetAttributes {
                            size: SetSize::Set(if operation == 1 { 4096 } else { 0 }),
                            ..Default::default()
                        },
                    )
                    .await
                    .map(|_| ()),
                2 => fs
                    .write(&auth, target, 0, &Bytes::from(vec![b'b'; 4096]))
                    .await
                    .map(|_| ()),
                4 => fs.remove(&auth, 0, b"target").await,
                _ => fs.rename(&auth, 0, b"replacement", 0, b"target").await,
            };
            assert!(
                matches!(result, Err(FsError::IoError)),
                "operation {operation}"
            );
            let applied_size = if operation < 3 { 4096 } else { 0 };
            assert_eq!(fs.global_stats.get_totals().0, applied_size);
            assert_eq!(fs.quota.committed_bytes(), applied_size);
            assert_eq!(fs.quota.pending_bytes(), 0);
            assert!(!fs.quota.is_poisoned());

            fail_flush.store(false, std::sync::atomic::Ordering::SeqCst);
            if operation < 3 {
                fs.setattr(
                    &test_creds(),
                    target,
                    &SetAttributes {
                        size: SetSize::Set(0),
                        ..Default::default()
                    },
                )
                .await
                .unwrap();
            }
            fs.write(&auth, replacement, 0, &Bytes::from(vec![b'c'; 4096]))
                .await
                .expect("applied reclaim must remain reusable after a durability failure");
        }
    }

    async fn cancelled_replay_protected_reclaim(rename: bool) {
        let mut fs = ZeroFS::new_in_memory().await.unwrap();
        fs.max_bytes = 4096;
        fs.quota = LogicalQuota::new(4096, 0);
        let fs = Arc::new(fs);
        let mut reclaims = fs.reclaim_rx.lock().unwrap().take().unwrap();
        let op_id = [7; 16];
        let (target, _) = fs
            .create_idempotent(
                &test_creds(),
                0,
                b"target",
                &SetAttributes::default(),
                op_id,
            )
            .await
            .unwrap();
        let (replacement, _) = fs
            .create(&test_creds(), 0, b"replacement", &SetAttributes::default())
            .await
            .unwrap();
        let auth: AuthContext = (&test_auth()).into();
        fs.write(&auth, target, 0, &Bytes::from(vec![b'a'; 4096]))
            .await
            .unwrap();
        assert_eq!(fs.open_handle_count(target), 0);
        assert!(fs.dedup.protects_replay_inode(target));
        let task_fs = Arc::clone(&fs);
        cancel_at_apply(
            &fs,
            async move {
                let auth: AuthContext = (&test_auth()).into();
                if rename {
                    task_fs.rename(&auth, 0, b"replacement", 0, b"target").await
                } else {
                    task_fs.remove(&auth, 0, b"target").await
                }
            },
            0,
        )
        .await;

        // Drive the production drainer one notification at a time. Its first
        // pass must register replay expiry even though the caller was aborted.
        assert_eq!(reclaims.try_recv().unwrap(), target);
        fs.reclaim_if_unreferenced(target).await;
        assert_eq!(fs.quota.committed_bytes(), 4096);
        tokio::time::pause();
        tokio::time::advance(crate::dedup::OP_ID_RETENTION + std::time::Duration::from_secs(1))
            .await;
        assert!(fs.dedup.get(&op_id).is_none());
        tokio::time::resume();
        assert_eq!(reclaims.try_recv().unwrap(), target);
        fs.reclaim_if_unreferenced(target).await;
        assert!(matches!(
            fs.inode_store.get(target).await,
            Err(FsError::NotFound)
        ));
        assert_eq!(fs.global_stats.get_totals().0, 0);
        assert_eq!(fs.quota.committed_bytes(), 0);
        assert!(!fs.quota.is_poisoned());
        fs.write(&auth, replacement, 0, &Bytes::from(vec![b'b'; 4096]))
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn cancelled_remove_reclaims_after_replay_expiry() {
        cancelled_replay_protected_reclaim(false).await;
    }

    #[tokio::test]
    async fn cancelled_rename_reclaims_after_replay_expiry() {
        cancelled_replay_protected_reclaim(true).await;
    }

    #[test]
    fn failed_metadata_settlement_preserves_committed_quota() {
        let quota = LogicalQuota::new(8, 4);
        let growth = QuotaSettlement::Growth(quota.reserve(4).unwrap());
        growth.finish(Err(FsError::IoError));
        assert_eq!(quota.pending_bytes(), 0);
        assert_eq!(quota.committed_bytes(), 4);
        QuotaSettlement::Shrink {
            quota: Arc::clone(&quota),
            bytes: 4,
        }
        .finish(Err(FsError::IoError));
        assert_eq!(quota.committed_bytes(), 4);
        assert!(!quota.is_poisoned());
    }

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

    #[test]
    fn overflow_poisons_quota() {
        let quota = LogicalQuota::new(u64::MAX, u64::MAX);
        assert_eq!(quota.reserve(1).unwrap_err(), FsError::IoError);
        assert!(quota.is_poisoned());
        assert_eq!(quota.reserve(0).unwrap_err(), FsError::IoError);
        assert_eq!(quota.reserve(1).unwrap_err(), FsError::IoError);
    }

    #[test]
    fn underflow_poisons_quota() {
        let quota = LogicalQuota::new(80, 40);
        quota.release_committed(41);
        assert!(quota.is_poisoned());
        assert_eq!(quota.reserve(1).unwrap_err(), FsError::IoError);
        assert_eq!(quota.committed_bytes(), 40);
    }
}
