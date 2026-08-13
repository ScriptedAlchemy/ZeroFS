//! Extent writes queued for commit but not yet applied, tracked per inode.
//!
//! # The invariant this replaces
//!
//! Staging an extent edit is a read-then-write: it reads the extents it only
//! partially overwrites, reads each edited extent's current `FrameLoc` to
//! debit the frame it supersedes, and may splice the per-inode tail cache.
//! Every one of those reads resolves against state the commit worker
//! publishes at *apply*, so a write whose batch has not applied yet is
//! invisible to them.
//!
//! Holding the per-inode lock across the whole commit used to make that
//! impossible -- the comment in `delete_range` names it directly: the inode
//! write lock is what stops one frame being debited twice. Once the write path
//! drops that lock at submit, the guarantee has to come from somewhere else,
//! and the consequences of losing it are not subtle:
//!
//! * a partial overwrite read before the predecessor applies rebuilds the
//!   extent from pre-predecessor bytes and silently drops that write;
//! * a second overwrite of the same extent debits the same superseded frame
//!   twice and never debits the frame the predecessor allocated, so a
//!   segment's live-byte counter can reach zero while it is still referenced
//!   and GC will reclaim live data.
//!
//! # What is actually serialised
//!
//! Both hazards are confined to the extents a queued write touches, so this
//! registry serialises on that range rather than on the inode. Writers whose
//! extent ranges are disjoint -- sequential appends, and the striped
//! fixed-offset member writes this pipelining exists for -- never wait on each
//! other, while any writer that would read an extent a queued write owns
//! blocks until that write has applied *and* published its tail bytes.
//!
//! Paths that rewrite or drop an inode's extents wholesale (truncate, hole
//! punch, unlink, compaction) wait for every queued write on the inode; the
//! wait lives inside the `ExtentStore` entry points so a caller cannot forget
//! it.
//!
//! A registration is retired by its guard, so a cancelled writer releases its
//! range without applying. That matches the pre-existing behaviour of a
//! cancelled write, which dropped the inode lock at the same point while its
//! transaction stayed queued.

use crate::fs::inode::InodeId;
use dashmap::DashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use tokio::sync::Notify;

/// One queued write's completion signal.
struct Completion {
    done: AtomicBool,
    notify: Notify,
}

impl Completion {
    fn complete(&self) {
        self.done.store(true, Ordering::Release);
        self.notify.notify_waiters();
    }

    async fn wait(&self) {
        loop {
            // Register before the check: completing between the two would
            // otherwise leave this waiter parked forever.
            let notified = self.notify.notified();
            if self.done.load(Ordering::Acquire) {
                return;
            }
            notified.await;
        }
    }
}

/// A queued write's inclusive extent range.
struct Entry {
    token: u64,
    start: u64,
    end: u64,
    completion: Arc<Completion>,
}

#[derive(Default)]
pub(super) struct InflightExtentWrites {
    inodes: DashMap<InodeId, Vec<Entry>>,
    next_token: AtomicU64,
}

impl InflightExtentWrites {
    /// Claim `[start, end]` on `id` until the returned guard drops. Callers
    /// register under the per-inode lock, so registrations for one inode are
    /// ordered by the same lock that orders their submissions.
    pub(super) fn register(
        self: &Arc<Self>,
        id: InodeId,
        start: u64,
        end: u64,
    ) -> InflightWriteGuard {
        let token = self.next_token.fetch_add(1, Ordering::Relaxed);
        let completion = Arc::new(Completion {
            done: AtomicBool::new(false),
            notify: Notify::new(),
        });
        self.inodes.entry(id).or_default().push(Entry {
            token,
            start,
            end,
            completion: Arc::clone(&completion),
        });
        InflightWriteGuard {
            registry: Arc::clone(self),
            id,
            token,
            completion,
        }
    }

    /// Wait until no queued write on `id` overlaps `[start, end]`.
    pub(super) async fn wait_for_overlap(&self, id: InodeId, start: u64, end: u64) {
        self.wait_matching(id, Some((start, end))).await
    }

    /// Wait until no write on `id` is queued at all.
    pub(super) async fn wait_for_all(&self, id: InodeId) {
        self.wait_matching(id, None).await
    }

    async fn wait_matching(&self, id: InodeId, range: Option<(u64, u64)>) {
        loop {
            let pending: Vec<Arc<Completion>> = {
                let Some(entries) = self.inodes.get(&id) else {
                    return;
                };
                entries
                    .iter()
                    .filter(|entry| match range {
                        Some((start, end)) => entry.start <= end && start <= entry.end,
                        None => true,
                    })
                    .map(|entry| Arc::clone(&entry.completion))
                    .collect()
                // The shard guard is released here: awaiting while holding it
                // would block the guard drop that resolves this wait.
            };
            if pending.is_empty() {
                return;
            }
            for completion in pending {
                completion.wait().await;
            }
        }
    }

    #[cfg(test)]
    fn queued_ranges(&self, id: InodeId) -> Vec<(u64, u64)> {
        self.inodes
            .get(&id)
            .map(|entries| entries.iter().map(|e| (e.start, e.end)).collect())
            .unwrap_or_default()
    }
}

/// Releases one queued write's extent range. Held from submit until the commit
/// reply resolves *and* the tail-cache update has been applied, so a waiter
/// that resumes can never observe a stale tail for a range it is about to
/// read-modify-write.
#[must_use = "hold the guard until the write has committed and published its tail"]
pub(crate) struct InflightWriteGuard {
    registry: Arc<InflightExtentWrites>,
    id: InodeId,
    token: u64,
    completion: Arc<Completion>,
}

impl Drop for InflightWriteGuard {
    fn drop(&mut self) {
        if let Some(mut entries) = self.registry.inodes.get_mut(&self.id) {
            entries.retain(|entry| entry.token != self.token);
            let empty = entries.is_empty();
            drop(entries);
            if empty {
                self.registry
                    .inodes
                    .remove_if(&self.id, |_, entries| entries.is_empty());
            }
        }
        self.completion.complete();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;

    #[tokio::test]
    async fn disjoint_ranges_do_not_wait() {
        let registry = Arc::new(InflightExtentWrites::default());
        let _queued = registry.register(1, 0, 7);
        // Would hang if a disjoint range serialised behind the queued write.
        registry.wait_for_overlap(1, 8, 15).await;
        registry.wait_for_overlap(2, 0, 7).await;
    }

    #[tokio::test]
    async fn an_overlapping_range_waits_for_the_queued_write() {
        let registry = Arc::new(InflightExtentWrites::default());
        let queued = registry.register(1, 4, 9);
        let resumed = Arc::new(AtomicUsize::new(0));

        let waiter_registry = Arc::clone(&registry);
        let waiter_resumed = Arc::clone(&resumed);
        let waiter = tokio::spawn(async move {
            waiter_registry.wait_for_overlap(1, 9, 12).await;
            waiter_resumed.store(1, Ordering::SeqCst);
        });

        tokio::task::yield_now().await;
        assert_eq!(resumed.load(Ordering::SeqCst), 0, "waiter resumed early");

        drop(queued);
        waiter.await.unwrap();
        assert_eq!(resumed.load(Ordering::SeqCst), 1);
        assert!(registry.queued_ranges(1).is_empty());
    }

    #[tokio::test]
    async fn waiting_for_all_covers_every_queued_range() {
        let registry = Arc::new(InflightExtentWrites::default());
        let first = registry.register(1, 0, 3);
        let second = registry.register(1, 100, 103);
        assert_eq!(registry.queued_ranges(1).len(), 2);

        let waiter_registry = Arc::clone(&registry);
        let waiter = tokio::spawn(async move { waiter_registry.wait_for_all(1).await });
        tokio::task::yield_now().await;
        assert!(!waiter.is_finished());

        drop(first);
        tokio::task::yield_now().await;
        assert!(!waiter.is_finished(), "one range still queued");

        drop(second);
        waiter.await.unwrap();
    }

    /// `lock_inode_settled` is what restores the exclusion the inode lock used
    /// to imply. Compaction's CAS depends on it: it compares the stored
    /// FrameLoc against the one it gathered, so a queued overwrite it cannot
    /// see leaves the comparison equal and the swap reverts the extent to the
    /// relocated copy of the superseded bytes.
    #[tokio::test]
    async fn a_settled_lock_does_not_return_while_a_write_is_queued() {
        let (store, _db) = super::super::test_util::make().await;
        let queued = store.register_inflight_write(7, 0, 3);

        let settling = tokio::spawn({
            let store = store.clone();
            async move {
                let _guard = store.lock_inode_settled(7).await;
            }
        });
        for _ in 0..32 {
            tokio::task::yield_now().await;
        }
        assert!(
            !settling.is_finished(),
            "the lock settled while a write was still queued"
        );

        drop(queued);
        settling.await.unwrap();
    }

    #[tokio::test]
    async fn a_guard_retires_only_its_own_registration() {
        let registry = Arc::new(InflightExtentWrites::default());
        let first = registry.register(1, 0, 3);
        let second = registry.register(1, 0, 3);
        drop(first);
        assert_eq!(registry.queued_ranges(1), vec![(0, 3)]);
        drop(second);
        assert!(registry.queued_ranges(1).is_empty());
    }
}
