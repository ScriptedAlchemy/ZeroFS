use crate::db::{Db, Transaction};
use crate::fs::errors::FsError;
use crate::fs::inode::{Inode, InodeAttrs, InodeId};
use crate::fs::key_codec::KeyCodec;
use crate::fs::store::read_cache::{InvalidationGuard, MetadataCache};
use bytes::Bytes;
use dashmap::DashMap;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

pub const MAX_HARDLINKS_PER_INODE: u32 = u32::MAX;

const INODE_CACHE_BYTES: usize = 8 * 1024 * 1024;

type InodeCache = MetadataCache<InodeId, Inode>;

fn inode_cache_weight(_: &InodeId, inode: &Inode) -> usize {
    let allocated = match inode {
        Inode::File(inode) => inode.name.as_ref().map_or(0, Vec::len),
        Inode::Directory(inode) => inode.name.as_ref().map_or(0, Vec::len),
        Inode::Symlink(inode) => inode.target.len() + inode.name.as_ref().map_or(0, Vec::len),
        Inode::Fifo(inode)
        | Inode::Socket(inode)
        | Inode::CharDevice(inode)
        | Inode::BlockDevice(inode) => inode.name.as_ref().map_or(0, Vec::len),
    };
    std::mem::size_of::<InodeId>() + std::mem::size_of::<Inode>() + allocated
}

/// One inode value whose commit is queued but not yet applied. `value` is
/// `None` for a queued deletion.
struct PendingInode {
    seq: u64,
    value: Option<Inode>,
}

/// Inode values submitted to the write coordinator but not yet applied.
///
/// # Why an overlay is needed at all
///
/// The write path releases the per-inode lock as soon as its transaction is
/// queued, so the next writer stages while the previous commit is still in
/// flight (see `fs::ops::io::write_idempotent_inner`). The read cache is only
/// promoted when the batch *applies*, and for most of the commit the affected
/// keys are actively invalidated -- inside that window [`InodeStore::get`]
/// falls through to the database and returns the pre-write value. A successor
/// that read that value would compute `max(stale_size, its own end)` and
/// silently drop the predecessor's size update.
///
/// The overlay closes the window from the other side: a mutation becomes
/// visible the instant its transaction is queued and stays visible until the
/// commit reply resolves. By then the apply has already published the same
/// value into the read cache, or the batch failed before the apply and the
/// cache still holds the last committed value. There is no instant at which
/// neither layer holds the newest submitted value.
///
/// # Ordering
///
/// Entries are stamped with a monotonic sequence and are removed only by the
/// installer that still owns the slot, so a later submitter always wins and a
/// resolving predecessor can never resurrect its own older value. Submissions
/// for one inode are ordered by the per-inode lock, and the coordinator queue
/// preserves that order, so the highest sequence is also the last to apply.
///
/// # What becomes visible earlier
///
/// A queued inode is visible to unlocked readers (`getattr`) slightly before
/// its data is published, so a concurrent reader can observe a grown size
/// whose bytes still read as a sparse hole for the length of one commit, and a
/// pre-apply commit failure can retract a size a reader already saw. Both are
/// already unspecified for a read that races an unsynchronized write, and the
/// alternative -- handing the value forward through the lock -- would silently
/// lose metadata updates on any lock path that forgot to consume it.
#[derive(Default)]
struct PendingInodes {
    entries: DashMap<InodeId, PendingInode>,
    next_seq: AtomicU64,
}

impl PendingInodes {
    /// Publish `updates` (later entries win per inode) until the guard drops.
    fn install(self: &Arc<Self>, updates: &[(InodeId, Option<Inode>)]) -> PendingInodeGuard {
        let mut latest: HashMap<InodeId, &Option<Inode>> = HashMap::new();
        for (id, value) in updates {
            latest.insert(*id, value);
        }
        let mut owned = Vec::with_capacity(latest.len());
        for (id, value) in latest {
            let seq = self.next_seq.fetch_add(1, Ordering::Relaxed);
            self.entries.insert(
                id,
                PendingInode {
                    seq,
                    value: value.clone(),
                },
            );
            owned.push((id, seq));
        }
        PendingInodeGuard {
            pending: Some(Arc::clone(self)),
            owned,
        }
    }

    /// The newest queued value, or `None` when nothing is in flight.
    fn get(&self, id: InodeId) -> Option<Option<Inode>> {
        self.entries.get(&id).map(|entry| entry.value.clone())
    }
}

/// Retires the queued inode values one commit installed. Dropping it hands
/// reads back to the read cache, which the apply has already promoted.
#[must_use = "hold the guard until the commit reply resolves"]
pub(crate) struct PendingInodeGuard {
    /// `None` for a transaction that mutates no inode, so the common
    /// extent-only commit allocates nothing.
    pending: Option<Arc<PendingInodes>>,
    owned: Vec<(InodeId, u64)>,
}

impl PendingInodeGuard {
    pub(crate) fn empty() -> Self {
        Self {
            pending: None,
            owned: Vec::new(),
        }
    }
}

impl Drop for PendingInodeGuard {
    fn drop(&mut self) {
        let Some(pending) = &self.pending else {
            return;
        };
        for (id, seq) in self.owned.drain(..) {
            // A later submitter that overwrote this slot owns it now; retiring
            // it here would resurrect a superseded value.
            pending.entries.remove_if(&id, |_, entry| entry.seq == seq);
        }
    }
}

#[derive(Clone)]
pub struct InodeStore {
    db: Arc<Db>,
    key_codec: Arc<KeyCodec>,
    next_id: Arc<AtomicU64>,
    cache: InodeCache,
    pending: Arc<PendingInodes>,
}

impl InodeStore {
    pub fn new(db: Arc<Db>, key_codec: Arc<KeyCodec>, initial_next_id: u64) -> Self {
        let cache = InodeCache::new(
            db.clone(),
            INODE_CACHE_BYTES,
            "zerofs-inode-cache",
            inode_cache_weight,
        );
        Self {
            db,
            key_codec,
            next_id: Arc::new(AtomicU64::new(initial_next_id)),
            cache,
            pending: Arc::new(PendingInodes::default()),
        }
    }

    pub fn allocate(&self) -> InodeId {
        self.next_id.fetch_add(1, Ordering::SeqCst)
    }

    pub fn next_id(&self) -> u64 {
        self.next_id.load(Ordering::SeqCst)
    }

    pub async fn get(&self, id: InodeId) -> Result<Inode, FsError> {
        // A queued mutation outranks both the read cache and the database: it
        // is the newest value, and during its apply the cache is invalidated
        // while the database still holds the previous one.
        if let Some(pending) = self.pending.get(id) {
            self.db.check_serving_authority()?;
            return pending.ok_or(FsError::NotFound);
        }
        self.cache.get_or_load(id, || self.load(id)).await
    }

    /// Publish a transaction's inode mutations as queued-but-unapplied values.
    /// Called by the write coordinator at submit; the returned guard must live
    /// until the commit reply resolves.
    pub(crate) fn install_pending(
        &self,
        updates: &[(InodeId, Option<Inode>)],
    ) -> PendingInodeGuard {
        if updates.is_empty() {
            return PendingInodeGuard::empty();
        }
        self.pending.install(updates)
    }

    #[cfg(test)]
    pub(crate) fn pending_inode(&self, id: InodeId) -> Option<Option<Inode>> {
        self.pending.get(id)
    }

    async fn load(&self, id: InodeId) -> Result<Inode, FsError> {
        let key = self.key_codec.inode_key(id);

        let data = self
            .db
            .get_bytes(&key)
            .await
            .map_err(|e| {
                let error = FsError::from_db_error(&e);
                if matches!(error, FsError::LeaderLeaseExpired | FsError::ShuttingDown) {
                    tracing::debug!("InodeStore::get({id}): serving authority lost");
                } else {
                    tracing::error!(
                        "InodeStore::get({}): database get_bytes failed: {:?}",
                        id,
                        e
                    );
                }
                error
            })?
            .ok_or_else(|| {
                // A missing inode is a normal ENOENT (a stat or deferred flush
                // racing a removal), not warning-worthy.
                tracing::debug!(
                    "InodeStore::get({}): inode key not found in database (key={:?}).",
                    id,
                    key
                );
                FsError::NotFound
            })?;

        bincode::deserialize(&data).map_err(|e| {
            tracing::warn!(
                "InodeStore::get({}): failed to deserialize inode data (len={}): {:?}.",
                id,
                data.len(),
                e
            );
            FsError::InvalidData
        })
    }

    pub(crate) fn invalidate_cache(
        &self,
        inode_ids: impl IntoIterator<Item = InodeId>,
    ) -> InvalidationGuard<InodeId, Inode> {
        self.cache.invalidate(inode_ids)
    }

    #[cfg(test)]
    pub(crate) fn cached_inode(&self, inode_id: InodeId) -> Option<Inode> {
        self.cache.peek(&inode_id)
    }

    #[cfg(test)]
    pub(crate) fn cache_enabled(&self) -> bool {
        self.cache.is_enabled()
    }

    #[cfg(test)]
    pub(crate) fn cache_load_count(&self) -> u64 {
        self.cache.load_count()
    }

    pub fn save(
        &self,
        txn: &mut Transaction,
        id: InodeId,
        inode: &Inode,
    ) -> Result<(), Box<bincode::ErrorKind>> {
        let key = self.key_codec.inode_key(id);
        let data = Bytes::from(bincode::serialize(inode)?);
        txn.put_bytes(&key, data);
        txn.update_cached_inode(id, Some(inode.clone()));
        Ok(())
    }

    pub fn delete(&self, txn: &mut Transaction, id: InodeId) {
        let key = self.key_codec.inode_key(id);
        txn.delete_bytes(&key);
        txn.update_cached_inode(id, None);
    }

    /// Resolve inode ID to full path components by walking parent chain.
    /// Returns Vec of path components (excluding root), in order from root to target.
    pub async fn resolve_path_components(&self, id: InodeId) -> Vec<Vec<u8>> {
        const ROOT_INODE_ID: InodeId = 0;

        if id == ROOT_INODE_ID {
            return Vec::new();
        }

        let mut components = Vec::new();
        let mut current_id = id;

        while current_id != ROOT_INODE_ID {
            if let Ok(inode) = self.get(current_id).await {
                let parent_id = match inode.parent() {
                    Some(p) => p,
                    None => {
                        // Hardlinked file - use placeholder
                        components.push(format!("<inode:{}>", current_id).into_bytes());
                        break;
                    }
                };

                if let Some(name) = inode.name() {
                    components.push(name.to_vec());
                    current_id = parent_id;
                } else {
                    // Name not available (shouldn't happen for non-hardlinked files)
                    components.push(format!("<inode:{}>", current_id).into_bytes());
                    break;
                }
            } else {
                break;
            }
        }

        components.reverse();
        components
    }

    /// Resolve inode ID to full path string.
    pub async fn resolve_path_lossy(&self, id: InodeId) -> String {
        let components = self.resolve_path_components(id).await;
        if components.is_empty() {
            return "/".to_string();
        }
        format!(
            "/{}",
            components
                .iter()
                .map(|b| String::from_utf8_lossy(b).to_string())
                .collect::<Vec<_>>()
                .join("/")
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fs::ZeroFS;
    use crate::fs::inode::test_file_inode;

    fn size_of(inode: Inode) -> u64 {
        match inode {
            Inode::File(file) => file.size,
            _ => panic!("expected a file inode"),
        }
    }

    async fn store() -> InodeStore {
        ZeroFS::new_in_memory().await.unwrap().inode_store.clone()
    }

    #[tokio::test]
    async fn a_queued_inode_outranks_the_read_cache() {
        let store = store().await;
        let id = store.allocate();
        let mut create = Transaction::new();
        store.save(&mut create, id, &test_file_inode(10)).unwrap();
        let committed = create.take_inode_cache_updates();
        // Stand in for the apply: promote the committed value into the cache.
        store
            .invalidate_cache([id])
            .publish(committed.into_iter().collect());
        assert_eq!(size_of(store.get(id).await.unwrap()), 10);

        let guard = store.install_pending(&[(id, Some(test_file_inode(20)))]);
        assert_eq!(
            size_of(store.get(id).await.unwrap()),
            20,
            "a queued value must be visible before its apply"
        );

        drop(guard);
        assert_eq!(
            size_of(store.get(id).await.unwrap()),
            10,
            "retiring the queue hands reads back to the cache"
        );
    }

    #[tokio::test]
    async fn a_later_submitter_owns_the_slot_until_it_retires() {
        let store = store().await;
        let id = store.allocate();

        let first = store.install_pending(&[(id, Some(test_file_inode(10)))]);
        let second = store.install_pending(&[(id, Some(test_file_inode(20)))]);
        assert_eq!(size_of(store.get(id).await.unwrap()), 20);

        // The predecessor resolving first must not resurrect its own value.
        drop(first);
        assert_eq!(
            size_of(store.get(id).await.unwrap()),
            20,
            "a resolved predecessor must not evict its successor"
        );

        drop(second);
        assert!(store.pending_inode(id).is_none());
    }

    #[tokio::test]
    async fn the_last_update_in_one_transaction_wins() {
        let store = store().await;
        let id = store.allocate();
        let guard = store.install_pending(&[
            (id, Some(test_file_inode(10))),
            (id, Some(test_file_inode(30))),
        ]);
        assert_eq!(size_of(store.get(id).await.unwrap()), 30);
        drop(guard);
    }

    #[tokio::test]
    async fn a_queued_deletion_reads_as_missing() {
        let store = store().await;
        let id = store.allocate();
        let mut create = Transaction::new();
        store.save(&mut create, id, &test_file_inode(10)).unwrap();
        let committed = create.take_inode_cache_updates();
        store
            .invalidate_cache([id])
            .publish(committed.into_iter().collect());

        let guard = store.install_pending(&[(id, None)]);
        assert!(matches!(store.get(id).await, Err(FsError::NotFound)));
        drop(guard);
        assert_eq!(size_of(store.get(id).await.unwrap()), 10);
    }
}
