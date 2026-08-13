//! Batched database writes through a single commit worker.
//!
//! The worker merges queued transactions, inode allocation state, usage
//! counters, segment counters, replication records, and dedup results into one
//! ordered commit.
//!
//! # Why the drain is opportunistic and never waits
//!
//! The worker takes one blocking `recv()` and then drains with `try_recv()`
//! only, so a batch closes the instant the queue is momentarily empty. A wave
//! of concurrent writers therefore often fragments into singleton applies. That
//! fragmentation is real but is a symptom, not the cost centre -- measured by
//! the `bench_lockstep_*` benches in this module against the in-memory backend:
//!
//! * Batching does amortize a genuine fixed cost. A one-commit apply costs
//!   ~18 us (~24 us once it must read a segment counter); a four-commit apply
//!   costs ~22 us (~50 us with counters), i.e. 3.2x (2.0x) cheaper per commit.
//! * But in the real `fs::write` path a four-member lockstep wave arrives
//!   ~155 us apart while an apply takes ~84 us, so the worker drains, applies,
//!   and goes idle before the next member arrives. It is busy only ~54% of the
//!   wave; it is not the saturated resource. Aggregate throughput is flat in
//!   writer count (1 member 1375 MB/s vs 4 members 1695 MB/s), so the writers
//!   are already queueing behind a serial resource *upstream* of this worker,
//!   which is exactly what spaces their arrivals out of phase with the applies.
//! * Closing a 155 us arrival gap requires waiting ~70 us after each apply --
//!   a timer, which would add that latency to every isolated writer. A
//!   cooperative `yield_now()` before the drain was measured and does nothing:
//!   a scheduler turn is two orders of magnitude shorter than the gap, and the
//!   wave still fragmented into 512 singleton applies out of 512 commits.
//! * In production the argument is decisive: a 1 MiB NBD wave at the observed
//!   17.5 MiB/s takes ~57 ms, of which four applies are ~0.3 ms. The commit
//!   worker is ~99% idle there, so perfect coalescing could return under 1%.
//!
//! So the drain stays opportunistic. The coalescing that *is* guaranteed --
//! everything queued during an in-flight apply lands in one following batch --
//! is pinned by `commits_queued_during_an_apply_drain_into_one_batch`.

use crate::db::{Db, Transaction};
use crate::fs::errors::FsError;
use crate::fs::flush_coordinator::FlushCoordinator;
use crate::fs::inode::Inode;
use crate::fs::key_codec::KeyCodec;
use crate::fs::stats::FileSystemGlobalStats;
use crate::fs::store::{DirectoryStore, ExtentStore, InodeStore};
use crate::replication::ShipOutcome;
use crate::replication::types::SlateDbSeqno;
use crate::task::spawn_named;
use futures::stream::{self, StreamExt, TryStreamExt};
use slatedb::WriteBatch;
use slatedb::config::WriteOptions;
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot};

/// Concurrency for the per-segment `segcount` base reads in [`stage_seg_deltas`].
const PARALLEL_SEGCOUNT_READS: usize = 16;

/// One segment's staged counter update read: (segcount key, `(live_delta,
/// total_delta)` netted for this batch, `(current_live, current_total)` base).
type SegBase = (bytes::Bytes, (i64, i64), (u64, u64));

type Reply = oneshot::Sender<Result<(), FsError>>;

// Boxing `Commit` would add an allocation to the hot path.
#[allow(clippy::large_enum_variant)]
enum Request {
    Commit(Transaction, Reply),
    Barrier(Reply),
}

#[derive(Clone)]
pub struct WriteCoordinator {
    sender: mpsc::UnboundedSender<Request>,
    /// Queues a transaction's inode mutations from submit until its reply, so
    /// a caller that releases its per-inode lock at submit cannot expose the
    /// pre-write value while the batch is still in flight.
    inode_store: InodeStore,
    #[cfg(test)]
    apply_probe: Arc<std::sync::Mutex<Option<oneshot::Sender<()>>>>,
    /// Size of every drained commit batch, in apply order.
    #[cfg(test)]
    batch_sizes: Arc<std::sync::Mutex<Vec<usize>>>,
    /// Worker time owned by applies, and the `stage_seg_deltas` share of it.
    #[cfg(test)]
    apply_nanos: Arc<std::sync::atomic::AtomicU64>,
    #[cfg(test)]
    stage_nanos: Arc<std::sync::atomic::AtomicU64>,
}

/// Commit worker dependencies.
struct WorkerContext {
    db: Arc<Db>,
    inode_store: InodeStore,
    directory_store: DirectoryStore,
    flush_coordinator: FlushCoordinator,
    key_codec: Arc<KeyCodec>,
    global_stats: Arc<FileSystemGlobalStats>,
    sync_writes: bool,
    /// Replication sequencer for commit-then-apply.
    replicator: Option<crate::replication::Replicator>,
    /// Applied mutation results.
    dedup: Arc<crate::dedup::DedupCache>,
    /// Lineage token stored on the first Solo commit.
    lineage_token: u64,
    /// Data plane used to attach un-PUT segment bytes to replication.
    extent_store: ExtentStore,
    #[cfg(test)]
    apply_probe: Arc<std::sync::Mutex<Option<oneshot::Sender<()>>>>,
    #[cfg(test)]
    batch_sizes: Arc<std::sync::Mutex<Vec<usize>>>,
    #[cfg(test)]
    apply_nanos: Arc<std::sync::atomic::AtomicU64>,
    #[cfg(test)]
    stage_nanos: Arc<std::sync::atomic::AtomicU64>,
}

impl WriteCoordinator {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        db: Arc<Db>,
        inode_store: InodeStore,
        directory_store: DirectoryStore,
        flush_coordinator: FlushCoordinator,
        key_codec: Arc<KeyCodec>,
        global_stats: Arc<FileSystemGlobalStats>,
        sync_writes: bool,
        replicator: Option<crate::replication::Replicator>,
        dedup: Arc<crate::dedup::DedupCache>,
        lineage_token: u64,
        extent_store: ExtentStore,
    ) -> Self {
        // Capture before spawning so concurrent allocations cannot advance the
        // worker's initial persisted watermark.
        let initial_counter = inode_store.next_id();
        let submit_inode_store = inode_store.clone();
        let (sender, receiver) = mpsc::unbounded_channel();
        #[cfg(test)]
        let apply_probe = Arc::new(std::sync::Mutex::new(None));
        #[cfg(test)]
        let batch_sizes = Arc::new(std::sync::Mutex::new(Vec::new()));
        #[cfg(test)]
        let apply_nanos = Arc::new(std::sync::atomic::AtomicU64::new(0));
        #[cfg(test)]
        let stage_nanos = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let ctx = WorkerContext {
            db,
            inode_store,
            directory_store,
            flush_coordinator,
            key_codec,
            global_stats,
            sync_writes,
            replicator,
            dedup,
            lineage_token,
            extent_store,
            #[cfg(test)]
            apply_probe: Arc::clone(&apply_probe),
            #[cfg(test)]
            batch_sizes: Arc::clone(&batch_sizes),
            #[cfg(test)]
            apply_nanos: Arc::clone(&apply_nanos),
            #[cfg(test)]
            stage_nanos: Arc::clone(&stage_nanos),
        };
        spawn_named("commit-worker", worker_loop(ctx, receiver, initial_counter));
        Self {
            sender,
            inode_store: submit_inode_store,
            #[cfg(test)]
            apply_probe,
            #[cfg(test)]
            batch_sizes,
            #[cfg(test)]
            apply_nanos,
            #[cfg(test)]
            stage_nanos,
        }
    }

    pub async fn commit(&self, txn: Transaction) -> Result<(), FsError> {
        self.submit(txn)?.wait().await
    }

    /// Queue `txn` without waiting for it to apply.
    ///
    /// Everything order-sensitive happens here, synchronously: the
    /// transaction's inode mutations are published to the overlay and the
    /// request takes its place in the queue. A caller holding a per-inode lock
    /// may therefore drop that lock as soon as this returns and await the
    /// reply outside it -- apply order for the inode still equals lock order,
    /// and no reader can observe the pre-write inode in between.
    ///
    /// Awaiting `commit` after releasing the lock would give neither
    /// guarantee: two writers could reach the send in either order.
    pub(crate) fn submit(&self, txn: Transaction) -> Result<PendingCommit, FsError> {
        let queued = self.inode_store.install_pending(txn.inode_cache_updates());
        let (reply_tx, reply_rx) = oneshot::channel();
        self.sender
            .send(Request::Commit(txn, reply_tx))
            .map_err(|_| FsError::IoError)?;
        Ok(PendingCommit {
            reply: reply_rx,
            queued,
        })
    }

    /// Wait until every commit submitted before this call has finished,
    /// including publication of its in-memory statistics.
    pub async fn barrier(&self) -> Result<(), FsError> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.sender
            .send(Request::Barrier(reply_tx))
            .map_err(|_| FsError::IoError)?;
        reply_rx.await.map_err(|_| FsError::IoError)?
    }

    /// Notify a test immediately before the next non-empty batch waits for its
    /// database write permit.
    #[cfg(test)]
    pub(crate) fn probe_next_apply(&self) -> oneshot::Receiver<()> {
        let (reached, receiver) = oneshot::channel();
        let previous = self
            .apply_probe
            .lock()
            .expect("write coordinator apply probe poisoned")
            .replace(reached);
        assert!(previous.is_none(), "an apply probe is already installed");
        receiver
    }

    /// Size of every commit batch the worker has drained so far, in apply
    /// order. A wave of concurrent writers that fragments into many singleton
    /// batches shows up here as a run of 1s.
    #[cfg(test)]
    pub(crate) fn batch_sizes(&self) -> Vec<usize> {
        self.batch_sizes
            .lock()
            .expect("write coordinator batch-size probe poisoned")
            .clone()
    }

    /// Total worker time owned by applies: from the end of a drain through the
    /// batch's reply sends. Comparing this with a benchmark's wall clock says
    /// whether the single commit worker is the bottleneck or is mostly idle.
    #[cfg(test)]
    pub(crate) fn apply_nanos(&self) -> u64 {
        self.apply_nanos.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// The [`stage_seg_deltas`] share of [`Self::apply_nanos`].
    #[cfg(test)]
    pub(crate) fn stage_nanos(&self) -> u64 {
        self.stage_nanos.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Weak commit handle for data-plane GC and compaction.
    pub fn downgrade(&self) -> WeakWriteCoordinator {
        WeakWriteCoordinator {
            sender: self.sender.downgrade(),
            inode_store: self.inode_store.clone(),
        }
    }
}

/// A queued commit awaiting its apply. Holding it keeps the transaction's
/// inode mutations published, so dropping it without awaiting hands reads back
/// to the read cache before the apply has promoted anything.
#[must_use = "a queued commit must be awaited"]
pub(crate) struct PendingCommit {
    reply: oneshot::Receiver<Result<(), FsError>>,
    /// Read only by its own drop: retiring it hands reads back to the cache.
    #[allow(dead_code)]
    queued: crate::fs::store::inode::PendingInodeGuard,
}

impl PendingCommit {
    pub(crate) async fn wait(self) -> Result<(), FsError> {
        let result = self.reply.await.map_err(|_| FsError::IoError)?;
        // `self.queued` drops here: on success the apply has already promoted
        // these values into the read cache, and on a pre-apply failure the
        // cache still holds the last committed value.
        result
    }
}

/// Weak commit handle held by `ExtentStore`; a strong sender would form a cycle.
#[derive(Clone)]
pub struct WeakWriteCoordinator {
    sender: mpsc::WeakUnboundedSender<Request>,
    inode_store: InodeStore,
}

impl WeakWriteCoordinator {
    pub async fn commit(&self, txn: Transaction) -> Result<(), FsError> {
        let sender = self.sender.upgrade().ok_or(FsError::IoError)?;
        // Same submit-time queueing as the strong handle; see there.
        let _queued = self.inode_store.install_pending(txn.inode_cache_updates());
        let (reply_tx, reply_rx) = oneshot::channel();
        sender
            .send(Request::Commit(txn, reply_tx))
            .map_err(|_| FsError::IoError)?;
        reply_rx.await.map_err(|_| FsError::IoError)?
    }
}

/// Whole-store footprint change for one committed batch.
#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct SegFootprintDelta {
    /// Counters changing from zero to nonzero.
    pub d_segments: i64,
    pub d_appended: i64,
    pub d_live: i64,
}

/// Materialize segment deltas as absolute counters and return their wire values.
/// The commit worker is the sole writer of these keys. `total` is monotonic.
pub(crate) async fn stage_seg_deltas(
    db: &Db,
    deltas: impl IntoIterator<Item = (bytes::Bytes, (i64, i64))>,
    batch: &mut WriteBatch,
) -> Result<(Vec<(bytes::Bytes, bytes::Bytes)>, SegFootprintDelta), FsError> {
    // Preserve deterministic read order.
    let mut agg: BTreeMap<bytes::Bytes, (i64, i64)> = BTreeMap::new();
    for (k, (dl, dt)) in deltas {
        let e = agg.entry(k).or_insert((0, 0));
        e.0 = e.0.saturating_add(dl);
        e.1 = e.1.saturating_add(dt);
    }
    // Missing counters start at zero. Read and decode failures abort the batch;
    // undercounting live bytes can make GC delete referenced data.
    let bases: Vec<SegBase> = stream::iter(agg)
        .map(|(key, net)| async move {
            match db.get_bytes_internal(&key).await {
                Ok(None) => Ok((key, net, (0, 0))),
                Ok(Some(b)) => KeyCodec::decode_segcount(&b)
                    .map(|base| (key, net, base))
                    .ok_or(FsError::IoError),
                Err(error) => Err(FsError::from_db_error(&error)),
            }
        })
        .buffer_unordered(PARALLEL_SEGCOUNT_READS)
        .try_collect()
        .await?;

    let mut out = Vec::with_capacity(bases.len());
    let mut fd = SegFootprintDelta::default();
    for (key, (net_live, net_total), (cur_live, cur_total)) in bases {
        let live = (cur_live as i128 + net_live as i128).max(0) as u64;
        // `total` is monotonic: clamp to at least its current value.
        let total = (cur_total as i128 + net_total as i128).max(cur_total as i128) as u64;
        // Monitoring deltas use the clamped absolute values and saturating sums.
        fd.d_live = fd.d_live.saturating_add(live as i64 - cur_live as i64);
        fd.d_appended = fd
            .d_appended
            .saturating_add(total as i64 - cur_total as i64);
        fd.d_segments = fd
            .d_segments
            .saturating_add((cur_total == 0 && total > 0) as i64);
        let val = KeyCodec::encode_segcount(live, total);
        batch.put_bytes(key.clone(), val.clone());
        out.push((key, val));
    }
    Ok((out, fd))
}

/// Accumulates one apply's worker time on drop, so the `continue` paths that
/// abandon a batch are measured alongside the ones that complete it.
#[cfg(test)]
struct ApplyTimer {
    start: std::time::Instant,
    sink: Arc<std::sync::atomic::AtomicU64>,
}

#[cfg(test)]
impl Drop for ApplyTimer {
    fn drop(&mut self) {
        self.sink.fetch_add(
            self.start.elapsed().as_nanos() as u64,
            std::sync::atomic::Ordering::Relaxed,
        );
    }
}

/// Byte-counter deltas this batch causes, as `(inode, delta)`.
///
/// The used-bytes counter is the sum of committed file sizes, so one inode's
/// contribution changes by exactly `post_size - pre_size` where `pre_size` is
/// what is committed right now -- read before this batch invalidates anything,
/// and deliberately bypassing the queued-inode overlay. An inode whose
/// post-image is not a file can never hold bytes (an inode's kind is fixed at
/// creation), so those skip the lookup entirely; directory mtime bumps are the
/// common case and cost nothing.
async fn derive_byte_deltas(
    ctx: &WorkerContext,
    updates: &HashMap<u64, Option<Inode>>,
) -> Result<Vec<(u64, i64)>, FsError> {
    let mut deltas = Vec::new();
    for (inode_id, post) in updates {
        let post_bytes = match post {
            Some(Inode::File(file)) => file.size,
            // A deletion still has to give back whatever the inode held.
            None => 0,
            Some(_) => continue,
        };
        let pre_bytes = ctx.inode_store.committed_byte_usage(*inode_id).await?;
        let delta = crate::fs::stats::size_delta(pre_bytes, post_bytes);
        if delta != 0 {
            deltas.push((*inode_id, delta));
        }
    }
    Ok(deltas)
}

async fn worker_loop(
    mut ctx: WorkerContext,
    mut rx: mpsc::UnboundedReceiver<Request>,
    initial_counter: u64,
) {
    let mut last_emitted_counter = initial_counter;
    // One durable Solo taint per leader process.
    let mut taint_written = false;
    loop {
        // Base repair shares mutation sequencing. Bias it over queued commits so
        // the flush covers the complete prior receiver prefix.
        let first = match ctx.replicator.as_mut() {
            Some(replicator) => {
                tokio::select! {
                    biased;
                    repair = replicator.next_base_repair() => {
                        let Some(request) = repair else {
                            break;
                        };
                        let required =
                            crate::replication::Replicator::begin_base_repair(replicator);
                        if let Some(required) = required {
                            let through = required.through().get();
                            let receipt = match ctx.flush_coordinator.flush_with_receipt().await {
                                Ok(receipt) => receipt,
                                Err(error) => crate::db::exit_on_write_error(format!(
                                    "HA receiver-base repair through local sequence {through} failed: \
                                     {error}"
                                )),
                            };
                            required.complete(receipt);
                        }
                        let _ = request.send(());
                        continue;
                    }
                    commit = rx.recv() => {
                        let Some(commit) = commit else {
                            break;
                        };
                        commit
                    }
                }
            }
            None => {
                let Some(commit) = rx.recv().await else {
                    break;
                };
                commit
            }
        };
        let (txn, reply) = match first {
            Request::Commit(txn, reply) => (txn, reply),
            Request::Barrier(reply) => {
                let _ = reply.send(Ok(()));
                continue;
            }
        };
        let mut batch = vec![(txn, reply)];
        let mut barrier_reply = None;
        while let Ok(msg) = rx.try_recv() {
            match msg {
                Request::Commit(txn, reply) => batch.push((txn, reply)),
                Request::Barrier(reply) => {
                    barrier_reply = Some(reply);
                    break;
                }
            }
        }
        #[cfg(test)]
        ctx.batch_sizes
            .lock()
            .expect("write coordinator batch-size probe poisoned")
            .push(batch.len());
        #[cfg(test)]
        let _apply_timer = ApplyTimer {
            start: std::time::Instant::now(),
            sink: Arc::clone(&ctx.apply_nanos),
        };

        let replicating = ctx.replicator.is_some();
        let mut merged = WriteBatch::new();
        let mut repl_ops: Vec<crate::replication::ReplOp> = Vec::new();
        let mut replies = Vec::with_capacity(batch.len());
        // Pin staged FrameLocs until the merged write resolves.
        let mut extent_ref_guards = Vec::new();
        let mut any_ops = false;
        let mut inode_cache_updates: HashMap<u64, Option<Inode>> = HashMap::new();
        let mut directory_entry_cache_updates: HashMap<(u64, bytes::Bytes), Option<(u64, u64)>> =
            HashMap::new();
        let mut extent_location_cache_updates: HashMap<
            (u64, u64),
            Option<crate::segment::FrameLoc>,
        > = HashMap::new();
        let mut shard_deltas: HashMap<usize, (i64, i64)> = HashMap::new();
        let mut seg_map: HashMap<bytes::Bytes, (i64, i64)> = HashMap::new();
        let mut batch_dedup_entries: Vec<crate::dedup::DedupEntry> = Vec::new();
        for (mut txn, reply) in batch {
            if let Some(guard) = txn.take_extent_ref_guard() {
                extent_ref_guards.push(guard);
            }
            any_ops |= !txn.is_empty();
            if let Some(entry) = txn.take_dedup_entry() {
                batch_dedup_entries.push(entry);
            }
            for (inode_id, inode) in txn.take_inode_cache_updates() {
                inode_cache_updates.insert(inode_id, inode);
            }
            for (key, entry) in txn.take_directory_entry_cache_updates() {
                directory_entry_cache_updates.insert(key, entry);
            }
            for (key, location) in txn.take_extent_location_cache_updates() {
                extent_location_cache_updates.insert(key, location);
            }
            for delta in txn.take_stats_deltas() {
                let entry = shard_deltas
                    .entry(ctx.global_stats.shard_of(delta.inode_id))
                    .or_default();
                // Saturating: individual deltas are already clamped to the
                // i64 range, so a batch of multi-EiB deltas could overflow a
                // plain `+=` and panic the singleton worker.
                entry.0 = entry.0.saturating_add(delta.bytes);
                entry.1 = entry.1.saturating_add(delta.inodes);
            }
            // `apply_to` consumes operations but not segment deltas.
            for (key, (dl, dt)) in txn.take_seg_deltas() {
                let e = seg_map.entry(key).or_default();
                e.0 = e.0.saturating_add(dl);
                e.1 = e.1.saturating_add(dt);
            }
            if replicating {
                repl_ops.extend(txn.apply_to_collecting(&mut merged));
            } else {
                txn.apply_to(&mut merged);
            }
            replies.push(reply);
        }

        // The byte dimension is derived here, against what this batch is about
        // to supersede, rather than staged by the callers. Doing it anywhere
        // else is unsound: a caller computes its base from the queued inode
        // (that is what lets the write path release its lock at submit), and a
        // transaction queued ahead of it can still fail before its apply and
        // drop its own delta -- leaving the successor's delta based on a value
        // that never landed and the counter permanently short. Only the worker
        // knows which transactions actually applied, so only the worker can
        // produce a delta that telescopes onto the durable sum of file sizes.
        match derive_byte_deltas(&ctx, &inode_cache_updates).await {
            Ok(byte_deltas) => {
                for (inode_id, bytes) in byte_deltas {
                    let entry = shard_deltas
                        .entry(ctx.global_stats.shard_of(inode_id))
                        .or_default();
                    entry.0 = entry.0.saturating_add(bytes);
                }
            }
            Err(e) => {
                // Undercounting used bytes would relax the quota permanently,
                // so an unreadable pre-image aborts the batch before it
                // applies, exactly as an unreadable segment counter does.
                // Nothing is staged yet at this point -- in particular the
                // allocation watermark is untouched, so unlike the
                // `stage_seg_deltas` failure below there is no ID to burn.
                for reply in replies {
                    let _ = reply.send(Err(e));
                }
                if let Some(reply) = barrier_reply {
                    let _ = reply.send(Err(e));
                }
                continue;
            }
        }

        // Persist the allocation watermark only after it advances.
        let current = ctx.inode_store.next_id();
        let counter_staged = current > last_emitted_counter;
        if counter_staged {
            let counter_key = ctx.key_codec.system_counter_key();
            let counter_value = KeyCodec::encode_counter(current);
            if replicating {
                repl_ops.push(crate::replication::ReplOp::Put(
                    counter_key.clone(),
                    counter_value.clone(),
                ));
            }
            merged.put_bytes(counter_key, counter_value);
            last_emitted_counter = current;
            any_ops = true;
        }

        let staged: Vec<_> = shard_deltas
            .into_iter()
            .map(|(shard_id, (bytes, inodes))| {
                ctx.global_stats.stage_delta(shard_id, bytes, inodes)
            })
            .collect();
        for shard in &staged {
            if replicating {
                repl_ops.push(crate::replication::ReplOp::Put(
                    shard.key.clone(),
                    shard.value.clone(),
                ));
            }
            merged.put_bytes(shard.key.clone(), shard.value.clone());
            any_ops = true;
        }

        // The commit worker is the sole segment-counter writer.
        #[cfg(test)]
        let stage_start = std::time::Instant::now();
        let staged_seg = stage_seg_deltas(&ctx.db, seg_map, &mut merged).await;
        #[cfg(test)]
        ctx.stage_nanos.fetch_add(
            stage_start.elapsed().as_nanos() as u64,
            std::sync::atomic::Ordering::Relaxed,
        );
        let (seg_abs, footprint_delta) = match staged_seg {
            Ok(v) => v,
            Err(e) => {
                // A failed staged counter may have advanced the in-memory inode
                // watermark. Burn one ID so the next commit persists past it.
                if counter_staged {
                    ctx.inode_store.allocate();
                }
                for reply in replies {
                    let _ = reply.send(Err(e));
                }
                if let Some(reply) = barrier_reply {
                    let _ = reply.send(Err(e));
                }
                continue;
            }
        };
        if !seg_abs.is_empty() {
            any_ops = true;
            if replicating {
                for (key, val) in seg_abs {
                    repl_ops.push(crate::replication::ReplOp::Put(key, val));
                }
            }
        }

        // Replication carries bytes for referenced segments not yet PUT.
        if replicating {
            repl_ops = ctx.extent_store.enrich_repl_ops(repl_ops);
        }

        // Dedup-only outcomes follow the same ordered replication path.
        let has_logical_work = any_ops || !batch_dedup_entries.is_empty();

        // After Solo operation, the local base must be durable before the first
        // dependent replicated suffix is shipped.
        let mut deposed = false;
        let mut apply_permit = match (has_logical_work, ctx.replicator.as_mut()) {
            (true, Some(repl)) => loop {
                match repl.ship(&repl_ops, &batch_dedup_entries).await {
                    ShipOutcome::Apply(permit) => break Some(permit),
                    ShipOutcome::NeedsBaseFlush(required) => {
                        let through = required.through().get();
                        let receipt = match ctx.flush_coordinator.flush_with_receipt().await {
                            Ok(receipt) => receipt,
                            Err(error) => {
                                // The current batch is unapplied. A base-flush failure
                                // retires the writer before the suffix can ship.
                                crate::db::exit_on_write_error(format!(
                                    "HA replication base through local sequence {through} failed \
                                     to flush before ship retry: {error}"
                                ));
                            }
                        };
                        required.complete(receipt);
                    }
                    ShipOutcome::Deposed => {
                        deposed = true;
                        break None;
                    }
                    ShipOutcome::Poisoned => {
                        crate::db::exit_on_write_error(
                            "HA replication sequencer is poisoned by an unresolved peer copy",
                        );
                    }
                }
            },
            _ => None,
        };
        // Peer rejection proves this batch was not appended or applied locally.
        // Revoke admission before returning CLEAN failures.
        if deposed {
            tracing::error!(
                "HA: standby rejected a ship: this leader is deposed; failing the \
                 batch and stepping down"
            );
            // A stale writer must not flush.
            ctx.db.revoke_lease();
            if counter_staged {
                ctx.inode_store.allocate();
            }
            for reply in replies {
                let _ = reply.send(Err(FsError::LeaderRejectedBeforeApply));
            }
            if let Some(reply) = barrier_reply {
                let _ = reply.send(Err(FsError::LeaderRejectedBeforeApply));
            }
            continue;
        }
        let ran_solo = apply_permit
            .as_ref()
            .is_some_and(|permit| permit.requires_solo_taint());

        // The provenance stamp remains local and independently durable.
        if let Some(permit) = apply_permit.as_ref() {
            merged.put_bytes(
                ctx.key_codec.ha_seqno_key(),
                KeyCodec::encode_ha_stamp(permit.stamp()),
            );
            any_ops = true;
        }

        // SlateDB rejects empty write batches; logical-only work is handled
        // without a database write.
        let mut result: Result<(), FsError> = Ok(());

        // Persist the lineage taint before acknowledging the first Solo write.
        if !taint_written && ran_solo {
            match ctx
                .db
                .put_with_options(
                    &ctx.key_codec.taint_key(),
                    &KeyCodec::encode_u64(ctx.lineage_token),
                    &slatedb::config::PutOptions::default(),
                    &WriteOptions {
                        await_durable: false,
                        ..Default::default()
                    },
                )
                .await
            {
                Ok(()) => match ctx.flush_coordinator.flush().await {
                    Ok(()) => taint_written = true,
                    Err(e) => result = Err(e),
                },
                Err(_) => result = Err(FsError::IoError),
            }
        }

        let mut local_applied = false;
        if any_ops && result.is_ok() {
            #[cfg(test)]
            if let Some(reached) = ctx
                .apply_probe
                .lock()
                .expect("write coordinator apply probe poisoned")
                .take()
            {
                let _ = reached.send(());
            }
            // Complete lease and flush-barrier admission before evicting. The
            // affected keys then stay uncacheable only across SlateDB's atomic
            // apply, not while a suspended lease or seal+flush is awaited.
            let write_result = match ctx.db.acquire_write_permit().await {
                Ok(permit) => {
                    let inode_cache_guard = ctx
                        .inode_store
                        .invalidate_cache(inode_cache_updates.keys().copied());
                    let directory_cache_guard = ctx
                        .directory_store
                        .invalidate_cache(directory_entry_cache_updates.keys().cloned());
                    let extent_location_cache_guard =
                        ctx.extent_store.invalidate_extent_location_cache(
                            extent_location_cache_updates.keys().copied(),
                        );

                    let write_result = permit
                        .write_with_options(
                            merged,
                            &WriteOptions {
                                await_durable: false,
                                ..Default::default()
                            },
                        )
                        .await;

                    match write_result {
                        Ok(seqno) => {
                            inode_cache_guard.publish(inode_cache_updates);
                            directory_cache_guard.publish(directory_entry_cache_updates);
                            ExtentStore::publish_extent_location_cache(
                                extent_location_cache_guard,
                                extent_location_cache_updates,
                            );
                            Ok(seqno)
                        }
                        Err(error) => {
                            drop(extent_location_cache_guard);
                            drop(directory_cache_guard);
                            drop(inode_cache_guard);
                            Err(error)
                        }
                    }
                }
                Err(error) => Err(error),
            };

            match write_result {
                Ok(slatedb_seq) => {
                    local_applied = true;
                    if let Some(permit) = apply_permit.take() {
                        permit.applied(SlateDbSeqno::new(slatedb_seq));
                    }
                }
                Err(_) => result = Err(FsError::IoError),
            }
        } else if result.is_ok() && !batch_dedup_entries.is_empty() {
            // Standalone logical outcomes complete when published to the ledger.
            local_applied = true;
        }
        if local_applied {
            for entry in batch_dedup_entries {
                ctx.dedup.record_entry(entry);
            }
        }

        // A failed local apply with a possible peer copy poisons sequencing.
        if !local_applied
            && let Some(permit) = apply_permit.take()
            && let Err(peer_copy) = permit.failed()
        {
            let error = result.as_ref().err().copied().unwrap_or(FsError::IoError);
            crate::db::exit_on_write_error(format!(
                "HA batch {} may be buffered on the standby but failed local apply: {error}",
                peer_copy.seqno().get()
            ));
        }

        // Publish in-memory counters after local commit.
        if result.is_ok() {
            for shard in &staged {
                ctx.global_stats.publish(shard);
            }
            ctx.extent_store.segment_gc_stats().apply_footprint_delta(
                footprint_delta.d_segments,
                footprint_delta.d_appended,
                footprint_delta.d_live,
            );
        }

        drop(extent_ref_guards);

        // `sync_writes` returns success only after the batch is durable.
        if ctx.sync_writes && result.is_ok() && any_ops {
            result = ctx.flush_coordinator.flush().await;
        }

        // Burn one ID when a failed batch may have dropped its staged watermark.
        if counter_staged && result.is_err() {
            ctx.inode_store.allocate();
        }
        for reply in replies {
            let _ = reply.send(result);
        }
        if let Some(reply) = barrier_reply {
            let _ = reply.send(result);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fs::ZeroFS;
    use crate::fs::inode::{Inode, test_file_inode};
    use crate::fs::test_util::{test_auth, test_creds};
    use crate::fs::types::{SetAttributes, SetMode};
    use bytes::Bytes;

    /// `DST_PANIC_ON_WRITE_ERROR` is process-global, so fatal-path unit tests
    /// must not toggle it concurrently.
    static FATAL_WRITE_TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

    struct PanicOnFatalWrite(bool);

    impl Drop for PanicOnFatalWrite {
        fn drop(&mut self) {
            crate::db::DST_PANIC_ON_WRITE_ERROR.store(self.0, std::sync::atomic::Ordering::SeqCst);
        }
    }

    async fn make_fs() -> ZeroFS {
        ZeroFS::new_in_memory().await.unwrap()
    }

    fn file_size(inode: Option<Inode>) -> Option<u64> {
        match inode {
            Some(Inode::File(file)) => Some(file.size),
            _ => None,
        }
    }

    fn codec() -> KeyCodec {
        KeyCodec::new()
    }

    /// `count` member files pre-sized to `size`, the shape NBD provisioning
    /// leaves behind. Pre-sizing is the point: every later write then lands in
    /// a hole below EOF rather than extending the file.
    async fn presized_members(fs: &ZeroFS, count: u8, size: u64) -> Vec<crate::fs::inode::InodeId> {
        let mut members = Vec::with_capacity(count as usize);
        for i in 0..count {
            let (id, _) = fs
                .create(
                    &test_creds(),
                    0,
                    &[b'm', b'0' + i],
                    &SetAttributes::default(),
                )
                .await
                .unwrap();
            fs.setattr(
                &test_creds(),
                id,
                &SetAttributes {
                    size: crate::fs::types::SetSize::Set(size),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
            members.push(id);
        }
        members
    }

    // One aligned 1 MiB NBD stripe write: four pre-sized member files receive
    // one 256 KiB chunk each, concurrently, through the shared coordinator.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn presized_member_wave_commits_without_metadata_scans() {
        let fs = make_fs().await;
        let member_size = 32 * 1024 * 1024u64;
        let chunk = 256 * 1024usize;
        let members = presized_members(&fs, 4, member_size).await;

        let auth = test_auth();
        let scans_before = fs.db.scan_call_count();
        let batches_before = fs.write_coordinator.batch_sizes().len();
        let payload = Bytes::from(vec![7u8; chunk]);
        let writes = members.iter().map(|id| fs.write(&auth, *id, 0, &payload));
        for result in futures::future::join_all(writes).await {
            result.unwrap();
        }
        let batches = fs.write_coordinator.batch_sizes()[batches_before..].to_vec();
        // Today the wave typically fragments into singleton applies (e.g.
        // [1, 1, 1, 1]); batching is opportunistic, so only the commit total
        // is deterministic.
        eprintln!("wave commit batches: {batches:?}");
        assert_eq!(
            batches.iter().sum::<usize>(),
            members.len(),
            "each member write commits exactly once through the coordinator"
        );
        assert_eq!(
            fs.db.scan_call_count(),
            scans_before,
            "a member-chunk wave below pre-sized EOFs must not range-scan \
             extent metadata"
        );
    }

    #[tokio::test]
    async fn inode_commits_promote_the_last_committed_value() {
        let fs = make_fs().await;
        assert!(fs.inode_store.cache_enabled());
        let inode_id = fs.inode_store.allocate();

        let mut create = Transaction::new();
        fs.inode_store
            .save(&mut create, inode_id, &test_file_inode(10))
            .unwrap();
        fs.write_coordinator.commit(create).await.unwrap();
        assert_eq!(file_size(fs.inode_store.cached_inode(inode_id)), Some(10));
        assert_eq!(
            file_size(Some(fs.inode_store.get(inode_id).await.unwrap())),
            Some(10)
        );
        assert_eq!(file_size(fs.inode_store.cached_inode(inode_id)), Some(10));

        let mut update = Transaction::new();
        fs.inode_store
            .save(&mut update, inode_id, &test_file_inode(20))
            .unwrap();
        fs.inode_store
            .save(&mut update, inode_id, &test_file_inode(30))
            .unwrap();
        fs.write_coordinator.commit(update).await.unwrap();
        assert_eq!(file_size(fs.inode_store.cached_inode(inode_id)), Some(30));
        assert_eq!(
            file_size(Some(fs.inode_store.get(inode_id).await.unwrap())),
            Some(30)
        );
        assert_eq!(file_size(fs.inode_store.cached_inode(inode_id)), Some(30));

        let mut delete = Transaction::new();
        fs.inode_store.delete(&mut delete, inode_id);
        fs.write_coordinator.commit(delete).await.unwrap();
        assert!(fs.inode_store.cached_inode(inode_id).is_none());
        assert!(matches!(
            fs.inode_store.get(inode_id).await,
            Err(FsError::NotFound)
        ));
    }

    #[tokio::test]
    async fn create_write_setattr_does_not_reload_promoted_metadata() {
        let fs = make_fs().await;
        let creds = test_creds();
        let auth = test_auth();
        let (inode_id, _) = fs
            .create(&creds, 0, b"hot", &SetAttributes::default())
            .await
            .unwrap();
        let inode_loads = fs.inode_store.cache_load_count();
        let entry_loads = fs.directory_store.cache_load_count();

        fs.write(&auth, inode_id, 0, &Bytes::from_static(b"payload"))
            .await
            .unwrap();
        fs.setattr(
            &creds,
            inode_id,
            &SetAttributes {
                mode: SetMode::Set(0o600),
                ..Default::default()
            },
        )
        .await
        .unwrap();

        assert_eq!(fs.inode_store.cache_load_count(), inode_loads);
        assert_eq!(fs.directory_store.cache_load_count(), entry_loads);
    }

    #[tokio::test]
    async fn extent_location_publication_follows_the_coordinator_commit() {
        let fs = make_fs().await;
        let initial = Bytes::from(vec![0x21; 2 * crate::fs::EXTENT_SIZE]);
        let mut create = Transaction::new();
        let tail = fs
            .extent_store
            .write(&mut create, 41, 0, &initial, 0)
            .await
            .unwrap();
        fs.write_coordinator.commit(create).await.unwrap();
        fs.extent_store.apply_tail_update(41, tail);

        let before = fs.db.scan_call_count();
        assert_eq!(
            fs.extent_store
                .read(41, 0, initial.len() as u64)
                .await
                .unwrap(),
            initial
        );
        assert_eq!(fs.db.scan_call_count(), before);

        let key = codec().extent_key(41, 0);
        let old_loc =
            crate::segment::FrameLoc::decode(&fs.db.get_bytes(&key).await.unwrap().unwrap())
                .unwrap();
        let segcount = codec().segcount_key(old_loc.segid.epoch, old_loc.segid.counter);
        fs.db
            .put_with_options(
                &segcount,
                b"bogus",
                &slatedb::config::PutOptions::default(),
                &WriteOptions::default(),
            )
            .await
            .unwrap();

        let mut failed = Transaction::new();
        fs.extent_store
            .write(
                &mut failed,
                41,
                0,
                &Bytes::from(vec![0x99; crate::fs::EXTENT_SIZE]),
                initial.len() as u64,
            )
            .await
            .unwrap();
        fs.write_coordinator.commit(failed).await.unwrap_err();

        let before = fs.db.scan_call_count();
        assert_eq!(
            fs.extent_store
                .read(41, 0, initial.len() as u64)
                .await
                .unwrap(),
            initial,
            "a failed batch must leave the previously committed location visible"
        );
        assert_eq!(fs.db.scan_call_count(), before);
    }

    #[tokio::test]
    async fn sequential_remove_does_not_reload_the_parent_or_entries() {
        let fs = make_fs().await;
        let creds = test_creds();
        let auth = test_auth();
        fs.create(&creds, 0, b"first", &SetAttributes::default())
            .await
            .unwrap();
        fs.create(&creds, 0, b"second", &SetAttributes::default())
            .await
            .unwrap();
        let inode_loads = fs.inode_store.cache_load_count();
        let entry_loads = fs.directory_store.cache_load_count();

        fs.remove(&auth, 0, b"first").await.unwrap();
        fs.remove(&auth, 0, b"second").await.unwrap();

        assert_eq!(fs.inode_store.cache_load_count(), inode_loads);
        assert_eq!(fs.directory_store.cache_load_count(), entry_loads);
    }

    #[tokio::test]
    async fn pre_apply_failure_leaves_the_existing_inode_cache_untouched() {
        let fs = make_fs().await;
        let inode_id = fs.inode_store.allocate();

        let mut create = Transaction::new();
        fs.inode_store
            .save(&mut create, inode_id, &test_file_inode(10))
            .unwrap();
        fs.write_coordinator.commit(create).await.unwrap();
        assert_eq!(
            file_size(Some(fs.inode_store.get(inode_id).await.unwrap())),
            Some(10)
        );

        let seg_key = codec().segcount_key(9, 9);
        fs.db
            .put_with_options(
                &seg_key,
                b"bogus",
                &slatedb::config::PutOptions::default(),
                &WriteOptions::default(),
            )
            .await
            .unwrap();
        let mut update = Transaction::new();
        fs.inode_store
            .save(&mut update, inode_id, &test_file_inode(99))
            .unwrap();
        update.add_seg_delta(&seg_key, 1, 1);
        fs.write_coordinator.commit(update).await.unwrap_err();

        assert_eq!(file_size(fs.inode_store.cached_inode(inode_id)), Some(10));
        assert_eq!(
            file_size(Some(fs.inode_store.get(inode_id).await.unwrap())),
            Some(10)
        );
        assert!(
            fs.inode_store.pending_inode(inode_id).is_none(),
            "a failed batch must retract the value it queued at submit"
        );
    }

    /// The window a caller releasing its inode lock at submit depends on: from
    /// the moment `commit` is entered until its reply resolves, the queued
    /// inode -- not the read cache and not the database -- answers reads.
    #[tokio::test]
    async fn a_queued_inode_answers_reads_while_its_commit_is_in_flight() {
        let fs = make_fs().await;
        let inode_id = fs.inode_store.allocate();
        let mut create = Transaction::new();
        fs.inode_store
            .save(&mut create, inode_id, &test_file_inode(10))
            .unwrap();
        fs.write_coordinator.commit(create).await.unwrap();
        assert_eq!(file_size(fs.inode_store.cached_inode(inode_id)), Some(10));

        // Stall the update at its write permit so it is provably submitted and
        // provably unapplied while the assertions below run.
        let commit_block = fs.db.flush_barrier().write_owned().await;
        let apply_reached = fs.write_coordinator.probe_next_apply();
        let mut update = Transaction::new();
        fs.inode_store
            .save(&mut update, inode_id, &test_file_inode(20))
            .unwrap();
        let coordinator = fs.write_coordinator.clone();
        let commit = tokio::spawn(async move { coordinator.commit(update).await });
        apply_reached.await.unwrap();

        assert_eq!(
            file_size(fs.inode_store.cached_inode(inode_id)),
            Some(10),
            "the apply has not promoted anything yet"
        );
        assert_eq!(
            file_size(Some(fs.inode_store.get(inode_id).await.unwrap())),
            Some(20),
            "the queued value must outrank the last committed one"
        );

        drop(commit_block);
        commit.await.unwrap().unwrap();
        assert!(fs.inode_store.pending_inode(inode_id).is_none());
        assert_eq!(
            file_size(Some(fs.inode_store.get(inode_id).await.unwrap())),
            Some(20),
            "the apply promoted the same value the queue was serving"
        );
    }

    /// `[(batch size, times drained)]`, ascending by size.
    fn batch_histogram(sizes: &[usize]) -> Vec<(usize, usize)> {
        let mut counts: BTreeMap<usize, usize> = BTreeMap::new();
        for size in sizes {
            *counts.entry(*size).or_default() += 1;
        }
        counts.into_iter().collect()
    }

    // Lockstep member waves: four writers each issue one 256 KiB member chunk
    // and every writer waits for all four replies before the next wave. This is
    // the NBD stripe shape when the client serializes on ACK, and the arrival
    // pattern where the worker's opportunistic drain has the least opportunity
    // to coalesce -- the queue is empty every time the worker blocks in recv().
    //
    // The free-running arm is the same total work without the per-wave join:
    // arrivals then overlap the in-flight apply, so fragmentation is expected
    // to self-correct into multi-commit batches.
    //   cargo test --release --lib -- --ignored --nocapture bench_lockstep_member_waves
    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    #[ignore = "throughput measurement, run explicitly in release"]
    async fn bench_lockstep_member_waves() {
        const WAVES: usize = 128;
        const CHUNK: usize = 256 * 1024;

        // The one-member lockstep arm is the control: the same per-chunk work
        // with nothing for the coordinator to serialize against. Comparing its
        // per-wave time with the four-member arm's prices the coordinator's
        // serial apply chain.
        for (members_count, lockstep) in [(1usize, true), (4, true), (4, false)] {
            let fs = make_fs().await;
            let member_size = (WAVES * CHUNK) as u64;
            let members = presized_members(&fs, members_count as u8, member_size).await;

            let auth = test_auth();
            let payload = Bytes::from(vec![7u8; CHUNK]);
            let batches_before = fs.write_coordinator.batch_sizes().len();
            let apply_before = fs.write_coordinator.apply_nanos();
            let stage_before = fs.write_coordinator.stage_nanos();
            let start = std::time::Instant::now();
            if lockstep {
                for wave in 0..WAVES {
                    let offset = (wave * CHUNK) as u64;
                    let writes = members
                        .iter()
                        .map(|id| fs.write(&auth, *id, offset, &payload));
                    for result in futures::future::join_all(writes).await {
                        result.unwrap();
                    }
                }
            } else {
                let writers = members.iter().map(|id| {
                    let fs = &fs;
                    let auth = &auth;
                    let payload = &payload;
                    async move {
                        for wave in 0..WAVES {
                            fs.write(auth, *id, (wave * CHUNK) as u64, payload)
                                .await
                                .unwrap();
                        }
                    }
                });
                futures::future::join_all(writers).await;
            }
            let secs = start.elapsed().as_secs_f64();
            let batches = fs.write_coordinator.batch_sizes()[batches_before..].to_vec();
            let commits: usize = batches.iter().sum();
            let bytes = (WAVES * members_count * CHUNK) as f64;
            let apply_secs = (fs.write_coordinator.apply_nanos() - apply_before) as f64 / 1e9;
            let stage_secs = (fs.write_coordinator.stage_nanos() - stage_before) as f64 / 1e9;
            eprintln!(
                "{} {members_count}-member waves: {:.0} waves/s ({:.0} us/wave), {:.0} MB/s, \
                 {} applies for {commits} commits ({:.2} commits/apply), sizes {:?}; \
                 worker busy {:.0}% ({:.0} us/apply, {:.0} us in stage_seg_deltas)",
                if lockstep { "lockstep" } else { "free-run" },
                WAVES as f64 / secs,
                secs * 1e6 / WAVES as f64,
                bytes / secs / 1e6,
                batches.len(),
                commits as f64 / batches.len() as f64,
                batch_histogram(&batches),
                100.0 * apply_secs / secs,
                apply_secs * 1e6 / batches.len() as f64,
                stage_secs * 1e6 / batches.len() as f64,
            );
        }
    }

    // Coordinator-only companion to bench_lockstep_member_waves: the same wave
    // shape with a trivial transaction per writer, so the reported rate is the
    // coordinator's own per-apply fixed cost with the extent path removed. The
    // `seg` arm adds one segment-counter delta per commit, which is what makes
    // an apply read before it writes; the difference between the arms is the
    // `stage_seg_deltas` share of that fixed cost.
    //   cargo test --release --lib -- --ignored --nocapture bench_lockstep_commit_waves
    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    #[ignore = "throughput measurement, run explicitly in release"]
    async fn bench_lockstep_commit_waves() {
        const WAVES: u64 = 2000;

        // The one-writer arm applies one commit per batch by construction, so
        // its us/apply is the un-amortized fixed cost of an apply; the
        // four-writer arm coalesces perfectly and prices the same fixed cost
        // spread over four commits.
        for (writers, seg_deltas) in [(1u64, false), (4, false), (1, true), (4, true)] {
            let fs = make_fs().await;
            let codec = codec();
            let batches_before = fs.write_coordinator.batch_sizes().len();
            let apply_before = fs.write_coordinator.apply_nanos();
            let start = std::time::Instant::now();
            for wave in 0..WAVES {
                let commits = (0..writers).map(|writer| {
                    let coord = fs.write_coordinator.clone();
                    let key = codec.extent_key(writer + 1, wave);
                    // One hot counter per writer, as a writer appending into
                    // its own active segment would produce.
                    let seg_key = codec.segcount_key(1, writer);
                    async move {
                        let mut txn = Transaction::new();
                        txn.put_bytes(&key, Bytes::from_static(b"payload"));
                        if seg_deltas {
                            txn.add_seg_delta(&seg_key, 7, 7);
                        }
                        coord.commit(txn).await
                    }
                });
                for result in futures::future::join_all(commits).await {
                    result.unwrap();
                }
            }
            let secs = start.elapsed().as_secs_f64();
            let batches = fs.write_coordinator.batch_sizes()[batches_before..].to_vec();
            let commits: usize = batches.iter().sum();
            let apply_secs = (fs.write_coordinator.apply_nanos() - apply_before) as f64 / 1e9;
            eprintln!(
                "coordinator-only {writers}-commit waves [{}]: {:.0} waves/s \
                 ({:.1} us/wave), {} applies for {commits} commits ({:.2} commits/apply); \
                 worker busy {:.0}%, {:.1} us/apply, {:.1} us/commit",
                if seg_deltas { "seg" } else { "no-seg" },
                WAVES as f64 / secs,
                secs * 1e6 / WAVES as f64,
                batches.len(),
                commits as f64 / batches.len() as f64,
                100.0 * apply_secs / secs,
                apply_secs * 1e6 / batches.len() as f64,
                apply_secs * 1e6 / commits as f64,
            );
        }
    }

    #[tokio::test]
    async fn commits_single_transaction() {
        let fs = make_fs().await;
        let mut txn = Transaction::new();
        // Use a real codec-built key so the segment extractor accepts it.
        let key = codec().extent_key(1, 0);
        txn.put_bytes(&key, Bytes::from_static(b"value"));
        fs.write_coordinator.commit(txn).await.unwrap();
        let v = fs.db.get_bytes(&key).await.unwrap();
        assert_eq!(v.as_deref(), Some(&b"value"[..]));
    }

    // The coalescing guarantee that holds without any timer: commits that queue
    // while an apply is in flight are drained into one following batch. This is
    // what makes singleton fragmentation self-correcting once the worker is
    // saturated, and it is the only coalescing the worker can do without
    // waiting. See the module header for why no wait was added.
    #[tokio::test]
    async fn commits_queued_during_an_apply_drain_into_one_batch() {
        let fs = make_fs().await;
        let codec = codec();
        let batches_before = fs.write_coordinator.batch_sizes().len();

        // Stall the first apply at its write permit, so the rest of the wave
        // provably arrives while that apply is still in flight.
        let commit_block = fs.db.flush_barrier().write_owned().await;
        let apply_reached = fs.write_coordinator.probe_next_apply();

        let mut replies = Vec::new();
        let (head_reply, head_rx) = oneshot::channel();
        let mut head = Transaction::new();
        head.put_bytes(&codec.extent_key(1, 0), Bytes::from_static(b"head"));
        fs.write_coordinator
            .sender
            .send(Request::Commit(head, head_reply))
            .unwrap();
        replies.push(head_rx);
        apply_reached.await.unwrap();

        for i in 1..4u64 {
            let (reply_tx, reply_rx) = oneshot::channel();
            let mut txn = Transaction::new();
            txn.put_bytes(&codec.extent_key(1, i), Bytes::from_static(b"tail"));
            fs.write_coordinator
                .sender
                .send(Request::Commit(txn, reply_tx))
                .unwrap();
            replies.push(reply_rx);
        }

        drop(commit_block);
        for reply in replies {
            reply.await.unwrap().unwrap();
        }

        assert_eq!(
            fs.write_coordinator.batch_sizes()[batches_before..],
            [1, 3],
            "the head commit applies alone; every commit that queued behind it \
             coalesces into the next batch"
        );
        for i in 0..4u64 {
            assert!(
                fs.db
                    .get_bytes(&codec.extent_key(1, i))
                    .await
                    .unwrap()
                    .is_some(),
                "commit {i} of the coalesced batch is missing"
            );
        }
    }

    #[tokio::test]
    async fn coalesces_concurrent_commits() {
        let fs = make_fs().await;
        let coord = fs.write_coordinator.clone();
        let codec = codec();
        let mut handles = Vec::new();
        for i in 0u64..32 {
            let c = coord.clone();
            let k = codec.extent_key(1, i);
            handles.push(tokio::spawn(async move {
                let mut txn = Transaction::new();
                txn.put_bytes(&k, Bytes::from(vec![1u8; 8]));
                c.commit(txn).await
            }));
        }
        for h in handles {
            h.await.unwrap().unwrap();
        }
        for i in 0u64..32 {
            let v = fs.db.get_bytes(&codec.extent_key(1, i)).await.unwrap();
            assert!(v.is_some());
        }
    }

    #[tokio::test]
    async fn sync_writes_commits_persist() {
        let fs = ZeroFS::new_in_memory_with_sync_writes(true).await.unwrap();
        let coord = fs.write_coordinator.clone();
        let codec = codec();
        let mut handles = Vec::new();
        for i in 0u64..16 {
            let c = coord.clone();
            let k = codec.extent_key(2, i);
            handles.push(tokio::spawn(async move {
                let mut txn = Transaction::new();
                txn.put_bytes(&k, Bytes::from(vec![2u8; 8]));
                c.commit(txn).await
            }));
        }
        for h in handles {
            h.await.unwrap().unwrap();
        }
        for i in 0u64..16 {
            let v = fs.db.get_bytes(&codec.extent_key(2, i)).await.unwrap();
            assert!(
                v.is_some(),
                "extent {i} not durable after sync_writes commit"
            );
        }
    }

    #[tokio::test]
    async fn empty_transaction_is_noop_not_fatal() {
        // SlateDB rejects empty WriteBatches with "empty write batch not
        // allowed". A no-op txn (e.g. sub-extent trim on a fully sparse range)
        // must short-circuit before reaching the db.
        let fs = make_fs().await;
        let txn = Transaction::new();
        assert!(txn.is_empty());
        fs.write_coordinator
            .commit(txn)
            .await
            .expect("empty txn should commit as a no-op");
    }

    #[tokio::test]
    async fn barrier_waits_for_prior_footprint_publication() {
        let fs = make_fs().await;
        let seg_key = codec().segcount_key(1, 1);
        let mut txn = Transaction::new();
        txn.add_seg_delta(&seg_key, 57_169, 57_169);

        // Queue the commit without awaiting its own reply. The barrier must not
        // complete until the worker has both applied it and published gauges.
        let (reply_tx, _reply_rx) = oneshot::channel();
        fs.write_coordinator
            .sender
            .send(Request::Commit(txn, reply_tx))
            .unwrap();
        fs.write_coordinator.barrier().await.unwrap();

        let stats = fs.extent_store.segment_gc_stats();
        assert_eq!(
            stats
                .segment_count
                .load(std::sync::atomic::Ordering::Relaxed),
            1
        );
        assert_eq!(
            stats
                .appended_bytes
                .load(std::sync::atomic::Ordering::Relaxed),
            57_169
        );
        assert_eq!(
            stats.live_bytes.load(std::sync::atomic::Ordering::Relaxed),
            57_169
        );
    }

    #[tokio::test]
    async fn dedup_only_transaction_publishes_terminal_outcome() {
        let fs = make_fs().await;
        let op_id = [0x5au8; 16];
        let mut txn = Transaction::new();
        txn.set_dedup_result(
            op_id,
            crate::dedup::DedupResult::Error {
                errno: libc::EEXIST as u32,
            },
        );
        assert!(txn.is_empty(), "ledger-only work has no user-data ops");

        fs.write_coordinator.commit(txn).await.unwrap();
        assert!(matches!(
            fs.dedup.get(&op_id),
            Some(crate::dedup::DedupResult::Error { errno })
                if errno == libc::EEXIST as u32
        ));
    }

    #[tokio::test]
    async fn counter_emitted_only_when_advanced() {
        let fs = make_fs().await;
        let counter_key = codec().system_counter_key();
        let before = fs.db.get_bytes(&counter_key).await.unwrap();

        // A commit that doesn't allocate any inode.
        let mut txn = Transaction::new();
        txn.put_bytes(&codec().extent_key(3, 0), Bytes::from_static(b"v"));
        fs.write_coordinator.commit(txn).await.unwrap();

        let after = fs.db.get_bytes(&counter_key).await.unwrap();
        assert_eq!(
            before, after,
            "counter key should not change without allocate"
        );

        // Now allocate and commit; counter must advance on disk.
        let _id = fs.inode_store.allocate();
        let mut txn = Transaction::new();
        txn.put_bytes(&codec().extent_key(3, 1), Bytes::from_static(b"v"));
        fs.write_coordinator.commit(txn).await.unwrap();

        let after_allocate = fs.db.get_bytes(&counter_key).await.unwrap();
        assert_ne!(
            after, after_allocate,
            "counter key should advance after allocate"
        );
    }

    #[tokio::test]
    async fn corrupt_segcount_value_fails_the_batch() {
        let fs = make_fs().await;
        let codec = codec();
        let seg_key = codec.segcount_key(1, 1);
        // Five bytes: neither the 16-byte encoding nor the legacy 8-byte one.
        fs.db
            .put_with_options(
                &seg_key,
                b"bogus",
                &slatedb::config::PutOptions::default(),
                &WriteOptions::default(),
            )
            .await
            .unwrap();

        let mut txn = Transaction::new();
        txn.put_bytes(&codec.extent_key(7, 0), Bytes::from_static(b"v"));
        txn.add_seg_delta(&seg_key, 5, 5);
        fs.write_coordinator
            .commit(txn)
            .await
            .expect_err("a corrupt segcount base must abort the batch, not default to 0");
    }

    #[tokio::test]
    async fn counter_reemitted_after_aborted_batch() {
        let fs = make_fs().await;
        let codec = codec();
        let seg_key = codec.segcount_key(1, 2);
        // Corrupt segcount base so the seg-delta-bearing batch aborts.
        fs.db
            .put_with_options(
                &seg_key,
                b"bogus",
                &slatedb::config::PutOptions::default(),
                &WriteOptions::default(),
            )
            .await
            .unwrap();

        // Allocate so the aborting batch stages a counter emission, then lose
        // that batch (and the staged counter put) to the segcount abort.
        let id = fs.inode_store.allocate();
        let mut txn = Transaction::new();
        txn.put_bytes(&codec.extent_key(id, 0), Bytes::from_static(b"v"));
        txn.add_seg_delta(&seg_key, 5, 5);
        fs.write_coordinator.commit(txn).await.unwrap_err();

        // A later batch with no new allocation must still emit a counter
        // covering `id` (via the id burned on abort); otherwise a restart
        // would hand out `id` again over this batch's durable records.
        let mut txn = Transaction::new();
        txn.put_bytes(&codec.extent_key(id, 1), Bytes::from_static(b"w"));
        fs.write_coordinator.commit(txn).await.unwrap();

        let persisted = fs
            .db
            .get_bytes(&codec.system_counter_key())
            .await
            .unwrap()
            .map(|b| KeyCodec::decode_counter(&b).unwrap())
            .unwrap_or(0);
        assert!(
            persisted > id,
            "persisted counter {persisted} must cover allocated id {id}"
        );
    }

    #[tokio::test]
    async fn stats_deltas_aggregate_per_shard_across_batches() {
        use crate::fs::STATS_SHARDS;
        use crate::fs::stats::StatsShardData;

        let fs = make_fs().await;
        let coord = fs.write_coordinator.clone();
        let codec = codec();

        // All inode ids congruent to 5 mod 100 map to stats shard 5.
        const SHARD: usize = 5;
        const TASKS: u64 = 32;

        let mut handles = Vec::new();
        for k in 0..TASKS {
            let c = coord.clone();
            let inode_id = SHARD as u64 + 100 * k;
            let key = codec.extent_key(inode_id, 0);
            handles.push(tokio::spawn(async move {
                let mut txn = Transaction::new();
                txn.put_bytes(&key, Bytes::from_static(b"x"));
                txn.add_raw_stats_delta(inode_id, ((k + 1) * 10) as i64, 1);
                c.commit(txn).await
            }));
        }
        for h in handles {
            h.await.unwrap().unwrap();
        }

        // 10 + 20 + ... + 320 = 5280
        let expected_bytes: u64 = (1..=TASKS).map(|k| k * 10).sum();
        assert_eq!(fs.global_stats.get_totals(), (expected_bytes, TASKS));

        let raw = fs
            .db
            .get_bytes(&codec.stats_shard_key(SHARD))
            .await
            .unwrap()
            .expect("shard 5 must be persisted");
        let shard: StatsShardData = bincode::deserialize(&raw).unwrap();
        assert_eq!(
            (shard.used_bytes, shard.used_inodes),
            (expected_bytes, TASKS)
        );

        // No other shard key may have been written.
        for i in 0..STATS_SHARDS {
            if i != SHARD {
                assert!(
                    fs.db
                        .get_bytes(&codec.stats_shard_key(i))
                        .await
                        .unwrap()
                        .is_none(),
                    "shard {i} written without any delta for it"
                );
            }
        }

        // Second wave: negative byte deltas must drain the shard back down,
        // both persisted and in memory.
        let mut handles = Vec::new();
        for k in 0..TASKS {
            let c = coord.clone();
            let inode_id = SHARD as u64 + 100 * k;
            let key = codec.extent_key(inode_id, 1);
            handles.push(tokio::spawn(async move {
                let mut txn = Transaction::new();
                txn.put_bytes(&key, Bytes::from_static(b"y"));
                txn.add_raw_stats_delta(inode_id, -(((k + 1) * 10) as i64), 0);
                c.commit(txn).await
            }));
        }
        for h in handles {
            h.await.unwrap().unwrap();
        }

        assert_eq!(fs.global_stats.get_totals(), (0, TASKS));
        let raw = fs
            .db
            .get_bytes(&codec.stats_shard_key(SHARD))
            .await
            .unwrap()
            .unwrap();
        let shard: StatsShardData = bincode::deserialize(&raw).unwrap();
        assert_eq!((shard.used_bytes, shard.used_inodes), (0, TASKS));
    }

    use crate::replication::transport::{
        ReceiverControl, ReplicationReceiver, ReplicationSender, ShipResult,
    };
    use crate::replication::types::{CoverageFrontier, PruneWatermark, ShipSeqno, WriterEpoch};
    use crate::replication::{ReplOp, Replicator};

    fn writer_epoch(value: u64) -> WriterEpoch {
        WriterEpoch::new(value).expect("test writer epochs are nonzero")
    }

    fn ship_seqno(value: u64) -> ShipSeqno {
        ShipSeqno::new(value).expect("test ship sequence numbers are nonzero")
    }

    fn prune_watermark(value: u64, current: u64) -> PruneWatermark {
        PruneWatermark::for_ship(value, ship_seqno(current))
            .expect("test watermark must precede its ship")
    }

    async fn serve_receiver(receiver: ReplicationReceiver) -> String {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = receiver.into_server();
        tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(server)
                .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
                .await
                .unwrap();
        });
        format!("http://{addr}")
    }

    async fn spawn_receiver() -> String {
        serve_receiver(ReplicationReceiver::new(
            Arc::new(crate::dedup::DedupCache::new()),
            None,
            "standby-under-test".to_string(),
        ))
        .await
    }

    async fn spawn_receiver_paused_before_append(
        epoch: u64,
        reached: Arc<tokio::sync::Notify>,
        resume: Arc<tokio::sync::Notify>,
    ) -> (String, ReceiverControl) {
        let receiver = ReplicationReceiver::new(
            Arc::new(crate::dedup::DedupCache::new()),
            None,
            "standby-under-test".to_string(),
        )
        .pause_epoch_before_append(epoch, reached, resume);
        let control = receiver.control();
        (serve_receiver(receiver).await, control)
    }

    async fn connect_sender(endpoint: &str) -> ReplicationSender {
        for _ in 0..100 {
            if let Ok(s) = ReplicationSender::connect(endpoint.to_string()).await {
                return s;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        panic!("could not connect to receiver");
    }

    async fn make_leased_replicating_fs_with_raw(
        lease: Arc<crate::replication::Lease>,
        replicator: Replicator,
    ) -> (ZeroFS, Arc<slatedb::Db>) {
        use crate::block_transformer::ZeroFsBlockTransformer;
        use crate::config::CompressionConfig;
        use slatedb::BlockTransformer;

        let test_key = [0u8; 32];
        let object_store: Arc<dyn slatedb::object_store::ObjectStore> =
            Arc::new(slatedb::object_store::memory::InMemory::new());
        let block_transformer: Arc<dyn BlockTransformer> =
            ZeroFsBlockTransformer::new_arc(&test_key, CompressionConfig::default());
        let raw_db = Arc::new(
            slatedb::DbBuilder::new(
                slatedb::object_store::path::Path::from("ha-apply-failure"),
                object_store.clone(),
            )
            .with_block_transformer(block_transformer)
            .with_filter_policies(crate::fs::filter_policy::filter_policies())
            .with_segment_extractor(Arc::new(crate::segment_extractor::ZeroFsSegmentExtractor))
            .build()
            .await
            .unwrap(),
        );
        let segment_codec = crate::frame_codec::FrameCodec::new(
            &test_key,
            crate::segment::SEGMENT_INFO,
            CompressionConfig::default(),
        );

        let fs = ZeroFS::new_with_slatedb_and_lease(
            crate::db::SlateDbHandle::ReadWrite(raw_db.clone()),
            u64::MAX,
            None,
            false,
            false,
            Some(lease),
            Some(replicator),
            Arc::new(crate::dedup::DedupCache::new()),
            None,
            crate::object_trace::ObjectTracer::new(),
            object_store,
            segment_codec,
            None,
            crate::config::StoreProfile::default(),
        )
        .await
        .unwrap();
        (fs, raw_db)
    }

    /// A second coordinator over the same fs's stores, with a replicator attached.
    fn replicating_coordinator(fs: &ZeroFS, replicator: Replicator) -> WriteCoordinator {
        WriteCoordinator::new(
            fs.db.clone(),
            fs.inode_store.clone(),
            fs.directory_store.clone(),
            fs.flush_coordinator.clone(),
            Arc::new(KeyCodec::new()),
            fs.global_stats.clone(),
            false,
            Some(replicator),
            fs.dedup.clone(),
            fs.lineage_token,
            fs.extent_store.clone(),
        )
    }

    #[tokio::test]
    async fn dedup_only_outcome_ships_and_publishes_on_standby() {
        let standby_dedup = Arc::new(crate::dedup::DedupCache::new());
        let endpoint = serve_receiver(ReplicationReceiver::new(
            standby_dedup.clone(),
            None,
            "dedup-only-standby".to_string(),
        ))
        .await;
        let (repl, control) = Replicator::new(endpoint.clone(), writer_epoch(7));
        control
            .set_sender_for_tests(Some(connect_sender(&endpoint).await))
            .await;
        let fs = make_fs().await;
        let coord = replicating_coordinator(&fs, repl);
        let op_id = [0x6bu8; 16];
        let mut txn = Transaction::new();
        txn.set_dedup_result(
            op_id,
            crate::dedup::DedupResult::Error {
                errno: libc::EEXIST as u32,
            },
        );
        coord.commit(txn).await.unwrap();

        // Watermark 1 publishes sequence 1's retained result.
        assert_eq!(
            connect_sender(&endpoint)
                .await
                .ship(
                    ship_seqno(2),
                    &[],
                    &[],
                    prune_watermark(1, 2),
                    writer_epoch(7),
                )
                .await
                .unwrap(),
            ShipResult::Accepted
        );
        assert!(matches!(
            standby_dedup.get(&op_id),
            Some(crate::dedup::DedupResult::Error { errno })
                if errno == libc::EEXIST as u32
        ));
    }

    #[tokio::test]
    async fn deposal_revokes_without_apply_or_flush() {
        let lease = crate::replication::Lease::new();
        lease.renew(std::time::Duration::from_secs(30));
        let (replicator, control) = Replicator::new("unused".to_string(), writer_epoch(1));
        let (fs, raw_db) = make_leased_replicating_fs_with_raw(lease.clone(), replicator).await;
        control.depose().await;

        let key = codec().extent_key(7, 0);
        let flushes_before = fs.flush_coordinator.completed_flush_count();
        let mut txn = Transaction::new();
        txn.put_bytes(&key, Bytes::from_static(b"never-applied"));
        assert_eq!(
            fs.write_coordinator
                .commit(txn)
                .await
                .expect_err("a terminally deposed replicator must reject the batch"),
            FsError::LeaderRejectedBeforeApply
        );

        assert!(!lease.is_valid(), "rejection must close the serving gate");
        assert!(
            raw_db.get(&key).await.unwrap().is_none(),
            "the rejected batch must not reach the local database"
        );
        assert_eq!(
            fs.flush_coordinator.completed_flush_count(),
            flushes_before,
            "deposal must not force-flush a stale database"
        );
    }

    // A standby's rejection is deposal evidence: a newer writer exists, so the
    // new history cannot contain this batch. It must fail, not be applied and
    // acked by the deposed leader.
    #[tokio::test]
    async fn rejected_ship_fails_the_batch_instead_of_acking() {
        let fs = make_fs().await;
        let endpoint = spawn_receiver().await;
        // A newer leader (epoch 5) shipped first: epoch-1 ships are rejected.
        let newer = connect_sender(&endpoint).await;
        assert_eq!(
            newer
                .ship(
                    ship_seqno(1),
                    &[ReplOp::Put("a".into(), "b".into())],
                    &[],
                    prune_watermark(0, 1),
                    writer_epoch(5),
                )
                .await
                .unwrap(),
            ShipResult::Accepted
        );

        let (repl, control) = Replicator::new(endpoint.clone(), writer_epoch(1));
        control
            .set_sender_for_tests(Some(connect_sender(&endpoint).await))
            .await;
        let coord = replicating_coordinator(&fs, repl);

        let codec = codec();
        let key = codec.extent_key(1, 0);
        let flushes_before = fs.flush_coordinator.completed_flush_count();
        let mut txn = Transaction::new();
        txn.put_bytes(&key, Bytes::from_static(b"v"));
        let error = coord
            .commit(txn)
            .await
            .expect_err("a deposed leader must fail the batch, not ack it");
        assert_eq!(error, FsError::LeaderRejectedBeforeApply);
        assert_eq!(
            fs.flush_coordinator.completed_flush_count(),
            flushes_before,
            "a writer proven stale must not flush before returning the clean failure"
        );
        assert!(
            fs.db.get_bytes(&key).await.unwrap().is_none(),
            "a deposed leader must not apply the rejected batch"
        );

        // Deposal is terminal: later batches fail too.
        let mut txn = Transaction::new();
        txn.put_bytes(&codec.extent_key(1, 1), Bytes::from_static(b"w"));
        assert_eq!(
            coord.commit(txn).await.expect_err("deposal must be sticky"),
            FsError::LeaderRejectedBeforeApply
        );
    }

    /// An acknowledged batch waits through recoverable suspension before local apply.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn shipped_batch_waits_through_suspension() {
        const EPOCH: u64 = 7;

        let _fatal_test_lock = FATAL_WRITE_TEST_LOCK.lock().await;

        let reached = Arc::new(tokio::sync::Notify::new());
        let resume = Arc::new(tokio::sync::Notify::new());
        let (endpoint, control) =
            spawn_receiver_paused_before_append(EPOCH, reached.clone(), resume.clone()).await;
        let (replicator, replication_control) =
            Replicator::new(endpoint.clone(), writer_epoch(EPOCH));
        replication_control
            .set_sender_for_tests(Some(connect_sender(&endpoint).await))
            .await;

        let lease = crate::replication::Lease::new();
        lease.renew(std::time::Duration::from_secs(30));
        let (fs, raw_db) = make_leased_replicating_fs_with_raw(lease.clone(), replicator).await;
        let codec = codec();
        let first_key = codec.extent_key(90, 0);

        let first_commit = {
            let coordinator = fs.write_coordinator.clone();
            let first_key = first_key.clone();
            tokio::spawn(async move {
                let mut txn = Transaction::new();
                txn.put_bytes(&first_key, Bytes::from_static(b"first"));
                coordinator.commit(txn).await
            })
        };
        tokio::time::timeout(std::time::Duration::from_secs(5), reached.notified())
            .await
            .expect("the first ship must reach standby admission");

        // Suspend after peer admission and before peer append.
        lease.suspend_for_tests();
        assert!(!fs.db.permits_successful_response());
        let previous =
            crate::db::DST_PANIC_ON_WRITE_ERROR.swap(true, std::sync::atomic::Ordering::SeqCst);
        let _fatal_guard = PanicOnFatalWrite(previous);
        resume.notify_one();
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            loop {
                let appended = control
                    .inspect_standby_for_tests(|tail, _| !tail.is_empty())
                    .await;
                if appended {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the standby must append the first ship");
        tokio::task::yield_now().await;
        assert!(
            !first_commit.is_finished(),
            "local apply must wait while successful responses are suspended"
        );
        assert!(raw_db.get(&first_key).await.unwrap().is_none());
        assert!(
            raw_db.get(&codec.ha_seqno_key()).await.unwrap().is_none(),
            "a suspended local apply must not publish provenance early"
        );

        assert!(lease.recover_for_tests(std::time::Duration::from_secs(30)));
        tokio::time::timeout(std::time::Duration::from_secs(5), first_commit)
            .await
            .expect("the recovered local apply must complete promptly")
            .expect("the commit caller task must not panic")
            .expect("the acknowledged batch must apply after recovery");

        assert!(
            raw_db.get(&codec.ha_seqno_key()).await.unwrap().is_some(),
            "the recovered local apply must persist its provenance"
        );
        assert_eq!(
            raw_db.get(&first_key).await.unwrap(),
            Some(Bytes::from_static(b"first"))
        );
        assert_eq!(
            control
                .inspect_standby_for_tests(|tail, _| {
                    tail.batches_in_order()
                        .map(|(seqno, _)| seqno)
                        .collect::<Vec<_>>()
                })
                .await,
            vec![1],
            "the standby must retain the sole acknowledged batch for takeover replay"
        );
    }

    /// A failed Solo-base flush prevents the dependent batch from shipping or applying.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn failed_solo_base_flush_stops_before_reconnect_ship() {
        const EPOCH: u64 = 7;

        let _fatal_test_lock = FATAL_WRITE_TEST_LOCK.lock().await;
        let receiver = ReplicationReceiver::new(
            Arc::new(crate::dedup::DedupCache::new()),
            None,
            "reconnect-flush-standby".to_string(),
        );
        let receiver_control = receiver.control();
        let endpoint = serve_receiver(receiver).await;
        let (replicator, replication_control) =
            Replicator::new(endpoint.clone(), writer_epoch(EPOCH));
        let lease = crate::replication::Lease::new();
        lease.renew(std::time::Duration::from_secs(30));
        let (fs, raw_db) = make_leased_replicating_fs_with_raw(lease.clone(), replicator).await;

        // The Solo mutation follows the lineage-taint flush.
        let solo_key = codec().extent_key(91, 0);
        let mut solo = Transaction::new();
        solo.put_bytes(&solo_key, Bytes::from_static(b"solo"));
        fs.write_coordinator.commit(solo).await.unwrap();
        let ha_stamp_key = codec().ha_seqno_key();
        let solo_stamp = fs
            .db
            .get_bytes(&ha_stamp_key)
            .await
            .unwrap()
            .expect("the applied Solo prefix must carry its durable-format HA stamp");
        let requested_before = fs.flush_coordinator.requested_flush_count();

        let barrier = fs.db.flush_barrier().read_owned().await;
        replication_control
            .set_sender_for_tests(Some(connect_sender(&endpoint).await))
            .await;
        let reconnect_key = codec().extent_key(91, 1);
        let op_id = [0x91; 16];
        let reconnect_commit = {
            let coordinator = fs.write_coordinator.clone();
            let reconnect_key = reconnect_key.clone();
            tokio::spawn(async move {
                let mut txn = Transaction::new();
                txn.put_bytes(&reconnect_key, Bytes::from_static(b"reconnected"));
                txn.set_dedup_result(op_id, crate::dedup::DedupResult::Applied);
                coordinator.commit(txn).await
            })
        };

        // Wait until the worker requests the blocked pre-ship flush.
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            loop {
                if fs.flush_coordinator.requested_flush_count() > requested_before {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the reconnect batch must request its pre-ship base flush");
        assert!(
            !reconnect_commit.is_finished(),
            "the reconnect batch must wait behind the Solo-base barrier"
        );
        assert!(
            fs.db.get_bytes(&reconnect_key).await.unwrap().is_none(),
            "the reconnect mutation must not apply before the base flush"
        );
        assert!(
            fs.dedup.get(&op_id).is_none(),
            "an unshipped, unapplied mutation cannot publish its exact result"
        );
        assert!(
            receiver_control
                .inspect_standby_for_tests(|tail, _| tail.is_empty())
                .await,
            "the reconnect mutation must not reach the standby before the base flush"
        );
        assert_eq!(
            fs.db.get_bytes(&ha_stamp_key).await.unwrap(),
            Some(solo_stamp.clone()),
            "the blocked reconnect must leave the database at the applied Solo prefix"
        );

        let previous =
            crate::db::DST_PANIC_ON_WRITE_ERROR.swap(true, std::sync::atomic::Ordering::SeqCst);
        let _fatal_guard = PanicOnFatalWrite(previous);
        lease.revoke();
        drop(barrier);

        tokio::time::timeout(std::time::Duration::from_secs(5), reconnect_commit)
            .await
            .expect("the failed Solo-base flush must terminate promptly")
            .expect("the commit caller task must not panic")
            .expect_err("fatal commit-worker exit must drop the reply");
        assert!(
            fs.dedup.get(&op_id).is_none(),
            "a failed pre-ship flush leaves the result unpublished"
        );
        assert!(
            receiver_control
                .inspect_standby_for_tests(|tail, _| tail.is_empty())
                .await,
            "a failed base flush must leave the standby untouched"
        );
        assert_eq!(
            raw_db.get(&ha_stamp_key).await.unwrap(),
            Some(solo_stamp),
            "a failed base flush must not persist provenance for the reconnect batch"
        );

        lease.renew(std::time::Duration::from_secs(30));
        let mut later = Transaction::new();
        later.put_bytes(&codec().extent_key(91, 2), Bytes::from_static(b"later"));
        fs.write_coordinator
            .commit(later)
            .await
            .expect_err("the fatal Solo-base flush must leave the worker dead");
    }

    /// The first post-Solo ship follows a flush of its local base.
    #[tokio::test]
    async fn solo_base_is_flushed_before_the_first_reconnect_ship() {
        let fs = make_fs().await;
        let endpoint = spawn_receiver().await;
        let (repl, control) = Replicator::new(endpoint.clone(), writer_epoch(7));
        control
            .set_sender_for_tests(Some(connect_sender(&endpoint).await))
            .await;
        let coord = replicating_coordinator(&fs, repl);
        let codec = codec();

        let mut txn = Transaction::new();
        txn.put_bytes(&codec.extent_key(1, 0), Bytes::from_static(b"a"));
        coord.commit(txn).await.unwrap();
        let baseline = fs.flush_coordinator.completed_flush_count();

        control.set_sender_for_tests(None).await;
        for i in 1..=2u64 {
            let mut txn = Transaction::new();
            txn.put_bytes(&codec.extent_key(1, i), Bytes::from_static(b"s"));
            coord.commit(txn).await.unwrap();
        }
        assert_eq!(
            fs.flush_coordinator.completed_flush_count(),
            baseline + 1,
            "the solo episode forces exactly the one-time taint flush"
        );

        control
            .set_sender_for_tests(Some(connect_sender(&endpoint).await))
            .await;
        let reconnect_op_id = [0xa7; 16];
        let mut txn = Transaction::new();
        txn.put_bytes(&codec.extent_key(1, 3), Bytes::from_static(b"c"));
        txn.set_dedup_result(reconnect_op_id, crate::dedup::DedupResult::Applied);
        coord.commit(txn).await.unwrap();
        assert_eq!(
            fs.flush_coordinator.completed_flush_count(),
            baseline + 2,
            "the Solo base must be forced durable before the first reconnect ship"
        );
        assert!(matches!(
            fs.dedup.get(&reconnect_op_id),
            Some(crate::dedup::DedupResult::Applied)
        ));

        let mut txn = Transaction::new();
        txn.put_bytes(&codec.extent_key(1, 4), Bytes::from_static(b"d"));
        coord.commit(txn).await.unwrap();
        assert_eq!(
            fs.flush_coordinator.completed_flush_count(),
            baseline + 2,
            "steady-state shipped batches must not force a flush"
        );
    }

    #[tokio::test]
    async fn idle_receiver_repair_wakes_commit_worker() {
        let fs = make_fs().await;
        let endpoint = spawn_receiver().await;
        let (repl, control) = Replicator::new(endpoint.clone(), writer_epoch(7));
        control
            .set_sender_for_tests(Some(connect_sender(&endpoint).await))
            .await;
        let coord = replicating_coordinator(&fs, repl);

        let mut txn = Transaction::new();
        txn.put_bytes(&codec().extent_key(1, 0), Bytes::from_static(b"acked"));
        coord.commit(txn).await.unwrap();
        let baseline = fs.flush_coordinator.completed_flush_count();
        assert_eq!(
            control.coverage_frontier(),
            CoverageFrontier::new(Some(ship_seqno(1)), None).unwrap()
        );

        // Request repair while the commit queue is idle.
        control.repair_base().await.unwrap();
        assert_eq!(
            fs.flush_coordinator.completed_flush_count(),
            baseline + 1,
            "an idle receiver replacement must actively repair the acknowledged base"
        );
        assert_eq!(
            control.coverage_frontier(),
            CoverageFrontier::new(Some(ship_seqno(1)), Some(ship_seqno(1))).unwrap()
        );
    }

    /// Reconnect-base flush preserves staged extent publication and readability.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn reconnect_flush_handles_staged_extent() {
        let fs = make_fs().await;
        let endpoint = spawn_receiver().await;
        let (repl, control) = Replicator::new(endpoint.clone(), writer_epoch(7));
        let coord = replicating_coordinator(&fs, repl);
        let baseline = fs.flush_coordinator.completed_flush_count();

        let mut solo = Transaction::new();
        let solo_tail = fs
            .extent_store
            .write(&mut solo, 41, 0, &Bytes::from_static(b"solo"), 0)
            .await
            .unwrap();
        coord.commit(solo).await.unwrap();
        fs.extent_store.apply_tail_update(41, solo_tail);

        control
            .set_sender_for_tests(Some(connect_sender(&endpoint).await))
            .await;
        let mut reconnect = Transaction::new();
        let reconnect_tail = fs
            .extent_store
            .write(
                &mut reconnect,
                41,
                4,
                &Bytes::from_static(b"-reconnected"),
                4,
            )
            .await
            .unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(5), coord.commit(reconnect))
            .await
            .expect("the base flush must not deadlock on the extent publication guard")
            .unwrap();
        fs.extent_store.apply_tail_update(41, reconnect_tail);

        assert_eq!(
            fs.extent_store.read(41, 0, 16).await.unwrap().as_ref(),
            b"solo-reconnected"
        );
        assert_eq!(
            fs.flush_coordinator.completed_flush_count(),
            baseline + 2,
            "the Solo taint and reconnect base each cross one durability barrier"
        );
    }
}
