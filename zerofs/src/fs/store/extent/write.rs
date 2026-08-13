//! Write path: read-modify-write over full extents staged into the in-RAM
//! open segment, background and synchronous sealing (the durability
//! barrier), delete/truncate/zero-range staging with segment-counter
//! debits, and the tail cache for sequential appends.

#[cfg(feature = "failpoints")]
use crate::failpoints::{self as fp, fail_point};

use super::inflight;
use super::{CachedExtentLocation, ExtentStore, PARALLEL_EXTENT_OPS, TailUpdate, ZERO_EXTENT};
use crate::db::Transaction;
use crate::frame_codec::Compressed;
use crate::fs::inode::InodeId;
use crate::fs::{EXTENT_SIZE, FsError};
use crate::replication::ReplOp;
use crate::segment::{DirEntry, FrameLoc, Segid};
use bytes::{Bytes, BytesMut};
use futures::stream::{self, FuturesUnordered, StreamExt, TryStreamExt};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tracing::error;

/// Frames per write batch before pre-compression fans out on rayon; below
/// this the dispatch overhead outweighs the parallelism.
const PARALLEL_COMPRESS_MIN_FRAMES: usize = 1024 * 1024 / EXTENT_SIZE;

/// Old-FrameLoc discovery pivot in `stage_edits`: at or below this many
/// candidate extents, per-key point lookups replace the single range scan.
/// Bloom filters make lookups of absent keys free, so overwriting holes in a
/// pre-sized sparse file costs no metadata I/O; a range scan pays read-ahead
/// in every sorted run regardless. One 1 MiB write spans this many extents,
/// comfortably above the canonical 256 KiB NBD stripe-member chunk.
const OLD_DEBIT_POINT_LOOKUPS_MAX: usize = 1024 * 1024 / EXTENT_SIZE;

#[inline]
fn should_parallel_compress(frame_count: usize) -> bool {
    frame_count >= PARALLEL_COMPRESS_MIN_FRAMES
}

/// Injectable stand-in for a batch AEAD failure, so tests can abandon a
/// reservation mid-window without corrupting the codec. See
/// [`fp::STAGE_BATCH_SEAL_FAIL`]; compiled out entirely without the feature.
#[cfg(feature = "failpoints")]
fn batch_seal_failpoint() -> Result<(), crate::segment::SegmentError> {
    fail_point!(fp::STAGE_BATCH_SEAL_FAIL, |_| Err(
        crate::segment::SegmentError::Malformed("injected batch AEAD failure")
    ));
    Ok(())
}

pub(super) const TAIL_CACHE_BYTES: usize = 32 * 1024 * 1024;

/// Independent inode-affine append lanes. Four matches the foreground fio
/// workload while keeping the number of preallocated open buffers bounded.
pub(super) const OPEN_SEGMENT_LANES: usize = 4;

/// Seal (PUT) the open segment once its packed frames reach this size, bounding
/// the in-RAM buffer between flushes. The seal PUT is concurrent multipart
/// (`SegmentStore::put_segment`), so its fsync-path latency stays bounded
/// despite the size.
pub(crate) const SEAL_THRESHOLD: usize = 256 * 1024 * 1024;

/// Max active seal PUTs and finalized seal generations retained in RAM. Separate
/// semaphores enforce both limits so a failed PUT keeps its residency charge
/// without consuming upload capacity needed by a flush retry. Each append lane
/// also has at most one open generation.
pub(crate) const MAX_INFLIGHT_SEALS: usize = 4;

/// The in-RAM open segment. Frames are sealed (compressed+encrypted) and appended
/// here at write time, so an extent's location is known and committed eagerly;
/// the segment object is PUT only on flush or when it crosses [`SEAL_THRESHOLD`].
pub(super) struct OpenSegment {
    pub(super) segid: Segid,
    pub(super) buf: Vec<u8>,
    pub(super) dir: Vec<DirEntry>,
}

/// One independently ordered append stream. Its gate covers frame-index/AAD
/// *assignment* — the short reservation window — while the batch AEAD that
/// fills a reservation runs outside it and other lanes remain concurrent.
pub(super) struct OpenLane {
    pub(super) append_gate: tokio::sync::Mutex<()>,
    pub(super) open: std::sync::Mutex<OpenSegment>,
    /// Held shared by every [`Reservation`] from the moment it claims a
    /// frame-index run and byte range until its sealed bytes are copied in.
    /// A rotation takes it exclusively — always while also holding
    /// `append_gate`, so no new reservation can start — and therefore never
    /// seals a segment containing an unfilled hole.
    pub(super) fill_barrier: tokio::sync::RwLock<()>,
}

/// A held append gate, bundled with the lane it belongs to. The two are never
/// separate values — every constructor takes one lane and locks that lane's own
/// gate — so a rotation cannot be aimed at a lane other than the one it froze.
/// Previously the witness was a bare `MutexGuard`, which any lane's guard
/// satisfied.
pub(super) struct LaneAppendGuard<'a> {
    lane: &'a OpenLane,
    _gate: tokio::sync::MutexGuard<'a, ()>,
}

impl<'a> LaneAppendGuard<'a> {
    async fn lock(lane: &'a OpenLane) -> Self {
        let gate = lane.append_gate.lock().await;
        Self { lane, _gate: gate }
    }

    fn try_lock(lane: &'a OpenLane) -> Option<Self> {
        lane.append_gate
            .try_lock()
            .ok()
            .map(|gate| Self { lane, _gate: gate })
    }

    /// The lane this gate belongs to. Reborrowed from the lane's own lifetime,
    /// so it outlives moves of the guard itself.
    fn lane(&self) -> &'a OpenLane {
        self.lane
    }
}

/// A lane frozen for rotation: its append gate is held, so no further
/// reservation can be placed, and its fill barrier is held exclusively, so every
/// reservation already placed has copied its sealed bytes in. Built only from a
/// [`LaneAppendGuard`], which is what ties both guards and the rotated lane to
/// one another.
pub(super) struct LaneFreeze<'a> {
    lane: &'a OpenLane,
    _filled: tokio::sync::RwLockWriteGuard<'a, ()>,
}

impl<'a> LaneFreeze<'a> {
    /// Waits out reservations already mid-AEAD on the lane; none can be added,
    /// so the wait is bounded by one batch's AEAD.
    async fn acquire(appended: &LaneAppendGuard<'a>) -> Self {
        let lane = appended.lane();
        let filled = lane.fill_barrier.write().await;
        Self {
            lane,
            _filled: filled,
        }
    }
}

/// A claim on a contiguous frame-index run in one lane's open segment, plus the
/// byte range backing it. Taken under the lane's append gate so frame indices
/// stay dense and AAD-unique; filled after the batch AEAD, which runs outside
/// that gate. Concurrent reservations on a lane fill in any order: each one
/// only ever writes its own disjoint byte range.
///
/// Claiming is not free: it zero-extends the buffer by the batch's sealed
/// size under the gate (~40% of the AEAD's cost), which is what makes an
/// abandoned reservation read as zeros instead of foreign heap bytes and
/// lets fills land out of order. The gate hold shrinks about 2x, not to
/// nothing; a rotation-triggering claim additionally keeps the gate through
/// its own AEAD, fill, and rotation (1 in N batches, unmeasured by the
/// gate_hold phase timer). A delete-only batch claims nothing and therefore
/// never triggers rotation; an over-threshold buffer left behind by an
/// abandoned rotation is picked up by the next frame-bearing claim or by
/// the flush barrier.
///
/// The claim-to-fill window in [`ExtentStore::stage_edits`] must stay free of
/// `.await`: with no suspension point in it, a dropped (cancelled) staging
/// future cannot strand a reservation and wedge every later rotation on the
/// lane. Compression and the AEAD are CPU work, so this costs nothing today —
/// but adding an await between the claim and the fill would need a Drop-based
/// abandon path instead.
struct Reservation {
    segid: Segid,
    first_frame: u32,
    /// Byte offset of each frame's length prefix, in frame-index order. The
    /// sealed body length is fixed at claim time (see [`Compressed::sealed_len`])
    /// and already written into the prefix, so only bodies remain.
    offsets: Vec<u64>,
    /// Sealed body length of each frame, matching `offsets`.
    lens: Vec<u32>,
}

impl Reservation {
    /// Claim space for `frames` — `(extent, sealed body length)` in order — at
    /// the end of `open`. Caller must hold the lane's append gate and a shared
    /// `fill_barrier` guard, and keep the latter until [`Self::fill`].
    ///
    /// The reserved bytes are zeroed and each frame's length prefix written
    /// immediately, so the buffer stays a structurally walkable frame stream at
    /// every instant; only the AEAD bodies are outstanding.
    fn claim(open: &mut OpenSegment, inode: InodeId, frames: &[(u64, usize)]) -> Self {
        let segid = open.segid;
        let first_frame = open.dir.len() as u32;
        let mut offset = open.buf.len() as u64;
        let mut offsets = Vec::with_capacity(frames.len());
        let mut lens = Vec::with_capacity(frames.len());
        open.dir.reserve(frames.len());
        for &(extent, sealed_len) in frames {
            let len = sealed_len as u32;
            offsets.push(offset);
            lens.push(len);
            open.dir.push(DirEntry {
                byte_offset: offset,
                len,
                inode,
                extent,
            });
            offset += crate::segment::LEN_PREFIX as u64 + sealed_len as u64;
        }
        open.buf.resize(offset as usize, 0);
        for (offset, len) in offsets.iter().zip(&lens) {
            let start = *offset as usize;
            open.buf[start..start + crate::segment::LEN_PREFIX].copy_from_slice(&len.to_le_bytes());
        }
        Self {
            segid,
            first_frame,
            offsets,
            lens,
        }
    }

    /// Copy this batch's sealed bodies into the reserved range and return the
    /// resulting [`FrameLoc`]s. `sealed` must be the output of
    /// `seal_compressed_batch` over exactly this claim's frames.
    fn fill(
        &self,
        open: &mut OpenSegment,
        sealed: &[(u64, u64, Vec<u8>)],
    ) -> Result<Vec<FrameLoc>, FsError> {
        // Unreachable: a rotation cannot run while this reservation holds its
        // shared `fill_barrier` guard, so the lane's segment identity is pinned
        // for the whole claim-to-fill window. Checked anyway — writing a frame
        // into the wrong segment's buffer would forge its AAD binding.
        if open.segid != self.segid || sealed.len() != self.offsets.len() {
            error!(
                "reservation for {:?} lost its open segment (now {:?}) or frame count",
                self.segid, open.segid
            );
            return Err(FsError::IoError);
        }
        let mut locs = Vec::with_capacity(sealed.len());
        for (i, (offset, len)) in self.offsets.iter().zip(&self.lens).enumerate() {
            let body = &sealed[i].2;
            if body.len() != *len as usize {
                error!(
                    "sealed frame {} of {:?} is {} bytes, reserved {}",
                    self.first_frame + i as u32,
                    self.segid,
                    body.len(),
                    len
                );
                return Err(FsError::IoError);
            }
            let start = *offset as usize + crate::segment::LEN_PREFIX;
            open.buf[start..start + body.len()].copy_from_slice(body);
            locs.push(FrameLoc {
                segid: self.segid,
                frame_index: self.first_frame + i as u32,
                byte_offset: *offset,
                byte_len: crate::segment::LEN_PREFIX as u32 + len,
            });
        }
        Ok(locs)
    }
}

/// Nanoseconds spent in each phase of [`ExtentStore::stage_edits`], summed over
/// every staging call. Concurrency benchmarks read these to attribute
/// serialization to a phase instead of guessing at it.
#[cfg(test)]
#[derive(Default)]
pub(super) struct StagePhaseNanos {
    /// Superseded-FrameLoc discovery (point lookups or the range scan).
    pub(super) old_debit: std::sync::atomic::AtomicU64,
    /// Compression, deliberately outside every lock.
    pub(super) compress: std::sync::atomic::AtomicU64,
    /// Waiting for the extent-ref publication read guard.
    pub(super) protect_ref: std::sync::atomic::AtomicU64,
    /// Waiting to enter the lane's append gate.
    pub(super) gate_wait: std::sync::atomic::AtomicU64,
    /// Holding the lane's append gate: reserving the frame-index run and byte
    /// range only (the per-lane serial section).
    pub(super) gate_hold: std::sync::atomic::AtomicU64,
    /// Batch AEAD, outside the append gate.
    pub(super) aead: std::sync::atomic::AtomicU64,
    /// Blocking on the lane's open-buffer std mutex to fill the reservation.
    pub(super) open_lock_wait: std::sync::atomic::AtomicU64,
    /// Copying sealed frames into the reserved range under that mutex.
    pub(super) append: std::sync::atomic::AtomicU64,
    /// Rotation admission: the residency permit plus the reservation drain.
    pub(super) spawn_seal: std::sync::atomic::AtomicU64,
    /// Staging pointers, cache inserts, and segment-counter deltas onto the txn.
    pub(super) txn_stage: std::sync::atomic::AtomicU64,
    /// Staging calls that carried at least one frame.
    pub(super) batches: std::sync::atomic::AtomicU64,
}

#[cfg(test)]
impl StagePhaseNanos {
    fn add(counter: &std::sync::atomic::AtomicU64, since: std::time::Instant) {
        counter.fetch_add(
            since.elapsed().as_nanos() as u64,
            std::sync::atomic::Ordering::Relaxed,
        );
    }
}

/// Immutable segment bytes retained until publication succeeds. The owned permit
/// makes removal from `sealing` the single release point for its RAM budget.
pub(super) struct SealingGeneration {
    pub(super) bytes: Bytes,
    _residency: tokio::sync::OwnedSemaphorePermit,
}

impl ExtentStore {
    #[inline]
    pub(super) fn open_lane(&self, id: InodeId) -> &OpenLane {
        let mixed = id ^ (id >> 32);
        &self.open_lanes[(mixed as usize) & (OPEN_SEGMENT_LANES - 1)]
    }

    /// Lock an append lane for one staging batch. Inode affinity is the fast
    /// path: an uncontended writer keeps its frames on one lane's segments.
    /// When the affine gate is held (two hot inodes colliding on a lane, the
    /// common case for a handful of NBD stripe members), spill to any idle
    /// lane instead of queueing: colliding writers would interleave frames in
    /// the shared open segment anyway, so spilling costs no read locality
    /// while restoring lane parallelism. When every gate is busy, wait on the
    /// affine one (tokio's FIFO mutex keeps the flush barrier's freeze fair).
    async fn lock_append_lane(&self, id: InodeId) -> LaneAppendGuard<'_> {
        let preferred = self.open_lane(id);
        if let Some(guard) = LaneAppendGuard::try_lock(preferred) {
            return guard;
        }
        for lane in self.open_lanes.iter() {
            if let Some(guard) = LaneAppendGuard::try_lock(lane) {
                return guard;
            }
        }
        LaneAppendGuard::lock(preferred).await
    }

    fn tail_get(&self, id: InodeId) -> Option<(u64, Bytes)> {
        self.tail_cache.get(&id).map(|e| (*e).clone())
    }
    fn tail_set(&self, id: InodeId, extent_idx: u64, data: Bytes) {
        self.tail_cache.insert(id, (extent_idx, data));
    }
    fn tail_invalidate(&self, id: InodeId) {
        self.tail_cache.remove(&id);
    }

    /// Inclusive extent range a write at `offset` for `len` bytes touches, or
    /// `None` for an empty write. Shared by the write path's in-flight
    /// registration and by [`Self::write`] itself so the two cannot diverge.
    pub(crate) fn extent_span(offset: u64, len: u64) -> Option<(u64, u64)> {
        let end_offset = offset.checked_add(len)?;
        if len == 0 {
            return None;
        }
        Some((
            offset / EXTENT_SIZE as u64,
            (end_offset - 1) / EXTENT_SIZE as u64,
        ))
    }

    /// Claim `[start, end]` until the returned guard drops, so a later staging
    /// read of those extents blocks until this write applies. Register under
    /// the per-inode lock and hold it until the commit reply resolves and
    /// [`Self::apply_tail_update`] has run.
    pub(crate) fn register_inflight_write(
        &self,
        id: InodeId,
        start: u64,
        end: u64,
    ) -> inflight::InflightWriteGuard {
        self.inflight_writes.register(id, start, end)
    }

    /// Wait until no queued write on `id` overlaps `[start, end]`.
    pub(crate) async fn wait_for_inflight_overlap(&self, id: InodeId, start: u64, end: u64) {
        self.inflight_writes.wait_for_overlap(id, start, end).await
    }

    /// Apply a `write`'s tail-cache effect. Call only after its commit succeeds.
    pub fn apply_tail_update(&self, id: InodeId, update: TailUpdate) {
        match update {
            TailUpdate::Set { extent_idx, data } => self.tail_set(id, extent_idx, data),
            TailUpdate::Clear => self.tail_invalidate(id),
            TailUpdate::Keep => {}
        }
    }

    /// Raw `[len][sealed]` bytes of a frame still resident in RAM (the open buffer
    /// or an in-flight seal), or `None` once its segment is PUT (the standby reads
    /// the shared store directly). Used to ship un-PUT segments' bytes for HA.
    fn read_frame_for_ship(&self, segid: Segid, byte_offset: u64, byte_len: u32) -> Option<Bytes> {
        let start = byte_offset as usize;
        let end = start.checked_add(byte_len as usize)?;
        for lane in self.open_lanes.iter() {
            let open = lane.open.lock().unwrap();
            if open.segid == segid {
                return open.buf.get(start..end).map(Bytes::copy_from_slice);
            }
        }
        let sealing = self.sealing.lock().unwrap();
        let generation = sealing.get(&segid)?;
        (end <= generation.bytes.len()).then(|| generation.bytes.slice(start..end))
    }

    /// Enrich a batch's replication ops: an extent-write `Put` whose segment is
    /// still un-PUT becomes a `PutFrame` carrying the sealed frame bytes, so
    /// the standby can materialize that segment on takeover. Already-PUT
    /// segments stay plain `Put`. Called by the commit worker when replicating.
    pub fn enrich_repl_ops(&self, ops: Vec<ReplOp>) -> Vec<ReplOp> {
        ops.into_iter()
            .map(|op| match op {
                ReplOp::Put(k, v) => {
                    if self.key_codec.parse_extent_key(&k).is_some()
                        && let Some(loc) = FrameLoc::decode(&v)
                        && let Some(frame) =
                            self.read_frame_for_ship(loc.segid, loc.byte_offset, loc.byte_len)
                    {
                        ReplOp::PutFrame(k, v, frame)
                    } else {
                        ReplOp::Put(k, v)
                    }
                }
                other => other,
            })
            .collect()
    }

    /// Stage the extent-key delete only: the segment-counter debit is the
    /// caller's job (see [`Self::delete_range`], which debits as it scans).
    pub fn delete(&self, txn: &mut Transaction, id: InodeId, extent_idx: u64) {
        let key = self.key_codec.extent_key(id, extent_idx);
        txn.delete_bytes(&key);
        txn.update_cached_extent_location(id, extent_idx, None);
    }

    /// Stage live/total byte deltas for `segid`'s counter onto the txn; the
    /// commit worker folds them into the absolute `(live, total)`. A debit
    /// passes `total_delta == 0` (`total` is monotonic).
    pub(super) fn seg_delta(
        &self,
        txn: &mut Transaction,
        segid: Segid,
        live_delta: i64,
        total_delta: i64,
    ) {
        debug_assert!(
            total_delta <= 0 || txn.has_extent_ref_guard(),
            "a positive segment credit must remain publication-protected through commit"
        );
        txn.add_seg_delta(
            &self.key_codec.segcount_key(segid.epoch, segid.counter),
            live_delta,
            total_delta,
        );
    }

    /// Stage deletes for allocated extents in `[start, end)` with their live-byte
    /// debits. The transaction must be fresh with respect to this inode's extent
    /// keys: the database scan cannot see writes already staged on `txn`.
    pub async fn delete_range(
        &self,
        txn: &mut Transaction,
        id: InodeId,
        start: u64,
        end: u64,
    ) -> Result<(), FsError> {
        // Drain queued writes before touching the tail: one that applies after
        // this invalidation would republish a tail for an extent this call is
        // about to delete. It also restores the debit exclusion the comment
        // below relies on, which the inode lock alone no longer provides once
        // the write path releases it at submit.
        self.inflight_writes.wait_for_all(id).await;
        self.tail_invalidate(id);
        if start >= end {
            return Ok(());
        }
        // Debit each removed extent's bytes from its segment's live-byte counter.
        // One forward-map scan (cheaper than a GET per extent); the caller's inode
        // write lock plus the in-flight drain above serialise this read-then-delete
        // against a concurrent write to the same extent, so no segment is debited
        // twice for one frame.
        let start_key = self.key_codec.extent_key(id, start);
        let end_key = self.key_codec.extent_key(id, end);
        let mut stream = self
            .db
            .scan(start_key..end_key)
            .await
            .map_err(|_| FsError::IoError)?;
        while let Some(result) = stream.next().await {
            let (key, value) = result.map_err(|_| FsError::IoError)?;
            if let Some(extent_idx) = self.key_codec.parse_extent_key(&key) {
                if let Some(loc) = FrameLoc::decode(&value) {
                    // Delete debit: live only, total untouched (monotonic).
                    self.seg_delta(txn, loc.segid, -(loc.byte_len as i64), 0);
                }
                self.delete(txn, id, extent_idx);
            }
        }
        Ok(())
    }

    /// Append the non-zero extents of `edits` to the open-segment buffer (no PUT)
    /// and stage their extent pointers; stage extent-key deletes for the holes.
    /// Kicks off a background seal when the buffer crosses [`SEAL_THRESHOLD`].
    async fn stage_edits(
        &self,
        txn: &mut Transaction,
        id: InodeId,
        edits: &[(u64, Option<Bytes>)],
        old_extent_end: u64,
    ) -> Result<(), FsError> {
        // Extent indices must be unique within one batch (order is free): the
        // point-lookup path below emits one debit per `edits` entry, so a
        // duplicated extent would debit the same superseded frame twice.
        debug_assert!(
            {
                let mut seen = HashSet::new();
                edits.iter().all(|(e, _)| seen.insert(*e))
            },
            "stage_edits requires unique extent indices per batch"
        );
        #[cfg(test)]
        let phase = Arc::clone(&self.stage_phase_nanos);
        #[cfg(test)]
        let t_old_debit = std::time::Instant::now();
        let mut old_debits: Vec<(Segid, u32)> = Vec::new();
        if let (Some(min), Some(max)) = (
            edits.iter().map(|(e, _)| *e).min(),
            edits.iter().map(|(e, _)| *e).max(),
        ) && min < old_extent_end
        {
            // An extent whose index starts at or beyond the old EOF cannot
            // supersede an old FrameLoc. Keep the unaligned old tail in range,
            // but do not make append-only metadata reads for newer extents.
            let scan_end = max.saturating_add(1).min(old_extent_end);
            #[cfg(test)]
            self.old_extent_scan_ranges
                .lock()
                .unwrap()
                .push((min, scan_end));
            let edited: Vec<u64> = edits
                .iter()
                .map(|(e, _)| *e)
                .filter(|extent| *extent < scan_end)
                .collect();
            if edited.len() <= OLD_DEBIT_POINT_LOOKUPS_MAX {
                // Few candidates: per-key point lookups. SlateDB answers an
                // absent key from bloom filters, so overwriting holes in a
                // pre-sized sparse file (the NBD stripe-member shape) needs no
                // metadata I/O, while a range scan pays iterator setup plus
                // read-ahead in every sorted run even when the range is empty.
                let found: Vec<Option<(Segid, u32)>> = stream::iter(edited)
                    .map(|extent| async move {
                        // Warm path: this store's own committed writes publish
                        // every extent's location, so overwrites of extents it
                        // wrote answer the debit from the location cache. The
                        // database fallback can land on a cold SST block on
                        // the remote object store, where one fetch costs a
                        // network round trip on the write ACK path.
                        if let Some(cached) = self
                            .extent_location_cache
                            .get(&(id, extent))
                            .map_err(|_| FsError::IoError)?
                        {
                            return Ok(match cached {
                                CachedExtentLocation::Hole => None,
                                CachedExtentLocation::Frame(loc) => Some((loc.segid, loc.byte_len)),
                            });
                        }
                        #[cfg(test)]
                        self.old_debit_db_lookups
                            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        let key = self.key_codec.extent_key(id, extent);
                        let value = self
                            .db
                            .get_bytes(&key)
                            .await
                            .map_err(|_| FsError::IoError)?;
                        Ok::<_, FsError>(
                            value
                                .and_then(|v| FrameLoc::decode(&v))
                                .map(|loc| (loc.segid, loc.byte_len)),
                        )
                    })
                    .buffer_unordered(PARALLEL_EXTENT_OPS)
                    .try_collect()
                    .await?;
                old_debits.extend(found.into_iter().flatten());
            } else {
                // Wide candidate sets (bulk hole punches, range deletes) keep
                // the single range scan: one iterator beats thousands of gets
                // over a dense key range.
                let edited: HashSet<u64> = edited.into_iter().collect();
                old_debits.reserve(edited.len());
                let start_key = self.key_codec.extent_key(id, min);
                let end_key = self.key_codec.extent_key(id, scan_end);
                let mut stream = self
                    .db
                    .scan(start_key..end_key)
                    .await
                    .map_err(|_| FsError::IoError)?;
                while let Some(result) = stream.next().await {
                    let (key, value) = result.map_err(|_| FsError::IoError)?;
                    if let Some(extent_idx) = self.key_codec.parse_extent_key(&key)
                        && edited.contains(&extent_idx)
                        && let Some(loc) = FrameLoc::decode(&value)
                    {
                        old_debits.push((loc.segid, loc.byte_len));
                    }
                }
            }
        }
        #[cfg(test)]
        StagePhaseNanos::add(&phase.old_debit, t_old_debit);
        #[cfg(test)]
        let t_compress = std::time::Instant::now();
        // Compress before taking the open-segment lock: compression is the
        // expensive half of the codec and depends only on the plaintext, while
        // the AEAD binds (segid, frame_index), assigned under the lock. Large
        // batches fan out on rayon; block_in_place needs the multi-thread
        // runtime (tests run current-thread), and small batches stay inline.
        let payloads: Vec<&Bytes> = edits.iter().filter_map(|(_, e)| e.as_ref()).collect();
        let compressed: Vec<Compressed> = if should_parallel_compress(payloads.len())
            && tokio::runtime::Handle::current().runtime_flavor()
                == tokio::runtime::RuntimeFlavor::MultiThread
        {
            tokio::task::block_in_place(|| {
                use rayon::prelude::*;
                payloads
                    .par_iter()
                    .map(|p| self.codec.compress(p))
                    .collect::<Result<_, _>>()
            })
            .map_err(|_| FsError::IoError)?
        } else {
            payloads
                .iter()
                .map(|p| self.codec.compress(p))
                .collect::<Result<_, _>>()
                .map_err(|_| FsError::IoError)?
        };
        #[cfg(test)]
        StagePhaseNanos::add(&phase.compress, t_compress);
        let has_frames = !compressed.is_empty();
        #[cfg(test)]
        if has_frames {
            phase
                .batches
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
        #[cfg(test)]
        let t_protect = std::time::Instant::now();
        if has_frames {
            // Must precede FrameLoc assignment under `open`.
            self.protect_extent_ref(txn).await;
        }
        #[cfg(test)]
        StagePhaseNanos::add(&phase.protect_ref, t_protect);
        #[cfg(test)]
        let t_gate_wait = std::time::Instant::now();
        let append_guard = self.lock_append_lane(id).await;
        let lane = append_guard.lane();
        #[cfg(test)]
        StagePhaseNanos::add(&phase.gate_wait, t_gate_wait);
        #[cfg(test)]
        let t_gate_hold = std::time::Instant::now();
        // Under the gate, claim only what the AAD binds and the buffer layout
        // owes: segment identity, a dense frame-index run, and the byte range.
        // The batch AEAD that fills the claim then runs off the gate, so
        // writers sharing a lane encrypt concurrently. `seal_open` and
        // `spawn_seal` take the same gate plus the lane's exclusive fill
        // barrier before rotating, so no rotation observes an unfilled claim.
        let (reserved, rotate_guard) = if has_frames {
            let fill_guard = lane.fill_barrier.read().await;
            let claim: Vec<(u64, usize)> = edits
                .iter()
                .filter(|(_, edit)| edit.is_some())
                .map(|(extent, _)| *extent)
                .zip(compressed.iter().map(Compressed::sealed_len))
                .collect();
            let (reservation, buffered) = {
                let mut open = lane.open.lock().unwrap();
                let reservation = Reservation::claim(&mut open, id, &claim);
                (reservation, open.buf.len())
            };
            // Whoever's claim first carries the buffer past the threshold owns
            // the rotation, and keeps the gate until it has rotated: discovery
            // and rotation stay atomic, and later writers on this lane are held
            // off exactly as they were before the AEAD moved out of the gate.
            let rotate = (buffered >= self.seal_threshold()).then_some(append_guard);
            (Some((reservation, fill_guard)), rotate)
        } else {
            drop(append_guard);
            (None, None)
        };
        #[cfg(test)]
        StagePhaseNanos::add(&phase.gate_hold, t_gate_hold);
        let mut locs = Vec::new();
        if let Some((reservation, fill_guard)) = reserved {
            #[cfg(test)]
            if let Some(probe) = &self.before_batch_seal {
                probe();
            }
            #[cfg(test)]
            let t_aead = std::time::Instant::now();
            let mut compressed = compressed.into_iter();
            let frames = edits
                .iter()
                .filter_map(|(extent, edit)| {
                    edit.as_ref().map(|_| {
                        (
                            id,
                            *extent,
                            compressed.next().expect("one compressed payload per edit"),
                        )
                    })
                })
                .collect();
            let sealed = crate::segment::seal_compressed_batch(
                &self.codec,
                reservation.segid,
                reservation.first_frame,
                frames,
            );
            #[cfg(feature = "failpoints")]
            let sealed = sealed.and_then(|frames| batch_seal_failpoint().map(|()| frames));
            #[cfg(test)]
            StagePhaseNanos::add(&phase.aead, t_aead);
            // A failed seal abandons the claim: its bytes stay zeroed behind a
            // valid length prefix and its directory entries name extents whose
            // committed FrameLocs point elsewhere (this transaction never
            // commits), so both compaction and reclaim resolve past them by key.
            let sealed = sealed.map_err(|e| {
                error!(
                    "batch AEAD failed for {:?}: {e}; {} reserved frames left unfilled \
                     and unreferenced",
                    reservation.segid,
                    reservation.offsets.len()
                );
                FsError::IoError
            })?;
            #[cfg(test)]
            let t_open_lock = std::time::Instant::now();
            let mut open = lane.open.lock().unwrap();
            #[cfg(test)]
            StagePhaseNanos::add(&phase.open_lock_wait, t_open_lock);
            #[cfg(test)]
            let t_append = std::time::Instant::now();
            locs = reservation.fill(&mut open, &sealed)?;
            #[cfg(test)]
            StagePhaseNanos::add(&phase.append, t_append);
            drop(open);
            drop(fill_guard);
        }
        #[cfg(test)]
        let t_txn_stage = std::time::Instant::now();
        let mut locs = locs.into_iter();
        for (extent, edit) in edits {
            match edit {
                Some(data) => {
                    let loc = locs.next().expect("one frame location per edit");
                    txn.put_bytes(
                        &self.key_codec.extent_key(id, *extent),
                        Bytes::copy_from_slice(&loc.encode()),
                    );
                    txn.update_cached_extent_location(id, *extent, Some(loc));
                    // The immutable FrameLoc is the cache identity. Publishing
                    // plaintext before metadata commit is safe: a failed
                    // transaction leaves this entry unreachable, while a
                    // successful one becomes read-ready without a decode pass.
                    self.decoded_insert(id, *extent, loc, data.clone());
                    // Credit the frame just appended: both live and total.
                    self.seg_delta(txn, loc.segid, loc.byte_len as i64, loc.byte_len as i64);
                }
                None => self.delete(txn, id, *extent),
            }
        }
        debug_assert!(locs.next().is_none());
        for (segid, byte_len) in old_debits {
            // Overwrite debit of the superseded frame: live only, total untouched.
            self.seg_delta(txn, segid, -(byte_len as i64), 0);
        }
        #[cfg(test)]
        StagePhaseNanos::add(&phase.txn_stage, t_txn_stage);
        if let Some(append_guard) = rotate_guard {
            #[cfg(test)]
            let t_spawn_seal = std::time::Instant::now();
            self.spawn_seal(&append_guard).await;
            #[cfg(test)]
            StagePhaseNanos::add(&phase.spawn_seal, t_spawn_seal);
        }
        Ok(())
    }

    fn pending_seals(&self) -> Vec<(Segid, Bytes)> {
        let sealing = self.sealing.lock().unwrap();
        sealing
            .iter()
            .map(|(segid, generation)| (*segid, generation.bytes.clone()))
            .collect()
    }

    async fn publish_pending_seals(&self) -> bool {
        let mut results: Vec<_> = stream::iter(self.pending_seals())
            .map(|(segid, bytes)| {
                let segments = Arc::clone(&self.segments);
                async move { (segid, segments.put_segment(segid, bytes).await) }
            })
            .buffer_unordered(self.max_inflight_seals)
            .collect()
            .await;
        results.sort_by_key(|(segid, _)| *segid);

        let mut failed = false;
        for (segid, result) in results {
            match result {
                Ok(()) => {
                    self.sealing.lock().unwrap().remove(&segid);
                }
                Err(e) => {
                    failed = true;
                    error!("seal PUT failed for {:?}: {}; retained for retry", segid, e);
                }
            }
        }
        failed
    }

    /// Finalize a lane's open generation into `sealing` and start a fresh one.
    ///
    /// `freeze` is what makes this sound, and it names the lane rotated: it
    /// proves every reservation on that lane has copied its sealed bytes in, so
    /// the buffer is a complete frame stream rather than one with holes, and
    /// (because it is built from the lane's append guard) that no further
    /// reservation can be placed against the generation being sealed.
    fn rotate_sealing_generation(
        &self,
        freeze: &LaneFreeze<'_>,
        residency: tokio::sync::OwnedSemaphorePermit,
    ) -> Result<Option<(Segid, Bytes)>, FsError> {
        let mut open = freeze.lane.open.lock().unwrap();
        if open.dir.is_empty() {
            return Ok(None);
        }
        let segid = open.segid;
        let sealed_dir = crate::segment::seal_directory(&self.codec, segid, &open.dir)
            .map_err(|_| FsError::IoError)?;
        let k = open.dir.len() as u32;
        let buf = std::mem::replace(&mut open.buf, Vec::with_capacity(self.seal_threshold()));
        open.dir.clear();
        open.segid = self.segments.next_segid();
        debug_assert_ne!(
            open.segid, segid,
            "rotated open segid must differ from the sealed one"
        );
        let bytes = Bytes::from(crate::segment::assemble_segment(
            segid,
            buf,
            k,
            &sealed_dir,
            segid.counter,
        ));
        let replaced = self.sealing.lock().unwrap().insert(
            segid,
            SealingGeneration {
                bytes: bytes.clone(),
                _residency: residency,
            },
        );
        debug_assert!(replaced.is_none(), "sealing generation ids must be unique");
        Ok(Some((segid, bytes)))
    }

    /// The durability barrier (called by the flush path before the manifest is
    /// flushed): wait for every in-flight background seal, re-PUT any that failed,
    /// then synchronously seal the current open buffer. After this returns, every
    /// segment referenced by a committed extent is durable on the object store.
    pub async fn seal_open(&self) -> Result<(), FsError> {
        // Active-upload capacity is independent from resident-buffer capacity.
        // Drain uploads first so a failed resident generation cannot deadlock a
        // writer holding an append gate while it waits for memory budget.
        let _all_uploads = self
            .seal_upload_sem
            .acquire_many(self.max_inflight_seals as u32)
            .await
            .map_err(|_| FsError::IoError)?;

        // Freeze the append lanes one at a time. Re-publishing before each wait
        // releases residency for a writer already blocked inside that lane, then
        // the semaphore's FIFO lock hands the gate to this barrier next.
        let mut append_guards = Vec::with_capacity(OPEN_SEGMENT_LANES);
        for lane in self.open_lanes.iter() {
            if self.publish_pending_seals().await {
                return Err(FsError::IoError);
            }
            append_guards.push(LaneAppendGuard::lock(lane).await);
        }
        if self.publish_pending_seals().await {
            return Err(FsError::IoError);
        }

        let dirty_lanes: Vec<_> = self
            .open_lanes
            .iter()
            .enumerate()
            .filter_map(|(index, lane)| {
                (!lane.open.lock().unwrap().dir.is_empty()).then_some(index)
            })
            .collect();
        if dirty_lanes.is_empty() {
            return Ok(());
        }

        let mut append_guards = Some(append_guards);
        let mut uploads = FuturesUnordered::new();
        let mut next_lane = 0;
        let mut failed = false;
        #[cfg(test)]
        let mut put_gate = self.seal_open_put_gate.clone();

        loop {
            while next_lane < dirty_lanes.len() {
                let residency = match Arc::clone(&self.seal_residency_sem).try_acquire_owned() {
                    Ok(permit) => permit,
                    Err(tokio::sync::TryAcquireError::NoPermits) => break,
                    Err(tokio::sync::TryAcquireError::Closed) => return Err(FsError::IoError),
                };
                let index = dirty_lanes[next_lane];
                next_lane += 1;
                #[cfg(feature = "failpoints")]
                fail_point!(fp::SEAL_OPEN_FAIL, |_| Err(FsError::IoError));
                // `append_guards` is built over `open_lanes` in order, so index
                // `index` is this lane's own gate; the freeze then rotates the
                // lane that gate names rather than one addressed separately.
                let freeze = LaneFreeze::acquire(
                    &append_guards
                        .as_ref()
                        .expect("append gates are held until every lane has rotated")[index],
                )
                .await;
                match self.rotate_sealing_generation(&freeze, residency) {
                    Ok(Some((segid, bytes))) => {
                        let segments = Arc::clone(&self.segments);
                        uploads
                            .push(async move { (segid, segments.put_segment(segid, bytes).await) });
                    }
                    Ok(None) => {}
                    Err(e) => {
                        failed = true;
                        error!("failed to seal open segment directory: {}", e);
                    }
                }
            }

            if next_lane == dirty_lanes.len() {
                // Every generation in the flush cutoff is immutable and resident.
                // Later writers may use the replacement buffers while only this
                // captured set is published below.
                drop(append_guards.take());
            }

            #[cfg(test)]
            if !uploads.is_empty()
                && let Some(gate) = put_gate.take()
            {
                let _permit = gate.acquire().await.map_err(|_| FsError::IoError)?;
            }

            let Some((segid, result)) = uploads.next().await else {
                break;
            };
            match result {
                Ok(()) => {
                    self.sealing.lock().unwrap().remove(&segid);
                }
                Err(e) => {
                    failed = true;
                    error!("seal PUT failed for {:?}: {}; retained for retry", segid, e);
                }
            }
        }

        if next_lane < dirty_lanes.len() {
            failed = true;
            error!(
                "seal residency budget exhausted by failed generations; remaining lanes retained"
            );
        }
        if failed {
            return Err(FsError::IoError);
        }
        Ok(())
    }

    /// Rotate the open buffer and PUT it in the background (the size-threshold
    /// path). Acquires a permit first, so a writer that outruns the object store
    /// blocks here (backpressure) instead of growing RAM without bound. The
    /// rotated buffer stays readable via `sealing` until its PUT lands.
    ///
    /// `appended` carries both the lane to rotate and its held gate, which keeps
    /// later writers from reserving into the generation being rotated — both
    /// while this waits for a residency permit and while it drains reservations
    /// already in flight.
    async fn spawn_seal(&self, appended: &LaneAppendGuard<'_>) {
        let residency = match Arc::clone(&self.seal_residency_sem).acquire_owned().await {
            Ok(p) => p,
            Err(_) => return,
        };
        let freeze = LaneFreeze::acquire(appended).await;
        let (segid, _) = match self.rotate_sealing_generation(&freeze, residency) {
            Ok(Some(generation)) => generation,
            Ok(None) => return,
            Err(e) => {
                error!("failed to seal open segment directory: {}", e);
                return;
            }
        };
        drop(freeze);
        let segments = self.segments.clone();
        let sealing = self.sealing.clone();
        let upload_sem = self.seal_upload_sem.clone();
        crate::task::spawn_named("segment-seal", async move {
            let _upload = match upload_sem.acquire_owned().await {
                Ok(permit) => permit,
                Err(_) => return,
            };
            // A flush may have published this generation while the background
            // task waited for upload capacity. Skip the stale queued attempt.
            let bytes = {
                let sealing = sealing.lock().unwrap();
                sealing
                    .get(&segid)
                    .map(|generation| generation.bytes.clone())
            };
            let Some(bytes) = bytes else {
                return;
            };
            match segments.put_segment(segid, bytes).await {
                Ok(()) => {
                    sealing.lock().unwrap().remove(&segid);
                }
                Err(e) => {
                    error!(
                        "background seal PUT failed for {:?}: {}; retried on flush",
                        segid, e
                    );
                }
            }
        });
    }

    /// Stage a write at `offset` as read-modify-write over full extents;
    /// all-zero extents become holes. Commits nothing itself: the returned
    /// [`TailUpdate`] must be applied only after the txn commits.
    pub async fn write(
        &self,
        txn: &mut Transaction,
        id: InodeId,
        offset: u64,
        data: &Bytes,
        old_size: u64,
    ) -> Result<TailUpdate, FsError> {
        if data.is_empty() {
            return Ok(TailUpdate::Keep);
        }
        let end_offset = offset
            .checked_add(data.len() as u64)
            .ok_or(FsError::InvalidArgument)?;
        let start_extent = offset / EXTENT_SIZE as u64;
        let end_extent = (end_offset - 1) / EXTENT_SIZE as u64;

        // Everything below reads apply-published state -- the tail cache, the
        // partially-overwritten extents, and each edited extent's current
        // FrameLoc -- so it must not run while a queued write owns any of
        // these extents. Disjoint ranges are unaffected.
        self.inflight_writes
            .wait_for_overlap(id, start_extent, end_extent)
            .await;

        let cached = self.tail_get(id);

        // Read the existing content of any partially-overwritten extent (full
        // overwrites and extents past EOF need no read).
        let existing_extents: HashMap<u64, Bytes> = stream::iter(start_extent..=end_extent)
            .map(|extent_idx| {
                let extent_start = extent_idx * EXTENT_SIZE as u64;
                let extent_end = extent_start + EXTENT_SIZE as u64;
                let will_overwrite_fully = offset <= extent_start && end_offset >= extent_end;
                let beyond_eof = extent_start >= old_size;
                let store = self.clone();
                let cached = cached.clone();
                async move {
                    let data = if will_overwrite_fully || beyond_eof {
                        Bytes::from_static(ZERO_EXTENT)
                    } else if let Some((_, bytes)) = cached.filter(|(ci, _)| *ci == extent_idx) {
                        bytes
                    } else {
                        store
                            .get(id, extent_idx)
                            .await?
                            .unwrap_or_else(|| Bytes::from_static(ZERO_EXTENT))
                    };
                    Ok::<(u64, Bytes), FsError>((extent_idx, data))
                }
            })
            .buffer_unordered(PARALLEL_EXTENT_OPS)
            .try_collect()
            .await?;

        let cache_tail = end_offset >= old_size && !end_offset.is_multiple_of(EXTENT_SIZE as u64);

        let mut data_offset = 0usize;
        let mut edits: Vec<(u64, Option<Bytes>)> =
            Vec::with_capacity((end_extent - start_extent + 1) as usize);
        let mut tail: Option<Bytes> = None;
        for extent_idx in start_extent..=end_extent {
            let extent_start = extent_idx * EXTENT_SIZE as u64;
            let extent_end = extent_start + EXTENT_SIZE as u64;
            let write_start = if offset > extent_start {
                (offset - extent_start) as usize
            } else {
                0
            };
            let write_end = if end_offset < extent_end {
                (end_offset - extent_start) as usize
            } else {
                EXTENT_SIZE
            };
            let write_len = write_end - write_start;
            let extent: Bytes = if write_start == 0 && write_end == EXTENT_SIZE {
                data.slice(data_offset..data_offset + write_len)
            } else {
                let mut buf = BytesMut::from(existing_extents[&extent_idx].as_ref());
                buf[write_start..write_end]
                    .copy_from_slice(&data[data_offset..data_offset + write_len]);
                buf.freeze()
            };
            data_offset += write_len;

            if extent.as_ref() == ZERO_EXTENT {
                edits.push((extent_idx, None));
            } else {
                if extent_idx == end_extent && cache_tail {
                    tail = Some(extent.clone());
                }
                edits.push((extent_idx, Some(extent)));
            }
        }

        self.stage_edits(txn, id, &edits, old_size.div_ceil(EXTENT_SIZE as u64))
            .await?;

        Ok(match tail {
            Some(data) => TailUpdate::Set {
                extent_idx: end_extent,
                data,
            },
            None => TailUpdate::Clear,
        })
    }

    /// Stage a shrink to `new_size` (growth is a no-op: extension is sparse):
    /// drops extents past the end and zero-fills the partial last one.
    pub async fn truncate(
        &self,
        txn: &mut Transaction,
        id: InodeId,
        old_size: u64,
        new_size: u64,
    ) -> Result<(), FsError> {
        if new_size >= old_size {
            return Ok(());
        }

        let old_extents = old_size.div_ceil(EXTENT_SIZE as u64);
        let new_extents = new_size.div_ceil(EXTENT_SIZE as u64);
        self.delete_range(txn, id, new_extents, old_extents).await?;

        if new_size > 0 {
            let last_extent_idx = new_extents - 1;
            let clear_from = (new_size % EXTENT_SIZE as u64) as usize;
            if clear_from > 0 {
                let existing = self.get(id, last_extent_idx).await?;
                let mut extent =
                    BytesMut::from(existing.as_ref().map(|b| b.as_ref()).unwrap_or(ZERO_EXTENT));
                extent[clear_from..].fill(0);
                let edit = if extent.as_ref() == ZERO_EXTENT {
                    (last_extent_idx, None)
                } else {
                    (last_extent_idx, Some(extent.freeze()))
                };
                self.stage_edits(txn, id, &[edit], old_size.div_ceil(EXTENT_SIZE as u64))
                    .await?;
            }
        }
        Ok(())
    }

    /// Stage zeroes over `[offset, offset + length)` capped at `file_size`:
    /// fully-covered extents become holes, partial ones are RMW-zeroed.
    pub async fn zero_range(
        &self,
        txn: &mut Transaction,
        id: InodeId,
        offset: u64,
        length: u64,
        file_size: u64,
    ) -> Result<(), FsError> {
        if length == 0 {
            return Ok(());
        }
        let end_offset = offset.checked_add(length).ok_or(FsError::InvalidArgument)?;
        // Zeroing reads the extents it only partially covers and debits the
        // frames it supersedes; both must see every queued write on the inode.
        self.inflight_writes.wait_for_all(id).await;
        self.tail_invalidate(id);

        let start_extent = offset / EXTENT_SIZE as u64;
        let end_extent = (end_offset - 1) / EXTENT_SIZE as u64;

        let mut edits: Vec<(u64, Option<Bytes>)> = Vec::new();
        for extent_idx in start_extent..=end_extent {
            let extent_start = extent_idx * EXTENT_SIZE as u64;
            let extent_end = extent_start + EXTENT_SIZE as u64;
            if extent_start >= file_size {
                continue;
            }
            if offset <= extent_start && end_offset >= extent_end {
                edits.push((extent_idx, None));
            } else if let Some(existing_data) = self.get(id, extent_idx).await? {
                let zero_start = if offset > extent_start {
                    (offset - extent_start) as usize
                } else {
                    0
                };
                let zero_end = if end_offset < extent_end {
                    (end_offset - extent_start) as usize
                } else {
                    EXTENT_SIZE
                };
                let mut extent_data = BytesMut::from(existing_data.as_ref());
                extent_data[zero_start..zero_end].fill(0);
                if extent_data.as_ref() == ZERO_EXTENT {
                    edits.push((extent_idx, None));
                } else {
                    edits.push((extent_idx, Some(extent_data.freeze())));
                }
            }
        }
        self.stage_edits(txn, id, &edits, file_size.div_ceil(EXTENT_SIZE as u64))
            .await
    }

    /// Delete an extent range under the inode's write lock, in its own transaction.
    /// The tombstone GC calls this so its deletes serialize with the compaction
    /// repoint, which takes the same lock: otherwise a repoint that read an extent
    /// live, then lost the race to a concurrent tombstone delete, would re-commit
    /// the extent last-writer-wins (the LSM has no CAS), resurrecting a deleted
    /// inode's extent and pinning the repacked segment forever.
    pub async fn delete_extents(
        &self,
        inode: InodeId,
        start_extent: u64,
        total_extents: u64,
    ) -> Result<(), FsError> {
        let _guard = self.lock_manager.acquire(inode).await;
        let mut txn = self.db.new_transaction()?;
        self.delete_range(&mut txn, inode, start_extent, total_extents)
            .await?;
        self.commit_via_coordinator(txn).await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::super::test_util::*;
    use super::*;
    use crate::config::CompressionConfig;
    use crate::fault_store::{FaultControls, FaultStore};
    use crate::replication::ReplOp;
    use slatedb::WriteBatch;
    use slatedb::object_store::ObjectStore;
    use slatedb::object_store::memory::InMemory;
    use tokio::sync::Semaphore;

    fn staged_deletes(mut txn: Transaction) -> (Vec<Bytes>, Vec<(InodeId, u64)>) {
        let _ = txn.take_seg_deltas();
        let cache_deletes = txn
            .take_extent_location_cache_updates()
            .into_iter()
            .map(|(key, location)| {
                assert!(location.is_none(), "an extent delete must cache a hole");
                key
            })
            .collect();
        let delete_keys = txn
            .apply_to_collecting(&mut WriteBatch::new())
            .into_iter()
            .filter_map(|op| match op {
                ReplOp::Delete(key) => Some(key),
                _ => None,
            })
            .collect();
        (delete_keys, cache_deletes)
    }

    async fn four_dirty_lanes(max_inflight_seals: usize) -> (ExtentStore, Arc<FaultControls>) {
        let (_store, db) = make().await;
        let (object_store, controls) = FaultStore::new(Arc::new(InMemory::new()));
        let object_store: Arc<dyn ObjectStore> = object_store;
        let mut store = make_store(object_store, db.clone(), CompressionConfig::Lz4, 7);
        store.max_inflight_seals = max_inflight_seals;
        store.seal_upload_sem = Arc::new(Semaphore::new(max_inflight_seals));
        store.seal_residency_sem = Arc::new(Semaphore::new(max_inflight_seals));
        for lane in 0..OPEN_SEGMENT_LANES as u64 {
            let inode = 100 + lane;
            let mut txn = db.new_transaction().unwrap();
            store
                .write(
                    &mut txn,
                    inode,
                    0,
                    &Bytes::from(incompressible(inode as usize, EXTENT_SIZE)),
                    0,
                )
                .await
                .unwrap();
            commit(&store, txn).await;
        }
        (store, controls)
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn seal_open_publishes_dirty_lanes_concurrently_within_limit() {
        let (store, controls) = four_dirty_lanes(2).await;
        controls.block_puts();

        let seal = tokio::spawn({
            let store = store.clone();
            async move { store.seal_open().await }
        });
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            while controls.max_active_puts() < 2 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("dirty lane PUTs did not fill the configured concurrency limit");

        assert_eq!(
            controls.max_active_puts(),
            2,
            "dirty lane PUTs must overlap without exceeding max_inflight_seals"
        );
        controls.release_puts();
        seal.await.unwrap().unwrap();
        assert!(store.sealing.lock().unwrap().is_empty());
        assert_eq!(store.segments.list_segments().await.unwrap().len(), 4);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn seal_open_refills_capacity_when_a_later_put_finishes_first() {
        let (store, controls) = four_dirty_lanes(2).await;
        controls.block_puts();

        let seal = tokio::spawn({
            let store = store.clone();
            async move { store.seal_open().await }
        });
        let first_pair = tokio::time::timeout(std::time::Duration::from_secs(1), async {
            loop {
                let paths: Vec<_> = controls
                    .put_paths()
                    .into_iter()
                    .filter(|path| path.starts_with("segments/"))
                    .collect();
                if paths.len() >= 2 {
                    break paths;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the first two dirty lane PUTs did not start");

        // Leave the oldest PUT blocked, but complete the second. The newly free
        // slot must immediately admit the third captured lane rather than wait
        // for results to become ready in segment order.
        controls.release_put_path(&first_pair[1]);
        let refilled = tokio::time::timeout(std::time::Duration::from_millis(250), async {
            loop {
                let started = controls
                    .put_paths()
                    .iter()
                    .filter(|path| path.starts_with("segments/"))
                    .count();
                if started >= 3 {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .is_ok();

        controls.release_puts();
        seal.await.unwrap().unwrap();
        assert!(
            refilled,
            "a completed later PUT left seal capacity idle behind the oldest straggler"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn seal_open_retains_only_failed_segments_for_retry() {
        let (store, controls) = four_dirty_lanes(2).await;
        let segment_puts = || {
            controls
                .put_paths()
                .iter()
                .filter(|path| path.starts_with("segments/"))
                .count()
        };
        let puts_before = segment_puts();
        controls.fail_puts(1);

        assert!(store.seal_open().await.is_err());
        assert_eq!(
            segment_puts() - puts_before,
            4,
            "one failed PUT must not cancel independent captured lanes"
        );
        assert_eq!(store.sealing.lock().unwrap().len(), 1);
        assert_eq!(store.segments.list_segments().await.unwrap().len(), 3);

        store.seal_open().await.unwrap();
        assert_eq!(segment_puts() - puts_before, 5);
        assert!(store.sealing.lock().unwrap().is_empty());
        assert_eq!(store.segments.list_segments().await.unwrap().len(), 4);
    }

    #[tokio::test]
    async fn segcount_tracks_live_bytes_across_overwrite_and_delete() {
        let (store, db) = make().await;
        let inode: InodeId = 1;

        // Three full extents land in the open segment; the counter equals their bytes.
        let mut txn = db.new_transaction().unwrap();
        store
            .write(
                &mut txn,
                inode,
                0,
                &Bytes::from(vec![1u8; 3 * EXTENT_SIZE]),
                0,
            )
            .await
            .unwrap();
        commit(&store, txn).await;
        let seg = frameloc_of(&store, &db, inode, 0).await.unwrap().segid;
        assert_eq!(
            segcount_of(&store, &db, seg).await,
            live_bytes(&store, &db, inode, 0..3, seg).await,
        );

        // Overwrite extent 1: old debited, new credited, the stale frame is dead
        // weight the counter does not count.
        let mut txn = db.new_transaction().unwrap();
        store
            .write(
                &mut txn,
                inode,
                EXTENT_SIZE as u64,
                &Bytes::from(vec![2u8; EXTENT_SIZE]),
                3 * EXTENT_SIZE as u64,
            )
            .await
            .unwrap();
        commit(&store, txn).await;
        assert_eq!(frameloc_of(&store, &db, inode, 1).await.unwrap().segid, seg);
        assert_eq!(
            segcount_of(&store, &db, seg).await,
            live_bytes(&store, &db, inode, 0..3, seg).await,
        );

        // Delete extent 2: its bytes leave the counter.
        let mut txn = db.new_transaction().unwrap();
        store.delete_range(&mut txn, inode, 2, 3).await.unwrap();
        commit(&store, txn).await;
        assert_eq!(
            segcount_of(&store, &db, seg).await,
            live_bytes(&store, &db, inode, 0..2, seg).await,
        );

        // Delete the rest: the counter reaches exactly zero.
        let mut txn = db.new_transaction().unwrap();
        store.delete_range(&mut txn, inode, 0, 2).await.unwrap();
        commit(&store, txn).await;
        assert_eq!(segcount_of(&store, &db, seg).await, 0);
    }

    #[tokio::test]
    async fn empty_one_tib_delete_range_stages_no_operations() {
        let (store, db) = make().await;
        let mut txn = db.new_transaction().unwrap();
        let one_tib_extents = (1_u64 << 40) / EXTENT_SIZE as u64;

        store
            .delete_range(&mut txn, 1, 0, one_tib_extents)
            .await
            .unwrap();

        assert!(
            txn.is_empty(),
            "an empty sparse range must not stage logical-hole tombstones"
        );
        assert!(txn.take_seg_deltas().is_empty());
    }

    #[tokio::test]
    async fn sparse_delete_range_stages_only_its_two_allocated_extents() {
        let (store, db) = make().await;
        let inode: InodeId = 17;
        let allocated = [2_u64, 7];
        let mut old_size = 0;
        for extent in allocated {
            let mut txn = db.new_transaction().unwrap();
            store
                .write(
                    &mut txn,
                    inode,
                    extent * EXTENT_SIZE as u64,
                    &Bytes::from_static(b"x"),
                    old_size,
                )
                .await
                .unwrap();
            commit(&store, txn).await;
            old_size = extent * EXTENT_SIZE as u64 + 1;
        }

        let mut txn = db.new_transaction().unwrap();
        store.delete_range(&mut txn, inode, 0, 8).await.unwrap();

        let (delete_keys, cache_deletes) = staged_deletes(txn);
        assert_eq!(
            delete_keys,
            allocated.map(|extent| store.key_codec.extent_key(inode, extent)),
        );
        assert_eq!(cache_deletes, allocated.map(|extent| (inode, extent)));
    }

    #[tokio::test]
    async fn sparse_truncate_preserves_prefix_and_zeroes_removed_data() {
        let (store, db) = make().await;
        let inode: InodeId = 23;
        let sparse_extent = 7_u64;
        let old_size = sparse_extent * EXTENT_SIZE as u64 + 1;

        let mut txn = db.new_transaction().unwrap();
        store
            .write(&mut txn, inode, 0, &Bytes::from(vec![0x5a; EXTENT_SIZE]), 0)
            .await
            .unwrap();
        commit(&store, txn).await;

        let mut txn = db.new_transaction().unwrap();
        store
            .write(
                &mut txn,
                inode,
                sparse_extent * EXTENT_SIZE as u64,
                &Bytes::from_static(b"x"),
                EXTENT_SIZE as u64,
            )
            .await
            .unwrap();
        commit(&store, txn).await;

        let mut txn = db.new_transaction().unwrap();
        store
            .truncate(&mut txn, inode, old_size, 100)
            .await
            .unwrap();
        commit(&store, txn).await;

        let got = store.read(inode, 0, old_size).await.unwrap();
        assert_eq!(&got[..100], &[0x5a; 100]);
        assert!(got[100..].iter().all(|byte| *byte == 0));
        assert!(
            db.get_bytes(&store.key_codec.extent_key(inode, sparse_extent))
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn sparse_truncate_invalidates_a_warm_extent_location() {
        let (store, db) = make().await;
        let inode: InodeId = 29;
        let sparse_extent = 7_u64;
        let old_size = sparse_extent * EXTENT_SIZE as u64 + 1;

        let mut txn = db.new_transaction().unwrap();
        store
            .write(
                &mut txn,
                inode,
                sparse_extent * EXTENT_SIZE as u64,
                &Bytes::from_static(b"x"),
                0,
            )
            .await
            .unwrap();
        commit(&store, txn).await;

        let location = frameloc_of(&store, &db, inode, sparse_extent)
            .await
            .unwrap();
        assert!(store.get(inode, sparse_extent).await.unwrap().is_some());
        assert_eq!(
            store.cached_extent_location(inode, sparse_extent),
            Some(location),
            "the regression requires a warm logical-to-physical cache entry"
        );
        let (live_before, total_before) = segcount_pair_of(&store, &db, location.segid).await;
        assert_eq!(live_before, total_before);
        assert!(live_before > 0);

        let mut txn = db.new_transaction().unwrap();
        store.truncate(&mut txn, inode, old_size, 0).await.unwrap();
        commit(&store, txn).await;

        assert_eq!(
            segcount_pair_of(&store, &db, location.segid).await,
            (0, total_before),
            "truncate must debit the deleted frame's live bytes without reducing total"
        );
        assert!(
            store.get(inode, sparse_extent).await.unwrap().is_none(),
            "a deleted sparse extent must not survive through its cached FrameLoc"
        );
    }

    // The debit scan must find and debit *every* prior frame of a multi-extent
    // overwrite, not just the range endpoints: a missed debit over-counts live
    // bytes (a space leak), a double debit under-counts (GC could drop a live
    // segment). Fresh-write and single-extent-overwrite tests never make the
    // scan return more than one key, so cover the multi-key case explicitly.
    #[tokio::test]
    async fn segcount_debits_every_extent_of_a_multi_extent_overwrite() {
        let (store, db) = make().await;
        let inode: InodeId = 1;

        // Four full extents into the open segment.
        let mut txn = db.new_transaction().unwrap();
        store
            .write(
                &mut txn,
                inode,
                0,
                &Bytes::from(vec![1u8; 4 * EXTENT_SIZE]),
                0,
            )
            .await
            .unwrap();
        commit(&store, txn).await;
        let seg = frameloc_of(&store, &db, inode, 0).await.unwrap().segid;
        assert_eq!(
            segcount_of(&store, &db, seg).await,
            live_bytes(&store, &db, inode, 0..4, seg).await,
        );

        // Overwrite all four in one write: the scan returns four keys, and each
        // superseded frame must be debited. The counter then equals only the
        // four live (current) frames; any missed or extra debit breaks equality.
        let mut txn = db.new_transaction().unwrap();
        store
            .write(
                &mut txn,
                inode,
                0,
                &Bytes::from(vec![2u8; 4 * EXTENT_SIZE]),
                4 * EXTENT_SIZE as u64,
            )
            .await
            .unwrap();
        commit(&store, txn).await;
        assert_eq!(
            segcount_of(&store, &db, seg).await,
            live_bytes(&store, &db, inode, 0..4, seg).await,
        );
    }

    // `total` is the cumulative-appended-bytes denominator: credited with every frame,
    // never debited. So a delete drops `live` but leaves `total` put, and `total - live`
    // is the segment's dead (reclaimable) bytes.
    #[tokio::test]
    async fn segcount_total_is_monotonic_never_debited() {
        let (store, db) = make().await;
        let inode: InodeId = 1;

        // Two extents into the open segment: fresh, so every appended byte is live.
        let mut txn = db.new_transaction().unwrap();
        store
            .write(
                &mut txn,
                inode,
                0,
                &Bytes::from(vec![1u8; 2 * EXTENT_SIZE]),
                0,
            )
            .await
            .unwrap();
        commit(&store, txn).await;
        let seg = frameloc_of(&store, &db, inode, 0).await.unwrap().segid;
        let ext1_len = frameloc_of(&store, &db, inode, 1).await.unwrap().byte_len as u64;
        let (live0, total0) = segcount_pair_of(&store, &db, seg).await;
        assert_eq!(live0, total0, "fresh segment: every appended byte is live");

        // Delete extent 1: live loses its bytes; total is never debited.
        let mut txn = db.new_transaction().unwrap();
        store.delete_range(&mut txn, inode, 1, 2).await.unwrap();
        commit(&store, txn).await;
        let (live1, total1) = segcount_pair_of(&store, &db, seg).await;
        assert_eq!(
            total1, total0,
            "total is monotonic: a delete never debits it"
        );
        assert_eq!(live1, live0 - ext1_len, "live drops by the deleted frame");
        assert_eq!(
            total1,
            live1 + ext1_len,
            "total - live is the dead-frame bytes (the fragmentation denominator)"
        );
    }

    #[tokio::test]
    async fn single_and_multi_extent_write_read() {
        let (store, db) = make().await;
        let mut model = Vec::new();
        write_and_check(&store, &db, &mut model, 0, b"hello").await;
        write_and_check(
            &store,
            &db,
            &mut model,
            3,
            &vec![7u8; EXTENT_SIZE * 2 + 100],
        )
        .await;
        write_and_check(&store, &db, &mut model, EXTENT_SIZE - 5, &[9u8; 20]).await;
    }

    // A foreground multi-frame write must not hold the globally shared open
    // buffer mutex while it performs per-frame AEAD. Moving or mis-indexing a
    // batch must also fail here: the independently opened run binds each frame
    // to its exact (segment, index, inode, extent) AAD.
    #[tokio::test]
    async fn multi_frame_stage_edits_seals_outside_open_lock_without_reordering_frames() {
        let (mut store, db) = make().await;
        let inode: InodeId = 73;
        let seed_inode: InodeId = 69;
        let seed = Bytes::from(vec![0x77; EXTENT_SIZE]);
        let mut seed_txn = db.new_transaction().unwrap();
        store
            .stage_edits(&mut seed_txn, seed_inode, &[(2, Some(seed.clone()))], 0)
            .await
            .unwrap();
        commit(&store, seed_txn).await;

        let observed_unlocked = Arc::new(std::sync::atomic::AtomicBool::new(false));
        store.before_batch_seal = Some({
            let lanes = Arc::clone(&store.open_lanes);
            let lane = (inode as usize) & (OPEN_SEGMENT_LANES - 1);
            let observed_unlocked = Arc::clone(&observed_unlocked);
            Arc::new(move || {
                observed_unlocked.store(
                    lanes[lane].open.try_lock().is_ok(),
                    std::sync::atomic::Ordering::SeqCst,
                );
            })
        });

        let plains = [
            Bytes::from(vec![0x11; EXTENT_SIZE]),
            Bytes::from(vec![0x22; EXTENT_SIZE]),
            Bytes::from(vec![0x33; EXTENT_SIZE]),
        ];
        let edits = vec![
            (9, Some(plains[0].clone())),
            (4, Some(plains[1].clone())),
            (12, Some(plains[2].clone())),
        ];
        let mut txn = db.new_transaction().unwrap();
        store.stage_edits(&mut txn, inode, &edits, 0).await.unwrap();

        assert!(
            observed_unlocked.load(std::sync::atomic::Ordering::SeqCst),
            "foreground batch AEAD held the global open-segment mutex"
        );

        let (segid, region, dir) = {
            let open = store.open_lane(inode).open.lock().unwrap();
            (open.segid, open.buf.clone(), open.dir.clone())
        };
        assert_eq!(
            dir.iter()
                .map(|entry| (entry.inode, entry.extent))
                .collect::<Vec<_>>(),
            vec![(seed_inode, 2), (inode, 9), (inode, 4), (inode, 12)]
        );
        let decoded = crate::segment::read_frames_from_region(
            &store.codec,
            &region,
            segid,
            0,
            &[(seed_inode, 2), (inode, 9), (inode, 4), (inode, 12)],
        )
        .unwrap();
        assert_eq!(
            decoded,
            std::iter::once(seed.to_vec())
                .chain(plains.clone().map(|plain| plain.to_vec()))
                .collect::<Vec<_>>()
        );

        commit(&store, txn).await;
        for ((extent, _), plain) in edits.iter().zip(plains) {
            assert_eq!(
                store
                    .read(inode, extent * EXTENT_SIZE as u64, EXTENT_SIZE as u64)
                    .await
                    .unwrap(),
                plain
            );
        }
    }

    // Removing the global append bottleneck must let independent files perform
    // their batch AEAD concurrently. A single shared append gate makes these
    // probes run one after another, keeping `max_active` at one.
    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn distinct_inode_batches_enter_aead_concurrently() {
        let (mut store, db) = make().await;
        let active = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let max_active = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        store.before_batch_seal = Some({
            let active = Arc::clone(&active);
            let max_active = Arc::clone(&max_active);
            Arc::new(move || {
                let now = active.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1;
                max_active.fetch_max(now, std::sync::atomic::Ordering::SeqCst);
                std::thread::sleep(std::time::Duration::from_millis(100));
                active.fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
            })
        });

        let writes: Vec<_> = (0..4u64)
            .map(|lane| {
                let store = store.clone();
                let db = db.clone();
                tokio::spawn(async move {
                    let inode = 100 + lane;
                    let mut txn = db.new_transaction().unwrap();
                    store
                        .stage_edits(
                            &mut txn,
                            inode,
                            &[(
                                0,
                                Some(Bytes::from(incompressible(inode as usize, EXTENT_SIZE))),
                            )],
                            0,
                        )
                        .await
                        .unwrap();
                    commit(&store, txn).await;
                })
            })
            .collect();
        for write in writes {
            write.await.unwrap();
        }

        assert_eq!(
            max_active.load(std::sync::atomic::Ordering::SeqCst),
            4,
            "independent inode batches did not seal concurrently"
        );
    }

    #[tokio::test]
    async fn configured_seal_threshold_preallocates_the_open_buffer() {
        let (store, db) = make().await;
        let store = store.with_seal_threshold(1024);
        {
            for lane in store.open_lanes.iter() {
                let open = lane.open.lock().unwrap();
                assert!(open.buf.capacity() >= store.seal_threshold());
            }
        }

        let mut txn = db.new_transaction().unwrap();
        store
            .write(
                &mut txn,
                1,
                0,
                &Bytes::from(incompressible(91, EXTENT_SIZE)),
                0,
            )
            .await
            .unwrap();
        {
            let open = store.open_lane(1).open.lock().unwrap();
            assert!(open.dir.is_empty(), "threshold write did not rotate");
            assert!(
                open.buf.capacity() >= store.seal_threshold(),
                "rotated open buffer capacity {} is below configured seal threshold {}",
                open.buf.capacity(),
                store.seal_threshold()
            );
        }
        store.seal_open().await.unwrap();
    }

    #[tokio::test]
    async fn sparse_hole_reads_zero() {
        let (store, db) = make().await;
        let mut model = Vec::new();
        // Write at a high offset; the gap before it is a hole.
        write_and_check(&store, &db, &mut model, 5 * EXTENT_SIZE, b"tail").await;
        // Read across the hole.
        let got = store.read(1, 0, 100).await.unwrap();
        assert_eq!(got.as_ref(), &vec![0u8; 100][..]);
    }

    #[tokio::test]
    async fn all_zero_write_makes_hole() {
        let (store, db) = make().await;
        let mut model = Vec::new();
        write_and_check(&store, &db, &mut model, 0, &vec![1u8; EXTENT_SIZE + 10]).await;
        // Overwrite the first extent with zeros -> becomes a hole (key deleted).
        write_and_check(&store, &db, &mut model, 0, &vec![0u8; EXTENT_SIZE]).await;
        // The extent key for extent 0 must be gone.
        let key = store.key_codec.extent_key(1, 0);
        assert!(db.get_bytes(&key).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn truncate_shrink_then_regrow_reads_zero() {
        let (store, db) = make().await;
        let mut model = Vec::new();
        write_and_check(&store, &db, &mut model, 0, &vec![5u8; 3 * EXTENT_SIZE]).await;

        let mut txn = db.new_transaction().unwrap();
        store
            .truncate(&mut txn, 1, model.len() as u64, 100)
            .await
            .unwrap();
        commit(&store, txn).await;
        model.truncate(100);
        assert_read_matches(&store, &model).await;

        // Regrow via a write past EOF; the gap must read as zeros.
        write_and_check(&store, &db, &mut model, 2 * EXTENT_SIZE, b"z").await;
    }

    #[tokio::test]
    async fn zero_range_punches_hole() {
        let (store, db) = make().await;
        let mut model = Vec::new();
        write_and_check(&store, &db, &mut model, 0, &vec![8u8; 2 * EXTENT_SIZE + 50]).await;

        let mut txn = db.new_transaction().unwrap();
        store
            .zero_range(&mut txn, 1, 100, EXTENT_SIZE as u64, model.len() as u64)
            .await
            .unwrap();
        commit(&store, txn).await;
        let end = (100 + EXTENT_SIZE).min(model.len());
        model[100..end].fill(0);
        assert_read_matches(&store, &model).await;
    }

    #[tokio::test]
    async fn writes_buffer_until_seal_with_read_your_writes() {
        let (store, db) = make().await;
        let mut model = Vec::new();
        // A write commits its extent but issues no PUT and serves reads from RAM.
        write_and_check(&store, &db, &mut model, 0, &[7u8; 100]).await;
        assert_eq!(
            store.segments.list_segments().await.unwrap().len(),
            0,
            "a write must not PUT a segment"
        );
        assert_eq!(
            store.segments.read_calls(),
            0,
            "the read was served from the open buffer"
        );

        // Sealing PUTs exactly one segment. The validated plaintext written
        // through before commit remains read-ready after the open buffer rotates,
        // so this reread must not issue a segment GET.
        store.seal_open().await.unwrap();
        assert_eq!(store.segments.list_segments().await.unwrap().len(), 1);
        assert_eq!(store.read(1, 0, 100).await.unwrap().as_ref(), &model[..]);
        assert_eq!(
            store.segments.read_calls(),
            0,
            "a freshly sealed write remains in the decoded extent cache"
        );
    }

    // Not a correctness test: measures single-stream engine-side write
    // throughput (codec + open-buffer append + seal + commit, no protocol, no
    // inode lock) against an in-memory object store.
    //   cargo test --release --lib -- --ignored --nocapture bench_sequential_write
    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    #[ignore = "throughput measurement, run explicitly in release"]
    async fn bench_sequential_write_throughput() {
        for (name, compression) in [
            ("lz4", CompressionConfig::Lz4),
            ("zstd(3)", CompressionConfig::Zstd(3)),
        ] {
            for (shape, data) in [
                ("incompressible", incompressible(1, 256 * 1024)),
                ("zeros", vec![0u8; 256 * 1024]),
            ] {
                let (store, db, _object_store) = make_with_compression(compression).await;
                let chunk = data.len();
                let total: usize = 512 * 1024 * 1024;
                let payload = Bytes::from(data);
                let start = std::time::Instant::now();
                let mut size = 0u64;
                for i in 0..(total / chunk) {
                    let mut txn = db.new_transaction().unwrap();
                    let tu = store
                        .write(&mut txn, 1, (i * chunk) as u64, &payload, size)
                        .await
                        .unwrap();
                    commit(&store, txn).await;
                    store.apply_tail_update(1, tu);
                    size += chunk as u64;
                }
                let secs = start.elapsed().as_secs_f64();
                eprintln!(
                    "engine sequential write [{name}, {shape}, 256 KiB ops]: {:.0} MB/s",
                    total as f64 / secs / 1e6
                );
            }
        }
    }

    // A batch of >= PARALLEL_COMPRESS_MIN_FRAMES frames takes the rayon
    // pre-compression fan-out (multi-thread runtimes only; the other tests run
    // current-thread and take the inline branch) and must roundtrip identically.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn large_batch_write_takes_parallel_compression_path() {
        let (store, db) = make().await;
        let mut model = Vec::new();
        let data = incompressible(3, PARALLEL_COMPRESS_MIN_FRAMES * 2 * EXTENT_SIZE);
        write_and_check(&store, &db, &mut model, 0, &data).await;
    }

    #[test]
    fn canonical_nbd_member_batch_stays_inline_while_one_mib_batch_parallelizes() {
        let member_frames = 256 * 1024 / EXTENT_SIZE;
        let one_mib_frames = 1024 * 1024 / EXTENT_SIZE;

        assert!(!should_parallel_compress(member_frames));
        assert!(should_parallel_compress(one_mib_frames));
    }

    #[tokio::test]
    async fn large_write_seals_async_then_drains() {
        let (store, db) = make().await;
        let mut model = Vec::new();
        // Incompressible bytes, so the sealed buffer actually crosses the threshold
        // (a repeating pattern would compress below it and never seal).
        let n = store.seal_threshold() + 4 * EXTENT_SIZE;
        let data = incompressible(0, n);
        // One >threshold write triggers a background seal; the data reads back
        // correctly whether still in flight (sealing buffer) or already sealed.
        write_and_check(&store, &db, &mut model, 0, &data).await;

        // The flush barrier drains every in-flight seal to the object store.
        store.seal_open().await.unwrap();
        assert!(!store.segments.list_segments().await.unwrap().is_empty());
        assert_eq!(
            store.read(1, 0, n as u64).await.unwrap().as_ref(),
            model.as_slice()
        );
    }

    #[tokio::test]
    async fn configured_seal_limit_backpressures_before_later_writers_append() {
        let (store, db) = make().await;
        let mut store = store.with_seal_threshold(1);
        store.max_inflight_seals = 7;
        store.seal_upload_sem = Arc::new(Semaphore::new(store.max_inflight_seals));
        store.seal_residency_sem = Arc::new(Semaphore::new(store.max_inflight_seals));

        // Model a full resident-buffer budget. The first writer can append, but
        // then has to wait before rotating.
        assert_eq!(store.max_inflight_seals, 7);
        let permits = store
            .seal_residency_sem
            .clone()
            .acquire_many_owned(store.max_inflight_seals as u32)
            .await
            .unwrap();
        let first = tokio::spawn({
            let store = store.clone();
            let db = db.clone();
            async move {
                let mut txn = db.new_transaction().unwrap();
                store
                    .write(&mut txn, 100, 0, &Bytes::from_static(b"a"), 0)
                    .await
                    .unwrap();
            }
        });

        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            loop {
                if store.open_lane(100).open.lock().unwrap().dir.len() == 1 {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("first writer did not reach the seal-permit wait");

        let second = tokio::spawn({
            let store = store.clone();
            let db = db.clone();
            async move {
                let mut txn = db.new_transaction().unwrap();
                store
                    .write(&mut txn, 104, 0, &Bytes::from_static(b"b"), 0)
                    .await
                    .unwrap();
            }
        });

        let appended_behind_blocked_seal =
            tokio::time::timeout(std::time::Duration::from_millis(100), async {
                loop {
                    if store.open_lane(100).open.lock().unwrap().dir.len() > 1 {
                        break;
                    }
                    tokio::task::yield_now().await;
                }
            })
            .await
            .is_ok();
        assert!(
            !appended_behind_blocked_seal,
            "a later writer grew the open segment while rotation was blocked"
        );
        assert!(!second.is_finished());

        drop(permits);
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            first.await.unwrap();
            second.await.unwrap();
            while !store.seals_quiet() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("writers did not resume after seal permits were released");
    }

    #[tokio::test]
    async fn failed_background_seal_keeps_residency_bound_and_flush_makes_progress() {
        let (_store, db) = make().await;
        let (object_store, controls) = FaultStore::new(Arc::new(InMemory::new()));
        let object_store: Arc<dyn ObjectStore> = object_store;
        let mut store =
            make_store(object_store, db.clone(), CompressionConfig::Lz4, 7).with_seal_threshold(1);
        store.max_inflight_seals = 1;
        store.seal_upload_sem = Arc::new(Semaphore::new(1));
        store.seal_residency_sem = Arc::new(Semaphore::new(1));
        controls.fail_puts(2);

        let mut first_txn = db.new_transaction().unwrap();
        store
            .write(&mut first_txn, 100, 0, &Bytes::from_static(b"a"), 0)
            .await
            .unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            while controls.put_count() < 1 || store.sealing.lock().unwrap().len() != 1 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the first background seal did not fail and remain resident");

        let mut second = tokio::spawn({
            let store = store.clone();
            let db = db.clone();
            async move {
                let mut txn = db.new_transaction().unwrap();
                store
                    .write(&mut txn, 104, 0, &Bytes::from_static(b"b"), 0)
                    .await
            }
        });
        let completed = tokio::time::timeout(std::time::Duration::from_millis(250), &mut second)
            .await
            .is_ok();
        assert!(
            !completed,
            "a failed resident seal released its memory budget to a later writer"
        );
        assert_eq!(store.sealing.lock().unwrap().len(), 1);
        assert_eq!(controls.put_count(), 1);

        assert!(store.seal_open().await.is_err());
        assert!(!second.is_finished());
        assert_eq!(store.sealing.lock().unwrap().len(), 1);

        store.seal_open().await.unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            second.await.unwrap().unwrap();
            while !store.seals_quiet() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("flush did not release the residency budget for the blocked writer");
        assert!(store.sealing.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn seal_open_releases_append_gate_after_generation_rotation() {
        let (mut store, db) = make().await;
        let put_gate = Arc::new(Semaphore::new(0));
        store.seal_open_put_gate = Some(Arc::clone(&put_gate));

        let mut first_txn = db.new_transaction().unwrap();
        store
            .write(
                &mut first_txn,
                201,
                0,
                &Bytes::from(vec![0x41; EXTENT_SIZE]),
                0,
            )
            .await
            .unwrap();
        commit(&store, first_txn).await;

        let flush = tokio::spawn({
            let store = store.clone();
            async move { store.seal_open().await }
        });
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            while !store.open_lane(201).open.lock().unwrap().dir.is_empty() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("seal_open did not rotate its captured generation");

        let later_write = tokio::spawn({
            let store = store.clone();
            let db = db.clone();
            async move {
                let mut txn = db.new_transaction().unwrap();
                store
                    .write(&mut txn, 202, 0, &Bytes::from(vec![0x42; EXTENT_SIZE]), 0)
                    .await
            }
        });
        tokio::time::timeout(std::time::Duration::from_millis(250), later_write)
            .await
            .expect("a captured generation PUT retained the global append gate")
            .unwrap()
            .unwrap();

        put_gate.add_permits(1);
        flush.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn sequential_append_via_tail_cache() {
        let (store, db) = make().await;
        let mut model = Vec::new();
        // Many small appends into the same tail extent: exercises the tail cache
        // (each append RMWs the cached tail rather than re-fetching the frame).
        for _ in 0..50 {
            let off = model.len();
            write_and_check(&store, &db, &mut model, off, b"0123456789").await;
        }
        assert_eq!(model.len(), 500);
    }

    #[tokio::test]
    async fn aligned_append_skips_old_extent_scan() {
        let (store, db) = make().await;
        let inode: InodeId = 1;

        // The first full extent leaves an aligned EOF. Appending the next full
        // extent cannot supersede any old FrameLoc, so it needs no debit scan.
        let mut txn = db.new_transaction().unwrap();
        store
            .write(&mut txn, inode, 0, &Bytes::from(vec![1u8; EXTENT_SIZE]), 0)
            .await
            .unwrap();
        commit(&store, txn).await;
        let scans_before_append = store.old_extent_scan_ranges().len();

        let mut txn = db.new_transaction().unwrap();
        store
            .write(
                &mut txn,
                inode,
                EXTENT_SIZE as u64,
                &Bytes::from(vec![2u8; EXTENT_SIZE]),
                EXTENT_SIZE as u64,
            )
            .await
            .unwrap();
        assert_eq!(
            store.old_extent_scan_ranges().len(),
            scans_before_append,
            "an aligned append must not probe extents at or beyond old EOF"
        );
    }

    #[tokio::test]
    async fn unaligned_append_scans_only_old_tail_extent() {
        let (store, db) = make().await;
        let inode: InodeId = 1;
        let old_size = 2 * EXTENT_SIZE as u64 - 13;

        let mut txn = db.new_transaction().unwrap();
        store
            .write(
                &mut txn,
                inode,
                0,
                &Bytes::from(vec![1u8; old_size as usize]),
                0,
            )
            .await
            .unwrap();
        commit(&store, txn).await;

        // An unaligned EOF still has one old tail FrameLoc. A write that fills
        // that tail and extends into a new extent must scan exactly the tail,
        // not the newly appended extent.
        let mut txn = db.new_transaction().unwrap();
        store
            .write(
                &mut txn,
                inode,
                old_size,
                &Bytes::from(vec![3u8; EXTENT_SIZE]),
                old_size,
            )
            .await
            .unwrap();
        assert_eq!(
            store.old_extent_scan_ranges().last().copied(),
            Some((1, 2)),
            "an unaligned append must probe only the old tail extent for debits"
        );
    }

    #[tokio::test]
    async fn presized_sparse_hole_overwrite_runs_no_metadata_scan() {
        let (store, db) = make().await;
        let inode: InodeId = 1;
        // The production NBD stripe-member shape: the member file is truncated
        // to its final size before the first write, so the inode reports a
        // large size while every extent is still an untouched hole.
        let presized = 128 * 1024 * 1024u64;
        let chunk = 8 * EXTENT_SIZE; // one canonical 256 KiB member chunk

        let scans_before = db.scan_call_count();
        let mut txn = db.new_transaction().unwrap();
        let tu = store
            .write(
                &mut txn,
                inode,
                0,
                &Bytes::from(incompressible(1, chunk)),
                presized,
            )
            .await
            .unwrap();
        commit(&store, txn).await;
        store.apply_tail_update(inode, tu);
        assert_eq!(
            db.scan_call_count(),
            scans_before,
            "a small overwrite below a pre-sized EOF must discover superseded \
             FrameLocs via point lookups, not a SlateDB range scan"
        );
    }

    #[tokio::test]
    async fn presized_sparse_overwrite_still_debits_superseded_frames() {
        let (store, db) = make().await;
        let inode: InodeId = 1;
        let presized = 128 * 1024 * 1024u64;
        let chunk = 8 * EXTENT_SIZE;

        let mut txn = db.new_transaction().unwrap();
        let tu = store
            .write(
                &mut txn,
                inode,
                0,
                &Bytes::from(incompressible(1, chunk)),
                presized,
            )
            .await
            .unwrap();
        commit(&store, txn).await;
        store.apply_tail_update(inode, tu);
        let old_segid = frameloc_of(&store, &db, inode, 0).await.unwrap().segid;
        let (live_before, total_before) = segcount_pair_of(&store, &db, old_segid).await;
        assert!(live_before > 0);
        // Rotate so the overwrite's frames land in a fresh segment and the old
        // segment's counter isolates the debits.
        store.seal_open().await.unwrap();

        let mut txn = db.new_transaction().unwrap();
        let tu = store
            .write(
                &mut txn,
                inode,
                0,
                &Bytes::from(incompressible(2, chunk)),
                presized,
            )
            .await
            .unwrap();
        commit(&store, txn).await;
        store.apply_tail_update(inode, tu);

        let (live_after, total_after) = segcount_pair_of(&store, &db, old_segid).await;
        assert_eq!(
            total_after, total_before,
            "total is monotonic, never debited"
        );
        assert_eq!(
            live_after, 0,
            "a pre-sized overwrite must debit every superseded frame"
        );
    }

    // Companion to bench_sequential_write_throughput: the same sequential
    // stream, but into an inode pre-sized to its final length (the production
    // NBD stripe-member shape), so every write lands below the reported EOF in
    // an untouched hole.
    //   cargo test --release --lib -- --ignored --nocapture bench_presized_sparse
    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    #[ignore = "throughput measurement, run explicitly in release"]
    async fn bench_presized_sparse_overwrite_throughput() {
        for (name, chunk) in [("256 KiB", 8 * EXTENT_SIZE), ("1 MiB", 32 * EXTENT_SIZE)] {
            let (store, db, _object_store) = make_with_compression(CompressionConfig::Lz4).await;
            let total: usize = 512 * 1024 * 1024;
            let payload = Bytes::from(incompressible(1, chunk));
            let scans_before = db.scan_call_count();
            let start = std::time::Instant::now();
            for i in 0..(total / chunk) {
                let mut txn = db.new_transaction().unwrap();
                let tu = store
                    .write(&mut txn, 1, (i * chunk) as u64, &payload, total as u64)
                    .await
                    .unwrap();
                commit(&store, txn).await;
                store.apply_tail_update(1, tu);
            }
            let secs = start.elapsed().as_secs_f64();
            let scans = db.scan_call_count() - scans_before;
            eprintln!(
                "engine pre-sized sparse overwrite [lz4, incompressible, {name} ops]: \
                 {:.0} MB/s, {scans} old-extent scans",
                total as f64 / secs / 1e6
            );
        }
    }

    // Two hot inodes colliding on one affine lane must not serialize behind a
    // single append gate while other lanes sit idle (the production NBD shape:
    // four stripe members mapped onto four lanes by inode id usually collide).
    #[tokio::test]
    async fn colliding_inodes_spill_to_an_idle_append_lane() {
        let (store, db) = make().await;
        let a: InodeId = 1;
        let b: InodeId = a + OPEN_SEGMENT_LANES as u64;
        assert!(
            std::ptr::eq(store.open_lane(a), store.open_lane(b)),
            "test premise: a and b share an affine lane"
        );

        // Writer A occupies the shared affine gate, as it does for the whole
        // AEAD + append window of a staging batch.
        let gate = store.open_lane(a).append_gate.lock().await;

        // Writer B must spill to an idle lane and complete while A's gate is
        // still held, instead of queueing behind it.
        let data = Bytes::from(incompressible(2, EXTENT_SIZE));
        let mut txn = db.new_transaction().unwrap();
        let staged = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            store.write(&mut txn, b, 0, &data, 0),
        )
        .await
        .expect("a colliding writer must spill to an idle lane, not queue")
        .unwrap();
        drop(gate);
        commit(&store, txn).await;
        store.apply_tail_update(b, staged);
        assert_eq!(
            store.read(b, 0, EXTENT_SIZE as u64).await.unwrap().as_ref(),
            data.as_ref()
        );
    }

    // Overwriting extents this store itself committed must answer old-debit
    // discovery from the extent-location cache: a database point read can
    // land on a cold SST block on the remote object store, where one fetch
    // costs a network round trip on the write ACK path.
    #[tokio::test]
    async fn warm_overwrite_debits_from_the_location_cache() {
        let (store, db) = make().await;
        let inode: InodeId = 1;
        let presized = 128 * 1024 * 1024u64;
        let chunk = 8 * EXTENT_SIZE;

        let mut txn = db.new_transaction().unwrap();
        let tu = store
            .write(
                &mut txn,
                inode,
                0,
                &Bytes::from(incompressible(1, chunk)),
                presized,
            )
            .await
            .unwrap();
        commit(&store, txn).await;
        store.apply_tail_update(inode, tu);

        let db_lookups_before = store.old_debit_db_lookup_count();
        let mut txn = db.new_transaction().unwrap();
        let tu = store
            .write(
                &mut txn,
                inode,
                0,
                &Bytes::from(incompressible(2, chunk)),
                presized,
            )
            .await
            .unwrap();
        commit(&store, txn).await;
        store.apply_tail_update(inode, tu);
        assert_eq!(
            store.old_debit_db_lookup_count(),
            db_lookups_before,
            "a warm overwrite must resolve superseded FrameLocs from the \
             location cache, not database point reads"
        );
    }

    /// A one-shot blocking latch usable from the synchronous `before_batch_seal`
    /// probe, which cannot await. Parks a runtime worker thread, so every test
    /// using it runs on the multi-thread flavor with spare workers.
    struct Latch {
        entered: std::sync::atomic::AtomicBool,
        released: std::sync::Mutex<bool>,
        wake: std::sync::Condvar,
    }

    impl Latch {
        fn new() -> Self {
            Self {
                entered: std::sync::atomic::AtomicBool::new(false),
                released: std::sync::Mutex::new(false),
                wake: std::sync::Condvar::new(),
            }
        }

        fn block(&self) {
            self.entered
                .store(true, std::sync::atomic::Ordering::SeqCst);
            let mut released = self.released.lock().unwrap();
            while !*released {
                released = self.wake.wait(released).unwrap();
            }
        }

        fn entered(&self) -> bool {
            self.entered.load(std::sync::atomic::Ordering::SeqCst)
        }

        fn release(&self) {
            *self.released.lock().unwrap() = true;
            self.wake.notify_all();
        }
    }

    // The per-lane append gate exists to hand out (segid, frame-index run, byte
    // range), not to guard the ~300us batch AEAD that follows it. Writers that
    // share one lane must therefore encrypt concurrently. Pin it by removing
    // the idle-lane spill (the other gates are held) so every writer really
    // does contend for one gate.
    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn same_lane_batches_overlap_their_aead() {
        const WRITERS: u64 = 4;
        let (mut store, db) = make().await;
        let active = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let max_active = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        store.before_batch_seal = Some({
            let active = Arc::clone(&active);
            let max_active = Arc::clone(&max_active);
            Arc::new(move || {
                // block_in_place hands this worker's remaining tasks to the
                // other workers. Without it the writer woken by the gate
                // release sits in this worker's (unstealable) LIFO slot for the
                // whole sleep, and the test measures tokio, not the gate.
                tokio::task::block_in_place(|| {
                    let now = active.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1;
                    max_active.fetch_max(now, std::sync::atomic::Ordering::SeqCst);
                    std::thread::sleep(std::time::Duration::from_millis(150));
                    active.fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
                });
            })
        });

        // Every writer's affine lane is lane 0; holding the other three gates
        // denies `lock_append_lane` its idle-lane spill.
        let inodes: Vec<InodeId> = (0..WRITERS)
            .map(|w| 100 + w * OPEN_SEGMENT_LANES as u64)
            .collect();
        for inode in &inodes {
            assert!(
                std::ptr::eq(store.open_lane(*inode), &store.open_lanes[0]),
                "test premise: every writer is affine to lane 0"
            );
        }
        let mut held = Vec::new();
        for lane in store.open_lanes.iter().skip(1) {
            held.push(lane.append_gate.lock().await);
        }

        let writes: Vec<_> = inodes
            .iter()
            .map(|inode| {
                let (store, db, inode) = (store.clone(), db.clone(), *inode);
                crate::task::spawn_named("same-lane-writer", async move {
                    let mut txn = db.new_transaction().unwrap();
                    let payload = Bytes::from(incompressible(inode as usize, EXTENT_SIZE));
                    store
                        .stage_edits(&mut txn, inode, &[(0, Some(payload))], 0)
                        .await
                        .unwrap();
                    commit(&store, txn).await;
                })
            })
            .collect();
        for write in writes {
            write.await.unwrap();
        }
        drop(held);

        assert_eq!(
            max_active.load(std::sync::atomic::Ordering::SeqCst),
            WRITERS as usize,
            "batches sharing one append lane serialized their AEAD"
        );

        // Overlapped AEAD must still land each frame at its reserved index.
        for inode in &inodes {
            assert_eq!(
                store.read(*inode, 0, EXTENT_SIZE as u64).await.unwrap(),
                Bytes::from(incompressible(*inode as usize, EXTENT_SIZE)),
            );
        }
        store.seal_open().await.unwrap();
    }

    // A batch that has reserved its byte range but not yet filled it leaves a
    // hole in the open buffer. Rotation (the flush barrier here) must wait for
    // that fill instead of sealing the hole into an immutable segment.
    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn rotation_waits_for_an_in_flight_reservation_instead_of_sealing_a_hole() {
        let (mut store, db, object_store) = make_with_compression(CompressionConfig::Lz4).await;
        let inode: InodeId = 5;
        let seed = Bytes::from(incompressible(11, EXTENT_SIZE));
        let stalled = Bytes::from(incompressible(22, EXTENT_SIZE));

        let latch = Arc::new(Latch::new());
        let armed = Arc::new(std::sync::atomic::AtomicBool::new(false));
        store.before_batch_seal = Some({
            let latch = Arc::clone(&latch);
            let armed = Arc::clone(&armed);
            Arc::new(move || {
                if armed.swap(false, std::sync::atomic::Ordering::SeqCst) {
                    latch.block();
                }
            })
        });

        let mut txn = db.new_transaction().unwrap();
        store.write(&mut txn, inode, 0, &seed, 0).await.unwrap();
        commit(&store, txn).await;
        let lane = store.open_lane(inode);
        let (segid, frames_before) = {
            let open = lane.open.lock().unwrap();
            (open.segid, open.dir.len())
        };
        assert_eq!(frames_before, 1);

        armed.store(true, std::sync::atomic::Ordering::SeqCst);
        let writer = crate::task::spawn_named("stalled-writer", {
            let (store, db, stalled) = (store.clone(), db.clone(), stalled.clone());
            async move {
                let mut txn = db.new_transaction().unwrap();
                store
                    .write(&mut txn, inode, EXTENT_SIZE as u64, &stalled, 0)
                    .await
                    .unwrap();
                commit(&store, txn).await;
            }
        });
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            while !latch.entered() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the second batch never reached its AEAD window");

        // The reservation is published (frame index and byte range claimed)
        // while its bytes are still unwritten: exactly the hole rotation must
        // not seal. It also proves the gate was released before the AEAD.
        let (reserved_frames, reserved_segid) = {
            let open = lane.open.lock().unwrap();
            (open.dir.len(), open.segid)
        };

        let mut flush = crate::task::spawn_named("flush", {
            let store = store.clone();
            async move { store.seal_open().await }
        });
        let sealed_early = tokio::time::timeout(std::time::Duration::from_millis(400), &mut flush)
            .await
            .is_ok();

        // Release before asserting: a parked latch would otherwise wedge
        // runtime shutdown and turn a failure into a hang.
        latch.release();
        writer.await.unwrap();
        flush.await.unwrap().unwrap();
        assert_eq!(
            reserved_frames, 2,
            "the stalled batch did not reserve its frame slot before the AEAD"
        );
        assert_eq!(reserved_segid, segid);
        assert!(
            !sealed_early,
            "the flush barrier sealed a segment while a reservation was unfilled"
        );

        // Cold reader over the published objects: proves the segment on the
        // object store carries real frame bytes, not a zero hole.
        let cold = make_store(object_store, db.clone(), CompressionConfig::Lz4, 8);
        assert_eq!(cold.read(inode, 0, EXTENT_SIZE as u64).await.unwrap(), seed);
        assert_eq!(
            cold.read(inode, EXTENT_SIZE as u64, EXTENT_SIZE as u64)
                .await
                .unwrap(),
            stalled
        );
    }

    // A reserved-but-unfilled range must never be read back or shipped as if it
    // held frame bytes. Committed neighbours in the same open segment stay
    // exact while the hole is open.
    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn a_reservation_hole_never_leaks_into_reads_or_shipped_frames() {
        let (mut store, db) = make().await;
        let inode: InodeId = 5;
        let seed = Bytes::from(incompressible(33, EXTENT_SIZE));

        let latch = Arc::new(Latch::new());
        let armed = Arc::new(std::sync::atomic::AtomicBool::new(false));
        store.before_batch_seal = Some({
            let latch = Arc::clone(&latch);
            let armed = Arc::clone(&armed);
            Arc::new(move || {
                if armed.swap(false, std::sync::atomic::Ordering::SeqCst) {
                    latch.block();
                }
            })
        });

        let mut txn = db.new_transaction().unwrap();
        store.write(&mut txn, inode, 0, &seed, 0).await.unwrap();
        commit(&store, txn).await;
        let seed_loc = frameloc_of(&store, &db, inode, 0).await.unwrap();

        armed.store(true, std::sync::atomic::Ordering::SeqCst);
        let writer = crate::task::spawn_named("stalled-writer", {
            let (store, db) = (store.clone(), db.clone());
            async move {
                let mut txn = db.new_transaction().unwrap();
                store
                    .write(
                        &mut txn,
                        inode,
                        EXTENT_SIZE as u64,
                        &Bytes::from(incompressible(44, EXTENT_SIZE)),
                        0,
                    )
                    .await
                    .unwrap();
                commit(&store, txn).await;
            }
        });
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            while !latch.entered() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the second batch never reached its AEAD window");

        // Force a real decode out of the open buffer, past the plaintext cache.
        evict_decoded_extents(&store);
        let read_through_hole = store.read(inode, 0, EXTENT_SIZE as u64).await;

        // The HA ship path reads the same buffer by (segid, offset, len).
        let key = store.key_codec.extent_key(inode, 0);
        let enriched = store.enrich_repl_ops(vec![ReplOp::Put(
            key.clone(),
            Bytes::copy_from_slice(&seed_loc.encode()),
        )]);
        let shipped = match enriched.as_slice() {
            [ReplOp::PutFrame(shipped_key, _, frame)] if *shipped_key == key => {
                Some(crate::segment::read_frames_from_region(
                    &store.codec,
                    frame,
                    seed_loc.segid,
                    seed_loc.frame_index,
                    &[(inode, 0)],
                ))
            }
            _ => None,
        };

        // Release before asserting: a parked latch would otherwise wedge
        // runtime shutdown and turn a failure into a hang.
        latch.release();
        writer.await.unwrap();
        store.seal_open().await.unwrap();
        assert_eq!(
            read_through_hole.unwrap(),
            seed,
            "a neighbouring reservation hole corrupted a committed frame's read"
        );
        assert_eq!(
            shipped
                .expect("a resident frame must ship as PutFrame")
                .expect("a shipped frame must AEAD-verify at its own index"),
            vec![seed.to_vec()],
        );
    }

    /// Mean per-staging-batch nanoseconds of every `stage_edits` phase, so a
    /// flat scaling curve can be attributed instead of guessed at. The phases
    /// are sequential, not nested: `gate_hold` is the reservation window alone,
    /// and everything after it runs off the lane's append gate.
    fn phase_report(store: &ExtentStore) -> String {
        use std::sync::atomic::Ordering::Relaxed;
        let p = &store.stage_phase_nanos;
        let batches = p.batches.load(Relaxed).max(1) as f64;
        let us =
            |counter: &std::sync::atomic::AtomicU64| counter.load(Relaxed) as f64 / batches / 1e3;
        format!(
            "per-batch us: old_debit {:.0}, compress {:.0}, protect_ref {:.0}, \
             gate_wait {:.0}, gate_hold(reserve) {:.0}, aead {:.0}, open_lock {:.0}, \
             append {:.0}, txn_stage {:.0}, spawn_seal {:.0}",
            us(&p.old_debit),
            us(&p.compress),
            us(&p.protect_ref),
            us(&p.gate_wait),
            us(&p.gate_hold),
            us(&p.aead),
            us(&p.open_lock_wait),
            us(&p.append),
            us(&p.txn_stage),
            us(&p.spawn_seal),
        )
    }

    /// One scaling point: `writers` independent stripe-member inodes, each
    /// overwritten with canonical 256 KiB chunks below a pre-sized EOF.
    async fn run_distinct_inode_scaling(writers: usize, stride: u64, label: &str) {
        let (store, db, _object_store) = make_with_compression(CompressionConfig::Lz4).await;
        let chunk = 8 * EXTENT_SIZE; // one canonical 256 KiB member chunk
        let per_writer = 64 * 1024 * 1024usize;
        let lanes: HashSet<u64> = (0..writers as u64)
            .map(|w| (100 + w * stride) & (OPEN_SEGMENT_LANES as u64 - 1))
            .collect();

        let start = std::time::Instant::now();
        let mut tasks = Vec::with_capacity(writers);
        for w in 0..writers as u64 {
            let store = store.clone();
            let db = db.clone();
            tasks.push(crate::task::spawn_named("bench-writer", async move {
                let inode: InodeId = 100 + w * stride;
                let payload = Bytes::from(incompressible(w as usize + 1, chunk));
                let (mut stage_ns, mut commit_ns) = (0u64, 0u64);
                for i in 0..(per_writer / chunk) {
                    let mut txn = db.new_transaction().unwrap();
                    let t0 = std::time::Instant::now();
                    let tu = store
                        .write(
                            &mut txn,
                            inode,
                            (i * chunk) as u64,
                            &payload,
                            per_writer as u64,
                        )
                        .await
                        .unwrap();
                    let t1 = std::time::Instant::now();
                    commit(&store, txn).await;
                    commit_ns += t1.elapsed().as_nanos() as u64;
                    stage_ns += (t1 - t0).as_nanos() as u64;
                    store.apply_tail_update(inode, tu);
                }
                (stage_ns, commit_ns)
            }));
        }
        let mut stage_ns = 0u64;
        let mut commit_ns = 0u64;
        for task in tasks {
            let (s, c) = task.await.unwrap();
            stage_ns += s;
            commit_ns += c;
        }
        let secs = start.elapsed().as_secs_f64();
        let ops = (writers * (per_writer / chunk)) as f64;
        eprintln!(
            "distinct-inode scaling [{label}] writers={writers} lanes={}: {:.0} MB/s aggregate \
             (mean per write: stage {:.0} us, commit {:.0} us); {}",
            lanes.len(),
            (writers * per_writer) as f64 / secs / 1e6,
            stage_ns as f64 / ops / 1e3,
            commit_ns as f64 / ops / 1e3,
            phase_report(&store),
        );
    }

    // Cross-inode write scaling for the production NBD stripe shape: several
    // pre-sized sparse member files overwritten concurrently. Aggregate
    // throughput should grow with the writer count; a flat curve means a serial
    // resource in the extent staging path rather than in the commit worker.
    //   cargo test --release --lib -- --ignored --nocapture bench_distinct_inode
    #[tokio::test(flavor = "multi_thread", worker_threads = 16)]
    #[ignore = "throughput measurement, run explicitly in release"]
    async fn bench_distinct_inode_write_scaling() {
        for writers in [1usize, 2, 4, 8] {
            run_distinct_inode_scaling(writers, 1, "consecutive inodes").await;
        }
        // Controls: four writers that all hash to one append lane, versus four
        // that each own a lane. The delta isolates lane collision from the rest
        // of the path.
        run_distinct_inode_scaling(4, 4, "one shared lane").await;
    }
}
