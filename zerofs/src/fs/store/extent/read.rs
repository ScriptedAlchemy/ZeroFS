//! Read path: FrameLoc resolution, run-coalesced ranged reads with
//! read-your-writes from the in-RAM buffers, and the sequential logical
//! read-ahead. Demand reads also record compaction heat (nominations,
//! crossing pairs).

use super::select::{NOMINATE_MIN_FANOUT, NOMINATE_PER_CALL_CAP, PAIR_BUMPS_PER_CALL, PairStats};
use super::{CachedExtentLocation, ExtentStore, PARALLEL_EXTENT_OPS, ZERO_EXTENT};
#[cfg(feature = "failpoints")]
use crate::failpoints::{self as fp, fail_point};
use crate::fs::inode::InodeId;
use crate::fs::{EXTENT_SIZE, FsError};
use crate::segment::{FrameLoc, Segid};
use crate::segment_store::SegmentStoreError;
use bytes::{Bytes, BytesMut};
use futures::stream::StreamExt;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Instant;
use tokio::sync::Semaphore;
use tracing::error;

/// Logical (file-offset) read-ahead distance for a confirmed-sequential
/// stream. Follows the file's extents across segments, which the physical
/// (per-object) prefetcher can't do.
const READ_AHEAD_WINDOW_BYTES: u64 = 8 * 1024 * 1024;
/// Consecutive sequential reads before prefetch kicks in, so a one-off read
/// doesn't drag in a whole window.
const READ_AHEAD_MIN_SEQ: u32 = 2;
/// Global cap on concurrent in-flight read-ahead fetches.
pub(super) const READ_AHEAD_MAX_CONCURRENT: usize = 16;
/// Bound on the per-inode read-ahead state map (LRU-evicted; ~24 B/entry).
pub(super) const READ_AHEAD_TRACK_BYTES: usize = 4 * 1024 * 1024;

/// Given the per-inode read-ahead state `(last_read_end, prefetched_to, seq_run)`
/// and a read of `[offset, offset+length)`, return the new state and, when a
/// prefetch is warranted, the `[start, end)` file range to fetch.
fn plan_read_ahead(
    (last_end, prefetched_to, seq): (u64, u64, u32),
    offset: u64,
    length: u64,
) -> ((u64, u64, u32), Option<(u64, u64)>) {
    let read_end = offset + length;
    // A jump starts a new (unconfirmed) sequence.
    if offset != last_end {
        return ((read_end, read_end, 1), None);
    }
    let seq = seq.saturating_add(1);
    if seq < READ_AHEAD_MIN_SEQ {
        return ((read_end, read_end, seq), None);
    }
    // Refill only once less than half a window remains ahead: few large
    // fetches, not one tiny fetch per read.
    let ahead = prefetched_to.saturating_sub(read_end);
    if ahead >= READ_AHEAD_WINDOW_BYTES / 2 {
        return ((read_end, prefetched_to, seq), None);
    }
    let target = read_end + READ_AHEAD_WINDOW_BYTES;
    let start = prefetched_to.max(read_end);
    ((read_end, target, seq), Some((start, target)))
}

impl ExtentStore {
    /// Metrics snapshot of this store's most recent fragmented read.
    /// Shared across clones of the store; replaces the old process-global
    /// slot that let concurrent tests clobber each other's snapshots.
    #[cfg(test)]
    fn last_read_metrics(&self) -> Option<metrics::ReadRunSnapshot> {
        *self
            .last_read_metrics
            .lock()
            .expect("last_read_metrics never poisoned")
    }

    async fn load_extent_location(
        &self,
        id: InodeId,
        extent_idx: u64,
    ) -> Result<CachedExtentLocation, FsError> {
        let key = self.key_codec.extent_key(id, extent_idx);
        let encoded = self.db.get_bytes(&key).await.map_err(|error| {
            error!(
                "Failed to read extent (inode={}, extent={}): {}",
                id, extent_idx, error
            );
            FsError::IoError
        })?;
        let Some(encoded) = encoded else {
            return Ok(CachedExtentLocation::Hole);
        };
        FrameLoc::decode(&encoded)
            .map(CachedExtentLocation::Frame)
            .ok_or_else(|| {
                error!("Corrupt extent value (inode={}, extent={})", id, extent_idx);
                FsError::IoError
            })
    }

    /// The full-extent (EXTENT_SIZE) plaintext for `(id, extent)`, or `None` for a
    /// hole. Resolves the extent key's `FrameLoc` then fetches the frame.
    pub(crate) async fn get(&self, id: InodeId, extent_idx: u64) -> Result<Option<Bytes>, FsError> {
        let key = self.key_codec.extent_key(id, extent_idx);
        let location = self
            .extent_location_cache
            .get_or_load((id, extent_idx), || {
                self.load_extent_location(id, extent_idx)
            })
            .await?;
        let loc = match location {
            CachedExtentLocation::Hole => return Ok(None),
            CachedExtentLocation::Frame(loc) => loc,
        };
        if let Some(frame) = self.decoded_get(id, extent_idx, loc) {
            return Ok(Some(frame));
        }
        match self.fetch_frame(id, extent_idx, loc).await? {
            Ok(b) => Ok(Some(b)),
            Err(first_err) => {
                // GC compaction repoints an extent to a freshly-sealed segment and
                // then deletes the drained source; a read that resolved the old
                // FrameLoc just before that delete can race it and 404. Re-resolve
                // the extent key once: if the pointer moved, read the new location;
                // if the extent was deleted concurrently (truncate/unlink) it is now
                // a hole; otherwise the error is real.
                let reresolved = self
                    .db
                    .get_bytes(&key)
                    .await
                    .map_err(|_| FsError::IoError)?;
                match Self::decode_reresolved_extent(id, extent_idx, reresolved.as_ref())? {
                    Some(new_loc) if new_loc.segid != loc.segid => {
                        match self.fetch_frame(id, extent_idx, new_loc).await? {
                            Ok(b) => Ok(Some(b)),
                            Err(e) => {
                                error!(
                                    "Failed to read frame after repoint retry \
                                     (inode={}, extent={}): {}",
                                    id, extent_idx, e
                                );
                                Err(FsError::IoError)
                            }
                        }
                    }
                    None => Ok(None),
                    _ => {
                        error!(
                            "Failed to read frame (inode={}, extent={}): {}",
                            id, extent_idx, first_err
                        );
                        Err(FsError::IoError)
                    }
                }
            }
        }
    }

    /// Fetch, validate, and decoded-cache the single frame at `loc` for
    /// `(id, extent)`. Reads the in-RAM open/sealing buffers first
    /// (read-your-writes) and falls back to the segment object. The object-read
    /// failure is handed back as `Ok(Err(..))` rather than logged here, because
    /// `get` treats it differently on the original location (retry the
    /// GC-repointed pointer) and on the relocated one (terminal EIO); an
    /// invalid frame or a corrupt `FrameLoc` is still the immediate `Err(FsError)`
    /// it has always been.
    async fn fetch_frame(
        &self,
        id: InodeId,
        extent: u64,
        loc: FrameLoc,
    ) -> Result<Result<Bytes, SegmentStoreError>, FsError> {
        if let Some(mut frames) = self.read_frames_in_ram(
            loc.segid,
            loc.byte_offset,
            loc.byte_len,
            loc.frame_index,
            &[(id, extent)],
        )? {
            let frame = frames.pop().expect("one frame");
            Self::validate_extent_frame(id, extent, &frame)?;
            self.decoded_insert(id, extent, loc, frame.clone());
            return Ok(Ok(frame));
        }
        match self.segments.read_extent(loc, id, extent).await {
            Ok(b) => {
                Self::validate_extent_frame(id, extent, &b)?;
                self.decoded_insert(id, extent, loc, b.clone());
                Ok(Ok(b))
            }
            Err(e) => Ok(Err(e)),
        }
    }

    /// Every stored frame decodes to exactly one full [`EXTENT_SIZE`] extent.
    /// Enforce that before callers slice the plaintext, so a bad frame that
    /// still passed AEAD surfaces as EIO instead of a panic.
    fn validate_extent_frame(id: InodeId, extent: u64, data: &Bytes) -> Result<(), FsError> {
        if data.len() != EXTENT_SIZE {
            error!(
                "Extent frame (inode={}, extent={}) decoded to {} bytes, expected {}",
                id,
                extent,
                data.len(),
                EXTENT_SIZE
            );
            return Err(FsError::IoError);
        }
        Ok(())
    }

    /// Classify the re-resolved extent value in `get`'s GC-repoint retry.
    /// An absent key means a concurrent truncate/unlink made the extent a
    /// genuine hole; a present but undecodable value is the same
    /// corrupt-value EIO as the primary path — never a fabricated hole of
    /// zeros.
    fn decode_reresolved_extent(
        id: InodeId,
        extent: u64,
        enc: Option<&Bytes>,
    ) -> Result<Option<FrameLoc>, FsError> {
        match enc {
            None => Ok(None),
            Some(enc) => match FrameLoc::decode(enc) {
                Some(loc) => Ok(Some(loc)),
                None => {
                    error!("Corrupt extent value (inode={}, extent={})", id, extent);
                    Err(FsError::IoError)
                }
            },
        }
    }

    /// Read a contiguous run of frames from the open or an in-flight sealing
    /// buffer (read-your-writes), or `None` if `segid` is already on the object
    /// store. Frame offsets are identical in the buffer and the finalized
    /// segment, so the same slice works for both.
    fn read_frames_in_ram(
        &self,
        segid: Segid,
        byte_offset: u64,
        byte_len: u32,
        first_frame: u32,
        slots: &[(InodeId, u64)],
    ) -> Result<Option<Vec<Bytes>>, FsError> {
        // The range comes from a db-stored FrameLoc: bounds-checked, never
        // trusted, so a corrupt value surfaces as EIO instead of an
        // out-of-range panic that would poison the open-segment lock for
        // every later writer.
        fn bounds(
            buf_len: usize,
            segid: Segid,
            byte_offset: u64,
            byte_len: u32,
        ) -> Result<std::ops::Range<usize>, FsError> {
            let start = byte_offset as usize;
            start
                .checked_add(byte_len as usize)
                .filter(|end| *end <= buf_len)
                .map(|end| start..end)
                .ok_or_else(|| {
                    error!(
                        "Corrupt FrameLoc for in-RAM {segid:?}: {byte_len} bytes at {byte_offset} \
                         exceed the {}-byte buffer",
                        buf_len
                    );
                    FsError::IoError
                })
        }
        // Snapshot only the sealed bytes while holding the matching mutable
        // lane buffer lock. AEAD verification and decompression are CPU work
        // and must not serialize appenders behind that mutex.
        let mut encoded = None;
        for lane in self.open_lanes.iter() {
            let open = lane.open.lock().unwrap();
            if segid == open.segid {
                let range = bounds(open.buf.len(), segid, byte_offset, byte_len)?;
                encoded = Some(Bytes::copy_from_slice(&open.buf[range]));
                break;
            }
        }
        let encoded = match encoded {
            Some(encoded) => Some(encoded),
            None => {
                // Sealing buffers are immutable Bytes, so cloning a slice is
                // zero-copy. Release the map lock before decoding it.
                let sealing = self.sealing.lock().unwrap();
                match sealing.get(&segid) {
                    Some(generation) => {
                        let range = bounds(generation.bytes.len(), segid, byte_offset, byte_len)?;
                        Some(generation.bytes.slice(range))
                    }
                    None => None,
                }
            }
        };
        let Some(encoded) = encoded else {
            return Ok(None);
        };

        let frames = crate::segment::read_frames_from_region(
            &self.codec,
            encoded.as_ref(),
            segid,
            first_frame,
            slots,
        )
        .map_err(|_| FsError::IoError)?;
        Ok(Some(frames.into_iter().map(Bytes::from).collect()))
    }

    /// Read `[offset, offset+length)`, then kick off the bounded,
    /// sequential-only logical read-ahead so the next read lands warm in the
    /// parts cache.
    #[cfg_attr(
        feature = "hotpath-profile",
        hotpath::measure(future = true, label = "zerofs.extent.read")
    )]
    pub async fn read(&self, id: InodeId, offset: u64, length: u64) -> Result<Bytes, FsError> {
        let data = self.read_range(id, offset, length, true).await?;
        self.trigger_read_ahead(id, offset, length);
        Ok(data)
    }

    /// Sequential-read detection + a bounded forward prefetch. Best-effort: the
    /// per-inode state is racy under concurrent readers of one file, which only
    /// costs a slightly-off prefetch.
    fn trigger_read_ahead(
        &self,
        id: InodeId,
        offset: u64,
        length: u64,
    ) -> Option<tokio::task::JoinHandle<()>> {
        if length == 0 {
            return None;
        }
        let prev = self.read_ahead.get(&id).map(|e| *e).unwrap_or((0, 0, 0));
        let (state, plan) = plan_read_ahead(prev, offset, length);
        let Some((start, end)) = plan else {
            self.read_ahead.insert(id, state);
            return None;
        };
        // Skip when at the concurrency cap rather than queueing (read-ahead is
        // best-effort). Coverage is committed only once the fetch is spawned:
        // on a skip, `prefetched_to` stays at the true high-water mark
        // (`start`), so the next read re-plans and re-tries the permit.
        let Ok(permit) = Arc::clone(&self.prefetch_sem).try_acquire_owned() else {
            let (read_end, _, seq) = state;
            self.read_ahead.insert(id, (read_end, start, seq));
            return None;
        };
        self.read_ahead.insert(id, state);
        let this = self.clone();
        let read_end = offset + length;
        Some(crate::task::spawn_named("read-ahead", async move {
            let _permit = permit;
            // Cross-segment only: within one segment the per-object prefetcher
            // already reads ahead, and a second interleaved stream would
            // fragment its window ramp into tiny GETs. Prefetch only when the
            // window reaches a segment it can't follow into.
            let cur_ext = read_end.saturating_sub(1) / EXTENT_SIZE as u64;
            let tgt_ext = end.saturating_sub(1) / EXTENT_SIZE as u64;
            let cur_seg = this.segment_at(id, cur_ext).await;
            if cur_seg.is_some() && cur_seg == this.segment_at(id, tgt_ext).await {
                return;
            }
            let _ = this.read_range(id, start, end - start, false).await;
        }))
    }

    /// The segment an extent's current frame lives in, or `None` for a hole.
    async fn segment_at(&self, id: InodeId, extent: u64) -> Option<Segid> {
        self.extent_location_cache
            .get_or_load((id, extent), || self.load_extent_location(id, extent))
            .await
            .ok()
            .and_then(|location| match location {
                CachedExtentLocation::Hole => None,
                CachedExtentLocation::Frame(loc) => Some(loc.segid),
            })
    }

    /// A byte-range read with no read-ahead side effect (the raw path; also what
    /// the read-ahead task calls, so it never recurses). Only demand reads
    /// (`is_demand`) feed compaction nominations, so prefetch traffic can't
    /// nominate.
    async fn read_range(
        &self,
        id: InodeId,
        offset: u64,
        length: u64,
        is_demand: bool,
    ) -> Result<Bytes, FsError> {
        if length == 0 {
            return Ok(Bytes::new());
        }
        let end = offset + length;
        let start_extent = offset / EXTENT_SIZE as u64;
        let end_extent = (end - 1) / EXTENT_SIZE as u64;
        let start_offset = (offset % EXTENT_SIZE as u64) as usize;

        if start_extent == end_extent {
            let extent_end = start_offset + length as usize;
            return Ok(match self.get(id, start_extent).await? {
                Some(data) => data.slice(start_offset..extent_end),
                None => Bytes::copy_from_slice(&ZERO_EXTENT[start_offset..extent_end]),
            });
        }

        let extent_keys: Vec<_> = (start_extent..=end_extent)
            .map(|extent| (id, extent))
            .collect();
        let locations = self
            .extent_location_cache
            .get_all_or_load(extent_keys, || async {
                let start_key = self.key_codec.extent_key(id, start_extent);
                let end_key = self.key_codec.extent_key(id, end_extent + 1);
                let mut locations =
                    vec![CachedExtentLocation::Hole; (end_extent - start_extent + 1) as usize];
                let mut stream = self.db.scan(start_key..end_key).await.map_err(|e| {
                    error!("Failed to scan extents (inode={}): {}", id, e);
                    FsError::IoError
                })?;
                while let Some(result) = stream.next().await {
                    let (key, value) = result.map_err(|e| {
                        error!("Failed to read extent during scan (inode={}): {}", id, e);
                        FsError::IoError
                    })?;
                    if let Some(extent_idx) = self.key_codec.parse_extent_key(&key) {
                        // A present-but-undecodable value is the same corrupt-value
                        // EIO as the single-extent path (`get`) — skipping it would
                        // serve the extent as a fabricated hole of zeros.
                        let loc = FrameLoc::decode(&value).ok_or_else(|| {
                            error!("Corrupt extent value (inode={}, extent={})", id, extent_idx);
                            FsError::IoError
                        })?;
                        locations[(extent_idx - start_extent) as usize] =
                            CachedExtentLocation::Frame(loc);
                    }
                }
                Ok(locations)
            })
            .await?;
        let loc_map: HashMap<u64, FrameLoc> = locations
            .into_iter()
            .enumerate()
            .filter_map(|(index, location)| match location {
                CachedExtentLocation::Hole => None,
                CachedExtentLocation::Frame(loc) => Some((start_extent + index as u64, loc)),
            })
            .collect();

        // Coalesce maximal same-segment runs first. Serve decoded/open-buffer
        // hits immediately; fetch independent on-store runs concurrently.
        let slice = |c: u64| -> (usize, usize) {
            let cs = if c == start_extent { start_offset } else { 0 };
            let ce = if c == end_extent {
                ((end - 1) % EXTENT_SIZE as u64 + 1) as usize
            } else {
                EXTENT_SIZE
            };
            (cs, ce)
        };
        let mut pieces: Vec<PlannedPiece> = Vec::new();
        let mut nominate: Vec<Segid> = Vec::new();
        let mut crossings: Vec<(Segid, Segid)> = Vec::new();
        let mut prev_nonram: Option<(Segid, u64)> = None;
        let track = is_demand && self.nominations_enabled.load(Ordering::Relaxed);
        let mut extent = start_extent;
        while extent <= end_extent {
            let Some(first) = loc_map.get(&extent).copied() else {
                let (cs, ce) = slice(extent);
                pieces.push(PlannedPiece::Bytes(Bytes::copy_from_slice(
                    &ZERO_EXTENT[cs..ce],
                )));
                prev_nonram = None;
                extent += 1;
                continue;
            };
            let mut n = 1u64;
            let mut total_len = first.byte_len as u64;
            let mut prev = first;
            let mut slots = vec![(id, extent)];
            while extent + n <= end_extent {
                match loc_map.get(&(extent + n)).copied() {
                    Some(loc)
                        if loc.segid == prev.segid
                            && loc.frame_index == prev.frame_index + 1
                            && loc.byte_offset == prev.byte_offset + prev.byte_len as u64 =>
                    {
                        total_len += loc.byte_len as u64;
                        prev = loc;
                        slots.push((id, extent + n));
                        n += 1;
                    }
                    _ => break,
                }
            }
            let cached_frames: Option<Vec<Bytes>> = (0..n)
                .map(|i| {
                    let idx = extent + i;
                    self.decoded_get(id, idx, loc_map[&idx])
                })
                .collect();
            match cached_frames {
                Some(frames) => {
                    prev_nonram = None;
                    pieces.push(PlannedPiece::Ready {
                        start_extent: extent,
                        frames,
                    });
                }
                None => match self.read_frames_in_ram(
                    first.segid,
                    first.byte_offset,
                    total_len as u32,
                    first.frame_index,
                    &slots,
                )? {
                    Some(frames) => {
                        prev_nonram = None;
                        pieces.push(PlannedPiece::Ready {
                            start_extent: extent,
                            frames,
                        });
                    }
                    None => {
                        if track {
                            if nominate.len() < NOMINATE_PER_CALL_CAP
                                && !nominate.contains(&first.segid)
                            {
                                nominate.push(first.segid);
                            }
                            if let Some((prev_segid, prev_end)) = prev_nonram
                                && prev_end == extent
                                && crossings.len() < PAIR_BUMPS_PER_CALL
                            {
                                let pair = PairStats::key(prev_segid, first.segid);
                                if !crossings.contains(&pair) {
                                    crossings.push(pair);
                                }
                            }
                        }
                        prev_nonram = Some((first.segid, extent + n));
                        pieces.push(PlannedPiece::OnStore(OnStoreRun {
                            start_extent: extent,
                            first,
                            total_len: total_len as u32,
                            slots,
                        }));
                    }
                },
            }
            extent += n;
        }

        let mut recorder = metrics::ReadRunRecorder::new(length, end_extent - start_extent + 1);
        let unique: HashSet<Segid> = pieces
            .iter()
            .filter_map(|piece| match piece {
                PlannedPiece::OnStore(run) => Some(run.first.segid),
                _ => None,
            })
            .collect();
        let on_store_runs = pieces
            .iter()
            .filter(|piece| matches!(piece, PlannedPiece::OnStore(_)))
            .count() as u64;
        let on_store_bytes = pieces
            .iter()
            .map(|piece| match piece {
                PlannedPiece::OnStore(run) => run.total_len as u64,
                _ => 0,
            })
            .sum();
        recorder.record_plan(
            pieces.len() as u64,
            unique.len() as u64,
            on_store_runs,
            on_store_bytes,
        );
        let recorder = Arc::new(recorder);

        #[cfg(feature = "failpoints")]
        {
            fail_point!(fp::READ_AFTER_RESOLVE_BEFORE_FETCH);
            fp::widen(fp::READ_AFTER_RESOLVE_BEFORE_FETCH).await;
        }

        let pending: Vec<(usize, OnStoreRun)> = pieces
            .iter()
            .enumerate()
            .filter_map(|(index, piece)| match piece {
                PlannedPiece::OnStore(run) => Some((index, run.clone())),
                _ => None,
            })
            .collect();
        let permits = Arc::new(Semaphore::new(PARALLEL_EXTENT_OPS));
        let fetched = futures::stream::iter(pending)
            .map(|(index, run)| {
                let permits = Arc::clone(&permits);
                let recorder = Arc::clone(&recorder);
                async move {
                    let _permit = permits
                        .acquire_owned()
                        .await
                        .expect("read-run semaphore is never closed");
                    let _guard = recorder.enter_fetch();
                    let frames = self.fetch_on_store_run(&run).await;
                    (index, frames)
                }
            })
            .buffer_unordered(PARALLEL_EXTENT_OPS)
            .collect::<Vec<_>>()
            .await;

        let mut fetched_frames = HashMap::new();
        let mut fetch_error = None;
        for (index, frames) in fetched {
            match frames {
                Ok(frames) => {
                    fetched_frames.insert(index, frames);
                }
                Err(error) if fetch_error.is_none() => fetch_error = Some(error),
                Err(_) => {}
            }
        }
        let run_metrics = recorder.finish();
        #[cfg(test)]
        {
            *self
                .last_read_metrics
                .lock()
                .expect("last_read_metrics never poisoned") = Some(run_metrics);
        }
        #[cfg(not(test))]
        let _ = run_metrics;
        if let Some(error) = fetch_error {
            return Err(error);
        }

        let mut result = BytesMut::with_capacity(length as usize);
        for (index, piece) in pieces.into_iter().enumerate() {
            match piece {
                PlannedPiece::Bytes(bytes) => result.extend_from_slice(&bytes),
                PlannedPiece::Ready {
                    start_extent: run_start,
                    frames,
                } => {
                    for (i, frame) in frames.iter().enumerate() {
                        let idx = run_start + i as u64;
                        Self::validate_extent_frame(id, idx, frame)?;
                        self.decoded_insert(id, idx, loc_map[&idx], frame.clone());
                        let (cs, ce) = slice(idx);
                        result.extend_from_slice(&frame[cs..ce]);
                    }
                }
                PlannedPiece::OnStore(run) => {
                    let frames = fetched_frames
                        .remove(&index)
                        .expect("every on-store run was fetched");
                    for (i, frame) in frames.iter().enumerate() {
                        let idx = run.start_extent + i as u64;
                        Self::validate_extent_frame(id, idx, frame)?;
                        self.decoded_insert(id, idx, loc_map[&idx], frame.clone());
                        let (cs, ce) = slice(idx);
                        result.extend_from_slice(&frame[cs..ce]);
                    }
                }
            }
        }
        if nominate.len() >= NOMINATE_MIN_FANOUT {
            let mut noms = self.nominations.lock().unwrap();
            for segid in nominate {
                noms.push(segid);
            }
        }
        if !crossings.is_empty() {
            let round = self.gc_round.load(Ordering::Relaxed);
            let now = Instant::now();
            let mut stats = self.pair_stats.lock().unwrap();
            for (a, b) in crossings {
                stats.bump(a, b, round, now);
            }
        }
        Ok(result.freeze())
    }

    async fn fetch_on_store_run(&self, run: &OnStoreRun) -> Result<Vec<Bytes>, FsError> {
        match self
            .segments
            .read_run(
                run.first.segid,
                run.first.byte_offset,
                run.total_len,
                run.first.frame_index,
                &run.slots,
            )
            .await
        {
            Ok(frames) => Ok(frames),
            Err(_) => {
                let mut frames = Vec::with_capacity(run.slots.len());
                for (fid, fext) in &run.slots {
                    frames.push(match self.get(*fid, *fext).await? {
                        Some(data) => data,
                        None => Bytes::from_static(ZERO_EXTENT),
                    });
                }
                Ok(frames)
            }
        }
    }
}

pub(super) mod metrics;
use metrics::{OnStoreRun, PlannedPiece};

#[cfg(test)]
mod tests;
