use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use foyer::{Cache, HybridCache};
use futures::FutureExt;
use futures::StreamExt;
use futures::future::{BoxFuture, Shared};
use futures::stream::{self, BoxStream};
use object_store::path::Path;
use object_store::{
    Attributes, CopyOptions, GetOptions, GetRange, GetResult, GetResultPayload, ListResult,
    MultipartUpload, ObjectMeta, ObjectStore, PutMultipartOptions, PutOptions, PutPayload,
    PutResult, RenameOptions,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt::{self, Debug, Display, Formatter};
use std::ops::Range;
use std::sync::Arc;
use std::sync::Mutex;

use crate::alloc_rss;

/// Marker in `GetOptions::extensions`: GC/reclaim reads that must not
/// re-admit segment parts into the user cache.
#[derive(Clone, Copy, Debug)]
pub struct SkipPartsCache;

pub const DEFAULT_PART_SIZE_BYTES: usize = 128 * 1024;
const HEADS_CAPACITY_ENTRIES: usize = 16 * 1024;
const ACCESS_TRACKER_CAPACITY: usize = 8 * 1024;

const FETCH_WINDOW_MIN: usize = 128 * 1024;
const FETCH_WINDOW_MAX: usize = 8 * 1024 * 1024;
const MAX_STREAMS: usize = 4;
/// Minimum confirmed-stream window relative to the random-access minimum. The
/// live SFTP profile therefore promotes 1 MiB to 16 MiB after sequential access
/// is proven, while an unproven/random stream stays at 1 MiB.
const SEQUENTIAL_FETCH_WINDOW_MULTIPLIER: usize = 15;
/// Look-ahead horizon for a proven sequential stream. Each window remains a
/// separate backend GET so SFTP can use several physical sessions at once.
const PREFETCH_DEPTH_WINDOWS: usize = 4;

/// Resolved cold-read geometry for one backend. `Default` is the tuning used by
/// every backend that does not publish a profile of its own.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PrefetchProfile {
    pub part_size_bytes: usize,
    pub fetch_window_min_bytes: usize,
    /// Window floor once a stream is confirmed sequential. Never below
    /// `fetch_window_min_bytes`, never above `fetch_window_max_bytes`.
    pub sequential_fetch_window_min_bytes: usize,
    pub fetch_window_max_bytes: usize,
}

impl Default for PrefetchProfile {
    fn default() -> Self {
        Self {
            part_size_bytes: DEFAULT_PART_SIZE_BYTES,
            fetch_window_min_bytes: FETCH_WINDOW_MIN,
            sequential_fetch_window_min_bytes: FETCH_WINDOW_MIN,
            fetch_window_max_bytes: FETCH_WINDOW_MAX,
        }
    }
}

impl PrefetchProfile {
    /// Tuning for a high-latency backend: the caller sets the part size and the
    /// window bounds, and a confirmed sequential stream starts at the multiplied
    /// minimum, capped by the maximum.
    pub fn tuned(
        part_size_bytes: usize,
        fetch_window_min_bytes: usize,
        fetch_window_max_bytes: usize,
    ) -> Self {
        Self {
            part_size_bytes,
            fetch_window_min_bytes,
            sequential_fetch_window_min_bytes: fetch_window_min_bytes
                .saturating_mul(SEQUENTIAL_FETCH_WINDOW_MULTIPLIER)
                .min(fetch_window_max_bytes),
            fetch_window_max_bytes,
        }
    }
}

type PartId = usize;

/// One window GET shared by every reader whose part falls inside it. Resolves
/// to `(base part id, window bytes)`; a joiner slices out its part. The error
/// is Arc-wrapped so the output is `Clone`, which `Shared` requires.
type SharedFetch = Shared<BoxFuture<'static, Result<(PartId, Bytes), Arc<object_store::Error>>>>;

/// Single-flight registry for all window fetches, demand and prefetch alike:
/// every part covered by an in-flight GET maps to its shared fetch, so anything
/// touching those parts joins or trims around it instead of duplicating the
/// GET. A plain mutex rather than a sharded map so a window can scan and
/// register its parts atomically; never held across an await.
type Fetches = Arc<Mutex<HashMap<PartKey, SharedFetch>>>;

#[derive(Clone)]
struct Stream {
    last_offset: u64,
    last_end: u64,
    fetch_window: usize,
    fetched_until: u64,
    scheduled_until: u64,
    /// Successful fetches that landed beyond a hole. They cannot advance the
    /// confirmed frontier until every byte before them is also covered.
    completed: Vec<Range<u64>>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct RecordDecision {
    fetch_window: usize,
    async_prefetch: Vec<AsyncPrefetch>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct AsyncPrefetch {
    start: u64,
    size: usize,
}

struct AccessHistory {
    streams: [Stream; MAX_STREAMS],
    len: usize,
    stride_limit: u64,
    fetch_window_min: usize,
    sequential_fetch_window_min: usize,
    fetch_window_max: usize,
}

impl AccessHistory {
    #[cfg(test)]
    fn new(part_size: usize) -> Self {
        Self::with_windows(
            part_size,
            FETCH_WINDOW_MIN,
            FETCH_WINDOW_MIN,
            FETCH_WINDOW_MAX,
        )
    }

    fn with_windows(
        part_size: usize,
        fetch_window_min: usize,
        sequential_fetch_window_min: usize,
        fetch_window_max: usize,
    ) -> Self {
        Self {
            streams: std::array::from_fn(|_| Stream {
                last_offset: u64::MAX,
                last_end: u64::MAX,
                fetch_window: fetch_window_min,
                fetched_until: 0,
                scheduled_until: 0,
                completed: Vec::new(),
            }),
            len: 0,
            stride_limit: part_size as u64 * 4,
            fetch_window_min,
            sequential_fetch_window_min,
            fetch_window_max,
        }
    }

    fn record(&mut self, offset: u64, len: u64) -> RecordDecision {
        if let Some(i) = self.find_stream(offset) {
            let s = &mut self.streams[i];
            s.last_offset = offset;
            s.last_end = offset.saturating_add(len);
            s.fetch_window = if self.sequential_fetch_window_min > self.fetch_window_min {
                // High-latency backends use a fixed one-wave window and obtain
                // throughput from several concurrent ranges/sessions instead
                // of growing one range until it monopolizes a session.
                self.sequential_fetch_window_min
            } else {
                (s.fetch_window * 2).min(self.fetch_window_max)
            };

            // Refill the prefetch frontier to PREFETCH_DEPTH_WINDOWS ahead of the
            // read, once the stream has proven sequential (window ramped past the
            // minimum) and the frontier is ahead of the read. These are candidates
            // only; the caller records scheduled coverage only for windows it
            // actually arranged, and a failed task rolls that credit back.
            let mut async_prefetch = Vec::new();
            let frontier = s.fetched_until.max(s.scheduled_until);
            if s.fetch_window > self.fetch_window_min && frontier > offset {
                let window = s.fetch_window as u64;
                let target =
                    offset.saturating_add((PREFETCH_DEPTH_WINDOWS as u64).saturating_mul(window));
                let mut cursor = frontier;
                while cursor < target && async_prefetch.len() < PREFETCH_DEPTH_WINDOWS {
                    async_prefetch.push(AsyncPrefetch {
                        start: cursor,
                        size: s.fetch_window,
                    });
                    cursor = cursor.saturating_add(window);
                }
            }

            return RecordDecision {
                fetch_window: s.fetch_window,
                async_prefetch,
            };
        }

        // FUSE readahead pipelines 9P READs, which can land slightly out of
        // order. Absorb a read a little behind a stream's head without touching
        // the head or ramping; a genuine backward seek (beyond the slack) still
        // resets below. Min window on purpose: a straggler's bytes are already
        // cached or in flight, and a random read that merely lands in the slack
        // zone must not inherit an 8 MiB window over cold bytes.
        if self.find_lagging_stream(offset, len).is_some() {
            return RecordDecision {
                fetch_window: self.fetch_window_min,
                async_prefetch: Vec::new(),
            };
        }

        let slot = if self.len < MAX_STREAMS {
            let s = self.len;
            self.len += 1;
            s
        } else {
            self.weakest_slot()
        };

        self.streams[slot] = Stream {
            last_offset: offset,
            last_end: offset.saturating_add(len),
            fetch_window: self.fetch_window_min,
            fetched_until: offset,
            scheduled_until: offset,
            completed: Vec::new(),
        };

        RecordDecision {
            fetch_window: self.fetch_window_min,
            async_prefetch: Vec::new(),
        }
    }

    /// Record a completed fetch for every related stream. Completions may be
    /// out of order, so only intervals contiguous with `fetched_until` advance
    /// the frontier; later intervals wait in `completed` until a gap is filled.
    /// The relation check is looser than `find_stream` because a concurrent
    /// window can complete after its triggering request has fallen behind the
    /// stream head.
    fn note_fetch(&mut self, offset: u64, fetch_end: u64) {
        for s in &mut self.streams[..self.len] {
            if fetch_end <= offset
                || (offset > s.fetched_until.max(s.last_end)
                    && !s
                        .completed
                        .iter()
                        .any(|range| offset <= range.end && fetch_end >= range.start))
            {
                continue;
            }

            s.completed.push(offset..fetch_end);
            s.completed.sort_unstable_by_key(|range| range.start);

            let mut frontier = s.fetched_until;
            let mut pending: Vec<Range<u64>> = Vec::with_capacity(s.completed.len());
            for range in s.completed.drain(..) {
                if range.end <= frontier {
                    continue;
                }
                if range.start <= frontier {
                    frontier = frontier.max(range.end);
                } else if let Some(last) = pending.last_mut()
                    && range.start <= last.end
                {
                    last.end = last.end.max(range.end);
                } else {
                    pending.push(range);
                }
            }
            s.completed = pending;
            s.fetched_until = frontier;
        }
    }

    fn note_schedule(&mut self, offset: u64, scheduled_end: u64) {
        for s in &mut self.streams[..self.len] {
            if scheduled_end > s.scheduled_until && offset <= s.scheduled_until.max(s.last_end) {
                s.scheduled_until = scheduled_end;
            }
        }
    }

    fn note_schedule_failure(&mut self, offset: u64, failed_start: u64) {
        for s in &mut self.streams[..self.len] {
            if failed_start < s.scheduled_until && offset <= s.scheduled_until.max(s.last_end) {
                s.scheduled_until = failed_start.max(s.fetched_until);
            }
        }
    }

    fn weakest_slot(&self) -> usize {
        let mut min_idx = 0;
        let mut min_window = usize::MAX;
        for i in 0..self.len {
            if self.streams[i].fetch_window < min_window {
                min_window = self.streams[i].fetch_window;
                min_idx = i;
            }
        }
        min_idx
    }

    /// Match `offset` to a stream: at or past its last request start, within
    /// `stride_limit` of where that request ended. Measured from the end so
    /// back-to-back 1 MiB coalesced runs stay on their stream: their
    /// start-to-start stride is huge, their end-to-start gap zero.
    fn find_stream(&self, offset: u64) -> Option<usize> {
        let mut best = None;
        let mut best_dist = u64::MAX;
        for i in 0..self.len {
            let s = &self.streams[i];
            if offset >= s.last_offset && offset.saturating_sub(s.last_end) <= self.stride_limit {
                let dist = offset.saturating_sub(s.last_end);
                if dist < best_dist {
                    best = Some(i);
                    best_dist = dist;
                }
            }
        }
        best
    }

    /// Match a read arriving behind a stream's head, within a reorder slack
    /// scaled by request size. Point reads get no slack, so a small backward
    /// seek still resets its stream.
    fn find_lagging_stream(&self, offset: u64, len: u64) -> Option<usize> {
        if len == 0 {
            return None;
        }
        let slack = len.saturating_mul(8);
        let mut best = None;
        let mut best_lag = u64::MAX;
        for i in 0..self.len {
            let s = &self.streams[i];
            if offset < s.last_offset {
                let lag = s.last_offset - offset;
                if lag <= slack && lag < best_lag {
                    best = Some(i);
                    best_lag = lag;
                }
            }
        }
        best
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct PartKey {
    location: String,
    // Persistent cache entries must not outlive their byte geometry or the
    // exact object revision that supplied them.
    part_size_bytes: usize,
    generation: CacheGeneration,
    part_id: PartId,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
enum CacheGeneration {
    Versioned {
        e_tag: Option<String>,
        version: Option<String>,
    },
    Unversioned {
        // A store without a version or ETag may reuse mtime+size on overwrite.
        // Keep those parts useful in-process but never recover them into a new
        // prefetching store instance.
        cache_instance: uuid::Uuid,
        size: u64,
        modified_nanos: i64,
    },
}

impl CacheGeneration {
    fn from_meta(meta: &ObjectMeta, cache_instance: uuid::Uuid) -> Self {
        if meta.e_tag.is_some() || meta.version.is_some() {
            Self::Versioned {
                e_tag: meta.e_tag.clone(),
                version: meta.version.clone(),
            }
        } else {
            Self::Unversioned {
                cache_instance,
                size: meta.size,
                modified_nanos: meta
                    .last_modified
                    .timestamp_nanos_opt()
                    .unwrap_or_else(|| meta.last_modified.timestamp_millis() * 1_000_000),
            }
        }
    }

    fn from_put_result(result: &PutResult) -> Option<Self> {
        (result.e_tag.is_some() || result.version.is_some()).then(|| Self::Versioned {
            e_tag: result.e_tag.clone(),
            version: result.version.clone(),
        })
    }
}

impl PartKey {
    fn new(
        location: &Path,
        part_size_bytes: usize,
        generation: &CacheGeneration,
        part_id: PartId,
    ) -> Self {
        Self {
            location: location.as_ref().to_string(),
            part_size_bytes,
            generation: generation.clone(),
            part_id,
        }
    }
}

#[derive(Clone)]
struct CachedHead {
    meta: ObjectMeta,
    attributes: Attributes,
}

pub struct PrefetchingObjectStore {
    inner: Arc<dyn ObjectStore>,
    part_size_bytes: usize,
    fetch_window_min_bytes: usize,
    sequential_fetch_window_min_bytes: usize,
    fetch_window_max_bytes: usize,
    parts: HybridCache<PartKey, Bytes>,
    heads: Cache<Path, Arc<CachedHead>>,
    /// Last object identity observed in this store instance. Kept separately
    /// from evictable heads so a head miss can safely inspect generation-keyed
    /// parts before deciding whether it needs a data GET or only a HEAD.
    generations: Cache<Path, CacheGeneration>,
    access_tracker: Cache<Path, Arc<Mutex<AccessHistory>>>,
    fetches: Fetches,
    cache_instance: uuid::Uuid,
    /// Clean-cache / cgroup-slack RSS ceiling. `0` disables admission.
    admission_cap_bytes: u64,
    /// Highest part_id+1 seen per object, so evict can form PartKeys
    /// after a seal warm that never saved a head.
    part_counts: Cache<Path, usize>,
}

/// Clonable handles for window fetches, which outlive `&self` (demand fetch
/// futures and prefetch tasks are `'static`).
#[derive(Clone)]
struct FetchCtx {
    inner: Arc<dyn ObjectStore>,
    parts: HybridCache<PartKey, Bytes>,
    heads: Cache<Path, Arc<CachedHead>>,
    generations: Cache<Path, CacheGeneration>,
    access_tracker: Cache<Path, Arc<Mutex<AccessHistory>>>,
    fetches: Fetches,
    part_size_bytes: usize,
    cache_instance: uuid::Uuid,
    admission_cap_bytes: u64,
    part_counts: Cache<Path, usize>,
}

impl FetchCtx {
    fn admission_ctx(&self) -> AdmissionCtx<'_> {
        AdmissionCtx {
            parts: &self.parts,
            part_counts: &self.part_counts,
            admission_cap_bytes: self.admission_cap_bytes,
            part_size_bytes: self.part_size_bytes,
        }
    }
}

/// Held by the leader of a window fetch; clears its registered part slots on
/// drop, whether the fetch completed or the awaiting read was cancelled.
/// Joiners hold `None`.
struct FetchGuard {
    map: Fetches,
    keys: Vec<PartKey>,
}

impl Drop for FetchGuard {
    fn drop(&mut self) {
        let mut map = self.map.lock().unwrap();
        for key in &self.keys {
            map.remove(key);
        }
    }
}

/// How a window fetch resolves against the registry: lead a new GET, join one
/// already in flight, or nothing to do.
enum WindowPlan {
    Lead {
        shared: SharedFetch,
        guard: FetchGuard,
        end_part: PartId,
    },
    Join(SharedFetch),
    Covered,
}

struct AdmissionCtx<'a> {
    parts: &'a HybridCache<PartKey, Bytes>,
    part_counts: &'a Cache<Path, usize>,
    admission_cap_bytes: u64,
    part_size_bytes: usize,
}

impl AdmissionCtx<'_> {
    fn admit_part(
        &self,
        location: &Path,
        generation: &CacheGeneration,
        part_id: PartId,
        bytes: Bytes,
    ) {
        if alloc_rss::over_rss_cap_of(self.admission_cap_bytes) {
            return;
        }
        self.parts.insert(
            PartKey::new(location, self.part_size_bytes, generation, part_id),
            bytes,
        );
        let n = part_id + 1;
        let cur = self
            .part_counts
            .get(location)
            .map(|e| *e.value())
            .unwrap_or(0);
        if n > cur {
            self.part_counts.insert(location.clone(), n);
        }
    }

    async fn save_parts_stream<S>(
        &self,
        location: &Path,
        generation: &CacheGeneration,
        mut stream: S,
        start_part_number: PartId,
        mut skip: usize,
    ) -> object_store::Result<()>
    where
        S: stream::Stream<Item = Result<Bytes, object_store::Error>> + Unpin,
    {
        let mut buffer = BytesMut::new();
        let mut part_number = start_part_number;
        while let Some(chunk) = stream.next().await {
            let mut chunk = chunk?;
            if skip > 0 {
                let n = skip.min(chunk.len());
                chunk = chunk.slice(n..);
                skip -= n;
                if chunk.is_empty() {
                    continue;
                }
            }
            buffer.extend_from_slice(&chunk);
            while buffer.len() >= self.part_size_bytes {
                let to_write = buffer.split_to(self.part_size_bytes);
                self.admit_part(
                    location,
                    generation,
                    part_number,
                    Bytes::copy_from_slice(&to_write),
                );
                part_number += 1;
            }
        }
        if !buffer.is_empty() {
            self.admit_part(
                location,
                generation,
                part_number,
                Bytes::copy_from_slice(&buffer),
            );
        }
        Ok(())
    }
}

#[derive(Clone)]
struct EvictionCtx {
    parts: HybridCache<PartKey, Bytes>,
    heads: Cache<Path, Arc<CachedHead>>,
    generations: Cache<Path, CacheGeneration>,
    access_tracker: Cache<Path, Arc<Mutex<AccessHistory>>>,
    part_counts: Cache<Path, usize>,
    part_size_bytes: usize,
    cache_instance: uuid::Uuid,
}

impl EvictionCtx {
    fn evict_location(&self, location: &Path) {
        let from_counts = self
            .part_counts
            .get(location)
            .map(|e| *e.value())
            .unwrap_or(0);
        let from_head = self
            .heads
            .get(location)
            .map(|e| e.value().meta.size.div_ceil(self.part_size_bytes as u64) as usize)
            .unwrap_or(0);
        let generation = self
            .generations
            .get(location)
            .map(|e| e.value().clone())
            .or_else(|| {
                self.heads
                    .get(location)
                    .map(|e| CacheGeneration::from_meta(&e.value().meta, self.cache_instance))
            });
        let from_gen = match &generation {
            Some(CacheGeneration::Unversioned { size, .. }) => {
                size.div_ceil(self.part_size_bytes as u64) as usize
            }
            _ => 0,
        };
        let n = from_counts.max(from_head).max(from_gen);
        if let Some(generation) = generation {
            for part_id in 0..n {
                self.parts.remove(&PartKey::new(
                    location,
                    self.part_size_bytes,
                    &generation,
                    part_id,
                ));
            }
        }
        self.heads.remove(location);
        self.generations.remove(location);
        self.access_tracker.remove(location);
        self.part_counts.remove(location);
    }
}

impl PrefetchingObjectStore {
    /// Build with the crate default tuning.
    pub fn new(inner: Arc<dyn ObjectStore>, parts: HybridCache<PartKey, Bytes>) -> Self {
        Self::with_profile(inner, parts, PrefetchProfile::default())
    }

    /// Build over a backend's resolved [`PrefetchProfile`]; `Default` gives the
    /// crate tuning.
    pub fn with_profile(
        inner: Arc<dyn ObjectStore>,
        parts: HybridCache<PartKey, Bytes>,
        profile: PrefetchProfile,
    ) -> Self {
        Self::with_windows(
            inner,
            parts,
            profile.part_size_bytes,
            profile.fetch_window_min_bytes,
            profile.sequential_fetch_window_min_bytes,
            profile.fetch_window_max_bytes,
        )
    }

    #[cfg(test)]
    pub fn with_options(
        inner: Arc<dyn ObjectStore>,
        parts: HybridCache<PartKey, Bytes>,
        part_size_bytes: usize,
    ) -> Self {
        Self::with_profile(
            inner,
            parts,
            PrefetchProfile {
                part_size_bytes,
                ..PrefetchProfile::default()
            },
        )
    }

    fn with_windows(
        inner: Arc<dyn ObjectStore>,
        parts: HybridCache<PartKey, Bytes>,
        part_size_bytes: usize,
        fetch_window_min_bytes: usize,
        sequential_fetch_window_min_bytes: usize,
        fetch_window_max_bytes: usize,
    ) -> Self {
        assert!(
            part_size_bytes > 0 && part_size_bytes.is_multiple_of(1024),
            "part_size_bytes must be a positive multiple of 1024"
        );
        assert!(
            fetch_window_min_bytes >= part_size_bytes
                && fetch_window_min_bytes.is_multiple_of(1024),
            "fetch_window_min_bytes must be at least one cache part and a multiple of 1024"
        );
        assert!(
            fetch_window_max_bytes >= fetch_window_min_bytes
                && fetch_window_max_bytes.is_multiple_of(1024),
            "fetch_window_max_bytes must be at least the minimum and a multiple of 1024"
        );
        assert!(
            sequential_fetch_window_min_bytes >= fetch_window_min_bytes
                && sequential_fetch_window_min_bytes <= fetch_window_max_bytes
                && sequential_fetch_window_min_bytes.is_multiple_of(1024),
            "sequential_fetch_window_min_bytes must be within the fetch window bounds and a multiple of 1024"
        );

        let heads = foyer::CacheBuilder::new(HEADS_CAPACITY_ENTRIES)
            .with_name("zerofs-object-prefetch-heads")
            .with_eviction_config(foyer::S3FifoConfig::default())
            .build();
        let access_tracker = foyer::CacheBuilder::new(ACCESS_TRACKER_CAPACITY)
            .with_name("zerofs-object-prefetch-access-tracker")
            .with_eviction_config(foyer::S3FifoConfig::default())
            .build();
        let generations = foyer::CacheBuilder::new(HEADS_CAPACITY_ENTRIES)
            .with_name("zerofs-object-prefetch-generations")
            .with_eviction_config(foyer::S3FifoConfig::default())
            .build();

        Self {
            inner,
            part_size_bytes,
            fetch_window_min_bytes,
            sequential_fetch_window_min_bytes,
            fetch_window_max_bytes,
            parts,
            heads,
            generations,
            access_tracker,
            fetches: Arc::new(Mutex::new(HashMap::new())),
            cache_instance: uuid::Uuid::new_v4(),
            admission_cap_bytes: 0,
            part_counts: foyer::CacheBuilder::new(HEADS_CAPACITY_ENTRIES)
                .with_name("zerofs-object-prefetch-part-counts")
                .with_eviction_config(foyer::S3FifoConfig::default())
                .build(),
        }
    }

    /// RSS admission ceiling (configured clean-cache total). `0` leaves
    /// inserts ungated (tests).
    pub fn with_admission_cap(mut self, cap_bytes: u64) -> Self {
        self.admission_cap_bytes = cap_bytes;
        self
    }

    fn ctx(&self) -> FetchCtx {
        FetchCtx {
            inner: self.inner.clone(),
            parts: self.parts.clone(),
            heads: self.heads.clone(),
            generations: self.generations.clone(),
            access_tracker: self.access_tracker.clone(),
            fetches: self.fetches.clone(),
            part_size_bytes: self.part_size_bytes,
            cache_instance: self.cache_instance,
            admission_cap_bytes: self.admission_cap_bytes,
            part_counts: self.part_counts.clone(),
        }
    }

    fn admission_ctx(&self) -> AdmissionCtx<'_> {
        AdmissionCtx {
            parts: &self.parts,
            part_counts: &self.part_counts,
            admission_cap_bytes: self.admission_cap_bytes,
            part_size_bytes: self.part_size_bytes,
        }
    }

    fn eviction_ctx(&self) -> EvictionCtx {
        EvictionCtx {
            parts: self.parts.clone(),
            heads: self.heads.clone(),
            generations: self.generations.clone(),
            access_tracker: self.access_tracker.clone(),
            part_counts: self.part_counts.clone(),
            part_size_bytes: self.part_size_bytes,
            cache_instance: self.cache_instance,
        }
    }

    fn save_head(&self, location: &Path, meta: &ObjectMeta, attrs: &Attributes) {
        self.generations.insert(
            location.clone(),
            CacheGeneration::from_meta(meta, self.cache_instance),
        );
        self.heads.insert(
            location.clone(),
            Arc::new(CachedHead {
                meta: meta.clone(),
                attributes: attrs.clone(),
            }),
        );
    }

    fn read_head(&self, location: &Path) -> Option<(ObjectMeta, Attributes)> {
        self.heads
            .get(location)
            .map(|entry| (entry.value().meta.clone(), entry.value().attributes.clone()))
    }

    #[cfg(test)]
    async fn cached_part(&self, location: &Path, part_id: PartId) -> Option<Bytes> {
        let generation = self
            .read_head(location)
            .map(|(meta, _)| CacheGeneration::from_meta(&meta, self.cache_instance))?;
        self.parts
            .get(&PartKey::new(
                location,
                self.part_size_bytes,
                &generation,
                part_id,
            ))
            .await
            .ok()
            .flatten()
            .map(|entry| entry.value().clone())
    }

    fn invalidate(&self, location: &Path) {
        self.heads.remove(location);
        self.generations.remove(location);
    }

    /// Drop every cached part for `location` plus heads / generations /
    /// access_tracker. Called from delete so a reclaim that only drops
    /// heads cannot keep charging foyer for the dead 32 MiB parts.
    ///
    /// Only exercised through `delete` and tests today; the binary target
    /// (which re-declares these modules) would otherwise flag it dead.
    #[allow(dead_code)]
    pub fn evict_location(&self, location: &Path) {
        self.eviction_ctx().evict_location(location);
    }

    /// Backend GET that does not `save_get_result`, `read_part`, or
    /// `spawn_async_prefetch`. GC/compaction use this (or `SkipPartsCache`).
    pub async fn get_opts_uncached(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        self.inner.get_opts(location, options).await
    }

    /// ObjectStore delete that evicts first so a failed backend delete
    /// still uncharges the user cache.
    ///
    /// Part of the lib public API; unused inside the binary target.
    #[allow(dead_code)]
    pub async fn delete(&self, location: &Path) -> object_store::Result<()> {
        self.evict_location(location);
        object_store::ObjectStoreExt::delete(&*self.inner, location).await
    }

    fn admit_part(
        &self,
        location: &Path,
        generation: &CacheGeneration,
        part_id: PartId,
        bytes: Bytes,
    ) {
        self.admission_ctx()
            .admit_part(location, generation, part_id, bytes);
    }

    fn record_access(&self, location: &Path, offset: u64, len: u64) -> RecordDecision {
        let entry = self
            .access_tracker
            .get(location)
            .map(|e| e.value().clone())
            .unwrap_or_else(|| {
                let hist = Arc::new(Mutex::new(AccessHistory::with_windows(
                    self.part_size_bytes,
                    self.fetch_window_min_bytes,
                    self.sequential_fetch_window_min_bytes,
                    self.fetch_window_max_bytes,
                )));
                self.access_tracker.insert(location.clone(), hist.clone());
                hist
            });
        let mut history = entry.lock().unwrap();
        history.record(offset, len)
    }

    fn note_schedule(&self, location: &Path, offset: u64, scheduled_end: u64) {
        if let Some(entry) = self.access_tracker.get(location) {
            entry
                .value()
                .clone()
                .lock()
                .unwrap()
                .note_schedule(offset, scheduled_end);
        }
    }

    fn note_fetch(&self, location: &Path, offset: u64, fetched_end: u64) {
        if let Some(entry) = self.access_tracker.get(location) {
            entry
                .value()
                .clone()
                .lock()
                .unwrap()
                .note_fetch(offset, fetched_end);
        }
    }

    async fn cached_head(&self, location: &Path) -> object_store::Result<ObjectMeta> {
        Ok(self.cached_head_with_attrs(location).await?.0)
    }

    async fn cached_head_with_attrs(
        &self,
        location: &Path,
    ) -> object_store::Result<(ObjectMeta, Attributes)> {
        if let Some((meta, attrs)) = self.read_head(location) {
            return Ok((meta, attrs));
        }
        let result = self
            .inner
            .get_opts(
                location,
                GetOptions {
                    range: None,
                    head: true,
                    ..Default::default()
                },
            )
            .await?;
        // Save only the head: a backend that ignores `head` (InMemory) hands
        // back the whole object, which must not bypass the parts discipline.
        self.save_head(location, &result.meta, &result.attributes);
        Ok((result.meta, result.attributes))
    }

    /// Establish metadata for a data read without an unconditional HEAD. A
    /// genuinely cold store learns the generation from the first aligned GET
    /// and saves that same payload as generation-keyed parts. If this process
    /// already knows the generation (for example, only the head entry was
    /// evicted), it may safely reuse/coordinate those parts first.
    async fn discover_data_meta(
        &self,
        location: &Path,
        mut opts: GetOptions,
        fetch_window: usize,
    ) -> object_store::Result<(ObjectMeta, Attributes)> {
        if let Some(head) = self.read_head(location) {
            return Ok(head);
        }

        if let Some(generation) = self
            .generations
            .get(location)
            .map(|entry| entry.value().clone())
        {
            if let Some(GetRange::Bounded(requested)) = &opts.range {
                let aligned = match self
                    .align_get_range(&GetRange::Bounded(requested.clone()), fetch_window)
                {
                    GetRange::Bounded(range) => range,
                    _ => unreachable!("bounded ranges align to bounded"),
                };
                let part_size = self.part_size_bytes as u64;
                let start_part: PartId = (aligned.start / part_size)
                    .try_into()
                    .expect("part id exceeds usize");
                let end_part: PartId = aligned
                    .end
                    .div_ceil(part_size)
                    .try_into()
                    .expect("part id exceeds usize");
                let needed_end: PartId = requested
                    .end
                    .div_ceil(part_size)
                    .try_into()
                    .expect("part id exceeds usize");
                let ctx = self.ctx();
                match Self::plan_window(
                    &ctx,
                    location,
                    &generation,
                    start_part,
                    end_part - start_part,
                    Some(needed_end),
                ) {
                    WindowPlan::Lead { shared, guard, .. } => {
                        let _guard = guard;
                        let _ = shared.await;
                    }
                    WindowPlan::Join(shared) => {
                        let _ = shared.await;
                    }
                    WindowPlan::Covered => {}
                }
                if let Some(head) = self.read_head(location) {
                    return Ok(head);
                }
            }

            // Cache coverage alone cannot reconstruct ObjectMeta. A HEAD is
            // sufficient here; this is not a cold data miss and avoids a
            // redundant payload transfer.
            return self.cached_head_with_attrs(location).await;
        }

        if let Some(range) = &opts.range {
            opts.range = Some(self.align_get_range(range, fetch_window));
        }
        let result = self.inner.get_opts(location, opts).await?;
        let meta = result.meta.clone();
        let attributes = result.attributes.clone();
        let fetched = result.range.clone();
        self.save_get_result(location, result).await?;
        self.note_fetch(location, fetched.start, fetched.end);
        Ok((meta, attributes))
    }

    async fn cached_get_opts(
        &self,
        location: &Path,
        opts: GetOptions,
    ) -> object_store::Result<GetResult> {
        if opts.if_match.is_some()
            || opts.if_none_match.is_some()
            || opts.if_modified_since.is_some()
            || opts.if_unmodified_since.is_some()
            || opts.version.is_some()
        {
            return self.inner.get_opts(location, opts).await;
        }

        if opts.head {
            let meta = self.cached_head(location).await?;
            return Ok(GetResult {
                payload: GetResultPayload::Stream(
                    stream::empty::<object_store::Result<Bytes>>().boxed(),
                ),
                range: 0..0,
                attributes: Attributes::default(),
                extensions: Default::default(),
                meta,
            });
        }

        let access_offset = self.range_start_offset(&opts.range);
        // Offset/Suffix/None request lengths aren't known until the head is; track
        // them as point accesses.
        let access_len = match &opts.range {
            Some(GetRange::Bounded(r)) => r.end.saturating_sub(r.start),
            _ => 0,
        };
        let decision = self.record_access(location, access_offset, access_len);
        let fetch_window = decision.fetch_window;

        let (meta, attributes) = self
            .discover_data_meta(location, opts.clone(), fetch_window)
            .await?;
        let generation = CacheGeneration::from_meta(&meta, self.cache_instance);

        let mut scheduled_to = None;
        for prefetch in decision.async_prefetch {
            match self.spawn_async_prefetch(location, &generation, prefetch, access_offset) {
                Some(end) => scheduled_to = Some(end),
                None => break,
            }
        }
        if let Some(end) = scheduled_to {
            self.note_schedule(location, access_offset, end);
        }

        let range = self.canonicalize_range(opts.range.clone(), meta.size)?;
        // Each part's window reaches at least to the request's aligned end, so
        // a large read is never split by unaligned bounds; the ramped fetch
        // window applies when larger.
        let end_part = usize::try_from(range.end.div_ceil(self.part_size_bytes as u64))
            .expect("part id exceeds usize");
        let parts = self.split_range_into_parts(range.clone());

        let futures = parts
            .into_iter()
            .map(|(part_id, range_in_part)| {
                let window = fetch_window.max((end_part - part_id) * self.part_size_bytes);
                self.read_part(
                    location.clone(),
                    generation.clone(),
                    part_id,
                    range_in_part,
                    window,
                )
            })
            .collect::<Vec<_>>();
        let result_stream = stream::iter(futures).then(|fut| fut).boxed();

        Ok(GetResult {
            meta,
            range,
            attributes,
            extensions: Default::default(),
            payload: GetResultPayload::Stream(result_stream),
        })
    }

    /// Cover a prefetch window: front-trim past parts already cached or in
    /// flight, then spawn one GET per remaining gap through the shared
    /// registry. Returns the offset scheduled to, or `None` if nothing could be
    /// arranged. Failed tasks roll their speculative frontier credit back.
    fn spawn_async_prefetch(
        &self,
        location: &Path,
        generation: &CacheGeneration,
        prefetch: AsyncPrefetch,
        trigger_offset: u64,
    ) -> Option<u64> {
        let part_size_u64 = self.part_size_bytes as u64;
        let window_end = prefetch.start + prefetch.size as u64;
        let head_size = self.read_head(location).map(|(meta, _)| meta.size);

        // A stream nearing EOF pushes the frontier past the end, and a GET
        // starting there is unsatisfiable (HTTP 416). Count the window as
        // covered so the frontier settles past EOF and stops re-emitting it.
        // A window merely ending past EOF is fine: the backend truncates.
        if let Some(size) = head_size
            && prefetch.start >= size
        {
            return Some(window_end);
        }

        if !prefetch.start.is_multiple_of(part_size_u64) {
            return None;
        }

        let start_part: PartId = (prefetch.start / part_size_u64).try_into().ok()?;
        let want_parts = prefetch.size.div_ceil(self.part_size_bytes).max(1);
        let mut end_part = start_part + want_parts;
        // Clamp the walk at EOF: front-trimming through a cached tail must not
        // lead a GET starting at or past the object's end (the same HTTP 416).
        if let Some(size) = head_size {
            let eof_part: PartId = size.div_ceil(part_size_u64).try_into().ok()?;
            end_part = end_part.min(eof_part);
        }

        let ctx = self.ctx();
        let mut part = start_part;
        while part < end_part {
            match Self::plan_window(
                &ctx,
                location,
                generation,
                part,
                end_part - part,
                Some(end_part),
            ) {
                WindowPlan::Covered => break,
                WindowPlan::Lead {
                    shared,
                    guard,
                    end_part: lead_end,
                } => {
                    let tracker = ctx.access_tracker.clone();
                    let location = location.clone();
                    let failed_start = prefetch.start;
                    tokio::spawn(async move {
                        let _guard = guard;
                        // Errors are contained: the parts stay uncached and a
                        // demand read fetches them itself.
                        if shared.await.is_err()
                            && let Some(entry) = tracker.get(&location)
                        {
                            entry
                                .value()
                                .clone()
                                .lock()
                                .unwrap()
                                .note_schedule_failure(trigger_offset, failed_start);
                        }
                    });
                    part = lead_end;
                }
                // Fully covered by an in-flight fetch: as good as covered here.
                WindowPlan::Join(_) => break,
            }
        }
        Some(window_end)
    }

    /// Plan a window of up to `max_parts` from `start_part`, scanning and
    /// registering under one lock so concurrent planners can't overlap.
    /// `front_trim` (`Some(needed_end)`) skips leading parts already cached or
    /// in flight, and refuses to lead past `needed_end`, the last part known
    /// to exist: beyond it a GET could start at or past EOF. If the skip
    /// crossed an in-flight fetch, that fetch is joined so a caller needing
    /// its side effects (the head install) can await it; `Covered` means cache
    /// alone suffices. A demand caller (`None`) just missed `start_part` and
    /// joins whatever is registered there, or leads. Led spans are end-trimmed
    /// at the first covered part; on a cold object nothing trims and a large
    /// contiguous read stays one GET.
    fn plan_window(
        ctx: &FetchCtx,
        location: &Path,
        generation: &CacheGeneration,
        start_part: PartId,
        max_parts: usize,
        front_trim: Option<PartId>,
    ) -> WindowPlan {
        let limit = start_part + max_parts.max(1);
        let mut map = ctx.fetches.lock().unwrap();

        let covered = |map: &HashMap<PartKey, SharedFetch>, part: PartId| {
            let key = PartKey::new(location, ctx.part_size_bytes, generation, part);
            map.contains_key(&key) || ctx.parts.contains(&key)
        };

        let mut start = start_part;
        if let Some(needed_end) = front_trim {
            let mut in_flight = None;
            while start < limit {
                let key = PartKey::new(location, ctx.part_size_bytes, generation, start);
                if let Some(fut) = map.get(&key) {
                    if in_flight.is_none() {
                        in_flight = Some(fut.clone());
                    }
                } else if !ctx.parts.contains(&key) {
                    break;
                }
                start += 1;
            }
            if start >= limit.min(needed_end) {
                return match in_flight {
                    Some(fut) => WindowPlan::Join(fut),
                    None => WindowPlan::Covered,
                };
            }
        } else {
            let key = PartKey::new(location, ctx.part_size_bytes, generation, start);
            if let Some(fut) = map.get(&key) {
                return WindowPlan::Join(fut.clone());
            }
            // The caller's initial HybridCache lookup may have begun as a
            // disk miss while a window fetch inserted this part and dropped
            // its registry guard. Recheck under the planner lock so demand
            // does not lead a redundant one-part GET.
            if ctx.parts.contains(&key) {
                return WindowPlan::Covered;
            }
        }

        let mut end = start + 1;
        while end < limit && !covered(&map, end) {
            end += 1;
        }

        let fut = Self::fetch_part_window(ctx.clone(), location.clone(), start, end - start)
            .boxed()
            .shared();
        let mut keys = Vec::with_capacity(end - start);
        for part in start..end {
            let key = PartKey::new(location, ctx.part_size_bytes, generation, part);
            map.insert(key.clone(), fut.clone());
            keys.push(key);
        }
        WindowPlan::Lead {
            shared: fut,
            guard: FetchGuard {
                map: ctx.fetches.clone(),
                keys,
            },
            end_part: end,
        }
    }

    async fn save_get_result(
        &self,
        location: &Path,
        result: GetResult,
    ) -> object_store::Result<()> {
        let generation = CacheGeneration::from_meta(&result.meta, self.cache_instance);
        self.save_head(location, &result.meta, &result.attributes);

        let part_size = self.part_size_bytes as u64;
        let aligned_end =
            result.range.end.is_multiple_of(part_size) || result.range.end == result.meta.size;
        if !aligned_end {
            return Ok(());
        }

        // A suffix GET can start inside a part. Discard only that partial
        // prefix; every complete part after it remains safe to cache.
        let skip = result.range.start.next_multiple_of(part_size) - result.range.start;
        let first_full = result.range.start + skip;
        if first_full >= result.range.end {
            return Ok(());
        }
        let start_part: PartId = (first_full / part_size)
            .try_into()
            .expect("part number exceeds usize");
        let stream = result.into_stream();
        let admission = self.admission_ctx();
        admission
            .save_parts_stream(location, &generation, stream, start_part, skip as usize)
            .await
    }

    /// Populate the parts cache from a just-uploaded object's bytes. Inserts
    /// are synchronous (foyer flushes to disk in the background), so this adds
    /// no I/O wait to the put.
    fn write_through(&self, location: &Path, payload: PutPayload, result: &PutResult) {
        let chunks: Vec<Bytes> = payload.into_iter().collect();
        let bytes = match chunks.len() {
            0 => return,
            1 => chunks.into_iter().next().expect("one chunk"),
            _ => {
                let mut buf = BytesMut::with_capacity(chunks.iter().map(Bytes::len).sum());
                for c in &chunks {
                    buf.extend_from_slice(c);
                }
                buf.freeze()
            }
        };
        self.warm_object(location, bytes, result);
    }

    /// Warm the parts cache with an object's full bytes, sliced into
    /// part-sized entries keyed as the read path expects. For callers that
    /// hold the bytes but upload via multipart, which doesn't write through.
    pub fn warm_object(&self, location: &Path, bytes: Bytes, result: &PutResult) {
        let Some(generation) = CacheGeneration::from_put_result(result) else {
            return;
        };
        self.generations
            .insert(location.clone(), generation.clone());
        let ps = self.part_size_bytes;
        let mut off = 0usize;
        let mut part_id: PartId = 0;
        while off < bytes.len() {
            let end = (off + ps).min(bytes.len());
            // Owned copy, not `bytes.slice(..)`: a slice keeps the whole
            // source allocation (a 256 MiB sealed segment) alive while any
            // one part survives in the cache, and the weigher only sees the
            // slice length. That was the multi-GB RSS retention bug.
            self.admit_part(
                location,
                &generation,
                part_id,
                Bytes::copy_from_slice(&bytes[off..end]),
            );
            off = end;
            part_id += 1;
        }
    }

    fn split_range_into_parts(&self, range: Range<u64>) -> Vec<(PartId, Range<usize>)> {
        let part_size_u64 = self.part_size_bytes as u64;
        let aligned = self.align_range(&range, self.part_size_bytes);
        let start_part = aligned.start / part_size_u64;
        let end_part = aligned.end / part_size_u64;
        let mut parts: Vec<_> = (start_part..end_part)
            .map(|part_id| {
                (
                    usize::try_from(part_id).expect("part id exceeds usize"),
                    Range {
                        start: 0,
                        end: self.part_size_bytes,
                    },
                )
            })
            .collect();
        if let Some(first) = parts.first_mut() {
            first.1.start = usize::try_from(range.start % part_size_u64)
                .expect("part_size too large for usize");
        }
        if let Some(last) = parts.last_mut()
            && !range.end.is_multiple_of(part_size_u64)
        {
            last.1.end =
                usize::try_from(range.end % part_size_u64).expect("part_size too large for usize");
        }
        parts
    }

    fn read_part(
        &self,
        location: Path,
        generation: CacheGeneration,
        part_id: PartId,
        range_in_part: Range<usize>,
        fetch_window: usize,
    ) -> BoxFuture<'static, object_store::Result<Bytes>> {
        let ctx = self.ctx();
        Box::pin(async move {
            let part_size_bytes = ctx.part_size_bytes;
            let key = PartKey::new(&location, part_size_bytes, &generation, part_id);
            if let Ok(Some(entry)) = ctx.parts.get(&key).await {
                let bytes = entry.value().clone();
                if range_in_part.end <= bytes.len() {
                    return Ok(bytes.slice(range_in_part));
                }
                ctx.parts.remove(&key);
            }

            // Single-flight the miss over the whole window: overlapping reads
            // join the one GET. The leader holds a `FetchGuard` so a cancelled
            // read can't strand registry slots.
            let extra_parts = (fetch_window / part_size_bytes).max(1);
            let (shared, guard) =
                match Self::plan_window(&ctx, &location, &generation, part_id, extra_parts, None) {
                    WindowPlan::Join(shared) => (shared, None),
                    WindowPlan::Lead { shared, guard, .. } => (shared, Some(guard)),
                    // A window may have populated the part after our initial
                    // cache miss. Read it once more; if it was concurrently
                    // evicted (or contains was stale), fall back to one exact
                    // part rather than widening the request.
                    WindowPlan::Covered => {
                        if let Ok(Some(entry)) = ctx.parts.get(&key).await {
                            let bytes = entry.value().clone();
                            if range_in_part.end <= bytes.len() {
                                return Ok(bytes.slice(range_in_part));
                            }
                            ctx.parts.remove(&key);
                        }
                        return Self::fetch_single_part(&ctx, &location, part_id, range_in_part)
                            .await;
                    }
                };
            let leading = guard.is_some();
            let _guard = guard;
            match shared.await {
                Ok((base, window)) => {
                    let off = part_id.saturating_sub(base) * part_size_bytes;
                    if off >= window.len() {
                        // Window EOF-truncated short of this part.
                        return Ok(Bytes::new());
                    }
                    let part_end = (off + part_size_bytes).min(window.len());
                    let part_bytes = window.slice(off..part_end);
                    let end = range_in_part.end.min(part_bytes.len());
                    let start = range_in_part.start.min(end);
                    Ok(part_bytes.slice(start..end))
                }
                Err(e) if leading => Err(object_store::Error::Generic {
                    store: "PrefetchingObjectStore",
                    source: format!("shared part fetch failed: {e}").into(),
                }),
                // A joined fetch (possibly an async prefetch) failed; its
                // error must not fail this read. Retry just this part.
                Err(_) => Self::fetch_single_part(&ctx, &location, part_id, range_in_part).await,
            }
        })
    }

    /// Direct one-part GET, bypassing the registry: the fallback when a shared
    /// fetch this read joined fails.
    async fn fetch_single_part(
        ctx: &FetchCtx,
        location: &Path,
        part_id: PartId,
        range_in_part: Range<usize>,
    ) -> object_store::Result<Bytes> {
        let part_size_bytes = ctx.part_size_bytes;
        let range = Range {
            start: (part_id * part_size_bytes) as u64,
            end: ((part_id + 1) * part_size_bytes) as u64,
        };
        let get_result = ctx
            .inner
            .get_opts(
                location,
                GetOptions {
                    range: Some(GetRange::Bounded(range)),
                    ..Default::default()
                },
            )
            .await?;
        let generation = CacheGeneration::from_meta(&get_result.meta, ctx.cache_instance);
        let bytes = get_result.bytes().await?;
        // Owned copy: a backend may serve this range as a slice of a larger
        // retained allocation, which a cached part must never pin.
        ctx.admission_ctx().admit_part(
            location,
            &generation,
            part_id,
            Bytes::copy_from_slice(&bytes),
        );
        let end = range_in_part.end.min(bytes.len());
        let start = range_in_part.start.min(end);
        Ok(bytes.slice(start..end))
    }

    /// One GET spanning `window_parts` from `part_id`; caches every part and
    /// resolves to `(part_id, window bytes)`.
    async fn fetch_part_window(
        ctx: FetchCtx,
        location: Path,
        part_id: PartId,
        window_parts: usize,
    ) -> Result<(PartId, Bytes), Arc<object_store::Error>> {
        let part_size_bytes = ctx.part_size_bytes;
        let fetch_start = part_id;
        let fetch_range = Range {
            start: (fetch_start * part_size_bytes) as u64,
            end: ((fetch_start + window_parts) * part_size_bytes) as u64,
        };
        let fetch_offset = fetch_range.start;
        let get_result = ctx
            .inner
            .get_opts(
                &location,
                GetOptions {
                    range: Some(GetRange::Bounded(fetch_range)),
                    ..Default::default()
                },
            )
            .await
            .map_err(Arc::new)?;
        let meta = get_result.meta.clone();
        let generation = CacheGeneration::from_meta(&meta, ctx.cache_instance);
        let attrs = get_result.attributes.clone();
        let actual_end = get_result.range.end;
        let all_bytes = get_result.bytes().await.map_err(Arc::new)?;

        ctx.heads.insert(
            location.clone(),
            Arc::new(CachedHead {
                meta,
                attributes: attrs,
            }),
        );
        ctx.generations.insert(location.clone(), generation.clone());
        if let Some(entry) = ctx.access_tracker.get(&location) {
            entry
                .value()
                .clone()
                .lock()
                .unwrap()
                .note_fetch(fetch_offset, actual_end);
        }
        for i in 0..window_parts {
            let start = i * part_size_bytes;
            if start >= all_bytes.len() {
                break;
            }
            let end = ((i + 1) * part_size_bytes).min(all_bytes.len());
            // Owned copy: a slice would pin the whole window allocation for
            // as long as any one part survives in the cache.
            ctx.admission_ctx().admit_part(
                &location,
                &generation,
                fetch_start + i,
                Bytes::copy_from_slice(&all_bytes[start..end]),
            );
        }
        Ok((fetch_start, all_bytes))
    }

    fn canonicalize_range(
        &self,
        range: Option<GetRange>,
        object_size: u64,
    ) -> object_store::Result<Range<u64>> {
        let (start, end) = match range {
            None => (0, object_size),
            Some(GetRange::Bounded(r)) => {
                if r.start >= object_size {
                    return Err(object_store::Error::Generic {
                        store: "PrefetchingObjectStore",
                        source: format!("range start {} >= size {}", r.start, object_size).into(),
                    });
                }
                if r.start >= r.end {
                    return Err(object_store::Error::Generic {
                        store: "PrefetchingObjectStore",
                        source: format!("inconsistent range {}..{}", r.start, r.end).into(),
                    });
                }
                (r.start, r.end.min(object_size))
            }
            Some(GetRange::Offset(o)) => {
                if o >= object_size {
                    return Err(object_store::Error::Generic {
                        store: "PrefetchingObjectStore",
                        source: format!("offset {} >= size {}", o, object_size).into(),
                    });
                }
                (o, object_size)
            }
            Some(GetRange::Suffix(s)) => (object_size.saturating_sub(s), object_size),
        };
        Ok(Range { start, end })
    }

    fn range_start_offset(&self, range: &Option<GetRange>) -> u64 {
        match range {
            None => 0,
            Some(GetRange::Bounded(r)) => r.start,
            Some(GetRange::Offset(o)) => *o,
            Some(GetRange::Suffix(_)) => u64::MAX,
        }
    }

    fn align_get_range(&self, range: &GetRange, fetch_window: usize) -> GetRange {
        let part = self.part_size_bytes;
        match range {
            GetRange::Bounded(requested) => {
                let part_aligned = self.align_range(requested, part);
                let expanded = self.align_range(
                    &Range {
                        start: part_aligned.start,
                        end: requested
                            .start
                            .saturating_add(fetch_window as u64)
                            .max(part_aligned.end),
                    },
                    part,
                );
                GetRange::Bounded(expanded)
            }
            GetRange::Suffix(size) => {
                let want = (*size).max(fetch_window as u64);
                GetRange::Suffix(self.align_range(&(0..want), part).end)
            }
            GetRange::Offset(offset) => GetRange::Offset(*offset - *offset % part as u64),
        }
    }

    fn align_range(&self, range: &Range<u64>, alignment: usize) -> Range<u64> {
        let alignment = alignment as u64;
        Range {
            start: range.start - range.start % alignment,
            end: range.end.div_ceil(alignment) * alignment,
        }
    }
}

impl Debug for PrefetchingObjectStore {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("PrefetchingObjectStore")
            .field("inner", &self.inner)
            .field("part_size_bytes", &self.part_size_bytes)
            .finish()
    }
}

impl Display for PrefetchingObjectStore {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "PrefetchingObjectStore({})", self.inner)
    }
}

#[async_trait]
impl ObjectStore for PrefetchingObjectStore {
    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        if options.extensions.get::<SkipPartsCache>().is_some() {
            return self.get_opts_uncached(location, options).await;
        }
        self.cached_get_opts(location, options).await
    }

    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        // Clone is cheap (Arc-backed) and lets us write the bytes through to the
        // parts cache once the upload commits.
        let cached = payload.clone();
        let result = self.inner.put_opts(location, payload, opts).await?;
        self.invalidate(location);
        self.write_through(location, cached, &result);
        Ok(result)
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.invalidate(location);
        self.inner.put_multipart_opts(location, opts).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, object_store::Result<Path>>,
    ) -> BoxStream<'static, object_store::Result<Path>> {
        let eviction = self.eviction_ctx();
        self.inner
            .delete_stream(locations)
            .map(move |res| {
                if let Ok(path) = &res {
                    eviction.evict_location(path);
                }
                res
            })
            .boxed()
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.inner.list(prefix)
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.inner.list_with_offset(prefix, offset)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        options: CopyOptions,
    ) -> object_store::Result<()> {
        let result = self.inner.copy_opts(from, to, options).await;
        self.invalidate(to);
        result
    }

    async fn rename_opts(
        &self,
        from: &Path,
        to: &Path,
        options: RenameOptions,
    ) -> object_store::Result<()> {
        let result = self.inner.rename_opts(from, to, options).await;
        self.invalidate(from);
        self.invalidate(to);
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use foyer::{BlockEngineConfig, FsDeviceBuilder, HybridCacheBuilder, PsyncIoEngineConfig};
    use object_store::ObjectStoreExt;
    use object_store::memory::InMemory;
    use tempfile::TempDir;

    async fn make_store(
        part_size: usize,
        memory_capacity: usize,
        disk_capacity: usize,
    ) -> (PrefetchingObjectStore, Arc<InMemory>, TempDir) {
        let inner = Arc::new(InMemory::new());
        let dir = tempfile::tempdir().unwrap();
        let parts = HybridCacheBuilder::new()
            .with_name("test-parts")
            .memory(memory_capacity)
            .with_weighter(|_: &PartKey, v: &Bytes| v.len())
            .storage()
            .with_io_engine_config(PsyncIoEngineConfig::new())
            .with_engine_config(
                BlockEngineConfig::new(
                    foyer::DeviceBuilder::build(
                        FsDeviceBuilder::new(dir.path()).with_capacity(disk_capacity),
                    )
                    .unwrap(),
                )
                .with_block_size(16 * 1024 * 1024),
            )
            .build()
            .await
            .unwrap();
        let store = PrefetchingObjectStore::with_options(inner.clone(), parts, part_size);
        (store, inner, dir)
    }

    async fn open_parts_cache(
        dir: &std::path::Path,
        memory_capacity: usize,
        disk_capacity: usize,
    ) -> HybridCache<PartKey, Bytes> {
        HybridCacheBuilder::new()
            .with_name("test-persistent-parts")
            .memory(memory_capacity)
            .with_weighter(|_: &PartKey, v: &Bytes| v.len())
            .storage()
            .with_io_engine_config(PsyncIoEngineConfig::new())
            .with_engine_config(
                BlockEngineConfig::new(
                    foyer::DeviceBuilder::build(
                        FsDeviceBuilder::new(dir).with_capacity(disk_capacity),
                    )
                    .unwrap(),
                )
                .with_block_size(16 * 1024 * 1024),
            )
            .build()
            .await
            .unwrap()
    }

    const MEM: usize = 4 * 1024 * 1024;
    const DISK: usize = 64 * 1024 * 1024;

    fn versioned_result(tag: &str) -> PutResult {
        PutResult {
            e_tag: Some(tag.to_owned()),
            version: None,
            extensions: Default::default(),
        }
    }

    fn cached_generation(store: &PrefetchingObjectStore, path: &Path) -> CacheGeneration {
        CacheGeneration::from_meta(
            &store.read_head(path).expect("cached object head").0,
            store.cache_instance,
        )
    }

    #[tokio::test]
    async fn persistent_cache_reopen_with_different_part_size_cannot_reuse_misaligned_part() {
        const KIB: usize = 1024;
        let inner = Arc::new(InMemory::new());
        let path = Path::from("segments/cache-geometry");
        let mut body = vec![b'A'; 64 * KIB];
        body.extend(vec![b'B'; 64 * KIB]);
        body.extend(vec![b'C'; 64 * KIB]);
        body.extend(vec![b'D'; 64 * KIB]);
        inner.put(&path, body.into()).await.unwrap();

        let dir = tempfile::tempdir().unwrap();
        let parts = open_parts_cache(dir.path(), MEM, DISK).await;
        let store = PrefetchingObjectStore::with_options(inner.clone(), parts.clone(), 64 * KIB);
        assert_eq!(
            store
                .get_range(&path, 64 * KIB as u64..64 * KIB as u64 + 1)
                .await
                .unwrap()
                .as_ref(),
            b"B"
        );
        drop(store);
        parts.close().await.unwrap();
        drop(parts);

        let parts = open_parts_cache(dir.path(), MEM, DISK).await;
        let store = PrefetchingObjectStore::with_options(inner, parts.clone(), 128 * KIB);
        assert_eq!(
            store
                .get_range(&path, 128 * KIB as u64..128 * KIB as u64 + 1)
                .await
                .unwrap()
                .as_ref(),
            b"C",
            "a cache part from the old geometry must not be interpreted under the new geometry"
        );
        drop(store);
        parts.close().await.unwrap();
    }

    #[tokio::test]
    async fn persistent_cache_reopen_after_overwrite_cannot_serve_predecessor_generation() {
        const KIB: usize = 1024;
        let inner = Arc::new(InMemory::new());
        let path = Path::from("segments/cache-generation");
        inner
            .put(&path, vec![b'A'; 256 * KIB].into())
            .await
            .unwrap();

        let dir = tempfile::tempdir().unwrap();
        let parts = open_parts_cache(dir.path(), MEM, DISK).await;
        let store = PrefetchingObjectStore::with_options(inner.clone(), parts.clone(), 128 * KIB);
        assert_eq!(store.get_range(&path, 0..1).await.unwrap().as_ref(), b"A");
        drop(store);
        parts.close().await.unwrap();
        drop(parts);

        inner
            .put(&path, vec![b'Z'; 256 * KIB].into())
            .await
            .unwrap();
        let parts = open_parts_cache(dir.path(), MEM, DISK).await;
        let store = PrefetchingObjectStore::with_options(inner, parts.clone(), 128 * KIB);
        assert_eq!(
            store.get_range(&path, 0..1).await.unwrap().as_ref(),
            b"Z",
            "a cache part from the predecessor object generation must not survive recovery"
        );
        drop(store);
        parts.close().await.unwrap();
    }

    #[tokio::test]
    async fn cold_data_read_uses_first_get_metadata_without_a_head_round_trip() {
        let inner = Arc::new(InMemory::new());
        let path = Path::from("segments/cold-first-get");
        inner
            .put(&path, vec![0x5A; FETCH_WINDOW_MIN * 2].into())
            .await
            .unwrap();
        let gets = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let backend: Arc<dyn ObjectStore> = Arc::new(CountingStore {
            inner,
            gets: gets.clone(),
            delay: std::time::Duration::ZERO,
        });
        let (store, _dir) = store_over(backend, DEFAULT_PART_SIZE_BYTES).await;

        assert_eq!(
            store.get_range(&path, 0..1).await.unwrap().as_ref(),
            &[0x5A]
        );
        assert_eq!(
            gets.load(std::sync::atomic::Ordering::Relaxed),
            1,
            "a cold data GET must supply both metadata and payload without a preceding HEAD"
        );
    }

    /// Wraps a store, counting `get_opts` calls and delaying each so concurrent
    /// reads overlap in flight (to exercise the single-flight coalescing).
    #[derive(Debug)]
    struct CountingStore {
        inner: Arc<InMemory>,
        gets: Arc<std::sync::atomic::AtomicUsize>,
        delay: std::time::Duration,
    }

    impl Display for CountingStore {
        fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
            write!(f, "CountingStore")
        }
    }

    #[async_trait]
    impl ObjectStore for CountingStore {
        async fn get_opts(&self, l: &Path, o: GetOptions) -> object_store::Result<GetResult> {
            self.gets.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            tokio::time::sleep(self.delay).await;
            self.inner.get_opts(l, o).await
        }
        async fn put_opts(
            &self,
            l: &Path,
            p: PutPayload,
            o: PutOptions,
        ) -> object_store::Result<PutResult> {
            self.inner.put_opts(l, p, o).await
        }
        async fn put_multipart_opts(
            &self,
            l: &Path,
            o: PutMultipartOptions,
        ) -> object_store::Result<Box<dyn MultipartUpload>> {
            self.inner.put_multipart_opts(l, o).await
        }
        fn delete_stream(
            &self,
            locations: BoxStream<'static, object_store::Result<Path>>,
        ) -> BoxStream<'static, object_store::Result<Path>> {
            self.inner.delete_stream(locations)
        }
        fn list(&self, p: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
            self.inner.list(p)
        }
        async fn list_with_delimiter(&self, p: Option<&Path>) -> object_store::Result<ListResult> {
            self.inner.list_with_delimiter(p).await
        }
        async fn copy_opts(&self, f: &Path, t: &Path, o: CopyOptions) -> object_store::Result<()> {
            self.inner.copy_opts(f, t, o).await
        }
        async fn rename_opts(
            &self,
            f: &Path,
            t: &Path,
            o: RenameOptions,
        ) -> object_store::Result<()> {
            self.inner.rename_opts(f, t, o).await
        }
    }

    /// Wraps a store, recording each `get_opts` range so a test can assert
    /// what actually reaches the backend.
    #[derive(Debug)]
    struct RangeRecordingStore {
        inner: Arc<InMemory>,
        ranges: Arc<std::sync::Mutex<Vec<GetRange>>>,
    }

    impl Display for RangeRecordingStore {
        fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
            write!(f, "RangeRecordingStore")
        }
    }

    #[async_trait]
    impl ObjectStore for RangeRecordingStore {
        async fn get_opts(&self, l: &Path, o: GetOptions) -> object_store::Result<GetResult> {
            if let Some(r) = &o.range {
                self.ranges.lock().unwrap().push(r.clone());
            }
            self.inner.get_opts(l, o).await
        }
        async fn put_opts(
            &self,
            l: &Path,
            p: PutPayload,
            o: PutOptions,
        ) -> object_store::Result<PutResult> {
            self.inner.put_opts(l, p, o).await
        }
        async fn put_multipart_opts(
            &self,
            l: &Path,
            o: PutMultipartOptions,
        ) -> object_store::Result<Box<dyn MultipartUpload>> {
            self.inner.put_multipart_opts(l, o).await
        }
        fn delete_stream(
            &self,
            locations: BoxStream<'static, object_store::Result<Path>>,
        ) -> BoxStream<'static, object_store::Result<Path>> {
            self.inner.delete_stream(locations)
        }
        fn list(&self, p: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
            self.inner.list(p)
        }
        async fn list_with_delimiter(&self, p: Option<&Path>) -> object_store::Result<ListResult> {
            self.inner.list_with_delimiter(p).await
        }
        async fn copy_opts(&self, f: &Path, t: &Path, o: CopyOptions) -> object_store::Result<()> {
            self.inner.copy_opts(f, t, o).await
        }
        async fn rename_opts(
            &self,
            f: &Path,
            t: &Path,
            o: RenameOptions,
        ) -> object_store::Result<()> {
            self.inner.rename_opts(f, t, o).await
        }
    }

    #[derive(Debug)]
    struct RangeFailingStore {
        inner: Arc<InMemory>,
        fail_at_or_after: u64,
    }

    impl Display for RangeFailingStore {
        fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
            write!(f, "RangeFailingStore")
        }
    }

    #[async_trait]
    impl ObjectStore for RangeFailingStore {
        async fn get_opts(&self, l: &Path, o: GetOptions) -> object_store::Result<GetResult> {
            if matches!(
                &o.range,
                Some(GetRange::Bounded(range)) if range.start >= self.fail_at_or_after
            ) {
                return Err(object_store::Error::Generic {
                    store: "RangeFailingStore",
                    source: "intentional prefetch failure".into(),
                });
            }
            self.inner.get_opts(l, o).await
        }

        async fn put_opts(
            &self,
            l: &Path,
            p: PutPayload,
            o: PutOptions,
        ) -> object_store::Result<PutResult> {
            self.inner.put_opts(l, p, o).await
        }

        async fn put_multipart_opts(
            &self,
            l: &Path,
            o: PutMultipartOptions,
        ) -> object_store::Result<Box<dyn MultipartUpload>> {
            self.inner.put_multipart_opts(l, o).await
        }

        fn delete_stream(
            &self,
            locations: BoxStream<'static, object_store::Result<Path>>,
        ) -> BoxStream<'static, object_store::Result<Path>> {
            self.inner.delete_stream(locations)
        }

        fn list(&self, p: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
            self.inner.list(p)
        }

        async fn list_with_delimiter(&self, p: Option<&Path>) -> object_store::Result<ListResult> {
            self.inner.list_with_delimiter(p).await
        }

        async fn copy_opts(&self, f: &Path, t: &Path, o: CopyOptions) -> object_store::Result<()> {
            self.inner.copy_opts(f, t, o).await
        }

        async fn rename_opts(
            &self,
            f: &Path,
            t: &Path,
            o: RenameOptions,
        ) -> object_store::Result<()> {
            self.inner.rename_opts(f, t, o).await
        }
    }

    async fn store_over(
        inner: Arc<dyn ObjectStore>,
        part_size: usize,
    ) -> (PrefetchingObjectStore, TempDir) {
        store_over_with_tuning(inner, part_size, FETCH_WINDOW_MIN, FETCH_WINDOW_MAX).await
    }

    async fn store_over_with_tuning(
        inner: Arc<dyn ObjectStore>,
        part_size: usize,
        fetch_window_min: usize,
        fetch_window_max: usize,
    ) -> (PrefetchingObjectStore, TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let parts = HybridCacheBuilder::new()
            .with_name("test-parts")
            .memory(MEM)
            .with_weighter(|_: &PartKey, v: &Bytes| v.len())
            .storage()
            .with_io_engine_config(PsyncIoEngineConfig::new())
            .with_engine_config(
                BlockEngineConfig::new(
                    foyer::DeviceBuilder::build(
                        FsDeviceBuilder::new(dir.path()).with_capacity(DISK),
                    )
                    .unwrap(),
                )
                .with_block_size(16 * 1024 * 1024),
            )
            .build()
            .await
            .unwrap();
        (
            PrefetchingObjectStore::with_profile(
                inner,
                parts,
                PrefetchProfile::tuned(part_size, fetch_window_min, fetch_window_max),
            ),
            dir,
        )
    }

    #[tokio::test]
    async fn sftp_tuning_hydrates_one_mib_first_then_uses_one_wave_windows() {
        const MIB: usize = 1024 * 1024;
        let mem = Arc::new(InMemory::new());
        let path = Path::from("segments/aa/large");
        mem.put(&path, vec![0x5Au8; 64 * MIB].into()).await.unwrap();
        let ranges = Arc::new(std::sync::Mutex::new(Vec::new()));
        let backend: Arc<dyn ObjectStore> = Arc::new(RangeRecordingStore {
            inner: mem,
            ranges: ranges.clone(),
        });
        let (store, _dir) = store_over_with_tuning(backend, MIB, MIB, 16 * MIB).await;

        for part in 0..8u64 {
            let start = part * MIB as u64;
            let result = store
                .get_opts(
                    &path,
                    GetOptions {
                        range: Some(GetRange::Bounded(start..start + 1)),
                        ..Default::default()
                    },
                )
                .await
                .unwrap();
            assert_eq!(result.bytes().await.unwrap().as_ref(), &[0x5A]);
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        let recorded = ranges.lock().unwrap();
        let bounded = recorded
            .iter()
            .filter_map(|range| match range {
                GetRange::Bounded(range) => Some(range.clone()),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(bounded.first().unwrap(), &(0..MIB as u64));
        assert!(
            bounded
                .iter()
                .any(|range| range.end - range.start == 15 * MIB as u64),
            "sequential reads never reached the 15 MiB one-wave window: {bounded:?}"
        );
        assert!(
            bounded
                .iter()
                .all(|range| range.end - range.start <= 15 * MIB as u64)
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn sftp_sequential_120_mib_stream_uses_parallel_one_wave_gets() {
        const MIB: usize = 1024 * 1024;
        const OBJECT_LEN: usize = 120 * MIB;
        let mem = Arc::new(InMemory::new());
        let path = Path::from("segments/aa/sftp-120m");
        mem.put(&path, vec![0x6Bu8; OBJECT_LEN].into())
            .await
            .unwrap();
        let ranges = Arc::new(std::sync::Mutex::new(Vec::new()));
        let backend: Arc<dyn ObjectStore> = Arc::new(RangeRecordingStore {
            inner: mem,
            ranges: ranges.clone(),
        });
        let dir = tempfile::tempdir().unwrap();
        let parts = HybridCacheBuilder::new()
            .with_name("sftp-120m-parts")
            .memory(192 * MIB)
            .with_weighter(|_: &PartKey, v: &Bytes| v.len())
            .storage()
            .with_io_engine_config(PsyncIoEngineConfig::new())
            .with_engine_config(
                BlockEngineConfig::new(
                    foyer::DeviceBuilder::build(
                        FsDeviceBuilder::new(dir.path()).with_capacity(256 * MIB),
                    )
                    .unwrap(),
                )
                .with_block_size(16 * MIB),
            )
            .build()
            .await
            .unwrap();
        let store = PrefetchingObjectStore::with_profile(
            backend,
            parts,
            PrefetchProfile::tuned(MIB, MIB, OBJECT_LEN),
        );

        for chunk in 0..120u64 {
            let start = chunk * MIB as u64;
            let bytes = store
                .get_range(&path, start..start + MIB as u64)
                .await
                .unwrap();
            assert_eq!(bytes.len(), MIB);
        }
        for _ in 0..100 {
            if store.fetches.lock().unwrap().is_empty() {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        assert!(
            store.fetches.lock().unwrap().is_empty(),
            "async prefetch did not settle before measurement"
        );

        let recorded = ranges.lock().unwrap();
        let bounded = recorded
            .iter()
            .filter_map(|range| match range {
                GetRange::Bounded(range) => Some(range.clone()),
                _ => None,
            })
            .collect::<Vec<_>>();
        let fetched = bounded
            .iter()
            .map(|range| range.end.min(OBJECT_LEN as u64) - range.start)
            .sum::<u64>();
        let amplification = fetched as f64 / OBJECT_LEN as f64;

        println!(
            "SFTP 120 MiB sequential: {} GETs, amplification {amplification:.3}x, ranges {bounded:?}",
            bounded.len()
        );

        assert_eq!(bounded.first(), Some(&(0..MIB as u64)));
        let confirmed = bounded.get(1).expect("confirmed-stream backend GET");
        assert_eq!(
            confirmed.end - confirmed.start,
            15 * MIB as u64,
            "the first confirmed-sequential GET must fit one 64-request SFTP payload wave"
        );
        assert!(
            bounded
                .iter()
                .skip(1)
                .all(|range| range.end - range.start <= 15 * MIB as u64),
            "a proven-sequential GET exceeded one 64-request SFTP wave: {bounded:?}"
        );
        let mut ordered = bounded.clone();
        ordered.sort_unstable_by_key(|range| range.start);
        assert!(
            ordered.windows(2).all(|pair| pair[0].end <= pair[1].start),
            "sequential SFTP windows overlapped: {bounded:?}"
        );
        assert!(
            bounded.len() <= 9,
            "120 MiB sequential stream used {} backend GETs instead of <=9: {bounded:?}",
            bounded.len()
        );
        assert!(
            amplification <= 1.15,
            "120 MiB sequential amplification {amplification:.3}x > 1.15x ({fetched} fetched)"
        );
    }

    #[tokio::test]
    async fn sftp_random_4k_and_32k_reads_never_exceed_one_mib_backend_gets() {
        const MIB: usize = 1024 * 1024;
        let mem = Arc::new(InMemory::new());
        let path = Path::from("segments/aa/sftp-random");
        mem.put(&path, vec![0x31u8; 120 * MIB].into())
            .await
            .unwrap();
        let ranges = Arc::new(std::sync::Mutex::new(Vec::new()));
        let backend: Arc<dyn ObjectStore> = Arc::new(RangeRecordingStore {
            inner: mem,
            ranges: ranges.clone(),
        });
        let (store, _dir) = store_over_with_tuning(backend, MIB, MIB, 120 * MIB).await;

        for (offset_mib, length) in [
            (0u64, 4 * 1024u64),
            (17, 32 * 1024),
            (39, 4 * 1024),
            (64, 32 * 1024),
            (91, 4 * 1024),
            (117, 32 * 1024),
        ] {
            let start = offset_mib * MIB as u64;
            let bytes = store.get_range(&path, start..start + length).await.unwrap();
            assert_eq!(bytes.len() as u64, length);
        }

        let recorded = ranges.lock().unwrap();
        let bounded = recorded
            .iter()
            .filter_map(|range| match range {
                GetRange::Bounded(range) => Some(range.clone()),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(bounded.len(), 6, "each random region should need one GET");
        assert!(
            bounded
                .iter()
                .all(|range| range.end - range.start <= MIB as u64),
            "random 4 KiB/32 KiB reads exceeded the 1 MiB SFTP minimum window: {bounded:?}"
        );
    }

    // A burst of concurrent reads whose parts fall inside one prefetch window must
    // collapse onto a single object-store GET, not one GET each.
    #[tokio::test]
    async fn overlapping_concurrent_reads_coalesce_into_one_get() {
        let mem = Arc::new(InMemory::new());
        let path = Path::from("segments/aa/seg");
        let body: Vec<u8> = (0..64 * 1024u32).map(|i| i as u8).collect();
        mem.put(&path, body.clone().into()).await.unwrap();

        let gets = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let counting: Arc<dyn ObjectStore> = Arc::new(CountingStore {
            inner: mem,
            gets: gets.clone(),
            delay: std::time::Duration::from_millis(40),
        });
        // 1 KiB parts; the first read ramps to a window covering many parts.
        let (store, _dir) = store_over(counting, 1024).await;
        let store = Arc::new(store);

        // Warm the head so the first-touch head GET doesn't skew the count, then
        // launch reads of distinct parts that all land in the leader's window.
        let _ = store.head(&path).await.unwrap();
        gets.store(0, std::sync::atomic::Ordering::Relaxed);

        let mut handles = Vec::new();
        for p in 0..16u64 {
            let s = store.clone();
            let path = path.clone();
            handles.push(tokio::spawn(async move {
                let off = p * 1024;
                s.get_opts(
                    &path,
                    GetOptions {
                        range: Some(GetRange::Bounded(off..off + 1024)),
                        ..Default::default()
                    },
                )
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap()
            }));
        }
        for (p, h) in handles.into_iter().enumerate() {
            let got = h.await.unwrap();
            assert_eq!(&got[..], &body[p * 1024..p * 1024 + 1024]);
        }

        // The first read ramps to a 128 KiB window (128 × 1 KiB parts) covering all
        // 16, so they collapse onto ~one GET; a couple extra tolerates the leader race.
        let n = gets.load(std::sync::atomic::Ordering::Relaxed);
        assert!(
            n <= 3,
            "expected coalescing, got {n} GETs for 16 overlapping reads"
        );
        assert!(
            store.fetches.lock().unwrap().is_empty(),
            "fetch slots leaked"
        );
    }

    // A contiguous read larger than the fetch window with unaligned bounds
    // (every coalesced frame run) must reach the object store as one GET: the
    // unaligned tail part must not fall outside the leader's window and
    // trigger a second, overfetching GET.
    #[tokio::test]
    async fn unaligned_large_read_is_one_get() {
        let mem = Arc::new(InMemory::new());
        let path = Path::from("segments/aa/seg");
        let body: Vec<u8> = (0..1024 * 1024u32).map(|i| i as u8).collect();
        mem.put(&path, body.clone().into()).await.unwrap();

        let gets = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let counting: Arc<dyn ObjectStore> = Arc::new(CountingStore {
            inner: mem,
            gets: gets.clone(),
            delay: std::time::Duration::ZERO,
        });
        let (store, _dir) = store_over(counting, 1024).await;

        // Warm the head via a small read far from the range under test (a head()
        // against InMemory would return the whole payload and pre-fill the parts
        // cache), then count only the read under test.
        let far = 900 * 1024u64;
        let r = store
            .get_opts(
                &path,
                GetOptions {
                    range: Some(GetRange::Bounded(far..far + 10)),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        r.bytes().await.unwrap();
        gets.store(0, std::sync::atomic::Ordering::Relaxed);

        let (off, len) = (500u64, 200 * 1024u64);
        let r = store
            .get_opts(
                &path,
                GetOptions {
                    range: Some(GetRange::Bounded(off..off + len)),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        let got = r.bytes().await.unwrap();
        assert_eq!(&got[..], &body[off as usize..(off + len) as usize]);
        assert_eq!(
            gets.load(std::sync::atomic::Ordering::Relaxed),
            1,
            "unaligned contiguous read should be a single GET"
        );
    }

    #[tokio::test]
    async fn put_writes_through_to_parts_cache() {
        let (store, _inner, _dir) = make_store(1024, MEM, DISK).await;
        let path = Path::from("segments/3b/seg");
        // 2.5 parts of 1024 bytes: parts 0 and 1 full, part 2 partial (512).
        let body: Vec<u8> = (0..2560u32).map(|i| i as u8).collect();
        let result = store.put(&path, body.clone().into()).await.unwrap();
        let generation = CacheGeneration::from_put_result(&result).unwrap();

        // The put populated every part; a read needs no object-store GET.
        for (part_id, range) in [(0usize, 0..1024usize), (1, 1024..2048), (2, 2048..2560)] {
            let entry = store
                .parts
                .get(&PartKey::new(&path, 1024, &generation, part_id))
                .await
                .unwrap()
                .expect("part cached by write-through");
            assert_eq!(entry.value().as_ref(), &body[range]);
        }
    }

    #[tokio::test]
    async fn warm_object_populates_parts_cache() {
        let (store, _inner, _dir) = make_store(1024, MEM, DISK).await;
        let path = Path::from("segments/3b/seg");
        // 2.5 parts: parts 0 and 1 full, part 2 partial (512). No object-store
        // put: warm_object caches bytes held in hand (the multipart-seal case).
        let body: Vec<u8> = (0..2560u32).map(|i| i as u8).collect();
        let result = versioned_result("warm-generation");
        let generation = CacheGeneration::from_put_result(&result).unwrap();
        store.warm_object(&path, body.clone().into(), &result);

        for (part_id, range) in [(0usize, 0..1024usize), (1, 1024..2048), (2, 2048..2560)] {
            let entry = store
                .parts
                .get(&PartKey::new(&path, 1024, &generation, part_id))
                .await
                .unwrap()
                .expect("part cached by warm_object");
            assert_eq!(entry.value().as_ref(), &body[range]);
        }
    }

    #[tokio::test]
    async fn small_read_caches_full_part() {
        let (store, inner, _dir) = make_store(1024, MEM, DISK).await;
        let path = Path::from("obj");
        let body = vec![7u8; 4096];
        inner.put(&path, body.clone().into()).await.unwrap();

        let r1 = store
            .get_opts(
                &path,
                GetOptions {
                    range: Some(GetRange::Bounded(10..20)),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        assert_eq!(r1.range, 10..20);
        assert_eq!(&r1.bytes().await.unwrap()[..], &body[10..20]);

        let r2 = store
            .get_opts(
                &path,
                GetOptions {
                    range: Some(GetRange::Bounded(100..200)),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        assert_eq!(&r2.bytes().await.unwrap()[..], &body[100..200]);
    }

    #[tokio::test]
    async fn full_object_read_populates_all_parts() {
        let (store, inner, _dir) = make_store(1024, MEM, DISK).await;
        let path = Path::from("obj");
        let body: Vec<u8> = (0..8192u32).map(|i| (i % 251) as u8).collect();
        inner.put(&path, body.clone().into()).await.unwrap();

        let r = store.get_opts(&path, GetOptions::default()).await.unwrap();
        assert_eq!(&r.bytes().await.unwrap()[..], &body[..]);

        for part_id in 0..8 {
            assert!(
                store.cached_part(&path, part_id).await.is_some(),
                "part {part_id} not cached"
            );
        }
    }

    #[tokio::test]
    async fn partial_cache_then_range_fetches_only_misses() {
        let (store, inner, _dir) = make_store(1024, MEM, DISK).await;
        let path = Path::from("obj");
        let body = vec![3u8; 4096];
        inner.put(&path, body.clone().into()).await.unwrap();

        let _ = store
            .get_opts(
                &path,
                GetOptions {
                    range: Some(GetRange::Bounded(0..1024)),
                    ..Default::default()
                },
            )
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        let r = store.get_opts(&path, GetOptions::default()).await.unwrap();
        assert_eq!(&r.bytes().await.unwrap()[..], &body[..]);
        for part_id in 0..4 {
            assert!(store.cached_part(&path, part_id).await.is_some());
        }
    }

    #[tokio::test]
    async fn put_invalidates_head() {
        let (store, _inner, _dir) = make_store(1024, MEM, DISK).await;
        let path = Path::from("obj");
        store.put(&path, vec![1u8; 100].into()).await.unwrap();
        let _ = store.head(&path).await.unwrap();
        store.put(&path, vec![2u8; 200].into()).await.unwrap();
        let meta = store.head(&path).await.unwrap();
        assert_eq!(meta.size, 200);
        let r = store
            .get_opts(
                &path,
                GetOptions {
                    range: Some(GetRange::Bounded(0..200)),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        assert_eq!(&r.bytes().await.unwrap()[..], &vec![2u8; 200][..]);
    }

    #[tokio::test]
    async fn suffix_range() {
        let (store, _inner, _dir) = make_store(1024, MEM, DISK).await;
        let path = Path::from("obj");
        let body = vec![5u8; 10_000];
        store.put(&path, body.clone().into()).await.unwrap();
        let r = store
            .get_opts(
                &path,
                GetOptions {
                    range: Some(GetRange::Suffix(100)),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        let got = r.bytes().await.unwrap();
        assert_eq!(got.len(), 100);
        assert_eq!(&got[..], &body[body.len() - 100..]);
    }

    #[tokio::test]
    async fn offset_range() {
        let (store, _inner, _dir) = make_store(1024, MEM, DISK).await;
        let path = Path::from("obj");
        let body: Vec<u8> = (0..5000u32).map(|i| (i % 251) as u8).collect();
        store.put(&path, body.clone().into()).await.unwrap();
        let r = store
            .get_opts(
                &path,
                GetOptions {
                    range: Some(GetRange::Offset(2500)),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        assert_eq!(&r.bytes().await.unwrap()[..], &body[2500..]);
    }

    #[tokio::test]
    async fn conditional_get_bypasses_cache() {
        let (store, inner, _dir) = make_store(1024, MEM, DISK).await;
        let path = Path::from("obj");
        inner.put(&path, vec![9u8; 100].into()).await.unwrap();
        let r = store
            .get_opts(
                &path,
                GetOptions {
                    if_match: Some("never-matches".into()),
                    ..Default::default()
                },
            )
            .await;
        let _ = r;
    }

    #[test]
    fn window_ramps_up_on_sequential_access() {
        let part_size = 1024;
        let mut h = AccessHistory::new(part_size);
        assert_eq!(h.record(0, 0).fetch_window, FETCH_WINDOW_MIN);
        assert_eq!(h.record(1024, 0).fetch_window, FETCH_WINDOW_MIN * 2);
        assert_eq!(h.record(2048, 0).fetch_window, FETCH_WINDOW_MIN * 4);
        assert_eq!(h.record(3072, 0).fetch_window, FETCH_WINDOW_MIN * 8);
        assert_eq!(h.record(4096, 0).fetch_window, FETCH_WINDOW_MIN * 16);
        assert_eq!(h.record(5120, 0).fetch_window, FETCH_WINDOW_MIN * 32);
        assert_eq!(h.record(6144, 0).fetch_window, FETCH_WINDOW_MIN * 64);
        assert_eq!(h.record(7168, 0).fetch_window, FETCH_WINDOW_MAX);
    }

    #[test]
    fn large_contiguous_requests_ramp_despite_stride() {
        // Back-to-back 1 MiB coalesced reads: the start-to-start stride is 1 MiB
        // (far beyond any point-read stride limit), but each request starts where
        // the previous one ended, so the stream must keep ramping.
        let mut h = AccessHistory::new(DEFAULT_PART_SIZE_BYTES);
        let m = 1024 * 1024u64;
        assert_eq!(h.record(0, m).fetch_window, FETCH_WINDOW_MIN);
        assert_eq!(h.record(m, m).fetch_window, FETCH_WINDOW_MIN * 2);
        assert_eq!(h.record(2 * m, m).fetch_window, FETCH_WINDOW_MIN * 4);
        assert_eq!(h.record(3 * m, m).fetch_window, FETCH_WINDOW_MIN * 8);
    }

    #[test]
    fn random_access_starts_new_stream() {
        let part_size = 1024;
        let mut h = AccessHistory::new(part_size);
        h.record(0, 0);
        h.record(1024, 0);
        h.record(2048, 0);
        assert_eq!(h.record(3072, 0).fetch_window, FETCH_WINDOW_MIN * 8);
        assert_eq!(h.record(100_000, 0).fetch_window, FETCH_WINDOW_MIN);
    }

    #[test]
    fn first_access_uses_min_window() {
        let mut h = AccessHistory::new(1024);
        assert_eq!(h.record(5000, 0).fetch_window, FETCH_WINDOW_MIN);
    }

    #[test]
    fn interleaved_streams_dont_interfere() {
        let part_size = 1024;
        let mut h = AccessHistory::new(part_size);

        h.record(0, 0);
        h.record(1024, 0);
        assert_eq!(h.record(2048, 0).fetch_window, FETCH_WINDOW_MIN * 4);

        h.record(100_000, 0);
        h.record(101_024, 0);

        assert_eq!(h.record(3072, 0).fetch_window, FETCH_WINDOW_MIN * 8);
    }

    #[test]
    fn random_burst_does_not_evict_ramped_stream() {
        let part_size = 1024;
        let mut h = AccessHistory::new(part_size);

        h.record(0, 0);
        h.record(1024, 0);
        h.record(2048, 0);
        assert_eq!(h.record(3072, 0).fetch_window, FETCH_WINDOW_MIN * 8);

        for i in 0..10 {
            assert_eq!(
                h.record((i + 1) * 100_000, 0).fetch_window,
                FETCH_WINDOW_MIN
            );
        }

        assert_eq!(h.record(4096, 0).fetch_window, FETCH_WINDOW_MIN * 16);
    }

    #[tokio::test]
    async fn sequential_reads_ramp_up_prefetch() {
        let part_size = 64 * 1024;
        let seq_reads = 7;
        let max_window_parts = FETCH_WINDOW_MAX / part_size;
        // Large enough that EOF sits beyond any legitimate prefetch reach, so the
        // boundedness assertion below is meaningful.
        let total_parts = seq_reads + (PREFETCH_DEPTH_WINDOWS + 1) * max_window_parts + 8;
        let (store, inner, _dir) = make_store(part_size, 64 * MEM, 4 * DISK).await;
        let path = Path::from("seq");
        let body = vec![0xABu8; part_size * total_parts];
        inner.put(&path, body.clone().into()).await.unwrap();

        for i in 0..seq_reads {
            store.heads.remove(&path);
            let start = (i * part_size) as u64;
            let r = store
                .get_opts(
                    &path,
                    GetOptions {
                        range: Some(GetRange::Bounded(start..start + 10)),
                        ..Default::default()
                    },
                )
                .await
                .unwrap();
            r.bytes().await.unwrap();
        }

        store.heads.remove(&path);
        let next_start = (seq_reads * part_size) as u64;
        let r = store
            .get_opts(
                &path,
                GetOptions {
                    range: Some(GetRange::Bounded(next_start..next_start + 10)),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        r.bytes().await.unwrap();

        let target_part = seq_reads;
        let last_prefetched_part = target_part + max_window_parts - 1;
        assert!(
            store
                .cached_part(&path, last_prefetched_part)
                .await
                .is_some(),
            "part {last_prefetched_part} should be prefetched at max window"
        );

        // The frontier keeps at most PREFETCH_DEPTH_WINDOWS ramped windows
        // ahead of the last read, and a window fetch overshoots the target by
        // less than one window; nothing may be cached past that bound.
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        let bound_part =
            (next_start as usize + (PREFETCH_DEPTH_WINDOWS + 1) * FETCH_WINDOW_MAX) / part_size;
        for part in bound_part..total_parts {
            assert!(
                store.cached_part(&path, part).await.is_none(),
                "part {part} cached beyond the prefetch depth bound {bound_part}"
            );
        }
    }

    #[test]
    fn backward_access_resets_window() {
        let part_size = 1024;
        let mut h = AccessHistory::new(part_size);
        h.record(0, 0);
        h.record(1024, 0);
        h.record(2048, 0);
        assert_eq!(h.record(3072, 0).fetch_window, FETCH_WINDOW_MIN * 8);
        assert_eq!(h.record(1024, 0).fetch_window, FETCH_WINDOW_MIN);
    }

    // A read absorbed behind a ramped stream's head must not inherit the
    // stream's window: over cold bytes that would turn a random read landing
    // in the slack zone into an up-to-8 MiB demand GET.
    #[test]
    fn lagging_read_uses_min_window() {
        let m = 1024 * 1024u64;
        let mut h = AccessHistory::new(DEFAULT_PART_SIZE_BYTES);
        for i in 0..8 {
            h.record(i * m, m);
        }
        assert_eq!(h.streams[0].fetch_window, FETCH_WINDOW_MAX);

        // 2 MiB behind the head, within the 8 MiB slack: absorbed, min window.
        let d = h.record(5 * m, m);
        assert_eq!(d.fetch_window, FETCH_WINDOW_MIN);
        assert!(d.async_prefetch.is_empty());

        // The absorption left the head alone: the stream continues ramped.
        assert_eq!(h.record(8 * m, m).fetch_window, FETCH_WINDOW_MAX);
    }

    #[tokio::test]
    async fn read_part_prefetches_window_when_head_cached() {
        let part_size = 1024;
        let window_parts = FETCH_WINDOW_MIN / part_size;
        let total_parts = window_parts * 32;
        let (store, inner, _dir) = make_store(part_size, 16 * MEM, DISK).await;
        let path = Path::from("steady");
        let body: Vec<u8> = (0..part_size * total_parts)
            .map(|i| (i % 251) as u8)
            .collect();
        inner.put(&path, body.clone().into()).await.unwrap();

        let r = store
            .get_opts(
                &path,
                GetOptions {
                    range: Some(GetRange::Bounded(0..10)),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        r.bytes().await.unwrap();
        assert!(store.read_head(&path).is_some());

        let offset = (window_parts * 4 * part_size) as u64;
        let r = store
            .get_opts(
                &path,
                GetOptions {
                    range: Some(GetRange::Bounded(offset..offset + 10)),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        let got = r.bytes().await.unwrap();
        assert_eq!(&got[..], &body[offset as usize..offset as usize + 10]);

        let target_part = offset as usize / part_size;
        let last_prefetched = target_part + window_parts - 1;
        assert!(
            store.cached_part(&path, last_prefetched).await.is_some(),
            "read_part should prefetch {window_parts} parts when head is cached"
        );
        let beyond = last_prefetched + 1;
        assert!(
            store.cached_part(&path, beyond).await.is_none(),
            "read_part should not prefetch beyond the window"
        );
    }

    #[tokio::test]
    async fn read_part_handles_eof_with_window() {
        let part_size = 1024;
        let total_parts = 4;
        let (store, inner, _dir) = make_store(part_size, MEM, DISK).await;
        let path = Path::from("eof");
        let body = vec![0xEFu8; part_size * total_parts];
        inner.put(&path, body.clone().into()).await.unwrap();

        let r = store
            .get_opts(
                &path,
                GetOptions {
                    range: Some(GetRange::Bounded(0..10)),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        r.bytes().await.unwrap();

        let last_part_offset = ((total_parts - 1) * part_size) as u64;
        let r = store
            .get_opts(
                &path,
                GetOptions {
                    range: Some(GetRange::Bounded(last_part_offset..last_part_offset + 10)),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        let got = r.bytes().await.unwrap();
        assert_eq!(
            &got[..],
            &body[last_part_offset as usize..last_part_offset as usize + 10]
        );
    }

    #[tokio::test]
    async fn random_reads_stay_at_min_window() {
        let part_size = 1024;
        let min_fetch_parts = FETCH_WINDOW_MIN / part_size;
        let total_parts = min_fetch_parts * 64;
        let (store, inner, _dir) = make_store(part_size, 16 * MEM, DISK).await;
        let path = Path::from("rnd");
        let body = vec![0xCDu8; part_size * total_parts];
        inner.put(&path, body.clone().into()).await.unwrap();

        let gap = min_fetch_parts * 4;
        let random_offsets: Vec<u64> = (0..8).map(|i| (i * gap * part_size) as u64).collect();
        for off in &random_offsets {
            let r = store
                .get_opts(
                    &path,
                    GetOptions {
                        range: Some(GetRange::Bounded(*off..*off + 10)),
                        ..Default::default()
                    },
                )
                .await
                .unwrap();
            r.bytes().await.unwrap();
        }

        store.heads.remove(&path);
        let target_part = total_parts / 2;
        let target_offset = (target_part * part_size) as u64;
        let r = store
            .get_opts(
                &path,
                GetOptions {
                    range: Some(GetRange::Bounded(target_offset..target_offset + 10)),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        r.bytes().await.unwrap();

        assert!(store.cached_part(&path, target_part).await.is_some());
        let beyond_part = target_part + min_fetch_parts + 1;
        assert!(
            store.cached_part(&path, beyond_part).await.is_none(),
            "part {beyond_part} should not be prefetched under random access"
        );
    }

    #[test]
    fn record_emits_parallel_async_prefetch_windows_in_trigger_zone() {
        let part_size = 64 * 1024;
        let mut h = AccessHistory::new(part_size);

        let d = h.record(0, 0);
        assert_eq!(d.fetch_window, FETCH_WINDOW_MIN);
        assert!(d.async_prefetch.is_empty());

        h.note_fetch(0, FETCH_WINDOW_MIN as u64);

        let d = h.record((FETCH_WINDOW_MIN / 2) as u64, 0);
        assert_eq!(d.fetch_window, FETCH_WINDOW_MIN * 2);
        // The frontier refills with separate windows so the SFTP backend can
        // lease several sessions instead of serializing one giant range.
        let p = d
            .async_prefetch
            .first()
            .expect("should fire in trigger zone");
        assert_eq!(p.start, FETCH_WINDOW_MIN as u64);
        assert_eq!(p.size, FETCH_WINDOW_MIN * 2);
        assert_eq!(d.async_prefetch.len(), PREFETCH_DEPTH_WINDOWS);
    }

    #[test]
    fn record_no_async_prefetch_when_far_from_fetched_until() {
        let part_size = 1024;
        let mut h = AccessHistory::new(part_size);

        h.record(0, 0);
        h.note_fetch(0, 8 * 1024 * 1024);

        let d = h.record(1024, 0);
        assert!(
            d.async_prefetch.is_empty(),
            "should not trigger when fetched_until is far ahead of offset"
        );
    }

    #[test]
    fn record_no_async_prefetch_at_min_window() {
        let part_size = 1024;
        let mut h = AccessHistory::new(part_size);

        h.record(100_000, 0);
        let d = h.record(100_000, 0);
        assert!(d.async_prefetch.is_empty());
    }

    #[test]
    fn note_fetch_only_advances_fetched_until() {
        let part_size = 1024;
        let mut h = AccessHistory::new(part_size);

        h.record(0, 0);
        h.note_fetch(0, 4096);
        h.note_fetch(0, 2048);
        assert_eq!(h.streams[0].fetched_until, 4096);
    }

    // A demand window GET completes after concurrent readers have advanced the
    // stream head past its leader's offset; the note must still credit the
    // stream, or the frontier never bootstraps and async prefetch stays dead.
    #[test]
    fn note_fetch_credits_lagging_window_leader() {
        let mut h = AccessHistory::new(1024);
        h.record(0, 1024);
        h.record(1024, 1024);
        h.record(2048, 1024);
        h.note_fetch(0, 16 * 1024);
        assert_eq!(h.streams[0].fetched_until, 16 * 1024);
    }

    // The looser matcher must not let one stream's fetch inflate another
    // stream's frontier (phantom coverage would silence its prefetch).
    #[test]
    fn note_fetch_ignores_unrelated_stream() {
        let mut h = AccessHistory::new(1024);
        h.record(0, 1024);
        h.record(1_000_000, 1024);
        h.note_fetch(1_000_000, 1_016_384);
        assert_eq!(
            h.streams[0].fetched_until, 0,
            "far fetch must not advance the near stream's frontier"
        );
        assert_eq!(h.streams[1].fetched_until, 1_016_384);
    }

    #[test]
    fn earlier_prefetch_failure_keeps_later_out_of_order_success_behind_the_gap() {
        let mut h = AccessHistory::new(1024);
        h.record(0, 1024);
        h.note_fetch(0, 4096);
        h.note_schedule(0, 12_288);
        h.record(4096, 4096);
        h.record(8192, 4096);

        // Window 8192..12288 completes first; it must not leap the confirmed
        // frontier over the still-in-flight 4096..8192 window.
        h.note_fetch(8192, 12_288);
        assert_eq!(h.streams[0].fetched_until, 4096);
        h.note_schedule_failure(4096, 4096);
        assert_eq!(h.streams[0].fetched_until, 4096);
        assert_eq!(h.streams[0].scheduled_until, 4096);

        // A late completion beyond the known hole still cannot reopen it.
        h.note_fetch(8192, 12_288);
        assert_eq!(h.streams[0].fetched_until, 4096);
        assert_eq!(h.streams[0].completed, vec![8192..12_288]);

        // Only a retry that spans the gap may move the contiguous frontier.
        h.note_fetch(4096, 8192);
        assert_eq!(
            h.streams[0].fetched_until, 12_288,
            "repairing the gap makes the remembered later success contiguous"
        );
    }

    #[test]
    fn out_of_order_fetches_merge_when_the_middle_completion_bridges_them() {
        let mut h = AccessHistory::new(1024);
        h.record(0, 1024);
        h.note_fetch(0, 4096);
        h.record(4096, 4096);
        h.record(8192, 4096);

        h.note_fetch(8192, 12_288);
        h.note_fetch(12_288, 16_384);
        assert_eq!(h.streams[0].fetched_until, 4096);

        h.note_fetch(4096, 8192);
        assert_eq!(h.streams[0].fetched_until, 16_384);
    }

    #[tokio::test]
    async fn async_prefetch_fills_cache_ahead_of_consumer() {
        let part_size = 64 * 1024;
        let total_parts = (FETCH_WINDOW_MIN / part_size) * 8;
        let (store, inner, _dir) = make_store(part_size, 4 * MEM, DISK).await;
        let path = Path::from("asyncpf");
        let body: Vec<u8> = (0..part_size * total_parts)
            .map(|i| (i % 251) as u8)
            .collect();
        inner.put(&path, body.clone().into()).await.unwrap();

        let r = store
            .get_opts(
                &path,
                GetOptions {
                    range: Some(GetRange::Bounded(0..10)),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        r.bytes().await.unwrap();

        let mid_offset = (FETCH_WINDOW_MIN / 2) as u64;
        let r = store
            .get_opts(
                &path,
                GetOptions {
                    range: Some(GetRange::Bounded(mid_offset..mid_offset + 10)),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        r.bytes().await.unwrap();

        let next_window_start_part = FETCH_WINDOW_MIN / part_size;
        for _ in 0..100 {
            if store
                .cached_part(&path, next_window_start_part)
                .await
                .is_some()
            {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        assert!(
            store
                .cached_part(&path, next_window_start_part)
                .await
                .is_some(),
            "async prefetch should fill part {next_window_start_part}"
        );
    }

    #[tokio::test]
    async fn failed_async_prefetch_does_not_advance_the_stream_frontier() {
        let part_size = 64 * 1024;
        let inner = Arc::new(InMemory::new());
        let path = Path::from("async-prefetch-failure-frontier");
        inner
            .put(&path, vec![0xA5; 2 * 1024 * 1024].into())
            .await
            .unwrap();
        let backend: Arc<dyn ObjectStore> = Arc::new(RangeFailingStore {
            inner,
            fail_at_or_after: FETCH_WINDOW_MIN as u64,
        });
        let (store, _dir) = store_over(backend, part_size).await;

        assert_eq!(
            store.get_range(&path, 0..1).await.unwrap().as_ref(),
            &[0xA5]
        );
        assert_eq!(
            store
                .get_range(&path, part_size as u64..part_size as u64 + 1)
                .await
                .unwrap()
                .as_ref(),
            &[0xA5]
        );

        for _ in 0..100 {
            if store.fetches.lock().unwrap().is_empty() {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        assert!(store.fetches.lock().unwrap().is_empty());
        let history = store.access_tracker.get(&path).unwrap().value().clone();
        let history = history.lock().unwrap();
        assert_eq!(
            history.streams[0].fetched_until, FETCH_WINDOW_MIN as u64,
            "only a completed fetch may advance the prefetch frontier"
        );
        assert_eq!(
            history.streams[0].scheduled_until, FETCH_WINDOW_MIN as u64,
            "a failed async fetch must roll back speculative frontier credit"
        );
    }

    // Two overlapping async prefetch windows must collapse onto one backend GET:
    // the second finds every part registered by the first and does nothing.
    #[tokio::test]
    async fn overlapping_async_prefetches_dedup_to_one_get() {
        let part_size = 1024;
        let mem = Arc::new(InMemory::new());
        let path = Path::from("dedup");
        let body = vec![0xAAu8; part_size * 32];
        mem.put(&path, body.clone().into()).await.unwrap();

        let gets = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let counting: Arc<dyn ObjectStore> = Arc::new(CountingStore {
            inner: mem,
            gets: gets.clone(),
            delay: std::time::Duration::from_millis(50),
        });
        let (store, _dir) = store_over(counting, part_size).await;
        store.head(&path).await.unwrap();
        let generation = cached_generation(&store, &path);
        gets.store(0, std::sync::atomic::Ordering::Relaxed);

        let prefetch = AsyncPrefetch {
            start: (5 * part_size) as u64,
            size: part_size * 4,
        };
        assert_eq!(
            store.spawn_async_prefetch(&path, &generation, prefetch, prefetch.start),
            Some((5 * part_size + 4 * part_size) as u64)
        );
        assert_eq!(
            store.spawn_async_prefetch(&path, &generation, prefetch, prefetch.start),
            Some((5 * part_size + 4 * part_size) as u64),
            "an in-flight window still counts as covered"
        );

        tokio::time::sleep(std::time::Duration::from_millis(300)).await;
        assert_eq!(
            gets.load(std::sync::atomic::Ordering::Relaxed),
            1,
            "second prefetch of the same window must not GET"
        );
        assert!(store.cached_part(&path, 5).await.is_some());
    }

    #[tokio::test]
    async fn fetch_slots_released_after_async_prefetch() {
        let part_size = 1024;
        let (store, inner, _dir) = make_store(part_size, MEM, DISK).await;
        let path = Path::from("guard");
        let body = vec![0xBBu8; part_size * 16];
        inner.put(&path, body.clone().into()).await.unwrap();
        store.head(&path).await.unwrap();
        let generation = cached_generation(&store, &path);

        let start_part = 2;
        store.spawn_async_prefetch(
            &path,
            &generation,
            AsyncPrefetch {
                start: (start_part * part_size) as u64,
                size: part_size * 4,
            },
            (start_part * part_size) as u64,
        );

        for _ in 0..100 {
            if store.fetches.lock().unwrap().is_empty() {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        assert!(
            store.fetches.lock().unwrap().is_empty(),
            "fetch slots should be released after prefetch completes"
        );
        assert!(store.cached_part(&path, start_part).await.is_some());
    }

    // The async prefetch frontier must stop at EOF. It advances in fixed
    // windows with no notion of object length, so near the end it would
    // otherwise emit windows starting past EOF: GETs the backend rejects
    // (HTTP 416) and a retrying layer below would re-send forever.
    #[tokio::test]
    async fn async_prefetch_never_fetches_past_eof() {
        let part_size = 1024;
        let len = 2 * FETCH_WINDOW_MIN;
        let inner = Arc::new(InMemory::new());
        let path = Path::from("seq");
        inner.put(&path, vec![7u8; len].into()).await.unwrap();
        let ranges = Arc::new(std::sync::Mutex::new(Vec::new()));
        let recording = Arc::new(RangeRecordingStore {
            inner,
            ranges: ranges.clone(),
        });
        let (store, _dir) = store_over(recording, part_size).await;

        // Sequential reads: the second ramps the window past the minimum and
        // refills the prefetch frontier, whose later windows start past EOF.
        store.get_range(&path, 0..1024).await.unwrap();
        store.get_range(&path, 1024..2048).await.unwrap();
        store.get_range(&path, 2048..3072).await.unwrap();
        // The prefetches run on spawned tasks; give them time to issue GETs.
        tokio::time::sleep(std::time::Duration::from_millis(300)).await;

        let seen = ranges.lock().unwrap().clone();
        assert!(!seen.is_empty());
        for r in &seen {
            if let GetRange::Bounded(b) = r {
                assert!(
                    b.start < len as u64,
                    "GET issued past EOF: {b:?} (object is {len} bytes)"
                );
            }
        }
    }

    // A head miss near EOF on a fully cached object must resolve the head with
    // a HEAD, not lead the demand window's remainder: front-trimming the cached
    // parts reaches EOF, so that GET would start exactly there (HTTP 416) and
    // block the read on a doomed round trip.
    #[tokio::test]
    async fn head_miss_near_eof_does_not_lead_past_cached_request() {
        let part_size = 1024;
        let len = 512 * 1024usize;
        let mem = Arc::new(InMemory::new());
        let path = Path::from("eofhead");
        let body: Vec<u8> = (0..len).map(|i| (i % 251) as u8).collect();
        let ranges = Arc::new(std::sync::Mutex::new(Vec::new()));
        let recording = Arc::new(RangeRecordingStore {
            inner: mem,
            ranges: ranges.clone(),
        });
        let (store, _dir) = store_over(recording, part_size).await;

        // Write-through caches every part but leaves the head invalidated.
        store.put(&path, body.clone().into()).await.unwrap();
        ranges.lock().unwrap().clear();

        let (off, end) = (500 * 1024u64, 504 * 1024u64);
        let r = store
            .get_opts(
                &path,
                GetOptions {
                    range: Some(GetRange::Bounded(off..end)),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        let got = r.bytes().await.unwrap();
        assert_eq!(&got[..], &body[off as usize..end as usize]);

        let seen = ranges.lock().unwrap().clone();
        assert!(
            seen.is_empty(),
            "fully cached near-EOF read needs no ranged GET, got {seen:?}"
        );
        assert!(store.read_head(&path).is_some(), "head restored via HEAD");
    }

    // A resident tail must not let the async prefetch walk front-trim through
    // it and lead a GET starting at EOF: with the head known, the walk is
    // clamped to the object's end.
    #[tokio::test]
    async fn cached_tail_prefetch_does_not_get_past_eof() {
        let part_size = 1024;
        let len = 4 * FETCH_WINDOW_MIN;
        let mem = Arc::new(InMemory::new());
        let path = Path::from("eoftail");
        let body: Vec<u8> = (0..len).map(|i| (i % 251) as u8).collect();
        mem.put(&path, body.clone().into()).await.unwrap();
        let ranges = Arc::new(std::sync::Mutex::new(Vec::new()));
        let recording = Arc::new(RangeRecordingStore {
            inner: mem,
            ranges: ranges.clone(),
        });
        let (store, _dir) = store_over(recording, part_size).await;

        let _ = store.head(&path).await.unwrap();
        let generation = cached_generation(&store, &path);

        for part_id in (len / 2 / part_size)..(len / part_size) {
            store.parts.insert(
                PartKey::new(&path, part_size, &generation, part_id),
                Bytes::copy_from_slice(&body[part_id * part_size..(part_id + 1) * part_size]),
            );
        }
        // Sequential reads ramp the window and refill the frontier; its later
        // windows front-trim across the cached tail up to EOF.
        store.get_range(&path, 0..1024).await.unwrap();
        store.get_range(&path, 1024..2048).await.unwrap();
        store.get_range(&path, 2048..3072).await.unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(300)).await;

        let seen = ranges.lock().unwrap().clone();
        assert!(!seen.is_empty());
        for r in &seen {
            if let GetRange::Bounded(b) = r {
                assert!(
                    b.start < len as u64,
                    "GET issued at/past EOF: {b:?} (object is {len} bytes)"
                );
            }
        }
    }

    /// Records every bounded GET range and delays each GET, so concurrent
    /// reads overlap in flight like they do against S3.
    #[derive(Debug)]
    struct DelayRecordingStore {
        inner: Arc<InMemory>,
        ranges: Arc<std::sync::Mutex<Vec<Range<u64>>>>,
        delay: std::time::Duration,
    }

    impl Display for DelayRecordingStore {
        fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
            write!(f, "DelayRecordingStore")
        }
    }

    #[async_trait]
    impl ObjectStore for DelayRecordingStore {
        async fn get_opts(&self, l: &Path, o: GetOptions) -> object_store::Result<GetResult> {
            if let Some(GetRange::Bounded(r)) = &o.range {
                self.ranges.lock().unwrap().push(r.clone());
            }
            tokio::time::sleep(self.delay).await;
            self.inner.get_opts(l, o).await
        }
        async fn put_opts(
            &self,
            l: &Path,
            p: PutPayload,
            o: PutOptions,
        ) -> object_store::Result<PutResult> {
            self.inner.put_opts(l, p, o).await
        }
        async fn put_multipart_opts(
            &self,
            l: &Path,
            o: PutMultipartOptions,
        ) -> object_store::Result<Box<dyn MultipartUpload>> {
            self.inner.put_multipart_opts(l, o).await
        }
        fn delete_stream(
            &self,
            locations: BoxStream<'static, object_store::Result<Path>>,
        ) -> BoxStream<'static, object_store::Result<Path>> {
            self.inner.delete_stream(locations)
        }
        fn list(&self, p: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
            self.inner.list(p)
        }
        async fn list_with_delimiter(&self, p: Option<&Path>) -> object_store::Result<ListResult> {
            self.inner.list_with_delimiter(p).await
        }
        async fn copy_opts(&self, f: &Path, t: &Path, o: CopyOptions) -> object_store::Result<()> {
            self.inner.copy_opts(f, t, o).await
        }
        async fn rename_opts(
            &self,
            f: &Path,
            t: &Path,
            o: RenameOptions,
        ) -> object_store::Result<()> {
            self.inner.rename_opts(f, t, o).await
        }
    }

    /// Stream a 48 MiB object in 1 MiB chunks pulled off a shared counter by
    /// `concurrency` workers (the shape FUSE readahead + concurrent 9P READs
    /// produce: mostly in order, several in flight, small reorder) and return
    /// `(bytes fetched from the backend clamped to EOF, union coverage, GETs,
    /// final prefetch frontier)`. `warm_head` pre-installs the head, the
    /// production steady state for segments: without the cold head GETs the
    /// frontier can only bootstrap from window-fetch completion notes.
    async fn run_amplification(concurrency: usize, warm_head: bool) -> (u64, u64, usize, u64) {
        let part = DEFAULT_PART_SIZE_BYTES;
        let len: u64 = 48 * 1024 * 1024;
        let chunk: u64 = 1024 * 1024;
        let mem = Arc::new(InMemory::new());
        let path = Path::from("segments/22/seg");
        mem.put(&path, vec![0xA5u8; len as usize].into())
            .await
            .unwrap();
        let ranges = Arc::new(std::sync::Mutex::new(Vec::new()));
        let backend: Arc<dyn ObjectStore> = Arc::new(DelayRecordingStore {
            inner: mem,
            ranges: ranges.clone(),
            delay: std::time::Duration::from_millis(15),
        });
        let dir = tempfile::tempdir().unwrap();
        let parts = HybridCacheBuilder::new()
            .with_name("amp-parts")
            .memory(256 * 1024 * 1024)
            .with_weighter(|_: &PartKey, v: &Bytes| v.len())
            .storage()
            .with_io_engine_config(PsyncIoEngineConfig::new())
            .with_engine_config(
                BlockEngineConfig::new(
                    foyer::DeviceBuilder::build(
                        FsDeviceBuilder::new(dir.path()).with_capacity(512 * 1024 * 1024),
                    )
                    .unwrap(),
                )
                .with_block_size(16 * 1024 * 1024),
            )
            .build()
            .await
            .unwrap();
        let store = Arc::new(PrefetchingObjectStore::with_options(backend, parts, part));

        if warm_head {
            store.cached_head(&path).await.unwrap();
        }

        let next = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let nchunks = len / chunk;
        let mut handles = Vec::new();
        for _ in 0..concurrency {
            let store = store.clone();
            let path = path.clone();
            let next = next.clone();
            handles.push(tokio::spawn(async move {
                loop {
                    let i = next.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    if i >= nchunks {
                        break;
                    }
                    let off = i * chunk;
                    let r = store
                        .get_opts(
                            &path,
                            GetOptions {
                                range: Some(GetRange::Bounded(off..off + chunk)),
                                ..Default::default()
                            },
                        )
                        .await
                        .unwrap();
                    let b = r.bytes().await.unwrap();
                    assert_eq!(b.len() as u64, chunk);
                }
            }));
        }
        for h in handles {
            h.await.unwrap();
        }
        // Let in-flight async prefetches land so their GETs are counted.
        tokio::time::sleep(std::time::Duration::from_millis(300)).await;

        let mut got: Vec<Range<u64>> = ranges.lock().unwrap().clone();
        let fetched: u64 = got.iter().map(|r| r.end.min(len) - r.start).sum();
        got.sort_by_key(|r| r.start);
        let mut union = 0u64;
        let mut covered_to = 0u64;
        for r in &got {
            let s = r.start.max(covered_to);
            let e = r.end.min(len);
            if e > s {
                union += e - s;
            }
            covered_to = covered_to.max(e);
        }
        let frontier = store
            .access_tracker
            .get(&path)
            .map(|e| {
                let h = e.value().lock().unwrap();
                h.streams[..h.len]
                    .iter()
                    .map(|s| s.fetched_until.max(s.scheduled_until))
                    .max()
                    .unwrap_or(0)
            })
            .unwrap_or(0);
        (fetched, union, got.len(), frontier)
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn serial_sequential_amplification_is_bounded() {
        let (fetched, union, gets, _) = run_amplification(1, false).await;
        let amp = fetched as f64 / union as f64;
        println!("serial: {gets} GETs, amplification {amp:.3}x");
        assert!(
            amp <= 1.02,
            "serial sequential read amplification {amp:.3}x > 1.02x ({fetched} fetched / {union} unique)"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn concurrent_sequential_amplification_is_bounded() {
        let (fetched, union, gets, _) = run_amplification(6, false).await;
        let amp = fetched as f64 / union as f64;
        println!("concurrent: {gets} GETs, amplification {amp:.3}x");
        assert!(
            amp <= 1.15,
            "6-way sequential read amplification {amp:.3}x > 1.15x ({fetched} fetched / {union} unique)"
        );
    }

    // Warm head + concurrent readers: the frontier can only bootstrap from
    // window-fetch completion notes, whose leader offsets lag the stream head.
    // The pipeline must engage (frontier reaches EOF) and stay within the
    // amplification bound; a dead pipeline shows amplification 1.00 with a
    // frozen frontier, which only the frontier assertion catches.
    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn warm_head_concurrent_prefetch_stays_alive() {
        let len: u64 = 48 * 1024 * 1024;
        let (fetched, union, gets, frontier) = run_amplification(6, true).await;
        let amp = fetched as f64 / union as f64;
        println!("warm-head concurrent: {gets} GETs, amplification {amp:.3}x, frontier {frontier}");
        assert!(
            amp <= 1.15,
            "warm-head 6-way amplification {amp:.3}x > 1.15x ({fetched} fetched / {union} unique)"
        );
        assert!(
            frontier >= len,
            "prefetch frontier stalled at {frontier} (< {len}): async pipeline never bootstrapped"
        );
    }

    // A demand miss on a part covered by an in-flight async prefetch must join
    // that fetch, not launch a duplicate GET (the demand and prefetch paths
    // share one registry).
    #[tokio::test]
    async fn demand_read_joins_in_flight_async_prefetch() {
        let part_size = 1024;
        let mem = Arc::new(InMemory::new());
        let path = Path::from("join");
        let body: Vec<u8> = (0..part_size * 64).map(|i| (i % 251) as u8).collect();
        mem.put(&path, body.clone().into()).await.unwrap();

        let gets = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let counting: Arc<dyn ObjectStore> = Arc::new(CountingStore {
            inner: mem,
            gets: gets.clone(),
            delay: std::time::Duration::from_millis(80),
        });
        let (store, _dir) = store_over(counting, part_size).await;

        // Warm the head far from the window under test (a head() against
        // InMemory would return the whole payload and pre-fill the parts cache).
        let far = (60 * part_size) as u64;
        let r = store
            .get_opts(
                &path,
                GetOptions {
                    range: Some(GetRange::Bounded(far..far + 10)),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        r.bytes().await.unwrap();
        gets.store(0, std::sync::atomic::Ordering::Relaxed);

        // Registration is synchronous, so the window is in the registry (and its
        // GET in flight, held open by the delay) before the read below starts.
        store.spawn_async_prefetch(
            &path,
            &cached_generation(&store, &path),
            AsyncPrefetch {
                start: 0,
                size: 16 * part_size,
            },
            0,
        );

        let off = (3 * part_size) as u64;
        let r = store
            .get_opts(
                &path,
                GetOptions {
                    range: Some(GetRange::Bounded(off..off + part_size as u64)),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        let got = r.bytes().await.unwrap();
        assert_eq!(&got[..], &body[3 * part_size..4 * part_size]);

        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        assert_eq!(
            gets.load(std::sync::atomic::Ordering::Relaxed),
            1,
            "demand read must join the in-flight prefetch GET"
        );
    }

    // A demand window whose tail is already cached must end-trim: fetch only the
    // missing head, not the whole window over the cached parts.
    #[tokio::test]
    async fn mostly_cached_window_is_not_refetched() {
        let part_size = 1024;
        let mem = Arc::new(InMemory::new());
        let path = Path::from("trim");
        let body: Vec<u8> = (0..part_size * 256).map(|i| (i % 251) as u8).collect();
        mem.put(&path, body.clone().into()).await.unwrap();

        let ranges = Arc::new(std::sync::Mutex::new(Vec::new()));
        let recording = Arc::new(RangeRecordingStore {
            inner: mem,
            ranges: ranges.clone(),
        });
        let (store, _dir) = store_over(recording, part_size).await;

        // Warm the head far away, then hand-cache everything the min fetch
        // window would cover except its first part.
        let far = (250 * part_size) as u64;
        let r = store
            .get_opts(
                &path,
                GetOptions {
                    range: Some(GetRange::Bounded(far..far + 10)),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        r.bytes().await.unwrap();
        let generation = cached_generation(&store, &path);
        for part_id in 1..(FETCH_WINDOW_MIN / part_size) {
            store.parts.insert(
                PartKey::new(&path, part_size, &generation, part_id),
                Bytes::copy_from_slice(&body[part_id * part_size..(part_id + 1) * part_size]),
            );
        }
        ranges.lock().unwrap().clear();

        let r = store
            .get_opts(
                &path,
                GetOptions {
                    range: Some(GetRange::Bounded(0..part_size as u64)),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        let got = r.bytes().await.unwrap();
        assert_eq!(&got[..], &body[..part_size]);

        let seen = ranges.lock().unwrap().clone();
        assert_eq!(
            seen,
            vec![GetRange::Bounded(0..part_size as u64)],
            "window must be end-trimmed at the first cached part"
        );
    }

    // A HybridCache lookup can begin as a disk miss while an in-flight window
    // concurrently inserts the requested part and then leaves the fetch
    // registry. Demand planning must recheck the cache instead of leading a
    // duplicate one-part backend GET.
    #[tokio::test]
    async fn demand_plan_rechecks_part_cached_after_initial_miss() {
        let part_size = 1024;
        let mem = Arc::new(InMemory::new());
        let path = Path::from("demand-cache-race");
        let body = Bytes::from(vec![0x5a; part_size]);
        mem.put(&path, body.clone().into()).await.unwrap();
        let (store, _dir) = store_over(mem, part_size).await;
        store.head(&path).await.unwrap();
        let generation = cached_generation(&store, &path);
        let ctx = store.ctx();
        let key = PartKey::new(&path, part_size, &generation, 0);

        assert!(ctx.parts.get(&key).await.unwrap().is_none());
        ctx.parts.insert(key, body);

        match PrefetchingObjectStore::plan_window(&ctx, &path, &generation, 0, 8, None) {
            WindowPlan::Covered => {}
            WindowPlan::Lead { .. } => {
                panic!("a newly cached demand part must not lead a duplicate GET")
            }
            WindowPlan::Join(_) => panic!("no fetch was registered for the cached part"),
        }
    }

    // A head eviction mid-stream must not re-GET bytes the parts cache already
    // holds: the head-miss demand path plans through the shared registry with
    // front trimming instead of blindly GETting its whole aligned window.
    #[tokio::test]
    async fn head_eviction_does_not_refetch_cached_parts() {
        let part_size = 1024;
        let mem = Arc::new(InMemory::new());
        let path = Path::from("headmiss");
        let body: Vec<u8> = (0..part_size * 512).map(|i| (i % 251) as u8).collect();
        mem.put(&path, body.clone().into()).await.unwrap();

        let ranges = Arc::new(std::sync::Mutex::new(Vec::new()));
        let recording = Arc::new(RangeRecordingStore {
            inner: mem,
            ranges: ranges.clone(),
        });
        let (store, _dir) = store_over(recording, part_size).await;

        // First read caches its min window: parts 0..128.
        let r = store
            .get_opts(
                &path,
                GetOptions {
                    range: Some(GetRange::Bounded(0..1024)),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        r.bytes().await.unwrap();

        store.heads.remove(&path);
        ranges.lock().unwrap().clear();

        let r = store
            .get_opts(
                &path,
                GetOptions {
                    range: Some(GetRange::Bounded(1024..2048)),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        let got = r.bytes().await.unwrap();
        assert_eq!(&got[..], &body[1024..2048]);

        let cached_end = (128 * part_size) as u64;
        let seen = ranges.lock().unwrap().clone();
        for r in &seen {
            if let GetRange::Bounded(b) = r {
                assert!(
                    b.start >= cached_end,
                    "head-miss GET {b:?} overlaps the cached span 0..{cached_end}"
                );
            }
        }
        assert!(
            store.read_head(&path).is_some(),
            "window GET must restore the head"
        );
    }

    // An async prefetch window whose first part is cached must front-trim and
    // fetch the uncached remainder, not silently drop the window (which would
    // leave a hole the frontier claims as covered).
    #[tokio::test]
    async fn cached_start_prefetch_window_fetches_remainder() {
        let part_size = 1024;
        let mem = Arc::new(InMemory::new());
        let path = Path::from("fronttrim");
        let body: Vec<u8> = (0..part_size * 64).map(|i| (i % 251) as u8).collect();
        mem.put(&path, body.clone().into()).await.unwrap();

        let ranges = Arc::new(std::sync::Mutex::new(Vec::new()));
        let recording = Arc::new(RangeRecordingStore {
            inner: mem,
            ranges: ranges.clone(),
        });
        let (store, _dir) = store_over(recording, part_size).await;
        store.head(&path).await.unwrap();
        let generation = cached_generation(&store, &path);

        let start_part = 8;
        store.parts.insert(
            PartKey::new(&path, part_size, &generation, start_part),
            Bytes::copy_from_slice(&body[start_part * part_size..(start_part + 1) * part_size]),
        );

        let covered = store.spawn_async_prefetch(
            &path,
            &generation,
            AsyncPrefetch {
                start: (start_part * part_size) as u64,
                size: 8 * part_size,
            },
            (start_part * part_size) as u64,
        );
        assert_eq!(covered, Some(((start_part + 8) * part_size) as u64));

        for _ in 0..100 {
            if store.cached_part(&path, start_part + 7).await.is_some() {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        for part_id in start_part + 1..start_part + 8 {
            assert!(
                store.cached_part(&path, part_id).await.is_some(),
                "part {part_id} should be fetched by the front-trimmed window"
            );
        }
        let seen = ranges.lock().unwrap().clone();
        assert_eq!(
            seen,
            vec![GetRange::Bounded(
                ((start_part + 1) * part_size) as u64..((start_part + 8) * part_size) as u64
            )],
            "prefetch must fetch exactly the uncached remainder"
        );
    }

    // A ramped stream must not lend its window to a read landing behind its
    // head: after eviction such a read (within the reorder slack) fetches its
    // own min-window span, not the stream's up-to-8 MiB window of cold bytes.
    #[tokio::test]
    async fn lagging_read_after_eviction_fetches_min_window() {
        let part_size = 1024;
        let len = 12 * 1024 * 1024usize;
        let mem = Arc::new(InMemory::new());
        let path = Path::from("lag");
        let body: Vec<u8> = (0..len).map(|i| (i % 251) as u8).collect();
        mem.put(&path, body.clone().into()).await.unwrap();
        let ranges = Arc::new(std::sync::Mutex::new(Vec::new()));
        let recording = Arc::new(RangeRecordingStore {
            inner: mem,
            ranges: ranges.clone(),
        });
        let (store, _dir) = store_over(recording, part_size).await;

        // Seven 64 KiB sequential reads ramp the stream's window to the max.
        let chunk = 64 * 1024u64;
        for i in 0..7u64 {
            let off = i * chunk;
            let r = store
                .get_opts(
                    &path,
                    GetOptions {
                        range: Some(GetRange::Bounded(off..off + chunk)),
                        ..Default::default()
                    },
                )
                .await
                .unwrap();
            r.bytes().await.unwrap();
        }
        for _ in 0..200 {
            if store.fetches.lock().unwrap().is_empty() {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        assert!(store.fetches.lock().unwrap().is_empty());
        let generation = cached_generation(&store, &path);
        for part_id in 0..len / part_size {
            store
                .parts
                .remove(&PartKey::new(&path, part_size, &generation, part_id));
        }
        ranges.lock().unwrap().clear();

        // 256 KiB behind the 448 KiB head, within the 512 KiB slack of a
        // 64 KiB read: absorbed by the stream, but the bytes are cold.
        let off = 2 * chunk;
        let r = store
            .get_opts(
                &path,
                GetOptions {
                    range: Some(GetRange::Bounded(off..off + chunk)),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        let got = r.bytes().await.unwrap();
        assert_eq!(&got[..], &body[off as usize..(off + chunk) as usize]);

        // Bound the fetched volume rather than the exact range: a part that
        // dodged eviction (an in-flight foyer disk write) shifts or splits the
        // led window, but the total must stay a min window, not the ramped one.
        let seen = ranges.lock().unwrap().clone();
        let fetched: u64 = seen
            .iter()
            .map(|r| match r {
                GetRange::Bounded(b) => b.end - b.start,
                _ => 0,
            })
            .sum();
        assert!(
            fetched <= FETCH_WINDOW_MIN as u64,
            "lagging read fetched {fetched} bytes (> min window {FETCH_WINDOW_MIN}): {seen:?}"
        );
    }

    // A cold suffix read (the segment-footer pattern) must cost one GET: the
    // aligned suffix payload's whole parts are cached despite the unaligned
    // start, so the demand read hits them instead of re-fetching the tail,
    // and a repeat suffix read is served from cache.
    #[tokio::test]
    async fn cold_suffix_read_is_one_get_and_warms_the_tail() {
        let part_size = 1024;
        let len = 300 * 1024 + 337; // unaligned length, like a real segment
        let mem = Arc::new(InMemory::new());
        let path = Path::from("segments/aa/seg");
        let body: Vec<u8> = (0..len).map(|i| (i % 251) as u8).collect();
        mem.put(&path, body.clone().into()).await.unwrap();
        let ranges = Arc::new(std::sync::Mutex::new(Vec::new()));
        let recording = Arc::new(RangeRecordingStore {
            inner: mem,
            ranges: ranges.clone(),
        });
        let (store, _dir) = store_over(recording, part_size).await;

        let r = store
            .get_opts(
                &path,
                GetOptions {
                    range: Some(GetRange::Suffix(64)),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        let got = r.bytes().await.unwrap();
        assert_eq!(&got[..], &body[len - 64..]);
        let seen = ranges.lock().unwrap().clone();
        assert_eq!(
            seen.len(),
            1,
            "cold suffix read must be one GET, got {seen:?}"
        );

        ranges.lock().unwrap().clear();
        let r = store
            .get_opts(
                &path,
                GetOptions {
                    range: Some(GetRange::Suffix(64)),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        let got = r.bytes().await.unwrap();
        assert_eq!(&got[..], &body[len - 64..]);
        let seen = ranges.lock().unwrap().clone();
        assert!(
            seen.is_empty(),
            "warm suffix read must be cache-served, got {seen:?}"
        );
    }

    // Regression guard for the RSS pinning bug: cache entries must own their
    // bytes, never alias the (up to 256 MiB) source allocation warmed in.
    #[tokio::test]
    async fn warm_object_parts_own_their_bytes() {
        let (store, _inner, _dir) = make_store(1024, MEM, DISK).await;
        let path = Path::from("own");
        let src = Bytes::from((0..2560u32).map(|i| i as u8).collect::<Vec<u8>>());
        let src_start = src.as_ptr() as usize;
        let src_end = src_start + src.len();
        let result = versioned_result("owned-generation");
        let generation = CacheGeneration::from_put_result(&result).unwrap();
        store.warm_object(&path, src.clone(), &result);

        for part_id in 0..3 {
            let bytes = store
                .parts
                .get(&PartKey::new(&path, 1024, &generation, part_id))
                .await
                .unwrap()
                .unwrap()
                .value()
                .clone();
            let p = bytes.as_ptr() as usize;
            assert!(
                p + bytes.len() <= src_start || p >= src_end,
                "part {part_id} aliases the warm source allocation"
            );
        }
    }

    /// Serves single-chunk payloads sliced zero-copy from one retained
    /// allocation, the shape a window GET arrives in when it lands in one
    /// network chunk, so a test can detect cache entries aliasing the window.
    #[derive(Debug)]
    struct SharedAllocStore {
        data: Bytes,
    }

    impl Display for SharedAllocStore {
        fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
            write!(f, "SharedAllocStore")
        }
    }

    #[async_trait]
    impl ObjectStore for SharedAllocStore {
        async fn get_opts(&self, l: &Path, o: GetOptions) -> object_store::Result<GetResult> {
            let len = self.data.len() as u64;
            let meta = ObjectMeta {
                location: l.clone(),
                last_modified: chrono::DateTime::from_timestamp(1, 0).unwrap(),
                size: len,
                e_tag: None,
                version: None,
            };
            if o.head {
                return Ok(GetResult {
                    payload: GetResultPayload::Stream(
                        stream::empty::<object_store::Result<Bytes>>().boxed(),
                    ),
                    range: 0..0,
                    attributes: Attributes::default(),
                    extensions: Default::default(),
                    meta,
                });
            }
            let range = match o.range {
                Some(GetRange::Bounded(r)) => r.start..r.end.min(len),
                _ => 0..len,
            };
            let slice = self.data.slice(range.start as usize..range.end as usize);
            Ok(GetResult {
                payload: GetResultPayload::Stream(
                    stream::once(async move { Ok::<_, object_store::Error>(slice) }).boxed(),
                ),
                range,
                attributes: Attributes::default(),
                extensions: Default::default(),
                meta,
            })
        }
        async fn put_opts(
            &self,
            _: &Path,
            _: PutPayload,
            _: PutOptions,
        ) -> object_store::Result<PutResult> {
            unimplemented!()
        }
        async fn put_multipart_opts(
            &self,
            _: &Path,
            _: PutMultipartOptions,
        ) -> object_store::Result<Box<dyn MultipartUpload>> {
            unimplemented!()
        }
        fn delete_stream(
            &self,
            _: BoxStream<'static, object_store::Result<Path>>,
        ) -> BoxStream<'static, object_store::Result<Path>> {
            unimplemented!()
        }
        fn list(&self, _: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
            unimplemented!()
        }
        async fn list_with_delimiter(&self, _: Option<&Path>) -> object_store::Result<ListResult> {
            unimplemented!()
        }
        async fn copy_opts(&self, _: &Path, _: &Path, _: CopyOptions) -> object_store::Result<()> {
            unimplemented!()
        }
        async fn rename_opts(
            &self,
            _: &Path,
            _: &Path,
            _: RenameOptions,
        ) -> object_store::Result<()> {
            unimplemented!()
        }
    }

    // Same pinning guard for the window-fetch path: `GetResult::bytes()` on a
    // single-chunk payload returns that chunk zero-copy, so without the owned
    // copy every cached part of the window would alias the backend allocation.
    #[tokio::test]
    async fn window_fetch_parts_own_their_bytes() {
        let part_size = 1024;
        let data = Bytes::from((0..8192u32).map(|i| (i % 251) as u8).collect::<Vec<u8>>());
        let data_start = data.as_ptr() as usize;
        let data_end = data_start + data.len();
        let backend: Arc<dyn ObjectStore> = Arc::new(SharedAllocStore { data: data.clone() });
        let (store, _dir) = store_over(backend, part_size).await;
        let path = Path::from("own-window");

        // head is served without payload, so no parts are cached by this.
        let _ = store.head(&path).await.unwrap();

        let r = store
            .get_opts(
                &path,
                GetOptions {
                    range: Some(GetRange::Bounded(0..2048)),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        let got = r.bytes().await.unwrap();
        assert_eq!(&got[..], &data[..2048]);

        // The min fetch window spans the whole 8-part object in one GET.
        for part_id in 0..8 {
            let bytes = store
                .cached_part(&path, part_id)
                .await
                .expect("cached by the window fetch");
            assert_eq!(&bytes[..], &data[part_id * 1024..(part_id + 1) * 1024]);
            let p = bytes.as_ptr() as usize;
            assert!(
                p + bytes.len() <= data_start || p >= data_end,
                "part {part_id} aliases the window GET allocation"
            );
        }
    }

    #[tokio::test]
    async fn delete_evicts_parts() {
        let (store, _inner, _dir) = make_store(64 * 1024, MEM, DISK).await;
        let path = Path::from("evict-me");
        let payload = vec![7u8; 128 * 1024];
        store.put(&path, payload.clone().into()).await.unwrap();
        // put write-through may not save a head; a GET does, and caches parts.
        let got = store.get(&path).await.unwrap().bytes().await.unwrap();
        assert_eq!(&got[..], &payload[..]);
        assert!(
            store.cached_part(&path, 0).await.is_some(),
            "GET should have admitted part 0"
        );
        object_store::ObjectStoreExt::delete(&store, &path)
            .await
            .unwrap();
        assert!(
            store.cached_part(&path, 0).await.is_none(),
            "delete must drop parts, not just heads"
        );
        assert!(store.read_head(&path).is_none());
    }

    #[tokio::test]
    async fn uncached_get_does_not_insert() {
        let (store, _inner, _dir) = make_store(64 * 1024, MEM, DISK).await;
        let path = Path::from("skip-cache");
        let payload = vec![9u8; 64 * 1024];
        store.put(&path, payload.clone().into()).await.unwrap();
        store.evict_location(&path);
        assert!(store.cached_part(&path, 0).await.is_none());

        let result = store
            .get_opts_uncached(&path, GetOptions::default())
            .await
            .unwrap();
        let got = result.bytes().await.unwrap();
        assert_eq!(&got[..], &payload[..]);
        assert!(
            store.cached_part(&path, 0).await.is_none(),
            "uncached GET must not save_get_result / insert parts"
        );

        let mut skip = GetOptions::default();
        skip.extensions.insert(SkipPartsCache);
        let result = store.get_opts(&path, skip).await.unwrap();
        let got = result.bytes().await.unwrap();
        assert_eq!(&got[..], &payload[..]);
        assert!(
            store.cached_part(&path, 0).await.is_none(),
            "SkipPartsCache must take the uncached path"
        );
    }

    #[tokio::test]
    async fn rss_cap_skips_part_admission() {
        crate::alloc_rss::set_test_rss_envelope(Some(100));
        let (store, _inner, _dir) = make_store(64 * 1024, MEM, DISK).await;
        let store = store.with_admission_cap(50);
        let path = Path::from("over-cap");
        let payload = vec![3u8; 64 * 1024];
        store.put(&path, payload.into()).await.unwrap();
        // write-through would have inserted part 0 if admission allowed it.
        // No head after put-only; use generations+part_counts via evict path:
        // cached_part needs a head, so do a GET (also gated) and assert empty.
        let _ = store.get(&path).await.unwrap().bytes().await.unwrap();
        assert!(
            store.cached_part(&path, 0).await.is_none(),
            "RSS admission ceiling must skip parts.insert"
        );
        crate::alloc_rss::set_test_rss_envelope(None);
    }
}
