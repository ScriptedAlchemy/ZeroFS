//! Bounded-cardinality metrics for fragmented extent reads.
//!
//! Counters are logical/run utilization only. No inode, path, object key,
//! request ID, or error-string labels.

use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(super) struct ReadRunSnapshot {
    pub logical_bytes: u64,
    pub extent_count: u64,
    pub run_count: u64,
    pub unique_segments: u64,
    pub on_store_runs: u64,
    pub on_store_bytes: u64,
    pub peak_run_fetches: usize,
    pub duration_ns: u64,
}

static LAST: Mutex<Option<ReadRunSnapshot>> = Mutex::new(None);

pub(super) struct ReadRunRecorder {
    started: Instant,
    logical_bytes: u64,
    extent_count: u64,
    run_count: u64,
    unique_segments: u64,
    on_store_runs: u64,
    on_store_bytes: u64,
    active: AtomicUsize,
    peak: AtomicUsize,
}

impl ReadRunRecorder {
    pub(super) fn new(logical_bytes: u64, extent_count: u64) -> Self {
        Self {
            started: Instant::now(),
            logical_bytes,
            extent_count,
            run_count: 0,
            unique_segments: 0,
            on_store_runs: 0,
            on_store_bytes: 0,
            active: AtomicUsize::new(0),
            peak: AtomicUsize::new(0),
        }
    }

    pub(super) fn record_plan(
        &mut self,
        run_count: u64,
        unique_segments: u64,
        on_store_runs: u64,
        on_store_bytes: u64,
    ) {
        self.run_count = run_count;
        self.unique_segments = unique_segments;
        self.on_store_runs = on_store_runs;
        self.on_store_bytes = on_store_bytes;
    }

    pub(super) fn enter_fetch(&self) -> FetchGuard<'_> {
        let now = self.active.fetch_add(1, Ordering::Relaxed) + 1;
        self.peak.fetch_max(now, Ordering::Relaxed);
        FetchGuard {
            active: &self.active,
        }
    }

    pub(super) fn finish(&self) -> ReadRunSnapshot {
        let snapshot = ReadRunSnapshot {
            logical_bytes: self.logical_bytes,
            extent_count: self.extent_count,
            run_count: self.run_count,
            unique_segments: self.unique_segments,
            on_store_runs: self.on_store_runs,
            on_store_bytes: self.on_store_bytes,
            peak_run_fetches: self.peak.load(Ordering::Relaxed),
            duration_ns: self.started.elapsed().as_nanos() as u64,
        };
        if let Ok(mut last) = LAST.lock() {
            *last = Some(snapshot);
        }
        snapshot
    }
}

pub(super) struct FetchGuard<'a> {
    active: &'a AtomicUsize,
}

impl Drop for FetchGuard<'_> {
    fn drop(&mut self) {
        self.active.fetch_sub(1, Ordering::Relaxed);
    }
}

#[cfg(test)]
pub(super) fn last_snapshot() -> Option<ReadRunSnapshot> {
    LAST.lock().ok().and_then(|guard| *guard)
}

use crate::fs::inode::InodeId;
use crate::segment::FrameLoc;
use bytes::Bytes;

#[derive(Clone)]
pub(super) struct OnStoreRun {
    pub start_extent: u64,
    pub first: FrameLoc,
    pub total_len: u32,
    pub slots: Vec<(InodeId, u64)>,
}

pub(super) enum PlannedPiece {
    Bytes(Bytes),
    Ready {
        start_extent: u64,
        frames: Vec<Bytes>,
    },
    OnStore(OnStoreRun),
}
