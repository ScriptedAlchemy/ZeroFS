//! Process RSS envelope used as the clean-cache admission / GC brake.
//!
//! foyer parts weighter is `Bytes.len()`, which is not RSS. Reclaim I/O that
//! re-admits deleted 32 MiB segment parts, plus SlateDB point-read fan-out,
//! grows resident+retained on top of an already-full 64 GiB floor. The
//! envelope is the admission ceiling: weighter stays length, RSS is the cap.

use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use tikv_jemalloc_ctl::{epoch, stats};

static RSS_CAP_BYTES: AtomicU64 = AtomicU64::new(0);

/// Envelope reads are throttled: the admission gate runs per cached 128 KiB
/// part, and a jemalloc epoch advance takes the allocator's global stats
/// mutex. RSS moves on millisecond timescales; per-part precision buys
/// nothing.
const SAMPLE_INTERVAL_MS: u64 = 100;

static CACHED_ENVELOPE: AtomicU64 = AtomicU64::new(0);
/// `now_ms()` of the last jemalloc sample; `0` means never sampled.
static LAST_SAMPLE_MS: AtomicU64 = AtomicU64::new(0);

#[cfg(test)]
thread_local! {
    static TEST_ENVELOPE: std::cell::Cell<Option<u64>> = const { std::cell::Cell::new(None) };
    static TEST_CAP: std::cell::Cell<Option<u64>> = const { std::cell::Cell::new(None) };
}

/// Install the configured clean-cache total (or `cgroup_limit - slack`).
/// `0` disables the RSS brake / admission ceiling.
pub fn set_rss_cap_bytes(cap: u64) {
    RSS_CAP_BYTES.store(cap, Ordering::Relaxed);
}

pub fn rss_cap_bytes() -> u64 {
    #[cfg(test)]
    if let Some(v) = TEST_CAP.with(|c| c.get()) {
        return v;
    }
    RSS_CAP_BYTES.load(Ordering::Relaxed)
}

fn now_ms() -> u64 {
    static START: OnceLock<Instant> = OnceLock::new();
    // +1 so a real timestamp is never 0 (the "never sampled" sentinel).
    START.get_or_init(Instant::now).elapsed().as_millis() as u64 + 1
}

/// Resident + retained (or the test override), refreshed from jemalloc at
/// most every [`SAMPLE_INTERVAL_MS`]; between samples this is atomic loads
/// only. `0` if jemalloc is unavailable (lib unit tests without the global
/// allocator).
pub fn jemalloc_rss_envelope() -> u64 {
    #[cfg(test)]
    if let Some(v) = TEST_ENVELOPE.with(|c| c.get()) {
        return v;
    }
    let now = now_ms();
    let last = LAST_SAMPLE_MS.load(Ordering::Relaxed);
    let fresh = last != 0 && now.saturating_sub(last) < SAMPLE_INTERVAL_MS;
    // The CAS elects one sampler per interval; losers use the cached value
    // (one sample old at worst) instead of queueing on jemalloc's mutex.
    if fresh
        || LAST_SAMPLE_MS
            .compare_exchange(last, now, Ordering::Relaxed, Ordering::Relaxed)
            .is_err()
    {
        return CACHED_ENVELOPE.load(Ordering::Relaxed);
    }
    let envelope = read_envelope();
    CACHED_ENVELOPE.store(envelope, Ordering::Relaxed);
    envelope
}

/// One epoch advance, then both stats: a coherent snapshot paying the
/// mallctl lock once, unlike per-stat advances.
fn read_envelope() -> u64 {
    struct Mibs {
        epoch: tikv_jemalloc_ctl::epoch_mib,
        resident: stats::resident_mib,
        retained: stats::retained_mib,
    }
    static MIBS: OnceLock<Option<Mibs>> = OnceLock::new();
    let mibs = MIBS.get_or_init(|| {
        Some(Mibs {
            epoch: epoch::mib().ok()?,
            resident: stats::resident::mib().ok()?,
            retained: stats::retained::mib().ok()?,
        })
    });
    let Some(mibs) = mibs else {
        return 0;
    };
    if mibs.epoch.advance().is_err() {
        return 0;
    }
    let resident = mibs.resident.read().unwrap_or(0) as u64;
    let retained = mibs.retained.read().unwrap_or(0) as u64;
    resident.saturating_add(retained)
}

pub fn over_rss_cap() -> bool {
    over_rss_cap_of(rss_cap_bytes())
}

pub fn over_rss_cap_of(cap: u64) -> bool {
    cap > 0 && jemalloc_rss_envelope() >= cap
}

/// `mallctl("arena.*.purge")` -- return unused dirty pages to the OS.
pub fn purge_arenas() {
    // `()` is zero-sized, so this is mallctl with newlen=0: a command.
    let _ = unsafe { tikv_jemalloc_ctl::raw::write::<()>(b"arena.*.purge\0", ()) };
}

#[cfg(test)]
pub fn set_test_rss_envelope(bytes: Option<u64>) {
    TEST_ENVELOPE.with(|c| c.set(bytes));
}

/// Thread-local cap override so parallel tests don't race on the global.
#[cfg(test)]
pub fn set_test_rss_cap(cap: Option<u64>) {
    TEST_CAP.with(|c| c.set(cap));
}
