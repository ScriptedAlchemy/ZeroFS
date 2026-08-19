//! Process RSS pressure used as the clean-cache admission / GC brake.
//!
//! foyer parts weighter is `Bytes.len()`, which is not process RSS. The brake
//! therefore compares jemalloc's resident pages to the pressure threshold
//! installed from the validated service memory envelope. jemalloc's retained
//! statistic is deliberately excluded: it is reusable virtual address space,
//! not resident physical memory.

use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use tikv_jemalloc_ctl::{epoch, stats};

static RSS_CAP_BYTES: AtomicU64 = AtomicU64::new(0);

#[cfg(test)]
static TEST_RSS_CAP_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

#[cfg(test)]
thread_local! {
    static TEST_ENVELOPE: std::cell::Cell<Option<u64>> = const { std::cell::Cell::new(None) };
    static TEST_ALLOCATOR_STATS: std::cell::Cell<Option<(u64, u64)>> = const { std::cell::Cell::new(None) };
}

/// Install an explicit pressure threshold. `0` disables the brake.
///
/// Production startup installs the cap from its validated explicit service
/// envelope before any cache/backend construction. This module deliberately
/// does not rediscover cgroups: a container namespace may hide its outer cap.
pub fn set_rss_cap_bytes(cap: u64) {
    RSS_CAP_BYTES.store(cap, Ordering::Relaxed);
}

pub fn rss_cap_bytes() -> u64 {
    RSS_CAP_BYTES.load(Ordering::Relaxed)
}

#[cfg(test)]
pub(crate) struct TestRssCapGuard {
    previous: u64,
    _lock: tokio::sync::MutexGuard<'static, ()>,
}

#[cfg(test)]
impl Drop for TestRssCapGuard {
    fn drop(&mut self) {
        set_rss_cap_bytes(self.previous);
    }
}

#[cfg(test)]
pub(crate) async fn lock_test_rss_cap() -> TestRssCapGuard {
    let lock = TEST_RSS_CAP_LOCK.lock().await;
    TestRssCapGuard {
        previous: rss_cap_bytes(),
        _lock: lock,
    }
}

/// How long a sampled resident value stays fresh. Admission gating runs per
/// cached part (~128 KiB), and every jemalloc mallctl read serializes on the
/// allocator's global stats mutex; between samples the gate is atomic loads.
const SAMPLE_INTERVAL_MS: u64 = 100;

static CACHED_RESIDENT: AtomicU64 = AtomicU64::new(0);
static LAST_SAMPLE_MS: AtomicU64 = AtomicU64::new(0);

/// Milliseconds since the first call, starting at 1 so `0` can mean
/// "never sampled".
fn now_ms() -> u64 {
    static START: OnceLock<Instant> = OnceLock::new();
    START.get_or_init(Instant::now).elapsed().as_millis() as u64 + 1
}

/// jemalloc `stats.resident` after an epoch advance, sampled at most once per
/// [`SAMPLE_INTERVAL_MS`] (losers of the sampling race and callers within the
/// interval get the cached value). `0` if jemalloc is unavailable (lib unit
/// tests without the global allocator).
pub fn jemalloc_resident() -> u64 {
    #[cfg(test)]
    if let Some((resident, _)) = TEST_ALLOCATOR_STATS.with(|stats| stats.get()) {
        return resident;
    }
    let now = now_ms();
    let last = LAST_SAMPLE_MS.load(Ordering::Relaxed);
    let fresh = last != 0 && now.saturating_sub(last) < SAMPLE_INTERVAL_MS;
    if fresh
        || LAST_SAMPLE_MS
            .compare_exchange(last, now, Ordering::Relaxed, Ordering::Relaxed)
            .is_err()
    {
        return CACHED_RESIDENT.load(Ordering::Relaxed);
    }
    let resident = read_resident();
    CACHED_RESIDENT.store(resident, Ordering::Relaxed);
    resident
}

/// One coherent read: advance the epoch, then read `stats.resident` through
/// MIBs resolved once (name lookups also take jemalloc's global mutex).
fn read_resident() -> u64 {
    struct Mibs {
        epoch: tikv_jemalloc_ctl::epoch_mib,
        resident: stats::resident_mib,
    }
    static MIBS: OnceLock<Option<Mibs>> = OnceLock::new();
    let Some(mibs) = MIBS.get_or_init(|| {
        Some(Mibs {
            epoch: epoch::mib().ok()?,
            resident: stats::resident::mib().ok()?,
        })
    }) else {
        return 0;
    };
    if mibs.epoch.advance().is_err() {
        return 0;
    }
    mibs.resident.read().unwrap_or(0) as u64
}

/// Resident allocator pages, or the test override. Retained virtual mappings
/// are intentionally not pressure.
pub fn jemalloc_rss_envelope() -> u64 {
    #[cfg(test)]
    if let Some(v) = TEST_ENVELOPE.with(|c| c.get()) {
        return v;
    }
    jemalloc_resident()
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

/// Snapshot of jemalloc memory statistics, for the RPC status surface and
/// Prometheus. One epoch advance, then every stat, so the values are fresh
/// and mutually coherent. Cold-path (scrapes and status calls); the hot
/// admission gate uses the throttled [`jemalloc_rss_envelope`] instead.
#[derive(Clone, Copy, Default)]
pub struct JemallocMemStats {
    /// Bytes actively allocated by the application.
    pub allocated: u64,
    /// Bytes in physically resident pages mapped by the allocator.
    pub resident: u64,
    /// Bytes in active pages mapped by the allocator.
    pub mapped: u64,
    /// Bytes in virtual memory mappings retained for future reuse.
    pub retained: u64,
    /// Bytes dedicated to allocator metadata.
    pub metadata: u64,
}

impl JemallocMemStats {
    pub fn read() -> Self {
        if epoch::mib().and_then(|e| e.advance()).is_err() {
            return Self::default();
        }
        Self {
            allocated: stats::allocated::read().unwrap_or(0) as u64,
            resident: stats::resident::read().unwrap_or(0) as u64,
            mapped: stats::mapped::read().unwrap_or(0) as u64,
            retained: stats::retained::read().unwrap_or(0) as u64,
            metadata: stats::metadata::read().unwrap_or(0) as u64,
        }
    }
}

#[cfg(test)]
pub fn set_test_rss_envelope(bytes: Option<u64>) {
    TEST_ENVELOPE.with(|c| c.set(bytes));
}

#[cfg(test)]
mod tests {
    use super::*;

    const GIB: u64 = 1024 * 1024 * 1024;
    struct Reset;

    impl Drop for Reset {
        fn drop(&mut self) {
            TEST_ALLOCATOR_STATS.with(|stats| stats.set(None));
            set_test_rss_envelope(None);
            set_rss_cap_bytes(0);
        }
    }

    #[tokio::test]
    async fn retained_virtual_mappings_do_not_count_as_resident_pressure() {
        let _rss_cap_guard = lock_test_rss_cap().await;
        let _reset = Reset;
        TEST_ALLOCATOR_STATS.with(|stats| stats.set(Some((2 * GIB, 80 * GIB))));
        set_rss_cap_bytes(8 * GIB);

        assert_eq!(jemalloc_rss_envelope(), 2 * GIB);
        assert!(
            !over_rss_cap(),
            "retained-only growth must not trip pressure"
        );
    }

    #[tokio::test]
    async fn validated_service_envelope_allows_full_clean_cache_plus_overhead() {
        let _rss_cap_guard = lock_test_rss_cap().await;
        let _reset = Reset;
        set_rss_cap_bytes(88 * GIB);
        TEST_ALLOCATOR_STATS.with(|stats| stats.set(Some((70 * GIB, 30 * GIB))));

        assert_eq!(rss_cap_bytes(), 88 * GIB);
        assert!(rss_cap_bytes() > 64 * GIB);
        assert!(
            !over_rss_cap(),
            "a full 64 GiB cache plus ordinary resident overhead must remain admissible"
        );
    }

    #[tokio::test]
    async fn validated_service_envelope_trips_before_its_hard_limit() {
        let _rss_cap_guard = lock_test_rss_cap().await;
        let _reset = Reset;
        set_rss_cap_bytes(56 * GIB);
        TEST_ALLOCATOR_STATS.with(|stats| stats.set(Some((57 * GIB, 80 * GIB))));

        assert_eq!(jemalloc_rss_envelope(), 57 * GIB);
        assert!(
            over_rss_cap(),
            "resident usage above the validated service cap must fail closed"
        );
    }

    #[tokio::test]
    async fn test_cap_guard_restores_previous_value() {
        let guard = super::lock_test_rss_cap().await;
        let previous = guard.previous;
        super::set_rss_cap_bytes(previous.wrapping_add(1));
        drop(guard);
        assert_eq!(super::rss_cap_bytes(), previous);
    }
}
