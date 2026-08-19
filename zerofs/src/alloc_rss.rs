//! Process RSS pressure used as the clean-cache admission / GC brake.
//!
//! foyer parts weighter is `Bytes.len()`, which is not process RSS. The brake
//! therefore compares jemalloc's resident pages to the pressure threshold
//! installed from the validated service memory envelope. jemalloc's retained
//! statistic is deliberately excluded: it is reusable virtual address space,
//! not resident physical memory.

use std::sync::atomic::{AtomicU64, Ordering};

static RSS_CAP_BYTES: AtomicU64 = AtomicU64::new(0);

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

fn advance_epoch() -> bool {
    tikv_jemalloc_ctl::epoch::mib()
        .and_then(|e| e.advance())
        .is_ok()
}

/// jemalloc `stats.resident` after an epoch advance. `0` if jemalloc is
/// unavailable (lib unit tests without the global allocator).
pub fn jemalloc_resident() -> u64 {
    #[cfg(test)]
    if let Some((resident, _)) = TEST_ALLOCATOR_STATS.with(|stats| stats.get()) {
        return resident;
    }
    if !advance_epoch() {
        return 0;
    }
    tikv_jemalloc_ctl::stats::resident::read().unwrap_or(0) as u64
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

#[cfg(test)]
pub fn set_test_rss_envelope(bytes: Option<u64>) {
    TEST_ENVELOPE.with(|c| c.set(bytes));
}

#[cfg(test)]
mod tests {
    use super::*;

    const GIB: u64 = 1024 * 1024 * 1024;
    static TEST_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    struct Reset;

    impl Drop for Reset {
        fn drop(&mut self) {
            TEST_ALLOCATOR_STATS.with(|stats| stats.set(None));
            set_test_rss_envelope(None);
            set_rss_cap_bytes(0);
        }
    }

    #[test]
    fn retained_virtual_mappings_do_not_count_as_resident_pressure() {
        let _lock = TEST_LOCK
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let _reset = Reset;
        TEST_ALLOCATOR_STATS.with(|stats| stats.set(Some((2 * GIB, 80 * GIB))));
        set_rss_cap_bytes(8 * GIB);

        assert_eq!(jemalloc_rss_envelope(), 2 * GIB);
        assert!(
            !over_rss_cap(),
            "retained-only growth must not trip pressure"
        );
    }

    #[test]
    fn validated_service_envelope_allows_full_clean_cache_plus_overhead() {
        let _lock = TEST_LOCK
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
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

    #[test]
    fn validated_service_envelope_trips_before_its_hard_limit() {
        let _lock = TEST_LOCK
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let _reset = Reset;
        set_rss_cap_bytes(56 * GIB);
        TEST_ALLOCATOR_STATS.with(|stats| stats.set(Some((57 * GIB, 80 * GIB))));

        assert_eq!(jemalloc_rss_envelope(), 57 * GIB);
        assert!(
            over_rss_cap(),
            "resident usage above the validated service cap must fail closed"
        );
    }
}
