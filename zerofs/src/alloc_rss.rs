//! Process RSS pressure used as the clean-cache admission / GC brake.
//!
//! foyer parts weighter is `Bytes.len()`, which is not process RSS. The brake
//! therefore compares jemalloc's resident pages to a host/cgroup pressure
//! threshold. jemalloc's retained statistic is deliberately excluded: it is
//! reusable virtual address space, not resident physical memory.

use std::sync::atomic::{AtomicU64, Ordering};

static RSS_CAP_BYTES: AtomicU64 = AtomicU64::new(0);
const GIB: u64 = 1024 * 1024 * 1024;
const MIN_PRESSURE_SLACK_BYTES: u64 = GIB;
const MAX_PRESSURE_SLACK_BYTES: u64 = 8 * GIB;

#[cfg(test)]
thread_local! {
    static TEST_ENVELOPE: std::cell::Cell<Option<u64>> = const { std::cell::Cell::new(None) };
    static TEST_ALLOCATOR_STATS: std::cell::Cell<Option<(u64, u64)>> = const { std::cell::Cell::new(None) };
    static TEST_SYSTEM_LIMITS: std::cell::Cell<Option<(u64, Option<u64>)>> = const { std::cell::Cell::new(None) };
}

/// Install an explicit pressure threshold. `0` disables the brake.
///
/// Production startup should use [`configure_rss_cap_bytes`] so the threshold
/// comes from the effective physical/cgroup memory limit rather than a cache
/// payload budget. This setter remains the narrow test/administrative seam.
pub fn set_rss_cap_bytes(cap: u64) {
    RSS_CAP_BYTES.store(cap, Ordering::Relaxed);
}

pub fn rss_cap_bytes() -> u64 {
    RSS_CAP_BYTES.load(Ordering::Relaxed)
}

/// Configure and return the resident-pressure threshold for this process.
///
/// The effective limit is the tighter of physical memory and the current
/// cgroup-v2 hierarchy. Ten percent is reserved for the kernel, non-jemalloc
/// mappings, request buffers, and allocator lag, clamped to 1..=8 GiB (or half
/// of a sub-2-GiB limit). If neither limit is observable, retain fail-closed
/// behavior with a conservative cache-plus-slack fallback instead of disabling
/// the brake.
pub fn configure_rss_cap_bytes(configured_clean_cache_bytes: u64) -> u64 {
    let (physical, cgroup) = system_memory_limits();
    let cap = pressure_cap_from_limits(physical, cgroup).unwrap_or_else(|| {
        configured_clean_cache_bytes.saturating_add(pressure_slack(configured_clean_cache_bytes))
    });
    set_rss_cap_bytes(cap);
    cap
}

fn pressure_cap_from_limits(physical: Option<u64>, cgroup: Option<u64>) -> Option<u64> {
    let limit = match (physical.filter(|v| *v > 0), cgroup.filter(|v| *v > 0)) {
        (Some(physical), Some(cgroup)) => physical.min(cgroup),
        (Some(physical), None) => physical,
        (None, Some(cgroup)) => cgroup,
        (None, None) => return None,
    };
    Some(limit.saturating_sub(pressure_slack(limit)))
}

fn pressure_slack(limit: u64) -> u64 {
    (limit / 10)
        .clamp(MIN_PRESSURE_SLACK_BYTES, MAX_PRESSURE_SLACK_BYTES)
        .min(limit / 2)
}

fn system_memory_limits() -> (Option<u64>, Option<u64>) {
    #[cfg(test)]
    if let Some((physical, cgroup)) = TEST_SYSTEM_LIMITS.with(|limits| limits.get()) {
        return (Some(physical), cgroup);
    }
    (physical_memory_bytes(), cgroup_memory_limit_bytes())
}

fn physical_memory_bytes() -> Option<u64> {
    // SAFETY: sysconf has no pointer arguments and these selectors are defined
    // by every Unix target ZeroFS supports.
    let pages = unsafe { libc::sysconf(libc::_SC_PHYS_PAGES) };
    let page_size = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
    if pages <= 0 || page_size <= 0 {
        return None;
    }
    u64::try_from(pages)
        .ok()?
        .checked_mul(u64::try_from(page_size).ok()?)
}

#[cfg(target_os = "linux")]
fn cgroup_memory_limit_bytes() -> Option<u64> {
    let cgroup = std::fs::read_to_string("/proc/self/cgroup").ok()?;
    let relative = cgroup.lines().find_map(|line| line.strip_prefix("0::"))?;
    let root = std::path::Path::new("/sys/fs/cgroup");
    let mut current = root.join(relative.trim_start_matches('/'));
    let mut limit: Option<u64> = None;
    loop {
        if let Ok(raw) = std::fs::read_to_string(current.join("memory.max"))
            && let Ok(value) = raw.trim().parse::<u64>()
            && value > 0
        {
            limit = Some(limit.map_or(value, |known| known.min(value)));
        }
        if current == root || !current.pop() || !current.starts_with(root) {
            break;
        }
    }
    limit
}

#[cfg(not(target_os = "linux"))]
fn cgroup_memory_limit_bytes() -> Option<u64> {
    None
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

    static TEST_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    struct Reset;

    impl Drop for Reset {
        fn drop(&mut self) {
            TEST_ALLOCATOR_STATS.with(|stats| stats.set(None));
            TEST_SYSTEM_LIMITS.with(|limits| limits.set(None));
            set_test_rss_envelope(None);
            set_rss_cap_bytes(0);
        }
    }

    fn set_test_system_limits(physical: u64, cgroup: Option<u64>) {
        TEST_SYSTEM_LIMITS.with(|limits| limits.set(Some((physical, cgroup))));
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
    fn full_clean_cache_plus_overhead_fits_below_physical_pressure_cap() {
        let _lock = TEST_LOCK
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let _reset = Reset;
        set_test_system_limits(96 * GIB, None);
        TEST_ALLOCATOR_STATS.with(|stats| stats.set(Some((70 * GIB, 30 * GIB))));

        configure_rss_cap_bytes(64 * GIB);

        assert_eq!(rss_cap_bytes(), 88 * GIB);
        assert!(rss_cap_bytes() > 64 * GIB);
        assert!(
            !over_rss_cap(),
            "a full 64 GiB cache plus ordinary resident overhead must remain admissible"
        );
    }

    #[test]
    fn hidden_cgroup_limit_bounds_pressure_cap_below_physical_memory() {
        let _lock = TEST_LOCK
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let _reset = Reset;
        set_test_system_limits(128 * GIB, Some(32 * GIB));
        TEST_ALLOCATOR_STATS.with(|stats| stats.set(Some((29 * GIB, 0))));

        configure_rss_cap_bytes(64 * GIB);

        assert_eq!(rss_cap_bytes(), 32 * GIB - (32 * GIB / 10));
        assert!(rss_cap_bytes() < 32 * GIB);
        assert!(
            over_rss_cap(),
            "resident usage inside the cgroup slack must fail closed"
        );
    }
}
