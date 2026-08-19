//! Process RSS envelope used as the clean-cache admission / GC brake.
//!
//! foyer parts weighter is `Bytes.len()`, which is not RSS. Reclaim I/O that
//! re-admits deleted 32 MiB segment parts, plus SlateDB point-read fan-out,
//! grows resident+retained on top of an already-full 64 GiB floor. The
//! envelope is the admission ceiling: weighter stays length, RSS is the cap.

use std::sync::atomic::{AtomicU64, Ordering};

static RSS_CAP_BYTES: AtomicU64 = AtomicU64::new(0);

#[cfg(test)]
static TEST_RSS_CAP_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

#[cfg(test)]
thread_local! {
    static TEST_ENVELOPE: std::cell::Cell<Option<u64>> = const { std::cell::Cell::new(None) };
}

/// Install the configured clean-cache total (or `cgroup_limit - slack`).
/// `0` disables the RSS brake / admission ceiling.
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

fn advance_epoch() -> bool {
    tikv_jemalloc_ctl::epoch::mib()
        .and_then(|e| e.advance())
        .is_ok()
}

/// jemalloc `stats.resident` after an epoch advance. `0` if jemalloc is
/// unavailable (lib unit tests without the global allocator).
pub fn jemalloc_resident() -> u64 {
    if !advance_epoch() {
        return 0;
    }
    tikv_jemalloc_ctl::stats::resident::read().unwrap_or(0) as u64
}

/// jemalloc `stats.retained` after an epoch advance.
pub fn jemalloc_retained() -> u64 {
    if !advance_epoch() {
        return 0;
    }
    tikv_jemalloc_ctl::stats::retained::read().unwrap_or(0) as u64
}

/// Resident + retained, or the test override.
pub fn jemalloc_rss_envelope() -> u64 {
    #[cfg(test)]
    if let Some(v) = TEST_ENVELOPE.with(|c| c.get()) {
        return v;
    }
    jemalloc_resident().saturating_add(jemalloc_retained())
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
    #[tokio::test]
    async fn test_cap_guard_restores_previous_value() {
        let guard = super::lock_test_rss_cap().await;
        let previous = guard.previous;
        super::set_rss_cap_bytes(previous.wrapping_add(1));
        drop(guard);
        assert_eq!(super::rss_cap_bytes(), previous);
    }
}
