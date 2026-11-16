use super::*;
use std::collections::BTreeMap;
use std::io;
use std::path::{Path, PathBuf};
use std::task::Poll;

const MIB: u64 = 1024 * 1024;
const GIB: u64 = 1024 * MIB;

fn fixture(files: &[(&str, &str)]) -> anyhow::Result<DetectedLimits> {
    let files: BTreeMap<_, _> = files
        .iter()
        .map(|(k, v)| (PathBuf::from(k), v.as_bytes().to_vec()))
        .collect();
    detect_limits_with(|path| {
        files
            .get(path)
            .cloned()
            .ok_or_else(|| io::Error::from(io::ErrorKind::NotFound))
    })
}

fn limits() -> DetectedLimits {
    fixture(&[("/proc/meminfo", "MemTotal: 8388608 kB\nSwapTotal: 0 kB\n")]).unwrap()
}

fn example() -> PlanRequirements {
    // These are proposed quotas for the unmeasured 4 GiB qualification target.
    let rows = [
        (PoolKind::Runtime, 256, 256, "fixed runtime"),
        (PoolKind::Metadata, 128, 512, "metadata and replay"),
        (PoolKind::Dirty, 128, 768, "atomic segment and writeback"),
        (
            PoolKind::Protocol,
            256,
            512,
            "largest request and its decode copy",
        ),
        (PoolKind::Io, 64, 384, "one remote stream"),
        (PoolKind::Progress, 128, 128, "one complete flush"),
        (PoolKind::Clean, 0, 512, "optional clean cache"),
        (
            PoolKind::Headroom,
            1024,
            1024,
            "resident and kernel headroom",
        ),
    ];
    PlanRequirements {
        pools: rows
            .into_iter()
            .map(|(kind, minimum, maximum, name)| PoolRequest {
                kind,
                requirements: vec![MemoryRequirement {
                    name: name.into(),
                    bytes: minimum * MIB,
                }],
                maximum_bytes: maximum * MIB,
                weight: 1,
                max_entries: 64,
                max_waiters: 8,
            })
            .collect(),
    }
}

fn plan(
    total: u64,
    requirements: &PlanRequirements,
    volatile: VolatileBudget,
) -> anyhow::Result<ResolvedMemoryPlan> {
    ResolvedMemoryPlan::resolve(
        MemorySelector::Total(total),
        &limits(),
        requirements,
        volatile,
    )
}

#[test]
fn memory_selector_requires_exactly_one_valid_selector() {
    for raw in [
        "",
        "total = '4 GiB'\nfraction_of_limit = 0.5",
        "total = '0 B'",
        "total = '-1 GiB'",
        "total = '4 giraffes'",
        "total = '18446744073709551616 B'",
        "total = '18446744073709551615 GiB'",
        "total = '1.5 GiB'",
        "fraction_of_limit = 0.0",
        "fraction_of_limit = -0.1",
        "fraction_of_limit = 1.01",
        "fraction_of_limit = nan",
        "fraction_of_limit = inf",
        "totla = '4 GiB'",
    ] {
        assert!(toml::from_str::<MemorySelector>(raw).is_err(), "{raw}");
    }
    for raw in [
        "total = '4 GiB'",
        "total = '4096MiB'",
        "total = '4294967296 B'",
    ] {
        assert_eq!(
            toml::from_str::<MemorySelector>(raw).unwrap(),
            MemorySelector::Total(4 * GIB)
        );
    }
}

#[test]
fn memory_selector_fraction_requires_an_observed_limit_and_rounds_down() {
    let unavailable = fixture(&[]).unwrap();
    assert!(
        MemorySelector::FractionOfLimit(0.5)
            .resolve(&unavailable)
            .is_err()
    );
    assert_eq!(
        MemorySelector::Total(4 * GIB)
            .resolve(&unavailable)
            .unwrap()
            .bytes,
        4 * GIB
    );
    assert_eq!(
        MemorySelector::FractionOfLimit(0.125)
            .resolve(&limits())
            .unwrap()
            .bytes,
        GIB
    );
    let small = fixture(&[("/proc/meminfo", "MemTotal: 1 kB")]).unwrap();
    assert_eq!(
        MemorySelector::FractionOfLimit(0.1)
            .resolve(&small)
            .unwrap()
            .bytes,
        102
    );
    for invalid in [0.0, -1.0, f64::NAN, f64::INFINITY, 1.1] {
        assert!(
            MemorySelector::FractionOfLimit(invalid)
                .resolve(&limits())
                .is_err()
        );
    }
    assert!(
        MemorySelector::FractionOfLimit(f64::MIN_POSITIVE)
            .resolve(&small)
            .is_err()
    );
}

#[test]
fn memory_detector_reports_current_ancestor_hard_and_high_sources() {
    let detected = fixture(&[
        ("/proc/self/cgroup", "0::/parent/service\n"),
        (
            "/proc/self/mountinfo",
            "36 25 0:32 / /cg rw - cgroup2 cgroup rw\n",
        ),
        ("/cg/parent/service/memory.max", "6442450944"),
        ("/cg/parent/service/memory.high", "5368709120"),
        ("/cg/parent/memory.max", "8589934592"),
        ("/cg/parent/memory.high", "4294967296"),
        ("/cg/memory.max", "max"),
        ("/cg/memory.high", "max"),
        ("/proc/meminfo", "MemTotal: 16777216 kB\nSwapTotal: 0 kB\n"),
    ])
    .unwrap();
    let basis = detected.effective_limit().unwrap();
    assert_eq!(basis.bytes, 4 * GIB);
    assert_eq!(basis.role, LimitRole::SharedAncestor);
    assert_eq!(basis.source, Path::new("/cg/parent/memory.high"));
    assert_eq!(detected.observations.len(), 8);
    let error = MemorySelector::Total(5 * GIB)
        .resolve(&detected)
        .unwrap_err();
    assert!(error.to_string().contains("operational"));
    assert!(error.to_string().contains("/cg/parent/memory.high"));
}

#[test]
fn memory_detector_missing_or_denied_is_unavailable_without_a_host_guess() {
    let missing = fixture(&[]).unwrap();
    assert!(missing.effective_limit().is_none());
    assert!(
        missing
            .observations
            .iter()
            .all(|row| matches!(row.value, Observation::Unavailable { .. }))
    );
    let denied =
        detect_limits_with(|_| Err(io::Error::from(io::ErrorKind::PermissionDenied))).unwrap();
    assert!(denied.effective_limit().is_none());
    assert!(denied.observations.iter().all(|row| matches!(&row.value, Observation::Unavailable { reason } if reason.contains("PermissionDenied"))));
    let partial = fixture(&[
        ("/proc/self/cgroup", "0::/service"),
        (
            "/proc/self/mountinfo",
            "36 25 0:32 / /cg rw - cgroup2 cgroup rw",
        ),
        ("/cg/service/memory.max", "4294967296"),
    ])
    .unwrap();
    assert_eq!(partial.effective_limit().unwrap().bytes, 4 * GIB);
    assert!(
        partial
            .observations
            .iter()
            .any(|row| row.source == Path::new("/cg/service/memory.high")
                && matches!(row.value, Observation::Unavailable { .. }))
    );
}

#[test]
fn memory_detector_preserves_zero_and_rejects_malformed_limits_and_overflow() {
    for value in ["zero", "-1", "18446744073709551616", "", "1 2"] {
        let error = fixture(&[
            ("/proc/self/cgroup", "0::/"),
            (
                "/proc/self/mountinfo",
                "36 25 0:32 / /cg rw - cgroup2 cgroup rw",
            ),
            ("/cg/memory.max", value),
        ])
        .unwrap_err();
        assert!(error.to_string().contains("/cg/memory.max"));
    }
    let zero = fixture(&[("/proc/meminfo", "MemTotal: 0 kB\nSwapTotal: 0 kB")]).unwrap();
    assert_eq!(zero.effective_limit().unwrap().bytes, 0);
    assert!(MemorySelector::Total(1).resolve(&zero).is_err());
    assert!(fixture(&[("/proc/meminfo", "MemTotal: 18446744073709551615 kB")]).is_err());
    assert!(fixture(&[("/proc/meminfo", "MemTotal: 10 MB")]).is_err());
}

#[test]
fn memory_detector_maps_escaped_mounts_and_private_namespaces() {
    for (current, mounts, path) in [
        (
            "/host/service",
            "36 25 0:32 /host /cg\\040space rw - cgroup2 cgroup rw",
            "/cg space/service",
        ),
        (
            "/service",
            "36 25 0:32 /host/ns /cg rw - cgroup2 cgroup rw",
            "/cg/service",
        ),
        (
            "/host/nested/service",
            "36 25 0:32 /host /cg rw - cgroup2 cgroup rw\n37 25 0:32 /host/nested /nested rw - cgroup2 cgroup rw",
            "/nested/service",
        ),
    ] {
        let source = format!("{path}/memory.max");
        let proc = format!("0::{current}");
        let detected = fixture(&[
            ("/proc/self/cgroup", &proc),
            ("/proc/self/mountinfo", mounts),
            (&source, "4294967296"),
        ])
        .unwrap();
        assert_eq!(
            detected.effective_limit().unwrap().source,
            Path::new(&source)
        );
        assert_eq!(
            detected.effective_limit().unwrap().role,
            LimitRole::Dedicated
        );
    }
}

#[test]
fn memory_detector_nested_binds_do_not_hide_visible_ancestor_limits() {
    for (current, outer_root, nested_root) in [
        ("/parent/service", "/", "/parent/service"),
        ("/parent/service", "/host/ns", "/host/ns/parent/service"),
    ] {
        for file in ["memory.max", "memory.high"] {
            let ancestor = format!("/cg/parent/{file}");
            for reverse in [false, true] {
                let mut mounts = [
                    format!("36 25 0:32 {outer_root} /cg rw - cgroup2 cgroup rw"),
                    format!("37 25 0:32 {nested_root} /nested rw - cgroup2 cgroup rw"),
                ];
                if reverse {
                    mounts.reverse();
                }
                let detected = fixture(&[
                    ("/proc/self/cgroup", &format!("0::{current}")),
                    ("/proc/self/mountinfo", &mounts.join("\n")),
                    ("/nested/memory.max", "8589934592"),
                    ("/cg/parent/service/memory.max", "8589934592"),
                    (&ancestor, "4294967296"),
                ])
                .unwrap();
                let limit = detected.effective_limit().unwrap();
                assert_eq!(limit.bytes, 4 * GIB);
                assert_eq!(limit.role, LimitRole::SharedAncestor);
                assert_eq!(limit.source, Path::new(&ancestor));
                assert!(MemorySelector::Total(5 * GIB).resolve(&detected).is_err());
            }
        }
    }
}

#[test]
fn memory_detector_retains_readable_aliases_when_another_mount_is_denied() {
    let files = BTreeMap::from([
        ("/proc/self/cgroup", "0::/parent/service"),
        (
            "/proc/self/mountinfo",
            "36 25 0:32 / /denied rw - cgroup2 cgroup rw\n37 25 0:32 / /cg rw - cgroup2 cgroup rw",
        ),
        ("/cg/parent/service/memory.max", "4294967296"),
    ]);
    let detected = detect_limits_with(|path| {
        if path.starts_with("/denied") {
            return Err(io::Error::from(io::ErrorKind::PermissionDenied));
        }
        files
            .get(path.to_str().unwrap())
            .map(|value| value.as_bytes().to_vec())
            .ok_or_else(|| io::Error::from(io::ErrorKind::NotFound))
    })
    .unwrap();
    assert_eq!(detected.effective_limit().unwrap().bytes, 4 * GIB);
    assert!(detected.observations.iter().any(|row| {
        row.source == Path::new("/denied/parent/service/memory.max")
            && matches!(&row.value, Observation::Unavailable { reason } if reason.contains("PermissionDenied"))
    }));
}

#[test]
fn memory_planner_fits_four_gib_and_names_a_requirement_that_does_not_fit() {
    let receipt = plan(4 * GIB, &example(), VolatileBudget::default()).unwrap();
    assert_eq!(
        receipt
            .pools()
            .iter()
            .map(|p| p.capacity_bytes)
            .sum::<u64>(),
        4 * GIB
    );
    assert!(receipt.pool(PoolKind::Progress).capacity_bytes >= 128 * MIB);
    let error = plan(512 * MIB, &example(), VolatileBudget::default()).unwrap_err();
    assert!(
        error
            .to_string()
            .contains("largest request and its decode copy"),
        "{error:#}"
    );
    let json = serde_json::to_value(&receipt).unwrap();
    assert_eq!(json["enforcement_active"], false);
    assert_eq!(json["schema_version"], 1);
    assert_eq!(json["budget"]["bytes"], 4 * GIB);
}

#[test]
fn memory_planner_volatile_alias_is_resolved_once_with_two_ownership_stages() {
    use crate::writeback::config::WritebackAccessMode;
    let base = r#"
[cache]
dir = "/tmp/clean"
disk_size_gb = 1.0
[storage]
url = "file:///tmp/backend"
encryption_password = "test"
[servers.nbd]
addresses = ["127.0.0.1:10809"]
[writeback]
enabled = true
dir = "/tmp/journal"
memory_size_gb = 1.0
disk_size_gb = 8.0
min_free_gb = 1.0
"#;
    let shared = "\n[filesystem]\nwrite_ack_mode = 'volatile_memory'\nvolatile_memory_gb = 0.2\n";
    let legacy = base.replace(
        "[servers.nbd]",
        "[servers.nbd]\nwrite_ack_mode = 'volatile_memory'\nvolatile_memory_gb = 0.2",
    );
    let mut dirty_minima = Vec::new();
    for text in [
        format!("{base}{shared}"),
        legacy.clone(),
        format!("{legacy}{shared}"),
    ] {
        let settings: crate::config::Settings = toml::from_str(&text).unwrap();
        let resolved = settings
            .filesystem_write_ack_settings(WritebackAccessMode::ReadWrite)
            .unwrap();
        let planned = plan(4 * GIB, &example(), resolved.into()).unwrap();
        dirty_minima.push(planned.pool(PoolKind::Dirty).minimum_bytes);
        assert_eq!(planned.volatile().bytes_per_stage, 200_000_000);
    }
    assert_eq!(dirty_minima, vec![128 * MIB + 400_000_000; 3]);
}

#[test]
fn memory_planner_rejects_overflow_and_preserves_explicit_maxima() {
    let mut requests = example();
    requests.pools[0].requirements.push(MemoryRequirement {
        name: "overflow".into(),
        bytes: u64::MAX,
    });
    assert!(
        plan(4 * GIB, &requests, VolatileBudget::default())
            .unwrap_err()
            .to_string()
            .contains("overflow")
    );
    let mut requests = example();
    requests.pools[2].requirements.push(MemoryRequirement {
        name: "explicit dirty maximum".into(),
        bytes: 4 * GIB,
    });
    let error = plan(4 * GIB, &requests, VolatileBudget::default()).unwrap_err();
    assert!(
        error.to_string().contains("explicit dirty maximum"),
        "{error:#}"
    );
    let volatile = VolatileBudget {
        bytes_per_stage: u64::MAX,
        source: "fixture".into(),
    };
    assert!(plan(4 * GIB, &example(), volatile).is_err());
}

fn pool(bytes: u64, entries: u64, waiters: usize) -> MemoryPool {
    MemoryPool::new(bytes, entries, waiters)
}

#[tokio::test]
async fn memory_leases_fill_release_and_zero_bytes_still_take_an_entry() {
    let pool = pool(10, 2, 1);
    let first = pool.acquire(10).await.unwrap();
    let empty = pool.try_acquire(0).unwrap();
    assert_eq!(pool.usage().entries, 2);
    assert_eq!(pool.try_acquire(0).unwrap_err(), LeaseError::Unavailable);
    let mut blocked = Box::pin(pool.acquire(1));
    assert!(matches!(futures::poll!(&mut blocked), Poll::Pending));
    assert_eq!(pool.usage().waiters, 1);
    assert_eq!(pool.acquire(1).await.unwrap_err(), LeaseError::WaitersFull);
    drop(first);
    let next = blocked.await.unwrap();
    assert_eq!(pool.usage().bytes, 1);
    drop(empty);
    drop(next);
    assert_eq!(pool.usage(), PoolUsage::default());
    assert!(matches!(
        pool.acquire(11).await,
        Err(LeaseError::TooLarge { .. })
    ));
}

#[tokio::test]
async fn memory_leases_cancel_fifo_head_without_leaking_or_bypassing_it() {
    let pool = pool(10, 4, 2);
    let held = pool.try_acquire(8).unwrap();
    let mut head = Box::pin(pool.acquire(9));
    assert!(matches!(futures::poll!(&mut head), Poll::Pending));
    let mut tail = Box::pin(pool.acquire(2));
    assert!(matches!(futures::poll!(&mut tail), Poll::Pending));
    assert!(pool.try_acquire(1).is_err());
    drop(head);
    let tail = tail.await.unwrap();
    assert_eq!(pool.usage().bytes, 10);
    drop(tail);
    drop(held);
    assert_eq!(pool.usage(), PoolUsage::default());
}

#[tokio::test]
async fn memory_leases_cancel_after_wake_and_close_waiters() {
    let pool = pool(1, 1, 2);
    let held = pool.try_acquire(1).unwrap();
    let mut waiting = Box::pin(pool.acquire(1));
    assert!(matches!(futures::poll!(&mut waiting), Poll::Pending));
    drop(held);
    drop(waiting);
    assert_eq!(pool.usage(), PoolUsage::default());
    let held = pool.try_acquire(1).unwrap();
    let mut waiting = Box::pin(pool.acquire(1));
    assert!(matches!(futures::poll!(&mut waiting), Poll::Pending));
    pool.close();
    assert_eq!(waiting.await.unwrap_err(), LeaseError::Closed);
    drop(held);
    assert_eq!(pool.usage(), PoolUsage::default());
}

#[test]
fn memory_leases_charge_backing_capacity_through_bytes_clones_and_slices() {
    let pool = pool(4096, 1, 0);
    let lease = pool.try_acquire(4096).unwrap();
    let bytes = lease.into_bytes(vec![7; 4096]).unwrap();
    let clone = bytes.clone();
    let slice = clone.slice(0..1);
    drop(bytes);
    drop(clone);
    assert_eq!(pool.usage().bytes, 4096);
    assert!(pool.try_acquire(1).is_err());
    drop(slice);
    assert_eq!(pool.usage(), PoolUsage::default());
    let lease = pool.try_acquire(1).unwrap();
    assert!(lease.into_bytes(Vec::with_capacity(2)).is_err());
    assert_eq!(pool.usage(), PoolUsage::default());
}

#[test]
fn memory_leases_never_wrap_byte_or_entry_counts() {
    let pool = pool(u64::MAX, u64::MAX, 0);
    let held = pool.try_acquire(u64::MAX).unwrap();
    assert_eq!(pool.try_acquire(1).unwrap_err(), LeaseError::Unavailable);
    drop(held);
    assert_eq!(pool.usage(), PoolUsage::default());
}

#[test]
fn memory_detector_zero_cgroup_limit_is_finite_and_swap_adds_no_capacity() {
    for file in ["memory.max", "memory.high"] {
        let source = format!("/cg/{file}");
        let detected = fixture(&[
            ("/proc/self/cgroup", "0::/"),
            (
                "/proc/self/mountinfo",
                "36 25 0:32 / /cg rw - cgroup2 cgroup rw",
            ),
            (&source, "0"),
            (
                "/proc/meminfo",
                "MemTotal: 8388608 kB\nSwapTotal: 16777216 kB",
            ),
        ])
        .unwrap();
        assert_eq!(detected.effective_limit().unwrap().bytes, 0);
        assert!(MemorySelector::Total(1).resolve(&detected).is_err());
    }
    let host = fixture(&[(
        "/proc/meminfo",
        "MemTotal: 8388608 kB\nSwapTotal: 16777216 kB",
    )])
    .unwrap();
    assert_eq!(host.effective_limit().unwrap().bytes, 8 * GIB);
    assert_eq!(host.effective_limit().unwrap().role, LimitRole::HostWide);
}

#[test]
fn memory_selector_preserves_u64_limits_without_float_roundup() {
    let detected = fixture(&[
        ("/proc/self/cgroup", "0::/"),
        (
            "/proc/self/mountinfo",
            "36 25 0:32 / /cg rw - cgroup2 cgroup rw",
        ),
        ("/cg/memory.max", "18446744073709551615"),
    ])
    .unwrap();
    assert_eq!(
        MemorySelector::FractionOfLimit(1.0)
            .resolve(&detected)
            .unwrap()
            .bytes,
        u64::MAX
    );
    assert_eq!(
        MemorySelector::FractionOfLimit(0.5)
            .resolve(&detected)
            .unwrap()
            .bytes,
        u64::MAX / 2
    );
}

#[test]
fn memory_plan_unavailable_envelope_has_reasons_without_a_numeric_guess() {
    let detected = fixture(&[]).unwrap();
    let planned = ResolvedMemoryPlan::resolve(
        MemorySelector::Total(4 * GIB),
        &detected,
        &example(),
        VolatileBudget::default(),
    )
    .unwrap();
    let value = serde_json::to_value(&planned).unwrap();
    assert!(value["budget"]["basis"].is_null());
    assert!(
        value["budget"]["envelope_validation"]
            .as_str()
            .unwrap()
            .contains("unavailable")
    );
    for observation in value["detected"]["observations"].as_array().unwrap() {
        assert_eq!(observation["value"]["status"], "unavailable");
        assert!(observation.get("bytes").is_none());
    }
}

#[tokio::test]
async fn memory_lease_timeout_refunds_waiter_and_empty_bytes_hold_an_entry() {
    let pool = pool(1, 1, 1);
    let bytes = pool.try_acquire(0).unwrap().into_bytes(Vec::new()).unwrap();
    assert_eq!(pool.usage().entries, 1);
    let clone = bytes.clone();
    drop(bytes);
    let result = tokio::time::timeout(std::time::Duration::from_millis(1), pool.acquire(0)).await;
    assert!(result.is_err());
    assert_eq!(pool.usage().waiters, 0);
    drop(clone);
    assert_eq!(pool.usage(), PoolUsage::default());
}

#[test]
fn memory_budget_keeps_headroom_unallocated_and_progress_independent() {
    let planned = plan(4 * GIB, &example(), VolatileBudget::default()).unwrap();
    let budget = MemoryBudget::new(&planned);
    assert!(budget.pool(PoolKind::Headroom).is_none());
    let mut leases = Vec::new();
    for kind in [
        PoolKind::Metadata,
        PoolKind::Dirty,
        PoolKind::Protocol,
        PoolKind::Io,
        PoolKind::Clean,
    ] {
        leases.push(
            budget
                .pool(kind)
                .unwrap()
                .try_acquire(planned.pool(kind).capacity_bytes)
                .unwrap(),
        );
    }
    let progress = budget.pool(PoolKind::Progress).unwrap();
    let drain = progress
        .try_acquire(planned.pool(PoolKind::Progress).capacity_bytes)
        .unwrap();
    drop(leases);
    assert!(progress.usage().bytes > 0);
    drop(drain);
    assert_eq!(progress.usage(), PoolUsage::default());
}
