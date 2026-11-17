use super::{CgroupMemoryLimits, MemoryBudgetReceipt, validate_server_startup_with_limits};
use crate::config::Settings;
use anyhow::{Context, Result};
use std::path::Path;

const GIB: u64 = 1024 * 1024 * 1024;
const CLEAN: u64 = 96 * 1024 * 1024;

fn package_config(root: &Path, limit: u64) -> String {
    include_str!("memory_package_fixture.toml")
        .replace("/storage/", &format!("{}/", root.display()))
        .replace(
            "memory_limit_gb = 8.0",
            &format!("memory_limit_gb = {:.9}", limit as f64 / 1e9),
        )
}

fn admission(root: &Path, text: &str) -> Result<MemoryBudgetReceipt> {
    let path = root.join("config.toml");
    std::fs::write(&path, text)?;
    let settings = Settings::from_file(path)?;
    validate_server_startup_with_limits(&settings, CgroupMemoryLimits::default())?
        .context("test requires a dedicated memory envelope")
}

#[test]
#[cfg(feature = "webui")]
fn package_profile_uses_its_measured_policy_and_fits_eight_gib() {
    let temp = tempfile::tempdir().unwrap();
    let receipt = admission(temp.path(), &package_config(temp.path(), 8 * GIB)).unwrap();
    assert_eq!(receipt.required_bytes, 6_543_114_240);
    assert!(receipt.required_bytes < 8 * GIB);
    assert!(format!("{receipt:?}").contains("small-nas-package-v1"));
    assert!(!temp.path().join("metadata").exists());
    assert!(!temp.path().join("catalog").exists());
}

#[test]
#[cfg(feature = "webui")]
fn unmeasured_configurations_keep_legacy_reserves_and_report_first_bound() {
    let temp = tempfile::tempdir().unwrap();
    let base = package_config(temp.path(), 64 * GIB);
    for (before, after, reason) in [
        (
            "memory_size_gb = 0.001",
            "memory_size_gb = 1.0",
            "clean_cache_bytes",
        ),
        (
            "disk_size_gb = 0.064",
            "disk_size_gb = 4.0",
            "cache.disk_size_gb",
        ),
        (
            "warm_metadata = \"off\"",
            "warm_metadata = \"full\"",
            "cache.warm_metadata",
        ),
        (
            "[servers.webui]",
            "[servers.nfs]\naddresses = [\"127.0.0.1:2049\"]\n[servers.webui]",
            "servers.nfs",
        ),
        (
            "[servers.rpc]",
            "[servers.nbd]\naddresses = [\"127.0.0.1:10809\"]\n[servers.rpc]",
            "servers.nbd",
        ),
        (
            "[servers.ninep]\naddresses = []",
            "[servers.ninep]\naddresses = [\"127.0.0.1:5564\"]",
            "servers.ninep",
        ),
        (
            "[servers.rpc]\naddresses = []",
            "[servers.rpc]\naddresses = [\"127.0.0.1:7000\"]",
            "servers.rpc",
        ),
        (
            "0.0.0.0:8080",
            "0.0.0.0:8080\", \"127.0.0.1:8081",
            "servers.webui",
        ),
        (
            "name_mode = \"insensitive\"",
            "name_mode = \"sensitive\"",
            "filesystem.name_mode",
        ),
        (
            "compression = \"lz4\"",
            "compression = \"zstd-3\"",
            "filesystem.compression",
        ),
        (
            "ignore_fsync = false",
            "ignore_fsync = true",
            "filesystem.ignore_fsync",
        ),
        ("sync_writes = false", "sync_writes = true", "lsm"),
        (
            "sync_writes = false",
            "sync_writes = false\nmax_concurrent_compactions = 3",
            "lsm",
        ),
        (
            "sync_writes = false",
            "sync_writes = false\nflush_interval_secs = 60",
            "lsm",
        ),
        (
            "[telemetry]",
            "[gc]\ncompact_round_max_mib = 1024\n[telemetry]",
            "gc",
        ),
        ("enabled = false", "enabled = true", "telemetry"),
    ] {
        let changed = base.replace(before, after);
        assert_ne!(base, changed, "fixture did not contain {before}");
        let receipt = admission(temp.path(), &changed).unwrap();
        let debug = format!("{receipt:?}");
        assert!(debug.contains("legacy"), "{reason}: {debug}");
        assert!(
            debug.contains(&format!("reason={reason}")),
            "{reason}: {debug}"
        );
        let clean = if reason == "clean_cache_bytes" {
            1_000_000_000
        } else {
            CLEAN
        };
        assert_eq!(receipt.required_bytes, 40 * GIB + clean, "{reason}");
    }
    let no_webui = base.split("[servers.webui]").next().unwrap();
    let receipt = admission(temp.path(), no_webui).unwrap();
    assert!(format!("{receipt:?}").contains("servers.webui"));
    assert_eq!(receipt.required_bytes, 40 * GIB + CLEAN);
    let replicated = base.replace(
        "[telemetry]",
        "[replication]\nnode_id = \"test\"\nrole = \"leader\"\n[telemetry]",
    );
    let error = format!("{:#}", admission(temp.path(), &replicated).unwrap_err());
    assert!(error.contains("does not support [replication]"), "{error}");
}

#[test]
fn writeback_profiles_keep_legacy_reserves_including_worker_variations() {
    let temp = tempfile::tempdir().unwrap();
    for workers in [1, 4] {
        let text = format!(
            "{}\n[writeback]\nenabled = true\ndir = {:?}\nmemory_size_gb = 0.75\ndisk_size_gb = 4.0\nmin_free_gb = 0.1\nupload_concurrency = {workers}\nlocal_concurrency = {workers}\n",
            package_config(temp.path(), 64 * GIB),
            temp.path().join("journal").display().to_string(),
        );
        let receipt = admission(temp.path(), &text).unwrap();
        assert_eq!(receipt.required_bytes, 40 * GIB + CLEAN + 750_000_000);
        assert!(format!("{receipt:?}").contains("writeback"));
    }
}

#[test]
fn shared_volatile_writes_charge_both_stages_without_recharging_matching_aliases() {
    let temp = tempfile::tempdir().unwrap();
    let base = package_config(temp.path(), 64 * GIB).replace(
        "write_ack_mode = \"materialized\"",
        "write_ack_mode = \"volatile_memory\"\nvolatile_memory_gb = 0.2",
    );
    let base = format!(
        "{base}\n[writeback]\nenabled = true\ndir = {:?}\nmemory_size_gb = 0.75\ndisk_size_gb = 4.0\nmin_free_gb = 0.1\n",
        temp.path().join("journal").display().to_string(),
    );
    let shared = format!("{base}\n[servers.nbd]\naddresses = [\"127.0.0.1:10809\"]\n");
    let matching =
        format!("{shared}write_ack_mode = \"volatile_memory\"\nvolatile_memory_gb = 0.2\n");
    let receipt = admission(temp.path(), &shared).unwrap();
    assert_eq!(
        receipt.required_bytes,
        CLEAN + 40 * GIB + 750_000_000 + 400_000_000
    );
    assert_eq!(
        receipt.required_bytes,
        admission(temp.path(), &matching).unwrap().required_bytes
    );
}

#[test]
fn reserve_overrides_enforce_upward_only_floors_and_finite_byte_values() {
    let temp = tempfile::tempdir().unwrap();
    for field in ["unmodeled_residency_reserve_gb", "engine_headroom_gb"] {
        for value in [
            "0.0",
            "-1.0",
            "nan",
            "inf",
            "1e100",
            "18446744073.709553",
            "1e-100",
            "0.000001",
        ] {
            let text = package_config(temp.path(), 64 * GIB)
                .replace("[runtime]", &format!("[runtime]\n{field} = {value}"));
            let error = format!("{:#}", admission(temp.path(), &text).unwrap_err());
            assert!(error.contains(field), "{error}");
            assert!(
                error.contains("computed minimum")
                    || error.contains("finite positive")
                    || error.contains("too large"),
                "{error}"
            );
        }
    }
}

#[test]
fn reserve_overrides_raise_required_bytes_and_headroom_lowers_the_brake() {
    let temp = tempfile::tempdir().unwrap();
    let base = package_config(temp.path(), 128 * GIB);
    let before = admission(temp.path(), &base).unwrap();
    for field in ["unmodeled_residency_reserve_gb", "engine_headroom_gb"] {
        let text = base.replace("[runtime]", &format!("[runtime]\n{field} = 40.0"));
        let after = admission(temp.path(), &text).unwrap();
        assert!(after.required_bytes > before.required_bytes);
        if field == "engine_headroom_gb" {
            assert!(after.rss_pressure_cap_bytes < before.rss_pressure_cap_bytes);
        }
    }
}

#[test]
#[cfg(feature = "webui")]
fn receipt_and_rejection_report_every_term_and_source() {
    let temp = tempfile::tempdir().unwrap();
    let receipt = admission(temp.path(), &package_config(temp.path(), 64 * GIB)).unwrap();
    let rejection = format!(
        "{:#}",
        admission(temp.path(), &package_config(temp.path(), GIB)).unwrap_err()
    );
    for output in [format!("{receipt:?}"), rejection] {
        for term in [
            "small-nas-package-v1",
            "clean_cache_bytes",
            "object_writeback_bytes",
            "volatile_write_bytes",
            "direct_segment_bytes",
            "unmodeled_residency_reserve_bytes",
            "engine_headroom_bytes",
            "computed",
            "ci+webui",
            "samples.jsonl",
            "memory_limit_gb",
            "external_companions",
        ] {
            assert!(output.contains(term), "missing {term}: {output}");
        }
    }
    assert!(!temp.path().join("metadata").exists());
}

#[test]
fn admission_cannot_be_disabled_with_an_unknown_runtime_setting() {
    let temp = tempfile::tempdir().unwrap();
    let text = package_config(temp.path(), GIB)
        .replace("[runtime]", "[runtime]\ndisable_memory_admission = true");
    let error = format!("{:#}", admission(temp.path(), &text).unwrap_err());
    assert!(error.contains("unknown field"), "{error}");
}

#[test]
fn admission_receipt_rejects_planning_only_memory_configuration() {
    let temp = tempfile::tempdir().unwrap();
    let text = include_str!("memory_package_fixture.toml")
        .replace("/storage/", &format!("{}/", temp.path().display()))
        .replace(
            "[runtime]\nmemory_limit_gb = 8.0",
            "[memory]\ntotal = \"4 GiB\"",
        );
    let path = temp.path().join("config.toml");
    std::fs::write(&path, text).unwrap();
    let settings = Settings::from_file(path).unwrap();
    let detected = CgroupMemoryLimits {
        dedicated: Some(super::MemoryEnvelope {
            hard_limit_bytes: 64 * GIB,
            source: "fixture/memory.max".into(),
        }),
        ..Default::default()
    };
    // memory-status uses this same admission helper. A wide cgroup must not
    // produce a successful receipt while ignoring a planning-only budget that
    // the server refuses to start with.
    let error = validate_server_startup_with_limits(&settings, detected).unwrap_err();
    assert!(
        error
            .to_string()
            .contains("[memory] is available for planning only"),
        "{error:#}"
    );
}

#[test]
fn exact_minimum_does_not_shrink_with_the_envelope() {
    let temp = tempfile::tempdir().unwrap();
    let wide = admission(temp.path(), &package_config(temp.path(), 64 * GIB)).unwrap();
    let exact = admission(
        temp.path(),
        &package_config(temp.path(), wide.required_bytes),
    )
    .unwrap();
    assert_eq!(exact.required_bytes, wide.required_bytes);
    assert_eq!(exact.remaining_bytes, 0);
    assert!(
        admission(
            temp.path(),
            &package_config(temp.path(), wide.required_bytes - 1)
        )
        .is_err()
    );
}

#[test]
#[cfg(feature = "webui")]
fn brake_at_the_admission_minimum_includes_normal_engine_residency() {
    let temp = tempfile::tempdir().unwrap();
    let wide = admission(temp.path(), &package_config(temp.path(), 64 * GIB)).unwrap();
    let minimum = admission(
        temp.path(),
        &package_config(temp.path(), wide.required_bytes),
    )
    .unwrap();
    assert!(minimum.rss_pressure_cap_bytes > 2 * GIB + CLEAN + 3_241_541_632);
    assert_eq!(
        minimum.rss_pressure_cap_bytes,
        minimum.hard_limit_bytes - 671_088_640
    );
}

#[test]
#[cfg(feature = "webui")]
fn pool_and_direct_write_mode_are_part_of_the_measured_envelope() {
    let temp = tempfile::tempdir().unwrap();
    let base = package_config(temp.path(), 64 * GIB);
    for (text, bound, additional) in [
        (format!("{base}\n{}", extra_members(3)), "storage.pool", 0),
        (
            base.replace("capacity_gb = 1.0", "capacity_gb = 18001.0"),
            "storage.pool",
            0,
        ),
    ] {
        let receipt = admission(temp.path(), &text).unwrap();
        assert_eq!(receipt.required_bytes, CLEAN + 40 * GIB + additional);
        assert!(format!("{receipt:?}").contains(bound));
    }
    let volatile = base.replace(
        "write_ack_mode = \"materialized\"",
        "write_ack_mode = \"volatile_memory\"\nvolatile_memory_gb = 0.2",
    );
    let error = format!("{:#}", admission(temp.path(), &volatile).unwrap_err());
    assert!(
        error.contains("requires the enabled [writeback] tier"),
        "{error}"
    );
}

#[test]
#[cfg(not(feature = "webui"))]
fn package_without_compiled_webui_keeps_legacy_reserves() {
    let temp = tempfile::tempdir().unwrap();
    let receipt = admission(temp.path(), &package_config(temp.path(), 64 * GIB)).unwrap();
    assert_eq!(receipt.required_bytes, CLEAN + 40 * GIB);
    assert!(format!("{receipt:?}").contains("servers.webui"));
}

#[test]
#[cfg(feature = "webui")]
fn equivalent_cache_floor_and_explicit_gc_defaults_match_the_package() {
    let temp = tempfile::tempdir().unwrap();
    let base = package_config(temp.path(), 64 * GIB);
    let original = admission(temp.path(), &base).unwrap();
    let equivalent = base.replace("memory_size_gb = 0.001", "memory_size_gb = 0.1");
    let equivalent =
        format!("{equivalent}\n[gc]\ninterval_secs = 60\ncompact_round_max_mib = 256\n");
    let receipt = admission(temp.path(), &equivalent).unwrap();
    assert_eq!(receipt.required_bytes, original.required_bytes);
    assert!(format!("{receipt:?}").contains("small-nas-package-v1"));
}

#[test]
#[cfg(feature = "webui")]
fn exact_standard_ninep_lookup_keeps_the_package_policy() {
    let temp = tempfile::tempdir().unwrap();
    let base = package_config(temp.path(), 8 * GIB);
    let original = admission(temp.path(), &base).unwrap();
    let socket = "unix_socket = \"/run/zerofs-app/9p.sock\"\n";
    assert!(base.contains(socket));
    let exact = base.replace(socket, &format!("{socket}standard_lookup = \"exact\"\n"));
    let receipt = admission(temp.path(), &exact).unwrap();
    assert_eq!(receipt.required_bytes, original.required_bytes);
    assert!(format!("{receipt:?}").contains("small-nas-package-v1"));
}

#[cfg(feature = "webui")]
fn extra_members(count: usize) -> String {
    (0..count).map(|index| format!(
        "\n[[storage.pool.members]]\nid = \"extra-{index}\"\nurl = \"file:///tmp/extra-{index}\"\ncapacity_gb = 18000.0\nreserve_free_gb = 0.01\n"
    )).collect()
}

#[test]
#[cfg(feature = "webui")]
fn real_nas_declarations_and_memory_neutral_identity_settings_are_admitted() {
    let temp = tempfile::tempdir().unwrap();
    let base =
        package_config(temp.path(), 7 * GIB).replace("capacity_gb = 1.0", "capacity_gb = 18000.0");
    for additional in 0..=2 {
        let text = format!("{base}{}", extra_members(additional));
        let receipt = admission(temp.path(), &text).unwrap();
        assert!(receipt.accounting.contains("small-nas-package"));
        assert!(receipt.required_bytes <= 7 * GIB);
    }
    let mixed = base.replacen("capacity_gb = 18000.0", "capacity_gb = 6000.0", 1);
    let mixed = format!("{mixed}{}", extra_members(1).replace("18000.0", "1000.0"));
    assert!(admission(temp.path(), &mixed).is_ok());
    for (uid, gid, address) in [
        (0u32, 0u32, "127.0.0.1:18080"),
        (65534, 65533, "0.0.0.0:8081"),
        (u32::MAX, u32::MAX, "[::1]:8080"),
        (1001, 999, "[::]:8080"),
    ] {
        let text = base
            .replace("uid = 1000", &format!("uid = {uid}"))
            .replace("gid = 1000", &format!("gid = {gid}"))
            .replace("0.0.0.0:8080", address);
        assert!(admission(temp.path(), &text).is_ok(), "{text}");
    }
    let diagnostic = format!("{base}\n[prometheus]\naddresses = [\"0.0.0.0:9091\"]\n");
    assert!(admission(temp.path(), &diagnostic).is_ok());
}

#[test]
#[cfg(feature = "webui")]
fn startup_receipt_states_empirical_stored_state_coverage() {
    let temp = tempfile::tempdir().unwrap();
    let receipt = admission(temp.path(), &package_config(temp.path(), 8 * GIB)).unwrap();
    for term in [
        "qualification_stored_logical_bytes=",
        "qualification_regular_files=",
        "empirical_not_enforced",
    ] {
        assert!(receipt.accounting.contains(term), "{}", receipt.accounting);
    }
}

#[test]
#[cfg(feature = "webui")]
fn json_receipt_distinguishes_empirical_coverage_from_legacy_policy() {
    let temp = tempfile::tempdir().unwrap();
    let base = package_config(temp.path(), 64 * GIB);
    let receipt = admission(temp.path(), &base).unwrap();
    let json = serde_json::to_value(receipt).unwrap();
    let state = &json["stored_state_qualification"];
    assert_eq!(state["logical_bytes"], 50 * GIB + 50_000 * 1024);
    assert_eq!(state["regular_files"], 50_039);
    assert_eq!(state["enforcement"], "empirical_not_enforced");
    let legacy = base.replace("memory_size_gb = 0.001", "memory_size_gb = 1.0");
    let receipt = admission(temp.path(), &legacy).unwrap();
    let json = serde_json::to_value(receipt).unwrap();
    assert!(json["stored_state_qualification"].is_null());
}
