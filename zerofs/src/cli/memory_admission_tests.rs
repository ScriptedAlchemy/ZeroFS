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

#[test]
#[cfg(feature = "webui")]
fn bounded_journal_keeps_an_independent_budget_and_rejects_shape_changes() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().display();
    let base = package_config(temp.path(), 6 * GIB + GIB / 2)
        .replace("[runtime]", "[runtime]\nprofile = \"small-nas-journal-v1\"");
    let config = format!(
        "{base}\n[writeback]\nenabled = true\ndir = \"{root}/journal\"\nack_mode = \"memory\"\nmemory_size_gb = 0.134217728\ndisk_size_gb = 0.536870912\nmin_free_gb = 0.067108864\nlocal_concurrency = 1\nupload_concurrency = 2\n"
    );
    let receipt = admission(temp.path(), &config).unwrap();
    assert!(
        receipt
            .accounting
            .contains("memory_policy=small-nas-journal-v1")
    );
    assert!(
        receipt
            .accounting
            .contains("direct_segment_bytes=201326592")
    );
    assert_eq!(receipt.required_bytes, 5_804_916_736);
    assert!(receipt.stored_state_qualification.is_none());
    let settings = Settings::from_file(temp.path().join("config.toml")).unwrap();
    let geometry = settings.store_profile().unwrap();
    assert_eq!(geometry.seal_threshold, Some(32 * 1024 * 1024));
    assert_eq!(geometry.max_inflight_seals, Some(2));
    let ram_ack = config.replace(
        "write_ack_mode = \"materialized\"",
        "write_ack_mode = \"volatile_memory\"\nvolatile_memory_gb = 0.268435456\nvolatile_max_operations = 2048",
    );
    let ram_receipt = admission(temp.path(), &ram_ack).unwrap();
    assert_eq!(
        ram_receipt.required_bytes,
        receipt.required_bytes + (512 << 20)
    );
    assert!(ram_receipt.stored_state_qualification.is_none());
    let minimum = ram_ack.replace("0.268435456", "0.016777216").replace(
        "volatile_max_operations = 2048",
        "volatile_max_operations = 1",
    );
    assert_eq!(
        admission(temp.path(), &minimum).unwrap().required_bytes,
        receipt.required_bytes + (32 << 20)
    );
    for (before, after) in [
        ("0.268435456", "0.536870912"),
        ("0.268435456", "0.016"),
        ("0.268435456", "0.017"),
        (
            "volatile_max_operations = 2048",
            "volatile_max_operations = 2049",
        ),
        ("volatile_max_operations = 2048", ""),
        ("6.979321856", "6.0"),
        ("profile = \"small-nas-journal-v1\"", ""),
    ] {
        assert!(
            admission(temp.path(), &ram_ack.replace(before, after)).is_err(),
            "{after}"
        );
    }
    for (before, after) in [
        ("0.134217728", "0.25"),
        ("0.536870912", "1.0"),
        ("0.067108864", "0.001"),
        ("local_concurrency = 1", "local_concurrency = 2"),
        ("upload_concurrency = 2", "upload_concurrency = 3"),
        ("ack_mode = \"memory\"", "ack_mode = \"ssd\""),
        ("enabled = true", "enabled = false"),
        ("/journal\"", "/cache/journal\""),
        ("0.064", "0.065"),
        ("6.979321856", "5.0"),
    ] {
        assert_ne!(config.replace(before, after), config);
        assert!(
            admission(temp.path(), &config.replace(before, after)).is_err(),
            "{after}"
        );
    }
    assert!(admission(temp.path(), &base).is_err());
}

#[cfg(all(target_os = "linux", feature = "webui"))]
mod raw_candidate {
    use super::*;
    use crate::config::StoreProfile;
    use std::fs::{self, File};

    const MIB: u64 = 1 << 20;

    fn fixture(root: &Path) -> String {
        for (name, size) in [("p3", 256), ("small", 32), ("large", 96)] {
            File::create(root.join(name))
                .unwrap()
                .set_len(size * MIB)
                .unwrap();
        }
        include_str!("memory_raw_fixture.toml")
            .replace("/fixture/", &format!("{}/", root.display()))
    }

    fn validate(
        root: &Path,
        config: &str,
        limit: Option<u64>,
        swap: &str,
    ) -> Result<MemoryBudgetReceipt> {
        let config_path = root.join("raw.toml");
        fs::write(&config_path, config)?;
        fs::write(root.join("memory.swap.max"), swap)?;
        let settings = Settings::from_file(config_path)?;
        validate_server_startup_with_limits(
            &settings,
            CgroupMemoryLimits {
                dedicated: limit.map(|hard_limit_bytes| super::super::MemoryEnvelope {
                    hard_limit_bytes,
                    source: root.join("memory.max"),
                }),
                shared_ceiling: None,
            },
        )?
        .context("raw candidate requires a startup receipt")
    }

    #[test]
    fn raw_candidate_fits_eight_gib_with_raised_destage_reserves() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path();
        let config = fixture(root);
        let raised = config
            .replace(
                "reserve_free_gb = 0.016777216",
                "reserve_free_gb = 0.033554432",
            )
            .replace(
                "reserve_free_gb = 0.008388608",
                "reserve_free_gb = 0.100663296",
            );
        let baseline = validate(root, &config, Some(8 * GIB), "0\n").unwrap();
        let receipt = validate(root, &raised, Some(8 * GIB), "0\n").unwrap();
        assert_eq!(receipt, baseline);
        assert_eq!(receipt.required_bytes, 7_512 * MIB);
        assert_eq!(receipt.included_companion_bytes, GIB);
        assert!(receipt.required_bytes < 8 * GIB);
        assert_eq!(receipt.hard_limit_bytes, 8 * GIB);
        assert!(receipt.stored_state_qualification.is_none());
        for field in [
            "memory_policy=raw-device-p3-candidate-v1",
            "companion_allowance_bytes=1073741824",
            "clean_cache_disk_bytes=0",
            "clean_cache_bytes=100663296",
            "object_writeback_bytes=16777216",
            "volatile_write_bytes=33554432",
        ] {
            assert!(receipt.accounting.contains(field), "{}", receipt.accounting);
        }
        let settings = Settings::from_file(root.join("raw.toml")).unwrap();
        let store = settings.store_profile().unwrap();
        assert_eq!(
            store.seal_threshold,
            Some(StoreProfile::RAW_CANDIDATE_SEAL_BYTES)
        );
        assert_eq!(store.max_inflight_seals, Some(1));
        for unused in ["unused-clean-cache", "unused-directory-journal"] {
            assert!(!root.join(unused).exists());
        }
    }

    #[test]
    fn raw_nas_fit_admits_sparse_disk_geometry_at_eight_gib_only() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path();
        for (name, size) in [("p3", 64 * GIB), ("small", 12 * GIB), ("large", 16 * GIB)] {
            File::create(root.join(name))
                .unwrap()
                .set_len(size)
                .unwrap();
        }
        let config = include_str!("memory_raw_nas_fit_fixture.toml")
            .replace("/fixture/", &format!("{}/", root.display()));
        let receipt = validate(root, &config, Some(8 * GIB), "0").unwrap();
        assert_eq!(receipt.included_companion_bytes, 3 * GIB / 2);
        assert_eq!(receipt.required_bytes, 7_000 * MIB);
        assert!(receipt.stored_state_qualification.is_none());
        assert!(
            receipt
                .accounting
                .contains("memory_policy=raw-device-p3-nas-fit-v1")
        );

        let error = validate(root, &config, Some(9 * GIB), "0").unwrap_err();
        assert!(format!("{error:#}").contains("exactly 8 GiB"), "{error:#}");
        let error = validate(root, &config, Some(8 * GIB), "1").unwrap_err();
        assert!(
            format!("{error:#}").contains("memory.swap.max = 0"),
            "{error:#}"
        );
        let legacy = config.replace("raw-device-p3-nas-fit-v1", "raw-device-p3-candidate-v1");
        let error = validate(root, &legacy, Some(8 * GIB), "0").unwrap_err();
        assert!(format!("{error:#}").contains("profile"), "{error:#}");
    }

    #[test]
    fn physical_contract_requires_distinct_designations_exact_geometry_and_nas_fit_budget() {
        let temp = tempfile::tempdir().unwrap();
        let config = include_str!("memory_raw_physical_test.toml");
        // This is config admission only. No host inventory or device is opened.
        let read = |toml: &str| validate(temp.path(), toml, Some(8 * GIB), "0");
        let receipt = read(config).unwrap();
        assert_eq!(receipt.included_companion_bytes, 1536 * MIB);
        assert_eq!(receipt.required_bytes, 7_000 * MIB);
        for (old, new) in [
            (
                "memory_size_gb = 0.016777216",
                "memory_size_gb = 0.008388608",
            ),
            ("/hdd6?", "/hdd18?"),
            ("/hdd6?", "/dev/sda?"),
            ("/hdd18?", "/ZL213G03?"),
            ("/nvme?", "/hdd18?"),
            ("raw+disk:///hdd6", "raw+loop:///hdd6"),
            ("raw+disk:///hdd6?", "raw+disk:///hdd6?backing=/tmp/unused&"),
            ("layout_bytes=", "layout_mib="),
            ("6001.175126016", "6001.174126016"),
            ("reserve_free_gb = 181.0", "reserve_free_gb = 1.0"),
        ] {
            let changed = config.replace(old, new);
            assert_ne!(config, changed);
            assert!(read(&changed).is_err(), "{changed}");
        }
    }

    #[test]
    fn raw_candidate_rejects_unbounded_or_incompatible_geometry() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path();
        let config = fixture(root);
        for (before, after, reason) in [
            ("96,32,88,8,16,16", "96,32,80,16,16,16", "P3 layout"),
            (
                "round_robin_local",
                "first_fit",
                "round_robin_local placement",
            ),
            (
                "reserve_free_gb = 0.016777216",
                "reserve_free_gb = 0.008388608",
                "reserves in",
            ),
            (
                "reserve_free_gb = 0.008388608",
                "reserve_free_gb = 0.101711872",
                "reserves in",
            ),
            (
                "disk_size_gb = 0.0\n",
                "disk_size_gb = 0.064\n",
                "cache.disk_size_gb",
            ),
            (
                "memory_size_gb = 0.064",
                "memory_size_gb = 1.0",
                "96 MiB resolved clean cache",
            ),
            (
                "volatile_max_operations = 128",
                "volatile_max_operations = 129",
                "128-operation",
            ),
            (
                "disk_size_gb = 0.016777216",
                "disk_size_gb = 0.032",
                "16 MiB disk",
            ),
            (
                "memory_size_gb = 0.016777216",
                "memory_size_gb = 0.008388608",
                "16 MiB journal RAM",
            ),
            (
                "local_concurrency = 1",
                "local_concurrency = 2",
                "one local",
            ),
            (
                "upload_concurrency = 2",
                "upload_concurrency = 3",
                "two upload workers",
            ),
        ] {
            let bad = config.replace(before, after);
            assert_ne!(bad, config);
            let error = validate(root, &bad, Some(8 * GIB), "0").unwrap_err();
            assert!(format!("{error:#}").contains(reason), "{error:#}");
        }
        File::options()
            .write(true)
            .open(root.join("small"))
            .unwrap()
            .set_len(96 * MIB)
            .unwrap();
        let error = validate(root, &config, Some(8 * GIB), "0").unwrap_err();
        assert!(
            format!("{error:#}").contains("exactly 33554432 bytes"),
            "{error:#}"
        );
    }

    #[test]
    fn raw_candidate_requires_a_dedicated_swap_free_cgroup_and_explicit_opt_in() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path();
        let config = fixture(root);
        for (limit, swap, reason) in [
            (None, "0", "dedicated cgroup-v2"),
            (Some(8 * GIB - 1), "0", "8 through 16 GiB"),
            (Some(16 * GIB + 1), "0", "8 through 16 GiB"),
            (Some(8 * GIB), "max", "memory.swap.max = 0"),
        ] {
            let error = validate(root, &config, limit, swap).unwrap_err();
            assert!(format!("{error:#}").contains(reason), "{error:#}");
        }
        let error = validate(
            root,
            &config.replace("profile = \"raw-device-p3-candidate-v1\"\n", ""),
            Some(8 * GIB),
            "0",
        )
        .unwrap_err();
        assert!(
            format!("{error:#}").contains("memory_policy=legacy"),
            "{error:#}"
        );
    }
}
