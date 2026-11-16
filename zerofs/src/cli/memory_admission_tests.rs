use super::{MemoryBudgetReceipt, validate_server_startup};
use crate::config::Settings;
use anyhow::{Context, Result};
use std::path::Path;

const GIB: u64 = 1024 * 1024 * 1024;

fn small_config(root: &Path, limit: u64) -> String {
    format!(
        r#"[runtime]
memory_limit_gb = {limit_gb:.9}
[cache]
dir = {cache:?}
disk_size_gb = 4.0
memory_size_gb = 0.1
[storage]
url = {storage:?}
encryption_password = "synthetic-memory-admission-test"
[storage.pool]
catalog_dir = {catalog:?}
[[storage.pool.members]]
id = "first"
url = {first:?}
capacity_gb = 8.0
reserve_free_gb = 0.1
[[storage.pool.members]]
id = "second"
url = {second:?}
capacity_gb = 32.0
reserve_free_gb = 0.1
[servers.ninep]
addresses = ["127.0.0.1:5564"]
[servers.rpc]
addresses = ["127.0.0.1:7000"]
[writeback]
enabled = true
dir = {writeback:?}
memory_size_gb = 0.3
disk_size_gb = 4.0
min_free_gb = 0.1
ack_mode = "memory"
upload_concurrency = 1
local_concurrency = 1
[telemetry]
enabled = false
"#,
        limit_gb = limit as f64 / 1_000_000_000.0,
        cache = root.join("cache").display().to_string(),
        storage = format!("file://{}", root.join("metadata").display()),
        catalog = root.join("catalog").display().to_string(),
        first = format!("file://{}", root.join("first").display()),
        second = format!("file://{}", root.join("second").display()),
        writeback = root.join("writeback").display().to_string(),
    )
}

fn admission(root: &Path, text: &str) -> Result<MemoryBudgetReceipt> {
    let path = root.join("config.toml");
    std::fs::write(&path, text)?;
    let settings = Settings::from_file(path)?;
    validate_server_startup(&settings)?.context("test requires a dedicated memory envelope")
}

#[test]
fn small_local_configuration_fits_four_and_eight_gib() {
    for limit in [4 * GIB, 8 * GIB] {
        let temp = tempfile::tempdir().unwrap();
        let receipt = admission(temp.path(), &small_config(temp.path(), limit)).unwrap();
        assert_eq!(receipt.hard_limit_bytes, limit);
        assert!(receipt.required_bytes <= limit);
        assert!(receipt.rss_pressure_cap_bytes > 0);
        assert!(!temp.path().join("metadata").exists());
        assert!(!temp.path().join("catalog").exists());
    }
}

#[test]
fn larger_clean_cache_also_increases_the_residency_allowance() {
    let temp = tempfile::tempdir().unwrap();
    let small = small_config(temp.path(), 64 * GIB);
    let large = small.replace("memory_size_gb = 0.1", "memory_size_gb = 4.0");
    let before = admission(temp.path(), &small).unwrap();
    let after = admission(temp.path(), &large).unwrap();
    let resolved_cache_increase = 4_000_000_000 - 96 * 1024 * 1024;
    assert!(after.required_bytes - before.required_bytes > resolved_cache_increase);
}

#[test]
fn additional_upload_workers_require_additional_process_allowance() {
    let temp = tempfile::tempdir().unwrap();
    let small = small_config(temp.path(), 64 * GIB);
    let parallel = small.replace("upload_concurrency = 1", "upload_concurrency = 8");
    let before = admission(temp.path(), &small).unwrap();
    let after = admission(temp.path(), &parallel).unwrap();
    assert!(after.required_bytes > before.required_bytes);
}

#[test]
fn additional_local_workers_require_additional_process_allowance() {
    let temp = tempfile::tempdir().unwrap();
    let small = small_config(temp.path(), 64 * GIB);
    let parallel = small.replace("local_concurrency = 1", "local_concurrency = 8");
    let before = admission(temp.path(), &small).unwrap();
    let after = admission(temp.path(), &parallel).unwrap();
    assert!(after.required_bytes > before.required_bytes);
}

#[test]
fn shared_volatile_writes_charge_both_live_ownership_stages() {
    let temp = tempfile::tempdir().unwrap();
    let base = small_config(temp.path(), 64 * GIB);
    let volatile = format!(
        "{base}\n[filesystem]\nwrite_ack_mode = \"volatile_memory\"\nvolatile_memory_gb = 0.2\n"
    );
    let before = admission(temp.path(), &base).unwrap();
    let after = admission(temp.path(), &volatile).unwrap();
    assert_eq!(after.required_bytes - before.required_bytes, 400_000_000);
}

#[test]
fn matching_legacy_volatile_alias_does_not_add_another_budget() {
    let temp = tempfile::tempdir().unwrap();
    let base = small_config(temp.path(), 64 * GIB);
    let nbd = "\n[servers.nbd]\naddresses = [\"127.0.0.1:10809\"]\n";
    let alias = "write_ack_mode = \"volatile_memory\"\nvolatile_memory_gb = 0.2\n";
    let shared = format!("{base}\n[filesystem]\n{alias}{nbd}");
    let matching = format!("{shared}{alias}");
    let shared_only = admission(temp.path(), &shared).unwrap();
    let both = admission(temp.path(), &matching).unwrap();
    assert_eq!(both.required_bytes, shared_only.required_bytes);
}

#[test]
fn disabled_writeback_releases_more_than_its_payload_allowance() {
    let temp = tempfile::tempdir().unwrap();
    let enabled = small_config(temp.path(), 64 * GIB);
    let disabled = enabled.replace("enabled = true", "enabled = false");
    let before = admission(temp.path(), &enabled).unwrap();
    let after = admission(temp.path(), &disabled).unwrap();
    assert!(before.required_bytes - after.required_bytes > 300_000_000);
}

#[test]
fn each_nfs_listener_increases_the_process_allowance() {
    let temp = tempfile::tempdir().unwrap();
    let base = small_config(temp.path(), 64 * GIB);
    let one = format!("{base}\n[servers.nfs]\naddresses = [\"127.0.0.1:20490\"]\n");
    let two = one.replace(
        "127.0.0.1:20490\"]",
        "127.0.0.1:20490\", \"127.0.0.1:20491\"]",
    );
    let without_nfs = admission(temp.path(), &base).unwrap();
    let one_listener = admission(temp.path(), &one).unwrap();
    let two_listeners = admission(temp.path(), &two).unwrap();
    assert!(one_listener.required_bytes > without_nfs.required_bytes);
    assert!(two_listeners.required_bytes > one_listener.required_bytes);
}

#[test]
fn explicit_reserves_can_raise_admission_and_lower_the_pressure_cap() {
    let temp = tempfile::tempdir().unwrap();
    let base = small_config(temp.path(), 64 * GIB);
    let raised = base.replace(
        "[runtime]",
        "[runtime]\nunmodeled_residency_reserve_gb = 16.0\ncompanion_process_reserve_gb = 16.0",
    );
    let before = admission(temp.path(), &base).unwrap();
    let after = admission(temp.path(), &raised).unwrap();
    assert!(after.required_bytes > before.required_bytes);
    assert!(after.required_bytes >= 32_000_000_000);
    assert!(after.rss_pressure_cap_bytes < before.rss_pressure_cap_bytes);
}

#[test]
fn explicit_reserves_cannot_lower_the_computed_minimum() {
    let temp = tempfile::tempdir().unwrap();
    for field in [
        "unmodeled_residency_reserve_gb",
        "companion_process_reserve_gb",
    ] {
        let text = small_config(temp.path(), 64 * GIB)
            .replace("[runtime]", &format!("[runtime]\n{field} = 0.000001"));
        let error = format!("{:#}", admission(temp.path(), &text).unwrap_err());
        assert!(error.contains(field), "{error}");
        assert!(error.contains("computed minimum"), "{error}");
        assert!(!temp.path().join("metadata").exists());
    }
}

#[test]
fn rejection_reports_the_tiers_reserves_and_their_sources() {
    let temp = tempfile::tempdir().unwrap();
    let text = small_config(temp.path(), GIB);
    let error = format!("{:#}", admission(temp.path(), &text).unwrap_err());
    for term in [
        "clean_cache_bytes=",
        "object_writeback_bytes=",
        "volatile_write_bytes=",
        "unmodeled_residency_reserve_bytes=",
        "companion_process_reserve_bytes=",
        "computed",
        "memory_limit_gb",
    ] {
        assert!(error.contains(term), "missing {term}: {error}");
    }
    assert!(!temp.path().join("metadata").exists());
}

#[test]
fn worker_allowance_overflow_rejects_before_storage_creation() {
    let temp = tempfile::tempdir().unwrap();
    let text = small_config(temp.path(), 64 * GIB).replace(
        "upload_concurrency = 1",
        "upload_concurrency = 9223372036854775807",
    );
    let error = format!("{:#}", admission(temp.path(), &text).unwrap_err());
    assert!(error.contains("overflow"), "{error}");
    assert!(!temp.path().join("metadata").exists());
}

#[test]
fn reserve_overrides_reject_zero_negative_nonfinite_and_overflowing_values() {
    let temp = tempfile::tempdir().unwrap();
    for field in [
        "unmodeled_residency_reserve_gb",
        "companion_process_reserve_gb",
    ] {
        for value in ["0.0", "-1.0", "nan", "inf", "1e100", "18446744073.709553"] {
            let text = small_config(temp.path(), 64 * GIB)
                .replace("[runtime]", &format!("[runtime]\n{field} = {value}"));
            let error = format!("{:#}", admission(temp.path(), &text).unwrap_err());
            assert!(error.contains(field), "{error}");
            assert!(
                error.contains("finite positive") || error.contains("too large"),
                "{error}"
            );
            assert!(!temp.path().join("metadata").exists());
        }
    }
}

#[test]
fn the_admission_minimum_does_not_shrink_with_the_container_envelope() {
    let temp = tempfile::tempdir().unwrap();
    let wide = admission(temp.path(), &small_config(temp.path(), 64 * GIB)).unwrap();
    let exact = admission(temp.path(), &small_config(temp.path(), wide.required_bytes)).unwrap();
    assert_eq!(exact.required_bytes, wide.required_bytes);
    assert_eq!(exact.remaining_bytes, 0);
    assert!(
        admission(
            temp.path(),
            &small_config(temp.path(), wide.required_bytes - 1),
        )
        .is_err()
    );
}
