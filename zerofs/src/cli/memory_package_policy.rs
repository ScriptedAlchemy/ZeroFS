use crate::config::{CompressionConfig, GcConfig, LsmConfig, Settings, WarmMetadata};
use crate::fs::name_mode::NameMode;

pub(super) fn exclusion(
    settings: &Settings,
    clean_cache_bytes: u64,
    volatile_write_bytes: u64,
) -> Option<&'static str> {
    let local_url = |value: &str| {
        url::Url::parse(value).is_ok_and(|url| url.scheme() == "file" && url.to_file_path().is_ok())
    };
    let unix_ninep = settings.servers.ninep.as_ref().is_some_and(|config| {
        config
            .addresses
            .as_ref()
            .is_none_or(|addresses| addresses.is_empty())
            && config.unix_socket.is_some()
            && config.shared_identity.is_none()
    });
    let unix_rpc = settings.servers.rpc.as_ref().is_some_and(|config| {
        config
            .addresses
            .as_ref()
            .is_none_or(|addresses| addresses.is_empty())
            && config.unix_socket.is_some()
    });
    let webui = cfg!(feature = "webui")
        && settings.servers.webui.as_ref().is_some_and(|config| {
            config.addresses.len() == 1 && config.p9_idle_timeout_secs == 120
        });
    let local_pool = settings.storage.pool.as_ref().is_some_and(|pool| {
        (2..=4).contains(&pool.members.len())
            && pool.members.iter().all(|member| {
                local_url(&member.url)
                    && member.capacity_gb <= 18000.0
                    && member.sftp.is_none()
                    && member.health_device.is_none()
            })
    });
    let lsm = settings.lsm.is_none_or(|config| {
        config.l0_max_ssts() == LsmConfig::DEFAULT_L0_MAX_SSTS
            && config.max_concurrent_compactions() == LsmConfig::DEFAULT_MAX_CONCURRENT_COMPACTIONS
            && config.flush_interval_secs() == LsmConfig::DEFAULT_FLUSH_INTERVAL_SECS
            && !config.sync_writes()
    });
    let gc = settings.gc.unwrap_or_default();
    let defaults = GcConfig::default();
    let package_gc = gc.interval_secs() == defaults.interval_secs()
        && gc.idle_interval_secs() == defaults.idle_interval_secs()
        && gc.read_directed() == defaults.read_directed()
        && gc.tail_scrub_min_dead_percent() == defaults.tail_scrub_min_dead_percent()
        && gc.min_batches_per_pass() == defaults.min_batches_per_pass()
        && gc.busy_backlog_interval_secs() == defaults.busy_backlog_interval_secs()
        && gc.busy_backlog_dead_percent() == defaults.busy_backlog_dead_percent()
        && gc.compact_round_bytes() == defaults.compact_round_bytes();
    [
        (clean_cache_bytes <= 96 * 1024 * 1024, "clean_cache_bytes exceeds 100663296"),
        (settings.cache.disk_size_gb.is_finite() && settings.cache.disk_size_gb > 0.0 && settings.cache.disk_size_gb <= 0.064, "cache.disk_size_gb exceeds measured 0.064 GB"),
        (settings.cache.warm_metadata == WarmMetadata::Off, "cache.warm_metadata must be off"),
        (settings.writeback.as_ref().is_none_or(|config| !config.enabled), "writeback must be disabled; its workers and journal are unqualified"),
        (volatile_write_bytes == 0, "filesystem.write_ack_mode must be materialized"),
        (settings.replication.is_none(), "replication must be off"),
        (settings.servers.nbd.is_none(), "servers.nbd is unqualified"),
        (settings.servers.nfs.is_none(), "servers.nfs is unqualified"),
        (unix_ninep, "servers.ninep must use only a Unix socket without shared_identity"),
        (unix_rpc, "servers.rpc must use only a Unix socket"),
        (webui, "servers.webui requires the webui build and one listener with the measured 120s session timeout"),
        (local_url(&settings.storage.url), "storage.url must be a local directory"),
        (local_pool, "storage.pool requires two to four local directory members of at most 18000 GB without health devices"),
        (settings.name_mode() == NameMode::Insensitive, "filesystem.name_mode must be insensitive"),
        (settings.compression() == CompressionConfig::Lz4, "filesystem.compression must be lz4"),
        (settings.filesystem.as_ref().is_none_or(|config| !config.ignore_fsync), "filesystem.ignore_fsync must be false"),
        (lsm, "lsm must use package flush and compaction defaults with sync_writes off"),
        (package_gc, "gc must use package defaults"),
        (settings.telemetry.as_ref().is_none_or(|config| !config.enabled), "telemetry must be disabled"),
        (settings.prometheus.as_ref().is_none_or(|config| config.addresses.len() <= 1), "prometheus permits at most one diagnostic listener"),
    ].into_iter().find_map(|(within, reason)| (!within).then_some(reason))
}

pub(super) const ENGINE_PEAK_BYTES: u64 = 3_241_541_632;
const KERNEL_PEAK_BYTES: u64 = 137_129_984;
const ROUNDING_BYTES: u64 = 64 * 1024 * 1024;
const fn round_up(value: u64) -> u64 {
    value.div_ceil(ROUNDING_BYTES) * ROUNDING_BYTES
}
pub(super) const RESIDENCY_BYTES: u64 = round_up((ENGINE_PEAK_BYTES * 110).div_ceil(100));
pub(super) const HEADROOM_BYTES: u64 =
    round_up((ENGINE_PEAK_BYTES * 15).div_ceil(100) + (KERNEL_PEAK_BYTES * 125).div_ceil(100));
pub(super) const EVIDENCE: &str = "computed from ci+webui package-baseline-ci-limit64g-cache0.001-wb0-jobs8-primary/samples.jsonl; R=ceil64MiB(1.10*3241541632); H=ceil64MiB(0.15*3241541632+1.25*137129984)";

pub(super) const QUALIFIED_LOGICAL_BYTES: u64 = 50 * 1024 * 1024 * 1024 + 50_000 * 1024;
pub(super) const QUALIFIED_REGULAR_FILES: u64 = 50_039;
