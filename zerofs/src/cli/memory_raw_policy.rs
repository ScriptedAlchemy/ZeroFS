use crate::config::{
    CompressionConfig, GcConfig, LsmConfig, SegmentPlacement, Settings, WarmMetadata,
};
use crate::fs::mutation::config::{
    ClientDurabilityTarget, FilesystemWriteAckMode, FilesystemWriteAckSettings,
};
use crate::fs::name_mode::NameMode;
use crate::storage_backend::raw::RawBinding;
use crate::writeback::config::{AckMode, ShutdownFlush, WritebackAccessMode};
use anyhow::{Context, Result, ensure};
use std::collections::BTreeSet;
use std::path::Path;
use url::Url;

pub(super) const NAME: &str = "raw-device-p3-candidate-v1";
const MIB: u64 = 1 << 20;
const GIB: u64 = 1 << 30;
pub(super) const MIN_CGROUP_BYTES: u64 = 8 * GIB;
pub(super) const MAX_CGROUP_BYTES: u64 = 16 * GIB;
pub(super) const COMPANION_BYTES: u64 = GIB;
// The Linux synthetic claim caps each device at 256 MiB. Budget two full
// device views for each of three devices, plus 512 MiB for indexes, journal
// database caches and replay staging. This is a provisional allowance.
pub(super) const RAW_RESIDENCY_BYTES: u64 = 2 * (3 * 256 * MIB) + 512 * MIB;

pub(super) fn validate(
    settings: &Settings,
    clean_cache_bytes: u64,
    write_ack: FilesystemWriteAckSettings,
) -> Result<()> {
    ensure!(
        cfg!(all(target_os = "linux", feature = "webui")),
        "requires Linux and the webui package feature"
    );
    let pool = settings
        .storage
        .pool
        .as_ref()
        .context("requires a raw pool")?;
    let authority = Url::parse(&settings.storage.url)?;
    ensure!(
        authority.scheme() == "raw+p3-v2",
        "requires a raw+p3-v2 authority"
    );
    let authority = RawBinding::parse(&authority)?;
    ensure!(
        authority.layout == Some([96, 32, 88, 8, 16, 16].map(|size| size * MIB))
            && pool.catalog_dir.as_os_str().is_empty()
            && settings.storage.readiness.is_none()
            && pool.segment_placement == SegmentPlacement::RoundRobinLocal,
        "requires the 256 MiB P3 layout, empty catalog_dir, no directory readiness, and round_robin_local placement"
    );
    ensure!(
        pool.members.len() == 2,
        "requires exactly two raw+loop members"
    );
    check_backing_size(&authority.backing, 256 * MIB)?;
    let mut member_ids = BTreeSet::from([authority.member]);
    let mut geometry = Vec::with_capacity(2);
    for member in &pool.members {
        let url = Url::parse(&member.url)?;
        ensure!(
            url.scheme() == "raw+loop",
            "requires exactly two raw+loop members"
        );
        let binding = RawBinding::parse(&url)?;
        ensure!(
            binding.pool == authority.pool && member_ids.insert(binding.member),
            "requires distinct device UUIDs within one raw pool"
        );
        let capacity = member.capacity_bytes()?;
        geometry.push((capacity, member.reserve_free_bytes()?));
        check_backing_size(&binding.backing, capacity)?;
    }
    geometry.sort_unstable();
    let [
        (small_capacity, small_reserve),
        (large_capacity, large_reserve),
    ] = geometry.as_slice()
    else {
        unreachable!("validated two pool members");
    };
    ensure!(
        *small_capacity == 32 * MIB
            && *large_capacity == 96 * MIB
            && (16 * MIB..=*small_capacity).contains(small_reserve)
            && (8 * MIB..=*large_capacity).contains(large_reserve),
        "requires 32/96 MiB member capacities with reserves in 16–32/8–96 MiB respectively"
    );
    let wb = settings
        .writeback_settings(WritebackAccessMode::ReadWrite)?
        .context("requires enabled raw writeback")?;
    ensure!(
        wb.ack_mode == AckMode::Memory
            && wb.shutdown_flush == ShutdownFlush::Local
            && wb.memory_bytes == 8 * MIB
            && wb.disk_bytes == 16 * MIB
            && (1..=8 * MIB).contains(&wb.min_free_bytes)
            && wb.local_concurrency == 1
            && wb.upload_concurrency == 2
            && wb.high_watermark_percent == 95
            && wb.resume_percent == 85,
        "requires memory ack, local shutdown, 8 MiB journal RAM, 16 MiB disk, up to 8 MiB free reserve, one local and two upload workers, and 95/85 watermarks"
    );
    ensure!(
        write_ack.mode == FilesystemWriteAckMode::VolatileMemory
            && write_ack.volatile_memory_bytes == 16 * MIB
            && write_ack.volatile_max_operations == 128
            && write_ack.client_durability_target == ClientDurabilityTarget::LocalSsd,
        "requires 16 MiB/128-operation volatile filesystem writes with explicit flush to local durability"
    );
    ensure!(
        clean_cache_bytes == 96 * MIB
            && settings.cache.disk_size_gb == 0.0
            && settings.cache.warm_metadata == WarmMetadata::Off,
        "requires the 96 MiB resolved clean cache, zero disk cache, and warm_metadata off"
    );
    let servers = &settings.servers;
    ensure!(
        servers.nbd.is_none()
            && servers.nfs.is_none()
            && servers
                .ninep
                .as_ref()
                .is_some_and(|s| s.unix_socket.is_some()
                    && s.addresses
                        .as_ref()
                        .is_none_or(|addresses| addresses.is_empty())
                    && s.shared_identity.is_none())
            && servers.rpc.as_ref().is_some_and(|s| s.unix_socket.is_some()
                && s.addresses
                    .as_ref()
                    .is_none_or(|addresses| addresses.is_empty()))
            && servers
                .webui
                .as_ref()
                .is_some_and(|s| s.addresses.len() == 1 && s.p9_idle_timeout_secs == 120),
        "requires Unix-only 9P and RPC, one WebUI listener with a 120s session timeout, and NFS/NBD disabled"
    );
    ensure!(
        settings.replication.is_none()
            && settings.telemetry.as_ref().is_none_or(|s| !s.enabled)
            && settings
                .prometheus
                .as_ref()
                .is_none_or(|s| s.addresses.len() <= 1)
            && settings.name_mode() == NameMode::Insensitive
            && settings.compression() == CompressionConfig::Lz4
            && settings
                .filesystem
                .as_ref()
                .is_some_and(|s| !s.ignore_fsync),
        "requires package filesystem and diagnostic settings with replication and telemetry disabled"
    );
    let gc = settings.gc.unwrap_or_default();
    let defaults = GcConfig::default();
    ensure!(
        settings
            .lsm
            .is_none_or(|s| s.l0_max_ssts() == LsmConfig::DEFAULT_L0_MAX_SSTS
                && s.max_concurrent_compactions() == LsmConfig::DEFAULT_MAX_CONCURRENT_COMPACTIONS
                && s.flush_interval_secs() == LsmConfig::DEFAULT_FLUSH_INTERVAL_SECS
                && !s.sync_writes())
            && gc.interval_secs() == defaults.interval_secs()
            && gc.idle_interval_secs() == defaults.idle_interval_secs()
            && gc.read_directed() == defaults.read_directed()
            && gc.tail_scrub_min_dead_percent() == defaults.tail_scrub_min_dead_percent()
            && gc.min_batches_per_pass() == defaults.min_batches_per_pass()
            && gc.busy_backlog_interval_secs() == defaults.busy_backlog_interval_secs()
            && gc.busy_backlog_dead_percent() == defaults.busy_backlog_dead_percent()
            && gc.compact_round_bytes() == defaults.compact_round_bytes(),
        "requires default LSM and GC geometry and sync_writes off"
    );
    Ok(())
}

// The binding path verifies the opened loop's identity and geometry. Check
// the declared backing before admission so a larger fixture cannot qualify
// solely by advertising a smaller logical member capacity.
fn check_backing_size(path: &Path, expected_bytes: u64) -> Result<()> {
    let metadata = std::fs::symlink_metadata(path)
        .with_context(|| format!("reading raw backing metadata at {}", path.display()))?;
    ensure!(
        metadata.is_file() && metadata.len() == expected_bytes,
        "raw backing {} must be a regular file of exactly {expected_bytes} bytes",
        path.display()
    );
    Ok(())
}
