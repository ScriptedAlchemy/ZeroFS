use super::package_policy::{gc_matches_defaults, lsm_matches_defaults};
use crate::config::{CompressionConfig, SegmentPlacement, Settings, WarmMetadata};
use crate::fs::mutation::config::{
    ClientDurabilityTarget, FilesystemWriteAckMode, FilesystemWriteAckSettings,
};
use crate::fs::name_mode::NameMode;
use crate::storage_backend::raw::{RawBinding, RawKind};
use crate::writeback::config::{AckMode, ShutdownFlush, WritebackAccessMode};
use anyhow::{Context, Result, ensure};
use std::collections::BTreeSet;
use std::path::Path;
use url::Url;
use zerofs_raw_device::p3::P3Layout;

pub(super) const NAME: &str = "raw-device-p3-candidate-v1";
pub(super) const NAS_FIT_NAME: &str = "raw-device-p3-nas-fit-v1";
const MIB: u64 = 1 << 20;
const GIB: u64 = 1 << 30;
pub(super) const MIN_CGROUP_BYTES: u64 = 8 * GIB;
pub(super) const MAX_CGROUP_BYTES: u64 = 16 * GIB;
pub(super) const COMPANION_BYTES: u64 = GIB;
// The Linux synthetic claim caps each device at 256 MiB. Budget two full
// device views for each of three devices, plus 512 MiB for indexes, journal
// database caches and replay staging. This is a provisional allowance.
pub(super) const RAW_RESIDENCY_BYTES: u64 = 2 * (3 * 256 * MIB) + 512 * MIB;

pub(super) const NAS_FIT_COMPANION_BYTES: u64 = 3 * GIB / 2;
// At the fixed NAS geometry, the reclaiming raw stores use up to 699 MiB
// of HDD allocation bitmap and 895 MiB of charged HDD live indexes.
// The remainder covers pending retirements, P3 roles and journal/catalog caches.
pub(super) const NAS_FIT_RESIDENCY_BYTES: u64 = 2_112 * MIB;

const LEGACY_LAYOUT: [u64; 6] = [96, 32, 88, 8, 16, 16];
const P3_BYTES: u64 = 1_000_204_886_016;
const HDD6_BYTES: u64 = 6_001_175_126_016;
const HDD18_BYTES: u64 = 18_000_207_937_536;

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
    let nas_fit = settings.raw_device_p3_nas_fit();
    let authority = RawBinding::parse(&Url::parse(&settings.storage.url)?)?;
    let raw_kind = authority.kind;
    let device_bytes = match raw_kind {
        RawKind::SmallLoop if !nas_fit => 256 * MIB,
        RawKind::LargeLoop if nas_fit => P3Layout::NAS_FIT_FIXTURE_BYTES,
        RawKind::Physical if nas_fit && authority.node == Path::new("/nvme") => P3_BYTES,
        _ => anyhow::bail!("raw profile does not permit this P3 binding"),
    };
    let layout = authority
        .layout
        .context("requires the profile's P3 layout")?;
    if nas_fit {
        P3Layout::validate_nas_fit(device_bytes, layout)?;
    }
    ensure!(
        (nas_fit || layout == LEGACY_LAYOUT.map(|n| n * MIB))
            && pool.catalog_dir.as_os_str().is_empty()
            && settings.storage.readiness.is_none()
            && pool.segment_placement == SegmentPlacement::RoundRobinLocal,
        "requires the profile's P3 layout, empty catalog_dir, no directory readiness, and round_robin_local placement"
    );
    ensure!(pool.members.len() == 2, "requires exactly two raw members");
    if let Some(backing) = authority.backing_path() {
        check_backing_size(backing, device_bytes, raw_kind)?;
    }
    let mut member_ids = BTreeSet::from([authority.member]);
    let mut geometry = Vec::with_capacity(2);
    for member in &pool.members {
        let binding = RawBinding::parse(&Url::parse(&member.url)?)?;
        ensure!(
            binding.kind == raw_kind,
            "requires one raw device type for the entire pool"
        );
        ensure!(
            binding.pool == authority.pool && member_ids.insert(binding.member),
            "requires distinct device UUIDs within one raw pool"
        );
        let capacity = member.capacity_bytes()?;
        let reserve = member.reserve_free_bytes()?;
        geometry.push((capacity, reserve));
        if let Some(backing) = binding.backing_path() {
            check_backing_size(backing, capacity, raw_kind)?;
        }
        if raw_kind == RawKind::Physical {
            ensure!(
                (binding.node == Path::new("/hdd6") && capacity == HDD6_BYTES)
                    || (binding.node == Path::new("/hdd18") && capacity == HDD18_BYTES),
                "requires the designated physical HDD paths and exact byte capacities"
            );
        }
    }
    geometry.sort_unstable();
    let expected = match raw_kind {
        RawKind::SmallLoop => [(32 * MIB, 16 * MIB), (96 * MIB, 8 * MIB)],
        RawKind::LargeLoop => [(12 * GIB, 256 * MIB), (16 * GIB, 256 * MIB)],
        RawKind::Physical => [
            (HDD6_BYTES, HDD6_BYTES.div_ceil(100)),
            (HDD18_BYTES, HDD18_BYTES.div_ceil(100)),
        ],
    };
    let mismatched_geometry = if raw_kind == RawKind::SmallLoop {
        "requires 32/96 MiB member capacities with reserves in 16–32/8–96 MiB respectively"
    } else {
        "requires the profile's exact member capacities and reserves between the profile minimum and full capacity"
    };
    ensure!(
        geometry
            .iter()
            .zip(expected)
            .all(|(&(capacity, reserve), (size, minimum))| capacity == size
                && (minimum..=size).contains(&reserve)),
        "{mismatched_geometry}"
    );
    let wb = settings
        .writeback_settings(WritebackAccessMode::ReadWrite)?
        .context("requires enabled raw writeback")?;
    let (disk_bytes, reserve, disk_label) = if nas_fit {
        (
            10 * GIB,
            4 * GIB..=4 * GIB,
            "10 GiB disk and 4 GiB free reserve",
        )
    } else {
        (
            16 * MIB,
            1..=8 * MIB,
            "16 MiB disk and up to 8 MiB free reserve",
        )
    };
    ensure!(
        wb.ack_mode == AckMode::Memory
            && wb.shutdown_flush == ShutdownFlush::Local
            && wb.memory_bytes == 16 * MIB
            && wb.disk_bytes == disk_bytes
            && reserve.contains(&wb.min_free_bytes)
            && wb.local_concurrency == 1
            && wb.upload_concurrency == 2
            && wb.high_watermark_percent == 95
            && wb.resume_percent == 85,
        "requires memory ack, local shutdown, 16 MiB journal RAM, {disk_label}, one local and two upload workers, and 95/85 watermarks"
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
    ensure!(
        lsm_matches_defaults(settings.lsm) && gc_matches_defaults(settings.gc),
        "requires default LSM and GC geometry and sync_writes off"
    );
    Ok(())
}

// The binding path verifies the opened loop's identity and geometry. Check
// the declared backing before admission so a larger fixture cannot qualify
// solely by advertising a smaller logical member capacity.
fn check_backing_size(path: &Path, expected_bytes: u64, kind: RawKind) -> Result<()> {
    let metadata = std::fs::symlink_metadata(path)
        .with_context(|| format!("reading raw backing metadata at {}", path.display()))?;
    ensure!(
        metadata.is_file() && metadata.len() == expected_bytes,
        "raw backing {} must be a regular file of exactly {expected_bytes} bytes",
        path.display()
    );
    #[cfg(target_os = "linux")]
    if kind == RawKind::LargeLoop {
        use std::os::unix::fs::OpenOptionsExt;
        let backing = std::fs::OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
            .open(path)?;
        let metadata = backing.metadata()?;
        ensure!(
            metadata.is_file() && metadata.len() == expected_bytes,
            "raw backing {} changed during admission",
            path.display()
        );
        let stat = rustix::fs::fstatfs(backing)?;
        ensure!(
            stat.f_type != libc::TMPFS_MAGIC && stat.f_type != 0x8584_58f6u32 as libc::c_long,
            "large raw fixture backing {} must reside on a non-RAM filesystem",
            path.display()
        );
    }
    #[cfg(not(target_os = "linux"))]
    let _ = kind;
    Ok(())
}
