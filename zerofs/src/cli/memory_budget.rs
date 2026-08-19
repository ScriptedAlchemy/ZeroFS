use crate::config::Settings;
use crate::writeback::config::WritebackAccessMode;
use anyhow::{Context, Result, bail};
use std::io::ErrorKind;
use std::path::{Path, PathBuf};

const GIB: u64 = 1024 * 1024 * 1024;
const FIXED_PROCESS_HEADROOM_BYTES: u64 = 8 * GIB;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct StartupMemoryTiers {
    pub(crate) clean_cache_bytes: u64,
    pub(crate) object_writeback_bytes: u64,
    pub(crate) volatile_write_bytes: u64,
}

impl StartupMemoryTiers {
    pub(crate) fn from_settings(settings: &Settings, volatile_write_bytes: u64) -> Result<Self> {
        let configured_clean = decimal_gb_to_bytes(
            "[cache] memory_size_gb",
            settings.cache.memory_size_gb.unwrap_or(0.25),
        )?;
        let configured_clean = usize::try_from(configured_clean)
            .context("[cache] memory_size_gb exceeds this platform's address space")?;
        let (parts, blocks, extents) = super::server::split_memory_budget(configured_clean);
        let clean_cache_bytes = (parts as u64)
            .checked_add(blocks as u64)
            .and_then(|total| total.checked_add(extents as u64))
            .context("resolved clean-cache memory budget overflowed")?;
        let object_writeback_bytes = settings
            .writeback_settings(WritebackAccessMode::ReadWrite)?
            .map_or(0, |writeback| writeback.memory_bytes);

        Ok(Self {
            clean_cache_bytes,
            object_writeback_bytes,
            // The caller supplies the one normalized volatile-write budget. In
            // configurations that retain a legacy NBD alias, it must not be
            // added a second time here.
            volatile_write_bytes,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct MemoryEnvelope {
    pub(crate) hard_limit_bytes: u64,
    pub(crate) source: PathBuf,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct MemoryBudgetReceipt {
    pub(crate) required_bytes: u64,
    pub(crate) remaining_bytes: u64,
}

pub(crate) fn validate_server_startup(
    settings: &Settings,
    volatile_write_bytes: u64,
) -> Result<Option<MemoryBudgetReceipt>> {
    let tiers = StartupMemoryTiers::from_settings(settings, volatile_write_bytes)?;
    let detected = detect_process_cgroup_v2_memory_limit()?;
    let configured = settings.runtime_memory_limit_bytes()?;
    let Some(envelope) = select_memory_limit(detected, configured) else {
        tracing::warn!(
            "no finite cgroup-v2 memory.max or [runtime] memory_limit_gb; startup memory budget cannot be enforced"
        );
        return Ok(None);
    };
    let source = envelope.source.display().to_string();
    let receipt = validate_memory_budget(tiers, envelope.hard_limit_bytes, &source)?;
    tracing::info!(
        hard_limit_bytes = envelope.hard_limit_bytes,
        required_bytes = receipt.required_bytes,
        remaining_bytes = receipt.remaining_bytes,
        source,
        "startup memory budget accepted"
    );
    Ok(Some(receipt))
}

pub(crate) fn validate_memory_budget(
    tiers: StartupMemoryTiers,
    hard_limit_bytes: u64,
    source: &str,
) -> Result<MemoryBudgetReceipt> {
    // The clean-cache capacity counts payload, not the transient duplicate
    // residency incurred by eviction, GC and allocator bookkeeping. Reserve
    // half of that payload ceiling plus a fixed process allowance. This policy
    // rejects the production incident's 64 GB + 4 GB tiers under 96 GiB while
    // retaining a useful 48 GB cache at that limit.
    let transient_clean_headroom = tiers.clean_cache_bytes / 2;
    let fixed_process_headroom = FIXED_PROCESS_HEADROOM_BYTES.min(hard_limit_bytes / 4);
    let required_bytes = tiers
        .clean_cache_bytes
        .checked_add(tiers.object_writeback_bytes)
        .and_then(|total| total.checked_add(tiers.volatile_write_bytes))
        .and_then(|total| total.checked_add(transient_clean_headroom))
        .and_then(|total| total.checked_add(fixed_process_headroom))
        .context("startup memory budget overflowed")?;
    if required_bytes > hard_limit_bytes {
        bail!(
            "unsafe startup memory budget: {:.2} GB clean cache + {:.2} GB object writeback + \
             {:.2} GB volatile writes require {:.2} GiB including transient and process \
             headroom, above the {:.2} GiB hard limit from {source}",
            decimal_gb(tiers.clean_cache_bytes),
            decimal_gb(tiers.object_writeback_bytes),
            decimal_gb(tiers.volatile_write_bytes),
            gib(required_bytes),
            gib(hard_limit_bytes),
        );
    }
    Ok(MemoryBudgetReceipt {
        required_bytes,
        remaining_bytes: hard_limit_bytes - required_bytes,
    })
}

pub(crate) fn select_memory_limit(
    detected: Option<MemoryEnvelope>,
    configured_bytes: Option<u64>,
) -> Option<MemoryEnvelope> {
    let configured = configured_bytes.map(|hard_limit_bytes| MemoryEnvelope {
        hard_limit_bytes,
        source: PathBuf::from("[runtime] memory_limit_gb"),
    });
    match (detected, configured) {
        (Some(detected), Some(configured)) => Some(
            if detected.hard_limit_bytes <= configured.hard_limit_bytes {
                detected
            } else {
                configured
            },
        ),
        (Some(detected), None) => Some(detected),
        (None, configured) => configured,
    }
}

fn detect_process_cgroup_v2_memory_limit() -> Result<Option<MemoryEnvelope>> {
    detect_cgroup_v2_memory_limit(
        Path::new("/proc/self/cgroup"),
        Path::new("/proc/self/mountinfo"),
    )
}

pub(crate) fn detect_cgroup_v2_memory_limit(
    proc_cgroup: &Path,
    mountinfo: &Path,
) -> Result<Option<MemoryEnvelope>> {
    let cgroup = match std::fs::read_to_string(proc_cgroup) {
        Ok(value) => value,
        Err(error) if error.kind() == ErrorKind::NotFound => return Ok(None),
        Err(error) => {
            return Err(error).with_context(|| format!("reading {}", proc_cgroup.display()));
        }
    };
    let mountinfo = match std::fs::read_to_string(mountinfo) {
        Ok(value) => value,
        Err(error) if error.kind() == ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error).context("reading cgroup mountinfo"),
    };
    let mounts = parse_cgroup2_mounts(&mountinfo)?;
    if mounts.is_empty() {
        return Ok(None);
    }
    let current = parse_unified_cgroup(&cgroup)?;
    let Some((mountpoint, mut path)) = select_cgroup2_mount(&current, &mounts) else {
        return Ok(None);
    };
    let mut limiting: Option<MemoryEnvelope> = None;
    loop {
        let limit_path = path.join("memory.max");
        match std::fs::read_to_string(&limit_path) {
            Ok(raw) => {
                let value = raw.trim();
                if value != "max" {
                    let hard_limit_bytes = value.parse::<u64>().with_context(|| {
                        format!(
                            "invalid cgroup memory limit {value:?} in {}",
                            limit_path.display()
                        )
                    })?;
                    if limiting
                        .as_ref()
                        .is_none_or(|current| hard_limit_bytes < current.hard_limit_bytes)
                    {
                        limiting = Some(MemoryEnvelope {
                            hard_limit_bytes,
                            source: limit_path,
                        });
                    }
                }
            }
            Err(error) if error.kind() == ErrorKind::NotFound => {}
            Err(error) => {
                return Err(error).with_context(|| format!("reading {}", limit_path.display()));
            }
        }
        if path == mountpoint {
            break;
        }
        path = path
            .parent()
            .filter(|parent| parent.starts_with(&mountpoint))
            .context("cgroup path escaped its cgroup2 mount")?
            .to_path_buf();
    }
    Ok(limiting)
}

fn parse_unified_cgroup(contents: &str) -> Result<PathBuf> {
    for line in contents.lines() {
        let mut fields = line.splitn(3, ':');
        if fields.next() == Some("0") && fields.next() == Some("") {
            let path = fields.next().context("cgroup-v2 entry has no path")?;
            return Ok(PathBuf::from(path));
        }
    }
    bail!("/proc/self/cgroup contains no unified cgroup-v2 entry")
}

fn parse_cgroup2_mounts(contents: &str) -> Result<Vec<(PathBuf, PathBuf)>> {
    let mut mounts = Vec::new();
    for line in contents.lines() {
        let fields: Vec<_> = line.split_whitespace().collect();
        let Some(separator) = fields.iter().position(|field| *field == "-") else {
            continue;
        };
        if fields.get(separator + 1) != Some(&"cgroup2") {
            continue;
        }
        let root = fields
            .get(3)
            .context("cgroup2 mountinfo entry has no root")?;
        let mountpoint = fields
            .get(4)
            .context("cgroup2 mountinfo entry has no mountpoint")?;
        mounts.push((
            PathBuf::from(unescape_mountinfo(root)?),
            PathBuf::from(unescape_mountinfo(mountpoint)?),
        ));
    }
    Ok(mounts)
}

fn select_cgroup2_mount(
    current: &Path,
    mounts: &[(PathBuf, PathBuf)],
) -> Option<(PathBuf, PathBuf)> {
    let exact = mounts
        .iter()
        .filter_map(|(root, mountpoint)| {
            current
                .strip_prefix(root)
                .ok()
                .map(|relative| (root, mountpoint, relative))
        })
        .max_by_key(|(root, _, _)| root.components().count());
    if let Some((_, mountpoint, relative)) = exact {
        return Some((mountpoint.clone(), mountpoint.join(relative)));
    }

    // A private cgroup namespace exposes its own root as `/`, while mountinfo
    // may retain the host-side subtree in the mount root field. In that case,
    // the process cgroup path is already relative to the mounted subtree.
    mounts
        .iter()
        .max_by_key(|(root, _)| root.components().count())
        .map(|(_, mountpoint)| {
            let relative = current.strip_prefix(Path::new("/")).unwrap_or(current);
            (mountpoint.clone(), mountpoint.join(relative))
        })
}

fn unescape_mountinfo(value: &str) -> Result<String> {
    let mut output = String::with_capacity(value.len());
    let bytes = value.as_bytes();
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] == b'\\' {
            let octal = bytes
                .get(index + 1..index + 4)
                .context("truncated mountinfo escape")?;
            if !octal.iter().all(u8::is_ascii_digit) || octal.iter().any(|byte| *byte > b'7') {
                bail!("invalid mountinfo escape in {value:?}");
            }
            let decoded = (octal[0] - b'0') * 64 + (octal[1] - b'0') * 8 + octal[2] - b'0';
            output.push(char::from(decoded));
            index += 4;
        } else {
            output.push(char::from(bytes[index]));
            index += 1;
        }
    }
    Ok(output)
}

fn decimal_gb_to_bytes(name: &str, value: f64) -> Result<u64> {
    if !value.is_finite() || value <= 0.0 {
        bail!("{name} must be a finite positive number");
    }
    let bytes = value * 1_000_000_000.0;
    if bytes > u64::MAX as f64 {
        bail!("{name} is too large");
    }
    Ok(bytes.round() as u64)
}

fn decimal_gb(bytes: u64) -> f64 {
    bytes as f64 / 1_000_000_000.0
}

fn gib(bytes: u64) -> f64 {
    bytes as f64 / GIB as f64
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    const GIB: u64 = 1024 * 1024 * 1024;

    #[test]
    fn incident_budget_rejects_64gb_clean_cache_inside_96gib() {
        let tiers = StartupMemoryTiers {
            clean_cache_bytes: 64_000_000_000,
            object_writeback_bytes: 4_000_000_000,
            volatile_write_bytes: 0,
        };

        let error = validate_memory_budget(tiers, 96 * GIB, "test cgroup").unwrap_err();

        let message = format!("{error:#}");
        assert!(message.contains("64.00 GB clean cache"), "{message}");
        assert!(message.contains("4.00 GB object writeback"), "{message}");
        assert!(message.contains("96.00 GiB hard limit"), "{message}");
    }

    #[test]
    fn incident_budget_allows_48gb_clean_cache_inside_96gib() {
        let tiers = StartupMemoryTiers {
            clean_cache_bytes: 48_000_000_000,
            object_writeback_bytes: 4_000_000_000,
            volatile_write_bytes: 0,
        };

        let receipt = validate_memory_budget(tiers, 96 * GIB, "test cgroup").unwrap();

        assert_eq!(receipt.required_bytes, 84_589_934_592);
        assert_eq!(receipt.remaining_bytes, 18_489_280_512);
    }

    #[test]
    fn startup_budget_overflow_fails_closed() {
        let tiers = StartupMemoryTiers {
            clean_cache_bytes: u64::MAX,
            object_writeback_bytes: 1,
            volatile_write_bytes: 0,
        };

        let error = validate_memory_budget(tiers, u64::MAX, "test cgroup").unwrap_err();

        assert!(format!("{error:#}").contains("overflowed"));
    }

    #[test]
    fn cgroup_discovery_uses_finite_parent_when_service_is_unlimited() {
        let temp = tempfile::tempdir().unwrap();
        let cgroup_mount = temp.path().join("cgroup");
        let service = cgroup_mount.join("system.slice/zerofs.service");
        fs::create_dir_all(&service).unwrap();
        fs::write(service.join("memory.max"), "max\n").unwrap();
        fs::write(
            cgroup_mount.join("system.slice/memory.max"),
            "103079215104\n",
        )
        .unwrap();
        fs::write(cgroup_mount.join("memory.max"), "max\n").unwrap();
        let proc_cgroup = temp.path().join("cgroup.txt");
        fs::write(&proc_cgroup, "0::/system.slice/zerofs.service\n").unwrap();
        let mountinfo = temp.path().join("mountinfo.txt");
        fs::write(
            &mountinfo,
            format!(
                "36 25 0:32 / {} rw,nosuid,nodev,noexec,relatime - cgroup2 cgroup rw\n",
                cgroup_mount.display()
            ),
        )
        .unwrap();

        let envelope = detect_cgroup_v2_memory_limit(&proc_cgroup, &mountinfo)
            .unwrap()
            .unwrap();

        assert_eq!(envelope.hard_limit_bytes, 103_079_215_104);
        assert_eq!(
            envelope.source,
            cgroup_mount.join("system.slice/memory.max")
        );
    }

    #[test]
    fn missing_memory_controller_allows_the_configured_fallback() {
        let temp = tempfile::tempdir().unwrap();
        let cgroup_mount = temp.path().join("cgroup");
        fs::create_dir_all(cgroup_mount.join("system.slice/zerofs.service")).unwrap();
        let proc_cgroup = temp.path().join("cgroup.txt");
        fs::write(&proc_cgroup, "0::/system.slice/zerofs.service\n").unwrap();
        let mountinfo = temp.path().join("mountinfo.txt");
        fs::write(
            &mountinfo,
            format!(
                "36 25 0:32 / {} rw,nosuid,nodev,noexec,relatime - cgroup2 cgroup rw\n",
                cgroup_mount.display()
            ),
        )
        .unwrap();

        let detected = detect_cgroup_v2_memory_limit(&proc_cgroup, &mountinfo).unwrap();
        let envelope = select_memory_limit(detected, Some(96_000_000_000)).unwrap();

        assert_eq!(envelope.hard_limit_bytes, 96_000_000_000);
        assert_eq!(envelope.source, PathBuf::from("[runtime] memory_limit_gb"));
    }

    #[test]
    fn cgroup_namespace_root_maps_to_the_mounted_host_subtree() {
        let temp = tempfile::tempdir().unwrap();
        let cgroup_mount = temp.path().join("cgroup");
        fs::create_dir_all(&cgroup_mount).unwrap();
        fs::write(cgroup_mount.join("memory.max"), "103079215104\n").unwrap();
        let proc_cgroup = temp.path().join("cgroup.txt");
        fs::write(&proc_cgroup, "0::/\n").unwrap();
        let mountinfo = temp.path().join("mountinfo.txt");
        fs::write(
            &mountinfo,
            format!(
                "36 25 0:32 /docker/abc {} rw,nosuid,nodev,noexec,relatime - cgroup2 cgroup rw\n",
                cgroup_mount.display()
            ),
        )
        .unwrap();

        let envelope = detect_cgroup_v2_memory_limit(&proc_cgroup, &mountinfo)
            .unwrap()
            .unwrap();

        assert_eq!(envelope.hard_limit_bytes, 103_079_215_104);
        assert_eq!(envelope.source, cgroup_mount.join("memory.max"));
    }

    #[test]
    fn cgroup_discovery_selects_the_most_specific_matching_mount() {
        let current = Path::new("/system.slice/zerofs.service");
        let mounts = vec![
            (PathBuf::from("/"), PathBuf::from("/sys/fs/cgroup")),
            (
                PathBuf::from("/system.slice"),
                PathBuf::from("/run/zerofs-cgroup"),
            ),
        ];

        let selected = select_cgroup2_mount(current, &mounts).unwrap();

        assert_eq!(selected.0, PathBuf::from("/run/zerofs-cgroup"));
        assert_eq!(
            selected.1,
            PathBuf::from("/run/zerofs-cgroup/zerofs.service")
        );
    }

    #[test]
    fn system_without_cgroup_v2_returns_no_detected_limit() {
        let temp = tempfile::tempdir().unwrap();
        let proc_cgroup = temp.path().join("cgroup.txt");
        fs::write(&proc_cgroup, "2:memory:/zerofs\n").unwrap();
        let mountinfo = temp.path().join("mountinfo.txt");
        fs::write(
            &mountinfo,
            "35 25 0:31 / /sys/fs/cgroup/memory rw - cgroup cgroup rw,memory\n",
        )
        .unwrap();

        let detected = detect_cgroup_v2_memory_limit(&proc_cgroup, &mountinfo).unwrap();

        assert_eq!(detected, None);
    }

    #[test]
    fn configured_limit_is_used_when_cgroup_namespace_hides_the_outer_limit() {
        let selected = select_memory_limit(None, Some(96_000_000_000)).unwrap();

        assert_eq!(selected.hard_limit_bytes, 96_000_000_000);
        assert_eq!(
            selected.source,
            std::path::PathBuf::from("[runtime] memory_limit_gb")
        );
    }

    #[test]
    fn finite_cgroup_limit_wins_over_a_larger_configured_fallback() {
        let selected = select_memory_limit(
            Some(MemoryEnvelope {
                hard_limit_bytes: 80_000_000_000,
                source: "/sys/fs/cgroup/memory.max".into(),
            }),
            Some(96_000_000_000),
        )
        .unwrap();

        assert_eq!(selected.hard_limit_bytes, 80_000_000_000);
        assert_eq!(
            selected.source,
            std::path::PathBuf::from("/sys/fs/cgroup/memory.max")
        );
    }

    #[test]
    fn tier_accounting_counts_the_normalized_volatile_budget_once() {
        let mut settings = crate::config::Settings::generate_default();
        settings.cache.dir = "/tmp/zerofs-clean".into();
        settings.cache.memory_size_gb = Some(64.0);
        settings.writeback = Some(crate::writeback::config::WritebackConfig {
            enabled: true,
            dir: "/tmp/zerofs-writeback".into(),
            memory_size_gb: 4.0,
            disk_size_gb: 64.0,
            min_free_gb: 32.0,
            ..Default::default()
        });
        let nbd = settings.servers.nbd.as_mut().unwrap();
        nbd.write_ack_mode = crate::config::NbdWriteAckMode::VolatileMemory;
        nbd.volatile_memory_gb = 7.0;

        let tiers = StartupMemoryTiers::from_settings(&settings, 7_000_000_000).unwrap();

        assert_eq!(tiers.clean_cache_bytes, 64_000_000_000);
        assert_eq!(tiers.object_writeback_bytes, 4_000_000_000);
        assert_eq!(tiers.volatile_write_bytes, 7_000_000_000);
    }
}
