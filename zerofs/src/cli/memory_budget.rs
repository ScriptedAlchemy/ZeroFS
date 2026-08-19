use crate::config::Settings;
use crate::writeback::config::WritebackAccessMode;
use anyhow::{Context, Result, bail};
use std::ffi::OsString;
use std::io::ErrorKind;
#[cfg(unix)]
use std::os::unix::ffi::OsStringExt;
use std::path::{Path, PathBuf};

const GIB: u64 = 1024 * 1024 * 1024;
const UNMODELED_RESIDENCY_RESERVE_BYTES: u64 = 32 * GIB;
const COMPANION_PROCESS_RESERVE_BYTES: u64 = 8 * GIB;

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

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct CgroupMemoryLimits {
    pub(crate) dedicated: Option<MemoryEnvelope>,
    pub(crate) shared_ceiling: Option<MemoryEnvelope>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct MemoryBudgetReceipt {
    pub(crate) hard_limit_bytes: u64,
    pub(crate) required_bytes: u64,
    pub(crate) remaining_bytes: u64,
    pub(crate) rss_pressure_cap_bytes: u64,
}

pub(crate) fn validate_server_startup(
    settings: &Settings,
    volatile_write_bytes: u64,
) -> Result<Option<MemoryBudgetReceipt>> {
    let tiers = StartupMemoryTiers::from_settings(settings, volatile_write_bytes)?;
    let detected = detect_process_cgroup_v2_memory_limit()?;
    let configured = settings.runtime_memory_limit_bytes()?;
    let Some(envelope) = select_memory_limit(detected, configured)? else {
        tracing::warn!(
            "no cgroup-v2 memory ceiling or [runtime] memory_limit_gb; startup memory defense-in-depth admission is unavailable"
        );
        return Ok(None);
    };
    let source = envelope.source.display().to_string();
    let receipt = validate_memory_budget(tiers, envelope.hard_limit_bytes, &source)?;
    tracing::info!(
        hard_limit_bytes = envelope.hard_limit_bytes,
        required_bytes = receipt.required_bytes,
        remaining_bytes = receipt.remaining_bytes,
        rss_pressure_cap_bytes = receipt.rss_pressure_cap_bytes,
        unmodeled_residency_reserve_bytes = UNMODELED_RESIDENCY_RESERVE_BYTES,
        companion_process_reserve_bytes = COMPANION_PROCESS_RESERVE_BYTES,
        source,
        "startup memory defense-in-depth admission passed"
    );
    Ok(Some(receipt))
}

pub(crate) fn validate_memory_budget(
    tiers: StartupMemoryTiers,
    hard_limit_bytes: u64,
    source: &str,
) -> Result<MemoryBudgetReceipt> {
    // The production OOM reproduced roughly 28.6 GiB of residency above the
    // charged cache/writeback tiers. Round that unmodeled excess up to 32 GiB,
    // then retain a separate 8 GiB allowance for GC, protocol companions and
    // the rest of the process. These are conservative admission reserves, not
    // proofs that every allocation path is bounded.
    let required_bytes = tiers
        .clean_cache_bytes
        .checked_add(tiers.object_writeback_bytes)
        .and_then(|total| total.checked_add(tiers.volatile_write_bytes))
        .and_then(|total| total.checked_add(UNMODELED_RESIDENCY_RESERVE_BYTES))
        .and_then(|total| total.checked_add(COMPANION_PROCESS_RESERVE_BYTES))
        .context("startup memory budget overflowed")?;
    if required_bytes > hard_limit_bytes {
        bail!(
            "startup memory admission rejected: {:.2} GB clean cache + {:.2} GB object writeback + \
             {:.2} GB volatile writes + 32.00 GiB observed-unmodeled reserve + 8.00 GiB \
             companion/process reserve require {:.2} GiB, above the {:.2} GiB hard limit \
             from {source}",
            decimal_gb(tiers.clean_cache_bytes),
            decimal_gb(tiers.object_writeback_bytes),
            decimal_gb(tiers.volatile_write_bytes),
            gib(required_bytes),
            gib(hard_limit_bytes),
        );
    }
    let rss_pressure_cap_bytes = hard_limit_bytes
        .checked_sub(UNMODELED_RESIDENCY_RESERVE_BYTES)
        .and_then(|cap| cap.checked_sub(COMPANION_PROCESS_RESERVE_BYTES))
        .context("startup memory envelope is smaller than its fixed policy reserves")?;
    Ok(MemoryBudgetReceipt {
        hard_limit_bytes,
        required_bytes,
        remaining_bytes: hard_limit_bytes - required_bytes,
        rss_pressure_cap_bytes,
    })
}

pub(crate) fn select_memory_limit(
    detected: CgroupMemoryLimits,
    configured_bytes: Option<u64>,
) -> Result<Option<MemoryEnvelope>> {
    let configured = configured_bytes.map(|hard_limit_bytes| MemoryEnvelope {
        hard_limit_bytes,
        source: PathBuf::from("[runtime] memory_limit_gb"),
    });
    let dedicated = match (detected.dedicated, configured) {
        (Some(detected), Some(configured)) => {
            if detected.hard_limit_bytes <= configured.hard_limit_bytes {
                detected
            } else {
                configured
            }
        }
        (Some(detected), None) => detected,
        (None, Some(configured)) => configured,
        (None, None) if detected.shared_ceiling.is_some() => bail!(
            "no dedicated memory envelope: the visible memory.max belongs to a shared ancestor; \
             configure a finite limit on the ZeroFS service cgroup or set [runtime] \
             memory_limit_gb to its dedicated limit"
        ),
        (None, None) => return Ok(None),
    };
    if let Some(shared) = detected.shared_ceiling
        && dedicated.hard_limit_bytes > shared.hard_limit_bytes
    {
        bail!(
            "dedicated ZeroFS memory envelope {} bytes from {} exceeds the shared ancestor \
             ceiling {} bytes at {}; lower the dedicated limit instead of budgeting the \
             shared parent as process capacity",
            dedicated.hard_limit_bytes,
            dedicated.source.display(),
            shared.hard_limit_bytes,
            shared.source.display(),
        );
    }
    Ok(Some(dedicated))
}

fn detect_process_cgroup_v2_memory_limit() -> Result<CgroupMemoryLimits> {
    detect_cgroup_v2_memory_limit(
        Path::new("/proc/self/cgroup"),
        Path::new("/proc/self/mountinfo"),
    )
}

pub(crate) fn detect_cgroup_v2_memory_limit(
    proc_cgroup: &Path,
    mountinfo: &Path,
) -> Result<CgroupMemoryLimits> {
    let cgroup = match std::fs::read(proc_cgroup) {
        Ok(value) => value,
        Err(error) if error.kind() == ErrorKind::NotFound => {
            return Ok(CgroupMemoryLimits::default());
        }
        Err(error) => {
            return Err(error).with_context(|| format!("reading {}", proc_cgroup.display()));
        }
    };
    let mountinfo = match std::fs::read(mountinfo) {
        Ok(value) => value,
        Err(error) if error.kind() == ErrorKind::NotFound => {
            return Ok(CgroupMemoryLimits::default());
        }
        Err(error) => return Err(error).context("reading cgroup mountinfo"),
    };
    let mounts = parse_cgroup2_mounts(&mountinfo)?;
    if mounts.is_empty() {
        return Ok(CgroupMemoryLimits::default());
    }
    let current = parse_unified_cgroup(&cgroup)?;
    let Some((mountpoint, mut path)) = select_cgroup2_mount(&current, &mounts) else {
        return Ok(CgroupMemoryLimits::default());
    };
    let mut limits = CgroupMemoryLimits::default();
    let mut is_current = true;
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
                    let slot = if is_current {
                        &mut limits.dedicated
                    } else {
                        &mut limits.shared_ceiling
                    };
                    if slot
                        .as_ref()
                        .is_none_or(|current| hard_limit_bytes < current.hard_limit_bytes)
                    {
                        *slot = Some(MemoryEnvelope {
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
        is_current = false;
    }
    Ok(limits)
}

fn parse_unified_cgroup(contents: &[u8]) -> Result<PathBuf> {
    for line in contents.split(|byte| *byte == b'\n') {
        let mut fields = line.splitn(3, |byte| *byte == b':');
        if fields.next() == Some(b"0".as_slice()) && fields.next() == Some(b"".as_slice()) {
            let path = fields.next().context("cgroup-v2 entry has no path")?;
            return Ok(PathBuf::from(os_string_from_bytes(path.to_vec())?));
        }
    }
    bail!("/proc/self/cgroup contains no unified cgroup-v2 entry")
}

fn parse_cgroup2_mounts(contents: &[u8]) -> Result<Vec<(PathBuf, PathBuf)>> {
    let mut mounts = Vec::new();
    for line in contents.split(|byte| *byte == b'\n') {
        let fields: Vec<_> = line
            .split(|byte| *byte == b' ')
            .filter(|field| !field.is_empty())
            .collect();
        let Some(separator) = fields.iter().position(|field| *field == b"-") else {
            continue;
        };
        if fields.get(separator + 1).copied() != Some(b"cgroup2".as_slice()) {
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
    // the process cgroup path is already relative to the mounted subtree. A
    // deeper root is a nested bind, not the namespace root; prefer the
    // shallowest root and retain mountinfo order as the deterministic tie-break.
    mounts
        .iter()
        .min_by_key(|(root, _)| root.components().count())
        .map(|(_, mountpoint)| {
            let relative = current.strip_prefix(Path::new("/")).unwrap_or(current);
            (mountpoint.clone(), mountpoint.join(relative))
        })
}

fn unescape_mountinfo(value: &[u8]) -> Result<OsString> {
    let mut output = Vec::with_capacity(value.len());
    let mut index = 0;
    while index < value.len() {
        if value[index] == b'\\' {
            let octal = value
                .get(index + 1..index + 4)
                .context("truncated mountinfo escape")?;
            if !octal.iter().all(u8::is_ascii_digit) || octal.iter().any(|byte| *byte > b'7') {
                bail!(
                    "invalid mountinfo escape in {:?}",
                    String::from_utf8_lossy(value)
                );
            }
            let decoded = (octal[0] - b'0') * 64 + (octal[1] - b'0') * 8 + (octal[2] - b'0');
            output.push(decoded);
            index += 4;
        } else {
            output.push(value[index]);
            index += 1;
        }
    }
    os_string_from_bytes(output)
}

fn os_string_from_bytes(value: Vec<u8>) -> Result<OsString> {
    #[cfg(unix)]
    {
        Ok(OsString::from_vec(value))
    }
    #[cfg(not(unix))]
    {
        Ok(String::from_utf8(value)
            .context("Linux cgroup path is not valid UTF-8 on this platform")?
            .into())
    }
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
        assert!(
            message.contains("32.00 GiB observed-unmodeled reserve"),
            "{message}"
        );
        assert!(
            message.contains("8.00 GiB companion/process reserve"),
            "{message}"
        );
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

        assert_eq!(receipt.hard_limit_bytes, 96 * GIB);
        assert_eq!(receipt.required_bytes, 94_949_672_960);
        assert_eq!(receipt.remaining_bytes, 8_129_542_144);
        assert_eq!(receipt.rss_pressure_cap_bytes, 56 * GIB);
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
    fn small_envelope_does_not_shrink_policy_reserves_into_acceptance() {
        let tiers = StartupMemoryTiers {
            clean_cache_bytes: 1,
            object_writeback_bytes: 0,
            volatile_write_bytes: 0,
        };

        let error = validate_memory_budget(tiers, 39 * GIB, "test cgroup").unwrap_err();

        assert!(format!("{error:#}").contains("40.00 GiB"));
    }

    #[test]
    fn volatile_writes_are_additive_to_fixed_policy_reserves() {
        let tiers = StartupMemoryTiers {
            clean_cache_bytes: 0,
            object_writeback_bytes: 0,
            volatile_write_bytes: 7 * GIB,
        };

        let receipt = validate_memory_budget(tiers, 48 * GIB, "test cgroup").unwrap();

        assert_eq!(receipt.required_bytes, 47 * GIB);
        assert_eq!(receipt.remaining_bytes, GIB);
        assert_eq!(receipt.rss_pressure_cap_bytes, 8 * GIB);
    }

    #[test]
    fn shared_parent_limit_is_not_treated_as_process_capacity() {
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

        let detected = detect_cgroup_v2_memory_limit(&proc_cgroup, &mountinfo).unwrap();
        assert_eq!(detected.dedicated, None);
        assert_eq!(
            detected
                .shared_ceiling
                .as_ref()
                .map(|limit| limit.hard_limit_bytes),
            Some(103_079_215_104)
        );

        let error = select_memory_limit(detected.clone(), None).unwrap_err();
        assert!(format!("{error:#}").contains("dedicated memory envelope"));

        let configured = select_memory_limit(detected, Some(64 * GIB))
            .unwrap()
            .unwrap();
        assert_eq!(configured.hard_limit_bytes, 64 * GIB);
        assert_eq!(
            configured.source,
            PathBuf::from("[runtime] memory_limit_gb")
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
        let envelope = select_memory_limit(detected, Some(96_000_000_000))
            .unwrap()
            .unwrap();

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

        let limits = detect_cgroup_v2_memory_limit(&proc_cgroup, &mountinfo).unwrap();
        let envelope = limits.dedicated.unwrap();

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
    fn private_namespace_uses_the_outer_cgroup_mount_not_a_nested_bind() {
        let current = Path::new("/");
        let mounts = vec![
            (
                PathBuf::from("/host.slice/zerofs.service/nested"),
                PathBuf::from("/run/nested-cgroup-bind"),
            ),
            (
                PathBuf::from("/host.slice/zerofs.service"),
                PathBuf::from("/sys/fs/cgroup"),
            ),
        ];

        let selected = select_cgroup2_mount(current, &mounts).unwrap();

        assert_eq!(selected.0, PathBuf::from("/sys/fs/cgroup"));
        assert_eq!(selected.1, PathBuf::from("/sys/fs/cgroup"));
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

        assert_eq!(detected, CgroupMemoryLimits::default());
        assert_eq!(select_memory_limit(detected, None).unwrap(), None);
    }

    #[test]
    fn mountinfo_octal_escapes_preserve_non_ascii_path_bytes() {
        let decoded = unescape_mountinfo(br"/sys/fs/cgroup/caf\303\251").unwrap();

        assert_eq!(decoded, OsString::from("/sys/fs/cgroup/café"));
    }

    #[cfg(unix)]
    #[test]
    fn mountinfo_octal_escapes_preserve_non_utf8_path_bytes() {
        use std::os::unix::ffi::OsStrExt;

        let decoded = unescape_mountinfo(br"/sys/fs/cgroup/raw-\377").unwrap();

        assert_eq!(decoded.as_os_str().as_bytes(), b"/sys/fs/cgroup/raw-\xff");
    }

    #[test]
    fn mountinfo_octal_escapes_decode_linux_separator_bytes() {
        let decoded = unescape_mountinfo(br"space\040tab\011line\012slash\134").unwrap();

        assert_eq!(decoded, OsString::from("space tab\tline\nslash\\"));
        for invalid in [br"bad\".as_slice(), br"bad\08x", br"bad\999"] {
            assert!(unescape_mountinfo(invalid).is_err());
        }
    }

    #[cfg(unix)]
    #[test]
    fn mount_selection_joins_non_utf8_paths_without_loss() {
        use std::os::unix::ffi::{OsStrExt, OsStringExt};

        let root = PathBuf::from(OsString::from_vec(b"/service-\xff".to_vec()));
        let mount = PathBuf::from(OsString::from_vec(b"/sys/cgroup-\xfe".to_vec()));
        let current = root.join("zerofs");

        let selected = select_cgroup2_mount(&current, &[(root, mount.clone())]).unwrap();

        assert_eq!(
            selected.0.as_os_str().as_bytes(),
            mount.as_os_str().as_bytes()
        );
        assert_eq!(
            selected.1.as_os_str().as_bytes(),
            b"/sys/cgroup-\xfe/zerofs"
        );
    }

    #[test]
    fn configured_limit_is_used_when_cgroup_namespace_hides_the_outer_limit() {
        let selected = select_memory_limit(CgroupMemoryLimits::default(), Some(96_000_000_000))
            .unwrap()
            .unwrap();

        assert_eq!(selected.hard_limit_bytes, 96_000_000_000);
        assert_eq!(
            selected.source,
            std::path::PathBuf::from("[runtime] memory_limit_gb")
        );
    }

    #[test]
    fn finite_cgroup_limit_wins_over_a_larger_configured_fallback() {
        let selected = select_memory_limit(
            CgroupMemoryLimits {
                dedicated: Some(MemoryEnvelope {
                    hard_limit_bytes: 80_000_000_000,
                    source: "/sys/fs/cgroup/memory.max".into(),
                }),
                shared_ceiling: None,
            },
            Some(96_000_000_000),
        )
        .unwrap();
        let selected = selected.unwrap();

        assert_eq!(selected.hard_limit_bytes, 80_000_000_000);
        assert_eq!(
            selected.source,
            std::path::PathBuf::from("/sys/fs/cgroup/memory.max")
        );
    }

    #[test]
    fn shared_ceiling_can_reject_but_never_authorize_a_budget() {
        let limits = CgroupMemoryLimits {
            dedicated: Some(MemoryEnvelope {
                hard_limit_bytes: 96 * GIB,
                source: "/sys/fs/cgroup/zerofs/memory.max".into(),
            }),
            shared_ceiling: Some(MemoryEnvelope {
                hard_limit_bytes: 80 * GIB,
                source: "/sys/fs/cgroup/memory.max".into(),
            }),
        };

        let error = select_memory_limit(limits.clone(), None).unwrap_err();
        assert!(format!("{error:#}").contains("shared ancestor ceiling"));

        let selected = select_memory_limit(limits, Some(64 * GIB))
            .unwrap()
            .unwrap();
        assert_eq!(selected.hard_limit_bytes, 64 * GIB);
        assert_eq!(selected.source, PathBuf::from("[runtime] memory_limit_gb"));
    }

    #[test]
    fn configured_budget_above_shared_ceiling_is_rejected_not_lowered() {
        let limits = CgroupMemoryLimits {
            dedicated: None,
            shared_ceiling: Some(MemoryEnvelope {
                hard_limit_bytes: 48 * GIB,
                source: "/sys/fs/cgroup/memory.max".into(),
            }),
        };

        let error = select_memory_limit(limits, Some(64 * GIB)).unwrap_err();
        let message = format!("{error:#}");
        assert!(message.contains("shared ancestor ceiling"), "{message}");
        assert!(!message.contains("accepted"), "{message}");
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
