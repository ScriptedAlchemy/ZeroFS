use super::super::identity::Serial;
#[cfg(target_os = "linux")]
use anyhow::Context;
use anyhow::{Result, bail};
#[cfg(target_os = "linux")]
use std::path::Path;
use std::path::PathBuf;

pub(super) fn parse_serial(input: &str) -> Result<Serial> {
    let value = input.trim();
    if value.contains(['\n', '\r', '\t']) {
        bail!("device serial unavailable because the adapter returned multiple values");
    }
    Serial::try_from(value.to_owned()).map_err(anyhow::Error::msg)
}

pub(super) fn zpool_device_paths(input: &str, pool: &str) -> Result<Vec<PathBuf>> {
    let mut rows = false;
    let mut saw_pool = false;
    let mut paths = Vec::new();
    for line in input.lines() {
        let fields = line.split_whitespace().collect::<Vec<_>>();
        if fields.starts_with(&["NAME", "STATE"]) {
            if rows {
                bail!("ambiguous ZFS device evidence");
            }
            rows = true;
            continue;
        }
        if !rows || fields.is_empty() {
            continue;
        }
        let name = fields[0];
        if name == "errors:" {
            break;
        }
        if name == pool && !saw_pool {
            saw_pool = true;
        } else if name.starts_with('/') {
            if fields.len() < 2 || fields[1] != "ONLINE" {
                bail!("ZFS backing device is not online");
            }
            paths.push(PathBuf::from(name));
        } else if !matches!(name, "logs" | "cache" | "spares" | "special" | "dedup")
            && !["mirror-", "raidz1-", "raidz2-", "raidz3-"]
                .iter()
                .any(|prefix| {
                    name.strip_prefix(prefix).is_some_and(|suffix| {
                        !suffix.is_empty() && suffix.bytes().all(|byte| byte.is_ascii_digit())
                    })
                })
        {
            bail!("ZFS backing device evidence unavailable for {name}");
        }
    }
    if !saw_pool || paths.is_empty() {
        bail!("ZFS backing device set unavailable");
    }
    Ok(paths)
}

#[cfg(target_os = "linux")]
fn block_serial(path: &Path, allow_partition: bool) -> Result<Serial> {
    use std::os::unix::fs::{FileTypeExt, MetadataExt};
    let metadata = std::fs::metadata(path)
        .with_context(|| format!("block device unavailable at {}", path.display()))?;
    if !metadata.file_type().is_block_device() {
        bail!("{} is not a block device", path.display());
    }
    let sysfs = PathBuf::from(format!(
        "/sys/dev/block/{}:{}",
        libc::major(metadata.rdev()),
        libc::minor(metadata.rdev())
    ));
    let observed_sysfs = sysfs
        .canonicalize()
        .context("device sysfs identity unavailable")?;
    let mut disk = observed_sysfs.clone();
    if disk.join("partition").try_exists()? {
        if !allow_partition {
            bail!("declared by-id must resolve to a whole block device");
        }
        disk = disk
            .parent()
            .context("partition parent unavailable")?
            .to_path_buf();
    }
    if std::fs::read_dir(disk.join("slaves"))?.next().is_some() {
        bail!("backing serials unavailable for mapped or stacked block devices");
    }
    let input = std::fs::read_to_string(disk.join("device/serial"))
        .context("device serial unavailable from sysfs")?;
    let serial = parse_serial(&input)?;
    if std::fs::metadata(path)?.rdev() != metadata.rdev() || sysfs.canonicalize()? != observed_sysfs
    {
        bail!("block device changed during serial observation");
    }
    Ok(serial)
}

#[cfg(target_os = "linux")]
pub(in crate::storage_readiness) fn observe_device(by_id: &Path) -> Result<Serial> {
    block_serial(by_id, false)
}

#[cfg(target_os = "linux")]
pub(in crate::storage_readiness) fn backing_serials(
    root: &Path,
) -> Result<std::collections::HashSet<Serial>> {
    use super::Probe;
    use std::os::unix::fs::MetadataExt;
    let before = std::fs::read_to_string("/proc/self/mountinfo")?;
    let mount = super::containing_mount(&before, root)?;
    let metadata = std::fs::metadata(root)?;
    if (libc::major(metadata.dev()), libc::minor(metadata.dev())) != mount.device {
        bail!("storage root device changed before backing observation");
    }
    let paths = match mount.filesystem.as_str() {
        "ext4" | "xfs" => vec![PathBuf::from(format!(
            "/dev/block/{}:{}",
            mount.device.0, mount.device.1
        ))],
        "zfs" => {
            let pool = mount
                .source
                .split('/')
                .next()
                .filter(|pool| !pool.is_empty() && !pool.starts_with('-'))
                .context("ZFS pool unavailable")?;
            let output = super::HostProbe.output("zpool", &["status", "-P", pool])?;
            zpool_device_paths(&output, pool)?
        }
        other => bail!("backing serials unavailable for filesystem {other}"),
    };
    let serials = paths
        .iter()
        .map(|path| block_serial(path, true))
        .collect::<Result<std::collections::HashSet<_>>>()?;
    if before != std::fs::read_to_string("/proc/self/mountinfo")?
        || metadata.dev() != std::fs::metadata(root)?.dev()
    {
        bail!("mount namespace changed during backing device observation");
    }
    Ok(serials)
}

#[cfg(test)]
#[path = "linux_device_tests.rs"]
mod tests;
