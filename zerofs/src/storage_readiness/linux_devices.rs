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
    super::with_stable_mount(
        root,
        "storage root device changed before backing observation",
        "mount namespace changed during backing device observation",
        |mount| {
            if !matches!(mount.filesystem.as_str(), "ext4" | "xfs") {
                bail!(
                    "backing serials unavailable for filesystem {}",
                    mount.filesystem
                );
            }
            let path = PathBuf::from(super::block_device_path(mount.device));
            Ok(std::collections::HashSet::from([block_serial(
                &path, true,
            )?]))
        },
    )
}

#[cfg(test)]
#[path = "linux_device_tests.rs"]
mod tests;
