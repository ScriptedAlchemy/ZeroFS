#[path = "linux_devices.rs"]
pub(super) mod devices;

use super::{FilesystemIdentity, MountedFilesystem};
use anyhow::{Context, Result, bail};
use std::path::{Path, PathBuf};

#[derive(Debug)]
pub(super) struct Mount {
    pub(super) device: (u32, u32),
    root: PathBuf,
    mountpoint: PathBuf,
    filesystem: String,
    source: String,
    writable: bool,
}

fn unescape(value: &str) -> Result<String> {
    let mut result = String::new();
    let mut remaining = value;
    while let Some(index) = remaining.find('\\') {
        result.push_str(&remaining[..index]);
        let suffix = &remaining[index..];
        let escaped = match suffix.get(..4) {
            Some("\\040") => ' ',
            Some("\\011") => '\t',
            Some("\\012") => '\n',
            Some("\\134") => '\\',
            _ => bail!("invalid mountinfo escape"),
        };
        result.push(escaped);
        remaining = &suffix[4..];
    }
    result.push_str(remaining);
    Ok(result)
}

pub(super) fn containing_mount(input: &str, path: &Path) -> Result<Mount> {
    let mut found: Option<Mount> = None;
    for line in input.lines() {
        let (left, right) = line
            .split_once(" - ")
            .context("invalid mountinfo separator")?;
        let fields = left.split_whitespace().collect::<Vec<_>>();
        let tail = right.split_whitespace().collect::<Vec<_>>();
        if fields.len() < 6 || tail.len() != 3 {
            bail!("invalid mountinfo fields")
        }
        let mountpoint = PathBuf::from(unescape(fields[4])?);
        if !mountpoint.is_absolute() {
            bail!("mountinfo mountpoint must be absolute")
        }
        if mountpoint != path && mountpoint.starts_with(path) {
            bail!(
                "descendant mount below storage root {} at {}",
                path.display(),
                mountpoint.display()
            );
        }
        if !path.starts_with(&mountpoint) {
            continue;
        }
        if let Some(previous) = &found {
            if previous.mountpoint == mountpoint {
                bail!("ambiguous overmounted storage root")
            }
            if previous.mountpoint.components().count() > mountpoint.components().count() {
                continue;
            }
        }
        let (major, minor) = fields[2]
            .split_once(':')
            .context("invalid mountinfo device")?;
        let writable = |options: &str| {
            options.split(',').any(|part| part == "rw")
                && !options.split(',').any(|part| part == "ro")
        };
        found = Some(Mount {
            device: (major.parse()?, minor.parse()?),
            root: PathBuf::from(unescape(fields[3])?),
            mountpoint,
            filesystem: tail[0].to_owned(),
            source: unescape(tail[1])?,
            writable: writable(fields[5]) && writable(tail[2]),
        });
    }
    found.context("no mounted filesystem contains storage root")
}

pub(super) trait Probe {
    fn output(&self, program: &str, args: &[&str]) -> Result<String>;
}

#[cfg(target_os = "linux")]
struct HostProbe;

#[cfg(target_os = "linux")]
impl Probe for HostProbe {
    fn output(&self, program: &str, args: &[&str]) -> Result<String> {
        let output = std::process::Command::new(program)
            .args(args)
            .env("LC_ALL", "C")
            .output()
            .with_context(|| format!("cannot run read-only readiness probe {program}"))?;
        if !output.status.success() {
            bail!(
                "read-only readiness probe {program} failed: {}",
                String::from_utf8_lossy(&output.stderr).trim()
            );
        }
        String::from_utf8(output.stdout).context("readiness probe returned non-UTF-8 output")
    }
}

fn scalar(output: &str) -> Result<&str> {
    let value = output.trim();
    if value.is_empty() || value.contains(['\n', '\r', '\t']) {
        bail!("readiness probe must return one nonempty value");
    }
    Ok(value)
}

fn block_device_path(device: (u32, u32)) -> String {
    format!("/dev/block/{}:{}", device.0, device.1)
}

pub(super) fn identify(mount: Mount, probe: &dyn Probe) -> Result<MountedFilesystem> {
    let (identity, unlocked, writable) = match mount.filesystem.as_str() {
        "ext4" | "xfs" => {
            // Probe the actual mounted device, not an operator-supplied UUID
            // symlink or a possibly stale blkid cache.
            let device = block_device_path(mount.device);
            let output = probe.output("blkid", &["-p", "-s", "UUID", "-o", "value", &device])?;
            let uuid = uuid::Uuid::parse_str(scalar(&output)?)?;
            if uuid.is_nil() {
                bail!("mounted filesystem has no stable UUID")
            }
            (
                FilesystemIdentity::Uuid {
                    uuid: uuid.hyphenated().to_string(),
                },
                true,
                mount.writable,
            )
        }
        other => bail!("strict storage readiness has no identity adapter for filesystem {other}"),
    };
    Ok(MountedFilesystem {
        mountpoint: mount.mountpoint,
        filesystem_root: mount.root,
        identity,
        writable,
        unlocked,
    })
}

#[cfg(target_os = "linux")]
fn with_stable_mount<T>(
    root: &Path,
    device_changed: &'static str,
    namespace_changed: &'static str,
    body: impl FnOnce(Mount) -> Result<T>,
) -> Result<T> {
    use std::os::unix::fs::MetadataExt;
    let before = std::fs::read_to_string("/proc/self/mountinfo")?;
    let mount = containing_mount(&before, root)?;
    let metadata = std::fs::metadata(root)?;
    if (libc::major(metadata.dev()), libc::minor(metadata.dev())) != mount.device {
        bail!("{device_changed}");
    }
    let value = body(mount)?;
    if before != std::fs::read_to_string("/proc/self/mountinfo")?
        || metadata.dev() != std::fs::metadata(root)?.dev()
    {
        bail!("{namespace_changed}");
    }
    Ok(value)
}

#[cfg(target_os = "linux")]
pub(super) fn observe(path: &Path) -> Result<MountedFilesystem> {
    use std::os::unix::fs::{FileTypeExt, MetadataExt};
    with_stable_mount(
        path,
        "storage root device changed while checking mountinfo",
        "mount namespace changed during storage readiness verification",
        |mount| {
            if matches!(mount.filesystem.as_str(), "ext4" | "xfs") {
                let device = std::fs::metadata(block_device_path(mount.device))?;
                if !device.file_type().is_block_device()
                    || (libc::major(device.rdev()), libc::minor(device.rdev())) != mount.device
                {
                    bail!("mounted block device is unavailable or changed");
                }
            }
            identify(mount, &HostProbe)
        },
    )
}

#[cfg(test)]
#[path = "linux_tests.rs"]
mod tests;
