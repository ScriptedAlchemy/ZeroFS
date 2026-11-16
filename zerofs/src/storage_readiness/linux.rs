//! Read-only Linux mount/identity adapter. Parser and probe fixtures also run
//! on macOS; they do not establish Linux or TrueNAS runtime acceptance.

use super::{FilesystemIdentity, MountedFilesystem};
use anyhow::{Context, Result, bail};
use std::collections::BTreeMap;
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

pub(super) fn identify(mount: Mount, probe: &dyn Probe) -> Result<MountedFilesystem> {
    let (identity, unlocked, writable) = match mount.filesystem.as_str() {
        "ext4" | "xfs" => {
            // Probe the actual mounted device, not an operator-supplied UUID
            // symlink or a possibly stale blkid cache.
            let device = format!("/dev/block/{}:{}", mount.device.0, mount.device.1);
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
        "zfs" => {
            let pool = mount
                .source
                .split('/')
                .next()
                .context("missing ZFS pool name")?;
            if pool.is_empty() || mount.source.starts_with('-') || mount.source.contains('@') {
                bail!("invalid mounted ZFS dataset name");
            }
            let output = probe.output(
                "zfs",
                &[
                    "get",
                    "-H",
                    "-p",
                    "-o",
                    "property,value",
                    "guid,mounted,mountpoint,keystatus,readonly",
                    &mount.source,
                ],
            )?;
            let mut properties = BTreeMap::new();
            for line in output.lines() {
                let (key, value) = line.split_once('\t').context("invalid ZFS property row")?;
                if properties.insert(key, value).is_some() {
                    bail!("duplicate ZFS property")
                }
            }
            let get = |key| {
                properties
                    .get(key)
                    .copied()
                    .with_context(|| format!("missing ZFS {key} evidence"))
            };
            if properties.len() != 5
                || get("mounted")? != "yes"
                || Path::new(get("mountpoint")?) != mount.mountpoint
            {
                bail!("ZFS dataset is not mounted at the observed root")
            }
            let unlocked = match get("keystatus")? {
                "available" | "none" => true,
                "unavailable" => false,
                _ => bail!("unknown ZFS unlock state"),
            };
            let writable = match get("readonly")? {
                "off" => mount.writable,
                "on" => false,
                _ => bail!("unknown ZFS readonly state"),
            };
            let dataset_guid = get("guid")?.parse::<u64>()?;
            let pool_output =
                probe.output("zpool", &["get", "-H", "-p", "-o", "value", "guid", pool])?;
            let pool_guid = scalar(&pool_output)?.parse::<u64>()?;
            if dataset_guid == 0 || pool_guid == 0 {
                bail!("missing stable ZFS GUID")
            }
            (
                FilesystemIdentity::Zfs {
                    pool_guid: pool_guid.to_string(),
                    dataset_guid: dataset_guid.to_string(),
                },
                unlocked,
                writable,
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
pub(super) fn observe(path: &Path) -> Result<MountedFilesystem> {
    use std::os::unix::fs::{FileTypeExt, MetadataExt};
    let before = std::fs::read_to_string("/proc/self/mountinfo")?;
    let mount = containing_mount(&before, path)?;
    let metadata = std::fs::metadata(path)?;
    let actual = (libc::major(metadata.dev()), libc::minor(metadata.dev()));
    if actual != mount.device {
        bail!("storage root device changed while checking mountinfo")
    }
    if matches!(mount.filesystem.as_str(), "ext4" | "xfs") {
        let device = std::fs::metadata(format!("/dev/block/{}:{}", actual.0, actual.1))?;
        if !device.file_type().is_block_device()
            || (libc::major(device.rdev()), libc::minor(device.rdev())) != actual
        {
            bail!("mounted block device is unavailable or changed")
        }
    }
    let facts = identify(mount, &HostProbe)?;
    let after = std::fs::read_to_string("/proc/self/mountinfo")?;
    if before != after || std::fs::metadata(path)?.dev() != metadata.dev() {
        bail!("mount namespace changed during storage readiness verification");
    }
    Ok(facts)
}

#[cfg(test)]
#[path = "linux_tests.rs"]
mod tests;
