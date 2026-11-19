use super::{ErrorKind, File, Path, PathBuf, PhysicalRole, fs, io, require, topology};
use std::ffi::OsStr;
use std::os::fd::AsRawFd;
use std::os::unix::fs::{FileTypeExt, MetadataExt};

pub(super) struct TrialDisk {
    alias: &'static str,
    serial: &'static str,
    wwn: Option<&'static str>,
    pub(super) bytes: u64,
}

impl TrialDisk {
    // Pinned to the N5 Air inventory and direct-device preflight receipts.
    pub(super) fn for_role(role: PhysicalRole) -> &'static Self {
        match role {
            PhysicalRole::Nvme => &Self {
                alias: "nvme-CT1000P3SSD8_2318E6D18CE7",
                serial: "2318E6D18CE7",
                wwn: None,
                bytes: 1_000_204_886_016,
            },
            PhysicalRole::Hdd6 => &Self {
                alias: "ata-ST6000DM003-2CY186_WF203KQ8",
                serial: "WF203KQ8",
                wwn: Some("5000c500ac8aed0a"),
                bytes: 6_001_175_126_016,
            },
            PhysicalRole::Hdd18 => &Self {
                alias: "ata-WDC_WD180EMFZ-11AFXA0_3WH7H3BJ",
                serial: "3WH7H3BJ",
                wwn: Some("5000cca284d17ed1"),
                bytes: 18_000_207_937_536,
            },
        }
    }
}

const PROTECTED_SERIALS: [&str; 2] = ["ZL213G03", "29036200000000000001"];
const STORAGE_WWN: &str = "5000c500c33993cb";
// Linux PROC_PID_INIT_INO is shared across supported architectures.
const INITIAL_PID_NAMESPACE: u64 = 0xefff_fffc;

pub(super) struct Machine {
    _root: File,
    root: PathBuf,
}

#[derive(PartialEq, Eq)]
pub(super) struct Snapshot {
    pub(super) node: PathBuf,
    pub(super) node_identity: (u64, u64, u64),
    pub(super) diskseq: u64,
    sys: PathBuf,
}

impl Machine {
    pub(super) fn host() -> io::Result<Self> {
        topology::verify_fs(Path::new("/proc"), libc::PROC_SUPER_MAGIC as _)?;
        require(
            fs::metadata("/proc/1/ns/pid")?.ino() == INITIAL_PID_NAMESPACE
                && fs::metadata("/proc/self/ns/pid")?.ino() == INITIAL_PID_NAMESPACE,
            "physical claim requires visibility from the initial Linux PID namespace",
        )?;
        let root_dir = File::open("/proc/1/root")?;
        let root = PathBuf::from(format!("/proc/self/fd/{}", root_dir.as_raw_fd()));
        topology::verify_fs(&root.join("sys"), libc::SYSFS_MAGIC as _)?;
        require(
            root.join("run/udev/data").is_dir(),
            "physical claim requires the host udev database",
        )?;
        Ok(Self {
            _root: root_dir,
            root,
        })
    }

    pub(super) fn inspect(&self, disk: &TrialDisk) -> io::Result<Snapshot> {
        let root = &self.root;
        let by_id = root.join("dev/disk/by-id");
        let name = read_alias(&by_id.join(disk.alias))?;
        let node = root.join("dev").join(&name);
        let meta = fs::symlink_metadata(&node)?;
        require(
            meta.file_type().is_block_device(),
            "by-id target is not a block node",
        )?;
        let dev = device_number(meta.rdev());
        let by_dev = read_sys_link(&root.join("sys/dev/block").join(&dev), "../../")?;
        let by_name = read_sys_link(&root.join("sys/block").join(&name), "../")?;
        let sys = root.join("sys").join(&by_dev);
        require(
            by_dev == by_name
                && by_dev
                    .components()
                    .nth(1)
                    .is_some_and(|part| part.as_os_str().as_encoded_bytes().starts_with(b"pci"))
                && by_dev.file_name() == Some(OsStr::new(&name))
                && fs::read_to_string(sys.join("dev"))?.trim() == dev,
            "by-id node is not the expected whole physical disk",
        )?;
        let (serial, wwid) = kernel_identity(&sys)?;
        let udev = Udev::read(root, &dev)?;
        require(
            serial.as_deref() == Some(disk.serial),
            "kernel disk serial mismatch",
        )?;
        if let Some(udev_serial) = &udev.serial {
            require(udev_serial == disk.serial, "udev disk serial mismatch")?;
        }
        if let Some(wwn) = disk.wwn {
            require(
                by_dev
                    .components()
                    .any(|part| is_ata_component(part.as_os_str().as_encoded_bytes()))
                    && udev.wwn.as_deref().is_none_or(|value| value == wwn)
                    && wwid.as_deref().is_none_or(|value| value == wwn)
                    && (wwid.is_some()
                        || udev.wwn.is_some()
                        || matching_wwn_alias(&by_id, &name, wwn)?),
                "expected the designated ATA disk and matching WWN",
            )?;
        } else {
            require(
                fs::read_to_string(sys.join("device/model"))?.trim() == "CT1000P3SSD8"
                    && by_dev.components().any(|part| part.as_os_str() == "nvme")
                    && fs::read_to_string(sys.join("nsid"))?.trim() == "1",
                "expected the designated Crucial P3 namespace",
            )?;
        }
        require(
            !udev.foreign_signature,
            "udev records a foreign storage signature",
        )?;
        self.unique_identity(&by_id, &name, &by_dev, disk)?;
        let diskseq = sys_number(&sys, "diskseq")?;
        require(
            diskseq != 0
                && sys_number(&sys, "size")?.checked_mul(512) == Some(disk.bytes)
                && sys_number(&sys, "queue/logical_block_size")? == 512
                && sys_number(&sys, "queue/rotational")? == u64::from(disk.wwn.is_some())
                && sys_number(&sys, "ro")? == 0
                && sys_number(&sys, "removable")? == 0,
            "unexpected physical disk sequence, size, geometry or write access",
        )?;
        topology::inspect(root, &sys, &dev, meta.rdev())?;
        Ok(Snapshot {
            node,
            node_identity: (meta.dev(), meta.ino(), meta.rdev()),
            diskseq,
            sys: by_dev,
        })
    }

    fn unique_identity(
        &self,
        by_id: &Path,
        name: &str,
        sys: &Path,
        disk: &TrialDisk,
    ) -> io::Result<()> {
        for entry in fs::read_dir(by_id)? {
            let entry = entry?;
            let alias = entry.file_name();
            let Some(alias) = alias.to_str() else {
                continue;
            };
            let (designated, protected) = alias_roles(alias, disk);
            if designated || protected {
                let target = read_alias(&entry.path())?;
                // A designated partition alias is stale until udev removes it.
                require(
                    !designated || target == name,
                    "by-id directory has ambiguous physical identity",
                )?;
                if protected {
                    let meta = fs::symlink_metadata(self.root.join("dev").join(&target))?;
                    require(
                        meta.file_type().is_block_device(),
                        "protected alias does not name a block node",
                    )?;
                    let link = self
                        .root
                        .join("sys/dev/block")
                        .join(device_number(meta.rdev()));
                    let protected_disk_or_partition = read_sys_link(&link, "../../")?;
                    require(
                        !protected_disk_or_partition.starts_with(sys),
                        "physical claim intersects a protected disk alias",
                    )?;
                }
            }
        }
        for entry in fs::read_dir(self.root.join("sys/block"))? {
            let entry = entry?;
            let other = entry.path();
            if entry.file_name() == OsStr::new(name) {
                continue;
            }
            let (serial, wwid) = kernel_identity(&other)?;
            let dev = fs::read_to_string(other.join("dev"))?;
            let udev = Udev::optional(&self.root, dev.trim())?;
            require(
                serial.as_deref() != Some(disk.serial)
                    && udev.serial.as_deref() != Some(disk.serial)
                    && disk.wwn.is_none_or(|wwn| {
                        wwid.as_deref() != Some(wwn) && udev.wwn.as_deref() != Some(wwn)
                    }),
                "physical serial or WWN is claimed by multiple whole disks",
            )?;
        }
        Ok(())
    }
}

fn device_number(rdev: u64) -> String {
    format!("{}:{}", libc::major(rdev), libc::minor(rdev))
}

fn sys_number(sys: &Path, key: &str) -> io::Result<u64> {
    fs::read_to_string(sys.join(key))?
        .trim()
        .parse()
        .map_err(|_| io::Error::new(ErrorKind::InvalidData, format!("invalid sysfs {key}")))
}

fn is_ata_component(part: &[u8]) -> bool {
    part.strip_prefix(b"ata")
        .is_some_and(|number| !number.is_empty() && number.iter().all(u8::is_ascii_digit))
}

fn alias_roles(alias: &str, disk: &TrialDisk) -> (bool, bool) {
    let match_token = |value: &str| alias.split(['-', '_']).any(|token| token == value);
    let designated =
        match_token(disk.serial) || disk.wwn.is_some_and(|wwn| match_token(&format!("0x{wwn}")));
    let protected =
        PROTECTED_SERIALS.into_iter().any(match_token) || match_token(&format!("0x{STORAGE_WWN}"));
    (designated, protected)
}

fn matching_wwn_alias(by_id: &Path, name: &str, wwn: &str) -> io::Result<bool> {
    let link = by_id.join(format!("wwn-0x{wwn}"));
    Ok(optional_read(link, |link| read_alias(&link))?.as_deref() == Some(name))
}

fn read_sys_link(link: &Path, prefix: &str) -> io::Result<PathBuf> {
    let target = fs::read_link(link)?;
    let relative = target.strip_prefix(prefix).map_err(io::Error::other)?;
    require(
        relative.starts_with("devices")
            && relative
                .components()
                .all(|part| matches!(part, std::path::Component::Normal(_))),
        "invalid kernel block topology link",
    )?;
    Ok(relative.to_path_buf())
}

fn read_alias(link: &Path) -> io::Result<String> {
    alias_node(&fs::read_link(link)?)
}

fn alias_node(target: &Path) -> io::Result<String> {
    let name = target
        .strip_prefix("../../")
        .or_else(|_| target.strip_prefix("/dev/"))
        .ok()
        .and_then(Path::to_str)
        .filter(|name| {
            !name.is_empty()
                && name
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
        });
    name.map(str::to_owned).ok_or_else(|| {
        io::Error::new(
            ErrorKind::InvalidData,
            "by-id alias does not name a direct device node",
        )
    })
}

fn kernel_identity(sys: &Path) -> io::Result<(Option<String>, Option<String>)> {
    let serial = if let Some(serial) = optional_text(sys.join("device/serial"))? {
        Some(serial)
    } else {
        optional_read(sys.join("device/vpd_pg80"), fs::read::<PathBuf>)?
            .map(|page| vpd_serial(&page))
            .transpose()?
    };
    let wwid = optional_text(sys.join("device/wwid"))?
        .or(optional_text(sys.join("wwid"))?)
        .and_then(|wwid| wwid.strip_prefix("naa.").map(str::to_owned));
    Ok((serial, wwid))
}

fn vpd_serial(page: &[u8]) -> io::Result<String> {
    require(
        page.len() >= 4 && page[1] == 0x80,
        "invalid SCSI serial VPD page",
    )?;
    let len = usize::from(u16::from_be_bytes([page[2], page[3]]));
    require(
        page.len() == 4 + len && len > 0,
        "invalid SCSI serial VPD length",
    )?;
    let serial = std::str::from_utf8(&page[4..]).map_err(io::Error::other)?;
    Ok(serial.trim().to_owned())
}

fn optional_read<T>(
    path: PathBuf,
    read: impl FnOnce(PathBuf) -> io::Result<T>,
) -> io::Result<Option<T>> {
    match read(path) {
        Ok(value) => Ok(Some(value)),
        Err(error) if error.kind() == ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error),
    }
}

fn optional_text(path: PathBuf) -> io::Result<Option<String>> {
    Ok(optional_read(path, fs::read_to_string::<PathBuf>)?.map(|value| value.trim().to_owned()))
}

#[derive(Default)]
struct Udev {
    serial: Option<String>,
    wwn: Option<String>,
    foreign_signature: bool,
}

impl Udev {
    fn read(root: &Path, dev: &str) -> io::Result<Self> {
        Self::parse(&fs::read_to_string(
            root.join(format!("run/udev/data/b{dev}")),
        )?)
    }

    fn optional(root: &Path, dev: &str) -> io::Result<Self> {
        match Self::read(root, dev) {
            Ok(value) => Ok(value),
            Err(error) if error.kind() == ErrorKind::NotFound => Ok(Self::default()),
            Err(error) => Err(error),
        }
    }

    fn parse(text: &str) -> io::Result<Self> {
        let mut identity = Self::default();
        for line in text.lines() {
            if [
                "E:ID_FS_TYPE=",
                "E:ID_PART_TABLE_TYPE=",
                "E:ID_PART_ENTRY_TYPE=",
            ]
            .into_iter()
            .any(|key| {
                line.strip_prefix(key)
                    .is_some_and(|value| !value.is_empty())
            }) {
                identity.foreign_signature = true;
            }
            if let Some(serial) = line.strip_prefix("E:ID_SERIAL_SHORT=") {
                require(
                    identity.serial.replace(serial.to_owned()).is_none(),
                    "duplicate udev serial",
                )?;
            }
            if let Some(wwn) = line.strip_prefix("E:ID_WWN=") {
                let value = wwn
                    .strip_prefix("0x")
                    .ok_or_else(|| io::Error::new(ErrorKind::InvalidData, "invalid udev WWN"))?;
                require(
                    identity.wwn.replace(value.to_owned()).is_none(),
                    "duplicate udev WWN",
                )?;
            }
        }
        Ok(identity)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pins_trial_designations_and_protects_boot_and_storage() {
        let disk = TrialDisk::for_role(PhysicalRole::Hdd6);
        assert_eq!(disk.bytes, 6_001_175_126_016);
        assert_eq!(alias_roles(disk.alias, disk), (true, false));
        assert_eq!(
            alias_roles("wwn-0x5000c500ac8aed0a-part1", disk),
            (true, false)
        );
        assert_eq!(
            alias_roles("ata-ST6000DM003-2CY186_XWF203KQ8", disk),
            (false, false)
        );
        assert_eq!(alias_roles("wwn-0x5000c500c33993cb", disk), (false, true));
        assert_eq!(
            alias_roles("ata-ST16000NM001G-2KK103_ZL213G03-part1", disk),
            (false, true)
        );
        assert_eq!(
            alias_roles(
                "nvme-BAYHUB_Foresee-C9A611-0x0013-64GB_29036200000000000001",
                disk
            ),
            (false, true)
        );
        assert_eq!(PhysicalRole::Nvme.expected_bytes(), 1_000_204_886_016);
        assert_eq!(PhysicalRole::Hdd18.expected_bytes(), 18_000_207_937_536);
        assert!("2318E6D18CE7".parse::<PhysicalRole>().is_err());
        assert!("/dev/sda".parse::<PhysicalRole>().is_err());
    }

    #[test]
    fn aliases_must_point_to_a_single_device_node() {
        assert_eq!(alias_node(Path::new("../../sda")).unwrap(), "sda");
        assert_eq!(alias_node(Path::new("/dev/nvme1n1")).unwrap(), "nvme1n1");
        assert!(alias_node(Path::new("../../../sda")).is_err());
        assert!(alias_node(Path::new("/dev/disk/by-id/link")).is_err());
        assert!(alias_node(Path::new("/proc/self/fd/1")).is_err());
    }

    #[test]
    fn refuses_malformed_kernel_serial_and_conflicting_udev_identity() {
        assert_eq!(vpd_serial(b"\0\x80\0\x06  ABC ").unwrap(), "ABC");
        assert!(vpd_serial(b"\0\x80\0\x07  ABC ").is_err());
        assert!(vpd_serial(b"\0\x83\0\x06  ABC ").is_err());
        assert!(Udev::parse("E:ID_SERIAL_SHORT=A\nE:ID_SERIAL_SHORT=B").is_err());
        assert!(Udev::parse("E:ID_WWN=0x5000c500ac8aed0a\nE:ID_WWN=0x5000cca284d17ed1").is_err());
    }
}
