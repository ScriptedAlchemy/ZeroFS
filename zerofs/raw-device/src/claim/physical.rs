use super::{Claim, ErrorKind, File, OpenOptions, Path, checked, fs, io, require};
use std::os::fd::AsRawFd;
use std::os::unix::fs::{FileTypeExt, MetadataExt, OpenOptionsExt};
use std::path::PathBuf;
use std::str::FromStr;

mod identity;
mod signatures;
mod topology;

use identity::{Machine, Snapshot, TrialDisk};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PhysicalRole {
    Nvme,
    Hdd6,
    Hdd18,
}

impl PhysicalRole {
    pub fn expected_bytes(self) -> u64 {
        TrialDisk::for_role(self).bytes
    }
}

impl FromStr for PhysicalRole {
    type Err = io::Error;

    fn from_str(role: &str) -> Result<Self, Self::Err> {
        match role {
            "nvme" => Ok(Self::Nvme),
            "hdd6" => Ok(Self::Hdd6),
            "hdd18" => Ok(Self::Hdd18),
            _ => Err(io::Error::new(
                ErrorKind::InvalidInput,
                "expected physical device role nvme, hdd6 or hdd18",
            )),
        }
    }
}

impl Claim {
    pub(super) fn verify_host_inventory_access() -> io::Result<()> {
        Machine::host().map(drop)
    }

    pub fn physical(role: PhysicalRole) -> io::Result<Self> {
        let host = Machine::host()?;
        let disk = TrialDisk::for_role(role);
        let before = match host.inspect(disk) {
            Err(error)
                if role != PhysicalRole::Nvme
                    && error.kind() == ErrorKind::NotFound
                    && host.confirm_absent(disk)? =>
            {
                return Err(super::device_absent());
            }
            result => result?,
        };
        let device = OpenOptions::new()
            .read(true)
            .write(true)
            .custom_flags(libc::O_EXCL | libc::O_NOFOLLOW | libc::O_NONBLOCK)
            .open(&before.node)?;
        let opened = device.metadata()?;
        require(
            opened.file_type().is_block_device()
                && (opened.dev(), opened.ino(), opened.rdev()) == before.node_identity,
            "physical device node changed during exclusive open",
        )?;
        before.verify_fd(&device, disk)?;
        let after = host.inspect(disk)?;
        require(
            before == after,
            "physical device identity changed during claim",
        )?;
        after.verify_fd(&device, disk)?;
        signatures::require_no_foreign_signatures(&device, disk.bytes)?;
        after.verify_fd(&device, disk)?;
        Ok(Self {
            device,
            size: disk.bytes,
            _backing_lock: None,
        })
    }
}

impl Snapshot {
    fn verify_fd(&self, device: &File, disk: &TrialDisk) -> io::Result<()> {
        let [rdev, sequence, _] = held_geometry(device, disk.bytes)?;
        require(
            rdev == self.node_identity.2 && sequence == self.diskseq,
            "held physical device sequence or identity changed",
        )
    }
}

pub(super) fn held_geometry(device: &File, expected_bytes: u64) -> io::Result<[u64; 3]> {
    let meta = device.metadata()?;
    require(
        meta.file_type().is_block_device(),
        "claim descriptor must be a block device",
    )?;
    // Linux BLKGETDISKSEQ, BLKGETSIZE64, BLKSSZGET and BLKROGET.
    let mut diskseq = 0u64;
    let mut bytes = 0u64;
    let mut logical_block_size = 0i32;
    let mut read_only = -1i32;
    checked(unsafe { libc::ioctl(device.as_raw_fd(), 0x8008_1280u32 as _, &mut diskseq) })?;
    #[cfg(target_pointer_width = "64")]
    checked(unsafe { libc::ioctl(device.as_raw_fd(), 0x8008_1272u32 as _, &mut bytes) })?;
    #[cfg(target_pointer_width = "32")]
    checked(unsafe { libc::ioctl(device.as_raw_fd(), 0x8004_1272u32 as _, &mut bytes) })?;
    checked(unsafe { libc::ioctl(device.as_raw_fd(), 0x1268, &mut logical_block_size) })?;
    checked(unsafe { libc::ioctl(device.as_raw_fd(), 0x125e, &mut read_only) })?;
    let flags = unsafe { libc::fcntl(device.as_raw_fd(), libc::F_GETFL) };
    checked(flags)?;
    require(
        diskseq > 0
            && bytes == expected_bytes
            && logical_block_size == 512
            && read_only == 0
            && flags & libc::O_ACCMODE == libc::O_RDWR,
        "held physical device has unexpected geometry or write access",
    )?;
    Ok([meta.rdev(), diskseq, bytes])
}
