//! Linux-only exclusive claims for trial disks and synthetic qualification loops.
use std::fs::{self, File, OpenOptions};
use std::io::{self, ErrorKind};
use std::os::fd::AsRawFd;
use std::os::unix::fs::{FileTypeExt, MetadataExt, OpenOptionsExt};
use std::path::Path;

pub mod broker;
mod physical;
pub use physical::PhysicalRole;

#[repr(C)]
struct LoopInfo {
    device: u64,
    inode: u64,
    rdevice: u64,
    offset: u64,
    size_limit: u64,
    number: u32,
    encryption: u32,
    key_size: u32,
    flags: u32,
    filename: [u8; 64],
    crypt_name: [u8; 64],
    key: [u8; 32],
    init: [u64; 2],
}

fn require(condition: bool, reason: &str) -> io::Result<()> {
    if condition {
        Ok(())
    } else {
        Err(io::Error::new(ErrorKind::InvalidData, reason))
    }
}

fn checked(rc: i32) -> io::Result<()> {
    if rc < 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(())
    }
}

#[derive(Debug)]
struct DeviceAbsent;

impl std::fmt::Display for DeviceAbsent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("designated raw device is absent")
    }
}

impl std::error::Error for DeviceAbsent {}

fn device_absent() -> io::Error {
    io::Error::new(ErrorKind::NotFound, DeviceAbsent)
}

/// True only for an absence confirmed by the raw claim implementation.
pub fn is_device_absent(error: &io::Error) -> bool {
    error
        .get_ref()
        .is_some_and(|cause| cause.is::<DeviceAbsent>())
}

pub struct Claim {
    pub device: File,
    pub size: u64,
    _backing_lock: Option<File>,
}

#[path = "claim/allocated_fixture.rs"]
mod allocated_fixture;

impl Claim {
    pub fn synthetic(node: &Path, backing: &Path) -> io::Result<Self> {
        Self::loop_device(node, backing, false)
    }

    /// A separately selected, fully allocated ext4-backed software fixture.
    pub fn allocated_fixture(node: &Path, backing: &Path) -> io::Result<Self> {
        Self::loop_device(node, backing, true)
    }

    fn loop_device(node: &Path, backing: &Path, allocated: bool) -> io::Result<Self> {
        let backing_lock = OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
            .open(backing)?;
        let meta = backing_lock.metadata()?;
        require(
            meta.is_file()
                && meta.nlink() == 1
                && meta.mode() & 0o077 == 0
                && meta.uid() == unsafe { libc::geteuid() },
            "expected private, singly linked backing inode",
        )?;
        let backing_parent = backing
            .parent()
            .ok_or_else(|| io::Error::other("missing private backing directory"))?;
        let parent = fs::metadata(backing_parent)?;
        require(
            parent.is_dir() && parent.mode() & 0o077 == 0 && parent.uid() == meta.uid(),
            "unsafe backing directory",
        )?;
        let mut fs_type = std::mem::MaybeUninit::<libc::statfs>::uninit();
        checked(unsafe { libc::fstatfs(backing_lock.as_raw_fd(), fs_type.as_mut_ptr()) })?;
        let fs_type = unsafe { fs_type.assume_init() }.f_type;
        if allocated {
            require(
                fs_type == libc::EXT4_SUPER_MAGIC,
                "large fixture backing must be on ext4",
            )?;
        } else {
            require(
                fs_type == libc::TMPFS_MAGIC,
                "synthetic backing inode must be on tmpfs",
            )?;
        }
        // LOCK_EX | LOCK_NB serializes our own callers, including loop aliases.
        checked(unsafe { libc::flock(backing_lock.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) })?;

        let node_meta = fs::symlink_metadata(node).map_err(|error| {
            if error.kind() == ErrorKind::NotFound {
                device_absent()
            } else {
                error
            }
        })?;
        let major = libc::major(node_meta.rdev());
        let minor = libc::minor(node_meta.rdev());
        require(
            node_meta.file_type().is_block_device() && major == 7,
            "only Linux loop block devices are admitted",
        )?;
        let sys = fs::canonicalize(format!("/sys/dev/block/{major}:{minor}"))?;
        require(
            sys == Path::new(&format!("/sys/devices/virtual/block/loop{minor}")),
            "expected a whole loop device",
        )?;
        let device = OpenOptions::new()
            .read(true)
            .write(true)
            .custom_flags(libc::O_EXCL | libc::O_NOFOLLOW | libc::O_NONBLOCK)
            .open(node)?;
        let opened = device.metadata()?;
        require(
            opened.file_type().is_block_device() && opened.rdev() == node_meta.rdev(),
            "device node changed",
        )?;
        let mut size = 0u64;
        let mut info = std::mem::MaybeUninit::<LoopInfo>::zeroed();
        // Linux LOOP_GET_STATUS64 copies a fixed-size struct into the supplied buffer.
        checked(unsafe { libc::ioctl(device.as_raw_fd(), 0x4c05, info.as_mut_ptr()) })?;
        let info = unsafe { info.assume_init() };
        require(
            (info.device, info.inode) == (meta.dev(), meta.ino())
                && info.inode != 0
                && info.number == minor,
            "loop backing identity mismatch",
        )?;
        require(
            info.offset == 0 && info.size_limit == 0 && info.encryption == 0 && info.flags & 1 == 0,
            "expected a writable whole-device mapping",
        )?;
        #[cfg(target_pointer_width = "64")]
        checked(unsafe { libc::ioctl(device.as_raw_fd(), 0x80081272, &mut size) })?; // BLKGETSIZE64
        let valid_size = if allocated {
            matches!(size, 68_719_476_736 | 12_884_901_888 | 17_179_869_184)
        } else {
            (1 << 20..=256 << 20).contains(&size)
        };
        require(
            size == meta.len() && valid_size && size.is_multiple_of(4096),
            "unexpected qualification geometry",
        )?;
        require(
            fs::read_dir(sys.join("holders"))?.next().is_none(),
            "device has holders",
        )?;
        for child in fs::read_dir(&sys)? {
            let child = child?;
            require(
                !child.file_type()?.is_dir() || !child.path().join("partition").try_exists()?,
                "device has partitions",
            )?;
        }
        if allocated {
            allocated_fixture::require_allocated(&backing_lock, size)?;
        }
        let id = format!("{major}:{minor}");
        require(
            !fs::read_to_string("/proc/self/mountinfo")?
                .lines()
                .any(|line| line.split_whitespace().nth(2) == Some(&id)),
            "device is mounted",
        )?;
        Ok(Self {
            device,
            size,
            _backing_lock: Some(backing_lock),
        })
    }
}
