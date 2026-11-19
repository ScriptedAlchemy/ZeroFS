use super::{ErrorKind, File, Path, checked, fs, io, require};
use std::os::fd::AsRawFd;
use std::os::unix::fs::{FileTypeExt, MetadataExt};

pub(super) fn verify_fs(path: &Path, expected: i64) -> io::Result<()> {
    let directory = File::open(path)?;
    let mut fs_type = std::mem::MaybeUninit::<libc::statfs>::uninit();
    checked(unsafe { libc::fstatfs(directory.as_raw_fd(), fs_type.as_mut_ptr()) })?;
    // libc uses signed or unsigned 32/64-bit fields across Linux targets.
    let actual = i128::from(unsafe { fs_type.assume_init() }.f_type);
    require(
        actual == i128::from(expected),
        "physical claim requires kernel procfs and sysfs",
    )
}

pub(super) fn inspect(root: &Path, sys: &Path, dev: &str, rdev: u64) -> io::Result<()> {
    require(
        !sys.join("partition").try_exists()?
            && fs::read_dir(sys.join("holders"))?.next().is_none()
            && fs::read_dir(sys.join("slaves"))?.next().is_none(),
        "physical disk has holders or slaves, or is a partition",
    )?;
    for child in fs::read_dir(sys)? {
        let child = child?;
        require(
            !child.file_type()?.is_dir() || !child.path().join("partition").try_exists()?,
            "physical disk has partitions",
        )?;
    }
    // Linux's exclusive block open also arbitrates mounted or swapped devices
    // whose mount namespace is not represented by PID 1 or this process.
    for (view, root) in [
        ("/proc/1/mountinfo", root),
        ("/proc/self/mountinfo", Path::new("/")),
    ] {
        check_mounts(&fs::read_to_string(view)?, root, dev, rdev)?;
    }
    check_swaps(&fs::read_to_string("/proc/swaps")?, root, rdev)
}

fn check_mounts(text: &str, root: &Path, dev: &str, rdev: u64) -> io::Result<()> {
    require(!text.is_empty(), "mount inventory is empty")?;
    for line in text.lines() {
        let (left, right) = line.split_once(" - ").ok_or_else(malformed_inventory)?;
        let number = left
            .split_ascii_whitespace()
            .nth(2)
            .ok_or_else(malformed_inventory)?;
        require(number != dev, "physical disk is mounted")?;
        let source = right
            .split_ascii_whitespace()
            .nth(1)
            .ok_or_else(malformed_inventory)?;
        let source = decode_path(source)?;
        if source.starts_with("/dev/") {
            match fs::metadata(root.join(source.trim_start_matches('/'))) {
                Ok(meta) => require(
                    !meta.file_type().is_block_device() || meta.rdev() != rdev,
                    "physical disk is a mount source",
                )?,
                // Filesystems can keep mount-source names after udev unlinks them.
                // mountinfo dev_t and O_EXCL remain authoritative in that case.
                Err(error) if error.kind() == ErrorKind::NotFound => {}
                Err(error) => return Err(error),
            }
        }
    }
    Ok(())
}

fn check_swaps(text: &str, root: &Path, rdev: u64) -> io::Result<()> {
    let mut lines = text.lines();
    require(
        lines
            .next()
            .and_then(|line| line.split_ascii_whitespace().next())
            == Some("Filename"),
        "swap inventory is missing",
    )?;
    for line in lines {
        let mut fields = line.split_ascii_whitespace();
        let source = decode_path(fields.next().ok_or_else(malformed_inventory)?)?;
        let kind = fields.next().ok_or_else(malformed_inventory)?;
        require(source.starts_with('/'), "unresolved active swap path")?;
        let meta = fs::metadata(root.join(source.trim_start_matches('/')))?;
        let swap_device = match kind {
            "partition" if meta.file_type().is_block_device() => meta.rdev(),
            "file" if meta.is_file() => meta.dev(),
            _ => return Err(malformed_inventory()),
        };
        require(swap_device != rdev, "physical disk backs active swap")?;
    }
    Ok(())
}

fn malformed_inventory() -> io::Error {
    io::Error::new(
        ErrorKind::InvalidData,
        "invalid kernel mount or swap inventory",
    )
}

fn decode_path(value: &str) -> io::Result<String> {
    let mut decoded = Vec::with_capacity(value.len());
    let mut bytes = value.as_bytes().iter().copied();
    while let Some(byte) = bytes.next() {
        let byte = if byte == b'\\' {
            match (bytes.next(), bytes.next(), bytes.next()) {
                (Some(b'0'), Some(b'4'), Some(b'0')) => b' ',
                (Some(b'0'), Some(b'1'), Some(b'1')) => b'\t',
                (Some(b'0'), Some(b'1'), Some(b'2')) => b'\n',
                (Some(b'1'), Some(b'3'), Some(b'4')) => b'\\',
                _ => return Err(malformed_inventory()),
            }
        } else {
            byte
        };
        decoded.push(byte);
    }
    String::from_utf8(decoded).map_err(io::Error::other)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn refuses_mounted_disk_and_missing_mount_inventory() {
        let mount = "82 68 8:16 / /mnt rw - ext4 /dev/sdb rw";
        assert!(check_mounts(mount, Path::new("/"), "8:16", 0).is_err());
        assert!(check_mounts("", Path::new("/"), "8:16", 0).is_err());
        assert!(check_mounts("malformed", Path::new("/"), "8:16", 0).is_err());
        let unrelated = "82 68 8:32 / /mnt rw - tmpfs none rw";
        assert!(check_mounts(unrelated, Path::new("/"), "8:16", 0).is_ok());
    }

    #[test]
    fn refuses_missing_or_unresolvable_swap_inventory() {
        assert!(check_swaps("", Path::new("/"), 0).is_err());
        let header = "Filename Type Size Used Priority";
        assert!(check_swaps(header, Path::new("/"), 0).is_ok());
        let unknown = format!("{header}\nrelative partition 1 0 -2");
        assert!(check_swaps(&unknown, Path::new("/"), 0).is_err());
    }

    #[test]
    fn decodes_kernel_path_escapes() {
        assert_eq!(decode_path(r"/a\040b\011c\134d").unwrap(), "/a b\tc\\d");
        assert!(decode_path(r"/a\777").is_err());
    }
}
