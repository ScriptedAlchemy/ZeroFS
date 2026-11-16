use super::record::{Field, RawBytes, Reason, Xattr};
use rustix::fd::BorrowedFd;
use rustix::fs::{fgetxattr, flistxattr};
use rustix::io::Errno;
use std::ffi::OsStr;
use std::os::unix::ffi::OsStrExt;

// Size probes may race with xattr changes. Never interpret an ERANGE as an empty value.
pub(super) fn variable_bytes(
    mut read: impl FnMut(&mut [u8]) -> Result<usize, Errno>,
) -> Result<Vec<u8>, Errno> {
    for _ in 0..3 {
        let size = read(&mut [])?;
        let mut bytes = vec![0; size];
        match read(&mut bytes) {
            Ok(length) if length <= bytes.len() => {
                bytes.truncate(length);
                return Ok(bytes);
            }
            Ok(_) | Err(Errno::RANGE) => continue,
            Err(error) => return Err(error),
        }
    }
    Err(Errno::RANGE)
}

pub(super) fn xattr(fd: BorrowedFd<'_>, name: &OsStr) -> Field<Option<RawBytes>> {
    match variable_bytes(|buf| fgetxattr(fd, name, buf)) {
        Ok(bytes) => Field::Observed(Some(RawBytes::new(&bytes))),
        #[cfg(target_os = "linux")]
        Err(Errno::NODATA) => Field::Observed(None),
        #[cfg(target_os = "macos")]
        Err(Errno::NOATTR) => Field::Observed(None),
        Err(Errno::RANGE) => Field::unavailable(Reason::EntryChanged),
        Err(error) => Field::error(error),
    }
}

pub(super) fn xattrs(fd: BorrowedFd<'_>) -> Field<Vec<Xattr>> {
    let bytes = match variable_bytes(|buf| flistxattr(fd, buf)) {
        Ok(bytes) => bytes,
        Err(Errno::RANGE) => return Field::unavailable(Reason::EntryChanged),
        Err(error) => return Field::error(error),
    };
    let mut names = bytes
        .split(|b| *b == 0)
        .filter(|n| !n.is_empty())
        .collect::<Vec<_>>();
    names.sort_unstable();
    Field::Observed(
        names
            .into_iter()
            .map(|name| {
                let value = match xattr(fd, OsStr::from_bytes(name)) {
                    Field::Observed(Some(value)) => Field::Observed(value),
                    Field::Observed(None) => Field::unavailable(Reason::EntryChanged),
                    Field::Unavailable { reason, errno } => Field::Unavailable { reason, errno },
                };
                Xattr {
                    name: RawBytes::new(name),
                    value,
                }
            })
            .collect(),
    )
}
