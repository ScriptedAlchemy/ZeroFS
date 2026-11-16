use super::collect::{self, xattr};
use super::record::{Field, Filesystem, RawAcl, RawBytes, Reason, Timestamp, Xattr};
use rustix::fd::{AsRawFd, BorrowedFd};
use rustix::fs::{AtFlags, Stat, StatFs, StatxFlags, statx};
use std::ffi::OsStr;
use std::path::Path;

pub(super) fn acl(fd: BorrowedFd<'_>) -> Field<Vec<RawAcl>> {
    acl_probes(|name| xattr(fd, OsStr::new(name)))
}

fn acl_probes(mut probe: impl FnMut(&str) -> Field<Option<RawBytes>>) -> Field<Vec<RawAcl>> {
    Field::Observed(
        [
            ("linux-posix-access-xattr", "system.posix_acl_access"),
            ("linux-posix-default-xattr", "system.posix_acl_default"),
            ("nfs4-xdr-xattr", "system.nfs4_acl_xdr"),
        ]
        .into_iter()
        .map(|(format, name)| RawAcl {
            format,
            raw: probe(name).format_probe(),
            decoded: Field::unavailable(Reason::NotDecoded),
        })
        .collect(),
    )
}

pub(super) fn symlink_metadata(
    parent: BorrowedFd<'_>,
    name: &OsStr,
) -> (Field<Vec<Xattr>>, Field<Vec<RawAcl>>) {
    symlink_metadata_at(parent, name, Path::new("/proc/self/fd"))
}

fn symlink_metadata_at(
    parent: BorrowedFd<'_>,
    name: &OsStr,
    proc_fds: &Path,
) -> (Field<Vec<Xattr>>, Field<Vec<RawAcl>>) {
    let proc_parent = proc_fds.join(parent.as_raw_fd().to_string());
    let error = match (rustix::fs::fstat(parent), rustix::fs::stat(&proc_parent)) {
        (Ok(expected), Ok(actual))
            if expected.st_dev == actual.st_dev && expected.st_ino == actual.st_ino =>
        {
            None
        }
        (Err(error), _) | (_, Err(error)) => Some(Some(error.raw_os_error())),
        _ => Some(None),
    };
    if let Some(errno) = error {
        return (
            Field::Unavailable {
                reason: Reason::ProcFdUnavailable,
                errno,
            },
            Field::Unavailable {
                reason: Reason::ProcFdUnavailable,
                errno,
            },
        );
    }
    let path = proc_parent.join(name);
    let get = |name: &OsStr| {
        collect::xattr_result(collect::variable_bytes(|buf| {
            rustix::fs::lgetxattr(&path, name, buf)
        }))
    };
    let xattrs = collect::xattrs_result(
        collect::variable_bytes(|buf| rustix::fs::llistxattr(&path, buf)),
        get,
    );
    let acl = acl_probes(|name| {
        if name == "system.nfs4_acl_xdr" {
            get(OsStr::new(name))
        } else {
            Field::unavailable(Reason::NotApplicableOnPlatform)
        }
    });
    (xattrs, acl)
}

pub(super) fn birth(parent: BorrowedFd<'_>, name: &OsStr, stat: &Stat) -> Field<Timestamp> {
    match statx(
        parent,
        name,
        AtFlags::SYMLINK_NOFOLLOW | AtFlags::NO_AUTOMOUNT,
        StatxFlags::BTIME | StatxFlags::INO,
    ) {
        Ok(s)
            if s.stx_ino != stat.st_ino
                || rustix::fs::makedev(s.stx_dev_major, s.stx_dev_minor) != stat.st_dev =>
        {
            Field::unavailable(Reason::EntryChanged)
        }
        Ok(s) if s.stx_mask & StatxFlags::BTIME.bits() != 0 => Field::Observed(Timestamp {
            seconds: s.stx_btime.tv_sec,
            nanoseconds: s.stx_btime.tv_nsec,
        }),
        Ok(_) => Field::unavailable(Reason::NotSupportedByFilesystem),
        Err(error) => Field::error(error),
    }
}

pub(super) fn flags(_stat: &Stat) -> Field<u32> {
    Field::unavailable(Reason::NotCollectedForKind)
}

pub(super) fn fd_flags(fd: BorrowedFd<'_>) -> Field<u32> {
    match rustix::fs::ioctl_getflags(fd) {
        Ok(flags) => Field::Observed(flags.bits()),
        Err(error @ (rustix::io::Errno::NOTTY | rustix::io::Errno::OPNOTSUPP)) => {
            Field::Unavailable {
                reason: Reason::FormatNotSupportedByFilesystem,
                errno: Some(error.raw_os_error()),
            }
        }
        Err(error) => Field::error(error),
    }
}

pub(super) fn filesystem(stat: &StatFs) -> Filesystem {
    let magic = stat.f_type as u64;
    let name = match magic {
        0x6969 => Some("nfs"),
        0xff534d42 => Some("cifs"),
        0xfe534d42 => Some("smb2"),
        0x73757245 => Some("coda"),
        0x5346414f => Some("afs"),
        0x01021997 => Some("9p"),
        0x65735546 => Some("fuse"),
        0xef53 => Some("ext"),
        0x2fc12fc1 => Some("zfs"),
        0x9123683e => Some("btrfs"),
        0x58465342 => Some("xfs"),
        _ => None,
    };
    Filesystem {
        type_name: name.map(str::to_owned),
        type_magic: magic,
        network: matches!(
            magic,
            0x6969 | 0xff534d42 | 0xfe534d42 | 0x73757245 | 0x5346414f | 0x01021997
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rustix::fd::AsFd;

    #[test]
    fn unsupported_inode_flags_are_complete() {
        let (socket, _) = std::os::unix::net::UnixStream::pair().unwrap();
        let flags = fd_flags(socket.as_fd());
        assert_eq!(
            flags,
            Field::Unavailable {
                reason: Reason::FormatNotSupportedByFilesystem,
                errno: Some(rustix::io::Errno::NOTTY.raw_os_error()),
            }
        );
        assert!(flags.is_complete());
    }

    #[test]
    fn unavailable_proc_anchor_is_incomplete() {
        let parent = rustix::fs::open(
            "/tmp",
            rustix::fs::OFlags::RDONLY | rustix::fs::OFlags::DIRECTORY,
            rustix::fs::Mode::empty(),
        )
        .unwrap();
        let (xattrs, acl) = symlink_metadata_at(
            parent.as_fd(),
            OsStr::new("link"),
            Path::new("/proc/self/missing-fd-directory"),
        );
        assert!(matches!(
            xattrs,
            Field::Unavailable {
                reason: Reason::ProcFdUnavailable,
                ..
            }
        ));
        assert!(!xattrs.is_complete());
        assert!(!acl.is_complete());
    }
}
