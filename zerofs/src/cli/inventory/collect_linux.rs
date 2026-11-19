use super::collect::{self, xattr};
use super::record::{
    EntryKind, Field, Filesystem, RawAcl, RawBytes, Reason, Timestamp, TrustedXattrNamespace, Xattr,
};
use rustix::fd::{AsRawFd, BorrowedFd};
use rustix::fs::{AtFlags, Stat, StatFs, StatxFlags, statx};
use std::ffi::OsStr;
use std::path::Path;

pub(super) fn proc_fd_path(file: &impl AsRawFd) -> std::path::PathBuf {
    format!("/proc/self/fd/{}", file.as_raw_fd()).into()
}

/// Map the source and scratch directories to paths within their backing filesystems.
/// A bind mount can give a descendant of the source an unrelated namespace path.
pub(super) fn open_scratch_parent(root: &Path, parent: &Path) -> anyhow::Result<std::fs::File> {
    use crate::memory::cgroup::unescape_mountinfo;
    use anyhow::{Context, ensure};
    use rustix::fs::{Mode, OFlags};
    use std::os::fd::AsFd;

    let directory = |path: &Path| -> std::io::Result<std::fs::File> {
        Ok(rustix::fs::open(
            path,
            OFlags::PATH | OFlags::DIRECTORY | OFlags::CLOEXEC,
            Mode::empty(),
        )?
        .into())
    };
    let root = directory(root)?;
    let parent = directory(parent)?;
    let mountinfo = std::fs::read("/proc/self/mountinfo")?;
    let mut mounts = Vec::new();
    for line in mountinfo
        .split(|b| *b == b'\n')
        .filter(|line| !line.is_empty())
    {
        let mut fields = line.split(|b| *b == b' ');
        let id = fields.next().context("missing mount ID")?;
        let device = fields.nth(1).context("missing mount device")?;
        let fs_root = unescape_mountinfo(fields.next().context("missing mount root")?)?;
        let mountpoint = unescape_mountinfo(fields.next().context("missing mount point")?)?;
        mounts.push((
            id,
            device,
            std::path::PathBuf::from(fs_root),
            std::path::PathBuf::from(mountpoint),
        ));
    }
    let location = |fd: &std::fs::File| -> anyhow::Result<_> {
        let mount = statx(fd.as_fd(), "", AtFlags::EMPTY_PATH, StatxFlags::MNT_ID)?;
        ensure!(
            mount.stx_mask & StatxFlags::MNT_ID.bits() != 0,
            "mount ID unavailable for inventory scratch check"
        );
        let id = mount.stx_mnt_id.to_string();
        let (_, device, fs_root, mountpoint) = mounts
            .iter()
            .find(|m| m.0 == id.as_bytes())
            .context("inventory directory mount is absent from mountinfo")?;
        let name = std::fs::read_link(proc_fd_path(fd))?;
        Ok((*device, fs_root.join(name.strip_prefix(mountpoint)?), name))
    };
    let (root_device, root_path, root_name) = location(&root)?;
    let (parent_device, parent_path, parent_name) = location(&parent)?;
    let inside_root = parent_device == root_device && parent_path.starts_with(root_path);
    let inside_child_mount = mounts.iter().any(|(_, device, fs_root, mountpoint)| {
        *device == parent_device
            && mountpoint.starts_with(&root_name)
            && parent_path.starts_with(fs_root)
    });
    ensure!(
        !parent_name.starts_with(&root_name) && !inside_root && !inside_child_mount,
        "--work-dir must be outside the source root, including through bind mounts"
    );
    Ok(parent)
}

pub(super) fn trusted_xattr_namespace() -> TrustedXattrNamespace {
    let status = std::fs::read_to_string("/proc/self/status").ok();
    let namespace = std::fs::read_link("/proc/self/ns/user").ok();
    TrustedXattrNamespace {
        visible: Some(trusted_visible(status.as_deref(), namespace.as_deref())),
        probe: "Effective CAP_SYS_ADMIN, bit 21 of /proc/self/status CapEff, in the initial user namespace.",
        user_namespace_policy: "Only Linux's reserved initial user namespace ID 4026531837 is accepted. Child or unrecognized namespaces and failed proc reads count as not visible.",
    }
}

fn trusted_visible(status: Option<&str>, namespace: Option<&Path>) -> bool {
    // Linux reserves PROC_USER_INIT_INO for the initial user namespace.
    if namespace != Some(Path::new("user:[4026531837]")) {
        return false;
    }
    status
        .and_then(|s| s.lines().find_map(|line| line.strip_prefix("CapEff:")))
        .and_then(|bits| u64::from_str_radix(bits.trim(), 16).ok())
        .is_some_and(|bits| bits & (1 << 21) != 0)
}

pub(super) fn mount_id(parent: BorrowedFd<'_>, name: &OsStr, stat: &Stat) -> Field<u64> {
    mount_id_result(
        statx(
            parent,
            name,
            AtFlags::SYMLINK_NOFOLLOW | AtFlags::NO_AUTOMOUNT | AtFlags::EMPTY_PATH,
            StatxFlags::MNT_ID | StatxFlags::INO,
        ),
        stat,
    )
}

fn mount_id_result(
    result: Result<rustix::fs::Statx, rustix::io::Errno>,
    stat: &Stat,
) -> Field<u64> {
    match result {
        Ok(s) if s.stx_mask & StatxFlags::MNT_ID.bits() == 0 => {
            Field::unavailable(Reason::MountIdUnavailable)
        }
        Ok(s)
            if s.stx_ino != stat.st_ino
                || rustix::fs::makedev(s.stx_dev_major, s.stx_dev_minor) != stat.st_dev =>
        {
            Field::unavailable(Reason::EntryChanged)
        }
        Ok(s) => Field::Observed(s.stx_mnt_id),
        Err(error) => Field::Unavailable {
            reason: Reason::MountIdUnavailable,
            errno: Some(error.raw_os_error()),
        },
    }
}

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
            posix_translation: None,
        })
        .collect(),
    )
}

pub(super) fn path_metadata(
    parent: BorrowedFd<'_>,
    name: &OsStr,
    kind: EntryKind,
) -> (Field<Vec<Xattr>>, Field<Vec<RawAcl>>) {
    path_metadata_at(parent, name, kind, Path::new("/proc/self/fd"))
}

fn path_metadata_at(
    parent: BorrowedFd<'_>,
    name: &OsStr,
    kind: EntryKind,
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
        if kind != EntryKind::Symlink || name == "system.nfs4_acl_xdr" {
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
    fn trusted_visibility_requires_initial_namespace_and_effective_capability() {
        let initial = Some(Path::new("user:[4026531837]"));
        assert!(trusted_visible(
            Some("CapEff:\t0000000000200000\n"),
            initial
        ));
        assert!(!trusted_visible(
            Some("CapEff:\t0000000000000000\n"),
            initial
        ));
        assert!(!trusted_visible(
            Some("CapEff:\t0000000000200000\n"),
            Some(Path::new("user:[1234]"))
        ));
        assert!(!trusted_visible(None, initial));
        assert!(!trusted_visible(Some("CapEff:\tbad input\n"), initial));
        assert!(!trusted_visible(Some("CapEff:\t0000000000200000\n"), None));
    }

    #[test]
    fn missing_mount_id_is_incomplete() {
        let fd = rustix::fs::open(
            "/tmp",
            rustix::fs::OFlags::RDONLY | rustix::fs::OFlags::DIRECTORY,
            rustix::fs::Mode::empty(),
        )
        .unwrap();
        let stat = rustix::fs::fstat(&fd).unwrap();
        let mut observed = statx(
            &fd,
            "",
            AtFlags::EMPTY_PATH,
            StatxFlags::INO | StatxFlags::MNT_ID,
        )
        .unwrap();
        observed.stx_mask &= !StatxFlags::MNT_ID.bits();
        let missing = mount_id_result(Ok(observed), &stat);
        assert_eq!(missing, Field::unavailable(Reason::MountIdUnavailable));
        assert!(!missing.is_complete());
        let unsupported = mount_id_result(Err(rustix::io::Errno::NOSYS), &stat);
        assert!(!unsupported.is_complete());
        assert_eq!(
            serde_json::to_value(unsupported).unwrap()["errno"],
            libc::ENOSYS
        );
    }

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
        let (xattrs, acl) = path_metadata_at(
            parent.as_fd(),
            OsStr::new("link"),
            EntryKind::Symlink,
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
