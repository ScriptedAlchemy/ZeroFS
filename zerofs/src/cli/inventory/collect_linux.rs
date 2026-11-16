use super::collect::xattr;
use super::record::{Field, Filesystem, RawAcl, Reason, Timestamp};
use rustix::fd::BorrowedFd;
use rustix::fs::{AtFlags, Stat, StatFs, StatxFlags, statx};
use std::ffi::OsStr;

pub(super) fn acl(fd: BorrowedFd<'_>) -> Field<Vec<RawAcl>> {
    Field::Observed(
        [
            ("linux-posix-access-xattr", "system.posix_acl_access"),
            ("linux-posix-default-xattr", "system.posix_acl_default"),
            ("nfs4-xdr-xattr", "system.nfs4_acl_xdr"),
        ]
        .into_iter()
        .map(|(format, name)| RawAcl {
            format,
            raw: xattr(fd, OsStr::new(name)).format_probe(),
            decoded: Field::unavailable(Reason::NotDecoded),
        })
        .collect(),
    )
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
    Field::unavailable(Reason::NotApplicableOnPlatform)
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
