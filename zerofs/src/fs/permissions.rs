pub(crate) mod acl;
pub(crate) mod nfs4;

use super::errors::FsError;
use super::inode::{DirectoryInode, Inode, InodeAttrs};
use super::types::{AuthContext, SetGid};

const S_IRUSR: u32 = 0o400;
const S_IWUSR: u32 = 0o200;
const S_IXUSR: u32 = 0o100;
const S_IRGRP: u32 = 0o040;
const S_IWGRP: u32 = 0o020;
const S_IXGRP: u32 = 0o010;
const S_IROTH: u32 = 0o004;
const S_IWOTH: u32 = 0o002;
const S_IXOTH: u32 = 0o001;
const S_ISVTX: u32 = 0o1000;

#[derive(Debug, Clone, Copy)]
pub struct Credentials {
    pub uid: u32,
    pub gid: u32,
    /// Whether `gid` came from the caller rather than a protocol fallback.
    pub gid_known: bool,
    pub groups: [u32; 16],
    pub groups_count: usize,
    /// False when an upstream kernel already performed group DAC but could not
    /// provide the complete supplementary-group list.
    pub groups_complete: bool,
}

impl Credentials {
    pub(crate) fn from_auth_context(auth: &AuthContext) -> Self {
        let mut creds = Self {
            uid: auth.uid,
            gid: auth.gid,
            gid_known: auth.gid_known,
            groups: [0; 16],
            groups_count: auth.gids.len().min(16),
            groups_complete: auth.groups_complete,
        };

        for (i, gid) in auth.gids.iter().take(16).enumerate() {
            creds.groups[i] = *gid;
        }

        creds
    }

    pub(crate) fn with_gid(self, gid: u32) -> Self {
        Self { gid, ..self }
    }

    pub(crate) fn is_member_of_group(&self, gid: u32) -> bool {
        self.is_known_member_of_group(gid) || !self.groups_complete
    }

    fn is_known_member_of_group(&self, gid: u32) -> bool {
        if self.gid_known && self.gid == gid {
            return true;
        }
        self.groups[..self.groups_count].contains(&gid)
    }
}

/// Group for a new entry. An explicit group wins. Otherwise a setgid parent
/// directory passes on its group, and any other parent leaves the caller's.
pub(crate) fn new_entry_gid(
    parent: &DirectoryInode,
    requested: &SetGid,
    creds: &Credentials,
) -> u32 {
    match requested {
        SetGid::Set(gid) => *gid,
        SetGid::NoChange if parent.mode & 0o2000 != 0 => parent.gid,
        SetGid::NoChange => creds.gid,
    }
}

/// Clears the setgid bit on a new non-directory entry unless the caller is
/// root or a member of the entry's group. An inherited group follows Linux
/// `mode_strip_sgid`, which keeps the bit when group execute is clear.
pub(crate) fn strip_sgid_unless_member(
    parent: &DirectoryInode,
    mode: u32,
    gid: u32,
    creds: &Credentials,
) -> u32 {
    let inherited = parent.mode & 0o2000 != 0 && gid == parent.gid;
    if inherited && mode & 0o010 == 0 || creds.uid == 0 || creds.is_member_of_group(gid) {
        mode
    } else {
        mode & !0o2000
    }
}

#[derive(Debug, Clone, Copy)]
pub enum AccessMode {
    Read,
    Write,
    Execute,
}

#[cfg(test)]
/// Mode-bit check for an inode the caller has not looked up an ACL for.
/// Paths that can reach an inode carrying a POSIX access ACL must use
/// `ZeroFS::check_inode_access` instead.
pub(crate) fn check_access(
    inode: &Inode,
    creds: &Credentials,
    mode: AccessMode,
) -> Result<(), FsError> {
    check_access_with_acl(inode, creds, mode, None)
}

/// Linux `generic_permission` with an optional access ACL. `acl` is the
/// stored ACL; owner, mask and other permissions come from the inode mode.
pub(crate) fn check_access_with_acl(
    inode: &Inode,
    creds: &Credentials,
    mode: AccessMode,
    acl: Option<&acl::PosixAcl>,
) -> Result<(), FsError> {
    let (uid, gid, file_mode) = (inode.uid(), inode.gid(), inode.mode());

    if creds.uid == 0 {
        // Root can search a directory even when none of its mode bits grant
        // search access. The execute-bit requirement applies to files only.
        if let AccessMode::Execute = mode
            && !inode.is_directory()
            && file_mode & 0o111 == 0
        {
            return Err(FsError::PermissionDenied);
        }
        return Ok(());
    }

    if let Some(acl) = acl
        && creds.uid != uid
    {
        return if acl.with_mode(file_mode).permits(uid, gid, creds, mode) {
            Ok(())
        } else {
            Err(FsError::PermissionDenied)
        };
    }

    let permission_bits = match mode {
        AccessMode::Read => (S_IRUSR, S_IRGRP, S_IROTH),
        AccessMode::Write => (S_IWUSR, S_IWGRP, S_IWOTH),
        AccessMode::Execute => (S_IXUSR, S_IXGRP, S_IXOTH),
    };

    if creds.uid == uid {
        if file_mode & permission_bits.0 != 0 {
            return Ok(());
        }
    } else if creds.is_known_member_of_group(gid) {
        if file_mode & permission_bits.1 != 0 {
            return Ok(());
        }
    } else if !creds.groups_complete {
        if file_mode & (permission_bits.1 | permission_bits.2) != 0 {
            return Ok(());
        }
    } else if file_mode & permission_bits.2 != 0 {
        return Ok(());
    }

    Err(FsError::PermissionDenied)
}

pub(crate) fn check_ownership(inode: &Inode, creds: &Credentials) -> Result<(), FsError> {
    if creds.uid == 0 || creds.uid == inode.uid() {
        Ok(())
    } else {
        Err(FsError::OperationNotPermitted)
    }
}

pub(crate) fn check_sticky_bit_delete(
    parent: &Inode,
    target: &Inode,
    creds: &Credentials,
) -> Result<(), FsError> {
    if parent.is_directory()
        && parent.mode() & S_ISVTX != 0
        && creds.uid != 0
        && creds.uid != parent.uid()
        && creds.uid != target.uid()
    {
        return Err(FsError::PermissionDenied);
    }
    Ok(())
}

pub(crate) fn validate_mode(mode: u32) -> u32 {
    mode & 0o7777
}

pub(crate) fn can_set_times(
    inode: &Inode,
    creds: &Credentials,
    setting_to_current_time: bool,
    acl: Option<&acl::PosixAcl>,
) -> Result<(), FsError> {
    if creds.uid == 0 || creds.uid == inode.uid() {
        return Ok(());
    }

    if setting_to_current_time {
        return check_access_with_acl(inode, creds, AccessMode::Write, acl);
    }

    Err(FsError::OperationNotPermitted)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fs::inode::test_file_inode;

    fn directory(mode: u32) -> Inode {
        Inode::Directory(DirectoryInode {
            mtime: 0,
            mtime_nsec: 0,
            ctime: 0,
            ctime_nsec: 0,
            atime: 0,
            atime_nsec: 0,
            mode,
            uid: 1000,
            gid: 1000,
            entry_count: 0,
            parent: 0,
            name: Some(b"restricted".to_vec()),
            nlink: 2,
        })
    }

    #[test]
    fn root_can_search_directories_without_execute_bits() {
        let root = Credentials::from_auth_context(&AuthContext::default());
        let owner = Credentials::from_auth_context(&AuthContext {
            uid: 1000,
            gid: 1000,
            ..AuthContext::default()
        });
        for mode in [0o000, 0o444, 0o600, 0o666] {
            let inode = directory(mode);
            assert_eq!(check_access(&inode, &root, AccessMode::Execute), Ok(()));
            assert_eq!(
                check_access(&inode, &owner, AccessMode::Execute),
                Err(FsError::PermissionDenied),
                "an ordinary owner still needs directory search permission"
            );
        }
    }

    #[test]
    fn root_still_needs_an_execute_bit_for_regular_files() {
        let root = Credentials::from_auth_context(&AuthContext::default());
        for mode in [0o000, 0o444, 0o600, 0o666, 0o100, 0o010, 0o001] {
            let mut inode = test_file_inode(0);
            let Inode::File(file) = &mut inode else {
                unreachable!()
            };
            file.mode = mode;
            let expected = if mode & 0o111 == 0 {
                Err(FsError::PermissionDenied)
            } else {
                Ok(())
            };
            assert_eq!(check_access(&inode, &root, AccessMode::Execute), expected);
            assert_eq!(check_access(&inode, &root, AccessMode::Read), Ok(()));
            assert_eq!(check_access(&inode, &root, AccessMode::Write), Ok(()));
        }
    }
}
