pub(crate) mod acl;
pub(crate) mod nfs4;

use super::errors::FsError;
use super::inode::{DirectoryInode, Inode, InodeAttrs};
use super::types::{AuthContext, SetGid};

const S_ISVTX: u32 = 0o1000;

#[derive(Debug, Clone, Copy)]
pub struct Credentials {
    pub uid: u32,
    pub gid: u32,
    /// Whether `gid` came from the caller rather than a protocol fallback.
    pub gid_known: bool,
    pub groups: [u32; 16],
    pub groups_count: usize,
    /// Whether every supplementary group is represented here.
    pub groups_complete: bool,
    /// True when this server shortened a supplied list. This cannot inherit
    /// the kernel-authorized fallback used for upstream incomplete lists.
    pub groups_truncated: bool,
}

impl Credentials {
    pub(crate) fn from_auth_context(auth: &AuthContext) -> Self {
        let groups_truncated = auth.groups_truncated || auth.gids.len() > 16;
        let mut creds = Self {
            uid: auth.uid,
            gid: auth.gid,
            gid_known: auth.gid_known,
            groups: [0; 16],
            groups_count: auth.gids.len().min(16),
            // Dropping a restrictive ACL group must not make OTHER appear to
            // be the caller's applicable class. Unknown groups fail closed
            // when an access ACL could distinguish their permissions.
            groups_complete: auth.groups_complete && !groups_truncated,
            groups_truncated,
        };

        for (i, gid) in auth.gids.iter().take(16).enumerate() {
            creds.groups[i] = *gid;
        }

        creds
    }

    #[cfg(test)]
    pub(crate) fn with_gid(self, gid: u32) -> Self {
        Self { gid, ..self }
    }

    pub(crate) fn is_member_of_group(&self, gid: u32) -> bool {
        self.is_known_member_of_group(gid) || (!self.groups_complete && !self.groups_truncated)
    }

    pub(crate) fn is_known_member_of_group(&self, gid: u32) -> bool {
        if self.gid_known && self.gid == gid {
            return true;
        }
        self.groups[..self.groups_count].contains(&gid)
    }
}

/// Group for a new entry. An explicit group wins. A setgid parent otherwise
/// passes on its group; a creation hint supplies the fallback without changing
/// the caller's group membership for permission checks.
pub(crate) fn new_entry_gid(
    parent: &DirectoryInode,
    requested: &SetGid,
    creds: &Credentials,
) -> u32 {
    match requested {
        SetGid::Set(gid) => *gid,
        SetGid::NoChange | SetGid::InheritOr(_) if parent.mode & 0o2000 != 0 => parent.gid,
        SetGid::InheritOr(gid) => *gid,
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
    ReadWrite,
    WriteExecute,
}

impl AccessMode {
    /// An operation's complete permission request. With POSIX ACLs, checking
    /// each bit separately can combine grants from different group entries.
    pub(crate) const fn bits(self) -> u16 {
        match self {
            Self::Read => 4,
            Self::Write => 2,
            Self::Execute => 1,
            Self::ReadWrite => 6,
            Self::WriteExecute => 3,
        }
    }
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
        if mode.bits() & 1 != 0 && !inode.is_directory() && file_mode & 0o111 == 0 {
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

    let want = u32::from(mode.bits());
    let grants = |bits: u32| bits & want == want;
    let allowed = if creds.uid == uid {
        grants(file_mode >> 6)
    } else if creds.is_known_member_of_group(gid) {
        grants(file_mode >> 3)
    } else if creds.groups_truncated {
        // Local truncation does not prove a kernel already authorized this
        // request. The caller may belong to either remaining class.
        grants(file_mode >> 3) && grants(file_mode)
    } else if !creds.groups_complete {
        // Preserve the existing kernel-authorized unknown-group fallback,
        // but do not assemble one request from different permission classes.
        grants(file_mode >> 3) || grants(file_mode)
    } else {
        grants(file_mode)
    };
    if allowed {
        Ok(())
    } else {
        Err(FsError::PermissionDenied)
    }
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
            assert_eq!(
                check_access_with_acl(&inode, &root, AccessMode::Execute, None),
                Ok(())
            );
            assert_eq!(
                check_access_with_acl(&inode, &owner, AccessMode::Execute, None),
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
            assert_eq!(
                check_access_with_acl(&inode, &root, AccessMode::Execute, None),
                expected
            );
            assert_eq!(
                check_access_with_acl(&inode, &root, AccessMode::Read, None),
                Ok(())
            );
            assert_eq!(
                check_access_with_acl(&inode, &root, AccessMode::Write, None),
                Ok(())
            );
            assert_eq!(
                check_access_with_acl(&inode, &root, AccessMode::ReadWrite, None),
                Ok(())
            );
            assert_eq!(
                check_access_with_acl(&inode, &root, AccessMode::WriteExecute, None),
                expected
            );
        }
    }

    #[test]
    fn combined_requests_require_every_bit_from_one_permission_class() {
        let owner = Credentials::from_auth_context(&AuthContext {
            uid: 1000,
            gid: 1000,
            groups_complete: true,
            groups_truncated: false,
            ..AuthContext::default()
        });
        let group = Credentials { uid: 2000, ..owner };
        let other = Credentials { gid: 2000, ..group };
        let unknown = Credentials {
            groups_complete: false,
            groups_truncated: false,
            ..other
        };
        for (creds, mode, allowed) in [
            (owner, 0o400, false),
            (owner, 0o600, true),
            (group, 0o040, false),
            (group, 0o060, true),
            (other, 0o004, false),
            (other, 0o006, true),
            (unknown, 0o042, false),
            (unknown, 0o060, true),
            (unknown, 0o006, true),
        ] {
            assert_eq!(
                check_access_with_acl(&directory(mode), &creds, AccessMode::ReadWrite, None)
                    .is_ok(),
                allowed,
                "mode {mode:o} for {creds:?}"
            );
        }
    }

    #[test]
    fn combined_acl_requests_do_not_union_matching_group_entries() {
        let creds = Credentials::from_auth_context(&AuthContext {
            uid: 2000,
            gid: 300,
            gids: vec![300, 301],
            groups_complete: true,
            groups_truncated: false,
            ..AuthContext::default()
        });
        for (first, second, combined, bits) in [
            (
                AccessMode::Read,
                AccessMode::Write,
                AccessMode::ReadWrite,
                6,
            ),
            (
                AccessMode::Write,
                AccessMode::Execute,
                AccessMode::WriteExecute,
                3,
            ),
        ] {
            let inode = directory(u32::from(bits) << 3);
            let acl = acl::PosixAcl::from_parts(
                7,
                &[],
                0,
                &[(300, first.bits()), (301, second.bits())],
                bits,
                0,
            );
            assert_eq!(
                check_access_with_acl(&inode, &creds, first, Some(&acl)),
                Ok(())
            );
            assert_eq!(
                check_access_with_acl(&inode, &creds, second, Some(&acl)),
                Ok(())
            );
            assert_eq!(
                check_access_with_acl(&inode, &creds, combined, Some(&acl)),
                Err(FsError::PermissionDenied)
            );
            let one_grant = acl::PosixAcl::from_parts(7, &[], 0, &[(300, bits), (301, 0)], bits, 0);
            assert_eq!(
                check_access_with_acl(&inode, &creds, combined, Some(&one_grant)),
                Ok(())
            );
        }
    }

    #[test]
    fn combined_acl_requests_preserve_named_user_precedence_and_mask() {
        let named = Credentials::from_auth_context(&AuthContext {
            uid: 2000,
            gid: 300,
            groups_complete: true,
            ..AuthContext::default()
        });
        // A permissive group must never supplement a matching named user.
        let named_read = acl::PosixAcl::from_parts(6, &[(2000, 4)], 0, &[(300, 6)], 6, 0);
        assert_eq!(
            check_access_with_acl(
                &directory(0o660),
                &named,
                AccessMode::ReadWrite,
                Some(&named_read)
            ),
            Err(FsError::PermissionDenied)
        );
        let named_read_write = acl::PosixAcl::from_parts(6, &[(2000, 6)], 0, &[(300, 6)], 6, 0);
        // The mask comes from the inode and limits the complete request.
        for (mode, expected) in [(0o640, Err(FsError::PermissionDenied)), (0o660, Ok(()))] {
            assert_eq!(
                check_access_with_acl(
                    &directory(mode),
                    &named,
                    AccessMode::ReadWrite,
                    Some(&named_read_write)
                ),
                expected
            );
        }
        // Ownership still wins over any named or group entry.
        let owner = Credentials { uid: 1000, ..named };
        assert_eq!(
            check_access_with_acl(
                &directory(0o460),
                &owner,
                AccessMode::ReadWrite,
                Some(&named_read_write)
            ),
            Err(FsError::PermissionDenied)
        );
    }

    #[test]
    fn truncated_groups_do_not_bypass_restrictive_acl_entries() {
        let auth = AuthContext {
            uid: 2000,
            gid: 2000,
            gids: (300..317).collect(),
            groups_complete: true,
            groups_truncated: false,
            ..AuthContext::default()
        };
        let creds = Credentials::from_auth_context(&auth);
        assert_eq!(creds.groups_count, 16);
        assert!(!creds.groups_complete);
        assert!(creds.groups_truncated);
        let inode = directory(0o777);
        let restrictive = acl::PosixAcl::from_parts(7, &[], 7, &[(316, 0)], 7, 7);
        assert_eq!(
            check_access_with_acl(&inode, &creds, AccessMode::Read, Some(&restrictive)),
            Err(FsError::PermissionDenied)
        );
        let unrestricted = acl::PosixAcl::from_parts(7, &[], 7, &[(316, 7)], 7, 7);
        assert_eq!(
            check_access_with_acl(&inode, &creds, AccessMode::Read, Some(&unrestricted)),
            Ok(())
        );
        let complete = Credentials::from_auth_context(&AuthContext {
            gids: vec![316],
            ..auth
        });
        assert!(complete.groups_complete);
        assert_eq!(
            check_access_with_acl(&inode, &complete, AccessMode::Read, Some(&restrictive)),
            Err(FsError::PermissionDenied)
        );
    }

    #[test]
    fn locally_truncated_groups_do_not_gain_mode_or_group_authority() {
        let creds = Credentials::from_auth_context(&AuthContext {
            uid: 2000,
            gid: 2000,
            gids: (300..317).collect(),
            ..AuthContext::default()
        });
        let mut omitted_group_inode = directory(0o604);
        let Inode::Directory(ref mut omitted_group) = omitted_group_inode else {
            unreachable!()
        };
        omitted_group.gid = 316;
        // The caller really belongs to the denied owning group, but its ID
        // fell beyond the inline list. Falling through to OTHER would allow.
        assert_eq!(
            check_access_with_acl(&omitted_group_inode, &creds, AccessMode::Read, None),
            Err(FsError::PermissionDenied)
        );
        for mode in [0o640, 0o604] {
            assert_eq!(
                check_access_with_acl(&directory(mode), &creds, AccessMode::Read, None),
                Err(FsError::PermissionDenied)
            );
        }
        // If both possible classes allow the request, truncation is harmless.
        assert_eq!(
            check_access_with_acl(&directory(0o644), &creds, AccessMode::Read, None),
            Ok(())
        );
        assert!(!creds.is_member_of_group(1000));
        assert!(!creds.is_member_of_group(316));
        assert!(creds.is_member_of_group(300));
        let Inode::Directory(parent) = directory(0o777) else {
            unreachable!()
        };
        assert_eq!(
            strip_sgid_unless_member(&parent, 0o2770, 1000, &creds),
            0o770
        );
        assert_eq!(
            strip_sgid_unless_member(&parent, 0o2770, 300, &creds),
            0o2770
        );
    }

    #[test]
    fn truncated_group_provenance_survives_roundtrip_and_changes_replay_identity() {
        let creds = Credentials::from_auth_context(&AuthContext {
            uid: 2000,
            gid: 2000,
            gids: (300..317).collect(),
            ..AuthContext::default()
        });
        let auth = AuthContext::from(&creds);
        assert_eq!(auth.gids.len(), 16);
        assert!(auth.groups_truncated);
        assert!(!auth.groups_complete);
        let roundtrip = Credentials::from_auth_context(&auth);
        assert!(roundtrip.groups_truncated);
        assert_eq!(
            check_access_with_acl(&directory(0o640), &roundtrip, AccessMode::Read, None),
            Err(FsError::PermissionDenied)
        );
        assert!(!roundtrip.is_member_of_group(1000));

        let kernel_checked = AuthContext {
            groups_truncated: false,
            ..auth.clone()
        };
        let fingerprint = |auth| {
            crate::fs::mutation::overlay_helpers::direct_write_fingerprint(
                auth, 1, 0, b"data", [0; 16], true, b"test",
            )
            .into_bytes()
        };
        assert_ne!(fingerprint(&auth), fingerprint(&kernel_checked));
    }

    #[test]
    fn upstream_kernel_authorized_unknown_groups_keep_the_existing_fallback() {
        let creds = Credentials::from_auth_context(&AuthContext {
            uid: 2000,
            gid: 2000,
            groups_complete: false,
            ..AuthContext::default()
        });
        assert!(!creds.groups_truncated);
        assert_eq!(
            check_access_with_acl(&directory(0o640), &creds, AccessMode::Read, None),
            Ok(())
        );
        assert!(creds.is_member_of_group(1000));
        let roundtrip = Credentials::from_auth_context(&AuthContext::from(&creds));
        assert!(!roundtrip.groups_complete);
        assert!(!roundtrip.groups_truncated);
        assert_eq!(
            check_access_with_acl(&directory(0o604), &roundtrip, AccessMode::Read, None),
            Ok(())
        );
    }

    #[tokio::test]
    async fn truncated_groups_cannot_change_ownership_to_an_unproven_group() {
        let fs = crate::fs::ZeroFS::new_in_memory().await.unwrap();
        let owner = Credentials::from_auth_context(&AuthContext {
            uid: 1000,
            gid: 1000,
            gids: (300..317).collect(),
            ..AuthContext::default()
        });
        let (id, _) = fs
            .create(&owner, 0, b"truncated-groups", &Default::default())
            .await
            .unwrap();
        let set_group = |gid| crate::fs::types::SetAttributes {
            gid: SetGid::Set(gid),
            ..Default::default()
        };
        assert_eq!(
            fs.setattr(&owner, id, &set_group(9000)).await.map(|_| ()),
            Err(FsError::OperationNotPermitted)
        );
        assert_eq!(fs.inode_store.get(id).await.unwrap().gid(), 1000);
        fs.setattr(&owner, id, &set_group(300)).await.unwrap();
        assert_eq!(fs.inode_store.get(id).await.unwrap().gid(), 300);
        assert_eq!(
            fs.setattr(
                &owner,
                id,
                &crate::fs::types::SetAttributes {
                    gid: SetGid::InheritOr(9000),
                    ..Default::default()
                },
            )
            .await
            .map(|_| ()),
            Err(FsError::InvalidArgument)
        );
        assert_eq!(fs.inode_store.get(id).await.unwrap().gid(), 300);
    }
}
