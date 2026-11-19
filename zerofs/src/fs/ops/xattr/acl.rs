//! Storage, inheritance and enforcement of POSIX ACLs, and translation of
//! written NFSv4 ACLs. The contract is `docs/permissions-and-acl-contract.md`.

use super::{XattrKind, XattrName};
use crate::fs::errors::FsError;
use crate::fs::inode::{Inode, InodeAttrs, InodeId};
use crate::fs::mutation::types::{ConflictKey, ConflictScope};
use crate::fs::permissions::acl::{POSIX_ACL_ACCESS, POSIX_ACL_DEFAULT, PosixAcl};
use crate::fs::permissions::nfs4::map_nfs4_acl;
use crate::fs::permissions::{AccessMode, Credentials, check_access_with_acl};
use crate::fs::store::xattr::XATTR_COUNT_MAX;
use crate::fs::{ZeroFS, get_current_time};
use bytes::Bytes;
use std::sync::atomic::{AtomicBool, Ordering};
use tracing::{error, info, warn};

/// Inode ids are allocated upward from zero and never reach this value, so
/// its attribute rows cannot belong to a file. Older binaries ignore them.
pub(super) const VOLUME_MARKER_INODE: InodeId = u64::MAX;
/// Written in the same transaction as the first stored POSIX ACL row and
/// never removed. Its absence lets a writer skip ACL lookups entirely.
const POSIX_ACL_MARKER: &[u8] = b"zerofs.posix_acl_in_use";

/// Whether this volume may hold POSIX ACLs.
#[derive(Debug, Default)]
pub(crate) struct AclState {
    loaded: tokio::sync::OnceCell<()>,
    posix_in_use: AtomicBool,
}

/// Decode a stored ACL row. A row that does not decode fails closed.
pub(super) fn decode_stored(id: InodeId, value: &[u8]) -> Result<PosixAcl, FsError> {
    match PosixAcl::parse(value) {
        Ok(Some(acl)) => Ok(acl),
        _ => {
            error!(
                inode = id,
                "stored POSIX ACL does not decode; denying access"
            );
            Err(FsError::IoError)
        }
    }
}

fn set_permission_mode(inode: &mut Inode, mode: u32) {
    match inode {
        Inode::File(i) => i.mode = mode,
        Inode::Directory(i) => i.mode = mode,
        Inode::Symlink(i) => i.mode = mode,
        Inode::Fifo(i) | Inode::Socket(i) | Inode::CharDevice(i) | Inode::BlockDevice(i) => {
            i.mode = mode
        }
    }
}

/// ACL rows a new inode inherits from its parent's default ACL.
#[derive(Debug, Default)]
pub(crate) struct InheritedAcls {
    access: Option<Bytes>,
    default: Option<Bytes>,
}

impl InheritedAcls {
    /// Stage the rows in the transaction that creates inode `id`.
    pub(crate) fn stage(self, fs: &ZeroFS, txn: &mut crate::db::Transaction, id: InodeId) {
        if let Some(access) = self.access {
            fs.xattr_store.put(txn, id, POSIX_ACL_ACCESS, access);
        }
        if let Some(default) = self.default {
            fs.xattr_store.put(txn, id, POSIX_ACL_DEFAULT, default);
        }
    }
}

impl ZeroFS {
    /// Whether any POSIX ACL may exist. A writer is the only process that can
    /// add the first ACL, so its cached "no" stays correct until it adds one
    /// itself. A read-only process cannot see a later writer's first ACL
    /// through its cache, so it always looks up per inode.
    pub(super) async fn posix_acls_possible(&self) -> Result<bool, FsError> {
        if self.acl_state.posix_in_use.load(Ordering::Acquire) || self.db.is_read_only() {
            return Ok(true);
        }
        self.acl_state
            .loaded
            .get_or_try_init(|| async {
                if self
                    .xattr_exists(VOLUME_MARKER_INODE, POSIX_ACL_MARKER)
                    .await?
                {
                    self.acl_state.posix_in_use.store(true, Ordering::Release);
                }
                Ok::<(), FsError>(())
            })
            .await?;
        Ok(self.acl_state.posix_in_use.load(Ordering::Acquire))
    }

    /// Whether a row exists, without keeping its value.
    pub(super) async fn xattr_exists(&self, id: InodeId, name: &[u8]) -> Result<bool, FsError> {
        match self.xattr_store.get(id, name).await {
            Ok(_) => Ok(true),
            Err(FsError::NoData) => Ok(false),
            Err(error) => Err(error),
        }
    }

    /// Log at startup whether this volume enforces stored POSIX ACLs.
    pub(crate) async fn report_acl_state(&self) -> Result<(), FsError> {
        if self
            .xattr_exists(VOLUME_MARKER_INODE, POSIX_ACL_MARKER)
            .await?
        {
            self.acl_state.posix_in_use.store(true, Ordering::Release);
            info!("volume stores POSIX ACLs; they are enforced in permission checks");
        }
        Ok(())
    }

    /// The stored access ACL that governs `creds` on `inode`, when one can
    /// change the mode-bit decision. Root and the owner never need it: the
    /// owner entry always equals the owner mode bits.
    pub(crate) async fn access_acl(
        &self,
        id: InodeId,
        inode: &Inode,
        creds: &Credentials,
    ) -> Result<Option<PosixAcl>, FsError> {
        if creds.uid == 0 || creds.uid == inode.uid() || matches!(inode, Inode::Symlink(_)) {
            return Ok(None);
        }
        if !self.posix_acls_possible().await? {
            return Ok(None);
        }
        match self.xattr_store.get(id, POSIX_ACL_ACCESS).await {
            Ok(value) => decode_stored(id, &value).map(Some),
            Err(FsError::NoData) => Ok(None),
            Err(error) => Err(error),
        }
    }

    /// Mode bits combined with any access ACL on the inode.
    pub(crate) async fn check_inode_access(
        &self,
        id: InodeId,
        inode: &Inode,
        creds: &Credentials,
        mode: AccessMode,
    ) -> Result<(), FsError> {
        let acl = self.access_acl(id, inode, creds).await?;
        check_access_with_acl(inode, creds, mode, acl.as_ref())
    }

    /// Linux `posix_acl_create`: a parent default ACL restricts the new
    /// inode's mode and becomes its access ACL; a new directory also inherits
    /// it as its own default. The caller holds the parent's lock.
    pub(crate) async fn inherited_acls(
        &self,
        parent_id: InodeId,
        new_is_directory: bool,
        mode: &mut u32,
        umask: u32,
        strip_sgid: impl FnOnce(u32) -> u32,
    ) -> Result<InheritedAcls, FsError> {
        let parent_default = if self.posix_acls_possible().await? {
            match self.xattr_store.get(parent_id, POSIX_ACL_DEFAULT).await {
                Ok(value) => Some(decode_stored(parent_id, &value)?),
                Err(FsError::NoData) => None,
                Err(error) => return Err(error),
            }
        } else {
            None
        };
        let Some(parent_default) = parent_default else {
            *mode = strip_sgid(*mode & !umask);
            return Ok(InheritedAcls::default());
        };
        // SGID stripping uses the requested mode before the ACL limits group execute.
        *mode = strip_sgid(*mode);
        let mut access = parent_default.clone();
        let store_access = access.create_masq(mode);
        Ok(InheritedAcls {
            access: store_access.then(|| access.to_bytes()),
            default: new_is_directory.then(|| parent_default.to_bytes()),
        })
    }

    /// Set or remove `system.posix_acl_access`, `system.posix_acl_default`,
    /// or translate a written `system.nfs4_acl_xdr`.
    pub(super) async fn mutate_acl(
        &self,
        creds: &Credentials,
        id: InodeId,
        name: &XattrName,
        value: Option<Bytes>,
    ) -> Result<(), FsError> {
        let kind = name.kind()?;
        // Decode before locking. A zero-length or header-only value removes
        // the ACL, as `posix_acl_from_xattr` returns no ACL for it.
        let decoded = match (kind, &value) {
            (XattrKind::PosixAccess | XattrKind::PosixDefault, Some(value)) => {
                PosixAcl::parse(value)?
            }
            _ => None,
        };
        let removal = value.is_none() || (kind != XattrKind::Nfs4Xdr && decoded.is_none());

        let fence = self
            .fence_metadata(ConflictScope::single(ConflictKey::Inode(id)))
            .await?;
        let guard = self.lock_manager.acquire(id).await;
        let mut inode = self.inode_store.get(id).await?;
        name.check_mutation(&inode, creds, removal, None)?;
        let is_directory = inode.is_directory();

        // `Some(None)` removes that ACL; `None` leaves it unchanged.
        let (access, default): (Option<Option<PosixAcl>>, Option<Option<PosixAcl>>) = match kind {
            XattrKind::PosixAccess => (Some(decoded), None),
            XattrKind::PosixDefault if !is_directory => {
                // Linux `set_posix_acl`: EACCES for a default ACL on a
                // non-directory, success for removing one.
                return if decoded.is_some() {
                    Err(FsError::PermissionDenied)
                } else {
                    Ok(())
                };
            }
            XattrKind::PosixDefault => (None, Some(decoded)),
            XattrKind::Nfs4Xdr => {
                // The blob is never stored, so there is nothing to remove.
                let Some(value) = value else {
                    return Err(FsError::NoData);
                };
                match map_nfs4_acl(&value, is_directory, inode.uid(), inode.gid()) {
                    Ok(mapped) => {
                        info!(
                            inode = id,
                            access = %mapped.access.to_text(),
                            default = %mapped.default.as_ref().map(PosixAcl::to_text).unwrap_or_default(),
                            narrowed = ?mapped.reports,
                            "translated NFSv4 ACL to POSIX ACLs"
                        );
                        (
                            Some(Some(mapped.access)),
                            is_directory.then_some(mapped.default),
                        )
                    }
                    Err(refusal) => {
                        warn!(
                            inode = id,
                            reason = %refusal,
                            "refused NFSv4 ACL that POSIX ACLs cannot enforce without loosening"
                        );
                        return Err(FsError::NotSupported);
                    }
                }
            }
            _ => return Err(FsError::NotSupported),
        };

        let mut txn = self.db.new_transaction()?;
        let mut changed = false;
        let mut new_rows = 0usize;
        let mut stored = false;
        if let Some(access) = access {
            let present = self.xattr_exists(id, POSIX_ACL_ACCESS).await?;
            match access {
                None if present => {
                    self.xattr_store.delete(&mut txn, id, POSIX_ACL_ACCESS);
                    changed = true;
                }
                None => {}
                Some(acl) => {
                    // Linux `posix_acl_update_mode`.
                    let (not_equiv, bits) = acl.equiv_mode();
                    let mut mode = (inode.mode() & !0o777) | bits;
                    if creds.uid != 0 && !creds.is_known_member_of_group(inode.gid()) {
                        mode &= !0o2000;
                    }
                    set_permission_mode(&mut inode, mode);
                    if not_equiv {
                        self.xattr_store
                            .put(&mut txn, id, POSIX_ACL_ACCESS, acl.to_bytes());
                        new_rows += usize::from(!present);
                        stored = true;
                    } else if present {
                        self.xattr_store.delete(&mut txn, id, POSIX_ACL_ACCESS);
                    }
                    changed = true;
                }
            }
        }
        if let Some(default) = default {
            let present = self.xattr_exists(id, POSIX_ACL_DEFAULT).await?;
            match default {
                None if present => {
                    self.xattr_store.delete(&mut txn, id, POSIX_ACL_DEFAULT);
                    changed = true;
                }
                None => {}
                Some(acl) => {
                    self.xattr_store
                        .put(&mut txn, id, POSIX_ACL_DEFAULT, acl.to_bytes());
                    new_rows += usize::from(!present);
                    stored = true;
                    changed = true;
                }
            }
        }
        if !changed {
            // Removing an absent ACL succeeds without a change, like tmpfs.
            return Ok(());
        }
        if new_rows > 0 && self.xattr_store.list(id).await?.len() + new_rows > XATTR_COUNT_MAX {
            return Err(FsError::NoSpace);
        }
        if stored {
            // Publish before the commit so no concurrent check skips the
            // lookup once the row is visible.
            self.acl_state.posix_in_use.store(true, Ordering::Release);
            self.xattr_store.put(
                &mut txn,
                VOLUME_MARKER_INODE,
                POSIX_ACL_MARKER,
                Bytes::new(),
            );
        }
        let (sec, nsec) = get_current_time();
        inode.set_ctime(sec, nsec);
        self.inode_store.save(&mut txn, id, &inode)?;
        if inode.nlink() != 0
            && let Some(parent) = inode.parent()
            && let Some(filename) = inode.name()
        {
            self.directory_store
                .update_inode_in_entry(&mut txn, parent, filename, id, &inode)
                .await?;
        }
        self.write_coordinator
            .commit_holding(txn, (fence, guard))
            .await
    }
}
