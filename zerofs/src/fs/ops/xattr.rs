use crate::fs::errors::FsError;
use crate::fs::inode::{Inode, InodeAttrs, InodeId};
use crate::fs::mutation::types::{ConflictKey, ConflictScope};
use crate::fs::permissions::acl::PosixAcl;
use crate::fs::permissions::acl::{NFS4_ACL_XDR, POSIX_ACL_ACCESS, POSIX_ACL_DEFAULT};
use crate::fs::permissions::{AccessMode, Credentials, check_access_with_acl, check_ownership};
use crate::fs::{ZeroFS, get_current_time};
use bytes::Bytes;

pub const XATTR_VALUE_MAX: usize = 2_097_152;

#[derive(Debug, Clone)]
pub struct XattrName(Bytes);

/// How ZeroFS treats an attribute name. See `docs/extended-attributes.md` and
/// `docs/permissions-and-acl-contract.md`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum XattrKind {
    User,
    Trusted,
    Security,
    /// `system.posix_acl_access`: stored, served and enforced.
    PosixAccess,
    /// `system.posix_acl_default`: stored, served and inherited on create.
    PosixDefault,
    /// `system.nfs4_acl_xdr`: translated to POSIX ACLs on write, or refused.
    Nfs4Xdr,
}

impl XattrName {
    pub fn new(name: &[u8]) -> Result<Self, FsError> {
        if name.is_empty() || name.contains(&0) {
            return Err(FsError::InvalidArgument);
        }
        if name.len() > 255 {
            return Err(FsError::NameTooLong);
        }
        let name = Self(Bytes::copy_from_slice(name));
        name.kind()?;
        Ok(name)
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }

    pub(crate) fn kind(&self) -> Result<XattrKind, FsError> {
        let Some(dot) = self.0.iter().position(|&b| b == b'.') else {
            return Err(FsError::NotSupported);
        };
        if dot + 1 == self.0.len() {
            return Err(FsError::InvalidArgument);
        }
        match &self.0[..dot] {
            b"user" => Ok(XattrKind::User),
            b"trusted" => Ok(XattrKind::Trusted),
            b"security" => Ok(XattrKind::Security),
            b"system" => match self.as_bytes() {
                POSIX_ACL_ACCESS => Ok(XattrKind::PosixAccess),
                POSIX_ACL_DEFAULT => Ok(XattrKind::PosixDefault),
                NFS4_ACL_XDR => Ok(XattrKind::Nfs4Xdr),
                _ => Err(FsError::NotSupported),
            },
            _ => Err(FsError::NotSupported),
        }
    }

    /// Whether the name is one of the ACL attributes ZeroFS interprets.
    pub(crate) fn is_acl(&self) -> bool {
        matches!(
            self.kind(),
            Ok(XattrKind::PosixAccess | XattrKind::PosixDefault | XattrKind::Nfs4Xdr)
        )
    }

    /// `acl` is the inode's access ACL, from `ZeroFS::access_acl`.
    fn check_read(
        &self,
        inode: &Inode,
        creds: &Credentials,
        acl: Option<&PosixAcl>,
    ) -> Result<(), FsError> {
        if self.as_bytes() == b"security.capability" {
            return Err(FsError::NoData);
        }
        self.check_access(inode, creds, false, acl)
    }

    pub(crate) fn check_mutation(
        &self,
        inode: &Inode,
        creds: &Credentials,
        remove: bool,
        acl: Option<&PosixAcl>,
    ) -> Result<(), FsError> {
        if !remove && self.as_bytes() == b"security.capability" {
            return Err(FsError::NotSupported);
        }
        self.check_access(inode, creds, true, acl)
    }

    fn check_access(
        &self,
        inode: &Inode,
        creds: &Credentials,
        write: bool,
        acl: Option<&PosixAcl>,
    ) -> Result<(), FsError> {
        match self.kind()? {
            XattrKind::User => {
                if !matches!(inode, Inode::File(_) | Inode::Directory(_)) {
                    return Err(FsError::OperationNotPermitted);
                }
                if write
                    && inode.is_directory()
                    && inode.mode() & 0o1000 != 0
                    && creds.uid != 0
                    && creds.uid != inode.uid()
                {
                    return Err(FsError::OperationNotPermitted);
                }
                check_access_with_acl(
                    inode,
                    creds,
                    if write {
                        AccessMode::Write
                    } else {
                        AccessMode::Read
                    },
                    acl,
                )
            }
            XattrKind::Trusted if creds.uid != 0 => Err(FsError::OperationNotPermitted),
            XattrKind::Security if write && creds.uid != 0 => Err(FsError::OperationNotPermitted),
            // Linux has no ACL operations on symlinks.
            XattrKind::PosixAccess | XattrKind::PosixDefault | XattrKind::Nfs4Xdr
                if matches!(inode, Inode::Symlink(_)) =>
            {
                Err(FsError::NotSupported)
            }
            // Any caller may read an ACL; only the owner or root may change it
            // (Linux `inode_owner_or_capable`).
            XattrKind::PosixAccess | XattrKind::PosixDefault | XattrKind::Nfs4Xdr if write => {
                check_ownership(inode, creds)
            }
            _ => Ok(()),
        }
    }
}

#[derive(Debug, Clone)]
pub struct XattrValue(Bytes);

impl XattrValue {
    pub fn new(value: Bytes) -> Result<Self, FsError> {
        if value.len() > XATTR_VALUE_MAX {
            return Err(FsError::TooBig);
        }
        Ok(Self(value))
    }
}

#[derive(Debug, Clone, Copy)]
pub enum XattrSetMode {
    Either,
    Create,
    Replace,
}

impl XattrSetMode {
    pub fn from_flags(flags: u32) -> Result<Self, FsError> {
        match flags {
            0 => Ok(Self::Either),
            1 => Ok(Self::Create),
            2 => Ok(Self::Replace),
            _ => Err(FsError::InvalidArgument),
        }
    }
}

impl ZeroFS {
    pub async fn get_xattr(
        &self,
        creds: &Credentials,
        id: InodeId,
        name: &XattrName,
    ) -> Result<Bytes, FsError> {
        let _guard = self.lock_manager.acquire(id).await;
        let inode = self.inode_store.get(id).await?;
        if name.kind()? == XattrKind::PosixAccess {
            name.check_read(&inode, creds, None)?;
            // Even an owner must receive ACL entries and mode from the same
            // view; this process's inode lock does not stop a reader refresh.
            let (inode, value) = self.xattr_store.get_with_inode(id, name.as_bytes()).await?;
            let value = value.ok_or(FsError::NoData)?;
            return Ok(acl::decode_stored(id, &value)?
                .with_mode(inode.mode())
                .to_bytes());
        }
        let (inode, acl) = self.inode_access_context(id, &inode, creds).await?;
        name.check_read(&inode, creds, acl.as_ref())?;
        self.xattr_store.get(id, name.as_bytes()).await
    }

    pub async fn list_xattrs(&self, creds: &Credentials, id: InodeId) -> Result<Bytes, FsError> {
        let _guard = self.lock_manager.acquire(id).await;
        let inode = self.inode_store.get(id).await?;
        let (inode, acl) = self.inode_access_context(id, &inode, creds).await?;
        let mut list = Vec::new();
        for name in self.xattr_store.list(id).await? {
            let Ok(name) = XattrName::new(&name) else {
                // Internal rows, such as the volume ACL marker, are not
                // attributes of any file.
                continue;
            };
            if name.check_read(&inode, creds, acl.as_ref()).is_ok() {
                list.extend_from_slice(name.as_bytes());
                list.push(0);
            }
        }
        Ok(Bytes::from(list))
    }

    pub async fn set_xattr(
        &self,
        creds: &Credentials,
        id: InodeId,
        name: &XattrName,
        value: XattrValue,
        mode: XattrSetMode,
    ) -> Result<(), FsError> {
        if name.is_acl() {
            // Linux ignores XATTR_CREATE and XATTR_REPLACE for ACLs.
            return self.mutate_acl(creds, id, name, Some(value.0)).await;
        }
        self.mutate_xattr(creds, id, name, Some((value, mode)))
            .await
    }

    pub async fn remove_xattr(
        &self,
        creds: &Credentials,
        id: InodeId,
        name: &XattrName,
    ) -> Result<(), FsError> {
        if name.is_acl() {
            return self.mutate_acl(creds, id, name, None).await;
        }
        self.mutate_xattr(creds, id, name, None).await
    }

    async fn mutate_xattr(
        &self,
        creds: &Credentials,
        id: InodeId,
        name: &XattrName,
        value: Option<(XattrValue, XattrSetMode)>,
    ) -> Result<(), FsError> {
        let fence = self
            .fence_metadata(ConflictScope::single(ConflictKey::Inode(id)))
            .await?;
        let guard = self.lock_manager.acquire(id).await;
        let mut inode = self.inode_store.get(id).await?;
        let acl = self.access_acl(id, &inode, creds).await?;
        name.check_mutation(&inode, creds, value.is_none(), acl.as_ref())?;
        let exists = self.xattr_exists(id, name.as_bytes()).await?;
        match &value {
            Some((_, XattrSetMode::Create)) if exists => return Err(FsError::Exists),
            Some((_, XattrSetMode::Replace)) | None if !exists => return Err(FsError::NoData),
            _ => {}
        }
        if value.is_some()
            && !exists
            && self.xattr_store.list(id).await?.len() >= crate::fs::store::xattr::XATTR_COUNT_MAX
        {
            return Err(FsError::NoSpace);
        }
        let (sec, nsec) = get_current_time();
        inode.set_ctime(sec, nsec);
        let mut txn = self.db.new_transaction()?;
        match value {
            Some((value, _)) => self.xattr_store.put(&mut txn, id, name.as_bytes(), value.0),
            None => self.xattr_store.delete(&mut txn, id, name.as_bytes()),
        }
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

mod acl;
pub(crate) use acl::AclState;

#[cfg(test)]
mod acl_tests;
#[cfg(test)]
mod tests;
