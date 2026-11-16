use crate::dedup::{DedupResult, OpId};
use crate::fs::errors::FsError;
use crate::fs::inode::{Inode, InodeAttrs, InodeId};
use crate::fs::mutation::types::{ConflictKey, ConflictScope};
use crate::fs::permissions::{AccessMode, Credentials, check_access};
use crate::fs::{ZeroFS, get_current_time};
use bytes::Bytes;

pub const XATTR_VALUE_MAX: usize = 2_097_152;

#[derive(Debug, Clone)]
pub struct XattrName(Bytes);

impl XattrName {
    pub fn new(name: &[u8]) -> Result<Self, FsError> {
        if name.is_empty() || name.contains(&0) {
            return Err(FsError::InvalidArgument);
        }
        if name.len() > 255 {
            return Err(FsError::NameTooLong);
        }
        let name = Self(Bytes::copy_from_slice(name));
        name.namespace()?;
        Ok(name)
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }

    fn namespace(&self) -> Result<&[u8], FsError> {
        let Some(dot) = self.0.iter().position(|&b| b == b'.') else {
            return Err(FsError::NotSupported);
        };
        if dot + 1 == self.0.len() {
            return Err(FsError::InvalidArgument);
        }
        match &self.0[..dot] {
            ns @ (b"user" | b"trusted" | b"security") => Ok(ns),
            _ => Err(FsError::NotSupported),
        }
    }

    pub(crate) fn check(
        &self,
        inode: &Inode,
        creds: &Credentials,
        write: bool,
    ) -> Result<(), FsError> {
        match self.namespace()? {
            b"user" => {
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
                check_access(
                    inode,
                    creds,
                    if write {
                        AccessMode::Write
                    } else {
                        AccessMode::Read
                    },
                )
            }
            b"trusted" if creds.uid != 0 => Err(FsError::OperationNotPermitted),
            b"security" if write && creds.uid != 0 => Err(FsError::OperationNotPermitted),
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

fn check_buffer(value: Bytes, size: Option<usize>) -> Result<Bytes, FsError> {
    if size.is_some_and(|size| size != 0 && size < value.len()) {
        return Err(FsError::Range);
    }
    Ok(value)
}

impl ZeroFS {
    pub async fn get_xattr(
        &self,
        creds: &Credentials,
        id: InodeId,
        name: &XattrName,
        size: Option<usize>,
    ) -> Result<Bytes, FsError> {
        let _guard = self.lock_manager.acquire(id).await;
        let inode = self.inode_store.get(id).await?;
        name.check(&inode, creds, false)?;
        check_buffer(self.xattr_store.get(id, name.as_bytes()).await?, size)
    }

    pub async fn list_xattrs(
        &self,
        creds: &Credentials,
        id: InodeId,
        size: Option<usize>,
    ) -> Result<Bytes, FsError> {
        let _guard = self.lock_manager.acquire(id).await;
        let inode = self.inode_store.get(id).await?;
        let mut list = Vec::new();
        for name in self.xattr_store.list(id).await? {
            let name = XattrName::new(&name)?;
            if name.check(&inode, creds, false).is_ok() {
                list.extend_from_slice(name.as_bytes());
                list.push(0);
            }
        }
        check_buffer(Bytes::from(list), size)
    }

    pub async fn set_xattr(
        &self,
        creds: &Credentials,
        id: InodeId,
        name: &XattrName,
        value: XattrValue,
        mode: XattrSetMode,
    ) -> Result<(), FsError> {
        self.mutate_xattr(creds, id, name, Some((value, mode)), [0; 16])
            .await
    }

    pub async fn remove_xattr(
        &self,
        creds: &Credentials,
        id: InodeId,
        name: &XattrName,
    ) -> Result<(), FsError> {
        self.mutate_xattr(creds, id, name, None, [0; 16]).await
    }

    async fn mutate_xattr(
        &self,
        creds: &Credentials,
        id: InodeId,
        name: &XattrName,
        value: Option<(XattrValue, XattrSetMode)>,
        op_id: OpId,
    ) -> Result<(), FsError> {
        let applied = |result| match result {
            DedupResult::Applied => Some(()),
            _ => None,
        };
        if self.replay_dedup_result(&op_id, applied)?.is_some() {
            return Ok(());
        }
        let fence = self
            .fence_metadata(ConflictScope::single(ConflictKey::Inode(id)))
            .await?;
        let guard = self.lock_manager.acquire(id).await;
        if self.replay_dedup_result(&op_id, applied)?.is_some() {
            return Ok(());
        }
        let mut inode = self.inode_store.get(id).await?;
        name.check(&inode, creds, true)?;
        let exists = match self.xattr_store.get(id, name.as_bytes()).await {
            Ok(_) => true,
            Err(FsError::NoData) => false,
            Err(error) => return Err(error),
        };
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
        match &mut inode {
            Inode::File(i) => {
                i.ctime = sec;
                i.ctime_nsec = nsec;
            }
            Inode::Directory(i) => {
                i.ctime = sec;
                i.ctime_nsec = nsec;
            }
            Inode::Symlink(i) => {
                i.ctime = sec;
                i.ctime_nsec = nsec;
            }
            Inode::Fifo(i) | Inode::Socket(i) | Inode::CharDevice(i) | Inode::BlockDevice(i) => {
                i.ctime = sec;
                i.ctime_nsec = nsec;
            }
        }
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
        txn.set_dedup_result(op_id, DedupResult::Applied);
        let pending = self.write_coordinator.submit(txn)?;
        // Keep the lock through apply even if the request stops awaiting us.
        crate::task::spawn_named("xattr-commit", async move {
            let _fence = fence;
            let _guard = guard;
            pending.wait().await
        })
        .await
        .map_err(|_| FsError::IoError)?
    }
}

#[cfg(test)]
mod tests;
