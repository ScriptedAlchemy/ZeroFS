use crate::db::{Db, Transaction};
use crate::fs::errors::FsError;
use crate::fs::inode::InodeId;
use crate::fs::key_codec::KeyCodec;
use bytes::Bytes;
use futures::StreamExt;
use std::sync::Arc;

pub(crate) const XATTR_COUNT_MAX: usize = 4096;

#[derive(Clone)]
pub struct XattrStore {
    db: Arc<Db>,
    key_codec: Arc<KeyCodec>,
}

impl XattrStore {
    pub(crate) fn new(db: Arc<Db>, key_codec: Arc<KeyCodec>) -> Self {
        Self { db, key_codec }
    }

    pub async fn get(&self, id: InodeId, name: &[u8]) -> Result<Bytes, FsError> {
        self.db
            .get_bytes(&self.key_codec.xattr_key(id, name))
            .await
            .map_err(|e| FsError::from_db_error(&e))?
            .ok_or(FsError::NoData)
    }

    pub async fn list(&self, id: InodeId) -> Result<Vec<Bytes>, FsError> {
        let prefix = self.key_codec.xattr_prefix(id);
        let mut rows = self
            .db
            .scan_prefix(prefix.clone(), None, 64 * 1024)
            .await
            .map_err(|e| FsError::from_db_error(&e))?;
        let mut names = Vec::new();
        while let Some(row) = rows.next().await {
            let (key, _) = row.map_err(|e| FsError::from_db_error(&e))?;
            if names.len() == XATTR_COUNT_MAX {
                return Err(FsError::NoSpace);
            }
            names.push(key.slice(prefix.len()..));
        }
        Ok(names)
    }

    pub(crate) fn put(&self, txn: &mut Transaction, id: InodeId, name: &[u8], value: Bytes) {
        txn.put_bytes(&self.key_codec.xattr_key(id, name), value);
    }

    pub(crate) fn delete(&self, txn: &mut Transaction, id: InodeId, name: &[u8]) {
        txn.delete_bytes(&self.key_codec.xattr_key(id, name));
    }

    pub(crate) async fn delete_all(
        &self,
        txn: &mut Transaction,
        id: InodeId,
    ) -> Result<(), FsError> {
        // Stream rows instead of using the capped public listing. The cap is
        // an admission rule for new attributes, not a reason to strand an
        // inode written by an earlier development build. Only keys remain in
        // the caller's transaction, keeping cleanup atomic with inode deletion
        // without retaining all names or any attribute values separately.
        let mut rows = self
            .db
            .scan_prefix(self.key_codec.xattr_prefix(id), None, 64 * 1024)
            .await
            .map_err(|e| FsError::from_db_error(&e))?;
        while let Some(row) = rows.next().await {
            let (key, _) = row.map_err(|e| FsError::from_db_error(&e))?;
            // Detach from scanner buffers before retaining the deletion key.
            txn.delete_bytes(&Bytes::copy_from_slice(&key));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fs::ZeroFS;
    use crate::fs::key_codec::{KeyPrefix, ParsedKey};

    #[test]
    fn xattr_key_preserves_inode_and_name_bytes() {
        let codec = KeyCodec::new();
        for id in [0, 1, 255, u64::MAX] {
            for name in [b"user.A".as_slice(), b"user.a", b"user.\xff"] {
                let key = codec.xattr_key(id, name);
                assert_eq!(&key[..5], b"meta\x0a");
                assert_eq!(&key[5..13], &id.to_be_bytes());
                assert_eq!(&key[13..], name);
                match codec.parse_key(&key) {
                    ParsedKey::Xattr {
                        inode_id,
                        name: decoded,
                    } => {
                        assert_eq!(inode_id, id);
                        assert_eq!(decoded.as_ref(), name);
                    }
                    _ => panic!("xattr key did not decode"),
                }
                let (start, end) = codec.prefix_range(KeyPrefix::Xattr);
                assert!(key >= start && key < end);
            }
            assert!(matches!(
                codec.parse_key(&codec.xattr_prefix(id)),
                ParsedKey::Unknown
            ));
        }
    }

    #[tokio::test]
    async fn xattr_store_lists_in_byte_order_and_deletes_only_one_inode() {
        let fs = ZeroFS::new_in_memory().await.unwrap();
        let mut txn = fs.db.new_transaction().unwrap();
        for (id, name) in [(1, b"user.z"), (1, b"user.A"), (2, b"user.a")] {
            fs.xattr_store
                .put(&mut txn, id, name, Bytes::from_static(b"value"));
        }
        fs.write_coordinator.commit(txn).await.unwrap();
        assert_eq!(
            fs.xattr_store.list(1).await.unwrap(),
            vec![b"user.A".as_slice(), b"user.z"]
        );
        assert_eq!(
            fs.xattr_store.get(1, b"user.A").await.unwrap(),
            b"value".as_slice()
        );
        let mut txn = fs.db.new_transaction().unwrap();
        fs.xattr_store.delete_all(&mut txn, 1).await.unwrap();
        fs.write_coordinator.commit(txn).await.unwrap();
        assert!(fs.xattr_store.list(1).await.unwrap().is_empty());
        assert_eq!(fs.xattr_store.get(1, b"user.A").await, Err(FsError::NoData));
        assert_eq!(fs.xattr_store.list(2).await.unwrap().len(), 1);
    }
}
