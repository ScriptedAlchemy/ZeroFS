//! The encryption envelope and database authority have independent physical
//! budgets. Every object-store entry point uses the same exact key binding.
use crate::raw_member::RawMemberStore;
use async_trait::async_trait;
use futures::{StreamExt, TryStreamExt, stream::BoxStream};
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, RenameOptions, Result, path::Path,
};
use std::fmt;

#[derive(Clone, Debug)]
pub(super) struct RawAuthority {
    pub metadata: RawMemberStore,
    pub keys: RawMemberStore,
    pub envelope: Path,
}

impl RawAuthority {
    fn select(&self, key: &Path) -> &RawMemberStore {
        if key == &self.envelope {
            &self.keys
        } else {
            &self.metadata
        }
    }

    fn same_region(&self, from: &Path, to: &Path) -> Result<&RawMemberStore> {
        if (from == &self.envelope) != (to == &self.envelope) {
            return Err(object_store::Error::NotSupported {
                source: "cross-region authority copies and renames require a transaction".into(),
            });
        }
        Ok(self.select(from))
    }
}

impl fmt::Display for RawAuthority {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "RawAuthority({})", self.metadata.identity())
    }
}

#[async_trait]
impl ObjectStore for RawAuthority {
    async fn put_opts(
        &self,
        key: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> Result<PutResult> {
        self.select(key).put_opts(key, payload, opts).await
    }
    async fn get_opts(&self, key: &Path, opts: GetOptions) -> Result<GetResult> {
        self.select(key).get_opts(key, opts).await
    }
    async fn get_ranges(
        &self,
        key: &Path,
        ranges: &[std::ops::Range<u64>],
    ) -> Result<Vec<bytes::Bytes>> {
        self.select(key).get_ranges(key, ranges).await
    }
    async fn put_multipart_opts(
        &self,
        key: &Path,
        opts: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>> {
        self.select(key).put_multipart_opts(key, opts).await
    }
    fn delete_stream(
        &self,
        locations: BoxStream<'static, Result<Path>>,
    ) -> BoxStream<'static, Result<Path>> {
        use object_store::ObjectStoreExt;
        let stores = self.clone();
        locations
            .and_then(move |key| {
                let store = stores.select(&key).clone();
                async move {
                    store.delete(&key).await?;
                    Ok(key)
                }
            })
            .boxed()
    }
    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        self.metadata
            .list(prefix)
            .chain(self.keys.list(prefix))
            .boxed()
    }
    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        let mut result = self.metadata.list_with_delimiter(prefix).await?;
        let keys = self.keys.list_with_delimiter(prefix).await?;
        result.objects.extend(keys.objects);
        result.objects.sort_by(|a, b| a.location.cmp(&b.location));
        result.common_prefixes.extend(keys.common_prefixes);
        result.common_prefixes.sort();
        result.common_prefixes.dedup();
        Ok(result)
    }
    async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> Result<()> {
        self.same_region(from, to)?
            .copy_opts(from, to, options)
            .await
    }
    async fn rename_opts(&self, from: &Path, to: &Path, options: RenameOptions) -> Result<()> {
        self.same_region(from, to)?
            .rename_opts(from, to, options)
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use object_store::{ObjectStoreExt, PutMode};
    use zerofs_raw_device::p3::P3Layout;

    #[tokio::test]
    async fn v2_reopen_preserves_role_boundaries_and_rejects_v1_reinitialization() {
        let mib = 1 << 20;
        let file = tempfile::tempfile().unwrap();
        file.set_len(32 * mib).unwrap();
        let layout = P3Layout::new(32 * mib, [4, 8, 8, 4, 4, 4].map(|n| n * mib)).unwrap();
        let pool = [5; 16];
        let member = [6; 16];
        layout
            .provision_v2(file.try_clone().unwrap(), pool, member)
            .unwrap();
        let open = || {
            layout
                .open_v2(file.try_clone().unwrap(), pool, member)
                .unwrap()
        };
        let envelope = Path::from("db/zerofs.key");
        let manifest = Path::from("db/manifest/1");
        let roles = open();
        let authority = RawAuthority {
            metadata: RawMemberStore::new(roles.metadata, (), 0).unwrap(),
            keys: RawMemberStore::new(roles.keys, (), 0).unwrap(),
            envelope: envelope.clone(),
        };
        for (key, payload) in [
            (&envelope, b"envelope".as_slice()),
            (&manifest, b"manifest".as_slice()),
        ] {
            authority
                .put_opts(
                    key,
                    Bytes::copy_from_slice(payload).into(),
                    PutMode::Create.into(),
                )
                .await
                .unwrap();
        }
        assert!(authority.rename(&envelope, &manifest).await.is_err());
        drop(authority);
        assert!(
            layout
                .provision(file.try_clone().unwrap(), pool, member)
                .is_err()
        );
        assert!(
            layout
                .open(file.try_clone().unwrap(), pool, member)
                .is_err()
        );
        assert!(
            layout
                .open_v2(file.try_clone().unwrap(), [7; 16], member)
                .is_err()
        );
        let roles = open();
        assert_eq!(
            roles.keys.get(b"db/zerofs.key").unwrap().as_deref(),
            Some(b"envelope".as_slice())
        );
        assert_eq!(
            roles.metadata.get(b"db/manifest/1").unwrap().as_deref(),
            Some(b"manifest".as_slice())
        );
        assert!(!roles.metadata.contains_key(b"db/zerofs.key"));
        assert!(!roles.keys.contains_key(b"db/manifest/1"));
        assert!(roles.cache.is_empty() && roles.scratch.is_empty());
        assert!(roles.catalog.is_empty().unwrap() && roles.journal.is_empty().unwrap());
    }
}
