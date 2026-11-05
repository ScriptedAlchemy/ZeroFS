//! Permanent local-first capacity pooling below the ZeroFS segment store.
//!
//! Filesystem metadata stays in one local authority. Canonical immutable
//! segment objects are placed on ordered, quota-bounded members; their durable
//! location catalog is not a cache. Each body has one unique physical key.
//! Replacement and migration publish the new location before removing the old
//! body, and failed cleanup remains charged and recoverable.

mod catalog;
#[cfg(test)]
mod tests;

pub use catalog::CATALOG_FILE_NAME;
use catalog::{Catalog, Entry, Garbage, Physical, StoredAttribute, Upload};

use async_trait::async_trait;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use futures::stream::{self, BoxStream};
use futures::{StreamExt, TryStreamExt};
use object_store::path::Path;
use object_store::{
    Attribute, Attributes, CopyMode, CopyOptions, Extensions, GetOptions, GetRange, GetResult,
    GetResultPayload, ListResult, MultipartUpload, ObjectMeta, ObjectStore, ObjectStoreExt,
    PutMode, PutMultipartOptions, PutOptions, PutPayload, PutResult, RenameOptions, UploadPart,
};
use sha2::{Digest, Sha256};
use std::collections::{BTreeSet, HashMap};
use std::fmt;
use std::hash::{Hash, Hasher};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex as StdMutex};
use tokio::sync::{Mutex, OwnedRwLockReadGuard, RwLock};
use uuid::Uuid;

use crate::segment::Segid;
use crate::segment_store::ConditionalMultipartCreate;

/// The pool keeps one physical object per ZeroFS segment. Multipart calls keep
/// zero-copy references to the caller's already sealed segment, not a second
/// concatenated allocation. Migration is bounded by this same limit. ZeroFS's
/// normal 256 MiB sealing threshold leaves room for the final extent/footer.
pub const MAX_TRANSFER_BYTES: u64 = 512 * 1024 * 1024;
const READ_CHUNK_BYTES: u64 = 8 * 1024 * 1024;
const KEY_LOCKS: usize = 256;
const MAX_MARKER_BYTES: u64 = 16 * 1024;

#[derive(Clone, Debug)]
pub struct PoolMember {
    pub id: String,
    /// Stable normalized backend URL, without credentials. Changing a known
    /// id's identity is rejected; adding a new id is the migration mechanism.
    pub identity: String,
    /// This store must already be rooted at the member's configured directory
    /// or bucket prefix. It must not be the pool itself or a clean read cache.
    pub store: Arc<dyn ObjectStore>,
    pub capacity_bytes: u64,
    pub local_path: Option<PathBuf>,
    pub min_free_bytes: u64,
}

#[derive(Clone, Debug, serde::Serialize)]
pub struct PoolMemberStatus {
    pub id: String,
    pub capacity_bytes: u64,
    /// Includes superseded bodies awaiting successful cleanup.
    pub used_bytes: u64,
    /// Includes interrupted/unpublished uploads until their cleanup succeeds.
    pub reserved_bytes: u64,
    pub objects: u64,
}

#[derive(Clone, Debug, serde::Serialize)]
pub struct PoolStatus {
    pub pool_id: String,
    pub members: Vec<PoolMemberStatus>,
    pub pending_operations: usize,
    pub mapped_objects: u64,
}

#[derive(Clone, Debug, serde::Serialize)]
pub struct PoolPlacement {
    #[serde(serialize_with = "serialize_path")]
    pub location: Path,
    pub member_id: String,
    pub size: u64,
}

#[derive(Clone)]
pub struct PoolObjectStore {
    inner: Arc<PoolInner>,
}

struct PoolInner {
    authority: Arc<dyn ObjectStore>,
    authority_prefix: Path,
    catalog: Arc<Catalog>,
    members: Vec<PoolMember>,
    member_indices: HashMap<String, usize>,
    member_checks: Vec<Mutex<bool>>,
    locks: Vec<Arc<RwLock<()>>>,
}

impl fmt::Debug for PoolObjectStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PoolObjectStore")
            .field("pool_id", &self.inner.catalog.identity.pool_id)
            .field("authority_prefix", &self.inner.authority_prefix)
            .field("members", &self.inner.member_indices.keys())
            .finish_non_exhaustive()
    }
}

impl fmt::Display for PoolObjectStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "PoolObjectStore({})", self.inner.catalog.identity.pool_id)
    }
}

fn error(message: impl Into<String>) -> object_store::Error {
    object_store::Error::Generic { store: "PoolObjectStore", source: Box::new(
        crate::retrying_object_store::PermanentError::new(std::io::Error::other(message.into()))
    ) }
}

fn serialize_path<S: serde::Serializer>(path: &Path, serializer: S) -> Result<S::Ok, S::Error> {
    serializer.serialize_str(path.as_ref())
}

fn store_attributes(attributes: &Attributes) -> object_store::Result<Vec<StoredAttribute>> {
    attributes.iter().map(|(attribute, value)| {
        let value = value.as_ref().to_owned();
        Ok(match attribute {
            Attribute::ContentDisposition => StoredAttribute::ContentDisposition(value),
            Attribute::ContentEncoding => StoredAttribute::ContentEncoding(value),
            Attribute::ContentLanguage => StoredAttribute::ContentLanguage(value),
            Attribute::ContentType => StoredAttribute::ContentType(value),
            Attribute::CacheControl => StoredAttribute::CacheControl(value),
            Attribute::StorageClass => StoredAttribute::StorageClass(value),
            Attribute::Metadata(key) => StoredAttribute::Metadata(key.to_string(), value),
            _ => return Err(unsupported("pool catalog cannot preserve this object attribute")),
        })
    }).collect()
}

fn load_attributes(attributes: &[StoredAttribute]) -> Attributes {
    attributes.iter().map(|attribute| match attribute {
        StoredAttribute::ContentDisposition(value) => (Attribute::ContentDisposition, value.clone()),
        StoredAttribute::ContentEncoding(value) => (Attribute::ContentEncoding, value.clone()),
        StoredAttribute::ContentLanguage(value) => (Attribute::ContentLanguage, value.clone()),
        StoredAttribute::ContentType(value) => (Attribute::ContentType, value.clone()),
        StoredAttribute::CacheControl(value) => (Attribute::CacheControl, value.clone()),
        StoredAttribute::StorageClass(value) => (Attribute::StorageClass, value.clone()),
        StoredAttribute::Metadata(key, value) => (Attribute::Metadata(key.clone().into()), value.clone()),
    }).collect()
}

fn unsupported(message: impl Into<String>) -> object_store::Error {
    object_store::Error::NotSupported { source: message.into().into() }
}

fn missing(location: &Path) -> object_store::Error {
    object_store::Error::NotFound { path: location.to_string(), source: "pool object does not exist".into() }
}

fn precondition(location: &Path, reason: &str) -> object_store::Error {
    object_store::Error::Precondition { path: location.to_string(), source: reason.to_owned().into() }
}

fn has_prefix(location: &Path, prefix: Option<&Path>) -> bool {
    prefix.is_none_or(|prefix| location.prefix_match(prefix).is_some())
}

fn catalog_path(raw: &str) -> object_store::Result<Path> {
    // Catalog strings are already encoded ObjectPath values. `Path::from`
    // would encode their '%' characters a second time.
    Path::parse(raw).map_err(|e| error(format!("invalid path in pool catalog: {e}")))
}

impl Entry {
    fn meta(&self, location: &Path) -> object_store::Result<ObjectMeta> {
        let last_modified = DateTime::<Utc>::from_timestamp(self.last_modified_secs, self.last_modified_nanos)
            .ok_or_else(|| error("pool catalog contains an invalid object timestamp"))?;
        Ok(ObjectMeta { location: location.clone(), size: self.size, last_modified,
            e_tag: self.e_tag.clone(), version: self.version.clone() })
    }
}

/// A caller may stop waiting while an atomic backend PUT is still in flight.
/// Its owned task retains the catalog lock, waits for that PUT to settle, and
/// cleans up instead of publishing if cancellation arrived before publication.
struct CancelOnDrop(Arc<AtomicBool>);

impl Drop for CancelOnDrop {
    fn drop(&mut self) {
        self.0.store(true, Ordering::Release);
    }
}

impl PoolObjectStore {
    pub async fn initialize(
        authority: Arc<dyn ObjectStore>, authority_prefix: Path,
        catalog_dir: PathBuf, members: Vec<PoolMember>,
    ) -> object_store::Result<Self> {
        Self::open_inner(authority, authority_prefix, catalog_dir, members, true).await
    }

    pub async fn open(
        authority: Arc<dyn ObjectStore>, authority_prefix: Path,
        catalog_dir: PathBuf, members: Vec<PoolMember>,
    ) -> object_store::Result<Self> {
        Self::open_inner(authority, authority_prefix, catalog_dir, members, false).await
    }

    async fn open_inner(
        authority: Arc<dyn ObjectStore>, authority_prefix: Path,
        catalog_dir: PathBuf, members: Vec<PoolMember>, initialize: bool,
    ) -> object_store::Result<Self> {
        let marker_path = authority_prefix.clone().join(".zerofs-pool").join("identity");
        let marker = read_marker(authority.as_ref(), &marker_path).await?;
        let prefix = authority_prefix.to_string();
        let marker_for_open = marker.clone();
        let catalog = tokio::task::spawn_blocking(move || {
            Catalog::open(&catalog_dir, &prefix, initialize, marker_for_open.as_deref())
        }).await.map_err(|e| error(format!("pool catalog open task failed: {e}")))?
            .map_err(|e| error(format!("pool catalog open failed: {e:#}")))?;
        let member_indices = members.iter().enumerate().map(|(i, member)| (member.id.clone(), i)).collect();
        let pool = Self { inner: Arc::new(PoolInner {
            authority, authority_prefix, catalog: Arc::new(catalog),
            member_checks: (0..members.len()).map(|_| Mutex::new(false)).collect(),
            members, member_indices,
            locks: (0..KEY_LOCKS).map(|_| Arc::new(RwLock::new(()))).collect(),
        }) };
        let configured = pool.inner.members.clone();
        pool.catalog(move |catalog| catalog.configure_members(&configured)).await?;
        let expected = pool.inner.catalog.identity.marker().map_err(|e| error(e.to_string()))?;
        if marker.is_none() {
            if !initialize {
                return Err(error("pool authority marker is missing"));
            }
            match pool.inner.authority.put_opts(&marker_path, Bytes::from(expected.clone()).into(), PutMode::Create.into()).await {
                Ok(_) | Err(object_store::Error::AlreadyExists { .. }) => {},
                Err(error) => return Err(error),
            }
        }
        if read_marker(pool.inner.authority.as_ref(), &marker_path).await?.as_deref() != Some(expected.as_slice()) {
            return Err(error("pool authority identity does not match its catalog"));
        }
        for member in pool.inner.members.iter().filter(|member| member.local_path.is_some()) {
            let id = member.id.clone();
            let record = pool.catalog(move |catalog| catalog.member(&id)).await?;
            if initialize || !record.initialized {
                // Bind a newly registered HDD while it is explicitly being
                // added, even before it receives its first segment. Existing
                // members remain lazy on ordinary reopen so an unavailable
                // disk does not prevent unrelated local reads.
                pool.ensure_member(&member.id, true).await?;
            }
        }
        pool.catalog(Catalog::finish_initialization).await?;
        // Idle remote members are deliberately not opened or probed here.
        // Only durable unfinished operations need recovery; an unavailable
        // member keeps its debt charged rather than making local data vanish.
        pool.recover_pending().await?;
        Ok(pool)
    }

    async fn catalog<T: Send + 'static>(
        &self, operation: impl FnOnce(&Catalog) -> anyhow::Result<T> + Send + 'static,
    ) -> object_store::Result<T> {
        let catalog = Arc::clone(&self.inner.catalog);
        tokio::task::spawn_blocking(move || operation(&catalog)).await
            .map_err(|e| error(format!("pool catalog task failed: {e}")))?
            .map_err(|e| error(format!("pool catalog: {e:#}")))
    }

    fn key_lock(&self, path: &Path) -> Arc<RwLock<()>> {
        let mut hash = std::collections::hash_map::DefaultHasher::new();
        path.hash(&mut hash);
        self.inner.locks[hash.finish() as usize % KEY_LOCKS].clone()
    }

    fn segment(&self, path: &Path) -> bool {
        let prefix = self.inner.authority_prefix.as_ref();
        let relative = if prefix.is_empty() {
            path.as_ref()
        } else {
            let Some(relative) = path.as_ref().strip_prefix(prefix).and_then(|s| s.strip_prefix('/')) else { return false };
            relative
        };
        Segid::from_object_key(relative).is_some()
    }

    fn internal(&self, path: &Path) -> bool {
        path.prefix_match(&self.inner.authority_prefix.clone().join(".zerofs-pool")).is_some()
    }

    fn member(&self, id: &str) -> object_store::Result<&PoolMember> {
        self.inner.member_indices.get(id).map(|&i| &self.inner.members[i])
            .ok_or_else(|| error(format!("pool member {id} is absent from this configuration")))
    }

    async fn ensure_member(&self, id: &str, writing: bool) -> object_store::Result<()> {
        let member = self.member(id)?;
        let index = self.inner.member_indices[id];
        let mut checked = self.inner.member_checks[index].lock().await;
        if *checked && !writing {
            return Ok(());
        }
        let member_id = id.to_owned();
        let record = self.catalog(move |catalog| catalog.member(&member_id)).await?;
        let path = Path::from(format!(".zerofs-pool/{}/members/{id}", self.inner.catalog.identity.pool_id));
        let expected = record.marker(&self.inner.catalog.identity.pool_id).map_err(|e| error(e.to_string()))?;
        match read_marker(member.store.as_ref(), &path).await? {
            Some(found) if found == expected => {},
            Some(_) => return Err(error(format!("pool member {id} has a different identity marker"))),
            None if !record.initialized && writing => {
                match member.store.put_opts(&path, Bytes::from(expected.clone()).into(), PutMode::Create.into()).await {
                    Ok(_) | Err(object_store::Error::AlreadyExists { .. }) => {},
                    Err(error) => return Err(error),
                }
                if read_marker(member.store.as_ref(), &path).await?.as_deref() != Some(expected.as_slice()) {
                    return Err(error(format!("pool member {id} identity initialization did not persist")));
                }
            },
            None => return Err(error(format!("pool member {id} identity marker is missing; the disk or backend may have been replaced"))),
        }
        if !record.initialized {
            let member_id = id.to_owned();
            self.catalog(move |catalog| catalog.mark_member_initialized(&member_id)).await?;
        }
        *checked = true;
        Ok(())
    }

    async fn visible(&self, location: &Path) -> object_store::Result<Option<Entry>> {
        let key = location.to_string();
        let lookup = self.catalog(move |catalog| catalog.lookup(&key)).await?;
        if let Some(entry) = lookup.entry {
            return Ok(Some(entry));
        }
        if lookup.legacy_hidden {
            return Ok(None);
        }
        // This is an explicit compatibility path for segments written to the
        // local authority before pooling was enabled. It is never a fallback
        // after a mapped member read fails.
        match self.inner.authority.head(location).await {
            Ok(meta) => Ok(Some(Entry {
                physical: Physical::Legacy { key: location.to_string(), e_tag: meta.e_tag.clone(), version: meta.version.clone() },
                size: meta.size, last_modified_secs: meta.last_modified.timestamp(),
                last_modified_nanos: meta.last_modified.timestamp_subsec_nanos(),
                e_tag: meta.e_tag, version: meta.version, sha256: None, attributes: Vec::new(),
            })),
            Err(object_store::Error::NotFound { .. }) => Ok(None),
            Err(error) => Err(error),
        }
    }

    fn check_mode(location: &Path, mode: &PutMode, previous: Option<&Entry>) -> object_store::Result<()> {
        match mode {
            PutMode::Create if previous.is_some() => Err(object_store::Error::AlreadyExists {
                path: location.to_string(), source: "pool object already exists".into(),
            }),
            PutMode::Update(version) => {
                if version.version.is_some() {
                    return Err(unsupported("pool conditional updates use logical ETags, not object versions"));
                }
                let previous = previous.ok_or_else(|| precondition(location, "pool update target does not exist"))?;
                if version.e_tag.is_none() || version.e_tag != previous.e_tag {
                    return Err(precondition(location, "pool update ETag did not match"));
                }
                Ok(())
            },
            _ => Ok(()),
        }
    }

    async fn reserve(&self, location: &Path, size: u64, target: Option<String>) -> object_store::Result<Upload> {
        if size > MAX_TRANSFER_BYTES {
            return Err(unsupported(format!("pool segment exceeds the {MAX_TRANSFER_BYTES}-byte transfer bound")));
        }
        if let Some(target) = &target { self.member(target)?; }
        let members = self.inner.members.clone();
        let key = location.to_string();
        self.catalog(move |catalog| catalog.reserve(&key, size, &members, target.as_deref())).await
    }

    async fn put_reserved(
        &self, location: &Path, payload: PutPayload, mut options: PutOptions,
        upload: Upload, previous: Option<Entry>, preserve: Option<Entry>,
        cancelled: Arc<AtomicBool>, verify: bool,
    ) -> object_store::Result<PutResult> {
        let outcome = async {
            if u64::try_from(payload.content_length()).ok() != Some(upload.size) {
                return Err(error("pool payload length does not match its reservation"));
            }
            self.ensure_member(&upload.member_id, true).await?;
            if cancelled.load(Ordering::Acquire) {
                return Err(error("pool publication was cancelled before upload"));
            }
            let digest_payload = payload.clone();
            let sha256: [u8; 32] = tokio::task::spawn_blocking(move || {
                let mut hash = Sha256::new();
                for bytes in digest_payload { hash.update(&bytes); }
                hash.finalize().into()
            }).await.map_err(|e| error(format!("pool checksum task failed: {e}")))?;
            if let Some(expected) = preserve.as_ref().and_then(|entry| entry.sha256)
                && expected != sha256 {
                return Err(error("pool source checksum does not match the placement catalog"));
            }
            let id = upload.id.clone();
            self.catalog(move |catalog| catalog.mark_started(&id)).await?;
            options.mode = PutMode::Create;
            let member = self.member(&upload.member_id)?;
            let logical_attributes = store_attributes(&options.attributes)?;
            if member.local_path.is_some() {
                // The local object store rejects attributes. Preserve them in
                // the authoritative catalog and expose the same logical view
                // before and after moving this object to another member.
                options.attributes = Attributes::default();
            }
            let path = Path::from(upload.physical_key.clone());
            let written = member.store.put_opts(&path, payload, options).await?;
            let physical = Physical::Member {
                member_id: upload.member_id.clone(), key: upload.physical_key.clone(),
                e_tag: written.e_tag, version: written.version,
            };
            let meta = member.store.head(&path).await?;
            if meta.size != upload.size {
                return Err(error("pool member stored a different object length"));
            }
            let (physical_etag, _) = physical.versions();
            if physical_etag.is_some() && physical_etag != meta.e_tag {
                return Err(error("pool member object changed before publication"));
            }
            if verify {
                let actual = self.hash_physical(&physical, upload.size).await?;
                if actual != sha256 {
                    return Err(error("pool migration destination checksum verification failed"));
                }
            }
            if cancelled.load(Ordering::Acquire) {
                return Err(error("pool publication was cancelled before placement commit"));
            }
            let entry = match preserve {
                Some(mut entry) => { entry.physical = physical; entry.sha256 = Some(sha256); entry },
                None => {
                    let now = Utc::now();
                    Entry { physical, size: upload.size, last_modified_secs: now.timestamp(),
                        last_modified_nanos: now.timestamp_subsec_nanos(), e_tag: Some(Uuid::new_v4().to_string()),
                        version: None, sha256: Some(sha256), attributes: logical_attributes }
                },
            };
            let result = PutResult { e_tag: entry.e_tag.clone(), version: entry.version.clone(), extensions: Extensions::default() };
            let id = upload.id.clone();
            let garbage = self.catalog(move |catalog| catalog.publish(&id, &entry, previous.as_ref())).await?;
            if let Some(garbage) = garbage
                && let Err(error) = self.cleanup_garbage(&garbage).await {
                    tracing::warn!(%error, location = %location, "pool placement committed; old object cleanup remains pending");
                }
            Ok(result)
        }.await;
        if outcome.is_err()
            && let Err(cleanup) = self.cleanup_upload(&upload).await {
                tracing::warn!(%cleanup, upload = %upload.id, "pool upload cleanup remains pending and capacity stays reserved");
            }
        outcome
    }

    async fn put_owned(
        &self, location: Path, payload: PutPayload, options: PutOptions, cancelled: Arc<AtomicBool>,
    ) -> object_store::Result<PutResult> {
        let _guard = self.key_lock(&location).write_owned().await;
        let previous = self.visible(&location).await?;
        Self::check_mode(&location, &options.mode, previous.as_ref())?;
        let size = u64::try_from(payload.content_length()).map_err(|_| error("pool payload length overflow"))?;
        let upload = self.reserve(&location, size, None).await?;
        self.put_reserved(&location, payload, options, upload, previous, None, cancelled, false).await
    }

    async fn physical_get(&self, physical: &Physical, options: GetOptions) -> object_store::Result<GetResult> {
        match physical {
            Physical::Member { member_id, key, .. } => {
                self.ensure_member(member_id, false).await?;
                match self.member(member_id)?.store.get_opts(&catalog_path(key)?, options).await {
                    Err(object_store::Error::NotFound { .. }) => Err(error(format!("mapped pool object is missing on member {member_id}; refusing fallback"))),
                    other => other,
                }
            },
            Physical::Legacy { key, .. } => self.inner.authority.get_opts(&catalog_path(key)?, options).await,
        }
    }

    async fn read_payload(&self, entry: &Entry) -> object_store::Result<(PutPayload, Attributes)> {
        if entry.size > MAX_TRANSFER_BYTES {
            return Err(unsupported("pool migration or copy exceeds the bounded segment transfer size"));
        }
        let mut bytes = Vec::new();
        let attributes = load_attributes(&entry.attributes);
        let mut offset = 0;
        if entry.size == 0 {
            let (e_tag, version) = entry.physical.versions();
            let result = self.physical_get(&entry.physical, GetOptions {
                head: true, if_match: e_tag, version, ..Default::default()
            }).await?;
            if result.meta.size != 0 { return Err(error("empty pool source changed during transfer")); }
        }
        while offset < entry.size {
            let end = entry.size.min(offset + READ_CHUNK_BYTES);
            let (e_tag, version) = entry.physical.versions();
            let result = self.physical_get(&entry.physical, GetOptions {
                range: Some(GetRange::Bounded(offset..end)), if_match: e_tag, version, ..Default::default()
            }).await?;
            if result.meta.size != entry.size { return Err(error("pool source length changed during transfer")); }
            let chunk = result.bytes().await?;
            if chunk.len() as u64 != end - offset { return Err(error("pool source returned a short range")); }
            bytes.push(chunk);
            offset = end;
        }
        Ok((PutPayload::from_iter(bytes), attributes))
    }

    async fn hash_physical(&self, physical: &Physical, size: u64) -> object_store::Result<[u8; 32]> {
        let mut hash = Sha256::new();
        let mut offset = 0;
        while offset < size {
            let end = size.min(offset + READ_CHUNK_BYTES);
            let (e_tag, version) = physical.versions();
            let result = self.physical_get(physical, GetOptions {
                range: Some(GetRange::Bounded(offset..end)), if_match: e_tag, version, ..Default::default()
            }).await?;
            if result.meta.size != size { return Err(error("pool verification length mismatch")); }
            let bytes = result.bytes().await?;
            if bytes.len() as u64 != end - offset { return Err(error("pool verification returned a short range")); }
            hash.update(&bytes);
            offset = end;
        }
        Ok(hash.finalize().into())
    }

    fn validate_physical(&self, physical: &Physical) -> object_store::Result<()> {
        match physical {
            Physical::Member { key, .. } => {
                let prefix = format!(".zerofs-pool/{}/objects/", self.inner.catalog.identity.pool_id);
                let suffix = key.strip_prefix(&prefix).ok_or_else(|| error("pool cleanup key is outside its owned namespace"))?;
                Uuid::parse_str(suffix).map_err(|_| error("pool cleanup key has an invalid object id"))?;
            },
            Physical::Legacy { key, .. } if !self.segment(&catalog_path(key)?) => {
                return Err(error("pool legacy cleanup key is not a canonical segment"));
            },
            _ => {},
        }
        Ok(())
    }

    async fn remove_physical(&self, physical: &Physical) -> object_store::Result<()> {
        self.validate_physical(physical)?;
        let result = match physical {
            Physical::Member { member_id, key, .. } => {
                self.ensure_member(member_id, true).await?;
                self.member(member_id)?.store.delete(&catalog_path(key)?).await
            },
            Physical::Legacy { key, e_tag, version } => {
                let path = catalog_path(key)?;
                // A legacy key is not a new unique pool key. Do not remove it
                // if an out-of-band writer replaced it during migration.
                match self.inner.authority.get_opts(&path, GetOptions {
                    head: true, if_match: e_tag.clone(), version: version.clone(), ..Default::default()
                }).await {
                    Ok(_) => self.inner.authority.delete(&path).await,
                    Err(object_store::Error::NotFound { .. }) => Ok(()),
                    Err(error) => Err(error),
                }
            },
        };
        match result { Ok(()) | Err(object_store::Error::NotFound { .. }) => Ok(()), Err(error) => Err(error) }
    }

    async fn cleanup_upload(&self, upload: &Upload) -> object_store::Result<()> {
        // Consult the durable state; a caller may have been cancelled just
        // after the started flag was committed by a blocking catalog task.
        let id = upload.id.clone();
        let current = self.catalog(move |catalog| {
            Ok(catalog.recovery()?.0.into_iter().find(|pending| pending.id == id))
        }).await?;
        let Some(current) = current else { return Ok(()) };
        if current.started {
            self.remove_physical(&Physical::Member { member_id: current.member_id, key: current.physical_key, e_tag: None, version: None }).await?;
        }
        let id = current.id;
        self.catalog(move |catalog| catalog.forget_upload(&id)).await
    }

    async fn cleanup_garbage(&self, garbage: &Garbage) -> object_store::Result<()> {
        self.remove_physical(&garbage.entry.physical).await?;
        let id = garbage.id.clone();
        self.catalog(move |catalog| catalog.forget_garbage(&id)).await
    }

    /// Recovery is bounded to operations recorded before a crash. It never
    /// scans member contents or requires an idle remote member to be online.
    async fn recover_pending(&self) -> object_store::Result<()> {
        let (mut uploads, mut garbage) = self.catalog(Catalog::recovery).await?;
        // Recover local debts first, and bound startup work even when a remote
        // member has thousands of interrupted operations. A timeout leaves the
        // durable debt intact; no detached cleanup worker can race later use.
        let is_local = |id: &str| self.member(id).is_ok_and(|member| member.local_path.is_some());
        uploads.sort_by_key(|upload| !is_local(&upload.member_id));
        garbage.sort_by_key(|garbage| garbage.entry.physical.member_id().is_some_and(|id| !is_local(id)));
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(3);
        let mut unavailable = BTreeSet::new();
        for upload in uploads {
            if unavailable.contains(&upload.member_id) { continue; }
            if tokio::time::Instant::now() >= deadline { break; }
            let path = catalog_path(&upload.location)?;
            let _guard = self.key_lock(&path).write_owned().await;
            let attempt_deadline = deadline.min(tokio::time::Instant::now() + std::time::Duration::from_secs(1));
            match tokio::time::timeout_at(attempt_deadline, self.cleanup_upload(&upload)).await {
                Ok(Ok(())) => {},
                Ok(Err(error)) => {
                    unavailable.insert(upload.member_id.clone());
                    tracing::warn!(%error, upload = %upload.id, "pool recovery retains an unpublished object and its reservation");
                },
                Err(_) => {
                    unavailable.insert(upload.member_id.clone());
                    tracing::warn!(upload = %upload.id, "pool recovery timed out; unpublished object and reservation remain recorded");
                },
            }
        }
        for garbage in garbage {
            let member_id = garbage.entry.physical.member_id().unwrap_or("authority").to_owned();
            if unavailable.contains(&member_id) { continue; }
            if tokio::time::Instant::now() >= deadline { break; }
            let path = catalog_path(&garbage.location)?;
            let _guard = self.key_lock(&path).write_owned().await;
            let attempt_deadline = deadline.min(tokio::time::Instant::now() + std::time::Duration::from_secs(1));
            match tokio::time::timeout_at(attempt_deadline, self.cleanup_garbage(&garbage)).await {
                Ok(Ok(())) => {},
                Ok(Err(error)) => {
                    unavailable.insert(member_id);
                    tracing::warn!(%error, object = %garbage.id, "pool recovery retains superseded object cleanup debt");
                },
                Err(_) => {
                    unavailable.insert(member_id);
                    tracing::warn!(object = %garbage.id, "pool recovery timed out; superseded object cleanup remains recorded");
                },
            }
        }
        Ok(())
    }

    pub async fn status(&self) -> object_store::Result<PoolStatus> {
        let members = self.inner.members.clone();
        self.catalog(move |catalog| catalog.status(&members)).await
    }

    /// Returns mapped segments and visible legacy segments in the authority.
    /// `authority` is reserved as the member id for those legacy objects.
    pub async fn placements(&self) -> object_store::Result<Vec<PoolPlacement>> {
        let mut placements = self.catalog(Catalog::placements).await?;
        let known: BTreeSet<_> = placements.iter().map(|entry| entry.location.clone()).collect();
        let prefix = self.inner.authority_prefix.clone().join("segments");
        let prefix_string = prefix.to_string();
        let snapshot = self.catalog(move |catalog| catalog.snapshot(Some(&prefix_string))).await?;
        let mut legacy = self.inner.authority.list(Some(&prefix));
        while let Some(meta) = legacy.try_next().await? {
            if self.segment(&meta.location) && !known.contains(&meta.location)
                && !snapshot.hidden.contains(meta.location.as_ref()) {
                placements.push(PoolPlacement { location: meta.location, member_id: "authority".to_owned(), size: meta.size });
            }
        }
        Ok(placements)
    }

    pub async fn migrate(&self, location: &Path, target_member_id: &str) -> object_store::Result<()> {
        if !self.segment(location) { return Err(unsupported("only canonical segment objects can be migrated into the pool")); }
        self.member(target_member_id)?;
        let pool = self.clone();
        let location = location.clone();
        let target = target_member_id.to_owned();
        let cancelled = Arc::new(AtomicBool::new(false));
        let _cancel = CancelOnDrop(cancelled.clone());
        tokio::spawn(async move {
            let _guard = pool.key_lock(&location).write_owned().await;
            let previous = pool.visible(&location).await?.ok_or_else(|| missing(&location))?;
            if previous.physical.member_id() == Some(target.as_str()) { return Ok(()) }
            let upload = pool.reserve(&location, previous.size, Some(target)).await?;
            let (payload, attributes) = match pool.read_payload(&previous).await {
                Ok(payload) => payload,
                Err(error) => { let _ = pool.cleanup_upload(&upload).await; return Err(error) },
            };
            let mut options = PutOptions::from(PutMode::Create);
            options.attributes = attributes;
            pool.put_reserved(&location, payload, options, upload, Some(previous.clone()), Some(previous), cancelled, true).await?;
            Ok(())
        }).await.map_err(|e| error(format!("pool migration task failed: {e}")))?
    }

    async fn delete_owned(&self, location: Path) -> object_store::Result<Path> {
        let _guard = self.key_lock(&location).write_owned().await;
        let Some(previous) = self.visible(&location).await? else { return Ok(location) };
        let key = location.to_string();
        let garbage = self.catalog(move |catalog| catalog.retire(&key, &previous)).await?;
        self.cleanup_garbage(&garbage).await?;
        Ok(location)
    }
}

async fn read_marker(store: &dyn ObjectStore, path: &Path) -> object_store::Result<Option<Vec<u8>>> {
    match store.get(path).await {
        Ok(result) => {
            if result.meta.size > MAX_MARKER_BYTES { return Err(error("pool identity marker is oversized")); }
            Ok(Some(result.bytes().await?.to_vec()))
        },
        Err(object_store::Error::NotFound { .. }) => Ok(None),
        Err(error) => Err(error),
    }
}

fn hold_read_lease(result: GetResult, meta: ObjectMeta, guard: OwnedRwLockReadGuard<()>) -> GetResult {
    let range = result.range.clone();
    let attributes = result.attributes.clone();
    let extensions = result.extensions.clone();
    let payload = stream::unfold((result.into_stream(), guard), |(mut stream, guard)| async move {
        stream.next().await.map(|chunk| (chunk, (stream, guard)))
    }).boxed();
    GetResult { payload: GetResultPayload::Stream(payload), meta, range, attributes, extensions }
}

#[async_trait]
impl ObjectStore for PoolObjectStore {
    async fn put_opts(&self, location: &Path, payload: PutPayload, options: PutOptions) -> object_store::Result<PutResult> {
        if self.internal(location) { return Err(unsupported("pool identity metadata is reserved")); }
        if !self.segment(location) { return self.inner.authority.put_opts(location, payload, options).await }
        let pool = self.clone();
        let location = location.clone();
        let cancelled = Arc::new(AtomicBool::new(false));
        let _cancel = CancelOnDrop(cancelled.clone());
        tokio::spawn(async move { pool.put_owned(location, payload, options, cancelled).await })
            .await.map_err(|e| error(format!("pool upload task failed: {e}")))?
    }

    async fn put_multipart_opts(&self, location: &Path, options: PutMultipartOptions) -> object_store::Result<Box<dyn MultipartUpload>> {
        if self.internal(location) { return Err(unsupported("pool identity metadata is reserved")); }
        if !self.segment(location) { return self.inner.authority.put_multipart_opts(location, options).await }
        let capability = options.extensions.get::<ConditionalMultipartCreate>().cloned()
            .ok_or_else(|| unsupported("pool multipart requires a declared conditional-create segment length"))?;
        let length = capability.declared_payload_len()
            .ok_or_else(|| unsupported("pool multipart requires a declared payload length"))?;
        {
            let _guard = self.key_lock(location).write_owned().await;
            let previous = self.visible(location).await?;
            Self::check_mode(location, &PutMode::Create, previous.as_ref())?;
        }
        let upload = self.reserve(location, length, None).await?;
        capability.acknowledge();
        Ok(Box::new(PoolMultipartUpload {
            pool: self.clone(), location: location.clone(), upload: Some(upload), options: Some(options),
            parts: Vec::new(), received: 0, completed: Arc::new(StdMutex::new(Vec::new())),
        }))
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> object_store::Result<GetResult> {
        if self.internal(location) { return Err(missing(location)); }
        if !self.segment(location) { return self.inner.authority.get_opts(location, options).await }
        if options.version.is_some() { return Err(unsupported("pool GET does not expose historical object versions")); }
        let guard = self.key_lock(location).read_owned().await;
        let entry = self.visible(location).await?.ok_or_else(|| missing(location))?;
        let meta = entry.meta(location)?;
        options.check_preconditions(&meta)?;
        let (e_tag, version) = entry.physical.versions();
        let mut result = self.physical_get(&entry.physical, GetOptions {
            range: options.range, head: options.head, if_match: e_tag, version,
            extensions: options.extensions, ..Default::default()
        }).await?;
        if result.meta.size != entry.size { return Err(error("mapped pool object has an unexpected length")); }
        result.attributes = load_attributes(&entry.attributes);
        Ok(hold_read_lease(result, meta, guard))
    }

    fn delete_stream(&self, locations: BoxStream<'static, object_store::Result<Path>>) -> BoxStream<'static, object_store::Result<Path>> {
        let pool = self.clone();
        locations.then(move |location| {
            let pool = pool.clone();
            async move {
                let location = location?;
                if pool.internal(&location) { return Err(unsupported("pool identity metadata is reserved")); }
                if !pool.segment(&location) {
                    pool.inner.authority.delete(&location).await?;
                    return Ok(location);
                }
                tokio::spawn(async move { pool.delete_owned(location).await }).await
                    .map_err(|e| error(format!("pool delete task failed: {e}")))?
            }
        }).boxed()
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        let pool = self.clone();
        let prefix = prefix.cloned();
        stream::once(async move {
            let catalog_prefix = prefix.as_ref().map(ToString::to_string);
            let snapshot = pool.catalog(move |catalog| catalog.snapshot(catalog_prefix.as_deref())).await?;
            let mut mapped = Vec::new();
            for (key, entry) in &snapshot.entries {
                let path = catalog_path(key)?;
                if has_prefix(&path, prefix.as_ref()) { mapped.push(entry.meta(&path)?); }
            }
            let filter_pool = pool.clone();
            let authority = pool.inner.authority.list(prefix.as_ref()).try_filter(move |meta| {
                futures::future::ready(!filter_pool.internal(&meta.location)
                    && !snapshot.entries.contains_key(meta.location.as_ref())
                    && !snapshot.hidden.contains(meta.location.as_ref()))
            });
            Ok::<_, object_store::Error>(authority.chain(stream::iter(mapped.into_iter().map(Ok))).boxed())
        }).try_flatten().boxed()
    }

    fn list_with_offset(&self, prefix: Option<&Path>, offset: &Path) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        let offset = offset.clone();
        self.list(prefix).try_filter(move |meta| futures::future::ready(meta.location > offset)).boxed()
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        let mut stream = self.list(prefix);
        let prefix = prefix.cloned().unwrap_or_default();
        let mut objects = Vec::new();
        let mut common_prefixes = BTreeSet::new();
        while let Some(meta) = stream.try_next().await? {
            let child_prefix = {
                let mut relative = meta.location.prefix_match(&prefix).ok_or_else(|| error("pool listing escaped its prefix"))?;
                match relative.next() {
                    Some(first) if relative.next().is_some() => Some(prefix.clone().join(first)),
                    _ => None,
                }
            };
            if let Some(child_prefix) = child_prefix { common_prefixes.insert(child_prefix); }
            else { objects.push(meta); }
        }
        Ok(ListResult { common_prefixes: common_prefixes.into_iter().collect(), objects, extensions: Extensions::default() })
    }

    async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> object_store::Result<()> {
        if self.internal(from) || self.internal(to) { return Err(unsupported("pool identity metadata is reserved")); }
        if !self.segment(from) && !self.segment(to) { return self.inner.authority.copy_opts(from, to, options).await }
        let result = self.get(from).await?;
        if result.meta.size > MAX_TRANSFER_BYTES { return Err(unsupported("pool copy exceeds the segment transfer bound")); }
        let length = result.meta.size;
        let attributes = result.attributes.clone();
        let mut stream = result.into_stream();
        let mut chunks = Vec::new();
        let mut received = 0_u64;
        while let Some(chunk) = stream.try_next().await? {
            received = received.checked_add(chunk.len() as u64).ok_or_else(|| error("pool copy length overflow"))?;
            if received > length { return Err(error("pool copy source grew during transfer")); }
            chunks.push(chunk);
        }
        drop(stream);
        if received != length { return Err(error("pool copy source returned too few bytes")); }
        let mut put = PutOptions::from(match options.mode { CopyMode::Create => PutMode::Create, CopyMode::Overwrite => PutMode::Overwrite });
        put.attributes = attributes;
        put.extensions = options.extensions;
        self.put_opts(to, PutPayload::from_iter(chunks), put).await?;
        Ok(())
    }

    async fn rename_opts(&self, from: &Path, to: &Path, options: RenameOptions) -> object_store::Result<()> {
        if self.internal(from) || self.internal(to) { return Err(unsupported("pool identity metadata is reserved")); }
        if self.segment(from) || self.segment(to) { return Err(unsupported("immutable pool segment keys cannot be renamed; use copy and delete")); }
        self.inner.authority.rename_opts(from, to, options).await
    }
}

struct PoolMultipartUpload {
    pool: PoolObjectStore,
    location: Path,
    upload: Option<Upload>,
    options: Option<PutMultipartOptions>,
    parts: Vec<Bytes>,
    received: u64,
    completed: Arc<StdMutex<Vec<bool>>>,
}

impl fmt::Debug for PoolMultipartUpload {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PoolMultipartUpload").field("location", &self.location).field("received", &self.received).finish_non_exhaustive()
    }
}

#[async_trait]
impl MultipartUpload for PoolMultipartUpload {
    fn put_part(&mut self, data: PutPayload) -> UploadPart {
        let Some(upload) = &self.upload else { return Box::pin(async { Err(error("pool multipart is already terminal")) }) };
        let Some(total) = self.received.checked_add(data.content_length() as u64) else { return Box::pin(async { Err(error("pool multipart length overflow")) }) };
        if total > upload.size { return Box::pin(async { Err(error("pool multipart exceeds its declared length")) }) }
        self.received = total;
        self.parts.extend(data);
        let mut completed = self.completed.lock().unwrap();
        let index = completed.len();
        completed.push(false);
        drop(completed);
        let completed = self.completed.clone();
        Box::pin(async move { completed.lock().unwrap()[index] = true; Ok(()) })
    }

    async fn complete(&mut self) -> object_store::Result<PutResult> {
        let upload = self.upload.as_ref().ok_or_else(|| error("pool multipart is already terminal"))?;
        if self.received != upload.size || self.completed.lock().unwrap().iter().any(|done| !done) {
            return Err(error("pool multipart is incomplete or does not match its declared length"));
        }
        let upload = self.upload.take().expect("checked above");
        let options = self.options.take().ok_or_else(|| error("pool multipart options are missing"))?;
        let payload = PutPayload::from_iter(std::mem::take(&mut self.parts));
        let pool = self.pool.clone();
        let location = self.location.clone();
        let cancelled = Arc::new(AtomicBool::new(false));
        let _cancel = CancelOnDrop(cancelled.clone());
        tokio::spawn(async move {
            let _guard = pool.key_lock(&location).write_owned().await;
            let previous = pool.visible(&location).await?;
            if let Err(error) = PoolObjectStore::check_mode(&location, &PutMode::Create, previous.as_ref()) {
                let _ = pool.cleanup_upload(&upload).await;
                return Err(error);
            }
            let mut put = PutOptions::from(PutMode::Create);
            put.attributes = options.attributes;
            put.tags = options.tags;
            put.extensions = options.extensions;
            pool.put_reserved(&location, payload, put, upload, previous, None, cancelled, false).await
        }).await.map_err(|e| error(format!("pool multipart completion task failed: {e}")))?
    }

    async fn abort(&mut self) -> object_store::Result<()> {
        self.parts.clear();
        let Some(upload) = self.upload.take() else { return Ok(()) };
        self.pool.cleanup_upload(&upload).await
    }
}

impl Drop for PoolMultipartUpload {
    fn drop(&mut self) {
        if let Some(upload) = self.upload.take()
            && let Ok(runtime) = tokio::runtime::Handle::try_current() {
                let pool = self.pool.clone();
                runtime.spawn(async move {
                    if let Err(error) = pool.cleanup_upload(&upload).await {
                        tracing::warn!(%error, "abandoned pool multipart reservation remains recoverable");
                    }
                });
            }
    }
}
