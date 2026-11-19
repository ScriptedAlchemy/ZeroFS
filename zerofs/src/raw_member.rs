//! `object_store` adapter over one claimed raw-device [`RawStore`].
//!
//! Every mutation appends to the device log and then flushes, inside one write
//! lock, before it returns. An `Ok` from PUT, DELETE, COPY or RENAME therefore
//! means both label slots on the device record it. Readers take the same lock
//! shared, so they never observe a record that is not yet durable. A failed
//! append or flush poisons the store, and every later call fails with
//! [`RawMemberError::Poisoned`]. To recover, reopen the device and build a new
//! adapter.
//!
//! Space is admitted exactly. Mutations are serialised by the write lock, and
//! each one checks its exact log cost against the free space at that moment:
//! one header block plus the payload rounded up to whole blocks, from
//! [`RawStore::put_cost`] and [`RawStore::delete_cost`].
//!
//! `reserve_bytes` is a physical floor that no mutation may cross, deletes
//! included. Instead, every live object funds its own future delete: the
//! adapter keeps `delete_cost() * live_objects` (the delete debt) free above
//! the reserve, and a mutation is admitted only if
//!
//! ```text
//! appended bytes + delete_cost() * live objects afterwards <= free - reserve
//! ```
//!
//! A create therefore pays for its record and its eventual tombstone, an
//! overwrite pays only for its record, and a delete spends exactly the debt
//! it releases. Once a store meets the invariant, deleting a live object never
//! fails for space. A store opened already below it refuses creates and
//! reports deletes it cannot fund as ENOSPC, leaving the debt in place, rather
//! than crossing the floor. A refusal is reported as
//! `io::ErrorKind::StorageFull` wrapping [`RawMemberError::NoSpace`], and
//! nothing is written.
//!
//! The log is append-only. Overwrites, updates and deletes add records and
//! never reclaim the space of the records they supersede: see
//! [`RawCapacity::appended_bytes`].
//!
//! Every read verifies the SHA-256 of the whole payload before returning any
//! byte, including ranged reads. The payload is hashed in bounded chunks and
//! only the requested ranges are retained, outside the store lock.

use async_trait::async_trait;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use futures::stream::{self, BoxStream};
use futures::{FutureExt, StreamExt};
use object_store::path::Path;
use object_store::{
    CopyMode, CopyOptions, Error, GetOptions, GetRange, GetResult, GetResultPayload, ListResult,
    MultipartUpload, ObjectMeta, ObjectStore, PutMode, PutMultipartOptions, PutOptions, PutPayload,
    PutResult, RenameOptions, RenameTargetMode, Result, UploadPart,
};
use std::any::Any;
use std::collections::BTreeSet;
use std::fmt;
use std::io;
use std::ops::Range;
use std::sync::{Arc, Mutex, RwLock, RwLockReadGuard, RwLockWriteGuard};
use zerofs_raw_device::{
    BLOCK_SIZE, Identity, MAX_OBJECT_LEN, ObjectHandle, ObjectInfo, RawReader, RawStore,
};

const STORE: &str = "RawMemberStore";
/// Deletes from one `delete_stream` batch share a single flush.
const DELETE_BATCH: usize = 64;

/// Why a raw member operation failed.
#[derive(Debug)]
pub enum RawMemberError {
    /// The mutation does not fit above the reserve. `needed` is its appended
    /// bytes plus the delete debt left afterwards; `available` is free space
    /// minus the reserve.
    NoSpace {
        needed: u64,
        available: u64,
        reserve: u64,
    },
    /// An earlier append or flush failed; the device state past the last
    /// durable checkpoint is unknown. Reopen the device.
    Poisoned,
    /// The object store path cannot be a raw key (empty or too long).
    InvalidKey(String),
    /// The payload exceeds [`MAX_OBJECT_LEN`].
    TooLarge { len: u64 },
    /// The request needs a feature the raw format does not store.
    Unsupported(&'static str),
    /// A thread panicked while holding the store lock.
    LockPoisoned,
    /// The raw store reported an error.
    Device(zerofs_raw_device::Error),
}

impl fmt::Display for RawMemberError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NoSpace {
                needed,
                available,
                reserve,
            } => write!(
                f,
                "raw member full: need {needed} bytes including delete debt, \
                 {available} free above the {reserve}-byte reserve"
            ),
            Self::Poisoned => write!(f, "raw member poisoned by an earlier write failure"),
            Self::InvalidKey(reason) => write!(f, "invalid raw member key: {reason}"),
            Self::TooLarge { len } => write!(
                f,
                "object of {len} bytes exceeds the raw member limit of {MAX_OBJECT_LEN}"
            ),
            Self::Unsupported(what) => write!(f, "raw member does not support {what}"),
            Self::LockPoisoned => write!(f, "raw member lock poisoned by a panic"),
            Self::Device(err) => write!(f, "{err}"),
        }
    }
}

impl std::error::Error for RawMemberError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Device(err) => Some(err),
            _ => None,
        }
    }
}

impl From<zerofs_raw_device::Error> for RawMemberError {
    fn from(err: zerofs_raw_device::Error) -> Self {
        match err {
            zerofs_raw_device::Error::Poisoned => Self::Poisoned,
            other => Self::Device(other),
        }
    }
}

impl RawMemberError {
    fn kind(&self) -> io::ErrorKind {
        use zerofs_raw_device::Error as Raw;
        match self {
            Self::NoSpace { .. } | Self::Device(Raw::NoSpace { .. }) => io::ErrorKind::StorageFull,
            Self::InvalidKey(_) | Self::TooLarge { .. } => io::ErrorKind::InvalidInput,
            Self::Unsupported(_) => io::ErrorKind::Unsupported,
            Self::Poisoned | Self::LockPoisoned => io::ErrorKind::Other,
            Self::Device(Raw::Io(inner)) => inner.kind(),
            Self::Device(Raw::InvalidArgument(_)) => io::ErrorKind::InvalidInput,
            Self::Device(_) => io::ErrorKind::InvalidData,
        }
    }

    /// The `object_store` form. The source chain holds an `io::Error` of the
    /// matching kind whose inner error is this value, so ENOSPC detection by
    /// kind works and callers can downcast to [`RawMemberError`].
    fn into_store(self) -> Error {
        if let Self::Unsupported(_) = self {
            return Error::NotSupported {
                source: Box::new(self),
            };
        }
        Error::Generic {
            store: STORE,
            source: Box::new(io::Error::new(self.kind(), self)),
        }
    }
}

fn store_err(err: impl Into<RawMemberError>) -> Error {
    err.into().into_store()
}

/// A snapshot of the member's physical space, in bytes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RawCapacity {
    /// Size of the store's region (the whole device, or one P3 region).
    pub device_bytes: u64,
    /// Region size minus the two label blocks.
    pub log_bytes: u64,
    /// Log bytes appended so far, including superseded and delete records.
    pub appended_bytes: u64,
    /// Log bytes not yet appended.
    pub free_bytes: u64,
    /// Physical floor no mutation crosses.
    pub reserve_bytes: u64,
    /// `delete_cost() * live_objects`: kept free so every live object can be
    /// deleted without touching the reserve.
    pub delete_debt_bytes: u64,
    /// Room new appends may consume: `free - reserve - delete debt`, floored
    /// at zero.
    pub admissible_bytes: u64,
    /// Live objects.
    pub live_objects: u64,
}

impl RawCapacity {
    /// Largest payload a put could store right now, or `None` when not even
    /// an empty object fits.
    /// A new key also funds its own delete; an overwrite of an existing key
    /// can be one tombstone larger.
    #[cfg(test)]
    pub fn max_put_payload(&self) -> Option<u64> {
        let room = self
            .admissible_bytes
            .checked_sub(BLOCK_SIZE + RawStore::delete_cost())?;
        Some((room / BLOCK_SIZE * BLOCK_SIZE).min(MAX_OBJECT_LEN))
    }
}

/// An [`ObjectStore`] over one raw member. Clones share the store.
#[derive(Clone)]
pub struct RawMemberStore {
    inner: Arc<Inner>,
}

// Field order is drop order: the store and reader (and with them the device
// descriptor) are released before the claim.
struct Inner {
    store: RwLock<RawStore>,
    reader: RawReader,
    identity: Identity,
    reserve: u64,
    _claim: Box<dyn Any + Send + Sync>,
}

impl fmt::Debug for RawMemberStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct(STORE)
            .field("identity", &self.inner.identity)
            .field("reserve", &self.inner.reserve)
            .finish_non_exhaustive()
    }
}

impl fmt::Display for RawMemberStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{STORE}({})", self.inner.identity)
    }
}

impl RawMemberStore {
    /// Wrap an opened store. `claim` is any guard that keeps the device
    /// exclusively claimed; it is held until the last clone is dropped and
    /// released after the store. `reserve_bytes` is rounded up to whole
    /// blocks.
    pub fn new<C: Send + Sync + 'static>(
        store: RawStore,
        claim: C,
        reserve_bytes: u64,
    ) -> std::result::Result<Self, RawMemberError> {
        if store.is_poisoned() {
            return Err(RawMemberError::Poisoned);
        }
        let reserve =
            reserve_bytes
                .checked_next_multiple_of(BLOCK_SIZE)
                .ok_or(RawMemberError::Device(
                    zerofs_raw_device::Error::InvalidArgument("reserve overflows"),
                ))?;
        Ok(Self {
            inner: Arc::new(Inner {
                reader: store.reader(),
                identity: store.identity(),
                store: RwLock::new(store),
                reserve,
                _claim: Box::new(claim),
            }),
        })
    }

    pub fn identity(&self) -> Identity {
        self.inner.identity
    }

    /// Exact log bytes a put of `value_len` bytes appends, or `None` if that
    /// overflows. Excludes the delete debt a new key adds.
    pub const fn put_cost(value_len: u64) -> Option<u64> {
        RawStore::put_cost(value_len)
    }

    /// Exact log bytes one delete consumes.
    pub const fn delete_cost() -> u64 {
        RawStore::delete_cost()
    }

    pub fn capacity(&self) -> std::result::Result<RawCapacity, RawMemberError> {
        let store = self.inner.read()?;
        if store.is_poisoned() {
            return Err(RawMemberError::Poisoned);
        }
        let geometry = store.geometry();
        let live = store.len() as u64;
        let debt = delete_debt(live);
        Ok(RawCapacity {
            device_bytes: geometry.device_size,
            log_bytes: geometry.device_size - geometry.log_start,
            appended_bytes: geometry.tail - geometry.log_start,
            free_bytes: geometry.free,
            reserve_bytes: self.inner.reserve,
            delete_debt_bytes: debt,
            admissible_bytes: geometry
                .free
                .saturating_sub(self.inner.reserve)
                .saturating_sub(debt),
            live_objects: live,
        })
    }

    async fn blocking<T: Send + 'static>(
        &self,
        work: impl FnOnce(&Inner) -> Result<T> + Send + 'static,
    ) -> Result<T> {
        let inner = Arc::clone(&self.inner);
        // A dropped caller does not cancel the blocking work: a mutation that
        // started still completes, or poisons the store, under its lock.
        tokio::task::spawn_blocking(move || work(&inner))
            .await
            .map_err(|err| Error::Generic {
                store: STORE,
                source: Box::new(err),
            })?
    }
}

impl Inner {
    fn read(&self) -> std::result::Result<RwLockReadGuard<'_, RawStore>, RawMemberError> {
        self.store.read().map_err(|_| RawMemberError::LockPoisoned)
    }

    fn write(&self) -> std::result::Result<RwLockWriteGuard<'_, RawStore>, RawMemberError> {
        let store = self
            .store
            .write()
            .map_err(|_| RawMemberError::LockPoisoned)?;
        if store.is_poisoned() {
            return Err(RawMemberError::Poisoned);
        }
        Ok(store)
    }

    /// Admit `appended` log bytes that leave `live_after` live objects: the
    /// append plus the remaining delete debt must fit above the reserve.
    fn admit(&self, store: &RawStore, appended: u64, live_after: u64) -> Result<()> {
        let available = store.geometry().free.saturating_sub(self.reserve);
        let needed = appended.saturating_add(delete_debt(live_after));
        if needed <= available {
            return Ok(());
        }
        Err(store_err(RawMemberError::NoSpace {
            needed,
            available,
            reserve: self.reserve,
        }))
    }

    fn lookup(&self, location: &Path) -> Result<(ObjectHandle, ObjectMeta)> {
        let key = key(location)?;
        let store = self.read().map_err(store_err)?;
        match store.handle(key).map_err(store_err)? {
            Some(handle) => Ok((handle, meta(location, &handle.info))),
            None => Err(not_found(location)),
        }
    }

    fn get(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        if options.version.is_some() {
            return Err(store_err(RawMemberError::Unsupported("object versions")));
        }
        let (handle, meta) = self.lookup(location)?;
        options.check_preconditions(&meta)?;
        let range = match &options.range {
            Some(range) => range
                .as_range(meta.size)
                .map_err(|err| invalid_range(location, err))?,
            None => 0..meta.size,
        };
        let payload = if options.head {
            stream::empty().boxed()
        } else {
            let bytes = self.read_ranges(&handle, std::slice::from_ref(&range))?;
            stream::iter(bytes.into_iter().map(Ok)).boxed()
        };
        Ok(GetResult {
            payload: GetResultPayload::Stream(payload),
            meta,
            range,
            attributes: Default::default(),
            extensions: Default::default(),
        })
    }

    /// Runs outside the store lock: the handle's record is immutable.
    fn read_ranges(&self, handle: &ObjectHandle, ranges: &[Range<u64>]) -> Result<Vec<Bytes>> {
        let parts = self.reader.read_ranges(handle, ranges).map_err(store_err)?;
        Ok(parts.into_iter().map(Bytes::from).collect())
    }

    fn put(&self, location: &Path, payload: PutPayload, mode: PutMode) -> Result<PutResult> {
        let key = key(location)?;
        let len = payload.content_length() as u64;
        let cost = RawStore::put_cost(len)
            .filter(|_| len <= MAX_OBJECT_LEN)
            .ok_or_else(|| store_err(RawMemberError::TooLarge { len }))?;
        let value = contiguous(&payload);
        let mut store = self.write().map_err(store_err)?;
        let existing = store.stat(key);
        match (&mode, existing) {
            (PutMode::Overwrite, _) => {}
            (PutMode::Create, None) => {}
            (PutMode::Create, Some(_)) => {
                return Err(Error::AlreadyExists {
                    path: location.to_string(),
                    source: "raw member object already exists".into(),
                });
            }
            (PutMode::Update(_), None) => {
                return Err(precondition(
                    location,
                    "conditional update target does not exist",
                ));
            }
            (PutMode::Update(expected), Some(actual)) => {
                if expected.version.is_some() || expected.e_tag != Some(e_tag(&actual)) {
                    return Err(precondition(
                        location,
                        "conditional update version does not match",
                    ));
                }
            }
        }
        let live_after = store.len() as u64 + u64::from(existing.is_none());
        self.admit(&store, cost, live_after)?;
        store.put(key, &value).map_err(store_err)?;
        store.flush().map_err(store_err)?;
        let info = store.stat(key).expect("object just stored");
        Ok(PutResult {
            e_tag: Some(e_tag(&info)),
            version: None,
            extensions: Default::default(),
        })
    }

    /// Delete each present key and flush once. Absent keys are not errors,
    /// matching the idempotent delete of the cloud stores.
    fn delete(&self, locations: &[Path]) -> Result<()> {
        let keys = locations.iter().map(key).collect::<Result<Vec<_>>>()?;
        let mut store = self.write().map_err(store_err)?;
        let present: BTreeSet<&[u8]> = keys
            .into_iter()
            .filter(|key| store.contains_key(key))
            .collect();
        if present.is_empty() {
            return Ok(());
        }
        let count = present.len() as u64;
        self.admit(&store, delete_debt(count), store.len() as u64 - count)?;
        for key in present {
            store.delete(key).map_err(store_err)?;
        }
        store.flush().map_err(store_err)
    }

    /// Copy, and for a rename also delete the source, committed by a single
    /// flush so a crash leaves either the old or the new namespace.
    fn copy(&self, from: &Path, to: &Path, create: bool, remove_source: bool) -> Result<()> {
        let (from_key, to_key) = (key(from)?, key(to)?);
        let mut store = self.write().map_err(store_err)?;
        let Some(handle) = store.handle(from_key).map_err(store_err)? else {
            return Err(not_found(from));
        };
        if create && store.contains_key(to_key) {
            return Err(Error::AlreadyExists {
                path: to.to_string(),
                source: "raw member copy target already exists".into(),
            });
        }
        if from_key == to_key {
            return Ok(());
        }
        let len = handle.info.len;
        let put = RawStore::put_cost(len).expect("stored objects are bounded");
        let appended = put
            + if remove_source {
                RawStore::delete_cost()
            } else {
                0
            };
        let live_after =
            store.len() as u64 + u64::from(!store.contains_key(to_key)) - u64::from(remove_source);
        self.admit(&store, appended, live_after)?;
        let value = self
            .reader
            .read_ranges(&handle, std::slice::from_ref(&(0..len)))
            .map_err(store_err)?
            .pop()
            .expect("one range requested");
        store.put(to_key, &value).map_err(store_err)?;
        if remove_source {
            store.delete(from_key).map_err(store_err)?;
        }
        store.flush().map_err(store_err)
    }

    /// Verified metadata of every live object under `prefix`, sorted by path.
    fn snapshot(&self, prefix: Option<&Path>) -> Result<Vec<ObjectMeta>> {
        let store = self.read().map_err(store_err)?;
        if store.is_poisoned() {
            return Err(store_err(RawMemberError::Poisoned));
        }
        let mut objects = Vec::with_capacity(store.len());
        for (key, info) in store.objects() {
            let location = std::str::from_utf8(key)
                .map_err(|_| {
                    store_err(RawMemberError::InvalidKey("stored key is not UTF-8".into()))
                })
                .and_then(|key| Path::parse(key).map_err(Error::from))?;
            let below = prefix.is_none_or(|prefix| {
                location
                    .prefix_match(prefix)
                    .is_some_and(|mut rest| rest.next().is_some())
            });
            if below {
                objects.push(meta(&location, &info));
            }
        }
        objects.sort_by(|a, b| a.location.cmp(&b.location));
        Ok(objects)
    }
}

fn delete_debt(live: u64) -> u64 {
    live.saturating_mul(RawStore::delete_cost())
}

fn key(location: &Path) -> Result<&[u8]> {
    let key = location.as_ref().as_bytes();
    if key.is_empty() {
        return Err(store_err(RawMemberError::InvalidKey("empty path".into())));
    }
    if key.len() > zerofs_raw_device::MAX_KEY_LEN {
        return Err(store_err(RawMemberError::InvalidKey(format!(
            "{} bytes exceeds {}",
            key.len(),
            zerofs_raw_device::MAX_KEY_LEN
        ))));
    }
    Ok(key)
}

fn contiguous(payload: &PutPayload) -> Bytes {
    match payload.as_ref() {
        [] => Bytes::new(),
        [single] => single.clone(),
        parts => Bytes::from(parts.concat()),
    }
}

/// Unique per put within one provisioning (the log sequence), and bound to
/// the content so a reused sequence after an unacknowledged write cannot
/// match an old tag.
fn e_tag(info: &ObjectInfo) -> String {
    let digest: String = info.sha256[..8]
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect();
    format!("{:016x}-{digest}", info.seq)
}

fn meta(location: &Path, info: &ObjectInfo) -> ObjectMeta {
    let nanos = i64::try_from(info.modified_unix_nanos).unwrap_or(i64::MAX);
    ObjectMeta {
        location: location.clone(),
        last_modified: DateTime::<Utc>::from_timestamp_nanos(nanos),
        size: info.len,
        e_tag: Some(e_tag(info)),
        version: None,
    }
}

fn not_found(location: &Path) -> Error {
    Error::NotFound {
        path: location.to_string(),
        source: "raw member object not found".into(),
    }
}

fn precondition(location: &Path, reason: &'static str) -> Error {
    Error::Precondition {
        path: location.to_string(),
        source: reason.into(),
    }
}

fn invalid_range(location: &Path, err: impl fmt::Display) -> Error {
    Error::Generic {
        store: STORE,
        source: Box::new(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("{location}: {err}"),
        )),
    }
}

fn reject_unstored(attributes_empty: bool, tags_empty: bool) -> Result<()> {
    if !attributes_empty {
        return Err(store_err(RawMemberError::Unsupported("object attributes")));
    }
    if !tags_empty {
        return Err(store_err(RawMemberError::Unsupported("object tags")));
    }
    Ok(())
}

#[async_trait]
impl ObjectStore for RawMemberStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> Result<PutResult> {
        reject_unstored(opts.attributes.is_empty(), opts.tags.encoded().is_empty())?;
        let location = location.clone();
        self.blocking(move |inner| inner.put(&location, payload, opts.mode))
            .await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>> {
        reject_unstored(opts.attributes.is_empty(), opts.tags.encoded().is_empty())?;
        key(location)?;
        Ok(Box::new(RawMultipart {
            store: self.clone(),
            location: location.clone(),
            parts: Arc::new(Mutex::new(Some(Parts::default()))),
        }))
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        let location = location.clone();
        self.blocking(move |inner| inner.get(&location, options))
            .await
    }

    /// One verified pass over the payload serves every range.
    async fn get_ranges(&self, location: &Path, ranges: &[Range<u64>]) -> Result<Vec<Bytes>> {
        let location = location.clone();
        let ranges = ranges.to_vec();
        self.blocking(move |inner| {
            let (handle, meta) = inner.lookup(&location)?;
            let ranges = ranges
                .into_iter()
                .map(|range| {
                    GetRange::Bounded(range)
                        .as_range(meta.size)
                        .map_err(|err| invalid_range(&location, err))
                })
                .collect::<Result<Vec<_>>>()?;
            inner.read_ranges(&handle, &ranges)
        })
        .await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, Result<Path>>,
    ) -> BoxStream<'static, Result<Path>> {
        let this = self.clone();
        locations
            .ready_chunks(DELETE_BATCH)
            .then(move |batch| {
                let this = this.clone();
                async move {
                    let mut out = Vec::with_capacity(batch.len());
                    let mut paths = Vec::with_capacity(batch.len());
                    for item in batch {
                        match item {
                            Ok(path) => paths.push(path),
                            Err(err) => out.push(Err(err)),
                        }
                    }
                    if !paths.is_empty() {
                        let deleting = paths.clone();
                        match this.blocking(move |inner| inner.delete(&deleting)).await {
                            Ok(()) => out.extend(paths.into_iter().map(Ok)),
                            Err(err) => {
                                // One flush covers the batch: report the
                                // failure against every path in it.
                                let reason = err.to_string();
                                out.push(Err(err));
                                out.extend(paths.into_iter().skip(1).map(|path| {
                                    Err(Error::Generic {
                                        store: STORE,
                                        source: format!(
                                            "delete of {path} failed with its batch: {reason}"
                                        )
                                        .into(),
                                    })
                                }));
                            }
                        }
                    }
                    stream::iter(out)
                }
            })
            .flatten()
            .boxed()
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        let this = self.clone();
        let prefix = prefix.cloned();
        async move {
            let listed = this
                .blocking(move |inner| inner.snapshot(prefix.as_ref()))
                .await;
            match listed {
                Ok(objects) => stream::iter(objects.into_iter().map(Ok)).boxed(),
                Err(err) => stream::once(async { Err(err) }).boxed(),
            }
        }
        .flatten_stream()
        .boxed()
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, Result<ObjectMeta>> {
        let offset = offset.clone();
        self.list(prefix)
            .filter(move |item| {
                let keep = item.as_ref().map_or(true, |meta| meta.location > offset);
                futures::future::ready(keep)
            })
            .boxed()
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        let root = prefix.cloned().unwrap_or_default();
        let listing_root = root.clone();
        let objects = self
            .blocking(move |inner| inner.snapshot(Some(&listing_root)))
            .await?;
        let mut common_prefixes = BTreeSet::new();
        let mut direct = Vec::new();
        for object in objects {
            let mut rest = object
                .location
                .prefix_match(&root)
                .expect("snapshot filtered by prefix");
            let first = rest.next().expect("snapshot excludes the prefix itself");
            if rest.next().is_some() {
                let child = root.clone().join(first);
                common_prefixes.insert(child);
                continue;
            }
            drop(rest);
            direct.push(object);
        }
        Ok(ListResult {
            objects: direct,
            common_prefixes: common_prefixes.into_iter().collect(),
            extensions: Default::default(),
        })
    }

    async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> Result<()> {
        let create = matches!(options.mode, CopyMode::Create);
        let (from, to) = (from.clone(), to.clone());
        self.blocking(move |inner| inner.copy(&from, &to, create, false))
            .await
    }

    async fn rename_opts(&self, from: &Path, to: &Path, options: RenameOptions) -> Result<()> {
        let create = matches!(options.target_mode, RenameTargetMode::Create);
        let (from, to) = (from.clone(), to.clone());
        self.blocking(move |inner| inner.copy(&from, &to, create, true))
            .await
    }
}

#[derive(Default)]
struct Parts {
    payload: Vec<Bytes>,
    len: u64,
    failed: bool,
}

/// Buffers parts in memory, bounded by [`MAX_OBJECT_LEN`], and stores them
/// as one overwrite on `complete`. Nothing reaches the device before then.
#[derive(Debug)]
struct RawMultipart {
    store: RawMemberStore,
    location: Path,
    parts: Arc<Mutex<Option<Parts>>>,
}

impl fmt::Debug for Parts {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Parts")
            .field("len", &self.len)
            .field("failed", &self.failed)
            .finish()
    }
}

#[async_trait]
impl MultipartUpload for RawMultipart {
    fn put_part(&mut self, data: PutPayload) -> UploadPart {
        let mut guard = self.parts.lock().unwrap_or_else(|e| e.into_inner());
        let result = match guard.as_mut() {
            None => Err(Error::Generic {
                store: STORE,
                source: "multipart upload already finished".into(),
            }),
            Some(parts) => {
                let len = parts.len.saturating_add(data.content_length() as u64);
                if len > MAX_OBJECT_LEN {
                    parts.failed = true;
                    Err(store_err(RawMemberError::TooLarge { len }))
                } else {
                    parts.len = len;
                    parts.payload.extend(data);
                    Ok(())
                }
            }
        };
        futures::future::ready(result).boxed()
    }

    async fn complete(&mut self) -> Result<PutResult> {
        let parts = self
            .parts
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .take()
            .ok_or_else(|| Error::Generic {
                store: STORE,
                source: "multipart upload already finished".into(),
            })?;
        if parts.failed {
            return Err(Error::Generic {
                store: STORE,
                source: "multipart upload had a failed part".into(),
            });
        }
        let payload = PutPayload::from_iter(parts.payload);
        self.store
            .put_opts(&self.location, payload, PutMode::Overwrite.into())
            .await
    }

    async fn abort(&mut self) -> Result<()> {
        self.parts.lock().unwrap_or_else(|e| e.into_inner()).take();
        Ok(())
    }
}

#[cfg(test)]
mod tests;
