use async_trait::async_trait;
use backon::{ExponentialBuilder, Retryable};
use bytes::Bytes;
use chrono::{DateTime, Utc};
use dashmap::DashSet;
use futures::stream::{self, BoxStream};
use futures::{StreamExt, TryStreamExt};
use object_store::path::{Path as ObjectPath, PathPart};
use object_store::{
    Attributes, CopyOptions, Extensions, GetOptions, GetResult, GetResultPayload, ListResult,
    MultipartUpload, ObjectMeta, ObjectStore, PutMode, PutMultipartOptions, PutOptions, PutPayload,
    PutResult, UploadPart,
};
use std::collections::BTreeSet;
use std::collections::HashMap;
use std::fmt::{self, Debug};
use std::future::Future;
use std::path::{Path as FilePath, PathBuf};
use std::sync::{Arc, LazyLock, Mutex as StdMutex, Weak};
use tokio::sync::Mutex as AsyncMutex;
use uuid::Uuid;

pub const OBJECT_HEADER_LEN: usize = 32;
const OBJECT_HEADER_MAGIC: &[u8; 8] = b"ZEROFS\x01\0";
const SFTP_REQUEST_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(45);
const SFTP_STAGING_CLEANUP_ATTEMPTS: usize = 3;

async fn bounded_sftp_request<T, F>(
    operation: &'static str,
    future: F,
) -> Result<T, crate::sftp_transport::TransportError>
where
    F: Future<Output = Result<T, crate::sftp_transport::TransportError>>,
{
    tokio::time::timeout(SFTP_REQUEST_TIMEOUT, future)
        .await
        .map_err(|_| {
            crate::sftp_transport::TransportError::Operation(format!(
                "{operation} timed out after {:.3}s",
                SFTP_REQUEST_TIMEOUT.as_secs_f64()
            ))
        })?
}

/// Owns the checkout -> bounded request -> lease finish choreography every
/// pooled SFTP call repeats. Expands to a future yielding the request result
/// with the lease already finished, so each call site only has to map the one
/// resulting `TransportError` into its own error type.
///
/// This is a macro rather than a generic helper because the request borrows the
/// lease mutably: a closure-based helper needs `for<'a> FnOnce(&'a mut _) ->
/// BoxFuture<'a, _>`, which rustc cannot infer for these closures without an
/// explicit return-type annotation plus a boxed future at every call site.
macro_rules! with_lease {
    ($pool:expr, $kind:expr, $operation:literal, |$lease:ident| $request:expr) => {
        async {
            let mut $lease = $pool.checkout($kind).await?;
            let result = bounded_sftp_request($operation, $request).await;
            finish_lease($lease, result).await
        }
    };
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ObjectHeader {
    pub generation: Uuid,
    pub logical_len: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SftpCapabilities {
    pub fsync: bool,
    pub hardlink: bool,
    pub posix_rename: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PublicationMode {
    Create,
    Overwrite,
    Update,
}

pub fn validate_publication_capabilities(
    capabilities: SftpCapabilities,
    mode: PublicationMode,
) -> Result<(), &'static str> {
    if !capabilities.fsync {
        return Err("fsync");
    }
    match mode {
        PublicationMode::Create if !capabilities.hardlink => Err("hardlink"),
        PublicationMode::Overwrite | PublicationMode::Update if !capabilities.posix_rename => {
            Err("posix-rename")
        }
        _ => Ok(()),
    }
}

pub fn encode_header(header: ObjectHeader) -> [u8; OBJECT_HEADER_LEN] {
    let mut encoded = [0; OBJECT_HEADER_LEN];
    encoded[..8].copy_from_slice(OBJECT_HEADER_MAGIC);
    encoded[8..24].copy_from_slice(header.generation.as_bytes());
    encoded[24..].copy_from_slice(&header.logical_len.to_be_bytes());
    encoded
}

pub fn decode_header(bytes: &[u8]) -> Result<ObjectHeader, String> {
    let bytes: &[u8; OBJECT_HEADER_LEN] = bytes
        .get(..OBJECT_HEADER_LEN)
        .ok_or_else(|| "SFTP object is shorter than its internal header".to_owned())?
        .try_into()
        .expect("slice length checked above");
    if &bytes[..8] != OBJECT_HEADER_MAGIC {
        return Err("SFTP object has an invalid internal header".to_owned());
    }

    let generation = Uuid::from_slice(&bytes[8..24])
        .map_err(|error| format!("SFTP object has an invalid generation: {error}"))?;
    let logical_len = u64::from_be_bytes(
        bytes[24..]
            .try_into()
            .expect("fixed-size header has an eight-byte logical length"),
    );
    Ok(ObjectHeader {
        generation,
        logical_len,
    })
}

const STAGING_PREFIX: &str = ".zerofs-staging-";

pub fn staging_path(target: &FilePath, upload_id: Uuid) -> Result<PathBuf, String> {
    let filename = target
        .file_name()
        .and_then(|filename| filename.to_str())
        .ok_or_else(|| "SFTP object path must end in a UTF-8 filename".to_owned())?;
    Ok(target.with_file_name(format!("{STAGING_PREFIX}{filename}-{upload_id}")))
}

pub fn is_staging_name(name: &FilePath) -> bool {
    let Some(name) = name.to_str() else {
        return false;
    };
    let Some(remainder) = name.strip_prefix(STAGING_PREFIX) else {
        return false;
    };
    let Some(upload_id_start) = remainder.len().checked_sub(36) else {
        return false;
    };
    if upload_id_start < 2 || remainder.as_bytes()[upload_id_start - 1] != b'-' {
        return false;
    }
    let target_name = &remainder[..upload_id_start - 1];
    let upload_id = &remainder[upload_id_start..];
    !target_name.is_empty() && Uuid::parse_str(upload_id).is_ok()
}

#[derive(Debug, thiserror::Error)]
pub enum RemoteError {
    #[error("remote path not found: {0}")]
    NotFound(String),
    #[error("remote path already exists: {0}")]
    AlreadyExists(String),
    #[error("remote precondition failed: {0}")]
    Precondition(String),
    #[error("remote path permission denied: {0}")]
    PermissionDenied(String),
    #[error("invalid remote object path: {0}")]
    InvalidPath(String),
    #[error("remote object is corrupt: {0}")]
    CorruptObject(String),
    #[error("remote operation is not supported: {0}")]
    NotSupported(String),
    #[error("SFTP session pool is closed")]
    PoolClosed,
    #[error("{operation}; cleanup required: {debt}")]
    CleanupRequired {
        operation: Box<RemoteError>,
        debt: StagingCleanupDebt,
    },
    #[error("remote SFTP operation failed: {0}")]
    Other(String),
}

impl RemoteError {
    fn is_pool_closed(&self) -> bool {
        match self {
            Self::PoolClosed => true,
            Self::CleanupRequired { operation, debt } => {
                operation.is_pool_closed() || debt.error.is_pool_closed()
            }
            _ => false,
        }
    }

    fn is_ambiguous(&self) -> bool {
        matches!(self, Self::Other(_))
    }

    pub(crate) fn is_retryable(&self) -> bool {
        match self {
            Self::NotFound(_)
            | Self::AlreadyExists(_)
            | Self::Precondition(_)
            | Self::PermissionDenied(_)
            | Self::InvalidPath(_)
            | Self::CorruptObject(_)
            | Self::NotSupported(_)
            | Self::PoolClosed => false,
            Self::CleanupRequired { operation, debt } => {
                operation.is_retryable() && debt.error.is_retryable()
            }
            Self::Other(_) => true,
        }
    }
}

pub type RemoteResult<T> = Result<T, RemoteError>;

#[derive(Debug, thiserror::Error)]
#[error("failed to remove staging path {}: {error}", path.display())]
pub struct StagingCleanupDebt {
    pub path: PathBuf,
    pub error: Box<RemoteError>,
}

#[derive(Debug)]
pub struct PublicationOutcome {
    pub header: ObjectHeader,
    pub cleanup_debt: Option<StagingCleanupDebt>,
}

type TargetLock = AsyncMutex<()>;

static TARGET_LOCKS: LazyLock<StdMutex<HashMap<PathBuf, Weak<TargetLock>>>> =
    LazyLock::new(|| StdMutex::new(HashMap::new()));

fn target_lock(target: &FilePath) -> Arc<TargetLock> {
    let mut locks = TARGET_LOCKS.lock().unwrap();
    locks.retain(|_, lock| lock.strong_count() > 0);
    if let Some(lock) = locks.get(target).and_then(Weak::upgrade) {
        return lock;
    }
    let lock = Arc::new(TargetLock::new(()));
    locks.insert(target.to_path_buf(), Arc::downgrade(&lock));
    lock
}

#[async_trait]
pub trait RemoteSession: Debug + Send + Sync {
    fn capabilities(&self) -> SftpCapabilities;
    async fn read_exact(&self, path: &FilePath, offset: u64, len: usize) -> RemoteResult<Bytes>;
    async fn write_file_durable(&self, path: &FilePath, chunks: Vec<Bytes>) -> RemoteResult<()>;
    async fn write_file_at_durable(
        &self,
        path: &FilePath,
        offset: u64,
        chunks: Vec<Bytes>,
    ) -> RemoteResult<()> {
        let _ = (path, offset, chunks);
        Err(RemoteError::Other(
            "write_file_at_durable is not implemented by this session".to_owned(),
        ))
    }
    async fn write_file_at(
        &self,
        path: &FilePath,
        offset: u64,
        chunks: Vec<Bytes>,
    ) -> RemoteResult<()> {
        let _ = (path, offset, chunks);
        Err(RemoteError::Other(
            "write_file_at is not implemented by this session".to_owned(),
        ))
    }
    async fn remove_file(&self, path: &FilePath) -> RemoteResult<()>;
    async fn hard_link(&self, from: &FilePath, to: &FilePath) -> RemoteResult<()>;
    async fn posix_rename(&self, from: &FilePath, to: &FilePath) -> RemoteResult<()>;
    fn schedule_cleanup(&self, path: PathBuf);
}

pub async fn publish_payload(
    session: Arc<dyn RemoteSession>,
    target: &FilePath,
    payload: Vec<Bytes>,
    mode: PublicationMode,
    expected_generation: Option<Uuid>,
) -> RemoteResult<PublicationOutcome> {
    let target_lock = target_lock(target);
    let _target_guard = target_lock.lock().await;
    validate_publication_capabilities(session.capabilities(), mode).map_err(|extension| {
        RemoteError::NotSupported(format!("SFTP server lacks required {extension} extension"))
    })?;
    let logical_len = payload.iter().try_fold(0_u64, |total, chunk| {
        total
            .checked_add(chunk.len() as u64)
            .ok_or_else(|| RemoteError::Other("SFTP object length overflow".to_owned()))
    })?;
    let header = ObjectHeader {
        generation: Uuid::new_v4(),
        logical_len,
    };
    let staging = staging_path(target, header.generation).map_err(RemoteError::InvalidPath)?;
    let mut cleanup = StagingCleanup::new(session.clone(), staging.clone());
    let mut physical_payload = Vec::with_capacity(payload.len() + 1);
    physical_payload.push(Bytes::copy_from_slice(&encode_header(header)));
    physical_payload.extend(payload);

    if let Err(error) = session.write_file_durable(&staging, physical_payload).await {
        return Err(failure_with_cleanup(&mut cleanup, error).await);
    }

    if mode == PublicationMode::Create {
        let publication = match session.hard_link(&staging, target).await {
            Ok(()) => Ok(()),
            Err(error) => reconcile_publication(&session, target, header, error).await,
        };
        return match publication {
            Ok(()) => Ok(PublicationOutcome {
                header,
                cleanup_debt: cleanup.remove_now().await.err(),
            }),
            Err(error) => Err(failure_with_cleanup(&mut cleanup, error).await),
        };
    }

    let publication = match mode {
        PublicationMode::Overwrite => match session.posix_rename(&staging, target).await {
            Ok(()) => Ok(()),
            Err(error) => reconcile_publication(&session, target, header, error).await,
        },
        PublicationMode::Update => match expected_generation {
            None => Err(RemoteError::Precondition(
                "Update requires an expected generation".to_owned(),
            )),
            Some(expected) => match session
                .read_exact(target, 0, OBJECT_HEADER_LEN)
                .await
                .and_then(|bytes| decode_header(&bytes).map_err(RemoteError::CorruptObject))
            {
                Err(error) => Err(error),
                Ok(current) if current.generation != expected => {
                    Err(RemoteError::Precondition(format!(
                        "expected generation {expected}, found {}",
                        current.generation
                    )))
                }
                Ok(_) => match session.posix_rename(&staging, target).await {
                    Ok(()) => Ok(()),
                    Err(error) => reconcile_publication(&session, target, header, error).await,
                },
            },
        },
        PublicationMode::Create => unreachable!("Create handled above"),
    };

    if let Err(error) = publication {
        return Err(failure_with_cleanup(&mut cleanup, error).await);
    }
    cleanup.disarm();
    Ok(PublicationOutcome {
        header,
        cleanup_debt: None,
    })
}

async fn reconcile_publication(
    session: &Arc<dyn RemoteSession>,
    target: &FilePath,
    expected: ObjectHeader,
    error: RemoteError,
) -> RemoteResult<()> {
    if !error.is_ambiguous() {
        return Err(error);
    }
    let observed = (|| async {
        let bytes = session.read_exact(target, 0, OBJECT_HEADER_LEN).await?;
        decode_header(&bytes).map_err(RemoteError::CorruptObject)
    })
    .retry(
        ExponentialBuilder::default()
            .without_max_times()
            .with_min_delay(std::time::Duration::from_millis(100))
            .with_max_delay(std::time::Duration::from_secs(1)),
    )
    .when(RemoteError::is_retryable)
    .await;

    match observed {
        Ok(found) if found == expected => Ok(()),
        Ok(_) | Err(RemoteError::NotFound(_)) => Err(error),
        Err(read_error) => Err(read_error),
    }
}

async fn failure_with_cleanup(cleanup: &mut StagingCleanup, operation: RemoteError) -> RemoteError {
    if operation.is_pool_closed() {
        return operation;
    }
    match cleanup.remove_now().await {
        Ok(()) => operation,
        Err(debt) => RemoteError::CleanupRequired {
            operation: Box::new(operation),
            debt,
        },
    }
}

struct StagingCleanup {
    session: Arc<dyn RemoteSession>,
    path: Option<PathBuf>,
}

impl StagingCleanup {
    fn new(session: Arc<dyn RemoteSession>, path: PathBuf) -> Self {
        Self {
            session,
            path: Some(path),
        }
    }

    fn disarm(&mut self) {
        self.path = None;
    }

    async fn remove_now(&mut self) -> Result<(), StagingCleanupDebt> {
        let path = self.path.as_ref().expect("armed cleanup").clone();
        match self.session.remove_file(&path).await {
            Ok(()) => {
                self.disarm();
                Ok(())
            }
            Err(RemoteError::NotFound(_)) => {
                self.disarm();
                Ok(())
            }
            Err(error) => Err(StagingCleanupDebt {
                path,
                error: Box::new(error),
            }),
        }
    }
}

impl Drop for StagingCleanup {
    fn drop(&mut self) {
        let Some(path) = self.path.take() else {
            return;
        };
        self.session.schedule_cleanup(path);
    }
}

const STORE_NAME: &str = "SFTP";
const SFTP_NEGATIVE_CACHE_MAX_ENTRIES: usize = 16 * 1024;
// Directory listings need one metadata round trip per child; fan them out so a
// large directory does not cost thousands of serialized WAN round trips. The
// pool's own read/metadata admission limit still gates real concurrency.
const SFTP_LIST_METADATA_CONCURRENCY: usize = 8;

#[derive(Clone)]
pub struct SftpObjectStore {
    pool: crate::sftp_transport::SftpSessionPool,
    prefix: ObjectPath,
    // This backend is deliberately single-owner: SFTP cannot provide the
    // distributed CAS needed by multiple ZeroFS writers. Remember confirmed
    // misses for this process lifetime so metadata/GC polling does not spend
    // four WAN round trips rediscovering the same absent object. Every local
    // publication invalidates its target before and after the remote write.
    known_missing: Arc<DashSet<String>>,
}

#[derive(Debug, Clone)]
struct PooledRemoteSession {
    pool: crate::sftp_transport::SftpSessionPool,
}

#[async_trait]
impl RemoteSession for PooledRemoteSession {
    fn capabilities(&self) -> SftpCapabilities {
        SftpCapabilities {
            fsync: true,
            hardlink: true,
            posix_rename: true,
        }
    }

    async fn read_exact(&self, path: &FilePath, offset: u64, len: usize) -> RemoteResult<Bytes> {
        with_lease!(
            self.pool,
            crate::sftp_transport::OperationKind::Read,
            "SFTP range read",
            |lease| lease.read_exact(path, offset, len)
        )
        .await
        .map_err(remote_transport_error)
    }

    async fn write_file_durable(&self, path: &FilePath, chunks: Vec<Bytes>) -> RemoteResult<()> {
        with_lease!(
            self.pool,
            crate::sftp_transport::OperationKind::Write,
            "SFTP durable write",
            |lease| async {
                if let Some(parent) = path.parent() {
                    self.pool.ensure_directory(&mut lease, parent).await?;
                }
                match lease.write_file_durable(path, chunks.clone()).await {
                    Err(crate::sftp_transport::TransportError::NotFound(_))
                        if path.parent().is_some() =>
                    {
                        let parent = path.parent().expect("parent checked above");
                        self.pool.repair_directory(&mut lease, parent).await?;
                        lease.write_file_durable(path, chunks).await
                    }
                    result => result,
                }
            }
        )
        .await
        .map_err(remote_transport_error)
    }

    async fn write_file_at_durable(
        &self,
        path: &FilePath,
        offset: u64,
        chunks: Vec<Bytes>,
    ) -> RemoteResult<()> {
        with_lease!(
            self.pool,
            crate::sftp_transport::OperationKind::Write,
            "SFTP durable ranged write",
            |lease| lease.write_file_at_durable(path, offset, chunks)
        )
        .await
        .map_err(remote_transport_error)
    }

    async fn write_file_at(
        &self,
        path: &FilePath,
        offset: u64,
        chunks: Vec<Bytes>,
    ) -> RemoteResult<()> {
        with_lease!(
            self.pool,
            crate::sftp_transport::OperationKind::Write,
            "SFTP ranged write",
            |lease| lease.write_file_at(path, offset, chunks)
        )
        .await
        .map_err(remote_transport_error)
    }

    async fn remove_file(&self, path: &FilePath) -> RemoteResult<()> {
        with_lease!(
            self.pool,
            crate::sftp_transport::OperationKind::Write,
            "SFTP remove",
            |lease| lease.remove_file(path)
        )
        .await
        .map_err(remote_transport_error)
    }

    async fn hard_link(&self, from: &FilePath, to: &FilePath) -> RemoteResult<()> {
        with_lease!(
            self.pool,
            crate::sftp_transport::OperationKind::Write,
            "SFTP hard link",
            |lease| lease.hard_link(from, to)
        )
        .await
        .map_err(remote_transport_error)
    }

    async fn posix_rename(&self, from: &FilePath, to: &FilePath) -> RemoteResult<()> {
        with_lease!(
            self.pool,
            crate::sftp_transport::OperationKind::Write,
            "SFTP POSIX rename",
            |lease| lease.posix_rename(from, to)
        )
        .await
        .map_err(remote_transport_error)
    }

    fn schedule_cleanup(&self, path: PathBuf) {
        let session = self.clone();
        let pool = self.pool.clone();
        let debt_path = path.clone();
        if !self.pool.spawn_cleanup(&debt_path, async move {
            for attempt in 1..=SFTP_STAGING_CLEANUP_ATTEMPTS {
                match session.remove_file(&path).await {
                    Ok(()) | Err(RemoteError::NotFound(_)) => return,
                    Err(error)
                        if error.is_retryable() && attempt < SFTP_STAGING_CLEANUP_ATTEMPTS =>
                    {
                        tokio::time::sleep(std::time::Duration::from_millis(100 * attempt as u64))
                            .await;
                    }
                    Err(error) if error.is_pool_closed() => {
                        // The pool closed mid-retry during shutdown. The
                        // staging file remains as invisible debris the next
                        // boot never observes — a warning, not a failed stop.
                        tracing::warn!(
                            path = %path.display(),
                            attempt,
                            "SFTP staging cleanup remains required because the session pool closed during shutdown"
                        );
                        return;
                    }
                    Err(error) => {
                        pool.record_cleanup_debt(&path, &error);
                        tracing::warn!(
                            path = %path.display(),
                            %error,
                            attempt,
                            "SFTP staging cleanup retries were exhausted"
                        );
                        return;
                    }
                }
            }
        }) {
            tracing::warn!(
                path = %debt_path.display(),
                "SFTP staging cleanup remains required because the session pool is shutting down"
            );
        }
    }
}

/// A directory entry that survived listing filters, resolved into the shape the
/// callers accumulate.
enum ListedChild {
    Directory(ObjectPath),
    Object(ObjectMeta),
}

impl SftpObjectStore {
    pub(crate) fn validate_prefix(prefix: &ObjectPath) -> object_store::Result<()> {
        if prefix.is_root() {
            return Err(generic_error(
                "the SFTP pilot requires a non-root dedicated prefix",
            ));
        }
        Ok(())
    }

    pub fn new(
        pool: crate::sftp_transport::SftpSessionPool,
        prefix: ObjectPath,
    ) -> object_store::Result<Self> {
        Self::validate_prefix(&prefix)?;
        Ok(Self {
            pool,
            prefix,
            known_missing: Arc::new(DashSet::new()),
        })
    }

    fn forget_missing(&self, location: &ObjectPath) {
        self.known_missing.remove(location.as_ref());
    }

    fn remember_missing(&self, location: &ObjectPath) {
        // Absent future manifests are normally removed as soon as this writer
        // publishes them. Deleted historical objects can remain absent for the
        // process lifetime, so bound that residue rather than letting a busy
        // long-running filesystem grow the cache indefinitely.
        if self.known_missing.len() >= SFTP_NEGATIVE_CACHE_MAX_ENTRIES {
            self.known_missing.clear();
        }
        self.known_missing.insert(location.to_string());
    }

    fn validate_location(
        &self,
        location: &ObjectPath,
        allow_prefix: bool,
    ) -> object_store::Result<()> {
        if !location.prefix_matches(&self.prefix) || (!allow_prefix && location == &self.prefix) {
            return Err(invalid_path_error(format!(
                "object path {location} is outside the configured SFTP prefix {}",
                self.prefix
            )));
        }
        Ok(())
    }

    fn remote_path(
        &self,
        location: &ObjectPath,
        allow_prefix: bool,
    ) -> object_store::Result<PathBuf> {
        self.validate_location(location, allow_prefix)?;
        let mut path = PathBuf::new();
        for part in location.parts() {
            let part = part.as_ref();
            if part.is_empty() || matches!(part, "." | "..") {
                return Err(invalid_path_error(format!(
                    "unsafe SFTP path component in {location}"
                )));
            }
            path.push(part);
        }
        Ok(path)
    }

    async fn read_remote(
        &self,
        location: &ObjectPath,
        options: &GetOptions,
    ) -> object_store::Result<crate::sftp_transport::RemoteObjectRead> {
        if options.version.is_some() {
            return Err(object_store::Error::NotSupported {
                source: "SFTP object versions are not exposed separately from ETags".into(),
            });
        }
        let remote = self.remote_path(location, false)?;
        if self.known_missing.contains(location.as_ref()) {
            return Err(object_store::Error::NotFound {
                path: location.to_string(),
                source: "SFTP object is known absent for this single-owner process".into(),
            });
        }
        let result = with_lease!(
            self.pool,
            if options.head {
                crate::sftp_transport::OperationKind::Metadata
            } else {
                crate::sftp_transport::OperationKind::Read
            },
            "SFTP object read",
            |lease| lease.read_object(&remote, options.range.clone(), options.head)
        )
        .await
        .map_err(transport_error);
        match &result {
            Ok(_) => {
                self.forget_missing(location);
            }
            Err(object_store::Error::NotFound { .. }) => {
                self.remember_missing(location);
            }
            Err(_) => {}
        }
        result
    }

    async fn directory_snapshot(
        &self,
        location: &ObjectPath,
    ) -> object_store::Result<Vec<crate::sftp_transport::RemoteDirectoryEntry>> {
        let remote = self.remote_path(location, true)?;
        with_lease!(
            self.pool,
            crate::sftp_transport::OperationKind::Metadata,
            "SFTP directory listing",
            |lease| lease.list_directory(&remote)
        )
        .await
        .map_err(transport_error)
    }

    async fn metadata(&self, location: &ObjectPath) -> object_store::Result<ObjectMeta> {
        let options = GetOptions {
            head: true,
            ..Default::default()
        };
        let object = self.read_remote(location, &options).await?;
        Ok(object_meta(location.clone(), &object))
    }

    async fn classify_entry(
        &self,
        directory: &ObjectPath,
        entry: crate::sftp_transport::RemoteDirectoryEntry,
    ) -> object_store::Result<Option<ListedChild>> {
        let Some(filename) = entry.filename.to_str() else {
            return Err(generic_error("SFTP listing returned a non-UTF-8 filename"));
        };
        if matches!(filename, "." | "..") || is_staging_name(FilePath::new(filename)) {
            return Ok(None);
        }
        let part = PathPart::parse(filename)
            .map_err(|error| generic_error(format!("invalid SFTP filename: {error}")))?;
        let child = directory.clone().join(part);
        match entry.kind {
            crate::sftp_transport::RemoteEntryKind::Directory => {
                Ok(Some(ListedChild::Directory(child)))
            }
            crate::sftp_transport::RemoteEntryKind::File => {
                let object = self.metadata(&child).await?;
                Ok(Some(ListedChild::Object(object)))
            }
            crate::sftp_transport::RemoteEntryKind::Symlink => Err(generic_error(format!(
                "refusing to follow SFTP symlink {child}"
            ))),
            crate::sftp_transport::RemoteEntryKind::Other => Err(generic_error(format!(
                "refusing non-regular SFTP entry {child}"
            ))),
        }
    }

    /// Resolve one directory snapshot into ordered children. Per-entry metadata
    /// lookups run with bounded concurrency; `buffered` keeps the results in
    /// listing order and surfaces the first error exactly as a serial `?` would.
    async fn classify_entries(
        &self,
        directory: &ObjectPath,
        entries: Vec<crate::sftp_transport::RemoteDirectoryEntry>,
    ) -> object_store::Result<Vec<ListedChild>> {
        let children: Vec<Option<ListedChild>> = stream::iter(entries)
            .map(|entry| self.classify_entry(directory, entry))
            .buffered(SFTP_LIST_METADATA_CONCURRENCY)
            .try_collect()
            .await?;
        Ok(children.into_iter().flatten().collect())
    }

    async fn collect_recursive(&self, prefix: ObjectPath) -> object_store::Result<Vec<ObjectMeta>> {
        self.validate_location(&prefix, true)?;
        let mut pending = vec![prefix];
        let mut objects = Vec::new();
        while let Some(directory) = pending.pop() {
            let entries = match self.directory_snapshot(&directory).await {
                Ok(entries) => entries,
                Err(object_store::Error::NotFound { .. }) => {
                    match self.metadata(&directory).await {
                        Ok(object) => objects.push(object),
                        Err(object_store::Error::NotFound { .. }) => {}
                        Err(error) => return Err(error),
                    }
                    continue;
                }
                Err(error) => return Err(error),
            };
            for child in self.classify_entries(&directory, entries).await? {
                match child {
                    ListedChild::Directory(path) => pending.push(path),
                    ListedChild::Object(object) => objects.push(object),
                }
            }
        }
        Ok(objects)
    }

    async fn remove_remote(&self, location: &ObjectPath) -> object_store::Result<()> {
        let remote = self.remote_path(location, false)?;
        let result = with_lease!(
            self.pool,
            crate::sftp_transport::OperationKind::Write,
            "SFTP remove",
            |lease| lease.remove_file(&remote)
        )
        .await
        .map_err(transport_error);
        if result.is_ok() || matches!(&result, Err(object_store::Error::NotFound { .. })) {
            self.remember_missing(location);
        }
        result
    }
}

impl fmt::Debug for SftpObjectStore {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SftpObjectStore")
            .field("prefix", &self.prefix)
            .finish_non_exhaustive()
    }
}

impl fmt::Display for SftpObjectStore {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "SftpObjectStore(prefix={})", self.prefix)
    }
}

#[async_trait]
impl ObjectStore for SftpObjectStore {
    async fn put_opts(
        &self,
        location: &ObjectPath,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        let target = self.remote_path(location, false)?;
        self.forget_missing(location);
        let (mode, expected_generation) = match opts.mode {
            PutMode::Overwrite => (PublicationMode::Overwrite, None),
            PutMode::Create => (PublicationMode::Create, None),
            PutMode::Update(version) => {
                if version.version.is_some() {
                    return Err(object_store::Error::NotSupported {
                        source: "SFTP conditional updates use ETags, not versions".into(),
                    });
                }
                let expected = version
                    .e_tag
                    .ok_or_else(|| object_store::Error::Precondition {
                        path: location.to_string(),
                        source: "SFTP update requires an ETag".into(),
                    })?;
                let expected = Uuid::parse_str(&expected).map_err(|error| {
                    object_store::Error::Precondition {
                        path: location.to_string(),
                        source: Box::new(error),
                    }
                })?;
                (PublicationMode::Update, Some(expected))
            }
        };
        let outcome = publish_payload(
            Arc::new(PooledRemoteSession {
                pool: self.pool.clone(),
            }),
            &target,
            payload.into_iter().collect(),
            mode,
            expected_generation,
        )
        .await
        .map_err(|error| publication_error(location, error))?;
        if let Some(debt) = outcome.cleanup_debt {
            tracing::warn!(%debt, "SFTP object committed with staging cleanup debt");
        }
        self.forget_missing(location);
        Ok(PutResult {
            e_tag: Some(outcome.header.generation.to_string()),
            version: None,
            extensions: Extensions::default(),
        })
    }

    async fn put_multipart_opts(
        &self,
        location: &ObjectPath,
        _opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        let target = self.remote_path(location, false)?;
        self.forget_missing(location);
        let session: Arc<dyn RemoteSession> = Arc::new(PooledRemoteSession {
            pool: self.pool.clone(),
        });
        let upload = SftpMultipartUpload::begin(
            session,
            location.clone(),
            target,
            self.known_missing.clone(),
        )
        .await
        .map_err(|error| publication_error(location, error))?;
        Ok(Box::new(upload))
    }

    async fn get_opts(
        &self,
        location: &ObjectPath,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        let has_preconditions = options.if_match.is_some()
            || options.if_none_match.is_some()
            || options.if_modified_since.is_some()
            || options.if_unmodified_since.is_some();
        if has_preconditions && !options.head {
            let metadata_options = GetOptions {
                version: options.version.clone(),
                head: true,
                ..Default::default()
            };
            let object = self.read_remote(location, &metadata_options).await?;
            let meta = object_meta(location.clone(), &object);
            options.check_preconditions(&meta)?;
        }
        let object = self.read_remote(location, &options).await?;
        let meta = object_meta(location.clone(), &object);
        options.check_preconditions(&meta)?;
        let payload = object.payload;
        Ok(GetResult {
            payload: GetResultPayload::Stream(stream::once(async move { Ok(payload) }).boxed()),
            meta,
            range: object.range,
            attributes: Attributes::default(),
            extensions: Extensions::default(),
        })
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, object_store::Result<ObjectPath>>,
    ) -> BoxStream<'static, object_store::Result<ObjectPath>> {
        let store = self.clone();
        let concurrency = self.pool.write_concurrency();
        locations
            .map(move |location| {
                let store = store.clone();
                async move {
                    let location = location?;
                    store.remove_remote(&location).await?;
                    Ok(location)
                }
            })
            .buffered(concurrency)
            .boxed()
    }

    fn list(
        &self,
        prefix: Option<&ObjectPath>,
    ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        let store = self.clone();
        let prefix = prefix.cloned().unwrap_or_else(|| self.prefix.clone());
        stream::once(async move {
            match store.collect_recursive(prefix).await {
                Ok(objects) => objects.into_iter().map(Ok).collect::<Vec<_>>(),
                Err(error) => vec![Err(error)],
            }
        })
        .flat_map(stream::iter)
        .boxed()
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&ObjectPath>,
    ) -> object_store::Result<ListResult> {
        let directory = prefix.cloned().unwrap_or_else(|| self.prefix.clone());
        let entries = match self.directory_snapshot(&directory).await {
            Ok(entries) => entries,
            Err(object_store::Error::NotFound { .. }) => Vec::new(),
            Err(error) => return Err(error),
        };
        let mut common_prefixes = BTreeSet::new();
        let mut objects = Vec::new();
        for child in self.classify_entries(&directory, entries).await? {
            match child {
                ListedChild::Directory(path) => {
                    common_prefixes.insert(path);
                }
                ListedChild::Object(object) => objects.push(object),
            }
        }
        Ok(ListResult {
            common_prefixes: common_prefixes.into_iter().collect(),
            objects,
            extensions: Extensions::default(),
        })
    }

    async fn copy_opts(
        &self,
        _from: &ObjectPath,
        _to: &ObjectPath,
        _options: CopyOptions,
    ) -> object_store::Result<()> {
        Err(not_implemented("copy_opts"))
    }
}

#[derive(Debug)]
struct SftpMultipartUpload {
    session: Arc<dyn RemoteSession>,
    location: ObjectPath,
    target: PathBuf,
    staging: Option<PathBuf>,
    generation: Uuid,
    state: Arc<StdMutex<SftpMultipartState>>,
    known_missing: Arc<DashSet<String>>,
    terminal: bool,
}

#[derive(Debug, Default)]
struct SftpMultipartState {
    logical_len: u64,
    completed: Vec<bool>,
}

impl SftpMultipartUpload {
    async fn begin(
        session: Arc<dyn RemoteSession>,
        location: ObjectPath,
        target: PathBuf,
        known_missing: Arc<DashSet<String>>,
    ) -> RemoteResult<Self> {
        validate_publication_capabilities(session.capabilities(), PublicationMode::Overwrite)
            .map_err(|extension| {
                RemoteError::NotSupported(format!(
                    "SFTP server lacks required {extension} extension"
                ))
            })?;
        let generation = Uuid::new_v4();
        let staging = staging_path(&target, generation).map_err(RemoteError::InvalidPath)?;
        let mut cleanup = StagingCleanup::new(session.clone(), staging.clone());
        let placeholder = encode_header(ObjectHeader {
            generation,
            logical_len: 0,
        });
        if let Err(error) = session
            .write_file_durable(&staging, vec![Bytes::copy_from_slice(&placeholder)])
            .await
        {
            return Err(failure_with_cleanup(&mut cleanup, error).await);
        }
        cleanup.disarm();
        Ok(Self {
            session,
            location,
            target,
            staging: Some(staging),
            generation,
            state: Arc::new(StdMutex::new(SftpMultipartState::default())),
            known_missing,
            terminal: false,
        })
    }

    fn staging(&self) -> object_store::Result<&FilePath> {
        self.staging
            .as_deref()
            .ok_or_else(|| generic_error("multipart upload has no staging path"))
    }
}

#[async_trait]
impl MultipartUpload for SftpMultipartUpload {
    fn put_part(&mut self, data: PutPayload) -> UploadPart {
        if self.terminal {
            return Box::pin(async {
                Err(generic_error(
                    "multipart upload is already completed or aborted",
                ))
            });
        }
        let chunks = data.into_iter().collect::<Vec<_>>();
        let len = chunks.iter().map(Bytes::len).sum::<usize>();
        let (index, offset) = {
            let mut state = self.state.lock().unwrap();
            let Ok(len) = u64::try_from(len) else {
                return Box::pin(async { Err(generic_error("multipart part is too large")) });
            };
            let Some(next) = state.logical_len.checked_add(len) else {
                return Box::pin(async { Err(generic_error("multipart object length overflow")) });
            };
            let index = state.completed.len();
            let offset = state.logical_len;
            state.logical_len = next;
            state.completed.push(false);
            (index, offset)
        };
        let session = self.session.clone();
        let staging = self.staging().map(PathBuf::from);
        let state = self.state.clone();
        let location = self.location.clone();
        Box::pin(async move {
            let staging = staging?;
            let physical_offset = (OBJECT_HEADER_LEN as u64)
                .checked_add(offset)
                .ok_or_else(|| generic_error("multipart physical offset overflow"))?;
            session
                .write_file_at(&staging, physical_offset, chunks)
                .await
                .map_err(|error| publication_error(&location, error))?;
            state.lock().unwrap().completed[index] = true;
            Ok(())
        })
    }

    async fn complete(&mut self) -> object_store::Result<PutResult> {
        if self.terminal {
            return Err(generic_error(
                "multipart upload is already completed or aborted",
            ));
        }
        let logical_len = {
            let state = self.state.lock().unwrap();
            if state.completed.iter().any(|completed| !completed) {
                return Err(generic_error(
                    "multipart upload completed before every part future finished",
                ));
            }
            state.logical_len
        };
        let staging = self.staging()?.to_path_buf();
        let target_lock = target_lock(&self.target);
        let _target_guard = target_lock.lock().await;
        let header = ObjectHeader {
            generation: self.generation,
            logical_len,
        };
        self.session
            .write_file_at_durable(
                &staging,
                0,
                vec![Bytes::copy_from_slice(&encode_header(header))],
            )
            .await
            .map_err(|error| publication_error(&self.location, error))?;
        let publication = match self.session.posix_rename(&staging, &self.target).await {
            Ok(()) => Ok(()),
            Err(error) => reconcile_publication(&self.session, &self.target, header, error).await,
        };
        publication.map_err(|error| publication_error(&self.location, error))?;
        self.terminal = true;
        self.staging = None;
        self.known_missing.remove(self.location.as_ref());
        Ok(PutResult {
            e_tag: Some(self.generation.to_string()),
            version: None,
            extensions: Extensions::default(),
        })
    }

    async fn abort(&mut self) -> object_store::Result<()> {
        if let Some(staging) = self.staging.as_ref() {
            self.session
                .remove_file(staging)
                .await
                .map_err(|error| publication_error(&self.location, error))?;
            self.staging = None;
        }
        self.terminal = true;
        Ok(())
    }
}

impl Drop for SftpMultipartUpload {
    fn drop(&mut self) {
        let Some(staging) = self.staging.take() else {
            return;
        };
        self.session.schedule_cleanup(staging);
    }
}

fn object_meta(
    location: ObjectPath,
    object: &crate::sftp_transport::RemoteObjectRead,
) -> ObjectMeta {
    ObjectMeta {
        location,
        last_modified: DateTime::<Utc>::from(object.modified),
        size: object.header.logical_len,
        e_tag: Some(object.header.generation.to_string()),
        version: None,
    }
}

async fn finish_lease<T>(
    lease: crate::sftp_transport::SessionLease,
    operation: Result<T, crate::sftp_transport::TransportError>,
) -> Result<T, crate::sftp_transport::TransportError> {
    match operation {
        Ok(value) => {
            lease.complete().await?;
            Ok(value)
        }
        Err(error) => {
            let reusable = matches!(
                error,
                crate::sftp_transport::TransportError::NotFound(_)
                    | crate::sftp_transport::TransportError::PermissionDenied(_)
                    | crate::sftp_transport::TransportError::AlreadyExists(_)
                    | crate::sftp_transport::TransportError::CorruptObject(_)
            );
            let cleanup = if reusable {
                lease.complete().await
            } else {
                lease.retire().await
            };
            if let Err(cleanup) = cleanup {
                tracing::warn!(%cleanup, "failed to finish SFTP session after operation error");
            }
            Err(error)
        }
    }
}

fn transport_error(error: crate::sftp_transport::TransportError) -> object_store::Error {
    match error {
        crate::sftp_transport::TransportError::NotFound(path) => object_store::Error::NotFound {
            path,
            source: "SFTP server reported no such file".into(),
        },
        crate::sftp_transport::TransportError::PermissionDenied(path) => {
            object_store::Error::PermissionDenied {
                path,
                source: "SFTP server denied access".into(),
            }
        }
        crate::sftp_transport::TransportError::AlreadyExists(path) => {
            object_store::Error::AlreadyExists {
                path,
                source: "SFTP server reported that the path already exists".into(),
            }
        }
        crate::sftp_transport::TransportError::PoolClosed => object_store::Error::NotSupported {
            source: Box::new(crate::sftp_transport::TransportError::PoolClosed),
        },
        error @ crate::sftp_transport::TransportError::MissingCapability(_) => {
            object_store::Error::NotSupported {
                source: Box::new(error),
            }
        }
        error @ (crate::sftp_transport::TransportError::InvalidLimits(_)
        | crate::sftp_transport::TransportError::CorruptObject(_)) => {
            object_store::Error::Generic {
                store: STORE_NAME,
                source: Box::new(crate::retrying_object_store::PermanentError::new(error)),
            }
        }
        error => object_store::Error::Generic {
            store: STORE_NAME,
            source: Box::new(error),
        },
    }
}

fn remote_transport_error(error: crate::sftp_transport::TransportError) -> RemoteError {
    match error {
        crate::sftp_transport::TransportError::NotFound(path) => RemoteError::NotFound(path),
        crate::sftp_transport::TransportError::PermissionDenied(path) => {
            RemoteError::PermissionDenied(path)
        }
        crate::sftp_transport::TransportError::AlreadyExists(path) => {
            RemoteError::AlreadyExists(path)
        }
        crate::sftp_transport::TransportError::CorruptObject(message) => {
            RemoteError::CorruptObject(message)
        }
        // Deterministically unsatisfiable: the requested range can never
        // exist for this object, so retrying is pure churn. Typed here so no
        // retry layer has to sniff provider error text to classify it.
        crate::sftp_transport::TransportError::InvalidRange(message) => {
            RemoteError::Precondition(message)
        }
        crate::sftp_transport::TransportError::MissingCapability(extension) => {
            RemoteError::NotSupported(format!("SFTP server lacks required {extension} extension"))
        }
        crate::sftp_transport::TransportError::PoolClosed => RemoteError::PoolClosed,
        error => RemoteError::Other(error.to_string()),
    }
}

fn publication_error(location: &ObjectPath, error: RemoteError) -> object_store::Error {
    if matches!(error, RemoteError::PoolClosed) {
        return object_store::Error::NotSupported {
            source: Box::new(crate::sftp_transport::TransportError::PoolClosed),
        };
    }
    if error.is_pool_closed() {
        return object_store::Error::NotSupported {
            source: Box::new(error),
        };
    }
    match error {
        RemoteError::NotFound(path) => object_store::Error::NotFound {
            path,
            source: "SFTP server reported no such file".into(),
        },
        RemoteError::AlreadyExists(source) => object_store::Error::AlreadyExists {
            path: location.to_string(),
            source: source.into(),
        },
        RemoteError::Precondition(source) => object_store::Error::Precondition {
            path: location.to_string(),
            source: source.into(),
        },
        RemoteError::PermissionDenied(source) => object_store::Error::PermissionDenied {
            path: location.to_string(),
            source: source.into(),
        },
        RemoteError::NotSupported(source) => object_store::Error::NotSupported {
            source: source.into(),
        },
        RemoteError::PoolClosed => unreachable!("bare pool-closed errors returned above"),
        error if !error.is_retryable() => object_store::Error::Generic {
            store: STORE_NAME,
            source: Box::new(crate::retrying_object_store::PermanentError::new(error)),
        },
        error => object_store::Error::Generic {
            store: STORE_NAME,
            source: Box::new(error),
        },
    }
}

fn generic_error(error: impl Into<String>) -> object_store::Error {
    object_store::Error::Generic {
        store: STORE_NAME,
        source: error.into().into(),
    }
}

fn invalid_path_error(error: impl Into<String>) -> object_store::Error {
    object_store::Error::Generic {
        store: STORE_NAME,
        source: Box::new(crate::retrying_object_store::PermanentError::new(
            RemoteError::InvalidPath(error.into()),
        )),
    }
}

fn not_implemented(operation: &str) -> object_store::Error {
    object_store::Error::NotImplemented {
        operation: operation.to_owned(),
        implementer: "SftpObjectStore".to_owned(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sftp_transport::{
        OpenSshTransportSession, RemoteDirectoryEntry, RemoteObjectRead, SessionFactory,
        TransportError, TransportSession,
    };
    use object_store::ObjectStoreExt;
    use std::collections::{HashMap, HashSet};
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;
    use tokio::sync::{Barrier, Notify};

    #[test]
    fn pool_closed_is_a_terminal_object_store_error() {
        let error = transport_error(TransportError::PoolClosed);
        assert!(matches!(error, object_store::Error::NotSupported { .. }));

        let nested = publication_error(
            &ObjectPath::from("object"),
            RemoteError::CleanupRequired {
                operation: Box::new(RemoteError::Other("write failed".to_owned())),
                debt: StagingCleanupDebt {
                    path: PathBuf::from("staging"),
                    error: Box::new(RemoteError::PoolClosed),
                },
            },
        );
        let object_store::Error::NotSupported { source } = nested else {
            panic!("nested pool closure must remain terminal");
        };
        let preserved = source
            .downcast_ref::<RemoteError>()
            .expect("terminal classification must retain the cleanup debt");
        let RemoteError::CleanupRequired { operation, debt } = preserved else {
            panic!("cleanup debt was flattened: {preserved:?}");
        };
        assert!(matches!(**operation, RemoteError::Other(_)));
        assert_eq!(debt.path, PathBuf::from("staging"));
        assert!(matches!(*debt.error, RemoteError::PoolClosed));
    }

    #[test]
    fn remote_transport_errors_keep_terminal_types() {
        assert!(matches!(
            remote_transport_error(TransportError::PermissionDenied("private".to_owned())),
            RemoteError::PermissionDenied(path) if path == "private"
        ));
        let corrupt = remote_transport_error(TransportError::CorruptObject(
            "invalid object header".to_owned(),
        ));
        assert!(matches!(
            corrupt,
            RemoteError::CorruptObject(ref message) if message == "invalid object header"
        ));
        assert!(!corrupt.is_retryable());

        let error = publication_error(
            &ObjectPath::from("root/corrupt"),
            RemoteError::CorruptObject("invalid object header".to_owned()),
        );
        let object_store::Error::Generic { source, .. } = error else {
            panic!("corruption uses a typed generic source");
        };
        let marker = source
            .downcast_ref::<crate::retrying_object_store::PermanentError>()
            .expect("terminal remote errors carry the permanent marker");
        let preserved = std::error::Error::source(marker)
            .and_then(|inner| inner.downcast_ref::<RemoteError>())
            .expect("RemoteError must not be flattened into a string");
        assert!(matches!(preserved, RemoteError::CorruptObject(_)));
        assert!(!preserved.is_retryable());
    }

    #[derive(Debug, Default)]
    struct ProtocolCleanupState {
        dials: AtomicUsize,
        live: AtomicUsize,
        close_started: AtomicUsize,
        protocol_close_failures: AtomicUsize,
        release_close: Notify,
    }

    #[derive(Debug, Clone)]
    struct ProtocolCleanupFactory(Arc<ProtocolCleanupState>);

    #[async_trait]
    impl SessionFactory for ProtocolCleanupFactory {
        async fn open(
            &self,
            _force: tokio_util::sync::CancellationToken,
        ) -> Result<Box<dyn TransportSession>, TransportError> {
            self.0.dials.fetch_add(1, Ordering::SeqCst);
            self.0.live.fetch_add(1, Ordering::SeqCst);
            Ok(Box::new(ProtocolCleanupSession(self.0.clone())))
        }
    }

    #[derive(Debug)]
    struct ProtocolCleanupSession(Arc<ProtocolCleanupState>);

    #[async_trait]
    impl TransportSession for ProtocolCleanupSession {
        fn capabilities(&self) -> SftpCapabilities {
            SftpCapabilities {
                fsync: true,
                hardlink: true,
                posix_rename: true,
            }
        }

        async fn ensure_directory_component(
            &mut self,
            _path: &FilePath,
        ) -> Result<(), TransportError> {
            Ok(())
        }

        async fn write_file_durable(
            &mut self,
            _path: &FilePath,
            _chunks: Vec<Bytes>,
        ) -> Result<(), TransportError> {
            Err(TransportError::Operation(
                "forced final-flush write failure".to_owned(),
            ))
        }

        async fn close(
            self: Box<Self>,
            _force: tokio_util::sync::CancellationToken,
        ) -> Result<(), TransportError> {
            self.0.close_started.fetch_add(1, Ordering::SeqCst);
            self.0.release_close.notified().await;
            self.0
                .protocol_close_failures
                .fetch_add(1, Ordering::SeqCst);
            self.0.live.fetch_sub(1, Ordering::SeqCst);
            Ok(())
        }
    }

    #[derive(Debug, Default)]
    struct HangingOperationState {
        dials: AtomicUsize,
        write_started: Notify,
    }

    #[derive(Debug, Clone)]
    struct HangingOperationFactory(Arc<HangingOperationState>);

    #[async_trait]
    impl SessionFactory for HangingOperationFactory {
        async fn open(
            &self,
            _force: tokio_util::sync::CancellationToken,
        ) -> Result<Box<dyn TransportSession>, TransportError> {
            let dial = self.0.dials.fetch_add(1, Ordering::SeqCst);
            Ok(Box::new(HangingOperationSession {
                state: self.0.clone(),
                hang_write: dial == 0,
            }))
        }
    }

    #[derive(Debug)]
    struct HangingOperationSession {
        state: Arc<HangingOperationState>,
        hang_write: bool,
    }

    #[async_trait]
    impl TransportSession for HangingOperationSession {
        fn capabilities(&self) -> SftpCapabilities {
            SftpCapabilities {
                fsync: true,
                hardlink: true,
                posix_rename: true,
            }
        }

        async fn ensure_directory_component(
            &mut self,
            _path: &FilePath,
        ) -> Result<(), TransportError> {
            Ok(())
        }

        async fn write_file_durable(
            &mut self,
            _path: &FilePath,
            _chunks: Vec<Bytes>,
        ) -> Result<(), TransportError> {
            if self.hang_write {
                self.state.write_started.notify_one();
                std::future::pending().await
            } else {
                Ok(())
            }
        }

        async fn remove_file(&mut self, _path: &FilePath) -> Result<(), TransportError> {
            Ok(())
        }

        async fn hard_link(
            &mut self,
            _from: &FilePath,
            _to: &FilePath,
        ) -> Result<(), TransportError> {
            Ok(())
        }

        async fn posix_rename(
            &mut self,
            _from: &FilePath,
            _to: &FilePath,
        ) -> Result<(), TransportError> {
            Ok(())
        }

        async fn close(
            self: Box<Self>,
            _force: tokio_util::sync::CancellationToken,
        ) -> Result<(), TransportError> {
            Ok(())
        }
    }

    #[tokio::test(start_paused = true)]
    async fn hung_protocol_operation_is_bounded_and_reconnects() {
        let state = Arc::new(HangingOperationState::default());
        let pool = crate::sftp_transport::SftpSessionPool::new_writable(
            Arc::new(HangingOperationFactory(state.clone())),
            1,
            1,
            1,
        )
        .await
        .unwrap();
        let store = Arc::new(SftpObjectStore::new(pool.clone(), ObjectPath::from("root")).unwrap());
        let first = tokio::spawn({
            let store = store.clone();
            async move {
                store
                    .put_opts(
                        &ObjectPath::from("root/first"),
                        PutPayload::from_static(b"first"),
                        PutOptions::default(),
                    )
                    .await
            }
        });
        state.write_started.notified().await;
        tokio::time::advance(Duration::from_secs(46)).await;
        tokio::task::yield_now().await;

        assert!(
            first.is_finished(),
            "a stalled SFTP request must release the writeback frontier"
        );
        assert!(first.await.unwrap().is_err());
        store
            .put_opts(
                &ObjectPath::from("root/second"),
                PutPayload::from_static(b"second"),
                PutOptions::default(),
            )
            .await
            .unwrap();
        assert!(state.dials.load(Ordering::SeqCst) >= 2);
        pool.shutdown().await.unwrap();
    }

    #[derive(Debug)]
    struct ConcurrentDeleteState {
        barrier: Barrier,
        in_flight: AtomicUsize,
        peak: AtomicUsize,
    }

    #[derive(Debug, Default)]
    struct StagingRetryState {
        remove_calls: AtomicUsize,
        retry_started: Notify,
        release_retry: Notify,
    }

    #[derive(Debug, Clone)]
    struct StagingRetryFactory(Arc<StagingRetryState>);

    #[async_trait]
    impl SessionFactory for StagingRetryFactory {
        async fn open(
            &self,
            _force: tokio_util::sync::CancellationToken,
        ) -> Result<Box<dyn TransportSession>, TransportError> {
            Ok(Box::new(StagingRetrySession(self.0.clone())))
        }
    }

    #[derive(Debug)]
    struct StagingRetrySession(Arc<StagingRetryState>);

    #[async_trait]
    impl TransportSession for StagingRetrySession {
        fn capabilities(&self) -> SftpCapabilities {
            SftpCapabilities {
                fsync: true,
                hardlink: true,
                posix_rename: true,
            }
        }

        async fn ensure_directory_component(
            &mut self,
            _path: &FilePath,
        ) -> Result<(), TransportError> {
            Ok(())
        }

        async fn write_file_durable(
            &mut self,
            _path: &FilePath,
            _chunks: Vec<Bytes>,
        ) -> Result<(), TransportError> {
            Ok(())
        }

        async fn hard_link(
            &mut self,
            _from: &FilePath,
            _to: &FilePath,
        ) -> Result<(), TransportError> {
            Ok(())
        }

        async fn remove_file(&mut self, _path: &FilePath) -> Result<(), TransportError> {
            if self.0.remove_calls.fetch_add(1, Ordering::SeqCst) == 0 {
                return Err(TransportError::Operation(
                    "forced first staging removal failure".to_owned(),
                ));
            }
            self.0.retry_started.notify_one();
            self.0.release_retry.notified().await;
            Ok(())
        }

        async fn close(
            self: Box<Self>,
            _force: tokio_util::sync::CancellationToken,
        ) -> Result<(), TransportError> {
            Ok(())
        }
    }

    #[derive(Debug, Clone)]
    struct ConcurrentDeleteFactory(Arc<ConcurrentDeleteState>);

    #[async_trait]
    impl SessionFactory for ConcurrentDeleteFactory {
        async fn open(
            &self,
            _force: tokio_util::sync::CancellationToken,
        ) -> Result<Box<dyn TransportSession>, TransportError> {
            Ok(Box::new(ConcurrentDeleteSession(self.0.clone())))
        }
    }

    #[derive(Debug)]
    struct ConcurrentDeleteSession(Arc<ConcurrentDeleteState>);

    #[async_trait]
    impl TransportSession for ConcurrentDeleteSession {
        fn capabilities(&self) -> SftpCapabilities {
            SftpCapabilities {
                fsync: true,
                hardlink: true,
                posix_rename: true,
            }
        }

        async fn remove_file(&mut self, _path: &FilePath) -> Result<(), TransportError> {
            let current = self.0.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
            self.0.peak.fetch_max(current, Ordering::SeqCst);
            self.0.barrier.wait().await;
            self.0.in_flight.fetch_sub(1, Ordering::SeqCst);
            Ok(())
        }

        async fn close(
            self: Box<Self>,
            _force: tokio_util::sync::CancellationToken,
        ) -> Result<(), TransportError> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn bulk_delete_uses_configured_write_concurrency() {
        let state = Arc::new(ConcurrentDeleteState {
            barrier: Barrier::new(2),
            in_flight: AtomicUsize::new(0),
            peak: AtomicUsize::new(0),
        });
        let pool = crate::sftp_transport::SftpSessionPool::new_writable(
            Arc::new(ConcurrentDeleteFactory(state.clone())),
            2,
            1,
            2,
        )
        .await
        .unwrap();
        let store = SftpObjectStore::new(pool.clone(), ObjectPath::from("root")).unwrap();
        let locations = (0..4)
            .map(|index| Ok(ObjectPath::from(format!("root/object-{index}"))))
            .collect::<Vec<_>>();

        tokio::time::timeout(
            Duration::from_secs(1),
            store
                .delete_stream(stream::iter(locations).boxed())
                .collect::<Vec<_>>(),
        )
        .await
        .expect("two deletes should be admitted together");

        assert_eq!(state.peak.load(Ordering::SeqCst), 2);
        pool.shutdown().await.unwrap();
    }

    #[derive(Debug, Default)]
    struct DirectoryCacheTestState {
        directories: Mutex<HashSet<PathBuf>>,
        stats: Mutex<HashMap<PathBuf, usize>>,
        mkdirs: Mutex<HashMap<PathBuf, usize>>,
        fail_component_once: Mutex<Option<PathBuf>>,
        parallel_leaf_barrier: Mutex<Option<Arc<Barrier>>>,
        write_failure: Mutex<Option<TransportError>>,
        write_attempts: AtomicUsize,
    }

    impl DirectoryCacheTestState {
        fn count(map: &Mutex<HashMap<PathBuf, usize>>, path: &str) -> usize {
            map.lock()
                .unwrap()
                .get(FilePath::new(path))
                .copied()
                .unwrap_or_default()
        }

        fn stat_count(&self, path: &str) -> usize {
            Self::count(&self.stats, path)
        }

        fn mkdir_count(&self, path: &str) -> usize {
            Self::count(&self.mkdirs, path)
        }

        fn remove_directory_tree(&self, path: &str) {
            let path = FilePath::new(path);
            self.directories
                .lock()
                .unwrap()
                .retain(|directory| !directory.starts_with(path));
        }
    }

    #[derive(Debug, Clone)]
    struct DirectoryCacheTestFactory(Arc<DirectoryCacheTestState>);

    #[async_trait]
    impl SessionFactory for DirectoryCacheTestFactory {
        async fn open(
            &self,
            _force: tokio_util::sync::CancellationToken,
        ) -> Result<Box<dyn TransportSession>, TransportError> {
            Ok(Box::new(DirectoryCacheTestSession(self.0.clone())))
        }
    }

    #[derive(Debug)]
    struct DirectoryCacheTestSession(Arc<DirectoryCacheTestState>);

    #[async_trait]
    impl TransportSession for DirectoryCacheTestSession {
        fn capabilities(&self) -> SftpCapabilities {
            SftpCapabilities {
                fsync: true,
                hardlink: true,
                posix_rename: true,
            }
        }

        async fn ensure_directory_component(
            &mut self,
            path: &FilePath,
        ) -> Result<(), TransportError> {
            for component in path.components() {
                if !matches!(component, std::path::Component::Normal(_)) {
                    return Err(TransportError::Operation(format!(
                        "unsafe directory path {}",
                        path.display()
                    )));
                }
            }
            *self
                .0
                .stats
                .lock()
                .unwrap()
                .entry(path.to_path_buf())
                .or_default() += 1;
            let should_fail = self
                .0
                .fail_component_once
                .lock()
                .unwrap()
                .as_ref()
                .is_some_and(|failure| failure == path);
            if should_fail {
                self.0.fail_component_once.lock().unwrap().take();
                return Err(TransportError::Operation(format!(
                    "forced directory failure for {}",
                    path.display()
                )));
            }
            if self
                .0
                .directories
                .lock()
                .unwrap()
                .insert(path.to_path_buf())
            {
                *self
                    .0
                    .mkdirs
                    .lock()
                    .unwrap()
                    .entry(path.to_path_buf())
                    .or_default() += 1;
            }
            let barrier = self.0.parallel_leaf_barrier.lock().unwrap().clone();
            if path.parent() == Some(FilePath::new("root"))
                && let Some(barrier) = barrier
            {
                barrier.wait().await;
            }
            tokio::task::yield_now().await;
            Ok(())
        }

        async fn write_file_durable(
            &mut self,
            path: &FilePath,
            _chunks: Vec<Bytes>,
        ) -> Result<(), TransportError> {
            self.0.write_attempts.fetch_add(1, Ordering::SeqCst);
            if let Some(error) = self.0.write_failure.lock().unwrap().take() {
                return Err(error);
            }
            let parent = path
                .parent()
                .ok_or_else(|| TransportError::NotFound(path.display().to_string()))?;
            if !self.0.directories.lock().unwrap().contains(parent) {
                return Err(TransportError::NotFound(path.display().to_string()));
            }
            Ok(())
        }

        async fn close(
            self: Box<Self>,
            _force: tokio_util::sync::CancellationToken,
        ) -> Result<(), TransportError> {
            Ok(())
        }
    }

    async fn directory_cache_test_pool(
        state: Arc<DirectoryCacheTestState>,
    ) -> crate::sftp_transport::SftpSessionPool {
        crate::sftp_transport::SftpSessionPool::new_writable(
            Arc::new(DirectoryCacheTestFactory(state)),
            8,
            7,
            7,
        )
        .await
        .unwrap()
    }

    async fn directory_cache_test_write(
        pool: &crate::sftp_transport::SftpSessionPool,
        path: &str,
    ) -> RemoteResult<()> {
        PooledRemoteSession { pool: pool.clone() }
            .write_file_durable(FilePath::new(path), vec![Bytes::from_static(b"payload")])
            .await
    }

    #[tokio::test]
    async fn sibling_writes_do_not_repeat_component_stats() {
        let state = Arc::new(DirectoryCacheTestState::default());
        let pool = directory_cache_test_pool(state.clone()).await;

        directory_cache_test_write(&pool, "root/shard-a/one")
            .await
            .unwrap();
        directory_cache_test_write(&pool, "root/shard-a/two")
            .await
            .unwrap();
        directory_cache_test_write(&pool, "root/shard-b/three")
            .await
            .unwrap();

        assert_eq!(state.stat_count("root"), 1);
        assert_eq!(state.stat_count("root/shard-a"), 1);
        assert_eq!(state.stat_count("root/shard-b"), 1);
        assert_eq!(state.mkdir_count("root"), 1);
        assert_eq!(state.mkdir_count("root/shard-a"), 1);
        assert_eq!(state.mkdir_count("root/shard-b"), 1);
        pool.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn concurrent_first_writers_single_flight_directory_creation() {
        let state = Arc::new(DirectoryCacheTestState::default());
        let pool = directory_cache_test_pool(state.clone()).await;
        let writes = (0..7).map(|index| {
            let pool = pool.clone();
            tokio::spawn(async move {
                directory_cache_test_write(&pool, &format!("root/shared/object-{index}")).await
            })
        });

        for write in writes {
            write.await.unwrap().unwrap();
        }

        assert_eq!(state.stat_count("root"), 1);
        assert_eq!(state.stat_count("root/shared"), 1);
        assert_eq!(state.mkdir_count("root"), 1);
        assert_eq!(state.mkdir_count("root/shared"), 1);
        pool.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn unrelated_directory_creations_can_progress_in_parallel() {
        let state = Arc::new(DirectoryCacheTestState::default());
        *state.parallel_leaf_barrier.lock().unwrap() = Some(Arc::new(Barrier::new(2)));
        let pool = directory_cache_test_pool(state.clone()).await;

        tokio::time::timeout(Duration::from_secs(1), async {
            let first = directory_cache_test_write(&pool, "root/one/object");
            let second = directory_cache_test_write(&pool, "root/two/object");
            futures::future::try_join(first, second).await
        })
        .await
        .expect("independent directory creation must not share a global WAN lock")
        .unwrap();

        assert_eq!(state.stat_count("root"), 1);
        assert_eq!(state.stat_count("root/one"), 1);
        assert_eq!(state.stat_count("root/two"), 1);
        pool.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn stale_cached_parent_is_recreated_and_the_write_is_retried_once() {
        let state = Arc::new(DirectoryCacheTestState::default());
        let pool = directory_cache_test_pool(state.clone()).await;
        directory_cache_test_write(&pool, "root/recover/first")
            .await
            .unwrap();
        state.remove_directory_tree("root/recover");

        directory_cache_test_write(&pool, "root/recover/second")
            .await
            .unwrap();

        assert_eq!(state.write_attempts.load(Ordering::SeqCst), 3);
        assert_eq!(state.stat_count("root"), 2);
        assert_eq!(state.stat_count("root/recover"), 2);
        assert_eq!(state.mkdir_count("root/recover"), 2);
        pool.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn failed_directory_creation_does_not_poison_successful_components() {
        let state = Arc::new(DirectoryCacheTestState::default());
        *state.fail_component_once.lock().unwrap() = Some(PathBuf::from("root/failure"));
        let pool = directory_cache_test_pool(state.clone()).await;

        directory_cache_test_write(&pool, "root/failure/first")
            .await
            .expect_err("the injected component failure must surface");
        directory_cache_test_write(&pool, "root/failure/second")
            .await
            .unwrap();

        assert_eq!(state.stat_count("root"), 1);
        assert_eq!(state.stat_count("root/failure"), 2);
        assert_eq!(state.mkdir_count("root"), 1);
        assert_eq!(state.mkdir_count("root/failure"), 1);
        pool.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn non_not_found_write_failure_is_not_retried() {
        let state = Arc::new(DirectoryCacheTestState::default());
        *state.write_failure.lock().unwrap() = Some(TransportError::PermissionDenied(
            "root/denied/object".to_owned(),
        ));
        let pool = directory_cache_test_pool(state.clone()).await;

        directory_cache_test_write(&pool, "root/denied/object")
            .await
            .expect_err("permission failures must surface without retrying a durable write");

        assert_eq!(state.write_attempts.load(Ordering::SeqCst), 1);
        assert_eq!(state.stat_count("root"), 1);
        assert_eq!(state.stat_count("root/denied"), 1);
        pool.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn staging_cleanup_retry_is_owned_and_drained_by_pool_shutdown() {
        let state = Arc::new(StagingRetryState::default());
        let pool = crate::sftp_transport::SftpSessionPool::new_writable(
            Arc::new(StagingRetryFactory(state.clone())),
            1,
            1,
            1,
        )
        .await
        .unwrap();
        let store = SftpObjectStore::new(pool.clone(), ObjectPath::from("root")).unwrap();

        store
            .put_opts(
                &ObjectPath::from("root/object"),
                PutPayload::from_static(b"payload"),
                PutOptions::from(PutMode::Create),
            )
            .await
            .expect("hardlink committed even though the first staging removal failed");
        tokio::time::timeout(Duration::from_secs(1), state.retry_started.notified())
            .await
            .expect("the pool-owned cleanup retry must start");

        let shutdown = tokio::spawn({
            let pool = pool.clone();
            async move { pool.shutdown().await }
        });
        tokio::task::yield_now().await;
        assert!(
            !shutdown.is_finished(),
            "shutdown must drain the cleanup retry"
        );
        state.release_retry.notify_one();
        tokio::time::timeout(Duration::from_secs(1), shutdown)
            .await
            .expect("bounded cleanup retry must finish within pool shutdown")
            .unwrap()
            .unwrap();
        assert_eq!(state.remove_calls.load(Ordering::SeqCst), 2);
    }

    #[derive(Debug, Default)]
    struct SleepingRetryState {
        remove_calls: AtomicUsize,
        second_attempt_failed: Notify,
    }

    #[derive(Debug, Clone)]
    struct SleepingRetryFactory(Arc<SleepingRetryState>);

    #[async_trait]
    impl SessionFactory for SleepingRetryFactory {
        async fn open(
            &self,
            _force: tokio_util::sync::CancellationToken,
        ) -> Result<Box<dyn TransportSession>, TransportError> {
            Ok(Box::new(SleepingRetrySession(self.0.clone())))
        }
    }

    #[derive(Debug)]
    struct SleepingRetrySession(Arc<SleepingRetryState>);

    #[async_trait]
    impl TransportSession for SleepingRetrySession {
        fn capabilities(&self) -> SftpCapabilities {
            SftpCapabilities {
                fsync: true,
                hardlink: true,
                posix_rename: true,
            }
        }

        async fn ensure_directory_component(
            &mut self,
            _path: &FilePath,
        ) -> Result<(), TransportError> {
            Ok(())
        }

        async fn write_file_durable(
            &mut self,
            _path: &FilePath,
            _chunks: Vec<Bytes>,
        ) -> Result<(), TransportError> {
            Ok(())
        }

        async fn hard_link(
            &mut self,
            _from: &FilePath,
            _to: &FilePath,
        ) -> Result<(), TransportError> {
            Ok(())
        }

        async fn remove_file(&mut self, _path: &FilePath) -> Result<(), TransportError> {
            let call = self.0.remove_calls.fetch_add(1, Ordering::SeqCst) + 1;
            if call <= 2 {
                if call == 2 {
                    self.0.second_attempt_failed.notify_one();
                }
                return Err(TransportError::Operation(
                    "injected transient staging removal failure".to_owned(),
                ));
            }
            Ok(())
        }

        async fn close(
            self: Box<Self>,
            _force: tokio_util::sync::CancellationToken,
        ) -> Result<(), TransportError> {
            Ok(())
        }
    }

    /// The vm100 pilot shutdown defect: a staging-cleanup retry that is
    /// sleeping between attempts holds no session, so nothing in the activity
    /// drain used to protect it — `fail_closed` landed first, the retry woke
    /// into a closed pool, and every service stop with pending staged uploads
    /// exited nonzero. Shutdown must give already-scheduled cleanups a
    /// bounded window while the pool can still serve them.
    #[tokio::test]
    async fn cleanup_retry_sleeping_between_attempts_survives_pool_shutdown() {
        let state = Arc::new(SleepingRetryState::default());
        let pool = crate::sftp_transport::SftpSessionPool::new_writable(
            Arc::new(SleepingRetryFactory(state.clone())),
            1,
            1,
            1,
        )
        .await
        .unwrap();
        let store = SftpObjectStore::new(pool.clone(), ObjectPath::from("root")).unwrap();

        store
            .put_opts(
                &ObjectPath::from("root/object"),
                PutPayload::from_static(b"payload"),
                PutOptions::from(PutMode::Create),
            )
            .await
            .expect("hardlink committed even though staging removal keeps failing");
        tokio::time::timeout(
            Duration::from_secs(1),
            state.second_attempt_failed.notified(),
        )
        .await
        .expect("the pool-owned cleanup retry must run its first attempt");

        // The retry task is now sleeping before its final attempt with no
        // session checked out. Shutdown must let that attempt finish instead
        // of closing the pool underneath it and reporting a failed stop.
        pool.shutdown()
            .await
            .expect("staging cleanup pending at shutdown must not fail the stop");
        assert_eq!(
            state.remove_calls.load(Ordering::SeqCst),
            3,
            "the sleeping retry must complete during the shutdown grace window"
        );
    }

    /// Once the pool is closed, a cleanup that can no longer run leaves the
    /// staging file behind as invisible debris. That is a warning, not a
    /// shutdown failure: the next boot's journal replay publishes through
    /// fresh staging names, so debris never blocks correctness.
    #[tokio::test]
    async fn cleanup_scheduled_after_pool_close_does_not_fail_shutdown() {
        let state = Arc::new(SleepingRetryState::default());
        let pool = crate::sftp_transport::SftpSessionPool::new_writable(
            Arc::new(SleepingRetryFactory(state.clone())),
            1,
            1,
            1,
        )
        .await
        .unwrap();
        pool.begin_shutdown();

        let session: Arc<dyn RemoteSession> = Arc::new(PooledRemoteSession { pool: pool.clone() });
        drop(StagingCleanup::new(
            session,
            PathBuf::from("root/manifest/.zerofs-staging-torn.manifest-id"),
        ));

        pool.shutdown()
            .await
            .expect("debris left by a post-close cleanup must not fail the stop");
        assert_eq!(
            state.remove_calls.load(Ordering::SeqCst),
            0,
            "a closed pool must not dial new sessions for cleanup"
        );
    }

    #[derive(Debug, Default)]
    struct SharedRemoteFs {
        files: Mutex<HashMap<PathBuf, Bytes>>,
        directories: Mutex<HashSet<PathBuf>>,
    }

    #[derive(Debug, Clone)]
    struct SharedRemoteFactory(Arc<SharedRemoteFs>);

    #[async_trait]
    impl SessionFactory for SharedRemoteFactory {
        async fn open(
            &self,
            _force: tokio_util::sync::CancellationToken,
        ) -> Result<Box<dyn TransportSession>, TransportError> {
            Ok(Box::new(SharedRemoteSession(self.0.clone())))
        }
    }

    #[derive(Debug)]
    struct SharedRemoteSession(Arc<SharedRemoteFs>);

    #[async_trait]
    impl TransportSession for SharedRemoteSession {
        fn capabilities(&self) -> SftpCapabilities {
            SftpCapabilities {
                fsync: true,
                hardlink: true,
                posix_rename: true,
            }
        }

        async fn read_object(
            &mut self,
            path: &FilePath,
            requested_range: Option<object_store::GetRange>,
            head: bool,
        ) -> Result<RemoteObjectRead, TransportError> {
            let bytes = self
                .0
                .files
                .lock()
                .unwrap()
                .get(path)
                .cloned()
                .ok_or_else(|| TransportError::NotFound(path.display().to_string()))?;
            let header = decode_header(&bytes).map_err(|error| {
                TransportError::CorruptObject(format!("{}: {error}", path.display()))
            })?;
            let range = match requested_range {
                Some(range) => range.as_range(header.logical_len).map_err(|error| {
                    TransportError::Operation(format!("invalid range: {error}"))
                })?,
                None => 0..header.logical_len,
            };
            let payload = if head || range.is_empty() {
                Bytes::new()
            } else {
                let start = OBJECT_HEADER_LEN + range.start as usize;
                let end = OBJECT_HEADER_LEN + range.end as usize;
                bytes.slice(start..end)
            };
            Ok(RemoteObjectRead {
                header,
                modified: std::time::SystemTime::now(),
                range,
                payload,
            })
        }

        async fn list_directory(
            &mut self,
            path: &FilePath,
        ) -> Result<Vec<RemoteDirectoryEntry>, TransportError> {
            use crate::sftp_transport::RemoteEntryKind;
            let files = self.0.files.lock().unwrap();
            let directories = self.0.directories.lock().unwrap();
            let mut entries = Vec::new();
            for file in files.keys() {
                if file.parent() == Some(path) {
                    entries.push(RemoteDirectoryEntry {
                        filename: file.file_name().unwrap().into(),
                        kind: RemoteEntryKind::File,
                    });
                }
            }
            for directory in directories.iter() {
                if directory.parent() == Some(path) {
                    entries.push(RemoteDirectoryEntry {
                        filename: directory.file_name().unwrap().into(),
                        kind: RemoteEntryKind::Directory,
                    });
                }
            }
            Ok(entries)
        }

        async fn remove_file(&mut self, path: &FilePath) -> Result<(), TransportError> {
            self.0
                .files
                .lock()
                .unwrap()
                .remove(path)
                .map(|_| ())
                .ok_or_else(|| TransportError::NotFound(path.display().to_string()))
        }

        async fn ensure_directory_component(
            &mut self,
            path: &FilePath,
        ) -> Result<(), TransportError> {
            self.0
                .directories
                .lock()
                .unwrap()
                .insert(path.to_path_buf());
            Ok(())
        }

        async fn write_file_durable(
            &mut self,
            path: &FilePath,
            chunks: Vec<Bytes>,
        ) -> Result<(), TransportError> {
            let bytes = chunks.into_iter().flatten().collect::<Vec<_>>();
            self.0
                .files
                .lock()
                .unwrap()
                .insert(path.to_path_buf(), bytes.into());
            Ok(())
        }

        async fn read_exact(
            &mut self,
            path: &FilePath,
            offset: u64,
            len: usize,
        ) -> Result<Bytes, TransportError> {
            let files = self.0.files.lock().unwrap();
            let bytes = files
                .get(path)
                .ok_or_else(|| TransportError::NotFound(path.display().to_string()))?;
            let start = offset as usize;
            Ok(bytes.slice(start..start + len))
        }

        async fn hard_link(
            &mut self,
            from: &FilePath,
            to: &FilePath,
        ) -> Result<(), TransportError> {
            let mut files = self.0.files.lock().unwrap();
            if files.contains_key(to) {
                return Err(TransportError::AlreadyExists(to.display().to_string()));
            }
            let bytes = files
                .get(from)
                .cloned()
                .ok_or_else(|| TransportError::NotFound(from.display().to_string()))?;
            files.insert(to.to_path_buf(), bytes);
            Ok(())
        }

        async fn posix_rename(
            &mut self,
            from: &FilePath,
            to: &FilePath,
        ) -> Result<(), TransportError> {
            let mut files = self.0.files.lock().unwrap();
            let bytes = files
                .remove(from)
                .ok_or_else(|| TransportError::NotFound(from.display().to_string()))?;
            files.insert(to.to_path_buf(), bytes);
            Ok(())
        }

        async fn close(
            self: Box<Self>,
            _force: tokio_util::sync::CancellationToken,
        ) -> Result<(), TransportError> {
            Ok(())
        }
    }

    /// The kill-during-staged-manifest recovery shape from the vm100 pilot: a
    /// process died after writing a manifest's staging file but before
    /// publishing it. On the next boot, recovery probes the manifest (and
    /// caches the miss), the writeback journal republishes it under a fresh
    /// staging name, and afterwards reads and listings must behave as if the
    /// torn staging file never existed — the cached miss must not keep
    /// poisoning reads and the orphan must stay invisible.
    #[tokio::test]
    async fn killed_staged_manifest_republishes_and_unpoisons_after_restart() {
        use futures::TryStreamExt;

        let remote = Arc::new(SharedRemoteFs::default());
        let target = ObjectPath::from("root/manifest/00000000000000000782.manifest");
        let orphan = staging_path(
            FilePath::new("root/manifest/00000000000000000782.manifest"),
            Uuid::new_v4(),
        )
        .unwrap();
        remote
            .files
            .lock()
            .unwrap()
            .insert(orphan.clone(), Bytes::from_static(b"torn staged manifest"));

        // "Restart": a fresh pool and store over the same remote filesystem.
        let pool = crate::sftp_transport::SftpSessionPool::new_writable(
            Arc::new(SharedRemoteFactory(remote.clone())),
            2,
            2,
            2,
        )
        .await
        .unwrap();
        let store = SftpObjectStore::new(pool.clone(), ObjectPath::from("root")).unwrap();

        let probe = store.get(&target).await;
        assert!(
            matches!(probe, Err(object_store::Error::NotFound { .. })),
            "recovery's pre-replay probe sees a genuine miss: {probe:?}"
        );

        store
            .put_opts(
                &target,
                PutPayload::from_static(b"manifest 782"),
                PutOptions::from(PutMode::Create),
            )
            .await
            .expect("journal replay republishes the manifest despite the orphaned staging file");

        let read = store
            .get(&target)
            .await
            .expect("the pre-replay miss must not stay cached once the manifest is published")
            .bytes()
            .await
            .unwrap();
        assert_eq!(read.as_ref(), b"manifest 782");

        let listed: Vec<_> = store
            .list(Some(&ObjectPath::from("root/manifest")))
            .try_collect()
            .await
            .unwrap();
        assert_eq!(
            listed
                .iter()
                .map(|meta| meta.location.clone())
                .collect::<Vec<_>>(),
            vec![target.clone()],
            "listings surface only the published manifest, never staging debris"
        );
        assert!(
            remote.files.lock().unwrap().contains_key(&orphan),
            "the orphan remains as invisible debris until swept"
        );

        pool.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn production_adapter_cleanup_overlaps_and_drains_before_pool_shutdown() {
        let state = Arc::new(ProtocolCleanupState::default());
        let pool = crate::sftp_transport::SftpSessionPool::new_writable(
            Arc::new(ProtocolCleanupFactory(state.clone())),
            3,
            3,
            3,
        )
        .await
        .unwrap();
        let store = Arc::new(SftpObjectStore::new(pool.clone(), ObjectPath::from("root")).unwrap());
        let operations = (0..3)
            .map(|index| {
                let store = store.clone();
                tokio::spawn(async move {
                    store
                        .put_opts(
                            &ObjectPath::from(format!("root/object-{index}")),
                            PutPayload::from_static(b"payload"),
                            PutOptions::default(),
                        )
                        .await
                })
            })
            .collect::<Vec<_>>();
        tokio::time::timeout(Duration::from_secs(1), async {
            while state.close_started.load(Ordering::SeqCst) != 3 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap_or_else(|_| {
            panic!(
                "cleanup did not overlap: dials={} live={} closes={}",
                state.dials.load(Ordering::SeqCst),
                state.live.load(Ordering::SeqCst),
                state.close_started.load(Ordering::SeqCst)
            )
        });

        let shutdown = tokio::spawn({
            let pool = pool.clone();
            async move { pool.shutdown().await }
        });
        tokio::task::yield_now().await;
        assert!(!shutdown.is_finished());
        state.release_close.notify_waiters();

        for operation in operations {
            let error = operation.await.unwrap().unwrap_err();
            assert!(matches!(error, object_store::Error::NotSupported { .. }));
        }
        tokio::time::timeout(Duration::from_secs(1), shutdown)
            .await
            .expect("shutdown drains production adapter cleanup")
            .unwrap()
            .unwrap();
        assert_eq!(state.dials.load(Ordering::SeqCst), 3);
        assert_eq!(state.live.load(Ordering::SeqCst), 0);
        assert_eq!(state.protocol_close_failures.load(Ordering::SeqCst), 3);
    }

    #[derive(Debug)]
    struct LocalSftpFactory {
        server: PathBuf,
        root: PathBuf,
        reads: Arc<AtomicUsize>,
        payload_reads: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl SessionFactory for LocalSftpFactory {
        async fn open(
            &self,
            _force: tokio_util::sync::CancellationToken,
        ) -> Result<Box<dyn TransportSession>, TransportError> {
            let mut child = tokio::process::Command::new(&self.server)
                .current_dir(&self.root)
                .stdin(std::process::Stdio::piped())
                .stdout(std::process::Stdio::piped())
                .kill_on_drop(true)
                .spawn()
                .map_err(|error| TransportError::Open(error.to_string()))?;
            let stdin = child
                .stdin
                .take()
                .ok_or_else(|| TransportError::Open("missing sftp-server stdin".to_owned()))?;
            let stdout = child
                .stdout
                .take()
                .ok_or_else(|| TransportError::Open("missing sftp-server stdout".to_owned()))?;
            let session = OpenSshTransportSession::from_streams(stdin, stdout).await?;
            Ok(Box::new(LocalSftpSession {
                session,
                child,
                reads: self.reads.clone(),
                payload_reads: self.payload_reads.clone(),
            }))
        }
    }

    struct LocalSftpSession {
        session: OpenSshTransportSession,
        child: tokio::process::Child,
        reads: Arc<AtomicUsize>,
        payload_reads: Arc<AtomicUsize>,
    }

    impl Debug for LocalSftpSession {
        fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter
                .debug_struct("LocalSftpSession")
                .finish_non_exhaustive()
        }
    }

    #[async_trait]
    impl TransportSession for LocalSftpSession {
        fn capabilities(&self) -> SftpCapabilities {
            self.session.capabilities()
        }

        async fn read_object(
            &mut self,
            path: &FilePath,
            range: Option<object_store::GetRange>,
            head: bool,
        ) -> Result<RemoteObjectRead, TransportError> {
            self.reads.fetch_add(1, Ordering::SeqCst);
            if !head {
                self.payload_reads.fetch_add(1, Ordering::SeqCst);
            }
            self.session.read_object(path, range, head).await
        }

        async fn list_directory(
            &mut self,
            path: &FilePath,
        ) -> Result<Vec<RemoteDirectoryEntry>, TransportError> {
            self.session.list_directory(path).await
        }

        async fn remove_file(&mut self, path: &FilePath) -> Result<(), TransportError> {
            self.session.remove_file(path).await
        }

        async fn ensure_directory_component(
            &mut self,
            path: &FilePath,
        ) -> Result<(), TransportError> {
            self.session.ensure_directory_component(path).await
        }

        async fn write_file_durable(
            &mut self,
            path: &FilePath,
            chunks: Vec<Bytes>,
        ) -> Result<(), TransportError> {
            self.session.write_file_durable(path, chunks).await
        }

        async fn write_file_at_durable(
            &mut self,
            path: &FilePath,
            offset: u64,
            chunks: Vec<Bytes>,
        ) -> Result<(), TransportError> {
            self.session
                .write_file_at_durable(path, offset, chunks)
                .await
        }

        async fn write_file_at(
            &mut self,
            path: &FilePath,
            offset: u64,
            chunks: Vec<Bytes>,
        ) -> Result<(), TransportError> {
            self.session.write_file_at(path, offset, chunks).await
        }

        async fn read_exact(
            &mut self,
            path: &FilePath,
            offset: u64,
            len: usize,
        ) -> Result<Bytes, TransportError> {
            self.session.read_exact(path, offset, len).await
        }

        async fn hard_link(
            &mut self,
            from: &FilePath,
            to: &FilePath,
        ) -> Result<(), TransportError> {
            self.session.hard_link(from, to).await
        }

        async fn posix_rename(
            &mut self,
            from: &FilePath,
            to: &FilePath,
        ) -> Result<(), TransportError> {
            self.session.posix_rename(from, to).await
        }

        async fn close(
            self: Box<Self>,
            force: tokio_util::sync::CancellationToken,
        ) -> Result<(), TransportError> {
            let LocalSftpSession {
                session, mut child, ..
            } = *self;
            Box::new(session).close(force).await?;
            let status = child
                .wait()
                .await
                .map_err(|error| TransportError::Close(error.to_string()))?;
            if !status.success() {
                return Err(TransportError::Close(format!(
                    "sftp-server exited with {status}"
                )));
            }
            Ok(())
        }
    }

    #[derive(Debug)]
    struct RecordingSession {
        capabilities: SftpCapabilities,
        files: Mutex<HashMap<PathBuf, Bytes>>,
        operations: Mutex<Vec<String>>,
        write_error: Option<&'static str>,
        remove_error: Option<&'static str>,
        remove_not_found: bool,
        scheduled_cleanups: AtomicUsize,
    }

    impl RecordingSession {
        fn new() -> Self {
            Self {
                capabilities: SftpCapabilities {
                    fsync: true,
                    hardlink: true,
                    posix_rename: true,
                },
                files: Mutex::new(HashMap::new()),
                operations: Mutex::new(Vec::new()),
                write_error: None,
                remove_error: None,
                remove_not_found: false,
                scheduled_cleanups: AtomicUsize::new(0),
            }
        }

        fn with_remove_failure() -> Self {
            Self {
                remove_error: Some("forced staging removal failure"),
                ..Self::new()
            }
        }

        fn with_write_and_remove_failure() -> Self {
            Self {
                write_error: Some("forced staging write failure"),
                remove_error: Some("forced staging removal failure"),
                ..Self::new()
            }
        }

        fn with_missing_cleanup() -> Self {
            Self {
                remove_not_found: true,
                ..Self::new()
            }
        }
    }

    #[derive(Debug)]
    struct LostReplySession {
        inner: RecordingSession,
        replace_target_header: bool,
        corrupt_target_header: bool,
        transient_read_call: Option<usize>,
        read_calls: AtomicUsize,
    }

    impl LostReplySession {
        fn matching() -> Self {
            Self {
                inner: RecordingSession::new(),
                replace_target_header: false,
                corrupt_target_header: false,
                transient_read_call: None,
                read_calls: AtomicUsize::new(0),
            }
        }

        fn matching_after_transient_read(call: usize) -> Self {
            Self {
                transient_read_call: Some(call),
                ..Self::matching()
            }
        }

        fn mismatching() -> Self {
            Self {
                inner: RecordingSession::new(),
                replace_target_header: true,
                corrupt_target_header: false,
                transient_read_call: None,
                read_calls: AtomicUsize::new(0),
            }
        }

        fn corrupting() -> Self {
            Self {
                inner: RecordingSession::new(),
                replace_target_header: false,
                corrupt_target_header: true,
                transient_read_call: None,
                read_calls: AtomicUsize::new(0),
            }
        }

        fn maybe_replace_target(&self, target: &FilePath) {
            if !self.replace_target_header && !self.corrupt_target_header {
                return;
            }
            let mut files = self.inner.files.lock().unwrap();
            let bytes = files.get_mut(target).expect("publication created target");
            let mut replacement = if self.corrupt_target_header {
                vec![0; OBJECT_HEADER_LEN]
            } else {
                encode_header(ObjectHeader {
                    generation: Uuid::nil(),
                    logical_len: 5,
                })
                .to_vec()
            };
            if !self.corrupt_target_header {
                replacement.extend_from_slice(b"other");
            }
            *bytes = replacement.into();
        }
    }

    #[derive(Debug)]
    struct ParallelMultipartSession {
        files: Mutex<HashMap<PathBuf, Bytes>>,
        part_barrier: Barrier,
        part_writes_in_flight: AtomicUsize,
        max_part_writes_in_flight: AtomicUsize,
        part_writes: AtomicUsize,
        durable_writes: AtomicUsize,
    }

    impl ParallelMultipartSession {
        fn new() -> Self {
            Self {
                files: Mutex::new(HashMap::new()),
                part_barrier: Barrier::new(2),
                part_writes_in_flight: AtomicUsize::new(0),
                max_part_writes_in_flight: AtomicUsize::new(0),
                part_writes: AtomicUsize::new(0),
                durable_writes: AtomicUsize::new(0),
            }
        }
    }

    #[async_trait]
    impl RemoteSession for ParallelMultipartSession {
        fn capabilities(&self) -> SftpCapabilities {
            SftpCapabilities {
                fsync: true,
                hardlink: true,
                posix_rename: true,
            }
        }

        async fn read_exact(
            &self,
            path: &FilePath,
            offset: u64,
            len: usize,
        ) -> RemoteResult<Bytes> {
            let files = self.files.lock().unwrap();
            let bytes = files
                .get(path)
                .ok_or_else(|| RemoteError::NotFound(path.display().to_string()))?;
            let start = usize::try_from(offset)
                .map_err(|_| RemoteError::Other("test offset overflow".to_owned()))?;
            let end = start + len;
            bytes
                .get(start..end)
                .map(Bytes::copy_from_slice)
                .ok_or_else(|| RemoteError::Other(format!("short read for {}", path.display())))
        }

        async fn write_file_durable(
            &self,
            path: &FilePath,
            chunks: Vec<Bytes>,
        ) -> RemoteResult<()> {
            let bytes = chunks.into_iter().flatten().collect::<Vec<_>>();
            self.files
                .lock()
                .unwrap()
                .insert(path.to_path_buf(), bytes.into());
            Ok(())
        }

        async fn write_file_at(
            &self,
            path: &FilePath,
            offset: u64,
            chunks: Vec<Bytes>,
        ) -> RemoteResult<()> {
            self.part_writes.fetch_add(1, Ordering::SeqCst);
            if offset >= OBJECT_HEADER_LEN as u64 {
                let current = self.part_writes_in_flight.fetch_add(1, Ordering::SeqCst) + 1;
                self.max_part_writes_in_flight
                    .fetch_max(current, Ordering::SeqCst);
                self.part_barrier.wait().await;
            }

            let mut files = self.files.lock().unwrap();
            let file = files
                .get_mut(path)
                .ok_or_else(|| RemoteError::NotFound(path.display().to_string()))?;
            let start = usize::try_from(offset)
                .map_err(|_| RemoteError::Other("test offset overflow".to_owned()))?;
            let bytes = chunks.into_iter().flatten().collect::<Vec<_>>();
            let end = start + bytes.len();
            let mut contents = file.to_vec();
            contents.resize(contents.len().max(end), 0);
            contents[start..end].copy_from_slice(&bytes);
            *file = contents.into();
            if offset >= OBJECT_HEADER_LEN as u64 {
                self.part_writes_in_flight.fetch_sub(1, Ordering::SeqCst);
            }
            Ok(())
        }

        async fn write_file_at_durable(
            &self,
            path: &FilePath,
            offset: u64,
            chunks: Vec<Bytes>,
        ) -> RemoteResult<()> {
            self.durable_writes.fetch_add(1, Ordering::SeqCst);
            let mut files = self.files.lock().unwrap();
            let file = files
                .get_mut(path)
                .ok_or_else(|| RemoteError::NotFound(path.display().to_string()))?;
            let start = usize::try_from(offset)
                .map_err(|_| RemoteError::Other("test offset overflow".to_owned()))?;
            let bytes = chunks.into_iter().flatten().collect::<Vec<_>>();
            let end = start + bytes.len();
            let mut contents = file.to_vec();
            contents.resize(contents.len().max(end), 0);
            contents[start..end].copy_from_slice(&bytes);
            *file = contents.into();
            Ok(())
        }

        async fn remove_file(&self, path: &FilePath) -> RemoteResult<()> {
            self.files.lock().unwrap().remove(path);
            Ok(())
        }

        async fn hard_link(&self, from: &FilePath, to: &FilePath) -> RemoteResult<()> {
            let mut files = self.files.lock().unwrap();
            let bytes = files
                .get(from)
                .cloned()
                .ok_or_else(|| RemoteError::NotFound(from.display().to_string()))?;
            files.insert(to.to_path_buf(), bytes);
            Ok(())
        }

        async fn posix_rename(&self, from: &FilePath, to: &FilePath) -> RemoteResult<()> {
            let mut files = self.files.lock().unwrap();
            let bytes = files
                .remove(from)
                .ok_or_else(|| RemoteError::NotFound(from.display().to_string()))?;
            files.insert(to.to_path_buf(), bytes);
            Ok(())
        }

        fn schedule_cleanup(&self, path: PathBuf) {
            self.files.lock().unwrap().remove(&path);
        }
    }

    #[tokio::test]
    async fn multipart_parts_write_in_parallel_before_atomic_completion() {
        let session = Arc::new(ParallelMultipartSession::new());
        let location = ObjectPath::from("zerofs/v1/parallel.bin");
        let target = FilePath::new("zerofs/v1/parallel.bin");
        let mut upload = SftpMultipartUpload::begin(
            session.clone(),
            location,
            target.to_path_buf(),
            Arc::new(DashSet::new()),
        )
        .await
        .unwrap();

        let first = upload.put_part(PutPayload::from_static(b"hello "));
        let second = upload.put_part(PutPayload::from_static(b"world"));
        tokio::time::timeout(
            Duration::from_secs(1),
            futures::future::try_join(first, second),
        )
        .await
        .expect("part uploads must make progress concurrently")
        .unwrap();

        assert_eq!(session.max_part_writes_in_flight.load(Ordering::SeqCst), 2);
        assert_eq!(session.part_writes.load(Ordering::SeqCst), 2);
        assert_eq!(session.durable_writes.load(Ordering::SeqCst), 0);
        assert!(!session.files.lock().unwrap().contains_key(target));
        upload.complete().await.unwrap();
        assert_eq!(session.durable_writes.load(Ordering::SeqCst), 1);

        let published = session.files.lock().unwrap().get(target).cloned().unwrap();
        assert_eq!(&published[OBJECT_HEADER_LEN..], b"hello world");
        assert_eq!(decode_header(&published).unwrap().logical_len, 11);
        assert!(
            session
                .files
                .lock()
                .unwrap()
                .keys()
                .all(|path| !is_staging_name(path.file_name().unwrap().as_ref()))
        );
    }

    #[tokio::test]
    async fn failed_multipart_abort_keeps_staging_armed_for_retry() {
        let session = Arc::new(RecordingSession::with_remove_failure());
        let location = ObjectPath::from("zerofs/v1/abort.bin");
        let target = PathBuf::from("zerofs/v1/abort.bin");
        let mut upload =
            SftpMultipartUpload::begin(session, location, target, Arc::new(DashSet::new()))
                .await
                .unwrap();

        upload.abort().await.expect_err("forced removal must fail");

        assert!(
            upload.staging.is_some(),
            "failed cleanup must remain armed for an explicit retry or Drop"
        );
        assert!(!upload.terminal);
    }

    #[tokio::test]
    async fn failed_cleanup_remains_armed_for_its_owner() {
        let session = Arc::new(RecordingSession::with_remove_failure());
        let staging = PathBuf::from("zerofs/v1/.zerofs-staging-object-id");
        let mut cleanup = StagingCleanup::new(session.clone(), staging.clone());

        let debt = cleanup
            .remove_now()
            .await
            .expect_err("forced removal must leave cleanup debt");

        assert_eq!(debt.path, staging);
        assert_eq!(cleanup.path.as_deref(), Some(staging.as_path()));
        drop(cleanup);
        tokio::task::yield_now().await;
        assert_eq!(session.scheduled_cleanups.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn missing_staging_path_completes_cleanup_idempotently() {
        let session = Arc::new(RecordingSession::with_missing_cleanup());
        let staging = PathBuf::from("zerofs/v1/.zerofs-staging-missing-id");
        let mut cleanup = StagingCleanup::new(session.clone(), staging);

        cleanup
            .remove_now()
            .await
            .expect("an already absent staging path has no cleanup debt");

        assert!(cleanup.path.is_none());
        drop(cleanup);
        assert_eq!(session.scheduled_cleanups.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn multipart_initiation_surfaces_staging_cleanup_debt() {
        let session = Arc::new(RecordingSession::with_write_and_remove_failure());
        let location = ObjectPath::from("zerofs/v1/failed-begin.bin");
        let target = PathBuf::from("zerofs/v1/failed-begin.bin");

        let error = SftpMultipartUpload::begin(session, location, target, Arc::new(DashSet::new()))
            .await
            .expect_err("failed initiation must report both write failure and cleanup debt");

        match error {
            RemoteError::CleanupRequired { operation, debt } => {
                assert!(matches!(
                    *operation,
                    RemoteError::Other(ref message) if message == "forced staging write failure"
                ));
                assert!(matches!(
                    *debt.error,
                    RemoteError::Other(ref message) if message == "forced staging removal failure"
                ));
            }
            error => panic!("expected cleanup debt, got {error:?}"),
        }
    }

    #[tokio::test]
    async fn lost_publication_reply_reconciles_matching_target_header() {
        for mode in [PublicationMode::Create, PublicationMode::Overwrite] {
            let session = Arc::new(LostReplySession::matching());
            let target = FilePath::new("/objects/segment.bin");

            let outcome = publish_payload(
                session,
                target,
                vec![Bytes::from_static(b"payload")],
                mode,
                None,
            )
            .await
            .expect("matching target header proves the atomic publication committed");

            assert_eq!(outcome.header.logical_len, 7);
        }
    }

    #[tokio::test]
    async fn lost_publication_reply_with_mismatching_target_remains_an_error() {
        let session = Arc::new(LostReplySession::mismatching());

        let error = publish_payload(
            session,
            FilePath::new("/objects/segment.bin"),
            vec![Bytes::from_static(b"payload")],
            PublicationMode::Create,
            None,
        )
        .await
        .expect_err("a different target generation cannot reconcile this publication");

        assert!(matches!(
            error,
            RemoteError::Other(ref message) if message == "publication reply lost"
        ));
    }

    #[tokio::test]
    async fn lost_publication_reply_retries_reconciliation_with_the_original_header() {
        let create = Arc::new(LostReplySession::matching_after_transient_read(1));
        let target = FilePath::new("/objects/create.bin");
        let created = publish_payload(
            create.clone(),
            target,
            vec![Bytes::from_static(b"create")],
            PublicationMode::Create,
            None,
        )
        .await
        .expect("create reconciliation must retain its committed generation");
        assert_eq!(created.header.logical_len, 6);
        assert_eq!(create.read_calls.load(Ordering::SeqCst), 2);

        let update = Arc::new(LostReplySession::matching_after_transient_read(2));
        let target = FilePath::new("/objects/update.bin");
        let current = ObjectHeader {
            generation: Uuid::new_v4(),
            logical_len: 7,
        };
        update.inner.files.lock().unwrap().insert(
            target.to_path_buf(),
            Bytes::copy_from_slice(&encode_header(current)),
        );
        let updated = publish_payload(
            update.clone(),
            target,
            vec![Bytes::from_static(b"updated")],
            PublicationMode::Update,
            Some(current.generation),
        )
        .await
        .expect("update reconciliation must retain its committed generation");
        assert_eq!(updated.header.logical_len, 7);
        assert_eq!(update.read_calls.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn corrupt_reconciliation_target_is_a_terminal_typed_error() {
        let error = publish_payload(
            Arc::new(LostReplySession::corrupting()),
            FilePath::new("/objects/corrupt.bin"),
            vec![Bytes::from_static(b"payload")],
            PublicationMode::Create,
            None,
        )
        .await
        .expect_err("corrupt target cannot prove that publication committed");

        assert!(matches!(error, RemoteError::CorruptObject(_)));
        assert!(!error.is_retryable());
    }

    #[async_trait]
    impl RemoteSession for LostReplySession {
        fn capabilities(&self) -> SftpCapabilities {
            self.inner.capabilities()
        }

        async fn read_exact(
            &self,
            path: &FilePath,
            offset: u64,
            len: usize,
        ) -> RemoteResult<Bytes> {
            let call = self.read_calls.fetch_add(1, Ordering::SeqCst) + 1;
            if self.transient_read_call == Some(call) {
                return Err(RemoteError::Other(
                    "transient reconciliation read failure".to_owned(),
                ));
            }
            self.inner.read_exact(path, offset, len).await
        }

        async fn write_file_durable(
            &self,
            path: &FilePath,
            chunks: Vec<Bytes>,
        ) -> RemoteResult<()> {
            self.inner.write_file_durable(path, chunks).await
        }

        async fn remove_file(&self, path: &FilePath) -> RemoteResult<()> {
            self.inner.remove_file(path).await
        }

        async fn hard_link(&self, from: &FilePath, to: &FilePath) -> RemoteResult<()> {
            self.inner.hard_link(from, to).await?;
            self.maybe_replace_target(to);
            Err(RemoteError::Other("publication reply lost".to_owned()))
        }

        async fn posix_rename(&self, from: &FilePath, to: &FilePath) -> RemoteResult<()> {
            self.inner.posix_rename(from, to).await?;
            self.maybe_replace_target(to);
            Err(RemoteError::Other("publication reply lost".to_owned()))
        }

        fn schedule_cleanup(&self, path: PathBuf) {
            self.inner.schedule_cleanup(path);
        }
    }

    #[async_trait]
    impl RemoteSession for RecordingSession {
        fn capabilities(&self) -> SftpCapabilities {
            self.capabilities
        }

        async fn read_exact(
            &self,
            path: &FilePath,
            offset: u64,
            len: usize,
        ) -> RemoteResult<Bytes> {
            let files = self.files.lock().unwrap();
            let bytes = files
                .get(path)
                .ok_or_else(|| RemoteError::NotFound(path.display().to_string()))?;
            let start = offset as usize;
            let end = start + len;
            bytes
                .get(start..end)
                .map(Bytes::copy_from_slice)
                .ok_or_else(|| RemoteError::Other(format!("short read for {}", path.display())))
        }

        async fn write_file_durable(
            &self,
            path: &FilePath,
            chunks: Vec<Bytes>,
        ) -> RemoteResult<()> {
            if let Some(error) = self.write_error {
                return Err(RemoteError::Other(error.to_owned()));
            }
            let bytes = chunks.into_iter().flatten().collect::<Vec<_>>();
            self.files
                .lock()
                .unwrap()
                .insert(path.to_path_buf(), bytes.into());
            self.operations
                .lock()
                .unwrap()
                .extend(["create", "write", "fsync", "close"].map(str::to_owned));
            Ok(())
        }

        async fn remove_file(&self, path: &FilePath) -> RemoteResult<()> {
            self.operations.lock().unwrap().push("remove".to_owned());
            if self.remove_not_found {
                return Err(RemoteError::NotFound(path.display().to_string()));
            }
            if let Some(error) = self.remove_error {
                return Err(RemoteError::Other(error.to_owned()));
            }
            self.files.lock().unwrap().remove(path);
            Ok(())
        }

        async fn hard_link(&self, from: &FilePath, to: &FilePath) -> RemoteResult<()> {
            let mut files = self.files.lock().unwrap();
            if files.contains_key(to) {
                return Err(RemoteError::AlreadyExists(to.display().to_string()));
            }
            let bytes = files
                .get(from)
                .cloned()
                .ok_or_else(|| RemoteError::NotFound(from.display().to_string()))?;
            files.insert(to.to_path_buf(), bytes);
            self.operations.lock().unwrap().push("hardlink".to_owned());
            Ok(())
        }

        async fn posix_rename(&self, from: &FilePath, to: &FilePath) -> RemoteResult<()> {
            let bytes = self
                .files
                .lock()
                .unwrap()
                .remove(from)
                .ok_or_else(|| RemoteError::NotFound(from.display().to_string()))?;
            self.files.lock().unwrap().insert(to.to_path_buf(), bytes);
            self.operations
                .lock()
                .unwrap()
                .push("posix-rename".to_owned());
            Ok(())
        }

        fn schedule_cleanup(&self, path: PathBuf) {
            self.scheduled_cleanups.fetch_add(1, Ordering::SeqCst);
            if self.remove_error.is_none() {
                self.files.lock().unwrap().remove(&path);
            }
        }
    }

    #[derive(Debug)]
    struct RacingSession {
        inner: RecordingSession,
        reads: Barrier,
    }

    impl RacingSession {
        fn new() -> Self {
            Self {
                inner: RecordingSession::new(),
                reads: Barrier::new(2),
            }
        }
    }

    #[async_trait]
    impl RemoteSession for RacingSession {
        fn capabilities(&self) -> SftpCapabilities {
            self.inner.capabilities()
        }

        async fn read_exact(
            &self,
            path: &FilePath,
            offset: u64,
            len: usize,
        ) -> RemoteResult<Bytes> {
            let snapshot = self.inner.read_exact(path, offset, len).await?;
            let _ = tokio::time::timeout(Duration::from_secs(1), self.reads.wait()).await;
            Ok(snapshot)
        }

        async fn write_file_durable(
            &self,
            path: &FilePath,
            chunks: Vec<Bytes>,
        ) -> RemoteResult<()> {
            self.inner.write_file_durable(path, chunks).await
        }

        async fn remove_file(&self, path: &FilePath) -> RemoteResult<()> {
            self.inner.remove_file(path).await
        }

        async fn hard_link(&self, from: &FilePath, to: &FilePath) -> RemoteResult<()> {
            self.inner.hard_link(from, to).await
        }

        async fn posix_rename(&self, from: &FilePath, to: &FilePath) -> RemoteResult<()> {
            self.inner.posix_rename(from, to).await
        }

        fn schedule_cleanup(&self, path: PathBuf) {
            self.inner.schedule_cleanup(path);
        }
    }

    #[test]
    fn object_header_round_trips_generation_and_logical_length() {
        let expected = ObjectHeader {
            generation: Uuid::from_u128(0x00112233_4455_6677_8899_aabbccddeeff),
            logical_len: 0x0102_0304_0506_0708,
        };

        let encoded = encode_header(expected);
        let decoded = decode_header(&encoded).expect("valid header");

        assert_eq!(decoded, expected);
        assert_eq!(&encoded[..8], b"ZEROFS\x01\0");
    }

    #[test]
    fn publication_is_gated_by_the_extensions_that_make_each_mode_safe() {
        let all = SftpCapabilities {
            fsync: true,
            hardlink: true,
            posix_rename: true,
        };

        assert_eq!(
            validate_publication_capabilities(
                SftpCapabilities {
                    fsync: false,
                    ..all
                },
                PublicationMode::Overwrite
            ),
            Err("fsync")
        );
        assert_eq!(
            validate_publication_capabilities(
                SftpCapabilities {
                    hardlink: false,
                    ..all
                },
                PublicationMode::Create
            ),
            Err("hardlink")
        );
        assert_eq!(
            validate_publication_capabilities(
                SftpCapabilities {
                    posix_rename: false,
                    ..all
                },
                PublicationMode::Update
            ),
            Err("posix-rename")
        );
        assert_eq!(
            validate_publication_capabilities(all, PublicationMode::Create),
            Ok(())
        );
        assert_eq!(
            validate_publication_capabilities(all, PublicationMode::Overwrite),
            Ok(())
        );
        assert_eq!(
            validate_publication_capabilities(all, PublicationMode::Update),
            Ok(())
        );
    }

    #[test]
    fn staging_files_are_hidden_siblings_and_are_filtered_from_listings() {
        let target = FilePath::new("/objects/nested/segment.bin");
        let staging = staging_path(
            target,
            Uuid::from_u128(0x00112233_4455_6677_8899_aabbccddeeff),
        )
        .expect("target has a filename");

        assert_eq!(
            staging,
            FilePath::new(
                "/objects/nested/.zerofs-staging-segment.bin-00112233-4455-6677-8899-aabbccddeeff"
            )
        );
        assert_eq!(staging.parent(), target.parent());
        assert!(is_staging_name(staging.file_name().unwrap().as_ref()));
        assert!(!is_staging_name(FilePath::new("segment.bin")));
        assert!(!is_staging_name(FilePath::new(".zerofs-staging-user-file")));
    }

    #[tokio::test]
    async fn overwrite_is_durable_before_atomic_publication() {
        let session = Arc::new(RecordingSession::new());
        let target = FilePath::new("/objects/segment.bin");

        let outcome = publish_payload(
            session.clone(),
            target,
            vec![Bytes::from_static(b"payload")],
            PublicationMode::Overwrite,
            None,
        )
        .await
        .expect("publication succeeds");
        let header = outcome.header;

        assert!(outcome.cleanup_debt.is_none());
        assert_eq!(header.logical_len, 7);
        assert_eq!(header.generation.get_version_num(), 4);
        assert_eq!(
            session.operations.lock().unwrap().as_slice(),
            ["create", "write", "fsync", "close", "posix-rename"]
        );
        let published = session.files.lock().unwrap().get(target).cloned().unwrap();
        assert_eq!(&published[OBJECT_HEADER_LEN..], b"payload");
        assert_eq!(decode_header(&published).unwrap(), header);
        assert!(
            session
                .files
                .lock()
                .unwrap()
                .keys()
                .all(|path| !is_staging_name(path.file_name().unwrap().as_ref()))
        );
    }

    #[tokio::test]
    async fn rejected_stale_update_cleans_its_unpublished_staging_file() {
        let session = Arc::new(RecordingSession::new());
        let target = FilePath::new("/objects/segment.bin");
        let current = ObjectHeader {
            generation: Uuid::from_u128(0x00112233_4455_6677_8899_aabbccddeeff),
            logical_len: 8,
        };
        let mut original = encode_header(current).to_vec();
        original.extend_from_slice(b"original");
        session
            .files
            .lock()
            .unwrap()
            .insert(target.to_path_buf(), original.into());

        let error = publish_payload(
            session.clone(),
            target,
            vec![Bytes::from_static(b"replacement")],
            PublicationMode::Update,
            Some(Uuid::from_u128(0xffeeddcc_bbaa_9988_7766_554433221100)),
        )
        .await
        .expect_err("stale generation must be rejected");

        assert!(matches!(error, RemoteError::Precondition(_)));
        let files = session.files.lock().unwrap();
        assert_eq!(files.len(), 1);
        assert_eq!(decode_header(files.get(target).unwrap()).unwrap(), current);
        assert!(
            files
                .keys()
                .all(|path| !is_staging_name(path.file_name().unwrap().as_ref()))
        );
        assert_eq!(
            session.operations.lock().unwrap().as_slice(),
            ["create", "write", "fsync", "close", "remove"]
        );
    }

    #[tokio::test]
    async fn corrupt_update_target_header_is_a_terminal_typed_error() {
        let session = Arc::new(RecordingSession::new());
        let target = FilePath::new("/objects/segment.bin");
        session.files.lock().unwrap().insert(
            target.to_path_buf(),
            Bytes::from(vec![0; OBJECT_HEADER_LEN]),
        );

        let error = publish_payload(
            session.clone(),
            target,
            vec![Bytes::from_static(b"replacement")],
            PublicationMode::Update,
            Some(Uuid::nil()),
        )
        .await
        .expect_err("a corrupt target header must reject a conditional update");

        assert!(matches!(error, RemoteError::CorruptObject(_)));
        assert!(!error.is_retryable());
        assert!(
            session
                .files
                .lock()
                .unwrap()
                .keys()
                .all(|path| !is_staging_name(path.file_name().unwrap().as_ref()))
        );
    }

    #[tokio::test(start_paused = true)]
    async fn concurrent_updates_with_one_expected_generation_have_one_winner() {
        let session = Arc::new(RacingSession::new());
        let target = PathBuf::from("/objects/segment.bin");
        let current = ObjectHeader {
            generation: Uuid::from_u128(0x00112233_4455_6677_8899_aabbccddeeff),
            logical_len: 8,
        };
        let mut original = encode_header(current).to_vec();
        original.extend_from_slice(b"original");
        session
            .inner
            .files
            .lock()
            .unwrap()
            .insert(target.clone(), original.into());

        let first = {
            let session = session.clone();
            let target = target.clone();
            tokio::spawn(async move {
                publish_payload(
                    session,
                    &target,
                    vec![Bytes::from_static(b"first")],
                    PublicationMode::Update,
                    Some(current.generation),
                )
                .await
            })
        };
        let second = {
            let session = session.clone();
            let target = target.clone();
            tokio::spawn(async move {
                publish_payload(
                    session,
                    &target,
                    vec![Bytes::from_static(b"second")],
                    PublicationMode::Update,
                    Some(current.generation),
                )
                .await
            })
        };

        let results = [first.await.unwrap(), second.await.unwrap()];
        assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 1);
        assert_eq!(
            results
                .iter()
                .filter(|result| matches!(result, Err(RemoteError::Precondition(_))))
                .count(),
            1
        );
    }

    #[tokio::test]
    async fn stale_update_remove_failure_reports_cleanup_debt() {
        let session = Arc::new(RecordingSession::with_remove_failure());
        let target = FilePath::new("/objects/segment.bin");
        let current = ObjectHeader {
            generation: Uuid::from_u128(0x00112233_4455_6677_8899_aabbccddeeff),
            logical_len: 8,
        };
        let mut original = encode_header(current).to_vec();
        original.extend_from_slice(b"original");
        session
            .files
            .lock()
            .unwrap()
            .insert(target.to_path_buf(), original.into());

        let error = publish_payload(
            session.clone(),
            target,
            vec![Bytes::from_static(b"replacement")],
            PublicationMode::Update,
            Some(Uuid::from_u128(0xffeeddcc_bbaa_9988_7766_554433221100)),
        )
        .await
        .expect_err("stale update must fail");

        let (operation, debt) = match error {
            RemoteError::CleanupRequired { operation, debt } => (operation, debt),
            error => panic!("expected cleanup debt, got {error:?}"),
        };
        assert!(matches!(*operation, RemoteError::Precondition(_)));
        let files = session.files.lock().unwrap();
        let staging = files
            .keys()
            .find(|path| is_staging_name(path.file_name().unwrap().as_ref()))
            .expect("failed removal leaves staging for a reaper");
        assert_eq!(debt.path, *staging);
        assert!(matches!(
            *debt.error,
            RemoteError::Other(ref message) if message == "forced staging removal failure"
        ));
    }

    #[tokio::test]
    async fn create_remove_failure_returns_committed_outcome_with_cleanup_debt() {
        let session = Arc::new(RecordingSession::with_remove_failure());
        let target = FilePath::new("/objects/segment.bin");

        let outcome = publish_payload(
            session.clone(),
            target,
            vec![Bytes::from_static(b"payload")],
            PublicationMode::Create,
            None,
        )
        .await
        .expect("hardlink committed the create");

        let files = session.files.lock().unwrap();
        assert!(files.contains_key(target));
        let staging = files
            .keys()
            .find(|path| is_staging_name(path.file_name().unwrap().as_ref()))
            .expect("failed removal leaves staging for a reaper");
        let debt = outcome
            .cleanup_debt
            .expect("committed create reports cleanup debt");
        assert_eq!(debt.path, *staging);
        assert!(matches!(
            *debt.error,
            RemoteError::Other(ref message) if message == "forced staging removal failure"
        ));
    }

    #[tokio::test]
    async fn create_uses_hardlink_then_removes_staging() {
        let session = Arc::new(RecordingSession::new());
        let target = FilePath::new("/objects/segment.bin");

        let outcome = publish_payload(
            session.clone(),
            target,
            vec![Bytes::from_static(b"payload")],
            PublicationMode::Create,
            None,
        )
        .await
        .expect("create succeeds");

        assert!(outcome.cleanup_debt.is_none());
        assert_eq!(
            session.operations.lock().unwrap().as_slice(),
            ["create", "write", "fsync", "close", "hardlink", "remove"]
        );
        let files = session.files.lock().unwrap();
        assert_eq!(files.len(), 1);
        assert_eq!(
            decode_header(files.get(target).unwrap()).unwrap(),
            outcome.header
        );
    }

    #[tokio::test]
    async fn update_with_current_generation_uses_posix_rename() {
        let session = Arc::new(RecordingSession::new());
        let target = FilePath::new("/objects/segment.bin");
        let current = ObjectHeader {
            generation: Uuid::from_u128(0x00112233_4455_6677_8899_aabbccddeeff),
            logical_len: 8,
        };
        let mut original = encode_header(current).to_vec();
        original.extend_from_slice(b"original");
        session
            .files
            .lock()
            .unwrap()
            .insert(target.to_path_buf(), original.into());

        let outcome = publish_payload(
            session.clone(),
            target,
            vec![Bytes::from_static(b"replacement")],
            PublicationMode::Update,
            Some(current.generation),
        )
        .await
        .expect("current generation update succeeds");

        assert!(outcome.cleanup_debt.is_none());
        assert_eq!(
            session.operations.lock().unwrap().as_slice(),
            ["create", "write", "fsync", "close", "posix-rename"]
        );
        let published = session.files.lock().unwrap().get(target).cloned().unwrap();
        assert_eq!(&published[OBJECT_HEADER_LEN..], b"replacement");
        assert_eq!(decode_header(&published).unwrap(), outcome.header);
    }

    #[tokio::test]
    async fn real_sftp_object_store_hides_headers_ranges_lists_and_deletes() {
        let Some(server) = ["/usr/libexec/sftp-server", "/usr/lib/openssh/sftp-server"]
            .into_iter()
            .find(|path| FilePath::new(path).is_file())
        else {
            eprintln!(
                "skipped: neither /usr/libexec/sftp-server nor /usr/lib/openssh/sftp-server exists"
            );
            return;
        };
        let root = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(root.path().join("zerofs/v1/nested")).unwrap();
        let write_object = |path: &FilePath, generation: Uuid, payload: &[u8]| {
            let mut physical = encode_header(ObjectHeader {
                generation,
                logical_len: payload.len() as u64,
            })
            .to_vec();
            physical.extend_from_slice(payload);
            std::fs::write(path, physical).unwrap();
        };
        let top_generation = Uuid::from_u128(0x00112233_4455_6677_8899_aabbccddeeff);
        write_object(
            &root.path().join("zerofs/v1/top.bin"),
            top_generation,
            b"hello world",
        );
        write_object(
            &root.path().join("zerofs/v1/nested/child.bin"),
            Uuid::from_u128(0xffeeddcc_bbaa_9988_7766_554433221100),
            b"child",
        );
        write_object(
            &root.path().join("zerofs/v1/empty.bin"),
            Uuid::from_u128(0x11111111_2222_3333_4444_555555555555),
            b"",
        );
        write_object(
            &root
                .path()
                .join("zerofs/v1/.zerofs-staging-top.bin-00112233-4455-6677-8899-aabbccddeeff"),
            Uuid::new_v4(),
            b"hidden",
        );

        let reads = Arc::new(AtomicUsize::new(0));
        let payload_reads = Arc::new(AtomicUsize::new(0));
        let pool = crate::sftp_transport::SftpSessionPool::new_writable(
            Arc::new(LocalSftpFactory {
                server: server.into(),
                root: root.path().to_path_buf(),
                reads: reads.clone(),
                payload_reads: payload_reads.clone(),
            }),
            2,
            1,
            1,
        )
        .await
        .unwrap();
        let store = SftpObjectStore::new(pool, ObjectPath::from("zerofs/v1")).unwrap();
        let location = ObjectPath::from("zerofs/v1/top.bin");

        let missing = ObjectPath::from("zerofs/v1/eventually-created.bin");
        assert!(matches!(
            store.get(&missing).await.unwrap_err(),
            object_store::Error::NotFound { .. }
        ));
        assert!(matches!(
            store.get(&missing).await.unwrap_err(),
            object_store::Error::NotFound { .. }
        ));
        assert_eq!(
            reads.load(Ordering::SeqCst),
            1,
            "a repeated known miss must not consume another SFTP request"
        );
        store
            .put(&missing, PutPayload::from_static(b"now present"))
            .await
            .unwrap();
        assert_eq!(
            store.get(&missing).await.unwrap().bytes().await.unwrap(),
            Bytes::from_static(b"now present"),
            "a successful publication must invalidate the negative entry"
        );
        assert_eq!(reads.load(Ordering::SeqCst), 2);
        store
            .delete_stream(stream::iter([Ok(missing)]).boxed())
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<object_store::Result<Vec<_>>>()
            .unwrap();

        let result = store
            .get_opts(
                &location,
                GetOptions::new().with_range(Some(object_store::GetRange::Bounded(6..11))),
            )
            .await
            .unwrap();
        assert_eq!(result.meta.size, 11);
        assert_eq!(
            result.meta.e_tag.as_deref(),
            Some(&*top_generation.to_string())
        );
        assert_eq!(result.range, 6..11);
        assert_eq!(result.bytes().await.unwrap().as_ref(), b"world");

        let payload_reads_before = payload_reads.load(Ordering::SeqCst);
        let not_modified = store
            .get_opts(
                &location,
                GetOptions {
                    if_none_match: Some(top_generation.to_string()),
                    ..Default::default()
                },
            )
            .await
            .unwrap_err();
        assert!(matches!(
            not_modified,
            object_store::Error::NotModified { .. }
        ));
        assert_eq!(
            payload_reads.load(Ordering::SeqCst),
            payload_reads_before,
            "a rejected conditional GET must not download object payload"
        );

        let head = store
            .get_opts(
                &location,
                GetOptions {
                    head: true,
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        assert_eq!(head.meta.size, 11);
        assert!(head.bytes().await.unwrap().is_empty());
        let empty = store
            .get_opts(
                &ObjectPath::from("zerofs/v1/empty.bin"),
                GetOptions::default(),
            )
            .await
            .unwrap();
        assert_eq!(empty.range, 0..0);
        assert_eq!(empty.meta.size, 0);
        assert!(empty.bytes().await.unwrap().is_empty());

        let mut listed = store
            .list(None)
            .map(|result| result.unwrap().location)
            .collect::<Vec<_>>()
            .await;
        listed.sort();
        assert_eq!(
            listed,
            [
                ObjectPath::from("zerofs/v1/empty.bin"),
                ObjectPath::from("zerofs/v1/nested/child.bin"),
                ObjectPath::from("zerofs/v1/top.bin"),
            ]
        );
        let delimiter = store.list_with_delimiter(None).await.unwrap();
        assert_eq!(
            delimiter.common_prefixes,
            [ObjectPath::from("zerofs/v1/nested")]
        );
        assert_eq!(delimiter.objects.len(), 2);
        assert!(
            delimiter
                .objects
                .iter()
                .any(|object| object.location == location)
        );
        let exact = store
            .list(Some(&location))
            .map(|result| result.unwrap().location)
            .collect::<Vec<_>>()
            .await;
        assert_eq!(
            exact.as_slice(),
            std::slice::from_ref(&location),
            "an exact object path is also a valid list prefix"
        );

        let deleted = store
            .delete_stream(stream::iter(vec![Ok(location.clone())]).boxed())
            .collect::<Vec<_>>()
            .await;
        assert_eq!(deleted.len(), 1);
        assert_eq!(deleted[0].as_ref().unwrap(), &location);
        assert!(!root.path().join("zerofs/v1/top.bin").exists());

        let outside = ObjectPath::from("other/prefix.bin");
        assert!(
            store
                .get_opts(&outside, GetOptions::default())
                .await
                .is_err()
        );

        let written = ObjectPath::from("zerofs/v1/new/deep/written.bin");
        let first = store
            .put_opts(
                &written,
                PutPayload::from_static(b"first"),
                PutOptions::default(),
            )
            .await
            .unwrap();
        assert_eq!(
            store
                .get_opts(&written, GetOptions::default())
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap()
                .as_ref(),
            b"first"
        );
        let create_error = store
            .put_opts(
                &written,
                PutPayload::from_static(b"must not replace"),
                PutOptions {
                    mode: PutMode::Create,
                    ..Default::default()
                },
            )
            .await
            .unwrap_err();
        assert!(matches!(
            create_error,
            object_store::Error::AlreadyExists { .. }
        ));
        let second = store
            .put_opts(
                &written,
                PutPayload::from_static(b"second"),
                PutOptions {
                    mode: PutMode::Update(first.clone().into()),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        let stale = store
            .put_opts(
                &written,
                PutPayload::from_static(b"stale"),
                PutOptions {
                    mode: PutMode::Update(first.into()),
                    ..Default::default()
                },
            )
            .await
            .unwrap_err();
        assert!(matches!(stale, object_store::Error::Precondition { .. }));
        assert!(second.e_tag.is_some());

        let multipart_location = ObjectPath::from("zerofs/v1/multipart.bin");
        let mut multipart = store
            .put_multipart_opts(&multipart_location, PutMultipartOptions::default())
            .await
            .unwrap();
        assert!(matches!(
            store.get(&multipart_location).await.unwrap_err(),
            object_store::Error::NotFound { .. }
        ));
        let first_part = multipart.put_part(PutPayload::from_static(b"hello "));
        let second_part = multipart.put_part(PutPayload::from_static(b"multipart"));
        futures::future::try_join(first_part, second_part)
            .await
            .unwrap();
        multipart.complete().await.unwrap();
        assert_eq!(
            store
                .get_opts(&multipart_location, GetOptions::default())
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap()
                .as_ref(),
            b"hello multipart"
        );
    }
}
