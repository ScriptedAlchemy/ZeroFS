use async_trait::async_trait;
use bytes::Bytes;
use std::collections::HashMap;
use std::fmt::Debug;
use std::path::{Path as FilePath, PathBuf};
use std::sync::{Arc, LazyLock, Mutex as StdMutex, Weak};
use tokio::sync::Mutex as AsyncMutex;
use uuid::Uuid;

pub const OBJECT_HEADER_LEN: usize = 32;
const OBJECT_HEADER_MAGIC: &[u8; 8] = b"ZEROFS\x01\0";

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
    #[error("{operation}; cleanup required: {debt}")]
    CleanupRequired {
        operation: Box<RemoteError>,
        debt: StagingCleanupDebt,
    },
    #[error("remote SFTP operation failed: {0}")]
    Other(String),
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
    async fn remove_file(&self, path: &FilePath) -> RemoteResult<()>;
    async fn hard_link(&self, from: &FilePath, to: &FilePath) -> RemoteResult<()>;
    async fn posix_rename(&self, from: &FilePath, to: &FilePath) -> RemoteResult<()>;
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
        RemoteError::Other(format!("SFTP server lacks required {extension} extension"))
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
    let staging = staging_path(target, header.generation).map_err(RemoteError::Other)?;
    let mut cleanup = StagingCleanup::new(session.clone(), staging.clone());
    let mut physical_payload = Vec::with_capacity(payload.len() + 1);
    physical_payload.push(Bytes::copy_from_slice(&encode_header(header)));
    physical_payload.extend(payload);

    if let Err(error) = session.write_file_durable(&staging, physical_payload).await {
        return Err(failure_with_cleanup(&mut cleanup, error).await);
    }

    if mode == PublicationMode::Create {
        return match session.hard_link(&staging, target).await {
            Ok(()) => Ok(PublicationOutcome {
                header,
                cleanup_debt: cleanup.remove_now().await.err(),
            }),
            Err(error) => Err(failure_with_cleanup(&mut cleanup, error).await),
        };
    }

    let publication = match mode {
        PublicationMode::Overwrite => session.posix_rename(&staging, target).await,
        PublicationMode::Update => match expected_generation {
            None => Err(RemoteError::Precondition(
                "Update requires an expected generation".to_owned(),
            )),
            Some(expected) => match session
                .read_exact(target, 0, OBJECT_HEADER_LEN)
                .await
                .and_then(|bytes| decode_header(&bytes).map_err(RemoteError::Other))
            {
                Err(error) => Err(error),
                Ok(current) if current.generation != expected => {
                    Err(RemoteError::Precondition(format!(
                        "expected generation {expected}, found {}",
                        current.generation
                    )))
                }
                Ok(_) => session.posix_rename(&staging, target).await,
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

async fn failure_with_cleanup(cleanup: &mut StagingCleanup, operation: RemoteError) -> RemoteError {
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
            Err(error) => {
                self.disarm();
                Err(StagingCleanupDebt {
                    path,
                    error: Box::new(error),
                })
            }
        }
    }
}

impl Drop for StagingCleanup {
    fn drop(&mut self) {
        let Some(path) = self.path.take() else {
            return;
        };
        let session = self.session.clone();
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            runtime.spawn(async move {
                if let Err(error) = session.remove_file(&path).await {
                    tracing::warn!(path = %path.display(), %error, "failed to clean cancelled SFTP staging write");
                }
            });
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::sync::Mutex;
    use std::time::Duration;
    use tokio::sync::Barrier;

    #[derive(Debug)]
    struct RecordingSession {
        capabilities: SftpCapabilities,
        files: Mutex<HashMap<PathBuf, Bytes>>,
        operations: Mutex<Vec<String>>,
        remove_error: Option<&'static str>,
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
                remove_error: None,
            }
        }

        fn with_remove_failure() -> Self {
            Self {
                remove_error: Some("forced staging removal failure"),
                ..Self::new()
            }
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
}
