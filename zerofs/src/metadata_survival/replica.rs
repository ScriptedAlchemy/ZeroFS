//! A metadata replica handle over a [`ReplicaBackend`], and
//! [`LocalDirectoryBackend`], the existing local backend.
//!
//! Survival code talks only to [`Replica`], which adds test fault injection
//! and maps backend errors to survival failures. Nothing above this module
//! touches a path, a directory or a file lock.

use super::SurvivalFailure;
use super::backend::{Fence, Presence, ReplicaBackend, ReplicaError, Staged};
use super::format::ROOT;
use bytes::Bytes;
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// Faults injected above the backend by acceptance tests. Production code
/// never sets one; every mutating call consults it so an injected failure
/// travels the same path a backend's `Full`, `Io` or stall would.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum InjectedFault {
    Full,
    Io,
    Slow(Duration),
}

#[derive(Debug, Default)]
pub(crate) struct FaultSlot(Mutex<Option<InjectedFault>>);

impl FaultSlot {
    pub(crate) fn set(&self, fault: Option<InjectedFault>) {
        *self.0.lock().unwrap_or_else(|p| p.into_inner()) = fault;
    }

    pub(crate) fn get(&self) -> Option<InjectedFault> {
        *self.0.lock().unwrap_or_else(|p| p.into_inner())
    }
}

pub(crate) fn classify(error: &ReplicaError) -> SurvivalFailure {
    match error {
        ReplicaError::Absent => SurvivalFailure::Lost,
        ReplicaError::Full => SurvivalFailure::Full,
        ReplicaError::Locked => SurvivalFailure::Locked,
        ReplicaError::Uninitialized => SurvivalFailure::Stale,
        ReplicaError::Io(_) => SurvivalFailure::Io,
    }
}

pub(crate) fn key(relative: &Path) -> String {
    relative
        .components()
        .map(|part| part.as_os_str().to_string_lossy())
        .collect::<Vec<_>>()
        .join("/")
}

#[derive(Debug)]
pub(crate) struct Replica {
    pub(crate) id: String,
    /// The configured device binding; checked against the backend identity.
    pub(crate) device_id: String,
    pub(crate) backend: Arc<dyn ReplicaBackend>,
    pub(crate) fault: Arc<FaultSlot>,
}

impl Replica {
    pub(crate) fn describe(&self) -> String {
        self.backend.describe()
    }

    async fn before_io(&self) -> Result<(), ReplicaError> {
        match self.fault.get() {
            None => Ok(()),
            Some(InjectedFault::Full) => Err(ReplicaError::Full),
            Some(InjectedFault::Io) => Err(ReplicaError::Io("injected EIO".into())),
            // A cancellable stall: a timeout drops this future and leaves no
            // backend I/O behind to settle.
            Some(InjectedFault::Slow(delay)) => {
                tokio::time::sleep(delay).await;
                Ok(())
            }
        }
    }

    /// The expected device is present. An absent device is lost; it is never
    /// treated as an empty replica to recreate.
    pub(crate) async fn present(&self) -> Result<(), SurvivalFailure> {
        match self.backend.probe().await {
            Ok(Presence::Present) => Ok(()),
            Ok(Presence::Absent) => Err(SurvivalFailure::Lost),
            Err(error) => Err(classify(&error)),
        }
    }

    /// Take the exclusive writer fence, initializing the region on first
    /// activation only.
    pub(crate) async fn lock_exclusive(
        &self,
        create: bool,
    ) -> Result<Box<dyn Fence>, SurvivalFailure> {
        self.present().await?;
        let initialized = self.backend.initialized().await.map_err(|e| classify(&e))?;
        if !initialized {
            if !create {
                return Err(SurvivalFailure::Stale);
            }
            self.backend.initialize().await.map_err(|e| classify(&e))?;
        }
        self.backend
            .fence_exclusive()
            .await
            .map_err(|e| classify(&e))
    }

    pub(crate) async fn read(&self, relative: &Path) -> Result<Option<Bytes>, SurvivalFailure> {
        self.backend
            .get(&key(relative))
            .await
            .map_err(|e| classify(&e))
    }

    pub(crate) async fn len(&self, relative: &Path) -> Result<Option<u64>, SurvivalFailure> {
        self.backend
            .len(&key(relative))
            .await
            .map_err(|e| classify(&e))
    }

    pub(crate) async fn exists(&self, relative: &Path) -> bool {
        matches!(self.len(relative).await, Ok(Some(_)))
    }

    pub(crate) async fn list(&self, relative: &Path) -> Result<Vec<String>, SurvivalFailure> {
        self.backend
            .list(&key(relative))
            .await
            .map_err(|e| classify(&e))
    }

    /// Durably write bytes without making them visible.
    pub(crate) async fn stage(
        &self,
        relative: &Path,
        bytes: Arc<Vec<u8>>,
    ) -> Result<Staged, SurvivalFailure> {
        self.before_io().await.map_err(|e| classify(&e))?;
        self.backend
            .stage(&key(relative), Bytes::from(bytes.as_ref().clone()))
            .await
            .map_err(|e| classify(&e))
    }

    /// Atomically make staged bytes visible.
    pub(crate) async fn publish(&self, staged: Staged) -> Result<(), SurvivalFailure> {
        self.before_io().await.map_err(|e| classify(&e))?;
        self.backend.publish(staged).await.map_err(|e| classify(&e))
    }

    /// Durable barrier for earlier publications and deletions.
    pub(crate) async fn flush(&self, relative: &Path) -> Result<(), SurvivalFailure> {
        self.before_io().await.map_err(|e| classify(&e))?;
        self.backend
            .flush(&key(relative))
            .await
            .map_err(|e| classify(&e))
    }

    /// Bytes this replica's region holds.
    pub(crate) async fn usage(&self) -> Result<u64, SurvivalFailure> {
        self.backend.usage().await.map_err(|e| classify(&e))
    }

    pub(crate) async fn remove(&self, relative: &Path) -> Result<(), SurvivalFailure> {
        self.before_io().await.map_err(|e| classify(&e))?;
        self.backend
            .delete(&key(relative))
            .await
            .map_err(|e| classify(&e))
    }
}

// ---------------------------------------------------------------------------
// The existing local backend.
// ---------------------------------------------------------------------------

/// The existing local backend: a survival region stored as files under
/// `<dir>/.zerofs-survival/v1` on a mounted local filesystem.
///
/// This is the reference adapter for the acceptance contracts. Its
/// guarantees come from POSIX semantics, which are this adapter's private
/// implementation of the [`ReplicaBackend`] contract:
///
/// - stage: write a temporary file, then fsync it;
/// - publish: rename(2) over the key;
/// - flush: fsync the key's directory and every new ancestor;
/// - fence: flock(2) on `writer.lock`;
/// - presence: the configured directory exists.
///
/// It has no reserved region: fullness surfaces as ENOSPC from the
/// filesystem. Raw pool members use [`super::member_region::MemberRegion`]
/// instead.
#[derive(Debug)]
pub(crate) struct LocalDirectoryBackend {
    dir: PathBuf,
}

#[derive(Debug)]
struct LocalFence(File);

impl Fence for LocalFence {}

impl Drop for LocalFence {
    fn drop(&mut self) {
        let _ = fs4::fs_std::FileExt::unlock(&self.0);
    }
}

fn io(error: std::io::Error) -> ReplicaError {
    if error.raw_os_error() == Some(libc::ENOSPC)
        || error.raw_os_error() == Some(libc::EDQUOT)
        || error.kind() == std::io::ErrorKind::StorageFull
    {
        ReplicaError::Full
    } else {
        ReplicaError::Io(error.to_string())
    }
}

async fn blocking<T: Send + 'static>(
    work: impl FnOnce() -> std::io::Result<T> + Send + 'static,
) -> Result<T, ReplicaError> {
    tokio::task::spawn_blocking(work)
        .await
        .map_err(|e| ReplicaError::Io(e.to_string()))?
        .map_err(io)
}

impl LocalDirectoryBackend {
    /// A replica rooted at a configured directory.
    pub(crate) fn new(dir: PathBuf) -> Self {
        Self { dir }
    }

    /// A replica named by its survival root (`<dir>/.zerofs-survival/v1`),
    /// as offline recovery receives it.
    pub(crate) fn from_root(root: &Path) -> Self {
        let dir = root
            .parent()
            .and_then(Path::parent)
            .map(Path::to_owned)
            .unwrap_or_else(|| root.to_owned());
        Self::new(dir)
    }

    fn root(&self) -> PathBuf {
        self.dir.join(ROOT)
    }

    fn path(&self, key: &str) -> Result<PathBuf, ReplicaError> {
        let relative =
            super::format::safe_relative(key).map_err(|e| ReplicaError::Io(format!("{e:#}")))?;
        Ok(self.root().join(relative))
    }
}

pub(crate) fn sync_dir(dir: &Path) -> std::io::Result<()> {
    File::open(dir)?.sync_all()
}

#[async_trait::async_trait]
impl ReplicaBackend for LocalDirectoryBackend {
    fn describe(&self) -> String {
        self.dir.display().to_string()
    }

    async fn probe(&self) -> Result<Presence, ReplicaError> {
        let dir = self.dir.clone();
        Ok(if blocking(move || Ok(dir.is_dir())).await? {
            Presence::Present
        } else {
            Presence::Absent
        })
    }

    async fn initialized(&self) -> Result<bool, ReplicaError> {
        let lock = self.root().join("writer.lock");
        blocking(move || Ok(lock.is_file())).await
    }

    async fn initialize(&self) -> Result<(), ReplicaError> {
        let dir = self.dir.clone();
        let root = self.root();
        blocking(move || {
            if root.join("identity.json").exists() {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::AlreadyExists,
                    "survival region already holds an identity",
                ));
            }
            std::fs::create_dir_all(&root)?;
            let lock = root.join("writer.lock");
            if !lock.exists() {
                OpenOptions::new()
                    .write(true)
                    .create_new(true)
                    .open(&lock)?
                    .sync_all()?;
            }
            sync_dir(&root)?;
            sync_dir(root.parent().expect("versioned root has a parent"))?;
            sync_dir(&dir)
        })
        .await
    }

    async fn fence_exclusive(&self) -> Result<Box<dyn Fence>, ReplicaError> {
        let path = self.root().join("writer.lock");
        let file = blocking(move || {
            // Never unlink a possibly held lock file; open it stably.
            OpenOptions::new().read(true).write(true).open(&path)
        })
        .await?;
        match fs4::fs_std::FileExt::try_lock_exclusive(&file) {
            Ok(true) => Ok(Box::new(LocalFence(file))),
            Ok(false) => Err(ReplicaError::Locked),
            Err(error) => Err(io(error)),
        }
    }

    async fn fence_shared(&self) -> Result<Box<dyn Fence>, ReplicaError> {
        let path = self.root().join("writer.lock");
        let file = match blocking(move || OpenOptions::new().read(true).open(&path)).await {
            Ok(file) => file,
            Err(ReplicaError::Io(_)) => return Err(ReplicaError::Uninitialized),
            Err(other) => return Err(other),
        };
        match fs4::fs_std::FileExt::try_lock_shared(&file) {
            Ok(true) => Ok(Box::new(LocalFence(file))),
            Ok(false) => Err(ReplicaError::Locked),
            Err(error) => Err(io(error)),
        }
    }

    async fn get(&self, key: &str) -> Result<Option<Bytes>, ReplicaError> {
        let path = self.path(key)?;
        blocking(move || match std::fs::read(&path) {
            Ok(bytes) => Ok(Some(Bytes::from(bytes))),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(error),
        })
        .await
    }

    async fn len(&self, key: &str) -> Result<Option<u64>, ReplicaError> {
        let path = self.path(key)?;
        blocking(move || match std::fs::metadata(&path) {
            Ok(metadata) if metadata.is_file() => Ok(Some(metadata.len())),
            Ok(_) => Ok(None),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(error),
        })
        .await
    }

    async fn list(&self, prefix: &str) -> Result<Vec<String>, ReplicaError> {
        let dir = if prefix.is_empty() {
            self.root()
        } else {
            self.path(prefix)?
        };
        blocking(move || {
            let mut names = Vec::new();
            let entries = match std::fs::read_dir(&dir) {
                Ok(entries) => entries,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                    return Ok(names);
                }
                Err(error) => return Err(error),
            };
            for entry in entries {
                let entry = entry?;
                let name = entry.file_name().to_string_lossy().into_owned();
                // Staged temporaries and the fence file are adapter-private.
                if name.starts_with('.') || name == "writer.lock" || !entry.file_type()?.is_file() {
                    continue;
                }
                names.push(name);
            }
            names.sort();
            Ok(names)
        })
        .await
    }

    async fn stage(&self, key: &str, bytes: Bytes) -> Result<Staged, ReplicaError> {
        let target = self.path(key)?;
        let root = self.root();
        let key = key.to_owned();
        let token = blocking(move || {
            // Never recreate anything at or above the region root: a missing
            // root means the device is gone, not an empty region.
            if !root.is_dir() {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    "survival region root is missing",
                ));
            }
            let parent = target.parent().expect("replica objects have a parent");
            let below = parent.strip_prefix(&root).map_err(std::io::Error::other)?;
            let mut current = root.clone();
            for part in below.components() {
                current.push(part);
                match std::fs::create_dir(&current) {
                    Ok(()) => {
                        sync_dir(&current)?;
                        sync_dir(current.parent().expect("below the root"))?;
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
                    Err(error) => return Err(error),
                }
            }
            let name = target
                .file_name()
                .and_then(|name| name.to_str())
                .unwrap_or("object");
            let temp = parent.join(format!(".{name}.tmp-{}", uuid::Uuid::new_v4()));
            let mut file = OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&temp)?;
            if let Err(error) = file.write_all(&bytes).and_then(|()| file.sync_all()) {
                let _ = std::fs::remove_file(&temp);
                return Err(error);
            }
            Ok(temp)
        })
        .await
        .map_err(|error| match error {
            ReplicaError::Io(_) if !self.dir.is_dir() => ReplicaError::Absent,
            other => other,
        })?;
        Ok(Staged {
            key,
            token: token.to_string_lossy().into_owned(),
        })
    }

    async fn publish(&self, staged: Staged) -> Result<(), ReplicaError> {
        let target = self.path(&staged.key)?;
        let temp = PathBuf::from(staged.token);
        if temp.parent() != target.parent() {
            return Err(ReplicaError::Io("staged object belongs elsewhere".into()));
        }
        blocking(move || std::fs::rename(&temp, &target)).await
    }

    async fn flush(&self, key_hint: &str) -> Result<(), ReplicaError> {
        let parent = self
            .path(key_hint)?
            .parent()
            .expect("replica objects have a parent")
            .to_owned();
        blocking(move || sync_dir(&parent)).await
    }

    async fn delete(&self, key: &str) -> Result<(), ReplicaError> {
        let path = self.path(key)?;
        blocking(move || match std::fs::remove_file(&path) {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(error),
        })
        .await
    }

    async fn usage(&self) -> Result<u64, ReplicaError> {
        let root = self.root();
        blocking(move || {
            Ok(walkdir::WalkDir::new(&root)
                .into_iter()
                .filter_map(Result::ok)
                .filter_map(|entry| entry.metadata().ok())
                .filter(std::fs::Metadata::is_file)
                .map(|metadata| metadata.len())
                .sum())
        })
        .await
    }
}
