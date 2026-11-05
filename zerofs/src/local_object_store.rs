//! Durable conditional writes for the native local backend.
//!
//! `object_store::local::LocalFileSystem` supports atomic replacement and
//! create-only writes, but not `PutMode::Update`. This adapter supplies the
//! missing compare-and-swap by coordinating *all* mutations with OS file locks.
//! The lock files are stable: deleting them while a store is in use would split
//! the lock domain. Writers must use this adapter rather than modify the object
//! files directly.
//!
//! Locks live beside the objects, one per directory. This also coordinates
//! independent processes, overlapping volume prefixes, and symlink aliases of
//! the storage directory. The reserved `#0` suffix makes the lock file invisible
//! and unaddressable through `LocalFileSystem`, just like its staging files.
//!
//! A bounded native worker owns both the locks and the complete I/O operation.
//! Dropping the caller's future cannot release the locks while a write is still
//! running. The worker deliberately has no Tokio runtime context: the inner
//! local backend performs synchronous I/O there rather than spawning a second,
//! independently cancellable blocking task. This also avoids nested blocking
//! pool starvation. File publication and ETag generation remain the upstream
//! implementation, preserving the existing on-disk layout and read semantics.

use async_trait::async_trait;
use bytes::Bytes;
use fs4::fs_std::FileExt;
use futures::stream::BoxStream;
use futures::{FutureExt, StreamExt};
use object_store::local::LocalFileSystem;
use object_store::path::Path;
use object_store::{
    CopyOptions, Error, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta,
    ObjectStore, ObjectStoreExt, PutMode, PutMultipartOptions, PutOptions, PutPayload, PutResult,
    RenameOptions, Result, UploadPart,
};
use std::fmt::{self, Display, Formatter};
use std::fs::{File, OpenOptions};
use std::ops::Range;
use std::path::{Path as FsPath, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};
use tokio::sync::{Semaphore, oneshot};

const STORE: &str = "LocalConditionalStore";
const LOCK_FILE: &str = ".zerofs-local-mutations#0";
const MAX_WORKERS: usize = 32;

/// The same absolute object paths as `LocalFileSystem::new()`, with durable CAS.
#[derive(Clone, Debug)]
pub(crate) struct LocalConditionalStore {
    inner: LocalFileSystem,
}

impl LocalConditionalStore {
    pub(crate) fn new() -> Self {
        Self {
            inner: LocalFileSystem::new().with_fsync(true),
        }
    }
}

impl Display for LocalConditionalStore {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{STORE}({})", self.inner)
    }
}

fn generic(source: impl Into<Box<dyn std::error::Error + Send + Sync>>) -> Error {
    Error::Generic {
        store: STORE,
        source: source.into(),
    }
}

/// LocalFileSystem's directory durability guarantee is supported on Unix.
/// Other platforms retain the same file-sync semantics as the inner backend.
fn sync_directory(path: &FsPath) -> Result<()> {
    #[cfg(unix)]
    File::open(path)
        .and_then(|directory| directory.sync_all())
        .map_err(generic)?;
    #[cfg(not(unix))]
    let _ = path;
    Ok(())
}

fn sync_parent(path: &FsPath) -> Result<()> {
    let parent = path
        .parent()
        .ok_or_else(|| generic("local object has no parent directory"))?;
    sync_directory(parent)
}

/// Lock canonical directories in a stable order, taking a shared parent once.
/// The lock file is never removed, including after an object is deleted.
fn lock_parents(paths: &[PathBuf]) -> Result<Vec<File>> {
    let mut parents = Vec::with_capacity(paths.len());
    for path in paths {
        let parent = path
            .parent()
            .ok_or_else(|| generic("local object has no parent directory"))?;
        std::fs::create_dir_all(parent).map_err(generic)?;
        let parent = std::fs::canonicalize(parent).map_err(generic)?;
        // Sync the ancestry too. Another process can have created a directory
        // without yet syncing its parent; existence alone is not durability.
        for directory in parent.ancestors() {
            sync_directory(directory)?;
        }
        parents.push(parent);
    }
    parents.sort();
    parents.dedup();

    let mut locks = Vec::with_capacity(parents.len());
    for parent in parents {
        let lock = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(parent.join(LOCK_FILE))
            .map_err(generic)?;
        FileExt::lock_exclusive(&lock).map_err(generic)?;
        lock.sync_all().map_err(generic)?;
        sync_directory(&parent)?;
        locks.push(lock);
    }
    Ok(locks)
}

/// The detached worker owns the admission permit, operation, and any lock guards
/// until I/O actually finishes, even when its result receiver is dropped.
async fn run_local<T, F>(operation: F) -> Result<T>
where
    T: Send + 'static,
    F: FnOnce() -> Result<T> + Send + 'static,
{
    static WORKERS: OnceLock<Arc<Semaphore>> = OnceLock::new();
    let permit = WORKERS
        .get_or_init(|| Arc::new(Semaphore::new(MAX_WORKERS)))
        .clone()
        .acquire_owned()
        .await
        .map_err(generic)?;
    let (sender, receiver) = oneshot::channel();
    let _worker = std::thread::Builder::new()
        .name("zerofs-local-store".into())
        .spawn(move || {
            let _permit = permit;
            let _ = sender.send(operation());
        })
        .map_err(generic)?;
    receiver
        .await
        .map_err(|_| generic("local storage worker stopped before returning a result"))?
}

#[async_trait]
impl ObjectStore for LocalConditionalStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        mut opts: PutOptions,
    ) -> Result<PutResult> {
        let path = self.inner.path_to_filesystem(location)?;
        let location = location.clone();
        let inner = self.inner.clone();
        run_local(move || {
            let _locks = lock_parents(&[path])?;
            if let PutMode::Update(expected) = &opts.mode {
                let actual = match futures::executor::block_on(inner.head(&location)) {
                    Ok(meta) => meta,
                    Err(Error::NotFound { .. }) => {
                        return Err(Error::Precondition {
                            path: location.to_string(),
                            source: "conditional update target does not exist".into(),
                        });
                    }
                    Err(error) => return Err(error),
                };
                if expected.e_tag.is_none()
                    || expected.e_tag != actual.e_tag
                    || expected.version != actual.version
                {
                    return Err(Error::Precondition {
                        path: location.to_string(),
                        source: "conditional update version does not match".into(),
                    });
                }
                opts.mode = PutMode::Overwrite;
            }
            futures::executor::block_on(inner.put_opts(&location, payload, opts))
        })
        .await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>> {
        let path = self.inner.path_to_filesystem(location)?;
        let location = location.clone();
        let inner = self.inner.clone();
        let upload = run_local(move || {
            futures::executor::block_on(inner.put_multipart_opts(&location, opts))
        })
        .await?;
        Ok(Box::new(LocalMultipartUpload {
            upload: Some(upload),
            path,
            parts: Arc::new(Parts::default()),
        }))
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        self.inner.get_opts(location, options).await
    }

    async fn get_ranges(&self, location: &Path, ranges: &[Range<u64>]) -> Result<Vec<Bytes>> {
        self.inner.get_ranges(location, ranges).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, Result<Path>>,
    ) -> BoxStream<'static, Result<Path>> {
        let inner = self.inner.clone();
        locations
            .map(move |location| {
                let inner = inner.clone();
                async move {
                    let location = location?;
                    let path = inner.path_to_filesystem(&location)?;
                    run_local(move || {
                        let _locks = lock_parents(std::slice::from_ref(&path))?;
                        futures::executor::block_on(inner.delete(&location))?;
                        // Upstream's with_fsync(true) does not cover standalone
                        // deletes. Keep deletions durable before releasing the lock.
                        sync_parent(&path)?;
                        Ok(location)
                    })
                    .await
                }
            })
            .buffered(10)
            .boxed()
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        self.inner.list(prefix)
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, Result<ObjectMeta>> {
        self.inner.list_with_offset(prefix, offset)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> Result<()> {
        let paths = vec![
            self.inner.path_to_filesystem(from)?,
            self.inner.path_to_filesystem(to)?,
        ];
        let (from, to) = (from.clone(), to.clone());
        let inner = self.inner.clone();
        run_local(move || {
            let _locks = lock_parents(&paths)?;
            futures::executor::block_on(inner.copy_opts(&from, &to, options))
        })
        .await
    }

    async fn rename_opts(&self, from: &Path, to: &Path, options: RenameOptions) -> Result<()> {
        let paths = vec![
            self.inner.path_to_filesystem(from)?,
            self.inner.path_to_filesystem(to)?,
        ];
        let (from, to) = (from.clone(), to.clone());
        let inner = self.inner.clone();
        run_local(move || {
            let _locks = lock_parents(&paths)?;
            // Directories are namespace containers, not objects. In particular
            // a directory rename must not move another writer's lock files.
            // Follow final symlinks too, so a directory alias cannot be moved
            // over a path through which another process is writing objects.
            for path in &paths {
                match std::fs::metadata(path) {
                    Ok(metadata) if metadata.is_dir() => {
                        return Err(generic("cannot rename a directory as a local object"));
                    }
                    Ok(_) => {}
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                    Err(error) => return Err(generic(error)),
                }
            }
            futures::executor::block_on(inner.rename_opts(&from, &to, options))
        })
        .await
    }
}

#[derive(Debug, Default)]
struct Parts {
    pending: AtomicUsize,
    failed: AtomicBool,
}

struct PartGuard {
    parts: Arc<Parts>,
    succeeded: bool,
}

impl Drop for PartGuard {
    fn drop(&mut self) {
        if !self.succeeded {
            self.parts.failed.store(true, Ordering::Release);
        }
        self.parts.pending.fetch_sub(1, Ordering::AcqRel);
    }
}

#[derive(Debug)]
struct LocalMultipartUpload {
    upload: Option<Box<dyn MultipartUpload>>,
    path: PathBuf,
    parts: Arc<Parts>,
}

#[async_trait]
impl MultipartUpload for LocalMultipartUpload {
    fn put_part(&mut self, payload: PutPayload) -> UploadPart {
        let Some(upload) = self.upload.as_mut() else {
            return futures::future::ready(Err(generic("multipart upload already finished")))
                .boxed();
        };
        self.parts.pending.fetch_add(1, Ordering::AcqRel);
        let mut guard = PartGuard {
            parts: Arc::clone(&self.parts),
            succeeded: false,
        };
        // Reserve this part's offset in call order; only the returned future
        // performs I/O, on our owned worker without a Tokio runtime context.
        let part = upload.put_part(payload);
        async move {
            run_local(move || {
                let result = futures::executor::block_on(part);
                guard.succeeded = result.is_ok();
                drop(guard);
                result
            })
            .await
        }
        .boxed()
    }

    async fn complete(&mut self) -> Result<PutResult> {
        if self.parts.pending.load(Ordering::Acquire) != 0
            || self.parts.failed.load(Ordering::Acquire)
        {
            return Err(generic(
                "multipart completion requires every part to finish successfully",
            ));
        }
        let mut upload = self
            .upload
            .take()
            .ok_or_else(|| generic("multipart upload already finished"))?;
        let path = self.path.clone();
        run_local(move || {
            let _locks = lock_parents(&[path])?;
            futures::executor::block_on(upload.complete())
        })
        .await
    }

    async fn abort(&mut self) -> Result<()> {
        let mut upload = self
            .upload
            .take()
            .ok_or_else(|| generic("multipart upload already finished"))?;
        run_local(move || futures::executor::block_on(upload.abort())).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::TryStreamExt;
    use object_store::{CopyMode, RenameTargetMode, UpdateVersion};
    use std::time::{Duration, Instant};
    use tempfile::TempDir;

    fn fixture() -> (TempDir, LocalConditionalStore, Path) {
        let directory = TempDir::new().unwrap();
        let prefix = Path::from_filesystem_path(directory.path()).unwrap();
        (directory, LocalConditionalStore::new(), prefix)
    }

    async fn bytes(store: &LocalConditionalStore, path: &Path) -> Bytes {
        store.get(path).await.unwrap().bytes().await.unwrap()
    }

    #[tokio::test]
    async fn conditional_writes_preserve_layout_and_versions() {
        let (directory, store, prefix) = fixture();
        let path = prefix.clone().join("object");
        let first = store
            .put_opts(&path, "first".into(), PutMode::Create.into())
            .await
            .unwrap();
        assert_eq!(
            std::fs::read(directory.path().join("object")).unwrap(),
            b"first"
        );
        assert_eq!(store.head(&path).await.unwrap().e_tag, first.e_tag);
        assert!(matches!(
            store
                .put_opts(&path, "duplicate".into(), PutMode::Create.into())
                .await,
            Err(Error::AlreadyExists { .. })
        ));
        let updated = store
            .put_opts(
                &path,
                "newer".into(),
                PutMode::Update(first.clone().into()).into(),
            )
            .await
            .unwrap();
        assert_ne!(updated.e_tag, first.e_tag);
        assert_eq!(store.head(&path).await.unwrap().e_tag, updated.e_tag);
        assert!(matches!(
            store
                .put_opts(&path, "stale".into(), PutMode::Update(first.into()).into())
                .await,
            Err(Error::Precondition { .. })
        ));
        assert!(matches!(
            store
                .put_opts(
                    &prefix.clone().join("absent"),
                    "new".into(),
                    PutMode::Update(updated.into()).into(),
                )
                .await,
            Err(Error::Precondition { .. })
        ));
        assert!(matches!(
            store
                .put_opts(
                    &path,
                    "no-version".into(),
                    PutMode::Update(UpdateVersion {
                        e_tag: None,
                        version: None,
                    })
                    .into(),
                )
                .await,
            Err(Error::Precondition { .. })
        ));
        assert_eq!(bytes(&store, &path).await, "newer");
        let listed = store
            .list(Some(&prefix))
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].location, path);
        let listing = store.list_with_delimiter(Some(&prefix)).await.unwrap();
        assert_eq!(listing.objects.len(), 1);
        assert!(listing.common_prefixes.is_empty());
    }

    #[tokio::test]
    async fn independent_instances_compete_for_one_version() {
        let (_directory, store, prefix) = fixture();
        let path = prefix.join("manifest");
        let initial = store.put(&path, "initial".into()).await.unwrap();
        let barrier = Arc::new(tokio::sync::Barrier::new(12));
        let mut tasks = Vec::new();
        for index in 0..12 {
            let independent_store = LocalConditionalStore::new();
            let (barrier, path, expected) = (barrier.clone(), path.clone(), initial.clone());
            tasks.push(tokio::spawn(async move {
                barrier.wait().await;
                independent_store
                    .put_opts(
                        &path,
                        format!("writer-{index}").into(),
                        PutMode::Update(expected.into()).into(),
                    )
                    .await
            }));
        }
        let mut wins = 0;
        for result in futures::future::join_all(tasks).await {
            match result.unwrap() {
                Ok(_) => wins += 1,
                Err(Error::Precondition { .. }) => {}
                other => panic!("unexpected CAS result: {other:?}"),
            }
        }
        assert_eq!(wins, 1);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn directory_aliases_share_the_same_cas_lock() {
        let (directory, store, prefix) = fixture();
        let aliases = TempDir::new().unwrap();
        let alias_directory = aliases.path().join("storage");
        std::os::unix::fs::symlink(directory.path(), &alias_directory).unwrap();
        // from_absolute_path deliberately preserves the symlink spelling.
        let alias = Path::from_absolute_path(alias_directory.join("object")).unwrap();
        let path = prefix.join("object");
        assert_ne!(alias, path);
        let initial = store.put(&path, "initial".into()).await.unwrap();
        let alias_store = LocalConditionalStore::new();
        let (a, b) = tokio::join!(
            store.put_opts(
                &path,
                "canonical".into(),
                PutMode::Update(initial.clone().into()).into(),
            ),
            alias_store.put_opts(
                &alias,
                "alias".into(),
                PutMode::Update(initial.into()).into(),
            ),
        );
        assert!(matches!(
            (a, b),
            (Ok(_), Err(Error::Precondition { .. })) | (Err(Error::Precondition { .. }), Ok(_))
        ));
        assert_eq!(bytes(&store, &path).await, bytes(&store, &alias).await);
    }

    #[tokio::test]
    async fn multipart_completion_and_cas_share_the_same_lock() {
        let (_directory, store, prefix) = fixture();
        for round in 0..12 {
            let path = prefix.clone().join(format!("object-{round}"));
            let initial = store.put(&path, "initial".into()).await.unwrap();
            let mut upload = store.put_multipart(&path).await.unwrap();
            upload.put_part("multipart".into()).await.unwrap();
            let barrier = Arc::new(tokio::sync::Barrier::new(2));
            let complete_barrier = barrier.clone();
            let complete = tokio::spawn(async move {
                complete_barrier.wait().await;
                upload.complete().await
            });
            let writer_path = path.clone();
            let update = tokio::spawn(async move {
                barrier.wait().await;
                LocalConditionalStore::new()
                    .put_opts(
                        &writer_path,
                        "conditional".into(),
                        PutMode::Update(initial.into()).into(),
                    )
                    .await
            });
            complete.await.unwrap().unwrap();
            assert!(matches!(
                update.await.unwrap(),
                Ok(_) | Err(Error::Precondition { .. })
            ));
            // A successful CAS must precede multipart publication. It must
            // never pass a stale HEAD and overwrite the completed upload.
            assert_eq!(bytes(&store, &path).await, "multipart");
        }
    }

    #[tokio::test]
    async fn unfinished_or_cancelled_parts_cannot_be_published() {
        let (_directory, store, prefix) = fixture();
        let path = prefix.join("object");
        store.put(&path, "original".into()).await.unwrap();
        let mut upload = store.put_multipart(&path).await.unwrap();
        let part = upload.put_part("never-written".into());
        assert!(upload.complete().await.is_err());
        drop(part);
        assert!(upload.complete().await.is_err());
        assert_eq!(bytes(&store, &path).await, "original");
        upload.abort().await.unwrap();
    }

    #[tokio::test]
    async fn delete_copy_and_rename_keep_versions_and_lock_files_consistent() {
        let (directory, store, prefix) = fixture();
        let source = prefix.clone().join("a").join("source");
        let copy = prefix.clone().join("b").join("copy");
        let target = prefix.clone().join("a").join("target");
        store.put(&source, "payload".into()).await.unwrap();
        store
            .copy_opts(
                &source,
                &copy,
                CopyOptions::new().with_mode(CopyMode::Create),
            )
            .await
            .unwrap();
        assert!(matches!(
            store
                .copy_opts(
                    &source,
                    &copy,
                    CopyOptions::new().with_mode(CopyMode::Create)
                )
                .await,
            Err(Error::AlreadyExists { .. })
        ));
        store
            .rename_opts(
                &copy,
                &target,
                RenameOptions::new().with_target_mode(RenameTargetMode::Create),
            )
            .await
            .unwrap();
        assert!(matches!(
            store.head(&copy).await,
            Err(Error::NotFound { .. })
        ));
        let previous = store.head(&target).await.unwrap();
        store.delete(&target).await.unwrap();
        assert!(matches!(
            LocalConditionalStore::new().head(&target).await,
            Err(Error::NotFound { .. })
        ));
        assert!(directory.path().join("a").join(LOCK_FILE).is_file());
        assert!(directory.path().join("b").join(LOCK_FILE).is_file());
        assert!(matches!(
            store
                .put_opts(
                    &target,
                    "stale".into(),
                    PutMode::Update(UpdateVersion {
                        e_tag: previous.e_tag,
                        version: previous.version,
                    })
                    .into(),
                )
                .await,
            Err(Error::Precondition { .. })
        ));
        let listed = store
            .list(Some(&prefix))
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].location, source);
    }

    #[tokio::test]
    async fn directory_rename_cannot_move_the_lock_namespace() {
        let (directory, store, prefix) = fixture();
        let object = prefix.clone().join("a").join("object");
        store.put(&object, "payload".into()).await.unwrap();
        assert!(
            store
                .rename_opts(
                    &prefix.clone().join("a"),
                    &prefix.join("b"),
                    RenameOptions::new(),
                )
                .await
                .is_err()
        );
        assert_eq!(bytes(&store, &object).await, "payload");
        assert!(directory.path().join("a").join(LOCK_FILE).is_file());
        assert!(!directory.path().join("b").exists());
    }

    #[tokio::test]
    async fn cancelled_caller_does_not_release_a_running_workers_lock() {
        let (directory, store, prefix) = fixture();
        let path = prefix.join("object");
        let initial = store.put(&path, "initial".into()).await.unwrap();
        let filesystem_path = store.inner.path_to_filesystem(&path).unwrap();
        let (entered_sender, entered_receiver) = oneshot::channel();
        let (release_sender, release_receiver) = std::sync::mpsc::channel();
        let inner = store.inner.clone();
        let worker_path = path.clone();
        let task = tokio::spawn(run_local(move || {
            let _locks = lock_parents(&[filesystem_path])?;
            entered_sender.send(()).unwrap();
            release_receiver.recv().unwrap();
            futures::executor::block_on(inner.put(&worker_path, "completed".into()))
        }));
        entered_receiver.await.unwrap();
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        let probe = OpenOptions::new()
            .read(true)
            .write(true)
            .open(directory.path().join(LOCK_FILE))
            .unwrap();
        assert!(!FileExt::try_lock_exclusive(&probe).unwrap());
        release_sender.send(()).unwrap();
        // This cannot enter until the cancelled caller's worker has published
        // and synced its write, at which point the old version must fail.
        assert!(matches!(
            store
                .put_opts(
                    &path,
                    "stale".into(),
                    PutMode::Update(initial.into()).into(),
                )
                .await,
            Err(Error::Precondition { .. })
        ));
        assert_eq!(bytes(&store, &path).await, "completed");
    }

    /// Invoked in child test processes by the cross-process CAS test below.
    #[test]
    fn subprocess_cas_writer() {
        let Ok(root) = std::env::var("ZEROFS_LOCAL_CAS_TEST_ROOT") else {
            return;
        };
        let name = std::env::var("ZEROFS_LOCAL_CAS_TEST_WRITER").unwrap();
        let expected = std::env::var("ZEROFS_LOCAL_CAS_TEST_ETAG").unwrap();
        let root = PathBuf::from(root);
        std::fs::write(root.join(format!("{name}.ready")), "ready").unwrap();
        let deadline = Instant::now() + Duration::from_secs(15);
        while !root.join("go").exists() {
            assert!(
                Instant::now() < deadline,
                "parent did not release CAS writers"
            );
            std::thread::sleep(Duration::from_millis(5));
        }
        let location = Path::from_filesystem_path(&root).unwrap().join("object");
        let result = futures::executor::block_on(
            LocalConditionalStore::new().put_opts(
                &location,
                name.clone().into(),
                PutMode::Update(UpdateVersion {
                    e_tag: Some(expected),
                    version: None,
                })
                .into(),
            ),
        );
        let outcome = match result {
            Ok(_) => "won",
            Err(Error::Precondition { .. }) => "stale",
            other => panic!("unexpected cross-process CAS result: {other:?}"),
        };
        std::fs::write(root.join(format!("{name}.result")), outcome).unwrap();
    }

    #[tokio::test]
    async fn independent_processes_compete_for_one_version() {
        let (directory, store, prefix) = fixture();
        let path = prefix.join("object");
        let initial = store.put(&path, "initial".into()).await.unwrap();
        let executable = std::env::current_exe().unwrap();
        let mut children = Vec::new();
        for name in ["left", "right"] {
            children.push(
                tokio::process::Command::new(&executable)
                    .arg("--exact")
                    .arg("local_object_store::tests::subprocess_cas_writer")
                    .arg("--nocapture")
                    .env("ZEROFS_LOCAL_CAS_TEST_ROOT", directory.path())
                    .env("ZEROFS_LOCAL_CAS_TEST_WRITER", name)
                    .env(
                        "ZEROFS_LOCAL_CAS_TEST_ETAG",
                        initial.e_tag.as_ref().unwrap(),
                    )
                    .kill_on_drop(true)
                    .stdout(std::process::Stdio::piped())
                    .stderr(std::process::Stdio::piped())
                    .spawn()
                    .unwrap(),
            );
        }
        tokio::time::timeout(Duration::from_secs(10), async {
            while !(directory.path().join("left.ready").exists()
                && directory.path().join("right.ready").exists())
            {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("child CAS writers did not start");
        std::fs::write(directory.path().join("go"), "go").unwrap();
        for child in children {
            let output = tokio::time::timeout(Duration::from_secs(20), child.wait_with_output())
                .await
                .expect("child CAS writer timed out")
                .unwrap();
            assert!(
                output.status.success(),
                "child failed: {} {}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr),
            );
        }
        let mut outcomes = ["left", "right"].map(|name| {
            std::fs::read_to_string(directory.path().join(format!("{name}.result"))).unwrap()
        });
        outcomes.sort();
        assert_eq!(outcomes, ["stale", "won"]);
        let actual = bytes(&store, &path).await;
        assert!(actual == "left" || actual == "right");
    }
}
