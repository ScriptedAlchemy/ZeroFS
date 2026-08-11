use crate::writeback::admission::{Admission, DiskAdmission};
use crate::writeback::config::{AckMode, WritebackSettings};
use crate::writeback::journal::Journal;
use crate::writeback::journaler::{LocalBarrierError, LocalJournaler};
use crate::writeback::model::{FenceClass, LocalEtag, MutationKind, MutationMode, MutationRecord};
use crate::writeback::overlay::{OverlayCommitObserver, OverlayIndex, VisibleVersion};
use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use futures::{StreamExt, TryStreamExt, stream};
use object_store::path::Path;
use object_store::{
    CopyMode, CopyOptions, Extensions, GetOptions, GetResult, ListResult, MultipartUpload,
    ObjectMeta, ObjectStore, PutMode, PutMultipartOptions, PutOptions, PutPayload, PutResult,
    RenameOptions, RenameTargetMode, UpdateVersion,
};
use sha2::{Digest, Sha256};
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::{Mutex, OwnedMutexGuard};
use uuid::Uuid;

const KEY_LOCK_SHARDS: usize = 256;

#[derive(Clone)]
pub struct WritebackObjectStore {
    inner: Arc<WritebackStoreInner>,
}

struct WritebackStoreInner {
    journal: Arc<Journal>,
    settings: WritebackSettings,
    overlay: OverlayIndex,
    admission: Admission,
    disk: DiskAdmission,
    journaler: LocalJournaler,
    incarnation: Uuid,
    next_sequence: AtomicU64,
    key_locks: Vec<Arc<Mutex<()>>>,
    admission_order: Mutex<()>,
}

impl std::fmt::Debug for WritebackObjectStore {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("WritebackObjectStore")
            .field("journal", &self.inner.journal.root())
            .field("ack_mode", &self.inner.settings.ack_mode)
            .finish_non_exhaustive()
    }
}

impl std::fmt::Display for WritebackObjectStore {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "WritebackObjectStore({})",
            self.inner.overlay_remote()
        )
    }
}

impl WritebackStoreInner {
    fn overlay_remote(&self) -> &'static str {
        "remote"
    }
}

impl WritebackObjectStore {
    pub async fn open(
        remote: Arc<dyn ObjectStore>,
        journal: Arc<Journal>,
        settings: WritebackSettings,
    ) -> anyhow::Result<Self> {
        if settings.ack_mode == AckMode::Remote {
            anyhow::bail!("remote acknowledgement requires the remote scheduler");
        }
        if settings.memory_bytes == 0 {
            anyhow::bail!("writeback requires a positive independent dirty RAM budget");
        }
        let snapshot = journal.snapshot()?;
        let available = fs4::available_space(&settings.dir)?;
        let admission = Admission::new(settings.memory_bytes);
        let disk = DiskAdmission::with_used(
            settings.disk_bytes,
            settings.high_watermark_percent,
            settings.resume_percent,
            settings.min_free_bytes,
            snapshot.dirty_blob_bytes,
            available,
        )?;
        let overlay = OverlayIndex::recover(remote, journal.clone()).await?;
        let observer = Arc::new(OverlayCommitObserver::new(overlay.clone(), journal.clone()));
        let queue_depth = settings.upload_concurrency.saturating_mul(4).max(16);
        let journaler = LocalJournaler::start_with_observer(
            journal.clone(),
            admission.clone(),
            queue_depth,
            Some(observer),
        )?;
        Ok(Self {
            inner: Arc::new(WritebackStoreInner {
                journal,
                settings,
                overlay,
                admission,
                disk,
                journaler,
                incarnation: snapshot.incarnation,
                next_sequence: AtomicU64::new(snapshot.local_seq),
                key_locks: (0..KEY_LOCK_SHARDS)
                    .map(|_| Arc::new(Mutex::new(())))
                    .collect(),
                admission_order: Mutex::new(()),
            }),
        })
    }

    pub async fn wait_local(&self, sequence: u64) -> Result<(), LocalBarrierError> {
        self.inner.journaler.barrier().wait_local(sequence).await
    }

    pub fn dirty_ram_bytes(&self) -> u64 {
        self.inner.admission.used_bytes()
    }

    pub fn dirty_ssd_bytes(&self) -> u64 {
        self.inner.disk.used_bytes()
    }

    pub async fn shutdown(&self) -> Result<(), LocalBarrierError> {
        self.inner.disk.close();
        self.inner.journaler.shutdown().await
    }

    fn key_lock(&self, path: &Path) -> Arc<Mutex<()>> {
        self.inner.key_locks[self.key_lock_index(path)].clone()
    }

    fn key_lock_index(&self, path: &Path) -> usize {
        let mut hasher = DefaultHasher::new();
        path.hash(&mut hasher);
        hasher.finish() as usize % self.inner.key_locks.len()
    }

    async fn lock_pair(&self, first: &Path, second: &Path) -> Vec<OwnedMutexGuard<()>> {
        let first_index = self.key_lock_index(first);
        let second_index = self.key_lock_index(second);
        if first_index == second_index {
            return vec![self.inner.key_locks[first_index].clone().lock_owned().await];
        }
        let (low, high) = if first_index < second_index {
            (first_index, second_index)
        } else {
            (second_index, first_index)
        };
        vec![
            self.inner.key_locks[low].clone().lock_owned().await,
            self.inner.key_locks[high].clone().lock_owned().await,
        ]
    }

    fn allocate_sequence(&self) -> object_store::Result<u64> {
        self.inner
            .next_sequence
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                current.checked_add(1)
            })
            .map(|previous| previous + 1)
            .map_err(|_| generic_error("writeback sequence overflow"))
    }

    async fn owned_put(
        self,
        location: Path,
        bytes: Bytes,
        options: PutOptions,
        ram: crate::writeback::admission::AcceptedAdmission,
        disk: crate::writeback::admission::DiskPermit,
    ) -> object_store::Result<PutResult> {
        let lock = self.key_lock(&location);
        let key_guard = lock.lock_owned().await;
        let order_guard = self.inner.admission_order.lock().await;
        let visible = self.inner.overlay.visible_version(&location).await?;
        let (mode, expected_visible_version, predecessor, fence) =
            validate_put_mode(&location, &options.mode, visible)?;
        let sequence = self.allocate_sequence()?;
        let local_etag = LocalEtag::new(self.inner.incarnation, sequence);
        let record = MutationRecord {
            format_version: 1,
            sequence,
            operation_id: Uuid::new_v4(),
            path: location.to_string(),
            kind: MutationKind::Put {
                mode,
                expected_visible_version,
                payload_len: bytes.len() as u64,
                payload_sha256: Sha256::digest(&bytes).into(),
                blob_path: String::new(),
            },
            local_etag: local_etag.clone(),
            accepted_at_unix_ms: chrono::Utc::now().timestamp_millis().max(0) as u64,
            remote_predecessor_etag: predecessor,
            remote_result_etag: None,
            fence,
            retry_count: 0,
            last_error: None,
        };
        self.inner
            .overlay
            .install_memory(record.clone(), bytes.clone())
            .await
            .map_err(|error| generic_error(format!("overlay admission failed: {error:#}")))?;
        let barrier = match self
            .inner
            .journaler
            .submit_put_with_disk(record, bytes, ram, disk)
            .await
        {
            Ok(barrier) => barrier,
            Err(error) => {
                self.inner.overlay.remove_sequence(sequence).await;
                return Err(generic_error(format!(
                    "local journal admission failed: {error}"
                )));
            }
        };
        drop(order_guard);
        drop(key_guard);
        if self.inner.settings.ack_mode == AckMode::Ssd {
            barrier
                .wait_local(sequence)
                .await
                .map_err(|error| generic_error(format!("local durability failed: {error}")))?;
        }
        Ok(PutResult {
            e_tag: Some(local_etag.as_str().to_owned()),
            version: Some(local_etag.as_str().to_owned()),
            extensions: Extensions::new(),
        })
    }

    async fn owned_delete(self, location: Path) -> object_store::Result<Path> {
        let lock = self.key_lock(&location);
        let key_guard = lock.lock_owned().await;
        let order_guard = self.inner.admission_order.lock().await;
        let sequence = self.allocate_sequence()?;
        let record = MutationRecord {
            format_version: 1,
            sequence,
            operation_id: Uuid::new_v4(),
            path: location.to_string(),
            kind: MutationKind::Delete,
            local_etag: LocalEtag::new(self.inner.incarnation, sequence),
            accepted_at_unix_ms: chrono::Utc::now().timestamp_millis().max(0) as u64,
            remote_predecessor_etag: None,
            remote_result_etag: None,
            fence: FenceClass::Fence,
            retry_count: 0,
            last_error: None,
        };
        self.inner
            .overlay
            .install_delete(record.clone())
            .await
            .map_err(|error| {
                generic_error(format!("delete overlay admission failed: {error:#}"))
            })?;
        let barrier = match self.inner.journaler.submit_metadata(record).await {
            Ok(barrier) => barrier,
            Err(error) => {
                self.inner.overlay.remove_sequence(sequence).await;
                return Err(generic_error(format!(
                    "delete journal admission failed: {error}"
                )));
            }
        };
        drop(order_guard);
        drop(key_guard);
        if self.inner.settings.ack_mode == AckMode::Ssd {
            barrier
                .wait_local(sequence)
                .await
                .map_err(|error| generic_error(format!("local durability failed: {error}")))?;
        }
        Ok(location)
    }

    async fn owned_copy_or_rename(
        self,
        from: Path,
        to: Path,
        mode: MutationMode,
        rename: bool,
    ) -> object_store::Result<()> {
        let key_guards = self.lock_pair(&from, &to).await;
        let target_visible = self.inner.overlay.visible_version(&to).await?;
        if mode == MutationMode::Create && target_visible.is_some() {
            return Err(object_store::Error::AlreadyExists {
                path: to.to_string(),
                source: "overlay-visible copy target already exists".into(),
            });
        }
        let source_meta = self.inner.overlay.head(&from).await?;
        if from == to {
            return Ok(());
        }
        let bytes_len = source_meta.size;
        let ram = self
            .inner
            .admission
            .reserve(bytes_len)
            .await
            .map_err(|error| generic_error(format!("dirty RAM admission failed: {error}")))?
            .accept();
        let available = fs4::available_space(&self.inner.settings.dir)
            .map_err(|error| generic_error(format!("failed to inspect writeback SSD: {error}")))?;
        let disk = self
            .inner
            .disk
            .reserve(bytes_len, available)
            .await
            .map_err(|error| generic_error(format!("dirty SSD admission failed: {error}")))?;
        let bytes = self.inner.overlay.get(&from).await?.bytes().await?;
        if bytes.len() as u64 != bytes_len {
            return Err(generic_error(
                "copy source changed while being materialized",
            ));
        }

        let order_guard = self.inner.admission_order.lock().await;
        let sequence = self.allocate_sequence()?;
        let local_etag = LocalEtag::new(self.inner.incarnation, sequence);
        let payload_sha256 = Sha256::digest(&bytes).into();
        let kind = if rename {
            MutationKind::Rename {
                source: from.to_string(),
                mode,
                payload_len: bytes_len,
                payload_sha256,
                blob_path: String::new(),
            }
        } else {
            MutationKind::Copy {
                source: from.to_string(),
                mode,
                payload_len: bytes_len,
                payload_sha256,
                blob_path: String::new(),
            }
        };
        let record = MutationRecord {
            format_version: 1,
            sequence,
            operation_id: Uuid::new_v4(),
            path: to.to_string(),
            kind,
            local_etag,
            accepted_at_unix_ms: chrono::Utc::now().timestamp_millis().max(0) as u64,
            remote_predecessor_etag: None,
            remote_result_etag: None,
            fence: if mode == MutationMode::Create {
                FenceClass::ImmutableCreate
            } else {
                FenceClass::Fence
            },
            retry_count: 0,
            last_error: None,
        };
        let overlay_result = if rename {
            self.inner
                .overlay
                .install_rename(record.clone(), bytes.clone())
                .await
        } else {
            self.inner
                .overlay
                .install_copy(record.clone(), bytes.clone())
                .await
        };
        overlay_result.map_err(|error| {
            generic_error(format!("copy/rename overlay admission failed: {error:#}"))
        })?;
        let barrier = match self
            .inner
            .journaler
            .submit_put_with_disk(record, bytes, ram, disk)
            .await
        {
            Ok(barrier) => barrier,
            Err(error) => {
                self.inner.overlay.remove_sequence(sequence).await;
                return Err(generic_error(format!(
                    "copy/rename journal admission failed: {error}"
                )));
            }
        };
        drop(order_guard);
        drop(key_guards);
        if self.inner.settings.ack_mode == AckMode::Ssd {
            barrier
                .wait_local(sequence)
                .await
                .map_err(|error| generic_error(format!("local durability failed: {error}")))?;
        }
        Ok(())
    }
}

#[async_trait]
impl ObjectStore for WritebackObjectStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        options: PutOptions,
    ) -> object_store::Result<PutResult> {
        let bytes_len = u64::try_from(payload.content_length())
            .map_err(|_| generic_error("put payload length exceeds u64"))?;
        let ram = self
            .inner
            .admission
            .reserve(bytes_len)
            .await
            .map_err(|error| generic_error(format!("dirty RAM admission failed: {error}")))?
            .accept();
        let available = fs4::available_space(&self.inner.settings.dir)
            .map_err(|error| generic_error(format!("failed to inspect writeback SSD: {error}")))?;
        let disk = self
            .inner
            .disk
            .reserve(bytes_len, available)
            .await
            .map_err(|error| generic_error(format!("dirty SSD admission failed: {error}")))?;
        let bytes = Bytes::from(payload);
        let owned = self.clone();
        let location = location.clone();
        tokio::spawn(async move { owned.owned_put(location, bytes, options, ram, disk).await })
            .await
            .map_err(|error| generic_error(format!("owned put task failed: {error}")))?
    }

    async fn put_multipart_opts(
        &self,
        _location: &Path,
        _options: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        Err(object_store::Error::NotImplemented {
            operation: "put_multipart_opts".to_owned(),
            implementer: "WritebackObjectStore".to_owned(),
        })
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        self.inner.overlay.get_opts(location, options).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, object_store::Result<Path>>,
    ) -> BoxStream<'static, object_store::Result<Path>> {
        let store = self.clone();
        locations
            .then(move |location| {
                let store = store.clone();
                async move {
                    let location = location?;
                    let owned = store.clone();
                    tokio::spawn(async move { owned.owned_delete(location).await })
                        .await
                        .map_err(|error| {
                            generic_error(format!("owned delete task failed: {error}"))
                        })?
                }
            })
            .boxed()
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        let store = self.clone();
        let prefix = prefix.cloned();
        stream::once(async move { store.inner.overlay.list(prefix.as_ref()).await })
            .map(|result| match result {
                Ok(objects) => objects.into_iter().map(Ok).collect::<Vec<_>>(),
                Err(error) => vec![Err(error)],
            })
            .flat_map(stream::iter)
            .boxed()
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        let offset = offset.clone();
        self.list(prefix)
            .try_filter(move |meta| futures::future::ready(meta.location > offset))
            .boxed()
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        self.inner.overlay.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        options: CopyOptions,
    ) -> object_store::Result<()> {
        let mode = match options.mode {
            CopyMode::Overwrite => MutationMode::Overwrite,
            CopyMode::Create => MutationMode::Create,
        };
        let owned = self.clone();
        let from = from.clone();
        let to = to.clone();
        tokio::spawn(async move { owned.owned_copy_or_rename(from, to, mode, false).await })
            .await
            .map_err(|error| generic_error(format!("owned copy task failed: {error}")))?
    }

    async fn rename_opts(
        &self,
        from: &Path,
        to: &Path,
        options: RenameOptions,
    ) -> object_store::Result<()> {
        let mode = match options.target_mode {
            RenameTargetMode::Overwrite => MutationMode::Overwrite,
            RenameTargetMode::Create => MutationMode::Create,
        };
        let owned = self.clone();
        let from = from.clone();
        let to = to.clone();
        tokio::spawn(async move { owned.owned_copy_or_rename(from, to, mode, true).await })
            .await
            .map_err(|error| generic_error(format!("owned rename task failed: {error}")))?
    }
}

fn validate_put_mode(
    location: &Path,
    mode: &PutMode,
    visible: Option<VisibleVersion>,
) -> object_store::Result<(MutationMode, Option<String>, Option<String>, FenceClass)> {
    match mode {
        PutMode::Overwrite => Ok((MutationMode::Overwrite, None, None, FenceClass::Fence)),
        PutMode::Create => {
            if visible.is_some() {
                return Err(object_store::Error::AlreadyExists {
                    path: location.to_string(),
                    source: "overlay-visible object already exists".into(),
                });
            }
            Ok((
                MutationMode::Create,
                None,
                None,
                FenceClass::ImmutableCreate,
            ))
        }
        PutMode::Update(expected) => {
            let Some(visible) = visible else {
                return Err(precondition(location, "update target does not exist"));
            };
            if !version_matches(expected, &visible) {
                return Err(precondition(location, "update version is stale"));
            }
            let expected_string = expected.e_tag.clone().or_else(|| expected.version.clone());
            if expected_string.is_none() {
                return Err(precondition(location, "update requires an ETag or version"));
            }
            let predecessor = match visible {
                VisibleVersion::Local(_) => None,
                VisibleVersion::Remote { e_tag, .. } => e_tag,
            };
            Ok((
                MutationMode::Update,
                expected_string,
                predecessor,
                FenceClass::Fence,
            ))
        }
    }
}

fn version_matches(expected: &UpdateVersion, visible: &VisibleVersion) -> bool {
    let (visible_etag, visible_version) = match visible {
        VisibleVersion::Local(etag) => (Some(etag.as_str()), Some(etag.as_str())),
        VisibleVersion::Remote { e_tag, version } => (e_tag.as_deref(), version.as_deref()),
    };
    let checks = [
        expected
            .e_tag
            .as_deref()
            .map(|expected| Some(expected) == visible_etag),
        expected
            .version
            .as_deref()
            .map(|expected| Some(expected) == visible_version),
    ];
    checks.into_iter().flatten().all(|matches| matches)
        && (expected.e_tag.is_some() || expected.version.is_some())
}

fn precondition(path: &Path, message: &'static str) -> object_store::Error {
    object_store::Error::Precondition {
        path: path.to_string(),
        source: message.into(),
    }
}

fn generic_error(message: impl Into<String>) -> object_store::Error {
    object_store::Error::Generic {
        store: "ZeroFSWriteback",
        source: message.into().into(),
    }
}

#[cfg(test)]
mod tests {
    use super::WritebackObjectStore;
    use crate::writeback::config::{AckMode, ShutdownFlush, WritebackSettings};
    use crate::writeback::journal::Journal;
    use crate::writeback::model::JournalIdentity;
    use bytes::Bytes;
    use futures::{StreamExt, stream};
    use object_store::memory::InMemory;
    use object_store::path::Path;
    use object_store::{
        CopyMode, CopyOptions, ObjectStore, ObjectStoreExt, PutMode, PutOptions, RenameOptions,
        RenameTargetMode, UpdateVersion,
    };
    use std::sync::Arc;
    use std::time::Duration;

    async fn test_store() -> (WritebackObjectStore, Arc<InMemory>, tempfile::TempDir) {
        let temp = tempfile::tempdir().unwrap();
        let journal = Arc::new(
            Journal::open(
                temp.path().join("writeback"),
                JournalIdentity {
                    format_version: 1,
                    bucket_id: "bucket-a".to_owned(),
                    backend_endpoint: "memory://remote".to_owned(),
                    database_prefix: "zerofs/pilot".to_owned(),
                    backend_kind: "memory".to_owned(),
                    encryption_key_identity_sha256: [0x77; 32],
                },
            )
            .unwrap(),
        );
        let remote = Arc::new(InMemory::new());
        let settings = WritebackSettings {
            dir: temp.path().join("writeback"),
            ack_mode: AckMode::Memory,
            memory_bytes: 1_000_000,
            disk_bytes: 10_000_000,
            min_free_bytes: 1,
            high_watermark_percent: 95,
            resume_percent: 85,
            upload_concurrency: 4,
            shutdown_flush: ShutdownFlush::Local,
        };
        let store = WritebackObjectStore::open(remote.clone(), journal, settings)
            .await
            .unwrap();
        (store, remote, temp)
    }

    #[tokio::test]
    async fn object_store_put_acknowledges_memory_and_reads_from_overlay_before_remote() {
        let (store, remote, _temp) = test_store().await;
        let path = Path::from("segments/a");

        let result = store
            .put(&path, Bytes::from_static(b"payload").into())
            .await
            .unwrap();

        assert!(result.e_tag.as_deref().unwrap().starts_with("wb:"));
        assert_eq!(
            store.get(&path).await.unwrap().bytes().await.unwrap(),
            Bytes::from_static(b"payload")
        );
        assert!(remote.head(&path).await.is_err());
        store.wait_local(1).await.unwrap();
        assert_eq!(store.dirty_ram_bytes(), 0);
        assert_eq!(store.dirty_ssd_bytes(), 7);
        store.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn create_and_local_update_preconditions_use_the_overlay_version() {
        let (store, _remote, _temp) = test_store().await;
        let path = Path::from("manifest");
        let created = store
            .put_opts(
                &path,
                Bytes::from_static(b"one").into(),
                PutOptions::from(PutMode::Create),
            )
            .await
            .unwrap();
        assert!(
            store
                .put_opts(
                    &path,
                    Bytes::from_static(b"collision").into(),
                    PutOptions::from(PutMode::Create),
                )
                .await
                .is_err()
        );

        let updated = store
            .put_opts(
                &path,
                Bytes::from_static(b"two").into(),
                PutOptions::from(PutMode::Update(created.clone().into())),
            )
            .await
            .unwrap();
        assert_ne!(updated.e_tag, created.e_tag);
        assert!(
            store
                .put_opts(
                    &path,
                    Bytes::from_static(b"stale").into(),
                    PutOptions::from(PutMode::Update(created.into())),
                )
                .await
                .is_err()
        );
        assert_eq!(
            store.get(&path).await.unwrap().bytes().await.unwrap(),
            Bytes::from_static(b"two")
        );
        store.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn update_precondition_can_start_from_a_remote_version() {
        let (store, remote, _temp) = test_store().await;
        let path = Path::from("manifest");
        remote
            .put(&path, Bytes::from_static(b"remote").into())
            .await
            .unwrap();
        let remote_meta = remote.head(&path).await.unwrap();

        store
            .put_opts(
                &path,
                Bytes::from_static(b"local").into(),
                PutOptions::from(PutMode::Update(UpdateVersion {
                    e_tag: remote_meta.e_tag,
                    version: remote_meta.version,
                })),
            )
            .await
            .unwrap();

        assert_eq!(
            store.get(&path).await.unwrap().bytes().await.unwrap(),
            Bytes::from_static(b"local")
        );
        store.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn delete_stream_preserves_input_order_and_hides_each_path_immediately() {
        let (store, remote, _temp) = test_store().await;
        for path in ["a", "b", "c"] {
            remote
                .put(&Path::from(path), Bytes::from_static(b"remote").into())
                .await
                .unwrap();
        }
        let input = stream::iter(["b", "a", "c"].map(|path| Ok(Path::from(path)))).boxed();

        let deleted = store
            .delete_stream(input)
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<object_store::Result<Vec<_>>>()
            .unwrap();

        assert_eq!(
            deleted
                .into_iter()
                .map(|path| path.to_string())
                .collect::<Vec<_>>(),
            vec!["b", "a", "c"]
        );
        for path in ["a", "b", "c"] {
            assert!(store.get(&Path::from(path)).await.is_err());
        }
        store.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn concurrent_cross_key_puts_enqueue_as_one_contiguous_journal_prefix() {
        let (store, _remote, _temp) = test_store().await;
        let puts = (0..64).map(|index| {
            let store = store.clone();
            async move {
                store
                    .put(
                        &Path::from(format!("segments/{index:02}")),
                        Bytes::from(vec![index as u8; 1024]).into(),
                    )
                    .await
            }
        });

        for result in futures::future::join_all(puts).await {
            result.unwrap();
        }
        store.wait_local(64).await.unwrap();
        assert_eq!(store.dirty_ram_bytes(), 0);
        assert_eq!(store.dirty_ssd_bytes(), 64 * 1024);
        store.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn caller_cancellation_after_owned_handoff_cannot_lose_the_write() {
        let (store, _remote, _temp) = test_store().await;
        let path = Path::from("segments/owned");
        let key_guard = store.key_lock(&path).lock_owned().await;
        let caller = tokio::spawn({
            let store = store.clone();
            let path = path.clone();
            async move {
                store
                    .put(&path, Bytes::from_static(b"payload").into())
                    .await
            }
        });
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            while store.dirty_ram_bytes() == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();

        caller.abort();
        assert!(caller.await.unwrap_err().is_cancelled());
        drop(key_guard);

        store.wait_local(1).await.unwrap();
        assert_eq!(
            store.get(&path).await.unwrap().bytes().await.unwrap(),
            Bytes::from_static(b"payload")
        );
        store.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn copy_resolves_pending_and_remote_sources_and_honors_create_mode() {
        let (store, remote, _temp) = test_store().await;
        store
            .put(
                &Path::from("pending-source"),
                Bytes::from_static(b"pending").into(),
            )
            .await
            .unwrap();
        remote
            .put(
                &Path::from("remote-source"),
                Bytes::from_static(b"remote").into(),
            )
            .await
            .unwrap();

        store
            .copy_opts(
                &Path::from("pending-source"),
                &Path::from("pending-copy"),
                CopyOptions::default(),
            )
            .await
            .unwrap();
        store
            .copy_opts(
                &Path::from("remote-source"),
                &Path::from("remote-copy"),
                CopyOptions {
                    mode: CopyMode::Create,
                    ..CopyOptions::default()
                },
            )
            .await
            .unwrap();
        assert!(
            store
                .copy_opts(
                    &Path::from("remote-source"),
                    &Path::from("remote-copy"),
                    CopyOptions {
                        mode: CopyMode::Create,
                        ..CopyOptions::default()
                    },
                )
                .await
                .is_err()
        );

        assert_eq!(
            store
                .get(&Path::from("pending-copy"))
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap(),
            Bytes::from_static(b"pending")
        );
        assert_eq!(
            store
                .get(&Path::from("remote-copy"))
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap(),
            Bytes::from_static(b"remote")
        );
        store.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn rename_atomically_hides_source_and_exposes_target() {
        let (store, _remote, _temp) = test_store().await;
        store
            .put(&Path::from("source"), Bytes::from_static(b"payload").into())
            .await
            .unwrap();

        store
            .rename_opts(
                &Path::from("source"),
                &Path::from("target"),
                RenameOptions {
                    target_mode: RenameTargetMode::Create,
                    ..RenameOptions::default()
                },
            )
            .await
            .unwrap();

        assert!(store.get(&Path::from("source")).await.is_err());
        assert_eq!(
            store
                .get(&Path::from("target"))
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap(),
            Bytes::from_static(b"payload")
        );
        store.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn copy_and_rename_recover_from_the_local_journal() {
        let temp = tempfile::tempdir().unwrap();
        let remote = Arc::new(InMemory::new());
        let identity = JournalIdentity {
            format_version: 1,
            bucket_id: "bucket-a".to_owned(),
            backend_endpoint: "memory://remote".to_owned(),
            database_prefix: "zerofs/pilot".to_owned(),
            backend_kind: "memory".to_owned(),
            encryption_key_identity_sha256: [0x77; 32],
        };
        let settings = WritebackSettings {
            dir: temp.path().join("writeback"),
            ack_mode: AckMode::Ssd,
            memory_bytes: 1_000_000,
            disk_bytes: 10_000_000,
            min_free_bytes: 1,
            high_watermark_percent: 95,
            resume_percent: 85,
            upload_concurrency: 4,
            shutdown_flush: ShutdownFlush::Local,
        };
        let journal = Arc::new(Journal::open(settings.dir.clone(), identity.clone()).unwrap());
        let store = WritebackObjectStore::open(remote.clone(), journal, settings.clone())
            .await
            .unwrap();
        store
            .put(&Path::from("source"), Bytes::from_static(b"payload").into())
            .await
            .unwrap();
        store
            .copy(&Path::from("source"), &Path::from("copy"))
            .await
            .unwrap();
        store
            .rename(&Path::from("source"), &Path::from("renamed"))
            .await
            .unwrap();
        store.shutdown().await.unwrap();
        drop(store);

        let journal = Arc::new(Journal::open(settings.dir.clone(), identity).unwrap());
        let recovered = WritebackObjectStore::open(remote, journal, settings)
            .await
            .unwrap();
        assert_eq!(recovered.dirty_ssd_bytes(), 21);
        assert!(recovered.get(&Path::from("source")).await.is_err());
        for path in ["copy", "renamed"] {
            assert_eq!(
                recovered
                    .get(&Path::from(path))
                    .await
                    .unwrap()
                    .bytes()
                    .await
                    .unwrap(),
                Bytes::from_static(b"payload")
            );
        }
        recovered.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn reversed_two_key_renames_do_not_deadlock() {
        let (store, _remote, _temp) = test_store().await;
        store
            .put(&Path::from("a"), Bytes::from_static(b"a").into())
            .await
            .unwrap();
        store
            .put(&Path::from("b"), Bytes::from_static(b"b").into())
            .await
            .unwrap();

        let a = Path::from("a");
        let b = Path::from("b");
        let first = store.rename(&a, &b);
        let second = store.rename(&b, &a);
        tokio::time::timeout(Duration::from_secs(2), async move {
            tokio::try_join!(first, second)
        })
        .await
        .expect("reversed key order deadlocked")
        .unwrap();
        store.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn canceled_copy_caller_does_not_cancel_owned_mutation() {
        let (store, _remote, _temp) = test_store().await;
        let source = Path::from("source");
        let target = Path::from("target");
        store
            .put(&source, Bytes::from_static(b"payload").into())
            .await
            .unwrap();
        let blocker = store.key_lock(&target).lock_owned().await;
        let caller_store = store.clone();
        let caller_source = source.clone();
        let caller_target = target.clone();
        let caller =
            tokio::spawn(async move { caller_store.copy(&caller_source, &caller_target).await });
        for _ in 0..10 {
            tokio::task::yield_now().await;
        }
        caller.abort();
        drop(blocker);

        let payload = tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if let Ok(result) = store.get(&target).await {
                    break result.bytes().await.unwrap();
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("owned copy did not finish after caller cancellation");
        assert_eq!(payload, Bytes::from_static(b"payload"));
        store.shutdown().await.unwrap();
    }
}
