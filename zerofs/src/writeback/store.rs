use crate::writeback::admission::{Admission, DiskAdmission};
use crate::writeback::config::{AckMode, WritebackSettings};
use crate::writeback::journal::Journal;
use crate::writeback::journaler::{LocalBarrierError, LocalJournaler};
use crate::writeback::model::{
    FenceClass, LocalEtag, MutationKind, MutationMode, MutationRecord, WritebackStatus,
};
use crate::writeback::overlay::{OverlayCommitObserver, OverlayIndex, VisibleVersion};
use crate::writeback::payload::VerifiedPayload;
use crate::writeback::remote::{RemoteBarrierError, RemoteScheduler};
use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use futures::{StreamExt, TryStreamExt, stream};
use object_store::path::Path;
use object_store::{
    CopyMode, CopyOptions, Extensions, GetOptions, GetResult, ListResult, MultipartUpload,
    ObjectMeta, ObjectStore, PutMode, PutMultipartOptions, PutOptions, PutPayload, PutResult,
    RenameOptions, RenameTargetMode, UpdateVersion, UploadPart,
};
use std::collections::hash_map::DefaultHasher;
use std::fs::{self, OpenOptions};
use std::hash::{Hash, Hasher};
use std::io::{Read, Write};
#[cfg(unix)]
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
use std::path::{Path as FilePath, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex as StdMutex};
use tokio::sync::{Mutex, Notify, OwnedMutexGuard};
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
    remote: RemoteScheduler,
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
        let overlay = OverlayIndex::recover(remote.clone(), journal.clone()).await?;
        let observer = Arc::new(OverlayCommitObserver::new(overlay.clone(), journal.clone()));
        let queue_depth = settings.upload_concurrency.saturating_mul(4).max(16);
        let journaler = LocalJournaler::start_with_observer(
            journal.clone(),
            admission.clone(),
            queue_depth,
            Some(observer),
        )?;
        let remote = RemoteScheduler::start(
            remote,
            journal.clone(),
            overlay.clone(),
            disk.clone(),
            journaler.barrier(),
            settings.upload_concurrency,
        )?;
        Ok(Self {
            inner: Arc::new(WritebackStoreInner {
                journal,
                settings,
                overlay,
                admission,
                disk,
                journaler,
                remote,
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

    /// Capture every mutation accepted before this barrier and wait until the
    /// contiguous local SSD journal covers that sequence.
    pub async fn wait_local_through_accepted(&self) -> Result<(), LocalBarrierError> {
        let target = {
            let _order_guard = self.inner.admission_order.lock().await;
            self.inner.next_sequence.load(Ordering::Acquire)
        };
        self.wait_local(target).await
    }

    pub async fn wait_remote(&self, sequence: u64) -> Result<(), RemoteBarrierError> {
        self.inner.remote.barrier().wait_remote(sequence).await
    }

    pub fn dirty_ram_bytes(&self) -> u64 {
        self.inner.admission.used_bytes()
    }

    pub fn dirty_ssd_bytes(&self) -> u64 {
        self.inner.disk.used_bytes()
    }

    pub fn status(&self) -> anyhow::Result<WritebackStatus> {
        let snapshot = self.inner.journal.snapshot()?;
        let now = chrono::Utc::now().timestamp_millis().max(0) as u64;
        let pending = snapshot
            .records
            .iter()
            .filter(|record| record.sequence > snapshot.remote_seq)
            .collect::<Vec<_>>();
        let oldest_pending_age_ms = pending
            .iter()
            .map(|record| now.saturating_sub(record.accepted_at_unix_ms))
            .max()
            .unwrap_or(0);
        Ok(WritebackStatus {
            accepted_seq: self.inner.next_sequence.load(Ordering::Acquire),
            local_seq: snapshot.local_seq,
            remote_seq: snapshot.remote_seq,
            dirty_ram_bytes: self.inner.admission.used_bytes(),
            dirty_ram_capacity_bytes: self.inner.settings.memory_bytes,
            dirty_ram_operations: self.inner.admission.used_operations(),
            dirty_ssd_bytes: self.inner.disk.used_bytes(),
            dirty_ssd_capacity_bytes: self.inner.settings.disk_bytes,
            dirty_ssd_operations: pending.len() as u64,
            oldest_pending_age_ms,
            remote_bytes_completed: snapshot.remote_bytes_completed,
            remote_operations_completed: snapshot.remote_seq,
            retries: snapshot.remote_retries,
            terminal_error: self.inner.remote.terminal_error(),
        })
    }

    pub async fn shutdown(&self) -> Result<(), LocalBarrierError> {
        match self.inner.settings.shutdown_flush {
            crate::writeback::config::ShutdownFlush::Local => {
                self.inner.remote.shutdown().await.map_err(|error| {
                    LocalBarrierError::LocalDurability(format!("remote shutdown failed: {error}"))
                })?;
                self.inner.journaler.shutdown().await?;
            }
            crate::writeback::config::ShutdownFlush::Remote => {
                self.inner.journaler.shutdown().await?;
                let target = self.inner.next_sequence.load(Ordering::Acquire);
                self.wait_remote(target).await.map_err(|error| {
                    LocalBarrierError::LocalDurability(format!("remote flush failed: {error}"))
                })?;
                self.inner.remote.shutdown().await.map_err(|error| {
                    LocalBarrierError::LocalDurability(format!("remote shutdown failed: {error}"))
                })?;
            }
        }
        self.inner.disk.close();
        Ok(())
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
        let visible = self.inner.overlay.visible_version(&location).await?;
        let (mode, expected_visible_version, predecessor, fence) =
            validate_put_mode(&location, &options.mode, visible)?;
        let payload = VerifiedPayload::new(bytes);
        let order_guard = self.inner.admission_order.lock().await;
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
                payload_len: payload.byte_len(),
                payload_sha256: payload.sha256(),
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
            .install_verified_memory(record.clone(), payload.clone())
            .await
            .map_err(|error| generic_error(format!("overlay admission failed: {error:#}")))?;
        let barrier = match self
            .inner
            .journaler
            .submit_verified_put_with_disk(record, payload, ram, disk)
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
        } else if self.inner.settings.ack_mode == AckMode::Remote {
            self.wait_remote(sequence)
                .await
                .map_err(|error| generic_error(format!("remote durability failed: {error}")))?;
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
        } else if self.inner.settings.ack_mode == AckMode::Remote {
            self.wait_remote(sequence)
                .await
                .map_err(|error| generic_error(format!("remote durability failed: {error}")))?;
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
        let payload = VerifiedPayload::new(bytes);

        let order_guard = self.inner.admission_order.lock().await;
        let sequence = self.allocate_sequence()?;
        let local_etag = LocalEtag::new(self.inner.incarnation, sequence);
        let payload_sha256 = payload.sha256();
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
                immutable_data_fence(&to)
            } else {
                FenceClass::Fence
            },
            retry_count: 0,
            last_error: None,
        };
        let overlay_result = if rename {
            self.inner
                .overlay
                .install_verified_rename(record.clone(), payload.clone())
                .await
        } else {
            self.inner
                .overlay
                .install_verified_copy(record.clone(), payload.clone())
                .await
        };
        overlay_result.map_err(|error| {
            generic_error(format!("copy/rename overlay admission failed: {error:#}"))
        })?;
        let barrier = match self
            .inner
            .journaler
            .submit_verified_put_with_disk(record, payload, ram, disk)
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
        } else if self.inner.settings.ack_mode == AckMode::Remote {
            self.wait_remote(sequence)
                .await
                .map_err(|error| generic_error(format!("remote durability failed: {error}")))?;
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
        location: &Path,
        options: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        let memory_parts = self.inner.settings.ack_mode == AckMode::Memory;
        let staging = if memory_parts {
            None
        } else {
            Some(
                create_multipart_staging(&self.inner.settings.dir).map_err(|error| {
                    generic_error(format!("failed to create multipart staging: {error}"))
                })?,
            )
        };
        Ok(Box::new(WritebackMultipartUpload {
            store: self.clone(),
            location: location.clone(),
            options,
            staging,
            memory_parts,
            state: Arc::new(StdMutex::new(MultipartState::default())),
            notify: Arc::new(Notify::new()),
            terminal: false,
        }))
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

#[derive(Debug)]
struct WritebackMultipartUpload {
    store: WritebackObjectStore,
    location: Path,
    options: PutMultipartOptions,
    staging: Option<PathBuf>,
    memory_parts: bool,
    state: Arc<StdMutex<MultipartState>>,
    notify: Arc<Notify>,
    terminal: bool,
}

#[derive(Debug, Default)]
struct MultipartState {
    parts: Vec<MultipartPart>,
    active: usize,
    aborted: bool,
}

#[derive(Debug, Clone)]
struct MultipartPart {
    len: u64,
    completed: bool,
    bytes: Option<Bytes>,
}

struct ActivePartGuard {
    state: Arc<StdMutex<MultipartState>>,
    notify: Arc<Notify>,
    index: usize,
    active: bool,
}

impl ActivePartGuard {
    fn finish(mut self, completed: bool, bytes: Option<Bytes>) -> bool {
        let mut state = self.state.lock().unwrap();
        state.active = state
            .active
            .checked_sub(1)
            .expect("active multipart part accounting underflow");
        if completed && !state.aborted {
            state.parts[self.index].completed = true;
            state.parts[self.index].bytes = bytes;
        }
        let aborted = state.aborted;
        self.active = false;
        drop(state);
        self.notify.notify_waiters();
        aborted
    }
}

impl Drop for ActivePartGuard {
    fn drop(&mut self) {
        if !self.active {
            return;
        }
        let mut state = self.state.lock().unwrap();
        state.active = state
            .active
            .checked_sub(1)
            .expect("active multipart part accounting underflow");
        drop(state);
        self.notify.notify_waiters();
    }
}

#[async_trait]
impl MultipartUpload for WritebackMultipartUpload {
    fn put_part(&mut self, data: PutPayload) -> UploadPart {
        if self.terminal {
            return Box::pin(async {
                Err(generic_error(
                    "multipart upload is already completed or aborted",
                ))
            });
        }
        let Ok(len) = u64::try_from(data.content_length()) else {
            return Box::pin(async { Err(generic_error("multipart part is too large")) });
        };
        let index = {
            let mut state = self.state.lock().unwrap();
            let Some(total) = state
                .parts
                .iter()
                .try_fold(len, |total, part| total.checked_add(part.len))
            else {
                return Box::pin(async { Err(generic_error("multipart length overflow")) });
            };
            if total > self.store.inner.settings.disk_bytes {
                return Box::pin(async {
                    Err(generic_error(
                        "multipart object exceeds the dirty SSD budget",
                    ))
                });
            }
            let index = state.parts.len();
            state.parts.push(MultipartPart {
                len,
                completed: false,
                bytes: None,
            });
            index
        };
        let staging = self.staging.clone();
        let memory_parts = self.memory_parts;
        let state = self.state.clone();
        let notify = self.notify.clone();
        let min_free_bytes = self.store.inner.settings.min_free_bytes;
        Box::pin(async move {
            {
                let mut state = state.lock().unwrap();
                if state.aborted {
                    return Err(generic_error("multipart upload was aborted"));
                }
                state.active += 1;
            }
            let guard = ActivePartGuard {
                state: state.clone(),
                notify,
                index,
                active: true,
            };
            if memory_parts {
                let bytes = Bytes::from(data);
                let valid = bytes.len() as u64 == len;
                let aborted = guard.finish(valid, valid.then_some(bytes));
                if aborted {
                    return Err(generic_error("multipart upload was aborted"));
                }
                return if valid {
                    Ok(())
                } else {
                    Err(generic_error("multipart part length mismatch"))
                };
            }
            let staging = staging.ok_or_else(|| generic_error("multipart staging is missing"))?;
            let part_path = staging.join(format!("part-{index:020}"));
            let write = match tokio::task::spawn_blocking(move || {
                write_multipart_part(&part_path, data, min_free_bytes)
            })
            .await
            {
                Ok(result) => result.map_err(|error| {
                    generic_error(format!("multipart part write failed: {error}"))
                }),
                Err(error) => Err(generic_error(format!(
                    "multipart part task failed: {error}"
                ))),
            };
            let aborted = guard.finish(write.is_ok(), None);
            if aborted {
                Err(generic_error("multipart upload was aborted"))
            } else {
                write
            }
        })
    }

    async fn complete(&mut self) -> object_store::Result<PutResult> {
        if self.terminal {
            return Err(generic_error(
                "multipart upload is already completed or aborted",
            ));
        }
        let (part_lengths, memory_payload, total_len) = {
            let state = self.state.lock().unwrap();
            if state.aborted {
                return Err(generic_error("multipart upload was aborted"));
            }
            if state.active != 0 || state.parts.iter().any(|part| !part.completed) {
                return Err(generic_error(
                    "multipart upload completed before every part future finished",
                ));
            }
            let total = state
                .parts
                .iter()
                .try_fold(0_u64, |total, part| total.checked_add(part.len));
            let memory_payload = if self.memory_parts {
                Some(
                    state
                        .parts
                        .iter()
                        .map(|part| part.bytes.clone())
                        .collect::<Option<Vec<_>>>()
                        .ok_or_else(|| generic_error("memory multipart part is missing"))?,
                )
            } else {
                None
            };
            (
                state.parts.iter().map(|part| part.len).collect::<Vec<_>>(),
                memory_payload,
                total.ok_or_else(|| generic_error("multipart length overflow"))?,
            )
        };
        let staging = self.staging.take();
        self.terminal = true;
        let store = self.store.clone();
        let location = self.location.clone();
        let options = self.options.clone();
        tokio::spawn(async move {
            if let Some(parts) = memory_payload {
                complete_memory_multipart(store, location, options, parts, total_len).await
            } else {
                let staging =
                    staging.ok_or_else(|| generic_error("multipart staging is missing"))?;
                complete_multipart(store, location, options, staging, part_lengths, total_len).await
            }
        })
        .await
        .map_err(|error| generic_error(format!("owned multipart completion failed: {error}")))?
    }

    async fn abort(&mut self) -> object_store::Result<()> {
        if self.terminal {
            return Ok(());
        }
        self.terminal = true;
        let staging = self.staging.take();
        {
            self.state.lock().unwrap().aborted = true;
        }
        wait_for_multipart_parts(&self.state, &self.notify).await;
        if let Some(staging) = staging {
            cleanup_multipart_staging(staging).await?;
        }
        Ok(())
    }
}

impl Drop for WritebackMultipartUpload {
    fn drop(&mut self) {
        if self.terminal {
            return;
        }
        self.state.lock().unwrap().aborted = true;
        let Some(staging) = self.staging.take() else {
            return;
        };
        let state = self.state.clone();
        let notify = self.notify.clone();
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            runtime.spawn(async move {
                wait_for_multipart_parts(&state, &notify).await;
                if let Err(error) = cleanup_multipart_staging(staging).await {
                    tracing::warn!(%error, "failed to clean dropped writeback multipart staging");
                }
            });
        } else if let Err(error) = remove_private_directory(&staging) {
            tracing::warn!(%error, "failed to clean dropped writeback multipart staging");
        }
    }
}

async fn complete_memory_multipart(
    store: WritebackObjectStore,
    location: Path,
    options: PutMultipartOptions,
    parts: Vec<Bytes>,
    total_len: u64,
) -> object_store::Result<PutResult> {
    let ram = store
        .inner
        .admission
        .reserve(total_len)
        .await
        .map_err(|error| generic_error(format!("dirty RAM admission failed: {error}")))?
        .accept();
    let capacity = usize::try_from(total_len)
        .map_err(|_| generic_error("multipart object exceeds addressable memory"))?;
    let mut assembled = Vec::with_capacity(capacity);
    for part in parts {
        assembled.extend_from_slice(&part);
    }
    if assembled.len() != capacity {
        return Err(generic_error("multipart assembled length mismatch"));
    }
    let available = fs4::available_space(&store.inner.settings.dir)
        .map_err(|error| generic_error(format!("failed to inspect writeback SSD: {error}")))?;
    let disk = store
        .inner
        .disk
        .reserve(total_len, available)
        .await
        .map_err(|error| generic_error(format!("dirty SSD admission failed: {error}")))?;
    let put_options = PutOptions {
        mode: PutMode::Overwrite,
        tags: options.tags,
        attributes: options.attributes,
        extensions: options.extensions,
    };
    store
        .clone()
        .owned_put(location, Bytes::from(assembled), put_options, ram, disk)
        .await
}

async fn complete_multipart(
    store: WritebackObjectStore,
    location: Path,
    options: PutMultipartOptions,
    staging: PathBuf,
    part_lengths: Vec<u64>,
    total_len: u64,
) -> object_store::Result<PutResult> {
    let mut cleanup = MultipartCleanupGuard::new(staging.clone());
    let ram = store
        .inner
        .admission
        .reserve(total_len)
        .await
        .map_err(|error| generic_error(format!("dirty RAM admission failed: {error}")))?
        .accept();
    let available = fs4::available_space(&store.inner.settings.dir)
        .map_err(|error| generic_error(format!("failed to inspect writeback SSD: {error}")))?;
    let disk = store
        .inner
        .disk
        .reserve(total_len, available)
        .await
        .map_err(|error| generic_error(format!("dirty SSD admission failed: {error}")))?;
    let read_staging = staging.clone();
    let bytes = tokio::task::spawn_blocking(move || {
        read_multipart_parts(&read_staging, &part_lengths, total_len)
    })
    .await
    .map_err(|error| generic_error(format!("multipart assembly task failed: {error}")))?
    .map_err(|error| generic_error(format!("multipart assembly failed: {error}")))?;
    let put_options = PutOptions {
        mode: PutMode::Overwrite,
        tags: options.tags,
        attributes: options.attributes,
        extensions: options.extensions,
    };
    let owned = store.clone();
    let result = tokio::spawn(async move {
        owned
            .owned_put(location, bytes, put_options, ram, disk)
            .await
    })
    .await
    .map_err(|error| generic_error(format!("owned multipart put failed: {error}")))?;
    match result {
        Ok(result) => {
            cleanup_multipart_staging(staging).await?;
            cleanup.disarm();
            Ok(result)
        }
        Err(error) => Err(error),
    }
}

struct MultipartCleanupGuard {
    staging: Option<PathBuf>,
}

impl MultipartCleanupGuard {
    fn new(staging: PathBuf) -> Self {
        Self {
            staging: Some(staging),
        }
    }

    fn disarm(&mut self) {
        self.staging = None;
    }
}

impl Drop for MultipartCleanupGuard {
    fn drop(&mut self) {
        if let Some(staging) = self.staging.take()
            && let Err(error) = remove_private_directory(&staging)
            && error.kind() != std::io::ErrorKind::NotFound
        {
            tracing::warn!(%error, "failed to clean failed writeback multipart staging");
        }
    }
}

async fn wait_for_multipart_parts(state: &StdMutex<MultipartState>, notify: &Notify) {
    loop {
        let notified = notify.notified();
        if state.lock().unwrap().active == 0 {
            return;
        }
        notified.await;
    }
}

async fn cleanup_multipart_staging(staging: PathBuf) -> object_store::Result<()> {
    tokio::task::spawn_blocking(move || remove_private_directory(&staging))
        .await
        .map_err(|error| generic_error(format!("multipart cleanup task failed: {error}")))?
        .map_err(|error| generic_error(format!("multipart cleanup failed: {error}")))
}

fn create_multipart_staging(writeback_root: &FilePath) -> std::io::Result<PathBuf> {
    let root = writeback_root.join("tmp").join("multipart");
    match fs::create_dir(&root) {
        Ok(()) => set_directory_mode(&root)?,
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
        Err(error) => return Err(error),
    }
    validate_private_directory(&root)?;
    let staging = root.join(Uuid::new_v4().to_string());
    fs::create_dir(&staging)?;
    set_directory_mode(&staging)?;
    Ok(staging)
}

fn write_multipart_part(
    path: &FilePath,
    payload: PutPayload,
    min_free_bytes: u64,
) -> std::io::Result<()> {
    let len = u64::try_from(payload.content_length())
        .map_err(|_| std::io::Error::other("multipart part length exceeds u64"))?;
    let available = fs4::available_space(path.parent().unwrap_or(path))?;
    if available < min_free_bytes.saturating_add(len) {
        return Err(std::io::Error::other(
            "multipart part would consume the writeback filesystem reserve",
        ));
    }
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    options.mode(0o600).custom_flags(libc::O_NOFOLLOW);
    let mut file = options.open(path)?;
    #[cfg(unix)]
    file.set_permissions(fs::Permissions::from_mode(0o600))?;
    for chunk in payload {
        file.write_all(&chunk)?;
    }
    file.sync_all()?;
    Ok(())
}

fn read_multipart_parts(
    staging: &FilePath,
    part_lengths: &[u64],
    total_len: u64,
) -> std::io::Result<Bytes> {
    let capacity = usize::try_from(total_len)
        .map_err(|_| std::io::Error::other("multipart object exceeds addressable memory"))?;
    let mut assembled = Vec::with_capacity(capacity);
    for (index, expected_len) in part_lengths.iter().copied().enumerate() {
        let path = staging.join(format!("part-{index:020}"));
        let mut options = OpenOptions::new();
        options.read(true);
        #[cfg(unix)]
        options.custom_flags(libc::O_NOFOLLOW);
        let mut file = options.open(path)?;
        if file.metadata()?.len() != expected_len {
            return Err(std::io::Error::other("multipart part length mismatch"));
        }
        file.read_to_end(&mut assembled)?;
    }
    if assembled.len() != capacity {
        return Err(std::io::Error::other("multipart assembled length mismatch"));
    }
    Ok(Bytes::from(assembled))
}

fn remove_private_directory(path: &FilePath) -> std::io::Result<()> {
    let metadata = fs::symlink_metadata(path)?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        return Err(std::io::Error::other(
            "multipart staging path is not a private directory",
        ));
    }
    fs::remove_dir_all(path)
}

fn validate_private_directory(path: &FilePath) -> std::io::Result<()> {
    let metadata = fs::symlink_metadata(path)?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        return Err(std::io::Error::other(
            "multipart staging root is not a directory",
        ));
    }
    #[cfg(unix)]
    if metadata.permissions().mode() & 0o777 != 0o700 {
        return Err(std::io::Error::other(
            "multipart staging root is not mode 0700",
        ));
    }
    Ok(())
}

fn set_directory_mode(path: &FilePath) -> std::io::Result<()> {
    #[cfg(unix)]
    fs::set_permissions(path, fs::Permissions::from_mode(0o700))?;
    Ok(())
}

fn validate_put_mode(
    location: &Path,
    mode: &PutMode,
    visible: Option<VisibleVersion>,
) -> object_store::Result<(MutationMode, Option<String>, Option<String>, FenceClass)> {
    match mode {
        PutMode::Overwrite => Ok((
            MutationMode::Overwrite,
            None,
            None,
            immutable_data_fence(location),
        )),
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
                immutable_data_fence(location),
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

fn immutable_data_fence(location: &Path) -> FenceClass {
    let parts = location
        .parts()
        .map(|part| part.as_ref().to_owned())
        .collect::<Vec<_>>();

    if let Some(index) = parts.iter().rposition(|part| part == "segments") {
        let tail = &parts[index + 1..];
        if tail.len() == 3
            && is_fixed_hex(&tail[0], 2)
            && is_fixed_hex(&tail[1], 16)
            && is_fixed_hex(&tail[2], 16)
        {
            return FenceClass::ImmutableCreate;
        }
    }

    if parts.len() >= 2 {
        let directory = parts[parts.len() - 2].as_str();
        let filename = parts[parts.len() - 1].as_str();
        let immutable_sst = match directory {
            "wal" => filename.strip_suffix(".sst").is_some_and(|stem| {
                stem.len() == 20 && stem.bytes().all(|byte| byte.is_ascii_digit())
            }),
            "compacted" => filename.strip_suffix(".sst").is_some_and(|stem| {
                stem.len() == 26
                    && stem
                        .bytes()
                        .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit())
            }),
            _ => false,
        };
        if immutable_sst {
            return FenceClass::ImmutableCreate;
        }
    }

    FenceClass::Fence
}

fn is_fixed_hex(value: &str, len: usize) -> bool {
    value.len() == len && value.bytes().all(|byte| byte.is_ascii_hexdigit())
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
    use super::{WritebackObjectStore, validate_put_mode};
    use crate::fault_store::{FaultControls, FaultStore};
    use crate::writeback::config::{AckMode, ShutdownFlush, WritebackSettings};
    use crate::writeback::journal::Journal;
    use crate::writeback::model::{FenceClass, JournalIdentity};
    use bytes::Bytes;
    use futures::{StreamExt, stream};
    use object_store::memory::InMemory;
    use object_store::path::Path;
    use object_store::{
        CopyMode, CopyOptions, ObjectStore, ObjectStoreExt, PutMode, PutOptions, RenameOptions,
        RenameTargetMode, UpdateVersion,
    };
    #[cfg(unix)]
    use std::os::unix::fs::PermissionsExt;
    use std::sync::Arc;
    use std::time::Duration;

    async fn test_store() -> (WritebackObjectStore, Arc<InMemory>, tempfile::TempDir) {
        test_store_with_remote_drain(false).await
    }

    #[test]
    fn only_recognized_immutable_data_objects_bypass_remote_fences() {
        let cases = [
            (
                "zerofs/pilot/segments/0a/0000000000000001/0000000000000002",
                PutMode::Overwrite,
                FenceClass::ImmutableCreate,
            ),
            (
                "zerofs/pilot/wal/00000000000000000042.sst",
                PutMode::Create,
                FenceClass::ImmutableCreate,
            ),
            (
                "zerofs/pilot/compacted/01KZS4K6C1G11KM91DJ3YA9TJE.sst",
                PutMode::Create,
                FenceClass::ImmutableCreate,
            ),
            (
                "zerofs/pilot/manifest/00000000000000000134.manifest",
                PutMode::Create,
                FenceClass::Fence,
            ),
            (
                "zerofs/pilot/gc/manifest.boundary",
                PutMode::Create,
                FenceClass::Fence,
            ),
            (
                "zerofs/pilot/unknown/object",
                PutMode::Create,
                FenceClass::Fence,
            ),
        ];

        for (path, mode, expected) in cases {
            let (_, _, _, actual) = validate_put_mode(&Path::from(path), &mode, None).unwrap();
            assert_eq!(actual, expected, "classification for {path}");
        }
    }

    async fn test_store_with_remote_drain(
        enabled: bool,
    ) -> (WritebackObjectStore, Arc<InMemory>, tempfile::TempDir) {
        let (store, remote, temp, _controls) = test_store_with_controls(enabled).await;
        (store, remote, temp)
    }

    async fn test_ssd_store() -> (WritebackObjectStore, Arc<InMemory>, tempfile::TempDir) {
        let (store, remote, temp, _controls) =
            test_store_with_options(false, AckMode::Ssd, ShutdownFlush::Local).await;
        (store, remote, temp)
    }

    async fn test_store_with_controls(
        enabled: bool,
    ) -> (
        WritebackObjectStore,
        Arc<InMemory>,
        tempfile::TempDir,
        Arc<FaultControls>,
    ) {
        test_store_with_options(enabled, AckMode::Memory, ShutdownFlush::Local).await
    }

    async fn test_store_with_options(
        enabled: bool,
        ack_mode: AckMode,
        shutdown_flush: ShutdownFlush,
    ) -> (
        WritebackObjectStore,
        Arc<InMemory>,
        tempfile::TempDir,
        Arc<FaultControls>,
    ) {
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
        let (writeback_remote, controls) = FaultStore::new(remote.clone());
        controls.partition_writes(!enabled);
        let settings = WritebackSettings {
            dir: temp.path().join("writeback"),
            ack_mode,
            memory_bytes: 1_000_000,
            disk_bytes: 10_000_000,
            min_free_bytes: 1,
            high_watermark_percent: 95,
            resume_percent: 85,
            upload_concurrency: 4,
            shutdown_flush,
        };
        let store = WritebackObjectStore::open(writeback_remote, journal, settings)
            .await
            .unwrap();
        (store, remote, temp, controls)
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
    async fn local_barrier_covers_current_accepted_sequence_in_memory_ack_mode() {
        let (store, _remote, _temp) = test_store().await;
        store
            .put(
                &Path::from("segments/local-barrier"),
                Bytes::from_static(b"payload").into(),
            )
            .await
            .unwrap();

        store.wait_local_through_accepted().await.unwrap();

        let status = store.status().unwrap();
        assert_eq!(status.accepted_seq, 1);
        assert_eq!(status.local_seq, status.accepted_seq);
        assert_eq!(status.remote_seq, 0);
        store.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn status_reports_independent_dirty_ram_and_ssd_tiers() {
        let (store, _remote, _temp) = test_store().await;
        store
            .put(
                &Path::from("status-pending"),
                Bytes::from_static(b"payload").into(),
            )
            .await
            .unwrap();
        store.wait_local(1).await.unwrap();

        let status = store.status().unwrap();
        assert_eq!(status.accepted_seq, 1);
        assert_eq!(status.local_seq, 1);
        assert_eq!(status.remote_seq, 0);
        assert_eq!(status.dirty_ram_bytes, 0);
        assert_eq!(status.dirty_ram_capacity_bytes, 1_000_000);
        assert_eq!(status.dirty_ssd_bytes, 7);
        assert_eq!(status.dirty_ssd_capacity_bytes, 10_000_000);
        assert_eq!(status.dirty_ssd_operations, 1);
        assert!(status.oldest_pending_age_ms < 10_000);
        store.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn status_persists_completed_remote_bytes_and_operations() {
        let (store, _remote, _temp) = test_store_with_remote_drain(true).await;
        store
            .put(
                &Path::from("status-complete"),
                Bytes::from_static(b"payload").into(),
            )
            .await
            .unwrap();
        store.wait_remote(1).await.unwrap();

        let status = store.status().unwrap();
        assert_eq!(status.remote_bytes_completed, 7);
        assert_eq!(status.remote_operations_completed, 1);
        assert_eq!(status.dirty_ssd_bytes, 0);
        assert_eq!(status.dirty_ssd_operations, 0);
        store.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn status_persists_remote_retry_count_after_recovery() {
        let (store, _remote, _temp, controls) = test_store_with_controls(true).await;
        controls.fail_puts(1);
        store
            .put(
                &Path::from("status-retry"),
                Bytes::from_static(b"payload").into(),
            )
            .await
            .unwrap();
        store.wait_remote(1).await.unwrap();

        assert_eq!(store.status().unwrap().retries, 1);
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
    async fn remote_precondition_lookup_does_not_hold_global_admission_order() {
        let (store, _remote, _temp, controls) = test_store_with_controls(false).await;
        let order_guard = store.inner.admission_order.lock().await;
        let path = Path::from("segments/independent");
        let put = tokio::spawn({
            let store = store.clone();
            let path = path.clone();
            async move {
                store
                    .put(&path, Bytes::from_static(b"payload").into())
                    .await
            }
        });
        tokio::time::timeout(Duration::from_secs(1), async {
            while store.dirty_ram_bytes() == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();

        let lookup_started = tokio::time::timeout(Duration::from_secs(1), async {
            while controls.get_count() == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await;

        drop(order_guard);
        put.await.unwrap().unwrap();
        store.wait_local(1).await.unwrap();
        store.shutdown().await.unwrap();
        assert!(
            lookup_started.is_ok(),
            "remote version lookup was serialized behind global admission order"
        );
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
        let (writeback_remote, controls) = FaultStore::new(remote.clone());
        controls.partition_writes(true);
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
        let store = WritebackObjectStore::open(writeback_remote.clone(), journal, settings.clone())
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
        let recovered = WritebackObjectStore::open(writeback_remote, journal, settings)
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

    #[tokio::test]
    async fn multipart_parts_are_private_ordered_and_publish_as_one_mutation() {
        let (store, _remote, _temp) = test_store().await;
        let location = Path::from("multipart-object");
        let before = store.inner.journal.snapshot().unwrap().local_seq;
        let mut upload = store.put_multipart(&location).await.unwrap();
        let first = upload.put_part(Bytes::from_static(b"first-").into());
        let second = upload.put_part(Bytes::from_static(b"second").into());
        futures::future::try_join(second, first).await.unwrap();

        assert!(store.get(&location).await.is_err());
        let result = upload.complete().await.unwrap();
        assert!(
            result
                .e_tag
                .as_deref()
                .is_some_and(|etag| etag.starts_with("wb:"))
        );
        store.wait_local(before + 1).await.unwrap();
        assert_eq!(
            store.inner.journal.snapshot().unwrap().local_seq,
            before + 1
        );
        assert_eq!(
            store.get(&location).await.unwrap().bytes().await.unwrap(),
            Bytes::from_static(b"first-second")
        );
        store.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn memory_ack_multipart_parts_do_not_stage_on_ssd() {
        let (store, _remote, _temp) = test_store().await;
        let staging_root = store.inner.settings.dir.join("tmp").join("multipart");
        let mut upload = store
            .put_multipart(&Path::from("ram-first-segment"))
            .await
            .unwrap();

        upload
            .put_part(Bytes::from_static(b"first").into())
            .await
            .unwrap();

        assert!(
            !staging_root.exists(),
            "memory acknowledgement staged multipart bytes on SSD"
        );
        upload.abort().await.unwrap();
        store.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn multipart_abort_removes_private_parts_without_a_visible_mutation() {
        let (store, _remote, _temp) = test_ssd_store().await;
        let location = Path::from("aborted-object");
        let mut upload = store.put_multipart(&location).await.unwrap();
        upload
            .put_part(Bytes::from_static(b"private").into())
            .await
            .unwrap();
        let staging_root = store.inner.settings.dir.join("tmp/multipart");
        assert_eq!(std::fs::read_dir(&staging_root).unwrap().count(), 1);

        upload.abort().await.unwrap();

        assert_eq!(std::fs::read_dir(&staging_root).unwrap().count(), 0);
        assert!(store.get(&location).await.is_err());
        assert_eq!(store.inner.journal.snapshot().unwrap().local_seq, 0);
        store.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn multipart_abort_does_not_wait_for_an_unpolled_part_future() {
        let (store, _remote, _temp) = test_ssd_store().await;
        let location = Path::from("aborted-unpolled-object");
        let mut upload = store.put_multipart(&location).await.unwrap();
        let unpolled = upload.put_part(Bytes::from_static(b"never-started").into());

        tokio::time::timeout(Duration::from_secs(1), upload.abort())
            .await
            .expect("abort waited for an unpolled part")
            .unwrap();
        assert!(unpolled.await.is_err());
        let staging_root = store.inner.settings.dir.join("tmp/multipart");
        assert_eq!(std::fs::read_dir(&staging_root).unwrap().count(), 0);
        store.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn multipart_assembly_failure_cleans_staging_and_publishes_nothing() {
        let (store, _remote, _temp) = test_ssd_store().await;
        let location = Path::from("corrupt-multipart-object");
        let mut upload = store.put_multipart(&location).await.unwrap();
        upload
            .put_part(Bytes::from_static(b"payload").into())
            .await
            .unwrap();
        let staging_root = store.inner.settings.dir.join("tmp/multipart");
        let staging = std::fs::read_dir(&staging_root)
            .unwrap()
            .next()
            .unwrap()
            .unwrap()
            .path();
        std::fs::OpenOptions::new()
            .write(true)
            .open(staging.join("part-00000000000000000000"))
            .unwrap()
            .set_len(1)
            .unwrap();

        assert!(upload.complete().await.is_err());
        assert_eq!(std::fs::read_dir(&staging_root).unwrap().count(), 0);
        assert!(store.get(&location).await.is_err());
        assert_eq!(store.inner.journal.snapshot().unwrap().local_seq, 0);
        store.shutdown().await.unwrap();
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn dropped_multipart_cleans_owner_only_staging() {
        let (store, _remote, _temp) = test_ssd_store().await;
        let mut upload = store
            .put_multipart(&Path::from("dropped-multipart-object"))
            .await
            .unwrap();
        upload
            .put_part(Bytes::from_static(b"private").into())
            .await
            .unwrap();
        let staging_root = store.inner.settings.dir.join("tmp/multipart");
        let staging = std::fs::read_dir(&staging_root)
            .unwrap()
            .next()
            .unwrap()
            .unwrap()
            .path();
        let part = staging.join("part-00000000000000000000");
        assert_eq!(
            std::fs::metadata(&staging).unwrap().permissions().mode() & 0o777,
            0o700
        );
        assert_eq!(
            std::fs::metadata(part).unwrap().permissions().mode() & 0o777,
            0o600
        );

        drop(upload);

        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if std::fs::read_dir(&staging_root).unwrap().next().is_none() {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("dropped multipart staging was not cleaned");
        store.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn local_mutation_replays_to_remote_and_reclaims_dirty_ssd() {
        let (store, remote, _temp) = test_store_with_remote_drain(true).await;
        let location = Path::from("remote-drain");
        store
            .put(&location, Bytes::from_static(b"payload").into())
            .await
            .unwrap();

        tokio::time::timeout(Duration::from_secs(3), store.wait_remote(1))
            .await
            .expect("remote replay did not advance")
            .unwrap();

        assert_eq!(
            remote.get(&location).await.unwrap().bytes().await.unwrap(),
            Bytes::from_static(b"payload")
        );
        assert_eq!(store.dirty_ssd_bytes(), 0);
        assert_eq!(store.inner.journal.snapshot().unwrap().remote_seq, 1);
        assert!(store.inner.journal.snapshot().unwrap().records.is_empty());
        store.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn remote_replay_uses_configured_upload_concurrency() {
        let (store, _remote, _temp, controls) = test_store_with_controls(true).await;
        controls.block_puts();
        let puts = (0..4).map(|index| {
            let store = store.clone();
            async move {
                store
                    .put(
                        &Path::from(format!(
                            "segments/{index:02x}/0000000000000001/{index:016x}"
                        )),
                        Bytes::from(vec![index; 1024]).into(),
                    )
                    .await
                    .unwrap();
            }
        });
        futures::future::join_all(puts).await;
        store.wait_local(4).await.unwrap();

        let concurrent = tokio::time::timeout(Duration::from_secs(2), async {
            while controls.max_active_puts() < 4 {
                controls.put_activity().notified().await;
            }
        })
        .await
        .is_ok();
        controls.release_puts();
        store.wait_remote(4).await.unwrap();
        store.shutdown().await.unwrap();

        assert!(
            concurrent,
            "remote replay never reached four concurrent puts"
        );
    }

    #[tokio::test]
    async fn remote_create_recovers_a_lost_success_response_idempotently() {
        let (store, remote, _temp, controls) = test_store_with_controls(true).await;
        controls.fail_puts_after_apply(1);
        let location = Path::from("immutable-create");
        store
            .put_opts(
                &location,
                Bytes::from_static(b"payload").into(),
                PutOptions::from(PutMode::Create),
            )
            .await
            .unwrap();

        let recovered = tokio::time::timeout(Duration::from_secs(2), store.wait_remote(1))
            .await
            .is_ok();
        store.shutdown().await.unwrap();

        assert!(recovered, "lost create reply was not recognized on retry");
        assert_eq!(
            remote.get(&location).await.unwrap().bytes().await.unwrap(),
            Bytes::from_static(b"payload")
        );
    }

    #[tokio::test]
    async fn remote_replay_never_overtakes_an_earlier_same_key_mutation() {
        let (store, remote, _temp, controls) = test_store_with_controls(true).await;
        controls.block_puts();
        let location = Path::from("ordered-key");
        store
            .put(&location, Bytes::from_static(b"obsolete").into())
            .await
            .unwrap();
        store.delete(&location).await.unwrap();
        store.wait_local(2).await.unwrap();
        tokio::time::timeout(Duration::from_secs(2), async {
            while controls.max_active_puts() == 0 {
                controls.put_activity().notified().await;
            }
        })
        .await
        .expect("the blocked predecessor put never started");
        tokio::task::yield_now().await;
        controls.release_puts();
        store.wait_remote(2).await.unwrap();
        store.shutdown().await.unwrap();

        assert!(
            remote.head(&location).await.is_err(),
            "later delete was overtaken by its blocked predecessor put"
        );
    }

    #[tokio::test]
    async fn remote_update_recovers_a_lost_success_response_idempotently() {
        let (store, remote, _temp, controls) = test_store_with_controls(true).await;
        let location = Path::from("mutable-manifest");
        remote
            .put(&location, Bytes::from_static(b"before").into())
            .await
            .unwrap();
        let predecessor = remote.head(&location).await.unwrap();
        controls.fail_puts_after_apply(1);
        store
            .put_opts(
                &location,
                Bytes::from_static(b"after").into(),
                PutOptions::from(PutMode::Update(UpdateVersion {
                    e_tag: predecessor.e_tag,
                    version: predecessor.version,
                })),
            )
            .await
            .unwrap();

        let recovered = tokio::time::timeout(Duration::from_secs(2), store.wait_remote(1))
            .await
            .is_ok();
        store.shutdown().await.unwrap();

        assert!(recovered, "lost update reply was not recognized on retry");
        assert_eq!(
            remote.get(&location).await.unwrap().bytes().await.unwrap(),
            Bytes::from_static(b"after")
        );
    }

    #[tokio::test]
    async fn local_shutdown_cancels_a_stalled_remote_batch() {
        let (store, _remote, _temp, controls) = test_store_with_controls(true).await;
        controls.block_puts();
        store
            .put(
                &Path::from("stalled-upload"),
                Bytes::from_static(b"payload").into(),
            )
            .await
            .unwrap();
        store.wait_local(1).await.unwrap();
        tokio::time::timeout(Duration::from_secs(2), async {
            while controls.max_active_puts() == 0 {
                controls.put_activity().notified().await;
            }
        })
        .await
        .expect("stalled remote put never started");

        let mut shutdown = tokio::spawn({
            let store = store.clone();
            async move { store.shutdown().await }
        });
        let bounded = tokio::time::timeout(Duration::from_secs(1), &mut shutdown)
            .await
            .is_ok();
        if !bounded {
            controls.release_puts();
            shutdown.await.unwrap().unwrap();
        }

        assert!(bounded, "local shutdown waited for a stalled remote upload");
    }

    #[tokio::test]
    async fn remote_shutdown_flush_drains_the_local_journal() {
        let (store, remote, _temp, _controls) =
            test_store_with_options(true, AckMode::Memory, ShutdownFlush::Remote).await;
        for index in 0..4 {
            store
                .put(
                    &Path::from(format!("shutdown-drain/{index}")),
                    Bytes::from(vec![index; 1024]).into(),
                )
                .await
                .unwrap();
        }

        tokio::time::timeout(Duration::from_secs(3), store.shutdown())
            .await
            .expect("remote shutdown flush did not finish")
            .unwrap();

        for index in 0..4 {
            assert_eq!(
                remote
                    .get(&Path::from(format!("shutdown-drain/{index}")))
                    .await
                    .unwrap()
                    .bytes()
                    .await
                    .unwrap(),
                Bytes::from(vec![index; 1024])
            );
        }
        assert_eq!(store.dirty_ssd_bytes(), 0);
        let snapshot = store.inner.journal.snapshot().unwrap();
        assert_eq!(snapshot.remote_seq, 4);
        assert!(snapshot.records.is_empty());
    }

    #[tokio::test]
    async fn restart_automatically_resumes_dirty_ssd_replay() {
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
            ack_mode: AckMode::Memory,
            memory_bytes: 1_000_000,
            disk_bytes: 10_000_000,
            min_free_bytes: 1,
            high_watermark_percent: 95,
            resume_percent: 85,
            upload_concurrency: 4,
            shutdown_flush: ShutdownFlush::Local,
        };
        let (partitioned, controls) = FaultStore::new(remote.clone());
        controls.partition_writes(true);
        let first = WritebackObjectStore::open(
            partitioned,
            Arc::new(Journal::open(settings.dir.clone(), identity.clone()).unwrap()),
            settings.clone(),
        )
        .await
        .unwrap();
        let location = Path::from("restart-dirty");
        first
            .put(&location, Bytes::from_static(b"payload").into())
            .await
            .unwrap();
        first.wait_local(1).await.unwrap();
        first.shutdown().await.unwrap();
        assert_eq!(first.inner.journal.snapshot().unwrap().dirty_blob_bytes, 7);
        drop(first);

        let resumed = WritebackObjectStore::open(
            remote.clone(),
            Arc::new(Journal::open(settings.dir.clone(), identity).unwrap()),
            settings,
        )
        .await
        .unwrap();
        tokio::time::timeout(Duration::from_secs(3), resumed.wait_remote(1))
            .await
            .expect("restart did not resume remote replay")
            .unwrap();

        assert_eq!(
            remote.get(&location).await.unwrap().bytes().await.unwrap(),
            Bytes::from_static(b"payload")
        );
        assert_eq!(resumed.dirty_ssd_bytes(), 0);
        resumed.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn remote_outage_retries_are_rate_limited_instead_of_hammering_sftp() {
        let (store, _remote, _temp, controls) = test_store_with_controls(false).await;
        store
            .put(
                &Path::from("outage-backoff"),
                Bytes::from_static(b"payload").into(),
            )
            .await
            .unwrap();
        store.wait_local(1).await.unwrap();
        tokio::time::timeout(Duration::from_secs(2), async {
            while controls.put_count() == 0 {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("remote replay never attempted the first upload");

        tokio::time::sleep(Duration::from_millis(400)).await;
        let attempts = controls.put_count();
        store.shutdown().await.unwrap();

        assert!(
            attempts <= 3,
            "remote outage caused {attempts} attempts in 400ms"
        );
    }
}
