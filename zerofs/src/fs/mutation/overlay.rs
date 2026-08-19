//! Filesystem-facing volatile overlay shared by NFS, 9P, WebUI, and NBD.
//!
//! One process-wide [`VolatileBudget`] bounds RAM. Each inode gets a single-lane
//! [`VolatileWriteRuntime`] so accepted writes are visible to later reads and
//! getattr before the canonical apply finishes. Protocol adapters call
//! [`ZeroFS::write_ack`] and [`ZeroFS::wait_configured_durability`].

use super::volatile_overlay::{
    Materializer, OverlayError, OverlayResult, VolatileAdmission, VolatileBudget,
    VolatileWriteRuntime, WriteChunk,
};
use crate::fs::ZeroFS;
use crate::fs::errors::FsError;
use crate::fs::inode::{Inode, InodeId};
use crate::fs::mutation::config::FilesystemWriteAckMode;
use crate::fs::mutation::types::{PrepareWriteMember, PrepareWriteRequest, PreparedWriteBatch};
use crate::fs::ops::write::{apply_prepared_batch, prepare_write};
use crate::fs::types::{AuthContext, FileAttributes};
use bytes::Bytes;
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, Weak};

pub(crate) struct FilesystemVolatileOverlay {
    budget: Arc<VolatileBudget>,
    runtimes: Mutex<HashMap<u64, Arc<VolatileWriteRuntime>>>,
    pending: Mutex<HashMap<u64, VecDeque<PreparedWriteBatch>>>,
    latest_attrs: Mutex<HashMap<u64, FileAttributes>>,
    frozen: AtomicBool,
    materializer_sequence: AtomicU64,
    fs: Weak<ZeroFS>,
}

impl FilesystemVolatileOverlay {
    pub(crate) fn new(max_bytes: u64, max_operations: usize, fs: Weak<ZeroFS>) -> Arc<Self> {
        Arc::new(Self {
            budget: VolatileBudget::new(max_bytes, max_operations),
            runtimes: Mutex::new(HashMap::new()),
            pending: Mutex::new(HashMap::new()),
            latest_attrs: Mutex::new(HashMap::new()),
            frozen: AtomicBool::new(false),
            materializer_sequence: AtomicU64::new(0),
            fs,
        })
    }

    pub(crate) fn budget(&self) -> Arc<VolatileBudget> {
        Arc::clone(&self.budget)
    }

    fn runtime(self: &Arc<Self>, inode: u64) -> Arc<VolatileWriteRuntime> {
        let mut runtimes = self.runtimes.lock().expect("filesystem overlay poisoned");
        runtimes
            .entry(inode)
            .or_insert_with(|| {
                let overlay = Arc::clone(self);
                let materialize: Materializer = Arc::new(move |inode, offset, data| {
                    let overlay = Arc::clone(&overlay);
                    Box::pin(async move { overlay.materialize(inode, offset, data).await })
                });
                VolatileWriteRuntime::new(Arc::clone(&self.budget), vec![inode], materialize)
            })
            .clone()
    }

    async fn materialize(
        self: Arc<Self>,
        inode: u64,
        _offset: u64,
        _data: Bytes,
    ) -> OverlayResult<()> {
        let mut batch = {
            let mut pending = self.pending.lock().expect("filesystem overlay poisoned");
            pending
                .get_mut(&inode)
                .and_then(VecDeque::pop_front)
                .ok_or(OverlayError::IoError)?
        };
        let Some(fs) = self.fs.upgrade() else {
            return Err(OverlayError::IoError);
        };
        if let Some(materializer) = fs.materializer.get() {
            let sequence = self.materializer_sequence.fetch_add(1, Ordering::Relaxed) + 1;
            let cutoff = crate::fs::mutation::types::MutationCutoff {
                mutation_incarnation: materializer.incarnation(),
                sequence,
            };
            materializer
                .dispatch_through(cutoff, batch)
                .await
                .map_err(|_| OverlayError::IoError)?;
            return Ok(());
        }
        // Accept drops the prepare locks once the write is overlay-visible.
        // Re-acquire them so apply cannot race setattr/unlink on the same inode.
        let _apply_guards = if batch.guards.is_none() {
            let ids = batch.members.iter().map(|member| member.id).collect();
            Some(fs.lock_manager.acquire_multi(ids).await)
        } else {
            None
        };
        apply_prepared_batch(&fs.write_apply_context(), &mut batch)
            .await
            .map(|_| ())?;
        let remaining = {
            let runtimes = self.runtimes.lock().expect("filesystem overlay poisoned");
            runtimes
                .get(&inode)
                .map(|runtime| runtime.dirty_end())
                .unwrap_or(0)
        };
        if remaining == 0 {
            self.latest_attrs
                .lock()
                .expect("filesystem overlay poisoned")
                .remove(&inode);
        }
        Ok(())
    }

    pub(crate) fn visible_size(&self, inode: u64, canonical: u64) -> u64 {
        let dirty = self
            .runtimes
            .lock()
            .expect("filesystem overlay poisoned")
            .get(&inode)
            .map(|runtime| runtime.dirty_end())
            .unwrap_or(0);
        let attrs = self
            .latest_attrs
            .lock()
            .expect("filesystem overlay poisoned")
            .get(&inode)
            .map(|attrs| attrs.size)
            .unwrap_or(0);
        canonical.max(dirty).max(attrs)
    }

    pub(crate) fn visible_attrs(&self, inode: u64, canonical: FileAttributes) -> FileAttributes {
        let canonical_size = canonical.size;
        let mut attrs = self
            .latest_attrs
            .lock()
            .expect("filesystem overlay poisoned")
            .get(&inode)
            .cloned()
            .unwrap_or(canonical);
        attrs.size = self.visible_size(inode, canonical_size);
        attrs
    }

    pub(crate) async fn reserve(
        self: &Arc<Self>,
        inode: u64,
        bytes: usize,
    ) -> OverlayResult<VolatileAdmission> {
        self.runtime(inode).reserve(bytes).await
    }

    pub(crate) fn preview_attrs(&self, inode: u64, attrs: FileAttributes) {
        self.latest_attrs
            .lock()
            .expect("filesystem overlay poisoned")
            .insert(inode, attrs);
    }

    pub(crate) fn retire_inode(&self, inode: u64) {
        // Pending batches are owned by the apply worker that popped them.
        // Only drop the preview attributes once canonical apply owns the bytes.
        self.latest_attrs
            .lock()
            .expect("filesystem overlay poisoned")
            .remove(&inode);
    }

    pub(crate) fn freeze_terminal(&self) {
        self.frozen.store(true, Ordering::Release);
    }

    pub(crate) fn is_frozen(&self) -> bool {
        self.frozen.load(Ordering::Acquire)
    }

    pub(crate) async fn accept(
        self: &Arc<Self>,
        inode: u64,
        admission: VolatileAdmission,
        offset: u64,
        data: Bytes,
        attrs: FileAttributes,
        batch: PreparedWriteBatch,
    ) -> OverlayResult<u64> {
        if self.is_frozen() {
            return Err(OverlayError::IoError);
        }
        let length = data.len();
        let groups = vec![vec![WriteChunk {
            inode,
            member_offset: offset,
            logical_offset: 0,
            length,
        }]];
        {
            let mut pending = self.pending.lock().expect("filesystem overlay poisoned");
            pending.entry(inode).or_default().push_back(batch);
        }
        self.latest_attrs
            .lock()
            .expect("filesystem overlay poisoned")
            .insert(inode, attrs);
        match self
            .runtime(inode)
            .accept_write(admission, offset, data, groups)
            .await
        {
            Ok(sequence) => {
                // The write is now readable from the overlay. Release prepare
                // locks so the next write on this inode can prepare.
                let mut pending = self.pending.lock().expect("filesystem overlay poisoned");
                if let Some(queue) = pending.get_mut(&inode) {
                    for queued in queue.iter_mut() {
                        queued.guards = None;
                    }
                }
                Ok(sequence)
            }
            Err(error) => {
                let mut pending = self.pending.lock().expect("filesystem overlay poisoned");
                if let Some(queue) = pending.get_mut(&inode) {
                    queue.pop_back();
                    if queue.is_empty() {
                        pending.remove(&inode);
                    }
                }
                Err(error)
            }
        }
    }

    pub(crate) async fn read(
        self: &Arc<Self>,
        inode: u64,
        offset: u64,
        length: usize,
        base: impl FnOnce() -> futures::future::BoxFuture<'static, OverlayResult<Bytes>>,
    ) -> OverlayResult<Bytes> {
        let runtime = {
            let runtimes = self.runtimes.lock().expect("filesystem overlay poisoned");
            runtimes.get(&inode).cloned()
        };
        match runtime {
            Some(runtime) => runtime.read(offset, length, base).await,
            None => base().await,
        }
    }

    pub(crate) async fn wait_inode(self: &Arc<Self>, inode: u64) -> OverlayResult<()> {
        let runtime = {
            let runtimes = self.runtimes.lock().expect("filesystem overlay poisoned");
            runtimes.get(&inode).cloned()
        };
        if let Some(runtime) = runtime {
            let target = runtime.accepted_cutoff();
            runtime.wait_materialized(target).await?;
        }
        Ok(())
    }

    pub(crate) async fn wait_all(self: &Arc<Self>) -> OverlayResult<()> {
        let runtimes = {
            let runtimes = self.runtimes.lock().expect("filesystem overlay poisoned");
            runtimes.values().cloned().collect::<Vec<_>>()
        };
        for runtime in runtimes {
            let target = runtime.accepted_cutoff();
            runtime.wait_materialized(target).await?;
        }
        Ok(())
    }

    pub(crate) async fn shutdown(self: &Arc<Self>) -> OverlayResult<()> {
        let runtimes = {
            let runtimes = self.runtimes.lock().expect("filesystem overlay poisoned");
            runtimes.values().cloned().collect::<Vec<_>>()
        };
        let mut first_error = None;
        for runtime in runtimes {
            if let Err(error) = runtime.shutdown().await {
                first_error.get_or_insert(error);
            }
        }
        first_error.map_or(Ok(()), Err)
    }
}

impl ZeroFS {
    /// Install the process-wide volatile overlay once the filesystem is in an
    /// `Arc`. Idempotent; a no-op when acknowledgement is materialized.
    pub fn install_volatile_overlay(self: &Arc<Self>) {
        if self.write_ack.mode != FilesystemWriteAckMode::VolatileMemory {
            return;
        }
        if self.write_ack.volatile_memory_bytes == 0 {
            return;
        }
        let _ = self.volatile_overlay.set(FilesystemVolatileOverlay::new(
            self.write_ack.volatile_memory_bytes,
            self.write_ack.volatile_max_operations,
            Arc::downgrade(self),
        ));
        self.start_materializer();
    }

    /// Start owned apply workers once the filesystem is in an `Arc`.
    pub fn start_materializer(self: &Arc<Self>) {
        let materializer = super::materializer::Materializer::start(
            super::types::MutationIncarnation::new(),
            Arc::downgrade(self),
            self.volatile_overlay.get().cloned(),
        );
        let coordinator = super::fence::MutationCoordinator::new(
            super::admission::PreparationGate::new(materializer.incarnation()),
            materializer.progress(),
        );
        let _ = self.materializer.set(materializer);
        let _ = self.mutation_coordinator.set(coordinator);
    }

    pub(crate) fn volatile_budget(&self) -> Option<Arc<VolatileBudget>> {
        self.volatile_overlay.get().map(|overlay| overlay.budget())
    }

    pub(crate) fn overlay_visible_size(&self, id: InodeId, canonical: u64) -> u64 {
        self.volatile_overlay
            .get()
            .map(|overlay| overlay.visible_size(id, canonical))
            .unwrap_or(canonical)
    }

    pub(crate) fn overlay_is_dirty(&self, id: InodeId) -> bool {
        self.volatile_overlay
            .get()
            .is_some_and(|overlay| overlay.visible_size(id, 0) > 0)
    }

    /// Drain acknowledged overlay writes for `id` so a later canonical
    /// mutation (setattr/trim/unlink) cannot race the materializer.
    pub(crate) async fn quiesce_overlay_inode(&self, id: InodeId) -> Result<(), FsError> {
        if let Some(overlay) = self.volatile_overlay.get() {
            overlay.wait_inode(id).await.map_err(overlay_fs_error)?;
        }
        Ok(())
    }

    pub(crate) async fn visible_inode(&self, id: InodeId) -> Result<Inode, FsError> {
        let mut inode = self.inode_store.get(id).await?;
        if let Inode::File(file) = &mut inode {
            file.size = self.overlay_visible_size(id, file.size);
        }
        Ok(inode)
    }

    /// RAM-ack write used by NFS, 9P, NBD, and WebUI. Materialized mode falls
    /// through to the canonical write path.
    pub async fn write_ack(
        &self,
        auth: &AuthContext,
        id: InodeId,
        offset: u64,
        data: &Bytes,
    ) -> Result<FileAttributes, FsError> {
        self.write_ack_idempotent(auth, id, offset, data, [0u8; 16], true)
            .await
    }

    pub(crate) async fn write_ack_opened_idempotent(
        &self,
        auth: &AuthContext,
        id: InodeId,
        offset: u64,
        data: &Bytes,
        op_id: crate::dedup::OpId,
    ) -> Result<FileAttributes, FsError> {
        self.write_ack_idempotent(auth, id, offset, data, op_id, false)
            .await
    }

    async fn write_ack_idempotent(
        &self,
        auth: &AuthContext,
        id: InodeId,
        offset: u64,
        data: &Bytes,
        op_id: crate::dedup::OpId,
        check_permissions: bool,
    ) -> Result<FileAttributes, FsError> {
        let Some(overlay) = self.volatile_overlay.get().cloned() else {
            return if check_permissions {
                self.write_idempotent(auth, id, offset, data, op_id).await
            } else {
                self.write_opened_idempotent(auth, id, offset, data, op_id)
                    .await
            };
        };
        if data.is_empty() {
            return self.write_idempotent(auth, id, offset, data, op_id).await;
        }

        let admission = overlay
            .reserve(id, data.len())
            .await
            .map_err(overlay_fs_error)?;
        let request = PrepareWriteRequest {
            members: vec![PrepareWriteMember {
                id,
                offset,
                data: data.clone(),
            }],
            auth: auth.clone(),
            op_id,
            check_permissions,
        };
        let batch = prepare_write(&self.write_prepare_context(), request).await?;
        let attrs = batch
            .replayed
            .as_ref()
            .map(|result| result.primary_attrs())
            .or_else(|| {
                batch
                    .members
                    .first()
                    .map(|member| member.post_attrs.clone())
            })
            .expect("prepared batch has attributes");
        overlay
            .accept(id, admission, offset, data.clone(), attrs.clone(), batch)
            .await
            .map_err(overlay_fs_error)?;
        Ok(attrs)
    }

    pub(crate) async fn read_file_visible(
        &self,
        auth: Option<&AuthContext>,
        id: InodeId,
        offset: u64,
        count: u32,
    ) -> Result<(Bytes, bool), FsError> {
        let Some(overlay) = self.volatile_overlay.get().cloned() else {
            return self
                .read_file_inner_canonical(auth, id, offset, count)
                .await;
        };

        let inode = self.inode_store.get(id).await?;
        if let Some(auth) = auth {
            let creds = crate::fs::permissions::Credentials::from_auth_context(auth);
            crate::fs::permissions::check_access(
                &inode,
                &creds,
                crate::fs::permissions::AccessMode::Read,
            )?;
        }
        let Inode::File(file) = &inode else {
            return Err(FsError::IsDirectory);
        };
        let visible_size = overlay.visible_size(id, file.size);
        if offset >= visible_size {
            return Ok((Bytes::new(), true));
        }
        let read_len = std::cmp::min(count as u64, visible_size - offset) as usize;
        let canonical_size = file.size;
        let filesystem = self.extent_store.clone();
        let data = overlay
            .read(id, offset, read_len, move || {
                Box::pin(async move {
                    canonical_read_base(&filesystem, id, offset, read_len, canonical_size).await
                })
            })
            .await
            .map_err(overlay_fs_error)?;
        Ok((data, offset + read_len as u64 >= visible_size))
    }
}

async fn canonical_read_base(
    extent_store: &crate::fs::store::ExtentStore,
    id: InodeId,
    offset: u64,
    read_len: usize,
    canonical_size: u64,
) -> OverlayResult<Bytes> {
    if offset >= canonical_size {
        return Ok(Bytes::from(vec![0u8; read_len]));
    }
    let canonical_len = std::cmp::min(read_len as u64, canonical_size - offset);
    let mut data = extent_store
        .read(id, offset, canonical_len)
        .await
        .map_err(OverlayError::from)?
        .to_vec();
    if data.len() < read_len {
        data.resize(read_len, 0);
    }
    Ok(Bytes::from(data))
}

fn overlay_fs_error(error: OverlayError) -> FsError {
    match error {
        OverlayError::NoSpace => FsError::NoSpace,
        OverlayError::InvalidArgument => FsError::InvalidArgument,
        OverlayError::IoError => FsError::IoError,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fs::mutation::config::{
        ClientDurabilityTarget, FilesystemWriteAckMode, FilesystemWriteAckSettings,
        FilesystemWriteAckSource,
    };
    use crate::fs::test_util::test_creds;
    use bytes::Bytes;
    use std::sync::Arc;

    fn volatile_settings() -> FilesystemWriteAckSettings {
        FilesystemWriteAckSettings {
            mode: FilesystemWriteAckMode::VolatileMemory,
            volatile_memory_bytes: 8 * 1024 * 1024,
            volatile_max_operations: 1024,
            source: FilesystemWriteAckSource::Filesystem,
            client_durability_target: ClientDurabilityTarget::LocalSsd,
        }
    }

    #[tokio::test]
    async fn write_ack_makes_data_and_size_visible() {
        let mut fs = ZeroFS::new_in_memory().await.unwrap();
        fs.write_ack = volatile_settings();
        let fs = Arc::new(fs);
        fs.install_volatile_overlay();
        assert!(fs.volatile_budget().is_some());

        let auth = crate::fs::types::AuthContext::from(&test_creds());
        let file = fs.create_exclusive(&auth, 0, b"overlay.txt").await.unwrap();
        let attrs = fs
            .write_ack(&auth, file, 0, &Bytes::from_static(b"hello-overlay"))
            .await
            .unwrap();
        assert_eq!(attrs.size, 13);

        let inode = fs.visible_inode(file).await.unwrap();
        match inode {
            Inode::File(file_inode) => assert_eq!(file_inode.size, 13),
            other => panic!("expected file, got {other:?}"),
        }

        let (data, eof) = fs.read_file(&auth, file, 0, 32).await.unwrap();
        assert_eq!(data.as_ref(), b"hello-overlay");
        assert!(eof);

        fs.wait_configured_durability().await.unwrap();
        let (data, eof) = fs.read_file(&auth, file, 0, 32).await.unwrap();
        assert_eq!(data.as_ref(), b"hello-overlay");
        assert!(eof);
    }

    #[tokio::test]
    async fn sequential_write_acks_extend_visible_size() {
        let mut fs = ZeroFS::new_in_memory().await.unwrap();
        fs.write_ack = volatile_settings();
        let fs = Arc::new(fs);
        fs.install_volatile_overlay();

        let auth = crate::fs::types::AuthContext::from(&test_creds());
        let file = fs.create_exclusive(&auth, 0, b"seq.txt").await.unwrap();
        fs.write_ack(&auth, file, 0, &Bytes::from_static(b"abc"))
            .await
            .unwrap();
        let attrs = fs
            .write_ack(&auth, file, 3, &Bytes::from_static(b"def"))
            .await
            .unwrap();
        assert_eq!(attrs.size, 6);

        let (data, eof) = fs.read_file(&auth, file, 0, 32).await.unwrap();
        assert_eq!(data.as_ref(), b"abcdef");
        assert!(eof);

        fs.wait_inode_durability(file).await.unwrap();
        let (data, eof) = fs.read_file(&auth, file, 0, 32).await.unwrap();
        assert_eq!(data.as_ref(), b"abcdef");
        assert!(eof);
    }
}
