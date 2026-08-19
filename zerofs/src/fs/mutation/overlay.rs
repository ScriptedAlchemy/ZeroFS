//! Filesystem-facing volatile overlay shared by NFS, 9P, WebUI, and NBD.
//!
//! One process-wide [`VolatileBudget`] bounds RAM. Each inode gets a single-lane
//! [`VolatileWriteRuntime`] so accepted writes are visible to later reads and
//! getattr before the canonical apply finishes. Protocol adapters call
//! [`ZeroFS::write_ack`] and [`ZeroFS::wait_configured_durability`].

// WIP on develop: landed but not fully wired into every protocol yet.
#![allow(dead_code)]

use super::admission::{PreparationAbort, PreparationGuard};
use super::overlay_dispatch::PendingDispatch;
use super::overlay_helpers::{direct_write_fingerprint, mutation_fs_error, overlay_fs_error};
use super::volatile_overlay::{
    Materializer, OverlayError, OverlayResult, VolatileAdmission, VolatileBudget,
    VolatileWriteRuntime, WriteChunk, WriteVisibility, record_published_staged_writes,
};
use crate::fs::ZeroFS;
use crate::fs::errors::FsError;
use crate::fs::inode::{Inode, InodeId};
use crate::fs::mutation::config::FilesystemWriteAckMode;
use crate::fs::mutation::request_cache::RequestLookup;
use crate::fs::mutation::types::{
    ConflictKey, ConflictScope, MutationCutoff, PrepareWriteMember, PrepareWriteRequest,
    PreparedWriteBatch, RequestFingerprint, RequestIdentity, RequestLifetime,
};
use crate::fs::ops::write::{apply_prepared_batch, prepare_write};
use crate::fs::types::{AuthContext, FileAttributes};
use bytes::Bytes;
use std::collections::{BTreeSet, HashMap, VecDeque};
#[cfg(test)]
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, Weak};
use tokio::sync::oneshot;

pub(crate) struct FilesystemVolatileOverlay {
    budget: Arc<VolatileBudget>,
    runtimes: Mutex<HashMap<u64, Arc<VolatileWriteRuntime>>>,
    pending: Mutex<HashMap<u64, VecDeque<Arc<PendingDispatch>>>>,
    latest_attrs: Mutex<HashMap<u64, VecDeque<VisibleAttrs>>>,
    frozen: AtomicBool,
    accepted_batches: AtomicU64,
    #[cfg(test)]
    fail_batch_after: AtomicUsize,
    #[cfg(test)]
    fail_publish: AtomicBool,
    #[cfg(test)]
    publish_pause: Mutex<Option<PublicationPause>>,
    fs: Weak<ZeroFS>,
}

struct VisibleAttrs {
    attrs: FileAttributes,
    visibility: Arc<WriteVisibility>,
}

pub(crate) struct WriteAckReceipt {
    pub(crate) attrs: FileAttributes,
    pub(crate) cutoff: MutationCutoff,
}

pub(crate) struct IdentifiedWrite<'a> {
    pub(crate) auth: &'a AuthContext,
    pub(crate) id: InodeId,
    pub(crate) offset: u64,
    pub(crate) data: &'a Bytes,
    pub(crate) op_id: crate::dedup::OpId,
    pub(crate) check_permissions: bool,
    pub(crate) identity: RequestIdentity,
    pub(crate) request_lifetime: RequestLifetime,
    pub(crate) fingerprint_context: &'a [u8],
}

struct StagedWriteAccept {
    inode: u64,
    admission: VolatileAdmission,
    offset: u64,
    data: Bytes,
    attrs: FileAttributes,
    guard: PreparationGuard,
    batch: PreparedWriteBatch,
    replay: Option<AcceptedWriteReplay>,
}

struct AcceptedWriteReplay {
    op_id: crate::dedup::OpId,
    fingerprint: [u8; 32],
    count: u32,
}

#[cfg(test)]
struct PublicationPause {
    reached: oneshot::Sender<()>,
    resume: oneshot::Receiver<()>,
}

impl FilesystemVolatileOverlay {
    pub(crate) fn new(max_bytes: u64, max_operations: usize, fs: Weak<ZeroFS>) -> Arc<Self> {
        Arc::new(Self {
            budget: VolatileBudget::new(max_bytes, max_operations),
            runtimes: Mutex::new(HashMap::new()),
            pending: Mutex::new(HashMap::new()),
            latest_attrs: Mutex::new(HashMap::new()),
            frozen: AtomicBool::new(false),
            accepted_batches: AtomicU64::new(0),
            #[cfg(test)]
            fail_batch_after: AtomicUsize::new(usize::MAX),
            #[cfg(test)]
            fail_publish: AtomicBool::new(false),
            #[cfg(test)]
            publish_pause: Mutex::new(None),
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
        let dispatch = {
            let mut pending = self.pending.lock().expect("filesystem overlay poisoned");
            match pending.get_mut(&inode).and_then(VecDeque::pop_front) {
                Some(dispatch) => dispatch,
                None => return Ok(()),
            }
        };
        let Some(fs) = self.fs.upgrade() else {
            return Err(OverlayError::IoError);
        };
        dispatch.arrive(fs).await
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
            .and_then(|attrs| {
                attrs
                    .iter()
                    .rev()
                    .find(|entry| entry.visibility.is_published())
            })
            .map(|entry| entry.attrs.size)
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
            .and_then(|attrs| {
                attrs
                    .iter()
                    .rev()
                    .find(|entry| entry.visibility.is_published())
            })
            .map(|entry| entry.attrs.clone())
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
            .entry(inode)
            .or_default()
            .push_back(VisibleAttrs {
                attrs,
                visibility: WriteVisibility::published(),
            });
    }

    pub(crate) fn retire_inode(&self, inode: u64) {
        // Pending batches are owned by the apply worker that popped them.
        // Only drop the preview attributes once canonical apply owns the bytes.
        let mut latest = self
            .latest_attrs
            .lock()
            .expect("filesystem overlay poisoned");
        if let Some(attrs) = latest.get_mut(&inode) {
            attrs.pop_front();
            if attrs.is_empty() {
                latest.remove(&inode);
            }
        }
    }

    pub(crate) fn freeze_terminal(&self) {
        self.frozen.store(true, Ordering::Release);
    }

    pub(crate) fn is_frozen(&self) -> bool {
        self.frozen.load(Ordering::Acquire)
    }

    async fn accept(self: &Arc<Self>, request: StagedWriteAccept) -> OverlayResult<MutationCutoff> {
        let StagedWriteAccept {
            inode,
            admission,
            offset,
            data,
            attrs,
            guard,
            batch,
            replay,
        } = request;
        if self.is_frozen() {
            return Err(OverlayError::IoError);
        }
        let visibility = WriteVisibility::staged();
        let length = data.len();
        let groups = vec![vec![WriteChunk {
            inode,
            member_offset: offset,
            logical_offset: 0,
            length,
        }]];
        let (accepted_tx, accepted_rx) = oneshot::channel();
        let dispatch = PendingDispatch::new(1, accepted_rx);
        {
            let mut pending = self.pending.lock().expect("filesystem overlay poisoned");
            pending
                .entry(inode)
                .or_default()
                .push_back(Arc::clone(&dispatch));
        }
        let runtime = self.runtime(inode);
        match runtime
            .accept_staged_write(admission, offset, data, groups, Arc::clone(&visibility))
            .await
        {
            Ok(sequence) => {
                #[cfg(test)]
                self.inject_publish_failure_if_requested();
                let accepted = match guard.publish(batch) {
                    Ok(accepted) => accepted,
                    Err(_) => {
                        drop(accepted_tx);
                        dispatch.cancel_unpublished();
                        runtime.wait_released(sequence).await;
                        return Err(OverlayError::IoError);
                    }
                };
                let cutoff = accepted.cutoff();
                self.latest_attrs
                    .lock()
                    .expect("filesystem overlay poisoned")
                    .entry(inode)
                    .or_default()
                    .push_back(VisibleAttrs {
                        attrs: attrs.clone(),
                        visibility: Arc::clone(&visibility),
                    });
                #[cfg(test)]
                self.pause_before_publish_if_requested().await;
                visibility.publish();
                record_published_staged_writes(length as u64, 1);
                if let Some(replay) = replay {
                    let fs = self.fs.upgrade().ok_or(OverlayError::IoError)?;
                    let accepted_write = fs.dedup.begin_accepted_write(
                        crate::dedup::DedupEntry {
                            op_id: replay.op_id,
                            result: crate::dedup::DedupResult::Write {
                                attrs: attrs.clone(),
                            },
                        },
                        replay.fingerprint,
                        replay.count,
                    );
                    dispatch.install_accepted_write(accepted_write);
                }
                if let Err(accepted) = accepted_tx.send(accepted) {
                    let (request, _batch, raw_permit, _cutoff) = accepted.into_parts();
                    if let Some(fs) = self.fs.upgrade()
                        && let Some(coordinator) = fs.mutation_coordinator.get()
                    {
                        coordinator.poison("overlay dispatch receiver dropped");
                        coordinator
                            .request_cache()
                            .complete(request, Err(FsError::IoError));
                    }
                    dispatch.cancel_unpublished();
                    drop(raw_permit);
                    return Err(OverlayError::IoError);
                }
                self.accepted_batches.fetch_add(1, Ordering::Relaxed);
                Ok(cutoff)
            }
            Err(error) => {
                let mut pending = self.pending.lock().expect("filesystem overlay poisoned");
                if let Some(queue) = pending.get_mut(&inode) {
                    queue.retain(|queued| !Arc::ptr_eq(queued, &dispatch));
                    if queue.is_empty() {
                        pending.remove(&inode);
                    }
                }
                let _ = guard.abort(PreparationAbort::RequestFailure(overlay_fs_error(error)));
                Err(error)
            }
        }
    }

    async fn rollback_unpublished(
        &self,
        dispatch: &Arc<PendingDispatch>,
        member_ids: &[InodeId],
        accepted: &[(Arc<VolatileWriteRuntime>, u64)],
    ) {
        dispatch.cancel_unpublished();
        {
            let mut pending = self.pending.lock().expect("filesystem overlay poisoned");
            for member_id in member_ids {
                if let Some(queue) = pending.get_mut(member_id) {
                    queue.retain(|queued| !Arc::ptr_eq(queued, dispatch));
                    if queue.is_empty() {
                        pending.remove(member_id);
                    }
                }
            }
        }
        for (runtime, sequence) in accepted {
            runtime.wait_released(*sequence).await;
        }
    }

    /// Publish every member of one logical write under a single pending batch.
    /// Visibility is accepted per inode; canonical apply happens once.
    pub(crate) async fn accept_batch(
        self: &Arc<Self>,
        admissions: Vec<VolatileAdmission>,
        guard: PreparationGuard,
        batch: PreparedWriteBatch,
    ) -> OverlayResult<MutationCutoff> {
        if self.is_frozen() {
            return Err(OverlayError::IoError);
        }
        if batch.members.is_empty() || admissions.len() != batch.members.len() {
            return Err(OverlayError::InvalidArgument);
        }
        let visibility = WriteVisibility::staged();
        let members: Vec<_> = batch
            .members
            .iter()
            .map(|member| {
                (
                    member.id,
                    member.offset,
                    member.data.clone(),
                    member.post_attrs.clone(),
                )
            })
            .collect();
        let member_ids = members.iter().map(|(id, _, _, _)| *id).collect::<Vec<_>>();
        let (accepted_tx, accepted_rx) = oneshot::channel();
        let dispatch = PendingDispatch::new(members.len(), accepted_rx);
        {
            let mut pending = self.pending.lock().expect("filesystem overlay poisoned");
            for (id, _, _, _) in &members {
                pending
                    .entry(*id)
                    .or_default()
                    .push_back(Arc::clone(&dispatch));
            }
        }
        let mut accepted_runtimes = Vec::with_capacity(members.len());
        for (admission, (id, offset, data, _attrs)) in
            admissions.into_iter().zip(members.iter().cloned())
        {
            let groups = vec![vec![WriteChunk {
                inode: id,
                member_offset: offset,
                logical_offset: 0,
                length: data.len(),
            }]];
            let runtime = self.runtime(id);
            match runtime
                .accept_staged_write(admission, offset, data, groups, Arc::clone(&visibility))
                .await
            {
                Ok(accepted) => {
                    accepted_runtimes.push((runtime, accepted));
                    #[cfg(test)]
                    if self.fail_batch_after.load(Ordering::Acquire) == accepted_runtimes.len() {
                        let error = OverlayError::IoError;
                        let _ =
                            guard.abort(PreparationAbort::RequestFailure(overlay_fs_error(error)));
                        drop(accepted_tx);
                        self.rollback_unpublished(&dispatch, &member_ids, &accepted_runtimes)
                            .await;
                        return Err(error);
                    }
                }
                Err(error) => {
                    let _ = guard.abort(PreparationAbort::RequestFailure(overlay_fs_error(error)));
                    drop(accepted_tx);
                    self.rollback_unpublished(&dispatch, &member_ids, &accepted_runtimes)
                        .await;
                    return Err(error);
                }
            }
        }
        #[cfg(test)]
        self.inject_publish_failure_if_requested();
        let accepted = match guard.publish(batch) {
            Ok(accepted) => accepted,
            Err(_) => {
                drop(accepted_tx);
                self.rollback_unpublished(&dispatch, &member_ids, &accepted_runtimes)
                    .await;
                return Err(OverlayError::IoError);
            }
        };
        let cutoff = accepted.cutoff();
        let unique_ids = member_ids.iter().copied().collect::<BTreeSet<_>>();
        {
            let mut latest = self
                .latest_attrs
                .lock()
                .expect("filesystem overlay poisoned");
            for member_id in &unique_ids {
                let attrs = members
                    .iter()
                    .rev()
                    .find(|(id, _, _, _)| id == member_id)
                    .expect("unique member id came from members")
                    .3
                    .clone();
                latest
                    .entry(*member_id)
                    .or_default()
                    .push_back(VisibleAttrs {
                        attrs,
                        visibility: Arc::clone(&visibility),
                    });
            }
        }
        #[cfg(test)]
        self.pause_before_publish_if_requested().await;
        visibility.publish();
        record_published_staged_writes(
            members
                .iter()
                .map(|(_, _, data, _)| data.len() as u64)
                .sum(),
            members.len() as u64,
        );
        if let Err(accepted) = accepted_tx.send(accepted) {
            let (request, _batch, raw_permit, _cutoff) = accepted.into_parts();
            if let Some(fs) = self.fs.upgrade()
                && let Some(coordinator) = fs.mutation_coordinator.get()
            {
                coordinator.poison("overlay dispatch receiver dropped");
                coordinator
                    .request_cache()
                    .complete(request, Err(FsError::IoError));
            }
            drop(raw_permit);
            return Err(OverlayError::IoError);
        }
        self.accepted_batches.fetch_add(1, Ordering::Relaxed);
        Ok(cutoff)
    }

    #[cfg(test)]
    pub(crate) fn accepted_batch_count(&self) -> u64 {
        self.accepted_batches.load(Ordering::Relaxed)
    }

    #[cfg(test)]
    pub(crate) fn fail_batch_after_for_test(&self, accepted_members: usize) {
        self.fail_batch_after
            .store(accepted_members, Ordering::Release);
    }

    #[cfg(test)]
    pub(crate) fn fail_publish_for_test(&self) {
        self.fail_publish.store(true, Ordering::Release);
    }

    #[cfg(test)]
    fn inject_publish_failure_if_requested(&self) {
        if self.fail_publish.swap(false, Ordering::AcqRel)
            && let Some(fs) = self.fs.upgrade()
            && let Some(coordinator) = fs.mutation_coordinator.get()
        {
            coordinator.poison("injected publication failure");
        }
    }

    #[cfg(test)]
    fn pause_publish_for_test(&self) -> (oneshot::Receiver<()>, oneshot::Sender<()>) {
        let (reached_tx, reached_rx) = oneshot::channel();
        let (resume_tx, resume_rx) = oneshot::channel();
        *self
            .publish_pause
            .lock()
            .expect("filesystem overlay poisoned") = Some(PublicationPause {
            reached: reached_tx,
            resume: resume_rx,
        });
        (reached_rx, resume_tx)
    }

    #[cfg(test)]
    async fn pause_before_publish_if_requested(&self) {
        let pause = self
            .publish_pause
            .lock()
            .expect("filesystem overlay poisoned")
            .take();
        if let Some(pause) = pause {
            let _ = pause.reached.send(());
            let _ = pause.resume.await;
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
        let coordinator = super::fence::MutationCoordinator::new_with_limits(
            super::admission::PreparationGate::new(materializer.incarnation()),
            materializer.progress(),
            self.write_ack.volatile_memory_bytes,
            self.write_ack.volatile_max_operations as u64,
        );
        let _ = self.materializer.set(materializer);
        let _ = self.mutation_coordinator.set(coordinator);
        let fs = Arc::downgrade(self);
        self.flush_coordinator
            .set_materialize(std::sync::Arc::new(move |cutoff| {
                let fs = fs.clone();
                Box::pin(async move {
                    let Some(fs) = fs.upgrade() else {
                        return Err(crate::fs::mutation::durability::DurabilityError::Closed);
                    };
                    fs.materialize_through_cutoff(cutoff)
                        .await
                        .map_err(crate::fs::mutation::durability::DurabilityError::Materialization)
                })
            }));
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
        self.write_ack_identified(IdentifiedWrite {
            auth,
            id,
            offset,
            data,
            op_id: [0; 16],
            check_permissions: true,
            identity: RequestIdentity::DirectOneShot(uuid::Uuid::new_v4()),
            request_lifetime: RequestLifetime::OneShot,
            fingerprint_context: &[],
        })
        .await
        .map(|receipt| receipt.attrs)
    }

    /// One prepared/accepted batch for a logical write that spans one or more
    /// backing inodes (NBD striped WRITE).
    pub(crate) async fn write_ack_batch(
        &self,
        auth: &AuthContext,
        members: Vec<PrepareWriteMember>,
    ) -> Result<FileAttributes, FsError> {
        if members.is_empty() {
            return Err(FsError::InvalidArgument);
        }
        if members.len() == 1 {
            let member = &members[0];
            return self
                .write_ack(auth, member.id, member.offset, &member.data)
                .await;
        }
        let request = PrepareWriteRequest {
            members,
            auth: auth.clone(),
            op_id: [0u8; 16],
            check_permissions: true,
        };
        if self.volatile_overlay.get().is_none() {
            let mut batch = prepare_write(&self.write_prepare_context(), request).await?;
            let result = apply_prepared_batch(&self.write_apply_context(), &mut batch).await?;
            return Ok(result.primary_attrs());
        }
        let coordinator = self.mutation_coordinator.get().ok_or(FsError::IoError)?;
        let request_cache = coordinator.request_cache();
        let pending = match request_cache
            .lookup_or_reserve(
                RequestIdentity::DirectOneShot(uuid::Uuid::new_v4()),
                RequestFingerprint::from_bytes([0; 32]),
                RequestLifetime::OneShot,
            )
            .map_err(|_| FsError::IoError)?
        {
            RequestLookup::Vacant(vacancy) => vacancy.begin_pending(),
            RequestLookup::Joined(retained) => return Ok(retained.wait().await?.primary_attrs()),
            RequestLookup::FingerprintMismatch => return Err(FsError::InvalidArgument),
            RequestLookup::Backpressured => return Err(FsError::RetryLater),
        };
        let raw_bytes = request.members.iter().try_fold(0_u64, |total, member| {
            total.checked_add(member.data.len() as u64)
        });
        let Some(raw_bytes) = raw_bytes else {
            pending.cancel();
            return Err(FsError::NoSpace);
        };
        let raw_permit = coordinator
            .raw_budget()
            .acquire(raw_bytes)
            .await
            .map_err(mutation_fs_error)?;
        let guard = PreparationGuard::new(
            coordinator.gate(),
            ConflictScope::new(
                request
                    .members
                    .iter()
                    .map(|member| ConflictKey::Inode(member.id)),
            ),
            raw_permit,
            pending,
        )
        .map_err(mutation_fs_error)?;
        self.write_ack_batch_admitted(request, guard)
            .await
            .map(|receipt| receipt.attrs)
    }

    /// Finish a logical write whose request identity, raw byte ownership, and
    /// preparation scope were acquired by the protocol before it copied the
    /// request body. This is the NBD handoff seam: it must not look up the
    /// request or acquire raw admission a second time.
    pub(crate) async fn write_ack_batch_admitted(
        &self,
        request: PrepareWriteRequest,
        guard: PreparationGuard,
    ) -> Result<WriteAckReceipt, FsError> {
        if request.members.is_empty() {
            let _ = guard.abort(PreparationAbort::RequestFailure(FsError::InvalidArgument));
            return Err(FsError::InvalidArgument);
        }
        let Some(overlay) = self.volatile_overlay.get().cloned() else {
            let _ = guard.abort(PreparationAbort::RequestFailure(FsError::IoError));
            return Err(FsError::IoError);
        };
        let mut admissions = Vec::with_capacity(request.members.len());
        for member in &request.members {
            match overlay.reserve(member.id, member.data.len()).await {
                Ok(admission) => admissions.push(admission),
                Err(error) => {
                    let fs_error = overlay_fs_error(error);
                    let _ = guard.abort(PreparationAbort::RequestFailure(fs_error));
                    return Err(fs_error);
                }
            }
        }
        let batch = match prepare_write(&self.write_prepare_context(), request).await {
            Ok(batch) => batch,
            Err(error) => {
                let _ = guard.abort(PreparationAbort::RequestFailure(error));
                return Err(error);
            }
        };
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
        let cutoff = overlay
            .accept_batch(admissions, guard, batch)
            .await
            .map_err(overlay_fs_error)?;
        Ok(WriteAckReceipt { attrs, cutoff })
    }

    pub(crate) async fn write_ack_opened_idempotent(
        &self,
        auth: &AuthContext,
        id: InodeId,
        offset: u64,
        data: &Bytes,
        op_id: crate::dedup::OpId,
    ) -> Result<FileAttributes, FsError> {
        self.write_ack_identified(IdentifiedWrite {
            auth,
            id,
            offset,
            data,
            op_id,
            check_permissions: false,
            identity: RequestIdentity::DirectOneShot(uuid::Uuid::new_v4()),
            request_lifetime: RequestLifetime::OneShot,
            fingerprint_context: &[],
        })
        .await
        .map(|receipt| receipt.attrs)
    }

    pub(crate) async fn write_ack_identified(
        &self,
        write: IdentifiedWrite<'_>,
    ) -> Result<WriteAckReceipt, FsError> {
        let IdentifiedWrite {
            auth,
            id,
            offset,
            data,
            op_id,
            check_permissions,
            identity,
            request_lifetime,
            fingerprint_context,
        } = write;
        let fingerprint = match &identity {
            RequestIdentity::DirectOneShot(_) => RequestFingerprint::from_bytes([0; 32]),
            _ => direct_write_fingerprint(
                auth,
                id,
                offset,
                data,
                op_id,
                check_permissions,
                fingerprint_context,
            ),
        };
        let replay_count = if matches!(&identity, RequestIdentity::NineP { .. }) {
            Some(u32::try_from(data.len()).map_err(|_| FsError::InvalidArgument)?)
        } else {
            None
        };
        let Some(overlay) = self.volatile_overlay.get().cloned() else {
            if matches!(&identity, RequestIdentity::Nfs { .. }) {
                return self
                    .write_materialized_nfs_identified(
                        IdentifiedWrite {
                            auth,
                            id,
                            offset,
                            data,
                            op_id,
                            check_permissions,
                            identity,
                            request_lifetime,
                            fingerprint_context,
                        },
                        fingerprint,
                    )
                    .await;
            }
            if replay_count.is_some()
                && matches!(
                    self.dedup.replay_write(&op_id, fingerprint.into_bytes()),
                    Some(crate::dedup::WriteReplay::FingerprintMismatch)
                )
            {
                return Err(FsError::InvalidArgument);
            }
            let pending_write = replay_count.map(|count| {
                self.dedup
                    .stage_write_request(op_id, fingerprint.into_bytes(), count)
            });
            let attrs = if let Some(pending_write) = pending_write {
                self.write_materialized_identified(
                    PrepareWriteRequest {
                        members: vec![PrepareWriteMember {
                            id,
                            offset,
                            data: data.clone(),
                        }],
                        auth: auth.clone(),
                        op_id,
                        check_permissions,
                    },
                    pending_write,
                )
                .await
            } else if check_permissions {
                self.write_idempotent(auth, id, offset, data, op_id).await
            } else {
                self.write_opened_idempotent(auth, id, offset, data, op_id)
                    .await
            }?;
            if let Some(count) = replay_count {
                self.dedup.record_materialized_write_entry(
                    crate::dedup::DedupEntry {
                        op_id,
                        result: crate::dedup::DedupResult::Write {
                            attrs: attrs.clone(),
                        },
                    },
                    fingerprint.into_bytes(),
                    count,
                );
            }
            return Ok(WriteAckReceipt {
                attrs,
                cutoff: self.capture_mutation_cutoff(),
            });
        };

        let coordinator = self.mutation_coordinator.get().ok_or(FsError::IoError)?;
        let request_cache = coordinator.request_cache();
        let pending = match request_cache
            .lookup_or_reserve(identity, fingerprint, request_lifetime)
            .map_err(|_| FsError::IoError)?
        {
            RequestLookup::Vacant(vacancy) => vacancy.begin_pending(),
            RequestLookup::Joined(retained) => {
                let result = retained.wait().await?;
                return Ok(WriteAckReceipt {
                    attrs: result.primary_attrs(),
                    cutoff: result.cutoff.ok_or(FsError::IoError)?,
                });
            }
            RequestLookup::FingerprintMismatch => return Err(FsError::InvalidArgument),
            RequestLookup::Backpressured => return Err(FsError::RetryLater),
        };
        let raw_permit = coordinator
            .raw_budget()
            .acquire(data.len() as u64)
            .await
            .map_err(mutation_fs_error)?;
        let guard = PreparationGuard::new(
            coordinator.gate(),
            ConflictScope::single(ConflictKey::Inode(id)),
            raw_permit,
            pending,
        )
        .map_err(mutation_fs_error)?;
        let admission = match overlay.reserve(id, data.len()).await {
            Ok(admission) => admission,
            Err(error) => {
                let fs_error = overlay_fs_error(error);
                let _ = guard.abort(PreparationAbort::RequestFailure(fs_error));
                return Err(fs_error);
            }
        };
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
        let batch = match prepare_write(&self.write_prepare_context(), request).await {
            Ok(batch) => batch,
            Err(error) => {
                let _ = guard.abort(PreparationAbort::RequestFailure(error));
                return Err(error);
            }
        };
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
        let cutoff = overlay
            .accept(StagedWriteAccept {
                inode: id,
                admission,
                offset,
                data: data.clone(),
                attrs: attrs.clone(),
                guard,
                batch,
                replay: replay_count.map(|count| AcceptedWriteReplay {
                    op_id,
                    fingerprint: fingerprint.into_bytes(),
                    count,
                }),
            })
            .await
            .map_err(overlay_fs_error)?;
        Ok(WriteAckReceipt { attrs, cutoff })
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
    use tokio::sync::Notify;

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
    async fn identified_write_replay_shares_the_original_cutoff_and_fingerprint() {
        let mut fs = ZeroFS::new_in_memory().await.unwrap();
        fs.write_ack = volatile_settings();
        let fs = Arc::new(fs);
        fs.install_volatile_overlay();
        let auth = crate::fs::types::AuthContext::from(&test_creds());
        let file = fs
            .create_exclusive(&auth, 0, b"identified.txt")
            .await
            .unwrap();
        let identity = RequestIdentity::DirectTagged {
            caller_incarnation: uuid::Uuid::new_v4(),
            operation_id: 17,
        };
        let (reached, resume) = fs
            .volatile_overlay
            .get()
            .expect("overlay")
            .pause_publish_for_test();

        let first = tokio::spawn({
            let fs = Arc::clone(&fs);
            let auth = auth.clone();
            let identity = identity.clone();
            async move {
                let data = Bytes::from_static(b"same-payload");
                fs.write_ack_identified(IdentifiedWrite {
                    auth: &auth,
                    id: file,
                    offset: 0,
                    data: &data,
                    op_id: [3; 16],
                    check_permissions: true,
                    identity,
                    request_lifetime: RequestLifetime::CanonicalDedup,
                    fingerprint_context: b"local-ssd-stable",
                })
                .await
            }
        });
        reached.await.expect("first write reached publication");

        let mismatch_data = Bytes::from_static(b"other-payload");
        let mismatch = fs
            .write_ack_identified(IdentifiedWrite {
                auth: &auth,
                id: file,
                offset: 0,
                data: &mismatch_data,
                op_id: [3; 16],
                check_permissions: true,
                identity: identity.clone(),
                request_lifetime: RequestLifetime::CanonicalDedup,
                fingerprint_context: b"local-ssd-stable",
            })
            .await;
        assert!(matches!(mismatch, Err(FsError::InvalidArgument)));

        let joined = tokio::spawn({
            let fs = Arc::clone(&fs);
            let auth = auth.clone();
            let identity = identity.clone();
            async move {
                let data = Bytes::from_static(b"same-payload");
                fs.write_ack_identified(IdentifiedWrite {
                    auth: &auth,
                    id: file,
                    offset: 0,
                    data: &data,
                    op_id: [3; 16],
                    check_permissions: true,
                    identity,
                    request_lifetime: RequestLifetime::CanonicalDedup,
                    fingerprint_context: b"local-ssd-stable",
                })
                .await
            }
        });
        resume.send(()).expect("resume first publication");

        let first = first.await.unwrap().unwrap();
        let joined = joined.await.unwrap().unwrap();
        assert_eq!(joined.attrs.size, first.attrs.size);
        assert_eq!(joined.cutoff, first.cutoff);

        fs.wait_configured_durability().await.unwrap();
        let retained_data = Bytes::from_static(b"same-payload");
        let retained = fs
            .write_ack_identified(IdentifiedWrite {
                auth: &auth,
                id: file,
                offset: 0,
                data: &retained_data,
                op_id: [3; 16],
                check_permissions: true,
                identity,
                request_lifetime: RequestLifetime::CanonicalDedup,
                fingerprint_context: b"local-ssd-stable",
            })
            .await
            .unwrap();
        assert_eq!(retained.attrs.size, first.attrs.size);
        assert_eq!(retained.cutoff, first.cutoff);
    }

    #[tokio::test]
    async fn volatile_identified_write_publishes_typed_dedup_before_materialization() {
        let mut fs = ZeroFS::new_in_memory().await.unwrap();
        fs.write_ack = volatile_settings();
        let fs = Arc::new(fs);
        let auth = crate::fs::types::AuthContext::from(&test_creds());
        let file = fs
            .create_exclusive(&auth, 0, b"typed-dedup.txt")
            .await
            .unwrap();

        let overlay = FilesystemVolatileOverlay::new(
            fs.write_ack.volatile_memory_bytes,
            fs.write_ack.volatile_max_operations,
            Arc::downgrade(&fs),
        );
        assert!(fs.volatile_overlay.set(Arc::clone(&overlay)).is_ok());
        let entered = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let hook: super::super::materializer::ApplyHook = {
            let entered = Arc::clone(&entered);
            let release = Arc::clone(&release);
            let fs = Arc::downgrade(&fs);
            Arc::new(move |_cutoff, mut batch| {
                let entered = Arc::clone(&entered);
                let release = Arc::clone(&release);
                let fs = fs.clone();
                Box::pin(async move {
                    entered.notify_one();
                    release.notified().await;
                    let fs = fs
                        .upgrade()
                        .ok_or(super::super::types::MutationError::Closed)?;
                    apply_prepared_batch(&fs.write_apply_context(), &mut batch)
                        .await
                        .map(|_| ())
                        .map_err(|error| {
                            super::super::types::MutationError::Poisoned(error.to_string())
                        })
                })
            })
        };
        let materializer = super::super::materializer::Materializer::start_with_hook(
            super::super::types::MutationIncarnation::new(),
            Arc::downgrade(&fs),
            Some(overlay),
            Some(hook),
        );
        let coordinator = super::super::fence::MutationCoordinator::new_with_limits(
            super::super::admission::PreparationGate::new(materializer.incarnation()),
            materializer.progress(),
            fs.write_ack.volatile_memory_bytes,
            fs.write_ack.volatile_max_operations as u64,
        );
        assert!(fs.materializer.set(Arc::clone(&materializer)).is_ok());
        assert!(fs.mutation_coordinator.set(coordinator).is_ok());

        let op_id = [9; 16];
        let identity = RequestIdentity::NineP {
            origin_epoch: 91,
            operation_id: op_id,
        };
        let data = Bytes::from_static(b"accepted-before-durable");
        let first = fs
            .write_ack_identified(IdentifiedWrite {
                auth: &auth,
                id: file,
                offset: 0,
                data: &data,
                op_id,
                check_permissions: true,
                identity: identity.clone(),
                request_lifetime: RequestLifetime::CanonicalDedup,
                fingerprint_context: b"9p-write",
            })
            .await
            .unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(2), entered.notified())
            .await
            .unwrap();

        let accepted = fs.dedup.get(&op_id).expect("accepted result is published");
        let crate::dedup::DedupResult::Write { attrs: accepted } = accepted else {
            panic!("accepted dedup result must retain typed write attributes")
        };
        assert_eq!(accepted.fileid, first.attrs.fileid);
        assert_eq!(accepted.size, first.attrs.size);
        let fingerprint = direct_write_fingerprint(&auth, file, 0, &data, op_id, true, b"9p-write");
        assert_eq!(
            fs.dedup.replay_write(&op_id, fingerprint.into_bytes()),
            Some(crate::dedup::WriteReplay::Match {
                count: data.len() as u32,
            })
        );
        assert_eq!(materializer.progress().materialized_through(), 0);

        release.notify_one();
        materializer
            .progress()
            .wait_materialized(first.cutoff)
            .await
            .unwrap();
        let (canonical, eof) = fs.read_file(&auth, file, 0, 64).await.unwrap();
        assert_eq!(canonical, data);
        assert!(eof);

        let replay = fs
            .write_ack_identified(IdentifiedWrite {
                auth: &auth,
                id: file,
                offset: 0,
                data: &data,
                op_id,
                check_permissions: true,
                identity,
                request_lifetime: RequestLifetime::CanonicalDedup,
                fingerprint_context: b"9p-write",
            })
            .await
            .unwrap();
        assert_eq!(replay.attrs.fileid, first.attrs.fileid);
        assert_eq!(replay.attrs.size, first.attrs.size);
        assert_eq!(replay.cutoff, first.cutoff);

        let Some(crate::dedup::DedupResult::Write { attrs: durable }) = fs.dedup.get(&op_id) else {
            panic!("materialized dedup result must remain a typed write")
        };
        assert_eq!(durable.fileid, first.attrs.fileid);
        assert_eq!(durable.size, first.attrs.size);
        materializer.stop().await;
    }

    #[tokio::test]
    async fn real_write_ack_enters_preparation_gate() {
        let mut fs = ZeroFS::new_in_memory().await.unwrap();
        fs.write_ack = volatile_settings();
        let fs = Arc::new(fs);
        fs.install_volatile_overlay();

        let auth = crate::fs::types::AuthContext::from(&test_creds());
        let file = fs.create_exclusive(&auth, 0, b"gated.txt").await.unwrap();
        fs.write_ack(&auth, file, 0, &Bytes::from_static(b"gated"))
            .await
            .unwrap();

        let coordinator = fs.mutation_coordinator.get().expect("coordinator");
        assert_eq!(coordinator.gate().published_through(), 1);
        fs.wait_configured_durability().await.unwrap();
        assert_eq!(coordinator.raw_budget().used_bytes(), 0);
        assert_eq!(coordinator.raw_budget().used_operations(), 0);
        assert_eq!(coordinator.request_cache().used_slots(), 0);
    }

    #[tokio::test]
    async fn request_cache_pressure_is_retry_later_even_for_zero_length_write() {
        let mut fs = ZeroFS::new_in_memory().await.unwrap();
        fs.write_ack = FilesystemWriteAckSettings {
            volatile_max_operations: 1,
            ..volatile_settings()
        };
        let fs = Arc::new(fs);
        fs.install_volatile_overlay();

        let auth = crate::fs::types::AuthContext::from(&test_creds());
        let file = fs
            .create_exclusive(&auth, 0, b"cache-pressure.txt")
            .await
            .unwrap();
        let coordinator = fs.mutation_coordinator.get().expect("coordinator");
        let held = match coordinator
            .request_cache()
            .lookup_or_reserve(
                RequestIdentity::Nbd {
                    connection_incarnation: 7,
                    handle: 1,
                },
                RequestFingerprint::from_parts(&[b"held"]),
                RequestLifetime::InFlightOnly,
            )
            .unwrap()
        {
            RequestLookup::Vacant(vacancy) => vacancy.begin_pending(),
            other => panic!("expected vacant request slot, got {other:?}"),
        };

        for (xid, data) in [(11, Bytes::new()), (12, Bytes::from_static(b"data"))] {
            let result = fs
                .write_ack_identified(IdentifiedWrite {
                    auth: &auth,
                    id: file,
                    offset: 0,
                    data: &data,
                    op_id: [0; 16],
                    check_permissions: true,
                    identity: RequestIdentity::Nfs {
                        server_incarnation: uuid::Uuid::nil(),
                        connection_incarnation: 3,
                        xid,
                    },
                    request_lifetime: RequestLifetime::InFlightOnly,
                    fingerprint_context: b"nfs-write",
                })
                .await;
            assert!(matches!(result, Err(FsError::RetryLater)));
        }
        assert_eq!(coordinator.raw_budget().used_bytes(), 0);
        assert_eq!(coordinator.gate().active_guards(), 0);
        held.cancel();
    }

    #[tokio::test]
    async fn real_striped_write_publishes_one_preparation_sequence() {
        let mut fs = ZeroFS::new_in_memory().await.unwrap();
        fs.write_ack = volatile_settings();
        let fs = Arc::new(fs);
        fs.install_volatile_overlay();

        let auth = crate::fs::types::AuthContext::from(&test_creds());
        let first = fs.create_exclusive(&auth, 0, b"stripe-a").await.unwrap();
        let second = fs.create_exclusive(&auth, 0, b"stripe-b").await.unwrap();
        fs.write_ack_batch(
            &auth,
            vec![
                PrepareWriteMember {
                    id: first,
                    offset: 0,
                    data: Bytes::from_static(b"first"),
                },
                PrepareWriteMember {
                    id: second,
                    offset: 0,
                    data: Bytes::from_static(b"second"),
                },
            ],
        )
        .await
        .unwrap();

        let coordinator = fs.mutation_coordinator.get().expect("coordinator");
        assert_eq!(coordinator.gate().published_through(), 1);
        fs.wait_configured_durability().await.unwrap();
        assert_eq!(coordinator.progress().materialized_through(), 1);
        assert_eq!(coordinator.raw_budget().used_bytes(), 0);
        assert_eq!(coordinator.request_cache().used_slots(), 0);
    }

    #[tokio::test]
    async fn failed_preparation_releases_shared_coordinator_ownership() {
        let mut fs = ZeroFS::new_in_memory().await.unwrap();
        fs.write_ack = volatile_settings();
        let fs = Arc::new(fs);
        fs.install_volatile_overlay();

        let auth = crate::fs::types::AuthContext::from(&test_creds());
        assert!(matches!(
            fs.write_ack(&auth, 0, 0, &Bytes::from_static(b"not-a-file"))
                .await,
            Err(FsError::IsDirectory)
        ));

        let coordinator = fs.mutation_coordinator.get().expect("coordinator");
        assert_eq!(coordinator.gate().active_guards(), 0);
        assert_eq!(coordinator.gate().published_through(), 0);
        assert_eq!(coordinator.raw_budget().used_bytes(), 0);
        assert_eq!(coordinator.raw_budget().used_operations(), 0);
        assert_eq!(coordinator.request_cache().used_slots(), 0);
    }

    #[tokio::test]
    async fn partial_batch_acceptance_rolls_back_without_stuck_lane() {
        let mut fs = ZeroFS::new_in_memory().await.unwrap();
        fs.write_ack = volatile_settings();
        let fs = Arc::new(fs);
        fs.install_volatile_overlay();

        let auth = crate::fs::types::AuthContext::from(&test_creds());
        let first = fs.create_exclusive(&auth, 0, b"rollback-a").await.unwrap();
        let second = fs.create_exclusive(&auth, 0, b"rollback-b").await.unwrap();
        let overlay = fs.volatile_overlay.get().expect("overlay");
        overlay.fail_batch_after_for_test(1);

        let result = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            fs.write_ack_batch(
                &auth,
                vec![
                    PrepareWriteMember {
                        id: first,
                        offset: 0,
                        data: Bytes::from_static(b"partial"),
                    },
                    PrepareWriteMember {
                        id: second,
                        offset: 0,
                        data: Bytes::from_static(b"never"),
                    },
                ],
            ),
        )
        .await
        .expect("failed batch left an inode lane parked");
        assert!(matches!(result, Err(FsError::IoError)));

        let coordinator = fs.mutation_coordinator.get().expect("coordinator");
        assert_eq!(coordinator.gate().active_guards(), 0);
        assert_eq!(coordinator.gate().published_through(), 0);
        assert_eq!(coordinator.raw_budget().used_bytes(), 0);
        assert_eq!(coordinator.request_cache().used_slots(), 0);
        let status = overlay.budget().status();
        assert_eq!(status.dirty_bytes, 0);
        assert_eq!(status.dirty_operations, 0);

        let (data, eof) = fs.read_file(&auth, first, 0, 32).await.unwrap();
        assert!(data.is_empty());
        assert!(eof);
    }

    #[tokio::test]
    async fn publish_failure_cancels_taken_dispatch_and_releases_ownership() {
        let mut fs = ZeroFS::new_in_memory().await.unwrap();
        fs.write_ack = volatile_settings();
        let fs = Arc::new(fs);
        fs.install_volatile_overlay();

        let auth = crate::fs::types::AuthContext::from(&test_creds());
        let file = fs
            .create_exclusive(&auth, 0, b"publish-failure")
            .await
            .unwrap();
        let overlay = fs.volatile_overlay.get().expect("overlay");
        overlay.fail_publish_for_test();

        let result = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            fs.write_ack(&auth, file, 0, &Bytes::from_static(b"never-visible")),
        )
        .await
        .expect("publish failure left the runtime waiting on its accepted sender");
        assert!(matches!(result, Err(FsError::IoError)));
        assert_eq!(overlay.budget().status().dirty_bytes, 0);
        assert_eq!(overlay.budget().status().dirty_operations, 0);
        let coordinator = fs.mutation_coordinator.get().expect("coordinator");
        assert_eq!(coordinator.gate().active_guards(), 0);
        assert_eq!(coordinator.request_cache().used_slots(), 0);
    }

    #[tokio::test]
    async fn slow_read_on_one_inode_does_not_block_unrelated_write_ack() {
        let mut fs = ZeroFS::new_in_memory().await.unwrap();
        fs.write_ack = volatile_settings();
        let fs = Arc::new(fs);
        fs.install_volatile_overlay();

        let auth = crate::fs::types::AuthContext::from(&test_creds());
        let read_file = fs.create_exclusive(&auth, 0, b"slow-read").await.unwrap();
        let write_file = fs
            .create_exclusive(&auth, 0, b"unrelated-write")
            .await
            .unwrap();
        let overlay = fs.volatile_overlay.get().expect("overlay").clone();
        let (started_tx, started_rx) = oneshot::channel();
        let release = Arc::new(Notify::new());
        let read_task = tokio::spawn({
            let release = Arc::clone(&release);
            async move {
                overlay
                    .read(read_file, 0, 1, move || {
                        Box::pin(async move {
                            let _ = started_tx.send(());
                            release.notified().await;
                            Ok(Bytes::from_static(b"\0"))
                        })
                    })
                    .await
            }
        });
        started_rx.await.unwrap();

        tokio::time::timeout(
            std::time::Duration::from_secs(1),
            fs.write_ack(&auth, write_file, 0, &Bytes::from_static(b"write")),
        )
        .await
        .expect("unrelated write ACK waited behind a slow read")
        .unwrap();

        release.notify_waiters();
        assert_eq!(read_task.await.unwrap().unwrap(), Bytes::from_static(b"\0"));
    }

    #[tokio::test]
    async fn metadata_and_data_publish_through_one_visibility_token() {
        let mut fs = ZeroFS::new_in_memory().await.unwrap();
        fs.write_ack = volatile_settings();
        let fs = Arc::new(fs);
        fs.install_volatile_overlay();

        let auth = crate::fs::types::AuthContext::from(&test_creds());
        let file = fs
            .create_exclusive(&auth, 0, b"atomic-publication")
            .await
            .unwrap();
        let overlay = fs.volatile_overlay.get().expect("overlay");
        let (reached, resume) = overlay.pause_publish_for_test();
        let write = tokio::spawn({
            let fs = Arc::clone(&fs);
            let auth = auth.clone();
            async move {
                fs.write_ack(&auth, file, 0, &Bytes::from_static(b"published"))
                    .await
            }
        });
        reached.await.unwrap();

        let (before, eof) = fs.read_file(&auth, file, 0, 32).await.unwrap();
        assert!(before.is_empty());
        assert!(eof);

        resume.send(()).unwrap();
        write.await.unwrap().unwrap();
        let (after, eof) = fs.read_file(&auth, file, 0, 32).await.unwrap();
        assert_eq!(after.as_ref(), b"published");
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
