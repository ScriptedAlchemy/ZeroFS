use crate::segment_store::{
    AuthoritativeSegmentRead, ConditionalMultipartCreate, GeneratedSegmentCreate,
};
use crate::writeback::admission::Admission;
use crate::writeback::config::{AckMode, WritebackSettings};
use crate::writeback::journal::Journal;
use crate::writeback::journaler::{LocalBarrier, LocalBarrierError, LocalJournaler};
use crate::writeback::model::{
    LocalEtag, MutationKind, MutationMode, MutationRecord, WritebackStatus, classify_mutation_fence,
};
use crate::writeback::multipart_reservation::{
    CleanedMultipartStaging, MultipartReservationSet, MultipartStagingCleanup, MutationReservation,
    RamMultipartPartReservation, SsdMultipartPartReservation, promote_multipart,
    remove_aborted_ssd_multipart,
};
use crate::writeback::overlay::{OverlayCommitObserver, OverlayIndex, VisibleVersion};
use crate::writeback::payload::VerifiedPayload;
use crate::writeback::remote::{RemoteBarrierError, RemoteScheduler};
use crate::writeback::reservation::{
    ReservationError, SsdAdmission, SsdReservationRequest, SsdReservationToken,
};
use crate::writeback::space_refresher::SpaceRefresher;
use crate::writeback::space_sample::{PhysicalSpaceSample, PhysicalSpaceSampler};
#[cfg(test)]
use crate::writeback::test_util::WriteAdmissionTestControl;
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
#[cfg(not(unix))]
use std::io::Write;
#[cfg(unix)]
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
use std::path::{Path as FilePath, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex as StdMutex};
use tokio::sync::{Mutex, Notify, OwnedMutexGuard};
use uuid::Uuid;

/// Object-writeback durability failure. Local and remote terminals stay distinct.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum WritebackError {
    #[error(transparent)]
    Local(#[from] LocalBarrierError),
    #[error(transparent)]
    Remote(#[from] RemoteBarrierError),
}

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
    space: Arc<PhysicalSpaceSampler>,
    ssd: Arc<SsdAdmission>,
    space_refresher: Arc<SpaceRefresher>,
    journaler: LocalJournaler,
    remote: RemoteScheduler,
    database_prefix: String,
    incarnation: Uuid,
    next_sequence: AtomicU64,
    key_locks: Vec<Arc<Mutex<()>>>,
    admission_order: Mutex<()>,
    stopped: AtomicBool,
    #[cfg(test)]
    journal_submit_pause: StdMutex<Option<Arc<JournalSubmitPause>>>,
}

#[cfg(test)]
#[derive(Default)]
struct JournalSubmitPause {
    entered: Notify,
    release: Notify,
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
    #[cfg(test)]
    fn pause_next_journal_submit(&self) -> Arc<JournalSubmitPause> {
        let pause = Arc::new(JournalSubmitPause::default());
        let replaced = self
            .inner
            .journal_submit_pause
            .lock()
            .unwrap()
            .replace(Arc::clone(&pause));
        assert!(replaced.is_none(), "journal-submit pause already armed");
        pause
    }

    /// Test-only convenience: production always opens through
    /// `open_paused_with_owners` (see `bootstrap.rs`), which supplies its own
    /// space/SSD owners and starts with the remote view paused.
    #[cfg(test)]
    async fn open(
        remote: Arc<dyn ObjectStore>,
        journal: Arc<Journal>,
        settings: WritebackSettings,
    ) -> anyhow::Result<Self> {
        Self::open_with_remote_state(remote, journal, settings, true, None).await
    }

    /// Open a recovered overlay without allowing the remote view to advance.
    // Landed-but-not-wired constructor variant.
    #[allow(dead_code)]
    pub(crate) async fn open_paused(
        remote: Arc<dyn ObjectStore>,
        journal: Arc<Journal>,
        settings: WritebackSettings,
    ) -> anyhow::Result<Self> {
        Self::open_with_remote_state(remote, journal, settings, false, None).await
    }

    pub(crate) async fn open_paused_with_owners(
        remote: Arc<dyn ObjectStore>,
        journal: Arc<Journal>,
        settings: WritebackSettings,
        space: Arc<PhysicalSpaceSampler>,
        ssd: Arc<SsdAdmission>,
    ) -> anyhow::Result<Self> {
        Self::open_with_remote_state(remote, journal, settings, false, Some((space, ssd))).await
    }

    async fn open_with_remote_state(
        remote: Arc<dyn ObjectStore>,
        journal: Arc<Journal>,
        settings: WritebackSettings,
        remote_active: bool,
        owners: Option<(Arc<PhysicalSpaceSampler>, Arc<SsdAdmission>)>,
    ) -> anyhow::Result<Self> {
        if settings.memory_bytes == 0 {
            anyhow::bail!("writeback requires a positive independent dirty RAM budget");
        }
        let snapshot = journal.snapshot()?;
        let database_prefix = snapshot.identity.database_prefix.clone();
        let (space, ssd) = match owners {
            Some((space, ssd)) => (space, ssd),
            None => {
                let space = Arc::new(PhysicalSpaceSampler::new(settings.dir.clone()));
                let sample = space.sample().await?;
                let pending = journal.pending_ssd_reservations()?;
                let ssd = Arc::new(SsdAdmission::recover(
                    settings.disk_bytes,
                    1 << 20,
                    settings.high_watermark_percent,
                    settings.resume_percent,
                    settings.min_free_bytes,
                    pending,
                    Some(sample),
                )?);
                (space, ssd)
            }
        };
        let admission = Admission::new(settings.memory_bytes);
        let overlay = OverlayIndex::recover(remote.clone(), journal.clone()).await?;
        let observer = Arc::new(OverlayCommitObserver::new(overlay.clone(), journal.clone()));
        let queue_depth = settings
            .upload_concurrency
            .max(settings.local_concurrency)
            .saturating_mul(4)
            .max(16);
        let journaler = LocalJournaler::start_with_observer_and_space(
            journal.clone(),
            admission.clone(),
            queue_depth,
            settings.local_concurrency,
            Some(observer),
            Arc::clone(&space),
        )?;
        let remote = if remote_active {
            RemoteScheduler::start(
                remote,
                journal.clone(),
                overlay.clone(),
                admission.clone(),
                Arc::clone(&ssd),
                Arc::clone(&space),
                journaler.barrier(),
                settings.upload_concurrency,
            )?
        } else {
            RemoteScheduler::start_paused(
                remote,
                journal.clone(),
                overlay.clone(),
                admission.clone(),
                Arc::clone(&ssd),
                Arc::clone(&space),
                journaler.barrier(),
                settings.upload_concurrency,
            )?
        };
        Ok(Self {
            inner: Arc::new(WritebackStoreInner {
                journal,
                settings,
                overlay,
                admission,
                space: Arc::clone(&space),
                ssd: Arc::clone(&ssd),
                space_refresher: SpaceRefresher::start(Arc::clone(&ssd), Arc::clone(&space)),
                journaler,
                remote,
                database_prefix,
                incarnation: snapshot.incarnation,
                next_sequence: AtomicU64::new(snapshot.local_seq),
                key_locks: (0..KEY_LOCK_SHARDS)
                    .map(|_| Arc::new(Mutex::new(())))
                    .collect(),
                admission_order: Mutex::new(()),
                stopped: AtomicBool::new(false),
                #[cfg(test)]
                journal_submit_pause: StdMutex::new(None),
            }),
        })
    }

    pub(crate) async fn wait_local(&self, sequence: u64) -> Result<(), LocalBarrierError> {
        self.inner.journaler.barrier().wait_local(sequence).await
    }

    async fn reconcile_local_wait_error(
        &self,
        sequence: u64,
        error: LocalBarrierError,
    ) -> object_store::Error {
        let journal = Arc::clone(&self.inner.journal);
        let durable = tokio::task::spawn_blocking(move || {
            journal
                .snapshot()
                .map(|snapshot| snapshot.local_seq >= sequence)
        })
        .await
        .map_err(|join_error| anyhow::anyhow!("journal snapshot task failed: {join_error}"))
        .and_then(|result| result);
        let reconciliation = match durable {
            Ok(true) => {
                self.inner
                    .overlay
                    .mark_local(sequence, Arc::clone(&self.inner.journal))
                    .await
            }
            Ok(false) => {
                self.inner.overlay.remove_sequence(sequence).await;
                Ok(())
            }
            Err(snapshot_error) => Err(snapshot_error),
        };
        match reconciliation {
            Ok(()) => generic_error(format!("local durability failed: {error}")),
            Err(reconcile_error) => generic_error(format!(
                "local durability failed: {error}; overlay reconciliation failed: {reconcile_error:#}"
            )),
        }
    }

    /// Hold the caller until the configured acknowledgement tier covers this
    /// sequence. `AckMode::Memory` acknowledges as soon as the overlay has the
    /// mutation, so it waits for nothing here.
    async fn await_ack(&self, barrier: &LocalBarrier, sequence: u64) -> object_store::Result<()> {
        if self.inner.settings.ack_mode == AckMode::Ssd {
            if let Err(error) = barrier.wait_local(sequence).await {
                return Err(self.reconcile_local_wait_error(sequence, error).await);
            }
        } else if self.inner.settings.ack_mode == AckMode::Remote {
            self.wait_remote(sequence)
                .await
                .map_err(|error| generic_error(format!("remote durability failed: {error}")))?;
        }
        Ok(())
    }

    /// Capture every mutation accepted before this barrier and wait until the
    /// contiguous local SSD journal covers that sequence.
    pub(crate) async fn wait_local_through_accepted(&self) -> Result<(), LocalBarrierError> {
        let target = {
            let _order_guard = self.inner.admission_order.lock().await;
            self.inner.next_sequence.load(Ordering::Acquire)
        };
        self.wait_local(target).await
    }

    pub(crate) async fn wait_remote(&self, sequence: u64) -> Result<(), RemoteBarrierError> {
        self.inner.remote.barrier().wait_remote(sequence).await
    }

    pub(crate) fn journal_incarnation(&self) -> uuid::Uuid {
        self.inner.incarnation
    }

    /// Conservative newest accepted sequence. Callers capture this while the
    /// filesystem flush barrier is held so later object mutations cannot commit
    /// without being included.
    pub(crate) fn accepted_sequence(&self) -> crate::writeback::model::Sequence {
        self.inner.next_sequence.load(Ordering::Acquire)
    }

    /// Conservative object coverage after database close, including objects
    /// emitted by close itself.
    pub(crate) fn object_coverage(&self) -> crate::fs::mutation::durability::ObjectCoverage {
        crate::fs::mutation::durability::ObjectCoverage::Writeback {
            journal_incarnation: crate::fs::mutation::durability::JournalIncarnation::new(
                self.journal_incarnation(),
            ),
            sequence: self.accepted_sequence(),
        }
    }

    pub(crate) async fn wait_local_coverage(
        &self,
        journal_incarnation: uuid::Uuid,
        sequence: crate::writeback::model::Sequence,
    ) -> Result<(), LocalBarrierError> {
        if journal_incarnation != self.inner.incarnation {
            return Err(LocalBarrierError::StaleIncarnation);
        }
        self.wait_local(sequence).await
    }

    pub(crate) async fn wait_remote_coverage(
        &self,
        journal_incarnation: uuid::Uuid,
        sequence: crate::writeback::model::Sequence,
    ) -> Result<(), RemoteBarrierError> {
        if journal_incarnation != self.inner.incarnation {
            return Err(RemoteBarrierError::StaleIncarnation);
        }
        self.wait_remote(sequence).await
    }

    pub(crate) async fn wait_coverage(
        &self,
        journal_incarnation: uuid::Uuid,
        sequence: crate::writeback::model::Sequence,
        remote: bool,
    ) -> Result<(), WritebackError> {
        if remote {
            self.wait_remote_coverage(journal_incarnation, sequence)
                .await
                .map_err(WritebackError::from)
        } else {
            self.wait_local_coverage(journal_incarnation, sequence)
                .await
                .map_err(WritebackError::from)
        }
    }

    // Landed-but-not-wired accessor.
    #[allow(dead_code)]
    pub(crate) fn space_sampler(&self) -> &Arc<PhysicalSpaceSampler> {
        &self.inner.space
    }

    pub(crate) fn ssd_admission(&self) -> &Arc<SsdAdmission> {
        &self.inner.ssd
    }

    async fn reserve_ssd(&self, bytes: u64) -> object_store::Result<SsdReservationToken> {
        reserve_ssd_token(&self.inner.ssd, &self.inner.space, bytes).await
    }

    #[cfg(test)]
    async fn reserve_ssd_observed(
        &self,
        bytes: u64,
        control: &WriteAdmissionTestControl,
    ) -> object_store::Result<SsdReservationToken> {
        let sample = self
            .inner
            .space
            .sample()
            .await
            .map_err(|error| generic_error(format!("writeback SSD sample failed: {error}")))?;
        reserve_ssd_token_from_sample_observed(
            &self.inner.ssd,
            &self.inner.space,
            bytes,
            sample,
            control,
        )
        .await
    }

    /// Start remote writeback after callers have finished opening over the
    /// stable recovered overlay. Repeated activation is harmless.
    pub(crate) fn activate_remote(&self) -> Result<(), RemoteBarrierError> {
        self.inner.remote.activate()
    }

    #[cfg(test)]
    fn dirty_ram_bytes(&self) -> u64 {
        self.inner.admission.used_bytes()
    }

    #[cfg(test)]
    fn dirty_ssd_reserved_bytes(&self) -> u64 {
        self.inner.ssd.used_bytes()
    }

    pub(crate) fn status(&self) -> anyhow::Result<WritebackStatus> {
        let progress = self.inner.journal.progress()?;
        let now = chrono::Utc::now().timestamp_millis().max(0) as u64;
        // One snapshot for the age: reading the watermark and the record in
        // separate transactions tears against a concurrent remote commit's
        // prune. From sequence 0 the first surviving record is the oldest
        // pending one.
        let oldest_pending_age_ms = self
            .inner
            .journal
            .pending_window(0, 1)?
            .records
            .first()
            .map(|record| now.saturating_sub(record.accepted_at_unix_ms))
            .unwrap_or(0);
        Ok(WritebackStatus {
            accepted_seq: self.inner.next_sequence.load(Ordering::Acquire),
            local_seq: progress.local_seq,
            remote_seq: progress.remote_seq,
            dirty_ram_bytes: self.inner.admission.used_bytes(),
            dirty_ram_capacity_bytes: self.inner.settings.memory_bytes,
            dirty_ram_operations: self.inner.admission.used_operations(),
            dirty_ssd_reserved_bytes: self.inner.ssd.used_bytes(),
            dirty_ssd_capacity_bytes: self.inner.settings.disk_bytes,
            dirty_ssd_operations: progress.local_seq.saturating_sub(progress.remote_seq),
            oldest_pending_age_ms,
            local_bytes_completed: progress.local_bytes_completed,
            remote_bytes_completed: progress.remote_bytes_completed,
            remote_operations_completed: progress.remote_seq,
            retries: progress.remote_retries,
            terminal_error: self.inner.remote.terminal_error(),
        })
    }

    pub(crate) async fn shutdown(&self) -> Result<(), LocalBarrierError> {
        if self
            .inner
            .stopped
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return Ok(());
        }
        match self.inner.settings.shutdown_flush {
            crate::writeback::config::ShutdownFlush::Local => {
                self.inner.remote.shutdown().await.map_err(|error| {
                    LocalBarrierError::LocalDurability(format!("remote shutdown failed: {error}"))
                })?;
                self.inner.journaler.shutdown().await?;
            }
            crate::writeback::config::ShutdownFlush::Remote => {
                self.inner.journaler.shutdown().await?;
                self.inner.remote.activate().map_err(|error| {
                    LocalBarrierError::LocalDurability(format!(
                        "remote flush activation failed: {error}"
                    ))
                })?;
                let target = self.inner.next_sequence.load(Ordering::Acquire);
                self.wait_remote(target).await.map_err(|error| {
                    LocalBarrierError::LocalDurability(format!("remote flush failed: {error}"))
                })?;
                self.inner.remote.shutdown().await.map_err(|error| {
                    LocalBarrierError::LocalDurability(format!("remote shutdown failed: {error}"))
                })?;
            }
        }
        self.inner.space_refresher.shutdown().await;
        self.inner.ssd.close();
        Ok(())
    }

    fn key_lock(&self, path: &Path) -> Arc<Mutex<()>> {
        self.inner.key_locks[self.key_lock_index(path)].clone()
    }

    fn ensure_writable(&self) -> object_store::Result<()> {
        self.inner
            .remote
            .check_available()
            .map_err(|error| generic_error(format!("writeback is unavailable: {error}")))
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
        disk: Option<SsdReservationToken>,
    ) -> object_store::Result<PutResult> {
        self.owned_put_verified(
            location,
            VerifiedPayload::new(bytes),
            options,
            Some(ram),
            disk,
            None,
        )
        .await
    }

    async fn owned_put_verified(
        self,
        location: Path,
        payload: VerifiedPayload,
        options: PutOptions,
        ram: Option<crate::writeback::admission::AcceptedAdmission>,
        disk: Option<SsdReservationToken>,
        multipart_cleanup: Option<MultipartStagingCleanup>,
    ) -> object_store::Result<PutResult> {
        let mut multipart_cleanup = multipart_cleanup;
        let result = self
            .clone()
            .owned_put_verified_inner(
                location,
                payload,
                options,
                ram,
                disk,
                &mut multipart_cleanup,
            )
            .await;
        let Some(cleanup) = multipart_cleanup else {
            return result;
        };
        let cleanup_result =
            cleanup_unsubmitted_multipart(cleanup, Arc::clone(&self.inner.space)).await;
        match (result, cleanup_result) {
            (Ok(result), Ok(())) => Ok(result),
            (Err(error), Ok(())) => Err(error),
            (Ok(_), Err(cleanup_error)) => Err(cleanup_error),
            (Err(error), Err(cleanup_error)) => Err(generic_error(format!(
                "{error}; multipart cleanup also failed: {cleanup_error}"
            ))),
        }
    }

    async fn owned_put_verified_inner(
        self,
        location: Path,
        payload: VerifiedPayload,
        options: PutOptions,
        ram: Option<crate::writeback::admission::AcceptedAdmission>,
        disk: Option<SsdReservationToken>,
        multipart_cleanup: &mut Option<MultipartStagingCleanup>,
    ) -> object_store::Result<PutResult> {
        #[cfg(test)]
        let admission_test_control = options
            .extensions
            .get::<WriteAdmissionTestControl>()
            .cloned();
        let disk_charge = MutationRecord::ssd_reservation_estimate(
            location.as_ref(),
            None,
            payload.byte_len(),
        )
        .map_err(|error| generic_error(format!("failed to size put journal entry: {error}")))?;
        let disk = match disk {
            Some(disk) => disk,
            None => {
                #[cfg(test)]
                if let Some(control) = &admission_test_control {
                    self.reserve_ssd_observed(disk_charge, control).await?
                } else {
                    self.reserve_ssd(disk_charge).await?
                }
                #[cfg(not(test))]
                self.reserve_ssd(disk_charge).await?
            }
        };
        #[cfg(test)]
        if let Some(control) = &admission_test_control {
            control.wait_for_allocation_release().await;
        }
        let lock = self.key_lock(&location);
        let key_guard = lock.lock_owned().await;
        let trusted_segment_create = options.extensions.get::<GeneratedSegmentCreate>().is_some();
        let (mode, expected_visible_version, predecessor) = match &options.mode {
            PutMode::Overwrite => (MutationMode::Overwrite, None, None),
            PutMode::Create if trusted_segment_create => {
                if self.inner.overlay.has_visible_local_object(&location).await {
                    return Err(object_store::Error::AlreadyExists {
                        path: location.to_string(),
                        source: "overlay-visible object already exists".into(),
                    });
                }
                (MutationMode::Create, None, None)
            }
            mode => {
                let visible = self.inner.overlay.visible_version(&location).await?;
                validate_put_mode(&location, mode, visible)?
            }
        };
        MutationRecord::validate_persisted_version_field(
            "expected visible version",
            expected_visible_version.as_deref(),
        )
        .map_err(|error| {
            generic_error(format!(
                "mutation metadata exceeds its bounded SSD reservation: {error}"
            ))
        })?;
        MutationRecord::validate_persisted_version_field(
            "remote predecessor ETag",
            predecessor.as_deref(),
        )
        .map_err(|error| {
            generic_error(format!(
                "mutation metadata exceeds its bounded SSD reservation: {error}"
            ))
        })?;
        // Wait for journal-queue capacity before taking the global order lock,
        // so a full queue cannot convoy unrelated writers behind this one.
        let slot =
            self.inner.journaler.reserve_slot().await.map_err(|error| {
                generic_error(format!("local journal admission failed: {error}"))
            })?;
        let order_guard = self.inner.admission_order.lock().await;
        let sequence = self.allocate_sequence()?;
        #[cfg(test)]
        if let Some(control) = &admission_test_control {
            control.mark_allocated();
        }
        let local_etag = LocalEtag::new(self.inner.incarnation, sequence);
        let path = location.to_string();
        let kind = MutationKind::Put {
            mode,
            expected_visible_version,
            payload_len: payload.byte_len(),
            payload_sha256: payload.sha256(),
            blob_path: String::new(),
        };
        let fence = classify_mutation_fence(&path, &kind, &self.inner.database_prefix);
        let record =
            MutationRecord::new(sequence, path, kind, local_etag.clone(), predecessor, fence);
        self.inner
            .overlay
            .install_verified_memory(record.clone(), payload.clone())
            .await
            .map_err(|error| generic_error(format!("overlay admission failed: {error:#}")))?;
        #[cfg(test)]
        let journal_submit_pause = { self.inner.journal_submit_pause.lock().unwrap().take() };
        #[cfg(test)]
        if let Some(pause) = journal_submit_pause {
            pause.entered.notify_one();
            pause.release.notified().await;
        }
        let barrier = match self
            .inner
            .journaler
            .submit_reserved_with_cleanup(
                slot,
                record,
                Some(payload),
                ram,
                Some(disk),
                multipart_cleanup,
            )
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
        self.await_ack(&barrier, sequence).await?;
        Ok(PutResult {
            e_tag: Some(local_etag.as_str().to_owned()),
            version: Some(local_etag.as_str().to_owned()),
            extensions: Extensions::new(),
        })
    }

    async fn owned_delete(self, location: Path) -> object_store::Result<Path> {
        let path = location.to_string();
        let disk_charge = MutationRecord::metadata_ssd_reservation(&path).map_err(|error| {
            generic_error(format!("failed to size delete journal record: {error}"))
        })?;
        let disk = self.reserve_ssd(disk_charge).await?;
        let lock = self.key_lock(&location);
        let key_guard = lock.lock_owned().await;
        let slot =
            self.inner.journaler.reserve_slot().await.map_err(|error| {
                generic_error(format!("delete journal admission failed: {error}"))
            })?;
        let order_guard = self.inner.admission_order.lock().await;
        let sequence = self.allocate_sequence()?;
        let kind = MutationKind::Delete;
        let fence = classify_mutation_fence(&path, &kind, &self.inner.database_prefix);
        let record = MutationRecord::new(
            sequence,
            path,
            kind,
            LocalEtag::new(self.inner.incarnation, sequence),
            None,
            fence,
        );
        self.inner
            .overlay
            .install_delete(record.clone())
            .await
            .map_err(|error| {
                generic_error(format!("delete overlay admission failed: {error:#}"))
            })?;
        let barrier = match self
            .inner
            .journaler
            .submit_reserved(slot, record, None, None, Some(disk))
            .await
        {
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
        self.await_ack(&barrier, sequence).await?;
        Ok(location)
    }

    async fn owned_copy_or_rename(
        self,
        from: Path,
        to: Path,
        mode: MutationMode,
        rename: bool,
    ) -> object_store::Result<()> {
        if from == to {
            let key_guards = self.lock_pair(&from, &to).await;
            let target_visible = self.inner.overlay.visible_version(&to).await?;
            if mode == MutationMode::Create && target_visible.is_some() {
                return Err(object_store::Error::AlreadyExists {
                    path: to.to_string(),
                    source: "overlay-visible copy target already exists".into(),
                });
            }
            self.inner.overlay.head(&from).await?;
            drop(key_guards);
            return Ok(());
        }

        let (key_guards, bytes_len, ram, disk) = loop {
            let expected_len = self.inner.overlay.head(&from).await?.size;
            let ram = self
                .inner
                .admission
                .reserve(expected_len)
                .await
                .map_err(|error| generic_error(format!("dirty RAM admission failed: {error}")))?
                .accept();
            let disk_charge = MutationRecord::ssd_reservation_estimate(
                to.as_ref(),
                Some(from.as_ref()),
                expected_len,
            )
            .map_err(|error| {
                generic_error(format!("failed to size copy/rename journal entry: {error}"))
            })?;
            let disk = self.reserve_ssd(disk_charge).await?;

            let key_guards = self.lock_pair(&from, &to).await;
            let target_visible = self.inner.overlay.visible_version(&to).await?;
            if mode == MutationMode::Create && target_visible.is_some() {
                return Err(object_store::Error::AlreadyExists {
                    path: to.to_string(),
                    source: "overlay-visible copy target already exists".into(),
                });
            }
            let actual_len = self.inner.overlay.head(&from).await?.size;
            if actual_len == expected_len {
                break (key_guards, actual_len, ram, disk);
            }
            drop(key_guards);
            drop(ram);
            drop(disk);
        };
        let bytes = self.inner.overlay.get(&from).await?.bytes().await?;
        if bytes.len() as u64 != bytes_len {
            return Err(generic_error(
                "copy source changed while being materialized",
            ));
        }
        let payload = VerifiedPayload::new(bytes);

        let slot = self.inner.journaler.reserve_slot().await.map_err(|error| {
            generic_error(format!("copy/rename journal admission failed: {error}"))
        })?;
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
        let path = to.to_string();
        let fence = classify_mutation_fence(&path, &kind, &self.inner.database_prefix);
        let record = MutationRecord::new(sequence, path, kind, local_etag, None, fence);
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
            .submit_reserved(slot, record, Some(payload), Some(ram), Some(disk))
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
        self.await_ack(&barrier, sequence).await?;
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
        let bytes = Bytes::from(payload);
        let owned = self.clone();
        let location = location.clone();
        tokio::spawn(async move { owned.owned_put(location, bytes, options, ram, None).await })
            .await
            .map_err(|error| generic_error(format!("owned put task failed: {error}")))?
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        options: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.ensure_writable()?;
        if options.extensions.get::<GeneratedSegmentCreate>().is_some()
            && let Some(context) = options.extensions.get::<ConditionalMultipartCreate>()
        {
            context.acknowledge();
        }
        let memory_parts = self.inner.settings.ack_mode == AckMode::Memory;
        let staging = if memory_parts {
            None
        } else {
            Some(
                allocate_multipart_staging(&self.inner.settings.dir).map_err(|error| {
                    generic_error(format!("failed to allocate multipart staging: {error}"))
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
        if options
            .extensions
            .get::<AuthoritativeSegmentRead>()
            .is_some()
        {
            return self.inner.overlay.get_remote_opts(location, options).await;
        }
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
    /// Running sum of `parts[..].len`, maintained on every push so the write
    /// path does not refold the whole part list under the state mutex.
    registered_len: u64,
    active: usize,
    aborted: bool,
}

#[derive(Debug)]
struct MultipartPart {
    len: u64,
    completed: bool,
    payload: Option<PutPayload>,
    reservation: Option<MultipartPartReservation>,
}

#[derive(Debug)]
enum MultipartPartReservation {
    Ram(RamMultipartPartReservation),
    Ssd(SsdMultipartPartReservation),
}

struct ActivePartGuard {
    state: Arc<StdMutex<MultipartState>>,
    notify: Arc<Notify>,
    index: usize,
    active: bool,
}

impl ActivePartGuard {
    fn finish(
        mut self,
        completed: bool,
        payload: Option<PutPayload>,
        reservation: Option<MultipartPartReservation>,
    ) -> bool {
        let mut state = self.state.lock().unwrap();
        state.active = state
            .active
            .checked_sub(1)
            .expect("active multipart part accounting underflow");
        if let Some(reservation) = reservation {
            state.parts[self.index].reservation = Some(reservation);
        }
        if completed && !state.aborted {
            state.parts[self.index].completed = true;
            state.parts[self.index].payload = payload;
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
        if let Err(error) = self.store.ensure_writable() {
            return Box::pin(async move { Err(error) });
        }
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
        let (index, offset, total) = {
            let mut state = self.state.lock().unwrap();
            let offset = state.registered_len;
            let Some(total) = offset.checked_add(len) else {
                return Box::pin(async { Err(generic_error("multipart length overflow")) });
            };
            if total > self.store.inner.settings.disk_bytes {
                return Box::pin(async {
                    Err(generic_error(
                        "multipart object exceeds the dirty SSD budget",
                    ))
                });
            }
            // Memory-mode parts hold their RAM admission until complete()/
            // abort(), so an object whose parts exceed the gate's capacity
            // would wait on bytes only its own completion can free -- a
            // self-deadlock that also wedges every writer queued behind it
            // on the FIFO gate. Fail fast instead.
            if self.memory_parts && total > self.store.inner.settings.memory_bytes {
                return Box::pin(async {
                    Err(generic_error(
                        "multipart object exceeds the dirty RAM budget",
                    ))
                });
            }
            let index = state.parts.len();
            state.parts.push(MultipartPart {
                len,
                completed: false,
                payload: None,
                reservation: None,
            });
            state.registered_len = total;
            (index, offset, total)
        };
        let journal_share = if self.memory_parts {
            None
        } else {
            let next =
                match MutationRecord::ssd_reservation_estimate(self.location.as_ref(), None, total)
                {
                    Ok(next) => next,
                    Err(error) => {
                        return Box::pin(async move {
                            Err(generic_error(format!(
                                "failed to size multipart journal share: {error}"
                            )))
                        });
                    }
                };
            let delta = if index == 0 {
                next
            } else {
                let previous = match MutationRecord::ssd_reservation_estimate(
                    self.location.as_ref(),
                    None,
                    offset,
                ) {
                    Ok(previous) => previous,
                    Err(error) => {
                        return Box::pin(async move {
                            Err(generic_error(format!(
                                "failed to size prior multipart journal share: {error}"
                            )))
                        });
                    }
                };
                match next.checked_sub(previous) {
                    Some(delta) => delta,
                    None => {
                        return Box::pin(async {
                            Err(generic_error("multipart journal estimate regressed"))
                        });
                    }
                }
            };
            Some((delta, u64::from(index == 0)))
        };
        let staging = self.staging.clone();
        let memory_parts = self.memory_parts;
        let state = self.state.clone();
        let notify = self.notify.clone();
        let store = self.store.clone();
        Box::pin(async move {
            store.ensure_writable()?;
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
                let reservation = tokio::select! {
                    result = reserve_memory_multipart_payload(
                        &store.inner.admission,
                        len,
                        data,
                    ) => {
                        result.map_err(|error| {
                            generic_error(format!("multipart RAM admission failed: {error}"))
                        })?
                    }
                    () = wait_for_multipart_abort(&state, &guard.notify) => {
                        return Err(generic_error("multipart upload was aborted"));
                    }
                };
                let (data, reservation) = reservation;
                let valid = data.content_length() as u64 == len;
                let aborted = guard.finish(
                    valid,
                    valid.then_some(data),
                    Some(MultipartPartReservation::Ram(reservation)),
                );
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
            let (journal_bytes, journal_operations) =
                journal_share.ok_or_else(|| generic_error("multipart journal share is missing"))?;
            let sample = store.inner.space.sample().await.map_err(|error| {
                generic_error(format!("multipart space sample failed: {error}"))
            })?;
            let reservation = tokio::select! {
                result = SsdMultipartPartReservation::reserve(
                    &store.inner.ssd,
                    len,
                    journal_bytes,
                    journal_operations,
                    sample,
                ) => {
                    result.map_err(|error| {
                        generic_error(format!("multipart SSD admission failed: {error}"))
                    })?
                }
                () = wait_for_multipart_abort(&state, &guard.notify) => {
                    return Err(generic_error("multipart upload was aborted"));
                }
            };
            let part_path = staging.join("payload.staged");
            let (write, aborted) = tokio::task::spawn_blocking(move || {
                let write = ensure_multipart_staging(&staging)
                    .and_then(|()| write_multipart_part(&part_path, data, offset))
                    .map_err(|error| {
                        generic_error(format!("multipart part write failed: {error}"))
                    });
                let aborted = guard.finish(
                    write.is_ok(),
                    None,
                    Some(MultipartPartReservation::Ssd(reservation)),
                );
                (write, aborted)
            })
            .await
            .map_err(|error| generic_error(format!("multipart part task failed: {error}")))?;
            if aborted {
                Err(generic_error("multipart upload was aborted"))
            } else {
                write
            }
        })
    }

    async fn complete(&mut self) -> object_store::Result<PutResult> {
        self.store.ensure_writable()?;
        if self.terminal {
            return Err(generic_error(
                "multipart upload is already completed or aborted",
            ));
        }
        let (parts, total_len) = {
            let mut state = self.state.lock().unwrap();
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
            validate_completed_multipart_parts(&state.parts, self.memory_parts)?;
            (
                std::mem::take(&mut state.parts),
                total.ok_or_else(|| generic_error("multipart length overflow"))?,
            )
        };
        let staging = self.staging.take();
        self.terminal = true;
        let store = self.store.clone();
        let location = self.location.clone();
        let options = self.options.clone();
        let memory_parts = self.memory_parts;
        tokio::spawn(async move {
            if memory_parts {
                complete_memory_multipart(store, location, options, parts, total_len).await
            } else {
                let staging =
                    staging.ok_or_else(|| generic_error("multipart staging is missing"))?;
                complete_multipart(store, location, options, staging, parts, total_len).await
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
        self.notify.notify_waiters();
        let state = self.state.clone();
        let notify = self.notify.clone();
        let ssd = self.store.inner.ssd.as_ref().clone();
        let space = Arc::clone(&self.store.inner.space);
        tokio::spawn(async move {
            wait_for_multipart_parts(&state, &notify).await;
            cleanup_aborted_multipart(&state, staging, ssd, space).await
        })
        .await
        .map_err(|error| generic_error(format!("owned multipart abort failed: {error}")))?
    }
}

impl Drop for WritebackMultipartUpload {
    fn drop(&mut self) {
        if self.terminal {
            return;
        }
        self.state.lock().unwrap().aborted = true;
        self.notify.notify_waiters();
        let Some(staging) = self.staging.take() else {
            return;
        };
        let state = self.state.clone();
        let notify = self.notify.clone();
        let ssd = self.store.inner.ssd.as_ref().clone();
        let space = Arc::clone(&self.store.inner.space);
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            runtime.spawn(async move {
                wait_for_multipart_parts(&state, &notify).await;
                if let Err(error) =
                    cleanup_aborted_multipart(&state, Some(staging), ssd, space).await
                {
                    tracing::warn!(%error, "failed to clean dropped writeback multipart staging");
                }
            });
        } else if state.lock().unwrap().active == 0 {
            let reservations = take_multipart_reservations(&state);
            match remove_aborted_reservations(staging, reservations) {
                Ok(Some(cleaned)) => {
                    cleaned.admission().poison(
                        "multipart staging was removed without a fresh physical-space sample",
                    );
                    cleaned.retain_claims();
                }
                Ok(None) => {}
                Err(error) => {
                    tracing::warn!(%error, "failed to clean dropped writeback multipart staging");
                }
            }
        } else {
            ssd.poison(format!(
                "multipart runtime disappeared with active staging writes at {}",
                staging.display()
            ));
            tracing::error!(
                path = %staging.display(),
                "multipart runtime disappeared with active staging writes; claims retained"
            );
        }
    }
}

async fn reserve_memory_multipart_payload(
    admission: &Admission,
    bytes: u64,
    payload: PutPayload,
) -> Result<(PutPayload, RamMultipartPartReservation), crate::writeback::admission::AdmissionError>
{
    let reservation = RamMultipartPartReservation::reserve(admission, bytes).await?;
    Ok((payload, reservation))
}

fn validate_completed_multipart_parts(
    parts: &[MultipartPart],
    memory_parts: bool,
) -> object_store::Result<()> {
    for part in parts {
        match (&part.payload, &part.reservation, memory_parts) {
            (Some(_), Some(MultipartPartReservation::Ram(_)), true)
            | (None, Some(MultipartPartReservation::Ssd(_)), false) => {}
            _ => return Err(generic_error("multipart part ownership is incomplete")),
        }
    }
    Ok(())
}

fn multipart_put_options(options: PutMultipartOptions) -> PutOptions {
    let mode = if options.extensions.get::<GeneratedSegmentCreate>().is_some() {
        PutMode::Create
    } else {
        PutMode::Overwrite
    };
    PutOptions {
        mode,
        tags: options.tags,
        attributes: options.attributes,
        extensions: options.extensions,
    }
}

async fn complete_memory_multipart(
    store: WritebackObjectStore,
    location: Path,
    options: PutMultipartOptions,
    parts: Vec<MultipartPart>,
    total_len: u64,
) -> object_store::Result<PutResult> {
    let mut payload_parts = Vec::with_capacity(parts.len());
    let mut reservations = Vec::with_capacity(parts.len().max(1));
    for mut part in parts {
        payload_parts.push(
            part.payload
                .take()
                .ok_or_else(|| generic_error("memory multipart payload is missing"))?,
        );
        match part.reservation.take() {
            Some(MultipartPartReservation::Ram(reservation)) => reservations.push(reservation),
            _ => return Err(generic_error("memory multipart reservation is missing")),
        }
    }
    if reservations.is_empty() {
        reservations.push(
            RamMultipartPartReservation::reserve(&store.inner.admission, 0)
                .await
                .map_err(|error| {
                    generic_error(format!("empty multipart RAM admission failed: {error}"))
                })?,
        );
    }
    let promoted = promote_multipart(MultipartReservationSet::Ram(reservations))
        .map_err(|error| generic_error(format!("multipart RAM promotion failed: {error}")))?;
    let MutationReservation::Ram(ram) = promoted.mutation else {
        return Err(generic_error(
            "multipart RAM promotion returned the wrong tier",
        ));
    };
    let payload = payload_parts
        .into_iter()
        .flat_map(IntoIterator::into_iter)
        .collect::<PutPayload>();
    if payload.content_length() as u64 != total_len {
        return Err(generic_error("multipart assembled length mismatch"));
    }
    let verified = VerifiedPayload::from_put_payload(payload);
    let disk_charge = MutationRecord::ssd_reservation_estimate(location.as_ref(), None, total_len)
        .map_err(|error| {
            generic_error(format!(
                "failed to size memory multipart journal entry: {error}"
            ))
        })?;
    let disk = store.reserve_ssd(disk_charge).await?;
    let put_options = multipart_put_options(options);
    store
        .clone()
        .owned_put_verified(
            location,
            verified,
            put_options,
            Some(ram.final_ram),
            Some(disk),
            None,
        )
        .await
}

async fn complete_multipart(
    store: WritebackObjectStore,
    location: Path,
    options: PutMultipartOptions,
    staging: PathBuf,
    parts: Vec<MultipartPart>,
    total_len: u64,
) -> object_store::Result<PutResult> {
    let mut reservations = Vec::with_capacity(parts.len().max(1));
    let mut create_empty_staging = false;
    for mut part in parts {
        match part.reservation.take() {
            Some(MultipartPartReservation::Ssd(reservation)) => reservations.push(reservation),
            _ => return Err(generic_error("SSD multipart reservation is missing")),
        }
    }
    if reservations.is_empty() {
        create_empty_staging = true;
        let journal_bytes =
            MutationRecord::ssd_reservation_estimate(location.as_ref(), None, total_len).map_err(
                |error| {
                    generic_error(format!(
                        "failed to size empty multipart journal entry: {error}"
                    ))
                },
            )?;
        let sample = store.inner.space.sample().await.map_err(|error| {
            generic_error(format!("empty multipart space sample failed: {error}"))
        })?;
        reservations.push(
            SsdMultipartPartReservation::reserve(
                store.inner.ssd.as_ref(),
                0,
                journal_bytes,
                1,
                sample,
            )
            .await
            .map_err(|error| {
                generic_error(format!("empty multipart SSD admission failed: {error}"))
            })?,
        );
    }
    let promoted = promote_multipart(MultipartReservationSet::Ssd(reservations))
        .map_err(|error| generic_error(format!("multipart SSD promotion failed: {error}")))?;
    let MutationReservation::Ssd(ssd) = promoted.mutation else {
        return Err(generic_error(
            "multipart SSD promotion returned the wrong tier",
        ));
    };
    let (disk, cleanup) = ssd.into_owned(staging.clone());
    if create_empty_staging {
        let empty_path = staging.join("payload.staged");
        let empty_staging = staging.clone();
        let write = tokio::task::spawn_blocking(move || {
            ensure_multipart_staging(&empty_staging)?;
            write_multipart_part(&empty_path, Bytes::new().into(), 0)
        })
        .await
        .map_err(|error| generic_error(format!("empty multipart staging task failed: {error}")))
        .and_then(|result| {
            result
                .map_err(|error| generic_error(format!("empty multipart staging failed: {error}")))
        });
        if let Err(error) = write {
            return cleanup_failed_multipart(&store, disk, cleanup, error).await;
        }
    }
    let payload_path = staging.join("payload.staged");
    let payload = tokio::task::spawn_blocking(move || {
        VerifiedPayload::from_staged_file(payload_path, total_len)
    })
    .await
    .map_err(|error| generic_error(format!("multipart verification task failed: {error}")))
    .and_then(|result| {
        result.map_err(|error| generic_error(format!("multipart verification failed: {error}")))
    });
    let payload = match payload {
        Ok(payload) => payload,
        Err(error) => return cleanup_failed_multipart(&store, disk, cleanup, error).await,
    };
    store
        .clone()
        .owned_put_verified(
            location,
            payload,
            multipart_put_options(options),
            None,
            Some(disk),
            Some(cleanup),
        )
        .await
}

async fn cleanup_failed_multipart<T>(
    store: &WritebackObjectStore,
    disk: SsdReservationToken,
    cleanup: MultipartStagingCleanup,
    error: object_store::Error,
) -> object_store::Result<T> {
    drop(disk);
    match cleanup_unsubmitted_multipart(cleanup, Arc::clone(&store.inner.space)).await {
        Ok(()) => Err(error),
        Err(cleanup_error) => Err(generic_error(format!(
            "{error}; multipart cleanup also failed: {cleanup_error}"
        ))),
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

async fn wait_for_multipart_abort(state: &StdMutex<MultipartState>, notify: &Notify) {
    loop {
        let notified = notify.notified();
        if state.lock().unwrap().aborted {
            return;
        }
        notified.await;
    }
}

fn take_multipart_reservations(state: &StdMutex<MultipartState>) -> Vec<MultipartPartReservation> {
    state
        .lock()
        .unwrap()
        .parts
        .iter_mut()
        .filter_map(|part| part.reservation.take())
        .collect()
}

fn remove_aborted_reservations(
    staging: PathBuf,
    reservations: Vec<MultipartPartReservation>,
) -> std::io::Result<Option<CleanedMultipartStaging>> {
    let mut ssd_reservations = Vec::with_capacity(reservations.len());
    for reservation in reservations {
        match reservation {
            MultipartPartReservation::Ram(reservation) => drop(reservation),
            MultipartPartReservation::Ssd(reservation) => ssd_reservations.push(reservation),
        }
    }
    remove_aborted_ssd_multipart(staging, ssd_reservations)
}

async fn cleanup_aborted_multipart(
    state: &StdMutex<MultipartState>,
    staging: Option<PathBuf>,
    ssd: SsdAdmission,
    space: Arc<PhysicalSpaceSampler>,
) -> object_store::Result<()> {
    let reservations = take_multipart_reservations(state);
    let Some(staging) = staging else {
        drop(reservations);
        return Ok(());
    };
    let cleaned = tokio::task::spawn_blocking(move || {
        drop(ssd);
        remove_aborted_reservations(staging, reservations)
    })
    .await
    .map_err(|error| generic_error(format!("multipart cleanup task failed: {error}")))?
    .map_err(|error| generic_error(format!("multipart cleanup failed: {error}")))?;
    match cleaned {
        Some(cleaned) => release_cleaned_multipart(cleaned, space, "abort").await,
        None => Ok(()),
    }
}

async fn cleanup_unsubmitted_multipart(
    cleanup: MultipartStagingCleanup,
    space: Arc<PhysicalSpaceSampler>,
) -> object_store::Result<()> {
    let cleaned = tokio::task::spawn_blocking(move || cleanup.remove())
        .await
        .map_err(|error| generic_error(format!("multipart cleanup task failed: {error}")))?
        .map_err(|error| generic_error(format!("multipart cleanup failed: {error}")))?;
    release_cleaned_multipart(cleaned, space, "unsubmitted mutation").await
}

async fn release_cleaned_multipart(
    cleaned: CleanedMultipartStaging,
    space: Arc<PhysicalSpaceSampler>,
    context: &'static str,
) -> object_store::Result<()> {
    let sample = match space.sample().await {
        Ok(sample) => sample,
        Err(error) => {
            cleaned.poison_and_retain(format!(
                "physical-space sample failed after multipart {context} cleanup: {error}"
            ));
            return Err(generic_error(format!(
                "multipart {context} post-cleanup space sample failed: {error}"
            )));
        }
    };
    if let Err(error) = cleaned.admission().observe_sample(sample) {
        cleaned.poison_and_retain(format!(
            "multipart {context} post-cleanup sample was rejected: {error}"
        ));
        return Err(generic_error(format!(
            "multipart {context} post-cleanup space sample was rejected: {error}"
        )));
    }
    drop(cleaned);
    Ok(())
}

fn allocate_multipart_staging(writeback_root: &FilePath) -> std::io::Result<PathBuf> {
    let root = writeback_root.join("tmp").join("multipart");
    match fs::create_dir(&root) {
        Ok(()) => set_directory_mode(&root)?,
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
        Err(error) => return Err(error),
    }
    validate_private_directory(&root)?;
    Ok(root.join(Uuid::new_v4().to_string()))
}

fn ensure_multipart_staging(staging: &FilePath) -> std::io::Result<()> {
    match fs::create_dir(staging) {
        Ok(()) => set_directory_mode(staging)?,
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
        Err(error) => return Err(error),
    }
    validate_private_directory(staging)
}

fn write_multipart_part(path: &FilePath, payload: PutPayload, offset: u64) -> std::io::Result<()> {
    let mut options = OpenOptions::new();
    options.write(true).create(true);
    #[cfg(unix)]
    options.mode(0o600).custom_flags(libc::O_NOFOLLOW);
    let file = options.open(path)?;
    #[cfg(unix)]
    file.set_permissions(fs::Permissions::from_mode(0o600))?;
    let mut written = 0u64;
    for chunk in payload {
        let mut remaining = chunk.as_ref();
        while !remaining.is_empty() {
            #[cfg(unix)]
            let count = {
                use std::os::unix::fs::FileExt;
                file.write_at(remaining, offset + written)?
            };
            #[cfg(not(unix))]
            let count = {
                use std::io::{Seek, SeekFrom};
                let mut file = &file;
                file.seek(SeekFrom::Start(offset + written))?;
                file.write(remaining)?
            };
            if count == 0 {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::WriteZero,
                    "multipart staging write made no progress",
                ));
            }
            written = written
                .checked_add(count as u64)
                .ok_or_else(|| std::io::Error::other("multipart staged offset overflow"))?;
            remaining = &remaining[count..];
        }
    }
    file.sync_data()
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
) -> object_store::Result<(MutationMode, Option<String>, Option<String>)> {
    match mode {
        PutMode::Overwrite => Ok((MutationMode::Overwrite, None, None)),
        PutMode::Create => {
            if visible.is_some() {
                return Err(object_store::Error::AlreadyExists {
                    path: location.to_string(),
                    source: "overlay-visible object already exists".into(),
                });
            }
            Ok((MutationMode::Create, None, None))
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
            Ok((MutationMode::Update, expected_string, predecessor))
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

async fn reserve_ssd_token(
    ssd: &SsdAdmission,
    space: &PhysicalSpaceSampler,
    bytes: u64,
) -> object_store::Result<SsdReservationToken> {
    let sample = space
        .sample()
        .await
        .map_err(|error| generic_error(format!("writeback SSD sample failed: {error}")))?;
    reserve_ssd_token_from_sample(ssd, space, bytes, sample).await
}

async fn reserve_ssd_token_from_sample(
    ssd: &SsdAdmission,
    space: &PhysicalSpaceSampler,
    bytes: u64,
    sample: PhysicalSpaceSample,
) -> object_store::Result<SsdReservationToken> {
    reserve_ssd_token_from_sample_with_observer(ssd, space, bytes, sample, || {}).await
}

#[cfg(test)]
async fn reserve_ssd_token_from_sample_observed(
    ssd: &SsdAdmission,
    space: &PhysicalSpaceSampler,
    bytes: u64,
    sample: PhysicalSpaceSample,
    control: &WriteAdmissionTestControl,
) -> object_store::Result<SsdReservationToken> {
    reserve_ssd_token_from_sample_with_observer(ssd, space, bytes, sample, || {
        control.mark_queued();
    })
    .await
}

async fn reserve_ssd_token_from_sample_with_observer(
    ssd: &SsdAdmission,
    space: &PhysicalSpaceSampler,
    bytes: u64,
    mut sample: PhysicalSpaceSample,
    on_queued: impl Fn(),
) -> object_store::Result<SsdReservationToken> {
    let request = SsdReservationRequest {
        ssd_reservation_bytes: bytes,
        physical_reservation_bytes: bytes,
        operations: 1,
    };
    loop {
        match ssd
            .reserve_with_queue_observer(request, sample, &on_queued)
            .await
        {
            Ok(token) => return Ok(token),
            Err(error @ ReservationError::StaleSample { latest, .. }) => {
                let Some(newer) = space.latest_sample().filter(|newer| {
                    newer.generation >= latest && newer.generation > sample.generation
                }) else {
                    return Err(generic_error(format!(
                        "dirty SSD admission failed: {error}"
                    )));
                };
                sample = newer;
            }
            Err(error) => {
                return Err(generic_error(format!(
                    "dirty SSD admission failed: {error}"
                )));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{WritebackObjectStore, reserve_ssd_token_from_sample};
    use crate::config::CompressionConfig;
    use crate::fault_store::{FaultControls, FaultStore};
    use crate::frame_codec::FrameCodec;
    use crate::segment::{SEGMENT_INFO, Segid};
    use crate::segment_store::{GeneratedSegmentCreate, SegmentStore};
    use crate::writeback::admission::Admission;
    use crate::writeback::config::{AckMode, ShutdownFlush, WritebackSettings};
    use crate::writeback::journal::Journal;
    use crate::writeback::model::{
        FenceClass, JournalIdentity, LocalEtag, MutationKind, MutationMode, MutationRecord,
        classify_mutation_fence,
    };
    use crate::writeback::payload::VerifiedPayload;
    use crate::writeback::reservation::{SsdAdmission, SsdReservationRequest};
    use crate::writeback::space_sample::PhysicalSpaceSampler;
    use crate::writeback::test_util::WriteAdmissionTestControl;
    use bytes::Bytes;
    use futures::{StreamExt, stream};
    use object_store::memory::InMemory;
    use object_store::path::Path;
    use object_store::prefix::PrefixStore;
    use object_store::{
        CopyMode, CopyOptions, ObjectStore, ObjectStoreExt, PutMode, PutMultipartOptions,
        PutOptions, RenameOptions, RenameTargetMode, UpdateVersion,
    };
    #[cfg(unix)]
    use std::os::unix::fs::PermissionsExt;
    use std::sync::Arc;
    use std::time::Duration;
    use uuid::Uuid;

    async fn test_store() -> (WritebackObjectStore, Arc<InMemory>, tempfile::TempDir) {
        test_store_with_remote_drain(false).await
    }

    #[tokio::test]
    async fn allocation_test_control_does_not_deadlock_on_a_key_shard_collision() {
        let (store, _remote, _temp) = test_store().await;
        let first_path = Path::from("zerofs/pilot/segments/01/0000000000000001/0000000000000001");
        let first_shard = store.key_lock_index(&first_path);
        let second_path = (2_u64..10_000)
            .map(|counter| {
                Path::from(format!(
                    "zerofs/pilot/segments/02/0000000000000001/{counter:016x}"
                ))
            })
            .find(|path| store.key_lock_index(path) == first_shard)
            .expect("a colliding benchmark path exists within the bounded search");

        let second_control = WriteAdmissionTestControl::new();
        let mut second_options = PutOptions::default();
        second_options.extensions.insert(second_control.clone());

        let second = tokio::spawn({
            let store = store.clone();
            let second_path = second_path.clone();
            async move {
                store
                    .put_opts(
                        &second_path,
                        Bytes::from_static(b"second").into(),
                        second_options,
                    )
                    .await
            }
        });
        tokio::time::timeout(
            Duration::from_secs(1),
            second_control.wait_until_allocation_waits(),
        )
        .await
        .expect("the later colliding path must reach the allocation test seam");

        let first_control = WriteAdmissionTestControl::new();
        let mut first_options = PutOptions::default();
        first_options.extensions.insert(first_control.clone());
        let first = tokio::spawn({
            let store = store.clone();
            let first_path = first_path.clone();
            async move {
                store
                    .put_opts(
                        &first_path,
                        Bytes::from_static(b"first").into(),
                        first_options,
                    )
                    .await
            }
        });
        tokio::time::timeout(
            Duration::from_secs(1),
            first_control.wait_until_allocation_waits(),
        )
        .await
        .expect("the earlier path must reach allocation despite the colliding later path");
        first_control.release_allocation();
        tokio::time::timeout(Duration::from_secs(1), first_control.wait_until_allocated())
            .await
            .expect("the earlier path must allocate first");
        second_control.release_allocation();

        let first = first.await.unwrap().unwrap();
        let second = second.await.unwrap().unwrap();

        assert_eq!(
            first
                .e_tag
                .as_deref()
                .and_then(LocalEtag::sequence_from_str),
            Some(1)
        );
        assert_eq!(
            second
                .e_tag
                .as_deref()
                .and_then(LocalEtag::sequence_from_str),
            Some(2)
        );
        store.shutdown().await.unwrap();
    }

    #[test]
    fn only_recognized_immutable_data_objects_bypass_remote_fences() {
        let cases = [
            (
                "zerofs/pilot/segments/0a/0000000000000001/0000000000000002",
                PutMode::Overwrite,
                FenceClass::Fence,
            ),
            (
                "zerofs/pilot/segments/02/0000000000000001/0000000000000002",
                PutMode::Create,
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
            (
                "other/prefix/segments/02/0000000000000001/0000000000000002",
                PutMode::Create,
                FenceClass::Fence,
            ),
            (
                "zerofs/pilot/uploads/segments/02/0000000000000001/0000000000000002",
                PutMode::Create,
                FenceClass::Fence,
            ),
            (
                "zerofs/pilot/segments/ff/0000000000000001/0000000000000002",
                PutMode::Create,
                FenceClass::Fence,
            ),
            (
                "zerofs/pilot/segments/02/0000000000000001/000000000000000A",
                PutMode::Create,
                FenceClass::Fence,
            ),
            (
                "zerofs/pilot/tmp/wal/00000000000000000042.sst",
                PutMode::Create,
                FenceClass::Fence,
            ),
            (
                "zerofs/pilot/wal/99999999999999999999.sst",
                PutMode::Create,
                FenceClass::Fence,
            ),
            (
                "zerofs/pilot/compacted/01KZS4K6C1G11KM91DI3YA9TJE.sst",
                PutMode::Create,
                FenceClass::Fence,
            ),
            (
                "zerofs/pilot/compacted/81KZS4K6C1G11KM91DJ3YA9TJE.sst",
                PutMode::Create,
                FenceClass::Fence,
            ),
        ];

        for (path, mode, expected) in cases {
            let mode = match mode {
                PutMode::Overwrite => MutationMode::Overwrite,
                PutMode::Create => MutationMode::Create,
                PutMode::Update(_) => MutationMode::Update,
            };
            let kind = MutationKind::Put {
                mode,
                expected_visible_version: None,
                payload_len: 0,
                payload_sha256: [0; 32],
                blob_path: String::new(),
            };
            let actual = classify_mutation_fence(path, &kind, "zerofs/pilot");
            assert_eq!(actual, expected, "classification for {path}");
        }

        let encoded_prefix_create = MutationKind::Put {
            mode: MutationMode::Create,
            expected_visible_version: None,
            payload_len: 0,
            payload_sha256: [0; 32],
            blob_path: String::new(),
        };
        assert_eq!(
            classify_mutation_fence(
                "zerofs/tenant%20a/segments/02/0000000000000001/0000000000000002",
                &encoded_prefix_create,
                "zerofs/tenant%20a",
            ),
            FenceClass::ImmutableCreate,
            "persisted encoded path components must be parsed without double encoding"
        );
    }

    #[tokio::test]
    async fn generated_segment_multipart_is_journaled_as_an_immutable_create() {
        let (store, _remote, _temp) = test_store().await;
        let path = Path::from("zerofs/pilot/segments/02/0000000000000001/0000000000000002");
        let mut options = PutMultipartOptions::default();
        options.extensions.insert(GeneratedSegmentCreate);
        let mut upload = store.put_multipart_opts(&path, options).await.unwrap();
        upload
            .put_part(Bytes::from_static(b"part one").into())
            .await
            .unwrap();
        upload
            .put_part(Bytes::from_static(b"part two").into())
            .await
            .unwrap();
        upload.complete().await.unwrap();
        store.wait_local(1).await.unwrap();

        let records = store.inner.journal.snapshot().unwrap().records;
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].fence, FenceClass::ImmutableCreate);
        assert!(matches!(
            records[0].kind,
            MutationKind::Put {
                mode: MutationMode::Create,
                ..
            }
        ));
        store.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn generated_segment_multiparts_preupload_across_later_ordering_fences() {
        async fn put_generated_multipart(store: &WritebackObjectStore, path: &Path, byte: u8) {
            let mut options = PutMultipartOptions::default();
            options.extensions.insert(GeneratedSegmentCreate);
            let mut upload = store.put_multipart_opts(path, options).await.unwrap();
            upload
                .put_part(Bytes::from(vec![byte; 1024]).into())
                .await
                .unwrap();
            upload.complete().await.unwrap();
        }

        let (store, _remote, _temp, controls) = test_store_with_controls(true).await;
        controls.block_puts();
        let manifest_one = Path::from("zerofs/pilot/manifest/one");
        let segment_two = Path::from("zerofs/pilot/segments/02/0000000000000001/0000000000000002");
        let manifest_three = Path::from("zerofs/pilot/manifest/three");
        let segment_four = Path::from("zerofs/pilot/segments/04/0000000000000001/0000000000000004");

        store
            .put(&manifest_one, Bytes::from_static(b"manifest one").into())
            .await
            .unwrap();
        put_generated_multipart(&store, &segment_two, 2).await;
        store
            .put(
                &manifest_three,
                Bytes::from_static(b"manifest three").into(),
            )
            .await
            .unwrap();
        put_generated_multipart(&store, &segment_four, 4).await;
        store.wait_local(4).await.unwrap();

        tokio::time::timeout(Duration::from_secs(2), async {
            while controls.put_count() < 3 {
                controls.put_activity().notified().await;
            }
        })
        .await
        .expect("multipart immutable creates did not preupload across the later fence");
        let started = controls.put_paths();
        assert!(started.contains(&manifest_one.to_string()));
        assert!(started.contains(&segment_two.to_string()));
        assert!(started.contains(&segment_four.to_string()));
        assert!(
            !started.contains(&manifest_three.to_string()),
            "a later ordering fence started before the frontier advanced"
        );

        controls.release_puts();
        store.wait_remote(4).await.unwrap();
        store.shutdown().await.unwrap();
    }

    async fn assert_generated_segment_preserves_remote_collision(size: usize) {
        let capacity = (size as u64).saturating_mul(2).max(4 * 1024 * 1024);
        let (store, remote, _temp, _controls) = test_store_with_capacities(
            true,
            AckMode::Remote,
            ShutdownFlush::Remote,
            capacity,
            capacity,
        )
        .await;
        let segid = Segid::new(5, 0);
        let remote_path = Path::from(format!("zerofs/pilot/{}", segid.object_key()));
        let existing = Bytes::from(vec![1u8; size]);
        remote
            .put(&remote_path, existing.clone().into())
            .await
            .unwrap();
        let prefixed: Arc<dyn ObjectStore> = Arc::new(PrefixStore::new(
            Arc::new(store.clone()),
            Path::from("zerofs/pilot"),
        ));
        let segments = SegmentStore::new(
            prefixed,
            FrameCodec::new(&[1u8; 32], SEGMENT_INFO, CompressionConfig::Lz4),
            5,
            None,
        );

        segments
            .put_segment(segid, Bytes::from(vec![2u8; size]))
            .await
            .expect_err("remote immutable-key collision must remain fatal");
        assert_eq!(
            remote
                .get(&remote_path)
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap(),
            existing
        );
        let _ = store.shutdown().await;
    }

    #[tokio::test]
    async fn generated_segment_single_put_preserves_a_conflicting_remote_object() {
        assert_generated_segment_preserves_remote_collision(1024).await;
    }

    #[tokio::test]
    async fn generated_segment_multipart_preserves_a_conflicting_remote_object() {
        assert_generated_segment_preserves_remote_collision(64 * 1024 * 1024).await;
    }

    #[tokio::test]
    async fn only_explicit_safe_put_create_records_are_immutable() {
        let (store, remote, _temp) = test_store().await;
        let update_path = Path::from("zerofs/pilot/segments/03/0000000000000001/0000000000000003");
        let updated = remote
            .put(&update_path, Bytes::from_static(b"old").into())
            .await
            .unwrap();
        for path in ["copy-source", "rename-source"] {
            remote
                .put(&Path::from(path), Bytes::from_static(b"source").into())
                .await
                .unwrap();
        }

        store
            .put(
                &Path::from("zerofs/pilot/segments/01/0000000000000001/0000000000000001"),
                Bytes::from_static(b"overwrite").into(),
            )
            .await
            .unwrap();
        store
            .put_opts(
                &Path::from("zerofs/pilot/segments/02/0000000000000001/0000000000000002"),
                Bytes::from_static(b"create").into(),
                PutOptions::from(PutMode::Create),
            )
            .await
            .unwrap();
        store
            .put_opts(
                &update_path,
                Bytes::from_static(b"update").into(),
                PutOptions::from(PutMode::Update(updated.into())),
            )
            .await
            .unwrap();
        store
            .delete(&Path::from(
                "zerofs/pilot/segments/04/0000000000000001/0000000000000004",
            ))
            .await
            .unwrap();
        store
            .copy_opts(
                &Path::from("copy-source"),
                &Path::from("zerofs/pilot/segments/05/0000000000000001/0000000000000005"),
                CopyOptions {
                    mode: CopyMode::Create,
                    ..CopyOptions::default()
                },
            )
            .await
            .unwrap();
        store
            .rename_opts(
                &Path::from("rename-source"),
                &Path::from("zerofs/pilot/segments/06/0000000000000001/0000000000000006"),
                RenameOptions {
                    target_mode: RenameTargetMode::Create,
                    ..RenameOptions::default()
                },
            )
            .await
            .unwrap();
        store.wait_local(6).await.unwrap();

        let records = store.inner.journal.snapshot().unwrap().records;
        assert_eq!(records.len(), 6);
        assert_eq!(records[0].fence, FenceClass::Fence);
        assert_eq!(records[1].fence, FenceClass::ImmutableCreate);
        assert_eq!(records[2].fence, FenceClass::Fence);
        assert_eq!(records[3].fence, FenceClass::Fence);
        assert_eq!(records[4].fence, FenceClass::Fence);
        assert_eq!(records[5].fence, FenceClass::Fence);

        store.shutdown().await.unwrap();
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
        test_store_with_disk_capacity(enabled, ack_mode, shutdown_flush, 10_000_000).await
    }

    async fn test_store_with_disk_capacity(
        enabled: bool,
        ack_mode: AckMode,
        shutdown_flush: ShutdownFlush,
        disk_bytes: u64,
    ) -> (
        WritebackObjectStore,
        Arc<InMemory>,
        tempfile::TempDir,
        Arc<FaultControls>,
    ) {
        test_store_with_capacities(enabled, ack_mode, shutdown_flush, 1_000_000, disk_bytes).await
    }

    async fn test_store_with_capacities(
        enabled: bool,
        ack_mode: AckMode,
        shutdown_flush: ShutdownFlush,
        memory_bytes: u64,
        disk_bytes: u64,
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
            memory_bytes,
            disk_bytes,
            min_free_bytes: 1,
            high_watermark_percent: 95,
            resume_percent: 85,
            upload_concurrency: 4,
            local_concurrency: 4,
            shutdown_flush,
        };
        let store = WritebackObjectStore::open(writeback_remote, journal, settings)
            .await
            .unwrap();
        (store, remote, temp, controls)
    }

    async fn test_store_with_resource_limits(
        ack_mode: AckMode,
        memory_bytes: u64,
        disk_bytes: u64,
        max_operations: u64,
    ) -> (WritebackObjectStore, Arc<InMemory>, tempfile::TempDir) {
        let temp = tempfile::tempdir().unwrap();
        let settings = WritebackSettings {
            dir: temp.path().join("writeback"),
            ack_mode,
            memory_bytes,
            disk_bytes,
            min_free_bytes: 1,
            high_watermark_percent: 95,
            resume_percent: 85,
            upload_concurrency: 2,
            local_concurrency: 2,
            shutdown_flush: ShutdownFlush::Local,
        };
        let journal = Arc::new(
            Journal::open(
                settings.dir.clone(),
                JournalIdentity {
                    format_version: 1,
                    bucket_id: "multipart-resource-limits".to_owned(),
                    backend_endpoint: "memory://remote".to_owned(),
                    database_prefix: "zerofs/pilot".to_owned(),
                    backend_kind: "memory".to_owned(),
                    encryption_key_identity_sha256: [0x77; 32],
                },
            )
            .unwrap(),
        );
        let space = Arc::new(PhysicalSpaceSampler::new(settings.dir.clone()));
        let sample = space.sample().await.unwrap();
        let ssd = Arc::new(
            SsdAdmission::recover(
                disk_bytes,
                max_operations,
                settings.high_watermark_percent,
                settings.resume_percent,
                settings.min_free_bytes,
                std::iter::empty(),
                Some(sample),
            )
            .unwrap(),
        );
        let remote = Arc::new(InMemory::new());
        let store = WritebackObjectStore::open_paused_with_owners(
            remote.clone(),
            journal,
            settings,
            space,
            ssd,
        )
        .await
        .unwrap();
        (store, remote, temp)
    }

    #[tokio::test]
    async fn out_of_order_space_probe_retries_with_the_latest_sample() {
        let temp = tempfile::tempdir().unwrap();
        let space = PhysicalSpaceSampler::new(temp.path());
        let stale = space.sample().await.unwrap();
        let latest = space.sample().await.unwrap();
        let ssd = SsdAdmission::recover(
            1 << 20,
            16,
            95,
            85,
            1,
            std::iter::empty::<SsdReservationRequest>(),
            Some(latest),
        )
        .unwrap();

        let token = reserve_ssd_token_from_sample(&ssd, &space, 4096, stale)
            .await
            .unwrap();

        assert_eq!(ssd.used_bytes(), 4096);
        drop(token);
        assert_eq!(ssd.used_bytes(), 0);
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
        let persisted_charge = store.inner.journal.snapshot().unwrap().records[0]
            .ssd_reservation_bytes()
            .unwrap();
        assert_eq!(store.dirty_ssd_reserved_bytes(), persisted_charge);
        store.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn overwrite_acknowledges_without_a_backend_head() {
        let (store, _remote, _temp, controls) = test_store_with_controls(false).await;
        let path = Path::from("segments/overwrite-with-blocked-head");
        controls.block_heads();
        let store_for_put = store.clone();
        let path_for_put = path.clone();
        let mut overwrite = tokio::spawn(async move {
            store_for_put
                .put_opts(
                    &path_for_put,
                    Bytes::from_static(b"payload").into(),
                    PutOptions::from(PutMode::Overwrite),
                )
                .await
        });

        let acknowledged = tokio::time::timeout(Duration::from_secs(1), &mut overwrite).await;
        if acknowledged.is_err() {
            controls.release_heads();
            overwrite.await.unwrap().unwrap();
            panic!("Overwrite waited for a blocked backend HEAD");
        }
        acknowledged.unwrap().unwrap().unwrap();
        assert_eq!(controls.head_count(), 0);

        controls.release_heads();
        store.wait_local(1).await.unwrap();
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
        let persisted_charge = store.inner.journal.snapshot().unwrap().records[0]
            .ssd_reservation_bytes()
            .unwrap();
        assert_eq!(status.dirty_ssd_reserved_bytes, persisted_charge);
        assert_eq!(status.dirty_ssd_capacity_bytes, 10_000_000);
        assert_eq!(status.dirty_ssd_operations, 1);
        assert_eq!(status.local_bytes_completed, 7);
        assert!(status.oldest_pending_age_ms < 10_000);
        store.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn status_does_not_materialize_the_pending_journal() {
        let (store, _remote, _temp) = test_store().await;
        for sequence in 1..=32 {
            store
                .put(
                    &Path::from(format!("status-pending-{sequence}")),
                    Bytes::from_static(b"payload").into(),
                )
                .await
                .unwrap();
        }
        store.wait_local(32).await.unwrap();
        store.inner.remote.shutdown().await.unwrap();
        store.inner.journal.reset_snapshot_calls();

        let status = store.status().unwrap();

        assert_eq!(status.dirty_ssd_operations, 32);
        assert_eq!(store.inner.journal.snapshot_calls(), 0);
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
        assert_eq!(status.dirty_ssd_reserved_bytes, 0);
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
    async fn chained_local_update_preserves_the_published_predecessor_cas() {
        let (store, remote, _temp, controls) = test_store_with_controls(true).await;
        let path = Path::from("manifest");
        remote
            .put(&path, Bytes::from_static(b"remote-zero").into())
            .await
            .unwrap();
        let remote_zero = remote.head(&path).await.unwrap();
        controls.block_puts();

        let first = store
            .put_opts(
                &path,
                Bytes::from_static(b"local-one").into(),
                PutOptions::from(PutMode::Update(UpdateVersion {
                    e_tag: remote_zero.e_tag,
                    version: remote_zero.version,
                })),
            )
            .await
            .unwrap();
        store
            .put_opts(
                &path,
                Bytes::from_static(b"local-two").into(),
                PutOptions::from(PutMode::Update(first.into())),
            )
            .await
            .unwrap();
        store.wait_local(2).await.unwrap();

        tokio::time::timeout(Duration::from_secs(2), async {
            while controls.put_count() < 1 {
                controls.put_activity().notified().await;
            }
        })
        .await
        .expect("first remote update did not start");
        controls.release_put_path(path.as_ref());
        tokio::time::timeout(Duration::from_secs(2), async {
            while controls.put_count() < 2 {
                controls.put_activity().notified().await;
            }
        })
        .await
        .expect("chained remote update did not start");

        remote
            .put(&path, Bytes::from_static(b"external").into())
            .await
            .unwrap();
        controls.release_put_path(path.as_ref());

        let error = tokio::time::timeout(Duration::from_secs(1), store.wait_remote(2))
            .await
            .expect("remote CAS divergence must wake waiters promptly")
            .expect_err("remote CAS divergence must be terminal");
        assert!(error.to_string().contains("permanent remote divergence"));
        assert_eq!(
            remote.get(&path).await.unwrap().bytes().await.unwrap(),
            Bytes::from_static(b"external")
        );
        assert!(store.status().unwrap().terminal_error.is_some());
        controls.release_puts();
        let shutdown_error = store
            .shutdown()
            .await
            .expect_err("shutdown must preserve the remote durability failure");
        assert!(
            shutdown_error
                .to_string()
                .contains("permanent remote divergence")
        );
    }

    #[tokio::test]
    async fn permanent_remote_divergence_rejects_every_new_mutation_before_admission() {
        let (store, remote, _temp, controls) = test_store_with_controls(true).await;
        let path = Path::from("manifest");
        remote
            .put(&path, Bytes::from_static(b"remote-zero").into())
            .await
            .unwrap();
        let remote_zero = remote.head(&path).await.unwrap();
        controls.block_puts();
        let mut multipart_started_before_terminal = store
            .put_multipart(&Path::from("multipart-started-before-terminal"))
            .await
            .unwrap();

        let first = store
            .put_opts(
                &path,
                Bytes::from_static(b"local-one").into(),
                PutOptions::from(PutMode::Update(UpdateVersion {
                    e_tag: remote_zero.e_tag,
                    version: remote_zero.version,
                })),
            )
            .await
            .unwrap();
        store
            .put_opts(
                &path,
                Bytes::from_static(b"local-two").into(),
                PutOptions::from(PutMode::Update(first.into())),
            )
            .await
            .unwrap();
        store.wait_local(2).await.unwrap();

        tokio::time::timeout(Duration::from_secs(2), async {
            while controls.put_count() < 1 {
                controls.put_activity().notified().await;
            }
        })
        .await
        .expect("first remote update did not start");
        controls.release_put_path(path.as_ref());
        tokio::time::timeout(Duration::from_secs(2), async {
            while controls.put_count() < 2 {
                controls.put_activity().notified().await;
            }
        })
        .await
        .expect("chained remote update did not start");
        remote
            .put(&path, Bytes::from_static(b"external").into())
            .await
            .unwrap();
        controls.release_put_path(path.as_ref());
        tokio::time::timeout(Duration::from_secs(1), store.wait_remote(2))
            .await
            .expect("remote divergence must be detected promptly")
            .expect_err("remote divergence must be terminal");

        let accepted_before = store.status().unwrap().accepted_seq;
        let existing_multipart_error = multipart_started_before_terminal
            .put_part(Bytes::from_static(b"must-not-be-buffered").into())
            .await
            .expect_err("terminal writeback must reject parts on an existing multipart upload");
        assert!(
            existing_multipart_error
                .to_string()
                .contains("permanent remote divergence")
        );

        let put_error = tokio::time::timeout(
            Duration::from_secs(1),
            store.put(
                &Path::from("after-terminal-put"),
                Bytes::from_static(b"must-not-be-accepted").into(),
            ),
        )
        .await
        .expect("terminal put rejection must not block")
        .expect_err("terminal writeback must reject put");
        assert!(
            put_error
                .to_string()
                .contains("permanent remote divergence")
        );

        let delete_error = tokio::time::timeout(
            Duration::from_secs(1),
            store.delete(&Path::from("after-terminal-delete")),
        )
        .await
        .expect("terminal delete rejection must not block")
        .expect_err("terminal writeback must reject delete");
        assert!(
            delete_error
                .to_string()
                .contains("permanent remote divergence")
        );

        let copy_error = tokio::time::timeout(
            Duration::from_secs(1),
            store.copy_opts(
                &path,
                &Path::from("after-terminal-copy"),
                CopyOptions::default(),
            ),
        )
        .await
        .expect("terminal copy rejection must not block")
        .expect_err("terminal writeback must reject copy");
        assert!(
            copy_error
                .to_string()
                .contains("permanent remote divergence")
        );

        let rename_error = tokio::time::timeout(
            Duration::from_secs(1),
            store.rename_opts(
                &path,
                &Path::from("after-terminal-rename"),
                RenameOptions::default(),
            ),
        )
        .await
        .expect("terminal rename rejection must not block")
        .expect_err("terminal writeback must reject rename");
        assert!(
            rename_error
                .to_string()
                .contains("permanent remote divergence")
        );

        let multipart_error = match store
            .put_multipart(&Path::from("after-terminal-multipart"))
            .await
        {
            Ok(_) => panic!("terminal writeback must reject multipart initiation"),
            Err(error) => error,
        };
        assert!(
            multipart_error
                .to_string()
                .contains("permanent remote divergence")
        );

        assert_eq!(
            store.status().unwrap().accepted_seq,
            accepted_before,
            "terminal rejection must not allocate another sequence"
        );
        controls.release_puts();
        let shutdown_error = store
            .shutdown()
            .await
            .expect_err("shutdown must preserve the remote durability failure");
        assert!(
            shutdown_error
                .to_string()
                .contains("permanent remote divergence")
        );
    }

    #[tokio::test]
    async fn recovered_chained_update_without_a_migratable_predecessor_is_terminal() {
        let (store, remote, _temp, controls) = test_store_with_controls(true).await;
        let path = Path::from("manifest");
        remote
            .put(&path, Bytes::from_static(b"remote-zero").into())
            .await
            .unwrap();
        let remote_zero = remote.head(&path).await.unwrap();
        controls.block_puts();
        let first = store
            .put_opts(
                &path,
                Bytes::from_static(b"local-one").into(),
                PutOptions::from(PutMode::Update(UpdateVersion {
                    e_tag: remote_zero.e_tag,
                    version: remote_zero.version,
                })),
            )
            .await
            .unwrap();
        store
            .put_opts(
                &path,
                Bytes::from_static(b"local-two").into(),
                PutOptions::from(PutMode::Update(first.into())),
            )
            .await
            .unwrap();
        store.wait_local(2).await.unwrap();
        tokio::time::timeout(Duration::from_secs(2), async {
            while controls.put_count() < 1 {
                controls.put_activity().notified().await;
            }
        })
        .await
        .expect("first remote update did not start");
        controls.release_put_path(path.as_ref());
        tokio::time::timeout(Duration::from_secs(2), async {
            while controls.put_count() < 2 {
                controls.put_activity().notified().await;
            }
        })
        .await
        .expect("chained remote update did not reach the blocked backend");
        assert_eq!(store.inner.journal.progress().unwrap().remote_seq, 1);
        let settings = store.inner.settings.clone();
        let identity = store.inner.journal.snapshot().unwrap().identity;
        let journal_root = store.inner.journal.root().to_path_buf();
        store.shutdown().await.unwrap();
        drop(store);
        controls.release_puts();

        let database = redb::Database::create(journal_root.join("journal.redb")).unwrap();
        let transaction = database.begin_write().unwrap();
        assert!(
            transaction
                .delete_table(redb::TableDefinition::<&str, &[u8]>::new(
                    "remote_object_versions"
                ))
                .unwrap()
        );
        transaction.commit().unwrap();
        drop(database);

        let journal = Arc::new(Journal::open(&journal_root, identity).unwrap());
        let (recovery_remote, recovery_controls) = FaultStore::new(remote);
        let recovered = WritebackObjectStore::open_paused(recovery_remote, journal, settings)
            .await
            .unwrap();
        recovered.activate_remote().unwrap();

        let error = tokio::time::timeout(Duration::from_secs(1), recovered.wait_remote(2))
            .await
            .expect("missing predecessor metadata must not retry forever")
            .expect_err("missing predecessor metadata must be terminal");
        assert!(error.to_string().contains("predecessor ETag"));
        assert!(recovered.status().unwrap().terminal_error.is_some());
        assert_eq!(
            recovery_controls.put_count(),
            0,
            "recovery must fail before mutating the remote object"
        );
        let shutdown_error = recovered
            .shutdown()
            .await
            .expect_err("shutdown must preserve the missing predecessor failure");
        assert!(shutdown_error.to_string().contains("predecessor ETag"));
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
    async fn metadata_delete_is_rejected_when_its_journal_charge_exceeds_disk_capacity() {
        let (store, _remote, _temp, _controls) =
            test_store_with_disk_capacity(false, AckMode::Memory, ShutdownFlush::Local, 64).await;

        let error = store
            .delete(&Path::from(
                "metadata/delete/whose/record/exceeds/sixty-four/bytes",
            ))
            .await
            .unwrap_err();

        assert!(
            error.to_string().contains("capacity"),
            "metadata mutation bypassed SSD admission: {error}"
        );
        assert_eq!(store.status().unwrap().accepted_seq, 0);
        store.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn metadata_delete_charge_is_persisted_and_released_after_remote_completion() {
        let (store, remote, _temp, controls) = test_store_with_controls(false).await;
        let path = Path::from("metadata/delete/accounted");
        remote
            .put(&path, Bytes::from_static(b"remote").into())
            .await
            .unwrap();

        store.delete(&path).await.unwrap();
        store.wait_local(1).await.unwrap();
        let expected = MutationRecord::metadata_ssd_reservation(path.as_ref()).unwrap();
        assert_eq!(store.dirty_ssd_reserved_bytes(), expected);
        assert_eq!(
            store
                .inner
                .journal
                .snapshot()
                .unwrap()
                .dirty_metadata_reserved_bytes,
            expected
        );

        controls.partition_writes(false);
        store.wait_remote(1).await.unwrap();
        assert_eq!(store.dirty_ssd_reserved_bytes(), 0);
        store.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn rejected_oversized_update_does_not_consume_a_journal_sequence() {
        let (store, _remote, _temp) = test_store().await;
        let path = Path::from("oversized-update");
        let oversized_etag = "e".repeat(50_000);
        let local_etag: LocalEtag =
            bincode::deserialize(&bincode::serialize(&oversized_etag).unwrap()).unwrap();
        let visible_payload = VerifiedPayload::new(Bytes::from_static(b"old"));
        store
            .inner
            .overlay
            .install_verified_memory(
                MutationRecord {
                    format_version: 1,
                    sequence: u64::MAX,
                    operation_id: Uuid::nil(),
                    path: path.to_string(),
                    kind: MutationKind::Put {
                        mode: MutationMode::Overwrite,
                        expected_visible_version: None,
                        payload_len: visible_payload.byte_len(),
                        payload_sha256: visible_payload.sha256(),
                        blob_path: String::new(),
                    },
                    local_etag,
                    accepted_at_unix_ms: 0,
                    remote_predecessor_etag: None,
                    remote_result_etag: None,
                    fence: FenceClass::Fence,
                    retry_count: 0,
                    last_error: None,
                },
                visible_payload,
            )
            .await
            .unwrap();

        let error = store
            .put_opts(
                &path,
                Bytes::from_static(b"new").into(),
                PutOptions::from(PutMode::Update(UpdateVersion {
                    e_tag: Some(oversized_etag),
                    version: None,
                })),
            )
            .await
            .expect_err("oversized foreground version metadata must be rejected");
        assert!(
            error.to_string().contains("bounded SSD reservation"),
            "{error}"
        );
        assert_eq!(store.status().unwrap().accepted_seq, 0);

        store
            .put(
                &Path::from("ordinary-after-rejection"),
                Bytes::from_static(b"healthy").into(),
            )
            .await
            .unwrap();
        store.wait_local(1).await.unwrap();
        let status = store.status().unwrap();
        assert_eq!(status.accepted_seq, 1);
        assert_eq!(status.local_seq, 1);
        assert!(status.terminal_error.is_none());
        store.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn legacy_oversized_version_metadata_recovers_and_releases_its_exact_reservation() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("writeback");
        let identity = JournalIdentity {
            format_version: 1,
            bucket_id: "bucket-a".to_owned(),
            backend_endpoint: "memory://remote".to_owned(),
            database_prefix: "zerofs/pilot".to_owned(),
            backend_kind: "memory".to_owned(),
            encryption_key_identity_sha256: [0x77; 32],
        };
        let journal = Arc::new(Journal::open(&root, identity).unwrap());
        let payload = b"legacy";
        let verified = VerifiedPayload::new(Bytes::from_static(payload));
        let record = MutationRecord {
            format_version: 1,
            sequence: 1,
            operation_id: Uuid::nil(),
            path: "legacy/oversized-version".to_owned(),
            kind: MutationKind::Put {
                mode: MutationMode::Overwrite,
                expected_visible_version: Some("e".repeat(50_000)),
                payload_len: verified.byte_len(),
                payload_sha256: verified.sha256(),
                blob_path: String::new(),
            },
            local_etag: LocalEtag::new(Uuid::nil(), 1),
            accepted_at_unix_ms: 0,
            remote_predecessor_etag: Some("p".repeat(50_000)),
            remote_result_etag: None,
            fence: FenceClass::Fence,
            retry_count: 0,
            last_error: None,
        };
        let expected_reservation = record.ssd_reservation_bytes().unwrap();
        let maximum_result = "r".repeat(MutationRecord::MAX_PERSISTED_VERSION_BYTES);
        let mut marked_record = record.clone();
        marked_record.remote_result_etag = Some(maximum_result.clone());
        marked_record.last_error = Some("\u{10ffff}".repeat(2_048));
        let post_mark_footprint = verified.byte_len()
            + bincode::serialized_size(&marked_record).unwrap()
            + 16
            + marked_record.path.len() as u64
            + bincode::serialized_size(&(marked_record.sequence, maximum_result.clone())).unwrap()
            + 12;
        assert!(expected_reservation >= post_mark_footprint);
        journal.commit_put(record, payload).unwrap();
        let settings = WritebackSettings {
            dir: root,
            ack_mode: AckMode::Memory,
            memory_bytes: 1_000_000,
            disk_bytes: 1_000_000,
            min_free_bytes: 1,
            high_watermark_percent: 95,
            resume_percent: 85,
            upload_concurrency: 1,
            local_concurrency: 1,
            shutdown_flush: ShutdownFlush::Local,
        };

        let (remote, controls) = FaultStore::new(Arc::new(InMemory::new()));
        controls.force_put_etag(maximum_result);
        let recovered = WritebackObjectStore::open_paused(remote, journal, settings)
            .await
            .expect("legacy oversized record must remain recoverable");
        let recovered_reservation = recovered.dirty_ssd_reserved_bytes();
        assert_eq!(recovered_reservation, expected_reservation);

        recovered.activate_remote().unwrap();
        recovered.wait_remote(1).await.unwrap();
        assert_eq!(recovered.dirty_ssd_reserved_bytes(), 0);
        recovered.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn empty_payload_storm_backpressures_before_exceeding_the_dirty_ssd_budget() {
        const DISK_BUDGET: u64 = 50_000;
        const PUT_COUNT: u64 = 32;
        let (store, _remote, _temp, _controls) = test_store_with_disk_capacity(
            false,
            AckMode::Memory,
            ShutdownFlush::Local,
            DISK_BUDGET,
        )
        .await;
        let mut puts = tokio::task::JoinSet::new();
        for index in 0..PUT_COUNT {
            let store = store.clone();
            puts.spawn(async move {
                store
                    .put(
                        &Path::from(format!("empty-payload-storm/{index:02}")),
                        Bytes::new().into(),
                    )
                    .await
            });
        }

        let completed = tokio::time::timeout(Duration::from_secs(2), async {
            while puts.join_next().await.is_some() {}
        })
        .await;
        assert!(
            completed.is_err(),
            "all empty puts bypassed dirty-SSD admission"
        );
        puts.abort_all();
        while puts.join_next().await.is_some() {}

        let accepted = store.status().unwrap().accepted_seq;
        assert!(accepted > 0 && accepted < PUT_COUNT);
        store.wait_local(accepted).await.unwrap();
        let snapshot = store.inner.journal.snapshot().unwrap();
        let actual_persisted_bytes = snapshot
            .records
            .iter()
            .map(|record| {
                let payload_len = record.payload().map_or(0, |(len, _)| len);
                payload_len + bincode::serialized_size(record).unwrap() + 12
            })
            .sum::<u64>();
        assert!(actual_persisted_bytes <= DISK_BUDGET);
        assert_eq!(
            store.dirty_ssd_reserved_bytes(),
            snapshot
                .records
                .iter()
                .map(|record| record.ssd_reservation_bytes().unwrap())
                .sum::<u64>()
        );
        assert!(store.dirty_ssd_reserved_bytes() <= DISK_BUDGET);
        store.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn payload_charge_survives_restart_and_remote_release_exactly() {
        let (store, remote, _temp, _controls) =
            test_store_with_disk_capacity(false, AckMode::Memory, ShutdownFlush::Local, 50_000)
                .await;
        let path = Path::from("tiny-payload-restart");
        store
            .put(&path, Bytes::from_static(b"x").into())
            .await
            .unwrap();
        store.wait_local(1).await.unwrap();
        let accepted_charge = store.dirty_ssd_reserved_bytes();
        assert!(accepted_charge > 1, "payload metadata was not charged");
        let settings = store.inner.settings.clone();
        let identity = store.inner.journal.snapshot().unwrap().identity;
        let journal_root = store.inner.journal.root().to_path_buf();
        store.shutdown().await.unwrap();
        drop(store);

        let journal = Arc::new(Journal::open(&journal_root, identity).unwrap());
        let recovered = WritebackObjectStore::open_paused(remote, journal, settings)
            .await
            .unwrap();
        assert_eq!(recovered.dirty_ssd_reserved_bytes(), accepted_charge);

        recovered.activate_remote().unwrap();
        recovered.wait_remote(1).await.unwrap();
        assert_eq!(recovered.dirty_ssd_reserved_bytes(), 0);
        assert!(recovered.status().unwrap().terminal_error.is_none());
        recovered
            .put(
                &Path::from("tiny-payload-after-release"),
                Bytes::from_static(b"x").into(),
            )
            .await
            .unwrap();
        recovered.wait_remote(2).await.unwrap();
        assert_eq!(recovered.dirty_ssd_reserved_bytes(), 0);
        recovered.shutdown().await.unwrap();
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
        let persisted_charge = store
            .inner
            .journal
            .snapshot()
            .unwrap()
            .records
            .iter()
            .map(|record| record.ssd_reservation_bytes().unwrap())
            .sum::<u64>();
        assert_eq!(store.dirty_ssd_reserved_bytes(), persisted_charge);
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
                    .put_opts(
                        &path,
                        Bytes::from_static(b"payload").into(),
                        PutOptions::from(PutMode::Create),
                    )
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
            local_concurrency: 4,
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
        let persisted_charge = recovered
            .inner
            .journal
            .snapshot()
            .unwrap()
            .records
            .iter()
            .map(|record| record.ssd_reservation_bytes().unwrap())
            .sum::<u64>();
        assert_eq!(recovered.dirty_ssd_reserved_bytes(), persisted_charge);
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
    async fn copy_does_not_hold_key_locks_while_waiting_for_admission() {
        let (store, remote, _temp) = test_store().await;
        let source = Path::from("copy-admission-source");
        let target = Path::from("copy-admission-target");
        let payload = Bytes::from(vec![0x5a; 1_000_000]);
        remote.put(&source, payload.clone().into()).await.unwrap();

        let ram = store
            .inner
            .admission
            .reserve(payload.len() as u64)
            .await
            .unwrap()
            .accept();
        let disk_charge =
            MutationRecord::ssd_reservation_estimate(target.as_ref(), None, payload.len() as u64)
                .unwrap();
        let disk = store.reserve_ssd(disk_charge).await.unwrap();
        let target_blocker = store.key_lock(&target).lock_owned().await;

        let copy = tokio::spawn({
            let store = store.clone();
            let source = source.clone();
            let target = target.clone();
            async move {
                store
                    .copy_opts(
                        &source,
                        &target,
                        CopyOptions {
                            mode: CopyMode::Create,
                            ..CopyOptions::default()
                        },
                    )
                    .await
            }
        });
        tokio::task::yield_now().await;

        let put = tokio::spawn({
            let store = store.clone();
            let target = target.clone();
            let payload = payload.clone();
            async move {
                store
                    .owned_put(target, payload, PutOptions::default(), ram, Some(disk))
                    .await
            }
        });
        tokio::task::yield_now().await;
        drop(target_blocker);

        let (put_result, copy_result) = tokio::time::timeout(Duration::from_secs(2), async {
            (put.await.unwrap(), copy.await.unwrap())
        })
        .await
        .expect("copy held its key locks while admission was owned by a waiting put");
        put_result.unwrap();
        assert!(matches!(
            copy_result,
            Err(object_store::Error::AlreadyExists { .. })
        ));
        assert_eq!(
            store.get(&target).await.unwrap().bytes().await.unwrap(),
            payload
        );
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
    async fn memory_part_is_reserved_before_buffer_copy() {
        let admission = Admission::new(5);
        let (payload, reservation) = super::reserve_memory_multipart_payload(
            &admission,
            5,
            [Bytes::from_static(b"abc"), Bytes::from_static(b"de")]
                .into_iter()
                .collect(),
        )
        .await
        .unwrap();
        assert_eq!(payload.into_iter().count(), 2);
        drop(reservation);

        let (store, _remote, _temp) =
            test_store_with_resource_limits(AckMode::Memory, 4, 1_000_000, 100).await;
        let mut upload = store
            .put_multipart(&Path::from("memory-admission-before-copy"))
            .await
            .unwrap();
        let error = upload
            .put_part(
                [Bytes::from_static(b"abc"), Bytes::from_static(b"de")]
                    .into_iter()
                    .collect(),
            )
            .await
            .unwrap_err();
        // The aggregate RAM-budget guard rejects before the gate is even
        // asked, which also means no buffer copy happened.
        assert!(
            error.to_string().contains("exceeds the dirty RAM budget"),
            "unexpected admission error: {error}"
        );
        assert_eq!(store.inner.admission.used_bytes(), 0);
        assert_eq!(store.inner.admission.used_operations(), 0);
        assert!(!store.inner.settings.dir.join("tmp/multipart").exists());
        upload.abort().await.unwrap();
        store.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn memory_multipart_over_ram_budget_fails_fast_instead_of_self_deadlocking() {
        let (store, _remote, _temp) =
            test_store_with_resource_limits(AckMode::Memory, 8, 1_000_000, 100).await;
        let mut upload = store
            .put_multipart(&Path::from("memory-over-budget"))
            .await
            .unwrap();
        upload
            .put_part(Bytes::from(vec![0x5a; 5]).into())
            .await
            .unwrap();

        // The second part fits the gate individually but pushes the object's
        // held total past it; without the aggregate check it would wait on
        // bytes only its own upload's completion can free.
        let error = tokio::time::timeout(
            Duration::from_secs(5),
            upload.put_part(Bytes::from(vec![0x5b; 5]).into()),
        )
        .await
        .expect("over-budget part must fail fast, not wait on its own bytes")
        .unwrap_err();
        assert!(
            error.to_string().contains("exceeds the dirty RAM budget"),
            "unexpected error: {error}"
        );
        upload.abort().await.unwrap();
        assert_eq!(store.inner.admission.used_bytes(), 0);
        store.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn ssd_part_is_reserved_before_staging_file_create() {
        let (store, _remote, _temp) =
            test_store_with_resource_limits(AckMode::Ssd, 4, 128, 100).await;
        let mut upload = store
            .put_multipart(&Path::from("ssd-admission-before-file"))
            .await
            .unwrap();
        let staging_root = store.inner.settings.dir.join("tmp/multipart");
        assert!(std::fs::read_dir(&staging_root).unwrap().next().is_none());
        let error = upload
            .put_part(Bytes::from(vec![0x5a; 64]).into())
            .await
            .unwrap_err();
        assert!(
            error.to_string().contains("capacity is 128 bytes"),
            "unexpected admission error: {error}"
        );
        assert!(std::fs::read_dir(&staging_root).unwrap().next().is_none());
        assert_eq!(store.inner.ssd.used_bytes(), 0);
        upload.abort().await.unwrap();
        assert!(std::fs::read_dir(staging_root).unwrap().next().is_none());
        store.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn idle_ssd_multipart_uploads_do_not_create_per_upload_staging() {
        let (store, _remote, _temp) = test_ssd_store().await;
        let staging_root = store.inner.settings.dir.join("tmp/multipart");
        let mut uploads = Vec::new();
        for index in 0..128 {
            uploads.push(
                store
                    .put_multipart(&Path::from(format!("idle-multipart-{index}")))
                    .await
                    .unwrap(),
            );
        }
        assert!(std::fs::read_dir(&staging_root).unwrap().next().is_none());
        for mut upload in uploads {
            upload.abort().await.unwrap();
        }
        assert!(std::fs::read_dir(&staging_root).unwrap().next().is_none());
        store.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn multipart_completion_releases_staging_after_local_commit() {
        let (store, _remote, _temp) =
            test_store_with_resource_limits(AckMode::Ssd, 4, 1_000_000, 100).await;
        let location = Path::from("staging-and-journal-coexist");
        let mut upload = store.put_multipart(&location).await.unwrap();
        upload
            .put_part(Bytes::from_static(b"payload").into())
            .await
            .unwrap();
        let journal_bytes =
            MutationRecord::ssd_reservation_estimate(location.as_ref(), None, 7).unwrap();
        let before = store.inner.ssd.snapshot();
        assert_eq!(before.used_ssd_bytes, 7 + journal_bytes);
        assert_eq!(before.outstanding_physical_claims, 7 + journal_bytes);
        assert_eq!(before.used_operations, 2);
        let staging_root = store.inner.settings.dir.join("tmp/multipart");
        assert_eq!(std::fs::read_dir(&staging_root).unwrap().count(), 1);

        upload.complete().await.unwrap();

        assert!(std::fs::read_dir(&staging_root).unwrap().next().is_none());
        let after = store.inner.ssd.snapshot();
        assert_eq!(after.used_ssd_bytes, journal_bytes);
        assert_eq!(after.outstanding_physical_claims, journal_bytes);
        assert_eq!(after.used_operations, 1);
        assert_eq!(store.inner.journal.snapshot().unwrap().local_seq, 1);
        store.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn ssd_multipart_larger_than_ram_completes_without_full_ram_materialization() {
        const RAM_BYTES: u64 = 64 * 1024;
        const PAYLOAD_BYTES: usize = 2 * 1024 * 1024 + 17;
        let (store, _remote, _temp) =
            test_store_with_resource_limits(AckMode::Ssd, RAM_BYTES, 8 * 1024 * 1024, 100).await;
        let location = Path::from("larger-than-ram");
        let payload = Bytes::from(vec![0x5a; PAYLOAD_BYTES]);
        let mut upload = store.put_multipart(&location).await.unwrap();
        upload.put_part(payload.clone().into()).await.unwrap();
        upload.complete().await.unwrap();
        assert_eq!(store.inner.admission.used_bytes(), 0);
        assert_eq!(
            store.get(&location).await.unwrap().bytes().await.unwrap(),
            payload
        );
        store.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn empty_ssd_multipart_completes_without_leaking_staging_or_claims() {
        let (store, _remote, _temp) = test_ssd_store().await;
        let location = Path::from("empty-multipart");
        let mut upload = store.put_multipart(&location).await.unwrap();
        let staging_root = store.inner.settings.dir.join("tmp/multipart");
        assert!(std::fs::read_dir(&staging_root).unwrap().next().is_none());

        upload.complete().await.unwrap();

        assert!(std::fs::read_dir(&staging_root).unwrap().next().is_none());
        assert_eq!(
            store.get(&location).await.unwrap().bytes().await.unwrap(),
            Bytes::new()
        );
        let snapshot = store.inner.ssd.snapshot();
        let journal_bytes =
            MutationRecord::ssd_reservation_estimate(location.as_ref(), None, 0).unwrap();
        assert_eq!(snapshot.used_ssd_bytes, journal_bytes);
        assert_eq!(snapshot.outstanding_physical_claims, journal_bytes);
        assert_eq!(snapshot.used_operations, 1);
        store.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn zero_length_multipart_parts_hit_operation_cap() {
        let (store, _remote, _temp) =
            test_store_with_resource_limits(AckMode::Ssd, 4, 1_000_000, 2).await;
        let mut upload = store
            .put_multipart(&Path::from("zero-length-operation-cap"))
            .await
            .unwrap();
        upload.put_part(Bytes::new().into()).await.unwrap();
        assert_eq!(store.inner.ssd.used_operations(), 2);

        let second = tokio::spawn(upload.put_part(Bytes::new().into()));
        tokio::task::yield_now().await;
        assert!(!second.is_finished());
        tokio::time::timeout(Duration::from_secs(1), upload.abort())
            .await
            .expect("abort did not cancel an operation-cap waiter")
            .unwrap();
        assert!(second.await.unwrap().is_err());
        assert_eq!(store.inner.ssd.used_operations(), 0);
        assert_eq!(store.inner.ssd.used_bytes(), 0);
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
            .open(staging.join("payload.staged"))
            .unwrap()
            .set_len(1)
            .unwrap();

        assert!(upload.complete().await.is_err());
        assert_eq!(std::fs::read_dir(&staging_root).unwrap().count(), 0);
        assert!(store.get(&location).await.is_err());
        assert_eq!(store.inner.journal.snapshot().unwrap().local_seq, 0);
        assert_eq!(store.inner.ssd.used_bytes(), 0);
        assert_eq!(store.inner.ssd.used_operations(), 0);
        assert_eq!(store.inner.ssd.outstanding_physical_claims(), 0);
        store
            .put(
                &Path::from("after-corrupt-multipart"),
                Bytes::from_static(b"healthy").into(),
            )
            .await
            .expect("multipart verification failure poisoned SSD admission");
        store.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn corruption_after_overlay_install_removes_pending_staged_entry() {
        let (store, _remote, _temp) = test_ssd_store().await;
        let location = Path::from("corrupt-after-overlay-install");
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
        let pause = store.pause_next_journal_submit();
        let complete = tokio::spawn(async move { upload.complete().await });
        pause.entered.notified().await;
        std::fs::write(staging.join("payload.staged"), b"changed").unwrap();
        pause.release.notify_one();

        assert!(complete.await.unwrap().is_err());
        assert!(store.get(&location).await.is_err());
        assert_eq!(store.inner.journal.snapshot().unwrap().local_seq, 0);
        assert!(std::fs::read_dir(&staging_root).unwrap().next().is_none());
        assert_eq!(store.inner.ssd.used_bytes(), 0);
        assert_eq!(store.inner.ssd.used_operations(), 0);
        assert_eq!(store.inner.ssd.outstanding_physical_claims(), 0);
        let _ = store.shutdown().await;
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
        let part = staging.join("payload.staged");
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
        assert_eq!(store.dirty_ssd_reserved_bytes(), 0);
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
                    .put_opts(
                        &Path::from(format!(
                            "zerofs/pilot/segments/{index:02x}/0000000000000001/{index:016x}"
                        )),
                        Bytes::from(vec![index; 1024]).into(),
                        PutOptions::from(PutMode::Create),
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
    async fn segment_store_seals_fill_the_configured_remote_upload_slots() {
        let (store, _remote, _temp, controls) = test_store_with_controls(true).await;
        controls.block_puts();
        let writeback: Arc<dyn ObjectStore> = Arc::new(store.clone());
        let prefixed: Arc<dyn ObjectStore> =
            Arc::new(PrefixStore::new(writeback, Path::from("zerofs/pilot")));
        let segments = Arc::new(SegmentStore::new(
            prefixed,
            FrameCodec::new(&[1u8; 32], SEGMENT_INFO, CompressionConfig::Lz4),
            7,
            None,
        ));

        let seals = (0..4).map(|index| {
            let segments = segments.clone();
            async move {
                let segid = segments.next_segid();
                segments
                    .put_segment(segid, Bytes::from(vec![index; 1024]))
                    .await
                    .unwrap();
            }
        });
        futures::future::join_all(seals).await;
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
            "the production segment path never filled four remote upload slots"
        );
    }

    #[tokio::test]
    async fn generated_segment_create_does_not_wait_for_remote_head() {
        let (store, _remote, _temp, controls) = test_store_with_controls(true).await;
        controls.block_heads();
        let writeback: Arc<dyn ObjectStore> = Arc::new(store.clone());
        let prefixed: Arc<dyn ObjectStore> =
            Arc::new(PrefixStore::new(writeback, Path::from("zerofs/pilot")));
        let segments = SegmentStore::new(
            prefixed,
            FrameCodec::new(&[1u8; 32], SEGMENT_INFO, CompressionConfig::Lz4),
            7,
            None,
        );

        let accepted = tokio::time::timeout(
            Duration::from_secs(1),
            segments.put_segment(segments.next_segid(), Bytes::from_static(b"segment")),
        )
        .await;
        controls.release_heads();
        store.wait_local(1).await.unwrap();
        store.shutdown().await.unwrap();

        accepted
            .expect("local admission waited for a remote HEAD")
            .expect("generated segment create failed");
        assert_eq!(controls.head_count(), 0);
    }

    #[tokio::test]
    async fn remote_replay_does_not_materialize_the_complete_backlog() {
        let (store, _remote, _temp, controls) = test_store_with_controls(true).await;
        controls.block_puts();
        let puts = (0..16).map(|index| {
            let store = store.clone();
            async move {
                store
                    .put_opts(
                        &Path::from(format!(
                            "zerofs/pilot/segments/{index:02x}/0000000000000001/{index:016x}"
                        )),
                        Bytes::from(vec![index; 1024]).into(),
                        PutOptions::from(PutMode::Create),
                    )
                    .await
                    .unwrap();
            }
        });
        futures::future::join_all(puts).await;
        store.wait_local(16).await.unwrap();
        tokio::time::timeout(Duration::from_secs(2), async {
            while controls.max_active_puts() < 4 {
                controls.put_activity().notified().await;
            }
        })
        .await
        .expect("remote replay did not fill the upload pipeline");
        store.inner.journal.reset_snapshot_calls();

        controls.release_puts();
        store.wait_remote(16).await.unwrap();

        assert_eq!(store.inner.journal.snapshot_calls(), 0);
        store.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn remote_replay_refills_a_slot_before_the_slowest_wave_member_finishes() {
        let (store, _remote, _temp, controls) = test_store_with_controls(true).await;
        controls.block_puts();
        for index in 1_u8..=8 {
            store
                .put_opts(
                    &Path::from(format!(
                        "zerofs/pilot/segments/{index:02x}/0000000000000001/{index:016x}"
                    )),
                    Bytes::from(vec![index; 1024]).into(),
                    PutOptions::from(PutMode::Create),
                )
                .await
                .unwrap();
        }
        store.wait_local(8).await.unwrap();

        tokio::time::timeout(Duration::from_secs(2), async {
            while controls.put_count() < 4 {
                controls.put_activity().notified().await;
            }
        })
        .await
        .expect("the first four remote slots were not filled");
        controls.release_put_path("zerofs/pilot/segments/02/0000000000000001/0000000000000002");

        tokio::time::timeout(Duration::from_secs(2), async {
            while !controls
                .put_paths()
                .iter()
                .any(|path| path == "zerofs/pilot/segments/05/0000000000000001/0000000000000005")
            {
                controls.put_activity().notified().await;
            }
        })
        .await
        .expect("a free remote slot waited for the rest of its batch");

        controls.release_puts();
        store.wait_remote(8).await.unwrap();
        store.shutdown().await.unwrap();
    }

    /// When uploads complete out of order and pile up behind the ordering
    /// frontier, the scheduler commits the whole contiguous run through one
    /// durable journal transaction instead of paying one fsync'd transaction
    /// per record. Releasing the frontier last makes the run deterministic:
    /// sequences 2..=6 are already held completions by the time sequence 1
    /// lands, so the drain must be a single batched watermark commit.
    #[tokio::test]
    async fn remote_commits_coalesce_held_completions_into_one_watermark_transaction() {
        let (store, remote, _temp, controls) = test_store_with_controls(true).await;
        let journal = store.inner.journal.clone();
        controls.block_puts();
        let paths = (1_u8..=6)
            .map(|sequence| {
                Path::from(format!(
                    "zerofs/pilot/segments/{sequence:02x}/0000000000000001/{sequence:016x}"
                ))
            })
            .collect::<Vec<_>>();
        for (index, path) in paths.iter().enumerate() {
            store
                .put_opts(
                    path,
                    Bytes::from(vec![index as u8 + 1; 1024]).into(),
                    PutOptions::from(PutMode::Create),
                )
                .await
                .unwrap();
        }
        store.wait_local(6).await.unwrap();

        for path in paths[1..].iter().rev() {
            controls.release_put_path(path.as_ref());
        }
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let published =
                    futures::future::join_all(paths[1..].iter().map(|path| remote.head(path)))
                        .await;
                if published.iter().all(Result::is_ok) {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("speculative uploads behind the frontier never completed");
        assert_eq!(
            journal.progress().unwrap().remote_seq,
            0,
            "no watermark may advance while the ordering frontier is unpublished"
        );
        let transactions_before = journal.remote_watermark_commit_count();

        controls.release_put_path(paths[0].as_ref());
        store.wait_remote(6).await.unwrap();

        assert_eq!(
            journal.remote_watermark_commit_count() - transactions_before,
            1,
            "six held completions must drain through one batched watermark commit"
        );
        assert_eq!(journal.progress().unwrap().remote_seq, 6);
        controls.release_puts();
        store.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn remote_replay_keeps_polling_and_refilling_while_ordered_cleanup_is_blocked() {
        let (store, remote, _temp, controls) = test_store_with_controls(true).await;
        let journal = store.inner.journal.clone();
        let pause = journal.pause_remote_mark(1);
        controls.block_puts();
        let paths = (1_u8..=6)
            .map(|sequence| {
                Path::from(format!(
                    "zerofs/pilot/segments/{sequence:02x}/0000000000000001/{sequence:016x}"
                ))
            })
            .collect::<Vec<_>>();
        for (sequence, path) in paths.iter().enumerate() {
            store
                .put_opts(
                    path,
                    Bytes::from(vec![sequence as u8 + 1; 1024]).into(),
                    PutOptions::from(PutMode::Create),
                )
                .await
                .unwrap();
        }
        store.wait_local(6).await.unwrap();

        tokio::time::timeout(Duration::from_secs(2), async {
            while controls.put_count() < 4 {
                controls.put_activity().notified().await;
            }
        })
        .await
        .expect("the first four remote slots were not filled");
        controls.release_put_path(paths[0].as_ref());
        tokio::time::timeout(Duration::from_secs(2), pause.wait_entered())
            .await
            .expect("the first ordered remote commit did not begin");
        assert_eq!(journal.progress().unwrap().remote_seq, 0);

        controls.release_put_path(paths[1].as_ref());
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if controls
                    .put_paths()
                    .iter()
                    .any(|path| path == paths[5].as_ref())
                {
                    break;
                }
                controls.put_activity().notified().await;
            }
        })
        .await
        .expect("a free remote slot was not refilled during ordered cleanup");
        for path in &paths[2..] {
            controls.release_put_path(path.as_ref());
        }
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                let published =
                    futures::future::join_all(paths[1..].iter().map(|path| remote.head(path)))
                        .await;
                if published.iter().all(Result::is_ok) {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("later remote operations stopped progressing during ordered cleanup");
        assert_eq!(
            journal.progress().unwrap().remote_seq,
            0,
            "remote durability must still publish strictly in sequence"
        );

        pause.release();
        controls.release_puts();
        tokio::time::timeout(Duration::from_secs(2), store.wait_remote(6))
            .await
            .expect("ordered remote commits did not catch up after cleanup resumed")
            .unwrap();
        store.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn remote_shutdown_joins_an_in_progress_ordered_commit() {
        let (store, _remote, _temp, controls) = test_store_with_controls(true).await;
        let journal = store.inner.journal.clone();
        let pause = journal.pause_remote_mark(1);
        controls.block_puts();
        let path = Path::from("shutdown/in-progress");
        store
            .put(&path, Bytes::from_static(b"payload").into())
            .await
            .unwrap();
        store.wait_local(1).await.unwrap();
        tokio::time::timeout(Duration::from_secs(2), async {
            while controls.put_count() == 0 {
                controls.put_activity().notified().await;
            }
        })
        .await
        .expect("the remote upload did not start");
        controls.release_put_path(path.as_ref());
        tokio::time::timeout(Duration::from_secs(2), pause.wait_entered())
            .await
            .expect("the ordered remote commit did not begin");

        let mut shutdown = tokio::spawn({
            let store = store.clone();
            async move { store.shutdown().await }
        });
        assert!(
            tokio::time::timeout(Duration::from_millis(100), &mut shutdown)
                .await
                .is_err(),
            "shutdown returned while ordered commit work was still running"
        );
        pause.release();
        tokio::time::timeout(Duration::from_secs(2), &mut shutdown)
            .await
            .expect("shutdown did not join the resumed ordered commit")
            .unwrap()
            .unwrap();
        assert_eq!(journal.progress().unwrap().remote_seq, 1);
    }

    #[tokio::test]
    async fn canceled_remote_shutdown_does_not_lose_shutdown_ownership() {
        let (store, _remote, _temp, controls) = test_store_with_controls(true).await;
        let journal = store.inner.journal.clone();
        let pause = journal.pause_remote_mark(1);
        controls.block_puts();
        let path = Path::from("shutdown/canceled");
        store
            .put(&path, Bytes::from_static(b"payload").into())
            .await
            .unwrap();
        store.wait_local(1).await.unwrap();
        tokio::time::timeout(Duration::from_secs(2), async {
            while controls.put_count() == 0 {
                controls.put_activity().notified().await;
            }
        })
        .await
        .expect("the remote upload did not start");
        controls.release_put_path(path.as_ref());
        tokio::time::timeout(Duration::from_secs(2), pause.wait_entered())
            .await
            .expect("the ordered remote commit did not begin");

        let first_shutdown = tokio::spawn({
            let scheduler = store.inner.remote.clone();
            async move { scheduler.shutdown().await }
        });
        tokio::time::timeout(Duration::from_secs(2), async {
            while !store.inner.remote.shutdown_requested() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the first caller did not request remote shutdown");
        assert!(!first_shutdown.is_finished());
        first_shutdown.abort();
        assert!(first_shutdown.await.unwrap_err().is_cancelled());

        let mut second_shutdown = tokio::spawn({
            let scheduler = store.inner.remote.clone();
            async move { scheduler.shutdown().await }
        });
        assert!(
            tokio::time::timeout(Duration::from_millis(100), &mut second_shutdown)
                .await
                .is_err(),
            "a replacement caller returned before the shared shutdown finished"
        );
        pause.release();
        tokio::time::timeout(Duration::from_secs(2), &mut second_shutdown)
            .await
            .expect("replacement shutdown caller did not observe completion")
            .unwrap()
            .unwrap();
        assert_eq!(journal.progress().unwrap().remote_seq, 1);
        store.inner.journaler.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn shutdown_preserves_a_staged_remote_terminal_error() {
        let (store, remote, _temp, controls) = test_store_with_controls(true).await;
        let terminal_pause = store.inner.remote.pause_terminal_publication();
        let path = Path::from("manifest");
        remote
            .put(&path, Bytes::from_static(b"remote-zero").into())
            .await
            .unwrap();
        let remote_zero = remote.head(&path).await.unwrap();
        controls.block_puts();
        let first = store
            .put_opts(
                &path,
                Bytes::from_static(b"local-one").into(),
                PutOptions::from(PutMode::Update(UpdateVersion {
                    e_tag: remote_zero.e_tag,
                    version: remote_zero.version,
                })),
            )
            .await
            .unwrap();
        store
            .put_opts(
                &path,
                Bytes::from_static(b"local-two").into(),
                PutOptions::from(PutMode::Update(first.into())),
            )
            .await
            .unwrap();
        store.wait_local(2).await.unwrap();
        tokio::time::timeout(Duration::from_secs(2), async {
            while controls.put_count() < 1 {
                controls.put_activity().notified().await;
            }
        })
        .await
        .expect("first remote update did not start");
        controls.release_put_path(path.as_ref());
        tokio::time::timeout(Duration::from_secs(2), async {
            while controls.put_count() < 2 {
                controls.put_activity().notified().await;
            }
        })
        .await
        .expect("chained remote update did not start");
        remote
            .put(&path, Bytes::from_static(b"external").into())
            .await
            .unwrap();
        controls.release_put_path(path.as_ref());
        tokio::time::timeout(Duration::from_secs(2), terminal_pause.wait_entered())
            .await
            .expect("terminal remote error was not staged");

        let shutdown = tokio::spawn({
            let scheduler = store.inner.remote.clone();
            async move { scheduler.shutdown().await }
        });
        tokio::time::timeout(Duration::from_secs(2), async {
            while !store.inner.remote.shutdown_requested() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the concurrent stop was not delivered");
        terminal_pause.release();
        let shutdown_error = tokio::time::timeout(Duration::from_secs(2), shutdown)
            .await
            .expect("remote shutdown did not finish")
            .unwrap()
            .expect_err("staged durability error must win over shutdown");
        assert!(
            shutdown_error
                .to_string()
                .contains("permanent remote divergence")
        );
        let barrier_error = store
            .wait_remote(2)
            .await
            .expect_err("staged durability error must remain observable");
        assert!(
            barrier_error
                .to_string()
                .contains("permanent remote divergence")
        );
        controls.release_puts();
        store.inner.journaler.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn remote_replay_preuploads_immutable_objects_across_later_fences() {
        let (store, _remote, _temp, controls) = test_store_with_controls(true).await;
        controls.block_puts();
        let records = [
            ("manifest/one".to_owned(), 1_u8),
            (
                "zerofs/pilot/segments/01/0000000000000001/0000000000000001".to_owned(),
                2,
            ),
            ("manifest/two".to_owned(), 3),
            (
                "zerofs/pilot/segments/02/0000000000000001/0000000000000002".to_owned(),
                4,
            ),
            ("manifest/three".to_owned(), 5),
            (
                "zerofs/pilot/segments/03/0000000000000001/0000000000000003".to_owned(),
                6,
            ),
            ("manifest/four".to_owned(), 7),
        ];
        for (path, byte) in &records {
            let mode = if path.contains("/segments/") {
                PutMode::Create
            } else {
                PutMode::Overwrite
            };
            store
                .put_opts(
                    &Path::from(path.as_str()),
                    Bytes::from(vec![*byte; 1024]).into(),
                    PutOptions::from(mode),
                )
                .await
                .unwrap();
        }
        store.wait_local(7).await.unwrap();

        tokio::time::timeout(Duration::from_secs(2), async {
            while controls.put_count() < 4 {
                controls.put_activity().notified().await;
            }
        })
        .await
        .expect("remote replay did not fill the four upload slots across fences");

        let mut started = controls.put_paths();
        started.sort();
        let mut expected = vec![
            "manifest/one".to_owned(),
            "zerofs/pilot/segments/01/0000000000000001/0000000000000001".to_owned(),
            "zerofs/pilot/segments/02/0000000000000001/0000000000000002".to_owned(),
            "zerofs/pilot/segments/03/0000000000000001/0000000000000003".to_owned(),
        ];
        expected.sort();
        assert_eq!(
            started, expected,
            "later ordering fences must wait while immutable objects preupload"
        );

        controls.release_puts();
        tokio::time::timeout(Duration::from_secs(2), store.wait_remote(7))
            .await
            .expect("held immutable completions were delayed behind fence coalescing")
            .unwrap();
        store.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn remote_replay_never_preuploads_segment_overwrite_across_an_earlier_fence() {
        let (store, _remote, _temp, controls) = test_store_with_controls(true).await;
        controls.block_puts();
        let manifest = Path::from("zerofs/pilot/manifest/one");
        let overwritten = Path::from("zerofs/pilot/segments/02/0000000000000001/0000000000000002");
        let created = Path::from("zerofs/pilot/segments/03/0000000000000001/0000000000000003");

        store
            .put(&manifest, Bytes::from_static(b"manifest").into())
            .await
            .unwrap();
        store
            .put(&overwritten, Bytes::from_static(b"overwrite").into())
            .await
            .unwrap();
        store
            .put_opts(
                &created,
                Bytes::from_static(b"create").into(),
                PutOptions::from(PutMode::Create),
            )
            .await
            .unwrap();
        store.wait_local(3).await.unwrap();

        tokio::time::timeout(Duration::from_secs(2), async {
            while controls.put_count() < 2 {
                controls.put_activity().notified().await;
            }
        })
        .await
        .expect("the frontier and safe immutable create did not start");
        let started = controls.put_paths();

        controls.release_puts();
        store.wait_remote(3).await.unwrap();
        store.shutdown().await.unwrap();

        assert!(started.contains(&manifest.to_string()));
        assert!(started.contains(&created.to_string()));
        assert!(
            !started.contains(&overwritten.to_string()),
            "an overwrite must not preupload across an earlier ordering fence"
        );
    }

    #[tokio::test]
    async fn remote_replay_never_preuploads_a_rename_that_deletes_its_source() {
        let (store, remote, _temp, controls) = test_store_with_controls(true).await;
        remote
            .put(
                &Path::from("rename-source"),
                Bytes::from_static(b"rename-payload").into(),
            )
            .await
            .unwrap();
        controls.block_puts();
        store
            .put(
                &Path::from("manifest/one"),
                Bytes::from_static(b"manifest").into(),
            )
            .await
            .unwrap();
        let rename_target = "zerofs/pilot/segments/09/0000000000000001/0000000000000009";
        store
            .rename_opts(
                &Path::from("rename-source"),
                &Path::from(rename_target),
                RenameOptions {
                    target_mode: RenameTargetMode::Create,
                    ..RenameOptions::default()
                },
            )
            .await
            .unwrap();
        for index in 10_u8..=12 {
            store
                .put_opts(
                    &Path::from(format!(
                        "zerofs/pilot/segments/{index:02x}/0000000000000001/{index:016x}"
                    )),
                    Bytes::from(vec![index; 1024]).into(),
                    PutOptions::from(PutMode::Create),
                )
                .await
                .unwrap();
        }
        store.wait_local(5).await.unwrap();

        tokio::time::timeout(Duration::from_secs(2), async {
            while controls.put_count() < 4 {
                controls.put_activity().notified().await;
            }
        })
        .await
        .expect("remote replay did not fill the safe upload slots");
        assert!(
            !controls
                .put_paths()
                .iter()
                .any(|path| path == rename_target),
            "rename publication started before its source deletion was ordered"
        );

        controls.release_puts();
        store.wait_remote(5).await.unwrap();
        store.shutdown().await.unwrap();
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
        assert_eq!(store.dirty_ssd_reserved_bytes(), 0);
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
            local_concurrency: 4,
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
        assert_eq!(resumed.dirty_ssd_reserved_bytes(), 0);
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

    /// An object store that models the SFTP backend's cost shape: a fixed
    /// number of protocol round trips per operation plus payload transfer at a
    /// bounded per-stream rate. Concurrent transfers genuinely overlap, so a
    /// full pipeline reaches `streams x stream_rate` aggregate throughput —
    /// exactly like the raw `sftp` control's parallel workers.
    struct ThrottledStore {
        inner: Arc<InMemory>,
        rtt: Duration,
        put_round_trips: u32,
        stream_bytes_per_sec: f64,
        stats: Arc<LinkStats>,
    }

    #[derive(Default)]
    struct LinkStats {
        puts: std::sync::atomic::AtomicUsize,
        payload_bytes: std::sync::atomic::AtomicU64,
        occupancy: std::sync::Mutex<LinkOccupancy>,
    }

    struct LinkOccupancy {
        active: usize,
        last_change: std::time::Instant,
        busy_stream_seconds: f64,
    }

    impl Default for LinkOccupancy {
        fn default() -> Self {
            Self {
                active: 0,
                last_change: std::time::Instant::now(),
                busy_stream_seconds: 0.0,
            }
        }
    }

    impl LinkStats {
        fn enter(&self) {
            let mut occupancy = self.occupancy.lock().unwrap();
            let now = std::time::Instant::now();
            occupancy.busy_stream_seconds +=
                occupancy.active as f64 * (now - occupancy.last_change).as_secs_f64();
            occupancy.last_change = now;
            occupancy.active += 1;
        }

        fn exit(&self) {
            let mut occupancy = self.occupancy.lock().unwrap();
            let now = std::time::Instant::now();
            occupancy.busy_stream_seconds +=
                occupancy.active as f64 * (now - occupancy.last_change).as_secs_f64();
            occupancy.last_change = now;
            occupancy.active -= 1;
        }

        fn busy_stream_seconds(&self) -> f64 {
            let occupancy = self.occupancy.lock().unwrap();
            occupancy.busy_stream_seconds
                + occupancy.active as f64
                    * (std::time::Instant::now() - occupancy.last_change).as_secs_f64()
        }
    }

    struct LinkSlot<'stats>(&'stats LinkStats);

    impl<'stats> LinkSlot<'stats> {
        fn enter(stats: &'stats LinkStats) -> Self {
            stats.enter();
            Self(stats)
        }
    }

    impl Drop for LinkSlot<'_> {
        fn drop(&mut self) {
            self.0.exit();
        }
    }

    impl std::fmt::Display for ThrottledStore {
        fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(formatter, "ThrottledStore({})", self.inner)
        }
    }

    impl std::fmt::Debug for ThrottledStore {
        fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(formatter, "ThrottledStore")
        }
    }

    #[async_trait::async_trait]
    impl ObjectStore for ThrottledStore {
        async fn put_opts(
            &self,
            location: &Path,
            payload: object_store::PutPayload,
            opts: PutOptions,
        ) -> object_store::Result<object_store::PutResult> {
            use std::sync::atomic::Ordering;
            let len = payload.content_length() as u64;
            self.stats.puts.fetch_add(1, Ordering::SeqCst);
            self.stats.payload_bytes.fetch_add(len, Ordering::SeqCst);
            let _slot = LinkSlot::enter(&self.stats);
            let transfer = Duration::from_secs_f64(len as f64 / self.stream_bytes_per_sec);
            tokio::time::sleep(self.rtt * self.put_round_trips + transfer).await;
            self.inner.put_opts(location, payload, opts).await
        }

        async fn put_multipart_opts(
            &self,
            location: &Path,
            opts: object_store::PutMultipartOptions,
        ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
            self.inner.put_multipart_opts(location, opts).await
        }

        async fn get_opts(
            &self,
            location: &Path,
            options: object_store::GetOptions,
        ) -> object_store::Result<object_store::GetResult> {
            let _slot = LinkSlot::enter(&self.stats);
            tokio::time::sleep(self.rtt * 2).await;
            self.inner.get_opts(location, options).await
        }

        fn delete_stream(
            &self,
            locations: futures::stream::BoxStream<'static, object_store::Result<Path>>,
        ) -> futures::stream::BoxStream<'static, object_store::Result<Path>> {
            let rtt = self.rtt;
            let inner = self.inner.clone();
            locations
                .then(move |location| {
                    let inner = inner.clone();
                    async move {
                        let location = location?;
                        tokio::time::sleep(rtt).await;
                        inner.delete(&location).await?;
                        Ok(location)
                    }
                })
                .boxed()
        }

        fn list(
            &self,
            prefix: Option<&Path>,
        ) -> futures::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>>
        {
            self.inner.list(prefix)
        }

        async fn list_with_delimiter(
            &self,
            prefix: Option<&Path>,
        ) -> object_store::Result<object_store::ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }

        async fn copy_opts(
            &self,
            from: &Path,
            to: &Path,
            options: CopyOptions,
        ) -> object_store::Result<()> {
            tokio::time::sleep(self.rtt * 2).await;
            self.inner.copy_opts(from, to, options).await
        }
    }

    fn bench_env<T: std::str::FromStr>(name: &str, default: T) -> T {
        std::env::var(name)
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(default)
    }

    /// The three writeback tiers, measured separately: how fast a burst of
    /// puts acknowledges into dirty RAM (`AckMode::Memory`), how fast the
    /// local journaler drains that burst to the SSD, and (already covered by
    /// the throttled-backend bench below) how fast remote replay drains to
    /// the link. Burst acks should sit far above the SSD drain rate — the
    /// dirty RAM budget, not the journal pipeline, is meant to be what a
    /// burst runs into. Run with:
    /// `cargo test --release -p zerofs --lib bench_writeback_tier_profile -- --ignored --nocapture`
    ///
    /// Knobs: ZEROFS_BENCH_TIER_TOTAL_MIB, ZEROFS_BENCH_TIER_PAYLOAD_KIB,
    /// ZEROFS_BENCH_TIER_WRITERS, ZEROFS_BENCH_LOCAL_CONCURRENCY,
    /// ZEROFS_BENCH_DIR.
    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    #[ignore = "throughput benchmark; needs a real disk and --release"]
    async fn bench_writeback_tier_profile() {
        let total_mib: usize = bench_env("ZEROFS_BENCH_TIER_TOTAL_MIB", 2048);
        let payload_kib: usize = bench_env("ZEROFS_BENCH_TIER_PAYLOAD_KIB", 1024);
        let writers: usize = bench_env("ZEROFS_BENCH_TIER_WRITERS", 16);
        let local_concurrency: usize = bench_env("ZEROFS_BENCH_LOCAL_CONCURRENCY", 8);
        let records = (total_mib * 1024 / payload_kib) as u64;

        let temp = match std::env::var("ZEROFS_BENCH_DIR") {
            Ok(dir) => tempfile::tempdir_in(dir).unwrap(),
            Err(_) => tempfile::tempdir().unwrap(),
        };
        let journal = Arc::new(
            Journal::open(
                temp.path().join("writeback"),
                JournalIdentity {
                    format_version: 1,
                    bucket_id: "bucket-bench".to_owned(),
                    backend_endpoint: "memory://remote".to_owned(),
                    database_prefix: "zerofs/pilot".to_owned(),
                    backend_kind: "memory".to_owned(),
                    encryption_key_identity_sha256: [0x77; 32],
                },
            )
            .unwrap(),
        );
        let settings = WritebackSettings {
            dir: temp.path().join("writeback"),
            ack_mode: AckMode::Memory,
            // The burst must fit in dirty RAM so this measures the ack path,
            // not RAM-budget backpressure.
            memory_bytes: (total_mib as u64 + 512) << 20,
            disk_bytes: 256 << 30,
            min_free_bytes: 1,
            high_watermark_percent: 95,
            resume_percent: 85,
            upload_concurrency: 7,
            local_concurrency,
            shutdown_flush: ShutdownFlush::Local,
        };
        let store = WritebackObjectStore::open_paused(Arc::new(InMemory::new()), journal, settings)
            .await
            .unwrap();

        let payload = Bytes::from(vec![0x5a_u8; payload_kib * 1024]);
        let started = std::time::Instant::now();
        let mut tasks = tokio::task::JoinSet::new();
        for writer in 0..writers as u64 {
            let store = store.clone();
            let payload = payload.clone();
            tasks.spawn(async move {
                let mut index = writer;
                while index < records {
                    store
                        .put_opts(
                            &Path::from(format!(
                                "zerofs/pilot/segments/{:02x}/0000000000000001/{index:016x}",
                                index & 0xff
                            )),
                            payload.clone().into(),
                            PutOptions::from(PutMode::Create),
                        )
                        .await
                        .unwrap();
                    index += writers as u64;
                }
            });
        }
        while tasks.join_next().await.is_some() {}
        let acked = started.elapsed();

        store.wait_local(records).await.unwrap();
        let local_done = started.elapsed();

        let total = total_mib as f64;
        println!(
            "tier profile: {records} x {payload_kib} KiB, {writers} writers, \
             local_concurrency {local_concurrency}: \
             RAM ack {:.0} MiB/s ({:.3}s), SSD drain {:.0} MiB/s ({:.3}s to local, \
             {:.3}s after last ack)",
            total / acked.as_secs_f64(),
            acked.as_secs_f64(),
            total / local_done.as_secs_f64(),
            local_done.as_secs_f64(),
            (local_done - acked).as_secs_f64(),
        );

        store.shutdown().await.unwrap();
    }

    /// Remote replay throughput against a backend that mimics the pilot SFTP
    /// link (per-op round trips + bounded per-stream rate). The raw `sftp`
    /// control on vm100 measures ~96 MiB/s over 7 parallel streams, so with
    /// `upload_concurrency = 7` a fully pipelined scheduler should approach the
    /// modeled link ceiling; the gap it prints is scheduler-side loss.
    ///
    /// Run with:
    /// `cargo test --release -p zerofs --lib bench_remote_replay -- --ignored --nocapture`
    ///
    /// Knobs: ZEROFS_BENCH_RTT_MS, ZEROFS_BENCH_LINK_MIBPS,
    /// ZEROFS_BENCH_UPLOAD_CONCURRENCY, ZEROFS_BENCH_PUT_RTTS,
    /// ZEROFS_BENCH_RECORDS, ZEROFS_BENCH_PAYLOAD_KIB, ZEROFS_BENCH_DIR.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[ignore = "throughput benchmark; needs a real disk and --release"]
    async fn bench_remote_replay_throughput_against_throttled_backend() {
        let rtt_ms: f64 = bench_env("ZEROFS_BENCH_RTT_MS", 1.0);
        let link_mibps: f64 = bench_env("ZEROFS_BENCH_LINK_MIBPS", 96.0);
        let concurrency: usize = bench_env("ZEROFS_BENCH_UPLOAD_CONCURRENCY", 7);
        let put_round_trips: u32 = bench_env("ZEROFS_BENCH_PUT_RTTS", 5);
        let records: u64 = bench_env("ZEROFS_BENCH_RECORDS", 384);
        let payload_kib: usize = bench_env("ZEROFS_BENCH_PAYLOAD_KIB", 256);

        let bench_dir = std::env::var("ZEROFS_BENCH_DIR").ok();
        let temp = match &bench_dir {
            Some(dir) => tempfile::tempdir_in(dir).unwrap(),
            None => tempfile::tempdir().unwrap(),
        };
        let journal = Arc::new(
            Journal::open(
                temp.path().join("writeback"),
                JournalIdentity {
                    format_version: 1,
                    bucket_id: "bucket-bench".to_owned(),
                    backend_endpoint: "memory://remote".to_owned(),
                    database_prefix: "zerofs/pilot".to_owned(),
                    backend_kind: "memory".to_owned(),
                    encryption_key_identity_sha256: [0x77; 32],
                },
            )
            .unwrap(),
        );
        let stats = Arc::new(LinkStats::default());
        let stream_bytes_per_sec = link_mibps * 1024.0 * 1024.0 / concurrency as f64;
        let remote = Arc::new(ThrottledStore {
            inner: Arc::new(InMemory::new()),
            rtt: Duration::from_secs_f64(rtt_ms / 1000.0),
            put_round_trips,
            stream_bytes_per_sec,
            stats: stats.clone(),
        });
        let settings = WritebackSettings {
            dir: temp.path().join("writeback"),
            ack_mode: AckMode::Memory,
            memory_bytes: 4 << 30,
            disk_bytes: 64 << 30,
            min_free_bytes: 1,
            high_watermark_percent: 95,
            resume_percent: 85,
            upload_concurrency: concurrency,
            local_concurrency: 8,
            shutdown_flush: ShutdownFlush::Local,
        };
        let store = WritebackObjectStore::open_paused(remote, journal, settings)
            .await
            .unwrap();

        let payload = Bytes::from(vec![0x5a_u8; payload_kib * 1024]);
        for index in 0..records {
            store
                .put_opts(
                    &Path::from(format!(
                        "zerofs/pilot/segments/{:02x}/0000000000000001/{index:016x}",
                        index & 0xff
                    )),
                    payload.clone().into(),
                    PutOptions::from(PutMode::Create),
                )
                .await
                .unwrap();
        }
        store.wait_local(records).await.unwrap();

        let started = std::time::Instant::now();
        store.activate_remote().unwrap();
        store.wait_remote(records).await.unwrap();
        let elapsed = started.elapsed();

        let total_mib = (records as usize * payload_kib) as f64 / 1024.0;
        let achieved = total_mib / elapsed.as_secs_f64();
        let avg_active = stats.busy_stream_seconds() / elapsed.as_secs_f64();
        // What the modeled link supports with every slot busy end to end.
        let per_put_seconds = (rtt_ms / 1000.0) * put_round_trips as f64
            + (payload_kib as f64 * 1024.0) / stream_bytes_per_sec;
        let ideal = (payload_kib as f64 / 1024.0) * concurrency as f64 / per_put_seconds;
        println!(
            "replay: {records} x {payload_kib} KiB via {concurrency} slots \
             (rtt {rtt_ms}ms x{put_round_trips}, stream {:.1} MiB/s): \
             {achieved:.1} MiB/s achieved vs {ideal:.1} MiB/s modeled ceiling \
             ({:.0}%), avg active uploads {avg_active:.2}/{concurrency}, {:.3}s",
            stream_bytes_per_sec / (1024.0 * 1024.0),
            achieved / ideal * 100.0,
            elapsed.as_secs_f64(),
        );

        store.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn coverage_wait_rejects_a_stale_journal_incarnation() {
        let (store, _, _temp) = test_store().await;
        let sequence = store.accepted_sequence();
        assert_eq!(
            store
                .wait_local_coverage(Uuid::nil(), sequence)
                .await
                .unwrap_err(),
            crate::writeback::journaler::LocalBarrierError::StaleIncarnation
        );
        assert_eq!(
            store
                .wait_remote_coverage(Uuid::nil(), sequence)
                .await
                .unwrap_err(),
            crate::writeback::remote::RemoteBarrierError::StaleIncarnation
        );
        store.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn object_coverage_includes_the_accepted_sequence() {
        let (store, _, _temp) = test_store().await;
        store
            .put(
                &Path::from("zerofs/pilot/close-coverage"),
                Bytes::from_static(b"close").into(),
            )
            .await
            .unwrap();
        let coverage = store.object_coverage();
        match coverage {
            crate::fs::mutation::durability::ObjectCoverage::Writeback {
                journal_incarnation,
                sequence,
            } => {
                assert_eq!(journal_incarnation.as_uuid(), store.journal_incarnation());
                assert_eq!(sequence, store.accepted_sequence());
                assert!(sequence >= 1);
            }
            other => panic!("expected writeback coverage, got {other:?}"),
        }
        store.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn coverage_wait_uses_the_captured_accepted_sequence() {
        let (store, _, _temp) = test_store().await;
        store
            .put(
                &Path::from("zerofs/pilot/coverage"),
                Bytes::from_static(b"one").into(),
            )
            .await
            .unwrap();
        let incarnation = store.journal_incarnation();
        let sequence = store.accepted_sequence();
        assert!(
            sequence >= 1,
            "accepted coverage must include the just-admitted put"
        );
        store
            .wait_local_coverage(incarnation, sequence)
            .await
            .unwrap();
        store.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn shutdown_is_idempotent_after_the_first_owner_joins() {
        let (store, _, _temp) = test_store().await;
        store.shutdown().await.unwrap();
        store.shutdown().await.unwrap();
    }
}
