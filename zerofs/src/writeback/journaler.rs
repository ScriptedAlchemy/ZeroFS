use crate::writeback::admission::{AcceptedAdmission, Admission};
use crate::writeback::barrier::{BarrierError, SequenceBarrier, SequenceProgress};
use crate::writeback::journal::{Journal, PreparedMutation, StagedBatch};
use crate::writeback::model::{MutationRecord, Sequence};
use crate::writeback::multipart_reservation::MultipartStagingCleanup;
use crate::writeback::payload::VerifiedPayload;
use crate::writeback::reservation::{
    CommittedSsdReservation, ReservationError, SsdReservationToken, commit_batch_local,
};
use crate::writeback::space_sample::{PhysicalSpaceSample, PhysicalSpaceSampler};
use anyhow::Result as AnyResult;
use bytes::Bytes;
use futures::{FutureExt, StreamExt, stream::FuturesUnordered};
use std::collections::{BTreeMap, VecDeque};
use std::sync::Arc;
use std::sync::Mutex as StdMutex;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::sync::{Mutex, mpsc, oneshot, watch};
use tokio::task::JoinHandle;

#[allow(dead_code)]
pub(crate) fn transition_reservations_then_publish_local<F, T>(
    tokens: Vec<SsdReservationToken>,
    physical_bytes: &[u64],
    sample: PhysicalSpaceSample,
    publish_watermark: F,
) -> Result<(Vec<CommittedSsdReservation>, T), ReservationError>
where
    F: FnOnce() -> T,
{
    let committed = commit_batch_local(tokens, physical_bytes, sample)?;
    Ok((committed, publish_watermark()))
}

#[async_trait::async_trait]
pub trait LocalCommitObserver: Send + Sync + 'static {
    /// Observe one fully committed publication batch. The records are the
    /// committed records returned by the journal's publication path, so
    /// observers must not re-read them from the journal.
    async fn committed_batch(&self, records: &[MutationRecord]) -> AnyResult<()>;
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum LocalBarrierError {
    #[error("local writeback journal is closed")]
    Closed,
    #[error("local writeback durability failed: {0}")]
    LocalDurability(String),
    #[error("local writeback journal incarnation is stale")]
    StaleIncarnation,
}

impl BarrierError for LocalBarrierError {
    fn closed() -> Self {
        Self::Closed
    }

    fn terminal(error: String) -> Self {
        Self::LocalDurability(error)
    }

    fn stale_incarnation() -> Self {
        Self::StaleIncarnation
    }
}

#[derive(Debug, Clone)]
pub struct LocalBarrier {
    progress: SequenceBarrier<LocalBarrierError>,
    incarnation: uuid::Uuid,
}

impl LocalBarrier {
    pub fn local_sequence(&self) -> Sequence {
        self.progress.sequence()
    }

    pub fn incarnation(&self) -> uuid::Uuid {
        self.incarnation
    }

    pub async fn wait_local(&self, sequence: Sequence) -> Result<(), LocalBarrierError> {
        self.progress.wait(self.incarnation, sequence).await
    }
}

trait LocalJournalSink: Send + Sync + 'static {
    fn prepare(
        &self,
        record: MutationRecord,
        payload: Option<&VerifiedPayload>,
    ) -> AnyResult<PreparedMutation>;

    fn publish(&self, prepared: PreparedMutation) -> AnyResult<MutationRecord>;

    fn publish_batch(&self, prepared: Vec<PreparedMutation>) -> AnyResult<Vec<MutationRecord>> {
        prepared
            .into_iter()
            .map(|mutation| self.publish(mutation))
            .collect()
    }

    /// Make a batch's payload bytes durable without publishing anything.
    ///
    /// The default keeps the batch whole and does all the work in
    /// `commit_staged`, which is what a sink that does not separate the two
    /// halves wants; the pipeline then simply never overlaps anything.
    fn stage_batch(
        &self,
        prepared: Vec<PreparedMutation>,
        _expected_first: Sequence,
    ) -> AnyResult<StagedBatch> {
        Ok(StagedBatch::Unstaged(prepared))
    }

    fn commit_staged(&self, staged: StagedBatch) -> AnyResult<Vec<MutationRecord>> {
        match staged {
            StagedBatch::Unstaged(prepared) => self.publish_batch(prepared),
            StagedBatch::Durable { .. } => {
                anyhow::bail!("sink cannot commit a durably staged batch")
            }
        }
    }

    fn discard_staged(&self, staged: StagedBatch) -> AnyResult<()> {
        match staged {
            StagedBatch::Unstaged(prepared) => {
                for mutation in prepared {
                    self.discard(mutation);
                }
                Ok(())
            }
            StagedBatch::Durable { .. } => {
                anyhow::bail!("sink cannot discard a durably staged batch")
            }
        }
    }

    /// Drop a prepared mutation that will never publish.
    ///
    /// Preparation writes nothing, so this cannot fail: it only releases the
    /// payload bytes the mutation was holding. It stays on the trait so test
    /// sinks can observe which sequences the drain abandoned.
    fn discard(&self, prepared: PreparedMutation) {
        drop(prepared);
    }
}

impl LocalJournalSink for Journal {
    fn prepare(
        &self,
        record: MutationRecord,
        payload: Option<&VerifiedPayload>,
    ) -> AnyResult<PreparedMutation> {
        match payload {
            Some(payload) => self.prepare_verified_put(record, payload),
            None => self.prepare_metadata(record),
        }
    }

    fn publish(&self, prepared: PreparedMutation) -> AnyResult<MutationRecord> {
        self.publish_prepared(prepared)
    }

    fn publish_batch(&self, prepared: Vec<PreparedMutation>) -> AnyResult<Vec<MutationRecord>> {
        Journal::publish_batch(self, prepared)
    }

    fn stage_batch(
        &self,
        prepared: Vec<PreparedMutation>,
        expected_first: Sequence,
    ) -> AnyResult<StagedBatch> {
        Journal::stage_batch(self, prepared, expected_first)
    }

    fn commit_staged(&self, staged: StagedBatch) -> AnyResult<Vec<MutationRecord>> {
        Journal::commit_staged(self, staged)
    }

    fn discard_staged(&self, staged: StagedBatch) -> AnyResult<()> {
        Journal::discard_staged(self, staged)
    }
}

// Preparation is validation and encoding only — container staging does the
// payload IO in `stage_batch` — so this bounds concurrent spawn_blocking
// validations, which is also the journal queue's dequeue burst. RAM held by
// prepared-but-unpublished payloads stays bounded by the submitters'
// admission permits, not by this constant.
const DEFAULT_LOCAL_PREPARE_CONCURRENCY: usize = 16;
// Blob payloads are already external files, so this bounds the serialized
// mutation metadata retained by one redb transaction without throttling large
// payload throughput. The record cap separately bounds transaction work when
// mutations are individually tiny.
// A batch's fixed cost is three fsync-class operations regardless of its
// record count, so this cap is what decides how far that cost amortizes. The
// payload-byte cap below is the real bound on a batch's latency and size;
// this one only stops a flood of tiny mutations from making one redb
// transaction arbitrarily large.
const MAX_LOCAL_PUBLISH_BATCH_RECORDS: usize = 512;
const MAX_LOCAL_PUBLISH_BATCH_RECORD_BYTES: usize = 16 * 1024 * 1024;
// One batch publishes as one container blob, so this caps the container. It
// bounds three things at once: the payload bytes held in RAM across
// publication, the size of the single sequential write the drain issues, and
// the transient disk overhead of container reclamation -- a container
// straddling the remote watermark keeps its already-drained members on disk
// until its final member drains, and that is at most this many bytes.
const MAX_LOCAL_PUBLISH_BATCH_PAYLOAD_BYTES: u64 = 64 * 1024 * 1024;
// How many containers may be written at once. A single writer leaves the
// device short of queue depth: measured on the target SSD with real (not
// all-zero) payloads, one writer sustains ~745 MiB/s of durable writes where
// two reach ~840 and four ~910. Two is where most of that gain lands, and it
// caps the payload bytes held across publication at two containers. Raising it
// to four is worth roughly another 8% at the cost of holding four containers'
// payloads (256 MiB at the current batch cap) inside the admission budget.
const MAX_CONCURRENT_CONTAINER_WRITES: usize = 2;
// Total batches between assembly and commit. Without this the drain would
// assemble as fast as records arrive and publish them one or two at a time;
// letting only one batch queue behind the writers is what makes the backlog
// accumulate into large containers under load, which is where the container's
// amortization comes from.
const MAX_IN_FLIGHT_PUBLICATION_BATCHES: usize = MAX_CONCURRENT_CONTAINER_WRITES + 1;

#[derive(Clone)]
pub struct LocalJournaler {
    inner: Arc<LocalJournalerInner>,
}

impl std::fmt::Debug for LocalJournaler {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("LocalJournaler")
            .field("closed", &self.inner.closed.load(Ordering::Acquire))
            .finish_non_exhaustive()
    }
}

struct LocalJournalerInner {
    sender: mpsc::Sender<JournalCommand>,
    barrier: LocalBarrier,
    admission_gate: Mutex<()>,
    closed: AtomicBool,
    join: Mutex<Option<JoinHandle<()>>>,
    shutdown_result: watch::Sender<Option<Result<(), LocalBarrierError>>>,
    _retained_failure_ownership: Arc<StdMutex<Vec<MutationOwnership>>>,
}

struct LocalJournalerOwnership {
    admission: Admission,
    retained_failure_ownership: Arc<StdMutex<Vec<MutationOwnership>>>,
    space: Option<Arc<PhysicalSpaceSampler>>,
}

enum JournalCommand {
    Mutation {
        record: Box<MutationRecord>,
        payload: Option<VerifiedPayload>,
        ram: Option<AcceptedAdmission>,
        disk: Option<SsdReservationToken>,
        multipart_cleanup: Option<MultipartStagingCleanup>,
    },
    Shutdown(oneshot::Sender<()>),
}

/// Reserved capacity in the journal queue. Reserving before entering the
/// store's global admission-order critical section lets a full queue apply
/// backpressure without stalling unrelated writers behind the order lock;
/// queue order is still the order of `submit_reserved` calls, not of
/// reservations.
pub(crate) struct SubmitSlot {
    permit: mpsc::OwnedPermit<JournalCommand>,
}

impl LocalJournaler {
    pub fn start(
        journal: Arc<Journal>,
        admission: Admission,
        queue_depth: usize,
    ) -> AnyResult<Self> {
        Self::start_with_observer(
            journal,
            admission,
            queue_depth,
            DEFAULT_LOCAL_PREPARE_CONCURRENCY,
            None,
        )
    }

    pub fn start_with_observer(
        journal: Arc<Journal>,
        admission: Admission,
        queue_depth: usize,
        prepare_concurrency: usize,
        observer: Option<Arc<dyn LocalCommitObserver>>,
    ) -> AnyResult<Self> {
        let snapshot = journal.snapshot()?;
        Ok(Self::start_with_sink_observer_space(
            journal,
            admission,
            snapshot.local_seq,
            snapshot.incarnation,
            queue_depth,
            prepare_concurrency,
            observer,
            None,
        ))
    }

    pub(crate) fn start_with_observer_and_space(
        journal: Arc<Journal>,
        admission: Admission,
        queue_depth: usize,
        prepare_concurrency: usize,
        observer: Option<Arc<dyn LocalCommitObserver>>,
        space: Arc<PhysicalSpaceSampler>,
    ) -> AnyResult<Self> {
        let snapshot = journal.snapshot()?;
        Ok(Self::start_with_sink_observer_space(
            journal,
            admission,
            snapshot.local_seq,
            snapshot.incarnation,
            queue_depth,
            prepare_concurrency,
            observer,
            Some(space),
        ))
    }

    #[cfg(test)]
    fn start_with_sink(
        sink: Arc<dyn LocalJournalSink>,
        admission: Admission,
        local_sequence: Sequence,
        queue_depth: usize,
    ) -> Self {
        Self::start_with_sink_and_observer(
            sink,
            admission,
            local_sequence,
            uuid::Uuid::nil(),
            queue_depth,
            DEFAULT_LOCAL_PREPARE_CONCURRENCY,
            None,
        )
    }

    // Landed-but-not-wired constructor variant.
    #[allow(dead_code)]
    fn start_with_sink_and_observer(
        sink: Arc<dyn LocalJournalSink>,
        admission: Admission,
        local_sequence: Sequence,
        incarnation: uuid::Uuid,
        queue_depth: usize,
        prepare_concurrency: usize,
        observer: Option<Arc<dyn LocalCommitObserver>>,
    ) -> Self {
        Self::start_with_sink_observer_space(
            sink,
            admission,
            local_sequence,
            incarnation,
            queue_depth,
            prepare_concurrency,
            observer,
            None,
        )
    }

    #[allow(clippy::too_many_arguments, dead_code)]
    fn start_with_sink_observer_space(
        sink: Arc<dyn LocalJournalSink>,
        admission: Admission,
        local_sequence: Sequence,
        incarnation: uuid::Uuid,
        queue_depth: usize,
        prepare_concurrency: usize,
        observer: Option<Arc<dyn LocalCommitObserver>>,
        space: Option<Arc<PhysicalSpaceSampler>>,
    ) -> Self {
        let (sender, receiver) = mpsc::channel(queue_depth.max(1));
        let (progress_sender, progress) = watch::channel(SequenceProgress {
            incarnation,
            sequence: local_sequence,
            terminal_error: None,
            closed: false,
        });
        let (shutdown_result, _) = watch::channel(None);
        let retained_failure_ownership = Arc::new(StdMutex::new(Vec::new()));
        let join = tokio::spawn(run_journaler(
            sink,
            receiver,
            progress_sender,
            local_sequence,
            observer,
            prepare_concurrency,
            LocalJournalerOwnership {
                admission,
                retained_failure_ownership: retained_failure_ownership.clone(),
                space,
            },
        ));
        Self {
            inner: Arc::new(LocalJournalerInner {
                sender,
                barrier: LocalBarrier {
                    progress: SequenceBarrier::new(progress),
                    incarnation,
                },
                admission_gate: Mutex::new(()),
                closed: AtomicBool::new(false),
                join: Mutex::new(Some(join)),
                shutdown_result,
                _retained_failure_ownership: retained_failure_ownership,
            }),
        }
    }

    pub fn barrier(&self) -> LocalBarrier {
        self.inner.barrier.clone()
    }

    pub async fn submit_put(
        &self,
        record: MutationRecord,
        payload: Bytes,
        ram: AcceptedAdmission,
    ) -> Result<LocalBarrier, LocalBarrierError> {
        self.submit_verified_put(record, VerifiedPayload::new(payload), ram)
            .await
    }

    async fn submit_verified_put(
        &self,
        record: MutationRecord,
        payload: VerifiedPayload,
        ram: AcceptedAdmission,
    ) -> Result<LocalBarrier, LocalBarrierError> {
        self.submit(record, Some(payload), Some(ram), None, None)
            .await
    }

    // Landed-but-not-wired: SSD-reservation submit variants for tiered admission.
    #[allow(dead_code)]
    pub(crate) async fn submit_put_with_disk(
        &self,
        record: MutationRecord,
        payload: Bytes,
        ram: AcceptedAdmission,
        disk: SsdReservationToken,
    ) -> Result<LocalBarrier, LocalBarrierError> {
        self.submit_verified_put_with_disk(record, VerifiedPayload::new(payload), ram, disk)
            .await
    }

    #[allow(dead_code)]
    pub(crate) async fn submit_verified_put_with_disk(
        &self,
        record: MutationRecord,
        payload: VerifiedPayload,
        ram: AcceptedAdmission,
        disk: SsdReservationToken,
    ) -> Result<LocalBarrier, LocalBarrierError> {
        self.submit(record, Some(payload), Some(ram), Some(disk), None)
            .await
    }

    #[allow(dead_code)]
    pub(crate) async fn submit_metadata_with_disk(
        &self,
        record: MutationRecord,
        disk: SsdReservationToken,
    ) -> Result<LocalBarrier, LocalBarrierError> {
        self.submit(record, None, None, Some(disk), None).await
    }

    async fn submit(
        &self,
        record: MutationRecord,
        payload: Option<VerifiedPayload>,
        ram: Option<AcceptedAdmission>,
        disk: Option<SsdReservationToken>,
        multipart_cleanup: Option<MultipartStagingCleanup>,
    ) -> Result<LocalBarrier, LocalBarrierError> {
        let _gate = self.inner.admission_gate.lock().await;
        if self.inner.closed.load(Ordering::Acquire) {
            return Err(LocalBarrierError::Closed);
        }
        self.inner
            .sender
            .send(JournalCommand::Mutation {
                record: Box::new(record),
                payload,
                ram,
                disk,
                multipart_cleanup,
            })
            .await
            .map_err(|_| terminal_or_closed(&self.inner.barrier))?;
        Ok(self.inner.barrier.clone())
    }

    /// Wait for queue capacity without submitting anything yet.
    pub(crate) async fn reserve_slot(&self) -> Result<SubmitSlot, LocalBarrierError> {
        if self.inner.closed.load(Ordering::Acquire) {
            return Err(LocalBarrierError::Closed);
        }
        let permit = self
            .inner
            .sender
            .clone()
            .reserve_owned()
            .await
            .map_err(|_| terminal_or_closed(&self.inner.barrier))?;
        Ok(SubmitSlot { permit })
    }

    /// Enqueue a mutation into previously reserved capacity. The message is
    /// queued at this call, so callers serialize submissions in sequence
    /// order without ever waiting on capacity here.
    pub(crate) async fn submit_reserved(
        &self,
        slot: SubmitSlot,
        record: MutationRecord,
        payload: Option<VerifiedPayload>,
        ram: Option<AcceptedAdmission>,
        disk: Option<SsdReservationToken>,
    ) -> Result<LocalBarrier, LocalBarrierError> {
        let _gate = self.inner.admission_gate.lock().await;
        if self.inner.closed.load(Ordering::Acquire) {
            return Err(LocalBarrierError::Closed);
        }
        slot.permit.send(JournalCommand::Mutation {
            record: Box::new(record),
            payload,
            ram,
            disk,
            multipart_cleanup: None,
        });
        Ok(self.inner.barrier.clone())
    }

    /// Enqueue a reserved multipart mutation without surrendering staging
    /// ownership until the journaler has accepted the command. This keeps the
    /// caller able to perform sampled cleanup if shutdown wins the race.
    pub(crate) async fn submit_reserved_with_cleanup(
        &self,
        slot: SubmitSlot,
        record: MutationRecord,
        payload: Option<VerifiedPayload>,
        ram: Option<AcceptedAdmission>,
        disk: Option<SsdReservationToken>,
        multipart_cleanup: &mut Option<MultipartStagingCleanup>,
    ) -> Result<LocalBarrier, LocalBarrierError> {
        let _gate = self.inner.admission_gate.lock().await;
        if self.inner.closed.load(Ordering::Acquire) {
            return Err(LocalBarrierError::Closed);
        }
        slot.permit.send(JournalCommand::Mutation {
            record: Box::new(record),
            payload,
            ram,
            disk,
            multipart_cleanup: multipart_cleanup.take(),
        });
        Ok(self.inner.barrier.clone())
    }

    pub async fn shutdown(&self) -> Result<(), LocalBarrierError> {
        let mut completion = self.inner.shutdown_result.subscribe();
        let mut local_progress = self.inner.barrier.progress.watcher();
        let mut local_progress_open = true;
        {
            let _gate = self.inner.admission_gate.lock().await;
            if !self.inner.closed.swap(true, Ordering::AcqRel) {
                let inner = self.inner.clone();
                tokio::spawn(async move {
                    drive_shutdown(inner).await;
                });
            }
        }

        loop {
            if let Some(result) = completion.borrow().clone() {
                return result;
            }
            tokio::select! {
                result = completion.changed() => {
                    if result.is_err() {
                        return Err(terminal_or_closed(&self.inner.barrier));
                    }
                }
                result = local_progress.changed(), if local_progress_open => {
                    if result.is_err() {
                        local_progress_open = false;
                    }
                }
            }
        }
    }
}

async fn drive_shutdown(inner: Arc<LocalJournalerInner>) {
    let mut outcome = None;
    let (done, acknowledged) = oneshot::channel();
    if inner
        .sender
        .send(JournalCommand::Shutdown(done))
        .await
        .is_err()
    {
        outcome = Some(Err(terminal_or_closed(&inner.barrier)));
    } else if acknowledged.await.is_err() {
        outcome = Some(Err(current_terminal(&inner.barrier).unwrap_or_else(|| {
            LocalBarrierError::LocalDurability(
                "journal worker dropped the shutdown acknowledgement".to_owned(),
            )
        })));
    }

    if let Some(join) = inner.join.lock().await.take()
        && let Err(error) = join.await
    {
        outcome = Some(Err(LocalBarrierError::LocalDurability(format!(
            "journal worker panicked: {error}"
        ))));
    }
    if let Some(error) = current_terminal(&inner.barrier) {
        outcome = Some(Err(error));
    }
    inner
        .shutdown_result
        .send_replace(Some(outcome.unwrap_or(Ok(()))));
}

type Preparation = (Sequence, AnyResult<PreparedMutation>, MutationOwnership);
type PreparedEntry = (AnyResult<PreparedMutation>, MutationOwnership);

struct MutationOwnership {
    sequence: Sequence,
    _ram: Option<AcceptedAdmission>,
    disk: Option<SsdReservationToken>,
    multipart_cleanup: Option<MultipartStagingCleanup>,
}

/// Files a finished preparation, returning a terminal error if its task died.
fn collect_preparation(
    result: Result<Preparation, tokio::task::JoinError>,
    prepared: &mut BTreeMap<Sequence, PreparedEntry>,
) -> Option<String> {
    match result {
        Ok((sequence, result, ownership)) => {
            prepared.insert(sequence, (result, ownership));
            None
        }
        Err(error) => Some(format!("local journal preparer panicked: {error}")),
    }
}

/// Admits one queued command, returning a terminal error if it breaks the
/// contiguous submission order the journal depends on.
fn accept_journal_command(
    command: Option<JournalCommand>,
    sink: &Arc<dyn LocalJournalSink>,
    preparations: &mut FuturesUnordered<JoinHandle<Preparation>>,
    next_received: &mut Option<Sequence>,
    input_closed: &mut bool,
    shutdown: &mut Option<oneshot::Sender<()>>,
) -> Option<String> {
    match command {
        Some(JournalCommand::Mutation {
            record,
            payload,
            ram,
            disk,
            multipart_cleanup,
        }) => {
            let record = *record;
            let sequence = record.sequence;
            if *next_received != Some(sequence) {
                let expected = next_received
                    .map_or_else(|| "after overflow".to_owned(), |value| value.to_string());
                drop(MutationOwnership {
                    sequence,
                    _ram: ram,
                    disk,
                    multipart_cleanup,
                });
                return Some(format!(
                    "journal worker expected sequence {expected}, got {sequence}"
                ));
            }
            *next_received = sequence.checked_add(1);
            let prepare_sink = sink.clone();
            preparations.push(tokio::task::spawn_blocking(move || {
                let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    prepare_sink.prepare(record, payload.as_ref())
                }))
                .unwrap_or_else(|_| Err(anyhow::anyhow!("local journal preparer panicked")));
                (
                    sequence,
                    result,
                    MutationOwnership {
                        sequence,
                        _ram: ram,
                        disk,
                        multipart_cleanup,
                    },
                )
            }));
            None
        }
        Some(JournalCommand::Shutdown(done)) => {
            *shutdown = Some(done);
            *input_closed = true;
            None
        }
        None => {
            *input_closed = true;
            None
        }
    }
}

/// The admission permits a batch owns, released when it commits.
type BatchOwnership = Vec<MutationOwnership>;

struct AssembledBatch {
    mutations: Vec<PreparedMutation>,
    ownership: BatchOwnership,
}

/// Drop a batch-head preparation that can never publish, releasing the
/// admission permits it was holding, and hand back its preparation result.
///
/// Only the head is ever discarded this way: a fatal record sitting behind
/// already-accumulated ones is left in place and reconsidered as the head of
/// the next batch.
fn take_fatal_head(
    prepared: &mut BTreeMap<Sequence, PreparedEntry>,
    sequence: Sequence,
) -> PreparedEntry {
    prepared
        .remove(&sequence)
        .expect("the fatal preparation still exists")
}

struct AssemblyError {
    message: String,
    ownership: BatchOwnership,
}

/// Take the longest contiguous run of ready preparations that fits one batch.
///
/// A fatal record (failed, unsizeable, or oversized preparation) only becomes
/// terminal once it is the head of the batch: while earlier records are
/// already accumulated, they are published first and the fatal one is
/// reconsidered as the head of the next batch.
fn assemble_batch(
    prepared: &mut BTreeMap<Sequence, PreparedEntry>,
    next_admitted: Sequence,
) -> Result<Option<AssembledBatch>, AssemblyError> {
    let mut mutations = Vec::new();
    let mut ownership: BatchOwnership = Vec::new();
    let mut encoded_record_bytes = 0_usize;
    let mut container_payload_bytes = 0_u64;
    let mut candidate = Some(next_admitted);

    while let Some(sequence) = candidate {
        let Some((result, _)) = prepared.get(&sequence) else {
            break;
        };
        if result.is_err() {
            if !mutations.is_empty() {
                break;
            }
            let (Err(error), fatal_ownership) = take_fatal_head(prepared, sequence) else {
                unreachable!("the preparation result was checked above")
            };
            return Err(AssemblyError {
                message: format!("{error:#}"),
                ownership: vec![fatal_ownership],
            });
        }
        if mutations.len() == MAX_LOCAL_PUBLISH_BATCH_RECORDS {
            break;
        }
        let Ok(mutation) = result else {
            unreachable!("preparation errors are handled above")
        };
        let mutation_bytes = match mutation.encoded_record_bytes() {
            Ok(bytes) => bytes,
            Err(error) => {
                if !mutations.is_empty() {
                    break;
                }
                let (_, fatal_ownership) = take_fatal_head(prepared, sequence);
                return Err(AssemblyError {
                    message: format!("{error:#}"),
                    ownership: vec![fatal_ownership],
                });
            }
        };
        if mutation_bytes > MAX_LOCAL_PUBLISH_BATCH_RECORD_BYTES {
            if !mutations.is_empty() {
                break;
            }
            let (_, fatal_ownership) = take_fatal_head(prepared, sequence);
            return Err(AssemblyError {
                message: format!(
                    "prepared journal mutation {sequence} encodes to {mutation_bytes} bytes, exceeding the {MAX_LOCAL_PUBLISH_BATCH_RECORD_BYTES}-byte local publication batch limit"
                ),
                ownership: vec![fatal_ownership],
            });
        }
        let Some(next_encoded_bytes) = encoded_record_bytes.checked_add(mutation_bytes) else {
            if !mutations.is_empty() {
                break;
            }
            let (_, fatal_ownership) = take_fatal_head(prepared, sequence);
            return Err(AssemblyError {
                message: "local publication batch byte count overflow".to_owned(),
                ownership: vec![fatal_ownership],
            });
        };
        if next_encoded_bytes > MAX_LOCAL_PUBLISH_BATCH_RECORD_BYTES {
            break;
        }
        let next_payload_bytes = container_payload_bytes.saturating_add(mutation.payload_bytes());
        // A single payload larger than the cap still publishes alone; the cap
        // only stops a batch from growing past it.
        if next_payload_bytes > MAX_LOCAL_PUBLISH_BATCH_PAYLOAD_BYTES && !mutations.is_empty() {
            break;
        }
        let (result, mutation_ownership) = prepared
            .remove(&sequence)
            .expect("the expected prepared mutation still exists");
        mutations.push(result.expect("the prepared mutation was checked above"));
        ownership.push(mutation_ownership);
        encoded_record_bytes = next_encoded_bytes;
        container_payload_bytes = next_payload_bytes;
        candidate = sequence.checked_add(1);
    }

    if mutations.is_empty() {
        return Ok(None);
    }
    Ok(Some(AssembledBatch {
        mutations,
        ownership,
    }))
}

/// The terminal error for a drain that can no longer make progress.
///
/// Reaching this means the backlog holds a sequence the drain will never
/// admit, which submission ordering is supposed to make impossible -- so the
/// message names the sequence it is stuck on and everything left stranded
/// behind it, because that pair is the whole diagnosis.
fn stalled_drain_error(
    next_admitted: Sequence,
    prepared: &BTreeMap<Sequence, PreparedEntry>,
) -> String {
    let stranded = prepared.keys().copied().collect::<Vec<_>>();
    format!(
        "local journal drain stalled at sequence {next_admitted} with stranded preparations {stranded:?}"
    )
}

/// Await one pipeline half, clearing its slot only once it has resolved.
///
/// Awaiting `&mut JoinHandle` is cancel safe, so losing a `select!` race here
/// leaves the task running and the handle usable on the next poll.
async fn join_pipeline_half<T>(
    handle: &mut Option<JoinHandle<T>>,
) -> Result<T, tokio::task::JoinError> {
    match handle.as_mut() {
        Some(join) => {
            let result = join.await;
            *handle = None;
            result
        }
        None => std::future::pending().await,
    }
}

async fn transition_ssd_tokens(
    ownership: &mut BatchOwnership,
    space: Option<&PhysicalSpaceSampler>,
) -> Option<String> {
    let mut tokens = Vec::new();
    let mut physicals = Vec::new();
    for owner in ownership.iter_mut() {
        if let Some(token) = owner.disk.take() {
            physicals.push(token.request().physical_reservation_bytes);
            tokens.push(token);
        }
    }
    if tokens.is_empty() {
        return None;
    }
    let Some(space) = space else {
        for mut token in tokens {
            token.disarm();
        }
        return None;
    };
    match space.sample().await {
        Ok(sample) => crate::writeback::reservation::commit_batch_local(tokens, &physicals, sample)
            .err()
            .map(|error| format!("SSD reservation transition failed: {error}")),
        Err(error) => {
            for mut token in tokens {
                token.disarm();
            }
            Some(format!("SSD reservation sample failed: {error}"))
        }
    }
}

async fn cleanup_multipart_staging(
    ownership: &mut BatchOwnership,
    space: Option<&PhysicalSpaceSampler>,
) -> Option<String> {
    let mut cleaned = Vec::new();
    for owner in ownership.iter_mut() {
        let Some(cleanup) = owner.multipart_cleanup.take() else {
            continue;
        };
        match tokio::task::spawn_blocking(move || cleanup.remove()).await {
            Ok(Ok(result)) => cleaned.push(result),
            Ok(Err(error)) => {
                let message = format!("multipart staging cleanup failed: {error}");
                for result in cleaned {
                    result.poison_and_retain(message.clone());
                }
                return Some(message);
            }
            Err(error) => {
                let message = format!("multipart staging cleanup task failed: {error}");
                for result in cleaned {
                    result.poison_and_retain(message.clone());
                }
                return Some(message);
            }
        }
    }
    if cleaned.is_empty() {
        return None;
    }
    let Some(space) = space else {
        let message = "multipart staging cleanup has no physical-space sampler".to_owned();
        for result in cleaned {
            result.poison_and_retain(message.clone());
        }
        return Some(message);
    };
    let sample = match space.sample().await {
        Ok(sample) => sample,
        Err(error) => {
            let message = format!("physical-space sample failed after multipart cleanup: {error}");
            for result in cleaned {
                result.poison_and_retain(message.clone());
            }
            return Some(message);
        }
    };
    let observe_error = cleaned
        .iter()
        .find_map(|result| result.admission().observe_sample(sample).err());
    if let Some(error) = observe_error {
        let message = format!("SSD admission rejected post-cleanup space sample: {error}");
        for retained in cleaned {
            retained.poison_and_retain(message.clone());
        }
        return Some(message);
    }
    drop(cleaned);
    None
}

async fn cleanup_uncommitted_ownership(
    mut ownership: BatchOwnership,
    space: Option<&PhysicalSpaceSampler>,
) -> Option<String> {
    let error = cleanup_multipart_staging(&mut ownership, space).await;
    drop(ownership);
    error
}

/// Settle one committed batch: release its permits, notify the observer, and
/// only then publish the new watermark. Returns a terminal error if the batch
/// did not become durable exactly as assembled.
async fn finish_commit(
    result: Result<AnyResult<Vec<MutationRecord>>, tokio::task::JoinError>,
    mut ownership: BatchOwnership,
    observer: Option<&Arc<dyn LocalCommitObserver>>,
    retained_failure_ownership: &Arc<StdMutex<Vec<MutationOwnership>>>,
    progress: &watch::Sender<SequenceProgress>,
    space: Option<&PhysicalSpaceSampler>,
) -> Option<String> {
    let committed = match result {
        Ok(Ok(records)) => records,
        Ok(Err(error)) => {
            drop(ownership);
            return Some(format!("{error:#}"));
        }
        Err(error) => {
            drop(ownership);
            return Some(format!("local journal publisher panicked: {error}"));
        }
    };
    let expected_sequences = ownership
        .iter()
        .map(|owner| owner.sequence)
        .collect::<Vec<_>>();
    let committed_sequences = committed
        .iter()
        .map(|record| record.sequence)
        .collect::<Vec<_>>();
    if committed_sequences != expected_sequences {
        drop(ownership);
        return Some(format!(
            "local journal publisher returned sequences {committed_sequences:?}, expected {expected_sequences:?}"
        ));
    }
    let durable_batch_tail = *expected_sequences
        .last()
        .expect("a committed batch contains at least one record");

    if let Some(error) = transition_ssd_tokens(&mut ownership, space).await {
        return Some(error);
    }

    if let Some(observer) = observer
        && let Err(error) = observer.committed_batch(&committed).await
    {
        let mut retained = retained_failure_ownership
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        retained.extend(ownership);
        return Some(format!("local commit observer failed: {error:#}"));
    }
    if let Some(error) = cleanup_multipart_staging(&mut ownership, space).await {
        return Some(error);
    }
    drop(ownership);
    // The watermark moves last, so `wait_local` releases a sequence only after
    // its bytes, its metadata, and every observer of its batch are done.
    progress.send_modify(|state| state.sequence = durable_batch_tail);
    None
}

async fn run_journaler(
    sink: Arc<dyn LocalJournalSink>,
    mut receiver: mpsc::Receiver<JournalCommand>,
    progress: watch::Sender<SequenceProgress>,
    local_sequence: Sequence,
    observer: Option<Arc<dyn LocalCommitObserver>>,
    prepare_concurrency: usize,
    ownership: LocalJournalerOwnership,
) {
    let LocalJournalerOwnership {
        admission,
        retained_failure_ownership,
        space,
    } = ownership;
    let prepare_concurrency = prepare_concurrency.max(1);
    let mut next_admitted = local_sequence.saturating_add(1);
    let mut next_received = local_sequence.checked_add(1);
    let mut preparations: FuturesUnordered<JoinHandle<Preparation>> = FuturesUnordered::new();
    let mut prepared: BTreeMap<Sequence, PreparedEntry> = BTreeMap::new();
    let mut shutdown = None;
    let mut input_closed = false;
    let mut terminal = None;

    // The publication pipeline. Staging (write the container, fsync it, fsync
    // its directory) and committing (insert the records and advance the
    // watermark) run as separate tasks so one batch's bytes reach the device
    // while the previous batch's metadata commits. Both halves are bounded to
    // one batch each, so at most two batches are in flight and the payload
    // bytes held across publication stay bounded by two containers.
    //
    // Ordering survives the overlap: only one commit runs at a time and
    // batches enter the commit slot in assembly order, and
    // `commit_record_batch` independently refuses any batch that is not
    // contiguous with the durable watermark. A record is still ACKed only by
    // its own commit completing, which happens strictly after its own
    // container was fsynced.
    let mut staging: FuturesUnordered<JoinHandle<(Sequence, AnyResult<StagedBatch>)>> =
        FuturesUnordered::new();
    let mut staged_ownership: BTreeMap<Sequence, BatchOwnership> = BTreeMap::new();
    let mut ready: BTreeMap<Sequence, StagedBatch> = BTreeMap::new();
    let mut commit_order: VecDeque<Sequence> = VecDeque::new();
    let mut committing: Option<JoinHandle<AnyResult<Vec<MutationRecord>>>> = None;
    let mut committing_ownership: BatchOwnership = Vec::new();

    loop {
        while terminal.is_none() {
            let Some(Some(result)) = preparations.next().now_or_never() else {
                break;
            };
            terminal = collect_preparation(result, &mut prepared);
        }

        // Commit the oldest staged batch as soon as the commit half is free.
        // Containers may finish out of order, so the commit slot is fed from
        // assembly order, never from completion order.
        if terminal.is_none()
            && committing.is_none()
            && let Some(first) = commit_order.front().copied()
            && let Some(staged) = ready.remove(&first)
        {
            commit_order.pop_front();
            let ownership = staged_ownership
                .remove(&first)
                .expect("a staged batch owns its permits");
            let commit_sink = sink.clone();
            committing = Some(tokio::task::spawn_blocking(move || {
                commit_sink.commit_staged(staged)
            }));
            committing_ownership = ownership;
        }

        // Keep the staging half busy. One container at a time leaves the
        // device short of queue depth -- a single writer tops out well below
        // what a few concurrent ones reach -- so more than one batch may be
        // writing its container at once. They are distinct files over disjoint
        // sequence ranges, so the writes are independent; only the commits are
        // ordered.
        while terminal.is_none()
            && staging.len() < MAX_CONCURRENT_CONTAINER_WRITES
            && staging.len() + ready.len() + usize::from(committing.is_some())
                < MAX_IN_FLIGHT_PUBLICATION_BATCHES
        {
            match assemble_batch(&mut prepared, next_admitted) {
                Err(error) => {
                    let cleanup_error =
                        cleanup_uncommitted_ownership(error.ownership, space.as_deref()).await;
                    let mut message = error.message;
                    if let Some(error) = cleanup_error {
                        message.push_str(&format!("; {error}"));
                    }
                    terminal = Some(message);
                    break;
                }
                Ok(Some(AssembledBatch {
                    mutations,
                    ownership,
                })) => {
                    let expected_first = next_admitted;
                    next_admitted = match ownership
                        .last()
                        .expect("an assembled batch owns at least one sequence")
                        .sequence
                        .checked_add(1)
                    {
                        Some(next) => next,
                        None => {
                            // The batch still has to be published; refuse the
                            // impossible successor instead of wrapping.
                            terminal = Some("local journal sequence overflow".to_owned());
                            drop(ownership);
                            break;
                        }
                    };
                    let stage_sink = sink.clone();
                    staging.push(tokio::task::spawn_blocking(move || {
                        (
                            expected_first,
                            stage_sink.stage_batch(mutations, expected_first),
                        )
                    }));
                    staged_ownership.insert(expected_first, ownership);
                    commit_order.push_back(expected_first);
                }
                Ok(None) => break,
            }
        }

        if terminal.is_some() && staging.is_empty() && committing.is_none() {
            break;
        }
        if terminal.is_none()
            && input_closed
            && preparations.is_empty()
            && prepared.is_empty()
            && staging.is_empty()
            && ready.is_empty()
            && committing.is_none()
        {
            break;
        }

        tokio::select! {
            biased;
            // Faults are never allowed to skip an in-flight batch: both halves
            // are always awaited to completion so their durability outcome is
            // observed before the drain gives up.
            result = join_pipeline_half(&mut committing), if committing.is_some() => {
                let ownership = std::mem::take(&mut committing_ownership);
                if let Some(error) = finish_commit(
                    result,
                    ownership,
                    observer.as_ref(),
                    &retained_failure_ownership,
                    &progress,
                    space.as_deref(),
                )
                .await
                    && terminal.is_none()
                {
                    terminal = Some(error);
                }
            }
            Some(result) = staging.next(), if !staging.is_empty() => {
                match result {
                    Ok((first, Ok(staged))) => {
                        let expected = staged_ownership
                            .get(&first)
                            .expect("a staged batch owns its permits")
                            .iter()
                            .map(|owner| owner.sequence)
                            .collect::<Vec<_>>();
                        let actual = staged.sequences();
                        if actual != expected {
                            // The permits released after a commit are keyed by
                            // the sequences assembled here, so a batch that
                            // staged something else would release the wrong
                            // ones. Refuse it rather than commit it.
                            let _ = sink.discard_staged(staged);
                            let ownership = staged_ownership
                                .remove(&first)
                                .expect("mismatched staged batch owns its permits");
                            let cleanup_error =
                                cleanup_uncommitted_ownership(ownership, space.as_deref()).await;
                            commit_order.retain(|queued| *queued != first);
                            if terminal.is_none() {
                                let mut message = format!(
                                    "local journal stager returned sequences {actual:?}, expected {expected:?}"
                                );
                                if let Some(error) = cleanup_error {
                                    message.push_str(&format!("; {error}"));
                                }
                                terminal = Some(message);
                            }
                        } else if terminal.is_some() {
                            // The batch's bytes are durable but nothing will
                            // reference them; unlink instead of leaking until
                            // the next open collects it.
                            let _ = sink.discard_staged(staged);
                            let ownership = staged_ownership
                                .remove(&first)
                                .expect("discarded staged batch owns its permits");
                            if let Some(error) =
                                cleanup_uncommitted_ownership(ownership, space.as_deref()).await
                                && terminal.is_none()
                            {
                                terminal = Some(error);
                            }
                            commit_order.retain(|queued| *queued != first);
                        } else {
                            ready.insert(first, staged);
                        }
                    }
                    Ok((first, Err(error))) => {
                        let ownership = staged_ownership
                            .remove(&first)
                            .expect("failed staged batch owns its permits");
                        let cleanup_error =
                            cleanup_uncommitted_ownership(ownership, space.as_deref()).await;
                        commit_order.retain(|queued| *queued != first);
                        if terminal.is_none() {
                            let mut message = format!("{error:#}");
                            if let Some(error) = cleanup_error {
                                message.push_str(&format!("; {error}"));
                            }
                            terminal = Some(message);
                        }
                    }
                    Err(error) => {
                        if terminal.is_none() {
                            terminal = Some(format!("local journal stager panicked: {error}"));
                        }
                    }
                }
            }
            Some(result) = preparations.next(), if !preparations.is_empty() => {
                if let Some(error) = collect_preparation(result, &mut prepared)
                    && terminal.is_none()
                {
                    terminal = Some(error);
                }
            }
            command = receiver.recv(),
                if terminal.is_none()
                    && !input_closed
                    && preparations.len() < prepare_concurrency =>
            {
                if let Some(error) = accept_journal_command(
                    command,
                    &sink,
                    &mut preparations,
                    &mut next_received,
                    &mut input_closed,
                    &mut shutdown,
                ) && terminal.is_none()
                {
                    terminal = Some(error);
                }
            }
            // Both halves idle, nothing preparing, and input closed, yet the
            // exit checks above did not fire: the backlog holds a sequence the
            // drain can never reach. Report it rather than let `select!` panic
            // on a fully disabled set of branches.
            else => {
                if terminal.is_none() {
                    terminal = Some(stalled_drain_error(next_admitted, &prepared));
                }
            }
        }
    }

    // Anything staged but never committed is durable bytes nothing will ever
    // reference. Recovery would collect it from its name, but unlink it now.
    for (first, staged) in std::mem::take(&mut ready) {
        let _ = sink.discard_staged(staged);
        if let Some(ownership) = staged_ownership.remove(&first) {
            let _ = cleanup_uncommitted_ownership(ownership, space.as_deref()).await;
        }
    }
    for (_, ownership) in std::mem::take(&mut staged_ownership) {
        let _ = cleanup_uncommitted_ownership(ownership, space.as_deref()).await;
    }
    if let Some(error) = terminal {
        admission.poison(error.clone());
        progress.send_modify(|state| state.terminal_error = Some(error.clone()));
        let mut terminal_ownership = Vec::new();
        receiver.close();
        while let Some(command) = receiver.recv().await {
            match command {
                JournalCommand::Mutation {
                    record,
                    payload,
                    ram,
                    disk,
                    multipart_cleanup,
                } => {
                    drop(payload);
                    terminal_ownership.push(MutationOwnership {
                        sequence: record.sequence,
                        _ram: ram,
                        disk,
                        multipart_cleanup,
                    });
                }
                JournalCommand::Shutdown(done) => {
                    if shutdown.is_none() {
                        shutdown = Some(done);
                    } else {
                        let _ = done.send(());
                    }
                }
            }
        }
        while let Some(result) = preparations.next().await {
            if let Ok((_, prepared, ownership)) = result {
                if let Ok(mutation) = prepared {
                    sink.discard(mutation);
                }
                terminal_ownership.push(ownership);
            }
        }
        for (_, (result, ownership)) in prepared {
            if let Ok(mutation) = result {
                sink.discard(mutation);
            }
            terminal_ownership.push(ownership);
        }
        if let Some(cleanup_error) =
            cleanup_uncommitted_ownership(terminal_ownership, space.as_deref()).await
        {
            progress.send_modify(|state| {
                state.terminal_error = Some(format!("{error}; {cleanup_error}"));
            });
        }
    } else {
        admission.close();
    }
    if let Some(done) = shutdown {
        let _ = done.send(());
    }
    progress.send_modify(|state| state.closed = true);
}

fn terminal_or_closed(barrier: &LocalBarrier) -> LocalBarrierError {
    barrier.progress.terminal_or_closed()
}

fn current_terminal(barrier: &LocalBarrier) -> Option<LocalBarrierError> {
    barrier.progress.current_terminal()
}

#[cfg(test)]
mod tests {
    use super::{
        DEFAULT_LOCAL_PREPARE_CONCURRENCY, LocalBarrierError, LocalCommitObserver,
        LocalJournalSink, LocalJournaler, MAX_LOCAL_PUBLISH_BATCH_PAYLOAD_BYTES,
        MAX_LOCAL_PUBLISH_BATCH_RECORDS,
    };
    use crate::fault_store::FaultStore;
    use crate::writeback::admission::{Admission, AdmissionError};
    use crate::writeback::reservation::{SsdAdmission, SsdReservationRequest};
    use crate::writeback::space_sample::{PhysicalSpaceSample, PhysicalSpaceSampler};

    fn test_ssd(capacity: u64, min_free: u64) -> SsdAdmission {
        SsdAdmission::new(capacity, 1 << 20, 95, 85, min_free).unwrap()
    }

    async fn reserve_ssd(
        ssd: &SsdAdmission,
        bytes: u64,
        available: u64,
    ) -> crate::writeback::reservation::SsdReservationToken {
        ssd.reserve(
            SsdReservationRequest {
                ssd_reservation_bytes: bytes,
                physical_reservation_bytes: bytes,
                operations: 1,
            },
            PhysicalSpaceSample {
                generation: 1,
                available_bytes: available,
            },
        )
        .await
        .unwrap()
    }
    use crate::writeback::journal::{Journal, StagedBatch};
    use crate::writeback::model::{
        FenceClass, JournalIdentity, MutationMode, MutationRecord, Sequence,
    };
    use crate::writeback::multipart_reservation::{
        MultipartReservationSet, MutationReservation, SsdMultipartPartReservation,
        promote_multipart,
    };
    use crate::writeback::overlay::{OverlayCommitObserver, OverlayIndex};
    use crate::writeback::payload::VerifiedPayload;
    use crate::writeback::remote::RemoteScheduler;
    use anyhow::{Result, bail};
    use bytes::Bytes;
    use object_store::ObjectStoreExt;
    use object_store::memory::InMemory;
    use object_store::path::Path;
    use std::collections::{BTreeMap, HashMap};
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::{Arc, Mutex, mpsc};
    use std::time::Duration;
    use tokio::sync::Notify;
    use tokio::sync::mpsc as tokio_mpsc;

    struct BlockingSink {
        entered: tokio_mpsc::UnboundedSender<u64>,
        release: Mutex<mpsc::Receiver<()>>,
        fail_sequence: Option<u64>,
        committed: Mutex<Vec<u64>>,
        prepared_operations: AtomicU64,
        prepared_payload_bytes: AtomicU64,
    }

    struct ControlledPreparationSink {
        entered: tokio_mpsc::UnboundedSender<u64>,
        prepared: tokio_mpsc::UnboundedSender<u64>,
        releases: Mutex<HashMap<u64, mpsc::Receiver<()>>>,
        fail_sequence: Option<u64>,
        published: Mutex<Vec<u64>>,
        published_batches: Mutex<Vec<Vec<u64>>>,
        discarded: Mutex<Vec<u64>>,
    }

    struct ControlledBatchSink {
        prepare_entered: tokio_mpsc::UnboundedSender<u64>,
        prepared: tokio_mpsc::UnboundedSender<u64>,
        prepare_releases: Mutex<HashMap<u64, mpsc::Receiver<()>>>,
        publish_entered: tokio_mpsc::UnboundedSender<u64>,
        publish_release: Mutex<mpsc::Receiver<()>>,
    }

    struct HeadBlockingBatchSink {
        head_release: Mutex<Option<mpsc::Receiver<()>>>,
        prepared: tokio_mpsc::UnboundedSender<u64>,
        published_batches: Mutex<Vec<Vec<u64>>>,
    }

    /// Parks the first publication batch so a test can observe what the drain
    /// loop does with the rest of the backlog while one batch is being made
    /// durable.
    struct PublishParkingSink {
        prepared: tokio_mpsc::UnboundedSender<u64>,
        publish_entered: tokio_mpsc::UnboundedSender<usize>,
        publish_release: Mutex<Option<mpsc::Receiver<()>>>,
        published_batches: Mutex<Vec<Vec<u64>>>,
    }

    struct HeadBlockingJournalSink {
        journal: Arc<Journal>,
        head_release: Mutex<Option<mpsc::Receiver<()>>>,
        prepared: tokio_mpsc::UnboundedSender<u64>,
    }

    /// A real journal with the publication pipeline's two halves under test
    /// control: block a chosen batch inside the commit half, and/or fail the
    /// staging of the batch that starts at a chosen sequence.
    struct PipelineGateSink {
        journal: Arc<Journal>,
        fail_prepare_on: Option<Sequence>,
        panic_prepare_on: Option<Sequence>,
        prepare_entered: Option<tokio_mpsc::UnboundedSender<Sequence>>,
        fail_prepare_release: Mutex<Option<mpsc::Receiver<()>>>,
        commit_gate_on: Sequence,
        commit_entered: tokio_mpsc::UnboundedSender<Vec<Sequence>>,
        commit_release: Mutex<Option<mpsc::Receiver<()>>>,
        fail_stage_from: Option<Sequence>,
        staged: tokio_mpsc::UnboundedSender<Vec<Sequence>>,
        stage_gate_on: Option<Sequence>,
        stage_entered: Option<tokio_mpsc::UnboundedSender<Sequence>>,
        stage_release: Mutex<Option<mpsc::Receiver<()>>>,
    }

    impl PipelineGateSink {
        fn new(
            journal: Arc<Journal>,
            commit_gate_on: Sequence,
            commit_entered: tokio_mpsc::UnboundedSender<Vec<Sequence>>,
            commit_release: mpsc::Receiver<()>,
            staged: tokio_mpsc::UnboundedSender<Vec<Sequence>>,
        ) -> Self {
            Self {
                journal,
                fail_prepare_on: None,
                panic_prepare_on: None,
                prepare_entered: None,
                fail_prepare_release: Mutex::new(None),
                commit_gate_on,
                commit_entered,
                commit_release: Mutex::new(Some(commit_release)),
                fail_stage_from: None,
                staged,
                stage_gate_on: None,
                stage_entered: None,
                stage_release: Mutex::new(None),
            }
        }
    }

    impl LocalJournalSink for PipelineGateSink {
        fn prepare(
            &self,
            record: MutationRecord,
            payload: Option<&VerifiedPayload>,
        ) -> Result<crate::writeback::journal::PreparedMutation> {
            if let Some(entered) = &self.prepare_entered {
                entered.send(record.sequence).unwrap();
            }
            if self.fail_prepare_on == Some(record.sequence) {
                if let Some(release) = self.fail_prepare_release.lock().unwrap().take() {
                    release.recv().unwrap();
                }
                bail!("injected preparation failure at {}", record.sequence);
            }
            if self.panic_prepare_on == Some(record.sequence) {
                panic!("injected preparation panic at {}", record.sequence);
            }
            match payload {
                Some(payload) => self.journal.prepare_verified_put(record, payload),
                None => self.journal.prepare_metadata(record),
            }
        }

        fn publish(
            &self,
            prepared: crate::writeback::journal::PreparedMutation,
        ) -> Result<MutationRecord> {
            self.journal.publish_prepared(prepared)
        }

        fn stage_batch(
            &self,
            prepared: Vec<crate::writeback::journal::PreparedMutation>,
            expected_first: Sequence,
        ) -> Result<StagedBatch> {
            if self.fail_stage_from == Some(expected_first) {
                // An empty report marks the injected failure so the test can
                // wait for it instead of racing the pipeline.
                self.staged.send(Vec::new()).unwrap();
                bail!("injected staging failure at {expected_first}");
            }
            if let Some(entered) = &self.stage_entered {
                entered.send(expected_first).unwrap();
            }
            if self.stage_gate_on == Some(expected_first) {
                self.stage_release
                    .lock()
                    .unwrap()
                    .take()
                    .expect("stage release gate exists")
                    .recv()
                    .unwrap();
            }
            let staged = self.journal.stage_batch(prepared, expected_first)?;
            self.staged.send(staged.sequences()).unwrap();
            Ok(staged)
        }

        fn commit_staged(&self, staged: StagedBatch) -> Result<Vec<MutationRecord>> {
            let sequences = staged.sequences();
            let gated = sequences.contains(&self.commit_gate_on);
            self.commit_entered.send(sequences).unwrap();
            if gated {
                self.commit_release
                    .lock()
                    .unwrap()
                    .take()
                    .expect("commit release gate exists")
                    .recv()
                    .unwrap();
            }
            self.journal.commit_staged(staged)
        }

        fn discard_staged(&self, staged: StagedBatch) -> Result<()> {
            self.journal.discard_staged(staged)
        }
    }

    fn pipeline_journal(temp: &tempfile::TempDir) -> Arc<Journal> {
        Arc::new(
            Journal::open(
                temp.path().join("writeback"),
                JournalIdentity {
                    format_version: 1,
                    bucket_id: "bucket-pipeline".to_owned(),
                    backend_endpoint: "sftp://example.com:23".to_owned(),
                    database_prefix: "zerofs/pilot".to_owned(),
                    backend_kind: "sftp".to_owned(),
                    encryption_key_identity_sha256: [0x51; 32],
                },
            )
            .unwrap(),
        )
    }

    /// Records the size of every committed publication batch so throughput
    /// benchmarks can report how far a batch's fixed cost is amortized.
    struct BatchSizeObserver {
        sizes: Arc<Mutex<Vec<usize>>>,
    }

    #[async_trait::async_trait]
    impl LocalCommitObserver for BatchSizeObserver {
        async fn committed_batch(&self, records: &[MutationRecord]) -> Result<()> {
            self.sizes.lock().unwrap().push(records.len());
            Ok(())
        }
    }

    #[derive(Default)]
    struct BlockingObserver {
        entered: Notify,
        release: Notify,
    }

    struct FailingSecondObserver;

    struct BlockingSecondOverlayObserver {
        inner: OverlayCommitObserver,
        entered: Notify,
        release: Notify,
    }

    #[async_trait::async_trait]
    impl LocalCommitObserver for BlockingObserver {
        async fn committed_batch(&self, _records: &[MutationRecord]) -> Result<()> {
            self.entered.notify_one();
            self.release.notified().await;
            Ok(())
        }
    }

    #[async_trait::async_trait]
    impl LocalCommitObserver for FailingSecondObserver {
        async fn committed_batch(&self, records: &[MutationRecord]) -> Result<()> {
            if records.iter().any(|record| record.sequence == 2) {
                bail!("injected overlay handoff failure")
            }
            Ok(())
        }
    }

    #[async_trait::async_trait]
    impl LocalCommitObserver for BlockingSecondOverlayObserver {
        async fn committed_batch(&self, records: &[MutationRecord]) -> Result<()> {
            if records.iter().any(|record| record.sequence == 2) {
                self.entered.notify_one();
                self.release.notified().await;
            }
            self.inner.committed_batch(records).await
        }
    }

    #[tokio::test]
    async fn multipart_completion_holds_staging_and_journal_until_cleanup() {
        let temp = tempfile::tempdir().unwrap();
        let journal = pipeline_journal(&temp);
        let space = Arc::new(PhysicalSpaceSampler::new(journal.root().to_path_buf()));
        let sample = space.sample().await.unwrap();
        let ssd =
            SsdAdmission::recover(1_000_000, 100, 95, 85, 1, std::iter::empty(), Some(sample))
                .unwrap();
        let record = put_record(1, b"payload");
        let journal_bytes = record.ssd_reservation_bytes().unwrap();
        let reservation = SsdMultipartPartReservation::reserve(&ssd, 7, journal_bytes, 1, sample)
            .await
            .unwrap();
        let staging = temp.path().join("multipart");
        std::fs::create_dir(&staging).unwrap();
        let staged_payload = staging.join("payload.staged");
        std::fs::write(&staged_payload, b"payload").unwrap();
        let verified = VerifiedPayload::from_staged_file(staged_payload, 7).unwrap();
        let promoted = promote_multipart(MultipartReservationSet::Ssd(vec![reservation])).unwrap();
        let MutationReservation::Ssd(mutation) = promoted.mutation else {
            panic!("expected SSD multipart promotion")
        };
        let (disk, cleanup) = mutation.into_owned(staging.clone());
        let observer = Arc::new(BlockingObserver::default());
        let admission = Admission::new(64);
        let journaler = LocalJournaler::start_with_observer_and_space(
            journal,
            admission,
            8,
            1,
            Some(observer.clone()),
            space,
        )
        .unwrap();
        let barrier = journaler
            .submit(record, Some(verified), None, Some(disk), Some(cleanup))
            .await
            .unwrap();

        observer.entered.notified().await;
        assert!(staging.exists());
        assert_eq!(barrier.local_sequence(), 0);
        assert_eq!(ssd.used_bytes(), 7 + journal_bytes);

        observer.release.notify_one();
        barrier.wait_local(1).await.unwrap();
        assert!(!staging.exists());
        assert_eq!(ssd.used_bytes(), journal_bytes);
        assert_eq!(ssd.used_operations(), 1);
        journaler.shutdown().await.unwrap();
    }

    async fn assert_fatal_preparation_cleans_multipart(panic: bool) {
        let temp = tempfile::tempdir().unwrap();
        let journal = pipeline_journal(&temp);
        let space = Arc::new(PhysicalSpaceSampler::new(journal.root().to_path_buf()));
        let sample = space.sample().await.unwrap();
        let ssd =
            SsdAdmission::recover(1_000_000, 100, 95, 85, 1, std::iter::empty(), Some(sample))
                .unwrap();
        let record = put_record(1, b"payload");
        let journal_bytes = record.ssd_reservation_bytes().unwrap();
        let reservation = SsdMultipartPartReservation::reserve(&ssd, 7, journal_bytes, 1, sample)
            .await
            .unwrap();
        let staging = temp.path().join("multipart-failed-prepare");
        std::fs::create_dir(&staging).unwrap();
        let staged_payload = staging.join("payload.staged");
        std::fs::write(&staged_payload, b"payload").unwrap();
        let verified = VerifiedPayload::from_staged_file(staged_payload, 7).unwrap();
        let promoted = promote_multipart(MultipartReservationSet::Ssd(vec![reservation])).unwrap();
        let MutationReservation::Ssd(mutation) = promoted.mutation else {
            panic!("expected SSD multipart promotion")
        };
        let (disk, cleanup) = mutation.into_owned(staging.clone());
        let (commit_entered_tx, _commit_entered) = tokio_mpsc::unbounded_channel();
        let (staged_tx, _staged) = tokio_mpsc::unbounded_channel();
        let (_release_tx, release_rx) = mpsc::channel();
        let sink = Arc::new(PipelineGateSink {
            fail_prepare_on: (!panic).then_some(1),
            panic_prepare_on: panic.then_some(1),
            ..PipelineGateSink::new(
                journal,
                Sequence::MAX,
                commit_entered_tx,
                release_rx,
                staged_tx,
            )
        });
        let admission = Admission::new(64);
        let journaler = LocalJournaler::start_with_sink_observer_space(
            sink,
            admission,
            0,
            uuid::Uuid::nil(),
            8,
            1,
            None,
            Some(space),
        );

        let barrier = journaler
            .submit(record, Some(verified), None, Some(disk), Some(cleanup))
            .await
            .unwrap();
        let error = barrier.wait_local(1).await.unwrap_err();
        let expected = if panic {
            "local journal preparer panicked"
        } else {
            "injected preparation failure"
        };
        assert!(error.to_string().contains(expected));
        assert!(!staging.exists());
        assert_eq!(ssd.used_bytes(), 0);
        assert_eq!(ssd.used_operations(), 0);
        assert_eq!(ssd.outstanding_physical_claims(), 0);
        journaler.shutdown().await.unwrap_err();
    }

    #[tokio::test]
    async fn fatal_preparation_failure_cleans_multipart_staging_before_releasing_claims() {
        assert_fatal_preparation_cleans_multipart(false).await;
    }

    #[tokio::test]
    async fn preparation_panic_keeps_multipart_ownership_recoverable_for_sampled_cleanup() {
        assert_fatal_preparation_cleans_multipart(true).await;
    }

    #[tokio::test]
    async fn terminal_preparation_failure_sample_cleans_every_later_multipart_owner() {
        let temp = tempfile::tempdir().unwrap();
        let journal = pipeline_journal(&temp);
        let space = Arc::new(PhysicalSpaceSampler::new(journal.root().to_path_buf()));
        let sample = space.sample().await.unwrap();
        let ssd =
            SsdAdmission::recover(1_000_000, 100, 95, 85, 1, std::iter::empty(), Some(sample))
                .unwrap();
        let mut submissions = Vec::new();
        for sequence in 1..=3 {
            let record = put_record(sequence, b"payload");
            let journal_bytes = record.ssd_reservation_bytes().unwrap();
            let reservation =
                SsdMultipartPartReservation::reserve(&ssd, 7, journal_bytes, 1, sample)
                    .await
                    .unwrap();
            let staging = temp.path().join(format!("multipart-{sequence}"));
            std::fs::create_dir(&staging).unwrap();
            let staged_payload = staging.join("payload.staged");
            std::fs::write(&staged_payload, b"payload").unwrap();
            let verified = VerifiedPayload::from_staged_file(staged_payload, 7).unwrap();
            let promoted =
                promote_multipart(MultipartReservationSet::Ssd(vec![reservation])).unwrap();
            let MutationReservation::Ssd(mutation) = promoted.mutation else {
                panic!("expected SSD multipart promotion")
            };
            let (disk, cleanup) = mutation.into_owned(staging.clone());
            submissions.push((record, verified, disk, cleanup, staging));
        }
        let (prepare_entered_tx, mut prepare_entered) = tokio_mpsc::unbounded_channel();
        let (failure_release_tx, failure_release_rx) = mpsc::channel();
        let (commit_entered_tx, _commit_entered) = tokio_mpsc::unbounded_channel();
        let (staged_tx, _staged) = tokio_mpsc::unbounded_channel();
        let (_commit_release_tx, commit_release_rx) = mpsc::channel();
        let sink = Arc::new(PipelineGateSink {
            fail_prepare_on: Some(1),
            prepare_entered: Some(prepare_entered_tx),
            fail_prepare_release: Mutex::new(Some(failure_release_rx)),
            ..PipelineGateSink::new(
                journal,
                Sequence::MAX,
                commit_entered_tx,
                commit_release_rx,
                staged_tx,
            )
        });
        let admission = Admission::new(64);
        let journaler = LocalJournaler::start_with_sink_observer_space(
            sink,
            admission,
            0,
            uuid::Uuid::nil(),
            8,
            2,
            None,
            Some(space),
        );
        let mut barrier = None;
        let mut staging_paths = Vec::new();
        for (record, verified, disk, cleanup, staging) in submissions {
            barrier = Some(
                journaler
                    .submit(record, Some(verified), None, Some(disk), Some(cleanup))
                    .await
                    .unwrap(),
            );
            staging_paths.push(staging);
        }
        let mut entered = vec![prepare_entered.recv().await.unwrap()];
        entered.push(prepare_entered.recv().await.unwrap());
        entered.sort_unstable();
        assert_eq!(entered, vec![1, 2]);
        failure_release_tx.send(()).unwrap();

        let barrier = barrier.unwrap();
        barrier.wait_local(3).await.unwrap_err();
        journaler.shutdown().await.unwrap_err();
        tokio::time::timeout(Duration::from_secs(2), async {
            while !barrier.progress.snapshot().closed {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("terminal journaler did not finish draining multipart ownership");
        assert!(staging_paths.iter().all(|path| !path.exists()));
        assert_eq!(ssd.used_bytes(), 0);
        assert_eq!(ssd.used_operations(), 0);
        assert_eq!(ssd.outstanding_physical_claims(), 0);
    }

    impl LocalJournalSink for BlockingSink {
        fn prepare(
            &self,
            record: MutationRecord,
            payload: Option<&VerifiedPayload>,
        ) -> Result<crate::writeback::journal::PreparedMutation> {
            self.prepared_operations.fetch_add(1, Ordering::Relaxed);
            self.prepared_payload_bytes.fetch_add(
                payload.map_or(0, VerifiedPayload::byte_len),
                Ordering::Relaxed,
            );
            self.entered.send(record.sequence).unwrap();
            self.release.lock().unwrap().recv().unwrap();
            if self.fail_sequence == Some(record.sequence) {
                bail!("injected local fsync failure");
            }
            Ok(crate::writeback::journal::PreparedMutation::metadata(
                record,
            ))
        }

        fn publish(
            &self,
            prepared: crate::writeback::journal::PreparedMutation,
        ) -> Result<MutationRecord> {
            let sequence = prepared.sequence();
            let record = put_record(
                sequence,
                match sequence {
                    1 => b"one",
                    2 => b"two",
                    _ => b"x",
                },
            );
            self.committed.lock().unwrap().push(record.sequence);
            Ok(record)
        }
    }

    impl LocalJournalSink for ControlledPreparationSink {
        fn prepare(
            &self,
            record: MutationRecord,
            _payload: Option<&VerifiedPayload>,
        ) -> Result<crate::writeback::journal::PreparedMutation> {
            let sequence = record.sequence;
            self.entered.send(sequence).unwrap();
            let release = self
                .releases
                .lock()
                .unwrap()
                .remove(&sequence)
                .expect("release gate exists");
            release.recv().unwrap();
            if self.fail_sequence == Some(sequence) {
                bail!("injected local preparation failure");
            }
            self.prepared.send(sequence).unwrap();
            Ok(crate::writeback::journal::PreparedMutation::metadata(
                record,
            ))
        }

        fn publish(
            &self,
            prepared: crate::writeback::journal::PreparedMutation,
        ) -> Result<MutationRecord> {
            let sequence = prepared.sequence();
            self.published.lock().unwrap().push(sequence);
            Ok(put_record(sequence, b"x"))
        }

        fn publish_batch(
            &self,
            prepared: Vec<crate::writeback::journal::PreparedMutation>,
        ) -> Result<Vec<MutationRecord>> {
            let sequences = prepared
                .iter()
                .map(crate::writeback::journal::PreparedMutation::sequence)
                .collect::<Vec<_>>();
            self.published.lock().unwrap().extend(&sequences);
            self.published_batches
                .lock()
                .unwrap()
                .push(sequences.clone());
            Ok(sequences
                .into_iter()
                .map(|sequence| put_record(sequence, b"x"))
                .collect())
        }

        fn discard(&self, prepared: crate::writeback::journal::PreparedMutation) {
            self.discarded.lock().unwrap().push(prepared.sequence());
        }
    }

    impl LocalJournalSink for ControlledBatchSink {
        fn prepare(
            &self,
            record: MutationRecord,
            _payload: Option<&VerifiedPayload>,
        ) -> Result<crate::writeback::journal::PreparedMutation> {
            let sequence = record.sequence;
            self.prepare_entered.send(sequence).unwrap();
            let release = self
                .prepare_releases
                .lock()
                .unwrap()
                .remove(&sequence)
                .expect("release gate exists");
            release.recv().unwrap();
            self.prepared.send(sequence).unwrap();
            Ok(crate::writeback::journal::PreparedMutation::metadata(
                record,
            ))
        }

        fn publish(
            &self,
            prepared: crate::writeback::journal::PreparedMutation,
        ) -> Result<MutationRecord> {
            let sequence = prepared.sequence();
            self.publish_entered.send(sequence).unwrap();
            self.publish_release.lock().unwrap().recv().unwrap();
            Ok(put_record(sequence, b"x"))
        }
    }

    impl LocalJournalSink for HeadBlockingBatchSink {
        fn prepare(
            &self,
            record: MutationRecord,
            payload: Option<&VerifiedPayload>,
        ) -> Result<crate::writeback::journal::PreparedMutation> {
            if record.sequence == 1 {
                self.head_release
                    .lock()
                    .unwrap()
                    .take()
                    .expect("head release gate exists")
                    .recv()
                    .unwrap();
            }
            self.prepared.send(record.sequence).unwrap();
            Ok(crate::writeback::journal::PreparedMutation::for_test(
                record, payload,
            ))
        }

        fn publish(
            &self,
            prepared: crate::writeback::journal::PreparedMutation,
        ) -> Result<MutationRecord> {
            let sequence = prepared.sequence();
            self.published_batches.lock().unwrap().push(vec![sequence]);
            Ok(put_record(sequence, b"x"))
        }

        fn publish_batch(
            &self,
            prepared: Vec<crate::writeback::journal::PreparedMutation>,
        ) -> Result<Vec<MutationRecord>> {
            let sequences = prepared
                .iter()
                .map(crate::writeback::journal::PreparedMutation::sequence)
                .collect::<Vec<_>>();
            self.published_batches
                .lock()
                .unwrap()
                .push(sequences.clone());
            Ok(sequences
                .into_iter()
                .map(|sequence| put_record(sequence, b"x"))
                .collect())
        }
    }

    impl LocalJournalSink for PublishParkingSink {
        fn prepare(
            &self,
            record: MutationRecord,
            _payload: Option<&VerifiedPayload>,
        ) -> Result<crate::writeback::journal::PreparedMutation> {
            self.prepared.send(record.sequence).unwrap();
            Ok(crate::writeback::journal::PreparedMutation::metadata(
                record,
            ))
        }

        fn publish(
            &self,
            prepared: crate::writeback::journal::PreparedMutation,
        ) -> Result<MutationRecord> {
            self.publish_batch(vec![prepared]).map(|mut records| {
                records
                    .pop()
                    .expect("a single publication returns a record")
            })
        }

        fn publish_batch(
            &self,
            prepared: Vec<crate::writeback::journal::PreparedMutation>,
        ) -> Result<Vec<MutationRecord>> {
            let sequences = prepared
                .iter()
                .map(crate::writeback::journal::PreparedMutation::sequence)
                .collect::<Vec<_>>();
            self.publish_entered.send(sequences.len()).unwrap();
            if let Some(release) = self
                .publish_release
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .take()
            {
                release.recv().unwrap();
            }
            self.published_batches
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .push(sequences.clone());
            Ok(sequences
                .into_iter()
                .map(|sequence| put_record(sequence, b"x"))
                .collect())
        }
    }

    impl LocalJournalSink for HeadBlockingJournalSink {
        fn prepare(
            &self,
            record: MutationRecord,
            payload: Option<&VerifiedPayload>,
        ) -> Result<crate::writeback::journal::PreparedMutation> {
            if record.sequence == 1 {
                self.head_release
                    .lock()
                    .unwrap()
                    .take()
                    .expect("head release gate exists")
                    .recv()
                    .unwrap();
            }
            let sequence = record.sequence;
            let prepared = match payload {
                Some(payload) => self.journal.prepare_verified_put(record, payload)?,
                None => self.journal.prepare_metadata(record)?,
            };
            self.prepared.send(sequence).unwrap();
            Ok(prepared)
        }

        fn publish(
            &self,
            prepared: crate::writeback::journal::PreparedMutation,
        ) -> Result<MutationRecord> {
            self.journal.publish_prepared(prepared)
        }

        fn publish_batch(
            &self,
            prepared: Vec<crate::writeback::journal::PreparedMutation>,
        ) -> Result<Vec<MutationRecord>> {
            self.journal.publish_batch(prepared)
        }
    }

    fn put_record(sequence: u64, payload: &[u8]) -> MutationRecord {
        crate::writeback::test_util::put_record(
            sequence,
            &format!("segments/{sequence}"),
            payload,
            MutationMode::Create,
            FenceClass::ImmutableCreate,
            0x3000,
            0,
        )
    }

    fn blocking_journaler(
        admission: Admission,
        fail_sequence: Option<u64>,
    ) -> (
        LocalJournaler,
        tokio_mpsc::UnboundedReceiver<u64>,
        mpsc::Sender<()>,
        Arc<BlockingSink>,
    ) {
        let (entered_tx, entered_rx) = tokio_mpsc::unbounded_channel();
        let (release_tx, release_rx) = mpsc::channel();
        let sink = Arc::new(BlockingSink {
            entered: entered_tx,
            release: Mutex::new(release_rx),
            fail_sequence,
            committed: Mutex::new(Vec::new()),
            prepared_operations: AtomicU64::new(0),
            prepared_payload_bytes: AtomicU64::new(0),
        });
        let journaler = LocalJournaler::start_with_sink(sink.clone(), admission, 0, 8);
        (journaler, entered_rx, release_tx, sink)
    }

    type ControlledJournaler = (
        LocalJournaler,
        tokio_mpsc::UnboundedReceiver<u64>,
        tokio_mpsc::UnboundedReceiver<u64>,
        HashMap<u64, mpsc::Sender<()>>,
        Arc<ControlledPreparationSink>,
    );

    fn controlled_journaler(
        admission: Admission,
        sequences: std::ops::RangeInclusive<u64>,
        fail_sequence: Option<u64>,
    ) -> ControlledJournaler {
        let (entered_tx, entered_rx) = tokio_mpsc::unbounded_channel();
        let (prepared_tx, prepared_rx) = tokio_mpsc::unbounded_channel();
        let mut releases = HashMap::new();
        let mut release_senders = HashMap::new();
        for sequence in sequences {
            let (sender, receiver) = mpsc::channel();
            release_senders.insert(sequence, sender);
            releases.insert(sequence, receiver);
        }
        let sink = Arc::new(ControlledPreparationSink {
            entered: entered_tx,
            prepared: prepared_tx,
            releases: Mutex::new(releases),
            fail_sequence,
            published: Mutex::new(Vec::new()),
            published_batches: Mutex::new(Vec::new()),
            discarded: Mutex::new(Vec::new()),
        });
        let journaler = LocalJournaler::start_with_sink(sink.clone(), admission, 0, 8);
        (journaler, entered_rx, prepared_rx, release_senders, sink)
    }

    #[tokio::test]
    async fn dirty_ram_is_released_only_after_the_local_journal_commit() {
        let admission = Admission::new(10);
        let (journaler, mut entered, release, _) = blocking_journaler(admission.clone(), None);
        let ram = admission.reserve(7).await.unwrap().accept();
        let barrier = journaler
            .submit_put(
                put_record(1, b"payload"),
                Bytes::from_static(b"payload"),
                ram,
            )
            .await
            .unwrap();
        assert_eq!(entered.recv().await.unwrap(), 1);

        assert_eq!(admission.used_bytes(), 7);
        assert!(
            tokio::time::timeout(Duration::from_millis(25), barrier.wait_local(1))
                .await
                .is_err()
        );
        release.send(()).unwrap();
        barrier.wait_local(1).await.unwrap();
        assert_eq!(admission.used_bytes(), 0);
        journaler.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn dirty_ram_remains_owned_until_overlay_switches_to_the_ssd_blob() {
        let admission = Admission::new(10);
        let (entered_tx, mut entered_rx) = tokio_mpsc::unbounded_channel();
        let (release_tx, release_rx) = mpsc::channel();
        let sink = Arc::new(BlockingSink {
            entered: entered_tx,
            release: Mutex::new(release_rx),
            fail_sequence: None,
            committed: Mutex::new(Vec::new()),
            prepared_operations: AtomicU64::new(0),
            prepared_payload_bytes: AtomicU64::new(0),
        });
        let observer = Arc::new(BlockingObserver::default());
        let journaler = LocalJournaler::start_with_sink_and_observer(
            sink,
            admission.clone(),
            0,
            uuid::Uuid::nil(),
            8,
            DEFAULT_LOCAL_PREPARE_CONCURRENCY,
            Some(observer.clone()),
        );
        let ram = admission.reserve(7).await.unwrap().accept();
        let barrier = journaler
            .submit_put(
                put_record(1, b"payload"),
                Bytes::from_static(b"payload"),
                ram,
            )
            .await
            .unwrap();
        assert_eq!(entered_rx.recv().await.unwrap(), 1);
        release_tx.send(()).unwrap();
        observer.entered.notified().await;

        assert_eq!(admission.used_bytes(), 7);
        observer.release.notify_one();
        barrier.wait_local(1).await.unwrap();
        assert_eq!(admission.used_bytes(), 0);
        journaler.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn reserved_slots_enqueue_in_submission_order_not_reservation_order() {
        let admission = Admission::new(20);
        let (journaler, mut entered, release, sink) = blocking_journaler(admission.clone(), None);
        let first = admission.reserve(3).await.unwrap().accept();
        let second = admission.reserve(3).await.unwrap().accept();

        // Capacity may be reserved in any order; the queue must follow the
        // submit_reserved calls, which the store serializes in sequence order
        // under its admission-order lock.
        let early = journaler.reserve_slot().await.unwrap();
        let late = journaler.reserve_slot().await.unwrap();
        let barrier = journaler
            .submit_reserved(
                late,
                put_record(1, b"one"),
                Some(VerifiedPayload::new(Bytes::from_static(b"one"))),
                Some(first),
                None,
            )
            .await
            .unwrap();
        journaler
            .submit_reserved(
                early,
                put_record(2, b"two"),
                Some(VerifiedPayload::new(Bytes::from_static(b"two"))),
                Some(second),
                None,
            )
            .await
            .unwrap();

        let mut started = [entered.recv().await.unwrap(), entered.recv().await.unwrap()];
        started.sort_unstable();
        assert_eq!(started, [1, 2]);
        release.send(()).unwrap();
        release.send(()).unwrap();
        barrier.wait_local(2).await.unwrap();
        assert_eq!(*sink.committed.lock().unwrap(), vec![1, 2]);
        journaler.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn reserved_submit_close_race_returns_multipart_cleanup_ownership() {
        let temp = tempfile::tempdir().unwrap();
        let journal = pipeline_journal(&temp);
        let space = Arc::new(PhysicalSpaceSampler::new(journal.root().to_path_buf()));
        let sample = space.sample().await.unwrap();
        let ssd =
            SsdAdmission::recover(1_000_000, 100, 95, 85, 1, std::iter::empty(), Some(sample))
                .unwrap();
        let record = put_record(1, b"payload");
        let journal_bytes = record.ssd_reservation_bytes().unwrap();
        let reservation = SsdMultipartPartReservation::reserve(&ssd, 7, journal_bytes, 1, sample)
            .await
            .unwrap();
        let staging = temp.path().join("reserved-close-race");
        std::fs::create_dir(&staging).unwrap();
        let staged_payload = staging.join("payload.staged");
        std::fs::write(&staged_payload, b"payload").unwrap();
        let verified = VerifiedPayload::from_staged_file(staged_payload, 7).unwrap();
        let promoted = promote_multipart(MultipartReservationSet::Ssd(vec![reservation])).unwrap();
        let MutationReservation::Ssd(mutation) = promoted.mutation else {
            panic!("expected SSD multipart promotion")
        };
        let (disk, cleanup) = mutation.into_owned(staging.clone());
        let mut cleanup = Some(cleanup);
        let journaler = LocalJournaler::start_with_observer_and_space(
            journal,
            Admission::new(64),
            8,
            1,
            None,
            Arc::clone(&space),
        )
        .unwrap();
        let slot = journaler.reserve_slot().await.unwrap();
        let shutdown_journaler = journaler.clone();
        let shutdown = tokio::spawn(async move { shutdown_journaler.shutdown().await });
        while !journaler.inner.closed.load(Ordering::Acquire) {
            tokio::task::yield_now().await;
        }

        let error = journaler
            .submit_reserved_with_cleanup(
                slot,
                record,
                Some(verified),
                None,
                Some(disk),
                &mut cleanup,
            )
            .await
            .unwrap_err();
        assert!(matches!(error, LocalBarrierError::Closed));
        let cleaned = cleanup
            .take()
            .expect("closed submit retained cleanup")
            .remove()
            .unwrap();
        let fresh = space.sample().await.unwrap();
        cleaned.admission().observe_sample(fresh).unwrap();
        drop(cleaned);
        assert!(!staging.exists());
        shutdown.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn journaler_commits_and_advances_only_a_contiguous_sequence() {
        let admission = Admission::new(20);
        let (journaler, mut entered, release, sink) = blocking_journaler(admission.clone(), None);
        let first = admission.reserve(3).await.unwrap().accept();
        let second = admission.reserve(3).await.unwrap().accept();
        let barrier = journaler
            .submit_put(put_record(1, b"one"), Bytes::from_static(b"one"), first)
            .await
            .unwrap();
        journaler
            .submit_put(put_record(2, b"two"), Bytes::from_static(b"two"), second)
            .await
            .unwrap();

        let mut started = [entered.recv().await.unwrap(), entered.recv().await.unwrap()];
        started.sort_unstable();
        assert_eq!(started, [1, 2]);
        assert!(
            tokio::time::timeout(Duration::from_millis(25), barrier.wait_local(2))
                .await
                .is_err()
        );
        release.send(()).unwrap();
        release.send(()).unwrap();
        barrier.wait_local(2).await.unwrap();
        assert_eq!(*sink.committed.lock().unwrap(), vec![1, 2]);
        journaler.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn local_journal_prepares_multiple_payloads_concurrently() {
        let admission = Admission::new(20);
        let (journaler, mut entered, release, _) = blocking_journaler(admission.clone(), None);
        for (sequence, payload) in [(1, b"one".as_slice()), (2, b"two".as_slice())] {
            let ram = admission
                .reserve(payload.len() as u64)
                .await
                .unwrap()
                .accept();
            journaler
                .submit_put(
                    put_record(sequence, payload),
                    Bytes::copy_from_slice(payload),
                    ram,
                )
                .await
                .unwrap();
        }

        let first = entered.recv().await.unwrap();
        let second = tokio::time::timeout(Duration::from_millis(250), entered.recv())
            .await
            .expect("a second payload should enter local preparation before the first is released")
            .unwrap();
        let mut started = [first, second];
        started.sort_unstable();
        assert_eq!(started, [1, 2]);

        release.send(()).unwrap();
        release.send(()).unwrap();
        journaler.barrier().wait_local(2).await.unwrap();
        journaler.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn default_local_prepare_concurrency_is_bounded() {
        let cap = DEFAULT_LOCAL_PREPARE_CONCURRENCY as u64;
        let submitted = cap + 2;
        let admission = Admission::new(64);
        let (journaler, mut entered, release, sink) = blocking_journaler(admission.clone(), None);
        for sequence in 1..=submitted {
            let ram = admission.reserve(1).await.unwrap().accept();
            journaler
                .submit_put(put_record(sequence, b"x"), Bytes::from_static(b"x"), ram)
                .await
                .unwrap();
        }

        let mut started = Vec::new();
        for _ in 0..cap {
            started.push(
                tokio::time::timeout(Duration::from_millis(250), entered.recv())
                    .await
                    .expect("a full wave of local preparations should start")
                    .unwrap(),
            );
        }
        started.sort_unstable();
        assert_eq!(started, (1..=cap).collect::<Vec<_>>());
        assert!(
            tokio::time::timeout(Duration::from_millis(25), entered.recv())
                .await
                .is_err(),
            "a preparation beyond the default concurrency cap started early"
        );

        for _ in 0..submitted {
            release.send(()).unwrap();
        }
        journaler.barrier().wait_local(submitted).await.unwrap();
        assert_eq!(sink.prepared_operations.load(Ordering::Relaxed), submitted);
        assert_eq!(
            sink.prepared_payload_bytes.load(Ordering::Relaxed),
            submitted
        );
        journaler.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn out_of_order_preparation_cannot_advance_the_local_barrier() {
        let admission = Admission::new(20);
        let (journaler, mut entered, mut prepared, releases, sink) =
            controlled_journaler(admission.clone(), 1..=2, None);
        for sequence in 1..=2 {
            let ram = admission.reserve(1).await.unwrap().accept();
            journaler
                .submit_put(put_record(sequence, b"x"), Bytes::from_static(b"x"), ram)
                .await
                .unwrap();
        }
        let mut started = [entered.recv().await.unwrap(), entered.recv().await.unwrap()];
        started.sort_unstable();
        assert_eq!(started, [1, 2]);

        releases[&2].send(()).unwrap();
        assert_eq!(prepared.recv().await.unwrap(), 2);
        assert!(
            tokio::time::timeout(Duration::from_millis(25), journaler.barrier().wait_local(1))
                .await
                .is_err()
        );
        assert!(sink.published.lock().unwrap().is_empty());

        releases[&1].send(()).unwrap();
        journaler.barrier().wait_local(2).await.unwrap();
        assert_eq!(*sink.published.lock().unwrap(), vec![1, 2]);
        assert_eq!(*sink.published_batches.lock().unwrap(), vec![vec![1, 2]]);
        journaler.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn contiguous_prepared_batch_keeps_all_ram_until_the_batch_is_durable() {
        let admission = Admission::new(20);
        let (prepare_entered_tx, mut prepare_entered_rx) = tokio_mpsc::unbounded_channel();
        let (prepared_tx, mut prepared_rx) = tokio_mpsc::unbounded_channel();
        let (publish_entered_tx, mut publish_entered_rx) = tokio_mpsc::unbounded_channel();
        let (publish_release_tx, publish_release_rx) = mpsc::channel();
        let mut prepare_releases = HashMap::new();
        let mut prepare_release_senders = HashMap::new();
        for sequence in 1..=3 {
            let (sender, receiver) = mpsc::channel();
            prepare_release_senders.insert(sequence, sender);
            prepare_releases.insert(sequence, receiver);
        }
        let sink = Arc::new(ControlledBatchSink {
            prepare_entered: prepare_entered_tx,
            prepared: prepared_tx,
            prepare_releases: Mutex::new(prepare_releases),
            publish_entered: publish_entered_tx,
            publish_release: Mutex::new(publish_release_rx),
        });
        // Two preparation slots: records 1 and 2 fill them and record 3 waits
        // in the queue, so record 3 entering preparation later proves a slot
        // was freed by collecting a finished preparation.
        let journaler = LocalJournaler::start_with_sink_and_observer(
            sink,
            admission.clone(),
            0,
            uuid::Uuid::nil(),
            8,
            2,
            None,
        );
        for sequence in 1..=3 {
            let ram = admission.reserve(1).await.unwrap().accept();
            journaler
                .submit_put(put_record(sequence, b"x"), Bytes::from_static(b"x"), ram)
                .await
                .unwrap();
        }
        let mut entered = [
            prepare_entered_rx.recv().await.unwrap(),
            prepare_entered_rx.recv().await.unwrap(),
        ];
        entered.sort_unstable();
        assert_eq!(entered, [1, 2]);
        prepare_release_senders[&2].send(()).unwrap();
        assert_eq!(prepared_rx.recv().await.unwrap(), 2);
        // Record 1 is still gated, so the slot record 3 occupies here was
        // freed by record 2's collected preparation: sequence 2 is in the
        // prepared backlog before sequence 1 finishes, and the drain must
        // assemble [1, 2] as one batch. Without this ordering the drain may
        // observe sequence 1 alone and durably commit it as its own batch,
        // which releases record 1's RAM early and makes the assertions below
        // race.
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(5), prepare_entered_rx.recv())
                .await
                .expect("record 3 did not enter preparation after record 2 was collected")
                .expect("preparation-entry channel closed before record 3"),
            3
        );
        prepare_release_senders[&1].send(()).unwrap();
        assert_eq!(prepared_rx.recv().await.unwrap(), 1);

        assert_eq!(publish_entered_rx.recv().await.unwrap(), 1);
        assert_eq!(admission.used_bytes(), 3);
        publish_release_tx.send(()).unwrap();
        assert_eq!(publish_entered_rx.recv().await.unwrap(), 2);
        assert_eq!(
            admission.used_bytes(),
            3,
            "no admission may be released before the durable batch returns"
        );
        publish_release_tx.send(()).unwrap();

        journaler.barrier().wait_local(2).await.unwrap();
        assert_eq!(
            admission.used_bytes(),
            1,
            "the durable batch releases exactly its own records"
        );

        prepare_release_senders[&3].send(()).unwrap();
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(5), prepared_rx.recv())
                .await
                .expect("record 3 did not finish preparation")
                .expect("prepared channel closed before record 3"),
            3
        );
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(5), publish_entered_rx.recv())
                .await
                .expect("record 3 did not enter durable publication")
                .expect("publication-entry channel closed before record 3"),
            3
        );
        publish_release_tx.send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(5), journaler.barrier().wait_local(3))
            .await
            .expect("record 3 did not become locally durable")
            .unwrap();
        assert_eq!(admission.used_bytes(), 0);
        journaler.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn stalled_head_with_many_ready_records_publishes_bounded_batches_without_stalling() {
        const RECORDS: u64 = MAX_LOCAL_PUBLISH_BATCH_RECORDS as u64 * 2 + 6;
        let admission = Admission::new(RECORDS);
        let (head_release_tx, head_release_rx) = mpsc::channel();
        let (prepared_tx, mut prepared_rx) = tokio_mpsc::unbounded_channel();
        let sink = Arc::new(HeadBlockingBatchSink {
            head_release: Mutex::new(Some(head_release_rx)),
            prepared: prepared_tx,
            published_batches: Mutex::new(Vec::new()),
        });
        let queue = RECORDS as usize * 2;
        let journaler = LocalJournaler::start_with_sink_and_observer(
            sink.clone(),
            admission.clone(),
            0,
            uuid::Uuid::nil(),
            queue,
            queue,
            None,
        );
        for sequence in 1..=RECORDS {
            let ram = admission.reserve(1).await.unwrap().accept();
            journaler
                .submit_put(put_record(sequence, b"x"), Bytes::from_static(b"x"), ram)
                .await
                .unwrap();
        }
        for _ in 2..=RECORDS {
            tokio::time::timeout(Duration::from_secs(2), prepared_rx.recv())
                .await
                .expect("later preparation stalled behind the blocked head")
                .unwrap();
        }
        assert!(sink.published_batches.lock().unwrap().is_empty());

        head_release_tx.send(()).unwrap();
        journaler.barrier().wait_local(RECORDS).await.unwrap();

        assert_eq!(
            sink.published_batches
                .lock()
                .unwrap()
                .iter()
                .map(Vec::len)
                .collect::<Vec<_>>(),
            vec![
                MAX_LOCAL_PUBLISH_BATCH_RECORDS,
                MAX_LOCAL_PUBLISH_BATCH_RECORDS,
                6
            ]
        );
        assert_eq!(admission.used_bytes(), 0);
        journaler.shutdown().await.unwrap();
    }

    /// A batch's payload bytes are capped independently of its record count,
    /// because the container is written and fsynced inside the batch's
    /// critical path and its size is what bounds both that latency and the
    /// transient disk overhead of container reclamation.
    #[tokio::test]
    async fn publication_batches_are_bounded_by_container_payload_bytes() {
        const PAYLOAD: usize = 8 * 1024 * 1024;
        let records = (MAX_LOCAL_PUBLISH_BATCH_PAYLOAD_BYTES / PAYLOAD as u64) + 2;
        let admission = Admission::new(records * PAYLOAD as u64);
        let (head_release_tx, head_release_rx) = mpsc::channel();
        let (prepared_tx, mut prepared_rx) = tokio_mpsc::unbounded_channel();
        let sink = Arc::new(HeadBlockingBatchSink {
            head_release: Mutex::new(Some(head_release_rx)),
            prepared: prepared_tx,
            published_batches: Mutex::new(Vec::new()),
        });
        let payload = Bytes::from(vec![0x5a_u8; PAYLOAD]);
        let queue = records as usize * 2;
        let journaler = LocalJournaler::start_with_sink_and_observer(
            sink.clone(),
            admission.clone(),
            0,
            uuid::Uuid::nil(),
            queue,
            queue,
            None,
        );
        for sequence in 1..=records {
            let ram = admission.reserve(PAYLOAD as u64).await.unwrap().accept();
            journaler
                .submit_put(put_record(sequence, &payload), payload.clone(), ram)
                .await
                .unwrap();
        }
        for _ in 2..=records {
            tokio::time::timeout(Duration::from_secs(5), prepared_rx.recv())
                .await
                .expect("later preparation stalled behind the blocked head")
                .unwrap();
        }

        head_release_tx.send(()).unwrap();
        journaler.barrier().wait_local(records).await.unwrap();

        let batches = sink
            .published_batches
            .lock()
            .unwrap()
            .iter()
            .map(Vec::len)
            .collect::<Vec<_>>();
        let cap = (MAX_LOCAL_PUBLISH_BATCH_PAYLOAD_BYTES / PAYLOAD as u64) as usize;
        assert!(
            batches.iter().all(|batch| *batch <= cap),
            "a batch exceeded the container payload cap of {cap} records: {batches:?}"
        );
        assert_eq!(batches.iter().sum::<usize>(), records as usize);
        journaler.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn publication_keeps_preparing_the_next_batch_instead_of_draining_one_wave_at_a_time() {
        const RECORDS: u64 = 64;
        // The production prepare concurrency. With publication blocking the
        // drain loop, this also becomes the publication batch size, so every
        // batch pays the journal's fixed fsync + commit cost for four records.
        const PREPARE_CONCURRENCY: usize = 4;
        let admission = Admission::new(RECORDS);
        let (prepared_tx, mut prepared_rx) = tokio_mpsc::unbounded_channel();
        let (entered_tx, mut entered_rx) = tokio_mpsc::unbounded_channel();
        let (release_tx, release_rx) = mpsc::channel();
        let sink = Arc::new(PublishParkingSink {
            prepared: prepared_tx,
            publish_entered: entered_tx,
            publish_release: Mutex::new(Some(release_rx)),
            published_batches: Mutex::new(Vec::new()),
        });
        let journaler = LocalJournaler::start_with_sink_and_observer(
            sink.clone(),
            admission.clone(),
            0,
            uuid::Uuid::nil(),
            RECORDS as usize,
            PREPARE_CONCURRENCY,
            None,
        );
        for sequence in 1..=RECORDS {
            let ram = admission.reserve(1).await.unwrap().accept();
            journaler
                .submit_put(put_record(sequence, b"x"), Bytes::from_static(b"x"), ram)
                .await
                .unwrap();
        }

        let first_batch = tokio::time::timeout(Duration::from_secs(5), entered_rx.recv())
            .await
            .expect("no batch reached publication")
            .unwrap();
        assert!(
            first_batch <= PREPARE_CONCURRENCY,
            "the first batch cannot exceed the records prepared so far: {first_batch}"
        );

        // While that batch is parked in publication the SSD write pipeline is
        // idle, so the drain loop must keep preparing the rest of the backlog.
        for _ in 1..=RECORDS {
            tokio::time::timeout(Duration::from_secs(5), prepared_rx.recv())
                .await
                .expect("publication stalled preparation of the remaining backlog")
                .unwrap();
        }

        release_tx.send(()).unwrap();
        journaler.barrier().wait_local(RECORDS).await.unwrap();

        let batches = sink
            .published_batches
            .lock()
            .unwrap()
            .iter()
            .map(Vec::len)
            .collect::<Vec<_>>();
        assert_eq!(
            batches.iter().sum::<usize>(),
            RECORDS as usize,
            "{batches:?}"
        );
        // Up to PREPARE_CONCURRENCY finished preparations may still be
        // uncollected when the parked batch releases, so the exact batch
        // count is timing-coupled; what matters is that some later batch
        // exceeds what one preparation wave could supply.
        assert!(
            batches.iter().max().copied().unwrap_or(0) > PREPARE_CONCURRENCY,
            "publication batches stayed capped at the prepare concurrency: {batches:?}"
        );
        assert_eq!(admission.used_bytes(), 0);
        journaler.shutdown().await.unwrap();
    }

    /// Post-ACK durability drain throughput against a real journal on real
    /// disk, at the production prepare concurrency. Ignored by default because
    /// it is timing sensitive and does real fsyncs; run it with
    /// `cargo test --release --lib drain_throughput -- --ignored --nocapture`.
    ///
    /// Records and payload digests are built before the clock starts. In
    /// production those SHA-256 passes happen once per payload on the
    /// submitting task, spread across many concurrent writers; doing them
    /// inside the timed loop instead measured one core's hash rate (~1.5
    /// GiB/s, halved by hashing each payload for both the record and the
    /// verified payload) and capped every result near 500 MiB/s no matter how
    /// fast the drain got.
    ///
    /// When comparing these numbers against a device baseline, do not use
    /// `dd if=/dev/zero`. The SSD this was tuned on special-cases all-zero
    /// blocks: the same write loop sustains ~1940 MiB/s of zeros and ~745
    /// MiB/s of either 0x5a or pseudorandom bytes, so a zero-filled baseline
    /// overstates the device by ~2.5x and makes a drain running at the device
    /// ceiling look like it is at 30% of it. The payload here is deliberately
    /// non-zero for the same reason.
    #[tokio::test]
    #[ignore = "throughput benchmark; needs a real disk and --release"]
    async fn drain_throughput_of_the_post_ack_durability_tail() {
        for (payload_bytes, records) in [
            (64 * 1024_usize, 2048_u64),
            (256 * 1024, 1024),
            (1024 * 1024, 512),
        ] {
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
            let payload = Bytes::from(vec![0x5a_u8; payload_bytes]);
            let total_bytes = records * payload_bytes as u64;
            let admission = Admission::new(total_bytes);
            let sizes = Arc::new(Mutex::new(Vec::new()));
            let journaler = LocalJournaler::start_with_observer(
                journal.clone(),
                admission.clone(),
                records as usize,
                DEFAULT_LOCAL_PREPARE_CONCURRENCY,
                Some(Arc::new(BatchSizeObserver {
                    sizes: sizes.clone(),
                })),
            )
            .unwrap();

            let verified = VerifiedPayload::new(payload.clone());
            let prebuilt = (1..=records)
                .map(|sequence| put_record(sequence, &payload))
                .collect::<Vec<_>>();

            let started = std::time::Instant::now();
            for record in prebuilt {
                let ram = admission
                    .reserve(payload_bytes as u64)
                    .await
                    .unwrap()
                    .accept();
                journaler
                    .submit_verified_put(record, verified.clone(), ram)
                    .await
                    .unwrap();
            }
            journaler.barrier().wait_local(records).await.unwrap();
            let elapsed = started.elapsed();

            let sizes = sizes.lock().unwrap().clone();
            println!(
                "drained {records} x {} KiB records ({:.1} MiB) in {:.3}s = {:.1} MiB/s, {:.0} ops/s \
                 [{} batches, mean {:.1}, max {}]",
                payload_bytes / 1024,
                total_bytes as f64 / (1024.0 * 1024.0),
                elapsed.as_secs_f64(),
                total_bytes as f64 / (1024.0 * 1024.0) / elapsed.as_secs_f64(),
                records as f64 / elapsed.as_secs_f64(),
                sizes.len(),
                records as f64 / sizes.len() as f64,
                sizes.iter().copied().max().unwrap_or(0),
            );
            journaler.shutdown().await.unwrap();
        }
    }

    #[tokio::test]
    async fn encoded_record_bytes_bound_each_ready_publication_batch() {
        let admission = Admission::new(3);
        let (head_release_tx, head_release_rx) = mpsc::channel();
        let (prepared_tx, mut prepared_rx) = tokio_mpsc::unbounded_channel();
        let sink = Arc::new(HeadBlockingBatchSink {
            head_release: Mutex::new(Some(head_release_rx)),
            prepared: prepared_tx,
            published_batches: Mutex::new(Vec::new()),
        });
        let journaler = LocalJournaler::start_with_sink_and_observer(
            sink.clone(),
            admission.clone(),
            0,
            uuid::Uuid::nil(),
            8,
            8,
            None,
        );
        for sequence in 1..=3 {
            let mut record = put_record(sequence, b"x");
            record.path = "x".repeat(8 * 1024 * 1024);
            let ram = admission.reserve(1).await.unwrap().accept();
            journaler
                .submit_put(record, Bytes::from_static(b"x"), ram)
                .await
                .unwrap();
        }
        for _ in 2..=3 {
            tokio::time::timeout(Duration::from_secs(2), prepared_rx.recv())
                .await
                .expect("later preparation stalled behind the blocked head")
                .unwrap();
        }

        head_release_tx.send(()).unwrap();
        journaler.barrier().wait_local(3).await.unwrap();

        assert_eq!(
            sink.published_batches
                .lock()
                .unwrap()
                .iter()
                .map(Vec::len)
                .collect::<Vec<_>>(),
            vec![1, 1, 1]
        );
        journaler.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn observer_failure_retains_durable_batch_ram_and_accepts_all_ssd_ownership() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("writeback");
        let identity = JournalIdentity {
            format_version: 1,
            bucket_id: "bucket-observer-failure".to_owned(),
            backend_endpoint: "sftp://example.com:23".to_owned(),
            database_prefix: "zerofs/pilot".to_owned(),
            backend_kind: "sftp".to_owned(),
            encryption_key_identity_sha256: [0x55; 32],
        };
        let journal = Arc::new(Journal::open(&root, identity.clone()).unwrap());
        let admission = Admission::new(10);
        let disk = test_ssd(100, 10);
        let (head_release_tx, head_release_rx) = mpsc::channel();
        let (prepared_tx, mut prepared_rx) = tokio_mpsc::unbounded_channel();
        let sink = Arc::new(HeadBlockingJournalSink {
            journal: journal.clone(),
            head_release: Mutex::new(Some(head_release_rx)),
            prepared: prepared_tx,
        });
        let journaler = LocalJournaler::start_with_sink_and_observer(
            sink,
            admission.clone(),
            0,
            uuid::Uuid::nil(),
            8,
            8,
            Some(Arc::new(FailingSecondObserver)),
        );
        for sequence in 1..=2 {
            let ram = admission.reserve(1).await.unwrap().accept();
            let disk_permit = reserve_ssd(&disk, 1, 1_000).await;
            journaler
                .submit_put_with_disk(
                    put_record(sequence, b"x"),
                    Bytes::from_static(b"x"),
                    ram,
                    disk_permit,
                )
                .await
                .unwrap();
        }
        assert_eq!(prepared_rx.recv().await.unwrap(), 2);
        head_release_tx.send(()).unwrap();

        assert!(matches!(
            journaler.barrier().wait_local(1).await,
            Err(LocalBarrierError::LocalDurability(_))
        ));
        assert_eq!(journal.progress().unwrap().local_seq, 2);
        assert_eq!(journaler.barrier().local_sequence(), 0);
        assert_eq!(
            admission.used_bytes(),
            2,
            "the RAM for the unobserved durable batch remains owned"
        );
        assert_eq!(disk.used_bytes(), 2);
        assert!(matches!(
            journaler.shutdown().await,
            Err(LocalBarrierError::LocalDurability(_))
        ));
        drop(journaler);
        tokio::time::timeout(Duration::from_secs(2), async {
            while admission.used_bytes() != 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("retained RAM ownership was not released with the journaler");
        drop(journal);

        let recovered = Journal::open(&root, identity).unwrap();
        assert_eq!(recovered.progress().unwrap().local_seq, 2);
        assert_eq!(recovered.read_blob(1).unwrap(), b"x");
        assert_eq!(recovered.read_blob(2).unwrap(), b"x");
    }

    #[tokio::test]
    async fn active_remote_scheduler_waits_for_every_observer_in_a_durable_batch() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("writeback");
        let journal = Arc::new(
            Journal::open(
                &root,
                JournalIdentity {
                    format_version: 1,
                    bucket_id: "bucket-active-remote".to_owned(),
                    backend_endpoint: "memory://remote".to_owned(),
                    database_prefix: "zerofs/pilot".to_owned(),
                    backend_kind: "memory".to_owned(),
                    encryption_key_identity_sha256: [0x66; 32],
                },
            )
            .unwrap(),
        );
        let remote_data = Arc::new(InMemory::new());
        let (remote, remote_controls) = FaultStore::new(remote_data.clone());
        let overlay = OverlayIndex::new(remote.clone());
        let admission = Admission::new(1_000_000);
        let disk = Arc::new(test_ssd(1_000_000, 1));
        let space = Arc::new(PhysicalSpaceSampler::new(root.clone()));
        let observer = Arc::new(BlockingSecondOverlayObserver {
            inner: OverlayCommitObserver::new(overlay.clone(), journal.clone()),
            entered: Notify::new(),
            release: Notify::new(),
        });
        let (head_release_tx, head_release_rx) = mpsc::channel();
        let (prepared_tx, mut prepared_rx) = tokio_mpsc::unbounded_channel();
        let sink = Arc::new(HeadBlockingJournalSink {
            journal: journal.clone(),
            head_release: Mutex::new(Some(head_release_rx)),
            prepared: prepared_tx,
        });
        let journaler = LocalJournaler::start_with_sink_and_observer(
            sink,
            admission.clone(),
            0,
            uuid::Uuid::nil(),
            8,
            8,
            Some(observer.clone()),
        );
        let remote_scheduler = RemoteScheduler::start(
            remote,
            journal.clone(),
            overlay.clone(),
            admission.clone(),
            Arc::clone(&disk),
            Arc::clone(&space),
            journaler.barrier(),
            4,
        )
        .unwrap();
        for (sequence, payload) in [(1, b"one".as_slice()), (2, b"two".as_slice())] {
            let record = put_record(sequence, payload);
            overlay
                .install_memory(record.clone(), Bytes::copy_from_slice(payload))
                .await
                .unwrap();
            let ram = admission
                .reserve(payload.len() as u64)
                .await
                .unwrap()
                .accept();
            let disk_permit =
                reserve_ssd(&disk, record.ssd_reservation_bytes().unwrap(), 1_000_000).await;
            journaler
                .submit_put_with_disk(record, Bytes::copy_from_slice(payload), ram, disk_permit)
                .await
                .unwrap();
        }
        assert_eq!(prepared_rx.recv().await.unwrap(), 2);
        head_release_tx.send(()).unwrap();
        observer.entered.notified().await;

        assert_eq!(journal.progress().unwrap().local_seq, 2);
        assert_eq!(
            journaler.barrier().local_sequence(),
            0,
            "the local barrier must not expose a partially observed durable batch"
        );
        assert_eq!(
            remote_controls.put_count(),
            0,
            "the active remote scheduler must not see the unobserved durable tail"
        );

        observer.release.notify_one();
        journaler.barrier().wait_local(2).await.unwrap();
        remote_scheduler.barrier().wait_remote(2).await.unwrap();
        assert_eq!(
            remote_data
                .get(&Path::from("segments/2"))
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap(),
            Bytes::from_static(b"two")
        );
        remote_scheduler.shutdown().await.unwrap();
        journaler.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn preparation_failure_blocks_later_publication_and_discards_prepared_work() {
        let admission = Admission::new(20);
        let (journaler, mut entered, mut prepared, releases, sink) =
            controlled_journaler(admission.clone(), 1..=2, Some(1));
        for sequence in 1..=2 {
            let ram = admission.reserve(1).await.unwrap().accept();
            journaler
                .submit_put(put_record(sequence, b"x"), Bytes::from_static(b"x"), ram)
                .await
                .unwrap();
        }
        let mut started = [entered.recv().await.unwrap(), entered.recv().await.unwrap()];
        started.sort_unstable();
        assert_eq!(started, [1, 2]);

        releases[&2].send(()).unwrap();
        assert_eq!(prepared.recv().await.unwrap(), 2);
        releases[&1].send(()).unwrap();

        assert!(matches!(
            journaler.barrier().wait_local(1).await,
            Err(LocalBarrierError::LocalDurability(_))
        ));
        // Barriers wake as soon as the terminal error is published, before the
        // drain discards prepared work and releases its admission permits, so
        // quiesce via shutdown before observing the discard outcome.
        assert!(matches!(
            journaler.shutdown().await,
            Err(LocalBarrierError::LocalDurability(_))
        ));
        assert!(sink.published.lock().unwrap().is_empty());
        assert_eq!(*sink.discarded.lock().unwrap(), vec![2]);
        assert_eq!(admission.used_bytes(), 0);
    }

    #[tokio::test]
    async fn known_preparation_failure_wakes_barriers_before_blocked_cleanup_finishes() {
        let admission = Admission::new(20);
        let (journaler, mut entered, _prepared, releases, _sink) =
            controlled_journaler(admission.clone(), 1..=2, Some(1));
        for sequence in 1..=2 {
            let ram = admission.reserve(1).await.unwrap().accept();
            journaler
                .submit_put(put_record(sequence, b"x"), Bytes::from_static(b"x"), ram)
                .await
                .unwrap();
        }
        let mut started = [entered.recv().await.unwrap(), entered.recv().await.unwrap()];
        started.sort_unstable();
        assert_eq!(started, [1, 2]);

        releases[&1].send(()).unwrap();
        let result = tokio::time::timeout(
            Duration::from_millis(250),
            journaler.barrier().wait_local(1),
        )
        .await;
        releases[&2].send(()).unwrap();

        assert!(
            matches!(result, Ok(Err(LocalBarrierError::LocalDurability(_)))),
            "the known failure stayed hidden behind blocked cleanup: {result:?}"
        );
        assert!(matches!(
            admission.reserve(1).await,
            Err(AdmissionError::Poisoned(_))
        ));
        assert!(matches!(
            journaler.shutdown().await,
            Err(LocalBarrierError::LocalDurability(_))
        ));
    }

    #[tokio::test]
    async fn local_failure_poisons_new_admission_and_every_barrier() {
        let admission = Admission::new(10);
        let (journaler, mut entered, release, _) = blocking_journaler(admission.clone(), Some(1));
        let ram = admission.reserve(7).await.unwrap().accept();
        let barrier = journaler
            .submit_put(
                put_record(1, b"payload"),
                Bytes::from_static(b"payload"),
                ram,
            )
            .await
            .unwrap();
        assert_eq!(entered.recv().await.unwrap(), 1);
        release.send(()).unwrap();

        let error = barrier.wait_local(1).await.unwrap_err();
        assert!(matches!(error, LocalBarrierError::LocalDurability(_)));
        assert!(matches!(
            admission.reserve(1).await,
            Err(AdmissionError::Poisoned(_))
        ));
        assert_eq!(admission.used_bytes(), 0);
        assert!(matches!(
            journaler.shutdown().await,
            Err(LocalBarrierError::LocalDurability(_))
        ));
    }

    #[tokio::test]
    async fn successful_local_commit_moves_bytes_from_ram_ownership_to_ssd_ownership() {
        let admission = Admission::new(10);
        let disk = test_ssd(100, 10);
        let (journaler, mut entered, release, _) = blocking_journaler(admission.clone(), None);
        let ram = admission.reserve(7).await.unwrap().accept();
        let disk_permit = reserve_ssd(&disk, 7, 1_000).await;
        let barrier = journaler
            .submit_put_with_disk(
                put_record(1, b"payload"),
                Bytes::from_static(b"payload"),
                ram,
                disk_permit,
            )
            .await
            .unwrap();
        assert_eq!(entered.recv().await.unwrap(), 1);

        assert_eq!(admission.used_bytes(), 7);
        assert_eq!(disk.used_bytes(), 7);
        release.send(()).unwrap();
        barrier.wait_local(1).await.unwrap();
        assert_eq!(admission.used_bytes(), 0);
        assert_eq!(disk.used_bytes(), 7);

        disk.release_remote(
            SsdReservationRequest {
                ssd_reservation_bytes: 7,
                physical_reservation_bytes: 7,
                operations: 1,
            },
            PhysicalSpaceSample {
                generation: 2,
                available_bytes: 1_000,
            },
        )
        .unwrap();
        assert_eq!(disk.used_bytes(), 0);
        journaler.shutdown().await.unwrap();
    }

    /// The durability contract across the pipeline's seam: a record's bytes
    /// being durable is not enough to ACK it. Observed from inside the commit
    /// half -- after staging fsynced the container, before the record commit
    /// runs -- the payload is on disk, the watermark has not moved, and
    /// `wait_local` must not release.
    #[tokio::test]
    async fn a_record_is_not_ackable_until_its_batch_commits_even_though_its_bytes_are_durable() {
        let temp = tempfile::tempdir().unwrap();
        let journal = pipeline_journal(&temp);
        let (commit_entered_tx, mut commit_entered) = tokio_mpsc::unbounded_channel();
        let (staged_tx, _staged) = tokio_mpsc::unbounded_channel();
        let (release_tx, release_rx) = mpsc::channel();
        let sink = Arc::new(PipelineGateSink::new(
            journal.clone(),
            1,
            commit_entered_tx,
            release_rx,
            staged_tx,
        ));
        let admission = Admission::new(64);
        let journaler = LocalJournaler::start_with_sink_and_observer(
            sink,
            admission.clone(),
            0,
            uuid::Uuid::nil(),
            8,
            8,
            None,
        );
        let ram = admission.reserve(7).await.unwrap().accept();
        let barrier = journaler
            .submit_put(
                put_record(1, b"payload"),
                Bytes::from_static(b"payload"),
                ram,
            )
            .await
            .unwrap();

        assert_eq!(
            tokio::time::timeout(Duration::from_secs(5), commit_entered.recv())
                .await
                .expect("the batch never reached the commit half")
                .unwrap(),
            vec![1]
        );

        // Staging has fsynced the container, so the bytes are durable...
        let container = journal
            .root()
            .join("blobs/00/0000000000000001-0000000000000001.blobs");
        assert!(container.exists(), "staged bytes must be durable");
        // ...but nothing references them yet, so nothing may be ACKed.
        assert_eq!(journal.progress().unwrap().local_seq, 0);
        assert_eq!(barrier.local_sequence(), 0);
        assert!(
            tokio::time::timeout(Duration::from_millis(250), barrier.wait_local(1))
                .await
                .is_err(),
            "wait_local released before the batch committed"
        );

        release_tx.send(()).unwrap();

        barrier.wait_local(1).await.unwrap();
        assert_eq!(journal.progress().unwrap().local_seq, 1);
        assert_eq!(journal.read_blob(1).unwrap(), b"payload");
        journaler.shutdown().await.unwrap();
    }

    /// The overlap itself: while batch 1 is parked inside its commit, batch 2
    /// must get all the way through the staging half and have its container
    /// on disk. Serial publication would leave batch 2 untouched until batch
    /// 1 finished, so this is what a regression to serial would break.
    #[tokio::test]
    async fn the_next_batch_stages_while_the_previous_batch_commits() {
        let temp = tempfile::tempdir().unwrap();
        let journal = pipeline_journal(&temp);
        let (commit_entered_tx, mut commit_entered) = tokio_mpsc::unbounded_channel();
        let (staged_tx, mut staged) = tokio_mpsc::unbounded_channel();
        let (release_tx, release_rx) = mpsc::channel();
        let sink = Arc::new(PipelineGateSink::new(
            journal.clone(),
            1,
            commit_entered_tx,
            release_rx,
            staged_tx,
        ));
        let admission = Admission::new(64);
        let journaler = LocalJournaler::start_with_sink_and_observer(
            sink,
            admission.clone(),
            0,
            uuid::Uuid::nil(),
            8,
            8,
            None,
        );

        let ram = admission.reserve(7).await.unwrap().accept();
        let barrier = journaler
            .submit_put(
                put_record(1, b"payload"),
                Bytes::from_static(b"payload"),
                ram,
            )
            .await
            .unwrap();
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(5), staged.recv())
                .await
                .expect("batch 1 never staged")
                .unwrap(),
            vec![1]
        );
        tokio::time::timeout(Duration::from_secs(5), commit_entered.recv())
            .await
            .expect("batch 1 never reached the commit half")
            .unwrap();

        let ram = admission.reserve(5).await.unwrap().accept();
        journaler
            .submit_put(put_record(2, b"again"), Bytes::from_static(b"again"), ram)
            .await
            .unwrap();

        assert_eq!(
            tokio::time::timeout(Duration::from_secs(5), staged.recv())
                .await
                .expect("batch 2 did not stage while batch 1 was committing")
                .unwrap(),
            vec![2]
        );
        let second = journal
            .root()
            .join("blobs/00/0000000000000002-0000000000000002.blobs");
        assert!(
            second.exists(),
            "batch 2's bytes must be durable while batch 1 is still committing"
        );
        // Overlapping the writes must not overlap the ACKs: neither batch is
        // ACKable while batch 1's commit is parked.
        assert_eq!(journal.progress().unwrap().local_seq, 0);
        assert_eq!(barrier.local_sequence(), 0);

        release_tx.send(()).unwrap();

        barrier.wait_local(2).await.unwrap();
        assert_eq!(journal.read_blob(1).unwrap(), b"payload");
        assert_eq!(journal.read_blob(2).unwrap(), b"again");
        journaler.shutdown().await.unwrap();
    }

    /// The drain's last-resort guard. Its branch is unreachable by
    /// construction -- submission is contiguous and every received sequence
    /// ends up in `prepared`, so the backlog can never strand one below
    /// `next_admitted` -- which is exactly why it exists: `select!` panics
    /// when every branch is disabled, and a panicked journaler task loses the
    /// terminal error that poisons admission. Cover the diagnosis it reports.
    #[test]
    fn a_stalled_drain_names_the_sequence_and_what_is_stranded_behind_it() {
        let mut prepared: BTreeMap<Sequence, super::PreparedEntry> = BTreeMap::new();
        for sequence in [7_u64, 9] {
            prepared.insert(
                sequence,
                (
                    Ok(crate::writeback::journal::PreparedMutation::metadata(
                        crate::writeback::test_util::delete_record(
                            sequence,
                            "obsolete",
                            FenceClass::Fence,
                            0x2000,
                            0,
                        ),
                    )),
                    super::MutationOwnership {
                        sequence,
                        _ram: None,
                        disk: None,
                        multipart_cleanup: None,
                    },
                ),
            );
        }

        let error = super::stalled_drain_error(6, &prepared);

        assert_eq!(
            error,
            "local journal drain stalled at sequence 6 with stranded preparations [7, 9]"
        );
        assert_eq!(
            super::stalled_drain_error(6, &BTreeMap::new()),
            "local journal drain stalled at sequence 6 with stranded preparations []"
        );
    }

    /// `assemble_batch` will happily end a batch on a payload-free record, so
    /// the production drain really does produce Put-then-Delete batches. Take
    /// one all the way through the real journaler and the real journal, drain
    /// it fully, and require that the container is reclaimed and the journal
    /// reopens -- the shape that used to leak a container permanently and then
    /// refuse to open at all.
    #[tokio::test]
    async fn a_payload_free_tail_batch_drains_and_reopens_through_the_real_journaler() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("writeback");
        let identity = JournalIdentity {
            format_version: 1,
            bucket_id: "bucket-pipeline".to_owned(),
            backend_endpoint: "sftp://example.com:23".to_owned(),
            database_prefix: "zerofs/pilot".to_owned(),
            backend_kind: "sftp".to_owned(),
            encryption_key_identity_sha256: [0x51; 32],
        };
        let journal = Arc::new(Journal::open(&root, identity.clone()).unwrap());
        let admission = Admission::new(64);
        let journaler = LocalJournaler::start(journal.clone(), admission.clone(), 8).unwrap();

        let disk = test_ssd(1_000_000, 1);
        let ram = admission.reserve(7).await.unwrap().accept();
        journaler
            .submit_put(
                put_record(1, b"payload"),
                Bytes::from_static(b"payload"),
                ram,
            )
            .await
            .unwrap();
        let barrier = journaler
            .submit_metadata_with_disk(
                crate::writeback::test_util::delete_record(
                    2,
                    "obsolete",
                    FenceClass::Fence,
                    0x2000,
                    0,
                ),
                reserve_ssd(&disk, 10, 1_000_000).await,
            )
            .await
            .unwrap();
        barrier.wait_local(2).await.unwrap();

        // Whatever batching the drain chose, every container it published must
        // be named by a record that references it.
        let snapshot = journal.snapshot().unwrap();
        for record in &snapshot.records {
            let Some(reference) = record.blob_path() else {
                continue;
            };
            let relative = reference.split('#').next().unwrap();
            let last = relative
                .rsplit('/')
                .next()
                .and_then(|name| name.strip_suffix(".blobs"))
                .and_then(|stem| stem.split_once('-'))
                .map(|(_, last)| u64::from_str_radix(last, 16).unwrap())
                .expect("a container names its sequence range");
            assert!(
                snapshot
                    .records
                    .iter()
                    .any(|other| other.sequence == last && other.blob_path().is_some()),
                "container {relative} is named by a record that does not reference it"
            );
        }

        journaler.shutdown().await.unwrap();
        for sequence in 1..=2 {
            journal
                .mark_remote(sequence, Some(format!("etag-{sequence}")))
                .unwrap();
            journal.remove_remote_prefix(sequence).unwrap();
        }
        let blobs = root.join("blobs");
        let mut leaked = Vec::new();
        for shard in std::fs::read_dir(&blobs).unwrap() {
            for blob in std::fs::read_dir(shard.unwrap().path()).unwrap() {
                leaked.push(blob.unwrap().path());
            }
        }
        assert!(
            leaked.is_empty(),
            "containers leaked after a full drain: {leaked:?}"
        );
        drop(journaler);
        drop(journal);

        let recovered =
            Journal::open(&root, identity).expect("a fully drained journal must reopen");
        assert_eq!(recovered.progress().unwrap().remote_seq, 2);
    }

    /// Two containers must be able to write at once. A single writer leaves
    /// the device short of queue depth, which is exactly the deficit that made
    /// one serial container slower than the per-record layout it replaced at
    /// large records.
    #[tokio::test]
    async fn two_containers_write_concurrently() {
        let temp = tempfile::tempdir().unwrap();
        let journal = pipeline_journal(&temp);
        let (commit_entered_tx, _commit_entered) = tokio_mpsc::unbounded_channel();
        let (staged_tx, _staged) = tokio_mpsc::unbounded_channel();
        let (stage_entered_tx, mut stage_entered) = tokio_mpsc::unbounded_channel();
        let (commit_release_tx, commit_release_rx) = mpsc::channel();
        let (stage_release_tx, stage_release_rx) = mpsc::channel();
        let sink = Arc::new(PipelineGateSink {
            // Park the first container mid-write so the second must overlap it.
            stage_gate_on: Some(1),
            stage_entered: Some(stage_entered_tx),
            stage_release: Mutex::new(Some(stage_release_rx)),
            // Nothing commits until both have entered staging.
            commit_gate_on: Sequence::MAX,
            ..PipelineGateSink::new(
                journal.clone(),
                Sequence::MAX,
                commit_entered_tx,
                commit_release_rx,
                staged_tx,
            )
        });
        let admission = Admission::new(64);
        let journaler = LocalJournaler::start_with_sink_and_observer(
            sink,
            admission.clone(),
            0,
            uuid::Uuid::nil(),
            8,
            8,
            None,
        );

        let ram = admission.reserve(7).await.unwrap().accept();
        let barrier = journaler
            .submit_put(
                put_record(1, b"payload"),
                Bytes::from_static(b"payload"),
                ram,
            )
            .await
            .unwrap();
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(5), stage_entered.recv())
                .await
                .expect("batch 1 never entered staging")
                .unwrap(),
            1
        );

        let ram = admission.reserve(5).await.unwrap().accept();
        journaler
            .submit_put(put_record(2, b"again"), Bytes::from_static(b"again"), ram)
            .await
            .unwrap();

        assert_eq!(
            tokio::time::timeout(Duration::from_secs(5), stage_entered.recv())
                .await
                .expect("batch 2 did not start writing while batch 1 was still writing")
                .unwrap(),
            2,
            "a second container must be writable while the first is in flight"
        );

        stage_release_tx.send(()).unwrap();
        drop(commit_release_tx);

        barrier.wait_local(2).await.unwrap();
        assert_eq!(journal.read_blob(1).unwrap(), b"payload");
        assert_eq!(journal.read_blob(2).unwrap(), b"again");
        journaler.shutdown().await.unwrap();
    }

    /// The fault path the overlap introduces: staging batch 2 fails while
    /// batch 1 is still inside its commit. The in-flight commit must still be
    /// awaited and honoured -- sequence 1 stays durable and ACKable -- while
    /// sequence 2 is refused, and the journal reopens on exactly the
    /// committed prefix with no container left above the watermark.
    #[tokio::test]
    async fn a_staging_failure_behind_an_in_flight_commit_keeps_the_committed_prefix() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("writeback");
        let journal = pipeline_journal(&temp);
        let (commit_entered_tx, mut commit_entered) = tokio_mpsc::unbounded_channel();
        let (staged_tx, mut staged) = tokio_mpsc::unbounded_channel();
        let (release_tx, release_rx) = mpsc::channel();
        let sink = Arc::new(PipelineGateSink {
            fail_stage_from: Some(2),
            ..PipelineGateSink::new(journal.clone(), 1, commit_entered_tx, release_rx, staged_tx)
        });
        let admission = Admission::new(64);
        let journaler = LocalJournaler::start_with_sink_and_observer(
            sink,
            admission.clone(),
            0,
            uuid::Uuid::nil(),
            8,
            8,
            None,
        );

        let ram = admission.reserve(7).await.unwrap().accept();
        let barrier = journaler
            .submit_put(
                put_record(1, b"payload"),
                Bytes::from_static(b"payload"),
                ram,
            )
            .await
            .unwrap();
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(5), staged.recv())
                .await
                .expect("batch 1 never staged")
                .unwrap(),
            vec![1]
        );
        tokio::time::timeout(Duration::from_secs(5), commit_entered.recv())
            .await
            .expect("batch 1 never reached the commit half")
            .unwrap();

        // Batch 2 stages while batch 1 is parked inside its commit.
        let ram = admission.reserve(5).await.unwrap().accept();
        journaler
            .submit_put(put_record(2, b"again"), Bytes::from_static(b"again"), ram)
            .await
            .unwrap();
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(5), staged.recv())
                .await
                .expect("batch 2 never reached the staging half")
                .unwrap(),
            Vec::<Sequence>::new(),
            "batch 2 should have reported its injected staging failure"
        );

        release_tx.send(()).unwrap();

        barrier
            .wait_local(1)
            .await
            .expect("the in-flight commit must still be honoured");
        let refused = barrier.wait_local(2).await.unwrap_err();
        assert!(
            matches!(refused, LocalBarrierError::LocalDurability(ref message)
                if message.contains("injected staging failure")),
            "{refused:?}"
        );
        assert!(journaler.shutdown().await.is_err());
        drop(journaler);
        drop(journal);

        let recovered = Journal::open(
            &root,
            JournalIdentity {
                format_version: 1,
                bucket_id: "bucket-pipeline".to_owned(),
                backend_endpoint: "sftp://example.com:23".to_owned(),
                database_prefix: "zerofs/pilot".to_owned(),
                backend_kind: "sftp".to_owned(),
                encryption_key_identity_sha256: [0x51; 32],
            },
        )
        .expect("the journal must reopen on the committed prefix");
        assert_eq!(recovered.progress().unwrap().local_seq, 1);
        assert_eq!(recovered.read_blob(1).unwrap(), b"payload");
        assert!(recovered.mutation(2).unwrap().is_none());
    }

    #[tokio::test]
    async fn production_journaler_persists_a_real_blob_before_advancing_local_barrier() {
        let temp = tempfile::tempdir().unwrap();
        let journal = Arc::new(
            Journal::open(
                temp.path().join("writeback"),
                JournalIdentity {
                    format_version: 1,
                    bucket_id: "bucket-a".to_owned(),
                    backend_endpoint: "sftp://example.com:23".to_owned(),
                    database_prefix: "zerofs/pilot".to_owned(),
                    backend_kind: "sftp".to_owned(),
                    encryption_key_identity_sha256: [0x44; 32],
                },
            )
            .unwrap(),
        );
        let admission = Admission::new(10);
        let journaler = LocalJournaler::start(journal.clone(), admission.clone(), 2).unwrap();
        let ram = admission.reserve(7).await.unwrap().accept();
        let barrier = journaler
            .submit_put(
                put_record(1, b"payload"),
                Bytes::from_static(b"payload"),
                ram,
            )
            .await
            .unwrap();

        barrier.wait_local(1).await.unwrap();
        assert_eq!(journal.snapshot().unwrap().local_seq, 1);
        assert_eq!(journal.read_blob(1).unwrap(), b"payload");
        assert_eq!(admission.used_bytes(), 0);
        journaler.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn shutdown_rejects_new_work_and_wakes_blocked_local_waiters() {
        let admission = Admission::new(10);
        let (journaler, mut entered, release, _) = blocking_journaler(admission.clone(), None);
        let ram = admission.reserve(7).await.unwrap().accept();
        let barrier = journaler
            .submit_put(
                put_record(1, b"payload"),
                Bytes::from_static(b"payload"),
                ram,
            )
            .await
            .unwrap();
        assert_eq!(entered.recv().await.unwrap(), 1);
        let shutdown = tokio::spawn({
            let journaler = journaler.clone();
            async move { journaler.shutdown().await }
        });
        assert!(!shutdown.is_finished());
        release.send(()).unwrap();
        shutdown.await.unwrap().unwrap();
        barrier.wait_local(1).await.unwrap();

        let ram = Admission::new(1).reserve(1).await.unwrap().accept();
        let error = journaler
            .submit_put(put_record(2, b"x"), Bytes::from_static(b"x"), ram)
            .await
            .unwrap_err();
        assert_eq!(error, LocalBarrierError::Closed);
    }

    #[tokio::test]
    async fn canceled_shutdown_while_queue_is_full_does_not_lose_shutdown_ownership() {
        let admission = Admission::new(10);
        let (entered_tx, mut entered) = tokio_mpsc::unbounded_channel();
        let (release, release_rx) = mpsc::channel();
        let sink = Arc::new(BlockingSink {
            entered: entered_tx,
            release: Mutex::new(release_rx),
            fail_sequence: None,
            committed: Mutex::new(Vec::new()),
            prepared_operations: AtomicU64::new(0),
            prepared_payload_bytes: AtomicU64::new(0),
        });
        let journaler = LocalJournaler::start_with_sink_and_observer(
            sink,
            admission.clone(),
            0,
            uuid::Uuid::nil(),
            1,
            1,
            None,
        );
        let first = admission.reserve(1).await.unwrap().accept();
        journaler
            .submit_put(put_record(1, b"x"), Bytes::from_static(b"x"), first)
            .await
            .unwrap();
        assert_eq!(entered.recv().await.unwrap(), 1);
        let second = admission.reserve(1).await.unwrap().accept();
        journaler
            .submit_put(put_record(2, b"x"), Bytes::from_static(b"x"), second)
            .await
            .unwrap();

        let first_shutdown = tokio::spawn({
            let journaler = journaler.clone();
            async move { journaler.shutdown().await }
        });
        while !journaler.inner.closed.load(Ordering::Acquire) {
            tokio::task::yield_now().await;
        }
        assert!(!first_shutdown.is_finished());
        first_shutdown.abort();
        assert!(first_shutdown.await.unwrap_err().is_cancelled());

        let mut second_shutdown = tokio::spawn({
            let journaler = journaler.clone();
            async move { journaler.shutdown().await }
        });
        release.send(()).unwrap();
        assert_eq!(entered.recv().await.unwrap(), 2);
        release.send(()).unwrap();

        let result = tokio::time::timeout(Duration::from_millis(250), &mut second_shutdown).await;
        if result.is_err() {
            second_shutdown.abort();
        }
        assert!(
            matches!(result, Ok(Ok(Ok(())))),
            "second shutdown did not complete: {result:?}"
        );
    }

    #[tokio::test]
    async fn shutdown_surfaces_the_stored_local_durability_failure() {
        let admission = Admission::new(10);
        let (journaler, mut entered, release, _) = blocking_journaler(admission.clone(), Some(1));
        let ram = admission.reserve(1).await.unwrap().accept();
        journaler
            .submit_put(put_record(1, b"x"), Bytes::from_static(b"x"), ram)
            .await
            .unwrap();
        assert_eq!(entered.recv().await.unwrap(), 1);
        release.send(()).unwrap();
        assert!(matches!(
            journaler.barrier().wait_local(1).await,
            Err(LocalBarrierError::LocalDurability(_))
        ));

        assert!(matches!(
            journaler.shutdown().await,
            Err(LocalBarrierError::LocalDurability(_))
        ));
    }

    #[tokio::test]
    async fn local_watermark_uses_the_newer_observed_space_sample() {
        use crate::writeback::reservation::{SsdAdmission, SsdReservationRequest};
        use crate::writeback::space_sample::PhysicalSpaceSample;
        use std::sync::atomic::{AtomicBool, Ordering};

        let admission = SsdAdmission::new(1_000, 8, 100, 50, 10).unwrap();
        let token = admission
            .reserve(
                SsdReservationRequest {
                    ssd_reservation_bytes: 20,
                    physical_reservation_bytes: 12,
                    operations: 1,
                },
                PhysicalSpaceSample {
                    generation: 1,
                    available_bytes: 1_000,
                },
            )
            .await
            .unwrap();
        let published = AtomicBool::new(false);
        let (committed, ()) = super::transition_reservations_then_publish_local(
            vec![token],
            &[12],
            PhysicalSpaceSample {
                generation: 0,
                available_bytes: 1_000,
            },
            || published.store(true, Ordering::SeqCst),
        )
        .unwrap();
        assert!(published.load(Ordering::SeqCst));
        assert_eq!(committed[0].sample().generation, 1);
        assert_eq!(admission.used_bytes(), 20);
    }

    #[tokio::test]
    async fn batch_uses_one_fresh_sample() {
        use crate::writeback::reservation::{SsdAdmission, SsdReservationRequest};
        use crate::writeback::space_sample::PhysicalSpaceSample;

        let admission = SsdAdmission::new(1_000, 8, 100, 50, 10).unwrap();
        let sample = PhysicalSpaceSample {
            generation: 4,
            available_bytes: 2_000,
        };
        let mut tokens = Vec::new();
        for bytes in [10_u64, 15] {
            tokens.push(
                admission
                    .reserve(
                        SsdReservationRequest {
                            ssd_reservation_bytes: bytes,
                            physical_reservation_bytes: bytes,
                            operations: 1,
                        },
                        PhysicalSpaceSample {
                            generation: 3,
                            available_bytes: 2_000,
                        },
                    )
                    .await
                    .unwrap(),
            );
        }
        let (committed, generation) =
            super::transition_reservations_then_publish_local(tokens, &[10, 15], sample, || {
                sample.generation
            })
            .unwrap();
        assert_eq!(generation, 4);
        assert!(committed.iter().all(|item| item.sample() == sample));
    }

    #[tokio::test]
    async fn stale_transition_sample_uses_newer_observation_without_poison() {
        use crate::writeback::reservation::{SsdAdmission, SsdReservationRequest};
        use crate::writeback::space_sample::PhysicalSpaceSample;

        let admission = SsdAdmission::new(1_000, 8, 100, 50, 10).unwrap();
        let token = admission
            .reserve(
                SsdReservationRequest {
                    ssd_reservation_bytes: 20,
                    physical_reservation_bytes: 12,
                    operations: 1,
                },
                PhysicalSpaceSample {
                    generation: 2,
                    available_bytes: 1_000,
                },
            )
            .await
            .unwrap();
        let committed = token
            .commit_local(
                12,
                PhysicalSpaceSample {
                    generation: 1,
                    available_bytes: 1_000,
                },
            )
            .unwrap();
        assert_eq!(committed.sample().generation, 2);
        assert_eq!(admission.used_bytes(), 20);
        let next = admission
            .reserve(
                SsdReservationRequest {
                    ssd_reservation_bytes: 1,
                    physical_reservation_bytes: 1,
                    operations: 1,
                },
                PhysicalSpaceSample {
                    generation: 3,
                    available_bytes: 1_000,
                },
            )
            .await
            .unwrap();
        drop(next);
    }

    #[tokio::test]
    async fn token_cannot_commit_or_release_twice() {
        use crate::writeback::reservation::{
            ReservationError, SsdAdmission, SsdReservationRequest,
        };
        use crate::writeback::space_sample::PhysicalSpaceSample;

        let admission = SsdAdmission::new(1_000, 8, 100, 50, 10).unwrap();
        let token = admission
            .reserve(
                SsdReservationRequest {
                    ssd_reservation_bytes: 8,
                    physical_reservation_bytes: 8,
                    operations: 1,
                },
                PhysicalSpaceSample {
                    generation: 1,
                    available_bytes: 1_000,
                },
            )
            .await
            .unwrap();
        let committed = token
            .commit_local(
                8,
                PhysicalSpaceSample {
                    generation: 1,
                    available_bytes: 1_000,
                },
            )
            .unwrap();
        assert!(matches!(
            committed.commit_local(
                8,
                PhysicalSpaceSample {
                    generation: 2,
                    available_bytes: 1_000,
                },
            ),
            Err(ReservationError::Poisoned(_))
        ));
        let token = admission
            .reserve(
                SsdReservationRequest {
                    ssd_reservation_bytes: 9,
                    physical_reservation_bytes: 9,
                    operations: 1,
                },
                PhysicalSpaceSample {
                    generation: 2,
                    available_bytes: 1_000,
                },
            )
            .await
            .unwrap();
        let committed = token
            .commit_local(
                9,
                PhysicalSpaceSample {
                    generation: 2,
                    available_bytes: 1_000,
                },
            )
            .unwrap();
        assert!(matches!(
            committed.release(),
            Err(ReservationError::Poisoned(_))
        ));
        assert_eq!(admission.used_bytes(), 17);
    }
}
