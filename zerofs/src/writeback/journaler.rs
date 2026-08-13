use crate::writeback::admission::{AcceptedAdmission, Admission, DiskPermit};
use crate::writeback::journal::{Journal, PreparedMutation};
use crate::writeback::model::{MutationRecord, Sequence};
use crate::writeback::payload::VerifiedPayload;
use anyhow::Result as AnyResult;
use bytes::Bytes;
use futures::{FutureExt, StreamExt, stream::FuturesUnordered};
use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::Mutex as StdMutex;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::sync::{Mutex, mpsc, oneshot, watch};
use tokio::task::JoinHandle;

#[async_trait::async_trait]
pub trait LocalCommitObserver: Send + Sync + 'static {
    async fn committed(&self, sequence: Sequence) -> AnyResult<()>;
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum LocalBarrierError {
    #[error("local writeback journal is closed")]
    Closed,
    #[error("local writeback durability failed: {0}")]
    LocalDurability(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct LocalProgress {
    local_seq: Sequence,
    terminal_error: Option<String>,
    closed: bool,
}

#[derive(Debug, Clone)]
pub struct LocalBarrier {
    progress: watch::Receiver<LocalProgress>,
}

impl LocalBarrier {
    pub fn local_sequence(&self) -> Sequence {
        self.progress.borrow().local_seq
    }

    pub async fn wait_local(&self, sequence: Sequence) -> Result<(), LocalBarrierError> {
        let mut progress = self.progress.clone();
        loop {
            let state = progress.borrow().clone();
            if state.local_seq >= sequence {
                return Ok(());
            }
            if let Some(error) = state.terminal_error {
                return Err(LocalBarrierError::LocalDurability(error));
            }
            if state.closed {
                return Err(LocalBarrierError::Closed);
            }
            progress
                .changed()
                .await
                .map_err(|_| LocalBarrierError::Closed)?;
        }
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

    fn discard(&self, prepared: PreparedMutation) -> AnyResult<()>;
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

    fn discard(&self, prepared: PreparedMutation) -> AnyResult<()> {
        self.discard_prepared(prepared)
    }
}

const DEFAULT_LOCAL_PREPARE_CONCURRENCY: usize = 4;
// Blob payloads are already external files, so this bounds the serialized
// mutation metadata retained by one redb transaction without throttling large
// payload throughput. The record cap separately bounds transaction work when
// mutations are individually tiny.
const MAX_LOCAL_PUBLISH_BATCH_RECORDS: usize = 64;
const MAX_LOCAL_PUBLISH_BATCH_RECORD_BYTES: usize = 16 * 1024 * 1024;

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
    _retained_ram: Arc<StdMutex<Vec<AcceptedAdmission>>>,
}

struct LocalJournalerOwnership {
    admission: Admission,
    retained_ram: Arc<StdMutex<Vec<AcceptedAdmission>>>,
}

enum JournalCommand {
    Mutation {
        record: Box<MutationRecord>,
        payload: Option<VerifiedPayload>,
        ram: Option<AcceptedAdmission>,
        disk: Option<DiskPermit>,
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
        let local_sequence = journal.progress()?.local_seq;
        Ok(Self::start_with_sink_and_observer(
            journal,
            admission,
            local_sequence,
            queue_depth,
            prepare_concurrency,
            observer,
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
            queue_depth,
            DEFAULT_LOCAL_PREPARE_CONCURRENCY,
            None,
        )
    }

    fn start_with_sink_and_observer(
        sink: Arc<dyn LocalJournalSink>,
        admission: Admission,
        local_sequence: Sequence,
        queue_depth: usize,
        prepare_concurrency: usize,
        observer: Option<Arc<dyn LocalCommitObserver>>,
    ) -> Self {
        let (sender, receiver) = mpsc::channel(queue_depth.max(1));
        let (progress_sender, progress) = watch::channel(LocalProgress {
            local_seq: local_sequence,
            terminal_error: None,
            closed: false,
        });
        let (shutdown_result, _) = watch::channel(None);
        let retained_ram = Arc::new(StdMutex::new(Vec::new()));
        let join = tokio::spawn(run_journaler(
            sink,
            receiver,
            progress_sender,
            local_sequence,
            observer,
            prepare_concurrency,
            LocalJournalerOwnership {
                admission,
                retained_ram: retained_ram.clone(),
            },
        ));
        Self {
            inner: Arc::new(LocalJournalerInner {
                sender,
                barrier: LocalBarrier { progress },
                admission_gate: Mutex::new(()),
                closed: AtomicBool::new(false),
                join: Mutex::new(Some(join)),
                shutdown_result,
                _retained_ram: retained_ram,
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
        self.submit(record, Some(payload), Some(ram), None).await
    }

    pub async fn submit_put_with_disk(
        &self,
        record: MutationRecord,
        payload: Bytes,
        ram: AcceptedAdmission,
        disk: DiskPermit,
    ) -> Result<LocalBarrier, LocalBarrierError> {
        self.submit_verified_put_with_disk(record, VerifiedPayload::new(payload), ram, disk)
            .await
    }

    pub(crate) async fn submit_verified_put_with_disk(
        &self,
        record: MutationRecord,
        payload: VerifiedPayload,
        ram: AcceptedAdmission,
        disk: DiskPermit,
    ) -> Result<LocalBarrier, LocalBarrierError> {
        self.submit(record, Some(payload), Some(ram), Some(disk))
            .await
    }

    pub async fn submit_metadata_with_disk(
        &self,
        record: MutationRecord,
        disk: DiskPermit,
    ) -> Result<LocalBarrier, LocalBarrierError> {
        self.submit(record, None, None, Some(disk)).await
    }

    async fn submit(
        &self,
        record: MutationRecord,
        payload: Option<VerifiedPayload>,
        ram: Option<AcceptedAdmission>,
        disk: Option<DiskPermit>,
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
        disk: Option<DiskPermit>,
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
        });
        Ok(self.inner.barrier.clone())
    }

    pub async fn shutdown(&self) -> Result<(), LocalBarrierError> {
        let mut completion = self.inner.shutdown_result.subscribe();
        let mut local_progress = self.inner.barrier.progress.clone();
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
            if let Some(error) = local_progress.borrow().terminal_error.clone() {
                return Err(LocalBarrierError::LocalDurability(error));
            }
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

async fn run_journaler(
    sink: Arc<dyn LocalJournalSink>,
    mut receiver: mpsc::Receiver<JournalCommand>,
    progress: watch::Sender<LocalProgress>,
    local_sequence: Sequence,
    observer: Option<Arc<dyn LocalCommitObserver>>,
    prepare_concurrency: usize,
    ownership: LocalJournalerOwnership,
) {
    let LocalJournalerOwnership {
        admission,
        retained_ram,
    } = ownership;
    let prepare_concurrency = prepare_concurrency.max(1);
    let mut next_admitted = local_sequence.saturating_add(1);
    let mut next_received = local_sequence.checked_add(1);
    type Preparation = (
        Sequence,
        AnyResult<PreparedMutation>,
        Option<AcceptedAdmission>,
        Option<DiskPermit>,
    );
    type PreparedEntry = (
        AnyResult<PreparedMutation>,
        Option<AcceptedAdmission>,
        Option<DiskPermit>,
    );
    let mut preparations: FuturesUnordered<JoinHandle<Preparation>> = FuturesUnordered::new();
    let mut prepared: BTreeMap<Sequence, PreparedEntry> = BTreeMap::new();
    let mut shutdown = None;
    let mut input_closed = false;
    let mut terminal = None;

    loop {
        while terminal.is_none() {
            match preparations.next().now_or_never() {
                Some(Some(Ok((sequence, result, ram, disk)))) => {
                    prepared.insert(sequence, (result, ram, disk));
                }
                Some(Some(Err(error))) => {
                    terminal = Some(format!("local journal preparer panicked: {error}"));
                }
                Some(None) | None => break,
            }
        }

        while terminal.is_none() {
            let mut mutations = Vec::new();
            let mut ownership = Vec::new();
            let mut encoded_record_bytes = 0_usize;
            let mut candidate = Some(next_admitted);
            while let Some(sequence) = candidate {
                let Some((result, _, _)) = prepared.get(&sequence) else {
                    break;
                };
                if result.is_err() {
                    if mutations.is_empty() {
                        let (result, ram, disk) = prepared
                            .remove(&sequence)
                            .expect("the expected failed preparation still exists");
                        drop(ram);
                        drop(disk);
                        let error = match result {
                            Err(error) => error,
                            Ok(_) => unreachable!("the preparation result was checked above"),
                        };
                        terminal = Some(format!("{error:#}"));
                    }
                    break;
                }
                if mutations.len() == MAX_LOCAL_PUBLISH_BATCH_RECORDS {
                    break;
                }
                let mutation_bytes = match result {
                    Ok(mutation) => match mutation.encoded_record_bytes() {
                        Ok(bytes) => bytes,
                        Err(error) => {
                            if mutations.is_empty() {
                                let (_, ram, disk) = prepared
                                    .remove(&sequence)
                                    .expect("the unsized prepared mutation still exists");
                                drop(ram);
                                drop(disk);
                                terminal = Some(format!("{error:#}"));
                            }
                            break;
                        }
                    },
                    Err(_) => unreachable!("preparation errors are handled above"),
                };
                if mutation_bytes > MAX_LOCAL_PUBLISH_BATCH_RECORD_BYTES {
                    if mutations.is_empty() {
                        let (_, ram, disk) = prepared
                            .remove(&sequence)
                            .expect("the oversized prepared mutation still exists");
                        drop(ram);
                        drop(disk);
                        terminal = Some(format!(
                            "prepared journal mutation {sequence} encodes to {mutation_bytes} bytes, exceeding the {MAX_LOCAL_PUBLISH_BATCH_RECORD_BYTES}-byte local publication batch limit"
                        ));
                    }
                    break;
                }
                let Some(next_encoded_bytes) = encoded_record_bytes.checked_add(mutation_bytes)
                else {
                    if mutations.is_empty() {
                        let (_, ram, disk) = prepared
                            .remove(&sequence)
                            .expect("the overflowed prepared mutation still exists");
                        drop(ram);
                        drop(disk);
                        terminal = Some("local publication batch byte count overflow".to_owned());
                    }
                    break;
                };
                if next_encoded_bytes > MAX_LOCAL_PUBLISH_BATCH_RECORD_BYTES {
                    break;
                }
                let (result, ram, disk) = prepared
                    .remove(&sequence)
                    .expect("the expected prepared mutation still exists");
                mutations.push(result.expect("the prepared mutation was checked above"));
                ownership.push((sequence, ram, disk));
                encoded_record_bytes = next_encoded_bytes;
                candidate = sequence.checked_add(1);
            }
            if terminal.is_some() {
                break;
            }
            if mutations.is_empty() {
                break;
            }

            let publish_sink = sink.clone();
            let published =
                match tokio::task::spawn_blocking(move || publish_sink.publish_batch(mutations))
                    .await
                {
                    Ok(Ok(records)) => records,
                    Ok(Err(error)) => {
                        drop(ownership);
                        terminal = Some(format!("{error:#}"));
                        break;
                    }
                    Err(error) => {
                        drop(ownership);
                        terminal = Some(format!("local journal publisher panicked: {error}"));
                        break;
                    }
                };
            let expected_sequences = ownership
                .iter()
                .map(|(sequence, _, _)| *sequence)
                .collect::<Vec<_>>();
            let published_sequences = published
                .iter()
                .map(|record| record.sequence)
                .collect::<Vec<_>>();
            if published_sequences != expected_sequences {
                drop(ownership);
                terminal = Some(format!(
                    "local journal publisher returned sequences {published_sequences:?}, expected {expected_sequences:?}"
                ));
                break;
            }
            let durable_batch_tail = *expected_sequences
                .last()
                .expect("a published batch contains at least one record");

            for (_, _, disk) in &mut ownership {
                if let Some(disk) = disk.take() {
                    disk.accept();
                }
            }

            let mut ownership = ownership.into_iter();
            while let Some((sequence, ram, _)) = ownership.next() {
                if let Some(observer) = &observer
                    && let Err(error) = observer.committed(sequence).await
                {
                    let mut retained = retained_ram
                        .lock()
                        .unwrap_or_else(|error| error.into_inner());
                    retained.extend(ram);
                    retained.extend(ownership.filter_map(|(_, ram, _)| ram));
                    terminal = Some(format!("local commit observer failed: {error:#}"));
                    break;
                }
                drop(ram);
            }
            if terminal.is_some() {
                break;
            }
            progress.send_modify(|state| state.local_seq = durable_batch_tail);
            next_admitted = match durable_batch_tail.checked_add(1) {
                Some(next) => next,
                None => {
                    terminal = Some("local journal sequence overflow".to_owned());
                    break;
                }
            };
        }

        if terminal.is_some() {
            break;
        }
        if input_closed && preparations.is_empty() && prepared.is_empty() {
            break;
        }

        tokio::select! {
            result = preparations.next(), if !preparations.is_empty() => {
                match result {
                    Some(Ok((sequence, result, ram, disk))) => {
                        prepared.insert(sequence, (result, ram, disk));
                    }
                    Some(Err(error)) => {
                        terminal = Some(format!("local journal preparer panicked: {error}"));
                    }
                    None => {}
                }
            }
            command = receiver.recv(), if !input_closed && preparations.len() < prepare_concurrency => {
                match command {
                    Some(JournalCommand::Mutation { record, payload, ram, disk }) => {
                        let record = *record;
                        let sequence = record.sequence;
                        if next_received != Some(sequence) {
                            terminal = Some(format!(
                                "journal worker expected sequence {}, got {sequence}",
                                next_received.map_or_else(|| "after overflow".to_owned(), |value| value.to_string())
                            ));
                            drop(ram);
                            drop(disk);
                            continue;
                        }
                        next_received = sequence.checked_add(1);
                        let prepare_sink = sink.clone();
                        preparations.push(tokio::task::spawn_blocking(move || {
                            let result = prepare_sink.prepare(record, payload.as_ref());
                            (sequence, result, ram, disk)
                        }));
                    }
                    Some(JournalCommand::Shutdown(done)) => {
                        shutdown = Some(done);
                        input_closed = true;
                    }
                    None => input_closed = true,
                }
            }
        }
    }

    if let Some(error) = terminal {
        admission.poison(error.clone());
        progress.send_modify(|state| state.terminal_error = Some(error.clone()));
        while let Some(result) = preparations.next().await {
            if let Ok((_, Ok(mutation), _, _)) = result {
                let _ = sink.discard(mutation);
            }
        }
        for (_, (result, _, _)) in prepared {
            if let Ok(mutation) = result {
                let _ = sink.discard(mutation);
            }
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
    current_terminal(barrier).unwrap_or(LocalBarrierError::Closed)
}

fn current_terminal(barrier: &LocalBarrier) -> Option<LocalBarrierError> {
    barrier
        .progress
        .borrow()
        .terminal_error
        .clone()
        .map(LocalBarrierError::LocalDurability)
}

#[cfg(test)]
mod tests {
    use super::{
        DEFAULT_LOCAL_PREPARE_CONCURRENCY, LocalBarrierError, LocalCommitObserver,
        LocalJournalSink, LocalJournaler,
    };
    use crate::fault_store::FaultStore;
    use crate::writeback::admission::{Admission, AdmissionError, DiskAdmission};
    use crate::writeback::journal::Journal;
    use crate::writeback::model::{
        FenceClass, JournalIdentity, LocalEtag, MutationKind, MutationMode, MutationRecord,
    };
    use crate::writeback::overlay::{OverlayCommitObserver, OverlayIndex};
    use crate::writeback::payload::VerifiedPayload;
    use crate::writeback::remote::RemoteScheduler;
    use anyhow::{Result, bail};
    use bytes::Bytes;
    use object_store::ObjectStoreExt;
    use object_store::memory::InMemory;
    use object_store::path::Path;
    use sha2::{Digest, Sha256};
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::{Arc, Mutex, mpsc};
    use std::time::Duration;
    use tokio::sync::Notify;
    use tokio::sync::mpsc as tokio_mpsc;
    use uuid::Uuid;

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

    struct HeadBlockingJournalSink {
        journal: Arc<Journal>,
        head_release: Mutex<Option<mpsc::Receiver<()>>>,
        prepared: tokio_mpsc::UnboundedSender<u64>,
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
        async fn committed(&self, _sequence: u64) -> Result<()> {
            self.entered.notify_one();
            self.release.notified().await;
            Ok(())
        }
    }

    #[async_trait::async_trait]
    impl LocalCommitObserver for FailingSecondObserver {
        async fn committed(&self, sequence: u64) -> Result<()> {
            if sequence == 2 {
                bail!("injected overlay handoff failure")
            }
            Ok(())
        }
    }

    #[async_trait::async_trait]
    impl LocalCommitObserver for BlockingSecondOverlayObserver {
        async fn committed(&self, sequence: u64) -> Result<()> {
            if sequence == 2 {
                self.entered.notify_one();
                self.release.notified().await;
            }
            self.inner.committed(sequence).await
        }
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

        fn discard(&self, _prepared: crate::writeback::journal::PreparedMutation) -> Result<()> {
            Ok(())
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

        fn discard(&self, prepared: crate::writeback::journal::PreparedMutation) -> Result<()> {
            self.discarded.lock().unwrap().push(prepared.sequence());
            Ok(())
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

        fn discard(&self, _prepared: crate::writeback::journal::PreparedMutation) -> Result<()> {
            Ok(())
        }
    }

    impl LocalJournalSink for HeadBlockingBatchSink {
        fn prepare(
            &self,
            record: MutationRecord,
            _payload: Option<&VerifiedPayload>,
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
            Ok(crate::writeback::journal::PreparedMutation::metadata(
                record,
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

        fn discard(&self, _prepared: crate::writeback::journal::PreparedMutation) -> Result<()> {
            Ok(())
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

        fn discard(&self, prepared: crate::writeback::journal::PreparedMutation) -> Result<()> {
            self.journal.discard_prepared(prepared)
        }
    }

    fn put_record(sequence: u64, payload: &[u8]) -> MutationRecord {
        MutationRecord {
            format_version: 1,
            sequence,
            operation_id: Uuid::from_u128(0x3000 + sequence as u128),
            path: format!("segments/{sequence}"),
            kind: MutationKind::Put {
                mode: MutationMode::Create,
                expected_visible_version: None,
                payload_len: payload.len() as u64,
                payload_sha256: Sha256::digest(payload).into(),
                blob_path: String::new(),
            },
            local_etag: LocalEtag::new(Uuid::nil(), sequence),
            accepted_at_unix_ms: sequence,
            remote_predecessor_etag: None,
            remote_result_etag: None,
            fence: FenceClass::ImmutableCreate,
            retry_count: 0,
            last_error: None,
        }
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
    async fn default_local_prepare_concurrency_is_bounded_at_four() {
        let admission = Admission::new(64);
        let (journaler, mut entered, release, sink) = blocking_journaler(admission.clone(), None);
        for sequence in 1..=6 {
            let ram = admission.reserve(1).await.unwrap().accept();
            journaler
                .submit_put(put_record(sequence, b"x"), Bytes::from_static(b"x"), ram)
                .await
                .unwrap();
        }

        let mut started = Vec::new();
        for _ in 0..4 {
            started.push(
                tokio::time::timeout(Duration::from_millis(250), entered.recv())
                    .await
                    .expect("four local preparations should start")
                    .unwrap(),
            );
        }
        started.sort_unstable();
        assert_eq!(started, vec![1, 2, 3, 4]);
        assert!(
            tokio::time::timeout(Duration::from_millis(25), entered.recv())
                .await
                .is_err(),
            "the fifth preparation exceeded the default concurrency cap"
        );

        for _ in 0..6 {
            release.send(()).unwrap();
        }
        journaler.barrier().wait_local(6).await.unwrap();
        assert_eq!(sink.prepared_operations.load(Ordering::Relaxed), 6);
        assert_eq!(sink.prepared_payload_bytes.load(Ordering::Relaxed), 6);
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
        for sequence in 1..=2 {
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
        let journaler = LocalJournaler::start_with_sink(sink, admission.clone(), 0, 8);
        for sequence in 1..=2 {
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
        prepare_release_senders[&1].send(()).unwrap();
        assert_eq!(prepared_rx.recv().await.unwrap(), 1);

        assert_eq!(publish_entered_rx.recv().await.unwrap(), 1);
        assert_eq!(admission.used_bytes(), 2);
        publish_release_tx.send(()).unwrap();
        assert_eq!(publish_entered_rx.recv().await.unwrap(), 2);
        assert_eq!(
            admission.used_bytes(),
            2,
            "no admission may be released before the durable batch returns"
        );
        publish_release_tx.send(()).unwrap();

        journaler.barrier().wait_local(2).await.unwrap();
        assert_eq!(admission.used_bytes(), 0);
        journaler.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn stalled_head_with_many_ready_records_publishes_bounded_batches_without_stalling() {
        let admission = Admission::new(130);
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
            256,
            256,
            None,
        );
        for sequence in 1..=130 {
            let ram = admission.reserve(1).await.unwrap().accept();
            journaler
                .submit_put(put_record(sequence, b"x"), Bytes::from_static(b"x"), ram)
                .await
                .unwrap();
        }
        for _ in 2..=130 {
            tokio::time::timeout(Duration::from_secs(2), prepared_rx.recv())
                .await
                .expect("later preparation stalled behind the blocked head")
                .unwrap();
        }
        assert!(sink.published_batches.lock().unwrap().is_empty());

        head_release_tx.send(()).unwrap();
        journaler.barrier().wait_local(130).await.unwrap();

        assert_eq!(
            sink.published_batches
                .lock()
                .unwrap()
                .iter()
                .map(Vec::len)
                .collect::<Vec<_>>(),
            vec![64, 64, 2]
        );
        assert_eq!(admission.used_bytes(), 0);
        journaler.shutdown().await.unwrap();
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
        let disk = DiskAdmission::new(100, 95, 85, 10).unwrap();
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
            8,
            8,
            Some(Arc::new(FailingSecondObserver)),
        );
        for sequence in 1..=2 {
            let ram = admission.reserve(1).await.unwrap().accept();
            let disk_permit = disk.reserve(1, 1_000).await.unwrap();
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
            1,
            "only the RAM for the unobserved durable tail remains owned"
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
        let disk = DiskAdmission::new(1_000_000, 95, 85, 1).unwrap();
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
            8,
            8,
            Some(observer.clone()),
        );
        let remote_scheduler = RemoteScheduler::start(
            remote,
            journal.clone(),
            overlay.clone(),
            admission.clone(),
            disk.clone(),
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
            let disk_permit = disk
                .reserve(record.ssd_reservation_bytes().unwrap(), 1_000_000)
                .await
                .unwrap();
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
        assert!(sink.published.lock().unwrap().is_empty());
        assert_eq!(*sink.discarded.lock().unwrap(), vec![2]);
        assert_eq!(admission.used_bytes(), 0);
        assert!(matches!(
            journaler.shutdown().await,
            Err(LocalBarrierError::LocalDurability(_))
        ));
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
        let disk = DiskAdmission::new(100, 95, 85, 10).unwrap();
        let (journaler, mut entered, release, _) = blocking_journaler(admission.clone(), None);
        let ram = admission.reserve(7).await.unwrap().accept();
        let disk_permit = disk.reserve(7, 1_000).await.unwrap();
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

        disk.set_remote_complete(7, 1_000).unwrap();
        assert_eq!(disk.used_bytes(), 0);
        journaler.shutdown().await.unwrap();
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
        let journaler =
            LocalJournaler::start_with_sink_and_observer(sink, admission.clone(), 0, 1, 1, None);
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
}
