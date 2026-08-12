use crate::writeback::admission::{AcceptedAdmission, Admission, DiskPermit};
use crate::writeback::journal::{Journal, PreparedMutation};
use crate::writeback::model::{MutationRecord, Sequence};
use crate::writeback::payload::VerifiedPayload;
use anyhow::Result as AnyResult;
use bytes::Bytes;
use futures::{StreamExt, stream::FuturesUnordered};
use std::collections::BTreeMap;
use std::sync::Arc;
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

    fn discard(&self, prepared: PreparedMutation) -> AnyResult<()> {
        self.discard_prepared(prepared)
    }
}

const DEFAULT_LOCAL_PREPARE_CONCURRENCY: usize = 4;

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
        let join = tokio::spawn(run_journaler(
            sink,
            admission,
            receiver,
            progress_sender,
            local_sequence,
            observer,
            prepare_concurrency,
        ));
        Self {
            inner: Arc::new(LocalJournalerInner {
                sender,
                barrier: LocalBarrier { progress },
                admission_gate: Mutex::new(()),
                closed: AtomicBool::new(false),
                join: Mutex::new(Some(join)),
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

    pub async fn shutdown(&self) -> Result<(), LocalBarrierError> {
        let receiver = {
            let _gate = self.inner.admission_gate.lock().await;
            if self.inner.closed.swap(true, Ordering::AcqRel) {
                None
            } else {
                let (sender, receiver) = oneshot::channel();
                self.inner
                    .sender
                    .send(JournalCommand::Shutdown(sender))
                    .await
                    .ok();
                Some(receiver)
            }
        };
        if let Some(receiver) = receiver {
            let _ = receiver.await;
        }
        if let Some(join) = self.inner.join.lock().await.take() {
            join.await.map_err(|error| {
                LocalBarrierError::LocalDurability(format!("journal worker panicked: {error}"))
            })?;
        }
        Ok(())
    }
}

async fn run_journaler(
    sink: Arc<dyn LocalJournalSink>,
    admission: Admission,
    mut receiver: mpsc::Receiver<JournalCommand>,
    progress: watch::Sender<LocalProgress>,
    local_sequence: Sequence,
    observer: Option<Arc<dyn LocalCommitObserver>>,
    prepare_concurrency: usize,
) {
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
            let Some((result, ram, disk)) = prepared.remove(&next_admitted) else {
                break;
            };
            let mutation = match result {
                Ok(mutation) => mutation,
                Err(error) => {
                    drop(ram);
                    drop(disk);
                    terminal = Some(format!("{error:#}"));
                    break;
                }
            };
            let sequence = mutation.sequence();
            let publish_sink = sink.clone();
            match tokio::task::spawn_blocking(move || publish_sink.publish(mutation)).await {
                Ok(Ok(_)) => {}
                Ok(Err(error)) => {
                    drop(ram);
                    drop(disk);
                    terminal = Some(format!("{error:#}"));
                    break;
                }
                Err(error) => {
                    drop(ram);
                    drop(disk);
                    terminal = Some(format!("local journal publisher panicked: {error}"));
                    break;
                }
            }
            if let Some(observer) = &observer
                && let Err(error) = observer.committed(sequence).await
            {
                drop(ram);
                drop(disk);
                terminal = Some(format!("local commit observer failed: {error:#}"));
                break;
            }
            if let Some(disk) = disk {
                disk.accept();
            }
            drop(ram);
            progress.send_modify(|state| state.local_seq = sequence);
            next_admitted = match next_admitted.checked_add(1) {
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
        admission.poison(error.clone());
        progress.send_modify(|state| state.terminal_error = Some(error));
    } else {
        admission.close();
    }
    if let Some(done) = shutdown {
        let _ = done.send(());
    }
    progress.send_modify(|state| state.closed = true);
}

fn terminal_or_closed(barrier: &LocalBarrier) -> LocalBarrierError {
    barrier
        .progress
        .borrow()
        .terminal_error
        .clone()
        .map(LocalBarrierError::LocalDurability)
        .unwrap_or(LocalBarrierError::Closed)
}

#[cfg(test)]
mod tests {
    use super::{
        DEFAULT_LOCAL_PREPARE_CONCURRENCY, LocalBarrierError, LocalCommitObserver,
        LocalJournalSink, LocalJournaler,
    };
    use crate::writeback::admission::{Admission, AdmissionError, DiskAdmission};
    use crate::writeback::journal::Journal;
    use crate::writeback::model::{
        FenceClass, JournalIdentity, LocalEtag, MutationKind, MutationMode, MutationRecord,
    };
    use crate::writeback::payload::VerifiedPayload;
    use anyhow::{Result, bail};
    use bytes::Bytes;
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
        discarded: Mutex<Vec<u64>>,
    }

    #[derive(Default)]
    struct BlockingObserver {
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

        fn discard(&self, prepared: crate::writeback::journal::PreparedMutation) -> Result<()> {
            self.discarded.lock().unwrap().push(prepared.sequence());
            Ok(())
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
        journaler.shutdown().await.unwrap();
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
        journaler.shutdown().await.unwrap();
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
}
