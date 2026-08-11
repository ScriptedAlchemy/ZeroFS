use crate::writeback::admission::{AcceptedAdmission, Admission, DiskPermit};
use crate::writeback::journal::Journal;
use crate::writeback::model::{MutationRecord, Sequence};
use crate::writeback::payload::VerifiedPayload;
use anyhow::Result as AnyResult;
use bytes::Bytes;
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
    fn commit(
        &self,
        record: MutationRecord,
        payload: Option<&VerifiedPayload>,
    ) -> AnyResult<MutationRecord>;
}

impl LocalJournalSink for Journal {
    fn commit(
        &self,
        record: MutationRecord,
        payload: Option<&VerifiedPayload>,
    ) -> AnyResult<MutationRecord> {
        match payload {
            Some(payload) => self.commit_verified_put(record, payload),
            None => self.commit_metadata(record),
        }
    }
}

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
        Self::start_with_observer(journal, admission, queue_depth, None)
    }

    pub fn start_with_observer(
        journal: Arc<Journal>,
        admission: Admission,
        queue_depth: usize,
        observer: Option<Arc<dyn LocalCommitObserver>>,
    ) -> AnyResult<Self> {
        let local_sequence = journal.snapshot()?.local_seq;
        Ok(Self::start_with_sink_and_observer(
            journal,
            admission,
            local_sequence,
            queue_depth,
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
        Self::start_with_sink_and_observer(sink, admission, local_sequence, queue_depth, None)
    }

    fn start_with_sink_and_observer(
        sink: Arc<dyn LocalJournalSink>,
        admission: Admission,
        local_sequence: Sequence,
        queue_depth: usize,
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

    pub async fn submit_metadata(
        &self,
        record: MutationRecord,
    ) -> Result<LocalBarrier, LocalBarrierError> {
        self.submit(record, None, None, None).await
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
    mut local_sequence: Sequence,
    observer: Option<Arc<dyn LocalCommitObserver>>,
) {
    while let Some(command) = receiver.recv().await {
        match command {
            JournalCommand::Mutation {
                record,
                payload,
                ram,
                disk,
            } => {
                let record = *record;
                let Some(expected) = local_sequence.checked_add(1) else {
                    let error = "local journal sequence overflow".to_owned();
                    admission.poison(error.clone());
                    progress.send_modify(|state| state.terminal_error = Some(error));
                    break;
                };
                if record.sequence != expected {
                    let error = format!(
                        "journal worker expected sequence {expected}, got {}",
                        record.sequence
                    );
                    admission.poison(error.clone());
                    progress.send_modify(|state| state.terminal_error = Some(error));
                    break;
                }
                let sink = sink.clone();
                let sequence = record.sequence;
                let result = tokio::task::spawn_blocking(move || {
                    let result = sink.commit(record, payload.as_ref());
                    (result, ram, disk)
                })
                .await;
                match result {
                    Ok((Ok(_), ram, disk)) => {
                        if let Some(observer) = &observer
                            && let Err(error) = observer.committed(sequence).await
                        {
                            let error = format!("local commit observer failed: {error:#}");
                            admission.poison(error.clone());
                            progress.send_modify(|state| state.terminal_error = Some(error));
                            drop(ram);
                            drop(disk);
                            break;
                        }
                        if let Some(disk) = disk {
                            disk.accept();
                        }
                        drop(ram);
                        local_sequence = sequence;
                        progress.send_modify(|state| state.local_seq = sequence);
                    }
                    Ok((Err(error), ram, disk)) => {
                        drop(ram);
                        drop(disk);
                        let error = format!("{error:#}");
                        admission.poison(error.clone());
                        progress.send_modify(|state| state.terminal_error = Some(error));
                        break;
                    }
                    Err(error) => {
                        let error = format!("local journal worker panicked: {error}");
                        admission.poison(error.clone());
                        progress.send_modify(|state| state.terminal_error = Some(error));
                        break;
                    }
                }
            }
            JournalCommand::Shutdown(done) => {
                admission.close();
                progress.send_modify(|state| state.closed = true);
                let _ = done.send(());
                break;
            }
        }
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
    use super::{LocalBarrierError, LocalCommitObserver, LocalJournalSink, LocalJournaler};
    use crate::writeback::admission::{Admission, AdmissionError, DiskAdmission};
    use crate::writeback::journal::Journal;
    use crate::writeback::model::{
        FenceClass, JournalIdentity, LocalEtag, MutationKind, MutationMode, MutationRecord,
    };
    use crate::writeback::payload::VerifiedPayload;
    use anyhow::{Result, bail};
    use bytes::Bytes;
    use sha2::{Digest, Sha256};
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
        fn commit(
            &self,
            record: MutationRecord,
            _payload: Option<&VerifiedPayload>,
        ) -> Result<MutationRecord> {
            self.entered.send(record.sequence).unwrap();
            self.release.lock().unwrap().recv().unwrap();
            if self.fail_sequence == Some(record.sequence) {
                bail!("injected local fsync failure");
            }
            self.committed.lock().unwrap().push(record.sequence);
            Ok(record)
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
        });
        let journaler = LocalJournaler::start_with_sink(sink.clone(), admission, 0, 8);
        (journaler, entered_rx, release_tx, sink)
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
        });
        let observer = Arc::new(BlockingObserver::default());
        let journaler = LocalJournaler::start_with_sink_and_observer(
            sink,
            admission.clone(),
            0,
            8,
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

        assert_eq!(entered.recv().await.unwrap(), 1);
        release.send(()).unwrap();
        assert_eq!(entered.recv().await.unwrap(), 2);
        assert!(
            tokio::time::timeout(Duration::from_millis(25), barrier.wait_local(2))
                .await
                .is_err()
        );
        release.send(()).unwrap();
        barrier.wait_local(2).await.unwrap();
        assert_eq!(*sink.committed.lock().unwrap(), vec![1, 2]);
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
