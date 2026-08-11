use crate::writeback::admission::DiskAdmission;
use crate::writeback::journal::Journal;
use crate::writeback::journaler::{LocalBarrier, LocalBarrierError};
use crate::writeback::model::{MutationKind, MutationMode, MutationRecord, Sequence};
use crate::writeback::overlay::OverlayIndex;
use bytes::Bytes;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt, PutMode, PutOptions, PutResult, UpdateVersion};
use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Mutex, watch};
use tokio::task::JoinHandle;

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum RemoteBarrierError {
    #[error("remote writeback scheduler is closed")]
    Closed,
    #[error("remote writeback failed: {0}")]
    Remote(String),
}

#[derive(Debug, Clone)]
struct RemoteProgress {
    sequence: Sequence,
    terminal_error: Option<String>,
    closed: bool,
}

#[derive(Clone)]
pub struct RemoteBarrier {
    progress: watch::Receiver<RemoteProgress>,
}

impl RemoteBarrier {
    pub async fn wait_remote(&self, sequence: Sequence) -> Result<(), RemoteBarrierError> {
        let mut progress = self.progress.clone();
        loop {
            let state = progress.borrow().clone();
            if state.sequence >= sequence {
                return Ok(());
            }
            if let Some(error) = state.terminal_error {
                return Err(RemoteBarrierError::Remote(error));
            }
            if state.closed {
                return Err(RemoteBarrierError::Closed);
            }
            progress
                .changed()
                .await
                .map_err(|_| RemoteBarrierError::Closed)?;
        }
    }
}

#[derive(Clone)]
pub struct RemoteScheduler {
    barrier: RemoteBarrier,
    stop: watch::Sender<bool>,
    join: Arc<Mutex<Option<JoinHandle<()>>>>,
}

impl std::fmt::Debug for RemoteScheduler {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("RemoteScheduler")
            .finish_non_exhaustive()
    }
}

impl RemoteScheduler {
    pub fn start(
        remote: Arc<dyn ObjectStore>,
        journal: Arc<Journal>,
        overlay: OverlayIndex,
        disk: DiskAdmission,
        local: LocalBarrier,
        upload_concurrency: usize,
    ) -> anyhow::Result<Self> {
        let snapshot = journal.snapshot()?;
        let (progress_sender, progress) = watch::channel(RemoteProgress {
            sequence: snapshot.remote_seq,
            terminal_error: None,
            closed: false,
        });
        let (stop, stop_receiver) = watch::channel(false);
        let join = tokio::spawn(run_remote_scheduler(RemoteWorker {
            remote,
            journal,
            overlay,
            disk,
            local,
            upload_concurrency: upload_concurrency.max(1),
            progress: progress_sender,
            stop: stop_receiver,
        }));
        Ok(Self {
            barrier: RemoteBarrier { progress },
            stop,
            join: Arc::new(Mutex::new(Some(join))),
        })
    }

    pub fn barrier(&self) -> RemoteBarrier {
        self.barrier.clone()
    }

    pub async fn shutdown(&self) -> Result<(), RemoteBarrierError> {
        let _ = self.stop.send(true);
        if let Some(join) = self.join.lock().await.take() {
            join.await
                .map_err(|error| RemoteBarrierError::Remote(format!("worker panicked: {error}")))?;
        }
        Ok(())
    }
}

struct RemoteWorker {
    remote: Arc<dyn ObjectStore>,
    journal: Arc<Journal>,
    overlay: OverlayIndex,
    disk: DiskAdmission,
    local: LocalBarrier,
    upload_concurrency: usize,
    progress: watch::Sender<RemoteProgress>,
    stop: watch::Receiver<bool>,
}

async fn run_remote_scheduler(worker: RemoteWorker) {
    let RemoteWorker {
        remote,
        journal,
        overlay,
        disk,
        local,
        upload_concurrency,
        progress,
        mut stop,
    } = worker;
    let mut next = progress.borrow().sequence.saturating_add(1);
    loop {
        if *stop.borrow() {
            break;
        }
        let local_wait = local.wait_local(next);
        tokio::select! {
            result = local_wait => {
                if let Err(error) = result {
                    if !matches!(error, LocalBarrierError::Closed) {
                        progress.send_modify(|state| state.terminal_error = Some(error.to_string()));
                    }
                    break;
                }
            }
            changed = stop.changed() => {
                if changed.is_err() || *stop.borrow() {
                    break;
                }
                continue;
            }
        }
        let snapshot =
            match coalesce_local_batch(&journal, next, upload_concurrency, &mut stop).await {
                Ok(Some(snapshot)) => snapshot,
                Ok(None) => break,
                Err(error) => {
                    progress.send_modify(|state| state.terminal_error = Some(format!("{error:#}")));
                    break;
                }
            };
        let batch = collect_disjoint_batch(&snapshot.records, next, upload_concurrency);
        if batch.is_empty() {
            progress.send_modify(|state| {
                state.terminal_error = Some(format!("local mutation {next} is missing"));
            });
            break;
        }
        let outcomes = futures::future::join_all(batch.iter().cloned().map(|record| {
            let remote = remote.clone();
            let journal = journal.clone();
            async move {
                let result = apply_record(remote, journal, record.clone()).await;
                (record, result)
            }
        }));
        let outcomes = tokio::select! {
            outcomes = outcomes => outcomes,
            changed = stop.changed() => {
                if changed.is_err() || *stop.borrow() {
                    break;
                }
                continue;
            }
        };
        let mut retry = false;
        for (record, result) in outcomes {
            let result = match result {
                Ok(result) => result,
                Err(_error) => {
                    retry = true;
                    break;
                }
            };
            if let Err(error) =
                commit_remote(&journal, &overlay, &disk, &record, result.e_tag).await
            {
                progress.send_modify(|state| state.terminal_error = Some(format!("{error:#}")));
                progress.send_modify(|state| state.closed = true);
                return;
            }
            progress.send_modify(|state| state.sequence = record.sequence);
            let Some(incremented) = record.sequence.checked_add(1) else {
                progress.send_modify(|state| {
                    state.terminal_error = Some("remote sequence overflow".to_owned());
                    state.closed = true;
                });
                return;
            };
            next = incremented;
        }
        if retry {
            tokio::select! {
                _ = tokio::time::sleep(Duration::from_millis(25)) => {}
                changed = stop.changed() => {
                    if changed.is_err() || *stop.borrow() {
                        break;
                    }
                }
            }
        }
    }
    progress.send_modify(|state| state.closed = true);
}

async fn coalesce_local_batch(
    journal: &Journal,
    next: Sequence,
    upload_concurrency: usize,
    stop: &mut watch::Receiver<bool>,
) -> anyhow::Result<Option<crate::writeback::journal::JournalSnapshot>> {
    let target = next
        .saturating_add(u64::try_from(upload_concurrency.saturating_sub(1)).unwrap_or(u64::MAX));
    let mut snapshot = journal.snapshot()?;
    let mut observed_local = snapshot.local_seq;
    let mut idle_deadline = tokio::time::Instant::now() + Duration::from_millis(500);
    loop {
        if snapshot.local_seq >= target || tokio::time::Instant::now() >= idle_deadline {
            return Ok(Some(snapshot));
        }
        tokio::select! {
            _ = tokio::time::sleep(Duration::from_millis(10)) => {}
            changed = stop.changed() => {
                if changed.is_err() || *stop.borrow() {
                    return Ok(None);
                }
            }
        }
        let next_snapshot = journal.snapshot()?;
        if next_snapshot.local_seq > observed_local {
            observed_local = next_snapshot.local_seq;
            idle_deadline = tokio::time::Instant::now() + Duration::from_millis(500);
        }
        snapshot = next_snapshot;
    }
}

fn collect_disjoint_batch(
    records: &[MutationRecord],
    first_sequence: Sequence,
    limit: usize,
) -> Vec<MutationRecord> {
    let mut batch = Vec::new();
    let mut touched = BTreeSet::new();
    let mut expected = first_sequence;
    for record in records
        .iter()
        .filter(|record| record.sequence >= first_sequence)
    {
        if record.sequence != expected || batch.len() == limit {
            break;
        }
        let keys = touched_keys(record);
        if keys.iter().any(|key| touched.contains(key)) {
            break;
        }
        touched.extend(keys);
        batch.push(record.clone());
        let Some(incremented) = expected.checked_add(1) else {
            break;
        };
        expected = incremented;
    }
    batch
}

fn touched_keys(record: &MutationRecord) -> Vec<String> {
    let mut keys = vec![record.path.clone()];
    if let MutationKind::Rename { source, .. } = &record.kind {
        keys.push(source.clone());
    }
    keys
}

async fn apply_record(
    remote: Arc<dyn ObjectStore>,
    journal: Arc<Journal>,
    record: MutationRecord,
) -> object_store::Result<PutResult> {
    let target = Path::parse(&record.path).map_err(|error| generic_error(error.to_string()))?;
    match &record.kind {
        MutationKind::Delete => {
            match remote.delete(&target).await {
                Ok(()) | Err(object_store::Error::NotFound { .. }) => {}
                Err(error) => return Err(error),
            }
            Ok(empty_result())
        }
        MutationKind::Put { mode, .. }
        | MutationKind::Copy { mode, .. }
        | MutationKind::Rename { mode, .. } => {
            let sequence = record.sequence;
            let bytes = tokio::task::spawn_blocking(move || journal.read_blob(sequence))
                .await
                .map_err(|error| generic_error(format!("journal read task failed: {error}")))?
                .map(Bytes::from)
                .map_err(|error| generic_error(format!("journal read failed: {error:#}")))?;
            let options = PutOptions::from(remote_put_mode(*mode, &record));
            let result = match remote
                .put_opts(&target, bytes.clone().into(), options)
                .await
            {
                Ok(result) => result,
                Err(object_store::Error::AlreadyExists { .. }) if *mode == MutationMode::Create => {
                    verify_existing(remote.as_ref(), &target, &bytes).await?
                }
                Err(object_store::Error::Precondition { .. }) if *mode == MutationMode::Update => {
                    verify_existing(remote.as_ref(), &target, &bytes).await?
                }
                Err(error) => return Err(error),
            };
            if let MutationKind::Rename { source, .. } = &record.kind {
                let source = Path::parse(source)
                    .map_err(|error| generic_error(format!("invalid rename source: {error}")))?;
                match remote.delete(&source).await {
                    Ok(()) | Err(object_store::Error::NotFound { .. }) => {}
                    Err(error) => return Err(error),
                }
            }
            Ok(result)
        }
    }
}

fn remote_put_mode(mode: MutationMode, record: &MutationRecord) -> PutMode {
    match mode {
        MutationMode::Overwrite => PutMode::Overwrite,
        MutationMode::Create => PutMode::Create,
        MutationMode::Update => match record.remote_predecessor_etag.clone() {
            Some(e_tag) => PutMode::Update(UpdateVersion {
                e_tag: Some(e_tag),
                version: None,
            }),
            None => PutMode::Overwrite,
        },
    }
}

async fn verify_existing(
    remote: &dyn ObjectStore,
    target: &Path,
    expected: &Bytes,
) -> object_store::Result<PutResult> {
    let existing = remote.get(target).await?.bytes().await?;
    if existing != *expected {
        return Err(object_store::Error::AlreadyExists {
            path: target.to_string(),
            source: "remote create target contains different bytes".into(),
        });
    }
    let meta = remote.head(target).await?;
    Ok(PutResult {
        e_tag: meta.e_tag,
        version: meta.version,
        extensions: Default::default(),
    })
}

async fn commit_remote(
    journal: &Journal,
    overlay: &OverlayIndex,
    disk: &DiskAdmission,
    record: &MutationRecord,
    e_tag: Option<String>,
) -> anyhow::Result<()> {
    journal.mark_remote(record.sequence, e_tag)?;
    overlay.remove_remote_prefix(record.sequence).await;
    journal.remove_remote_prefix(record.sequence)?;
    if let Some((payload_len, _)) = record.payload() {
        let available = fs4::available_space(journal.root())?;
        disk.set_remote_complete(payload_len, available)?;
    }
    Ok(())
}

fn empty_result() -> PutResult {
    PutResult {
        e_tag: None,
        version: None,
        extensions: Default::default(),
    }
}

fn generic_error(message: impl Into<String>) -> object_store::Error {
    object_store::Error::Generic {
        store: "ZeroFSWritebackRemote",
        source: message.into().into(),
    }
}
