use crate::writeback::admission::{Admission, DiskAdmission};
use crate::writeback::journal::Journal;
use crate::writeback::journaler::{LocalBarrier, LocalBarrierError};
use crate::writeback::model::{
    FenceClass, LocalEtag, MutationKind, MutationMode, MutationRecord, Sequence,
};
use crate::writeback::overlay::OverlayIndex;
use bytes::Bytes;
use futures::future::BoxFuture;
use futures::stream::FuturesUnordered;
use futures::{FutureExt, StreamExt};
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt, PutMode, PutOptions, PutResult, UpdateVersion};
use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Mutex, watch};
use tokio::task::JoinHandle;

const REMOTE_COALESCE_IDLE: Duration = Duration::from_millis(500);
const REMOTE_RETRY_DELAY: Duration = Duration::from_millis(200);
const REMOTE_OPERATION_TIMEOUT: Duration = Duration::from_secs(120);

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

fn publish_terminal(
    progress: &watch::Sender<RemoteProgress>,
    admission: &Admission,
    disk: &DiskAdmission,
    error: impl Into<String>,
    closed: bool,
) {
    let error = error.into();
    tracing::error!(error = %error, "remote writeback scheduler entered terminal state");
    admission.poison(error.clone());
    disk.poison(error.clone());
    progress.send_modify(|state| {
        state.terminal_error = Some(error.clone());
        state.closed |= closed;
    });
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
    activate: watch::Sender<bool>,
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
        admission: Admission,
        disk: DiskAdmission,
        local: LocalBarrier,
        upload_concurrency: usize,
    ) -> anyhow::Result<Self> {
        Self::start_with_state(
            remote,
            journal,
            overlay,
            (admission, disk),
            local,
            upload_concurrency,
            true,
        )
    }

    pub fn start_paused(
        remote: Arc<dyn ObjectStore>,
        journal: Arc<Journal>,
        overlay: OverlayIndex,
        admission: Admission,
        disk: DiskAdmission,
        local: LocalBarrier,
        upload_concurrency: usize,
    ) -> anyhow::Result<Self> {
        Self::start_with_state(
            remote,
            journal,
            overlay,
            (admission, disk),
            local,
            upload_concurrency,
            false,
        )
    }

    fn start_with_state(
        remote: Arc<dyn ObjectStore>,
        journal: Arc<Journal>,
        overlay: OverlayIndex,
        admissions: (Admission, DiskAdmission),
        local: LocalBarrier,
        upload_concurrency: usize,
        active: bool,
    ) -> anyhow::Result<Self> {
        let (admission, disk) = admissions;
        let journal_progress = journal.progress()?;
        let (progress_sender, progress) = watch::channel(RemoteProgress {
            sequence: journal_progress.remote_seq,
            terminal_error: None,
            closed: false,
        });
        let (activate, activation) = watch::channel(active);
        let (stop, stop_receiver) = watch::channel(false);
        let join = tokio::spawn(run_remote_scheduler(RemoteWorker {
            remote,
            journal,
            overlay,
            admission,
            disk,
            local,
            upload_concurrency: upload_concurrency.max(1),
            progress: progress_sender,
            activation,
            stop: stop_receiver,
        }));
        Ok(Self {
            barrier: RemoteBarrier { progress },
            activate,
            stop,
            join: Arc::new(Mutex::new(Some(join))),
        })
    }

    pub fn barrier(&self) -> RemoteBarrier {
        self.barrier.clone()
    }

    pub fn terminal_error(&self) -> Option<String> {
        self.barrier.progress.borrow().terminal_error.clone()
    }

    pub fn check_available(&self) -> Result<(), RemoteBarrierError> {
        let state = self.barrier.progress.borrow();
        if let Some(error) = &state.terminal_error {
            return Err(RemoteBarrierError::Remote(error.clone()));
        }
        if state.closed {
            return Err(RemoteBarrierError::Closed);
        }
        Ok(())
    }

    pub fn activate(&self) -> Result<(), RemoteBarrierError> {
        self.check_available()?;
        self.activate
            .send(true)
            .map_err(|_| RemoteBarrierError::Closed)
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
    admission: Admission,
    disk: DiskAdmission,
    local: LocalBarrier,
    upload_concurrency: usize,
    progress: watch::Sender<RemoteProgress>,
    activation: watch::Receiver<bool>,
    stop: watch::Receiver<bool>,
}

struct CompletedRemote {
    // The object is durable remotely, but its journal watermark cannot advance
    // until every earlier ordering fence has committed.
    record: MutationRecord,
    e_tag: Option<String>,
}

struct SchedulerWindow {
    local_seq: Sequence,
    records: Vec<MutationRecord>,
}

#[derive(Debug, thiserror::Error)]
#[error("remote target contains different bytes than the locally durable mutation")]
struct RemoteContentDivergence;

#[derive(Debug, thiserror::Error)]
#[error(
    "remote predecessor ETag for local sequence {predecessor_sequence} is unavailable at writeback sequence {sequence}"
)]
struct MissingRemotePredecessor {
    sequence: Sequence,
    predecessor_sequence: Sequence,
}

async fn run_remote_scheduler(worker: RemoteWorker) {
    let RemoteWorker {
        remote,
        journal,
        overlay,
        admission,
        disk,
        local,
        upload_concurrency,
        progress,
        mut activation,
        mut stop,
    } = worker;
    loop {
        if *stop.borrow() {
            progress.send_modify(|state| state.closed = true);
            return;
        }
        if *activation.borrow() {
            break;
        }
        tokio::select! {
            changed = activation.changed() => {
                if changed.is_err() {
                    progress.send_modify(|state| state.closed = true);
                    return;
                }
            }
            changed = stop.changed() => {
                if changed.is_err() || *stop.borrow() {
                    progress.send_modify(|state| state.closed = true);
                    return;
                }
            }
        }
    }
    let mut next = progress.borrow().sequence.saturating_add(1);
    // A new burst gets one coalescing window. Once its local tail is known, do
    // not make each intervening manifest fence pay that delay again.
    let mut known_local_tail = progress.borrow().sequence;
    let mut completed = BTreeMap::<Sequence, CompletedRemote>::new();
    'scheduler: loop {
        if *stop.borrow() {
            break;
        }
        let local_wait = local.wait_local(next);
        tokio::select! {
            result = local_wait => {
                if let Err(error) = result {
                    if !matches!(error, LocalBarrierError::Closed) {
                        publish_terminal(&progress, &admission, &disk, error.to_string(), false);
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
        let window = match coalesce_local_batch(
            &journal,
            next,
            upload_concurrency,
            &completed,
            next <= known_local_tail,
            &mut stop,
        )
        .await
        {
            Ok(Some(window)) => window,
            Ok(None) => break,
            Err(error) => {
                publish_terminal(&progress, &admission, &disk, format!("{error:#}"), false);
                break;
            }
        };
        let mut window = window;
        let mut active = FuturesUnordered::<
            BoxFuture<'static, (MutationRecord, object_store::Result<PutResult>)>,
        >::new();
        let mut active_sequences = BTreeSet::new();
        let mut retry = false;
        loop {
            if !retry {
                let batch = collect_pipeline_batch(
                    &window.records,
                    next,
                    upload_concurrency,
                    &completed,
                    &active_sequences,
                );
                for record in batch {
                    active_sequences.insert(record.sequence);
                    let remote = remote.clone();
                    let journal = journal.clone();
                    active.push(
                        async move {
                            let applying = apply_record(remote, journal, record.clone());
                            let result = bounded_remote_operation(
                                &record,
                                REMOTE_OPERATION_TIMEOUT,
                                applying,
                            )
                            .await;
                            (record, result)
                        }
                        .boxed(),
                    );
                }
            }
            if active.is_empty() {
                break;
            }
            let outcome = tokio::select! {
                outcome = active.next() => outcome.expect("active remote upload set is non-empty"),
                changed = stop.changed() => {
                    if changed.is_err() || *stop.borrow() {
                        break 'scheduler;
                    }
                    continue;
                }
            };
            let (record, result) = outcome;
            active_sequences.remove(&record.sequence);
            let result = match result {
                Ok(result) => result,
                Err(error) => {
                    tracing::warn!(
                        sequence = record.sequence,
                        path = %record.path,
                        error = %error,
                        "remote writeback operation failed; retrying from the durable journal"
                    );
                    let failure_journal = Arc::clone(&journal);
                    let failure_sequence = record.sequence;
                    let failure_error = error.to_string();
                    let persisted_failure = tokio::task::spawn_blocking(move || {
                        failure_journal.record_remote_failure(failure_sequence, &failure_error)
                    })
                    .await;
                    if let Err(journal_error) = persisted_failure
                        .map_err(|error| anyhow::anyhow!("remote retry task failed: {error}"))
                        .and_then(|result| result)
                    {
                        publish_terminal(
                            &progress,
                            &admission,
                            &disk,
                            format!(
                                "failed to persist remote retry for sequence {}: {journal_error:#}",
                                record.sequence
                            ),
                            true,
                        );
                        return;
                    }
                    if is_terminal_remote_error(&error) {
                        publish_terminal(
                            &progress,
                            &admission,
                            &disk,
                            format!(
                                "permanent remote divergence at sequence {}: {error}",
                                record.sequence
                            ),
                            true,
                        );
                        return;
                    }
                    retry = true;
                    continue;
                }
            };
            completed.insert(
                record.sequence,
                CompletedRemote {
                    record,
                    e_tag: result.e_tag,
                },
            );
            if let Err(error) = commit_ready_prefix(
                &journal,
                &overlay,
                &disk,
                &progress,
                &mut next,
                &mut completed,
            )
            .await
            {
                publish_terminal(&progress, &admission, &disk, format!("{error:#}"), true);
                return;
            }
            if !retry {
                window = match load_scheduler_window(&journal, next, upload_concurrency) {
                    Ok(window) => window,
                    Err(error) => {
                        publish_terminal(&progress, &admission, &disk, format!("{error:#}"), true);
                        return;
                    }
                };
                known_local_tail = known_local_tail.max(window.local_seq);
            }
        }
        known_local_tail = known_local_tail.max(window.local_seq);
        if retry {
            tokio::select! {
                _ = tokio::time::sleep(REMOTE_RETRY_DELAY) => {}
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

fn is_terminal_remote_error(error: &object_store::Error) -> bool {
    match error {
        object_store::Error::AlreadyExists { source, .. } => source.is::<RemoteContentDivergence>(),
        object_store::Error::Precondition { source, .. } => source.is::<MissingRemotePredecessor>(),
        _ => false,
    }
}

async fn bounded_remote_operation<F>(
    record: &MutationRecord,
    deadline: Duration,
    operation: F,
) -> object_store::Result<PutResult>
where
    F: Future<Output = object_store::Result<PutResult>>,
{
    tokio::time::timeout(deadline, operation)
        .await
        .map_err(|_| {
            generic_error(format!(
                "remote operation for sequence {} timed out after {:.3}s",
                record.sequence,
                deadline.as_secs_f64()
            ))
        })?
}

async fn coalesce_local_batch(
    journal: &Journal,
    next: Sequence,
    upload_concurrency: usize,
    completed: &BTreeMap<Sequence, CompletedRemote>,
    drain_known_backlog: bool,
    stop: &mut watch::Receiver<bool>,
) -> anyhow::Result<Option<SchedulerWindow>> {
    let mut window = load_scheduler_window(journal, next, upload_concurrency)?;
    if drain_known_backlog {
        return Ok(Some(window));
    }
    let mut observed_local = window.local_seq;
    let mut idle_deadline = tokio::time::Instant::now() + REMOTE_COALESCE_IDLE;
    loop {
        if !completed.is_empty()
            || collect_pipeline_batch(
                &window.records,
                next,
                upload_concurrency,
                completed,
                &BTreeSet::new(),
            )
            .len()
                == upload_concurrency
            || tokio::time::Instant::now() >= idle_deadline
        {
            return Ok(Some(window));
        }
        tokio::select! {
            _ = tokio::time::sleep(Duration::from_millis(10)) => {}
            changed = stop.changed() => {
                if changed.is_err() || *stop.borrow() {
                    return Ok(None);
                }
            }
        }
        let next_window = load_scheduler_window(journal, next, upload_concurrency)?;
        if next_window.local_seq > observed_local {
            observed_local = next_window.local_seq;
            idle_deadline = tokio::time::Instant::now() + REMOTE_COALESCE_IDLE;
        }
        window = next_window;
    }
}

fn load_scheduler_window(
    journal: &Journal,
    first_sequence: Sequence,
    upload_concurrency: usize,
) -> anyhow::Result<SchedulerWindow> {
    let progress = journal.progress()?;
    let scan_limit = upload_concurrency.saturating_mul(8).max(upload_concurrency);
    let window = SchedulerWindow {
        local_seq: progress.local_seq,
        records: journal.pending_from(first_sequence, scan_limit)?,
    };
    validate_scheduler_window(&window, first_sequence)?;
    Ok(window)
}

fn validate_scheduler_window(
    window: &SchedulerWindow,
    first_sequence: Sequence,
) -> anyhow::Result<()> {
    if window.local_seq < first_sequence {
        return Ok(());
    }
    let mut expected = first_sequence;
    for record in &window.records {
        if record.sequence != expected {
            anyhow::bail!(
                "durable writeback journal is missing remote frontier sequence {expected}; found sequence {}",
                record.sequence
            );
        }
        expected = expected
            .checked_add(1)
            .ok_or_else(|| anyhow::anyhow!("remote scheduler sequence overflow"))?;
    }
    if window.records.is_empty() {
        anyhow::bail!(
            "durable writeback journal is missing remote frontier sequence {first_sequence}"
        );
    }
    Ok(())
}

fn collect_pipeline_batch(
    records: &[MutationRecord],
    first_sequence: Sequence,
    limit: usize,
    completed: &BTreeMap<Sequence, CompletedRemote>,
    active: &BTreeSet<Sequence>,
) -> Vec<MutationRecord> {
    let held_limit = limit.saturating_mul(4).max(limit);
    let mut available = limit
        .saturating_sub(active.len())
        .min(held_limit.saturating_sub(completed.len()));
    let frontier_needs_slot = records.first().is_some_and(|record| {
        record.sequence == first_sequence
            && !completed.contains_key(&first_sequence)
            && !active.contains(&first_sequence)
    });
    if available == 0 && frontier_needs_slot && active.len() < limit {
        // Speculative immutable uploads may fill the held-completion budget
        // while an earlier ordering fence is retrying. Always reserve enough
        // execution capacity for that frontier; otherwise the scheduler spins
        // forever with a full completion map and can never advance its journal
        // watermark.
        available = 1;
    }
    if available == 0 {
        return Vec::new();
    }
    let mut batch = Vec::new();
    let mut earlier_keys = BTreeSet::new();
    let mut expected = first_sequence;
    for record in records
        .iter()
        .filter(|record| record.sequence >= first_sequence)
    {
        if record.sequence != expected || batch.len() == available {
            break;
        }
        let keys = touched_keys(record);
        let conflicts_with_earlier = keys.iter().any(|key| earlier_keys.contains(key));
        let is_frontier = record.sequence == first_sequence;
        let may_preupload = record.fence == FenceClass::ImmutableCreate;
        // Only immutable, unreferenced data may cross a fence. Results remain
        // held in memory and are journal-committed strictly in sequence order.
        if !completed.contains_key(&record.sequence)
            && !active.contains(&record.sequence)
            && !conflicts_with_earlier
            && (is_frontier || may_preupload)
        {
            batch.push(record.clone());
        }
        earlier_keys.extend(keys);
        let Some(incremented) = expected.checked_add(1) else {
            break;
        };
        expected = incremented;
    }
    batch
}

async fn commit_ready_prefix(
    journal: &Arc<Journal>,
    overlay: &OverlayIndex,
    disk: &DiskAdmission,
    progress: &watch::Sender<RemoteProgress>,
    next: &mut Sequence,
    completed: &mut BTreeMap<Sequence, CompletedRemote>,
) -> anyhow::Result<()> {
    while let Some(completion) = completed.remove(next) {
        commit_remote(journal, overlay, disk, &completion.record, completion.e_tag).await?;
        progress.send_modify(|state| state.sequence = completion.record.sequence);
        *next = completion
            .record
            .sequence
            .checked_add(1)
            .ok_or_else(|| anyhow::anyhow!("remote sequence overflow"))?;
    }
    Ok(())
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
            let options = PutOptions::from(remote_put_mode(*mode, &record, journal.as_ref())?);
            let sequence = record.sequence;
            let bytes = tokio::task::spawn_blocking(move || journal.read_blob(sequence))
                .await
                .map_err(|error| generic_error(format!("journal read task failed: {error}")))?
                .map(Bytes::from)
                .map_err(|error| generic_error(format!("journal read failed: {error:#}")))?;
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

fn remote_put_mode(
    mode: MutationMode,
    record: &MutationRecord,
    journal: &Journal,
) -> object_store::Result<PutMode> {
    match mode {
        MutationMode::Overwrite => Ok(PutMode::Overwrite),
        MutationMode::Create => Ok(PutMode::Create),
        MutationMode::Update => {
            let e_tag = if let Some(e_tag) = record.remote_predecessor_etag.clone() {
                e_tag
            } else {
                let expected = expected_visible_version(record).ok_or_else(|| {
                    precondition(&record.path, "update has no visible predecessor")
                })?;
                let predecessor_sequence =
                    LocalEtag::sequence_from_str(expected).ok_or_else(|| {
                        precondition(
                            &record.path,
                            "update has no durable remote predecessor ETag",
                        )
                    })?;
                journal
                    .remote_object_etag(&record.path, predecessor_sequence)
                    .map_err(|error| {
                        generic_error(format!(
                            "failed to resolve remote predecessor for sequence {}: {error:#}",
                            record.sequence
                        ))
                    })?
                    .ok_or_else(|| missing_remote_predecessor(record, predecessor_sequence))?
            };
            Ok(PutMode::Update(UpdateVersion {
                e_tag: Some(e_tag),
                version: None,
            }))
        }
    }
}

fn expected_visible_version(record: &MutationRecord) -> Option<&str> {
    match &record.kind {
        MutationKind::Put {
            expected_visible_version,
            ..
        } => expected_visible_version.as_deref(),
        _ => None,
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
            source: Box::new(RemoteContentDivergence),
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
    journal: &Arc<Journal>,
    overlay: &OverlayIndex,
    disk: &DiskAdmission,
    record: &MutationRecord,
    e_tag: Option<String>,
) -> anyhow::Result<()> {
    let sequence = record.sequence;
    let mark_journal = Arc::clone(journal);
    tokio::task::spawn_blocking(move || mark_journal.mark_remote(sequence, e_tag))
        .await
        .map_err(|error| anyhow::anyhow!("remote watermark task failed: {error}"))??;
    overlay.remove_remote_prefix(record.sequence).await;
    let charge = record.disk_charge_bytes()?;
    let cleanup_journal = Arc::clone(journal);
    let available = tokio::task::spawn_blocking(move || -> anyhow::Result<u64> {
        cleanup_journal.remove_remote_prefix(sequence)?;
        Ok(fs4::available_space(cleanup_journal.root())?)
    })
    .await
    .map_err(|error| anyhow::anyhow!("remote cleanup task failed: {error}"))??;
    disk.set_remote_complete(charge, available)?;
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

fn precondition(path: &str, message: &'static str) -> object_store::Error {
    object_store::Error::Precondition {
        path: path.to_owned(),
        source: message.into(),
    }
}

fn missing_remote_predecessor(
    record: &MutationRecord,
    predecessor_sequence: Sequence,
) -> object_store::Error {
    object_store::Error::Precondition {
        path: record.path.clone(),
        source: Box::new(MissingRemotePredecessor {
            sequence: record.sequence,
            predecessor_sequence,
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::{
        CompletedRemote, SchedulerWindow, bounded_remote_operation, collect_pipeline_batch,
        validate_scheduler_window,
    };
    use crate::writeback::model::{FenceClass, LocalEtag, MutationKind, MutationRecord};
    use futures::future;
    use object_store::PutResult;
    use std::time::Duration;
    use uuid::Uuid;

    fn record(sequence: u64, path: &str, fence: FenceClass) -> MutationRecord {
        MutationRecord {
            format_version: 1,
            sequence,
            operation_id: Uuid::from_u128(sequence as u128),
            path: path.to_owned(),
            kind: MutationKind::Delete,
            local_etag: LocalEtag::new(Uuid::nil(), sequence),
            accepted_at_unix_ms: sequence,
            remote_predecessor_etag: None,
            remote_result_etag: None,
            fence,
            retry_count: 0,
            last_error: None,
        }
    }

    #[tokio::test]
    async fn pending_remote_operation_becomes_a_retryable_timeout() {
        let mutation = record(41, "segments/stalled", FenceClass::ImmutableCreate);

        let error = bounded_remote_operation(
            &mutation,
            Duration::from_millis(10),
            future::pending::<object_store::Result<PutResult>>(),
        )
        .await
        .expect_err("a permanently pending remote operation must time out");

        let message = error.to_string();
        assert!(message.contains("41"), "timeout identifies the sequence");
        assert!(
            message.contains("timed out"),
            "timeout remains distinguishable from a provider error"
        );
    }

    #[test]
    fn remote_batches_preupload_only_immutable_objects_across_fences() {
        let records = vec![
            record(1, "segments/1", FenceClass::ImmutableCreate),
            record(2, "segments/2", FenceClass::ImmutableCreate),
            record(3, "manifest/current", FenceClass::Fence),
            record(4, "segments/4", FenceClass::ImmutableCreate),
        ];

        let completed = Default::default();
        let first = collect_pipeline_batch(&records, 1, 8, &completed, &Default::default());
        assert_eq!(
            first
                .iter()
                .map(|record| record.sequence)
                .collect::<Vec<_>>(),
            vec![1, 2, 4]
        );

        let fence = collect_pipeline_batch(&records, 3, 8, &completed, &Default::default());
        assert_eq!(
            fence
                .iter()
                .map(|record| record.sequence)
                .collect::<Vec<_>>(),
            vec![3, 4]
        );

        let after = collect_pipeline_batch(&records, 4, 8, &completed, &Default::default());
        assert_eq!(
            after
                .iter()
                .map(|record| record.sequence)
                .collect::<Vec<_>>(),
            vec![4]
        );
    }

    #[test]
    fn remote_preupload_never_overtakes_an_earlier_same_key_mutation() {
        let records = vec![
            record(1, "segments/shared", FenceClass::Fence),
            record(2, "segments/shared", FenceClass::ImmutableCreate),
            record(3, "segments/independent", FenceClass::ImmutableCreate),
        ];

        let batch =
            collect_pipeline_batch(&records, 1, 8, &Default::default(), &Default::default());

        assert_eq!(
            batch
                .iter()
                .map(|record| record.sequence)
                .collect::<Vec<_>>(),
            vec![1, 3]
        );
    }

    #[test]
    fn held_remote_completions_do_not_idle_network_slots() {
        let records = vec![
            record(1, "manifest/frontier", FenceClass::Fence),
            record(2, "segments/held-2", FenceClass::ImmutableCreate),
            record(3, "segments/new-3", FenceClass::ImmutableCreate),
            record(4, "segments/held-4", FenceClass::ImmutableCreate),
            record(5, "segments/new-5", FenceClass::ImmutableCreate),
            record(6, "segments/held-6", FenceClass::ImmutableCreate),
            record(7, "segments/new-7", FenceClass::ImmutableCreate),
        ];
        let completed = [2_u64, 4, 6]
            .into_iter()
            .map(|sequence| {
                (
                    sequence,
                    CompletedRemote {
                        record: records[(sequence - 1) as usize].clone(),
                        e_tag: None,
                    },
                )
            })
            .collect();

        let batch = collect_pipeline_batch(&records, 1, 4, &completed, &Default::default());

        assert_eq!(
            batch
                .iter()
                .map(|record| record.sequence)
                .collect::<Vec<_>>(),
            vec![1, 3, 5, 7]
        );
    }

    #[test]
    fn held_completion_cap_never_blocks_the_remote_frontier() {
        let records = (1_u64..=17)
            .map(|sequence| {
                record(
                    sequence,
                    &format!("segments/{sequence}"),
                    FenceClass::ImmutableCreate,
                )
            })
            .collect::<Vec<_>>();
        let completed = (2_u64..=17)
            .map(|sequence| {
                (
                    sequence,
                    CompletedRemote {
                        record: records[(sequence - 1) as usize].clone(),
                        e_tag: None,
                    },
                )
            })
            .collect();

        let batch = collect_pipeline_batch(&records, 1, 4, &completed, &Default::default());

        assert_eq!(
            batch
                .iter()
                .map(|record| record.sequence)
                .collect::<Vec<_>>(),
            vec![1],
            "held speculative completions must reserve one slot for the ordering frontier"
        );
    }

    #[test]
    fn scheduler_window_rejects_a_missing_durable_frontier() {
        let window = SchedulerWindow {
            local_seq: 3,
            records: vec![record(2, "segments/2", FenceClass::ImmutableCreate)],
        };

        let error = validate_scheduler_window(&window, 1)
            .expect_err("a durable journal gap must fail closed instead of spinning");

        assert!(error.to_string().contains("sequence 1"));
    }

    #[test]
    fn active_remote_uploads_are_not_dispatched_twice() {
        let records = (1_u64..=6)
            .map(|sequence| {
                record(
                    sequence,
                    &format!("segments/{sequence}"),
                    FenceClass::ImmutableCreate,
                )
            })
            .collect::<Vec<_>>();
        let active = [1_u64, 2, 3].into_iter().collect();

        let batch = collect_pipeline_batch(&records, 1, 4, &Default::default(), &active);

        assert_eq!(
            batch
                .iter()
                .map(|record| record.sequence)
                .collect::<Vec<_>>(),
            vec![4]
        );
    }
}
