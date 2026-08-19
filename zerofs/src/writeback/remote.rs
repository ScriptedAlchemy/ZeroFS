use crate::writeback::admission::{Admission, DiskAdmission};
use crate::writeback::barrier::{BarrierError, SequenceBarrier, SequenceProgress};
use crate::writeback::journal::Journal;
use crate::writeback::journaler::{LocalBarrier, LocalBarrierError};
use crate::writeback::model::{
    FenceClass, LocalEtag, MutationKind, MutationMode, MutationRecord, Sequence,
};
use crate::writeback::overlay::OverlayIndex;
use bytes::Bytes;
use futures::FutureExt;
use futures::future::BoxFuture;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt, PutMode, PutOptions, PutResult, UpdateVersion};
use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use tokio::sync::{Mutex, watch};
use tokio::task::{JoinHandle, JoinSet};

const REMOTE_COALESCE_IDLE: Duration = Duration::from_millis(500);
const REMOTE_RETRY_DELAY: Duration = Duration::from_millis(200);
const REMOTE_OPERATION_TIMEOUT: Duration = Duration::from_secs(120);

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum RemoteBarrierError {
    #[error("remote writeback scheduler is closed")]
    Closed,
    #[error("remote writeback failed: {0}")]
    Remote(String),
    #[error("remote writeback journal incarnation is stale")]
    StaleIncarnation,
}

impl BarrierError for RemoteBarrierError {
    fn closed() -> Self {
        Self::Closed
    }

    fn terminal(error: String) -> Self {
        Self::Remote(error)
    }

    fn stale_incarnation() -> Self {
        Self::StaleIncarnation
    }
}

fn publish_terminal(
    progress: &watch::Sender<SequenceProgress>,
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
    progress: SequenceBarrier<RemoteBarrierError>,
    incarnation: uuid::Uuid,
}

impl RemoteBarrier {
    pub fn incarnation(&self) -> uuid::Uuid {
        self.incarnation
    }

    pub async fn wait_remote(&self, sequence: Sequence) -> Result<(), RemoteBarrierError> {
        self.progress.wait(self.incarnation, sequence).await
    }
}

#[derive(Clone)]
pub struct RemoteScheduler {
    inner: Arc<RemoteSchedulerInner>,
}

struct RemoteSchedulerInner {
    barrier: RemoteBarrier,
    activate: watch::Sender<bool>,
    stop: watch::Sender<bool>,
    shutdown_started: AtomicBool,
    join: Mutex<Option<JoinHandle<()>>>,
    shutdown_result: watch::Sender<Option<Result<(), RemoteBarrierError>>>,
    #[cfg(test)]
    terminal_pause: Arc<TerminalPublicationPauseState>,
}

#[cfg(test)]
#[derive(Debug, Default)]
struct TerminalPublicationPauseState {
    armed: AtomicBool,
    entered: AtomicBool,
    released: AtomicBool,
    entered_notify: tokio::sync::Notify,
    release_notify: tokio::sync::Notify,
}

#[cfg(test)]
pub(crate) struct TerminalPublicationPause {
    state: Arc<TerminalPublicationPauseState>,
}

#[cfg(test)]
impl TerminalPublicationPause {
    pub(crate) async fn wait_entered(&self) {
        loop {
            let entered = self.state.entered_notify.notified();
            if self.state.entered.load(Ordering::Acquire) {
                return;
            }
            entered.await;
        }
    }

    pub(crate) fn release(&self) {
        self.state.released.store(true, Ordering::Release);
        self.state.release_notify.notify_one();
    }
}

#[cfg(test)]
impl Drop for TerminalPublicationPause {
    fn drop(&mut self) {
        self.release();
    }
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
        let snapshot = journal.snapshot()?;
        let journal_progress = journal.progress()?;
        let (progress_sender, progress) = watch::channel(SequenceProgress {
            incarnation: snapshot.incarnation,
            sequence: journal_progress.remote_seq,
            terminal_error: None,
            closed: false,
        });
        let (activate, activation) = watch::channel(active);
        let (stop, stop_receiver) = watch::channel(false);
        let (shutdown_result, _) = watch::channel(None);
        #[cfg(test)]
        let terminal_pause = Arc::new(TerminalPublicationPauseState::default());
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
            #[cfg(test)]
            terminal_pause: terminal_pause.clone(),
        }));
        Ok(Self {
            inner: Arc::new(RemoteSchedulerInner {
                barrier: RemoteBarrier {
                    progress: SequenceBarrier::new(progress),
                    incarnation: snapshot.incarnation,
                },
                activate,
                stop,
                shutdown_started: AtomicBool::new(false),
                join: Mutex::new(Some(join)),
                shutdown_result,
                #[cfg(test)]
                terminal_pause,
            }),
        })
    }

    pub fn barrier(&self) -> RemoteBarrier {
        self.inner.barrier.clone()
    }

    pub fn terminal_error(&self) -> Option<String> {
        self.inner.barrier.progress.snapshot().terminal_error
    }

    pub fn check_available(&self) -> Result<(), RemoteBarrierError> {
        let state = self.inner.barrier.progress.snapshot();
        if let Some(error) = &state.terminal_error {
            return Err(RemoteBarrierError::Remote(error.clone()));
        }
        if state.closed || self.inner.shutdown_started.load(Ordering::Acquire) {
            return Err(RemoteBarrierError::Closed);
        }
        Ok(())
    }

    pub fn activate(&self) -> Result<(), RemoteBarrierError> {
        self.check_available()?;
        self.inner
            .activate
            .send(true)
            .map_err(|_| RemoteBarrierError::Closed)
    }

    pub async fn shutdown(&self) -> Result<(), RemoteBarrierError> {
        let mut completion = self.inner.shutdown_result.subscribe();
        if !self.inner.shutdown_started.swap(true, Ordering::AcqRel) {
            let inner = self.inner.clone();
            tokio::spawn(async move {
                drive_remote_shutdown(inner).await;
            });
        }

        loop {
            if let Some(result) = completion.borrow().clone() {
                return result;
            }
            if completion.changed().await.is_err() {
                return Err(remote_terminal_or_closed(&self.inner.barrier));
            }
        }
    }

    #[cfg(test)]
    pub(crate) fn pause_terminal_publication(&self) -> TerminalPublicationPause {
        assert!(
            !self.inner.terminal_pause.armed.swap(true, Ordering::AcqRel),
            "terminal publication pause is already armed"
        );
        TerminalPublicationPause {
            state: self.inner.terminal_pause.clone(),
        }
    }

    #[cfg(test)]
    pub(crate) fn shutdown_requested(&self) -> bool {
        *self.inner.stop.borrow()
    }
}

async fn drive_remote_shutdown(inner: Arc<RemoteSchedulerInner>) {
    let _ = inner.stop.send(true);
    let mut outcome = Ok(());
    if let Some(join) = inner.join.lock().await.take()
        && let Err(error) = join.await
    {
        outcome = Err(RemoteBarrierError::Remote(format!(
            "worker panicked: {error}"
        )));
    }
    if let Some(error) = current_remote_terminal(&inner.barrier) {
        outcome = Err(error);
    }
    inner.shutdown_result.send_replace(Some(outcome));
}

fn remote_terminal_or_closed(barrier: &RemoteBarrier) -> RemoteBarrierError {
    barrier.progress.terminal_or_closed()
}

fn current_remote_terminal(barrier: &RemoteBarrier) -> Option<RemoteBarrierError> {
    barrier.progress.current_terminal()
}

struct RemoteWorker {
    remote: Arc<dyn ObjectStore>,
    journal: Arc<Journal>,
    overlay: OverlayIndex,
    admission: Admission,
    disk: DiskAdmission,
    local: LocalBarrier,
    upload_concurrency: usize,
    progress: watch::Sender<SequenceProgress>,
    activation: watch::Receiver<bool>,
    stop: watch::Receiver<bool>,
    #[cfg(test)]
    terminal_pause: Arc<TerminalPublicationPauseState>,
}

struct CompletedRemote {
    // The object is durable remotely, but its journal watermark cannot advance
    // until every earlier ordering fence has committed.
    record: MutationRecord,
    e_tag: Option<String>,
}

type RemoteOutcome = (MutationRecord, object_store::Result<PutResult>);
type RemoteCommit = BoxFuture<'static, (Sequence, anyhow::Result<()>)>;

struct SchedulerWindow {
    local_seq: Sequence,
    remote_seq: Sequence,
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
        #[cfg(test)]
        terminal_pause,
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
        let mut window = match coalesce_local_batch(
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
        let mut active = JoinSet::<RemoteOutcome>::new();
        let mut active_sequences = BTreeSet::new();
        let mut committing = None::<RemoteCommit>;
        let mut retry = false;
        let mut terminal_error = None::<String>;
        loop {
            #[cfg(test)]
            if terminal_error.is_some() && terminal_pause.armed.load(Ordering::Acquire) {
                terminal_pause.entered.store(true, Ordering::Release);
                terminal_pause.entered_notify.notify_one();
                loop {
                    let released = terminal_pause.release_notify.notified();
                    if terminal_pause.released.load(Ordering::Acquire) {
                        break;
                    }
                    released.await;
                }
            }
            if committing.is_none() && terminal_error.is_none() {
                committing = start_ready_commit(&journal, &overlay, &disk, next, &completed);
            }
            if !retry && terminal_error.is_none() {
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
                    active.spawn(async move {
                        let applying = apply_record(remote, journal, record.clone());
                        let result =
                            bounded_remote_operation(&record, REMOTE_OPERATION_TIMEOUT, applying)
                                .await;
                        (record, result)
                    });
                }
            }
            if active.is_empty() && committing.is_none() {
                if let Some(error) = terminal_error {
                    publish_terminal(&progress, &admission, &disk, error, true);
                    return;
                }
                break;
            }
            enum SchedulerEvent {
                Remote,
                Commit((Sequence, anyhow::Result<()>)),
            }
            let mut remote_outcome = None;
            let event = tokio::select! {
                outcome = active.join_next(), if !active.is_empty() => {
                    remote_outcome = Some(outcome);
                    SchedulerEvent::Remote
                },
                outcome = async {
                    committing
                        .as_mut()
                        .expect("guarded remote commit future exists")
                        .await
                }, if committing.is_some() => SchedulerEvent::Commit(outcome),
                changed = stop.changed() => {
                    if changed.is_err() || *stop.borrow() {
                        let mut shutdown_error = terminal_error.take();
                        abort_and_join_remote(&mut active).await;
                        if let Some(commit) = committing.take() {
                            let (sequence, result) = commit.await;
                            if let Err(error) = finish_ordered_commit(
                                sequence,
                                result,
                                &progress,
                                &mut next,
                                &mut completed,
                            ) {
                                shutdown_error.get_or_insert_with(|| format!("{error:#}"));
                            }
                        }
                        if let Some(error) = shutdown_error {
                            publish_terminal(&progress, &admission, &disk, error, true);
                        }
                        break 'scheduler;
                    }
                    continue;
                },
            };
            let (record, result) = match event {
                SchedulerEvent::Commit((sequence, result)) => {
                    committing = None;
                    if let Err(error) = finish_ordered_commit(
                        sequence,
                        result,
                        &progress,
                        &mut next,
                        &mut completed,
                    ) {
                        terminal_error = Some(format!("{error:#}"));
                        active.abort_all();
                    }
                    continue;
                }
                SchedulerEvent::Remote => {
                    match remote_outcome.expect("remote scheduler event stores its join result") {
                        Some(Ok(_)) if terminal_error.is_some() => continue,
                        Some(Ok(outcome)) => outcome,
                        Some(Err(error)) if error.is_cancelled() && terminal_error.is_some() => {
                            continue;
                        }
                        Some(Err(error)) => {
                            terminal_error = Some(format!("remote upload task failed: {error}"));
                            active.abort_all();
                            continue;
                        }
                        None => continue,
                    }
                }
            };
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
                        terminal_error = Some(format!(
                            "failed to persist remote retry for sequence {}: {journal_error:#}",
                            record.sequence
                        ));
                        active.abort_all();
                        continue;
                    }
                    if is_terminal_remote_error(&error) {
                        terminal_error = Some(format!(
                            "permanent remote divergence at sequence {}: {error}",
                            record.sequence
                        ));
                        active.abort_all();
                        continue;
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
            if !retry {
                window = match load_scheduler_window(&journal, next, upload_concurrency) {
                    Ok(window) => window,
                    Err(error) => {
                        terminal_error = Some(format!("{error:#}"));
                        active.abort_all();
                        continue;
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

async fn abort_and_join_remote(active: &mut JoinSet<RemoteOutcome>) {
    active.abort_all();
    while active.join_next().await.is_some() {}
}

fn finish_ordered_commit(
    sequence: Sequence,
    result: anyhow::Result<()>,
    progress: &watch::Sender<SequenceProgress>,
    next: &mut Sequence,
    completed: &mut BTreeMap<Sequence, CompletedRemote>,
) -> anyhow::Result<()> {
    result?;
    // The committed run is every held completion up to and including the
    // batch tail; nothing below the frontier can re-enter `completed` while
    // the commit was in flight because completed sequences are never
    // re-dispatched.
    let incremented = sequence
        .checked_add(1)
        .ok_or_else(|| anyhow::anyhow!("remote sequence overflow"))?;
    let retained = completed.split_off(&incremented);
    let committed = std::mem::replace(completed, retained);
    if committed.last_key_value().map(|(&last, _)| last) != Some(sequence) {
        anyhow::bail!("completed remote commit {sequence} was no longer tracked");
    }
    progress.send_modify(|state| state.sequence = sequence);
    *next = incremented;
    Ok(())
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
    let scan_limit = upload_concurrency.saturating_mul(8).max(upload_concurrency);
    let pending = journal.pending_window(first_sequence, scan_limit)?;
    let window = SchedulerWindow {
        local_seq: pending.local_seq,
        remote_seq: pending.remote_seq,
        records: pending.records,
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
    // `commit_remote_run` advances the durable remote watermark and then prunes the
    // records it covers, while the scheduler's own frontier only advances once
    // `finish_ordered_commit` runs. Any read taken during that gap legitimately
    // sees the frontier already pruned, so the lowest sequence the journal must
    // still carry is the first one above the durable watermark. Everything from
    // there stays strictly contiguous: a hole above the watermark is corruption.
    let durable_frontier = window
        .remote_seq
        .saturating_add(1)
        .max(first_sequence)
        .min(window.local_seq.saturating_add(1));
    let mut expected = None::<Sequence>;
    for record in &window.records {
        match expected {
            Some(expected) if record.sequence != expected => {
                anyhow::bail!(
                    "durable writeback journal is missing remote frontier sequence {expected}; found sequence {}",
                    record.sequence
                );
            }
            None if record.sequence < first_sequence || record.sequence > durable_frontier => {
                anyhow::bail!(
                    "durable writeback journal is missing remote frontier sequence {durable_frontier}; found sequence {}",
                    record.sequence
                );
            }
            _ => {}
        }
        expected = Some(
            record
                .sequence
                .checked_add(1)
                .ok_or_else(|| anyhow::anyhow!("remote scheduler sequence overflow"))?,
        );
    }
    if window.records.is_empty() && durable_frontier <= window.local_seq {
        anyhow::bail!(
            "durable writeback journal is missing remote frontier sequence {durable_frontier}"
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

fn start_ready_commit(
    journal: &Arc<Journal>,
    overlay: &OverlayIndex,
    disk: &DiskAdmission,
    next: Sequence,
    completed: &BTreeMap<Sequence, CompletedRemote>,
) -> Option<RemoteCommit> {
    completed.get(&next)?;
    // Every completion contiguous with the frontier commits in one durable
    // journal transaction. The watermark still advances contiguously — the
    // run is contiguous by construction — but a burst of held speculative
    // completions no longer pays one fsync'd transaction per record, which
    // otherwise becomes the remote replay throughput ceiling.
    let mut run = Vec::new();
    let mut expected = next;
    for (&sequence, completion) in completed.range(next..) {
        if sequence != expected {
            break;
        }
        run.push((completion.record.clone(), completion.e_tag.clone()));
        let Some(incremented) = expected.checked_add(1) else {
            break;
        };
        expected = incremented;
    }
    let journal = Arc::clone(journal);
    let overlay = overlay.clone();
    let disk = disk.clone();
    Some(
        async move {
            let last = run
                .last()
                .expect("ready commit run contains the frontier")
                .0
                .sequence;
            let result = commit_remote_run(&journal, &overlay, &disk, &run).await;
            (last, result)
        }
        .boxed(),
    )
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
                let (predecessor_incarnation, predecessor_sequence) = LocalEtag::parse(expected)
                    .ok_or_else(|| {
                        precondition(
                            &record.path,
                            "update has no durable remote predecessor ETag",
                        )
                    })?;
                let current_incarnation = journal
                    .snapshot()
                    .map_err(|error| generic_error(format!("journal snapshot failed: {error}")))?
                    .incarnation;
                if predecessor_incarnation != current_incarnation {
                    return Err(precondition(
                        &record.path,
                        "update predecessor ETag belongs to a stale journal incarnation",
                    ));
                }
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
    let existing = remote.get(target).await?;
    let meta = existing.meta.clone();
    let existing = existing.bytes().await?;
    if existing != *expected {
        return Err(object_store::Error::AlreadyExists {
            path: target.to_string(),
            source: Box::new(RemoteContentDivergence),
        });
    }
    Ok(PutResult {
        e_tag: meta.e_tag,
        version: meta.version,
        extensions: Default::default(),
    })
}

async fn commit_remote_run(
    journal: &Arc<Journal>,
    overlay: &OverlayIndex,
    disk: &DiskAdmission,
    run: &[(MutationRecord, Option<String>)],
) -> anyhow::Result<()> {
    let last = run
        .last()
        .ok_or_else(|| anyhow::anyhow!("remote commit run must not be empty"))?
        .0
        .sequence;
    let completions = run
        .iter()
        .map(|(record, e_tag)| (record.sequence, e_tag.clone()))
        .collect::<Vec<_>>();
    let mark_journal = Arc::clone(journal);
    tokio::task::spawn_blocking(move || mark_journal.mark_remote_batch(&completions))
        .await
        .map_err(|error| anyhow::anyhow!("remote watermark task failed: {error}"))??;
    overlay.remove_remote_prefix(last).await;
    let charge = run.iter().try_fold(0_u64, |total, (record, _)| {
        let bytes = record.ssd_reservation_bytes()?;
        total
            .checked_add(bytes)
            .ok_or_else(|| anyhow::anyhow!("remote SSD reservation overflow"))
    })?;
    let cleanup_journal = Arc::clone(journal);
    let available = tokio::task::spawn_blocking(move || -> anyhow::Result<u64> {
        cleanup_journal.remove_remote_prefix(last)?;
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
        load_scheduler_window, validate_scheduler_window, verify_existing,
    };
    use crate::fault_store::FaultStore;
    use crate::writeback::journal::Journal;
    use crate::writeback::model::{FenceClass, JournalIdentity, MutationMode, MutationRecord};
    use bytes::Bytes;
    use futures::future;
    use object_store::memory::InMemory;
    use object_store::{ObjectStore, ObjectStoreExt, PutPayload, PutResult, path::Path};
    use std::sync::Arc;
    use std::time::Duration;

    fn record(sequence: u64, path: &str, fence: FenceClass) -> MutationRecord {
        crate::writeback::test_util::delete_record(sequence, path, fence, 0, 0)
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

    #[tokio::test]
    async fn verify_existing_reuses_metadata_from_its_single_get() {
        let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let target = Path::from("segments/existing");
        let expected = Bytes::from_static(b"matching payload");
        let created = inner
            .put(&target, PutPayload::from(expected.clone()))
            .await
            .unwrap();
        let (remote, controls) = FaultStore::new(inner);

        let reconciled = verify_existing(remote.as_ref(), &target, &expected)
            .await
            .expect("matching existing content is a successful lost-reply reconciliation");

        assert_eq!(reconciled.e_tag, created.e_tag);
        assert_eq!(reconciled.version, created.version);
        assert_eq!(
            controls.get_count(),
            1,
            "GetResult already carries the metadata; a second HEAD is redundant"
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

    fn put_record(sequence: u64, path: &str, payload: &[u8]) -> MutationRecord {
        crate::writeback::test_util::put_record(
            sequence,
            path,
            payload,
            MutationMode::Create,
            FenceClass::ImmutableCreate,
            0x1000,
            1_786_435_200_000,
        )
    }

    fn journal_with_local_records(root: &std::path::Path, count: u64) -> Journal {
        let journal = Journal::open(
            root.join("writeback"),
            JournalIdentity {
                format_version: 1,
                bucket_id: "bucket-a".to_owned(),
                backend_endpoint: "memory://remote".to_owned(),
                database_prefix: "zerofs/pilot".to_owned(),
                backend_kind: "memory".to_owned(),
                encryption_key_identity_sha256: [0x77; 32],
            },
        )
        .unwrap();
        for sequence in 1..=count {
            journal
                .commit_put(
                    put_record(sequence, &format!("segments/{sequence}"), b"payload"),
                    b"payload",
                )
                .unwrap();
        }
        journal
    }

    #[test]
    fn scheduler_window_tolerates_the_frontier_its_own_commit_already_pruned() {
        let temp = tempfile::tempdir().unwrap();
        let journal = journal_with_local_records(temp.path(), 3);
        // `commit_remote_run` advances the durable watermark and prunes the run
        // it just uploaded. The scheduler's in-memory frontier only advances
        // afterwards, in `finish_ordered_commit`, so every journal read taken
        // while that commit is in flight still carries the pre-commit frontier.
        journal.mark_remote(1, None).unwrap();
        journal.remove_remote_prefix(1).unwrap();

        let window = load_scheduler_window(&journal, 1, 4)
            .expect("a commit that already pruned its own frontier is not journal corruption");

        assert_eq!(
            window.records.first().map(|record| record.sequence),
            Some(2)
        );
    }

    #[test]
    fn scheduler_window_rejects_a_missing_durable_frontier() {
        let window = SchedulerWindow {
            local_seq: 3,
            remote_seq: 0,
            records: vec![record(2, "segments/2", FenceClass::ImmutableCreate)],
        };

        let error = validate_scheduler_window(&window, 1)
            .expect_err("a durable journal gap must fail closed instead of spinning");

        assert!(error.to_string().contains("sequence 1"));
    }

    #[test]
    fn scheduler_window_rejects_a_durable_gap_above_the_remote_watermark() {
        // Sequence 1 is legitimately pruned by its own in-flight commit, but the
        // hole between 2 and 4 is not covered by any watermark: still corruption.
        let window = SchedulerWindow {
            local_seq: 4,
            remote_seq: 1,
            records: vec![
                record(2, "segments/2", FenceClass::ImmutableCreate),
                record(4, "segments/4", FenceClass::ImmutableCreate),
            ],
        };

        let error = validate_scheduler_window(&window, 1)
            .expect_err("a hole above the durable watermark must still fail closed");

        assert!(error.to_string().contains("sequence 3"), "{error}");
    }

    #[test]
    fn scheduler_window_rejects_an_empty_journal_below_the_remote_watermark() {
        let window = SchedulerWindow {
            local_seq: 4,
            remote_seq: 1,
            records: Vec::new(),
        };

        let error = validate_scheduler_window(&window, 1)
            .expect_err("a vanished uncommitted backlog must fail closed");

        assert!(error.to_string().contains("sequence 2"), "{error}");
    }

    #[test]
    fn scheduler_window_accepts_a_fully_committed_journal() {
        let window = SchedulerWindow {
            local_seq: 4,
            remote_seq: 4,
            records: Vec::new(),
        };

        validate_scheduler_window(&window, 1)
            .expect("a journal whose backlog is fully committed is not corrupt");
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
