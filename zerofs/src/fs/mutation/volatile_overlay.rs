//! Shared volatile write acknowledgement overlay.
//!
//! WRITE owns the payload in a bounded process-wide RAM pool, publishes it to
//! the read overlay, and only then replies. Each runtime owns visibility state
//! for one backing inode; the materializer's inode lane is the sole execution
//! owner for its accepted sequence. Striped logical writes fan out one runtime
//! per member above this layer. FLUSH/FUA wait for the captured sequence before
//! entering the existing filesystem durability barrier.

use crate::fs::errors::FsError;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum OverlayError {
    InvalidArgument,
    IoError,
    NoSpace,
}

impl From<FsError> for OverlayError {
    fn from(_: FsError) -> Self {
        Self::IoError
    }
}

pub(crate) type OverlayResult<T> = Result<T, OverlayError>;
use bytes::{Bytes, BytesMut};
#[cfg(test)]
use futures::FutureExt;
use futures::future::BoxFuture;
use std::collections::{BTreeMap, BTreeSet};
#[cfg(test)]
use std::panic::AssertUnwindSafe;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
#[cfg(test)]
use tokio::sync::mpsc;
#[cfg(test)]
use tokio::sync::oneshot;
use tokio::sync::{Notify, RwLock};
use tokio_util::sync::CancellationToken;
#[cfg(test)]
use tokio_util::task::TaskTracker;

#[cfg(test)]
pub(crate) type Materializer =
    Arc<dyn Fn(u64, u64, Bytes) -> BoxFuture<'static, OverlayResult<()>> + Send + Sync + 'static>;
pub(crate) type IdleHook = Arc<dyn Fn(u64, u64) + Send + Sync + 'static>;

const GRACEFUL_DRAIN_TIMEOUT: Duration = Duration::from_secs(60);

#[derive(Debug)]
struct BudgetState {
    used_bytes: u64,
    used_operations: usize,
    terminal: bool,
}

/// One admission authority shared by every volatile NBD export.
pub(crate) struct VolatileBudget {
    max_bytes: u64,
    max_operations: usize,
    state: Mutex<BudgetState>,
    changed: Notify,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct VolatileBudgetStatus {
    capacity_bytes: u64,
    pub(crate) dirty_bytes: u64,
    pub(crate) dirty_operations: usize,
    terminal: bool,
}

impl VolatileBudget {
    pub(crate) fn new(max_bytes: u64, max_operations: usize) -> Arc<Self> {
        let budget = Arc::new(Self {
            max_bytes,
            max_operations,
            state: Mutex::new(BudgetState {
                used_bytes: 0,
                used_operations: 0,
                terminal: false,
            }),
            changed: Notify::new(),
        });
        budget.record_metrics();
        budget
    }

    pub(crate) async fn reserve_many_while<F>(
        self: &Arc<Self>,
        member_bytes: &[u64],
        accepting: F,
    ) -> OverlayResult<Vec<BudgetPermit>>
    where
        F: Fn() -> bool,
    {
        let total_bytes = member_bytes
            .iter()
            .try_fold(0_u64, |total, bytes| total.checked_add(*bytes))
            .ok_or(OverlayError::NoSpace)?;
        if member_bytes.is_empty() {
            return Err(OverlayError::InvalidArgument);
        }
        if total_bytes > self.max_bytes {
            return Err(OverlayError::NoSpace);
        }
        loop {
            let changed = self.changed.notified();
            if !accepting() {
                return Err(OverlayError::IoError);
            }
            let permits = {
                let mut state = self.state.lock().expect("volatile budget poisoned");
                if state.terminal {
                    return Err(OverlayError::IoError);
                }
                if let Some(used_bytes) = state.used_bytes.checked_add(total_bytes)
                    && used_bytes <= self.max_bytes
                    && state.used_operations < self.max_operations
                {
                    state.used_bytes = used_bytes;
                    state.used_operations += 1;
                    let operation = Arc::new(BudgetOperationPermit {
                        budget: Arc::clone(self),
                    });
                    Some(
                        member_bytes
                            .iter()
                            .map(|bytes| BudgetPermit {
                                operation: Arc::clone(&operation),
                                bytes: *bytes,
                            })
                            .collect::<Vec<_>>(),
                    )
                } else {
                    None
                }
            };
            if let Some(permits) = permits {
                self.record_metrics();
                if !accepting() {
                    drop(permits);
                    return Err(OverlayError::IoError);
                }
                return Ok(permits);
            }
            changed.await;
        }
    }

    fn poison(&self) {
        let mut state = self.state.lock().expect("volatile budget poisoned");
        state.terminal = true;
        drop(state);
        self.record_metrics();
        self.changed.notify_waiters();
    }

    pub(crate) fn status(&self) -> VolatileBudgetStatus {
        let state = self.state.lock().expect("volatile budget poisoned");
        VolatileBudgetStatus {
            capacity_bytes: self.max_bytes,
            dirty_bytes: state.used_bytes,
            dirty_operations: state.used_operations,
            terminal: state.terminal,
        }
    }

    fn is_terminal(&self) -> bool {
        self.state
            .lock()
            .expect("volatile budget poisoned")
            .terminal
    }

    fn record_metrics(&self) {
        let status = self.status();
        metrics::gauge!("zerofs_nbd_volatile_memory_enabled").set(1.0);
        metrics::gauge!("zerofs_nbd_volatile_memory_capacity_bytes")
            .set(status.capacity_bytes as f64);
        metrics::gauge!("zerofs_nbd_volatile_memory_dirty_bytes").set(status.dirty_bytes as f64);
        metrics::gauge!("zerofs_nbd_volatile_memory_dirty_operations")
            .set(status.dirty_operations as f64);
        metrics::gauge!("zerofs_nbd_volatile_memory_terminal").set(f64::from(status.terminal));
    }

    pub(crate) fn notify_waiters(&self) {
        self.changed.notify_waiters();
    }

    #[cfg(test)]
    fn used_bytes(&self) -> u64 {
        self.state
            .lock()
            .expect("volatile budget poisoned")
            .used_bytes
    }
}

pub(crate) struct BudgetPermit {
    operation: Arc<BudgetOperationPermit>,
    bytes: u64,
}

impl Drop for BudgetPermit {
    fn drop(&mut self) {
        let mut state = self
            .operation
            .budget
            .state
            .lock()
            .expect("volatile budget poisoned");
        state.used_bytes -= self.bytes;
        drop(state);
        self.operation.budget.record_metrics();
        self.operation.budget.changed.notify_waiters();
    }
}

struct BudgetOperationPermit {
    budget: Arc<VolatileBudget>,
}

impl Drop for BudgetOperationPermit {
    fn drop(&mut self) {
        let mut state = self.budget.state.lock().expect("volatile budget poisoned");
        state.used_operations -= 1;
        drop(state);
        self.budget.record_metrics();
        self.budget.changed.notify_waiters();
    }
}

pub(crate) struct VolatileAdmission {
    permit: BudgetPermit,
}

impl VolatileAdmission {
    pub(crate) fn from_permit(permit: BudgetPermit) -> Self {
        Self { permit }
    }
}

struct OverlayEntry {
    offset: u64,
    data: Bytes,
    visibility: Arc<WriteVisibility>,
    _permit: BudgetPermit,
}

/// One atomic visibility decision shared by every member of a logical write.
/// Staged runtime entries can materialize or roll back, but reads do not
/// observe any member until this token is published once for the whole write.
pub(crate) struct WriteVisibility {
    published: AtomicBool,
}

impl WriteVisibility {
    pub(crate) fn staged() -> Arc<Self> {
        Arc::new(Self {
            published: AtomicBool::new(false),
        })
    }

    #[allow(dead_code)]
    pub(crate) fn published() -> Arc<Self> {
        Arc::new(Self {
            published: AtomicBool::new(true),
        })
    }

    pub(crate) fn publish(&self) {
        self.published.store(true, Ordering::Release);
    }

    pub(crate) fn is_published(&self) -> bool {
        self.published.load(Ordering::Acquire)
    }
}

struct State {
    accepting: bool,
    next_sequence: u64,
    materialized_through: u64,
    completed: BTreeSet<u64>,
    entries: BTreeMap<u64, Arc<OverlayEntry>>,
    terminal: Option<(u64, OverlayError)>,
}

#[cfg(test)]
struct AcceptedWrite {
    sequence: u64,
}

#[cfg(test)]
struct IdleRetirementPause {
    reached: oneshot::Sender<()>,
    resume: oneshot::Receiver<()>,
}

#[cfg(test)]
struct RuntimeWorkerCompletion {
    runtime: std::sync::Weak<VolatileWriteRuntime>,
}

#[cfg(test)]
impl Drop for RuntimeWorkerCompletion {
    fn drop(&mut self) {
        if let Some(runtime) = self.runtime.upgrade() {
            runtime.worker_running.store(false, Ordering::Release);
            runtime.worker_stopped.notify_waiters();
        }
    }
}

pub(crate) struct VolatileWriteRuntime {
    state: Mutex<State>,
    changed: Notify,
    /// Completion takes the write side before retiring entries.  A partial
    /// read holds the read side across its canonical read and overlay snapshot,
    /// so partially materialized data can never escape through the base layer.
    retirement: RwLock<()>,
    budget: Arc<VolatileBudget>,
    inode: u64,
    generation: u64,
    idle_hook: Option<IdleHook>,
    task_shutdown: CancellationToken,
    #[cfg(test)]
    ingress: Option<mpsc::UnboundedSender<AcceptedWrite>>,
    #[cfg(test)]
    worker_running: AtomicBool,
    #[cfg(test)]
    worker_stopped: Notify,
    #[cfg(test)]
    idle_retirement_pause: Mutex<Option<IdleRetirementPause>>,
}

impl VolatileWriteRuntime {
    #[cfg(test)]
    pub(crate) fn new(
        budget: Arc<VolatileBudget>,
        inode: u64,
        materialize: Materializer,
    ) -> Arc<Self> {
        let workers = TaskTracker::new();
        let (ingress, mut ingress_rx) = mpsc::unbounded_channel::<AcceptedWrite>();
        let task_shutdown = CancellationToken::new();
        let runtime = Arc::new(Self {
            state: Mutex::new(State {
                accepting: true,
                next_sequence: 0,
                materialized_through: 0,
                completed: BTreeSet::new(),
                entries: BTreeMap::new(),
                terminal: None,
            }),
            changed: Notify::new(),
            retirement: RwLock::new(()),
            budget,
            inode,
            generation: 0,
            idle_hook: None,
            task_shutdown: task_shutdown.clone(),
            ingress: Some(ingress),
            worker_running: AtomicBool::new(true),
            worker_stopped: Notify::new(),
            idle_retirement_pause: Mutex::new(None),
        });

        let worker_runtime_weak = Arc::downgrade(&runtime);
        let panic_runtime_weak = worker_runtime_weak.clone();
        drop(workers.spawn(async move {
            let _completion = RuntimeWorkerCompletion {
                runtime: worker_runtime_weak.clone(),
            };
            let outcome = AssertUnwindSafe(async move {
                loop {
                    let write = tokio::select! {
                        biased;
                        _ = task_shutdown.cancelled() => return,
                        write = ingress_rx.recv() => match write {
                            Some(write) => write,
                            None => return,
                        },
                    };
                    let Some(runtime) = worker_runtime_weak.upgrade() else {
                        return;
                    };
                    if runtime.terminal().is_some() {
                        return;
                    }
                    let Some(entry) = runtime.entry_for_test(write.sequence) else {
                        runtime.fail(write.sequence, OverlayError::IoError);
                        return;
                    };
                    let materialization =
                        AssertUnwindSafe(materialize(inode, entry.offset, entry.data.clone()))
                            .catch_unwind();
                    tokio::pin!(materialization);
                    let outcome = tokio::select! {
                        biased;
                        _ = task_shutdown.cancelled() => return,
                        outcome = &mut materialization => outcome,
                    };
                    match outcome {
                        Ok(Ok(())) => runtime.complete(write.sequence).await,
                        Ok(Err(error)) => {
                            runtime.fail(write.sequence, error);
                            return;
                        }
                        Err(_) => {
                            runtime.fail(write.sequence, OverlayError::IoError);
                            return;
                        }
                    }
                }
            })
            .catch_unwind()
            .await;
            if outcome.is_err()
                && let Some(runtime) = panic_runtime_weak.upgrade()
            {
                runtime.fail(runtime.accepted_cutoff(), OverlayError::IoError);
            }
        }));

        runtime
    }

    pub(crate) fn new_state_only(
        budget: Arc<VolatileBudget>,
        inode: u64,
        generation: u64,
        idle_hook: Option<IdleHook>,
    ) -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(State {
                accepting: true,
                next_sequence: 0,
                materialized_through: 0,
                completed: BTreeSet::new(),
                entries: BTreeMap::new(),
                terminal: None,
            }),
            changed: Notify::new(),
            retirement: RwLock::new(()),
            budget,
            inode,
            generation,
            idle_hook,
            task_shutdown: CancellationToken::new(),
            #[cfg(test)]
            ingress: None,
            #[cfg(test)]
            worker_running: AtomicBool::new(false),
            #[cfg(test)]
            worker_stopped: Notify::new(),
            #[cfg(test)]
            idle_retirement_pause: Mutex::new(None),
        })
    }

    pub(crate) async fn reserve(&self, bytes: usize) -> OverlayResult<VolatileAdmission> {
        let mut permits = self
            .budget
            .reserve_many_while(&[bytes as u64], || self.can_accept())
            .await?;
        Ok(VolatileAdmission {
            permit: permits.pop().expect("one requested budget permit"),
        })
    }

    pub(crate) fn can_accept(&self) -> bool {
        let state = self.state.lock().expect("volatile runtime poisoned");
        state.terminal.is_none() && state.accepting
    }

    /// Accept a write that is visible the moment it is accepted, the shape a
    /// single-member logical write has once its preparation is published.
    #[cfg(test)]
    async fn accept_write(
        &self,
        admission: VolatileAdmission,
        offset: u64,
        data: Bytes,
    ) -> OverlayResult<u64> {
        self.accept_staged_write(admission, offset, data, WriteVisibility::published())
            .await
    }

    #[cfg(test)]
    async fn accept_staged_write(
        &self,
        admission: VolatileAdmission,
        offset: u64,
        data: Bytes,
        visibility: Arc<WriteVisibility>,
    ) -> OverlayResult<u64> {
        let ingress = self.ingress.as_ref().ok_or(OverlayError::IoError)?;
        self.accept_staged_write_enqueued(admission, offset, data, visibility, |sequence| {
            ingress
                .send(AcceptedWrite { sequence })
                .map_err(|_| OverlayError::IoError)
        })
    }

    /// Stage one write for this runtime's inode. The caller publishes
    /// `visibility` once every member of its logical write is accepted; a
    /// write staged with an already-published token is accounted immediately.
    pub(crate) fn accept_staged_write_enqueued<F>(
        &self,
        admission: VolatileAdmission,
        offset: u64,
        data: Bytes,
        visibility: Arc<WriteVisibility>,
        enqueue: F,
    ) -> OverlayResult<u64>
    where
        F: FnOnce(u64) -> OverlayResult<()>,
    {
        let record_accepted = visibility.is_published();
        if admission.permit.bytes != data.len() as u64 {
            return Err(OverlayError::InvalidArgument);
        }
        if data.is_empty() || offset.checked_add(data.len() as u64).is_none() {
            return Err(OverlayError::InvalidArgument);
        }
        if self.budget.is_terminal() {
            return Err(OverlayError::IoError);
        }
        let mut state = self.state.lock().expect("volatile runtime poisoned");
        if state.terminal.is_some() || !state.accepting {
            return Err(OverlayError::IoError);
        }
        let sequence = state
            .next_sequence
            .checked_add(1)
            .ok_or(OverlayError::IoError)?;
        let accepted_bytes = data.len() as u64;
        let entry = Arc::new(OverlayEntry {
            offset,
            data,
            visibility,
            _permit: admission.permit,
        });
        enqueue(sequence)?;
        state.next_sequence = sequence;
        state.entries.insert(sequence, Arc::clone(&entry));
        drop(state);
        if self.budget.is_terminal() {
            self.fail(sequence, OverlayError::IoError);
            return Err(OverlayError::IoError);
        }
        self.changed.notify_waiters();
        if record_accepted {
            record_published_staged_writes(accepted_bytes, 1);
        }
        Ok(sequence)
    }

    #[cfg(test)]
    fn entry_for_test(&self, sequence: u64) -> Option<Arc<OverlayEntry>> {
        self.state
            .lock()
            .expect("volatile runtime poisoned")
            .entries
            .get(&sequence)
            .cloned()
    }

    pub(crate) fn accepted_cutoff(&self) -> u64 {
        self.state
            .lock()
            .expect("volatile runtime poisoned")
            .next_sequence
    }

    #[cfg(test)]
    pub(crate) fn pause_next_idle_retirement_for_test(
        &self,
    ) -> (oneshot::Receiver<()>, oneshot::Sender<()>) {
        let (reached_tx, reached_rx) = oneshot::channel();
        let (resume_tx, resume_rx) = oneshot::channel();
        *self
            .idle_retirement_pause
            .lock()
            .expect("volatile worker poisoned") = Some(IdleRetirementPause {
            reached: reached_tx,
            resume: resume_rx,
        });
        (reached_rx, resume_tx)
    }

    #[cfg(test)]
    async fn pause_before_idle_retirement_for_test(&self) {
        let pause = self
            .idle_retirement_pause
            .lock()
            .expect("volatile worker poisoned")
            .take();
        if let Some(pause) = pause {
            let _ = pause.reached.send(());
            let _ = pause.resume.await;
        }
    }

    pub(crate) fn dirty_end(&self) -> u64 {
        self.snapshot()
            .iter()
            .map(|entry| entry.offset.saturating_add(entry.data.len() as u64))
            .max()
            .unwrap_or(0)
    }

    pub(crate) async fn wait_materialized(&self, target: u64) -> OverlayResult<()> {
        loop {
            let changed = self.changed.notified();
            let budget_changed = self.budget.changed.notified();
            {
                let state = self.state.lock().expect("volatile runtime poisoned");
                if state.terminal.is_some() || self.budget.is_terminal() {
                    return Err(OverlayError::IoError);
                }
                if state.materialized_through >= target {
                    return Ok(());
                }
            }
            tokio::select! {
                _ = changed => {}
                _ = budget_changed => {}
            }
        }
    }

    /// Wait until an unacknowledged entry has been retired or its runtime has
    /// reached a terminal state. Used only to finish rollback before returning
    /// a failed multi-member acceptance.
    pub(crate) async fn wait_released(&self, sequence: u64) {
        loop {
            let changed = self.changed.notified();
            {
                let state = self.state.lock().expect("volatile runtime poisoned");
                if !state.entries.contains_key(&sequence) || state.terminal.is_some() {
                    return;
                }
            }
            changed.await;
        }
    }

    async fn wait_materialized_locally(&self, target: u64) -> OverlayResult<()> {
        loop {
            let changed = self.changed.notified();
            {
                let state = self.state.lock().expect("volatile runtime poisoned");
                if state.terminal.is_some() {
                    return Err(OverlayError::IoError);
                }
                if state.materialized_through >= target {
                    return Ok(());
                }
            }
            changed.await;
        }
    }

    pub(crate) async fn read<F>(&self, offset: u64, length: usize, base: F) -> OverlayResult<Bytes>
    where
        F: FnOnce() -> BoxFuture<'static, OverlayResult<Bytes>>,
    {
        let _retirement = self.retirement.read().await;
        let first = self.snapshot();
        if let Some(data) = fully_covered(offset, length, &first) {
            return Ok(data);
        }

        let base = base().await?;
        if base.len() != length {
            return Err(OverlayError::IoError);
        }
        // Terminal state stops admission and durability progress, but accepted
        // entries stay pinned. Overlaying that frozen logical view over the
        // canonical read preserves the last coherent bytes after a partial or
        // failed materialization instead of turning every read into EIO.
        // Snapshot after the canonical read while retirement remains pinned.
        // Any write that could have partially changed the base is therefore
        // still present here in its complete logical form.
        let overlay = self.snapshot();
        if overlay.is_empty() {
            // Nothing to apply: the canonical bytes are already the answer, so
            // skip copying them into a mutable buffer.
            return Ok(base);
        }

        let mut output = BytesMut::from(base.as_ref());
        for entry in overlay {
            apply_entry(&mut output, offset, &entry);
        }
        Ok(output.freeze())
    }

    pub(crate) fn stop_admission(&self) -> u64 {
        let mut state = self.state.lock().expect("volatile runtime poisoned");
        state.accepting = false;
        let cutoff = state.next_sequence;
        drop(state);
        self.changed.notify_waiters();
        self.budget.changed.notify_waiters();
        cutoff
    }

    #[allow(dead_code)]
    pub(crate) fn fence_abort(&self) {
        let sequence = self.accepted_cutoff().saturating_add(1);
        self.fail(sequence, OverlayError::IoError);
    }

    pub(crate) async fn shutdown(&self) -> OverlayResult<()> {
        self.shutdown_with_timeout(GRACEFUL_DRAIN_TIMEOUT).await
    }

    async fn shutdown_with_timeout(&self, timeout: Duration) -> OverlayResult<()> {
        let cutoff = self.stop_admission();
        // A terminal failure is service-wide for new requests and client
        // fences, but already-acknowledged writes in otherwise healthy
        // exports still deserve a bounded materialization attempt during
        // graceful shutdown.
        let drain_result =
            match tokio::time::timeout(timeout, self.wait_materialized_locally(cutoff)).await {
                Ok(result) => result,
                Err(_) => {
                    self.fail(cutoff, OverlayError::IoError);
                    Err(OverlayError::IoError)
                }
            };
        #[cfg(test)]
        let mut result = drain_result;
        #[cfg(not(test))]
        let result = drain_result;
        if result.is_err() {
            self.task_shutdown.cancel();
        }
        #[cfg(test)]
        if self.ingress.is_some() {
            self.task_shutdown.cancel();
            if tokio::time::timeout(timeout, self.wait_worker_stopped())
                .await
                .is_err()
            {
                self.fail(cutoff, OverlayError::IoError);
                result = Err(OverlayError::IoError);
            }
        }
        result
    }

    #[cfg(test)]
    pub(crate) async fn shutdown_with_timeout_for_test(
        &self,
        timeout: Duration,
    ) -> OverlayResult<()> {
        self.shutdown_with_timeout(timeout).await
    }

    pub(crate) fn shutdown_token(&self) -> CancellationToken {
        self.task_shutdown.clone()
    }

    #[cfg(test)]
    async fn wait_worker_stopped(&self) {
        loop {
            let stopped = self.worker_stopped.notified();
            if !self.worker_running.load(Ordering::Acquire) {
                return;
            }
            stopped.await;
        }
    }

    #[cfg(test)]
    fn terminal(&self) -> Option<(u64, OverlayError)> {
        self.state
            .lock()
            .expect("volatile runtime poisoned")
            .terminal
    }

    pub(crate) fn is_reapable(&self) -> bool {
        let state = self.state.lock().expect("volatile runtime poisoned");
        state.terminal.is_none()
            && state.entries.is_empty()
            && state.completed.is_empty()
            && state.materialized_through == state.next_sequence
    }

    pub(crate) fn fail(&self, sequence: u64, error: OverlayError) {
        let mut state = self.state.lock().expect("volatile runtime poisoned");
        state.accepting = false;
        state.terminal.get_or_insert((sequence, error));
        drop(state);
        self.budget.poison();
        self.changed.notify_waiters();
        self.budget.changed.notify_waiters();
    }

    pub(crate) async fn complete(&self, sequence: u64) {
        let retirement = self.retirement.write().await;
        let mut released_operations = 0_u64;
        let mut released_bytes = 0_u64;
        {
            let mut state = self.state.lock().expect("volatile runtime poisoned");
            if state.terminal.is_some() {
                return;
            }
            state.completed.insert(sequence);
            loop {
                let next = state.materialized_through + 1;
                if !state.completed.remove(&next) {
                    break;
                }
                state.materialized_through += 1;
                let retired = state.materialized_through;
                if let Some(entry) = state.entries.remove(&retired)
                    && entry.visibility.is_published()
                {
                    released_operations += 1;
                    released_bytes += entry.data.len() as u64;
                }
            }
        }
        self.changed.notify_waiters();
        if released_operations > 0 {
            metrics::counter!("zerofs_nbd_volatile_writes_materialized_total")
                .increment(released_operations);
            metrics::counter!("zerofs_nbd_volatile_bytes_materialized_total")
                .increment(released_bytes);
            self.budget.changed.notify_waiters();
        }
        drop(retirement);
        #[cfg(test)]
        self.pause_before_idle_retirement_for_test().await;
        if let Some(idle_hook) = &self.idle_hook {
            idle_hook(self.inode, self.generation);
        }
    }

    fn snapshot(&self) -> Vec<Arc<OverlayEntry>> {
        self.state
            .lock()
            .expect("volatile runtime poisoned")
            .entries
            .values()
            .filter(|entry| entry.visibility.is_published())
            .cloned()
            .collect()
    }
}

pub(crate) fn record_published_staged_writes(bytes: u64, operations: u64) {
    metrics::counter!("zerofs_nbd_volatile_writes_accepted_total").increment(operations);
    metrics::counter!("zerofs_nbd_volatile_bytes_accepted_total").increment(bytes);
}

fn overlap(
    request_offset: u64,
    request_len: usize,
    entry: &OverlayEntry,
) -> Option<(usize, usize, usize)> {
    let request_end = request_offset.checked_add(request_len as u64)?;
    let entry_end = entry.offset.checked_add(entry.data.len() as u64)?;
    let start = request_offset.max(entry.offset);
    let end = request_end.min(entry_end);
    (start < end).then(|| {
        (
            (start - request_offset) as usize,
            (start - entry.offset) as usize,
            (end - start) as usize,
        )
    })
}

fn apply_entry(output: &mut BytesMut, request_offset: u64, entry: &OverlayEntry) {
    if let Some((destination, source, length)) = overlap(request_offset, output.len(), entry) {
        output[destination..destination + length]
            .copy_from_slice(&entry.data[source..source + length]);
    }
}

fn fully_covered(
    request_offset: u64,
    request_len: usize,
    entries: &[Arc<OverlayEntry>],
) -> Option<Bytes> {
    if entries.is_empty() {
        return None;
    }
    let mut output = BytesMut::zeroed(request_len);
    let mut uncovered = vec![(0usize, request_len)];
    for entry in entries.iter().rev() {
        let Some((destination, source, length)) = overlap(request_offset, request_len, entry)
        else {
            continue;
        };
        let covered_start = destination;
        let covered_end = destination + length;
        let mut next = Vec::with_capacity(uncovered.len() + 1);
        for (start, end) in uncovered {
            let copy_start = start.max(covered_start);
            let copy_end = end.min(covered_end);
            if copy_start < copy_end {
                let entry_start = source + (copy_start - covered_start);
                output[copy_start..copy_end]
                    .copy_from_slice(&entry.data[entry_start..entry_start + copy_end - copy_start]);
                if start < copy_start {
                    next.push((start, copy_start));
                }
                if copy_end < end {
                    next.push((copy_end, end));
                }
            } else {
                next.push((start, end));
            }
        }
        uncovered = next;
        if uncovered.is_empty() {
            return Some(output.freeze());
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::{Materializer, VolatileBudget, VolatileWriteRuntime};
    use bytes::Bytes;
    use futures::FutureExt;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;
    use tokio::sync::{Notify, mpsc};

    #[tokio::test]
    async fn acknowledges_and_reads_before_materialization() {
        let release = Arc::new(Notify::new());
        let entered = Arc::new(Notify::new());
        let materialize: Materializer = {
            let release = Arc::clone(&release);
            let entered = Arc::clone(&entered);
            Arc::new(move |_, _, _| {
                let release = Arc::clone(&release);
                let entered = Arc::clone(&entered);
                async move {
                    entered.notify_one();
                    release.notified().await;
                    Ok(())
                }
                .boxed()
            })
        };
        let budget = VolatileBudget::new(4096, 16);
        let runtime = VolatileWriteRuntime::new(Arc::clone(&budget), 7, materialize);
        let admission = runtime.reserve(4).await.unwrap();
        runtime
            .accept_write(admission, 0, Bytes::from_static(b"fast"))
            .await
            .unwrap();
        entered.notified().await;

        let read = runtime
            .read(0, 4, || {
                async { panic!("fully covered read touched base") }.boxed()
            })
            .await
            .unwrap();
        assert_eq!(read, Bytes::from_static(b"fast"));
        assert_eq!(budget.used_bytes(), 4);

        release.notify_one();
        runtime.wait_materialized(1).await.unwrap();
        assert_eq!(budget.used_bytes(), 0);
    }

    #[tokio::test]
    async fn newest_overlapping_write_wins_while_older_write_is_pending() {
        let release = Arc::new(Notify::new());
        let materialize: Materializer = {
            let release = Arc::clone(&release);
            Arc::new(move |_, _, _| {
                let release = Arc::clone(&release);
                async move {
                    release.notified().await;
                    Ok(())
                }
                .boxed()
            })
        };
        let runtime = VolatileWriteRuntime::new(VolatileBudget::new(4096, 16), 7, materialize);
        let first = runtime.reserve(4).await.unwrap();
        runtime
            .accept_write(first, 0, Bytes::from_static(b"aaaa"))
            .await
            .unwrap();
        let second = runtime.reserve(2).await.unwrap();
        runtime
            .accept_write(second, 1, Bytes::from_static(b"BB"))
            .await
            .unwrap();

        let read = runtime
            .read(0, 4, || {
                async { panic!("fully covered read touched base") }.boxed()
            })
            .await
            .unwrap();
        assert_eq!(read, Bytes::from_static(b"aBBa"));
        release.notify_waiters();
    }

    #[tokio::test]
    async fn partial_read_merges_the_pinned_overlay_over_base() {
        let release = Arc::new(Notify::new());
        let materialize: Materializer = {
            let release = Arc::clone(&release);
            Arc::new(move |_, _, _| {
                let release = Arc::clone(&release);
                async move {
                    release.notified().await;
                    Ok(())
                }
                .boxed()
            })
        };
        let runtime = VolatileWriteRuntime::new(VolatileBudget::new(4096, 16), 7, materialize);
        let admission = runtime.reserve(2).await.unwrap();
        runtime
            .accept_write(admission, 1, Bytes::from_static(b"XX"))
            .await
            .unwrap();

        let read = runtime
            .read(0, 4, || async { Ok(Bytes::from_static(b"base")) }.boxed())
            .await
            .unwrap();
        assert_eq!(read, Bytes::from_static(b"bXXe"));
        release.notify_waiters();
    }

    #[tokio::test]
    async fn budget_waiter_is_rejected_when_admission_stops() {
        let release = Arc::new(Notify::new());
        let materialize: Materializer = {
            let release = Arc::clone(&release);
            Arc::new(move |_, _, _| {
                let release = Arc::clone(&release);
                async move {
                    release.notified().await;
                    Ok(())
                }
                .boxed()
            })
        };
        let runtime = VolatileWriteRuntime::new(VolatileBudget::new(4, 16), 7, materialize);
        let admission = runtime.reserve(4).await.unwrap();
        runtime
            .accept_write(admission, 0, Bytes::from_static(b"full"))
            .await
            .unwrap();

        let waiting_runtime = Arc::clone(&runtime);
        let waiter = tokio::spawn(async move { waiting_runtime.reserve(1).await });
        tokio::task::yield_now().await;
        runtime.stop_admission();

        let result = tokio::time::timeout(Duration::from_millis(250), waiter)
            .await
            .expect("stopped admission must wake the budget waiter")
            .expect("budget waiter task");
        assert!(matches!(result, Err(super::OverlayError::IoError)));
        release.notify_waiters();
    }

    #[tokio::test]
    async fn malformed_write_geometry_is_rejected_before_acknowledgement() {
        let materialize: Materializer = Arc::new(|_, _, _| async { Ok(()) }.boxed());
        let budget = VolatileBudget::new(4096, 16);
        let runtime = VolatileWriteRuntime::new(Arc::clone(&budget), 7, materialize);

        let empty = runtime.reserve(0).await.unwrap();
        let result = runtime.accept_write(empty, 0, Bytes::new()).await;
        assert!(matches!(result, Err(super::OverlayError::InvalidArgument)));
        assert_eq!(runtime.accepted_cutoff(), 0);
        assert_eq!(budget.used_bytes(), 0);

        let overflowing = runtime.reserve(4).await.unwrap();
        let result = runtime
            .accept_write(overflowing, u64::MAX - 1, Bytes::from_static(b"tiny"))
            .await;
        assert!(matches!(result, Err(super::OverlayError::InvalidArgument)));
        assert_eq!(runtime.accepted_cutoff(), 0);
        assert_eq!(budget.used_bytes(), 0);

        let mismatched = runtime.reserve(8).await.unwrap();
        let result = runtime
            .accept_write(mismatched, 0, Bytes::from_static(b"tiny"))
            .await;
        assert!(matches!(result, Err(super::OverlayError::InvalidArgument)));
        assert_eq!(runtime.accepted_cutoff(), 0);
        assert_eq!(budget.used_bytes(), 0);
    }

    #[tokio::test]
    async fn failure_in_one_runtime_wakes_another_export_budget_waiter() {
        let entered = Arc::new(Notify::new());
        let fail = Arc::new(Notify::new());
        let failing_materializer: Materializer = {
            let entered = Arc::clone(&entered);
            let fail = Arc::clone(&fail);
            Arc::new(move |_, _, _| {
                let entered = Arc::clone(&entered);
                let fail = Arc::clone(&fail);
                async move {
                    entered.notify_one();
                    fail.notified().await;
                    Err(super::OverlayError::IoError)
                }
                .boxed()
            })
        };
        let budget = VolatileBudget::new(4, 16);
        let first = VolatileWriteRuntime::new(Arc::clone(&budget), 7, failing_materializer);
        let second = VolatileWriteRuntime::new(
            Arc::clone(&budget),
            8,
            Arc::new(|_, _, _| async { Ok(()) }.boxed()),
        );
        let admission = first.reserve(4).await.unwrap();
        first
            .accept_write(admission, 0, Bytes::from_static(b"full"))
            .await
            .unwrap();
        entered.notified().await;

        let waiter = tokio::spawn(async move { second.reserve(1).await });
        tokio::task::yield_now().await;
        fail.notify_one();

        let result = tokio::time::timeout(Duration::from_millis(250), waiter)
            .await
            .expect("shared terminal budget must wake every export waiter")
            .expect("budget waiter task");
        assert!(matches!(result, Err(super::OverlayError::IoError)));
    }

    #[tokio::test]
    async fn failure_in_one_runtime_wakes_another_export_flush_waiter() {
        let first_entered = Arc::new(Notify::new());
        let fail_first = Arc::new(Notify::new());
        let failing_materializer: Materializer = {
            let first_entered = Arc::clone(&first_entered);
            let fail_first = Arc::clone(&fail_first);
            Arc::new(move |_, _, _| {
                let first_entered = Arc::clone(&first_entered);
                let fail_first = Arc::clone(&fail_first);
                async move {
                    first_entered.notify_one();
                    fail_first.notified().await;
                    Err(super::OverlayError::IoError)
                }
                .boxed()
            })
        };
        let hold_second = Arc::new(Notify::new());
        let second_entered = Arc::new(Notify::new());
        let blocked_materializer: Materializer = {
            let hold_second = Arc::clone(&hold_second);
            let second_entered = Arc::clone(&second_entered);
            Arc::new(move |_, _, _| {
                let hold_second = Arc::clone(&hold_second);
                let second_entered = Arc::clone(&second_entered);
                async move {
                    second_entered.notify_one();
                    hold_second.notified().await;
                    Ok(())
                }
                .boxed()
            })
        };
        let budget = VolatileBudget::new(16, 16);
        let first = VolatileWriteRuntime::new(Arc::clone(&budget), 7, failing_materializer);
        let second = VolatileWriteRuntime::new(Arc::clone(&budget), 8, blocked_materializer);
        let first_admission = first.reserve(4).await.unwrap();
        first
            .accept_write(first_admission, 0, Bytes::from_static(b"fail"))
            .await
            .unwrap();
        let second_admission = second.reserve(4).await.unwrap();
        second
            .accept_write(second_admission, 0, Bytes::from_static(b"wait"))
            .await
            .unwrap();
        first_entered.notified().await;
        second_entered.notified().await;

        let waiting_runtime = Arc::clone(&second);
        let waiter = tokio::spawn(async move { waiting_runtime.wait_materialized(1).await });
        tokio::task::yield_now().await;
        fail_first.notify_one();

        let result = tokio::time::timeout(Duration::from_millis(250), waiter)
            .await
            .expect("shared terminal failure must wake every export flush waiter")
            .expect("flush waiter task");
        assert!(matches!(result, Err(super::OverlayError::IoError)));

        hold_second.notify_waiters();
        let _ = first
            .shutdown_with_timeout(Duration::from_millis(250))
            .await;
        let _ = second
            .shutdown_with_timeout(Duration::from_millis(250))
            .await;
    }

    #[tokio::test]
    async fn graceful_shutdown_drains_healthy_export_after_global_failure() {
        let fail_first = Arc::new(Notify::new());
        let failing_materializer: Materializer = {
            let fail_first = Arc::clone(&fail_first);
            Arc::new(move |_, _, _| {
                let fail_first = Arc::clone(&fail_first);
                async move {
                    fail_first.notified().await;
                    Err(super::OverlayError::IoError)
                }
                .boxed()
            })
        };
        let release_second = Arc::new(Notify::new());
        let second_entered = Arc::new(Notify::new());
        let healthy_materializer: Materializer = {
            let release_second = Arc::clone(&release_second);
            let second_entered = Arc::clone(&second_entered);
            Arc::new(move |_, _, _| {
                let release_second = Arc::clone(&release_second);
                let second_entered = Arc::clone(&second_entered);
                async move {
                    second_entered.notify_one();
                    release_second.notified().await;
                    Ok(())
                }
                .boxed()
            })
        };
        let budget = VolatileBudget::new(16, 16);
        let first = VolatileWriteRuntime::new(Arc::clone(&budget), 7, failing_materializer);
        let second = VolatileWriteRuntime::new(Arc::clone(&budget), 8, healthy_materializer);
        let first_admission = first.reserve(4).await.unwrap();
        first
            .accept_write(first_admission, 0, Bytes::from_static(b"fail"))
            .await
            .unwrap();
        let second_admission = second.reserve(4).await.unwrap();
        second
            .accept_write(second_admission, 0, Bytes::from_static(b"keep"))
            .await
            .unwrap();
        second_entered.notified().await;
        fail_first.notify_one();
        tokio::time::timeout(Duration::from_millis(250), first.wait_materialized(1))
            .await
            .expect("first export failure became visible")
            .unwrap_err();

        let shutdown_runtime = Arc::clone(&second);
        let mut shutdown = tokio::spawn(async move {
            shutdown_runtime
                .shutdown_with_timeout(Duration::from_millis(250))
                .await
        });
        assert!(
            tokio::time::timeout(Duration::from_millis(25), &mut shutdown)
                .await
                .is_err(),
            "healthy shutdown must keep draining its acknowledged local cutoff"
        );
        release_second.notify_waiters();
        tokio::time::timeout(Duration::from_millis(250), shutdown)
            .await
            .expect("healthy shutdown completed after local materialization")
            .expect("healthy shutdown task")
            .expect("healthy export must drain despite another export's failure");
        assert_eq!(
            budget.used_bytes(),
            4,
            "only the failed export remains dirty"
        );

        let _ = first
            .shutdown_with_timeout(Duration::from_millis(250))
            .await;
    }

    #[tokio::test]
    async fn blocked_partial_read_does_not_block_new_write_acknowledgement() {
        let materialize_release = Arc::new(Notify::new());
        let materialize: Materializer = {
            let materialize_release = Arc::clone(&materialize_release);
            Arc::new(move |_, _, _| {
                let materialize_release = Arc::clone(&materialize_release);
                async move {
                    materialize_release.notified().await;
                    Ok(())
                }
                .boxed()
            })
        };
        let runtime = VolatileWriteRuntime::new(VolatileBudget::new(4096, 16), 7, materialize);
        let base_entered = Arc::new(Notify::new());
        let base_release = Arc::new(Notify::new());
        let reader_runtime = Arc::clone(&runtime);
        let reader = tokio::spawn({
            let base_entered = Arc::clone(&base_entered);
            let base_release = Arc::clone(&base_release);
            async move {
                reader_runtime
                    .read(0, 4, move || {
                        async move {
                            base_entered.notify_one();
                            base_release.notified().await;
                            Ok(Bytes::from_static(b"base"))
                        }
                        .boxed()
                    })
                    .await
            }
        });
        base_entered.notified().await;

        let admission = runtime.reserve(2).await.unwrap();
        tokio::time::timeout(
            Duration::from_millis(250),
            runtime.accept_write(admission, 1, Bytes::from_static(b"XX")),
        )
        .await
        .expect("slow base read must not delay volatile acknowledgement")
        .unwrap();

        base_release.notify_one();
        assert_eq!(reader.await.unwrap().unwrap(), Bytes::from_static(b"bXXe"));
        materialize_release.notify_waiters();
    }

    #[tokio::test]
    async fn materializer_panic_poisoned_runtime_instead_of_hanging_flush() {
        let materialize: Materializer = Arc::new(|_, _, _| {
            async move {
                panic!("injected volatile materializer panic");
                #[allow(unreachable_code)]
                Ok(())
            }
            .boxed()
        });
        let runtime = VolatileWriteRuntime::new(VolatileBudget::new(4096, 16), 7, materialize);
        let admission = runtime.reserve(4).await.unwrap();
        runtime
            .accept_write(admission, 0, Bytes::from_static(b"boom"))
            .await
            .unwrap();

        let result = tokio::time::timeout(Duration::from_millis(250), runtime.wait_materialized(1))
            .await
            .expect("worker panic must wake the durability fence");
        assert!(matches!(result, Err(super::OverlayError::IoError)));
        assert_eq!(runtime.accepted_cutoff(), 1);
    }

    #[tokio::test]
    async fn fencing_cancels_and_joins_a_blocked_materializer() {
        let entered = Arc::new(Notify::new());
        let materialize: Materializer = {
            let entered = Arc::clone(&entered);
            Arc::new(move |_, _, _| {
                let entered = Arc::clone(&entered);
                async move {
                    entered.notify_one();
                    std::future::pending::<()>().await;
                    Ok(())
                }
                .boxed()
            })
        };
        let runtime = VolatileWriteRuntime::new(VolatileBudget::new(4096, 16), 7, materialize);
        let admission = runtime.reserve(4).await.unwrap();
        runtime
            .accept_write(admission, 0, Bytes::from_static(b"data"))
            .await
            .unwrap();
        entered.notified().await;
        runtime.fence_abort();

        let result = tokio::time::timeout(Duration::from_millis(250), runtime.shutdown())
            .await
            .expect("fencing must cancel and join a blocked materializer");
        assert!(matches!(result, Err(super::OverlayError::IoError)));
    }

    #[tokio::test]
    async fn graceful_drain_deadline_aborts_and_joins_a_blocked_materializer() {
        let entered = Arc::new(Notify::new());
        let materialize: Materializer = {
            let entered = Arc::clone(&entered);
            Arc::new(move |_, _, _| {
                let entered = Arc::clone(&entered);
                async move {
                    entered.notify_one();
                    std::future::pending::<()>().await;
                    Ok(())
                }
                .boxed()
            })
        };
        let runtime = VolatileWriteRuntime::new(VolatileBudget::new(4096, 16), 7, materialize);
        let admission = runtime.reserve(4).await.unwrap();
        runtime
            .accept_write(admission, 0, Bytes::from_static(b"data"))
            .await
            .unwrap();
        entered.notified().await;

        let result = runtime
            .shutdown_with_timeout(Duration::from_millis(25))
            .await;
        assert!(matches!(result, Err(super::OverlayError::IoError)));
    }

    #[tokio::test]
    async fn one_export_failure_prevents_another_export_from_acknowledging_reserved_bytes() {
        let fail = Arc::new(Notify::new());
        let failing_materializer: Materializer = {
            let fail = Arc::clone(&fail);
            Arc::new(move |_, _, _| {
                let fail = Arc::clone(&fail);
                async move {
                    fail.notified().await;
                    Err(super::OverlayError::IoError)
                }
                .boxed()
            })
        };
        let budget = VolatileBudget::new(16, 16);
        let first = VolatileWriteRuntime::new(Arc::clone(&budget), 7, failing_materializer);
        let second = VolatileWriteRuntime::new(
            Arc::clone(&budget),
            8,
            Arc::new(|_, _, _| async { Ok(()) }.boxed()),
        );
        let first_admission = first.reserve(4).await.unwrap();
        first
            .accept_write(first_admission, 0, Bytes::from_static(b"fail"))
            .await
            .unwrap();
        let reserved_before_failure = second.reserve(4).await.unwrap();
        fail.notify_one();
        tokio::time::timeout(Duration::from_millis(250), first.wait_materialized(1))
            .await
            .expect("failure became visible")
            .unwrap_err();

        let result = second
            .accept_write(reserved_before_failure, 0, Bytes::from_static(b"late"))
            .await;
        assert!(matches!(result, Err(super::OverlayError::IoError)));
        assert!(matches!(
            second.wait_materialized(0).await,
            Err(super::OverlayError::IoError)
        ));
    }

    #[tokio::test]
    async fn terminal_materialization_failure_retains_acknowledged_read_view() {
        let materialize: Materializer =
            Arc::new(|_, _, _| async { Err(super::OverlayError::IoError) }.boxed());
        let runtime = VolatileWriteRuntime::new(VolatileBudget::new(4096, 16), 7, materialize);
        let admission = runtime.reserve(4).await.unwrap();
        runtime
            .accept_write(admission, 0, Bytes::from_static(b"data"))
            .await
            .unwrap();
        runtime.wait_materialized(1).await.unwrap_err();

        let result = runtime
            .read(0, 4, || async { Ok(Bytes::from_static(b"base")) }.boxed())
            .await
            .unwrap();
        assert_eq!(result, Bytes::from_static(b"data"));
    }

    #[tokio::test]
    async fn terminal_partial_read_merges_frozen_overlay_over_canonical_base() {
        let materialize: Materializer =
            Arc::new(|_, _, _| async { Err(super::OverlayError::IoError) }.boxed());
        let runtime = VolatileWriteRuntime::new(VolatileBudget::new(4096, 16), 7, materialize);
        let admission = runtime.reserve(2).await.unwrap();
        runtime
            .accept_write(admission, 1, Bytes::from_static(b"XX"))
            .await
            .unwrap();
        runtime.wait_materialized(1).await.unwrap_err();

        let result = runtime
            .read(0, 4, || async { Ok(Bytes::from_static(b"base")) }.boxed())
            .await
            .unwrap();
        assert_eq!(result, Bytes::from_static(b"bXXe"));
    }

    #[tokio::test]
    async fn materialized_cutoff_does_not_wait_for_a_later_sequence() {
        let second_entered = Arc::new(Notify::new());
        let release_second = Arc::new(Notify::new());
        let materialize: Materializer = {
            let second_entered = Arc::clone(&second_entered);
            let release_second = Arc::clone(&release_second);
            Arc::new(move |_, offset, _| {
                let second_entered = Arc::clone(&second_entered);
                let release_second = Arc::clone(&release_second);
                async move {
                    if offset == 4 {
                        second_entered.notify_one();
                        release_second.notified().await;
                    }
                    Ok(())
                }
                .boxed()
            })
        };
        let runtime = VolatileWriteRuntime::new(VolatileBudget::new(4096, 16), 7, materialize);
        for offset in [0, 4] {
            let admission = runtime.reserve(4).await.unwrap();
            runtime
                .accept_write(admission, offset, Bytes::from_static(b"data"))
                .await
                .unwrap();
        }
        second_entered.notified().await;

        tokio::time::timeout(Duration::from_millis(100), runtime.wait_materialized(1))
            .await
            .expect("the earlier cutoff must not wait for a later blocked write")
            .unwrap();
        release_second.notify_one();
        runtime.wait_materialized(2).await.unwrap();
        runtime.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn writes_materialize_in_acceptance_order() {
        let release_first = Arc::new(Notify::new());
        let call_count = Arc::new(AtomicUsize::new(0));
        let (entered_tx, mut entered_rx) = mpsc::unbounded_channel();
        let materialize: Materializer = {
            let release_first = Arc::clone(&release_first);
            let call_count = Arc::clone(&call_count);
            Arc::new(move |_, offset, _| {
                let release_first = Arc::clone(&release_first);
                let call_count = Arc::clone(&call_count);
                let entered_tx = entered_tx.clone();
                async move {
                    let call = call_count.fetch_add(1, Ordering::AcqRel);
                    entered_tx.send(offset).unwrap();
                    if call == 0 {
                        release_first.notified().await;
                    }
                    Ok(())
                }
                .boxed()
            })
        };
        let runtime = VolatileWriteRuntime::new(VolatileBudget::new(4096, 16), 7, materialize);

        for (offset, data) in [(0, b"first".as_slice()), (5, b"second".as_slice())] {
            let admission = runtime.reserve(data.len()).await.unwrap();
            runtime
                .accept_write(admission, offset, Bytes::copy_from_slice(data))
                .await
                .unwrap();
        }

        assert_eq!(entered_rx.recv().await, Some(0));
        assert!(
            tokio::time::timeout(Duration::from_millis(100), entered_rx.recv())
                .await
                .is_err(),
            "the second write must not overtake a blocked write on the same lane"
        );
        release_first.notify_one();
        assert_eq!(entered_rx.recv().await, Some(5));
        runtime.wait_materialized(2).await.unwrap();
        runtime.shutdown().await.unwrap();
    }

    /// A striped logical write stages one member per lane runtime, so the two
    /// members must materialize concurrently rather than serially.
    #[tokio::test]
    async fn distinct_lanes_materialize_one_logical_write_in_parallel() {
        let release = Arc::new(Notify::new());
        let (entered_tx, mut entered_rx) = mpsc::unbounded_channel();
        let materialize: Materializer = {
            let release = Arc::clone(&release);
            Arc::new(move |inode, _, _| {
                let release = Arc::clone(&release);
                let entered_tx = entered_tx.clone();
                async move {
                    entered_tx.send(inode).unwrap();
                    release.notified().await;
                    Ok(())
                }
                .boxed()
            })
        };
        let budget = VolatileBudget::new(4096, 16);
        let first_lane =
            VolatileWriteRuntime::new(Arc::clone(&budget), 7, Arc::clone(&materialize));
        let second_lane = VolatileWriteRuntime::new(Arc::clone(&budget), 8, materialize);
        let visibility = super::WriteVisibility::staged();
        for lane in [&first_lane, &second_lane] {
            let admission = lane.reserve(2).await.unwrap();
            lane.accept_staged_write(
                admission,
                0,
                Bytes::from_static(b"da"),
                Arc::clone(&visibility),
            )
            .await
            .unwrap();
        }
        visibility.publish();

        let first = entered_rx.recv().await.unwrap();
        let second = tokio::time::timeout(Duration::from_millis(250), entered_rx.recv())
            .await
            .expect("the other lane must enter while the first remains blocked")
            .unwrap();
        assert_ne!(first, second);
        release.notify_waiters();
        first_lane.wait_materialized(1).await.unwrap();
        second_lane.wait_materialized(1).await.unwrap();
        first_lane.shutdown().await.unwrap();
        second_lane.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn budget_status_reports_dirty_ownership_and_terminal_state() {
        let release = Arc::new(Notify::new());
        let materialize: Materializer = {
            let release = Arc::clone(&release);
            Arc::new(move |_, _, _| {
                let release = Arc::clone(&release);
                async move {
                    release.notified().await;
                    Ok(())
                }
                .boxed()
            })
        };
        let budget = VolatileBudget::new(8, 2);
        let runtime = VolatileWriteRuntime::new(Arc::clone(&budget), 7, materialize);
        let admission = runtime.reserve(4).await.unwrap();
        runtime
            .accept_write(admission, 0, Bytes::from_static(b"data"))
            .await
            .unwrap();

        let status = budget.status();
        assert_eq!(status.capacity_bytes, 8);
        assert_eq!(status.dirty_bytes, 4);
        assert_eq!(status.dirty_operations, 1);
        assert!(!status.terminal);

        runtime.fence_abort();
        assert!(budget.status().terminal);
        release.notify_waiters();
    }

    #[tokio::test]
    async fn logical_reservation_releases_operation_after_its_last_member() {
        let budget = VolatileBudget::new(8, 1);
        let mut permits = budget.reserve_many_while(&[3, 5], || true).await.unwrap();

        assert_eq!(budget.status().dirty_bytes, 8);
        assert_eq!(budget.status().dirty_operations, 1);
        drop(permits.pop());
        assert_eq!(budget.status().dirty_bytes, 3);
        assert_eq!(budget.status().dirty_operations, 1);
        drop(permits);
        assert_eq!(budget.status().dirty_bytes, 0);
        assert_eq!(budget.status().dirty_operations, 0);
    }
}
