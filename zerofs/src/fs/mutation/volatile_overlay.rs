//! Shared volatile write acknowledgement overlay.
//!
//! WRITE owns the payload in a bounded process-wide RAM pool, publishes it to
//! the read overlay, and only then replies.  Per-member workers materialize the
//! accepted sequence through the ordinary filesystem path.  FLUSH/FUA wait for
//! the captured sequence before entering the existing filesystem durability
//! barrier.

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
use futures::{FutureExt, future::BoxFuture};
use std::collections::{BTreeMap, BTreeSet};
use std::panic::AssertUnwindSafe;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;
use tokio::sync::{Notify, RwLock, mpsc};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

pub(crate) type Materializer =
    Arc<dyn Fn(u64, u64, Bytes) -> BoxFuture<'static, OverlayResult<()>> + Send + Sync + 'static>;

const GRACEFUL_DRAIN_TIMEOUT: Duration = Duration::from_secs(60);

#[derive(Debug, Clone)]
pub(crate) struct WriteChunk {
    pub(crate) inode: u64,
    pub(crate) member_offset: u64,
    pub(crate) logical_offset: usize,
    pub(crate) length: usize,
}

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
    pub(crate) capacity_bytes: u64,
    pub(crate) dirty_bytes: u64,
    pub(crate) dirty_operations: usize,
    pub(crate) terminal: bool,
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

    async fn reserve(self: &Arc<Self>, bytes: u64) -> OverlayResult<BudgetPermit> {
        if bytes > self.max_bytes {
            return Err(OverlayError::NoSpace);
        }
        loop {
            let changed = self.changed.notified();
            {
                let mut state = self.state.lock().expect("volatile budget poisoned");
                if state.terminal {
                    return Err(OverlayError::IoError);
                }
                if state.used_bytes.saturating_add(bytes) <= self.max_bytes
                    && state.used_operations < self.max_operations
                {
                    state.used_bytes += bytes;
                    state.used_operations += 1;
                    let permit = BudgetPermit {
                        budget: Arc::clone(self),
                        bytes,
                    };
                    drop(state);
                    self.record_metrics();
                    return Ok(permit);
                }
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

    #[cfg(test)]
    fn used_bytes(&self) -> u64 {
        self.state
            .lock()
            .expect("volatile budget poisoned")
            .used_bytes
    }
}

pub(crate) struct BudgetPermit {
    budget: Arc<VolatileBudget>,
    bytes: u64,
}

impl Drop for BudgetPermit {
    fn drop(&mut self) {
        let mut state = self.budget.state.lock().expect("volatile budget poisoned");
        state.used_bytes -= self.bytes;
        state.used_operations -= 1;
        drop(state);
        self.budget.record_metrics();
        self.budget.changed.notify_waiters();
    }
}

pub(crate) struct VolatileAdmission {
    permit: BudgetPermit,
}

struct OverlayEntry {
    offset: u64,
    data: Bytes,
    _permit: BudgetPermit,
}

struct State {
    accepting: bool,
    next_sequence: u64,
    materialized_through: u64,
    completed: BTreeSet<u64>,
    entries: BTreeMap<u64, Arc<OverlayEntry>>,
    terminal: Option<(u64, OverlayError)>,
}

struct LogicalWrite {
    sequence: u64,
    entry: Arc<OverlayEntry>,
    groups: Vec<Vec<WriteChunk>>,
}

struct MemberWrite {
    sequence: u64,
    entry: Arc<OverlayEntry>,
    chunks: Vec<WriteChunk>,
    completion: Arc<LogicalCompletion>,
}

struct LogicalCompletion {
    sequence: u64,
    remaining: AtomicUsize,
    runtime: Weak<VolatileWriteRuntime>,
}

impl LogicalCompletion {
    async fn member_finished(&self) {
        if self.remaining.fetch_sub(1, Ordering::AcqRel) == 1
            && let Some(runtime) = self.runtime.upgrade()
        {
            runtime.complete(self.sequence).await;
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
    lane_inodes: Arc<[u64]>,
    ingress: mpsc::UnboundedSender<LogicalWrite>,
    task_shutdown: CancellationToken,
    task_handles: Mutex<Option<Vec<JoinHandle<()>>>>,
}

impl VolatileWriteRuntime {
    pub(crate) fn new(
        budget: Arc<VolatileBudget>,
        lane_inodes: Vec<u64>,
        materialize: Materializer,
    ) -> Arc<Self> {
        let lane_count = lane_inodes.len();
        assert!(
            lane_count > 0,
            "volatile runtime requires at least one lane"
        );
        let (ingress, mut ingress_rx) = mpsc::unbounded_channel::<LogicalWrite>();
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
            lane_inodes: lane_inodes.into(),
            ingress,
            task_shutdown: task_shutdown.clone(),
            task_handles: Mutex::new(Some(Vec::with_capacity(lane_count + 1))),
        });

        let mut lane_senders = Vec::with_capacity(lane_count);
        for _ in 0..lane_count {
            let (sender, mut receiver) = mpsc::unbounded_channel::<MemberWrite>();
            lane_senders.push(sender);
            let worker_runtime_weak = Arc::downgrade(&runtime);
            let materialize = Arc::clone(&materialize);
            let worker_shutdown = task_shutdown.clone();
            let handle = tokio::spawn(async move {
                let worker_runtime = worker_runtime_weak.clone();
                let outcome = AssertUnwindSafe(async move {
                    loop {
                        let member = tokio::select! {
                            biased;
                            _ = worker_shutdown.cancelled() => return,
                            member = receiver.recv() => match member {
                                Some(member) => member,
                                None => return,
                            },
                        };
                        let Some(runtime) = worker_runtime_weak.upgrade() else {
                            return;
                        };
                        if runtime.terminal().is_some() {
                            return;
                        }
                        let mut result = Ok(());
                        for chunk in &member.chunks {
                            let start = chunk.logical_offset;
                            let data = member.entry.data.slice(start..start + chunk.length);
                            let materialization = AssertUnwindSafe(materialize(
                                chunk.inode,
                                chunk.member_offset,
                                data,
                            ))
                            .catch_unwind();
                            tokio::pin!(materialization);
                            let outcome = tokio::select! {
                                biased;
                                _ = worker_shutdown.cancelled() => return,
                                outcome = &mut materialization => outcome,
                            };
                            match outcome {
                                Ok(Ok(())) => {}
                                Ok(Err(error)) => {
                                    result = Err(error);
                                    break;
                                }
                                Err(_) => {
                                    result = Err(OverlayError::IoError);
                                    break;
                                }
                            }
                        }
                        match result {
                            Ok(()) => member.completion.member_finished().await,
                            Err(error) => {
                                runtime.fail(member.sequence, error);
                                return;
                            }
                        }
                    }
                })
                .catch_unwind()
                .await;
                if outcome.is_err()
                    && let Some(runtime) = worker_runtime.upgrade()
                {
                    runtime.fail(runtime.accepted_cutoff(), OverlayError::IoError);
                }
            });
            runtime
                .task_handles
                .lock()
                .expect("volatile task handles poisoned")
                .as_mut()
                .expect("volatile tasks not yet shut down")
                .push(handle);
        }

        let weak_runtime = Arc::downgrade(&runtime);
        let dispatcher_runtime = weak_runtime.clone();
        let dispatcher_shutdown = task_shutdown;
        let dispatcher = tokio::spawn(async move {
            let outcome = AssertUnwindSafe(async move {
                loop {
                    let write = tokio::select! {
                        biased;
                        _ = dispatcher_shutdown.cancelled() => return,
                        write = ingress_rx.recv() => match write {
                            Some(write) => write,
                            None => return,
                        },
                    };
                    let Some(runtime) = weak_runtime.upgrade() else {
                        return;
                    };
                    if runtime.terminal().is_some() {
                        continue;
                    }
                    let touched = write
                        .groups
                        .iter()
                        .filter(|group| !group.is_empty())
                        .count();
                    let completion = Arc::new(LogicalCompletion {
                        sequence: write.sequence,
                        remaining: AtomicUsize::new(touched),
                        runtime: Arc::downgrade(&runtime),
                    });
                    if touched == 0 {
                        runtime.complete(write.sequence).await;
                        continue;
                    }
                    for (lane, chunks) in write.groups.into_iter().enumerate() {
                        if chunks.is_empty() {
                            continue;
                        }
                        if lane_senders[lane]
                            .send(MemberWrite {
                                sequence: write.sequence,
                                entry: Arc::clone(&write.entry),
                                chunks,
                                completion: Arc::clone(&completion),
                            })
                            .is_err()
                        {
                            runtime.fail(write.sequence, OverlayError::IoError);
                            return;
                        }
                    }
                }
            })
            .catch_unwind()
            .await;
            if outcome.is_err()
                && let Some(runtime) = dispatcher_runtime.upgrade()
            {
                runtime.fail(runtime.accepted_cutoff(), OverlayError::IoError);
            }
        });
        runtime
            .task_handles
            .lock()
            .expect("volatile task handles poisoned")
            .as_mut()
            .expect("volatile tasks not yet shut down")
            .push(dispatcher);

        runtime
    }

    pub(crate) async fn reserve(&self, bytes: usize) -> OverlayResult<VolatileAdmission> {
        loop {
            let changed = self.changed.notified();
            if self.terminal().is_some() || !self.is_accepting() {
                return Err(OverlayError::IoError);
            }
            tokio::select! {
                permit = self.budget.reserve(bytes as u64) => {
                    let permit = permit?;
                    if self.terminal().is_some() || !self.is_accepting() {
                        drop(permit);
                        return Err(OverlayError::IoError);
                    }
                    return Ok(VolatileAdmission { permit });
                }
                _ = changed => {}
            }
        }
    }

    pub(crate) async fn accept_write(
        &self,
        admission: VolatileAdmission,
        offset: u64,
        data: Bytes,
        groups: Vec<Vec<WriteChunk>>,
    ) -> OverlayResult<u64> {
        if admission.permit.bytes != data.len() as u64 {
            return Err(OverlayError::InvalidArgument);
        }
        if !valid_write_groups(&data, &groups, &self.lane_inodes) {
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
            _permit: admission.permit,
        });
        state.next_sequence = sequence;
        state.entries.insert(sequence, Arc::clone(&entry));
        if self
            .ingress
            .send(LogicalWrite {
                sequence,
                entry,
                groups,
            })
            .is_err()
        {
            state.terminal = Some((sequence, OverlayError::IoError));
            drop(state);
            self.changed.notify_waiters();
            self.budget.changed.notify_waiters();
            return Err(OverlayError::IoError);
        }
        drop(state);
        if self.budget.is_terminal() {
            self.fail(sequence, OverlayError::IoError);
            return Err(OverlayError::IoError);
        }
        self.changed.notify_waiters();
        metrics::counter!("zerofs_nbd_volatile_writes_accepted_total").increment(1);
        metrics::counter!("zerofs_nbd_volatile_bytes_accepted_total").increment(accepted_bytes);
        Ok(sequence)
    }

    pub(crate) fn accepted_cutoff(&self) -> u64 {
        self.state
            .lock()
            .expect("volatile runtime poisoned")
            .next_sequence
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
        if self.terminal().is_some() || self.budget.is_terminal() {
            return Err(OverlayError::IoError);
        }
        let _retirement = self.retirement.read().await;
        let first = self.snapshot();
        if let Some(data) = fully_covered(offset, length, &first) {
            return Ok(data);
        }

        let mut output = BytesMut::from(base().await?.as_ref());
        if self.terminal().is_some() || self.budget.is_terminal() {
            return Err(OverlayError::IoError);
        }
        if output.len() != length {
            return Err(OverlayError::IoError);
        }
        // Snapshot after the canonical read while retirement remains pinned.
        // Any write that could have partially changed the base is therefore
        // still present here in its complete logical form.
        for entry in self.snapshot() {
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
        let mut result =
            match tokio::time::timeout(timeout, self.wait_materialized_locally(cutoff)).await {
                Ok(result) => result,
                Err(_) => {
                    self.fail(cutoff, OverlayError::IoError);
                    Err(OverlayError::IoError)
                }
            };
        self.task_shutdown.cancel();
        let handles = self
            .task_handles
            .lock()
            .expect("volatile task handles poisoned")
            .take()
            .unwrap_or_default();
        for handle in handles {
            if handle.await.is_err() {
                self.fail(cutoff, OverlayError::IoError);
                result = Err(OverlayError::IoError);
            }
        }
        result
    }

    fn is_accepting(&self) -> bool {
        self.state
            .lock()
            .expect("volatile runtime poisoned")
            .accepting
    }

    fn terminal(&self) -> Option<(u64, OverlayError)> {
        self.state
            .lock()
            .expect("volatile runtime poisoned")
            .terminal
    }

    fn fail(&self, sequence: u64, error: OverlayError) {
        let mut state = self.state.lock().expect("volatile runtime poisoned");
        state.accepting = false;
        state.terminal.get_or_insert((sequence, error));
        drop(state);
        self.budget.poison();
        self.changed.notify_waiters();
        self.budget.changed.notify_waiters();
    }

    async fn complete(&self, sequence: u64) {
        let _retirement = self.retirement.write().await;
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
                if let Some(entry) = state.entries.remove(&retired) {
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
    }

    fn snapshot(&self) -> Vec<Arc<OverlayEntry>> {
        self.state
            .lock()
            .expect("volatile runtime poisoned")
            .entries
            .values()
            .cloned()
            .collect()
    }
}

fn valid_write_groups(data: &Bytes, groups: &[Vec<WriteChunk>], lane_inodes: &[u64]) -> bool {
    if data.is_empty() || groups.len() != lane_inodes.len() {
        return false;
    }
    let mut spans = Vec::new();
    for (lane, chunks) in groups.iter().enumerate() {
        for chunk in chunks {
            if chunk.inode != lane_inodes[lane] || chunk.length == 0 {
                return false;
            }
            let Some(logical_end) = chunk.logical_offset.checked_add(chunk.length) else {
                return false;
            };
            if logical_end > data.len()
                || chunk
                    .member_offset
                    .checked_add(chunk.length as u64)
                    .is_none()
            {
                return false;
            }
            spans.push((chunk.logical_offset, logical_end));
        }
    }
    spans.sort_unstable();
    let mut covered_through = 0;
    for (start, end) in spans {
        if start != covered_through {
            return false;
        }
        covered_through = end;
    }
    covered_through == data.len()
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
    use super::{Materializer, VolatileBudget, VolatileWriteRuntime, WriteChunk};
    use bytes::Bytes;
    use futures::FutureExt;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;
    use tokio::sync::{Notify, mpsc};

    fn one_chunk(length: usize) -> Vec<Vec<WriteChunk>> {
        vec![vec![WriteChunk {
            inode: 7,
            member_offset: 0,
            logical_offset: 0,
            length,
        }]]
    }

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
        let runtime = VolatileWriteRuntime::new(Arc::clone(&budget), vec![7], materialize);
        let admission = runtime.reserve(4).await.unwrap();
        runtime
            .accept_write(admission, 0, Bytes::from_static(b"fast"), one_chunk(4))
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
        let runtime =
            VolatileWriteRuntime::new(VolatileBudget::new(4096, 16), vec![7], materialize);
        let first = runtime.reserve(4).await.unwrap();
        runtime
            .accept_write(first, 0, Bytes::from_static(b"aaaa"), one_chunk(4))
            .await
            .unwrap();
        let second = runtime.reserve(2).await.unwrap();
        runtime
            .accept_write(
                second,
                1,
                Bytes::from_static(b"BB"),
                vec![vec![WriteChunk {
                    inode: 7,
                    member_offset: 1,
                    logical_offset: 0,
                    length: 2,
                }]],
            )
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
        let runtime =
            VolatileWriteRuntime::new(VolatileBudget::new(4096, 16), vec![7], materialize);
        let admission = runtime.reserve(2).await.unwrap();
        runtime
            .accept_write(
                admission,
                1,
                Bytes::from_static(b"XX"),
                vec![vec![WriteChunk {
                    inode: 7,
                    member_offset: 1,
                    logical_offset: 0,
                    length: 2,
                }]],
            )
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
        let runtime = VolatileWriteRuntime::new(VolatileBudget::new(4, 16), vec![7], materialize);
        let admission = runtime.reserve(4).await.unwrap();
        runtime
            .accept_write(admission, 0, Bytes::from_static(b"full"), one_chunk(4))
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
    async fn malformed_chunk_geometry_is_rejected_before_acknowledgement() {
        let materialize: Materializer = Arc::new(|_, _, _| async { Ok(()) }.boxed());
        let budget = VolatileBudget::new(4096, 16);
        let runtime = VolatileWriteRuntime::new(Arc::clone(&budget), vec![7], materialize);
        let admission = runtime.reserve(4).await.unwrap();
        let result = runtime
            .accept_write(
                admission,
                0,
                Bytes::from_static(b"tiny"),
                vec![vec![WriteChunk {
                    inode: 7,
                    member_offset: 0,
                    logical_offset: 3,
                    length: 2,
                }]],
            )
            .await;

        assert!(matches!(result, Err(super::OverlayError::InvalidArgument)));
        assert_eq!(runtime.accepted_cutoff(), 0);
        assert_eq!(budget.used_bytes(), 0);
    }

    #[tokio::test]
    async fn cross_lane_chunk_is_rejected_before_acknowledgement() {
        let materialize: Materializer = Arc::new(|_, _, _| async { Ok(()) }.boxed());
        let budget = VolatileBudget::new(4096, 16);
        let runtime = VolatileWriteRuntime::new(Arc::clone(&budget), vec![7], materialize);
        let admission = runtime.reserve(4).await.unwrap();
        let result = runtime
            .accept_write(
                admission,
                0,
                Bytes::from_static(b"tiny"),
                vec![vec![WriteChunk {
                    inode: 8,
                    member_offset: 0,
                    logical_offset: 0,
                    length: 4,
                }]],
            )
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
        let first = VolatileWriteRuntime::new(Arc::clone(&budget), vec![7], failing_materializer);
        let second = VolatileWriteRuntime::new(
            Arc::clone(&budget),
            vec![8],
            Arc::new(|_, _, _| async { Ok(()) }.boxed()),
        );
        let admission = first.reserve(4).await.unwrap();
        first
            .accept_write(admission, 0, Bytes::from_static(b"full"), one_chunk(4))
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
        let first = VolatileWriteRuntime::new(Arc::clone(&budget), vec![7], failing_materializer);
        let second = VolatileWriteRuntime::new(Arc::clone(&budget), vec![8], blocked_materializer);
        let first_admission = first.reserve(4).await.unwrap();
        first
            .accept_write(
                first_admission,
                0,
                Bytes::from_static(b"fail"),
                one_chunk(4),
            )
            .await
            .unwrap();
        let second_admission = second.reserve(4).await.unwrap();
        second
            .accept_write(
                second_admission,
                0,
                Bytes::from_static(b"wait"),
                vec![vec![WriteChunk {
                    inode: 8,
                    member_offset: 0,
                    logical_offset: 0,
                    length: 4,
                }]],
            )
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
        let first = VolatileWriteRuntime::new(Arc::clone(&budget), vec![7], failing_materializer);
        let second = VolatileWriteRuntime::new(Arc::clone(&budget), vec![8], healthy_materializer);
        let first_admission = first.reserve(4).await.unwrap();
        first
            .accept_write(
                first_admission,
                0,
                Bytes::from_static(b"fail"),
                one_chunk(4),
            )
            .await
            .unwrap();
        let second_admission = second.reserve(4).await.unwrap();
        second
            .accept_write(
                second_admission,
                0,
                Bytes::from_static(b"keep"),
                vec![vec![WriteChunk {
                    inode: 8,
                    member_offset: 0,
                    logical_offset: 0,
                    length: 4,
                }]],
            )
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
        let runtime =
            VolatileWriteRuntime::new(VolatileBudget::new(4096, 16), vec![7], materialize);
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
            runtime.accept_write(
                admission,
                1,
                Bytes::from_static(b"XX"),
                vec![vec![WriteChunk {
                    inode: 7,
                    member_offset: 1,
                    logical_offset: 0,
                    length: 2,
                }]],
            ),
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
        let runtime =
            VolatileWriteRuntime::new(VolatileBudget::new(4096, 16), vec![7], materialize);
        let admission = runtime.reserve(4).await.unwrap();
        runtime
            .accept_write(admission, 0, Bytes::from_static(b"boom"), one_chunk(4))
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
        let runtime =
            VolatileWriteRuntime::new(VolatileBudget::new(4096, 16), vec![7], materialize);
        let admission = runtime.reserve(4).await.unwrap();
        runtime
            .accept_write(admission, 0, Bytes::from_static(b"data"), one_chunk(4))
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
        let runtime =
            VolatileWriteRuntime::new(VolatileBudget::new(4096, 16), vec![7], materialize);
        let admission = runtime.reserve(4).await.unwrap();
        runtime
            .accept_write(admission, 0, Bytes::from_static(b"data"), one_chunk(4))
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
        let first = VolatileWriteRuntime::new(Arc::clone(&budget), vec![7], failing_materializer);
        let second = VolatileWriteRuntime::new(
            Arc::clone(&budget),
            vec![8],
            Arc::new(|_, _, _| async { Ok(()) }.boxed()),
        );
        let first_admission = first.reserve(4).await.unwrap();
        first
            .accept_write(
                first_admission,
                0,
                Bytes::from_static(b"fail"),
                one_chunk(4),
            )
            .await
            .unwrap();
        let reserved_before_failure = second.reserve(4).await.unwrap();
        fail.notify_one();
        tokio::time::timeout(Duration::from_millis(250), first.wait_materialized(1))
            .await
            .expect("failure became visible")
            .unwrap_err();

        let result = second
            .accept_write(
                reserved_before_failure,
                0,
                Bytes::from_static(b"late"),
                vec![vec![WriteChunk {
                    inode: 8,
                    member_offset: 0,
                    logical_offset: 0,
                    length: 4,
                }]],
            )
            .await;
        assert!(matches!(result, Err(super::OverlayError::IoError)));
        assert!(matches!(
            second.wait_materialized(0).await,
            Err(super::OverlayError::IoError)
        ));
    }

    #[tokio::test]
    async fn terminal_materialization_failure_makes_subsequent_reads_fail_closed() {
        let materialize: Materializer =
            Arc::new(|_, _, _| async { Err(super::OverlayError::IoError) }.boxed());
        let runtime =
            VolatileWriteRuntime::new(VolatileBudget::new(4096, 16), vec![7], materialize);
        let admission = runtime.reserve(4).await.unwrap();
        runtime
            .accept_write(admission, 0, Bytes::from_static(b"data"), one_chunk(4))
            .await
            .unwrap();
        runtime.wait_materialized(1).await.unwrap_err();

        let result = runtime
            .read(0, 4, || async { Ok(Bytes::from_static(b"base")) }.boxed())
            .await;
        assert!(matches!(result, Err(super::OverlayError::IoError)));
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
        let runtime =
            VolatileWriteRuntime::new(VolatileBudget::new(4096, 16), vec![7], materialize);
        for offset in [0, 4] {
            let admission = runtime.reserve(4).await.unwrap();
            runtime
                .accept_write(
                    admission,
                    offset,
                    Bytes::from_static(b"data"),
                    vec![vec![WriteChunk {
                        inode: 7,
                        member_offset: offset,
                        logical_offset: 0,
                        length: 4,
                    }]],
                )
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
    async fn one_lane_materializes_writes_in_acceptance_order() {
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
        let runtime =
            VolatileWriteRuntime::new(VolatileBudget::new(4096, 16), vec![7], materialize);

        for (offset, data) in [(0, b"first".as_slice()), (5, b"second".as_slice())] {
            let admission = runtime.reserve(data.len()).await.unwrap();
            runtime
                .accept_write(
                    admission,
                    offset,
                    Bytes::copy_from_slice(data),
                    vec![vec![WriteChunk {
                        inode: 7,
                        member_offset: offset,
                        logical_offset: 0,
                        length: data.len(),
                    }]],
                )
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
        let runtime =
            VolatileWriteRuntime::new(VolatileBudget::new(4096, 16), vec![7, 8], materialize);
        let admission = runtime.reserve(4).await.unwrap();
        runtime
            .accept_write(
                admission,
                0,
                Bytes::from_static(b"data"),
                vec![
                    vec![WriteChunk {
                        inode: 7,
                        member_offset: 0,
                        logical_offset: 0,
                        length: 2,
                    }],
                    vec![WriteChunk {
                        inode: 8,
                        member_offset: 0,
                        logical_offset: 2,
                        length: 2,
                    }],
                ],
            )
            .await
            .unwrap();

        let first = entered_rx.recv().await.unwrap();
        let second = tokio::time::timeout(Duration::from_millis(250), entered_rx.recv())
            .await
            .expect("the other lane must enter while the first remains blocked")
            .unwrap();
        assert_ne!(first, second);
        release.notify_waiters();
        runtime.wait_materialized(1).await.unwrap();
        runtime.shutdown().await.unwrap();
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
        let runtime = VolatileWriteRuntime::new(Arc::clone(&budget), vec![7], materialize);
        let admission = runtime.reserve(4).await.unwrap();
        runtime
            .accept_write(admission, 0, Bytes::from_static(b"data"), one_chunk(4))
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
}
