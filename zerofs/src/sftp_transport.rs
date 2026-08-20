use crate::sftp_object_store::{ObjectHeader, SftpCapabilities};
use async_trait::async_trait;
use bytes::Bytes;
use dashmap::DashMap;
use std::collections::{HashMap, VecDeque};
use std::fmt;
use std::ops::Range;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex as StdMutex, Weak};
use std::time::{Duration, SystemTime};
use tokio::sync::{Mutex, Notify, OwnedSemaphorePermit, Semaphore, oneshot};
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

pub use crate::russh_session::{
    RUSSH_MAXIMUM_PACKET_SIZE, RUSSH_SFTP_MAX_CONCURRENT_WRITES, RUSSH_WINDOW_SIZE,
    RusshSessionFactory,
};

#[cfg(test)]
pub use crate::russh_session::OpenSshTransportSession;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OperationKind {
    Read,
    Write,
    Metadata,
}

#[cfg(test)]
/// Match the largest safe OpenSSH SFTP v3 payload used by the proven rclone
/// path. The client still clips a request if a server negotiates a lower limit.
const SFTP_WRITE_PACKET_SIZE: usize = 255 * 1024;

#[cfg(test)]
/// Maximum outstanding WRITE requests on one leased SFTP session. A 64-request
/// window hides the Storage Box WAN RTT without consuming more TCP sessions.
const SFTP_WRITE_REQUEST_CONCURRENCY: usize = 64;

#[cfg(test)]
/// Match the write-side request size so sequential prefetch windows use the
/// same proven Storage Box packet shape in both directions.
const SFTP_READ_PACKET_SIZE: usize = 255 * 1024;

#[cfg(test)]
/// Maximum outstanding READ requests on one leased SFTP session. ZeroFS ramps
/// sequential cache windows to 8 MiB; issuing their packets together hides the
/// WAN RTT while preserving the shared physical-session limit.
const SFTP_READ_REQUEST_CONCURRENCY: usize = 64;

#[cfg(test)]
#[derive(Debug)]
struct PipelinedWrite {
    offset: u64,
    payload: Bytes,
}

#[cfg(test)]
#[derive(Debug)]
struct PipelinedRead {
    index: usize,
    offset: u64,
    len: usize,
}

#[cfg(test)]
fn plan_pipelined_reads(
    initial_offset: u64,
    len: usize,
) -> Result<Vec<PipelinedRead>, TransportError> {
    let mut offset = initial_offset;
    let mut remaining = len;
    let mut requests = Vec::with_capacity(len.div_ceil(SFTP_READ_PACKET_SIZE));
    while remaining != 0 {
        let request_len = remaining.min(SFTP_READ_PACKET_SIZE);
        requests.push(PipelinedRead {
            index: requests.len(),
            offset,
            len: request_len,
        });
        offset = offset
            .checked_add(request_len as u64)
            .ok_or_else(|| TransportError::Operation("SFTP read offset overflow".to_owned()))?;
        remaining -= request_len;
    }
    Ok(requests)
}

#[cfg(test)]
fn plan_pipelined_writes(
    initial_offset: u64,
    chunks: Vec<Bytes>,
) -> Result<Vec<PipelinedWrite>, TransportError> {
    let mut offset = initial_offset;
    let mut requests = Vec::new();
    for mut chunk in chunks {
        while !chunk.is_empty() {
            let len = chunk.len().min(SFTP_WRITE_PACKET_SIZE);
            let payload = chunk.split_to(len);
            requests.push(PipelinedWrite { offset, payload });
            offset = offset.checked_add(len as u64).ok_or_else(|| {
                TransportError::Operation("SFTP write offset overflow".to_owned())
            })?;
        }
    }
    Ok(requests)
}

const SFTP_SESSION_OPEN_TIMEOUT: Duration = Duration::from_secs(30);
const SFTP_SESSION_CLOSE_TIMEOUT: Duration = Duration::from_secs(10);
const SFTP_SESSION_FORCE_CLEANUP_TIMEOUT: Duration = Duration::from_secs(5);
const SFTP_IDLE_TIMEOUT: Duration = Duration::from_secs(60);
const SFTP_IDLE_REAP_INTERVAL: Duration = Duration::from_secs(10);
const SFTP_IDLE_WARM_FLOOR: usize = 1;
const SFTP_POOL_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(45);
// How long shutdown lets already-scheduled staging cleanups finish while the
// pool can still serve them. A cleanup sleeping between retry attempts holds
// no session, so the activity drain alone would close the pool underneath it
// and turn ordinary staging debris into a failed service stop.
const SFTP_SHUTDOWN_CLEANUP_GRACE: Duration = Duration::from_secs(10);
// Pacing for session dials after a failure. Storage backends cap concurrent
// SSH sessions per account (Hetzner Storage Boxes around ten) and kill the
// excess, and stale sessions from a previous crash still count against the
// cap until the server reaps them. Every retrying caller redialing
// immediately turns one over-limit moment into a sustained churn storm the
// server can never shed; a shared backoff lets it drain instead.
const SFTP_DIAL_BACKOFF_BASE: Duration = Duration::from_millis(100);
const SFTP_DIAL_BACKOFF_MAX: Duration = Duration::from_secs(5);
const SFTP_DIRECTORY_CACHE_MAX_ENTRIES: usize = 64 * 1024;
/// Maximum operations multiplexed onto one SSH session. The SFTP layer
/// pipelines up to 64 outstanding requests per session, so a handful of
/// concurrent operations sharing one connection hide the WAN RTT instead of
/// serializing on it; the cap keeps one session's request window and remote
/// handle usage bounded.
pub(crate) const SFTP_SESSION_MAX_CONCURRENT_OPS: usize = 16;

/// Concurrent metadata operations admitted across the pool. Metadata requests
/// move no payload, so their cost is one WAN round trip each; without a
/// budget of their own a GC or cleanup storm of single-round-trip calls
/// would either starve or be starved by bulk transfers.
fn metadata_admission_limit(connections: usize) -> usize {
    (connections * 8).min(64)
}

#[derive(Debug, thiserror::Error)]
pub enum TransportError {
    #[error("invalid SFTP session limits: {0}")]
    InvalidLimits(String),
    #[error("SFTP server lacks required {0} extension")]
    MissingCapability(&'static str),
    #[error("failed to open SFTP session: {0}")]
    Open(String),
    #[error("failed to close SFTP session: {0}")]
    Close(String),
    #[error("SFTP session pool is closed")]
    PoolClosed,
    #[error("remote path not found: {0}")]
    NotFound(String),
    #[error("unsatisfiable remote object range: {0}")]
    InvalidRange(String),
    #[error("remote path permission denied: {0}")]
    PermissionDenied(String),
    #[error("remote path already exists: {0}")]
    AlreadyExists(String),
    #[error("SFTP operation failed: {0}")]
    Operation(String),
    #[error("remote object is corrupt: {0}")]
    CorruptObject(String),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RemoteEntryKind {
    File,
    Directory,
    Symlink,
    Other,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RemoteDirectoryEntry {
    pub filename: PathBuf,
    pub kind: RemoteEntryKind,
}

#[derive(Debug, Clone)]
pub struct RemoteObjectRead {
    pub header: ObjectHeader,
    pub modified: SystemTime,
    pub range: Range<u64>,
    pub payload: Bytes,
}

#[async_trait]
pub trait TransportSession: fmt::Debug + Send + Sync + 'static {
    fn capabilities(&self) -> SftpCapabilities;
    async fn read_object(
        &self,
        _path: &std::path::Path,
        _range: Option<object_store::GetRange>,
        _head: bool,
    ) -> Result<RemoteObjectRead, TransportError> {
        Err(TransportError::Operation(
            "read_object is not implemented by this session".to_owned(),
        ))
    }
    async fn list_directory(
        &self,
        _path: &std::path::Path,
    ) -> Result<Vec<RemoteDirectoryEntry>, TransportError> {
        Err(TransportError::Operation(
            "list_directory is not implemented by this session".to_owned(),
        ))
    }
    async fn remove_file(&self, _path: &std::path::Path) -> Result<(), TransportError> {
        Err(TransportError::Operation(
            "remove_file is not implemented by this session".to_owned(),
        ))
    }
    async fn remove_directory(&self, _path: &std::path::Path) -> Result<(), TransportError> {
        Err(TransportError::Operation(
            "remove_directory is not implemented by this session".to_owned(),
        ))
    }
    async fn ensure_directory_component(
        &self,
        _path: &std::path::Path,
    ) -> Result<(), TransportError> {
        Err(TransportError::Operation(
            "ensure_directory_component is not implemented by this session".to_owned(),
        ))
    }
    async fn write_file_durable(
        &self,
        _path: &std::path::Path,
        _chunks: Vec<Bytes>,
    ) -> Result<(), TransportError> {
        Err(TransportError::Operation(
            "write_file_durable is not implemented by this session".to_owned(),
        ))
    }
    async fn write_file_at_durable(
        &self,
        _path: &std::path::Path,
        _offset: u64,
        _chunks: Vec<Bytes>,
    ) -> Result<(), TransportError> {
        Err(TransportError::Operation(
            "write_file_at_durable is not implemented by this session".to_owned(),
        ))
    }
    async fn write_file_at(
        &self,
        _path: &std::path::Path,
        _offset: u64,
        _chunks: Vec<Bytes>,
    ) -> Result<(), TransportError> {
        Err(TransportError::Operation(
            "write_file_at is not implemented by this session".to_owned(),
        ))
    }
    async fn read_exact(
        &self,
        _path: &std::path::Path,
        _offset: u64,
        _len: usize,
    ) -> Result<Bytes, TransportError> {
        Err(TransportError::Operation(
            "read_exact is not implemented by this session".to_owned(),
        ))
    }
    async fn hard_link(
        &self,
        _from: &std::path::Path,
        _to: &std::path::Path,
    ) -> Result<(), TransportError> {
        Err(TransportError::Operation(
            "hard_link is not implemented by this session".to_owned(),
        ))
    }
    async fn posix_rename(
        &self,
        _from: &std::path::Path,
        _to: &std::path::Path,
    ) -> Result<(), TransportError> {
        Err(TransportError::Operation(
            "posix_rename is not implemented by this session".to_owned(),
        ))
    }
    async fn close(&self, force: CancellationToken) -> Result<(), TransportError>;
}

#[async_trait]
pub trait SessionFactory: fmt::Debug + Send + Sync + 'static {
    async fn open(
        &self,
        force: CancellationToken,
    ) -> Result<Box<dyn TransportSession>, TransportError>;
}

#[derive(Clone)]
struct FairAdmission {
    inner: Arc<AdmissionInner>,
}

struct AdmissionInner {
    total_limit: usize,
    read_limit: usize,
    write_limit: usize,
    metadata_limit: usize,
    next_id: AtomicU64,
    state: StdMutex<AdmissionState>,
}

#[derive(Default)]
struct AdmissionState {
    active_reads: usize,
    active_writes: usize,
    active_metadata: usize,
    waiters: VecDeque<AdmissionWaiter>,
    closed: bool,
}

impl AdmissionState {
    fn active_total(&self) -> usize {
        self.active_reads + self.active_writes + self.active_metadata
    }
}

struct AdmissionWaiter {
    id: u64,
    kind: OperationKind,
    sender: oneshot::Sender<OperationAdmission>,
}

struct OperationAdmission {
    admission: FairAdmission,
    kind: OperationKind,
    active: bool,
}

impl FairAdmission {
    fn new(
        total_limit: usize,
        read_limit: usize,
        write_limit: usize,
        metadata_limit: usize,
    ) -> Self {
        Self {
            inner: Arc::new(AdmissionInner {
                total_limit,
                read_limit,
                write_limit,
                metadata_limit,
                next_id: AtomicU64::new(0),
                state: StdMutex::new(AdmissionState::default()),
            }),
        }
    }

    async fn acquire(&self, kind: OperationKind) -> Result<OperationAdmission, TransportError> {
        let id = self.inner.next_id.fetch_add(1, Ordering::Relaxed);
        let (sender, receiver) = oneshot::channel();
        let mut registration = AdmissionRegistration {
            admission: self.clone(),
            id,
            waiting: true,
        };
        {
            let mut state = self.inner.state.lock().unwrap();
            if state.closed {
                return Err(TransportError::PoolClosed);
            }
            state
                .waiters
                .push_back(AdmissionWaiter { id, kind, sender });
            self.dispatch_locked(&mut state);
        }
        let permit = receiver.await.map_err(|_| TransportError::PoolClosed)?;
        registration.waiting = false;
        Ok(permit)
    }

    fn dispatch_locked(&self, state: &mut AdmissionState) {
        if state.closed {
            return;
        }
        while state.active_total() < self.inner.total_limit {
            let writes_waiting = state
                .waiters
                .iter()
                .any(|waiter| waiter.kind == OperationKind::Write);
            // Draining dirty data is the long-running bulk path. Prefer queued
            // writes up to their configured ceiling when that ceiling reserves
            // shared capacity for reads. Once writeback empties, reads
            // immediately expand to their own configured ceiling.
            let write_reserves_opposite_slot = self.inner.write_limit < self.inner.total_limit;
            let preferred_write = (writes_waiting && write_reserves_opposite_slot).then(|| {
                state.waiters.iter().position(|waiter| {
                    waiter.kind == OperationKind::Write && self.can_admit(state, waiter.kind)
                })
            });
            let index = preferred_write.flatten().or_else(|| {
                state
                    .waiters
                    .iter()
                    .position(|waiter| self.can_admit(state, waiter.kind))
            });
            let Some(index) = index else {
                break;
            };
            let waiter = state.waiters.remove(index).unwrap();
            increment_active(state, waiter.kind);
            let permit = OperationAdmission {
                admission: self.clone(),
                kind: waiter.kind,
                active: true,
            };
            if let Err(mut permit) = waiter.sender.send(permit) {
                permit.active = false;
                decrement_active(state, permit.kind);
            }
        }
    }

    fn can_admit(&self, state: &AdmissionState, kind: OperationKind) -> bool {
        let (active, configured_limit) = match kind {
            OperationKind::Read => (state.active_reads, self.inner.read_limit),
            OperationKind::Write => (state.active_writes, self.inner.write_limit),
            OperationKind::Metadata => (state.active_metadata, self.inner.metadata_limit),
        };
        active < configured_limit
    }

    fn cancel_waiter(&self, id: u64) {
        let mut state = self.inner.state.lock().unwrap();
        if let Some(index) = state.waiters.iter().position(|waiter| waiter.id == id) {
            state.waiters.remove(index);
            self.dispatch_locked(&mut state);
        }
    }

    fn release(&self, kind: OperationKind) {
        let mut state = self.inner.state.lock().unwrap();
        decrement_active(&mut state, kind);
        self.dispatch_locked(&mut state);
    }

    fn close(&self) {
        let mut state = self.inner.state.lock().unwrap();
        state.closed = true;
        state.waiters.clear();
    }

    #[cfg(test)]
    fn waiter_count(&self) -> usize {
        self.inner.state.lock().unwrap().waiters.len()
    }
}

fn increment_active(state: &mut AdmissionState, kind: OperationKind) {
    match kind {
        OperationKind::Read => state.active_reads += 1,
        OperationKind::Write => state.active_writes += 1,
        OperationKind::Metadata => state.active_metadata += 1,
    }
}

fn decrement_active(state: &mut AdmissionState, kind: OperationKind) {
    match kind {
        OperationKind::Read => state.active_reads -= 1,
        OperationKind::Write => state.active_writes -= 1,
        OperationKind::Metadata => state.active_metadata -= 1,
    }
}

struct AdmissionRegistration {
    admission: FairAdmission,
    id: u64,
    waiting: bool,
}

impl Drop for AdmissionRegistration {
    fn drop(&mut self) {
        if self.waiting {
            self.admission.cancel_waiter(self.id);
        }
    }
}

impl Drop for OperationAdmission {
    fn drop(&mut self) {
        if self.active {
            self.active = false;
            self.admission.release(self.kind);
        }
    }
}

/// One SSH connection shared by up to [`SFTP_SESSION_MAX_CONCURRENT_OPS`]
/// concurrent operations. The underlying transport pipelines independent
/// requests, so sharing multiplies small-operation throughput per connection
/// instead of serializing every operation on one WAN round trip at a time.
struct SharedSession {
    transport: Arc<dyn TransportSession>,
    // Dropped only after the transport finished closing, so a redial cannot
    // race the remote server still counting the old session against its cap.
    lifetime: StdMutex<Option<OwnedSemaphorePermit>>,
    active_ops: AtomicUsize,
    active_writes: AtomicUsize,
    broken: AtomicBool,
    // Whoever swaps this to true owns the close; releases and reapers race
    // for it once a session must go away.
    closing: AtomicBool,
    idle_since: StdMutex<Instant>,
}

impl SharedSession {
    fn new(transport: Arc<dyn TransportSession>, lifetime: OwnedSemaphorePermit) -> Self {
        Self {
            transport,
            lifetime: StdMutex::new(Some(lifetime)),
            active_ops: AtomicUsize::new(0),
            active_writes: AtomicUsize::new(0),
            broken: AtomicBool::new(false),
            closing: AtomicBool::new(false),
            idle_since: StdMutex::new(Instant::now()),
        }
    }

    fn take_lifetime(&self) -> Option<OwnedSemaphorePermit> {
        self.lifetime.lock().unwrap().take()
    }

    fn claim(&self, kind: OperationKind) {
        if kind == OperationKind::Write {
            self.active_writes.fetch_add(1, Ordering::SeqCst);
        }
        self.active_ops.fetch_add(1, Ordering::SeqCst);
    }
}

#[derive(Default)]
struct DirectoryCache {
    epoch: AtomicU64,
    known: DashMap<PathBuf, u64>,
    locks: StdMutex<HashMap<PathBuf, Weak<Mutex<()>>>>,
}

impl DirectoryCache {
    fn contains(&self, path: &std::path::Path) -> bool {
        let epoch = self.epoch.load(Ordering::Acquire);
        self.known.get(path).is_some_and(|entry| *entry == epoch)
    }

    fn insert(&self, path: PathBuf, epoch: u64) {
        if self.epoch.load(Ordering::Acquire) != epoch {
            return;
        }
        if self.known.len() >= SFTP_DIRECTORY_CACHE_MAX_ENTRIES {
            self.invalidate();
        }
        let epoch = self.epoch.load(Ordering::Acquire);
        self.known.insert(path, epoch);
    }

    fn invalidate(&self) {
        self.epoch.fetch_add(1, Ordering::AcqRel);
        self.known.clear();
    }

    fn component_lock(&self, path: &std::path::Path) -> Arc<Mutex<()>> {
        let mut locks = self.locks.lock().unwrap();
        locks.retain(|_, lock| lock.strong_count() > 0);
        if let Some(lock) = locks.get(path).and_then(Weak::upgrade) {
            return lock;
        }
        let lock = Arc::new(Mutex::new(()));
        locks.insert(path.to_path_buf(), Arc::downgrade(&lock));
        lock
    }
}

struct PoolInner {
    factory: Arc<dyn SessionFactory>,
    shared: Arc<Semaphore>,
    pending_dials: AtomicUsize,
    admission: FairAdmission,
    roster: StdMutex<Vec<Arc<SharedSession>>>,
    roster_changed: Notify,
    directories: DirectoryCache,
    writable: bool,
    closed: AtomicBool,
    activity_gate: StdMutex<()>,
    active: AtomicUsize,
    activity_changed: Notify,
    reaper_shutdown: CancellationToken,
    session_shutdown: CancellationToken,
    tasks: TaskTracker,
    // Staging-cleanup retries live in their own tracker so shutdown can wait
    // for exactly them before closing the pool; session-close tasks in
    // `tasks` keep their original shutdown ordering.
    cleanup_tasks: TaskTracker,
    // One dial at a time, paced by the shared failure backoff below.
    dial_gate: Mutex<()>,
    dial_backoff: StdMutex<DialBackoff>,
    runtime: tokio::runtime::Handle,
    shutdown_lock: Mutex<()>,
    shutdown_complete: AtomicBool,
    close_error: StdMutex<Option<String>>,
}

#[derive(Debug, Default)]
struct DialBackoff {
    consecutive_failures: u32,
    next_allowed: Option<Instant>,
}

struct FailClosedOnOwnerDrop {
    pool: Arc<PoolInner>,
    armed: bool,
}

struct PendingDial {
    pool: Arc<PoolInner>,
}

impl PendingDial {
    fn new(pool: Arc<PoolInner>) -> Self {
        pool.pending_dials.fetch_add(1, Ordering::SeqCst);
        Self { pool }
    }
}

impl Drop for PendingDial {
    fn drop(&mut self) {
        self.pool.pending_dials.fetch_sub(1, Ordering::SeqCst);
        self.pool.roster_changed.notify_waiters();
    }
}

impl FailClosedOnOwnerDrop {
    fn new(pool: Arc<PoolInner>) -> Self {
        Self { pool, armed: true }
    }

    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for FailClosedOnOwnerDrop {
    fn drop(&mut self) {
        if self.armed {
            self.pool.fail_closed();
        }
    }
}

impl PoolInner {
    fn fail_closed(self: &Arc<Self>) {
        let _gate = self.activity_gate.lock().unwrap();
        if self.closed.swap(true, Ordering::SeqCst) {
            return;
        }
        self.reaper_shutdown.cancel();
        self.session_shutdown.cancel();
        self.admission.close();
        self.shared.close();
        self.roster_changed.notify_waiters();
        // Sessions with operations still in flight close when their last
        // release observes the closed pool.
        self.close_departing_sessions(self.drain_unused_sessions());
    }

    /// Removes every roster session that has no operation in flight and
    /// returns them for closing.
    fn drain_unused_sessions(self: &Arc<Self>) -> Vec<Arc<SharedSession>> {
        let mut roster = self.roster.lock().unwrap();
        let mut departing = Vec::new();
        roster.retain(|session| {
            if session.active_ops.load(Ordering::SeqCst) == 0 {
                departing.push(session.clone());
                false
            } else {
                true
            }
        });
        departing
    }

    fn close_departing_sessions(self: &Arc<Self>, departing: Vec<Arc<SharedSession>>) {
        if departing.is_empty() {
            return;
        }
        if tokio::runtime::Handle::try_current().is_ok() {
            for session in departing {
                let pool = self.clone();
                self.tasks.spawn(async move {
                    let _ = pool.close_shared_session(session).await;
                });
            }
        } else {
            // No runtime to run the graceful close; dropping the transport
            // still tears the connection down via its Drop.
            for session in departing {
                if !session.closing.swap(true, Ordering::SeqCst) {
                    drop(session.take_lifetime());
                }
            }
        }
    }

    /// Closes a session removed from the roster. Exactly one caller wins the
    /// `closing` flag; late duplicates (a release racing the reaper) are
    /// no-ops.
    async fn close_shared_session(
        self: &Arc<Self>,
        session: Arc<SharedSession>,
    ) -> Result<(), TransportError> {
        if session.closing.swap(true, Ordering::SeqCst) {
            return Ok(());
        }
        let lifetime = session.take_lifetime();
        let result = self.close_owned(session.transport.clone(), lifetime).await;
        self.roster_changed.notify_waiters();
        result
    }

    /// Called when a lease finishes with its session. Broken sessions leave
    /// the roster immediately so no new operation lands on them; the close
    /// itself waits for the last in-flight operation.
    fn release_session(self: &Arc<Self>, session: &Arc<SharedSession>, kind: OperationKind) {
        if kind == OperationKind::Write {
            session.active_writes.fetch_sub(1, Ordering::SeqCst);
        }
        let remaining = session.active_ops.fetch_sub(1, Ordering::SeqCst) - 1;
        let broken = session.broken.load(Ordering::SeqCst);
        if broken {
            self.remove_from_roster(session);
        }
        if remaining == 0 {
            *session.idle_since.lock().unwrap() = Instant::now();
            if broken || self.closed.load(Ordering::SeqCst) {
                self.remove_from_roster(session);
                if tokio::runtime::Handle::try_current().is_ok() {
                    let pool = self.clone();
                    let session = session.clone();
                    self.tasks.spawn(async move {
                        let _ = pool.close_shared_session(session).await;
                    });
                } else if !session.closing.swap(true, Ordering::SeqCst) {
                    self.fail_closed();
                    drop(session.take_lifetime());
                }
                return;
            }
        }
        self.roster_changed.notify_waiters();
    }

    fn remove_from_roster(&self, session: &Arc<SharedSession>) {
        let mut roster = self.roster.lock().unwrap();
        // Placement claims sessions while holding this same lock. Publish the
        // broken state only after acquiring it so no checkout can pass the
        // healthy-session filter and claim the session between these steps.
        session.broken.store(true, Ordering::SeqCst);
        roster.retain(|entry| !Arc::ptr_eq(entry, session));
    }

    fn register_activity(self: &Arc<Self>) -> Result<PoolActivity, TransportError> {
        let _gate = self.activity_gate.lock().unwrap();
        if self.closed.load(Ordering::SeqCst) {
            return Err(TransportError::PoolClosed);
        }
        self.active.fetch_add(1, Ordering::SeqCst);
        Ok(PoolActivity { pool: self.clone() })
    }

    fn finish_activity(&self) {
        self.active.fetch_sub(1, Ordering::SeqCst);
        self.activity_changed.notify_waiters();
    }

    async fn wait_for_activity_drain(&self) {
        loop {
            let changed = self.activity_changed.notified();
            if self.active.load(Ordering::SeqCst) == 0 {
                return;
            }
            changed.await;
        }
    }

    fn record_close_error(&self, error: &TransportError) {
        self.record_error_message(error.to_string());
    }

    fn record_error_message(&self, error: String) {
        let mut first = self.close_error.lock().unwrap();
        if first.is_none() {
            *first = Some(error);
        }
    }

    fn record_forced_cleanup_error(&self, error: &TransportError) {
        let mut recorded = self.close_error.lock().unwrap();
        match recorded.as_mut() {
            Some(recorded) => {
                recorded.push_str("; forced cleanup also failed: ");
                recorded.push_str(&error.to_string());
            }
            None => *recorded = Some(error.to_string()),
        }
    }

    async fn close_transport(
        self: &Arc<Self>,
        transport: Arc<dyn TransportSession>,
    ) -> Result<(), TransportError> {
        self.close_owned(transport, None).await
    }

    async fn close_owned(
        self: &Arc<Self>,
        transport: Arc<dyn TransportSession>,
        lifetime: Option<OwnedSemaphorePermit>,
    ) -> Result<(), TransportError> {
        let (sender, receiver) = oneshot::channel();
        let owner_pool = self.clone();
        self.tasks.spawn(async move {
            let lifetime = lifetime;
            let mut fail_closed_on_drop = FailClosedOnOwnerDrop::new(owner_pool.clone());
            let force = CancellationToken::new();
            let closing = transport.close(force.clone());
            tokio::pin!(closing);
            let result = match tokio::time::timeout(SFTP_SESSION_CLOSE_TIMEOUT, &mut closing).await {
                Ok(result) => result,
                Err(_) => {
                    force.cancel();
                    match tokio::time::timeout(
                        SFTP_SESSION_FORCE_CLEANUP_TIMEOUT,
                        &mut closing,
                    )
                    .await
                    {
                        Ok(result) => result,
                        Err(_) => {
                            owner_pool.fail_closed();
                            let error = TransportError::Close(format!(
                                "forced SFTP session cleanup timed out after {}s",
                                SFTP_SESSION_FORCE_CLEANUP_TIMEOUT.as_secs()
                            ));
                            owner_pool.record_close_error(&error);
                            let _ = sender.send(Err(error));
                            if let Err(error) = closing.await {
                                tracing::error!(%error, "forced SFTP child/master cleanup failed after close timeout");
                                owner_pool.record_forced_cleanup_error(&error);
                            }
                            fail_closed_on_drop.disarm();
                            drop(lifetime);
                            return;
                        }
                    }
                }
            };

            if let Err(error) = &result {
                owner_pool.fail_closed();
                owner_pool.record_close_error(error);
            }
            fail_closed_on_drop.disarm();
            drop(lifetime);
            let _ = sender.send(result);
        });
        match receiver.await {
            Ok(result) => result,
            Err(_) => {
                self.fail_closed();
                let error = TransportError::Close("SFTP close owner task failed".to_owned());
                self.record_close_error(&error);
                Err(error)
            }
        }
    }

    async fn reap_expired_idle(self: &Arc<Self>) {
        let expired = {
            let now = Instant::now();
            let mut roster = self.roster.lock().unwrap();
            let mut expired = Vec::new();
            let mut retained = roster.len();
            roster.retain(|session| {
                let expirable = retained > SFTP_IDLE_WARM_FLOOR
                    && session.active_ops.load(Ordering::SeqCst) == 0
                    && now.saturating_duration_since(*session.idle_since.lock().unwrap())
                        >= SFTP_IDLE_TIMEOUT;
                if expirable {
                    retained -= 1;
                    expired.push(session.clone());
                }
                !expirable
            });
            expired
        };

        for session in expired {
            let inner = self.clone();
            self.tasks.spawn(async move {
                let _ = inner.close_shared_session(session).await;
            });
        }
    }
}

#[derive(Clone)]
pub struct SftpSessionPool {
    inner: Arc<PoolInner>,
}

impl fmt::Debug for SftpSessionPool {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SftpSessionPool")
            .field("writable", &self.inner.writable)
            .finish_non_exhaustive()
    }
}

impl SftpSessionPool {
    pub(crate) fn write_concurrency(&self) -> usize {
        self.inner.admission.inner.write_limit
    }

    pub(crate) async fn ensure_directory(
        &self,
        lease: &mut SessionLease,
        path: &std::path::Path,
    ) -> Result<(), TransportError> {
        self.ensure_directory_components(lease, path).await
    }

    pub(crate) async fn repair_directory(
        &self,
        lease: &mut SessionLease,
        path: &std::path::Path,
    ) -> Result<(), TransportError> {
        self.inner.directories.invalidate();
        metrics::counter!("zerofs_sftp_directory_cache_invalidations_total").increment(1);
        self.ensure_directory_components(lease, path).await
    }

    async fn ensure_directory_components(
        &self,
        lease: &mut SessionLease,
        path: &std::path::Path,
    ) -> Result<(), TransportError> {
        let mut current = PathBuf::new();
        for component in path.components() {
            let std::path::Component::Normal(component) = component else {
                return Err(TransportError::Operation(format!(
                    "unsafe directory path {}",
                    path.display()
                )));
            };
            current.push(component);
            if self.inner.directories.contains(&current) {
                metrics::counter!("zerofs_sftp_directory_cache_hits_total").increment(1);
                continue;
            }
            let component_lock = self.inner.directories.component_lock(&current);
            let _guard = component_lock.lock().await;
            if self.inner.directories.contains(&current) {
                metrics::counter!("zerofs_sftp_directory_cache_hits_total").increment(1);
                continue;
            }
            metrics::counter!("zerofs_sftp_directory_cache_misses_total").increment(1);
            let epoch = self.inner.directories.epoch.load(Ordering::Acquire);
            lease.ensure_directory_component(&current).await?;
            self.inner.directories.insert(current.clone(), epoch);
        }
        Ok(())
    }

    pub(crate) fn spawn_cleanup<F>(&self, _path: &std::path::Path, future: F) -> bool
    where
        F: std::future::Future<Output = ()> + Send + 'static,
    {
        let _gate = self.inner.activity_gate.lock().unwrap();
        if self.inner.closed.load(Ordering::SeqCst) {
            // The staging file stays behind as invisible debris; the next
            // boot's replay publishes through fresh staging names, so this is
            // the caller's warning, never a shutdown failure.
            return false;
        }
        self.inner
            .cleanup_tasks
            .spawn_on(future, &self.inner.runtime);
        true
    }

    pub(crate) fn record_cleanup_debt(
        &self,
        path: &std::path::Path,
        error: &impl std::fmt::Display,
    ) {
        self.inner.record_error_message(format!(
            "SFTP staging cleanup remains required for {}: {error}",
            path.display()
        ));
    }

    pub async fn from_config_writable(
        factory: Arc<dyn SessionFactory>,
        config: &crate::config::SftpConfig,
    ) -> Result<Self, TransportError> {
        Self::new_writable(
            factory,
            config.max_connections,
            config.read_concurrency,
            config.write_concurrency,
        )
        .await
    }

    pub async fn new_writable(
        factory: Arc<dyn SessionFactory>,
        shared: usize,
        reads: usize,
        writes: usize,
    ) -> Result<Self, TransportError> {
        Self::validate_limits(shared, reads, writes)?;
        let pool = Self {
            inner: Arc::new(PoolInner {
                factory,
                shared: Arc::new(Semaphore::new(shared)),
                pending_dials: AtomicUsize::new(0),
                admission: FairAdmission::new(
                    shared * SFTP_SESSION_MAX_CONCURRENT_OPS,
                    reads,
                    writes,
                    metadata_admission_limit(shared),
                ),
                roster: StdMutex::new(Vec::new()),
                roster_changed: Notify::new(),
                directories: DirectoryCache::default(),
                writable: true,
                closed: AtomicBool::new(false),
                activity_gate: StdMutex::new(()),
                active: AtomicUsize::new(0),
                activity_changed: Notify::new(),
                reaper_shutdown: CancellationToken::new(),
                session_shutdown: CancellationToken::new(),
                tasks: TaskTracker::new(),
                cleanup_tasks: TaskTracker::new(),
                dial_gate: Mutex::new(()),
                dial_backoff: StdMutex::new(DialBackoff::default()),
                runtime: tokio::runtime::Handle::current(),
                shutdown_lock: Mutex::new(()),
                shutdown_complete: AtomicBool::new(false),
                close_error: StdMutex::new(None),
            }),
        };

        let session = pool.open_physical().await?;
        pool.inner.roster.lock().unwrap().push(session);
        pool.start_idle_reaper();
        Ok(pool)
    }

    fn start_idle_reaper(&self) {
        let inner = Arc::downgrade(&self.inner);
        let shutdown = self.inner.reaper_shutdown.clone();
        let mut next_tick = Instant::now() + SFTP_IDLE_REAP_INTERVAL;
        self.inner.tasks.spawn(async move {
            loop {
                tokio::select! {
                    _ = shutdown.cancelled() => break,
                    _ = tokio::time::sleep_until(next_tick) => {
                        next_tick += SFTP_IDLE_REAP_INTERVAL;
                        let Some(inner) = inner.upgrade() else {
                            break;
                        };
                        inner.reap_expired_idle().await;
                    }
                }
            }
        });
    }

    fn validate_limits(shared: usize, reads: usize, writes: usize) -> Result<(), TransportError> {
        if !(1..=8).contains(&shared) {
            return Err(TransportError::InvalidLimits(
                "shared sessions must be between 1 and 8".to_owned(),
            ));
        }
        // Operations multiplex onto shared sessions, so per-kind concurrency
        // may exceed the connection count; the per-session cap still bounds
        // what one connection carries.
        let concurrency_ceiling = shared * SFTP_SESSION_MAX_CONCURRENT_OPS;
        for (name, value) in [("read", reads), ("write", writes)] {
            if value == 0 || value > concurrency_ceiling {
                return Err(TransportError::InvalidLimits(format!(
                    "{name} concurrency must be between 1 and {concurrency_ceiling}"
                )));
            }
        }
        Ok(())
    }

    pub async fn checkout(&self, kind: OperationKind) -> Result<SessionLease, TransportError> {
        let admission = self.inner.admission.acquire(kind).await?;
        let activity = self.inner.register_activity()?;

        let session = self.acquire_session(kind).await?;
        Ok(SessionLease {
            pool: self.inner.clone(),
            session: Some(session),
            kind,
            admission: Some(admission),
            activity: Some(activity),
        })
    }

    pub fn begin_shutdown(&self) {
        self.inner.fail_closed();
    }

    pub async fn shutdown(&self) -> Result<(), TransportError> {
        let deadline = Instant::now() + SFTP_POOL_SHUTDOWN_TIMEOUT;
        let _shutdown = tokio::time::timeout_at(deadline, self.inner.shutdown_lock.lock())
            .await
            .map_err(|_| {
                TransportError::Close(format!(
                    "SFTP pool shutdown timed out after {}s",
                    SFTP_POOL_SHUTDOWN_TIMEOUT.as_secs()
                ))
            })?;
        if self.inner.shutdown_complete.load(Ordering::SeqCst) {
            return match self.inner.close_error.lock().unwrap().as_ref() {
                Some(error) => Err(TransportError::Close(error.clone())),
                None => Ok(()),
            };
        }

        // Give scheduled staging cleanups a bounded window to finish while
        // checkouts still work. A cleanup retry sleeping between attempts
        // owns no session, so the activity drain below cannot protect it: the
        // close would land first and the retry could only fail.
        self.inner.reaper_shutdown.cancel();
        let quiesce = async {
            while !self.inner.cleanup_tasks.is_empty() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        };
        let grace = Instant::now() + SFTP_SHUTDOWN_CLEANUP_GRACE;
        let _ = tokio::time::timeout_at(deadline.min(grace), quiesce).await;

        self.inner.fail_closed();
        let inner = self.inner.clone();
        let drain = async move {
            inner.wait_for_activity_drain().await;
            // After the activity drain no operation is in flight, so this
            // removes every remaining session.
            inner.close_departing_sessions(inner.drain_unused_sessions());
            inner.tasks.close();
            inner.tasks.wait().await;
            inner.cleanup_tasks.close();
            inner.cleanup_tasks.wait().await;
        };

        if tokio::time::timeout_at(deadline, drain).await.is_err() {
            return Err(TransportError::Close(format!(
                "SFTP pool shutdown timed out after {}s",
                SFTP_POOL_SHUTDOWN_TIMEOUT.as_secs()
            )));
        }
        self.inner.shutdown_complete.store(true, Ordering::SeqCst);
        match self.inner.close_error.lock().unwrap().as_ref() {
            Some(error) => Err(TransportError::Close(error.clone())),
            None => Ok(()),
        }
    }

    /// Places one operation on a session. Prefers a fully idle session, then
    /// dials a new connection while capacity remains, and only then stacks
    /// the operation onto the least-loaded session below the per-session cap
    /// so bulk transfers spread across connections before they share one.
    async fn acquire_session(
        &self,
        kind: OperationKind,
    ) -> Result<Arc<SharedSession>, TransportError> {
        enum Placement {
            Use(Arc<SharedSession>),
            Dial(OwnedSemaphorePermit, PendingDial),
            Wait,
        }
        loop {
            let changed = self.inner.roster_changed.notified();
            tokio::pin!(changed);
            // Register interest before inspecting the roster: notify_waiters
            // only wakes already-registered waiters, so a release landing
            // between the roster check and the first poll would otherwise be
            // a lost wakeup.
            changed.as_mut().enable();
            let placement = {
                let roster = self.inner.roster.lock().unwrap();
                if self.inner.closed.load(Ordering::SeqCst) {
                    return Err(TransportError::PoolClosed);
                }
                let candidate = roster
                    .iter()
                    .filter(|session| {
                        !session.broken.load(Ordering::SeqCst)
                            && session.active_ops.load(Ordering::SeqCst)
                                < SFTP_SESSION_MAX_CONCURRENT_OPS
                    })
                    .min_by_key(|session| {
                        let ops = session.active_ops.load(Ordering::SeqCst);
                        match kind {
                            OperationKind::Write => {
                                (session.active_writes.load(Ordering::SeqCst), ops)
                            }
                            OperationKind::Read | OperationKind::Metadata => (ops, 0),
                        }
                    })
                    .cloned();
                match candidate {
                    Some(session) if session.active_ops.load(Ordering::SeqCst) == 0 => {
                        session.claim(kind);
                        Placement::Use(session)
                    }
                    other => match self.inner.shared.clone().try_acquire_owned() {
                        Ok(permit) => Placement::Dial(permit, PendingDial::new(self.inner.clone())),
                        Err(tokio::sync::TryAcquireError::Closed) => {
                            return Err(TransportError::PoolClosed);
                        }
                        Err(tokio::sync::TryAcquireError::NoPermits)
                            if self.inner.pending_dials.load(Ordering::SeqCst) != 0 =>
                        {
                            // The missing permits belong to expansion dials or
                            // retiring sessions that have not released their
                            // remote connection slots yet. Wait for those
                            // owners instead of stacking the rest of a burst
                            // onto the first live session while its peers are
                            // still opening.
                            Placement::Wait
                        }
                        Err(tokio::sync::TryAcquireError::NoPermits) => match other {
                            Some(session) => {
                                session.claim(kind);
                                Placement::Use(session)
                            }
                            None => Placement::Wait,
                        },
                    },
                }
            };
            match placement {
                Placement::Use(session) => return Ok(session),
                Placement::Dial(permit, pending_dial) => {
                    match self.open_with_permit(permit).await {
                        Ok(session) => {
                            session.claim(kind);
                            self.inner.roster.lock().unwrap().push(session.clone());
                            drop(pending_dial);
                            self.inner.roster_changed.notify_waiters();
                            return Ok(session);
                        }
                        Err(error) => {
                            // A busy session can still serve this operation;
                            // only fail when nothing can carry it.
                            let fallback = {
                                let roster = self.inner.roster.lock().unwrap();
                                if self.inner.closed.load(Ordering::SeqCst) {
                                    return Err(error);
                                }
                                let candidate = roster
                                    .iter()
                                    .filter(|session| {
                                        !session.broken.load(Ordering::SeqCst)
                                            && session.active_ops.load(Ordering::SeqCst)
                                                < SFTP_SESSION_MAX_CONCURRENT_OPS
                                    })
                                    .min_by_key(|session| session.active_ops.load(Ordering::SeqCst))
                                    .cloned();
                                if let Some(session) = &candidate {
                                    session.claim(kind);
                                }
                                candidate
                            };
                            drop(pending_dial);
                            match fallback {
                                Some(session) => return Ok(session),
                                None => return Err(error),
                            }
                        }
                    }
                }
                Placement::Wait => changed.await,
            }
        }
    }

    async fn open_physical(&self) -> Result<Arc<SharedSession>, TransportError> {
        let permit = self
            .inner
            .shared
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| TransportError::PoolClosed)?;
        self.open_with_permit(permit).await
    }

    async fn open_with_permit(
        &self,
        permit: OwnedSemaphorePermit,
    ) -> Result<Arc<SharedSession>, TransportError> {
        // Serialize dials and pace them behind the shared failure backoff: a
        // backend at its concurrent-session cap kills excess SSH sessions,
        // and unpaced parallel redials from every retrying caller turn one
        // over-limit moment into a sustained churn storm.
        let _dial_turn = self.inner.dial_gate.lock().await;
        let next_allowed = self
            .inner
            .dial_backoff
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .next_allowed;
        if let Some(next_allowed) = next_allowed {
            tokio::time::sleep_until(next_allowed).await;
        }
        let result = self.open_with_permit_unpaced(permit).await;
        let mut backoff = self
            .inner
            .dial_backoff
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if result.is_ok() {
            *backoff = DialBackoff::default();
        } else {
            backoff.consecutive_failures = backoff.consecutive_failures.saturating_add(1);
            let exponent = backoff.consecutive_failures.saturating_sub(1).min(10);
            let delay = SFTP_DIAL_BACKOFF_BASE
                .saturating_mul(1_u32 << exponent)
                .min(SFTP_DIAL_BACKOFF_MAX);
            backoff.next_allowed = Some(Instant::now() + delay);
        }
        drop(backoff);
        result
    }

    async fn open_with_permit_unpaced(
        &self,
        permit: OwnedSemaphorePermit,
    ) -> Result<Arc<SharedSession>, TransportError> {
        let inner = self.inner.clone();
        let factory = inner.factory.clone();
        let writable = inner.writable;
        let owner_pool = inner.clone();
        let (sender, receiver) = oneshot::channel();
        self.inner.tasks.spawn(async move {
            let mut permit = Some(permit);
            let mut fail_closed_on_drop = FailClosedOnOwnerDrop::new(owner_pool.clone());
            let force = owner_pool.session_shutdown.child_token();
            let opening = factory.open(force.clone());
            tokio::pin!(opening);
            let result = tokio::select! {
                result = &mut opening => result.map(|transport| {
                    Arc::new(SharedSession::new(
                        Arc::from(transport),
                        permit.take().expect("open permit is taken exactly once"),
                    ))
                }),
                _ = tokio::time::sleep(SFTP_SESSION_OPEN_TIMEOUT) => {
                    owner_pool.fail_closed();
                    drop(permit.take());
                    let _ = sender.send(Err(TransportError::Open(format!(
                        "SFTP session open timed out after {}s",
                        SFTP_SESSION_OPEN_TIMEOUT.as_secs()
                    ))));
                    force.cancel();
                    match opening.await {
                        Ok(transport) => {
                            let _ = owner_pool.close_transport(Arc::from(transport)).await;
                        }
                        Err(error @ TransportError::Close(_)) => {
                            tracing::error!(%error, "SFTP opener cleanup failed after open timeout");
                            owner_pool.record_forced_cleanup_error(&error);
                        }
                        Err(_) => {}
                    }
                    return;
                }
            };
            if matches!(
                result,
                Err(TransportError::PoolClosed | TransportError::Close(_))
            ) {
                owner_pool.fail_closed();
            }
            if let Err(error @ TransportError::Close(_)) = &result {
                owner_pool.record_forced_cleanup_error(error);
            }
            fail_closed_on_drop.disarm();
            if let Err(Ok(session)) = sender.send(result) {
                let _ = owner_pool.close_shared_session(session).await;
            }
        });
        let session = match receiver.await {
            Ok(Ok(session)) => session,
            Ok(Err(error)) => return Err(error),
            Err(_) => {
                self.inner.fail_closed();
                return Err(TransportError::PoolClosed);
            }
        };
        if writable
            && let Err(error) = require_publication_capabilities(session.transport.capabilities())
        {
            match self.inner.close_shared_session(session).await {
                Ok(()) => return Err(error),
                Err(cleanup) => {
                    self.inner.fail_closed();
                    tracing::error!(
                        %cleanup,
                        "failed to clean up a capability-rejected SFTP session"
                    );
                    return Err(cleanup);
                }
            }
        }
        Ok(session)
    }
}

fn require_publication_capabilities(capabilities: SftpCapabilities) -> Result<(), TransportError> {
    for (present, extension) in [
        (capabilities.fsync, "fsync@openssh.com"),
        (capabilities.hardlink, "hardlink@openssh.com"),
        (capabilities.posix_rename, "posix-rename@openssh.com"),
    ] {
        if !present {
            return Err(TransportError::MissingCapability(extension));
        }
    }
    Ok(())
}

#[cfg(test)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SessionDisposition {
    Reuse,
    BrokenOrAmbiguous,
}

#[cfg(test)]
#[derive(Debug)]
pub enum LeaseFinishError<E> {
    Operation {
        error: E,
        cleanup: Option<TransportError>,
    },
    Lifecycle(TransportError),
}

pub struct SessionLease {
    pool: Arc<PoolInner>,
    session: Option<Arc<SharedSession>>,
    kind: OperationKind,
    admission: Option<OperationAdmission>,
    activity: Option<PoolActivity>,
}

struct PoolActivity {
    pool: Arc<PoolInner>,
}

impl Drop for PoolActivity {
    fn drop(&mut self) {
        self.pool.finish_activity();
    }
}

impl fmt::Debug for SessionLease {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SessionLease")
            .finish_non_exhaustive()
    }
}

impl SessionLease {
    /// The leased session's transport. A lease holds a claim on its shared
    /// session from checkout until `complete`/`retire`/drop releases it, so
    /// every request method below can assume it is still present.
    fn transport(&mut self) -> &dyn TransportSession {
        self.session
            .as_ref()
            .expect("lease always owns a session until completion")
            .transport
            .as_ref()
    }

    pub async fn read_object(
        &mut self,
        path: &std::path::Path,
        range: Option<object_store::GetRange>,
        head: bool,
    ) -> Result<RemoteObjectRead, TransportError> {
        self.transport().read_object(path, range, head).await
    }

    pub async fn list_directory(
        &mut self,
        path: &std::path::Path,
    ) -> Result<Vec<RemoteDirectoryEntry>, TransportError> {
        self.transport().list_directory(path).await
    }

    pub async fn remove_file(&mut self, path: &std::path::Path) -> Result<(), TransportError> {
        self.transport().remove_file(path).await
    }

    pub async fn remove_directory(&mut self, path: &std::path::Path) -> Result<(), TransportError> {
        self.transport().remove_directory(path).await
    }

    async fn ensure_directory_component(
        &mut self,
        path: &std::path::Path,
    ) -> Result<(), TransportError> {
        self.transport().ensure_directory_component(path).await
    }

    pub async fn write_file_durable(
        &mut self,
        path: &std::path::Path,
        chunks: Vec<Bytes>,
    ) -> Result<(), TransportError> {
        self.transport().write_file_durable(path, chunks).await
    }

    pub async fn write_file_at_durable(
        &mut self,
        path: &std::path::Path,
        offset: u64,
        chunks: Vec<Bytes>,
    ) -> Result<(), TransportError> {
        self.transport()
            .write_file_at_durable(path, offset, chunks)
            .await
    }

    pub async fn write_file_at(
        &mut self,
        path: &std::path::Path,
        offset: u64,
        chunks: Vec<Bytes>,
    ) -> Result<(), TransportError> {
        self.transport().write_file_at(path, offset, chunks).await
    }

    pub async fn read_exact(
        &mut self,
        path: &std::path::Path,
        offset: u64,
        len: usize,
    ) -> Result<Bytes, TransportError> {
        self.transport().read_exact(path, offset, len).await
    }

    pub async fn hard_link(
        &mut self,
        from: &std::path::Path,
        to: &std::path::Path,
    ) -> Result<(), TransportError> {
        self.transport().hard_link(from, to).await
    }

    pub async fn posix_rename(
        &mut self,
        from: &std::path::Path,
        to: &std::path::Path,
    ) -> Result<(), TransportError> {
        self.transport().posix_rename(from, to).await
    }

    pub async fn complete(mut self) -> Result<(), TransportError> {
        let session = self
            .session
            .take()
            .expect("lease always owns a session until completion");
        self.pool.release_session(&session, self.kind);
        drop(self.admission.take());
        drop(self.activity.take());
        Ok(())
    }

    pub async fn retire(mut self) -> Result<(), TransportError> {
        let session = self
            .session
            .take()
            .expect("lease always owns a session until retirement");
        // Marking the session broken removes it from placement; the close
        // itself happens once the last concurrent operation releases it.
        let pool = self.pool.clone();
        if self.kind == OperationKind::Write {
            session.active_writes.fetch_sub(1, Ordering::SeqCst);
        }
        pool.remove_from_roster(&session);
        let admission = self.admission.take();
        let activity = self.activity.take();
        let remaining = session.active_ops.fetch_sub(1, Ordering::SeqCst) - 1;
        // The close runs in an owned task so a canceled caller cannot abandon
        // it half-way; admission capacity stays held until it finishes.
        let cleanup = self.pool.tasks.spawn(async move {
            let result = if remaining == 0 {
                pool.close_shared_session(session).await
            } else {
                Ok(())
            };
            pool.roster_changed.notify_waiters();
            drop(admission);
            drop(activity);
            result
        });
        cleanup
            .await
            .map_err(|_| TransportError::Close("SFTP retirement task failed".to_owned()))?
    }

    #[cfg(test)]
    pub async fn finish<T, E>(
        self,
        operation: Result<T, E>,
        error_disposition: SessionDisposition,
    ) -> Result<T, LeaseFinishError<E>> {
        match operation {
            Ok(value) => {
                self.complete().await.map_err(LeaseFinishError::Lifecycle)?;
                Ok(value)
            }
            Err(error) => {
                let cleanup = match error_disposition {
                    SessionDisposition::Reuse => self.complete().await,
                    SessionDisposition::BrokenOrAmbiguous => self.retire().await,
                }
                .err();
                Err(LeaseFinishError::Operation { error, cleanup })
            }
        }
    }
}

impl Drop for SessionLease {
    fn drop(&mut self) {
        let Some(session) = self.session.take() else {
            return;
        };
        // A lease dropped without `complete` abandoned its operation mid
        // flight; the session's protocol state is ambiguous, so it must not
        // serve new operations.
        self.pool.remove_from_roster(&session);
        self.pool.release_session(&session, self.kind);
        drop(self.admission.take());
        drop(self.activity.take());
    }
}

#[cfg(test)]
mod tests {
    use super::{
        LeaseFinishError, OpenSshTransportSession, OperationKind, RemoteEntryKind,
        SFTP_READ_PACKET_SIZE, SFTP_READ_REQUEST_CONCURRENCY, SFTP_WRITE_PACKET_SIZE,
        SFTP_WRITE_REQUEST_CONCURRENCY, SessionDisposition, SessionFactory, SftpSessionPool,
        TransportError, TransportSession, plan_pipelined_reads, plan_pipelined_writes,
    };
    use crate::sftp_object_store::{ObjectHeader, SftpCapabilities, encode_header};
    use async_trait::async_trait;
    use bytes::Bytes;
    use std::fmt;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};
    use std::time::Duration;
    use tokio::sync::Notify;
    use tokio::time::Instant;
    use tokio_util::sync::CancellationToken;

    #[test]
    fn pipelined_write_plan_matches_rclone_packet_window_and_offsets() {
        let first = Bytes::from(vec![0x11; SFTP_WRITE_PACKET_SIZE + 3]);
        let second = Bytes::from_static(b"tail");

        let requests = plan_pipelined_writes(17, vec![first, second]).unwrap();

        assert_eq!(SFTP_WRITE_REQUEST_CONCURRENCY, 64);
        assert_eq!(requests.len(), 3);
        assert_eq!(requests[0].offset, 17);
        assert_eq!(requests[0].payload.len(), SFTP_WRITE_PACKET_SIZE);
        assert_eq!(requests[1].offset, 17 + SFTP_WRITE_PACKET_SIZE as u64);
        assert_eq!(requests[1].payload.len(), 3);
        assert_eq!(requests[2].offset, 20 + SFTP_WRITE_PACKET_SIZE as u64);
        assert_eq!(requests[2].payload.as_ref(), b"tail");
    }

    #[test]
    fn pipelined_read_plan_matches_rclone_packet_window_and_preserves_order() {
        let total_len = 2 * SFTP_READ_PACKET_SIZE + 17;

        let requests = plan_pipelined_reads(19, total_len).unwrap();

        assert_eq!(SFTP_READ_REQUEST_CONCURRENCY, 64);
        assert_eq!(requests.len(), 3);
        assert_eq!((requests[0].index, requests[0].offset), (0, 19));
        assert_eq!(requests[0].len, SFTP_READ_PACKET_SIZE);
        assert_eq!(
            (requests[1].index, requests[1].offset),
            (1, 19 + SFTP_READ_PACKET_SIZE as u64)
        );
        assert_eq!(requests[1].len, SFTP_READ_PACKET_SIZE);
        assert_eq!(
            (requests[2].index, requests[2].offset),
            (2, 19 + 2 * SFTP_READ_PACKET_SIZE as u64)
        );
        assert_eq!(requests[2].len, 17);
    }

    #[derive(Clone)]
    struct RecordingFactory {
        state: Arc<FactoryState>,
        capabilities: SftpCapabilities,
    }

    struct FactoryState {
        dials: AtomicUsize,
        live: AtomicUsize,
        peak: AtomicUsize,
        close_started: Notify,
        close_started_count: AtomicUsize,
        allow_close: Notify,
        block_close: AtomicUsize,
        open_started: Notify,
        allow_open: Notify,
        block_open_from: AtomicUsize,
        panic_open_from: AtomicUsize,
        fail_open_from: AtomicUsize,
        fail_close: AtomicUsize,
        panic_close: AtomicUsize,
        protocol_close_failure: AtomicUsize,
        protocol_close_failures: AtomicUsize,
    }

    impl fmt::Debug for RecordingFactory {
        fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.debug_struct("RecordingFactory").finish()
        }
    }

    impl RecordingFactory {
        fn new(capabilities: SftpCapabilities) -> Self {
            Self {
                state: Arc::new(FactoryState {
                    dials: AtomicUsize::new(0),
                    live: AtomicUsize::new(0),
                    peak: AtomicUsize::new(0),
                    close_started: Notify::new(),
                    close_started_count: AtomicUsize::new(0),
                    allow_close: Notify::new(),
                    block_close: AtomicUsize::new(0),
                    open_started: Notify::new(),
                    allow_open: Notify::new(),
                    block_open_from: AtomicUsize::new(0),
                    panic_open_from: AtomicUsize::new(0),
                    fail_open_from: AtomicUsize::new(0),
                    fail_close: AtomicUsize::new(0),
                    panic_close: AtomicUsize::new(0),
                    protocol_close_failure: AtomicUsize::new(0),
                    protocol_close_failures: AtomicUsize::new(0),
                }),
                capabilities,
            }
        }

        fn fully_capable() -> Self {
            Self::new(SftpCapabilities {
                fsync: true,
                hardlink: true,
                posix_rename: true,
            })
        }

        fn dials(&self) -> usize {
            self.state.dials.load(Ordering::SeqCst)
        }

        fn live(&self) -> usize {
            self.state.live.load(Ordering::SeqCst)
        }

        fn peak(&self) -> usize {
            self.state.peak.load(Ordering::SeqCst)
        }
    }

    #[async_trait]
    impl SessionFactory for RecordingFactory {
        async fn open(
            &self,
            _force: CancellationToken,
        ) -> Result<Box<dyn TransportSession>, TransportError> {
            let id = self.state.dials.fetch_add(1, Ordering::SeqCst) + 1;
            let live = self.state.live.fetch_add(1, Ordering::SeqCst) + 1;
            self.state.peak.fetch_max(live, Ordering::SeqCst);
            let block_open_from = self.state.block_open_from.load(Ordering::SeqCst);
            if block_open_from != 0 && id >= block_open_from {
                self.state.open_started.notify_one();
                self.state.allow_open.notified().await;
            }
            let panic_open_from = self.state.panic_open_from.load(Ordering::SeqCst);
            if panic_open_from != 0 && id >= panic_open_from {
                panic!("injected SFTP open owner panic");
            }
            let fail_open_from = self.state.fail_open_from.load(Ordering::SeqCst);
            if fail_open_from != 0 && id >= fail_open_from {
                self.state.live.fetch_sub(1, Ordering::SeqCst);
                return Err(TransportError::PoolClosed);
            }
            Ok(Box::new(RecordingSession {
                id,
                state: self.state.clone(),
                capabilities: self.capabilities,
            }))
        }
    }

    struct RecordingSession {
        id: usize,
        state: Arc<FactoryState>,
        capabilities: SftpCapabilities,
    }

    #[derive(Debug, Clone, Default)]
    struct ShutdownAwareFactory {
        live_processes: Arc<AtomicUsize>,
        process_exited: Arc<Notify>,
    }

    #[async_trait]
    impl SessionFactory for ShutdownAwareFactory {
        async fn open(
            &self,
            force: CancellationToken,
        ) -> Result<Box<dyn TransportSession>, TransportError> {
            self.live_processes.fetch_add(1, Ordering::SeqCst);
            let live_processes = self.live_processes.clone();
            let process_exited = self.process_exited.clone();
            tokio::spawn(async move {
                force.cancelled().await;
                live_processes.fetch_sub(1, Ordering::SeqCst);
                process_exited.notify_waiters();
            });
            Ok(Box::new(ShutdownAwareSession))
        }
    }

    #[derive(Debug)]
    struct ShutdownAwareSession;

    #[async_trait]
    impl TransportSession for ShutdownAwareSession {
        fn capabilities(&self) -> SftpCapabilities {
            SftpCapabilities {
                fsync: true,
                hardlink: true,
                posix_rename: true,
            }
        }

        async fn close(&self, _force: CancellationToken) -> Result<(), TransportError> {
            Ok(())
        }
    }

    #[derive(Debug, Clone, Default)]
    struct ForcedCleanupFactory {
        state: Arc<ForcedCleanupState>,
    }

    #[derive(Debug, Default)]
    struct ForcedCleanupState {
        live: AtomicUsize,
        force_seen: Notify,
        allow_cleanup: Notify,
    }

    #[async_trait]
    impl SessionFactory for ForcedCleanupFactory {
        async fn open(
            &self,
            _force: CancellationToken,
        ) -> Result<Box<dyn TransportSession>, TransportError> {
            self.state.live.fetch_add(1, Ordering::SeqCst);
            Ok(Box::new(ForcedCleanupSession {
                state: self.state.clone(),
            }))
        }
    }

    #[derive(Debug)]
    struct ForcedCleanupSession {
        state: Arc<ForcedCleanupState>,
    }

    #[async_trait]
    impl TransportSession for ForcedCleanupSession {
        fn capabilities(&self) -> SftpCapabilities {
            SftpCapabilities {
                fsync: true,
                hardlink: true,
                posix_rename: true,
            }
        }

        async fn close(&self, force: CancellationToken) -> Result<(), TransportError> {
            force.cancelled().await;
            self.state.force_seen.notify_one();
            self.state.allow_cleanup.notified().await;
            self.state.live.fetch_sub(1, Ordering::SeqCst);
            Ok(())
        }
    }

    impl fmt::Debug for RecordingSession {
        fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter
                .debug_struct("RecordingSession")
                .field("id", &self.id)
                .finish()
        }
    }

    #[async_trait]
    impl TransportSession for RecordingSession {
        fn capabilities(&self) -> SftpCapabilities {
            self.capabilities
        }

        async fn close(&self, _force: CancellationToken) -> Result<(), TransportError> {
            if self.state.block_close.load(Ordering::SeqCst) != 0 {
                self.state
                    .close_started_count
                    .fetch_add(1, Ordering::SeqCst);
                self.state.close_started.notify_one();
                self.state.allow_close.notified().await;
            }
            if self.state.panic_close.load(Ordering::SeqCst) != 0 {
                panic!("injected SFTP close owner panic");
            }
            if self.state.fail_close.load(Ordering::SeqCst) != 0 {
                return Err(TransportError::Close("forced close failure".to_owned()));
            }
            if self.state.protocol_close_failure.load(Ordering::SeqCst) != 0 {
                self.state
                    .protocol_close_failures
                    .fetch_add(1, Ordering::SeqCst);
            }
            self.state.live.fetch_sub(1, Ordering::SeqCst);
            Ok(())
        }
    }

    async fn pool(
        factory: RecordingFactory,
        shared: usize,
        reads: usize,
        writes: usize,
    ) -> SftpSessionPool {
        SftpSessionPool::new_writable(Arc::new(factory), shared, reads, writes)
            .await
            .expect("fully capable writable pool")
    }

    #[derive(Debug, Default)]
    struct FlakyDialState {
        dials: AtomicUsize,
        fail: AtomicBool,
    }

    #[derive(Debug, Clone)]
    struct FlakyDialFactory(Arc<FlakyDialState>);

    #[async_trait]
    impl SessionFactory for FlakyDialFactory {
        async fn open(
            &self,
            _force: CancellationToken,
        ) -> Result<Box<dyn TransportSession>, TransportError> {
            self.0.dials.fetch_add(1, Ordering::SeqCst);
            if self.0.fail.load(Ordering::SeqCst) {
                return Err(TransportError::Open(
                    "injected dial failure: backend session limit exceeded".to_owned(),
                ));
            }
            Ok(Box::new(FlakyDialSession))
        }
    }

    #[derive(Debug)]
    struct FlakyDialSession;

    #[async_trait]
    impl TransportSession for FlakyDialSession {
        fn capabilities(&self) -> SftpCapabilities {
            SftpCapabilities {
                fsync: true,
                hardlink: true,
                posix_rename: true,
            }
        }

        async fn close(&self, _force: CancellationToken) -> Result<(), TransportError> {
            Ok(())
        }
    }

    /// The vm100 pilot death spiral: a Storage Box at its concurrent-session
    /// cap kills SSH sessions, and every retrying caller redialing
    /// immediately keeps the account over the cap forever — the daemon loops
    /// in broken-pipe churn and never binds. Failed dials must back off on a
    /// shared schedule so the server can shed stale sessions, and one
    /// successful dial must reset the schedule.
    #[tokio::test(start_paused = true)]
    async fn failed_session_dials_back_off_instead_of_storming_the_server() {
        let state = Arc::new(FlakyDialState::default());
        let pool =
            SftpSessionPool::new_writable(Arc::new(FlakyDialFactory(state.clone())), 3, 2, 2)
                .await
                .expect("the pool opens its first session while the backend is healthy");
        assert_eq!(state.dials.load(Ordering::SeqCst), 1);

        // Retire the warm session so every checkout must dial: with a live
        // session in the roster a failed dial falls back to sharing it
        // instead of surfacing the error.
        state.fail.store(true, Ordering::SeqCst);
        pool.checkout(OperationKind::Write)
            .await
            .unwrap()
            .retire()
            .await
            .unwrap();
        let paced_from = Instant::now();
        for _ in 0..5 {
            let error = pool.checkout(OperationKind::Read).await.unwrap_err();
            assert!(matches!(error, TransportError::Open(_)), "{error:?}");
        }
        assert_eq!(state.dials.load(Ordering::SeqCst), 6);
        let paced = paced_from.elapsed();
        assert!(
            paced >= Duration::from_millis(1500),
            "five failed dials must be paced by the shared backoff \
             (100+200+400+800 ms), not fired back to back: {paced:?}"
        );

        // The backend sheds its stale sessions; the next (paced) dial
        // succeeds and resets the schedule.
        state.fail.store(false, Ordering::SeqCst);
        let recovered = pool.checkout(OperationKind::Read).await.unwrap();
        assert_eq!(state.dials.load(Ordering::SeqCst), 7);
        recovered.retire().await.unwrap();

        state.fail.store(true, Ordering::SeqCst);
        let reset_from = Instant::now();
        pool.checkout(OperationKind::Read).await.unwrap_err();
        assert_eq!(state.dials.load(Ordering::SeqCst), 8);
        assert!(
            reset_from.elapsed() < Duration::from_millis(100),
            "a successful dial must reset the backoff schedule"
        );
    }

    #[test]
    fn tracked_cleanup_uses_pool_runtime_outside_caller_runtime() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let pool = runtime.block_on(pool(RecordingFactory::fully_capable(), 1, 1, 1));

        let ran = Arc::new(AtomicUsize::new(0));
        let ran_in_task = ran.clone();
        let registration = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            pool.spawn_cleanup(std::path::Path::new("test-staging"), async move {
                ran_in_task.store(1, Ordering::SeqCst);
            });
        }));
        assert!(
            registration.is_ok(),
            "cleanup registration must use the pool runtime rather than the caller's context"
        );
        runtime.block_on(async {
            while ran.load(Ordering::SeqCst) == 0 {
                tokio::task::yield_now().await;
            }
            pool.shutdown().await.unwrap();
        });
    }

    #[tokio::test]
    async fn tracked_cleanup_is_rejected_after_pool_shutdown() {
        let pool = pool(RecordingFactory::fully_capable(), 1, 1, 1).await;
        pool.shutdown().await.unwrap();
        let ran = Arc::new(AtomicUsize::new(0));
        let ran_in_task = ran.clone();

        assert!(
            !pool.spawn_cleanup(std::path::Path::new("test-staging"), async move {
                ran_in_task.store(1, Ordering::SeqCst);
            })
        );
        tokio::task::yield_now().await;
        assert_eq!(ran.load(Ordering::SeqCst), 0);
        // The unrunnable cleanup leaves staging debris behind, which is the
        // caller's warning to log — not a failed shutdown. Every service stop
        // with pending staged uploads used to exit nonzero through this path.
        pool.shutdown()
            .await
            .expect("staging debris after close must not fail an otherwise clean shutdown");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn thirty_two_waiters_observe_exact_directional_caps_and_connection_ceiling() {
        let factory = RecordingFactory::fully_capable();
        let pool = Arc::new(pool(factory.clone(), 8, 7, 7).await);
        let mut held = Vec::new();
        for _ in 0..7 {
            held.push(pool.checkout(OperationKind::Read).await.unwrap());
        }
        for _ in 0..7 {
            held.push(pool.checkout(OperationKind::Write).await.unwrap());
        }
        let mut tasks = Vec::new();

        for index in 0..32 {
            let pool = pool.clone();
            tasks.push(tokio::spawn(async move {
                let kind = if index % 8 == 7 {
                    OperationKind::Write
                } else {
                    OperationKind::Read
                };
                let lease = pool.checkout(kind).await.unwrap();
                lease.complete().await.unwrap();
            }));
        }

        while pool.inner.admission.waiter_count() != 32 {
            tokio::task::yield_now().await;
        }
        assert!(tasks.iter().all(|task| !task.is_finished()));
        assert!(
            factory.peak() <= 8,
            "fourteen multiplexed operations never exceed the connection cap"
        );
        for lease in held {
            lease.complete().await.unwrap();
        }
        for task in tasks {
            task.await.unwrap();
        }
        assert!(factory.peak() <= 8);
    }

    #[tokio::test]
    async fn seven_reads_leave_one_shared_slot_for_a_write() {
        let factory = RecordingFactory::fully_capable();
        let pool = Arc::new(pool(factory.clone(), 8, 7, 7).await);
        let mut reads = Vec::new();
        for _ in 0..7 {
            reads.push(pool.checkout(OperationKind::Read).await.unwrap());
        }

        let eighth_read = tokio::spawn({
            let pool = pool.clone();
            async move { pool.checkout(OperationKind::Read).await }
        });
        while pool.inner.admission.waiter_count() != 1 {
            tokio::task::yield_now().await;
        }
        assert!(!eighth_read.is_finished());

        let write = tokio::time::timeout(
            std::time::Duration::from_secs(1),
            pool.checkout(OperationKind::Write),
        )
        .await
        .expect("write uses reserved slot")
        .unwrap();
        assert_eq!(factory.live(), 8);

        write.complete().await.unwrap();
        reads.pop().unwrap().complete().await.unwrap();
        eighth_read
            .await
            .unwrap()
            .unwrap()
            .complete()
            .await
            .unwrap();
        for read in reads {
            read.complete().await.unwrap();
        }
    }

    #[tokio::test]
    async fn seven_writes_leave_one_shared_slot_for_a_read() {
        let factory = RecordingFactory::fully_capable();
        let pool = Arc::new(pool(factory.clone(), 8, 7, 7).await);
        let mut writes = Vec::new();
        for _ in 0..7 {
            writes.push(pool.checkout(OperationKind::Write).await.unwrap());
        }

        let eighth_write = tokio::spawn({
            let pool = pool.clone();
            async move { pool.checkout(OperationKind::Write).await }
        });
        while pool.inner.admission.waiter_count() != 1 {
            tokio::task::yield_now().await;
        }
        assert!(!eighth_write.is_finished());

        let read = tokio::time::timeout(
            std::time::Duration::from_secs(1),
            pool.checkout(OperationKind::Read),
        )
        .await
        .expect("read uses reserved slot")
        .unwrap();
        assert_eq!(factory.live(), 8);

        read.complete().await.unwrap();
        writes.pop().unwrap().complete().await.unwrap();
        eighth_write
            .await
            .unwrap()
            .unwrap()
            .complete()
            .await
            .unwrap();
        for write in writes {
            write.complete().await.unwrap();
        }
    }

    #[tokio::test]
    async fn queued_writes_reclaim_read_slots_until_the_pool_reaches_seven_one() {
        let admission = super::FairAdmission::new(8, 7, 7, 8);
        let mut held_reads = Vec::new();
        for _ in 0..7 {
            held_reads.push(admission.acquire(OperationKind::Read).await.unwrap());
        }
        let held_write = admission.acquire(OperationKind::Write).await.unwrap();

        let mut queued_writes = Vec::new();
        for expected_waiters in 1..=6 {
            let task_admission = admission.clone();
            queued_writes.push(tokio::spawn(async move {
                task_admission.acquire(OperationKind::Write).await.unwrap()
            }));
            while admission.waiter_count() != expected_waiters {
                tokio::task::yield_now().await;
            }
        }

        let mut queued_reads = Vec::new();
        for expected_waiters in 7..=12 {
            let task_admission = admission.clone();
            queued_reads.push(tokio::spawn(async move {
                task_admission.acquire(OperationKind::Read).await.unwrap()
            }));
            while admission.waiter_count() != expected_waiters {
                tokio::task::yield_now().await;
            }
        }

        let mut promoted_writes = Vec::new();
        for queued_write in queued_writes {
            drop(held_reads.pop().unwrap());
            promoted_writes.push(
                tokio::time::timeout(std::time::Duration::from_secs(1), queued_write)
                    .await
                    .expect("writeback reclaims a released read slot up to seven writers")
                    .unwrap(),
            );
            assert!(queued_reads.iter().all(|waiter| !waiter.is_finished()));
        }

        drop(held_reads.pop().unwrap());
        let reserved_read =
            tokio::time::timeout(std::time::Duration::from_secs(1), queued_reads.remove(0))
                .await
                .expect("the eighth shared slot remains available to reads")
                .unwrap();

        drop(reserved_read);
        drop(held_write);
        drop(held_reads);
        drop(promoted_writes);
        for queued_read in queued_reads {
            drop(queued_read.await.unwrap());
        }
    }

    #[tokio::test]
    async fn queued_metadata_reserves_read_capacity_during_writeback_drain() {
        let admission = super::FairAdmission::new(8, 7, 7, 8);
        let mut held_writes = Vec::new();
        for _ in 0..7 {
            held_writes.push(admission.acquire(OperationKind::Write).await.unwrap());
        }
        let held_read = admission.acquire(OperationKind::Read).await.unwrap();

        let queued_write = tokio::spawn({
            let admission = admission.clone();
            async move { admission.acquire(OperationKind::Write).await.unwrap() }
        });
        while admission.waiter_count() != 1 {
            tokio::task::yield_now().await;
        }
        let queued_metadata = tokio::spawn({
            let admission = admission.clone();
            async move { admission.acquire(OperationKind::Metadata).await.unwrap() }
        });
        while admission.waiter_count() != 2 {
            tokio::task::yield_now().await;
        }

        drop(held_writes.pop().unwrap());
        let replacement_write =
            tokio::time::timeout(std::time::Duration::from_secs(1), queued_write)
                .await
                .expect("writeback retains its released writer slot")
                .unwrap();
        assert!(!queued_metadata.is_finished());

        drop(held_read);
        let metadata = tokio::time::timeout(std::time::Duration::from_secs(1), queued_metadata)
            .await
            .expect("metadata shares reserved foreground read capacity")
            .unwrap();

        drop(metadata);
        drop(replacement_write);
        drop(held_writes);
    }

    #[tokio::test]
    async fn canceling_the_opposite_waiter_restores_idle_direction_burst_capacity() {
        let admission = super::FairAdmission::new(8, 7, 7, 8);
        let mut held_writes = Vec::new();
        for _ in 0..7 {
            held_writes.push(admission.acquire(OperationKind::Write).await.unwrap());
        }
        let held_read = admission.acquire(OperationKind::Read).await.unwrap();

        let queued_read = tokio::spawn({
            let admission = admission.clone();
            async move { admission.acquire(OperationKind::Read).await.unwrap() }
        });
        while admission.waiter_count() != 1 {
            tokio::task::yield_now().await;
        }
        let queued_write = tokio::spawn({
            let admission = admission.clone();
            async move { admission.acquire(OperationKind::Write).await.unwrap() }
        });
        while admission.waiter_count() != 2 {
            tokio::task::yield_now().await;
        }

        queued_read.abort();
        assert!(matches!(queued_read.await, Err(error) if error.is_cancelled()));
        while admission.waiter_count() != 1 {
            tokio::task::yield_now().await;
        }
        drop(held_writes.pop().unwrap());
        let expanded_write = tokio::time::timeout(std::time::Duration::from_secs(1), queued_write)
            .await
            .expect("cancelled read demand no longer reserves write capacity")
            .unwrap();

        drop(expanded_write);
        drop(held_read);
        drop(held_writes);
    }

    #[tokio::test]
    async fn cross_direction_waiters_stay_fifo_without_a_reserved_slot() {
        let admission = super::FairAdmission::new(1, 1, 1, 1);
        let held = admission.acquire(OperationKind::Read).await.unwrap();
        let order = Arc::new(Mutex::new(Vec::new()));
        let mut tasks = Vec::new();

        for (position, kind) in [
            OperationKind::Write,
            OperationKind::Read,
            OperationKind::Write,
            OperationKind::Metadata,
        ]
        .into_iter()
        .enumerate()
        {
            let task_admission = admission.clone();
            let order = order.clone();
            tasks.push(tokio::spawn(async move {
                let permit = task_admission.acquire(kind).await.unwrap();
                order.lock().unwrap().push(position);
                drop(permit);
            }));
            tokio::time::timeout(std::time::Duration::from_secs(1), async {
                while admission.waiter_count() != position + 1 {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("waiter is queued in deterministic order");
        }

        drop(held);
        for task in tasks {
            task.await.unwrap();
        }
        assert_eq!(*order.lock().unwrap(), vec![0, 1, 2, 3]);
    }

    #[tokio::test]
    async fn metadata_admission_does_not_queue_behind_the_read_limit() {
        let factory = RecordingFactory::fully_capable();
        let pool = Arc::new(pool(factory, 2, 1, 1).await);
        let first = pool.checkout(OperationKind::Read).await.unwrap();

        let queued_read = tokio::spawn({
            let pool = pool.clone();
            async move { pool.checkout(OperationKind::Read).await }
        });
        while pool.inner.admission.waiter_count() != 1 {
            tokio::task::yield_now().await;
        }

        // Metadata has its own admission budget: a storm of one-round-trip
        // calls proceeds while the read class is saturated.
        let metadata = tokio::time::timeout(
            std::time::Duration::from_secs(1),
            pool.checkout(OperationKind::Metadata),
        )
        .await
        .expect("metadata admits while the read limit is exhausted")
        .unwrap();
        assert!(!queued_read.is_finished());

        metadata.complete().await.unwrap();
        first.complete().await.unwrap();
        queued_read
            .await
            .unwrap()
            .unwrap()
            .complete()
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn canceled_waiter_does_not_dial_or_consume_admission() {
        let factory = RecordingFactory::fully_capable();
        let pool = Arc::new(pool(factory.clone(), 1, 1, 1).await);
        let held = pool.checkout(OperationKind::Read).await.unwrap();
        // A second read queues in admission (read limit 1); metadata would
        // multiplex onto the held session immediately.
        let waiter = tokio::spawn({
            let pool = pool.clone();
            async move { pool.checkout(OperationKind::Read).await }
        });
        while pool.inner.admission.waiter_count() != 1 {
            tokio::task::yield_now().await;
        }
        waiter.abort();
        assert!(waiter.await.unwrap_err().is_cancelled());
        assert_eq!(factory.dials(), 1);

        held.complete().await.unwrap();
        pool.checkout(OperationKind::Read)
            .await
            .unwrap()
            .complete()
            .await
            .unwrap();
        assert_eq!(factory.dials(), 1);
    }

    #[tokio::test]
    async fn canceled_operation_retires_session_and_next_operation_reconnects() {
        let factory = RecordingFactory::fully_capable();
        let pool = Arc::new(pool(factory.clone(), 1, 1, 1).await);
        let checked_out = Arc::new(Notify::new());
        let task = tokio::spawn({
            let pool = pool.clone();
            let checked_out = checked_out.clone();
            async move {
                let _lease = pool.checkout(OperationKind::Write).await.unwrap();
                checked_out.notify_one();
                std::future::pending::<()>().await;
            }
        });
        checked_out.notified().await;
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());

        pool.checkout(OperationKind::Write)
            .await
            .unwrap()
            .complete()
            .await
            .unwrap();
        assert_eq!(factory.dials(), 2);
    }

    #[tokio::test]
    async fn concurrent_operations_multiplex_onto_one_connection() {
        let factory = RecordingFactory::fully_capable();
        let pool = Arc::new(pool(factory.clone(), 1, 4, 4).await);
        let mut leases = Vec::new();
        for _ in 0..4 {
            leases.push(
                tokio::time::timeout(
                    std::time::Duration::from_secs(1),
                    pool.checkout(OperationKind::Read),
                )
                .await
                .expect("operations beyond the connection count share the session")
                .unwrap(),
            );
        }
        assert_eq!(factory.dials(), 1);
        for lease in leases {
            lease.complete().await.unwrap();
        }
        pool.checkout(OperationKind::Read)
            .await
            .unwrap()
            .complete()
            .await
            .unwrap();
        assert_eq!(
            factory.dials(),
            1,
            "sessions released by multiplexed leases are reused"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn pending_dials_do_not_stack_excess_writes_on_the_first_session() {
        let factory = RecordingFactory::fully_capable();
        let pool = Arc::new(pool(factory.clone(), 4, 8, 8).await);
        let first = pool.checkout(OperationKind::Write).await.unwrap();
        factory.state.block_open_from.store(2, Ordering::SeqCst);

        let mut tasks = Vec::new();
        for _ in 0..7 {
            let pool = pool.clone();
            tasks.push(tokio::spawn(async move {
                pool.checkout(OperationKind::Write).await.unwrap()
            }));
        }
        tokio::time::timeout(
            std::time::Duration::from_secs(1),
            factory.state.open_started.notified(),
        )
        .await
        .expect("the first expansion dial reaches the controlled pause");

        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(100), async {
                loop {
                    if tasks.iter().any(tokio::task::JoinHandle::is_finished) {
                        return;
                    }
                    tokio::task::yield_now().await;
                }
            })
            .await
            .is_err(),
            "uploads beyond the connection cap must wait for pending dials instead of stacking on the first session"
        );

        factory.state.block_open_from.store(0, Ordering::SeqCst);
        factory.state.allow_open.notify_waiters();
        let mut leases = vec![first];
        for task in tasks {
            leases.push(task.await.unwrap());
        }
        let mut writes_per_session = pool
            .inner
            .roster
            .lock()
            .unwrap()
            .iter()
            .map(|session| session.active_writes.load(Ordering::SeqCst))
            .collect::<Vec<_>>();
        writes_per_session.sort_unstable();
        assert_eq!(writes_per_session, [2, 2, 2, 2]);

        for lease in leases {
            lease.complete().await.unwrap();
        }
        pool.shutdown().await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn retirement_marks_broken_only_while_removing_from_the_roster() {
        let factory = RecordingFactory::fully_capable();
        let pool = Arc::new(pool(factory, 1, 1, 1).await);
        let lease = pool.checkout(OperationKind::Write).await.unwrap();
        let session = lease.session.as_ref().unwrap().clone();
        let roster = pool.inner.roster.lock().unwrap();

        let (started_tx, started_rx) = std::sync::mpsc::channel();
        let retiring = tokio::spawn(async move {
            started_tx.send(()).unwrap();
            lease.retire().await
        });
        started_rx
            .recv_timeout(std::time::Duration::from_secs(1))
            .expect("retirement task started");
        for _ in 0..100 {
            if session.broken.load(Ordering::SeqCst) {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(1));
        }

        let published_before_removal = session.broken.load(Ordering::SeqCst);
        drop(roster);
        retiring.await.unwrap().unwrap();
        assert!(
            !published_before_removal,
            "placement can claim a session while retirement has marked it broken but not removed it"
        );
    }

    #[tokio::test]
    async fn canceled_open_keeps_lifetime_capacity_until_opened_session_closes() {
        let factory = RecordingFactory::fully_capable();
        let pool = Arc::new(pool(factory.clone(), 2, 2, 2).await);
        let held = pool.checkout(OperationKind::Read).await.unwrap();
        factory.state.block_open_from.store(2, Ordering::SeqCst);

        let opening = tokio::spawn({
            let pool = pool.clone();
            async move { pool.checkout(OperationKind::Write).await }
        });
        tokio::time::timeout(
            std::time::Duration::from_secs(1),
            factory.state.open_started.notified(),
        )
        .await
        .expect("second dial reaches blocked open");
        opening.abort();
        assert!(opening.await.unwrap_err().is_cancelled());

        // The abandoned dial still holds a lifetime permit, so a replacement
        // write multiplexes onto the held session instead of over-dialing.
        let replacement = tokio::time::timeout(
            std::time::Duration::from_secs(1),
            pool.checkout(OperationKind::Write),
        )
        .await
        .expect("replacement multiplexes while the canceled open is pending")
        .unwrap();
        assert_eq!(factory.dials(), 2);
        assert_eq!(factory.peak(), 2);
        assert_eq!(pool.inner.shared.available_permits(), 0);

        factory.state.block_close.store(1, Ordering::SeqCst);
        factory.state.block_open_from.store(0, Ordering::SeqCst);
        factory.state.allow_open.notify_waiters();
        tokio::time::timeout(
            std::time::Duration::from_secs(1),
            factory.state.close_started.notified(),
        )
        .await
        .expect("abandoned opened session reaches blocked cleanup close");
        tokio::task::yield_now().await;
        assert_eq!(factory.dials(), 2);
        assert_eq!(factory.live(), 2);
        assert_eq!(
            pool.inner.shared.available_permits(),
            0,
            "the abandoned session keeps its lifetime permit until its close finishes"
        );

        factory.state.block_close.store(0, Ordering::SeqCst);
        factory.state.allow_close.notify_waiters();
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            while pool.inner.shared.available_permits() != 1 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("closing the abandoned session releases its lifetime permit");
        assert_eq!(factory.live(), 1);
        assert_eq!(factory.peak(), 2);
        replacement.complete().await.unwrap();
        held.complete().await.unwrap();
    }

    #[tokio::test]
    async fn canceled_open_owner_panic_closes_pool_before_capacity_is_released() {
        let factory = RecordingFactory::fully_capable();
        let pool = Arc::new(pool(factory.clone(), 2, 2, 2).await);
        let held = pool.checkout(OperationKind::Read).await.unwrap();
        factory.state.block_open_from.store(2, Ordering::SeqCst);
        factory.state.panic_open_from.store(2, Ordering::SeqCst);

        let opening = tokio::spawn({
            let pool = pool.clone();
            async move { pool.checkout(OperationKind::Write).await }
        });
        tokio::time::timeout(
            std::time::Duration::from_secs(1),
            factory.state.open_started.notified(),
        )
        .await
        .expect("second dial reached the injected open panic gate");
        opening.abort();
        assert!(opening.await.unwrap_err().is_cancelled());

        factory.state.allow_open.notify_waiters();
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            while !pool.inner.closed.load(Ordering::SeqCst) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("owner unwind closes the pool");

        assert!(matches!(
            pool.checkout(OperationKind::Write).await,
            Err(TransportError::PoolClosed)
        ));
        assert_eq!(factory.dials(), 2, "owner panic must not permit a redial");
        held.retire().await.unwrap();
    }

    #[tokio::test]
    async fn canceled_close_owner_panic_closes_pool_before_capacity_is_released() {
        let factory = RecordingFactory::fully_capable();
        let pool = Arc::new(pool(factory.clone(), 1, 1, 1).await);
        let session = pool
            .inner
            .roster
            .lock()
            .unwrap()
            .pop()
            .expect("constructor leaves one warm session");
        factory.state.block_close.store(1, Ordering::SeqCst);
        factory.state.panic_close.store(1, Ordering::SeqCst);

        let closing = tokio::spawn({
            let pool = pool.clone();
            async move { pool.inner.close_shared_session(session).await }
        });
        tokio::time::timeout(
            std::time::Duration::from_secs(1),
            factory.state.close_started.notified(),
        )
        .await
        .expect("close reached the injected panic gate");
        closing.abort();
        assert!(closing.await.unwrap_err().is_cancelled());

        factory.state.allow_close.notify_waiters();
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            while !pool.inner.closed.load(Ordering::SeqCst) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("owner unwind closes the pool");

        assert!(matches!(
            pool.checkout(OperationKind::Write).await,
            Err(TransportError::PoolClosed)
        ));
        assert_eq!(factory.dials(), 1, "owner panic must not permit a redial");
    }

    #[tokio::test(start_paused = true)]
    async fn forever_pending_open_times_out_and_closes_admission() {
        let factory = RecordingFactory::fully_capable();
        let pool = pool(factory.clone(), 2, 2, 2).await;
        let held = pool.checkout(OperationKind::Read).await.unwrap();
        factory.state.block_open_from.store(2, Ordering::SeqCst);

        let open_started = factory.state.open_started.notified();
        let opening = tokio::spawn({
            let pool = pool.clone();
            async move { pool.checkout(OperationKind::Write).await }
        });
        open_started.await;
        tokio::time::advance(std::time::Duration::from_secs(31)).await;
        tokio::task::yield_now().await;
        let opening = tokio::time::timeout(std::time::Duration::from_secs(1), opening).await;

        assert!(matches!(
            opening,
            Ok(Ok(Err(TransportError::Open(ref message)))) if message == "SFTP session open timed out after 30s"
        ));
        assert!(matches!(
            pool.checkout(OperationKind::Read).await,
            Err(TransportError::PoolClosed)
        ));
        assert_eq!(factory.dials(), 2);
        held.retire().await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn canceled_forever_pending_open_still_expires_owner_deadline() {
        let factory = RecordingFactory::fully_capable();
        let pool = pool(factory.clone(), 2, 2, 2).await;
        let held = pool.checkout(OperationKind::Read).await.unwrap();
        factory.state.block_open_from.store(2, Ordering::SeqCst);

        let open_started = factory.state.open_started.notified();
        let opening = tokio::spawn({
            let pool = pool.clone();
            async move { pool.checkout(OperationKind::Write).await }
        });
        open_started.await;
        opening.abort();
        assert!(opening.await.unwrap_err().is_cancelled());

        tokio::time::advance(std::time::Duration::from_secs(31)).await;
        for _ in 0..4 {
            tokio::task::yield_now().await;
        }
        assert!(pool.inner.closed.load(Ordering::SeqCst));
        assert_eq!(pool.inner.shared.available_permits(), 1);

        held.retire().await.unwrap();
    }

    #[tokio::test]
    async fn ambiguous_partial_open_cleanup_fails_the_actual_pool_closed() {
        let factory = RecordingFactory::fully_capable();
        let pool = pool(factory.clone(), 2, 2, 2).await;
        let held = pool.checkout(OperationKind::Read).await.unwrap();
        factory.state.fail_open_from.store(2, Ordering::SeqCst);

        assert!(matches!(
            pool.checkout(OperationKind::Write).await,
            Err(TransportError::PoolClosed)
        ));
        assert!(pool.inner.closed.load(Ordering::SeqCst));
        assert!(matches!(
            pool.checkout(OperationKind::Read).await,
            Err(TransportError::PoolClosed)
        ));
        assert_eq!(factory.dials(), 2);

        held.retire().await.unwrap();
        pool.shutdown().await.unwrap();
        assert_eq!(factory.live(), 0);
    }

    #[tokio::test]
    async fn blocking_close_keeps_shared_lifetime_permit_until_close_finishes() {
        let factory = RecordingFactory::fully_capable();
        let pool = Arc::new(pool(factory.clone(), 1, 1, 1).await);
        factory.state.block_close.store(1, Ordering::SeqCst);
        let lease = pool.checkout(OperationKind::Write).await.unwrap();
        let retiring = tokio::spawn(async move { lease.retire().await });
        tokio::time::timeout(
            std::time::Duration::from_secs(1),
            factory.state.close_started.notified(),
        )
        .await
        .expect("retirement reaches blocked close");

        let reconnect = tokio::spawn({
            let pool = pool.clone();
            async move { pool.checkout(OperationKind::Write).await }
        });
        tokio::task::yield_now().await;
        assert!(!reconnect.is_finished());
        assert_eq!(factory.live(), 1);
        assert_eq!(factory.peak(), 1);

        factory.state.block_close.store(0, Ordering::SeqCst);
        factory.state.allow_close.notify_waiters();
        retiring.await.unwrap().unwrap();
        reconnect.await.unwrap().unwrap().complete().await.unwrap();
        assert_eq!(factory.peak(), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn forever_pending_close_times_out_and_closes_admission() {
        let factory = RecordingFactory::fully_capable();
        let pool = pool(factory.clone(), 1, 1, 1).await;
        factory.state.block_close.store(1, Ordering::SeqCst);
        let lease = pool.checkout(OperationKind::Write).await.unwrap();

        let retiring =
            tokio::time::timeout(std::time::Duration::from_secs(17), lease.retire()).await;

        assert!(matches!(
            retiring,
            Ok(Err(TransportError::Close(ref message))) if message == "forced SFTP session cleanup timed out after 5s"
        ));
        assert!(matches!(
            pool.checkout(OperationKind::Write).await,
            Err(TransportError::PoolClosed)
        ));
        assert_eq!(factory.dials(), 1);
        assert_eq!(
            pool.inner.shared.available_permits(),
            0,
            "a still-live session keeps its lifetime permit after the caller timeout"
        );
        assert_eq!(
            pool.inner
                .admission
                .inner
                .state
                .lock()
                .unwrap()
                .active_writes,
            0
        );

        let shutdown = tokio::spawn({
            let pool = pool.clone();
            async move { pool.shutdown().await }
        });
        tokio::time::advance(std::time::Duration::from_secs(46)).await;
        assert!(matches!(
            shutdown.await.unwrap(),
            Err(TransportError::Close(ref message)) if message == "SFTP pool shutdown timed out after 45s"
        ));
        assert_eq!(factory.live(), 1);

        factory.state.block_close.store(0, Ordering::SeqCst);
        factory.state.allow_close.notify_waiters();
        while factory.live() != 0 {
            tokio::task::yield_now().await;
        }
        assert!(pool.shutdown().await.is_err());
    }

    #[tokio::test(start_paused = true)]
    async fn successful_forced_cleanup_keeps_pool_open_and_holds_lifetime_capacity() {
        let factory = ForcedCleanupFactory::default();
        let pool = Arc::new(
            SftpSessionPool::new_writable(Arc::new(factory.clone()), 1, 1, 1)
                .await
                .unwrap(),
        );
        let lease = pool.checkout(OperationKind::Write).await.unwrap();
        let force_seen = factory.state.force_seen.notified();
        let retiring = tokio::spawn(async move { lease.retire().await });

        tokio::time::advance(std::time::Duration::from_secs(11)).await;
        force_seen.await;

        assert!(!pool.inner.closed.load(Ordering::SeqCst));
        assert_eq!(pool.inner.shared.available_permits(), 0);
        assert_eq!(factory.state.live.load(Ordering::SeqCst), 1);
        assert!(!retiring.is_finished());

        factory.state.allow_cleanup.notify_one();
        retiring.await.unwrap().unwrap();
        assert!(!pool.inner.closed.load(Ordering::SeqCst));
        assert_eq!(factory.state.live.load(Ordering::SeqCst), 0);

        pool.checkout(OperationKind::Write)
            .await
            .unwrap()
            .complete()
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn canceled_retire_keeps_admission_and_lifetime_capacity_until_close_finishes() {
        let factory = RecordingFactory::fully_capable();
        let pool = Arc::new(pool(factory.clone(), 1, 1, 1).await);
        factory.state.block_close.store(1, Ordering::SeqCst);
        let lease = pool.checkout(OperationKind::Write).await.unwrap();
        let retiring = tokio::spawn(async move { lease.retire().await });
        tokio::time::timeout(
            std::time::Duration::from_secs(1),
            factory.state.close_started.notified(),
        )
        .await
        .expect("retirement reaches blocked close");
        retiring.abort();
        assert!(retiring.await.unwrap_err().is_cancelled());

        let replacement = tokio::spawn({
            let pool = pool.clone();
            async move { pool.checkout(OperationKind::Write).await }
        });
        tokio::task::yield_now().await;
        assert!(!replacement.is_finished());
        assert_eq!(factory.dials(), 1);
        assert_eq!(factory.live(), 1);

        factory.state.block_close.store(0, Ordering::SeqCst);
        factory.state.allow_close.notify_waiters();
        tokio::time::timeout(std::time::Duration::from_secs(1), replacement)
            .await
            .expect("replacement proceeds after canceled retirement finishes")
            .unwrap()
            .unwrap()
            .complete()
            .await
            .unwrap();
        assert_eq!(factory.dials(), 2);
        assert_eq!(factory.peak(), 1);
    }

    #[tokio::test]
    async fn successful_operation_finish_reuses_session_without_redial() {
        let factory = RecordingFactory::fully_capable();
        let pool = pool(factory.clone(), 1, 1, 1).await;
        let value = pool
            .checkout(OperationKind::Read)
            .await
            .unwrap()
            .finish(Ok::<_, &'static str>(42), SessionDisposition::Reuse)
            .await
            .unwrap();
        assert_eq!(value, 42);

        pool.checkout(OperationKind::Read)
            .await
            .unwrap()
            .complete()
            .await
            .unwrap();
        assert_eq!(factory.dials(), 1);
    }

    #[tokio::test]
    async fn genuine_broken_operation_error_retires_before_redial_and_is_preserved() {
        let factory = RecordingFactory::fully_capable();
        let pool = Arc::new(pool(factory.clone(), 1, 1, 1).await);
        factory.state.block_close.store(1, Ordering::SeqCst);
        let lease = pool.checkout(OperationKind::Write).await.unwrap();
        let finishing = tokio::spawn(async move {
            let operation: Result<(), &'static str> = Err("connection lost after write request");
            lease
                .finish(operation, SessionDisposition::BrokenOrAmbiguous)
                .await
        });
        tokio::time::timeout(
            std::time::Duration::from_secs(1),
            factory.state.close_started.notified(),
        )
        .await
        .expect("broken operation reaches blocked retirement close");

        let replacement = tokio::spawn({
            let pool = pool.clone();
            async move { pool.checkout(OperationKind::Write).await }
        });
        tokio::task::yield_now().await;
        assert!(!replacement.is_finished());
        assert_eq!(factory.dials(), 1);

        factory.state.block_close.store(0, Ordering::SeqCst);
        factory.state.allow_close.notify_waiters();
        let failure = finishing.await.unwrap().unwrap_err();
        match failure {
            LeaseFinishError::Operation { error, cleanup } => {
                assert_eq!(error, "connection lost after write request");
                assert!(cleanup.is_none());
            }
            LeaseFinishError::Lifecycle(error) => panic!("operation error was lost: {error}"),
        }
        replacement
            .await
            .unwrap()
            .unwrap()
            .complete()
            .await
            .unwrap();
        assert_eq!(factory.dials(), 2);
        assert_eq!(factory.peak(), 1);
    }

    #[tokio::test]
    async fn close_failure_with_live_master_fails_closed_without_redial() {
        let factory = RecordingFactory::fully_capable();
        let pool = pool(factory.clone(), 1, 1, 1).await;
        factory.state.fail_close.store(1, Ordering::SeqCst);
        let operation: Result<(), &'static str> = Err("ambiguous remote mutation");

        let failure = pool
            .checkout(OperationKind::Write)
            .await
            .unwrap()
            .finish(operation, SessionDisposition::BrokenOrAmbiguous)
            .await
            .unwrap_err();

        match failure {
            LeaseFinishError::Operation { error, cleanup } => {
                assert_eq!(error, "ambiguous remote mutation");
                assert!(matches!(
                    cleanup,
                    Some(TransportError::Close(ref message)) if message == "forced close failure"
                ));
            }
            LeaseFinishError::Lifecycle(error) => panic!("operation error was lost: {error}"),
        }
        assert_eq!(factory.live(), 1);
        assert!(matches!(
            pool.checkout(OperationKind::Write).await,
            Err(TransportError::PoolClosed)
        ));
        assert_eq!(factory.dials(), 1);
    }

    #[tokio::test]
    async fn concurrent_operation_cleanup_finishes_before_terminal_shutdown_returns() {
        let factory = RecordingFactory::fully_capable();
        let pool = pool(factory.clone(), 3, 3, 3).await;
        let mut leases = Vec::new();
        for _ in 0..3 {
            leases.push(pool.checkout(OperationKind::Write).await.unwrap());
        }
        factory
            .state
            .protocol_close_failure
            .store(1, Ordering::SeqCst);
        factory.state.block_close.store(1, Ordering::SeqCst);

        let cleanups = leases
            .into_iter()
            .map(|lease| {
                tokio::spawn(async move {
                    lease
                        .finish(
                            Err::<(), _>("ambiguous final-flush write"),
                            SessionDisposition::BrokenOrAmbiguous,
                        )
                        .await
                })
            })
            .collect::<Vec<_>>();
        while factory.state.close_started_count.load(Ordering::SeqCst) != 3 {
            tokio::task::yield_now().await;
        }
        let shutting_down = tokio::spawn({
            let pool = pool.clone();
            async move { pool.shutdown().await }
        });
        while !pool.inner.closed.load(Ordering::SeqCst) {
            tokio::task::yield_now().await;
        }
        assert!(!shutting_down.is_finished());

        factory.state.block_close.store(0, Ordering::SeqCst);
        factory.state.allow_close.notify_waiters();
        for cleanup in cleanups {
            let failure = cleanup.await.unwrap().unwrap_err();
            assert!(matches!(
                failure,
                LeaseFinishError::Operation { cleanup: None, .. }
            ));
        }

        let shutdown = tokio::time::timeout(std::time::Duration::from_secs(1), shutting_down)
            .await
            .expect("terminal shutdown is reachable after concurrent cleanup")
            .unwrap();
        shutdown.unwrap();
        assert_eq!(factory.dials(), 3);
        assert_eq!(factory.live(), 0);
        assert_eq!(
            factory.state.protocol_close_failures.load(Ordering::SeqCst),
            3
        );
        assert!(matches!(
            pool.checkout(OperationKind::Write).await,
            Err(TransportError::PoolClosed)
        ));
    }

    #[tokio::test]
    async fn missing_each_publication_capability_refuses_writable_pool() {
        for (capabilities, missing) in [
            (
                SftpCapabilities {
                    fsync: false,
                    hardlink: true,
                    posix_rename: true,
                },
                "fsync@openssh.com",
            ),
            (
                SftpCapabilities {
                    fsync: true,
                    hardlink: false,
                    posix_rename: true,
                },
                "hardlink@openssh.com",
            ),
            (
                SftpCapabilities {
                    fsync: true,
                    hardlink: true,
                    posix_rename: false,
                },
                "posix-rename@openssh.com",
            ),
        ] {
            let error = SftpSessionPool::new_writable(
                Arc::new(RecordingFactory::new(capabilities)),
                8,
                7,
                7,
            )
            .await
            .expect_err("writable construction must fail closed");
            assert_eq!(
                error.to_string(),
                format!("SFTP server lacks required {missing} extension")
            );
        }
    }

    #[tokio::test]
    async fn writable_pool_accepts_limits_from_sftp_config() {
        let factory = RecordingFactory::fully_capable();
        let config = crate::config::SftpConfig {
            identity_file: "/tmp/id-ed25519".into(),
            known_hosts: "/tmp/known-hosts".into(),
            max_connections: 3,
            read_concurrency: 2,
            write_concurrency: 1,
            segment_size_mib: 32,
            read_cache_part_size_kib: 1024,
            ..Default::default()
        };
        let pool = SftpSessionPool::from_config_writable(Arc::new(factory.clone()), &config)
            .await
            .unwrap();
        let mut leases = Vec::new();
        for _ in 0..2 {
            leases.push(pool.checkout(OperationKind::Read).await.unwrap());
        }
        leases.push(pool.checkout(OperationKind::Write).await.unwrap());
        assert_eq!(factory.peak(), 3);
        for lease in leases {
            lease.complete().await.unwrap();
        }
    }

    #[tokio::test]
    async fn writable_config_warms_the_full_connection_budget_before_returning() {
        let factory = RecordingFactory::fully_capable();
        let config = crate::config::SftpConfig {
            identity_file: "/tmp/id-ed25519".into(),
            known_hosts: "/tmp/known-hosts".into(),
            max_connections: 4,
            read_concurrency: 8,
            write_concurrency: 8,
            segment_size_mib: 32,
            read_cache_part_size_kib: 1024,
            ..Default::default()
        };

        let pool = SftpSessionPool::from_config_writable(Arc::new(factory.clone()), &config)
            .await
            .unwrap();

        assert_eq!(factory.dials(), 4);
        assert_eq!(factory.live(), 4);
        assert_eq!(pool.inner.roster.lock().unwrap().len(), 4);
        pool.shutdown().await.unwrap();
        assert_eq!(factory.live(), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn idle_reaper_closes_expired_sessions_but_keeps_one_warm() {
        let factory = RecordingFactory::fully_capable();
        let pool = pool(factory.clone(), 3, 3, 3).await;
        let mut leases = Vec::new();
        for _ in 0..3 {
            leases.push(pool.checkout(OperationKind::Read).await.unwrap());
        }
        for lease in leases {
            lease.complete().await.unwrap();
        }
        assert_eq!(factory.live(), 3);

        tokio::time::advance(std::time::Duration::from_secs(59)).await;
        tokio::task::yield_now().await;
        assert_eq!(factory.live(), 3);

        tokio::time::advance(std::time::Duration::from_secs(1)).await;
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            while factory.live() != 1 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("expired idle sessions close on the reap tick");
        assert_eq!(factory.live(), 1);

        pool.checkout(OperationKind::Read)
            .await
            .unwrap()
            .complete()
            .await
            .unwrap();
        assert_eq!(factory.dials(), 3);
    }

    #[tokio::test]
    async fn shutdown_wakes_queued_checkout_and_drains_returned_active_session() {
        let factory = RecordingFactory::fully_capable();
        let pool = pool(factory.clone(), 1, 1, 1).await;
        // Hold the single write slot so the second write queues in admission
        // instead of multiplexing onto the busy session.
        let held = pool.checkout(OperationKind::Write).await.unwrap();
        let waiter = tokio::spawn({
            let pool = pool.clone();
            async move { pool.checkout(OperationKind::Write).await }
        });
        while pool.inner.admission.waiter_count() != 1 {
            tokio::task::yield_now().await;
        }

        let shutting_down = tokio::spawn({
            let pool = pool.clone();
            async move { pool.shutdown().await }
        });
        assert!(matches!(
            waiter.await.unwrap(),
            Err(TransportError::PoolClosed)
        ));
        assert!(!shutting_down.is_finished());

        held.complete().await.unwrap();
        shutting_down.await.unwrap().unwrap();
        assert_eq!(factory.live(), 0);
        assert!(matches!(
            pool.checkout(OperationKind::Read).await,
            Err(TransportError::PoolClosed)
        ));
    }

    #[tokio::test]
    async fn shutdown_closes_every_idle_session_and_is_idempotent() {
        let factory = RecordingFactory::fully_capable();
        let pool = pool(factory.clone(), 3, 3, 3).await;
        let mut leases = Vec::new();
        for _ in 0..3 {
            leases.push(pool.checkout(OperationKind::Read).await.unwrap());
        }
        for lease in leases {
            lease.complete().await.unwrap();
        }
        assert_eq!(factory.live(), 3);

        pool.shutdown().await.unwrap();
        assert_eq!(factory.live(), 0);
        pool.shutdown().await.unwrap();
        assert_eq!(factory.live(), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn shutdown_deadline_bounds_an_active_lease_that_never_returns() {
        let factory = RecordingFactory::fully_capable();
        let pool = pool(factory.clone(), 1, 1, 1).await;
        let held = pool.checkout(OperationKind::Read).await.unwrap();
        let shutting_down = tokio::spawn({
            let pool = pool.clone();
            async move { pool.shutdown().await }
        });
        while !pool.inner.closed.load(Ordering::SeqCst) {
            tokio::task::yield_now().await;
        }

        tokio::time::advance(std::time::Duration::from_secs(46)).await;
        let error = shutting_down.await.unwrap().unwrap_err();
        assert_eq!(
            error.to_string(),
            "failed to close SFTP session: SFTP pool shutdown timed out after 45s"
        );
        assert_eq!(factory.live(), 1);

        held.retire().await.unwrap();
        pool.shutdown().await.unwrap();
        assert_eq!(factory.live(), 0);
    }

    #[tokio::test]
    async fn shutdown_forces_the_process_behind_an_active_lease_before_returning() {
        let factory = ShutdownAwareFactory::default();
        let pool = SftpSessionPool::new_writable(Arc::new(factory.clone()), 1, 1, 1)
            .await
            .unwrap();
        let held = pool.checkout(OperationKind::Write).await.unwrap();
        let process_exited = factory.process_exited.notified();
        let shutting_down = tokio::spawn({
            let pool = pool.clone();
            async move { pool.shutdown().await }
        });

        tokio::time::timeout(std::time::Duration::from_secs(1), process_exited)
            .await
            .expect("terminal pool cancellation reaches the active physical process");
        assert_eq!(factory.live_processes.load(Ordering::SeqCst), 0);
        assert!(!shutting_down.is_finished());

        held.retire().await.unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(1), shutting_down)
            .await
            .expect("shutdown finishes after the active lease returns")
            .unwrap()
            .unwrap();
    }

    #[tokio::test]
    async fn local_openssh_sftp_server_advertises_publication_extensions() {
        let Some(server) = ["/usr/libexec/sftp-server", "/usr/lib/openssh/sftp-server"]
            .into_iter()
            .find(|path| std::path::Path::new(path).is_file())
        else {
            eprintln!(
                "skipped: neither /usr/libexec/sftp-server nor /usr/lib/openssh/sftp-server exists"
            );
            return;
        };
        let root = tempfile::tempdir().unwrap();
        let generation = uuid::Uuid::from_u128(0x00112233_4455_6677_8899_aabbccddeeff);
        let mut object = encode_header(ObjectHeader {
            generation,
            logical_len: 11,
        })
        .to_vec();
        object.extend_from_slice(b"hello world");
        std::fs::write(root.path().join("object.bin"), object).unwrap();
        let large_payload = (0..2 * SFTP_READ_PACKET_SIZE + 17)
            .map(|index| (index % 251) as u8)
            .collect::<Vec<_>>();
        let mut large_object = encode_header(ObjectHeader {
            generation,
            logical_len: large_payload.len() as u64,
        })
        .to_vec();
        large_object.extend_from_slice(&large_payload);
        std::fs::write(root.path().join("large-object.bin"), large_object).unwrap();
        std::fs::create_dir(root.path().join("nested")).unwrap();

        let mut child = tokio::process::Command::new(server)
            .current_dir(root.path())
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .kill_on_drop(true)
            .spawn()
            .expect("launch local OpenSSH sftp-server");
        let stdin = child.stdin.take().unwrap();
        let stdout = child.stdout.take().unwrap();

        let session = OpenSshTransportSession::from_streams(stdin, stdout)
            .await
            .expect("complete the real SFTP extension handshake");
        assert_eq!(
            session.capabilities(),
            SftpCapabilities {
                fsync: true,
                hardlink: true,
                posix_rename: true,
            }
        );
        let read = session
            .read_object(
                std::path::Path::new("object.bin"),
                Some(object_store::GetRange::Bounded(6..11)),
                false,
            )
            .await
            .unwrap();
        assert_eq!(read.header.generation, generation);
        assert_eq!(read.header.logical_len, 11);
        assert_eq!(read.range, 6..11);
        assert_eq!(read.payload.as_ref(), b"world");
        let large_read = session
            .read_object(
                std::path::Path::new("large-object.bin"),
                Some(object_store::GetRange::Bounded(
                    7..large_payload.len() as u64 - 9,
                )),
                false,
            )
            .await
            .unwrap();
        assert_eq!(
            large_read.payload.as_ref(),
            &large_payload[7..large_payload.len() - 9]
        );
        // A bounded end past the logical length clamps: the speculative
        // over-read is discarded and the validated range is re-fetched.
        let clamped = session
            .read_object(
                std::path::Path::new("object.bin"),
                Some(object_store::GetRange::Bounded(6..20)),
                false,
            )
            .await
            .unwrap();
        assert_eq!(clamped.range, 6..11);
        assert_eq!(clamped.payload.as_ref(), b"world");
        // A bounded start past the logical length is deterministically
        // unsatisfiable — typed, so no retry layer ever spins on it.
        let unsatisfiable = session
            .read_object(
                std::path::Path::new("object.bin"),
                Some(object_store::GetRange::Bounded(20..25)),
                false,
            )
            .await
            .unwrap_err();
        assert!(
            matches!(unsatisfiable, TransportError::InvalidRange(_)),
            "{unsatisfiable:?}"
        );
        // Corrupt headers are still rejected before any speculatively read
        // payload can be returned.
        std::fs::write(root.path().join("corrupt.bin"), vec![0xff_u8; 64]).unwrap();
        let corrupt = session
            .read_object(
                std::path::Path::new("corrupt.bin"),
                Some(object_store::GetRange::Bounded(0..8)),
                false,
            )
            .await
            .unwrap_err();
        assert!(
            matches!(corrupt, TransportError::CorruptObject(_)),
            "{corrupt:?}"
        );
        let entries = session
            .list_directory(std::path::Path::new("."))
            .await
            .unwrap();
        assert!(entries.iter().any(|entry| {
            entry.filename == std::path::Path::new("object.bin")
                && entry.kind == RemoteEntryKind::File
        }));
        assert!(entries.iter().any(|entry| {
            entry.filename == std::path::Path::new("nested")
                && entry.kind == RemoteEntryKind::Directory
        }));
        let pipelined_payload = Bytes::from(vec![0x5a; 2 * SFTP_WRITE_PACKET_SIZE + 17]);
        session
            .write_file_durable(
                std::path::Path::new("pipelined.bin"),
                vec![pipelined_payload.clone()],
            )
            .await
            .unwrap();
        assert_eq!(
            std::fs::read(root.path().join("pipelined.bin")).unwrap(),
            pipelined_payload.as_ref()
        );
        session
            .remove_file(std::path::Path::new("object.bin"))
            .await
            .unwrap();
        session
            .remove_file(std::path::Path::new("pipelined.bin"))
            .await
            .unwrap();
        session
            .remove_file(std::path::Path::new("large-object.bin"))
            .await
            .unwrap();
        assert!(!root.path().join("object.bin").exists());
        session.close(CancellationToken::new()).await.unwrap();
        assert!(child.wait().await.unwrap().success());
    }
}
