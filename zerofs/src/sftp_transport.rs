use crate::sftp_object_store::{OBJECT_HEADER_LEN, ObjectHeader, SftpCapabilities, decode_header};
use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use dashmap::DashMap;
use futures::{StreamExt, TryStreamExt};
use std::collections::{HashMap, VecDeque};
use std::fmt;
use std::io::Write;
use std::ops::Range;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex as StdMutex, Weak};
use std::time::{Duration, SystemTime};
#[cfg(test)]
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::io::{AsyncReadExt, AsyncSeekExt, SeekFrom};
use tokio::sync::{Mutex, Notify, OwnedSemaphorePermit, Semaphore, oneshot};
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OperationKind {
    Read,
    Write,
    Metadata,
}

/// Match the largest safe OpenSSH SFTP v3 payload used by the proven rclone
/// path. The client still clips a request if a server negotiates a lower limit.
const SFTP_WRITE_PACKET_SIZE: usize = 255 * 1024;

/// Maximum outstanding WRITE requests on one leased SFTP session. A 64-request
/// window hides the Storage Box WAN RTT without consuming more TCP sessions.
const SFTP_WRITE_REQUEST_CONCURRENCY: usize = 64;

/// Match the write-side request size so sequential prefetch windows use the
/// same proven Storage Box packet shape in both directions.
const SFTP_READ_PACKET_SIZE: usize = 255 * 1024;

/// Maximum outstanding READ requests on one leased SFTP session. ZeroFS ramps
/// sequential cache windows to 8 MiB; issuing their packets together hides the
/// WAN RTT while preserving the shared physical-session limit.
const SFTP_READ_REQUEST_CONCURRENCY: usize = 64;

const SFTP_SESSION_OPEN_TIMEOUT: Duration = Duration::from_secs(30);
const SFTP_SESSION_CLOSE_TIMEOUT: Duration = Duration::from_secs(10);
const SFTP_SESSION_FORCE_CLEANUP_TIMEOUT: Duration = Duration::from_secs(5);
const SFTP_IDLE_TIMEOUT: Duration = Duration::from_secs(60);
const SFTP_IDLE_REAP_INTERVAL: Duration = Duration::from_secs(10);
const SFTP_IDLE_WARM_FLOOR: usize = 1;
const SFTP_POOL_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(45);
const SSH_PROCESS_FORCE_REAP_TIMEOUT: Duration = Duration::from_secs(5);
const SFTP_DIRECTORY_CACHE_MAX_ENTRIES: usize = 64 * 1024;

#[derive(Debug)]
struct PipelinedWrite {
    offset: u64,
    payload: Bytes,
}

#[derive(Debug)]
struct PipelinedRead {
    index: usize,
    offset: u64,
    len: usize,
}

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

async fn write_file_pipelined(
    file: &openssh_sftp_client::file::File,
    path: &std::path::Path,
    offset: u64,
    chunks: Vec<Bytes>,
) -> Result<(), TransportError> {
    let requests = plan_pipelined_writes(offset, chunks)?;
    futures::stream::iter(requests)
        .map(|request| {
            let mut writer = file.clone();
            async move {
                writer
                    .seek(SeekFrom::Start(request.offset))
                    .await
                    .map_err(|error| {
                        TransportError::Operation(format!(
                            "failed to seek {} to {}: {error}",
                            path.display(),
                            request.offset
                        ))
                    })?;
                writer
                    .write_all(&request.payload)
                    .await
                    .map_err(|error| map_sftp_error(path, error))
            }
        })
        .buffer_unordered(SFTP_WRITE_REQUEST_CONCURRENCY)
        .try_collect::<Vec<_>>()
        .await?;
    Ok(())
}

async fn read_file_pipelined(
    file: &openssh_sftp_client::file::File,
    path: &std::path::Path,
    offset: u64,
    len: usize,
) -> Result<Bytes, TransportError> {
    let requests = plan_pipelined_reads(offset, len)?;
    // Carve one allocation into the disjoint region each request fills, so
    // reassembly stitches the regions back together instead of copying them.
    let mut buffer = BytesMut::zeroed(len);
    let mut reads = Vec::with_capacity(requests.len());
    for request in requests {
        let region = buffer.split_to(request.len);
        reads.push((request, region));
    }
    let mut chunks = futures::stream::iter(reads)
        .map(|(request, mut region)| {
            let reader = openssh_sftp_client::file::TokioCompatFile::new(file.clone());
            async move {
                tokio::pin!(reader);
                reader
                    .as_mut()
                    .seek(SeekFrom::Start(request.offset))
                    .await
                    .map_err(|error| {
                        TransportError::Operation(format!(
                            "failed to seek {} to {}: {error}",
                            path.display(),
                            request.offset
                        ))
                    })?;
                reader
                    .as_mut()
                    .read_exact(&mut region)
                    .await
                    .map_err(|error| {
                        TransportError::Operation(format!(
                            "short read from {} at {}: {error}",
                            path.display(),
                            request.offset
                        ))
                    })?;
                Ok::<_, TransportError>((request.index, region))
            }
        })
        .buffer_unordered(SFTP_READ_REQUEST_CONCURRENCY)
        .try_collect::<Vec<_>>()
        .await?;
    chunks.sort_unstable_by_key(|(index, _)| *index);
    let mut payload = BytesMut::new();
    for (_, chunk) in chunks {
        payload.unsplit(chunk);
    }
    Ok(payload.freeze())
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
        &mut self,
        _path: &std::path::Path,
        _range: Option<object_store::GetRange>,
        _head: bool,
    ) -> Result<RemoteObjectRead, TransportError> {
        Err(TransportError::Operation(
            "read_object is not implemented by this session".to_owned(),
        ))
    }
    async fn list_directory(
        &mut self,
        _path: &std::path::Path,
    ) -> Result<Vec<RemoteDirectoryEntry>, TransportError> {
        Err(TransportError::Operation(
            "list_directory is not implemented by this session".to_owned(),
        ))
    }
    async fn remove_file(&mut self, _path: &std::path::Path) -> Result<(), TransportError> {
        Err(TransportError::Operation(
            "remove_file is not implemented by this session".to_owned(),
        ))
    }
    async fn ensure_directory_component(
        &mut self,
        _path: &std::path::Path,
    ) -> Result<(), TransportError> {
        Err(TransportError::Operation(
            "ensure_directory_component is not implemented by this session".to_owned(),
        ))
    }
    async fn write_file_durable(
        &mut self,
        _path: &std::path::Path,
        _chunks: Vec<Bytes>,
    ) -> Result<(), TransportError> {
        Err(TransportError::Operation(
            "write_file_durable is not implemented by this session".to_owned(),
        ))
    }
    async fn write_file_at_durable(
        &mut self,
        _path: &std::path::Path,
        _offset: u64,
        _chunks: Vec<Bytes>,
    ) -> Result<(), TransportError> {
        Err(TransportError::Operation(
            "write_file_at_durable is not implemented by this session".to_owned(),
        ))
    }
    async fn write_file_at(
        &mut self,
        _path: &std::path::Path,
        _offset: u64,
        _chunks: Vec<Bytes>,
    ) -> Result<(), TransportError> {
        Err(TransportError::Operation(
            "write_file_at is not implemented by this session".to_owned(),
        ))
    }
    async fn read_exact(
        &mut self,
        _path: &std::path::Path,
        _offset: u64,
        _len: usize,
    ) -> Result<Bytes, TransportError> {
        Err(TransportError::Operation(
            "read_exact is not implemented by this session".to_owned(),
        ))
    }
    async fn hard_link(
        &mut self,
        _from: &std::path::Path,
        _to: &std::path::Path,
    ) -> Result<(), TransportError> {
        Err(TransportError::Operation(
            "hard_link is not implemented by this session".to_owned(),
        ))
    }
    async fn posix_rename(
        &mut self,
        _from: &std::path::Path,
        _to: &std::path::Path,
    ) -> Result<(), TransportError> {
        Err(TransportError::Operation(
            "posix_rename is not implemented by this session".to_owned(),
        ))
    }
    async fn close(self: Box<Self>, force: CancellationToken) -> Result<(), TransportError>;
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
    shared_limit: usize,
    read_limit: usize,
    write_limit: usize,
    next_id: AtomicU64,
    state: StdMutex<AdmissionState>,
}

#[derive(Default)]
struct AdmissionState {
    active_reads: usize,
    active_writes: usize,
    waiters: VecDeque<AdmissionWaiter>,
    closed: bool,
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
    fn new(shared_limit: usize, read_limit: usize, write_limit: usize) -> Self {
        Self {
            inner: Arc::new(AdmissionInner {
                shared_limit,
                read_limit,
                write_limit,
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
        while state.active_reads + state.active_writes < self.inner.shared_limit {
            let writes_waiting = state
                .waiters
                .iter()
                .any(|waiter| waiter.kind == OperationKind::Write);
            // Draining dirty data is the long-running bulk path. Prefer queued
            // writes up to their configured ceiling when that ceiling reserves
            // shared capacity for reads. Once writeback empties, reads
            // immediately expand to their own configured ceiling.
            let write_reserves_opposite_slot = self.inner.write_limit < self.inner.shared_limit;
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
            OperationKind::Read | OperationKind::Metadata => {
                (state.active_reads, self.inner.read_limit)
            }
            OperationKind::Write => (state.active_writes, self.inner.write_limit),
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
        OperationKind::Read | OperationKind::Metadata => state.active_reads += 1,
        OperationKind::Write => state.active_writes += 1,
    }
}

fn decrement_active(state: &mut AdmissionState, kind: OperationKind) {
    match kind {
        OperationKind::Read | OperationKind::Metadata => state.active_reads -= 1,
        OperationKind::Write => state.active_writes -= 1,
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

pub struct OpenSshSessionFactory {
    endpoint: crate::config::SftpEndpoint,
    identity_file: PathBuf,
    known_hosts: PathBuf,
    authentication_config: Arc<tempfile::NamedTempFile>,
}

impl OpenSshSessionFactory {
    pub fn new(
        endpoint: crate::config::SftpEndpoint,
        identity_file: PathBuf,
        known_hosts: PathBuf,
    ) -> Result<Self, TransportError> {
        let mut authentication_config = tempfile::Builder::new()
            .prefix("zerofs-ssh-")
            .suffix(".conf")
            .tempfile()
            .map_err(|_| TransportError::Open("could not create SSH policy file".to_owned()))?;
        authentication_config
            .write_all(
                b"Host *\n\
                  BatchMode yes\n\
                  PasswordAuthentication no\n\
                  KbdInteractiveAuthentication no\n\
                  ChallengeResponseAuthentication no\n\
                  PreferredAuthentications publickey\n\
                  PubkeyAuthentication yes\n\
                  ConnectTimeout 20\n\
                  ConnectionAttempts 1\n\
                  ServerAliveInterval 30\n\
                  ServerAliveCountMax 3\n\
                  ControlMaster no\n\
                  ControlPersist no\n",
            )
            .map_err(|_| TransportError::Open("could not write SSH policy file".to_owned()))?;
        authentication_config
            .flush()
            .map_err(|_| TransportError::Open("could not flush SSH policy file".to_owned()))?;
        Ok(Self {
            endpoint,
            identity_file,
            known_hosts,
            authentication_config: Arc::new(authentication_config),
        })
    }
}

impl fmt::Debug for OpenSshSessionFactory {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OpenSshSessionFactory")
            .field("host", &self.endpoint.host)
            .field("port", &self.endpoint.port)
            .field("known_hosts", &self.known_hosts)
            .finish_non_exhaustive()
    }
}

async fn force_reap_ssh_process(child: &mut tokio::process::Child) -> Result<(), String> {
    // The direct ssh child is the physical session. Unlike an OpenSSH
    // ControlMaster, it cannot daemonize away from this owned process handle.
    // start_kill followed by wait gives positive local-process death evidence
    // before the pool may reuse the lifetime permit.
    let kill_error = child.start_kill().err().map(|error| error.to_string());
    match tokio::time::timeout(SSH_PROCESS_FORCE_REAP_TIMEOUT, child.wait()).await {
        Ok(Ok(_)) => Ok(()),
        Ok(Err(error)) => Err(match kill_error {
            Some(kill_error) => {
                format!("could not kill SSH process: {kill_error}; could not reap it: {error}")
            }
            None => format!("could not reap SSH process: {error}"),
        }),
        Err(_) => Err(match kill_error {
            Some(kill_error) => format!(
                "could not kill SSH process: {kill_error}; process was not reaped within {}s",
                SSH_PROCESS_FORCE_REAP_TIMEOUT.as_secs()
            ),
            None => format!(
                "SSH process was killed but not reaped within {}s",
                SSH_PROCESS_FORCE_REAP_TIMEOUT.as_secs()
            ),
        }),
    }
}

async fn supervise_ssh_process(
    mut child: tokio::process::Child,
    force: CancellationToken,
) -> Result<(), String> {
    tokio::select! {
        result = child.wait() => result.map(|_| ()).map_err(|error| error.to_string()),
        _ = force.cancelled() => force_reap_ssh_process(&mut child).await,
    }
}

#[async_trait]
impl SessionFactory for OpenSshSessionFactory {
    async fn open(
        &self,
        force: CancellationToken,
    ) -> Result<Box<dyn TransportSession>, TransportError> {
        // One owned, foreground ssh process is one physical pool session. Do
        // not use OpenSSH multiplexing here: its daemonized ControlMaster is
        // outside Tokio's process ownership and cannot be reliably reaped on a
        // lifecycle deadline.
        if force.is_cancelled() {
            return Err(TransportError::PoolClosed);
        }

        let mut command = tokio::process::Command::new("ssh");
        command
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::null())
            .kill_on_drop(true)
            .arg("-F")
            .arg(self.authentication_config.path())
            .arg("-o")
            .arg("StrictHostKeyChecking=yes")
            .arg("-o")
            .arg(format!("UserKnownHostsFile={}", self.known_hosts.display()))
            .arg("-o")
            .arg("IdentitiesOnly=yes")
            .arg("-i")
            .arg(&self.identity_file)
            .arg("-p")
            .arg(self.endpoint.port.to_string())
            .arg("-l")
            .arg(&self.endpoint.username)
            .arg("-T")
            .arg("-s")
            .arg("--")
            .arg(&self.endpoint.host)
            .arg("sftp");

        let mut child = command.spawn().map_err(|_| {
            TransportError::Open(format!(
                "OpenSSH SFTP process for {}:{} failed to start",
                self.endpoint.host, self.endpoint.port
            ))
        })?;
        let stdin = match child.stdin.take() {
            Some(stdin) => stdin,
            None => {
                let cleanup = force_reap_ssh_process(&mut child).await;
                return match cleanup {
                    Ok(()) => Err(TransportError::Open(
                        "OpenSSH SFTP process has no stdin".to_owned(),
                    )),
                    Err(cleanup) => {
                        tracing::error!(%cleanup, "failed to reap OpenSSH process with no stdin");
                        Err(TransportError::PoolClosed)
                    }
                };
            }
        };
        let stdout = match child.stdout.take() {
            Some(stdout) => stdout,
            None => {
                drop(stdin);
                let cleanup = force_reap_ssh_process(&mut child).await;
                return match cleanup {
                    Ok(()) => Err(TransportError::Open(
                        "OpenSSH SFTP process has no stdout".to_owned(),
                    )),
                    Err(cleanup) => {
                        tracing::error!(%cleanup, "failed to reap OpenSSH process with no stdout");
                        Err(TransportError::PoolClosed)
                    }
                };
            }
        };
        let mut handshake = Box::pin(openssh_sftp_client::Sftp::new(
            stdin,
            stdout,
            openssh_sftp_client::SftpOptions::default(),
        ));
        let sftp = tokio::select! {
            result = &mut handshake => result,
            _ = force.cancelled() => {
                drop(handshake);
                if let Err(cleanup) = force_reap_ssh_process(&mut child).await {
                    return Err(TransportError::Close(format!(
                        "OpenSSH process cleanup after SFTP open timeout failed: {cleanup}"
                    )));
                }
                return Err(TransportError::PoolClosed);
            }
        };
        let sftp = match sftp {
            Ok(sftp) => sftp,
            Err(_) => {
                let error = TransportError::Open(format!(
                    "SFTP handshake with {}:{} failed",
                    self.endpoint.host, self.endpoint.port
                ));
                if let Err(cleanup) = force_reap_ssh_process(&mut child).await {
                    return Err(TransportError::Close(format!(
                        "OpenSSH process cleanup after SFTP handshake failure failed: {cleanup}"
                    )));
                }
                return Err(error);
            }
        };
        let process_force = force.clone();
        let process_owner =
            tokio::spawn(async move { supervise_ssh_process(child, process_force).await });
        Ok(Box::new(OpenSshTransportSession {
            sftp: Some(sftp),
            ssh_force: force,
            ssh_process: Some(process_owner),
        }))
    }
}

pub struct OpenSshTransportSession {
    sftp: Option<openssh_sftp_client::Sftp>,
    ssh_force: CancellationToken,
    ssh_process: Option<tokio::task::JoinHandle<Result<(), String>>>,
}

impl fmt::Debug for OpenSshTransportSession {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OpenSshTransportSession")
            .finish_non_exhaustive()
    }
}

impl Drop for OpenSshTransportSession {
    fn drop(&mut self) {
        // The process owner holds and reaps the direct ssh child. Cancellation
        // here is the last-resort path for a session dropped outside the pool's
        // awaited close protocol.
        self.ssh_force.cancel();
    }
}

impl OpenSshTransportSession {
    #[cfg(test)]
    pub async fn from_streams<W, R>(stdin: W, stdout: R) -> Result<Self, TransportError>
    where
        W: AsyncWrite + Send + 'static,
        R: AsyncRead + Send + 'static,
    {
        let sftp = openssh_sftp_client::Sftp::new(
            stdin,
            stdout,
            openssh_sftp_client::SftpOptions::default(),
        )
        .await
        .map_err(|_| TransportError::Open("SFTP stream handshake failed".to_owned()))?;
        Ok(Self {
            sftp: Some(sftp),
            ssh_force: CancellationToken::new(),
            ssh_process: None,
        })
    }
}

#[async_trait]
impl TransportSession for OpenSshTransportSession {
    fn capabilities(&self) -> SftpCapabilities {
        let sftp = self.sftp.as_ref().expect("open transport owns SFTP client");
        SftpCapabilities {
            fsync: sftp.support_fsync(),
            hardlink: sftp.support_hardlink(),
            posix_rename: sftp.support_posix_rename(),
        }
    }

    async fn read_object(
        &mut self,
        path: &std::path::Path,
        requested_range: Option<object_store::GetRange>,
        head: bool,
    ) -> Result<RemoteObjectRead, TransportError> {
        let sftp = self.sftp.as_ref().expect("open transport owns SFTP client");
        let file = sftp
            .open(path)
            .await
            .map_err(|error| map_sftp_error(path, error))?;
        // The header always occupies the same fixed prefix, so its read does not
        // depend on the metadata fetch: issue both round trips together.
        let mut metadata_file = file.clone();
        let (metadata, encoded_header) = tokio::join!(
            metadata_file.metadata(),
            read_file_pipelined(&file, path, 0, OBJECT_HEADER_LEN),
        );
        let metadata = metadata.map_err(|error| map_sftp_error(path, error))?;
        if !metadata.file_type().is_some_and(|kind| kind.is_file()) {
            return Err(TransportError::CorruptObject(format!(
                "{} is not a regular file",
                path.display()
            )));
        }
        let physical_len = metadata.len().ok_or_else(|| {
            TransportError::CorruptObject(format!("{} has no physical length", path.display()))
        })?;
        let modified = metadata.modified().ok_or_else(|| {
            TransportError::CorruptObject(format!("{} has no modification time", path.display()))
        })?;

        let encoded_header = encoded_header?;
        let header = decode_header(&encoded_header).map_err(|error| {
            TransportError::CorruptObject(format!("{}: {error}", path.display()))
        })?;
        let expected_physical_len = (OBJECT_HEADER_LEN as u64)
            .checked_add(header.logical_len)
            .ok_or_else(|| {
                TransportError::CorruptObject(format!(
                    "{} logical length overflows its physical representation",
                    path.display()
                ))
            })?;
        if physical_len != expected_physical_len {
            return Err(TransportError::CorruptObject(format!(
                "{} physical length {physical_len} does not match expected {expected_physical_len}",
                path.display()
            )));
        }

        let range = match requested_range {
            Some(range) => range.as_range(header.logical_len).map_err(|error| {
                TransportError::Operation(format!(
                    "invalid logical range for {}: {error}",
                    path.display()
                ))
            })?,
            None => 0..header.logical_len,
        };
        let payload = if head || range.is_empty() {
            Bytes::new()
        } else {
            let physical_start = (OBJECT_HEADER_LEN as u64)
                .checked_add(range.start)
                .ok_or_else(|| {
                    TransportError::CorruptObject(format!(
                        "{} physical read offset overflow",
                        path.display()
                    ))
                })?;
            let len: usize = (range.end - range.start).try_into().map_err(|_| {
                TransportError::Operation(format!(
                    "requested range for {} does not fit memory",
                    path.display()
                ))
            })?;
            read_file_pipelined(&file, path, physical_start, len).await?
        };
        file.close()
            .await
            .map_err(|error| map_sftp_error(path, error))?;
        Ok(RemoteObjectRead {
            header,
            modified: modified.as_system_time(),
            range,
            payload,
        })
    }

    async fn list_directory(
        &mut self,
        path: &std::path::Path,
    ) -> Result<Vec<RemoteDirectoryEntry>, TransportError> {
        let sftp = self.sftp.as_ref().expect("open transport owns SFTP client");
        let mut fs = sftp.fs();
        let directory = fs
            .open_dir(path)
            .await
            .map_err(|error| map_sftp_error(path, error))?;
        let entries = directory.read_dir();
        tokio::pin!(entries);
        let mut result = Vec::new();
        while let Some(entry) = entries.as_mut().next().await {
            let entry = entry.map_err(|error| map_sftp_error(path, error))?;
            let filename = entry.filename().to_path_buf();
            let kind = match entry.file_type() {
                Some(kind) if kind.is_file() => RemoteEntryKind::File,
                Some(kind) if kind.is_dir() => RemoteEntryKind::Directory,
                Some(kind) if kind.is_symlink() => RemoteEntryKind::Symlink,
                _ => RemoteEntryKind::Other,
            };
            result.push(RemoteDirectoryEntry { filename, kind });
        }
        Ok(result)
    }

    async fn remove_file(&mut self, path: &std::path::Path) -> Result<(), TransportError> {
        let sftp = self.sftp.as_ref().expect("open transport owns SFTP client");
        sftp.fs()
            .remove_file(path)
            .await
            .map_err(|error| map_sftp_error(path, error))
    }

    async fn ensure_directory_component(
        &mut self,
        path: &std::path::Path,
    ) -> Result<(), TransportError> {
        let sftp = self.sftp.as_ref().expect("open transport owns SFTP client");
        let mut fs = sftp.fs();
        for component in path.components() {
            if !matches!(component, std::path::Component::Normal(_)) {
                return Err(TransportError::Operation(format!(
                    "unsafe directory path {}",
                    path.display()
                )));
            }
        }

        let stat_started = Instant::now();
        let metadata = fs.symlink_metadata(path).await;
        metrics::counter!("zerofs_sftp_directory_stats_total").increment(1);
        metrics::histogram!("zerofs_sftp_directory_stat_duration_seconds")
            .record(stat_started.elapsed().as_secs_f64());
        match metadata {
            Ok(metadata) if metadata.file_type().is_some_and(|kind| kind.is_dir()) => Ok(()),
            Ok(_) => Err(TransportError::Operation(format!(
                "{} exists and is not a directory",
                path.display()
            ))),
            Err(openssh_sftp_client::Error::SftpError(
                openssh_sftp_client::error::SftpErrorKind::NoSuchFile,
                _,
            )) => {
                let mkdir_started = Instant::now();
                let created = fs.create_dir(path).await;
                metrics::counter!("zerofs_sftp_directory_mkdirs_total").increment(1);
                metrics::histogram!("zerofs_sftp_directory_mkdir_duration_seconds")
                    .record(mkdir_started.elapsed().as_secs_f64());
                match created {
                    Ok(()) => Ok(()),
                    Err(create_error) => {
                        // Another writer outside this pool can win the mkdir
                        // race. Verify that result instead of turning a valid
                        // directory into a publication failure.
                        let verify_started = Instant::now();
                        let verified = fs.symlink_metadata(path).await;
                        metrics::counter!("zerofs_sftp_directory_stats_total").increment(1);
                        metrics::histogram!("zerofs_sftp_directory_stat_duration_seconds")
                            .record(verify_started.elapsed().as_secs_f64());
                        match verified {
                            Ok(metadata)
                                if metadata.file_type().is_some_and(|kind| kind.is_dir()) =>
                            {
                                Ok(())
                            }
                            _ => Err(map_sftp_error(path, create_error)),
                        }
                    }
                }
            }
            Err(error) => Err(map_sftp_error(path, error)),
        }
    }

    async fn write_file_durable(
        &mut self,
        path: &std::path::Path,
        chunks: Vec<Bytes>,
    ) -> Result<(), TransportError> {
        let sftp = self.sftp.as_ref().expect("open transport owns SFTP client");
        let mut file = sftp
            .create(path)
            .await
            .map_err(|error| map_sftp_error(path, error))?;
        write_file_pipelined(&file, path, 0, chunks).await?;
        file.sync_all()
            .await
            .map_err(|error| map_sftp_error(path, error))?;
        file.close()
            .await
            .map_err(|error| map_sftp_error(path, error))
    }

    async fn write_file_at_durable(
        &mut self,
        path: &std::path::Path,
        offset: u64,
        chunks: Vec<Bytes>,
    ) -> Result<(), TransportError> {
        let sftp = self.sftp.as_ref().expect("open transport owns SFTP client");
        let mut options = sftp.options();
        options.write(true);
        let mut file = options
            .open(path)
            .await
            .map_err(|error| map_sftp_error(path, error))?;
        write_file_pipelined(&file, path, offset, chunks).await?;
        file.sync_all()
            .await
            .map_err(|error| map_sftp_error(path, error))?;
        file.close()
            .await
            .map_err(|error| map_sftp_error(path, error))
    }

    async fn write_file_at(
        &mut self,
        path: &std::path::Path,
        offset: u64,
        chunks: Vec<Bytes>,
    ) -> Result<(), TransportError> {
        let sftp = self.sftp.as_ref().expect("open transport owns SFTP client");
        let mut options = sftp.options();
        options.write(true);
        let file = options
            .open(path)
            .await
            .map_err(|error| map_sftp_error(path, error))?;
        write_file_pipelined(&file, path, offset, chunks).await?;
        file.close()
            .await
            .map_err(|error| map_sftp_error(path, error))
    }

    async fn read_exact(
        &mut self,
        path: &std::path::Path,
        offset: u64,
        len: usize,
    ) -> Result<Bytes, TransportError> {
        let sftp = self.sftp.as_ref().expect("open transport owns SFTP client");
        let file = sftp
            .open(path)
            .await
            .map_err(|error| map_sftp_error(path, error))?;
        let bytes = read_file_pipelined(&file, path, offset, len).await?;
        file.close()
            .await
            .map_err(|error| map_sftp_error(path, error))?;
        Ok(bytes)
    }

    async fn hard_link(
        &mut self,
        from: &std::path::Path,
        to: &std::path::Path,
    ) -> Result<(), TransportError> {
        let sftp = self.sftp.as_ref().expect("open transport owns SFTP client");
        match sftp.fs().hard_link(from, to).await {
            Ok(()) => Ok(()),
            Err(openssh_sftp_client::Error::SftpError(
                openssh_sftp_client::error::SftpErrorKind::Failure,
                _,
            )) => Err(TransportError::AlreadyExists(to.display().to_string())),
            Err(error) => Err(map_sftp_error(to, error)),
        }
    }

    async fn posix_rename(
        &mut self,
        from: &std::path::Path,
        to: &std::path::Path,
    ) -> Result<(), TransportError> {
        let sftp = self.sftp.as_ref().expect("open transport owns SFTP client");
        if !sftp.support_posix_rename() {
            return Err(TransportError::MissingCapability(
                "posix-rename@openssh.com",
            ));
        }
        sftp.fs()
            .rename(from, to)
            .await
            .map_err(|error| map_sftp_error(to, error))
    }

    async fn close(mut self: Box<Self>, force: CancellationToken) -> Result<(), TransportError> {
        let sftp = self.sftp.take().expect("open transport owns SFTP client");
        let mut graceful = Box::pin(sftp.close());
        let graceful_result = tokio::select! {
            result = &mut graceful => Some(result),
            _ = force.cancelled() => None,
            _ = self.ssh_force.cancelled() => None,
        };
        drop(graceful);

        self.ssh_force.cancel();
        if let Some(process_owner) = self.ssh_process.take() {
            match process_owner.await {
                Ok(Ok(())) => {}
                Ok(Err(error)) => {
                    return Err(TransportError::Close(format!(
                        "OpenSSH SFTP process did not terminate: {error}"
                    )));
                }
                Err(error) => {
                    return Err(TransportError::Close(format!(
                        "OpenSSH SFTP process owner failed: {error}"
                    )));
                }
            }
        }

        if let Some(Err(error)) = graceful_result {
            tracing::warn!(%error, "SFTP protocol shutdown failed after the SSH process exited");
        } else if graceful_result.is_none() {
            tracing::warn!(
                "SFTP protocol shutdown exceeded its deadline; the SSH process was killed and reaped"
            );
        }
        Ok(())
    }
}

fn map_sftp_error(path: &std::path::Path, error: openssh_sftp_client::Error) -> TransportError {
    match error {
        openssh_sftp_client::Error::SftpError(
            openssh_sftp_client::error::SftpErrorKind::NoSuchFile,
            _,
        ) => TransportError::NotFound(path.display().to_string()),
        openssh_sftp_client::Error::SftpError(
            openssh_sftp_client::error::SftpErrorKind::PermDenied,
            _,
        ) => TransportError::PermissionDenied(path.display().to_string()),
        error => TransportError::Operation(format!("{}: {error}", path.display())),
    }
}

struct PhysicalSession {
    transport: Option<Box<dyn TransportSession>>,
    lifetime: Option<OwnedSemaphorePermit>,
    idle_since: Instant,
    pool: Weak<PoolInner>,
}

impl PhysicalSession {
    fn take_parts(&mut self) -> (Box<dyn TransportSession>, OwnedSemaphorePermit) {
        (
            self.transport
                .take()
                .expect("physical session transport is taken exactly once"),
            self.lifetime
                .take()
                .expect("physical session permit is taken exactly once"),
        )
    }

    fn transport(&self) -> &dyn TransportSession {
        self.transport
            .as_deref()
            .expect("leased physical session owns its transport")
    }

    fn transport_mut(&mut self) -> &mut dyn TransportSession {
        self.transport
            .as_deref_mut()
            .expect("leased physical session owns its transport")
    }
}

impl Drop for PhysicalSession {
    fn drop(&mut self) {
        let (Some(transport), Some(lifetime)) = (self.transport.take(), self.lifetime.take())
        else {
            return;
        };
        let Some(pool) = self.pool.upgrade() else {
            drop(transport);
            drop(lifetime);
            return;
        };
        if tokio::runtime::Handle::try_current().is_ok() {
            let cleanup_pool = pool.clone();
            pool.tasks.spawn(async move {
                let _ = cleanup_pool.close_parts(transport, lifetime).await;
            });
        } else {
            pool.fail_closed();
            drop(transport);
            drop(lifetime);
        }
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
    admission: FairAdmission,
    idle: Mutex<VecDeque<PhysicalSession>>,
    idle_available: Notify,
    directories: DirectoryCache,
    writable: bool,
    closed: AtomicBool,
    activity_gate: StdMutex<()>,
    active: AtomicUsize,
    activity_changed: Notify,
    reaper_shutdown: CancellationToken,
    session_shutdown: CancellationToken,
    tasks: TaskTracker,
    runtime: tokio::runtime::Handle,
    shutdown_lock: Mutex<()>,
    shutdown_complete: AtomicBool,
    close_error: StdMutex<Option<String>>,
}

struct FailClosedOnOwnerDrop {
    pool: Arc<PoolInner>,
    armed: bool,
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
        self.idle_available.notify_waiters();
        if tokio::runtime::Handle::try_current().is_ok() {
            let pool = self.clone();
            self.tasks.spawn(async move {
                let idle = {
                    let mut idle = pool.idle.lock().await;
                    idle.drain(..).collect::<Vec<_>>()
                };
                for session in idle {
                    let cleanup_pool = pool.clone();
                    pool.tasks.spawn(async move {
                        let _ = cleanup_pool.close_session(session).await;
                    });
                }
            });
        }
    }

    fn register_activity(self: &Arc<Self>) -> Result<PoolActivity, TransportError> {
        let _gate = self.activity_gate.lock().unwrap();
        if self.closed.load(Ordering::SeqCst) {
            return Err(TransportError::PoolClosed);
        }
        self.active.fetch_add(1, Ordering::SeqCst);
        Ok(PoolActivity {
            pool: self.clone(),
            active: true,
        })
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

    async fn close_parts(
        self: &Arc<Self>,
        transport: Box<dyn TransportSession>,
        lifetime: OwnedSemaphorePermit,
    ) -> Result<(), TransportError> {
        self.close_owned(transport, Some(lifetime)).await
    }

    async fn close_transport(
        self: &Arc<Self>,
        transport: Box<dyn TransportSession>,
    ) -> Result<(), TransportError> {
        self.close_owned(transport, None).await
    }

    async fn close_owned(
        self: &Arc<Self>,
        transport: Box<dyn TransportSession>,
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

    async fn close_session(
        self: &Arc<Self>,
        mut session: PhysicalSession,
    ) -> Result<(), TransportError> {
        let (transport, lifetime) = session.take_parts();
        self.close_parts(transport, lifetime).await
    }

    async fn reap_expired_idle(self: &Arc<Self>) {
        let expired = {
            let now = Instant::now();
            let mut idle = self.idle.lock().await;
            let mut expired = Vec::new();
            while idle.len() > SFTP_IDLE_WARM_FLOOR
                && idle.front().is_some_and(|session| {
                    now.saturating_duration_since(session.idle_since) >= SFTP_IDLE_TIMEOUT
                })
            {
                expired.push(idle.pop_front().expect("idle front checked above"));
            }
            expired
        };

        for session in expired {
            let inner = self.clone();
            self.tasks.spawn(async move {
                let _ = inner.close_session(session).await;
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

    pub(crate) fn spawn_cleanup<F>(&self, path: &std::path::Path, future: F) -> bool
    where
        F: std::future::Future<Output = ()> + Send + 'static,
    {
        let _gate = self.inner.activity_gate.lock().unwrap();
        if self.inner.closed.load(Ordering::SeqCst) {
            self.inner.record_error_message(format!(
                "SFTP staging cleanup remains required for {} because the session pool is closed",
                path.display()
            ));
            return false;
        }
        self.inner.tasks.spawn_on(future, &self.inner.runtime);
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
                admission: FairAdmission::new(shared, reads, writes),
                idle: Mutex::new(VecDeque::new()),
                idle_available: Notify::new(),
                directories: DirectoryCache::default(),
                writable: true,
                closed: AtomicBool::new(false),
                activity_gate: StdMutex::new(()),
                active: AtomicUsize::new(0),
                activity_changed: Notify::new(),
                reaper_shutdown: CancellationToken::new(),
                session_shutdown: CancellationToken::new(),
                tasks: TaskTracker::new(),
                runtime: tokio::runtime::Handle::current(),
                shutdown_lock: Mutex::new(()),
                shutdown_complete: AtomicBool::new(false),
                close_error: StdMutex::new(None),
            }),
        };

        let session = pool.open_physical().await?;
        pool.inner.idle.lock().await.push_back(session);
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
        for (name, value) in [("read", reads), ("write", writes)] {
            if !(1..=7).contains(&value) {
                return Err(TransportError::InvalidLimits(format!(
                    "{name} sessions must be between 1 and 7"
                )));
            }
            if value > shared {
                return Err(TransportError::InvalidLimits(format!(
                    "{name} sessions must not exceed shared sessions"
                )));
            }
        }
        Ok(())
    }

    pub async fn checkout(&self, kind: OperationKind) -> Result<SessionLease, TransportError> {
        let admission = self.inner.admission.acquire(kind).await?;
        let activity = self.inner.register_activity()?;

        let session = self.checkout_physical().await?;
        Ok(SessionLease {
            pool: self.inner.clone(),
            session: Some(session),
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

        self.inner.fail_closed();
        self.inner.reaper_shutdown.cancel();
        let inner = self.inner.clone();
        let drain = async move {
            inner.wait_for_activity_drain().await;
            let idle = {
                let mut idle = inner.idle.lock().await;
                idle.drain(..).collect::<Vec<_>>()
            };
            for session in idle {
                let cleanup_pool = inner.clone();
                inner.tasks.spawn(async move {
                    let _ = cleanup_pool.close_session(session).await;
                });
            }
            inner.tasks.close();
            inner.tasks.wait().await;
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

    async fn checkout_physical(&self) -> Result<PhysicalSession, TransportError> {
        loop {
            let idle_available = self.inner.idle_available.notified();
            tokio::pin!(idle_available);
            if let Some(session) = self.inner.idle.lock().await.pop_front() {
                return Ok(session);
            }

            tokio::select! {
                biased;
                permit = self.inner.shared.clone().acquire_owned() => {
                    let permit = permit.map_err(|_| TransportError::PoolClosed)?;
                    return self.open_with_permit(permit).await;
                }
                () = &mut idle_available => {}
            }
        }
    }

    async fn open_physical(&self) -> Result<PhysicalSession, TransportError> {
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
    ) -> Result<PhysicalSession, TransportError> {
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
                    let session = PhysicalSession {
                        transport: Some(transport),
                        lifetime: permit.take(),
                        idle_since: Instant::now(),
                        pool: Arc::downgrade(&owner_pool),
                    };
                    debug_assert!(session.lifetime.is_some());
                    session
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
                            let _ = owner_pool.close_transport(transport).await;
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
                let _ = owner_pool.close_session(session).await;
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
            && let Err(error) = require_publication_capabilities(session.transport().capabilities())
        {
            match self.inner.close_session(session).await {
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
    session: Option<PhysicalSession>,
    admission: Option<OperationAdmission>,
    activity: Option<PoolActivity>,
}

struct PoolActivity {
    pool: Arc<PoolInner>,
    active: bool,
}

impl Drop for PoolActivity {
    fn drop(&mut self) {
        if self.active {
            self.active = false;
            self.pool.finish_activity();
        }
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
    pub async fn read_object(
        &mut self,
        path: &std::path::Path,
        range: Option<object_store::GetRange>,
        head: bool,
    ) -> Result<RemoteObjectRead, TransportError> {
        self.session
            .as_mut()
            .expect("lease always owns a session until completion")
            .transport_mut()
            .read_object(path, range, head)
            .await
    }

    pub async fn list_directory(
        &mut self,
        path: &std::path::Path,
    ) -> Result<Vec<RemoteDirectoryEntry>, TransportError> {
        self.session
            .as_mut()
            .expect("lease always owns a session until completion")
            .transport_mut()
            .list_directory(path)
            .await
    }

    pub async fn remove_file(&mut self, path: &std::path::Path) -> Result<(), TransportError> {
        self.session
            .as_mut()
            .expect("lease always owns a session until completion")
            .transport_mut()
            .remove_file(path)
            .await
    }

    async fn ensure_directory_component(
        &mut self,
        path: &std::path::Path,
    ) -> Result<(), TransportError> {
        self.session
            .as_mut()
            .expect("lease always owns a session until completion")
            .transport_mut()
            .ensure_directory_component(path)
            .await
    }

    pub async fn write_file_durable(
        &mut self,
        path: &std::path::Path,
        chunks: Vec<Bytes>,
    ) -> Result<(), TransportError> {
        self.session
            .as_mut()
            .expect("lease always owns a session until completion")
            .transport_mut()
            .write_file_durable(path, chunks)
            .await
    }

    pub async fn write_file_at_durable(
        &mut self,
        path: &std::path::Path,
        offset: u64,
        chunks: Vec<Bytes>,
    ) -> Result<(), TransportError> {
        self.session
            .as_mut()
            .expect("lease always owns a session until completion")
            .transport_mut()
            .write_file_at_durable(path, offset, chunks)
            .await
    }

    pub async fn write_file_at(
        &mut self,
        path: &std::path::Path,
        offset: u64,
        chunks: Vec<Bytes>,
    ) -> Result<(), TransportError> {
        self.session
            .as_mut()
            .expect("lease always owns a session until completion")
            .transport_mut()
            .write_file_at(path, offset, chunks)
            .await
    }

    pub async fn read_exact(
        &mut self,
        path: &std::path::Path,
        offset: u64,
        len: usize,
    ) -> Result<Bytes, TransportError> {
        self.session
            .as_mut()
            .expect("lease always owns a session until completion")
            .transport_mut()
            .read_exact(path, offset, len)
            .await
    }

    pub async fn hard_link(
        &mut self,
        from: &std::path::Path,
        to: &std::path::Path,
    ) -> Result<(), TransportError> {
        self.session
            .as_mut()
            .expect("lease always owns a session until completion")
            .transport_mut()
            .hard_link(from, to)
            .await
    }

    pub async fn posix_rename(
        &mut self,
        from: &std::path::Path,
        to: &std::path::Path,
    ) -> Result<(), TransportError> {
        self.session
            .as_mut()
            .expect("lease always owns a session until completion")
            .transport_mut()
            .posix_rename(from, to)
            .await
    }

    pub async fn complete(mut self) -> Result<(), TransportError> {
        let session = self
            .session
            .take()
            .expect("lease always owns a session until completion");
        let pool = self.pool.clone();
        let admission = self.admission.take();
        let activity = self.activity.take();
        let returning = self.pool.tasks.spawn(async move {
            let mut idle = pool.idle.lock().await;
            if pool.closed.load(Ordering::SeqCst) {
                drop(idle);
                let _ = pool.close_session(session).await;
            } else {
                let mut session = session;
                session.idle_since = Instant::now();
                idle.push_back(session);
                drop(idle);
                pool.idle_available.notify_one();
            }
            drop(admission);
            drop(activity);
        });
        returning
            .await
            .map_err(|_| TransportError::Close("SFTP return task failed".to_owned()))
    }

    pub async fn retire(mut self) -> Result<(), TransportError> {
        let session = self
            .session
            .take()
            .expect("lease always owns a session until retirement");
        let admission = self.admission.take();
        let activity = self.activity.take();
        let pool = self.pool.clone();
        let cleanup = self.pool.tasks.spawn(async move {
            let result = pool.close_session(session).await;
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
        let admission = self.admission.take();
        let activity = self.activity.take();
        let pool = self.pool.clone();
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            drop(runtime);
            self.pool.tasks.spawn(async move {
                let _ = pool.close_session(session).await;
                drop(admission);
                drop(activity);
            });
        } else {
            pool.fail_closed();
            drop(session);
            drop(admission);
            drop(activity);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        LeaseFinishError, OpenSshSessionFactory, OpenSshTransportSession, OperationKind,
        RemoteEntryKind, SFTP_READ_PACKET_SIZE, SFTP_READ_REQUEST_CONCURRENCY,
        SFTP_WRITE_PACKET_SIZE, SFTP_WRITE_REQUEST_CONCURRENCY, SessionDisposition, SessionFactory,
        SftpSessionPool, TransportError, TransportSession, plan_pipelined_reads,
        plan_pipelined_writes, supervise_ssh_process,
    };
    use crate::config::SftpEndpoint;
    use crate::sftp_object_store::{ObjectHeader, SftpCapabilities, encode_header};
    use async_trait::async_trait;
    use bytes::Bytes;
    use std::fmt;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};
    use tokio::sync::Notify;
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

        async fn close(self: Box<Self>, _force: CancellationToken) -> Result<(), TransportError> {
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

        async fn close(self: Box<Self>, force: CancellationToken) -> Result<(), TransportError> {
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

        async fn close(self: Box<Self>, _force: CancellationToken) -> Result<(), TransportError> {
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
        assert!(matches!(
            pool.shutdown().await,
            Err(TransportError::Close(ref message)) if message.contains("test-staging")
        ));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn thirty_two_waiters_observe_exact_shared_and_directional_caps() {
        let factory = RecordingFactory::fully_capable();
        let pool = Arc::new(pool(factory.clone(), 8, 7, 7).await);
        let mut held = Vec::new();
        for _ in 0..7 {
            held.push(pool.checkout(OperationKind::Read).await.unwrap());
        }
        held.push(pool.checkout(OperationKind::Write).await.unwrap());
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
        assert_eq!(factory.live(), 8);
        assert_eq!(factory.peak(), 8);
        for lease in held {
            lease.complete().await.unwrap();
        }
        for task in tasks {
            task.await.unwrap();
        }
        assert_eq!(factory.peak(), 8);
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
        let admission = super::FairAdmission::new(8, 7, 7);
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
        let admission = super::FairAdmission::new(8, 7, 7);
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
        let admission = super::FairAdmission::new(8, 7, 7);
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
        let admission = super::FairAdmission::new(1, 1, 1);
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
    async fn metadata_uses_fifo_read_admission_without_starvation() {
        let factory = RecordingFactory::fully_capable();
        let pool = Arc::new(pool(factory, 2, 1, 1).await);
        let first = pool.checkout(OperationKind::Read).await.unwrap();
        let order = Arc::new(Mutex::new(Vec::new()));
        let mut tasks = Vec::new();
        for (position, kind) in [
            OperationKind::Metadata,
            OperationKind::Read,
            OperationKind::Metadata,
        ]
        .into_iter()
        .enumerate()
        {
            let pool = pool.clone();
            let order = order.clone();
            tasks.push(tokio::spawn(async move {
                let lease = pool.checkout(kind).await.unwrap();
                order.lock().unwrap().push(position);
                lease.complete().await.unwrap();
            }));
            tokio::task::yield_now().await;
        }

        first.complete().await.unwrap();
        for task in tasks {
            task.await.unwrap();
        }
        assert_eq!(*order.lock().unwrap(), vec![0, 1, 2]);
    }

    #[tokio::test]
    async fn canceled_waiter_does_not_dial_or_consume_admission() {
        let factory = RecordingFactory::fully_capable();
        let pool = Arc::new(pool(factory.clone(), 1, 1, 1).await);
        let held = pool.checkout(OperationKind::Read).await.unwrap();
        let waiter = tokio::spawn({
            let pool = pool.clone();
            async move { pool.checkout(OperationKind::Metadata).await }
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
    async fn canceled_complete_returns_session_before_releasing_admission() {
        let factory = RecordingFactory::fully_capable();
        let pool = Arc::new(pool(factory.clone(), 1, 1, 1).await);
        let lease = pool.checkout(OperationKind::Read).await.unwrap();
        let idle_guard = pool.inner.idle.lock().await;
        let completing = tokio::spawn(async move { lease.complete().await });
        tokio::task::yield_now().await;
        completing.abort();
        assert!(completing.await.unwrap_err().is_cancelled());
        drop(idle_guard);

        tokio::time::timeout(
            std::time::Duration::from_secs(1),
            pool.checkout(OperationKind::Read),
        )
        .await
        .expect("session return completes after caller cancellation")
        .unwrap()
        .complete()
        .await
        .unwrap();
        assert_eq!(factory.dials(), 1);
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

        let replacement = tokio::spawn({
            let pool = pool.clone();
            async move { pool.checkout(OperationKind::Write).await }
        });
        tokio::task::yield_now().await;
        assert!(!replacement.is_finished());
        assert_eq!(factory.dials(), 2);
        assert_eq!(factory.peak(), 2);

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
        assert!(!replacement.is_finished());
        assert_eq!(factory.dials(), 2);
        assert_eq!(factory.live(), 2);

        factory.state.block_close.store(0, Ordering::SeqCst);
        factory.state.allow_close.notify_waiters();
        let replacement = tokio::time::timeout(std::time::Duration::from_secs(1), replacement)
            .await
            .expect("replacement proceeds after abandoned open is closed")
            .unwrap()
            .unwrap();
        assert_eq!(factory.dials(), 3);
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
            .idle
            .lock()
            .await
            .pop_front()
            .expect("constructor leaves one warm session");
        factory.state.block_close.store(1, Ordering::SeqCst);
        factory.state.panic_close.store(1, Ordering::SeqCst);

        let closing = tokio::spawn({
            let pool = pool.clone();
            async move { pool.inner.close_session(session).await }
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
        let held = pool.checkout(OperationKind::Read).await.unwrap();
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

    #[test]
    fn openssh_factory_debug_redacts_the_username() {
        let factory = OpenSshSessionFactory::new(
            SftpEndpoint {
                host: "storage.example.test".to_owned(),
                port: 2222,
                username: "account-secret-name".to_owned(),
            },
            "/tmp/id-ed25519".into(),
            "/tmp/known-hosts".into(),
        )
        .unwrap();

        let debug = format!("{factory:?}");
        assert!(debug.contains("storage.example.test"));
        assert!(debug.contains("2222"));
        assert!(!debug.contains("account-secret-name"));
    }

    #[test]
    fn openssh_factory_effective_policy_bounds_connect_and_dead_peer_detection() {
        let factory = OpenSshSessionFactory::new(
            SftpEndpoint {
                host: "storage.example.test".to_owned(),
                port: 2222,
                username: "account-name".to_owned(),
            },
            "/tmp/id-ed25519".into(),
            "/tmp/known-hosts".into(),
        )
        .unwrap();

        let output = std::process::Command::new("ssh")
            .args(["-G", "-F"])
            .arg(factory.authentication_config.path())
            .arg("storage.example.test")
            .output()
            .expect("local ssh client evaluates the generated policy");
        assert!(output.status.success());
        let policy = String::from_utf8(output.stdout).unwrap();

        assert!(policy.lines().any(|line| line == "connecttimeout 20"));
        assert!(policy.lines().any(|line| line == "batchmode yes"));
        assert!(policy.lines().any(|line| line == "serveraliveinterval 30"));
        assert!(policy.lines().any(|line| line == "serveralivecountmax 3"));
        assert!(policy.lines().any(|line| line == "controlmaster false"));
        assert!(policy.lines().any(|line| line == "controlpersist no"));
    }

    #[tokio::test]
    async fn terminal_force_kills_and_reaps_the_owned_ssh_process() {
        let child = tokio::process::Command::new("sleep")
            .arg("60")
            .kill_on_drop(true)
            .spawn()
            .expect("spawn local stand-in for the owned ssh process");
        let force = CancellationToken::new();
        let owner = tokio::spawn(supervise_ssh_process(child, force.clone()));

        force.cancel();
        tokio::time::timeout(std::time::Duration::from_secs(1), owner)
            .await
            .expect("forced process owner terminates within its deadline")
            .expect("process owner task does not panic")
            .expect("owned process is positively reaped");
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

        let mut session = OpenSshTransportSession::from_streams(stdin, stdout)
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
        Box::new(session)
            .close(CancellationToken::new())
            .await
            .unwrap();
        assert!(child.wait().await.unwrap().success());
    }
}
