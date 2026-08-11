use crate::sftp_object_store::{OBJECT_HEADER_LEN, ObjectHeader, SftpCapabilities, decode_header};
use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use futures::{StreamExt, TryStreamExt};
use std::collections::VecDeque;
use std::fmt;
use std::io::Write;
use std::ops::Range;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::SystemTime;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncSeekExt, AsyncWrite, SeekFrom};
use tokio::sync::{Mutex, Notify, OwnedSemaphorePermit, Semaphore, oneshot};

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
    let mut chunks = futures::stream::iter(requests)
        .map(|request| {
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
                let mut payload = vec![0_u8; request.len];
                reader
                    .as_mut()
                    .read_exact(&mut payload)
                    .await
                    .map_err(|error| {
                        TransportError::Operation(format!(
                            "short read from {} at {}: {error}",
                            path.display(),
                            request.offset
                        ))
                    })?;
                Ok::<_, TransportError>((request.index, Bytes::from(payload)))
            }
        })
        .buffer_unordered(SFTP_READ_REQUEST_CONCURRENCY)
        .try_collect::<Vec<_>>()
        .await?;
    chunks.sort_unstable_by_key(|(index, _)| *index);
    let mut payload = BytesMut::with_capacity(len);
    for (_, chunk) in chunks {
        payload.extend_from_slice(&chunk);
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
    async fn create_dir_all(&mut self, _path: &std::path::Path) -> Result<(), TransportError> {
        Err(TransportError::Operation(
            "create_dir_all is not implemented by this session".to_owned(),
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
    async fn close(self: Box<Self>) -> Result<(), TransportError>;
}

#[async_trait]
pub trait SessionFactory: fmt::Debug + Send + Sync + 'static {
    async fn open(&self) -> Result<Box<dyn TransportSession>, TransportError>;
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
        while state.active_reads + state.active_writes < self.inner.shared_limit {
            let Some(index) = state.waiters.iter().position(|waiter| match waiter.kind {
                OperationKind::Read | OperationKind::Metadata => {
                    state.active_reads < self.inner.read_limit
                }
                OperationKind::Write => state.active_writes < self.inner.write_limit,
            }) else {
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
                  PasswordAuthentication no\n\
                  KbdInteractiveAuthentication no\n\
                  ChallengeResponseAuthentication no\n\
                  PreferredAuthentications publickey\n\
                  PubkeyAuthentication yes\n",
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

#[async_trait]
impl SessionFactory for OpenSshSessionFactory {
    async fn open(&self) -> Result<Box<dyn TransportSession>, TransportError> {
        let mut builder = openssh::SessionBuilder::default();
        builder
            .user(self.endpoint.username.clone())
            .port(self.endpoint.port)
            .known_hosts_check(openssh::KnownHosts::Strict)
            .keyfile(&self.identity_file)
            .user_known_hosts_file(&self.known_hosts)
            .config_file(self.authentication_config.path());
        let ssh = builder.connect(&self.endpoint.host).await.map_err(|_| {
            TransportError::Open(format!(
                "OpenSSH connection to {}:{} failed",
                self.endpoint.host, self.endpoint.port
            ))
        })?;
        let sftp = openssh_sftp_client::Sftp::from_session(
            ssh,
            openssh_sftp_client::SftpOptions::default(),
        )
        .await
        .map_err(|_| {
            TransportError::Open(format!(
                "SFTP handshake with {}:{} failed",
                self.endpoint.host, self.endpoint.port
            ))
        })?;
        Ok(Box::new(OpenSshTransportSession { sftp: Some(sftp) }))
    }
}

pub struct OpenSshTransportSession {
    sftp: Option<openssh_sftp_client::Sftp>,
}

impl fmt::Debug for OpenSshTransportSession {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OpenSshTransportSession")
            .finish_non_exhaustive()
    }
}

impl OpenSshTransportSession {
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
        Ok(Self { sftp: Some(sftp) })
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
        let mut file = sftp
            .open(path)
            .await
            .map_err(|error| map_sftp_error(path, error))?;
        let metadata = file
            .metadata()
            .await
            .map_err(|error| map_sftp_error(path, error))?;
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

        let encoded_header = read_file_pipelined(&file, path, 0, OBJECT_HEADER_LEN).await?;
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

    async fn create_dir_all(&mut self, path: &std::path::Path) -> Result<(), TransportError> {
        let sftp = self.sftp.as_ref().expect("open transport owns SFTP client");
        let mut fs = sftp.fs();
        let mut current = PathBuf::new();
        for component in path.components() {
            let std::path::Component::Normal(component) = component else {
                return Err(TransportError::Operation(format!(
                    "unsafe directory path {}",
                    path.display()
                )));
            };
            current.push(component);
            match fs.symlink_metadata(&current).await {
                Ok(metadata) if metadata.file_type().is_some_and(|kind| kind.is_dir()) => {}
                Ok(_) => {
                    return Err(TransportError::Operation(format!(
                        "{} exists and is not a directory",
                        current.display()
                    )));
                }
                Err(openssh_sftp_client::Error::SftpError(
                    openssh_sftp_client::error::SftpErrorKind::NoSuchFile,
                    _,
                )) => fs
                    .create_dir(&current)
                    .await
                    .map_err(|error| map_sftp_error(&current, error))?,
                Err(error) => return Err(map_sftp_error(&current, error)),
            }
        }
        Ok(())
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

    async fn close(mut self: Box<Self>) -> Result<(), TransportError> {
        self.sftp
            .take()
            .expect("open transport owns SFTP client")
            .close()
            .await
            .map_err(|_| TransportError::Close("OpenSSH SFTP shutdown failed".to_owned()))
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
    transport: Box<dyn TransportSession>,
    _lifetime: OwnedSemaphorePermit,
}

impl PhysicalSession {
    async fn close(self) -> Result<(), TransportError> {
        let Self {
            transport,
            _lifetime,
        } = self;
        let result = transport.close().await;
        drop(_lifetime);
        result
    }
}

struct PoolInner {
    factory: Arc<dyn SessionFactory>,
    shared: Arc<Semaphore>,
    admission: FairAdmission,
    idle: Mutex<VecDeque<PhysicalSession>>,
    idle_available: Notify,
    writable: bool,
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
                writable: true,
            }),
        };

        let session = pool.open_physical().await?;
        pool.inner.idle.lock().await.push_back(session);
        Ok(pool)
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

        let session = self.checkout_physical().await?;
        Ok(SessionLease {
            pool: self.inner.clone(),
            session: Some(session),
            admission: Some(admission),
        })
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
        let factory = self.inner.factory.clone();
        let writable = self.inner.writable;
        let (sender, receiver) = oneshot::channel();
        tokio::spawn(async move {
            let result = match factory.open().await {
                Err(error) => Err(error),
                Ok(transport) => {
                    if writable {
                        if let Err(error) =
                            require_publication_capabilities(transport.capabilities())
                        {
                            let _ = transport.close().await;
                            Err(error)
                        } else {
                            Ok(PhysicalSession {
                                transport,
                                _lifetime: permit,
                            })
                        }
                    } else {
                        Ok(PhysicalSession {
                            transport,
                            _lifetime: permit,
                        })
                    }
                }
            };
            if let Err(result) = sender.send(result)
                && let Ok(session) = result
            {
                let _ = session.close().await;
            }
        });
        receiver.await.map_err(|_| TransportError::PoolClosed)?
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SessionDisposition {
    Reuse,
    BrokenOrAmbiguous,
}

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
}

impl fmt::Debug for SessionLease {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SessionLease")
            .finish_non_exhaustive()
    }
}

impl SessionLease {
    pub fn capabilities(&self) -> SftpCapabilities {
        self.session
            .as_ref()
            .expect("lease always owns a session until completion")
            .transport
            .capabilities()
    }

    pub async fn read_object(
        &mut self,
        path: &std::path::Path,
        range: Option<object_store::GetRange>,
        head: bool,
    ) -> Result<RemoteObjectRead, TransportError> {
        self.session
            .as_mut()
            .expect("lease always owns a session until completion")
            .transport
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
            .transport
            .list_directory(path)
            .await
    }

    pub async fn remove_file(&mut self, path: &std::path::Path) -> Result<(), TransportError> {
        self.session
            .as_mut()
            .expect("lease always owns a session until completion")
            .transport
            .remove_file(path)
            .await
    }

    pub async fn create_dir_all(&mut self, path: &std::path::Path) -> Result<(), TransportError> {
        self.session
            .as_mut()
            .expect("lease always owns a session until completion")
            .transport
            .create_dir_all(path)
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
            .transport
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
            .transport
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
            .transport
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
            .transport
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
            .transport
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
            .transport
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
        let returning = tokio::spawn(async move {
            pool.idle.lock().await.push_back(session);
            pool.idle_available.notify_one();
            drop(admission);
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
        let cleanup = tokio::spawn(async move {
            let result = session.close().await;
            drop(admission);
            result
        });
        cleanup
            .await
            .map_err(|_| TransportError::Close("SFTP retirement task failed".to_owned()))?
    }

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
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            runtime.spawn(async move {
                let _ = session.close().await;
                drop(admission);
            });
        } else {
            // Releasing a physical permit before an async close would make a
            // ninth connection possible. Without a runtime, retain both.
            std::mem::forget(session);
            std::mem::forget(admission);
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
        plan_pipelined_writes,
    };
    use crate::config::SftpEndpoint;
    use crate::sftp_object_store::{ObjectHeader, SftpCapabilities, encode_header};
    use async_trait::async_trait;
    use bytes::Bytes;
    use std::fmt;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};
    use tokio::sync::Notify;

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
        allow_close: Notify,
        block_close: AtomicUsize,
        open_started: Notify,
        allow_open: Notify,
        block_open_from: AtomicUsize,
        fail_close: AtomicUsize,
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
                    allow_close: Notify::new(),
                    block_close: AtomicUsize::new(0),
                    open_started: Notify::new(),
                    allow_open: Notify::new(),
                    block_open_from: AtomicUsize::new(0),
                    fail_close: AtomicUsize::new(0),
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
        async fn open(&self) -> Result<Box<dyn TransportSession>, TransportError> {
            let id = self.state.dials.fetch_add(1, Ordering::SeqCst) + 1;
            let live = self.state.live.fetch_add(1, Ordering::SeqCst) + 1;
            self.state.peak.fetch_max(live, Ordering::SeqCst);
            let block_open_from = self.state.block_open_from.load(Ordering::SeqCst);
            if block_open_from != 0 && id >= block_open_from {
                self.state.open_started.notify_waiters();
                self.state.allow_open.notified().await;
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

        async fn close(self: Box<Self>) -> Result<(), TransportError> {
            if self.state.block_close.load(Ordering::SeqCst) != 0 {
                self.state.close_started.notify_waiters();
                self.state.allow_close.notified().await;
            }
            self.state.live.fetch_sub(1, Ordering::SeqCst);
            if self.state.fail_close.load(Ordering::SeqCst) != 0 {
                return Err(TransportError::Close("forced close failure".to_owned()));
            }
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

        tokio::task::yield_now().await;
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
        tokio::task::yield_now().await;
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
        tokio::task::yield_now().await;
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
    async fn cross_direction_waiters_are_admitted_in_fifo_order() {
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
        tokio::task::yield_now().await;
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
    async fn broken_operation_preserves_close_failure_as_cleanup_evidence() {
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
        assert_eq!(factory.live(), 0);
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
        Box::new(session).close().await.unwrap();
        assert!(child.wait().await.unwrap().success());
    }
}
