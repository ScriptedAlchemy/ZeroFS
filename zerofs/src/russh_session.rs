use crate::sftp_object_store::{OBJECT_HEADER_LEN, SftpCapabilities, decode_header};
use crate::sftp_transport::{
    RemoteDirectoryEntry, RemoteEntryKind, RemoteObjectRead, SessionFactory, TransportError,
    TransportSession,
};
use async_trait::async_trait;
use bytes::{Buf, Bytes, BytesMut};
use futures::{StreamExt, TryStreamExt};
use russh::keys::{PrivateKeyWithHashAlg, load_secret_key};
use russh::{ChannelMsg, client};
use russh_sftp::client::rawsession::RawSftpSession;
use russh_sftp::extensions::HardlinkExtension;
use russh_sftp::protocol::{FileAttributes, OpenFlags, StatusCode};
use std::borrow::Cow;
use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_util::sync::CancellationToken;

#[cfg(test)]
use std::io;
#[cfg(test)]
use std::pin::Pin;
#[cfg(test)]
use std::task::{Context, Poll};
#[cfg(test)]
use tokio::io::ReadBuf;

/// HPN-style static SSH channel window. russh has no dynamic scaler.
pub const RUSSH_WINDOW_SIZE: u32 = 16 * 1024 * 1024;
/// russh channel_buffer_size is an mpsc *message* depth, not bytes.
/// 1024 slots covers a 16 MiB window of 256 KiB packets plus control messages.
pub const RUSSH_CHANNEL_BUFFER_SIZE: usize = 1024;
/// 256 KiB SSH packets. russh's default 32 KiB is the ACK-per-write trap's cousin.
pub const RUSSH_MAXIMUM_PACKET_SIZE: u32 = 256 * 1024;
/// russh-sftp 2.4 packet cap. Must match the SSH maximum packet size.
pub const RUSSH_SFTP_MAX_PACKET_LEN: u32 = 256 * 1024;
/// OpenSSH sftp's proven in-flight WRITE window. The crate default of 8 is leftover.
pub const RUSSH_SFTP_MAX_CONCURRENT_WRITES: usize = 64;

const SFTP_WRITE_PACKET_SIZE: usize = 255 * 1024;
const SFTP_READ_PACKET_SIZE: usize = 255 * 1024;
const SFTP_WRITE_REQUEST_CONCURRENCY: usize = RUSSH_SFTP_MAX_CONCURRENT_WRITES;
const SFTP_READ_REQUEST_CONCURRENCY: usize = RUSSH_SFTP_MAX_CONCURRENT_WRITES;
const SFTP_SESSION_OPEN_TIMEOUT: Duration = Duration::from_secs(30);
const POSIX_RENAME: &str = "posix-rename@openssh.com";
const FSYNC: &str = "fsync@openssh.com";
const HARDLINK: &str = "hardlink@openssh.com";

struct PipelinedWrite {
    offset: u64,
    payload: Bytes,
}

struct PipelinedRead {
    index: usize,
    offset: u64,
    len: usize,
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

pub fn russh_client_config() -> client::Config {
    client::Config {
        window_size: RUSSH_WINDOW_SIZE,
        maximum_packet_size: RUSSH_MAXIMUM_PACKET_SIZE,
        channel_buffer_size: RUSSH_CHANNEL_BUFFER_SIZE,
        preferred: russh::Preferred {
            cipher: Cow::Borrowed(&[
                russh::cipher::AES_256_GCM,
                russh::cipher::AES_128_GCM,
                russh::cipher::AES_256_CTR,
                russh::cipher::AES_192_CTR,
                russh::cipher::AES_128_CTR,
            ]),
            ..russh::Preferred::DEFAULT
        },
        // The session pool owns idle lifetime. A russh inactivity timeout
        // would murder warm connections sitting in the idle queue.
        inactivity_timeout: None,
        keepalive_interval: Some(Duration::from_secs(30)),
        keepalive_max: 3,
        nodelay: true,
        ..client::Config::default()
    }
}

pub fn russh_sftp_config() -> russh_sftp::client::Config {
    russh_sftp::client::Config {
        max_packet_len: RUSSH_SFTP_MAX_PACKET_LEN,
        max_concurrent_writes: RUSSH_SFTP_MAX_CONCURRENT_WRITES,
        request_timeout_secs: 60,
    }
}

fn sftp_path(path: &Path) -> Result<String, TransportError> {
    path.to_str()
        .map(str::to_owned)
        .ok_or_else(|| TransportError::Operation(format!("non-UTF8 SFTP path {}", path.display())))
}

fn has_extension(version: &russh_sftp::protocol::Version, name: &str) -> bool {
    version
        .extensions
        .get(name)
        .is_some_and(|value| value == "1")
}

fn map_sftp_error(path: &Path, error: russh_sftp::client::error::Error) -> TransportError {
    match error {
        russh_sftp::client::error::Error::Status(status)
            if status.status_code == StatusCode::NoSuchFile =>
        {
            TransportError::NotFound(path.display().to_string())
        }
        russh_sftp::client::error::Error::Status(status)
            if status.status_code == StatusCode::PermissionDenied =>
        {
            TransportError::PermissionDenied(path.display().to_string())
        }
        error => TransportError::Operation(format!("{}: {error}", path.display())),
    }
}

#[cfg(test)]
struct Duplex<R, W> {
    reader: R,
    writer: W,
}

#[cfg(test)]
impl<R: AsyncRead + Unpin, W: Unpin> AsyncRead for Duplex<R, W> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.reader).poll_read(cx, buf)
    }
}

#[cfg(test)]
impl<R: Unpin, W: AsyncWrite + Unpin> AsyncWrite for Duplex<R, W> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, io::Error>> {
        Pin::new(&mut self.writer).poll_write(cx, buf)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), io::Error>> {
        Pin::new(&mut self.writer).poll_flush(cx)
    }

    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), io::Error>> {
        Pin::new(&mut self.writer).poll_shutdown(cx)
    }
}

struct StrictHostKey {
    host: String,
    port: u16,
    known_hosts: PathBuf,
}

impl client::Handler for StrictHostKey {
    type Error = russh::Error;

    async fn check_server_key(
        &mut self,
        server_public_key: &russh::keys::PublicKey,
    ) -> Result<bool, Self::Error> {
        match russh::keys::check_known_hosts_path(
            &self.host,
            self.port,
            server_public_key,
            &self.known_hosts,
        ) {
            Ok(true) => Ok(true),
            Ok(false) => {
                tracing::error!(
                    host = %self.host,
                    port = self.port,
                    "SSH host key is missing from or does not match known_hosts"
                );
                Ok(false)
            }
            Err(error) => {
                tracing::error!(
                    host = %self.host,
                    port = self.port,
                    %error,
                    "failed to consult known_hosts"
                );
                Ok(false)
            }
        }
    }
}

pub struct RusshSessionFactory {
    endpoint: crate::config::SftpEndpoint,
    identity_file: PathBuf,
    known_hosts: PathBuf,
}

/// Historical name kept so object-store tests and call sites fold onto russh.
#[cfg(test)]
#[allow(dead_code)]
pub type OpenSshSessionFactory = RusshSessionFactory;

impl RusshSessionFactory {
    pub fn new(
        endpoint: crate::config::SftpEndpoint,
        identity_file: PathBuf,
        known_hosts: PathBuf,
    ) -> Result<Self, TransportError> {
        Ok(Self {
            endpoint,
            identity_file,
            known_hosts,
        })
    }
}

impl fmt::Debug for RusshSessionFactory {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RusshSessionFactory")
            .field("host", &self.endpoint.host)
            .field("port", &self.endpoint.port)
            .field("known_hosts", &self.known_hosts)
            .field("window_size", &RUSSH_WINDOW_SIZE)
            .field("maximum_packet_size", &RUSSH_MAXIMUM_PACKET_SIZE)
            .finish_non_exhaustive()
    }
}

async fn handshake_sftp<S>(stream: S) -> Result<(RawSftpSession, SftpCapabilities), TransportError>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let mut raw = RawSftpSession::new_with_config(stream, russh_sftp_config());
    let version = raw
        .init()
        .await
        .map_err(|error| TransportError::Open(format!("SFTP handshake failed: {error}")))?;
    if has_extension(&version, russh_sftp::extensions::LIMITS) {
        if let Ok(limits) = raw.limits().await {
            raw.set_limits(russh_sftp::client::rawsession::Limits::from(limits));
        }
    }
    Ok((
        raw,
        SftpCapabilities {
            fsync: has_extension(&version, FSYNC),
            hardlink: has_extension(&version, HARDLINK),
            posix_rename: has_extension(&version, POSIX_RENAME),
        },
    ))
}

#[async_trait]
impl SessionFactory for RusshSessionFactory {
    async fn open(
        &self,
        force: CancellationToken,
    ) -> Result<Box<dyn TransportSession>, TransportError> {
        if force.is_cancelled() {
            return Err(TransportError::PoolClosed);
        }

        let connect = async {
            let key = load_secret_key(&self.identity_file, None).map_err(|error| {
                TransportError::Open(format!(
                    "failed to load identity {}: {error}",
                    self.identity_file.display()
                ))
            })?;
            let handler = StrictHostKey {
                host: self.endpoint.host.clone(),
                port: self.endpoint.port,
                known_hosts: self.known_hosts.clone(),
            };
            let mut handle = client::connect(
                Arc::new(russh_client_config()),
                (self.endpoint.host.as_str(), self.endpoint.port),
                handler,
            )
            .await
            .map_err(|error| {
                TransportError::Open(format!(
                    "russh connect to {}:{} failed: {error}",
                    self.endpoint.host, self.endpoint.port
                ))
            })?;
            let hash = handle
                .best_supported_rsa_hash()
                .await
                .map_err(|error| TransportError::Open(format!("RSA hash probe failed: {error}")))?
                .flatten();
            let authenticated = handle
                .authenticate_publickey(
                    self.endpoint.username.as_str(),
                    PrivateKeyWithHashAlg::new(Arc::new(key), hash),
                )
                .await
                .map_err(|error| {
                    TransportError::Open(format!("public-key authentication failed: {error}"))
                })?;
            if !authenticated.success() {
                return Err(TransportError::Open(format!(
                    "public-key authentication rejected by {}:{}",
                    self.endpoint.host, self.endpoint.port
                )));
            }

            let channel = handle.channel_open_session().await.map_err(|error| {
                TransportError::Open(format!("failed to open SSH session channel: {error}"))
            })?;
            channel
                .request_subsystem(true, "sftp")
                .await
                .map_err(|error| {
                    TransportError::Open(format!("failed to start SFTP subsystem: {error}"))
                })?;
            let (sftp, capabilities) = handshake_sftp(channel.into_stream()).await?;
            tracing::info!(
                host = %self.endpoint.host,
                port = self.endpoint.port,
                window_size = RUSSH_WINDOW_SIZE,
                maximum_packet_size = RUSSH_MAXIMUM_PACKET_SIZE,
                max_concurrent_writes = RUSSH_SFTP_MAX_CONCURRENT_WRITES,
                fsync = capabilities.fsync,
                hardlink = capabilities.hardlink,
                posix_rename = capabilities.posix_rename,
                "opened russh SFTP session"
            );
            Ok(Box::new(RusshTransportSession {
                handle: Some(handle),
                sftp: Some(sftp),
                capabilities,
                scp_disabled: AtomicBool::new(false),
                force: force.clone(),
            }) as Box<dyn TransportSession>)
        };

        tokio::select! {
            result = connect => result,
            _ = force.cancelled() => Err(TransportError::PoolClosed),
            _ = tokio::time::sleep(SFTP_SESSION_OPEN_TIMEOUT) => Err(TransportError::Open(
                format!(
                    "russh SFTP open to {}:{} timed out",
                    self.endpoint.host, self.endpoint.port
                ),
            )),
        }
    }
}

pub struct RusshTransportSession {
    handle: Option<client::Handle<StrictHostKey>>,
    sftp: Option<RawSftpSession>,
    capabilities: SftpCapabilities,
    scp_disabled: AtomicBool,
    force: CancellationToken,
}

/// Historical name kept so local sftp-server tests fold onto russh-sftp.
#[cfg(test)]
pub type OpenSshTransportSession = RusshTransportSession;

impl fmt::Debug for RusshTransportSession {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RusshTransportSession")
            .field("capabilities", &self.capabilities)
            .finish_non_exhaustive()
    }
}

impl Drop for RusshTransportSession {
    fn drop(&mut self) {
        self.force.cancel();
        if let Some(sftp) = self.sftp.take() {
            let _ = sftp.close_session();
        }
    }
}

impl RusshTransportSession {
    #[cfg(test)]
    pub async fn from_streams<W, R>(stdin: W, stdout: R) -> Result<Self, TransportError>
    where
        W: AsyncWrite + Unpin + Send + 'static,
        R: AsyncRead + Unpin + Send + 'static,
    {
        let (sftp, capabilities) = handshake_sftp(Duplex {
            reader: stdout,
            writer: stdin,
        })
        .await?;
        Ok(Self {
            handle: None,
            sftp: Some(sftp),
            capabilities,
            scp_disabled: AtomicBool::new(true),
            force: CancellationToken::new(),
        })
    }

    fn sftp(&self) -> Result<&RawSftpSession, TransportError> {
        self.sftp
            .as_ref()
            .ok_or_else(|| TransportError::Operation("SFTP session is closed".to_owned()))
    }

    async fn write_chunks(
        &self,
        path: &Path,
        offset: u64,
        chunks: Vec<Bytes>,
        create: bool,
        durable: bool,
    ) -> Result<(), TransportError> {
        if create
            && offset == 0
            && self.handle.is_some()
            && !self.scp_disabled.load(Ordering::Relaxed)
        {
            match self.write_file_via_scp(path, &chunks).await {
                Ok(()) => {
                    if durable {
                        self.fsync_path(path).await?;
                    }
                    return Ok(());
                }
                Err(error) => {
                    tracing::debug!(
                        path = %path.display(),
                        %error,
                        "SCP bulk write unavailable; falling back to pipelined SFTP"
                    );
                    self.scp_disabled.store(true, Ordering::Relaxed);
                }
            }
        }
        self.write_file_via_sftp(path, offset, chunks, create, durable)
            .await
    }

    async fn write_file_via_sftp(
        &self,
        path: &Path,
        offset: u64,
        chunks: Vec<Bytes>,
        create: bool,
        durable: bool,
    ) -> Result<(), TransportError> {
        let sftp = self.sftp()?;
        let remote = sftp_path(path)?;
        let flags = if create {
            OpenFlags::CREATE | OpenFlags::TRUNCATE | OpenFlags::WRITE
        } else {
            OpenFlags::WRITE
        };
        let opened = sftp
            .open(remote, flags, FileAttributes::default())
            .await
            .map_err(|error| map_sftp_error(path, error))?;
        let handle = opened.handle;
        let result = write_handle_pipelined(sftp, &handle, path, offset, chunks).await;
        if durable && result.is_ok() {
            if let Err(error) = sftp.fsync(handle.as_str()).await {
                let _ = sftp.close(handle.clone()).await;
                return Err(map_sftp_error(path, error));
            }
        }
        let close = sftp
            .close(handle)
            .await
            .map_err(|error| map_sftp_error(path, error));
        result.and(close.map(|_| ()))
    }

    async fn fsync_path(&self, path: &Path) -> Result<(), TransportError> {
        let sftp = self.sftp()?;
        let remote = sftp_path(path)?;
        let opened = sftp
            .open(remote, OpenFlags::WRITE, FileAttributes::default())
            .await
            .map_err(|error| map_sftp_error(path, error))?;
        let handle = opened.handle;
        let sync = sftp
            .fsync(handle.as_str())
            .await
            .map_err(|error| map_sftp_error(path, error));
        let close = sftp
            .close(handle)
            .await
            .map_err(|error| map_sftp_error(path, error));
        sync.and(close.map(|_| ()))
    }

    async fn write_file_via_scp(
        &self,
        path: &Path,
        chunks: &[Bytes],
    ) -> Result<(), TransportError> {
        let handle = self
            .handle
            .as_ref()
            .ok_or_else(|| TransportError::Operation("SCP requires a russh handle".to_owned()))?;
        let remote = sftp_path(path)?;
        let file_name = path
            .file_name()
            .and_then(|name| name.to_str())
            .ok_or_else(|| {
                TransportError::Operation(format!("SCP target {} has no file name", path.display()))
            })?;
        let size = chunks.iter().try_fold(0u64, |acc, chunk| {
            acc.checked_add(chunk.len() as u64)
                .ok_or_else(|| TransportError::Operation("SCP payload length overflow".to_owned()))
        })?;
        let command = scp_sink_command(&remote)?;
        let mut channel = handle.channel_open_session().await.map_err(|error| {
            TransportError::Operation(format!("failed to open SCP channel: {error}"))
        })?;
        channel
            .exec(true, command.as_str())
            .await
            .map_err(|error| TransportError::Operation(format!("SCP exec failed: {error}")))?;
        scp_expect_ok(&mut channel, path).await?;
        let header = Bytes::from(format!("C0644 {size} {file_name}\n"));
        channel.data_bytes(header).await.map_err(|error| {
            TransportError::Operation(format!("SCP header send failed: {error}"))
        })?;
        scp_expect_ok(&mut channel, path).await?;
        for chunk in chunks {
            for piece in chunk.chunks(RUSSH_MAXIMUM_PACKET_SIZE as usize) {
                channel.data_bytes(piece.to_vec()).await.map_err(|error| {
                    TransportError::Operation(format!("SCP data send failed: {error}"))
                })?;
            }
        }
        channel
            .data_bytes(Bytes::from_static(&[0]))
            .await
            .map_err(|error| {
                TransportError::Operation(format!("SCP trailer send failed: {error}"))
            })?;
        scp_expect_ok(&mut channel, path).await?;
        channel
            .eof()
            .await
            .map_err(|error| TransportError::Operation(format!("SCP eof failed: {error}")))?;
        channel
            .close()
            .await
            .map_err(|error| TransportError::Operation(format!("SCP close failed: {error}")))?;
        Ok(())
    }
}

fn scp_sink_command(remote: &str) -> Result<String, TransportError> {
    if remote
        .as_bytes()
        .iter()
        .any(|byte| matches!(byte, 0 | b'\n' | b'\r'))
    {
        return Err(TransportError::Operation(
            "SCP path contains a control character".to_owned(),
        ));
    }
    Ok(format!("scp -t -- {}", shell_single_quote(remote)))
}

fn shell_single_quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', "'\\''"))
}

async fn scp_expect_ok(
    channel: &mut russh::Channel<client::Msg>,
    path: &Path,
) -> Result<(), TransportError> {
    let mut pending = BytesMut::new();
    loop {
        match channel.wait().await {
            Some(ChannelMsg::Data { ref data }) => {
                pending.extend_from_slice(data);
                if pending.is_empty() {
                    continue;
                }
                match pending[0] {
                    0 => {
                        pending.advance(1);
                        return Ok(());
                    }
                    1 | 2 => {
                        let message = String::from_utf8_lossy(&pending[1..]).trim().to_owned();
                        return Err(TransportError::Operation(format!(
                            "SCP rejected {}: {message}",
                            path.display()
                        )));
                    }
                    _ => {
                        return Err(TransportError::Operation(format!(
                            "SCP produced an unexpected status for {}",
                            path.display()
                        )));
                    }
                }
            }
            Some(ChannelMsg::Failure) => {
                return Err(TransportError::Operation(format!(
                    "SCP exec rejected for {}",
                    path.display()
                )));
            }
            Some(ChannelMsg::ExitStatus { exit_status }) if exit_status != 0 => {
                return Err(TransportError::Operation(format!(
                    "SCP exited {exit_status} for {}",
                    path.display()
                )));
            }
            Some(ChannelMsg::Eof) | Some(ChannelMsg::Close) | None => {
                return Err(TransportError::Operation(format!(
                    "SCP channel closed before an ACK for {}",
                    path.display()
                )));
            }
            Some(ChannelMsg::Success) | Some(_) => {}
        }
    }
}

#[cfg(test)]
static CLIENT_WRITE_IN_FLIGHT: std::sync::atomic::AtomicUsize =
    std::sync::atomic::AtomicUsize::new(0);
#[cfg(test)]
static CLIENT_WRITE_PEAK: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

async fn write_handle_pipelined(
    sftp: &RawSftpSession,
    handle: &str,
    path: &Path,
    offset: u64,
    chunks: Vec<Bytes>,
) -> Result<(), TransportError> {
    let requests = plan_pipelined_writes(offset, chunks)?;
    futures::stream::iter(requests)
        .map(|request| {
            let handle = handle.to_owned();
            async move {
                #[cfg(test)]
                {
                    let current = CLIENT_WRITE_IN_FLIGHT.fetch_add(1, Ordering::SeqCst) + 1;
                    CLIENT_WRITE_PEAK.fetch_max(current, Ordering::SeqCst);
                }
                let result = sftp
                    .write(handle, request.offset, request.payload.to_vec())
                    .await
                    .map_err(|error| map_sftp_error(path, error));
                #[cfg(test)]
                {
                    CLIENT_WRITE_IN_FLIGHT.fetch_sub(1, Ordering::SeqCst);
                }
                result
            }
        })
        .buffer_unordered(SFTP_WRITE_REQUEST_CONCURRENCY)
        .try_collect::<Vec<_>>()
        .await?;
    Ok(())
}

async fn read_handle_pipelined(
    sftp: &RawSftpSession,
    handle: &str,
    path: &Path,
    offset: u64,
    len: usize,
) -> Result<Bytes, TransportError> {
    let requests = plan_pipelined_reads(offset, len)?;
    let mut buffer = BytesMut::zeroed(len);
    let mut reads = Vec::with_capacity(requests.len());
    for request in requests {
        let region = buffer.split_to(request.len);
        reads.push((request, region));
    }
    let mut chunks = futures::stream::iter(reads)
        .map(|(request, mut region)| {
            let handle = handle.to_owned();
            async move {
                let data = sftp
                    .read(handle, request.offset, request.len as u32)
                    .await
                    .map_err(|error| {
                        TransportError::Operation(format!(
                            "short read from {} at {}: {error}",
                            path.display(),
                            request.offset
                        ))
                    })?;
                if data.data.len() != request.len {
                    return Err(TransportError::Operation(format!(
                        "short read from {} at {}: got {} want {}",
                        path.display(),
                        request.offset,
                        data.data.len(),
                        request.len
                    )));
                }
                region.copy_from_slice(&data.data);
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

async fn stat_directory(
    sftp: &RawSftpSession,
    path: &Path,
) -> Result<FileAttributes, russh_sftp::client::error::Error> {
    let started = tokio::time::Instant::now();
    let metadata = sftp
        .lstat(sftp_path(path).map_err(|error| {
            russh_sftp::client::error::Error::UnexpectedBehavior(error.to_string())
        })?)
        .await;
    metrics::counter!("zerofs_sftp_directory_stats_total").increment(1);
    metrics::histogram!("zerofs_sftp_directory_stat_duration_seconds")
        .record(started.elapsed().as_secs_f64());
    metadata.map(|attrs| attrs.attrs)
}

#[async_trait]
impl TransportSession for RusshTransportSession {
    fn capabilities(&self) -> SftpCapabilities {
        self.capabilities
    }

    async fn read_object(
        &mut self,
        path: &Path,
        requested_range: Option<object_store::GetRange>,
        head: bool,
    ) -> Result<RemoteObjectRead, TransportError> {
        let sftp = self.sftp()?;
        let remote = sftp_path(path)?;
        let opened = sftp
            .open(remote, OpenFlags::READ, FileAttributes::default())
            .await
            .map_err(|error| map_sftp_error(path, error))?;
        let handle = opened.handle;
        let speculative_range = match (&requested_range, head) {
            (Some(object_store::GetRange::Bounded(range)), false) if range.start < range.end => {
                usize::try_from(range.end - range.start)
                    .ok()
                    .and_then(|len| {
                        (OBJECT_HEADER_LEN as u64)
                            .checked_add(range.start)
                            .map(|physical_start| (range.clone(), physical_start, len))
                    })
            }
            _ => None,
        };
        let speculative_payload = async {
            match &speculative_range {
                Some((_, physical_start, len)) => {
                    Some(read_handle_pipelined(sftp, &handle, path, *physical_start, *len).await)
                }
                None => None,
            }
        };
        let (metadata, encoded_header, speculative_payload) = tokio::join!(
            sftp.fstat(handle.as_str()),
            read_handle_pipelined(sftp, &handle, path, 0, OBJECT_HEADER_LEN),
            speculative_payload,
        );
        let metadata = metadata.map_err(|error| map_sftp_error(path, error))?.attrs;
        if !metadata.is_regular() {
            let _ = sftp.close(handle.clone()).await;
            return Err(TransportError::CorruptObject(format!(
                "{} is not a regular file",
                path.display()
            )));
        }
        let physical_len = metadata.size.ok_or_else(|| {
            TransportError::CorruptObject(format!("{} has no physical length", path.display()))
        })?;
        let modified = metadata.modified().map_err(|_| {
            TransportError::CorruptObject(format!("{} has no modification time", path.display()))
        })?;

        let encoded_header = match encoded_header {
            Ok(header) => header,
            Err(error) => {
                let _ = sftp.close(handle.clone()).await;
                return Err(error);
            }
        };
        let header = match decode_header(&encoded_header) {
            Ok(header) => header,
            Err(error) => {
                let _ = sftp.close(handle.clone()).await;
                return Err(TransportError::CorruptObject(format!(
                    "{}: {error}",
                    path.display()
                )));
            }
        };
        let expected_physical_len = (OBJECT_HEADER_LEN as u64)
            .checked_add(header.logical_len)
            .ok_or_else(|| {
                TransportError::CorruptObject(format!(
                    "{} logical length overflows its physical representation",
                    path.display()
                ))
            })?;
        if physical_len != expected_physical_len {
            let _ = sftp.close(handle.clone()).await;
            return Err(TransportError::CorruptObject(format!(
                "{} physical length {physical_len} does not match expected {expected_physical_len}",
                path.display()
            )));
        }

        let range = match requested_range {
            Some(range) => range.as_range(header.logical_len).map_err(|error| {
                TransportError::InvalidRange(format!(
                    "invalid logical range for {}: {error}",
                    path.display()
                ))
            })?,
            None => 0..header.logical_len,
        };
        let payload = if head || range.is_empty() {
            Bytes::new()
        } else if let (Some(result), Some((requested, _, _))) =
            (speculative_payload, &speculative_range)
            && *requested == range
        {
            match result {
                Ok(payload) => payload,
                Err(error) => {
                    let _ = sftp.close(handle.clone()).await;
                    return Err(error);
                }
            }
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
            match read_handle_pipelined(sftp, &handle, path, physical_start, len).await {
                Ok(payload) => payload,
                Err(error) => {
                    let _ = sftp.close(handle.clone()).await;
                    return Err(error);
                }
            }
        };
        let close_path = path.to_path_buf();
        let close_handle = handle.clone();
        let close_sftp = self.sftp()?;
        // The handle close acknowledges nothing the caller depends on.
        let _ = close_sftp.close(close_handle).await.map_err(|error| {
            tracing::debug!(
                path = %close_path.display(),
                %error,
                "SFTP read handle close failed off the critical path"
            );
        });
        Ok(RemoteObjectRead {
            header,
            modified,
            range,
            payload,
        })
    }

    async fn list_directory(
        &mut self,
        path: &Path,
    ) -> Result<Vec<RemoteDirectoryEntry>, TransportError> {
        let sftp = self.sftp()?;
        let remote = sftp_path(path)?;
        let opened = sftp
            .opendir(remote)
            .await
            .map_err(|error| map_sftp_error(path, error))?;
        let handle = opened.handle;
        let mut result = Vec::new();
        loop {
            match sftp.readdir(handle.as_str()).await {
                Ok(name) => {
                    for file in name.files {
                        if file.filename == "." || file.filename == ".." {
                            continue;
                        }
                        let kind = if file.attrs.is_regular() {
                            RemoteEntryKind::File
                        } else if file.attrs.is_dir() {
                            RemoteEntryKind::Directory
                        } else if file.attrs.is_symlink() {
                            RemoteEntryKind::Symlink
                        } else {
                            RemoteEntryKind::Other
                        };
                        result.push(RemoteDirectoryEntry {
                            filename: PathBuf::from(file.filename),
                            kind,
                        });
                    }
                }
                Err(russh_sftp::client::error::Error::Status(status))
                    if status.status_code == StatusCode::Eof =>
                {
                    break;
                }
                Err(error) => {
                    let _ = sftp.close(handle.clone()).await;
                    return Err(map_sftp_error(path, error));
                }
            }
        }
        sftp.close(handle)
            .await
            .map_err(|error| map_sftp_error(path, error))?;
        Ok(result)
    }

    async fn remove_file(&mut self, path: &Path) -> Result<(), TransportError> {
        let remote = sftp_path(path)?;
        self.sftp()?
            .remove(remote)
            .await
            .map_err(|error| map_sftp_error(path, error))
            .map(|_| ())
    }

    async fn ensure_directory_component(&mut self, path: &Path) -> Result<(), TransportError> {
        let sftp = self.sftp()?;
        for component in path.components() {
            if !matches!(component, std::path::Component::Normal(_)) {
                return Err(TransportError::Operation(format!(
                    "unsafe directory path {}",
                    path.display()
                )));
            }
        }

        match stat_directory(sftp, path).await {
            Ok(metadata) if metadata.is_dir() => Ok(()),
            Ok(_) => Err(TransportError::Operation(format!(
                "{} exists and is not a directory",
                path.display()
            ))),
            Err(russh_sftp::client::error::Error::Status(status))
                if status.status_code == StatusCode::NoSuchFile =>
            {
                let mkdir_started = tokio::time::Instant::now();
                let created = sftp
                    .mkdir(sftp_path(path)?, FileAttributes::default())
                    .await;
                metrics::counter!("zerofs_sftp_directory_mkdirs_total").increment(1);
                metrics::histogram!("zerofs_sftp_directory_mkdir_duration_seconds")
                    .record(mkdir_started.elapsed().as_secs_f64());
                match created {
                    Ok(_) => Ok(()),
                    Err(create_error) => match stat_directory(sftp, path).await {
                        Ok(metadata) if metadata.is_dir() => Ok(()),
                        _ => Err(map_sftp_error(path, create_error)),
                    },
                }
            }
            Err(error) => Err(map_sftp_error(path, error)),
        }
    }

    async fn write_file_durable(
        &mut self,
        path: &Path,
        chunks: Vec<Bytes>,
    ) -> Result<(), TransportError> {
        self.write_chunks(path, 0, chunks, true, true).await
    }

    async fn write_file_at_durable(
        &mut self,
        path: &Path,
        offset: u64,
        chunks: Vec<Bytes>,
    ) -> Result<(), TransportError> {
        self.write_chunks(path, offset, chunks, false, true).await
    }

    async fn write_file_at(
        &mut self,
        path: &Path,
        offset: u64,
        chunks: Vec<Bytes>,
    ) -> Result<(), TransportError> {
        self.write_chunks(path, offset, chunks, false, false).await
    }

    async fn read_exact(
        &mut self,
        path: &Path,
        offset: u64,
        len: usize,
    ) -> Result<Bytes, TransportError> {
        let sftp = self.sftp()?;
        let remote = sftp_path(path)?;
        let opened = sftp
            .open(remote, OpenFlags::READ, FileAttributes::default())
            .await
            .map_err(|error| map_sftp_error(path, error))?;
        let handle = opened.handle;
        let bytes = read_handle_pipelined(sftp, &handle, path, offset, len).await;
        let close = sftp
            .close(handle)
            .await
            .map_err(|error| map_sftp_error(path, error));
        close?;
        bytes
    }

    async fn hard_link(&mut self, from: &Path, to: &Path) -> Result<(), TransportError> {
        let sftp = self.sftp()?;
        match sftp.hardlink(sftp_path(from)?, sftp_path(to)?).await {
            Ok(_) => Ok(()),
            Err(russh_sftp::client::error::Error::Status(status))
                if status.status_code == StatusCode::Failure =>
            {
                Err(TransportError::AlreadyExists(to.display().to_string()))
            }
            Err(error) => Err(map_sftp_error(to, error)),
        }
    }

    async fn posix_rename(&mut self, from: &Path, to: &Path) -> Result<(), TransportError> {
        if !self.capabilities.posix_rename {
            return Err(TransportError::MissingCapability(POSIX_RENAME));
        }
        let data: Vec<u8> = HardlinkExtension {
            oldpath: sftp_path(from)?,
            newpath: sftp_path(to)?,
        }
        .try_into()
        .map_err(|error| {
            TransportError::Operation(format!("failed to encode posix-rename: {error}"))
        })?;
        match self.sftp()?.extended(POSIX_RENAME, data).await {
            Ok(russh_sftp::protocol::Packet::Status(status))
                if status.status_code == StatusCode::Ok =>
            {
                Ok(())
            }
            Ok(russh_sftp::protocol::Packet::Status(status)) => Err(map_sftp_error(
                to,
                russh_sftp::client::error::Error::Status(status),
            )),
            Ok(_) => Err(TransportError::Operation(format!(
                "unexpected posix-rename reply for {}",
                to.display()
            ))),
            Err(error) => Err(map_sftp_error(to, error)),
        }
    }

    async fn close(mut self: Box<Self>, force: CancellationToken) -> Result<(), TransportError> {
        if let Some(sftp) = self.sftp.take() {
            let _ = sftp.close_session();
        }
        if let Some(handle) = self.handle.take() {
            let disconnect = handle.disconnect(russh::Disconnect::ByApplication, "", "");
            tokio::select! {
                result = disconnect => {
                    if let Err(error) = result {
                        tracing::warn!(%error, "russh disconnect failed after SFTP close");
                    }
                }
                _ = force.cancelled() => {}
                _ = self.force.cancelled() => {}
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::Ordering;

    const CLIENT_KEY: &str = r#"-----BEGIN OPENSSH PRIVATE KEY-----
b3BlbnNzaC1rZXktdjEAAAAABG5vbmUAAAAEbm9uZQAAAAAAAAABAAAAMwAAAAtzc2gtZW
QyNTUxOQAAACAQigpuTi7JbbNeJzxMkcn7aTGhwCw72ItsT8P0jEgymAAAAJgFEUUfBRFF
HwAAAAtzc2gtZWQyNTUxOQAAACAQigpuTi7JbbNeJzxMkcn7aTGhwCw72ItsT8P0jEgymA
AAAEDC+zNpo65+8VYQLiNUGNNxWqEww8yfkOaqMHdwmfuTzhCKCm5OLslts14nPEyRyftp
MaHALDvYi2xPw/SMSDKYAAAAEnplcm9mcy1jbGllbnQtdGVzdAECAw==
-----END OPENSSH PRIVATE KEY-----
"#;
    const SERVER_KEY: &str = r#"-----BEGIN OPENSSH PRIVATE KEY-----
b3BlbnNzaC1rZXktdjEAAAAABG5vbmUAAAAEbm9uZQAAAAAAAAABAAAAMwAAAAtzc2gtZW
QyNTUxOQAAACDC6IEYT3UM1UuN2KP2eb4ToFDSgq120q/tQOu7O1c+egAAAJg0qMDxNKjA
8QAAAAtzc2gtZWQyNTUxOQAAACDC6IEYT3UM1UuN2KP2eb4ToFDSgq120q/tQOu7O1c+eg
AAAEDU8+PlxIN0Fyv3xBh4UgtcDTbPt7CQ3KIkmRRR8dsOaMLogRhPdQzVS43Yo/Z5vhOg
UNKCrXbSr+1A67s7Vz56AAAAEnplcm9mcy1zZXJ2ZXItdGVzdAECAw==
-----END OPENSSH PRIVATE KEY-----
"#;
    const OTHER_KEY: &str = r#"-----BEGIN OPENSSH PRIVATE KEY-----
b3BlbnNzaC1rZXktdjEAAAAABG5vbmUAAAAEbm9uZQAAAAAAAAABAAAAMwAAAAtzc2gtZW
QyNTUxOQAAACCRi+8XRj8Tq1Or1mR02iOEWRKCh7xRszil1JfMIs9RjwAAAJhxG/RDcRv0
QwAAAAtzc2gtZWQyNTUxOQAAACCRi+8XRj8Tq1Or1mR02iOEWRKCh7xRszil1JfMIs9Rjw
AAAEAB9KRIf/0YJRj8Atj1eLnGRkrHutd6JDXafNflsuUDDJGL7xdGPxOrU6vWZHTaI4RZ
EoKHvFGzOKXUl8wiz1GPAAAAEXplcm9mcy1vdGhlci10ZXN0AQIDBA==
-----END OPENSSH PRIVATE KEY-----
"#;

    #[test]
    fn russh_client_config_uses_hpn_static_windows() {
        let config = russh_client_config();
        assert_eq!(config.window_size, 16 * 1024 * 1024);
        assert_eq!(config.maximum_packet_size, 256 * 1024);
        assert_eq!(config.channel_buffer_size, 1024);
        assert_eq!(config.inactivity_timeout, None);
        assert_eq!(
            config.keepalive_interval,
            Some(std::time::Duration::from_secs(30))
        );
        assert_eq!(config.keepalive_max, 3);
        assert!(config.nodelay);
        assert!(
            config.preferred.cipher.iter().all(|cipher| matches!(
                cipher.as_ref(),
                "aes256-gcm@openssh.com"
                    | "aes128-gcm@openssh.com"
                    | "aes256-ctr"
                    | "aes192-ctr"
                    | "aes128-ctr"
            )),
            "ciphers must be AES-GCM or AES-CTR: {:?}",
            config
                .preferred
                .cipher
                .iter()
                .map(|cipher| cipher.as_ref())
                .collect::<Vec<_>>()
        );
        assert!(
            !config
                .preferred
                .cipher
                .iter()
                .any(|cipher| cipher.as_ref().contains("cbc")
                    || cipher.as_ref() == "none"
                    || cipher.as_ref() == "clear")
        );
    }

    #[test]
    fn russh_sftp_config_pipelines_64_by_256kib() {
        let config = russh_sftp_config();
        assert_eq!(config.max_packet_len, 256 * 1024);
        assert_eq!(config.max_concurrent_writes, 64);
        assert_ne!(
            config.max_concurrent_writes,
            russh_sftp::client::Config::default().max_concurrent_writes,
            "must not preserve the leftover default of 8 (or 4)"
        );
    }

    #[test]
    fn scp_sink_command_quotes_the_remote_path() {
        assert_eq!(
            scp_sink_command("/data/object.bin").unwrap(),
            "scp -t -- '/data/object.bin'"
        );
        assert_eq!(
            scp_sink_command("/data/o'bject.bin").unwrap(),
            "scp -t -- '/data/o'\\''bject.bin'"
        );
        assert!(scp_sink_command("/data/bad\npath").is_err());
    }

    #[test]
    fn pipelined_write_plan_uses_255kib_packets_not_ack_per_byte() {
        let payload = Bytes::from(vec![0u8; SFTP_WRITE_PACKET_SIZE * 64 + 17]);
        let plan = plan_pipelined_writes(0, vec![payload]).unwrap();
        assert_eq!(plan.len(), 65);
        assert!(
            plan.iter()
                .all(|request| request.payload.len() <= SFTP_WRITE_PACKET_SIZE)
        );
        assert_eq!(plan[0].payload.len(), SFTP_WRITE_PACKET_SIZE);
        assert_eq!(plan[63].payload.len(), SFTP_WRITE_PACKET_SIZE);
        assert_eq!(plan[64].payload.len(), 17);
        assert_eq!(plan[1].offset, SFTP_WRITE_PACKET_SIZE as u64);
        assert_eq!(SFTP_WRITE_REQUEST_CONCURRENCY, 64);
        assert_eq!(SFTP_WRITE_PACKET_SIZE, 255 * 1024);
    }

    #[test]
    fn pipelined_read_plan_matches_the_write_window() {
        let plan = plan_pipelined_reads(0, SFTP_READ_PACKET_SIZE * 3).unwrap();
        assert_eq!(plan.len(), 3);
        assert_eq!(plan[2].offset, (SFTP_READ_PACKET_SIZE * 2) as u64);
        assert_eq!(SFTP_READ_REQUEST_CONCURRENCY, 64);
    }

    #[test]
    fn factory_debug_redacts_the_username() {
        let factory = RusshSessionFactory::new(
            crate::config::SftpEndpoint {
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
        assert!(debug.contains("16777216"));
        assert!(!debug.contains("account-secret-name"));
    }

    #[tokio::test]
    async fn russh_factory_rejects_an_unknown_host_key() {
        let env = Loopback::start().await;
        std::fs::write(&env.known_hosts, "").unwrap();
        let factory = RusshSessionFactory::new(
            env.endpoint.clone(),
            env.identity.clone(),
            env.known_hosts.clone(),
        )
        .unwrap();
        let error = factory
            .open(CancellationToken::new())
            .await
            .expect_err("unknown host keys must fail closed");
        assert!(matches!(error, TransportError::Open(_)), "{error:?}");
    }

    #[tokio::test]
    async fn russh_factory_rejects_a_changed_host_key() {
        let env = Loopback::start().await;
        let other = russh::keys::PrivateKey::from_openssh(OTHER_KEY).unwrap();
        russh::keys::known_hosts::learn_known_hosts_path(
            &env.endpoint.host,
            env.endpoint.port,
            other.public_key(),
            &env.known_hosts,
        )
        .unwrap();
        let factory = RusshSessionFactory::new(
            env.endpoint.clone(),
            env.identity.clone(),
            env.known_hosts.clone(),
        )
        .unwrap();
        let error = factory
            .open(CancellationToken::new())
            .await
            .expect_err("changed host keys must fail closed");
        assert!(matches!(error, TransportError::Open(_)), "{error:?}");
    }

    #[tokio::test]
    async fn russh_loopback_pipelines_writes_and_reads_with_hpn_windows() {
        let env = Loopback::start().await;
        let factory = RusshSessionFactory::new(
            env.endpoint.clone(),
            env.identity.clone(),
            env.known_hosts.clone(),
        )
        .unwrap();
        let mut session = factory
            .open(CancellationToken::new())
            .await
            .expect("native russh client must complete a loopback handshake");
        assert_eq!(
            session.capabilities(),
            crate::sftp_object_store::SftpCapabilities {
                fsync: true,
                hardlink: true,
                posix_rename: true,
            }
        );

        let packets = 32;
        let payload = Bytes::from(vec![0x5a; packets * SFTP_WRITE_PACKET_SIZE]);
        CLIENT_WRITE_IN_FLIGHT.store(0, Ordering::SeqCst);
        CLIENT_WRITE_PEAK.store(0, Ordering::SeqCst);
        let started = std::time::Instant::now();
        session
            .write_file_durable(std::path::Path::new("bulk.bin"), vec![payload.clone()])
            .await
            .unwrap();
        let elapsed = started.elapsed();
        let peak = CLIENT_WRITE_PEAK.load(Ordering::SeqCst);
        assert!(
            peak >= 16,
            "client must pipeline WRITE requests; peak in-flight={peak} elapsed={elapsed:?}"
        );
        eprintln!(
            "russh client WRITE pipeline: peak_in_flight={peak} packets={packets} elapsed={elapsed:?}"
        );
        let read = session
            .read_exact(std::path::Path::new("bulk.bin"), 0, payload.len())
            .await
            .unwrap();
        assert_eq!(read, payload);

        session
            .ensure_directory_component(std::path::Path::new("nested"))
            .await
            .unwrap();
        session
            .hard_link(
                std::path::Path::new("bulk.bin"),
                std::path::Path::new("nested/link.bin"),
            )
            .await
            .unwrap();
        session
            .posix_rename(
                std::path::Path::new("nested/link.bin"),
                std::path::Path::new("nested/renamed.bin"),
            )
            .await
            .unwrap();
        let entries = session
            .list_directory(std::path::Path::new("nested"))
            .await
            .unwrap();
        assert!(entries.iter().any(|entry| {
            entry.filename == std::path::Path::new("renamed.bin")
                && entry.kind == crate::sftp_transport::RemoteEntryKind::File
        }));
        session.close(CancellationToken::new()).await.unwrap();
        eprintln!(
            "russh loopback pipelined write: peak_in_flight={peak} elapsed={elapsed:?} bytes={}",
            payload.len()
        );
    }

    struct Loopback {
        _root: tempfile::TempDir,
        endpoint: crate::config::SftpEndpoint,
        identity: PathBuf,
        known_hosts: PathBuf,
        _inflight: std::sync::Arc<std::sync::atomic::AtomicUsize>,
        _peak: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    }

    impl Loopback {
        async fn start() -> Self {
            use russh::server::{Auth, Msg, Server, Session};
            use russh::{Channel, ChannelId};
            use std::net::SocketAddr;
            use tokio::net::TcpListener;

            let root = tempfile::tempdir().unwrap();
            let identity = root.path().join("id_ed25519");
            let known_hosts = root.path().join("known_hosts");
            let fs_root = root.path().join("fs");
            std::fs::create_dir(&fs_root).unwrap();

            let client_key = russh::keys::PrivateKey::from_openssh(CLIENT_KEY).unwrap();
            std::fs::write(&identity, CLIENT_KEY.as_bytes()).unwrap();
            let client_public = client_key.public_key().clone();

            let server_key = russh::keys::PrivateKey::from_openssh(SERVER_KEY).unwrap();
            let server_public = server_key.public_key().clone();

            let inflight = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
            let peak = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));

            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();
            russh::keys::known_hosts::learn_known_hosts_path(
                "127.0.0.1",
                addr.port(),
                &server_public,
                &known_hosts,
            )
            .unwrap();

            let mut server_config = russh::server::Config::default();
            server_config.window_size = RUSSH_WINDOW_SIZE;
            server_config.maximum_packet_size = RUSSH_MAXIMUM_PACKET_SIZE;
            server_config.channel_buffer_size = RUSSH_CHANNEL_BUFFER_SIZE;
            server_config.inactivity_timeout = None;
            server_config.nodelay = true;
            server_config.auth_rejection_time = Duration::from_secs(0);
            server_config.auth_rejection_time_initial = Some(Duration::from_secs(0));
            server_config.keys = vec![server_key];
            server_config.preferred = russh::Preferred {
                cipher: Cow::Borrowed(&[
                    russh::cipher::AES_256_GCM,
                    russh::cipher::AES_128_GCM,
                    russh::cipher::AES_256_CTR,
                    russh::cipher::AES_192_CTR,
                    russh::cipher::AES_128_CTR,
                ]),
                ..russh::Preferred::DEFAULT
            };
            let server_config = std::sync::Arc::new(server_config);

            #[derive(Clone)]
            struct ServerState {
                fs_root: PathBuf,
                client_public: russh::keys::PublicKey,
                inflight: std::sync::Arc<std::sync::atomic::AtomicUsize>,
                peak: std::sync::Arc<std::sync::atomic::AtomicUsize>,
            }

            struct SshSession {
                state: ServerState,
                channels: std::sync::Arc<
                    tokio::sync::Mutex<std::collections::HashMap<ChannelId, Channel<Msg>>>,
                >,
            }

            impl russh::server::Server for ServerState {
                type Handler = SshSession;
                fn new_client(&mut self, _: Option<SocketAddr>) -> Self::Handler {
                    SshSession {
                        state: self.clone(),
                        channels: std::sync::Arc::new(tokio::sync::Mutex::new(
                            std::collections::HashMap::new(),
                        )),
                    }
                }
            }

            impl russh::server::Handler for SshSession {
                type Error = anyhow::Error;

                async fn auth_publickey(
                    &mut self,
                    _user: &str,
                    public_key: &russh::keys::PublicKey,
                ) -> Result<Auth, Self::Error> {
                    // Compare key material only; comments and encoding wrappers differ.
                    if public_key.key_data() == self.state.client_public.key_data() {
                        Ok(Auth::Accept)
                    } else {
                        Ok(Auth::Reject {
                            proceed_with_methods: None,
                            partial_success: false,
                        })
                    }
                }

                async fn channel_open_session(
                    &mut self,
                    channel: Channel<Msg>,
                    reply: russh::server::ChannelOpenHandle,
                    _session: &mut Session,
                ) -> Result<(), Self::Error> {
                    self.channels.lock().await.insert(channel.id(), channel);
                    reply.accept().await;
                    Ok(())
                }

                async fn subsystem_request(
                    &mut self,
                    channel_id: ChannelId,
                    name: &str,
                    session: &mut Session,
                ) -> Result<(), Self::Error> {
                    if name != "sftp" {
                        session.channel_failure(channel_id)?;
                        return Ok(());
                    }
                    let channel = self
                        .channels
                        .lock()
                        .await
                        .remove(&channel_id)
                        .ok_or_else(|| anyhow::anyhow!("missing sftp channel"))?;
                    session.channel_success(channel_id)?;
                    let sftp = FsSftp {
                        root: self.state.fs_root.clone(),
                        files: std::collections::HashMap::new(),
                        dirs: std::collections::HashMap::new(),
                        next: 1,
                        inflight: self.state.inflight.clone(),
                        peak: self.state.peak.clone(),
                    };
                    // Drive SFTP off the russh session task so SSH packets
                    // keep flowing into ChannelStream.
                    tokio::spawn(russh_sftp::server::run(channel.into_stream(), sftp));
                    Ok(())
                }

                async fn exec_request(
                    &mut self,
                    channel_id: ChannelId,
                    _data: &[u8],
                    session: &mut Session,
                ) -> Result<(), Self::Error> {
                    // Loopback has no scp(1). Fail fast so durable writes
                    // measure the pipelined SFTP path instead of hanging
                    // on a want-reply exec.
                    session.channel_failure(channel_id)?;
                    Ok(())
                }
            }

            let mut server = ServerState {
                fs_root: fs_root.clone(),
                client_public,
                inflight: inflight.clone(),
                peak: peak.clone(),
            };
            tokio::spawn(async move {
                loop {
                    let (socket, _) = match listener.accept().await {
                        Ok(pair) => pair,
                        Err(_) => break,
                    };
                    let handler = server.new_client(socket.peer_addr().ok());
                    let config = server_config.clone();
                    tokio::spawn(async move {
                        if let Ok(running) =
                            russh::server::run_stream(config, socket, handler).await
                        {
                            let _ = running.await;
                        }
                    });
                }
            });

            Self {
                _root: root,
                endpoint: crate::config::SftpEndpoint {
                    host: "127.0.0.1".to_owned(),
                    port: addr.port(),
                    username: "zerofs".to_owned(),
                },
                identity,
                known_hosts,
                _inflight: inflight,
                _peak: peak,
            }
        }
    }

    struct Opened {
        path: PathBuf,
    }

    struct FsSftp {
        root: PathBuf,
        files: std::collections::HashMap<String, Opened>,
        dirs: std::collections::HashMap<String, std::vec::IntoIter<std::fs::DirEntry>>,
        next: u64,
        inflight: std::sync::Arc<std::sync::atomic::AtomicUsize>,
        peak: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    }

    impl FsSftp {
        fn resolve(&self, path: &str) -> PathBuf {
            let trimmed = path.trim_start_matches('/');
            if trimmed.is_empty() {
                self.root.clone()
            } else {
                self.root.join(trimmed)
            }
        }

        fn attrs(
            path: &std::path::Path,
        ) -> Result<FileAttributes, russh_sftp::protocol::StatusCode> {
            let meta = std::fs::symlink_metadata(path)
                .map_err(|_| russh_sftp::protocol::StatusCode::NoSuchFile)?;
            let mut attrs = FileAttributes::default();
            attrs.size = Some(meta.len());
            if meta.is_dir() {
                attrs.set_dir(true);
            } else if meta.is_file() {
                attrs.set_regular(true);
            } else if meta.file_type().is_symlink() {
                attrs.set_symlink(true);
            }
            if let Ok(modified) = meta.modified() {
                if let Ok(secs) = modified.duration_since(std::time::UNIX_EPOCH) {
                    attrs.mtime = Some(secs.as_secs() as u32);
                }
            }
            Ok(attrs)
        }

        fn ok(id: u32) -> russh_sftp::protocol::Status {
            russh_sftp::protocol::Status {
                id,
                status_code: StatusCode::Ok,
                error_message: "Ok".to_owned(),
                language_tag: "en-US".to_owned(),
            }
        }

        fn alloc(&mut self, path: PathBuf) -> String {
            let handle = format!("h{}", self.next);
            self.next += 1;
            self.files.insert(handle.clone(), Opened { path });
            handle
        }
    }

    impl russh_sftp::server::Handler for FsSftp {
        type Error = StatusCode;

        fn unimplemented(&self) -> Self::Error {
            StatusCode::OpUnsupported
        }

        async fn init(
            &mut self,
            _version: u32,
            _extensions: std::collections::HashMap<String, String>,
        ) -> Result<russh_sftp::protocol::Version, Self::Error> {
            let mut version = russh_sftp::protocol::Version::new();
            version.extensions.insert(FSYNC.to_owned(), "1".to_owned());
            version
                .extensions
                .insert(HARDLINK.to_owned(), "1".to_owned());
            version
                .extensions
                .insert(POSIX_RENAME.to_owned(), "1".to_owned());
            Ok(version)
        }

        async fn open(
            &mut self,
            id: u32,
            filename: String,
            pflags: OpenFlags,
            _attrs: FileAttributes,
        ) -> Result<russh_sftp::protocol::Handle, Self::Error> {
            let path = self.resolve(&filename);
            let mut options = std::fs::OpenOptions::new();
            options.read(pflags.contains(OpenFlags::READ) || !pflags.contains(OpenFlags::WRITE));
            options.write(pflags.contains(OpenFlags::WRITE));
            options.create(pflags.contains(OpenFlags::CREATE));
            options.truncate(pflags.contains(OpenFlags::TRUNCATE));
            options.open(&path).map_err(|_| StatusCode::NoSuchFile)?;
            let handle = self.alloc(path);
            Ok(russh_sftp::protocol::Handle { id, handle })
        }

        async fn close(
            &mut self,
            id: u32,
            handle: String,
        ) -> Result<russh_sftp::protocol::Status, Self::Error> {
            self.files.remove(&handle);
            self.dirs.remove(&handle);
            Ok(Self::ok(id))
        }

        async fn read(
            &mut self,
            id: u32,
            handle: String,
            offset: u64,
            len: u32,
        ) -> Result<russh_sftp::protocol::Data, Self::Error> {
            use std::io::{Read, Seek, SeekFrom};
            let path = &self.files.get(&handle).ok_or(StatusCode::Failure)?.path;
            let mut file = std::fs::File::open(path).map_err(|_| StatusCode::Failure)?;
            file.seek(SeekFrom::Start(offset))
                .map_err(|_| StatusCode::Failure)?;
            let mut buf = vec![0; len as usize];
            let n = file.read(&mut buf).map_err(|_| StatusCode::Failure)?;
            buf.truncate(n);
            Ok(russh_sftp::protocol::Data { id, data: buf })
        }

        async fn write(
            &mut self,
            id: u32,
            handle: String,
            offset: u64,
            data: Vec<u8>,
        ) -> Result<russh_sftp::protocol::Status, Self::Error> {
            use std::io::{Seek, SeekFrom, Write};
            let current = self.inflight.fetch_add(1, Ordering::SeqCst) + 1;
            self.peak.fetch_max(current, Ordering::SeqCst);
            let result = (|| {
                let path = &self.files.get(&handle).ok_or(StatusCode::Failure)?.path;
                let mut file = std::fs::OpenOptions::new()
                    .write(true)
                    .open(path)
                    .map_err(|_| StatusCode::Failure)?;
                file.seek(SeekFrom::Start(offset))
                    .map_err(|_| StatusCode::Failure)?;
                file.write_all(&data).map_err(|_| StatusCode::Failure)?;
                Ok(Self::ok(id))
            })();
            self.inflight.fetch_sub(1, Ordering::SeqCst);
            result
        }

        async fn lstat(
            &mut self,
            id: u32,
            path: String,
        ) -> Result<russh_sftp::protocol::Attrs, Self::Error> {
            Ok(russh_sftp::protocol::Attrs {
                id,
                attrs: Self::attrs(&self.resolve(&path))?,
            })
        }

        async fn fstat(
            &mut self,
            id: u32,
            handle: String,
        ) -> Result<russh_sftp::protocol::Attrs, Self::Error> {
            let path = &self.files.get(&handle).ok_or(StatusCode::Failure)?.path;
            Ok(russh_sftp::protocol::Attrs {
                id,
                attrs: Self::attrs(path)?,
            })
        }

        async fn opendir(
            &mut self,
            id: u32,
            path: String,
        ) -> Result<russh_sftp::protocol::Handle, Self::Error> {
            let resolved = self.resolve(&path);
            let entries = std::fs::read_dir(&resolved)
                .map_err(|_| StatusCode::NoSuchFile)?
                .collect::<Result<Vec<_>, _>>()
                .map_err(|_| StatusCode::Failure)?;
            let handle = format!("d{}", self.next);
            self.next += 1;
            self.dirs.insert(handle.clone(), entries.into_iter());
            Ok(russh_sftp::protocol::Handle { id, handle })
        }

        async fn readdir(
            &mut self,
            id: u32,
            handle: String,
        ) -> Result<russh_sftp::protocol::Name, Self::Error> {
            let entries = self.dirs.get_mut(&handle).ok_or(StatusCode::Failure)?;
            let batch: Vec<_> = entries.by_ref().take(64).collect();
            if batch.is_empty() {
                return Err(StatusCode::Eof);
            }
            let mut files = Vec::new();
            for entry in batch {
                let attrs = Self::attrs(&entry.path()).unwrap_or_default();
                files.push(russh_sftp::protocol::File::new(
                    entry.file_name().to_string_lossy().into_owned(),
                    attrs,
                ));
            }
            Ok(russh_sftp::protocol::Name { id, files })
        }

        async fn remove(
            &mut self,
            id: u32,
            filename: String,
        ) -> Result<russh_sftp::protocol::Status, Self::Error> {
            std::fs::remove_file(self.resolve(&filename)).map_err(|_| StatusCode::NoSuchFile)?;
            Ok(Self::ok(id))
        }

        async fn mkdir(
            &mut self,
            id: u32,
            path: String,
            _attrs: FileAttributes,
        ) -> Result<russh_sftp::protocol::Status, Self::Error> {
            match std::fs::create_dir(self.resolve(&path)) {
                Ok(()) => Ok(Self::ok(id)),
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => Ok(Self::ok(id)),
                Err(_) => Err(StatusCode::Failure),
            }
        }

        async fn extended(
            &mut self,
            id: u32,
            request: String,
            data: Vec<u8>,
        ) -> Result<russh_sftp::protocol::Packet, Self::Error> {
            let mut bytes = Bytes::from(data);
            match request.as_str() {
                FSYNC => {
                    let _ext: russh_sftp::extensions::FsyncExtension =
                        russh_sftp::de::from_bytes(&mut bytes)
                            .map_err(|_| StatusCode::BadMessage)?;
                    Ok(russh_sftp::protocol::Packet::Status(Self::ok(id)))
                }
                HARDLINK => {
                    let ext: russh_sftp::extensions::HardlinkExtension =
                        russh_sftp::de::from_bytes(&mut bytes)
                            .map_err(|_| StatusCode::BadMessage)?;
                    std::fs::hard_link(self.resolve(&ext.oldpath), self.resolve(&ext.newpath))
                        .map_err(|_| StatusCode::Failure)?;
                    Ok(russh_sftp::protocol::Packet::Status(Self::ok(id)))
                }
                POSIX_RENAME => {
                    let ext: russh_sftp::extensions::HardlinkExtension =
                        russh_sftp::de::from_bytes(&mut bytes)
                            .map_err(|_| StatusCode::BadMessage)?;
                    std::fs::rename(self.resolve(&ext.oldpath), self.resolve(&ext.newpath))
                        .map_err(|_| StatusCode::Failure)?;
                    Ok(russh_sftp::protocol::Packet::Status(Self::ok(id)))
                }
                _ => Err(StatusCode::OpUnsupported),
            }
        }
    }
}
