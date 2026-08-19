use crate::sftp_object_store::{OBJECT_HEADER_LEN, SftpCapabilities, decode_header};
use crate::sftp_transport::{
    RemoteDirectoryEntry, RemoteEntryKind, RemoteObjectRead, SessionFactory, TransportError,
    TransportSession,
};
use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use futures::{StreamExt, TryStreamExt};
use russh::client;
use russh::keys::{Algorithm, EcdsaCurve, HashAlg, PrivateKeyWithHashAlg, load_secret_key};
use russh_sftp::client::rawsession::{Limits as SftpLimits, RawSftpSession};
use russh_sftp::extensions::HardlinkExtension;
use russh_sftp::protocol::{FileAttributes, OpenFlags, StatusCode};
use std::borrow::Cow;
use std::fmt;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::Ordering;
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
/// 256 KiB SSH packets.
pub const RUSSH_MAXIMUM_PACKET_SIZE: u32 = 256 * 1024;
/// russh-sftp 2.4 packet cap. Must match the SSH maximum packet size.
pub const RUSSH_SFTP_MAX_PACKET_LEN: u32 = 256 * 1024;
/// OpenSSH sftp's proven in-flight WRITE window. The crate default of 8 is leftover.
pub const RUSSH_SFTP_MAX_CONCURRENT_WRITES: usize = 64;

const SFTP_WRITE_PACKET_SIZE: usize = 255 * 1024;
const SFTP_READ_PACKET_SIZE: usize = 255 * 1024;
const SFTP_WRITE_REQUEST_CONCURRENCY: usize = RUSSH_SFTP_MAX_CONCURRENT_WRITES;
const SFTP_READ_REQUEST_CONCURRENCY: usize = RUSSH_SFTP_MAX_CONCURRENT_WRITES;
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
    packet_size: usize,
) -> Result<Vec<PipelinedWrite>, TransportError> {
    if packet_size == 0 {
        return Err(TransportError::Operation(
            "SFTP write packet size is zero".to_owned(),
        ));
    }
    let mut offset = initial_offset;
    let mut requests = Vec::new();
    for mut chunk in chunks {
        while !chunk.is_empty() {
            let len = chunk.len().min(packet_size);
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
    packet_size: usize,
) -> Result<Vec<PipelinedRead>, TransportError> {
    if packet_size == 0 {
        return Err(TransportError::Operation(
            "SFTP read packet size is zero".to_owned(),
        ));
    }
    let mut offset = initial_offset;
    let mut remaining = len;
    let mut requests = Vec::with_capacity(len.div_ceil(packet_size));
    while remaining != 0 {
        let request_len = remaining.min(packet_size);
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

fn transfer_request_len(
    preferred: usize,
    operation_limit: Option<u64>,
    packet_limit: Option<u64>,
    handle_len: usize,
) -> Result<usize, TransportError> {
    // Reserve 64 bytes of SFTP request framing besides the handle.
    let framing = handle_len
        .checked_add(64)
        .ok_or_else(|| TransportError::Operation("SFTP handle length overflow".to_owned()))?;
    let packet_payload = packet_limit.map(|limit| limit.saturating_sub(framing as u64));
    let limit = [Some(preferred as u64), operation_limit, packet_payload]
        .into_iter()
        .flatten()
        .min()
        .unwrap_or(preferred as u64);
    let limit = usize::try_from(limit).unwrap_or(usize::MAX);
    if limit == 0 {
        return Err(TransportError::Operation(
            "SFTP server negotiated a zero-byte transfer limit".to_owned(),
        ));
    }
    Ok(limit)
}

pub fn russh_client_config() -> client::Config {
    client::Config {
        window_size: RUSSH_WINDOW_SIZE,
        maximum_packet_size: RUSSH_MAXIMUM_PACKET_SIZE,
        channel_buffer_size: RUSSH_CHANNEL_BUFFER_SIZE,
        preferred: russh::Preferred {
            key: Cow::Borrowed(&[
                Algorithm::Ed25519,
                Algorithm::Ecdsa {
                    curve: EcdsaCurve::NistP256,
                },
                Algorithm::Ecdsa {
                    curve: EcdsaCurve::NistP384,
                },
                Algorithm::Ecdsa {
                    curve: EcdsaCurve::NistP521,
                },
                Algorithm::Rsa {
                    hash: Some(HashAlg::Sha512),
                },
                Algorithm::Rsa {
                    hash: Some(HashAlg::Sha256),
                },
            ]),
            cipher: Cow::Borrowed(&[
                russh::cipher::AES_256_GCM,
                russh::cipher::AES_128_GCM,
                russh::cipher::AES_256_CTR,
                russh::cipher::AES_192_CTR,
                russh::cipher::AES_128_CTR,
            ]),
            ..russh::Preferred::DEFAULT
        },
        inactivity_timeout: None, // pool owns idle lifetime
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
    identity_key: Arc<russh::keys::PrivateKey>,
    known_hosts: PathBuf,
}

/// Historical test-only name retained while existing integration tests move to russh.
#[cfg(test)]
#[allow(dead_code)]
pub type OpenSshSessionFactory = RusshSessionFactory;

impl RusshSessionFactory {
    pub fn new(
        endpoint: crate::config::SftpEndpoint,
        identity_file: PathBuf,
        known_hosts: PathBuf,
    ) -> Result<Self, TransportError> {
        let identity_metadata = fs::metadata(&identity_file).map_err(|error| {
            TransportError::Open(format!(
                "[sftp] identity_file {} is unavailable: {error}",
                identity_file.display()
            ))
        })?;
        if !identity_metadata.is_file() {
            return Err(TransportError::Open(format!(
                "[sftp] identity_file {} is not a regular file",
                identity_file.display()
            )));
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;

            let mode = identity_metadata.permissions().mode();
            if mode & 0o077 != 0 {
                return Err(TransportError::Open(format!(
                    "[sftp] identity_file {} permissions are too open: {:04o}; expected 0600 or stricter",
                    identity_file.display(),
                    mode & 0o7777
                )));
            }
        }
        let identity_key = load_secret_key(&identity_file, None).map_err(|error| {
            TransportError::Open(format!(
                "[sftp] identity_file {} is not an unencrypted OpenSSH private key: {error}",
                identity_file.display()
            ))
        })?;
        let known_hosts_metadata = fs::metadata(&known_hosts).map_err(|error| {
            TransportError::Open(format!(
                "[sftp] known_hosts {} is unavailable: {error}",
                known_hosts.display()
            ))
        })?;
        if !known_hosts_metadata.is_file() {
            return Err(TransportError::Open(format!(
                "[sftp] known_hosts {} is not a regular file",
                known_hosts.display()
            )));
        }
        Ok(Self {
            endpoint,
            identity_file,
            identity_key: Arc::new(identity_key),
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

async fn authenticate_identity(
    handle: &mut client::Handle<StrictHostKey>,
    username: &str,
    identity_file: &Path,
    identity_key: Arc<russh::keys::PrivateKey>,
    hash: Option<HashAlg>,
) -> Result<(), TransportError> {
    let authenticated = handle
        .authenticate_publickey(username, PrivateKeyWithHashAlg::new(identity_key, hash))
        .await
        .map_err(|error| {
            TransportError::Open(format!("public-key authentication failed: {error}"))
        })?;
    if authenticated.success() {
        Ok(())
    } else {
        Err(TransportError::Open(format!(
            "public-key authentication rejected for {}",
            identity_file.display()
        )))
    }
}

async fn handshake_sftp<S>(
    stream: S,
) -> Result<(RawSftpSession, SftpCapabilities, SftpLimits), TransportError>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let mut raw = RawSftpSession::new_with_config(stream, russh_sftp_config());
    let version = raw
        .init()
        .await
        .map_err(|error| TransportError::Open(format!("SFTP handshake failed: {error}")))?;
    let limits = if has_extension(&version, russh_sftp::extensions::LIMITS) {
        match raw.limits().await {
            Ok(limits) => SftpLimits::from(limits),
            Err(error) => {
                tracing::warn!(%error, "SFTP server advertised limits but the query failed");
                SftpLimits::default()
            }
        }
    } else {
        SftpLimits::default()
    };
    raw.set_limits(limits);
    Ok((
        raw,
        SftpCapabilities {
            fsync: has_extension(&version, FSYNC),
            hardlink: has_extension(&version, HARDLINK),
            posix_rename: has_extension(&version, POSIX_RENAME),
        },
        limits,
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
            let hash = if self.identity_key.algorithm().is_rsa() {
                handle
                    .best_supported_rsa_hash()
                    .await
                    .map_err(|error| {
                        TransportError::Open(format!("RSA hash probe failed: {error}"))
                    })?
                    .flatten()
            } else {
                None
            };
            authenticate_identity(
                &mut handle,
                self.endpoint.username.as_str(),
                &self.identity_file,
                self.identity_key.clone(),
                hash,
            )
            .await?;

            let channel = handle.channel_open_session().await.map_err(|error| {
                TransportError::Open(format!("failed to open SSH session channel: {error}"))
            })?;
            channel
                .request_subsystem(true, "sftp")
                .await
                .map_err(|error| {
                    TransportError::Open(format!("failed to start SFTP subsystem: {error}"))
                })?;
            let (sftp, capabilities, limits) = handshake_sftp(channel.into_stream()).await?;
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
                limits,
                force: force.clone(),
            }) as Box<dyn TransportSession>)
        };

        tokio::select! {
            result = connect => result,
            _ = force.cancelled() => Err(TransportError::PoolClosed),
        }
    }
}

pub struct RusshTransportSession {
    handle: Option<client::Handle<StrictHostKey>>,
    sftp: Option<RawSftpSession>,
    capabilities: SftpCapabilities,
    limits: SftpLimits,
    force: CancellationToken,
}

/// Historical test-only name retained while existing integration tests move to russh-sftp.
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
        let (sftp, capabilities, limits) = handshake_sftp(Duplex {
            reader: stdout,
            writer: stdin,
        })
        .await?;
        Ok(Self {
            handle: None,
            sftp: Some(sftp),
            capabilities,
            limits,
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
        let result = write_handle_pipelined(sftp, &handle, path, offset, chunks, self.limits).await;
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
    limits: SftpLimits,
) -> Result<(), TransportError> {
    let packet_size = transfer_request_len(
        SFTP_WRITE_PACKET_SIZE,
        limits.write_len,
        limits.packet_len,
        handle.len(),
    )?;
    let requests = plan_pipelined_writes(offset, chunks, packet_size)?;
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
    limits: SftpLimits,
) -> Result<Bytes, TransportError> {
    let packet_size = transfer_request_len(
        SFTP_READ_PACKET_SIZE,
        limits.read_len,
        limits.packet_len,
        handle.len(),
    )?;
    let requests = plan_pipelined_reads(offset, len, packet_size)?;
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
                let mut received = 0;
                while received < request.len {
                    let remaining = request.len - received;
                    let offset = request.offset.checked_add(received as u64).ok_or_else(|| {
                        TransportError::Operation("SFTP read offset overflow".to_owned())
                    })?;
                    let data = sftp
                        .read(handle.clone(), offset, remaining as u32)
                        .await
                        .map_err(|error| {
                            TransportError::Operation(format!(
                                "short read from {} at {}: {error}",
                                path.display(),
                                offset
                            ))
                        })?;
                    let len = data.data.len();
                    if len == 0 || len > remaining {
                        return Err(TransportError::Operation(format!(
                            "short read from {} at {}: got {len} bytes with {remaining} remaining",
                            path.display(),
                            offset
                        )));
                    }
                    region[received..received + len].copy_from_slice(&data.data);
                    received += len;
                }
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
        let result = async {
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
                Some((_, physical_start, len)) => Some(
                    read_handle_pipelined(sftp, &handle, path, *physical_start, *len, self.limits)
                        .await,
                ),
                None => None,
            }
            };
            let (metadata, encoded_header, speculative_payload) = tokio::join!(
            sftp.fstat(handle.as_str()),
            read_handle_pipelined(sftp, &handle, path, 0, OBJECT_HEADER_LEN, self.limits),
            speculative_payload,
            );
            let metadata = metadata.map_err(|error| map_sftp_error(path, error))?.attrs;
            if !metadata.is_regular() {
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
                result?
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
                read_handle_pipelined(sftp, &handle, path, physical_start, len, self.limits).await?
            };
            Ok(RemoteObjectRead {
                header,
                modified,
                range,
                payload,
            })
        }
        .await;
        let close = sftp
            .close(handle)
            .await
            .map_err(|error| map_sftp_error(path, error));
        match (result, close) {
            (_, Err(close_error)) => Err(close_error),
            (result, Ok(_)) => result,
        }
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
        let bytes = read_handle_pipelined(sftp, &handle, path, offset, len, self.limits).await;
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
        if let Some(mut handle) = self.handle.take() {
            let disconnect = handle.disconnect(russh::Disconnect::ByApplication, "", "");
            let queued = tokio::select! {
                result = disconnect => {
                    if let Err(error) = result {
                        tracing::warn!(%error, "russh disconnect failed after SFTP close");
                    }
                    true
                }
                _ = force.cancelled() => false,
                _ = self.force.cancelled() => false,
            };
            if queued {
                tokio::select! {
                    result = &mut handle => {
                        if let Err(error) = result {
                            tracing::warn!(%error, "russh connection task failed during close");
                        }
                    }
                    _ = force.cancelled() => {}
                    _ = self.force.cancelled() => {}
                }
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
    const ENCRYPTED_KEY: &str = r#"-----BEGIN OPENSSH PRIVATE KEY-----
b3BlbnNzaC1rZXktdjEAAAAACmFlczI1Ni1jdHIAAAAGYmNyeXB0AAAAGAAAABBA6hrXpS
LPoPaV7M7G9DtaAAAAGAAAAAEAAAAzAAAAC3NzaC1lZDI1NTE5AAAAIC3aUS9vD1G77q+1
0WpfmDRbafRtG5Ry+YTGXiQynPOsAAAAoG12ZqxPoDsGoR9DwynbDlcIfoCqrOE13219I1
bcxvi0nQxL+AqlsH6Ws1XeGzQJI43NujzRhKle1UUAIW5yIX9Hu5Nzjmc/L58QMyNVejCl
qCqH6hiJAS8PQ2MVjb9TYJ9ujM5FIr0Goy2X3x+HX0oExG56xthfBD0pkUwXWdiD451TLR
SiHvLIjvZnsP6UHEZvepD9dSLx72qVi3Qb2/E=
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
        assert!(
            !config.preferred.key.iter().any(|algorithm| matches!(
                algorithm,
                russh::keys::Algorithm::Dsa | russh::keys::Algorithm::Rsa { hash: None }
            )),
            "host-key negotiation must exclude DSA and SHA-1 ssh-rsa: {:?}",
            config.preferred.key
        );
    }

    #[test]
    fn russh_sftp_config_pipelines_64_by_256kib() {
        let config = russh_sftp_config();
        assert_eq!(config.max_packet_len, 256 * 1024);
        assert_eq!(config.max_concurrent_writes, 64);
        assert_ne!(
            config.max_concurrent_writes,
            russh_sftp::client::Config::default().max_concurrent_writes
        );
    }

    #[test]
    fn pipelined_write_plan_uses_255kib_packets_not_ack_per_byte() {
        let payload = Bytes::from(vec![0u8; SFTP_WRITE_PACKET_SIZE * 64 + 17]);
        let plan = plan_pipelined_writes(0, vec![payload], SFTP_WRITE_PACKET_SIZE).unwrap();
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
        let plan =
            plan_pipelined_reads(0, SFTP_READ_PACKET_SIZE * 3, SFTP_READ_PACKET_SIZE).unwrap();
        assert_eq!(plan.len(), 3);
        assert_eq!(plan[2].offset, (SFTP_READ_PACKET_SIZE * 2) as u64);
        assert_eq!(SFTP_READ_REQUEST_CONCURRENCY, 64);
    }

    #[test]
    fn negotiated_transfer_lengths_obey_every_server_limit() {
        let limits = russh_sftp::client::rawsession::Limits {
            packet_len: Some(64 * 1024),
            read_len: Some(48 * 1024),
            write_len: Some(32 * 1024),
            open_handles: None,
        };
        let handle_len = 40;

        let read_len = transfer_request_len(
            SFTP_READ_PACKET_SIZE,
            limits.read_len,
            limits.packet_len,
            handle_len,
        )
        .unwrap();
        let write_len = transfer_request_len(
            SFTP_WRITE_PACKET_SIZE,
            limits.write_len,
            limits.packet_len,
            handle_len,
        )
        .unwrap();

        assert!(read_len <= 48 * 1024);
        assert!(write_len <= 32 * 1024);
        assert!(read_len + handle_len + 64 <= 64 * 1024);
        assert!(write_len + handle_len + 64 <= 64 * 1024);
    }

    #[test]
    fn factory_debug_redacts_the_username() {
        let root = tempfile::tempdir().unwrap();
        let identity = root.path().join("identity");
        let known_hosts = root.path().join("known_hosts");
        std::fs::write(&identity, CLIENT_KEY).unwrap();
        std::fs::write(&known_hosts, "").unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&identity, std::fs::Permissions::from_mode(0o600)).unwrap();
        }
        let factory = RusshSessionFactory::new(
            crate::config::SftpEndpoint {
                host: "storage.example.test".to_owned(),
                port: 2222,
                username: "account-secret-name".to_owned(),
            },
            identity,
            known_hosts,
        )
        .unwrap();
        let debug = format!("{factory:?}");
        assert!(debug.contains("storage.example.test"));
        assert!(debug.contains("2222"));
        assert!(debug.contains("16777216"));
        assert!(!debug.contains("account-secret-name"));
    }

    #[test]
    fn factory_rejects_a_missing_configured_identity_before_network_dial() {
        let root = tempfile::tempdir().unwrap();
        let known_hosts = root.path().join("known_hosts");
        std::fs::write(&known_hosts, "").unwrap();
        let error = RusshSessionFactory::new(
            crate::config::SftpEndpoint {
                host: "127.0.0.1".to_owned(),
                port: 1,
                username: "zerofs".to_owned(),
            },
            root.path().join("missing-identity"),
            known_hosts,
        )
        .expect_err("a missing configured identity must fail before network startup");

        assert!(error.to_string().contains("identity_file"));
    }

    #[test]
    fn factory_rejects_missing_known_hosts_before_network_dial() {
        let root = tempfile::tempdir().unwrap();
        let identity = root.path().join("identity");
        std::fs::write(&identity, CLIENT_KEY).unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&identity, std::fs::Permissions::from_mode(0o600)).unwrap();
        }
        let error = RusshSessionFactory::new(
            crate::config::SftpEndpoint {
                host: "127.0.0.1".to_owned(),
                port: 1,
                username: "zerofs".to_owned(),
            },
            identity,
            root.path().join("missing-known-hosts"),
        )
        .expect_err("missing known_hosts must fail before network startup");

        assert!(error.to_string().contains("known_hosts"));
    }

    #[cfg(unix)]
    #[test]
    fn factory_rejects_a_group_readable_private_key() {
        use std::os::unix::fs::PermissionsExt;

        let root = tempfile::tempdir().unwrap();
        let identity = root.path().join("identity");
        let known_hosts = root.path().join("known_hosts");
        std::fs::write(&identity, CLIENT_KEY).unwrap();
        std::fs::write(&known_hosts, "").unwrap();
        std::fs::set_permissions(&identity, std::fs::Permissions::from_mode(0o640)).unwrap();
        let error = RusshSessionFactory::new(
            crate::config::SftpEndpoint {
                host: "127.0.0.1".to_owned(),
                port: 1,
                username: "zerofs".to_owned(),
            },
            identity,
            known_hosts,
        )
        .expect_err("group-readable private keys must fail closed");

        assert!(error.to_string().contains("permissions"));
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
    async fn russh_factory_rejects_an_unconfigured_client_key() {
        let env = Loopback::start().await;
        std::fs::write(&env.identity, OTHER_KEY.as_bytes()).unwrap();
        let factory = RusshSessionFactory::new(
            env.endpoint.clone(),
            env.identity.clone(),
            env.known_hosts.clone(),
        )
        .unwrap();

        let error = factory
            .open(CancellationToken::new())
            .await
            .expect_err("the server must reject a client key it did not configure");
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
        assert_eq!(
            env.exec_requests.load(Ordering::SeqCst),
            0,
            "production writes must remain entirely within the SFTP namespace"
        );
        let elapsed = started.elapsed();
        let peak = CLIENT_WRITE_PEAK.load(Ordering::SeqCst);
        assert!(
            peak >= 16,
            "client must pipeline WRITE requests; peak in-flight={peak} elapsed={elapsed:?}"
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
    }

    #[tokio::test]
    async fn russh_loopback_reassembles_short_sftp_read_replies() {
        let env = Loopback::start_with_read_cap(Some(32 * 1024)).await;
        let factory = RusshSessionFactory::new(
            env.endpoint.clone(),
            env.identity.clone(),
            env.known_hosts.clone(),
        )
        .unwrap();
        let mut session = factory.open(CancellationToken::new()).await.unwrap();
        let payload = Bytes::from(vec![0xa5; SFTP_READ_PACKET_SIZE + 17]);

        session
            .write_file_durable(
                std::path::Path::new("short-read.bin"),
                vec![payload.clone()],
            )
            .await
            .unwrap();
        let read = session
            .read_exact(std::path::Path::new("short-read.bin"), 0, payload.len())
            .await
            .unwrap();

        assert_eq!(read, payload);
        session.close(CancellationToken::new()).await.unwrap();
    }

    #[tokio::test]
    async fn russh_close_waits_for_the_ssh_connection_to_terminate() {
        let env = Loopback::start().await;
        let factory = RusshSessionFactory::new(
            env.endpoint.clone(),
            env.identity.clone(),
            env.known_hosts.clone(),
        )
        .unwrap();
        let session = factory.open(CancellationToken::new()).await.unwrap();
        assert_eq!(env.active_connections.load(Ordering::SeqCst), 1);

        session.close(CancellationToken::new()).await.unwrap();

        tokio::time::timeout(Duration::from_secs(1), async {
            while env.active_connections.load(Ordering::SeqCst) != 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the remote SSH handler must terminate after close returns");
    }

    #[tokio::test]
    async fn russh_factory_rejects_an_encrypted_identity_without_a_passphrase() {
        let env = Loopback::start().await;
        std::fs::write(&env.identity, ENCRYPTED_KEY.as_bytes()).unwrap();
        let error = RusshSessionFactory::new(
            env.endpoint.clone(),
            env.identity.clone(),
            env.known_hosts.clone(),
        )
        .expect_err("encrypted identities must fail before network startup");
        match error {
            TransportError::Open(message) => {
                assert!(
                    message.contains("identity_file"),
                    "failure must name the invalid configured identity: {message}"
                );
            }
            other => panic!("expected Open error, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn russh_read_object_closes_handle_after_metadata_error() {
        let env = Loopback::start_without_mtime().await;
        let factory = RusshSessionFactory::new(
            env.endpoint.clone(),
            env.identity.clone(),
            env.known_hosts.clone(),
        )
        .unwrap();
        let mut session = factory.open(CancellationToken::new()).await.unwrap();
        session
            .write_file_durable(
                Path::new("missing-mtime.bin"),
                vec![Bytes::from_static(b"not-an-object")],
            )
            .await
            .unwrap();
        assert_eq!(env.open_handles.load(Ordering::SeqCst), 0);

        let error = session
            .read_object(Path::new("missing-mtime.bin"), None, false)
            .await
            .expect_err("missing object metadata must fail closed");

        assert!(matches!(error, TransportError::CorruptObject(_)));
        assert_eq!(
            env.open_handles.load(Ordering::SeqCst),
            0,
            "every read_object error path must close its raw SFTP handle"
        );
        session.close(CancellationToken::new()).await.unwrap();
    }

    struct Loopback {
        _root: tempfile::TempDir,
        endpoint: crate::config::SftpEndpoint,
        identity: PathBuf,
        known_hosts: PathBuf,
        _inflight: std::sync::Arc<std::sync::atomic::AtomicUsize>,
        _peak: std::sync::Arc<std::sync::atomic::AtomicUsize>,
        exec_requests: std::sync::Arc<std::sync::atomic::AtomicUsize>,
        active_connections: std::sync::Arc<std::sync::atomic::AtomicUsize>,
        open_handles: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    }

    impl Loopback {
        async fn start() -> Self {
            Self::start_with(None, false).await
        }

        async fn start_with_read_cap(read_cap: Option<usize>) -> Self {
            Self::start_with(read_cap, false).await
        }

        async fn start_without_mtime() -> Self {
            Self::start_with(None, true).await
        }

        async fn start_with(read_cap: Option<usize>, omit_mtime: bool) -> Self {
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
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                std::fs::set_permissions(&identity, std::fs::Permissions::from_mode(0o600))
                    .unwrap();
            }
            let client_public = client_key.public_key().clone();

            let server_key = russh::keys::PrivateKey::from_openssh(SERVER_KEY).unwrap();
            let server_public = server_key.public_key().clone();

            let inflight = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
            let peak = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
            let exec_requests = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
            let active_connections = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
            let open_handles = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));

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
                read_cap: Option<usize>,
                omit_mtime: bool,
                inflight: std::sync::Arc<std::sync::atomic::AtomicUsize>,
                peak: std::sync::Arc<std::sync::atomic::AtomicUsize>,
                exec_requests: std::sync::Arc<std::sync::atomic::AtomicUsize>,
                active_connections: std::sync::Arc<std::sync::atomic::AtomicUsize>,
                open_handles: std::sync::Arc<std::sync::atomic::AtomicUsize>,
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
                    self.active_connections.fetch_add(1, Ordering::SeqCst);
                    SshSession {
                        state: self.clone(),
                        channels: std::sync::Arc::new(tokio::sync::Mutex::new(
                            std::collections::HashMap::new(),
                        )),
                    }
                }
            }

            impl Drop for SshSession {
                fn drop(&mut self) {
                    self.state.active_connections.fetch_sub(1, Ordering::SeqCst);
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
                        read_cap: self.state.read_cap,
                        omit_mtime: self.state.omit_mtime,
                        files: std::collections::HashMap::new(),
                        dirs: std::collections::HashMap::new(),
                        next: 1,
                        inflight: self.state.inflight.clone(),
                        peak: self.state.peak.clone(),
                        open_handles: self.state.open_handles.clone(),
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
                    self.state.exec_requests.fetch_add(1, Ordering::SeqCst);
                    session.channel_failure(channel_id)?;
                    Ok(())
                }
            }

            let mut server = ServerState {
                fs_root: fs_root.clone(),
                client_public,
                read_cap,
                omit_mtime,
                inflight: inflight.clone(),
                peak: peak.clone(),
                exec_requests: exec_requests.clone(),
                active_connections: active_connections.clone(),
                open_handles: open_handles.clone(),
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
                exec_requests,
                active_connections,
                open_handles,
            }
        }
    }

    struct Opened {
        path: PathBuf,
    }

    struct FsSftp {
        root: PathBuf,
        read_cap: Option<usize>,
        omit_mtime: bool,
        files: std::collections::HashMap<String, Opened>,
        dirs: std::collections::HashMap<String, std::vec::IntoIter<std::fs::DirEntry>>,
        next: u64,
        inflight: std::sync::Arc<std::sync::atomic::AtomicUsize>,
        peak: std::sync::Arc<std::sync::atomic::AtomicUsize>,
        open_handles: std::sync::Arc<std::sync::atomic::AtomicUsize>,
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
            &self,
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
            if !self.omit_mtime
                && let Ok(modified) = meta.modified()
            {
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
            self.open_handles.fetch_add(1, Ordering::SeqCst);
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
            if self.files.remove(&handle).is_some() {
                self.open_handles.fetch_sub(1, Ordering::SeqCst);
            }
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
            let requested = usize::try_from(len).map_err(|_| StatusCode::Failure)?;
            let mut buf = vec![0; self.read_cap.map_or(requested, |cap| requested.min(cap))];
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
                attrs: self.attrs(&self.resolve(&path))?,
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
                attrs: self.attrs(path)?,
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
                let attrs = self.attrs(&entry.path()).unwrap_or_default();
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
