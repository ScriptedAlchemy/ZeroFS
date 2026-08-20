use crate::sftp_object_store::{OBJECT_HEADER_LEN, SftpCapabilities, decode_header};
use crate::sftp_transport::{
    RemoteDirectoryEntry, RemoteEntryKind, RemoteObjectRead, TransportError, TransportSession,
};
use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use futures::{StreamExt, TryStreamExt};
use russh_sftp::client::rawsession::{Limits as SftpLimits, RawSftpSession};
use russh_sftp::extensions::HardlinkExtension;
use russh_sftp::protocol::{FileAttributes, OpenFlags, StatusCode};
use std::fmt;
use std::path::{Path, PathBuf};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_util::sync::CancellationToken;

#[cfg(test)]
use std::sync::atomic::{AtomicU64, Ordering};

#[cfg(test)]
#[derive(Debug, Clone, Copy)]
pub(crate) struct SftpBenchTiming {
    pub open_nanos: u64,
    /// Serialized wall time of the whole write reply window. Since writes,
    /// fsync, and close are issued together and their replies awaited in one
    /// window, this single counter carries the pipelined window's cost;
    /// `fsync_nanos`/`close_nanos` then record only serialized time spent
    /// beyond it (the large-write tail window), not per-request latency.
    pub write_nanos: u64,
    pub fsync_nanos: u64,
    pub close_nanos: u64,
    pub hardlink_nanos: u64,
    pub remove_nanos: u64,
    pub publications: u64,
    pub session_publications: [u64; 32],
    pub session_write_bytes: [u64; 32],
}

#[cfg(test)]
static BENCH_OPEN_NANOS: AtomicU64 = AtomicU64::new(0);
#[cfg(test)]
static BENCH_WRITE_NANOS: AtomicU64 = AtomicU64::new(0);
#[cfg(test)]
static BENCH_FSYNC_NANOS: AtomicU64 = AtomicU64::new(0);
#[cfg(test)]
static BENCH_CLOSE_NANOS: AtomicU64 = AtomicU64::new(0);
#[cfg(test)]
static BENCH_HARDLINK_NANOS: AtomicU64 = AtomicU64::new(0);
#[cfg(test)]
static BENCH_REMOVE_NANOS: AtomicU64 = AtomicU64::new(0);
#[cfg(test)]
static BENCH_PUBLICATIONS: AtomicU64 = AtomicU64::new(0);
#[cfg(test)]
static BENCH_SESSION_PUBLICATIONS: [AtomicU64; 32] = [const { AtomicU64::new(0) }; 32];
#[cfg(test)]
static BENCH_SESSION_WRITE_BYTES: [AtomicU64; 32] = [const { AtomicU64::new(0) }; 32];
#[cfg(test)]
static NEXT_BENCH_SESSION_SLOT: AtomicU64 = AtomicU64::new(0);

#[cfg(test)]
fn bench_nanos(elapsed: std::time::Duration) -> u64 {
    u64::try_from(elapsed.as_nanos()).unwrap_or(u64::MAX)
}

#[cfg(test)]
pub(crate) fn reset_bench_timing() {
    for counter in [
        &BENCH_OPEN_NANOS,
        &BENCH_WRITE_NANOS,
        &BENCH_FSYNC_NANOS,
        &BENCH_CLOSE_NANOS,
        &BENCH_HARDLINK_NANOS,
        &BENCH_REMOVE_NANOS,
        &BENCH_PUBLICATIONS,
    ] {
        counter.store(0, Ordering::SeqCst);
    }
    for counter in BENCH_SESSION_PUBLICATIONS
        .iter()
        .chain(BENCH_SESSION_WRITE_BYTES.iter())
    {
        counter.store(0, Ordering::SeqCst);
    }
}

#[cfg(test)]
pub(crate) fn bench_timing() -> SftpBenchTiming {
    SftpBenchTiming {
        open_nanos: BENCH_OPEN_NANOS.load(Ordering::SeqCst),
        write_nanos: BENCH_WRITE_NANOS.load(Ordering::SeqCst),
        fsync_nanos: BENCH_FSYNC_NANOS.load(Ordering::SeqCst),
        close_nanos: BENCH_CLOSE_NANOS.load(Ordering::SeqCst),
        hardlink_nanos: BENCH_HARDLINK_NANOS.load(Ordering::SeqCst),
        remove_nanos: BENCH_REMOVE_NANOS.load(Ordering::SeqCst),
        publications: BENCH_PUBLICATIONS.load(Ordering::SeqCst),
        session_publications: std::array::from_fn(|index| {
            BENCH_SESSION_PUBLICATIONS[index].load(Ordering::SeqCst)
        }),
        session_write_bytes: std::array::from_fn(|index| {
            BENCH_SESSION_WRITE_BYTES[index].load(Ordering::SeqCst)
        }),
    }
}

/// russh-sftp 2.4 frame cap. SFTP frames may span multiple SSH transport packets.
pub const RUSSH_SFTP_MAX_PACKET_LEN: u32 = 256 * 1024;
/// Match the raw OpenSSH SFTP control's proven `-R 128` in-flight WRITE window.
/// The crate default of 8 leaves WAN bandwidth idle.
pub const RUSSH_SFTP_MAX_CONCURRENT_WRITES: usize = 128;

pub(crate) const SFTP_WRITE_PACKET_SIZE: usize = 255 * 1024;
pub(crate) const SFTP_READ_PACKET_SIZE: usize = 255 * 1024;
const SFTP_WRITE_REQUEST_CONCURRENCY: usize = RUSSH_SFTP_MAX_CONCURRENT_WRITES;
// Read prefetch retains its separately proven 64-request memory bound.
const SFTP_READ_REQUEST_CONCURRENCY: usize = 64;
pub(crate) const POSIX_RENAME: &str = "posix-rename@openssh.com";
pub(crate) const FSYNC: &str = "fsync@openssh.com";
pub(crate) const HARDLINK: &str = "hardlink@openssh.com";

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

fn finish_raw_handle<T>(
    operation: Result<T, TransportError>,
    close: Result<(), TransportError>,
) -> Result<T, TransportError> {
    match close {
        Err(close_error) => Err(close_error),
        Ok(()) => operation,
    }
}

pub fn russh_sftp_config() -> russh_sftp::client::Config {
    russh_sftp::client::Config {
        max_packet_len: RUSSH_SFTP_MAX_PACKET_LEN,
        max_concurrent_writes: RUSSH_SFTP_MAX_CONCURRENT_WRITES,
        request_timeout_secs: 60,
    }
}

struct BoundedSftpStream<S> {
    inner: S,
    max_packet_len: u32,
    prefix: [u8; 4],
    prefix_len: usize,
    prefix_emitted: usize,
    payload_remaining: usize,
    failed: bool,
}

impl<S> BoundedSftpStream<S> {
    fn new(inner: S, max_packet_len: u32) -> Self {
        Self {
            inner,
            max_packet_len,
            prefix: [0; 4],
            prefix_len: 0,
            prefix_emitted: 0,
            payload_remaining: 0,
            failed: false,
        }
    }
}

impl<S: AsyncRead + Unpin> AsyncRead for BoundedSftpStream<S> {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        if buf.remaining() == 0 {
            return std::task::Poll::Ready(Ok(()));
        }
        if self.failed {
            return std::task::Poll::Ready(Ok(()));
        }

        if self.prefix_emitted == self.prefix.len() && self.payload_remaining == 0 {
            self.prefix_len = 0;
            self.prefix_emitted = 0;
        }

        while self.prefix_len < self.prefix.len() {
            let Self {
                inner,
                prefix,
                prefix_len,
                ..
            } = &mut *self;
            let mut prefix_buf = tokio::io::ReadBuf::new(&mut prefix[*prefix_len..]);
            match std::pin::Pin::new(inner).poll_read(cx, &mut prefix_buf) {
                std::task::Poll::Pending => return std::task::Poll::Pending,
                std::task::Poll::Ready(Err(error)) => return std::task::Poll::Ready(Err(error)),
                std::task::Poll::Ready(Ok(())) => {
                    let read = prefix_buf.filled().len();
                    if read == 0 {
                        return std::task::Poll::Ready(Ok(()));
                    }
                    *prefix_len += read;
                }
            }
        }

        if self.prefix_emitted == 0 {
            let packet_len = u32::from_be_bytes(self.prefix);
            if packet_len > self.max_packet_len {
                self.failed = true;
                return std::task::Poll::Ready(Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!(
                        "SFTP packet length {packet_len} exceeds limit {}",
                        self.max_packet_len
                    ),
                )));
            }
            self.payload_remaining = packet_len as usize;
        }

        if self.prefix_emitted < self.prefix.len() {
            let available = &self.prefix[self.prefix_emitted..];
            let emitted = available.len().min(buf.remaining());
            buf.put_slice(&available[..emitted]);
            self.prefix_emitted += emitted;
            return std::task::Poll::Ready(Ok(()));
        }

        let read_limit = self.payload_remaining.min(buf.remaining());
        let mut limited = buf.take(read_limit);
        match std::pin::Pin::new(&mut self.inner).poll_read(cx, &mut limited) {
            std::task::Poll::Pending => std::task::Poll::Pending,
            std::task::Poll::Ready(Err(error)) => std::task::Poll::Ready(Err(error)),
            std::task::Poll::Ready(Ok(())) => {
                let read = limited.filled().len();
                // `limited` borrows the unfilled portion of `buf`; a successful
                // AsyncRead initialized exactly the bytes it reports as filled.
                unsafe { buf.assume_init(read) };
                buf.advance(read);
                self.payload_remaining -= read;
                std::task::Poll::Ready(Ok(()))
            }
        }
    }
}

impl<S: AsyncWrite + Unpin> AsyncWrite for BoundedSftpStream<S> {
    fn poll_write(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        std::pin::Pin::new(&mut self.inner).poll_write(cx, buf)
    }

    fn poll_flush(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut self.inner).poll_flush(cx)
    }

    fn poll_shutdown(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut self.inner).poll_shutdown(cx)
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

fn map_sftp_close_error(path: &Path, error: russh_sftp::client::error::Error) -> TransportError {
    TransportError::Close(format!("{}: {error}", path.display()))
}

pub(crate) async fn handshake_sftp<S>(
    stream: S,
) -> Result<(RawSftpSession, SftpCapabilities, SftpLimits), TransportError>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let bounded = BoundedSftpStream::new(stream, RUSSH_SFTP_MAX_PACKET_LEN);
    let mut raw = RawSftpSession::new_with_config(bounded, russh_sftp_config());
    let version = raw
        .init()
        .await
        .map_err(|error| TransportError::Open(format!("SFTP handshake failed: {error}")))?;
    let limits = if has_extension(&version, russh_sftp::extensions::LIMITS) {
        SftpLimits::from(raw.limits().await.map_err(|error| {
            TransportError::Open(format!(
                "SFTP server advertised limits but the query failed: {error}"
            ))
        })?)
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
pub(crate) trait SshConnectionOwner: fmt::Debug + Send + Sync {
    async fn close(self: Box<Self>, force: CancellationToken) -> Result<(), TransportError>;
}

#[cfg(test)]
#[derive(Debug)]
struct DetachedConnectionOwner;

#[cfg(test)]
#[async_trait]
impl SshConnectionOwner for DetachedConnectionOwner {
    async fn close(self: Box<Self>, _force: CancellationToken) -> Result<(), TransportError> {
        Ok(())
    }
}

pub struct SftpProtocolSession {
    sftp: RawSftpSession,
    closed: std::sync::atomic::AtomicBool,
    capabilities: SftpCapabilities,
    limits: SftpLimits,
    // Taken exactly once by `close`; the pool guarantees no operations are in
    // flight when it closes a session, so contention here is teardown-only.
    owner: std::sync::Mutex<Option<Box<dyn SshConnectionOwner>>>,
    #[cfg(test)]
    bench_session_slot: usize,
}

impl fmt::Debug for SftpProtocolSession {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SftpProtocolSession")
            .field("capabilities", &self.capabilities)
            .finish_non_exhaustive()
    }
}

impl Drop for SftpProtocolSession {
    fn drop(&mut self) {
        let _ = self.sftp.close_session();
    }
}

impl SftpProtocolSession {
    pub(crate) fn new(
        sftp: RawSftpSession,
        capabilities: SftpCapabilities,
        limits: SftpLimits,
        owner: Box<dyn SshConnectionOwner>,
    ) -> Self {
        Self {
            sftp,
            closed: std::sync::atomic::AtomicBool::new(false),
            capabilities,
            limits,
            owner: std::sync::Mutex::new(Some(owner)),
            #[cfg(test)]
            bench_session_slot: usize::try_from(
                NEXT_BENCH_SESSION_SLOT.fetch_add(1, Ordering::SeqCst) % 32,
            )
            .expect("benchmark session slot fits usize"),
        }
    }

    #[cfg(test)]
    pub(crate) async fn from_streams<W, R>(stdin: W, stdout: R) -> Result<Self, TransportError>
    where
        W: AsyncWrite + Unpin + Send + 'static,
        R: AsyncRead + Unpin + Send + 'static,
    {
        let (sftp, capabilities, limits) = handshake_sftp(tokio::io::join(stdout, stdin)).await?;
        Ok(Self::new(
            sftp,
            capabilities,
            limits,
            Box::new(DetachedConnectionOwner),
        ))
    }
    fn sftp(&self) -> Result<&RawSftpSession, TransportError> {
        if self.closed.load(std::sync::atomic::Ordering::Acquire) {
            return Err(TransportError::Operation(
                "SFTP session is closed".to_owned(),
            ));
        }
        Ok(&self.sftp)
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
        #[cfg(test)]
        let write_bytes = chunks.iter().fold(0_u64, |total, chunk| {
            total.saturating_add(u64::try_from(chunk.len()).unwrap_or(u64::MAX))
        });
        #[cfg(test)]
        let open_started = std::time::Instant::now();
        let opened = sftp
            .open(remote, flags, FileAttributes::default())
            .await
            .map_err(|error| map_sftp_error(path, error))?;
        #[cfg(test)]
        BENCH_OPEN_NANOS.fetch_add(bench_nanos(open_started.elapsed()), Ordering::SeqCst);
        let handle = opened.handle;
        let fsync = || async {
            if durable {
                sftp.fsync(handle.as_str())
                    .await
                    .map(|_| ())
                    .map_err(|error| map_sftp_error(path, error))
            } else {
                Ok(())
            }
        };
        let close_handle = || async {
            sftp.close(handle.as_str())
                .await
                .map(|_| ())
                .map_err(|error| map_sftp_close_error(path, error))
        };
        let planned = transfer_request_len(
            SFTP_WRITE_PACKET_SIZE,
            self.limits.write_len,
            self.limits.packet_len,
            handle.len(),
        )
        .and_then(|packet_size| plan_pipelined_writes(offset, chunks, packet_size));
        let requests = match planned {
            Ok(requests) => requests,
            Err(error) => {
                // The handle is already open on the server: a planning
                // failure must still retire it instead of leaking it for the
                // lifetime of the session.
                let close = close_handle().await;
                return finish_raw_handle(Err(error), close);
            }
        };
        #[cfg(test)]
        let window_started = std::time::Instant::now();
        let (operation, close) = if requests.len() <= SFTP_WRITE_REQUEST_CONCURRENCY {
            // The raw session queues every request onto the outbound stream
            // before its first await, and the server executes one channel's
            // requests in arrival order. Polling the writes, the fsync, and
            // the close in that order therefore preserves the durable
            // write-before-fsync-before-close ordering while all of their
            // replies are awaited in a single round-trip window.
            let writes = futures::future::join_all(
                requests
                    .into_iter()
                    .map(|request| send_pipelined_write(sftp, &handle, path, request)),
            );
            let (writes, fsync, close) = futures::join!(writes, fsync(), close_handle());
            let operation = writes
                .into_iter()
                .collect::<Result<(), TransportError>>()
                .and(fsync);
            (operation, close)
        } else {
            // Too many write requests for one burst: the write stream itself
            // paces sends against replies, so fsync and close only join the
            // window once every write has been acknowledged.
            match write_handle_pipelined(sftp, &handle, path, requests).await {
                Ok(()) => futures::join!(fsync(), close_handle()),
                Err(error) => (Err(error), close_handle().await),
            }
        };
        #[cfg(test)]
        {
            BENCH_WRITE_NANOS.fetch_add(bench_nanos(window_started.elapsed()), Ordering::SeqCst);
            BENCH_PUBLICATIONS.fetch_add(1, Ordering::SeqCst);
            BENCH_SESSION_PUBLICATIONS[self.bench_session_slot].fetch_add(1, Ordering::SeqCst);
            BENCH_SESSION_WRITE_BYTES[self.bench_session_slot]
                .fetch_add(write_bytes, Ordering::SeqCst);
        }
        finish_raw_handle(operation, close)
    }
}

#[cfg(test)]
pub(crate) static CLIENT_WRITE_TRACKING: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(false);
#[cfg(test)]
pub(crate) static CLIENT_WRITE_IN_FLIGHT: std::sync::atomic::AtomicUsize =
    std::sync::atomic::AtomicUsize::new(0);
#[cfg(test)]
pub(crate) static CLIENT_WRITE_PEAK: std::sync::atomic::AtomicUsize =
    std::sync::atomic::AtomicUsize::new(0);

async fn send_pipelined_write(
    sftp: &RawSftpSession,
    handle: &str,
    path: &Path,
    request: PipelinedWrite,
) -> Result<(), TransportError> {
    #[cfg(test)]
    let track_write = CLIENT_WRITE_TRACKING.load(Ordering::SeqCst);
    #[cfg(test)]
    if track_write {
        let current = CLIENT_WRITE_IN_FLIGHT.fetch_add(1, Ordering::SeqCst) + 1;
        CLIENT_WRITE_PEAK.fetch_max(current, Ordering::SeqCst);
    }
    let result = sftp
        .write(handle, request.offset, request.payload.to_vec())
        .await
        .map(|_| ())
        .map_err(|error| map_sftp_error(path, error));
    #[cfg(test)]
    if track_write {
        CLIENT_WRITE_IN_FLIGHT.fetch_sub(1, Ordering::SeqCst);
    }
    result
}

async fn write_handle_pipelined(
    sftp: &RawSftpSession,
    handle: &str,
    path: &Path,
    requests: Vec<PipelinedWrite>,
) -> Result<(), TransportError> {
    futures::stream::iter(requests)
        .map(|request| send_pipelined_write(sftp, handle, path, request))
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
impl TransportSession for SftpProtocolSession {
    fn capabilities(&self) -> SftpCapabilities {
        self.capabilities
    }

    async fn read_object(
        &self,
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
            .map_err(|error| map_sftp_close_error(path, error));
        finish_raw_handle(result, close.map(|_| ()))
    }

    async fn list_directory(
        &self,
        path: &Path,
    ) -> Result<Vec<RemoteDirectoryEntry>, TransportError> {
        let sftp = self.sftp()?;
        let remote = sftp_path(path)?;
        let opened = sftp
            .opendir(remote)
            .await
            .map_err(|error| map_sftp_error(path, error))?;
        let handle = opened.handle;
        let operation = async {
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
                    Err(error) => return Err(map_sftp_error(path, error)),
                }
            }
            Ok(result)
        }
        .await;
        let close = sftp
            .close(handle)
            .await
            .map_err(|error| map_sftp_close_error(path, error))
            .map(|_| ());
        finish_raw_handle(operation, close)
    }

    async fn remove_file(&self, path: &Path) -> Result<(), TransportError> {
        let remote = sftp_path(path)?;
        #[cfg(test)]
        let started = std::time::Instant::now();
        let result = self
            .sftp()?
            .remove(remote)
            .await
            .map_err(|error| map_sftp_error(path, error))
            .map(|_| ());
        #[cfg(test)]
        BENCH_REMOVE_NANOS.fetch_add(bench_nanos(started.elapsed()), Ordering::SeqCst);
        result
    }

    async fn remove_directory(&self, path: &Path) -> Result<(), TransportError> {
        let remote = sftp_path(path)?;
        self.sftp()?
            .rmdir(remote)
            .await
            .map_err(|error| map_sftp_error(path, error))
            .map(|_| ())
    }

    async fn ensure_directory_component(&self, path: &Path) -> Result<(), TransportError> {
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
        &self,
        path: &Path,
        chunks: Vec<Bytes>,
    ) -> Result<(), TransportError> {
        self.write_chunks(path, 0, chunks, true, true).await
    }

    async fn write_file_at_durable(
        &self,
        path: &Path,
        offset: u64,
        chunks: Vec<Bytes>,
    ) -> Result<(), TransportError> {
        self.write_chunks(path, offset, chunks, false, true).await
    }

    async fn write_file_at(
        &self,
        path: &Path,
        offset: u64,
        chunks: Vec<Bytes>,
    ) -> Result<(), TransportError> {
        self.write_chunks(path, offset, chunks, false, false).await
    }

    async fn read_exact(
        &self,
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
            .map_err(|error| map_sftp_close_error(path, error))
            .map(|_| ());
        finish_raw_handle(bytes, close)
    }

    async fn hard_link(&self, from: &Path, to: &Path) -> Result<(), TransportError> {
        let sftp = self.sftp()?;
        #[cfg(test)]
        let started = std::time::Instant::now();
        let result = sftp
            .hardlink(sftp_path(from)?, sftp_path(to)?)
            .await
            .map(|_| ())
            .map_err(|error| map_sftp_error(to, error));
        #[cfg(test)]
        BENCH_HARDLINK_NANOS.fetch_add(bench_nanos(started.elapsed()), Ordering::SeqCst);
        result
    }

    async fn posix_rename(&self, from: &Path, to: &Path) -> Result<(), TransportError> {
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

    async fn close(&self, force: CancellationToken) -> Result<(), TransportError> {
        self.closed
            .store(true, std::sync::atomic::Ordering::Release);
        let _ = self.sftp.close_session();
        let owner = self.owner.lock().unwrap().take();
        match owner {
            Some(owner) => owner.close(force).await,
            None => Ok(()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn bounded_sftp_stream_rejects_oversized_length_before_forwarding_it() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let (mut peer, stream) = tokio::io::duplex(16);
        peer.write_u32(RUSSH_SFTP_MAX_PACKET_LEN + 1).await.unwrap();
        let mut bounded = BoundedSftpStream::new(stream, RUSSH_SFTP_MAX_PACKET_LEN);

        let error = bounded
            .read_u32()
            .await
            .expect_err("oversized length must be rejected before downstream allocation");

        assert_eq!(error.kind(), std::io::ErrorKind::InvalidData);
        assert!(error.to_string().contains("exceeds limit"));
        let eof = bounded
            .read_u32()
            .await
            .expect_err("an oversized frame must poison the reader instead of busy-looping");
        assert_eq!(eof.kind(), std::io::ErrorKind::UnexpectedEof);
    }

    #[tokio::test]
    async fn bounded_sftp_stream_preserves_valid_packet_boundaries() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let (mut peer, stream) = tokio::io::duplex(32);
        peer.write_all(&[0, 0, 0, 3, b'a', b'b', b'c', 0, 0, 0, 2, b'd', b'e'])
            .await
            .unwrap();
        let mut bounded = BoundedSftpStream::new(stream, 3);

        assert_eq!(bounded.read_u32().await.unwrap(), 3);
        let mut first = [0; 3];
        bounded.read_exact(&mut first).await.unwrap();
        assert_eq!(&first, b"abc");
        assert_eq!(bounded.read_u32().await.unwrap(), 2);
        let mut second = [0; 2];
        bounded.read_exact(&mut second).await.unwrap();
        assert_eq!(&second, b"de");
    }

    #[test]
    fn raw_handle_close_failure_overrides_reusable_operation_errors() {
        let close_error = map_sftp_close_error(
            Path::new("object"),
            russh_sftp::client::error::Error::Status(russh_sftp::protocol::Status {
                id: 1,
                status_code: StatusCode::NoSuchFile,
                error_message: "missing handle".to_owned(),
                language_tag: "en-US".to_owned(),
            }),
        );
        let error = finish_raw_handle::<()>(
            Err(TransportError::NotFound("object".to_owned())),
            Err(close_error),
        )
        .expect_err("a failed CLOSE must retire the physical session");

        assert!(matches!(error, TransportError::Close(_)), "{error:?}");
    }

    #[test]
    fn pipelined_write_plan_matches_the_proven_raw_sftp_request_window() {
        let payload = Bytes::from(vec![0u8; SFTP_WRITE_PACKET_SIZE * 128 + 17]);
        let plan = plan_pipelined_writes(0, vec![payload], SFTP_WRITE_PACKET_SIZE).unwrap();
        assert_eq!(plan.len(), 129);
        assert!(
            plan.iter()
                .all(|request| request.payload.len() <= SFTP_WRITE_PACKET_SIZE)
        );
        assert_eq!(plan[0].payload.len(), SFTP_WRITE_PACKET_SIZE);
        assert_eq!(plan[63].payload.len(), SFTP_WRITE_PACKET_SIZE);
        assert_eq!(plan[128].payload.len(), 17);
        assert_eq!(plan[1].offset, SFTP_WRITE_PACKET_SIZE as u64);
        assert_eq!(SFTP_WRITE_REQUEST_CONCURRENCY, 128);
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

    #[tokio::test]
    async fn a_write_plan_failure_after_open_still_closes_the_remote_handle() {
        use russh_sftp::protocol::{Handle as HandlePacket, Packet, Status, StatusCode, Version};
        use std::collections::HashMap;
        use std::sync::{Arc, Mutex};
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        async fn send_packet<W: tokio::io::AsyncWrite + Unpin>(writer: &mut W, packet: Packet) {
            let bytes = Bytes::try_from(packet).unwrap();
            writer.write_all(&bytes).await.unwrap();
            writer.flush().await.unwrap();
        }

        let (client_stream, server_stream) = tokio::io::duplex(1 << 16);
        let arrivals: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
        let server_arrivals = Arc::clone(&arrivals);
        tokio::spawn(async move {
            let (mut reader, mut writer) = tokio::io::split(server_stream);
            loop {
                let Ok(length) = reader.read_u32().await else {
                    break;
                };
                let mut frame = vec![0_u8; length as usize];
                if frame.is_empty() || reader.read_exact(&mut frame).await.is_err() {
                    break;
                }
                let mut frame = Bytes::from(frame);
                let Ok(packet) = Packet::try_from(&mut frame) else {
                    break;
                };
                match packet {
                    Packet::Init(_) => {
                        send_packet(
                            &mut writer,
                            Packet::Version(Version {
                                version: 3,
                                extensions: HashMap::from([(FSYNC.to_owned(), "1".to_owned())]),
                            }),
                        )
                        .await;
                    }
                    Packet::Open(open) => {
                        send_packet(
                            &mut writer,
                            Packet::Handle(HandlePacket {
                                id: open.id,
                                handle: "handle-1".to_owned(),
                            }),
                        )
                        .await;
                    }
                    Packet::Close(close) => {
                        server_arrivals.lock().unwrap().push("close".to_owned());
                        send_packet(
                            &mut writer,
                            Packet::Status(Status {
                                id: close.id,
                                status_code: StatusCode::Ok,
                                error_message: "ok".to_owned(),
                                language_tag: "en-US".to_owned(),
                            }),
                        )
                        .await;
                    }
                    _ => break,
                }
            }
        });

        let (read_half, write_half) = tokio::io::split(client_stream);
        let session = SftpProtocolSession::from_streams(write_half, read_half)
            .await
            .unwrap();
        // An offset at u64::MAX makes the write plan overflow after the
        // handle is already open on the server.
        let result = tokio::time::timeout(
            std::time::Duration::from_secs(3),
            session.write_file_at_durable(
                Path::new("staging-object"),
                u64::MAX,
                vec![Bytes::from_static(b"payload")],
            ),
        )
        .await
        .expect("a failed write plan must resolve promptly");
        result.expect_err("an overflowing write plan must fail the operation");

        assert_eq!(
            arrivals.lock().unwrap().as_slice(),
            ["close"],
            "the opened remote handle must be closed even when planning fails"
        );
    }

    #[tokio::test]
    async fn durable_write_pipelines_writes_fsync_and_close_into_one_request_window() {
        use russh_sftp::protocol::{Handle as HandlePacket, Packet, Status, StatusCode, Version};
        use std::collections::HashMap;
        use std::sync::{Arc, Mutex};
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        async fn send_packet<W: tokio::io::AsyncWrite + Unpin>(writer: &mut W, packet: Packet) {
            let bytes = Bytes::try_from(packet).unwrap();
            writer.write_all(&bytes).await.unwrap();
            writer.flush().await.unwrap();
        }

        let (client_stream, server_stream) = tokio::io::duplex(1 << 20);
        let arrivals: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
        let server_arrivals = Arc::clone(&arrivals);
        tokio::spawn(async move {
            let (mut reader, mut writer) = tokio::io::split(server_stream);
            let mut withheld: Vec<u32> = Vec::new();
            loop {
                let Ok(length) = reader.read_u32().await else {
                    break;
                };
                let mut frame = vec![0_u8; length as usize];
                if frame.is_empty() || reader.read_exact(&mut frame).await.is_err() {
                    break;
                }
                let mut frame = Bytes::from(frame);
                let Ok(packet) = Packet::try_from(&mut frame) else {
                    break;
                };
                match packet {
                    Packet::Init(_) => {
                        let extensions = HashMap::from([
                            (FSYNC.to_owned(), "1".to_owned()),
                            (HARDLINK.to_owned(), "1".to_owned()),
                            (POSIX_RENAME.to_owned(), "1".to_owned()),
                        ]);
                        send_packet(
                            &mut writer,
                            Packet::Version(Version {
                                version: 3,
                                extensions,
                            }),
                        )
                        .await;
                    }
                    Packet::Open(open) => {
                        send_packet(
                            &mut writer,
                            Packet::Handle(HandlePacket {
                                id: open.id,
                                handle: "handle-1".to_owned(),
                            }),
                        )
                        .await;
                    }
                    Packet::Write(write) => {
                        server_arrivals
                            .lock()
                            .unwrap()
                            .push(format!("write@{}", write.offset));
                        withheld.push(write.id);
                    }
                    Packet::Extended(extended) => {
                        server_arrivals
                            .lock()
                            .unwrap()
                            .push(extended.request.clone());
                        withheld.push(extended.id);
                    }
                    Packet::Close(close) => {
                        // Nothing is acknowledged until the whole durable
                        // window has arrived: a client that awaits each of
                        // write, fsync, and close before sending the next
                        // request can never reach this reply.
                        server_arrivals.lock().unwrap().push("close".to_owned());
                        withheld.push(close.id);
                        for id in withheld.drain(..) {
                            send_packet(
                                &mut writer,
                                Packet::Status(Status {
                                    id,
                                    status_code: StatusCode::Ok,
                                    error_message: "ok".to_owned(),
                                    language_tag: "en-US".to_owned(),
                                }),
                            )
                            .await;
                        }
                    }
                    _ => break,
                }
            }
        });

        let (read_half, write_half) = tokio::io::split(client_stream);
        let session = SftpProtocolSession::from_streams(write_half, read_half)
            .await
            .unwrap();
        let chunks = vec![
            Bytes::from_static(b"first"),
            Bytes::from_static(b"second"),
            Bytes::from_static(b"third"),
        ];
        tokio::time::timeout(
            std::time::Duration::from_secs(3),
            session.write_file_durable(Path::new("staging-object"), chunks),
        )
        .await
        .expect("a durable write must issue writes, fsync, and close in one request window")
        .unwrap();

        let arrivals = arrivals.lock().unwrap().clone();
        let writes = arrivals
            .iter()
            .filter(|entry| entry.starts_with("write@"))
            .count();
        let last_write = arrivals
            .iter()
            .rposition(|entry| entry.starts_with("write@"))
            .expect("write requests must arrive");
        let fsync_at = arrivals
            .iter()
            .position(|entry| entry == FSYNC)
            .expect("the fsync request must arrive");
        let close_at = arrivals
            .iter()
            .position(|entry| entry == "close")
            .expect("the close request must arrive");
        assert_eq!(writes, 3);
        assert!(
            last_write < fsync_at,
            "fsync must be sent after every write request: {arrivals:?}"
        );
        assert!(
            fsync_at < close_at,
            "close must be sent after fsync: {arrivals:?}"
        );
    }
}
