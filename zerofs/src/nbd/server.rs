use super::error::{CommandError, NBDError, Result};
use super::handler::{NBDDevice, NBDHandler, NbdExportGates, OptionReply, OptionResult};
use super::out_of_bounds;
use crate::fs::ZeroFS;
use bytes::BytesMut;
use deku::prelude::*;
use nbd_proto::*;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader, BufWriter};
use tokio::net::{TcpListener, UnixListener};
use tokio::task::JoinSet;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

const MAX_OPTION_LENGTH: u32 = 4096;
const MAX_REQUEST_LENGTH: u32 = 128 * 1024 * 1024;
const DISCARD_CHUNK_SIZE: usize = 64 * 1024;
/// Leave half of the CLI's serving-drain interval for abort/join and handoff.
const CLIENT_DRAIN_TIMEOUT: Duration = Duration::from_secs(1);
#[cfg(not(test))]
const WRITE_PAYLOAD_TIMEOUT: Duration = Duration::from_secs(30);
#[cfg(test)]
const WRITE_PAYLOAD_TIMEOUT: Duration = Duration::from_millis(100);

pub enum Transport {
    Tcp(SocketAddr),
    Unix(std::path::PathBuf),
}

pub struct NBDServer {
    filesystem: Arc<ZeroFS>,
    export_gates: Arc<NbdExportGates>,
    transport: Transport,
}

impl NBDServer {
    pub fn new_tcp(
        filesystem: Arc<ZeroFS>,
        export_gates: Arc<NbdExportGates>,
        socket: SocketAddr,
    ) -> Self {
        Self {
            filesystem,
            export_gates,
            transport: Transport::Tcp(socket),
        }
    }

    pub fn new_unix(
        filesystem: Arc<ZeroFS>,
        export_gates: Arc<NbdExportGates>,
        socket_path: impl Into<std::path::PathBuf>,
    ) -> Self {
        Self {
            filesystem,
            export_gates,
            transport: Transport::Unix(socket_path.into()),
        }
    }

    fn spawn_client_handler<S>(
        &self,
        clients: &mut JoinSet<()>,
        stream: S,
        shutdown: &CancellationToken,
        client_name: String,
    ) where
        S: AsyncRead + AsyncWrite + Unpin + Send + Sync + 'static,
    {
        let filesystem = Arc::clone(&self.filesystem);
        let export_gates = Arc::clone(&self.export_gates);
        let client_shutdown = shutdown.child_token();

        clients.spawn(async move {
            if let Err(e) =
                handle_client_stream(stream, filesystem, export_gates, client_shutdown).await
            {
                error!("Error handling NBD client {}: {}", client_name, e);
            }
        });
    }

    pub async fn start(&self, shutdown: CancellationToken) -> std::io::Result<()> {
        let clients_shutdown = shutdown.child_token();
        let mut clients = JoinSet::new();
        let serve_result = match &self.transport {
            Transport::Tcp(socket) => {
                let listener = TcpListener::bind(socket)
                    .await
                    .map_err(|e| crate::net_util::tcp_bind_error("NBD", socket, &e))?;
                info!("NBD server listening on {}", socket);

                loop {
                    tokio::select! {
                        biased;
                        _ = shutdown.cancelled() => {
                            info!("NBD TCP server shutting down on {}", socket);
                            break Ok(());
                        }
                        finished = clients.join_next(), if !clients.is_empty() => {
                            if let Some(Err(error)) = finished {
                                warn!("NBD client task failed: {error}");
                            }
                        }
                        result = listener.accept() => {
                            let (stream, addr) = match result {
                                Ok(accepted) => accepted,
                                Err(error) => break Err(error),
                            };
                            info!("NBD client connected from {}", addr);
                            if let Err(error) = stream.set_nodelay(true) {
                                warn!("Failed to configure NBD TCP client {addr}: {error}");
                                continue;
                            }
                            self.spawn_client_handler(
                                &mut clients,
                                stream,
                                &clients_shutdown,
                                addr.to_string(),
                            );
                        }
                    }
                }
            }
            Transport::Unix(path) => {
                // Remove existing socket file if it exists
                let _ = std::fs::remove_file(path);

                let listener = UnixListener::bind(path).map_err(|e| {
                    std::io::Error::new(
                        e.kind(),
                        format!("Failed to bind NBD Unix socket at {:?}: {}", path, e),
                    )
                })?;
                info!("NBD server listening on Unix socket {:?}", path);

                loop {
                    tokio::select! {
                        biased;
                        _ = shutdown.cancelled() => {
                            info!("NBD Unix socket server shutting down at {:?}", path);
                            break Ok(());
                        }
                        finished = clients.join_next(), if !clients.is_empty() => {
                            if let Some(Err(error)) = finished {
                                warn!("NBD client task failed: {error}");
                            }
                        }
                        result = listener.accept() => {
                            let (stream, _) = match result {
                                Ok(accepted) => accepted,
                                Err(error) => break Err(error),
                            };
                            info!("NBD client connected via Unix socket");
                            self.spawn_client_handler(
                                &mut clients,
                                stream,
                                &clients_shutdown,
                                "unix".to_string(),
                            );
                        }
                    }
                }
            }
        };

        clients_shutdown.cancel();
        drain_clients(&mut clients, Instant::now() + CLIENT_DRAIN_TIMEOUT).await;

        // A canceled filesystem operation can leave its already-submitted
        // transaction owned solely by the commit worker. Do not let the CLI
        // close the database until that ordered prefix has fully published.
        let commit_drain = self
            .filesystem
            .write_coordinator
            .barrier()
            .await
            .map_err(std::io::Error::other);

        serve_result?;
        commit_drain
    }
}

async fn drain_clients(clients: &mut JoinSet<()>, deadline: Instant) {
    while !clients.is_empty() {
        match tokio::time::timeout_at(deadline, clients.join_next()).await {
            Ok(Some(Err(error))) => warn!("NBD client task failed while draining: {error}"),
            Ok(Some(Ok(()))) => {}
            Ok(None) => return,
            Err(_) => break,
        }
    }

    if clients.is_empty() {
        return;
    }

    warn!(
        "NBD client drain reached its deadline with {} active connection(s); aborting them",
        clients.len()
    );
    clients.abort_all();
    while let Some(result) = clients.join_next().await {
        if let Err(error) = result
            && !error.is_cancelled()
        {
            warn!("NBD client task failed during abort: {error}");
        }
    }
}

async fn handle_client_stream<S>(
    stream: S,
    filesystem: Arc<ZeroFS>,
    export_gates: Arc<NbdExportGates>,
    shutdown: CancellationToken,
) -> Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + Sync + 'static,
{
    let (reader, writer) = tokio::io::split(stream);
    let reader = BufReader::new(reader);
    let writer = BufWriter::new(writer);

    let mut session = NBDSession::new(reader, writer, filesystem, export_gates, shutdown);
    session.perform_handshake().await?;

    match session.negotiate_options().await {
        Ok(device) => {
            info!(
                "Client selected device: {}",
                String::from_utf8_lossy(&device.name)
            );
            session.handle_transmission(device).await?;
        }
        Err(NBDError::Io(ref e)) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
            debug!("Client disconnected cleanly after option negotiation");
            return Ok(());
        }
        Err(e) => return Err(e),
    }

    Ok(())
}

struct NBDSession<R, W> {
    reader: R,
    writer: W,
    handler: NBDHandler,
    client_no_zeroes: bool,
    shutdown: CancellationToken,
}

impl<R: AsyncRead + Unpin, W: AsyncWrite + Unpin> NBDSession<R, W> {
    fn new(
        reader: R,
        writer: W,
        filesystem: Arc<ZeroFS>,
        export_gates: Arc<NbdExportGates>,
        shutdown: CancellationToken,
    ) -> Self {
        Self {
            reader,
            writer,
            handler: NBDHandler::new(filesystem, export_gates),
            client_no_zeroes: false,
            shutdown,
        }
    }

    async fn perform_handshake(&mut self) -> Result<()> {
        let handshake = NBDServerHandshake::new(NBD_FLAG_FIXED_NEWSTYLE | NBD_FLAG_NO_ZEROES);
        let handshake_bytes = handshake.to_bytes()?;
        self.writer.write_all(&handshake_bytes).await?;
        self.writer.flush().await?;

        let mut buf = [0u8; 4];
        self.read_exact_or_shutdown(&mut buf).await?;
        let client_flags = NBDClientFlags::from_bytes((&buf, 0))?.1;

        debug!("Client flags: 0x{:x}", client_flags.flags);

        if (client_flags.flags & NBD_FLAG_C_FIXED_NEWSTYLE) == 0 {
            return Err(NBDError::IncompatibleClient);
        }

        self.client_no_zeroes = (client_flags.flags & NBD_FLAG_C_NO_ZEROES) != 0;

        Ok(())
    }

    async fn negotiate_options(&mut self) -> Result<NBDDevice> {
        loop {
            let mut header_buf = [0u8; NBD_OPTION_HEADER_SIZE];
            match self.read_exact_or_shutdown(&mut header_buf).await {
                Ok(_) => {}
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                    // Client disconnected, this is normal after LIST
                    debug!("Client disconnected during option negotiation");
                    return Err(NBDError::Io(e));
                }
                Err(e) => return Err(NBDError::Io(e)),
            }
            let header = NBDOptionHeader::from_bytes((&header_buf, 0))
                .map_err(|e| {
                    debug!("Raw header bytes: {:02x?}", header_buf);
                    NBDError::Protocol(format!("Invalid option header: {e}"))
                })?
                .1;

            debug!(
                "Received option: {} (length: {})",
                header.option, header.length
            );

            if header.length > MAX_OPTION_LENGTH {
                return Err(NBDError::Protocol(format!(
                    "option data length {} exceeds max {MAX_OPTION_LENGTH}",
                    header.length
                )));
            }

            match header.option {
                NBD_OPT_LIST => {
                    debug!("Handling LIST option");
                    self.handle_list_option(header.length).await?;
                }
                NBD_OPT_EXPORT_NAME => {
                    debug!("Handling EXPORT_NAME option");
                    return self.handle_export_name_option(header.length).await;
                }
                NBD_OPT_INFO => {
                    debug!("Handling INFO option");
                    self.handle_info_option(header.length).await?;
                }
                NBD_OPT_GO => {
                    match self.handle_go_option(header.length).await {
                        Ok(device) => return Ok(device),
                        Err(NBDError::DeviceNotFound(_)) => {
                            // Device not found - stay in negotiation loop
                            // Error reply already sent by handle_go_option
                        }
                        Err(e) => return Err(e),
                    }
                }
                NBD_OPT_STRUCTURED_REPLY => {
                    debug!("Handling STRUCTURED_REPLY option");
                    self.handle_structured_reply_option(header.length).await?;
                }
                NBD_OPT_ABORT => {
                    debug!("Handling ABORT option");
                    self.send_option_reply(header.option, NBD_REP_ACK, &[])
                        .await?;
                    self.writer.flush().await?;
                    return Err(NBDError::Protocol("Client aborted".to_string()));
                }
                _ => {
                    debug!("Unknown option: {}", header.option);
                    self.drain_option_data(header.length).await?;
                    self.send_option_reply(header.option, NBD_REP_ERR_UNSUP, &[])
                        .await?;
                    self.writer.flush().await?;
                }
            }
        }
    }

    async fn handle_list_option(&mut self, length: u32) -> Result<()> {
        self.drain_option_data(length).await?;
        let result = self.handler.list().await;
        self.process_option_result(NBD_OPT_LIST, result).await?;
        Ok(())
    }

    async fn handle_export_name_option(&mut self, length: u32) -> Result<NBDDevice> {
        let mut name_buf = vec![0u8; length as usize];
        self.read_exact_or_shutdown(&mut name_buf).await?;

        debug!(
            "Client requested export: '{}' (length: {})",
            String::from_utf8_lossy(&name_buf),
            length
        );

        // For NBD_OPT_EXPORT_NAME, we can't send an error reply
        // We must either send the export info or close the connection
        let device = self.handler.get_device(&name_buf).await.map_err(|e| {
            error!(
                "Export '{}' not found, closing connection: {:?}",
                String::from_utf8_lossy(&name_buf),
                e
            );
            NBDError::DeviceNotFound(name_buf.clone())
        })?;

        self.writer.write_all(&device.size.to_be_bytes()).await?;
        self.writer
            .write_all(&TRANSMISSION_FLAGS.to_be_bytes())
            .await?;

        if !self.client_no_zeroes {
            self.writer
                .write_all(&[0u8; NBD_EXPORT_NAME_PADDING])
                .await?;
        }

        self.writer.flush().await?;
        Ok(device)
    }

    async fn handle_info_option(&mut self, length: u32) -> Result<()> {
        let data = self.read_option_data(length).await?;
        let result = self.handler.info(&data).await;
        self.process_option_result(NBD_OPT_INFO, result).await?;
        Ok(())
    }

    async fn handle_go_option(&mut self, length: u32) -> Result<NBDDevice> {
        let data = self.read_option_data(length).await?;
        let result = self.handler.go(&data).await;
        match self.process_option_result(NBD_OPT_GO, result).await? {
            Some(device) => Ok(device),
            None => Err(NBDError::DeviceNotFound(Vec::new())),
        }
    }

    async fn handle_structured_reply_option(&mut self, length: u32) -> Result<()> {
        self.drain_option_data(length).await?;
        self.send_option_reply(NBD_OPT_STRUCTURED_REPLY, NBD_REP_ERR_UNSUP, &[])
            .await?;
        self.writer.flush().await?;
        Ok(())
    }

    /// Read option data from the stream
    async fn read_option_data(&mut self, length: u32) -> Result<Vec<u8>> {
        let mut data = vec![0u8; length as usize];
        self.read_exact_or_shutdown(&mut data).await?;
        Ok(data)
    }

    /// Drain any remaining option data from the reader
    async fn drain_option_data(&mut self, length: u32) -> Result<()> {
        if length > 0 {
            let mut buf = vec![0u8; length as usize];
            self.read_exact_or_shutdown(&mut buf).await?;
        }
        Ok(())
    }

    /// Process option result from handler - send replies and return device if done
    async fn process_option_result(
        &mut self,
        option: u32,
        result: OptionResult,
    ) -> Result<Option<NBDDevice>> {
        match result {
            OptionResult::Continue(replies) => {
                self.send_option_replies(option, &replies).await?;
                Ok(None)
            }
            OptionResult::Done(device, replies) => {
                self.send_option_replies(option, &replies).await?;
                Ok(Some(device))
            }
            OptionResult::Error(err, replies) => {
                self.send_option_replies(option, &replies).await?;
                Err(err)
            }
        }
    }

    /// Send multiple option replies and flush
    async fn send_option_replies(&mut self, option: u32, replies: &[OptionReply]) -> Result<()> {
        for reply in replies {
            self.send_option_reply(option, reply.reply_type, &reply.data)
                .await?;
        }
        self.writer.flush().await?;
        Ok(())
    }

    async fn send_option_reply(&mut self, option: u32, reply_type: u32, data: &[u8]) -> Result<()> {
        let reply = NBDOptionReply::new(option, reply_type, data.len() as u32);
        let reply_bytes = reply.to_bytes()?;
        self.writer.write_all(&reply_bytes).await?;
        if !data.is_empty() {
            self.writer.write_all(data).await?;
        }
        Ok(())
    }

    async fn handle_transmission(&mut self, device: NBDDevice) -> Result<()> {
        loop {
            let mut request_buf = [0u8; NBD_REQUEST_HEADER_SIZE];

            tokio::select! {
                biased;
                _ = self.shutdown.cancelled() => {
                    debug!("NBD client handler shutting down");
                    return Ok(());
                }
                result = self.reader.read_exact(&mut request_buf) => {
                    result?;
                }
            }

            let request = NBDRequest::from_bytes((&request_buf, 0))
                .map_err(|e| NBDError::Protocol(format!("Invalid request: {e}")))?
                .1;

            debug!(
                "NBD command: {:?}, offset={}, length={}",
                request.cmd_type, request.offset, request.length
            );

            if request.length > MAX_REQUEST_LENGTH {
                if request.cmd_type == NBDCommand::Write {
                    return Err(NBDError::Protocol(format!(
                        "write length {} exceeds max {MAX_REQUEST_LENGTH}",
                        request.length
                    )));
                }
                self.send_unit_result(request.cookie, Err(CommandError::InvalidArgument))
                    .await;
                continue;
            }

            let fua = (request.flags & NBD_CMD_FLAG_FUA) != 0;

            match request.cmd_type {
                NBDCommand::Read => {
                    let result = self
                        .handler
                        .read(&device, request.offset, request.length)
                        .await;
                    self.send_read_result(request.cookie, result).await;
                }
                NBDCommand::Write => {
                    let result = self
                        .read_write_data(&device, request.offset, request.length, fua, device.size)
                        .await;
                    self.send_unit_result(request.cookie, result).await;
                }
                NBDCommand::Disconnect => {
                    info!("Client disconnecting");
                    return Ok(());
                }
                NBDCommand::Flush => {
                    let result = self.handler.flush(&device).await;
                    self.send_unit_result(request.cookie, result).await;
                }
                NBDCommand::Trim => {
                    let result = self
                        .handler
                        .trim(&device, request.offset, request.length, fua)
                        .await;
                    self.send_unit_result(request.cookie, result).await;
                }
                NBDCommand::WriteZeroes => {
                    self.send_unit_result(request.cookie, Err(CommandError::InvalidArgument))
                        .await;
                }
                NBDCommand::Cache => {
                    let result = self
                        .handler
                        .cache(request.offset, request.length, device.size)
                        .await;
                    self.send_unit_result(request.cookie, result).await;
                }
                NBDCommand::Unknown(cmd) => {
                    warn!("Unknown NBD command: {}", cmd);
                    self.send_unit_result(
                        request.cookie,
                        Err(super::error::CommandError::InvalidArgument),
                    )
                    .await;
                }
            }
        }
    }

    async fn read_exact_or_shutdown(&mut self, buffer: &mut [u8]) -> std::io::Result<()> {
        tokio::select! {
            biased;
            _ = self.shutdown.cancelled() => Err(std::io::Error::new(
                std::io::ErrorKind::Interrupted,
                "NBD session is shutting down",
            )),
            result = self.reader.read_exact(buffer) => result.map(|_| ()),
        }
    }

    /// Read write data from stream and delegate to handler
    async fn read_write_data(
        &mut self,
        device: &NBDDevice,
        offset: u64,
        length: u32,
        fua: bool,
        device_size: u64,
    ) -> super::error::CommandResult<()> {
        // Consume an invalid write's payload to keep the request stream aligned.
        if out_of_bounds(offset, length, device_size) {
            let mut remaining = length as usize;
            let mut buf = vec![0; remaining.min(DISCARD_CHUNK_SIZE)];
            while remaining > 0 {
                let chunk = remaining.min(buf.len());
                self.reader
                    .read_exact(&mut buf[..chunk])
                    .await
                    .map_err(|_| CommandError::IoError)?;
                remaining -= chunk;
            }
            return Err(CommandError::NoSpace);
        }

        if length == 0 {
            return Ok(());
        }

        // Admission starts when the valid WRITE request is accepted, before
        // its payload arrives. A FLUSH on another NBD connection must not
        // overtake a request whose body is still in flight.
        let admission = self.handler.begin_mutation(device).await;
        let mut data = BytesMut::zeroed(length as usize);
        tokio::select! {
            _ = self.shutdown.cancelled() => return Err(CommandError::IoError),
            result = tokio::time::timeout(
                WRITE_PAYLOAD_TIMEOUT,
                self.reader.read_exact(&mut data),
            ) => {
                match result {
                    Ok(read) => {
                        read.map_err(|_| CommandError::IoError)?;
                    }
                    Err(_) => {
                        // The remainder of a timed-out payload cannot be
                        // distinguished from a later request. Close this
                        // session after returning EIO rather than continuing
                        // on a desynchronized transmission stream.
                        self.shutdown.cancel();
                        return Err(CommandError::IoError);
                    }
                }
            }
        }

        let data = data.freeze();
        self.handler
            .write_admitted(device, offset, &data, fua, admission)
            .await
    }

    /// Send read result (with data) as NBD reply
    async fn send_read_result(
        &mut self,
        cookie: u64,
        result: super::error::CommandResult<bytes::Bytes>,
    ) {
        match result {
            Ok(data) => {
                if let Err(e) = self.send_simple_reply(cookie, NBD_SUCCESS, &data).await {
                    debug!("Failed to send reply: {:?}", e);
                }
            }
            Err(e) => {
                let _ = self.send_simple_reply(cookie, e.to_errno(), &[]).await;
            }
        }
    }

    /// Send unit result (no data) as NBD reply
    async fn send_unit_result(&mut self, cookie: u64, result: super::error::CommandResult<()>) {
        match result {
            Ok(()) => {
                if let Err(e) = self.send_simple_reply(cookie, NBD_SUCCESS, &[]).await {
                    debug!("Failed to send reply: {:?}", e);
                }
            }
            Err(e) => {
                let _ = self.send_simple_reply(cookie, e.to_errno(), &[]).await;
            }
        }
    }

    async fn send_simple_reply(&mut self, cookie: u64, error: u32, data: &[u8]) -> Result<()> {
        let reply = NBDSimpleReply::new(cookie, error);
        let reply_bytes = reply.to_bytes()?;
        self.writer.write_all(&reply_bytes).await?;
        if !data.is_empty() {
            self.writer.write_all(data).await?;
        }
        self.writer.flush().await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::{NBDServer, NBDSession};
    use crate::fs::ZeroFS;
    use crate::fs::permissions::Credentials;
    use crate::fs::types::{SetAttributes, SetSize};
    use crate::nbd::handler::{NBDHandler, NbdExportGates};
    use bytes::Bytes;
    use deku::{DekuContainerRead, DekuContainerWrite};
    use nbd_proto::{
        NBD_EINVAL, NBD_FLAG_C_FIXED_NEWSTYLE, NBD_FLAG_C_NO_ZEROES, NBD_IHAVEOPT,
        NBD_OPT_EXPORT_NAME, NBD_REQUEST_MAGIC, NBDCommand, NBDRequest, NBDSimpleReply,
    };
    use std::io;
    use std::pin::Pin;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::task::{Context, Poll};
    use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt, ReadBuf};
    use tokio::net::{TcpStream, UnixStream};
    use tokio::sync::oneshot;
    use tokio::time::{Duration, timeout};
    use tokio_util::sync::CancellationToken;

    struct BlockingPayload {
        started: Option<oneshot::Sender<()>>,
    }

    impl AsyncRead for BlockingPayload {
        fn poll_read(
            mut self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            _buf: &mut ReadBuf<'_>,
        ) -> Poll<io::Result<()>> {
            if let Some(started) = self.started.take() {
                let _ = started.send(());
            }
            Poll::Pending
        }
    }

    struct PartialThenBlockingPayload {
        started: Option<oneshot::Sender<()>>,
        delivered: bool,
    }

    struct PrebufferedRequest {
        bytes: Vec<u8>,
        position: usize,
        polls: Arc<AtomicUsize>,
    }

    impl AsyncRead for PrebufferedRequest {
        fn poll_read(
            mut self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            buf: &mut ReadBuf<'_>,
        ) -> Poll<io::Result<()>> {
            self.polls.fetch_add(1, Ordering::Relaxed);
            let remaining = &self.bytes[self.position..];
            let length = remaining.len().min(buf.remaining());
            buf.put_slice(&remaining[..length]);
            self.position += length;
            Poll::Ready(Ok(()))
        }
    }

    impl AsyncRead for PartialThenBlockingPayload {
        fn poll_read(
            mut self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            buf: &mut ReadBuf<'_>,
        ) -> Poll<io::Result<()>> {
            if !self.delivered {
                let length = 512.min(buf.remaining());
                buf.put_slice(&vec![0x5a; length]);
                self.delivered = true;
                if let Some(started) = self.started.take() {
                    let _ = started.send(());
                }
                return Poll::Ready(Ok(()));
            }
            Poll::Pending
        }
    }

    fn root_credentials() -> Credentials {
        Credentials {
            uid: 0,
            gid: 0,
            gid_known: true,
            groups: [0; 16],
            groups_count: 1,
            groups_complete: true,
        }
    }

    async fn single_file_export(
        filesystem: &Arc<ZeroFS>,
        export_gates: &Arc<NbdExportGates>,
    ) -> crate::nbd::handler::NBDDevice {
        let credentials = root_credentials();
        let (nbd_dir, _) = filesystem
            .mkdir(&credentials, 0, b".nbd", &SetAttributes::default())
            .await
            .expect("create .nbd directory");
        let (inode, _) = filesystem
            .create(
                &credentials,
                nbd_dir,
                b"flush-ordering-test",
                &SetAttributes::default(),
            )
            .await
            .expect("create test export");
        filesystem
            .setattr(
                &credentials,
                inode,
                &SetAttributes {
                    size: SetSize::Set(4096),
                    ..Default::default()
                },
            )
            .await
            .expect("size test export");

        NBDHandler::new(Arc::clone(filesystem), Arc::clone(export_gates))
            .get_device(b"flush-ordering-test")
            .await
            .expect("discover test export")
    }

    async fn assert_server_shutdown_closes_stalled_handshake<S>(
        mut client: S,
        shutdown: CancellationToken,
        server_task: tokio::task::JoinHandle<io::Result<()>>,
    ) where
        S: AsyncRead + Unpin,
    {
        let mut handshake = [0; 18];
        timeout(Duration::from_secs(2), client.read_exact(&mut handshake))
            .await
            .expect("server sent its handshake")
            .expect("read server handshake");

        shutdown.cancel();
        timeout(Duration::from_secs(2), server_task)
            .await
            .expect("NBD server stopped after cancellation")
            .expect("NBD server task did not panic")
            .expect("NBD server stopped cleanly");

        let mut trailing = [0];
        assert_eq!(
            timeout(Duration::from_secs(2), client.read(&mut trailing))
                .await
                .expect("tracked client task closed after server shutdown")
                .expect("read client EOF"),
            0,
            "NBD start() must not return while a stalled handshake task owns the socket"
        );
    }

    async fn enter_transmission<S>(client: &mut S, export: &[u8])
    where
        S: AsyncRead + tokio::io::AsyncWrite + Unpin,
    {
        let mut handshake = [0; 18];
        timeout(Duration::from_secs(2), client.read_exact(&mut handshake))
            .await
            .expect("server sent its handshake")
            .expect("read server handshake");
        client
            .write_all(&(NBD_FLAG_C_FIXED_NEWSTYLE | NBD_FLAG_C_NO_ZEROES).to_be_bytes())
            .await
            .expect("send client flags");
        client
            .write_all(&NBD_IHAVEOPT.to_be_bytes())
            .await
            .expect("send option magic");
        client
            .write_all(&NBD_OPT_EXPORT_NAME.to_be_bytes())
            .await
            .expect("send export-name option");
        client
            .write_all(&(export.len() as u32).to_be_bytes())
            .await
            .expect("send export-name length");
        client.write_all(export).await.expect("send export name");

        let mut export_info = [0; 10];
        timeout(Duration::from_secs(2), client.read_exact(&mut export_info))
            .await
            .expect("server sent export info")
            .expect("read export info");
    }

    async fn assert_server_shutdown_waits_for_accepted_commit<S>(
        mut client: S,
        shutdown: CancellationToken,
        mut server_task: tokio::task::JoinHandle<io::Result<()>>,
        filesystem: Arc<ZeroFS>,
        export_gates: Arc<NbdExportGates>,
        apply_reached: oneshot::Receiver<()>,
        commit_block: tokio::sync::OwnedRwLockWriteGuard<()>,
    ) where
        S: AsyncRead + tokio::io::AsyncWrite + Unpin,
    {
        enter_transmission(&mut client, b"flush-ordering-test").await;
        let write = NBDRequest {
            magic: NBD_REQUEST_MAGIC,
            flags: 0,
            cmd_type: NBDCommand::Write,
            cookie: 0x1122_3344_5566_7788,
            offset: 0,
            length: 1,
        };
        client
            .write_all(&write.to_bytes().expect("encode write"))
            .await
            .expect("send write header");
        client.write_all(&[0x7b]).await.expect("send write payload");
        timeout(Duration::from_secs(2), apply_reached)
            .await
            .expect("accepted write reached the commit boundary")
            .expect("commit apply probe remained installed");

        shutdown.cancel();
        let mut trailing = [0];
        assert_eq!(
            timeout(Duration::from_secs(3), client.read(&mut trailing))
                .await
                .expect("bounded client retirement closed the active connection")
                .expect("read active-client EOF"),
            0,
            "the active NBD client must be aborted and joined at the drain deadline"
        );
        assert!(
            !server_task.is_finished(),
            "NBD start() returned before its accepted WriteCoordinator commit finished"
        );

        drop(commit_block);
        timeout(Duration::from_secs(2), &mut server_task)
            .await
            .expect("NBD server returned after the accepted commit completed")
            .expect("NBD server task did not panic")
            .expect("NBD server stopped cleanly");

        let handler = NBDHandler::new(Arc::clone(&filesystem), export_gates);
        let device = handler
            .get_device(b"flush-ordering-test")
            .await
            .expect("reopen test export");
        assert_eq!(
            handler
                .read(&device, 0, 1)
                .await
                .expect("read committed byte"),
            Bytes::from_static(&[0x7b]),
            "the queued write must finish before NBD server shutdown returns"
        );
    }

    #[tokio::test]
    async fn unix_shutdown_joins_a_client_that_withholds_handshake_flags() {
        let filesystem = Arc::new(
            ZeroFS::new_in_memory()
                .await
                .expect("create test filesystem"),
        );
        let directory = tempfile::tempdir().expect("temporary socket directory");
        let socket = directory.path().join("nbd.sock");
        let server = NBDServer::new_unix(filesystem, Arc::new(NbdExportGates::default()), &socket);
        let shutdown = CancellationToken::new();
        let server_shutdown = shutdown.clone();
        let server_task = tokio::spawn(async move { server.start(server_shutdown).await });
        let client = timeout(Duration::from_secs(2), async {
            loop {
                match UnixStream::connect(&socket).await {
                    Ok(client) => return client,
                    Err(error) if error.kind() == io::ErrorKind::NotFound => {
                        tokio::task::yield_now().await;
                    }
                    Err(error) => panic!("connect Unix NBD client: {error}"),
                }
            }
        })
        .await
        .expect("Unix NBD listener became ready");

        assert_server_shutdown_closes_stalled_handshake(client, shutdown, server_task).await;
    }

    #[tokio::test]
    async fn tcp_shutdown_joins_a_client_that_withholds_handshake_flags() {
        let filesystem = Arc::new(
            ZeroFS::new_in_memory()
                .await
                .expect("create test filesystem"),
        );
        let reserved = std::net::TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0))
            .expect("reserve loopback address");
        let address = reserved.local_addr().expect("reserved loopback address");
        drop(reserved);
        let server = NBDServer::new_tcp(filesystem, Arc::new(NbdExportGates::default()), address);
        let shutdown = CancellationToken::new();
        let server_shutdown = shutdown.clone();
        let server_task = tokio::spawn(async move { server.start(server_shutdown).await });
        let client = timeout(Duration::from_secs(2), async {
            loop {
                match TcpStream::connect(address).await {
                    Ok(client) => return client,
                    Err(error) if error.kind() == io::ErrorKind::ConnectionRefused => {
                        tokio::task::yield_now().await;
                    }
                    Err(error) => panic!("connect TCP NBD client: {error}"),
                }
            }
        })
        .await
        .expect("TCP NBD listener became ready");

        assert_server_shutdown_closes_stalled_handshake(client, shutdown, server_task).await;
    }

    #[tokio::test]
    async fn unix_shutdown_waits_for_an_accepted_commit_before_returning() {
        let filesystem = Arc::new(
            ZeroFS::new_in_memory()
                .await
                .expect("create test filesystem"),
        );
        let export_gates = Arc::new(NbdExportGates::default());
        single_file_export(&filesystem, &export_gates).await;
        let commit_block = filesystem.db.flush_barrier().write_owned().await;
        let apply_reached = filesystem.write_coordinator.probe_next_apply();
        let directory = tempfile::tempdir().expect("temporary socket directory");
        let socket = directory.path().join("nbd.sock");
        let server =
            NBDServer::new_unix(Arc::clone(&filesystem), Arc::clone(&export_gates), &socket);
        let shutdown = CancellationToken::new();
        let server_shutdown = shutdown.clone();
        let server_task = tokio::spawn(async move { server.start(server_shutdown).await });
        let client = timeout(Duration::from_secs(2), async {
            loop {
                match UnixStream::connect(&socket).await {
                    Ok(client) => return client,
                    Err(error) if error.kind() == io::ErrorKind::NotFound => {
                        tokio::task::yield_now().await;
                    }
                    Err(error) => panic!("connect Unix NBD client: {error}"),
                }
            }
        })
        .await
        .expect("Unix NBD listener became ready");

        assert_server_shutdown_waits_for_accepted_commit(
            client,
            shutdown,
            server_task,
            filesystem,
            export_gates,
            apply_reached,
            commit_block,
        )
        .await;
    }

    #[tokio::test]
    async fn tcp_shutdown_waits_for_an_accepted_commit_before_returning() {
        let filesystem = Arc::new(
            ZeroFS::new_in_memory()
                .await
                .expect("create test filesystem"),
        );
        let export_gates = Arc::new(NbdExportGates::default());
        single_file_export(&filesystem, &export_gates).await;
        let commit_block = filesystem.db.flush_barrier().write_owned().await;
        let apply_reached = filesystem.write_coordinator.probe_next_apply();
        let reserved = std::net::TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0))
            .expect("reserve loopback address");
        let address = reserved.local_addr().expect("reserved loopback address");
        drop(reserved);
        let server =
            NBDServer::new_tcp(Arc::clone(&filesystem), Arc::clone(&export_gates), address);
        let shutdown = CancellationToken::new();
        let server_shutdown = shutdown.clone();
        let server_task = tokio::spawn(async move { server.start(server_shutdown).await });
        let client = timeout(Duration::from_secs(2), async {
            loop {
                match TcpStream::connect(address).await {
                    Ok(client) => return client,
                    Err(error) if error.kind() == io::ErrorKind::ConnectionRefused => {
                        tokio::task::yield_now().await;
                    }
                    Err(error) => panic!("connect TCP NBD client: {error}"),
                }
            }
        })
        .await
        .expect("TCP NBD listener became ready");

        assert_server_shutdown_waits_for_accepted_commit(
            client,
            shutdown,
            server_task,
            filesystem,
            export_gates,
            apply_reached,
            commit_block,
        )
        .await;
    }

    #[tokio::test]
    async fn shutdown_wins_over_a_prebuffered_transmission_header() {
        let filesystem = Arc::new(
            ZeroFS::new_in_memory()
                .await
                .expect("create test filesystem"),
        );
        let export_gates = Arc::new(NbdExportGates::default());
        let device = single_file_export(&filesystem, &export_gates).await;
        let request = NBDRequest {
            magic: NBD_REQUEST_MAGIC,
            flags: 0,
            cmd_type: NBDCommand::WriteZeroes,
            cookie: 0x9988_7766_5544_3322,
            offset: 0,
            length: 4096,
        }
        .to_bytes()
        .expect("encode prebuffered request");
        let polls = Arc::new(AtomicUsize::new(0));

        for _ in 0..64 {
            let shutdown = CancellationToken::new();
            shutdown.cancel();
            let mut session = NBDSession::new(
                PrebufferedRequest {
                    bytes: request.clone(),
                    position: 0,
                    polls: Arc::clone(&polls),
                },
                tokio::io::sink(),
                Arc::clone(&filesystem),
                Arc::clone(&export_gates),
                shutdown,
            );
            session
                .handle_transmission(device.clone())
                .await
                .expect("canceled transmission stopped cleanly");
        }

        assert_eq!(
            polls.load(Ordering::Relaxed),
            0,
            "a buffered command must not be read or dispatched after shutdown is observable"
        );
    }

    #[tokio::test]
    async fn unadvertised_write_zeroes_returns_einval_on_the_wire() {
        let filesystem = Arc::new(
            ZeroFS::new_in_memory()
                .await
                .expect("create test filesystem"),
        );
        let export_gates = Arc::new(NbdExportGates::default());
        let device = single_file_export(&filesystem, &export_gates).await;
        let (server_stream, mut client_stream) = tokio::io::duplex(1024);
        let (reader, writer) = tokio::io::split(server_stream);
        let mut session = NBDSession::new(
            reader,
            writer,
            filesystem,
            export_gates,
            CancellationToken::new(),
        );
        let session_task = tokio::spawn(async move { session.handle_transmission(device).await });

        let write_zeroes = NBDRequest {
            magic: NBD_REQUEST_MAGIC,
            flags: 0,
            cmd_type: NBDCommand::WriteZeroes,
            cookie: 0x0102_0304_0506_0708,
            offset: 0,
            length: 4096,
        };
        client_stream
            .write_all(&write_zeroes.to_bytes().expect("encode WRITE_ZEROES"))
            .await
            .expect("send WRITE_ZEROES");

        let mut reply_bytes = [0; 16];
        timeout(
            Duration::from_secs(2),
            client_stream.read_exact(&mut reply_bytes),
        )
        .await
        .expect("server replied to WRITE_ZEROES")
        .expect("read WRITE_ZEROES reply");
        let (_, reply) = NBDSimpleReply::from_bytes((&reply_bytes, 0)).expect("decode reply");
        assert_eq!(reply.cookie, write_zeroes.cookie);
        assert_eq!(
            reply.error, NBD_EINVAL,
            "unadvertised WRITE_ZEROES must return EINVAL"
        );

        let disconnect = NBDRequest {
            magic: NBD_REQUEST_MAGIC,
            flags: 0,
            cmd_type: NBDCommand::Disconnect,
            cookie: 0,
            offset: 0,
            length: 0,
        };
        client_stream
            .write_all(&disconnect.to_bytes().expect("encode disconnect"))
            .await
            .expect("send disconnect");
        timeout(Duration::from_secs(2), session_task)
            .await
            .expect("server stopped after disconnect")
            .expect("server task did not panic")
            .expect("server accepted disconnect");
    }

    #[tokio::test]
    async fn flush_waits_for_an_earlier_write_whose_payload_is_still_arriving() {
        let filesystem = Arc::new(
            ZeroFS::new_in_memory()
                .await
                .expect("create test filesystem"),
        );
        let export_gates = Arc::new(NbdExportGates::default());
        let write_device = single_file_export(&filesystem, &export_gates).await;
        let flush_device = NBDHandler::new(Arc::clone(&filesystem), Arc::clone(&export_gates))
            .get_device(b"flush-ordering-test")
            .await
            .expect("open the same export on another connection");

        let (payload_started_tx, payload_started_rx) = oneshot::channel();
        let mut write_session = NBDSession::new(
            BlockingPayload {
                started: Some(payload_started_tx),
            },
            tokio::io::sink(),
            Arc::clone(&filesystem),
            Arc::clone(&export_gates),
            CancellationToken::new(),
        );
        let write_task = tokio::spawn(async move {
            write_session
                .read_write_data(&write_device, 0, 4096, false, write_device.size)
                .await
        });
        payload_started_rx
            .await
            .expect("write reached its blocked payload read");

        let flush_handler = NBDHandler::new(filesystem, export_gates);
        let mut flush_task = tokio::spawn(async move { flush_handler.flush(&flush_device).await });
        assert!(
            timeout(Duration::from_millis(50), &mut flush_task)
                .await
                .is_err(),
            "FLUSH overtook a WRITE whose request was already admitted"
        );

        write_task.abort();
        let _ = write_task.await;
        timeout(Duration::from_secs(2), flush_task)
            .await
            .expect("FLUSH resumed after the earlier write was canceled")
            .expect("FLUSH task did not panic")
            .expect("FLUSH succeeded");
    }

    #[tokio::test]
    async fn shutdown_releases_a_half_delivered_write_for_a_waiting_flush() {
        let filesystem = Arc::new(
            ZeroFS::new_in_memory()
                .await
                .expect("create test filesystem"),
        );
        let export_gates = Arc::new(NbdExportGates::default());
        let write_device = single_file_export(&filesystem, &export_gates).await;
        let flush_device = NBDHandler::new(Arc::clone(&filesystem), Arc::clone(&export_gates))
            .get_device(b"flush-ordering-test")
            .await
            .expect("open the same export on another connection");
        let shutdown = CancellationToken::new();
        let (payload_started_tx, payload_started_rx) = oneshot::channel();
        let mut write_session = NBDSession::new(
            PartialThenBlockingPayload {
                started: Some(payload_started_tx),
                delivered: false,
            },
            tokio::io::sink(),
            Arc::clone(&filesystem),
            Arc::clone(&export_gates),
            shutdown.clone(),
        );
        let write_task = tokio::spawn(async move {
            write_session
                .read_write_data(&write_device, 0, 4096, false, write_device.size)
                .await
        });
        payload_started_rx
            .await
            .expect("write received the first payload fragment");

        let flush_handler = NBDHandler::new(filesystem, export_gates);
        let flush_task = tokio::spawn(async move { flush_handler.flush(&flush_device).await });
        shutdown.cancel();

        assert!(
            timeout(Duration::from_secs(2), write_task)
                .await
                .expect("WRITE stopped after session shutdown")
                .expect("WRITE task did not panic")
                .is_err(),
            "a half-delivered WRITE must fail when its session shuts down"
        );
        timeout(Duration::from_secs(2), flush_task)
            .await
            .expect("FLUSH resumed after the stalled session shut down")
            .expect("FLUSH task did not panic")
            .expect("FLUSH succeeded");
    }

    #[tokio::test]
    async fn payload_timeout_releases_a_stalled_write_for_a_waiting_fua() {
        let filesystem = Arc::new(
            ZeroFS::new_in_memory()
                .await
                .expect("create test filesystem"),
        );
        let export_gates = Arc::new(NbdExportGates::default());
        let write_device = single_file_export(&filesystem, &export_gates).await;
        let fua_device = NBDHandler::new(Arc::clone(&filesystem), Arc::clone(&export_gates))
            .get_device(b"flush-ordering-test")
            .await
            .expect("open the same export on another connection");
        let (payload_started_tx, payload_started_rx) = oneshot::channel();
        let mut write_session = NBDSession::new(
            BlockingPayload {
                started: Some(payload_started_tx),
            },
            tokio::io::sink(),
            Arc::clone(&filesystem),
            Arc::clone(&export_gates),
            CancellationToken::new(),
        );
        let write_task = tokio::spawn(async move {
            write_session
                .read_write_data(&write_device, 0, 4096, false, write_device.size)
                .await
        });
        payload_started_rx
            .await
            .expect("write reached its blocked payload read");

        let fua_handler = NBDHandler::new(filesystem, export_gates);
        let fua_task = tokio::spawn(async move {
            let admission = fua_handler.begin_mutation(&fua_device).await;
            fua_handler
                .write_admitted(
                    &fua_device,
                    0,
                    &Bytes::from(vec![0x33; 4096]),
                    true,
                    admission,
                )
                .await
        });

        assert!(
            timeout(Duration::from_secs(2), write_task)
                .await
                .expect("WRITE stopped after the payload deadline")
                .expect("WRITE task did not panic")
                .is_err(),
            "a WRITE that misses its payload deadline must fail"
        );
        timeout(Duration::from_secs(2), fua_task)
            .await
            .expect("FUA write resumed after the stalled WRITE timed out")
            .expect("FUA task did not panic")
            .expect("FUA write and its flush succeeded");
    }
}
