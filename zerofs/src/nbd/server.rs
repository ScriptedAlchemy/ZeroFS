use super::error::{CommandError, NBDError, Result};
use super::handler::{
    MutationAdmission, NBDDevice, NBDHandler, NbdExportGates, NbdMutationRequest, OptionReply,
    OptionResult,
};
use super::out_of_bounds;
use crate::fs::ZeroFS;
use bytes::BytesMut;
use deku::prelude::*;
use futures::stream::{self, FuturesUnordered, StreamExt};
use nbd_proto::*;
use std::collections::HashSet;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
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

#[derive(Clone, Default)]
struct ActiveHandles {
    handles: Arc<Mutex<HashSet<u64>>>,
}

impl ActiveHandles {
    fn reserve(&self, handle: u64) -> std::result::Result<ActiveHandle, ()> {
        let mut handles = self.handles.lock().expect("active NBD handles poisoned");
        if !handles.insert(handle) {
            return Err(());
        }
        Ok(ActiveHandle {
            handles: Arc::clone(&self.handles),
            handle,
        })
    }
}

struct ActiveHandle {
    handles: Arc<Mutex<HashSet<u64>>>,
    handle: u64,
}

impl Drop for ActiveHandle {
    fn drop(&mut self) {
        self.handles
            .lock()
            .expect("active NBD handles poisoned")
            .remove(&self.handle);
    }
}

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

        self.writer.write_all(&device.size().to_be_bytes()).await?;
        self.writer
            .write_all(&device.transmission_flags().to_be_bytes())
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

    /// Service the transmission phase, overlapping command execution with the
    /// arrival of later commands on the same connection.
    ///
    /// The socket is still read strictly in order — a request's header and, for
    /// a WRITE, its payload arrive as one contiguous run of bytes, so there is
    /// no other way to read them. What used to be serial is *execution*: the
    /// old loop awaited each handler to completion and wrote its reply before
    /// looking at the socket again, so one connection was serviced at queue
    /// depth 1 however deep the client queued. Now a command whose wire bytes
    /// are fully consumed (see [`AdmittedCommand`]) is moved into `inflight`
    /// and runs while the next request is being read.
    ///
    /// Replies leave in completion order rather than submission order, which
    /// the protocol allows: every simple reply carries the request's cookie,
    /// and that is what the client matches on.
    ///
    /// Two kinds of concurrency are new here and both are legal. Commands that
    /// the specification leaves unordered — WRITE against WRITE, TRIM against
    /// WRITE, READ against either — may now execute simultaneously, so a client
    /// that needs one to land before another must separate them with FLUSH or
    /// FUA, exactly as it must against any server that does not serialize.
    /// What has *not* changed is the ordering the specification does impose,
    /// because it never lived in this loop: a WRITE takes its admission guard
    /// (`begin_mutation`) while still being read here, in request order, and
    /// holds it until the write completes, while FLUSH takes the same gate
    /// exclusively. A FLUSH therefore still covers every write admitted before
    /// it, whether that write is mid-payload, queued, or executing.
    async fn handle_transmission(&mut self, device: NBDDevice) -> Result<()> {
        let Self {
            reader,
            writer,
            handler,
            shutdown,
            ..
        } = self;
        let handler = &*handler;
        let shutdown = &*shutdown;
        let device = &device;

        // Commands are capped by count *and* by bytes. A single request may ask
        // for up to `MAX_REQUEST_LENGTH`, so a count-only cap would let one
        // connection pin `MAX_INFLIGHT_COMMANDS * MAX_REQUEST_LENGTH` of
        // payload. Each admitted command holds byte credit from the moment its
        // payload is read until its reply has been written, or deliberately
        // discarded after a terminal writer failure. Reply data is charged the
        // same way, since a queued READ reply is just as resident.
        let budget = Arc::new(tokio::sync::Semaphore::new(MAX_INFLIGHT_BYTES));
        let active_handles = ActiveHandles::default();
        let (reply_tx, reply_rx) = tokio::sync::mpsc::channel::<Reply>(MAX_INFLIGHT_COMMANDS);

        // Streams rather than futures rebuilt each iteration, for two reasons.
        // Each borrows its half of the socket exactly once, and — because
        // `unfold` parks its in-progress future inside the stream — a
        // `select!` branch that loses the race drops only the `next()` handle,
        // never a partially completed read or write. Neither `read_exact` nor
        // `write_all` is cancellation-safe.
        let mut commands = Box::pin(stream::unfold((reader, false), move |(reader, stop)| {
            let budget = Arc::clone(&budget);
            let active_handles = active_handles.clone();
            async move {
                if stop {
                    return None;
                }
                match next_admitted(reader, handler, device, shutdown, &budget, &active_handles)
                    .await
                {
                    Ok(None) => None,
                    Ok(Some(command)) => Some((Ok(command), (reader, false))),
                    // Surface the failure, then stop reading this session.
                    Err(e) => Some((Err(e), (reader, true))),
                }
            }
        }));
        // Replies drain here rather than inline in the loop body. Blocking the
        // loop on a socket write would stop `inflight` from being polled, and
        // an unpolled write cannot reach the `drop(admission)` inside
        // `write_admitted` — so a client that stopped reading would pin
        // admission guards on a gate shared by every connection to the export,
        // stalling `flush` and `begin_mutation` fleet-wide.
        let mut replies = Box::pin(stream::unfold(
            (writer, reply_rx, false),
            |(writer, mut reply_rx, writer_failed)| async move {
                let (cookie, result, budget, active_handle) = reply_rx.recv().await?;
                let outcome = if writer_failed {
                    // The outbound half is irrecoverable. Continue draining
                    // completed commands so their byte credit and admission
                    // ownership are released, but never touch it again.
                    Ok(())
                } else {
                    write_simple_reply(writer, cookie, result).await
                };
                // Held until the reply is on the wire, not merely produced.
                drop(budget);
                drop(active_handle);
                let writer_failed = writer_failed || outcome.is_err();
                Some((outcome, (writer, reply_rx, writer_failed)))
            },
        ));
        let mut inflight = FuturesUnordered::new();
        let mut accepting = true;
        let mut unwritten = 0usize;
        let mut terminal_error = None;

        loop {
            // Checked before the stream is polled, so a session that is already
            // shutting down never touches the socket.
            if accepting && shutdown.is_cancelled() {
                debug!("NBD client handler shutting down");
                accepting = false;
            }
            let reading = accepting && inflight.len() + unwritten < MAX_INFLIGHT_COMMANDS;
            if !reading && inflight.is_empty() && unwritten == 0 {
                return match terminal_error {
                    Some(error) => Err(error),
                    None => Ok(()),
                };
            }

            tokio::select! {
                biased;
                Some(result) = replies.next(), if unwritten > 0 => {
                    unwritten -= 1;
                    if let Err(error) = result {
                        accepting = false;
                        terminal_error.get_or_insert(error);
                    }
                }
                Some(reply) = inflight.next(), if !inflight.is_empty() => {
                    unwritten += 1;
                    // Capacity equals the in-flight cap and every admitted
                    // command yields exactly one reply, so this cannot be full.
                    assert!(
                        reply_tx.try_send(reply).is_ok(),
                        "reply channel is sized for the in-flight cap",
                    );
                }
                next = commands.next(), if reading => {
                    match next {
                        // Disconnect, clean EOF, or shutdown observed mid-read.
                        // Stop accepting, then drain what was already admitted.
                        None => accepting = false,
                        Some(Err(error)) => {
                            accepting = false;
                            terminal_error.get_or_insert(error);
                        }
                        Some(Ok((cookie, command, budget, active_handle))) => inflight.push(async move {
                            (
                                cookie,
                                run_admitted(handler, device, command).await,
                                budget,
                                active_handle,
                            )
                        }),
                    }
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

    /// Read a WRITE's payload off the stream and run it to completion, the way
    /// the transmission loop does in two stages. Lets a test drive one write
    /// against a controlled reader without standing up the whole loop.
    #[cfg(test)]
    async fn read_write_data(
        &mut self,
        device: &NBDDevice,
        offset: u64,
        length: u32,
        fua: bool,
    ) -> super::error::CommandResult<()> {
        match admit_write(
            &mut self.reader,
            &self.handler,
            device,
            offset,
            length,
            fua,
            &self.shutdown,
        )
        .await?
        {
            None => Ok(()),
            Some((data, admission)) => {
                self.handler
                    .write_admitted(device, offset, data, fua, admission)
                    .await
            }
        }
    }
}

/// In-flight commands allowed per connection: deep enough to keep the storage
/// engine busy while a client's queue drains. Memory is bounded by
/// [`MAX_INFLIGHT_BYTES`], not by this — a request may be as large as
/// [`MAX_REQUEST_LENGTH`], so a count alone would bound nothing useful.
const MAX_INFLIGHT_COMMANDS: usize = 32;

/// Payload and reply bytes one connection may hold at once. Credit is taken
/// before a WRITE's payload is read (or a READ is dispatched) and returned only
/// once the reply has been written or deliberately discarded after a terminal
/// writer failure, so it covers both directions. A request larger than the
/// whole budget is clamped to it, which keeps a single oversized command
/// admissible instead of deadlocking against its own cap.
const MAX_INFLIGHT_BYTES: usize = 64 * 1024 * 1024;

/// A finished command waiting to go out: cookie, result, and the byte credit it
/// still owes until the reply is written or terminally discarded.
type Reply = (
    u64,
    super::error::CommandResult<bytes::Bytes>,
    tokio::sync::OwnedSemaphorePermit,
    Option<ActiveHandle>,
);

/// Byte credit a command should hold while in flight, clamped so one oversized
/// request can still be admitted on its own.
fn budget_cost(length: u32) -> u32 {
    length.min(MAX_INFLIGHT_BYTES as u32)
}

/// A transmission command that owes the socket nothing further: a WRITE already
/// carries its payload and its admission guard, an oversized or malformed
/// request has already had its body discarded. Running one therefore needs only
/// the handler, which is what lets it overlap with the next request's arrival.
#[allow(clippy::large_enum_variant)]
enum AdmittedCommand {
    Read {
        offset: u64,
        length: u32,
    },
    Write {
        offset: u64,
        data: bytes::Bytes,
        fua: bool,
        admission: MutationAdmission,
    },
    Flush,
    Trim {
        offset: u64,
        length: u32,
        fua: bool,
    },
    Cache {
        offset: u64,
        length: u32,
    },
    /// Resolved while reading: a zero-length write, or a rejected command.
    Settled(super::error::CommandResult<()>),
}

/// Take a WRITE's admission and pull its payload off the wire.
///
/// Admission is acquired before the payload arrives, so a FLUSH cannot overtake
/// a request that has already been accepted. `Ok(None)` is a zero-length write:
/// nothing to admit and nothing to read.
async fn admit_write<R>(
    reader: &mut R,
    handler: &NBDHandler,
    device: &NBDDevice,
    offset: u64,
    length: u32,
    fua: bool,
    shutdown: &CancellationToken,
) -> super::error::CommandResult<Option<(bytes::Bytes, MutationAdmission)>>
where
    R: AsyncRead + Unpin,
{
    // Consume an invalid write's payload to keep the request stream aligned.
    if out_of_bounds(offset, length, device.size()) {
        discard_write_payload(reader, length, shutdown).await?;
        return Err(CommandError::NoSpace);
    }

    if length == 0 {
        return Ok(None);
    }

    let begin = handler.begin_mutation(
        device,
        NbdMutationRequest {
            offset,
            length: length as usize,
            fua,
        },
    );
    let admission = match tokio::select! {
        biased;
        _ = shutdown.cancelled() => return Err(CommandError::IoError),
        result = begin => result,
    } {
        Ok(admission) => admission,
        Err(error) => {
            discard_write_payload(reader, length, shutdown).await?;
            return Err(error);
        }
    };
    let mut data = BytesMut::with_capacity(length as usize);
    tokio::select! {
        _ = shutdown.cancelled() => return Err(CommandError::IoError),
        result = tokio::time::timeout(
            WRITE_PAYLOAD_TIMEOUT,
            read_write_payload(reader, &mut data, length),
        ) => {
            match result {
                Ok(read) => {
                    if read.is_err() {
                        // Any short/failed payload read destroys request
                        // framing just like a timeout. Never parse its tail as
                        // another NBD header.
                        shutdown.cancel();
                        return Err(CommandError::IoError);
                    }
                }
                Err(_) => {
                    // The remainder of a timed-out payload cannot be
                    // distinguished from a later request. Close this session
                    // after returning EIO rather than continuing on a
                    // desynchronized transmission stream.
                    shutdown.cancel();
                    return Err(CommandError::IoError);
                }
            }
        }
    }

    Ok(Some((data.freeze(), admission)))
}

/// Append exactly `length` payload bytes to `data`, filling the buffer's spare
/// capacity directly instead of pre-zeroing bytes the payload overwrites.
/// The reader is limited to the payload so no byte of the next request header
/// is consumed, and a short stream reports `UnexpectedEof`, the same error
/// `read_exact` produced here.
async fn read_write_payload<R>(
    reader: &mut R,
    data: &mut BytesMut,
    length: u32,
) -> std::io::Result<()>
where
    R: AsyncRead + Unpin,
{
    let length = length as usize;
    let mut payload = reader.take(length as u64);
    while data.len() < length {
        if payload.read_buf(data).await? == 0 {
            return Err(std::io::Error::from(std::io::ErrorKind::UnexpectedEof));
        }
    }
    Ok(())
}

async fn discard_write_payload<R>(
    reader: &mut R,
    length: u32,
    shutdown: &CancellationToken,
) -> super::error::CommandResult<()>
where
    R: AsyncRead + Unpin,
{
    let discard = async {
        let mut remaining = length as usize;
        let mut buffer = vec![0; remaining.min(DISCARD_CHUNK_SIZE)];
        while remaining > 0 {
            let chunk = remaining.min(buffer.len());
            reader
                .read_exact(&mut buffer[..chunk])
                .await
                .map_err(|_| CommandError::IoError)?;
            remaining -= chunk;
        }
        Ok(())
    };
    let result = tokio::select! {
        biased;
        _ = shutdown.cancelled() => Err(CommandError::IoError),
        result = tokio::time::timeout(WRITE_PAYLOAD_TIMEOUT, discard) => {
            result.unwrap_or(Err(CommandError::IoError))
        }
    };
    if result.is_err() {
        // A partial discard leaves the next request boundary unknowable. The
        // only safe recovery is to terminate this client session.
        shutdown.cancel();
    }
    result
}

/// Read the next request, consuming everything it owes the stream, and return
/// it ready to run. `Ok(None)` means stop reading: the client disconnected, or
/// shutdown became observable before the header arrived.
async fn next_admitted<R>(
    reader: &mut R,
    handler: &NBDHandler,
    device: &NBDDevice,
    shutdown: &CancellationToken,
    budget: &Arc<tokio::sync::Semaphore>,
    active_handles: &ActiveHandles,
) -> Result<
    Option<(
        u64,
        AdmittedCommand,
        tokio::sync::OwnedSemaphorePermit,
        Option<ActiveHandle>,
    )>,
>
where
    R: AsyncRead + Unpin,
{
    let mut request_buf = [0u8; NBD_REQUEST_HEADER_SIZE];
    tokio::select! {
        biased;
        _ = shutdown.cancelled() => {
            debug!("NBD client handler shutting down");
            return Ok(None);
        }
        result = reader.read_exact(&mut request_buf) => {
            if let Err(e) = result {
                // A client that closes or resets between requests has simply
                // gone away. Treat it exactly like NBD_CMD_DISC: stop reading
                // and let already-admitted commands finish, rather than
                // abandoning up to `MAX_INFLIGHT_COMMANDS` writes — which for a
                // striped export could tear a single logical write across its
                // members mid-`try_join_all`.
                return match e.kind() {
                    std::io::ErrorKind::UnexpectedEof
                    | std::io::ErrorKind::ConnectionReset
                    | std::io::ErrorKind::BrokenPipe => {
                        debug!("NBD client went away: {:?}", e.kind());
                        Ok(None)
                    }
                    _ => Err(e.into()),
                };
            }
        }
    }

    let request = NBDRequest::from_bytes((&request_buf, 0))
        .map_err(|e| NBDError::Protocol(format!("Invalid request: {e}")))?
        .1;

    debug!(
        "NBD command: {:?}, offset={}, length={}",
        request.cmd_type, request.offset, request.length
    );

    let active_handle = match active_handles.reserve(request.cookie) {
        Ok(active_handle) => active_handle,
        Err(()) => {
            if request.cmd_type == NBDCommand::Write {
                discard_write_payload(reader, request.length, shutdown)
                    .await
                    .map_err(|_| {
                        NBDError::Protocol(
                            "failed to drain colliding NBD WRITE payload".to_string(),
                        )
                    })?;
            }
            let credit = Arc::clone(budget)
                .acquire_many_owned(0)
                .await
                .map_err(|_| NBDError::Protocol("connection byte budget closed".into()))?;
            return Ok(Some((
                request.cookie,
                AdmittedCommand::Settled(Err(CommandError::InvalidArgument)),
                credit,
                None,
            )));
        }
    };

    // Charged before any payload is read and released only once the reply is
    // written, so both the request body and the reply body are covered. A
    // command that carries neither costs nothing but is still count-capped.
    let cost = match request.cmd_type {
        NBDCommand::Read | NBDCommand::Write => budget_cost(request.length),
        _ => 0,
    };
    let credit = Arc::clone(budget)
        .acquire_many_owned(cost)
        .await
        .map_err(|_| NBDError::Protocol("connection byte budget closed".into()))?;

    if request.length > MAX_REQUEST_LENGTH {
        if request.cmd_type == NBDCommand::Write {
            return Err(NBDError::Protocol(format!(
                "write length {} exceeds max {MAX_REQUEST_LENGTH}",
                request.length
            )));
        }
        return Ok(Some((
            request.cookie,
            AdmittedCommand::Settled(Err(CommandError::InvalidArgument)),
            credit,
            Some(active_handle),
        )));
    }

    let fua = (request.flags & NBD_CMD_FLAG_FUA) != 0;
    let command = match request.cmd_type {
        NBDCommand::Read => AdmittedCommand::Read {
            offset: request.offset,
            length: request.length,
        },
        NBDCommand::Write => {
            match admit_write(
                reader,
                handler,
                device,
                request.offset,
                request.length,
                fua,
                shutdown,
            )
            .await
            {
                Ok(Some((data, admission))) => AdmittedCommand::Write {
                    offset: request.offset,
                    data,
                    fua,
                    admission,
                },
                Ok(None) => AdmittedCommand::Settled(Ok(())),
                Err(e) => AdmittedCommand::Settled(Err(e)),
            }
        }
        NBDCommand::Disconnect => {
            info!("Client disconnecting");
            return Ok(None);
        }
        NBDCommand::Flush => AdmittedCommand::Flush,
        NBDCommand::Trim => AdmittedCommand::Trim {
            offset: request.offset,
            length: request.length,
            fua,
        },
        NBDCommand::WriteZeroes => AdmittedCommand::Settled(Err(CommandError::InvalidArgument)),
        NBDCommand::Cache => AdmittedCommand::Cache {
            offset: request.offset,
            length: request.length,
        },
        NBDCommand::Unknown(cmd) => {
            warn!("Unknown NBD command: {}", cmd);
            AdmittedCommand::Settled(Err(CommandError::InvalidArgument))
        }
    };
    Ok(Some((request.cookie, command, credit, Some(active_handle))))
}

/// Run an already-admitted command. Touches no socket, so several may be in
/// flight at once on one connection.
async fn run_admitted(
    handler: &NBDHandler,
    device: &NBDDevice,
    command: AdmittedCommand,
) -> super::error::CommandResult<bytes::Bytes> {
    let empty = bytes::Bytes::new();
    match command {
        AdmittedCommand::Read { offset, length } => handler.read(device, offset, length).await,
        AdmittedCommand::Write {
            offset,
            data,
            fua,
            admission,
        } => handler
            .write_admitted(device, offset, data, fua, admission)
            .await
            .map(|()| empty),
        AdmittedCommand::Flush => handler.flush(device).await.map(|()| empty),
        AdmittedCommand::Trim {
            offset,
            length,
            fua,
        } => handler
            .trim(device, offset, length, fua)
            .await
            .map(|()| empty),
        AdmittedCommand::Cache { offset, length } => {
            handler.cache(device, offset, length).await.map(|()| empty)
        }
        AdmittedCommand::Settled(result) => result.map(|()| empty),
    }
}

/// Write one simple reply. The caller owns failure policy because the socket's
/// read half may remain healthy after its write half is irrecoverably broken.
async fn write_simple_reply<W>(
    writer: &mut W,
    cookie: u64,
    result: super::error::CommandResult<bytes::Bytes>,
) -> Result<()>
where
    W: AsyncWrite + Unpin,
{
    let (error, data) = match result {
        Ok(data) => (NBD_SUCCESS, data),
        Err(e) => (e.to_errno(), bytes::Bytes::new()),
    };
    let reply_bytes = NBDSimpleReply::new(cookie, error).to_bytes()?;
    writer.write_all(&reply_bytes).await?;
    if !data.is_empty() {
        writer.write_all(&data).await?;
    }
    writer.flush().await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{
        CommandError, MAX_INFLIGHT_BYTES, MAX_REQUEST_LENGTH, NBDServer, NBDSession, admit_write,
        budget_cost,
    };
    use crate::fs::ZeroFS;
    use crate::fs::permissions::Credentials;
    use crate::fs::types::{SetAttributes, SetSize};
    use crate::nbd::handler::{NBDHandler, NbdExportGates, NbdMutationRequest};
    use bytes::Bytes;
    use deku::{DekuContainerRead, DekuContainerWrite};
    use nbd_proto::{
        NBD_CMD_FLAG_FUA, NBD_EINVAL, NBD_FLAG_C_FIXED_NEWSTYLE, NBD_FLAG_C_NO_ZEROES,
        NBD_IHAVEOPT, NBD_OPT_EXPORT_NAME, NBD_REQUEST_HEADER_SIZE, NBD_REQUEST_MAGIC, NBDCommand,
        NBDRequest, NBDSimpleReply,
    };
    use std::io;
    use std::pin::Pin;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
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
        /// Bytes actually consumed by the session, so a test can see the read
        /// side stop when admission runs out of byte credit.
        delivered: Arc<AtomicUsize>,
    }

    /// A writer that never accepts a byte, standing in for a client that
    /// stopped reading its replies while continuing to hold the connection.
    struct StalledWriter {
        stalled: Option<oneshot::Sender<()>>,
    }

    /// Supplies one complete request, then exposes whether the transmission
    /// loop tries to consume any later request after its reply writer fails.
    struct RequestThenWriterFailure {
        first: Vec<u8>,
        tail: Vec<u8>,
        position: usize,
        writer_failed: Arc<AtomicBool>,
        tail_delivered: Arc<AtomicUsize>,
    }

    struct FailingWriter {
        failed: Arc<AtomicBool>,
        polls: Arc<AtomicUsize>,
    }

    struct RequestThenReadError {
        bytes: Vec<u8>,
        position: usize,
        kind: io::ErrorKind,
    }

    impl tokio::io::AsyncWrite for StalledWriter {
        fn poll_write(
            mut self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            _buf: &[u8],
        ) -> Poll<io::Result<usize>> {
            if let Some(stalled) = self.stalled.take() {
                let _ = stalled.send(());
            }
            Poll::Pending
        }

        fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Pending
        }

        fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }

    impl tokio::io::AsyncWrite for FailingWriter {
        fn poll_write(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            _buf: &[u8],
        ) -> Poll<io::Result<usize>> {
            self.polls.fetch_add(1, Ordering::Relaxed);
            self.failed.store(true, Ordering::Release);
            Poll::Ready(Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "injected reply failure",
            )))
        }

        fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }

        fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }
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
            self.delivered.fetch_add(length, Ordering::Relaxed);
            Poll::Ready(Ok(()))
        }
    }

    impl AsyncRead for RequestThenWriterFailure {
        fn poll_read(
            mut self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            buf: &mut ReadBuf<'_>,
        ) -> Poll<io::Result<()>> {
            if self.position < self.first.len() {
                let remaining = &self.first[self.position..];
                let length = remaining.len().min(buf.remaining());
                buf.put_slice(&remaining[..length]);
                self.position += length;
                return Poll::Ready(Ok(()));
            }
            if !self.writer_failed.load(Ordering::Acquire) {
                return Poll::Pending;
            }
            let tail_position = self.position - self.first.len();
            let remaining = &self.tail[tail_position..];
            let length = remaining.len().min(buf.remaining());
            buf.put_slice(&remaining[..length]);
            self.position += length;
            self.tail_delivered.fetch_add(length, Ordering::Relaxed);
            Poll::Ready(Ok(()))
        }
    }

    impl AsyncRead for RequestThenReadError {
        fn poll_read(
            mut self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            buf: &mut ReadBuf<'_>,
        ) -> Poll<io::Result<()>> {
            if self.position < self.bytes.len() {
                let remaining = &self.bytes[self.position..];
                let length = remaining.len().min(buf.remaining());
                buf.put_slice(&remaining[..length]);
                self.position += length;
                return Poll::Ready(Ok(()));
            }
            Poll::Ready(Err(io::Error::new(self.kind, "injected read failure")))
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
                    delivered: Arc::new(AtomicUsize::new(0)),
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
                .read_write_data(&write_device, 0, 4096, false)
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
                .read_write_data(&write_device, 0, 4096, false)
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
                .read_write_data(&write_device, 0, 4096, false)
                .await
        });
        payload_started_rx
            .await
            .expect("write reached its blocked payload read");

        let fua_handler = NBDHandler::new(filesystem, export_gates);
        let fua_task = tokio::spawn(async move {
            let payload = Bytes::from(vec![0x33; 4096]);
            let admission = fua_handler
                .begin_mutation(
                    &fua_device,
                    NbdMutationRequest {
                        offset: 0,
                        length: payload.len(),
                        fua: true,
                    },
                )
                .await?;
            fua_handler
                .write_admitted(&fua_device, 0, payload, true, admission)
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

    /// A single-file export of an arbitrary size, for throughput probes that need
    /// more room than [`single_file_export`]'s 4 KiB.
    async fn sized_single_file_export(
        filesystem: &Arc<ZeroFS>,
        name: &[u8],
        size: u64,
    ) -> crate::fs::inode::InodeId {
        let credentials = root_credentials();
        let nbd_dir = match filesystem
            .mkdir(&credentials, 0, b".nbd", &SetAttributes::default())
            .await
        {
            Ok((id, _)) => id,
            // Already created by an earlier export in the same filesystem.
            Err(_) => filesystem
                .lookup(&credentials, 0, b".nbd")
                .await
                .expect("locate .nbd directory"),
        };
        let (inode, _) = filesystem
            .create(&credentials, nbd_dir, name, &SetAttributes::default())
            .await
            .expect("create export");
        filesystem
            .setattr(
                &credentials,
                inode,
                &SetAttributes {
                    size: SetSize::Set(size),
                    ..Default::default()
                },
            )
            .await
            .expect("size export");
        inode
    }

    fn probe_request(cmd: NBDCommand, cookie: u64, offset: u64, length: u32) -> Vec<u8> {
        NBDRequest {
            magic: NBD_REQUEST_MAGIC,
            flags: 0,
            cmd_type: cmd,
            cookie,
            offset,
            length,
        }
        .to_bytes()
        .expect("encode probe request")
    }

    /// Run one `handle_transmission` over a loopback TCP socket, mirroring
    /// `handle_client_stream`'s split and buffering so the measured path is the
    /// production one rather than an in-memory shortcut.
    async fn transmission_over_tcp(
        filesystem: Arc<ZeroFS>,
        export_gates: Arc<NbdExportGates>,
        name: &[u8],
    ) -> (TcpStream, tokio::task::JoinHandle<()>) {
        let device = NBDHandler::new(Arc::clone(&filesystem), Arc::clone(&export_gates))
            .get_device(name)
            .await
            .expect("discover probe export");
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind loopback");
        let addr = listener.local_addr().expect("loopback address");
        let client = TcpStream::connect(addr).await.expect("connect loopback");
        let (server, _) = listener.accept().await.expect("accept loopback");
        client.set_nodelay(true).expect("client nodelay");
        server.set_nodelay(true).expect("server nodelay");
        let task = tokio::spawn(async move {
            let (reader, writer) = tokio::io::split(server);
            let mut session = NBDSession::new(
                tokio::io::BufReader::new(reader),
                tokio::io::BufWriter::new(writer),
                filesystem,
                export_gates,
                CancellationToken::new(),
            );
            let _ = session.handle_transmission(device).await;
        });
        (client, task)
    }

    /// Protocol-floor decomposition for the NBD write ACK path.
    ///
    /// Separates three costs that the end-to-end fio number folds together:
    ///
    ///   1. What the server spends per request with **zero** storage work
    ///      (header parse, dispatch, 16-byte reply, socket write + flush). An
    ///      unadvertised WRITE_ZEROES is rejected before it touches the
    ///      filesystem, so it isolates exactly this.
    ///   2. What a real WRITE adds on top: payload transfer plus the storage
    ///      engine's staging and commit.
    ///   3. What one TCP connection does as its queue deepens, and what
    ///      several connections do in aggregate. The transmission loop
    ///      overlaps execution, so depth on a single connection buys real
    ///      concurrency; the barrier-free and FUA arms bracket that. FUA may
    ///      still overlap pre-flush write work, but its exclusive flush gate
    ///      serializes durability cutoffs and can cover later admitted writes.
    ///
    /// Anything the fio round trip costs beyond (1)+(2) is the kernel NBD
    /// driver, the block layer, XFS, and fio itself — none of it addressable by
    /// server-side work.
    ///
    ///   cargo test --release --lib -- --ignored --nocapture bench_nbd_protocol_floor
    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    #[ignore = "protocol measurement, run explicitly in release"]
    async fn bench_nbd_protocol_floor() {
        const EXPORT_BYTES: u64 = 256 * 1024 * 1024;
        let filesystem = Arc::new(
            ZeroFS::new_in_memory()
                .await
                .expect("create probe filesystem"),
        );
        let export_gates = Arc::new(NbdExportGates::default());
        sized_single_file_export(&filesystem, b"floor-probe", EXPORT_BYTES).await;
        let (mut client, task) = transmission_over_tcp(
            Arc::clone(&filesystem),
            Arc::clone(&export_gates),
            b"floor-probe",
        )
        .await;

        // (1) No-op round trip: the pure protocol + loopback TCP floor.
        let mut reply = [0u8; 16];
        let noop_iterations = 4000u64;
        for i in 0..200 {
            client
                .write_all(&probe_request(NBDCommand::WriteZeroes, i, 0, 4096))
                .await
                .expect("warm no-op");
            client
                .read_exact(&mut reply)
                .await
                .expect("warm no-op reply");
        }
        let start = std::time::Instant::now();
        for i in 0..noop_iterations {
            client
                .write_all(&probe_request(NBDCommand::WriteZeroes, i, 0, 4096))
                .await
                .expect("send no-op");
            client
                .read_exact(&mut reply)
                .await
                .expect("read no-op reply");
        }
        let noop_qd1_us = start.elapsed().as_secs_f64() * 1e6 / noop_iterations as f64;

        // (1b) The same no-op pipelined, so the per-request cost excludes the
        // round-trip stall and shows the server loop's own dispatch cost.
        let depth = 64u64;
        let rounds = 200u64;
        let start = std::time::Instant::now();
        for round in 0..rounds {
            let mut batch = Vec::with_capacity(depth as usize * NBD_REQUEST_HEADER_SIZE);
            for i in 0..depth {
                batch.extend_from_slice(&probe_request(
                    NBDCommand::WriteZeroes,
                    round * depth + i,
                    0,
                    4096,
                ));
            }
            client.write_all(&batch).await.expect("send no-op batch");
            let mut replies = vec![0u8; depth as usize * 16];
            client
                .read_exact(&mut replies)
                .await
                .expect("read no-op batch replies");
        }
        let noop_pipelined_us = start.elapsed().as_secs_f64() * 1e6 / (rounds * depth) as f64;

        // (2) Real writes at QD1, in place, at the canonical NBD request sizes.
        let mut write_qd1 = Vec::new();
        for size in [256 * 1024usize, 1024 * 1024] {
            let payload = vec![0xa5u8; size];
            let iterations = (32 * 1024 * 1024 / size) as u64;
            let slots = EXPORT_BYTES / size as u64;
            for i in 0..8 {
                let offset = (i % slots) * size as u64;
                client
                    .write_all(&probe_request(NBDCommand::Write, i, offset, size as u32))
                    .await
                    .expect("warm write header");
                client
                    .write_all(&payload)
                    .await
                    .expect("warm write payload");
                client
                    .read_exact(&mut reply)
                    .await
                    .expect("warm write reply");
            }
            let start = std::time::Instant::now();
            for i in 0..iterations {
                let offset = (i % slots) * size as u64;
                client
                    .write_all(&probe_request(NBDCommand::Write, i, offset, size as u32))
                    .await
                    .expect("send write header");
                client
                    .write_all(&payload)
                    .await
                    .expect("send write payload");
                client
                    .read_exact(&mut reply)
                    .await
                    .expect("read write reply");
            }
            let elapsed = start.elapsed().as_secs_f64();
            write_qd1.push((
                size,
                elapsed * 1e6 / iterations as f64,
                (iterations as usize * size) as f64 / elapsed / 1e6,
            ));
        }

        eprintln!(
            "nbd protocol floor: no-op QD1 {noop_qd1_us:.1} us/req, \
             no-op pipelined(depth {depth}) {noop_pipelined_us:.1} us/req"
        );
        for (size, us, mbps) in &write_qd1 {
            eprintln!(
                "nbd write QD1 {} KiB: {us:.0} us/req, {mbps:.0} MB/s single connection",
                size / 1024,
            );
        }

        // (2b) The same writes queued on ONE connection at increasing depth.
        // Before the loop overlapped execution every depth here collapsed onto
        // the QD1 number above, because one command was read, run, and replied
        // to before the socket was looked at again.
        for queue_depth in [1usize, 4, 16, 32] {
            let size = 256 * 1024usize;
            let iterations = (32 * 1024 * 1024 / size) as u64;
            let slots = EXPORT_BYTES / size as u64;
            let name = format!("floor-pipe-{queue_depth}");
            sized_single_file_export(&filesystem, name.as_bytes(), EXPORT_BYTES).await;
            let (stream, session) = transmission_over_tcp(
                Arc::clone(&filesystem),
                Arc::clone(&export_gates),
                name.as_bytes(),
            )
            .await;
            let (mut rx, mut tx) = tokio::io::split(stream);
            // Bounds how many requests are outstanding, so `queue_depth` is the
            // real in-flight count rather than whatever the socket buffers.
            let credits = Arc::new(tokio::sync::Semaphore::new(queue_depth));
            let start = std::time::Instant::now();
            let sender = {
                let credits = Arc::clone(&credits);
                tokio::spawn(async move {
                    let payload = vec![0xa5u8; size];
                    for i in 0..iterations {
                        let permit = Arc::clone(&credits)
                            .acquire_owned()
                            .await
                            .expect("queue credit");
                        tx.write_all(&probe_request(
                            NBDCommand::Write,
                            i,
                            (i % slots) * size as u64,
                            size as u32,
                        ))
                        .await
                        .expect("send pipelined header");
                        tx.write_all(&payload)
                            .await
                            .expect("send pipelined payload");
                        permit.forget();
                    }
                    tx
                })
            };
            let mut reply = [0u8; 16];
            for _ in 0..iterations {
                rx.read_exact(&mut reply)
                    .await
                    .expect("read pipelined reply");
                credits.add_permits(1);
            }
            let elapsed = start.elapsed().as_secs_f64();
            let mut tx = sender.await.expect("pipelined sender finished");
            eprintln!(
                "nbd write 256 KiB one connection at depth {queue_depth:>2}: {:.0} MB/s",
                (iterations as usize * size) as f64 / elapsed / 1e6,
            );
            tx.write_all(&probe_request(NBDCommand::Disconnect, 0, 0, 0))
                .await
                .expect("send disconnect");
            let _ = timeout(Duration::from_secs(5), session).await;
        }

        // (2c) The same depth sweep with every write carrying FUA. A FUA write
        // is acknowledged only once its exclusive flush completes. Later
        // writes may already have acquired shared admission and can therefore
        // overlap before the flush gate, or be swept into that cutoff; this arm
        // measures the actual implementation instead of assuming depth is
        // useless. Use enough requests to fill even the QD32 case repeatedly.
        for queue_depth in [1usize, 4, 32] {
            let size = 256 * 1024usize;
            let iterations = (32 * 1024 * 1024 / size) as u64;
            let slots = EXPORT_BYTES / size as u64;
            let name = format!("floor-fua-{queue_depth}");
            sized_single_file_export(&filesystem, name.as_bytes(), EXPORT_BYTES).await;
            let (stream, session) = transmission_over_tcp(
                Arc::clone(&filesystem),
                Arc::clone(&export_gates),
                name.as_bytes(),
            )
            .await;
            let (mut rx, mut tx) = tokio::io::split(stream);
            let credits = Arc::new(tokio::sync::Semaphore::new(queue_depth));
            let start = std::time::Instant::now();
            let sender = {
                let credits = Arc::clone(&credits);
                tokio::spawn(async move {
                    let payload = vec![0xa5u8; size];
                    for i in 0..iterations {
                        let permit = Arc::clone(&credits)
                            .acquire_owned()
                            .await
                            .expect("queue credit");
                        let mut request = NBDRequest {
                            magic: NBD_REQUEST_MAGIC,
                            flags: NBD_CMD_FLAG_FUA,
                            cmd_type: NBDCommand::Write,
                            cookie: i,
                            offset: (i % slots) * size as u64,
                            length: size as u32,
                        }
                        .to_bytes()
                        .expect("encode FUA write");
                        request.truncate(NBD_REQUEST_HEADER_SIZE);
                        tx.write_all(&request).await.expect("send FUA header");
                        tx.write_all(&payload).await.expect("send FUA payload");
                        permit.forget();
                    }
                    tx
                })
            };
            let mut reply = [0u8; 16];
            for _ in 0..iterations {
                rx.read_exact(&mut reply).await.expect("read FUA reply");
                credits.add_permits(1);
            }
            let elapsed = start.elapsed().as_secs_f64();
            let mut tx = sender.await.expect("FUA sender finished");
            eprintln!(
                "nbd FUA write 256 KiB one connection at depth {queue_depth:>2}: {:.0} MB/s",
                (iterations as usize * size) as f64 / elapsed / 1e6,
            );
            tx.write_all(&probe_request(NBDCommand::Disconnect, 0, 0, 0))
                .await
                .expect("send disconnect");
            let _ = timeout(Duration::from_secs(5), session).await;
        }

        // (3) Aggregate across independent connections.
        for connections in [1usize, 2, 4, 8] {
            let size = 256 * 1024usize;
            let per_connection = (16 * 1024 * 1024 / size) as u64;
            let mut sessions = Vec::with_capacity(connections);
            for c in 0..connections {
                let name = format!("floor-probe-{connections}-{c}");
                sized_single_file_export(&filesystem, name.as_bytes(), EXPORT_BYTES).await;
                sessions.push(
                    transmission_over_tcp(
                        Arc::clone(&filesystem),
                        Arc::clone(&export_gates),
                        name.as_bytes(),
                    )
                    .await,
                );
            }
            let start = std::time::Instant::now();
            let mut tasks = Vec::with_capacity(connections);
            for (mut stream, task) in sessions {
                tasks.push((
                    tokio::spawn(async move {
                        let payload = vec![0xa5u8; size];
                        let mut reply = [0u8; 16];
                        for i in 0..per_connection {
                            stream
                                .write_all(&probe_request(
                                    NBDCommand::Write,
                                    i,
                                    i * size as u64,
                                    size as u32,
                                ))
                                .await
                                .expect("send scaling write header");
                            stream
                                .write_all(&payload)
                                .await
                                .expect("send scaling write payload");
                            stream
                                .read_exact(&mut reply)
                                .await
                                .expect("read scaling write reply");
                        }
                        stream
                    }),
                    task,
                ));
            }
            for (client_task, _) in tasks {
                client_task.await.expect("scaling client finished");
            }
            let elapsed = start.elapsed().as_secs_f64();
            eprintln!(
                "nbd write 256 KiB across {connections} connections: {:.0} MB/s aggregate",
                (connections as u64 * per_connection) as f64 * size as f64 / elapsed / 1e6,
            );
        }

        client
            .write_all(&probe_request(NBDCommand::Disconnect, 0, 0, 0))
            .await
            .expect("send disconnect");
        let _ = timeout(Duration::from_secs(5), task).await;
    }

    /// Queued commands on one connection execute concurrently rather than one
    /// after another. A READ of an unwritten region resolves without touching
    /// the object store, so if the loop still serialized, the second request
    /// could not be answered until the first write had finished; here both
    /// replies are outstanding at once.
    ///
    /// The observable proof is a reply arriving out of submission order, which
    /// only a loop with more than one command in flight can produce.
    #[tokio::test]
    async fn one_connection_runs_queued_commands_concurrently() {
        let filesystem = Arc::new(
            ZeroFS::new_in_memory()
                .await
                .expect("create test filesystem"),
        );
        let export_gates = Arc::new(NbdExportGates::default());
        let device = single_file_export(&filesystem, &export_gates).await;
        let (server_stream, mut client_stream) = tokio::io::duplex(1024 * 1024);
        let (reader, writer) = tokio::io::split(server_stream);
        let mut session = NBDSession::new(
            reader,
            writer,
            filesystem,
            export_gates,
            CancellationToken::new(),
        );
        let session_task = tokio::spawn(async move { session.handle_transmission(device).await });

        // A WRITE, then several READs of the same region behind it. They
        // overlap exactly, which is the point: the specification orders a READ
        // against a WRITE only across a FLUSH or FUA boundary, so with neither
        // present the server is free to run them together and reply in any
        // order. Each READ therefore returns either the old or the new bytes,
        // and the test asserts only that all eight complete.
        client_stream
            .write_all(&probe_request(NBDCommand::Write, 1, 0, 4096))
            .await
            .expect("send write header");
        client_stream
            .write_all(&vec![0x5a; 4096])
            .await
            .expect("send write payload");
        for cookie in 2..=8u64 {
            client_stream
                .write_all(&probe_request(NBDCommand::Read, cookie, 0, 512))
                .await
                .expect("send read header");
        }

        let mut order = Vec::new();
        for _ in 1..=8 {
            let mut reply_bytes = [0; 16];
            timeout(
                Duration::from_secs(5),
                client_stream.read_exact(&mut reply_bytes),
            )
            .await
            .expect("server replied")
            .expect("read reply");
            let (_, reply) = NBDSimpleReply::from_bytes((&reply_bytes, 0)).expect("decode reply");
            assert_eq!(reply.error, 0, "every queued command succeeded");
            order.push(reply.cookie);
            // A READ reply carries its data; drain it before the next header.
            if reply.cookie != 1 {
                let mut data = vec![0; 512];
                timeout(Duration::from_secs(5), client_stream.read_exact(&mut data))
                    .await
                    .expect("server sent read data")
                    .expect("read data");
            }
        }

        let mut sorted = order.clone();
        sorted.sort_unstable();
        assert_eq!(sorted, (1..=8).collect::<Vec<_>>(), "every cookie replied");
        assert_ne!(
            order,
            (1..=8).collect::<Vec<_>>(),
            "replies in strict submission order mean the loop never overlapped \
             two commands; it should service a connection's queue concurrently"
        );

        client_stream
            .write_all(&probe_request(NBDCommand::Disconnect, 0, 0, 0))
            .await
            .expect("send disconnect");
        timeout(Duration::from_secs(5), session_task)
            .await
            .expect("server stopped after disconnect")
            .expect("server task did not panic")
            .expect("server accepted disconnect");
    }

    /// Concurrency must not weaken the flush barrier. A FLUSH queued behind a
    /// WRITE on the same connection still covers it, because the write takes
    /// its admission guard while being read — in request order — and holds it
    /// until it completes, while FLUSH takes the same gate exclusively.
    #[tokio::test]
    async fn a_queued_flush_still_covers_the_write_ahead_of_it() {
        let filesystem = Arc::new(
            ZeroFS::new_in_memory()
                .await
                .expect("create test filesystem"),
        );
        let export_gates = Arc::new(NbdExportGates::default());
        let device = single_file_export(&filesystem, &export_gates).await;
        let (server_stream, mut client_stream) = tokio::io::duplex(1024 * 1024);
        let (reader, writer) = tokio::io::split(server_stream);
        let mut session = NBDSession::new(
            reader,
            writer,
            filesystem,
            export_gates,
            CancellationToken::new(),
        );
        let session_task = tokio::spawn(async move { session.handle_transmission(device).await });

        client_stream
            .write_all(&probe_request(NBDCommand::Write, 1, 0, 4096))
            .await
            .expect("send write header");
        client_stream
            .write_all(&vec![0xc3; 4096])
            .await
            .expect("send write payload");
        client_stream
            .write_all(&probe_request(NBDCommand::Flush, 2, 0, 0))
            .await
            .expect("send flush");

        let mut seen = Vec::new();
        for _ in 0..2 {
            let mut reply_bytes = [0; 16];
            timeout(
                Duration::from_secs(5),
                client_stream.read_exact(&mut reply_bytes),
            )
            .await
            .expect("server replied")
            .expect("read reply");
            let (_, reply) = NBDSimpleReply::from_bytes((&reply_bytes, 0)).expect("decode reply");
            assert_eq!(reply.error, 0, "write and flush both succeeded");
            seen.push(reply.cookie);
        }
        assert_eq!(
            seen,
            vec![1, 2],
            "the flush must not complete before the write it follows"
        );

        client_stream
            .write_all(&probe_request(NBDCommand::Disconnect, 0, 0, 0))
            .await
            .expect("send disconnect");
        timeout(Duration::from_secs(5), session_task)
            .await
            .expect("server stopped after disconnect")
            .expect("server task did not panic")
            .expect("server accepted disconnect");
    }

    fn volatile_ack(bytes: u64) -> crate::fs::mutation::config::FilesystemWriteAckSettings {
        crate::fs::mutation::config::FilesystemWriteAckSettings {
            mode: crate::fs::mutation::config::FilesystemWriteAckMode::VolatileMemory,
            volatile_memory_bytes: bytes,
            volatile_max_operations: 1024,
            source: crate::fs::mutation::config::FilesystemWriteAckSource::Filesystem,
            client_durability_target: crate::fs::mutation::config::ClientDurabilityTarget::LocalSsd,
        }
    }

    async fn volatile_filesystem(bytes: u64) -> (Arc<ZeroFS>, Arc<NbdExportGates>) {
        let mut filesystem = ZeroFS::new_in_memory()
            .await
            .expect("create test filesystem");
        filesystem.write_ack = volatile_ack(bytes);
        let filesystem = Arc::new(filesystem);
        filesystem.install_volatile_overlay();
        let gates = Arc::new(match filesystem.volatile_budget() {
            Some(budget) => NbdExportGates::with_budget(Some(budget)),
            None => NbdExportGates::new(bytes),
        });
        (filesystem, gates)
    }

    #[tokio::test]
    async fn volatile_write_replies_and_reads_from_ram_before_flush_materializes_it() {
        let (filesystem, export_gates) = volatile_filesystem(1024 * 1024).await;
        let device = single_file_export(&filesystem, &export_gates).await;
        let commit_block = filesystem.db.flush_barrier().write_owned().await;
        let apply_reached = filesystem.write_coordinator.probe_next_apply();
        let (server_stream, mut client_stream) = tokio::io::duplex(1024 * 1024);
        let (reader, writer) = tokio::io::split(server_stream);
        let mut session = NBDSession::new(
            reader,
            writer,
            Arc::clone(&filesystem),
            Arc::clone(&export_gates),
            CancellationToken::new(),
        );
        let session_task = tokio::spawn(async move { session.handle_transmission(device).await });

        client_stream
            .write_all(&probe_request(NBDCommand::Write, 1, 0, 4096))
            .await
            .expect("send volatile write header");
        client_stream
            .write_all(&vec![0x6d; 4096])
            .await
            .expect("send volatile write payload");
        let mut reply_bytes = [0; 16];
        timeout(
            Duration::from_secs(1),
            client_stream.read_exact(&mut reply_bytes),
        )
        .await
        .expect("volatile write ACK arrived before materialization")
        .expect("read volatile write ACK");
        let (_, reply) = NBDSimpleReply::from_bytes((&reply_bytes, 0)).expect("decode write ACK");
        assert_eq!((reply.cookie, reply.error), (1, 0));

        timeout(Duration::from_secs(2), apply_reached)
            .await
            .expect("background materializer reached the blocked coordinator")
            .expect("coordinator apply probe remained available");

        let reader_handler = NBDHandler::new(Arc::clone(&filesystem), Arc::clone(&export_gates));
        let reader_device = reader_handler
            .get_device(b"flush-ordering-test")
            .await
            .expect("resolve volatile export through another connection");
        assert_eq!(
            reader_handler.read(&reader_device, 0, 4096).await.unwrap(),
            Bytes::from(vec![0x6d; 4096]),
            "accepted bytes must be immediately visible from the RAM overlay"
        );

        client_stream
            .write_all(&probe_request(NBDCommand::Write, 1, 0, 4096))
            .await
            .expect("reuse cookie after its reply");
        client_stream
            .write_all(&vec![0x7e; 4096])
            .await
            .expect("send reused-cookie payload");
        assert!(
            timeout(
                Duration::from_millis(100),
                client_stream.read_exact(&mut reply_bytes),
            )
            .await
            .is_err(),
            "legal cookie reuse must not be rejected while the prior materialization is blocked"
        );

        client_stream
            .write_all(&probe_request(NBDCommand::Flush, 2, 0, 0))
            .await
            .expect("send durability fence");
        assert!(
            timeout(
                Duration::from_millis(100),
                client_stream.read_exact(&mut reply_bytes),
            )
            .await
            .is_err(),
            "FLUSH must not reply while materialization is blocked"
        );

        drop(commit_block);
        let mut replies = Vec::new();
        for _ in 0..2 {
            timeout(
                Duration::from_secs(5),
                client_stream.read_exact(&mut reply_bytes),
            )
            .await
            .expect("WRITE and FLUSH replied after materialization")
            .expect("read post-materialization reply");
            let (_, reply) =
                NBDSimpleReply::from_bytes((&reply_bytes, 0)).expect("decode simple reply");
            replies.push((reply.cookie, reply.error));
        }
        replies.sort_unstable();
        assert_eq!(replies, vec![(1, 0), (2, 0)]);

        client_stream
            .write_all(&probe_request(NBDCommand::Disconnect, 0, 0, 0))
            .await
            .expect("send disconnect");
        timeout(Duration::from_secs(5), session_task)
            .await
            .expect("session stopped")
            .expect("session task did not panic")
            .expect("session accepted disconnect");
        export_gates.stop_and_drain().await.unwrap();
    }

    #[tokio::test]
    async fn materialized_duplicate_active_cookie_is_rejected_and_its_body_is_drained() {
        let filesystem = Arc::new(ZeroFS::new_in_memory().await.expect("create filesystem"));
        let export_gates = Arc::new(NbdExportGates::default());
        let device = single_file_export(&filesystem, &export_gates).await;
        let commit_block = filesystem.db.flush_barrier().write_owned().await;
        let apply_reached = filesystem.write_coordinator.probe_next_apply();
        let (server_stream, mut client_stream) = tokio::io::duplex(1024 * 1024);
        let (reader, writer) = tokio::io::split(server_stream);
        let mut session = NBDSession::new(
            reader,
            writer,
            Arc::clone(&filesystem),
            Arc::clone(&export_gates),
            CancellationToken::new(),
        );
        let session_task = tokio::spawn(async move { session.handle_transmission(device).await });

        client_stream
            .write_all(&probe_request(NBDCommand::Write, 9, 0, 4096))
            .await
            .unwrap();
        client_stream.write_all(&vec![0x31; 4096]).await.unwrap();
        timeout(Duration::from_secs(2), apply_reached)
            .await
            .expect("first write reached canonical apply")
            .expect("apply probe remained available");
        client_stream
            .write_all(&probe_request(NBDCommand::Write, 9, 0, 4096))
            .await
            .unwrap();
        client_stream.write_all(&vec![0x62; 4096]).await.unwrap();

        let mut reply_bytes = [0; 16];
        timeout(
            Duration::from_secs(1),
            client_stream.read_exact(&mut reply_bytes),
        )
        .await
        .expect("duplicate active cookie rejected before first write completes")
        .expect("read collision reply");
        let (_, collision) =
            NBDSimpleReply::from_bytes((&reply_bytes, 0)).expect("decode collision reply");
        assert_eq!(collision.cookie, 9);
        assert_ne!(collision.error, 0);

        drop(commit_block);
        timeout(
            Duration::from_secs(5),
            client_stream.read_exact(&mut reply_bytes),
        )
        .await
        .expect("first write replied after canonical apply")
        .expect("read first reply");
        let (_, first) = NBDSimpleReply::from_bytes((&reply_bytes, 0)).expect("decode first reply");
        assert_eq!((first.cookie, first.error), (9, 0));

        client_stream
            .write_all(&probe_request(NBDCommand::Read, 10, 0, 4096))
            .await
            .expect("send request after rejected payload");
        let mut read_reply = vec![0; 16 + 4096];
        timeout(
            Duration::from_secs(5),
            client_stream.read_exact(&mut read_reply),
        )
        .await
        .expect("stream remained aligned after collision body drain")
        .expect("read data reply");
        let (_, reply) = NBDSimpleReply::from_bytes((&read_reply[..16], 0)).unwrap();
        assert_eq!((reply.cookie, reply.error), (10, 0));
        assert_eq!(&read_reply[16..], &[0x31; 4096]);

        client_stream
            .write_all(&probe_request(NBDCommand::Disconnect, 0, 0, 0))
            .await
            .unwrap();
        timeout(Duration::from_secs(5), session_task)
            .await
            .expect("session stopped")
            .expect("session task did not panic")
            .expect("session accepted disconnect");
        export_gates.stop_and_drain().await.unwrap();
    }

    #[tokio::test]
    async fn volatile_fua_write_replies_only_after_materialization_and_flush() {
        let (filesystem, export_gates) = volatile_filesystem(1024 * 1024).await;
        let device = single_file_export(&filesystem, &export_gates).await;
        let commit_block = filesystem.db.flush_barrier().write_owned().await;
        let apply_reached = filesystem.write_coordinator.probe_next_apply();
        let (server_stream, mut client_stream) = tokio::io::duplex(1024 * 1024);
        let (reader, writer) = tokio::io::split(server_stream);
        let mut session = NBDSession::new(
            reader,
            writer,
            Arc::clone(&filesystem),
            Arc::clone(&export_gates),
            CancellationToken::new(),
        );
        let session_task = tokio::spawn(async move { session.handle_transmission(device).await });

        let request = NBDRequest {
            magic: NBD_REQUEST_MAGIC,
            flags: NBD_CMD_FLAG_FUA,
            cmd_type: NBDCommand::Write,
            cookie: 7,
            offset: 0,
            length: 4096,
        }
        .to_bytes()
        .expect("encode FUA write");
        client_stream.write_all(&request).await.unwrap();
        client_stream.write_all(&vec![0x7a; 4096]).await.unwrap();
        timeout(Duration::from_secs(2), apply_reached)
            .await
            .expect("background materializer reached the blocked coordinator")
            .expect("coordinator apply probe remained available");

        let mut reply_bytes = [0; 16];
        assert!(
            timeout(
                Duration::from_millis(100),
                client_stream.read_exact(&mut reply_bytes),
            )
            .await
            .is_err(),
            "FUA must not acknowledge volatile RAM ownership as durable"
        );

        drop(commit_block);
        timeout(
            Duration::from_secs(5),
            client_stream.read_exact(&mut reply_bytes),
        )
        .await
        .expect("FUA replied after materialization and flush")
        .expect("read FUA reply");
        let (_, reply) = NBDSimpleReply::from_bytes((&reply_bytes, 0)).expect("decode FUA reply");
        assert_eq!((reply.cookie, reply.error), (7, 0));

        client_stream
            .write_all(&probe_request(NBDCommand::Disconnect, 0, 0, 0))
            .await
            .expect("send disconnect");
        timeout(Duration::from_secs(5), session_task)
            .await
            .expect("session stopped")
            .expect("session task did not panic")
            .expect("session accepted disconnect");
        export_gates.stop_and_drain().await.unwrap();
    }

    /// One command may be as large as `MAX_REQUEST_LENGTH`, which is bigger
    /// than the whole per-connection byte budget. Its credit is clamped to the
    /// budget so it remains admissible on its own instead of waiting forever
    /// for capacity that cannot exist.
    #[test]
    fn an_oversized_request_still_fits_the_connection_byte_budget() {
        assert_eq!(budget_cost(4096), 4096);
        assert_eq!(
            budget_cost(MAX_INFLIGHT_BYTES as u32),
            MAX_INFLIGHT_BYTES as u32
        );
        assert_eq!(budget_cost(MAX_REQUEST_LENGTH), MAX_INFLIGHT_BYTES as u32);
        assert!(
            MAX_REQUEST_LENGTH as usize > MAX_INFLIGHT_BYTES,
            "the clamp is only load-bearing while a request can exceed the budget"
        );
    }

    #[tokio::test]
    async fn volatile_admission_rejection_consumes_the_write_body() {
        let (filesystem, export_gates) = volatile_filesystem(4).await;
        let device = single_file_export(&filesystem, &export_gates).await;
        let handler = NBDHandler::new(filesystem, export_gates);
        let mut wire: &[u8] = b"payloadNEXT";

        let result = admit_write(
            &mut wire,
            &handler,
            &device,
            0,
            7,
            false,
            &CancellationToken::new(),
        )
        .await;

        assert!(matches!(result, Err(CommandError::NoSpace)));
        let mut next = [0; 4];
        wire.read_exact(&mut next)
            .await
            .expect("read next header bytes");
        assert_eq!(&next, b"NEXT", "rejected WRITE body must be consumed");
    }

    #[tokio::test]
    async fn failed_rejected_write_discard_closes_the_desynchronized_session() {
        let (filesystem, export_gates) = volatile_filesystem(4).await;
        let device = single_file_export(&filesystem, &export_gates).await;
        let handler = NBDHandler::new(filesystem, export_gates);
        let shutdown = CancellationToken::new();
        let mut truncated_body: &[u8] = b"x";

        let result = admit_write(
            &mut truncated_body,
            &handler,
            &device,
            4095,
            2,
            false,
            &shutdown,
        )
        .await;

        assert!(matches!(result, Err(CommandError::IoError)));
        assert!(
            shutdown.is_cancelled(),
            "an incomplete rejected body leaves framing unknown and must close the session"
        );
    }

    #[tokio::test]
    async fn failed_admitted_write_payload_closes_the_desynchronized_session() {
        let filesystem = Arc::new(
            ZeroFS::new_in_memory()
                .await
                .expect("create test filesystem"),
        );
        let export_gates = Arc::new(NbdExportGates::default());
        let device = single_file_export(&filesystem, &export_gates).await;
        let handler = NBDHandler::new(filesystem, export_gates);
        let shutdown = CancellationToken::new();
        let mut truncated_body: &[u8] = b"x";

        let result = admit_write(
            &mut truncated_body,
            &handler,
            &device,
            0,
            2,
            false,
            &shutdown,
        )
        .await;

        assert!(matches!(result, Err(CommandError::IoError)));
        assert!(
            shutdown.is_cancelled(),
            "a partial admitted body leaves framing unknown and must close the session"
        );
    }

    /// The byte budget must actually gate admission, not merely be computed.
    ///
    /// Twenty 4 MiB writes are 80 MiB of payload against a 64 MiB budget, and
    /// twenty commands against a cap of `MAX_INFLIGHT_COMMANDS` (32) — so the
    /// count cap cannot be what stops this. With replies stalled, no credit is
    /// ever returned, and the read side must stop consuming requests partway
    /// through rather than buffering all 80 MiB.
    #[tokio::test]
    async fn admission_stops_reading_once_the_byte_budget_is_spent() {
        const CHUNK: usize = 4 * 1024 * 1024;
        const WRITES: usize = 20;
        let filesystem = Arc::new(
            ZeroFS::new_in_memory()
                .await
                .expect("create test filesystem"),
        );
        let export_gates = Arc::new(NbdExportGates::default());
        sized_single_file_export(&filesystem, b"budget-probe", (CHUNK * WRITES) as u64).await;

        let mut requests = Vec::with_capacity(WRITES * (CHUNK + 32));
        for i in 0..WRITES {
            requests.extend_from_slice(&probe_request(
                NBDCommand::Write,
                i as u64,
                (i * CHUNK) as u64,
                CHUNK as u32,
            ));
            requests.extend_from_slice(&vec![0x6b; CHUNK]);
        }
        let total = requests.len();
        let delivered = Arc::new(AtomicUsize::new(0));
        let (stalled_tx, stalled_rx) = oneshot::channel();
        let mut session = NBDSession::new(
            PrebufferedRequest {
                bytes: requests,
                position: 0,
                polls: Arc::new(AtomicUsize::new(0)),
                delivered: Arc::clone(&delivered),
            },
            StalledWriter {
                stalled: Some(stalled_tx),
            },
            Arc::clone(&filesystem),
            Arc::clone(&export_gates),
            CancellationToken::new(),
        );
        let device = NBDHandler::new(Arc::clone(&filesystem), Arc::clone(&export_gates))
            .get_device(b"budget-probe")
            .await
            .expect("discover budget export");
        let session_task = tokio::spawn(async move { session.handle_transmission(device).await });

        stalled_rx
            .await
            .expect("the session produced a reply and blocked writing it");
        let requests_at_budget = MAX_INFLIGHT_BYTES / CHUNK;
        let full_budget_consumed =
            MAX_INFLIGHT_BYTES + requests_at_budget * NBD_REQUEST_HEADER_SIZE;
        timeout(Duration::from_secs(5), async {
            while delivered.load(Ordering::Relaxed) < full_budget_consumed {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("admission reached the byte budget");

        let consumed = delivered.load(Ordering::Relaxed);
        assert!(
            consumed >= full_budget_consumed,
            "admission stopped at {consumed} bytes before filling its {} MiB budget \
             ({full_budget_consumed} bytes including admitted headers)",
            MAX_INFLIGHT_BYTES / (1024 * 1024)
        );
        assert!(
            consumed <= full_budget_consumed + NBD_REQUEST_HEADER_SIZE,
            "admission consumed {consumed} bytes after its {} MiB budget was full; \
             at most the next request header ({}) may be read",
            MAX_INFLIGHT_BYTES / (1024 * 1024),
            full_budget_consumed + NBD_REQUEST_HEADER_SIZE,
        );
        assert!(
            consumed < total,
            "the source must contain work beyond the budget"
        );

        session_task.abort();
        let _ = session_task.await;
    }

    /// A client that stops reading its replies must not pin admission guards.
    ///
    /// Replies drain on their own stream, so a blocked socket write leaves
    /// `inflight` still being polled and every write still reaches the
    /// `drop(admission)` inside `write_admitted`. If replies were written
    /// inline in the loop instead, the loop would park in the socket write, the
    /// second write would never be polled to completion, and its read guard
    /// would hold the export gate — which is shared by every connection to that
    /// export — stalling `flush` fleet-wide.
    #[tokio::test]
    async fn a_client_that_stops_reading_replies_does_not_stall_flush() {
        let filesystem = Arc::new(
            ZeroFS::new_in_memory()
                .await
                .expect("create test filesystem"),
        );
        let export_gates = Arc::new(NbdExportGates::default());
        let device = single_file_export(&filesystem, &export_gates).await;
        let flush_device = NBDHandler::new(Arc::clone(&filesystem), Arc::clone(&export_gates))
            .get_device(b"flush-ordering-test")
            .await
            .expect("open the same export on another connection");

        let mut requests = Vec::new();
        for cookie in 1..=2u64 {
            requests.extend_from_slice(&probe_request(NBDCommand::Write, cookie, 0, 512));
            requests.extend_from_slice(&vec![0x3c; 512]);
        }
        let (stalled_tx, stalled_rx) = oneshot::channel();
        let mut session = NBDSession::new(
            PrebufferedRequest {
                bytes: requests,
                position: 0,
                polls: Arc::new(AtomicUsize::new(0)),
                delivered: Arc::new(AtomicUsize::new(0)),
            },
            StalledWriter {
                stalled: Some(stalled_tx),
            },
            Arc::clone(&filesystem),
            Arc::clone(&export_gates),
            CancellationToken::new(),
        );
        let session_task = tokio::spawn(async move { session.handle_transmission(device).await });

        stalled_rx
            .await
            .expect("the session tried to write a reply and blocked");

        let flush_handler = NBDHandler::new(filesystem, export_gates);
        timeout(Duration::from_secs(5), flush_handler.flush(&flush_device))
            .await
            .expect("a stalled reply socket must not hold the export gate")
            .expect("FLUSH succeeded");

        session_task.abort();
        let _ = session_task.await;
    }

    /// An unclean disconnect is not a reason to abandon accepted work. A client
    /// that vanishes after its request — RST or plain EOF — leaves a write that
    /// was already admitted, and dropping it mid-flight could tear a striped
    /// write across its members. The session drains instead, exactly as it does
    /// for NBD_CMD_DISC.
    #[tokio::test]
    async fn a_vanished_client_still_finishes_its_admitted_write() {
        let filesystem = Arc::new(
            ZeroFS::new_in_memory()
                .await
                .expect("create test filesystem"),
        );
        let export_gates = Arc::new(NbdExportGates::default());
        let device = single_file_export(&filesystem, &export_gates).await;
        let probe = NBDHandler::new(Arc::clone(&filesystem), Arc::clone(&export_gates))
            .get_device(b"flush-ordering-test")
            .await
            .expect("open the same export to read back");

        // The request is complete, but nothing follows it: the reader reports
        // EOF where the next header would begin.
        let mut requests = probe_request(NBDCommand::Write, 1, 0, 512);
        requests.extend_from_slice(&vec![0xd7; 512]);
        let mut session = NBDSession::new(
            PrebufferedRequest {
                bytes: requests,
                position: 0,
                polls: Arc::new(AtomicUsize::new(0)),
                delivered: Arc::new(AtomicUsize::new(0)),
            },
            tokio::io::sink(),
            Arc::clone(&filesystem),
            Arc::clone(&export_gates),
            CancellationToken::new(),
        );
        timeout(Duration::from_secs(5), session.handle_transmission(device))
            .await
            .expect("the session ended after its client vanished")
            .expect("an EOF between requests is a clean end, not a session error");

        let landed = NBDHandler::new(filesystem, export_gates)
            .read(&probe, 0, 512)
            .await
            .expect("read back the admitted write");
        assert_eq!(
            landed,
            Bytes::from(vec![0xd7; 512]),
            "a write admitted before the client vanished must still land"
        );
    }

    #[tokio::test]
    async fn reply_failure_stops_admission_and_returns_the_writer_error() {
        let filesystem = Arc::new(
            ZeroFS::new_in_memory()
                .await
                .expect("create test filesystem"),
        );
        let export_gates = Arc::new(NbdExportGates::default());
        let device = single_file_export(&filesystem, &export_gates).await;

        let mut first = probe_request(NBDCommand::Write, 1, 0, 512);
        first.extend_from_slice(&vec![0xa7; 512]);
        let mut tail = probe_request(NBDCommand::Write, 2, 512, 512);
        tail.extend_from_slice(&vec![0xb8; 512]);
        let writer_failed = Arc::new(AtomicBool::new(false));
        let tail_delivered = Arc::new(AtomicUsize::new(0));
        let mut session = NBDSession::new(
            RequestThenWriterFailure {
                first,
                tail,
                position: 0,
                writer_failed: Arc::clone(&writer_failed),
                tail_delivered: Arc::clone(&tail_delivered),
            },
            FailingWriter {
                failed: writer_failed,
                polls: Arc::new(AtomicUsize::new(0)),
            },
            Arc::clone(&filesystem),
            Arc::clone(&export_gates),
            CancellationToken::new(),
        );

        let error = timeout(Duration::from_secs(5), session.handle_transmission(device))
            .await
            .expect("session stopped after reply failure")
            .expect_err("reply failure must fail the session");
        assert!(
            error.to_string().contains("injected reply failure"),
            "the original writer error must be returned: {error}"
        );
        assert_eq!(
            tail_delivered.load(Ordering::Relaxed),
            0,
            "no later request may be consumed after the reply path fails"
        );

        let handler = NBDHandler::new(filesystem, export_gates);
        let probe = handler
            .get_device(b"flush-ordering-test")
            .await
            .expect("reopen export after failed reply");
        assert_eq!(
            handler
                .read(&probe, 0, 512)
                .await
                .expect("read landed write"),
            Bytes::from(vec![0xa7; 512]),
            "the command admitted before the reply failure must finish"
        );
        timeout(Duration::from_secs(2), handler.flush(&probe))
            .await
            .expect("reply failure released the export gate")
            .expect("flush after reply failure succeeded");
    }

    #[tokio::test]
    async fn reply_failure_drains_every_command_admitted_before_it() {
        let filesystem = Arc::new(
            ZeroFS::new_in_memory()
                .await
                .expect("create test filesystem"),
        );
        let export_gates = Arc::new(NbdExportGates::default());
        let device = single_file_export(&filesystem, &export_gates).await;
        let commit_block = filesystem.db.flush_barrier().write_owned().await;

        let mut requests = Vec::new();
        for (cookie, offset, byte) in [(1, 0, 0x51), (2, 512, 0x62)] {
            requests.extend_from_slice(&probe_request(NBDCommand::Write, cookie, offset, 512));
            requests.extend_from_slice(&vec![byte; 512]);
        }
        let total = requests.len();
        let delivered = Arc::new(AtomicUsize::new(0));
        let writer_failed = Arc::new(AtomicBool::new(false));
        let writer_polls = Arc::new(AtomicUsize::new(0));
        let mut session = NBDSession::new(
            PrebufferedRequest {
                bytes: requests,
                position: 0,
                polls: Arc::new(AtomicUsize::new(0)),
                delivered: Arc::clone(&delivered),
            },
            FailingWriter {
                failed: writer_failed,
                polls: Arc::clone(&writer_polls),
            },
            Arc::clone(&filesystem),
            Arc::clone(&export_gates),
            CancellationToken::new(),
        );
        let session_task = tokio::spawn(async move { session.handle_transmission(device).await });

        timeout(Duration::from_secs(2), async {
            while delivered.load(Ordering::Relaxed) < total {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("both requests were admitted before their commits resumed");
        drop(commit_block);

        let error = timeout(Duration::from_secs(5), session_task)
            .await
            .expect("session drained after reply failure")
            .expect("session task did not panic")
            .expect_err("reply failure must fail the session");
        assert!(
            error.to_string().contains("injected reply failure"),
            "the original writer error must be returned: {error}"
        );
        assert_eq!(
            writer_polls.load(Ordering::Relaxed),
            1,
            "the irrecoverably broken writer must never be touched again"
        );

        let handler = NBDHandler::new(filesystem, export_gates);
        let probe = handler
            .get_device(b"flush-ordering-test")
            .await
            .expect("reopen export after failed reply");
        assert_eq!(
            handler
                .read(&probe, 0, 1024)
                .await
                .expect("read both writes"),
            Bytes::from([vec![0x51; 512], vec![0x62; 512]].concat()),
            "every write admitted before the reply failure must finish"
        );
        timeout(Duration::from_secs(2), handler.flush(&probe))
            .await
            .expect("reply failure released every export guard")
            .expect("flush after reply failure succeeded");
    }

    #[tokio::test]
    async fn terminal_read_error_waits_for_an_admitted_write_before_returning() {
        let filesystem = Arc::new(
            ZeroFS::new_in_memory()
                .await
                .expect("create test filesystem"),
        );
        let export_gates = Arc::new(NbdExportGates::default());
        let device = single_file_export(&filesystem, &export_gates).await;
        let commit_block = filesystem.db.flush_barrier().write_owned().await;
        let apply_reached = filesystem.write_coordinator.probe_next_apply();

        let mut request = probe_request(NBDCommand::Write, 1, 0, 512);
        request.extend_from_slice(&vec![0xc9; 512]);
        let mut session = NBDSession::new(
            RequestThenReadError {
                bytes: request,
                position: 0,
                kind: io::ErrorKind::TimedOut,
            },
            tokio::io::sink(),
            Arc::clone(&filesystem),
            Arc::clone(&export_gates),
            CancellationToken::new(),
        );
        let mut session_task =
            tokio::spawn(async move { session.handle_transmission(device).await });

        timeout(Duration::from_secs(2), apply_reached)
            .await
            .expect("write reached the blocked apply")
            .expect("apply probe remained installed");
        assert!(
            !session_task.is_finished(),
            "the terminal read error must not abandon an admitted write"
        );

        drop(commit_block);
        let error = timeout(Duration::from_secs(5), &mut session_task)
            .await
            .expect("session returned after admitted work drained")
            .expect("session task did not panic")
            .expect_err("the terminal read error must be preserved");
        assert!(
            error.to_string().contains("injected read failure"),
            "the original read error must be returned: {error}"
        );

        let handler = NBDHandler::new(filesystem, export_gates);
        let probe = handler
            .get_device(b"flush-ordering-test")
            .await
            .expect("reopen export after read failure");
        assert_eq!(
            handler
                .read(&probe, 0, 512)
                .await
                .expect("read landed write"),
            Bytes::from(vec![0xc9; 512]),
            "the command admitted before the read failure must finish"
        );
    }
}
