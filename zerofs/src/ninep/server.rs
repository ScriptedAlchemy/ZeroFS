#[cfg(test)]
pub(crate) use super::handler::NinePHandler;
pub(crate) use super::lock_manager::FileLockManager;
use crate::fs::ZeroFS;
use crate::task::spawn_named;
#[cfg(test)]
use bytes::Bytes;
use futures::StreamExt;
use futures::stream::FuturesUnordered;
#[cfg(test)]
use ninep_proto::{
    Message, P9_COUNT_FIELD_LEN, P9_HEADER_SIZE, P9_MAX_MSIZE, P9_OP_ENVELOPE_LEN,
    P9_OP_FLAG_RETRY, P9_OP_ID_LEN, P9_SIZE_FIELD_LEN, P9Message, Rlerror, T_WRITE, message_type,
};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
#[cfg(test)]
use tokio::io::AsyncWriteExt;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::{TcpListener, UnixListener};
#[cfg(test)]
use tokio::sync::mpsc;
#[cfg(test)]
use tokio::sync::oneshot;
use tokio_util::sync::CancellationToken;
use tokio_util::task::AbortOnDropHandle;
use tracing::{error, info, warn};

mod admission;
mod frame_codec;
mod response_writer;
mod session;

#[cfg(test)]
use admission::P9AdmissionSnapshot;
#[cfg(feature = "webui")]
pub(crate) use admission::P9TransportPermit;
#[cfg(test)]
use admission::{
    CONNECTION_INFLIGHT_MEMORY, CONNECTION_INFLIGHT_REQUESTS, GLOBAL_INFLIGHT_MEMORY,
    GLOBAL_INFLIGHT_REQUESTS, GLOBAL_TRANSPORT_SESSIONS, P9ConnectionAdmission,
};
pub(crate) use admission::{P9AcceptedWorkTracker, P9GlobalAdmission};
#[cfg(test)]
use frame_codec::{
    FidFootprint, P9_RWRITE_MAX_SIZE, ShallowFrameMetadata, inspect_frame_metadata,
    possible_response_bytes, read_9p_frame,
};
#[cfg(any(feature = "webui", test))]
#[allow(unused_imports)]
pub(crate) use response_writer::{P9Response, response_may_be_emitted};
#[cfg(test)]
use response_writer::{RESPONSE_BUFFER_CAPACITY, ResponseAuthority, spawn_response_writer};
#[cfg(test)]
use session::{CLIENT_DRAIN_TIMEOUT, CompletionWaiter, enqueue_terminal_response};
#[cfg(any(feature = "webui", test))]
pub(crate) use session::{InflightRegistry, dispatch_9p_frame, settle_request_tasks};
use session::{P9SessionAdmission, handle_client_stream};
#[cfg(test)]
use session::{handle_client_loop, join_with_timeout};

/// TCP keepalive idle interval.
const TCP_KEEPALIVE_IDLE: std::time::Duration = std::time::Duration::from_secs(10);
const TCP_KEEPALIVE_INTERVAL: std::time::Duration = std::time::Duration::from_secs(5);
const TCP_KEEPALIVE_RETRIES: u32 = 4;
/// TCP timeout for unacknowledged data.
#[cfg(any(
    target_os = "android",
    target_os = "cygwin",
    target_os = "fuchsia",
    target_os = "linux"
))]
const TCP_USER_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

fn configure_accepted_tcp_stream(stream: &tokio::net::TcpStream) -> std::io::Result<()> {
    stream.set_nodelay(true)?;

    let socket = socket2::SockRef::from(stream);
    let keepalive = socket2::TcpKeepalive::new()
        .with_time(TCP_KEEPALIVE_IDLE)
        .with_interval(TCP_KEEPALIVE_INTERVAL)
        .with_retries(TCP_KEEPALIVE_RETRIES);
    socket.set_tcp_keepalive(&keepalive)?;

    #[cfg(any(
        target_os = "android",
        target_os = "cygwin",
        target_os = "fuchsia",
        target_os = "linux"
    ))]
    socket.set_tcp_user_timeout(Some(TCP_USER_TIMEOUT))?;

    Ok(())
}

pub enum Transport {
    Tcp(SocketAddr),
    Unix(PathBuf),
}

pub struct NinePServer {
    filesystem: Arc<ZeroFS>,
    transport: Transport,
    lock_manager: Arc<FileLockManager>,
    credential_override: Option<(u32, u32)>,
}

impl NinePServer {
    pub fn new(filesystem: Arc<ZeroFS>, addr: SocketAddr) -> Self {
        Self {
            filesystem,
            transport: Transport::Tcp(addr),
            lock_manager: Arc::new(FileLockManager::new()),
            credential_override: None,
        }
    }

    pub fn new_unix(filesystem: Arc<ZeroFS>, path: PathBuf) -> Self {
        Self {
            filesystem,
            transport: Transport::Unix(path),
            lock_manager: Arc::new(FileLockManager::new()),
            credential_override: None,
        }
    }

    /// Override client-provided credentials for a shared writable namespace.
    pub fn with_credential_override(mut self, uid: u32, gid: u32) -> Self {
        self.credential_override = Some((uid, gid));
        self
    }

    fn spawn_client_handler<R, W>(
        &self,
        read_stream: R,
        write_stream: W,
        shutdown: &CancellationToken,
        client_name: String,
        session: P9SessionAdmission,
    ) -> AbortOnDropHandle<()>
    where
        R: AsyncRead + Unpin + Send + 'static,
        W: AsyncWrite + Unpin + Send + 'static,
    {
        let filesystem = Arc::clone(&self.filesystem);
        let lock_manager = Arc::clone(&self.lock_manager);
        let credential_override = self.credential_override;
        let client_shutdown = shutdown.child_token();

        AbortOnDropHandle::new(spawn_named("9p-client", async move {
            if let Err(e) = handle_client_stream(
                read_stream,
                write_stream,
                filesystem,
                lock_manager,
                credential_override,
                client_shutdown,
                session,
            )
            .await
            {
                error!("Error handling 9P client {}: {}", client_name, e);
            }
        }))
    }

    #[allow(dead_code)]
    pub async fn start(&self, shutdown: CancellationToken) -> std::io::Result<()> {
        let accepted_work = P9AcceptedWorkTracker::new();
        let result = self
            .start_with_accepted_work(shutdown, accepted_work.clone())
            .await;
        accepted_work.stop_accepting();
        accepted_work.wait().await;
        result
    }

    pub(crate) async fn start_with_accepted_work(
        &self,
        shutdown: CancellationToken,
        accepted_work: P9AcceptedWorkTracker,
    ) -> std::io::Result<()> {
        let mut clients = FuturesUnordered::new();
        let clients_shutdown = shutdown.child_token();
        let serve_result = match &self.transport {
            Transport::Tcp(addr) => {
                let listener = TcpListener::bind(addr)
                    .await
                    .map_err(|e| crate::net_util::tcp_bind_error("9P", addr, &e))?;
                info!("9P server listening on TCP {}", addr);

                loop {
                    tokio::select! {
                        biased;
                        _ = shutdown.cancelled() => {
                            info!("9P TCP server shutting down on {}", addr);
                            break Ok(());
                        }
                        finished = clients.next(), if !clients.is_empty() => {
                            if let Some(Err(e)) = finished {
                                warn!("9P client task failed: {e}");
                            }
                        }
                        result = listener.accept() => {
                            let (stream, peer_addr) = match result {
                                Ok(accepted) => accepted,
                                Err(e) => break Err(e),
                            };
                            info!("9P client connected from {}", peer_addr);
                            if let Err(e) = configure_accepted_tcp_stream(&stream) {
                                warn!("Failed to configure 9P TCP client {peer_addr}: {e}");
                                continue;
                            }
                            let transport = match P9GlobalAdmission::shared().try_admit_transport() {
                                Ok(transport) => transport,
                                Err(error) => {
                                    warn!("Rejecting 9P TCP client {peer_addr}: {error}");
                                    continue;
                                }
                            };
                            let (read_half, write_half) = stream.into_split();
                            clients.push(
                                self.spawn_client_handler(
                                    read_half,
                                    write_half,
                                    &clients_shutdown,
                                    peer_addr.to_string(),
                                    P9SessionAdmission {
                                        transport,
                                        transport_label: "tcp",
                                        accepted_work: accepted_work.clone(),
                                    },
                                ),
                            );
                        }
                    }
                }
            }
            Transport::Unix(path) => {
                let _ = std::fs::remove_file(path);

                let listener = UnixListener::bind(path).map_err(|e| {
                    std::io::Error::new(
                        e.kind(),
                        format!("Failed to bind Unix socket at {:?}: {}", path, e),
                    )
                })?;
                info!("9P server listening on Unix socket {:?}", path);

                loop {
                    tokio::select! {
                        biased;
                        _ = shutdown.cancelled() => {
                            info!("9P Unix socket server shutting down at {:?}", path);
                            break Ok(());
                        }
                        finished = clients.next(), if !clients.is_empty() => {
                            if let Some(Err(e)) = finished {
                                warn!("9P client task failed: {e}");
                            }
                        }
                        result = listener.accept() => {
                            let (stream, _) = match result {
                                Ok(accepted) => accepted,
                                Err(e) => break Err(e),
                            };
                            info!("9P client connected via Unix socket");
                            let transport = match P9GlobalAdmission::shared().try_admit_transport() {
                                Ok(transport) => transport,
                                Err(error) => {
                                    warn!("Rejecting 9P Unix client: {error}");
                                    continue;
                                }
                            };
                            let (read_half, write_half) = stream.into_split();
                            clients.push(
                                self.spawn_client_handler(
                                    read_half,
                                    write_half,
                                    &clients_shutdown,
                                    "unix".to_string(),
                                    P9SessionAdmission {
                                        transport,
                                        transport_label: "unix",
                                        accepted_work: accepted_work.clone(),
                                    },
                                ),
                            );
                        }
                    }
                }
            }
        };

        // Listener exit drains existing connection tasks.
        clients_shutdown.cancel();

        // Each connection bounds and joins its writer before returning.
        while let Some(result) = clients.next().await {
            if let Err(e) = result {
                warn!("9P client task failed while draining: {e}");
            }
        }

        serve_result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fs::inode::Inode;
    use crate::fs::permissions::Credentials;
    use crate::ninep::handler::{SessionReleaseGuard, pause_success_terminal_dedup};
    use crate::ninep::lock_manager::FileLock;
    use ninep_proto::{
        DekuBytes, GETATTR_ALL, LockType, P9String, Rclunk, Rflush, Rlopenat, Rread, Tattach,
        Tclunk, Tflush, Tgetattr, Tlopenat, Tmkdir, Tversion, Twrite, VERSION_9P2000L,
        VERSION_9P2000L_ZEROFS,
    };
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;
    use tokio::io::AsyncReadExt;
    use tokio::sync::Notify;
    use tokio_util::task::TaskTracker;

    const TEST_TIMEOUT: Duration = Duration::from_secs(1);
    const QUIET_TIMEOUT: Duration = Duration::from_millis(20);

    #[tokio::test]
    async fn transport_receive_envelope_bounds_reconnect_frames() {
        const TEST_TRANSPORTS: usize = GLOBAL_TRANSPORT_SESSIONS;
        let global = P9GlobalAdmission::for_test_with_transports(
            GLOBAL_INFLIGHT_MEMORY,
            GLOBAL_INFLIGHT_REQUESTS,
            TEST_TRANSPORTS,
        );
        let mut admitted = Vec::new();
        for _ in 0..TEST_TRANSPORTS {
            admitted.push(global.try_admit_transport().unwrap());
        }

        for _ in 0..128 {
            assert!(
                global.try_admit_transport().is_err(),
                "excess reconnects must be rejected before handler spawn or WebSocket upgrade"
            );
        }
        assert_eq!(global.snapshot().active_transports, TEST_TRANSPORTS);
        assert!(
            global.transport_limit >= 32,
            "the envelope must retain room for sixteen idle Mesh and sixteen upload sessions"
        );
        let documented_bound = global.byte_limit + global.transport_limit * P9_MAX_MSIZE as usize;
        assert_eq!(
            documented_bound,
            1024 * 1024 * 1024,
            "the admitted budget plus one maximum frame per transport is the documented bound"
        );

        drop(admitted);
        assert_eq!(global.snapshot().active_transports, 0);
    }

    #[tokio::test]
    async fn exact_reader_transfers_a_max_frame_from_receive_to_request_credit() {
        let global = P9GlobalAdmission::for_test_with_transports(
            P9_MAX_MSIZE as usize + P9_RWRITE_MAX_SIZE,
            1,
            1,
        );
        let _transport = global.try_admit_transport().unwrap();
        let connection = global.connection_for_test(P9_MAX_MSIZE as usize + P9_RWRITE_MAX_SIZE, 1);
        let shutdown = CancellationToken::new();
        let receive = global.admit_receive(&shutdown).await.unwrap();
        let (mut client, mut server) = tokio::io::duplex(P9_MAX_MSIZE as usize + 1);
        let mut encoded = vec![0; P9_MAX_MSIZE as usize];
        encoded[..P9_SIZE_FIELD_LEN].copy_from_slice(&P9_MAX_MSIZE.to_le_bytes());
        let writer = tokio::spawn(async move { client.write_all(&encoded).await });

        let frame = read_9p_frame(&mut server, &shutdown)
            .await
            .unwrap()
            .expect("maximum frame");
        writer.await.unwrap().unwrap();
        assert_eq!(frame.len(), P9_MAX_MSIZE as usize);
        assert_eq!(
            global.snapshot().receive_reserved_bytes,
            P9_MAX_MSIZE as usize,
            "the complete frame allocation must remain covered by receive credit"
        );

        let admitted = connection
            .admit_request(frame.len(), P9_RWRITE_MAX_SIZE)
            .await
            .unwrap();
        drop(receive);
        assert_eq!(global.snapshot().receive_reserved_bytes, 0);
        assert_eq!(
            global.snapshot().reserved_bytes,
            P9_MAX_MSIZE as usize + P9_RWRITE_MAX_SIZE
        );
        drop(frame);
        drop(admitted);
    }

    #[test]
    fn large_read_response_reserves_source_and_serialized_buffers() {
        assert_eq!(
            possible_response_bytes(message_type::TREAD),
            2 * P9_MAX_MSIZE as usize,
            "a large read retains its source payload while serializing the wire response"
        );
    }

    #[tokio::test]
    async fn fragmented_websocket_receive_reserves_collector_and_frame() {
        let global = P9GlobalAdmission::for_test_with_transports(
            GLOBAL_INFLIGHT_MEMORY,
            GLOBAL_INFLIGHT_REQUESTS,
            2,
        );
        let shutdown = CancellationToken::new();
        let receive = global.admit_websocket_receive(&shutdown).await.unwrap();
        assert_eq!(
            global.snapshot().receive_reserved_bytes,
            2 * P9_MAX_MSIZE as usize,
            "fragment assembly must charge both the collector and current frame"
        );
        drop(receive);
        assert_eq!(global.snapshot().receive_reserved_bytes, 0);
    }

    #[tokio::test]
    async fn default_upload_pipeline_fits_the_global_byte_budget() {
        const DEFAULT_CONNECTIONS: usize = 16;
        const PIPELINE_PER_CONNECTION: usize = 2;
        const CHUNK_BYTES: usize = 9 * 1024 * 1024;
        const TWRITE_FRAME_BYTES: usize =
            CHUNK_BYTES + P9_HEADER_SIZE + P9_OP_ENVELOPE_LEN + 4 + 8 + P9_COUNT_FIELD_LEN;

        let global = P9GlobalAdmission::for_test(GLOBAL_INFLIGHT_MEMORY, GLOBAL_INFLIGHT_REQUESTS);
        let connections = (0..DEFAULT_CONNECTIONS)
            .map(|_| {
                global.connection_for_test(CONNECTION_INFLIGHT_MEMORY, CONNECTION_INFLIGHT_REQUESTS)
            })
            .collect::<Vec<_>>();
        let mut admitted = Vec::new();
        for connection in &connections {
            for _ in 0..PIPELINE_PER_CONNECTION {
                admitted.push(
                    connection
                        .admit_request(TWRITE_FRAME_BYTES, possible_response_bytes(T_WRITE))
                        .await
                        .unwrap(),
                );
            }
        }

        assert_eq!(admitted.len(), 32);
        assert!(global.snapshot().reserved_bytes < GLOBAL_INFLIGHT_MEMORY);
        drop(admitted);
        assert_eq!(global.snapshot().reserved_bytes, 0);
    }

    #[tokio::test]
    async fn connection_request_bytes_apply_backpressure_before_dispatch() {
        let global = P9GlobalAdmission::for_test(16, 4);
        let connection = global.connection_for_test(8, 4);
        let first = connection.admit_request(5, 3).await.unwrap();

        assert!(
            tokio::time::timeout(QUIET_TIMEOUT, connection.admit_request(1, 0))
                .await
                .is_err(),
            "request and possible response bytes must share the local budget"
        );

        drop(first);
        tokio::time::timeout(TEST_TIMEOUT, connection.admit_request(1, 0))
            .await
            .expect("released request bytes must unblock the connection")
            .unwrap();
    }

    #[tokio::test]
    async fn global_request_bytes_apply_backpressure_across_connections() {
        let global = P9GlobalAdmission::for_test(8, 4);
        let first_connection = global.connection_for_test(8, 4);
        let second_connection = global.connection_for_test(8, 4);
        let first = first_connection.admit_request(5, 3).await.unwrap();

        assert!(
            tokio::time::timeout(QUIET_TIMEOUT, second_connection.admit_request(1, 0))
                .await
                .is_err(),
            "connections must share the process request-byte budget"
        );

        drop(first);
        tokio::time::timeout(TEST_TIMEOUT, second_connection.admit_request(1, 0))
            .await
            .expect("released global bytes must unblock another connection")
            .unwrap();
    }

    #[tokio::test]
    async fn shutdown_cancels_blocked_request_admission_without_leaking_permits() {
        let global = P9GlobalAdmission::for_test(8, 2);
        let first_connection = global.connection_for_test(8, 1);
        let blocked_connection = global.connection_for_test(8, 1);
        let first = first_connection.admit_request(8, 0).await.unwrap();
        let shutdown = CancellationToken::new();
        let blocked_shutdown = shutdown.clone();
        let blocked = tokio::spawn(async move {
            blocked_connection
                .admit_request_or_shutdown(1, 0, &blocked_shutdown)
                .await
        });
        tokio::task::yield_now().await;
        shutdown.cancel();

        assert!(
            tokio::time::timeout(TEST_TIMEOUT, blocked)
                .await
                .expect("shutdown-aware admission must return")
                .unwrap()
                .is_err()
        );
        drop(first);
        assert_eq!(
            global.snapshot(),
            P9AdmissionSnapshot {
                reserved_bytes: 0,
                active_requests: 0,
                active_transports: 0,
                receive_reserved_bytes: 0,
            }
        );
    }

    #[tokio::test]
    async fn request_task_limit_bounds_tiny_frames() {
        let global = P9GlobalAdmission::for_test(128, 1);
        let connection = global.connection_for_test(128, 1);
        let first = connection.admit_request(1, 0).await.unwrap();

        assert!(
            tokio::time::timeout(QUIET_TIMEOUT, connection.admit_request(1, 0))
                .await
                .is_err(),
            "small frames must not bypass the request-task bound"
        );

        drop(first);
        tokio::time::timeout(TEST_TIMEOUT, connection.admit_request(1, 0))
            .await
            .expect("released task capacity must admit the next frame")
            .unwrap();
    }

    #[tokio::test]
    async fn queued_response_holds_byte_admission_until_consumed() {
        let global = P9GlobalAdmission::for_test(8, 4);
        let connection = global.connection_for_test(8, 4);
        let admitted = connection.admit_request(1, 7).await.unwrap();
        let response = P9Response::new(7, vec![0; 7], admitted);

        assert!(
            tokio::time::timeout(QUIET_TIMEOUT, connection.admit_request(1, 0))
                .await
                .is_err(),
            "queued response bytes must remain charged until the writer consumes them"
        );

        drop(response);
        tokio::time::timeout(TEST_TIMEOUT, connection.admit_request(1, 0))
            .await
            .expect("consuming a response must release its byte charge")
            .unwrap();
    }

    #[tokio::test]
    async fn response_flush_releases_admission_exactly_once() {
        let global = P9GlobalAdmission::for_test(32, 1);
        let connection = global.connection_for_test(32, 1);
        let admitted = connection.admit_request(8, 8).await.unwrap();
        let response = P9Response::new(7, vec![0; 8], admitted);
        let (tx, rx) = mpsc::channel(1);
        tx.send(response).await.unwrap();
        drop(tx);
        let (mut client, server) = tokio::io::duplex(64);
        let writer = spawn_response_writer(
            server,
            rx,
            ResponseAuthority::always(),
            CancellationToken::new(),
        );

        drain_writer(writer, "response writer must flush").await;
        let mut bytes = [0; 8];
        client.read_exact(&mut bytes).await.unwrap();
        assert_eq!(
            global.snapshot(),
            P9AdmissionSnapshot {
                reserved_bytes: 0,
                active_requests: 0,
                active_transports: 0,
                receive_reserved_bytes: 0,
            },
            "transport completion must release both byte and request permits once"
        );
    }

    #[tokio::test]
    async fn reconnect_storm_cannot_exceed_the_global_budget() {
        let global = P9GlobalAdmission::for_test(16, 2);
        let connections = (0..32)
            .map(|_| global.connection_for_test(8, 1))
            .collect::<Vec<_>>();
        let mut admitted = Vec::new();

        for connection in &connections {
            if let Ok(Ok(permit)) =
                tokio::time::timeout(Duration::from_millis(1), connection.admit_request(5, 3)).await
            {
                admitted.push(permit);
            }
        }

        assert_eq!(admitted.len(), 2);
        assert_eq!(
            global.snapshot(),
            P9AdmissionSnapshot {
                reserved_bytes: 16,
                active_requests: 2,
                active_transports: 0,
                receive_reserved_bytes: 0,
            }
        );
    }

    #[tokio::test]
    async fn reconnect_waiters_are_bounded_before_connection_local_admission() {
        let global = P9GlobalAdmission::for_test(8, 2);
        let first_connection = global.connection_for_test(8, 1);
        let waiting_connection = global.connection_for_test(8, 1);
        let blocked_connection = global.connection_for_test(8, 1);
        let first = first_connection.admit_request(8, 0).await.unwrap();
        let mut waiting = Box::pin(waiting_connection.admit_request(8, 0));

        tokio::select! {
            _ = &mut waiting => panic!("byte-saturated admission completed"),
            _ = tokio::task::yield_now() => {}
        }
        assert_eq!(global.snapshot().active_requests, 2);

        assert!(
            tokio::time::timeout(QUIET_TIMEOUT, blocked_connection.admit_request(1, 0))
                .await
                .is_err(),
            "a reconnect cannot queue past the process-wide request slots"
        );

        drop(first);
        let admitted = tokio::time::timeout(TEST_TIMEOUT, waiting)
            .await
            .expect("the queued request must advance when bytes are released")
            .unwrap();
        drop(admitted);
        assert_eq!(
            global.snapshot(),
            P9AdmissionSnapshot {
                reserved_bytes: 0,
                active_requests: 0,
                active_transports: 0,
                receive_reserved_bytes: 0,
            }
        );
    }

    #[tokio::test]
    async fn disconnect_settlement_waits_for_tracked_requests() {
        let requests = TaskTracker::new();
        let release = Arc::new(Notify::new());
        let task_release = Arc::clone(&release);
        requests.spawn(async move {
            task_release.notified().await;
        });

        let mut settlement = tokio::spawn(settle_request_tasks(requests));
        assert!(
            tokio::time::timeout(QUIET_TIMEOUT, &mut settlement)
                .await
                .is_err(),
            "disconnect settlement must keep accepted request work alive"
        );

        release.notify_waiters();
        tokio::time::timeout(TEST_TIMEOUT, settlement)
            .await
            .expect("settled requests must release the disconnected session")
            .unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn timed_out_settlement_transfers_to_process_ownership() {
        let requests = TaskTracker::new();
        let accepted_work = P9AcceptedWorkTracker::new();
        let accepted_guard = accepted_work.try_accept().unwrap();
        let release = Arc::new(Notify::new());
        let task_release = Arc::clone(&release);
        let (finished_tx, mut finished_rx) = oneshot::channel();
        requests.spawn(async move {
            let _accepted_guard = accepted_guard;
            task_release.notified().await;
            let _ = finished_tx.send(());
        });

        let settlement = tokio::spawn(settle_request_tasks(requests));
        tokio::task::yield_now().await;
        tokio::time::advance(CLIENT_DRAIN_TIMEOUT + Duration::from_millis(1)).await;
        settlement
            .await
            .expect("connection settlement must return after its bound");
        assert!(
            finished_rx.try_recv().is_err(),
            "accepted work must not be aborted at the connection deadline"
        );
        assert_eq!(accepted_work.len(), 1);
        accepted_work.stop_accepting();
        let mut process_settlement = tokio::spawn({
            let accepted_work = accepted_work.clone();
            async move { accepted_work.wait().await }
        });
        assert!(
            tokio::time::timeout(QUIET_TIMEOUT, &mut process_settlement)
                .await
                .is_err(),
            "process shutdown must retain accepted work ownership"
        );

        release.notify_waiters();
        tokio::time::timeout(TEST_TIMEOUT, finished_rx)
            .await
            .expect("process-owned accepted work must finish")
            .unwrap();
        tokio::time::timeout(TEST_TIMEOUT, process_settlement)
            .await
            .expect("process ownership must drain after accepted work finishes")
            .unwrap();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn process_shutdown_cancels_dispatched_request_before_settlement_wait() {
        let filesystem = Arc::new(ZeroFS::new_in_memory().await.unwrap());
        let handler = Arc::new(NinePHandler::new(
            filesystem,
            Arc::new(FileLockManager::new()),
        ));
        let accepted_work = P9AcceptedWorkTracker::new();
        let byte_limit = P9_MAX_MSIZE as usize * 2;
        let global = P9GlobalAdmission::for_test(byte_limit, 2);
        let admission = global.connection_with_accepted_work(accepted_work.clone());
        let requests = TaskTracker::new();
        let (tx, _rx) = mpsc::channel(1);
        let shutdown = CancellationToken::new();
        let (request_reached, release_request) = pause_success_terminal_dedup([0; 16]);

        dispatch_9p_frame(
            frame(
                1,
                Message::Tversion(Tversion {
                    msize: P9_MAX_MSIZE,
                    version: P9String::new(VERSION_9P2000L.to_vec()),
                }),
            ),
            &handler,
            &tx,
            &InflightRegistry::default(),
            &admission,
            &requests,
            &shutdown,
        )
        .await
        .unwrap();
        tokio::time::timeout(TEST_TIMEOUT, request_reached)
            .await
            .expect("dispatched request must reach its controlled pause")
            .unwrap();
        assert_eq!(accepted_work.len(), 1);

        shutdown.cancel();
        requests.close();
        let settlement = tokio::time::timeout(QUIET_TIMEOUT, requests.wait()).await;
        if settlement.is_err() {
            let _ = release_request.send(());
            requests.wait().await;
        }
        accepted_work.stop_accepting();
        let process_settlement = tokio::time::timeout(TEST_TIMEOUT, accepted_work.wait()).await;

        assert!(
            settlement.is_ok(),
            "process shutdown must cancel a dispatched request before waiting for its ownership token"
        );
        process_settlement.expect("process ownership must drain after request cancellation");
    }

    #[tokio::test]
    async fn sealed_process_tracker_rejects_late_acceptance() {
        let accepted_work = P9AcceptedWorkTracker::new();
        let guard = accepted_work.try_accept().unwrap();
        accepted_work.stop_accepting();
        assert!(accepted_work.try_accept().is_none());

        let mut settlement = tokio::spawn({
            let accepted_work = accepted_work.clone();
            async move { accepted_work.wait().await }
        });
        assert!(
            tokio::time::timeout(QUIET_TIMEOUT, &mut settlement)
                .await
                .is_err()
        );
        drop(guard);
        tokio::time::timeout(TEST_TIMEOUT, settlement)
            .await
            .expect("sealing must wait only for work accepted before the cutoff")
            .unwrap();
    }

    struct DropTrackedFrame {
        bytes: Vec<u8>,
        dropped: Option<oneshot::Sender<()>>,
    }

    impl AsRef<[u8]> for DropTrackedFrame {
        fn as_ref(&self) -> &[u8] {
            &self.bytes
        }
    }

    impl Drop for DropTrackedFrame {
        fn drop(&mut self) {
            if let Some(dropped) = self.dropped.take() {
                let _ = dropped.send(());
            }
        }
    }

    fn tracked_retry_write_frame(
        tag: u16,
        op_id: [u8; P9_OP_ID_LEN],
        count: usize,
    ) -> (Bytes, oneshot::Receiver<()>) {
        let encoded = P9Message::new_with_op_id_flags_and_origin(
            tag,
            op_id,
            P9_OP_FLAG_RETRY,
            0,
            Message::Twrite(Twrite {
                fid: u32::MAX,
                offset: 0,
                count: count as u32,
                data: DekuBytes::from(Bytes::from(vec![0x5a; count])),
            }),
        )
        .to_bytes_ctx(true)
        .unwrap();
        let (dropped_tx, dropped_rx) = oneshot::channel();
        (
            Bytes::from_owner(DropTrackedFrame {
                bytes: encoded,
                dropped: Some(dropped_tx),
            }),
            dropped_rx,
        )
    }

    #[tokio::test(flavor = "current_thread")]
    async fn duplicate_write_retry_drops_payload_before_waiting_for_first() {
        let filesystem = Arc::new(ZeroFS::new_in_memory().await.unwrap());
        let handler = Arc::new(NinePHandler::new(
            Arc::clone(&filesystem),
            Arc::new(FileLockManager::new()),
        ));
        negotiate(&handler).await;

        let op_id = [0x72; 16];
        let first = filesystem
            .dedup
            .reserve_initial(op_id)
            .expect("reserve the original mutation");
        let (frame, dropped_rx) = tracked_retry_write_frame(7, op_id, 1024 * 1024);
        let reserved = frame.len() + P9_MAX_MSIZE as usize;
        let global = P9GlobalAdmission::for_test(reserved, 1);
        let admission = global.connection_for_test(reserved, 1);
        let requests = TaskTracker::new();
        let (tx, mut rx) = mpsc::channel(1);
        let inflight = InflightRegistry::default();

        dispatch_9p_frame(
            frame,
            &handler,
            &tx,
            &inflight,
            &admission,
            &requests,
            &CancellationToken::new(),
        )
        .await
        .unwrap();
        tokio::time::timeout(TEST_TIMEOUT, dropped_rx)
            .await
            .expect("retry payload allocation must be dropped while the first remains in flight")
            .unwrap();

        filesystem.dedup.record_entry(crate::dedup::DedupEntry {
            op_id,
            result: crate::dedup::DedupResult::Write {
                attrs: crate::fs::types::FileAttributes::default(),
            },
        });
        drop(first);
        let response = tokio::time::timeout(TEST_TIMEOUT, rx.recv())
            .await
            .expect("retry must replay after the first completes")
            .expect("response channel");
        let (_, response_bytes) = response.into_parts();
        let response = decode(&response_bytes);
        assert!(matches!(
            response.body,
            Message::Rwrite(ninep_proto::Rwrite { count }) if count == 1024 * 1024
        ));

        requests.close();
        requests.wait().await;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn shallow_write_retry_rejects_a_mismatched_completed_result() {
        let filesystem = Arc::new(ZeroFS::new_in_memory().await.unwrap());
        let handler = Arc::new(NinePHandler::new(
            Arc::clone(&filesystem),
            Arc::new(FileLockManager::new()),
        ));
        negotiate(&handler).await;

        let op_id = [0x73; 16];
        let first = filesystem
            .dedup
            .reserve_initial(op_id)
            .expect("reserve the original mutation");
        let (frame, dropped_rx) = tracked_retry_write_frame(8, op_id, 64 * 1024);
        let reserved = frame.len() + P9_MAX_MSIZE as usize;
        let global = P9GlobalAdmission::for_test(reserved, 1);
        let admission = global.connection_for_test(reserved, 1);
        let requests = TaskTracker::new();
        let (tx, mut rx) = mpsc::channel(1);

        dispatch_9p_frame(
            frame,
            &handler,
            &tx,
            &InflightRegistry::default(),
            &admission,
            &requests,
            &CancellationToken::new(),
        )
        .await
        .unwrap();
        tokio::time::timeout(TEST_TIMEOUT, dropped_rx)
            .await
            .expect("mismatched retry payload must still be released before joining")
            .unwrap();

        filesystem.dedup.record_entry(crate::dedup::DedupEntry {
            op_id,
            result: crate::dedup::DedupResult::Mkdir {
                inode_id: 0,
                attrs: crate::fs::types::FileAttributes::default(),
            },
        });
        drop(first);
        let response = tokio::time::timeout(TEST_TIMEOUT, rx.recv())
            .await
            .expect("mismatched retry response")
            .expect("response channel");
        assert!(matches!(
            decode(&response.into_parts().1).body,
            Message::Rlerror(Rlerror {
                ecode: ninep_proto::P9_EOPIDSTALE
            })
        ));

        requests.close();
        requests.wait().await;
    }

    fn frame(tag: u16, body: Message) -> Bytes {
        Bytes::from(P9Message::new(tag, body).to_bytes().unwrap())
    }

    fn decode(bytes: &[u8]) -> P9Message {
        P9Message::from_bytes_ctx(bytes, false).unwrap()
    }

    async fn in_memory_handler() -> (Arc<ZeroFS>, Arc<NinePHandler>) {
        let filesystem = Arc::new(ZeroFS::new_in_memory().await.unwrap());
        let handler = Arc::new(NinePHandler::new(
            Arc::clone(&filesystem),
            Arc::new(FileLockManager::new()),
        ));
        (filesystem, handler)
    }

    #[tokio::test]
    async fn server_builder_carries_shared_identity_to_client_handlers() {
        let filesystem = Arc::new(ZeroFS::new_in_memory().await.unwrap());
        let server = NinePServer::new(filesystem, "127.0.0.1:0".parse().unwrap())
            .with_credential_override(501, 20);

        assert_eq!(server.credential_override, Some((501, 20)));
    }

    async fn exchange_client_frame(
        client: &mut tokio::io::DuplexStream,
        request: Vec<u8>,
    ) -> P9Message {
        client.write_all(&request).await.unwrap();
        let mut size = [0_u8; P9_SIZE_FIELD_LEN];
        client.read_exact(&mut size).await.unwrap();
        let total = u32::from_le_bytes(size) as usize;
        let mut response = Vec::with_capacity(total);
        response.extend_from_slice(&size);
        response.resize(total, 0);
        client
            .read_exact(&mut response[P9_SIZE_FIELD_LEN..])
            .await
            .unwrap();
        decode(&response)
    }

    async fn exchange_client_message(
        client: &mut tokio::io::DuplexStream,
        tag: u16,
        body: Message,
    ) -> P9Message {
        exchange_client_frame(client, P9Message::new(tag, body).to_bytes().unwrap()).await
    }

    #[tokio::test]
    async fn spawned_client_handler_applies_shared_identity_to_mutations() {
        let filesystem = Arc::new(ZeroFS::new_in_memory().await.unwrap());
        let server = NinePServer::new(Arc::clone(&filesystem), "127.0.0.1:0".parse().unwrap())
            .with_credential_override(501, 20);
        let (mut client, server_stream) = tokio::io::duplex(4096);
        let (read_stream, write_stream) = tokio::io::split(server_stream);
        let shutdown = CancellationToken::new();
        let accepted_work = P9AcceptedWorkTracker::new();
        let task = server.spawn_client_handler(
            read_stream,
            write_stream,
            &shutdown,
            "shared-identity-test".to_string(),
            P9SessionAdmission {
                transport: P9GlobalAdmission::shared().try_admit_transport().unwrap(),
                transport_label: "test",
                accepted_work: accepted_work.clone(),
            },
        );

        let version = exchange_client_message(
            &mut client,
            0,
            Message::Tversion(Tversion {
                msize: super::super::handler::DEFAULT_MSIZE,
                version: P9String::new(VERSION_9P2000L_ZEROFS.to_vec()),
            }),
        )
        .await;
        assert!(matches!(version.body, Message::Rversion(_)));
        let attach = exchange_client_message(
            &mut client,
            1,
            Message::Tattach(Tattach {
                fid: 1,
                afid: u32::MAX,
                uname: P9String::new(b"untrusted-client".to_vec()),
                aname: P9String::new(b"/".to_vec()),
                n_uname: 9_000,
            }),
        )
        .await;
        assert!(matches!(attach.body, Message::Rattach(_)));
        let mkdir = exchange_client_frame(
            &mut client,
            P9Message::new_with_op_id(
                2,
                [0x51; P9_OP_ID_LEN],
                Message::Tmkdir(Tmkdir {
                    dfid: 1,
                    name: P9String::new(b"shared-owner".to_vec()),
                    mode: 0o755,
                    gid: 9_000,
                }),
            )
            .to_bytes_ctx(true)
            .unwrap(),
        )
        .await;
        assert!(
            matches!(mkdir.body, Message::Rmkdir(_)),
            "shared-identity mkdir failed: {:?}",
            mkdir.body
        );

        let root = Credentials {
            uid: 0,
            gid: 0,
            gid_known: true,
            groups: [0; 16],
            groups_count: 0,
            groups_complete: true,
        };
        let inode_id = filesystem.lookup(&root, 0, b"shared-owner").await.unwrap();
        let Inode::Directory(inode) = filesystem.inode_store.get(inode_id).await.unwrap() else {
            panic!("shared-owner must be a directory");
        };
        assert_eq!((inode.uid, inode.gid), (501, 20));

        shutdown.cancel();
        drop(client);
        tokio::time::timeout(TEST_TIMEOUT, task)
            .await
            .expect("client handler shutdown")
            .unwrap();
        accepted_work.stop_accepting();
        accepted_work.wait().await;
    }

    async fn negotiate(handler: &NinePHandler) {
        let response = handler
            .handle_message(
                0,
                Message::Tversion(Tversion {
                    msize: super::super::handler::DEFAULT_MSIZE,
                    version: P9String::new(VERSION_9P2000L_ZEROFS.to_vec()),
                }),
            )
            .await;
        assert!(matches!(response.body, Message::Rversion(_)));
    }

    async fn establish_session(
        handler: &NinePHandler,
        attach_tag: u16,
        user: &[u8],
        uid: u32,
        op_id: Option<[u8; 16]>,
    ) {
        negotiate(handler).await;
        let request = Message::Tattach(Tattach {
            fid: 1,
            afid: u32::MAX,
            uname: P9String::new(user.to_vec()),
            aname: P9String::new(b"/".to_vec()),
            n_uname: uid,
        });
        let response = match op_id {
            Some(op_id) => {
                handler
                    .handle_message_with_op_id(attach_tag, op_id, request)
                    .await
            }
            None => handler.handle_message(attach_tag, request).await,
        };
        assert!(matches!(response.body, Message::Rattach(_)));
    }

    struct DispatchFixture {
        handler: Arc<NinePHandler>,
        tx: mpsc::Sender<P9Response>,
        rx: mpsc::Receiver<P9Response>,
        inflight: InflightRegistry,
        admission: P9ConnectionAdmission,
        requests: TaskTracker,
    }

    impl DispatchFixture {
        fn new(handler: Arc<NinePHandler>, capacity: usize) -> Self {
            let (tx, rx) = mpsc::channel(capacity);
            let byte_limit = P9_MAX_MSIZE as usize * 64;
            let global = P9GlobalAdmission::for_test(byte_limit, 64);
            Self {
                handler,
                tx,
                rx,
                inflight: InflightRegistry::default(),
                admission: global.connection_for_test(byte_limit, 64),
                requests: TaskTracker::new(),
            }
        }

        async fn in_memory(capacity: usize) -> Self {
            let (_, handler) = in_memory_handler().await;
            Self::new(handler, capacity)
        }

        async fn dispatch(&self, tag: u16, body: Message) {
            self.dispatch_frame(frame(tag, body)).await;
        }

        async fn dispatch_frame(&self, bytes: Bytes) {
            dispatch_9p_frame(
                bytes,
                &self.handler,
                &self.tx,
                &self.inflight,
                &self.admission,
                &self.requests,
                &CancellationToken::new(),
            )
            .await
            .unwrap();
        }

        async fn recv(&mut self, context: &str) -> (u16, P9Message) {
            let response = tokio::time::timeout(TEST_TIMEOUT, self.rx.recv())
                .await
                .expect(context)
                .expect("response channel");
            let (tag, bytes) = response.into_parts();
            (tag, decode(&bytes))
        }

        async fn recv_flush(&mut self, context: &str) -> u16 {
            let (tag, response) = self.recv(context).await;
            assert!(matches!(response.body, Message::Rflush(Rflush)));
            tag
        }

        async fn expect_quiet(&mut self, message: &str) {
            assert!(
                tokio::time::timeout(QUIET_TIMEOUT, self.rx.recv())
                    .await
                    .is_err(),
                "{message}"
            );
        }
    }

    async fn expect_completion(waiter: CompletionWaiter, message: &str) {
        tokio::time::timeout(TEST_TIMEOUT, waiter.wait())
            .await
            .expect(message);
    }

    async fn expect_pending(waiter: &CompletionWaiter, message: &str) {
        let probe = Arc::clone(waiter);
        assert!(
            tokio::time::timeout(QUIET_TIMEOUT, probe.wait())
                .await
                .is_err(),
            "{message}"
        );
    }

    async fn drain_writer(writer: AbortOnDropHandle<()>, message: &str) {
        join_with_timeout(writer, TEST_TIMEOUT)
            .await
            .expect(message)
            .expect("writer task");
    }

    async fn response_queue<const N: usize>(
        responses: [(u16, Vec<u8>); N],
    ) -> mpsc::Receiver<P9Response> {
        let (tx, rx) = mpsc::channel(N);
        let byte_limit = responses
            .iter()
            .map(|(_, bytes)| bytes.len())
            .sum::<usize>()
            .max(1);
        let global = P9GlobalAdmission::for_test(byte_limit, N.max(1));
        let admission = global.connection_for_test(byte_limit, N.max(1));
        for (tag, bytes) in responses {
            let permit = admission.admit_request(0, bytes.len()).await.unwrap();
            tx.try_send(P9Response::new(tag, bytes, permit)).unwrap();
        }
        rx
    }

    async fn test_response(tag: u16, bytes: Vec<u8>) -> P9Response {
        let byte_limit = bytes.len().max(1);
        let global = P9GlobalAdmission::for_test(byte_limit, 1);
        let admission = global.connection_for_test(byte_limit, 1);
        let permit = admission.admit_request(0, bytes.len()).await.unwrap();
        P9Response::new(tag, bytes, permit)
    }

    fn not_leader(tag: u16) -> Vec<u8> {
        frame(
            tag,
            Message::Rlerror(Rlerror {
                ecode: ninep_proto::P9_ENOTLEADER,
            }),
        )
        .to_vec()
    }

    async fn race_lopenat_with(
        followup_tag: u16,
        followup: Message,
        quiet_message: &str,
        response_context: &str,
    ) -> (Arc<ZeroFS>, DispatchFixture, P9Message, P9Message) {
        let (filesystem, handler) = in_memory_handler().await;
        establish_session(&handler, 0, b"test", 1000, Some([0; 16])).await;

        // Pause before `Tlopenat` installs `newfid`.
        let inode_lock = filesystem.lock_manager.acquire(0).await;
        let mut io = DispatchFixture::new(handler, 4);
        io.dispatch(
            10,
            Message::Tlopenat(Tlopenat {
                fid: 1,
                newfid: 2,
                flags: libc::O_RDONLY as u32,
            }),
        )
        .await;
        io.dispatch(followup_tag, followup).await;
        io.expect_quiet(quiet_message).await;
        drop(inode_lock);

        let (_, open) = io.recv("open response").await;
        let (_, followup) = io.recv(response_context).await;
        (filesystem, io, open, followup)
    }

    async fn leased_filesystem_for_response_gate() -> (
        Arc<ZeroFS>,
        Arc<crate::replication::Lease>,
        Arc<slatedb::Db>,
    ) {
        let object_store: Arc<dyn slatedb::object_store::ObjectStore> =
            Arc::new(slatedb::object_store::memory::InMemory::new());
        let raw_db = Arc::new(
            slatedb::DbBuilder::new(
                slatedb::object_store::path::Path::from("ninep-response-authority"),
                object_store,
            )
            .build()
            .await
            .unwrap(),
        );
        let lease = crate::replication::Lease::new();
        assert!(lease.activate_from(tokio::time::Instant::now(), Duration::from_secs(30)));

        // `Tflush` does not access the database; the response gate is sufficient.
        let mut filesystem = ZeroFS::new_in_memory().await.unwrap();
        filesystem.db =
            Arc::new(crate::db::Db::new(Arc::clone(&raw_db), None).with_lease(Arc::clone(&lease)));
        (Arc::new(filesystem), lease, raw_db)
    }

    struct DropSignal(Option<oneshot::Sender<()>>);

    impl Drop for DropSignal {
        fn drop(&mut self) {
            if let Some(signal) = self.0.take() {
                let _ = signal.send(());
            }
        }
    }

    struct RevokeOnFirstWrite<W> {
        inner: W,
        lease: Arc<crate::replication::Lease>,
        revoked: bool,
    }

    impl<W: AsyncWrite + Unpin> AsyncWrite for RevokeOnFirstWrite<W> {
        fn poll_write(
            mut self: std::pin::Pin<&mut Self>,
            cx: &mut std::task::Context<'_>,
            buf: &[u8],
        ) -> std::task::Poll<std::io::Result<usize>> {
            if !self.revoked {
                self.lease.revoke();
                self.revoked = true;
            }
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

    struct CountWrites<W> {
        inner: W,
        writes: Arc<AtomicUsize>,
    }

    impl<W: AsyncWrite + Unpin> AsyncWrite for CountWrites<W> {
        fn poll_write(
            self: std::pin::Pin<&mut Self>,
            cx: &mut std::task::Context<'_>,
            buf: &[u8],
        ) -> std::task::Poll<std::io::Result<usize>> {
            let this = self.get_mut();
            match std::pin::Pin::new(&mut this.inner).poll_write(cx, buf) {
                std::task::Poll::Ready(Ok(written)) => {
                    if written != 0 {
                        this.writes.fetch_add(1, Ordering::Relaxed);
                    }
                    std::task::Poll::Ready(Ok(written))
                }
                result => result,
            }
        }

        fn poll_flush(
            self: std::pin::Pin<&mut Self>,
            cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<std::io::Result<()>> {
            std::pin::Pin::new(&mut self.get_mut().inner).poll_flush(cx)
        }

        fn poll_shutdown(
            self: std::pin::Pin<&mut Self>,
            cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<std::io::Result<()>> {
            std::pin::Pin::new(&mut self.get_mut().inner).poll_shutdown(cx)
        }
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn accepted_tcp_stream_has_bounded_liveness_options() {
        let listener = TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0))
            .await
            .expect("bind loopback listener");
        let address = listener.local_addr().expect("listener address");
        let (client, accepted) =
            tokio::join!(tokio::net::TcpStream::connect(address), listener.accept());
        let _client = client.expect("connect loopback client");
        let (server, _) = accepted.expect("accept loopback client");

        configure_accepted_tcp_stream(&server).expect("configure accepted TCP stream");

        let socket = socket2::SockRef::from(&server);
        assert!(server.nodelay().expect("TCP_NODELAY"));
        assert!(socket.keepalive().expect("SO_KEEPALIVE"));
        assert_eq!(
            socket.tcp_keepalive_time().expect("TCP_KEEPIDLE"),
            TCP_KEEPALIVE_IDLE
        );
        assert_eq!(
            socket.tcp_keepalive_interval().expect("TCP_KEEPINTVL"),
            TCP_KEEPALIVE_INTERVAL
        );
        assert_eq!(
            socket.tcp_keepalive_retries().expect("TCP_KEEPCNT"),
            TCP_KEEPALIVE_RETRIES
        );
        assert_eq!(
            socket.tcp_user_timeout().expect("TCP_USER_TIMEOUT"),
            Some(TCP_USER_TIMEOUT)
        );
    }

    #[tokio::test]
    async fn listener_shutdown_completes_with_an_idle_accepted_client() {
        let filesystem = Arc::new(ZeroFS::new_in_memory().await.unwrap());
        let directory = tempfile::tempdir().unwrap();
        let socket = directory.path().join("ninep-shutdown.sock");
        let server = NinePServer::new_unix(filesystem, socket.clone());
        let shutdown = CancellationToken::new();
        let server_shutdown = shutdown.clone();
        let task = tokio::spawn(async move { server.start(server_shutdown).await });

        let client = loop {
            match tokio::net::UnixStream::connect(&socket).await {
                Ok(client) => break client,
                Err(_) => tokio::task::yield_now().await,
            }
        };
        shutdown.cancel();
        tokio::time::timeout(TEST_TIMEOUT, task)
            .await
            .expect("listener and accepted client must stop after shutdown")
            .unwrap()
            .unwrap();
        drop(client);
    }

    #[tokio::test(start_paused = true)]
    async fn timed_join_aborts_and_awaits_the_task_before_returning() {
        let (started_tx, started_rx) = oneshot::channel();
        let (dropped_tx, mut dropped_rx) = oneshot::channel();
        let task = AbortOnDropHandle::new(spawn_named("timed-abort-test", async move {
            let _drop_signal = DropSignal(Some(dropped_tx));
            let _ = started_tx.send(());
            std::future::pending::<()>().await;
        }));
        started_rx.await.expect("task started");

        assert!(
            join_with_timeout(task, TEST_TIMEOUT).await.is_none(),
            "a stuck task must take the timeout path"
        );
        dropped_rx
            .try_recv()
            .expect("the aborted task must be joined before timeout returns");
    }

    #[tokio::test]
    async fn single_response_flushes_buffered_transport_before_writer_drain_returns() {
        let (mut client, server) = tokio::io::duplex(4096);
        // Exercise the single-response fast path with an underlying writer
        // whose write_all only fills its own buffer. Dropping it without an
        // explicit flush would discard the response.
        let buffered_server = tokio::io::BufWriter::with_capacity(4096, server);
        let response = frame(21, Message::Rflush(Rflush)).to_vec();
        let writer = spawn_response_writer(
            buffered_server,
            response_queue([(21, response.clone())]).await,
            ResponseAuthority::always(),
            CancellationToken::new(),
        );

        drain_writer(writer, "writer must drain before the deadline").await;
        let mut received = vec![0; response.len()];
        client
            .read_exact(&mut received)
            .await
            .expect("queued response bytes");
        assert_eq!(received, response);
        assert!(matches!(decode(&received).body, Message::Rflush(Rflush)));

        let mut trailing = [0];
        assert_eq!(
            client.read(&mut trailing).await.expect("writer EOF"),
            0,
            "a completed drain must drop the write half before returning"
        );
    }

    #[tokio::test]
    async fn already_queued_responses_share_one_transport_write() {
        let (mut client, server) = tokio::io::duplex(4096);
        let writes = Arc::new(AtomicUsize::new(0));
        let counted_server = CountWrites {
            inner: server,
            writes: Arc::clone(&writes),
        };
        let first = not_leader(31);
        let second = not_leader(32);
        let writer = spawn_response_writer(
            counted_server,
            response_queue([(31, first.clone()), (32, second.clone())]).await,
            ResponseAuthority::always(),
            CancellationToken::new(),
        );

        drain_writer(writer, "coalesced response writer must drain").await;
        let mut received = Vec::new();
        client.read_to_end(&mut received).await.unwrap();
        assert_eq!(received, [first, second].concat());
        assert_eq!(
            writes.load(Ordering::Relaxed),
            1,
            "both small responses should occupy one BufWriter flush"
        );
    }

    #[tokio::test]
    async fn revocation_filters_success_not_errors() {
        let (filesystem, lease, raw_db) = leased_filesystem_for_response_gate().await;
        let handler = NinePHandler::new(Arc::clone(&filesystem), Arc::new(FileLockManager::new()));

        // Revoke authority after response serialization and before transport write.
        let completed = handler
            .handle_message(41, Message::Tflush(Tflush { oldtag: 40 }))
            .await;
        assert!(matches!(completed.body, Message::Rflush(Rflush)));
        let completed_bytes = completed.to_bytes().unwrap();
        let (mut client, server) = tokio::io::duplex(4096);
        let rx = response_queue([(41, completed_bytes)]).await;
        lease.revoke();

        let connection_shutdown = CancellationToken::new();
        let writer = spawn_response_writer(
            server,
            rx,
            ResponseAuthority::from_database(Arc::clone(&filesystem.db)),
            connection_shutdown.clone(),
        );
        drain_writer(writer, "writer must close after dropping stale success").await;
        assert!(connection_shutdown.is_cancelled());
        let mut received = Vec::new();
        client.read_to_end(&mut received).await.unwrap();
        assert!(
            received.is_empty(),
            "a success completed before revocation must not reach the wire afterward"
        );

        // Protocol errors remain permitted after authority loss.
        let error_bytes = not_leader(42);
        let (mut client, server) = tokio::io::duplex(4096);
        let writer = spawn_response_writer(
            server,
            response_queue([(42, error_bytes.clone())]).await,
            ResponseAuthority::from_database(Arc::clone(&filesystem.db)),
            CancellationToken::new(),
        );
        drain_writer(writer, "error response writer must drain").await;
        let mut received = vec![0; error_bytes.len()];
        client.read_exact(&mut received).await.unwrap();
        assert_eq!(received, error_bytes);
        assert!(matches!(decode(&received).body, Message::Rlerror(_)));

        raw_db.close().await.unwrap();
    }

    #[tokio::test]
    async fn revocation_blocks_implicit_success_flush() {
        let (filesystem, lease, raw_db) = leased_filesystem_for_response_gate().await;
        let first_error = not_leader(51);
        let trailing_error = not_leader(53);
        let buffered_success = frame(
            52,
            Message::Rread(Rread {
                count: 0,
                data: DekuBytes::from(vec![0; RESPONSE_BUFFER_CAPACITY - 17]),
            }),
        )
        .to_vec();
        assert_eq!(buffered_success.len(), RESPONSE_BUFFER_CAPACITY - 6);
        assert!(first_error.len() + buffered_success.len() > RESPONSE_BUFFER_CAPACITY);
        assert!(buffered_success.len() + trailing_error.len() > RESPONSE_BUFFER_CAPACITY);

        let (mut client, server) = tokio::io::duplex(RESPONSE_BUFFER_CAPACITY * 2);
        let write_stream = RevokeOnFirstWrite {
            inner: server,
            lease: Arc::clone(&lease),
            revoked: false,
        };
        let writer = spawn_response_writer(
            write_stream,
            response_queue([
                (51, first_error.clone()),
                (52, buffered_success),
                (53, trailing_error),
            ])
            .await,
            ResponseAuthority::from_database(Arc::clone(&filesystem.db)),
            CancellationToken::new(),
        );
        drain_writer(writer, "writer must close without flushing stale success").await;

        let mut received = Vec::new();
        client.read_to_end(&mut received).await.unwrap();
        assert_eq!(
            received, first_error,
            "the safe error may flush, but the success buffered after revocation must not"
        );
        raw_db.close().await.unwrap();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn received_first_precedes_reconnect_retry() {
        let filesystem = Arc::new(ZeroFS::new_in_memory().await.unwrap());
        let old = Arc::new(NinePHandler::new(
            Arc::clone(&filesystem),
            Arc::new(FileLockManager::new()),
        ));
        let replacement =
            NinePHandler::new(Arc::clone(&filesystem), Arc::new(FileLockManager::new()));

        for handler in [&*old, &replacement] {
            establish_session(handler, 1, b"root", 0, None).await;
        }

        let op_id = [0x63; 16];
        let mkdir = Message::Tmkdir(Tmkdir {
            dfid: 1,
            name: P9String::new(b"survived-disconnect".to_vec()),
            mode: 0o755,
            gid: 0,
        });
        let first = P9Message::new_with_op_id(10, op_id, mkdir.clone())
            .to_bytes_ctx(true)
            .unwrap();
        let (tx, _rx) = mpsc::channel(2);
        let inflight = InflightRegistry::default();
        let byte_limit = P9_MAX_MSIZE as usize * 2;
        let global = P9GlobalAdmission::for_test(byte_limit, 2);
        let admission = global.connection_for_test(byte_limit, 2);
        let requests = TaskTracker::new();

        // The current-thread runtime leaves the received FIRST task unpolled.
        dispatch_9p_frame(
            Bytes::from(first),
            &old,
            &tx,
            &inflight,
            &admission,
            &requests,
            &CancellationToken::new(),
        )
        .await
        .unwrap();
        old.close_for_disconnect();

        // RETRY must wait for the received FIRST and replay its mkdir result.
        let retried = tokio::time::timeout(
            TEST_TIMEOUT,
            replacement.handle_message_with_op_envelope_origin(
                11,
                op_id,
                ninep_proto::P9_OP_FLAG_RETRY,
                0,
                mkdir,
            ),
        )
        .await
        .expect("replacement retry must follow the queued FIRST");
        assert!(
            matches!(retried.body, Message::Rmkdir(_)),
            "the retry must replay success, got {:?}",
            retried.body
        );

        let cached_inode = match filesystem.dedup.get(&op_id) {
            Some(crate::dedup::DedupResult::Mkdir { inode_id, .. }) => inode_id,
            other => panic!("queued FIRST must publish Mkdir, not a fid error: {other:?}"),
        };
        let creds = Credentials {
            uid: 0,
            gid: 0,
            gid_known: true,
            groups: [0; 16],
            groups_count: 0,
            groups_complete: true,
        };
        assert_eq!(
            filesystem
                .lookup(&creds, 0, b"survived-disconnect")
                .await
                .unwrap(),
            cached_inode
        );
    }

    #[tokio::test(start_paused = true)]
    async fn stuck_response_sender_times_out_and_closes_writer() {
        let (mut client, server) = tokio::io::duplex(64);
        let (stuck_tx, rx) = mpsc::channel(1);
        let writer = spawn_response_writer(
            server,
            rx,
            ResponseAuthority::always(),
            CancellationToken::new(),
        );

        assert!(
            join_with_timeout(writer, CLIENT_DRAIN_TIMEOUT)
                .await
                .is_none(),
            "a live sender with no response must hit the bounded abort path"
        );
        let mut byte = [0];
        assert_eq!(
            client.read(&mut byte).await.expect("writer EOF"),
            0,
            "the aborted writer must be dropped before timeout returns"
        );
        drop(stuck_tx);
    }

    #[tokio::test]
    async fn writer_failure_cancels_further_request_dispatch() {
        let (client, server) = tokio::io::duplex(64);
        drop(client);
        let (tx, rx) = mpsc::channel(1);
        let connection_shutdown = CancellationToken::new();
        let writer = spawn_response_writer(
            server,
            rx,
            ResponseAuthority::always(),
            connection_shutdown.clone(),
        );

        tx.send(test_response(21, vec![1, 2, 3]).await)
            .await
            .expect("response enqueue");
        tokio::time::timeout(TEST_TIMEOUT, connection_shutdown.cancelled())
            .await
            .expect("writer exit must stop the connection reader");
        drain_writer(writer, "writer exited").await;
    }

    #[tokio::test]
    async fn session_release_drop_sweeps_byte_range_locks() {
        let filesystem = Arc::new(ZeroFS::new_in_memory().await.unwrap());
        let lock_manager = Arc::new(FileLockManager::new());
        let handler = Arc::new(NinePHandler::new(filesystem, Arc::clone(&lock_manager)));
        let handler_id = handler.handler_id();
        assert!(lock_manager.try_add_lock(
            handler_id,
            FileLock {
                lock_type: LockType::WriteLock,
                start: 0,
                length: 0,
                proc_id: 1,
                client_id: b"cancelled-session".to_vec(),
                fid: 1,
                inode_id: 0,
            },
        ));
        assert!(lock_manager.session_has_locks(handler_id));

        drop(SessionReleaseGuard::new(handler));

        assert!(
            !lock_manager.session_has_locks(handler_id),
            "abnormal session teardown must release byte-range locks"
        );
    }

    #[tokio::test]
    async fn early_completion_is_observable() {
        let inflight = InflightRegistry::default();
        let lease = inflight.register(7, FidFootprint::None).unwrap();
        let waiter = inflight.waiter(7).expect("request is in flight");

        drop(lease);

        expect_completion(
            waiter,
            "the persistent completion predicate must not lose an early completion",
        )
        .await;
    }

    #[tokio::test]
    async fn occupied_tag_is_rejected_and_reusable_after_completion() {
        let inflight = InflightRegistry::default();
        let lease = inflight.register(7, FidFootprint::None).unwrap();
        let waiter = inflight.waiter(7).expect("original request");

        assert!(inflight.register(7, FidFootprint::None).is_err());
        assert!(
            Arc::ptr_eq(&waiter, &inflight.waiter(7).expect("original remains")),
            "duplicate rejection must not replace the original request"
        );
        expect_pending(
            &waiter,
            "duplicate rejection must not complete the original",
        )
        .await;

        drop(lease);
        expect_completion(waiter, "the original request must complete normally").await;

        let replacement = inflight
            .register(7, FidFootprint::None)
            .expect("completed tag is reusable");
        drop(replacement);
    }

    #[test]
    fn response_publication_holds_the_tag_registry_lock() {
        let inflight = InflightRegistry::default();
        let original = inflight.register(7, FidFootprint::None).unwrap();
        original.publish_terminal_response(|| {
            assert!(
                inflight.inner.entries.try_get(&7).is_locked(),
                "response publication must exclude Tflush lookup and tag reuse"
            );
        });
        assert!(inflight.waiter(7).is_none());

        drop(original);
    }

    #[tokio::test]
    async fn backpressured_response_keeps_tag_until_publication() {
        let inflight = InflightRegistry::default();
        let original = inflight.register(7, FidFootprint::None).unwrap();
        let original_waiter = inflight.waiter(7).expect("original request");
        let response = vec![1, 2, 3];
        let (tx, mut rx) = mpsc::channel(1);
        tx.send(test_response(99, vec![0]).await).await.unwrap();
        let global = P9GlobalAdmission::for_test(response.len(), 1);
        let admission = global.connection_for_test(response.len(), 1);
        let response_admission = admission.admit_request(0, response.len()).await.unwrap();

        let mut enqueue = Box::pin(enqueue_terminal_response(
            &tx,
            response.clone(),
            &original,
            response_admission,
        ));
        tokio::select! {
            biased;
            result = &mut enqueue => panic!("full response queue accepted a send: {result:?}"),
            _ = tokio::task::yield_now() => {}
        }
        assert!(
            inflight.register(7, FidFootprint::None).is_err(),
            "queue backpressure must keep the tag occupied until capacity is reserved"
        );
        assert_eq!(rx.recv().await.unwrap().into_parts(), (99, vec![0]));

        tokio::time::timeout(TEST_TIMEOUT, &mut enqueue)
            .await
            .expect("response enqueue")
            .unwrap();
        drop(enqueue);
        assert_eq!(rx.recv().await.unwrap().into_parts(), (7, response));

        let replacement = inflight
            .register(7, FidFootprint::None)
            .expect("the queued response makes its tag reusable");
        expect_pending(
            &original_waiter,
            "response publication need not wait for the old task to reach Drop",
        )
        .await;

        drop(original);
        expect_completion(original_waiter, "the old request must still complete").await;
        assert!(
            Arc::ptr_eq(
                &replacement.state,
                &inflight.waiter(7).expect("replacement remains registered")
            ),
            "dropping the old lease must not remove the reused tag"
        );
        drop(replacement);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn duplicate_wire_tag_stops_the_connection_reader() {
        let (_, handler) = in_memory_handler().await;
        let request = frame(7, Message::Tflush(Tflush { oldtag: 99 }));
        let mut input = Vec::with_capacity(request.len() * 2);
        input.extend_from_slice(&request);
        input.extend_from_slice(&request);

        let (mut client, server) = tokio::io::duplex(4096);
        client.write_all(&input).await.unwrap();
        let (tx, _rx) = mpsc::channel(2);
        let byte_limit = P9_MAX_MSIZE as usize * 4;
        let global = P9GlobalAdmission::for_test(byte_limit, 4);
        let admission = global.connection_for_test(byte_limit, 4);
        let requests = TaskTracker::new();

        let error = handle_client_loop(
            handler,
            server,
            tx,
            CancellationToken::new(),
            &admission,
            &requests,
        )
        .await
        .expect_err("duplicate tag must terminate the reader");
        assert!(error.to_string().contains("reused in-flight 9P tag 7"));

        let mut byte = [0];
        assert_eq!(client.read(&mut byte).await.unwrap(), 0);
    }

    #[tokio::test]
    async fn barrier_snapshot_waits_for_all_prior_tags() {
        let inflight = InflightRegistry::default();
        let first = inflight.register(7, FidFootprint::None).unwrap();
        let second = inflight.register(8, FidFootprint::None).unwrap();
        let (barrier, prior_waiters) = inflight.register_after_prior_snapshot(9).unwrap();
        assert_eq!(prior_waiters.len(), 2);

        drop(first);
        assert!(
            tokio::time::timeout(QUIET_TIMEOUT, async {
                for waiter in &prior_waiters {
                    waiter.wait().await;
                }
            })
            .await
            .is_err(),
            "the barrier must still wait for the second request"
        );

        drop(second);
        for waiter in prior_waiters {
            expect_completion(waiter, "the barrier must observe every prior request").await;
        }
        expect_pending(
            &inflight.waiter(9).expect("barrier request"),
            "prior completion must not retire the barrier itself",
        )
        .await;
        drop(barrier);
    }

    #[tokio::test]
    async fn fid_snapshot_waits_only_for_matching_prior_requests() {
        let inflight = InflightRegistry::default();
        let matching = inflight.register(7, FidFootprint::Two(1, 2)).unwrap();
        let unrelated = inflight.register(8, FidFootprint::One(3)).unwrap();
        let global = inflight.register(9, FidFootprint::All).unwrap();

        let (clunk, waiters) = inflight.register_after_fid_snapshot(10, 2).unwrap();
        assert_eq!(waiters.len(), 2, "same-fid and global requests must wait");
        let unrelated_state = inflight.waiter(8).unwrap();
        assert!(
            waiters
                .iter()
                .all(|waiter| !Arc::ptr_eq(waiter, &unrelated_state)),
            "an unrelated fid must not enter the Tclunk barrier"
        );

        drop(unrelated);
        for waiter in &waiters {
            expect_pending(waiter, "matching requests are still in flight").await;
        }
        drop(matching);
        drop(global);
        for waiter in waiters {
            expect_completion(waiter, "matching request must complete").await;
        }

        let (_, later_waiters) = inflight.register_after_fid_snapshot(11, 2).unwrap();
        assert_eq!(
            later_waiters.len(),
            1,
            "the earlier Tclunk is itself the same-fid tail"
        );
        drop(clunk);
    }

    #[test]
    fn shallow_inspection_does_not_decode_a_write_payload() {
        let op_id = [0x5a; P9_OP_ID_LEN];
        let frame = P9Message::new_with_op_id(
            7,
            op_id,
            Message::Twrite(Twrite {
                fid: 42,
                offset: 0,
                count: u32::MAX,
                data: vec![1, 2, 3, 4].into(),
            }),
        )
        .to_bytes_ctx(true)
        .unwrap();
        assert!(
            P9Message::from_bytes_ctx(&frame, true).is_err(),
            "the declared payload is truncated"
        );

        assert_eq!(
            inspect_frame_metadata(&frame, true),
            ShallowFrameMetadata {
                fids: FidFootprint::One(42),
                received_op: Some((op_id, 0, 0)),
            }
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn malformed_first_releases_reservation() {
        let (filesystem, handler) = in_memory_handler().await;
        negotiate(&handler).await;
        let frame = P9Message::new_with_op_id(
            7,
            [0x6b; P9_OP_ID_LEN],
            Message::Twrite(Twrite {
                fid: 1,
                offset: 0,
                count: u32::MAX,
                data: vec![1, 2, 3, 4].into(),
            }),
        )
        .to_bytes_ctx(true)
        .unwrap();

        let mut io = DispatchFixture::new(handler, 1);
        io.dispatch_frame(Bytes::from(frame)).await;
        assert!(matches!(
            io.recv("malformed request response").await.1.body,
            Message::Rlerror(_)
        ));
        assert_eq!(
            filesystem.dedup.stats().inflight_ids,
            0,
            "parse failure must drop the synchronous FIRST reservation"
        );
    }

    #[test]
    fn rejected_flush_does_not_install_a_tail() {
        let inflight = InflightRegistry::default();
        let occupied = inflight.register(7, FidFootprint::None).unwrap();

        assert!(inflight.register_flush(7, 20).is_err());
        let valid = inflight.register_flush(8, 20).unwrap();
        assert!(valid.predecessor.is_none());

        drop(valid);
        drop(occupied);
    }

    #[tokio::test]
    async fn flush_tail_cleanup_preserves_the_newer_tail() {
        let inflight = InflightRegistry::default();
        let target_lease = inflight.register(20, FidFootprint::None).unwrap();
        let first = inflight.register_flush(21, 20).unwrap();
        let second = inflight.register_flush(22, 20).unwrap();

        drop(first.lease);
        let third = inflight.register_flush(23, 20).unwrap();
        let third_predecessor = third
            .predecessor
            .expect("the second flush must remain the chain tail");

        drop(target_lease);
        expect_pending(
            &third_predecessor,
            "retiring the old flush must not let a later flush bypass its predecessor",
        )
        .await;

        drop(second.lease);
        expect_completion(
            third_predecessor,
            "completing the actual predecessor releases the chain",
        )
        .await;
        drop(third.lease);
    }

    #[tokio::test]
    async fn flush_targets_stay_bound_across_oldtag_reuse() {
        let inflight = InflightRegistry::default();
        let old_target_lease = inflight.register(20, FidFootprint::None).unwrap();
        let first_flush = inflight.register_flush(21, 20).unwrap();
        let first_target = first_flush.target.expect("old target request");

        drop(old_target_lease);
        expect_completion(first_target, "the first flush follows the old target").await;

        let new_target_lease = inflight.register(20, FidFootprint::None).unwrap();
        let second_flush = inflight.register_flush(22, 20).unwrap();
        let second_target = second_flush.target.expect("new target request");
        let second_predecessor = second_flush
            .predecessor
            .expect("the first flush remains the response-order predecessor");

        drop(first_flush.lease);
        expect_completion(
            second_predecessor,
            "the second flush observes the first flush response",
        )
        .await;
        expect_pending(
            &second_target,
            "the old target must not release a flush of the reused tag",
        )
        .await;

        drop(new_target_lease);
        expect_completion(
            second_target,
            "the second flush follows the replacement target",
        )
        .await;
        drop(second_flush.lease);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn tflush_captures_its_target_before_dispatch_returns() {
        let mut io = DispatchFixture::in_memory(2).await;
        let target_lease = io.inflight.register(20, FidFootprint::None).unwrap();
        io.dispatch(21, Message::Tflush(Tflush { oldtag: 20 }))
            .await;
        io.expect_quiet("Rflush must wait for the captured target generation")
            .await;
        drop(target_lease);

        io.recv_flush("flush response").await;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn concurrent_flushes_each_receive_a_response() {
        let mut io = DispatchFixture::in_memory(2).await;
        let target_lease = io.inflight.register(20, FidFootprint::None).unwrap();

        for tag in [21, 22] {
            io.dispatch(tag, Message::Tflush(Tflush { oldtag: 20 }))
                .await;
        }

        io.expect_quiet("both flushes must wait for the same target generation")
            .await;
        drop(target_lease);

        let mut response_tags = Vec::new();
        for _ in 0..2 {
            response_tags.push(io.recv_flush("flush response").await);
        }
        assert_eq!(response_tags, [21, 22]);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn flushing_a_flush_waits_for_its_response() {
        let mut io = DispatchFixture::in_memory(2).await;
        let target_lease = io.inflight.register(20, FidFootprint::None).unwrap();
        io.dispatch(21, Message::Tflush(Tflush { oldtag: 20 }))
            .await;
        io.dispatch(22, Message::Tflush(Tflush { oldtag: 21 }))
            .await;
        io.expect_quiet("the second flush must observe the first flush as in flight")
            .await;
        drop(target_lease);

        for expected_tag in [21, 22] {
            assert_eq!(io.recv_flush("flush response").await, expected_tag);
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn flush_of_an_unknown_tag_responds_immediately() {
        let mut io = DispatchFixture::in_memory(1).await;
        io.dispatch(21, Message::Tflush(Tflush { oldtag: 20 }))
            .await;

        assert_eq!(io.recv_flush("immediate flush response").await, 21);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn zerofs_tflush_keeps_the_standard_body_offset() {
        let (_, handler) = in_memory_handler().await;
        negotiate(&handler).await;

        let frame = P9Message::new(21, Message::Tflush(Tflush { oldtag: 20 }))
            .to_bytes_ctx(true)
            .unwrap();
        assert_eq!(frame.len(), P9_HEADER_SIZE + 2);
        assert_eq!(&frame[P9_HEADER_SIZE..], 20u16.to_le_bytes());

        let mut io = DispatchFixture::new(handler, 2);
        let target_lease = io.inflight.register(20, FidFootprint::None).unwrap();
        io.dispatch_frame(Bytes::from(frame)).await;

        drop(target_lease);
        io.recv_flush("flush response").await;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn tclunk_does_not_wait_for_an_unrelated_fid() {
        let (filesystem, handler) = in_memory_handler().await;
        establish_session(&handler, 0, b"test", 1000, Some([0; 16])).await;
        let attach = handler
            .handle_message(
                1,
                Message::Tattach(Tattach {
                    fid: 3,
                    afid: u32::MAX,
                    uname: P9String::new(b"test".to_vec()),
                    aname: P9String::new(b"/".to_vec()),
                    n_uname: 1000,
                }),
            )
            .await;
        assert!(matches!(attach.body, Message::Rattach(_)));

        let inode_lock = filesystem.lock_manager.acquire(0).await;
        let mut io = DispatchFixture::new(handler, 4);
        io.dispatch(
            10,
            Message::Tlopenat(Tlopenat {
                fid: 1,
                newfid: 2,
                flags: libc::O_RDONLY as u32,
            }),
        )
        .await;
        io.dispatch(11, Message::Tclunk(Tclunk { fid: 3 })).await;

        let (tag, clunk) = io.recv("unrelated clunk response").await;
        assert_eq!(tag, 11);
        assert!(matches!(clunk.body, Message::Rclunk(Rclunk)));
        io.expect_quiet("the locked open must still be pending")
            .await;

        drop(inode_lock);
        let (tag, open) = io.recv("open response").await;
        assert_eq!(tag, 10);
        assert!(matches!(open.body, Message::Rlopenat(Rlopenat { .. })));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn tclunk_cannot_overtake_an_earlier_lopenat_install() {
        let (filesystem, mut io, open, clunk) = race_lopenat_with(
            11,
            Message::Tclunk(Tclunk { fid: 2 }),
            "Rclunk must wait for the earlier open before removing the fid",
            "clunk response",
        )
        .await;
        assert!(matches!(open.body, Message::Rlopenat(Rlopenat { .. })));
        assert!(matches!(clunk.body, Message::Rclunk(Rclunk)));

        // `Rclunk` precedes both fid removal and handle release.
        io.dispatch(
            12,
            Message::Tgetattr(Tgetattr {
                fid: 2,
                request_mask: GETATTR_ALL,
            }),
        )
        .await;
        assert!(matches!(
            io.recv("getattr response").await.1.body,
            Message::Rlerror(_)
        ));
        assert!(filesystem.open_handles.get(&0).is_none());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn tversion_waits_for_prior_lopenat() {
        let (filesystem, io, open, version) = race_lopenat_with(
            u16::MAX,
            Message::Tversion(Tversion {
                msize: super::super::handler::DEFAULT_MSIZE,
                version: P9String::new(VERSION_9P2000L_ZEROFS.to_vec()),
            }),
            "Rversion must wait for old-session requests before clearing their fids",
            "version response",
        )
        .await;
        assert!(matches!(open.body, Message::Rlopenat(Rlopenat { .. })));
        assert!(matches!(version.body, Message::Rversion(_)));

        let getattr = io
            .handler
            .handle_message(
                12,
                Message::Tgetattr(Tgetattr {
                    fid: 2,
                    request_mask: GETATTR_ALL,
                }),
            )
            .await;
        assert!(matches!(getattr.body, Message::Rlerror(_)));
        assert!(filesystem.open_handles.get(&0).is_none());
    }
}
