use super::handler::ListenerPolicy;
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
#[cfg(test)]
mod create_bench;
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
pub(crate) use admission::{P9AcceptedWorkTracker, P9GlobalAdmission, P9XattrPermit};
#[cfg(test)]
use frame_codec::{
    FidFootprint, P9_RWRITE_MAX_SIZE, REQUEST_STATE_BYTES, ShallowFrameMetadata,
    decoded_request_bytes, frame_admission_bytes, inspect_frame_metadata,
    possible_frame_response_bytes, possible_response_bytes, read_9p_frame,
};
#[cfg(any(feature = "webui", test))]
#[allow(unused_imports)]
pub(crate) use response_writer::{P9Response, response_may_be_emitted};
#[cfg(test)]
use response_writer::{
    RESPONSE_BUFFER_CAPACITY, ResponseAuthority, ResponseCredit, spawn_response_writer,
};
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
    policy: ListenerPolicy,
}

impl NinePServer {
    pub fn new(filesystem: Arc<ZeroFS>, addr: SocketAddr) -> Self {
        Self {
            filesystem,
            transport: Transport::Tcp(addr),
            lock_manager: Arc::new(FileLockManager::new()),
            policy: ListenerPolicy::default(),
        }
    }

    pub fn new_unix(filesystem: Arc<ZeroFS>, path: PathBuf) -> Self {
        Self {
            filesystem,
            transport: Transport::Unix(path),
            lock_manager: Arc::new(FileLockManager::new()),
            policy: ListenerPolicy::default(),
        }
    }

    /// Override client-provided credentials for a shared writable namespace.
    pub fn with_credential_override(mut self, uid: u32, gid: u32) -> Self {
        self.policy.credential_override = Some((uid, gid));
        self
    }

    /// Name matching for standard `9P2000.L` sessions on this listener.
    pub fn with_standard_lookup(
        mut self,
        standard_lookup: crate::fs::name_mode::NameMatch,
    ) -> Self {
        self.policy.standard_lookup = standard_lookup;
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
        let policy = self.policy;
        let client_shutdown = shutdown.child_token();

        AbortOnDropHandle::new(spawn_named("9p-client", async move {
            if let Err(e) = handle_client_stream(
                read_stream,
                write_stream,
                filesystem,
                lock_manager,
                policy,
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
        accepted_work.settle(session::CLIENT_DRAIN_TIMEOUT).await;
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
        Tclunk, Tflush, Tgetattr, Tlopenat, Tmkdir, Trenameat, Tversion, Twalk, Twrite,
        VERSION_9P2000L, VERSION_9P2000L_ZEROFS,
    };
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;
    use tokio::io::AsyncReadExt;
    use tokio::sync::Notify;
    use tokio_util::task::TaskTracker;

    const TEST_TIMEOUT: Duration = Duration::from_secs(1);
    const QUIET_TIMEOUT: Duration = Duration::from_millis(20);

    #[test]
    fn xattr_frames_declare_their_fids_for_clunk_barriers() {
        for (body, expected) in [
            (
                Message::Txattrwalk(ninep_proto::Txattrwalk {
                    fid: 7,
                    newfid: 8,
                    name: ninep_proto::P9String::new(vec![]),
                }),
                FidFootprint::Two(7, 8),
            ),
            (
                Message::Txattrcreate(ninep_proto::Txattrcreate {
                    fid: 8,
                    name: ninep_proto::P9String::new(b"user.key".to_vec()),
                    size: 0,
                    flags: 2,
                }),
                FidFootprint::One(8),
            ),
        ] {
            let frame = P9Message::new(1, body).to_bytes().unwrap();
            for private in [false, true] {
                let metadata = inspect_frame_metadata(&frame, private);
                assert_eq!(metadata.fids, expected);
                assert!(metadata.received_op.is_none());
            }
        }
    }

    #[test]
    fn retained_xattrs_keep_credit_and_leave_room_for_clunk() {
        let global = P9GlobalAdmission::for_test(1024, 4);
        let retained = global.try_admit_xattr(256).unwrap();
        assert_eq!(global.snapshot().reserved_bytes, 256);
        assert!(matches!(
            global.try_admit_xattr(1),
            Err(crate::fs::errors::FsError::RetryLater)
        ));
        assert_eq!(global.snapshot().reserved_bytes, 256);
        drop(retained);
        assert_eq!(global.snapshot().reserved_bytes, 0);
        assert!(global.try_admit_xattr(256).is_ok());
    }

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

    fn encoded_read(count: u32) -> Vec<u8> {
        P9Message::new(
            7,
            Message::Tread(ninep_proto::Tread {
                fid: 1,
                offset: 0,
                count,
            }),
        )
        .to_bytes()
        .unwrap()
    }

    #[test]
    fn read_response_credit_validates_frame_shape_and_bounds_count() {
        for count in [0, 4096, 128 * 1024, u32::MAX] {
            let frame = encoded_read(count);
            let payload = count.min(P9_MAX_MSIZE - ninep_proto::P9_IOHDRSZ) as usize;
            assert_eq!(
                possible_frame_response_bytes(&frame),
                2 * (ninep_proto::P9_IOHDRSZ as usize + payload.max(crate::fs::EXTENT_SIZE))
            );
            let mut truncated = frame.clone();
            truncated.pop();
            let mut extended = frame.clone();
            extended.push(0);
            let mut wrong_size = frame;
            wrong_size[..4].copy_from_slice(&7u32.to_le_bytes());
            for malformed in [truncated, extended, wrong_size] {
                assert_eq!(
                    possible_frame_response_bytes(&malformed),
                    2 * P9_MAX_MSIZE as usize
                );
            }
        }
    }

    fn typed_frame(type_byte: u8) -> Vec<u8> {
        let mut frame = vec![0; P9_HEADER_SIZE];
        frame[..4].copy_from_slice(&(P9_HEADER_SIZE as u32).to_le_bytes());
        frame[4] = type_byte;
        frame
    }

    fn reply_len(body: Message) -> usize {
        P9Message::new(1, body).to_bytes().unwrap().len()
    }

    fn fixed_stat() -> ninep_proto::Stat {
        ninep_proto::Stat::default()
    }

    /// Each request type's charge is at least the largest reply its handler
    /// can send, from the reply's wire layout. Fixed-size replies are charged
    /// exactly that, so the bound is tight; read-like replies also carry the
    /// buffer the handler builds the payload in.
    #[test]
    fn every_reply_fits_its_request_charge() {
        use message_type as t;
        use ninep_proto::{
            Qid, Rattach, Rfallocate, Rfsync, Rgetattr, Rgetlineage, Rgetlock, Rlcreate,
            Rlcreateattr, Rlink, Rlinkattr, Rlock, Rlopen, Rmkdir, Rmkdirattr, Rmknod, Rmknodattr,
            Rreadlink, Rrebind, Rrename, Rrenameat, Rsetattr, Rsetattrattr, Rstatfs, Rsymlink,
            Rsymlinkattr, Runlinkat, Rversion, Rwrite, Rxattrcreate, Rxattrwalk,
        };
        let qid = Qid::default();
        let rlerror = reply_len(Message::Rlerror(Rlerror { ecode: 0 }));
        let max_string = || P9String::new(vec![b'x'; usize::from(u16::MAX)]);
        let fixed: Vec<(u8, Message)> = vec![
            (
                t::TVERSION,
                Message::Rversion(Rversion {
                    msize: P9_MAX_MSIZE,
                    version: P9String::new(ninep_proto::VERSION_9P2000L_ZEROFS_CI.to_vec()),
                }),
            ),
            (t::TATTACH, Message::Rattach(Rattach { qid })),
            (t::TREBIND, Message::Rrebind(Rrebind { qid })),
            (t::TLOPEN, Message::Rlopen(Rlopen { qid, iounit: 0 })),
            (t::TLOPENAT, Message::Rlopenat(Rlopenat { qid, iounit: 0 })),
            (t::TLCREATE, Message::Rlcreate(Rlcreate { qid, iounit: 0 })),
            (T_WRITE, Message::Rwrite(Rwrite { count: u32::MAX })),
            (t::TCLUNK, Message::Rclunk(Rclunk)),
            (t::TSETATTR, Message::Rsetattr(Rsetattr)),
            (t::TFALLOCATE, Message::Rfallocate(Rfallocate)),
            (t::TLINK, Message::Rlink(Rlink)),
            (t::TRENAME, Message::Rrename(Rrename)),
            (t::TRENAMEAT, Message::Rrenameat(Rrenameat)),
            (t::TRENAMENOREPLACE, Message::Rrenameat(Rrenameat)),
            (t::TUNLINKAT, Message::Runlinkat(Runlinkat)),
            (t::TFSYNC, Message::Rfsync(Rfsync)),
            (t::TFSYNCDUR, Message::Rfsync(Rfsync)),
            (t::TFLUSH, Message::Rflush(Rflush)),
            (t::TXATTRCREATE, Message::Rxattrcreate(Rxattrcreate)),
            (
                t::TGETATTR,
                Message::Rgetattr(Rgetattr {
                    valid: u64::MAX,
                    stat: fixed_stat(),
                }),
            ),
            (t::TMKDIR, Message::Rmkdir(Rmkdir { qid })),
            (t::TSYMLINK, Message::Rsymlink(Rsymlink { qid })),
            (t::TMKNOD, Message::Rmknod(Rmknod { qid })),
            (
                t::TSETATTRATTR,
                Message::Rsetattrattr(Rsetattrattr { stat: fixed_stat() }),
            ),
            (
                t::TMKDIRATTR,
                Message::Rmkdirattr(Rmkdirattr { stat: fixed_stat() }),
            ),
            (
                t::TSYMLINKATTR,
                Message::Rsymlinkattr(Rsymlinkattr { stat: fixed_stat() }),
            ),
            (
                t::TMKNODATTR,
                Message::Rmknodattr(Rmknodattr { stat: fixed_stat() }),
            ),
            (
                t::TLINKATTR,
                Message::Rlinkattr(Rlinkattr { stat: fixed_stat() }),
            ),
            (
                t::TLCREATEATTR,
                Message::Rlcreateattr(Rlcreateattr {
                    iounit: 0,
                    stat: fixed_stat(),
                }),
            ),
            (t::TLOCK, Message::Rlock(Rlock { status: 0 })),
            (t::TSTATFS, Message::Rstatfs(Rstatfs::default())),
            (t::TGETLINEAGE, Message::Rgetlineage(Rgetlineage::default())),
        ];
        for (request_type, reply) in fixed {
            let encoded = reply_len(reply);
            assert_eq!(
                possible_frame_response_bytes(&typed_frame(request_type)),
                encoded.max(rlerror),
                "request type {request_type}"
            );
        }

        // Replies that also hold the source of a variable field.
        let readlink = reply_len(Message::Rreadlink(Rreadlink {
            target: max_string(),
        }));
        assert_eq!(
            possible_frame_response_bytes(&typed_frame(t::TREADLINK)),
            readlink + 2 + usize::from(u16::MAX),
            "the target is held once in the reply body and once on the wire"
        );
        let getlock = reply_len(Message::Rgetlock(Rgetlock {
            lock_type: LockType::WriteLock,
            start: u64::MAX,
            length: u64::MAX,
            proc_id: u32::MAX,
            client_id: max_string(),
        }));
        assert_eq!(
            possible_frame_response_bytes(&typed_frame(t::TGETLOCK)),
            2 * getlock
        );
        let xattrwalk = reply_len(Message::Rxattrwalk(Rxattrwalk { size: u64::MAX }));
        assert_eq!(
            possible_frame_response_bytes(&typed_frame(t::TXATTRWALK)),
            xattrwalk + 2 * crate::fs::XATTR_VALUE_MAX
        );

        // A walk reply has one qid per name the frame carries.
        for names in [0, 1, 16, 1000] {
            let wnames = (0..names)
                .map(|_| P9String::new(b"n".to_vec()))
                .collect::<Vec<_>>();
            let walk = P9Message::new(
                1,
                Message::Twalk(ninep_proto::Twalk {
                    fid: 1,
                    newfid: 2,
                    nwname: names as u16,
                    wnames: wnames.clone(),
                }),
            )
            .to_bytes()
            .unwrap();
            let rwalk = reply_len(Message::Rwalk(ninep_proto::Rwalk {
                nwqid: names as u16,
                wqids: vec![qid; names],
            }));
            assert!(rwalk <= possible_frame_response_bytes(&walk));
            let walk_getattr = P9Message::new(
                1,
                Message::Twalkgetattr(ninep_proto::Twalkgetattr {
                    fid: 1,
                    newfid: 2,
                    nwname: names as u16,
                    wnames,
                }),
            )
            .to_bytes()
            .unwrap();
            let rwalkgetattr = reply_len(Message::Rwalkgetattr(ninep_proto::Rwalkgetattr {
                nwqid: names as u16,
                wqids: vec![qid; names],
                stat: fixed_stat(),
            }));
            assert!(rwalkgetattr <= possible_frame_response_bytes(&walk_getattr));
        }
        // A walk that declares more names than its frame carries is charged
        // for the names that fit, two bytes each.
        let mut lying_walk = typed_frame(t::TWALK);
        lying_walk.extend_from_slice(&[0; 8]);
        lying_walk.extend_from_slice(&u16::MAX.to_le_bytes());
        lying_walk.extend_from_slice(&[0; 4]);
        let lying_len = lying_walk.len() as u32;
        lying_walk[..4].copy_from_slice(&lying_len.to_le_bytes());
        assert!(possible_frame_response_bytes(&lying_walk) < 1024);
        assert!(
            decoded_request_bytes(&lying_walk) >= 2 * usize::from(u16::MAX) * 32,
            "the decoder may size its name vector by the declared count"
        );

        // Count-bounded replies hold the payload source and the wire copy.
        for count in [0, 4096, 1024 * 1024, u32::MAX] {
            let read = encoded_read(count);
            let payload = count.min(P9_MAX_MSIZE - ninep_proto::P9_IOHDRSZ) as usize;
            let rread = reply_len(Message::Rread(Rread {
                count: payload as u32,
                data: DekuBytes::from(vec![0; payload]),
            }));
            assert!(2 * rread <= possible_frame_response_bytes(&read));
            let readdir = P9Message::new(
                1,
                Message::Treaddir(ninep_proto::Treaddir {
                    fid: 1,
                    offset: 0,
                    count,
                }),
            )
            .to_bytes()
            .unwrap();
            assert!(2 * rread <= possible_frame_response_bytes(&readdir));
            let readdirattr = P9Message::new(
                1,
                Message::Treaddirattr(ninep_proto::Treaddirattr {
                    fid: 1,
                    offset: 0,
                    count,
                }),
            )
            .to_bytes()
            .unwrap();
            assert!(
                possible_frame_response_bytes(&readdir)
                    < possible_frame_response_bytes(&readdirattr)
            );
            let openatread = P9Message::new(
                1,
                Message::Tlopenatread(ninep_proto::Tlopenatread {
                    fid: 1,
                    newfid: 2,
                    flags: 0,
                    count,
                }),
            )
            .to_bytes()
            .unwrap();
            let prefetch = count.min(P9_MAX_MSIZE - ninep_proto::P9_RLOPENATREAD_HDR) as usize;
            let rlopenatread = reply_len(Message::Rlopenatread(ninep_proto::Rlopenatread {
                qid,
                iounit: 0,
                eof: 1,
                count: prefetch as u32,
                data: DekuBytes::from(vec![0; prefetch]),
            }));
            assert!(2 * rlopenatread <= possible_frame_response_bytes(&openatread));
            for frame in [read, readdir, readdirattr, openatread] {
                assert!(
                    possible_frame_response_bytes(&frame)
                        <= possible_response_bytes(frame[4]) + (2 << 20),
                    "a lying count is charged by the protocol maximum, not by its value"
                );
                let mut truncated = frame.clone();
                truncated.pop();
                let len = truncated.len() as u32;
                truncated[..4].copy_from_slice(&len.to_le_bytes());
                assert_eq!(
                    possible_frame_response_bytes(&truncated),
                    possible_response_bytes(frame[4]),
                    "a malformed count keeps the conservative charge"
                );
            }
        }

        // Unknown and future types keep the conservative maximum.
        for unknown in [0, 102, 122, 199, 227, 255] {
            assert_eq!(
                possible_frame_response_bytes(&typed_frame(unknown)),
                P9_MAX_MSIZE as usize,
                "type {unknown}"
            );
        }
        assert_eq!(
            decoded_request_bytes(&encoded_read(1)),
            encoded_read(1).len()
        );
    }

    /// A count beyond the negotiated msize is clamped by the handler, and the
    /// reply it then builds stays inside the admission it was charged.
    #[tokio::test(flavor = "current_thread")]
    async fn lying_counts_cannot_grow_a_reply_past_its_charge() {
        const MSIZE: u32 = 64 * 1024;
        let (_, handler) = in_memory_handler().await;
        let version = handler
            .handle_message(
                0,
                Message::Tversion(Tversion {
                    msize: MSIZE,
                    version: P9String::new(ninep_proto::VERSION_9P2000L.to_vec()),
                }),
            )
            .await;
        assert!(matches!(version.body, Message::Rversion(ref r) if r.msize == MSIZE));
        let call = |tag: u16, body: Message| {
            let handler = Arc::clone(&handler);
            async move {
                let reply = handler.handle_message(tag, body).await;
                assert!(
                    !matches!(reply.body, Message::Rlerror(_)),
                    "setup request {tag} failed: {:?}",
                    reply.body
                );
                reply
            }
        };
        call(
            1,
            Message::Tattach(Tattach {
                fid: 1,
                afid: u32::MAX,
                uname: P9String::new(b"root".to_vec()),
                aname: P9String::new(b"/".to_vec()),
                n_uname: 0,
            }),
        )
        .await;
        let walk = |fid: u32, newfid: u32, names: &[&[u8]]| {
            Message::Twalk(ninep_proto::Twalk {
                fid,
                newfid,
                nwname: names.len() as u16,
                wnames: names
                    .iter()
                    .map(|name| P9String::new(name.to_vec()))
                    .collect(),
            })
        };

        // A file four times the msize.
        call(2, walk(1, 2, &[])).await;
        call(
            3,
            Message::Tlcreate(ninep_proto::Tlcreate {
                fid: 2,
                name: P9String::new(b"large".to_vec()),
                flags: libc::O_RDWR as u32,
                mode: 0o644,
                gid: 0,
            }),
        )
        .await;
        for chunk in 0..8u64 {
            call(
                4,
                Message::Twrite(Twrite {
                    fid: 2,
                    offset: chunk * 32 * 1024,
                    count: 32 * 1024,
                    data: DekuBytes::from(vec![0x5a; 32 * 1024]),
                }),
            )
            .await;
        }
        call(5, walk(1, 3, &[b"large"])).await;
        call(
            6,
            Message::Tlopen(ninep_proto::Tlopen {
                fid: 3,
                flags: libc::O_RDONLY as u32,
            }),
        )
        .await;

        // A directory whose entries need more than one msize.
        call(
            7,
            Message::Tmkdir(Tmkdir {
                dfid: 1,
                name: P9String::new(b"wide".to_vec()),
                mode: 0o755,
                gid: 0,
            }),
        )
        .await;
        for index in 0..600u32 {
            call(8, walk(1, 4, &[b"wide"])).await;
            call(
                9,
                Message::Tlcreate(ninep_proto::Tlcreate {
                    fid: 4,
                    name: P9String::new(format!("{index:0>100}").into_bytes()),
                    flags: libc::O_RDWR as u32,
                    mode: 0o644,
                    gid: 0,
                }),
            )
            .await;
            call(10, Message::Tclunk(Tclunk { fid: 4 })).await;
        }
        call(11, walk(1, 5, &[b"wide"])).await;
        call(
            12,
            Message::Tlopen(ninep_proto::Tlopen {
                fid: 5,
                flags: libc::O_RDONLY as u32,
            }),
        )
        .await;

        let mut io = DispatchFixture::new(Arc::clone(&handler), 4);
        for (tag, request) in [
            (
                20,
                Message::Tread(ninep_proto::Tread {
                    fid: 3,
                    offset: 0,
                    count: u32::MAX,
                }),
            ),
            (
                21,
                Message::Treaddir(ninep_proto::Treaddir {
                    fid: 5,
                    offset: 0,
                    count: u32::MAX,
                }),
            ),
        ] {
            let request = frame(tag, request);
            io.dispatch_frame(request.clone()).await;
            let response = tokio::time::timeout(TEST_TIMEOUT, io.rx.recv())
                .await
                .expect("reply")
                .expect("response channel");
            let (_, reply, credit) = response.into_guarded_parts();
            let ResponseCredit::Admitted(permit) = credit else {
                panic!("tag {tag} replied without request admission");
            };
            assert!(
                !matches!(decode(&reply).body, Message::Rlerror(_)),
                "tag {tag} failed"
            );
            assert!(
                reply.len() > MSIZE as usize / 2 && reply.len() <= MSIZE as usize,
                "the handler fills the negotiated msize and no more: {} bytes",
                reply.len()
            );
            assert_eq!(permit.reserved_bytes(), frame_admission_bytes(&request));
            assert!(
                request.len() + 2 * reply.len() + REQUEST_STATE_BYTES <= permit.reserved_bytes(),
                "the source and wire copies of the reply fit the charge"
            );
            assert!(
                permit.reserved_bytes() < CONNECTION_INFLIGHT_MEMORY,
                "a lying count is charged by the handler's cap, not by its value"
            );
        }
    }

    #[tokio::test]
    async fn small_reads_fill_the_request_pipeline_within_existing_byte_limits() {
        let global = P9GlobalAdmission::for_test(GLOBAL_INFLIGHT_MEMORY, GLOBAL_INFLIGHT_REQUESTS);
        let connection =
            global.connection_for_test(CONNECTION_INFLIGHT_MEMORY, CONNECTION_INFLIGHT_REQUESTS);
        let frame = encoded_read(128 * 1024);
        let mut admitted = Vec::new();
        for _ in 0..CONNECTION_INFLIGHT_REQUESTS {
            admitted.push(
                tokio::time::timeout(
                    TEST_TIMEOUT,
                    connection.admit_request(frame.len(), possible_frame_response_bytes(&frame)),
                )
                .await
                .expect("small reads exhausted the byte budget before the request budget")
                .unwrap(),
            );
        }
        assert!(global.snapshot().reserved_bytes < CONNECTION_INFLIGHT_MEMORY);
        drop(admitted);
        assert_eq!(global.snapshot().reserved_bytes, 0);
    }

    #[tokio::test]
    async fn download_read_window_leaves_admission_for_liveness() {
        let global = P9GlobalAdmission::for_test(GLOBAL_INFLIGHT_MEMORY, GLOBAL_INFLIGHT_REQUESTS);
        let connection =
            global.connection_for_test(CONNECTION_INFLIGHT_MEMORY, CONNECTION_INFLIGHT_REQUESTS);
        // Reproduce the old geometry: three upload-sized reads fit, but the
        // fourth blocks the receive loop and the probe cannot be admitted.
        let large = encoded_read(9 * 1024 * 1024);
        let mut previous = Vec::new();
        for _ in 0..3 {
            previous.push(
                connection
                    .admit_request(large.len(), possible_frame_response_bytes(&large))
                    .await
                    .unwrap(),
            );
        }
        let mut fourth =
            Box::pin(connection.admit_request(large.len(), possible_frame_response_bytes(&large)));
        assert!(futures::poll!(&mut fourth).is_pending());
        let probe = P9Message::new(9, Message::Tgetlineage(ninep_proto::Tgetlineage))
            .to_bytes()
            .unwrap();
        let mut blocked_probe =
            Box::pin(connection.admit_request(probe.len(), possible_frame_response_bytes(&probe)));
        assert!(futures::poll!(&mut blocked_probe).is_pending());
        drop(blocked_probe);
        drop(fourth);
        drop(previous);
        // The CLI's wire-level regression separately verifies this geometry.
        // Retain every reply credit, as a slow backend does, while admitting
        // the real lineage probe used by the client's reply-timeout path.
        let frame = encoded_read(4 * 1024 * 1024);
        let mut reads = Vec::new();
        for _ in 0..4 {
            reads.push(
                connection
                    .admit_request(frame.len(), possible_frame_response_bytes(&frame))
                    .await
                    .unwrap(),
            );
        }
        let admitted_probe = tokio::time::timeout(
            TEST_TIMEOUT,
            connection.admit_request(probe.len(), possible_frame_response_bytes(&probe)),
        )
        .await
        .expect("slow download reads must not prevent a liveness reply")
        .unwrap();
        drop(admitted_probe);
        drop(reads);
        assert_eq!(global.snapshot().reserved_bytes, 0);
    }

    #[tokio::test]
    async fn maximum_reads_still_obey_connection_byte_backpressure() {
        let global = P9GlobalAdmission::for_test(GLOBAL_INFLIGHT_MEMORY, GLOBAL_INFLIGHT_REQUESTS);
        let connection =
            global.connection_for_test(CONNECTION_INFLIGHT_MEMORY, CONNECTION_INFLIGHT_REQUESTS);
        let frame = encoded_read(u32::MAX);
        let mut admitted = Vec::new();
        for _ in 0..3 {
            admitted.push(
                connection
                    .admit_request(frame.len(), possible_frame_response_bytes(&frame))
                    .await
                    .unwrap(),
            );
        }
        let mut fourth =
            Box::pin(connection.admit_request(frame.len(), possible_frame_response_bytes(&frame)));
        assert!(futures::poll!(&mut fourth).is_pending());
        drop(admitted.pop());
        tokio::time::timeout(TEST_TIMEOUT, fourth)
            .await
            .unwrap()
            .unwrap();
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

    /// Frames past the request limit wait in their own tasks, charged to the
    /// reader's one-msize pending envelope. Offer a burst of paused metadata
    /// requests on one connection and check that the reader reads all of it,
    /// and that the waiting frames do not take the process-wide request slots
    /// another connection needs.
    #[tokio::test]
    async fn waiting_frames_on_one_connection_leave_other_connections_admissible() {
        const OFFERED: usize = CONNECTION_INFLIGHT_REQUESTS + 100;
        let (_, handler) = in_memory_handler().await;
        establish_session(&handler, 1, b"root", 0, None).await;
        let global = P9GlobalAdmission::for_test(GLOBAL_INFLIGHT_MEMORY, GLOBAL_INFLIGHT_REQUESTS);
        let busy =
            global.connection_for_test(CONNECTION_INFLIGHT_MEMORY, CONNECTION_INFLIGHT_REQUESTS);
        let other =
            global.connection_for_test(CONNECTION_INFLIGHT_MEMORY, CONNECTION_INFLIGHT_REQUESTS);
        let requests = TaskTracker::new();
        let (tx, mut rx) = mpsc::channel(OFFERED + 1);
        let (mut client, server) = tokio::io::duplex(4 * 1024 * 1024);
        let shutdown = CancellationToken::new();

        let mut releases = Vec::new();
        let mut frames = Vec::new();
        let mut frame_len = 0;
        for index in 0..OFFERED {
            let mut op_id = [0xd5; P9_OP_ID_LEN];
            op_id[..2].copy_from_slice(&(index as u16).to_le_bytes());
            let (_reached, release) = pause_success_terminal_dedup(op_id);
            releases.push(release);
            let frame = P9Message::new_with_op_id(
                10 + index as u16,
                op_id,
                Message::Tmkdir(Tmkdir {
                    dfid: 1,
                    name: P9String::new(format!("burst-{index:04}").into_bytes()),
                    mode: 0o755,
                    gid: 0,
                }),
            )
            .to_bytes_ctx(true)
            .unwrap();
            frame_len = frame.len();
            frames.extend(frame);
        }
        client.write_all(&frames).await.unwrap();

        let reader = handle_client_loop(
            Arc::clone(&handler),
            server,
            tx,
            shutdown.clone(),
            CancellationToken::new(),
            &busy,
            &requests,
        );
        let observe = async {
            // The reader starts one task per frame it reads.
            let read_all = tokio::time::timeout(TEST_TIMEOUT, async {
                while requests.len() < OFFERED {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .is_ok();
            let read = requests.len();
            let probe = frame(9, Message::Tgetlineage(ninep_proto::Tgetlineage));
            let other_admitted = tokio::time::timeout(
                TEST_TIMEOUT,
                other.admit_frame_or_shutdown(&probe, &CancellationToken::new()),
            )
            .await
            .ok()
            .map(|admitted| admitted.unwrap());
            let busy_global_slots =
                global.snapshot().active_requests - usize::from(other_admitted.is_some());
            let other_admitted_now = other_admitted.is_some();
            drop(other_admitted);
            for release in releases.drain(..) {
                let _ = release.send(());
            }
            for _ in 0..OFFERED {
                tokio::time::timeout(Duration::from_secs(10), rx.recv())
                    .await
                    .expect("released requests must reply")
                    .expect("response channel");
            }
            shutdown.cancel();
            (read_all, read, other_admitted_now, busy_global_slots)
        };
        let (reader_result, (read_all, read, other_admitted, busy_global_slots)) =
            tokio::join!(reader, observe);
        reader_result.unwrap();
        requests.close();
        requests.wait().await;

        let pending_capacity =
            P9_MAX_MSIZE as usize / (frame_len + session::PENDING_REQUEST_TASK_BYTES);
        assert!(
            read_all,
            "the reader read {read} of {OFFERED} frames; its pending envelope holds about \
             {pending_capacity} small frames"
        );
        assert!(
            other_admitted,
            "one connection's waiting frames hold {busy_global_slots} of \
             {GLOBAL_INFLIGHT_REQUESTS} process-wide request slots"
        );
        assert!(busy_global_slots <= CONNECTION_INFLIGHT_REQUESTS);
    }

    /// A Samba share served through one 9P mount shares one connection, so
    /// the number of metadata requests that connection can hold is the
    /// share's metadata concurrency. Offer more paused `Tmkdir` requests than
    /// the request limit and count how many reach the handler.
    #[tokio::test]
    async fn one_connection_holds_metadata_requests_up_to_its_request_limit() {
        const OFFERED: usize = CONNECTION_INFLIGHT_REQUESTS + 4;
        let (_, handler) = in_memory_handler().await;
        establish_session(&handler, 1, b"root", 0, None).await;
        let global = P9GlobalAdmission::for_test(GLOBAL_INFLIGHT_MEMORY, GLOBAL_INFLIGHT_REQUESTS);
        let admission =
            global.connection_for_test(CONNECTION_INFLIGHT_MEMORY, CONNECTION_INFLIGHT_REQUESTS);
        let requests = TaskTracker::new();
        let (tx, mut rx) = mpsc::channel(OFFERED + 1);
        let (mut client, server) = tokio::io::duplex(1024 * 1024);
        let shutdown = CancellationToken::new();

        let mut reached = Vec::new();
        let mut releases = Vec::new();
        let mut frames = Vec::new();
        for index in 0..OFFERED {
            let mut op_id = [0xb7; P9_OP_ID_LEN];
            op_id[0] = index as u8;
            let (request_reached, release) = pause_success_terminal_dedup(op_id);
            reached.push(request_reached);
            releases.push(release);
            frames.extend(
                P9Message::new_with_op_id(
                    10 + index as u16,
                    op_id,
                    Message::Tmkdir(Tmkdir {
                        dfid: 1,
                        name: P9String::new(format!("held-{index}").into_bytes()),
                        mode: 0o755,
                        gid: 0,
                    }),
                )
                .to_bytes_ctx(true)
                .unwrap(),
            );
        }
        client.write_all(&frames).await.unwrap();

        let reader = handle_client_loop(
            Arc::clone(&handler),
            server,
            tx,
            shutdown.clone(),
            CancellationToken::new(),
            &admission,
            &requests,
        );
        let observe = async {
            // Admission is in receive order, so the first request that does
            // not reach its pause marks where the reader stopped.
            let mut held = 0;
            for request_reached in &mut reached {
                if tokio::time::timeout(TEST_TIMEOUT, request_reached)
                    .await
                    .is_err()
                {
                    break;
                }
                held += 1;
            }
            for release in releases.drain(..) {
                let _ = release.send(());
            }
            for _ in 0..OFFERED {
                tokio::time::timeout(TEST_TIMEOUT, rx.recv())
                    .await
                    .expect("released requests must reply")
                    .expect("response channel");
            }
            shutdown.cancel();
            held
        };
        let (reader_result, held) = tokio::join!(reader, observe);
        reader_result.unwrap();
        requests.close();
        requests.wait().await;

        assert_eq!(
            held, CONNECTION_INFLIGHT_REQUESTS,
            "one connection held {held} paused metadata requests; small replies must not \
             each reserve a maximum-msize reply"
        );
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

    /// A connection whose requests run under an explicit process shutdown
    /// token and a process-level accepted-work tracker.
    struct ProcessShutdownFixture {
        filesystem: Arc<ZeroFS>,
        handler: Arc<NinePHandler>,
        accepted_work: P9AcceptedWorkTracker,
        admission: P9ConnectionAdmission,
        requests: TaskTracker,
        tx: mpsc::Sender<P9Response>,
        rx: mpsc::Receiver<P9Response>,
        connection_shutdown: CancellationToken,
        process_shutdown: CancellationToken,
    }

    impl ProcessShutdownFixture {
        async fn new() -> Self {
            let filesystem = Arc::new(ZeroFS::new_in_memory().await.unwrap());
            let handler = Arc::new(NinePHandler::new(
                Arc::clone(&filesystem),
                Arc::new(FileLockManager::new()),
            ));
            establish_session(&handler, 1, b"root", 0, None).await;
            let accepted_work = P9AcceptedWorkTracker::new();
            let byte_limit = P9_MAX_MSIZE as usize * 2;
            let global = P9GlobalAdmission::for_test(byte_limit, 2);
            let admission = global.connection_with_accepted_work(accepted_work.clone());
            let (tx, rx) = mpsc::channel(1);
            Self {
                filesystem,
                handler,
                accepted_work,
                admission,
                requests: TaskTracker::new(),
                tx,
                rx,
                connection_shutdown: CancellationToken::new(),
                process_shutdown: CancellationToken::new(),
            }
        }

        /// Dispatch a `Tmkdir` of `name` under the attach root (inode 0).
        async fn dispatch_mkdir(&self, tag: u16, op_id: [u8; 16], name: &[u8]) {
            let request = P9Message::new_with_op_id(
                tag,
                op_id,
                Message::Tmkdir(Tmkdir {
                    dfid: 1,
                    name: P9String::new(name.to_vec()),
                    mode: 0o755,
                    gid: 0,
                }),
            )
            .to_bytes_ctx(true)
            .unwrap();
            dispatch_9p_frame(
                Bytes::from(request),
                &self.handler,
                &self.tx,
                &InflightRegistry::default(),
                &self.admission,
                &self.requests,
                &self.connection_shutdown,
                &self.process_shutdown,
            )
            .await
            .unwrap();
        }

        /// Begin process shutdown and stop tracking new request tasks.
        fn shut_down(&self) {
            self.process_shutdown.cancel();
            self.requests.close();
        }

        async fn entry_exists(&self, name: &[u8]) -> bool {
            self.filesystem
                .directory_store
                .exists(0, name)
                .await
                .unwrap()
        }
    }

    /// The request below has applied its mkdir and is paused before it
    /// publishes its terminal ledger result and replies. That is past its point
    /// of no return: flush(5) and `crate::abandonment` both say it now runs to
    /// completion. This test used to assert that process shutdown drops it
    /// anyway. Shutdown now lets it finish, and bounds the wait separately
    /// (`process_shutdown_settlement_is_bounded_when_a_commit_never_applies`).
    #[tokio::test(flavor = "current_thread")]
    async fn process_shutdown_settles_a_request_past_its_point_of_no_return() {
        let mut fixture = ProcessShutdownFixture::new().await;
        let op_id = [0x92; 16];
        let (request_reached, release_request) = pause_success_terminal_dedup(op_id);
        fixture
            .dispatch_mkdir(10, op_id, b"process-shutdown-first")
            .await;
        tokio::time::timeout(TEST_TIMEOUT, request_reached)
            .await
            .expect("dispatched request must reach its controlled pause")
            .unwrap();
        assert_eq!(fixture.accepted_work.len(), 1);

        fixture.shut_down();
        let mut settlement = Box::pin(fixture.requests.wait());
        assert!(
            tokio::time::timeout(QUIET_TIMEOUT, &mut settlement)
                .await
                .is_err(),
            "process shutdown dropped a request whose mkdir had already applied"
        );
        assert_eq!(fixture.accepted_work.len(), 1);

        release_request.send(()).unwrap();
        tokio::time::timeout(TEST_TIMEOUT, settlement)
            .await
            .expect("the request must finish once its pause is released");
        let response = tokio::time::timeout(TEST_TIMEOUT, fixture.rx.recv())
            .await
            .expect("the settled request must still reply")
            .expect("response channel");
        let (tag, bytes) = response.into_parts();
        assert_eq!(tag, 10);
        assert!(matches!(decode(&bytes).body, Message::Rmkdir(_)));
        fixture.accepted_work.stop_accepting();
        tokio::time::timeout(TEST_TIMEOUT, fixture.accepted_work.wait())
            .await
            .expect("process ownership must drain after the request settles");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn process_shutdown_keeps_a_queued_mkdir_locked_until_it_applies() {
        let mut fixture = ProcessShutdownFixture::new().await;
        let block_apply = fixture.filesystem.db.flush_barrier().write_owned().await;
        let queued = fixture.filesystem.write_coordinator.probe_next_apply();
        fixture.dispatch_mkdir(10, [0x93; 16], b"queued").await;
        tokio::time::timeout(TEST_TIMEOUT, queued)
            .await
            .expect("the mkdir must queue its commit")
            .unwrap();

        fixture.shut_down();
        let mut settlement = Box::pin(fixture.requests.wait());
        assert!(
            tokio::time::timeout(QUIET_TIMEOUT, &mut settlement)
                .await
                .is_err(),
            "process shutdown dropped a mkdir whose commit was queued"
        );
        assert!(
            fixture.filesystem.lock_manager.is_locked_for_test(0),
            "the directory lock was released before the queued mkdir applied"
        );

        // A second actor that takes the directory lock during shutdown, as NFS,
        // the web UI or a background reclaim could, must see the applied entry.
        let observer = tokio::spawn({
            let filesystem = Arc::clone(&fixture.filesystem);
            async move {
                let _lock = filesystem.lock_manager.acquire(0).await;
                filesystem
                    .directory_store
                    .exists(0, b"queued")
                    .await
                    .unwrap()
            }
        });
        drop(block_apply);
        assert!(
            tokio::time::timeout(TEST_TIMEOUT, observer)
                .await
                .expect("the lock must be released once the mkdir applies")
                .unwrap(),
            "the next holder of the directory lock did not see the queued mkdir"
        );
        tokio::time::timeout(TEST_TIMEOUT, settlement)
            .await
            .expect("the request must finish once its commit applies");
        let response = tokio::time::timeout(TEST_TIMEOUT, fixture.rx.recv())
            .await
            .expect("the settled mkdir must still reply")
            .expect("response channel");
        let (tag, bytes) = response.into_parts();
        assert_eq!(tag, 10);
        assert!(matches!(decode(&bytes).body, Message::Rmkdir(_)));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn process_shutdown_drops_a_request_before_its_point_of_no_return() {
        let mut fixture = ProcessShutdownFixture::new().await;
        // The mkdir waits for the directory lock, before anything is queued.
        let directory_lock = fixture.filesystem.lock_manager.acquire(0).await;
        fixture.dispatch_mkdir(10, [0x94; 16], b"never").await;
        fixture.requests.close();
        let mut settlement = Box::pin(fixture.requests.wait());
        assert!(
            tokio::time::timeout(QUIET_TIMEOUT, &mut settlement)
                .await
                .is_err(),
            "the mkdir must still be waiting for the directory lock"
        );

        fixture.process_shutdown.cancel();
        tokio::time::timeout(TEST_TIMEOUT, settlement)
            .await
            .expect("process shutdown must drop a request that changed nothing");
        fixture.accepted_work.stop_accepting();
        tokio::time::timeout(TEST_TIMEOUT, fixture.accepted_work.wait())
            .await
            .expect("process ownership must drain after the request is dropped");

        drop(directory_lock);
        fixture
            .filesystem
            .write_coordinator
            .barrier()
            .await
            .unwrap();
        assert!(!fixture.entry_exists(b"never").await);
        assert!(!fixture.filesystem.lock_manager.is_locked_for_test(0));
        assert!(
            fixture.rx.try_recv().is_err(),
            "a dropped request must not reply"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn process_shutdown_settlement_is_bounded_when_a_commit_never_applies() {
        let mut fixture = ProcessShutdownFixture::new().await;
        let block_apply = fixture.filesystem.db.flush_barrier().write_owned().await;
        let queued = fixture.filesystem.write_coordinator.probe_next_apply();
        fixture.dispatch_mkdir(10, [0x95; 16], b"stuck").await;
        tokio::time::timeout(TEST_TIMEOUT, queued)
            .await
            .expect("the mkdir must queue its commit")
            .unwrap();

        fixture.shut_down();
        let revoked = tokio::time::timeout(
            TEST_TIMEOUT,
            fixture.accepted_work.settle(Duration::from_millis(50)),
        )
        .await
        .expect("process settlement must return within its grace");
        assert_eq!(revoked, 1, "settlement must report the request it revoked");
        tokio::time::timeout(TEST_TIMEOUT, fixture.requests.wait())
            .await
            .expect("a revoked request must be dropped");
        assert!(
            fixture.rx.try_recv().is_err(),
            "a revoked request must not reply"
        );

        // The queued commit still owns the directory lock and applies once
        // its backend can.
        assert!(fixture.filesystem.lock_manager.is_locked_for_test(0));
        assert!(!fixture.entry_exists(b"stuck").await);
        drop(block_apply);
        drop(
            tokio::time::timeout(TEST_TIMEOUT, fixture.filesystem.lock_manager.acquire(0))
                .await
                .expect("the lock must be released once the commit applies"),
        );
        assert!(fixture.entry_exists(b"stuck").await);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn connection_shutdown_retains_an_accepted_first_request() {
        let filesystem = Arc::new(ZeroFS::new_in_memory().await.unwrap());
        let handler = Arc::new(NinePHandler::new(
            filesystem,
            Arc::new(FileLockManager::new()),
        ));
        establish_session(&handler, 1, b"root", 0, None).await;

        let accepted_work = P9AcceptedWorkTracker::new();
        let byte_limit = P9_MAX_MSIZE as usize * 2;
        let global = P9GlobalAdmission::for_test(byte_limit, 2);
        let admission = global.connection_with_accepted_work(accepted_work.clone());
        let requests = TaskTracker::new();
        let (tx, _rx) = mpsc::channel(1);
        let connection_shutdown = CancellationToken::new();
        let process_shutdown = CancellationToken::new();
        let op_id = [0x91; 16];
        let (request_reached, release_request) = pause_success_terminal_dedup(op_id);
        let request = P9Message::new_with_op_id(
            10,
            op_id,
            Message::Tmkdir(Tmkdir {
                dfid: 1,
                name: P9String::new(b"retained-first".to_vec()),
                mode: 0o755,
                gid: 0,
            }),
        )
        .to_bytes_ctx(true)
        .unwrap();

        dispatch_9p_frame(
            Bytes::from(request),
            &handler,
            &tx,
            &InflightRegistry::default(),
            &admission,
            &requests,
            &connection_shutdown,
            &process_shutdown,
        )
        .await
        .unwrap();
        tokio::time::timeout(TEST_TIMEOUT, request_reached)
            .await
            .expect("accepted FIRST must reach its controlled pause")
            .unwrap();
        assert_eq!(accepted_work.len(), 1);

        connection_shutdown.cancel();
        requests.close();
        let mut settlement = Box::pin(requests.wait());
        let retained = tokio::time::timeout(QUIET_TIMEOUT, &mut settlement)
            .await
            .is_err();
        if retained {
            let _ = release_request.send(());
            tokio::time::timeout(TEST_TIMEOUT, settlement)
                .await
                .expect("released accepted FIRST must settle");
        }
        accepted_work.stop_accepting();
        tokio::time::timeout(TEST_TIMEOUT, accepted_work.wait())
            .await
            .expect("process ownership must drain after the FIRST completes");

        assert!(
            retained,
            "connection shutdown must not cancel already-accepted FIRST work"
        );
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

        assert_eq!(server.policy.credential_override, Some((501, 20)));
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
    async fn spawned_standard_session_uses_the_listener_lookup_rule() {
        use crate::fs::name_mode::{NameMatch, NameMode};
        let filesystem = Arc::new(
            ZeroFS::new_in_memory_with_name_mode(NameMode::Insensitive)
                .await
                .unwrap(),
        );
        let root = Credentials {
            uid: 0,
            gid: 0,
            gid_known: true,
            groups: [0; 16],
            groups_count: 0,
            groups_complete: true,
        };
        let (id, _) = filesystem
            .create(&root, 0, b"Foo", &Default::default())
            .await
            .unwrap();
        let server = NinePServer::new_unix(Arc::clone(&filesystem), "/unused".into())
            .with_standard_lookup(NameMatch::Exact);
        let (mut client, server_stream) = tokio::io::duplex(4096);
        let (read_stream, write_stream) = tokio::io::split(server_stream);
        let shutdown = CancellationToken::new();
        let task = server.spawn_client_handler(
            read_stream,
            write_stream,
            &shutdown,
            "standard-lookup-test".to_string(),
            P9SessionAdmission {
                transport: P9GlobalAdmission::shared().try_admit_transport().unwrap(),
                transport_label: "test",
                accepted_work: P9AcceptedWorkTracker::new(),
            },
        );

        let version = exchange_client_message(
            &mut client,
            0,
            Message::Tversion(Tversion {
                msize: super::super::handler::DEFAULT_MSIZE,
                version: P9String::new(VERSION_9P2000L.to_vec()),
            }),
        )
        .await;
        assert!(
            matches!(version.body, Message::Rversion(ref reply) if reply.version.data == VERSION_9P2000L)
        );
        let attach = exchange_client_message(
            &mut client,
            1,
            Message::Tattach(Tattach {
                fid: 1,
                afid: u32::MAX,
                uname: P9String::new(b"root".to_vec()),
                aname: P9String::new(Vec::new()),
                n_uname: 0,
            }),
        )
        .await;
        assert!(matches!(attach.body, Message::Rattach(_)));
        let walk = exchange_client_message(
            &mut client,
            2,
            Message::Twalk(Twalk {
                fid: 1,
                newfid: 2,
                nwname: 1,
                wnames: vec![P9String::new(b"foo".to_vec())],
            }),
        )
        .await;
        assert!(
            matches!(walk.body, Message::Rlerror(Rlerror { ecode }) if ecode == crate::linux_errno::ENOENT),
            "{:?}",
            walk.body
        );
        let rename = exchange_client_message(
            &mut client,
            3,
            Message::Trenameat(Trenameat {
                olddirfid: 1,
                oldname: P9String::new(b"Foo".to_vec()),
                newdirfid: 1,
                newname: P9String::new(b"foo".to_vec()),
            }),
        )
        .await;
        assert!(
            matches!(rename.body, Message::Rrenameat(_)),
            "{:?}",
            rename.body
        );
        assert_eq!(
            filesystem
                .directory_store
                .get_exact(0, b"foo")
                .await
                .unwrap(),
            id
        );

        shutdown.cancel();
        drop(client);
        tokio::time::timeout(TEST_TIMEOUT, task)
            .await
            .expect("client handler shutdown")
            .unwrap();
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

        // Revoke authority after response serialization and before transport write.
        // Rclunk stands for any success; Rflush is exempt because it reports
        // no filesystem state (`tflush_needs_no_serving_authority`).
        let completed_bytes = frame(41, Message::Rclunk(Rclunk)).to_vec();
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

    async fn blocked_first_reserves_replay_before_admission(cancel_process: bool) {
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

        let op_id = [0x64; 16];
        let mkdir = Message::Tmkdir(Tmkdir {
            dfid: 1,
            name: P9String::new(b"received-before-admission".to_vec()),
            mode: 0o755,
            gid: 0,
        });
        let first = P9Message::new_with_op_id(10, op_id, mkdir.clone())
            .to_bytes_ctx(true)
            .unwrap();
        let (tx, mut rx) = mpsc::channel(2);
        let inflight = InflightRegistry::default();
        let byte_limit = P9_MAX_MSIZE as usize * 2;
        let global = P9GlobalAdmission::for_test(byte_limit, 1);
        let admission = global.connection_for_test(byte_limit, 1);
        let blocked = admission.admit_request(1, 0).await.unwrap();
        let requests = TaskTracker::new();
        let connection_shutdown = CancellationToken::new();
        let process_shutdown = CancellationToken::new();
        let dispatch = dispatch_9p_frame(
            Bytes::from(first),
            &old,
            &tx,
            &inflight,
            &admission,
            &requests,
            &connection_shutdown,
            &process_shutdown,
        );
        tokio::pin!(dispatch);
        assert!(futures::poll!(&mut dispatch).is_pending());
        assert_eq!(
            filesystem.dedup.stats().inflight_ids,
            1,
            "a fully received FIRST must reserve its identity before waiting for capacity"
        );
        assert_eq!(admission.accepted_work.len(), 1);
        assert_eq!(global.snapshot().active_requests, 1);

        if cancel_process {
            process_shutdown.cancel();
            assert!(dispatch.await.is_err());
            assert_eq!(filesystem.dedup.stats().inflight_ids, 0);
            assert_eq!(admission.accepted_work.len(), 0);
            drop(blocked);
            assert_eq!(global.snapshot().reserved_bytes, 0);
            return;
        }

        old.close_for_disconnect();
        connection_shutdown.cancel();
        let retry = replacement.handle_message_with_op_envelope_origin(
            11,
            op_id,
            ninep_proto::P9_OP_FLAG_RETRY,
            0,
            mkdir,
        );
        tokio::pin!(retry);
        assert!(
            futures::poll!(&mut retry).is_pending(),
            "a reconnect retry must join the received FIRST, not report ESTALE"
        );

        drop(blocked);
        dispatch.await.unwrap();
        let replayed = tokio::time::timeout(TEST_TIMEOUT, retry)
            .await
            .expect("retry must finish when request capacity returns");
        assert!(matches!(replayed.body, Message::Rmkdir(_)));
        drop(rx.recv().await.unwrap());
        requests.close();
        requests.wait().await;
        assert_eq!(admission.accepted_work.len(), 0);
        assert_eq!(global.snapshot().active_requests, 0);
        assert_eq!(global.snapshot().reserved_bytes, 0);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn received_first_survives_disconnect_while_waiting_for_admission() {
        blocked_first_reserves_replay_before_admission(false).await;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn process_shutdown_releases_received_first_waiting_for_admission() {
        blocked_first_reserves_replay_before_admission(true).await;
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
    async fn tclunk_cannot_overtake_an_earlier_exact_walk() {
        for clunk_fid in [1, 2] {
            let filesystem = Arc::new(
                ZeroFS::new_in_memory_with_name_mode(crate::fs::name_mode::NameMode::Insensitive)
                    .await
                    .unwrap(),
            );
            let directory = filesystem
                .mkdir(
                    &Credentials::from_auth_context(
                        &(&crate::test_helpers::test_helpers_mod::test_auth()).into(),
                    ),
                    0,
                    b"Dir",
                    &crate::fs::types::SetAttributes::default(),
                )
                .await
                .unwrap()
                .0;
            let handler = Arc::new(NinePHandler::new(
                Arc::clone(&filesystem),
                Arc::new(FileLockManager::new()),
            ));
            establish_session(&handler, 0, b"test", 1000, Some([0; 16])).await;
            // Stop the exact walk while it pins the directory, before it can
            // install newfid. A clunk of either the source or the destination
            // must wait.
            let inode_lock = filesystem.lock_manager.acquire(directory).await;
            let mut io = DispatchFixture::new(handler, 4);
            io.dispatch(
                10,
                Message::Twalkgetattrexact(ninep_proto::Twalkgetattr {
                    fid: 1,
                    newfid: 2,
                    nwname: 1,
                    wnames: vec![P9String::new(b"Dir".to_vec())],
                }),
            )
            .await;
            io.dispatch(11, Message::Tclunk(Tclunk { fid: clunk_fid }))
                .await;
            io.expect_quiet("clunk must wait for the exact walk's fid installation")
                .await;
            drop(inode_lock);
            let (tag, walked) = io.recv("exact walk response").await;
            assert_eq!(tag, 10);
            assert!(matches!(walked.body, Message::Rwalkgetattr(_)));
            let (tag, clunked) = io.recv("clunk response").await;
            assert_eq!(tag, 11);
            assert!(matches!(clunked.body, Message::Rclunk(_)));
            io.dispatch(
                12,
                Message::Tgetattr(Tgetattr {
                    fid: clunk_fid,
                    request_mask: GETATTR_ALL,
                }),
            )
            .await;
            assert!(matches!(
                io.recv("clunked fid must remain absent").await.1.body,
                Message::Rlerror(_)
            ));
            if clunk_fid == 2 {
                assert_eq!(filesystem.open_handle_count(directory), 0);
            }
        }
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

    // Tflush cancellation (flush(5); Linux net/9p/client.c p9_client_rpc).

    /// Upper bound for a Tflush that must not depend on the flushed work.
    const PROMPT_FLUSH: Duration = Duration::from_millis(250);

    async fn wait_until(mut ready: impl FnMut() -> bool, message: &str) {
        tokio::time::timeout(TEST_TIMEOUT, async {
            while !ready() {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .expect(message);
    }

    fn admission_is_idle(admission: &P9ConnectionAdmission) -> bool {
        let snapshot = admission.global.snapshot();
        snapshot.reserved_bytes == 0 && snapshot.active_requests == 0
    }

    async fn plain_root_session(handler: &NinePHandler) {
        let version = handler
            .handle_message(
                0,
                Message::Tversion(Tversion {
                    msize: super::super::handler::DEFAULT_MSIZE,
                    version: P9String::new(ninep_proto::VERSION_9P2000L.to_vec()),
                }),
            )
            .await;
        assert!(matches!(version.body, Message::Rversion(_)));
        let attach = handler
            .handle_message(
                1,
                Message::Tattach(Tattach {
                    fid: 1,
                    afid: u32::MAX,
                    uname: P9String::new(b"root".to_vec()),
                    aname: P9String::new(b"/".to_vec()),
                    n_uname: 0,
                }),
            )
            .await;
        assert!(matches!(attach.body, Message::Rattach(_)));
    }

    const HELD_FILE_BYTES: usize = 64 * 1024;

    fn held_file_payload() -> Vec<u8> {
        (0..HELD_FILE_BYTES)
            .map(|index| (index % 251) as u8)
            .collect()
    }

    async fn open_test_filesystem(
        metadata: Arc<dyn slatedb::object_store::ObjectStore>,
        segments: Arc<dyn slatedb::object_store::ObjectStore>,
    ) -> Arc<ZeroFS> {
        use crate::block_transformer::ZeroFsBlockTransformer;
        use crate::config::CompressionConfig;
        use slatedb::object_store::path::Path;
        use slatedb::{BlockTransformer, DbBuilder};

        let key = [0x3c; 32];
        let transformer: Arc<dyn BlockTransformer> =
            ZeroFsBlockTransformer::new_arc(&key, CompressionConfig::default());
        let db = Arc::new(
            DbBuilder::new(Path::from("db"), metadata)
                .with_block_transformer(transformer)
                .with_filter_policies(crate::fs::filter_policy::filter_policies())
                .with_segment_extractor(Arc::new(crate::segment_extractor::ZeroFsSegmentExtractor))
                .build()
                .await
                .unwrap(),
        );
        Arc::new(
            ZeroFS::new_with_slatedb_and_lease(
                crate::db::SlateDbHandle::ReadWrite(db),
                u64::MAX,
                None,
                false,
                false,
                None,
                None,
                Arc::new(crate::dedup::DedupCache::new()),
                None,
                crate::object_trace::ObjectTracer::new(),
                segments,
                crate::frame_codec::FrameCodec::new(
                    &key,
                    crate::segment::SEGMENT_INFO,
                    CompressionConfig::default(),
                ),
                None,
                crate::config::StoreProfile::default(),
                crate::fs::name_mode::NameMode::Sensitive,
            )
            .await
            .unwrap(),
        )
    }

    fn walk_message(fid: u32, newfid: u32, names: &[&[u8]]) -> Message {
        Message::Twalk(ninep_proto::Twalk {
            fid,
            newfid,
            nwname: names.len() as u16,
            wnames: names
                .iter()
                .map(|name| P9String::new(name.to_vec()))
                .collect(),
        })
    }

    /// A plain-dialect session whose fid 2 is a file opened for reading.
    /// The file was written and closed by an earlier instance, so its bytes
    /// live only in a segment object and this instance has no cached copy.
    /// That segment store's GET bodies can be held, so a Tread waits on the
    /// backend.
    async fn held_segment_session() -> (
        Arc<ZeroFS>,
        Arc<NinePHandler>,
        Arc<crate::fault_store::FaultControls>,
    ) {
        use slatedb::object_store::ObjectStore;
        use slatedb::object_store::memory::InMemory;

        let metadata: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let segments: Arc<dyn ObjectStore> = Arc::new(InMemory::new());

        let writer = open_test_filesystem(Arc::clone(&metadata), Arc::clone(&segments)).await;
        let handler = NinePHandler::new(Arc::clone(&writer), Arc::new(FileLockManager::new()));
        plain_root_session(&handler).await;
        let walk = handler.handle_message(2, walk_message(1, 2, &[])).await;
        assert!(matches!(walk.body, Message::Rwalk(_)));
        let create = handler
            .handle_message(
                3,
                Message::Tlcreate(ninep_proto::Tlcreate {
                    fid: 2,
                    name: P9String::new(b"held".to_vec()),
                    flags: libc::O_RDWR as u32,
                    mode: 0o644,
                    gid: 0,
                }),
            )
            .await;
        assert!(matches!(create.body, Message::Rlcreate(_)));
        let payload = held_file_payload();
        let write = handler
            .handle_message(
                4,
                Message::Twrite(Twrite {
                    fid: 2,
                    offset: 0,
                    count: payload.len() as u32,
                    data: payload.into(),
                }),
            )
            .await;
        assert!(matches!(write.body, Message::Rwrite(_)));
        drop(handler);
        writer.flush_coordinator.close().await.unwrap();
        drop(writer);

        let (held, controls) = crate::fault_store::FaultStore::new(segments);
        let filesystem = open_test_filesystem(metadata, held).await;
        let handler = Arc::new(NinePHandler::new(
            Arc::clone(&filesystem),
            Arc::new(FileLockManager::new()),
        ));
        plain_root_session(&handler).await;
        let walk = handler
            .handle_message(2, walk_message(1, 2, &[b"held"]))
            .await;
        assert!(matches!(walk.body, Message::Rwalk(_)));
        let open = handler
            .handle_message(
                3,
                Message::Tlopen(ninep_proto::Tlopen {
                    fid: 2,
                    flags: libc::O_RDONLY as u32,
                }),
            )
            .await;
        assert!(matches!(open.body, Message::Rlopen(_)));
        (filesystem, handler, controls)
    }

    fn held_read() -> Message {
        Message::Tread(ninep_proto::Tread {
            fid: 2,
            offset: 0,
            count: 4096,
        })
    }

    #[tokio::test]
    async fn tflush_abandons_a_read_held_on_its_backend_fetch() {
        let (_filesystem, handler, controls) = held_segment_session().await;
        let mut io = DispatchFixture::new(handler, 4);
        assert!(admission_is_idle(&io.admission));

        controls.hold_get_bodies(usize::MAX);
        let gets = controls.get_count();
        io.dispatch(10, held_read()).await;
        wait_until(
            || controls.get_count() > gets,
            "the read must reach its backend fetch",
        )
        .await;
        io.expect_quiet("the read must wait on its held backend fetch")
            .await;
        assert!(!admission_is_idle(&io.admission));

        let flushed = std::time::Instant::now();
        io.dispatch(11, Message::Tflush(Tflush { oldtag: 10 }))
            .await;
        let (tag, response) = tokio::time::timeout(PROMPT_FLUSH, io.rx.recv())
            .await
            .expect("Rflush must not wait for the held backend fetch")
            .expect("response channel")
            .into_parts();
        assert_eq!(tag, 11);
        assert!(matches!(decode(&response).body, Message::Rflush(Rflush)));
        assert!(flushed.elapsed() < PROMPT_FLUSH);
        wait_until(
            || admission_is_idle(&io.admission),
            "the cancelled read must return its admission bytes",
        )
        .await;

        controls.release_get_bodies();
        io.expect_quiet("a cancelled read must never answer after Rflush")
            .await;

        // The client may reuse the flushed tag as soon as Rflush arrives.
        io.dispatch(10, held_read()).await;
        let (tag, reply) = io.recv("a read on the reused tag").await;
        assert_eq!(tag, 10);
        let Message::Rread(read) = reply.body else {
            panic!("the reused tag must carry the new read's reply");
        };
        assert_eq!(read.data.as_ref(), &held_file_payload()[..4096]);
    }

    #[tokio::test]
    async fn tflush_abandons_a_held_fsync_without_reporting_success() {
        let fs = ZeroFS::new_in_memory().await.unwrap();
        let entered = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        fs.flush_coordinator.set_local_durability_barrier({
            let entered = Arc::clone(&entered);
            let release = Arc::clone(&release);
            Arc::new(move || {
                let entered = Arc::clone(&entered);
                let release = Arc::clone(&release);
                Box::pin(async move {
                    entered.notify_one();
                    release.notified().await;
                    Ok(())
                })
            })
        });
        let filesystem = Arc::new(fs);
        let handler = Arc::new(NinePHandler::new(
            Arc::clone(&filesystem),
            Arc::new(FileLockManager::new()),
        ));
        plain_root_session(&handler).await;
        let mut io = DispatchFixture::new(handler, 4);
        let fsync = || {
            Message::Tfsync(ninep_proto::Tfsync {
                fid: 1,
                datasync: 0,
            })
        };

        io.dispatch(10, fsync()).await;
        tokio::time::timeout(TEST_TIMEOUT, entered.notified())
            .await
            .expect("Tfsync must reach its durability wait");
        io.dispatch(11, Message::Tflush(Tflush { oldtag: 10 }))
            .await;
        let (tag, response) = tokio::time::timeout(PROMPT_FLUSH, io.rx.recv())
            .await
            .expect("Rflush must not wait for the durability barrier")
            .expect("response channel")
            .into_parts();
        assert_eq!(tag, 11);
        assert!(matches!(decode(&response).body, Message::Rflush(Rflush)));
        wait_until(
            || admission_is_idle(&io.admission),
            "the cancelled fsync must return its admission",
        )
        .await;

        release.notify_one();
        io.expect_quiet("a cancelled fsync must never report success after Rflush")
            .await;

        // The durability machinery is intact: a later fsync completes.
        io.dispatch(12, fsync()).await;
        tokio::time::timeout(TEST_TIMEOUT, entered.notified())
            .await
            .expect("the next Tfsync must reach its durability wait");
        release.notify_one();
        let (tag, reply) = io.recv("the next fsync").await;
        assert_eq!(tag, 12);
        assert!(matches!(reply.body, Message::Rfsync(_)));
    }

    fn mkdir_frame(tag: u16, op_id: [u8; 16], op_flags: u8, name: &[u8]) -> Bytes {
        Bytes::from(
            P9Message::new_with_op_id_flags_and_origin(
                tag,
                op_id,
                op_flags,
                0,
                Message::Tmkdir(Tmkdir {
                    dfid: 1,
                    name: P9String::new(name.to_vec()),
                    mode: 0o755,
                    gid: 0,
                }),
            )
            .to_bytes_ctx(true)
            .unwrap(),
        )
    }

    async fn walk_errno(handler: &NinePHandler, name: &[u8]) -> Option<u32> {
        let walk = handler
            .handle_message(
                90,
                Message::Twalk(ninep_proto::Twalk {
                    fid: 1,
                    newfid: 90,
                    nwname: 1,
                    wnames: vec![P9String::new(name.to_vec())],
                }),
            )
            .await;
        match walk.body {
            Message::Rwalk(_) => {
                let clunk = handler
                    .handle_message(91, Message::Tclunk(Tclunk { fid: 90 }))
                    .await;
                assert!(matches!(clunk.body, Message::Rclunk(_)));
                None
            }
            Message::Rlerror(error) => Some(error.ecode),
            other => panic!("unexpected walk reply {other:?}"),
        }
    }

    #[tokio::test]
    async fn tflush_before_submission_applies_nothing() {
        let (filesystem, handler) = in_memory_handler().await;
        establish_session(&handler, 0, b"root", 0, Some([0; 16])).await;
        let op_id = [0x71; 16];

        // The parent's inode lock holds mkdir before it submits its commit.
        let parent_lock = filesystem.lock_manager.acquire(0).await;
        let mut io = DispatchFixture::new(Arc::clone(&handler), 4);
        io.dispatch_frame(mkdir_frame(10, op_id, 0, b"cancelled"))
            .await;
        io.expect_quiet("mkdir must wait for the parent lock").await;

        io.dispatch(11, Message::Tflush(Tflush { oldtag: 10 }))
            .await;
        let (tag, response) = tokio::time::timeout(PROMPT_FLUSH, io.rx.recv())
            .await
            .expect("Rflush must not wait for a mutation that has not submitted")
            .expect("response channel")
            .into_parts();
        assert_eq!(tag, 11);
        assert!(matches!(decode(&response).body, Message::Rflush(Rflush)));
        wait_until(
            || filesystem.dedup.stats().inflight_ids == 0,
            "the cancelled mkdir must release its operation reservation",
        )
        .await;

        drop(parent_lock);
        io.expect_quiet("a cancelled mkdir must never answer after Rflush")
            .await;
        assert_eq!(
            walk_errno(&handler, b"cancelled").await,
            Some(crate::linux_errno::ENOENT),
            "a mutation cancelled before submission must apply nothing"
        );
        assert!(filesystem.dedup.get(&op_id).is_none());

        // Nothing was applied, so the client may send the operation again.
        io.dispatch_frame(mkdir_frame(12, op_id, 0, b"cancelled"))
            .await;
        let (tag, reply) = io.recv("the resent mkdir").await;
        assert_eq!(tag, 12);
        assert!(matches!(reply.body, Message::Rmkdir(_)));
        assert_eq!(walk_errno(&handler, b"cancelled").await, None);
    }

    #[tokio::test]
    async fn tflush_after_submission_waits_for_the_single_applied_reply() {
        let (filesystem, handler) = in_memory_handler().await;
        establish_session(&handler, 0, b"root", 0, Some([0; 16])).await;
        let op_id = [0x72; 16];
        let (reached, resume) = pause_success_terminal_dedup(op_id);
        let mut io = DispatchFixture::new(Arc::clone(&handler), 4);
        io.dispatch_frame(mkdir_frame(10, op_id, 0, b"applied"))
            .await;
        tokio::time::timeout(TEST_TIMEOUT, reached)
            .await
            .expect("mkdir must apply and reach its terminal pause")
            .unwrap();

        io.dispatch(11, Message::Tflush(Tflush { oldtag: 10 }))
            .await;
        io.expect_quiet("Rflush must wait for an applied mutation to answer")
            .await;
        resume.send(()).unwrap();

        let (tag, original) = io.recv("the applied mkdir reply").await;
        assert_eq!(tag, 10, "the applied reply must precede Rflush");
        let Message::Rmkdir(original) = original.body else {
            panic!("the applied mkdir must report success");
        };
        assert_eq!(io.recv_flush("Rflush after the applied reply").await, 11);

        // A retry of the same operation replays the applied result.
        io.dispatch_frame(mkdir_frame(
            12,
            op_id,
            ninep_proto::P9_OP_FLAG_RETRY,
            b"applied",
        ))
        .await;
        let (tag, retried) = io.recv("the retried mkdir").await;
        assert_eq!(tag, 12);
        let Message::Rmkdir(retried) = retried.body else {
            panic!("the retry must replay the applied result");
        };
        assert_eq!(retried.qid.path, original.qid.path);

        // The directory exists exactly once.
        io.dispatch_frame(mkdir_frame(13, [0x73; 16], 0, b"applied"))
            .await;
        let (_, second) = io.recv("a new operation on the same name").await;
        assert!(matches!(
            second.body,
            Message::Rlerror(Rlerror { ecode }) if ecode == crate::linux_errno::EEXIST
        ));
        drop(filesystem);
    }

    struct SaturatedConnection {
        client: tokio::io::DuplexStream,
        rx: mpsc::Receiver<P9Response>,
        admission: P9ConnectionAdmission,
        reader: tokio::task::JoinHandle<anyhow::Result<()>>,
    }

    impl SaturatedConnection {
        /// Run the real connection reader with room for exactly one request.
        fn start(handler: Arc<NinePHandler>) -> Self {
            let (client, server) = tokio::io::duplex(64 * 1024);
            let (tx, rx) = mpsc::channel(8);
            let byte_limit = P9_MAX_MSIZE as usize * 4;
            let global = P9GlobalAdmission::for_test(byte_limit, 1);
            let admission = global.connection_for_test(byte_limit, 1);
            let reader_admission = admission.clone();
            let reader = tokio::spawn(async move {
                handle_client_loop(
                    handler,
                    server,
                    tx,
                    CancellationToken::new(),
                    CancellationToken::new(),
                    &reader_admission,
                    &TaskTracker::new(),
                )
                .await
            });
            Self {
                client,
                rx,
                admission,
                reader,
            }
        }

        async fn send(&mut self, tag: u16, body: Message) {
            self.client.write_all(&frame(tag, body)).await.unwrap();
        }

        async fn recv_within(&mut self, bound: Duration, context: &str) -> (u16, P9Message) {
            let (tag, bytes) = tokio::time::timeout(bound, self.rx.recv())
                .await
                .expect(context)
                .expect("response channel")
                .into_parts();
            (tag, decode(&bytes))
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

    fn plain_mkdir(name: &[u8]) -> Message {
        Message::Tmkdir(Tmkdir {
            dfid: 1,
            name: P9String::new(name.to_vec()),
            mode: 0o755,
            gid: 0,
        })
    }

    fn root_getattr() -> Message {
        Message::Tgetattr(Tgetattr {
            fid: 1,
            request_mask: GETATTR_ALL,
        })
    }

    #[tokio::test]
    async fn saturated_connection_still_reads_tflush() {
        let (filesystem, handler) = in_memory_handler().await;
        plain_root_session(&handler).await;
        let parent_lock = filesystem.lock_manager.acquire(0).await;
        let mut connection = SaturatedConnection::start(Arc::clone(&handler));

        // The held mkdir owns the connection's only request slot, so the
        // getattr behind it cannot be admitted.
        connection.send(10, plain_mkdir(b"held")).await;
        connection.send(11, root_getattr()).await;
        connection
            .expect_quiet("both requests must wait while the slot is held")
            .await;

        connection
            .send(12, Message::Tflush(Tflush { oldtag: 10 }))
            .await;
        let mut replies = Vec::new();
        for _ in 0..2 {
            replies.push(
                connection
                    .recv_within(
                        PROMPT_FLUSH,
                        "a saturated connection must still read Tflush",
                    )
                    .await,
            );
        }
        replies.sort_by_key(|(tag, _)| *tag);
        assert_eq!(
            replies[0].0, 11,
            "the queued getattr runs once the slot is free"
        );
        assert!(matches!(replies[0].1.body, Message::Rgetattr(_)));
        assert_eq!(replies[1].0, 12);
        assert!(matches!(replies[1].1.body, Message::Rflush(Rflush)));

        drop(parent_lock);
        connection
            .expect_quiet("the cancelled mkdir must never answer after Rflush")
            .await;
        assert_eq!(
            walk_errno(&handler, b"held").await,
            Some(crate::linux_errno::ENOENT)
        );
        assert!(admission_is_idle(&connection.admission));
        drop(connection.client);
        connection.reader.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn tflush_cancels_a_request_still_waiting_for_admission() {
        let (filesystem, handler) = in_memory_handler().await;
        plain_root_session(&handler).await;
        let parent_lock = filesystem.lock_manager.acquire(0).await;
        let mut connection = SaturatedConnection::start(Arc::clone(&handler));

        connection.send(10, plain_mkdir(b"kept")).await;
        connection.send(11, root_getattr()).await;
        connection
            .send(12, Message::Tflush(Tflush { oldtag: 11 }))
            .await;
        let (tag, response) = connection
            .recv_within(
                PROMPT_FLUSH,
                "Tflush of a queued request must answer at once",
            )
            .await;
        assert_eq!(tag, 12);
        assert!(matches!(response.body, Message::Rflush(Rflush)));

        drop(parent_lock);
        let (tag, mkdir) = connection
            .recv_within(TEST_TIMEOUT, "the held mkdir completes")
            .await;
        assert_eq!(tag, 10);
        assert!(matches!(mkdir.body, Message::Rmkdir(_)));
        connection
            .expect_quiet("the cancelled getattr must never run or answer")
            .await;
        wait_until(
            || admission_is_idle(&connection.admission),
            "every request must release admission",
        )
        .await;
        drop(connection.client);
        connection.reader.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn tflush_needs_no_serving_authority() {
        let (filesystem, lease, raw_db) = leased_filesystem_for_response_gate().await;
        let handler = NinePHandler::new(Arc::clone(&filesystem), Arc::new(FileLockManager::new()));
        lease.revoke();

        // flush(5): "A Tflush can never be responded to by an Rerror message."
        let flushed = handler
            .handle_message(41, Message::Tflush(Tflush { oldtag: 40 }))
            .await;
        assert!(matches!(flushed.body, Message::Rflush(Rflush)));
        let bytes = flushed.to_bytes().unwrap();
        assert!(
            response_may_be_emitted(&filesystem.db, &bytes),
            "Rflush reports no filesystem state, so a deposed server may still send it"
        );
        raw_db.close().await.unwrap();
    }
}
