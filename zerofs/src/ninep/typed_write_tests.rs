use super::NinePServer;
use crate::fs::ZeroFS;
use crate::fs::mutation::config::{
    ClientDurabilityTarget, FilesystemWriteAckMode, FilesystemWriteAckSettings,
    FilesystemWriteAckSource,
};
use crate::fs::permissions::Credentials;
use crate::fs::types::SetAttributes;
use bytes::Bytes;
use ninep_proto::{
    DekuBytes, Message, P9_OP_FLAG_RETRY, P9Message, P9String, Tattach, Tlopen, Tversion, Twalk,
    Twrite, VERSION_9P2000L, VERSION_9P2000L_ZEROFS,
};
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::UnixStream;
use tokio_util::sync::CancellationToken;

struct FramedNinePClient {
    stream: UnixStream,
    private: bool,
}

impl FramedNinePClient {
    async fn connect(socket: &Path) -> Self {
        for _ in 0..100 {
            if let Ok(stream) = UnixStream::connect(socket).await {
                return Self {
                    stream,
                    private: false,
                };
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        panic!("production 9P client never connected to Unix server");
    }

    async fn send(&mut self, tag: u16, body: Message) -> P9Message {
        self.send_frame(P9Message::new(tag, body)).await;
        self.receive().await
    }

    async fn send_frame(&mut self, message: P9Message) {
        self.stream
            .write_all(&message.to_bytes_ctx(self.private).unwrap())
            .await
            .unwrap();
    }

    async fn negotiate(&mut self, version: &[u8]) {
        self.private = false;
        let response = self
            .send(
                u16::MAX,
                Message::Tversion(Tversion {
                    msize: 256 * 1024,
                    version: P9String::new(version.to_vec()),
                }),
            )
            .await;
        assert!(matches!(response.body, Message::Rversion(_)));
        self.private = version == VERSION_9P2000L_ZEROFS;
    }

    async fn attach_root(&mut self) {
        let response = self
            .send(
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
        assert!(matches!(response.body, Message::Rattach(_)));
    }

    async fn walk_open(&mut self, tag: u16, fid: u32, name: &[u8]) {
        let walk = self
            .send(
                tag,
                Message::Twalk(Twalk {
                    fid: 1,
                    newfid: fid,
                    nwname: 1,
                    wnames: vec![P9String::new(name.to_vec())],
                }),
            )
            .await;
        assert!(matches!(walk.body, Message::Rwalk(_)));
        let open = self
            .send(
                tag + 10,
                Message::Tlopen(Tlopen {
                    fid,
                    flags: libc::O_WRONLY as u32,
                }),
            )
            .await;
        assert!(matches!(open.body, Message::Rlopen(_)));
    }

    async fn receive(&mut self) -> P9Message {
        let mut size = [0u8; 4];
        self.stream.read_exact(&mut size).await.unwrap();
        let size = u32::from_le_bytes(size) as usize;
        assert!(size >= 7, "server returned undersized 9P frame");
        let mut frame = vec![0u8; size];
        frame[..4].copy_from_slice(&(size as u32).to_le_bytes());
        self.stream.read_exact(&mut frame[4..]).await.unwrap();
        P9Message::from_owned_bytes_ctx(Bytes::from(frame), false).unwrap()
    }
}

async fn wait_for_slots(cache: &crate::fs::mutation::request_cache::RequestCache, expected: usize) {
    tokio::time::timeout(Duration::from_secs(2), async {
        while cache.used_slots() != expected {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap_or_else(|_| {
        panic!(
            "shared request cache never reached {expected} slots; current={}",
            cache.used_slots()
        )
    });
}

fn test_creds() -> Credentials {
    Credentials {
        uid: 1000,
        gid: 1000,
        gid_known: true,
        groups: [1000; 16],
        groups_count: 1,
        groups_complete: true,
    }
}

async fn volatile_fs_with_file(name: &[u8]) -> (Arc<ZeroFS>, u64) {
    let mut fs = ZeroFS::new_in_memory().await.unwrap();
    fs.write_ack = FilesystemWriteAckSettings {
        mode: FilesystemWriteAckMode::VolatileMemory,
        volatile_memory_bytes: 8 * 1024 * 1024,
        volatile_max_operations: 1024,
        source: FilesystemWriteAckSource::Filesystem,
        client_durability_target: ClientDurabilityTarget::LocalSsd,
    };
    let (file, _) = fs
        .create(&test_creds(), 0, name, &SetAttributes::default())
        .await
        .unwrap();
    (Arc::new(fs), file)
}

async fn stop_server(
    client: FramedNinePClient,
    shutdown: CancellationToken,
    server_task: tokio::task::JoinHandle<std::io::Result<()>>,
    fs: &ZeroFS,
) {
    drop(client);
    shutdown.cancel();
    tokio::time::timeout(Duration::from_secs(2), server_task)
        .await
        .expect("9P server did not stop")
        .expect("9P server task panicked")
        .expect("9P server failed");
    fs.stop_new_mutation_admission();
    fs.stop_mutation_workers().await;
    fs.flush_coordinator.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn standard_framed_writes_without_operation_ids_do_not_collapse() {
    let (fs, file) = volatile_fs_with_file(b"standard-write").await;
    let temp = tempfile::tempdir().unwrap();
    let socket = temp.path().join("ninep.sock");
    let shutdown = CancellationToken::new();
    let server = NinePServer::new_unix(Arc::clone(&fs), socket.clone());
    let server_task = tokio::spawn({
        let shutdown = shutdown.clone();
        async move { server.start(shutdown).await }
    });
    let mut client = FramedNinePClient::connect(&socket).await;

    client.negotiate(VERSION_9P2000L).await;
    client.attach_root().await;
    let request_cache = fs
        .mutation_coordinator
        .get()
        .expect("9P handler installed the shared mutation coordinator")
        .request_cache();
    for (tag, fid) in [(2, 2), (3, 3)] {
        client.walk_open(tag, fid, b"standard-write").await;
    }

    let inode_lock = fs.lock_manager.acquire(file).await;
    for (tag, fid, offset, payload) in [
        (20, 2, 0, b"first".as_slice()),
        (21, 3, 5, b"second".as_slice()),
    ] {
        client
            .send_frame(P9Message::new(
                tag,
                Message::Twrite(Twrite {
                    fid,
                    offset,
                    count: payload.len() as u32,
                    data: DekuBytes::from(payload.to_vec()),
                }),
            ))
            .await;
    }
    wait_for_slots(&request_cache, 2).await;
    drop(inode_lock);

    let mut response_tags = Vec::new();
    for _ in 0..2 {
        let response = client.receive().await;
        assert!(matches!(response.body, Message::Rwrite(_)));
        response_tags.push(response.tag);
    }
    response_tags.sort_unstable();
    assert_eq!(response_tags, [20, 21]);
    wait_for_slots(&request_cache, 0).await;

    stop_server(client, shutdown, server_task, &fs).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn private_framed_writes_use_full_operation_id_width() {
    let (fs, file) = volatile_fs_with_file(b"private-write").await;
    let temp = tempfile::tempdir().unwrap();
    let socket = temp.path().join("ninep.sock");
    let shutdown = CancellationToken::new();
    let server = NinePServer::new_unix(Arc::clone(&fs), socket.clone());
    let server_task = tokio::spawn({
        let shutdown = shutdown.clone();
        async move { server.start(shutdown).await }
    });
    let mut client = FramedNinePClient::connect(&socket).await;
    client.negotiate(VERSION_9P2000L_ZEROFS).await;
    client.attach_root().await;
    for (tag, fid) in [(2, 2), (3, 3)] {
        client.walk_open(tag, fid, b"private-write").await;
    }
    let request_cache = fs
        .mutation_coordinator
        .get()
        .expect("9P handler installed the shared mutation coordinator")
        .request_cache();

    let first_operation = [0xa5; 16];
    let mut second_operation = first_operation;
    second_operation[15] = 0x5a;
    assert_eq!(first_operation[..8], second_operation[..8]);
    let origin_epoch = 0x1122_3344_5566_7788;
    let inode_lock = fs.lock_manager.acquire(file).await;
    for (tag, fid, operation_id, offset) in
        [(20, 2, first_operation, 0), (21, 3, second_operation, 4)]
    {
        client
            .send_frame(P9Message::new_with_op_id_flags_and_origin(
                tag,
                operation_id,
                0,
                origin_epoch,
                Message::Twrite(Twrite {
                    fid,
                    offset,
                    count: 4,
                    data: DekuBytes::from(b"wide".to_vec()),
                }),
            ))
            .await;
    }
    wait_for_slots(&request_cache, 2).await;
    drop(inode_lock);
    let mut response_tags = Vec::new();
    for _ in 0..2 {
        let response = client.receive().await;
        assert!(matches!(response.body, Message::Rwrite(_)));
        response_tags.push(response.tag);
    }
    response_tags.sort_unstable();
    assert_eq!(response_tags, [20, 21]);
    wait_for_slots(&request_cache, 0).await;
    stop_server(client, shutdown, server_task, &fs).await;
}

#[tokio::test(flavor = "current_thread")]
#[ignore = "RED: requires typed Write publication at volatile RAM acceptance"]
async fn private_framed_reconnect_retry_replays_typed_write_result() {
    let (fs, _) = volatile_fs_with_file(b"retry-write").await;
    let temp = tempfile::tempdir().unwrap();
    let socket = temp.path().join("ninep.sock");
    let shutdown = CancellationToken::new();
    let server = NinePServer::new_unix(Arc::clone(&fs), socket.clone());
    let server_task = tokio::spawn({
        let shutdown = shutdown.clone();
        async move { server.start(shutdown).await }
    });
    let request_cache = {
        let mut first = FramedNinePClient::connect(&socket).await;
        first.negotiate(VERSION_9P2000L_ZEROFS).await;
        first.attach_root().await;
        first.walk_open(2, 2, b"retry-write").await;
        let request_cache = fs
            .mutation_coordinator
            .get()
            .expect("9P handler installed the shared mutation coordinator")
            .request_cache();
        let operation_id = [0x93; 16];
        let origin_epoch = 0x8877_6655_4433_2211;
        let payload = b"typed-retry";
        first
            .send_frame(P9Message::new_with_op_id_flags_and_origin(
                20,
                operation_id,
                0,
                origin_epoch,
                Message::Twrite(Twrite {
                    fid: 2,
                    offset: 0,
                    count: payload.len() as u32,
                    data: DekuBytes::from(payload.to_vec()),
                }),
            ))
            .await;
        let first_reply = first.receive().await;
        assert!(matches!(first_reply.body, Message::Rwrite(_)));
        wait_for_slots(&request_cache, 0).await;
        drop(first);

        let mut retry = FramedNinePClient::connect(&socket).await;
        retry.negotiate(VERSION_9P2000L_ZEROFS).await;
        retry.attach_root().await;
        retry.walk_open(2, 2, b"retry-write").await;
        retry
            .send_frame(P9Message::new_with_op_id_flags_and_origin(
                21,
                operation_id,
                P9_OP_FLAG_RETRY,
                origin_epoch,
                Message::Twrite(Twrite {
                    fid: 2,
                    offset: 0,
                    count: payload.len() as u32,
                    data: DekuBytes::from(payload.to_vec()),
                }),
            ))
            .await;
        let retry_reply = retry.receive().await;
        assert!(matches!(retry_reply.body, Message::Rwrite(_)));
        stop_server(retry, shutdown, server_task, &fs).await;
        request_cache
    };
    assert_eq!(request_cache.used_slots(), 0);
}
