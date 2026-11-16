use super::*;
use bytes::Bytes;
use ninep_proto::*;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::UnixStream;

struct WireClient {
    stream: UnixStream,
    tag: u16,
    shutdown: CancellationToken,
    _dir: tempfile::TempDir,
}

impl Drop for WireClient {
    fn drop(&mut self) {
        self.shutdown.cancel();
    }
}

impl WireClient {
    async fn rpc(&mut self, body: Message) -> Message {
        self.tag += 1;
        let tag = if matches!(body, Message::Tversion(_)) {
            u16::MAX
        } else {
            self.tag
        };
        let frame = P9Message::new(tag, body).to_bytes().unwrap();
        self.stream.write_all(&frame).await.unwrap();
        let size = self.stream.read_u32_le().await.unwrap();
        let mut response = vec![0; size as usize];
        response[..4].copy_from_slice(&size.to_le_bytes());
        self.stream.read_exact(&mut response[4..]).await.unwrap();
        let message = P9Message::from_bytes_ctx(&response, false).unwrap();
        assert_eq!(message.tag, tag);
        message.body
    }
    async fn create(&mut self, fid: u32, size: u64, flags: u32) -> Message {
        self.rpc(Message::Txattrcreate(Txattrcreate {
            fid,
            name: P9String::new(b"user.key".to_vec()),
            size,
            flags,
        }))
        .await
    }
    async fn write(&mut self, fid: u32, offset: u64, data: &[u8]) -> Message {
        self.rpc(Message::Twrite(Twrite {
            fid,
            offset,
            count: data.len() as u32,
            data: Bytes::copy_from_slice(data).into(),
        }))
        .await
    }
    async fn clunk(&mut self, fid: u32) -> Message {
        self.rpc(Message::Tclunk(Tclunk { fid })).await
    }
    async fn clone_file(&mut self, newfid: u32) {
        assert!(matches!(
            self.rpc(Message::Twalk(Twalk {
                fid: 1,
                newfid,
                nwname: 0,
                wnames: vec![]
            }))
            .await,
            Message::Rwalk(_)
        ));
    }
}

async fn wire_setup() -> (WireClient, Arc<ZeroFS>, u64) {
    let fs = Arc::new(ZeroFS::new_in_memory().await.unwrap());
    let id = fs
        .create(
            &crate::fs::permissions::Credentials::from_auth_context(
                &(&crate::test_helpers::test_helpers_mod::test_auth()).into(),
            ),
            0,
            b"file",
            &crate::fs::types::SetAttributes::default(),
        )
        .await
        .unwrap()
        .0;
    let dir = tempfile::tempdir().unwrap();
    let socket = dir.path().join("x.sock");
    let shutdown = start_server(fs.clone(), socket.clone());
    let mut stream = None;
    for _ in 0..100 {
        match UnixStream::connect(&socket).await {
            Ok(s) => {
                stream = Some(s);
                break;
            }
            Err(_) => tokio::time::sleep(Duration::from_millis(20)).await,
        }
    }
    let mut client = WireClient {
        stream: stream.expect("server did not start"),
        tag: 0,
        shutdown,
        _dir: dir,
    };
    assert!(matches!(
        client
            .rpc(Message::Tversion(Tversion {
                msize: 64 * 1024,
                version: P9String::new(b"9P2000.L".to_vec())
            }))
            .await,
        Message::Rversion(_)
    ));
    assert!(matches!(
        client
            .rpc(Message::Tattach(Tattach {
                fid: 0,
                afid: NOFID,
                uname: P9String::new(vec![]),
                aname: P9String::new(vec![]),
                n_uname: 1000
            }))
            .await,
        Message::Rattach(_)
    ));
    assert!(matches!(
        client
            .rpc(Message::Twalk(Twalk {
                fid: 0,
                newfid: 1,
                nwname: 1,
                wnames: vec![P9String::new(b"file".to_vec())]
            }))
            .await,
        Message::Rwalk(_)
    ));
    (client, fs, id)
}

fn errno(reply: Message, expected: u32) {
    match reply {
        Message::Rlerror(e) => assert_eq!(e.ecode, expected),
        other => panic!("expected errno {expected}, got {other:?}"),
    }
}

#[tokio::test]
async fn xattr_wire_two_mib_and_remove_roundtrip() {
    let (mut c, fs, id) = wire_setup().await;
    c.clone_file(2).await;
    errno(c.create(2, 2_097_153, 0).await, 7);
    assert!(matches!(
        c.create(2, 2_097_152, 1).await,
        Message::Rxattrcreate(_)
    ));
    let payload = (0..2_097_152).map(|i| (i % 251) as u8).collect::<Vec<_>>();
    for (i, chunk) in payload.chunks(32 * 1024).enumerate() {
        assert!(matches!(
            c.write(2, (i * 32 * 1024) as u64, chunk).await,
            Message::Rwrite(_)
        ));
    }
    assert_eq!(
        fs.xattr_store.get(id, b"user.key").await,
        Err(crate::fs::errors::FsError::NoData)
    );
    assert!(matches!(c.clunk(2).await, Message::Rclunk(_)));
    assert!(matches!(
        c.rpc(Message::Txattrwalk(Txattrwalk {
            fid: 1,
            newfid: 3,
            name: P9String::new(b"user.key".to_vec())
        }))
        .await,
        Message::Rxattrwalk(Rxattrwalk { size: 2_097_152 })
    ));
    let mut read = Vec::new();
    while read.len() < payload.len() {
        match c
            .rpc(Message::Tread(Tread {
                fid: 3,
                offset: read.len() as u64,
                count: 32 * 1024,
            }))
            .await
        {
            Message::Rread(r) => {
                assert!(!r.data.is_empty());
                read.extend_from_slice(&r.data);
            }
            other => panic!("{other:?}"),
        }
    }
    assert_eq!(read, payload);
    assert!(matches!(c.clunk(3).await, Message::Rclunk(_)));
    assert!(matches!(
        c.rpc(Message::Txattrwalk(Txattrwalk {
            fid: 1,
            newfid: 4,
            name: P9String::new(vec![])
        }))
        .await,
        Message::Rxattrwalk(Rxattrwalk { size: 9 })
    ));
    match c
        .rpc(Message::Tread(Tread {
            fid: 4,
            offset: 0,
            count: 1024,
        }))
        .await
    {
        Message::Rread(r) => assert_eq!(r.data.as_ref(), b"user.key\0"),
        other => panic!("{other:?}"),
    }
    c.clunk(4).await;
    c.clone_file(5).await;
    assert!(matches!(c.create(5, 0, 2).await, Message::Rxattrcreate(_)));
    assert!(matches!(c.clunk(5).await, Message::Rclunk(_)));
    errno(
        c.rpc(Message::Txattrwalk(Txattrwalk {
            fid: 1,
            newfid: 6,
            name: P9String::new(b"user.key".to_vec()),
        }))
        .await,
        61,
    );
}

#[tokio::test]
async fn xattr_wire_rejects_incomplete_and_out_of_bounds_writes() {
    let (mut c, fs, id) = wire_setup().await;
    c.clone_file(2).await;
    assert!(matches!(c.create(2, 4, 0).await, Message::Rxattrcreate(_)));
    errno(c.write(2, 3, b"xx").await, 22);
    assert!(matches!(c.write(2, 2, b"cd").await, Message::Rwrite(_)));
    assert!(matches!(c.write(2, 2, b"cd").await, Message::Rwrite(_)));
    errno(c.clunk(2).await, 22);
    assert!(fs.xattr_store.list(id).await.unwrap().is_empty());
    c.clone_file(3).await;
    assert!(matches!(c.create(3, 4, 0).await, Message::Rxattrcreate(_)));
    c.write(3, 2, b"cd").await;
    c.write(3, 0, b"ab").await;
    assert!(matches!(c.clunk(3).await, Message::Rclunk(_)));
    assert_eq!(
        fs.xattr_store.get(id, b"user.key").await.unwrap(),
        b"abcd".as_slice()
    );
    c.clone_file(4).await;
    assert!(matches!(c.create(4, 1, 1).await, Message::Rxattrcreate(_)));
    c.write(4, 0, b"x").await;
    errno(c.clunk(4).await, 17);
    assert_eq!(
        fs.xattr_store.get(id, b"user.key").await.unwrap(),
        b"abcd".as_slice()
    );
}

#[tokio::test]
async fn xattr_wire_disconnect_discards_staged_value_and_releases_pin() {
    let (mut c, fs, id) = wire_setup().await;
    c.clone_file(2).await;
    assert!(matches!(c.create(2, 4, 0).await, Message::Rxattrcreate(_)));
    c.write(2, 0, b"abcd").await;
    drop(c);
    tokio::time::timeout(Duration::from_secs(5), async {
        while fs.open_handle_count(id) != 0 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    assert!(fs.xattr_store.list(id).await.unwrap().is_empty());
}
