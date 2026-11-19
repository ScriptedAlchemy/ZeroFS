use super::*;
use bytes::Bytes;
use ninep_proto::*;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::UnixStream;

struct WireClient {
    stream: UnixStream,
    tag: u16,
    zerofs: bool,
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
        let frame = P9Message::new(tag, body).to_bytes_ctx(self.zerofs).unwrap();
        self.stream.write_all(&frame).await.unwrap();
        let size = self.stream.read_u32_le().await.unwrap();
        let mut response = vec![0; size as usize];
        response[..4].copy_from_slice(&size.to_le_bytes());
        self.stream.read_exact(&mut response[4..]).await.unwrap();
        let message = P9Message::from_bytes_ctx(&response, self.zerofs).unwrap();
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
    async fn create_named(&mut self, fid: u32, name: &[u8], size: u64, flags: u32) -> Message {
        self.rpc(Message::Txattrcreate(Txattrcreate {
            fid,
            name: P9String::new(name.to_vec()),
            size,
            flags,
        }))
        .await
    }
    async fn read_named(&mut self, newfid: u32, name: &[u8]) -> Result<Vec<u8>, u32> {
        match self
            .rpc(Message::Txattrwalk(Txattrwalk {
                fid: 1,
                newfid,
                name: P9String::new(name.to_vec()),
            }))
            .await
        {
            Message::Rxattrwalk(_) => {}
            Message::Rlerror(e) => return Err(e.ecode),
            other => panic!("unexpected {other:?}"),
        }
        let data = match self
            .rpc(Message::Tread(Tread {
                fid: newfid,
                offset: 0,
                count: 4096,
            }))
            .await
        {
            Message::Rread(r) => r.data.to_vec(),
            other => panic!("unexpected {other:?}"),
        };
        self.clunk(newfid).await;
        Ok(data)
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
    wire_setup_version(b"9P2000.L").await
}

async fn wire_setup_version(version: &[u8]) -> (WireClient, Arc<ZeroFS>, u64) {
    let mode = if version == VERSION_9P2000L_ZEROFS_CI {
        crate::fs::name_mode::NameMode::Insensitive
    } else {
        crate::fs::name_mode::NameMode::Sensitive
    };
    let fs = Arc::new(ZeroFS::new_in_memory_with_name_mode(mode).await.unwrap());
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
        zerofs: false,
        shutdown,
        _dir: dir,
    };
    assert!(matches!(
        client
            .rpc(Message::Tversion(Tversion {
                msize: 64 * 1024,
                version: P9String::new(version.to_vec())
            }))
            .await,
        Message::Rversion(_)
    ));
    client.zerofs = version != b"9P2000.L";
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

#[tokio::test]
async fn xattr_wire_cancelled_clunk_then_unlink_leaves_no_rows() {
    let (mut c, fs, id) = wire_setup().await;
    c.clone_file(2).await;
    assert!(matches!(c.create(2, 4, 0).await, Message::Rxattrcreate(_)));
    c.write(2, 0, b"abcd").await;
    let block_apply = fs.db.flush_barrier().write_owned().await;
    let apply_reached = fs.write_coordinator.probe_next_apply();
    let clunking = tokio::spawn(async move { c.clunk(2).await });
    apply_reached.await.unwrap();
    clunking.abort();
    assert!(clunking.await.unwrap_err().is_cancelled());
    assert!(
        tokio::time::timeout(Duration::from_millis(50), fs.lock_manager.acquire(id))
            .await
            .is_err()
    );
    let removing_fs = Arc::clone(&fs);
    let removing = tokio::spawn(async move {
        removing_fs
            .remove(
                &(&crate::test_helpers::test_helpers_mod::test_auth()).into(),
                0,
                b"file",
            )
            .await
    });
    drop(block_apply);
    removing.await.unwrap().unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            fs.reclaim_if_unreferenced(id).await;
            if matches!(
                fs.inode_store.get(id).await,
                Err(crate::fs::errors::FsError::NotFound)
            ) {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(fs.xattr_store.list(id).await.unwrap().is_empty());
}

#[tokio::test]
async fn xattr_wire_private_dialects_preserve_empty_replace_and_remove_explicitly() {
    for version in [VERSION_9P2000L_ZEROFS, VERSION_9P2000L_ZEROFS_CI] {
        let (mut c, fs, id) = wire_setup_version(version).await;
        c.clone_file(2).await;
        assert!(matches!(c.create(2, 1, 1).await, Message::Rxattrcreate(_)));
        c.write(2, 0, b"x").await;
        assert!(matches!(c.clunk(2).await, Message::Rclunk(_)));
        c.clone_file(3).await;
        assert!(matches!(c.create(3, 0, 2).await, Message::Rxattrcreate(_)));
        assert!(matches!(c.clunk(3).await, Message::Rclunk(_)));
        assert!(
            fs.xattr_store
                .get(id, b"user.key")
                .await
                .unwrap()
                .is_empty()
        );
        c.clone_file(4).await;
        assert!(matches!(
            c.create(4, 0, XATTR_REMOVE_ZEROFS).await,
            Message::Rxattrcreate(_)
        ));
        assert!(matches!(c.clunk(4).await, Message::Rclunk(_)));
        assert_eq!(
            fs.xattr_store.get(id, b"user.key").await,
            Err(crate::fs::errors::FsError::NoData)
        );
        c.clone_file(5).await;
        errno(c.create(5, 1, XATTR_REMOVE_ZEROFS).await, 22);
        errno(c.create(5, 0, XATTR_REMOVE_ZEROFS | 1).await, 22);
        errno(c.create(5, 0, XATTR_REMOVE_ZEROFS | 2).await, 22);
        assert!(matches!(
            c.create(5, 0, XATTR_REMOVE_ZEROFS).await,
            Message::Rxattrcreate(_)
        ));
        errno(c.clunk(5).await, 61);
    }
}

#[tokio::test]
async fn xattr_wire_plain_dialect_rejects_private_remove_flag() {
    let (mut c, _, _) = wire_setup().await;
    c.clone_file(2).await;
    errno(c.create(2, 0, XATTR_REMOVE_ZEROFS).await, 22);
}

/// The frames `v9fs_iop_set_acl` and `v9fs_iop_get_acl` (fs/9p/acl.c) build
/// for a mount that is not `access=client`: the ACL as a plain xattr with
/// flags 0, and size 0 with flags 0 for `setfacl -b`. Linux 7.0 refuses ACL
/// calls on such a mount before they reach v9fs, so these frames come from
/// other 9P clients; the server must still store, serve and enforce them.
#[tokio::test]
async fn xattr_wire_plain_dialect_carries_linux_v9fs_acl_frames() {
    use crate::fs::inode::InodeAttrs;
    use crate::fs::permissions::acl::{POSIX_ACL_ACCESS, POSIX_ACL_DEFAULT, PosixAcl};
    use crate::fs::types::AuthContext;
    let (mut c, fs, id) = wire_setup().await;
    let acl = PosixAcl::from_text("u::rw-,u:2000:r--,g::---,m::r--,o::---").to_bytes();
    c.clone_file(2).await;
    assert!(matches!(
        c.create_named(2, POSIX_ACL_ACCESS, acl.len() as u64, 0)
            .await,
        Message::Rxattrcreate(_)
    ));
    assert!(matches!(c.write(2, 0, &acl).await, Message::Rwrite(_)));
    assert!(matches!(c.clunk(2).await, Message::Rclunk(_)));
    assert_eq!(
        c.read_named(3, POSIX_ACL_ACCESS).await.unwrap(),
        acl.to_vec()
    );
    assert_eq!(fs.inode_store.get(id).await.unwrap().mode() & 0o777, 0o640);
    let named = AuthContext {
        uid: 2000,
        gid: 2000,
        gid_known: true,
        gids: vec![],
        groups_complete: true,
    };
    assert!(fs.read_file(&named, id, 0, 1).await.is_ok());
    let list = c.read_named(4, b"").await.unwrap();
    assert!(
        list.windows(POSIX_ACL_ACCESS.len())
            .any(|w| w == POSIX_ACL_ACCESS)
    );

    // A default ACL on a regular file is refused at commit with EACCES.
    let default = PosixAcl::from_text("u::rwx,g::r-x,o::---").to_bytes();
    c.clone_file(5).await;
    assert!(matches!(
        c.create_named(5, POSIX_ACL_DEFAULT, default.len() as u64, 0)
            .await,
        Message::Rxattrcreate(_)
    ));
    c.write(5, 0, &default).await;
    errno(c.clunk(5).await, 13);

    // `setfacl -b`: size 0, flags 0, no Twrite.
    c.clone_file(6).await;
    assert!(matches!(
        c.create_named(6, POSIX_ACL_ACCESS, 0, 0).await,
        Message::Rxattrcreate(_)
    ));
    assert!(matches!(c.clunk(6).await, Message::Rclunk(_)));
    assert_eq!(c.read_named(7, POSIX_ACL_ACCESS).await, Err(61));
    assert_eq!(
        fs.read_file(&named, id, 0, 1).await.err(),
        Some(crate::fs::errors::FsError::PermissionDenied)
    );
}

/// Linux v9fs encodes an empty `XATTR_REPLACE` and `removexattr` as the same
/// frames, so a plain session keeps removal. This pins the server behaviour
/// the kernel 9P mount matrix observed.
#[tokio::test]
async fn xattr_wire_plain_dialect_empty_replace_frames_remove() {
    let (mut c, fs, id) = wire_setup().await;
    c.clone_file(2).await;
    assert!(matches!(c.create(2, 1, 0).await, Message::Rxattrcreate(_)));
    c.write(2, 0, b"v").await;
    assert!(matches!(c.clunk(2).await, Message::Rclunk(_)));
    // Size 0, flags XATTR_REPLACE, no Twrite: identical for both syscalls.
    c.clone_file(3).await;
    assert!(matches!(c.create(3, 0, 2).await, Message::Rxattrcreate(_)));
    assert!(matches!(c.clunk(3).await, Message::Rclunk(_)));
    assert_eq!(
        fs.xattr_store.get(id, b"user.key").await,
        Err(crate::fs::errors::FsError::NoData)
    );
    // Size 0 with flags 0 or XATTR_CREATE stores an empty value.
    for (fid, flags) in [(4, 0), (5, 1)] {
        if flags == 1 {
            c.clone_file(9).await;
            assert!(matches!(c.create(9, 0, 2).await, Message::Rxattrcreate(_)));
            assert!(matches!(c.clunk(9).await, Message::Rclunk(_)));
        }
        c.clone_file(fid).await;
        assert!(matches!(
            c.create(fid, 0, flags).await,
            Message::Rxattrcreate(_)
        ));
        assert!(matches!(c.clunk(fid).await, Message::Rclunk(_)));
        assert!(
            fs.xattr_store
                .get(id, b"user.key")
                .await
                .unwrap()
                .is_empty()
        );
    }
}
