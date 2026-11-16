use super::*;
use ninep_client::{ClientError, NOFID, NinePClient};

async fn client_setup() -> (Arc<NinePClient>, CancellationToken, tempfile::TempDir, u32) {
    let fs = Arc::new(ZeroFS::new_in_memory().await.unwrap());
    let dir = tempfile::tempdir().unwrap();
    let sock = dir.path().join("c.sock");
    let shutdown = start_server(fs, sock.clone());
    let mut client = None;
    for _ in 0..100 {
        if let Ok(c) = NinePClient::connect_unix(&sock, 16 * 1024).await {
            client = Some(c);
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let client = client.expect("server did not start");
    let root = client.alloc_fid();
    client.attach(root, NOFID, "", "", 1000).await.unwrap();
    let fid = client.alloc_fid();
    client.walk(root, fid, &[]).await.unwrap();
    client.lcreate(fid, b"file", 2, 0o600, 1000).await.unwrap();
    // Xattrcreate uses an unopened clone of the inode fid.
    let unopened = client.alloc_fid();
    client.walk(root, unopened, &[b"file"]).await.unwrap();
    (client, shutdown, dir, unopened)
}

fn errno<T: std::fmt::Debug>(result: Result<T, ClientError>, expected: u32) {
    match result {
        Err(ClientError::Errno(e)) => assert_eq!(e, expected),
        other => panic!("expected errno {expected}, got {other:?}"),
    }
}

#[tokio::test]
async fn xattr_client_two_mib_roundtrip_and_set_modes() {
    let (client, shutdown, _dir, fid) = client_setup().await;
    let baseline = client.outstanding_fids();
    let payload = (0..2_097_152).map(|i| (i % 251) as u8).collect::<Vec<_>>();
    client
        .set_xattr(fid, b"user.large", &payload, 1)
        .await
        .unwrap();
    assert_eq!(
        client.get_xattr(fid, b"user.large").await.unwrap().as_ref(),
        payload
    );
    assert_eq!(
        client.list_xattrs(fid).await.unwrap().as_ref(),
        b"user.large\0"
    );
    errno(client.set_xattr(fid, b"user.large", b"exists", 1).await, 17);
    client
        .set_xattr(fid, b"user.large", b"replaced", 2)
        .await
        .unwrap();
    assert_eq!(
        client.get_xattr(fid, b"user.large").await.unwrap().as_ref(),
        b"replaced"
    );
    client.remove_xattr(fid, b"user.large").await.unwrap();
    errno(client.get_xattr(fid, b"user.large").await, 61);
    errno(client.remove_xattr(fid, b"user.large").await, 61);
    errno(
        client
            .set_xattr(fid, b"user.large", &vec![0; 2_097_153], 0)
            .await,
        7,
    );
    assert!(client.list_xattrs(fid).await.unwrap().is_empty());
    assert_eq!(client.outstanding_fids(), baseline);
    client.fsync(fid, 0).await.unwrap();
    shutdown.cancel();
}

#[tokio::test]
async fn xattr_client_empty_values_and_namespace_errors() {
    let (client, shutdown, _dir, fid) = client_setup().await;
    client.set_xattr(fid, b"user.empty", b"", 1).await.unwrap();
    assert!(
        client
            .get_xattr(fid, b"user.empty")
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        client.list_xattrs(fid).await.unwrap().as_ref(),
        b"user.empty\0"
    );
    client.set_xattr(fid, b"user.empty", b"", 2).await.unwrap();
    assert!(
        client
            .get_xattr(fid, b"user.empty")
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        client.list_xattrs(fid).await.unwrap().as_ref(),
        b"user.empty\0"
    );
    client.remove_xattr(fid, b"user.empty").await.unwrap();
    errno(client.get_xattr(fid, b"user.empty").await, 61);
    errno(client.set_xattr(fid, b"system.acl", b"x", 0).await, 95);
    errno(client.set_xattr(fid, b"trusted.secret", b"x", 0).await, 1);
    errno(client.set_xattr(fid, b"user.bad", b"x", 3).await, 22);
    shutdown.cancel();
}
