use super::*;
use crate::fs::key_codec::{KeyCodec, KeyPrefix, ParsedKey};
use crate::fs::name_mode::NameMode;
use crate::fs::test_util::{test_auth, test_creds};
use crate::fs::types::{FileType, SetAttributes, SetMode, SetUid};
use futures::StreamExt;
use std::sync::Arc;

fn name(s: &[u8]) -> XattrName {
    XattrName::new(s).unwrap()
}
fn value(s: &[u8]) -> XattrValue {
    XattrValue::new(Bytes::copy_from_slice(s)).unwrap()
}
async fn file(fs: &ZeroFS, s: &[u8]) -> InodeId {
    fs.create(&test_creds(), 0, s, &SetAttributes::default())
        .await
        .unwrap()
        .0
}
async fn rows(fs: &ZeroFS, id: InodeId) -> usize {
    let codec = KeyCodec::new();
    let (start, end) = codec.prefix_range(KeyPrefix::Xattr);
    let mut scan = fs.db.scan(start..end).await.unwrap();
    let mut count = 0;
    while let Some(row) = scan.next().await {
        match codec.parse_key(&row.unwrap().0) {
            ParsedKey::Xattr { inode_id, .. } if inode_id == id => count += 1,
            ParsedKey::Xattr { .. } => {}
            _ => panic!("invalid xattr key"),
        }
    }
    count
}

#[tokio::test]
async fn create_replace_remove_list_and_buffer_errors() {
    let fs = ZeroFS::new_in_memory().await.unwrap();
    let id = file(&fs, b"file").await;
    let creds = test_creds();
    let a = name(b"user.A");
    assert_eq!(
        fs.get_xattr(&creds, id, &a, None).await,
        Err(FsError::NoData)
    );
    assert_eq!(
        fs.set_xattr(&creds, id, &a, value(b"x"), XattrSetMode::Replace)
            .await,
        Err(FsError::NoData)
    );
    fs.set_xattr(&creds, id, &a, value(b"first"), XattrSetMode::Create)
        .await
        .unwrap();
    assert_eq!(
        fs.set_xattr(&creds, id, &a, value(b"bad"), XattrSetMode::Create)
            .await,
        Err(FsError::Exists)
    );
    fs.set_xattr(&creds, id, &a, value(b"second"), XattrSetMode::Replace)
        .await
        .unwrap();
    assert_eq!(
        fs.get_xattr(&creds, id, &a, Some(1)).await,
        Err(FsError::Range)
    );
    assert_eq!(
        fs.get_xattr(&creds, id, &a, Some(0)).await.unwrap(),
        b"second".as_slice()
    );
    for n in [b"user.z".as_slice(), b"user.\xff", b"user.a"] {
        fs.set_xattr(&creds, id, &name(n), value(b""), XattrSetMode::Either)
            .await
            .unwrap();
    }
    assert_eq!(
        fs.list_xattrs(&creds, id, None).await.unwrap(),
        b"user.A\0user.a\0user.z\0user.\xff\0".as_slice()
    );
    assert_eq!(
        fs.list_xattrs(&creds, id, Some(1)).await,
        Err(FsError::Range)
    );
    fs.remove_xattr(&creds, id, &a).await.unwrap();
    assert_eq!(fs.remove_xattr(&creds, id, &a).await, Err(FsError::NoData));
    assert_eq!(rows(&fs, id).await, 3);
}

#[test]
fn names_values_and_flags_are_bounded() {
    for n in [b"".as_slice(), b"user.\0a", b"user."] {
        assert!(matches!(XattrName::new(n), Err(FsError::InvalidArgument)));
    }
    for n in [b"system.posix_acl_access".as_slice(), b"other.key", b"key"] {
        assert!(matches!(XattrName::new(n), Err(FsError::NotSupported)));
    }
    assert!(XattrName::new(&[b"user.".as_slice(), &[b'x'; 250]].concat()).is_ok());
    assert!(matches!(
        XattrName::new(&[b"user.".as_slice(), &[b'x'; 251]].concat()),
        Err(FsError::NameTooLong)
    ));
    assert!(matches!(
        XattrValue::new(Bytes::from(vec![0; XATTR_VALUE_MAX + 1])),
        Err(FsError::TooBig)
    ));
    assert!(matches!(
        XattrSetMode::from_flags(3),
        Err(FsError::InvalidArgument)
    ));
}

#[tokio::test]
async fn two_mib_roundtrip_and_ctime_changes() {
    let fs = ZeroFS::new_in_memory().await.unwrap();
    let id = file(&fs, b"large").await;
    let n = name(b"user.large");
    let before = fs.inode_store.get(id).await.unwrap();
    let payload = Bytes::from(
        (0..XATTR_VALUE_MAX)
            .map(|i| (i % 251) as u8)
            .collect::<Vec<_>>(),
    );
    fs.set_xattr(
        &test_creds(),
        id,
        &n,
        XattrValue::new(payload.clone()).unwrap(),
        XattrSetMode::Either,
    )
    .await
    .unwrap();
    assert_eq!(
        fs.get_xattr(&test_creds(), id, &n, None).await.unwrap(),
        payload
    );
    let after = fs.inode_store.get(id).await.unwrap();
    assert!((after.ctime(), after.ctime_nsec()) > (before.ctime(), before.ctime_nsec()));
    fs.remove_xattr(&test_creds(), id, &n).await.unwrap();
    let removed = fs.inode_store.get(id).await.unwrap();
    assert!((removed.ctime(), removed.ctime_nsec()) > (after.ctime(), after.ctime_nsec()));
    assert_eq!(removed.mtime(), before.mtime());
}

#[tokio::test]
async fn namespace_permissions_and_list_visibility() {
    let fs = ZeroFS::new_in_memory().await.unwrap();
    let owner = Credentials {
        uid: 1000,
        gid: 1000,
        ..test_creds()
    };
    let other = Credentials {
        uid: 1001,
        gid: 1001,
        ..owner
    };
    let root = Credentials {
        uid: 0,
        gid: 0,
        ..owner
    };
    let (id, _) = fs
        .create(
            &root,
            0,
            b"private",
            &SetAttributes {
                uid: SetUid::Set(1000),
                mode: SetMode::Set(0o600),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let user = name(b"user.key");
    let trusted = name(b"trusted.key");
    let security = name(b"security.key");
    fs.set_xattr(&owner, id, &user, value(b"owner"), XattrSetMode::Either)
        .await
        .unwrap();
    assert_eq!(
        fs.get_xattr(&other, id, &user, None).await,
        Err(FsError::PermissionDenied)
    );
    assert_eq!(
        fs.set_xattr(&other, id, &user, value(b"bad"), XattrSetMode::Either)
            .await,
        Err(FsError::PermissionDenied)
    );
    assert_eq!(
        fs.remove_xattr(&other, id, &user).await,
        Err(FsError::PermissionDenied)
    );
    assert_eq!(
        fs.set_xattr(&owner, id, &trusted, value(b"bad"), XattrSetMode::Either)
            .await,
        Err(FsError::OperationNotPermitted)
    );
    assert_eq!(
        fs.set_xattr(&owner, id, &security, value(b"bad"), XattrSetMode::Either)
            .await,
        Err(FsError::OperationNotPermitted)
    );
    for n in [&trusted, &security] {
        fs.set_xattr(&root, id, n, value(b"root"), XattrSetMode::Either)
            .await
            .unwrap();
    }
    assert_eq!(
        fs.get_xattr(&owner, id, &trusted, None).await,
        Err(FsError::OperationNotPermitted)
    );
    assert_eq!(
        fs.get_xattr(&other, id, &security, None).await.unwrap(),
        b"root".as_slice()
    );
    assert_eq!(
        fs.remove_xattr(&owner, id, &security).await,
        Err(FsError::OperationNotPermitted)
    );
    assert_eq!(
        fs.list_xattrs(&other, id, None).await.unwrap(),
        b"security.key\0".as_slice()
    );
    assert_eq!(
        fs.list_xattrs(&root, id, None).await.unwrap(),
        b"security.key\0trusted.key\0user.key\0".as_slice()
    );
    fs.remove_xattr(&root, id, &trusted).await.unwrap();
}

#[tokio::test]
async fn symlinks_and_fifos_reject_user_attributes() {
    let fs = ZeroFS::new_in_memory().await.unwrap();
    let creds = test_creds();
    let link = fs
        .symlink(&creds, 0, b"link", b"missing", &SetAttributes::default())
        .await
        .unwrap()
        .0;
    let fifo = fs
        .mknod(
            &creds,
            0,
            b"fifo",
            FileType::Fifo,
            &SetAttributes::default(),
            None,
        )
        .await
        .unwrap()
        .0;
    for id in [link, fifo] {
        assert_eq!(
            fs.set_xattr(
                &creds,
                id,
                &name(b"user.key"),
                value(b"x"),
                XattrSetMode::Either
            )
            .await,
            Err(FsError::OperationNotPermitted)
        );
        assert_eq!(
            fs.get_xattr(&creds, id, &name(b"user.key"), None).await,
            Err(FsError::OperationNotPermitted)
        );
        fs.set_xattr(
            &Credentials { uid: 0, ..creds },
            id,
            &name(b"security.key"),
            value(b"x"),
            XattrSetMode::Either,
        )
        .await
        .unwrap();
    }
    fs.set_xattr(
        &creds,
        0,
        &name(b"user.directory"),
        value(b"dir"),
        XattrSetMode::Either,
    )
    .await
    .unwrap();
}

#[tokio::test]
async fn hardlinks_share_attributes_until_last_unlink() {
    let fs = ZeroFS::new_in_memory().await.unwrap();
    let id = file(&fs, b"first").await;
    fs.set_xattr(
        &test_creds(),
        id,
        &name(b"user.key"),
        value(b"shared"),
        XattrSetMode::Either,
    )
    .await
    .unwrap();
    fs.link(&test_auth(), id, 0, b"second").await.unwrap();
    let second = fs.lookup(&test_creds(), 0, b"second").await.unwrap();
    assert_eq!(
        fs.get_xattr(&test_creds(), second, &name(b"user.key"), None)
            .await
            .unwrap(),
        b"shared".as_slice()
    );
    fs.remove(&test_auth(), 0, b"first").await.unwrap();
    assert_eq!(rows(&fs, id).await, 1);
    fs.remove(&test_auth(), 0, b"second").await.unwrap();
    assert_eq!(rows(&fs, id).await, 0);
}

#[tokio::test]
async fn rename_overwrite_deletes_target_attributes() {
    let fs = ZeroFS::new_in_memory().await.unwrap();
    let src = file(&fs, b"src").await;
    let dst = file(&fs, b"dst").await;
    for id in [src, dst] {
        fs.set_xattr(
            &test_creds(),
            id,
            &name(b"user.key"),
            value(b"x"),
            XattrSetMode::Either,
        )
        .await
        .unwrap();
    }
    fs.rename(&test_auth(), 0, b"src", 0, b"dst").await.unwrap();
    assert_eq!(rows(&fs, dst).await, 0);
    assert_eq!(rows(&fs, src).await, 1);
}

#[tokio::test]
async fn open_unlinked_attributes_survive_until_reclaim() {
    let fs = ZeroFS::new_in_memory().await.unwrap();
    let id = file(&fs, b"open").await;
    fs.open_handle_inc(id);
    fs.set_xattr(
        &test_creds(),
        id,
        &name(b"user.key"),
        value(b"before"),
        XattrSetMode::Either,
    )
    .await
    .unwrap();
    fs.remove(&test_auth(), 0, b"open").await.unwrap();
    fs.set_xattr(
        &test_creds(),
        id,
        &name(b"user.key"),
        value(b"after"),
        XattrSetMode::Replace,
    )
    .await
    .unwrap();
    assert_eq!(
        fs.get_xattr(&test_creds(), id, &name(b"user.key"), None)
            .await
            .unwrap(),
        b"after".as_slice()
    );
    assert_eq!(rows(&fs, id).await, 1);
    fs.handle_closed(id).await;
    assert_eq!(rows(&fs, id).await, 0);
}

#[tokio::test]
async fn insensitive_filenames_leave_xattr_names_byte_exact() {
    let fs = ZeroFS::new_in_memory_with_name_mode(NameMode::Insensitive)
        .await
        .unwrap();
    let id = file(&fs, b"File").await;
    for n in [b"user.A", b"user.a"] {
        fs.set_xattr(&test_creds(), id, &name(n), value(n), XattrSetMode::Either)
            .await
            .unwrap();
    }
    let id2 = fs.lookup(&test_creds(), 0, b"FILE").await.unwrap();
    assert_eq!(id, id2);
    assert_eq!(
        fs.list_xattrs(&test_creds(), id2, None).await.unwrap(),
        b"user.A\0user.a\0".as_slice()
    );
    assert_eq!(
        fs.get_xattr(&test_creds(), id, &name(b"user.A"), None)
            .await
            .unwrap(),
        b"user.A".as_slice()
    );
}

#[tokio::test]
async fn xattrs_survive_database_reopen() {
    use crate::block_transformer::ZeroFsBlockTransformer;
    use crate::config::CompressionConfig;
    use crate::db::SlateDbHandle;
    use slatedb::BlockTransformer;
    use slatedb::DbBuilder;
    use slatedb::object_store::path::Path;

    let test_key = [0u8; 32];
    let object_store: Arc<dyn slatedb::object_store::ObjectStore> =
        Arc::new(slatedb::object_store::memory::InMemory::new());
    let block_transformer: Arc<dyn BlockTransformer> =
        ZeroFsBlockTransformer::new_arc(&test_key, CompressionConfig::default());

    // Path must match `new_in_memory_read_only` so the reader finds the db.
    let slatedb = Arc::new(
        DbBuilder::new(Path::from("test_slatedb"), object_store.clone())
            .with_block_transformer(block_transformer)
            .with_filter_policies(crate::fs::filter_policy::filter_policies())
            .with_segment_extractor(Arc::new(crate::segment_extractor::ZeroFsSegmentExtractor))
            .build()
            .await
            .unwrap(),
    );
    let fs = ZeroFS::new_with_slatedb(
        SlateDbHandle::ReadWrite(slatedb),
        u64::MAX,
        None,
        false,
        object_store.clone(),
        crate::frame_codec::FrameCodec::new(
            &test_key,
            crate::segment::SEGMENT_INFO,
            CompressionConfig::default(),
        ),
    )
    .await
    .unwrap();

    let id = file(&fs, b"persisted").await;
    fs.set_xattr(
        &test_creds(),
        id,
        &name(b"user.bytes"),
        value(b"\0\xffpersisted"),
        XattrSetMode::Either,
    )
    .await
    .unwrap();
    fs.flush_coordinator.flush().await.unwrap();
    drop(fs);
    let reopened = ZeroFS::new_in_memory_read_only(object_store).await.unwrap();
    assert_eq!(
        reopened
            .get_xattr(&test_creds(), id, &name(b"user.bytes"), None)
            .await
            .unwrap(),
        b"\0\xffpersisted".as_slice()
    );
    assert_eq!(
        reopened.list_xattrs(&test_creds(), id, None).await.unwrap(),
        b"user.bytes\0".as_slice()
    );
}
