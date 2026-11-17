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
async fn create_replace_remove_list_and_errors() {
    let fs = ZeroFS::new_in_memory().await.unwrap();
    let id = file(&fs, b"file").await;
    let creds = test_creds();
    let a = name(b"user.A");
    assert_eq!(fs.get_xattr(&creds, id, &a).await, Err(FsError::NoData));
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
        fs.get_xattr(&creds, id, &a).await.unwrap(),
        b"second".as_slice()
    );
    for n in [b"user.z".as_slice(), b"user.\xff", b"user.a"] {
        fs.set_xattr(&creds, id, &name(n), value(b""), XattrSetMode::Either)
            .await
            .unwrap();
    }
    assert_eq!(
        fs.list_xattrs(&creds, id).await.unwrap(),
        b"user.A\0user.a\0user.z\0user.\xff\0".as_slice()
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
    assert_eq!(fs.get_xattr(&test_creds(), id, &n).await.unwrap(), payload);
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
        fs.get_xattr(&other, id, &user).await,
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
        fs.get_xattr(&owner, id, &trusted).await,
        Err(FsError::OperationNotPermitted)
    );
    assert_eq!(
        fs.get_xattr(&other, id, &security).await.unwrap(),
        b"root".as_slice()
    );
    assert_eq!(
        fs.remove_xattr(&owner, id, &security).await,
        Err(FsError::OperationNotPermitted)
    );
    assert_eq!(
        fs.list_xattrs(&other, id).await.unwrap(),
        b"security.key\0".as_slice()
    );
    assert_eq!(
        fs.list_xattrs(&root, id).await.unwrap(),
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
            fs.get_xattr(&creds, id, &name(b"user.key")).await,
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
    let directory = fs
        .mkdir(&creds, 0, b"directory", &SetAttributes::default())
        .await
        .unwrap()
        .0;
    fs.set_xattr(
        &creds,
        directory,
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
        fs.get_xattr(&test_creds(), second, &name(b"user.key"))
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
        fs.get_xattr(&test_creds(), id, &name(b"user.key"))
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
        fs.list_xattrs(&test_creds(), id2).await.unwrap(),
        b"user.A\0user.a\0".as_slice()
    );
    assert_eq!(
        fs.get_xattr(&test_creds(), id, &name(b"user.A"))
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

    // The path must match `new_in_memory_read_only` so the reader finds the db.
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
            .get_xattr(&test_creds(), id, &name(b"user.bytes"))
            .await
            .unwrap(),
        b"\0\xffpersisted".as_slice()
    );
    assert_eq!(
        reopened.list_xattrs(&test_creds(), id).await.unwrap(),
        b"user.bytes\0".as_slice()
    );
}

#[tokio::test]
async fn cancelled_queued_set_then_unlink_leaves_no_attribute_rows() {
    let fs = Arc::new(ZeroFS::new_in_memory().await.unwrap());
    let id = file(&fs, b"cancelled").await;
    let block_apply = fs.db.flush_barrier().write_owned().await;
    let apply_reached = fs.write_coordinator.probe_next_apply();
    let setting_fs = Arc::clone(&fs);
    let setting = tokio::spawn(async move {
        setting_fs
            .set_xattr(
                &test_creds(),
                id,
                &name(b"user.pending"),
                value(b"queued"),
                XattrSetMode::Create,
            )
            .await
    });
    apply_reached.await.unwrap();
    setting.abort();
    assert!(setting.await.unwrap_err().is_cancelled());
    assert_eq!(
        fs.xattr_store.get(id, b"user.pending").await,
        Err(FsError::NoData)
    );

    let removing_fs = Arc::clone(&fs);
    let removing =
        tokio::spawn(async move { removing_fs.remove(&test_auth(), 0, b"cancelled").await });
    assert!(
        tokio::time::timeout(
            std::time::Duration::from_millis(50),
            fs.lock_manager.acquire(id)
        )
        .await
        .is_err(),
        "cancelled setter released its inode lock before apply"
    );
    assert!(!removing.is_finished());
    assert!(!matches!(fs.inode_store.pending_inode(id), Some(None)));
    drop(block_apply);
    removing.await.unwrap().unwrap();
    assert!(matches!(
        fs.inode_store.get(id).await,
        Err(FsError::NotFound)
    ));
    assert_eq!(rows(&fs, id).await, 0);
}

#[tokio::test]
async fn queued_unlink_then_set_rejects_gone_inode_and_leaves_no_attribute_rows() {
    let fs = Arc::new(ZeroFS::new_in_memory().await.unwrap());
    let id = file(&fs, b"deleted").await;
    let block_apply = fs.db.flush_barrier().write_owned().await;
    let apply_reached = fs.write_coordinator.probe_next_apply();
    let removing_fs = Arc::clone(&fs);
    let removing =
        tokio::spawn(async move { removing_fs.remove(&test_auth(), 0, b"deleted").await });
    apply_reached.await.unwrap();
    assert!(matches!(fs.inode_store.pending_inode(id), Some(None)));
    let setting_fs = Arc::clone(&fs);
    let mut setting = tokio::spawn(async move {
        setting_fs
            .set_xattr(
                &test_creds(),
                id,
                &name(b"user.late"),
                value(b"orphan"),
                XattrSetMode::Either,
            )
            .await
    });
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(50), &mut setting)
            .await
            .is_err(),
        "setter did not wait for the queued unlink's inode lock"
    );
    drop(block_apply);
    removing.await.unwrap().unwrap();
    assert_eq!(setting.await.unwrap(), Err(FsError::NotFound));
    assert!(matches!(
        fs.inode_store.get(id).await,
        Err(FsError::NotFound)
    ));
    assert_eq!(rows(&fs, id).await, 0);
}

#[tokio::test]
async fn cancelled_queued_remove_holds_lock_until_next_create_can_see_apply() {
    let fs = Arc::new(ZeroFS::new_in_memory().await.unwrap());
    let id = file(&fs, b"replace").await;
    fs.set_xattr(
        &test_creds(),
        id,
        &name(b"user.key"),
        value(b"old"),
        XattrSetMode::Create,
    )
    .await
    .unwrap();
    let block_apply = fs.db.flush_barrier().write_owned().await;
    let apply_reached = fs.write_coordinator.probe_next_apply();
    let removing_fs = Arc::clone(&fs);
    let removing = tokio::spawn(async move {
        removing_fs
            .remove_xattr(&test_creds(), id, &name(b"user.key"))
            .await
    });
    apply_reached.await.unwrap();
    removing.abort();
    assert!(removing.await.unwrap_err().is_cancelled());
    assert!(
        tokio::time::timeout(
            std::time::Duration::from_millis(50),
            fs.lock_manager.acquire(id)
        )
        .await
        .is_err()
    );
    let setting_fs = Arc::clone(&fs);
    let setting = tokio::spawn(async move {
        setting_fs
            .set_xattr(
                &test_creds(),
                id,
                &name(b"user.key"),
                value(b"new"),
                XattrSetMode::Create,
            )
            .await
    });
    drop(block_apply);
    setting.await.unwrap().unwrap();
    assert_eq!(
        fs.get_xattr(&test_creds(), id, &name(b"user.key"))
            .await
            .unwrap(),
        b"new".as_slice()
    );
}

#[tokio::test]
async fn sticky_directory_user_xattrs_require_owner_or_root() {
    let fs = ZeroFS::new_in_memory().await.unwrap();
    let root = Credentials {
        uid: 0,
        gid: 0,
        ..test_creds()
    };
    let owner = Credentials {
        uid: 1000,
        gid: 1000,
        ..test_creds()
    };
    let other = Credentials {
        uid: 1001,
        gid: 1001,
        ..test_creds()
    };
    let id = fs
        .mkdir(
            &root,
            0,
            b"sticky",
            &SetAttributes {
                uid: SetUid::Set(owner.uid),
                mode: SetMode::Set(0o1777),
                ..Default::default()
            },
        )
        .await
        .unwrap()
        .0;
    let key = name(b"user.key");
    for caller in [&owner, &root] {
        fs.set_xattr(caller, id, &key, value(b"allowed"), XattrSetMode::Either)
            .await
            .unwrap();
        assert_eq!(
            fs.get_xattr(&other, id, &key).await.unwrap(),
            b"allowed".as_slice()
        );
        assert_eq!(
            fs.set_xattr(&other, id, &key, value(b"denied"), XattrSetMode::Replace)
                .await,
            Err(FsError::OperationNotPermitted)
        );
        assert_eq!(
            fs.remove_xattr(&other, id, &key).await,
            Err(FsError::OperationNotPermitted)
        );
        fs.remove_xattr(caller, id, &key).await.unwrap();
    }
    assert_eq!(
        fs.set_xattr(&other, id, &key, value(b"denied"), XattrSetMode::Create)
            .await,
        Err(FsError::OperationNotPermitted)
    );
}

#[tokio::test]
async fn xattr_count_cap_allows_replace_and_delete_but_rejects_growth() {
    let fs = ZeroFS::new_in_memory().await.unwrap();
    let id = file(&fs, b"capped").await;
    let mut txn = fs.db.new_transaction().unwrap();
    for i in 0..4096 {
        fs.xattr_store.put(
            &mut txn,
            id,
            format!("user.{i:04}").as_bytes(),
            Bytes::new(),
        );
    }
    fs.write_coordinator.commit(txn).await.unwrap();
    let creds = test_creds();
    assert_eq!(fs.xattr_store.list(id).await.unwrap().len(), 4096);
    fs.set_xattr(
        &creds,
        id,
        &name(b"user.0000"),
        value(b"replace"),
        XattrSetMode::Replace,
    )
    .await
    .unwrap();
    assert_eq!(
        fs.set_xattr(
            &creds,
            id,
            &name(b"user.overflow"),
            value(b""),
            XattrSetMode::Create
        )
        .await,
        Err(FsError::NoSpace)
    );
    fs.remove_xattr(&creds, id, &name(b"user.0000"))
        .await
        .unwrap();
    fs.set_xattr(
        &creds,
        id,
        &name(b"user.overflow"),
        value(b""),
        XattrSetMode::Create,
    )
    .await
    .unwrap();
    fs.remove(&test_auth(), 0, b"capped").await.unwrap();
    assert_eq!(rows(&fs, id).await, 0);
}

#[tokio::test]
async fn xattr_listing_refuses_rows_beyond_the_cap() {
    let fs = ZeroFS::new_in_memory().await.unwrap();
    let mut txn = fs.db.new_transaction().unwrap();
    for i in 0..4097 {
        fs.xattr_store
            .put(&mut txn, 0, format!("user.{i:04}").as_bytes(), Bytes::new());
    }
    fs.write_coordinator.commit(txn).await.unwrap();
    assert!(matches!(
        fs.xattr_store.list(0).await,
        Err(FsError::NoSpace)
    ));
}

#[tokio::test]
async fn over_cap_attributes_do_not_block_unlink_or_open_inode_reclaim() {
    let fs = ZeroFS::new_in_memory().await.unwrap();
    let neighbor = file(&fs, b"neighbor").await;
    fs.set_xattr(
        &test_creds(),
        neighbor,
        &name(b"user.keep"),
        value(b"neighbor"),
        XattrSetMode::Create,
    )
    .await
    .unwrap();
    for keep_open in [false, true] {
        let id = file(&fs, b"over-cap").await;
        // Bypass write admission to model rows from an earlier development
        // build. Such an inode must still be removable in one transaction.
        let mut txn = fs.db.new_transaction().unwrap();
        for i in 0..=crate::fs::store::xattr::XATTR_COUNT_MAX {
            fs.xattr_store.put(
                &mut txn,
                id,
                format!("user.{i:04}").as_bytes(),
                Bytes::new(),
            );
        }
        fs.write_coordinator.commit(txn).await.unwrap();
        assert!(matches!(
            fs.xattr_store.list(id).await,
            Err(FsError::NoSpace)
        ));
        if keep_open {
            fs.open_handle_inc(id);
        }
        fs.remove(&test_auth(), 0, b"over-cap").await.unwrap();
        if keep_open {
            assert_eq!(
                rows(&fs, id).await,
                crate::fs::store::xattr::XATTR_COUNT_MAX + 1
            );
            fs.handle_closed(id).await;
        }
        assert_eq!(rows(&fs, id).await, 0);
        assert!(matches!(
            fs.inode_store.get(id).await,
            Err(FsError::NotFound)
        ));
        assert_eq!(
            fs.get_xattr(&test_creds(), neighbor, &name(b"user.keep"))
                .await
                .unwrap(),
            b"neighbor".as_slice()
        );
    }
}

#[tokio::test]
async fn rename_overwrite_deletes_xattrs_beyond_cap() {
    let fs = ZeroFS::new_in_memory().await.unwrap();
    let src = file(&fs, b"src").await;
    let dst = file(&fs, b"dst").await;
    fs.set_xattr(
        &test_creds(),
        src,
        &name(b"user.keep"),
        value(b"source"),
        XattrSetMode::Create,
    )
    .await
    .unwrap();
    let count = crate::fs::store::xattr::XATTR_COUNT_MAX + 1;
    let mut txn = fs.db.new_transaction().unwrap();
    for i in 0..count {
        fs.xattr_store.put(
            &mut txn,
            dst,
            format!("user.{i:04}").as_bytes(),
            Bytes::new(),
        );
    }
    fs.write_coordinator.commit(txn).await.unwrap();
    assert_eq!(rows(&fs, dst).await, count);
    assert!(matches!(
        fs.xattr_store.list(dst).await,
        Err(FsError::NoSpace)
    ));

    fs.rename(&test_auth(), 0, b"src", 0, b"dst").await.unwrap();

    assert_eq!(rows(&fs, dst).await, 0);
    assert!(matches!(
        fs.inode_store.get(dst).await,
        Err(FsError::NotFound)
    ));
    assert_eq!(fs.lookup(&test_creds(), 0, b"dst").await.unwrap(), src);
    assert_eq!(rows(&fs, src).await, 1);
    assert_eq!(
        fs.get_xattr(&test_creds(), src, &name(b"user.keep"))
            .await
            .unwrap(),
        b"source".as_slice()
    );
}

#[tokio::test]
async fn xattr_set_is_submitted_before_detaching_and_shutdown_barrier() {
    let fs = ZeroFS::new_in_memory().await.unwrap();
    let id = file(&fs, b"shutdown").await;
    let creds = test_creds();
    let key = name(b"user.shutdown");
    let mut setting =
        Box::pin(fs.set_xattr(&creds, id, &key, value(b"queued"), XattrSetMode::Create));
    assert!(futures::poll!(&mut setting).is_pending());
    assert!(
        fs.inode_store.pending_inode(id).is_some(),
        "the setter detached before submitting its transaction"
    );
    fs.write_coordinator.barrier().await.unwrap();
    assert_eq!(rows(&fs, id).await, 1);
    assert!(fs.inode_store.get(id).await.is_ok());
    fs.db.close().await.unwrap();
    let result = setting.await;
    assert!(result.is_ok(), "queued set returned {result:?}");
}

async fn nonregular_inode(fs: &ZeroFS, kind: FileType, filename: &[u8]) -> InodeId {
    let root = Credentials {
        uid: 0,
        gid: 0,
        ..test_creds()
    };
    let attrs = SetAttributes::default();
    match kind {
        FileType::Directory => fs.mkdir(&root, 0, filename, &attrs).await.unwrap().0,
        FileType::Symlink => {
            fs.symlink(&root, 0, filename, b"missing", &attrs)
                .await
                .unwrap()
                .0
        }
        _ => {
            fs.mknod(&root, 0, filename, kind, &attrs, Some((1, 3)))
                .await
                .unwrap()
                .0
        }
    }
}

#[tokio::test]
async fn nonregular_xattrs_are_deleted_on_last_unlink_rename_over_and_reclaim() {
    for kind in [
        FileType::Directory,
        FileType::Symlink,
        FileType::Fifo,
        FileType::Socket,
        FileType::CharDevice,
        FileType::BlockDevice,
    ] {
        let fs = ZeroFS::new_in_memory().await.unwrap();
        let root = Credentials {
            uid: 0,
            gid: 0,
            ..test_creds()
        };
        let auth = (&root).into();
        let key = name(b"trusted.cleanup");
        let unlinked = nonregular_inode(&fs, kind, b"unlinked").await;
        fs.set_xattr(&root, unlinked, &key, value(b"last"), XattrSetMode::Create)
            .await
            .unwrap();
        fs.remove(&auth, 0, b"unlinked").await.unwrap();
        assert_eq!(rows(&fs, unlinked).await, 0, "unlink {kind:?}");

        let src = nonregular_inode(&fs, kind, b"src").await;
        let dst = nonregular_inode(&fs, kind, b"dst").await;
        for id in [src, dst] {
            fs.set_xattr(&root, id, &key, value(b"rename"), XattrSetMode::Create)
                .await
                .unwrap();
        }
        fs.rename(&auth, 0, b"src", 0, b"dst").await.unwrap();
        assert_eq!(rows(&fs, dst).await, 0, "rename target {kind:?}");
        assert_eq!(rows(&fs, src).await, 1, "rename source {kind:?}");

        let opened = nonregular_inode(&fs, kind, b"opened").await;
        fs.open_handle_inc(opened);
        fs.set_xattr(&root, opened, &key, value(b"reclaim"), XattrSetMode::Create)
            .await
            .unwrap();
        fs.remove(&auth, 0, b"opened").await.unwrap();
        assert_eq!(rows(&fs, opened).await, 1, "before reclaim {kind:?}");
        fs.handle_closed(opened).await;
        assert_eq!(rows(&fs, opened).await, 0, "after reclaim {kind:?}");
        assert!(matches!(
            fs.inode_store.get(opened).await,
            Err(FsError::NotFound)
        ));
    }
}

#[tokio::test]
async fn xattr_set_returns_error_if_shutdown_has_closed_the_commit_queue() {
    let fs = ZeroFS::new_in_memory().await.unwrap();
    let id = file(&fs, b"closed-queue").await;
    let worker = fs.write_coordinator.take_worker_for_tests();
    worker.abort();
    assert!(worker.await.unwrap_err().is_cancelled());
    assert_eq!(
        fs.set_xattr(
            &test_creds(),
            id,
            &name(b"user.late"),
            value(b"error"),
            XattrSetMode::Create
        )
        .await,
        Err(FsError::IoError)
    );
    assert_eq!(rows(&fs, id).await, 0);
    assert!(fs.inode_store.get(id).await.is_ok());
}
