use super::*;

fn test_creds() -> Credentials {
    Credentials::from_auth_context(&AuthContext {
        uid: 1000,
        gid: 1000,
        ..AuthContext::default()
    })
}

async fn bind(handler: &NinePHandler, fid: u32, inode_id: InodeId, path: &[&[u8]]) {
    let inode = handler.filesystem.visible_inode(inode_id).await.unwrap();
    handler.session_state().fids.insert(
        fid,
        FidSlot::unopened(Fid {
            path: path
                .iter()
                .map(|name| Bytes::copy_from_slice(name))
                .collect(),
            inode_id,
            root: 0,
            qid: inode_to_qid(&inode, inode_id),
            opened: false,
            mode: None,
            creds: test_creds(),
        }),
    );
}

async fn rename(
    handler: &NinePHandler,
    dfid: u32,
    name: &[u8],
    op_id: crate::dedup::OpId,
) -> P9Result<Message> {
    handler
        .rename(
            Trename {
                fid: 2,
                dfid,
                name: P9String::new(name.to_vec()),
            },
            op_id,
        )
        .await
}

#[tokio::test]
async fn trename_updates_the_fid_for_repeated_file_and_directory_moves() {
    for directory in [false, true] {
        let fs = Arc::new(ZeroFS::new_in_memory().await.unwrap());
        let creds = test_creds();
        let (source, _) = if directory {
            fs.mkdir(&creds, 0, b"source", &SetAttributes::default())
                .await
        } else {
            fs.create(&creds, 0, b"source", &SetAttributes::default())
                .await
        }
        .unwrap();
        let (destination, _) = fs
            .mkdir(&creds, 0, b"destination", &SetAttributes::default())
            .await
            .unwrap();
        let handler = NinePHandler::new(Arc::clone(&fs), Arc::new(FileLockManager::new()));
        bind(&handler, 1, 0, &[]).await;
        bind(&handler, 2, source, &[b"source"]).await;
        bind(&handler, 3, destination, &[b"destination"]).await;

        assert!(matches!(
            rename(&handler, 3, b"first", [0; 16]).await,
            Ok(Message::Rrename(_))
        ));
        assert_eq!(
            handler.get_fid(2).unwrap().path,
            vec![
                Bytes::from_static(b"destination"),
                Bytes::from_static(b"first")
            ]
        );
        assert!(matches!(
            rename(&handler, 1, b"second", [0; 16]).await,
            Ok(Message::Rrename(_))
        ));
        assert_eq!(
            handler.get_fid(2).unwrap().path,
            vec![Bytes::from_static(b"second")]
        );
        assert_eq!(fs.lookup(&creds, 0, b"second").await, Ok(source));
        assert_eq!(
            fs.lookup(&creds, 0, b"source").await,
            Err(FsError::NotFound)
        );
        assert_eq!(
            fs.lookup(&creds, destination, b"first").await,
            Err(FsError::NotFound)
        );
    }
}

#[tokio::test]
async fn trename_hardlink_noop_keeps_the_original_entry_bound() {
    for cross_directory in [false, true] {
        let fs = Arc::new(ZeroFS::new_in_memory().await.unwrap());
        let creds = test_creds();
        let (source, _) = fs
            .create(&creds, 0, b"source", &SetAttributes::default())
            .await
            .unwrap();
        let alias_parent = if cross_directory {
            fs.mkdir(&creds, 0, b"other", &SetAttributes::default())
                .await
                .unwrap()
                .0
        } else {
            0
        };
        fs.link(&AuthContext::from(&creds), source, alias_parent, b"alias")
            .await
            .unwrap();
        let handler = NinePHandler::new(Arc::clone(&fs), Arc::new(FileLockManager::new()));
        bind(&handler, 1, 0, &[]).await;
        bind(&handler, 2, source, &[b"source"]).await;
        bind(
            &handler,
            3,
            alias_parent,
            if cross_directory { &[b"other"] } else { &[] },
        )
        .await;

        assert!(matches!(
            rename(&handler, 3, b"alias", [0x91; 16]).await,
            Ok(Message::Rrename(_))
        ));
        assert_eq!(
            handler.get_fid(2).unwrap().path,
            vec![Bytes::from_static(b"source")]
        );
        assert!(matches!(
            rename(&handler, 1, b"moved", [0; 16]).await,
            Ok(Message::Rrename(_))
        ));
        assert_eq!(
            fs.lookup(&creds, 0, b"source").await,
            Err(FsError::NotFound)
        );
        assert_eq!(fs.lookup(&creds, 0, b"moved").await, Ok(source));
        assert_eq!(fs.lookup(&creds, alias_parent, b"alias").await, Ok(source));
    }
}

#[tokio::test]
async fn trename_reconstructs_a_renamed_destination_directory_path() {
    let fs = Arc::new(ZeroFS::new_in_memory().await.unwrap());
    let creds = test_creds();
    let (source, _) = fs
        .create(&creds, 0, b"source", &SetAttributes::default())
        .await
        .unwrap();
    let (destination, _) = fs
        .mkdir(&creds, 0, b"old-directory", &SetAttributes::default())
        .await
        .unwrap();
    let handler = NinePHandler::new(Arc::clone(&fs), Arc::new(FileLockManager::new()));
    bind(&handler, 1, 0, &[]).await;
    bind(&handler, 2, source, &[b"source"]).await;
    bind(&handler, 3, destination, &[b"old-directory"]).await;
    fs.rename(
        &AuthContext::from(&creds),
        0,
        b"old-directory",
        0,
        b"current-directory",
    )
    .await
    .unwrap();

    assert!(matches!(
        rename(&handler, 3, b"first", [0; 16]).await,
        Ok(Message::Rrename(_))
    ));
    assert_eq!(
        handler.get_fid(2).unwrap().path,
        vec![
            Bytes::from_static(b"current-directory"),
            Bytes::from_static(b"first")
        ]
    );
    assert!(matches!(
        rename(&handler, 1, b"second", [0; 16]).await,
        Ok(Message::Rrename(_))
    ));
    assert_eq!(fs.lookup(&creds, 0, b"second").await, Ok(source));
    assert_eq!(
        fs.lookup(&creds, destination, b"first").await,
        Err(FsError::NotFound)
    );
}

#[tokio::test]
async fn failed_trename_keeps_the_fid_path_and_both_entries() {
    let fs = Arc::new(ZeroFS::new_in_memory().await.unwrap());
    let creds = test_creds();
    let (source, _) = fs
        .create(&creds, 0, b"source", &SetAttributes::default())
        .await
        .unwrap();
    let (occupied, _) = fs
        .mkdir(&creds, 0, b"occupied", &SetAttributes::default())
        .await
        .unwrap();
    let handler = NinePHandler::new(Arc::clone(&fs), Arc::new(FileLockManager::new()));
    bind(&handler, 1, 0, &[]).await;
    bind(&handler, 2, source, &[b"source"]).await;

    let error = rename(&handler, 1, b"occupied", [0; 16]).await.unwrap_err();
    assert_eq!(error.to_errno(), crate::linux_errno::EISDIR);
    assert_eq!(
        handler.get_fid(2).unwrap().path,
        vec![Bytes::from_static(b"source")]
    );
    assert_eq!(fs.lookup(&creds, 0, b"source").await, Ok(source));
    assert_eq!(fs.lookup(&creds, 0, b"occupied").await, Ok(occupied));

    let (not_directory, _) = fs
        .create(&creds, 0, b"not-directory", &SetAttributes::default())
        .await
        .unwrap();
    bind(&handler, 3, not_directory, &[b"not-directory"]).await;
    let error = rename(&handler, 3, b"child", [0; 16]).await.unwrap_err();
    assert_eq!(error.to_errno(), crate::linux_errno::ENOTDIR);
    assert_eq!(
        handler.get_fid(2).unwrap().path,
        vec![Bytes::from_static(b"source")]
    );
    assert_eq!(fs.lookup(&creds, 0, b"source").await, Ok(source));
    assert_eq!(
        fs.lookup(&creds, 0, b"not-directory").await,
        Ok(not_directory)
    );
}

#[tokio::test]
async fn trename_replay_does_not_rewind_a_later_fid_path() {
    let fs = Arc::new(ZeroFS::new_in_memory().await.unwrap());
    let creds = test_creds();
    let (source, _) = fs
        .create(&creds, 0, b"source", &SetAttributes::default())
        .await
        .unwrap();
    let handler = NinePHandler::new(Arc::clone(&fs), Arc::new(FileLockManager::new()));
    bind(&handler, 1, 0, &[]).await;
    bind(&handler, 2, source, &[b"source"]).await;

    assert!(matches!(
        rename(&handler, 1, b"first", [0x92; 16]).await,
        Ok(Message::Rrename(_))
    ));
    assert!(matches!(
        rename(&handler, 1, b"second", [0x93; 16]).await,
        Ok(Message::Rrename(_))
    ));
    assert!(matches!(
        rename(&handler, 1, b"first", [0x92; 16]).await,
        Ok(Message::Rrename(_))
    ));
    assert_eq!(
        handler.get_fid(2).unwrap().path,
        vec![Bytes::from_static(b"second")]
    );
    assert_eq!(fs.lookup(&creds, 0, b"second").await, Ok(source));
    assert_eq!(fs.lookup(&creds, 0, b"first").await, Err(FsError::NotFound));
}

#[tokio::test]
async fn case_only_trename_updates_the_fid_before_the_next_rename() {
    use crate::fs::name_mode::NameMode;

    for mode in [NameMode::Sensitive, NameMode::Insensitive] {
        let fs = Arc::new(ZeroFS::new_in_memory_with_name_mode(mode).await.unwrap());
        let creds = test_creds();
        let (source, _) = fs
            .create(&creds, 0, b"File", &SetAttributes::default())
            .await
            .unwrap();
        let handler = NinePHandler::new(Arc::clone(&fs), Arc::new(FileLockManager::new()));
        bind(&handler, 1, 0, &[]).await;
        bind(&handler, 2, source, &[b"File"]).await;

        assert!(matches!(
            rename(&handler, 1, b"file", [0; 16]).await,
            Ok(Message::Rrename(_))
        ));
        assert_eq!(
            handler.get_fid(2).unwrap().path,
            vec![Bytes::from_static(b"file")]
        );
        assert_eq!(fs.directory_store.get_exact(0, b"file").await, Ok(source));
        assert_eq!(
            fs.directory_store.get_exact(0, b"File").await,
            Err(FsError::NotFound)
        );
        assert!(matches!(
            rename(&handler, 1, b"next", [0; 16]).await,
            Ok(Message::Rrename(_))
        ));
        assert_eq!(fs.lookup(&creds, 0, b"next").await, Ok(source));
    }
}
