use super::*;

#[tokio::test]
async fn opens_require_one_acl_entry_to_grant_the_complete_access_mode() {
    use crate::fs::permissions::acl::POSIX_ACL_ACCESS;
    use crate::fs::{XattrName, XattrSetMode, XattrValue};

    let fs = Arc::new(ZeroFS::new_in_memory().await.unwrap());
    let owner = credentials();
    let (file, _) = fs
        .create(&owner, 0, b"acl-open", &SetAttributes::default())
        .await
        .unwrap();
    let acl_name = XattrName::new(POSIX_ACL_ACCESS).unwrap();
    let actor = Credentials::from_auth_context(&AuthContext {
        uid: owner.uid + 1,
        gid: 300,
        gid_known: true,
        gids: vec![300, 301],
        groups_complete: true,
        groups_truncated: false,
    });
    for combined_grant in [false, true] {
        let entries = if combined_grant {
            [(300, 6), (301, 0)]
        } else {
            [(300, 4), (301, 2)]
        };
        let acl = PosixAcl::from_parts(6, &[], 0, &entries, 6, 0);
        fs.set_xattr(
            &owner,
            file,
            &acl_name,
            XattrValue::new(acl.to_bytes()).unwrap(),
            XattrSetMode::Either,
        )
        .await
        .unwrap();
        let inode = fs.visible_inode(file).await.unwrap();
        for cloned_open in [false, true] {
            for (flags, requires_both) in [
                (0, false),
                (LINUX_O_WRONLY, false),
                (LINUX_O_RDWR, true),
                (LINUX_O_TRUNC, true),
            ] {
                let handler = NinePHandler::new(Arc::clone(&fs), Arc::new(FileLockManager::new()));
                handler.session_state().fids.insert(
                    2,
                    FidSlot::unopened(Fid {
                        path: vec![Bytes::from_static(b"acl-open")],
                        inode_id: file,
                        root: 0,
                        qid: inode_to_qid(&inode, file),
                        opened: false,
                        mode: None,
                        creds: actor,
                    }),
                );
                let result = if cloned_open {
                    handler
                        .lopenat(Tlopenat {
                            fid: 2,
                            newfid: 3,
                            flags,
                        })
                        .await
                } else {
                    handler.lopen(Tlopen { fid: 2, flags }).await
                };
                if requires_both && !combined_grant {
                    assert_eq!(result.unwrap_err().to_errno(), crate::linux_errno::EACCES);
                    assert!(!handler.get_fid(2).unwrap().opened);
                    assert!(matches!(handler.get_fid(3), Err(P9Error::BadFid)));
                    assert_eq!(fs.open_handle_count(file), 0);
                } else {
                    result.unwrap();
                    assert!(
                        handler
                            .get_fid(if cloned_open { 3 } else { 2 })
                            .unwrap()
                            .opened
                    );
                    assert_eq!(fs.open_handle_count(file), 1);
                }
                drop(handler);
                assert_eq!(fs.open_handle_count(file), 0);
            }
        }
    }
}

fn credentials() -> Credentials {
    Credentials {
        uid: 1000,
        gid: 123,
        gid_known: true,
        groups: [456; 16],
        groups_count: 2,
        groups_complete: false,
        groups_truncated: false,
    }
}

#[test]
fn dot_walk_steps_do_not_grow_paths_or_displace_parent_components() {
    let mut path = vec![Bytes::from_static(b"a"), Bytes::from_static(b"b")];
    for _ in 0..10_000 {
        NinePHandler::step_path(&mut path, Bytes::from_static(b"."), 2, 0);
    }
    assert_eq!(
        path.len(),
        2,
        "dot walks must not allocate extra path entries"
    );
    assert_eq!(
        path,
        vec![Bytes::from_static(b"a"), Bytes::from_static(b"b")]
    );
    NinePHandler::step_path(&mut path, Bytes::from_static(b".."), 2, 0);
    assert_eq!(path, vec![Bytes::from_static(b"a")]);
    // A subtree attach root retains its absolute prefix for both dot forms.
    for name in [b".".as_slice(), b"..".as_slice()] {
        NinePHandler::step_path(&mut path, Bytes::from_static(name), 1, 1);
        assert_eq!(path, vec![Bytes::from_static(b"a")]);
    }
    NinePHandler::step_path(&mut path, Bytes::from_static(b".literal"), 1, 1);
    assert_eq!(
        path,
        vec![Bytes::from_static(b"a"), Bytes::from_static(b".literal")]
    );
}

#[tokio::test]
async fn dot_then_parent_walk_keeps_rename_source_consistent() {
    for compound in [false, true] {
        let fs = Arc::new(ZeroFS::new_in_memory().await.unwrap());
        let creds = credentials();
        let (parent, _) = fs
            .mkdir(&creds, 0, b"a", &SetAttributes::default())
            .await
            .unwrap();
        fs.mkdir(&creds, parent, b"b", &SetAttributes::default())
            .await
            .unwrap();
        let handler = NinePHandler::new(Arc::clone(&fs), Arc::new(FileLockManager::new()));
        let (regular, _) = fs
            .create(&creds, 0, b"regular", &SetAttributes::default())
            .await
            .unwrap();
        assert!(matches!(
            handler.walk_component(&creds, 0, regular, b".").await,
            Err(P9Error::NotADirectory)
        ));
        let (locked, _) = fs
            .mkdir(
                &creds,
                0,
                b"locked",
                &SetAttributes {
                    mode: SetMode::Set(0),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        assert_eq!(
            handler
                .walk_component(&creds, 0, locked, b".")
                .await
                .unwrap_err()
                .to_errno(),
            crate::linux_errno::EACCES
        );
        handler
            .attach(Tattach {
                fid: 1,
                afid: u32::MAX,
                uname: P9String::new(b"test".to_vec()),
                aname: P9String::new(Vec::new()),
                n_uname: creds.uid,
            })
            .await
            .unwrap();
        let names: Vec<P9String> = [b"a".as_slice(), b"b", b".", b".."]
            .into_iter()
            .map(|name| P9String::new(name.to_vec()))
            .collect();
        let walked = if compound {
            handler
                .walk_getattr(
                    Twalkgetattr {
                        fid: 1,
                        newfid: 2,
                        nwname: names.len() as u16,
                        wnames: names,
                    },
                    NameMatch::Folded,
                )
                .await
                .unwrap()
        } else {
            handler
                .walk(
                    Twalk {
                        fid: 1,
                        newfid: 2,
                        nwname: names.len() as u16,
                        wnames: names,
                    },
                    NameMatch::Folded,
                )
                .await
                .unwrap()
        };
        let qids = match walked {
            Message::Rwalk(reply) => reply.wqids,
            Message::Rwalkgetattr(reply) => reply.wqids,
            _ => panic!("expected a walk reply"),
        };
        assert_eq!(qids.len(), 4);
        assert_eq!(qids.last().unwrap().path, parent);
        let fid = handler.get_fid(2).unwrap();
        assert_eq!(fid.inode_id, parent);
        assert_eq!(fid.path, vec![Bytes::from_static(b"a")]);
        handler
            .rename(
                Trename {
                    fid: 2,
                    dfid: 1,
                    name: P9String::new(b"renamed".to_vec()),
                },
                [0; 16],
            )
            .await
            .expect("a dot/parent walk must still identify the same rename source");
        assert_eq!(fs.lookup(&creds, 0, b"renamed").await.unwrap(), parent);
        assert!(matches!(
            fs.lookup(&creds, 0, b"a").await,
            Err(FsError::NotFound)
        ));
    }
}

#[tokio::test]
async fn detached_parent_walk_restores_root_path_before_rename() {
    async fn walk(
        handler: &NinePHandler,
        fid: u32,
        newfid: u32,
        names: &[&[u8]],
        compound: bool,
        rule: NameMatch,
    ) -> Vec<Qid> {
        let wnames = names
            .iter()
            .map(|name| P9String::new(name.to_vec()))
            .collect();
        let reply = if compound {
            handler
                .walk_getattr(
                    Twalkgetattr {
                        fid,
                        newfid,
                        nwname: names.len() as u16,
                        wnames,
                    },
                    rule,
                )
                .await
                .unwrap()
        } else {
            handler
                .walk(
                    Twalk {
                        fid,
                        newfid,
                        nwname: names.len() as u16,
                        wnames,
                    },
                    rule,
                )
                .await
                .unwrap()
        };
        match reply {
            Message::Rwalk(reply) => reply.wqids,
            Message::Rwalkgetattr(reply) => {
                assert_eq!(reply.stat.qid, *reply.wqids.last().unwrap());
                reply.wqids
            }
            _ => panic!("expected a walk reply"),
        }
    }

    for compound in [false, true] {
        for rule in [NameMatch::Folded, NameMatch::Exact] {
            for rename_root in [false, true] {
                let fs = Arc::new(ZeroFS::new_in_memory().await.unwrap());
                let creds = credentials();
                let auth = AuthContext::from(&creds);
                let (exports, _) = fs
                    .mkdir(&creds, 0, b"exports", &SetAttributes::default())
                    .await
                    .unwrap();
                let (root, _) = fs
                    .mkdir(&creds, exports, b"tenant", &SetAttributes::default())
                    .await
                    .unwrap();
                let (parent, _) = fs
                    .mkdir(&creds, root, b"a", &SetAttributes::default())
                    .await
                    .unwrap();
                let (child, _) = fs
                    .mkdir(&creds, parent, b"b", &SetAttributes::default())
                    .await
                    .unwrap();
                let (inside, _) = fs
                    .create(&creds, root, b"file", &SetAttributes::default())
                    .await
                    .unwrap();
                let (outside, _) = fs
                    .create(&creds, parent, b"file", &SetAttributes::default())
                    .await
                    .unwrap();
                let handler = NinePHandler::new(Arc::clone(&fs), Arc::new(FileLockManager::new()));
                handler
                    .attach(Tattach {
                        fid: 1,
                        afid: u32::MAX,
                        uname: P9String::new(b"test".to_vec()),
                        aname: P9String::new(b"exports/tenant".to_vec()),
                        n_uname: creds.uid,
                    })
                    .await
                    .unwrap();
                let initial = walk(&handler, 1, 2, &[b"a", b"b"], compound, rule).await;
                assert_eq!(initial.last().unwrap().path, child);

                // The fid still names b, but its parent chain now misses the
                // attach root entirely. The next '..' must jump to that root.
                fs.rename(&auth, root, b"a", 0, b"escaped").await.unwrap();
                let mut root_path = vec![
                    Bytes::from_static(b"exports"),
                    Bytes::from_static(b"tenant"),
                ];
                let mut root_parent = exports;
                let mut root_name = b"tenant".as_slice();
                if rename_root {
                    let (relocated, _) = fs
                        .mkdir(&creds, 0, b"relocated", &SetAttributes::default())
                        .await
                        .unwrap();
                    let (new_parent, _) = fs
                        .mkdir(&creds, relocated, b"parent", &SetAttributes::default())
                        .await
                        .unwrap();
                    fs.rename(&auth, exports, b"tenant", new_parent, b"rebased")
                        .await
                        .unwrap();
                    root_path = vec![
                        Bytes::from_static(b"relocated"),
                        Bytes::from_static(b"parent"),
                        Bytes::from_static(b"rebased"),
                    ];
                    root_parent = new_parent;
                    root_name = b"rebased";
                }

                let clamped = walk(&handler, 2, 3, &[b"..", b".", b".."], compound, rule).await;
                assert_eq!(clamped.len(), 3);
                assert!(clamped.iter().all(|qid| qid.path == root));
                let root_fid = handler.get_fid(3).unwrap();
                assert_eq!(root_fid.inode_id, root);
                assert_eq!(root_fid.root, root);

                let file = walk(&handler, 3, 4, &[b"file"], compound, rule).await;
                assert_eq!(file.last().unwrap().path, inside);
                let mut file_path = root_path.clone();
                file_path.push(Bytes::from_static(b"file"));
                assert_eq!(handler.get_fid(4).unwrap().path, file_path);
                handler
                    .rename(
                        Trename {
                            fid: 4,
                            dfid: 3,
                            name: P9String::new(b"renamed-file".to_vec()),
                        },
                        [0; 16],
                    )
                    .await
                    .expect("a clamped walk must retain the correct absolute rename source");
                assert_eq!(root_fid.path, root_path);
                *file_path.last_mut().unwrap() = Bytes::from_static(b"renamed-file");
                assert_eq!(handler.get_fid(4).unwrap().path, file_path);
                assert_eq!(
                    fs.lookup(&creds, root, b"renamed-file").await.unwrap(),
                    inside
                );
                assert!(matches!(
                    fs.lookup(&creds, root, b"file").await,
                    Err(FsError::NotFound)
                ));
                assert_eq!(fs.lookup(&creds, parent, b"file").await.unwrap(), outside);

                // A pinned but removed root no longer has an absolute name.
                // Fail the clamp without installing a fid with a stale path.
                fs.remove(&auth, root, b"renamed-file").await.unwrap();
                fs.remove(&auth, root_parent, root_name).await.unwrap();
                let wnames = vec![P9String::new(b"..".to_vec())];
                let failed = if compound {
                    handler
                        .walk_getattr(
                            Twalkgetattr {
                                fid: 2,
                                newfid: 5,
                                nwname: 1,
                                wnames,
                            },
                            rule,
                        )
                        .await
                } else {
                    handler
                        .walk(
                            Twalk {
                                fid: 2,
                                newfid: 5,
                                nwname: 1,
                                wnames,
                            },
                            rule,
                        )
                        .await
                };
                assert!(matches!(failed, Err(P9Error::Fs(FsError::StaleHandle))));
                assert!(handler.get_fid(5).is_err());
                assert_eq!(handler.get_fid(2).unwrap().inode_id, child);
            }
        }
    }
}

#[tokio::test]
async fn io_snapshot_preserves_open_modes_credentials_and_pin_ownership() {
    let fs = Arc::new(ZeroFS::new_in_memory().await.unwrap());
    let creds = credentials();
    let (file, _) = fs
        .create(&creds, 0, b"io-context", &SetAttributes::default())
        .await
        .unwrap();
    let handler = NinePHandler::new(Arc::clone(&fs), Arc::new(FileLockManager::new()));
    let inode = fs.visible_inode(file).await.unwrap();
    let mut slot = FidSlot::unopened(Fid {
        path: (0..48).map(|i| Bytes::from(vec![i; 64])).collect(),
        inode_id: file,
        root: 0,
        qid: inode_to_qid(&inode, file),
        opened: true,
        mode: Some(LINUX_O_RDWR),
        creds,
    });
    slot.handle = Some(fs.new_open_handle(file));
    handler.session_state().fids.insert(2, slot);

    for (opened, mode, read, write) in [
        (true, Some(0), true, false),
        (true, Some(LINUX_O_WRONLY), false, true),
        (true, Some(LINUX_O_RDWR), true, true),
        (true, Some(LINUX_O_PATH), false, false),
        (true, Some(LINUX_O_PATH | LINUX_O_RDWR), false, false),
        (true, Some(LINUX_O_ACCMODE), false, false),
        (true, None, false, false),
        (false, Some(LINUX_O_RDWR), false, false),
    ] {
        {
            let mut state = handler.session_state();
            let fid = &mut state.fids.get_mut(&2).unwrap().fid;
            fid.opened = opened;
            fid.mode = mode;
        }
        let context = handler.get_fid_io_context(2).unwrap();
        assert_eq!(context.inode_id, file);
        assert_eq!((context.allows_read, context.allows_write), (read, write));
        assert_eq!(context.creds.uid, creds.uid);
        assert_eq!(context.creds.gid, creds.gid);
        assert_eq!(context.creds.gid_known, creds.gid_known);
        assert_eq!(context.creds.groups, creds.groups);
        assert_eq!(context.creds.groups_count, creds.groups_count);
        assert_eq!(context.creds.groups_complete, creds.groups_complete);
        assert_eq!(fs.open_handle_count(file), 1);
        assert!(
            handler.session_state().fids[&2]
                .fid
                .path
                .iter()
                .all(Bytes::is_unique)
        );
        if !read {
            assert!(matches!(
                handler
                    .read(Tread {
                        fid: 2,
                        offset: 0,
                        count: 1
                    })
                    .await,
                Err(P9Error::BadFid)
            ));
        }
        if !write {
            assert!(matches!(
                handler
                    .write(
                        Twrite {
                            fid: 2,
                            offset: 0,
                            count: 1,
                            data: Bytes::from_static(b"x").into()
                        },
                        [0; 16],
                        0,
                    )
                    .await,
                Err(P9Error::BadFid)
            ));
        }
    }

    handler.session_state().fids.get_mut(&2).unwrap().xattr = Some(XattrFid::Closed);
    assert!(matches!(
        handler.get_fid_io_context(2),
        Err(P9Error::BadFid)
    ));
    assert!(matches!(
        handler.get_fid_io_context(3),
        Err(P9Error::BadFid)
    ));
    assert_eq!(fs.open_handle_count(file), 1);
    handler.session_state().fids.remove(&2);
    assert_eq!(fs.open_handle_count(file), 0);
}

#[tokio::test]
async fn io_uses_the_pinned_open_inode_after_name_and_permission_changes() {
    let fs = Arc::new(ZeroFS::new_in_memory().await.unwrap());
    let creds = credentials();
    let auth = AuthContext::from(&creds);
    let (file, _) = fs
        .create(&creds, 0, b"before", &SetAttributes::default())
        .await
        .unwrap();
    let handler = NinePHandler::new(Arc::clone(&fs), Arc::new(FileLockManager::new()));
    handler
        .attach(Tattach {
            fid: 1,
            afid: u32::MAX,
            uname: P9String::new(b"test".to_vec()),
            aname: P9String::new(Vec::new()),
            n_uname: creds.uid,
        })
        .await
        .unwrap();
    handler
        .walk(
            Twalk {
                fid: 1,
                newfid: 2,
                nwname: 1,
                wnames: vec![P9String::new(b"before".to_vec())],
            },
            NameMatch::Folded,
        )
        .await
        .unwrap();
    handler
        .lopen(Tlopen {
            fid: 2,
            flags: LINUX_O_RDWR,
        })
        .await
        .unwrap();
    assert_eq!(fs.open_handle_count(file), 1);

    fs.rename(&auth, 0, b"before", 0, b"after").await.unwrap();
    fs.setattr(
        &creds,
        file,
        &SetAttributes {
            mode: SetMode::Set(0),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    fs.remove(&auth, 0, b"after").await.unwrap();
    assert_eq!(fs.open_handle_count(file), 1);
    let reply = handler
        .write(
            Twrite {
                fid: 2,
                offset: 0,
                count: 7,
                data: Bytes::from_static(b"payload").into(),
            },
            [0; 16],
            0,
        )
        .await
        .unwrap();
    assert!(matches!(reply, Message::Rwrite(Rwrite { count: 7 })));
    let Message::Rread(read) = handler
        .read(Tread {
            fid: 2,
            offset: 0,
            count: 7,
        })
        .await
        .unwrap()
    else {
        panic!("expected an ordinary read reply");
    };
    assert_eq!(read.data.as_ref(), b"payload");
    handler.clunk(Tclunk { fid: 2 }).await.unwrap();
    assert_eq!(fs.open_handle_count(file), 0);
}
