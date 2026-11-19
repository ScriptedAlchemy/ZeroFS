use super::*;
use crate::fs::permissions::acl::{POSIX_ACL_ACCESS, POSIX_ACL_DEFAULT, PosixAcl};
use crate::fs::{XattrName, XattrSetMode, XattrValue};

const NAMES: [&[u8]; 4] = [b"file", b"directory", b"symlink", b"fifo"];

fn test_creds() -> Credentials {
    Credentials::from_auth_context(&AuthContext {
        uid: 1000,
        gid: 1000,
        ..AuthContext::default()
    })
}

async fn bind_parent(handler: &NinePHandler, parent: InodeId, creds: Credentials) {
    let inode = handler.filesystem.visible_inode(parent).await.unwrap();
    handler.session_state().fids.insert(
        1,
        FidSlot::unopened(Fid {
            path: vec![Bytes::copy_from_slice(inode.name().unwrap())],
            inode_id: parent,
            root: 0,
            qid: inode_to_qid(&inode, parent),
            opened: false,
            mode: None,
            creds,
        }),
    );
}

async fn create_kind(
    handler: &NinePHandler,
    kind: usize,
    gid: u32,
) -> P9Result<(InodeId, FileAttributes)> {
    let name = P9String::new(NAMES[kind].to_vec());
    match kind {
        0 => {
            handler
                .create_child(&handler.get_fid(1)?, &name.data, 0o600, gid, [0; 16], 0)
                .await
        }
        1 => {
            handler
                .make_dir(
                    &Tmkdir {
                        dfid: 1,
                        name,
                        mode: 0o700,
                        gid,
                    },
                    [0; 16],
                    false,
                )
                .await
        }
        2 => {
            handler
                .make_symlink(
                    &Tsymlink {
                        dfid: 1,
                        name,
                        symtgt: P9String::new(b"target".to_vec()),
                        gid,
                    },
                    [0; 16],
                )
                .await
        }
        3 => {
            handler
                .make_node(
                    &Tmknod {
                        dfid: 1,
                        name,
                        mode: S_IFIFO | 0o600,
                        major: 0,
                        minor: 0,
                        gid,
                    },
                    [0; 16],
                    false,
                )
                .await
        }
        _ => unreachable!(),
    }
}

#[tokio::test]
async fn requested_creation_gid_never_changes_parent_authorization() {
    struct Case {
        name: &'static [u8],
        mode: u32,
        parent_gid: u32,
        complete: bool,
        supplementary: &'static [u32],
        acl: Option<PosixAcl>,
    }
    let fs = Arc::new(ZeroFS::new_in_memory().await.unwrap());
    let owner = test_creds();
    for case in [
        Case {
            name: b"mode-missing-group",
            mode: 0o731,
            parent_gid: 300,
            complete: true,
            supplementary: &[],
            acl: None,
        },
        Case {
            name: b"acl-missing-group",
            mode: 0o700,
            parent_gid: 1000,
            complete: true,
            supplementary: &[],
            acl: Some(PosixAcl::from_parts(7, &[], 0, &[(300, 3)], 3, 1)),
        },
        Case {
            name: b"acl-unknown-groups",
            mode: 0o700,
            parent_gid: 1000,
            complete: false,
            supplementary: &[],
            acl: Some(PosixAcl::from_parts(7, &[], 0, &[(300, 3)], 3, 1)),
        },
        Case {
            name: b"mode-primary-group",
            mode: 0o703,
            parent_gid: 2000,
            complete: true,
            supplementary: &[300],
            acl: None,
        },
        Case {
            name: b"mode-unknown-primary-group",
            mode: 0o703,
            parent_gid: 2000,
            complete: false,
            supplementary: &[],
            acl: None,
        },
        Case {
            name: b"acl-primary-group",
            mode: 0o700,
            parent_gid: 2000,
            complete: true,
            supplementary: &[300],
            acl: Some(PosixAcl::from_parts(7, &[(4000, 3)], 0, &[], 3, 3)),
        },
    ] {
        let (parent, _) = fs
            .mkdir(
                &owner,
                0,
                case.name,
                &SetAttributes {
                    mode: SetMode::Set(case.mode),
                    gid: SetGid::Set(case.parent_gid),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        if let Some(acl) = case.acl {
            fs.set_xattr(
                &owner,
                parent,
                &XattrName::new(POSIX_ACL_ACCESS).unwrap(),
                XattrValue::new(acl.to_bytes()).unwrap(),
                XattrSetMode::Either,
            )
            .await
            .unwrap();
        }
        let actor = Credentials::from_auth_context(&AuthContext {
            uid: 2000,
            gid: 2000,
            gids: case.supplementary.to_vec(),
            groups_complete: case.complete,
            ..AuthContext::default()
        });
        let handler = NinePHandler::new(Arc::clone(&fs), Arc::new(FileLockManager::new()));
        bind_parent(&handler, parent, actor).await;
        for (kind, name) in NAMES.iter().enumerate() {
            let result = create_kind(&handler, kind, 300).await;
            assert_eq!(
                result.unwrap_err().to_errno(),
                crate::linux_errno::EACCES,
                "case {:?}, kind {kind}",
                case.name
            );
            assert_eq!(
                fs.lookup(&owner, parent, name).await,
                Err(FsError::NotFound)
            );
        }
        assert_eq!(handler.get_fid(1).unwrap().creds.gid, actor.gid);
    }
}

#[tokio::test]
async fn creation_gid_selection_preserves_inheritance_and_listener_override() {
    let fs = Arc::new(ZeroFS::new_in_memory().await.unwrap());
    let owner = test_creds();
    for (index, inherit, override_credentials) in [
        (0, false, false),
        (1, true, false),
        (2, false, true),
        (3, true, true),
    ] {
        let (parent, _) = fs
            .mkdir(
                &owner,
                0,
                format!("creation-gid-{index}").as_bytes(),
                &SetAttributes {
                    mode: SetMode::Set(if inherit { 0o2777 } else { 0o777 }),
                    gid: SetGid::Set(4000),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        let handler = NinePHandler::new(Arc::clone(&fs), Arc::new(FileLockManager::new()));
        let (handler, actor) = if override_credentials {
            let handler = handler.with_credential_override(2000, 2000);
            let actor = handler.creds_for(9999, "untrusted");
            (handler, actor)
        } else {
            (
                handler,
                Credentials::from_auth_context(&AuthContext {
                    uid: 2000,
                    gid: 2000,
                    gids: vec![300],
                    ..AuthContext::default()
                }),
            )
        };
        bind_parent(&handler, parent, actor).await;
        let expected_gid = if inherit {
            4000
        } else if override_credentials {
            2000
        } else {
            300
        };
        for kind in 0..NAMES.len() {
            let (_, attrs) = create_kind(&handler, kind, 300).await.unwrap();
            assert_eq!(attrs.uid, 2000);
            assert_eq!(attrs.gid, expected_gid, "case {index}, kind {kind}");
            if kind == 1 {
                assert_eq!(attrs.mode & 0o2000 != 0, inherit);
            }
        }
        assert_eq!(handler.get_fid(1).unwrap().creds.gid, actor.gid);
    }
}

#[tokio::test]
async fn creation_group_hint_is_rejected_by_setattr() {
    let fs = ZeroFS::new_in_memory().await.unwrap();
    let owner = test_creds();
    let (id, _) = fs
        .create(&owner, 0, b"setattr", &SetAttributes::default())
        .await
        .unwrap();
    assert_eq!(
        fs.setattr(
            &owner,
            id,
            &SetAttributes {
                gid: SetGid::InheritOr(300),
                ..Default::default()
            }
        )
        .await
        .map(|_| ()),
        Err(FsError::InvalidArgument)
    );
    assert_eq!(fs.inode_store.get(id).await.unwrap().gid(), owner.gid);
}

#[tokio::test]
async fn private_create_umask_is_applied_only_without_a_default_acl() {
    const O_RDWR: u32 = 2;
    let fs = Arc::new(ZeroFS::new_in_memory().await.unwrap());
    let owner = test_creds();
    for inherit_acl in [false, true] {
        let (parent, _) = fs
            .mkdir(
                &owner,
                0,
                if inherit_acl { b"inherited" } else { b"plain" },
                &SetAttributes {
                    mode: SetMode::Set(0o2770),
                    gid: SetGid::Set(3000),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        if inherit_acl {
            let acl = PosixAcl::from_parts(7, &[(2000, 7)], 7, &[], 7, 0);
            fs.set_xattr(
                &owner,
                parent,
                &XattrName::new(POSIX_ACL_DEFAULT).unwrap(),
                XattrValue::new(acl.to_bytes()).unwrap(),
                XattrSetMode::Either,
            )
            .await
            .unwrap();
        }
        let handler = NinePHandler::new(Arc::clone(&fs), Arc::new(FileLockManager::new()));
        let version = handler
            .version(Tversion {
                msize: P9_MAX_MSIZE,
                version: P9String::new(VERSION_9P2000L_ZEROFS_UMASK.to_vec()),
            })
            .await
            .unwrap();
        assert!(matches!(version, Message::Rversion(version)
            if version.version.data == VERSION_9P2000L_ZEROFS_UMASK));
        bind_parent(&handler, parent, owner).await;
        for (kind, name) in [b"open".as_slice(), b"directory", b"regular", b"fifo"]
            .into_iter()
            .enumerate()
        {
            let file_type = match kind {
                1 => S_IFDIR,
                3 => S_IFIFO,
                _ => S_IFREG,
            };
            let requested = if kind == 1 { 0o770 } else { 0o666 };
            let mode = file_type | requested | (0o777 << CREATE_UMASK_SHIFT);
            match kind {
                0 => {
                    handler
                        .lcreateattr(
                            Tlcreateattr {
                                dfid: 1,
                                newfid: 2,
                                name: P9String::new(name.to_vec()),
                                flags: O_RDWR,
                                mode,
                                gid: owner.gid,
                            },
                            [0; 16],
                        )
                        .await
                        .unwrap();
                }
                1 => {
                    handler
                        .mkdir_attr(
                            Tmkdir {
                                dfid: 1,
                                name: P9String::new(name.to_vec()),
                                mode,
                                gid: owner.gid,
                            },
                            [0; 16],
                        )
                        .await
                        .unwrap();
                }
                _ => {
                    handler
                        .mknod_attr(
                            Tmknod {
                                dfid: 1,
                                name: P9String::new(name.to_vec()),
                                mode,
                                major: 0,
                                minor: 0,
                                gid: owner.gid,
                            },
                            [0; 16],
                        )
                        .await
                        .unwrap();
                }
            }
            let child = fs.lookup(&owner, parent, name).await.unwrap();
            let inode = fs.inode_store.get(child).await.unwrap();
            let expected = match (inherit_acl, kind == 1) {
                (false, false) => 0,
                (false, true) => 0o2000,
                (true, false) => 0o660,
                (true, true) => 0o2770,
            };
            assert_eq!(inode.mode(), expected, "ACL {inherit_acl}, kind {kind}");
            assert_eq!(inode.gid(), 3000);
            assert_eq!(inode.is_directory(), kind == 1);
            assert_eq!(matches!(inode, Inode::Fifo(_)), kind == 3);
            let access = fs
                .get_xattr(&owner, child, &XattrName::new(POSIX_ACL_ACCESS).unwrap())
                .await;
            assert_eq!(access.is_ok(), inherit_acl);
        }
    }
}
