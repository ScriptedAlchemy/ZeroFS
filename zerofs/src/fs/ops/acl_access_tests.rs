use super::*;
use crate::fs::inode::{InodeAttrs, InodeId};
use crate::fs::permissions::acl::{POSIX_ACL_ACCESS, PosixAcl};
use crate::fs::permissions::{AccessMode, Credentials};
use crate::fs::test_util::test_creds;
use crate::fs::types::{AuthContext, FileType, SetAttributes, SetMode};

#[tokio::test]
async fn namespace_mutations_require_a_combined_acl_group_grant() {
    let fs = ZeroFS::new_in_memory().await.unwrap();
    let owner = test_creds();
    let actor = AuthContext {
        uid: owner.uid + 1,
        gid: 300,
        gid_known: true,
        gids: vec![300, 301],
        groups_complete: true,
        groups_truncated: false,
    };
    let creds = Credentials::from_auth_context(&actor);
    let attrs = SetAttributes {
        mode: SetMode::Set(0o777),
        ..Default::default()
    };
    let restricted = fs.mkdir(&owner, 0, b"restricted", &attrs).await.unwrap().0;
    let unrestricted = fs
        .mkdir(&owner, 0, b"unrestricted", &attrs)
        .await
        .unwrap()
        .0;
    let victim = fs
        .create(&owner, restricted, b"victim", &attrs)
        .await
        .unwrap()
        .0;
    let incoming = fs
        .create(&owner, unrestricted, b"incoming", &attrs)
        .await
        .unwrap()
        .0;
    let acl_name = XattrName::new(POSIX_ACL_ACCESS).unwrap();
    let split = PosixAcl::from_parts(7, &[], 0, &[(300, 2), (301, 1)], 3, 0);
    fs.set_xattr(
        &owner,
        restricted,
        &acl_name,
        XattrValue::new(split.to_bytes()).unwrap(),
        XattrSetMode::Either,
    )
    .await
    .unwrap();
    let inode = fs.inode_store.get(restricted).await.unwrap();
    for mode in [AccessMode::Write, AccessMode::Execute] {
        fs.check_inode_access(restricted, &inode, &creds, mode)
            .await
            .unwrap();
    }
    let denied = Err(FsError::PermissionDenied);
    assert_eq!(
        fs.create(&creds, restricted, b"new-file", &attrs)
            .await
            .map(|_| ()),
        denied
    );
    assert_eq!(
        fs.mkdir(&creds, restricted, b"new-directory", &attrs)
            .await
            .map(|_| ()),
        denied
    );
    assert_eq!(
        fs.mknod(
            &creds,
            restricted,
            b"new-fifo",
            FileType::Fifo,
            &attrs,
            None
        )
        .await
        .map(|_| ()),
        denied
    );
    assert_eq!(
        fs.symlink(&creds, restricted, b"new-symlink", b"target", &attrs)
            .await
            .map(|_| ()),
        denied
    );
    assert_eq!(
        fs.link(&actor, incoming, restricted, b"new-hardlink").await,
        denied
    );
    assert_eq!(fs.remove(&actor, restricted, b"victim").await, denied);
    assert_eq!(
        fs.rename(&actor, restricted, b"victim", restricted, b"renamed")
            .await,
        denied
    );
    assert_eq!(
        fs.rename(&actor, restricted, b"victim", unrestricted, b"outgoing")
            .await,
        denied
    );
    assert_eq!(
        fs.rename(&actor, unrestricted, b"incoming", restricted, b"incoming")
            .await,
        denied
    );
    assert_eq!(
        fs.lookup(&owner, restricted, b"victim").await.unwrap(),
        victim
    );
    assert_eq!(
        fs.lookup(&owner, unrestricted, b"incoming").await.unwrap(),
        incoming
    );
    assert_eq!(fs.inode_store.get(incoming).await.unwrap().nlink(), 1);

    // A single matching group granting both bits restores normal mutations.
    let together = PosixAcl::from_parts(7, &[], 0, &[(300, 3), (301, 0)], 3, 0);
    fs.set_xattr(
        &owner,
        restricted,
        &acl_name,
        XattrValue::new(together.to_bytes()).unwrap(),
        XattrSetMode::Either,
    )
    .await
    .unwrap();
    let new_file: InodeId = fs
        .create(&creds, restricted, b"new-file", &attrs)
        .await
        .unwrap()
        .0;
    fs.rename(&actor, restricted, b"new-file", restricted, b"renamed")
        .await
        .unwrap();
    assert_eq!(
        fs.lookup(&owner, restricted, b"renamed").await.unwrap(),
        new_file
    );
    fs.remove(&actor, restricted, b"renamed").await.unwrap();
}
