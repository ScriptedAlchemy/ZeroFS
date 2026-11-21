//! Content changes must discard inherited executable privileges even when
//! the caller owns the file. Ownership does not imply membership in its group.

use crate::fs::ZeroFS;
use crate::fs::inode::InodeAttrs;
use crate::fs::permissions::Credentials;
use crate::fs::types::{
    AuthContext, FallocateMode, SetAttributes, SetGid, SetMode, SetSize, SetUid,
};
use bytes::Bytes;

#[derive(Clone, Copy, Debug)]
enum Change {
    Write,
    Truncate(u64),
    Fallocate(FallocateMode),
}

async fn check_content_changes(actor: AuthContext) {
    let fs = ZeroFS::new_in_memory().await.unwrap();
    let root_auth = AuthContext::default();
    let root = Credentials::from_auth_context(&root_auth);
    let creds = Credentials::from_auth_context(&actor);
    let changes = [
        Change::Write,
        Change::Truncate(3),
        Change::Truncate(8),
        Change::Truncate(16),
        Change::Fallocate(FallocateMode::Allocate),
        Change::Fallocate(FallocateMode::PunchHole),
        Change::Fallocate(FallocateMode::ZeroRange { keep_size: true }),
    ];

    // Non-executable SGID has no privilege effect and must survive, while
    // SUID is cleared even on a currently non-executable file.
    for (mode, unprivileged_mode) in [(0o6755, 0o755), (0o2644, 0o2644), (0o4644, 0o644)] {
        for (index, change) in changes.iter().enumerate() {
            let name = format!("setid-{mode:o}-{index}");
            let (id, _) = fs
                .create(
                    &root,
                    0,
                    name.as_bytes(),
                    &SetAttributes {
                        uid: SetUid::Set(1000),
                        gid: SetGid::Set(0),
                        mode: SetMode::Set(mode),
                        ..Default::default()
                    },
                )
                .await
                .unwrap();
            fs.write(&root_auth, id, 0, &Bytes::from_static(b"original"))
                .await
                .unwrap();
            let attrs = match *change {
                Change::Write => {
                    fs.write(&actor, id, 0, &Bytes::from_static(b"modified"))
                        .await
                }
                Change::Truncate(size) => {
                    fs.setattr(
                        &creds,
                        id,
                        &SetAttributes {
                            size: SetSize::Set(size),
                            ..Default::default()
                        },
                    )
                    .await
                }
                Change::Fallocate(mode) => fs.fallocate_opened(&actor, id, 1, 2, mode).await,
            }
            .unwrap();
            let expected = if actor.uid == 0 {
                mode
            } else {
                unprivileged_mode
            };
            assert_eq!(attrs.mode, expected, "{change:?} of {mode:o}");
            assert_eq!(fs.inode_store.get(id).await.unwrap().mode(), expected);
            let listing = fs.readdir(&root_auth, 0, 0, 128).await.unwrap();
            let entry = listing
                .entries
                .iter()
                .find(|entry| entry.fileid == id)
                .unwrap();
            assert_eq!(
                entry.attr.mode, expected,
                "directory entry after {change:?}"
            );
        }
    }
}

#[tokio::test]
async fn owner_content_changes_clear_executable_privileges() {
    check_content_changes(AuthContext {
        uid: 1000,
        gid: 1000,
        ..Default::default()
    })
    .await;
}

#[tokio::test]
async fn root_content_changes_preserve_executable_privileges() {
    check_content_changes(AuthContext::default()).await;
}

#[tokio::test]
async fn empty_owner_write_preserves_executable_privileges() {
    let fs = ZeroFS::new_in_memory().await.unwrap();
    let root_auth = AuthContext::default();
    let root = Credentials::from_auth_context(&root_auth);
    let (id, attrs) = fs
        .create(
            &root,
            0,
            b"empty-owner-write",
            &SetAttributes {
                uid: SetUid::Set(1000),
                gid: SetGid::Set(0),
                mode: SetMode::Set(0o6755),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let owner = AuthContext {
        uid: 1000,
        gid: 1000,
        ..Default::default()
    };
    let after = fs.write(&owner, id, 0, &Bytes::new()).await.unwrap();
    assert_eq!(after.mode, attrs.mode);
    assert_eq!(fs.inode_store.get(id).await.unwrap().mode(), attrs.mode);
}
