use super::*;
use crate::fs::permissions::acl::{POSIX_ACL_ACCESS, PosixAcl};
use crate::fs::types::SetMode;
use crate::fs::{XattrName, XattrSetMode, XattrValue};

fn auth(uid: u32, gid: u32, gids: &[u32]) -> NfsAuthContext {
    NfsAuthContext {
        uid,
        gid,
        gids: gids.to_vec(),
    }
}

async fn fixture() -> (Arc<ZeroFS>, Credentials, fileid3) {
    let fs = Arc::new(ZeroFS::new_in_memory().await.unwrap());
    let owner = Credentials::from_auth_context(&AuthContext {
        uid: 1000,
        gid: 1000,
        ..AuthContext::default()
    });
    let (file, _) = fs
        .create(
            &owner,
            0,
            b"access-file",
            &SetAttributes {
                mode: SetMode::Set(0o600),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    (fs, owner, file)
}

async fn set_acl(fs: &ZeroFS, owner: &Credentials, id: fileid3, acl: PosixAcl) {
    fs.set_xattr(
        owner,
        id,
        &XattrName::new(POSIX_ACL_ACCESS).unwrap(),
        XattrValue::new(acl.to_bytes()).unwrap(),
        XattrSetMode::Either,
    )
    .await
    .unwrap();
}

#[tokio::test]
async fn access_honors_modes_shared_identity_and_requested_rights() {
    let (fs, _, file) = fixture().await;
    let adapter = NFSAdapter::new(Arc::clone(&fs));
    let owner = auth(1000, 1000, &[]);
    let stranger = auth(2000, 2000, &[]);
    let writable = ACCESS3_READ | ACCESS3_MODIFY | ACCESS3_EXTEND;
    assert_eq!(
        adapter.access(&owner, file, u32::MAX).await.unwrap().access,
        writable
    );
    assert_eq!(
        adapter
            .access(&stranger, file, u32::MAX)
            .await
            .unwrap()
            .access,
        0
    );
    let requested = adapter.access(&owner, file, ACCESS3_READ).await.unwrap();
    assert_eq!(requested.access, ACCESS3_READ);
    assert_eq!(requested.attributes.fileid, file);
    assert_eq!(requested.attributes.mode, 0o600);
    assert_eq!(adapter.access(&owner, file, 0).await.unwrap().access, 0);
    // Root's file-execute exception still applies, and file LOOKUP/DELETE
    // are not meaningful rights in an ACCESS response.
    assert_eq!(
        adapter
            .access(&auth(0, 0, &[]), file, ACCESS3_ALL)
            .await
            .unwrap()
            .access,
        writable
    );
    assert!(matches!(
        adapter.access(&owner, u64::MAX, ACCESS3_ALL).await,
        Err(nfsstat3::NFS3ERR_NOENT)
    ));
    let shared = adapter.with_shared_identity(Some(NfsSharedIdentity {
        uid: 1000,
        gid: 1000,
    }));
    assert_eq!(
        shared
            .access(&stranger, file, ACCESS3_ALL)
            .await
            .unwrap()
            .access,
        writable
    );
}

#[tokio::test]
async fn access_honors_named_user_acl_and_owning_group_entry() {
    use crate::fs::permissions::acl::PosixAcl;
    use crate::fs::{XattrName, XattrSetMode, XattrValue};

    let (fs, owner, file) = fixture().await;
    let acl = PosixAcl::from_text("u::rw-,u:2000:r--,g::---,m::r--,o::---");
    fs.set_xattr(
        &owner,
        file,
        &XattrName::new(b"system.posix_acl_access").unwrap(),
        XattrValue::new(acl.to_bytes()).unwrap(),
        XattrSetMode::Either,
    )
    .await
    .unwrap();

    let adapter = NFSAdapter::new(fs);
    let named = adapter
        .access(&auth(2000, 2000, &[]), file, ACCESS3_ALL)
        .await
        .unwrap();
    assert_eq!(named.access, ACCESS3_READ);
    assert_eq!(named.attributes.mode, 0o640);
    let owning_group = adapter
        .access(&auth(3000, 1000, &[]), file, ACCESS3_ALL)
        .await
        .unwrap();
    assert_eq!(owning_group.access, 0);
}

#[tokio::test]
async fn access_honors_acl_named_users_groups_and_directory_combined_rights() {
    let (fs, owner, file) = fixture().await;
    let adapter = NFSAdapter::new(Arc::clone(&fs));
    set_acl(
        &fs,
        &owner,
        file,
        PosixAcl::from_parts(6, &[(2000, 0)], 0, &[(300, 4)], 4, 4),
    )
    .await;
    assert_eq!(
        adapter
            .access(&auth(2000, 300, &[]), file, ACCESS3_ALL)
            .await
            .unwrap()
            .access,
        0
    );
    assert_eq!(
        adapter
            .access(&auth(2001, 2001, &[300]), file, ACCESS3_ALL)
            .await
            .unwrap()
            .access,
        ACCESS3_READ
    );

    // NFS ACCESS evaluates rights separately, even though a combined open
    // must be authorized by one matching group ACL entry.
    set_acl(
        &fs,
        &owner,
        file,
        PosixAcl::from_parts(6, &[], 0, &[(300, 4), (301, 2)], 6, 0),
    )
    .await;
    let actor = auth(2001, 300, &[301]);
    assert_eq!(
        adapter
            .access(&actor, file, ACCESS3_ALL)
            .await
            .unwrap()
            .access,
        ACCESS3_READ | ACCESS3_MODIFY | ACCESS3_EXTEND
    );

    let (dir, _) = fs
        .mkdir(&owner, 0, b"access-dir", &SetAttributes::default())
        .await
        .unwrap();
    set_acl(
        &fs,
        &owner,
        dir,
        PosixAcl::from_parts(7, &[], 0, &[(300, 2), (301, 1)], 3, 0),
    )
    .await;
    assert_eq!(
        adapter
            .access(&actor, dir, ACCESS3_ALL)
            .await
            .unwrap()
            .access,
        ACCESS3_LOOKUP
    );
    set_acl(
        &fs,
        &owner,
        dir,
        PosixAcl::from_parts(7, &[], 0, &[(300, 3), (301, 0)], 3, 0),
    )
    .await;
    assert_eq!(
        adapter
            .access(&actor, dir, ACCESS3_ALL)
            .await
            .unwrap()
            .access,
        ACCESS3_LOOKUP | ACCESS3_MODIFY | ACCESS3_EXTEND | ACCESS3_DELETE
    );
}

#[tokio::test]
async fn access_preserves_acknowledged_acl_write_attributes_before_materialization() {
    use crate::fs::mutation::config::{
        ClientDurabilityTarget, FilesystemWriteAckMode, FilesystemWriteAckSettings,
        FilesystemWriteAckSource,
    };
    use crate::fs::types::{SetTime, Timestamp};
    use bytes::Bytes;
    use zerofs_nfsserve::xdr::XDR;

    fn encoded(attrs: &fattr3) -> Vec<u8> {
        let mut bytes = Vec::new();
        attrs.serialize(&mut bytes).unwrap();
        bytes
    }

    let mut fs = ZeroFS::new_in_memory().await.unwrap();
    fs.write_ack = FilesystemWriteAckSettings {
        mode: FilesystemWriteAckMode::VolatileMemory,
        volatile_memory_bytes: 2 * 1024 * 1024,
        volatile_max_operations: 64,
        source: FilesystemWriteAckSource::Filesystem,
        client_durability_target: ClientDurabilityTarget::LocalSsd,
    };
    let fs = Arc::new(fs);
    fs.install_volatile_overlay();
    let owner_auth = AuthContext {
        uid: 1000,
        gid: 1000,
        ..AuthContext::default()
    };
    let owner = Credentials::from_auth_context(&owner_auth);
    let file = fs
        .create_exclusive(&owner_auth, 0, b"volatile-access-file")
        .await
        .unwrap();
    fs.write(&owner_auth, file, 0, &Bytes::from_static(b"seed"))
        .await
        .unwrap();
    set_acl(
        &fs,
        &owner,
        file,
        PosixAcl::from_parts(6, &[(2000, 6)], 0, &[], 6, 0),
    )
    .await;
    fs.setattr(
        &owner,
        file,
        &SetAttributes {
            mode: SetMode::Set(0o6660),
            atime: SetTime::SetToClientTime(Timestamp {
                seconds: 1,
                nanoseconds: 2,
            }),
            mtime: SetTime::SetToClientTime(Timestamp {
                seconds: 3,
                nanoseconds: 4,
            }),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let adapter = NFSAdapter::new(Arc::clone(&fs));
    let writer = auth(2000, 2000, &[]);
    let before = adapter.getattr(&writer, file).await.unwrap();
    assert_eq!((before.size, before.mode), (4, 0o6660));

    let release = fs
        .materializer
        .get()
        .unwrap()
        .hold_inode_lane_for_test(file)
        .await;
    let written = tokio::time::timeout(
        Duration::from_secs(2),
        adapter.write(&writer, file, before.size, b"tail"),
    )
    .await
    .expect("RAM acknowledgement must not wait for materialization")
    .unwrap();
    // SUID is discarded by a content change, while non-executable SGID
    // survives, as on Linux. ACCESS must preserve both resulting decisions.
    assert_eq!((written.size, written.mode), (8, 0o2660));
    assert_ne!(written.mtime.seconds, before.mtime.seconds);
    let visible = adapter.getattr(&writer, file).await.unwrap();
    assert_eq!(encoded(&visible), encoded(&written));

    // The named user takes the ACL snapshot path, while owner/root keep their
    // fast paths. All must return the same acknowledged metadata as GETATTR,
    // including SUID clearing, even though canonical apply is still held.
    let writable = ACCESS3_READ | ACCESS3_MODIFY | ACCESS3_EXTEND;
    for (identity, expected_access) in [
        (auth(2000, 2000, &[]), writable),
        (auth(1000, 1000, &[]), writable),
        (auth(0, 0, &[]), writable),
        (auth(2001, 2001, &[]), 0),
    ] {
        let result = tokio::time::timeout(
            Duration::from_secs(2),
            adapter.access(&identity, file, ACCESS3_ALL),
        )
        .await
        .expect("ACCESS must not drain acknowledged writes")
        .unwrap();
        assert_eq!(result.access, expected_access);
        assert_eq!(encoded(&result.attributes), encoded(&visible));
    }
    let canonical = fs.inode_store.get(file).await.unwrap();
    let canonical: fattr3 = InodeWithId {
        inode: &canonical,
        id: file,
    }
    .into();
    assert_eq!(encoded(&canonical), encoded(&before));
    assert_eq!(fs.volatile_budget().unwrap().status().dirty_operations, 1);

    release.send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(3), fs.quiesce_overlay_inode(file))
        .await
        .unwrap()
        .unwrap();
    let after = adapter.access(&writer, file, ACCESS3_ALL).await.unwrap();
    assert_eq!(after.access, writable);
    assert_eq!(encoded(&after.attributes), encoded(&visible));
    assert_eq!(fs.volatile_budget().unwrap().status().dirty_operations, 0);
    assert_eq!(
        adapter.read(&writer, file, 0, 64).await.unwrap(),
        (b"seedtail".to_vec(), true)
    );
}

#[tokio::test]
async fn access_without_supported_rights_does_not_read_a_corrupt_acl() {
    let (fs, owner, file) = fixture().await;
    set_acl(
        &fs,
        &owner,
        file,
        PosixAcl::from_parts(6, &[(2000, 4)], 0, &[], 4, 0),
    )
    .await;
    let mut txn = fs.db.new_transaction().unwrap();
    fs.xattr_store.put(
        &mut txn,
        file,
        POSIX_ACL_ACCESS,
        bytes::Bytes::from_static(b"corrupt"),
    );
    fs.write_coordinator.commit(txn).await.unwrap();
    let adapter = NFSAdapter::new(fs);
    let named = auth(2000, 2000, &[]);
    for requested in [0, !ACCESS3_ALL] {
        let result = adapter.access(&named, file, requested).await.unwrap();
        assert_eq!(result.access, 0);
        assert_eq!(result.attributes.mode, 0o640);
    }
    assert!(matches!(
        adapter.access(&named, file, ACCESS3_READ).await,
        Err(nfsstat3::NFS3ERR_IO)
    ));
}
