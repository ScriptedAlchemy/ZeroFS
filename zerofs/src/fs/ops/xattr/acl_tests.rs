//! Engine tests for POSIX ACL storage, enforcement and inheritance, and for
//! NFSv4 ACL translation.

use super::*;
use crate::fs::permissions::acl::PosixAcl;
use crate::fs::permissions::nfs4::encode::{self, Who};
use crate::fs::test_util::test_creds;
use crate::fs::types::{AuthContext, SetAttributes, SetMode};
use std::sync::Arc;

const OWNER: u32 = 1000;
const NAMED: u32 = 2000;
const STRANGER: u32 = 3000;
const TEAM: u32 = 300;

fn who(uid: u32, groups: &[u32]) -> Credentials {
    Credentials::from_auth_context(&auth(uid, groups))
}

fn auth(uid: u32, groups: &[u32]) -> AuthContext {
    AuthContext {
        uid,
        gid: groups.first().copied().unwrap_or(65000 + uid),
        gid_known: true,
        gids: groups.to_vec(),
        groups_complete: true,
        groups_truncated: false,
    }
}

fn acl_name(n: &[u8]) -> XattrName {
    XattrName::new(n).unwrap()
}

fn acl_value(text: &str) -> XattrValue {
    XattrValue::new(PosixAcl::from_text(text).to_bytes()).unwrap()
}

async fn get_text(fs: &ZeroFS, id: InodeId, n: &[u8]) -> Result<String, FsError> {
    let raw = fs.get_xattr(&test_creds(), id, &acl_name(n)).await?;
    Ok(PosixAcl::parse(&raw).unwrap().unwrap().to_text())
}

async fn set_text(fs: &ZeroFS, id: InodeId, n: &[u8], text: &str) -> Result<(), FsError> {
    fs.set_xattr(
        &test_creds(),
        id,
        &acl_name(n),
        acl_value(text),
        XattrSetMode::Either,
    )
    .await
}

async fn mode_of(fs: &ZeroFS, id: InodeId) -> u32 {
    fs.inode_store.get(id).await.unwrap().mode() & 0o7777
}

async fn mkdir(fs: &ZeroFS, parent: InodeId, name: &[u8], mode: u32) -> InodeId {
    fs.mkdir(
        &test_creds(),
        parent,
        name,
        &SetAttributes {
            mode: SetMode::Set(mode),
            ..Default::default()
        },
    )
    .await
    .unwrap()
    .0
}

async fn create(fs: &ZeroFS, parent: InodeId, name: &[u8], mode: u32) -> InodeId {
    fs.create(
        &test_creds(),
        parent,
        name,
        &SetAttributes {
            mode: SetMode::Set(mode),
            ..Default::default()
        },
    )
    .await
    .unwrap()
    .0
}

async fn read_ok(fs: &ZeroFS, uid: u32, groups: &[u32], id: InodeId) -> bool {
    match fs.read_file(&auth(uid, groups), id, 0, 16).await {
        Ok(_) => true,
        Err(FsError::PermissionDenied) => false,
        Err(other) => panic!("unexpected {other:?}"),
    }
}

async fn write_ok(fs: &ZeroFS, uid: u32, groups: &[u32], id: InodeId) -> bool {
    match fs
        .write(&auth(uid, groups), id, 0, &Bytes::from_static(b"x"))
        .await
    {
        Ok(_) => true,
        Err(FsError::PermissionDenied) => false,
        Err(other) => panic!("unexpected {other:?}"),
    }
}

async fn scratch(fs: &ZeroFS) -> InodeId {
    // An owner-controlled directory so creation does not depend on root.
    mkdir(fs, 0, b"scratch", 0o777).await
}

#[tokio::test]
async fn access_acl_round_trip_updates_mode_and_enforces_each_class() {
    let fs = ZeroFS::new_in_memory().await.unwrap();
    let dir = scratch(&fs).await;
    let id = create(&fs, dir, b"f", 0o600).await;
    assert!(!read_ok(&fs, NAMED, &[], id).await);
    set_text(
        &fs,
        id,
        POSIX_ACL_ACCESS,
        "u::rw-,u:2000:rw-,g::---,g:300:r--,m::rw-,o::---",
    )
    .await
    .unwrap();
    // The mode's group bits are now the mask.
    assert_eq!(mode_of(&fs, id).await, 0o660);
    assert_eq!(
        get_text(&fs, id, POSIX_ACL_ACCESS).await.unwrap(),
        "u::rw-,u:2000:rw-,g::---,g:300:r--,m::rw-,o::---"
    );
    // Owner, named user, named group, owning group and other.
    assert!(write_ok(&fs, OWNER, &[], id).await);
    assert!(read_ok(&fs, NAMED, &[], id).await);
    assert!(write_ok(&fs, NAMED, &[], id).await);
    assert!(read_ok(&fs, STRANGER, &[TEAM], id).await);
    assert!(!write_ok(&fs, STRANGER, &[TEAM], id).await);
    assert!(!read_ok(&fs, STRANGER, &[1000], id).await);
    assert!(!read_ok(&fs, STRANGER, &[], id).await);
    // Any caller may read the ACL; only the owner may change it.
    assert!(
        fs.get_xattr(&who(STRANGER, &[]), id, &acl_name(POSIX_ACL_ACCESS))
            .await
            .is_ok()
    );
    assert_eq!(
        fs.set_xattr(
            &who(NAMED, &[]),
            id,
            &acl_name(POSIX_ACL_ACCESS),
            acl_value("u::rwx,g::rwx,o::rwx"),
            XattrSetMode::Either,
        )
        .await,
        Err(FsError::OperationNotPermitted)
    );
    // listxattr shows the ACL.
    let list = fs.list_xattrs(&test_creds(), id).await.unwrap();
    assert_eq!(list.as_ref(), b"system.posix_acl_access\0");
}

#[tokio::test]
async fn chmod_rewrites_the_mask_and_the_acl_follows() {
    let fs = ZeroFS::new_in_memory().await.unwrap();
    let dir = scratch(&fs).await;
    let id = create(&fs, dir, b"f", 0o600).await;
    set_text(
        &fs,
        id,
        POSIX_ACL_ACCESS,
        "u::rw-,u:2000:rw-,g::r--,m::rw-,o::---",
    )
    .await
    .unwrap();
    assert!(write_ok(&fs, NAMED, &[], id).await);
    fs.setattr(
        &test_creds(),
        id,
        &SetAttributes {
            mode: SetMode::Set(0o640),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    assert_eq!(
        get_text(&fs, id, POSIX_ACL_ACCESS).await.unwrap(),
        "u::rw-,u:2000:rw-,g::r--,m::r--,o::---"
    );
    assert!(read_ok(&fs, NAMED, &[], id).await);
    assert!(!write_ok(&fs, NAMED, &[], id).await);
    // Widening the mode back restores the named user's write.
    fs.setattr(
        &test_creds(),
        id,
        &SetAttributes {
            mode: SetMode::Set(0o660),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    assert!(write_ok(&fs, NAMED, &[], id).await);
}

#[tokio::test]
async fn an_acl_equivalent_to_the_mode_is_stored_as_the_mode() {
    let fs = ZeroFS::new_in_memory().await.unwrap();
    let dir = scratch(&fs).await;
    let id = create(&fs, dir, b"f", 0o600).await;
    set_text(&fs, id, POSIX_ACL_ACCESS, "u::rwx,g::r-x,o::r--")
        .await
        .unwrap();
    assert_eq!(mode_of(&fs, id).await, 0o754);
    assert_eq!(
        get_text(&fs, id, POSIX_ACL_ACCESS).await,
        Err(FsError::NoData)
    );
    // An empty value removes an ACL, as `setfacl -b` does.
    set_text(
        &fs,
        id,
        POSIX_ACL_ACCESS,
        "u::rw-,u:2000:r--,g::---,m::r--,o::---",
    )
    .await
    .unwrap();
    fs.set_xattr(
        &test_creds(),
        id,
        &acl_name(POSIX_ACL_ACCESS),
        XattrValue::new(Bytes::new()).unwrap(),
        XattrSetMode::Either,
    )
    .await
    .unwrap();
    assert_eq!(
        get_text(&fs, id, POSIX_ACL_ACCESS).await,
        Err(FsError::NoData)
    );
    assert!(!read_ok(&fs, NAMED, &[], id).await);
    // Removing an absent ACL succeeds, as on Linux local filesystems.
    fs.remove_xattr(&test_creds(), id, &acl_name(POSIX_ACL_ACCESS))
        .await
        .unwrap();
}

#[tokio::test]
async fn default_acls_are_inherited_on_create_and_mkdir() {
    let fs = ZeroFS::new_in_memory().await.unwrap();
    let dir = scratch(&fs).await;
    let shared = mkdir(&fs, dir, b"shared", 0o750).await;
    set_text(
        &fs,
        shared,
        POSIX_ACL_DEFAULT,
        "u::rwx,u:2000:rwx,g::r-x,m::rwx,o::---",
    )
    .await
    .unwrap();
    assert_eq!(
        get_text(&fs, shared, POSIX_ACL_DEFAULT).await.unwrap(),
        "u::rwx,u:2000:rwx,g::r-x,m::rwx,o::---"
    );
    // A file created with 0666 inherits the default limited by that mode.
    let file = create(&fs, shared, b"f", 0o666).await;
    assert_eq!(mode_of(&fs, file).await, 0o660);
    assert_eq!(
        get_text(&fs, file, POSIX_ACL_ACCESS).await.unwrap(),
        "u::rw-,u:2000:rwx,g::r-x,m::rw-,o::---"
    );
    assert!(write_ok(&fs, NAMED, &[], file).await);
    assert_eq!(
        get_text(&fs, file, POSIX_ACL_DEFAULT).await,
        Err(FsError::NoData)
    );
    // A subdirectory inherits both the access ACL and the default.
    let sub = mkdir(&fs, shared, b"d", 0o777).await;
    assert_eq!(mode_of(&fs, sub).await, 0o770);
    assert_eq!(
        get_text(&fs, sub, POSIX_ACL_DEFAULT).await.unwrap(),
        "u::rwx,u:2000:rwx,g::r-x,m::rwx,o::---"
    );
    let nested = create(&fs, sub, b"g", 0o640).await;
    assert_eq!(
        get_text(&fs, nested, POSIX_ACL_ACCESS).await.unwrap(),
        "u::rw-,u:2000:rwx,g::r-x,m::r--,o::---"
    );
    assert!(read_ok(&fs, NAMED, &[], nested).await);
    assert!(!write_ok(&fs, NAMED, &[], nested).await);
    // A named user may create inside the directory through the default ACL.
    fs.create(
        &who(NAMED, &[]),
        sub,
        b"by-named",
        &SetAttributes::default(),
    )
    .await
    .unwrap();
    assert_eq!(
        fs.create(&who(STRANGER, &[]), sub, b"x", &SetAttributes::default())
            .await
            .unwrap_err(),
        FsError::PermissionDenied
    );
}

#[tokio::test]
async fn directory_acls_govern_lookup_and_listing() {
    let fs = ZeroFS::new_in_memory().await.unwrap();
    let dir = scratch(&fs).await;
    let private = mkdir(&fs, dir, b"private", 0o700).await;
    create(&fs, private, b"f", 0o644).await;
    let named = who(NAMED, &[]);
    assert_eq!(
        fs.lookup(&named, private, b"f").await,
        Err(FsError::PermissionDenied)
    );
    set_text(
        &fs,
        private,
        POSIX_ACL_ACCESS,
        "u::rwx,u:2000:--x,g::---,m::--x,o::---",
    )
    .await
    .unwrap();
    assert!(fs.lookup(&named, private, b"f").await.is_ok());
    assert_eq!(
        fs.readdir(&auth(NAMED, &[]), private, 0, 16).await.err(),
        Some(FsError::PermissionDenied)
    );
}

#[tokio::test]
async fn a_default_acl_is_refused_on_a_file() {
    let fs = ZeroFS::new_in_memory().await.unwrap();
    let dir = scratch(&fs).await;
    let id = create(&fs, dir, b"f", 0o600).await;
    assert_eq!(
        set_text(&fs, id, POSIX_ACL_DEFAULT, "u::rwx,g::r-x,o::---").await,
        Err(FsError::PermissionDenied)
    );
    fs.remove_xattr(&test_creds(), id, &acl_name(POSIX_ACL_DEFAULT))
        .await
        .unwrap();
}

#[tokio::test]
async fn invalid_acl_values_are_rejected_without_change() {
    let fs = ZeroFS::new_in_memory().await.unwrap();
    let dir = scratch(&fs).await;
    let id = create(&fs, dir, b"f", 0o600).await;
    let mut raw = PosixAcl::from_text("u::rw-,g::r--,o::---")
        .to_bytes()
        .to_vec();
    raw.truncate(raw.len() - 8);
    assert_eq!(
        fs.set_xattr(
            &test_creds(),
            id,
            &acl_name(POSIX_ACL_ACCESS),
            XattrValue::new(Bytes::from(raw)).unwrap(),
            XattrSetMode::Either,
        )
        .await,
        Err(FsError::InvalidArgument)
    );
    assert_eq!(mode_of(&fs, id).await, 0o600);
}

#[tokio::test]
async fn nfs4_acls_are_translated_or_refused() {
    let fs = ZeroFS::new_in_memory().await.unwrap();
    let dir = scratch(&fs).await;
    let share = mkdir(&fs, dir, b"share", 0o700).await;
    let restricted = encode::acl(&[
        (encode::ALLOW, encode::FD, encode::FULL_CONTROL, Who::Owner),
        (encode::ALLOW, encode::FD, encode::MODIFY, Who::Group),
        (
            encode::ALLOW,
            encode::FD,
            encode::MODIFY,
            Who::GroupId(TEAM),
        ),
    ]);
    fs.set_xattr(
        &test_creds(),
        share,
        &acl_name(NFS4_ACL_XDR),
        XattrValue::new(Bytes::from(restricted)).unwrap(),
        XattrSetMode::Either,
    )
    .await
    .unwrap();
    assert_eq!(
        get_text(&fs, share, POSIX_ACL_ACCESS).await.unwrap(),
        "u::rwx,g::rwx,g:300:rwx,m::rwx,o::---"
    );
    assert_eq!(
        get_text(&fs, share, POSIX_ACL_DEFAULT).await.unwrap(),
        "u::rwx,g::rwx,g:300:rwx,m::rwx,o::---"
    );
    assert_eq!(mode_of(&fs, share).await, 0o770);
    // The blob itself is not stored or served.
    assert_eq!(
        fs.get_xattr(&test_creds(), share, &acl_name(NFS4_ACL_XDR))
            .await,
        Err(FsError::NoData)
    );
    let file = create(&fs, share, b"f", 0o666).await;
    assert!(write_ok(&fs, STRANGER, &[TEAM], file).await);
    assert!(!read_ok(&fs, STRANGER, &[], file).await);

    // A refused ACL changes nothing.
    let before = (
        get_text(&fs, share, POSIX_ACL_ACCESS).await,
        mode_of(&fs, share).await,
    );
    let refused = encode::acl(&[(
        encode::ALLOW,
        encode::FILE_INHERIT,
        encode::FULL_CONTROL,
        Who::Owner,
    )]);
    assert_eq!(
        fs.set_xattr(
            &test_creds(),
            share,
            &acl_name(NFS4_ACL_XDR),
            XattrValue::new(Bytes::from(refused)).unwrap(),
            XattrSetMode::Either,
        )
        .await,
        Err(FsError::NotSupported)
    );
    assert_eq!(
        before,
        (
            get_text(&fs, share, POSIX_ACL_ACCESS).await,
            mode_of(&fs, share).await
        )
    );
    // Only the owner may translate an ACL onto an inode.
    assert_eq!(
        fs.set_xattr(
            &who(STRANGER, &[TEAM]),
            share,
            &acl_name(NFS4_ACL_XDR),
            XattrValue::new(Bytes::from(encode::acl(&[(
                encode::ALLOW,
                0,
                encode::FULL_CONTROL,
                Who::Everyone
            )])))
            .unwrap(),
            XattrSetMode::Either,
        )
        .await,
        Err(FsError::OperationNotPermitted)
    );
}

#[tokio::test]
async fn user_xattr_access_follows_the_acl() {
    let fs = ZeroFS::new_in_memory().await.unwrap();
    let dir = scratch(&fs).await;
    let id = create(&fs, dir, b"f", 0o600).await;
    let user_key = acl_name(b"user.k");
    fs.set_xattr(
        &test_creds(),
        id,
        &user_key,
        XattrValue::new(Bytes::from_static(b"v")).unwrap(),
        XattrSetMode::Either,
    )
    .await
    .unwrap();
    assert_eq!(
        fs.get_xattr(&who(NAMED, &[]), id, &user_key).await,
        Err(FsError::PermissionDenied)
    );
    set_text(
        &fs,
        id,
        POSIX_ACL_ACCESS,
        "u::rw-,u:2000:r--,g::---,m::r--,o::---",
    )
    .await
    .unwrap();
    assert_eq!(
        fs.get_xattr(&who(NAMED, &[]), id, &user_key)
            .await
            .unwrap()
            .as_ref(),
        b"v"
    );
    assert_eq!(
        fs.set_xattr(
            &who(NAMED, &[]),
            id,
            &user_key,
            XattrValue::new(Bytes::new()).unwrap(),
            XattrSetMode::Either,
        )
        .await,
        Err(FsError::PermissionDenied)
    );
}

#[tokio::test]
async fn volumes_without_acls_skip_lookups_until_the_first_acl() {
    let fs = ZeroFS::new_in_memory().await.unwrap();
    assert!(!fs.posix_acls_possible().await.unwrap());
    let dir = scratch(&fs).await;
    let id = create(&fs, dir, b"f", 0o600).await;
    // An equivalent ACL stores no row and does not mark the volume.
    set_text(&fs, id, POSIX_ACL_ACCESS, "u::rw-,g::---,o::---")
        .await
        .unwrap();
    assert!(!fs.posix_acls_possible().await.unwrap());
    set_text(
        &fs,
        id,
        POSIX_ACL_ACCESS,
        "u::rw-,u:2000:r--,g::---,m::r--,o::---",
    )
    .await
    .unwrap();
    assert!(fs.posix_acls_possible().await.unwrap());
    assert_eq!(
        fs.xattr_store
            .get(acl::VOLUME_MARKER_INODE, b"zerofs.posix_acl_in_use")
            .await
            .unwrap()
            .len(),
        0
    );
}

#[tokio::test]
async fn acls_persist_and_are_enforced_after_reopening() {
    use crate::block_transformer::ZeroFsBlockTransformer;
    use crate::config::CompressionConfig;
    use crate::db::SlateDbHandle;
    use slatedb::DbBuilder;
    use slatedb::object_store::path::Path;

    let store: Arc<dyn slatedb::object_store::ObjectStore> =
        Arc::new(slatedb::object_store::memory::InMemory::new());
    let key = [0u8; 32];
    let db = Arc::new(
        DbBuilder::new(Path::from("test_slatedb"), store.clone())
            .with_block_transformer(ZeroFsBlockTransformer::new_arc(
                &key,
                CompressionConfig::default(),
            ))
            .with_filter_policies(crate::fs::filter_policy::filter_policies())
            .with_segment_extractor(Arc::new(crate::segment_extractor::ZeroFsSegmentExtractor))
            .build()
            .await
            .unwrap(),
    );
    let fs = ZeroFS::new_with_slatedb(
        SlateDbHandle::ReadWrite(db),
        u64::MAX,
        None,
        false,
        store.clone(),
        crate::frame_codec::FrameCodec::new(
            &key,
            crate::segment::SEGMENT_INFO,
            CompressionConfig::default(),
        ),
    )
    .await
    .unwrap();
    let dir = scratch(&fs).await;
    let shared = mkdir(&fs, dir, b"shared", 0o750).await;
    set_text(
        &fs,
        shared,
        POSIX_ACL_DEFAULT,
        "u::rwx,u:2000:rwx,g::r-x,m::rwx,o::---",
    )
    .await
    .unwrap();
    let file = create(&fs, shared, b"f", 0o640).await;
    set_text(
        &fs,
        shared,
        POSIX_ACL_ACCESS,
        "u::rwx,u:2000:r-x,g::r-x,m::r-x,o::---",
    )
    .await
    .unwrap();
    fs.flush_coordinator.flush().await.unwrap();
    drop(fs);

    let reopened = ZeroFS::new_in_memory_read_only(store).await.unwrap();
    assert_eq!(
        get_text(&reopened, shared, POSIX_ACL_DEFAULT)
            .await
            .unwrap(),
        "u::rwx,u:2000:rwx,g::r-x,m::rwx,o::---"
    );
    assert_eq!(
        get_text(&reopened, file, POSIX_ACL_ACCESS).await.unwrap(),
        "u::rw-,u:2000:rwx,g::r-x,m::r--,o::---"
    );
    assert!(read_ok(&reopened, NAMED, &[], file).await);
    assert!(!read_ok(&reopened, STRANGER, &[], file).await);
    assert!(
        reopened
            .lookup(&who(NAMED, &[]), shared, b"f")
            .await
            .is_ok()
    );
}
