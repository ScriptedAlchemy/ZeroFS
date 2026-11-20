use super::ZeroFS;
use super::errors::FsError;
use super::key_codec::KeyCodec;
use super::name_mode::NameMode;
use super::store::directory::{DirScanValue, decode_dir_scan_value};
use super::test_util::{test_auth, test_creds};
use super::types::{FileType, SetAttributes, SetMode};
use bytes::Bytes;
use futures::TryStreamExt;

pub(super) async fn assert_preserved(fs: &ZeroFS, directory: u64, name: &[u8], id: u64) -> u64 {
    let entries: Vec<_> = fs
        .directory_store
        .list(directory)
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    let entry = entries.iter().find(|entry| entry.name == name).unwrap();
    assert_eq!(entry.inode_id, id);
    let inode = fs.inode_store.get(id).await.unwrap();
    assert_eq!(inode.name(), Some(name));
    assert_eq!(
        bincode::serialize(entry.inode.as_ref().unwrap()).unwrap(),
        bincode::serialize(&inode).unwrap()
    );
    let codec = KeyCodec::with_name_mode(fs.name_mode());
    let raw = fs
        .db
        .get_bytes(&codec.dir_scan_key(directory, entry.cookie))
        .await
        .unwrap()
        .unwrap();
    let (preserved, embedded) = decode_dir_scan_value(&raw).unwrap();
    assert_eq!(preserved, name);
    let DirScanValue::WithInode {
        inode: embedded, ..
    } = embedded
    else {
        panic!("expected an embedded inode")
    };
    assert_eq!(embedded.name(), Some(name));
    entry.cookie
}

#[tokio::test]
async fn insensitive_collisions_lookup_listing_and_backpointer_updates() {
    let fs = ZeroFS::new_in_memory_with_name_mode(NameMode::Insensitive)
        .await
        .unwrap();
    let attrs = SetAttributes::default();
    let creds = test_creds();
    let auth = test_auth();
    let (id, _) = fs.create(&creds, 0, b"Foo", &attrs).await.unwrap();
    assert_eq!(
        fs.create(&creds, 0, b"foo", &attrs).await.unwrap_err(),
        FsError::Exists
    );
    assert_eq!(
        fs.mkdir(&creds, 0, b"foo", &attrs).await.unwrap_err(),
        FsError::Exists
    );
    assert_eq!(
        fs.symlink(&creds, 0, b"foo", b"target", &attrs)
            .await
            .unwrap_err(),
        FsError::Exists
    );
    assert_eq!(
        fs.mknod(&creds, 0, b"foo", FileType::Fifo, &attrs, None)
            .await
            .unwrap_err(),
        FsError::Exists
    );
    assert_eq!(fs.link(&auth, id, 0, b"foo").await, Err(FsError::Exists));
    assert_eq!(fs.lookup(&creds, 0, b"FOO").await.unwrap(), id);
    assert_preserved(&fs, 0, b"Foo", id).await;
    fs.write(&auth, id, 0, &Bytes::from_static(b"payload"))
        .await
        .unwrap();
    fs.setattr(
        &creds,
        id,
        &SetAttributes {
            mode: SetMode::Set(0o600),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    assert_preserved(&fs, 0, b"Foo", id).await;
    fs.remove(&auth, 0, b"fOO").await.unwrap();
    assert_eq!(fs.lookup(&creds, 0, b"FOO").await, Err(FsError::NotFound));
    let (replacement, _) = fs.create(&creds, 0, b"foo", &attrs).await.unwrap();
    assert_ne!(replacement, id);
    assert_eq!(fs.lookup(&creds, 0, b"Foo").await.unwrap(), replacement);
}

#[tokio::test]
async fn sensitive_names_coexist_and_insensitive_creates_serialize() {
    let creds = test_creds();
    let attrs = SetAttributes::default();
    let fs = ZeroFS::new_in_memory().await.unwrap();
    let (a, _) = fs.create(&creds, 0, b"Foo", &attrs).await.unwrap();
    let (b, _) = fs.create(&creds, 0, b"foo", &attrs).await.unwrap();
    assert_ne!(a, b);
    let fs = ZeroFS::new_in_memory_with_name_mode(NameMode::Insensitive)
        .await
        .unwrap();
    let (a, b) = tokio::join!(
        fs.create(&creds, 0, b"Foo", &attrs),
        fs.create(&creds, 0, b"foo", &attrs)
    );
    assert_ne!(a.is_ok(), b.is_ok());
    assert_eq!(a.err().or(b.err()), Some(FsError::Exists));
    assert_eq!(
        fs.directory_store
            .list(0)
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap()
            .len(),
        1
    );
}

#[tokio::test]
async fn insensitive_opaque_names_and_normalization_forms_round_trip() {
    let fs = ZeroFS::new_in_memory_with_name_mode(NameMode::Insensitive)
        .await
        .unwrap();
    let creds = test_creds();
    let attrs = SetAttributes::default();
    for name in [
        "é".as_bytes(),
        "e\u{301}".as_bytes(),
        b"a\xffb",
        "ß".as_bytes(),
        b"ss",
    ] {
        let (id, _) = fs.create(&creds, 0, name, &attrs).await.unwrap();
        assert_preserved(&fs, 0, name, id).await;
    }
    assert_eq!(
        fs.lookup(&creds, 0, b"A\xffB").await.unwrap(),
        fs.lookup(&creds, 0, b"a\xffb").await.unwrap()
    );
    assert_eq!(
        fs.create(&creds, 0, b"A\xffB", &attrs).await.unwrap_err(),
        FsError::Exists
    );
    let (dir, _) = fs.mkdir(&creds, 0, b"Parent", &attrs).await.unwrap();
    fs.create(&creds, dir, b"child", &attrs).await.unwrap();
    assert_preserved(&fs, 0, b"Parent", dir).await;
}

#[tokio::test]
async fn recase_updates_every_copy_without_changing_identity_or_counts() {
    use super::inode::InodeAttrs;
    let fs = ZeroFS::new_in_memory_with_name_mode(NameMode::Insensitive)
        .await
        .unwrap();
    let creds = test_creds();
    let auth = test_auth();
    let attrs = SetAttributes::default();
    let (parent, _) = fs.mkdir(&creds, 0, b"Parent", &attrs).await.unwrap();
    for kind in [
        FileType::Regular,
        FileType::Directory,
        FileType::Symlink,
        FileType::Fifo,
        FileType::Socket,
        FileType::CharDevice,
        FileType::BlockDevice,
    ] {
        let (id, _) = match kind {
            FileType::Regular => fs.create(&creds, parent, b"Foo", &attrs).await.unwrap(),
            FileType::Directory => fs.mkdir(&creds, parent, b"Foo", &attrs).await.unwrap(),
            FileType::Symlink => fs
                .symlink(&creds, parent, b"Foo", b"target", &attrs)
                .await
                .unwrap(),
            _ => fs
                .mknod(&creds, parent, b"Foo", kind, &attrs, Some((1, 3)))
                .await
                .unwrap(),
        };
        let before = fs.inode_store.get(id).await.unwrap();
        let parent_before = fs.inode_store.get(parent).await.unwrap();
        let totals = fs.global_stats.get_totals();
        let cookie = assert_preserved(&fs, parent, b"Foo", id).await;
        fs.rename(&auth, parent, b"FOO", parent, b"foo")
            .await
            .unwrap();
        assert_eq!(assert_preserved(&fs, parent, b"foo", id).await, cookie);
        let after = fs.inode_store.get(id).await.unwrap();
        assert_eq!(after.nlink(), before.nlink());
        assert!((after.ctime(), after.ctime_nsec()) > (before.ctime(), before.ctime_nsec()));
        assert_eq!(
            fs.inode_store.get(parent).await.unwrap().nlink(),
            parent_before.nlink()
        );
        assert_eq!(fs.global_stats.get_totals(), totals);
        assert_preserved(&fs, 0, b"Parent", parent).await;
        let exact = bincode::serialize(&after).unwrap();
        fs.rename(&auth, parent, b"foo", parent, b"foo")
            .await
            .unwrap();
        assert_eq!(
            bincode::serialize(&fs.inode_store.get(id).await.unwrap()).unwrap(),
            exact
        );
        fs.setattr(
            &creds,
            id,
            &SetAttributes {
                mode: SetMode::Set(0o600),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        if kind == FileType::Regular {
            fs.write(&auth, id, 0, &Bytes::from_static(b"after recase"))
                .await
                .unwrap();
        }
        assert_preserved(&fs, parent, b"foo", id).await;
        fs.remove(&auth, parent, b"FOO").await.unwrap();
    }
}

#[tokio::test]
async fn rename_replaces_a_distinct_entry_but_keeps_hardlink_aliases() {
    use super::inode::InodeAttrs;
    let fs = ZeroFS::new_in_memory_with_name_mode(NameMode::Insensitive)
        .await
        .unwrap();
    let creds = test_creds();
    let auth = test_auth();
    let attrs = SetAttributes::default();
    let (a, _) = fs.create(&creds, 0, b"a", &attrs).await.unwrap();
    let (b, _) = fs.create(&creds, 0, b"b", &attrs).await.unwrap();
    fs.lookup(&creds, 0, b"B").await.unwrap();
    fs.rename(&auth, 0, b"a", 0, b"B").await.unwrap();
    assert_eq!(fs.lookup(&creds, 0, b"b").await.unwrap(), a);
    assert!(fs.inode_store.get(b).await.is_err());
    assert_preserved(&fs, 0, b"B", a).await;
    let (x, _) = fs.create(&creds, 0, b"x", &attrs).await.unwrap();
    fs.link(&auth, x, 0, b"y").await.unwrap();
    fs.rename(&auth, 0, b"x", 0, b"Y").await.unwrap();
    assert_eq!(fs.lookup(&creds, 0, b"x").await.unwrap(), x);
    assert_eq!(fs.lookup(&creds, 0, b"y").await.unwrap(), x);
    assert_eq!(fs.inode_store.get(x).await.unwrap().nlink(), 2);
    fs.rename(&auth, 0, b"x", 0, b"X").await.unwrap();
    let entries: Vec<_> = fs
        .directory_store
        .list(0)
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    assert!(
        entries
            .iter()
            .any(|entry| entry.name == b"X" && entry.inode.is_none())
    );
    assert!(entries.iter().any(|entry| entry.name == b"y"));
    assert_eq!(fs.inode_store.get(x).await.unwrap().name(), None);
    fs.remove(&auth, 0, b"y").await.unwrap();
    fs.rename(&auth, 0, b"X", 0, b"x").await.unwrap();
    assert_preserved(&fs, 0, b"x", x).await;
}
