//! A mutation that holds a directory or inode lock across its commit must keep
//! that lock until the commit applies, even when its caller is cancelled after
//! the commit was queued. Protocol servers drop request futures at process
//! shutdown, NFS listeners are aborted after their drain grace, and GC tasks
//! are aborted before the final flush. Another actor that takes the same lock
//! in that gap would read state the queued commit is about to change.
//!
//! Each case holds every apply behind the database flush barrier, starts the
//! mutation, waits until its commit reaches the commit worker, cancels the
//! caller, and then checks the lock. Releasing the barrier lets the commit
//! apply; the lock must then be free and the change visible to whoever takes
//! it next.

use crate::fs::ZeroFS;
use crate::fs::inode::{Inode, InodeAttrs, InodeId};
use crate::fs::ops::{XattrName, XattrSetMode, XattrValue};
use crate::fs::test_util::{test_auth, test_creds};
use crate::fs::types::{FallocateMode, FileType, SetAttributes, SetMode, SetSize, Timestamp};
use bytes::Bytes;
use std::future::Future;
use std::sync::Arc;

/// Start `operation`, cancel it once its commit is queued, and return the
/// guard that keeps the commit from applying.
async fn cancel_after_queue<T: Send + 'static>(
    fs: &Arc<ZeroFS>,
    operation: impl Future<Output = T> + Send + 'static,
) -> tokio::sync::OwnedRwLockWriteGuard<()> {
    let block_apply = fs.db.flush_barrier().write_owned().await;
    let queued = fs.write_coordinator.probe_next_apply();
    let task = tokio::spawn(operation);
    queued.await.expect("the mutation must queue its commit");
    task.abort();
    match task.await {
        Err(error) => assert!(error.is_cancelled(), "the mutation task failed: {error}"),
        Ok(_) => panic!("the mutation finished although its commit cannot apply"),
    }
    block_apply
}

/// Assert `keys` stay locked while the commit is held, then let it apply and
/// wait until whoever holds the locks has released them.
async fn assert_locked_until_apply(
    fs: &Arc<ZeroFS>,
    operation: &str,
    keys: &[InodeId],
    block_apply: tokio::sync::OwnedRwLockWriteGuard<()>,
) {
    for &key in keys {
        assert!(
            fs.lock_manager.is_locked_for_test(key),
            "a cancelled {operation} released the lock on inode {key} before its queued commit applied"
        );
    }
    drop(block_apply);
    drop(fs.lock_manager.acquire_multi(keys.to_vec()).await);
}

async fn new_fs() -> Arc<ZeroFS> {
    Arc::new(ZeroFS::new_in_memory().await.unwrap())
}

async fn file_with_data(fs: &ZeroFS, name: &[u8], bytes: usize) -> InodeId {
    let (id, _) = fs
        .create(&test_creds(), 0, name, &SetAttributes::default())
        .await
        .unwrap();
    if bytes > 0 {
        fs.write(&test_auth(), id, 0, &Bytes::from(vec![7u8; bytes]))
            .await
            .unwrap();
    }
    id
}

fn file_size(inode: Inode) -> u64 {
    match inode {
        Inode::File(file) => file.size,
        other => panic!("expected a file, found {other:?}"),
    }
}

async fn entry_exists(fs: &ZeroFS, dir: InodeId, name: &[u8]) -> bool {
    fs.directory_store.exists(dir, name).await.unwrap()
}

#[tokio::test]
async fn cancelled_create_holds_the_directory_lock_until_apply() {
    let fs = new_fs().await;
    let creating = Arc::clone(&fs);
    let block_apply = cancel_after_queue(&fs, async move {
        creating
            .create(&test_creds(), 0, b"created", &SetAttributes::default())
            .await
    })
    .await;
    assert_locked_until_apply(&fs, "create", &[0], block_apply).await;
    assert!(entry_exists(&fs, 0, b"created").await);
}

#[tokio::test]
async fn cancelled_mkdir_holds_the_directory_lock_until_apply() {
    let fs = new_fs().await;
    let creating = Arc::clone(&fs);
    let block_apply = cancel_after_queue(&fs, async move {
        creating
            .mkdir(&test_creds(), 0, b"made", &SetAttributes::default())
            .await
    })
    .await;
    assert_locked_until_apply(&fs, "mkdir", &[0], block_apply).await;
    assert!(entry_exists(&fs, 0, b"made").await);
}

#[tokio::test]
async fn cancelled_mknod_holds_the_directory_lock_until_apply() {
    let fs = new_fs().await;
    let creating = Arc::clone(&fs);
    let block_apply = cancel_after_queue(&fs, async move {
        creating
            .mknod(
                &test_creds(),
                0,
                b"fifo",
                FileType::Fifo,
                &SetAttributes::default(),
                None,
            )
            .await
    })
    .await;
    assert_locked_until_apply(&fs, "mknod", &[0], block_apply).await;
    assert!(entry_exists(&fs, 0, b"fifo").await);
}

#[tokio::test]
async fn cancelled_symlink_holds_the_directory_lock_until_apply() {
    let fs = new_fs().await;
    let creating = Arc::clone(&fs);
    let block_apply = cancel_after_queue(&fs, async move {
        creating
            .symlink(
                &test_creds(),
                0,
                b"link",
                b"target",
                &SetAttributes::default(),
            )
            .await
    })
    .await;
    assert_locked_until_apply(&fs, "symlink", &[0], block_apply).await;
    assert!(entry_exists(&fs, 0, b"link").await);
}

#[tokio::test]
async fn cancelled_link_holds_both_locks_until_apply() {
    let fs = new_fs().await;
    let file = file_with_data(&fs, b"original", 0).await;
    let linking = Arc::clone(&fs);
    let block_apply = cancel_after_queue(&fs, async move {
        linking.link(&test_auth(), file, 0, b"second").await
    })
    .await;
    assert_locked_until_apply(&fs, "link", &[0, file], block_apply).await;
    assert!(entry_exists(&fs, 0, b"second").await);
}

#[tokio::test]
async fn cancelled_rename_holds_its_locks_until_apply() {
    let fs = new_fs().await;
    let file = file_with_data(&fs, b"before", 0).await;
    let renaming = Arc::clone(&fs);
    let block_apply = cancel_after_queue(&fs, async move {
        renaming
            .rename(&test_auth(), 0, b"before", 0, b"after")
            .await
    })
    .await;
    assert_locked_until_apply(&fs, "rename", &[0, file], block_apply).await;
    assert!(!entry_exists(&fs, 0, b"before").await);
    assert!(entry_exists(&fs, 0, b"after").await);
}

#[tokio::test]
async fn cancelled_unlink_holds_both_locks_until_apply() {
    let fs = new_fs().await;
    let file = file_with_data(&fs, b"doomed", 0).await;
    let removing = Arc::clone(&fs);
    let block_apply = cancel_after_queue(&fs, async move {
        removing.remove(&test_auth(), 0, b"doomed").await
    })
    .await;
    assert_locked_until_apply(&fs, "unlink", &[0, file], block_apply).await;
    assert!(!entry_exists(&fs, 0, b"doomed").await);
}

#[tokio::test]
async fn cancelled_setattr_holds_the_inode_lock_until_apply() {
    let fs = new_fs().await;
    let file = file_with_data(&fs, b"chmod", 0).await;
    let changing = Arc::clone(&fs);
    let block_apply = cancel_after_queue(&fs, async move {
        let attrs = SetAttributes {
            mode: SetMode::Set(0o600),
            ..SetAttributes::default()
        };
        changing.setattr(&test_creds(), file, &attrs).await
    })
    .await;
    assert_locked_until_apply(&fs, "setattr", &[file], block_apply).await;
    assert_eq!(
        fs.inode_store.get(file).await.unwrap().mode() & 0o777,
        0o600
    );
}

#[tokio::test]
async fn cancelled_truncate_holds_the_inode_lock_until_apply() {
    let fs = new_fs().await;
    let file = file_with_data(&fs, b"truncate", 4096).await;
    let truncating = Arc::clone(&fs);
    let block_apply = cancel_after_queue(&fs, async move {
        let attrs = SetAttributes {
            size: SetSize::Set(10),
            ..SetAttributes::default()
        };
        truncating.setattr(&test_creds(), file, &attrs).await
    })
    .await;
    assert_locked_until_apply(&fs, "truncate", &[file], block_apply).await;
    assert_eq!(file_size(fs.inode_store.get(file).await.unwrap()), 10);
}

#[tokio::test]
async fn cancelled_access_time_update_holds_the_inode_lock_until_apply() {
    let fs = new_fs().await;
    let file = file_with_data(&fs, b"atime", 0).await;
    let touching = Arc::clone(&fs);
    let atime = Timestamp {
        seconds: u32::MAX as u64 + 1,
        nanoseconds: 0,
    };
    let block_apply = cancel_after_queue(&fs, async move {
        touching
            .record_access_time_idempotent(file, atime, [0x51; 16])
            .await
    })
    .await;
    assert_locked_until_apply(&fs, "access-time update", &[file], block_apply).await;
    match fs.inode_store.get(file).await.unwrap() {
        Inode::File(inode) => assert_eq!(inode.atime, atime.seconds),
        other => panic!("expected a file, found {other:?}"),
    }
}

#[tokio::test]
async fn cancelled_fallocate_holds_the_inode_lock_until_apply() {
    let fs = new_fs().await;
    let file = file_with_data(&fs, b"fallocate", 0).await;
    let allocating = Arc::clone(&fs);
    let block_apply = cancel_after_queue(&fs, async move {
        allocating
            .fallocate_opened(&test_auth(), file, 0, 8192, FallocateMode::Allocate)
            .await
    })
    .await;
    assert_locked_until_apply(&fs, "fallocate", &[file], block_apply).await;
    assert_eq!(file_size(fs.inode_store.get(file).await.unwrap()), 8192);
}

#[tokio::test]
async fn cancelled_trim_holds_the_inode_lock_until_apply() {
    let fs = new_fs().await;
    let file = file_with_data(&fs, b"trim", 8192).await;
    let trimming = Arc::clone(&fs);
    let block_apply = cancel_after_queue(&fs, async move {
        trimming.trim(&test_auth(), file, 0, 4096).await
    })
    .await;
    assert_locked_until_apply(&fs, "trim", &[file], block_apply).await;
    let data = fs.read_file(&test_auth(), file, 0, 4096).await.unwrap().0;
    assert!(data.iter().all(|&byte| byte == 0));
}

#[tokio::test]
async fn cancelled_extent_delete_holds_the_inode_lock_until_apply() {
    let fs = new_fs().await;
    let file = file_with_data(&fs, b"tombstoned", 4096).await;
    let deleting = Arc::clone(&fs);
    let block_apply = cancel_after_queue(&fs, async move {
        deleting.extent_store.delete_extents(file, 0, 1).await
    })
    .await;
    assert_locked_until_apply(&fs, "extent delete", &[file], block_apply).await;
}

#[tokio::test]
async fn cancelled_xattr_set_holds_the_inode_lock_until_apply() {
    let fs = new_fs().await;
    let file = file_with_data(&fs, b"xattr", 0).await;
    let setting = Arc::clone(&fs);
    let block_apply = cancel_after_queue(&fs, async move {
        setting
            .set_xattr(
                &test_creds(),
                file,
                &XattrName::new(b"user.key").unwrap(),
                XattrValue::new(Bytes::from_static(b"value")).unwrap(),
                XattrSetMode::Create,
            )
            .await
    })
    .await;
    assert_locked_until_apply(&fs, "xattr set", &[file], block_apply).await;
    assert_eq!(
        fs.get_xattr(&test_creds(), file, &XattrName::new(b"user.key").unwrap())
            .await
            .unwrap(),
        b"value".as_slice()
    );
}
