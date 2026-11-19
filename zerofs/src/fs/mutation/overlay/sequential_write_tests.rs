use super::*;
use crate::fs::EXTENT_SIZE;
use crate::fs::mutation::config::{
    ClientDurabilityTarget, FilesystemWriteAckSettings, FilesystemWriteAckSource,
};
use crate::fs::test_util::test_creds;
use crate::fs::types::{InodeWithId, SetAttributes, SetMode, SetSize, SetTime, Timestamp};
use std::time::Duration;

async fn filesystem() -> (Arc<ZeroFS>, AuthContext) {
    let mut fs = ZeroFS::new_in_memory().await.unwrap();
    fs.write_ack = FilesystemWriteAckSettings {
        mode: FilesystemWriteAckMode::VolatileMemory,
        volatile_memory_bytes: 8 * 1024 * 1024,
        volatile_max_operations: 1024,
        source: FilesystemWriteAckSource::Filesystem,
        client_durability_target: ClientDurabilityTarget::LocalSsd,
    };
    let fs = Arc::new(fs);
    fs.install_volatile_overlay();
    (fs, AuthContext::from(&test_creds()))
}

fn identified<'a>(
    auth: &'a AuthContext,
    id: InodeId,
    offset: u64,
    data: &'a Bytes,
    operation: u8,
) -> IdentifiedWrite<'a> {
    IdentifiedWrite {
        auth,
        id,
        offset,
        data,
        op_id: [operation; 16],
        check_permissions: true,
        identity: RequestIdentity::NineP {
            origin_epoch: 1,
            operation_id: [operation; 16],
        },
        request_lifetime: RequestLifetime::CanonicalDedup,
        fingerprint_context: b"sequential-overwrite",
    }
}

#[tokio::test]
async fn sequential_overwrites_ack_before_apply_preserve_bytes_metadata_and_replay() {
    let (fs, owner) = filesystem().await;
    let file = fs.create_exclusive(&owner, 0, b"existing").await.unwrap();
    let original: Vec<u8> = (0..EXTENT_SIZE + 13).map(|i| (i % 251) as u8).collect();
    fs.write(&owner, file, 0, &Bytes::copy_from_slice(&original))
        .await
        .unwrap();
    let before = fs
        .setattr(
            &test_creds(),
            file,
            &SetAttributes {
                mode: SetMode::Set(0o6670),
                mtime: SetTime::SetToClientTime(Timestamp {
                    seconds: 3,
                    nanoseconds: 4,
                }),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let writer = AuthContext {
        uid: owner.uid + 1,
        ..owner.clone()
    };
    let initial_cutoff = fs.capture_mutation_cutoff();
    let release = fs
        .materializer
        .get()
        .unwrap()
        .hold_inode_lane_for_test(file)
        .await;
    let start = (EXTENT_SIZE - 3) as u64;
    let payloads = [
        Bytes::from_static(b"AAAAA"),
        Bytes::from_static(b"BBBBBBB"),
        Bytes::from_static(b"CCCCCCCC"),
    ];
    let mut offset = start;
    let mut expected = original.clone();
    let mut receipts = Vec::new();
    for (index, data) in payloads.iter().enumerate() {
        let request = || identified(&writer, file, offset, data, 0x70 + index as u8);
        let receipt =
            tokio::time::timeout(Duration::from_secs(2), fs.write_ack_identified(request()))
                .await
                .expect("sequential overwrite waited for held materialization")
                .unwrap();
        let end = offset as usize + data.len();
        expected.resize(expected.len().max(end), 0);
        expected[offset as usize..end].copy_from_slice(data);
        assert_eq!(receipt.attrs.size, expected.len() as u64);
        assert_eq!(receipt.attrs.mode & 0o6000, 0);
        assert_eq!(receipt.attrs.atime, before.atime);
        assert_ne!(receipt.attrs.mtime, before.mtime);
        // The two overwrites share a partial extent. Neither may discard its
        // predecessor or the old suffix, even though canonical apply is held.
        assert_eq!(
            fs.read_file(&owner, file, 0, (EXTENT_SIZE + 32) as u32)
                .await
                .unwrap(),
            (Bytes::copy_from_slice(&expected), true)
        );
        let replay = fs.write_ack_identified(request()).await.unwrap();
        assert_eq!(
            bincode::serialize(&replay.attrs).unwrap(),
            bincode::serialize(&receipt.attrs).unwrap()
        );
        assert_eq!(replay.cutoff, receipt.cutoff);
        assert_eq!(
            fs.volatile_budget().unwrap().status().dirty_operations,
            index + 1
        );
        receipts.push(receipt);
        offset = end as u64;
    }
    assert_eq!(fs.quota.committed_bytes(), original.len() as u64);
    assert_eq!(fs.quota.pending_bytes(), 4);
    let canonical = fs.inode_store.get(file).await.unwrap();
    let canonical: FileAttributes = InodeWithId {
        inode: &canonical,
        id: file,
    }
    .into();
    assert_eq!(
        bincode::serialize(&canonical).unwrap(),
        bincode::serialize(&before).unwrap()
    );

    // A shared guard never grants the next caller the previous caller's access.
    let denied = AuthContext {
        uid: owner.uid + 2,
        gid: owner.gid + 2,
        gids: vec![owner.gid + 2],
        ..owner.clone()
    };
    let denied_data = Bytes::from_static(b"!");
    let denied_result = tokio::time::timeout(
        Duration::from_secs(2),
        fs.write_ack_identified(identified(&denied, file, offset, &denied_data, 0x7f)),
    )
    .await
    .expect("permission validation waited for canonical apply");
    assert!(matches!(denied_result, Err(FsError::PermissionDenied)));
    let last = receipts.last().unwrap();
    assert_eq!(last.cutoff.sequence, initial_cutoff.sequence + 3);
    assert_eq!(fs.capture_mutation_cutoff(), last.cutoff);
    release.send(()).unwrap();
    tokio::time::timeout(
        Duration::from_secs(3),
        fs.materialize_through_cutoff(last.cutoff),
    )
    .await
    .unwrap()
    .unwrap();
    fs.quiesce_overlay_inode(file).await.unwrap();
    let canonical = fs.inode_store.get(file).await.unwrap();
    let canonical: FileAttributes = InodeWithId {
        inode: &canonical,
        id: file,
    }
    .into();
    assert_eq!(
        bincode::serialize(&canonical).unwrap(),
        bincode::serialize(&last.attrs).unwrap()
    );
    assert_eq!(fs.quota.committed_bytes(), expected.len() as u64);
    assert_eq!(fs.quota.pending_bytes(), 0);
    assert_eq!(
        fs.read_file(&owner, file, 0, (EXTENT_SIZE + 32) as u32)
            .await
            .unwrap(),
        (Bytes::copy_from_slice(&expected), true)
    );
    let replay = fs
        .write_ack_identified(identified(&writer, file, start, &payloads[0], 0x70))
        .await
        .unwrap();
    assert_eq!(
        bincode::serialize(&replay.attrs).unwrap(),
        bincode::serialize(&receipts[0].attrs).unwrap()
    );
    assert_eq!(fs.volatile_budget().unwrap().status().dirty_operations, 0);
    assert_eq!(fs.capture_mutation_cutoff(), last.cutoff);
}

#[tokio::test]
async fn noncontiguous_or_empty_overwrites_wait_and_cancel_without_retaining_a_hint() {
    for (offset, data) in [
        (5, Bytes::from_static(b"ZZ")), // gap
        (3, Bytes::from_static(b"ZZ")), // overlap
        (1, Bytes::from_static(b"ZZ")), // backwards
        (4, Bytes::new()),              // empty, even at the previous range end
    ] {
        let (fs, auth) = filesystem().await;
        let file = fs.create_exclusive(&auth, 0, b"fallback").await.unwrap();
        fs.write(&auth, file, 0, &Bytes::from_static(b"abcdefgh"))
            .await
            .unwrap();
        let release = fs
            .materializer
            .get()
            .unwrap()
            .hold_inode_lane_for_test(file)
            .await;
        fs.write_ack(&auth, file, 2, &Bytes::from_static(b"XY"))
            .await
            .unwrap();
        let cutoff = fs.capture_mutation_cutoff();
        let runtime = fs.volatile_overlay.get().unwrap().runtime(file).unwrap();
        let mut fallback = Box::pin(fs.write_ack(&auth, file, offset, &data));
        tokio::task::yield_now().await;
        assert!(futures::poll!(fallback.as_mut()).is_pending());
        assert!(runtime.append.try_lock().is_err());
        drop(fallback);
        assert!(runtime.append.try_lock().unwrap().is_none());
        // Cancelling the fallback wait cleared the hint; a later contiguous
        // request must also wait rather than reuse an abandoned projection.
        let contiguous_data = Bytes::from_static(b"!");
        let mut contiguous = Box::pin(fs.write_ack(&auth, file, 4, &contiguous_data));
        tokio::task::yield_now().await;
        assert!(futures::poll!(contiguous.as_mut()).is_pending());
        assert!(runtime.append.try_lock().is_err());
        drop(contiguous);
        assert!(runtime.append.try_lock().unwrap().is_none());
        drop(runtime);
        assert_eq!(fs.capture_mutation_cutoff(), cutoff);
        let status = fs.volatile_budget().unwrap().status();
        assert_eq!(status.dirty_bytes, 2);
        assert_eq!(status.dirty_operations, 1);
        assert_eq!(
            fs.read_file(&auth, file, 0, 16).await.unwrap().0.as_ref(),
            b"abXYefgh"
        );
        release.send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(3), fs.quiesce_overlay_inode(file))
            .await
            .unwrap()
            .unwrap();
        fs.write_ack(&auth, file, offset, &data).await.unwrap();
        fs.quiesce_overlay_inode(file).await.unwrap();
        let mut expected = b"abXYefgh".to_vec();
        expected[offset as usize..offset as usize + data.len()].copy_from_slice(&data);
        assert_eq!(
            fs.read_file(&auth, file, 0, 16).await.unwrap(),
            (Bytes::from(expected), true)
        );
    }
}

#[tokio::test]
async fn presized_sequential_writes_yield_to_a_canonical_waiter_after_256_operations() {
    let (fs, auth) = filesystem().await;
    let file = fs.create_exclusive(&auth, 0, b"presized").await.unwrap();
    fs.setattr(
        &test_creds(),
        file,
        &SetAttributes {
            size: SetSize::Set(512),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let release = fs
        .materializer
        .get()
        .unwrap()
        .hold_inode_lane_for_test(file)
        .await;
    let payload: Vec<u8> = (0..256).map(|i| (i % 251 + 1) as u8).collect();
    fs.write_ack(&auth, file, 0, &Bytes::copy_from_slice(&payload[..1]))
        .await
        .unwrap();
    let mut waiter = Box::pin(fs.lock_manager.acquire(file));
    tokio::task::yield_now().await;
    assert!(futures::poll!(waiter.as_mut()).is_pending());
    tokio::time::timeout(Duration::from_secs(2), async {
        for (offset, byte) in payload.iter().enumerate().skip(1) {
            let attrs = fs
                .write_ack(
                    &auth,
                    file,
                    offset as u64,
                    &Bytes::copy_from_slice(&[*byte]),
                )
                .await
                .unwrap();
            assert_eq!(attrs.size, 512);
        }
    })
    .await
    .expect("pre-sized sequential writes waited for materialization before the fairness cap");
    assert_eq!(fs.volatile_budget().unwrap().status().dirty_operations, 256);
    assert_eq!(fs.quota.committed_bytes(), 512);
    assert_eq!(fs.quota.pending_bytes(), 0);
    let extra = Bytes::from_static(b"!");
    let mut capped = Box::pin(fs.write_ack(&auth, file, 256, &extra));
    tokio::task::yield_now().await;
    assert!(futures::poll!(capped.as_mut()).is_pending());
    let mut expected = vec![0; 512];
    expected[..256].copy_from_slice(&payload);
    assert_eq!(
        fs.read_file(&auth, file, 0, 513).await.unwrap(),
        (Bytes::copy_from_slice(&expected), true)
    );
    release.send(()).unwrap();
    let canonical_guard = tokio::time::timeout(Duration::from_secs(3), waiter)
        .await
        .expect("continuation starved the canonical lock waiter");
    tokio::task::yield_now().await;
    assert!(futures::poll!(capped.as_mut()).is_pending());
    drop(canonical_guard);
    let attrs = tokio::time::timeout(Duration::from_secs(3), capped)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(attrs.size, 512);
    fs.quiesce_overlay_inode(file).await.unwrap();
    expected[256] = b'!';
    assert_eq!(
        fs.read_file(&auth, file, 0, 513).await.unwrap(),
        (Bytes::from(expected), true)
    );
    assert_eq!(fs.quota.committed_bytes(), 512);
    assert_eq!(fs.quota.pending_bytes(), 0);
    assert_eq!(fs.volatile_budget().unwrap().status().dirty_operations, 0);
}
