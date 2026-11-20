use super::config::{
    ClientDurabilityTarget, FilesystemWriteAckMode, FilesystemWriteAckSettings,
    FilesystemWriteAckSource,
};
use super::overlay::{IdentifiedWrite, WriteAdmission};
use super::types::{
    ConflictKey, ConflictScope, PrepareWriteMember, PrepareWriteRequest, RequestFingerprint,
    RequestIdentity, RequestLifetime,
};
use crate::fs::ZeroFS;
use crate::fs::errors::FsError;
use crate::fs::test_util::test_auth;
use bytes::Bytes;
use std::sync::Arc;
use std::time::Duration;

async fn volatile_fs() -> Arc<ZeroFS> {
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
    fs
}

#[tokio::test]
async fn survival_drain_waits_for_volatile_preparation_to_publish() {
    let fs = volatile_fs().await;
    let auth = test_auth();
    let file = fs.create_exclusive(&auth, 0, b"prepared").await.unwrap();
    let data = Bytes::from_static(b"admitted before the cut");
    let admission = fs
        .begin_write_admission(
            RequestIdentity::DirectOneShot(uuid::Uuid::new_v4()),
            RequestFingerprint::from_bytes([0; 32]),
            RequestLifetime::OneShot,
            ConflictScope::single(ConflictKey::Inode(file)),
            data.len() as u64,
        )
        .await
        .unwrap();
    let WriteAdmission::Prepared(guard) = admission else {
        panic!("a new request must enter preparation");
    };
    fs.survival_admission.close().unwrap();
    let drain = fs.survival_admission.drained();
    tokio::pin!(drain);
    assert!(
        futures::poll!(&mut drain).is_pending(),
        "the cut must wait until an admitted write has a published cutoff"
    );
    let before = fs.capture_mutation_cutoff();
    let receipt = fs
        .write_ack_batch_admitted(
            PrepareWriteRequest {
                members: vec![PrepareWriteMember {
                    id: file,
                    offset: 0,
                    data: data.clone(),
                }],
                auth: auth.clone(),
                op_id: [0; 16],
                check_permissions: true,
            },
            guard,
        )
        .await
        .unwrap();
    drain.await;
    assert!(receipt.cutoff.sequence > before.sequence);
    assert_eq!(fs.capture_mutation_cutoff(), receipt.cutoff);
    let durable = fs.administrative_remote_durability().await.unwrap();
    assert_eq!(durable.mutation_cutoff, receipt.cutoff);
    assert_eq!(fs.read_file(&auth, file, 0, 64).await.unwrap().0, data);
    fs.survival_admission.reopen();
}

#[tokio::test]
async fn cancelled_volatile_preparation_releases_survival_admission() {
    let fs = volatile_fs().await;
    let admission = fs
        .begin_write_admission(
            RequestIdentity::DirectOneShot(uuid::Uuid::new_v4()),
            RequestFingerprint::from_bytes([0; 32]),
            RequestLifetime::OneShot,
            ConflictScope::single(ConflictKey::Inode(1)),
            4,
        )
        .await
        .unwrap();
    fs.survival_admission.close().unwrap();
    let drain = fs.survival_admission.drained();
    tokio::pin!(drain);
    assert!(futures::poll!(&mut drain).is_pending());
    drop(admission);
    drain.await;
    assert_eq!(fs.capture_mutation_cutoff().sequence, 0);
    fs.survival_admission.reopen();
}

#[derive(Clone, Copy, Debug)]
enum MaterializedMutation {
    Direct,
    IdentifiedNfs,
    Striped,
    EmptyIdentified,
    AccessTime,
    NoopRename,
}

impl MaterializedMutation {
    async fn mutate(self, fs: &ZeroFS, files: [u64; 2]) -> Result<(), FsError> {
        let auth = test_auth();
        let data = Bytes::from_static(b"survives cancellation");
        match self {
            Self::Direct => fs.write(&auth, files[0], 0, &data).await.map(|_| ()),
            Self::IdentifiedNfs => fs
                .write_ack_identified(IdentifiedWrite {
                    auth: &auth,
                    id: files[0],
                    offset: 0,
                    data: &data,
                    op_id: [0; 16],
                    check_permissions: true,
                    identity: RequestIdentity::Nfs {
                        server_incarnation: uuid::Uuid::new_v4(),
                        connection_incarnation: 1,
                        xid: 1,
                    },
                    request_lifetime: RequestLifetime::ReplayWindow(Duration::from_secs(60)),
                    fingerprint_context: b"nfs3-write-v1",
                })
                .await
                .map(|_| ()),
            Self::Striped => fs
                .write_ack_batch(
                    &auth,
                    files
                        .iter()
                        .map(|&id| PrepareWriteMember {
                            id,
                            offset: 0,
                            data: data.clone(),
                        })
                        .collect(),
                )
                .await
                .map(|_| ()),
            Self::AccessTime => fs
                .record_access_time_idempotent(
                    files[0],
                    crate::fs::types::Timestamp {
                        seconds: u64::from(u32::MAX),
                        nanoseconds: 0,
                    },
                    [8; 16],
                )
                .await
                .map(|_| ()),
            Self::NoopRename => {
                fs.rename_idempotent(&auth, 0, b"first", 0, b"first", [9; 16])
                    .await
            }
            Self::EmptyIdentified => fs
                .write_idempotent(&auth, files[0], 0, &Bytes::new(), [7; 16])
                .await
                .map(|_| ()),
        }
    }
}

async fn materialized_fs() -> (Arc<ZeroFS>, [u64; 2]) {
    let fs = Arc::new(ZeroFS::new_in_memory().await.unwrap());
    let auth = test_auth();
    let first = fs.create_exclusive(&auth, 0, b"first").await.unwrap();
    let second = fs.create_exclusive(&auth, 0, b"second").await.unwrap();
    (fs, [first, second])
}

#[tokio::test]
async fn materialized_mutation_paths_observe_survival_latch() {
    for kind in [
        MaterializedMutation::Direct,
        MaterializedMutation::IdentifiedNfs,
        MaterializedMutation::Striped,
        MaterializedMutation::AccessTime,
        MaterializedMutation::NoopRename,
    ] {
        let (fs, files) = materialized_fs().await;
        for error in [FsError::IoError, FsError::ReadOnlyFilesystem] {
            fs.survival_admission.latch(error, "replica unavailable");
            assert_eq!(kind.mutate(&fs, files).await, Err(error), "{kind:?}");
        }
        for file in files {
            assert!(
                fs.read_file(&test_auth(), file, 0, 64)
                    .await
                    .unwrap()
                    .0
                    .is_empty()
            );
        }
    }
}

#[tokio::test]
async fn cancelled_materialized_mutation_retains_survival_admission_until_commit() {
    for kind in [
        MaterializedMutation::Direct,
        MaterializedMutation::IdentifiedNfs,
        MaterializedMutation::Striped,
        MaterializedMutation::EmptyIdentified,
        MaterializedMutation::AccessTime,
        MaterializedMutation::NoopRename,
    ] {
        let (fs, files) = materialized_fs().await;
        // Persist an allocation watermark so even a dedup-only outcome must
        // wait for the database write permit before it can be published.
        fs.inode_store.allocate();
        let block_apply = fs.db.flush_barrier().write_owned().await;
        let queued = fs.write_coordinator.probe_next_apply();
        let caller = tokio::spawn({
            let fs = Arc::clone(&fs);
            async move { kind.mutate(&fs, files).await }
        });
        tokio::time::timeout(Duration::from_secs(10), queued)
            .await
            .unwrap_or_else(|_| panic!("{kind:?} did not reach its queued commit"))
            .unwrap();
        fs.survival_admission.close().unwrap();
        caller.abort();
        assert!(caller.await.unwrap_err().is_cancelled());
        let drain = fs.survival_admission.drained();
        tokio::pin!(drain);
        assert!(
            futures::poll!(&mut drain).is_pending(),
            "{kind:?} released survival admission before its queued commit applied"
        );
        drop(block_apply);
        tokio::time::timeout(Duration::from_secs(2), drain)
            .await
            .expect("the completed commit must release survival admission");
        let expected = if matches!(
            kind,
            MaterializedMutation::EmptyIdentified
                | MaterializedMutation::AccessTime
                | MaterializedMutation::NoopRename
        ) {
            Bytes::new()
        } else {
            Bytes::from_static(b"survives cancellation")
        };
        assert_eq!(
            fs.read_file(&test_auth(), files[0], 0, 64).await.unwrap().0,
            expected
        );
        if matches!(kind, MaterializedMutation::AccessTime) {
            use crate::fs::inode::InodeAttrs;
            assert_eq!(
                fs.inode_store.get(files[0]).await.unwrap().atime(),
                u64::from(u32::MAX)
            );
        }
        fs.survival_admission.reopen();
    }
}
