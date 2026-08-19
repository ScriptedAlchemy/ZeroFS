use super::*;
use crate::fs::mutation::config::{
    ClientDurabilityTarget, FilesystemWriteAckMode, FilesystemWriteAckSettings,
    FilesystemWriteAckSource,
};
use crate::test_helpers::test_helpers_mod::filename;
use std::time::Duration;
use tokio::sync::Notify;
use zerofs_nfsserve::nfs::{nfsstat3, sattr3, stable_how};
use zerofs_nfsserve::vfs::{
    CommitRequestContext, NFSFileSystem, RpcRequestContext, WriteRequestContext,
};

const TEST_CLIENT: &str = "127.0.0.1:42000";

fn volatile_local_settings() -> FilesystemWriteAckSettings {
    FilesystemWriteAckSettings {
        mode: FilesystemWriteAckMode::VolatileMemory,
        volatile_memory_bytes: 2 * 1024 * 1024,
        volatile_max_operations: 64,
        source: FilesystemWriteAckSource::Filesystem,
        client_durability_target: ClientDurabilityTarget::LocalSsd,
    }
}

fn nfs_auth(uid: u32) -> NfsAuthContext {
    NfsAuthContext {
        uid,
        gid: 100,
        gids: vec![100, 20],
    }
}

fn write_context(
    xid: u32,
    connection_incarnation: u64,
    requested_stability: stable_how,
) -> WriteRequestContext {
    WriteRequestContext {
        rpc: RpcRequestContext {
            xid,
            client_addr: TEST_CLIENT.to_string(),
            connection_incarnation,
        },
        requested_stability,
    }
}

fn commit_context(xid: u32, connection_incarnation: u64) -> CommitRequestContext {
    CommitRequestContext {
        rpc: RpcRequestContext {
            xid,
            client_addr: TEST_CLIENT.to_string(),
            connection_incarnation,
        },
    }
}

async fn volatile_adapter(name: &[u8]) -> (Arc<ZeroFS>, NFSAdapter, fileid3) {
    let mut fs = ZeroFS::new_in_memory().await.expect("in-memory filesystem");
    fs.write_ack = volatile_local_settings();
    let fs = Arc::new(fs);
    let adapter = NFSAdapter::new(Arc::clone(&fs));
    let file = adapter
        .create(&nfs_auth(1000), 0, &filename(name), sattr3::default())
        .await
        .expect("create NFS test file")
        .0;
    (fs, adapter, file)
}

fn blocking_local_barrier(fs: &ZeroFS) -> (Arc<Notify>, Arc<Notify>) {
    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    fs.flush_coordinator.set_local_durability_barrier({
        let entered = Arc::clone(&entered);
        let release = Arc::clone(&release);
        Arc::new(move || {
            let entered = Arc::clone(&entered);
            let release = Arc::clone(&release);
            Box::pin(async move {
                entered.notify_one();
                release.notified().await;
                Ok(())
            })
        })
    });
    (entered, release)
}

async fn assert_stable_write_waits(requested_stability: stable_how, xid: u32) {
    let (fs, adapter, file) = volatile_adapter(b"stable-write").await;
    let (entered, release) = blocking_local_barrier(&fs);
    let mut write = tokio::spawn(async move {
        adapter
            .write_with_context(
                &write_context(xid, 9, requested_stability),
                &nfs_auth(1000),
                file,
                0,
                b"stable payload",
            )
            .await
    });

    tokio::time::timeout(Duration::from_secs(2), entered.notified())
        .await
        .expect("stable NFS WRITE did not reach configured durability");
    assert!(
        tokio::time::timeout(Duration::from_millis(25), &mut write)
            .await
            .is_err(),
        "stable NFS WRITE returned before configured durability completed",
    );
    release.notify_one();
    let result = tokio::time::timeout(Duration::from_secs(2), write)
        .await
        .expect("stable NFS WRITE did not resume")
        .expect("stable NFS WRITE task panicked")
        .expect("stable NFS WRITE failed");
    assert_eq!(result.committed, stable_how::FILE_SYNC);
}

#[tokio::test]
async fn unstable_write_returns_without_explicit_barrier() {
    let (fs, adapter, file) = volatile_adapter(b"unstable-write").await;
    let (entered, _release) = blocking_local_barrier(&fs);

    let result = tokio::time::timeout(
        Duration::from_secs(2),
        adapter.write_with_context(
            &write_context(100, 7, stable_how::UNSTABLE),
            &nfs_auth(1000),
            file,
            0,
            b"volatile payload",
        ),
    )
    .await
    .expect("UNSTABLE NFS WRITE waited for durability")
    .expect("UNSTABLE NFS WRITE failed");

    assert_eq!(result.committed, stable_how::UNSTABLE);
    assert!(
        tokio::time::timeout(Duration::from_millis(25), entered.notified())
            .await
            .is_err(),
        "UNSTABLE NFS WRITE entered an explicit durability barrier",
    );
}

#[tokio::test]
async fn data_sync_waits_configured_target_and_reports_achieved_level() {
    assert_stable_write_waits(stable_how::DATA_SYNC, 101).await;
}

#[tokio::test]
async fn file_sync_waits_configured_target() {
    assert_stable_write_waits(stable_how::FILE_SYNC, 102).await;
}

#[tokio::test]
async fn replay_uses_connection_xid_identity() {
    let (fs, adapter, file) = volatile_adapter(b"nfs-replay").await;
    let context = write_context(200, 31, stable_how::UNSTABLE);

    let first = adapter
        .write_with_context(&context, &nfs_auth(1000), file, 0, b"first")
        .await
        .expect("first identified write");
    let overlay = fs.volatile_overlay.get().expect("volatile overlay");
    let accepted_after_first = overlay.accepted_batch_count();

    let replay = adapter
        .write_with_context(&context, &nfs_auth(1000), file, 0, b"first")
        .await
        .expect("retained NFS replay");
    assert_eq!(replay.attributes.size, first.attributes.size);
    assert_eq!(
        overlay.accepted_batch_count(),
        accepted_after_first,
        "retained NFS replay published a second mutation",
    );
}

#[tokio::test]
async fn credential_or_stability_fingerprint_mismatch_is_rejected() {
    let (_fs, adapter, file) = volatile_adapter(b"nfs-collision").await;
    let context = write_context(201, 32, stable_how::UNSTABLE);
    adapter
        .write_with_context(&context, &nfs_auth(1000), file, 0, b"first")
        .await
        .expect("first identified write");

    let stability_collision = adapter
        .write_with_context(
            &write_context(201, 32, stable_how::DATA_SYNC),
            &nfs_auth(1000),
            file,
            0,
            b"first",
        )
        .await;
    assert!(matches!(stability_collision, Err(nfsstat3::NFS3ERR_INVAL)));

    let credential_collision = adapter
        .write_with_context(&context, &nfs_auth(2000), file, 0, b"first")
        .await;
    assert!(matches!(credential_collision, Err(nfsstat3::NFS3ERR_INVAL)));
}

#[tokio::test]
async fn reconnect_address_reuse_does_not_join_old_xid() {
    let (fs, adapter, file) = volatile_adapter(b"nfs-reconnect").await;
    adapter
        .write_with_context(
            &write_context(300, 41, stable_how::UNSTABLE),
            &nfs_auth(1000),
            file,
            0,
            b"old",
        )
        .await
        .expect("old connection write");
    let overlay = fs.volatile_overlay.get().expect("volatile overlay");
    let accepted_after_old = overlay.accepted_batch_count();

    adapter
        .write_with_context(
            &write_context(300, 42, stable_how::UNSTABLE),
            &nfs_auth(1000),
            file,
            0,
            b"new",
        )
        .await
        .expect("reconnected write");
    assert_eq!(
        overlay.accepted_batch_count(),
        accepted_after_old + 1,
        "a fresh transport incarnation joined the old XID",
    );
    assert_eq!(
        fs.mutation_coordinator
            .get()
            .expect("mutation coordinator")
            .request_cache()
            .len(),
        2,
        "both transport incarnations must retain independent replay entries",
    );
}

#[tokio::test]
async fn commit_covers_prior_cross_adapter_cutoff() {
    let (fs, adapter, commit_file) = volatile_adapter(b"commit-target").await;
    let auth = AuthContext {
        uid: 1000,
        gid: 100,
        gid_known: true,
        gids: vec![100, 20],
        groups_complete: true,
    };
    let direct_file = fs
        .create_exclusive(&auth, 0, b"direct-writer")
        .await
        .expect("create direct writer file");
    fs.write_ack(
        &auth,
        direct_file,
        0,
        &bytes::Bytes::from_static(b"cross-adapter"),
    )
    .await
    .expect("direct shared write");
    let expected_cutoff = fs.capture_mutation_cutoff();
    assert!(expected_cutoff.sequence > 0);
    let (entered, release) = blocking_local_barrier(&fs);

    let mut commit = tokio::spawn(async move {
        adapter
            .commit_with_context(&commit_context(301, 43), &nfs_auth(1000), commit_file, 0, 0)
            .await
    });
    tokio::time::timeout(Duration::from_secs(2), entered.notified())
        .await
        .expect("NFS COMMIT did not enter configured durability");
    assert!(
        tokio::time::timeout(Duration::from_millis(25), &mut commit)
            .await
            .is_err(),
        "NFS COMMIT returned before configured durability completed",
    );
    release.notify_one();
    let result = tokio::time::timeout(Duration::from_secs(2), commit)
        .await
        .expect("NFS COMMIT did not resume")
        .expect("NFS COMMIT task panicked")
        .expect("NFS COMMIT failed");
    assert_ne!(result.verifier, [0; 8]);
    let materialized = fs
        .materializer
        .get()
        .expect("materializer")
        .progress()
        .materialized_through();
    assert!(materialized >= expected_cutoff.sequence);
}

#[tokio::test]
async fn write_and_commit_share_service_verifier() {
    let (_fs, adapter, file) = volatile_adapter(b"shared-verifier").await;
    let write = adapter
        .write_with_context(
            &write_context(400, 51, stable_how::UNSTABLE),
            &nfs_auth(1000),
            file,
            0,
            b"verifier",
        )
        .await
        .expect("NFS write");
    let commit = adapter
        .commit_with_context(&commit_context(401, 51), &nfs_auth(1000), file, 0, 0)
        .await
        .expect("NFS commit");

    assert_ne!(write.verifier, [0; 8]);
    assert_eq!(write.verifier, commit.verifier);
    assert_eq!(write.verifier, adapter.get_write_verf());
}

#[tokio::test]
async fn service_restart_changes_nonzero_verifier() {
    let fs = Arc::new(ZeroFS::new_in_memory().await.expect("in-memory filesystem"));
    let first = NFSAdapter::new(Arc::clone(&fs)).get_write_verf();
    let restarted = NFSAdapter::new(fs).get_write_verf();

    assert_ne!(first, [0; 8]);
    assert_ne!(restarted, [0; 8]);
    assert_ne!(first, restarted);
}
