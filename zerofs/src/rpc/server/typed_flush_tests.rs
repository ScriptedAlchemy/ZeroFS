use super::tests::{make_fs, make_fs_with_write_ack, setup_fs};
use super::*;
use crate::fs::mutation::config::{
    ClientDurabilityTarget, FilesystemWriteAckMode, FilesystemWriteAckSettings,
    FilesystemWriteAckSource,
};
use crate::fs::mutation::durability::{DurabilityError, DurabilityTarget};
use bytes::Bytes;
use std::time::Duration;
use tokio::sync::{Notify, mpsc};

fn volatile_write_ack_settings() -> FilesystemWriteAckSettings {
    FilesystemWriteAckSettings {
        mode: FilesystemWriteAckMode::VolatileMemory,
        volatile_memory_bytes: 8 * 1024 * 1024,
        volatile_max_operations: 1024,
        source: FilesystemWriteAckSource::Filesystem,
        client_durability_target: ClientDurabilityTarget::LocalSsd,
    }
}

#[tokio::test]
async fn unix_admin_flush_materializes_overlay_before_configured_target() {
    let (fs, checkpoint_manager) = make_fs_with_write_ack(volatile_write_ack_settings()).await;
    let (client, shutdown, _dir) = setup_fs(Arc::clone(&fs), checkpoint_manager, false).await;

    let auth = root_auth();
    let file = fs
        .create_exclusive(&auth, 0, b"rpc-flush-overlay")
        .await
        .unwrap();
    let payload = Bytes::from_static(b"rpc-flush-materialized");
    fs.write_ack(&auth, file, 0, &payload).await.unwrap();
    let payload_len = payload.len() as u64;
    let expected_cutoff = fs.capture_mutation_cutoff();
    assert!(
        expected_cutoff.sequence > 0,
        "the production write must publish a mutation cutoff"
    );

    let (entered_tx, mut entered_rx) = mpsc::unbounded_channel();
    let release = Arc::new(Notify::new());
    fs.flush_coordinator.set_object_wait({
        let fs = Arc::clone(&fs);
        let release = Arc::clone(&release);
        Arc::new(move |_coverage, target| {
            let fs = Arc::clone(&fs);
            let entered_tx = entered_tx.clone();
            let release = Arc::clone(&release);
            Box::pin(async move {
                let canonical = fs
                    .extent_store
                    .read(file, 0, payload_len)
                    .await
                    .map_err(DurabilityError::Materialization)?;
                entered_tx
                    .send((target, canonical))
                    .map_err(|_| DurabilityError::Closed)?;
                release.notified().await;
                Ok(())
            })
        })
    });

    let mut flush = tokio::spawn(async move { client.flush().await });
    let (target, canonical) = tokio::time::timeout(Duration::from_secs(2), entered_rx.recv())
        .await
        .expect("admin Flush never entered typed durability")
        .expect("typed durability observation channel closed");
    assert_eq!(target, DurabilityTarget::LocalSsd);
    assert_eq!(canonical.as_ref(), b"rpc-flush-materialized");
    let materialized = fs
        .materializer
        .get()
        .expect("volatile filesystem has a materializer")
        .progress()
        .materialized_through();
    assert!(
        materialized >= expected_cutoff.sequence,
        "typed durability ran before the published cutoff materialized"
    );
    assert!(
        !flush.is_finished(),
        "Flush returned before target durability"
    );

    release.notify_one();
    tokio::time::timeout(Duration::from_secs(2), &mut flush)
        .await
        .expect("admin Flush did not resume")
        .expect("admin Flush task panicked")
        .expect("admin Flush failed");
    shutdown.cancel();
}

#[tokio::test]
async fn unix_admin_flush_uses_normalized_remote_backend_target() {
    let (fs, checkpoint_manager) = make_fs().await;
    let (client, shutdown, _dir) = setup_fs(Arc::clone(&fs), checkpoint_manager, false).await;
    let file = fs
        .create_exclusive(&root_auth(), 0, b"rpc-flush-direct")
        .await
        .unwrap();
    fs.write_ack(
        &root_auth(),
        file,
        0,
        &Bytes::from_static(b"direct-backend"),
    )
    .await
    .unwrap();

    let (target_tx, mut target_rx) = mpsc::unbounded_channel();
    fs.flush_coordinator
        .set_object_wait(Arc::new(move |_coverage, target| {
            let target_tx = target_tx.clone();
            Box::pin(async move { target_tx.send(target).map_err(|_| DurabilityError::Closed) })
        }));

    client.flush().await.unwrap();
    let target = tokio::time::timeout(Duration::from_secs(2), target_rx.recv())
        .await
        .expect("admin Flush never entered typed durability")
        .expect("typed durability observation channel closed");
    assert_eq!(target, DurabilityTarget::RemoteBackend);
    shutdown.cancel();
}
