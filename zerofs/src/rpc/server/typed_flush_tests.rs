use super::super::root_auth;
use super::*;
use crate::block_transformer::ZeroFsBlockTransformer;
use crate::config::CompressionConfig;
use crate::db::SlateDbHandle;
use crate::ninep::NinePServer;
use crate::fs::mutation::config::{
    ClientDurabilityTarget, FilesystemWriteAckMode, FilesystemWriteAckSettings,
    FilesystemWriteAckSource,
};
use crate::fs::mutation::durability::{DurabilityError, DurabilityTarget};
use crate::writeback::config::{AckMode, ShutdownFlush, WritebackSettings};
use crate::writeback::model::JournalIdentity;
use crate::writeback::store::WritebackObjectStore;
use bytes::Bytes;
use futures::TryStreamExt;
use ninep_client::{NOFID, NinePClient};
use object_store::ObjectStore;
use object_store::local::LocalFileSystem;
use slatedb::DbBuilder;
use slatedb::object_store::path::Path as DbPath;
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
async fn unix_admin_flush_materializes_overlay_before_remote_target() {
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
    assert_eq!(
        target,
        DurabilityTarget::RemoteBackend,
        "administrative Flush is remote-durable even when client fsync resolves to LocalSsd"
    );
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
    assert!(!flush.is_finished(), "Flush returned before target durability");

    release.notify_one();
    tokio::time::timeout(Duration::from_secs(2), &mut flush)
        .await
        .expect("admin Flush did not resume")
        .expect("admin Flush task panicked")
        .expect("admin Flush failed");
    shutdown.cancel();
}

async fn real_writeback_fs(
    ignore_fsync: bool,
    client_target: ClientDurabilityTarget,
) -> (
    Arc<ZeroFS>,
    Arc<CheckpointManager>,
    WritebackObjectStore,
    Arc<LocalFileSystem>,
    mpsc::UnboundedReceiver<(
        crate::fs::mutation::durability::ObjectCoverage,
        DurabilityTarget,
    )>,
    tempfile::TempDir,
) {
    let temp = tempfile::tempdir().unwrap();
    let remote_root = temp.path().join("remote");
    std::fs::create_dir(&remote_root).unwrap();
    let remote = Arc::new(LocalFileSystem::new_with_prefix(&remote_root).unwrap());
    let test_id = uuid::Uuid::new_v4().simple().to_string();
    let database_prefix = format!("rpc/{test_id}");
    let attached = crate::writeback::bootstrap::attach(
        remote.clone(),
        WritebackSettings {
            dir: temp.path().join("writeback"),
            ack_mode: AckMode::Memory,
            memory_bytes: 16 * 1024 * 1024,
            disk_bytes: 64 * 1024 * 1024,
            min_free_bytes: 1,
            high_watermark_percent: 95,
            resume_percent: 85,
            upload_concurrency: 2,
            local_concurrency: 2,
            shutdown_flush: ShutdownFlush::Remote,
        },
        JournalIdentity {
            format_version: 1,
            bucket_id: format!("rpc-{test_id}"),
            backend_endpoint: remote_root.display().to_string(),
            database_prefix: database_prefix.clone(),
            backend_kind: "local".to_owned(),
            encryption_key_identity_sha256: [0x52; 32],
        },
        &format!("rpc_{test_id}"),
    )
    .await
    .unwrap();
    let writeback = attached.lifecycle;
    let object_store = attached.store;

    let test_key = [0u8; 32];
    let block_transformer: Arc<dyn slatedb::BlockTransformer> =
        ZeroFsBlockTransformer::new_arc(&test_key, CompressionConfig::default());
    let db_path = DbPath::from(format!("{database_prefix}/slatedb"));
    let slatedb = Arc::new(
        DbBuilder::new(db_path.clone(), Arc::clone(&object_store))
            .with_block_transformer(block_transformer)
            .with_filter_policies(crate::fs::filter_policy::filter_policies())
            .with_segment_extractor(Arc::new(crate::segment_extractor::ZeroFsSegmentExtractor))
            .build()
            .await
            .unwrap(),
    );
    let db_handle = SlateDbHandle::ReadWrite(slatedb);
    let mut fs = ZeroFS::new_with_slatedb_and_lease(
        db_handle.clone(),
        u64::MAX,
        None,
        false,
        ignore_fsync,
        None,
        None,
        Arc::new(crate::dedup::DedupCache::new()),
        None,
        crate::object_trace::ObjectTracer::new(),
        Arc::clone(&object_store),
        crate::frame_codec::FrameCodec::new(
            &test_key,
            crate::segment::SEGMENT_INFO,
            CompressionConfig::default(),
        ),
        None,
        crate::config::StoreProfile::default(),
    )
    .await
    .unwrap();
    fs.write_ack = FilesystemWriteAckSettings {
        client_durability_target: client_target,
        ..volatile_write_ack_settings()
    };
    fs.flush_coordinator.set_local_durability_barrier({
        let writeback = writeback.clone();
        Arc::new(move || {
            let writeback = writeback.clone();
            Box::pin(async move {
                writeback
                    .wait_local_through_accepted()
                    .await
                    .map_err(|_| crate::fs::errors::FsError::IoError)
            })
        })
    });
    fs.flush_coordinator.set_object_capture({
        let writeback = writeback.clone();
        Arc::new(move || writeback.object_coverage())
    });
    let (coverage_tx, coverage_rx) = mpsc::unbounded_channel();
    fs.flush_coordinator.set_object_wait({
        let writeback = writeback.clone();
        Arc::new(move |coverage, target| {
            let writeback = writeback.clone();
            let coverage_tx = coverage_tx.clone();
            Box::pin(async move {
                use crate::fs::mutation::durability::ObjectCoverage;
                coverage_tx
                    .send((coverage, target))
                    .map_err(|_| DurabilityError::Closed)?;
                match coverage {
                    ObjectCoverage::DirectRemote => Ok(()),
                    ObjectCoverage::Writeback {
                        journal_incarnation,
                        sequence,
                    } => writeback
                        .wait_coverage(
                            journal_incarnation.as_uuid(),
                            sequence,
                            matches!(target, DurabilityTarget::RemoteBackend),
                        )
                        .await
                        .map_err(DurabilityError::Object),
                }
            })
        })
    });
    let fs = Arc::new(fs);
    fs.install_volatile_overlay();
    fs.start_reclaim_drainer();
    let checkpoint_manager = Arc::new(CheckpointManager::new(
        db_handle,
        db_path,
        object_store,
        None,
    ));
    (
        fs,
        checkpoint_manager,
        writeback,
        remote,
        coverage_rx,
        temp,
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unix_admin_flush_waits_for_real_remote_writeback() {
    let (fs, checkpoint_manager, writeback, remote, mut coverage_rx, temp) =
        real_writeback_fs(true, ClientDurabilityTarget::LocalSsd).await;
    let shutdown = CancellationToken::new();
    let service = AdminRpcServer::new(checkpoint_manager, Arc::clone(&fs), shutdown.clone());
    let socket = temp.path().join("admin.sock");
    let server = tokio::spawn({
        let socket = socket.clone();
        let shutdown = shutdown.clone();
        async move { serve_unix(socket, service, shutdown).await }
    });
    let client = super::connect_with_retry(&socket).await;

    let auth = root_auth();
    let file = fs
        .create_exclusive(&auth, 0, b"real-writeback-flush")
        .await
        .unwrap();
    fs.write_ack(
        &auth,
        file,
        0,
        &Bytes::from_static(b"remote-durability"),
    )
    .await
    .unwrap();
    let expected_cutoff = fs.capture_mutation_cutoff();
    assert!(expected_cutoff.sequence > 0);

    let mut flush = tokio::spawn(async move { client.flush().await });
    let locally_durable = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let status = writeback.status().unwrap();
            if status.accepted_seq > 0 && status.local_seq == status.accepted_seq {
                break status;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("real SSD journal did not cover accepted writeback objects");
    assert_eq!(locally_durable.remote_seq, 0);
    let (captured_coverage, captured_target) =
        tokio::time::timeout(Duration::from_secs(5), coverage_rx.recv())
            .await
            .expect("administrative Flush did not capture object coverage")
            .expect("administrative Flush object-coverage observer closed");
    assert_eq!(captured_target, DurabilityTarget::RemoteBackend);
    let captured_sequence = match captured_coverage {
        crate::fs::mutation::durability::ObjectCoverage::Writeback {
            journal_incarnation,
            sequence,
        } => {
            assert_eq!(
                journal_incarnation.as_uuid(),
                writeback.journal_incarnation()
            );
            assert!(
                sequence >= locally_durable.accepted_seq,
                "captured object coverage predates the accepted writeback objects"
            );
            sequence
        }
        other => panic!("writeback Flush captured non-writeback coverage: {other:?}"),
    };
    assert!(
        !flush.is_finished(),
        "administrative Flush returned after local journal coverage while remote replay was paused"
    );
    assert!(
        fs.materializer
            .get()
            .expect("volatile filesystem has a materializer")
            .progress()
            .materialized_through()
            >= expected_cutoff.sequence
    );

    writeback.activate_remote().unwrap();
    tokio::time::timeout(Duration::from_secs(10), &mut flush)
        .await
        .expect("administrative Flush did not resume after remote replay")
        .expect("administrative Flush task panicked")
        .expect("administrative Flush failed");
    let remote_status = writeback.status().unwrap();
    assert!(remote_status.accepted_seq > 0);
    assert_eq!(remote_status.remote_seq, remote_status.accepted_seq);
    assert!(
        remote_status.remote_seq >= captured_sequence,
        "remote replay did not cross the Flush object-coverage cutoff"
    );
    let remote_objects = remote
        .list(None)
        .try_collect::<Vec<_>>()
        .await
        .expect("local filesystem remote listing failed");
    assert!(!remote_objects.is_empty(), "remote backend stayed empty");

    shutdown.cancel();
    tokio::time::timeout(Duration::from_secs(2), server)
        .await
        .expect("admin RPC server did not stop")
        .expect("admin RPC server task panicked")
        .expect("admin RPC server failed");
    fs.stop_new_mutation_admission();
    fs.stop_mutation_workers().await;
    fs.flush_coordinator.close().await.unwrap();
    writeback.shutdown().await.unwrap();
    let cleanup_root = temp.path().to_path_buf();
    drop(remote);
    drop(writeback);
    drop(fs);
    drop(temp);
    assert!(!cleanup_root.exists(), "isolated writeback resources leaked");
}

async fn connect_ninep_with_retry(socket: &std::path::Path) -> Arc<NinePClient> {
    for _ in 0..100 {
        if let Ok(client) = NinePClient::connect_unix(socket, 256 * 1024).await {
            return client;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("production 9P client never connected to Unix server");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unix_ninep_verified_fsync_waits_for_real_remote_writeback() {
    let (fs, checkpoint_manager, writeback, remote, mut coverage_rx, temp) =
        real_writeback_fs(false, ClientDurabilityTarget::RemoteBackend).await;
    let shutdown = CancellationToken::new();
    let socket = temp.path().join("ninep.sock");
    let server = NinePServer::new_unix(Arc::clone(&fs), socket.clone());
    let server_task = tokio::spawn({
        let shutdown = shutdown.clone();
        async move { server.start(shutdown).await }
    });
    let client = connect_ninep_with_retry(&socket).await;
    client.attach(1, NOFID, "root", "", 0).await.unwrap();
    let fid = client.alloc_fid();
    client.walk(1, fid, &[]).await.unwrap();
    client
        .lcreate(
            fid,
            b"typed-durability",
            (libc::O_RDWR | libc::O_CREAT) as u32,
            u32::from(libc::S_IFREG | 0o644),
            0,
        )
        .await
        .unwrap();
    let payload = b"production-ninep-remote";
    assert_eq!(
        client.write(fid, 0, payload).await.unwrap(),
        payload.len() as u64
    );
    let expected_cutoff = fs.capture_mutation_cutoff();
    assert!(expected_cutoff.sequence > 0);

    let mut fsync = tokio::spawn({
        let client = Arc::clone(&client);
        async move { client.fsync(fid, 0).await }
    });
    let (coverage, target) = tokio::time::timeout(Duration::from_secs(5), coverage_rx.recv())
        .await
        .expect("9P verified fsync did not capture object coverage")
        .expect("9P object-coverage observer closed");
    assert_eq!(target, DurabilityTarget::RemoteBackend);
    let captured_sequence = match coverage {
        crate::fs::mutation::durability::ObjectCoverage::Writeback {
            journal_incarnation,
            sequence,
        } => {
            assert_eq!(
                journal_incarnation.as_uuid(),
                writeback.journal_incarnation()
            );
            assert!(sequence > 0);
            sequence
        }
        other => panic!("9P fsync captured non-writeback coverage: {other:?}"),
    };
    let locally_durable = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let status = writeback.status().unwrap();
            if status.local_seq >= captured_sequence {
                break status;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("real SSD journal did not cross the 9P coverage cutoff");
    assert_eq!(locally_durable.remote_seq, 0);
    assert!(
        !fsync.is_finished(),
        "9P verified fsync returned while remote replay was paused"
    );
    assert!(
        fs.materializer
            .get()
            .expect("volatile filesystem has a materializer")
            .progress()
            .materialized_through()
            >= expected_cutoff.sequence
    );

    writeback.activate_remote().unwrap();
    tokio::time::timeout(Duration::from_secs(10), &mut fsync)
        .await
        .expect("9P verified fsync did not resume after remote replay")
        .expect("9P verified fsync task panicked")
        .expect("9P verified fsync failed");
    let remote_status = writeback.status().unwrap();
    assert!(remote_status.remote_seq >= captured_sequence);
    assert_eq!(client.read(fid, 0, payload.len() as u32).await.unwrap(), payload);
    let remote_objects = remote
        .list(None)
        .try_collect::<Vec<_>>()
        .await
        .expect("local filesystem remote listing failed");
    assert!(!remote_objects.is_empty(), "9P remote backend stayed empty");

    client.clunk(fid).await.unwrap();
    client.free_fid(fid);
    drop(client);
    shutdown.cancel();
    tokio::time::timeout(Duration::from_secs(2), server_task)
        .await
        .expect("9P server did not stop")
        .expect("9P server task panicked")
        .expect("9P server failed");
    fs.stop_new_mutation_admission();
    fs.stop_mutation_workers().await;
    fs.flush_coordinator.close().await.unwrap();
    writeback.shutdown().await.unwrap();
    let cleanup_root = temp.path().to_path_buf();
    drop(checkpoint_manager);
    drop(remote);
    drop(writeback);
    drop(fs);
    drop(temp);
    assert!(!cleanup_root.exists(), "isolated 9P resources leaked");
}
