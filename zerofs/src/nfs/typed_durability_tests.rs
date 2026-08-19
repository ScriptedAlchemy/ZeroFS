use super::*;
use crate::fs::mutation::config::{
    ClientDurabilityTarget, FilesystemWriteAckMode, FilesystemWriteAckSettings,
    FilesystemWriteAckSource,
};
use crate::test_helpers::test_helpers_mod::filename;
use object_store::memory::InMemory;
use slatedb::{BlockTransformer, DbBuilder};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::Notify;
use zerofs_nfsserve::nfs::{nfsstat3, sattr3, stable_how};
use zerofs_nfsserve::tcp::{NFSTcp, NFSTcpListener};
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

fn rpc_call_prefix(xid: u32, procedure: u32) -> Vec<u8> {
    let mut call = Vec::with_capacity(96);
    for value in [xid, 0, 2, 100003, 3, procedure, 0, 0, 0, 0] {
        call.extend_from_slice(&value.to_be_bytes());
    }
    call
}

fn wire_write_call(xid: u32, file: fileid3, stability: stable_how, data: &[u8]) -> Vec<u8> {
    assert_eq!(data.len() % 4, 0, "wire test payload must be XDR aligned");
    let mut call = rpc_call_prefix(xid, 7);
    call.extend_from_slice(&16u32.to_be_bytes());
    call.extend_from_slice(&0u64.to_le_bytes());
    call.extend_from_slice(&file.to_le_bytes());
    call.extend_from_slice(&0u64.to_be_bytes());
    call.extend_from_slice(&(data.len() as u32).to_be_bytes());
    call.extend_from_slice(&(stability as u32).to_be_bytes());
    call.extend_from_slice(&(data.len() as u32).to_be_bytes());
    call.extend_from_slice(data);
    call
}

fn wire_commit_call(xid: u32, file: fileid3) -> Vec<u8> {
    let mut call = rpc_call_prefix(xid, 21);
    call.extend_from_slice(&16u32.to_be_bytes());
    call.extend_from_slice(&0u64.to_le_bytes());
    call.extend_from_slice(&file.to_le_bytes());
    call.extend_from_slice(&0u64.to_be_bytes());
    call.extend_from_slice(&0u32.to_be_bytes());
    call
}

async fn rpc_roundtrip(stream: &mut TcpStream, call: &[u8]) -> Vec<u8> {
    let marker = (call.len() as u32) | (1 << 31);
    stream.write_all(&marker.to_be_bytes()).await.unwrap();
    stream.write_all(call).await.unwrap();

    let mut marker = [0; 4];
    tokio::time::timeout(Duration::from_secs(5), stream.read_exact(&mut marker))
        .await
        .expect("NFS wire reply timed out")
        .expect("read NFS wire reply marker");
    let length = (u32::from_be_bytes(marker) & 0x7fff_ffff) as usize;
    let mut reply = vec![0; length];
    stream
        .read_exact(&mut reply)
        .await
        .expect("read NFS wire reply");
    reply
}

fn take_u32(reply: &[u8], offset: &mut usize) -> u32 {
    let value = u32::from_be_bytes(reply[*offset..*offset + 4].try_into().unwrap());
    *offset += 4;
    value
}

fn skip_wcc(reply: &[u8], offset: &mut usize) {
    if take_u32(reply, offset) != 0 {
        *offset += 24;
    }
    if take_u32(reply, offset) != 0 {
        *offset += 84;
    }
}

fn parse_nfs_status(reply: &[u8], expected_xid: u32) -> u32 {
    let mut offset = 0;
    assert_eq!(take_u32(reply, &mut offset), expected_xid);
    assert_eq!(
        take_u32(reply, &mut offset),
        1,
        "RPC response was not REPLY"
    );
    assert_eq!(take_u32(reply, &mut offset), 0, "RPC reply was denied");
    assert_eq!(take_u32(reply, &mut offset), 0, "unexpected RPC verifier");
    assert_eq!(take_u32(reply, &mut offset), 0, "unexpected verifier body");
    assert_eq!(take_u32(reply, &mut offset), 0, "RPC call was not accepted");
    take_u32(reply, &mut offset)
}

fn parse_write_reply(
    reply: &[u8],
    expected_xid: u32,
    expected_count: u32,
) -> (stable_how, writeverf3) {
    assert_eq!(
        parse_nfs_status(reply, expected_xid),
        nfsstat3::NFS3_OK as u32
    );
    let mut offset = 28;
    skip_wcc(reply, &mut offset);
    assert_eq!(take_u32(reply, &mut offset), expected_count);
    let committed = match take_u32(reply, &mut offset) {
        0 => stable_how::UNSTABLE,
        1 => stable_how::DATA_SYNC,
        2 => stable_how::FILE_SYNC,
        value => panic!("invalid committed stable_how {value}"),
    };
    let verifier = reply[offset..offset + 8].try_into().unwrap();
    offset += 8;
    assert_eq!(offset, reply.len(), "unparsed NFS WRITE reply bytes");
    (committed, verifier)
}

fn parse_commit_reply(reply: &[u8], expected_xid: u32) -> writeverf3 {
    assert_eq!(
        parse_nfs_status(reply, expected_xid),
        nfsstat3::NFS3_OK as u32
    );
    let mut offset = 28;
    skip_wcc(reply, &mut offset);
    let verifier = reply[offset..offset + 8].try_into().unwrap();
    offset += 8;
    assert_eq!(offset, reply.len(), "unparsed NFS COMMIT reply bytes");
    verifier
}

fn writeback_settings(base: std::path::PathBuf) -> crate::writeback::config::WritebackSettings {
    use crate::writeback::config::{AckMode, ShutdownFlush, WritebackSettings};
    WritebackSettings {
        dir: base,
        ack_mode: AckMode::Memory,
        memory_bytes: 2_000_000,
        disk_bytes: 128_000_000,
        min_free_bytes: 1,
        high_watermark_percent: 95,
        resume_percent: 85,
        upload_concurrency: 2,
        local_concurrency: 2,
        shutdown_flush: ShutdownFlush::Local,
    }
}

fn writeback_identity() -> crate::writeback::model::JournalIdentity {
    crate::writeback::model::JournalIdentity {
        format_version: 1,
        bucket_id: "nfs-shipping-test".to_string(),
        backend_endpoint: "memory://nfs-shipping-test".to_string(),
        database_prefix: "nfs-shipping-db".to_string(),
        backend_kind: "memory".to_string(),
        encryption_key_identity_sha256: [0x4e; 32],
    }
}

async fn open_writeback_fs(
    remote: Arc<InMemory>,
    settings: crate::writeback::config::WritebackSettings,
    identity: crate::writeback::model::JournalIdentity,
    namespace: &str,
) -> (Arc<ZeroFS>, crate::writeback::store::WritebackObjectStore) {
    use crate::block_transformer::ZeroFsBlockTransformer;
    use crate::config::CompressionConfig;
    use crate::db::SlateDbHandle;
    use crate::fs::mutation::durability::{DurabilityError, DurabilityTarget, ObjectCoverage};

    let attached = crate::writeback::bootstrap::attach(remote, settings, identity, namespace)
        .await
        .expect("attach real SSD writeback");
    let store = attached.store;
    let writeback = attached.lifecycle;
    let key = [0x4e; 32];
    let transformer: Arc<dyn BlockTransformer> =
        ZeroFsBlockTransformer::new_arc(&key, CompressionConfig::default());
    let slatedb = Arc::new(
        DbBuilder::new(
            object_store::path::Path::from("nfs-shipping-db"),
            store.clone(),
        )
        .with_block_transformer(transformer)
        .with_filter_policies(crate::fs::filter_policy::filter_policies())
        .with_segment_extractor(Arc::new(crate::segment_extractor::ZeroFsSegmentExtractor))
        .build()
        .await
        .expect("open SlateDB over SSD writeback"),
    );
    let segment_codec = crate::frame_codec::FrameCodec::new(
        &key,
        crate::segment::SEGMENT_INFO,
        CompressionConfig::default(),
    );
    let mut fs = ZeroFS::new_with_slatedb(
        SlateDbHandle::ReadWrite(slatedb),
        u64::MAX,
        None,
        false,
        store,
        segment_codec,
    )
    .await
    .expect("open ZeroFS over SSD writeback");
    fs.write_ack = volatile_local_settings();

    let local = writeback.clone();
    fs.flush_coordinator
        .set_local_durability_barrier(Arc::new(move || {
            let writeback = local.clone();
            Box::pin(async move {
                writeback
                    .wait_local_through_accepted()
                    .await
                    .map_err(|_| crate::fs::errors::FsError::IoError)
            })
        }));
    let captured = writeback.clone();
    fs.flush_coordinator
        .set_object_capture(Arc::new(move || captured.object_coverage()));
    let waited = writeback.clone();
    fs.flush_coordinator
        .set_object_wait(Arc::new(move |coverage, target| {
            let writeback = waited.clone();
            Box::pin(async move {
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
        }));

    let fs = Arc::new(fs);
    fs.install_volatile_overlay();
    fs.start_materializer();
    (fs, writeback)
}

async fn close_writeback_fs(
    fs: &Arc<ZeroFS>,
    writeback: crate::writeback::store::WritebackObjectStore,
    remote: bool,
) {
    use crate::fs::mutation::durability::ObjectCoverage;

    fs.write_coordinator
        .barrier()
        .await
        .expect("drain the canonical commit worker before close");
    fs.stop_new_mutation_admission();
    let cutoff = crate::fs::mutation::closed_admission_cutoff(fs);
    fs.materialize_through_cutoff(cutoff)
        .await
        .expect("materialize through close cutoff");
    fs.flush_coordinator
        .stop_worker()
        .await
        .expect("stop durability coordinator before close");
    let barrier = fs.db.flush_barrier().write_owned().await;
    fs.close_canonical_database()
        .await
        .expect("close canonical database");
    let coverage = writeback.object_coverage();
    drop(barrier);
    if let ObjectCoverage::Writeback {
        journal_incarnation,
        sequence,
    } = coverage
    {
        writeback
            .wait_coverage(journal_incarnation.as_uuid(), sequence, remote)
            .await
            .expect("wait final writeback coverage");
    }
    fs.stop_mutation_workers()
        .await
        .expect("stop mutation workers before writeback shutdown");
    writeback.shutdown().await.expect("shutdown SSD writeback");
}

async fn wait_for_journal_lock_release(lock_path: &std::path::Path) {
    use fs4::fs_std::FileExt;
    use std::fs::OpenOptions;

    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let lock = OpenOptions::new()
                .read(true)
                .write(true)
                .open(lock_path)
                .expect("open writeback journal lock");
            if FileExt::try_lock_exclusive(&lock).expect("probe writeback journal lock") {
                FileExt::unlock(&lock).expect("release writeback journal lock probe");
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("closed writeback owners did not release the journal lock");
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

#[tokio::test]
async fn shipping_tcp_write_commit_survive_writeback_restart_with_new_verifier() {
    let temp = tempfile::tempdir().expect("temporary SSD writeback directory");
    let base = temp.path().join("writeback");
    let settings = writeback_settings(base.clone());
    let identity = writeback_identity();
    let remote = Arc::new(InMemory::new());
    let unstable_payload = *b"nfs-unstable-001";
    let data_sync_payload = *b"nfs-datasync-001";
    let payload = *b"nfs-ssd-wire-001";

    let (fs, writeback) = open_writeback_fs(
        Arc::clone(&remote),
        settings.clone(),
        identity.clone(),
        "nfs_shipping_test",
    )
    .await;
    let service = NfsServiceIdentity::new();
    let adapter = NFSAdapter::with_service_identity(Arc::clone(&fs), service);
    let file = adapter
        .create(
            &nfs_auth(1000),
            0,
            &filename(b"shipping-wire"),
            sattr3::default(),
        )
        .await
        .expect("create shipping NFS test file")
        .0;
    let listener = NFSTcpListener::bind("127.0.0.1:0".parse().unwrap(), adapter)
        .await
        .expect("bind shipping NFS listener");
    let port = listener.get_listen_port();
    let shutdown = tokio_util::sync::CancellationToken::new();
    let server_shutdown = shutdown.clone();
    let server = tokio::spawn(async move {
        listener
            .handle_with_shutdown(server_shutdown)
            .await
            .expect("shipping NFS listener failed")
    });
    let mut client = TcpStream::connect(("127.0.0.1", port))
        .await
        .expect("connect shipping NFS client");

    let unstable_reply = rpc_roundtrip(
        &mut client,
        &wire_write_call(600, file, stable_how::UNSTABLE, &unstable_payload),
    )
    .await;
    let (committed, first_verifier) =
        parse_write_reply(&unstable_reply, 600, unstable_payload.len() as u32);
    assert_eq!(committed, stable_how::UNSTABLE);
    assert_ne!(first_verifier, [0; 8]);

    let data_sync_reply = rpc_roundtrip(
        &mut client,
        &wire_write_call(601, file, stable_how::DATA_SYNC, &data_sync_payload),
    )
    .await;
    let (committed, verifier) =
        parse_write_reply(&data_sync_reply, 601, data_sync_payload.len() as u32);
    assert_eq!(committed, stable_how::FILE_SYNC);
    assert_eq!(verifier, first_verifier);

    let file_sync_call = wire_write_call(602, file, stable_how::FILE_SYNC, &payload);
    let file_sync_reply = rpc_roundtrip(&mut client, &file_sync_call).await;
    let (committed, verifier) = parse_write_reply(&file_sync_reply, 602, payload.len() as u32);
    assert_eq!(committed, stable_how::FILE_SYNC);
    assert_eq!(verifier, first_verifier);
    let accepted_after_file_sync = fs
        .volatile_overlay
        .get()
        .expect("volatile overlay")
        .accepted_batch_count();

    let replay_reply = rpc_roundtrip(&mut client, &file_sync_call).await;
    let (committed, verifier) = parse_write_reply(&replay_reply, 602, payload.len() as u32);
    assert_eq!(committed, stable_how::FILE_SYNC);
    assert_eq!(verifier, first_verifier);
    assert_eq!(
        fs.volatile_overlay
            .get()
            .expect("volatile overlay")
            .accepted_batch_count(),
        accepted_after_file_sync,
        "same-connection NFS replay published another mutation",
    );

    let collision_reply = rpc_roundtrip(
        &mut client,
        &wire_write_call(602, file, stable_how::FILE_SYNC, b"nfs-collision-01"),
    )
    .await;
    assert_eq!(
        parse_nfs_status(&collision_reply, 602),
        nfsstat3::NFS3ERR_INVAL as u32
    );
    assert_eq!(
        fs.volatile_overlay
            .get()
            .expect("volatile overlay")
            .accepted_batch_count(),
        accepted_after_file_sync,
        "same-XID fingerprint collision published a mutation",
    );

    let commit_reply = rpc_roundtrip(&mut client, &wire_commit_call(603, file)).await;
    assert_eq!(parse_commit_reply(&commit_reply, 603), first_verifier);
    assert!(writeback.accepted_sequence() > 0);
    assert!(
        base.join("nfs_shipping_test").join("journal.redb").exists(),
        "stable NFS reply did not create the real SSD journal"
    );

    drop(client);
    shutdown.cancel();
    server.await.expect("shipping NFS server task panicked");
    close_writeback_fs(&fs, writeback, false).await;
    match Arc::try_unwrap(fs) {
        Ok(fs) => drop(fs),
        Err(fs) => panic!(
            "closed filesystem retained {} strong owners",
            Arc::strong_count(&fs)
        ),
    }
    wait_for_journal_lock_release(&base.join("nfs_shipping_test").join("LOCK")).await;

    let (recovered, recovered_writeback) =
        open_writeback_fs(Arc::clone(&remote), settings, identity, "nfs_shipping_test").await;
    let auth = AuthContext {
        uid: 0,
        gid: 0,
        gid_known: true,
        gids: vec![0],
        groups_complete: true,
    };
    let (bytes, _) = recovered
        .read_file(&auth, file, 0, payload.len() as u32)
        .await
        .expect("read NFS bytes after SSD recovery");
    assert_eq!(bytes.as_ref(), payload);
    recovered_writeback
        .activate_remote()
        .expect("activate recovered writeback replay");

    let restarted =
        NFSAdapter::with_service_identity(Arc::clone(&recovered), NfsServiceIdentity::new());
    let listener = NFSTcpListener::bind("127.0.0.1:0".parse().unwrap(), restarted)
        .await
        .expect("bind restarted NFS listener");
    let port = listener.get_listen_port();
    let shutdown = tokio_util::sync::CancellationToken::new();
    let server_shutdown = shutdown.clone();
    let server = tokio::spawn(async move {
        listener
            .handle_with_shutdown(server_shutdown)
            .await
            .expect("restarted NFS listener failed")
    });
    let mut client = TcpStream::connect(("127.0.0.1", port))
        .await
        .expect("connect restarted NFS client");
    let commit_reply = rpc_roundtrip(&mut client, &wire_commit_call(604, file)).await;
    let restarted_verifier = parse_commit_reply(&commit_reply, 604);
    assert_ne!(restarted_verifier, [0; 8]);
    assert_ne!(restarted_verifier, first_verifier);

    drop(client);
    shutdown.cancel();
    server.await.expect("restarted NFS server task panicked");
    close_writeback_fs(&recovered, recovered_writeback, true).await;
}

#[tokio::test]
async fn ignore_fsync_never_reports_stable_write_or_commit() {
    let mut fs = ZeroFS::new_in_memory().await.expect("in-memory filesystem");
    fs.ignore_fsync = true;
    let fs = Arc::new(fs);
    let adapter = NFSAdapter::new(Arc::clone(&fs));
    let file = adapter
        .create(
            &nfs_auth(1000),
            0,
            &filename(b"ignore-fsync"),
            sattr3::default(),
        )
        .await
        .expect("create NFS test file")
        .0;

    let write = adapter
        .write_with_context(
            &write_context(500, 61, stable_how::FILE_SYNC),
            &nfs_auth(1000),
            file,
            0,
            b"not stable",
        )
        .await
        .expect("NFS should report the achieved weaker level");
    assert_eq!(write.committed, stable_how::UNSTABLE);

    let commit = adapter
        .commit_with_context(&commit_context(501, 61), &nfs_auth(1000), file, 0, 0)
        .await;
    assert!(matches!(commit, Err(nfsstat3::NFS3ERR_NOTSUPP)));
}

#[tokio::test]
async fn shipping_nfs_listener_rejects_ignore_fsync() {
    let mut fs = ZeroFS::new_in_memory().await.expect("in-memory filesystem");
    fs.ignore_fsync = true;
    let result = tokio::time::timeout(
        Duration::from_millis(100),
        start_nfs_server_with_service_identity(
            Arc::new(fs),
            "127.0.0.1:0".parse().unwrap(),
            tokio_util::sync::CancellationToken::new(),
            None,
            NfsServiceIdentity::new(),
        ),
    )
    .await
    .expect("NFS startup must reject ignore_fsync before binding");

    let error = result.expect_err("NFS must fail closed when fsync is disabled");
    assert!(format!("{error:#}").contains("ignore_fsync"));
}
