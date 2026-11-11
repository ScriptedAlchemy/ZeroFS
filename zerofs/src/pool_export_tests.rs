//! Pool-backed NFS and NBD wire regressions. No kernel mounts or disk management.

use crate::block_transformer::ZeroFsBlockTransformer;
use crate::config::{CompressionConfig, Settings};
use crate::db::SlateDbHandle;
use crate::frame_codec::FrameCodec;
use crate::fs::ZeroFS;
use crate::fs::permissions::Credentials;
use crate::fs::types::{SetAttributes, SetSize};
use crate::nbd::{NBDServer, NbdExportGates};
use crate::nfs::NFSAdapter;
use crate::pool_object_store::PoolObjectStore;
use crate::storage_backend::open_pool_storage;
use crate::test_helpers::test_helpers_mod::filename;
use deku::{DekuContainerRead, DekuContainerWrite};
use nbd_proto::{
    NBD_CMD_FLAG_FUA, NBD_FLAG_C_FIXED_NEWSTYLE, NBD_FLAG_C_NO_ZEROES, NBD_IHAVEOPT, NBD_MAGIC,
    NBD_OPT_EXPORT_NAME, NBD_REQUEST_MAGIC, NBD_SIMPLE_REPLY_MAGIC, NBD_SUCCESS, NBDCommand,
    NBDRequest, NBDSimpleReply,
};
use object_store::ObjectStore;
use sha2::{Digest, Sha256};
use slatedb::{BlockTransformer, DbBuilder};
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpStream, UnixStream};
use tokio_util::sync::CancellationToken;
use zerofs_nfsserve::nfs::{sattr3, stable_how};
use zerofs_nfsserve::tcp::{NFSTcp, NFSTcpListener};
use zerofs_nfsserve::vfs::{AuthContext as NfsAuthContext, NFSFileSystem};

const EXPORT: &[u8] = b"pool-volume";
const PAYLOAD_SIZE: usize = 64 * 1024;

fn credentials() -> Credentials {
    Credentials {
        uid: 0,
        gid: 0,
        gid_known: true,
        groups: [0; 16],
        groups_count: 1,
        groups_complete: true,
    }
}

fn settings(root: &Path) -> Settings {
    for directory in ["metadata", "disk-a", "disk-b"] {
        std::fs::create_dir(root.join(directory)).unwrap();
    }
    let uri = |name: &str| {
        url::Url::from_directory_path(root.join(name))
            .unwrap()
            .to_string()
    };
    let document = serde_json::json!({
        "cache": {"dir": root.join("cache"), "disk_size_gb": 0.001},
        "storage": {
            "url": uri("metadata"),
            "encryption_password": "pool-export-fixture",
            "pool": {
                "catalog_dir": root.join("catalog"),
                "members": [
                    {"id": "disk-a", "url": uri("disk-a"), "capacity_gb": 0.008, "reserve_free_gb": 0.0},
                    {"id": "disk-b", "url": uri("disk-b"), "capacity_gb": 0.008, "reserve_free_gb": 0.0}
                ]
            }
        },
        "servers": {},
        "telemetry": {"enabled": false}
    });
    let config = root.join("zerofs.toml");
    std::fs::write(&config, toml::to_string(&document).unwrap()).unwrap();
    Settings::from_file(config).unwrap()
}

async fn open(settings: &Settings, initialize: bool) -> (Arc<ZeroFS>, PoolObjectStore) {
    let (opened, pool) = open_pool_storage(settings, initialize).await.unwrap();
    let store: Arc<dyn ObjectStore> = Arc::from(opened.store);
    let db_path = opened.path;
    let key = [0x67; 32];
    let transformer: Arc<dyn BlockTransformer> =
        ZeroFsBlockTransformer::new_arc(&key, CompressionConfig::default());
    let db = Arc::new(
        DbBuilder::new(db_path.clone(), Arc::clone(&store))
            .with_block_transformer(transformer)
            .with_filter_policies(crate::fs::filter_policy::filter_policies())
            .with_segment_extractor(Arc::new(crate::segment_extractor::ZeroFsSegmentExtractor))
            .build()
            .await
            .unwrap(),
    );
    let codec = FrameCodec::new(
        &key,
        crate::segment::SEGMENT_INFO,
        CompressionConfig::default(),
    );
    // Match production: SlateDB receives the raw store and database path,
    // while relative segment keys are prefixed before reaching the pool.
    let segment_store: Arc<dyn ObjectStore> =
        Arc::new(object_store::prefix::PrefixStore::new(store, db_path));
    let mut fs = ZeroFS::new_with_slatedb(
        SlateDbHandle::ReadWrite(db),
        u64::MAX,
        None,
        false,
        segment_store,
        codec,
    )
    .await
    .unwrap();
    fs.write_ack = settings
        .filesystem_write_ack_settings(crate::writeback::config::WritebackAccessMode::ReadWrite)
        .unwrap();
    let fs = Arc::new(fs);
    fs.install_volatile_overlay();
    fs.start_materializer();
    (fs, pool)
}

async fn close(fs: Arc<ZeroFS>, pool: PoolObjectStore) {
    fs.stop_new_mutation_admission();
    fs.stop_mutation_workers().await.unwrap();
    fs.flush_coordinator.close().await.unwrap();
    pool.settle().await;
    drop(fs);
    drop(pool);
    tokio::task::yield_now().await;
}

fn nfs_call(xid: u32, procedure: u32, inode: u64) -> Vec<u8> {
    let mut call = Vec::new();
    for value in [xid, 0, 2, 100003, 3, procedure, 0, 0, 0, 0, 16] {
        call.extend_from_slice(&value.to_be_bytes());
    }
    call.extend_from_slice(&0_u64.to_le_bytes());
    call.extend_from_slice(&inode.to_le_bytes());
    call.extend_from_slice(&0_u64.to_be_bytes());
    call
}

async fn nfs_roundtrip(client: &mut TcpStream, call: &[u8], xid: u32) -> Vec<u8> {
    client
        .write_all(&((call.len() as u32) | 0x8000_0000).to_be_bytes())
        .await
        .unwrap();
    client.write_all(call).await.unwrap();
    let mut reply = Vec::new();
    loop {
        let marker = client.read_u32().await.unwrap();
        let length = (marker & 0x7fff_ffff) as usize;
        assert!(reply.len() + length <= 2 * PAYLOAD_SIZE);
        let start = reply.len();
        reply.resize(start + length, 0);
        client.read_exact(&mut reply[start..]).await.unwrap();
        if marker & 0x8000_0000 != 0 {
            break;
        }
    }
    let header: Vec<_> = reply[..28]
        .as_chunks::<4>()
        .0
        .iter()
        .map(|word| u32::from_be_bytes(*word))
        .collect();
    assert_eq!(header, [xid, 1, 0, 0, 0, 0, 0], "NFS/RPC operation failed");
    reply
}

fn take_u32(bytes: &[u8], offset: &mut usize) -> u32 {
    let value = u32::from_be_bytes(bytes[*offset..*offset + 4].try_into().unwrap());
    *offset += 4;
    value
}

fn skip_nfs_wcc(reply: &[u8], offset: &mut usize) {
    for length in [24, 84] {
        match take_u32(reply, offset) {
            0 => {}
            1 => *offset += length,
            flag => panic!("invalid NFS WCC presence flag {flag}"),
        }
    }
}

fn take_nfs_verifier(reply: &[u8], offset: &mut usize) -> [u8; 8] {
    let verifier = reply[*offset..*offset + 8].try_into().unwrap();
    *offset += 8;
    assert_eq!(*offset, reply.len(), "unparsed NFS reply bytes");
    verifier
}

fn parse_nfs_write_reply(reply: &[u8], expected_count: u32) -> [u8; 8] {
    let mut offset = 28;
    skip_nfs_wcc(reply, &mut offset);
    assert_eq!(take_u32(reply, &mut offset), expected_count);
    assert_eq!(take_u32(reply, &mut offset), stable_how::FILE_SYNC as u32);
    take_nfs_verifier(reply, &mut offset)
}

fn validate_nfs_commit_reply(reply: &[u8], expected_verifier: [u8; 8]) {
    let mut offset = 28;
    skip_nfs_wcc(reply, &mut offset);
    assert_eq!(
        take_nfs_verifier(reply, &mut offset),
        expected_verifier,
        "NFS WRITE and COMMIT must return the same service verifier"
    );
}

#[test]
fn nfs_write_and_commit_replies_require_complete_matching_verifiers() {
    let verifier = [0x3a; 8];
    for attributes in [false, true] {
        let prefix = || {
            // The roundtrip validates the RPC header and status separately.
            let mut reply = vec![0; 28];
            for length in [24, 84] {
                reply.extend_from_slice(&u32::from(attributes).to_be_bytes());
                if attributes {
                    reply.resize(reply.len() + length, 0);
                }
            }
            reply
        };
        let mut write = prefix();
        write.extend_from_slice(&4096_u32.to_be_bytes());
        write.extend_from_slice(&(stable_how::FILE_SYNC as u32).to_be_bytes());
        write.extend_from_slice(&verifier);
        assert_eq!(parse_nfs_write_reply(&write, 4096), verifier);
        let mut commit = prefix();
        commit.extend_from_slice(&verifier);
        validate_nfs_commit_reply(&commit, verifier);

        for length in [28, write.len() - 1] {
            assert!(
                std::panic::catch_unwind(|| parse_nfs_write_reply(&write[..length], 4096)).is_err()
            );
        }
        for length in [28, commit.len() - 1] {
            assert!(
                std::panic::catch_unwind(|| validate_nfs_commit_reply(&commit[..length], verifier))
                    .is_err()
            );
        }
        assert!(
            std::panic::catch_unwind(|| validate_nfs_commit_reply(&commit, [0x3b; 8])).is_err()
        );
        write.push(0);
        commit.push(0);
        assert!(std::panic::catch_unwind(|| parse_nfs_write_reply(&write, 4096)).is_err());
        assert!(std::panic::catch_unwind(|| validate_nfs_commit_reply(&commit, verifier)).is_err());
    }
}

async fn nfs_io(fs: Arc<ZeroFS>, inode: u64, payload: &[u8], write: bool) {
    let listener = NFSTcpListener::bind("127.0.0.1:0".parse().unwrap(), NFSAdapter::new(fs))
        .await
        .unwrap();
    let port = listener.get_listen_port();
    let stop = CancellationToken::new();
    let _cancel_on_drop = stop.clone().drop_guard();
    let stopping = stop.clone();
    let server = tokio::spawn(async move { listener.handle_with_shutdown(stopping).await });
    let mut client = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
    if write {
        let mut call = nfs_call(1, 7, inode);
        call.extend_from_slice(&(payload.len() as u32).to_be_bytes());
        call.extend_from_slice(&(stable_how::FILE_SYNC as u32).to_be_bytes());
        call.extend_from_slice(&(payload.len() as u32).to_be_bytes());
        call.extend_from_slice(payload);
        let reply = nfs_roundtrip(&mut client, &call, 1).await;
        let verifier = parse_nfs_write_reply(&reply, payload.len() as u32);
        let mut commit = nfs_call(2, 21, inode);
        commit.extend_from_slice(&0_u32.to_be_bytes());
        let reply = nfs_roundtrip(&mut client, &commit, 2).await;
        validate_nfs_commit_reply(&reply, verifier);
    }
    let mut call = nfs_call(3, 6, inode);
    call.extend_from_slice(&(payload.len() as u32).to_be_bytes());
    let reply = nfs_roundtrip(&mut client, &call, 3).await;
    let mut offset = 28;
    if take_u32(&reply, &mut offset) != 0 {
        offset += 84;
    }
    assert_eq!(take_u32(&reply, &mut offset), payload.len() as u32);
    assert_eq!(take_u32(&reply, &mut offset), 1, "NFS read must reach EOF");
    assert_eq!(take_u32(&reply, &mut offset), payload.len() as u32);
    assert_eq!(&reply[offset..], payload);
    drop(client);
    stop.cancel();
    tokio::time::timeout(Duration::from_secs(5), server)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
}

async fn nbd_reply(client: &mut UnixStream, cookie: u64) {
    let mut bytes = [0; 16];
    client.read_exact(&mut bytes).await.unwrap();
    let (_, reply) = NBDSimpleReply::from_bytes((&bytes, 0)).unwrap();
    assert_eq!(reply.magic, NBD_SIMPLE_REPLY_MAGIC);
    assert_eq!(reply.cookie, cookie);
    assert_eq!(reply.error, NBD_SUCCESS);
}

async fn nbd_io(fs: Arc<ZeroFS>, root: &Path, payload: &[u8], write: bool) {
    let socket = root.join("nbd.sock");
    let server = NBDServer::new_unix(fs, Arc::new(NbdExportGates::default()), &socket);
    let stop = CancellationToken::new();
    let _cancel_on_drop = stop.clone().drop_guard();
    let stopping = stop.clone();
    let task = tokio::spawn(async move { server.start(stopping).await });
    let mut client = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if let Ok(client) = UnixStream::connect(&socket).await {
                break client;
            }
            assert!(
                !task.is_finished(),
                "NBD server failed before accepting a client"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(client.read_u64().await.unwrap(), NBD_MAGIC);
    assert_eq!(client.read_u64().await.unwrap(), NBD_IHAVEOPT);
    let _server_flags = client.read_u16().await.unwrap();
    client
        .write_u32(NBD_FLAG_C_FIXED_NEWSTYLE | NBD_FLAG_C_NO_ZEROES)
        .await
        .unwrap();
    client.write_u64(NBD_IHAVEOPT).await.unwrap();
    client.write_u32(NBD_OPT_EXPORT_NAME).await.unwrap();
    client.write_u32(EXPORT.len() as u32).await.unwrap();
    client.write_all(EXPORT).await.unwrap();
    assert_eq!(client.read_u64().await.unwrap(), payload.len() as u64);
    let _transmission_flags = client.read_u16().await.unwrap();
    let request = |cmd_type, cookie, flags, length| {
        NBDRequest {
            magic: NBD_REQUEST_MAGIC,
            flags,
            cmd_type,
            cookie,
            offset: 0,
            length,
        }
        .to_bytes()
        .unwrap()
    };
    if write {
        client
            .write_all(&request(
                NBDCommand::Write,
                1,
                NBD_CMD_FLAG_FUA,
                payload.len() as u32,
            ))
            .await
            .unwrap();
        client.write_all(payload).await.unwrap();
        nbd_reply(&mut client, 1).await;
        client
            .write_all(&request(NBDCommand::Flush, 2, 0, 0))
            .await
            .unwrap();
        nbd_reply(&mut client, 2).await;
    }
    client
        .write_all(&request(NBDCommand::Read, 3, 0, payload.len() as u32))
        .await
        .unwrap();
    nbd_reply(&mut client, 3).await;
    let mut actual = vec![0; payload.len()];
    client.read_exact(&mut actual).await.unwrap();
    assert_eq!(actual, payload);
    client
        .write_all(&request(NBDCommand::Disconnect, 4, 0, 0))
        .await
        .unwrap();
    drop(client);
    stop.cancel();
    tokio::time::timeout(Duration::from_secs(5), task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pool_exports_nfs_and_nbd_survive_locator_evacuation_and_cold_reopen() {
    tokio::time::timeout(Duration::from_secs(60), async {
        let root = tempfile::tempdir().unwrap();
        let settings = settings(root.path());
        let (fs, pool) = open(&settings, true).await;
        let auth = NfsAuthContext {
            uid: 0,
            gid: 0,
            gids: vec![0],
        };
        let adapter = NFSAdapter::new(Arc::clone(&fs));
        let (nfs_inode, _) = adapter
            .create(&auth, 0, &filename(b"nfs-file"), sattr3::default())
            .await
            .unwrap();
        drop(adapter);
        let (nbd_directory, _) = fs
            .mkdir(&credentials(), 0, b".nbd", &SetAttributes::default())
            .await
            .unwrap();
        let (nbd_inode, _) = fs
            .create(
                &credentials(),
                nbd_directory,
                EXPORT,
                &SetAttributes::default(),
            )
            .await
            .unwrap();
        fs.setattr(
            &credentials(),
            nbd_inode,
            &SetAttributes {
                size: SetSize::Set(PAYLOAD_SIZE as u64),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        let payload = |seed: u32| -> Vec<u8> {
            (0..PAYLOAD_SIZE / 32)
                .flat_map(|index| {
                    Sha256::digest([seed.to_le_bytes(), (index as u32).to_le_bytes()].concat())
                        .to_vec()
                })
                .collect()
        };
        let nfs_payload = payload(1);
        let nbd_payload = payload(2);
        nfs_io(Arc::clone(&fs), nfs_inode, &nfs_payload, true).await;
        nbd_io(Arc::clone(&fs), root.path(), &nbd_payload, true).await;
        let before = pool.placements().await.unwrap();
        assert!(
            before.len() >= 2,
            "protocol durability barriers must seal real pool segments"
        );
        assert!(
            before
                .iter()
                .all(|placement| placement.member_id == "disk-a")
        );
        let hashes = before
            .iter()
            .map(|placement| {
                let body = std::fs::read(root.path().join("disk-a").join(&placement.physical_key))
                    .unwrap();
                (placement.location.clone(), Sha256::digest(body))
            })
            .collect::<Vec<_>>();
        pool.quarantine("disk-a", "export regression health signal")
            .await
            .unwrap();
        pool.evacuate_pass(&CancellationToken::new()).await.unwrap();
        let after = pool.placements().await.unwrap();
        assert_eq!(before.len(), after.len());
        assert!(
            after
                .iter()
                .all(|placement| placement.member_id == "disk-b")
        );
        for placement in &after {
            let expected = &hashes
                .iter()
                .find(|(location, _)| location == &placement.location)
                .unwrap()
                .1;
            let body =
                std::fs::read(root.path().join("disk-b").join(&placement.physical_key)).unwrap();
            assert_eq!(&Sha256::digest(body), expected);
        }
        assert!(pool.status().await.unwrap().members[0].safe_to_remove);
        close(fs, pool).await;
        let (fs, pool) = open(&settings, false).await;
        assert_eq!(
            serde_json::to_value(pool.placements().await.unwrap()).unwrap(),
            serde_json::to_value(&after).unwrap()
        );
        nfs_io(Arc::clone(&fs), nfs_inode, &nfs_payload, false).await;
        nbd_io(Arc::clone(&fs), root.path(), &nbd_payload, false).await;
        let status = pool.status().await.unwrap();
        assert!(status.members[0].safe_to_remove);
        let acceptance = serde_json::json!({
            "fixture_id": "fx-exports-unchanged", "result": "passed",
            "protocols": ["nfs3_tcp", "nbd_unix"], "kernel_mount": false,
            "locator_evacuation": true, "cold_reopen": true,
            "nfs_sha256": format!("{:x}", Sha256::digest(&nfs_payload)),
            "nbd_sha256": format!("{:x}", Sha256::digest(&nbd_payload)),
            "placements_before": before, "placements_after": after, "status": status
        });
        close(fs, pool).await;
        root.close().expect("remove pool export fixture");
        println!("POOL_EXPORTS_ACCEPTANCE {acceptance}");
    })
    .await
    .expect("pool-backed NFS/NBD wire regression exceeded its deadline");
}
