//! A local I/O error on the metadata store must end in a bounded failure that
//! names the file and the errno, never in an unbounded retry.
//!
//! These tests drive the real startup path over a pooled local configuration.
//! `fault_store::native_errno` makes the configured local stores return a
//! native errno for chosen object names, the way a bad block under an SST does.

use super::init::initialize_filesystem;
use super::server::{DatabaseMode, InitResult};
use crate::config::Settings;
use crate::fault_store::native_errno::{self, Operation};
use crate::fs::permissions::Credentials;
use crate::fs::types::{AuthContext, SetAttributes};
use bytes::Bytes;
use std::path::PathBuf;
use std::time::Duration;

/// Startup either serves or fails well inside this bound; the retry loop it
/// replaces never ends.
const BOUND: Duration = Duration::from_secs(30);

struct Fixture {
    _root: tempfile::TempDir,
    metadata: PathBuf,
    cache: PathBuf,
    settings: Settings,
}

impl Fixture {
    /// A pooled volume with one file written, made durable, and closed.
    async fn with_one_file() -> Self {
        let root = tempfile::tempdir().unwrap();
        let base = root.path().canonicalize().unwrap();
        for directory in ["metadata", "disk-a", "disk-b"] {
            std::fs::create_dir_all(base.join(directory)).unwrap();
        }
        let uri = |name: &str| {
            url::Url::from_directory_path(base.join(name))
                .unwrap()
                .to_string()
        };
        let document = serde_json::json!({
            "cache": {"dir": base.join("cache"), "disk_size_gb": 0.001, "memory_size_gb": 0.01},
            "storage": {
                "url": uri("metadata"),
                "encryption_password": "metadata-fault-fixture",
                "pool": {
                    "catalog_dir": base.join("catalog"),
                    "members": [
                        {"id": "disk-a", "url": uri("disk-a"), "capacity_gb": 0.008, "reserve_free_gb": 0.0},
                        {"id": "disk-b", "url": uri("disk-b"), "capacity_gb": 0.008, "reserve_free_gb": 0.0}
                    ]
                }
            },
            "servers": {},
            "telemetry": {"enabled": false}
        });
        let config = base.join("zerofs.toml");
        std::fs::write(&config, toml::to_string(&document).unwrap()).unwrap();
        let settings = Settings::from_file(config).unwrap();
        drop(
            crate::storage_backend::open_pool_storage(&settings, true)
                .await
                .unwrap(),
        );

        let fixture = Self {
            metadata: base.join("metadata"),
            cache: base.join("cache"),
            _root: root,
            settings,
        };
        let initialized = fixture.start().await.unwrap();
        let auth = AuthContext::default();
        let creds = Credentials::from_auth_context(&auth);
        let (file, _) = initialized
            .fs
            .create(&creds, 0, b"kept", &SetAttributes::default())
            .await
            .unwrap();
        initialized
            .fs
            .write(&auth, file, 0, &Bytes::from_static(b"acknowledged bytes"))
            .await
            .unwrap();
        let _receipt = initialized
            .fs
            .administrative_remote_durability()
            .await
            .unwrap();
        close(initialized).await;
        fixture
    }

    async fn start(&self) -> anyhow::Result<InitResult> {
        initialize_filesystem(&self.settings, DatabaseMode::ReadWrite).await
    }

    /// Drop the clean caches so the next start reads metadata from the store.
    fn cold(&self) {
        std::fs::remove_dir_all(&self.cache).unwrap();
    }
}

async fn close(initialized: InitResult) {
    let InitResult { fs, pool, .. } = initialized;
    fs.stop_new_mutation_admission();
    fs.stop_mutation_workers().await.unwrap();
    fs.flush_coordinator.close().await.unwrap();
    let worker = fs.write_coordinator.take_worker_for_tests();
    drop(fs);
    worker.await.unwrap();
    if let Some(pool) = pool {
        pool.settle().await;
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn sst_read_eio_at_startup_fails_naming_the_file_and_errno() {
    let fixture = Fixture::with_one_file().await;
    fixture.cold();
    let _eio = native_errno::arm(&fixture.metadata, Operation::Read, ".sst", libc::EIO, None);

    let outcome = tokio::time::timeout(BOUND, fixture.start())
        .await
        .unwrap_or_else(|_| {
            panic!(
                "startup neither served nor failed within {BOUND:?}: \
                 it is retrying the EIO without limit"
            )
        });
    let error = match outcome {
        Ok(_) => panic!("startup served although every SST read fails with EIO"),
        Err(error) => format!("{error:#}"),
    };
    assert!(
        error.contains(&format!("{}/compacted/", fixture.metadata.display()))
            && error.contains(".sst"),
        "the error must name the unreadable SST: {error}"
    );
    assert!(
        error.contains("EIO"),
        "the error must name the errno: {error}"
    );
}

/// Wait briefly for a fault that must not trip.
async fn assert_untripped(initialized: &InitResult) {
    if let Ok(error) =
        tokio::time::timeout(Duration::from_secs(2), initialized.metadata_fault.tripped()).await
    {
        panic!("the metadata store must keep serving, but it reported: {error:#}");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn sst_read_eio_while_serving_reports_the_file_and_errno() {
    let fixture = Fixture::with_one_file().await;
    let initialized = fixture.start().await.unwrap();
    let auth = AuthContext::default();
    let creds = Credentials::from_auth_context(&auth);
    let eio = native_errno::arm(&fixture.metadata, Operation::Read, ".sst", libc::EIO, None);

    // Each durable flush adds an L0 SST. The serving compactor reads L0 SSTs
    // from the store, past every cache, once enough of them accumulate.
    let served = tokio::time::timeout(
        BOUND,
        initialized.metadata_fault.bound(async {
            for index in 0..12_u32 {
                let name = format!("flush-{index}");
                let (file, _) = initialized
                    .fs
                    .create(&creds, 0, name.as_bytes(), &SetAttributes::default())
                    .await
                    .map_err(|error| anyhow::anyhow!("create failed: {error:?}"))?;
                initialized
                    .fs
                    .write(&auth, file, 0, &Bytes::from(name.into_bytes()))
                    .await
                    .map_err(|error| anyhow::anyhow!("write failed: {error:?}"))?;
                let _receipt = initialized
                    .fs
                    .administrative_remote_durability()
                    .await
                    .map_err(|error| anyhow::anyhow!("flush failed: {error:?}"))?;
            }
            std::future::pending::<anyhow::Result<()>>().await
        }),
    )
    .await
    .unwrap_or_else(|_| {
        panic!("a serving metadata read EIO did not end in a failure within {BOUND:?}")
    });
    let error = format!(
        "{:#}",
        served.expect_err("only the metadata fault ends this wait")
    );
    assert!(
        error.contains(&format!("{}/compacted/", fixture.metadata.display()))
            && error.contains(".sst")
            && error.contains("metadata store read")
            && error.contains("EIO"),
        "the failure must name the SST read and the errno: {error}"
    );
    // A server exits here. In process, clear the fault so SlateDB's retry ends
    // and the database closes before its directory is removed.
    drop(eio);
    close(initialized).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn sst_fsync_eio_fails_the_flush_instead_of_blocking_it() {
    let fixture = Fixture::with_one_file().await;
    let initialized = fixture.start().await.unwrap();
    let auth = AuthContext::default();
    let creds = Credentials::from_auth_context(&auth);
    let (file, _) = initialized
        .fs
        .create(&creds, 0, b"pending", &SetAttributes::default())
        .await
        .unwrap();
    initialized
        .fs
        .write(&auth, file, 0, &Bytes::from_static(b"not yet durable"))
        .await
        .unwrap();
    let eio = native_errno::arm(&fixture.metadata, Operation::Write, ".sst", libc::EIO, None);

    let flushed = tokio::time::timeout(
        BOUND,
        initialized.metadata_fault.bound(async {
            initialized
                .fs
                .administrative_remote_durability()
                .await
                .map_err(|error| anyhow::anyhow!("flush failed: {error:?}"))
        }),
    )
    .await
    .unwrap_or_else(|_| panic!("the flush blocked for {BOUND:?} on an SST fsync EIO"));
    let error = format!(
        "{:#}",
        flushed.expect_err("an SST that cannot be made durable must fail the flush")
    );
    assert!(
        error.contains(&format!("{}/compacted/", fixture.metadata.display()))
            && error.contains(".sst")
            && error.contains("EIO"),
        "the failure must name the SST and the errno: {error}"
    );
    drop(eio);
    close(initialized).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn sst_write_enospc_waits_for_space_instead_of_failing() {
    let fixture = Fixture::with_one_file().await;
    let initialized = fixture.start().await.unwrap();
    let auth = AuthContext::default();
    let creds = Credentials::from_auth_context(&auth);
    let (file, _) = initialized
        .fs
        .create(&creds, 0, b"waits", &SetAttributes::default())
        .await
        .unwrap();
    initialized
        .fs
        .write(
            &auth,
            file,
            0,
            &Bytes::from_static(b"durable once space returns"),
        )
        .await
        .unwrap();
    // The disk is full for three SST writes, then space returns.
    let _enospc = native_errno::arm(
        &fixture.metadata,
        Operation::Write,
        ".sst",
        libc::ENOSPC,
        Some(3),
    );

    let _receipt = tokio::time::timeout(BOUND, initialized.fs.administrative_remote_durability())
        .await
        .expect("the flush must finish once space returns")
        .expect("ENOSPC is transient: the flush must succeed after space returns");
    assert_untripped(&initialized).await;
    close(initialized).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn data_member_read_eio_fails_the_read_and_keeps_serving() {
    let fixture = Fixture::with_one_file().await;
    let initialized = fixture.start().await.unwrap();
    let auth = AuthContext::default();
    let creds = Credentials::from_auth_context(&auth);
    let (file, _) = initialized
        .fs
        .create(&creds, 0, b"on-a-member", &SetAttributes::default())
        .await
        .unwrap();
    let payload = Bytes::from(vec![0x5a; 1024 * 1024]);
    initialized
        .fs
        .write(&auth, file, 0, &payload)
        .await
        .unwrap();
    let _receipt = initialized
        .fs
        .administrative_remote_durability()
        .await
        .unwrap();
    close(initialized).await;
    fixture.cold();

    let initialized = fixture.start().await.unwrap();
    let base = fixture.metadata.parent().unwrap();
    let _members = [
        native_errno::arm(base.join("disk-a"), Operation::Read, "", libc::EIO, None),
        native_errno::arm(base.join("disk-b"), Operation::Read, "", libc::EIO, None),
    ];

    let read = tokio::time::timeout(
        BOUND,
        initialized
            .fs
            .read_file(&auth, file, 0, payload.len() as u32),
    )
    .await
    .unwrap_or_else(|_| panic!("a member read EIO did not reach the reader within {BOUND:?}"));
    assert!(
        read.is_err(),
        "an unreadable member must fail the read, got {} bytes",
        read.map(|(bytes, _)| bytes.len()).unwrap_or_default()
    );
    // The pool records the unreadable segment against its member, as it did
    // before the metadata store classified its own errors.
    let status = initialized.pool.as_ref().unwrap().status().await.unwrap();
    assert!(
        status
            .unreadable
            .iter()
            .any(|fault| fault.member_id == "disk-a" || fault.member_id == "disk-b"),
        "the pool must record the unreadable segment: {:?}",
        status.unreadable
    );
    assert!(
        initialized.fs.lookup(&creds, 0, b"kept").await.is_ok(),
        "metadata must keep serving after a data member fails"
    );
    assert_untripped(&initialized).await;
    close(initialized).await;
}
