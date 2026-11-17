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
