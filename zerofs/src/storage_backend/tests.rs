//! Exercise the real configuration-to-backend path with separate local disks.
//! Pool state-machine fault injection lives in pool_object_store::tests.

use super::{open_pool_storage, open_storage};
use crate::config::Settings;
use crate::pool_object_store::{CATALOG_FILE_NAME, PoolObjectStore};
use crate::segment::Segid;
use crate::segment_store::GeneratedSegmentCreate;
use crate::storage_compatibility::check_if_match_support;
use bytes::Bytes;
use futures::TryStreamExt;
use object_store::local::LocalFileSystem;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt, PutMode, PutOptions};
use std::path::{Path as FsPath, PathBuf};
use std::sync::Arc;

struct Fixture {
    root: tempfile::TempDir,
    settings: Settings,
    metadata: PathBuf,
    members: [PathBuf; 2],
}

fn file_url(path: &FsPath) -> String {
    url::Url::from_directory_path(path).unwrap().to_string()
}

impl Fixture {
    fn new(first_capacity: u64, second_capacity: u64) -> Self {
        let root = tempfile::tempdir().unwrap();
        let metadata = root.path().join("metadata");
        let members = [root.path().join("disk-a"), root.path().join("disk-b")];
        for directory in [&metadata, &members[0], &members[1]] {
            std::fs::create_dir(directory).unwrap();
        }
        // Serialization supplies correct quoting for Windows paths too. These
        // tests do not configure cloud credentials, SSH keys, or remote services.
        let document = serde_json::json!({
            "cache": {
                "dir": root.path().join("clean-cache"),
                "disk_size_gb": 0.001,
            },
            "storage": {
                "url": file_url(&metadata),
                "encryption_password": "local-pool-test-password",
                "pool": {
                    "catalog_dir": root.path().join("catalog"),
                    "members": [
                        {
                            "id": "disk-a",
                            "url": file_url(&members[0]),
                            "capacity_gb": first_capacity as f64 / 1_000_000_000.0,
                            "reserve_free_gb": 0.0,
                        },
                        {
                            "id": "disk-b",
                            "url": file_url(&members[1]),
                            "capacity_gb": second_capacity as f64 / 1_000_000_000.0,
                            "reserve_free_gb": 0.0,
                        },
                    ],
                },
            },
            "servers": {},
            "telemetry": { "enabled": false },
        });
        let config = root.path().join("config.toml");
        std::fs::write(&config, toml::to_string(&document).unwrap()).unwrap();
        let settings = Settings::from_file(config).unwrap();
        Self {
            root,
            settings,
            metadata,
            members,
        }
    }

    fn catalog(&self) -> &FsPath {
        &self.settings.storage.pool.as_ref().unwrap().catalog_dir
    }

    fn reload(&self, settings: &Settings) -> anyhow::Result<Settings> {
        let config = self.root.path().join("config.toml");
        std::fs::write(&config, toml::to_string(settings)?)?;
        Settings::from_file(config)
    }
}

fn segment(prefix: &Path, counter: u64) -> Path {
    let key = Segid::new(97, counter).object_key();
    if prefix.as_ref().is_empty() {
        Path::from(key)
    } else {
        Path::from(format!("{}/{key}", prefix.as_ref()))
    }
}

async fn create_segment(store: &dyn ObjectStore, path: &Path, payload: &'static [u8]) {
    let mut options = PutOptions::from(PutMode::Create);
    options.extensions.insert(GeneratedSegmentCreate);
    store
        .put_opts(path, Bytes::from_static(payload).into(), options)
        .await
        .unwrap();
}

async fn read(store: &dyn ObjectStore, path: &Path) -> Bytes {
    store.get(path).await.unwrap().bytes().await.unwrap()
}

async fn member_for(pool: &PoolObjectStore, path: &Path) -> String {
    pool.placements()
        .await
        .unwrap()
        .into_iter()
        .find(|placement| placement.location == *path)
        .expect("segment must have a placement")
        .member_id
}

/// Inspect physical member files independently of the pool's placement lookup.
/// Identity markers are allowed, but a payload must occur on exactly its disk.
async fn copies_on_disk(directory: &FsPath, payload: &[u8]) -> usize {
    let disk = LocalFileSystem::new_with_prefix(directory).unwrap();
    let objects = disk.list(None).try_collect::<Vec<_>>().await.unwrap();
    let mut copies = 0;
    for object in objects {
        if object.size == payload.len() as u64
            && read(&disk, &object.location).await.as_ref() == payload
        {
            copies += 1;
        }
    }
    copies
}

#[tokio::test]
async fn native_file_backend_passes_the_real_startup_compatibility_probe() {
    let fixture = Fixture::new(8, 16);
    let mut settings = fixture.settings.clone();
    settings.storage.pool = None;
    let settings = fixture.reload(&settings).unwrap();
    let opened = open_storage(&settings).await.unwrap();
    assert!(opened.sftp_pool.is_none());
    let store: Arc<dyn ObjectStore> = Arc::from(opened.store);
    check_if_match_support(&store, opened.path.as_ref())
        .await
        .unwrap();
    let metadata = opened.path.clone().join("manifest").join("current");
    store.put(&metadata, "metadata".into()).await.unwrap();
    assert_eq!(
        std::fs::read(fixture.metadata.join("manifest/current")).unwrap(),
        b"metadata",
    );
    assert!(!fixture.catalog().exists());
}

#[tokio::test]
async fn initialized_local_pool_needs_no_cloud_credentials_or_remote_sessions() {
    let fixture = Fixture::new(8, 16);
    let (opened, pool) = open_pool_storage(&fixture.settings, true).await.unwrap();
    assert!(opened.sftp_pool.is_none());
    let store: Arc<dyn ObjectStore> = Arc::from(opened.store);
    check_if_match_support(&store, opened.path.as_ref())
        .await
        .unwrap();
    let metadata = opened.path.clone().join("manifest").join("current");
    store.put(&metadata, "metadata".into()).await.unwrap();
    assert_eq!(
        std::fs::read(fixture.metadata.join("manifest/current")).unwrap(),
        b"metadata",
    );
    let status = pool.status().await.unwrap();
    assert_eq!(status.mapped_objects, 0);
    assert!(status.members.iter().all(|member| member.used_bytes == 0));
    assert!(fixture.catalog().join(CATALOG_FILE_NAME).is_file());
    // Initializing a local member may persist its identity and lock metadata,
    // but opening filesystem metadata must not create any segment bodies.
    let data_prefix = Path::from(format!(".zerofs-pool/{}/objects", status.pool_id));
    for directory in &fixture.members {
        let disk = LocalFileSystem::new_with_prefix(directory).unwrap();
        assert!(
            disk.list(Some(&data_prefix))
                .try_collect::<Vec<_>>()
                .await
                .unwrap()
                .is_empty()
        );
    }
}

#[tokio::test]
async fn configured_local_disks_spill_at_capacity_and_reopen_with_the_same_placements() {
    let fixture = Fixture::new(8, 16);
    let (opened, pool) = open_pool_storage(&fixture.settings, true).await.unwrap();
    let first = segment(&opened.path, 1);
    let second = segment(&opened.path, 2);
    create_segment(opened.store.as_ref(), &first, b"12345678").await;
    create_segment(opened.store.as_ref(), &second, b"abcd").await;
    assert_eq!(member_for(&pool, &first).await, "disk-a");
    assert_eq!(member_for(&pool, &second).await, "disk-b");
    let before = pool.status().await.unwrap();
    assert_eq!(before.mapped_objects, 2);
    let a = before
        .members
        .iter()
        .find(|member| member.id == "disk-a")
        .unwrap();
    let b = before
        .members
        .iter()
        .find(|member| member.id == "disk-b")
        .unwrap();
    assert_eq!(
        (a.capacity_bytes, a.used_bytes, a.reserved_bytes),
        (8, 8, 0)
    );
    assert_eq!(
        (b.capacity_bytes, b.used_bytes, b.reserved_bytes),
        (16, 4, 0)
    );
    assert_eq!(copies_on_disk(&fixture.members[0], b"12345678").await, 1);
    assert_eq!(copies_on_disk(&fixture.members[1], b"12345678").await, 0);
    assert_eq!(copies_on_disk(&fixture.members[0], b"abcd").await, 0);
    assert_eq!(copies_on_disk(&fixture.members[1], b"abcd").await, 1);
    drop(opened);
    drop(pool);

    let (reopened, pool) = open_pool_storage(&fixture.settings, false).await.unwrap();
    assert_eq!(pool.status().await.unwrap().pool_id, before.pool_id);
    assert_eq!(member_for(&pool, &first).await, "disk-a");
    assert_eq!(member_for(&pool, &second).await, "disk-b");
    assert_eq!(read(reopened.store.as_ref(), &first).await, "12345678");
    assert_eq!(read(reopened.store.as_ref(), &second).await, "abcd");
}

#[tokio::test]
async fn ordinary_open_requires_initialization_and_never_recreates_a_lost_catalog() {
    let fixture = Fixture::new(8, 16);
    let missing = open_storage(&fixture.settings)
        .await
        .expect_err("a new pool must be initialized explicitly");
    assert!(format!("{missing:#}").contains("catalog is missing"));
    assert!(!fixture.catalog().exists());

    let (opened, pool) = open_pool_storage(&fixture.settings, true).await.unwrap();
    let path = segment(&opened.path, 1);
    create_segment(opened.store.as_ref(), &path, b"retained").await;
    let pool_id = pool.status().await.unwrap().pool_id;
    drop(opened);
    drop(pool);
    let backup = fixture.root.path().join("saved-catalog");
    std::fs::rename(fixture.catalog(), &backup).unwrap();

    assert!(open_storage(&fixture.settings).await.is_err());
    let reinitialize = open_pool_storage(&fixture.settings, true)
        .await
        .expect_err("an existing authority must not accept a replacement catalog");
    assert!(format!("{reinitialize:#}").contains("catalog is missing"));
    assert!(!fixture.catalog().exists());
    assert_eq!(copies_on_disk(&fixture.members[0], b"retained").await, 1);

    std::fs::rename(&backup, fixture.catalog()).unwrap();
    let (restored, pool) = open_pool_storage(&fixture.settings, false).await.unwrap();
    assert_eq!(pool.status().await.unwrap().pool_id, pool_id);
    assert_eq!(read(restored.store.as_ref(), &path).await, "retained");
}

#[tokio::test]
async fn adding_a_configured_disk_preserves_existing_data_and_uses_new_capacity() {
    let fixture = Fixture::new(8, 16);
    let mut initial_settings = fixture.settings.clone();
    initial_settings
        .storage
        .pool
        .as_mut()
        .unwrap()
        .members
        .truncate(1);
    let initial_settings = fixture.reload(&initial_settings).unwrap();
    let (opened, pool) = open_pool_storage(&initial_settings, true).await.unwrap();
    let first = segment(&opened.path, 1);
    create_segment(opened.store.as_ref(), &first, b"original").await;
    let old_version = opened.store.head(&first).await.unwrap().e_tag;
    let pool_id = pool.status().await.unwrap().pool_id;
    drop(opened);
    drop(pool);

    let mut expanded_settings = fixture.settings.clone();
    // Preparing provider settings for later cloud expansion must not change
    // the identity of existing local disks or trigger a remote connection.
    expanded_settings.aws = Some(
        toml::from_str(
            "endpoint = 'https://unused-provider.invalid'\ndefault_region = 'us-east-1'\n",
        )
        .unwrap(),
    );
    let expanded_settings = fixture.reload(&expanded_settings).unwrap();
    let (opened, pool) = open_pool_storage(&expanded_settings, false).await.unwrap();
    assert_eq!(pool.status().await.unwrap().pool_id, pool_id);
    assert_eq!(opened.store.head(&first).await.unwrap().e_tag, old_version);
    assert_eq!(read(opened.store.as_ref(), &first).await, "original");
    assert_eq!(member_for(&pool, &first).await, "disk-a");
    let second = segment(&opened.path, 2);
    create_segment(opened.store.as_ref(), &second, b"expanded").await;
    assert_eq!(member_for(&pool, &second).await, "disk-b");
    assert_eq!(copies_on_disk(&fixture.members[0], b"original").await, 1);
    assert_eq!(copies_on_disk(&fixture.members[1], b"expanded").await, 1);
}

async fn assert_overlap_rejected(settings: &Settings, name: &str) {
    let error = open_pool_storage(settings, true)
        .await
        .expect_err("overlapping permanent and evictable roots must be rejected");
    let message = format!("{error:#}");
    assert!(message.contains("must not overlap"), "{message}");
    assert!(message.contains(name), "{message}");
}

#[tokio::test]
async fn metadata_catalog_members_and_clean_cache_cannot_share_a_directory_tree() {
    let fixture = Fixture::new(8, 16);
    let mut settings = fixture.settings.clone();
    settings.storage.pool.as_mut().unwrap().members[0].url = settings.storage.url.clone();
    assert_overlap_rejected(&settings, "metadata").await;

    let mut settings = fixture.settings.clone();
    settings.storage.pool.as_mut().unwrap().catalog_dir = settings.cache.dir.join("catalog");
    assert_overlap_rejected(&settings, "catalog").await;

    let mut settings = fixture.settings.clone();
    settings.cache.dir = fixture.members[0].join("not-yet-created-cache");
    assert_overlap_rejected(&settings, "clean cache").await;
    assert!(!fixture.members[0].join("not-yet-created-cache").exists());
    assert!(!fixture.catalog().exists());
}

#[cfg(unix)]
#[tokio::test]
async fn symlink_aliases_cannot_bypass_member_or_cache_overlap_checks() {
    let fixture = Fixture::new(8, 16);
    let alias = fixture.root.path().join("disk-alias");
    std::os::unix::fs::symlink(&fixture.members[0], &alias).unwrap();
    let mut settings = fixture.settings.clone();
    settings.storage.pool.as_mut().unwrap().members[1].url = file_url(&alias);
    assert_overlap_rejected(&settings, "member disk-a").await;

    let mut settings = fixture.settings.clone();
    settings.cache.dir = alias.join("future-cache");
    assert_overlap_rejected(&settings, "clean cache").await;
    assert!(!fixture.members[0].join("future-cache").exists());
    assert!(!fixture.catalog().exists());
}

#[test]
fn invalid_member_ids_and_capacity_budgets_are_rejected_during_settings_validation() {
    let fixture = Fixture::new(8, 16);
    for id in ["", "../disk", "authority", "disk-b"] {
        let mut settings = fixture.settings.clone();
        settings.storage.pool.as_mut().unwrap().members[0].id = id.to_owned();
        assert!(
            fixture.reload(&settings).is_err(),
            "accepted invalid member id {id:?}"
        );
    }
    for capacity in [0.0, -1.0, f64::NAN, f64::INFINITY] {
        let mut settings = fixture.settings.clone();
        settings.storage.pool.as_mut().unwrap().members[0].capacity_gb = capacity;
        let error = settings.validate().unwrap_err();
        assert!(format!("{error:#}").contains("capacity_gb"));
    }
    let mut settings = fixture.settings.clone();
    settings.storage.pool.as_mut().unwrap().members[0].reserve_free_gb = -1.0;
    assert!(format!("{:#}", settings.validate().unwrap_err()).contains("reserve_free_gb"));
    assert!(!fixture.catalog().exists());
}

#[test]
fn unsupported_member_schemes_and_remote_metadata_are_rejected_before_opening_storage() {
    let fixture = Fixture::new(8, 16);
    for url in [
        "memory:///transient",
        "https://example.invalid/data",
        "ftp://example.invalid/data",
    ] {
        let mut settings = fixture.settings.clone();
        settings.storage.pool.as_mut().unwrap().members[0].url = url.to_owned();
        assert!(
            fixture.reload(&settings).is_err(),
            "accepted unsupported member URL {url}"
        );
    }
    let mut settings = fixture.settings.clone();
    settings.storage.url = "s3://metadata-must-stay-local/data".to_owned();
    let error = fixture.reload(&settings).unwrap_err();
    assert!(format!("{error:#}").contains("authoritative metadata"));
    assert!(!fixture.catalog().exists());
}
