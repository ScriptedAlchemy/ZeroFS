use super::*;
use crate::local_object_store::LocalConditionalStore;
use crate::writeback::anchored_dir::DIRECTORY_READS;
use futures::TryStreamExt;
use object_store::{ObjectStore, ObjectStoreExt};

#[test]
fn storage_readiness_startup_does_not_enumerate_object_entries() {
    let fixture = Fixture::new();
    let objects = fixture
        .path("member data")
        .join(".zerofs-pool/pool/objects");
    std::fs::create_dir_all(&objects).unwrap();
    for index in 0..4096 {
        std::fs::write(objects.join(format!("{index:032x}")), b"x").unwrap();
    }
    let before = DIRECTORY_READS.with(Cell::get);
    fixture.verify().unwrap();
    assert_eq!(DIRECTORY_READS.with(Cell::get) - before, 0);
}

#[test]
fn storage_readiness_startup_leaves_data_entry_symlinks_for_their_open_checks() {
    let mut fixture = Fixture::new();
    let pool_id = "00000000-0000-0000-0000-000000000001";
    for directory in [
        fixture.path("authority").join("manifest/nested"),
        fixture
            .path("member data")
            .join(format!(".zerofs-pool/{pool_id}/objects/nested")),
        fixture.path("journal").join("bucket_fixture/blobs/00"),
    ] {
        std::fs::create_dir_all(&directory).unwrap();
        std::os::unix::fs::symlink(directory.join("missing"), directory.join("unchecked")).unwrap();
    }
    for strict in [true, false] {
        fixture.document["storage"]["readiness"]["strict"] = json!(strict);
        fixture.verify().unwrap();
        verify_pool_paths(&fixture.parse().unwrap(), pool_id).unwrap();
    }
    verify_bucket_paths(&fixture.parse().unwrap(), "bucket_fixture").unwrap();
}

#[tokio::test]
async fn storage_readiness_listing_inspects_only_the_requested_subtree() {
    let fixture = Fixture::new();
    let root = fixture.path("member data");
    std::fs::create_dir(root.join("listed")).unwrap();
    std::fs::write(root.join("listed/object"), b"x").unwrap();
    std::os::unix::fs::symlink(root.join("missing"), root.join("unrelated")).unwrap();
    let store = LocalConditionalStore::new().with_readiness_root(root.clone());
    let prefix = object_store::path::Path::from_absolute_path(root.join("listed")).unwrap();
    assert_eq!(
        store
            .list(Some(&prefix))
            .try_collect::<Vec<_>>()
            .await
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        store
            .list_with_offset(Some(&prefix), &prefix)
            .try_collect::<Vec<_>>()
            .await
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        store
            .list_with_delimiter(Some(&prefix))
            .await
            .unwrap()
            .objects
            .len(),
        1
    );
}

#[tokio::test]
async fn storage_readiness_listing_checks_prefix_components_when_polled() {
    let fixture = Fixture::new();
    let root = fixture.path("member data");
    let directory = root.join("listed");
    std::fs::create_dir(&directory).unwrap();
    let prefix = object_store::path::Path::from_absolute_path(&directory).unwrap();
    let store = LocalConditionalStore::new().with_readiness_root(root.clone());
    let listing = store.list(Some(&prefix));
    let offset_listing = store.list_with_offset(Some(&prefix), &prefix);
    std::fs::rename(&directory, root.join("saved")).unwrap();
    let outside = tempfile::tempdir().unwrap();
    std::fs::write(outside.path().join("object"), b"outside").unwrap();
    std::os::unix::fs::symlink(outside.path(), &directory).unwrap();
    for result in [
        listing.try_collect::<Vec<_>>().await,
        offset_listing.try_collect::<Vec<_>>().await,
    ] {
        assert!(result.unwrap_err().to_string().contains("symlink"));
    }
    assert!(
        store
            .list_with_delimiter(Some(&prefix))
            .await
            .unwrap_err()
            .to_string()
            .contains("symlink")
    );
    assert_eq!(
        std::fs::read(outside.path().join("object")).unwrap(),
        b"outside"
    );
}

#[tokio::test]
async fn storage_readiness_object_directories_are_checked_on_operations_and_listings() {
    let fixture = Fixture::new();
    let root = fixture.path("member data");
    let outside = tempfile::tempdir().unwrap();
    std::fs::write(outside.path().join("object"), b"unchanged").unwrap();
    std::fs::create_dir(root.join("objects")).unwrap();
    std::os::unix::fs::symlink(outside.path(), root.join("objects/linked")).unwrap();
    let store = LocalConditionalStore::new().with_readiness_root(root.clone());
    let prefix = object_store::path::Path::from_absolute_path(root.join("objects")).unwrap();
    let linked = prefix.clone().join("linked");
    let object = linked.clone().join("object");
    assert!(
        store
            .get(&object)
            .await
            .unwrap_err()
            .to_string()
            .contains("symlink")
    );
    assert!(
        store
            .put(&object, "forbidden".into())
            .await
            .unwrap_err()
            .to_string()
            .contains("symlink")
    );
    for path in [&prefix, &linked] {
        assert!(
            store
                .list(Some(path))
                .try_collect::<Vec<_>>()
                .await
                .unwrap_err()
                .to_string()
                .contains("symlink")
        );
        assert!(
            store
                .list_with_offset(Some(path), &object)
                .try_collect::<Vec<_>>()
                .await
                .unwrap_err()
                .to_string()
                .contains("symlink")
        );
        assert!(
            store
                .list_with_delimiter(Some(path))
                .await
                .unwrap_err()
                .to_string()
                .contains("symlink")
        );
    }
    assert_eq!(
        std::fs::read(outside.path().join("object")).unwrap(),
        b"unchanged"
    );
}

#[tokio::test]
async fn storage_readiness_listing_preserves_native_metadata_and_hidden_files() {
    let fixture = Fixture::new();
    let root = fixture.path("member data");
    std::fs::create_dir(root.join("directory#1")).unwrap();
    for name in ["a", "b", "hidden#1", "visible#x", "directory#1/child%2F"] {
        std::fs::write(root.join(name), b"payload").unwrap();
    }
    let native = LocalConditionalStore::new();
    let checked = LocalConditionalStore::new().with_readiness_root(root.clone());
    let prefix = object_store::path::Path::from_absolute_path(root).unwrap();
    let mut expected = native
        .list(Some(&prefix))
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    let mut actual = checked.list(None).try_collect::<Vec<_>>().await.unwrap();
    expected.sort_by(|a, b| a.location.cmp(&b.location));
    actual.sort_by(|a, b| a.location.cmp(&b.location));
    assert_eq!(actual, expected);
    let expected = native.list_with_delimiter(Some(&prefix)).await.unwrap();
    let actual = checked.list_with_delimiter(None).await.unwrap();
    assert_eq!(actual.common_prefixes, expected.common_prefixes);
    let mut objects = actual.objects;
    objects.sort_by(|a, b| a.location.cmp(&b.location));
    let mut expected = expected.objects;
    expected.sort_by(|a, b| a.location.cmp(&b.location));
    assert_eq!(objects, expected);
    let directory = prefix.join("directory#1");
    assert_eq!(
        checked
            .list(Some(&directory))
            .try_collect::<Vec<_>>()
            .await
            .unwrap(),
        native
            .list(Some(&directory))
            .try_collect::<Vec<_>>()
            .await
            .unwrap(),
    );
}

#[tokio::test]
#[ignore = "Linux-only local diagnostic with one million files"]
async fn measure_storage_readiness_scan() {
    assert!(cfg!(target_os = "linux"));
    assert_eq!(std::env::var("ZEROFS_READINESS_SCAN_MEASURE").unwrap(), "1");
    let _ = tracing_subscriber::fmt().with_test_writer().try_init();
    let mut fixture = Fixture::new();
    fixture.document["storage"]["readiness"]["strict"] = json!(false);
    let settings = fixture.parse().unwrap();
    let root = fixture.path("member data");
    let objects = root.join(".zerofs-pool/00000000-0000-0000-0000-000000000001/objects");
    std::fs::create_dir_all(&objects).unwrap();
    let mut progress = std::time::Instant::now();
    for index in 0..1_000_000_u128 {
        std::fs::write(
            objects.join(uuid::Uuid::from_u128(index + 1).to_string()),
            b"x",
        )
        .unwrap();
        if progress.elapsed().as_secs() >= 5 {
            eprintln!("Created {} of 1000000 synthetic object files.", index + 1);
            progress = std::time::Instant::now();
        }
    }
    let started = std::time::Instant::now();
    verify(&settings).unwrap();
    let preflight_seconds = started.elapsed().as_secs_f64();
    let store = LocalConditionalStore::new().with_readiness_root(root);
    let prefix = object_store::path::Path::from_absolute_path(&objects).unwrap();
    let started = std::time::Instant::now();
    let count = store
        .list(Some(&prefix))
        .try_fold(0_u64, |count, _| async move { Ok(count + 1) })
        .await
        .unwrap();
    let listing_seconds = started.elapsed().as_secs_f64();
    assert_eq!(count, 1_000_000);
    let scratch = fixture._temp.path().to_path_buf();
    drop(fixture);
    assert!(!scratch.exists());
    println!(
        "READINESS_SCAN_MEASUREMENT {}",
        json!({
            "objects": count, "payload_bytes_per_file": 1,
            "preflight_seconds": preflight_seconds, "listing_seconds": listing_seconds,
            "strict": false, "cleanup_verified": true,
            "storage": "Docker Desktop Linux virtual disk on an SSD",
            "profile": "test with debug information disabled"
        })
    );
}
