use super::{GenerationState, PoolObjectCreate, remove};
use crate::local_object_store::{LOCK_FILE, LocalConditionalStore, lock_parents};
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt, PutMode, PutOptions};
use std::path::PathBuf;
use std::time::Duration;

fn fixture() -> (tempfile::TempDir, PathBuf, String) {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path().canonicalize().unwrap();
    let key = format!(
        ".zerofs-pool/{}/objects/{}",
        uuid::Uuid::new_v4(),
        uuid::Uuid::new_v4()
    );
    std::fs::create_dir_all(root.join(&key).parent().unwrap()).unwrap();
    (directory, root, key)
}

fn pool_options() -> PutOptions {
    let mut options = PutOptions::from(PutMode::Create);
    options.extensions.insert(PoolObjectCreate);
    options
}

#[tokio::test]
async fn pool_create_uses_durable_rename_without_changing_create_semantics() {
    let (_directory, root, key) = fixture();
    let store = LocalConditionalStore::new();
    let path = Path::from_absolute_path(root.join(&key)).unwrap();
    store
        .put_opts(&path, "owned".into(), pool_options())
        .await
        .unwrap();
    assert_eq!(std::fs::read(root.join(&key)).unwrap(), b"owned");
    assert!(!root.join(format!("{key}#1")).exists());
    assert!(matches!(
        store
            .put_opts(&path, "replacement".into(), pool_options())
            .await,
        Err(object_store::Error::AlreadyExists { .. })
    ));
    assert_eq!(
        store.get(&path).await.unwrap().bytes().await.unwrap(),
        "owned"
    );
    assert_eq!(
        std::fs::read_dir(root.join(&key).parent().unwrap())
            .unwrap()
            .count(),
        2,
        "only the published object and stable mutation lock remain"
    );
}

#[tokio::test]
async fn pool_create_rejects_preexisting_staging_and_invalid_namespace() {
    let (_directory, root, key) = fixture();
    let store = LocalConditionalStore::new();
    let path = Path::from_absolute_path(root.join(&key)).unwrap();
    let staging = root.join(format!("{key}#1"));
    std::fs::write(&staging, "interrupted").unwrap();
    assert!(matches!(
        store.put_opts(&path, "retry".into(), pool_options()).await,
        Err(object_store::Error::AlreadyExists { .. })
    ));
    assert_eq!(std::fs::read(&staging).unwrap(), b"interrupted");
    assert!(!root.join(&key).exists());
    assert!(!root.join(format!("{key}#2")).exists());
    remove(&root, &key, GenerationState::Unpublished)
        .await
        .unwrap();
    assert!(!staging.exists());

    let ordinary = Path::from_absolute_path(root.join("ordinary")).unwrap();
    assert!(
        store
            .put_opts(&ordinary, "body".into(), pool_options())
            .await
            .is_err()
    );
    assert!(!root.join("ordinary").exists());
}

#[tokio::test]
async fn pool_cleanup_removes_only_the_owned_generation_and_keeps_live_multipart() {
    let (_directory, root, key) = fixture();
    let path = root.join(&key);
    let parent = path.parent().unwrap();
    let object = path.file_name().unwrap().to_str().unwrap();
    let mut owned = vec![path.clone()];
    for suffix in ["1", "3", "14"] {
        owned.push(parent.join(format!("{object}#{suffix}")));
    }
    for path in &owned {
        std::fs::write(path, "owned").unwrap();
    }
    let unrelated = [
        format!("{object}#"),
        format!("{object}#0"),
        format!("{object}#01"),
        format!("{object}#1.backup"),
        format!("{object}-other#1"),
        format!("{}#1", uuid::Uuid::new_v4()),
    ];
    for name in &unrelated {
        std::fs::write(parent.join(name), "unrelated").unwrap();
    }
    let store = LocalConditionalStore::new();
    let ordinary = Path::from_absolute_path(parent.join("ordinary")).unwrap();
    let mut multipart = store.put_multipart(&ordinary).await.unwrap();
    multipart.put_part("live multipart".into()).await.unwrap();

    remove(&root, &key, GenerationState::Unpublished)
        .await
        .unwrap();
    assert!(owned.iter().all(|path| !path.exists()));
    for name in unrelated {
        assert_eq!(std::fs::read(parent.join(name)).unwrap(), b"unrelated");
    }
    assert!(parent.join(LOCK_FILE).is_file());
    multipart.complete().await.unwrap();
    assert_eq!(
        store.get(&ordinary).await.unwrap().bytes().await.unwrap(),
        "live multipart"
    );
    remove(&root, &key, GenerationState::Unpublished)
        .await
        .unwrap();
}

#[tokio::test]
async fn pool_published_cleanup_removes_legacy_hardlink_remainder() {
    let (_directory, root, key) = fixture();
    let final_path = root.join(&key);
    let staging = root.join(format!("{key}#1"));
    let unrelated = final_path
        .parent()
        .unwrap()
        .join(format!("{}#1", uuid::Uuid::new_v4()));
    std::fs::write(&final_path, "published").unwrap();
    std::fs::write(&unrelated, "unrelated").unwrap();
    std::fs::hard_link(&final_path, &staging).unwrap();
    remove(&root, &key, GenerationState::Published)
        .await
        .unwrap();
    assert!(!final_path.exists());
    assert!(!staging.exists());
    assert_eq!(std::fs::read(unrelated).unwrap(), b"unrelated");
    assert!(final_path.parent().unwrap().join(LOCK_FILE).is_file());
    remove(&root, &key, GenerationState::Published)
        .await
        .unwrap();
}

#[tokio::test]
async fn pool_cleanup_waits_for_native_writer_before_removing_final_and_stage() {
    let (_directory, root, key) = fixture();
    let path = root.join(&key);
    let locks = lock_parents(std::slice::from_ref(&path)).unwrap();
    let cleaning_root = root.clone();
    let cleaning_key = key.clone();
    let mut cleaning = tokio::spawn(async move {
        remove(&cleaning_root, &cleaning_key, GenerationState::Unpublished).await
    });
    assert!(
        tokio::time::timeout(Duration::from_millis(100), &mut cleaning)
            .await
            .is_err()
    );
    // Model a native worker finishing after recovery began waiting for its lock.
    std::fs::write(&path, "late final").unwrap();
    std::fs::write(root.join(format!("{key}#1")), "late stage").unwrap();
    drop(locks);
    tokio::time::timeout(Duration::from_secs(5), cleaning)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(!path.exists());
    assert!(!root.join(format!("{key}#1")).exists());
}

#[tokio::test]
async fn pool_cleanup_rejects_symlinks_without_touching_their_targets() {
    let (_directory, root, key) = fixture();
    let outside = tempfile::tempdir().unwrap();
    let target = outside.path().join("untouched");
    std::fs::write(&target, "outside").unwrap();
    let staged = root.join(format!("{key}#1"));
    std::os::unix::fs::symlink(&target, &staged).unwrap();
    assert!(
        remove(&root, &key, GenerationState::Unpublished)
            .await
            .is_err()
    );
    assert_eq!(std::fs::read(&target).unwrap(), b"outside");
    assert!(
        std::fs::symlink_metadata(&staged)
            .unwrap()
            .file_type()
            .is_symlink()
    );
    std::fs::remove_file(&staged).unwrap();

    let objects = root.join(&key).parent().unwrap().to_owned();
    std::fs::rename(&objects, objects.with_file_name("retained")).unwrap();
    std::os::unix::fs::symlink(outside.path(), &objects).unwrap();
    let outside_stage = outside.path().join(format!(
        "{}#1",
        root.join(&key).file_name().unwrap().to_str().unwrap()
    ));
    std::fs::write(&outside_stage, "outside stage").unwrap();
    assert!(
        remove(&root, &key, GenerationState::Unpublished)
            .await
            .is_err()
    );
    assert_eq!(std::fs::read(&outside_stage).unwrap(), b"outside stage");
}

#[tokio::test]
async fn pool_cleanup_rejects_keys_outside_the_uuid_namespace() {
    let (_directory, root, key) = fixture();
    for invalid in [
        "ordinary".to_owned(),
        format!("/{key}"),
        format!("{key}/.."),
        key.replace("/objects/", "/members/"),
        key.replace("/objects/", "/../objects/"),
    ] {
        assert!(
            remove(&root, &invalid, GenerationState::Unpublished)
                .await
                .is_err(),
            "accepted {invalid}"
        );
    }
    std::fs::write(root.join(&key), "owned").unwrap();
    remove(&root, &key, GenerationState::Unpublished)
        .await
        .unwrap();
    assert!(!root.join(&key).exists());
}
