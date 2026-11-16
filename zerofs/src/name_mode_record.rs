use crate::fs::name_mode::NameMode;
use anyhow::{Context, Result, bail};
use object_store::{ObjectStore, ObjectStoreExt, PutMode, PutOptions, path::Path};
use std::sync::Arc;

const RECORD_NAME: &str = "zerofs.namemode";

fn encode(mode: NameMode) -> [u8; 2] {
    [
        1,
        match mode {
            NameMode::Sensitive => 0,
            NameMode::Insensitive => 1,
        },
    ]
}

fn decode(data: &[u8]) -> Result<NameMode> {
    match data {
        [1, 0] => Ok(NameMode::Sensitive),
        [1, 1] => Ok(NameMode::Insensitive),
        [1, _] => bail!("unknown filesystem name mode in zerofs.namemode"),
        [_, _] => bail!("unknown zerofs.namemode version"),
        _ => bail!("invalid zerofs.namemode record length"),
    }
}

async fn read(store: &Arc<dyn ObjectStore>, path: &Path) -> Result<Option<NameMode>> {
    match store.get(path).await {
        Ok(object) => Ok(Some(decode(&object.bytes().await?)?)),
        Err(object_store::Error::NotFound { .. }) => Ok(None),
        Err(error) => Err(error).context("failed to read zerofs.namemode"),
    }
}

fn check(stored: NameMode, configured: NameMode) -> Result<NameMode> {
    if stored != configured {
        bail!(
            "filesystem name mode mismatch: stored {stored:?}, configured {configured:?}; converting an existing filesystem is not supported"
        );
    }
    Ok(stored)
}

/// Check authority metadata before key initialization, database open or replication admission.
pub(crate) async fn load_or_init(
    store: &Arc<dyn ObjectStore>,
    db_path: &Path,
    configured: NameMode,
    read_only: bool,
) -> Result<NameMode> {
    let path = db_path.clone().join(RECORD_NAME);
    if let Some(mode) = read(store, &path).await? {
        return check(mode, configured);
    }
    match store.head(&db_path.clone().join("zerofs.key")).await {
        Ok(_) => {
            // A concurrent initializer may have published both objects since the first read.
            if let Some(mode) = read(store, &path).await? {
                return check(mode, configured);
            }
            if configured != NameMode::Sensitive {
                bail!(
                    "legacy filesystem without zerofs.namemode is case-sensitive; conversion to insensitive mode is not supported"
                );
            }
            return Ok(NameMode::Sensitive);
        }
        Err(object_store::Error::NotFound { .. }) => {}
        Err(error) => return Err(error).context("failed to inspect existing encryption key"),
    }
    if read_only {
        bail!("cannot initialize zerofs.namemode in read-only mode");
    }
    match store
        .put_opts(
            &path,
            encode(configured).to_vec().into(),
            PutOptions::from(PutMode::Create),
        )
        .await
    {
        Ok(_) => Ok(configured),
        Err(object_store::Error::AlreadyExists { .. }) => {
            let winner = read(store, &path)
                .await?
                .context("zerofs.namemode disappeared after concurrent initialization")?;
            check(winner, configured)
        }
        Err(error) => Err(error).context("failed to create zerofs.namemode"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fault_store::FaultStore;
    use object_store::memory::InMemory;

    fn store() -> Arc<dyn ObjectStore> {
        Arc::new(InMemory::new())
    }

    #[tokio::test]
    async fn fresh_create_is_fixed_and_precedes_the_key() {
        let store = store();
        let path = Path::from("data");
        assert_eq!(
            load_or_init(&store, &path, NameMode::Insensitive, false)
                .await
                .unwrap(),
            NameMode::Insensitive
        );
        assert!(store.head(&path.clone().join("zerofs.key")).await.is_err());
        assert_eq!(
            read(&store, &path.clone().join(RECORD_NAME)).await.unwrap(),
            Some(NameMode::Insensitive)
        );
        assert!(
            load_or_init(&store, &path, NameMode::Sensitive, false)
                .await
                .unwrap_err()
                .to_string()
                .contains("mismatch")
        );
        let key = crate::key_management::load_or_init_encryption_key(
            &store,
            &path,
            "record-test-password",
            false,
        )
        .await
        .unwrap();
        assert_eq!(
            load_or_init(&store, &path, NameMode::Insensitive, true)
                .await
                .unwrap(),
            NameMode::Insensitive
        );
        assert_eq!(
            crate::key_management::load_or_init_encryption_key(
                &store,
                &path,
                "record-test-password",
                true
            )
            .await
            .unwrap(),
            key
        );
    }

    #[tokio::test]
    async fn simultaneous_different_modes_refuse_the_loser() {
        let (fault, controls) = FaultStore::new(store());
        let store: Arc<dyn ObjectStore> = fault;
        controls.block_puts();
        let mut tasks = Vec::new();
        for mode in [NameMode::Sensitive, NameMode::Insensitive] {
            let store = store.clone();
            tasks.push(tokio::spawn(async move {
                load_or_init(&store, &Path::from("race"), mode, false).await
            }));
        }
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            while controls.put_count() < 2 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        controls.release_puts();
        let a = tasks.remove(0).await.unwrap();
        let b = tasks.remove(0).await.unwrap();
        assert_ne!(a.is_ok(), b.is_ok());
        let winner = read(&store, &Path::from("race").join(RECORD_NAME))
            .await
            .unwrap()
            .unwrap();
        for result in [a, b] {
            match result {
                Ok(mode) => assert_eq!(mode, winner),
                Err(error) => assert!(error.to_string().contains("mismatch")),
            }
        }
    }

    #[tokio::test]
    async fn a_key_appearing_after_the_first_read_does_not_look_legacy() {
        let inner = store();
        let (fault, controls) = FaultStore::new(inner.clone());
        let guarded: Arc<dyn ObjectStore> = fault;
        controls.block_heads();
        let task = tokio::spawn(async move {
            load_or_init(&guarded, &Path::from("late"), NameMode::Sensitive, false).await
        });
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            while controls.head_count() == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        inner
            .put(
                &Path::from("late").join(RECORD_NAME),
                encode(NameMode::Insensitive).to_vec().into(),
            )
            .await
            .unwrap();
        inner
            .put(
                &Path::from("late").join("zerofs.key"),
                b"key".to_vec().into(),
            )
            .await
            .unwrap();
        controls.release_heads();
        assert!(
            task.await
                .unwrap()
                .unwrap_err()
                .to_string()
                .contains("mismatch")
        );
        assert_eq!(controls.put_count(), 0);
    }

    #[tokio::test]
    async fn legacy_key_without_record_remains_sensitive() {
        let store = store();
        let path = Path::from("legacy");
        store
            .put(
                &path.clone().join("zerofs.key"),
                b"legacy-key".to_vec().into(),
            )
            .await
            .unwrap();
        for read_only in [false, true] {
            assert_eq!(
                load_or_init(&store, &path, NameMode::Sensitive, read_only)
                    .await
                    .unwrap(),
                NameMode::Sensitive
            );
            assert!(
                load_or_init(&store, &path, NameMode::Insensitive, read_only)
                    .await
                    .unwrap_err()
                    .to_string()
                    .contains("legacy")
            );
            assert!(store.head(&path.clone().join(RECORD_NAME)).await.is_err());
        }
    }

    #[tokio::test]
    async fn read_only_never_creates_and_rejects_unknown_records() {
        let (fault, controls) = FaultStore::new(store());
        let store: Arc<dyn ObjectStore> = fault;
        let path = Path::from("readonly");
        assert!(
            load_or_init(&store, &path, NameMode::Sensitive, true)
                .await
                .unwrap_err()
                .to_string()
                .contains("read-only")
        );
        assert_eq!(controls.put_count(), 0);
        for data in [vec![2, 0], vec![1, 2], vec![], vec![1, 0, 0]] {
            store
                .put(&path.clone().join(RECORD_NAME), data.into())
                .await
                .unwrap();
            let puts = controls.put_count();
            for read_only in [false, true] {
                assert!(
                    load_or_init(&store, &path, NameMode::Sensitive, read_only)
                        .await
                        .is_err()
                );
            }
            assert_eq!(controls.put_count(), puts);
        }
    }

    #[tokio::test]
    async fn record_without_key_checks_mode_without_writing() {
        let (fault, controls) = FaultStore::new(store());
        let store: Arc<dyn ObjectStore> = fault;
        let path = Path::from("interrupted");
        store
            .put(
                &path.clone().join(RECORD_NAME),
                encode(NameMode::Insensitive).to_vec().into(),
            )
            .await
            .unwrap();
        for read_only in [false, true] {
            assert_eq!(
                load_or_init(&store, &path, NameMode::Insensitive, read_only)
                    .await
                    .unwrap(),
                NameMode::Insensitive
            );
            assert!(
                load_or_init(&store, &path, NameMode::Sensitive, read_only)
                    .await
                    .is_err()
            );
        }
        assert_eq!(controls.put_count(), 1);
        assert!(store.head(&path.clone().join("zerofs.key")).await.is_err());
    }
}
