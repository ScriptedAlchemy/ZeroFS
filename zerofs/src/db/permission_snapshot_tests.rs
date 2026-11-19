use super::*;
use slatedb::config::DbReaderOptions;
use std::time::Duration;

const FIRST: &[u8] = b"a";
const SECOND: &[u8] = b"z";

async fn database() -> (Arc<slatedb::Db>, Arc<dyn object_store::ObjectStore>) {
    let store: Arc<dyn object_store::ObjectStore> =
        Arc::new(slatedb::object_store::memory::InMemory::new());
    let inner = Arc::new(
        slatedb::DbBuilder::new(
            slatedb::object_store::path::Path::from("pairs"),
            store.clone(),
        )
        .build()
        .await
        .unwrap(),
    );
    (inner, store)
}

async fn write_pair(inner: &slatedb::Db, value: &'static [u8]) {
    let mut batch = WriteBatch::new();
    batch.put_bytes(Bytes::from_static(FIRST), Bytes::from_static(value));
    batch.put_bytes(Bytes::from_static(SECOND), Bytes::from_static(value));
    inner.write(batch).await.unwrap();
}

async fn reader(store: Arc<dyn object_store::ObjectStore>) -> Arc<DbReader> {
    Arc::new(
        DbReader::builder(slatedb::object_store::path::Path::from("pairs"), store)
            .with_options(DbReaderOptions {
                manifest_poll_interval: Duration::from_millis(10),
                ..Default::default()
            })
            .build()
            .await
            .unwrap(),
    )
}

#[tokio::test]
async fn pair_snapshot_keeps_one_writer_version() {
    let (inner, _) = database().await;
    write_pair(&inner, b"old").await;
    let db = Db::new(inner.clone(), None);
    let first = Bytes::from_static(FIRST);
    let second = Bytes::from_static(SECOND);
    let pair = db
        .get_pair_snapshot_after_first(&first, &second, async {
            write_pair(&inner, b"new").await;
        })
        .await
        .unwrap();
    assert_eq!(
        pair,
        (
            Some(Bytes::from_static(b"old")),
            Some(Bytes::from_static(b"old"))
        )
    );
    assert_eq!(
        db.get_pair_snapshot(&first, &second).await.unwrap(),
        (
            Some(Bytes::from_static(b"new")),
            Some(Bytes::from_static(b"new"))
        )
    );
    db.close().await.unwrap();
}

#[tokio::test]
async fn pair_snapshot_keeps_one_reader_version_across_refresh() {
    let (inner, store) = database().await;
    write_pair(&inner, b"old").await;
    inner.flush().await.unwrap();
    let reader = reader(store).await;
    let db = Db::new_read_only(ArcSwap::new(reader.clone()));
    let first = Bytes::from_static(FIRST);
    let second = Bytes::from_static(SECOND);
    let pair = db
        .get_pair_snapshot_after_first(&first, &second, async {
            write_pair(&inner, b"new").await;
            inner.flush().await.unwrap();
            tokio::time::timeout(Duration::from_secs(5), async {
                loop {
                    if reader.get(SECOND).await.unwrap().as_deref() == Some(b"new") {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(1)).await;
                }
            })
            .await
            .expect("reader did not refresh between the two snapshot reads");
        })
        .await
        .unwrap();
    assert_eq!(
        pair,
        (
            Some(Bytes::from_static(b"old")),
            Some(Bytes::from_static(b"old"))
        )
    );
    assert_eq!(
        db.get_pair_snapshot(&first, &second).await.unwrap(),
        (
            Some(Bytes::from_static(b"new")),
            Some(Bytes::from_static(b"new"))
        )
    );
    db.close().await.unwrap();
    inner.close().await.unwrap();
}

#[tokio::test]
async fn pair_snapshot_handles_missing_keys_and_requires_order() {
    let (inner, store) = database().await;
    write_pair(&inner, b"value").await;
    inner.put(b"m", b"middle").await.unwrap();
    inner.flush().await.unwrap();
    let reader = reader(store).await;
    let db = Db::new_read_only(ArcSwap::new(reader));
    for (first, second, expected) in [
        (b"b", b"z", (None, Some(Bytes::from_static(b"value")))),
        (b"n", b"z", (None, Some(Bytes::from_static(b"value")))),
        (b"a", b"y", (Some(Bytes::from_static(b"value")), None)),
        (b"b", b"l", (None, None)),
    ] {
        assert_eq!(
            db.get_pair_snapshot(&Bytes::from_static(first), &Bytes::from_static(second))
                .await
                .unwrap(),
            expected
        );
    }
    for (first, second) in [(b"a", b"a"), (b"z", b"a")] {
        assert!(
            db.get_pair_snapshot(&Bytes::from_static(first), &Bytes::from_static(second))
                .await
                .is_err()
        );
    }
    db.close().await.unwrap();
    inner.close().await.unwrap();
}

#[tokio::test]
async fn pair_snapshot_rechecks_serving_authority_after_both_reads() {
    let (inner, _) = database().await;
    write_pair(&inner, b"value").await;
    let lease = crate::replication::Lease::new();
    lease.renew(Duration::from_secs(60));
    let db = Db::new(inner, None).with_lease(lease);
    let error = db
        .get_pair_snapshot_after_first(
            &Bytes::from_static(FIRST),
            &Bytes::from_static(SECOND),
            async {
                db.revoke_lease();
            },
        )
        .await
        .unwrap_err();
    assert_eq!(FsError::from_db_error(&error), FsError::LeaderLeaseExpired);
    db.close().await.unwrap();
}
