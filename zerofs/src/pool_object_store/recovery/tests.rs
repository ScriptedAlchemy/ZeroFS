use super::super::{PoolMember, SegmentFault};
use super::{MAX_FAULT_VERIFICATIONS_PER_PASS, PoolObjectStore};
use crate::segment::Segid;
use crate::segment_store::GeneratedSegmentCreate;
use bytes::Bytes;
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt, PutMode, PutOptions};
use std::sync::Arc;
use std::time::Duration;
use tokio_util::sync::CancellationToken;

async fn fault_fixture(count: usize) -> (tempfile::TempDir, PoolObjectStore, Vec<SegmentFault>) {
    let root = tempfile::tempdir().unwrap();
    let pool = PoolObjectStore::initialize(
        Arc::new(InMemory::new()),
        Path::from("db"),
        root.path().join("catalog"),
        vec![PoolMember {
            id: "disk".into(),
            identity: "memory://disk".into(),
            store: Arc::new(InMemory::new()),
            capacity_bytes: 64 * 1024,
            local_path: None,
            min_free_bytes: 0,
            health_device: None,
        }],
    )
    .await
    .unwrap();
    // Use distinct lock stripes so a locked prefix cannot accidentally also
    // lock the later fault whose progress the regression is testing.
    let mut locks = Vec::new();
    for counter in 1..=4096 {
        if locks.len() == count {
            break;
        }
        let path = Path::from(format!("db/{}", Segid::new(41, counter).object_key()));
        let lock = pool.key_lock(&path);
        if locks.iter().any(|other| Arc::ptr_eq(other, &lock)) {
            continue;
        }
        let mut options = PutOptions::from(PutMode::Create);
        options.extensions.insert(GeneratedSegmentCreate);
        pool.put_opts(&path, Bytes::from_static(b"healthy").into(), options)
            .await
            .unwrap();
        let entry = pool.visible(&path).await.unwrap().unwrap();
        let location = path.to_string();
        pool.catalog(move |c| c.record_source_fault(&location, &entry, "test fault".into()))
            .await
            .unwrap();
        locks.push(lock);
    }
    assert_eq!(locks.len(), count, "fixture needs distinct key-lock stripes");
    let faults = pool.faults().await.unwrap();
    assert_eq!(faults.len(), count);
    (root, pool, faults)
}

#[tokio::test]
async fn locked_fault_prefix_does_not_starve_later_verification() {
    let (_root, pool, faults) = fault_fixture(MAX_FAULT_VERIFICATIONS_PER_PASS + 1).await;
    let mut guards = Vec::new();
    for fault in faults.iter().take(MAX_FAULT_VERIFICATIONS_PER_PASS) {
        let path = Path::parse(&fault.location).unwrap();
        guards.push(pool.key_lock(&path).read_owned().await);
    }
    let stop = CancellationToken::new();
    tokio::time::timeout(
        Duration::from_secs(5),
        pool.verify_due_faults(&stop, i64::MAX),
    )
    .await
    .unwrap()
    .unwrap();
    let remaining = pool.faults().await.unwrap();
    assert_eq!(remaining.len(), MAX_FAULT_VERIFICATIONS_PER_PASS);
    assert!(
        remaining
            .iter()
            .all(|fault| fault.location != faults.last().unwrap().location)
    );
    // Skipping locked entries must not remove them or change their backoff.
    for (actual, expected) in remaining.iter().zip(&faults) {
        assert_eq!(actual.location, expected.location);
        assert_eq!(actual.attempts, expected.attempts);
        assert_eq!(actual.next_retry_ms, expected.next_retry_ms);
    }
    drop(guards);
    pool.verify_due_faults(&stop, i64::MAX).await.unwrap();
    assert!(pool.faults().await.unwrap().is_empty());
}

#[tokio::test]
async fn verification_pass_keeps_its_physical_attempt_limit() {
    let (_root, pool, faults) = fault_fixture(MAX_FAULT_VERIFICATIONS_PER_PASS + 3).await;
    let stop = CancellationToken::new();
    pool.verify_due_faults(&stop, i64::MAX).await.unwrap();
    let remaining = pool.faults().await.unwrap();
    assert_eq!(remaining.len(), 3);
    for (actual, expected) in remaining
        .iter()
        .zip(&faults[MAX_FAULT_VERIFICATIONS_PER_PASS..])
    {
        assert_eq!(actual.location, expected.location);
        assert_eq!(actual.attempts, expected.attempts);
    }
    pool.verify_due_faults(&stop, i64::MAX).await.unwrap();
    assert!(pool.faults().await.unwrap().is_empty());
}

#[tokio::test]
async fn failed_physical_verification_also_consumes_the_budget() {
    let (_root, pool, faults) = fault_fixture(MAX_FAULT_VERIFICATIONS_PER_PASS + 1).await;
    pool.member("disk")
        .unwrap()
        .store
        .delete(&Path::parse(&faults[0].physical_key).unwrap())
        .await
        .unwrap();
    pool.verify_due_faults(&CancellationToken::new(), i64::MAX)
        .await
        .unwrap();
    let remaining = pool.faults().await.unwrap();
    assert_eq!(remaining.len(), 2);
    assert_eq!(remaining[0].location, faults[0].location);
    assert_eq!(remaining[0].attempts, faults[0].attempts + 1);
    assert_eq!(remaining[1].location, faults.last().unwrap().location);
    assert_eq!(remaining[1].attempts, faults.last().unwrap().attempts);
}

#[tokio::test]
async fn verification_pass_respects_backoff_and_cancellation() {
    let (_root, pool, faults) = fault_fixture(2).await;
    let before_due = faults.iter().map(|f| f.next_retry_ms).min().unwrap() - 1;
    let stop = CancellationToken::new();
    pool.verify_due_faults(&stop, before_due).await.unwrap();
    assert_eq!(pool.faults().await.unwrap().len(), faults.len());
    stop.cancel();
    pool.verify_due_faults(&stop, i64::MAX).await.unwrap();
    let remaining = pool.faults().await.unwrap();
    assert_eq!(remaining.len(), faults.len());
    for (actual, expected) in remaining.iter().zip(&faults) {
        assert_eq!(actual.location, expected.location);
        assert_eq!(actual.attempts, expected.attempts);
        assert_eq!(actual.next_retry_ms, expected.next_retry_ms);
    }
}
