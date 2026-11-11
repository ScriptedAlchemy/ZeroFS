use super::super::{PoolMember, SegmentFault};
use super::{MAX_FAULT_VERIFICATIONS_PER_PASS, PoolObjectStore};
use crate::segment::Segid;
use crate::segment_store::GeneratedSegmentCreate;
use bytes::Bytes;
use futures::stream::BoxStream;
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    ObjectStoreExt, PutMode, PutMultipartOptions, PutOptions, PutPayload, PutResult,
};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use tokio_util::sync::CancellationToken;

async fn evacuation_fixture(capacities: &[u64]) -> (tempfile::TempDir, PoolObjectStore) {
    evacuation_fixture_with_target(capacities, Arc::new(InMemory::new())).await
}

async fn evacuation_fixture_with_target(
    capacities: &[u64],
    target: Arc<dyn ObjectStore>,
) -> (tempfile::TempDir, PoolObjectStore) {
    let root = tempfile::tempdir().unwrap();
    let members = capacities
        .iter()
        .enumerate()
        .map(|(index, capacity)| PoolMember {
            id: format!("disk-{index}"),
            identity: format!("memory://disk-{index}"),
            store: if index + 1 == capacities.len() {
                target.clone()
            } else {
                Arc::new(InMemory::new())
            },
            capacity_bytes: *capacity,
            local_path: None,
            min_free_bytes: 0,
            health_device: None,
        })
        .collect();
    let pool = PoolObjectStore::initialize(
        Arc::new(InMemory::new()),
        Path::from("db"),
        root.path().join("catalog"),
        members,
    )
    .await
    .unwrap();
    (root, pool)
}

/// Pauses after the destination has accepted a body, so a concurrent health
/// event occurs after the upload-start fence and before the placement commit.
#[derive(Debug, Default)]
struct PublicationGateStore {
    inner: InMemory,
    armed: AtomicBool,
    entered: tokio::sync::Notify,
    resume: tokio::sync::Notify,
}

impl std::fmt::Display for PublicationGateStore {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("PublicationGateStore")
    }
}

#[async_trait::async_trait]
impl ObjectStore for PublicationGateStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        options: PutOptions,
    ) -> object_store::Result<PutResult> {
        let written = self.inner.put_opts(location, payload, options).await?;
        if location.as_ref().contains("/objects/") && self.armed.swap(false, Ordering::SeqCst) {
            self.entered.notify_one();
            self.resume.notified().await;
        }
        Ok(written)
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        options: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, options).await
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        self.inner.get_opts(location, options).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, object_store::Result<Path>>,
    ) -> BoxStream<'static, object_store::Result<Path>> {
        self.inner.delete_stream(locations)
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        source: &Path,
        target: &Path,
        options: CopyOptions,
    ) -> object_store::Result<()> {
        self.inner.copy_opts(source, target, options).await
    }
}

fn segment(counter: u64) -> Path {
    Path::from(format!("db/{}", Segid::new(41, counter).object_key()))
}

async fn create(pool: &PoolObjectStore, path: &Path, bytes: &'static [u8]) {
    let mut options = PutOptions::from(PutMode::Create);
    options.extensions.insert(GeneratedSegmentCreate);
    pool.put_opts(path, Bytes::from_static(bytes).into(), options)
        .await
        .unwrap();
}

async fn member_for(pool: &PoolObjectStore, path: &Path) -> String {
    pool.placements()
        .await
        .unwrap()
        .into_iter()
        .find(|placement| placement.location == *path)
        .unwrap()
        .member_id
}

#[tokio::test]
async fn two_sick_members_stop_new_copies_and_report_failure_across_restart() {
    let (root, pool) = evacuation_fixture(&[8, 8, 128]).await;
    let first = segment(3000);
    let second = segment(3001);
    create(&pool, &first, b"source-a").await;
    create(&pool, &second, b"source-b").await;
    pool.quarantine("disk-0", "first media failure")
        .await
        .unwrap();
    pool.quarantine("disk-1", "second media failure")
        .await
        .unwrap();
    pool.evacuate_pass(&CancellationToken::new()).await.unwrap();
    assert_eq!(member_for(&pool, &first).await, "disk-0");
    assert_eq!(member_for(&pool, &second).await, "disk-1");
    let status = pool.status().await.unwrap();
    assert_eq!(status.pending_operations, 0);
    assert_eq!(status.members[2].objects, 0);
    for member in &status.members[..2] {
        assert_eq!(
            member.health.evacuation_error.as_deref(),
            Some(super::MULTIPLE_SICK_MEMBERS)
        );
        assert!(!member.safe_to_remove);
        assert!(!member.safe_to_pull_with_known_loss);
    }
    let authority = pool.inner.authority.clone();
    let members = pool.inner.members.clone();
    drop(pool);
    let reopened = PoolObjectStore::open(
        authority,
        Path::from("db"),
        root.path().join("catalog"),
        members,
    )
    .await
    .unwrap();
    for member in &reopened.status().await.unwrap().members[..2] {
        assert_eq!(
            member.health.evacuation_error.as_deref(),
            Some(super::MULTIPLE_SICK_MEMBERS)
        );
    }
    reopened
        .evacuate_pass(&CancellationToken::new())
        .await
        .unwrap();
    assert_eq!(member_for(&reopened, &first).await, "disk-0");
    assert_eq!(member_for(&reopened, &second).await, "disk-1");
}

#[tokio::test]
async fn emptied_historical_quarantine_does_not_block_later_single_failure() {
    let (_root, pool) = evacuation_fixture(&[8, 8, 128]).await;
    let first = segment(3002);
    let second = segment(3003);
    create(&pool, &first, b"source-a").await;
    create(&pool, &second, b"source-b").await;
    pool.quarantine("disk-0", "old media failure")
        .await
        .unwrap();
    pool.evacuate_pass(&CancellationToken::new()).await.unwrap();
    assert_eq!(member_for(&pool, &first).await, "disk-2");
    assert!(pool.status().await.unwrap().members[0].safe_to_remove);
    pool.quarantine("disk-1", "later media failure")
        .await
        .unwrap();
    pool.evacuate_pass(&CancellationToken::new()).await.unwrap();
    assert_eq!(member_for(&pool, &second).await, "disk-2");
    for member in &pool.status().await.unwrap().members[..2] {
        assert!(member.safe_to_remove);
        assert!(member.health.evacuation_error.is_none());
    }
}

#[tokio::test]
async fn zero_byte_pending_upload_counts_as_an_active_failed_member() {
    use crate::segment_store::ConditionalMultipartCreate;
    use object_store::PutMultipartOptions;

    let (_root, pool) = evacuation_fixture(&[8, 8, 128]).await;
    let second = segment(3004);
    create(&pool, &second, b"source-b").await;
    pool.migrate(&second, "disk-1").await.unwrap();
    let mut options = PutMultipartOptions::default();
    options.extensions.insert(GeneratedSegmentCreate);
    options
        .extensions
        .insert(ConditionalMultipartCreate::for_payload_len(0));
    let mut upload = pool
        .put_multipart_opts(&segment(3005), options)
        .await
        .unwrap();
    pool.quarantine("disk-0", "failed with pending upload")
        .await
        .unwrap();
    pool.quarantine("disk-1", "failed with live object")
        .await
        .unwrap();
    pool.evacuate_pass(&CancellationToken::new()).await.unwrap();
    let status = pool.status().await.unwrap();
    assert_eq!(status.members[0].used_bytes, 0);
    assert_eq!(status.members[0].reserved_bytes, 0);
    assert_eq!(status.members[0].pending_uploads, 1);
    assert_eq!(member_for(&pool, &second).await, "disk-1");
    for member in &status.members[..2] {
        assert_eq!(
            member.health.evacuation_error.as_deref(),
            Some(super::MULTIPLE_SICK_MEMBERS)
        );
    }
    upload.abort().await.unwrap();
    drop(upload);
    pool.evacuate_pass(&CancellationToken::new()).await.unwrap();
    assert_eq!(member_for(&pool, &second).await, "disk-2");
    assert!(
        pool.status().await.unwrap().members[1]
            .health
            .evacuation_error
            .is_none()
    );
}

#[tokio::test]
async fn second_failure_during_reserved_copy_fences_remap_and_keeps_committed_copy() {
    let (_root, pool) = evacuation_fixture(&[16, 16, 128]).await;
    let committed = segment(3006);
    let remaining = segment(3007);
    let second = segment(3008);
    create(&pool, &committed, b"copied-a").await;
    create(&pool, &remaining, b"source-a").await;
    create(&pool, &second, b"second-disk-data").await;
    pool.migrate(&committed, "disk-2").await.unwrap();
    pool.quarantine("disk-0", "first media failure")
        .await
        .unwrap();
    let entered = Arc::new(tokio::sync::Notify::new());
    let resume = Arc::new(tokio::sync::Notify::new());
    *pool.inner.reserve_pause.lock().unwrap() = Some((entered.clone(), resume.clone()));
    let recovering = pool.clone();
    let task =
        tokio::spawn(async move { recovering.evacuate_pass(&CancellationToken::new()).await });
    tokio::time::timeout(Duration::from_secs(5), entered.notified())
        .await
        .unwrap();
    pool.quarantine("disk-1", "second media failure")
        .await
        .unwrap();
    resume.notify_one();
    tokio::time::timeout(Duration::from_secs(5), task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(member_for(&pool, &committed).await, "disk-2");
    assert_eq!(member_for(&pool, &remaining).await, "disk-0");
    assert_eq!(member_for(&pool, &second).await, "disk-1");
    assert_eq!(
        pool.get(&committed).await.unwrap().bytes().await.unwrap(),
        Bytes::from_static(b"copied-a")
    );
    let status = pool.status().await.unwrap();
    assert_eq!(status.pending_operations, 0);
    assert_eq!(status.members[2].objects, 1);
    for member in &status.members[..2] {
        assert_eq!(
            member.health.evacuation_error.as_deref(),
            Some(super::MULTIPLE_SICK_MEMBERS)
        );
    }
}

#[tokio::test]
async fn running_copy_rechecks_two_disk_policy_at_publication() {
    let gate = Arc::new(PublicationGateStore::default());
    let (_root, pool) = evacuation_fixture_with_target(&[16, 16, 128], gate.clone()).await;
    let first = segment(3020);
    let second = segment(3021);
    create(&pool, &first, b"first-disk-bytes").await;
    create(&pool, &second, b"second-disk-data").await;
    pool.quarantine("disk-0", "first media failure")
        .await
        .unwrap();
    gate.armed.store(true, Ordering::SeqCst);
    let recovering = pool.clone();
    let task =
        tokio::spawn(async move { recovering.evacuate_pass(&CancellationToken::new()).await });
    tokio::time::timeout(Duration::from_secs(5), gate.entered.notified())
        .await
        .unwrap();
    let status = tokio::time::timeout(Duration::from_secs(5), pool.status())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(status.members[0].evacuation, "running");
    assert_eq!(status.members[0].evacuation_capacity.required_bytes, 0);
    assert_eq!(
        status.members[0]
            .evacuation_capacity
            .reserved_destination_bytes,
        16
    );
    assert!(status.members[0].evacuation_capacity.sufficient);
    assert!(!status.members[0].safe_to_remove);
    assert_eq!(status.pending_operations, 1);
    assert_eq!(member_for(&pool, &first).await, "disk-0");
    let (uploads, _) = pool
        .catalog(super::super::catalog::Catalog::recovery)
        .await
        .unwrap();
    assert_eq!(uploads.len(), 1);
    assert!(
        uploads[0].started,
        "the upload-start policy fence already passed"
    );
    assert_eq!(
        gate.inner
            .get(&Path::from(uploads[0].physical_key.clone()))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap(),
        Bytes::from_static(b"first-disk-bytes")
    );
    pool.quarantine("disk-1", "second media failure")
        .await
        .unwrap();
    assert_eq!(
        pool.status().await.unwrap().members[0].evacuation,
        "failed_multiple_devices"
    );
    gate.resume.notify_one();
    tokio::time::timeout(Duration::from_secs(5), task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(member_for(&pool, &first).await, "disk-0");
    assert_eq!(member_for(&pool, &second).await, "disk-1");
    let status = pool.status().await.unwrap();
    assert_eq!(status.pending_operations, 0);
    assert_eq!(status.members[2].objects, 0);
    assert!(status.unreadable.is_empty());
    assert!(matches!(
        gate.inner
            .head(&Path::from(uploads[0].physical_key.clone()))
            .await,
        Err(object_store::Error::NotFound { .. })
    ));
}

#[tokio::test]
async fn abandoned_upload_debt_is_not_credited_as_evacuation_progress() {
    let (_root, pool) = evacuation_fixture(&[8, 8]).await;
    let source = segment(3022);
    create(&pool, &source, b"source-a").await;
    let upload = pool
        .reserve(&source, 8, Some("disk-1".into()))
        .await
        .unwrap();
    let id = upload.id.clone();
    pool.catalog(move |catalog| catalog.mark_started_for_migration(&id, false))
        .await
        .unwrap();
    pool.ensure_member("disk-1", true).await.unwrap();
    pool.member("disk-1")
        .unwrap()
        .store
        .put(
            &Path::from(upload.physical_key.clone()),
            Bytes::from_static(b"orphaned").into(),
        )
        .await
        .unwrap();
    // An interrupted writer has left a charged body/reservation, but there is
    // no active automatic copy that could use it to remap this generation.
    drop(upload);
    pool.quarantine("disk-0", "source media failure")
        .await
        .unwrap();
    assert!(pool.inner.evacuating.lock().unwrap().is_none());
    let status = pool.status().await.unwrap();
    assert_eq!(
        status.members[0]
            .evacuation_capacity
            .reserved_destination_bytes,
        0
    );
    assert_eq!(status.members[0].evacuation_capacity.required_bytes, 8);
    assert_eq!(status.members[0].evacuation_capacity.available_bytes, 0);
    assert!(!status.members[0].evacuation_capacity.sufficient);
    assert_eq!(status.members[0].evacuation, "cannot_complete_capacity");
    assert_eq!(status.members[1].reserved_bytes, 8);
    assert_eq!(status.pending_operations, 1);
    pool.recover_pending().await.unwrap();
    let recovered = pool.status().await.unwrap();
    assert_eq!(recovered.pending_operations, 0);
    assert_eq!(
        recovered.members[0]
            .evacuation_capacity
            .reserved_destination_bytes,
        0
    );
    assert_eq!(recovered.members[0].evacuation_capacity.required_bytes, 8);
    assert_eq!(recovered.members[0].evacuation_capacity.available_bytes, 8);
    assert!(recovered.members[0].evacuation_capacity.sufficient);
    pool.evacuate_pass(&CancellationToken::new()).await.unwrap();
    assert_eq!(member_for(&pool, &source).await, "disk-1");
    assert_eq!(
        pool.get(&source).await.unwrap().bytes().await.unwrap(),
        Bytes::from_static(b"source-a")
    );
}

#[tokio::test]
async fn held_read_lease_does_not_starve_other_source_segments() {
    let (_root, pool) = evacuation_fixture(&[16, 128]).await;
    let first = segment(3009);
    let second = (3010..4096)
        .map(segment)
        .find(|candidate| {
            candidate > &first && !Arc::ptr_eq(&pool.key_lock(&first), &pool.key_lock(candidate))
        })
        .unwrap();
    create(&pool, &first, b"source-a").await;
    create(&pool, &second, b"source-b").await;
    let held = pool.get(&first).await.unwrap();
    pool.quarantine("disk-0", "media failure").await.unwrap();
    tokio::time::timeout(
        Duration::from_secs(5),
        pool.evacuate_pass(&CancellationToken::new()),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(member_for(&pool, &first).await, "disk-0");
    assert_eq!(member_for(&pool, &second).await, "disk-1");
    assert!(pool.faults().await.unwrap().is_empty());
    assert!(!pool.status().await.unwrap().members[0].safe_to_remove);
    assert_eq!(held.bytes().await.unwrap(), Bytes::from_static(b"source-a"));
    pool.evacuate_pass(&CancellationToken::new()).await.unwrap();
    assert_eq!(member_for(&pool, &first).await, "disk-1");
    assert!(pool.status().await.unwrap().members[0].safe_to_remove);
}

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
    assert_eq!(
        locks.len(),
        count,
        "fixture needs distinct key-lock stripes"
    );
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
