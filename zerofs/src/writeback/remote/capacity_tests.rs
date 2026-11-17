use super::*;
use crate::retrying_object_store::{CapacityError, PermanentError, RetryingObjectStore};
use crate::writeback::journaler::LocalJournaler;
use crate::writeback::model::JournalIdentity;
use bytes::Bytes;
use futures::stream::BoxStream;
use object_store::{CopyOptions, ListResult, ObjectMeta, RenameOptions};

/// The same explicitly marked capacity refusal the pool returns when no
/// member can reserve an object. Ordinary permanent failures stay distinct.
fn full() -> object_store::Error {
    object_store::Error::Generic {
        store: "PoolObjectStore",
        source: Box::new(PermanentError::new(CapacityError::new("pool is full"))),
    }
}

#[derive(Debug)]
struct FullUntilReleased {
    inner: object_store::memory::InMemory,
    full: AtomicBool,
    attempts: AtomicUsize,
}

impl std::fmt::Display for FullUntilReleased {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("FullUntilReleased")
    }
}

#[async_trait::async_trait]
impl ObjectStore for FullUntilReleased {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        options: PutOptions,
    ) -> object_store::Result<PutResult> {
        self.attempts.fetch_add(1, Ordering::SeqCst);
        if self.full.load(Ordering::SeqCst) {
            return Err(full());
        }
        self.inner.put_opts(location, payload, options).await
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
        path: &Path,
        options: GetOptions,
    ) -> object_store::Result<object_store::GetResult> {
        self.inner.get_opts(path, options).await
    }

    fn delete_stream(
        &self,
        paths: BoxStream<'static, object_store::Result<Path>>,
    ) -> BoxStream<'static, object_store::Result<Path>> {
        self.inner.delete_stream(paths)
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        options: CopyOptions,
    ) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, options).await
    }

    async fn rename_opts(
        &self,
        from: &Path,
        to: &Path,
        options: RenameOptions,
    ) -> object_store::Result<()> {
        self.inner.rename_opts(from, to, options).await
    }
}

#[test]
fn only_explicit_capacity_refusals_are_retryable_after_a_permanent_error() {
    assert!(crate::retrying_object_store::is_no_space(&full()));
    assert!(crate::retrying_object_store::has_permanent_source(&full()));
    assert!(!is_terminal_remote_error(&full()));
    for errno in [libc::EIO, libc::ENOSPC] {
        let failure = object_store::Error::Generic {
            store: "failed-cleanup",
            source: Box::new(PermanentError::new(std::io::Error::from_raw_os_error(
                errno,
            ))),
        };
        assert!(is_terminal_remote_error(&failure));
    }
}

#[tokio::test]
async fn full_pool_drain_keeps_queued_payload_and_recovers_without_restart() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path().join("journal");
    let journal = Arc::new(
        Journal::open(
            &root,
            JournalIdentity {
                format_version: 1,
                bucket_id: "capacity-recovery".into(),
                backend_endpoint: "memory://capacity-recovery".into(),
                database_prefix: "root".into(),
                backend_kind: "memory".into(),
                encryption_key_identity_sha256: [0x42; 32],
            },
        )
        .unwrap(),
    );
    let backend = Arc::new(FullUntilReleased {
        inner: object_store::memory::InMemory::new(),
        full: AtomicBool::new(true),
        attempts: AtomicUsize::new(0),
    });
    let remote: Arc<dyn ObjectStore> = Arc::new(RetryingObjectStore::new(backend.clone()));
    let path = Path::from("root/segments/01/0000000000000001/0000000000000001");
    let payload = Bytes::from_static(b"acknowledged bytes waiting for pool capacity");

    let direct_error = tokio::time::timeout(
        Duration::from_secs(1),
        remote.put(&path, payload.clone().into()),
    )
    .await
    .expect("direct capacity refusals must not retry indefinitely")
    .unwrap_err();
    assert!(crate::retrying_object_store::is_no_space(&direct_error));
    assert_eq!(backend.attempts.swap(0, Ordering::SeqCst), 1);

    let record = crate::writeback::test_util::put_record(
        1,
        path.as_ref(),
        &payload,
        MutationMode::Create,
        FenceClass::ImmutableCreate,
        0,
        0,
    );
    let record = journal.commit_put(record, &payload).unwrap();
    let reservation = SsdReservationRequest::from_pending_record(&record).unwrap();
    let admission = Admission::new(1024 * 1024);
    let ssd =
        Arc::new(SsdAdmission::recover(1024 * 1024, 100, 90, 70, 1, [reservation], None).unwrap());
    let space = Arc::new(PhysicalSpaceSampler::new(root));
    let overlay = OverlayIndex::recover(remote.clone(), journal.clone())
        .await
        .unwrap();
    let journaler = LocalJournaler::start_with_observer_and_space(
        journal.clone(),
        admission.clone(),
        4,
        1,
        None,
        space.clone(),
    )
    .unwrap();
    let scheduler = RemoteScheduler::start(
        remote,
        journal.clone(),
        overlay,
        admission.clone(),
        ssd.clone(),
        space,
        journaler.barrier(),
        1,
    )
    .unwrap();

    tokio::time::timeout(Duration::from_secs(3), async {
        while backend.attempts.load(Ordering::SeqCst) < 2 {
            assert!(
                scheduler.terminal_error().is_none(),
                "full storage poisoned the drain"
            );
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the journal must retry a full pool");
    assert!(scheduler.terminal_error().is_none());
    assert_eq!(journal.progress().unwrap().remote_seq, 0);
    assert_eq!(journal.read_blob(1).unwrap(), payload);
    assert_eq!(ssd.used_bytes(), reservation.ssd_reservation_bytes);

    backend.full.store(false, Ordering::SeqCst);
    tokio::time::timeout(Duration::from_secs(3), scheduler.barrier().wait_remote(1))
        .await
        .expect("the same scheduler must drain once capacity returns")
        .unwrap();
    assert_eq!(
        backend.get(&path).await.unwrap().bytes().await.unwrap(),
        payload
    );
    assert_eq!(journal.progress().unwrap().remote_seq, 1);
    assert_eq!(ssd.used_bytes(), 0);
    assert!(scheduler.terminal_error().is_none());
    drop(
        admission
            .reserve(1)
            .await
            .expect("RAM admission must remain usable"),
    );
    scheduler.shutdown().await.unwrap();
    journaler.shutdown().await.unwrap();
}
