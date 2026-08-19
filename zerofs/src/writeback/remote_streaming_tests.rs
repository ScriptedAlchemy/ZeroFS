use super::*;
use crate::writeback::journaler::LocalJournaler;
use crate::writeback::model::JournalIdentity;
use async_trait::async_trait;
use futures::future;
use futures::stream::BoxStream;
use object_store::local::LocalFileSystem;
use object_store::memory::InMemory;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, ObjectMeta, PutOptions, UploadPart,
};
use std::fmt::{Display, Formatter};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::Notify;

#[derive(Debug)]
struct RecordingMultipartStore {
    inner: Arc<LocalFileSystem>,
    put_opts_calls: AtomicUsize,
    multipart_calls: AtomicUsize,
    part_sizes: Arc<Mutex<Vec<usize>>>,
}

impl Display for RecordingMultipartStore {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("RecordingMultipartStore")
    }
}

#[async_trait]
impl ObjectStore for RecordingMultipartStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        options: PutOptions,
    ) -> object_store::Result<PutResult> {
        self.put_opts_calls.fetch_add(1, Ordering::SeqCst);
        self.inner.put_opts(location, payload, options).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        options: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.multipart_calls.fetch_add(1, Ordering::SeqCst);
        let inner = self.inner.put_multipart_opts(location, options).await?;
        Ok(Box::new(RecordingUpload {
            inner,
            part_sizes: Arc::clone(&self.part_sizes),
        }))
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
        from: &Path,
        to: &Path,
        options: CopyOptions,
    ) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, options).await
    }
}

#[derive(Debug)]
struct RecordingUpload {
    inner: Box<dyn MultipartUpload>,
    part_sizes: Arc<Mutex<Vec<usize>>>,
}

#[async_trait]
impl MultipartUpload for RecordingUpload {
    fn put_part(&mut self, data: PutPayload) -> UploadPart {
        self.part_sizes.lock().unwrap().push(data.content_length());
        self.inner.put_part(data)
    }

    async fn complete(&mut self) -> object_store::Result<PutResult> {
        self.inner.complete().await
    }

    async fn abort(&mut self) -> object_store::Result<()> {
        self.inner.abort().await
    }
}

#[derive(Debug)]
struct BlockingAbortUpload {
    aborts: Arc<AtomicUsize>,
    entered: Arc<Notify>,
    release: Arc<Notify>,
}

#[derive(Debug)]
struct FailingAbortUpload;

#[async_trait]
impl MultipartUpload for FailingAbortUpload {
    fn put_part(&mut self, _data: PutPayload) -> UploadPart {
        panic!("cleanup-only test never uploads a part")
    }

    async fn complete(&mut self) -> object_store::Result<PutResult> {
        panic!("cleanup-only test never completes the upload")
    }

    async fn abort(&mut self) -> object_store::Result<()> {
        Err(generic_error("injected multipart abort failure"))
    }
}

#[derive(Debug)]
struct FailedPartAndAbortUpload {
    aborts: Arc<AtomicUsize>,
}

#[async_trait]
impl MultipartUpload for FailedPartAndAbortUpload {
    fn put_part(&mut self, _data: PutPayload) -> UploadPart {
        Box::pin(async { Err(generic_error("injected multipart part failure")) })
    }

    async fn complete(&mut self) -> object_store::Result<PutResult> {
        panic!("failed part must not complete")
    }

    async fn abort(&mut self) -> object_store::Result<()> {
        self.aborts.fetch_add(1, Ordering::SeqCst);
        Err(generic_error("injected multipart abort failure"))
    }
}

#[derive(Debug)]
struct PendingPartUpload {
    aborts: Arc<AtomicUsize>,
}

#[async_trait]
impl MultipartUpload for PendingPartUpload {
    fn put_part(&mut self, _data: PutPayload) -> UploadPart {
        Box::pin(future::pending())
    }

    async fn complete(&mut self) -> object_store::Result<PutResult> {
        panic!("pending part must not complete")
    }

    async fn abort(&mut self) -> object_store::Result<()> {
        self.aborts.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

#[derive(Debug)]
struct SchedulerCleanupFailureStore {
    inner: Arc<InMemory>,
    multipart_calls: AtomicUsize,
    failed_aborts: Arc<AtomicUsize>,
    pending_aborts: Arc<AtomicUsize>,
}

impl Display for SchedulerCleanupFailureStore {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("SchedulerCleanupFailureStore")
    }
}

#[async_trait]
impl ObjectStore for SchedulerCleanupFailureStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        options: PutOptions,
    ) -> object_store::Result<PutResult> {
        self.inner.put_opts(location, payload, options).await
    }

    async fn put_multipart_opts(
        &self,
        _location: &Path,
        _options: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        let call = self.multipart_calls.fetch_add(1, Ordering::SeqCst);
        if call == 0 {
            Ok(Box::new(FailedPartAndAbortUpload {
                aborts: Arc::clone(&self.failed_aborts),
            }))
        } else {
            Ok(Box::new(PendingPartUpload {
                aborts: Arc::clone(&self.pending_aborts),
            }))
        }
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
        from: &Path,
        to: &Path,
        options: CopyOptions,
    ) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, options).await
    }
}

#[async_trait]
impl MultipartUpload for BlockingAbortUpload {
    fn put_part(&mut self, _data: PutPayload) -> UploadPart {
        panic!("cleanup-only test never uploads a part")
    }

    async fn complete(&mut self) -> object_store::Result<PutResult> {
        panic!("cleanup-only test never completes the upload")
    }

    async fn abort(&mut self) -> object_store::Result<()> {
        self.aborts.fetch_add(1, Ordering::SeqCst);
        self.entered.notify_one();
        self.release.notified().await;
        Ok(())
    }
}

fn identity() -> JournalIdentity {
    JournalIdentity {
        format_version: 1,
        bucket_id: "bounded-remote".to_owned(),
        backend_endpoint: "file://remote".to_owned(),
        database_prefix: "zerofs/test".to_owned(),
        backend_kind: "local".to_owned(),
        encryption_key_identity_sha256: [0x4d; 32],
    }
}

#[tokio::test]
async fn remote_replay_streams_payload_larger_than_the_window_in_bounded_parts() {
    let temp = tempfile::tempdir().unwrap();
    let journal = Arc::new(Journal::open(temp.path().join("journal"), identity()).unwrap());
    let payload = vec![0x5a; REMOTE_STREAM_CHUNK_BYTES + 1];
    let record = crate::writeback::test_util::put_record(
        1,
        "large-object",
        &payload,
        MutationMode::Overwrite,
        FenceClass::Fence,
        0x1000,
        1_786_435_200_000,
    );
    let record = journal.commit_put(record, &payload).unwrap();
    let remote_root = temp.path().join("remote");
    std::fs::create_dir(&remote_root).unwrap();
    let store = Arc::new(RecordingMultipartStore {
        inner: Arc::new(LocalFileSystem::new_with_prefix(&remote_root).unwrap()),
        put_opts_calls: AtomicUsize::new(0),
        multipart_calls: AtomicUsize::new(0),
        part_sizes: Arc::new(Mutex::new(Vec::new())),
    });

    apply_record_with_tracked_cleanup(store.clone(), Arc::clone(&journal), record)
        .await
        .unwrap();

    assert_eq!(store.put_opts_calls.load(Ordering::SeqCst), 0);
    assert_eq!(store.multipart_calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        *store.part_sizes.lock().unwrap(),
        vec![REMOTE_STREAM_CHUNK_BYTES, 1]
    );
    let result = store.inner.get(&Path::from("large-object")).await.unwrap();
    assert_eq!(result.meta.size, payload.len() as u64);
    assert_eq!(result.bytes().await.unwrap().as_ref(), payload.as_slice());
}

#[tokio::test]
async fn remote_replay_at_the_window_boundary_keeps_atomic_put_semantics() {
    let temp = tempfile::tempdir().unwrap();
    let journal = Arc::new(Journal::open(temp.path().join("journal"), identity()).unwrap());
    let payload = vec![0x6b; REMOTE_STREAM_CHUNK_BYTES];
    let record = crate::writeback::test_util::put_record(
        1,
        "boundary-object",
        &payload,
        MutationMode::Overwrite,
        FenceClass::Fence,
        0x1000,
        1_786_435_200_000,
    );
    let record = journal.commit_put(record, &payload).unwrap();
    let remote_root = temp.path().join("remote");
    std::fs::create_dir(&remote_root).unwrap();
    let store = Arc::new(RecordingMultipartStore {
        inner: Arc::new(LocalFileSystem::new_with_prefix(&remote_root).unwrap()),
        put_opts_calls: AtomicUsize::new(0),
        multipart_calls: AtomicUsize::new(0),
        part_sizes: Arc::new(Mutex::new(Vec::new())),
    });

    apply_record_with_tracked_cleanup(store.clone(), Arc::clone(&journal), record)
        .await
        .unwrap();

    assert_eq!(store.put_opts_calls.load(Ordering::SeqCst), 1);
    assert_eq!(store.multipart_calls.load(Ordering::SeqCst), 0);
    assert!(store.part_sizes.lock().unwrap().is_empty());
    assert_eq!(
        store
            .inner
            .get(&Path::from("boundary-object"))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap()
            .as_ref(),
        payload.as_slice()
    );
}

#[tokio::test]
async fn multipart_owner_cancellation_is_drained_by_the_tracked_cleanup_worker() {
    let cleanup_state = RemoteCleanupState::new();
    let mut failure_receiver = cleanup_state.failure.subscribe();
    let (cleanup_sender, cleanup_receiver) = mpsc::channel(1);
    let cleanup_worker = tokio::spawn(drain_remote_multipart_cleanup(
        cleanup_receiver,
        1,
        Arc::clone(&cleanup_state),
    ));
    let aborts = Arc::new(AtomicUsize::new(0));
    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let owner = RemoteMultipartOwner::new(
        Box::new(BlockingAbortUpload {
            aborts: Arc::clone(&aborts),
            entered: Arc::clone(&entered),
            release: Arc::clone(&release),
        }),
        cleanup_sender.clone(),
        Arc::clone(&cleanup_state),
    );

    drop(owner);
    drop(cleanup_sender);
    entered.notified().await;
    assert!(!cleanup_worker.is_finished());
    release.notify_one();
    cleanup_worker.await.unwrap().unwrap();
    assert!(failure_receiver.borrow_and_update().is_none());
    assert_eq!(aborts.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn multipart_abort_failure_is_published_while_cleanup_input_remains_live() {
    let cleanup_state = RemoteCleanupState::new();
    let mut failure_receiver = cleanup_state.failure.subscribe();
    let (cleanup_sender, cleanup_receiver) = mpsc::channel(1);
    let cleanup_worker = tokio::spawn(drain_remote_multipart_cleanup(
        cleanup_receiver,
        1,
        Arc::clone(&cleanup_state),
    ));
    drop(RemoteMultipartOwner::new(
        Box::new(FailingAbortUpload),
        cleanup_sender.clone(),
        cleanup_state,
    ));

    tokio::time::timeout(Duration::from_secs(1), failure_receiver.changed())
        .await
        .expect("cleanup failure was not published while the input remained live")
        .unwrap();
    assert!(
        failure_receiver
            .borrow_and_update()
            .as_deref()
            .is_some_and(|error| error.contains("injected multipart abort failure"))
    );
    assert!(!cleanup_worker.is_finished());
    drop(cleanup_sender);
    assert!(cleanup_worker.await.unwrap().is_err());
}

#[tokio::test]
async fn shipping_scheduler_terminally_drains_cleanup_failure_without_retry() {
    let temp = tempfile::tempdir().unwrap();
    let journal = Arc::new(Journal::open(temp.path().join("journal"), identity()).unwrap());
    let payload = vec![0x7c; REMOTE_STREAM_CHUNK_BYTES + 1];
    for sequence in 1..=2 {
        let record = crate::writeback::test_util::put_record(
            sequence,
            &format!("immutable-{sequence}"),
            &payload,
            MutationMode::Create,
            FenceClass::ImmutableCreate,
            0x1000,
            1_786_435_200_000,
        );
        journal.commit_put(record, &payload).unwrap();
    }
    let failed_aborts = Arc::new(AtomicUsize::new(0));
    let pending_aborts = Arc::new(AtomicUsize::new(0));
    let store = Arc::new(SchedulerCleanupFailureStore {
        inner: Arc::new(InMemory::new()),
        multipart_calls: AtomicUsize::new(0),
        failed_aborts: Arc::clone(&failed_aborts),
        pending_aborts: Arc::clone(&pending_aborts),
    });
    let remote: Arc<dyn ObjectStore> = store.clone();
    let overlay = OverlayIndex::new(Arc::clone(&remote));
    let admission = Admission::new(64 * 1024 * 1024);
    let space = Arc::new(PhysicalSpaceSampler::new(journal.root().to_path_buf()));
    let sample = space.sample().await.unwrap();
    let ssd = Arc::new(
        SsdAdmission::recover(
            64 * 1024 * 1024,
            64,
            95,
            85,
            0,
            std::iter::empty(),
            Some(sample),
        )
        .unwrap(),
    );
    let journaler = LocalJournaler::start_with_observer_and_space(
        Arc::clone(&journal),
        admission.clone(),
        4,
        1,
        None,
        Arc::clone(&space),
    )
    .unwrap();
    let scheduler = RemoteScheduler::start(
        remote,
        journal,
        overlay,
        admission,
        ssd,
        space,
        journaler.barrier(),
        2,
    )
    .unwrap();

    let error = tokio::time::timeout(Duration::from_secs(3), scheduler.barrier().wait_remote(1))
        .await
        .expect("cleanup failure did not terminally stop remote replay")
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("remote multipart cleanup failed")
            && error
                .to_string()
                .contains("injected multipart abort failure"),
        "unexpected terminal error: {error}"
    );
    let shutdown = tokio::time::timeout(Duration::from_secs(3), scheduler.shutdown())
        .await
        .expect("scheduler shutdown did not wait for cleanup drain")
        .unwrap_err();
    assert!(
        shutdown
            .to_string()
            .contains("remote multipart cleanup failed")
            && shutdown
                .to_string()
                .contains("injected multipart abort failure"),
        "unexpected shutdown error: {shutdown}"
    );
    assert_eq!(store.multipart_calls.load(Ordering::SeqCst), 2);
    assert_eq!(failed_aborts.load(Ordering::SeqCst), 2);
    assert_eq!(pending_aborts.load(Ordering::SeqCst), 1);
    journaler.shutdown().await.unwrap();
}
