use super::*;
use crate::writeback::model::JournalIdentity;
use async_trait::async_trait;
use futures::stream::BoxStream;
use object_store::local::LocalFileSystem;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, ObjectMeta, PutOptions, UploadPart,
};
use std::fmt::{Display, Formatter};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
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
    let (cleanup_sender, cleanup_receiver) = mpsc::unbounded_channel();
    let cleanup_worker = tokio::spawn(drain_remote_multipart_cleanup(cleanup_receiver, 1));
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
    );

    drop(owner);
    drop(cleanup_sender);
    entered.notified().await;
    assert!(!cleanup_worker.is_finished());
    release.notify_one();
    cleanup_worker.await.unwrap().unwrap();
    assert_eq!(aborts.load(Ordering::SeqCst), 1);
}
