use super::*;
use crate::retrying_object_store::{PermanentError, has_permanent_source};

#[derive(Debug)]
struct FailedAbortUpload {
    already_resolved: bool,
    attempts: Arc<AtomicUsize>,
}

#[async_trait::async_trait]
impl MultipartUpload for FailedAbortUpload {
    fn put_part(&mut self, _: PutPayload) -> object_store::UploadPart {
        unreachable!("the publication already failed")
    }

    async fn complete(&mut self) -> object_store::Result<PutResult> {
        unreachable!("the publication already failed")
    }

    async fn abort(&mut self) -> object_store::Result<()> {
        let attempt = self.attempts.fetch_add(1, Ordering::SeqCst);
        if self.already_resolved {
            return Err(object_store::Error::NotFound {
                path: "already-resolved-upload".into(),
                source: "upload no longer exists".into(),
            });
        }
        if attempt == 0 {
            return Err(generic_error("transient abort response failure"));
        }
        Ok(())
    }
}

async fn assert_abort_preserves_original_failure(already_resolved: bool) {
    let cleanup_state = RemoteCleanupState::new();
    let (sender, receiver) = mpsc::channel(1);
    let attempts = Arc::new(AtomicUsize::new(0));
    let mut owner = RemoteMultipartOwner::new(
        Box::new(FailedAbortUpload {
            already_resolved,
            attempts: attempts.clone(),
        }),
        sender,
        cleanup_state.clone(),
    );
    let record = crate::writeback::test_util::put_record(
        1,
        "root/failed-multipart",
        b"payload",
        MutationMode::Create,
        FenceClass::ImmutableCreate,
        0,
        0,
    );
    let original = object_store::Error::Generic {
        store: "failed-upload",
        source: Box::new(PermanentError::new(std::io::Error::from_raw_os_error(
            libc::EIO,
        ))),
    };
    let result = abort_remote_multipart::<()>(&mut owner, &record, original).await;

    // A failed abort retains the upload. Its cleanup worker can later settle
    // it, but that success cannot make the original media failure retryable.
    drop(owner);
    assert_eq!(cleanup_state.pending.load(Ordering::Acquire), 1);
    tokio::time::timeout(
        Duration::from_secs(1),
        drain_remote_multipart_cleanup(receiver, 1, cleanup_state.clone(), None),
    )
    .await
    .expect("the retained multipart cleanup must finish")
    .unwrap();
    assert_eq!(cleanup_state.pending.load(Ordering::Acquire), 0);
    assert_eq!(attempts.load(Ordering::SeqCst), 2);
    let error = result.unwrap_err();
    assert!(
        has_permanent_source(&error),
        "abort failure erased the original permanent error: {error}"
    );
    assert!(is_terminal_remote_error(&error));
}

#[tokio::test]
async fn already_resolved_abort_keeps_original_permanent_failure() {
    assert_abort_preserves_original_failure(true).await;
}

#[tokio::test]
async fn transient_abort_keeps_original_permanent_failure_after_cleanup() {
    assert_abort_preserves_original_failure(false).await;
}
