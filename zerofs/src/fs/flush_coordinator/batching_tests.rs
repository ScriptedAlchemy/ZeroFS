use super::*;
use crate::fs::ZeroFS;
use crate::fs::mutation::types::MutationIncarnation;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;
use tokio::sync::Notify;

fn cutoff(sequence: u64) -> MutationCutoff {
    MutationCutoff {
        mutation_incarnation: MutationIncarnation::new(),
        sequence,
    }
}

async fn coordinator() -> (ZeroFS, FlushCoordinator, Arc<AtomicUsize>) {
    let fs = ZeroFS::new_in_memory().await.unwrap();
    let coordinator = FlushCoordinator::new(Arc::clone(&fs.db));
    let captures = Arc::new(AtomicUsize::new(0));
    coordinator.set_object_capture({
        let captures = Arc::clone(&captures);
        Arc::new(move || {
            captures.fetch_add(1, Ordering::SeqCst);
            ObjectCoverage::DirectRemote
        })
    });
    (fs, coordinator, captures)
}

fn submit(
    coordinator: &FlushCoordinator,
    cutoff: MutationCutoff,
    target: DurabilityTarget,
) -> JoinHandle<Result<DurabilityReceipt, DurabilityError>> {
    let coordinator = coordinator.clone();
    tokio::spawn(async move { coordinator.durable_through(cutoff, target).await })
}

async fn until(mut ready: impl FnMut() -> bool) {
    tokio::time::timeout(Duration::from_secs(2), async {
        while !ready() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("durability task did not reach the expected boundary");
}

async fn pending_waiters(coordinator: &FlushCoordinator, expected: usize) {
    until(|| {
        coordinator
            .pending_capture
            .lock()
            .unwrap()
            .as_ref()
            .and_then(Weak::upgrade)
            .is_some_and(|batch| batch.result.receiver_count() == expected)
    })
    .await;
}

async fn receipt(
    task: JoinHandle<Result<DurabilityReceipt, DurabilityError>>,
) -> DurabilityReceipt {
    tokio::time::timeout(Duration::from_secs(2), task)
        .await
        .expect("durability waiter did not finish")
        .expect("durability waiter panicked")
        .expect("durability wait failed")
}

#[tokio::test]
async fn overlapping_cutoffs_share_one_capture_without_caching_completed_results() {
    let (fs, coordinator, captures) = coordinator().await;
    let seals = Arc::new(AtomicUsize::new(0));
    coordinator.set_sealer({
        let seals = Arc::clone(&seals);
        Arc::new(move || {
            seals.fetch_add(1, Ordering::SeqCst);
            Box::pin(async { Ok(()) })
        })
    });
    let barrier = fs.db.flush_barrier().write_owned().await;
    let first_cutoff = cutoff(0);
    let callers: Vec<_> = (0..8)
        .map(|sequence| {
            let cutoff = MutationCutoff {
                sequence,
                ..first_cutoff
            };
            (
                cutoff,
                submit(&coordinator, cutoff, DurabilityTarget::LocalSsd),
            )
        })
        .collect();
    pending_waiters(&coordinator, callers.len()).await;
    drop(barrier);
    for (cutoff, task) in callers {
        let receipt = receipt(task).await;
        assert_eq!(receipt.mutation_cutoff, cutoff);
        assert_eq!(receipt.target, DurabilityTarget::LocalSsd);
    }
    assert_eq!(seals.load(Ordering::SeqCst), 1);
    assert_eq!(captures.load(Ordering::SeqCst), 1);

    // Materialized-mode writes keep cutoff zero. A completed receipt cannot
    // stand in for a new flush even when the next caller has that same cutoff.
    let _receipt = receipt(submit(
        &coordinator,
        first_cutoff,
        DurabilityTarget::LocalSsd,
    ))
    .await;
    assert_eq!(seals.load(Ordering::SeqCst), 2);
    assert_eq!(captures.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn caller_after_seal_starts_uses_the_next_capture() {
    let (_fs, coordinator, captures) = coordinator().await;
    let seal_entered = Arc::new(Notify::new());
    let release_seal = Arc::new(Notify::new());
    let seals = Arc::new(AtomicUsize::new(0));
    coordinator.set_sealer({
        let seal_entered = Arc::clone(&seal_entered);
        let release_seal = Arc::clone(&release_seal);
        let seals = Arc::clone(&seals);
        Arc::new(move || {
            let first = seals.fetch_add(1, Ordering::SeqCst) == 0;
            let seal_entered = Arc::clone(&seal_entered);
            let release_seal = Arc::clone(&release_seal);
            Box::pin(async move {
                if first {
                    seal_entered.notify_one();
                    release_seal.notified().await;
                }
                Ok(())
            })
        })
    });
    let first = submit(&coordinator, cutoff(0), DurabilityTarget::LocalSsd);
    seal_entered.notified().await;
    let later = submit(&coordinator, cutoff(0), DurabilityTarget::LocalSsd);
    pending_waiters(&coordinator, 1).await;
    release_seal.notify_one();
    let _first = receipt(first).await;
    let _later = receipt(later).await;
    assert_eq!(seals.load(Ordering::SeqCst), 2);
    assert_eq!(captures.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn materialization_must_finish_before_joining_a_capture() {
    let (fs, coordinator, captures) = coordinator().await;
    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    coordinator.set_materialize({
        let entered = Arc::clone(&entered);
        let release = Arc::clone(&release);
        Arc::new(move |cutoff| {
            let entered = Arc::clone(&entered);
            let release = Arc::clone(&release);
            Box::pin(async move {
                if cutoff.sequence == 2 {
                    entered.notify_one();
                    release.notified().await;
                }
                Ok(())
            })
        })
    });
    let barrier = fs.db.flush_barrier().write_owned().await;
    let first = submit(&coordinator, cutoff(1), DurabilityTarget::LocalSsd);
    pending_waiters(&coordinator, 1).await;
    let blocked = submit(&coordinator, cutoff(2), DurabilityTarget::LocalSsd);
    entered.notified().await;
    pending_waiters(&coordinator, 1).await;
    drop(barrier);
    let _first = receipt(first).await;
    assert!(!blocked.is_finished());
    assert_eq!(captures.load(Ordering::SeqCst), 1);
    release.notify_one();
    let _blocked = receipt(blocked).await;
    assert_eq!(captures.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn shared_capture_keeps_local_and_remote_waits_independent() {
    let (fs, coordinator, captures) = coordinator().await;
    let remote_entered = Arc::new(Notify::new());
    let release_remote = Arc::new(Notify::new());
    coordinator.set_object_wait({
        let barrier = fs.db.flush_barrier();
        let remote_entered = Arc::clone(&remote_entered);
        let release_remote = Arc::clone(&release_remote);
        Arc::new(move |_, target| {
            assert!(barrier.try_write().is_ok());
            let remote_entered = Arc::clone(&remote_entered);
            let release_remote = Arc::clone(&release_remote);
            Box::pin(async move {
                if target == DurabilityTarget::RemoteBackend {
                    remote_entered.notify_one();
                    release_remote.notified().await;
                }
                Ok(())
            })
        })
    });
    let barrier = fs.db.flush_barrier().write_owned().await;
    let local = submit(&coordinator, cutoff(1), DurabilityTarget::LocalSsd);
    let remote = submit(&coordinator, cutoff(2), DurabilityTarget::RemoteBackend);
    pending_waiters(&coordinator, 2).await;
    drop(barrier);
    remote_entered.notified().await;
    assert_eq!(receipt(local).await.target, DurabilityTarget::LocalSsd);
    assert!(!remote.is_finished());
    release_remote.notify_one();
    assert_eq!(
        receipt(remote).await.target,
        DurabilityTarget::RemoteBackend
    );
    assert_eq!(captures.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn shared_seal_failure_preserves_errors_and_allows_retry() {
    for error in [FsError::NoSpace, FsError::ShuttingDown, FsError::IoError] {
        let (fs, coordinator, captures) = coordinator().await;
        let failing = Arc::new(AtomicBool::new(true));
        let seals = Arc::new(AtomicUsize::new(0));
        let waits = Arc::new(AtomicUsize::new(0));
        coordinator.set_sealer({
            let failing = Arc::clone(&failing);
            let seals = Arc::clone(&seals);
            Arc::new(move || {
                seals.fetch_add(1, Ordering::SeqCst);
                let failed = failing.load(Ordering::SeqCst);
                Box::pin(async move { if failed { Err(error) } else { Ok(()) } })
            })
        });
        coordinator.set_object_wait({
            let waits = Arc::clone(&waits);
            Arc::new(move |_, _| {
                waits.fetch_add(1, Ordering::SeqCst);
                Box::pin(async { Ok(()) })
            })
        });
        let barrier = fs.db.flush_barrier().write_owned().await;
        let callers: Vec<_> = (0..4)
            .map(|sequence| submit(&coordinator, cutoff(sequence), DurabilityTarget::LocalSsd))
            .collect();
        pending_waiters(&coordinator, callers.len()).await;
        drop(barrier);
        for caller in callers {
            let result = tokio::time::timeout(Duration::from_secs(2), caller)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(FsError::from(result.unwrap_err()), error);
        }
        assert_eq!(seals.load(Ordering::SeqCst), 1);
        assert_eq!(captures.load(Ordering::SeqCst), 0);
        assert_eq!(waits.load(Ordering::SeqCst), 0);
        failing.store(false, Ordering::SeqCst);
        let _receipt = receipt(submit(&coordinator, cutoff(0), DurabilityTarget::LocalSsd)).await;
        assert_eq!(seals.load(Ordering::SeqCst), 2);
        assert_eq!(captures.load(Ordering::SeqCst), 1);
        assert_eq!(waits.load(Ordering::SeqCst), 1);
    }
}

#[tokio::test]
async fn shared_database_failure_preserves_the_original_cause() {
    let (fs, coordinator, captures) = coordinator().await;
    let barrier = fs.db.flush_barrier().write_owned().await;
    fs.db.mark_closing();
    let callers: Vec<_> = (0..4)
        .map(|sequence| submit(&coordinator, cutoff(sequence), DurabilityTarget::LocalSsd))
        .collect();
    pending_waiters(&coordinator, callers.len()).await;
    drop(barrier);
    for caller in callers {
        let result = tokio::time::timeout(Duration::from_secs(2), caller)
            .await
            .unwrap()
            .unwrap();
        let Err(DurabilityError::FilesystemFlush(error)) = result else {
            panic!("expected the database flush failure: {result:?}");
        };
        assert!(
            error
                .chain()
                .any(|cause| { cause.downcast_ref::<FsError>() == Some(&FsError::ShuttingDown) })
        );
    }
    assert_eq!(captures.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn cancelling_the_initiator_keeps_live_batch_waiters() {
    let (fs, coordinator, captures) = coordinator().await;
    let barrier = fs.db.flush_barrier().write_owned().await;
    let first = submit(&coordinator, cutoff(0), DurabilityTarget::LocalSsd);
    pending_waiters(&coordinator, 1).await;
    let sibling = submit(&coordinator, cutoff(1), DurabilityTarget::LocalSsd);
    pending_waiters(&coordinator, 2).await;
    first.abort();
    assert!(first.await.unwrap_err().is_cancelled());
    pending_waiters(&coordinator, 1).await;
    drop(barrier);
    let _receipt = receipt(sibling).await;
    assert_eq!(captures.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn cancelling_all_pending_waiters_releases_the_owner_without_sealing() {
    let (fs, coordinator, captures) = coordinator().await;
    let barrier = fs.db.flush_barrier().write_owned().await;
    let abandoned = submit(&coordinator, cutoff(0), DurabilityTarget::LocalSsd);
    pending_waiters(&coordinator, 1).await;
    let owner = coordinator.pending_capture.lock().unwrap().clone().unwrap();
    abandoned.abort();
    assert!(abandoned.await.unwrap_err().is_cancelled());
    until(|| coordinator.pending_capture.lock().unwrap().is_none()).await;
    until(|| owner.strong_count() == 0).await;
    assert_eq!(captures.load(Ordering::SeqCst), 0);
    let next = submit(&coordinator, cutoff(1), DurabilityTarget::LocalSsd);
    pending_waiters(&coordinator, 1).await;
    drop(barrier);
    let _receipt = receipt(next).await;
    assert_eq!(captures.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn cancelled_stale_batch_cannot_remove_a_newer_pending_batch() {
    let (fs, coordinator, captures) = coordinator().await;
    let barrier = fs.db.flush_barrier().write_owned().await;
    let (old_result, old_receiver) = watch::channel(None);
    let old = Arc::new(CaptureBatch { result: old_result });
    drop(old_receiver);
    let (new_result, _new_receiver) = watch::channel(None);
    let next = Arc::new(CaptureBatch { result: new_result });
    *coordinator.pending_capture.lock().unwrap() = Some(Arc::downgrade(&next));
    // Exercise cleanup with a superseded owner: its cancellation must not
    // unlink the batch now accepting callers, even if both wait for the lock.
    tokio::time::timeout(
        Duration::from_secs(2),
        FlushCoordinator::run_capture_batch(
            Arc::clone(&fs.db),
            Arc::clone(&coordinator.seal_hook),
            Arc::clone(&coordinator.object_capture),
            Arc::clone(&coordinator.pending_capture),
            old,
        ),
    )
    .await
    .unwrap();
    assert_eq!(
        coordinator
            .pending_capture
            .lock()
            .unwrap()
            .as_ref()
            .unwrap()
            .as_ptr(),
        Arc::as_ptr(&next),
    );
    assert_eq!(captures.load(Ordering::SeqCst), 0);
    *coordinator.pending_capture.lock().unwrap() = None;
    drop(barrier);
}
