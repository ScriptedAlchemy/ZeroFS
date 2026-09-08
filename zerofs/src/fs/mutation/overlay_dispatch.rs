//! Dispatch coordination for one logical volatile filesystem write.

use super::admission::AcceptedMutation;
use super::fence::MutationCoordinator;
use super::materializer::Materializer;
use super::types::{PreparedBatchResult, PreparedWriteBatch};
use super::volatile_overlay::{OverlayError, OverlayResult};
use crate::fs::ZeroFS;
use crate::fs::errors::FsError;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use tokio::sync::{Notify, oneshot};
use tokio_util::sync::CancellationToken;

pub(crate) struct PendingDispatch {
    remaining_members: AtomicUsize,
    accepted: Mutex<Option<oneshot::Receiver<AcceptedMutation>>>,
    accepted_write: Mutex<Option<crate::dedup::AcceptedWriteLifecycle>>,
    result: Mutex<Option<OverlayResult<()>>>,
    changed: Notify,
    cancelled: CancellationToken,
}

struct DispatchCompletion<'a> {
    dispatch: &'a PendingDispatch,
    armed: bool,
}

impl DispatchCompletion<'_> {
    fn finish(mut self, result: OverlayResult<()>) -> OverlayResult<()> {
        self.dispatch.set_result(result);
        self.armed = false;
        result
    }
}

impl Drop for DispatchCompletion<'_> {
    fn drop(&mut self) {
        if self.armed {
            self.dispatch.set_result(Err(OverlayError::IoError));
        }
    }
}

struct PublishedDispatchCompletion {
    coordinator: Arc<MutationCoordinator>,
    armed: bool,
}

impl Drop for PublishedDispatchCompletion {
    fn drop(&mut self) {
        if self.armed {
            self.coordinator
                .poison("published overlay dispatch was cancelled");
        }
    }
}

impl PendingDispatch {
    pub(crate) fn new(
        member_count: usize,
        accepted: oneshot::Receiver<AcceptedMutation>,
    ) -> Arc<Self> {
        Arc::new(Self {
            remaining_members: AtomicUsize::new(member_count),
            accepted: Mutex::new(Some(accepted)),
            accepted_write: Mutex::new(None),
            result: Mutex::new(None),
            changed: Notify::new(),
            cancelled: CancellationToken::new(),
        })
    }

    pub(crate) async fn arrive_held(
        &self,
        fs: Arc<ZeroFS>,
        materializer: &Materializer,
    ) -> OverlayResult<()> {
        if let Some(result) = *self.result.lock().expect("pending dispatch poisoned") {
            return result;
        }
        let remaining = self.remaining_members.fetch_sub(1, Ordering::AcqRel);
        if remaining == 0 {
            return Err(OverlayError::IoError);
        }
        if remaining == 1 {
            let completion = DispatchCompletion {
                dispatch: self,
                armed: true,
            };
            return completion.finish(self.dispatch_held(fs, materializer).await);
        }
        loop {
            let changed = self.changed.notified();
            if let Some(result) = *self.result.lock().expect("pending dispatch poisoned") {
                return result;
            }
            changed.await;
        }
    }

    pub(crate) fn cancel_unpublished(&self) {
        self.cancelled.cancel();
        self.accepted
            .lock()
            .expect("pending dispatch poisoned")
            .take();
        self.accepted_write
            .lock()
            .expect("pending dispatch poisoned")
            .take();
        self.set_result(Ok(()));
    }

    pub(crate) fn fail_terminal(&self) {
        self.set_result(Err(OverlayError::IoError));
        self.cancelled.cancel();
        self.accepted
            .lock()
            .expect("pending dispatch poisoned")
            .take();
        if let Some(accepted_write) = self
            .accepted_write
            .lock()
            .expect("pending dispatch poisoned")
            .take()
        {
            accepted_write.finish(false);
        }
    }

    pub(crate) fn install_accepted_write(
        &self,
        accepted_write: crate::dedup::AcceptedWriteLifecycle,
    ) {
        let previous = self
            .accepted_write
            .lock()
            .expect("pending dispatch poisoned")
            .replace(accepted_write);
        debug_assert!(previous.is_none());
    }

    fn set_result(&self, result: OverlayResult<()>) {
        let mut current = self.result.lock().expect("pending dispatch poisoned");
        if current.is_none() {
            *current = Some(result);
        }
        drop(current);
        self.changed.notify_waiters();
    }

    async fn dispatch_held(
        &self,
        fs: Arc<ZeroFS>,
        materializer: &Materializer,
    ) -> OverlayResult<()> {
        let receiver = self
            .accepted
            .lock()
            .expect("pending dispatch poisoned")
            .take()
            .ok_or(OverlayError::IoError)?;
        let accepted = tokio::select! {
            biased;
            _ = self.cancelled.cancelled() => return Ok(()),
            accepted = receiver => accepted.map_err(|_| OverlayError::IoError)?,
        };
        let (request, batch, raw_permit, cutoff) = accepted.into_parts();
        let accepted_write = self
            .accepted_write
            .lock()
            .expect("pending dispatch poisoned")
            .take();
        let reply = prepared_batch_result(&batch).with_cutoff(cutoff);
        let coordinator = fs
            .mutation_coordinator
            .get()
            .cloned()
            .ok_or(OverlayError::IoError)?;
        let mut completion = PublishedDispatchCompletion {
            coordinator: Arc::clone(&coordinator),
            armed: true,
        };
        let result = materializer.apply_held(cutoff, batch, accepted_write).await;
        coordinator.request_cache().complete(
            request,
            result.as_ref().map(|_| reply).map_err(|_| FsError::IoError),
        );
        drop(raw_permit);
        completion.armed = false;
        result.map_err(|_| OverlayError::IoError)
    }
}

fn prepared_batch_result(batch: &PreparedWriteBatch) -> PreparedBatchResult {
    batch
        .replayed
        .clone()
        .unwrap_or_else(|| PreparedBatchResult {
            members: batch
                .members
                .iter()
                .map(|member| (member.id, member.post_attrs.clone()))
                .collect(),
            cutoff: None,
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fs::mutation::admission::PreparationGate;
    use crate::fs::mutation::progress::MutationProgress;
    use crate::fs::mutation::types::{MutationError, MutationIncarnation};

    #[test]
    fn cancelled_published_dispatch_poison_progress_instead_of_leaving_a_hole() {
        let incarnation = MutationIncarnation::new();
        let coordinator = MutationCoordinator::new(
            PreparationGate::new(incarnation),
            MutationProgress::new(incarnation),
        );
        drop(PublishedDispatchCompletion {
            coordinator: Arc::clone(&coordinator),
            armed: true,
        });

        assert!(matches!(
            coordinator.progress().check(),
            Err(MutationError::Poisoned(_))
        ));
    }
}
