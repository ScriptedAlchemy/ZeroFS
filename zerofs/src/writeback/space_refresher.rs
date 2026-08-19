//! One owned physical-reserve refresher.
//!
//! Samples the writeback filesystem only while min-free waiters exist, using
//! [`PhysicalSpaceSampler`] as the sole generation authority.

use crate::writeback::reservation::SsdAdmission;
use crate::writeback::space_sample::PhysicalSpaceSampler;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

const SAMPLE_INTERVAL: Duration = Duration::from_millis(250);

/// Publishes increasing space-sample generations while physical waiters exist.
pub(crate) struct SpaceRefresher {
    cancel: CancellationToken,
    join: Mutex<Option<JoinHandle<()>>>,
}

impl SpaceRefresher {
    pub(crate) fn start(ssd: Arc<SsdAdmission>, space: Arc<PhysicalSpaceSampler>) -> Arc<Self> {
        let cancel = CancellationToken::new();
        let runner_cancel = cancel.clone();
        let notify = ssd.physical_waiter_notify();
        let handle = tokio::spawn(async move {
            run(runner_cancel, ssd, space, notify).await;
        });
        Arc::new(Self {
            cancel,
            join: Mutex::new(Some(handle)),
        })
    }

    // WIP on develop: landed but not wired yet.
    #[allow(dead_code)]
    pub(crate) async fn stop(&self) {
        self.shutdown().await;
    }

    pub(crate) async fn shutdown(&self) {
        self.cancel.cancel();
        let handle = self
            .join
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        if let Some(handle) = handle {
            let _ = handle.await;
        }
    }
}

async fn run(
    cancel: CancellationToken,
    ssd: Arc<SsdAdmission>,
    space: Arc<PhysicalSpaceSampler>,
    notify: Arc<tokio::sync::Notify>,
) {
    loop {
        if cancel.is_cancelled() {
            return;
        }
        if !ssd.has_min_free_waiters() {
            tokio::select! {
                _ = cancel.cancelled() => return,
                _ = notify.notified() => {}
            }
            continue;
        }
        if let Ok(sample) = space.sample().await {
            let _ = ssd.observe_sample(sample);
        }
        tokio::select! {
            _ = cancel.cancelled() => return,
            _ = tokio::time::sleep(SAMPLE_INTERVAL) => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::SpaceRefresher;
    use crate::writeback::reservation::{SsdAdmission, SsdReservationRequest};
    use crate::writeback::space_sample::{PhysicalSpaceSample, PhysicalSpaceSampler};
    use std::sync::Arc;

    fn request(physical: u64) -> SsdReservationRequest {
        SsdReservationRequest {
            ssd_reservation_bytes: 10,
            physical_reservation_bytes: physical,
            operations: 1,
        }
    }

    fn sample(generation: u64, available_bytes: u64) -> PhysicalSpaceSample {
        PhysicalSpaceSample {
            generation,
            available_bytes,
        }
    }

    #[tokio::test]
    async fn external_cleanup_wakes_physical_waiter() {
        let admission = Arc::new(SsdAdmission::new(1_000, 8, 100, 50, 100).unwrap());
        let first = admission
            .reserve(request(40), sample(1, 150))
            .await
            .unwrap();
        let blocked = tokio::spawn({
            let admission = Arc::clone(&admission);
            async move { admission.reserve(request(20), sample(1, 150)).await }
        });
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        assert!(!blocked.is_finished());
        assert!(admission.has_min_free_waiters());

        let temp = tempfile::tempdir().unwrap();
        let space = Arc::new(PhysicalSpaceSampler::new(temp.path()));
        let refresher = SpaceRefresher::start(Arc::clone(&admission), space);
        let granted = tokio::time::timeout(std::time::Duration::from_secs(2), blocked)
            .await
            .expect("refresher must publish a fresh sample for the physical waiter")
            .expect("physical waiter must join")
            .expect("physical waiter must be granted");
        assert_eq!(granted.request().physical_reservation_bytes, 20);
        assert_eq!(admission.outstanding_physical_claims(), 60);
        drop(first);
        refresher.shutdown().await;
    }

    #[tokio::test]
    async fn one_refresher_is_joined_on_shutdown() {
        let admission = Arc::new(SsdAdmission::new(1_000, 8, 100, 50, 10).unwrap());
        let temp = tempfile::tempdir().unwrap();
        let space = Arc::new(PhysicalSpaceSampler::new(temp.path()));
        let refresher = SpaceRefresher::start(admission, space);
        tokio::time::timeout(std::time::Duration::from_secs(1), refresher.stop())
            .await
            .expect("shutdown must join the owned refresher");
        tokio::time::timeout(std::time::Duration::from_secs(1), refresher.stop())
            .await
            .expect("a second shutdown must be a no-op join");
    }
}
