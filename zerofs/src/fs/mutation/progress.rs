//! Gap-free mutation materialization progress.

use crate::fs::mutation::types::{MutationCutoff, MutationError, MutationIncarnation};
use std::collections::BTreeSet;
use std::sync::{Arc, Mutex, MutexGuard};
use tokio::sync::Notify;

/// Contiguous materialized prefix for one mutation incarnation.
#[derive(Clone)]
pub(crate) struct MutationProgress {
    inner: Arc<ProgressInner>,
}

struct ProgressInner {
    incarnation: MutationIncarnation,
    state: Mutex<ProgressState>,
    notify: Notify,
}

struct ProgressState {
    materialized_through: u64,
    pending: BTreeSet<u64>,
    terminal: Option<MutationError>,
}

impl MutationProgress {
    pub(crate) fn new(incarnation: MutationIncarnation) -> Self {
        Self {
            inner: Arc::new(ProgressInner {
                incarnation,
                state: Mutex::new(ProgressState {
                    materialized_through: 0,
                    pending: BTreeSet::new(),
                    terminal: None,
                }),
                notify: Notify::new(),
            }),
        }
    }

    pub(crate) fn materialized_through(&self) -> u64 {
        lock(&self.inner.state).materialized_through
    }

    pub(crate) fn check(&self) -> Result<(), MutationError> {
        match &lock(&self.inner.state).terminal {
            Some(error) => Err(error.clone()),
            None => Ok(()),
        }
    }

    pub(crate) fn poison(&self, message: impl Into<String>) {
        {
            let mut state = lock(&self.inner.state);
            if state.terminal.is_none() {
                state.terminal = Some(MutationError::Poisoned(message.into()));
            }
        }
        self.inner.notify.notify_waiters();
    }

    pub(crate) fn record_materialized(&self, cutoff: MutationCutoff) -> Result<(), MutationError> {
        if cutoff.mutation_incarnation != self.inner.incarnation {
            return Err(MutationError::StaleIncarnation);
        }
        {
            let mut state = lock(&self.inner.state);
            if let Some(error) = &state.terminal {
                return Err(error.clone());
            }
            if cutoff.sequence <= state.materialized_through {
                return Ok(());
            }
            state.pending.insert(cutoff.sequence);
            while state.pending.contains(&(state.materialized_through + 1)) {
                state.materialized_through += 1;
                let retired = state.materialized_through;
                state.pending.remove(&retired);
            }
        }
        self.inner.notify.notify_waiters();
        Ok(())
    }

    pub(crate) async fn wait_materialized(
        &self,
        cutoff: MutationCutoff,
    ) -> Result<(), MutationError> {
        if cutoff.mutation_incarnation != self.inner.incarnation {
            return Err(MutationError::StaleIncarnation);
        }
        loop {
            let notified = self.inner.notify.notified();
            {
                let state = lock(&self.inner.state);
                if let Some(error) = &state.terminal {
                    return Err(error.clone());
                }
                if state.materialized_through >= cutoff.sequence {
                    return Ok(());
                }
            }
            notified.await;
        }
    }
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|error| error.into_inner())
}

#[cfg(test)]
mod tests {
    use super::MutationProgress;
    use crate::fs::mutation::admission::{
        PreparationAbort, PreparationGate, PreparationGuard, RawMutationBudget,
    };
    use crate::fs::mutation::request_cache::{RequestCache, RequestLookup};
    use crate::fs::mutation::types::{
        ConflictKey, ConflictScope, MutationCutoff, MutationError, MutationIncarnation,
        RequestFingerprint, RequestIdentity, RequestLifetime,
    };
    use std::sync::Arc;
    use std::time::Duration;

    fn cutoff(incarnation: MutationIncarnation, sequence: u64) -> MutationCutoff {
        MutationCutoff {
            mutation_incarnation: incarnation,
            sequence,
        }
    }

    #[tokio::test]
    async fn out_of_order_completion_advances_gap_free_prefix() {
        let incarnation = MutationIncarnation::new();
        let progress = MutationProgress::new(incarnation);

        progress
            .record_materialized(cutoff(incarnation, 2))
            .unwrap();
        progress
            .record_materialized(cutoff(incarnation, 3))
            .unwrap();
        assert_eq!(
            progress.materialized_through(),
            0,
            "sequence 1 is still missing; the prefix must not advance"
        );

        let waiter = tokio::spawn({
            let progress = progress.clone();
            async move { progress.wait_materialized(cutoff(incarnation, 3)).await }
        });
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(!waiter.is_finished());

        progress
            .record_materialized(cutoff(incarnation, 1))
            .unwrap();
        assert_eq!(progress.materialized_through(), 3);
        tokio::time::timeout(Duration::from_secs(1), waiter)
            .await
            .unwrap()
            .unwrap()
            .unwrap();

        let stale = progress.record_materialized(cutoff(MutationIncarnation::new(), 4));
        assert_eq!(stale, Err(MutationError::StaleIncarnation));
    }

    #[tokio::test]
    async fn terminal_wakes_all_waiters() {
        let incarnation = MutationIncarnation::new();
        let budget = RawMutationBudget::new(4, 4);
        let gate = PreparationGate::new(incarnation);
        let progress = MutationProgress::new(incarnation);
        let cache = RequestCache::new(8);

        let held = budget.acquire(3).await.unwrap();
        let guard_permit = budget.acquire(1).await.unwrap();
        let admission_waiter = tokio::spawn({
            let budget = budget.clone();
            async move { budget.acquire(1).await.map(|_| ()) }
        });
        let guard = PreparationGuard::new(
            Arc::clone(&gate),
            ConflictScope::single(ConflictKey::Inode(1)),
            guard_permit,
            match cache
                .lookup_or_reserve(
                    RequestIdentity::Nbd {
                        connection_incarnation: 7,
                        handle: 1,
                    },
                    RequestFingerprint::from_parts(&[&[1]]),
                    RequestLifetime::InFlightOnly,
                )
                .unwrap()
            {
                RequestLookup::Vacant(vacancy) => vacancy.begin_pending(),
                other => panic!("expected vacant lookup, got {other:?}"),
            },
        )
        .unwrap();
        let preparation_waiter = tokio::spawn({
            let gate = Arc::clone(&gate);
            let closing = ConflictScope::single(ConflictKey::Inode(1));
            async move { gate.close_scope(&closing).await }
        });
        let progress_waiter = tokio::spawn({
            let progress = progress.clone();
            async move { progress.wait_materialized(cutoff(incarnation, 5)).await }
        });

        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(!admission_waiter.is_finished());
        assert!(!preparation_waiter.is_finished());
        assert!(!progress_waiter.is_finished());

        budget.poison("volatile journal failed");
        gate.poison("volatile journal failed");
        progress.poison("volatile journal failed");

        for error in [
            admission_waiter.await.unwrap().unwrap_err(),
            preparation_waiter.await.unwrap().unwrap_err(),
            progress_waiter.await.unwrap().unwrap_err(),
        ] {
            assert!(
                matches!(error, MutationError::Poisoned(ref message)
                    if message == "volatile journal failed"),
                "terminal must surface poison, not fabricate success: {error:?}"
            );
        }

        guard
            .abort(PreparationAbort::TransportCancellation)
            .unwrap();
        drop(held);
    }
}
