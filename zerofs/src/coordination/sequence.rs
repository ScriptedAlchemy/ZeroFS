//! Shared durability-barrier primitive.
//!
//! The local journaler and the remote scheduler both publish the same progress
//! shape over a `tokio::sync::watch` channel and wait on it with the same loop;
//! only the error vocabulary differs, and that is supplied by [`BarrierError`].
//!
//! Every wait is bound to the publisher's journal incarnation. A sequence
//! number from a previous incarnation can never complete against a later one.

use crate::writeback::model::Sequence;
use std::marker::PhantomData;
use tokio::sync::watch;
use uuid::Uuid;

/// Durability progress published by one writeback stage.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SequenceProgress {
    /// Journal incarnation that owns `sequence`.
    pub(crate) incarnation: Uuid,
    /// Highest sequence that is durable at this stage.
    pub(crate) sequence: Sequence,
    /// Set once the stage fails terminally; never cleared afterwards.
    pub(crate) terminal_error: Option<String>,
    /// Set once the stage can no longer make progress.
    pub(crate) closed: bool,
}

/// The error vocabulary a stage exposes for barrier waits.
pub(crate) trait BarrierError {
    /// The stage stopped before reaching the requested sequence.
    fn closed() -> Self;
    /// The stage failed terminally with `error`.
    fn terminal(error: String) -> Self;
    /// The caller asked about a different journal incarnation than the publisher.
    fn stale_incarnation() -> Self;
}

/// A durability barrier over a stage's published [`SequenceProgress`].
#[derive(Debug, Clone)]
pub(crate) struct SequenceBarrier<E> {
    progress: watch::Receiver<SequenceProgress>,
    _error: PhantomData<fn() -> E>,
}

impl<E: BarrierError> SequenceBarrier<E> {
    pub(crate) fn new(progress: watch::Receiver<SequenceProgress>) -> Self {
        Self {
            progress,
            _error: PhantomData,
        }
    }

    /// An independent receiver on the same progress channel.
    pub(crate) fn watcher(&self) -> watch::Receiver<SequenceProgress> {
        self.progress.clone()
    }

    /// The sequence that is currently durable at this stage.
    pub(crate) fn sequence(&self) -> Sequence {
        self.progress.borrow().sequence
    }

    /// The journal incarnation currently published by this stage.
    pub(crate) fn incarnation(&self) -> Uuid {
        self.progress.borrow().incarnation
    }

    /// A consistent snapshot of the published progress.
    pub(crate) fn snapshot(&self) -> SequenceProgress {
        self.progress.borrow().clone()
    }

    /// The stage's terminal failure, if it has one.
    pub(crate) fn current_terminal(&self) -> Option<E> {
        self.progress
            .borrow()
            .terminal_error
            .clone()
            .map(E::terminal)
    }

    /// The stage's terminal failure, or `closed` when it merely stopped.
    pub(crate) fn terminal_or_closed(&self) -> E {
        self.current_terminal().unwrap_or_else(E::closed)
    }

    /// Wait until `sequence` is durable in `incarnation`.
    pub(crate) async fn wait(&self, incarnation: Uuid, sequence: Sequence) -> Result<(), E> {
        let mut progress = self.progress.clone();
        loop {
            let state = progress.borrow().clone();
            if state.incarnation != incarnation {
                return Err(E::stale_incarnation());
            }
            if state.sequence >= sequence {
                return Ok(());
            }
            if let Some(error) = state.terminal_error {
                return Err(E::terminal(error));
            }
            if state.closed {
                return Err(E::closed());
            }
            progress.changed().await.map_err(|_| E::closed())?;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{BarrierError, SequenceBarrier, SequenceProgress};
    use tokio::sync::watch;
    use uuid::Uuid;

    #[derive(Debug, Clone, PartialEq, Eq)]
    enum TestError {
        Closed,
        Terminal(String),
        StaleIncarnation,
    }

    impl BarrierError for TestError {
        fn closed() -> Self {
            Self::Closed
        }

        fn terminal(error: String) -> Self {
            Self::Terminal(error)
        }

        fn stale_incarnation() -> Self {
            Self::StaleIncarnation
        }
    }

    fn progress(incarnation: Uuid, sequence: u64) -> SequenceProgress {
        SequenceProgress {
            incarnation,
            sequence,
            terminal_error: None,
            closed: false,
        }
    }

    #[tokio::test]
    async fn wait_rejects_a_stale_incarnation() {
        let incarnation = Uuid::from_u128(1);
        let (sender, receiver) = watch::channel(progress(incarnation, 4));
        let barrier = SequenceBarrier::<TestError>::new(receiver);

        let error = barrier.wait(Uuid::from_u128(2), 1).await.unwrap_err();
        assert_eq!(error, TestError::StaleIncarnation);

        sender.send_modify(|state| state.sequence = 8);
        let error = barrier.wait(Uuid::from_u128(2), 8).await.unwrap_err();
        assert_eq!(error, TestError::StaleIncarnation);
    }

    #[tokio::test]
    async fn wait_accepts_the_current_incarnation() {
        let incarnation = Uuid::from_u128(7);
        let (sender, receiver) = watch::channel(progress(incarnation, 0));
        let barrier = SequenceBarrier::<TestError>::new(receiver);
        sender.send_modify(|state| state.sequence = 3);
        barrier.wait(incarnation, 3).await.unwrap();
    }

    #[tokio::test]
    async fn wait_fails_when_the_publisher_rotates_incarnation() {
        let first = Uuid::from_u128(1);
        let (sender, receiver) = watch::channel(progress(first, 2));
        let barrier = SequenceBarrier::<TestError>::new(receiver);
        let waiter = tokio::spawn({
            let barrier = barrier.clone();
            async move { barrier.wait(first, 9).await }
        });
        tokio::task::yield_now().await;
        sender.send_modify(|state| {
            state.incarnation = Uuid::from_u128(2);
            state.sequence = 9;
        });
        assert_eq!(
            waiter.await.unwrap().unwrap_err(),
            TestError::StaleIncarnation
        );
    }
}
