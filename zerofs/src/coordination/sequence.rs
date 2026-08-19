//! Shared durability-barrier primitive.
//!
//! The local journaler and the remote scheduler both publish the same progress
//! shape over a `tokio::sync::watch` channel and wait on it with the same loop;
//! only the error vocabulary differs, and that is supplied by [`BarrierError`].

use crate::writeback::model::Sequence;
use std::marker::PhantomData;
use tokio::sync::watch;

/// Durability progress published by one writeback stage.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SequenceProgress {
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

    /// Wait until `sequence` is durable at this stage.
    pub(crate) async fn wait(&self, sequence: Sequence) -> Result<(), E> {
        let mut progress = self.progress.clone();
        loop {
            let state = progress.borrow().clone();
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
