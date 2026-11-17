//! Whether a protocol request may still be abandoned by its client.
//!
//! A 9P client cancels a pending request with `Tflush`. flush(5) lets the
//! server answer `Rflush` without the original reply only if the request can
//! be "treated as though it had never been sent". A request satisfies that
//! until it changes state that the client could later observe: a filesystem
//! commit or a fid-table change. Every such change first calls
//! [`pass_point_of_no_return`]. From then on the request runs to completion and
//! its reply precedes `Rflush`, which flush(5) also allows.
//!
//! The phase is one atomic word, so a cancellation and a commit that race
//! resolve to exactly one winner.

use crate::fs::errors::FsError;
use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicU8, Ordering};
use tokio_util::sync::CancellationToken;

const ABANDONABLE: u8 = 0;
const ABANDONED: u8 = 1;
const COMMITTED: u8 = 2;

tokio::task_local! {
    static CURRENT: Arc<Abandonment>;
}

/// The cancellation state of one request.
#[derive(Debug)]
pub(crate) struct Abandonment {
    phase: AtomicU8,
    abandoned: CancellationToken,
}

/// The request was abandoned before it reached its point of no return.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Abandoned;

impl From<Abandoned> for FsError {
    fn from(_: Abandoned) -> Self {
        FsError::Abandoned
    }
}

impl Abandonment {
    /// A request that has made no observable change yet.
    pub(crate) fn abandonable() -> Arc<Self> {
        Self::with_phase(ABANDONABLE)
    }

    /// A request that must run to completion and answer.
    pub(crate) fn committed() -> Arc<Self> {
        Self::with_phase(COMMITTED)
    }

    fn with_phase(phase: u8) -> Arc<Self> {
        Arc::new(Self {
            phase: AtomicU8::new(phase),
            abandoned: CancellationToken::new(),
        })
    }

    /// Cancel the request if it has made no observable change. Returns true
    /// only for the call that abandons it; the request then never takes effect
    /// and never answers.
    pub(crate) fn abandon(&self) -> bool {
        let abandoned = self
            .phase
            .compare_exchange(ABANDONABLE, ABANDONED, Ordering::AcqRel, Ordering::Acquire)
            .is_ok();
        if abandoned {
            self.abandoned.cancel();
        }
        abandoned
    }

    /// Commit the request to taking effect and answering. Fails once the
    /// request has been abandoned.
    pub(crate) fn commit(&self) -> Result<(), Abandoned> {
        match self.phase.compare_exchange(
            ABANDONABLE,
            COMMITTED,
            Ordering::AcqRel,
            Ordering::Acquire,
        ) {
            Ok(_) | Err(COMMITTED) => Ok(()),
            Err(_) => Err(Abandoned),
        }
    }

    /// Resolves when [`Self::abandon`] succeeds.
    pub(crate) async fn abandoned(&self) {
        self.abandoned.cancelled().await;
    }

    /// Run `request` with this state as its point-of-no-return authority.
    pub(crate) fn scope<F: Future>(self: Arc<Self>, request: F) -> impl Future<Output = F::Output> {
        CURRENT.scope(self, request)
    }
}

/// Call immediately before a change the client could observe. Outside a
/// request scope (NFS, NBD, background work) there is nothing to abandon.
pub(crate) fn pass_point_of_no_return() -> Result<(), Abandoned> {
    CURRENT
        .try_with(|abandonment| abandonment.commit())
        .unwrap_or(Ok(()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn abandon_and_commit_have_one_winner() {
        let first_abandon = Abandonment::abandonable();
        assert!(first_abandon.abandon());
        assert!(!first_abandon.abandon(), "only the first call abandons");
        assert_eq!(first_abandon.commit(), Err(Abandoned));

        let first_commit = Abandonment::abandonable();
        assert_eq!(first_commit.commit(), Ok(()));
        assert_eq!(first_commit.commit(), Ok(()), "commit is idempotent");
        assert!(!first_commit.abandon());

        assert!(!Abandonment::committed().abandon());
    }

    #[tokio::test]
    async fn the_point_of_no_return_follows_the_request_scope() {
        assert_eq!(
            pass_point_of_no_return(),
            Ok(()),
            "no scope, nothing to abandon"
        );

        let abandonment = Abandonment::abandonable();
        assert!(abandonment.abandon());
        let result = Arc::clone(&abandonment)
            .scope(async { pass_point_of_no_return() })
            .await;
        assert_eq!(result, Err(Abandoned));

        let committed = Abandonment::abandonable();
        Arc::clone(&committed)
            .scope(async { pass_point_of_no_return() })
            .await
            .unwrap();
        assert!(!committed.abandon(), "a passed point of no return is final");
    }
}
