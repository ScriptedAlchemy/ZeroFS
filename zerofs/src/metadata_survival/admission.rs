//! The survival commit's mutation admission gate.
//!
//! Every top-level filesystem mutation takes one token before its conflict
//! fence or inode lock. A survival commit closes the gate, waits for admitted
//! tokens, takes its consistent cut and reopens the gate before any replica
//! I/O. A mutation that arrives while the gate is closed waits until it
//! reopens. The commit bounds how long it keeps the gate closed, so a commit
//! delays client mutations but never fails one. A replica fault or device loss
//! latches the gate: mutations then fail at once with EIO or EROFS, each one
//! logged with the latch reason, until an operator repairs the replica set.

use crate::fs::errors::FsError;
use std::sync::Mutex;
use tokio::sync::Notify;

#[derive(Debug)]
struct State {
    active: u64,
    closed: bool,
    latched: Option<(FsError, String)>,
}

#[derive(Debug)]
pub(crate) struct SurvivalAdmission {
    state: Mutex<State>,
    changed: Notify,
}

impl Default for SurvivalAdmission {
    fn default() -> Self {
        Self {
            state: Mutex::new(State {
                active: 0,
                closed: false,
                latched: None,
            }),
            changed: Notify::new(),
        }
    }
}

/// An owned token for detached work that must outlive its caller's borrow.
#[must_use = "admission lasts as long as the token is held"]
#[derive(Debug)]
pub(crate) struct OwnedAdmissionToken {
    gate: Option<std::sync::Arc<SurvivalAdmission>>,
}

impl Drop for OwnedAdmissionToken {
    fn drop(&mut self) {
        if let Some(gate) = self.gate.take() {
            gate.release();
        }
    }
}

impl SurvivalAdmission {
    fn lock(&self) -> std::sync::MutexGuard<'_, State> {
        self.state
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
    }

    fn try_enter(&self) -> Result<bool, FsError> {
        let mut state = self.lock();
        if let Some((error, reason)) = &state.latched {
            tracing::warn!(
                ?error,
                reason = reason.as_str(),
                "refused a mutation: metadata survival admission is latched"
            );
            return Err(*error);
        }
        if state.closed {
            return Ok(false);
        }
        state.active += 1;
        Ok(true)
    }

    async fn enter(&self) -> Result<(), FsError> {
        loop {
            let notified = self.changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.try_enter()? {
                return Ok(());
            }
            notified.await;
        }
    }

    /// Admit one top-level mutation. Never call while holding an inode,
    /// pool, catalog or database lock.
    pub(crate) async fn admit_owned(
        self: &std::sync::Arc<Self>,
    ) -> Result<OwnedAdmissionToken, FsError> {
        self.enter().await?;
        Ok(OwnedAdmissionToken {
            gate: Some(std::sync::Arc::clone(self)),
        })
    }

    fn release(&self) {
        let mut state = self.lock();
        state.active = state.active.saturating_sub(1);
        drop(state);
        self.changed.notify_waiters();
    }

    /// Close admission for a commit. Fails if a fault already latched it.
    pub(crate) fn close(&self) -> Result<(), FsError> {
        let mut state = self.lock();
        if let Some((error, _)) = &state.latched {
            return Err(*error);
        }
        state.closed = true;
        Ok(())
    }

    /// Wait until every admitted token has been released.
    pub(crate) async fn drained(&self) {
        loop {
            let notified = self.changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.lock().active == 0 {
                return;
            }
            notified.await;
        }
    }

    pub(crate) fn reopen(&self) {
        self.lock().closed = false;
        self.changed.notify_waiters();
    }

    /// Latch admission closed. EIO for a faulty replica, EROFS after loss.
    pub(crate) fn latch(&self, error: FsError, reason: impl Into<String>) {
        let mut state = self.lock();
        // A device loss outranks a transient fault.
        let replace = match &state.latched {
            None => true,
            Some((FsError::ReadOnlyFilesystem, _)) => false,
            Some(_) => error == FsError::ReadOnlyFilesystem,
        };
        if replace {
            state.latched = Some((error, reason.into()));
        }
        state.closed = false;
        drop(state);
        self.changed.notify_waiters();
    }

    pub(crate) fn unlatch(&self) {
        self.lock().latched = None;
        self.changed.notify_waiters();
    }

    pub(crate) fn is_open(&self) -> bool {
        let state = self.lock();
        !state.closed && state.latched.is_none()
    }

    pub(crate) fn latched(&self) -> Option<(FsError, String)> {
        self.lock().latched.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::time::Duration;

    #[tokio::test]
    async fn closed_gate_holds_a_mutation_until_reopen_and_latch_refuses_at_once() {
        let gate = Arc::new(SurvivalAdmission::default());
        let held = gate.admit_owned().await.unwrap();
        gate.close().unwrap();
        let drained = tokio::spawn({
            let gate = Arc::clone(&gate);
            async move { gate.drained().await }
        });
        let late = tokio::spawn({
            let gate = Arc::clone(&gate);
            async move { gate.admit_owned().await.map(drop) }
        });
        tokio::time::sleep(Duration::from_millis(1500)).await;
        assert!(!drained.is_finished(), "drain must wait for admitted work");
        assert!(!late.is_finished(), "a closed gate must hold, not refuse");
        drop(held);
        drained.await.unwrap();
        gate.reopen();
        late.await.unwrap().unwrap();
        drop(gate.admit_owned().await.unwrap());
        gate.latch(FsError::IoError, "replica full");
        let started = std::time::Instant::now();
        assert_eq!(gate.admit_owned().await.unwrap_err(), FsError::IoError);
        assert!(started.elapsed() < Duration::from_millis(20));
        gate.latch(FsError::ReadOnlyFilesystem, "replica lost");
        gate.latch(FsError::IoError, "later fault");
        assert_eq!(
            gate.admit_owned().await.unwrap_err(),
            FsError::ReadOnlyFilesystem
        );
        assert!(gate.close().is_err());
    }

    #[tokio::test]
    async fn reopen_admits_a_waiting_mutation() {
        let gate = Arc::new(SurvivalAdmission::default());
        gate.close().unwrap();
        let waiter = tokio::spawn({
            let gate = Arc::clone(&gate);
            async move { gate.admit_owned().await.map(drop) }
        });
        tokio::time::sleep(Duration::from_millis(20)).await;
        gate.reopen();
        waiter.await.unwrap().unwrap();
    }
}
