//! Generic FIFO byte-budget admission gate shared by the writeback tiers.

use std::collections::VecDeque;
use std::fmt;
use std::sync::{Arc, Mutex, MutexGuard};
use tokio::sync::oneshot;

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum AdmissionError {
    #[error("writeback admission is closed")]
    Closed,
    #[error("writeback admission is poisoned: {0}")]
    Poisoned(String),
    #[error("writeback mutation requires {requested} bytes but capacity is {capacity} bytes")]
    TooLarge { requested: u64, capacity: u64 },
    #[error("invalid writeback admission configuration: {0}")]
    InvalidConfiguration(&'static str),
    /// Reserved for the underflow-poison contract documented on
    /// [`AdmissionPolicy::release`]: a policy's `release` is allowed to
    /// report an accounting underflow through this variant. `RamPolicy`
    /// (the only policy today) reports underflow as `Poisoned` instead, so
    /// no production path constructs this yet. Keep it defined for future
    /// `AdmissionPolicy` implementations rather than dropping the contract.
    #[allow(dead_code)]
    #[error("writeback admission accounting underflow")]
    AccountingUnderflow,
}

/// Tier-specific behaviour plugged into the shared admission [`Gate`].
///
/// The FIFO waiter queue, the wait-registration cancellation guard, the
/// terminate/poison path, and the grant loop exist exactly once in [`Gate`].
/// A policy supplies only what genuinely differs between the dirty-RAM and
/// dirty-SSD tiers: the capacity check, the extra state each tier tracks
/// (operation counts for RAM, watermark/pause/free-space for SSD), the
/// accounting hooks run on admit/block/rollback, and the release ladder.
///
/// The implementing type doubles as the gate's immutable configuration, so
/// hooks read their limits from `gate.policy`.
trait AdmissionPolicy: fmt::Debug + Sized {
    /// Mutable per-tier state carried alongside the shared `used` counter.
    type Extra: fmt::Debug + Default;
    /// Guard handed to an admitted reservation.
    type Permit: fmt::Debug;

    /// Panic message used when a checked fit is contradicted by the add.
    const FIT_CHECKED: &'static str;

    /// Recomputes derived state before a fit check. Runs under the state lock.
    fn refresh(gate: &Gate<Self>, state: &mut GateState<Self>);

    /// Whether `bytes` may be admitted right now.
    fn fits(gate: &Gate<Self>, state: &GateState<Self>, bytes: u64) -> bool;

    /// Runs after `state.used` grew by an admitted reservation.
    fn on_admitted(gate: &Gate<Self>, state: &mut GateState<Self>);

    /// Runs when `bytes` cannot be admitted and the caller must queue or wait.
    fn on_blocked(gate: &Gate<Self>, state: &mut GateState<Self>, bytes: u64);

    /// Runs after `state.used` was rolled back for a grant nobody received.
    fn on_grant_canceled(gate: &Gate<Self>, state: &mut GateState<Self>);

    /// Releases `bytes`, returning a terminal error when accounting underflows.
    fn release(
        gate: &Gate<Self>,
        state: &mut GateState<Self>,
        bytes: u64,
    ) -> Option<AdmissionError>;

    /// Builds the tier's permit guard for a granted reservation.
    fn permit(gate: &Arc<Gate<Self>>, bytes: u64) -> Self::Permit;

    /// Disarms a permit whose receiver vanished before delivery.
    fn disarm(permit: &mut Self::Permit);
}

/// Byte-budget gate shared by both writeback tiers.
#[derive(Debug)]
struct Gate<P: AdmissionPolicy> {
    capacity: u64,
    // Landed-but-not-wired: retained for the tier policy that owns this gate.
    #[allow(dead_code)]
    policy: P,
    state: Mutex<GateState<P>>,
}

#[derive(Debug)]
struct GateState<P: AdmissionPolicy> {
    used: u64,
    next_waiter: u64,
    waiters: VecDeque<Waiter<P>>,
    terminal: Option<AdmissionError>,
    extra: P::Extra,
}

#[derive(Debug)]
struct Waiter<P: AdmissionPolicy> {
    id: u64,
    bytes: u64,
    sender: oneshot::Sender<Result<P::Permit, AdmissionError>>,
}

impl<P: AdmissionPolicy> Gate<P> {
    fn new(capacity: u64, policy: P, state: GateState<P>) -> Arc<Self> {
        Arc::new(Self {
            capacity,
            policy,
            state: Mutex::new(state),
        })
    }
}

impl<P: AdmissionPolicy> GateState<P> {
    fn new(used: u64, extra: P::Extra) -> Self {
        Self {
            used,
            next_waiter: 0,
            waiters: VecDeque::new(),
            terminal: None,
            extra,
        }
    }
}

impl<P: AdmissionPolicy> Default for GateState<P> {
    fn default() -> Self {
        Self::new(0, P::Extra::default())
    }
}

/// Cancellation guard: a caller that stops awaiting must leave the FIFO queue
/// and hand its place to the next waiter that fits.
struct WaitRegistration<P: AdmissionPolicy> {
    gate: Arc<Gate<P>>,
    id: u64,
    active: bool,
}

impl<P: AdmissionPolicy> Drop for WaitRegistration<P> {
    fn drop(&mut self) {
        if !self.active {
            return;
        }
        {
            let mut state = lock(&self.gate.state);
            state.waiters.retain(|waiter| waiter.id != self.id);
        }
        grant_waiters(&self.gate);
    }
}

/// Reserves `bytes`, queueing in FIFO order behind any existing waiter.
///
/// `prepare` runs first under the state lock so a tier can refresh inputs
/// (the SSD tier's free-space probe) before pause state is recomputed.
async fn reserve_bytes<P: AdmissionPolicy>(
    gate: &Arc<Gate<P>>,
    bytes: u64,
    prepare: impl FnOnce(&mut GateState<P>),
) -> Result<P::Permit, AdmissionError> {
    if bytes > gate.capacity {
        return Err(AdmissionError::TooLarge {
            requested: bytes,
            capacity: gate.capacity,
        });
    }
    let (id, receiver) = {
        let mut state = lock(&gate.state);
        prepare(&mut state);
        P::refresh(gate, &mut state);
        if let Some(error) = &state.terminal {
            return Err(error.clone());
        }
        if state.waiters.is_empty() && P::fits(gate, &state, bytes) {
            state.used += bytes;
            P::on_admitted(gate, &mut state);
            return Ok(P::permit(gate, bytes));
        }
        P::on_blocked(gate, &mut state, bytes);
        let id = state.next_waiter;
        state.next_waiter = state.next_waiter.wrapping_add(1);
        let (sender, receiver) = oneshot::channel();
        state.waiters.push_back(Waiter { id, bytes, sender });
        (id, receiver)
    };
    let mut registration = WaitRegistration {
        gate: gate.clone(),
        id,
        active: true,
    };
    let result = receiver.await.unwrap_or(Err(AdmissionError::Closed));
    registration.active = false;
    result
}

/// Wakes waiters from the front of the queue while the head still fits.
fn grant_waiters<P: AdmissionPolicy>(gate: &Arc<Gate<P>>) {
    let mut state = lock(&gate.state);
    if state.terminal.is_some() {
        return;
    }
    P::refresh(gate, &mut state);
    while let Some(bytes) = state.waiters.front().map(|waiter| waiter.bytes) {
        if !P::fits(gate, &state, bytes) {
            P::on_blocked(gate, &mut state, bytes);
            break;
        }
        let waiter = state.waiters.pop_front().expect("front waiter exists");
        state.used = state.used.checked_add(waiter.bytes).expect(P::FIT_CHECKED);
        P::on_admitted(gate, &mut state);
        let permit = P::permit(gate, waiter.bytes);
        if let Err(Ok(mut permit)) = waiter.sender.send(Ok(permit)) {
            P::disarm(&mut permit);
            state.used -= waiter.bytes;
            P::on_grant_canceled(gate, &mut state);
        }
    }
}

/// Latches the terminal error once and fails every queued waiter.
fn terminate<P: AdmissionPolicy>(gate: &Arc<Gate<P>>, error: AdmissionError) {
    let waiters = {
        let mut state = lock(&gate.state);
        if state.terminal.is_some() {
            return;
        }
        state.terminal = Some(error.clone());
        state.waiters.drain(..).collect::<Vec<_>>()
    };
    for waiter in waiters {
        let _ = waiter.sender.send(Err(error.clone()));
    }
}

/// Returns `bytes` to the budget, poisoning the gate if accounting underflows.
fn release_bytes<P: AdmissionPolicy>(gate: &Arc<Gate<P>>, bytes: u64) {
    let accounting_error = {
        let mut state = lock(&gate.state);
        P::release(gate, &mut state, bytes)
    };
    if let Some(error) = accounting_error {
        terminate(gate, error);
    } else {
        grant_waiters(gate);
    }
}

#[derive(Debug)]
struct RamPolicy;

#[derive(Debug, Default)]
struct RamCounters {
    used_operations: u64,
}

impl AdmissionPolicy for RamPolicy {
    type Extra = RamCounters;
    type Permit = AdmissionPermit;

    const FIT_CHECKED: &'static str = "RAM admission fit was checked before accounting";

    fn refresh(_gate: &Gate<Self>, _state: &mut GateState<Self>) {}

    fn fits(gate: &Gate<Self>, state: &GateState<Self>, bytes: u64) -> bool {
        !projected_exceeds(state.used, bytes, gate.capacity)
    }

    fn on_admitted(_gate: &Gate<Self>, state: &mut GateState<Self>) {
        state.extra.used_operations += 1;
    }

    fn on_blocked(_gate: &Gate<Self>, _state: &mut GateState<Self>, _bytes: u64) {}

    fn on_grant_canceled(_gate: &Gate<Self>, state: &mut GateState<Self>) {
        state.extra.used_operations -= 1;
    }

    fn release(
        _gate: &Gate<Self>,
        state: &mut GateState<Self>,
        bytes: u64,
    ) -> Option<AdmissionError> {
        match (
            state.used.checked_sub(bytes),
            state.extra.used_operations.checked_sub(1),
        ) {
            (Some(remaining), Some(remaining_operations)) => {
                state.used = remaining;
                state.extra.used_operations = remaining_operations;
                None
            }
            (None, _) => {
                state.used = 0;
                state.extra.used_operations = 0;
                Some(AdmissionError::Poisoned(
                    "dirty RAM accounting underflow".to_owned(),
                ))
            }
            (_, None) => {
                state.used = 0;
                state.extra.used_operations = 0;
                Some(AdmissionError::Poisoned(
                    "dirty RAM operation accounting underflow".to_owned(),
                ))
            }
        }
    }

    fn permit(gate: &Arc<Gate<Self>>, bytes: u64) -> AdmissionPermit {
        AdmissionPermit::new(gate.clone(), bytes)
    }

    fn disarm(permit: &mut AdmissionPermit) {
        permit.active = false;
    }
}

#[derive(Debug, Clone)]
pub struct Admission {
    inner: Arc<Gate<RamPolicy>>,
}

#[derive(Debug)]
pub struct AdmissionPermit {
    inner: Arc<Gate<RamPolicy>>,
    bytes: u64,
    active: bool,
}

#[derive(Debug)]
pub struct AcceptedAdmission(AdmissionPermit);

impl Admission {
    pub(crate) fn new(capacity: u64) -> Self {
        Self {
            inner: Gate::new(capacity, RamPolicy, GateState::default()),
        }
    }

    pub(crate) async fn reserve(&self, bytes: u64) -> Result<AdmissionPermit, AdmissionError> {
        reserve_bytes(&self.inner, bytes, |_| {}).await
    }

    pub(crate) fn used_bytes(&self) -> u64 {
        lock(&self.inner.state).used
    }

    pub(crate) fn used_operations(&self) -> u64 {
        lock(&self.inner.state).extra.used_operations
    }

    pub(crate) fn poison(&self, message: impl Into<String>) {
        terminate(&self.inner, AdmissionError::Poisoned(message.into()));
    }

    pub(crate) fn close(&self) {
        terminate(&self.inner, AdmissionError::Closed);
    }

    /// Merge already-charged multipart shares without releasing, refitting,
    /// or reacquiring RAM capacity.
    pub(crate) fn merge_accepted(
        &self,
        mut shares: Vec<AcceptedAdmission>,
    ) -> Result<AcceptedAdmission, AdmissionError> {
        if shares.is_empty() {
            return Err(AdmissionError::InvalidConfiguration(
                "multipart RAM promotion requires at least one share",
            ));
        }
        if shares
            .iter()
            .any(|share| !share.0.active || !Arc::ptr_eq(&share.0.inner, &self.inner))
        {
            return Err(AdmissionError::InvalidConfiguration(
                "multipart RAM shares must belong to one admission gate",
            ));
        }
        let Some(bytes) = shares
            .iter()
            .try_fold(0_u64, |total, share| total.checked_add(share.bytes()))
        else {
            let error =
                AdmissionError::Poisoned("multipart RAM promotion byte overflow".to_owned());
            terminate(&self.inner, error.clone());
            return Err(error);
        };
        let accounting_mismatch = {
            let mut state = lock(&self.inner.state);
            if let Some(error) = &state.terminal {
                return Err(error.clone());
            }
            if state.used < bytes || state.extra.used_operations < shares.len() as u64 {
                true
            } else {
                state.extra.used_operations -= shares.len() as u64 - 1;
                for share in &mut shares {
                    share.0.active = false;
                }
                false
            }
        };
        if accounting_mismatch {
            let error =
                AdmissionError::Poisoned("multipart RAM promotion accounting mismatch".to_owned());
            terminate(&self.inner, error.clone());
            return Err(error);
        }
        Ok(AcceptedAdmission(AdmissionPermit::new(
            Arc::clone(&self.inner),
            bytes,
        )))
    }
}

impl AdmissionPermit {
    fn new(inner: Arc<Gate<RamPolicy>>, bytes: u64) -> Self {
        Self {
            inner,
            bytes,
            active: true,
        }
    }

    fn bytes(&self) -> u64 {
        self.bytes
    }

    pub(crate) fn accept(self) -> AcceptedAdmission {
        AcceptedAdmission(self)
    }
}

impl AcceptedAdmission {
    pub(crate) fn bytes(&self) -> u64 {
        self.0.bytes()
    }
}

impl Drop for AdmissionPermit {
    fn drop(&mut self) {
        if !self.active {
            return;
        }
        self.active = false;
        release_bytes(&self.inner, self.bytes);
    }
}

fn projected_exceeds(used: u64, requested: u64, limit: u64) -> bool {
    used.checked_add(requested)
        .is_none_or(|total| total > limit)
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

#[cfg(test)]
mod tests {
    use super::{Admission, AdmissionError, lock};
    use std::time::Duration;

    #[tokio::test]
    async fn concurrent_puts_cannot_oversubscribe_dirty_ram() {
        let admission = Admission::new(10);
        let first = admission.reserve(7).await.unwrap();
        let blocked = tokio::spawn({
            let admission = admission.clone();
            async move { admission.reserve(4).await }
        });

        tokio::time::sleep(Duration::from_millis(20)).await;
        assert_eq!(admission.used_bytes(), 7);
        assert!(!blocked.is_finished());
        drop(first);

        let second = tokio::time::timeout(Duration::from_secs(1), blocked)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(second.bytes(), 4);
        assert_eq!(admission.used_bytes(), 4);
    }

    #[tokio::test]
    async fn dirty_ram_operation_accounting_tracks_each_live_reservation() {
        let admission = Admission::new(10);
        let first = admission.reserve(3).await.unwrap();
        let second = admission.reserve(4).await.unwrap();
        assert_eq!(admission.used_operations(), 2);

        drop(first);
        assert_eq!(admission.used_operations(), 1);
        drop(second);
        assert_eq!(admission.used_operations(), 0);
    }

    #[tokio::test]
    async fn dirty_ram_operation_underflow_poisons_admission() {
        let admission = Admission::new(10);
        let permit = admission.reserve(3).await.unwrap();
        lock(&admission.inner.state).extra.used_operations = 0;

        drop(permit);

        assert!(matches!(
            admission.reserve(1).await,
            Err(AdmissionError::Poisoned(message))
                if message.contains("operation accounting underflow")
        ));
    }

    #[tokio::test]
    async fn canceled_head_waiter_does_not_wedge_fifo_queue() {
        let admission = Admission::new(10);
        let held = admission.reserve(8).await.unwrap();
        let head = tokio::spawn({
            let admission = admission.clone();
            async move { admission.reserve(9).await }
        });
        let tail = tokio::spawn({
            let admission = admission.clone();
            async move { admission.reserve(2).await }
        });

        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(!tail.is_finished(), "FIFO tail must not bypass the head");
        head.abort();
        head.await.unwrap_err();

        let tail = tokio::time::timeout(Duration::from_secs(1), tail)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(tail.bytes(), 2);
        drop(tail);
        drop(held);
    }

    #[tokio::test]
    async fn byte_accounting_never_wraps_at_u64_capacity() {
        let admission = Admission::new(u64::MAX);
        let held = admission.reserve(u64::MAX - 1).await.unwrap();
        let blocked = tokio::spawn({
            let admission = admission.clone();
            async move { admission.reserve(2).await }
        });

        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(!blocked.is_finished());
        assert_eq!(admission.used_bytes(), u64::MAX - 1);
        drop(held);
        assert_eq!(blocked.await.unwrap().unwrap().bytes(), 2);
    }

    #[tokio::test]
    async fn poison_and_shutdown_wake_every_waiter_without_leaking_capacity() {
        let admission = Admission::new(10);
        let held = admission.reserve(10).await.unwrap();
        let poisoned = tokio::spawn({
            let admission = admission.clone();
            async move { admission.reserve(1).await }
        });
        tokio::time::sleep(Duration::from_millis(20)).await;

        admission.poison("local journal fsync failed");
        let error = poisoned.await.unwrap().unwrap_err();
        assert_eq!(
            error,
            AdmissionError::Poisoned("local journal fsync failed".to_owned())
        );
        drop(held);
        assert_eq!(admission.used_bytes(), 0);
        assert!(matches!(
            admission.reserve(1).await,
            Err(AdmissionError::Poisoned(_))
        ));

        let closing = Admission::new(1);
        let held = closing.reserve(1).await.unwrap();
        let waiter = tokio::spawn({
            let closing = closing.clone();
            async move { closing.reserve(1).await }
        });
        tokio::time::sleep(Duration::from_millis(20)).await;
        closing.close();
        assert_eq!(waiter.await.unwrap().unwrap_err(), AdmissionError::Closed);
        drop(held);
        assert_eq!(closing.used_bytes(), 0);
    }
}
