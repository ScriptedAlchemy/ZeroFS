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
    pub fn new(capacity: u64) -> Self {
        Self {
            inner: Gate::new(capacity, RamPolicy, GateState::default()),
        }
    }

    pub async fn reserve(&self, bytes: u64) -> Result<AdmissionPermit, AdmissionError> {
        reserve_bytes(&self.inner, bytes, |_| {}).await
    }

    pub fn used_bytes(&self) -> u64 {
        lock(&self.inner.state).used
    }

    pub fn used_operations(&self) -> u64 {
        lock(&self.inner.state).extra.used_operations
    }

    pub fn poison(&self, message: impl Into<String>) {
        terminate(&self.inner, AdmissionError::Poisoned(message.into()));
    }

    pub fn close(&self) {
        terminate(&self.inner, AdmissionError::Closed);
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

    pub fn bytes(&self) -> u64 {
        self.bytes
    }

    pub fn accept(self) -> AcceptedAdmission {
        AcceptedAdmission(self)
    }
}

impl AcceptedAdmission {
    pub fn bytes(&self) -> u64 {
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

#[derive(Debug)]
struct DiskPolicy {
    high_bytes: u64,
    resume_bytes: u64,
    min_free_bytes: u64,
}

#[derive(Debug, Default)]
struct DiskGauges {
    available: u64,
    paused: bool,
}

impl AdmissionPolicy for DiskPolicy {
    type Extra = DiskGauges;
    type Permit = DiskPermit;

    const FIT_CHECKED: &'static str = "disk admission fit was checked before accounting";

    fn refresh(gate: &Gate<Self>, state: &mut GateState<Self>) {
        if state.extra.paused
            && state.used <= gate.policy.resume_bytes
            && state.extra.available >= gate.policy.min_free_bytes
        {
            state.extra.paused = false;
        }
    }

    fn fits(gate: &Gate<Self>, state: &GateState<Self>, bytes: u64) -> bool {
        !state.extra.paused
            && (!projected_exceeds(state.used, bytes, gate.policy.high_bytes)
                || (state.used == 0 && !projected_exceeds(0, bytes, gate.capacity)))
            && state.extra.available.saturating_sub(bytes) >= gate.policy.min_free_bytes
    }

    fn on_admitted(gate: &Gate<Self>, state: &mut GateState<Self>) {
        if state.used > gate.policy.high_bytes {
            state.extra.paused = true;
        }
    }

    fn on_blocked(gate: &Gate<Self>, state: &mut GateState<Self>, bytes: u64) {
        if projected_exceeds(state.used, bytes, gate.policy.high_bytes) {
            state.extra.paused = true;
        }
    }

    fn on_grant_canceled(gate: &Gate<Self>, state: &mut GateState<Self>) {
        Self::refresh(gate, state);
    }

    fn release(
        gate: &Gate<Self>,
        state: &mut GateState<Self>,
        bytes: u64,
    ) -> Option<AdmissionError> {
        match state.used.checked_sub(bytes) {
            Some(remaining) => {
                state.used = remaining;
                Self::refresh(gate, state);
                None
            }
            None => {
                state.used = 0;
                Some(AdmissionError::Poisoned(
                    "dirty SSD accounting underflow".to_owned(),
                ))
            }
        }
    }

    fn permit(gate: &Arc<Gate<Self>>, bytes: u64) -> DiskPermit {
        DiskPermit::new(gate.clone(), bytes)
    }

    fn disarm(permit: &mut DiskPermit) {
        permit.active = false;
    }
}

#[derive(Debug, Clone)]
pub struct DiskAdmission {
    inner: Arc<Gate<DiskPolicy>>,
}

#[derive(Debug)]
pub struct DiskPermit {
    inner: Arc<Gate<DiskPolicy>>,
    bytes: u64,
    active: bool,
}

impl DiskAdmission {
    pub fn new(
        capacity: u64,
        high_watermark_percent: u8,
        resume_percent: u8,
        min_free_bytes: u64,
    ) -> Result<Self, AdmissionError> {
        Self::with_used(
            capacity,
            high_watermark_percent,
            resume_percent,
            min_free_bytes,
            0,
            0,
        )
    }

    pub fn with_used(
        capacity: u64,
        high_watermark_percent: u8,
        resume_percent: u8,
        min_free_bytes: u64,
        used_bytes: u64,
        available_filesystem_bytes: u64,
    ) -> Result<Self, AdmissionError> {
        if capacity == 0
            || resume_percent == 0
            || resume_percent >= high_watermark_percent
            || high_watermark_percent > 100
        {
            return Err(AdmissionError::InvalidConfiguration(
                "requires capacity > 0 and 0 < resume < high <= 100",
            ));
        }
        let high_bytes = percent_bytes(capacity, high_watermark_percent);
        Ok(Self {
            inner: Gate::new(
                capacity,
                DiskPolicy {
                    high_bytes,
                    resume_bytes: percent_bytes(capacity, resume_percent),
                    min_free_bytes,
                },
                GateState::new(
                    used_bytes,
                    DiskGauges {
                        available: available_filesystem_bytes,
                        paused: used_bytes > high_bytes
                            || available_filesystem_bytes < min_free_bytes,
                    },
                ),
            ),
        })
    }

    pub async fn reserve(
        &self,
        bytes: u64,
        available_filesystem_bytes: u64,
    ) -> Result<DiskPermit, AdmissionError> {
        reserve_bytes(&self.inner, bytes, |state| {
            state.extra.available = available_filesystem_bytes;
        })
        .await
    }

    pub fn set_remote_complete(
        &self,
        bytes: u64,
        available_filesystem_bytes: u64,
    ) -> Result<(), AdmissionError> {
        {
            let mut state = lock(&self.inner.state);
            state.used = state
                .used
                .checked_sub(bytes)
                .ok_or(AdmissionError::AccountingUnderflow)?;
            state.extra.available = available_filesystem_bytes;
            DiskPolicy::refresh(&self.inner, &mut state);
        }
        grant_waiters(&self.inner);
        Ok(())
    }

    pub fn update_available_space(&self, available: u64) -> Result<(), AdmissionError> {
        {
            let mut state = lock(&self.inner.state);
            if let Some(error) = &state.terminal {
                return Err(error.clone());
            }
            state.extra.available = available;
            DiskPolicy::refresh(&self.inner, &mut state);
        }
        grant_waiters(&self.inner);
        Ok(())
    }

    pub fn used_bytes(&self) -> u64 {
        lock(&self.inner.state).used
    }

    pub fn poison(&self, message: impl Into<String>) {
        terminate(&self.inner, AdmissionError::Poisoned(message.into()));
    }

    pub fn close(&self) {
        terminate(&self.inner, AdmissionError::Closed);
    }
}

impl DiskPermit {
    fn new(inner: Arc<Gate<DiskPolicy>>, bytes: u64) -> Self {
        Self {
            inner,
            bytes,
            active: true,
        }
    }

    pub fn bytes(&self) -> u64 {
        self.bytes
    }

    pub fn accept(mut self) {
        self.active = false;
    }
}

impl Drop for DiskPermit {
    fn drop(&mut self) {
        if !self.active {
            return;
        }
        self.active = false;
        release_bytes(&self.inner, self.bytes);
    }
}

fn percent_bytes(capacity: u64, percent: u8) -> u64 {
    ((capacity as u128 * percent as u128) / 100) as u64
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
    use super::{Admission, AdmissionError, DiskAdmission, Waiter, grant_waiters, lock};
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
    async fn reservation_larger_than_the_dirty_ram_budget_fails_immediately() {
        let admission = Admission::new(10);

        let error = admission.reserve(11).await.unwrap_err();

        assert_eq!(
            error,
            AdmissionError::TooLarge {
                requested: 11,
                capacity: 10
            }
        );
        assert_eq!(admission.used_bytes(), 0);
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

    #[tokio::test]
    async fn disk_gate_pauses_at_high_water_and_resumes_only_below_low_water() {
        let disk = DiskAdmission::new(100, 90, 70, 10).unwrap();
        let first = disk.reserve(80, 1_000).await.unwrap();
        let blocked = tokio::spawn({
            let disk = disk.clone();
            async move { disk.reserve(11, 1_000).await }
        });
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(!blocked.is_finished());

        first.accept();
        disk.set_remote_complete(5, 1_000).unwrap();
        assert!(
            !blocked.is_finished(),
            "75 bytes remains above the 70-byte resume mark"
        );
        disk.set_remote_complete(5, 1_000).unwrap();
        let second = tokio::time::timeout(Duration::from_secs(1), blocked)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(second.bytes(), 11);
        drop(second);
    }

    #[tokio::test]
    async fn disk_gate_preserves_filesystem_free_space_reserve() {
        let disk = DiskAdmission::new(1_000, 95, 85, 200).unwrap();
        let blocked = tokio::spawn({
            let disk = disk.clone();
            async move { disk.reserve(100, 250).await }
        });
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(!blocked.is_finished());

        disk.update_available_space(400).unwrap();
        let permit = tokio::time::timeout(Duration::from_secs(1), blocked)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(permit.bytes(), 100);
    }

    #[tokio::test]
    async fn empty_disk_gate_accepts_one_capacity_fitting_reservation_above_high_water() {
        let disk = DiskAdmission::new(100, 90, 70, 10).unwrap();

        let permit = tokio::time::timeout(Duration::from_secs(1), disk.reserve(95, 1_000))
            .await
            .expect("an empty tier must not wait forever for a capacity-fitting mutation")
            .unwrap();

        assert_eq!(permit.bytes(), 95);
        assert_eq!(disk.used_bytes(), 95);
        permit.accept();

        let blocked = tokio::spawn({
            let disk = disk.clone();
            async move { disk.reserve(1, 1_000).await }
        });
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(
            !blocked.is_finished(),
            "the exceptional reservation must still engage high-water backpressure"
        );

        disk.set_remote_complete(25, 1_000).unwrap();
        let resumed = tokio::time::timeout(Duration::from_secs(1), blocked)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(resumed.bytes(), 1);
    }

    #[tokio::test]
    async fn canceled_exceptional_disk_grant_does_not_strand_the_next_waiter() {
        let disk = DiskAdmission::new(100, 90, 70, 10).unwrap();
        let (canceled_sender, canceled_receiver) = tokio::sync::oneshot::channel();
        drop(canceled_receiver);
        let (next_sender, next_receiver) = tokio::sync::oneshot::channel();
        {
            let mut state = lock(&disk.inner.state);
            state.extra.available = 1_000;
            state.waiters.push_back(Waiter {
                id: 0,
                bytes: 95,
                sender: canceled_sender,
            });
            state.waiters.push_back(Waiter {
                id: 1,
                bytes: 1,
                sender: next_sender,
            });
        }

        grant_waiters(&disk.inner);

        let permit = tokio::time::timeout(Duration::from_secs(1), next_receiver)
            .await
            .expect("the next waiter was stranded behind a canceled exceptional grant")
            .unwrap()
            .unwrap();
        assert_eq!(permit.bytes(), 1);
    }

    #[test]
    fn disk_gate_restores_pending_blob_bytes_before_accepting_new_writes() {
        let disk = DiskAdmission::with_used(100, 90, 70, 10, 80, 1_000).unwrap();

        assert_eq!(disk.used_bytes(), 80);
    }
}
