//! Exact SSD reservation tokens and accounting.
//!
//! [`SsdAdmission`] is the sole owner of dirty-SSD byte, operation, and
//! physical-claim budgets. Tokens are move-only: drop rolls the charge back
//! exactly once, and [`SsdReservationToken::disarm`] transfers ownership to a
//! later journal-commit path.

use crate::writeback::space_sample::PhysicalSpaceSample;
use std::collections::VecDeque;
use std::sync::{Arc, Mutex, MutexGuard};
use tokio::sync::oneshot;

/// Stable journal reservation plus the conservative local allocation claim.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct SsdReservationRequest {
    /// Journal reservation reconstructed from [`crate::writeback::model::MutationRecord::ssd_reservation_bytes`].
    /// Never logical payload length.
    pub(crate) ssd_reservation_bytes: u64,
    /// Conservative local allocation claim. Tracked separately from the journal reservation.
    pub(crate) physical_reservation_bytes: u64,
    pub(crate) operations: u64,
}

/// Lifecycle of a charged reservation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ReservationState {
    /// Charged against admission; drop rolls the charge back.
    Admitted,
    /// Ownership transferred; drop is a no-op.
    Disarmed,
}

/// Point-in-time SSD admission gauges.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct SsdAdmissionSnapshot {
    pub(crate) used_ssd_bytes: u64,
    pub(crate) used_operations: u64,
    pub(crate) outstanding_physical_claims: u64,
    pub(crate) available_bytes: u64,
    pub(crate) sample_generation: u64,
    pub(crate) paused: bool,
}
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub(crate) enum ReservationError {
    #[error("writeback SSD admission is closed")]
    Closed,
    #[error("writeback SSD admission is poisoned: {0}")]
    Poisoned(String),
    #[error("SSD reservation requires {requested} bytes but capacity is {capacity} bytes")]
    TooLarge { requested: u64, capacity: u64 },
    #[error("SSD reservation requires {requested} operations but capacity is {capacity}")]
    TooManyOperations { requested: u64, capacity: u64 },
    #[error(
        "SSD physical reservation {requested} cannot keep {min_free} free bytes from {available} available"
    )]
    TooLargePhysical {
        requested: u64,
        available: u64,
        min_free: u64,
    },
    #[error("invalid SSD admission configuration: {0}")]
    InvalidConfiguration(&'static str),
    #[error("writeback space sample generation {sample} is stale; latest is {latest}")]
    StaleSample { sample: u64, latest: u64 },
}

/// Move-only charged reservation.
#[derive(Debug)]
pub(crate) struct SsdReservationToken {
    admission: Arc<SsdAdmissionInner>,
    request: SsdReservationRequest,
    state: ReservationState,
}

impl SsdReservationToken {
    pub(crate) fn request(&self) -> SsdReservationRequest {
        self.request
    }

    pub(crate) fn state(&self) -> ReservationState {
        self.state
    }

    /// Transfer ownership so drop no longer rolls the charge back.
    pub(crate) fn disarm(&mut self) {
        self.state = ReservationState::Disarmed;
    }
}

impl Drop for SsdReservationToken {
    fn drop(&mut self) {
        if self.state != ReservationState::Admitted {
            return;
        }
        self.state = ReservationState::Disarmed;
        self.admission.release(self.request);
    }
}

/// Exact SSD byte/operation/physical-claim admission owner.
#[derive(Debug, Clone)]
pub(crate) struct SsdAdmission {
    inner: Arc<SsdAdmissionInner>,
}

#[derive(Debug)]
struct SsdAdmissionInner {
    capacity_bytes: u64,
    max_operations: u64,
    high_bytes: u64,
    resume_bytes: u64,
    min_free_bytes: u64,
    state: Mutex<SsdState>,
}

#[derive(Debug)]
struct SsdState {
    used_ssd_bytes: u64,
    used_operations: u64,
    outstanding_physical_claims: u64,
    available_bytes: u64,
    sample_generation: u64,
    paused: bool,
    next_waiter: u64,
    waiters: VecDeque<SsdWaiter>,
    terminal: Option<ReservationError>,
}

#[derive(Debug)]
struct SsdWaiter {
    id: u64,
    request: SsdReservationRequest,
    sender: oneshot::Sender<Result<SsdReservationToken, ReservationError>>,
}

struct WaitRegistration {
    inner: Arc<SsdAdmissionInner>,
    id: u64,
    active: bool,
}

impl Drop for WaitRegistration {
    fn drop(&mut self) {
        if !self.active {
            return;
        }
        {
            let mut state = lock(&self.inner.state);
            state.waiters.retain(|waiter| waiter.id != self.id);
        }
        self.inner.grant_waiters();
    }
}
impl SsdAdmission {
    pub(crate) fn new(
        capacity_bytes: u64,
        max_operations: u64,
        high_watermark_percent: u8,
        resume_percent: u8,
        min_free_bytes: u64,
    ) -> Result<Self, ReservationError> {
        Self::recover(
            capacity_bytes,
            max_operations,
            high_watermark_percent,
            resume_percent,
            min_free_bytes,
            std::iter::empty(),
            None,
        )
    }

    /// Seed exact pending bytes, operations, and physical claims before writes resume.
    pub(crate) fn recover(
        capacity_bytes: u64,
        max_operations: u64,
        high_watermark_percent: u8,
        resume_percent: u8,
        min_free_bytes: u64,
        pending: impl IntoIterator<Item = SsdReservationRequest>,
        sample: Option<PhysicalSpaceSample>,
    ) -> Result<Self, ReservationError> {
        if capacity_bytes == 0
            || max_operations == 0
            || resume_percent == 0
            || resume_percent >= high_watermark_percent
            || high_watermark_percent > 100
        {
            return Err(ReservationError::InvalidConfiguration(
                "requires capacity > 0, max_operations > 0, and 0 < resume < high <= 100",
            ));
        }

        let mut used_ssd_bytes = 0u64;
        let mut used_operations = 0u64;
        let mut outstanding_physical_claims = 0u64;
        for request in pending {
            used_ssd_bytes = used_ssd_bytes
                .checked_add(request.ssd_reservation_bytes)
                .ok_or_else(|| {
                    ReservationError::Poisoned(
                        "SSD reservation byte overflow during recovery".into(),
                    )
                })?;
            used_operations = used_operations
                .checked_add(request.operations)
                .ok_or_else(|| {
                    ReservationError::Poisoned(
                        "SSD reservation operation overflow during recovery".into(),
                    )
                })?;
            outstanding_physical_claims = outstanding_physical_claims
                .checked_add(request.physical_reservation_bytes)
                .ok_or_else(|| {
                    ReservationError::Poisoned("SSD physical-claim overflow during recovery".into())
                })?;
        }

        let high_bytes = percent_bytes(capacity_bytes, high_watermark_percent);
        Ok(Self {
            inner: Arc::new(SsdAdmissionInner {
                capacity_bytes,
                max_operations,
                high_bytes,
                resume_bytes: percent_bytes(capacity_bytes, resume_percent),
                min_free_bytes,
                state: Mutex::new(SsdState {
                    used_ssd_bytes,
                    used_operations,
                    outstanding_physical_claims,
                    available_bytes: sample.map(|s| s.available_bytes).unwrap_or(0),
                    sample_generation: sample.map(|s| s.generation).unwrap_or(0),
                    paused: used_ssd_bytes > high_bytes
                        || sample.is_some_and(|s| {
                            !physical_headroom(
                                s.available_bytes,
                                outstanding_physical_claims,
                                0,
                                min_free_bytes,
                            )
                        }),
                    next_waiter: 0,
                    waiters: VecDeque::new(),
                    terminal: None,
                }),
            }),
        })
    }
    pub(crate) async fn reserve(
        &self,
        request: SsdReservationRequest,
        sample: PhysicalSpaceSample,
    ) -> Result<SsdReservationToken, ReservationError> {
        if request.ssd_reservation_bytes > self.inner.capacity_bytes {
            return Err(ReservationError::TooLarge {
                requested: request.ssd_reservation_bytes,
                capacity: self.inner.capacity_bytes,
            });
        }
        if request.operations > self.inner.max_operations {
            return Err(ReservationError::TooManyOperations {
                requested: request.operations,
                capacity: self.inner.max_operations,
            });
        }

        let (id, receiver) = {
            let mut state = lock(&self.inner.state);
            self.inner.observe_locked(&mut state, sample)?;
            if let Some(error) = &state.terminal {
                return Err(error.clone());
            }
            if !physical_headroom(
                state.available_bytes,
                0,
                request.physical_reservation_bytes,
                self.inner.min_free_bytes,
            ) {
                return Err(ReservationError::TooLargePhysical {
                    requested: request.physical_reservation_bytes,
                    available: state.available_bytes,
                    min_free: self.inner.min_free_bytes,
                });
            }
            self.inner.refresh(&mut state);
            if state.waiters.is_empty() && self.inner.fits(&state, request) {
                self.inner.charge(&mut state, request)?;
                return Ok(self.inner.token(request));
            }
            if projected_exceeds(
                state.used_ssd_bytes,
                request.ssd_reservation_bytes,
                self.inner.high_bytes,
            ) {
                state.paused = true;
            }
            let id = state.next_waiter;
            state.next_waiter = state.next_waiter.wrapping_add(1);
            let (sender, receiver) = oneshot::channel();
            state.waiters.push_back(SsdWaiter {
                id,
                request,
                sender,
            });
            (id, receiver)
        };
        let mut registration = WaitRegistration {
            inner: self.inner.clone(),
            id,
            active: true,
        };
        let result = receiver.await.unwrap_or(Err(ReservationError::Closed));
        registration.active = false;
        result
    }

    #[allow(dead_code)]
    pub(crate) fn observe_sample(
        &self,
        sample: PhysicalSpaceSample,
    ) -> Result<(), ReservationError> {
        {
            let mut state = lock(&self.inner.state);
            self.inner.observe_locked(&mut state, sample)?;
            if let Some(error) = &state.terminal {
                return Err(error.clone());
            }
            self.inner.refresh(&mut state);
        }
        self.inner.grant_waiters();
        Ok(())
    }

    pub(crate) fn snapshot(&self) -> SsdAdmissionSnapshot {
        let state = lock(&self.inner.state);
        SsdAdmissionSnapshot {
            used_ssd_bytes: state.used_ssd_bytes,
            used_operations: state.used_operations,
            outstanding_physical_claims: state.outstanding_physical_claims,
            available_bytes: state.available_bytes,
            sample_generation: state.sample_generation,
            paused: state.paused,
        }
    }

    pub(crate) fn used_bytes(&self) -> u64 {
        lock(&self.inner.state).used_ssd_bytes
    }

    pub(crate) fn used_operations(&self) -> u64 {
        lock(&self.inner.state).used_operations
    }

    pub(crate) fn outstanding_physical_claims(&self) -> u64 {
        lock(&self.inner.state).outstanding_physical_claims
    }

    pub(crate) fn poison(&self, message: impl Into<String>) {
        self.inner
            .terminate(ReservationError::Poisoned(message.into()));
    }

    pub(crate) fn close(&self) {
        self.inner.terminate(ReservationError::Closed);
    }

    #[cfg(test)]
    pub(crate) fn force_release(&self, request: SsdReservationRequest) {
        self.inner.release(request);
    }

    /// Release a locally committed reservation after remote cleanup.
    ///
    /// Observes `sample` first so waiters see the post-cleanup free space.
    /// The charge stays held if the sampler generation is stale or admission
    /// is already terminal.
    pub(crate) fn release_remote(
        &self,
        request: SsdReservationRequest,
        sample: PhysicalSpaceSample,
    ) -> Result<(), ReservationError> {
        {
            let mut state = lock(&self.inner.state);
            self.inner.observe_locked(&mut state, sample)?;
            if let Some(error) = &state.terminal {
                return Err(error.clone());
            }
        }
        self.inner.release(request);
        Ok(())
    }
}
impl SsdAdmissionInner {
    fn observe_locked(
        &self,
        state: &mut SsdState,
        sample: PhysicalSpaceSample,
    ) -> Result<(), ReservationError> {
        if sample.generation < state.sample_generation {
            return Err(ReservationError::StaleSample {
                sample: sample.generation,
                latest: state.sample_generation,
            });
        }
        state.sample_generation = sample.generation;
        state.available_bytes = sample.available_bytes;
        Ok(())
    }

    fn refresh(&self, state: &mut SsdState) {
        if state.paused
            && state.used_ssd_bytes <= self.resume_bytes
            && physical_headroom(
                state.available_bytes,
                state.outstanding_physical_claims,
                0,
                self.min_free_bytes,
            )
        {
            state.paused = false;
        }
    }

    fn fits(&self, state: &SsdState, request: SsdReservationRequest) -> bool {
        let bytes_fit = !state.paused
            && (!projected_exceeds(
                state.used_ssd_bytes,
                request.ssd_reservation_bytes,
                self.high_bytes,
            ) || (state.used_ssd_bytes == 0
                && !projected_exceeds(0, request.ssd_reservation_bytes, self.capacity_bytes)));
        bytes_fit
            && !projected_exceeds(
                state.used_operations,
                request.operations,
                self.max_operations,
            )
            && physical_headroom(
                state.available_bytes,
                state.outstanding_physical_claims,
                request.physical_reservation_bytes,
                self.min_free_bytes,
            )
    }

    fn charge(
        &self,
        state: &mut SsdState,
        request: SsdReservationRequest,
    ) -> Result<(), ReservationError> {
        let used_ssd_bytes = match state
            .used_ssd_bytes
            .checked_add(request.ssd_reservation_bytes)
        {
            Some(bytes) => bytes,
            None => {
                let error = ReservationError::Poisoned("SSD reservation byte overflow".into());
                state.terminal = Some(error.clone());
                return Err(error);
            }
        };
        let used_operations = match state.used_operations.checked_add(request.operations) {
            Some(operations) => operations,
            None => {
                let error = ReservationError::Poisoned("SSD reservation operation overflow".into());
                state.terminal = Some(error.clone());
                return Err(error);
            }
        };
        let outstanding_physical_claims = match state
            .outstanding_physical_claims
            .checked_add(request.physical_reservation_bytes)
        {
            Some(physical) => physical,
            None => {
                let error = ReservationError::Poisoned("SSD physical-claim overflow".into());
                state.terminal = Some(error.clone());
                return Err(error);
            }
        };
        state.used_ssd_bytes = used_ssd_bytes;
        state.used_operations = used_operations;
        state.outstanding_physical_claims = outstanding_physical_claims;
        if state.used_ssd_bytes > self.high_bytes {
            state.paused = true;
        }
        Ok(())
    }

    fn token(self: &Arc<Self>, request: SsdReservationRequest) -> SsdReservationToken {
        SsdReservationToken {
            admission: self.clone(),
            request,
            state: ReservationState::Admitted,
        }
    }
    fn release(self: &Arc<Self>, request: SsdReservationRequest) {
        let poison = {
            let mut state = lock(&self.state);
            match (
                state
                    .used_ssd_bytes
                    .checked_sub(request.ssd_reservation_bytes),
                state.used_operations.checked_sub(request.operations),
                state
                    .outstanding_physical_claims
                    .checked_sub(request.physical_reservation_bytes),
            ) {
                (Some(bytes), Some(operations), Some(physical)) => {
                    state.used_ssd_bytes = bytes;
                    state.used_operations = operations;
                    state.outstanding_physical_claims = physical;
                    self.refresh(&mut state);
                    None
                }
                _ => Some(ReservationError::Poisoned(
                    "SSD reservation accounting underflow".into(),
                )),
            }
        };
        if let Some(error) = poison {
            self.terminate(error);
        } else {
            self.grant_waiters();
        }
    }

    fn grant_waiters(self: &Arc<Self>) {
        let mut state = lock(&self.state);
        if state.terminal.is_some() {
            return;
        }
        self.refresh(&mut state);
        while let Some(request) = state.waiters.front().map(|waiter| waiter.request) {
            if !self.fits(&state, request) {
                if projected_exceeds(
                    state.used_ssd_bytes,
                    request.ssd_reservation_bytes,
                    self.high_bytes,
                ) {
                    state.paused = true;
                }
                break;
            }
            let waiter = state.waiters.pop_front().expect("front waiter exists");
            if let Err(error) = self.charge(&mut state, waiter.request) {
                state.terminal = Some(error.clone());
                let _ = waiter.sender.send(Err(error.clone()));
                let waiters = state.waiters.drain(..).collect::<Vec<_>>();
                drop(state);
                for leftover in waiters {
                    let _ = leftover.sender.send(Err(error.clone()));
                }
                return;
            }
            let token = self.token(waiter.request);
            if let Err(Ok(mut token)) = waiter.sender.send(Ok(token)) {
                token.disarm();
                match (
                    state
                        .used_ssd_bytes
                        .checked_sub(waiter.request.ssd_reservation_bytes),
                    state.used_operations.checked_sub(waiter.request.operations),
                    state
                        .outstanding_physical_claims
                        .checked_sub(waiter.request.physical_reservation_bytes),
                ) {
                    (Some(bytes), Some(operations), Some(physical)) => {
                        state.used_ssd_bytes = bytes;
                        state.used_operations = operations;
                        state.outstanding_physical_claims = physical;
                        self.refresh(&mut state);
                    }
                    _ => {
                        let error = ReservationError::Poisoned(
                            "SSD reservation accounting underflow".into(),
                        );
                        state.terminal = Some(error.clone());
                        let waiters = state.waiters.drain(..).collect::<Vec<_>>();
                        drop(state);
                        for leftover in waiters {
                            let _ = leftover.sender.send(Err(error.clone()));
                        }
                        return;
                    }
                }
            }
        }
    }

    fn terminate(&self, error: ReservationError) {
        let waiters = {
            let mut state = lock(&self.state);
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
}

fn percent_bytes(capacity: u64, percent: u8) -> u64 {
    ((capacity as u128 * percent as u128) / 100) as u64
}

fn projected_exceeds(used: u64, requested: u64, limit: u64) -> bool {
    used.checked_add(requested)
        .is_none_or(|total| total > limit)
}

fn physical_headroom(
    available_bytes: u64,
    outstanding_physical_claims: u64,
    request_physical_bytes: u64,
    min_free_bytes: u64,
) -> bool {
    available_bytes
        .checked_sub(outstanding_physical_claims)
        .and_then(|remaining| remaining.checked_sub(request_physical_bytes))
        .is_some_and(|remaining| remaining >= min_free_bytes)
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// Durable local ownership after a reservation has been committed to the journal.
#[derive(Debug)]
pub(crate) struct CommittedSsdReservation {
    request: SsdReservationRequest,
    physical_bytes: u64,
    sample: PhysicalSpaceSample,
}

impl CommittedSsdReservation {
    pub(crate) fn request(&self) -> SsdReservationRequest {
        self.request
    }

    pub(crate) fn physical_bytes(&self) -> u64 {
        self.physical_bytes
    }

    pub(crate) fn sample(&self) -> PhysicalSpaceSample {
        self.sample
    }
}

impl SsdReservationRequest {
    pub(crate) fn from_pending_record(
        record: &crate::writeback::model::MutationRecord,
    ) -> bincode::Result<Self> {
        let ssd_reservation_bytes = record.ssd_reservation_bytes()?;
        Ok(Self {
            ssd_reservation_bytes,
            physical_reservation_bytes: ssd_reservation_bytes,
            operations: record.ssd_reservation_operations(),
        })
    }
}

impl SsdReservationToken {
    /// Transition an admitted reservation to durable local ownership.
    ///
    /// On sample failure the charge is retained and admission is poisoned.
    pub(crate) fn commit_local(
        mut self,
        current_physical_bytes: u64,
        sample: PhysicalSpaceSample,
    ) -> Result<CommittedSsdReservation, ReservationError> {
        if self.state != ReservationState::Admitted {
            self.state = ReservationState::Disarmed;
            return Err(ReservationError::Poisoned(
                "SSD reservation already committed or released".into(),
            ));
        }
        match self
            .admission
            .transition_local(self.request, current_physical_bytes, sample)
        {
            Ok(sample) => {
                self.state = ReservationState::Disarmed;
                Ok(CommittedSsdReservation {
                    request: self.request,
                    physical_bytes: current_physical_bytes,
                    sample,
                })
            }
            Err(error) => {
                self.state = ReservationState::Disarmed;
                Err(error)
            }
        }
    }
}

impl SsdAdmissionInner {
    fn transition_local(
        self: &Arc<Self>,
        request: SsdReservationRequest,
        current_physical_bytes: u64,
        sample: PhysicalSpaceSample,
    ) -> Result<PhysicalSpaceSample, ReservationError> {
        let mut state = lock(&self.state);
        if let Err(error) = self.observe_locked(&mut state, sample) {
            drop(state);
            self.terminate(ReservationError::Poisoned(format!(
                "SSD reservation transition failed: {error}"
            )));
            return Err(error);
        }
        if let Some(error) = &state.terminal {
            return Err(error.clone());
        }
        let outstanding = match state
            .outstanding_physical_claims
            .checked_sub(request.physical_reservation_bytes)
            .and_then(|remaining| remaining.checked_add(current_physical_bytes))
        {
            Some(outstanding) => outstanding,
            None => {
                drop(state);
                let error = ReservationError::Poisoned(
                    "SSD physical-claim adjustment overflow or underflow".into(),
                );
                self.terminate(error.clone());
                return Err(error);
            }
        };
        state.outstanding_physical_claims = outstanding;
        self.refresh(&mut state);
        Ok(sample)
    }
}

/// Transition every token in a durable batch with one shared sample.
pub(crate) fn commit_batch_local(
    tokens: Vec<SsdReservationToken>,
    physical_bytes: &[u64],
    sample: PhysicalSpaceSample,
) -> Result<Vec<CommittedSsdReservation>, ReservationError> {
    if tokens.len() != physical_bytes.len() {
        return Err(ReservationError::Poisoned(
            "SSD batch transition requires one physical size per token".into(),
        ));
    }
    let mut committed = Vec::with_capacity(tokens.len());
    for (token, physical) in tokens.into_iter().zip(physical_bytes.iter().copied()) {
        committed.push(token.commit_local(physical, sample)?);
    }
    Ok(committed)
}

impl CommittedSsdReservation {
    pub(crate) fn commit_local(
        self,
        _current_physical_bytes: u64,
        _sample: PhysicalSpaceSample,
    ) -> Result<Self, ReservationError> {
        Err(ReservationError::Poisoned(
            "SSD reservation already committed or released".into(),
        ))
    }

    pub(crate) fn release(self) -> Result<(), ReservationError> {
        Err(ReservationError::Poisoned(
            "cannot release a committed SSD reservation".into(),
        ))
    }
}
