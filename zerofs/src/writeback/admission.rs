use std::collections::VecDeque;
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

#[derive(Debug, Clone)]
pub struct Admission {
    inner: Arc<RamInner>,
}

#[derive(Debug)]
struct RamInner {
    capacity: u64,
    state: Mutex<RamState>,
}

#[derive(Debug, Default)]
struct RamState {
    used: u64,
    next_waiter: u64,
    waiters: VecDeque<RamWaiter>,
    terminal: Option<AdmissionError>,
}

#[derive(Debug)]
struct RamWaiter {
    id: u64,
    bytes: u64,
    sender: oneshot::Sender<Result<AdmissionPermit, AdmissionError>>,
}

#[derive(Debug)]
pub struct AdmissionPermit {
    inner: Arc<RamInner>,
    bytes: u64,
    active: bool,
}

#[derive(Debug)]
pub struct AcceptedAdmission(AdmissionPermit);

impl Admission {
    pub fn new(capacity: u64) -> Self {
        Self {
            inner: Arc::new(RamInner {
                capacity,
                state: Mutex::new(RamState::default()),
            }),
        }
    }

    pub async fn reserve(&self, bytes: u64) -> Result<AdmissionPermit, AdmissionError> {
        if bytes > self.inner.capacity {
            return Err(AdmissionError::TooLarge {
                requested: bytes,
                capacity: self.inner.capacity,
            });
        }
        let receiver = {
            let mut state = lock(&self.inner.state);
            if let Some(error) = &state.terminal {
                return Err(error.clone());
            }
            if state.waiters.is_empty() && fits(state.used, bytes, self.inner.capacity) {
                state.used += bytes;
                return Ok(AdmissionPermit::new(self.inner.clone(), bytes));
            }
            let id = state.next_waiter;
            state.next_waiter = state.next_waiter.wrapping_add(1);
            let (sender, receiver) = oneshot::channel();
            state.waiters.push_back(RamWaiter { id, bytes, sender });
            (id, receiver)
        };
        let mut registration = RamWaitRegistration {
            inner: self.inner.clone(),
            id: receiver.0,
            active: true,
        };
        let result = receiver.1.await.unwrap_or(Err(AdmissionError::Closed));
        registration.active = false;
        result
    }

    pub fn used_bytes(&self) -> u64 {
        lock(&self.inner.state).used
    }

    pub fn poison(&self, message: impl Into<String>) {
        terminate_ram(&self.inner, AdmissionError::Poisoned(message.into()));
    }

    pub fn close(&self) {
        terminate_ram(&self.inner, AdmissionError::Closed);
    }
}

impl AdmissionPermit {
    fn new(inner: Arc<RamInner>, bytes: u64) -> Self {
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
        {
            let mut state = lock(&self.inner.state);
            state.used = state.used.saturating_sub(self.bytes);
        }
        grant_ram_waiters(&self.inner);
    }
}

struct RamWaitRegistration {
    inner: Arc<RamInner>,
    id: u64,
    active: bool,
}

impl Drop for RamWaitRegistration {
    fn drop(&mut self) {
        if !self.active {
            return;
        }
        {
            let mut state = lock(&self.inner.state);
            state.waiters.retain(|waiter| waiter.id != self.id);
        }
        grant_ram_waiters(&self.inner);
    }
}

fn terminate_ram(inner: &Arc<RamInner>, error: AdmissionError) {
    let waiters = {
        let mut state = lock(&inner.state);
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

fn grant_ram_waiters(inner: &Arc<RamInner>) {
    let mut state = lock(&inner.state);
    if state.terminal.is_some() {
        return;
    }
    loop {
        let Some(waiter) = state.waiters.front() else {
            break;
        };
        if !fits(state.used, waiter.bytes, inner.capacity) {
            break;
        }
        let waiter = state.waiters.pop_front().expect("front waiter exists");
        state.used += waiter.bytes;
        let permit = AdmissionPermit::new(inner.clone(), waiter.bytes);
        if let Err(Ok(mut permit)) = waiter.sender.send(Ok(permit)) {
            permit.active = false;
            state.used -= waiter.bytes;
        }
    }
}

#[derive(Debug, Clone)]
pub struct DiskAdmission {
    inner: Arc<DiskInner>,
}

#[derive(Debug)]
struct DiskInner {
    capacity: u64,
    high_bytes: u64,
    resume_bytes: u64,
    min_free_bytes: u64,
    state: Mutex<DiskState>,
}

#[derive(Debug, Default)]
struct DiskState {
    used: u64,
    available: u64,
    paused: bool,
    next_waiter: u64,
    waiters: VecDeque<DiskWaiter>,
    terminal: Option<AdmissionError>,
}

#[derive(Debug)]
struct DiskWaiter {
    id: u64,
    bytes: u64,
    sender: oneshot::Sender<Result<DiskPermit, AdmissionError>>,
}

#[derive(Debug)]
pub struct DiskPermit {
    inner: Arc<DiskInner>,
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
        if capacity == 0
            || resume_percent == 0
            || resume_percent >= high_watermark_percent
            || high_watermark_percent > 100
        {
            return Err(AdmissionError::InvalidConfiguration(
                "requires capacity > 0 and 0 < resume < high <= 100",
            ));
        }
        Ok(Self {
            inner: Arc::new(DiskInner {
                capacity,
                high_bytes: percent_bytes(capacity, high_watermark_percent),
                resume_bytes: percent_bytes(capacity, resume_percent),
                min_free_bytes,
                state: Mutex::new(DiskState::default()),
            }),
        })
    }

    pub async fn reserve(
        &self,
        bytes: u64,
        available_filesystem_bytes: u64,
    ) -> Result<DiskPermit, AdmissionError> {
        if bytes > self.inner.capacity {
            return Err(AdmissionError::TooLarge {
                requested: bytes,
                capacity: self.inner.capacity,
            });
        }
        let receiver = {
            let mut state = lock(&self.inner.state);
            state.available = available_filesystem_bytes;
            refresh_disk_pause(&self.inner, &mut state);
            if let Some(error) = &state.terminal {
                return Err(error.clone());
            }
            if state.waiters.is_empty() && disk_fits(&self.inner, &state, bytes) {
                state.used += bytes;
                return Ok(DiskPermit::new(self.inner.clone(), bytes));
            }
            if projected(state.used, bytes) > self.inner.high_bytes {
                state.paused = true;
            }
            let id = state.next_waiter;
            state.next_waiter = state.next_waiter.wrapping_add(1);
            let (sender, receiver) = oneshot::channel();
            state.waiters.push_back(DiskWaiter { id, bytes, sender });
            (id, receiver)
        };
        let mut registration = DiskWaitRegistration {
            inner: self.inner.clone(),
            id: receiver.0,
            active: true,
        };
        let result = receiver.1.await.unwrap_or(Err(AdmissionError::Closed));
        registration.active = false;
        result
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
            state.available = available_filesystem_bytes;
            refresh_disk_pause(&self.inner, &mut state);
        }
        grant_disk_waiters(&self.inner);
        Ok(())
    }

    pub fn update_available_space(&self, available: u64) -> Result<(), AdmissionError> {
        {
            let mut state = lock(&self.inner.state);
            if let Some(error) = &state.terminal {
                return Err(error.clone());
            }
            state.available = available;
            refresh_disk_pause(&self.inner, &mut state);
        }
        grant_disk_waiters(&self.inner);
        Ok(())
    }

    pub fn used_bytes(&self) -> u64 {
        lock(&self.inner.state).used
    }

    pub fn close(&self) {
        let waiters = {
            let mut state = lock(&self.inner.state);
            if state.terminal.is_some() {
                return;
            }
            state.terminal = Some(AdmissionError::Closed);
            state.waiters.drain(..).collect::<Vec<_>>()
        };
        for waiter in waiters {
            let _ = waiter.sender.send(Err(AdmissionError::Closed));
        }
    }
}

impl DiskPermit {
    fn new(inner: Arc<DiskInner>, bytes: u64) -> Self {
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
        {
            let mut state = lock(&self.inner.state);
            state.used = state.used.saturating_sub(self.bytes);
            refresh_disk_pause(&self.inner, &mut state);
        }
        grant_disk_waiters(&self.inner);
    }
}

struct DiskWaitRegistration {
    inner: Arc<DiskInner>,
    id: u64,
    active: bool,
}

impl Drop for DiskWaitRegistration {
    fn drop(&mut self) {
        if !self.active {
            return;
        }
        {
            let mut state = lock(&self.inner.state);
            state.waiters.retain(|waiter| waiter.id != self.id);
        }
        grant_disk_waiters(&self.inner);
    }
}

fn grant_disk_waiters(inner: &Arc<DiskInner>) {
    let mut state = lock(&inner.state);
    if state.terminal.is_some() {
        return;
    }
    refresh_disk_pause(inner, &mut state);
    loop {
        let Some(waiter) = state.waiters.front() else {
            break;
        };
        if !disk_fits(inner, &state, waiter.bytes) {
            if projected(state.used, waiter.bytes) > inner.high_bytes {
                state.paused = true;
            }
            break;
        }
        let waiter = state.waiters.pop_front().expect("front waiter exists");
        state.used += waiter.bytes;
        let permit = DiskPermit::new(inner.clone(), waiter.bytes);
        if let Err(Ok(mut permit)) = waiter.sender.send(Ok(permit)) {
            permit.active = false;
            state.used -= waiter.bytes;
        }
    }
}

fn refresh_disk_pause(inner: &DiskInner, state: &mut DiskState) {
    if state.paused && state.used <= inner.resume_bytes && state.available >= inner.min_free_bytes {
        state.paused = false;
    }
}

fn disk_fits(inner: &DiskInner, state: &DiskState, bytes: u64) -> bool {
    !state.paused
        && projected(state.used, bytes) <= inner.high_bytes
        && state.available.saturating_sub(bytes) >= inner.min_free_bytes
}

fn percent_bytes(capacity: u64, percent: u8) -> u64 {
    ((capacity as u128 * percent as u128) / 100) as u64
}

fn fits(used: u64, requested: u64, capacity: u64) -> bool {
    projected(used, requested) <= capacity
}

fn projected(used: u64, requested: u64) -> u64 {
    used.saturating_add(requested)
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

#[cfg(test)]
mod tests {
    use super::{Admission, AdmissionError, DiskAdmission};
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
}
