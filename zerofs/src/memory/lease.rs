use super::{PoolKind, ResolvedMemoryPlan};
use bytes::Bytes;
use serde::Serialize;
use std::collections::{BTreeMap, VecDeque};
use std::sync::{Arc, Mutex, MutexGuard};
use tokio::sync::Notify;

#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum LeaseError {
    #[error("memory admission is closed")]
    Closed,
    #[error("memory request needs {requested} bytes but pool capacity is {capacity} bytes")]
    TooLarge { requested: u64, capacity: u64 },
    #[error("memory capacity is unavailable")]
    Unavailable,
    #[error("memory waiter capacity is full")]
    WaitersFull,
    #[error("memory accounting overflowed")]
    Overflow,
    #[error("backing allocation capacity exceeds its memory lease")]
    UnchargedCapacity,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize)]
pub struct PoolUsage {
    pub bytes: u64,
    pub entries: u64,
    pub waiters: usize,
}

#[derive(Debug)]
struct Waiter {
    id: u64,
    notify: Arc<Notify>,
}

#[derive(Debug, Default)]
struct State {
    bytes: u64,
    entries: u64,
    next_id: u64,
    closed: bool,
    waiters: VecDeque<Waiter>,
}

#[derive(Debug)]
struct Inner {
    capacity: u64,
    entries: u64,
    max_waiters: usize,
    state: Mutex<State>,
}

#[derive(Clone, Debug)]
pub struct MemoryPool {
    inner: Arc<Inner>,
}

/// A move-only reservation for backing bytes and one entry. Obtain it before
/// allocation, retain it while waiting downstream, and move it with accepted work.
#[derive(Debug)]
pub struct MemoryLease {
    inner: Arc<Inner>,
    bytes: u64,
}

impl MemoryPool {
    pub fn new(capacity: u64, entries: u64, max_waiters: usize) -> Self {
        Self {
            inner: Arc::new(Inner {
                capacity,
                entries,
                max_waiters,
                state: Mutex::new(State::default()),
            }),
        }
    }

    pub fn try_acquire(&self, bytes: u64) -> Result<MemoryLease, LeaseError> {
        self.validate_size(bytes)?;
        let mut state = self.inner.lock();
        if state.closed {
            return Err(LeaseError::Closed);
        }
        if !state.waiters.is_empty() || !self.inner.fits(&state, bytes) {
            return Err(LeaseError::Unavailable);
        }
        self.inner.charge(&mut state, bytes)
    }

    pub async fn acquire(&self, bytes: u64) -> Result<MemoryLease, LeaseError> {
        self.validate_size(bytes)?;
        let (id, notify) = {
            let mut state = self.inner.lock();
            if state.closed {
                return Err(LeaseError::Closed);
            }
            if state.waiters.is_empty() && self.inner.fits(&state, bytes) {
                return self.inner.charge(&mut state, bytes);
            }
            if state.waiters.len() >= self.inner.max_waiters {
                return Err(LeaseError::WaitersFull);
            }
            let id = state.next_id;
            state.next_id = id.checked_add(1).ok_or(LeaseError::Overflow)?;
            let notify = Arc::new(Notify::new());
            state.waiters.push_back(Waiter {
                id,
                notify: notify.clone(),
            });
            (id, notify)
        };
        let mut registration = Registration {
            inner: self.inner.clone(),
            id,
            active: true,
        };
        loop {
            {
                let mut state = self.inner.lock();
                if state.closed {
                    return Err(LeaseError::Closed);
                }
                if state.waiters.front().is_some_and(|head| head.id == id)
                    && self.inner.fits(&state, bytes)
                {
                    let lease = self.inner.charge(&mut state, bytes)?;
                    state.waiters.pop_front();
                    registration.active = false;
                    wake_head(&state);
                    return Ok(lease);
                }
            }
            notify.notified().await;
        }
    }

    pub fn usage(&self) -> PoolUsage {
        let state = self.inner.lock();
        PoolUsage {
            bytes: state.bytes,
            entries: state.entries,
            waiters: state.waiters.len(),
        }
    }

    pub fn close(&self) {
        let mut state = self.inner.lock();
        state.closed = true;
        for waiter in &state.waiters {
            waiter.notify.notify_one();
        }
    }

    fn validate_size(&self, bytes: u64) -> Result<(), LeaseError> {
        if bytes > self.inner.capacity {
            return Err(LeaseError::TooLarge {
                requested: bytes,
                capacity: self.inner.capacity,
            });
        }
        if self.inner.entries == 0 {
            return Err(LeaseError::Unavailable);
        }
        Ok(())
    }
}

impl Inner {
    fn lock(&self) -> MutexGuard<'_, State> {
        self.state.lock().expect("memory accounting mutex poisoned")
    }
    fn fits(&self, state: &State, bytes: u64) -> bool {
        state
            .bytes
            .checked_add(bytes)
            .is_some_and(|sum| sum <= self.capacity)
            && state
                .entries
                .checked_add(1)
                .is_some_and(|sum| sum <= self.entries)
    }
    fn charge(self: &Arc<Self>, state: &mut State, bytes: u64) -> Result<MemoryLease, LeaseError> {
        let next_bytes = state.bytes.checked_add(bytes).ok_or(LeaseError::Overflow)?;
        let next_entries = state.entries.checked_add(1).ok_or(LeaseError::Overflow)?;
        assert!(next_bytes <= self.capacity && next_entries <= self.entries);
        state.bytes = next_bytes;
        state.entries = next_entries;
        Ok(MemoryLease {
            inner: self.clone(),
            bytes,
        })
    }
}

fn wake_head(state: &State) {
    if let Some(head) = state.waiters.front() {
        head.notify.notify_one();
    }
}

struct Registration {
    inner: Arc<Inner>,
    id: u64,
    active: bool,
}
impl Drop for Registration {
    fn drop(&mut self) {
        if self.active {
            let mut state = self.inner.lock();
            state.waiters.retain(|waiter| waiter.id != self.id);
            wake_head(&state);
        }
    }
}

impl Drop for MemoryLease {
    fn drop(&mut self) {
        let mut state = self.inner.lock();
        state.bytes = state
            .bytes
            .checked_sub(self.bytes)
            .expect("memory byte accounting underflow");
        state.entries = state
            .entries
            .checked_sub(1)
            .expect("memory entry accounting underflow");
        wake_head(&state);
    }
}

struct LeasedBuffer {
    data: Vec<u8>,
    _lease: MemoryLease,
}
impl AsRef<[u8]> for LeasedBuffer {
    fn as_ref(&self) -> &[u8] {
        &self.data
    }
}

impl MemoryLease {
    pub fn bytes(&self) -> u64 {
        self.bytes
    }

    /// Allocate the vector only after acquiring its capacity. Bytes clones and
    /// slices retain the owner and its whole charge until the final reference drops.
    pub fn into_bytes(self, data: Vec<u8>) -> Result<Bytes, LeaseError> {
        if u64::try_from(data.capacity()).map_err(|_| LeaseError::Overflow)? > self.bytes {
            return Err(LeaseError::UnchargedCapacity);
        }
        Ok(Bytes::from_owner(LeasedBuffer { data, _lease: self }))
    }
}

/// Headroom has no admission pool. The protected progress pool is distinct from
/// ordinary I/O so later integration can restrict it to drain and control workers.
#[derive(Clone, Debug)]
pub struct MemoryBudget {
    pools: BTreeMap<PoolKind, MemoryPool>,
}
impl MemoryBudget {
    pub fn new(plan: &ResolvedMemoryPlan) -> Self {
        Self {
            pools: plan
                .pools()
                .iter()
                .filter(|pool| pool.kind != PoolKind::Headroom)
                .map(|pool| {
                    (
                        pool.kind,
                        MemoryPool::new(pool.capacity_bytes, pool.max_entries, pool.max_waiters),
                    )
                })
                .collect(),
        }
    }
    pub fn pool(&self, kind: PoolKind) -> Option<&MemoryPool> {
        self.pools.get(&kind)
    }
}
