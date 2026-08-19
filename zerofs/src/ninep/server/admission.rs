use ninep_proto::P9_MAX_MSIZE;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, OnceLock};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tokio_util::task::task_tracker::TaskTrackerToken;

/// Process-wide protocol memory available to requests and their possible replies.
pub(super) const GLOBAL_INFLIGHT_MEMORY: usize = 384 * 1024 * 1024;
/// Bound one connection to a stable share of the process budget.
pub(super) const CONNECTION_INFLIGHT_MEMORY: usize = 64 * 1024 * 1024;
pub(super) const GLOBAL_INFLIGHT_REQUESTS: usize = 64;
pub(super) const CONNECTION_INFLIGHT_REQUESTS: usize = 16;
/// Active transports allowed to receive a frame before byte admission.
/// This leaves room for the expected sixteen idle Mesh sessions and the
/// default uploader's sixteen connections while bounding reconnect storms.
pub(super) const GLOBAL_TRANSPORT_SESSIONS: usize = 64;
pub(super) const MAX_PRE_ADMISSION_MEMORY: usize =
    GLOBAL_TRANSPORT_SESSIONS * P9_MAX_MSIZE as usize;
pub(super) const DOCUMENTED_P9_MEMORY_BOUND: usize =
    GLOBAL_INFLIGHT_MEMORY + MAX_PRE_ADMISSION_MEMORY;
#[cfg(any(feature = "webui", test))]
const WEBSOCKET_RECEIVE_RESERVATION: u32 = 2 * P9_MAX_MSIZE;

pub(crate) struct P9GlobalAdmission {
    bytes: Arc<Semaphore>,
    requests: Arc<Semaphore>,
    transports: Arc<Semaphore>,
    receive_bytes: Arc<Semaphore>,
    pub(super) byte_limit: usize,
    request_limit: usize,
    pub(super) transport_limit: usize,
}

#[derive(Clone)]
pub(crate) struct P9AcceptedWorkTracker {
    inner: Arc<P9AcceptedWorkInner>,
}

struct P9AcceptedWorkInner {
    accepting: AtomicBool,
    tasks: TaskTracker,
}

pub(super) struct P9AcceptedWorkGuard {
    _token: TaskTrackerToken,
}

impl P9AcceptedWorkTracker {
    pub(crate) fn new() -> Self {
        Self {
            inner: Arc::new(P9AcceptedWorkInner {
                accepting: AtomicBool::new(true),
                tasks: TaskTracker::new(),
            }),
        }
    }

    pub(super) fn try_accept(&self) -> Option<P9AcceptedWorkGuard> {
        // Register first so shutdown cannot observe an empty tracker after a
        // racing request has observed the accepting state.
        let token = self.inner.tasks.token();
        if !self.inner.accepting.load(Ordering::Acquire) {
            return None;
        }
        Some(P9AcceptedWorkGuard { _token: token })
    }

    pub(crate) fn stop_accepting(&self) {
        self.inner.accepting.store(false, Ordering::Release);
        self.inner.tasks.close();
    }

    pub(crate) async fn wait(&self) {
        self.inner.tasks.wait().await;
    }

    #[cfg(test)]
    pub(super) fn len(&self) -> usize {
        self.inner.tasks.len()
    }
}

impl P9GlobalAdmission {
    pub(crate) fn shared() -> &'static Arc<Self> {
        static ADMISSION: OnceLock<Arc<P9GlobalAdmission>> = OnceLock::new();
        ADMISSION.get_or_init(|| {
            Arc::new(Self::new(
                GLOBAL_INFLIGHT_MEMORY,
                GLOBAL_INFLIGHT_REQUESTS,
                GLOBAL_TRANSPORT_SESSIONS,
            ))
        })
    }

    fn new(byte_limit: usize, request_limit: usize, transport_limit: usize) -> Self {
        Self {
            bytes: Arc::new(Semaphore::new(byte_limit)),
            requests: Arc::new(Semaphore::new(request_limit)),
            transports: Arc::new(Semaphore::new(transport_limit)),
            receive_bytes: Arc::new(Semaphore::new(transport_limit * P9_MAX_MSIZE as usize)),
            byte_limit,
            request_limit,
            transport_limit,
        }
    }

    pub(crate) fn try_admit_transport(self: &Arc<Self>) -> anyhow::Result<P9TransportPermit> {
        metrics::gauge!("zerofs_p9_transport_session_capacity").set(self.transport_limit as f64);
        metrics::gauge!("zerofs_p9_memory_bound_bytes").set(DOCUMENTED_P9_MEMORY_BOUND as f64);
        let permit = Arc::clone(&self.transports)
            .try_acquire_owned()
            .map_err(|_| anyhow::anyhow!("9P transport capacity exhausted"))?;
        metrics::gauge!("zerofs_p9_active_sessions").increment(1.0);
        Ok(P9TransportPermit { _permit: permit })
    }

    pub(crate) async fn admit_receive(
        self: &Arc<Self>,
        shutdown: &CancellationToken,
    ) -> anyhow::Result<P9ReceivePermit> {
        self.admit_receive_bytes(P9_MAX_MSIZE, shutdown).await
    }

    #[cfg(any(feature = "webui", test))]
    pub(crate) async fn admit_websocket_receive(
        self: &Arc<Self>,
        shutdown: &CancellationToken,
    ) -> anyhow::Result<P9ReceivePermit> {
        // Tungstenite copies fragmented messages into a growing collector
        // while retaining the current frame. Charging both maximum buffers
        // keeps their combined peak inside the shared receive envelope.
        self.admit_receive_bytes(WEBSOCKET_RECEIVE_RESERVATION, shutdown)
            .await
    }

    async fn admit_receive_bytes(
        self: &Arc<Self>,
        permits: u32,
        shutdown: &CancellationToken,
    ) -> anyhow::Result<P9ReceivePermit> {
        let permit = tokio::select! {
            biased;
            _ = shutdown.cancelled() => anyhow::bail!("9P receive cancelled by shutdown"),
            result = Arc::clone(&self.receive_bytes).acquire_many_owned(permits) => result?,
        };
        metrics::gauge!("zerofs_p9_receive_reserved_bytes").increment(f64::from(permits));
        Ok(P9ReceivePermit {
            _permit: permit,
            reserved_bytes: permits as usize,
        })
    }

    pub(crate) fn connection_with_accepted_work(
        self: &Arc<Self>,
        accepted_work: P9AcceptedWorkTracker,
    ) -> P9ConnectionAdmission {
        metrics::gauge!("zerofs_p9_inflight_memory_capacity_bytes").set(self.byte_limit as f64);
        metrics::gauge!("zerofs_p9_inflight_request_capacity").set(self.request_limit as f64);
        P9ConnectionAdmission {
            global: Arc::clone(self),
            accepted_work,
            bytes: Arc::new(Semaphore::new(CONNECTION_INFLIGHT_MEMORY)),
            requests: Arc::new(Semaphore::new(CONNECTION_INFLIGHT_REQUESTS)),
            byte_limit: CONNECTION_INFLIGHT_MEMORY,
        }
    }

    #[cfg(test)]
    pub(super) fn for_test(byte_limit: usize, request_limit: usize) -> Arc<Self> {
        Arc::new(Self::new(
            byte_limit,
            request_limit,
            GLOBAL_TRANSPORT_SESSIONS,
        ))
    }

    #[cfg(test)]
    pub(super) fn for_test_with_transports(
        byte_limit: usize,
        request_limit: usize,
        transport_limit: usize,
    ) -> Arc<Self> {
        Arc::new(Self::new(byte_limit, request_limit, transport_limit))
    }

    #[cfg(test)]
    pub(super) fn connection_for_test(
        self: &Arc<Self>,
        byte_limit: usize,
        request_limit: usize,
    ) -> P9ConnectionAdmission {
        P9ConnectionAdmission {
            global: Arc::clone(self),
            accepted_work: P9AcceptedWorkTracker::new(),
            bytes: Arc::new(Semaphore::new(byte_limit)),
            requests: Arc::new(Semaphore::new(request_limit)),
            byte_limit,
        }
    }

    #[cfg(test)]
    pub(super) fn snapshot(&self) -> P9AdmissionSnapshot {
        P9AdmissionSnapshot {
            reserved_bytes: self.byte_limit - self.bytes.available_permits(),
            active_requests: self.request_limit - self.requests.available_permits(),
            active_transports: self.transport_limit - self.transports.available_permits(),
            receive_reserved_bytes: self.transport_limit * P9_MAX_MSIZE as usize
                - self.receive_bytes.available_permits(),
        }
    }
}

#[cfg(test)]
#[derive(Debug, PartialEq, Eq)]
pub(super) struct P9AdmissionSnapshot {
    pub(super) reserved_bytes: usize,
    pub(super) active_requests: usize,
    pub(super) active_transports: usize,
    pub(super) receive_reserved_bytes: usize,
}

pub(crate) struct P9TransportPermit {
    _permit: OwnedSemaphorePermit,
}

impl Drop for P9TransportPermit {
    fn drop(&mut self) {
        metrics::gauge!("zerofs_p9_active_sessions").decrement(1.0);
    }
}

pub(crate) struct P9ReceivePermit {
    _permit: OwnedSemaphorePermit,
    reserved_bytes: usize,
}

impl Drop for P9ReceivePermit {
    fn drop(&mut self) {
        metrics::gauge!("zerofs_p9_receive_reserved_bytes").decrement(self.reserved_bytes as f64);
    }
}

#[derive(Clone)]
pub(crate) struct P9ConnectionAdmission {
    pub(super) global: Arc<P9GlobalAdmission>,
    pub(super) accepted_work: P9AcceptedWorkTracker,
    bytes: Arc<Semaphore>,
    requests: Arc<Semaphore>,
    byte_limit: usize,
}

impl P9ConnectionAdmission {
    #[cfg(test)]
    pub(super) async fn admit_request(
        &self,
        request_bytes: usize,
        possible_response_bytes: usize,
    ) -> anyhow::Result<P9AdmissionPermit> {
        self.admit_request_or_shutdown(
            request_bytes,
            possible_response_bytes,
            &CancellationToken::new(),
        )
        .await
    }

    pub(super) async fn admit_request_or_shutdown(
        &self,
        request_bytes: usize,
        possible_response_bytes: usize,
        shutdown: &CancellationToken,
    ) -> anyhow::Result<P9AdmissionPermit> {
        let reserved_bytes = request_bytes
            .checked_add(possible_response_bytes)
            .ok_or_else(|| anyhow::anyhow!("9P request memory reservation overflow"))?;
        if reserved_bytes > self.byte_limit || reserved_bytes > self.global.byte_limit {
            anyhow::bail!("9P request requires {reserved_bytes} bytes, above the admission limit");
        }
        let permits = u32::try_from(reserved_bytes)
            .map_err(|_| anyhow::anyhow!("9P request memory reservation exceeds u32"))?;

        let global_request =
            acquire_owned_or_shutdown(Arc::clone(&self.global.requests), 1, shutdown).await?;
        let local_request =
            acquire_owned_or_shutdown(Arc::clone(&self.requests), 1, shutdown).await?;
        let local_bytes =
            acquire_owned_or_shutdown(Arc::clone(&self.bytes), permits, shutdown).await?;
        let global_bytes =
            acquire_owned_or_shutdown(Arc::clone(&self.global.bytes), permits, shutdown).await?;

        metrics::gauge!("zerofs_p9_inflight_reserved_bytes").increment(reserved_bytes as f64);
        metrics::gauge!("zerofs_p9_inflight_requests").increment(1.0);
        Ok(P9AdmissionPermit {
            _local_request: local_request,
            _global_request: global_request,
            _local_bytes: local_bytes,
            _global_bytes: global_bytes,
            reserved_bytes,
        })
    }
}

async fn acquire_owned_or_shutdown(
    semaphore: Arc<Semaphore>,
    permits: u32,
    shutdown: &CancellationToken,
) -> anyhow::Result<OwnedSemaphorePermit> {
    tokio::select! {
        biased;
        _ = shutdown.cancelled() => anyhow::bail!("9P admission cancelled by shutdown"),
        result = semaphore.acquire_many_owned(permits) => Ok(result?),
    }
}

pub(crate) struct P9AdmissionPermit {
    _local_request: OwnedSemaphorePermit,
    _global_request: OwnedSemaphorePermit,
    _local_bytes: OwnedSemaphorePermit,
    _global_bytes: OwnedSemaphorePermit,
    reserved_bytes: usize,
}

impl Drop for P9AdmissionPermit {
    fn drop(&mut self) {
        metrics::gauge!("zerofs_p9_inflight_reserved_bytes").decrement(self.reserved_bytes as f64);
        metrics::gauge!("zerofs_p9_inflight_requests").decrement(1.0);
    }
}
