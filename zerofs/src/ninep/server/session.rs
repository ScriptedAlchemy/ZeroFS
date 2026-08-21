use super::admission::{
    P9AcceptedWorkTracker, P9AdmissionPermit, P9ConnectionAdmission, P9GlobalAdmission,
    P9TransportPermit,
};
use super::frame_codec::{
    FidFootprint, TCLUNK_TYPE, TFLUSH_TYPE, TVERSION_TYPE, inspect_frame_metadata,
    possible_response_bytes, read_9p_frame, shallow_epoch_zero_retry_write,
};
use super::response_writer::{P9Response, ResponseAuthority, spawn_response_writer};
use crate::fs::ZeroFS;
use crate::ninep::errors::P9Error;
use crate::ninep::handler::{NinePHandler, SessionReleaseGuard};
use crate::ninep::lock_manager::FileLockManager;
use crate::task::spawn_named;
use bytes::Bytes;
use dashmap::DashMap;
use dashmap::mapref::entry::Entry;
use ninep_proto::{
    Message, P9_CHANNEL_SIZE, P9_DEBUG_BUFFER_SIZE, P9_HEADER_SIZE, P9_MAX_MSIZE,
    P9_MIN_MESSAGE_SIZE, P9Message, Rlerror, T_WRITE,
};
use std::sync::Arc;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tokio_util::task::{AbortOnDropHandle, TaskTracker};
use tracing::{debug, error, warn};

pub(super) const CLIENT_DRAIN_TIMEOUT: std::time::Duration =
    crate::replication::RESPONSE_DRAIN_TIMEOUT;

pub(super) struct P9SessionAdmission {
    pub(super) transport: P9TransportPermit,
    pub(super) transport_label: &'static str,
    pub(super) accepted_work: P9AcceptedWorkTracker,
}

struct P9SettlingMetricGuard;

impl P9SettlingMetricGuard {
    fn new() -> Self {
        metrics::gauge!("zerofs_p9_sessions_settling_requests").increment(1.0);
        Self
    }
}

impl Drop for P9SettlingMetricGuard {
    fn drop(&mut self) {
        metrics::gauge!("zerofs_p9_sessions_settling_requests").decrement(1.0);
    }
}

pub(crate) async fn settle_request_tasks(requests: TaskTracker) {
    requests.close();
    let settling = requests.len();
    if settling == 0 {
        return;
    }
    metrics::counter!("zerofs_p9_post_disconnect_requests_total").increment(settling as u64);
    let _settling_metric = P9SettlingMetricGuard::new();
    if tokio::time::timeout(CLIENT_DRAIN_TIMEOUT, requests.wait())
        .await
        .is_ok()
    {
        return;
    }

    // Every dispatched request also owns a process-level accepted-work token.
    // The connection may retire after its bounded grace; process shutdown
    // still waits for that token before final flush and database close.
    metrics::counter!("zerofs_p9_process_owned_settlements_total").increment(1);
    warn!(
        settling,
        "9P session settlement exceeded its connection grace; process ownership retained"
    );
}

pub(super) async fn join_with_timeout<T>(
    mut task: AbortOnDropHandle<T>,
    timeout: std::time::Duration,
) -> Option<Result<T, tokio::task::JoinError>> {
    match tokio::time::timeout(timeout, &mut task).await {
        Ok(result) => Some(result),
        Err(_) => {
            task.abort();
            let _ = task.await;
            None
        }
    }
}

pub(super) async fn handle_client_stream<R, W>(
    read_stream: R,
    write_stream: W,
    filesystem: Arc<ZeroFS>,
    lock_manager: Arc<FileLockManager>,
    credential_override: Option<(u32, u32)>,
    shutdown: CancellationToken,
    session: P9SessionAdmission,
) -> anyhow::Result<()>
where
    R: AsyncRead + Unpin + Send + 'static,
    W: AsyncWrite + Unpin + Send + 'static,
{
    let P9SessionAdmission {
        transport: _transport,
        transport_label,
        accepted_work,
    } = session;
    let mut handler = NinePHandler::new(Arc::clone(&filesystem), lock_manager);
    if let Some((uid, gid)) = credential_override {
        handler = handler.with_credential_override(uid, gid);
    }
    let handler = Arc::new(handler);
    let admission = P9GlobalAdmission::shared().connection_with_accepted_work(accepted_work);
    let requests = TaskTracker::new();

    let (tx, rx) = mpsc::channel::<P9Response>(P9_CHANNEL_SIZE);
    let connection_shutdown = shutdown.child_token();
    let writer_task = spawn_response_writer(
        write_stream,
        rx,
        ResponseAuthority::from_database(Arc::clone(&filesystem.db)),
        connection_shutdown.clone(),
    );

    // Retire resource guards and locks before draining received requests.
    let mut release_guard = SessionReleaseGuard::new(Arc::clone(&handler));

    let result = handle_client_loop(
        handler,
        read_stream,
        tx,
        connection_shutdown,
        &admission,
        &requests,
    )
    .await;
    release_guard.release();

    match join_with_timeout(writer_task, CLIENT_DRAIN_TIMEOUT).await {
        Some(Ok(())) => {}
        Some(Err(e)) => warn!("9P writer task failed: {e}"),
        None => {
            metrics::counter!(
                "zerofs_p9_response_drain_timeouts_total",
                "transport" => transport_label
            )
            .increment(1);
            warn!("timed out draining 9P client responses; writer aborted");
        }
    }

    settle_request_tasks(requests).await;

    result
}

/// Fid footprint used by receive-order barriers.
/// Request completion signal and fid footprint.
pub(super) struct RequestState {
    completed: CancellationToken,
    fids: FidFootprint,
}

impl RequestState {
    fn new(fids: FidFootprint) -> Self {
        Self {
            completed: CancellationToken::new(),
            fids,
        }
    }

    fn complete(&self) {
        self.completed.cancel();
    }

    pub(super) async fn wait(&self) {
        self.completed.cancelled().await;
    }
}

pub(super) type CompletionWaiter = Arc<RequestState>;

/// Per-connection request registry.
#[derive(Default)]
pub(super) struct InflightRegistryInner {
    /// One active request per wire tag, as required by 9P.
    pub(super) entries: DashMap<u16, Arc<RequestState>>,
    /// Tail request of the receive-ordered Tflush chain for each oldtag.
    flush_tails: DashMap<u16, Arc<RequestState>>,
}

#[derive(Clone, Default)]
pub(crate) struct InflightRegistry {
    pub(super) inner: Arc<InflightRegistryInner>,
}

impl InflightRegistry {
    pub(super) fn register(&self, tag: u16, fids: FidFootprint) -> anyhow::Result<RequestLease> {
        let Entry::Vacant(entry) = self.inner.entries.entry(tag) else {
            anyhow::bail!("client reused in-flight 9P tag {tag}");
        };
        let state = Arc::new(RequestState::new(fids));
        entry.insert(Arc::clone(&state));
        Ok(RequestLease {
            registry: self.clone(),
            tag,
            state,
            flush_oldtag: None,
        })
    }

    pub(super) fn waiter(&self, tag: u16) -> Option<CompletionWaiter> {
        self.inner
            .entries
            .get(&tag)
            .map(|entry| Arc::clone(entry.value()))
    }

    /// Register a whole-session barrier after snapshotting earlier requests.
    pub(super) fn register_after_prior_snapshot(
        &self,
        tag: u16,
    ) -> anyhow::Result<(RequestLease, Vec<CompletionWaiter>)> {
        let waiters = self
            .inner
            .entries
            .iter()
            .map(|entry| Arc::clone(entry.value()))
            .collect();
        Ok((self.register(tag, FidFootprint::All)?, waiters))
    }

    /// Register a fid barrier after snapshotting earlier requests for that fid.
    pub(super) fn register_after_fid_snapshot(
        &self,
        tag: u16,
        fid: u32,
    ) -> anyhow::Result<(RequestLease, Vec<CompletionWaiter>)> {
        let waiters = self
            .inner
            .entries
            .iter()
            .filter(|entry| entry.value().fids.contains(fid))
            .map(|entry| Arc::clone(entry.value()))
            .collect();
        Ok((self.register(tag, FidFootprint::One(fid))?, waiters))
    }

    /// Append `Tflush` to the target tag's response-order chain.
    pub(super) fn register_flush(
        &self,
        tag: u16,
        oldtag: u16,
    ) -> anyhow::Result<FlushRegistration> {
        let target = self.waiter(oldtag);
        let mut lease = self.register(tag, FidFootprint::None)?;
        let predecessor = self
            .inner
            .flush_tails
            .insert(oldtag, Arc::clone(&lease.state));
        lease.flush_oldtag = Some(oldtag);
        Ok(FlushRegistration {
            lease,
            target,
            predecessor,
        })
    }
}

pub(super) struct FlushRegistration {
    pub(super) lease: RequestLease,
    pub(super) target: Option<CompletionWaiter>,
    pub(super) predecessor: Option<CompletionWaiter>,
}

/// Non-cloneable authority to retire one request.
#[must_use = "dropping the lease completes and retires its request"]
pub(super) struct RequestLease {
    registry: InflightRegistry,
    tag: u16,
    pub(super) state: Arc<RequestState>,
    flush_oldtag: Option<u16>,
}

impl RequestLease {
    /// Publish a terminal response before making the tag reusable.
    pub(super) fn publish_terminal_response(&self, publish: impl FnOnce()) {
        let retired = self
            .registry
            .inner
            .entries
            .remove_if(&self.tag, |_, current| {
                if !Arc::ptr_eq(current, &self.state) {
                    return false;
                }
                publish();
                true
            });
        assert!(retired.is_some(), "request tag retired before its response");
    }

    /// Remove this lease's registry entry if it is still present.
    fn retire_registration(&self) {
        self.registry
            .inner
            .entries
            .remove_if(&self.tag, |_, current| Arc::ptr_eq(current, &self.state));
    }
}

impl Drop for RequestLease {
    fn drop(&mut self) {
        self.retire_registration();
        if let Some(oldtag) = self.flush_oldtag {
            self.registry
                .inner
                .flush_tails
                .remove_if(&oldtag, |_, current| Arc::ptr_eq(current, &self.state));
        }
        self.state.complete();
    }
}

/// Reserve response capacity, enqueue the terminal response, then retire its tag.
pub(super) async fn enqueue_terminal_response(
    tx: &mpsc::Sender<P9Response>,
    response_bytes: Vec<u8>,
    request_lease: &RequestLease,
    admission: P9AdmissionPermit,
) -> Result<(), mpsc::error::SendError<()>> {
    let permit = tx.reserve().await?;
    request_lease.publish_terminal_response(|| {
        permit.send(P9Response::new(
            request_lease.tag,
            response_bytes,
            admission,
        ))
    });
    Ok(())
}

/// Dispatch a frame from the exact-size TCP reader or bounded WebSocket.
/// Receive-time dedup and process ownership are reserved before task spawn.
pub(crate) async fn dispatch_9p_frame(
    frame: Bytes,
    handler: &Arc<NinePHandler>,
    tx: &mpsc::Sender<P9Response>,
    inflight: &InflightRegistry,
    admission: &P9ConnectionAdmission,
    requests: &TaskTracker,
    shutdown: &CancellationToken,
) -> anyhow::Result<()> {
    if frame.len() < P9_MIN_MESSAGE_SIZE as usize {
        error!("Message too short: {} bytes", frame.len());
        return Err(anyhow::anyhow!("Message too short"));
    }
    if frame.len() > P9_MAX_MSIZE as usize {
        anyhow::bail!("9P message exceeds maximum negotiated size");
    }

    let type_byte = frame[4];
    // Reserve the received allocation and a conservative type-specific reply
    // before detaching request work. Bulk writes have a fixed-size Rwrite;
    // other requests retain the full negotiated-response allowance.
    let admission_permit = admission
        .admit_request_or_shutdown(frame.len(), possible_response_bytes(type_byte), shutdown)
        .await?;
    let accepted_work = admission
        .accepted_work
        .try_accept()
        .ok_or_else(|| anyhow::anyhow!("9P request admission is sealed for shutdown"))?;
    let admission = admission_permit;

    let tag = u16::from_le_bytes([frame[5], frame[6]]);
    let zerofs_protocol = handler.zerofs_protocol_enabled();
    let metadata = inspect_frame_metadata(&frame, zerofs_protocol);
    let shallow_retry = shallow_epoch_zero_retry_write(&frame, metadata);

    // Capture `oldtag` before any yield or tag reuse.
    let (flush_oldtag, flush_waiters, request_lease, prior_waiters) = if type_byte == TFLUSH_TYPE {
        if frame.len() < P9_HEADER_SIZE + 2 {
            error!("Tflush message too short: {} bytes", frame.len());
            return Err(anyhow::anyhow!("Tflush message too short"));
        }
        let oldtag = u16::from_le_bytes([frame[P9_HEADER_SIZE], frame[P9_HEADER_SIZE + 1]]);
        let FlushRegistration {
            lease,
            target,
            predecessor,
        } = inflight.register_flush(tag, oldtag)?;
        let waiters = target.into_iter().chain(predecessor).collect();
        (Some(oldtag), waiters, lease, Vec::new())
    } else {
        // `Tclunk` waits on its fid; `Tversion` waits on the whole session.
        let (request_lease, prior_waiters) = match (type_byte, metadata.fids) {
            (TVERSION_TYPE, _) => inflight.register_after_prior_snapshot(tag)?,
            (TCLUNK_TYPE, FidFootprint::One(fid)) => {
                inflight.register_after_fid_snapshot(tag, fid)?
            }
            _ => (inflight.register(tag, metadata.fids)?, Vec::new()),
        };
        (None, Vec::new(), request_lease, prior_waiters)
    };

    // Reserve a FIRST envelope before EOF can admit a replacement RETRY.
    let received_guard = metadata
        .received_op
        .and_then(|(op_id, op_flags, _origin_epoch)| {
            handler.try_reserve_received_first(op_id, op_flags, type_byte)
        });

    let handler = Arc::clone(handler);
    let tx = tx.clone();

    let request = requests.track_future(async move {
        let _accepted_work = accepted_work;
        // Only Twrite has an inbound bulk payload worth retaining. Other
        // requests keep the regular decoder and the original error diagnostics.
        let parsed = if let Some(parsed) = shallow_retry {
            Ok(parsed)
        } else if type_byte == T_WRITE {
            P9Message::from_owned_bytes_ctx(frame.clone(), zerofs_protocol)
        } else {
            P9Message::from_bytes_ctx(&frame, zerofs_protocol)
        };

        // Barrier waiters were captured synchronously in receive order.
        for waiter in prior_waiters {
            waiter.wait().await;
        }

        // Parse failures stay registered until their `Rlerror` is enqueued.
        let response_bytes = match parsed {
            Ok(parsed) => {
                drop(frame);
                debug!(
                    "Received message type {} tag {}: {:?}",
                    parsed.type_, parsed.tag, parsed.body
                );
                let response = handler
                    .handle_message_with_received_admission(
                        tag,
                        parsed.op_id,
                        parsed.op_flags,
                        parsed.op_origin_epoch,
                        parsed.body,
                        received_guard,
                    )
                    .await;
                response.to_bytes().ok()
            }
            Err(e) => {
                debug!(
                    "Failed to parse message type {} (0x{:02x}) tag {}: {:?}",
                    type_byte, type_byte, tag, e
                );
                debug!(
                    "Message size: {}, buffer (first {} bytes): {:?}",
                    frame.len(),
                    P9_DEBUG_BUFFER_SIZE,
                    &frame[..std::cmp::min(P9_DEBUG_BUFFER_SIZE, frame.len())]
                );
                P9Message::new(
                    tag,
                    Message::Rlerror(Rlerror {
                        ecode: P9Error::NotImplemented.to_errno(),
                    }),
                )
                .to_bytes()
                .ok()
            }
        };

        if let Some(oldtag) = flush_oldtag {
            debug!("Tflush: waiting for oldtag {} to complete", oldtag);
            for waiter in flush_waiters {
                waiter.wait().await;
            }
            debug!("Tflush: oldtag {} completed", oldtag);
        }

        match response_bytes {
            Some(response_bytes) => {
                if let Err(e) =
                    enqueue_terminal_response(&tx, response_bytes, &request_lease, admission).await
                {
                    warn!("Failed to send response for tag {}: {}", tag, e);
                }
            }
            None => {
                error!("Failed to serialize response for tag {}", tag);
            }
        }

        drop(request_lease);
    });
    drop(spawn_named("9p-request", request));
    Ok(())
}

pub(super) async fn handle_client_loop<R>(
    handler: Arc<NinePHandler>,
    mut read_stream: R,
    tx: mpsc::Sender<P9Response>,
    shutdown: CancellationToken,
    admission: &P9ConnectionAdmission,
    requests: &TaskTracker,
) -> anyhow::Result<()>
where
    R: AsyncRead + Unpin,
{
    let inflight = InflightRegistry::default();

    loop {
        // Hold a maximum-frame receive credit before polling the transport.
        // The exact reader below never allocates a replacement decode buffer.
        let receive = admission.global.admit_receive(&shutdown).await?;
        let Some(full_buf) = read_9p_frame(&mut read_stream, &shutdown).await? else {
            debug!("Client disconnected");
            return Ok(());
        };

        dispatch_9p_frame(
            full_buf, &handler, &tx, &inflight, admission, requests, &shutdown,
        )
        .await?;
        drop(receive);
    }
}
