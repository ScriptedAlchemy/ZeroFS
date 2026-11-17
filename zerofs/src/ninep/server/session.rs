use super::admission::{
    P9AcceptedWorkTracker, P9ConnectionAdmission, P9GlobalAdmission, P9TransportPermit,
};
use super::frame_codec::{
    FidFootprint, FrameHeader, TCLUNK_TYPE, TFLUSH_TYPE, TVERSION_TYPE, inspect_frame_metadata,
    read_9p_frame_charged, shallow_epoch_zero_retry_write,
};
use super::response_writer::{
    P9Response, ResponseAuthority, ResponseCredit, spawn_response_writer,
};
use crate::abandonment::Abandonment;
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
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc, oneshot};
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
    policy: super::super::handler::ListenerPolicy,
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
    let handler = Arc::new(NinePHandler::for_listener(
        Arc::clone(&filesystem),
        lock_manager,
        policy,
    ));
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
        shutdown,
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

/// What a `Tflush` may do to a request, by message type.
///
/// flush(5) lets a server answer `Rflush` without the original reply only if
/// the request can be treated as never sent. Each class says when that holds.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Cancellation {
    /// The request runs to completion and answers; a `Tflush` naming it
    /// waits for that answer. `Tversion` resets the session and `Tclunk`
    /// retires a fid (and commits a pending xattr), both promptly. A `Tflush`
    /// is never itself abandoned, so flushes are answered in order.
    RunToCompletion,
    /// The request is dropped by a `Tflush` until it passes its point of no
    /// return (`crate::abandonment`), and runs to completion afterward:
    /// - Reads, lookups, getattr, readdir, readlink, statfs, getlock and the
    ///   durability waits (`Tfsync`, `Tfsyncdur`) never pass one. Dropping
    ///   them ends the wait and returns their admission. A cancelled fsync
    ///   reports nothing, so its caller may assume nothing about durability;
    ///   the flush coordinator finishes any barrier the wait had started.
    /// - Fid-binding requests (attach, walk, open, rebind, xattr walk and
    ///   create) pass it when they change the session's fid table.
    /// - Mutations pass it when they queue a commit or stage a write. Before
    ///   that nothing has been applied; after it the reply precedes `Rflush`
    ///   and the operation ledger replays the applied result to a retry.
    /// - Waiting for request admission, and for earlier same-fid requests,
    ///   is always abandonable.
    UntilPointOfNoReturn,
}

pub(super) fn cancellation(type_byte: u8) -> Cancellation {
    match type_byte {
        TVERSION_TYPE | TCLUNK_TYPE | TFLUSH_TYPE => Cancellation::RunToCompletion,
        _ => Cancellation::UntilPointOfNoReturn,
    }
}

impl Cancellation {
    fn abandonment(self) -> Arc<Abandonment> {
        match self {
            Self::RunToCompletion => Abandonment::committed(),
            Self::UntilPointOfNoReturn => Abandonment::abandonable(),
        }
    }
}

/// Request completion signal, fid footprint and cancellation state.
pub(super) struct RequestState {
    completed: CancellationToken,
    fids: FidFootprint,
    abandonment: Arc<Abandonment>,
}

impl RequestState {
    fn new(fids: FidFootprint, abandonment: Arc<Abandonment>) -> Self {
        Self {
            completed: CancellationToken::new(),
            fids,
            abandonment,
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

/// `Tflush` replies a connection may have outstanding. Each holds one tag
/// and a 7-byte reply, and needs no request admission.
pub(super) const CONNECTION_FLUSH_SLOTS: usize = 16;

/// Per-connection request registry.
pub(super) struct InflightRegistryInner {
    /// One active request per wire tag, as required by 9P.
    pub(super) entries: DashMap<u16, Arc<RequestState>>,
    /// Tail request of the receive-ordered Tflush chain for each oldtag.
    flush_tails: DashMap<u16, Arc<RequestState>>,
    /// Reply credit for outstanding `Tflush` requests.
    flush_slots: Arc<Semaphore>,
}

impl Default for InflightRegistryInner {
    fn default() -> Self {
        Self {
            entries: DashMap::new(),
            flush_tails: DashMap::new(),
            flush_slots: Arc::new(Semaphore::new(CONNECTION_FLUSH_SLOTS)),
        }
    }
}

#[derive(Clone, Default)]
pub(crate) struct InflightRegistry {
    pub(super) inner: Arc<InflightRegistryInner>,
}

impl InflightRegistry {
    /// Register a request that runs to completion.
    #[cfg(test)]
    pub(super) fn register(&self, tag: u16, fids: FidFootprint) -> anyhow::Result<RequestLease> {
        self.register_with(tag, fids, Abandonment::committed())
    }

    fn register_with(
        &self,
        tag: u16,
        fids: FidFootprint,
        abandonment: Arc<Abandonment>,
    ) -> anyhow::Result<RequestLease> {
        let Entry::Vacant(entry) = self.inner.entries.entry(tag) else {
            anyhow::bail!("client reused in-flight 9P tag {tag}");
        };
        let state = Arc::new(RequestState::new(fids, abandonment));
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
        Ok((
            self.register_with(tag, FidFootprint::All, Abandonment::committed())?,
            waiters,
        ))
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
        Ok((
            self.register_with(tag, FidFootprint::One(fid), Abandonment::committed())?,
            waiters,
        ))
    }

    /// Append `Tflush` to the target tag's response-order chain, cancelling
    /// the target if it has made no observable change.
    ///
    /// A cancelled target never answers, so its tag is retired and its
    /// barrier waiters released here, before `Rflush` lets the client reuse
    /// the tag. Otherwise `Rflush` waits for the target's reply.
    pub(super) fn register_flush(
        &self,
        tag: u16,
        oldtag: u16,
    ) -> anyhow::Result<FlushRegistration> {
        let mut target = self.waiter(oldtag);
        let mut lease = self.register_with(tag, FidFootprint::None, Abandonment::committed())?;
        if let Some(abandoned) = target.take_if(|target| target.abandonment.abandon()) {
            self.inner
                .entries
                .remove_if(&oldtag, |_, current| Arc::ptr_eq(current, &abandoned));
            abandoned.complete();
        }
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

    async fn flush_slot(&self) -> anyhow::Result<OwnedSemaphorePermit> {
        Ok(Arc::clone(&self.inner.flush_slots).acquire_owned().await?)
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
    credit: impl Into<ResponseCredit>,
) -> Result<(), mpsc::error::SendError<()>> {
    let permit = tx.reserve().await?;
    request_lease.publish_terminal_response(|| {
        permit.send(P9Response::new(request_lease.tag, response_bytes, credit))
    });
    Ok(())
}

/// Connection state every received request needs.
pub(crate) struct ConnectionContext<'a> {
    pub(crate) handler: &'a Arc<NinePHandler>,
    pub(crate) tx: &'a mpsc::Sender<P9Response>,
    pub(crate) inflight: &'a InflightRegistry,
    pub(crate) admission: &'a P9ConnectionAdmission,
    pub(crate) requests: &'a TaskTracker,
    pub(crate) connection_shutdown: &'a CancellationToken,
    pub(crate) process_shutdown: &'a CancellationToken,
}

/// Memory that covers a received frame until its request is admitted.
enum ReceiveCharge {
    /// The caller holds receive credit until admission (WebSocket, tests).
    Caller,
    /// The connection reader's pending-frame envelope.
    Pending(#[allow(dead_code, reason = "held for its Drop")] OwnedSemaphorePermit),
    /// A `Tflush` reply slot.
    Flush(OwnedSemaphorePermit),
}

/// Upper bound on one spawned request task, charged with its frame against the
/// reader's pending envelope. A debug assertion at spawn keeps it honest in
/// every test that dispatches a request.
pub(super) const PENDING_REQUEST_TASK_BYTES: usize = 16 * 1024;

/// A request whose task has started and is waiting for request admission.
struct StartedRequest {
    admitted: oneshot::Receiver<anyhow::Result<()>>,
}

/// Dispatch a frame from a bounded WebSocket or a test. Returns once the
/// request is admitted, so the caller's receive credit stays charged until
/// request admission covers the frame.
#[cfg_attr(
    not(any(feature = "webui", test)),
    allow(dead_code, reason = "the TCP reader starts requests without waiting")
)]
#[allow(
    clippy::too_many_arguments,
    reason = "dispatch keeps connection and process ownership explicit"
)]
pub(crate) async fn dispatch_9p_frame(
    frame: Bytes,
    handler: &Arc<NinePHandler>,
    tx: &mpsc::Sender<P9Response>,
    inflight: &InflightRegistry,
    admission: &P9ConnectionAdmission,
    requests: &TaskTracker,
    connection_shutdown: &CancellationToken,
    process_shutdown: &CancellationToken,
) -> anyhow::Result<()> {
    let context = ConnectionContext {
        handler,
        tx,
        inflight,
        admission,
        requests,
        connection_shutdown,
        process_shutdown,
    };
    let charge = if frame.get(4) == Some(&TFLUSH_TYPE) {
        ReceiveCharge::Flush(inflight.flush_slot().await?)
    } else {
        ReceiveCharge::Caller
    };
    let started = start_9p_request(frame, &context, charge)?;
    match started.admitted.await {
        Ok(result) => result,
        // The request ended before admission: a Tflush cancelled it, or
        // process shutdown dropped it.
        Err(_) if process_shutdown.is_cancelled() => {
            anyhow::bail!("9P request admission is sealed for shutdown")
        }
        Err(_) => Ok(()),
    }
}

/// Register a received frame in receive order and spawn its request task.
///
/// Registration, the Tflush target capture, receive-order barrier snapshots
/// and the FIRST dedup reservation all happen here, synchronously. Request
/// admission happens inside the task, so the reader can keep reading while a
/// request waits for capacity. Only `Tflush` skips admission.
fn start_9p_request(
    frame: Bytes,
    context: &ConnectionContext<'_>,
    charge: ReceiveCharge,
) -> anyhow::Result<StartedRequest> {
    if frame.len() < P9_MIN_MESSAGE_SIZE as usize {
        error!("Message too short: {} bytes", frame.len());
        return Err(anyhow::anyhow!("Message too short"));
    }
    if frame.len() > P9_MAX_MSIZE as usize {
        anyhow::bail!("9P message exceeds maximum negotiated size");
    }

    let type_byte = frame[4];
    let tag = u16::from_le_bytes([frame[5], frame[6]]);
    let zerofs_protocol = context.handler.zerofs_protocol_enabled();
    let metadata = inspect_frame_metadata(&frame, zerofs_protocol);
    let shallow_retry = shallow_epoch_zero_retry_write(&frame, metadata);
    let accepted_work = context
        .admission
        .accepted_work
        .try_accept()
        .ok_or_else(|| anyhow::anyhow!("9P request admission is sealed for shutdown"))?;

    // A replacement connection can deliver RETRY while this fully received
    // FIRST is waiting for request/response capacity. Reserve its identity
    // before that first await, and retain process ownership if the old
    // connection closes. The received frame stays charged to receive credit
    // until request admission covers it.
    let received_guard = metadata
        .received_op
        .and_then(|(op_id, op_flags, _origin_epoch)| {
            context
                .handler
                .try_reserve_received_first(op_id, op_flags, type_byte)
        });
    let admission_shutdown = if received_guard.is_some() {
        context.process_shutdown.clone()
    } else {
        context.connection_shutdown.clone()
    };

    // Capture `oldtag` and barrier snapshots before any yield or tag reuse.
    let (flush_oldtag, flush_waiters, request_lease, prior_waiters) = if type_byte == TFLUSH_TYPE {
        if frame.len() != P9_HEADER_SIZE + 2 {
            error!("Tflush message has {} bytes", frame.len());
            anyhow::bail!("Tflush message must carry exactly one oldtag");
        }
        let oldtag = u16::from_le_bytes([frame[P9_HEADER_SIZE], frame[P9_HEADER_SIZE + 1]]);
        let FlushRegistration {
            lease,
            target,
            predecessor,
        } = context.inflight.register_flush(tag, oldtag)?;
        let waiters = target.into_iter().chain(predecessor).collect();
        (Some(oldtag), waiters, lease, Vec::new())
    } else {
        // `Tclunk` waits on its fid; `Tversion` waits on the whole session.
        let (request_lease, prior_waiters) = match (type_byte, metadata.fids) {
            (TVERSION_TYPE, _) => context.inflight.register_after_prior_snapshot(tag)?,
            (TCLUNK_TYPE, FidFootprint::One(fid)) => {
                context.inflight.register_after_fid_snapshot(tag, fid)?
            }
            _ => (
                context.inflight.register_with(
                    tag,
                    metadata.fids,
                    cancellation(type_byte).abandonment(),
                )?,
                Vec::new(),
            ),
        };
        (None, Vec::new(), request_lease, prior_waiters)
    };
    let abandonment = Arc::clone(&request_lease.state.abandonment);
    let revoked = accepted_work.revoked();

    let handler = Arc::clone(context.handler);
    let tx = context.tx.clone();
    let admission = context.admission.clone();
    let request_shutdown = context.process_shutdown.clone();
    let (admitted_tx, admitted) = oneshot::channel();

    let request = async move {
        let _accepted_work = accepted_work;
        let credit = match charge {
            ReceiveCharge::Flush(slot) => {
                let _ = admitted_tx.send(Ok(()));
                ResponseCredit::Flush(slot)
            }
            pending => {
                match admission
                    .admit_frame_or_shutdown(&frame, &admission_shutdown)
                    .await
                {
                    Ok(admitted) => {
                        // Request admission now covers the frame.
                        drop(pending);
                        let _ = admitted_tx.send(Ok(()));
                        ResponseCredit::from(admitted)
                    }
                    Err(error) => {
                        debug!("9P request {tag} ended before admission: {error}");
                        let _ = admitted_tx.send(Err(error));
                        return;
                    }
                }
            }
        };

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
                let response = Arc::clone(&request_lease.state.abandonment)
                    .scope(handler.handle_message_with_received_admission(
                        tag,
                        parsed.op_id,
                        parsed.op_flags,
                        parsed.op_origin_epoch,
                        parsed.body,
                        received_guard,
                    ))
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

        // A reply commits the request: from here a Tflush waits for it. If a
        // Tflush won instead, the client already treats it as never sent.
        if request_lease.state.abandonment.commit().is_err() {
            return;
        }

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
                    enqueue_terminal_response(&tx, response_bytes, &request_lease, credit).await
                {
                    warn!("Failed to send response for tag {}: {}", tag, e);
                }
            }
            None => {
                error!("Failed to serialize response for tag {}", tag);
            }
        }

        drop(request_lease);
    };
    debug_assert!(
        std::mem::size_of_val(&request) <= PENDING_REQUEST_TASK_BYTES,
        "a 9P request task outgrew its pending charge: {} bytes",
        std::mem::size_of_val(&request)
    );

    let request = context.requests.track_future(async move {
        // A disconnected client retains process ownership during its bounded
        // settlement grace. A Tflush, and process shutdown, drop only a
        // request that has made no observable change. Past its point of no
        // return a request may hold locks across a queued commit and owes the
        // operation ledger its result, so it runs on until process shutdown
        // revokes it at the end of its grace
        // (`P9AcceptedWorkTracker::settle`).
        tokio::select! {
            biased;
            _ = revoked => {}
            _ = abandoned(&request_shutdown, &abandonment) => {}
            _ = request => {}
        }
    });
    // Request admission charges the spawned task at most this much.
    debug_assert!(
        std::mem::size_of_val(&request) <= PENDING_REQUEST_TASK_BYTES,
        "the spawned 9P request task is {} bytes",
        std::mem::size_of_val(&request)
    );
    drop(spawn_named("9p-request", request));
    Ok(StartedRequest { admitted })
}

/// Resolves once `abandonment` is abandoned: by a `Tflush`, or by process
/// shutdown while the request has not passed its point of no return.
async fn abandoned(shutdown: &CancellationToken, abandonment: &Abandonment) {
    let abandon_at_shutdown = async {
        shutdown.cancelled().await;
        // Fails once the request is committed; it then runs to completion.
        abandonment.abandon();
        std::future::pending::<()>().await
    };
    tokio::select! {
        _ = abandonment.abandoned() => {}
        _ = abandon_at_shutdown => {}
    }
}

/// Charge for one frame read by the connection reader.
///
/// Frames that wait for request admission share one maximum frame of pending
/// envelope, so the reader keeps reading while the connection's request
/// slots are full. A `Tflush` takes a flush slot instead: it must stay
/// readable when the envelope is full, because cancelling is how a client
/// frees the capacity it is waiting for.
async fn charge_received_frame(
    header: FrameHeader,
    pending: &Arc<Semaphore>,
    inflight: &InflightRegistry,
) -> anyhow::Result<ReceiveCharge> {
    if header.type_byte == TFLUSH_TYPE {
        return Ok(ReceiveCharge::Flush(inflight.flush_slot().await?));
    }
    let bytes = header
        .len
        .saturating_add(PENDING_REQUEST_TASK_BYTES)
        .min(P9_MAX_MSIZE as usize);
    let permits = u32::try_from(bytes).expect("a 9P frame charge fits in u32");
    Ok(ReceiveCharge::Pending(
        Arc::clone(pending).acquire_many_owned(permits).await?,
    ))
}

pub(super) async fn handle_client_loop<R>(
    handler: Arc<NinePHandler>,
    mut read_stream: R,
    tx: mpsc::Sender<P9Response>,
    connection_shutdown: CancellationToken,
    process_shutdown: CancellationToken,
    admission: &P9ConnectionAdmission,
    requests: &TaskTracker,
) -> anyhow::Result<()>
where
    R: AsyncRead + Unpin,
{
    let inflight = InflightRegistry::default();
    let context = ConnectionContext {
        handler: &handler,
        tx: &tx,
        inflight: &inflight,
        admission,
        requests,
        connection_shutdown: &connection_shutdown,
        process_shutdown: &process_shutdown,
    };

    // Hold one maximum-frame receive credit for the reader's life. It covers
    // the frame being read plus every frame still waiting for request
    // admission; the exact reader never allocates a replacement buffer.
    let _receive = admission.global.admit_receive(&connection_shutdown).await?;
    let pending = Arc::new(Semaphore::new(P9_MAX_MSIZE as usize));

    loop {
        let Some((frame, charge)) =
            read_9p_frame_charged(&mut read_stream, &connection_shutdown, async |header| {
                charge_received_frame(header, &pending, &inflight).await
            })
            .await?
        else {
            debug!("Client disconnected");
            return Ok(());
        };
        drop(start_9p_request(frame, &context, charge)?);
    }
}
