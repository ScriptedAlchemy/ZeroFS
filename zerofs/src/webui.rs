use crate::config::WebUIConfig;
use crate::fs::errors::FsError;
use crate::fs::inode::{Inode, InodeId};
use crate::fs::permissions::Credentials;
use crate::fs::types::{AuthContext, SetAttributes, SetSize};
use crate::fs::{VerifiedFileContent, VerifiedFileExpectation, VerifiedRenameOutcome, ZeroFS};
use crate::ninep::handler::{NinePHandler, SessionReleaseGuard};
use crate::ninep::lock_manager::FileLockManager;
use crate::ninep::server::{
    InflightRegistry, P9AcceptedWorkTracker, P9GlobalAdmission, P9Response, P9TransportPermit,
    dispatch_9p_frame, response_may_be_emitted, settle_request_tasks,
};
use crate::rpc::proto;
use crate::rpc::server::AdminRpcServer;
use crate::task::spawn_named;
use crate::writeback::reservation::WriteAdmissionHealth;
use crate::writeback::store::WritebackObjectStore;
use axum::Router;
use axum::extract::State;
use axum::extract::ws::{Message as WsMessage, WebSocket, WebSocketUpgrade};
use axum::http::{HeaderValue, StatusCode, header};
use axum::response::IntoResponse;
use axum::routing::{get, put};
use ninep_proto::P9_CHANNEL_SIZE;
use rust_embed::Embed;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tokio_util::task::task_tracker::TaskTrackerToken;
use tokio_util::task::{AbortOnDropHandle, TaskTracker};
use tonic_web::GrpcWebLayer;
use tower_http::cors::{AllowOrigin, CorsLayer};
use tracing::{debug, error, info, warn};

#[derive(Embed)]
#[folder = "../webui/dist"]
struct WebUIAssets;

#[derive(Clone)]
struct AppState {
    filesystem: Arc<ZeroFS>,
    lock_manager: Arc<FileLockManager>,
    uid: u32,
    gid: u32,
    shutdown: CancellationToken,
    ws_drain: TaskTracker,
    accepted_work: P9AcceptedWorkTracker,
    /// Close a session with no inbound message and no completed request for
    /// this long; `None` disables the reaper.
    p9_idle_timeout: Option<std::time::Duration>,
    /// Caps concurrent filesystem writes made by HTTP uploads; request bodies
    /// are streamed before they enter this admission boundary.
    upload_write_permits: Arc<tokio::sync::Semaphore>,
    /// Fail-fast request and retained-byte ownership, independent of active
    /// filesystem write concurrency.
    upload_ingress: Arc<UploadIngressAdmission>,
    writeback: Option<WritebackObjectStore>,
}

fn upload_write_permits() -> Arc<tokio::sync::Semaphore> {
    Arc::new(tokio::sync::Semaphore::new(MAX_CONCURRENT_UPLOAD_WRITES))
}

struct UploadIngressAdmission {
    requests: Arc<tokio::sync::Semaphore>,
    bytes: Arc<tokio::sync::Semaphore>,
}

struct UploadRequestGuard {
    _permit: tokio::sync::OwnedSemaphorePermit,
}

struct UploadBytesGuard {
    _permit: tokio::sync::OwnedSemaphorePermit,
}

impl UploadIngressAdmission {
    fn new(requests: usize, bytes: usize) -> Arc<Self> {
        Arc::new(Self {
            requests: Arc::new(tokio::sync::Semaphore::new(requests)),
            bytes: Arc::new(tokio::sync::Semaphore::new(bytes)),
        })
    }

    fn try_request(self: &Arc<Self>) -> Result<Arc<UploadRequestGuard>, FsError> {
        Arc::clone(&self.requests)
            .try_acquire_owned()
            .map(|permit| Arc::new(UploadRequestGuard { _permit: permit }))
            .map_err(|_| FsError::RetryLater)
    }

    fn try_bytes(self: &Arc<Self>, bytes: usize) -> Result<Arc<UploadBytesGuard>, FsError> {
        let permits = u32::try_from(bytes).map_err(|_| FsError::RetryLater)?;
        Arc::clone(&self.bytes)
            .try_acquire_many_owned(permits)
            .map(|permit| Arc::new(UploadBytesGuard { _permit: permit }))
            .map_err(|_| FsError::RetryLater)
    }
}

fn upload_ingress() -> Arc<UploadIngressAdmission> {
    UploadIngressAdmission::new(MAX_UPLOAD_INGRESS_REQUESTS, MAX_UPLOAD_INGRESS_BYTES)
}

#[cfg(test)]
#[derive(Clone)]
struct CountedTestState {
    app: AppState,
    connections: Arc<std::sync::atomic::AtomicUsize>,
}

#[cfg(test)]
async fn counted_test_ws_upgrade(
    ws: WebSocketUpgrade,
    State(state): State<CountedTestState>,
) -> impl IntoResponse {
    state
        .connections
        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let drain_guard = state.app.ws_drain.token();
    let transport = P9GlobalAdmission::shared()
        .try_admit_transport()
        .expect("test WebSocket transport admission");
    configure_9p_ws(ws)
        .on_upgrade(move |socket| handle_9p_ws(socket, state.app, drain_guard, transport))
}

#[cfg(test)]
pub(crate) fn test_9p_websocket_router(
    filesystem: Arc<ZeroFS>,
    connections: Arc<std::sync::atomic::AtomicUsize>,
) -> Router {
    let state = CountedTestState {
        app: AppState {
            filesystem,
            lock_manager: Arc::new(FileLockManager::new()),
            uid: 0,
            gid: 0,
            shutdown: CancellationToken::new(),
            ws_drain: TaskTracker::new(),
            accepted_work: P9AcceptedWorkTracker::new(),
            p9_idle_timeout: None,
            upload_write_permits: upload_write_permits(),
            upload_ingress: upload_ingress(),
            writeback: None,
        },
        connections,
    };
    Router::new()
        .route("/ws/9p", get(counted_test_ws_upgrade))
        .with_state(state)
}

const WS_DRAIN_TIMEOUT: std::time::Duration = crate::replication::RESPONSE_DRAIN_TIMEOUT;

async fn drain_ws_sessions(ws_drain: TaskTracker) -> std::io::Result<()> {
    ws_drain.close();
    if tokio::time::timeout(WS_DRAIN_TIMEOUT, ws_drain.wait())
        .await
        .is_err()
    {
        tracing::warn!("timed out waiting for 9P WebSocket sessions to drain");
        return Err(std::io::Error::new(
            std::io::ErrorKind::TimedOut,
            "timed out waiting for 9P WebSocket sessions to drain",
        ));
    }
    Ok(())
}

fn configure_9p_ws(ws: WebSocketUpgrade) -> WebSocketUpgrade {
    ws.max_message_size(ninep_proto::P9_MAX_MSIZE as usize)
        .max_frame_size(ninep_proto::P9_MAX_MSIZE as usize)
}

/// WebSocket close code 1013 "Try Again Later" (RFC 6455 / IANA registry).
const WS_CLOSE_TRY_AGAIN_LATER: u16 = 1013;

/// Complete the upgrade only to deliver an explicit close frame. Bridge
/// clients that ignore a non-101 upgrade response would otherwise wait on a
/// connection the server never speaks on; an accepted-then-closed WebSocket
/// surfaces the rejection at the transport level and disconnects promptly.
fn reject_9p_ws(ws: WebSocketUpgrade, reason: String) -> axum::response::Response {
    configure_9p_ws(ws)
        .on_upgrade(move |mut socket| async move {
            let close = WsMessage::Close(Some(axum::extract::ws::CloseFrame {
                code: WS_CLOSE_TRY_AGAIN_LATER,
                reason: reason.into(),
            }));
            let _ = socket.send(close).await;
        })
        .into_response()
}

async fn ws_9p_upgrade(ws: WebSocketUpgrade, State(state): State<AppState>) -> impl IntoResponse {
    // Gate before completing the upgrade so a queued client cannot deliver a
    // full WebSocket frame outside the process receive envelope.
    let transport = match P9GlobalAdmission::shared().try_admit_transport() {
        Ok(transport) => transport,
        Err(error) => {
            error!("9P WebSocket transport admission failed: {error}");
            metrics::counter!("zerofs_p9_ws_admission_rejections_total").increment(1);
            return reject_9p_ws(ws, error.to_string());
        }
    };
    // Register the session before returning the upgrade response.
    let drain_guard = state.ws_drain.token();
    configure_9p_ws(ws)
        .on_upgrade(move |socket| handle_9p_ws(socket, state, drain_guard, transport))
        .into_response()
}

/// Resolves once `last_activity` is at least `idle_timeout` old, re-arming
/// whenever traffic advances the timestamp. Raced inside `select!` against
/// the transport reads it polices.
async fn wait_for_idle_expiry(
    last_activity: &std::sync::Mutex<tokio::time::Instant>,
    idle_timeout: std::time::Duration,
) {
    loop {
        let deadline = *last_activity.lock().unwrap() + idle_timeout;
        if tokio::time::Instant::now() >= deadline {
            return;
        }
        tokio::time::sleep_until(deadline).await;
    }
}

async fn handle_9p_ws(
    socket: WebSocket,
    state: AppState,
    _drain_guard: TaskTrackerToken,
    _transport: P9TransportPermit,
) {
    let response_db = Arc::clone(&state.filesystem.db);
    let handler = Arc::new(
        NinePHandler::new(
            Arc::clone(&state.filesystem),
            Arc::clone(&state.lock_manager),
        )
        .with_credential_override(state.uid, state.gid),
    );
    let mut release_guard = SessionReleaseGuard::new(Arc::clone(&handler));
    let inflight = InflightRegistry::default();
    let admission =
        P9GlobalAdmission::shared().connection_with_accepted_work(state.accepted_work.clone());
    let requests = TaskTracker::new();

    let (tx, mut rx) = mpsc::channel::<P9Response>(P9_CHANNEL_SIZE);

    // A dead peer (VPN restart, killed app) never sends a clean close, so
    // this session would otherwise pin its transport permit forever. Any
    // received WebSocket message or delivered response counts as life; a
    // session quiet on both sides past the configured window is reaped.
    let idle_timeout = state.p9_idle_timeout;
    let last_activity = Arc::new(std::sync::Mutex::new(tokio::time::Instant::now()));
    let mut reaped_idle = false;

    // Writer task: sends response bytes as WS binary messages
    let (mut ws_tx, mut ws_rx) = socket.split();
    let response_authority_lost = CancellationToken::new();
    let writer_authority_lost = response_authority_lost.clone();
    let writer_activity = Arc::clone(&last_activity);

    // Abort on early exit; normal teardown drains responses for a bounded interval.
    let mut writer = AbortOnDropHandle::new(spawn_named("9p-ws-writer", async move {
        use futures::SinkExt;
        while let Some(response) = rx.recv().await {
            let (tag, response_bytes, admission) = response.into_guarded_parts();
            if !response_may_be_emitted(&response_db, &response_bytes) {
                warn!(
                    "Dropping successful WebSocket 9P response for tag {tag} after serving \
                     authority was lost; closing the connection"
                );
                writer_authority_lost.cancel();
                break;
            }
            if ws_tx
                .send(WsMessage::Binary(response_bytes.into()))
                .await
                .is_err()
            {
                break;
            }
            *writer_activity.lock().unwrap() = tokio::time::Instant::now();
            drop(admission);
        }
    }));

    use futures::StreamExt;
    loop {
        let admit = P9GlobalAdmission::shared().admit_websocket_receive(&state.shutdown);
        let receive = tokio::select! {
            biased;
            _ = wait_for_idle_expiry(&last_activity, idle_timeout.unwrap_or_default()),
                if idle_timeout.is_some() =>
            {
                reaped_idle = true;
                break;
            }
            receive = admit => match receive {
                Ok(receive) => receive,
                Err(error) => {
                    debug!("9P WebSocket receive admission ended: {error}");
                    break;
                }
            },
        };
        let next = tokio::select! {
            biased;
            _ = state.shutdown.cancelled() => {
                debug!("9P WebSocket handler shutting down");
                break;
            }
            _ = response_authority_lost.cancelled() => {
                debug!("9P WebSocket handler closing after serving authority loss");
                break;
            }
            _ = wait_for_idle_expiry(&last_activity, idle_timeout.unwrap_or_default()),
                if idle_timeout.is_some() =>
            {
                reaped_idle = true;
                break;
            }
            next = ws_rx.next() => next,
        };
        if matches!(next, Some(Ok(_))) {
            *last_activity.lock().unwrap() = tokio::time::Instant::now();
        }
        match next {
            Some(Ok(WsMessage::Binary(data))) => {
                if let Err(e) = dispatch_9p_frame(
                    data,
                    &handler,
                    &tx,
                    &inflight,
                    &admission,
                    &requests,
                    &state.shutdown,
                    &state.shutdown,
                )
                .await
                {
                    error!("9P WebSocket dispatch error: {}", e);
                    break;
                }
            }
            Some(Ok(WsMessage::Close(_))) | None => {
                debug!("9P WebSocket client disconnected");
                break;
            }
            Some(Err(e)) => {
                debug!("9P WebSocket read error: {}", e);
                break;
            }
            _ => {} // ping/pong/text ignored
        }
        drop(receive);
    }

    if reaped_idle {
        metrics::counter!("zerofs_p9_ws_idle_sessions_reaped_total").increment(1);
        warn!(
            idle_secs = idle_timeout.unwrap_or_default().as_secs(),
            "closing 9P WebSocket session with no inbound message or completed \
             request inside the idle window; releasing its transport permit"
        );
    }

    // Disconnect before awaiting request tasks to block late resource installs.
    release_guard.release();
    drop(tx);

    // `SinkExt::send` flushes queued CLEAN responses to the WebSocket transport.
    // Bound the drain for unrelated stalled handlers.
    if tokio::time::timeout(WS_DRAIN_TIMEOUT, &mut writer)
        .await
        .is_err()
    {
        metrics::counter!(
            "zerofs_p9_response_drain_timeouts_total",
            "transport" => "websocket"
        )
        .increment(1);
        writer.abort();
        let _ = writer.await;
        tracing::warn!("timed out draining 9P WebSocket responses during shutdown");
    }
    settle_request_tasks(requests).await;
}

// === HTTP upload API ========================================================
//
// Plain-HTTP file ingestion for clients that cannot hold a 9P WebSocket open
// (an iOS background `URLSession` uploads one part per HTTP request and may be
// suspended between parts). Trust model matches 9P over WebSocket: no auth,
// the port is only reachable over a private tunnel/LAN, and requests act with
// the configured WebUI uid/gid exactly like `with_credential_override` above.
//
// Contract (all paths are absolute filesystem paths, `/`-separated, no `.`
// or `..` components):
//
//   PUT  /api/v1/upload/{path}?offset=N     binary body, one part (up to 16 MiB)
//   POST /api/v1/upload/{path}/commit       {"size": N, "sha256": "hex",
//                                            "publish_to": "path"?}
//   GET  /api/v1/upload/{path}/status       {"size": N}
//
// Resume contract: parts are sequential appends. A part whose offset is past
// the current file size would leave a hole, so it is rejected with 409 and
// the current size; a part at or before the current size is accepted (a
// retransmit whose acknowledgement was lost rewrites the same bytes). Under
// that rule the size reported by `status` is always the length of the
// contiguously-written prefix, so an interrupted client resumes from exactly
// `size` without re-reading anything.
//
// `commit` is the durability and integrity barrier: the server drains its own
// RAM-acked writes for the inode and waits for the filesystem's configured
// durability target (the same barrier 9P `Tfsync` uses), then re-reads and
// hashes the bytes it holds server-side — never trusting a client readback —
// and compares size and SHA-256. With `publish_to` the verified file is then
// atomically renamed over the destination via the filesystem's transactional
// `rename` (the fs layer's atomic-promote primitive); without it the file is
// verified in place.

/// Largest accepted upload part body. Clients send parts up to 16 MiB;
/// anything larger is refused with 413 while it is streamed.
const MAX_UPLOAD_PART_BYTES: usize = 16 * 1024 * 1024;

/// Upload handlers admitted before body polling or filesystem path creation.
/// Admission is fail-fast so excess connections do not form a waiter queue.
const MAX_UPLOAD_INGRESS_REQUESTS: usize = 32;

/// Concurrent filesystem writes made by HTTP uploads. A slow or abandoned
/// request body must not consume one of these permits; admission happens only
/// around each bounded `write_ack` call.
const MAX_CONCURRENT_UPLOAD_WRITES: usize = 16;

/// Maximum body data copied into one filesystem write. This bounds the extra
/// per-request buffer without tying write admission to network read latency.
const UPLOAD_STREAM_WRITE_CHUNK_BYTES: usize = 4 * 1024 * 1024;

/// An accepted Body frame can retain a full part-sized backing allocation
/// while a copied write chunk is live. Preserve sixteen active writers while
/// charging that conservative 20 MiB per-frame peak.
const UPLOAD_FRAME_AND_COPY_BYTES: usize = MAX_UPLOAD_PART_BYTES + UPLOAD_STREAM_WRITE_CHUNK_BYTES;
const MAX_UPLOAD_INGRESS_BYTES: usize =
    (MAX_CONCURRENT_UPLOAD_WRITES + 1) * UPLOAD_FRAME_AND_COPY_BYTES;

const UPLOAD_BODY_IDLE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);
const MAX_UPLOAD_COMMIT_BODY_BYTES: usize = 1024 * 1024;
/// Commit JSON frames, their contiguous parse copy, worst-case Vec<String>
/// headers and owned string allocations, and one assembly/hash chunk share
/// this conservative fixed reservation before JSON polling begins.
const UPLOAD_COMMIT_WORKSPACE_BYTES: usize = 32 * 1024 * 1024;

/// Read granularity while re-hashing a committed file server-side.
const UPLOAD_COMMIT_HASH_CHUNK: u32 = 1024 * 1024;

#[derive(serde::Deserialize)]
struct UploadPartQuery {
    #[serde(default)]
    offset: u64,
}

#[derive(serde::Deserialize)]
struct UploadCommitRequest {
    size: u64,
    sha256: String,
    publish_to: Option<String>,
    /// Staging segments to concatenate into `{path}`, in order, before
    /// verification. Lets a client upload one file over several parallel
    /// connections: each segment is written contiguously to its own staging
    /// file, so the "size == contiguously-written prefix" resume contract
    /// still holds per segment and survives a restart. Absent for the
    /// single-stream path, which is unchanged.
    #[serde(default)]
    assemble_from: Vec<String>,
}

fn upload_router() -> Router<AppState> {
    Router::new().route(
        "/api/v1/upload/{*path}",
        put(upload_part).post(upload_commit).get(upload_status),
    )
}

fn upload_auth(state: &AppState) -> AuthContext {
    AuthContext {
        uid: state.uid,
        gid: state.gid,
        gid_known: true,
        gids: Vec::new(),
        groups_complete: true,
    }
}

fn upload_json(status: StatusCode, body: serde_json::Value) -> axum::response::Response {
    (status, axum::Json(body)).into_response()
}

fn upload_error(status: StatusCode, message: &str) -> axum::response::Response {
    upload_json(status, serde_json::json!({ "error": message }))
}

async fn upload_writeback_preflight(
    writeback: Option<&WritebackObjectStore>,
) -> Result<(), FsError> {
    let Some(writeback) = writeback else {
        return Ok(());
    };
    match writeback.write_admission_health().await {
        Ok(WriteAdmissionHealth::Ready) => Ok(()),
        Ok(WriteAdmissionHealth::Pressured) => Err(FsError::RetryLater),
        Err(error) => {
            warn!(error = %error, "HTTP upload writeback preflight failed");
            Err(FsError::IoError)
        }
    }
}

fn upload_fs_error(error: FsError) -> axum::response::Response {
    let status = match error {
        FsError::NotFound | FsError::StaleHandle => StatusCode::NOT_FOUND,
        FsError::PermissionDenied
        | FsError::OperationNotPermitted
        | FsError::ReadOnlyFilesystem => StatusCode::FORBIDDEN,
        FsError::Exists => StatusCode::CONFLICT,
        FsError::IsDirectory
        | FsError::NotDirectory
        | FsError::InvalidArgument
        | FsError::InvalidData
        | FsError::NameTooLong => StatusCode::BAD_REQUEST,
        FsError::NoSpace => StatusCode::INSUFFICIENT_STORAGE,
        FsError::RetryLater => StatusCode::SERVICE_UNAVAILABLE,
        _ => StatusCode::INTERNAL_SERVER_ERROR,
    };
    let mut response = upload_json(status, serde_json::json!({ "error": error.to_string() }));
    if error == FsError::RetryLater {
        response
            .headers_mut()
            .insert(header::RETRY_AFTER, HeaderValue::from_static("1"));
    }
    response
}

/// Split a wildcard-captured upload path into name components. The fs treats
/// names as opaque bytes, so traversal must be rejected at this boundary.
fn upload_path_components(raw: &str) -> Result<Vec<Vec<u8>>, &'static str> {
    let components: Vec<Vec<u8>> = raw
        .split('/')
        .map(|component| component.as_bytes().to_vec())
        .collect();
    if components
        .iter()
        .any(|c| c.is_empty() || c == b"." || c == b"..")
    {
        return Err("path must not contain empty, '.', or '..' components");
    }
    if components.is_empty() {
        return Err("empty upload path");
    }
    Ok(components)
}

/// Walk `dirs` from the root, creating missing directories. A concurrent
/// create of the same component is folded into a lookup of the winner.
async fn upload_resolve_dir_creating(
    fs: &ZeroFS,
    creds: &Credentials,
    dirs: &[Vec<u8>],
) -> Result<InodeId, FsError> {
    let mut dir: InodeId = 0;
    for name in dirs {
        dir = match fs.lookup(creds, dir, name).await {
            Ok(id) => id,
            Err(FsError::NotFound) => {
                match fs.mkdir(creds, dir, name, &SetAttributes::default()).await {
                    Ok((id, _)) => id,
                    Err(FsError::Exists) => fs.lookup(creds, dir, name).await?,
                    Err(error) => return Err(error),
                }
            }
            Err(error) => return Err(error),
        };
    }
    Ok(dir)
}

/// Resolve an existing path to `(parent_dir, file_inode)` without creating
/// anything.
async fn upload_resolve_existing(
    fs: &ZeroFS,
    creds: &Credentials,
    components: &[Vec<u8>],
) -> Result<(InodeId, InodeId), FsError> {
    upload_resolve_existing_entry(fs, creds, components)
        .await
        .map(|(dir, file, _)| (dir, file))
}

/// Resolve an existing path and retain the namespace cookie that distinguishes
/// remove/recreate and same-inode hard-link replacement.
async fn upload_resolve_existing_entry(
    fs: &ZeroFS,
    creds: &Credentials,
    components: &[Vec<u8>],
) -> Result<(InodeId, InodeId, u64), FsError> {
    let (dirs, name) = components.split_at(components.len() - 1);
    let mut dir: InodeId = 0;
    for component in dirs {
        dir = fs.lookup(creds, dir, component).await?;
    }
    let (file, cookie) = fs.entry_identity(dir, &name[0]).await?;
    Ok((dir, file, cookie))
}

/// Overlay-visible size of a regular file (RAM-acked writes included), so an
/// interrupted client resumes from every byte the server has accepted.
async fn upload_visible_file_size(fs: &ZeroFS, id: InodeId) -> Result<u64, FsError> {
    match fs.visible_inode(id).await? {
        Inode::File(file) => Ok(file.size),
        Inode::Directory(_) => Err(FsError::IsDirectory),
        _ => Err(FsError::InvalidArgument),
    }
}

fn upload_admit_request(
    state: &AppState,
) -> Result<Arc<UploadRequestGuard>, axum::response::Response> {
    if state.shutdown.is_cancelled() {
        return Err(upload_fs_error(FsError::RetryLater));
    }
    state.upload_ingress.try_request().map_err(upload_fs_error)
}

async fn upload_next_body_data(
    state: &AppState,
    body: &mut axum::body::Body,
) -> Result<Option<bytes::Bytes>, axum::response::Response> {
    use http_body_util::BodyExt as _;

    let deadline = tokio::time::Instant::now() + UPLOAD_BODY_IDLE_TIMEOUT;
    loop {
        let frame = tokio::select! {
            biased;
            _ = state.shutdown.cancelled() => {
                return Err(upload_fs_error(FsError::RetryLater));
            }
            result = tokio::time::timeout_at(deadline, body.frame()) => {
                match result {
                    Ok(frame) => frame,
                    Err(_) => return Err(upload_error(StatusCode::REQUEST_TIMEOUT, "upload body idle timeout")),
                }
            }
        };
        let Some(frame) = frame else {
            return Ok(None);
        };
        let frame = frame
            .map_err(|_| upload_error(StatusCode::BAD_REQUEST, "failed to read request body"))?;
        if let Ok(data) = frame.into_data()
            && !data.is_empty()
        {
            return Ok(Some(data));
        }
    }
}

async fn upload_write_admitted(
    state: &AppState,
    request_guard: Arc<UploadRequestGuard>,
    auth: &AuthContext,
    file: InodeId,
    offset: u64,
    data: bytes::Bytes,
    bytes_guard: Arc<UploadBytesGuard>,
) -> Result<u64, FsError> {
    let write_permit = tokio::select! {
        biased;
        _ = state.shutdown.cancelled() => return Err(FsError::RetryLater),
        permit = Arc::clone(&state.upload_write_permits).acquire_owned() => {
            permit.map_err(|_| FsError::IoError)?
        }
    };
    let accepted_guard = state
        .accepted_work
        .try_accept()
        .ok_or(FsError::RetryLater)?;
    let filesystem = Arc::clone(&state.filesystem);
    let auth = auth.clone();
    tokio::spawn(async move {
        let _request_guard = request_guard;
        let _bytes_guard = bytes_guard;
        let _write_permit = write_permit;
        let _accepted_guard = accepted_guard;
        // Same RAM-ack seam 9P Twrite lands on (`write_ack` forwards to
        // `write_ack_identified`, the path `NinePHandler::write` uses).
        filesystem
            .write_ack(&auth, file, offset, &data)
            .await
            .map(|attrs| attrs.size)
    })
    .await
    .map_err(|_| FsError::IoError)?
}

async fn upload_part(
    State(state): State<AppState>,
    axum::extract::Path(path): axum::extract::Path<String>,
    axum::extract::Query(query): axum::extract::Query<UploadPartQuery>,
    mut body: axum::body::Body,
) -> axum::response::Response {
    let request_guard = match upload_admit_request(&state) {
        Ok(guard) => guard,
        Err(response) => return response,
    };
    if let Err(error) = upload_writeback_preflight(state.writeback.as_ref()).await {
        return upload_fs_error(error);
    }

    let components = match upload_path_components(&path) {
        Ok(components) => components,
        Err(message) => return upload_error(StatusCode::BAD_REQUEST, message),
    };

    // Poll and own the first retained Body frame before creating directories
    // or a staging inode. The fixed frame charge covers its backing allocation
    // even when a Bytes view is shorter than that allocation.
    let first_frame_guard = match state.upload_ingress.try_bytes(MAX_UPLOAD_PART_BYTES) {
        Ok(guard) => guard,
        Err(error) => return upload_fs_error(error),
    };
    let mut next_frame = match upload_next_body_data(&state, &mut body).await {
        Ok(Some(data)) => {
            if data.len() > MAX_UPLOAD_PART_BYTES {
                return upload_error(
                    StatusCode::PAYLOAD_TOO_LARGE,
                    "part body exceeds the 16 MiB part limit",
                );
            }
            Some((data, first_frame_guard))
        }
        Ok(None) => {
            drop(first_frame_guard);
            None
        }
        Err(response) => return response,
    };

    let auth = upload_auth(&state);
    let creds = Credentials::from_auth_context(&auth);
    let (dirs, name) = components.split_at(components.len() - 1);
    let parent = match upload_resolve_dir_creating(&state.filesystem, &creds, dirs).await {
        Ok(parent) => parent,
        Err(error) => return upload_fs_error(error),
    };
    let file = match state.filesystem.lookup(&creds, parent, &name[0]).await {
        Ok(id) => id,
        Err(FsError::NotFound) => {
            if query.offset != 0 {
                // No file yet: any nonzero offset is a gap.
                metrics::counter!("zerofs_http_upload_offset_conflicts_total").increment(1);
                return upload_json(StatusCode::CONFLICT, serde_json::json!({ "size": 0 }));
            }
            match state
                .filesystem
                .create(&creds, parent, &name[0], &SetAttributes::default())
                .await
            {
                Ok((id, _)) => id,
                // Lost a create race with a concurrent part of the same file.
                Err(FsError::Exists) => {
                    match state.filesystem.lookup(&creds, parent, &name[0]).await {
                        Ok(id) => id,
                        Err(error) => return upload_fs_error(error),
                    }
                }
                Err(error) => return upload_fs_error(error),
            }
        }
        Err(error) => return upload_fs_error(error),
    };

    let current_size = match upload_visible_file_size(&state.filesystem, file).await {
        Ok(size) => size,
        Err(error) => return upload_fs_error(error),
    };
    if query.offset > current_size {
        // A hole would break the "size == contiguously-written prefix"
        // resume contract, so the client must re-check status and back up.
        metrics::counter!("zerofs_http_upload_offset_conflicts_total").increment(1);
        return upload_json(
            StatusCode::CONFLICT,
            serde_json::json!({ "size": current_size }),
        );
    }

    let mut received = 0usize;
    let mut write_offset = query.offset;
    let mut final_size = current_size;
    let mut buffered = bytes::BytesMut::new();
    let mut buffer_guard: Option<Arc<UploadBytesGuard>> = None;

    while let Some((data, frame_guard)) = next_frame {
        let Some(next_received) = received.checked_add(data.len()) else {
            return upload_error(StatusCode::PAYLOAD_TOO_LARGE, "part body is too large");
        };
        if next_received > MAX_UPLOAD_PART_BYTES {
            return upload_error(
                StatusCode::PAYLOAD_TOO_LARGE,
                "part body exceeds the 16 MiB part limit",
            );
        }
        received = next_received;

        let mut remaining = data.as_ref();
        while !remaining.is_empty() {
            if buffer_guard.is_none() {
                buffer_guard = match state
                    .upload_ingress
                    .try_bytes(UPLOAD_STREAM_WRITE_CHUNK_BYTES)
                {
                    Ok(guard) => Some(guard),
                    Err(error) => return upload_fs_error(error),
                };
                buffered = bytes::BytesMut::with_capacity(UPLOAD_STREAM_WRITE_CHUNK_BYTES);
            }
            let take = remaining
                .len()
                .min(UPLOAD_STREAM_WRITE_CHUNK_BYTES - buffered.len());
            buffered.extend_from_slice(&remaining[..take]);
            remaining = &remaining[take..];
            if buffered.len() == UPLOAD_STREAM_WRITE_CHUNK_BYTES {
                let Some(next_offset) = write_offset.checked_add(buffered.len() as u64) else {
                    return upload_error(StatusCode::BAD_REQUEST, "offset + length overflows");
                };
                let write_data = std::mem::take(&mut buffered).freeze();
                let copy_guard = buffer_guard.take().expect("non-empty buffer is charged");
                final_size = match upload_write_admitted(
                    &state,
                    Arc::clone(&request_guard),
                    &auth,
                    file,
                    write_offset,
                    write_data,
                    copy_guard,
                )
                .await
                {
                    Ok(size) => size,
                    Err(error) => return upload_fs_error(error),
                };
                write_offset = next_offset;
            }
        }
        drop(data);
        drop(frame_guard);

        let frame_guard = match state.upload_ingress.try_bytes(MAX_UPLOAD_PART_BYTES) {
            Ok(guard) => guard,
            Err(error) => return upload_fs_error(error),
        };
        next_frame = match upload_next_body_data(&state, &mut body).await {
            Ok(Some(data)) => Some((data, frame_guard)),
            Ok(None) => {
                drop(frame_guard);
                None
            }
            Err(response) => return response,
        };
    }

    if !buffered.is_empty() {
        if write_offset.checked_add(buffered.len() as u64).is_none() {
            return upload_error(StatusCode::BAD_REQUEST, "offset + length overflows");
        }
        let write_data = buffered.freeze();
        let copy_guard = buffer_guard.take().expect("non-empty buffer is charged");
        final_size = match upload_write_admitted(
            &state,
            Arc::clone(&request_guard),
            &auth,
            file,
            write_offset,
            write_data,
            copy_guard,
        )
        .await
        {
            Ok(size) => size,
            Err(error) => return upload_fs_error(error),
        };
    }
    metrics::counter!("zerofs_http_upload_parts_total").increment(1);
    metrics::counter!("zerofs_http_upload_bytes_total").increment(received as u64);
    upload_json(StatusCode::OK, serde_json::json!({ "size": final_size }))
}

async fn upload_status(
    State(state): State<AppState>,
    axum::extract::Path(path): axum::extract::Path<String>,
) -> axum::response::Response {
    let _request_guard = match upload_admit_request(&state) {
        Ok(guard) => guard,
        Err(response) => return response,
    };
    let Some(path) = path.strip_suffix("/status") else {
        return upload_error(
            StatusCode::NOT_FOUND,
            "expected /api/v1/upload/{path}/status",
        );
    };
    let components = match upload_path_components(path) {
        Ok(components) => components,
        Err(message) => return upload_error(StatusCode::BAD_REQUEST, message),
    };
    let auth = upload_auth(&state);
    let creds = Credentials::from_auth_context(&auth);
    let file = match upload_resolve_existing(&state.filesystem, &creds, &components).await {
        Ok((_, file)) => file,
        Err(error) => return upload_fs_error(error),
    };
    match upload_visible_file_size(&state.filesystem, file).await {
        Ok(size) => upload_json(StatusCode::OK, serde_json::json!({ "size": size })),
        Err(error) => upload_fs_error(error),
    }
}

fn upload_parse_sha256(value: &str) -> Option<[u8; 32]> {
    if value.len() != 64 {
        return None;
    }
    let mut digest = [0u8; 32];
    for (index, pair) in value.as_bytes().chunks_exact(2).enumerate() {
        let pair = std::str::from_utf8(pair).ok()?;
        digest[index] = u8::from_str_radix(pair, 16).ok()?;
    }
    Some(digest)
}

fn upload_sha256_hex(digest: [u8; 32]) -> String {
    use std::fmt::Write as _;
    let mut hex = String::with_capacity(64);
    for byte in digest {
        write!(hex, "{byte:02x}").expect("writing to a String cannot fail");
    }
    hex
}

fn upload_verification_failed(
    path: &str,
    expected_size: u64,
    expected_sha256: &str,
    actual: VerifiedFileContent,
) -> axum::response::Response {
    let actual_sha256 = upload_sha256_hex(actual.sha256);
    metrics::counter!("zerofs_http_upload_verify_failures_total").increment(1);
    warn!(
        path,
        expected_size,
        actual_size = actual.size,
        expected_sha256,
        actual_sha256 = %actual_sha256,
        "HTTP upload commit verification failed"
    );
    upload_json(
        StatusCode::UNPROCESSABLE_ENTITY,
        serde_json::json!({
            "verified": false,
            "size": actual.size,
            "sha256": actual_sha256,
        }),
    )
}

fn upload_commit_fs_error(error: FsError) -> axum::response::Response {
    if error == FsError::StaleHandle {
        upload_error(StatusCode::CONFLICT, "upload source changed during commit")
    } else {
        upload_fs_error(error)
    }
}

/// Concatenate `segments` into a freshly truncated file at `components`, in
/// order. Every source is resolved before the destination is modified, and
/// sources remain available until the caller verifies and publishes the
/// result so a failed commit can be retried.
async fn upload_assemble_segments(
    state: &AppState,
    request_guard: &Arc<UploadRequestGuard>,
    workspace_guard: &Arc<UploadBytesGuard>,
    auth: &AuthContext,
    creds: &Credentials,
    components: &[Vec<u8>],
    segments: &[String],
) -> Result<Vec<(InodeId, InodeId, u64, Vec<u8>)>, FsError> {
    let fs = &state.filesystem;
    let (dirs, name) = components.split_at(components.len() - 1);
    let parent = upload_resolve_dir_creating(fs, creds, dirs).await?;

    let mut resolved_segments = Vec::with_capacity(segments.len());
    for segment in segments {
        let segment_components = upload_path_components(segment.trim_start_matches('/'))
            .map_err(|_| FsError::InvalidArgument)?;
        let (segment_dir, segment_file, segment_cookie) =
            upload_resolve_existing_entry(fs, creds, &segment_components).await?;
        let segment_name = segment_components[segment_components.len() - 1].clone();
        resolved_segments.push((segment_dir, segment_file, segment_cookie, segment_name));
    }

    let target = match fs.lookup(creds, parent, &name[0]).await {
        Ok(id) => id,
        Err(FsError::NotFound) => match fs
            .create(creds, parent, &name[0], &SetAttributes::default())
            .await
        {
            Ok((id, _)) => id,
            Err(FsError::Exists) => fs.lookup(creds, parent, &name[0]).await?,
            Err(error) => return Err(error),
        },
        Err(error) => return Err(error),
    };
    if resolved_segments
        .iter()
        .any(|(_, segment_file, _, _)| *segment_file == target)
    {
        return Err(FsError::InvalidArgument);
    }
    fs.setattr(
        creds,
        target,
        &SetAttributes {
            size: SetSize::Set(0),
            ..SetAttributes::default()
        },
    )
    .await?;

    let mut offset: u64 = 0;
    for (_, segment_file, _, _) in &resolved_segments {
        let mut read_at: u64 = 0;
        loop {
            let (chunk, eof) = fs
                .read_file(auth, *segment_file, read_at, UPLOAD_COMMIT_HASH_CHUNK)
                .await?;
            if !chunk.is_empty() {
                let chunk_len = chunk.len() as u64;
                upload_write_admitted(
                    state,
                    Arc::clone(request_guard),
                    auth,
                    target,
                    offset,
                    chunk,
                    Arc::clone(workspace_guard),
                )
                .await?;
                read_at += chunk_len;
                offset += chunk_len;
            }
            if eof {
                break;
            }
        }
    }
    Ok(resolved_segments)
}

async fn upload_read_commit_request(
    state: &AppState,
    body: &mut axum::body::Body,
) -> Result<UploadCommitRequest, axum::response::Response> {
    let mut json = Vec::with_capacity(MAX_UPLOAD_COMMIT_BODY_BYTES);
    let mut received = 0usize;
    while let Some(data) = upload_next_body_data(state, body).await? {
        received = received.checked_add(data.len()).ok_or_else(|| {
            upload_error(StatusCode::PAYLOAD_TOO_LARGE, "commit body is too large")
        })?;
        if received > MAX_UPLOAD_COMMIT_BODY_BYTES {
            return Err(upload_error(
                StatusCode::PAYLOAD_TOO_LARGE,
                "commit body is too large",
            ));
        }
        json.extend_from_slice(&data);
        drop(data);
    }
    serde_json::from_slice(&json)
        .map_err(|_| upload_error(StatusCode::BAD_REQUEST, "invalid commit request"))
}

async fn upload_commit(
    State(state): State<AppState>,
    axum::extract::Path(path): axum::extract::Path<String>,
    mut body: axum::body::Body,
) -> axum::response::Response {
    let request_guard = match upload_admit_request(&state) {
        Ok(guard) => guard,
        Err(response) => return response,
    };
    let workspace_guard = match state
        .upload_ingress
        .try_bytes(UPLOAD_COMMIT_WORKSPACE_BYTES)
    {
        Ok(guard) => guard,
        Err(error) => return upload_fs_error(error),
    };
    if let Err(error) = upload_writeback_preflight(state.writeback.as_ref()).await {
        return upload_fs_error(error);
    }

    let request = match upload_read_commit_request(&state, &mut body).await {
        Ok(request) => request,
        Err(response) => return response,
    };

    let Some(path) = path.strip_suffix("/commit") else {
        return upload_error(
            StatusCode::NOT_FOUND,
            "expected /api/v1/upload/{path}/commit",
        );
    };
    let components = match upload_path_components(path) {
        Ok(components) => components,
        Err(message) => return upload_error(StatusCode::BAD_REQUEST, message),
    };
    let auth = upload_auth(&state);
    let creds = Credentials::from_auth_context(&auth);
    let assembled_segments = if request.assemble_from.is_empty() {
        Vec::new()
    } else {
        match upload_assemble_segments(
            &state,
            &request_guard,
            &workspace_guard,
            &auth,
            &creds,
            &components,
            &request.assemble_from,
        )
        .await
        {
            Ok(segments) => segments,
            Err(error) => return upload_fs_error(error),
        }
    };
    let (staging_dir, file, staging_cookie) =
        match upload_resolve_existing_entry(&state.filesystem, &creds, &components).await {
            Ok(resolved) => resolved,
            Err(error) => return upload_fs_error(error),
        };

    // Durability first: drain this inode's RAM-acked overlay writes and wait
    // for the configured durability target — the barrier 9P Tfsync takes —
    // so a positive verify never describes bytes that can still be lost.
    if let Err(error) = state.filesystem.wait_inode_durability(file).await {
        return upload_fs_error(error);
    }

    let expected = VerifiedFileExpectation {
        inode: file,
        entry_cookie: staging_cookie,
        size: request.size,
        sha256: upload_parse_sha256(&request.sha256),
    };
    let staging_name = &components[components.len() - 1];
    let mut final_path = format!("/{path}");
    let (publish_dir, publish_name) = if let Some(publish_to) = &request.publish_to {
        let publish_components = match upload_path_components(publish_to.trim_start_matches('/')) {
            Ok(components) => components,
            Err(message) => return upload_error(StatusCode::BAD_REQUEST, message),
        };
        let (publish_dirs, publish_name) =
            publish_components.split_at(publish_components.len() - 1);
        let publish_dir =
            match upload_resolve_dir_creating(&state.filesystem, &creds, publish_dirs).await {
                Ok(dir) => dir,
                Err(error) => return upload_fs_error(error),
            };
        final_path = format!("/{}", publish_to.trim_start_matches('/'));
        (publish_dir, publish_name[0].clone())
    } else {
        (staging_dir, staging_name.clone())
    };

    // Hash exactly once while the fs layer's ordinary rename fence and
    // ordered locks remain held through either publication or the same-path
    // verification-only linearization point.
    let verified = match state
        .filesystem
        .rename_verified(
            &auth,
            staging_dir,
            staging_name,
            publish_dir,
            &publish_name,
            expected,
        )
        .await
    {
        Ok(VerifiedRenameOutcome::Published(content)) => content,
        Ok(VerifiedRenameOutcome::Mismatch(actual)) => {
            return upload_verification_failed(path, request.size, &request.sha256, actual);
        }
        Err(error) => return upload_commit_fs_error(error),
    };

    // Cover writes accepted after the first barrier but before the metadata
    // fence closed, plus the rename transaction itself, at the configured
    // durability target before returning success.
    if let Err(error) = state.filesystem.wait_inode_durability(file).await {
        return upload_fs_error(error);
    }

    for (segment_dir, segment_file, segment_cookie, segment_name) in assembled_segments {
        // The committed target is already verified and durable. A failed
        // cleanup only leaves an independently named staging object behind.
        // The compare and unlink share the normal unlink fence, ordered locks,
        // and transaction. A recreated or same-inode/new-cookie entry wins.
        let _ = state
            .filesystem
            .remove_if_entry_matches(
                &auth,
                segment_dir,
                &segment_name,
                segment_file,
                segment_cookie,
            )
            .await;
    }

    metrics::counter!("zerofs_http_upload_commits_total").increment(1);
    info!(
        path = %final_path,
        size = verified.size,
        sha256 = %upload_sha256_hex(verified.sha256),
        "HTTP upload committed"
    );
    upload_json(
        StatusCode::OK,
        serde_json::json!({
            "verified": true,
            "size": verified.size,
            "sha256": upload_sha256_hex(verified.sha256),
            "path": final_path,
        }),
    )
}

fn content_type(path: &str) -> &'static str {
    match path.rsplit('.').next() {
        Some("html") => "text/html; charset=utf-8",
        Some("js") => "application/javascript; charset=utf-8",
        Some("css") => "text/css; charset=utf-8",
        Some("json") => "application/json",
        Some("wasm") => "application/wasm",
        Some("svg") => "image/svg+xml",
        Some("png") => "image/png",
        Some("ico") => "image/x-icon",
        Some("woff2") => "font/woff2",
        Some("woff") => "font/woff",
        _ => "application/octet-stream",
    }
}

fn cache_control(path: &str) -> &'static str {
    if path.starts_with("assets/") {
        "public, max-age=31536000, immutable"
    } else {
        "no-cache"
    }
}

async fn serve_spa(axum::extract::Path(path): axum::extract::Path<String>) -> impl IntoResponse {
    serve_asset(&path)
}

async fn serve_index() -> impl IntoResponse {
    serve_asset("index.html")
}

fn serve_asset(path: &str) -> axum::response::Response {
    use axum::http::{StatusCode, header};
    use axum::response::Response;

    if let Some(file) = WebUIAssets::get(path) {
        Response::builder()
            .header(header::CONTENT_TYPE, content_type(path))
            .header(header::CACHE_CONTROL, cache_control(path))
            .body(axum::body::Body::from(file.data.to_vec()))
            .unwrap()
    } else if let Some(index) = WebUIAssets::get("index.html") {
        Response::builder()
            .header(header::CONTENT_TYPE, "text/html; charset=utf-8")
            .body(axum::body::Body::from(index.data.to_vec()))
            .unwrap()
    } else {
        Response::builder()
            .status(StatusCode::NOT_FOUND)
            .body(axum::body::Body::from("Web UI not available"))
            .unwrap()
    }
}

pub fn start(
    config: &WebUIConfig,
    filesystem: Arc<ZeroFS>,
    lock_manager: Arc<FileLockManager>,
    rpc_service: AdminRpcServer,
    shutdown: CancellationToken,
    accepted_work: P9AcceptedWorkTracker,
    writeback: Option<WritebackObjectStore>,
) -> Vec<JoinHandle<Result<(), std::io::Error>>> {
    let ws_drain = TaskTracker::new();
    let state = AppState {
        filesystem,
        lock_manager,
        uid: config.uid,
        gid: config.gid,
        shutdown: shutdown.clone(),
        ws_drain: ws_drain.clone(),
        accepted_work,
        p9_idle_timeout: (config.p9_idle_timeout_secs != 0)
            .then(|| std::time::Duration::from_secs(config.p9_idle_timeout_secs)),
        upload_write_permits: upload_write_permits(),
        upload_ingress: upload_ingress(),
        writeback,
    };

    // gRPC-web: wrap tonic service with GrpcWebService + CORS
    let grpc_service = proto::admin_service_server::AdminServiceServer::new(rpc_service);
    let grpc_web_service = tower::ServiceBuilder::new()
        .layer(
            CorsLayer::new()
                .allow_origin(AllowOrigin::mirror_request())
                .allow_headers(tower_http::cors::Any)
                .expose_headers(tower_http::cors::Any),
        )
        .layer(GrpcWebLayer::new())
        .service(grpc_service);

    let app = Router::new()
        // 9P over WebSocket
        .route("/ws/9p", get(ws_9p_upgrade))
        // Plain-HTTP upload API (background URLSession clients)
        .merge(upload_router())
        // gRPC-web
        .route_service("/zerofs.admin.AdminService/{method}", grpc_web_service)
        // Static assets and SPA fallback
        .route("/{*path}", get(serve_spa))
        .route("/", get(serve_index))
        .with_state(state);

    let mut handles = Vec::new();
    for &addr in &config.addresses {
        let app = app.clone();
        let shutdown = shutdown.clone();
        let ws_drain = ws_drain.clone();
        handles.push(spawn_named("webui-http", async move {
            let listener = match tokio::net::TcpListener::bind(addr).await {
                Ok(l) => l,
                Err(e) => {
                    tracing::error!("Failed to bind Web UI server to {}: {}", addr, e);
                    return Ok(());
                }
            };
            info!("Web UI server listening on http://{}", addr);
            let result = axum::serve(listener, app)
                .with_graceful_shutdown(shutdown.cancelled_owned())
                .await
                .map_err(|e| std::io::Error::other(e.to_string()));
            drain_ws_sessions(ws_drain).await?;
            result
        }));
    }
    handles
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::writeback::config::{AckMode, ShutdownFlush, WritebackSettings};
    use crate::writeback::journal::Journal;
    use crate::writeback::model::JournalIdentity;
    use crate::writeback::store::WritebackObjectStore;
    use axum::http::StatusCode;
    use axum::routing::post;
    use object_store::memory::InMemory;
    use std::process::Stdio;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    #[tokio::test(start_paused = true)]
    async fn websocket_session_drain_timeout_is_listener_failure() {
        let sessions = TaskTracker::new();
        let _active_session = sessions.token();
        let drain = tokio::spawn(drain_ws_sessions(sessions));
        tokio::task::yield_now().await;
        tokio::time::advance(WS_DRAIN_TIMEOUT + std::time::Duration::from_millis(1)).await;
        let error = drain.await.unwrap().unwrap_err();
        assert_eq!(error.kind(), std::io::ErrorKind::TimedOut);
    }

    #[derive(Clone)]
    struct SmokeState {
        app: AppState,
        connections: Arc<std::sync::Mutex<CancellationToken>>,
        connection_closed: Arc<tokio::sync::Notify>,
    }

    async fn smoke_ws_upgrade(
        ws: WebSocketUpgrade,
        State(state): State<SmokeState>,
    ) -> impl IntoResponse {
        let connection_shutdown = state.connections.lock().unwrap().clone();
        let connection_closed = state.connection_closed.clone();
        let drain_guard = state.app.ws_drain.token();
        let transport = P9GlobalAdmission::shared()
            .try_admit_transport()
            .expect("smoke WebSocket transport admission");
        configure_9p_ws(ws).on_upgrade(move |socket| async move {
            tokio::select! {
                _ = handle_9p_ws(socket, state.app, drain_guard, transport) => {}
                _ = connection_shutdown.cancelled() => {}
            }
            connection_closed.notify_one();
        })
    }

    async fn drop_smoke_connections(State(state): State<SmokeState>) -> StatusCode {
        let closed = state.connection_closed.notified();
        tokio::pin!(closed);
        closed.as_mut().enable();
        {
            let mut shutdown = state.connections.lock().unwrap();
            shutdown.cancel();
            *shutdown = CancellationToken::new();
        }
        match tokio::time::timeout(std::time::Duration::from_secs(1), closed).await {
            Ok(()) => StatusCode::NO_CONTENT,
            Err(_) => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }

    #[tokio::test]
    async fn websocket_rejects_messages_above_the_p9_limit() {
        let filesystem = Arc::new(ZeroFS::new_in_memory().await.unwrap());
        let app = Router::new()
            .route("/ws/9p", get(ws_9p_upgrade))
            .with_state(AppState {
                filesystem,
                lock_manager: Arc::new(FileLockManager::new()),
                uid: 0,
                gid: 0,
                shutdown: CancellationToken::new(),
                ws_drain: TaskTracker::new(),
                accepted_work: P9AcceptedWorkTracker::new(),
                p9_idle_timeout: None,
                upload_write_permits: upload_write_permits(),
                upload_ingress: upload_ingress(),
                writeback: None,
            });
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move { axum::serve(listener, app).await });
        let mut client = tokio::net::TcpStream::connect(address).await.unwrap();
        client
            .write_all(
                format!(
                    "GET /ws/9p HTTP/1.1\r\nHost: {address}\r\nUpgrade: websocket\r\n\
                     Connection: Upgrade\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\
                     Sec-WebSocket-Version: 13\r\n\r\n"
                )
                .as_bytes(),
            )
            .await
            .unwrap();
        let mut handshake = Vec::new();
        loop {
            let mut byte = [0];
            client.read_exact(&mut byte).await.unwrap();
            handshake.push(byte[0]);
            if handshake.ends_with(b"\r\n\r\n") {
                break;
            }
        }
        assert!(handshake.starts_with(b"HTTP/1.1 101"));

        // A masked binary frame whose declared payload is one byte above the
        // P9 transport limit. Sending only its header proves the WebSocket
        // layer rejects the frame from its declared length, before buffering
        // a body outside the global receive envelope.
        let oversize = u64::from(ninep_proto::P9_MAX_MSIZE) + 1;
        let mut frame_header = vec![0x82, 0xff];
        frame_header.extend_from_slice(&oversize.to_be_bytes());
        frame_header.extend_from_slice(&[0; 4]);
        client.write_all(&frame_header).await.unwrap();

        let mut response = [0; 2];
        let read = tokio::time::timeout(
            std::time::Duration::from_secs(1),
            client.read(&mut response),
        )
        .await
        .expect("oversize WebSocket must be closed promptly")
        .unwrap();
        assert!(
            read == 0 || response[0] & 0x0f == 0x08,
            "oversize frame produced a non-close WebSocket response: {response:?}"
        );
        server.abort();
        let _ = server.await;
    }

    async fn ws_handshake(client: &mut tokio::net::TcpStream, address: std::net::SocketAddr) {
        client
            .write_all(
                format!(
                    "GET /ws/9p HTTP/1.1\r\nHost: {address}\r\nUpgrade: websocket\r\n\
                     Connection: Upgrade\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\
                     Sec-WebSocket-Version: 13\r\n\r\n"
                )
                .as_bytes(),
            )
            .await
            .unwrap();
        let mut handshake = Vec::new();
        loop {
            let mut byte = [0];
            client.read_exact(&mut byte).await.unwrap();
            handshake.push(byte[0]);
            if handshake.ends_with(b"\r\n\r\n") {
                break;
            }
        }
        assert!(handshake.starts_with(b"HTTP/1.1 101"));
    }

    #[tokio::test]
    async fn idle_websocket_session_is_reaped_and_finishes_its_handler() {
        let filesystem = Arc::new(ZeroFS::new_in_memory().await.unwrap());
        let ws_drain = TaskTracker::new();
        let app = Router::new()
            .route("/ws/9p", get(ws_9p_upgrade))
            .with_state(AppState {
                filesystem,
                lock_manager: Arc::new(FileLockManager::new()),
                uid: 0,
                gid: 0,
                shutdown: CancellationToken::new(),
                ws_drain: ws_drain.clone(),
                accepted_work: P9AcceptedWorkTracker::new(),
                p9_idle_timeout: Some(std::time::Duration::from_millis(100)),
                upload_write_permits: upload_write_permits(),
                upload_ingress: upload_ingress(),
                writeback: None,
            });
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move { axum::serve(listener, app).await });
        let mut client = tokio::net::TcpStream::connect(address).await.unwrap();
        ws_handshake(&mut client, address).await;

        // A session that sends nothing and completes no request must be
        // closed by the server once the idle window elapses.
        let mut response = [0; 2];
        let read = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            client.read(&mut response),
        )
        .await
        .expect("idle WebSocket session must be reaped promptly")
        .unwrap();
        assert!(
            read == 0 || response[0] & 0x0f == 0x08,
            "idle reap produced a non-close WebSocket response: {response:?}"
        );

        // The handler itself finished, so its drain token (and with it the
        // transport permit the handler owned) has been released.
        ws_drain.close();
        tokio::time::timeout(std::time::Duration::from_secs(5), ws_drain.wait())
            .await
            .expect("reaped session must release its drain token");
        server.abort();
        let _ = server.await;
    }

    async fn rejecting_ws_upgrade(ws: WebSocketUpgrade) -> impl IntoResponse {
        reject_9p_ws(ws, "9P transport capacity exhausted".to_owned())
    }

    #[tokio::test]
    async fn admission_rejection_surfaces_as_a_websocket_close_frame() {
        let app = Router::new().route("/ws/9p", get(rejecting_ws_upgrade));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move { axum::serve(listener, app).await });
        let mut client = tokio::net::TcpStream::connect(address).await.unwrap();
        ws_handshake(&mut client, address).await;

        // The rejected session completes the upgrade only to deliver an
        // explicit close frame, so a bridge client that ignores non-101
        // responses still observes a prompt transport-level rejection.
        let mut header = [0; 2];
        tokio::time::timeout(
            std::time::Duration::from_secs(1),
            client.read_exact(&mut header),
        )
        .await
        .expect("rejected WebSocket must be closed promptly")
        .unwrap();
        assert_eq!(header[0] & 0x0f, 0x08, "expected a close frame");
        let mut code = [0; 2];
        client.read_exact(&mut code).await.unwrap();
        assert_eq!(
            u16::from_be_bytes(code),
            WS_CLOSE_TRY_AGAIN_LATER,
            "close frame must carry the try-again-later code"
        );
        server.abort();
        let _ = server.await;
    }

    /// Browser-runtime smoke test requiring generated wasm output and Node 22.
    #[tokio::test]
    #[ignore]
    async fn wasm_client_smoke() {
        let filesystem = Arc::new(ZeroFS::new_in_memory().await.expect("in-memory filesystem"));
        let state = SmokeState {
            app: AppState {
                filesystem,
                lock_manager: Arc::new(FileLockManager::new()),
                uid: 0,
                gid: 0,
                shutdown: CancellationToken::new(),
                ws_drain: TaskTracker::new(),
                accepted_work: P9AcceptedWorkTracker::new(),
                p9_idle_timeout: None,
                upload_write_permits: upload_write_permits(),
                upload_ingress: upload_ingress(),
                writeback: None,
            },
            connections: Arc::new(std::sync::Mutex::new(CancellationToken::new())),
            connection_closed: Arc::new(tokio::sync::Notify::new()),
        };
        let app = Router::new()
            .route("/ws/9p", get(smoke_ws_upgrade))
            .route("/drop-connections", post(drop_smoke_connections))
            .with_state(state);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind smoke server");
        let address = listener.local_addr().expect("smoke server address");
        let server = tokio::spawn(async move {
            axum::serve(listener, app)
                .await
                .expect("serve smoke endpoint");
        });

        let script = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../webui/scripts/smoke-wasm-client.mjs");
        let output = tokio::process::Command::new("node")
            .arg(script)
            .arg(format!("ws://{address}/ws/9p"))
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .output()
            .await
            .expect("run Node WASM smoke client");
        server.abort();

        assert!(
            output.status.success(),
            "WASM client failed\nstdout:\n{}\nstderr:\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr),
        );
    }

    fn upload_test_router(filesystem: Arc<ZeroFS>) -> Router {
        upload_test_router_with_permits(filesystem, upload_write_permits())
    }

    fn upload_test_router_with_permits(
        filesystem: Arc<ZeroFS>,
        upload_write_permits: Arc<tokio::sync::Semaphore>,
    ) -> Router {
        upload_test_router_with_admission(filesystem, upload_write_permits, upload_ingress())
    }

    fn upload_test_router_with_admission(
        filesystem: Arc<ZeroFS>,
        upload_write_permits: Arc<tokio::sync::Semaphore>,
        upload_ingress: Arc<UploadIngressAdmission>,
    ) -> Router {
        Router::new().merge(upload_router()).with_state(AppState {
            filesystem,
            lock_manager: Arc::new(FileLockManager::new()),
            uid: 0,
            gid: 0,
            shutdown: CancellationToken::new(),
            ws_drain: TaskTracker::new(),
            accepted_work: P9AcceptedWorkTracker::new(),
            p9_idle_timeout: None,
            upload_write_permits,
            upload_ingress,
            writeback: None,
        })
    }

    fn upload_test_router_with_writeback(
        filesystem: Arc<ZeroFS>,
        writeback: WritebackObjectStore,
    ) -> Router {
        Router::new().merge(upload_router()).with_state(AppState {
            filesystem,
            lock_manager: Arc::new(FileLockManager::new()),
            uid: 0,
            gid: 0,
            shutdown: CancellationToken::new(),
            ws_drain: TaskTracker::new(),
            accepted_work: P9AcceptedWorkTracker::new(),
            p9_idle_timeout: None,
            upload_write_permits: upload_write_permits(),
            upload_ingress: upload_ingress(),
            writeback: Some(writeback),
        })
    }

    async fn writeback_with_min_free(
        min_free_bytes: u64,
    ) -> (WritebackObjectStore, tempfile::TempDir) {
        let temp = tempfile::tempdir().unwrap();
        let dir = temp.path().join("writeback");
        let journal = Arc::new(
            Journal::open(
                dir.clone(),
                JournalIdentity {
                    format_version: 1,
                    bucket_id: "upload-pressure".to_owned(),
                    backend_endpoint: "memory://remote".to_owned(),
                    database_prefix: "zerofs/upload-pressure".to_owned(),
                    backend_kind: "memory".to_owned(),
                    encryption_key_identity_sha256: [0x77; 32],
                },
            )
            .unwrap(),
        );
        let writeback = WritebackObjectStore::open_paused(
            Arc::new(InMemory::new()),
            journal,
            WritebackSettings {
                dir,
                ack_mode: AckMode::Memory,
                memory_bytes: 1024 * 1024,
                disk_bytes: 1024 * 1024,
                min_free_bytes,
                high_watermark_percent: 95,
                resume_percent: 85,
                upload_concurrency: 1,
                local_concurrency: 1,
                shutdown_flush: ShutdownFlush::Local,
            },
        )
        .await
        .unwrap();
        (writeback, temp)
    }

    async fn upload_request(
        app: &Router,
        method: &str,
        uri: &str,
        body: Vec<u8>,
    ) -> (StatusCode, serde_json::Value) {
        use tower::ServiceExt;
        let content_type = if method == "POST" {
            "application/json"
        } else {
            "application/octet-stream"
        };
        let response = app
            .clone()
            .oneshot(
                axum::http::Request::builder()
                    .method(method)
                    .uri(uri)
                    .header("content-type", content_type)
                    .body(axum::body::Body::from(body))
                    .unwrap(),
            )
            .await
            .unwrap();
        let status = response.status();
        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let value = if bytes.is_empty() {
            serde_json::Value::Null
        } else {
            serde_json::from_slice(&bytes).unwrap_or(serde_json::Value::Null)
        };
        (status, value)
    }

    fn sha256_hex(data: &[u8]) -> String {
        use sha2::Digest;
        use std::fmt::Write as _;
        let mut hex = String::new();
        for byte in sha2::Sha256::digest(data) {
            write!(hex, "{byte:02x}").unwrap();
        }
        hex
    }

    #[tokio::test]
    async fn known_storage_pressure_returns_retryable_upload_response_before_path_work() {
        let (writeback, _temp) = writeback_with_min_free(u64::MAX).await;
        let error = upload_writeback_preflight(Some(&writeback))
            .await
            .unwrap_err();
        let response = upload_fs_error(error);
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(
            response.headers()[header::RETRY_AFTER],
            HeaderValue::from_static("1")
        );
    }

    #[tokio::test]
    async fn pressured_upload_routes_reject_before_creating_staging_paths() {
        use tower::ServiceExt;

        let (writeback, _temp) = writeback_with_min_free(u64::MAX).await;
        let filesystem = Arc::new(ZeroFS::new_in_memory().await.unwrap());
        let app = upload_test_router_with_writeback(Arc::clone(&filesystem), writeback);
        let response = app
            .clone()
            .oneshot(
                axum::http::Request::builder()
                    .method("PUT")
                    .uri("/api/v1/upload/staging/blocked.bin?offset=0")
                    .body(axum::body::Body::from("blocked"))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(response.headers()[header::RETRY_AFTER], "1");

        let response = app
            .oneshot(
                axum::http::Request::builder()
                    .method("POST")
                    .uri("/api/v1/upload/staging/blocked.bin/commit")
                    .header("content-type", "application/json")
                    .body(axum::body::Body::from(r#"{"size": 0, "sha256": "00"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(response.headers()[header::RETRY_AFTER], "1");

        let creds = Credentials::from_auth_context(&AuthContext::default());
        assert!(matches!(
            upload_resolve_existing(
                &filesystem,
                &creds,
                &upload_path_components("staging/blocked.bin").unwrap(),
            )
            .await,
            Err(FsError::NotFound)
        ));
    }

    #[tokio::test]
    async fn healthy_writeback_preflight_leaves_uploads_admitted() {
        let (writeback, _temp) = writeback_with_min_free(1).await;
        assert!(upload_writeback_preflight(Some(&writeback)).await.is_ok());
    }

    #[tokio::test]
    async fn closed_writeback_preflight_is_not_retryable_pressure() {
        let (writeback, _temp) = writeback_with_min_free(1).await;
        writeback.ssd_admission().close();
        assert_eq!(
            upload_writeback_preflight(Some(&writeback))
                .await
                .unwrap_err(),
            FsError::IoError
        );
    }

    #[tokio::test]
    async fn stalled_request_bodies_do_not_block_a_ready_upload() {
        use tower::ServiceExt;

        let filesystem = Arc::new(ZeroFS::new_in_memory().await.unwrap());
        let permits = upload_write_permits();
        let app = upload_test_router_with_permits(filesystem, Arc::clone(&permits));
        let bodies_polled = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let mut stalled = Vec::new();

        for index in 0..MAX_CONCURRENT_UPLOAD_WRITES {
            let bodies_polled = Arc::clone(&bodies_polled);
            let first_chunk = futures::stream::once(async {
                Ok::<bytes::Bytes, std::convert::Infallible>(bytes::Bytes::from_static(b"partial"))
            });
            let stalled_tail = futures::stream::once(async move {
                bodies_polled.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                futures::future::pending::<()>().await;
                Ok::<bytes::Bytes, std::convert::Infallible>(bytes::Bytes::new())
            });
            let stream = futures::StreamExt::chain(first_chunk, stalled_tail);
            let request = axum::http::Request::builder()
                .method("PUT")
                .uri(format!("/api/v1/upload/stalled/{index}.bin?offset=0"))
                .header("content-type", "application/octet-stream")
                .body(axum::body::Body::from_stream(stream))
                .unwrap();
            let service = app.clone();
            stalled.push(tokio::spawn(async move { service.oneshot(request).await }));
        }

        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            while bodies_polled.load(std::sync::atomic::Ordering::SeqCst)
                != MAX_CONCURRENT_UPLOAD_WRITES
            {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("every stalled request body must be polled");

        let ready = tokio::time::timeout(
            std::time::Duration::from_secs(1),
            upload_request(
                &app,
                "PUT",
                "/api/v1/upload/ready.bin?offset=0",
                b"ready".to_vec(),
            ),
        )
        .await
        .expect("slow request bodies must not monopolize write admission");
        assert_eq!(ready.0, StatusCode::OK, "{}", ready.1);

        for task in stalled {
            task.abort();
        }
    }

    #[tokio::test]
    async fn small_network_frames_are_coalesced_into_bounded_filesystem_writes() {
        use tower::ServiceExt;

        const FRAME_BYTES: usize = 8 * 1024;
        const TOTAL_BYTES: usize = 5 * 1024 * 1024;
        let filesystem = Arc::new(ZeroFS::new_in_memory().await.unwrap());
        let auth = AuthContext::default();
        let creds = Credentials::from_auth_context(&auth);
        filesystem
            .create(&creds, 0, b"batched.bin", &SetAttributes::default())
            .await
            .unwrap();
        let before = filesystem
            .stats
            .write_operations
            .load(std::sync::atomic::Ordering::Relaxed);
        let app = upload_test_router(Arc::clone(&filesystem));
        let stream = futures::stream::iter((0..TOTAL_BYTES / FRAME_BYTES).map(|index| {
            Ok::<bytes::Bytes, std::convert::Infallible>(bytes::Bytes::from(vec![
                index as u8;
                FRAME_BYTES
            ]))
        }));
        let response = app
            .oneshot(
                axum::http::Request::builder()
                    .method("PUT")
                    .uri("/api/v1/upload/batched.bin?offset=0")
                    .body(axum::body::Body::from_stream(stream))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let after = filesystem
            .stats
            .write_operations
            .load(std::sync::atomic::Ordering::Relaxed);
        assert_eq!(
            after - before,
            2,
            "5 MiB from 8 KiB frames must flush as 4 MiB plus one EOF tail"
        );
    }

    #[tokio::test]
    async fn stalled_partial_bodies_are_bounded_and_excess_is_rejected() {
        use tower::ServiceExt;

        const REQUEST_LIMIT: usize = 32;
        let filesystem = Arc::new(ZeroFS::new_in_memory().await.unwrap());
        let app = upload_test_router_with_admission(
            Arc::clone(&filesystem),
            upload_write_permits(),
            UploadIngressAdmission::new(REQUEST_LIMIT, REQUEST_LIMIT * UPLOAD_FRAME_AND_COPY_BYTES),
        );
        let bodies_polled = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let mut stalled = Vec::new();

        for index in 0..REQUEST_LIMIT {
            let bodies_polled = Arc::clone(&bodies_polled);
            let first_chunk = futures::stream::once(async {
                Ok::<bytes::Bytes, std::convert::Infallible>(bytes::Bytes::from_static(b"partial"))
            });
            let stalled_tail = futures::stream::once(async move {
                bodies_polled.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                futures::future::pending::<()>().await;
                Ok::<bytes::Bytes, std::convert::Infallible>(bytes::Bytes::new())
            });
            let request = axum::http::Request::builder()
                .method("PUT")
                .uri(format!("/api/v1/upload/bounded/{index}.bin?offset=0"))
                .body(axum::body::Body::from_stream(futures::StreamExt::chain(
                    first_chunk,
                    stalled_tail,
                )))
                .unwrap();
            let service = app.clone();
            stalled.push(tokio::spawn(async move { service.oneshot(request).await }));
        }

        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            while bodies_polled.load(std::sync::atomic::Ordering::SeqCst) != REQUEST_LIMIT {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("all admitted request bodies must reach their stalled tail");

        let response = tokio::time::timeout(
            std::time::Duration::from_secs(1),
            upload_request(
                &app,
                "PUT",
                "/api/v1/upload/bounded/excess.bin?offset=0",
                b"excess".to_vec(),
            ),
        )
        .await
        .expect("excess request must be rejected promptly");
        assert_eq!(
            response.0,
            StatusCode::SERVICE_UNAVAILABLE,
            "{}",
            response.1
        );

        let creds = Credentials::from_auth_context(&AuthContext::default());
        assert!(matches!(
            upload_resolve_existing(
                &filesystem,
                &creds,
                &upload_path_components("bounded/excess.bin").unwrap(),
            )
            .await,
            Err(FsError::NotFound)
        ));
        for task in stalled {
            task.abort();
        }
    }

    #[tokio::test]
    async fn shutdown_cancels_idle_body_after_accepted_partial_write_settles() {
        use tower::ServiceExt;

        let filesystem = Arc::new(ZeroFS::new_in_memory().await.unwrap());
        let shutdown = CancellationToken::new();
        let state = AppState {
            filesystem: Arc::clone(&filesystem),
            lock_manager: Arc::new(FileLockManager::new()),
            uid: 0,
            gid: 0,
            shutdown: shutdown.clone(),
            ws_drain: TaskTracker::new(),
            accepted_work: P9AcceptedWorkTracker::new(),
            p9_idle_timeout: None,
            upload_write_permits: upload_write_permits(),
            upload_ingress: upload_ingress(),
            writeback: None,
        };
        let app = Router::new().merge(upload_router()).with_state(state);
        let tail_polled = Arc::new(tokio::sync::Notify::new());
        let accepted = vec![0x5a; UPLOAD_STREAM_WRITE_CHUNK_BYTES];
        let accepted_len = accepted.len() as u64;
        let stream = futures::StreamExt::chain(
            futures::stream::once(async move {
                Ok::<bytes::Bytes, std::convert::Infallible>(bytes::Bytes::from(accepted))
            }),
            futures::stream::once({
                let tail_polled = Arc::clone(&tail_polled);
                async move {
                    tail_polled.notify_one();
                    futures::future::pending::<()>().await;
                    Ok::<bytes::Bytes, std::convert::Infallible>(bytes::Bytes::new())
                }
            }),
        );
        let request = axum::http::Request::builder()
            .method("PUT")
            .uri("/api/v1/upload/shutdown.bin?offset=0")
            .body(axum::body::Body::from_stream(stream))
            .unwrap();
        let upload = tokio::spawn(app.oneshot(request));
        tail_polled.notified().await;
        shutdown.cancel();

        let response = tokio::time::timeout(std::time::Duration::from_secs(1), upload)
            .await
            .expect("shutdown must stop polling an idle body")
            .unwrap()
            .unwrap();
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        let creds = Credentials::from_auth_context(&AuthContext::default());
        let (_, file) = upload_resolve_existing(
            &filesystem,
            &creds,
            &upload_path_components("shutdown.bin").unwrap(),
        )
        .await
        .unwrap();
        assert_eq!(
            upload_visible_file_size(&filesystem, file).await.unwrap(),
            accepted_len
        );
    }

    #[tokio::test]
    async fn cancelled_caller_cannot_drop_accepted_write_ownership() {
        let filesystem = Arc::new(ZeroFS::new_in_memory().await.unwrap());
        let auth = AuthContext::default();
        let creds = Credentials::from_auth_context(&auth);
        let (file, _) = filesystem
            .create(&creds, 0, b"accepted.bin", &SetAttributes::default())
            .await
            .unwrap();
        let inode_lock = filesystem.lock_manager.acquire(file).await;
        let payload = bytes::Bytes::from_static(b"accepted");
        let ingress = UploadIngressAdmission::new(1, payload.len());
        let request_guard = ingress.try_request().unwrap();
        let bytes_guard = ingress.try_bytes(payload.len()).unwrap();
        let write_permits = Arc::new(tokio::sync::Semaphore::new(1));
        let accepted_work = P9AcceptedWorkTracker::new();
        let state = AppState {
            filesystem: Arc::clone(&filesystem),
            lock_manager: Arc::new(FileLockManager::new()),
            uid: 0,
            gid: 0,
            shutdown: CancellationToken::new(),
            ws_drain: TaskTracker::new(),
            accepted_work: accepted_work.clone(),
            p9_idle_timeout: None,
            upload_write_permits: Arc::clone(&write_permits),
            upload_ingress: Arc::clone(&ingress),
            writeback: None,
        };
        let caller = tokio::spawn(async move {
            upload_write_admitted(&state, request_guard, &auth, file, 0, payload, bytes_guard).await
        });

        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            while accepted_work.len() != 1 || write_permits.available_permits() != 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("write did not enter accepted ownership");
        assert_eq!(ingress.requests.available_permits(), 0);
        assert_eq!(ingress.bytes.available_permits(), 0);

        caller.abort();
        assert!(caller.await.unwrap_err().is_cancelled());
        accepted_work.stop_accepting();
        let mut drain = tokio::spawn({
            let accepted_work = accepted_work.clone();
            async move { accepted_work.wait().await }
        });
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(25), &mut drain)
                .await
                .is_err(),
            "cancelled caller released accepted write ownership"
        );
        assert_eq!(ingress.requests.available_permits(), 0);
        assert_eq!(ingress.bytes.available_permits(), 0);
        assert_eq!(write_permits.available_permits(), 0);

        drop(inode_lock);
        tokio::time::timeout(std::time::Duration::from_secs(2), drain)
            .await
            .expect("accepted write did not settle")
            .unwrap();
        assert_eq!(ingress.requests.available_permits(), 1);
        assert_eq!(ingress.bytes.available_permits(), 8);
        assert_eq!(write_permits.available_permits(), 1);
        let (contents, eof) = filesystem
            .read_file(&AuthContext::default(), file, 0, 32)
            .await
            .unwrap();
        assert!(eof);
        assert_eq!(contents.as_ref(), b"accepted");
    }

    #[tokio::test(start_paused = true)]
    async fn body_idle_timeout_releases_request_without_creating_a_path() {
        use tower::ServiceExt;

        let filesystem = Arc::new(ZeroFS::new_in_memory().await.unwrap());
        let app = upload_test_router(Arc::clone(&filesystem));
        let empty_frames = futures::StreamExt::take(
            futures::stream::repeat(Ok::<bytes::Bytes, std::convert::Infallible>(
                bytes::Bytes::new(),
            )),
            100,
        );
        let stalled = futures::stream::once(async {
            futures::future::pending::<()>().await;
            Ok::<bytes::Bytes, std::convert::Infallible>(bytes::Bytes::new())
        });
        let stream = futures::StreamExt::chain(empty_frames, stalled);
        let request = axum::http::Request::builder()
            .method("PUT")
            .uri("/api/v1/upload/idle.bin?offset=0")
            .body(axum::body::Body::from_stream(stream))
            .unwrap();
        let upload = tokio::spawn(app.oneshot(request));
        tokio::task::yield_now().await;
        tokio::time::advance(std::time::Duration::from_secs(31)).await;

        let response = tokio::time::timeout(std::time::Duration::from_millis(1), upload)
            .await
            .expect("idle body must be cancelled")
            .unwrap()
            .unwrap();
        assert_eq!(response.status(), StatusCode::REQUEST_TIMEOUT);
        let creds = Credentials::from_auth_context(&AuthContext::default());
        assert!(matches!(
            upload_resolve_existing(
                &filesystem,
                &creds,
                &upload_path_components("idle.bin").unwrap(),
            )
            .await,
            Err(FsError::NotFound)
        ));
    }

    #[tokio::test]
    async fn full_upload_frames_blocked_on_writers_exhaust_the_byte_budget() {
        use tower::ServiceExt;

        const SATURATING_REQUESTS: usize = MAX_CONCURRENT_UPLOAD_WRITES + 1;
        let filesystem = Arc::new(ZeroFS::new_in_memory().await.unwrap());
        let write_permits = Arc::new(tokio::sync::Semaphore::new(0));
        let app = upload_test_router_with_permits(filesystem, write_permits);
        let frames_polled = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let mut blocked = Vec::new();

        for index in 0..SATURATING_REQUESTS {
            let frames_polled = Arc::clone(&frames_polled);
            let frame = vec![index as u8; UPLOAD_STREAM_WRITE_CHUNK_BYTES];
            let stream = futures::stream::once(async move {
                frames_polled.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                Ok::<bytes::Bytes, std::convert::Infallible>(bytes::Bytes::from(frame))
            });
            let request = axum::http::Request::builder()
                .method("PUT")
                .uri(format!("/api/v1/upload/full/{index}.bin?offset=0"))
                .body(axum::body::Body::from_stream(stream))
                .unwrap();
            let service = app.clone();
            blocked.push(tokio::spawn(async move { service.oneshot(request).await }));
        }

        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            while frames_polled.load(std::sync::atomic::Ordering::SeqCst) != SATURATING_REQUESTS {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("all saturating frames must be polled");
        tokio::task::yield_now().await;

        let response = tokio::time::timeout(
            std::time::Duration::from_secs(1),
            upload_request(
                &app,
                "PUT",
                "/api/v1/upload/full/excess.bin?offset=0",
                b"x".to_vec(),
            ),
        )
        .await
        .expect("byte-budget exhaustion must reject instead of queueing a retained body");
        assert_eq!(
            response.0,
            StatusCode::SERVICE_UNAVAILABLE,
            "{}",
            response.1
        );

        for task in blocked {
            task.abort();
        }
    }

    #[tokio::test]
    async fn upload_assembles_parallel_segments_then_publishes() {
        use sha2::Digest;
        let filesystem = Arc::new(ZeroFS::new_in_memory().await.unwrap());
        let app = upload_test_router(Arc::clone(&filesystem));
        // Two segments of one logical file, each uploaded on its own
        // connection. Segment 1 is written before segment 0 to prove the
        // order on the wire does not matter — only the commit list does.
        let seg0 = b"the first half of the file, ".to_vec();
        let seg1 = b"and the second half of it.".to_vec();
        let total: Vec<u8> = [seg0.clone(), seg1.clone()].concat();

        let (status, _) = upload_request(
            &app,
            "PUT",
            "/api/v1/upload/staging/big.bin.seg1?offset=0",
            seg1.clone(),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        let (status, _) = upload_request(
            &app,
            "PUT",
            "/api/v1/upload/staging/big.bin.seg0?offset=0",
            seg0.clone(),
        )
        .await;
        assert_eq!(status, StatusCode::OK);

        let mut hasher = sha2::Sha256::new();
        hasher.update(&total);
        let digest = hasher.finalize();
        let mut sha = String::new();
        {
            use std::fmt::Write as _;
            for byte in digest {
                write!(sha, "{byte:02x}").unwrap();
            }
        }

        let commit = serde_json::json!({
            "size": total.len(),
            "sha256": sha,
            "publish_to": "/published/big.bin",
            "assemble_from": ["/staging/big.bin.seg0", "/staging/big.bin.seg1"],
        });
        let (status, body) = upload_request(
            &app,
            "POST",
            "/api/v1/upload/staging/big.bin/commit",
            serde_json::to_vec(&commit).unwrap(),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(body["verified"], true);
        assert_eq!(body["size"], total.len() as u64);
        assert_eq!(body["path"], "/published/big.bin");

        // The assembled bytes are the concatenation in commit-list order,
        // not upload order.
        let (status, body) = upload_request(
            &app,
            "GET",
            "/api/v1/upload/published/big.bin/status",
            Vec::new(),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(body["size"], total.len() as u64);

        // Segments are cleaned up once folded in.
        let (status, _) = upload_request(
            &app,
            "GET",
            "/api/v1/upload/staging/big.bin.seg0/status",
            Vec::new(),
        )
        .await;
        assert_eq!(status, StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn upload_segment_assembly_replaces_a_longer_staging_file() {
        let filesystem = Arc::new(ZeroFS::new_in_memory().await.unwrap());
        let app = upload_test_router(Arc::clone(&filesystem));
        let replacement = b"short replacement".to_vec();

        let (status, body) = upload_request(
            &app,
            "PUT",
            "/api/v1/upload/staging/target.bin?offset=0",
            b"an older staging value with a long stale tail".to_vec(),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        let (status, body) = upload_request(
            &app,
            "PUT",
            "/api/v1/upload/staging/target.bin.segment?offset=0",
            replacement.clone(),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");

        let commit = serde_json::json!({
            "size": replacement.len(),
            "sha256": sha256_hex(&replacement),
            "assemble_from": ["/staging/target.bin.segment"],
        });
        let (status, body) = upload_request(
            &app,
            "POST",
            "/api/v1/upload/staging/target.bin/commit",
            serde_json::to_vec(&commit).unwrap(),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(body["size"], replacement.len() as u64);

        let auth = AuthContext::default();
        let creds = Credentials::from_auth_context(&auth);
        let (_, target) = upload_resolve_existing(
            &filesystem,
            &creds,
            &upload_path_components("staging/target.bin").unwrap(),
        )
        .await
        .unwrap();
        let (contents, eof) = filesystem.read_file(&auth, target, 0, 1024).await.unwrap();
        assert!(eof);
        assert_eq!(contents.as_ref(), replacement.as_slice());
    }

    #[tokio::test]
    async fn failed_segment_assembly_preserves_every_source_segment() {
        let filesystem = Arc::new(ZeroFS::new_in_memory().await.unwrap());
        let app = upload_test_router(filesystem);
        let segment = b"retryable segment".to_vec();
        let (status, body) = upload_request(
            &app,
            "PUT",
            "/api/v1/upload/staging/retry.segment0?offset=0",
            segment.clone(),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");

        let commit = serde_json::json!({
            "size": segment.len(),
            "sha256": sha256_hex(&segment),
            "assemble_from": [
                "/staging/retry.segment0",
                "/staging/missing.segment1"
            ],
        });
        let (status, _) = upload_request(
            &app,
            "POST",
            "/api/v1/upload/staging/retry.bin/commit",
            serde_json::to_vec(&commit).unwrap(),
        )
        .await;
        assert_eq!(status, StatusCode::NOT_FOUND);

        let (status, body) = upload_request(
            &app,
            "GET",
            "/api/v1/upload/staging/retry.segment0/status",
            Vec::new(),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(body["size"], segment.len() as u64);
    }

    #[tokio::test]
    async fn failed_segment_verification_preserves_sources_for_retry() {
        let filesystem = Arc::new(ZeroFS::new_in_memory().await.unwrap());
        let app = upload_test_router(filesystem);
        let segment = b"complete but not verified".to_vec();
        let (status, body) = upload_request(
            &app,
            "PUT",
            "/api/v1/upload/staging/verify.segment?offset=0",
            segment.clone(),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");

        let commit = serde_json::json!({
            "size": segment.len(),
            "sha256": "0".repeat(64),
            "assemble_from": ["/staging/verify.segment"],
        });
        let (status, body) = upload_request(
            &app,
            "POST",
            "/api/v1/upload/staging/verify.bin/commit",
            serde_json::to_vec(&commit).unwrap(),
        )
        .await;
        assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{body}");

        let (status, body) = upload_request(
            &app,
            "GET",
            "/api/v1/upload/staging/verify.segment/status",
            Vec::new(),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(body["size"], segment.len() as u64);
    }

    #[tokio::test]
    async fn segment_cleanup_does_not_remove_publish_destination() {
        let filesystem = Arc::new(ZeroFS::new_in_memory().await.unwrap());
        let app = upload_test_router(filesystem);
        let segment = b"published over its source path".to_vec();
        let (status, body) = upload_request(
            &app,
            "PUT",
            "/api/v1/upload/staging/publish.segment?offset=0",
            segment.clone(),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");

        let commit = serde_json::json!({
            "size": segment.len(),
            "sha256": sha256_hex(&segment),
            "assemble_from": ["/staging/publish.segment"],
            "publish_to": "/staging/publish.segment",
        });
        let (status, body) = upload_request(
            &app,
            "POST",
            "/api/v1/upload/staging/publish.target/commit",
            serde_json::to_vec(&commit).unwrap(),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(body["path"], "/staging/publish.segment");

        let (status, body) = upload_request(
            &app,
            "GET",
            "/api/v1/upload/staging/publish.segment/status",
            Vec::new(),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(body["size"], segment.len() as u64);
    }

    #[tokio::test]
    async fn upload_parts_commit_and_publish_roundtrip() {
        let filesystem = Arc::new(ZeroFS::new_in_memory().await.unwrap());
        let app = upload_test_router(Arc::clone(&filesystem));
        let part1 = b"hello upload world, this is part one; ".to_vec();
        let part2 = b"and this is part two.".to_vec();
        let total: Vec<u8> = [part1.clone(), part2.clone()].concat();

        // Parent directories are created on demand.
        let (status, body) = upload_request(
            &app,
            "PUT",
            "/api/v1/upload/staging/photos/img.bin?offset=0",
            part1.clone(),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(body["size"], part1.len() as u64);

        // An interrupted client resumes from the server's byte count.
        let (status, body) = upload_request(
            &app,
            "GET",
            "/api/v1/upload/staging/photos/img.bin/status",
            Vec::new(),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["size"], part1.len() as u64);

        let (status, body) = upload_request(
            &app,
            "PUT",
            &format!(
                "/api/v1/upload/staging/photos/img.bin?offset={}",
                part1.len()
            ),
            part2.clone(),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(body["size"], total.len() as u64);

        // Commit verifies size + sha256 server-side, then atomically
        // publishes the staging file to its final path.
        let commit = serde_json::json!({
            "size": total.len(),
            "sha256": sha256_hex(&total),
            "publish_to": "photos/final.bin",
        });
        let (status, body) = upload_request(
            &app,
            "POST",
            "/api/v1/upload/staging/photos/img.bin/commit",
            commit.to_string().into_bytes(),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(body["verified"], true);
        assert_eq!(body["size"], total.len() as u64);
        assert_eq!(body["path"], "/photos/final.bin");

        // The staging path is gone and the published file holds the bytes.
        let (status, _) = upload_request(
            &app,
            "GET",
            "/api/v1/upload/staging/photos/img.bin/status",
            Vec::new(),
        )
        .await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        let auth = AuthContext::default();
        let creds = Credentials::from_auth_context(&auth);
        let (_, published) = upload_resolve_existing(
            &filesystem,
            &creds,
            &upload_path_components("photos/final.bin").unwrap(),
        )
        .await
        .unwrap();
        let (contents, eof) = filesystem
            .read_file(&auth, published, 0, u32::try_from(total.len()).unwrap() + 1)
            .await
            .unwrap();
        assert!(eof);
        assert_eq!(contents.as_ref(), total.as_slice());
    }

    #[tokio::test]
    async fn upload_rejects_offset_gaps_with_current_size() {
        let filesystem = Arc::new(ZeroFS::new_in_memory().await.unwrap());
        let app = upload_test_router(filesystem);

        // A nonzero offset into a file that does not exist yet is a gap.
        let (status, body) = upload_request(
            &app,
            "PUT",
            "/api/v1/upload/gap.bin?offset=5",
            b"x".to_vec(),
        )
        .await;
        assert_eq!(status, StatusCode::CONFLICT);
        assert_eq!(body["size"], 0);

        let (status, _) = upload_request(
            &app,
            "PUT",
            "/api/v1/upload/gap.bin?offset=0",
            b"abc".to_vec(),
        )
        .await;
        assert_eq!(status, StatusCode::OK);

        // Past-the-end offsets report the current size so the client can
        // trust it as the verified contiguous prefix and back up to it.
        let (status, body) = upload_request(
            &app,
            "PUT",
            "/api/v1/upload/gap.bin?offset=5",
            b"x".to_vec(),
        )
        .await;
        assert_eq!(status, StatusCode::CONFLICT);
        assert_eq!(body["size"], 3);

        // A retransmitted part whose acknowledgement was lost is accepted.
        let (status, body) = upload_request(
            &app,
            "PUT",
            "/api/v1/upload/gap.bin?offset=0",
            b"abc".to_vec(),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["size"], 3);
    }

    #[tokio::test]
    async fn upload_commit_verify_failure_is_nondestructive() {
        let filesystem = Arc::new(ZeroFS::new_in_memory().await.unwrap());
        let app = upload_test_router(filesystem);
        let data = b"payload".to_vec();
        let (status, _) =
            upload_request(&app, "PUT", "/api/v1/upload/doc.bin?offset=0", data.clone()).await;
        assert_eq!(status, StatusCode::OK);

        // A wrong hash reports what the server actually holds...
        let bad_commit = serde_json::json!({
            "size": data.len(),
            "sha256": "0".repeat(64),
        });
        let (status, body) = upload_request(
            &app,
            "POST",
            "/api/v1/upload/doc.bin/commit",
            bad_commit.to_string().into_bytes(),
        )
        .await;
        assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
        assert_eq!(body["verified"], false);
        assert_eq!(body["size"], data.len() as u64);
        assert_eq!(body["sha256"], sha256_hex(&data));

        // ...and leaves the staging file in place for a corrected commit.
        let good_commit = serde_json::json!({
            "size": data.len(),
            "sha256": sha256_hex(&data),
        });
        let (status, body) = upload_request(
            &app,
            "POST",
            "/api/v1/upload/doc.bin/commit",
            good_commit.to_string().into_bytes(),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(body["verified"], true);
        assert_eq!(body["path"], "/doc.bin");

        // Committing a path that was never uploaded is 404, not a create.
        let (status, _) = upload_request(
            &app,
            "POST",
            "/api/v1/upload/missing.bin/commit",
            good_commit.to_string().into_bytes(),
        )
        .await;
        assert_eq!(status, StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn upload_rejects_traversal_and_oversized_parts() {
        let filesystem = Arc::new(ZeroFS::new_in_memory().await.unwrap());
        let app = upload_test_router(filesystem);

        for path in ["a/../b", "a//b", "."] {
            let (status, _) = upload_request(
                &app,
                "PUT",
                &format!("/api/v1/upload/{path}?offset=0"),
                b"x".to_vec(),
            )
            .await;
            assert_eq!(status, StatusCode::BAD_REQUEST, "path {path:?}");
        }

        // One byte over the part cap is refused before it reaches the fs.
        let oversize = vec![0u8; MAX_UPLOAD_PART_BYTES + 1];
        let (status, _) =
            upload_request(&app, "PUT", "/api/v1/upload/big.bin?offset=0", oversize).await;
        assert_eq!(status, StatusCode::PAYLOAD_TOO_LARGE);
    }
}
