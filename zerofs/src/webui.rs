use crate::config::WebUIConfig;
use crate::fs::ZeroFS;
use crate::fs::errors::FsError;
use crate::fs::inode::{Inode, InodeId};
use crate::fs::permissions::Credentials;
use crate::fs::types::{AuthContext, SetAttributes};
use crate::ninep::handler::{NinePHandler, SessionReleaseGuard};
use crate::ninep::lock_manager::FileLockManager;
use crate::ninep::server::{
    InflightRegistry, P9AcceptedWorkTracker, P9GlobalAdmission, P9Response, P9TransportPermit,
    dispatch_9p_frame, response_may_be_emitted, settle_request_tasks,
};
use crate::rpc::proto;
use crate::rpc::server::AdminRpcServer;
use crate::task::spawn_named;
use axum::Router;
use axum::extract::State;
use axum::extract::ws::{Message as WsMessage, WebSocket, WebSocketUpgrade};
use axum::http::StatusCode;
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
    /// Caps how many HTTP upload part bodies are buffered in memory at once;
    /// see [`MAX_CONCURRENT_UPLOAD_PARTS`].
    upload_permits: Arc<tokio::sync::Semaphore>,
}

fn upload_permits() -> Arc<tokio::sync::Semaphore> {
    Arc::new(tokio::sync::Semaphore::new(MAX_CONCURRENT_UPLOAD_PARTS))
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
            upload_permits: upload_permits(),
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
//   PUT  /api/v1/upload/{path}?offset=N     binary body, one part (8-64 MiB)
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

/// Largest accepted upload part body. Clients send 8-64 MiB parts; anything
/// larger is refused with 413 before it is buffered.
const MAX_UPLOAD_PART_BYTES: usize = 16 * 1024 * 1024;

/// Upload part bodies buffered concurrently. Bodies are only read after a
/// permit is held, so HTTP upload buffering is capped at
/// `MAX_CONCURRENT_UPLOAD_PARTS * MAX_UPLOAD_PART_BYTES` = 16 * 16 MiB
/// = 256 MiB; further parts wait unbuffered on the socket.
const MAX_CONCURRENT_UPLOAD_PARTS: usize = 16;

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
    upload_json(status, serde_json::json!({ "error": error.to_string() }))
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
    let (dirs, name) = components.split_at(components.len() - 1);
    let mut dir: InodeId = 0;
    for component in dirs {
        dir = fs.lookup(creds, dir, component).await?;
    }
    let file = fs.lookup(creds, dir, &name[0]).await?;
    Ok((dir, file))
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

fn body_hit_length_limit(error: &axum::Error) -> bool {
    let mut source: Option<&(dyn std::error::Error + 'static)> = Some(error);
    while let Some(inner) = source {
        if inner.is::<http_body_util::LengthLimitError>() {
            return true;
        }
        source = inner.source();
    }
    false
}

async fn upload_part(
    State(state): State<AppState>,
    axum::extract::Path(path): axum::extract::Path<String>,
    axum::extract::Query(query): axum::extract::Query<UploadPartQuery>,
    body: axum::body::Body,
) -> axum::response::Response {
    let components = match upload_path_components(&path) {
        Ok(components) => components,
        Err(message) => return upload_error(StatusCode::BAD_REQUEST, message),
    };

    // Admission before buffering: at most MAX_CONCURRENT_UPLOAD_PARTS bodies
    // are ever resident; later parts wait here without consuming memory.
    let _permit = state
        .upload_permits
        .acquire()
        .await
        .expect("upload semaphore is never closed");
    let data = match axum::body::to_bytes(body, MAX_UPLOAD_PART_BYTES).await {
        Ok(data) => data,
        Err(error) if body_hit_length_limit(&error) => {
            return upload_error(
                StatusCode::PAYLOAD_TOO_LARGE,
                "part body exceeds the 64 MiB part limit",
            );
        }
        Err(_) => return upload_error(StatusCode::BAD_REQUEST, "failed to read request body"),
    };
    let Some(end_offset) = query.offset.checked_add(data.len() as u64) else {
        return upload_error(StatusCode::BAD_REQUEST, "offset + length overflows");
    };
    let _ = end_offset;

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

    // Same RAM-ack seam 9P Twrite lands on (`write_ack` forwards to
    // `write_ack_identified`, the path `NinePHandler::write` uses).
    let attrs = match state
        .filesystem
        .write_ack(&auth, file, query.offset, &data)
        .await
    {
        Ok(attrs) => attrs,
        Err(error) => return upload_fs_error(error),
    };
    metrics::counter!("zerofs_http_upload_parts_total").increment(1);
    metrics::counter!("zerofs_http_upload_bytes_total").increment(data.len() as u64);
    upload_json(StatusCode::OK, serde_json::json!({ "size": attrs.size }))
}

async fn upload_status(
    State(state): State<AppState>,
    axum::extract::Path(path): axum::extract::Path<String>,
) -> axum::response::Response {
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

/// Size and lowercase-hex SHA-256 of the bytes the server holds for `id`,
/// read back through the filesystem's own read path.
async fn upload_hash_file(
    fs: &ZeroFS,
    auth: &AuthContext,
    id: InodeId,
) -> Result<(u64, String), FsError> {
    use sha2::Digest;
    let mut hasher = sha2::Sha256::new();
    let mut offset: u64 = 0;
    loop {
        let (chunk, eof) = fs
            .read_file(auth, id, offset, UPLOAD_COMMIT_HASH_CHUNK)
            .await?;
        hasher.update(&chunk);
        offset += chunk.len() as u64;
        if eof {
            break;
        }
    }
    use std::fmt::Write as _;
    let mut hex = String::with_capacity(64);
    for byte in hasher.finalize() {
        write!(hex, "{byte:02x}").expect("writing to a String cannot fail");
    }
    Ok((offset, hex))
}

async fn upload_commit(
    State(state): State<AppState>,
    axum::extract::Path(path): axum::extract::Path<String>,
    axum::extract::Json(request): axum::extract::Json<UploadCommitRequest>,
) -> axum::response::Response {
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
    let (staging_dir, file) =
        match upload_resolve_existing(&state.filesystem, &creds, &components).await {
            Ok(resolved) => resolved,
            Err(error) => return upload_fs_error(error),
        };

    // Durability first: drain this inode's RAM-acked overlay writes and wait
    // for the configured durability target — the barrier 9P Tfsync takes —
    // so a positive verify never describes bytes that can still be lost.
    if let Err(error) = state.filesystem.wait_inode_durability(file).await {
        return upload_fs_error(error);
    }

    let (actual_size, actual_sha256) = match upload_hash_file(&state.filesystem, &auth, file).await
    {
        Ok(hashed) => hashed,
        Err(error) => return upload_fs_error(error),
    };
    if actual_size != request.size || !actual_sha256.eq_ignore_ascii_case(&request.sha256) {
        metrics::counter!("zerofs_http_upload_verify_failures_total").increment(1);
        warn!(
            path,
            expected_size = request.size,
            actual_size,
            expected_sha256 = %request.sha256,
            actual_sha256 = %actual_sha256,
            "HTTP upload commit verification failed"
        );
        return upload_json(
            StatusCode::UNPROCESSABLE_ENTITY,
            serde_json::json!({
                "verified": false,
                "size": actual_size,
                "sha256": actual_sha256,
            }),
        );
    }

    let mut final_path = format!("/{path}");
    if let Some(publish_to) = &request.publish_to {
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
        let staging_name = &components[components.len() - 1];
        // The fs layer's atomic-promote primitive: `rename` moves the
        // verified file over the destination in one transaction.
        if let Err(error) = state
            .filesystem
            .rename(
                &auth,
                staging_dir,
                staging_name,
                publish_dir,
                &publish_name[0],
            )
            .await
        {
            return upload_fs_error(error);
        }
        // Second barrier so the rename itself is durable before we report
        // the publish as committed.
        if let Err(error) = state.filesystem.wait_inode_durability(file).await {
            return upload_fs_error(error);
        }
        final_path = format!("/{}", publish_to.trim_start_matches('/'));
    }

    metrics::counter!("zerofs_http_upload_commits_total").increment(1);
    info!(
        path = %final_path,
        size = actual_size,
        sha256 = %actual_sha256,
        "HTTP upload committed"
    );
    upload_json(
        StatusCode::OK,
        serde_json::json!({
            "verified": true,
            "size": actual_size,
            "sha256": actual_sha256,
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
        upload_permits: upload_permits(),
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
    use axum::http::StatusCode;
    use axum::routing::post;
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
                upload_permits: upload_permits(),
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
                upload_permits: upload_permits(),
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
                upload_permits: upload_permits(),
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
        Router::new().merge(upload_router()).with_state(AppState {
            filesystem,
            lock_manager: Arc::new(FileLockManager::new()),
            uid: 0,
            gid: 0,
            shutdown: CancellationToken::new(),
            ws_drain: TaskTracker::new(),
            accepted_work: P9AcceptedWorkTracker::new(),
            p9_idle_timeout: None,
            upload_permits: upload_permits(),
        })
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
