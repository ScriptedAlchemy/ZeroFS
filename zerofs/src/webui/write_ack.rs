//! Optional, restart-only settings editor. The gRPC-web service has no write route for these fields.
use crate::cli::memory_budget::validate_server_startup;
use crate::config::Settings;
use crate::config::write_ack::{self, Document, Fields};
use crate::fs::mutation::config::FilesystemWriteAckSettings;
use crate::writeback::config::WritebackAccessMode;
use anyhow::{Context, Result, ensure};
use axum::{
    Json, Router,
    extract::{Request, State},
    http::{HeaderMap, StatusCode, Uri, header},
    routing::get,
};
use std::{
    path::{Path, PathBuf},
    sync::Arc,
};
use subtle::ConstantTimeEq;

const TOKEN_ENV: &str = "ZEROFS_WRITE_ACK_ADMIN_TOKEN_FILE";
const BODY_READ_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);
type Failure = (StatusCode, &'static str);
type Response = Result<([(header::HeaderName, &'static str); 1], Json<Status>), Failure>;

#[derive(serde::Serialize)]
struct Status {
    active: Fields,
    desired: Fields,
    activation: &'static str,
    pending_restart: bool,
    editable: bool,
}

struct Editor {
    config: PathBuf,
    path: Option<PathBuf>,
    token: Option<[u8; 64]>,
    active: Fields,
    io: Arc<tokio::sync::Semaphore>,
}

pub(crate) fn router(
    config: &Path,
    settings: &Settings,
    ack: FilesystemWriteAckSettings,
    read_write: bool,
) -> Result<Router> {
    let path = std::env::var_os(write_ack::FILE_ENV).map(PathBuf::from);
    let token_path = std::env::var_os(TOKEN_ENV).map(PathBuf::from);
    let token = match (&path, token_path) {
        (Some(path), Some(secret)) if read_write => {
            ensure!(
                path != &secret,
                "token and write-ack document must have distinct paths"
            );
            write_ack::check_path(settings, &secret, config)?;
            let bytes =
                write_ack::private_bytes(&secret, 65)?.context("missing private settings token")?;
            let bytes = bytes.strip_suffix(b"\n").unwrap_or(&bytes);
            ensure!(
                bytes.len() == 64 && bytes.iter().all(u8::is_ascii_hexdigit),
                "settings token must contain 64 hexadecimal characters"
            );
            Some(bytes.try_into()?)
        }
        _ => None,
    };
    Ok(routes(Editor {
        config: config.into(),
        path,
        token,
        active: Fields::effective(ack),
        io: Arc::new(tokio::sync::Semaphore::new(1)),
    }))
}

fn routes(editor: Editor) -> Router {
    Router::new()
        .route("/api/settings/write-ack", get(read).put(save))
        .with_state(Arc::new(editor))
}

impl Editor {
    fn status(&self, desired: Fields) -> Response {
        Ok((
            [(header::CACHE_CONTROL, "no-store")],
            Json(Status {
                active: self.active.clone(),
                pending_restart: desired != self.active,
                desired,
                activation: "restart",
                editable: self.token.is_some(),
            }),
        ))
    }

    fn desired(&self) -> Result<Fields> {
        if self.path.is_none() {
            return Ok(self.active.clone());
        }
        let settings =
            Settings::from_runtime_file_with_override(&self.config, self.path.as_deref())?;
        Ok(Fields::effective(settings.filesystem_write_ack_settings(
            WritebackAccessMode::ReadWrite,
        )?))
    }
}

async fn read(State(editor): State<Arc<Editor>>) -> Response {
    if editor.path.is_none() {
        return editor.status(editor.active.clone());
    }
    let permit = editor.io.clone().try_acquire_owned().map_err(|_| {
        (
            StatusCode::TOO_MANY_REQUESTS,
            "Settings request in progress",
        )
    })?;
    tokio::task::spawn_blocking(move || {
        let _permit = permit;
        let desired = editor.desired().map_err(|_| {
            (
                StatusCode::SERVICE_UNAVAILABLE,
                "Desired settings could not be read",
            )
        })?;
        editor.status(desired)
    })
    .await
    .map_err(|_| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            "Settings read interrupted",
        )
    })?
}

fn same_origin(headers: &HeaderMap) -> bool {
    if headers
        .get("sec-fetch-site")
        .is_some_and(|v| v != "same-origin")
    {
        return false;
    }
    let Some(origin) = headers.get(header::ORIGIN) else {
        return true;
    };
    let Some(uri) = origin.to_str().ok().and_then(|v| v.parse::<Uri>().ok()) else {
        return false;
    };
    (uri.scheme_str() == Some("http")
        || (uri.scheme_str() == Some("https")
            && headers
                .get("sec-fetch-site")
                .is_some_and(|v| v == "same-origin")))
        && uri.authority().is_some_and(|a| {
            headers
                .get(header::HOST)
                .is_some_and(|h| h.as_bytes().eq_ignore_ascii_case(a.as_str().as_bytes()))
        })
        && matches!(uri.path_and_query().map(|p| p.as_str()), None | Some("/"))
}

async fn save(State(editor): State<Arc<Editor>>, request: Request) -> Response {
    let expected = editor
        .token
        .as_ref()
        .ok_or((StatusCode::FORBIDDEN, "Settings editing is disabled"))?;
    let headers = request.headers();
    let supplied = headers
        .get(header::AUTHORIZATION)
        .and_then(|v| v.as_bytes().strip_prefix(b"Bearer "));
    if !supplied.is_some_and(|v| bool::from(expected.as_slice().ct_eq(v))) {
        return Err((StatusCode::UNAUTHORIZED, "Administrator token required"));
    }
    if !same_origin(headers) {
        return Err((StatusCode::FORBIDDEN, "Cross-origin edit refused"));
    }
    if !headers
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| {
            v.split(';')
                .next()
                .is_some_and(|m| m.trim().eq_ignore_ascii_case("application/json"))
        })
    {
        return Err((StatusCode::UNSUPPORTED_MEDIA_TYPE, "Use application/json"));
    }
    let permit = editor.io.clone().try_acquire_owned().map_err(|_| {
        (
            StatusCode::TOO_MANY_REQUESTS,
            "Settings request in progress",
        )
    })?;
    let bytes = tokio::time::timeout(
        BODY_READ_TIMEOUT,
        axum::body::to_bytes(request.into_body(), write_ack::MAX_DOCUMENT),
    )
    .await
    .map_err(|_| (StatusCode::REQUEST_TIMEOUT, "Settings body timed out"))?
    .map_err(|_| {
        (
            StatusCode::PAYLOAD_TOO_LARGE,
            "Settings document exceeds its size limit",
        )
    })?;
    let doc: Document = serde_json::from_slice(&bytes)
        .map_err(|_| (StatusCode::BAD_REQUEST, "Invalid settings document"))?;
    tokio::task::spawn_blocking(move || {
        // Keep validation, replacement and the reply serialized even if the HTTP caller disconnects.
        let _permit = permit;
        let path = editor
            .path
            .as_ref()
            .ok_or((StatusCode::FORBIDDEN, "Settings editing is disabled"))?;
        let mut settings = Settings::from_file(&editor.config).map_err(|_| {
            (
                StatusCode::SERVICE_UNAVAILABLE,
                "Base configuration is unavailable",
            )
        })?;
        write_ack::check_path(&settings, path, &editor.config)
            .and_then(|_| write_ack::read_document(path))
            .map_err(|_| {
                (
                    StatusCode::SERVICE_UNAVAILABLE,
                    "Private settings file unavailable",
                )
            })?;
        doc.apply(&mut settings)
            .and_then(|_| validate_server_startup(&settings))
            .map_err(|_| {
                (
                    StatusCode::UNPROCESSABLE_ENTITY,
                    "Settings exceed the allowed bounds or startup memory envelope",
                )
            })?;
        write_ack::persist(path, &doc).map_err(|_| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                "Settings could not be saved durably",
            )
        })?;
        editor.status(doc.filesystem)
    })
    .await
    .map_err(|_| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            "Settings save interrupted",
        )
    })?
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::{Body, to_bytes};
    use axum::http::Method;
    use serde_json::{Value, json};
    use std::os::unix::fs::PermissionsExt;
    use tower::ServiceExt;

    fn base(root: &Path) -> PathBuf {
        let path = root.join("engine.toml");
        let config = include_str!("../../../scripts/fixtures/nas-journal/engine.toml.in")
            .replace("@ROOT@", root.to_str().unwrap())
            .replace("@CACHE@", root.join("cache").to_str().unwrap())
            .replace("@JOURNAL@", root.join("journal").to_str().unwrap())
            .replace("@MEMBER_A@", root.join("a").to_str().unwrap())
            .replace("@MEMBER_B@", root.join("b").to_str().unwrap())
            .replace("@NINEP@", root.join("9p.sock").to_str().unwrap())
            .replace("@RPC@", root.join("rpc.sock").to_str().unwrap())
            .replace("@WEBUI@", "127.0.0.1:8088")
            .replace("@METRICS@", "127.0.0.1:0");
        std::fs::write(&path, config).unwrap();
        path
    }

    fn instance(config: &Path, path: &Path, token: Option<[u8; 64]>) -> Router {
        let active = Settings::from_runtime_file_with_override(config, Some(path))
            .unwrap()
            .filesystem_write_ack_settings(WritebackAccessMode::ReadWrite)
            .unwrap();
        routes(Editor {
            config: config.to_owned(),
            path: Some(path.to_owned()),
            token,
            active: Fields::effective(active),
            io: Arc::new(tokio::sync::Semaphore::new(1)),
        })
    }

    async fn request(
        router: &Router,
        method: Method,
        token: &str,
        origin: &str,
        body: Value,
    ) -> (StatusCode, Vec<u8>) {
        let req = Request::builder()
            .method(method)
            .uri("/api/settings/write-ack")
            .header(header::CONTENT_TYPE, "application/json")
            .header(header::HOST, "localhost:8088")
            .header(header::ORIGIN, origin)
            .header(header::AUTHORIZATION, format!("Bearer {token}"))
            .body(Body::from(body.to_string()))
            .unwrap();
        let response = router.clone().oneshot(req).await.unwrap();
        let code = response.status();
        let bytes = to_bytes(response.into_body(), 4096).await.unwrap();
        (code, bytes.to_vec())
    }

    #[tokio::test(start_paused = true)]
    async fn stalled_body_releases_editor_admission_without_saving() {
        use std::sync::atomic::{AtomicBool, Ordering};
        use std::time::Duration;

        let root = tempfile::tempdir().unwrap();
        let root_path = root.path().canonicalize().unwrap();
        let private = root_path.join("private");
        std::fs::create_dir(&private).unwrap();
        std::fs::set_permissions(&private, std::fs::Permissions::from_mode(0o700)).unwrap();
        let config = base(&root_path);
        let path = private.join("ack.json");
        let token = "a".repeat(64);
        let router = instance(&config, &path, Some([b'a'; 64]));
        let polled = Arc::new(AtomicBool::new(false));
        let seen = Arc::clone(&polled);
        let stalled = Body::from_stream(futures::stream::poll_fn(move |_| {
            seen.store(true, Ordering::Release);
            std::task::Poll::<Option<Result<bytes::Bytes, std::io::Error>>>::Pending
        }));
        let req = Request::builder()
            .method(Method::PUT)
            .uri("/api/settings/write-ack")
            .header(header::CONTENT_TYPE, "application/json")
            .header(header::AUTHORIZATION, format!("Bearer {token}"))
            .body(stalled)
            .unwrap();
        let saving = tokio::spawn(router.clone().oneshot(req));
        while !polled.load(Ordering::Acquire) {
            tokio::task::yield_now().await;
        }
        let same = "http://localhost:8088";
        assert_eq!(
            request(&router, Method::GET, "", same, Value::Null).await.0,
            StatusCode::TOO_MANY_REQUESTS
        );

        tokio::time::advance(Duration::from_secs(6)).await;
        let response = tokio::time::timeout(Duration::from_secs(1), saving)
            .await
            .expect("an incomplete body must not retain the editor admission indefinitely")
            .unwrap()
            .unwrap();
        assert_eq!(response.status(), StatusCode::REQUEST_TIMEOUT);
        assert!(!path.exists());
        assert_eq!(
            request(&router, Method::GET, "", same, Value::Null).await.0,
            StatusCode::OK
        );
        let body = json!({"version":1,"filesystem":{"write_ack_mode":"materialized"}});
        assert_eq!(
            request(&router, Method::PUT, &token, same, body.clone())
                .await
                .0,
            StatusCode::OK
        );
        assert_eq!(
            serde_json::from_slice::<Value>(&std::fs::read(&path).unwrap()).unwrap(),
            body
        );
    }

    #[tokio::test]
    async fn only_authorized_valid_edits_survive_restart() {
        let root = tempfile::tempdir().unwrap();
        let root_path = root.path().canonicalize().unwrap();
        let private = root_path.join("private");
        std::fs::create_dir(&private).unwrap();
        std::fs::set_permissions(&private, std::fs::Permissions::from_mode(0o700)).unwrap();
        let config = base(&root_path);
        let path = private.join("ack.json");
        let token = "a".repeat(64);
        let enabled = instance(&config, &path, Some([b'a'; 64]));
        let disabled = instance(&config, &path, None);
        let same = "http://localhost:8088";
        let fields = json!({"write_ack_mode":"volatile_memory","volatile_memory_gb":0.016777216,"volatile_max_operations":1});
        let body = json!({"version":1,"filesystem":fields});
        for (router, credential, origin, status) in [
            (&disabled, token.as_str(), same, StatusCode::FORBIDDEN),
            (&enabled, "", same, StatusCode::UNAUTHORIZED),
            (&enabled, "wrong", same, StatusCode::UNAUTHORIZED),
            (
                &enabled,
                token.as_str(),
                "https://other.example",
                StatusCode::FORBIDDEN,
            ),
        ] {
            assert_eq!(
                request(router, Method::PUT, credential, origin, body.clone())
                    .await
                    .0,
                status
            );
            assert!(!path.exists());
        }
        for invalid in [
            json!({"version":2,"filesystem":fields}),
            json!({"version":1,"filesystem":{"write_ack_mode":"volatile_memory","volatile_memory_gb":0.015,"volatile_max_operations":1}}),
            json!({"version":1,"filesystem":{"write_ack_mode":"materialized","volatile_max_operations":1}}),
            json!({"version":1,"filesystem":fields,"cache":{}}),
        ] {
            assert!(
                request(&enabled, Method::PUT, &token, same, invalid)
                    .await
                    .0
                    .is_client_error()
            );
            assert!(!path.exists());
        }
        let (code, bytes) = request(&enabled, Method::PUT, &token, same, body.clone()).await;
        assert_eq!(code, StatusCode::OK, "{}", String::from_utf8_lossy(&bytes));
        let saved: Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(saved["active"]["write_ack_mode"], "materialized");
        assert_eq!(saved["desired"], fields);
        assert_eq!(saved["pending_restart"], true);
        assert_eq!(
            serde_json::from_slice::<Value>(&std::fs::read(&path).unwrap()).unwrap(),
            body
        );
        assert_eq!(path.metadata().unwrap().permissions().mode() & 0o777, 0o600);
        let restarted = instance(&config, &path, None);
        let (code, bytes) = request(&restarted, Method::GET, "", same, Value::Null).await;
        assert_eq!(code, StatusCode::OK);
        let status: Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(status["active"], fields);
        assert_eq!(status["pending_restart"], false);
        assert_eq!(status["editable"], false);
    }
}
