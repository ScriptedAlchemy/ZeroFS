//! What an SDK caller sees when the 9P server process goes away.

use super::{connect_opts_with_retry, start_server};
use crate::fs::ZeroFS;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio_util::sync::CancellationToken;
use zerofs_client::{Client, ConnectOptions, ZeroFsError};

/// Longer than any reconnect bound these tests configure, so expiry means
/// the request was held without limit.
const NEVER: Duration = Duration::from_secs(45);
/// Scheduling slack on top of a configured bound.
const SLACK: Duration = Duration::from_secs(3);

struct Served {
    fs: Arc<ZeroFS>,
    _dir: tempfile::TempDir,
    sock: std::path::PathBuf,
    server: CancellationToken,
    client: Arc<Client>,
}

impl Served {
    async fn start(reconnect_timeout: Duration) -> Self {
        let fs = Arc::new(ZeroFS::new_in_memory().await.unwrap());
        let dir = tempfile::tempdir().unwrap();
        let sock = dir.path().join("exit.9p.sock");
        let server = start_server(Arc::clone(&fs), sock.clone());
        let opts = ConnectOptions {
            reconnect_timeout_ms: reconnect_timeout.as_millis() as u32,
            ..ConnectOptions::default()
        };
        let client = connect_opts_with_retry(&sock, opts).await;
        client.write("/a", b"payload").await.unwrap();
        Self {
            fs,
            _dir: dir,
            sock,
            server,
            client,
        }
    }

    /// Stops the server and waits until the client has noticed.
    async fn exit(&self) {
        self.server.cancel();
        tokio::time::timeout(Duration::from_secs(5), async {
            while self.client.is_connected() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("the client never noticed the server leave");
    }

    /// Starts a server on the same socket and filesystem and waits until it
    /// accepts connections.
    async fn restart(&mut self) {
        self.server = start_server(Arc::clone(&self.fs), self.sock.clone());
        tokio::time::timeout(Duration::from_secs(5), async {
            while tokio::net::UnixStream::connect(&self.sock).await.is_err() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("the restarted server never accepted a connection");
    }
}

async fn read_failing_within(client: &Client, bound: Duration) -> ZeroFsError {
    let started = Instant::now();
    let error = tokio::time::timeout(NEVER, client.read("/a"))
        .await
        .unwrap_or_else(|_| panic!("a read was still blocked {NEVER:?} after the server exited"))
        .expect_err("no server answered the read");
    assert!(
        started.elapsed() <= bound + SLACK,
        "the read failed after {:?}, past the {bound:?} reconnect bound",
        started.elapsed()
    );
    error
}

#[tokio::test]
async fn a_read_after_the_server_exits_fails_instead_of_waiting_for_a_server() {
    let bound = Duration::from_secs(2);
    let served = Served::start(bound).await;
    served.exit().await;

    let error = read_failing_within(&served.client, bound).await;
    assert!(
        matches!(error, ZeroFsError::ConnectFailed { .. }),
        "{error}"
    );
    assert_eq!(error.to_errno(), libc::EIO);
}

#[tokio::test]
async fn requests_succeed_again_once_a_server_returns_after_the_bound() {
    let bound = Duration::from_secs(2);
    let mut served = Served::start(bound).await;
    served.exit().await;
    read_failing_within(&served.client, bound).await;

    served.restart().await;
    let read = tokio::time::timeout(Duration::from_secs(10), served.client.read("/a"))
        .await
        .expect("the first read after the server returned never finished")
        .expect("the first read after the server returned must succeed");
    assert_eq!(read.as_ref(), b"payload");
}

#[tokio::test]
async fn a_restart_inside_the_bound_returns_no_error() {
    let mut served = Served::start(Duration::from_secs(20)).await;
    served.exit().await;

    let client = Arc::clone(&served.client);
    let read = tokio::spawn(async move { client.read("/a").await });
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(!read.is_finished(), "the read must wait for the restart");
    served.restart().await;

    let bytes = tokio::time::timeout(NEVER, read)
        .await
        .expect("the read held through a restart never finished")
        .unwrap()
        .expect("a restart inside the reconnect bound must not fail the read");
    assert_eq!(bytes.as_ref(), b"payload");
}

#[tokio::test]
async fn an_out_of_range_reconnect_timeout_is_rejected() {
    for reconnect_timeout_ms in [0, 120_001] {
        let opts = ConnectOptions {
            reconnect_timeout_ms,
            ..ConnectOptions::default()
        };
        let error = Client::connect_with("unix:/nonexistent/zerofs.9p.sock", opts)
            .await
            .expect_err("an out-of-range reconnect timeout must be rejected");
        assert!(
            matches!(error, ZeroFsError::InvalidArgument { .. }),
            "{reconnect_timeout_ms}: {error}"
        );
    }
}

/// `Client::write` closes its handle before it returns, so `Client::sync` is
/// the only fsync that can answer for it.
#[tokio::test]
async fn sync_reports_a_write_lost_after_its_handle_was_closed() {
    let on_lineage = |lineage| async move {
        let mut fs = ZeroFS::new_in_memory().await.unwrap();
        fs.lineage_token = lineage;
        Arc::new(fs)
    };
    // One server before and after it lost its unflushed writes.
    let (before, after) = (on_lineage(41).await, on_lineage(42).await);
    let dir = tempfile::tempdir().unwrap();
    let sock = dir.path().join("closed.9p.sock");
    let server = start_server(before, sock.clone());
    let client = connect_opts_with_retry(&sock, ConnectOptions::default()).await;

    client.write("/lost", b"payload").await.unwrap();
    // The handle is clunked and its number recycled.
    client.wait_for_cleanup().await;

    server.cancel();
    tokio::time::timeout(Duration::from_secs(5), async {
        while client.is_connected() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("the client never noticed the server leave");
    let _ = std::fs::remove_file(&sock);
    let _server = start_server(after, sock.clone());

    let error = tokio::time::timeout(Duration::from_secs(10), client.sync())
        .await
        .expect("sync timed out waiting for the reconnect")
        .expect_err("sync must report the write the server lost after its handle closed");
    assert!(matches!(error, ZeroFsError::Stale { .. }), "{error}");
    client
        .sync()
        .await
        .expect("the loss was reported twice with nothing written since");
}
