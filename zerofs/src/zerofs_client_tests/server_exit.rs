//! What an SDK caller sees when the 9P server process goes away.

use super::{connect_with_retry, start_server};
use crate::fs::ZeroFS;
use std::sync::Arc;
use std::time::Duration;
use zerofs_client::{Client, ZeroFsError};

/// Longer than the client's reconnect bound, so expiry means the request was
/// held without limit.
const NEVER: Duration = Duration::from_secs(45);

async fn wait_disconnected(client: &Client) {
    tokio::time::timeout(Duration::from_secs(5), async {
        while client.is_connected() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("the client never noticed the server leave");
}

#[tokio::test]
async fn a_read_after_the_server_exits_fails_instead_of_waiting_for_a_server() {
    let fs = Arc::new(ZeroFS::new_in_memory().await.unwrap());
    let dir = tempfile::tempdir().unwrap();
    let sock = dir.path().join("exit.9p.sock");
    let server = start_server(Arc::clone(&fs), sock.clone());
    let client = connect_with_retry(&sock).await;
    client.write("/a", b"payload").await.unwrap();

    server.cancel();
    wait_disconnected(&client).await;

    let error = tokio::time::timeout(NEVER, client.read("/a"))
        .await
        .unwrap_or_else(|_| panic!("a read was still blocked {NEVER:?} after the server exited"))
        .expect_err("no server answered the read");
    assert!(
        matches!(error, ZeroFsError::ConnectFailed { .. }),
        "{error}"
    );
}
