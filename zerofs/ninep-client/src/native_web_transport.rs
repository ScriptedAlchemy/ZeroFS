use crate::runtime;
use crate::{ClientError, ClientResult, Conn};
use futures::{SinkExt, StreamExt};
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;
use tokio::sync::{Notify, mpsc};
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async, tungstenite::Message};

const CONNECT_TIMEOUT: Duration = Duration::from_secs(3);

pub(super) struct WebSocketIo {
    stream: WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>,
}

pub(super) async fn connect(url: &str) -> ClientResult<WebSocketIo> {
    if !url.starts_with("ws://") {
        return Err(ClientError::Unexpected(
            "native websocket transport requires ws://",
        ));
    }
    let (stream, _) = runtime::timeout(CONNECT_TIMEOUT, connect_async(url))
        .await
        .map_err(|_| ClientError::Disconnected)?
        .map_err(|_| ClientError::Disconnected)?;
    Ok(WebSocketIo { stream })
}

pub(super) fn spawn(
    io: WebSocketIo,
    mut outgoing: mpsc::Receiver<Vec<u8>>,
    conn: Arc<Conn>,
    reconnect: Arc<Notify>,
) {
    let (mut writer, mut reader) = io.stream.split();
    let writer_conn = Arc::clone(&conn);
    let writer_reconnect = Arc::clone(&reconnect);
    runtime::spawn(async move {
        loop {
            let frame = tokio::select! {
                biased;
                _ = writer_conn.writer_shutdown.notified() => break,
                frame = outgoing.recv() => frame,
            };
            let Some(frame) = frame else { break };
            writer_conn
                .counters
                .bytes_sent
                .fetch_add(frame.len() as u64, Ordering::Relaxed);
            writer_conn
                .counters
                .operations
                .fetch_add(1, Ordering::Relaxed);
            if writer.send(Message::Binary(frame.into())).await.is_err() {
                break;
            }
        }
        let _ = writer.close().await;
        writer_conn.shutdown();
        writer_reconnect.notify_waiters();
    });

    runtime::spawn(async move {
        loop {
            let next = tokio::select! {
                biased;
                _ = conn.reader_shutdown.notified() => break,
                next = reader.next() => next,
            };
            match next {
                Some(Ok(Message::Binary(frame))) => conn.deliver(frame),
                Some(Ok(Message::Ping(_) | Message::Pong(_))) => {}
                Some(Ok(Message::Close(_))) | None | Some(Err(_)) => break,
                Some(Ok(_)) => {
                    conn.connection_lost(&reconnect);
                    return;
                }
            }
        }
        conn.connection_lost(&reconnect);
    });
}
