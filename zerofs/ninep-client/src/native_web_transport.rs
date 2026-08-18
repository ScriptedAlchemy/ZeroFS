use crate::runtime;
use crate::write_progress::{TrackedIo, WriteOutcome, WriteProgress, wait_for_write};
use crate::{ClientError, ClientResult, Conn, OutboundFrame, configure_tcp_socket};
use futures::{SinkExt, StreamExt};
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;
use tokio::sync::{Notify, mpsc};
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::{WebSocketStream, client_async, tungstenite::Message};

const CONNECT_TIMEOUT: Duration = Duration::from_secs(3);

pub(super) struct WebSocketIo {
    stream: WebSocketStream<TrackedIo<tokio::net::TcpStream>>,
    progress: WriteProgress,
}

pub(super) async fn connect(url: &str) -> ClientResult<WebSocketIo> {
    if !url.starts_with("ws://") {
        return Err(ClientError::Unexpected(
            "native websocket transport requires ws://",
        ));
    }
    let request = url
        .into_client_request()
        .map_err(|_| ClientError::Disconnected)?;
    let uri_host = request.uri().host().ok_or(ClientError::Disconnected)?;
    let host = uri_host
        .strip_prefix('[')
        .and_then(|host| host.strip_suffix(']'))
        .unwrap_or(uri_host)
        .to_owned();
    let port = request.uri().port_u16().unwrap_or(80);
    let connected = runtime::timeout(CONNECT_TIMEOUT, async move {
        let socket = tokio::net::TcpStream::connect((host.as_str(), port))
            .await
            .map_err(|_| ClientError::Disconnected)?;
        configure_tcp_socket(&socket);
        let (socket, progress) = TrackedIo::new(socket);
        let (stream, _) = client_async(request, socket)
            .await
            .map_err(|_| ClientError::Disconnected)?;
        Ok::<_, ClientError>((stream, progress))
    })
    .await
    .map_err(|_| ClientError::Disconnected)?;
    let (stream, progress) = connected?;
    Ok(WebSocketIo { stream, progress })
}

pub(super) fn spawn(
    io: WebSocketIo,
    mut outgoing: mpsc::Receiver<OutboundFrame>,
    conn: Arc<Conn>,
    reconnect: Arc<Notify>,
) {
    let WebSocketIo { stream, progress } = io;
    let (mut writer, mut reader) = stream.split();
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
                .fetch_add(frame.bytes.len() as u64, Ordering::Relaxed);
            writer_conn
                .counters
                .operations
                .fetch_add(1, Ordering::Relaxed);
            if !matches!(
                wait_for_write(
                    writer.send(Message::Binary(frame.bytes.into())),
                    &progress,
                    &writer_conn.writer_shutdown,
                )
                .await,
                WriteOutcome::Completed(Ok(())),
            ) {
                break;
            }
            let _ = frame.sent.send(());
        }
        let _ = runtime::timeout(CONNECT_TIMEOUT, writer.close()).await;
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
