use super::super::copy::{stream_upload, sync_remote_files};
use super::*;
use futures::{SinkExt, StreamExt};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use tokio::task::JoinSet;
use tokio_tungstenite::tungstenite::Message as WebSocketMessage;
use zerofs_client::OpenOptions;

#[derive(Debug, Default)]
struct WriteReplyFault {
    writes_seen: AtomicUsize,
    fired: AtomicBool,
}

async fn run_websocket_fault_proxy(
    listener: tokio::net::TcpListener,
    upstream: String,
    fault: Arc<WriteReplyFault>,
    shutdown: CancellationToken,
) {
    let mut sessions = JoinSet::new();
    loop {
        tokio::select! {
            _ = shutdown.cancelled() => break,
            accepted = listener.accept() => {
                let Ok((downstream, _)) = accepted else { break };
                let upstream = upstream.clone();
                let fault = Arc::clone(&fault);
                sessions.spawn(async move {
                    let downstream = tokio_tungstenite::accept_async(downstream).await.unwrap();
                    let (upstream, _) = tokio_tungstenite::connect_async(&upstream).await.unwrap();
                    proxy_websocket_session(downstream, upstream, fault).await;
                });
            }
            joined = sessions.join_next(), if !sessions.is_empty() => {
                joined.expect("proxy session set became empty")
                    .expect("WebSocket fault-proxy session panicked");
            }
        }
    }
    sessions.abort_all();
    while sessions.join_next().await.is_some() {}
}

async fn proxy_websocket_session<Downstream, Upstream>(
    downstream: tokio_tungstenite::WebSocketStream<Downstream>,
    upstream: tokio_tungstenite::WebSocketStream<Upstream>,
    fault: Arc<WriteReplyFault>,
) where
    Downstream: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
    Upstream: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let (mut downstream_tx, mut downstream_rx) = downstream.split();
    let (mut upstream_tx, mut upstream_rx) = upstream.split();
    loop {
        tokio::select! {
            incoming = downstream_rx.next() => {
                let Some(Ok(message)) = incoming else { return };
                let drop_reply_tag = match &message {
                    WebSocketMessage::Binary(frame) => {
                        P9Message::from_bytes_ctx(frame, true).ok().and_then(|request| {
                            if matches!(request.body, Message::Twrite(_))
                                && fault.writes_seen.fetch_add(1, Ordering::AcqRel) == 0
                            {
                                Some(request.tag)
                            } else {
                                None
                            }
                        })
                    }
                    _ => None,
                };
                if upstream_tx.send(message).await.is_err() {
                    return;
                }
                let Some(drop_reply_tag) = drop_reply_tag else { continue };
                while let Some(Ok(response)) = upstream_rx.next().await {
                    let matching_reply = match &response {
                        WebSocketMessage::Binary(frame) => P9Message::from_bytes_ctx(frame, false)
                            .is_ok_and(|message| message.tag == drop_reply_tag),
                        _ => false,
                    };
                    if matching_reply {
                        fault.fired.store(true, Ordering::Release);
                        return;
                    }
                    if downstream_tx.send(response).await.is_err() {
                        return;
                    }
                }
                return;
            }
            incoming = upstream_rx.next() => {
                let Some(Ok(message)) = incoming else { return };
                if downstream_tx.send(message).await.is_err() {
                    return;
                }
            }
        }
    }
}

#[tokio::test]
async fn websocket_upload_rebinds_linked_temp_after_an_accepted_write_loses_its_reply() {
    let filesystem = Arc::new(ZeroFS::new_in_memory().await.unwrap());
    let inspect_filesystem = Arc::clone(&filesystem);
    let connections = Arc::new(AtomicUsize::new(0));
    let app = crate::webui::test_9p_websocket_router(filesystem, connections);
    let backend_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let backend_address = backend_listener.local_addr().unwrap();
    let backend = tokio::spawn(async move { axum::serve(backend_listener, app).await.unwrap() });

    let proxy_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let proxy_address = proxy_listener.local_addr().unwrap();
    let fault = Arc::new(WriteReplyFault::default());
    let proxy_shutdown = CancellationToken::new();
    let proxy = tokio::spawn(run_websocket_fault_proxy(
        proxy_listener,
        format!("ws://{backend_address}/ws/9p"),
        Arc::clone(&fault),
        proxy_shutdown.clone(),
    ));
    let target = format!("ws://{proxy_address}/ws/9p");
    let primary_client = connect_transfer_client(&target).await.unwrap();
    let secondary_client = connect_transfer_client(&target).await.unwrap();

    primary_client.create_dir_all("/dest", 0o755).await.unwrap();
    let primary_remote = primary_client
        .open(
            "/dest/.zerofs-reconnect.tmp",
            OpenOptions::write_only().create_new(true).mode(0o644),
        )
        .await
        .unwrap();
    let secondary_remote = secondary_client
        .open("/dest/.zerofs-reconnect.tmp", OpenOptions::write_only())
        .await
        .unwrap();
    let inode_id = primary_remote.metadata().await.unwrap().ino;
    let local = tempfile::tempdir().unwrap();
    let source = local.path().join("source.bin");
    let payload = (0..(64 * 1024 * 6))
        .map(|index| (index % 251) as u8)
        .collect::<Vec<_>>();
    fs::write(&source, &payload).unwrap();
    let planned = scan_local(&source).unwrap().files.pop().unwrap();
    let progress = Progress::new("upload", planned.size, 1);
    let file_progress = progress.start_file(Path::new("source.bin"), planned.size);
    let remotes = [primary_remote, secondary_remote];

    let upload = tokio::time::timeout(
        Duration::from_secs(10),
        stream_upload(
            &remotes,
            &planned,
            64 * 1024,
            &file_progress,
            &CancellationToken::new(),
        ),
    )
    .await
    .expect("upload did not recover from the lost WebSocket write reply");
    if let Err(error) = upload {
        let inode = inspect_filesystem.inode_store.get(inode_id).await;
        let path = inspect_filesystem
            .inode_store
            .resolve_path_components(inode_id)
            .await;
        panic!("upload replay failed: {error:#}; inode={inode:?}; path={path:?}");
    }
    sync_remote_files(&remotes).await.unwrap();
    assert!(fault.fired.load(Ordering::Acquire));
    assert_eq!(
        primary_client
            .read("/dest/.zerofs-reconnect.tmp")
            .await
            .unwrap(),
        payload
    );

    futures::future::join_all(remotes.iter().map(|remote| remote.close())).await;
    primary_client
        .remove_file("/dest/.zerofs-reconnect.tmp")
        .await
        .unwrap();
    close_client(&primary_client).await.unwrap();
    close_client(&secondary_client).await.unwrap();
    proxy_shutdown.cancel();
    proxy.await.unwrap();
    backend.abort();
    let _ = backend.await;
}
