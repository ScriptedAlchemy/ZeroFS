use super::*;
use futures::{SinkExt, StreamExt};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use tokio::task::JoinSet;
use tokio_tungstenite::tungstenite::Message as WebSocketMessage;
use zerofs_client::ConnectOptions;

#[derive(Debug, Default)]
struct WriteReplyFault {
    writes_seen: AtomicUsize,
    held_unsent_writes: AtomicUsize,
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
    while let Some(joined) = sessions.join_next().await {
        if let Err(error) = joined {
            assert!(
                error.is_cancelled(),
                "WebSocket fault-proxy session failed: {error}"
            );
        }
    }
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

                // The original failure requires two distinct ambiguous states:
                // the first write reached the server and lost its reply, while
                // a second write was accepted by the client transport but never
                // reached the server. Hold that second frame locally before
                // observing the first response so the regression cannot pass by
                // replaying only an already-accepted operation.
                let held = downstream_rx
                    .next()
                    .await
                    .expect("faulted upload did not pipeline a second write")
                    .expect("faulted upload's second write frame failed");
                let WebSocketMessage::Binary(held_frame) = held else {
                    panic!("faulted upload's second frame was not binary");
                };
                let held_request = P9Message::from_bytes_ctx(&held_frame, true)
                    .expect("faulted upload's second frame was not valid 9P");
                assert!(
                    matches!(held_request.body, Message::Twrite(_)),
                    "faulted upload's second frame was not Twrite"
                );
                fault.writes_seen.fetch_add(1, Ordering::AcqRel);
                fault.held_unsent_writes.fetch_add(1, Ordering::AcqRel);

                while let Some(Ok(response)) = upstream_rx.next().await {
                    let matching_reply = match &response {
                        WebSocketMessage::Binary(frame) => {
                            P9Message::from_bytes_ctx(frame, false).is_ok_and(|message| {
                                message.tag == drop_reply_tag
                                    && matches!(message.body, Message::Rwrite(_))
                            })
                        }
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
async fn websocket_upload_restarts_fresh_temp_after_an_unseen_write_retry() {
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
    let msize = 64 * 1024 + ninep_proto::P9_TWRITE_HDR + ninep_proto::P9_OP_ENVELOPE_LEN as u32;
    let connect_options = ConnectOptions {
        msize,
        ..ConnectOptions::default()
    };
    let primary_client = Client::connect_with(&target, connect_options.clone())
        .await
        .unwrap();
    let secondary_client = Client::connect_with(&target, connect_options)
        .await
        .unwrap();
    let clients = vec![Arc::clone(&primary_client), Arc::clone(&secondary_client)];
    let workers = vec![UploadWorker::new(&clients)];

    let local = tempfile::tempdir().unwrap();
    let local_path = local.path().to_path_buf();
    let source = local.path().join("source.bin");
    let payload = (0..(64 * 1024 * 6))
        .map(|index| (index % 251) as u8)
        .collect::<Vec<_>>();
    fs::write(&source, &payload).unwrap();
    let plan = scan_local(&source).unwrap();
    let baseline_files_created = inspect_filesystem
        .stats
        .files_created
        .load(Ordering::Relaxed);

    let upload = tokio::time::timeout(
        Duration::from_secs(10),
        execute_upload(
            &workers,
            plan,
            Path::new("/dest/reconnect.bin"),
            false,
            Progress::new("upload", payload.len() as u64, 1),
            CancellationToken::new(),
        ),
    )
    .await
    .expect("upload did not recover from the lost WebSocket write reply");
    if let Err(error) = upload {
        panic!("upload replay failed: {error:#}");
    }
    assert!(fault.fired.load(Ordering::Acquire));
    assert_eq!(fault.held_unsent_writes.load(Ordering::Acquire), 1);
    assert!(
        fault.writes_seen.load(Ordering::Acquire) > 6,
        "the proxy must observe retries in addition to six logical chunks"
    );
    assert_eq!(
        primary_client.read("/dest/reconnect.bin").await.unwrap(),
        payload
    );
    assert_eq!(
        inspect_filesystem
            .stats
            .files_created
            .load(Ordering::Relaxed)
            - baseline_files_created,
        2,
        "one stale private temporary file must be replaced by one fresh bounded retry"
    );
    let entries = primary_client.read_dir("/dest").await.unwrap();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].name, "reconnect.bin");

    primary_client
        .remove_file("/dest/reconnect.bin")
        .await
        .unwrap();
    primary_client.remove_dir("/dest").await.unwrap();
    super::super::finish_clients(&clients, Ok(()))
        .await
        .unwrap();
    proxy_shutdown.cancel();
    proxy.await.unwrap();
    backend.abort();
    let backend_error = backend.await.unwrap_err();
    assert!(backend_error.is_cancelled());
    inspect_filesystem.stop_new_mutation_admission();
    inspect_filesystem.stop_mutation_workers().await.unwrap();
    inspect_filesystem.flush_coordinator.close().await.unwrap();
    local.close().unwrap();
    assert!(!local_path.exists());
}
