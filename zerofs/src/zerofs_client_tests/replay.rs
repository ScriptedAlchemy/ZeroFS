use super::{connect_with_retry, start_server};
use crate::fs::ZeroFS;
use ninep_proto::{Message, P9_SIZE_FIELD_LEN, P9Message};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use zerofs_client::{Client, OpenOptions, ZeroFsError};

#[derive(Debug, Default)]
struct LostWriteReply {
    fired: AtomicBool,
    tag: std::sync::Mutex<Option<u16>>,
    applied: tokio::sync::Notify,
    release: tokio::sync::Notify,
}

async fn read_frame<R>(reader: &mut R) -> std::io::Result<Option<Vec<u8>>>
where
    R: AsyncRead + Unpin,
{
    let mut size = [0u8; P9_SIZE_FIELD_LEN];
    match reader.read_exact(&mut size).await {
        Ok(_) => {}
        Err(error) if error.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(error) => return Err(error),
    }
    let frame_len = u32::from_le_bytes(size) as usize;
    if frame_len < P9_SIZE_FIELD_LEN {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "9P frame shorter than its size field",
        ));
    }
    let mut frame = Vec::with_capacity(frame_len);
    frame.extend_from_slice(&size);
    frame.resize(frame_len, 0);
    reader.read_exact(&mut frame[P9_SIZE_FIELD_LEN..]).await?;
    Ok(Some(frame))
}

async fn proxy_session(
    downstream: tokio::net::TcpStream,
    upstream: tokio::net::UnixStream,
    fault: Arc<LostWriteReply>,
) -> std::io::Result<()> {
    let (mut downstream_read, mut downstream_write) = downstream.into_split();
    let (mut upstream_read, mut upstream_write) = upstream.into_split();

    let request_fault = Arc::clone(&fault);
    let requests = async move {
        loop {
            let Some(request) = read_frame(&mut downstream_read).await? else {
                return Ok(());
            };
            if let Ok(message) = P9Message::from_bytes_ctx(&request, true)
                && matches!(message.body, Message::Twrite(_))
                && !request_fault.fired.swap(true, Ordering::AcqRel)
            {
                *request_fault.tag.lock().unwrap() = Some(message.tag);
            }
            upstream_write.write_all(&request).await?;
        }
    };

    let responses = async move {
        loop {
            let Some(response) = read_frame(&mut upstream_read).await? else {
                return Ok(());
            };
            let lose_reply = P9Message::from_bytes_ctx(&response, false).is_ok_and(|message| {
                if !matches!(message.body, Message::Rwrite(_)) {
                    return false;
                }
                let mut lost_tag = fault.tag.lock().unwrap();
                if *lost_tag != Some(message.tag) {
                    return false;
                }
                lost_tag.take();
                true
            });
            if lose_reply {
                fault.applied.notify_one();
                fault.release.notified().await;
                return Ok(());
            }
            downstream_write.write_all(&response).await?;
        }
    };

    tokio::select! {
        result = requests => result,
        result = responses => result,
    }
}

async fn run_fault_proxy(
    listener: tokio::net::TcpListener,
    backend: std::path::PathBuf,
    fault: Arc<LostWriteReply>,
    shutdown: CancellationToken,
) {
    let mut sessions = JoinSet::new();
    loop {
        tokio::select! {
            _ = shutdown.cancelled() => break,
            accepted = listener.accept() => {
                let Ok((downstream, _)) = accepted else { break };
                let upstream = tokio::time::timeout(
                    std::time::Duration::from_secs(2),
                    tokio::net::UnixStream::connect(&backend),
                ).await;
                let Ok(Ok(upstream)) = upstream else {
                    break;
                };
                let fault = Arc::clone(&fault);
                sessions.spawn(async move {
                    proxy_session(downstream, upstream, fault).await.unwrap();
                });
            }
            joined = sessions.join_next(), if !sessions.is_empty() => {
                joined.expect("proxy session set became empty")
                    .expect("9P fault-proxy session panicked");
            }
        }
    }
    sessions.abort_all();
    while let Some(joined) = sessions.join_next().await {
        if let Err(error) = joined {
            assert!(
                error.is_cancelled(),
                "9P fault-proxy session failed: {error}"
            );
        }
    }
}

#[tokio::test]
async fn lost_write_reply_cannot_overwrite_a_later_fsynced_write_after_replay_state_loss() {
    let filesystem = Arc::new(ZeroFS::new_in_memory().await.unwrap());
    assert!(
        !filesystem.ignore_fsync,
        "the regression requires a real filesystem durability barrier"
    );
    let temp = tempfile::tempdir().unwrap();
    let socket = temp.path().join("replay.9p.sock");
    let server_shutdown = start_server(Arc::clone(&filesystem), socket.clone());
    let direct = connect_with_retry(&socket).await;
    let later = direct.open("/data.bin", OpenOptions::write_only().create_new(true));
    let later = tokio::time::timeout(std::time::Duration::from_secs(2), later)
        .await
        .expect("create of the test file timed out")
        .unwrap();

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let target = format!("tcp://{}", listener.local_addr().unwrap());
    let fault = Arc::new(LostWriteReply::default());
    let proxy_shutdown = CancellationToken::new();
    let proxy = tokio::spawn(run_fault_proxy(
        listener,
        socket,
        Arc::clone(&fault),
        proxy_shutdown.clone(),
    ));
    let ambiguous =
        tokio::time::timeout(std::time::Duration::from_secs(2), Client::connect(&target))
            .await
            .expect("fault-proxy client connect timed out")
            .unwrap();
    let earlier = tokio::time::timeout(
        std::time::Duration::from_secs(2),
        ambiguous.open("/data.bin", OpenOptions::write_only()),
    )
    .await
    .expect("open through the fault proxy timed out")
    .unwrap();

    let earlier_write = Arc::clone(&earlier);
    let write = tokio::spawn(async move { earlier_write.write_at(0, b"AAAA").await });
    tokio::time::timeout(std::time::Duration::from_secs(2), fault.applied.notified())
        .await
        .expect("the server did not apply the write whose reply was lost");

    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        later.write_at(0, b"BBBB").await.unwrap();
        later.sync_all().await.unwrap();
    })
    .await
    .expect("the later write and fsync timed out");
    filesystem.dedup.clear_replay_state_for_test();
    fault.release.notify_one();

    let error = tokio::time::timeout(std::time::Duration::from_secs(5), write)
        .await
        .expect("ambiguous write did not settle after reconnect")
        .expect("ambiguous write task panicked")
        .expect_err("lost replay state must reject an ambiguous write retry");
    assert!(matches!(error, ZeroFsError::Stale { .. }), "{error}");
    let survived =
        tokio::time::timeout(std::time::Duration::from_secs(2), direct.read("/data.bin"))
            .await
            .expect("read after the ambiguous retry timed out")
            .unwrap();
    assert_eq!(survived.as_ref(), b"BBBB");

    earlier.close().await;
    later.close().await;
    proxy_shutdown.cancel();
    tokio::time::timeout(std::time::Duration::from_secs(2), proxy)
        .await
        .expect("fault proxy did not stop")
        .unwrap();
    server_shutdown.cancel();
}
