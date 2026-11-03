use super::super::{TRANSFER_CONNECT_CONCURRENCY, connect_workers};
use super::*;

#[tokio::test]
async fn transfer_workers_start_independent_handshakes_concurrently() {
    let (first, shutdown, local) = remote_client().await;
    let socket = local.path().join("transfer.9p.sock");
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let target = format!("tcp://{}", listener.local_addr().unwrap());
    let accepted = Arc::new(AtomicUsize::new(0));
    let accepted_changed = Arc::new(Notify::new());
    let release = Arc::new(tokio::sync::Semaphore::new(0));
    let proxy = tokio::spawn({
        let accepted = accepted.clone();
        let accepted_changed = accepted_changed.clone();
        let release = release.clone();
        async move {
            let mut relays = tokio::task::JoinSet::new();
            for _ in 0..2 {
                let (mut downstream, _) = listener.accept().await.unwrap();
                accepted.fetch_add(1, Ordering::SeqCst);
                accepted_changed.notify_one();
                let socket = socket.clone();
                let release = release.clone();
                relays.spawn(async move {
                    release.acquire().await.unwrap().forget();
                    let mut upstream = tokio::net::UnixStream::connect(socket).await.unwrap();
                    let _ = tokio::io::copy_bidirectional(&mut downstream, &mut upstream).await;
                });
            }
            drop(listener);
            while let Some(result) = relays.join_next().await {
                result.unwrap();
            }
        }
    });
    let connecting = tokio::spawn(async move { connect_workers(&target, first, 3).await });
    let overlapped = tokio::time::timeout(Duration::from_secs(2), async {
        while accepted.load(Ordering::SeqCst) < 2 {
            accepted_changed.notified().await;
        }
    })
    .await
    .is_ok();
    release.add_permits(2);
    let clients = tokio::time::timeout(Duration::from_secs(10), connecting)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(clients.len(), 3);
    for client in clients {
        client.close().await;
    }
    tokio::time::timeout(Duration::from_secs(2), proxy)
        .await
        .unwrap()
        .unwrap();
    shutdown.cancel();
    assert!(
        overlapped,
        "the second worker handshake waited for the first to finish"
    );
}

#[tokio::test]
async fn failed_worker_setup_settles_and_closes_every_started_connection() {
    let (first, shutdown, local) = remote_client().await;
    let socket = local.path().join("transfer.9p.sock");
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let target = format!("tcp://{}", listener.local_addr().unwrap());
    let accepted = Arc::new(AtomicUsize::new(0));
    let changed = Arc::new(Notify::new());
    let reject_first = Arc::new(tokio::sync::Semaphore::new(0));
    let release_peers = Arc::new(tokio::sync::Semaphore::new(0));
    let stop_proxy = CancellationToken::new();
    let (first_rejected, wait_for_rejection) = tokio::sync::oneshot::channel();
    let proxy = tokio::spawn({
        let accepted = accepted.clone();
        let changed = changed.clone();
        let reject_first = reject_first.clone();
        let release_peers = release_peers.clone();
        let stop_proxy = stop_proxy.clone();
        async move {
            let mut first_rejected = Some(first_rejected);
            let mut relays = tokio::task::JoinSet::new();
            loop {
                let (mut downstream, _) = tokio::select! {
                    _ = stop_proxy.cancelled() => break,
                    accepted = listener.accept() => accepted.unwrap(),
                };
                let index = accepted.fetch_add(1, Ordering::SeqCst);
                changed.notify_one();
                if index == 0 {
                    let reject = reject_first.clone();
                    let rejected = first_rejected.take().unwrap();
                    relays.spawn(async move {
                        reject.acquire().await.unwrap().forget();
                        drop(downstream);
                        let _ = rejected.send(());
                    });
                } else {
                    let socket = socket.clone();
                    let release = release_peers.clone();
                    relays.spawn(async move {
                        release.acquire().await.unwrap().forget();
                        let mut upstream = tokio::net::UnixStream::connect(socket).await.unwrap();
                        let _ = tokio::io::copy_bidirectional(&mut downstream, &mut upstream).await;
                    });
                }
            }
            drop(listener);
            while let Some(result) = relays.join_next().await {
                result.unwrap();
            }
        }
    });
    let count = 2 * TRANSFER_CONNECT_CONCURRENCY + 1;
    let mut connecting = tokio::spawn({
        let first = first.clone();
        async move { connect_workers(&target, first, count).await }
    });
    tokio::time::timeout(Duration::from_secs(2), async {
        while accepted.load(Ordering::SeqCst) < TRANSFER_CONNECT_CONCURRENCY {
            changed.notified().await;
        }
    })
    .await
    .expect("worker setup did not fill its bounded handshake window");
    let exceeded_limit = tokio::time::timeout(Duration::from_millis(50), async {
        while accepted.load(Ordering::SeqCst) == TRANSFER_CONNECT_CONCURRENCY {
            changed.notified().await;
        }
    })
    .await
    .is_ok();
    reject_first.add_permits(1);
    wait_for_rejection.await.unwrap();
    let early = tokio::time::timeout(Duration::from_millis(100), &mut connecting).await;
    let abandoned_peers = early.is_ok();
    release_peers.add_permits(count);
    let result = match early {
        Ok(result) => result,
        Err(_) => tokio::time::timeout(Duration::from_secs(10), connecting)
            .await
            .unwrap(),
    };
    let error = result.unwrap().unwrap_err();
    assert!(
        format!("{error:#}").contains("connect transfer worker"),
        "{error:#}"
    );
    assert!(matches!(
        first.stat("/").await.unwrap_err(),
        zerofs_client::ZeroFsError::Closed
    ));
    stop_proxy.cancel();
    tokio::time::timeout(Duration::from_secs(2), proxy)
        .await
        .expect("successful peer connections were left running after setup failed")
        .unwrap();
    shutdown.cancel();
    assert!(!exceeded_limit, "worker handshakes exceeded their limit");
    assert!(
        !abandoned_peers,
        "setup returned before its pending peers settled"
    );
    assert_eq!(
        accepted.load(Ordering::SeqCst),
        TRANSFER_CONNECT_CONCURRENCY,
        "new worker connections started after setup had failed"
    );
}
