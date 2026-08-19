use super::plan::{PlannedFile, TransferPlan, scan_local, scan_remote};
use super::progress::{DeleteProgress, Progress};
#[cfg(feature = "webui")]
use super::run_upload;
use super::{
    InterruptPolicy, SETTLE_NOTICE_INTERVAL, TRANSFER_MSIZE, UploadWorker, close_client,
    connect_transfer_client, execute_delete, execute_download, execute_upload,
    resume_enabled_for_attempt, run_file_workers, with_settling_notices,
};
use crate::cli::attach_cleanup_errors;
use crate::fs::ZeroFS;
use crate::ninep::NinePServer;
use std::fs;
use std::os::unix::fs::symlink;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;
use zerofs_client::Client;

mod resume;
mod safety;
mod throughput;

async fn remote_client() -> (Arc<Client>, CancellationToken, tempfile::TempDir) {
    let (client, shutdown, temp, _) = remote_client_with_filesystem().await;
    (client, shutdown, temp)
}

async fn remote_client_with_filesystem() -> (
    Arc<Client>,
    CancellationToken,
    tempfile::TempDir,
    Arc<ZeroFS>,
) {
    let filesystem = Arc::new(ZeroFS::new_in_memory().await.unwrap());
    let temp = tempfile::tempdir().unwrap();
    let socket = temp.path().join("transfer.9p.sock");
    let server = NinePServer::new_unix(Arc::clone(&filesystem), socket.clone());
    let shutdown = CancellationToken::new();
    let server_shutdown = shutdown.clone();
    tokio::spawn(async move { server.start(server_shutdown).await.unwrap() });
    let target = format!("unix:{}", socket.display());
    for _ in 0..100 {
        if let Ok(client) = connect_transfer_client(&target).await {
            return (client, shutdown, temp, filesystem);
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("test 9P client did not connect");
}

#[tokio::test]
async fn transfer_clients_negotiate_nine_mibibyte_write_payloads() {
    let (client, _shutdown, _local) = remote_client().await;
    assert_eq!(client.capabilities().msize, TRANSFER_MSIZE);
    assert_eq!(client.capabilities().max_write_chunk, 9 * 1024 * 1024);
}

async fn quiesced_fids(client: &Client) -> usize {
    let mut previous = client.outstanding_fids();
    for _ in 0..100 {
        tokio::time::sleep(Duration::from_millis(10)).await;
        let current = client.outstanding_fids();
        if current == previous {
            return current;
        }
        previous = current;
    }
    previous
}

fn upload_workers(clients: &[Arc<Client>]) -> Vec<UploadWorker> {
    clients
        .iter()
        .map(|client| UploadWorker::new(std::slice::from_ref(client)))
        .collect()
}

#[tokio::test]
async fn file_failures_do_not_cancel_remaining_files() {
    let (client, _shutdown, _local) = remote_client().await;
    let clients = vec![client];
    let files = vec![
        PlannedFile {
            source: "first".into(),
            relative: "first".into(),
            size: 1,
        },
        PlannedFile {
            source: "second".into(),
            relative: "second".into(),
            size: 1,
        },
        PlannedFile {
            source: "third".into(),
            relative: "third".into(),
            size: 1,
        },
    ];
    let calls = Arc::new(AtomicUsize::new(0));
    let transfer_calls = Arc::clone(&calls);

    let error = run_file_workers(
        &clients,
        files,
        Progress::new("upload", 3, 3),
        CancellationToken::new(),
        move |_client, file, _cancellation, _attempt| {
            let calls = Arc::clone(&transfer_calls);
            async move {
                calls.fetch_add(1, Ordering::Relaxed);
                if file.relative != Path::new("third") {
                    anyhow::bail!("{} failed", file.relative.display());
                }
                Ok(())
            }
        },
    )
    .await
    .unwrap_err();

    let message = error.to_string();
    assert!(message.contains("2 file transfers failed"), "{message}");
    assert!(message.contains("first failed"), "{message}");
    assert!(message.contains("second failed"), "{message}");
    assert_eq!(calls.load(Ordering::Relaxed), 3);
}

#[tokio::test(start_paused = true)]
async fn cleanup_failure_is_terminal_instead_of_being_hidden_by_a_retry() {
    let (client, _shutdown, _local) = remote_client().await;
    let clients = vec![client];
    let files = vec![PlannedFile {
        source: "book.m4b".into(),
        relative: "book.m4b".into(),
        size: 1,
    }];
    let calls = Arc::new(AtomicUsize::new(0));
    let transfer_calls = Arc::clone(&calls);

    let error = run_file_workers(
        &clients,
        files,
        Progress::new("upload", 1, 1),
        CancellationToken::new(),
        move |_client, _file, _cancellation, _attempt| {
            let attempt = transfer_calls.fetch_add(1, Ordering::Relaxed);
            async move {
                if attempt == 0 {
                    return Err(attach_cleanup_errors(
                        zerofs_client::ZeroFsError::Stale {
                            path: "book.m4b".into(),
                        }
                        .into(),
                        vec![anyhow::anyhow!("temporary file removal failed")],
                    ));
                }
                Ok(())
            }
        },
    )
    .await
    .unwrap_err();

    assert_eq!(calls.load(Ordering::Relaxed), 1);
    assert!(
        error.to_string().contains("temporary file removal failed"),
        "{error:#}"
    );
}

#[tokio::test(start_paused = true)]
async fn transient_file_failure_is_retried_from_the_file_boundary() {
    let (client, _shutdown, _local) = remote_client().await;
    let clients = vec![client];
    let files = vec![PlannedFile {
        source: "book.m4b".into(),
        relative: "book.m4b".into(),
        size: 1,
    }];
    let calls = Arc::new(AtomicUsize::new(0));
    let transfer_calls = Arc::clone(&calls);
    let attempts = Arc::new(tokio::sync::Mutex::new(Vec::new()));
    let transfer_attempts = Arc::clone(&attempts);

    run_file_workers(
        &clients,
        files,
        Progress::new("upload", 1, 1),
        CancellationToken::new(),
        move |_client, _file, _cancellation, attempt| {
            let call = transfer_calls.fetch_add(1, Ordering::Relaxed);
            let transfer_attempts = Arc::clone(&transfer_attempts);
            async move {
                transfer_attempts.lock().await.push(attempt);
                if call < 2 {
                    return Err(zerofs_client::ZeroFsError::Stale {
                        path: "book.m4b".into(),
                    }
                    .into());
                }
                Ok(())
            }
        },
    )
    .await
    .unwrap();

    assert_eq!(calls.load(Ordering::Relaxed), 3);
    assert_eq!(&*attempts.lock().await, &[1, 2, 3]);
}

#[tokio::test(start_paused = true)]
async fn connection_loss_is_retried_from_the_file_boundary() {
    let (client, _shutdown, _local) = remote_client().await;
    let clients = vec![client];
    let files = vec![PlannedFile {
        source: "book.m4b".into(),
        relative: "book.m4b".into(),
        size: 1,
    }];
    let calls = Arc::new(AtomicUsize::new(0));
    let transfer_calls = Arc::clone(&calls);

    run_file_workers(
        &clients,
        files,
        Progress::new("upload", 1, 1),
        CancellationToken::new(),
        move |_client, _file, _cancellation, _attempt| {
            let attempt = transfer_calls.fetch_add(1, Ordering::Relaxed);
            async move {
                if attempt == 0 {
                    return Err(zerofs_client::ZeroFsError::ConnectFailed {
                        message: "book.m4b: 9P connection lost".into(),
                    }
                    .into());
                }
                Ok(())
            }
        },
    )
    .await
    .unwrap();

    assert_eq!(calls.load(Ordering::Relaxed), 2);
}

#[tokio::test(start_paused = true)]
async fn retry_later_io_failure_is_retried_from_the_file_boundary() {
    let (client, _shutdown, _local) = remote_client().await;
    let clients = vec![client];
    let files = vec![PlannedFile {
        source: "book.m4b".into(),
        relative: "book.m4b".into(),
        size: 1,
    }];
    let calls = Arc::new(AtomicUsize::new(0));
    let transfer_calls = Arc::clone(&calls);

    run_file_workers(
        &clients,
        files,
        Progress::new("upload", 1, 1),
        CancellationToken::new(),
        move |_client, _file, _cancellation, _attempt| {
            let attempt = transfer_calls.fetch_add(1, Ordering::Relaxed);
            async move {
                if attempt == 0 {
                    return Err(zerofs_client::ZeroFsError::Io {
                        errno: 11,
                        path: "book.m4b".into(),
                        message: "try again".into(),
                    }
                    .into());
                }
                Ok(())
            }
        },
    )
    .await
    .unwrap();

    assert_eq!(calls.load(Ordering::Relaxed), 2);
}

#[tokio::test(start_paused = true)]
async fn cancellation_during_retry_backoff_is_reported_as_cancellation() {
    let (client, _shutdown, _local) = remote_client().await;
    let clients = vec![client];
    let cancellation = CancellationToken::new();
    let task_cancellation = cancellation.clone();
    let calls = Arc::new(AtomicUsize::new(0));
    let transfer_calls = Arc::clone(&calls);
    let progress = Progress::new_ordered(
        "upload",
        2,
        &[Path::new("book.m4b").into(), Path::new("later.m4b").into()],
    );
    let later = progress.start_file(Path::new("later.m4b"), 1);
    later.advance(1);
    later.finish();
    assert!(progress.emitted_lines().is_empty());
    let observed_progress = progress.clone();

    let task = tokio::spawn(async move {
        run_file_workers(
            &clients,
            vec![PlannedFile {
                source: "book.m4b".into(),
                relative: "book.m4b".into(),
                size: 1,
            }],
            progress,
            task_cancellation,
            move |_client, _file, _cancellation, _attempt| {
                transfer_calls.fetch_add(1, Ordering::Relaxed);
                async move {
                    Err(zerofs_client::ZeroFsError::Stale {
                        path: "book.m4b".into(),
                    }
                    .into())
                }
            },
        )
        .await
    });
    while calls.load(Ordering::Relaxed) == 0 {
        tokio::task::yield_now().await;
    }
    cancellation.cancel();

    let error = task.await.unwrap().unwrap_err();
    assert!(
        error.to_string().contains("transfer cancelled"),
        "{error:#}"
    );
    assert_eq!(calls.load(Ordering::Relaxed), 1);
    let lines = observed_progress.emitted_lines();
    assert_eq!(lines.len(), 3, "{lines:#?}");
    assert!(lines[0].contains("retry 2/3: book.m4b"), "{lines:#?}");
    assert!(
        lines[1].contains("failed after 1 attempt: book.m4b"),
        "{lines:#?}"
    );
    assert_eq!(lines[2], "upload file 2/2 complete: later.m4b");
}

#[tokio::test(start_paused = true)]
async fn permanent_io_failure_is_not_retried() {
    let (client, _shutdown, _local) = remote_client().await;
    let clients = vec![client];
    let files = vec![PlannedFile {
        source: "book.m4b".into(),
        relative: "book.m4b".into(),
        size: 1,
    }];
    let calls = Arc::new(AtomicUsize::new(0));
    let transfer_calls = Arc::clone(&calls);

    let error = run_file_workers(
        &clients,
        files,
        Progress::new("upload", 1, 1),
        CancellationToken::new(),
        move |_client, _file, _cancellation, _attempt| {
            transfer_calls.fetch_add(1, Ordering::Relaxed);
            async move {
                Err(zerofs_client::ZeroFsError::Io {
                    errno: 28,
                    path: "book.m4b".into(),
                    message: "no space left on device".into(),
                }
                .into())
            }
        },
    )
    .await
    .unwrap_err();

    assert_eq!(calls.load(Ordering::Relaxed), 1);
    assert!(error.to_string().contains("no space left on device"));
}

#[cfg(feature = "webui")]
#[tokio::test]
async fn upload_jobs_use_two_bounded_sessions_per_file_worker() {
    let filesystem = Arc::new(ZeroFS::new_in_memory().await.unwrap());
    let connections = Arc::new(AtomicUsize::new(0));
    let app = crate::webui::test_9p_websocket_router(filesystem, Arc::clone(&connections));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

    let local = tempfile::tempdir().unwrap();
    let source = local.path().join("source");
    fs::create_dir_all(&source).unwrap();
    fs::write(source.join("one.bin"), b"one").unwrap();
    fs::write(source.join("two.bin"), b"two").unwrap();
    fs::write(source.join("three.bin"), b"three").unwrap();

    run_upload(
        &format!("ws://{address}/ws/9p"),
        source,
        "/dest".into(),
        3,
        false,
    )
    .await
    .unwrap();

    assert_eq!(connections.load(Ordering::Relaxed), 6);
    server.abort();
}

#[tokio::test]
async fn upload_streams_each_chunk_once_and_releases_temporary_resources() {
    let (client, _shutdown, local) = remote_client().await;
    client.create_dir_all("/dest", 0o755).await.unwrap();
    client.write("/dest/keep.txt", b"keep").await.unwrap();
    let source = local.path().join("source");
    fs::create_dir_all(source.join("nested/empty")).unwrap();
    let chunk = client.capabilities().max_write_chunk as usize;
    let payload: Vec<u8> = (0..(chunk * 3 + 17)).map(|i| (i % 251) as u8).collect();
    fs::write(source.join("nested/big.bin"), &payload).unwrap();
    let plan = scan_local(&source).unwrap();
    let baseline_fids = quiesced_fids(&client).await;
    let operations_before = client.traffic_stats().operations;
    let progress = Progress::new("upload", plan.total_bytes, plan.files.len());

    execute_upload(
        &upload_workers(std::slice::from_ref(&client)),
        plan,
        Path::new("/dest"),
        false,
        progress,
        CancellationToken::new(),
    )
    .await
    .unwrap();

    let operations_after = client.traffic_stats().operations;
    assert_eq!(
        &client.read("/dest/nested/big.bin").await.unwrap()[..],
        payload
    );
    assert_eq!(&client.read("/dest/keep.txt").await.unwrap()[..], b"keep");
    assert!(
        client
            .metadata("/dest/nested/empty")
            .await
            .unwrap()
            .is_dir()
    );
    assert!(
        client
            .read_dir("/dest/nested")
            .await
            .unwrap()
            .iter()
            .all(|entry| !entry.name.starts_with(".zerofs-") || !entry.name.ends_with(".tmp"))
    );
    assert_eq!(quiesced_fids(&client).await, baseline_fids);
    assert!(
        operations_after - operations_before <= 31,
        "upload used too many 9P operations: {}",
        operations_after - operations_before
    );
}

#[tokio::test]
async fn completed_files_are_visible_before_the_rest_of_the_batch_finishes() {
    let (client, _shutdown, local) = remote_client().await;
    let source = local.path().join("source");
    fs::create_dir_all(&source).unwrap();
    fs::write(source.join("a-complete.txt"), b"complete").unwrap();
    fs::write(source.join("z-fails.txt"), b"planned").unwrap();
    let plan = scan_local(&source).unwrap();
    fs::remove_file(source.join("z-fails.txt")).unwrap();

    let error = execute_upload(
        &upload_workers(std::slice::from_ref(&client)),
        plan,
        Path::new("/dest"),
        false,
        Progress::new("upload", 15, 2),
        CancellationToken::new(),
    )
    .await
    .unwrap_err();

    assert!(
        format!("{error:#}").contains("open local source"),
        "{error:#}"
    );
    assert_eq!(
        &client.read("/dest/a-complete.txt").await.unwrap()[..],
        b"complete"
    );
    assert!(client.metadata("/dest/z-fails.txt").await.is_err());
}

#[tokio::test]
async fn download_streams_nested_files_and_preserves_unrelated_entries() {
    let (client, _shutdown, local) = remote_client().await;
    client
        .create_dir_all("/source/nested/empty", 0o755)
        .await
        .unwrap();
    let chunk = client.capabilities().max_read_chunk as usize;
    let payload: Vec<u8> = (0..(chunk * 3 + 17)).map(|i| (i % 239) as u8).collect();
    client
        .write("/source/nested/big.bin", &payload)
        .await
        .unwrap();
    let plan = scan_remote(&client, Path::new("/source")).await.unwrap();
    let destination = local.path().join("download");
    fs::create_dir_all(&destination).unwrap();
    fs::write(destination.join("keep.txt"), b"keep").unwrap();
    let baseline_fids = quiesced_fids(&client).await;

    execute_download(
        std::slice::from_ref(&client),
        plan,
        &destination,
        Progress::new("download", payload.len() as u64, 1),
        CancellationToken::new(),
    )
    .await
    .unwrap();

    assert_eq!(
        fs::read(destination.join("nested/big.bin")).unwrap(),
        payload
    );
    assert_eq!(fs::read(destination.join("keep.txt")).unwrap(), b"keep");
    assert!(destination.join("nested/empty").is_dir());
    assert!(
        fs::read_dir(destination.join("nested"))
            .unwrap()
            .all(|entry| !entry
                .unwrap()
                .file_name()
                .to_string_lossy()
                .starts_with(".zerofs-"))
    );
    assert_eq!(quiesced_fids(&client).await, baseline_fids);
}

#[tokio::test]
async fn single_file_transfers_use_the_exact_destination_path() {
    let (client, _shutdown, local) = remote_client().await;
    let source = local.path().join("source.bin");
    fs::write(&source, b"payload").unwrap();
    client.write("/uploaded.bin", b"old").await.unwrap();
    let upload_plan = scan_local(&source).unwrap();
    execute_upload(
        &upload_workers(std::slice::from_ref(&client)),
        upload_plan,
        Path::new("/uploaded.bin"),
        false,
        Progress::new("upload", 7, 1),
        CancellationToken::new(),
    )
    .await
    .unwrap();
    assert_eq!(&client.read("/uploaded.bin").await.unwrap()[..], b"payload");

    let download_plan = scan_remote(&client, Path::new("/uploaded.bin"))
        .await
        .unwrap();
    let destination = local.path().join("destination.bin");
    fs::write(&destination, b"old").unwrap();

    execute_download(
        std::slice::from_ref(&client),
        download_plan,
        &destination,
        Progress::new("download", 7, 1),
        CancellationToken::new(),
    )
    .await
    .unwrap();

    assert_eq!(fs::read(destination).unwrap(), b"payload");
}

#[tokio::test]
async fn rm_removes_files_and_browser_style_directory_trees() {
    let (client, _shutdown, _local) = remote_client().await;
    client
        .create_dir_all("/remove/nested/empty", 0o755)
        .await
        .unwrap();
    client.write("/remove/a.txt", b"a").await.unwrap();
    client.write("/remove/nested/b.txt", b"b").await.unwrap();
    client.write("/single.txt", b"single").await.unwrap();
    client.write("/keep.txt", b"keep").await.unwrap();
    let baseline_fids = quiesced_fids(&client).await;

    let tree_progress = DeleteProgress::new();
    execute_delete(
        Arc::clone(&client),
        Path::new("/remove"),
        tree_progress.clone(),
        CancellationToken::new(),
    )
    .await
    .unwrap();

    assert!(client.stat("/remove").await.is_err());
    assert_eq!(tree_progress.removed_entries(), 5);
    let file_progress = DeleteProgress::new();
    execute_delete(
        Arc::clone(&client),
        Path::new("/single.txt"),
        file_progress.clone(),
        CancellationToken::new(),
    )
    .await
    .unwrap();
    assert!(client.stat("/single.txt").await.is_err());
    assert_eq!(file_progress.removed_entries(), 1);
    assert_eq!(&client.read("/keep.txt").await.unwrap()[..], b"keep");
    assert_eq!(quiesced_fids(&client).await, baseline_fids);
}

#[tokio::test]
async fn rm_refuses_the_attach_root() {
    let (client, _shutdown, _local) = remote_client().await;
    client.create_dir_all("/nested", 0o755).await.unwrap();
    client.write("/keep.txt", b"keep").await.unwrap();

    for root_alias in ["/", "/nested/.."] {
        let error = execute_delete(
            Arc::clone(&client),
            Path::new(root_alias),
            DeleteProgress::new(),
            CancellationToken::new(),
        )
        .await
        .unwrap_err();

        assert!(
            error.to_string().contains("refusing to remove"),
            "{root_alias}: {error:#}"
        );
    }
    assert_eq!(&client.read("/keep.txt").await.unwrap()[..], b"keep");
}

#[tokio::test]
async fn rm_cancellation_interrupts_an_in_flight_operation() {
    let (client, _shutdown, _local) = remote_client().await;
    client.create_dir("/cancel", 0o755).await.unwrap();
    for index in 0..64 {
        client
            .write(format!("/cancel/{index:02}.txt"), b"payload")
            .await
            .unwrap();
    }
    let progress = DeleteProgress::new();
    let cancellation = CancellationToken::new();
    let task = tokio::spawn(execute_delete(
        Arc::clone(&client),
        Path::new("/cancel"),
        progress.clone(),
        cancellation.clone(),
    ));

    while progress.removed_entries() == 0 && !task.is_finished() {
        tokio::task::yield_now().await;
    }
    assert!(!task.is_finished(), "delete completed before cancellation");

    cancellation.cancel();

    let error = tokio::time::timeout(Duration::from_secs(5), task)
        .await
        .expect("delete cancellation should settle active operations")
        .unwrap()
        .unwrap_err();
    assert!(error.to_string().contains("delete cancelled"), "{error:#}");
    assert_eq!(progress.removed_entries(), 1);
    assert!(client.stat("/cancel").await.unwrap().is_dir());
}

#[test]
fn first_interrupt_is_graceful_and_second_interrupt_is_immediate() {
    let cancellation = CancellationToken::new();
    let mut policy = InterruptPolicy::default();

    assert!(!policy.register(&cancellation));
    assert!(cancellation.is_cancelled());
    assert!(policy.register(&cancellation));
}

#[tokio::test]
async fn cli_close_waits_for_all_fid_replies() {
    let (client, _shutdown, _local) = remote_client().await;
    client.write("/file.txt", b"payload").await.unwrap();

    close_client(&client).await.unwrap();

    assert_eq!(client.outstanding_fids(), 0);
}

#[tokio::test(start_paused = true)]
async fn settling_wrapper_stays_quiet_until_cancellation_then_repeats_notices() {
    let cancellation = CancellationToken::new();
    let observed = Arc::new(std::sync::Mutex::new(Vec::new()));
    let sink = Arc::clone(&observed);
    let quiet = Arc::clone(&observed);
    let work_cancellation = cancellation.clone();
    let work = async move {
        tokio::time::sleep(SETTLE_NOTICE_INTERVAL * 3).await;
        assert!(
            quiet.lock().unwrap().is_empty(),
            "work that was never cancelled must not report settling"
        );
        work_cancellation.cancel();
        // Two full intervals of in-flight work that refuses to settle.
        tokio::time::sleep(SETTLE_NOTICE_INTERVAL * 2 + Duration::from_secs(1)).await;
        42
    };
    let result = with_settling_notices(work, cancellation, move |waited| {
        sink.lock().unwrap().push(waited);
    })
    .await;
    assert_eq!(result, 42);
    assert_eq!(
        *observed.lock().unwrap(),
        vec![SETTLE_NOTICE_INTERVAL, SETTLE_NOTICE_INTERVAL * 2],
        "a cancelled transfer must keep telling the operator what it waits on"
    );
}

#[tokio::test]
async fn cancellation_during_empty_directory_sync_never_prints_completion() {
    let filesystem = Arc::new(ZeroFS::new_in_memory().await.unwrap());
    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    filesystem.flush_coordinator.set_local_durability_barrier({
        let entered = Arc::clone(&entered);
        let release = Arc::clone(&release);
        Arc::new(move || {
            let entered = Arc::clone(&entered);
            let release = Arc::clone(&release);
            Box::pin(async move {
                entered.notify_one();
                release.notified().await;
                Ok(())
            })
        })
    });

    let local = tempfile::tempdir().unwrap();
    let socket = local.path().join("empty-directory-sync.9p.sock");
    let server = NinePServer::new_unix(filesystem, socket.clone());
    let shutdown = CancellationToken::new();
    let server_shutdown = shutdown.clone();
    tokio::spawn(async move { server.start(server_shutdown).await.unwrap() });
    let client = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if let Ok(client) = connect_transfer_client(&format!("unix:{}", socket.display())).await
            {
                break client;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("test 9P client did not connect");

    let cancellation = CancellationToken::new();
    let task_cancellation = cancellation.clone();
    let task = tokio::spawn(async move {
        execute_upload(
            &upload_workers(&[client]),
            TransferPlan {
                source_is_dir: true,
                directories: vec![Path::new("").to_path_buf()],
                files: Vec::new(),
                total_bytes: 0,
            },
            Path::new("/empty"),
            false,
            Progress::new("upload", 0, 0),
            task_cancellation,
        )
        .await
    });

    entered.notified().await;
    cancellation.cancel();
    release.notify_one();
    let error = task.await.unwrap().unwrap_err();
    assert!(error.to_string().contains("upload cancelled"), "{error:#}");
    shutdown.cancel();
}

#[tokio::test]
async fn cancellation_after_upload_bytes_removes_the_temporary_file() {
    let (client, _shutdown, local, filesystem) = remote_client_with_filesystem().await;
    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    filesystem.flush_coordinator.set_local_durability_barrier({
        let entered = Arc::clone(&entered);
        let release = Arc::clone(&release);
        Arc::new(move || {
            let entered = Arc::clone(&entered);
            let release = Arc::clone(&release);
            Box::pin(async move {
                entered.notify_one();
                release.notified().await;
                Ok(())
            })
        })
    });
    let source = local.path().join("source.bin");
    fs::write(&source, b"payload").unwrap();
    let cancellation = CancellationToken::new();
    let task_cancellation = cancellation.clone();
    let task_client = Arc::clone(&client);
    let task = tokio::spawn(async move {
        execute_upload(
            &upload_workers(&[task_client]),
            scan_local(&source).unwrap(),
            Path::new("/destination.bin"),
            false,
            Progress::new("upload", 7, 1),
            task_cancellation,
        )
        .await
    });

    entered.notified().await;
    cancellation.cancel();
    release.notify_one();

    let error = task.await.unwrap().unwrap_err();
    assert!(error.to_string().contains("upload cancelled"), "{error:#}");
    assert!(client.stat("/destination.bin").await.is_err());
    assert!(
        client
            .read_dir("/")
            .await
            .unwrap()
            .iter()
            .all(|entry| !String::from_utf8_lossy(&entry.name_bytes).starts_with(".zerofs-"))
    );
}

#[tokio::test]
async fn upload_sync_failure_preserves_the_old_destination_and_cleans_temporary_file() {
    let (client, _shutdown, local, filesystem) = remote_client_with_filesystem().await;
    client.write("/destination.bin", b"old").await.unwrap();
    filesystem
        .flush_coordinator
        .set_local_durability_barrier(Arc::new(|| {
            Box::pin(async { Err(crate::fs::errors::FsError::IoError) })
        }));
    let source = local.path().join("source.bin");
    fs::write(&source, b"replacement").unwrap();

    let error = execute_upload(
        &upload_workers(std::slice::from_ref(&client)),
        scan_local(&source).unwrap(),
        Path::new("/destination.bin"),
        false,
        Progress::new("upload", 11, 1),
        CancellationToken::new(),
    )
    .await
    .unwrap_err();

    assert!(format!("{error:#}").contains("sync"), "{error:#}");
    assert_eq!(&client.read("/destination.bin").await.unwrap()[..], b"old");
    assert!(
        client
            .read_dir("/")
            .await
            .unwrap()
            .iter()
            .all(|entry| !String::from_utf8_lossy(&entry.name_bytes).starts_with(".zerofs-"))
    );
}

#[tokio::test]
async fn final_upload_sync_failure_reports_visible_but_unverified_publication() {
    let (client, _shutdown, local, filesystem) = remote_client_with_filesystem().await;
    let barriers = Arc::new(AtomicUsize::new(0));
    filesystem.flush_coordinator.set_local_durability_barrier({
        let barriers = Arc::clone(&barriers);
        Arc::new(move || {
            let barriers = Arc::clone(&barriers);
            Box::pin(async move {
                if barriers.fetch_add(1, Ordering::SeqCst) == 0 {
                    Ok(())
                } else {
                    Err(crate::fs::errors::FsError::IoError)
                }
            })
        })
    });
    let source = local.path().join("source.bin");
    fs::write(&source, b"replacement").unwrap();

    let error = execute_upload(
        &upload_workers(std::slice::from_ref(&client)),
        scan_local(&source).unwrap(),
        Path::new("/destination.bin"),
        false,
        Progress::new("upload", 11, 1),
        CancellationToken::new(),
    )
    .await
    .unwrap_err();

    assert!(
        format!("{error:#}").contains("sync published remote file"),
        "{error:#}"
    );
    assert_eq!(
        &client.read("/destination.bin").await.unwrap()[..],
        b"replacement"
    );
    assert!(
        client
            .read_dir("/")
            .await
            .unwrap()
            .iter()
            .all(|entry| !String::from_utf8_lossy(&entry.name_bytes).starts_with(".zerofs-"))
    );
}
