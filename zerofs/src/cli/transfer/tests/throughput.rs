use super::super::copy::{stream_download, stream_upload, sync_remote_files};
use super::*;

async fn download_with_reordered_pipeline(
    short_reads: bool,
    through_cursor: bool,
    negotiated_chunks: bool,
) {
    let chunk = if negotiated_chunks {
        1024 * 1024
    } else {
        64 * 1024
    };
    let (primary, shutdown, local) = remote_client().await;
    let chunks = if through_cursor { 5 } else { 9 };
    let payload: Vec<u8> = (0..chunk * chunks + 17).map(|i| (i % 251) as u8).collect();
    primary.write("/source.bin", &payload).await.unwrap();
    let socket = local.path().join("transfer.9p.sock");
    let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
        .await
        .unwrap();
    let target = format!("tcp://{}", listener.local_addr().unwrap());
    let wave_size = 8;
    let (wave_ready, wait_for_wave) = tokio::sync::oneshot::channel();
    let release_wave = Arc::new(tokio::sync::Semaphore::new(usize::from(!through_cursor)));
    let proxy_release_wave = Arc::clone(&release_wave);
    let proxy = tokio::spawn(async move {
        let mut wave_ready = Some(wave_ready);
        let (downstream, _) = listener.accept().await.unwrap();
        let upstream = tokio::net::UnixStream::connect(&socket).await.unwrap();
        let (mut requests, mut responses) = downstream.into_split();
        let (mut backend_responses, mut backend_requests) = upstream.into_split();
        let reply_task = tokio::spawn(async move {
            let _ = tokio::io::copy(&mut backend_responses, &mut responses).await;
        });
        let mut first_wave = Vec::new();
        let mut reads = 0usize;
        loop {
            let mut size = [0; P9_SIZE_FIELD_LEN];
            if requests.read_exact(&mut size).await.is_err() {
                break;
            }
            let mut frame = vec![0; u32::from_le_bytes(size) as usize];
            frame[..P9_SIZE_FIELD_LEN].copy_from_slice(&size);
            if requests
                .read_exact(&mut frame[P9_SIZE_FIELD_LEN..])
                .await
                .is_err()
            {
                break;
            }
            if let Ok(mut message) = P9Message::from_bytes_ctx(&frame, true)
                && let Message::Tread(read) = &mut message.body
            {
                reads += 1;
                if negotiated_chunks {
                    assert!(
                        read.count <= 1024 * 1024,
                        "download used the upload-sized negotiation for a read: {} bytes",
                        read.count
                    );
                }
                let mut changed = false;
                if short_reads && read.count > 1 {
                    read.count /= 2;
                    changed = true;
                }
                if through_cursor && read.offset >= (chunk * 6) as u64 {
                    // Errors from speculative ranges beyond the earlier EOF
                    // must be discarded instead of failing a complete copy.
                    read.fid = u32::MAX;
                    changed = true;
                }
                if changed {
                    frame = message.to_bytes().unwrap();
                }
                let wave_read = reads.saturating_sub(usize::from(through_cursor));
                if wave_read > 0 && wave_read <= wave_size {
                    first_wave.push(frame);
                    if wave_read == wave_size {
                        let _ = wave_ready.take().unwrap().send(());
                        proxy_release_wave.acquire().await.unwrap().forget();
                        // No reply is possible until the entire wave is issued.
                        // Forward their disjoint ranges in reverse order.
                        for pending in first_wave.drain(..).rev() {
                            backend_requests.write_all(&pending).await.unwrap();
                        }
                    }
                    continue;
                }
            }
            if backend_requests.write_all(&frame).await.is_err() {
                break;
            }
        }
        reply_task.abort();
        let _ = reply_task.await;
    });
    let client = if through_cursor {
        Client::connect_with(
            &target,
            zerofs_client::ConnectOptions {
                msize: chunk as u32 + ninep_proto::P9_IOHDRSZ,
                ..Default::default()
            },
        )
        .await
        .unwrap()
    } else {
        connect_transfer_client(&target).await.unwrap()
    };
    let planned = scan_remote(&client, Path::new("/source.bin"))
        .await
        .unwrap()
        .files
        .pop()
        .unwrap();
    let remote = client
        .open("/source.bin", zerofs_client::OpenOptions::read_only())
        .await
        .unwrap();
    let destination = local.path().join("download.bin");
    let mut output = tokio::fs::File::create(&destination).await.unwrap();
    let progress = Progress::new("download", planned.size, 1);
    let file_progress = progress.start_file(Path::new("source.bin"), planned.size);
    tokio::time::timeout(Duration::from_secs(10), async {
        if through_cursor {
            let mut cursor = remote.cursor();
            let mut priming_byte = [0];
            cursor.read_exact(&mut priming_byte).await.unwrap();
            output.write_all(&priming_byte).await.unwrap();
            {
                let mut first_buffer = vec![0; chunk];
                let first_read = cursor.read(&mut first_buffer);
                tokio::pin!(first_read);
                tokio::select! {
                    _ = wait_for_wave => {},
                    result = &mut first_read => panic!("read completed before the gate: {result:?}"),
                }
                // Cancel a pending demand read, then resume with copy's much
                // smaller buffer. No prefetched bytes may be lost or skipped.
            }
            assert_eq!(cursor.position(), 1);
            release_wave.add_permits(1);
            let copied = tokio::io::copy(&mut cursor, &mut output).await.unwrap();
            assert_eq!(copied, planned.size - 1);
            assert_eq!(cursor.position(), planned.size);
        } else {
            stream_download(
                &remote,
                &mut output,
                &planned,
                if negotiated_chunks {
                    client.capabilities().max_read_chunk
                } else {
                    chunk as u32
                },
                &file_progress,
                &CancellationToken::new(),
            )
            .await
            .unwrap();
        }
    })
    .await
    .expect("download waited for its first reply instead of filling the pipeline");
    output.sync_all().await.unwrap();
    assert_eq!(fs::read(&destination).unwrap(), payload);
    remote.close().await;
    client.close().await;
    proxy.abort();
    let _ = proxy.await;
    shutdown.cancel();
}

#[tokio::test]
async fn download_pipelines_one_file_and_preserves_order() {
    download_with_reordered_pipeline(false, false, false).await;
}

#[tokio::test]
async fn download_fills_short_responses_without_skipping_ranges() {
    download_with_reordered_pipeline(true, false, false).await;
}

#[tokio::test]
async fn download_bounds_reads_independently_of_the_upload_negotiation() {
    download_with_reordered_pipeline(false, false, true).await;
}

#[tokio::test]
async fn cursor_pipelines_reads_and_resumes_cancelled_read_with_smaller_buffer() {
    download_with_reordered_pipeline(false, true, false).await;
}

#[tokio::test]
async fn cursor_fills_short_responses_without_skipping_ranges() {
    download_with_reordered_pipeline(true, true, false).await;
}

#[tokio::test]
async fn upload_stripes_positioned_writes_across_remote_sessions() {
    let (primary_client, _shutdown, local, _filesystem) = remote_client_with_filesystem().await;
    let target = format!("unix:{}", local.path().join("transfer.9p.sock").display());
    let secondary_client = connect_transfer_client(&target).await.unwrap();
    primary_client.create_dir_all("/dest", 0o755).await.unwrap();
    let primary_remote = primary_client
        .open(
            "/dest/striped.tmp",
            zerofs_client::OpenOptions::write_only()
                .create_new(true)
                .mode(0o644),
        )
        .await
        .unwrap();
    let secondary_remote = secondary_client
        .open(
            "/dest/striped.tmp",
            zerofs_client::OpenOptions::write_only(),
        )
        .await
        .unwrap();
    let chunk = primary_client.capabilities().max_write_chunk as usize;
    let source = local.path().join("source.bin");
    let mut payload = vec![0; chunk * 4];
    for (index, range) in payload.chunks_mut(chunk).enumerate() {
        range.fill((index + 1) as u8);
    }
    fs::write(&source, &payload).unwrap();
    let planned = scan_local(&source).unwrap().files.pop().unwrap();
    let progress = Progress::new("upload", planned.size, 1);
    let file_progress = progress.start_file(Path::new("source.bin"), planned.size);
    let primary_before = primary_client.traffic_stats().operations;
    let secondary_before = secondary_client.traffic_stats().operations;

    stream_upload(
        &[primary_remote.clone(), secondary_remote.clone()],
        &planned,
        chunk,
        &file_progress,
        &CancellationToken::new(),
    )
    .await
    .unwrap();

    let primary_after_writes = primary_client.traffic_stats().operations;
    let secondary_after_writes = secondary_client.traffic_stats().operations;
    assert!(primary_after_writes > primary_before);
    assert!(secondary_after_writes > secondary_before);
    sync_remote_files(&[primary_remote.clone(), secondary_remote.clone()])
        .await
        .unwrap();
    assert!(primary_client.traffic_stats().operations > primary_after_writes);
    assert!(secondary_client.traffic_stats().operations > secondary_after_writes);
    assert_eq!(
        primary_client.read("/dest/striped.tmp").await.unwrap(),
        payload
    );
    primary_remote.close().await;
    secondary_remote.close().await;
    secondary_client.close().await;
}

#[tokio::test]
async fn upload_queues_a_second_aligned_chunk_while_the_first_is_committing() {
    let (client, _shutdown, local, filesystem) = remote_client_with_filesystem().await;
    client.create_dir_all("/dest", 0o755).await.unwrap();
    let remote = client
        .open(
            "/dest/pipelined.tmp",
            zerofs_client::OpenOptions::write_only()
                .create_new(true)
                .mode(0o644),
        )
        .await
        .unwrap();
    let inode = remote.metadata().await.unwrap().ino;
    let chunk = client.capabilities().max_write_chunk as usize;
    let source = local.path().join("source.bin");
    fs::write(&source, vec![0x5a; chunk * 2]).unwrap();
    let planned = scan_local(&source).unwrap().files.pop().unwrap();
    let progress = Progress::new("upload", planned.size, 1);
    let file_progress = progress.start_file(Path::new("source.bin"), planned.size);

    let stalled_apply = filesystem.db.flush_barrier().write_owned().await;
    let first_apply_reached = filesystem.write_coordinator.probe_next_apply();
    let upload = tokio::spawn(async move {
        stream_upload(
            std::slice::from_ref(&remote),
            &planned,
            chunk,
            &file_progress,
            &CancellationToken::new(),
        )
        .await
    });
    first_apply_reached.await.unwrap();

    let second_queued = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if let Some(Some(crate::fs::inode::Inode::File(file))) =
                filesystem.inode_store.pending_inode(inode)
                && file.size == (chunk * 2) as u64
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .is_ok();

    drop(stalled_apply);
    upload.await.unwrap().unwrap();
    assert!(
        second_queued,
        "the uploader waited for the first Rwrite instead of queueing the next chunk"
    );
}

#[tokio::test]
async fn cancellation_settles_issued_chunks_before_returning() {
    let (primary_client, _shutdown, local, filesystem) = remote_client_with_filesystem().await;
    let target = format!("unix:{}", local.path().join("transfer.9p.sock").display());
    let secondary_client = connect_transfer_client(&target).await.unwrap();
    primary_client.create_dir_all("/dest", 0o755).await.unwrap();
    let primary_remote = primary_client
        .open(
            "/dest/pipelined.tmp",
            zerofs_client::OpenOptions::write_only()
                .create_new(true)
                .mode(0o644),
        )
        .await
        .unwrap();
    let secondary_remote = secondary_client
        .open(
            "/dest/pipelined.tmp",
            zerofs_client::OpenOptions::write_only(),
        )
        .await
        .unwrap();
    let inode = primary_remote.metadata().await.unwrap().ino;
    let chunk = primary_client.capabilities().max_write_chunk as usize;
    let source = local.path().join("source.bin");
    fs::write(&source, vec![0x5a; chunk * 4]).unwrap();
    let planned = scan_local(&source).unwrap().files.pop().unwrap();
    let progress = Progress::new("upload", planned.size, 1);
    let file_progress = progress.start_file(Path::new("source.bin"), planned.size);
    let cancellation = CancellationToken::new();
    let task_cancellation = cancellation.clone();

    let stalled_apply = filesystem.db.flush_barrier().write_owned().await;
    let first_apply_reached = filesystem.write_coordinator.probe_next_apply();
    let upload = tokio::spawn(async move {
        stream_upload(
            &[primary_remote, secondary_remote],
            &planned,
            chunk,
            &file_progress,
            &task_cancellation,
        )
        .await
    });
    first_apply_reached.await.unwrap();
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if let Some(Some(crate::fs::inode::Inode::File(file))) =
                filesystem.inode_store.pending_inode(inode)
                && file.size == (chunk * 4) as u64
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the four-write multi-session pipeline never filled");

    cancellation.cancel();
    tokio::task::yield_now().await;
    assert!(
        !upload.is_finished(),
        "cancellation dropped an already dispatched write"
    );
    drop(stalled_apply);
    let error = upload.await.unwrap().unwrap_err();
    assert!(
        format!("{error:#}").contains("upload cancelled"),
        "{error:#}"
    );
}
