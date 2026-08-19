use super::super::copy::{stream_upload, sync_remote_files};
use super::*;

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
