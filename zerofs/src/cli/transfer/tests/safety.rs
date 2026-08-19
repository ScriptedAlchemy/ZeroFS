use super::*;

#[tokio::test]
async fn cancelled_download_preserves_the_existing_destination() {
    let (client, _shutdown, local) = remote_client().await;
    client.write("/source.bin", b"replacement").await.unwrap();
    let plan = scan_remote(&client, Path::new("/source.bin"))
        .await
        .unwrap();
    let destination = local.path().join("destination.bin");
    fs::write(&destination, b"original").unwrap();
    let cancellation = CancellationToken::new();
    cancellation.cancel();

    let error = execute_download(
        std::slice::from_ref(&client),
        plan,
        &destination,
        Progress::new("download", 11, 1),
        cancellation,
    )
    .await
    .unwrap_err();

    assert!(
        error.to_string().contains("download cancelled"),
        "{error:#}"
    );
    assert_eq!(fs::read(&destination).unwrap(), b"original");
    assert!(fs::read_dir(local.path()).unwrap().all(|entry| {
        !entry
            .unwrap()
            .file_name()
            .to_string_lossy()
            .starts_with(".zerofs-")
    }));
}

#[tokio::test]
async fn file_directory_conflicts_fail_before_copying_bytes() {
    let (client, _shutdown, local) = remote_client().await;
    let source = local.path().join("source.bin");
    fs::write(&source, b"payload").unwrap();
    client.create_dir_all("/occupied", 0o755).await.unwrap();
    let upload_progress = Progress::new("upload", 7, 1);

    let upload_error = execute_upload(
        &upload_workers(std::slice::from_ref(&client)),
        scan_local(&source).unwrap(),
        Path::new("/occupied"),
        false,
        upload_progress.clone(),
        CancellationToken::new(),
    )
    .await
    .unwrap_err();

    assert!(
        upload_error
            .to_string()
            .contains("destination is a directory"),
        "{upload_error:#}"
    );
    assert_eq!(upload_progress.transferred_bytes(), 0);
    assert!(
        client
            .read_dir("/")
            .await
            .unwrap()
            .iter()
            .all(|entry| !entry.name.starts_with(".zerofs-"))
    );

    client.write("/source.bin", b"payload").await.unwrap();
    let destination = local.path().join("occupied");
    fs::create_dir(&destination).unwrap();
    let download_progress = Progress::new("download", 7, 1);
    let download_error = execute_download(
        std::slice::from_ref(&client),
        scan_remote(&client, Path::new("/source.bin"))
            .await
            .unwrap(),
        &destination,
        download_progress.clone(),
        CancellationToken::new(),
    )
    .await
    .unwrap_err();

    assert!(
        download_error
            .to_string()
            .contains("destination is a directory"),
        "{download_error:#}"
    );
    assert_eq!(download_progress.transferred_bytes(), 0);
}

#[tokio::test]
async fn upload_does_not_follow_a_source_replaced_by_a_symlink_after_planning() {
    let (client, _shutdown, local) = remote_client().await;
    let target = local.path().join("target.m4b");
    fs::write(&target, b"not the planned source").unwrap();
    let size = fs::metadata(&target).unwrap().len();
    let source = local.path().join("planned.m4b");
    symlink(&target, &source).unwrap();
    let clients = vec![client.clone()];

    let error = execute_upload(
        &upload_workers(&clients),
        TransferPlan {
            source_is_dir: false,
            directories: Vec::new(),
            files: vec![PlannedFile {
                source,
                relative: Path::new("").to_path_buf(),
                size,
            }],
            total_bytes: size,
        },
        Path::new("/uploaded.m4b"),
        false,
        Progress::new("upload", size, 1),
        CancellationToken::new(),
    )
    .await
    .unwrap_err();

    assert!(
        format!("{error:#}").contains("open local source"),
        "{error:#}"
    );
    assert!(matches!(
        client.stat("/uploaded.m4b").await,
        Err(zerofs_client::ZeroFsError::NotFound { .. })
    ));
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
async fn download_rejects_growth_without_copying_beyond_the_plan() {
    let (client, _shutdown, local) = remote_client().await;
    client.write("/source.bin", b"planned").await.unwrap();
    let plan = scan_remote(&client, Path::new("/source.bin"))
        .await
        .unwrap();
    let remote = client
        .open("/source.bin", zerofs_client::OpenOptions::write_only())
        .await
        .unwrap();
    remote.write_at(7, b"-extra").await.unwrap();
    remote.close().await;
    let destination = local.path().join("destination.bin");
    let progress = Progress::new("download", 7, 1);

    let error = execute_download(
        std::slice::from_ref(&client),
        plan,
        &destination,
        progress.clone(),
        CancellationToken::new(),
    )
    .await
    .unwrap_err();

    assert!(
        format!("{error:#}").contains("remote source changed while downloading"),
        "{error:#}"
    );
    assert_eq!(progress.transferred_bytes(), 0);
    assert!(!destination.exists());
}

#[tokio::test]
async fn download_rejects_truncation_and_preserves_the_existing_destination() {
    let (client, _shutdown, local) = remote_client().await;
    client.write("/source.bin", b"planned").await.unwrap();
    let plan = scan_remote(&client, Path::new("/source.bin"))
        .await
        .unwrap();
    let remote = client
        .open("/source.bin", zerofs_client::OpenOptions::write_only())
        .await
        .unwrap();
    remote.set_len(3).await.unwrap();
    remote.close().await;
    let destination = local.path().join("destination.bin");
    fs::write(&destination, b"original").unwrap();
    let progress = Progress::new("download", 7, 1);

    let error = execute_download(
        std::slice::from_ref(&client),
        plan,
        &destination,
        progress.clone(),
        CancellationToken::new(),
    )
    .await
    .unwrap_err();

    assert!(
        format!("{error:#}").contains("remote source changed while downloading"),
        "{error:#}"
    );
    assert_eq!(fs::read(&destination).unwrap(), b"original");
    assert_eq!(progress.transferred_bytes(), 0);
    assert!(fs::read_dir(local.path()).unwrap().all(|entry| {
        !entry
            .unwrap()
            .file_name()
            .to_string_lossy()
            .starts_with(".zerofs-")
    }));
}

#[tokio::test]
async fn download_rejects_symlinked_destination_ancestors() {
    let (client, _shutdown, local) = remote_client().await;
    client.create_dir_all("/source/link", 0o755).await.unwrap();
    client
        .write("/source/link/escaped.bin", b"payload")
        .await
        .unwrap();
    let plan = scan_remote(&client, Path::new("/source")).await.unwrap();
    let destination = local.path().join("destination");
    let outside = local.path().join("outside");
    fs::create_dir_all(&destination).unwrap();
    fs::create_dir_all(&outside).unwrap();
    symlink(&outside, destination.join("link")).unwrap();

    let error = execute_download(
        std::slice::from_ref(&client),
        plan,
        &destination,
        Progress::new("download", 7, 1),
        CancellationToken::new(),
    )
    .await
    .unwrap_err();

    assert!(error.to_string().contains("symbolic link"), "{error:#}");
    assert!(!outside.join("escaped.bin").exists());
}

#[tokio::test]
async fn single_file_download_rejects_a_symlinked_destination_ancestor() {
    let (client, _shutdown, local) = remote_client().await;
    client.write("/source.bin", b"payload").await.unwrap();
    let plan = scan_remote(&client, Path::new("/source.bin"))
        .await
        .unwrap();
    let destination_root = local.path().join("destination");
    let outside = local.path().join("outside");
    fs::create_dir_all(&destination_root).unwrap();
    fs::create_dir_all(&outside).unwrap();
    symlink(&outside, destination_root.join("link")).unwrap();
    let destination = destination_root.join("link/nested/result.bin");

    let error = execute_download(
        std::slice::from_ref(&client),
        plan,
        &destination,
        Progress::new("download", 7, 1),
        CancellationToken::new(),
    )
    .await
    .unwrap_err();

    assert!(error.to_string().contains("symbolic link"), "{error:#}");
    assert!(!outside.join("nested/result.bin").exists());
}

#[tokio::test]
async fn download_preflights_all_file_directory_conflicts_before_copying() {
    let (client, _shutdown, local) = remote_client().await;
    client.create_dir_all("/source", 0o755).await.unwrap();
    client.write("/source/a-first.bin", b"first").await.unwrap();
    client
        .write("/source/z-conflict.bin", b"last")
        .await
        .unwrap();
    let plan = scan_remote(&client, Path::new("/source")).await.unwrap();
    let destination = local.path().join("destination");
    fs::create_dir_all(destination.join("z-conflict.bin")).unwrap();
    let progress = Progress::new("download", 9, 2);

    let error = execute_download(
        std::slice::from_ref(&client),
        plan,
        &destination,
        progress.clone(),
        CancellationToken::new(),
    )
    .await
    .unwrap_err();

    assert!(
        error.to_string().contains("destination is a directory"),
        "{error:#}"
    );
    assert!(!destination.join("a-first.bin").exists());
    assert_eq!(progress.transferred_bytes(), 0);
}

#[tokio::test]
async fn upload_preflights_all_file_directory_conflicts_before_copying() {
    let (client, _shutdown, local) = remote_client().await;
    let source = local.path().join("source");
    fs::create_dir_all(&source).unwrap();
    fs::write(source.join("a-first.bin"), b"first").unwrap();
    fs::write(source.join("z-conflict.bin"), b"last").unwrap();
    client
        .create_dir_all("/destination/z-conflict.bin", 0o755)
        .await
        .unwrap();
    let plan = scan_local(&source).unwrap();
    let progress = Progress::new("upload", 9, 2);

    let error = execute_upload(
        &upload_workers(std::slice::from_ref(&client)),
        plan,
        Path::new("/destination"),
        false,
        progress.clone(),
        CancellationToken::new(),
    )
    .await
    .unwrap_err();

    assert!(
        error.to_string().contains("destination is a directory"),
        "{error:#}"
    );
    assert!(client.stat("/destination/a-first.bin").await.is_err());
    assert_eq!(progress.transferred_bytes(), 0);
}
