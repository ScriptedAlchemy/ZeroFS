use super::*;

#[tokio::test]
async fn upload_skips_materialized_same_length_files() {
    let (client, _shutdown, local) = remote_client().await;
    client.create_dir_all("/dest", 0o755).await.unwrap();
    client.write("/dest/book.m4b", b"remote").await.unwrap();
    let source = local.path().join("book.m4b");
    fs::write(&source, b"source").unwrap();
    let plan = scan_local(&source).unwrap();
    let progress = Progress::new("upload", plan.total_bytes, plan.files.len());

    execute_upload(
        std::slice::from_ref(&client),
        plan,
        Path::new("/dest/book.m4b"),
        true,
        progress.clone(),
        CancellationToken::new(),
    )
    .await
    .unwrap();

    assert_eq!(&client.read("/dest/book.m4b").await.unwrap()[..], b"remote");
    assert_eq!(progress.transferred_bytes(), 6);
    assert!(
        client
            .read_dir("/dest")
            .await
            .unwrap()
            .iter()
            .all(|entry| !entry.name.starts_with(".zerofs-"))
    );
}

#[tokio::test]
async fn directory_skips_complete_files_and_replaces_partial_files() {
    let (client, _shutdown, local) = remote_client().await;
    client.create_dir_all("/dest/nested", 0o755).await.unwrap();
    client.write("/dest/complete.m4b", b"remote").await.unwrap();
    client
        .write("/dest/nested/partial.m4b", b"x")
        .await
        .unwrap();
    let source = local.path().join("Audiobooks");
    fs::create_dir_all(source.join("nested")).unwrap();
    fs::write(source.join("complete.m4b"), b"source").unwrap();
    fs::write(source.join("nested/partial.m4b"), b"finished").unwrap();
    let plan = scan_local(&source).unwrap();
    let progress = Progress::new("upload", plan.total_bytes, plan.files.len());

    execute_upload(
        std::slice::from_ref(&client),
        plan,
        Path::new("/dest"),
        true,
        progress.clone(),
        CancellationToken::new(),
    )
    .await
    .unwrap();

    assert_eq!(
        &client.read("/dest/complete.m4b").await.unwrap()[..],
        b"remote"
    );
    assert_eq!(
        &client.read("/dest/nested/partial.m4b").await.unwrap()[..],
        b"finished"
    );
    assert_eq!(progress.transferred_bytes(), 14);
}

#[tokio::test]
async fn rechecks_local_source_before_skipping() {
    let (client, _shutdown, local) = remote_client().await;
    client.create_dir_all("/dest", 0o755).await.unwrap();
    client.write("/dest/book.m4b", b"remote").await.unwrap();
    let source = local.path().join("book.m4b");
    fs::write(&source, b"source").unwrap();
    let plan = scan_local(&source).unwrap();
    fs::write(&source, b"x").unwrap();

    let error = execute_upload(
        std::slice::from_ref(&client),
        plan,
        Path::new("/dest/book.m4b"),
        true,
        Progress::new("upload", 6, 1),
        CancellationToken::new(),
    )
    .await
    .unwrap_err();

    assert!(
        format!("{error:#}").contains("local source changed while uploading"),
        "{error:#}"
    );
    assert_eq!(&client.read("/dest/book.m4b").await.unwrap()[..], b"remote");
}
