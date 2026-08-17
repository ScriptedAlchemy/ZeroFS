mod copy;
mod plan;
mod progress;

use self::copy::{download_file, upload_file};
use self::plan::{TransferPlan, scan_local, scan_remote};
use self::progress::{DeleteProgress, Progress};
use crate::cli::attach_cleanup_errors;
use anyhow::{Context, Result, bail};
use futures::StreamExt;
use std::future::Future;
use std::os::unix::ffi::OsStringExt;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use tokio_util::sync::CancellationToken;
use zerofs_client::{Client, ConnectOptions, FileType, ZeroFsError};

const DELETE_CONCURRENCY: usize = 8;
const CLIENT_CLOSE_TIMEOUT: Duration = Duration::from_secs(5);
const TRANSFER_MSIZE: u32 = 9 * 1024 * 1024;

async fn connect_transfer_client(target: &str) -> std::result::Result<Arc<Client>, ZeroFsError> {
    Client::connect_with(
        target,
        ConnectOptions {
            msize: TRANSFER_MSIZE,
            ..ConnectOptions::default()
        },
    )
    .await
}

pub(crate) async fn run_upload(
    target: &str,
    source: PathBuf,
    destination: PathBuf,
    jobs: usize,
) -> Result<()> {
    if jobs == 0 {
        bail!("upload jobs must be at least 1");
    }
    let plan = tokio::task::spawn_blocking(move || scan_local(&source))
        .await
        .context("local transfer scan task failed")??;
    let client = connect_transfer_client(target)
        .await
        .with_context(|| format!("connect to 9P target {target}"))?;
    let progress = Progress::new("upload", plan.total_bytes, plan.files.len());
    let (cancellation, signal) = cancellation_on_ctrl_c();
    let result = execute_upload(
        Arc::clone(&client),
        plan,
        &destination,
        jobs,
        progress,
        cancellation,
    )
    .await;
    signal.abort();
    finish_client(&client, result).await
}

pub(crate) async fn run_download(
    target: &str,
    source: PathBuf,
    destination: PathBuf,
    jobs: usize,
) -> Result<()> {
    if jobs == 0 {
        bail!("download jobs must be at least 1");
    }
    let client = connect_transfer_client(target)
        .await
        .with_context(|| format!("connect to 9P target {target}"))?;
    let plan = match scan_remote(&client, &source).await {
        Ok(plan) => plan,
        Err(error) => return finish_client(&client, Err(error)).await,
    };
    let progress = Progress::new("download", plan.total_bytes, plan.files.len());
    let (cancellation, signal) = cancellation_on_ctrl_c();
    let result = execute_download(
        Arc::clone(&client),
        plan,
        &destination,
        jobs,
        progress,
        cancellation,
    )
    .await;
    signal.abort();
    finish_client(&client, result).await
}

pub(crate) async fn run_delete(target: &str, path: PathBuf) -> Result<()> {
    let client = Client::connect(target)
        .await
        .with_context(|| format!("connect to 9P target {target}"))?;
    let (cancellation, signal) = cancellation_on_ctrl_c();
    let result = execute_delete(
        Arc::clone(&client),
        &path,
        DeleteProgress::new(),
        cancellation,
    )
    .await;
    signal.abort();
    finish_client(&client, result).await.map(|_| ())
}

async fn finish_client<T>(client: &Client, result: Result<T>) -> Result<T> {
    let cleanup = close_client(client).await.err();
    match (result, cleanup) {
        (Ok(value), None) => Ok(value),
        (Ok(_), Some(cleanup)) => Err(cleanup),
        (Err(primary), None) => Err(primary),
        (Err(primary), Some(cleanup)) => Err(attach_cleanup_errors(primary, vec![cleanup])),
    }
}

fn cancellation_on_ctrl_c() -> (CancellationToken, tokio::task::JoinHandle<()>) {
    let cancellation = CancellationToken::new();
    let signal_cancellation = cancellation.clone();
    let signal = tokio::spawn(async move {
        if tokio::signal::ctrl_c().await.is_ok() {
            signal_cancellation.cancel();
        }
    });
    (cancellation, signal)
}

async fn close_client(client: &Client) -> Result<()> {
    client.close().await;
    tokio::time::timeout(CLIENT_CLOSE_TIMEOUT, client.wait_for_cleanup())
        .await
        .context("timed out waiting for 9P client cleanup")
}

async fn execute_upload(
    client: Arc<Client>,
    plan: TransferPlan,
    destination: &Path,
    jobs: usize,
    progress: Progress,
    cancellation: CancellationToken,
) -> Result<()> {
    if jobs == 0 {
        bail!("upload jobs must be at least 1");
    }
    if plan.source_is_dir {
        client
            .create_dir_all(destination, 0o755)
            .await
            .with_context(|| format!("create remote directory {}", destination.display()))?;
        for relative in &plan.directories {
            if relative.as_os_str().is_empty() {
                continue;
            }
            if cancellation.is_cancelled() {
                bail!("upload cancelled");
            }
            let directory = destination.join(relative);
            create_directory(&client, &directory).await?;
        }
    } else if let Some(parent) = destination.parent() {
        client
            .create_dir_all(parent, 0o755)
            .await
            .with_context(|| format!("create remote parent directory {}", parent.display()))?;
    }

    let file_count = plan.files.len();
    let batch_cancellation = cancellation.child_token();
    // Settle every active mutation instead of dropping in-flight writes on the first error.
    let results = futures::stream::iter(plan.files.into_iter().map(|file| {
        let client = Arc::clone(&client);
        let target = file_destination(destination, &file);
        let progress = progress.clone();
        let cancellation = batch_cancellation.clone();
        async move {
            let result = upload_file(client, file, target, progress, cancellation.clone()).await;
            if result.is_err() {
                cancellation.cancel();
            }
            result
        }
    }))
    .buffer_unordered(jobs)
    .collect::<Vec<_>>()
    .await;
    if let Some(error) = results.into_iter().find_map(Result::err) {
        return Err(error);
    }
    if cancellation.is_cancelled() {
        bail!("upload cancelled");
    }
    if file_count == 0 {
        progress.syncing_directories();
        client.sync().await.context("sync uploaded directories")?;
    }
    progress.finish();
    Ok(())
}

async fn execute_download(
    client: Arc<Client>,
    plan: TransferPlan,
    destination: &Path,
    jobs: usize,
    progress: Progress,
    cancellation: CancellationToken,
) -> Result<()> {
    if jobs == 0 {
        bail!("download jobs must be at least 1");
    }
    if plan.source_is_dir {
        for relative in &plan.directories {
            if cancellation.is_cancelled() {
                bail!("download cancelled");
            }
            let directory = destination.join(relative);
            tokio::fs::create_dir_all(&directory)
                .await
                .with_context(|| format!("create local directory {}", directory.display()))?;
        }
    } else if let Some(parent) = destination.parent() {
        tokio::fs::create_dir_all(parent)
            .await
            .with_context(|| format!("create local parent directory {}", parent.display()))?;
    }

    let batch_cancellation = cancellation.child_token();
    // Settle every active read before returning the first error.
    let results = futures::stream::iter(plan.files.into_iter().map(|file| {
        let client = Arc::clone(&client);
        let target = file_destination(destination, &file);
        let progress = progress.clone();
        let cancellation = batch_cancellation.clone();
        async move {
            let result = download_file(client, file, target, progress, cancellation.clone()).await;
            if result.is_err() {
                cancellation.cancel();
            }
            result
        }
    }))
    .buffer_unordered(jobs)
    .collect::<Vec<_>>()
    .await;
    if let Some(error) = results.into_iter().find_map(Result::err) {
        return Err(error);
    }
    if cancellation.is_cancelled() {
        bail!("download cancelled");
    }
    progress.finish();
    Ok(())
}

async fn execute_delete(
    client: Arc<Client>,
    path: &Path,
    progress: DeleteProgress,
    cancellation: CancellationToken,
) -> Result<u64> {
    let mut has_name = false;
    for component in path.components() {
        match component {
            std::path::Component::Normal(_) => has_name = true,
            std::path::Component::ParentDir => {
                bail!("refusing to remove a path containing '..'")
            }
            _ => {}
        }
    }
    if !has_name {
        bail!("refusing to remove the 9P attach root");
    }
    let result = async {
        if cancellation.is_cancelled() {
            bail!("delete cancelled");
        }
        let metadata = client
            .stat(path)
            .await
            .with_context(|| format!("inspect remote path {}", path.display()))?;
        let deleted = if metadata.file_type == FileType::Dir {
            let children = delete_directory_contents(
                Arc::clone(&client),
                path.to_path_buf(),
                progress.clone(),
                cancellation.clone(),
            )
            .await?;
            if cancellation.is_cancelled() {
                bail!("delete cancelled");
            }
            client
                .remove_dir(path)
                .await
                .with_context(|| format!("remove remote directory {}", path.display()))?;
            progress.deleted(path);
            children + 1
        } else {
            client
                .remove_file(path)
                .await
                .with_context(|| format!("remove remote file {}", path.display()))?;
            progress.deleted(path);
            1
        };
        Ok(deleted)
    }
    .await;
    match result {
        Ok(deleted) => {
            progress.finish();
            Ok(deleted)
        }
        Err(error) => {
            progress.abandon();
            Err(error)
        }
    }
}

type DeleteFuture = Pin<Box<dyn Future<Output = Result<u64>> + Send>>;

fn delete_directory_contents(
    client: Arc<Client>,
    directory: PathBuf,
    progress: DeleteProgress,
    cancellation: CancellationToken,
) -> DeleteFuture {
    Box::pin(async move {
        let entries = client
            .read_dir(&directory)
            .await
            .with_context(|| format!("read remote directory {}", directory.display()))?;
        // Settle every active unlink before reporting the first failed entry.
        let results = futures::stream::iter(entries.into_iter().map(|entry| {
            let client = Arc::clone(&client);
            let child = directory.join(std::ffi::OsString::from_vec(entry.name_bytes));
            let progress = progress.clone();
            let cancellation = cancellation.clone();
            async move {
                if cancellation.is_cancelled() {
                    bail!("delete cancelled");
                }
                let descendants = if entry.file_type == FileType::Dir {
                    let descendants = delete_directory_contents(
                        Arc::clone(&client),
                        child.clone(),
                        progress.clone(),
                        cancellation.clone(),
                    )
                    .await?;
                    if cancellation.is_cancelled() {
                        bail!("delete cancelled");
                    }
                    client
                        .remove_dir(&child)
                        .await
                        .with_context(|| format!("remove remote directory {}", child.display()))?;
                    descendants
                } else {
                    client
                        .remove_file(&child)
                        .await
                        .with_context(|| format!("remove remote file {}", child.display()))?;
                    0
                };
                progress.deleted(&child);
                Ok(descendants + 1)
            }
        }))
        .buffer_unordered(DELETE_CONCURRENCY)
        .collect::<Vec<Result<u64>>>()
        .await;
        results.into_iter().try_fold(0u64, |total, result| {
            total
                .checked_add(result?)
                .context("deleted entry count exceeds u64")
        })
    })
}

async fn create_directory(client: &Client, path: &Path) -> Result<()> {
    match client.create_dir(path, 0o755).await {
        Ok(_) => Ok(()),
        Err(ZeroFsError::AlreadyExists { .. }) => {
            let metadata = client
                .metadata(path)
                .await
                .with_context(|| format!("inspect remote directory {}", path.display()))?;
            if metadata.is_dir() {
                Ok(())
            } else {
                bail!("remote destination is not a directory: {}", path.display())
            }
        }
        Err(error) => {
            Err(error).with_context(|| format!("create remote directory {}", path.display()))
        }
    }
}

fn file_destination(root: &Path, file: &plan::PlannedFile) -> PathBuf {
    if file.relative.as_os_str().is_empty() {
        root.to_path_buf()
    } else {
        root.join(&file.relative)
    }
}

#[cfg(test)]
mod tests {
    use super::plan::{scan_local, scan_remote};
    use super::progress::{DeleteProgress, Progress};
    use super::{
        TRANSFER_MSIZE, close_client, connect_transfer_client, execute_delete, execute_download,
        execute_upload,
    };
    use crate::fs::ZeroFS;
    use crate::ninep::NinePServer;
    use std::fs;
    use std::path::Path;
    use std::sync::Arc;
    use std::time::Duration;
    use tokio_util::sync::CancellationToken;
    use zerofs_client::Client;

    async fn remote_client() -> (Arc<Client>, CancellationToken, tempfile::TempDir) {
        let filesystem = Arc::new(ZeroFS::new_in_memory().await.unwrap());
        let temp = tempfile::tempdir().unwrap();
        let socket = temp.path().join("transfer.9p.sock");
        let server = NinePServer::new_unix(filesystem, socket.clone());
        let shutdown = CancellationToken::new();
        let server_shutdown = shutdown.clone();
        tokio::spawn(async move { server.start(server_shutdown).await.unwrap() });
        let target = format!("unix:{}", socket.display());
        for _ in 0..100 {
            if let Ok(client) = connect_transfer_client(&target).await {
                return (client, shutdown, temp);
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        panic!("test 9P client did not connect");
    }

    #[tokio::test]
    async fn transfer_client_negotiates_nine_mibibyte_messages() {
        let (client, shutdown, _temp) = remote_client().await;

        assert_eq!(client.capabilities().msize, TRANSFER_MSIZE);
        close_client(&client).await.unwrap();
        shutdown.cancel();
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
            Arc::clone(&client),
            plan,
            Path::new("/dest"),
            8,
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
            operations_after - operations_before <= 24,
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
            Arc::clone(&client),
            plan,
            Path::new("/dest"),
            1,
            Progress::new("upload", 15, 2),
            CancellationToken::new(),
        )
        .await
        .unwrap_err();

        assert!(error.to_string().contains("open local source"), "{error:#}");
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
            Arc::clone(&client),
            plan,
            &destination,
            8,
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
        let upload_plan = scan_local(&source).unwrap();
        execute_upload(
            Arc::clone(&client),
            upload_plan,
            Path::new("/uploaded.bin"),
            1,
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

        execute_download(
            Arc::clone(&client),
            download_plan,
            &destination,
            1,
            Progress::new("download", 7, 1),
            CancellationToken::new(),
        )
        .await
        .unwrap();

        assert_eq!(fs::read(destination).unwrap(), b"payload");
    }

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
            Arc::clone(&client),
            plan,
            &destination,
            1,
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

        let deleted = execute_delete(
            Arc::clone(&client),
            Path::new("/remove"),
            DeleteProgress::new(),
            CancellationToken::new(),
        )
        .await
        .unwrap();

        assert_eq!(deleted, 5);
        assert!(client.stat("/remove").await.is_err());
        assert_eq!(
            execute_delete(
                Arc::clone(&client),
                Path::new("/single.txt"),
                DeleteProgress::new(),
                CancellationToken::new(),
            )
            .await
            .unwrap(),
            1
        );
        assert!(client.stat("/single.txt").await.is_err());
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
    async fn cli_close_waits_for_all_fid_replies() {
        let (client, _shutdown, _local) = remote_client().await;
        client.write("/file.txt", b"payload").await.unwrap();

        close_client(&client).await.unwrap();

        assert_eq!(client.outstanding_fids(), 0);
    }
}
