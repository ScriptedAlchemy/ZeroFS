mod copy;
mod plan;
mod progress;

use self::copy::upload_file;
use self::plan::{TransferPlan, scan_local};
use self::progress::Progress;
use anyhow::{Context, Result, bail};
use futures::StreamExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio_util::sync::CancellationToken;
use zerofs_client::{Client, ZeroFsError};

pub(crate) async fn run_upload(
    target: &str,
    source: PathBuf,
    destination: PathBuf,
    jobs: usize,
) -> Result<()> {
    if jobs == 0 {
        bail!("upload jobs must be at least 1");
    }
    let plan_source = source.clone();
    let plan = tokio::task::spawn_blocking(move || scan_local(&plan_source))
        .await
        .context("local transfer scan task failed")??;
    let client = Client::connect(target)
        .await
        .with_context(|| format!("connect to 9P target {target}"))?;
    let progress = Progress::new("upload", plan.total_bytes, plan.files.len());
    let cancellation = CancellationToken::new();
    let signal_cancellation = cancellation.clone();
    let signal = tokio::spawn(async move {
        if tokio::signal::ctrl_c().await.is_ok() {
            signal_cancellation.cancel();
        }
    });
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
    client.close().await;
    result
}

async fn execute_upload(
    client: Arc<Client>,
    plan: TransferPlan,
    destination: &Path,
    jobs: usize,
    progress: Progress,
    cancellation: CancellationToken,
) -> Result<()> {
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
    let results = futures::stream::iter(plan.files.into_iter().map(|file| {
        let client = Arc::clone(&client);
        let target = destination.join(&file.relative);
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

#[cfg(test)]
mod tests {
    use super::execute_upload;
    use super::plan::scan_local;
    use super::progress::Progress;
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
            if let Ok(client) = Client::connect(&target).await {
                return (client, shutdown, temp);
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        panic!("test 9P client did not connect");
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
}
