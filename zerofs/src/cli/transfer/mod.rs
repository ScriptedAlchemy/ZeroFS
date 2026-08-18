mod copy;
mod plan;
mod progress;

use self::copy::{download_file, upload_file};
use self::plan::{PlannedFile, TransferPlan, scan_local, scan_remote, validate_remote_child_name};
use self::progress::{DeleteProgress, Progress};
use crate::cli::{attach_cleanup_errors, has_attached_cleanup_error};
use anyhow::{Context, Result, bail};
use futures::StreamExt;
use std::collections::VecDeque;
use std::future::Future;
use std::os::unix::ffi::OsStringExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use tokio_util::sync::CancellationToken;
use zerofs_client::{Client, ConnectOptions, FileType, ZeroFsError};

const CLIENT_CLOSE_TIMEOUT: Duration = Duration::from_secs(5);
const TRANSFER_MSIZE: u32 = 9 * 1024 * 1024;
const FILE_TRANSFER_ATTEMPTS: usize = 3;
const LINUX_EAGAIN: i32 = 11;
/// How long cancelled work may settle before the CLI restates what it waits on.
const SETTLE_NOTICE_INTERVAL: Duration = Duration::from_secs(10);

async fn connect_transfer_client(target: &str) -> Result<Arc<Client>, ZeroFsError> {
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
    resume: bool,
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
    let clients = connect_workers(target, client, jobs.min(plan.files.len().max(1))).await?;
    let progress = Progress::new("upload", plan.total_bytes, plan.files.len());
    let (cancellation, signal) = cancellation_on_ctrl_c();
    let notice_progress = progress.clone();
    let result = with_settling_notices(
        execute_upload(
            &clients,
            plan,
            &destination,
            resume,
            progress,
            cancellation.clone(),
        ),
        cancellation,
        move |waited| notice_progress.settling(waited),
    )
    .await;
    signal.abort();
    finish_clients(&clients, result).await
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
    let clients = connect_workers(target, client, jobs.min(plan.files.len().max(1))).await?;
    let progress = Progress::new("download", plan.total_bytes, plan.files.len());
    let (cancellation, signal) = cancellation_on_ctrl_c();
    let notice_progress = progress.clone();
    let result = with_settling_notices(
        execute_download(&clients, plan, &destination, progress, cancellation.clone()),
        cancellation,
        move |waited| notice_progress.settling(waited),
    )
    .await;
    signal.abort();
    finish_clients(&clients, result).await
}

pub(crate) async fn run_delete(target: &str, path: PathBuf) -> Result<()> {
    let client = Client::connect(target)
        .await
        .with_context(|| format!("connect to 9P target {target}"))?;
    let (cancellation, signal) = cancellation_on_ctrl_c();
    let progress = DeleteProgress::new();
    let notice_progress = progress.clone();
    let result = with_settling_notices(
        execute_delete(Arc::clone(&client), &path, progress, cancellation.clone()),
        cancellation,
        move |waited| notice_progress.settling(waited),
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

async fn finish_clients<T>(clients: &[Arc<Client>], result: Result<T>) -> Result<T> {
    let cleanup = futures::future::join_all(clients.iter().map(|client| close_client(client)))
        .await
        .into_iter()
        .filter_map(Result::err)
        .collect::<Vec<_>>();
    match (result, cleanup.is_empty()) {
        (Ok(value), true) => Ok(value),
        (Ok(_), false) => Err(attach_cleanup_errors(
            anyhow::anyhow!("failed to close 9P transfer clients"),
            cleanup,
        )),
        (Err(primary), true) => Err(primary),
        (Err(primary), false) => Err(attach_cleanup_errors(primary, cleanup)),
    }
}

async fn connect_workers(
    target: &str,
    first: Arc<Client>,
    count: usize,
) -> Result<Vec<Arc<Client>>> {
    let mut clients = Vec::with_capacity(count);
    clients.push(first);
    while clients.len() < count {
        let worker = match connect_transfer_client(target).await.with_context(|| {
            format!(
                "connect transfer worker {}/{} to 9P target {target}",
                clients.len() + 1,
                count
            )
        }) {
            Ok(worker) => worker,
            Err(error) => return finish_clients(&clients, Err(error)).await,
        };
        clients.push(worker);
    }
    Ok(clients)
}

fn cancellation_on_ctrl_c() -> (CancellationToken, tokio::task::JoinHandle<()>) {
    let cancellation = CancellationToken::new();
    let signal_cancellation = cancellation.clone();
    let signal = tokio::spawn(async move {
        if tokio::signal::ctrl_c().await.is_ok() {
            signal_cancellation.cancel();
            if tokio::signal::ctrl_c().await.is_ok() {
                std::process::exit(130);
            }
        }
    });
    (cancellation, signal)
}

/// Repeats a settling notice for as long as cancelled work stays in flight.
///
/// A 9P reply wait has no aggregate deadline while its connection proves live,
/// so the first Ctrl-C can legitimately take a long time to settle. Never
/// returns: callers race it against the work being settled.
async fn announce_settling(cancellation: CancellationToken, notice: impl Fn(Duration)) {
    cancellation.cancelled().await;
    let mut waited = Duration::ZERO;
    loop {
        tokio::time::sleep(SETTLE_NOTICE_INTERVAL).await;
        waited += SETTLE_NOTICE_INTERVAL;
        notice(waited);
    }
}

async fn with_settling_notices<T>(
    work: impl Future<Output = T>,
    cancellation: CancellationToken,
    notice: impl Fn(Duration),
) -> T {
    tokio::select! {
        result = work => result,
        _ = announce_settling(cancellation, notice) => {
            unreachable!("the settling notice never completes")
        }
    }
}

async fn close_client(client: &Client) -> Result<()> {
    client.close().await;
    tokio::time::timeout(CLIENT_CLOSE_TIMEOUT, client.wait_for_cleanup())
        .await
        .context("timed out waiting for 9P client cleanup")
}

async fn run_file_workers<F, Fut>(
    clients: &[Arc<Client>],
    files: Vec<PlannedFile>,
    progress: Progress,
    cancellation: CancellationToken,
    transfer: F,
) -> Result<()>
where
    F: Fn(Arc<Client>, PlannedFile, CancellationToken) -> Fut + Clone,
    Fut: Future<Output = Result<()>>,
{
    let file_count = files.len();
    let queue = Arc::new(tokio::sync::Mutex::new(VecDeque::from(files)));
    // Settle every active transfer instead of dropping in-flight I/O on the first error.
    let workers = futures::future::join_all(clients.iter().take(file_count).map(|client| {
        let client = Arc::clone(client);
        let queue = Arc::clone(&queue);
        let cancellation = cancellation.clone();
        let progress = progress.clone();
        let transfer = transfer.clone();
        async move {
            let mut errors = Vec::new();
            loop {
                let Some(file) = queue.lock().await.pop_front() else {
                    return errors;
                };
                let path = if file.relative.as_os_str().is_empty() {
                    file.source.clone()
                } else {
                    file.relative.clone()
                };
                let mut attempts = 0;
                loop {
                    attempts += 1;
                    match transfer(Arc::clone(&client), file.clone(), cancellation.clone()).await {
                        Ok(()) => break,
                        Err(error)
                            if !cancellation.is_cancelled()
                                && attempts < FILE_TRANSFER_ATTEMPTS
                                && is_retryable_transfer_error(&error) =>
                        {
                            progress.retry_file(
                                &path,
                                attempts + 1,
                                FILE_TRANSFER_ATTEMPTS,
                                &error,
                            );
                            tokio::select! {
                                _ = cancellation.cancelled() => {
                                    errors.push(error.context("transfer cancelled while waiting to retry"));
                                    return errors;
                                }
                                _ = tokio::time::sleep(Duration::from_secs(attempts as u64)) => {}
                            }
                        }
                        Err(error) => {
                            progress.fail_file(&path, attempts, &error);
                            errors.push(error);
                            if cancellation.is_cancelled() {
                                return errors;
                            }
                            break;
                        }
                    }
                }
            }
        }
    }));
    let worker_errors = workers.await;
    let mut errors = worker_errors.into_iter().flatten().collect::<Vec<_>>();
    match errors.len() {
        0 => Ok(()),
        1 => Err(errors.remove(0)),
        count => {
            let details = errors
                .iter()
                .enumerate()
                .map(|(index, error)| format!("  {}. {error:#}", index + 1))
                .collect::<Vec<_>>()
                .join("\n");
            bail!("{count} file transfers failed:\n{details}")
        }
    }
}

fn is_retryable_transfer_error(error: &anyhow::Error) -> bool {
    if has_attached_cleanup_error(error) {
        return false;
    }
    error.chain().any(|cause| {
        matches!(
            cause.downcast_ref::<ZeroFsError>(),
            Some(
                ZeroFsError::ConnectFailed { .. }
                    | ZeroFsError::NotLeader { .. }
                    | ZeroFsError::Stale { .. }
                    | ZeroFsError::Io {
                        errno: LINUX_EAGAIN,
                        ..
                    }
            )
        )
    })
}

async fn execute_upload(
    clients: &[Arc<Client>],
    plan: TransferPlan,
    destination: &Path,
    resume: bool,
    progress: Progress,
    cancellation: CancellationToken,
) -> Result<()> {
    let client = clients.first().context("upload requires a 9P client")?;
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
            create_directory(client, &directory).await?;
        }
    } else if let Some(parent) = destination.parent() {
        client
            .create_dir_all(parent, 0o755)
            .await
            .with_context(|| format!("create remote parent directory {}", parent.display()))?;
    }

    let file_count = plan.files.len();
    let destination = destination.to_path_buf();
    let worker_progress = progress.clone();
    run_file_workers(
        clients,
        plan.files,
        progress.clone(),
        cancellation.child_token(),
        move |client, file, cancellation| {
            let target = file_destination(&destination, &file);
            upload_file(
                client,
                file,
                target,
                resume,
                worker_progress.clone(),
                cancellation,
            )
        },
    )
    .await?;
    if cancellation.is_cancelled() {
        bail!("upload cancelled");
    }
    if file_count == 0 {
        progress.syncing_directories();
        client.sync().await.context("sync uploaded directories")?;
        if cancellation.is_cancelled() {
            bail!("upload cancelled");
        }
    }
    progress.finish();
    Ok(())
}

async fn execute_download(
    clients: &[Arc<Client>],
    plan: TransferPlan,
    destination: &Path,
    progress: Progress,
    cancellation: CancellationToken,
) -> Result<()> {
    if clients.is_empty() {
        bail!("download requires a 9P client");
    }
    if plan.source_is_dir {
        for relative in &plan.directories {
            if cancellation.is_cancelled() {
                bail!("download cancelled");
            }
            let directory = destination.join(relative);
            reject_symlinked_download_directory(&directory).await?;
            tokio::fs::create_dir_all(&directory)
                .await
                .with_context(|| format!("create local directory {}", directory.display()))?;
        }
    } else if let Some(parent) = destination.parent() {
        tokio::fs::create_dir_all(parent)
            .await
            .with_context(|| format!("create local parent directory {}", parent.display()))?;
    }

    let destination = destination.to_path_buf();
    let worker_progress = progress.clone();
    run_file_workers(
        clients,
        plan.files,
        progress.clone(),
        cancellation.child_token(),
        move |client, file, cancellation| {
            let target = file_destination(&destination, &file);
            download_file(client, file, target, worker_progress.clone(), cancellation)
        },
    )
    .await?;
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
) -> Result<()> {
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
    progress.start(path);
    let removal = async {
        if cancellation.is_cancelled() {
            bail!("delete cancelled");
        }
        let metadata = client
            .stat(path)
            .await
            .with_context(|| format!("inspect remote path {}", path.display()))?;
        let levels = scan_delete_levels(&client, path, metadata.file_type, &cancellation).await?;
        for level in levels.into_iter().rev() {
            delete_level(
                Arc::clone(&client),
                level,
                progress.clone(),
                cancellation.clone(),
            )
            .await?;
            if cancellation.is_cancelled() {
                bail!("delete cancelled");
            }
        }
        Ok(())
    };
    let result = removal.await;
    match result {
        Ok(()) => {
            progress.finish(path);
            Ok(())
        }
        Err(error) => {
            progress.abandon();
            Err(error)
        }
    }
}

#[derive(Debug)]
struct DeleteEntry {
    path: PathBuf,
    file_type: FileType,
}

async fn scan_delete_levels(
    client: &Client,
    root: &Path,
    root_type: FileType,
    cancellation: &CancellationToken,
) -> Result<Vec<Vec<DeleteEntry>>> {
    let mut levels = vec![vec![DeleteEntry {
        path: root.to_path_buf(),
        file_type: root_type,
    }]];
    if root_type != FileType::Dir {
        return Ok(levels);
    }

    let mut directories = VecDeque::from([(root.to_path_buf(), 0usize)]);
    while let Some((directory, depth)) = directories.pop_front() {
        if cancellation.is_cancelled() {
            bail!("delete cancelled");
        }
        let entries = client
            .read_dir(&directory)
            .await
            .with_context(|| format!("list remote directory {}", directory.display()))?;
        for entry in entries {
            validate_remote_child_name(&entry.name_bytes, &directory)?;
            let path = directory.join(std::ffi::OsString::from_vec(entry.name_bytes));
            let child_depth = depth + 1;
            if levels.len() == child_depth {
                levels.push(Vec::new());
            }
            if entry.file_type == FileType::Dir {
                directories.push_back((path.clone(), child_depth));
            }
            levels[child_depth].push(DeleteEntry {
                path,
                file_type: entry.file_type,
            });
        }
    }
    Ok(levels)
}

async fn delete_level(
    client: Arc<Client>,
    entries: Vec<DeleteEntry>,
    progress: DeleteProgress,
    cancellation: CancellationToken,
) -> Result<()> {
    let results = futures::stream::iter(entries)
        .map(|entry| {
            let client = Arc::clone(&client);
            let progress = progress.clone();
            let cancellation = cancellation.clone();
            async move {
                if cancellation.is_cancelled() {
                    return Ok(());
                }
                match entry.file_type {
                    FileType::Dir => client.remove_dir(&entry.path).await.with_context(|| {
                        format!("remove remote directory {}", entry.path.display())
                    })?,
                    _ => client
                        .remove_file(&entry.path)
                        .await
                        .with_context(|| format!("remove remote file {}", entry.path.display()))?,
                }
                progress.removed(&entry.path);
                Ok(())
            }
        })
        .buffer_unordered(8)
        .collect::<Vec<Result<()>>>()
        .await;
    let mut errors = results
        .into_iter()
        .filter_map(Result::err)
        .collect::<Vec<_>>();
    match errors.len() {
        0 => Ok(()),
        1 => Err(errors.remove(0)),
        count => {
            let details = errors
                .iter()
                .enumerate()
                .map(|(index, error)| format!("  {}. {error:#}", index + 1))
                .collect::<Vec<_>>()
                .join("\n");
            bail!("{count} delete operations failed:\n{details}")
        }
    }
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

async fn reject_symlinked_download_directory(path: &Path) -> Result<()> {
    match tokio::fs::symlink_metadata(path).await {
        Ok(metadata) if metadata.file_type().is_symlink() => {
            bail!(
                "local destination contains a symbolic link: {}",
                path.display()
            )
        }
        Ok(_) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error)
            .with_context(|| format!("inspect local destination directory {}", path.display())),
    }
}

#[cfg(test)]
mod tests;
