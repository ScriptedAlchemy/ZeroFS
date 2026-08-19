use super::UploadWorker;
use super::local_destination::LocalFileTarget;
use super::plan::PlannedFile;
use super::progress::{FileProgress, Progress};
use crate::cli::attach_cleanup_errors;
use anyhow::{Context, Result, anyhow, bail};
use futures::stream::{FuturesUnordered, StreamExt};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;
use zerofs_client::{Client, File, OpenOptions};

const UPLOAD_PIPELINE_DEPTH_PER_CONNECTION: usize = 2;

pub(super) async fn upload_file(
    worker: UploadWorker,
    planned: PlannedFile,
    destination: PathBuf,
    resume: bool,
    progress: Progress,
    cancellation: CancellationToken,
) -> Result<()> {
    let client = worker.primary();
    if cancellation.is_cancelled() {
        bail!("upload cancelled");
    }
    match client.stat(&destination).await {
        Ok(metadata) if metadata.is_dir() => {
            bail!(
                "remote destination is a directory: {}",
                destination.display()
            )
        }
        Ok(metadata) if resume && metadata.is_file() && metadata.size == planned.size => {
            drop(open_local_source(&planned).await?);
            progress.skip_file(progress_path(&planned), planned.size);
            return Ok(());
        }
        Ok(_) | Err(zerofs_client::ZeroFsError::NotFound { .. }) => {}
        Err(error) => {
            return Err(error)
                .with_context(|| format!("inspect remote destination {}", destination.display()));
        }
    }
    let file_progress = progress.start_file(progress_path(&planned), planned.size);
    let temp = temporary_sibling(&destination)?;
    let primary_remote = client
        .open(
            &temp,
            OpenOptions::write_only().create_new(true).mode(0o644),
        )
        .await
        .with_context(|| format!("create remote temporary file {}", temp.display()))?;
    let mut remotes = vec![primary_remote];
    for (index, stream_client) in worker.clients().iter().enumerate().skip(1) {
        match stream_client
            .open(&temp, OpenOptions::write_only())
            .await
            .with_context(|| {
                format!(
                    "open remote temporary file {} for upload stream {}",
                    temp.display(),
                    index + 1
                )
            }) {
            Ok(remote) => remotes.push(remote),
            Err(primary) => {
                close_remote_files(&remotes).await;
                return Err(cleanup_remote_temp(client, &temp, primary).await);
            }
        }
    }
    let result = stream_upload(
        &remotes,
        &planned,
        client.capabilities().max_write_chunk.max(1) as usize,
        &file_progress,
        &cancellation,
    )
    .await
    .with_context(|| {
        format!(
            "upload {} to {}",
            planned.source.display(),
            destination.display()
        )
    });
    if let Err(primary) = result {
        close_remote_files(&remotes).await;
        return Err(cleanup_remote_temp(client, &temp, primary).await);
    }
    if let Err(primary) = sync_remote_files(&remotes).await {
        close_remote_files(&remotes).await;
        return Err(cleanup_remote_temp(client, &temp, primary).await);
    }
    if cancellation.is_cancelled() {
        close_remote_files(&remotes).await;
        return Err(cleanup_remote_temp(client, &temp, anyhow!("upload cancelled")).await);
    }
    close_remote_files(&remotes[1..]).await;
    let remote = &remotes[0];
    if let Err(primary) = client.rename(&temp, &destination).await.with_context(|| {
        format!(
            "publish remote file {} as {}",
            temp.display(),
            destination.display()
        )
    }) {
        remote.close().await;
        return Err(cleanup_remote_temp(client, &temp, primary).await);
    }
    let sync = client
        .sync()
        .await
        .with_context(|| format!("sync published remote file {}", destination.display()));
    remote.close().await;
    sync?;
    file_progress.finish();
    Ok(())
}

pub(super) async fn download_file(
    client: Arc<Client>,
    planned: PlannedFile,
    destination: LocalFileTarget,
    progress: Progress,
    cancellation: CancellationToken,
) -> Result<()> {
    if cancellation.is_cancelled() {
        bail!("download cancelled");
    }
    let file_progress = progress.start_file(progress_path(&planned), planned.size);
    let remote = client
        .open(&planned.source, OpenOptions::read_only())
        .await
        .with_context(|| format!("open remote source {}", planned.source.display()))?;
    let temporary = destination.create_temporary();
    let mut temporary = match temporary {
        Ok(temporary) => temporary,
        Err(error) => {
            remote.close().await;
            return Err(error);
        }
    };
    let result = stream_download(
        &remote,
        &mut temporary.file,
        &planned,
        client.capabilities().max_read_chunk.max(1),
        &file_progress,
        &cancellation,
    )
    .await
    .with_context(|| {
        format!(
            "download {} to {}",
            planned.source.display(),
            destination.path().display()
        )
    });
    remote.close().await;
    let result = match result {
        Ok(()) => temporary
            .file
            .sync_all()
            .await
            .with_context(|| format!("sync local temporary file {}", temporary.path.display())),
        Err(error) => Err(error),
    };
    let result = match result {
        Ok(()) if cancellation.is_cancelled() => Err(anyhow!("download cancelled")),
        other => other,
    };
    let temp_name = temporary.name;
    let temp_path = temporary.path;
    drop(temporary.file);
    if let Err(primary) = result {
        return Err(destination.cleanup_temporary(&temp_name, &temp_path, primary));
    }
    if let Err(primary) = destination.publish_temporary(&temp_name, &temp_path) {
        return Err(destination.cleanup_temporary(&temp_name, &temp_path, primary));
    }
    file_progress.finish();
    Ok(())
}

pub(super) async fn stream_upload(
    remotes: &[Arc<File>],
    planned: &PlannedFile,
    chunk_size: usize,
    progress: &FileProgress,
    cancellation: &CancellationToken,
) -> Result<()> {
    if remotes.is_empty() {
        bail!("upload requires at least one remote file handle");
    }
    let mut local = open_local_source(planned).await?;
    let buffer_size = upload_buffer_size(planned.size, chunk_size);
    let mut buffers = (0..UPLOAD_PIPELINE_DEPTH_PER_CONNECTION * remotes.len())
        .map(|_| vec![0; buffer_size])
        .collect::<Vec<_>>();
    let mut writes = FuturesUnordered::new();
    let mut in_flight = vec![0usize; remotes.len()];
    let mut first_error = None;
    let mut offset = 0u64;
    let mut next_remote = 0usize;
    while first_error.is_none() && offset < planned.size {
        if buffers.is_empty() {
            let (remote_index, buffer, result) = writes.next().await.expect("full upload pipeline");
            in_flight[remote_index] -= 1;
            buffers.push(buffer);
            match result {
                Ok(written) => progress.advance(written as u64),
                Err(error) => first_error = Some(error),
            }
            continue;
        }
        if cancellation.is_cancelled() {
            first_error = Some(anyhow::anyhow!("upload cancelled"));
            break;
        }
        let mut buffer = buffers.pop().expect("non-empty upload buffer pool");
        let wanted = (planned.size - offset).min(chunk_size as u64) as usize;
        if let Err(error) = local.read_exact(&mut buffer[..wanted]).await {
            if error.kind() == std::io::ErrorKind::UnexpectedEof {
                first_error = Some(anyhow::anyhow!(
                    "local source changed while uploading: {}",
                    planned.source.display()
                ));
            } else {
                first_error = Some(
                    anyhow::Error::new(error)
                        .context(format!("read local source {}", planned.source.display())),
                );
            }
            buffers.push(buffer);
            break;
        }
        let remote_index = (0..remotes.len())
            .map(|step| (next_remote + step) % remotes.len())
            .find(|&index| in_flight[index] < UPLOAD_PIPELINE_DEPTH_PER_CONNECTION)
            .expect("an available upload buffer implies an available remote slot");
        writes.push(write_upload_chunk(
            remote_index,
            Arc::clone(&remotes[remote_index]),
            offset,
            buffer,
            wanted,
        ));
        in_flight[remote_index] += 1;
        next_remote = (remote_index + 1) % remotes.len();
        offset += wanted as u64;
    }
    while let Some((remote_index, _buffer, result)) = writes.next().await {
        in_flight[remote_index] -= 1;
        match result {
            Ok(written) => progress.advance(written as u64),
            Err(error) if first_error.is_none() => first_error = Some(error),
            Err(_) => {}
        }
    }
    if let Some(error) = first_error {
        return Err(error);
    }
    if cancellation.is_cancelled() {
        bail!("upload cancelled");
    }
    let mut extra = [0u8; 1];
    if local
        .read(&mut extra)
        .await
        .with_context(|| format!("finish reading local source {}", planned.source.display()))?
        != 0
    {
        bail!(
            "local source changed while uploading: {}",
            planned.source.display()
        );
    }
    Ok(())
}

async fn close_remote_files(remotes: &[Arc<File>]) {
    futures::future::join_all(remotes.iter().map(|remote| remote.close())).await;
}

pub(super) async fn sync_remote_files(remotes: &[Arc<File>]) -> Result<()> {
    for (index, result) in futures::future::join_all(
        remotes
            .iter()
            .enumerate()
            .map(|(index, remote)| async move { (index, remote.sync_all().await) }),
    )
    .await
    {
        result.with_context(|| format!("sync remote upload stream {}", index + 1))?;
    }
    Ok(())
}

async fn write_upload_chunk(
    remote_index: usize,
    remote: Arc<File>,
    offset: u64,
    buffer: Vec<u8>,
    length: usize,
) -> (usize, Vec<u8>, Result<usize>) {
    let result = remote
        .write_at(offset, &buffer[..length])
        .await
        .with_context(|| format!("write remote destination at offset {offset}"))
        .map(|()| length);
    (remote_index, buffer, result)
}

async fn open_local_source(planned: &PlannedFile) -> Result<tokio::fs::File> {
    let mut options = tokio::fs::OpenOptions::new();
    options.read(true).custom_flags(libc::O_NOFOLLOW);
    let local = options
        .open(&planned.source)
        .await
        .with_context(|| format!("open local source {}", planned.source.display()))?;
    let metadata = local
        .metadata()
        .await
        .with_context(|| format!("inspect local source {}", planned.source.display()))?;
    if !metadata.is_file() || metadata.len() != planned.size {
        bail!(
            "local source changed while uploading: {}",
            planned.source.display()
        );
    }
    Ok(local)
}

async fn stream_download(
    remote: &zerofs_client::File,
    local: &mut tokio::fs::File,
    planned: &PlannedFile,
    chunk_size: u32,
    progress: &FileProgress,
    cancellation: &CancellationToken,
) -> Result<()> {
    let mut offset = 0u64;
    while offset < planned.size {
        if cancellation.is_cancelled() {
            bail!("download cancelled");
        }
        let wanted = (planned.size - offset).min(u64::from(chunk_size)) as u32;
        let chunk = tokio::select! {
            biased;
            _ = cancellation.cancelled() => bail!("download cancelled"),
            result = remote.read_at(offset, wanted) => {
                result.with_context(|| format!("read remote source at offset {offset}"))?
            }
        };
        if chunk.len() > wanted as usize {
            bail!(
                "remote source returned more than the planned read size: {}",
                planned.source.display()
            );
        }
        if chunk.is_empty() {
            break;
        }
        local
            .write_all(&chunk)
            .await
            .with_context(|| format!("write local temporary file at offset {offset}"))?;
        offset += chunk.len() as u64;
        progress.advance(chunk.len() as u64);
    }
    if offset != planned.size {
        bail!(
            "remote source changed while downloading: {}",
            planned.source.display()
        );
    }
    let trailing = tokio::select! {
        biased;
        _ = cancellation.cancelled() => bail!("download cancelled"),
        result = remote.read_at(offset, 1) => {
            result.with_context(|| format!("check remote source length at offset {offset}"))?
        }
    };
    if !trailing.is_empty() {
        bail!(
            "remote source changed while downloading: {}",
            planned.source.display()
        );
    }
    Ok(())
}

fn temporary_sibling(destination: &Path) -> Result<PathBuf> {
    let parent = destination
        .parent()
        .context("destination has no parent directory")?;
    Ok(parent.join(format!(".zerofs-{}.tmp", Uuid::new_v4())))
}

fn progress_path(planned: &PlannedFile) -> &Path {
    if planned.relative.as_os_str().is_empty() {
        &planned.source
    } else {
        &planned.relative
    }
}

fn upload_buffer_size(file_size: u64, chunk_size: usize) -> usize {
    file_size.min(chunk_size as u64) as usize
}

async fn cleanup_remote_temp(
    client: &Client,
    temp: &Path,
    primary: anyhow::Error,
) -> anyhow::Error {
    let cleanup = match client.remove_file(temp).await {
        Ok(()) | Err(zerofs_client::ZeroFsError::NotFound { .. }) => Vec::new(),
        Err(error) => vec![
            anyhow::Error::from(error)
                .context(format!("remove remote temporary file {}", temp.display())),
        ],
    };
    attach_cleanup_errors(primary, cleanup)
}

#[cfg(test)]
mod tests {
    use super::upload_buffer_size;

    #[test]
    fn upload_buffers_never_exceed_the_file_or_remaining_chunk() {
        assert_eq!(upload_buffer_size(0, 9 * 1024 * 1024), 0);
        assert_eq!(upload_buffer_size(7, 9 * 1024 * 1024), 7);
        assert_eq!(upload_buffer_size(20, 8), 8);
    }
}
