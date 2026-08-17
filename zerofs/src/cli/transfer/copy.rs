use super::plan::PlannedFile;
use super::progress::Progress;
use crate::cli::attach_cleanup_errors;
use anyhow::{Context, Result, bail};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;
use zerofs_client::{Client, OpenOptions};

pub(super) async fn upload_file(
    client: Arc<Client>,
    planned: PlannedFile,
    destination: PathBuf,
    progress: Progress,
    cancellation: CancellationToken,
) -> Result<()> {
    if cancellation.is_cancelled() {
        bail!("upload cancelled");
    }
    progress.start_file(progress_path(&planned));
    let temp = temporary_sibling(&destination)?;
    let remote = client
        .open(
            &temp,
            OpenOptions::write_only().create_new(true).mode(0o644),
        )
        .await
        .with_context(|| format!("create remote temporary file {}", temp.display()))?;
    let result = stream_upload(
        &remote,
        &planned,
        client.capabilities().max_write_chunk.max(1) as usize,
        &progress,
        &cancellation,
    )
    .await;
    if let Err(primary) = result {
        remote.close().await;
        return Err(cleanup_remote_temp(&client, &temp, primary).await);
    }
    if let Err(primary) = client.rename(&temp, &destination).await.with_context(|| {
        format!(
            "publish remote file {} as {}",
            temp.display(),
            destination.display()
        )
    }) {
        remote.close().await;
        return Err(cleanup_remote_temp(&client, &temp, primary).await);
    }
    let sync = remote
        .sync_all()
        .await
        .with_context(|| format!("sync remote file {}", destination.display()));
    remote.close().await;
    sync?;
    progress.finish_file(progress_path(&planned));
    Ok(())
}

pub(super) async fn download_file(
    client: Arc<Client>,
    planned: PlannedFile,
    destination: PathBuf,
    progress: Progress,
    cancellation: CancellationToken,
) -> Result<()> {
    if cancellation.is_cancelled() {
        bail!("download cancelled");
    }
    progress.start_file(progress_path(&planned));
    let temp = temporary_sibling(&destination)?;
    let remote = client
        .open(&planned.source, OpenOptions::read_only())
        .await
        .with_context(|| format!("open remote source {}", planned.source.display()))?;
    let local = tokio::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&temp)
        .await;
    let mut local = match local {
        Ok(local) => local,
        Err(error) => {
            remote.close().await;
            return Err(error)
                .with_context(|| format!("create local temporary file {}", temp.display()));
        }
    };
    let result = stream_download(
        &remote,
        &mut local,
        &planned,
        client.capabilities().max_read_chunk.max(1),
        &progress,
        &cancellation,
    )
    .await;
    remote.close().await;
    let result = match result {
        Ok(()) => local
            .sync_all()
            .await
            .with_context(|| format!("sync local temporary file {}", temp.display())),
        Err(error) => Err(error),
    };
    drop(local);
    if let Err(primary) = result {
        return Err(cleanup_local_temp(&temp, primary).await);
    }
    if let Err(primary) = tokio::fs::rename(&temp, &destination)
        .await
        .with_context(|| {
            format!(
                "publish local file {} as {}",
                temp.display(),
                destination.display()
            )
        })
    {
        return Err(cleanup_local_temp(&temp, primary).await);
    }
    progress.finish_file(progress_path(&planned));
    Ok(())
}

async fn stream_upload(
    remote: &zerofs_client::File,
    planned: &PlannedFile,
    chunk_size: usize,
    progress: &Progress,
    cancellation: &CancellationToken,
) -> Result<()> {
    let mut local = tokio::fs::File::open(&planned.source)
        .await
        .with_context(|| format!("open local source {}", planned.source.display()))?;
    let mut buffer = vec![0; chunk_size];
    let mut offset = 0u64;
    while offset < planned.size {
        if cancellation.is_cancelled() {
            bail!("upload cancelled");
        }
        let wanted = (planned.size - offset).min(chunk_size as u64) as usize;
        let read = local
            .read(&mut buffer[..wanted])
            .await
            .with_context(|| format!("read local source {}", planned.source.display()))?;
        if read == 0 {
            bail!(
                "local source changed while uploading: {}",
                planned.source.display()
            );
        }
        remote
            .write_at(offset, &buffer[..read])
            .await
            .with_context(|| format!("write remote destination at offset {offset}"))?;
        offset += read as u64;
        progress.advance(read as u64);
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

async fn stream_download(
    remote: &zerofs_client::File,
    local: &mut tokio::fs::File,
    planned: &PlannedFile,
    chunk_size: u32,
    progress: &Progress,
    cancellation: &CancellationToken,
) -> Result<()> {
    let mut offset = 0u64;
    loop {
        if cancellation.is_cancelled() {
            bail!("download cancelled");
        }
        let chunk = remote
            .read_at(offset, chunk_size)
            .await
            .with_context(|| format!("read remote source at offset {offset}"))?;
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

async fn cleanup_remote_temp(
    client: &Client,
    temp: &Path,
    primary: anyhow::Error,
) -> anyhow::Error {
    let cleanup = client
        .remove_file(temp)
        .await
        .with_context(|| format!("remove remote temporary file {}", temp.display()))
        .err()
        .into_iter()
        .collect();
    attach_cleanup_errors(primary, cleanup)
}

async fn cleanup_local_temp(temp: &Path, primary: anyhow::Error) -> anyhow::Error {
    let cleanup = tokio::fs::remove_file(temp)
        .await
        .with_context(|| format!("remove local temporary file {}", temp.display()))
        .err()
        .into_iter()
        .collect();
    attach_cleanup_errors(primary, cleanup)
}
