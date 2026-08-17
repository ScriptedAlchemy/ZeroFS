use super::plan::PlannedFile;
use super::progress::Progress;
use crate::cli::attach_cleanup_errors;
use anyhow::{Context, Result, bail};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::io::AsyncReadExt;
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
    progress.start_file(&planned.relative);
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
        let cleanup = client
            .remove_file(&temp)
            .await
            .with_context(|| format!("remove remote temporary file {}", temp.display()))
            .err()
            .into_iter()
            .collect();
        return Err(attach_cleanup_errors(primary, cleanup));
    }
    if let Err(primary) = client.rename(&temp, &destination).await.with_context(|| {
        format!(
            "publish remote file {} as {}",
            temp.display(),
            destination.display()
        )
    }) {
        remote.close().await;
        let cleanup = client
            .remove_file(&temp)
            .await
            .with_context(|| format!("remove remote temporary file {}", temp.display()))
            .err()
            .into_iter()
            .collect();
        return Err(attach_cleanup_errors(primary, cleanup));
    }
    let sync = remote
        .sync_all()
        .await
        .with_context(|| format!("sync remote file {}", destination.display()));
    remote.close().await;
    sync?;
    progress.finish_file(&planned.relative);
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
        let wanted = usize::try_from((planned.size - offset).min(chunk_size as u64)).unwrap();
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

fn temporary_sibling(destination: &Path) -> Result<PathBuf> {
    let parent = destination
        .parent()
        .context("remote destination has no parent directory")?;
    Ok(parent.join(format!(".zerofs-{}.tmp", Uuid::new_v4())))
}
