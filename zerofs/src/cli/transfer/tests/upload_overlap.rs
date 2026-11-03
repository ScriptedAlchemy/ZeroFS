use super::super::copy::stream_upload_from;
use super::*;
use bytes::Bytes;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, ReadBuf};
use tokio::sync::oneshot;

/// Stop the local source partway through its second chunk. This makes the network
/// overlap observable without relying on filesystem speed or wall-time ratios.
struct GatedSource {
    data: Bytes,
    offset: usize,
    gate_after: usize,
    reached: Option<oneshot::Sender<()>>,
    release: Option<oneshot::Receiver<()>>,
}

impl GatedSource {
    fn new(data: Bytes, gate_after: usize) -> (Self, oneshot::Receiver<()>, oneshot::Sender<()>) {
        let (reached, wait_for_source) = oneshot::channel();
        let (release, wait_for_release) = oneshot::channel();
        (
            Self {
                data,
                offset: 0,
                gate_after,
                reached: Some(reached),
                release: Some(wait_for_release),
            },
            wait_for_source,
            release,
        )
    }
}

impl AsyncRead for GatedSource {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        if buf.remaining() == 0 {
            return Poll::Ready(Ok(()));
        }
        if self.offset >= self.gate_after && self.release.is_some() {
            if let Some(reached) = self.reached.take() {
                let _ = reached.send(());
            }
            match Pin::new(self.release.as_mut().unwrap()).poll(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(result) => {
                    self.release = None;
                    if result.is_err() {
                        return Poll::Ready(Err(std::io::Error::new(
                            std::io::ErrorKind::BrokenPipe,
                            "gated local source failed",
                        )));
                    }
                }
            }
        }
        let available = if self.offset < self.gate_after {
            self.gate_after.min(self.data.len()) - self.offset
        } else {
            self.data.len() - self.offset
        };
        let count = buf.remaining().min(available);
        buf.put_slice(&self.data[self.offset..self.offset + count]);
        self.offset += count;
        Poll::Ready(Ok(()))
    }
}

#[tokio::test]
async fn upload_dispatches_ready_chunk_while_the_next_source_read_is_pending() {
    let (client, shutdown, _local, _filesystem) = remote_client_with_filesystem().await;
    let remote = client
        .open(
            "/source-overlap.tmp",
            zerofs_client::OpenOptions::write_only().create_new(true),
        )
        .await
        .unwrap();
    let chunk = 64 * 1024;
    let payload = Bytes::from(
        (0..chunk * 3 + 17)
            .map(|position| ((position * 31 + position / 257) % 251) as u8)
            .collect::<Vec<_>>(),
    );
    let (mut source, source_blocked, release_source) =
        GatedSource::new(payload.clone(), chunk + 17);
    let planned = PlannedFile {
        source: "gated-source.bin".into(),
        relative: "gated-source.bin".into(),
        size: payload.len() as u64,
    };
    let progress = Progress::new("upload", planned.size, 1);
    let file_progress = Arc::new(progress.start_file(&planned.source, planned.size));
    let observed_progress = file_progress.clone();
    let upload = tokio::spawn(async move {
        stream_upload_from(
            &[remote],
            &mut source,
            &planned,
            chunk,
            &file_progress,
            &CancellationToken::new(),
        )
        .await
    });
    tokio::time::timeout(Duration::from_secs(2), source_blocked)
        .await
        .expect("second local read did not reach the gate")
        .unwrap();
    let overlapped = tokio::time::timeout(Duration::from_secs(2), async {
        while observed_progress.transferred_bytes() < chunk as u64 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .is_ok();
    release_source.send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(10), upload)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(
        client.read("/source-overlap.tmp").await.unwrap().as_ref(),
        payload.as_ref()
    );
    client.close().await;
    shutdown.cancel();
    assert!(
        overlapped,
        "a ready p9 write waited for the next local source read"
    );
}

enum StopDuringRead {
    Cancel,
    SourceError,
}

async fn stop_during_partial_read_settles_the_dispatched_write(stop: StopDuringRead) {
    let (client, shutdown, _local, filesystem) = remote_client_with_filesystem().await;
    let remote = client
        .open(
            "/stopped-overlap.tmp",
            zerofs_client::OpenOptions::write_only().create_new(true),
        )
        .await
        .unwrap();
    let chunk = 64 * 1024;
    let payload = Bytes::from(
        (0..chunk * 3)
            .map(|position| ((position * 13 + position / 257) % 251) as u8)
            .collect::<Vec<_>>(),
    );
    let (mut source, source_blocked, release_source) =
        GatedSource::new(payload.clone(), chunk + 17);
    let mut release_source = Some(release_source);
    let planned = PlannedFile {
        source: "gated-source.bin".into(),
        relative: "gated-source.bin".into(),
        size: payload.len() as u64,
    };
    let progress = Progress::new("upload", planned.size, 1);
    let file_progress = progress.start_file(&planned.source, planned.size);
    let cancellation = CancellationToken::new();
    let transfer_cancellation = cancellation.clone();
    let stalled_apply = filesystem.db.flush_barrier().write_owned().await;
    let first_write_reached = filesystem.write_coordinator.probe_next_apply();
    let mut upload = tokio::spawn(async move {
        stream_upload_from(
            &[remote],
            &mut source,
            &planned,
            chunk,
            &file_progress,
            &transfer_cancellation,
        )
        .await
    });
    tokio::time::timeout(Duration::from_secs(2), source_blocked)
        .await
        .unwrap()
        .unwrap();
    tokio::time::timeout(Duration::from_secs(2), first_write_reached)
        .await
        .expect("ready write did not reach the server while the source was pending")
        .unwrap();
    let expected_error = match stop {
        StopDuringRead::Cancel => {
            cancellation.cancel();
            "upload cancelled"
        }
        StopDuringRead::SourceError => {
            drop(release_source.take());
            "gated local source failed"
        }
    };
    let early = tokio::time::timeout(Duration::from_millis(50), &mut upload).await;
    let returned_before_write_settled = early.is_ok();
    drop(stalled_apply);
    let result = match early {
        Ok(result) => result,
        Err(_) => tokio::time::timeout(Duration::from_secs(10), upload)
            .await
            .unwrap(),
    };
    let error = result.unwrap().unwrap_err();
    drop(release_source);
    assert!(format!("{error:#}").contains(expected_error), "{error:#}");
    assert_eq!(
        client.read("/stopped-overlap.tmp").await.unwrap().as_ref(),
        &payload[..chunk],
        "a partially read source chunk must not be submitted after cancellation or failure"
    );
    client.close().await;
    shutdown.cancel();
    assert!(
        !returned_before_write_settled,
        "the uploader abandoned a dispatched write while stopping its local read"
    );
}

#[tokio::test]
async fn upload_cancellation_settles_writes_during_a_partial_source_read() {
    stop_during_partial_read_settles_the_dispatched_write(StopDuringRead::Cancel).await;
}

#[tokio::test]
async fn upload_source_error_settles_writes_during_a_partial_source_read() {
    stop_during_partial_read_settles_the_dispatched_write(StopDuringRead::SourceError).await;
}
