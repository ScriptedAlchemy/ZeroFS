use crate::SEND_STALL_TIMEOUT;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::sync::{Notify, watch};

#[derive(Clone)]
pub(crate) struct WriteProgress {
    generation: watch::Sender<u64>,
}

impl WriteProgress {
    fn new() -> Self {
        let (generation, _) = watch::channel(0);
        Self { generation }
    }

    fn advanced(&self) {
        self.generation
            .send_modify(|generation| *generation = generation.wrapping_add(1));
    }
}

pub(crate) struct TrackedIo<T> {
    inner: T,
    progress: WriteProgress,
}

impl<T> TrackedIo<T> {
    pub(crate) fn new(inner: T) -> (Self, WriteProgress) {
        let progress = WriteProgress::new();
        (
            Self {
                inner,
                progress: progress.clone(),
            },
            progress,
        )
    }
}

impl<T: AsyncRead + Unpin> AsyncRead for TrackedIo<T> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_read(cx, buffer)
    }
}

impl<T: AsyncWrite + Unpin> AsyncWrite for TrackedIo<T> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buffer: &[u8],
    ) -> Poll<Result<usize, std::io::Error>> {
        let result = Pin::new(&mut self.inner).poll_write(cx, buffer);
        if matches!(result, Poll::Ready(Ok(written)) if written > 0) {
            self.progress.advanced();
        }
        result
    }

    fn poll_flush(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), std::io::Error>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }

    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), std::io::Error>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}

pub(crate) enum WriteOutcome<T> {
    Completed(T),
    Shutdown,
    Stalled,
}

pub(crate) async fn wait_for_write<F>(
    future: F,
    progress: &WriteProgress,
    shutdown: &Notify,
) -> WriteOutcome<F::Output>
where
    F: Future,
{
    enum Event<T> {
        Completed(T),
        Shutdown,
        Advanced,
    }

    let mut changes = progress.generation.subscribe();
    tokio::pin!(future);
    loop {
        let event = tokio::time::timeout(SEND_STALL_TIMEOUT, async {
            tokio::select! {
                biased;
                _ = shutdown.notified() => Event::Shutdown,
                result = &mut future => Event::Completed(result),
                changed = changes.changed() => {
                    debug_assert!(changed.is_ok(), "the progress sender outlives this wait");
                    Event::Advanced
                }
            }
        })
        .await;
        match event {
            Ok(Event::Completed(result)) => return WriteOutcome::Completed(result),
            Ok(Event::Shutdown) => return WriteOutcome::Shutdown,
            Ok(Event::Advanced) => continue,
            Err(_) => return WriteOutcome::Stalled,
        }
    }
}
