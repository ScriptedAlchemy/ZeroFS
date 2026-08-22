use crate::SEND_STALL_TIMEOUT;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::sync::{Notify, watch};

#[derive(Clone)]
pub(crate) struct IoProgress {
    reads: watch::Sender<u64>,
    writes: watch::Sender<u64>,
}

impl IoProgress {
    fn new() -> Self {
        let (reads, _) = watch::channel(0);
        let (writes, _) = watch::channel(0);
        Self { reads, writes }
    }

    fn read_advanced(&self) {
        self.reads
            .send_modify(|generation| *generation = generation.wrapping_add(1));
    }

    fn write_advanced(&self) {
        self.writes
            .send_modify(|generation| *generation = generation.wrapping_add(1));
    }
}

pub(crate) struct TrackedIo<T> {
    inner: T,
    progress: IoProgress,
}

impl<T> TrackedIo<T> {
    pub(crate) fn new(inner: T) -> (Self, IoProgress) {
        let progress = IoProgress::new();
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
        let filled_before = buffer.filled().len();
        let result = Pin::new(&mut self.inner).poll_read(cx, buffer);
        if buffer.filled().len() > filled_before {
            self.progress.read_advanced();
        }
        result
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
            self.progress.write_advanced();
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

pub(crate) async fn wait_for_write<F, P>(
    future: F,
    progress: &IoProgress,
    shutdown: &Notify,
    mut on_progress: P,
) -> WriteOutcome<F::Output>
where
    F: Future,
    P: FnMut(),
{
    enum Event<T> {
        Completed(T),
        Shutdown,
        Advanced,
    }

    let mut changes = progress.writes.subscribe();
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
            Ok(Event::Advanced) => {
                on_progress();
                continue;
            }
            Err(_) => return WriteOutcome::Stalled,
        }
    }
}

pub(crate) enum ReadOutcome<T> {
    Completed(T),
    Shutdown,
}

pub(crate) async fn wait_for_read<F, P>(
    future: F,
    progress: &IoProgress,
    shutdown: &Notify,
    mut on_progress: P,
) -> ReadOutcome<F::Output>
where
    F: Future,
    P: FnMut(),
{
    let mut changes = progress.reads.subscribe();
    tokio::pin!(future);
    loop {
        tokio::select! {
            biased;
            _ = shutdown.notified() => return ReadOutcome::Shutdown,
            result = &mut future => return ReadOutcome::Completed(result),
            changed = changes.changed() => {
                debug_assert!(changed.is_ok(), "the progress sender outlives this wait");
                if changed.is_err() {
                    return ReadOutcome::Shutdown;
                }
                on_progress();
            }
        }
    }
}
