use super::admission::P9AdmissionPermit;
use crate::task::spawn_named;
use std::sync::Arc;
use tokio::io::{AsyncWrite, AsyncWriteExt};
use tokio::sync::OwnedSemaphorePermit;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tokio_util::task::AbortOnDropHandle;
use tracing::{error, warn};

const RLERROR_TYPE: u8 = 7;
const RVERSION_TYPE: u8 = 101;
const RFLUSH_TYPE: u8 = 109;
pub(super) const RESPONSE_BUFFER_CAPACITY: usize = 64 * 1024;

/// What keeps a queued reply's memory accounted until the writer sends it.
pub(crate) enum ResponseCredit {
    /// The request's admission covers its reply.
    Admitted(#[allow(dead_code, reason = "held for its Drop")] P9AdmissionPermit),
    /// A Tflush needs no request admission: its reply is one fixed 7-byte
    /// frame, and the connection bounds how many flushes are outstanding.
    Flush(#[allow(dead_code, reason = "held for its Drop")] OwnedSemaphorePermit),
}

impl From<P9AdmissionPermit> for ResponseCredit {
    fn from(admission: P9AdmissionPermit) -> Self {
        Self::Admitted(admission)
    }
}

pub(crate) struct P9Response {
    tag: u16,
    bytes: Vec<u8>,
    _credit: ResponseCredit,
}

impl P9Response {
    pub(super) fn new(tag: u16, bytes: Vec<u8>, credit: impl Into<ResponseCredit>) -> Self {
        Self {
            tag,
            bytes,
            _credit: credit.into(),
        }
    }

    pub(crate) fn into_guarded_parts(self) -> (u16, Vec<u8>, ResponseCredit) {
        (self.tag, self.bytes, self._credit)
    }

    #[cfg(test)]
    pub(super) fn into_parts(self) -> (u16, Vec<u8>) {
        let (tag, bytes, _admission) = self.into_guarded_parts();
        (tag, bytes)
    }
}

/// Errors, version negotiation and Rflush report no filesystem state. Rflush
/// either follows the flushed request's own (gated) reply or means the request
/// never took effect, which is true on a deposed server too.
fn response_requires_serving_authority(response_bytes: &[u8]) -> bool {
    !matches!(
        response_bytes.get(4),
        Some(&RLERROR_TYPE) | Some(&RVERSION_TYPE) | Some(&RFLUSH_TYPE)
    )
}

/// Check serving authority at the final response boundary.
pub(crate) fn response_may_be_emitted(db: &crate::db::Db, response_bytes: &[u8]) -> bool {
    !response_requires_serving_authority(response_bytes) || db.permits_successful_response()
}

/// Poll response I/O only while it is authorized. `None` requires closing the
/// transport: some bytes may already be buffered or a frame partially written.
pub(crate) async fn with_response_authority<F: std::future::Future>(
    io: F,
    may_emit: impl Fn() -> bool,
) -> Option<F::Output> {
    let mut io = std::pin::pin!(io);
    std::future::poll_fn(|cx| {
        if may_emit() {
            io.as_mut().poll(cx).map(Some)
        } else {
            std::task::Poll::Ready(None)
        }
    })
    .await
}

#[derive(Clone)]
pub(super) enum ResponseAuthority {
    Database(Arc<crate::db::Db>),
    #[cfg(test)]
    Always,
}

impl ResponseAuthority {
    pub(super) fn from_database(db: Arc<crate::db::Db>) -> Self {
        Self::Database(db)
    }

    #[cfg(test)]
    pub(super) fn always() -> Self {
        Self::Always
    }

    fn permits_successful_response(&self) -> bool {
        match self {
            Self::Database(db) => db.permits_successful_response(),
            #[cfg(test)]
            Self::Always => true,
        }
    }

    fn may_emit(&self, response_bytes: &[u8]) -> bool {
        match self {
            Self::Database(db) => response_may_be_emitted(db, response_bytes),
            #[cfg(test)]
            Self::Always => true,
        }
    }
}

pub(super) fn spawn_response_writer<W>(
    write_stream: W,
    mut rx: mpsc::Receiver<P9Response>,
    authority: ResponseAuthority,
    connection_shutdown: CancellationToken,
) -> AbortOnDropHandle<()>
where
    W: AsyncWrite + Unpin + Send + 'static,
{
    AbortOnDropHandle::new(spawn_named("9p-writer", async move {
        // Stop request dispatch when the response writer exits.
        let _cancel_connection_on_exit = connection_shutdown.drop_guard();
        let mut writer =
            tokio::io::BufWriter::with_capacity(RESPONSE_BUFFER_CAPACITY, write_stream);
        loop {
            let first = match rx.recv().await {
                Some(msg) => msg,
                None => break,
            };

            // Avoid allocating a batch for the uncontended case. Adaptive
            // kernel lanes normally carry one direct request; a Vec is useful
            // only after a second response is already waiting.
            let second = match rx.try_recv() {
                Ok(more) => more,
                Err(_) => {
                    let (tag, response_bytes, admission) = first.into_guarded_parts();
                    // The outer BufWriter is empty after every batch. Bypass
                    // its copy for this latency-critical case, but still flush
                    // the underlying AsyncWrite: write_all only guarantees
                    // acceptance, and the transport itself may be buffered.
                    match with_response_authority(
                        writer.get_mut().write_all(&response_bytes),
                        || authority.may_emit(&response_bytes),
                    )
                    .await
                    {
                        None => {
                            warn!(
                                "Dropping successful 9P response for tag {tag} after serving \
                                 authority was lost; closing the connection"
                            );
                            return;
                        }
                        Some(Err(e)) => {
                            error!("Failed to write response for tag {}: {}", tag, e);
                            return;
                        }
                        Some(Ok(())) => {}
                    }
                    match with_response_authority(writer.get_mut().flush(), || {
                        authority.may_emit(&response_bytes)
                    })
                    .await
                    {
                        None => {
                            warn!(
                                "Dropping buffered successful 9P response for tag {tag} after \
                                 serving authority was lost; closing the connection"
                            );
                            return;
                        }
                        Some(Err(e)) => {
                            error!("Failed to flush response for tag {}: {}", tag, e);
                            return;
                        }
                        Some(Ok(())) => {}
                    }
                    drop(admission);
                    continue;
                }
            };

            // Form one bounded batch before any transport write. Continuing
            // to drain across writes can indefinitely delay the final flush.
            let mut batch = vec![first, second];
            // Consolidate responses that are already queued without delaying
            // an uncontended RPC by a scheduler turn.
            while let Ok(more) = rx.try_recv() {
                batch.push(more);
            }

            let mut buffered_authority_gated_success = false;
            let mut admissions = Vec::with_capacity(batch.len());
            for response in batch {
                let (tag, response_bytes, admission) = response.into_guarded_parts();
                admissions.push(admission);
                let requires_authority = response_requires_serving_authority(&response_bytes);
                buffered_authority_gated_success |= requires_authority;
                // `write_all` can flush earlier frames and yield under
                // backpressure. Check before every resumed transport poll.
                match with_response_authority(writer.write_all(&response_bytes), || {
                    !buffered_authority_gated_success || authority.permits_successful_response()
                })
                .await
                {
                    None => {
                        warn!(
                            "Dropping successful 9P responses before writing response for tag \
                             {tag} after serving authority was lost; closing the connection"
                        );
                        // A later Rflush may depend on this committed reply.
                        // Closing must discard it together with the success.
                        return;
                    }
                    Some(Err(e)) => {
                        error!("Failed to write response for tag {}: {}", tag, e);
                        return;
                    }
                    Some(Ok(())) => {}
                }
            }

            match with_response_authority(writer.flush(), || {
                !buffered_authority_gated_success || authority.permits_successful_response()
            })
            .await
            {
                None => {
                    warn!(
                        "Dropping buffered successful 9P responses after serving authority was \
                         lost; closing the connection"
                    );
                    return;
                }
                Some(Err(e)) => {
                    error!("Failed to flush writer: {}", e);
                    return;
                }
                Some(Ok(())) => {}
            }
            drop(admissions);
        }
    }))
}

#[cfg(test)]
mod authority_tests {
    use super::with_response_authority;
    use futures::{Sink, SinkExt};
    use std::cell::Cell;
    use std::pin::Pin;
    use std::task::{Context, Poll};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    struct StalledSink<'a> {
        ready: &'a Cell<bool>,
        pause_flush: bool,
        buffered: Option<Vec<u8>>,
        sent: Vec<Vec<u8>>,
    }

    impl Sink<Vec<u8>> for StalledSink<'_> {
        type Error = std::io::Error;

        fn poll_ready(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            if self.pause_flush || self.ready.get() {
                Poll::Ready(Ok(()))
            } else {
                Poll::Pending
            }
        }

        fn start_send(mut self: Pin<&mut Self>, item: Vec<u8>) -> Result<(), Self::Error> {
            self.buffered = Some(item);
            Ok(())
        }

        fn poll_flush(
            mut self: Pin<&mut Self>,
            _: &mut Context<'_>,
        ) -> Poll<Result<(), Self::Error>> {
            if self.pause_flush && !self.ready.get() {
                return Poll::Pending;
            }
            if let Some(frame) = self.buffered.take() {
                self.sent.push(frame);
            }
            Poll::Ready(Ok(()))
        }

        fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            self.poll_flush(cx)
        }
    }

    async fn revoke_during_sink_backpressure(pause_flush: bool) {
        let ready = Cell::new(false);
        let authorized = Cell::new(true);
        let mut sink = StalledSink {
            ready: &ready,
            pause_flush,
            buffered: None,
            sent: Vec::new(),
        };
        {
            let send = with_response_authority(sink.send(b"success".to_vec()), || authorized.get());
            tokio::pin!(send);
            assert!(futures::poll!(&mut send).is_pending());
            authorized.set(false);
            ready.set(true);
            assert!(
                send.await.is_none(),
                "a resumed send must recheck serving authority"
            );
        }
        assert!(
            sink.sent.is_empty(),
            "the buffered success must not reach the peer"
        );
    }

    #[tokio::test]
    async fn authority_is_rechecked_after_sink_readiness_waits() {
        revoke_during_sink_backpressure(false).await;
    }

    #[tokio::test]
    async fn authority_is_rechecked_after_sink_flush_waits() {
        revoke_during_sink_backpressure(true).await;
    }

    #[tokio::test]
    async fn authority_is_rechecked_after_stream_write_waits() {
        let (mut peer, mut transport) = tokio::io::duplex(1);
        transport.write_all(b"x").await.unwrap();
        let authorized = Cell::new(true);
        {
            let write =
                with_response_authority(transport.write_all(b"success"), || authorized.get());
            tokio::pin!(write);
            assert!(futures::poll!(&mut write).is_pending());
            authorized.set(false);
            peer.read_exact(&mut [0]).await.unwrap();
            assert!(matches!(futures::poll!(&mut write), Poll::Ready(None)));
        }
        drop(transport);
        let mut received = Vec::new();
        peer.read_to_end(&mut received).await.unwrap();
        assert!(received.is_empty());
    }

    #[tokio::test]
    async fn authority_is_rechecked_after_buffered_stream_flush_waits() {
        let (mut peer, mut transport) = tokio::io::duplex(1);
        transport.write_all(b"x").await.unwrap();
        let mut transport = tokio::io::BufWriter::with_capacity(64, transport);
        transport.write_all(b"success").await.unwrap();
        let authorized = Cell::new(true);
        {
            let flush = with_response_authority(transport.flush(), || authorized.get());
            tokio::pin!(flush);
            assert!(futures::poll!(&mut flush).is_pending());
            authorized.set(false);
            peer.read_exact(&mut [0]).await.unwrap();
            assert!(matches!(futures::poll!(&mut flush), Poll::Ready(None)));
        }
        drop(transport);
        let mut received = Vec::new();
        peer.read_to_end(&mut received).await.unwrap();
        assert!(received.is_empty());
    }
}
