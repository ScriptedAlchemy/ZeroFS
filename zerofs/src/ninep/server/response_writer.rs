use super::admission::P9AdmissionPermit;
use crate::task::spawn_named;
use std::sync::Arc;
use tokio::io::{AsyncWrite, AsyncWriteExt};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tokio_util::task::AbortOnDropHandle;
use tracing::{error, warn};

const RLERROR_TYPE: u8 = 7;
const RVERSION_TYPE: u8 = 101;
pub(super) const RESPONSE_BUFFER_CAPACITY: usize = 64 * 1024;

pub(crate) struct P9Response {
    tag: u16,
    bytes: Vec<u8>,
    _admission: P9AdmissionPermit,
}

impl P9Response {
    pub(super) fn new(tag: u16, bytes: Vec<u8>, admission: P9AdmissionPermit) -> Self {
        Self {
            tag,
            bytes,
            _admission: admission,
        }
    }

    pub(crate) fn into_guarded_parts(self) -> (u16, Vec<u8>, P9AdmissionPermit) {
        (self.tag, self.bytes, self._admission)
    }

    #[cfg(test)]
    pub(super) fn into_parts(self) -> (u16, Vec<u8>) {
        let (tag, bytes, _admission) = self.into_guarded_parts();
        (tag, bytes)
    }
}

pub(crate) fn response_requires_serving_authority(response_bytes: &[u8]) -> bool {
    !matches!(
        response_bytes.get(4),
        Some(&RLERROR_TYPE) | Some(&RVERSION_TYPE)
    )
}

/// Check serving authority at the final response boundary.
pub(crate) fn response_may_be_emitted(db: &crate::db::Db, response_bytes: &[u8]) -> bool {
    !response_requires_serving_authority(response_bytes) || db.permits_successful_response()
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
                    let requires_authority = response_requires_serving_authority(&response_bytes);
                    if requires_authority && !authority.may_emit(&response_bytes) {
                        warn!(
                            "Dropping successful 9P response for tag {tag} after serving authority \
                             was lost; closing the connection"
                        );
                        return;
                    }
                    // Keep the final authority check adjacent to the actual
                    // transport write, just as on the buffered batch path.
                    if requires_authority && !authority.permits_successful_response() {
                        warn!(
                            "Dropping successful 9P response for tag {tag} immediately before \
                             write after serving authority was lost; closing the connection"
                        );
                        return;
                    }
                    // The outer BufWriter is empty after every batch. Bypass
                    // its copy for this latency-critical case, but still flush
                    // the underlying AsyncWrite: write_all only guarantees
                    // acceptance, and the transport itself may be buffered.
                    if let Err(e) = writer.get_mut().write_all(&response_bytes).await {
                        error!("Failed to write response for tag {}: {}", tag, e);
                        return;
                    }
                    // Mirror the buffered batch path's final authority check.
                    // If the underlying writer retained the bytes, authority
                    // loss must prevent its flush.
                    if requires_authority && !authority.permits_successful_response() {
                        warn!(
                            "Dropping buffered successful 9P response for tag {tag} before flush \
                             after serving authority was lost; closing the connection"
                        );
                        return;
                    }
                    if let Err(e) = writer.get_mut().flush().await {
                        error!("Failed to flush response for tag {}: {}", tag, e);
                        return;
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
            let mut dropped_authority_gated_success = false;
            let mut admissions = Vec::with_capacity(batch.len());
            for response in batch {
                let (tag, response_bytes, admission) = response.into_guarded_parts();
                admissions.push(admission);
                let requires_authority = response_requires_serving_authority(&response_bytes);
                if requires_authority && !authority.may_emit(&response_bytes) {
                    warn!(
                        "Dropping successful 9P response for tag {tag} after serving authority \
                         was lost; closing the connection"
                    );
                    dropped_authority_gated_success = true;
                    continue;
                }
                // `write_all` may flush buffered frames; recheck authority first.
                if buffered_authority_gated_success && !authority.permits_successful_response() {
                    warn!(
                        "Dropping buffered successful 9P responses before writing response for \
                         tag {tag} after serving authority was lost; closing the connection"
                    );
                    return;
                }
                buffered_authority_gated_success |= requires_authority;
                if let Err(e) = writer.write_all(&response_bytes).await {
                    error!("Failed to write response for tag {}: {}", tag, e);
                    return;
                }
            }

            // Recheck authority before the final buffer flush.
            if buffered_authority_gated_success && !authority.permits_successful_response() {
                warn!(
                    "Dropping buffered successful 9P responses after serving authority was \
                     lost; closing the connection"
                );
                return;
            }
            if let Err(e) = writer.flush().await {
                error!("Failed to flush writer: {}", e);
                return;
            }
            drop(admissions);
            if dropped_authority_gated_success {
                return;
            }
        }
    }))
}
