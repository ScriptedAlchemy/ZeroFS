//! Shared volatile mutation layer used by every write protocol (NBD, NFS,
//! 9P, WebUI).
//!
//! [`config::FilesystemWriteAckSettings`] is the normalized
//! write-acknowledgement contract, [`request_cache`] the protocol replay
//! cache, [`admission`] the raw byte/operation budget and preparation
//! quiescence gate, [`progress`] the gap-free materialization barrier,
//! [`durability`] typed local/remote receipts, [`volatile_overlay`] the
//! bounded RAM overlay runtime, [`overlay`] the filesystem-facing overlay
//! manager, [`materializer`] the ordered canonical apply workers, and
//! [`fence`] deadlock-safe conflict fences.

pub(crate) mod ack;
pub(crate) mod admission;
pub(crate) mod config;
pub(crate) mod durability;
pub(crate) mod fence;
pub(crate) mod materialized_replay;
pub(crate) mod materializer;
pub(crate) mod overlay;
mod overlay_dispatch;
pub(crate) mod overlay_helpers;
pub(crate) mod progress;
pub(crate) mod volatile_overlay;

pub(crate) mod request_cache;
pub(crate) mod types;

use crate::fs::ZeroFS;
use crate::fs::mutation::types::MutationCutoff;

/// Capture the final published mutation cutoff after admission has stopped.
// WIP on develop: landed but not wired into shutdown yet.
#[allow(dead_code)]
pub(crate) fn closed_admission_cutoff(fs: &ZeroFS) -> MutationCutoff {
    fs.capture_mutation_cutoff()
}
