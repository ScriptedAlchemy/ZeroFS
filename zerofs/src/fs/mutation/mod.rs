//! Shared volatile mutation layer used by every write protocol (NBD, NFS,
//! 9P, WebUI).
//!
//! [`config::FilesystemWriteAckSettings`] is the normalized
//! write-acknowledgement contract, [`request_cache`] the protocol replay
//! cache, [`admission`] the raw byte/operation budget and preparation
//! quiescence gate, [`progress`] the gap-free materialization barrier, and
//! [`volatile_overlay`] the shared bounded RAM overlay runtime.

pub(crate) mod ack;
pub(crate) mod admission;
pub(crate) mod config;
pub(crate) mod progress;
pub(crate) mod volatile_overlay;

pub(crate) mod request_cache;
pub(crate) mod types;
