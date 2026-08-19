//! Shared volatile mutation layer used by every write protocol (NBD, NFS,
//! 9P, WebUI).
//!
//! For now this module carries the normalized write-acknowledgement
//! configuration contract ([`config::FilesystemWriteAckSettings`]) and the
//! shared bounded RAM overlay runtime ([`volatile_overlay`]).

pub(crate) mod ack;
pub(crate) mod config;
pub(crate) mod volatile_overlay;

pub(crate) mod request_cache;
pub(crate) mod types;
