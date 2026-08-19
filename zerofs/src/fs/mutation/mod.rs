//! Shared volatile mutation layer used by every write protocol (NBD, NFS,
//! 9P, WebUI).
//!
//! For now this module carries only the normalized write-acknowledgement
//! configuration contract ([`config::FilesystemWriteAckSettings`]); the
//! admission and overlay runtime lands in follow-up work.

pub(crate) mod config;

pub(crate) mod request_cache;
pub(crate) mod types;
