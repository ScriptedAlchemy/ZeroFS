//! Prepared write batch types shared by every protocol adapter.
//!
//! [`prepare_write`](crate::fs::ops::write::prepare_write) produces a
//! [`PreparedWriteBatch`] under the existing inode mutation locks.
//! Application consumes those exact attributes and payloads; it does not
//! re-decide timestamps, set-id bits, or post-write size.

use crate::dedup::OpId;
use crate::fs::inode::{Inode, InodeId};
use crate::fs::lock_manager::MultiLockGuard;
use crate::fs::types::{AuthContext, FileAttributes};
use bytes::Bytes;
use std::time::Instant;

/// One member of a logical write request, before validation.
#[derive(Debug, Clone)]
pub(crate) struct PrepareWriteMember {
    pub(crate) id: InodeId,
    pub(crate) offset: u64,
    pub(crate) data: Bytes,
}

/// One logical write, with one member for a normal file write and one
/// member per backing inode for a striped NBD request.
#[derive(Debug, Clone)]
pub(crate) struct PrepareWriteRequest {
    pub(crate) members: Vec<PrepareWriteMember>,
    pub(crate) auth: AuthContext,
    pub(crate) op_id: OpId,
    pub(crate) check_permissions: bool,
}

/// Validation and attribute decisions for one member. No canonical
/// mutation has been published yet.
#[derive(Debug, Clone)]
pub(crate) struct PreparedWriteMember {
    pub(crate) id: InodeId,
    pub(crate) offset: u64,
    pub(crate) data: Bytes,
    pub(crate) old_size: u64,
    pub(crate) new_size: u64,
    pub(crate) inode: Inode,
    pub(crate) post_attrs: FileAttributes,
    pub(crate) republish_metadata: bool,
    pub(crate) parent_name_for_update: Option<(InodeId, Vec<u8>)>,
    pub(crate) span: Option<(u64, u64)>,
}

/// Outcome of [`prepare_write`](crate::fs::ops::write::prepare_write).
///
/// A live batch holds the mutation locks until apply submits, matching the
/// existing materialized write path. A replayed batch carries the original
/// typed result and does not hold locks.
pub(crate) struct PreparedWriteBatch {
    pub(crate) op_id: OpId,
    pub(crate) start_time: Instant,
    pub(crate) members: Vec<PreparedWriteMember>,
    pub(crate) replayed: Option<PreparedBatchResult>,
    pub(crate) guards: Option<MultiLockGuard<InodeId>>,
}

/// One result boundary for the whole batch.
#[derive(Debug, Clone)]
pub(crate) struct PreparedBatchResult {
    pub(crate) members: Vec<(InodeId, FileAttributes)>,
}

impl PreparedWriteBatch {
    pub(crate) fn replayed(op_id: OpId, result: PreparedBatchResult) -> Self {
        Self {
            op_id,
            start_time: Instant::now(),
            members: Vec::new(),
            replayed: Some(result),
            guards: None,
        }
    }

    pub(crate) fn is_replayed(&self) -> bool {
        self.replayed.is_some()
    }
}

impl PreparedBatchResult {
    pub(crate) fn primary_attrs(&self) -> FileAttributes {
        self.members
            .first()
            .map(|(_, attrs)| attrs.clone())
            .expect("prepared batch result has at least one member")
    }
}
