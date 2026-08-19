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
    pub(crate) quota: Option<crate::fs::quota::ProvisionalQuotaReservation>,
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
    pub(crate) cutoff: Option<MutationCutoff>,
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

    pub(crate) fn with_cutoff(mut self, cutoff: MutationCutoff) -> Self {
        self.cutoff = Some(cutoff);
        self
    }
}

/// Protocol-scoped identity for request replay and collision detection.
///
/// NFS always includes a server-minted transport `connection_incarnation`.
/// Client address is a fingerprint input, never a substitute for that
/// incarnation, so reconnect address reuse cannot join old work.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) enum RequestIdentity {
    NineP {
        session_incarnation: u64,
        operation_id: crate::dedup::OpId,
    },
    Nfs {
        server_incarnation: uuid::Uuid,
        connection_incarnation: u64,
        xid: u32,
    },
    Nbd {
        connection_incarnation: u64,
        handle: u64,
    },
    DirectTagged {
        caller_incarnation: uuid::Uuid,
        operation_id: u128,
    },
    DirectOneShot(uuid::Uuid),
}

/// Hash of payload, auth, requested stability, durability, and (for NFS)
/// client address.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct RequestFingerprint([u8; 32]);

impl RequestFingerprint {
    pub(crate) fn from_parts(parts: &[&[u8]]) -> Self {
        use sha2::{Digest, Sha256};
        let mut hasher = Sha256::new();
        for part in parts {
            hasher.update((part.len() as u64).to_le_bytes());
            hasher.update(part);
        }
        Self(hasher.finalize().into())
    }

    pub(crate) fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }
}

#[cfg(test)]
mod request_fingerprint_tests {
    use super::RequestFingerprint;

    #[test]
    fn fingerprint_parts_are_structurally_unambiguous() {
        assert_ne!(
            RequestFingerprint::from_parts(&[b"ab", b"c"]),
            RequestFingerprint::from_parts(&[b"a", b"bc"]),
            "field boundaries must affect the fingerprint",
        );
    }
}

/// How long a completed request may occupy the replay cache.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RequestLifetime {
    CanonicalDedup,
    ReplayWindow(std::time::Duration),
    InFlightOnly,
    OneShot,
}

/// Terminal vocabulary shared by raw admission, preparation quiescence, and
/// materialization progress.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub(crate) enum MutationError {
    #[error("mutation coordinator is closed")]
    Closed,
    #[error("mutation coordinator is poisoned: {0}")]
    Poisoned(String),
    #[error("mutation requires {requested} bytes but the volatile budget is {capacity} bytes")]
    TooLarge { requested: u64, capacity: u64 },
    #[error("mutation cutoff belongs to a stale mutation incarnation")]
    StaleIncarnation,
}

/// Canonical conflict unit counted by the preparation gate. Directory
/// membership conflicts are distinct from the directory inode's own
/// attribute conflicts.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) enum ConflictKey {
    Inode(InodeId),
    Directory(InodeId),
}

/// The set of conflict keys one preparation touches.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct ConflictScope(std::collections::BTreeSet<ConflictKey>);

impl ConflictScope {
    pub(crate) fn new(keys: impl IntoIterator<Item = ConflictKey>) -> Self {
        Self(keys.into_iter().collect())
    }

    pub(crate) fn single(key: ConflictKey) -> Self {
        Self::new([key])
    }

    pub(crate) fn keys(&self) -> impl Iterator<Item = ConflictKey> + '_ {
        self.0.iter().copied()
    }
}

/// One boot of the mutation coordinator. Sequences from different
/// incarnations are never comparable.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct MutationIncarnation(uuid::Uuid);

impl MutationIncarnation {
    pub(crate) fn new() -> Self {
        Self(uuid::Uuid::new_v4())
    }

    pub(crate) fn as_uuid(self) -> uuid::Uuid {
        self.0
    }
}

/// A published mutation position: incarnation plus assigned sequence.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct MutationCutoff {
    pub(crate) mutation_incarnation: MutationIncarnation,
    pub(crate) sequence: u64,
}

/// Cutoffs are ordered only within one incarnation; comparing across
/// incarnations yields no ordering rather than a fabricated one.
impl PartialOrd for MutationCutoff {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        (self.mutation_incarnation == other.mutation_incarnation)
            .then(|| self.sequence.cmp(&other.sequence))
    }
}
