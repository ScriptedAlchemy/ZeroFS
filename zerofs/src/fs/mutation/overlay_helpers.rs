use super::types::{MutationError, RequestFingerprint};
use super::volatile_overlay::OverlayError;
use crate::fs::errors::FsError;
use crate::fs::inode::InodeId;
use crate::fs::types::AuthContext;

pub(crate) fn direct_write_fingerprint(
    auth: &AuthContext,
    id: InodeId,
    offset: u64,
    data: &[u8],
    op_id: crate::dedup::OpId,
    check_permissions: bool,
    protocol_context: &[u8],
) -> RequestFingerprint {
    let id = id.to_le_bytes();
    let offset = offset.to_le_bytes();
    let length = (data.len() as u64).to_le_bytes();
    let uid = auth.uid.to_le_bytes();
    let gid = auth.gid.to_le_bytes();
    let flags = [
        u8::from(auth.gid_known),
        u8::from(auth.groups_complete),
        u8::from(check_permissions),
    ];
    let mut gids = Vec::with_capacity(auth.gids.len() * std::mem::size_of::<u32>());
    for supplementary_gid in &auth.gids {
        gids.extend_from_slice(&supplementary_gid.to_le_bytes());
    }
    RequestFingerprint::from_parts(&[
        b"filesystem-write",
        &id,
        &offset,
        &length,
        &uid,
        &gid,
        &flags,
        &gids,
        &op_id,
        data,
        protocol_context,
    ])
}

pub(super) fn mutation_fs_error(error: MutationError) -> FsError {
    match error {
        MutationError::TooLarge { .. } => FsError::NoSpace,
        MutationError::Backpressure => FsError::RetryLater,
        MutationError::StaleIncarnation => FsError::StaleHandle,
        MutationError::Closed | MutationError::Poisoned(_) => FsError::IoError,
    }
}

pub(super) fn overlay_fs_error(error: OverlayError) -> FsError {
    match error {
        OverlayError::NoSpace => FsError::NoSpace,
        OverlayError::InvalidArgument => FsError::InvalidArgument,
        OverlayError::IoError => FsError::IoError,
    }
}
