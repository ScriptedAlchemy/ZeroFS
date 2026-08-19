use super::types::{MutationError, PrepareWriteRequest, RequestFingerprint};
use super::volatile_overlay::OverlayError;
use crate::fs::errors::FsError;
use crate::fs::inode::InodeId;
use crate::fs::types::AuthContext;

pub(super) fn direct_write_fingerprint(
    auth: &AuthContext,
    id: InodeId,
    offset: u64,
    length: usize,
    op_id: crate::dedup::OpId,
    check_permissions: bool,
) -> RequestFingerprint {
    let id = id.to_le_bytes();
    let offset = offset.to_le_bytes();
    let length = (length as u64).to_le_bytes();
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
        b"direct-write",
        &id,
        &offset,
        &length,
        &uid,
        &gid,
        &flags,
        &gids,
        &op_id,
    ])
}

pub(super) fn direct_batch_fingerprint(request: &PrepareWriteRequest) -> RequestFingerprint {
    let mut encoded = Vec::new();
    encoded.extend_from_slice(&request.auth.uid.to_le_bytes());
    encoded.extend_from_slice(&request.auth.gid.to_le_bytes());
    encoded.push(u8::from(request.auth.gid_known));
    encoded.push(u8::from(request.auth.groups_complete));
    encoded.push(u8::from(request.check_permissions));
    encoded.extend_from_slice(&request.op_id);
    for supplementary_gid in &request.auth.gids {
        encoded.extend_from_slice(&supplementary_gid.to_le_bytes());
    }
    for member in &request.members {
        encoded.extend_from_slice(&member.id.to_le_bytes());
        encoded.extend_from_slice(&member.offset.to_le_bytes());
        encoded.extend_from_slice(&(member.data.len() as u64).to_le_bytes());
    }
    RequestFingerprint::from_parts(&[b"direct-batch", &encoded])
}

pub(super) fn mutation_fs_error(error: MutationError) -> FsError {
    match error {
        MutationError::TooLarge { .. } => FsError::NoSpace,
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
