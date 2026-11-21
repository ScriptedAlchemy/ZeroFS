use super::types::{MutationError, RequestFingerprint};
use super::volatile_overlay::OverlayError;
use crate::fs::errors::FsError;
use crate::fs::inode::InodeId;
use crate::fs::types::AuthContext;

/// Bytes of a write payload hashed as one piece of its fingerprint digest.
const FINGERPRINT_PIECE: usize = 128 * 1024;

/// Payloads this large hash their pieces on rayon instead of inline.
const PARALLEL_FINGERPRINT_MIN: usize = 4 * FINGERPRINT_PIECE;

/// The payload's part of a write fingerprint: SHA-256 over the payload's
/// length and the SHA-256 of each fixed 128 KiB piece. Like one SHA-256 of
/// the whole payload, two payloads share it only through a SHA-256 collision.
/// Unlike it, the pieces of one payload hash at once, so a 1 MiB write does
/// not spend a whole serial SHA-256 before it is acknowledged.
///
/// A fingerprint lives only in this process's request cache and replay
/// ledger, so changing how it is computed needs no migration.
fn payload_digest(data: &[u8]) -> [u8; 32] {
    use rayon::prelude::*;
    use sha2::{Digest, Sha256};
    let piece = |piece: &[u8]| -> [u8; 32] { Sha256::digest(piece).into() };
    // Rayon blocks this thread for the parallel hash, which is shorter than
    // the serial hash it replaces would have blocked it.
    let pieces: Vec<[u8; 32]> = if data.len() >= PARALLEL_FINGERPRINT_MIN {
        data.par_chunks(FINGERPRINT_PIECE).map(piece).collect()
    } else {
        data.chunks(FINGERPRINT_PIECE).map(piece).collect()
    };
    let mut digest = Sha256::new();
    digest.update((data.len() as u64).to_le_bytes());
    for piece in &pieces {
        digest.update(piece);
    }
    digest.finalize().into()
}

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
        // Keep existing fingerprints when no local truncation occurred.
        u8::from(auth.groups_complete) | (u8::from(auth.groups_truncated) << 1),
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
        &payload_digest(data),
        protocol_context,
    ])
}

fn mutation_fs_error(error: MutationError) -> FsError {
    match error {
        MutationError::TooLarge { .. } => FsError::NoSpace,
        MutationError::Backpressure => FsError::RetryLater,
        MutationError::StaleIncarnation => FsError::StaleHandle,
        MutationError::Closed | MutationError::Poisoned(_) => FsError::IoError,
    }
}

/// Filesystem vocabulary for the shared write-admission sequence. Protocols
/// that speak a different error language (NBD) map the same typed outcomes
/// themselves rather than reimplementing the sequence.
pub(super) fn write_admission_fs_error(error: super::overlay::WriteAdmissionError) -> FsError {
    use super::overlay::WriteAdmissionError;
    match error {
        WriteAdmissionError::Unavailable => FsError::IoError,
        WriteAdmissionError::Backpressured => FsError::RetryLater,
        WriteAdmissionError::FingerprintMismatch => FsError::InvalidArgument,
        WriteAdmissionError::Mutation(error) => mutation_fs_error(error),
        WriteAdmissionError::Survival(error) => error,
    }
}

pub(super) fn overlay_fs_error(error: OverlayError) -> FsError {
    match error {
        OverlayError::NoSpace => FsError::NoSpace,
        OverlayError::InvalidArgument => FsError::InvalidArgument,
        OverlayError::IoError => FsError::IoError,
    }
}

#[cfg(test)]
mod tests {
    use super::{FINGERPRINT_PIECE, PARALLEL_FINGERPRINT_MIN, payload_digest};

    fn serial_digest(data: &[u8]) -> [u8; 32] {
        use sha2::{Digest, Sha256};
        let mut digest = Sha256::new();
        digest.update((data.len() as u64).to_le_bytes());
        for piece in data.chunks(FINGERPRINT_PIECE) {
            digest.update(Sha256::digest(piece));
        }
        digest.finalize().into()
    }

    #[test]
    fn the_parallel_payload_digest_equals_the_serial_one() {
        let data: Vec<u8> = (0..PARALLEL_FINGERPRINT_MIN * 2 + 12345)
            .map(|index| (index * 31 % 251) as u8)
            .collect();
        for length in [
            0,
            1,
            FINGERPRINT_PIECE - 1,
            FINGERPRINT_PIECE,
            FINGERPRINT_PIECE + 1,
            PARALLEL_FINGERPRINT_MIN - 1,
            PARALLEL_FINGERPRINT_MIN,
            data.len(),
        ] {
            assert_eq!(
                payload_digest(&data[..length]),
                serial_digest(&data[..length]),
                "{length}"
            );
        }
    }

    #[test]
    fn the_payload_digest_tells_apart_payloads_that_differ_in_one_byte_or_in_length() {
        let data = vec![7u8; PARALLEL_FINGERPRINT_MIN + 3];
        let mut changed = data.clone();
        changed[PARALLEL_FINGERPRINT_MIN + 1] ^= 1;
        assert_ne!(payload_digest(&data), payload_digest(&changed));
        assert_ne!(payload_digest(&data), payload_digest(&data[..data.len() - 1]));
        // A piece boundary cannot hide a length change: the length is hashed too.
        let zeros = vec![0u8; FINGERPRINT_PIECE * 2];
        assert_ne!(
            payload_digest(&zeros),
            payload_digest(&zeros[..FINGERPRINT_PIECE])
        );
    }
}
