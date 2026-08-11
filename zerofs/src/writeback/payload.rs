use crate::writeback::model::MutationRecord;
use bytes::Bytes;
use sha2::{Digest, Sha256};

/// Immutable payload bytes paired with the SHA-256 digest computed from them.
///
/// Keeping construction private to this module lets overlay and journal admission
/// compare record metadata in constant time without hashing the same bytes again.
#[derive(Clone)]
pub(crate) struct VerifiedPayload {
    bytes: Bytes,
    sha256: [u8; 32],
}

impl VerifiedPayload {
    pub(crate) fn new(bytes: Bytes) -> Self {
        let sha256 = Sha256::digest(&bytes).into();
        Self { bytes, sha256 }
    }

    pub(crate) fn byte_len(&self) -> u64 {
        self.bytes.len() as u64
    }

    pub(crate) fn sha256(&self) -> [u8; 32] {
        self.sha256
    }

    pub(crate) fn bytes(&self) -> &[u8] {
        &self.bytes
    }

    pub(crate) fn into_bytes(self) -> Bytes {
        self.bytes
    }

    pub(crate) fn matches_record(&self, record: &MutationRecord) -> bool {
        record.payload() == Some((self.byte_len(), self.sha256))
    }
}

#[cfg(test)]
mod tests {
    use super::VerifiedPayload;
    use bytes::Bytes;

    #[test]
    fn verified_payload_carries_the_digest_of_its_immutable_bytes() {
        let payload = VerifiedPayload::new(Bytes::from_static(b"abc"));

        assert_eq!(payload.byte_len(), 3);
        assert_eq!(
            payload.sha256(),
            [
                0xba, 0x78, 0x16, 0xbf, 0x8f, 0x01, 0xcf, 0xea, 0x41, 0x41, 0x40, 0xde, 0x5d, 0xae,
                0x22, 0x23, 0xb0, 0x03, 0x61, 0xa3, 0x96, 0x17, 0x7a, 0x9c, 0xb4, 0x10, 0xff, 0x61,
                0xf2, 0x00, 0x15, 0xad,
            ]
        );
        assert_eq!(payload.bytes(), b"abc");
    }
}
