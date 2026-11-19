//! On-disk layout, version 1.
//!
//! ```text
//! block 0            label slot 0
//! block 1            label slot 1
//! block 2 ..         log: [record header block][payload blocks, zero padded] ...
//! ```
//!
//! All integers are little-endian. Every label and record header occupies one
//! [`BLOCK_SIZE`] block whose last 32 bytes are the SHA-256 of the preceding
//! bytes, so a torn or misdirected block fails validation.
//!
//! Label slot (block 0 or 1):
//!
//! | offset | size | field                                   |
//! |--------|------|-----------------------------------------|
//! | 0      | 8    | magic `ZFSRAWL1`                        |
//! | 8      | 4    | format version (1)                      |
//! | 12     | 4    | block size (4096)                       |
//! | 16     | 8    | generation, +1 per flush                |
//! | 24     | 8    | device size                             |
//! | 32     | 16   | pool UUID                               |
//! | 48     | 16   | member UUID                             |
//! | 64     | 1    | role                                    |
//! | 65     | 1    | slot index this block was written to    |
//! | 72     | 16   | provision nonce                         |
//! | 88     | 8    | durable tail (absolute offset)          |
//! | 96     | 8    | durable record count                    |
//! | 4064   | 32   | SHA-256 of bytes 0..4064                |
//!
//! Both slots hold the same checkpoint once a flush is acknowledged. A flush
//! writes the next generation to slot 0, syncs, writes it to slot 1, and
//! syncs again before returning. Open therefore accepts exactly these states:
//!
//! * both slots valid, same generation: the steady state;
//! * slot 0 at generation `g + 1`, slot 1 at `g`: a flush was interrupted
//!   between its two label writes; slot 0 wins;
//! * one slot valid, the other blank or invalid: an interrupted write or
//!   later damage to one slot. The valid slot is still at least the last
//!   acknowledged checkpoint, because that checkpoint was in both slots.
//!
//! Anything else (slot 1 ahead of slot 0, generations further apart, slots
//! from different provisionings, both slots unusable) is reported as corrupt.
//! The slot index inside each block catches a label copied to the wrong slot.
//!
//! Record header (one block, followed by `ceil(value_len / 4096)` payload
//! blocks):
//!
//! | offset | size    | field                                |
//! |--------|---------|--------------------------------------|
//! | 0      | 8       | magic `ZFSRAWR1`                     |
//! | 8      | 16      | provision nonce                      |
//! | 24     | 8       | sequence number, 1-based, contiguous |
//! | 32     | 1       | kind: 1 = put, 2 = delete            |
//! | 34     | 2       | key length                           |
//! | 40     | 8       | value length (0 for delete)          |
//! | 48     | 32      | SHA-256 of the value                 |
//! | 80     | key len | key                                  |
//! | 1104   | 8       | modification time, Unix nanoseconds  |
//! | 4064   | 32      | SHA-256 of bytes 0..4064             |
//!
//! The nonce ties records to one provisioning, and the contiguous sequence
//! ties them to their position, so leftovers from an earlier provisioning or
//! a stale tail cannot be mistaken for live records.

use sha2::{Digest, Sha256};

use crate::{BLOCK_SIZE, Identity, MAX_KEY_LEN, Role};

pub(crate) const BLOCK: usize = BLOCK_SIZE as usize;
pub(crate) const LABEL_SLOTS: u64 = 2;
pub(crate) const LOG_START: u64 = LABEL_SLOTS * BLOCK_SIZE;
pub(crate) const VERSION: u32 = 1;

const LABEL_MAGIC: [u8; 8] = *b"ZFSRAWL1";
const RECORD_MAGIC: [u8; 8] = *b"ZFSRAWR1";
const CHECKSUM_AT: usize = BLOCK - 32;
const KEY_AT: usize = 80;
const MODIFIED_AT: usize = KEY_AT + MAX_KEY_LEN;

const _: () = assert!(MODIFIED_AT + 8 <= CHECKSUM_AT);

pub(crate) type Block = Box<[u8; BLOCK]>;

pub(crate) fn zero_block() -> Block {
    Box::new([0u8; BLOCK])
}

pub(crate) fn sha256(bytes: &[u8]) -> [u8; 32] {
    Sha256::digest(bytes).into()
}

fn seal(block: &mut [u8; BLOCK]) {
    let digest = sha256(&block[..CHECKSUM_AT]);
    block[CHECKSUM_AT..].copy_from_slice(&digest);
}

fn sealed(block: &[u8; BLOCK]) -> bool {
    sha256(&block[..CHECKSUM_AT]) == block[CHECKSUM_AT..]
}

fn u32_at(block: &[u8], at: usize) -> u32 {
    u32::from_le_bytes(block[at..at + 4].try_into().unwrap())
}

fn u64_at(block: &[u8], at: usize) -> u64 {
    u64::from_le_bytes(block[at..at + 8].try_into().unwrap())
}

fn array_at<const N: usize>(block: &[u8], at: usize) -> [u8; N] {
    block[at..at + N].try_into().unwrap()
}

/// Round up to a whole number of blocks.
pub(crate) fn blocks_for(len: u64) -> u64 {
    len.div_ceil(BLOCK_SIZE) * BLOCK_SIZE
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Label {
    pub generation: u64,
    pub device_size: u64,
    pub identity: Identity,
    pub nonce: [u8; 16],
    pub durable_tail: u64,
    pub record_count: u64,
}

/// What a label slot holds.
#[derive(Debug)]
pub(crate) enum SlotState {
    /// No ZeroFS magic: blank or foreign.
    Absent,
    /// ZeroFS magic, but the block fails validation (torn or damaged).
    Invalid(String),
    /// A ZeroFS label of a version this code cannot read.
    Unsupported(u32),
    Valid(Label),
}

impl Label {
    pub(crate) fn encode(&self, slot: u64) -> Block {
        let mut block = zero_block();
        block[0..8].copy_from_slice(&LABEL_MAGIC);
        block[8..12].copy_from_slice(&VERSION.to_le_bytes());
        block[12..16].copy_from_slice(&(BLOCK_SIZE as u32).to_le_bytes());
        block[16..24].copy_from_slice(&self.generation.to_le_bytes());
        block[24..32].copy_from_slice(&self.device_size.to_le_bytes());
        block[32..48].copy_from_slice(&self.identity.pool_uuid);
        block[48..64].copy_from_slice(&self.identity.member_uuid);
        block[64] = self.identity.role as u8;
        block[65] = slot as u8;
        block[72..88].copy_from_slice(&self.nonce);
        block[88..96].copy_from_slice(&self.durable_tail.to_le_bytes());
        block[96..104].copy_from_slice(&self.record_count.to_le_bytes());
        seal(&mut block);
        block
    }

    /// Decode the label read from `slot`. Every field is range checked, so a
    /// sealed but nonsensical label is invalid rather than trusted.
    pub(crate) fn decode(block: &[u8; BLOCK], slot: u64) -> SlotState {
        if block[0..8] != LABEL_MAGIC {
            return SlotState::Absent;
        }
        if !sealed(block) {
            return SlotState::Invalid("label checksum mismatch".into());
        }
        let version = u32_at(block, 8);
        if version != VERSION {
            return SlotState::Unsupported(version);
        }
        if u32_at(block, 12) != BLOCK_SIZE as u32 {
            return SlotState::Invalid(format!("label block size {}", u32_at(block, 12)));
        }
        if u64::from(block[65]) != slot {
            return SlotState::Invalid(format!(
                "label written for slot {} found in slot {slot}",
                block[65]
            ));
        }
        let Some(role) = Role::from_u8(block[64]) else {
            return SlotState::Invalid(format!("unknown role {}", block[64]));
        };
        let label = Label {
            generation: u64_at(block, 16),
            device_size: u64_at(block, 24),
            identity: Identity {
                pool_uuid: array_at(block, 32),
                member_uuid: array_at(block, 48),
                role,
            },
            nonce: array_at(block, 72),
            durable_tail: u64_at(block, 88),
            record_count: u64_at(block, 96),
        };
        match label.check() {
            Ok(()) => SlotState::Valid(label),
            Err(reason) => SlotState::Invalid(reason),
        }
    }

    fn check(&self) -> Result<(), String> {
        if self.generation == 0 || self.generation == u64::MAX {
            return Err(format!("label generation {} out of range", self.generation));
        }
        if let Err(reason) = check_device_size(self.device_size) {
            return Err(format!("label device size: {reason}"));
        }
        if self.identity.validate().is_err() {
            return Err("label carries a nil UUID".into());
        }
        let tail_ok = self.durable_tail >= LOG_START
            && self.durable_tail <= self.device_size
            && self.durable_tail.is_multiple_of(BLOCK_SIZE);
        if !tail_ok {
            return Err(format!("durable tail {} out of range", self.durable_tail));
        }
        // Each record takes at least one block.
        if self.record_count > (self.durable_tail - LOG_START) / BLOCK_SIZE {
            return Err(format!(
                "record count {} cannot fit below durable tail {}",
                self.record_count, self.durable_tail
            ));
        }
        Ok(())
    }
}

/// Bounds and alignment every device size must meet.
pub(crate) fn check_device_size(size: u64) -> Result<(), String> {
    if size < crate::MIN_DEVICE_SIZE {
        return Err(format!(
            "{size} bytes is below the {}-byte minimum",
            crate::MIN_DEVICE_SIZE
        ));
    }
    if size > crate::MAX_DEVICE_SIZE {
        return Err(format!(
            "{size} bytes exceeds the {}-byte maximum",
            crate::MAX_DEVICE_SIZE
        ));
    }
    if !size.is_multiple_of(BLOCK_SIZE) {
        return Err(format!("{size} bytes is not a multiple of {BLOCK_SIZE}"));
    }
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub(crate) enum Kind {
    Put = 1,
    Delete = 2,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RecordHeader {
    pub seq: u64,
    pub kind: Kind,
    pub key: Vec<u8>,
    pub value_len: u64,
    pub value_sha256: [u8; 32],
    /// Wall-clock time of the append. Recorded, never validated: clocks may
    /// step backwards between records.
    pub modified_unix_nanos: u64,
}

impl RecordHeader {
    /// Bytes this record occupies in the log, header block included.
    pub(crate) fn span(&self) -> u64 {
        BLOCK_SIZE + blocks_for(self.value_len)
    }

    pub(crate) fn encode(&self, nonce: &[u8; 16]) -> Block {
        debug_assert!(self.key.len() <= MAX_KEY_LEN);
        let mut block = zero_block();
        block[0..8].copy_from_slice(&RECORD_MAGIC);
        block[8..24].copy_from_slice(nonce);
        block[24..32].copy_from_slice(&self.seq.to_le_bytes());
        block[32] = self.kind as u8;
        block[34..36].copy_from_slice(&(self.key.len() as u16).to_le_bytes());
        block[40..48].copy_from_slice(&self.value_len.to_le_bytes());
        block[48..80].copy_from_slice(&self.value_sha256);
        block[KEY_AT..KEY_AT + self.key.len()].copy_from_slice(&self.key);
        block[MODIFIED_AT..MODIFIED_AT + 8]
            .copy_from_slice(&self.modified_unix_nanos.to_le_bytes());
        seal(&mut block);
        block
    }

    /// Decode and validate a header against the provisioning nonce and the
    /// sequence number its position requires.
    pub(crate) fn decode(
        block: &[u8; BLOCK],
        nonce: &[u8; 16],
        expected_seq: u64,
    ) -> Result<Self, String> {
        if block[0..8] != RECORD_MAGIC {
            return Err("record magic missing".into());
        }
        if !sealed(block) {
            return Err("record header checksum mismatch".into());
        }
        if block[8..24] != nonce[..] {
            return Err("record belongs to a different provisioning".into());
        }
        let seq = u64_at(block, 24);
        if seq != expected_seq {
            return Err(format!("record sequence {seq}, expected {expected_seq}"));
        }
        let kind = match block[32] {
            1 => Kind::Put,
            2 => Kind::Delete,
            other => return Err(format!("unknown record kind {other}")),
        };
        let key_len = u16::from_le_bytes([block[34], block[35]]) as usize;
        if key_len == 0 || key_len > MAX_KEY_LEN {
            return Err(format!("key length {key_len} out of range"));
        }
        let value_len = u64_at(block, 40);
        if value_len > crate::MAX_OBJECT_LEN || (kind == Kind::Delete && value_len != 0) {
            return Err(format!("value length {value_len} invalid for {kind:?}"));
        }
        Ok(Self {
            seq,
            kind,
            key: block[KEY_AT..KEY_AT + key_len].to_vec(),
            value_len,
            value_sha256: array_at(block, 48),
            modified_unix_nanos: u64_at(block, MODIFIED_AT),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn identity() -> Identity {
        Identity {
            pool_uuid: [1; 16],
            member_uuid: [2; 16],
            role: Role::Metadata,
        }
    }

    fn label() -> Label {
        Label {
            generation: 7,
            device_size: 1 << 20,
            identity: identity(),
            nonce: [9; 16],
            durable_tail: LOG_START + BLOCK_SIZE,
            record_count: 1,
        }
    }

    fn valid(label: &Label, slot: u64) -> bool {
        matches!(
            Label::decode(&label.encode(slot), slot),
            SlotState::Valid(_)
        )
    }

    #[test]
    fn label_round_trips_in_both_slots() {
        for slot in 0..LABEL_SLOTS {
            match Label::decode(&label().encode(slot), slot) {
                SlotState::Valid(decoded) => assert_eq!(decoded, label()),
                other => panic!("unexpected {other:?}"),
            }
        }
    }

    #[test]
    fn label_detects_every_single_byte_flip() {
        let encoded = label().encode(1);
        for at in 0..BLOCK {
            let mut damaged = encoded.clone();
            damaged[at] ^= 0x40;
            assert!(
                !matches!(Label::decode(&damaged, 1), SlotState::Valid(_)),
                "flip at byte {at} went undetected"
            );
        }
    }

    #[test]
    fn label_copied_to_the_other_slot_is_invalid() {
        assert!(matches!(
            Label::decode(&label().encode(0), 1),
            SlotState::Invalid(_)
        ));
    }

    #[test]
    fn blank_and_foreign_blocks_are_absent() {
        assert!(matches!(Label::decode(&zero_block(), 0), SlotState::Absent));
        let mut foreign = zero_block();
        foreign[..4].copy_from_slice(b"XFSB");
        assert!(matches!(Label::decode(&foreign, 0), SlotState::Absent));
    }

    #[test]
    fn sealed_labels_with_out_of_range_fields_are_invalid() {
        let mutations: [fn(&mut Label); 10] = [
            |l| l.durable_tail = l.device_size + BLOCK_SIZE,
            |l| l.durable_tail = LOG_START - BLOCK_SIZE,
            |l| l.durable_tail = LOG_START + 1,
            |l| l.generation = 0,
            |l| l.generation = u64::MAX,
            |l| l.device_size = crate::MAX_DEVICE_SIZE + BLOCK_SIZE,
            |l| l.device_size += 512,
            |l| l.record_count = u64::MAX,
            |l| l.identity.pool_uuid = [0; 16],
            |l| l.identity.member_uuid = [0; 16],
        ];
        for (i, mutate) in mutations.iter().enumerate() {
            let mut bad = label();
            mutate(&mut bad);
            assert!(!valid(&bad, 0), "mutation {i} was accepted");
        }
        let mut full = label();
        full.record_count = 1;
        assert!(valid(&full, 0));
    }

    #[test]
    fn record_round_trips_and_binds_nonce_and_sequence() {
        let header = RecordHeader {
            seq: 5,
            kind: Kind::Put,
            key: b"objects/a".to_vec(),
            value_len: 5000,
            value_sha256: sha256(b"x"),
            modified_unix_nanos: 1_700_000_000_123_456_789,
        };
        let nonce = [3; 16];
        let encoded = header.encode(&nonce);
        assert_eq!(RecordHeader::decode(&encoded, &nonce, 5).unwrap(), header);
        assert_eq!(header.span(), 3 * BLOCK_SIZE);
        assert!(RecordHeader::decode(&encoded, &[4; 16], 5).is_err());
        assert!(RecordHeader::decode(&encoded, &nonce, 6).is_err());
    }
}
