//! Object-store format v2. See docs/raw-device-engine.md for the commit protocol.
//!
//! Two 4 KiB labels precede two equal index banks, then allocatable 4 KiB
//! blocks. Labels, index pages and explicit block maps are SHA-256 sealed.
//! Numeric fields are little-endian.

use sha2::{Digest, Sha256};

use crate::{BLOCK_SIZE, Identity, Role};

pub(crate) const BLOCK: usize = BLOCK_SIZE as usize;
pub(crate) const LABEL_SLOTS: u64 = 2;
pub(crate) const LOG_START: u64 = LABEL_SLOTS * BLOCK_SIZE;
pub(crate) const VERSION: u32 = 2;

const LABEL_MAGIC: [u8; 8] = *b"ZFSRAWL2";
const OLD_LABEL_MAGIC: [u8; 8] = *b"ZFSRAWL1";
pub(crate) const CHECKSUM_AT: usize = BLOCK - 32;

pub(crate) const fn bank_bytes(size: u64) -> u64 {
    let small = size / 32;
    let small = if small < 16 << 20 { small } else { 16 << 20 };
    let scaled = size / 4096;
    let bytes = if small > scaled { small } else { scaled };
    bytes.div_ceil(BLOCK_SIZE) * BLOCK_SIZE
}

pub(crate) const fn data_start(size: u64) -> u64 {
    LOG_START + 2 * bank_bytes(size)
}

pub(crate) type Block = Box<[u8; BLOCK]>;

pub(crate) fn zero_block() -> Block {
    Box::new([0u8; BLOCK])
}

pub(crate) fn sha256(bytes: &[u8]) -> [u8; 32] {
    Sha256::digest(bytes).into()
}

pub(crate) fn seal(block: &mut [u8; BLOCK]) {
    let digest = sha256(&block[..CHECKSUM_AT]);
    block[CHECKSUM_AT..].copy_from_slice(&digest);
}

pub(crate) fn sealed(block: &[u8; BLOCK]) -> bool {
    sha256(&block[..CHECKSUM_AT]) == block[CHECKSUM_AT..]
}

fn u32_at(block: &[u8], at: usize) -> u32 {
    u32::from_le_bytes(block[at..at + 4].try_into().unwrap())
}

pub(crate) fn u64_at(block: &[u8], at: usize) -> u64 {
    u64::from_le_bytes(block[at..at + 8].try_into().unwrap())
}

pub(crate) fn array_at<const N: usize>(block: &[u8], at: usize) -> [u8; N] {
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
    pub last_seq: u64,
    pub bank: u8,
    pub bank_generation: u64,
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
    pub(crate) fn bank_start(&self) -> u64 {
        LOG_START + u64::from(self.bank) * bank_bytes(self.device_size)
    }

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
        block[96..104].copy_from_slice(&self.last_seq.to_le_bytes());
        block[104] = self.bank;
        block[112..120].copy_from_slice(&self.bank_generation.to_le_bytes());
        seal(&mut block);
        block
    }

    /// Decode the label read from `slot`. Every field is range checked, so a
    /// sealed but nonsensical label is invalid rather than trusted.
    pub(crate) fn decode(block: &[u8; BLOCK], slot: u64) -> SlotState {
        if block[0..8] != LABEL_MAGIC && block[0..8] != OLD_LABEL_MAGIC {
            return SlotState::Absent;
        }
        if !sealed(block) {
            return SlotState::Invalid("label checksum mismatch".into());
        }
        let version = u32_at(block, 8);
        if version != VERSION || block[0..8] == OLD_LABEL_MAGIC {
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
            last_seq: u64_at(block, 96),
            bank: block[104],
            bank_generation: u64_at(block, 112),
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
        if self.bank > 1 || !(1..=self.generation).contains(&self.bank_generation) {
            return Err("invalid index bank".into());
        }
        let start = self.bank_start();
        if !(start..=start + bank_bytes(self.device_size)).contains(&self.durable_tail)
            || !self.durable_tail.is_multiple_of(BLOCK_SIZE)
        {
            return Err(format!("durable tail {} out of range", self.durable_tail));
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
