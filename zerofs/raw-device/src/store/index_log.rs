//! A bounded index log. New pages are immutable until both labels leave the bank.
use super::{Entry, ObjectInfo, allocation::Allocation, corrupt};
use crate::format::{
    BLOCK, Block, CHECKSUM_AT, Label, bank_bytes, seal, sealed, u64_at, zero_block,
};
use crate::region::Region;
use crate::{BLOCK_SIZE, MAX_KEY_LEN, MAX_OBJECT_LEN, Result};
use std::io;

const MAGIC: [u8; 8] = *b"ZFSRAWI2";
const HEADER: usize = 42;
const PUT_FIELDS: usize = 56;
const RECORD_HEAD: usize = 10;
const MAX_RECORD: usize = RECORD_HEAD + MAX_KEY_LEN + PUT_FIELDS;
const _: () = assert!(CHECKSUM_AT - HEADER - MAX_RECORD > BLOCK / 2);

// This also bounds snapshot size. All but the last packed page use more
// than half a block, and each live entry is charged more than its encoding.
pub(super) const ENTRY_CHARGE: usize = 192;

pub(super) struct IndexWriter<'a> {
    file: &'a Region,
    label: &'a Label,
    offset: u64,
    block: Block,
    used: usize,
}

impl<'a> IndexWriter<'a> {
    pub(super) fn new(file: &'a Region, label: &'a Label) -> Self {
        Self {
            file,
            label,
            offset: label.durable_tail,
            block: zero_block(),
            used: HEADER,
        }
    }

    pub(super) fn push(&mut self, key: &[u8], entry: Option<&Entry>) -> io::Result<()> {
        let bytes = RECORD_HEAD + key.len() + entry.map_or(0, |_| PUT_FIELDS);
        if self.used + bytes > CHECKSUM_AT {
            self.write_page()?;
        }
        let at = self.used;
        self.block[at..at + 2].copy_from_slice(&(key.len() as u16).to_le_bytes());
        let allocation = entry.map_or(0, |entry| entry.allocation.0);
        self.block[at + 2..at + RECORD_HEAD].copy_from_slice(&allocation.to_le_bytes());
        self.block[at + RECORD_HEAD..at + RECORD_HEAD + key.len()].copy_from_slice(key);
        if let Some(entry) = entry {
            let at = at + RECORD_HEAD + key.len();
            self.block[at..at + 8].copy_from_slice(&entry.info.seq.to_le_bytes());
            self.block[at + 8..at + 16].copy_from_slice(&entry.info.len.to_le_bytes());
            self.block[at + 16..at + 24]
                .copy_from_slice(&entry.info.modified_unix_nanos.to_le_bytes());
            self.block[at + 24..at + PUT_FIELDS].copy_from_slice(&entry.info.sha256);
        }
        self.used += bytes;
        Ok(())
    }

    pub(super) fn finish(mut self) -> io::Result<u64> {
        if self.used != HEADER {
            self.write_page()?;
        }
        Ok(self.offset)
    }

    fn write_page(&mut self) -> io::Result<()> {
        if self.offset >= self.label.bank_start() + bank_bytes(self.label.device_size) {
            return Err(io::Error::other("raw index bank exceeded"));
        }
        self.block[..8].copy_from_slice(&MAGIC);
        self.block[8..24].copy_from_slice(&self.label.nonce);
        self.block[24..32].copy_from_slice(&self.label.bank_generation.to_le_bytes());
        self.block[32..40].copy_from_slice(&self.offset.to_le_bytes());
        self.block[40..42].copy_from_slice(&(self.used as u16).to_le_bytes());
        seal(&mut self.block);
        self.file.write_all_at(&self.block[..], self.offset)?;
        self.offset += BLOCK_SIZE;
        self.block.fill(0);
        self.used = HEADER;
        Ok(())
    }
}

pub(super) fn replay(
    file: &Region,
    label: &Label,
    mut apply: impl FnMut(&[u8], Option<Entry>) -> Result<()>,
) -> Result<()> {
    let mut block = zero_block();
    for offset in (label.bank_start()..label.durable_tail).step_by(BLOCK) {
        file.read_exact_at(&mut block[..], offset)?;
        let used = u16::from_le_bytes(block[40..42].try_into().unwrap()) as usize;
        if block[..8] != MAGIC
            || block[8..24] != label.nonce
            || u64_at(&block[..], 24) != label.bank_generation
            || u64_at(&block[..], 32) != offset
            || !(HEADER + RECORD_HEAD + 1..=CHECKSUM_AT).contains(&used)
            || !sealed(&block)
        {
            return Err(corrupt(offset, "invalid index page"));
        }
        let mut records = &block[HEADER..used];
        while !records.is_empty() {
            let (key, entry, rest) = decode(records, label.last_seq)
                .ok_or_else(|| corrupt(offset, "invalid index record"))?;
            apply(key, entry)?;
            records = rest;
        }
    }
    Ok(())
}

fn decode(record: &[u8], last_seq: u64) -> Option<(&[u8], Option<Entry>, &[u8])> {
    let key_len = u16::from_le_bytes(record.get(..2)?.try_into().ok()?) as usize;
    if !(1..=MAX_KEY_LEN).contains(&key_len) {
        return None;
    }
    let allocation = u64::from_le_bytes(record.get(2..RECORD_HEAD)?.try_into().ok()?);
    let (key, rest) = record.get(RECORD_HEAD..)?.split_at_checked(key_len)?;
    if allocation == 0 {
        return Some((key, None, rest));
    }
    let (fields, rest) = rest.split_at_checked(PUT_FIELDS)?;
    let seq = u64::from_le_bytes(fields[..8].try_into().ok()?);
    let len = u64::from_le_bytes(fields[8..16].try_into().ok()?);
    if seq == 0 || seq > last_seq || len > MAX_OBJECT_LEN {
        return None;
    }
    Some((
        key,
        Some(Entry {
            allocation: Allocation(allocation),
            info: ObjectInfo {
                seq,
                len,
                modified_unix_nanos: u64::from_le_bytes(fields[16..24].try_into().ok()?),
                sha256: fields[24..56].try_into().ok()?,
            },
        }),
        rest,
    ))
}
