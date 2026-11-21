//! 4 KiB block allocation. Only live and still-pinned objects own blocks.
use super::corrupt;
use crate::format::{BLOCK, CHECKSUM_AT, array_at, blocks_for, seal, sealed, u64_at, zero_block};
use crate::region::Region;
use crate::{BLOCK_SIZE, Error, Result};
use std::io;
use std::ops::Range;

const MAP_MAGIC: [u8; 8] = *b"ZFSRAWM2";
const MAP_HEADER: usize = 48;
const MAP_ENTRIES: u64 = ((CHECKSUM_AT - MAP_HEADER) / 8) as u64;
const CONTIGUOUS: u64 = 1 << 63;

const fn map_blocks(value_blocks: u64) -> u64 {
    let blocks = value_blocks.div_ceil(MAP_ENTRIES);
    if blocks == 0 { 1 } else { blocks }
}

pub(super) const fn cost(len: u64) -> Option<u64> {
    let value = len.div_ceil(BLOCK_SIZE);
    match value.checked_add(map_blocks(value)) {
        Some(blocks) => blocks.checked_mul(BLOCK_SIZE),
        None => None,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct Allocation(pub(super) u64);

impl Allocation {
    pub(super) fn new(blocks: &[u64]) -> Self {
        let contiguous = blocks
            .windows(2)
            .all(|pair| pair[0] + BLOCK_SIZE == pair[1]);
        Self(blocks[0] | if contiguous { CONTIGUOUS } else { 0 })
    }

    pub(super) fn start(self) -> u64 {
        self.0 & !CONTIGUOUS
    }

    fn contiguous(self) -> bool {
        self.0 & CONTIGUOUS != 0
    }

    pub(super) fn write(
        self,
        file: &Region,
        nonce: &[u8; 16],
        seq: u64,
        blocks: &[u64],
        value: &[u8],
    ) -> io::Result<()> {
        let value_blocks = blocks_for(value.len() as u64) / BLOCK_SIZE;
        let (maps, payload) = blocks.split_at(map_blocks(value_blocks) as usize);
        if !self.contiguous() {
            let mut block = zero_block();
            for (index, positions) in payload.chunks(MAP_ENTRIES as usize).enumerate() {
                block.fill(0);
                block[..8].copy_from_slice(&MAP_MAGIC);
                block[8..24].copy_from_slice(nonce);
                block[24..32].copy_from_slice(&seq.to_le_bytes());
                block[32..40].copy_from_slice(&(index as u64).to_le_bytes());
                let next = maps.get(index + 1).copied().unwrap_or(0);
                block[40..48].copy_from_slice(&next.to_le_bytes());
                let (entries, _) = block[MAP_HEADER..CHECKSUM_AT].as_chunks_mut::<8>();
                for (dest, position) in entries.iter_mut().zip(positions) {
                    *dest = position.to_le_bytes();
                }
                seal(&mut block);
                file.write_all_at(&block[..], maps[index])?;
            }
        }
        let mut cursor = 0;
        for run in runs(payload) {
            let end = (cursor + (run.end - run.start) as usize).min(value.len());
            file.write_all_at(&value[cursor..end], run.start)?;
            cursor = end;
        }
        Ok(())
    }

    /// The maps themselves and the payload blocks, in allocation order.
    pub(super) fn blocks(
        self,
        file: &Region,
        data_start: u64,
        nonce: &[u8; 16],
        seq: u64,
        len: u64,
    ) -> Result<Blocks> {
        let value_blocks = blocks_for(len) / BLOCK_SIZE;
        let maps = map_blocks(value_blocks);
        let total = maps + value_blocks;
        let at = self.start();
        let check = |offset: u64| {
            if (data_start..file.len).contains(&offset) && offset.is_multiple_of(BLOCK_SIZE) {
                Ok(())
            } else {
                Err(corrupt(offset, "allocation block outside payload area"))
            }
        };
        check(at)?;
        if self.contiguous() {
            let end = at + total * BLOCK_SIZE;
            if end > file.len {
                return Err(corrupt(at, "contiguous allocation outside payload area"));
            }
            return Ok(Blocks::Contiguous { at, maps, end });
        }
        if value_blocks == 0 {
            return Err(corrupt(at, "empty allocation has a block map"));
        }
        let mut positions = vec![0; total as usize];
        let (map_positions, payload) = positions.split_at_mut(maps as usize);
        let mut block = zero_block();
        let mut offset = at;
        for (index, chunk) in payload.chunks_mut(MAP_ENTRIES as usize).enumerate() {
            check(offset)?;
            file.read_exact_at(&mut block[..], offset)?;
            if !valid_map(&block, nonce, seq, index as u64) {
                return Err(corrupt(offset, "invalid allocation map"));
            }
            map_positions[index] = offset;
            for (index, dest) in chunk.iter_mut().enumerate() {
                *dest = u64_at(&block[..], MAP_HEADER + index * 8);
                check(*dest)?;
            }
            offset = u64_at(&block[..], 40);
        }
        if offset != 0 {
            return Err(corrupt(
                at,
                "allocation map continues past the object length",
            ));
        }
        Ok(Blocks::Mapped {
            positions,
            maps: maps as usize,
        })
    }
}

fn valid_map(block: &[u8; BLOCK], nonce: &[u8; 16], seq: u64, index: u64) -> bool {
    block[..8] == MAP_MAGIC
        && array_at::<16>(block, 8) == *nonce
        && u64_at(block, 24) == seq
        && u64_at(block, 32) == index
        && sealed(block)
}

pub(super) enum Blocks {
    Contiguous { at: u64, maps: u64, end: u64 },
    Mapped { positions: Vec<u64>, maps: usize },
}

impl Blocks {
    pub(super) fn all_runs(&self) -> impl Iterator<Item = Range<u64>> {
        let (contiguous, mapped) = match self {
            Self::Contiguous { at, end, .. } => (Some(*at..*end), &[][..]),
            Self::Mapped { positions, .. } => (None, positions.as_slice()),
        };
        contiguous.into_iter().chain(runs(mapped))
    }

    pub(super) fn data_runs(&self) -> impl Iterator<Item = Range<u64>> {
        let (contiguous, mapped) = match self {
            Self::Contiguous { at, maps, end } => {
                let payload = *at + *maps * BLOCK_SIZE;
                ((payload < *end).then_some(payload..*end), &[][..])
            }
            Self::Mapped { positions, maps } => (None, &positions[*maps..]),
        };
        contiguous.into_iter().chain(runs(mapped))
    }
}

fn runs(blocks: &[u64]) -> impl Iterator<Item = Range<u64>> {
    blocks
        .chunk_by(|a, b| *a + BLOCK_SIZE == *b)
        .map(|run| run[0]..run[run.len() - 1] + BLOCK_SIZE)
}

#[derive(Debug)]
pub(super) struct Allocator {
    start: u64,
    count: u64,
    used: u64,
    cursor: u64,
    bits: Vec<u64>,
}

impl Allocator {
    pub(super) fn new(start: u64, end: u64) -> Self {
        Self {
            start,
            count: (end - start) / BLOCK_SIZE,
            used: 0,
            cursor: 0,
            bits: Vec::new(),
        }
    }

    pub(super) fn free(&self) -> u64 {
        (self.count - self.used) * BLOCK_SIZE
    }

    pub(super) fn allocate(&mut self, bytes: u64) -> Result<Vec<u64>> {
        if bytes > self.free() {
            return Err(Error::NoSpace {
                needed: bytes,
                available: self.free(),
            });
        }
        let count = (bytes / BLOCK_SIZE) as usize;
        let mut positions = Vec::with_capacity(count);
        let mut cursor = self.cursor;
        while positions.len() < count {
            let word = (cursor / 64) as usize;
            let free = !self.bits.get(word).copied().unwrap_or(0) & (u64::MAX << (cursor % 64));
            let next = word as u64 * 64 + u64::from(free.trailing_zeros());
            if next >= self.count {
                cursor = 0;
                continue;
            }
            if free == 0 {
                cursor = next;
                continue;
            }
            positions.push(self.start + next * BLOCK_SIZE);
            cursor = next + 1;
        }
        self.claim(runs(&positions))?;
        self.cursor = cursor % self.count;
        Ok(positions)
    }

    fn ensure_word(&mut self, word: usize) -> Result<()> {
        if self.bits.is_empty() {
            let count = self.count.div_ceil(64) as usize;
            self.bits
                .try_reserve_exact(count)
                .map_err(io::Error::other)?;
            self.bits.resize(count, 0);
        }
        debug_assert!(word < self.bits.len());
        Ok(())
    }

    pub(super) fn claim(&mut self, ranges: impl Iterator<Item = Range<u64>>) -> Result<()> {
        for range in ranges {
            let blocks = self.block_range(range);
            self.ensure_word(((blocks.end - 1) / 64) as usize)?;
            for (word, mask) in word_masks(blocks) {
                if self.bits[word] & mask != 0 {
                    return Err(corrupt(
                        self.start + word as u64 * 64 * BLOCK_SIZE,
                        "live allocations overlap",
                    ));
                }
                self.bits[word] |= mask;
                self.used += u64::from(mask.count_ones());
            }
        }
        Ok(())
    }

    pub(super) fn release(&mut self, ranges: impl Iterator<Item = Range<u64>>) {
        for range in ranges {
            for (word, mask) in word_masks(self.block_range(range)) {
                self.bits[word] &= !mask;
                self.used -= u64::from(mask.count_ones());
            }
        }
    }

    fn block_range(&self, bytes: Range<u64>) -> Range<u64> {
        (bytes.start - self.start) / BLOCK_SIZE..(bytes.end - self.start) / BLOCK_SIZE
    }
}

fn word_masks(blocks: Range<u64>) -> impl Iterator<Item = (usize, u64)> {
    let first = blocks.start / 64;
    let last = (blocks.end - 1) / 64;
    (first..=last).map(move |word| {
        let start = blocks.start.saturating_sub(word * 64);
        let end = (blocks.end - word * 64).min(64);
        let mask = (u64::MAX << start) & (u64::MAX >> (64 - end));
        (word as usize, mask)
    })
}
