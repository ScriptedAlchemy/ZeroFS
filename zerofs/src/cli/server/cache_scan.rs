//! Reads clean-cache partition files back and finds the bytes foyer writes
//! without defining them.
//!
//! foyer lays each block out as blobs. A blob is a 4 KiB index (checksum,
//! count, then one 24-byte record per entry) followed by its entries, each a
//! 36-byte header, key and value padded up to the next 4 KiB page. The index
//! tail past the last record and each entry's page padding carry no data, so
//! with the patched foyer-storage they are zero. This is the only description
//! of that layout in ZeroFS; the library tests and the scan report use it.

use anyhow::{Context, Result};
use serde::Serialize;
use std::ops::Range;
use std::path::Path;

const PAGE: usize = 4096;
const BLOB_INDEX: usize = 4096;
const BLOB_INDEX_HEAD: usize = 12;
const BLOB_INDEX_RECORD: usize = 24;
const ENTRY_HEADER: usize = 36;
const ENTRY_MAGIC: u32 = 0x9703_2700;
const ENTRY_MAGIC_MASK: u32 = 0xFFFF_FF00;

/// One blob of a block, as byte ranges within the block.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct BlobLayout {
    /// The index bytes past the last record.
    pub(crate) index_tail: Range<usize>,
    pub(crate) entries: Vec<EntryLayout>,
}

/// One entry of a blob, as byte ranges within the block.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct EntryLayout {
    /// Header, key and value.
    pub(crate) data: Range<usize>,
    /// From the end of the data to the next 4 KiB page.
    pub(crate) padding: Range<usize>,
}

fn be_u32(bytes: &[u8], at: usize) -> usize {
    u32::from_be_bytes(bytes[at..at + 4].try_into().expect("four bytes")) as usize
}

/// Walk one block's blobs the way foyer's recovery scanner does: from the
/// start, one blob after another, until an index does not describe entries
/// with valid headers.
pub(crate) fn blobs(block: &[u8]) -> Vec<BlobLayout> {
    let mut found = Vec::new();
    let mut blob = 0;
    while blob + BLOB_INDEX <= block.len() {
        let index = &block[blob..blob + BLOB_INDEX];
        let count = be_u32(index, 8);
        let capacity = (BLOB_INDEX - BLOB_INDEX_HEAD) / BLOB_INDEX_RECORD;
        if count == 0 || count > capacity {
            break;
        }
        let records = BLOB_INDEX_HEAD + count * BLOB_INDEX_RECORD;
        let (index_records, _) = index[BLOB_INDEX_HEAD..records].as_chunks::<BLOB_INDEX_RECORD>();
        let mut entries = Vec::with_capacity(count);
        for record in index_records {
            let start = blob + be_u32(record, 16);
            let len = be_u32(record, 20);
            let end = start + len.next_multiple_of(PAGE);
            if len < ENTRY_HEADER
                || end > block.len()
                || be_u32(block, start + 32) as u32 & ENTRY_MAGIC_MASK != ENTRY_MAGIC
            {
                // Not a blob foyer wrote.
                return found;
            }
            entries.push(EntryLayout {
                data: start..start + len,
                padding: start + len..end,
            });
        }
        let next = entries.last().map_or(blob, |entry| entry.padding.end);
        found.push(BlobLayout {
            index_tail: blob + records..blob + BLOB_INDEX,
            entries,
        });
        blob = next;
    }
    found
}

/// Counts of undefined bytes in one cache directory's partitions.
#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct UndefinedBytes {
    /// Partition files of the cache's block size that were read.
    pub(crate) partitions: usize,
    /// Partition files of another size, which hold no data blocks.
    pub(crate) other_partitions: usize,
    pub(crate) blobs: usize,
    pub(crate) entries: usize,
    pub(crate) index_tail_bytes: usize,
    pub(crate) index_tail_nonzero: usize,
    pub(crate) padding_bytes: usize,
    pub(crate) padding_nonzero: usize,
}

fn count_nonzero(bytes: &[u8]) -> usize {
    bytes.iter().filter(|b| **b != 0).count()
}

/// Read every foyer partition file directly in `dir`, read-only, and count
/// the undefined bytes of every blob in each block of `block_size`.
/// `visit` sees each block with its blobs, for callers that look further.
pub(crate) fn scan_partitions(
    dir: &Path,
    block_size: usize,
    mut visit: impl FnMut(&[u8], &[BlobLayout]),
) -> Result<UndefinedBytes> {
    let mut counts = UndefinedBytes::default();
    let mut partitions = Vec::new();
    for entry in std::fs::read_dir(dir)
        .with_context(|| format!("reading cache directory {}", dir.display()))?
    {
        let entry = entry?;
        if entry
            .file_name()
            .to_str()
            .and_then(super::foyer_partition_id)
            .is_none()
        {
            continue;
        }
        let path = entry.path();
        if !std::fs::symlink_metadata(&path)?.file_type().is_file() {
            anyhow::bail!("cache partition {} is not a regular file", path.display());
        }
        partitions.push(path);
    }
    partitions.sort();
    for path in partitions {
        let block = std::fs::read(&path)
            .with_context(|| format!("reading cache partition {}", path.display()))?;
        if block.len() != block_size {
            counts.other_partitions += 1;
            continue;
        }
        counts.partitions += 1;
        let found = blobs(&block);
        for blob in &found {
            counts.blobs += 1;
            let tail = &block[blob.index_tail.clone()];
            counts.index_tail_bytes += tail.len();
            counts.index_tail_nonzero += count_nonzero(tail);
            for entry in &blob.entries {
                let padding = &block[entry.padding.clone()];
                counts.entries += 1;
                counts.padding_bytes += padding.len();
                counts.padding_nonzero += count_nonzero(padding);
            }
        }
        visit(&block, &found);
    }
    Ok(counts)
}
