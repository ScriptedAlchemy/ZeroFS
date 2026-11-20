use super::{JournalBlobIntegrityError, MutationRecord, Sequence};
use crate::writeback::payload::VerifiedPayload;
use anyhow::{Context, Result, ensure};
use bytes::Bytes;
use redb::{ReadOnlyTable, Table, TableDefinition};
use sha2::{Digest, Sha256};
use std::io::{self, Write};

pub(super) const PAYLOADS: TableDefinition<(Sequence, u64), &[u8]> =
    TableDefinition::new("raw_payload_chunks_v1");
pub(super) const RAW_REFERENCE: &str = "raw_payload_chunks_v1";
// Leave room for redb's leaf metadata within a 64 KiB buddy allocation.
const CHUNK_BYTES: usize = 60 * 1024;

pub(super) fn insert(
    table: &mut Table<'_, (Sequence, u64), &'static [u8]>,
    sequence: Sequence,
    payload: &VerifiedPayload,
) -> Result<()> {
    let mut writer = ChunkWriter {
        table,
        sequence,
        index: 0,
        buffer: Vec::with_capacity(CHUNK_BYTES),
    };
    payload.write_to(&mut writer)?;
    writer.flush()?;
    Ok(())
}

struct ChunkWriter<'a, 'txn> {
    table: &'a mut Table<'txn, (Sequence, u64), &'static [u8]>,
    sequence: Sequence,
    index: u64,
    buffer: Vec<u8>,
}

impl Write for ChunkWriter<'_, '_> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let count = bytes.len().min(CHUNK_BYTES - self.buffer.len());
        self.buffer.extend_from_slice(&bytes[..count]);
        if self.buffer.len() == CHUNK_BYTES {
            self.flush()?;
        }
        Ok(count)
    }

    fn flush(&mut self) -> io::Result<()> {
        if !self.buffer.is_empty() {
            self.table
                .insert((self.sequence, self.index), self.buffer.as_slice())
                .map_err(io::Error::other)?;
            self.index += 1;
            self.buffer.clear();
        }
        Ok(())
    }
}

pub(super) struct RawPayload {
    // The read-only table pins its redb snapshot even after remote cleanup.
    table: ReadOnlyTable<(Sequence, u64), &'static [u8]>,
    sequence: Sequence,
    len: u64,
    sha256: [u8; 32],
}

impl RawPayload {
    /// Open `record`'s payload and verify every chunk against its digest.
    pub(super) fn open(
        table: ReadOnlyTable<(Sequence, u64), &'static [u8]>,
        record: &MutationRecord,
        maximum_bytes: u64,
    ) -> Result<Self> {
        let payload = Self::bind(table, record, maximum_bytes)?;
        payload.verify(|_| ())?;
        Ok(payload)
    }

    /// Read `record`'s payload, verified against its digest in the same
    /// pass: the digest is of exactly the bytes returned.
    pub(super) fn read(
        table: ReadOnlyTable<(Sequence, u64), &'static [u8]>,
        record: &MutationRecord,
        maximum_bytes: u64,
    ) -> Result<Vec<u8>> {
        let payload = Self::bind(table, record, maximum_bytes)?;
        let mut bytes = Vec::new();
        bytes.try_reserve_exact(usize::try_from(payload.len)?)?;
        payload.verify(|chunk| bytes.extend_from_slice(chunk))?;
        Ok(bytes)
    }

    fn bind(
        table: ReadOnlyTable<(Sequence, u64), &'static [u8]>,
        record: &MutationRecord,
        maximum_bytes: u64,
    ) -> Result<Self> {
        ensure!(
            record.blob_path() == Some(RAW_REFERENCE),
            "invalid raw payload reference"
        );
        let (len, sha256) = record.payload().context("mutation has no payload")?;
        ensure!(
            len <= maximum_bytes,
            "committed blob exceeds bounded read limit"
        );
        Ok(Self {
            table,
            sequence: record.sequence,
            len,
            sha256,
        })
    }

    /// Pass every chunk to `visit` in order, then check their count, lengths
    /// and digest against the record.
    fn verify(&self, mut visit: impl FnMut(&[u8])) -> Result<()> {
        let mut hasher = Sha256::new();
        let mut bytes = 0;
        let chunks = self
            .table
            .range((self.sequence, 0)..=(self.sequence, u64::MAX))?;
        for (index, entry) in (0_u64..).zip(chunks) {
            let (key, chunk) = entry?;
            let expected_len = (self.len - bytes).min(CHUNK_BYTES as u64);
            ensure!(
                expected_len != 0
                    && key.value().1 == index
                    && chunk.value().len() as u64 == expected_len,
                JournalBlobIntegrityError::LengthMismatch
            );
            hasher.update(chunk.value());
            visit(chunk.value());
            bytes += expected_len;
        }
        ensure!(bytes == self.len, JournalBlobIntegrityError::LengthMismatch);
        let actual: [u8; 32] = hasher.finalize().into();
        ensure!(
            actual == self.sha256,
            JournalBlobIntegrityError::HashMismatch
        );
        Ok(())
    }

    pub(super) fn len(&self) -> u64 {
        self.len
    }

    pub(super) fn sha256(&self) -> [u8; 32] {
        self.sha256
    }

    pub(super) fn chunk_count(&self) -> u64 {
        self.len.div_ceil(CHUNK_BYTES as u64)
    }

    pub(super) fn read_chunk(&self, offset: u64, maximum: usize) -> Result<Bytes> {
        let (index, within) = (
            offset / CHUNK_BYTES as u64,
            (offset % CHUNK_BYTES as u64) as usize,
        );
        let chunk = self
            .table
            .get((self.sequence, index))?
            .context(JournalBlobIntegrityError::LengthMismatch)?;
        let count = maximum.min(chunk.value().len().saturating_sub(within));
        let bytes = chunk
            .value()
            .get(within..within + count)
            .context(JournalBlobIntegrityError::LengthMismatch)?;
        Ok(Bytes::copy_from_slice(bytes))
    }
}
