use super::raw_log::{self, Pin};
use super::{JournalBlobIntegrityError, MutationRecord, Sequence};
use anyhow::{Context, Result, ensure};
use bytes::Bytes;
use redb::{ReadOnlyTable, TableDefinition};
use sha2::{Digest, Sha256};

/// Payload chunks written before the payload log. Still read, never written.
pub(super) const PAYLOADS: TableDefinition<(Sequence, u64), &[u8]> =
    TableDefinition::new("raw_payload_chunks_v1");
/// One read snapshot of the payload chunks.
pub(super) type PayloadChunks = ReadOnlyTable<(Sequence, u64), &'static [u8]>;
pub(super) const RAW_REFERENCE: &str = "raw_payload_chunks_v1";
// Leave room for redb's leaf metadata within a 64 KiB buddy allocation.
const CHUNK_BYTES: usize = 60 * 1024;
const LOG_READ_BYTES: usize = 1 << 20;

#[cfg(test)]
pub(super) fn insert_chunks(
    table: &mut redb::Table<'_, (Sequence, u64), &'static [u8]>,
    sequence: Sequence,
    payload: &[u8],
) -> Result<()> {
    for (index, chunk) in (0_u64..).zip(payload.chunks(CHUNK_BYTES)) {
        table.insert((sequence, index), chunk)?;
    }
    Ok(())
}

/// One committed raw payload, held readable while this lives.
pub(super) struct RawPayload {
    source: Source,
    len: u64,
    sha256: [u8; 32],
}

enum Source {
    /// The read-only table pins its redb snapshot even after remote cleanup.
    Chunks {
        table: PayloadChunks,
        sequence: Sequence,
    },
    Log(Pin),
}

/// What a reader takes before it looks a record up: a pin on the log's tail
/// and a snapshot of the chunk table. A row seen after that is still readable.
pub(super) struct Held {
    pin: Pin,
    table: PayloadChunks,
}

impl Held {
    /// `pin` must be taken before the snapshot `table` reads.
    pub(super) fn new(pin: Pin, table: PayloadChunks) -> Self {
        Self { pin, table }
    }
}

impl RawPayload {
    /// Open `record`'s payload and verify every byte against its digest.
    pub(super) fn open(held: Held, record: &MutationRecord, maximum_bytes: u64) -> Result<Self> {
        let payload = Self::bind(held, record, maximum_bytes)?;
        payload.verify(|_| ())?;
        Ok(payload)
    }

    /// Read `record`'s payload, verified against its digest in the same
    /// pass: the digest is of exactly the bytes returned.
    pub(super) fn read(held: Held, record: &MutationRecord, maximum_bytes: u64) -> Result<Vec<u8>> {
        let payload = Self::bind(held, record, maximum_bytes)?;
        let mut bytes = Vec::new();
        bytes.try_reserve_exact(usize::try_from(payload.len)?)?;
        payload.verify(|chunk| bytes.extend_from_slice(chunk))?;
        Ok(bytes)
    }

    fn bind(held: Held, record: &MutationRecord, maximum_bytes: u64) -> Result<Self> {
        let reference = record.blob_path().context("mutation has no payload")?;
        let (len, sha256) = record.payload().context("mutation has no payload")?;
        ensure!(
            len <= maximum_bytes,
            "committed blob exceeds bounded read limit"
        );
        let Held { mut pin, table } = held;
        let source = match raw_log::parse_reference(reference, len)? {
            Some(position) => {
                pin.narrow(position)?;
                Source::Log(pin)
            }
            None => {
                ensure!(reference == RAW_REFERENCE, "invalid raw payload reference");
                Source::Chunks {
                    table,
                    sequence: record.sequence,
                }
            }
        };
        Ok(Self {
            source,
            len,
            sha256,
        })
    }

    /// Pass every byte to `visit` in order, then check the length and digest
    /// against the record.
    fn verify(&self, mut visit: impl FnMut(&[u8])) -> Result<()> {
        let mut hasher = Sha256::new();
        let mut bytes = 0;
        match &self.source {
            Source::Chunks { table, sequence } => {
                let chunks = table.range((*sequence, 0)..=(*sequence, u64::MAX))?;
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
            }
            Source::Log(pin) => {
                let mut buffer = vec![0; LOG_READ_BYTES.min(self.len as usize)];
                while bytes < self.len {
                    let count = buffer.len().min((self.len - bytes) as usize);
                    pin.read(bytes, &mut buffer[..count])?;
                    hasher.update(&buffer[..count]);
                    visit(&buffer[..count]);
                    bytes += count as u64;
                }
            }
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
        match self.source {
            Source::Chunks { .. } => self.len.div_ceil(CHUNK_BYTES as u64),
            Source::Log(_) => 0,
        }
    }

    pub(super) fn read_chunk(&self, offset: u64, maximum: usize) -> Result<Bytes> {
        match &self.source {
            Source::Chunks { table, sequence } => {
                let (index, within) = (
                    offset / CHUNK_BYTES as u64,
                    (offset % CHUNK_BYTES as u64) as usize,
                );
                let chunk = table
                    .get((*sequence, index))?
                    .context(JournalBlobIntegrityError::LengthMismatch)?;
                let count = maximum.min(chunk.value().len().saturating_sub(within));
                let bytes = chunk
                    .value()
                    .get(within..within + count)
                    .context(JournalBlobIntegrityError::LengthMismatch)?;
                Ok(Bytes::copy_from_slice(bytes))
            }
            Source::Log(pin) => {
                let count = (maximum as u64).min(self.len.saturating_sub(offset)) as usize;
                let mut bytes = vec![0; count];
                pin.read(offset, &mut bytes)?;
                Ok(Bytes::from(bytes))
            }
        }
    }
}
