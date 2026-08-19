use crate::writeback::model::MutationRecord;
use bytes::Bytes;
use futures::{StreamExt, stream, stream::BoxStream};
use object_store::PutPayload;
use sha2::{Digest, Sha256};
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

const STREAM_BUFFER_BYTES: usize = 1024 * 1024;

#[derive(Clone)]
enum PayloadSource {
    Memory(PutPayload),
    StagedFile(Arc<StagedPayload>),
}

struct StagedPayload {
    file: Mutex<File>,
}

/// Immutable payload source paired with the SHA-256 digest computed from it.
///
/// Memory sources retain the original `Bytes` chunks without coalescing.
/// Multipart SSD sources retain a private immutable file and are streamed into
/// the local journal with bounded memory.
#[derive(Clone)]
pub(crate) struct VerifiedPayload {
    source: PayloadSource,
    byte_len: u64,
    sha256: [u8; 32],
}

impl VerifiedPayload {
    pub(crate) fn new(bytes: Bytes) -> Self {
        Self::from_put_payload(bytes.into())
    }

    pub(crate) fn from_put_payload(payload: PutPayload) -> Self {
        let mut hasher = Sha256::new();
        for chunk in &payload {
            hasher.update(chunk);
        }
        Self {
            byte_len: payload.content_length() as u64,
            sha256: hasher.finalize().into(),
            source: PayloadSource::Memory(payload),
        }
    }

    pub(crate) fn from_staged_file(path: PathBuf, byte_len: u64) -> std::io::Result<Self> {
        let mut file = open_staged_file(&path)?;
        let (actual_len, sha256) = hash_reader(&mut file)?;
        if actual_len != byte_len {
            return Err(std::io::Error::other(format!(
                "staged payload length mismatch: expected {byte_len}, got {actual_len}"
            )));
        }
        Ok(Self {
            source: PayloadSource::StagedFile(Arc::new(StagedPayload {
                file: Mutex::new(file),
            })),
            byte_len,
            sha256,
        })
    }

    pub(crate) fn byte_len(&self) -> u64 {
        self.byte_len
    }

    pub(crate) fn sha256(&self) -> [u8; 32] {
        self.sha256
    }

    pub(crate) fn memory_range(&self, range: Range<usize>) -> Option<Bytes> {
        let PayloadSource::Memory(payload) = &self.source else {
            return None;
        };
        if range.is_empty() {
            return Some(Bytes::new());
        }
        let mut offset = 0usize;
        let mut selected = Vec::new();
        for chunk in payload {
            let chunk_end = offset + chunk.len();
            if chunk_end > range.start && offset < range.end {
                let start = range.start.saturating_sub(offset);
                let end = (range.end - offset).min(chunk.len());
                selected.push(chunk.slice(start..end));
            }
            offset = chunk_end;
            if offset >= range.end {
                break;
            }
        }
        match selected.len() {
            0 => Some(Bytes::new()),
            1 => selected.pop(),
            _ => {
                let mut bytes = Vec::with_capacity(range.len());
                for chunk in selected {
                    bytes.extend_from_slice(&chunk);
                }
                Some(Bytes::from(bytes))
            }
        }
    }

    pub(crate) fn staged_range(
        &self,
        range: Range<u64>,
    ) -> Option<BoxStream<'static, std::io::Result<Bytes>>> {
        match &self.source {
            PayloadSource::Memory(_) => None,
            PayloadSource::StagedFile(staged) => {
                let staged = Arc::clone(staged);
                Some(
                    stream::try_unfold((staged, range.start, range.end), |state| async move {
                        let (staged, offset, end) = state;
                        if offset >= end {
                            return Ok(None);
                        }
                        let wanted = (end - offset).min(STREAM_BUFFER_BYTES as u64) as usize;
                        let reader = Arc::clone(&staged);
                        let bytes = tokio::task::spawn_blocking(move || {
                            reader.read_chunk_at(offset, wanted)
                        })
                        .await
                        .map_err(|error| {
                            std::io::Error::other(format!(
                                "staged payload range task failed: {error}"
                            ))
                        })??;
                        if bytes.is_empty() {
                            return Err(std::io::Error::new(
                                std::io::ErrorKind::UnexpectedEof,
                                "staged payload ended before the requested range",
                            ));
                        }
                        let next = offset.checked_add(bytes.len() as u64).ok_or_else(|| {
                            std::io::Error::other("staged payload range overflow")
                        })?;
                        Ok(Some((bytes, (staged, next, end))))
                    })
                    .boxed(),
                )
            }
        }
    }

    pub(crate) fn write_to(&self, output: &mut File) -> std::io::Result<()> {
        match &self.source {
            PayloadSource::Memory(payload) => {
                for chunk in payload {
                    output.write_all(chunk)?;
                }
                Ok(())
            }
            PayloadSource::StagedFile(staged) => {
                let mut input = staged
                    .file
                    .lock()
                    .map_err(|_| std::io::Error::other("staged payload file lock poisoned"))?;
                input.seek(SeekFrom::Start(0))?;
                let mut hasher = Sha256::new();
                let mut total = 0u64;
                let mut buffer = vec![0u8; STREAM_BUFFER_BYTES];
                loop {
                    let read = input.read(&mut buffer)?;
                    if read == 0 {
                        break;
                    }
                    total = total
                        .checked_add(read as u64)
                        .ok_or_else(|| std::io::Error::other("staged payload length overflow"))?;
                    hasher.update(&buffer[..read]);
                    output.write_all(&buffer[..read])?;
                }
                let sha256: [u8; 32] = hasher.finalize().into();
                if total != self.byte_len || sha256 != self.sha256 {
                    return Err(std::io::Error::other(
                        "staged payload changed after multipart completion",
                    ));
                }
                Ok(())
            }
        }
    }

    pub(crate) fn matches_record(&self, record: &MutationRecord) -> bool {
        record.payload() == Some((self.byte_len(), self.sha256))
    }
}

impl StagedPayload {
    fn read_chunk_at(&self, offset: u64, wanted: usize) -> std::io::Result<Bytes> {
        let mut file = self
            .file
            .lock()
            .map_err(|_| std::io::Error::other("staged payload file lock poisoned"))?;
        file.seek(SeekFrom::Start(offset))?;
        let mut bytes = vec![0; wanted];
        let mut read = 0usize;
        while read < wanted {
            let count = file.read(&mut bytes[read..])?;
            if count == 0 {
                break;
            }
            read += count;
        }
        bytes.truncate(read);
        Ok(Bytes::from(bytes))
    }
}

fn open_staged_file(path: &Path) -> std::io::Result<File> {
    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_NOFOLLOW);
    }
    options.open(path)
}

fn hash_reader(reader: &mut File) -> std::io::Result<(u64, [u8; 32])> {
    let mut hasher = Sha256::new();
    let mut total = 0u64;
    let mut buffer = vec![0u8; STREAM_BUFFER_BYTES];
    loop {
        let read = reader.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        total = total
            .checked_add(read as u64)
            .ok_or_else(|| std::io::Error::other("staged payload length overflow"))?;
        hasher.update(&buffer[..read]);
    }
    Ok((total, hasher.finalize().into()))
}

#[cfg(test)]
mod tests {
    use super::VerifiedPayload;
    use bytes::Bytes;
    use futures::StreamExt;
    use object_store::PutPayload;
    use std::fs::OpenOptions;

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
        assert_eq!(payload.memory_range(0..3).unwrap(), b"abc"[..]);
    }

    #[test]
    fn multi_chunk_payload_hashes_without_coalescing() {
        let payload = VerifiedPayload::from_put_payload(
            [Bytes::from_static(b"ab"), Bytes::from_static(b"c")]
                .into_iter()
                .collect::<PutPayload>(),
        );
        assert_eq!(payload.byte_len(), 3);
        assert_eq!(payload.memory_range(1..3).unwrap(), b"bc"[..]);
        assert_eq!(
            payload.sha256(),
            VerifiedPayload::new(Bytes::from_static(b"abc")).sha256()
        );
    }

    #[test]
    fn staged_payload_is_revalidated_while_streaming_to_journal() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("payload.staged");
        std::fs::write(&path, b"original").unwrap();
        let payload = VerifiedPayload::from_staged_file(path.clone(), 8).unwrap();
        std::fs::write(&path, b"tampered").unwrap();
        let output_path = root.path().join("journal");
        let mut output = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(output_path)
            .unwrap();
        let error = payload.write_to(&mut output).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("changed after multipart completion")
        );
    }

    #[tokio::test]
    async fn staged_range_read_survives_cleanup_unlink_after_handoff() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("payload.staged");
        std::fs::write(&path, b"0123456789").unwrap();
        let payload = VerifiedPayload::from_staged_file(path.clone(), 10).unwrap();
        let mut stream = payload.staged_range(2..8).unwrap();
        std::fs::remove_file(path).unwrap();

        let mut bytes = Vec::new();
        while let Some(chunk) = stream.next().await {
            bytes.extend_from_slice(&chunk.unwrap());
        }
        assert_eq!(bytes, b"234567");
    }
}
