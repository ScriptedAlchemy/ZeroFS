//! The store: label handling, log replay, appends, and the flush barrier.

use crate::region::Region;
use std::collections::HashMap;
use std::fmt;
use std::fs::File;
use std::io::{self, Read};
use std::ops::Range;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::format::{
    BLOCK, Block, Kind, LABEL_SLOTS, LOG_START, Label, RecordHeader, SlotState, blocks_for,
    check_device_size, sha256, zero_block,
};
use crate::{BLOCK_SIZE, Error, Geometry, Identity, MAX_KEY_LEN, MAX_OBJECT_LEN, Result};

/// Read size for the provisioning blank scan.
const SCAN_CHUNK: usize = 1 << 20;
/// Appends up to this size go to the device as one write.
const COALESCE_LIMIT: u64 = 256 << 10;
/// Read size for verified range reads, which hash the whole payload.
const VERIFY_CHUNK: u64 = 1 << 20;

#[derive(Debug, Clone, Copy)]
struct Entry {
    value_offset: u64,
    info: ObjectInfo,
}

/// What the log records about one live object.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ObjectInfo {
    /// Payload length in bytes.
    pub len: u64,
    /// SHA-256 of the whole payload.
    pub sha256: [u8; 32],
    /// Log sequence number of the put record. Unique within one provisioning
    /// and strictly larger for every later append.
    pub seq: u64,
    /// Wall-clock time of the put, in Unix nanoseconds.
    pub modified_unix_nanos: u64,
}

/// A live object located by [`RawStore::handle`], readable through a
/// [`RawReader`] without holding the store. Log records are never rewritten
/// while the store is open, so the handle stays valid after later overwrites
/// or deletes of its key.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ObjectHandle {
    value_offset: u64,
    pub info: ObjectInfo,
}

/// Shares the store's device for checksum-verified reads that do not borrow
/// the store. Only hand it handles from the store that created it.
#[derive(Debug, Clone)]
pub struct RawReader {
    file: Arc<Region>,
}

impl RawReader {
    /// Read `ranges` of the object, verifying the whole payload's SHA-256.
    ///
    /// The full payload is read in bounded chunks to check the digest, but
    /// only the requested bytes are retained, so memory stays near the sum of
    /// the ranges. Each range must lie within the object. Nothing is returned
    /// unless the digest matches.
    pub fn read_ranges(
        &self,
        handle: &ObjectHandle,
        ranges: &[Range<u64>],
    ) -> Result<Vec<Vec<u8>>> {
        let len = handle.info.len;
        if ranges.iter().any(|r| r.start > r.end || r.end > len) {
            return Err(Error::InvalidArgument("range outside the object"));
        }
        let mut out: Vec<Vec<u8>> = ranges
            .iter()
            .map(|r| Vec::with_capacity((r.end - r.start) as usize))
            .collect();
        let mut hasher = <sha2::Sha256 as sha2::Digest>::new();
        let mut buf = vec![0u8; VERIFY_CHUNK.min(len) as usize];
        let mut at = 0u64;
        while at < len {
            let n = VERIFY_CHUNK.min(len - at);
            let chunk = &mut buf[..n as usize];
            self.file.read_exact_at(chunk, handle.value_offset + at)?;
            sha2::Digest::update(&mut hasher, &*chunk);
            for (range, dest) in ranges.iter().zip(out.iter_mut()) {
                let start = range.start.max(at);
                let end = range.end.min(at + n);
                if start < end {
                    dest.extend_from_slice(&chunk[(start - at) as usize..(end - at) as usize]);
                }
            }
            at += n;
        }
        let digest: [u8; 32] = sha2::Digest::finalize(hasher).into();
        if digest != handle.info.sha256 {
            return Err(corrupt(handle.value_offset, "payload checksum mismatch"));
        }
        Ok(out)
    }
}

/// A durable key/value object store over one claimed device.
///
/// `get` takes `&self` and may run concurrently from several threads. Appends
/// and `flush` take `&mut self`. Dropping the store does not flush.
pub struct RawStore {
    file: Arc<Region>,
    /// The last durable checkpoint.
    label: Label,
    /// One label slot is damaged, blank, or behind; the next flush rewrites
    /// both.
    label_degraded: bool,
    tail: u64,
    next_seq: u64,
    index: HashMap<Vec<u8>, Entry>,
    poisoned: bool,
}

impl fmt::Debug for RawStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RawStore")
            .field("identity", &self.label.identity)
            .field("geometry", &self.geometry())
            .field("objects", &self.index.len())
            .field("label_degraded", &self.label_degraded)
            .field("poisoned", &self.poisoned)
            .finish()
    }
}

impl RawStore {
    /// Bind a blank device to `identity` and return an empty store. The label
    /// is durable in both slots when this returns.
    ///
    /// Reads the entire device first and refuses it if any byte is nonzero:
    /// [`Error::AlreadyProvisioned`] when a ZeroFS label is present, otherwise
    /// [`Error::NotBlank`] with the first nonzero offset. Nothing is written
    /// unless the whole device is zero. The scan costs one full sequential
    /// read of the device.
    pub fn provision(file: File, device_size: u64, identity: Identity) -> Result<Self> {
        Self::provision_region(Region::whole(file, device_size)?, identity)
    }

    pub(crate) fn provision_region(file: Region, identity: Identity) -> Result<Self> {
        let device_size = file.len;
        identity.validate()?;
        check_device_size(device_size).map_err(Error::InvalidGeometry)?;
        for state in read_slots(&file)? {
            if let SlotState::Valid(label) = state {
                return Err(Error::AlreadyProvisioned {
                    found: label.identity,
                });
            }
        }
        require_blank(&file)?;

        let label = Label {
            generation: 1,
            device_size,
            identity,
            nonce: random_nonce()?,
            durable_tail: LOG_START,
            record_count: 0,
        };
        write_checkpoint(&file, &label)?;

        Ok(Self {
            file: Arc::new(file),
            label,
            label_degraded: false,
            tail: LOG_START,
            next_seq: 1,
            index: HashMap::new(),
            poisoned: false,
        })
    }

    /// Reopen a provisioned device and replay its durable log.
    ///
    /// The label must record exactly `expected` and `device_size`. Anything
    /// appended after the last durable checkpoint is ignored and will be
    /// overwritten by later appends. Open never writes to the device.
    pub fn open(file: File, device_size: u64, expected: Identity) -> Result<Self> {
        Self::open_region(Region::whole(file, device_size)?, expected)
    }

    pub(crate) fn open_region(file: Region, expected: Identity) -> Result<Self> {
        let device_size = file.len;
        expected.validate()?;
        check_device_size(device_size).map_err(Error::InvalidGeometry)?;
        let (label, label_degraded) = select_label(read_slots(&file)?)?;
        if label.identity != expected {
            return Err(Error::IdentityMismatch {
                expected,
                found: label.identity,
            });
        }
        if label.device_size != device_size {
            return Err(Error::DeviceSizeMismatch {
                recorded: label.device_size,
                supplied: device_size,
            });
        }

        let mut index = HashMap::new();
        let mut offset = LOG_START;
        let mut seq: u64 = 1;
        let mut block = zero_block();
        while offset < label.durable_tail {
            if label.durable_tail - offset < BLOCK_SIZE {
                return Err(corrupt(offset, "durable tail splits a record header"));
            }
            read_block(&file, offset, &mut block)?;
            let header = RecordHeader::decode(&block, &label.nonce, seq)
                .map_err(|reason| Error::Corrupt { offset, reason })?;
            let end = offset
                .checked_add(header.span())
                .filter(|end| *end <= label.durable_tail)
                .ok_or_else(|| corrupt(offset, "record extends past the durable tail"))?;
            apply(&mut index, offset, header);
            offset = end;
            seq += 1;
        }
        if seq - 1 != label.record_count {
            return Err(Error::Corrupt {
                offset: label.durable_tail,
                reason: format!(
                    "label records {} records, log holds {}",
                    label.record_count,
                    seq - 1
                ),
            });
        }

        Ok(Self {
            file: Arc::new(file),
            tail: label.durable_tail,
            label,
            label_degraded,
            next_seq: seq,
            index,
            poisoned: false,
        })
    }

    /// Append `value` under `key`, replacing any earlier value. Visible to
    /// `get` at once; durable only after the next successful `flush`.
    pub fn put(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        self.check_writable()?;
        check_key(key)?;
        if value.len() as u64 > MAX_OBJECT_LEN {
            return Err(Error::InvalidArgument("value exceeds MAX_OBJECT_LEN"));
        }
        let header = RecordHeader {
            seq: self.next_seq,
            kind: Kind::Put,
            key: key.to_vec(),
            value_len: value.len() as u64,
            value_sha256: sha256(value),
            modified_unix_nanos: now_unix_nanos(),
        };
        let span = self.reserve(&header)?;
        let at = self.tail;
        let encoded = header.encode(&self.label.nonce);
        let written = self.write_record(at, &encoded, value, span);
        self.poison_on_err(written)?;

        self.tail += span;
        self.next_seq += 1;
        apply(&mut self.index, at, header);
        Ok(())
    }

    /// Append a delete of `key`. Returns `false`, writing nothing, if the key
    /// is absent. Durable only after the next successful `flush`.
    pub fn delete(&mut self, key: &[u8]) -> Result<bool> {
        self.check_writable()?;
        check_key(key)?;
        if !self.index.contains_key(key) {
            return Ok(false);
        }
        let header = RecordHeader {
            seq: self.next_seq,
            kind: Kind::Delete,
            key: key.to_vec(),
            value_len: 0,
            value_sha256: sha256(&[]),
            modified_unix_nanos: now_unix_nanos(),
        };
        let span = self.reserve(&header)?;
        let at = self.tail;
        let written = self
            .file
            .write_all_at(&header.encode(&self.label.nonce)[..], at);
        self.poison_on_err(written)?;

        self.tail += span;
        self.next_seq += 1;
        apply(&mut self.index, at, header);
        Ok(true)
    }

    /// Make every append so far durable, and repair a degraded label.
    ///
    /// Syncs the log, then writes the next checkpoint to slot 0, syncs, writes
    /// it to slot 1, and syncs again. Returns only after both slots are
    /// durable, so damage to either one later cannot roll the store back past
    /// this flush. Any I/O failure poisons the store; reopen it to recover the
    /// last durable state.
    pub fn flush(&mut self) -> Result<()> {
        self.check_writable()?;
        if self.tail == self.label.durable_tail && !self.label_degraded {
            return Ok(());
        }
        let next = self.next_checkpoint()?;
        let synced = self
            .file
            .sync_data()
            .and_then(|()| write_checkpoint(&self.file, &next));
        self.poison_on_err(synced)?;
        self.label = next;
        self.label_degraded = false;
        Ok(())
    }

    /// Read and verify the value stored under `key`.
    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        if self.poisoned {
            return Err(Error::Poisoned);
        }
        let Some(entry) = self.index.get(key) else {
            return Ok(None);
        };
        self.read_verified(entry).map(Some)
    }

    /// Metadata of the live object under `key`, without reading its payload.
    pub fn stat(&self, key: &[u8]) -> Option<ObjectInfo> {
        self.index.get(key).map(|entry| entry.info)
    }

    /// Every live object and its metadata, in no particular order.
    pub fn objects(&self) -> impl Iterator<Item = (&[u8], ObjectInfo)> {
        self.index
            .iter()
            .map(|(key, entry)| (key.as_slice(), entry.info))
    }

    /// Locate the live object under `key` for reading through a [`RawReader`].
    pub fn handle(&self, key: &[u8]) -> Result<Option<ObjectHandle>> {
        if self.poisoned {
            return Err(Error::Poisoned);
        }
        Ok(self.index.get(key).map(|entry| ObjectHandle {
            value_offset: entry.value_offset,
            info: entry.info,
        }))
    }

    /// A reader sharing this store's device. See [`RawReader`].
    pub fn reader(&self) -> RawReader {
        RawReader {
            file: Arc::clone(&self.file),
        }
    }

    /// Exact log bytes a put of a `value_len`-byte value appends: one header
    /// block plus the value rounded up to whole blocks. `None` when that does
    /// not fit in a `u64`; such a value is refused long before by
    /// [`crate::MAX_OBJECT_LEN`], but the arithmetic never wraps.
    pub const fn put_cost(value_len: u64) -> Option<u64> {
        match value_len.div_ceil(BLOCK_SIZE).checked_mul(BLOCK_SIZE) {
            Some(payload) => payload.checked_add(BLOCK_SIZE),
            None => None,
        }
    }

    /// Exact log bytes a delete appends. Deletes never reclaim space.
    pub const fn delete_cost() -> u64 {
        BLOCK_SIZE
    }

    /// Read and verify every live object. Returns how many were checked.
    pub fn scrub(&self) -> Result<usize> {
        if self.poisoned {
            return Err(Error::Poisoned);
        }
        for entry in self.index.values() {
            self.read_verified(entry)?;
        }
        Ok(self.index.len())
    }

    pub fn contains_key(&self, key: &[u8]) -> bool {
        self.index.contains_key(key)
    }

    /// Live keys, in no particular order.
    pub fn keys(&self) -> impl Iterator<Item = &[u8]> {
        self.index.keys().map(Vec::as_slice)
    }

    pub fn len(&self) -> usize {
        self.index.len()
    }

    pub fn is_empty(&self) -> bool {
        self.index.is_empty()
    }

    pub fn identity(&self) -> Identity {
        self.label.identity
    }

    pub fn is_poisoned(&self) -> bool {
        self.poisoned
    }

    /// One label slot was blank, damaged, or one generation behind at open.
    /// Data is intact, but a second fault could lose the label. The next
    /// `flush` rewrites both slots and clears this.
    pub fn label_degraded(&self) -> bool {
        self.label_degraded
    }

    pub fn geometry(&self) -> Geometry {
        Geometry {
            device_size: self.label.device_size,
            log_start: LOG_START,
            tail: self.tail,
            durable_tail: self.label.durable_tail,
            free: self.label.device_size - self.tail,
            generation: self.label.generation,
        }
    }

    fn next_checkpoint(&self) -> Result<Label> {
        // u64::MAX is never a valid generation, so the next one must stay
        // below it.
        let generation = self
            .label
            .generation
            .checked_add(1)
            .filter(|g| *g < u64::MAX)
            .ok_or_else(|| corrupt(0, "label generation exhausted"))?;
        Ok(Label {
            generation,
            durable_tail: self.tail,
            record_count: self.next_seq - 1,
            ..self.label
        })
    }

    fn check_writable(&self) -> Result<()> {
        if self.poisoned {
            Err(Error::Poisoned)
        } else {
            Ok(())
        }
    }

    fn reserve(&self, header: &RecordHeader) -> Result<u64> {
        let needed = header.span();
        let available = self.label.device_size - self.tail;
        if needed > available {
            return Err(Error::NoSpace { needed, available });
        }
        Ok(needed)
    }

    fn write_record(&self, at: u64, header: &Block, value: &[u8], span: u64) -> io::Result<()> {
        if span <= COALESCE_LIMIT {
            let mut buf = vec![0u8; span as usize];
            buf[..BLOCK].copy_from_slice(&header[..]);
            buf[BLOCK..BLOCK + value.len()].copy_from_slice(value);
            return self.file.write_all_at(&buf, at);
        }
        self.file.write_all_at(&header[..], at)?;
        let value_at = at + BLOCK_SIZE;
        self.file.write_all_at(value, value_at)?;
        let padding = blocks_for(value.len() as u64) - value.len() as u64;
        if padding > 0 {
            let zeros = vec![0u8; padding as usize];
            self.file
                .write_all_at(&zeros, value_at + value.len() as u64)?;
        }
        Ok(())
    }

    fn read_verified(&self, entry: &Entry) -> Result<Vec<u8>> {
        let mut value = vec![0u8; entry.info.len as usize];
        self.file.read_exact_at(&mut value, entry.value_offset)?;
        if sha256(&value) != entry.info.sha256 {
            return Err(corrupt(entry.value_offset, "payload checksum mismatch"));
        }
        Ok(value)
    }

    fn poison_on_err<T>(&mut self, result: io::Result<T>) -> Result<T> {
        result.map_err(|err| {
            self.poisoned = true;
            Error::Io(err)
        })
    }
}

/// Write `label` to slot 0, sync, then slot 1, sync. The fixed order is what
/// lets open recognise an interrupted checkpoint: slot 1 is never ahead.
fn write_checkpoint(file: &Region, label: &Label) -> io::Result<()> {
    for slot in 0..LABEL_SLOTS {
        file.write_all_at(&label.encode(slot)[..], slot * BLOCK_SIZE)?;
        file.sync_data()?;
    }
    Ok(())
}

fn apply(index: &mut HashMap<Vec<u8>, Entry>, at: u64, header: RecordHeader) {
    match header.kind {
        Kind::Put => {
            index.insert(
                header.key,
                Entry {
                    value_offset: at + BLOCK_SIZE,
                    info: ObjectInfo {
                        len: header.value_len,
                        sha256: header.value_sha256,
                        seq: header.seq,
                        modified_unix_nanos: header.modified_unix_nanos,
                    },
                },
            );
        }
        Kind::Delete => {
            index.remove(&header.key);
        }
    }
}

fn check_key(key: &[u8]) -> Result<()> {
    if key.is_empty() {
        return Err(Error::InvalidArgument("key is empty"));
    }
    if key.len() > MAX_KEY_LEN {
        return Err(Error::InvalidArgument("key exceeds MAX_KEY_LEN"));
    }
    Ok(())
}

fn corrupt(offset: u64, reason: &str) -> Error {
    Error::Corrupt {
        offset,
        reason: reason.into(),
    }
}

fn read_block(file: &Region, offset: u64, block: &mut Block) -> Result<()> {
    file.read_exact_at(&mut block[..], offset)?;
    Ok(())
}

fn read_slots(file: &Region) -> Result<[SlotState; 2]> {
    let mut block = zero_block();
    read_block(file, 0, &mut block)?;
    let first = Label::decode(&block, 0);
    read_block(file, BLOCK_SIZE, &mut block)?;
    let second = Label::decode(&block, 1);
    Ok([first, second])
}

/// Choose the durable checkpoint and report whether the slots disagree. See
/// the slot rules in `format.rs`.
fn select_label(slots: [SlotState; 2]) -> Result<(Label, bool)> {
    for state in &slots {
        if let SlotState::Unsupported(version) = state {
            return Err(Error::UnsupportedVersion(*version));
        }
    }
    let [first, second] = slots;
    match (first, second) {
        (SlotState::Valid(a), SlotState::Valid(b)) => {
            let same_provisioning =
                a.nonce == b.nonce && a.identity == b.identity && a.device_size == b.device_size;
            if !same_provisioning {
                return Err(corrupt(0, "label slots describe different provisionings"));
            }
            if a == b {
                Ok((a, false))
            } else if a.generation.checked_sub(1) == Some(b.generation) {
                // A flush wrote slot 0 and stopped before slot 1.
                Ok((a, true))
            } else {
                Err(Error::Corrupt {
                    offset: 0,
                    reason: format!(
                        "label slots hold generations {} and {}, which no checkpoint sequence produces",
                        a.generation, b.generation
                    ),
                })
            }
        }
        (SlotState::Valid(a), _) => Ok((a, true)),
        (_, SlotState::Valid(b)) => Ok((b, true)),
        (SlotState::Absent, SlotState::Absent) => Err(Error::NotProvisioned),
        (first, second) => Err(Error::Corrupt {
            offset: 0,
            reason: format!(
                "no usable label: slot 0 {}, slot 1 {}",
                describe(&first),
                describe(&second)
            ),
        }),
    }
}

fn describe(state: &SlotState) -> String {
    match state {
        SlotState::Absent => "blank".into(),
        SlotState::Invalid(reason) => reason.clone(),
        SlotState::Unsupported(version) => format!("version {version}"),
        SlotState::Valid(label) => format!("generation {}", label.generation),
    }
}

/// Offset of the first nonzero byte anywhere on the device.
fn first_nonzero(file: &Region, device_size: u64) -> Result<Option<u64>> {
    let mut buf = vec![0u8; SCAN_CHUNK];
    let mut offset = 0;
    while offset < device_size {
        let len = (device_size - offset).min(SCAN_CHUNK as u64) as usize;
        file.read_exact_at(&mut buf[..len], offset)?;
        if let Some(at) = buf[..len].iter().position(|&b| b != 0) {
            return Ok(Some(offset + at as u64));
        }
        offset += len as u64;
    }
    Ok(None)
}

fn now_unix_nanos() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |elapsed| {
            u64::try_from(elapsed.as_nanos()).unwrap_or(u64::MAX)
        })
}

fn random_nonce() -> Result<[u8; 16]> {
    let mut nonce = [0u8; 16];
    File::open("/dev/urandom")?.read_exact(&mut nonce)?;
    Ok(nonce)
}

pub(crate) fn require_blank(file: &Region) -> Result<()> {
    if let Some(offset) = first_nonzero(file, file.len)? {
        return Err(Error::NotBlank { offset });
    }
    Ok(())
}

#[cfg(test)]
mod tests;
