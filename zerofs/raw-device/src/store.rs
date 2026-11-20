//! The raw object index, copy-on-write payload allocations and flush barrier.
mod allocation;
mod index_log;

use self::allocation::{Allocation, Allocator, Blocks};
use self::index_log::{ENTRY_CHARGE, IndexWriter};
use crate::format::{
    Block, LABEL_SLOTS, LOG_START, Label, SlotState, bank_bytes, check_device_size, data_start,
    sha256, zero_block,
};
use crate::region::Region;
use crate::{BLOCK_SIZE, Error, Geometry, Identity, MAX_KEY_LEN, MAX_OBJECT_LEN, Result};
use std::collections::{HashMap, HashSet};
use std::fmt;
use std::fs::File;
use std::io::{self, Read};
use std::ops::Range;
use std::sync::{Arc, Weak};
use std::time::{SystemTime, UNIX_EPOCH};

const SCAN_CHUNK: usize = 1 << 20;
const VERIFY_CHUNK: u64 = 1 << 20;
const MAX_PENDING_KEYS: usize = 4096;

type Index = HashMap<Arc<[u8]>, Arc<Entry>>;

#[derive(Debug, PartialEq, Eq)]
struct Entry {
    allocation: Allocation,
    info: ObjectInfo,
}

struct Retired {
    allocation: Allocation,
    len: u64,
    seq: u64,
    reader: Weak<Entry>,
}

impl From<Arc<Entry>> for Retired {
    fn from(entry: Arc<Entry>) -> Self {
        Self {
            allocation: entry.allocation,
            len: entry.info.len,
            seq: entry.info.seq,
            reader: if Arc::strong_count(&entry) > 1 {
                Arc::downgrade(&entry)
            } else {
                Weak::new()
            },
        }
    }
}

/// Metadata of one live object.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ObjectInfo {
    /// Payload length in bytes.
    pub len: u64,
    /// SHA-256 of the whole payload.
    pub sha256: [u8; 32],
    /// Sequence of the PUT, retained when the index is compacted.
    pub seq: u64,
    /// Wall-clock time of the put, in Unix nanoseconds.
    pub modified_unix_nanos: u64,
}

/// A live object located by [`RawStore::handle`]. The allocation is pinned
/// until the last clone of the handle is dropped, even after overwrite or delete.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectHandle {
    entry: Arc<Entry>,
    pub info: ObjectInfo,
}

/// Reads through handles from the same open store without borrowing it.
#[derive(Debug, Clone)]
pub struct RawReader {
    file: Arc<Region>,
    data_start: u64,
    nonce: [u8; 16],
}

impl RawReader {
    /// Return only the requested ranges after verifying the whole payload.
    pub fn read_ranges(
        &self,
        handle: &ObjectHandle,
        ranges: &[Range<u64>],
    ) -> Result<Vec<Vec<u8>>> {
        self.read(&handle.entry, ranges)
    }

    fn blocks(&self, entry: &Entry) -> Result<Blocks> {
        entry.allocation.blocks(
            &self.file,
            self.data_start,
            &self.nonce,
            entry.info.seq,
            entry.info.len,
        )
    }

    fn read(&self, entry: &Entry, ranges: &[Range<u64>]) -> Result<Vec<Vec<u8>>> {
        let len = entry.info.len;
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
        for run in self.blocks(entry)?.data_runs() {
            let mut offset = run.start;
            while offset < run.end && at < len {
                let n = VERIFY_CHUNK.min(len - at).min(run.end - offset);
                let chunk = &mut buf[..n as usize];
                self.file.read_exact_at(chunk, offset)?;
                sha2::Digest::update(&mut hasher, &*chunk);
                for (range, dest) in ranges.iter().zip(out.iter_mut()) {
                    let start = range.start.max(at);
                    let end = range.end.min(at + n);
                    if start < end {
                        dest.extend_from_slice(&chunk[(start - at) as usize..(end - at) as usize]);
                    }
                }
                at += n;
                offset += n;
            }
        }
        let digest: [u8; 32] = sha2::Digest::finalize(hasher).into();
        if digest != entry.info.sha256 {
            return Err(corrupt(
                entry.allocation.start(),
                "payload checksum mismatch",
            ));
        }
        Ok(out)
    }
}

/// A durable key/value object store over one claimed device. Dropping it does
/// not flush. PUTs are copy-on-write while either label can still reach the
/// previous value.
pub struct RawStore {
    file: Arc<Region>,
    label: Label,
    stale_slot: Option<u64>,
    seq: u64,
    index: Index,
    index_charge: usize,
    // None requests a full snapshot in the other bank. Bound pending key
    // memory even when the caller flushes after an arbitrarily long batch.
    pending: Option<HashSet<Arc<[u8]>>>,
    allocator: Allocator,
    retired: Vec<Retired>,
    pinned: Vec<Retired>,
    poisoned: bool,
}

impl fmt::Debug for RawStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RawStore")
            .field("identity", &self.label.identity)
            .field("geometry", &self.geometry())
            .field("objects", &self.index.len())
            .field("label_degraded", &self.label_degraded())
            .field("poisoned", &self.poisoned)
            .finish()
    }
}

impl RawStore {
    /// Bind a device to `identity` and return an empty store with both label
    /// slots durable.
    ///
    /// Returns [`Error::AlreadyProvisioned`] if either slot has a valid label.
    /// Both label slots and the first log block must be zero. A nonzero byte
    /// in that range returns [`Error::NotBlank`] with its offset. Provisioning
    /// checks this range before any writes. The rest of the device can contain
    /// old data.
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
        if let Some(offset) = first_nonzero(&file, LOG_START + BLOCK_SIZE)? {
            return Err(Error::NotBlank { offset });
        }

        let label = Label {
            generation: 1,
            device_size,
            identity,
            nonce: random_nonce()?,
            durable_tail: LOG_START,
            last_seq: 0,
            bank: 0,
            bank_generation: 1,
        };
        write_checkpoint(&file, &label)?;

        Ok(Self::empty(file, label, None))
    }

    fn empty(file: Region, label: Label, stale_slot: Option<u64>) -> Self {
        Self {
            file: Arc::new(file),
            label,
            stale_slot,
            seq: label.last_seq,
            index: HashMap::new(),
            index_charge: 0,
            pending: Some(HashSet::new()),
            allocator: Allocator::new(data_start(label.device_size), label.device_size),
            retired: Vec::new(),
            pinned: Vec::new(),
            poisoned: false,
        }
    }

    pub(crate) fn binding(file: &Region, expected: Identity) -> Result<(Label, Option<u64>)> {
        let device_size = file.len;
        expected.validate()?;
        check_device_size(device_size).map_err(Error::InvalidGeometry)?;
        let (label, stale_slot) = select_label(read_slots(file)?)?;
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
        Ok((label, stale_slot))
    }

    /// Reopen a provisioned device and rebuild the live index and allocator.
    /// Open never writes to the device.
    pub fn open(file: File, device_size: u64, expected: Identity) -> Result<Self> {
        Self::open_region(Region::whole(file, device_size)?, expected)
    }

    pub(crate) fn open_region(file: Region, expected: Identity) -> Result<Self> {
        let (label, stale_slot) = Self::binding(&file, expected)?;
        let mut store = Self::empty(file, label, stale_slot);
        let reader = store.reader();
        index_log::replay(&reader.file, &label, |key, entry| {
            if let Some(entry) = entry {
                store.reserve_index(key)?;
                store.insert(key, entry);
            } else {
                store.remove(key);
            }
            Ok(())
        })?;
        // Historical entries in the index log can point into space that
        // already belongs to something else. Claim only final live allocations.
        for entry in store.index.values() {
            let blocks = reader.blocks(entry)?;
            store.allocator.claim(blocks.all_runs())?;
        }
        Ok(store)
    }

    /// Stage a replacement visible to this store. Until FLUSH the last
    /// committed value remains recoverable.
    pub fn put(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        self.put_digested(key, value, sha256(value))
    }

    /// [`Self::put`] for a value whose SHA-256 the caller has already
    /// checked against these bytes. Reads verify the payload against it.
    pub fn put_digested(&mut self, key: &[u8], value: &[u8], sha256: [u8; 32]) -> Result<()> {
        self.check_healthy()?;
        check_key(key)?;
        let len = value.len() as u64;
        if len > MAX_OBJECT_LEN {
            return Err(Error::InvalidArgument("value exceeds MAX_OBJECT_LEN"));
        }
        let seq = self.next_seq()?;
        self.reserve_index(key)?;
        self.reclaim()?;
        let positions = self.allocator.allocate(Self::put_cost(len).unwrap())?;
        let allocation = Allocation::new(&positions);
        self.repair_label()?;
        let written = allocation.write(&self.file, &self.label.nonce, seq, &positions, value);
        self.poison_on_io(written)?;

        let entry = Entry {
            allocation,
            info: ObjectInfo {
                len,
                sha256,
                seq,
                modified_unix_nanos: now_unix_nanos(),
            },
        };
        self.seq = seq;
        let (key, previous) = self.insert(key, entry);
        self.mark_pending(key);
        if let Some(previous) = previous {
            self.retire(previous)?;
        }
        Ok(())
    }

    /// Stage deletion of a present key. No payload space is needed to delete.
    pub fn delete(&mut self, key: &[u8]) -> Result<bool> {
        self.check_healthy()?;
        check_key(key)?;
        if !self.index.contains_key(key) {
            return Ok(false);
        }
        self.seq = self.next_seq()?;
        let (key, entry) = self.remove(key).expect("key was present");
        self.mark_pending(key);
        self.retire(entry)?;
        Ok(true)
    }

    /// Sync the staged payload and index, then publish the checkpoint to both
    /// labels with a barrier after each. Only then can old blocks be reused.
    pub fn flush(&mut self) -> Result<()> {
        self.flush_with_checkpoint(write_checkpoint)
    }

    fn flush_with_checkpoint(
        &mut self,
        checkpoint: impl FnOnce(&Region, &Label) -> io::Result<()>,
    ) -> Result<()> {
        self.check_healthy()?;
        self.repair_label()?;
        if self.seq == self.label.last_seq {
            return self.reclaim();
        }
        let mut next = self.next_checkpoint()?;
        let index_tail = self.write_index(&next);
        next.durable_tail = self.poison_on_io(index_tail)?;
        let synced = self
            .file
            .sync_data()
            .and_then(|()| checkpoint(&self.file, &next));
        self.poison_on_io(synced)?;
        self.label = next;
        self.pending = Some(HashSet::new());
        for entry in std::mem::take(&mut self.retired) {
            self.release_or_pin(entry)?;
        }
        self.reclaim()
    }

    fn repair_label(&mut self) -> Result<()> {
        // After an interrupted checkpoint the stale slot may still name
        // extents called free by the selected generation. Repair before
        // writing either payload or index to any such extent.
        if let Some(slot) = self.stale_slot {
            let repaired = self
                .file
                .write_all_at(&self.label.encode(slot)[..], slot * BLOCK_SIZE)
                .and_then(|()| self.file.sync_data());
            self.poison_on_io(repaired)?;
            self.stale_slot = None;
        }
        Ok(())
    }

    fn next_checkpoint(&self) -> Result<Label> {
        let generation = self
            .label
            .generation
            .checked_add(1)
            .filter(|g| *g < u64::MAX)
            .ok_or_else(|| corrupt(0, "label generation exhausted"))?;
        let mut next = Label {
            generation,
            last_seq: self.seq,
            ..self.label
        };
        let remaining =
            self.label.bank_start() + bank_bytes(self.label.device_size) - self.label.durable_tail;
        if self
            .pending
            .as_ref()
            .is_none_or(|keys| keys.len() as u64 * BLOCK_SIZE > remaining)
        {
            next.bank ^= 1;
            next.bank_generation = generation;
            next.durable_tail = next.bank_start();
        }
        Ok(next)
    }

    fn write_index(&self, next: &Label) -> io::Result<u64> {
        let mut writer = IndexWriter::new(&self.file, next);
        if next.bank == self.label.bank {
            let keys = self.pending.as_ref().expect("delta needs pending keys");
            // Deletions go first so replay also fits the live index budget.
            for key in keys.iter().filter(|key| !self.index.contains_key(*key)) {
                writer.push(key, None)?;
            }
            for key in keys {
                if let Some(entry) = self.index.get(key) {
                    writer.push(key, Some(entry))?;
                }
            }
        } else {
            for (key, entry) in &self.index {
                writer.push(key, Some(entry))?;
            }
        }
        writer.finish()
    }

    fn next_seq(&self) -> Result<u64> {
        self.seq
            .checked_add(1)
            .ok_or_else(|| corrupt(0, "object sequence exhausted"))
    }

    fn reserve_index(&mut self, key: &[u8]) -> Result<()> {
        if self.index.contains_key(key) {
            return Ok(());
        }
        let size = self.label.device_size;
        let limit = (bank_bytes(size) / 2).min((size / 25_600).max(8 << 20)) as usize;
        if self.index_charge + ENTRY_CHARGE + key.len() > limit {
            return Err(Error::IndexFull { limit });
        }
        self.index.try_reserve(1).map_err(io::Error::other)?;
        Ok(())
    }

    fn insert(&mut self, key: &[u8], entry: Entry) -> (Arc<[u8]>, Option<Arc<Entry>>) {
        if let Some((stored_key, _)) = self.index.get_key_value(key) {
            let key = Arc::clone(stored_key);
            let previous = self.index.insert(Arc::clone(&key), Arc::new(entry));
            return (key, previous);
        }
        self.index_charge += ENTRY_CHARGE + key.len();
        let key: Arc<[u8]> = Arc::from(key);
        self.index.insert(Arc::clone(&key), Arc::new(entry));
        (key, None)
    }

    fn remove(&mut self, key: &[u8]) -> Option<(Arc<[u8]>, Arc<Entry>)> {
        let (key, entry) = self.index.remove_entry(key)?;
        self.index_charge -= ENTRY_CHARGE + key.len();
        Some((key, entry))
    }

    fn mark_pending(&mut self, key: Arc<[u8]>) {
        if let Some(keys) = &mut self.pending {
            keys.insert(key);
            if keys.len() > MAX_PENDING_KEYS {
                self.pending = None;
            }
        }
    }

    fn retire(&mut self, entry: Arc<Entry>) -> Result<()> {
        let entry = Retired::from(entry);
        if entry.seq <= self.label.last_seq {
            self.retired.push(entry);
            Ok(())
        } else {
            self.release_or_pin(entry)
        }
    }

    fn release_or_pin(&mut self, entry: Retired) -> Result<()> {
        if entry.reader.strong_count() == 0 {
            self.release(&entry)
        } else {
            self.pinned.push(entry);
            Ok(())
        }
    }

    fn release(&mut self, entry: &Retired) -> Result<()> {
        let blocks = entry
            .allocation
            .blocks(
                &self.file,
                data_start(self.label.device_size),
                &self.label.nonce,
                entry.seq,
                entry.len,
            )
            .inspect_err(|_| self.poisoned = true)?;
        self.allocator.release(blocks.all_runs());
        Ok(())
    }

    /// Make flushed allocations available once their last reader has left.
    pub fn reclaim(&mut self) -> Result<()> {
        self.check_healthy()?;
        let mut index = 0;
        while index < self.pinned.len() {
            if self.pinned[index].reader.strong_count() == 0 {
                let entry = self.pinned.swap_remove(index);
                self.release(&entry)?;
            } else {
                index += 1;
            }
        }
        Ok(())
    }

    /// Read and verify the value stored under `key`.
    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.check_healthy()?;
        self.index
            .get(key)
            .map(|entry| {
                self.reader()
                    .read(entry, std::slice::from_ref(&(0..entry.info.len)))
                    .map(|mut data| data.pop().expect("one range"))
            })
            .transpose()
    }

    pub fn stat(&self, key: &[u8]) -> Option<ObjectInfo> {
        self.index.get(key).map(|entry| entry.info)
    }

    pub fn objects(&self) -> impl Iterator<Item = (&[u8], ObjectInfo)> {
        self.index
            .iter()
            .map(|(key, entry)| (key.as_ref(), entry.info))
    }

    pub fn handle(&self, key: &[u8]) -> Result<Option<ObjectHandle>> {
        self.check_healthy()?;
        Ok(self.index.get(key).map(|entry| ObjectHandle {
            entry: Arc::clone(entry),
            info: entry.info,
        }))
    }

    pub fn reader(&self) -> RawReader {
        RawReader {
            file: Arc::clone(&self.file),
            data_start: data_start(self.label.device_size),
            nonce: self.label.nonce,
        }
    }

    /// Exact allocation for a PUT, including its block map allowance.
    pub const fn put_cost(value_len: u64) -> Option<u64> {
        allocation::cost(value_len)
    }

    /// Reads and verifies every live object.
    pub fn scrub(&self) -> Result<usize> {
        self.check_healthy()?;
        let reader = self.reader();
        for entry in self.index.values() {
            reader.read(entry, &[])?;
        }
        Ok(self.index.len())
    }

    pub fn contains_key(&self, key: &[u8]) -> bool {
        self.index.contains_key(key)
    }

    pub fn keys(&self) -> impl Iterator<Item = &[u8]> {
        self.index.keys().map(Arc::as_ref)
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

    pub fn label_degraded(&self) -> bool {
        self.stale_slot.is_some()
    }

    pub fn geometry(&self) -> Geometry {
        Geometry {
            device_size: self.label.device_size,
            data_start: data_start(self.label.device_size),
            durable_tail: self.label.durable_tail,
            free: self.allocator.free(),
            generation: self.label.generation,
        }
    }

    fn check_healthy(&self) -> Result<()> {
        if self.poisoned {
            Err(Error::Poisoned)
        } else {
            Ok(())
        }
    }

    fn poison_on_io<T>(&mut self, result: io::Result<T>) -> Result<T> {
        result.map_err(|err| {
            self.poisoned = true;
            Error::Io(err)
        })
    }
}

fn write_checkpoint(file: &Region, label: &Label) -> io::Result<()> {
    for slot in 0..LABEL_SLOTS {
        file.write_all_at(&label.encode(slot)[..], slot * BLOCK_SIZE)?;
        file.sync_data()?;
    }
    Ok(())
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

/// Choose the durable checkpoint and identify the slot requiring repair. See
/// the slot rules in `format.rs`.
fn select_label(slots: [SlotState; 2]) -> Result<(Label, Option<u64>)> {
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
                Ok((a, None))
            } else if a.generation.checked_sub(1) == Some(b.generation) {
                // A flush wrote slot 0 and stopped before slot 1.
                Ok((a, Some(1)))
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
        (SlotState::Valid(a), _) => Ok((a, Some(1))),
        (_, SlotState::Valid(b)) => Ok((b, Some(0))),
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

fn first_nonzero(file: &Region, end: u64) -> Result<Option<u64>> {
    let mut buf = vec![0u8; end.min(SCAN_CHUNK as u64) as usize];
    let mut offset = 0;
    while offset < end {
        let len = (end - offset).min(buf.len() as u64) as usize;
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
