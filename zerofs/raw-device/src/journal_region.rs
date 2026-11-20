//! Bounded, resizable file semantics on a dedicated raw Journal or Catalog region.
//!
//! The first two blocks hold checksummed length checkpoints. The rest of the
//! region is a direct byte mapping for a transactional database such as redb.
//! The v2 checkpoints may carry a checksummed high-water offset for backing
//! verified blank at provisioning. Beyond it the initial zero bytes remain
//! untouched. Older v2 writers discard the optional field when checkpointing.
//! The database owns transaction ordering and page reclamation. In particular
//! its durable metadata must stop referencing truncated bytes before it calls
//! `set_len`. Ordinary writes are durable only once `sync_data` succeeds.

use crate::{BLOCK_SIZE, Error, Identity, MIN_DEVICE_SIZE, Result, Role, region::Region};
use sha2::{Digest, Sha256};
use std::{
    any::Any,
    fmt,
    fs::File,
    io,
    sync::{Arc, Mutex},
};

const DATA_START: u64 = 2 * BLOCK_SIZE;
const JOURNAL_MAGIC: [u8; 8] = *b"ZWBREG02";
const CATALOG_MAGIC: [u8; 8] = *b"ZCTREG02";
const ZERO_FRONTIER_MAGIC: [u8; 8] = *b"ZTRZERO1";
const DIGEST_START: usize = BLOCK_SIZE as usize - 32;
type Block = [u8; BLOCK_SIZE as usize];

pub struct TransactionRegion {
    region: Region,
    binding: Binding,
    resize_ceiling: u64,
    state: Mutex<State>,
    /// Holds a flush open between its two waits.
    #[cfg(test)]
    flush_hold: Mutex<Option<Arc<std::sync::Barrier>>>,
    // Drop the region descriptor before releasing exclusive device ownership.
    _claim: Option<Arc<dyn Any + Send + Sync>>,
}

impl fmt::Debug for TransactionRegion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TransactionRegion")
            .field("binding", &self.binding)
            .finish_non_exhaustive()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Binding {
    identity: Identity,
    device_len: u64,
    start: u64,
    region_len: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Length {
    generation: u64,
    len: u64,
    // A provision-time blank scan proves the suffix at and beyond this
    // block-aligned high-water offset has never been exposed to a caller.
    zero_from: Option<u64>,
}

#[derive(Debug)]
struct State {
    length: Length,
    stale_slot: Option<u64>,
    poisoned: bool,
}

impl TransactionRegion {
    /// Initialize a blank subregion on an exclusively claimed descriptor.
    /// Existing RawStore regions require an explicit migration.
    pub fn provision(
        file: File,
        device_len: u64,
        start: u64,
        region_len: u64,
        identity: Identity,
    ) -> Result<Self> {
        let (region, binding) = Self::bind(file, device_len, start, region_len, identity)?;
        crate::store::require_blank(&region)?;
        let length = Length {
            generation: 1,
            len: 0,
            zero_from: Some(0),
        };
        checkpoint(&region, binding, length)?;
        Ok(Self::new(region, binding, length, None))
    }

    /// Open the labeled subregion. This performs no writes.
    pub fn open(
        file: File,
        device_len: u64,
        start: u64,
        region_len: u64,
        identity: Identity,
    ) -> Result<Self> {
        let (region, binding) = Self::bind(file, device_len, start, region_len, identity)?;
        let mut block = [0; BLOCK_SIZE as usize];
        let mut slots = [None; 2];
        for (slot, length) in slots.iter_mut().enumerate() {
            region.read_exact_at(&mut block, slot as u64 * BLOCK_SIZE)?;
            *length = decode(&block, binding)?;
        }
        let (mut length, stale_slot) = match slots {
            [Some(first), Some(second)]
                if first.generation == second.generation && first.len == second.len =>
            {
                let stale = (first.zero_from != second.zero_from).then_some(BLOCK_SIZE);
                (first, stale)
            }
            [Some(first), Some(second)]
                if first.generation.checked_sub(1) == Some(second.generation) =>
            {
                (first, Some(BLOCK_SIZE))
            }
            [Some(first), None] => (first, Some(BLOCK_SIZE)),
            [None, Some(second)] => (second, Some(0)),
            _ => {
                return Err(corrupt(
                    "no consistent raw transactional region length checkpoint",
                ));
            }
        };
        // V2 writers that predate the extension discard it on resize and
        // repair. Only use the proof when both slots carry it and agree on
        // the exact high-water transition. Keep unknown history unknown.
        length.zero_from = match slots {
            [Some(first), Some(second)] => match (first.zero_from, second.zero_from) {
                (Some(new), Some(old))
                    if new == old.max(first.len.next_multiple_of(BLOCK_SIZE)) =>
                {
                    Some(new)
                }
                _ => None,
            },
            _ => None,
        };
        Ok(Self::new(region, binding, length, stale_slot))
    }

    fn bind(
        file: File,
        device_len: u64,
        start: u64,
        region_len: u64,
        identity: Identity,
    ) -> Result<(Region, Binding)> {
        identity.validate()?;
        if !matches!(identity.role, Role::Journal | Role::Catalog) {
            return Err(Error::InvalidArgument(
                "expected a Journal or Catalog region identity",
            ));
        }
        if region_len < MIN_DEVICE_SIZE {
            return Err(Error::InvalidGeometry(
                "transactional region is too small".into(),
            ));
        }
        // Validate the claimed descriptor's reported boundary before trusting
        // the caller's subregion geometry.
        Region::whole(file.try_clone()?, device_len)?;
        let region = Region::new(file, device_len, start, region_len)?;
        Ok((
            region,
            Binding {
                identity,
                device_len,
                start,
                region_len,
            },
        ))
    }

    fn new(region: Region, binding: Binding, length: Length, stale_slot: Option<u64>) -> Self {
        Self {
            region,
            binding,
            resize_ceiling: binding.region_len - DATA_START,
            state: Mutex::new(State {
                length,
                stale_slot,
                poisoned: false,
            }),
            #[cfg(test)]
            flush_hold: Mutex::new(None),
            _claim: None,
        }
    }

    /// Keep the exclusive claim alive until after this region's descriptor closes.
    pub fn retain_claim<C: Any + Send + Sync>(mut self, claim: Arc<C>) -> Self {
        self._claim = Some(claim);
        self
    }

    pub fn identity(&self) -> Identity {
        self.binding.identity
    }

    pub fn capacity(&self) -> u64 {
        self.binding.region_len - DATA_START
    }

    /// Restrict future growth before handing the file to the database.
    /// Refuse to start if the current file already occupies the reserve.
    pub fn with_resize_ceiling(mut self, ceiling: u64) -> io::Result<Self> {
        if ceiling > self.resize_ceiling {
            return Err(io::Error::other(
                "raw transactional resize ceiling exceeds the region",
            ));
        }
        if self.len()? > ceiling {
            return Err(io::Error::new(
                io::ErrorKind::StorageFull,
                "existing raw transactional file exceeds the requested resize ceiling",
            ));
        }
        self.resize_ceiling = ceiling;
        Ok(self)
    }

    pub fn resize_ceiling(&self) -> u64 {
        self.resize_ceiling
    }

    pub fn len(&self) -> io::Result<u64> {
        Ok(self.state()?.length.len)
    }

    pub fn is_empty(&self) -> io::Result<bool> {
        Ok(self.len()? == 0)
    }

    pub fn read(&self, offset: u64, out: &mut [u8]) -> io::Result<()> {
        // Positioned reads need no exclusion, so the lock covers only the
        // bounds check and a read never waits for a flush.
        let start = data_offset(offset, out.len(), self.state()?.length.len)?;
        self.region.read_exact_at(out, start)
    }

    pub fn write(&self, offset: u64, bytes: &[u8]) -> io::Result<()> {
        let mut state = self.state()?;
        let start = data_offset(offset, bytes.len(), state.length.len)?;
        self.repair(&mut state)?;
        let result = self.region.write_all_at(bytes, start);
        poison_on_error(&mut state, result)
    }

    /// Restore file-style zero-on-growth after previously exposed physical
    /// bytes are used again. Persist any required zeros before publishing the
    /// length. A checkpointed, verified blank suffix needs no writes.
    /// Shrink keeps the truncated bytes intact and fences prior database writes
    /// before publishing the shorter length.
    pub fn set_len(&self, len: u64) -> io::Result<()> {
        if len > self.resize_ceiling {
            return Err(io::Error::new(
                io::ErrorKind::StorageFull,
                "raw transactional region is full",
            ));
        }
        let mut state = self.state()?;
        self.repair(&mut state)?;
        if len == state.length.len {
            return Ok(());
        }
        let next =
            Length {
                generation: state.length.generation.checked_add(1).ok_or_else(|| {
                    io::Error::other("raw transactional region generation exhausted")
                })?,
                len,
                zero_from: state
                    .length
                    .zero_from
                    .map(|offset| offset.max(len.next_multiple_of(BLOCK_SIZE))),
            };
        let result = (|| {
            let zero_until = len.min(state.length.zero_from.unwrap_or(len));
            if zero_until > state.length.len {
                let zeros = [0; 64 * 1024];
                let mut offset = state.length.len;
                while offset < zero_until {
                    let count = (zero_until - offset).min(zeros.len() as u64) as usize;
                    self.region
                        .write_all_at(&zeros[..count], DATA_START + offset)?;
                    offset += count as u64;
                }
            }
            self.region.sync_data()?;
            checkpoint(&self.region, self.binding, next)
        })();
        poison_on_error(&mut state, result)?;
        state.length = next;
        Ok(())
    }

    /// Make every completed write durable. The device flush runs without the
    /// state lock, so `len` and `read` never wait for it. A write that races
    /// the flush may or may not be covered, as for a file.
    pub fn sync_data(&self) -> io::Result<()> {
        self.repair(&mut *self.state()?)?;
        #[cfg(test)]
        if let Some(hold) = self.flush_hold.lock().unwrap().clone() {
            hold.wait();
            hold.wait();
        }
        let result = self.region.sync_data();
        if result.is_err() {
            // Record the failure even if a panicking thread poisoned the lock.
            self.state
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .poisoned = true;
        }
        result
    }

    fn state(&self) -> io::Result<std::sync::MutexGuard<'_, State>> {
        let state = self
            .state
            .lock()
            .map_err(|_| io::Error::other("raw transactional region lock poisoned"))?;
        if state.poisoned {
            return Err(Error::Poisoned.into());
        }
        Ok(state)
    }

    fn repair(&self, state: &mut State) -> io::Result<()> {
        if let Some(offset) = state.stale_slot {
            let bytes = encode(self.binding, state.length);
            let result = self
                .region
                .write_all_at(&bytes, offset)
                .and_then(|()| self.region.sync_data());
            poison_on_error(state, result)?;
            state.stale_slot = None;
        }
        Ok(())
    }
}

fn poison_on_error(state: &mut State, result: io::Result<()>) -> io::Result<()> {
    state.poisoned |= result.is_err();
    result
}

fn data_offset(offset: u64, count: usize, len: u64) -> io::Result<u64> {
    if offset.checked_add(count as u64).is_none_or(|end| end > len) {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "I/O outside raw transactional region file",
        ));
    }
    Ok(DATA_START + offset)
}

fn checkpoint(region: &Region, binding: Binding, length: Length) -> io::Result<()> {
    let bytes = encode(binding, length);
    for offset in [0, BLOCK_SIZE] {
        region.write_all_at(&bytes, offset)?;
        region.sync_data()?;
    }
    Ok(())
}

fn encode(binding: Binding, length: Length) -> Block {
    let mut block = [0; BLOCK_SIZE as usize];
    let magic = match binding.identity.role {
        Role::Journal => JOURNAL_MAGIC,
        Role::Catalog => CATALOG_MAGIC,
        _ => unreachable!("validated transactional role"),
    };
    block[..8].copy_from_slice(&magic);
    block[8..24].copy_from_slice(&binding.identity.pool_uuid);
    block[24..40].copy_from_slice(&binding.identity.member_uuid);
    for (index, value) in [
        binding.device_len,
        binding.start,
        binding.region_len,
        length.generation,
        length.len,
    ]
    .into_iter()
    .enumerate()
    {
        let offset = 40 + index * 8;
        block[offset..offset + 8].copy_from_slice(&value.to_le_bytes());
    }
    if let Some(offset) = length.zero_from {
        block[80..88].copy_from_slice(&ZERO_FRONTIER_MAGIC);
        block[88..96].copy_from_slice(&offset.to_le_bytes());
    }
    let digest = Sha256::digest(&block[..DIGEST_START]);
    block[DIGEST_START..].copy_from_slice(&digest);
    block
}

fn decode(block: &Block, binding: Binding) -> Result<Option<Length>> {
    let role = match block[..8].try_into().unwrap() {
        JOURNAL_MAGIC => Role::Journal,
        CATALOG_MAGIC => Role::Catalog,
        _ => return Ok(None),
    };
    if block[DIGEST_START..] != Sha256::digest(&block[..DIGEST_START])[..] {
        return Ok(None);
    }
    let word = |offset| u64::from_le_bytes(block[offset..offset + 8].try_into().unwrap());
    let identity = Identity {
        pool_uuid: block[8..24].try_into().unwrap(),
        member_uuid: block[24..40].try_into().unwrap(),
        role,
    };
    if identity != binding.identity {
        return Err(Error::IdentityMismatch {
            expected: binding.identity,
            found: identity,
        });
    }
    if word(40) != binding.device_len || word(48) != binding.start || word(56) != binding.region_len
    {
        return Err(corrupt(
            "raw transactional region checkpoint geometry mismatch",
        ));
    }
    let length = Length {
        generation: word(64),
        len: word(72),
        zero_from: (block[80..88] == ZERO_FRONTIER_MAGIC).then(|| word(88)),
    };
    let capacity = binding.region_len - DATA_START;
    if length.generation == 0
        || length.len > capacity
        || length.zero_from.is_some_and(|offset| {
            !offset.is_multiple_of(BLOCK_SIZE) || offset < length.len || offset > capacity
        })
    {
        return Err(corrupt(
            "invalid raw transactional region checkpoint length",
        ));
    }
    Ok(Some(length))
}

fn corrupt(reason: &str) -> Error {
    Error::Corrupt {
        offset: 0,
        reason: reason.into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::OpenOptions;
    use std::os::unix::fs::FileExt;
    use std::sync::atomic::{AtomicU64, Ordering};

    const START: u64 = MIN_DEVICE_SIZE;
    const DEVICE_LEN: u64 = 3 * MIN_DEVICE_SIZE;
    const ID: Identity = Identity {
        pool_uuid: [1; 16],
        member_uuid: [2; 16],
        role: Role::Journal,
    };

    struct Media {
        path: std::path::PathBuf,
        file: File,
    }

    impl Media {
        fn new() -> Self {
            static NEXT: AtomicU64 = AtomicU64::new(0);
            let path = std::env::temp_dir().join(format!(
                "zerofs-journal-region-{}-{}",
                std::process::id(),
                NEXT.fetch_add(1, Ordering::Relaxed),
            ));
            let file = OpenOptions::new()
                .read(true)
                .write(true)
                .create_new(true)
                .open(&path)
                .unwrap();
            file.set_len(DEVICE_LEN).unwrap();
            Self { path, file }
        }

        fn reopen(&self) -> TransactionRegion {
            TransactionRegion::open(
                self.file.try_clone().unwrap(),
                DEVICE_LEN,
                START,
                MIN_DEVICE_SIZE,
                ID,
            )
            .unwrap()
        }

        fn provision(&self) -> TransactionRegion {
            TransactionRegion::provision(
                self.file.try_clone().unwrap(),
                DEVICE_LEN,
                START,
                MIN_DEVICE_SIZE,
                ID,
            )
            .unwrap()
        }
    }

    impl Drop for Media {
        fn drop(&mut self) {
            std::fs::remove_file(&self.path).unwrap();
        }
    }

    #[test]
    fn journal_and_catalog_hold_the_claim_independently() {
        let media = Media::new();
        let claim = Arc::new(());
        let weak = Arc::downgrade(&claim);
        let journal = media.provision().retain_claim(Arc::clone(&claim));
        let catalog_id = Identity {
            role: Role::Catalog,
            ..ID
        };
        let catalog = TransactionRegion::provision(
            media.file.try_clone().unwrap(),
            DEVICE_LEN,
            2 * MIN_DEVICE_SIZE,
            MIN_DEVICE_SIZE,
            catalog_id,
        )
        .unwrap()
        .retain_claim(claim);
        assert!(matches!(
            TransactionRegion::open(
                media.file.try_clone().unwrap(),
                DEVICE_LEN,
                START,
                MIN_DEVICE_SIZE,
                catalog_id,
            ),
            Err(Error::IdentityMismatch { .. })
        ));
        drop(journal);
        assert!(weak.upgrade().is_some());
        drop(catalog);
        assert!(weak.upgrade().is_none());
    }

    #[test]
    fn length_and_reads_do_not_wait_for_a_flush() {
        let journal = Arc::new(Media::new().provision());
        journal.set_len(BLOCK_SIZE).unwrap();
        journal.write(0, &[0x5a]).unwrap();
        let hold = Arc::new(std::sync::Barrier::new(2));
        *journal.flush_hold.lock().unwrap() = Some(Arc::clone(&hold));
        let flushing = std::thread::spawn({
            let journal = Arc::clone(&journal);
            move || journal.sync_data()
        });
        hold.wait();

        let (done, observed) = std::sync::mpsc::channel();
        let reader = std::thread::spawn({
            let journal = Arc::clone(&journal);
            move || {
                let mut byte = [0];
                journal.read(0, &mut byte).unwrap();
                done.send((journal.len().unwrap(), byte)).unwrap();
            }
        });
        let seen = observed.recv_timeout(std::time::Duration::from_secs(10));
        hold.wait();
        flushing.join().unwrap().unwrap();
        reader.join().unwrap();
        assert_eq!(seen, Ok((BLOCK_SIZE, [0x5a])));
    }

    #[test]
    fn reuses_retired_physical_bytes_and_preserves_adjacent_regions() {
        let media = Media::new();
        media.file.write_all_at(&[0xba], START - 1).unwrap();
        media
            .file
            .write_all_at(&[0xbe], START + MIN_DEVICE_SIZE)
            .unwrap();

        let journal = media.provision();
        let len = journal.capacity() - BLOCK_SIZE;
        let journal = journal.with_resize_ceiling(len).unwrap();
        journal.set_len(len).unwrap();
        assert_eq!(
            journal.set_len(len + 1).unwrap_err().kind(),
            io::ErrorKind::StorageFull
        );
        journal.write(len - 2, &[0xa1, 0xa2]).unwrap();
        journal.sync_data().unwrap();
        drop(journal);

        let error = media.reopen().with_resize_ceiling(len - 1).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::StorageFull);
        let reopen_with_reserve = || media.reopen().with_resize_ceiling(len).unwrap();
        let journal = reopen_with_reserve();
        let mut data = [0; 2];
        journal.read(len - 2, &mut data).unwrap();
        assert_eq!(data, [0xa1, 0xa2]);

        // The caller's durable retirement allows it to truncate this tail.
        journal.set_len(len - 2).unwrap();
        drop(journal);
        let journal = reopen_with_reserve();
        assert!(journal.read(len - 2, &mut data).is_err());
        journal.set_len(len).unwrap();
        drop(journal);

        let journal = reopen_with_reserve();
        journal.read(len - 2, &mut data).unwrap();
        assert_eq!(data, [0, 0]);
        journal.write(len - 2, &[0xf1, 0xf2]).unwrap();
        journal.sync_data().unwrap();
        drop(journal);
        let journal = reopen_with_reserve();
        journal.read(len - 2, &mut data).unwrap();
        assert_eq!(data, [0xf1, 0xf2]);
        assert_eq!(
            journal.set_len(len + 1).unwrap_err().kind(),
            io::ErrorKind::StorageFull
        );
        let mut reserve = [0; BLOCK_SIZE as usize];
        media
            .file
            .read_exact_at(&mut reserve, START + DATA_START + len)
            .unwrap();
        assert_eq!(reserve, [0; BLOCK_SIZE as usize]);
        media.file.read_exact_at(&mut data[..1], START - 1).unwrap();
        media
            .file
            .read_exact_at(&mut data[1..], START + MIN_DEVICE_SIZE)
            .unwrap();
        assert_eq!(data, [0xba, 0xbe]);
    }

    #[test]
    fn recovers_torn_and_interrupted_length_updates_without_destroying_the_good_slot() {
        let media = Media::new();
        let journal = media.provision();
        journal.set_len(2 * BLOCK_SIZE).unwrap();
        journal.write(BLOCK_SIZE, &[0x42]).unwrap();
        journal.sync_data().unwrap();

        let binding = journal.binding;
        let length = journal.state().unwrap().length;
        // A torn first slot represents a cut before the new shrink checkpoint.
        media.file.write_all_at(&[0; 4], START).unwrap();
        media.file.sync_data().unwrap();
        drop(journal);

        let journal = media.reopen();
        assert_eq!(journal.len().unwrap(), 2 * BLOCK_SIZE);
        let mut data = [0];
        journal.read(BLOCK_SIZE, &mut data).unwrap();
        assert_eq!(data, [0x42]);
        journal.sync_data().unwrap();
        drop(journal);

        // A cut after the first shorter label is durable must expose that
        // shorter length. Even interrupted repair leaves the first slot valid.
        let shorter = Length {
            generation: length.generation + 1,
            len: BLOCK_SIZE,
            zero_from: length.zero_from,
        };
        media
            .file
            .write_all_at(&encode(binding, shorter), START)
            .unwrap();
        media.file.sync_data().unwrap();
        let journal = media.reopen();
        assert_eq!(journal.state().unwrap().stale_slot, Some(BLOCK_SIZE));
        drop(journal);
        media
            .file
            .write_all_at(&[0; 4], START + BLOCK_SIZE)
            .unwrap();
        media.file.sync_data().unwrap();
        let journal = media.reopen();
        assert_eq!(journal.len().unwrap(), BLOCK_SIZE);
        journal.set_len(2 * BLOCK_SIZE).unwrap();
        journal.read(BLOCK_SIZE, &mut data).unwrap();
        assert_eq!(data, [0]);
    }
    #[test]
    fn first_growth_keeps_blank_suffix_sparse_and_advances_the_frontier() {
        use std::os::unix::fs::MetadataExt;

        let media = Media::new();
        let journal = media.provision();
        let blocks = media.file.metadata().unwrap().blocks();
        let first_len = MIN_DEVICE_SIZE / 2;
        journal.set_len(first_len).unwrap();
        assert_eq!(media.file.metadata().unwrap().blocks(), blocks);
        let mut byte = [1];
        journal.read(first_len - 1, &mut byte).unwrap();
        assert_eq!(byte, [0]);
        journal.write(first_len - 1, &[0xa5]).unwrap();
        journal.set_len(0).unwrap();
        drop(journal);

        let journal = media.reopen();
        let blocks = media.file.metadata().unwrap().blocks();
        assert_eq!(journal.state().unwrap().length.zero_from, Some(first_len));
        journal.set_len(2 * first_len - DATA_START).unwrap();
        assert!(media.file.metadata().unwrap().blocks() < blocks + first_len / 512);
        journal.read(first_len - 1, &mut byte).unwrap();
        assert_eq!(byte, [0]);
    }

    #[test]
    fn partial_block_exposure_advances_the_frontier_to_a_whole_block() {
        let media = Media::new();
        let journal = media.provision();
        journal.set_len(1).unwrap();
        assert_eq!(journal.state().unwrap().length.zero_from, Some(BLOCK_SIZE));
        journal.write(0, &[0xa5]).unwrap();
        journal.sync_data().unwrap();
        drop(journal);

        let journal = media.reopen();
        assert_eq!(journal.state().unwrap().length.zero_from, Some(BLOCK_SIZE));
        journal.set_len(BLOCK_SIZE + 1).unwrap();
        let mut bytes = [1; 2];
        journal.read(0, &mut bytes).unwrap();
        assert_eq!(bytes, [0xa5, 0]);
        assert_eq!(
            journal.state().unwrap().length.zero_from,
            Some(2 * BLOCK_SIZE)
        );
    }

    #[test]
    fn interrupted_growth_exposes_only_the_durable_checkpoint_and_remembers_the_frontier() {
        let media = Media::new();
        let journal = media.provision();
        let binding = journal.binding;
        let next = Length {
            generation: 2,
            len: 2 * BLOCK_SIZE,
            zero_from: Some(2 * BLOCK_SIZE),
        };
        let mut checkpoint = encode(binding, next);
        checkpoint[DIGEST_START] ^= 1;
        drop(journal);
        media.file.write_all_at(&checkpoint, START).unwrap();
        media.file.sync_data().unwrap();
        let journal = media.reopen();
        assert_eq!(journal.len().unwrap(), 0);
        assert_eq!(journal.state().unwrap().length.zero_from, None);
        drop(journal);

        media
            .file
            .write_all_at(&encode(binding, next), START)
            .unwrap();
        media.file.sync_data().unwrap();
        let journal = media.reopen();
        assert_eq!(journal.state().unwrap().length, next);
        assert_eq!(journal.state().unwrap().stale_slot, Some(BLOCK_SIZE));
        let mut byte = [1];
        journal.read(2 * BLOCK_SIZE - 1, &mut byte).unwrap();
        assert_eq!(byte, [0]);

        journal.write(2 * BLOCK_SIZE - 1, &[0xa5]).unwrap();
        journal.sync_data().unwrap();
        drop(journal);
        let journal = media.reopen();
        assert_eq!(journal.state().unwrap().length, next);
        journal.set_len(BLOCK_SIZE).unwrap();
        journal.set_len(2 * BLOCK_SIZE).unwrap();
        journal.read(2 * BLOCK_SIZE - 1, &mut byte).unwrap();
        assert_eq!(byte, [0]);
    }

    #[test]
    fn legacy_or_downgraded_checkpoints_never_infer_blank_history() {
        let media = Media::new();
        let journal = media.provision();
        let binding = journal.binding;
        journal.set_len(BLOCK_SIZE).unwrap();
        journal.write(0, &[0xa5]).unwrap();
        journal.sync_data().unwrap();

        // The legacy encoder used the same sealed v2 block and wrote zeros
        // in the extension bytes. Its reader used only the fields up to 80.
        let current = journal.state().unwrap().length;
        let legacy_current = Length {
            zero_from: None,
            ..current
        };
        assert_eq!(
            encode(binding, current)[..80],
            encode(binding, legacy_current)[..80]
        );
        drop(journal);

        // A downgrade repairing only one copy produces a mixed same-length
        // pair. Both old and new readers must retain the acknowledged data.
        media
            .file
            .write_all_at(&encode(binding, legacy_current), START)
            .unwrap();
        let journal = media.reopen();
        let mut byte = [0];
        journal.read(0, &mut byte).unwrap();
        assert_eq!(byte, [0xa5]);
        assert_eq!(journal.state().unwrap().length.zero_from, None);
        drop(journal);

        // An old writer has grown, written past the tracked frontier, then
        // shrunk to zero. The old encoder does not preserve the extension.
        media
            .file
            .write_all_at(&[0xab], START + DATA_START + BLOCK_SIZE)
            .unwrap();
        let legacy_empty = Length {
            generation: current.generation + 2,
            len: 0,
            zero_from: None,
        };
        for offset in [START, START + BLOCK_SIZE] {
            media
                .file
                .write_all_at(&encode(binding, legacy_empty), offset)
                .unwrap();
        }
        media.file.sync_data().unwrap();
        let journal = media.reopen();
        journal.set_len(2 * BLOCK_SIZE).unwrap();
        journal.read(BLOCK_SIZE, &mut byte).unwrap();
        assert_eq!(byte, [0]);
        assert_eq!(journal.state().unwrap().length.zero_from, None);
        drop(journal);
        assert_eq!(media.reopen().state().unwrap().length.zero_from, None);
    }
}
