//! The raw journal's payload log.
//!
//! redb holds the records. Payload bytes go here once, in the reserve above
//! the redb file: a ring addressed by position. A position counts the bytes
//! appended since the log was created and never wraps, and byte `p` lives at
//! `p % capacity`. A record names its payload as
//! `raw_payload_log_v1#{position}+{len}`.
//!
//! A batch is staged past the head in whole blocks and is not synced here,
//! so its bytes can reach the device while the batch before it commits. The
//! redb commit that records the batch flushes the device once for both, and
//! only then does the head move over it. Until that commit is durable no row
//! names the batch, so after a power cut its bytes are free space.
//!
//! Space is reclaimed from the tail. The tail follows the oldest row the
//! journal still holds and never passes an extent a reader has pinned.

use anyhow::{Context, Result, ensure};
use std::collections::BTreeMap;
use std::io::{self, Write};
use std::sync::{Arc, Mutex, MutexGuard};
use zerofs_raw_device::journal_region::TransactionRegion;

pub(super) const REFERENCE: &str = "raw_payload_log_v1";
const BLOCK: u64 = zerofs_raw_device::BLOCK_SIZE;
const WRITE_BYTES: usize = 4 << 20;

/// Where the redb file ends and the log begins in a region of `capacity`.
pub(super) fn file_ceiling(capacity: u64) -> u64 {
    capacity / 2 / BLOCK * BLOCK
}

pub(super) struct PayloadLog {
    region: Arc<TransactionRegion>,
    capacity: u64,
    state: Mutex<State>,
    appender: Mutex<()>,
}

struct State {
    tail: u64,
    /// The end of the last recorded batch.
    head: u64,
    /// The end of the last staged batch, at or past the head.
    staged: u64,
    /// Readers per pinned position.
    pins: BTreeMap<u64, usize>,
}

impl State {
    fn floor(&self) -> u64 {
        self.pins
            .first_key_value()
            .map_or(self.tail, |(&pinned, _)| pinned.min(self.tail))
    }
}

/// One batch written past the head and not yet recorded.
pub(super) struct Staged {
    /// Where each payload starts, in the order given.
    pub(super) positions: Vec<u64>,
    /// The batch's bytes. The end is the head once the batch is recorded.
    pub(super) extent: std::ops::Range<u64>,
}

impl PayloadLog {
    /// `head` is the end of the last recorded batch and `first_held` the
    /// position of the oldest payload a row still names.
    pub(super) fn open(
        region: Arc<TransactionRegion>,
        head: u64,
        first_held: Option<u64>,
    ) -> Result<Self> {
        let capacity = region.reserve_len();
        let tail = first_held.unwrap_or(head);
        ensure!(
            capacity >= BLOCK
                && capacity.is_multiple_of(BLOCK)
                && head.is_multiple_of(BLOCK)
                && tail <= head
                && head - tail <= capacity,
            "invalid raw payload log geometry"
        );
        Ok(Self {
            region,
            capacity,
            state: Mutex::new(State {
                tail,
                head,
                staged: head,
                pins: BTreeMap::new(),
            }),
            appender: Mutex::new(()),
        })
    }

    fn state(&self) -> MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(|error| error.into_inner())
    }

    /// Bytes between the oldest held extent and the last staged batch.
    pub(super) fn held_bytes(&self) -> u64 {
        let state = self.state();
        state.staged - state.floor()
    }

    pub(super) fn available_bytes(&self) -> u64 {
        self.capacity - self.held_bytes()
    }

    /// Write `payloads` back to back after the last staged batch, padded to
    /// a whole block. Batches must be staged in the order they are recorded.
    pub(super) fn stage<'a>(
        &self,
        payloads: impl IntoIterator<Item = &'a crate::writeback::payload::VerifiedPayload>,
    ) -> Result<Staged> {
        let _appender = self
            .appender
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let payloads = payloads.into_iter().collect::<Vec<_>>();
        let bytes = payloads
            .iter()
            .try_fold(0_u64, |total, payload| {
                total.checked_add(payload.byte_len())
            })
            .context("raw payload batch size overflow")?;
        let (start, floor) = {
            let state = self.state();
            (state.staged, state.floor())
        };
        let end = start
            .checked_add(bytes)
            .and_then(|end| end.checked_next_multiple_of(BLOCK))
            .context("raw payload log position overflow")?;
        if end - floor > self.capacity {
            return Err(
                io::Error::new(io::ErrorKind::StorageFull, "raw payload log is full").into(),
            );
        }
        let mut writer = Writer {
            log: self,
            position: start,
            buffer: Vec::with_capacity(WRITE_BYTES.min((end - start) as usize)),
        };
        let mut positions = Vec::with_capacity(payloads.len());
        for payload in payloads {
            positions.push(writer.position + writer.buffer.len() as u64);
            payload
                .write_to(&mut writer)
                .context("failed to write payload to the raw log")?;
        }
        let written = writer.position + writer.buffer.len() as u64;
        ensure!(
            written == start + bytes,
            "raw payload batch length mismatch"
        );
        writer.buffer.resize((end - writer.position) as usize, 0);
        writer.flush().context("failed to write the raw log tail")?;
        self.state().staged = end;
        Ok(Staged {
            positions,
            extent: start..end,
        })
    }

    /// The batch ending at `end` is recorded in a commit.
    pub(super) fn recorded(&self, end: u64) {
        let mut state = self.state();
        state.head = state.head.max(end);
        state.staged = state.staged.max(end);
    }

    /// A staged batch starting at `start` will never be recorded, and neither
    /// will any staged after it. Their bytes are free again.
    pub(super) fn abandon(&self, start: u64) {
        let _appender = self
            .appender
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let mut state = self.state();
        state.staged = state.staged.min(start).max(state.head);
    }

    /// No row names bytes below `first_held`. `None` means no row holds any.
    pub(super) fn retire_to(&self, first_held: Option<u64>) {
        let mut state = self.state();
        state.tail = first_held
            .unwrap_or(state.head)
            .clamp(state.tail, state.head);
    }

    /// Keep the tail where it is until the returned pin is dropped.
    pub(super) fn pin(self: &Arc<Self>) -> Pin {
        let mut state = self.state();
        let position = state.floor();
        *state.pins.entry(position).or_default() += 1;
        Pin {
            log: Arc::clone(self),
            position,
        }
    }

    fn unpin(&self, position: u64) {
        let mut state = self.state();
        if let Some(count) = state.pins.get_mut(&position) {
            *count -= 1;
            if *count == 0 {
                state.pins.remove(&position);
            }
        }
    }

    fn write_at(&self, position: u64, bytes: &[u8]) -> io::Result<()> {
        let offset = position % self.capacity;
        let first = bytes.len().min((self.capacity - offset) as usize);
        self.region.write_reserve(offset, &bytes[..first])?;
        if first < bytes.len() {
            self.region.write_reserve(0, &bytes[first..])?;
        }
        Ok(())
    }

    fn read_at(&self, position: u64, out: &mut [u8]) -> io::Result<()> {
        let offset = position % self.capacity;
        let first = out.len().min((self.capacity - offset) as usize);
        let (head, wrapped) = out.split_at_mut(first);
        self.region.read_reserve(offset, head)?;
        if !wrapped.is_empty() {
            self.region.read_reserve(0, wrapped)?;
        }
        Ok(())
    }
}

struct Writer<'a> {
    log: &'a PayloadLog,
    /// Position of the first buffered byte.
    position: u64,
    buffer: Vec<u8>,
}

impl Write for Writer<'_> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let count = bytes.len().min(WRITE_BYTES - self.buffer.len());
        self.buffer.extend_from_slice(&bytes[..count]);
        if self.buffer.len() == WRITE_BYTES {
            self.flush()?;
        }
        Ok(count)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.log.write_at(self.position, &self.buffer)?;
        self.position += self.buffer.len() as u64;
        self.buffer.clear();
        Ok(())
    }
}

/// Holds log space against reclaim: every extent at or above `position` stays
/// readable while this lives.
pub(super) struct Pin {
    log: Arc<PayloadLog>,
    position: u64,
}

impl Pin {
    /// Move the pin up to an extent it already covers.
    pub(super) fn narrow(&mut self, position: u64) -> Result<()> {
        ensure!(position >= self.position, "raw payload extent was retired");
        let mut state = self.log.state();
        *state.pins.entry(position).or_default() += 1;
        drop(state);
        self.log.unpin(self.position);
        self.position = position;
        Ok(())
    }

    /// Read from the pinned extent, `offset` bytes in.
    pub(super) fn read(&self, offset: u64, out: &mut [u8]) -> io::Result<()> {
        self.log.read_at(self.position + offset, out)
    }
}

impl Drop for Pin {
    fn drop(&mut self) {
        self.log.unpin(self.position);
    }
}

pub(super) fn format_reference(position: u64, len: u64) -> String {
    format!("{REFERENCE}#{position}+{len}")
}

/// The position a log reference names, or `None` for any other reference.
pub(super) fn parse_reference(reference: &str, len: u64) -> Result<Option<u64>> {
    let Some(extent) = reference
        .strip_prefix(REFERENCE)
        .and_then(|rest| rest.strip_prefix('#'))
    else {
        return Ok(None);
    };
    let (position, recorded_len) = extent
        .split_once('+')
        .context("malformed raw payload log reference")?;
    ensure!(
        recorded_len.parse::<u64>().ok() == Some(len),
        "raw payload log reference disagrees with its record"
    );
    Ok(Some(
        position
            .parse()
            .context("malformed raw payload log position")?,
    ))
}
