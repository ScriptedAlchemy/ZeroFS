//! # Batch container blobs
//!
//! A publication batch packs every payload in the batch into ONE container
//! file instead of one file per record. The drain then pays one large
//! sequential write plus one fsync per batch rather than N pipelined
//! small-write fsyncs, which is what makes the post-ACK durability tail
//! bandwidth-bound instead of fsync-latency-bound.
//!
//! ## Layout
//!
//! A container is named for the contiguous sequence range it covers and is
//! sharded by the same `(sequence >> 8) & 0xff` rule the per-record layout
//! used, so a run of consecutive batches keeps sharing one directory and one
//! directory fsync:
//!
//! ```text
//! blobs/{(first >> 8) & 0xff:02x}/{first:016x}-{last:016x}.blobs
//! ```
//!
//! Encoding the range in the name means recovery and pruning can decide a
//! container's fate from its path alone -- no side table, no refcount.
//!
//! ## Blob references
//!
//! `MutationRecord::blob_path` stays a `String`, and gains an optional slice
//! suffix. Both forms are accepted forever, so journals written by earlier
//! binaries replay unchanged (see [`BlobRef::parse`]):
//!
//! * legacy, whole file:  `blobs/ab/{uuid}.blob`
//! * container slice:     `blobs/ab/{first}-{last}.blobs#{offset}+{len}`
//!
//! `#` cannot appear in a generated path, so the split is unambiguous. Only
//! publication mints new references; every reader goes through `BlobRef`, so
//! the two forms cost one parse and no branching anywhere else.
//!
//! ## Ordering and the durability contract
//!
//! A record is ACKed durable only after its bytes AND the metadata naming
//! them are fsynced. One batch therefore runs exactly three fsync-class
//! operations, whatever its record count:
//!
//! 1. write the container at its final path and `fsync` it (payload bytes
//!    durable);
//! 2. `fsync` the shard directory (the name is durable);
//! 3. insert the records and advance `LOCAL_SEQ_KEY` in one
//!    `Durability::Immediate` transaction.
//!
//! The batch is atomic: any failure before step 3 unlinks the container and
//! leaves `LOCAL_SEQ_KEY` untouched, so the whole batch fails together and the
//! journal stays replayable.
//!
//! There is deliberately no staging rename and no durable pending-blob
//! intent. Both exist to let recovery tell a finished container from an
//! interrupted one, and the container's name already answers that: a batch
//! commits atomically at step 3, so a container is committed exactly when
//! `last <= LOCAL_SEQ`. `remove_uncommitted_containers` unlinks everything
//! above the watermark on open -- including a container torn mid-write, which
//! no record references. Dropping the intent removes a whole
//! `Durability::Immediate` commit from the batch's critical path.
//!
//! `PENDING_BLOBS` survives for journals written before containers: those
//! recorded a per-record intent, and `recover_local_artifacts` still drains
//! any surviving rows before scanning by name.
//!
//! ## Reclamation
//!
//! A container holds one contiguous sequence range, so "every member is
//! remote-committed" is exactly `last <= remote watermark` -- a watermark
//! comparison, not a refcount. [`Journal::remove_remote_prefix`] parses `last`
//! out of the container name and unlinks only fully drained containers.
//!
//! Transient overhead: a container straddling the remote watermark keeps its
//! already-drained members on disk until its final member drains. That is
//! bounded by one container, whose size the journaler caps at
//! `MAX_LOCAL_PUBLISH_BATCH_PAYLOAD_BYTES`. Because the remote watermark
//! advances in order, at most one container straddles it at a time, so the
//! SSD holds at most that many bytes beyond what `dirty_ssd_reserved_bytes`
//! accounts for.

use crate::writeback::model::{
    FenceClass, JournalIdentity, MutationKind, MutationRecord, Sequence, classify_mutation_fence,
};
use crate::writeback::payload::VerifiedPayload;
use anyhow::{Context, Result, bail};
use fs4::fs_std::FileExt;
use redb::{Database, Durability, ReadableDatabase, ReadableTable, TableDefinition};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, Write};
#[cfg(unix)]
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
use std::path::{Component, Path, PathBuf};
#[cfg(test)]
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Condvar, Mutex};
use std::time::{Duration, Instant};
use uuid::Uuid;

const META: TableDefinition<&str, &[u8]> = TableDefinition::new("meta");
const MUTATIONS: TableDefinition<u64, &[u8]> = TableDefinition::new("mutations");
const PENDING_BLOBS: TableDefinition<&str, &[u8]> = TableDefinition::new("pending_blobs");
const REMOTE_OBJECT_VERSIONS: TableDefinition<&str, &[u8]> =
    TableDefinition::new("remote_object_versions");

const IDENTITY_KEY: &str = "identity";
const INCARNATION_KEY: &str = "incarnation";
const LOCAL_SEQ_KEY: &str = "local_seq";
const LOCAL_BYTES_COMPLETED_KEY: &str = "local_bytes_completed";
const REMOTE_SEQ_KEY: &str = "remote_seq";
const REMOTE_BYTES_COMPLETED_KEY: &str = "remote_bytes_completed";
const REMOTE_RETRIES_KEY: &str = "remote_retries";
const FENCE_CLASSIFICATION_VERSION_KEY: &str = "fence_classification_version";
const FENCE_CLASSIFICATION_VERSION: u32 = 1;

pub struct Journal {
    root: PathBuf,
    database: Database,
    write_gate: JournalWriteGate,
    _lock_file: File,
    format_version: u32,
    #[cfg(test)]
    snapshot_calls: AtomicU64,
    #[cfg(test)]
    remote_mark_pause: Mutex<Option<std::sync::Arc<RemoteMarkPauseInner>>>,
}

#[cfg(test)]
#[derive(Debug)]
struct RemoteMarkPauseInner {
    sequence: Sequence,
    entered: AtomicBool,
    entered_notify: tokio::sync::Notify,
    released: Mutex<bool>,
    release_ready: Condvar,
}

#[cfg(test)]
pub(crate) struct RemoteMarkPause {
    inner: std::sync::Arc<RemoteMarkPauseInner>,
}

#[cfg(test)]
impl RemoteMarkPause {
    pub(crate) async fn wait_entered(&self) {
        loop {
            let entered = self.inner.entered_notify.notified();
            if self.inner.entered.load(Ordering::Acquire) {
                return;
            }
            entered.await;
        }
    }

    pub(crate) fn release(&self) {
        let mut released = self
            .inner
            .released
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        *released = true;
        drop(released);
        self.inner.release_ready.notify_all();
    }
}

#[cfg(test)]
impl Drop for RemoteMarkPause {
    fn drop(&mut self) {
        self.release();
    }
}

#[derive(Debug, Default)]
struct JournalWriteGate {
    state: Mutex<JournalWriteGateState>,
    ready: Condvar,
}

#[derive(Debug, Default)]
struct JournalWriteGateState {
    next_ticket: u64,
    serving: u64,
}

struct JournalWriteGuard<'a> {
    gate: &'a JournalWriteGate,
}

impl JournalWriteGate {
    fn lock(&self) -> JournalWriteGuard<'_> {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        let ticket = state.next_ticket;
        state.next_ticket = state
            .next_ticket
            .checked_add(1)
            .expect("journal write ticket overflow");
        while state.serving != ticket {
            state = self
                .ready
                .wait(state)
                .unwrap_or_else(|error| error.into_inner());
        }
        drop(state);
        JournalWriteGuard { gate: self }
    }
}

impl Drop for JournalWriteGuard<'_> {
    fn drop(&mut self) {
        let mut state = self
            .gate
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        state.serving = state
            .serving
            .checked_add(1)
            .expect("journal write ticket overflow");
        drop(state);
        self.gate.ready.notify_all();
    }
}

/// A validated mutation waiting to join a publication batch.
///
/// Payload bytes stay in memory until publication because a record's slice
/// offset is only known once its batch is assembled. The RAM is already
/// reserved by the submitter's admission permit, which is held across
/// publication anyway, so retaining the bytes this long costs no new budget.
pub(crate) struct PreparedMutation {
    record: MutationRecord,
    payload: Option<VerifiedPayload>,
}

trait PublicationFilesystem {
    fn sync_directory(&self, path: &Path) -> Result<()>;
}

struct StdPublicationFilesystem;

impl PublicationFilesystem for StdPublicationFilesystem {
    fn sync_directory(&self, path: &Path) -> Result<()> {
        sync_directory(path)
    }
}

impl PreparedMutation {
    pub(crate) fn sequence(&self) -> Sequence {
        self.record.sequence
    }

    pub(crate) fn encoded_record_bytes(&self) -> Result<usize> {
        usize::try_from(
            bincode::serialized_size(&self.record)
                .context("failed to size prepared journal mutation")?,
        )
        .context("prepared journal mutation size exceeds addressable memory")
    }

    /// Payload bytes this mutation contributes to its batch container. The
    /// journaler uses it to cap a container's size, which is what bounds both
    /// the retained RAM and the transient disk overhead of reclamation.
    pub(crate) fn payload_bytes(&self) -> u64 {
        self.payload
            .as_ref()
            .map_or(0, crate::writeback::payload::VerifiedPayload::byte_len)
    }

    #[cfg(test)]
    pub(crate) fn metadata(record: MutationRecord) -> Self {
        Self {
            record,
            payload: None,
        }
    }

    /// A prepared mutation for sinks that stand in for the journal. It keeps
    /// the payload so batching decisions that depend on container size behave
    /// as they do in production.
    #[cfg(test)]
    pub(crate) fn for_test(record: MutationRecord, payload: Option<&VerifiedPayload>) -> Self {
        Self {
            record,
            payload: payload.cloned(),
        }
    }
}

impl fmt::Debug for Journal {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Journal")
            .field("root", &self.root)
            .finish_non_exhaustive()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JournalSnapshot {
    pub identity: JournalIdentity,
    pub incarnation: Uuid,
    pub local_seq: Sequence,
    pub remote_seq: Sequence,
    pub local_bytes_completed: u64,
    pub remote_bytes_completed: u64,
    pub remote_retries: u64,
    pub records: Vec<MutationRecord>,
    pub dirty_blob_bytes: u64,
    pub dirty_metadata_reserved_bytes: u64,
    pub pending_blob_count: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct JournalProgress {
    pub local_seq: Sequence,
    pub remote_seq: Sequence,
    pub local_bytes_completed: u64,
    pub remote_bytes_completed: u64,
    pub remote_retries: u64,
}

/// A bounded pending-record slice read together with the watermarks it is
/// consistent with. Reading the watermarks and the records in two transactions
/// tears: a remote commit landing in between prunes records the earlier
/// watermark says must still exist.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PendingWindow {
    pub local_seq: Sequence,
    pub remote_seq: Sequence,
    pub records: Vec<MutationRecord>,
}

impl Journal {
    pub fn open_existing(root: impl AsRef<Path>) -> Result<Self> {
        let root = root.as_ref().to_path_buf();
        let database_path = root.join("journal.redb");
        if !database_path.is_file() {
            bail!(
                "writeback journal database does not exist at {}",
                database_path.display()
            );
        }
        let database = Database::create(&database_path).with_context(|| {
            format!(
                "failed to open existing journal database {}",
                database_path.display()
            )
        })?;
        let read = database
            .begin_read()
            .context("failed to read existing journal identity")?;
        let meta = read
            .open_table(META)
            .context("failed to open existing journal metadata")?;
        let identity = read_required::<JournalIdentity>(&meta, IDENTITY_KEY)?;
        drop(meta);
        drop(read);
        drop(database);
        Self::open(root, identity)
    }

    pub fn open(root: impl AsRef<Path>, expected_identity: JournalIdentity) -> Result<Self> {
        let root = root.as_ref().to_path_buf();
        ensure_journal_root(&root)?;
        let lock_path = root.join("LOCK");
        reject_symlink_if_present(&lock_path, "journal lock")?;
        let lock_file = open_owner_file(&lock_path, true)
            .with_context(|| format!("failed to open journal lock {}", lock_path.display()))?;
        if !FileExt::try_lock_exclusive(&lock_file).context("failed to acquire journal lock")? {
            bail!("writeback journal is already locked by another process");
        }

        let blobs = root.join("blobs");
        let tmp = root.join("tmp");
        ensure_owner_directory(&blobs, true)?;
        ensure_owner_directory(&tmp, true)?;
        let database_path = root.join("journal.redb");
        reject_symlink_if_present(&database_path, "journal database")?;
        let database_existed = database_path.exists();
        if database_existed {
            let metadata =
                fs::metadata(&database_path).context("failed to inspect journal database")?;
            if !metadata.is_file() {
                bail!(
                    "journal database {} is not a regular file",
                    database_path.display()
                );
            }
            validate_owner_only(&database_path, &metadata, 0o600)?;
        }
        let database = Database::create(&database_path).with_context(|| {
            format!(
                "failed to open journal database {}",
                database_path.display()
            )
        })?;
        if !database_existed {
            set_owner_only_file(&database_path)?;
            sync_directory(&root)?;
        }

        initialize_or_validate_identity(&database, &expected_identity)?;
        normalize_mutation_fences(&database, &expected_identity)?;
        backfill_local_payload_bytes(&database)?;
        backfill_remote_object_versions(&database)?;
        let journal = Self {
            root,
            database,
            write_gate: JournalWriteGate::default(),
            _lock_file: lock_file,
            format_version: expected_identity.format_version,
            #[cfg(test)]
            snapshot_calls: AtomicU64::new(0),
            #[cfg(test)]
            remote_mark_pause: Mutex::new(None),
        };
        journal.recover_local_artifacts()?;
        let remote_seq = journal.progress()?.remote_seq;
        if remote_seq > 0 {
            journal.remove_remote_prefix(remote_seq)?;
        }
        journal.validate_recovery_state()?;
        Ok(journal)
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    pub fn snapshot(&self) -> Result<JournalSnapshot> {
        #[cfg(test)]
        self.snapshot_calls.fetch_add(1, Ordering::Relaxed);
        let read = self
            .database
            .begin_read()
            .context("failed to read journal")?;
        let meta = read
            .open_table(META)
            .context("failed to open journal metadata")?;
        let identity = read_required::<JournalIdentity>(&meta, IDENTITY_KEY)?;
        let incarnation = read_required::<Uuid>(&meta, INCARNATION_KEY)?;
        let local_seq = read_required::<u64>(&meta, LOCAL_SEQ_KEY)?;
        let remote_seq = read_required::<u64>(&meta, REMOTE_SEQ_KEY)?;
        let local_bytes_completed = read_required::<u64>(&meta, LOCAL_BYTES_COMPLETED_KEY)?;
        let remote_bytes_completed =
            read_optional::<u64>(&meta, REMOTE_BYTES_COMPLETED_KEY)?.unwrap_or_default();
        let remote_retries = read_optional::<u64>(&meta, REMOTE_RETRIES_KEY)?.unwrap_or_default();
        drop(meta);

        let table = read
            .open_table(MUTATIONS)
            .context("failed to open journal mutations")?;
        let mut records = Vec::new();
        let mut dirty_blob_bytes = 0_u64;
        let mut dirty_metadata_reserved_bytes = 0_u64;
        for entry in table
            .iter()
            .context("failed to iterate journal mutations")?
        {
            let (_, value) = entry.context("failed to read journal mutation")?;
            let record: MutationRecord =
                bincode::deserialize(value.value()).context("failed to decode journal mutation")?;
            if record.sequence > remote_seq {
                let payload_len = record.payload().map_or(0, |(payload_len, _)| payload_len);
                let charge = record.ssd_reservation_bytes()?;
                let metadata_len = charge
                    .checked_sub(payload_len)
                    .context("dirty journal charge is smaller than its payload")?;
                dirty_blob_bytes = dirty_blob_bytes
                    .checked_add(payload_len)
                    .context("dirty journal blob byte count overflow")?;
                dirty_metadata_reserved_bytes = dirty_metadata_reserved_bytes
                    .checked_add(metadata_len)
                    .context("dirty journal metadata byte count overflow")?;
            }
            records.push(record);
        }
        drop(table);
        let pending = read
            .open_table(PENDING_BLOBS)
            .context("failed to open pending blob table")?;
        let pending_blob_count = pending
            .iter()
            .context("failed to iterate pending blobs")?
            .count() as u64;
        Ok(JournalSnapshot {
            identity,
            incarnation,
            local_seq,
            remote_seq,
            local_bytes_completed,
            remote_bytes_completed,
            remote_retries,
            records,
            dirty_blob_bytes,
            dirty_metadata_reserved_bytes,
            pending_blob_count,
        })
    }

    #[cfg(test)]
    pub(crate) fn reset_snapshot_calls(&self) {
        self.snapshot_calls.store(0, Ordering::Relaxed);
    }

    #[cfg(test)]
    pub(crate) fn snapshot_calls(&self) -> u64 {
        self.snapshot_calls.load(Ordering::Relaxed)
    }

    #[cfg(test)]
    pub(crate) fn pause_remote_mark(&self, sequence: Sequence) -> RemoteMarkPause {
        let inner = std::sync::Arc::new(RemoteMarkPauseInner {
            sequence,
            entered: AtomicBool::new(false),
            entered_notify: tokio::sync::Notify::new(),
            released: Mutex::new(false),
            release_ready: Condvar::new(),
        });
        let replaced = self
            .remote_mark_pause
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .replace(inner.clone());
        assert!(replaced.is_none(), "remote mark pause is already installed");
        RemoteMarkPause { inner }
    }

    #[cfg(test)]
    fn wait_if_remote_mark_paused(&self, sequence: Sequence) {
        let pause = self
            .remote_mark_pause
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .clone();
        let Some(pause) = pause.filter(|pause| pause.sequence == sequence) else {
            return;
        };
        pause.entered.store(true, Ordering::Release);
        pause.entered_notify.notify_one();
        let mut released = pause
            .released
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        while !*released {
            released = pause
                .release_ready
                .wait(released)
                .unwrap_or_else(|error| error.into_inner());
        }
    }

    pub fn progress(&self) -> Result<JournalProgress> {
        let read = self
            .database
            .begin_read()
            .context("failed to read journal progress")?;
        let meta = read
            .open_table(META)
            .context("failed to open journal metadata")?;
        Ok(JournalProgress {
            local_seq: read_required::<u64>(&meta, LOCAL_SEQ_KEY)?,
            remote_seq: read_required::<u64>(&meta, REMOTE_SEQ_KEY)?,
            local_bytes_completed: read_required::<u64>(&meta, LOCAL_BYTES_COMPLETED_KEY)?,
            remote_bytes_completed: read_optional::<u64>(&meta, REMOTE_BYTES_COMPLETED_KEY)?
                .unwrap_or_default(),
            remote_retries: read_optional::<u64>(&meta, REMOTE_RETRIES_KEY)?.unwrap_or_default(),
        })
    }

    pub fn pending_from(
        &self,
        first_sequence: Sequence,
        limit: usize,
    ) -> Result<Vec<MutationRecord>> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let read = self
            .database
            .begin_read()
            .context("failed to read pending journal mutations")?;
        let table = read
            .open_table(MUTATIONS)
            .context("failed to open journal mutations")?;
        Self::read_pending_range(&table, first_sequence, limit)
    }

    /// The bounded pending slice plus the watermarks it is consistent with, in
    /// one read transaction. Callers that validate the slice against a
    /// watermark must use this instead of `progress` + `pending_from`.
    pub fn pending_window(&self, first_sequence: Sequence, limit: usize) -> Result<PendingWindow> {
        let read = self
            .database
            .begin_read()
            .context("failed to read pending journal window")?;
        let meta = read
            .open_table(META)
            .context("failed to open journal metadata")?;
        let local_seq = read_required::<u64>(&meta, LOCAL_SEQ_KEY)?;
        let remote_seq = read_required::<u64>(&meta, REMOTE_SEQ_KEY)?;
        drop(meta);
        let records = if limit == 0 {
            Vec::new()
        } else {
            let table = read
                .open_table(MUTATIONS)
                .context("failed to open journal mutations")?;
            Self::read_pending_range(&table, first_sequence, limit)?
        };
        Ok(PendingWindow {
            local_seq,
            remote_seq,
            records,
        })
    }

    fn read_pending_range(
        table: &impl redb::ReadableTable<Sequence, &'static [u8]>,
        first_sequence: Sequence,
        limit: usize,
    ) -> Result<Vec<MutationRecord>> {
        let mut records = Vec::with_capacity(limit);
        for entry in table
            .range(first_sequence..)
            .context("failed to seek pending journal mutations")?
            .take(limit)
        {
            let (_, value) = entry.context("failed to read pending journal mutation")?;
            records.push(
                bincode::deserialize(value.value())
                    .context("failed to decode pending journal mutation")?,
            );
        }
        Ok(records)
    }

    pub fn commit_put(&self, record: MutationRecord, payload: &[u8]) -> Result<MutationRecord> {
        let verified = VerifiedPayload::new(bytes::Bytes::copy_from_slice(payload));
        let prepared = self.prepare_verified_put(record, &verified)?;
        self.publish_prepared(prepared)
    }

    /// Validate a payload mutation and hold its bytes for the next batch.
    ///
    /// Preparation deliberately touches no disk. The record's blob reference
    /// names a slice of its batch's container, and neither the container nor
    /// the offset exists until the batch is assembled, so the write is the
    /// publication's job.
    pub(crate) fn prepare_verified_put(
        &self,
        record: MutationRecord,
        payload: &VerifiedPayload,
    ) -> Result<PreparedMutation> {
        self.validate_record_format(&record)?;
        let (payload_len, payload_sha256) = record
            .payload()
            .context("commit_put requires a payload mutation")?;
        if payload_len != payload.byte_len() {
            bail!("put payload length does not match mutation record");
        }
        if payload.sha256() != payload_sha256 {
            bail!("put payload hash does not match mutation record");
        }
        Ok(PreparedMutation {
            record,
            payload: Some(payload.clone()),
        })
    }

    pub(crate) fn prepare_metadata(&self, record: MutationRecord) -> Result<PreparedMutation> {
        self.validate_record_format(&record)?;
        if record.payload().is_some() {
            bail!("prepare_metadata cannot prepare a payload mutation");
        }
        Ok(PreparedMutation {
            record,
            payload: None,
        })
    }

    pub(crate) fn publish_prepared(&self, prepared: PreparedMutation) -> Result<MutationRecord> {
        let mut committed = self.publish_batch(vec![prepared])?;
        Ok(committed
            .pop()
            .expect("one prepared mutation must publish as one record"))
    }

    pub(crate) fn publish_batch(
        &self,
        prepared: Vec<PreparedMutation>,
    ) -> Result<Vec<MutationRecord>> {
        self.publish_batch_with(prepared, &StdPublicationFilesystem)
    }

    fn publish_batch_with(
        &self,
        prepared: Vec<PreparedMutation>,
        filesystem: &dyn PublicationFilesystem,
    ) -> Result<Vec<MutationRecord>> {
        if prepared.is_empty() {
            return Ok(Vec::new());
        }
        if let Err(error) = self.require_contiguous_local_batch(&prepared) {
            let cleanup = self.discard_prepared_batch(prepared);
            return match cleanup {
                Ok(()) => Err(error),
                Err(cleanup) => {
                    Err(error.context(format!("prepared batch cleanup also failed: {cleanup:#}")))
                }
            };
        }
        if let Err(error) = self.require_unique_local_batch_identities(&prepared) {
            let cleanup = self.discard_prepared_batch(prepared);
            return match cleanup {
                Ok(()) => Err(error),
                Err(cleanup) => {
                    Err(error.context(format!("prepared batch cleanup also failed: {cleanup:#}")))
                }
            };
        }

        let mut records = Vec::with_capacity(prepared.len());
        let mut payloads = Vec::with_capacity(prepared.len());
        for prepared in prepared {
            let PreparedMutation { record, payload } = prepared;
            if record.payload().is_some() != payload.is_some() {
                bail!("prepared payload and mutation record disagree");
            }
            if let Some(payload) = payload {
                payloads.push((records.len(), payload));
            }
            records.push(record);
        }

        // One batch, one container: assign every payload a slice of a single
        // file named for the batch's contiguous sequence range.
        let container = if payloads.is_empty() {
            None
        } else {
            let first = records
                .first()
                .expect("a non-empty batch has a first record")
                .sequence;
            let last = records
                .last()
                .expect("a non-empty batch has a last record")
                .sequence;
            let relative = path_to_portable_string(&container_relative_path(first, last))?;
            let mut offset = 0_u64;
            for (index, payload) in &payloads {
                let len = payload.byte_len();
                *records[*index]
                    .blob_path_mut()
                    .context("payload mutation has no blob path")? =
                    BlobRef::format(&relative, offset, len);
                offset = offset
                    .checked_add(len)
                    .context("publication container size overflow")?;
            }
            Some(relative)
        };

        // The container is written straight to its final name. It carries the
        // sequence range it covers, and a batch commits atomically, so a
        // container is committed exactly when `last <= LOCAL_SEQ`. Recovery
        // therefore recognises a torn or orphaned container from its name
        // alone -- which is why publication needs neither a staging rename nor
        // a durable pending-blob intent, and pays two fewer fsync-class
        // operations per batch than a stage-then-rename would.
        let write_started = Instant::now();
        let written = match container.as_deref() {
            Some(relative) => Some(self.write_container(relative, &payloads)?),
            None => None,
        };
        record_local_publish_phase("container_write", write_started.elapsed());

        let mut directories = BTreeSet::new();
        if let Some(path) = written.as_deref() {
            directories.insert(
                path.parent()
                    .context("blob path has no parent")?
                    .to_path_buf(),
            );
        }

        let fsync_started = Instant::now();
        for directory in &directories {
            if let Err(error) = filesystem.sync_directory(directory) {
                let publication = error.context(format!(
                    "failed to fsync published blob directory {}",
                    directory.display()
                ));
                let cleanup =
                    self.rollback_uncommitted_batch(written.as_deref(), &directories, filesystem);
                return match cleanup {
                    Ok(()) => Err(publication),
                    Err(cleanup) => {
                        Err(publication
                            .context(format!("container cleanup also failed: {cleanup:#}")))
                    }
                };
            }
        }
        record_local_publish_phase("directory_fsync", fsync_started.elapsed());

        let commit_started = Instant::now();
        if let Err(error) = self.commit_record_batch(&records) {
            // Recovery would unlink this container anyway -- the watermark
            // never reached its last member -- but a live process should not
            // sit on bytes nothing references until the next open.
            let cleanup =
                self.rollback_uncommitted_batch(written.as_deref(), &directories, filesystem);
            return match cleanup {
                Ok(()) => Err(error),
                Err(cleanup) => {
                    Err(error.context(format!("container cleanup also failed: {cleanup:#}")))
                }
            };
        }
        record_local_publish_phase("record_commit", commit_started.elapsed());
        metrics::counter!("zerofs_writeback_local_publish_batches_total").increment(1);
        metrics::counter!("zerofs_writeback_local_publish_records_total")
            .increment(records.len() as u64);
        metrics::histogram!("zerofs_writeback_local_publish_batch_records")
            .record(records.len() as f64);
        Ok(records)
    }

    /// Write one batch's payloads back to back into its container and fsync
    /// it. This is the whole point of the container: N payloads cost one
    /// sequential write and one fsync instead of N of each.
    fn write_container(
        &self,
        relative: &str,
        payloads: &[(usize, VerifiedPayload)],
    ) -> Result<PathBuf> {
        let path = checked_join(&self.root, relative)?;
        let shard = path.parent().context("blob path has no parent")?;
        ensure_owner_directory(shard, true)?;
        reject_symlink_if_present(&path, "journal container blob")?;

        let expected_len = payloads
            .iter()
            .try_fold(0_u64, |total, (_, payload)| {
                total.checked_add(payload.byte_len())
            })
            .context("publication container size overflow")?;
        // Create first, and only arm the cleanup once the file exists: a
        // failure to create it means there is nothing of ours to unlink, and
        // whatever occupies the name is not ours to remove.
        let mut file = open_owner_file(&path, false)
            .with_context(|| format!("failed to create container {}", path.display()))?;
        let write = (|| -> Result<()> {
            write_all_vectored(
                &mut file,
                &payloads
                    .iter()
                    .map(|(_, payload)| payload.bytes())
                    .collect::<Vec<_>>(),
            )
            .context("failed to write container")?;
            // fdatasync, not fsync: it still persists the data and the
            // metadata needed to read it back (size and extents), which is
            // all a container needs. The link itself is made durable by the
            // shard directory fsync below, and the inode timestamps a full
            // fsync would additionally journal are not load bearing here.
            file.sync_data().context("failed to fsync container")?;
            let written_len = file
                .metadata()
                .context("failed to inspect container")?
                .len();
            if written_len != expected_len {
                bail!("container length mismatch: expected {expected_len}, got {written_len}");
            }
            Ok(())
        })();
        if let Err(error) = write {
            let cleanup = self.discard_container(&path);
            return match cleanup {
                Ok(()) => Err(error),
                Err(cleanup) => {
                    Err(error.context(format!("container cleanup also failed: {cleanup:#}")))
                }
            };
        }
        Ok(path)
    }

    fn discard_prepared_batch(&self, prepared: Vec<PreparedMutation>) -> Result<()> {
        let mut first_error = None;
        for mutation in prepared {
            if let Err(error) = self.discard_prepared(mutation)
                && first_error.is_none()
            {
                first_error = Some(error);
            }
        }
        match first_error {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }

    fn rollback_uncommitted_batch(
        &self,
        written: Option<&Path>,
        directories: &BTreeSet<PathBuf>,
        filesystem: &dyn PublicationFilesystem,
    ) -> Result<()> {
        let mut first_error = None;
        if let Some(path) = written {
            match fs::remove_file(path) {
                Ok(()) => {}
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => {
                    first_error = Some(
                        anyhow::Error::new(error)
                            .context("failed to remove uncommitted publication container"),
                    );
                }
            }
        }
        for directory in directories {
            if let Err(error) = filesystem.sync_directory(directory)
                && first_error.is_none()
            {
                first_error = Some(error.context(format!(
                    "failed to fsync uncommitted blob cleanup directory {}",
                    directory.display()
                )));
            }
        }
        match first_error {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }

    fn require_contiguous_local_batch(&self, prepared: &[PreparedMutation]) -> Result<()> {
        let progress = self.progress()?;
        let mut expected = progress
            .local_seq
            .checked_add(1)
            .context("local sequence overflow")?;
        for (index, mutation) in prepared.iter().enumerate() {
            if mutation.sequence() != expected {
                bail!(
                    "local sequence must advance contiguously from {} to {expected}, got {}",
                    progress.local_seq,
                    mutation.sequence()
                );
            }
            if index + 1 < prepared.len() {
                expected = expected.checked_add(1).context("local sequence overflow")?;
            }
        }
        Ok(())
    }

    fn require_unique_local_batch_identities(&self, prepared: &[PreparedMutation]) -> Result<()> {
        let mut operation_ids = BTreeSet::new();
        for mutation in prepared {
            if !operation_ids.insert(mutation.record.operation_id) {
                bail!(
                    "local publication batch contains duplicate operation ID {}",
                    mutation.record.operation_id
                );
            }
        }
        Ok(())
    }

    /// The batch's single durability point. Inserting the records and
    /// advancing `LOCAL_SEQ_KEY` in one immediate transaction is what makes a
    /// batch atomic, and what makes `last <= LOCAL_SEQ` a sound test for
    /// "this container committed".
    fn commit_record_batch(&self, records: &[MutationRecord]) -> Result<()> {
        let Some(last) = records.last() else {
            return Ok(());
        };
        let _write = self.write_gate.lock();
        let mut transaction = self
            .database
            .begin_write()
            .context("failed to commit journal record batch")?;
        transaction
            .set_durability(Durability::Immediate)
            .context("failed to set journal durability")?;
        {
            let mut meta = transaction
                .open_table(META)
                .context("failed to open journal metadata")?;
            let current = read_required::<u64>(&meta, LOCAL_SEQ_KEY)?;
            let mut expected = current.checked_add(1).context("local sequence overflow")?;
            let mut total_completed =
                read_optional::<u64>(&meta, LOCAL_BYTES_COMPLETED_KEY)?.unwrap_or_default();
            let mut table = transaction
                .open_table(MUTATIONS)
                .context("failed to open journal mutations")?;
            for (index, record) in records.iter().enumerate() {
                if record.sequence != expected {
                    bail!(
                        "local sequence must advance contiguously from {current} to {expected}, got {}",
                        record.sequence
                    );
                }
                total_completed = total_completed
                    .checked_add(record.payload().map_or(0, |(payload_len, _)| payload_len))
                    .context("local completed byte counter overflow")?;
                let encoded =
                    bincode::serialize(record).context("failed to encode journal mutation")?;
                table
                    .insert(record.sequence, encoded.as_slice())
                    .context("failed to store journal mutation")?;
                if index + 1 < records.len() {
                    expected = expected.checked_add(1).context("local sequence overflow")?;
                }
            }
            drop(table);
            write_value(&mut meta, LOCAL_SEQ_KEY, &last.sequence)?;
            write_value(&mut meta, LOCAL_BYTES_COMPLETED_KEY, &total_completed)?;
        }
        transaction
            .commit()
            .context("failed to commit journal mutation batch")
    }

    /// Preparation writes nothing, so discarding one only drops its bytes.
    pub(crate) fn discard_prepared(&self, prepared: PreparedMutation) -> Result<()> {
        drop(prepared);
        Ok(())
    }

    fn discard_container(&self, path: &Path) -> Result<()> {
        match fs::remove_file(path) {
            Ok(()) => match path.parent() {
                Some(parent) => sync_directory(parent),
                None => Ok(()),
            },
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(error).context("failed to remove uncommitted container"),
        }
    }

    pub fn commit_metadata(&self, record: MutationRecord) -> Result<MutationRecord> {
        let prepared = self.prepare_metadata(record)?;
        self.publish_prepared(prepared)
    }

    pub fn read_blob(&self, sequence: Sequence) -> Result<Vec<u8>> {
        let record = self
            .mutation(sequence)?
            .with_context(|| format!("journal mutation {sequence} does not exist"))?;
        let reference = record
            .blob_path()
            .with_context(|| format!("journal mutation {sequence} has no blob"))?;
        let reference = BlobRef::parse(reference)?;
        let path = checked_join(&self.root, reference.relative)?;
        read_verified_blob(&path, reference.slice, &record)
    }

    pub fn mark_remote(&self, sequence: Sequence, result_etag: Option<String>) -> Result<()> {
        MutationRecord::validate_persisted_version_field(
            "remote result ETag",
            result_etag.as_deref(),
        )?;
        let _write = self.write_gate.lock();
        #[cfg(test)]
        self.wait_if_remote_mark_paused(sequence);
        let mut transaction = self
            .database
            .begin_write()
            .context("failed to update remote watermark")?;
        transaction
            .set_durability(Durability::Immediate)
            .context("failed to set journal durability")?;
        {
            let mut meta = transaction
                .open_table(META)
                .context("failed to open journal metadata")?;
            let remote_seq = read_required::<u64>(&meta, REMOTE_SEQ_KEY)?;
            let local_seq = read_required::<u64>(&meta, LOCAL_SEQ_KEY)?;
            let expected = remote_seq
                .checked_add(1)
                .context("remote sequence overflow")?;
            if sequence != expected || sequence > local_seq {
                bail!(
                    "remote sequence must advance contiguously from {remote_seq} to {expected}, got {sequence}"
                );
            }
            let mut mutations = transaction
                .open_table(MUTATIONS)
                .context("failed to open journal mutations")?;
            let encoded = mutations
                .get(sequence)
                .context("failed to read remote mutation")?
                .map(|value| value.value().to_vec())
                .with_context(|| format!("journal mutation {sequence} does not exist"))?;
            let mut record: MutationRecord =
                bincode::deserialize(&encoded).context("failed to decode remote mutation")?;
            let completed_bytes = record.payload().map_or(0, |(payload_len, _)| payload_len);
            let total_completed = read_optional::<u64>(&meta, REMOTE_BYTES_COMPLETED_KEY)?
                .unwrap_or_default()
                .checked_add(completed_bytes)
                .context("remote completed byte counter overflow")?;
            record.remote_result_etag = result_etag;
            let encoded =
                bincode::serialize(&record).context("failed to encode remote mutation")?;
            mutations
                .insert(sequence, encoded.as_slice())
                .context("failed to store remote result")?;
            drop(mutations);
            let mut versions = transaction
                .open_table(REMOTE_OBJECT_VERSIONS)
                .context("failed to open remote object versions")?;
            apply_remote_object_version(&mut versions, &record)?;
            drop(versions);
            write_value(&mut meta, REMOTE_SEQ_KEY, &sequence)?;
            write_value(&mut meta, REMOTE_BYTES_COMPLETED_KEY, &total_completed)?;
        }
        transaction
            .commit()
            .context("failed to commit remote watermark")
    }

    pub(crate) fn remote_object_etag(
        &self,
        path: &str,
        sequence: Sequence,
    ) -> Result<Option<String>> {
        let read = self
            .database
            .begin_read()
            .context("failed to read remote object version")?;
        let versions = read
            .open_table(REMOTE_OBJECT_VERSIONS)
            .context("failed to open remote object versions")?;
        let Some(encoded) = versions
            .get(path)
            .context("failed to fetch remote object version")?
        else {
            return Ok(None);
        };
        let (published_sequence, e_tag): (Sequence, String) = bincode::deserialize(encoded.value())
            .context("failed to decode remote object version")?;
        Ok((published_sequence == sequence).then_some(e_tag))
    }

    pub fn seed_remote_object_etag(
        &self,
        path: &str,
        sequence: Sequence,
        e_tag: &str,
    ) -> Result<()> {
        if path.is_empty() || e_tag.is_empty() {
            bail!("remote object path and ETag must be non-empty");
        }
        MutationRecord::validate_persisted_version_field(
            "seeded remote predecessor ETag",
            Some(e_tag),
        )?;
        let progress = self.progress()?;
        if sequence > progress.remote_seq {
            bail!(
                "cannot seed predecessor sequence {sequence} above remote watermark {}",
                progress.remote_seq
            );
        }
        let _write = self.write_gate.lock();
        let mut transaction = self
            .database
            .begin_write()
            .context("failed to seed remote object predecessor")?;
        transaction
            .set_durability(Durability::Immediate)
            .context("failed to set predecessor seed durability")?;
        {
            let mut versions = transaction
                .open_table(REMOTE_OBJECT_VERSIONS)
                .context("failed to open remote object versions")?;
            if let Some(existing) = versions
                .get(path)
                .context("failed to read existing remote object version")?
            {
                let existing: (Sequence, String) = bincode::deserialize(existing.value())
                    .context("failed to decode existing remote object version")?;
                if existing == (sequence, e_tag.to_owned()) {
                    return Ok(());
                }
                bail!(
                    "remote object version for {path} is already seeded at sequence {}",
                    existing.0
                );
            }
            let encoded = bincode::serialize(&(sequence, e_tag.to_owned()))
                .context("failed to encode remote object predecessor")?;
            versions
                .insert(path, encoded.as_slice())
                .context("failed to store remote object predecessor")?;
        }
        transaction
            .commit()
            .context("failed to commit remote object predecessor seed")
    }

    pub fn record_remote_failure(&self, sequence: Sequence, error: &str) -> Result<()> {
        let _write = self.write_gate.lock();
        let mut transaction = self
            .database
            .begin_write()
            .context("failed to record remote retry")?;
        transaction
            .set_durability(Durability::Immediate)
            .context("failed to set journal durability")?;
        {
            let mut meta = transaction
                .open_table(META)
                .context("failed to open journal metadata")?;
            let remote_seq = read_required::<u64>(&meta, REMOTE_SEQ_KEY)?;
            let local_seq = read_required::<u64>(&meta, LOCAL_SEQ_KEY)?;
            if sequence <= remote_seq || sequence > local_seq {
                bail!(
                    "remote failure sequence {sequence} must be above remote watermark {remote_seq} and at or below local watermark {local_seq}"
                );
            }

            let mut mutations = transaction
                .open_table(MUTATIONS)
                .context("failed to open journal mutations")?;
            let encoded = mutations
                .get(sequence)
                .context("failed to read failed remote mutation")?
                .map(|value| value.value().to_vec())
                .with_context(|| format!("journal mutation {sequence} does not exist"))?;
            let mut record: MutationRecord = bincode::deserialize(&encoded)
                .context("failed to decode failed remote mutation")?;
            record.retry_count = record
                .retry_count
                .checked_add(1)
                .context("remote mutation retry count overflow")?;
            record.last_error = Some(error.chars().take(2_048).collect());
            let encoded =
                bincode::serialize(&record).context("failed to encode failed remote mutation")?;
            mutations
                .insert(sequence, encoded.as_slice())
                .context("failed to store failed remote mutation")?;
            drop(mutations);

            let remote_retries = read_optional::<u64>(&meta, REMOTE_RETRIES_KEY)?
                .unwrap_or_default()
                .checked_add(1)
                .context("remote retry counter overflow")?;
            write_value(&mut meta, REMOTE_RETRIES_KEY, &remote_retries)?;
        }
        transaction
            .commit()
            .context("failed to commit remote retry")
    }

    pub fn remove_remote_prefix(&self, through: Sequence) -> Result<()> {
        let progress = self.progress()?;
        if through > progress.remote_seq {
            bail!(
                "cannot clean through sequence {through} above remote watermark {}",
                progress.remote_seq
            );
        }
        let read = self
            .database
            .begin_read()
            .context("failed to read remote-complete journal prefix")?;
        let table = read
            .open_table(MUTATIONS)
            .context("failed to open journal mutations")?;
        let mut removable = Vec::<MutationRecord>::new();
        for entry in table
            .range(..=through)
            .context("failed to scan remote-complete journal prefix")?
        {
            let (_, value) = entry.context("failed to read remote-complete mutation")?;
            removable.push(
                bincode::deserialize(value.value())
                    .context("failed to decode remote-complete mutation")?,
            );
        }
        drop(table);
        drop(read);
        // A container holds one contiguous sequence range, so "every member is
        // remote-committed" is exactly `last <= through`. The range is in the
        // container's own name, so no refcount or side table is needed; a
        // straddling container simply survives until its final member drains.
        let mut reclaimable = BTreeSet::new();
        for record in &removable {
            let Some(relative) = record.blob_path() else {
                continue;
            };
            let reference = BlobRef::parse(relative)?;
            if reference.slice.is_some() {
                match container_last_sequence(reference.relative) {
                    Some(last) if last > through => continue,
                    Some(_) => {}
                    None => bail!("container blob {relative} does not name a sequence range"),
                }
            }
            reclaimable.insert(reference.relative.to_owned());
        }
        for relative in &reclaimable {
            let path = checked_join(&self.root, relative)?;
            match fs::remove_file(&path) {
                Ok(()) => {
                    if let Some(parent) = path.parent() {
                        sync_directory(parent)?;
                    }
                }
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => {
                    return Err(error).context("failed to remove remote-complete blob");
                }
            }
        }

        let _write = self.write_gate.lock();
        let mut transaction = self
            .database
            .begin_write()
            .context("failed to clean journal prefix")?;
        transaction
            .set_durability(Durability::Immediate)
            .context("failed to set journal durability")?;
        {
            let mut table = transaction
                .open_table(MUTATIONS)
                .context("failed to open journal mutations")?;
            for record in removable {
                table
                    .remove(record.sequence)
                    .context("failed to remove remote-complete mutation")?;
            }
        }
        transaction
            .commit()
            .context("failed to commit journal cleanup")
    }

    fn validate_record_format(&self, record: &MutationRecord) -> Result<()> {
        if record.format_version != self.format_version {
            bail!(
                "mutation format version {} does not match journal format version {}",
                record.format_version,
                self.format_version
            );
        }
        Ok(())
    }

    pub(crate) fn mutation(&self, sequence: Sequence) -> Result<Option<MutationRecord>> {
        let read = self
            .database
            .begin_read()
            .context("failed to read journal mutation")?;
        let table = read
            .open_table(MUTATIONS)
            .context("failed to open journal mutations")?;
        table
            .get(sequence)
            .context("failed to fetch journal mutation")?
            .map(|value| {
                bincode::deserialize(value.value()).context("failed to decode journal mutation")
            })
            .transpose()
    }

    fn recover_local_artifacts(&self) -> Result<()> {
        remove_directory_contents(&self.root.join("tmp"))?;
        sync_directory(self.root.join("tmp"))?;

        let pending = self.pending_blobs()?;
        for relative in pending.values() {
            let path = checked_join(&self.root, BlobRef::parse(relative)?.relative)?;
            match fs::remove_file(&path) {
                Ok(()) => {
                    if let Some(parent) = path.parent() {
                        sync_directory(parent)?;
                    }
                }
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(error).context("failed to remove uncommitted blob"),
            }
        }
        if !pending.is_empty() {
            let _write = self.write_gate.lock();
            let mut transaction = self
                .database
                .begin_write()
                .context("failed to clear pending blobs")?;
            transaction
                .set_durability(Durability::Immediate)
                .context("failed to set journal durability")?;
            {
                let mut table = transaction
                    .open_table(PENDING_BLOBS)
                    .context("failed to open pending blob table")?;
                for operation_id in pending.keys() {
                    table
                        .remove(operation_id.as_str())
                        .context("failed to clear pending blob")?;
                }
            }
            transaction
                .commit()
                .context("failed to commit pending cleanup")?;
        }
        self.remove_uncommitted_containers()
    }

    /// Unlink every container a crash left behind.
    ///
    /// A batch commits its records and `LOCAL_SEQ_KEY` in one immediate
    /// transaction, so a container is committed exactly when the local
    /// watermark has reached its last member. Anything above the watermark was
    /// interrupted before that commit -- possibly mid-write -- and no record
    /// references it, so it is unlinked here, before recovery validation would
    /// reject it as unreferenced.
    fn remove_uncommitted_containers(&self) -> Result<()> {
        let local_seq = self.progress()?.local_seq;
        let mut removed = BTreeSet::new();
        for shard in
            fs::read_dir(self.root.join("blobs")).context("failed to scan blob directory")?
        {
            let shard = shard.context("failed to read blob shard")?;
            if !fs::symlink_metadata(shard.path())
                .context("failed to inspect blob shard")?
                .is_dir()
            {
                continue;
            }
            for blob in fs::read_dir(shard.path()).context("failed to scan blob shard")? {
                let path = blob.context("failed to read blob entry")?.path();
                let Some(last) = path.to_str().and_then(container_last_sequence) else {
                    continue;
                };
                if last <= local_seq {
                    continue;
                }
                match fs::remove_file(&path) {
                    Ok(()) => {
                        removed.insert(shard.path());
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                    Err(error) => {
                        return Err(error).context("failed to remove uncommitted container");
                    }
                }
            }
        }
        for shard in removed {
            sync_directory(shard)?;
        }
        Ok(())
    }

    fn pending_blobs(&self) -> Result<BTreeMap<String, String>> {
        let read = self
            .database
            .begin_read()
            .context("failed to read pending blobs")?;
        let table = read
            .open_table(PENDING_BLOBS)
            .context("failed to open pending blob table")?;
        let mut pending = BTreeMap::new();
        for entry in table.iter().context("failed to iterate pending blobs")? {
            let (key, value) = entry.context("failed to read pending blob")?;
            // Pre-container journals keyed intents by operation UUID and
            // containers key them by the container path; only the value is
            // load bearing, so both forms recover through the same scan.
            let intent_key = key.value().to_owned();
            let relative = std::str::from_utf8(value.value())
                .context("pending blob path is not UTF-8")?
                .to_owned();
            checked_join(&self.root, BlobRef::parse(&relative)?.relative)?;
            pending.insert(intent_key, relative);
        }
        Ok(pending)
    }

    fn validate_recovery_state(&self) -> Result<()> {
        let snapshot = self.snapshot()?;
        if snapshot.remote_seq > snapshot.local_seq {
            bail!("remote journal watermark exceeds local watermark");
        }
        let mut expected = snapshot.remote_seq.saturating_add(1);
        let mut referenced_blobs = BTreeSet::new();
        for record in &snapshot.records {
            self.validate_record_format(record)?;
            if record.sequence <= snapshot.remote_seq {
                continue;
            }
            if record.sequence != expected {
                bail!(
                    "journal sequence gap: expected {expected}, found {}",
                    record.sequence
                );
            }
            expected = expected
                .checked_add(1)
                .context("journal sequence overflow")?;
            if let Some(relative) = record.blob_path() {
                let reference = BlobRef::parse(relative)?;
                let path = checked_join(&self.root, reference.relative)?;
                verify_record_blob(&path, reference.slice, record).with_context(|| {
                    if path.exists() {
                        format!(
                            "committed blob validation failed for sequence {}",
                            record.sequence
                        )
                    } else {
                        format!("missing committed blob for sequence {}", record.sequence)
                    }
                })?;
                referenced_blobs.insert(path);
            }
        }
        if snapshot.local_seq >= snapshot.remote_seq
            && expected != snapshot.local_seq.saturating_add(1)
        {
            bail!(
                "journal sequence gap before local watermark {}",
                snapshot.local_seq
            );
        }
        self.reject_unreferenced_blobs(&referenced_blobs)
    }

    fn reject_unreferenced_blobs(&self, referenced: &BTreeSet<PathBuf>) -> Result<()> {
        for shard in
            fs::read_dir(self.root.join("blobs")).context("failed to scan blob directory")?
        {
            let shard = shard.context("failed to read blob shard")?;
            let metadata =
                fs::symlink_metadata(shard.path()).context("failed to inspect blob shard")?;
            if metadata.file_type().is_symlink() || !metadata.is_dir() {
                bail!(
                    "blob shard {} is not a safe directory",
                    shard.path().display()
                );
            }
            for blob in fs::read_dir(shard.path()).context("failed to scan blob shard")? {
                let blob = blob.context("failed to read blob entry")?;
                let path = blob.path();
                let metadata =
                    fs::symlink_metadata(&path).context("failed to inspect blob entry")?;
                if metadata.file_type().is_symlink() || !metadata.is_file() {
                    bail!("blob entry {} is not a safe regular file", path.display());
                }
                if !referenced.contains(&path) {
                    bail!("unreferenced committed blob {}", path.display());
                }
            }
        }
        Ok(())
    }
}

fn record_local_publish_phase(phase: &'static str, elapsed: Duration) {
    metrics::histogram!("zerofs_writeback_local_publish_phase_seconds", "phase" => phase)
        .record(elapsed.as_secs_f64());
}

fn initialize_or_validate_identity(database: &Database, expected: &JournalIdentity) -> Result<()> {
    let mut transaction = database
        .begin_write()
        .context("failed to initialize journal")?;
    transaction
        .set_durability(Durability::Immediate)
        .context("failed to set journal durability")?;
    {
        let mut meta = transaction
            .open_table(META)
            .context("failed to open journal metadata")?;
        let existing = meta
            .get(IDENTITY_KEY)
            .context("failed to read journal identity")?
            .map(|value| value.value().to_vec());
        match existing {
            Some(encoded) => {
                let actual: JournalIdentity =
                    bincode::deserialize(&encoded).context("failed to decode journal identity")?;
                if &actual != expected {
                    bail!("writeback journal identity mismatch");
                }
            }
            None => {
                write_value(&mut meta, IDENTITY_KEY, expected)?;
                write_value(&mut meta, INCARNATION_KEY, &Uuid::new_v4())?;
                write_value(&mut meta, LOCAL_SEQ_KEY, &0_u64)?;
                write_value(&mut meta, LOCAL_BYTES_COMPLETED_KEY, &0_u64)?;
                write_value(&mut meta, REMOTE_SEQ_KEY, &0_u64)?;
                write_value(&mut meta, REMOTE_BYTES_COMPLETED_KEY, &0_u64)?;
                write_value(&mut meta, REMOTE_RETRIES_KEY, &0_u64)?;
            }
        }
        drop(meta);
        transaction
            .open_table(MUTATIONS)
            .context("failed to create journal mutations")?;
        transaction
            .open_table(PENDING_BLOBS)
            .context("failed to create pending blob table")?;
        transaction
            .open_table(REMOTE_OBJECT_VERSIONS)
            .context("failed to create remote object versions")?;
    }
    transaction
        .commit()
        .context("failed to commit journal identity")
}

/// Re-derive persisted fence classes before any recovery path can use them.
///
/// Version-1 journals serialized `FenceClass` while classification was based
/// on a loose path heuristic, so an overwrite, copy, or malformed key could be
/// recovered as `ImmutableCreate`. This durable migration uses the persisted
/// mutation kind and the journal identity's database prefix as the authority.
/// The marker and rewrites share one immediate transaction so replay and
/// remote-version backfill never observe a partially normalized journal.
fn normalize_mutation_fences(database: &Database, identity: &JournalIdentity) -> Result<()> {
    let mut transaction = database
        .begin_write()
        .context("failed to migrate mutation fence classifications")?;
    transaction
        .set_durability(Durability::Immediate)
        .context("failed to set fence classification migration durability")?;
    let existing_version = {
        let meta = transaction
            .open_table(META)
            .context("failed to open journal metadata for fence classification migration")?;
        read_optional::<u32>(&meta, FENCE_CLASSIFICATION_VERSION_KEY)?
    };
    if let Some(existing_version) = existing_version {
        if existing_version == FENCE_CLASSIFICATION_VERSION {
            return Ok(());
        }
        if existing_version > FENCE_CLASSIFICATION_VERSION {
            bail!(
                "journal fence classification version {existing_version} is newer than supported version {FENCE_CLASSIFICATION_VERSION}"
            );
        }
    }

    let rewritten = {
        let mutations = transaction
            .open_table(MUTATIONS)
            .context("failed to open journal mutations for fence classification migration")?;
        let mut rewritten = Vec::new();
        for entry in mutations
            .iter()
            .context("failed to scan mutations for fence classification migration")?
        {
            let (sequence, encoded) =
                entry.context("failed to read mutation for fence classification migration")?;
            let mut record: MutationRecord = bincode::deserialize(encoded.value())
                .context("failed to decode mutation for fence classification migration")?;
            if record.format_version != identity.format_version {
                bail!(
                    "journal mutation {} format version {} does not match journal format version {}",
                    record.sequence,
                    record.format_version,
                    identity.format_version
                );
            }
            let expected =
                classify_mutation_fence(&record.path, &record.kind, &identity.database_prefix);
            if record.fence != expected {
                record.fence = expected;
                rewritten.push((sequence.value(), bincode::serialize(&record)?));
            }
        }
        rewritten
    };
    if !rewritten.is_empty() {
        let mut mutations = transaction
            .open_table(MUTATIONS)
            .context("failed to reopen journal mutations for fence classification migration")?;
        for (sequence, encoded) in &rewritten {
            mutations
                .insert(*sequence, encoded.as_slice())
                .context("failed to rewrite normalized mutation fence classification")?;
        }
    }
    {
        let mut meta = transaction
            .open_table(META)
            .context("failed to reopen journal metadata for fence classification migration")?;
        write_value(
            &mut meta,
            FENCE_CLASSIFICATION_VERSION_KEY,
            &FENCE_CLASSIFICATION_VERSION,
        )?;
    }
    transaction
        .commit()
        .context("failed to commit mutation fence classification migration")
}

fn backfill_remote_object_versions(database: &Database) -> Result<()> {
    let mut transaction = database
        .begin_write()
        .context("failed to migrate remote object versions")?;
    transaction
        .set_durability(Durability::Immediate)
        .context("failed to set remote object version migration durability")?;
    let remote_seq = {
        let meta = transaction
            .open_table(META)
            .context("failed to open journal metadata for migration")?;
        read_required::<u64>(&meta, REMOTE_SEQ_KEY)?
    };
    let completed = {
        let mutations = transaction
            .open_table(MUTATIONS)
            .context("failed to open journal mutations for migration")?;
        let mut completed = Vec::new();
        for entry in mutations
            .range(..=remote_seq)
            .context("failed to scan completed mutations for migration")?
        {
            let (_, value) = entry.context("failed to read completed migration record")?;
            completed.push(
                bincode::deserialize(value.value())
                    .context("failed to decode completed migration record")?,
            );
        }
        completed
    };
    if completed.is_empty() {
        return Ok(());
    }
    {
        let mut versions = transaction
            .open_table(REMOTE_OBJECT_VERSIONS)
            .context("failed to open remote object versions for migration")?;
        for record in completed {
            apply_remote_object_version(&mut versions, &record)?;
        }
    }
    transaction
        .commit()
        .context("failed to commit remote object version migration")
}

fn backfill_local_payload_bytes(database: &Database) -> Result<()> {
    let mut transaction = database
        .begin_write()
        .context("failed to migrate local completed byte counter")?;
    transaction
        .set_durability(Durability::Immediate)
        .context("failed to set local byte counter migration durability")?;
    let (remote_seq, remote_bytes_completed, counter_exists) = {
        let meta = transaction
            .open_table(META)
            .context("failed to open journal metadata for local byte migration")?;
        (
            read_required::<u64>(&meta, REMOTE_SEQ_KEY)?,
            read_optional::<u64>(&meta, REMOTE_BYTES_COMPLETED_KEY)?.unwrap_or_default(),
            read_optional::<u64>(&meta, LOCAL_BYTES_COMPLETED_KEY)?.is_some(),
        )
    };
    if counter_exists {
        return Ok(());
    }
    let pending_bytes = {
        let mutations = transaction
            .open_table(MUTATIONS)
            .context("failed to open journal mutations for local byte migration")?;
        let mut total = 0_u64;
        if let Some(first_pending) = remote_seq.checked_add(1) {
            for entry in mutations
                .range(first_pending..)
                .context("failed to scan pending mutations for local byte migration")?
            {
                let (_, value) = entry.context("failed to read local byte migration record")?;
                let record: MutationRecord = bincode::deserialize(value.value())
                    .context("failed to decode local byte migration record")?;
                total = total
                    .checked_add(record.payload().map_or(0, |(payload_len, _)| payload_len))
                    .context("local completed byte counter migration overflow")?;
            }
        }
        total
    };
    let local_bytes_completed = remote_bytes_completed
        .checked_add(pending_bytes)
        .context("local completed byte counter migration overflow")?;
    {
        let mut meta = transaction
            .open_table(META)
            .context("failed to open journal metadata for local byte migration")?;
        write_value(&mut meta, LOCAL_BYTES_COMPLETED_KEY, &local_bytes_completed)?;
    }
    transaction
        .commit()
        .context("failed to commit local completed byte counter migration")
}

fn read_required<T: serde::de::DeserializeOwned>(
    table: &impl ReadableTable<&'static str, &'static [u8]>,
    key: &str,
) -> Result<T> {
    let bytes = table
        .get(key)
        .with_context(|| format!("failed to read journal metadata key {key}"))?
        .map(|value| value.value().to_vec())
        .with_context(|| format!("journal metadata key {key} is missing"))?;
    bincode::deserialize(&bytes)
        .with_context(|| format!("failed to decode journal metadata key {key}"))
}

fn read_optional<T: serde::de::DeserializeOwned>(
    table: &impl ReadableTable<&'static str, &'static [u8]>,
    key: &str,
) -> Result<Option<T>> {
    let Some(bytes) = table
        .get(key)
        .with_context(|| format!("failed to read journal metadata key {key}"))?
        .map(|value| value.value().to_vec())
    else {
        return Ok(None);
    };
    bincode::deserialize(&bytes)
        .with_context(|| format!("failed to decode journal metadata key {key}"))
        .map(Some)
}

fn write_value<T: SerializeValue>(
    table: &mut redb::Table<'_, &str, &[u8]>,
    key: &str,
    value: &T,
) -> Result<()> {
    let encoded = value.encode()?;
    table
        .insert(key, encoded.as_slice())
        .with_context(|| format!("failed to write journal metadata key {key}"))?;
    Ok(())
}

fn store_remote_object_version(
    versions: &mut redb::Table<'_, &str, &[u8]>,
    record: &MutationRecord,
) -> Result<()> {
    if record.fence == FenceClass::ImmutableCreate {
        return Ok(());
    }
    let Some(e_tag) = record.remote_result_etag.as_ref() else {
        versions
            .remove(record.path.as_str())
            .context("failed to clear remote object version without an ETag")?;
        return Ok(());
    };
    let encoded = bincode::serialize(&(record.sequence, e_tag))
        .context("failed to encode remote object version")?;
    versions
        .insert(record.path.as_str(), encoded.as_slice())
        .context("failed to store remote object version")?;
    Ok(())
}

fn apply_remote_object_version(
    versions: &mut redb::Table<'_, &str, &[u8]>,
    record: &MutationRecord,
) -> Result<()> {
    match &record.kind {
        MutationKind::Delete => {
            versions
                .remove(record.path.as_str())
                .context("failed to clear deleted remote object version")?;
        }
        MutationKind::Rename { source, .. } => {
            versions
                .remove(source.as_str())
                .context("failed to clear renamed remote source version")?;
            store_remote_object_version(versions, record)?;
        }
        MutationKind::Put { .. } | MutationKind::Copy { .. } => {
            store_remote_object_version(versions, record)?;
        }
    }
    Ok(())
}

trait SerializeValue {
    fn encode(&self) -> Result<Vec<u8>>;
}

impl<T: serde::Serialize> SerializeValue for T {
    fn encode(&self) -> Result<Vec<u8>> {
        bincode::serialize(self).context("failed to encode journal metadata")
    }
}

/// The byte range a record occupies inside its batch container.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct BlobSlice {
    offset: u64,
    len: u64,
}

/// A parsed `MutationRecord::blob_path`.
///
/// Two forms are accepted for the life of the format. Journals written before
/// batch containers store one whole file per record and carry no slice; those
/// keep replaying, so a journal may hold both forms side by side.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct BlobRef<'a> {
    relative: &'a str,
    slice: Option<BlobSlice>,
}

impl<'a> BlobRef<'a> {
    fn parse(reference: &'a str) -> Result<Self> {
        let Some((relative, fragment)) = reference.split_once('#') else {
            return Ok(Self {
                relative: reference,
                slice: None,
            });
        };
        let (offset, len) = fragment
            .split_once('+')
            .with_context(|| format!("blob reference {reference} has a malformed slice"))?;
        let offset = offset
            .parse::<u64>()
            .with_context(|| format!("blob reference {reference} has a malformed slice offset"))?;
        let len = len
            .parse::<u64>()
            .with_context(|| format!("blob reference {reference} has a malformed slice length"))?;
        offset
            .checked_add(len)
            .with_context(|| format!("blob reference {reference} slice overflows"))?;
        Ok(Self {
            relative,
            slice: Some(BlobSlice { offset, len }),
        })
    }

    fn format(relative: &str, offset: u64, len: u64) -> String {
        format!("{relative}#{offset}+{len}")
    }
}

/// One batch, one container, named for the contiguous sequence range it
/// covers. Sharding on the first sequence's high bits keeps a run of
/// consecutive batches inside one directory, so publication keeps paying one
/// directory fsync; encoding the range in the name lets pruning decide
/// reclamation from the path alone.
fn container_relative_path(first: Sequence, last: Sequence) -> PathBuf {
    PathBuf::from("blobs")
        .join(format!("{:02x}", (first >> 8) & 0xff))
        .join(format!("{first:016x}-{last:016x}.blobs"))
}

/// The last sequence a container covers, parsed back out of its file name.
fn container_last_sequence(relative: &str) -> Option<Sequence> {
    let name = Path::new(relative).file_name()?.to_str()?;
    let (_, last) = name.strip_suffix(".blobs")?.split_once('-')?;
    Sequence::from_str_radix(last, 16).ok()
}

/// Write every payload in one batch with as few syscalls as the kernel allows.
///
/// Empty payloads are dropped first: a vectored write over nothing but empty
/// buffers reports zero bytes written, which is indistinguishable from a
/// stalled device, and zero-length records are legal.
fn write_all_vectored(file: &mut File, payloads: &[&[u8]]) -> std::io::Result<()> {
    let mut slices = payloads
        .iter()
        .filter(|payload| !payload.is_empty())
        .map(|payload| std::io::IoSlice::new(payload))
        .collect::<Vec<_>>();
    let mut cursor = slices.as_mut_slice();
    while !cursor.is_empty() {
        let written = file.write_vectored(cursor)?;
        if written == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::WriteZero,
                "container write made no progress",
            ));
        }
        std::io::IoSlice::advance_slices(&mut cursor, written);
    }
    Ok(())
}

#[cfg(test)]
fn blob_relative_path(sequence: Sequence, operation_id: Uuid) -> PathBuf {
    // Shard on the sequence's high bits so a run of 256 consecutive records
    // shares one directory: a publication batch then pays one directory fsync
    // instead of one per record. (Sharding on the low bits scattered every
    // batch across N directories.) The path is stored in the record at
    // prepare time, so journals written under either scheme replay fine.
    PathBuf::from("blobs")
        .join(format!("{:02x}", (sequence >> 8) & 0xff))
        .join(format!("{operation_id}.blob"))
}

fn path_to_portable_string(path: &Path) -> Result<String> {
    path.to_str()
        .map(str::to_owned)
        .context("journal blob path is not UTF-8")
}

fn checked_join(root: &Path, relative: &str) -> Result<PathBuf> {
    let relative = Path::new(relative);
    if relative.is_absolute() {
        bail!("journal blob path must be relative");
    }
    let mut safe = PathBuf::new();
    for component in relative.components() {
        match component {
            Component::Normal(part) => safe.push(part),
            _ => bail!("journal blob path contains an unsafe component"),
        }
    }
    if safe.components().next().map(|part| part.as_os_str()) != Some("blobs".as_ref()) {
        bail!("journal blob path must be below blobs/");
    }
    Ok(root.join(safe))
}

fn read_verified_blob(
    path: &Path,
    slice: Option<BlobSlice>,
    record: &MutationRecord,
) -> Result<Vec<u8>> {
    let (payload_len, payload_sha256) = record
        .payload()
        .context("journal record does not reference a payload blob")?;
    verify_file_payload(path, slice, payload_len, payload_sha256, true)?
        .context("verified blob read did not return payload bytes")
}

fn verify_record_blob(
    path: &Path,
    slice: Option<BlobSlice>,
    record: &MutationRecord,
) -> Result<()> {
    let (payload_len, payload_sha256) = record
        .payload()
        .context("journal record does not reference a payload blob")?;
    verify_file_payload(path, slice, payload_len, payload_sha256, false).map(drop)
}

/// Verify (and optionally collect) one record's payload.
///
/// Verification is always per record, never per file: a container member is
/// hashed over exactly its own slice, so a neighbour's bytes can never satisfy
/// it. A whole-file reference still demands an exact file length, while a
/// container only requires that it be long enough to hold the slice.
fn verify_file_payload(
    path: &Path,
    slice: Option<BlobSlice>,
    expected_len: u64,
    expected_sha256: [u8; 32],
    collect: bool,
) -> Result<Option<Vec<u8>>> {
    let offset = slice.map_or(0, |slice| slice.offset);
    if let Some(slice) = slice
        && slice.len != expected_len
    {
        bail!("committed blob length mismatch");
    }
    let required_len = offset
        .checked_add(expected_len)
        .context("committed blob slice overflows")?;
    let metadata = fs::symlink_metadata(path)
        .with_context(|| format!("missing committed blob {}", path.display()))?;
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        bail!(
            "committed blob {} is not a safe regular file",
            path.display()
        );
    }
    validate_owner_only(path, &metadata, 0o600)?;
    let length_matches = if slice.is_some() {
        metadata.len() >= required_len
    } else {
        metadata.len() == expected_len
    };
    if !length_matches {
        bail!("committed blob length mismatch");
    }

    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    options.custom_flags(libc::O_NOFOLLOW);
    let mut file = options
        .open(path)
        .with_context(|| format!("failed to read blob {}", path.display()))?;
    let opened_metadata = file
        .metadata()
        .with_context(|| format!("failed to inspect open blob {}", path.display()))?;
    if !opened_metadata.is_file() || opened_metadata.len() != metadata.len() {
        bail!("committed blob changed while opening");
    }
    if offset > 0 {
        file.seek(std::io::SeekFrom::Start(offset))
            .with_context(|| format!("failed to seek blob {}", path.display()))?;
    }

    let mut collected = if collect {
        let capacity = usize::try_from(expected_len).context("blob is too large to read")?;
        Some(Vec::with_capacity(capacity))
    } else {
        None
    };
    let mut hasher = Sha256::new();
    let mut remaining = expected_len;
    let mut buffer = vec![0_u8; 1024 * 1024];
    while remaining > 0 {
        let want = usize::try_from(remaining.min(buffer.len() as u64))
            .context("blob read window overflow")?;
        let read = file
            .read(&mut buffer[..want])
            .with_context(|| format!("failed to read blob {}", path.display()))?;
        if read == 0 {
            bail!("committed blob length mismatch");
        }
        remaining -= read as u64;
        hasher.update(&buffer[..read]);
        if let Some(bytes) = &mut collected {
            bytes.extend_from_slice(&buffer[..read]);
        }
    }
    let actual_sha256: [u8; 32] = hasher.finalize().into();
    if actual_sha256 != expected_sha256 {
        bail!("committed blob hash mismatch");
    }
    Ok(collected)
}

fn ensure_journal_root(root: &Path) -> Result<()> {
    reject_symlink_if_present(root, "journal root")?;
    if root.exists() {
        ensure_owner_directory(root, false)
    } else {
        fs::create_dir(root)
            .with_context(|| format!("failed to create journal root {}", root.display()))?;
        set_owner_only_directory(root)?;
        if let Some(parent) = root.parent() {
            sync_directory(parent)?;
        }
        Ok(())
    }
}

fn ensure_owner_directory(path: &Path, create: bool) -> Result<()> {
    if !path.exists() {
        if !create {
            bail!("journal directory {} does not exist", path.display());
        }
        match fs::create_dir(path) {
            Ok(()) => {
                set_owner_only_directory(path)?;
                if let Some(parent) = path.parent() {
                    sync_directory(parent)?;
                }
            }
            // Concurrent preparations share a shard directory; losing the
            // create race to a sibling is success. Convergence happens below.
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
            Err(error) => {
                return Err(error).with_context(|| {
                    format!("failed to create journal directory {}", path.display())
                });
            }
        }
    }
    let mut metadata = fs::symlink_metadata(path)
        .with_context(|| format!("failed to inspect journal directory {}", path.display()))?;
    if metadata.file_type().is_symlink() {
        bail!("journal directory {} must not be a symlink", path.display());
    }
    if !metadata.is_dir() {
        bail!("journal path {} is not a directory", path.display());
    }
    // A sibling that created this directory may not have tightened its
    // permissions yet (create_dir then chmod is not atomic), and the racer
    // can observe the window either via EEXIST above or via a bare exists().
    // With create rights, converge any real directory to owner-only -- chmod
    // only ever tightens -- then let validation have the final word (a
    // foreign owner still fails).
    if create && validate_owner_only(path, &metadata, 0o700).is_err() {
        set_owner_only_directory(path)?;
        metadata = fs::symlink_metadata(path)
            .with_context(|| format!("failed to inspect journal directory {}", path.display()))?;
    }
    validate_owner_only(path, &metadata, 0o700)
}

fn reject_symlink_if_present(path: &Path, description: &str) -> Result<()> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_symlink() => {
            bail!("{description} {} must not be a symlink", path.display())
        }
        Ok(_) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error).with_context(|| format!("failed to inspect {description}")),
    }
}

fn open_owner_file(path: &Path, allow_existing: bool) -> Result<File> {
    let existed = match fs::symlink_metadata(path) {
        Ok(metadata) => {
            if metadata.file_type().is_symlink() {
                bail!("journal file {} must not be a symlink", path.display());
            }
            if !metadata.is_file() {
                bail!("journal path {} is not a regular file", path.display());
            }
            validate_owner_only(path, &metadata, 0o600)?;
            true
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => false,
        Err(error) => return Err(error).context("failed to inspect journal file"),
    };
    let mut options = OpenOptions::new();
    options.read(true).write(true);
    if allow_existing {
        options.create(true);
    } else {
        options.create_new(true);
    }
    #[cfg(unix)]
    options.mode(0o600).custom_flags(libc::O_NOFOLLOW);
    let file = options.open(path)?;
    if !existed {
        set_owner_only_file(path)?;
    }
    let metadata = file.metadata()?;
    validate_owner_only(path, &metadata, 0o600)?;
    Ok(file)
}

#[cfg(unix)]
fn set_owner_only_file(path: &Path) -> Result<()> {
    fs::set_permissions(path, fs::Permissions::from_mode(0o600))
        .with_context(|| format!("failed to chmod journal file {}", path.display()))
}

#[cfg(not(unix))]
fn set_owner_only_file(_path: &Path) -> Result<()> {
    Ok(())
}

#[cfg(unix)]
fn set_owner_only_directory(path: &Path) -> Result<()> {
    fs::set_permissions(path, fs::Permissions::from_mode(0o700))
        .with_context(|| format!("failed to chmod journal directory {}", path.display()))
}

#[cfg(not(unix))]
fn set_owner_only_directory(_path: &Path) -> Result<()> {
    Ok(())
}

fn validate_owner_only(path: &Path, metadata: &fs::Metadata, expected_mode: u32) -> Result<()> {
    super::validate_owner_only(path, metadata, expected_mode, "journal path")
}

fn remove_directory_contents(path: &Path) -> Result<()> {
    for entry in fs::read_dir(path).with_context(|| format!("failed to scan {}", path.display()))? {
        let entry = entry.context("failed to read temporary journal entry")?;
        let entry_path = entry.path();
        let metadata =
            fs::symlink_metadata(&entry_path).context("failed to inspect temporary entry")?;
        if metadata.file_type().is_symlink() || metadata.is_file() {
            fs::remove_file(&entry_path).context("failed to remove temporary file")?;
        } else if metadata.is_dir() {
            fs::remove_dir_all(&entry_path).context("failed to remove temporary directory")?;
        } else {
            bail!("temporary journal entry {} is unsafe", entry_path.display());
        }
    }
    Ok(())
}

fn sync_directory(path: impl AsRef<Path>) -> Result<()> {
    File::open(path.as_ref())
        .with_context(|| {
            format!(
                "failed to open directory {} for fsync",
                path.as_ref().display()
            )
        })?
        .sync_all()
        .with_context(|| format!("failed to fsync directory {}", path.as_ref().display()))
}

#[cfg(test)]
mod tests {
    use super::{
        FENCE_CLASSIFICATION_VERSION, FENCE_CLASSIFICATION_VERSION_KEY, Journal, JournalSnapshot,
        JournalWriteGate, META, PublicationFilesystem, REMOTE_OBJECT_VERSIONS, blob_relative_path,
        read_optional, write_value,
    };
    use crate::writeback::model::{
        FenceClass, JournalIdentity, LocalEtag, MutationKind, MutationMode, MutationRecord,
    };
    use crate::writeback::payload::VerifiedPayload;
    use bytes::Bytes;
    use redb::ReadableDatabase;
    use sha2::{Digest, Sha256};
    use std::fs;
    #[cfg(unix)]
    use std::os::unix::fs::PermissionsExt;
    use std::path::{Path, PathBuf};
    use std::sync::{Arc, Mutex};
    use std::thread;
    use std::time::{Duration, Instant};
    use tempfile::TempDir;
    use uuid::Uuid;

    struct RecordingPublicationFilesystem {
        sync_calls: Mutex<Vec<PathBuf>>,
        fail_sync_call: Option<usize>,
    }

    impl RecordingPublicationFilesystem {
        fn new(fail_sync_call: Option<usize>) -> Self {
            Self {
                sync_calls: Mutex::new(Vec::new()),
                fail_sync_call,
            }
        }
    }

    impl PublicationFilesystem for RecordingPublicationFilesystem {
        fn sync_directory(&self, path: &Path) -> anyhow::Result<()> {
            let call = {
                let mut calls = self.sync_calls.lock().unwrap();
                calls.push(path.to_path_buf());
                calls.len()
            };
            if self.fail_sync_call == Some(call) {
                anyhow::bail!("injected directory fsync failure");
            }
            super::sync_directory(path)
        }
    }

    #[test]
    fn journal_write_gate_serves_an_older_remote_waiter_before_new_local_writers() {
        let gate = Arc::new(JournalWriteGate::default());
        let held = gate.lock();
        let order = Arc::new(Mutex::new(Vec::new()));

        let remote_gate = Arc::clone(&gate);
        let remote_order = Arc::clone(&order);
        let remote = thread::spawn(move || {
            let _write = remote_gate.lock();
            remote_order.lock().unwrap().push("remote");
        });

        let deadline = Instant::now() + Duration::from_secs(2);
        loop {
            let queued = gate
                .state
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .next_ticket;
            if queued == 2 {
                break;
            }
            assert!(Instant::now() < deadline, "remote waiter did not queue");
            thread::yield_now();
        }

        let mut locals = Vec::new();
        for _ in 0..8 {
            let local_gate = Arc::clone(&gate);
            let local_order = Arc::clone(&order);
            locals.push(thread::spawn(move || {
                let _write = local_gate.lock();
                local_order.lock().unwrap().push("local");
            }));
        }
        drop(held);
        remote.join().unwrap();
        for local in locals {
            local.join().unwrap();
        }
        assert_eq!(order.lock().unwrap().first(), Some(&"remote"));
    }

    fn identity(bucket: &str) -> JournalIdentity {
        JournalIdentity {
            format_version: 1,
            bucket_id: bucket.to_owned(),
            backend_endpoint: "sftp://example.com:23".to_owned(),
            database_prefix: "zerofs/pilot".to_owned(),
            backend_kind: "sftp".to_owned(),
            encryption_key_identity_sha256: [0x22; 32],
        }
    }

    fn put_record(sequence: u64, path: &str, payload: &[u8]) -> MutationRecord {
        crate::writeback::test_util::put_record(
            sequence,
            path,
            payload,
            MutationMode::Create,
            FenceClass::ImmutableCreate,
            0x1000,
            1_786_435_200_000,
        )
    }

    fn delete_record(sequence: u64, path: &str) -> MutationRecord {
        crate::writeback::test_util::delete_record(
            sequence,
            path,
            FenceClass::Fence,
            0x2000,
            1_786_435_200_000,
        )
    }

    /// Concurrent preparations of consecutive sequences target the same shard
    /// directory, so the first-creation path must treat losing the create
    /// race as success (and still end owner-only).
    #[test]
    fn racing_shard_directory_creation_is_not_an_error() {
        let temp = tempfile::tempdir().unwrap();
        for round in 0..20 {
            let shard = temp.path().join(format!("blobs-{round}"));
            thread::scope(|scope| {
                let handles: Vec<_> = (0..4)
                    .map(|_| scope.spawn(|| super::ensure_owner_directory(&shard, true)))
                    .collect();
                for handle in handles {
                    handle.join().unwrap().unwrap();
                }
            });
            let mode = fs::symlink_metadata(&shard).unwrap().permissions().mode();
            assert_eq!(mode & 0o777, 0o700, "shard directory must end owner-only");
        }
    }

    /// Blob durability pays one directory fsync per unique shard directory in
    /// a publication batch. Contiguous sequences must therefore share a shard,
    /// or an N-record batch degenerates to N directory fsyncs.
    #[test]
    fn contiguous_batch_blobs_share_a_shard_directory() {
        let dirs: std::collections::HashSet<PathBuf> = (1000_u64..1064)
            .map(|seq| {
                blob_relative_path(seq, uuid::Uuid::from_u128(seq as u128))
                    .parent()
                    .expect("blob path has a shard parent")
                    .to_path_buf()
            })
            .collect();
        assert!(
            dirs.len() <= 2,
            "a contiguous 64-record batch fans out to {} shard directories",
            dirs.len()
        );
    }

    fn open_temp_journal(temp: &TempDir, bucket: &str) -> Journal {
        Journal::open(temp.path().join("writeback"), identity(bucket)).unwrap()
    }

    /// The file a record's blob reference names, with any container slice
    /// suffix stripped.
    fn blob_file(journal: &Journal, record: &MutationRecord) -> PathBuf {
        journal.root().join(
            super::BlobRef::parse(record.blob_path().unwrap())
                .unwrap()
                .relative,
        )
    }

    #[test]
    fn committed_put_reopens_with_verified_blob_and_contiguous_local_watermark() {
        let temp = tempfile::tempdir().unwrap();
        let journal = open_temp_journal(&temp, "bucket-a");
        let committed = journal
            .commit_put(put_record(1, "segments/1", b"payload"), b"payload")
            .unwrap();
        assert!(committed.blob_path().unwrap().starts_with("blobs/"));
        drop(journal);

        let reopened = open_temp_journal(&temp, "bucket-a");
        let snapshot: JournalSnapshot = reopened.snapshot().unwrap();
        assert_eq!(snapshot.local_seq, 1);
        assert_eq!(snapshot.remote_seq, 0);
        assert_eq!(snapshot.records, vec![committed]);
        assert_eq!(reopened.read_blob(1).unwrap(), b"payload");
    }

    #[test]
    fn reopening_a_v1_journal_normalizes_legacy_immutable_fences_before_replay() {
        let temp = tempfile::tempdir().unwrap();
        let journal = open_temp_journal(&temp, "bucket-a");
        let canonical_segment = "zerofs/pilot/segments/02/0000000000000001/0000000000000002";

        let mut overwrite = put_record(1, canonical_segment, b"overwrite");
        overwrite.kind = MutationKind::Put {
            mode: MutationMode::Overwrite,
            expected_visible_version: None,
            payload_len: 9,
            payload_sha256: Sha256::digest(b"overwrite").into(),
            blob_path: String::new(),
        };
        journal.commit_put(overwrite, b"overwrite").unwrap();

        let mut copy = put_record(2, canonical_segment, b"copy");
        copy.kind = MutationKind::Copy {
            source: "source".to_owned(),
            mode: MutationMode::Create,
            payload_len: 4,
            payload_sha256: Sha256::digest(b"copy").into(),
            blob_path: String::new(),
        };
        journal.commit_put(copy, b"copy").unwrap();

        journal
            .commit_put(
                put_record(
                    3,
                    "zerofs/pilot/uploads/segments/02/0000000000000001/0000000000000002",
                    b"malformed",
                ),
                b"malformed",
            )
            .unwrap();
        journal
            .commit_put(
                put_record(
                    4,
                    "zerofs/pilot/segments/04/0000000000000001/0000000000000004",
                    b"create",
                ),
                b"create",
            )
            .unwrap();

        // Simulate a journal created before fence-classification migrations
        // were versioned, regardless of which implementation created this fixture.
        let transaction = journal.database.begin_write().unwrap();
        {
            let mut meta = transaction.open_table(META).unwrap();
            meta.remove(FENCE_CLASSIFICATION_VERSION_KEY).unwrap();
        }
        transaction.commit().unwrap();
        drop(journal);

        let reopened = open_temp_journal(&temp, "bucket-a");
        let pending = reopened.pending_from(1, 4).unwrap();
        assert_eq!(
            pending
                .iter()
                .map(|record| record.fence)
                .collect::<Vec<_>>(),
            vec![
                FenceClass::Fence,
                FenceClass::Fence,
                FenceClass::Fence,
                FenceClass::ImmutableCreate,
            ]
        );
        assert!(pending.iter().all(|record| record.format_version == 1));
        let read = reopened.database.begin_read().unwrap();
        let meta = read.open_table(META).unwrap();
        assert_eq!(
            read_optional::<u32>(&meta, FENCE_CLASSIFICATION_VERSION_KEY).unwrap(),
            Some(FENCE_CLASSIFICATION_VERSION)
        );
        drop(meta);
        drop(read);
        drop(reopened);

        let reopened_again = open_temp_journal(&temp, "bucket-a");
        assert_eq!(
            reopened_again
                .pending_from(1, 4)
                .unwrap()
                .iter()
                .map(|record| record.fence)
                .collect::<Vec<_>>(),
            vec![
                FenceClass::Fence,
                FenceClass::Fence,
                FenceClass::Fence,
                FenceClass::ImmutableCreate,
            ]
        );
    }

    #[test]
    fn reopening_rejects_a_newer_fence_classification_version() {
        let temp = tempfile::tempdir().unwrap();
        let journal = open_temp_journal(&temp, "bucket-a");
        let transaction = journal.database.begin_write().unwrap();
        {
            let mut meta = transaction.open_table(META).unwrap();
            write_value(
                &mut meta,
                FENCE_CLASSIFICATION_VERSION_KEY,
                &(FENCE_CLASSIFICATION_VERSION + 1),
            )
            .unwrap();
        }
        transaction.commit().unwrap();
        drop(journal);

        let error = Journal::open(temp.path().join("writeback"), identity("bucket-a")).unwrap_err();
        assert!(
            format!("{error:#}").contains("fence classification version"),
            "{error:#}"
        );
    }

    #[test]
    fn v1_fence_migration_precedes_remote_version_backfill() {
        let temp = tempfile::tempdir().unwrap();
        let journal = open_temp_journal(&temp, "bucket-a");
        let path = "zerofs/pilot/segments/02/0000000000000001/0000000000000002";
        let mut overwrite = put_record(1, path, b"overwrite");
        overwrite.kind = MutationKind::Put {
            mode: MutationMode::Overwrite,
            expected_visible_version: None,
            payload_len: 9,
            payload_sha256: Sha256::digest(b"overwrite").into(),
            blob_path: String::new(),
        };
        journal.commit_put(overwrite, b"overwrite").unwrap();
        journal
            .mark_remote(1, Some("remote-etag-one".to_owned()))
            .unwrap();

        let transaction = journal.database.begin_write().unwrap();
        assert!(transaction.delete_table(REMOTE_OBJECT_VERSIONS).unwrap());
        {
            let mut meta = transaction.open_table(META).unwrap();
            meta.remove(FENCE_CLASSIFICATION_VERSION_KEY).unwrap();
        }
        transaction.commit().unwrap();
        drop(journal);

        let reopened = open_temp_journal(&temp, "bucket-a");
        assert_eq!(
            reopened.remote_object_etag(path, 1).unwrap().as_deref(),
            Some("remote-etag-one")
        );
    }

    #[test]
    fn local_payload_bytes_advance_atomically_with_the_local_watermark_and_survive_restart() {
        let temp = tempfile::tempdir().unwrap();
        let journal = open_temp_journal(&temp, "bucket-a");

        journal
            .commit_put(put_record(1, "segments/1", b"payload"), b"payload")
            .unwrap();
        let after_put = journal.progress().unwrap();
        assert_eq!(after_put.local_seq, 1);
        assert_eq!(after_put.local_bytes_completed, 7);

        journal
            .commit_metadata(delete_record(2, "obsolete"))
            .unwrap();
        let after_metadata = journal.progress().unwrap();
        assert_eq!(after_metadata.local_seq, 2);
        assert_eq!(after_metadata.local_bytes_completed, 7);

        journal
            .commit_put(put_record(3, "segments/3", b"abc"), b"abc")
            .unwrap();
        assert_eq!(journal.progress().unwrap().local_bytes_completed, 10);
        drop(journal);

        let reopened = open_temp_journal(&temp, "bucket-a");
        let progress = reopened.progress().unwrap();
        assert_eq!(progress.local_seq, 3);
        assert_eq!(progress.local_bytes_completed, 10);
    }

    #[test]
    fn prepared_batch_commits_one_contiguous_local_prefix() {
        let temp = tempfile::tempdir().unwrap();
        let journal = open_temp_journal(&temp, "bucket-a");
        let first_payload = VerifiedPayload::new(Bytes::from_static(b"one"));
        let third_payload = VerifiedPayload::new(Bytes::from_static(b"three"));
        let first = journal
            .prepare_verified_put(
                put_record(1, "segments/1", first_payload.bytes()),
                &first_payload,
            )
            .unwrap();
        let second = journal
            .prepare_metadata(delete_record(2, "obsolete"))
            .unwrap();
        let third = journal
            .prepare_verified_put(
                put_record(3, "segments/3", third_payload.bytes()),
                &third_payload,
            )
            .unwrap();

        let committed = journal.publish_batch(vec![first, second, third]).unwrap();

        assert_eq!(
            committed
                .iter()
                .map(|record| record.sequence)
                .collect::<Vec<_>>(),
            vec![1, 2, 3]
        );
        let snapshot = journal.snapshot().unwrap();
        assert_eq!(snapshot.local_seq, 3);
        assert_eq!(snapshot.local_bytes_completed, 8);
        assert_eq!(snapshot.records, committed);
        assert_eq!(snapshot.pending_blob_count, 0);
        assert_eq!(journal.read_blob(1).unwrap(), b"one");
        assert_eq!(journal.read_blob(3).unwrap(), b"three");
    }

    #[test]
    fn prepared_batch_rejects_out_of_order_records_without_advancing_watermark() {
        let temp = tempfile::tempdir().unwrap();
        let journal = open_temp_journal(&temp, "bucket-a");
        let first = journal
            .prepare_metadata(delete_record(1, "obsolete-1"))
            .unwrap();
        let second = journal
            .prepare_metadata(delete_record(2, "obsolete-2"))
            .unwrap();

        let error = journal.publish_batch(vec![second, first]).unwrap_err();

        assert!(format!("{error:#}").contains("contiguous"), "{error:#}");
        let snapshot = journal.snapshot().unwrap();
        assert_eq!(snapshot.local_seq, 0);
        assert!(snapshot.records.is_empty());
    }

    #[test]
    fn prepared_batch_rejects_sequence_gap_and_discards_temporary_blobs() {
        let temp = tempfile::tempdir().unwrap();
        let journal = open_temp_journal(&temp, "bucket-a");
        let payload = VerifiedPayload::new(Bytes::from_static(b"payload"));
        let first = journal
            .prepare_verified_put(put_record(1, "segments/1", b"payload"), &payload)
            .unwrap();
        let third = journal
            .prepare_verified_put(put_record(3, "segments/3", b"payload"), &payload)
            .unwrap();

        let error = journal.publish_batch(vec![first, third]).unwrap_err();

        assert!(format!("{error:#}").contains("contiguous"), "{error:#}");
        let snapshot = journal.snapshot().unwrap();
        assert_eq!(snapshot.local_seq, 0);
        assert!(snapshot.records.is_empty());
        assert_eq!(snapshot.pending_blob_count, 0);
        assert_eq!(fs::read_dir(journal.root().join("tmp")).unwrap().count(), 0);
    }

    #[test]
    fn prepared_batch_rejects_duplicate_operation_ids_before_publication() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("writeback");
        let journal = Journal::open(&root, identity("bucket-a")).unwrap();
        let payload = VerifiedPayload::new(Bytes::from_static(b"payload"));
        let first_record = put_record(1, "segments/1", b"payload");
        let first = journal
            .prepare_verified_put(first_record.clone(), &payload)
            .unwrap();
        let mut second_record = delete_record(2, "obsolete");
        second_record.operation_id = first_record.operation_id;
        let second = journal.prepare_metadata(second_record).unwrap();

        let error = journal.publish_batch(vec![first, second]).unwrap_err();

        assert!(
            format!("{error:#}").contains("duplicate operation ID"),
            "{error:#}"
        );
        let snapshot = journal.snapshot().unwrap();
        assert_eq!(snapshot.local_seq, 0);
        assert_eq!(snapshot.pending_blob_count, 0);
        assert_eq!(fs::read_dir(journal.root().join("tmp")).unwrap().count(), 0);
        drop(journal);

        let recovered = Journal::open(&root, identity("bucket-a")).unwrap();
        assert_eq!(recovered.progress().unwrap().local_seq, 0);
        assert!(recovered.snapshot().unwrap().records.is_empty());
    }

    /// Blob references are minted by publication, not carried in from
    /// preparation, so a stale reference on a prepared record cannot alias
    /// another member's bytes: the batch overwrites it with its own slice.
    #[test]
    fn publication_overwrites_any_blob_reference_a_prepared_record_arrived_with() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("writeback");
        let journal = Journal::open(&root, identity("bucket-a")).unwrap();
        let first_payload = VerifiedPayload::new(Bytes::from_static(b"one"));
        let second_payload = VerifiedPayload::new(Bytes::from_static(b"two"));
        let mut first = journal
            .prepare_verified_put(put_record(1, "segments/1", b"one"), &first_payload)
            .unwrap();
        *first.record.blob_path_mut().unwrap() = "blobs/00/stale.blob".to_owned();
        let mut second = journal
            .prepare_verified_put(put_record(2, "segments/2", b"two"), &second_payload)
            .unwrap();
        *second.record.blob_path_mut().unwrap() = "blobs/00/stale.blob".to_owned();

        let committed = journal.publish_batch(vec![first, second]).unwrap();

        assert!(!root.join("blobs/00/stale.blob").exists());
        assert_ne!(committed[0].blob_path(), committed[1].blob_path());
        assert_eq!(journal.read_blob(1).unwrap(), b"one");
        assert_eq!(journal.read_blob(2).unwrap(), b"two");
        assert_eq!(journal.snapshot().unwrap().pending_blob_count, 0);
        drop(journal);

        let recovered = Journal::open(&root, identity("bucket-a")).unwrap();
        assert_eq!(recovered.progress().unwrap().local_seq, 2);
        assert_eq!(recovered.read_blob(2).unwrap(), b"two");
    }

    #[test]
    fn prepared_batch_can_commit_the_final_u64_sequence() {
        let temp = tempfile::tempdir().unwrap();
        let journal = open_temp_journal(&temp, "bucket-a");
        let transaction = journal.database.begin_write().unwrap();
        {
            let mut meta = transaction.open_table(super::META).unwrap();
            super::write_value(&mut meta, super::LOCAL_SEQ_KEY, &(u64::MAX - 1)).unwrap();
            super::write_value(&mut meta, super::REMOTE_SEQ_KEY, &(u64::MAX - 1)).unwrap();
        }
        transaction.commit().unwrap();
        let mut record = delete_record(0, "last");
        record.sequence = u64::MAX;
        record.local_etag = LocalEtag::new(Uuid::nil(), u64::MAX);
        let prepared = journal.prepare_metadata(record).unwrap();

        journal.publish_batch(vec![prepared]).unwrap();

        assert_eq!(journal.progress().unwrap().local_seq, u64::MAX);
    }

    #[test]
    fn container_write_failure_fails_the_whole_batch() {
        let temp = tempfile::tempdir().unwrap();
        let journal = open_temp_journal(&temp, "bucket-a");
        // The batch writes one container, so parking a directory on its name
        // fails the whole batch -- no member can land without the others.
        let container = journal.root().join(super::container_relative_path(1, 2));
        let first_payload = VerifiedPayload::new(Bytes::from_static(b"one"));
        let second_payload = VerifiedPayload::new(Bytes::from_static(b"two"));
        let first = journal
            .prepare_verified_put(put_record(1, "segments/1", b"one"), &first_payload)
            .unwrap();
        let second = journal
            .prepare_verified_put(put_record(2, "segments/2", b"two"), &second_payload)
            .unwrap();
        fs::create_dir_all(&container).unwrap();

        let error = journal.publish_batch(vec![first, second]).unwrap_err();

        assert!(
            format!("{error:#}").contains("failed to create container"),
            "{error:#}"
        );
        assert!(container.is_dir());
        let snapshot = journal.snapshot().unwrap();
        assert_eq!(snapshot.local_seq, 0);
        assert!(snapshot.records.is_empty());
        assert_eq!(snapshot.pending_blob_count, 0);
        assert_eq!(fs::read_dir(journal.root().join("tmp")).unwrap().count(), 0);
    }

    #[test]
    fn partial_batch_fsync_failure_keeps_watermark_old_and_rolls_back_blobs() {
        let temp = tempfile::tempdir().unwrap();
        let journal = open_temp_journal(&temp, "bucket-a");
        let container = journal.root().join(super::container_relative_path(1, 2));
        let first = journal
            .prepare_verified_put(
                put_record(1, "segments/1", b"one"),
                &VerifiedPayload::new(Bytes::from_static(b"one")),
            )
            .unwrap();
        let second = journal
            .prepare_verified_put(
                put_record(2, "segments/2", b"two"),
                &VerifiedPayload::new(Bytes::from_static(b"two")),
            )
            .unwrap();
        // The batch publishes one container into one shard directory, so it
        // makes exactly one directory fsync; fail it.
        let filesystem = RecordingPublicationFilesystem::new(Some(1));

        let error = journal
            .publish_batch_with(vec![first, second], &filesystem)
            .unwrap_err();

        assert!(
            format!("{error:#}").contains("injected directory fsync failure"),
            "{error:#}"
        );
        assert!(!container.exists());
        let snapshot = journal.snapshot().unwrap();
        assert_eq!(snapshot.local_seq, 0);
        assert!(snapshot.records.is_empty());
        assert_eq!(snapshot.pending_blob_count, 0);
    }

    /// How much of the journal's durability cost is fixed per publication
    /// batch rather than per record. Run with
    /// `cargo test --release --lib publication_batch_size_amortizes -- --ignored --nocapture`.
    ///
    /// Records are built before the clock starts: constructing one hashes its
    /// payload, and at these sizes a single core's SHA-256 rate would
    /// otherwise dominate the measurement instead of the durability path.
    #[test]
    #[ignore = "throughput benchmark; needs a real disk and --release"]
    fn publication_batch_size_amortizes_the_journal_fixed_cost() {
        const TOTAL_BYTES: usize = 128 * 1024 * 1024;
        for payload_bytes in [64 * 1024_usize, 256 * 1024, 1024 * 1024] {
            for batch in [1_usize, 8, 64, 512] {
                let records = (TOTAL_BYTES / payload_bytes).min(batch * 64);
                let temp = tempfile::tempdir().unwrap();
                let journal = open_temp_journal(&temp, "bucket-a");
                let payload = vec![0x5a_u8; payload_bytes];
                let verified = VerifiedPayload::new(Bytes::from(payload.clone()));
                let prebuilt = (1..=records as u64)
                    .map(|sequence| put_record(sequence, &format!("segments/{sequence}"), &payload))
                    .collect::<Vec<_>>();

                let started = Instant::now();
                for chunk in prebuilt.chunks(batch) {
                    let prepared = chunk
                        .iter()
                        .map(|record| {
                            journal
                                .prepare_verified_put(record.clone(), &verified)
                                .unwrap()
                        })
                        .collect::<Vec<_>>();
                    journal.publish_batch(prepared).unwrap();
                }
                let elapsed = started.elapsed();
                let total = (records * payload_bytes) as f64 / (1024.0 * 1024.0);
                println!(
                    "payload={:>4} KiB batch={batch:>3}: {:.3} ms/record, {:>7.1} MiB/s \
                     ({records} records, {total:.1} MiB in {:.3}s)",
                    payload_bytes / 1024,
                    elapsed.as_secs_f64() * 1000.0 / records as f64,
                    total / elapsed.as_secs_f64(),
                    elapsed.as_secs_f64(),
                );
            }
        }
    }

    #[test]
    fn prepared_batch_fsyncs_each_unique_blob_directory_once() {
        let temp = tempfile::tempdir().unwrap();
        let journal = open_temp_journal(&temp, "bucket-a");
        let payload = VerifiedPayload::new(Bytes::from_static(b"payload"));
        // Contiguous sequences shard into one blobs directory, so a batch of
        // payload blobs must cost exactly one directory fsync, not one each.
        let prepared: Vec<_> = (1..=4)
            .map(|sequence| {
                journal
                    .prepare_verified_put(
                        put_record(sequence, &format!("segments/{sequence}"), b"payload"),
                        &payload,
                    )
                    .unwrap()
            })
            .collect();
        let filesystem = RecordingPublicationFilesystem::new(None);

        journal.publish_batch_with(prepared, &filesystem).unwrap();

        assert_eq!(
            *filesystem.sync_calls.lock().unwrap(),
            vec![journal.root().join("blobs/00")]
        );
        assert_eq!(journal.progress().unwrap().local_seq, 4);
    }

    #[test]
    fn opening_a_pre_counter_journal_restores_the_monotonic_local_payload_total() {
        let temp = tempfile::tempdir().unwrap();
        let journal = open_temp_journal(&temp, "bucket-a");
        journal
            .commit_put(put_record(1, "segments/1", b"payload"), b"payload")
            .unwrap();
        journal
            .commit_metadata(delete_record(2, "obsolete"))
            .unwrap();
        let transaction = journal.database.begin_write().unwrap();
        {
            let mut meta = transaction.open_table(super::META).unwrap();
            meta.remove(super::LOCAL_BYTES_COMPLETED_KEY).unwrap();
        }
        transaction.commit().unwrap();
        drop(journal);

        let reopened = open_temp_journal(&temp, "bucket-a");

        assert_eq!(reopened.progress().unwrap().local_bytes_completed, 7);
    }

    #[test]
    fn metadata_mutation_commits_without_a_blob() {
        let temp = tempfile::tempdir().unwrap();
        let journal = open_temp_journal(&temp, "bucket-a");

        let committed = journal
            .commit_metadata(delete_record(1, "obsolete"))
            .unwrap();

        let snapshot = journal.snapshot().unwrap();
        assert_eq!(snapshot.local_seq, 1);
        assert_eq!(snapshot.dirty_blob_bytes, 0);
        assert_eq!(
            snapshot.dirty_metadata_reserved_bytes,
            MutationRecord::metadata_ssd_reservation("obsolete").unwrap()
        );
        assert_eq!(snapshot.records, vec![committed]);
    }

    #[test]
    fn pending_from_returns_only_the_requested_contiguous_window() {
        let temp = tempfile::tempdir().unwrap();
        let journal = open_temp_journal(&temp, "bucket-a");
        for sequence in 1..=32 {
            journal
                .commit_metadata(delete_record(sequence, &format!("obsolete-{sequence}")))
                .unwrap();
        }

        let pending = journal.pending_from(17, 4).unwrap();

        assert_eq!(
            pending
                .iter()
                .map(|record| record.sequence)
                .collect::<Vec<_>>(),
            vec![17, 18, 19, 20]
        );
    }

    #[test]
    fn progress_reads_watermarks_without_materializing_pending_records() {
        let temp = tempfile::tempdir().unwrap();
        let journal = open_temp_journal(&temp, "bucket-a");
        for sequence in 1..=32 {
            journal
                .commit_metadata(delete_record(sequence, &format!("obsolete-{sequence}")))
                .unwrap();
        }

        let progress = journal.progress().unwrap();

        assert_eq!(progress.local_seq, 32);
        assert_eq!(progress.remote_seq, 0);
        assert_eq!(progress.remote_bytes_completed, 0);
        assert_eq!(progress.remote_retries, 0);
    }

    #[test]
    fn journal_rejects_noncontiguous_local_sequences() {
        let temp = tempfile::tempdir().unwrap();
        let journal = open_temp_journal(&temp, "bucket-a");

        let error = journal
            .commit_metadata(delete_record(2, "skipped-one"))
            .unwrap_err();

        assert!(format!("{error:#}").contains("contiguous"));
        assert_eq!(journal.snapshot().unwrap().local_seq, 0);
    }

    #[test]
    fn rejected_prepared_sequence_does_not_leak_temporary_bytes() {
        let temp = tempfile::tempdir().unwrap();
        let journal = open_temp_journal(&temp, "bucket-a");
        let payload = VerifiedPayload::new(Bytes::from_static(b"payload"));
        let prepared = journal
            .prepare_verified_put(put_record(2, "segments/2", b"payload"), &payload)
            .unwrap();
        // Preparation is pure: a record's container slice is not known until
        // its batch is assembled, so nothing is staged before publication.
        assert_eq!(fs::read_dir(journal.root().join("tmp")).unwrap().count(), 0);

        let error = journal.publish_prepared(prepared).unwrap_err();

        assert!(format!("{error:#}").contains("contiguous"));
        assert_eq!(fs::read_dir(journal.root().join("tmp")).unwrap().count(), 0);
        assert_eq!(journal.snapshot().unwrap().pending_blob_count, 0);
        assert_eq!(journal.snapshot().unwrap().local_seq, 0);
    }

    #[test]
    fn journal_rejects_a_mutation_from_another_format_version() {
        let temp = tempfile::tempdir().unwrap();
        let journal = open_temp_journal(&temp, "bucket-a");
        let mut record = delete_record(1, "obsolete");
        record.format_version = 2;

        let error = journal.commit_metadata(record).unwrap_err();

        assert!(format!("{error:#}").contains("format version"), "{error:#}");
        assert_eq!(journal.snapshot().unwrap().local_seq, 0);
    }

    #[test]
    fn second_writer_cannot_open_the_same_journal() {
        let temp = tempfile::tempdir().unwrap();
        let first = open_temp_journal(&temp, "bucket-a");

        let error = Journal::open(temp.path().join("writeback"), identity("bucket-a")).unwrap_err();

        assert!(format!("{error:#}").contains("already locked"));
        drop(first);
        open_temp_journal(&temp, "bucket-a");
    }

    #[test]
    fn reopening_with_a_different_identity_fails_closed() {
        let temp = tempfile::tempdir().unwrap();
        drop(open_temp_journal(&temp, "bucket-a"));

        let error = Journal::open(temp.path().join("writeback"), identity("bucket-b")).unwrap_err();

        assert!(format!("{error:#}").contains("identity mismatch"));
    }

    #[test]
    fn reopening_rejects_a_missing_committed_blob() {
        let temp = tempfile::tempdir().unwrap();
        let journal = open_temp_journal(&temp, "bucket-a");
        let committed = journal
            .commit_put(put_record(1, "segments/1", b"payload"), b"payload")
            .unwrap();
        let blob = blob_file(&journal, &committed);
        drop(journal);
        fs::remove_file(blob).unwrap();

        let error = Journal::open(temp.path().join("writeback"), identity("bucket-a")).unwrap_err();

        assert!(format!("{error:#}").contains("missing committed blob"));
    }

    #[test]
    fn reopening_rejects_a_corrupt_committed_blob() {
        let temp = tempfile::tempdir().unwrap();
        let journal = open_temp_journal(&temp, "bucket-a");
        let committed = journal
            .commit_put(put_record(1, "segments/1", b"payload"), b"payload")
            .unwrap();
        let blob = blob_file(&journal, &committed);
        drop(journal);
        fs::write(blob, b"payloae").unwrap();

        let error = Journal::open(temp.path().join("writeback"), identity("bucket-a")).unwrap_err();

        assert!(format!("{error:#}").contains("hash mismatch"));
    }

    #[test]
    fn reopening_removes_only_uncommitted_tmp_files() {
        let temp = tempfile::tempdir().unwrap();
        let journal = open_temp_journal(&temp, "bucket-a");
        let tmp_file = journal.root().join("tmp/abandoned");
        fs::write(&tmp_file, b"partial").unwrap();
        let committed = journal
            .commit_put(put_record(1, "segments/1", b"good"), b"good")
            .unwrap();
        let blob = blob_file(&journal, &committed);
        drop(journal);

        let reopened = open_temp_journal(&temp, "bucket-a");

        assert!(!tmp_file.exists());
        assert!(blob.exists());
        assert_eq!(reopened.read_blob(1).unwrap(), b"good");
    }

    #[test]
    fn remote_watermark_is_contiguous_and_cleanup_preserves_it() {
        let temp = tempfile::tempdir().unwrap();
        let journal = open_temp_journal(&temp, "bucket-a");
        let first = journal
            .commit_put(put_record(1, "segments/1", b"one"), b"one")
            .unwrap();
        journal
            .commit_metadata(delete_record(2, "obsolete"))
            .unwrap();

        let gap = journal.mark_remote(2, None).unwrap_err();
        assert!(format!("{gap:#}").contains("contiguous"));
        journal.mark_remote(1, Some("etag-one".to_owned())).unwrap();
        journal.mark_remote(2, None).unwrap();
        journal.remove_remote_prefix(2).unwrap();

        let snapshot = journal.snapshot().unwrap();
        assert_eq!(snapshot.local_seq, 2);
        assert_eq!(snapshot.remote_seq, 2);
        assert!(snapshot.records.is_empty());
        assert!(!blob_file(&journal, &first).exists());
    }

    #[test]
    fn remote_result_etag_larger_than_the_persisted_bound_is_rejected_atomically() {
        let temp = tempfile::tempdir().unwrap();
        let journal = open_temp_journal(&temp, "bucket-a");
        journal
            .commit_put(put_record(1, "segments/1", b"one"), b"one")
            .unwrap();

        let error = journal
            .mark_remote(1, Some("e".repeat(4_097)))
            .expect_err("oversized backend ETag must not enter either journal table");

        assert!(format!("{error:#}").contains("ETag"));
        assert_eq!(journal.progress().unwrap().remote_seq, 0);
        assert!(
            journal
                .mutation(1)
                .unwrap()
                .unwrap()
                .remote_result_etag
                .is_none()
        );
        assert!(
            journal
                .remote_object_etag("segments/1", 1)
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn explicit_reseed_repairs_a_pruned_predecessor_without_overwriting_conflicts() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("writeback");
        let journal = open_temp_journal(&temp, "bucket-a");
        journal
            .commit_put(put_record(1, "gc/manifest.boundary", b"one"), b"one")
            .unwrap();
        journal
            .mark_remote(1, Some("remote-etag-one".to_owned()))
            .unwrap();
        journal.remove_remote_prefix(1).unwrap();
        let transaction = journal.database.begin_write().unwrap();
        assert!(transaction.delete_table(REMOTE_OBJECT_VERSIONS).unwrap());
        transaction.commit().unwrap();
        drop(journal);

        let reopened = Journal::open_existing(&root).unwrap();
        assert!(
            reopened
                .remote_object_etag("gc/manifest.boundary", 1)
                .unwrap()
                .is_none()
        );
        reopened
            .seed_remote_object_etag("gc/manifest.boundary", 1, "remote-etag-one")
            .unwrap();
        reopened
            .seed_remote_object_etag("gc/manifest.boundary", 1, "remote-etag-one")
            .unwrap();
        assert_eq!(
            reopened
                .remote_object_etag("gc/manifest.boundary", 1)
                .unwrap()
                .as_deref(),
            Some("remote-etag-one")
        );
        let conflict = reopened
            .seed_remote_object_etag("gc/manifest.boundary", 1, "different")
            .unwrap_err();
        assert!(format!("{conflict:#}").contains("already seeded"));
        let future = reopened
            .seed_remote_object_etag("other", 2, "future")
            .unwrap_err();
        assert!(format!("{future:#}").contains("above remote watermark"));
    }

    #[test]
    fn opening_a_pre_version_table_journal_backfills_preserved_remote_results() {
        let temp = tempfile::tempdir().unwrap();
        let journal = open_temp_journal(&temp, "bucket-a");
        let path = "manifest/current";
        let mut predecessor = put_record(1, path, b"one");
        predecessor.fence = FenceClass::Fence;
        journal.commit_put(predecessor, b"one").unwrap();
        let mut chained = put_record(2, path, b"two");
        chained.fence = FenceClass::Fence;
        chained.kind = MutationKind::Put {
            mode: MutationMode::Update,
            expected_visible_version: Some(LocalEtag::new(Uuid::nil(), 1).as_str().to_owned()),
            payload_len: 3,
            payload_sha256: Sha256::digest(b"two").into(),
            blob_path: String::new(),
        };
        journal.commit_put(chained, b"two").unwrap();
        journal
            .mark_remote(1, Some("remote-etag-one".to_owned()))
            .unwrap();
        let transaction = journal.database.begin_write().unwrap();
        assert!(transaction.delete_table(REMOTE_OBJECT_VERSIONS).unwrap());
        transaction.commit().unwrap();
        drop(journal);

        let reopened = open_temp_journal(&temp, "bucket-a");

        assert_eq!(
            reopened.remote_object_etag(path, 1).unwrap().as_deref(),
            Some("remote-etag-one")
        );
    }

    #[test]
    fn failed_container_publication_leaves_no_intent_or_stray_bytes() {
        let temp = tempfile::tempdir().unwrap();
        let journal = open_temp_journal(&temp, "bucket-a");
        let record = put_record(1, "segments/1", b"payload");
        // Block the rename by parking a directory on the container's name.
        let final_path = journal.root().join(super::container_relative_path(1, 1));
        fs::create_dir_all(&final_path).unwrap();
        fs::set_permissions(
            final_path.parent().unwrap(),
            fs::Permissions::from_mode(0o700),
        )
        .unwrap();

        let error = journal.commit_put(record, b"payload").unwrap_err();

        assert!(
            format!("{error:#}").contains("failed to create container"),
            "{error:#}"
        );
        assert_eq!(journal.snapshot().unwrap().pending_blob_count, 0);
        assert_eq!(fs::read_dir(journal.root().join("tmp")).unwrap().count(), 0);
    }

    #[test]
    fn restart_finishes_cleanup_after_remote_watermark_commit() {
        let temp = tempfile::tempdir().unwrap();
        let journal = open_temp_journal(&temp, "bucket-a");
        let committed = journal
            .commit_put(put_record(1, "segments/1", b"one"), b"one")
            .unwrap();
        let blob = blob_file(&journal, &committed);
        journal.mark_remote(1, Some("etag-one".to_owned())).unwrap();
        drop(journal);

        let reopened = open_temp_journal(&temp, "bucket-a");

        let snapshot = reopened.snapshot().unwrap();
        assert_eq!(snapshot.local_seq, 1);
        assert_eq!(snapshot.remote_seq, 1);
        assert!(snapshot.records.is_empty());
        assert!(!blob.exists());
    }

    #[cfg(unix)]
    #[test]
    fn journal_directories_and_blob_are_owner_only() {
        let temp = tempfile::tempdir().unwrap();
        let journal = open_temp_journal(&temp, "bucket-a");
        let committed = journal
            .commit_put(put_record(1, "segments/1", b"secret"), b"secret")
            .unwrap();

        let directories = [
            journal.root().to_path_buf(),
            journal.root().join("blobs"),
            journal.root().join("tmp"),
        ];
        for directory in directories {
            assert_eq!(
                fs::metadata(directory).unwrap().permissions().mode() & 0o777,
                0o700
            );
        }
        assert_eq!(
            fs::metadata(blob_file(&journal, &committed))
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o600
        );
        assert_eq!(
            fs::metadata(journal.root().join("journal.redb"))
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o600
        );
    }

    #[cfg(unix)]
    #[test]
    fn journal_rejects_a_symlink_root() {
        let temp = tempfile::tempdir().unwrap();
        let actual = temp.path().join("actual");
        fs::create_dir(&actual).unwrap();
        let link = temp.path().join("writeback");
        std::os::unix::fs::symlink(&actual, &link).unwrap();

        let error = Journal::open(&link, identity("bucket-a")).unwrap_err();

        assert!(format!("{error:#}").contains("symlink"));
    }

    #[cfg(unix)]
    #[test]
    fn journal_rejects_a_dangling_symlink_root() {
        let temp = tempfile::tempdir().unwrap();
        let link = temp.path().join("writeback");
        std::os::unix::fs::symlink(temp.path().join("missing"), &link).unwrap();

        let error = Journal::open(&link, identity("bucket-a")).unwrap_err();

        assert!(format!("{error:#}").contains("symlink"), "{error:#}");
    }

    #[cfg(unix)]
    #[test]
    fn journal_rejects_a_permissive_existing_lock_file() {
        let temp = tempfile::tempdir().unwrap();
        let journal = open_temp_journal(&temp, "bucket-a");
        let lock = journal.root().join("LOCK");
        drop(journal);
        fs::set_permissions(&lock, fs::Permissions::from_mode(0o644)).unwrap();

        let error = Journal::open(temp.path().join("writeback"), identity("bucket-a")).unwrap_err();

        assert!(format!("{error:#}").contains("mode 644"), "{error:#}");
    }

    // --- batch container blobs -------------------------------------------

    fn published_blob_files(journal: &Journal) -> Vec<PathBuf> {
        let mut files = Vec::new();
        for shard in fs::read_dir(journal.root().join("blobs")).unwrap() {
            for blob in fs::read_dir(shard.unwrap().path()).unwrap() {
                files.push(blob.unwrap().path());
            }
        }
        files.sort();
        files
    }

    fn prepare_puts(
        journal: &Journal,
        sequences: std::ops::RangeInclusive<u64>,
    ) -> Vec<super::PreparedMutation> {
        sequences
            .map(|sequence| {
                let payload = format!("payload-{sequence}");
                let verified = VerifiedPayload::new(Bytes::from(payload.clone().into_bytes()));
                journal
                    .prepare_verified_put(
                        put_record(
                            sequence,
                            &format!("segments/{sequence}"),
                            payload.as_bytes(),
                        ),
                        &verified,
                    )
                    .unwrap()
            })
            .collect()
    }

    #[test]
    fn blob_reference_parses_the_container_slice_and_the_legacy_whole_file_form() {
        let legacy =
            super::BlobRef::parse("blobs/07/00000000-0000-0000-0000-000000000001.blob").unwrap();
        assert_eq!(
            legacy.relative,
            "blobs/07/00000000-0000-0000-0000-000000000001.blob"
        );
        assert_eq!(legacy.slice, None);

        let container =
            super::BlobRef::parse("blobs/07/0000000000000701-0000000000000740.blobs#4096+128")
                .unwrap();
        assert_eq!(
            container.relative,
            "blobs/07/0000000000000701-0000000000000740.blobs"
        );
        assert_eq!(
            container.slice,
            Some(super::BlobSlice {
                offset: 4096,
                len: 128
            })
        );

        for malformed in [
            "blobs/07/x.blobs#",
            "blobs/07/x.blobs#12",
            "blobs/07/x.blobs#12+",
            "blobs/07/x.blobs#a+1",
            "blobs/07/x.blobs#1+2#3",
        ] {
            assert!(
                super::BlobRef::parse(malformed).is_err(),
                "{malformed} must not parse"
            );
        }
    }

    /// The point of the container: one publication batch writes exactly one
    /// blob file, no matter how many payload records it carries.
    #[test]
    fn publication_batch_packs_every_payload_into_one_container_blob() {
        let temp = tempfile::tempdir().unwrap();
        let journal = open_temp_journal(&temp, "bucket-a");

        let committed = journal
            .publish_batch(prepare_puts(&journal, 1..=8))
            .unwrap();

        let files = published_blob_files(&journal);
        assert_eq!(
            files.len(),
            1,
            "a batch must publish one container: {files:?}"
        );
        assert_eq!(
            files[0],
            journal
                .root()
                .join("blobs/00/0000000000000001-0000000000000008.blobs"),
            "the container is named for its contiguous sequence range"
        );
        let mut offsets = Vec::new();
        for record in &committed {
            let reference = super::BlobRef::parse(record.blob_path().unwrap()).unwrap();
            assert_eq!(
                reference.relative,
                "blobs/00/0000000000000001-0000000000000008.blobs"
            );
            offsets.push(reference.slice.unwrap().offset);
        }
        let mut sorted = offsets.clone();
        sorted.sort_unstable();
        sorted.dedup();
        assert_eq!(
            sorted.len(),
            8,
            "every member needs its own slice: {offsets:?}"
        );
        for sequence in 1..=8_u64 {
            assert_eq!(
                journal.read_blob(sequence).unwrap(),
                format!("payload-{sequence}").into_bytes()
            );
        }
        assert_eq!(journal.progress().unwrap().local_seq, 8);
    }

    /// Metadata-only records carry no payload, so a batch mixing them with
    /// puts must still land exactly one container holding only the payloads.
    #[test]
    fn container_publication_skips_metadata_only_records() {
        let temp = tempfile::tempdir().unwrap();
        let journal = open_temp_journal(&temp, "bucket-a");
        let payload = VerifiedPayload::new(Bytes::from_static(b"one"));
        let first = journal
            .prepare_verified_put(put_record(1, "segments/1", b"one"), &payload)
            .unwrap();
        let second = journal
            .prepare_metadata(delete_record(2, "obsolete"))
            .unwrap();
        let third = journal
            .prepare_verified_put(
                put_record(3, "segments/3", b"three"),
                &VerifiedPayload::new(Bytes::from_static(b"three")),
            )
            .unwrap();

        let committed = journal.publish_batch(vec![first, second, third]).unwrap();

        assert_eq!(published_blob_files(&journal).len(), 1);
        assert_eq!(committed[1].blob_path(), None);
        assert_eq!(journal.read_blob(1).unwrap(), b"one");
        assert_eq!(journal.read_blob(3).unwrap(), b"three");
        let container = journal.root().join(
            super::BlobRef::parse(committed[0].blob_path().unwrap())
                .unwrap()
                .relative,
        );
        assert_eq!(
            fs::metadata(&container).unwrap().len(),
            8,
            "the container holds only payload bytes"
        );
    }

    /// A batch that crashed between the container rename and the record
    /// commit leaves an orphan container plus its intent. Recovery must
    /// delete the orphan and leave the watermark where it was.
    #[test]
    fn reopening_deletes_a_container_orphaned_by_a_crash_before_the_record_commit() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("writeback");
        let journal = Journal::open(&root, identity("bucket-a")).unwrap();
        journal
            .publish_batch(prepare_puts(&journal, 1..=2))
            .unwrap();

        // Reproduce the crash window: the next batch's container is renamed
        // into place and its intent is durable, but its records never commit.
        let orphan_relative = "blobs/00/0000000000000003-0000000000000004.blobs";
        let orphan = root.join(orphan_relative);
        fs::write(&orphan, b"orphaned-container-bytes").unwrap();
        fs::set_permissions(&orphan, fs::Permissions::from_mode(0o600)).unwrap();
        let transaction = journal.database.begin_write().unwrap();
        {
            let mut table = transaction.open_table(super::PENDING_BLOBS).unwrap();
            table
                .insert(orphan_relative, orphan_relative.as_bytes())
                .unwrap();
        }
        transaction.commit().unwrap();
        drop(journal);

        let recovered = Journal::open(&root, identity("bucket-a")).unwrap();

        assert!(!orphan.exists(), "the orphaned container must be deleted");
        let snapshot = recovered.snapshot().unwrap();
        assert_eq!(snapshot.local_seq, 2);
        assert_eq!(snapshot.pending_blob_count, 0);
        assert_eq!(recovered.read_blob(1).unwrap(), b"payload-1");
        assert_eq!(recovered.read_blob(2).unwrap(), b"payload-2");
    }

    /// The production crash shape: a container is written straight to its
    /// final name, so an interrupted batch leaves a possibly-torn file under
    /// `blobs/` with no intent row anywhere. Its name puts it above the local
    /// watermark, which is the whole signal recovery needs.
    #[test]
    fn reopening_discards_a_partially_written_container_above_the_local_watermark() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("writeback");
        let journal = Journal::open(&root, identity("bucket-a")).unwrap();
        let committed = journal
            .publish_batch(prepare_puts(&journal, 1..=3))
            .unwrap();
        let container = root.join(
            super::BlobRef::parse(committed[0].blob_path().unwrap())
                .unwrap()
                .relative,
        );
        let torn = root.join(super::container_relative_path(4, 6));
        fs::write(&torn, b"half-written").unwrap();
        fs::set_permissions(&torn, fs::Permissions::from_mode(0o600)).unwrap();
        assert_eq!(journal.snapshot().unwrap().pending_blob_count, 0);
        drop(journal);

        let recovered = Journal::open(&root, identity("bucket-a")).unwrap();

        assert!(!torn.exists(), "the torn container must be unlinked");
        assert!(container.exists());
        assert_eq!(recovered.progress().unwrap().local_seq, 3);
        assert_eq!(recovered.read_blob(2).unwrap(), b"payload-2");
    }

    /// Publication records no pending-blob intent at all: the container's
    /// name is the durable evidence, so the batch's critical path carries no
    /// extra immediate commit.
    #[test]
    fn publication_records_no_pending_blob_intent() {
        let temp = tempfile::tempdir().unwrap();
        let journal = open_temp_journal(&temp, "bucket-a");

        journal
            .publish_batch(prepare_puts(&journal, 1..=4))
            .unwrap();

        assert_eq!(journal.snapshot().unwrap().pending_blob_count, 0);
        assert_eq!(fs::read_dir(journal.root().join("tmp")).unwrap().count(), 0);
    }

    /// Recovery must not confuse a container that merely reaches the
    /// watermark with one that overshoots it: the committed batch stays, and
    /// the interrupted one starting at the very next sequence goes.
    #[test]
    fn reopening_keeps_the_container_that_ends_exactly_at_the_local_watermark() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("writeback");
        let journal = Journal::open(&root, identity("bucket-a")).unwrap();
        let committed = journal
            .publish_batch(prepare_puts(&journal, 1..=4))
            .unwrap();
        let container = root.join(
            super::BlobRef::parse(committed[0].blob_path().unwrap())
                .unwrap()
                .relative,
        );
        let interrupted = root.join(super::container_relative_path(5, 5));
        fs::write(&interrupted, b"x").unwrap();
        fs::set_permissions(&interrupted, fs::Permissions::from_mode(0o600)).unwrap();
        drop(journal);

        let recovered = Journal::open(&root, identity("bucket-a")).unwrap();

        assert!(container.exists(), "the committed container must survive");
        assert!(!interrupted.exists());
        assert_eq!(recovered.read_blob(4).unwrap(), b"payload-4");
    }

    /// Journals written before containers store one whole-file blob per
    /// record. Those references must keep validating, reading, and pruning.
    #[test]
    fn a_legacy_per_record_blob_reference_still_validates_reads_and_prunes() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("writeback");
        let journal = Journal::open(&root, identity("bucket-a")).unwrap();

        // Hand-build the pre-container layout: one whole-file blob per record.
        let mut record = put_record(1, "segments/1", b"legacy-payload");
        let legacy_relative =
            super::path_to_portable_string(&blob_relative_path(1, record.operation_id)).unwrap();
        *record.blob_path_mut().unwrap() = legacy_relative.clone();
        let legacy_path = root.join(&legacy_relative);
        fs::create_dir_all(legacy_path.parent().unwrap()).unwrap();
        fs::set_permissions(
            legacy_path.parent().unwrap(),
            fs::Permissions::from_mode(0o700),
        )
        .unwrap();
        fs::write(&legacy_path, b"legacy-payload").unwrap();
        fs::set_permissions(&legacy_path, fs::Permissions::from_mode(0o600)).unwrap();
        let transaction = journal.database.begin_write().unwrap();
        {
            let mut mutations = transaction.open_table(super::MUTATIONS).unwrap();
            mutations
                .insert(1_u64, bincode::serialize(&record).unwrap().as_slice())
                .unwrap();
            drop(mutations);
            let mut meta = transaction.open_table(META).unwrap();
            write_value(&mut meta, super::LOCAL_SEQ_KEY, &1_u64).unwrap();
            write_value(&mut meta, super::LOCAL_BYTES_COMPLETED_KEY, &14_u64).unwrap();
        }
        transaction.commit().unwrap();
        drop(journal);

        let recovered = Journal::open(&root, identity("bucket-a")).unwrap();
        assert_eq!(recovered.read_blob(1).unwrap(), b"legacy-payload");

        // A new batch appended after the legacy record uses containers, and
        // both forms coexist in one journal.
        let committed = recovered
            .publish_batch(prepare_puts(&recovered, 2..=3))
            .unwrap();
        assert!(committed[0].blob_path().unwrap().contains(".blobs#"));
        assert_eq!(recovered.read_blob(1).unwrap(), b"legacy-payload");
        assert_eq!(recovered.read_blob(3).unwrap(), b"payload-3");

        recovered.mark_remote(1, Some("etag-1".to_owned())).unwrap();
        recovered.remove_remote_prefix(1).unwrap();
        assert!(
            !legacy_path.exists(),
            "a fully drained legacy blob is still pruned per record"
        );
        assert_eq!(recovered.read_blob(2).unwrap(), b"payload-2");
    }

    /// A container is reclaimable only once every member is remote-committed.
    /// Draining a prefix of its members must not unlink bytes the rest still
    /// need, and the final member's commit must release the whole file.
    #[test]
    fn a_container_is_reclaimed_only_after_its_last_member_is_remote_committed() {
        let temp = tempfile::tempdir().unwrap();
        let journal = open_temp_journal(&temp, "bucket-a");
        let committed = journal
            .publish_batch(prepare_puts(&journal, 1..=4))
            .unwrap();
        let container = journal.root().join(
            super::BlobRef::parse(committed[0].blob_path().unwrap())
                .unwrap()
                .relative,
        );

        for sequence in 1..=3_u64 {
            journal
                .mark_remote(sequence, Some(format!("etag-{sequence}")))
                .unwrap();
            journal.remove_remote_prefix(sequence).unwrap();
            assert!(
                container.exists(),
                "container unlinked while sequence {} of 4 still needs it",
                sequence + 1
            );
            assert_eq!(
                journal.read_blob(4).unwrap(),
                b"payload-4",
                "an undrained member must stay readable"
            );
        }

        journal.mark_remote(4, Some("etag-4".to_owned())).unwrap();
        journal.remove_remote_prefix(4).unwrap();

        assert!(
            !container.exists(),
            "the container must be reclaimed once every member drained"
        );
        assert!(published_blob_files(&journal).is_empty());
    }

    /// Reclaiming on reopen goes through the same watermark rule, and a
    /// container straddling the remote watermark must survive the scan that
    /// `Journal::open` runs before it validates recovery state.
    #[test]
    fn reopening_keeps_a_container_that_straddles_the_remote_watermark() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("writeback");
        let journal = Journal::open(&root, identity("bucket-a")).unwrap();
        let committed = journal
            .publish_batch(prepare_puts(&journal, 1..=4))
            .unwrap();
        let container = root.join(
            super::BlobRef::parse(committed[0].blob_path().unwrap())
                .unwrap()
                .relative,
        );
        journal.mark_remote(1, Some("etag-1".to_owned())).unwrap();
        journal.mark_remote(2, Some("etag-2".to_owned())).unwrap();
        drop(journal);

        let recovered = Journal::open(&root, identity("bucket-a")).unwrap();

        assert!(container.exists());
        assert_eq!(recovered.read_blob(3).unwrap(), b"payload-3");
        assert_eq!(recovered.read_blob(4).unwrap(), b"payload-4");
        let snapshot = recovered.snapshot().unwrap();
        assert_eq!(snapshot.remote_seq, 2);
        assert_eq!(snapshot.local_seq, 4);
    }

    /// Payload verification is per record, not per file: corrupting one
    /// member's slice must fail that record without silently reading a
    /// neighbour's bytes.
    #[test]
    fn reopening_rejects_a_corrupt_slice_inside_an_otherwise_valid_container() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("writeback");
        let journal = Journal::open(&root, identity("bucket-a")).unwrap();
        let committed = journal
            .publish_batch(prepare_puts(&journal, 1..=3))
            .unwrap();
        let reference = super::BlobRef::parse(committed[1].blob_path().unwrap()).unwrap();
        let container = root.join(reference.relative);
        let offset = reference.slice.unwrap().offset as usize;
        drop(journal);
        let mut bytes = fs::read(&container).unwrap();
        bytes[offset] ^= 0xff;
        fs::write(&container, &bytes).unwrap();

        let error = Journal::open(&root, identity("bucket-a")).unwrap_err();

        assert!(format!("{error:#}").contains("hash mismatch"), "{error:#}");
    }

    /// A container truncated after publication must be caught by the same
    /// recovery validation that catches a missing whole-file blob.
    #[test]
    fn reopening_rejects_a_truncated_container() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("writeback");
        let journal = Journal::open(&root, identity("bucket-a")).unwrap();
        let committed = journal
            .publish_batch(prepare_puts(&journal, 1..=3))
            .unwrap();
        let container = root.join(
            super::BlobRef::parse(committed[0].blob_path().unwrap())
                .unwrap()
                .relative,
        );
        drop(journal);
        let bytes = fs::read(&container).unwrap();
        fs::write(&container, &bytes[..bytes.len() - 1]).unwrap();

        let error = Journal::open(&root, identity("bucket-a")).unwrap_err();

        assert!(
            format!("{error:#}").contains("length mismatch"),
            "{error:#}"
        );
    }
}
