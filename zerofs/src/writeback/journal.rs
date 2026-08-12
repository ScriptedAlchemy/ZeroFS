use crate::writeback::model::{
    FenceClass, JournalIdentity, MutationKind, MutationRecord, Sequence,
};
use crate::writeback::payload::VerifiedPayload;
use anyhow::{Context, Result, bail};
use fs4::fs_std::FileExt;
use redb::{Database, Durability, ReadableDatabase, ReadableTable, TableDefinition};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
#[cfg(unix)]
use std::os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt};
use std::path::{Component, Path, PathBuf};
#[cfg(test)]
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Condvar, Mutex};
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

pub(crate) struct PreparedMutation {
    record: MutationRecord,
    temporary_blob: Option<PathBuf>,
}

impl PreparedMutation {
    pub(crate) fn sequence(&self) -> Sequence {
        self.record.sequence
    }

    #[cfg(test)]
    pub(crate) fn metadata(record: MutationRecord) -> Self {
        Self {
            record,
            temporary_blob: None,
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
    pub dirty_metadata_bytes: u64,
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
        let mut dirty_metadata_bytes = 0_u64;
        for entry in table
            .iter()
            .context("failed to iterate journal mutations")?
        {
            let (_, value) = entry.context("failed to read journal mutation")?;
            let record: MutationRecord =
                bincode::deserialize(value.value()).context("failed to decode journal mutation")?;
            if record.sequence > remote_seq {
                match record.payload() {
                    Some((payload_len, _)) => {
                        dirty_blob_bytes = dirty_blob_bytes
                            .checked_add(payload_len)
                            .context("dirty journal blob byte count overflow")?;
                    }
                    None => {
                        dirty_metadata_bytes = dirty_metadata_bytes
                            .checked_add(record.disk_charge_bytes()?)
                            .context("dirty journal metadata byte count overflow")?;
                    }
                }
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
            dirty_metadata_bytes,
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
        let payload_sha256 = Sha256::digest(payload).into();
        let prepared = self.prepare_put_inner(record, payload, payload_sha256)?;
        self.publish_prepared(prepared)
    }

    pub(crate) fn prepare_verified_put(
        &self,
        record: MutationRecord,
        payload: &VerifiedPayload,
    ) -> Result<PreparedMutation> {
        self.prepare_put_inner(record, payload.bytes(), payload.sha256())
    }

    fn prepare_put_inner(
        &self,
        mut record: MutationRecord,
        payload: &[u8],
        actual_hash: [u8; 32],
    ) -> Result<PreparedMutation> {
        self.validate_record_format(&record)?;
        let (payload_len, payload_sha256) = record
            .payload()
            .context("commit_put requires a payload mutation")?;
        if payload_len != payload.len() as u64 {
            bail!("put payload length does not match mutation record");
        }
        if actual_hash != payload_sha256 {
            bail!("put payload hash does not match mutation record");
        }
        let relative = blob_relative_path(record.sequence, record.operation_id);
        let blob_path = path_to_portable_string(&relative)?;
        *record
            .blob_path_mut()
            .context("payload mutation has no blob path")? = blob_path.clone();
        let operation_id = record.operation_id;
        let final_path = checked_join(&self.root, &blob_path)?;
        let shard = final_path.parent().context("blob path has no parent")?;
        ensure_owner_directory(shard, true)?;

        let tmp_path = self.root.join("tmp").join(format!("{operation_id}.tmp"));
        reject_symlink_if_present(&tmp_path, "journal temporary blob")?;
        let preparation = (|| -> Result<()> {
            let mut tmp_file = open_owner_file(&tmp_path, false).with_context(|| {
                format!("failed to create temporary blob {}", tmp_path.display())
            })?;
            tmp_file
                .write_all(payload)
                .context("failed to write temporary blob")?;
            tmp_file
                .sync_all()
                .context("failed to fsync temporary blob")?;
            let written_len = tmp_file
                .metadata()
                .context("failed to inspect prepared temporary blob")?
                .len();
            if written_len != payload_len {
                bail!(
                    "prepared temporary blob length mismatch: expected {payload_len}, got {written_len}"
                );
            }
            Ok(())
        })();
        if let Err(error) = preparation {
            let cleanup = self.discard_temporary_blob(&tmp_path);
            return match cleanup {
                Ok(()) => Err(error),
                Err(cleanup) => {
                    Err(error.context(format!("temporary blob cleanup also failed: {cleanup:#}")))
                }
            };
        }
        Ok(PreparedMutation {
            record,
            temporary_blob: Some(tmp_path),
        })
    }

    pub(crate) fn prepare_metadata(&self, record: MutationRecord) -> Result<PreparedMutation> {
        self.validate_record_format(&record)?;
        if record.payload().is_some() {
            bail!("prepare_metadata cannot prepare a payload mutation");
        }
        Ok(PreparedMutation {
            record,
            temporary_blob: None,
        })
    }

    pub(crate) fn publish_prepared(&self, prepared: PreparedMutation) -> Result<MutationRecord> {
        if let Err(error) = self.require_next_local_sequence(prepared.sequence()) {
            let cleanup = self.discard_prepared(prepared);
            return match cleanup {
                Ok(()) => Err(error),
                Err(cleanup) => {
                    Err(error.context(format!("prepared blob cleanup also failed: {cleanup:#}")))
                }
            };
        }
        let PreparedMutation {
            record,
            temporary_blob,
        } = prepared;
        let Some(tmp_path) = temporary_blob else {
            self.commit_record(&record, None)?;
            return Ok(record);
        };
        let blob_path = record
            .blob_path()
            .context("prepared payload mutation has no blob path")?;
        let final_path = checked_join(&self.root, blob_path)?;
        let shard = final_path.parent().context("blob path has no parent")?;
        let operation_id = record.operation_id;
        if let Err(error) = self.record_pending_blob(operation_id, blob_path) {
            let cleanup = self.discard_temporary_blob(&tmp_path);
            return match cleanup {
                Ok(()) => Err(error),
                Err(cleanup) => {
                    Err(error.context(format!("temporary blob cleanup also failed: {cleanup:#}")))
                }
            };
        }
        if let Err(error) = fs::rename(&tmp_path, &final_path) {
            let cleanup = self.abort_unpublished_blob(operation_id, &tmp_path);
            let publication = anyhow::Error::new(error).context(format!(
                "failed to publish local blob {} to {}",
                tmp_path.display(),
                final_path.display()
            ));
            return match cleanup {
                Ok(()) => Err(publication),
                Err(cleanup) => {
                    Err(publication
                        .context(format!("pending-intent cleanup also failed: {cleanup:#}")))
                }
            };
        }
        sync_directory(shard)?;
        self.commit_record(&record, Some(operation_id))?;
        Ok(record)
    }

    pub(crate) fn discard_prepared(&self, prepared: PreparedMutation) -> Result<()> {
        match prepared.temporary_blob {
            Some(path) => self.discard_temporary_blob(&path),
            None => Ok(()),
        }
    }

    fn discard_temporary_blob(&self, path: &Path) -> Result<()> {
        match fs::remove_file(path) {
            Ok(()) => sync_directory(self.root.join("tmp")),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(error).context("failed to remove prepared temporary blob"),
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
        let relative = record
            .blob_path()
            .with_context(|| format!("journal mutation {sequence} has no blob"))?;
        let path = checked_join(&self.root, relative)?;
        read_verified_blob(&path, &record)
    }

    pub fn mark_remote(&self, sequence: Sequence, result_etag: Option<String>) -> Result<()> {
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
        for record in &removable {
            if let Some(relative) = record.blob_path() {
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

    fn require_next_local_sequence(&self, sequence: Sequence) -> Result<()> {
        let progress = self.progress()?;
        let expected = progress
            .local_seq
            .checked_add(1)
            .context("local sequence overflow")?;
        if sequence != expected {
            bail!(
                "local sequence must advance contiguously from {} to {expected}, got {sequence}",
                progress.local_seq
            );
        }
        Ok(())
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

    fn record_pending_blob(&self, operation_id: Uuid, relative: &str) -> Result<()> {
        let _write = self.write_gate.lock();
        let mut transaction = self
            .database
            .begin_write()
            .context("failed to record pending blob")?;
        transaction
            .set_durability(Durability::Immediate)
            .context("failed to set journal durability")?;
        {
            let mut table = transaction
                .open_table(PENDING_BLOBS)
                .context("failed to open pending blob table")?;
            table
                .insert(operation_id.to_string().as_str(), relative.as_bytes())
                .context("failed to store pending blob")?;
        }
        transaction
            .commit()
            .context("failed to commit pending blob")
    }

    fn abort_unpublished_blob(&self, operation_id: Uuid, tmp_path: &Path) -> Result<()> {
        match fs::remove_file(tmp_path) {
            Ok(()) => sync_directory(self.root.join("tmp"))?,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error).context("failed to remove unpublished temporary blob"),
        }
        let _write = self.write_gate.lock();
        let mut transaction = self
            .database
            .begin_write()
            .context("failed to clear pending blob")?;
        transaction
            .set_durability(Durability::Immediate)
            .context("failed to set journal durability")?;
        {
            let mut table = transaction
                .open_table(PENDING_BLOBS)
                .context("failed to open pending blob table")?;
            table
                .remove(operation_id.to_string().as_str())
                .context("failed to clear pending blob")?;
        }
        transaction
            .commit()
            .context("failed to commit pending cleanup")
    }

    fn commit_record(&self, record: &MutationRecord, pending: Option<Uuid>) -> Result<()> {
        let _write = self.write_gate.lock();
        let mut transaction = self
            .database
            .begin_write()
            .context("failed to commit journal record")?;
        transaction
            .set_durability(Durability::Immediate)
            .context("failed to set journal durability")?;
        {
            let mut meta = transaction
                .open_table(META)
                .context("failed to open journal metadata")?;
            let current = read_required::<u64>(&meta, LOCAL_SEQ_KEY)?;
            let expected = current.checked_add(1).context("local sequence overflow")?;
            if record.sequence != expected {
                bail!(
                    "local sequence must advance contiguously from {current} to {expected}, got {}",
                    record.sequence
                );
            }
            let completed_bytes = record.payload().map_or(0, |(payload_len, _)| payload_len);
            let total_completed = read_optional::<u64>(&meta, LOCAL_BYTES_COMPLETED_KEY)?
                .unwrap_or_default()
                .checked_add(completed_bytes)
                .context("local completed byte counter overflow")?;
            let encoded =
                bincode::serialize(record).context("failed to encode journal mutation")?;
            let mut table = transaction
                .open_table(MUTATIONS)
                .context("failed to open journal mutations")?;
            table
                .insert(record.sequence, encoded.as_slice())
                .context("failed to store journal mutation")?;
            drop(table);
            if let Some(operation_id) = pending {
                let mut pending = transaction
                    .open_table(PENDING_BLOBS)
                    .context("failed to open pending blob table")?;
                pending
                    .remove(operation_id.to_string().as_str())
                    .context("failed to clear pending blob")?;
            }
            write_value(&mut meta, LOCAL_SEQ_KEY, &record.sequence)?;
            write_value(&mut meta, LOCAL_BYTES_COMPLETED_KEY, &total_completed)?;
        }
        transaction
            .commit()
            .context("failed to commit journal mutation")
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
            let path = checked_join(&self.root, relative)?;
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
            let operation_id = key.value().to_owned();
            Uuid::parse_str(&operation_id).context("pending blob has an invalid operation UUID")?;
            let relative = std::str::from_utf8(value.value())
                .context("pending blob path is not UTF-8")?
                .to_owned();
            checked_join(&self.root, &relative)?;
            pending.insert(operation_id, relative);
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
                let path = checked_join(&self.root, relative)?;
                verify_record_blob(&path, record).with_context(|| {
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

fn blob_relative_path(sequence: Sequence, operation_id: Uuid) -> PathBuf {
    PathBuf::from("blobs")
        .join(format!("{:02x}", sequence & 0xff))
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

fn read_verified_blob(path: &Path, record: &MutationRecord) -> Result<Vec<u8>> {
    let (payload_len, payload_sha256) = record
        .payload()
        .context("journal record does not reference a payload blob")?;
    verify_file_payload(path, payload_len, payload_sha256, true)?
        .context("verified blob read did not return payload bytes")
}

fn verify_record_blob(path: &Path, record: &MutationRecord) -> Result<()> {
    let (payload_len, payload_sha256) = record
        .payload()
        .context("journal record does not reference a payload blob")?;
    verify_file_payload(path, payload_len, payload_sha256, false).map(drop)
}

fn verify_file_payload(
    path: &Path,
    expected_len: u64,
    expected_sha256: [u8; 32],
    collect: bool,
) -> Result<Option<Vec<u8>>> {
    let metadata = fs::symlink_metadata(path)
        .with_context(|| format!("missing committed blob {}", path.display()))?;
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        bail!(
            "committed blob {} is not a safe regular file",
            path.display()
        );
    }
    validate_owner_only(path, &metadata, 0o600)?;
    if metadata.len() != expected_len {
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
    if !opened_metadata.is_file() || opened_metadata.len() != expected_len {
        bail!("committed blob changed while opening");
    }

    let mut collected = if collect {
        let capacity = usize::try_from(expected_len).context("blob is too large to read")?;
        Some(Vec::with_capacity(capacity))
    } else {
        None
    };
    let mut hasher = Sha256::new();
    let mut total = 0_u64;
    let mut buffer = vec![0_u8; 1024 * 1024];
    loop {
        let read = file
            .read(&mut buffer)
            .with_context(|| format!("failed to read blob {}", path.display()))?;
        if read == 0 {
            break;
        }
        total = total
            .checked_add(read as u64)
            .context("blob length overflow")?;
        if total > expected_len {
            bail!("committed blob length mismatch");
        }
        hasher.update(&buffer[..read]);
        if let Some(bytes) = &mut collected {
            bytes.extend_from_slice(&buffer[..read]);
        }
    }
    if total != expected_len {
        bail!("committed blob length mismatch");
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
        fs::create_dir(path)
            .with_context(|| format!("failed to create journal directory {}", path.display()))?;
        set_owner_only_directory(path)?;
        if let Some(parent) = path.parent() {
            sync_directory(parent)?;
        }
    }
    let metadata = fs::symlink_metadata(path)
        .with_context(|| format!("failed to inspect journal directory {}", path.display()))?;
    if metadata.file_type().is_symlink() {
        bail!("journal directory {} must not be a symlink", path.display());
    }
    if !metadata.is_dir() {
        bail!("journal path {} is not a directory", path.display());
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

#[cfg(unix)]
fn validate_owner_only(path: &Path, metadata: &fs::Metadata, expected_mode: u32) -> Result<()> {
    let mode = metadata.mode() & 0o777;
    if mode != expected_mode {
        bail!(
            "journal path {} has mode {mode:o}; expected {expected_mode:o}",
            path.display()
        );
    }
    if metadata.uid() != unsafe { libc::geteuid() } {
        bail!(
            "journal path {} is not owned by the service user",
            path.display()
        );
    }
    Ok(())
}

#[cfg(not(unix))]
fn validate_owner_only(_path: &Path, _metadata: &fs::Metadata, _expected_mode: u32) -> Result<()> {
    Ok(())
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
    use super::{Journal, JournalSnapshot, JournalWriteGate, REMOTE_OBJECT_VERSIONS};
    use crate::writeback::model::{
        FenceClass, JournalIdentity, LocalEtag, MutationKind, MutationMode, MutationRecord,
    };
    use crate::writeback::payload::VerifiedPayload;
    use bytes::Bytes;
    use sha2::{Digest, Sha256};
    use std::fs;
    #[cfg(unix)]
    use std::os::unix::fs::PermissionsExt;
    use std::sync::{Arc, Mutex};
    use std::thread;
    use std::time::{Duration, Instant};
    use tempfile::TempDir;
    use uuid::Uuid;

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
        MutationRecord {
            format_version: 1,
            sequence,
            operation_id: Uuid::from_u128(0x1000 + sequence as u128),
            path: path.to_owned(),
            kind: MutationKind::Put {
                mode: MutationMode::Create,
                expected_visible_version: None,
                payload_len: payload.len() as u64,
                payload_sha256: Sha256::digest(payload).into(),
                blob_path: String::new(),
            },
            local_etag: LocalEtag::new(Uuid::nil(), sequence),
            accepted_at_unix_ms: 1_786_435_200_000 + sequence,
            remote_predecessor_etag: None,
            remote_result_etag: None,
            fence: FenceClass::ImmutableCreate,
            retry_count: 0,
            last_error: None,
        }
    }

    fn delete_record(sequence: u64, path: &str) -> MutationRecord {
        MutationRecord {
            format_version: 1,
            sequence,
            operation_id: Uuid::from_u128(0x2000 + sequence as u128),
            path: path.to_owned(),
            kind: MutationKind::Delete,
            local_etag: LocalEtag::new(Uuid::nil(), sequence),
            accepted_at_unix_ms: 1_786_435_200_000 + sequence,
            remote_predecessor_etag: None,
            remote_result_etag: None,
            fence: FenceClass::Fence,
            retry_count: 0,
            last_error: None,
        }
    }

    fn open_temp_journal(temp: &TempDir, bucket: &str) -> Journal {
        Journal::open(temp.path().join("writeback"), identity(bucket)).unwrap()
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
            snapshot.dirty_metadata_bytes,
            MutationRecord::metadata_disk_charge("obsolete").unwrap()
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
        assert_eq!(fs::read_dir(journal.root().join("tmp")).unwrap().count(), 1);

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
        let blob = journal.root().join(committed.blob_path().unwrap());
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
        let blob = journal.root().join(committed.blob_path().unwrap());
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
        let blob = journal.root().join(committed.blob_path().unwrap());
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
        assert!(!journal.root().join(first.blob_path().unwrap()).exists());
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
    fn failed_blob_publication_clears_pending_intent_and_temporary_bytes() {
        let temp = tempfile::tempdir().unwrap();
        let journal = open_temp_journal(&temp, "bucket-a");
        let record = put_record(1, "segments/1", b"payload");
        let final_path = journal
            .root()
            .join("blobs/01")
            .join(format!("{}.blob", record.operation_id));
        fs::create_dir_all(&final_path).unwrap();
        fs::set_permissions(
            final_path.parent().unwrap(),
            fs::Permissions::from_mode(0o700),
        )
        .unwrap();

        let error = journal.commit_put(record, b"payload").unwrap_err();

        assert!(
            format!("{error:#}").contains("failed to publish local blob"),
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
        let blob = journal.root().join(committed.blob_path().unwrap());
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
            fs::metadata(journal.root().join(committed.blob_path().unwrap()))
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
}
