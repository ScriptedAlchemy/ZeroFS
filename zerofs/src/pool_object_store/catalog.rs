//! Durable placement, capacity reservations, and deletion debts.
//!
//! Every change that makes data visible is one immediate-durability redb
//! transaction. An unpublished upload remains reserved until its unique object
//! has been deleted. Superseded objects remain charged until cleanup succeeds.

use super::health::{MemberHealth, SegmentFault, SmartSample};
use super::{EvacuationCapacity, PoolMember, PoolMemberStatus, PoolPlacement, PoolStatus};
use anyhow::{Context, Result, bail};
use fs4::fs_std::FileExt;
use object_store::path::Path;
use redb::{
    Builder, Database, Durability, ReadableDatabase, ReadableTable, ReadableTableMetadata,
    TableDefinition,
};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::fs::File;
use std::path::Path as FilePath;
use std::sync::Mutex;
use uuid::Uuid;

use crate::writeback::anchored_dir::AnchoredDir;

#[derive(Debug, thiserror::Error)]
#[error("no pool member has sufficient capacity and local free-space reserve")]
pub(super) struct NoCapacity;

#[derive(Debug, thiserror::Error)]
#[error("automatic evacuation stopped: multiple devices have unfinished evacuation")]
pub(super) struct AutomaticEvacuationBlocked;

#[derive(Debug, thiserror::Error)]
#[error("pool member {member} is allocation-blocked: {reason}")]
pub(super) struct AllocationBlocked {
    member: String,
    reason: String,
}

pub const CATALOG_FILE_NAME: &str = "pool.redb";
const HEALTH: TableDefinition<&str, &[u8]> = TableDefinition::new("pool_member_health_v1");
const FAULTS: TableDefinition<&str, &[u8]> = TableDefinition::new("pool_segment_faults_v1");
const META: TableDefinition<&str, &[u8]> = TableDefinition::new("pool_meta");
const MEMBERS: TableDefinition<&str, &[u8]> = TableDefinition::new("pool_members");
const OBJECTS: TableDefinition<&str, &[u8]> = TableDefinition::new("pool_objects");
const UPLOADS: TableDefinition<&str, &[u8]> = TableDefinition::new("pool_uploads");
const GARBAGE: TableDefinition<&str, &[u8]> = TableDefinition::new("pool_garbage");
const HIDDEN_LEGACY: TableDefinition<&str, u8> = TableDefinition::new("pool_hidden_legacy");

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(super) struct Identity {
    pub format: u32,
    pub pool_id: String,
    pub authority_prefix: String,
    pub catalog_directory: String,
    pub initialized: bool,
}

impl Identity {
    pub fn marker(&self) -> Result<Vec<u8>> {
        Ok(bincode::serialize(&(
            self.format,
            &self.pool_id,
            &self.authority_prefix,
            &self.catalog_directory,
        ))?)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(super) struct MemberRecord {
    pub id: String,
    pub identity: String,
    pub marker_token: String,
    pub initialized: bool,
    pub capacity: u64,
    pub used: u64,
    pub reserved: u64,
}

impl MemberRecord {
    pub fn marker(&self, pool_id: &str) -> Result<Vec<u8>> {
        Ok(bincode::serialize(&(
            1_u32,
            pool_id,
            &self.id,
            &self.marker_token,
        ))?)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(super) enum Physical {
    Member {
        member_id: String,
        key: String,
        e_tag: Option<String>,
        version: Option<String>,
    },
    Legacy {
        key: String,
        e_tag: Option<String>,
        version: Option<String>,
    },
}

impl Physical {
    pub fn key(&self) -> &str {
        match self {
            Self::Member { key, .. } | Self::Legacy { key, .. } => key,
        }
    }

    pub fn member_id(&self) -> Option<&str> {
        match self {
            Self::Member { member_id, .. } => Some(member_id),
            Self::Legacy { .. } => None,
        }
    }

    pub fn versions(&self) -> (Option<String>, Option<String>) {
        match self {
            Self::Member { e_tag, version, .. } | Self::Legacy { e_tag, version, .. } => {
                (e_tag.clone(), version.clone())
            }
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(super) struct Entry {
    pub physical: Physical,
    pub size: u64,
    pub last_modified_secs: i64,
    pub last_modified_nanos: u32,
    pub e_tag: Option<String>,
    pub version: Option<String>,
    pub sha256: Option<[u8; 32]>,
    pub attributes: Vec<StoredAttribute>,
}

/// Logical attributes must not be inferred from a provider's HTTP response
/// headers: those can appear even when the writer supplied no attributes.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub(super) enum StoredAttribute {
    ContentDisposition(String),
    ContentEncoding(String),
    ContentLanguage(String),
    ContentType(String),
    CacheControl(String),
    StorageClass(String),
    Metadata(String, String),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(super) struct Upload {
    pub id: String,
    pub location: String,
    pub member_id: String,
    pub physical_key: String,
    pub size: u64,
    /// False means no request that could create the physical object has begun.
    pub started: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(super) struct Garbage {
    pub id: String,
    pub location: String,
    pub entry: Entry,
}

#[derive(Clone, Debug)]
pub(super) struct Lookup {
    pub entry: Option<Entry>,
    pub legacy_hidden: bool,
}

pub(super) struct Snapshot {
    pub entries: BTreeMap<String, Entry>,
    pub hidden: BTreeSet<String>,
}

pub(super) struct Catalog {
    // Fields drop in declaration order: finish database shutdown before
    // releasing the catalog's exclusive ownership below.
    database: Database,
    pub identity: Identity,
    /// Quarantine is sticky and forbids every new target reservation and
    /// publication. Once a writer transaction proves all ownership absent,
    /// this member cannot regain data. Reopening discards and rechecks proof.
    verified_empty_quarantined: Mutex<BTreeSet<String>>,
    #[cfg(test)]
    empty_ownership_scans: std::sync::atomic::AtomicUsize,
    _root: AnchoredDir,
    _lock: CatalogLock,
}

struct CatalogLock(File);

impl Drop for CatalogLock {
    fn drop(&mut self) {
        // CLOEXEC only closes a child's inherited descriptor at exec. Closing
        // our descriptor alone can leave its flock held across that interval;
        // explicitly unlock the shared open file description after the owner
        // has finished all catalog work.
        if let Err(error) = FileExt::unlock(&self.0) {
            tracing::warn!(%error, "failed to release pool catalog lock");
        }
    }
}

impl std::fmt::Debug for Catalog {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PoolCatalog")
            .field("pool_id", &self.identity.pool_id)
            .finish_non_exhaustive()
    }
}

impl Catalog {
    pub fn open(
        root: &FilePath,
        prefix: &str,
        initialize: bool,
        marker: Option<&[u8]>,
    ) -> Result<Self> {
        let existed = root.join(CATALOG_FILE_NAME).try_exists()?;
        if !existed && !initialize {
            bail!(
                "pool catalog is missing; use `zerofs pool init` for a new pool, or restore the existing catalog"
            );
        }
        if !existed && marker.is_some() {
            bail!(
                "authority belongs to an existing pool but its catalog is missing; restore that catalog instead of initializing a new pool"
            );
        }
        let anchored = if initialize {
            AnchoredDir::open_or_create_absolute(root, 0o700)?
        } else {
            AnchoredDir::open_absolute(root, 0o700)?
        };
        let resolved_directory =
            std::fs::canonicalize(root).context("resolve pool catalog directory")?;
        let catalog_directory = resolved_directory
            .to_str()
            .context("pool catalog directory must be valid UTF-8")?
            .to_owned();
        let (lock, _) = anchored.open_owner_file("pool.lock".as_ref(), true)?;
        if !FileExt::try_lock_exclusive(&lock)? {
            bail!("pool catalog is already locked by another process");
        }
        let lock = CatalogLock(lock);
        if !initialize {
            // Do not create a replacement database if the file disappeared
            // between the preliminary check and descriptor-anchored open.
            anchored.open_existing_owner_file_read_only(CATALOG_FILE_NAME.as_ref())?;
        }
        let (file, database_existed) =
            anchored.open_owner_file(CATALOG_FILE_NAME.as_ref(), true)?;
        if !database_existed && marker.is_some() {
            bail!("existing pool catalog disappeared while opening it");
        }
        let database = Builder::new()
            .create_file(file)
            .context("open pool catalog")?;
        if !database_existed {
            anchored.sync()?;
        }
        let mut tx = database.begin_write()?;
        tx.set_durability(Durability::Immediate)?;
        let identity = {
            let mut table = tx.open_table(META)?;
            let existing: Option<Identity> = table
                .get("identity")?
                .map(|value| bincode::deserialize(value.value()))
                .transpose()?;
            let identity = match existing {
                Some(identity) => identity,
                None if initialize && marker.is_none() => Identity {
                    format: 1,
                    pool_id: Uuid::new_v4().to_string(),
                    authority_prefix: prefix.to_owned(),
                    catalog_directory: catalog_directory.clone(),
                    initialized: false,
                },
                None => bail!(
                    "pool catalog has no identity; explicit initialization or recovery is required"
                ),
            };
            if identity.format != 1 || identity.authority_prefix != prefix {
                bail!("pool catalog format or authority prefix does not match this configuration");
            }
            if identity.catalog_directory != catalog_directory {
                bail!(
                    "pool is bound to a different catalog directory; restore the catalog at {} instead of opening a moved or copied catalog at {}",
                    identity.catalog_directory,
                    catalog_directory,
                );
            }
            if let Some(marker) = marker {
                if marker != identity.marker()? {
                    bail!(
                        "pool catalog does not belong to this authority (pool identity mismatch)"
                    );
                }
            } else if identity.initialized {
                bail!(
                    "pool authority identity marker is missing; refusing to recreate an existing pool"
                );
            }
            if !initialize && !identity.initialized {
                bail!(
                    "pool initialization did not finish; rerun `zerofs pool init` with the original configuration"
                );
            }
            table.insert("identity", bincode::serialize(&identity)?.as_slice())?;
            identity
        };
        {
            let mut meta = tx.open_table(META)?;
            let version: Option<u32> = meta
                .get("health_format")?
                .map(|v| bincode::deserialize(v.value()))
                .transpose()?;
            if version.is_some_and(|v| v != 1) {
                bail!("unsupported pool health version");
            }
            meta.insert("health_format", bincode::serialize(&1_u32)?.as_slice())?;
        }
        tx.open_table(HEALTH)?;
        tx.open_table(FAULTS)?;
        tx.open_table(MEMBERS)?;
        tx.open_table(OBJECTS)?;
        tx.open_table(UPLOADS)?;
        tx.open_table(GARBAGE)?;
        tx.open_table(HIDDEN_LEGACY)?;
        tx.commit()?;
        Ok(Self {
            database,
            identity,
            verified_empty_quarantined: Mutex::new(BTreeSet::new()),
            #[cfg(test)]
            empty_ownership_scans: std::sync::atomic::AtomicUsize::new(0),
            _root: anchored,
            _lock: lock,
        })
    }

    pub fn finish_initialization(&self) -> Result<()> {
        let mut identity = self.identity.clone();
        identity.initialized = true;
        let mut tx = self.database.begin_write()?;
        tx.set_durability(Durability::Immediate)?;
        tx.open_table(META)?
            .insert("identity", bincode::serialize(&identity)?.as_slice())?;
        tx.commit()?;
        Ok(())
    }

    pub fn configure_members(&self, members: &[PoolMember]) -> Result<()> {
        let mut ids = BTreeSet::new();
        let mut identities = BTreeSet::new();
        for member in members {
            if member.id.is_empty()
                || member.id.len() > 64
                || !member
                    .id
                    .bytes()
                    .all(|c| c.is_ascii_alphanumeric() || c == b'-' || c == b'_')
            {
                bail!("pool member id must contain 1-64 ASCII letters, digits, '-' or '_'");
            }
            if member.id == "authority" {
                bail!("pool member id 'authority' is reserved for legacy local segments");
            }
            if member.capacity_bytes == 0 || member.identity.is_empty() {
                bail!(
                    "pool member {} needs a positive capacity and stable identity",
                    member.id
                );
            }
            if !ids.insert(member.id.clone()) || !identities.insert(member.identity.clone()) {
                bail!("pool member ids and backend identities must be unique");
            }
        }
        if members.is_empty() {
            bail!("a pool requires at least one data member");
        }
        let mut tx = self.database.begin_write()?;
        tx.set_durability(Durability::Immediate)?;
        {
            let mut table = tx.open_table(MEMBERS)?;
            for row in table.iter()? {
                let (key, _) = row?;
                if !ids.contains(key.value()) {
                    bail!(
                        "configured pool member {} is missing; members may be added or reordered but not silently removed",
                        key.value()
                    );
                }
            }
            for member in members {
                let existing: Option<MemberRecord> = table
                    .get(member.id.as_str())?
                    .map(|value| bincode::deserialize(value.value()))
                    .transpose()?;
                let record = match existing {
                    Some(mut record) => {
                        if record.identity != member.identity {
                            bail!(
                                "pool member {} changed backend identity; add a new member id and migrate instead",
                                member.id
                            );
                        }
                        let occupied = record
                            .used
                            .checked_add(record.reserved)
                            .context("pool usage overflow")?;
                        if member.capacity_bytes < occupied {
                            bail!(
                                "pool member {} capacity is below its used and reserved bytes",
                                member.id
                            );
                        }
                        record.capacity = member.capacity_bytes;
                        record
                    }
                    None => MemberRecord {
                        id: member.id.clone(),
                        identity: member.identity.clone(),
                        marker_token: Uuid::new_v4().to_string(),
                        initialized: false,
                        capacity: member.capacity_bytes,
                        used: 0,
                        reserved: 0,
                    },
                };
                table.insert(member.id.as_str(), bincode::serialize(&record)?.as_slice())?;
            }
        }
        tx.commit()?;
        Ok(())
    }

    pub fn member(&self, id: &str) -> Result<MemberRecord> {
        let tx = self.database.begin_read()?;
        let table = tx.open_table(MEMBERS)?;
        let value = table
            .get(id)?
            .with_context(|| format!("pool member {id} is not registered"))?;
        Ok(bincode::deserialize(value.value())?)
    }

    pub fn mark_member_initialized(&self, id: &str) -> Result<()> {
        let mut tx = self.database.begin_write()?;
        tx.set_durability(Durability::Immediate)?;
        {
            let mut table = tx.open_table(MEMBERS)?;
            let mut member: MemberRecord =
                bincode::deserialize(table.get(id)?.context("missing pool member")?.value())?;
            member.initialized = true;
            table.insert(id, bincode::serialize(&member)?.as_slice())?;
        }
        tx.commit()?;
        Ok(())
    }

    pub fn kernel_observation(
        &self,
        member: &str,
        coverage: String,
        cursor: Option<(String, u64)>,
    ) -> Result<()> {
        let mut tx = self.database.begin_write()?;
        tx.set_durability(Durability::Immediate)?;
        let mut health = health_in(&tx, member)?;
        health.kernel_coverage = Some(coverage);
        if let Some((boot, sequence)) = cursor
            && !health
                .kernel_cursor
                .as_ref()
                .is_some_and(|(old_boot, old_seq)| old_boot == &boot && *old_seq >= sequence)
        {
            health.quarantine(
                "kernel block I/O error".into(),
                chrono::Utc::now().timestamp_millis(),
            );
            health.kernel_cursor = Some((boot, sequence));
        }
        tx.open_table(HEALTH)?
            .insert(member, bincode::serialize(&health)?.as_slice())?;
        tx.commit()?;
        Ok(())
    }

    pub fn blocked_members(&self) -> Result<BTreeSet<String>> {
        let tx = self.database.begin_read()?;
        let mut blocked = BTreeSet::new();
        for row in tx.open_table(HEALTH)?.iter()? {
            let (id, value) = row?;
            let health: MemberHealth = bincode::deserialize(value.value())?;
            if health.quarantine.is_some() || health.monitoring_error.is_some() {
                blocked.insert(id.value().into());
            }
        }
        Ok(blocked)
    }

    pub fn require_eligible(&self, member: &str) -> Result<()> {
        let tx = self.database.begin_read()?;
        let health: MemberHealth = tx
            .open_table(HEALTH)?
            .get(member)?
            .map(|v| bincode::deserialize(v.value()))
            .transpose()?
            .unwrap_or_default();
        if let Some(reason) = health.quarantine.or(health.monitoring_error) {
            return Err(AllocationBlocked {
                member: member.into(),
                reason,
            }
            .into());
        }
        Ok(())
    }

    pub fn evacuation_error(&self, member: &str, issue: Option<String>) -> Result<()> {
        let mut tx = self.database.begin_write()?;
        tx.set_durability(Durability::Immediate)?;
        let mut health = health_in(&tx, member)?;
        health.evacuation_error = issue;
        tx.open_table(HEALTH)?
            .insert(member, bincode::serialize(&health)?.as_slice())?;
        tx.commit()?;
        Ok(())
    }

    pub fn monitoring_error(&self, member: &str, error: Option<String>) -> Result<()> {
        let mut tx = self.database.begin_write()?;
        tx.set_durability(Durability::Immediate)?;
        let mut health = health_in(&tx, member)?;
        health.monitoring_error = error;
        tx.open_table(HEALTH)?
            .insert(member, bincode::serialize(&health)?.as_slice())?;
        tx.commit()?;
        Ok(())
    }

    pub fn observe_health(
        &self,
        member: &str,
        sample: Option<SmartSample>,
        reason: Option<String>,
        coverage_error: Option<String>,
    ) -> Result<()> {
        let mut tx = self.database.begin_write()?;
        tx.set_durability(Durability::Immediate)?;
        if tx.open_table(MEMBERS)?.get(member)?.is_none() {
            bail!("unknown pool member");
        }
        let mut health = health_in(&tx, member)?;
        let now = chrono::Utc::now().timestamp_millis();
        if let Some(sample) = sample {
            health.monitoring_error = coverage_error;
            health.observe(sample, now);
        }
        if let Some(reason) = reason {
            health.quarantine(reason, now);
        }
        tx.open_table(HEALTH)?
            .insert(member, bincode::serialize(&health)?.as_slice())?;
        tx.commit()?;
        Ok(())
    }

    pub fn record_source_fault(
        &self,
        location: &str,
        expected: &Entry,
        reason: String,
    ) -> Result<()> {
        let mut tx = self.database.begin_write()?;
        tx.set_durability(Durability::Immediate)?;
        let current: Option<Entry> = tx
            .open_table(OBJECTS)?
            .get(location)?
            .map(|v| bincode::deserialize(v.value()))
            .transpose()?;
        if current
            .as_ref()
            .is_some_and(|e| e.physical.key() == expected.physical.key())
        {
            let old: Option<SegmentFault> = tx
                .open_table(FAULTS)?
                .get(location)?
                .map(|v| bincode::deserialize(v.value()))
                .transpose()?;
            let attempts = old
                .filter(|f| f.physical_key == expected.physical.key())
                .map_or(1, |f| f.attempts.saturating_add(1));
            let fault = SegmentFault {
                location: location.into(),
                physical_key: expected.physical.key().into(),
                member_id: expected.physical.member_id().unwrap_or("authority").into(),
                reason,
                observed_ms: chrono::Utc::now().timestamp_millis(),
                attempts,
                next_retry_ms: chrono::Utc::now().timestamp_millis()
                    + (60_000_i64.saturating_mul(1_i64 << attempts.min(4))).min(600_000),
            };
            tx.open_table(FAULTS)?
                .insert(location, bincode::serialize(&fault)?.as_slice())?;
        }
        tx.commit()?;
        Ok(())
    }

    pub fn faults(&self) -> Result<Vec<SegmentFault>> {
        let tx = self.database.begin_read()?;
        let objects = tx.open_table(OBJECTS)?;
        let mut faults = Vec::new();
        for row in tx.open_table(FAULTS)?.iter()? {
            let fault: SegmentFault = bincode::deserialize(row?.1.value())?;
            let current: Option<Entry> = objects
                .get(fault.location.as_str())?
                .map(|v| bincode::deserialize(v.value()))
                .transpose()?;
            if current.is_some_and(|e| e.physical.key() == fault.physical_key) {
                faults.push(fault);
            }
        }
        Ok(faults)
    }

    pub fn lookup(&self, location: &str) -> Result<Lookup> {
        let tx = self.database.begin_read()?;
        let objects = tx.open_table(OBJECTS)?;
        let entry = objects
            .get(location)?
            .map(|v| bincode::deserialize(v.value()))
            .transpose()?;
        let legacy_hidden = tx.open_table(HIDDEN_LEGACY)?.get(location)?.is_some();
        Ok(Lookup {
            entry,
            legacy_hidden,
        })
    }

    pub fn reserve(
        &self,
        location: &str,
        size: u64,
        members: &[PoolMember],
        target: Option<&str>,
    ) -> Result<Upload> {
        let mut tx = self.database.begin_write()?;
        tx.set_durability(Durability::Immediate)?;
        let upload = {
            let mut table = tx.open_table(MEMBERS)?;
            // Conservatively share the reservation budget across all local
            // paths. In particular, two directories on the same filesystem
            // must not both promise the same currently-free physical bytes.
            let mut local_reserved = 0_u64;
            for member in members.iter().filter(|member| member.local_path.is_some()) {
                let record: MemberRecord = bincode::deserialize(
                    table
                        .get(member.id.as_str())?
                        .context("unregistered pool member")?
                        .value(),
                )?;
                local_reserved = local_reserved
                    .checked_add(record.reserved)
                    .context("local pool reservation overflow")?;
            }
            let mut chosen = None;
            for member in members
                .iter()
                .filter(|m| target.is_none_or(|id| id == m.id))
            {
                if health_in(&tx, &member.id)?.quarantine.is_some()
                    || health_in(&tx, &member.id)?.monitoring_error.is_some()
                {
                    continue;
                }
                let mut record: MemberRecord = bincode::deserialize(
                    table
                        .get(member.id.as_str())?
                        .context("unregistered pool member")?
                        .value(),
                )?;
                let occupied = record
                    .used
                    .checked_add(record.reserved)
                    .context("pool usage overflow")?;
                if record.capacity.saturating_sub(occupied) < size {
                    continue;
                }
                if let Some(path) = &member.local_path {
                    let Ok(available) = fs4::available_space(path) else {
                        continue;
                    };
                    // Outstanding reservations may not have reached the device
                    // yet. Counting all of them is deliberately conservative.
                    let required = member
                        .min_free_bytes
                        .checked_add(local_reserved)
                        .and_then(|n| n.checked_add(size))
                        .context("pool free-space reservation overflow")?;
                    if available < required {
                        continue;
                    }
                }
                record.reserved = record
                    .reserved
                    .checked_add(size)
                    .context("pool reservation overflow")?;
                table.insert(member.id.as_str(), bincode::serialize(&record)?.as_slice())?;
                chosen = Some(member.id.clone());
                break;
            }
            let member_id = chosen.ok_or(NoCapacity)?;
            let id = Uuid::new_v4().to_string();
            Upload {
                physical_key: format!(".zerofs-pool/{}/objects/{id}", self.identity.pool_id),
                id,
                location: location.to_owned(),
                member_id,
                size,
                started: false,
            }
        };
        tx.open_table(UPLOADS)?
            .insert(upload.id.as_str(), bincode::serialize(&upload)?.as_slice())?;
        tx.commit()?;
        Ok(upload)
    }

    pub fn require_automatic_evacuation(&self) -> Result<()> {
        let tx = self.database.begin_write()?;
        self.ensure_single_active_failure(&tx)
    }

    pub fn mark_started_for_migration(&self, id: &str, automatic: bool) -> Result<()> {
        let mut tx = self.database.begin_write()?;
        tx.set_durability(Durability::Immediate)?;
        if automatic {
            self.ensure_single_active_failure(&tx)?;
        }
        {
            let mut table = tx.open_table(UPLOADS)?;
            let mut upload: Upload = bincode::deserialize(
                table
                    .get(id)?
                    .context("pool upload reservation is missing")?
                    .value(),
            )?;
            ensure_eligible(&tx, &upload.member_id)?;
            upload.started = true;
            table.insert(id, bincode::serialize(&upload)?.as_slice())?;
        }
        tx.commit()?;
        Ok(())
    }

    pub fn publish_for_migration(
        &self,
        id: &str,
        entry: &Entry,
        previous: Option<&Entry>,
        automatic: bool,
    ) -> Result<Option<Garbage>> {
        let mut tx = self.database.begin_write()?;
        tx.set_durability(Durability::Immediate)?;
        if automatic {
            self.ensure_single_active_failure(&tx)?;
        }
        let upload: Upload = {
            let table = tx.open_table(UPLOADS)?;
            bincode::deserialize(
                table
                    .get(id)?
                    .context("pool upload reservation is missing")?
                    .value(),
            )?
        };
        ensure_eligible(&tx, &upload.member_id)?;
        if upload.size != entry.size
            || entry.physical.key() != upload.physical_key
            || entry.physical.member_id() != Some(upload.member_id.as_str())
        {
            bail!("pool publication does not match its durable reservation");
        }
        {
            let mut members = tx.open_table(MEMBERS)?;
            let mut member: MemberRecord = bincode::deserialize(
                members
                    .get(upload.member_id.as_str())?
                    .context("missing member")?
                    .value(),
            )?;
            member.reserved = member
                .reserved
                .checked_sub(upload.size)
                .context("pool reserved bytes underflow")?;
            member.used = member
                .used
                .checked_add(upload.size)
                .context("pool used bytes overflow")?;
            members.insert(
                upload.member_id.as_str(),
                bincode::serialize(&member)?.as_slice(),
            )?;
        }
        tx.open_table(OBJECTS)?.insert(
            upload.location.as_str(),
            bincode::serialize(entry)?.as_slice(),
        )?;
        tx.open_table(UPLOADS)?.remove(id)?;
        tx.open_table(FAULTS)?.remove(upload.location.as_str())?;
        let garbage = previous.map(|entry| Garbage {
            id: Uuid::new_v4().to_string(),
            location: upload.location.clone(),
            entry: entry.clone(),
        });
        if let Some(garbage) = &garbage {
            tx.open_table(GARBAGE)?
                .insert(garbage.id.as_str(), bincode::serialize(garbage)?.as_slice())?;
            if matches!(garbage.entry.physical, Physical::Legacy { .. }) {
                tx.open_table(HIDDEN_LEGACY)?
                    .insert(garbage.location.as_str(), 1)?;
            }
        }
        tx.commit()?;
        Ok(garbage)
    }

    pub fn retire(&self, location: &str, entry: &Entry) -> Result<Garbage> {
        let garbage = Garbage {
            id: Uuid::new_v4().to_string(),
            location: location.to_owned(),
            entry: entry.clone(),
        };
        let mut tx = self.database.begin_write()?;
        tx.set_durability(Durability::Immediate)?;
        tx.open_table(OBJECTS)?.remove(location)?;
        tx.open_table(FAULTS)?.remove(location)?;
        tx.open_table(GARBAGE)?.insert(
            garbage.id.as_str(),
            bincode::serialize(&garbage)?.as_slice(),
        )?;
        if matches!(entry.physical, Physical::Legacy { .. }) {
            tx.open_table(HIDDEN_LEGACY)?.insert(location, 1)?;
        }
        tx.commit()?;
        Ok(garbage)
    }

    pub fn forget_upload(&self, id: &str) -> Result<()> {
        let mut tx = self.database.begin_write()?;
        tx.set_durability(Durability::Immediate)?;
        let upload: Option<Upload> = tx
            .open_table(UPLOADS)?
            .get(id)?
            .map(|value| bincode::deserialize(value.value()))
            .transpose()?;
        if let Some(upload) = upload {
            let mut table = tx.open_table(MEMBERS)?;
            let mut member: MemberRecord = bincode::deserialize(
                table
                    .get(upload.member_id.as_str())?
                    .context("missing member")?
                    .value(),
            )?;
            member.reserved = member
                .reserved
                .checked_sub(upload.size)
                .context("pool reserved bytes underflow")?;
            table.insert(
                upload.member_id.as_str(),
                bincode::serialize(&member)?.as_slice(),
            )?;
            drop(table);
            tx.open_table(UPLOADS)?.remove(id)?;
        }
        tx.commit()?;
        Ok(())
    }

    pub fn forget_garbage(&self, id: &str) -> Result<()> {
        let mut tx = self.database.begin_write()?;
        tx.set_durability(Durability::Immediate)?;
        let garbage: Option<Garbage> = tx
            .open_table(GARBAGE)?
            .get(id)?
            .map(|value| bincode::deserialize(value.value()))
            .transpose()?;
        if let Some(garbage) = garbage {
            if let Some(id) = garbage.entry.physical.member_id() {
                let mut table = tx.open_table(MEMBERS)?;
                let mut member: MemberRecord =
                    bincode::deserialize(table.get(id)?.context("missing member")?.value())?;
                member.used = member
                    .used
                    .checked_sub(garbage.entry.size)
                    .context("pool used bytes underflow")?;
                table.insert(id, bincode::serialize(&member)?.as_slice())?;
            } else {
                tx.open_table(HIDDEN_LEGACY)?
                    .remove(garbage.location.as_str())?;
            }
            tx.open_table(GARBAGE)?.remove(garbage.id.as_str())?;
        }
        tx.commit()?;
        Ok(())
    }

    pub fn recovery(&self) -> Result<(Vec<Upload>, Vec<Garbage>)> {
        let tx = self.database.begin_read()?;
        let mut uploads = Vec::new();
        for row in tx.open_table(UPLOADS)?.iter()? {
            uploads.push(bincode::deserialize(row?.1.value())?);
        }
        let mut garbage = Vec::new();
        for row in tx.open_table(GARBAGE)?.iter()? {
            garbage.push(bincode::deserialize(row?.1.value())?);
        }
        Ok((uploads, garbage))
    }

    pub fn snapshot(&self, prefix: Option<&str>) -> Result<Snapshot> {
        let tx = self.database.begin_read()?;
        let start = prefix.unwrap_or("");
        let mut entries = BTreeMap::new();
        for row in tx.open_table(OBJECTS)?.range(start..)? {
            let (key, value) = row?;
            if !key.value().starts_with(start) {
                break;
            }
            entries.insert(key.value().to_owned(), bincode::deserialize(value.value())?);
        }
        let mut hidden = BTreeSet::new();
        for row in tx.open_table(HIDDEN_LEGACY)?.range(start..)? {
            let (key, _) = row?;
            if !key.value().starts_with(start) {
                break;
            }
            hidden.insert(key.value().to_owned());
        }
        Ok(Snapshot { entries, hidden })
    }

    pub fn placement_page(&self, after: &str) -> Result<Vec<PoolPlacement>> {
        let tx = self.database.begin_read()?;
        let table = tx.open_table(OBJECTS)?;
        let mut result = Vec::new();
        for row in table.range(after..)? {
            let (key, value) = row?;
            if key.value() == after {
                continue;
            }
            let entry: Entry = bincode::deserialize(value.value())?;
            result.push(PoolPlacement {
                physical_key: entry.physical.key().to_owned(),
                location: Path::parse(key.value())?,
                member_id: entry
                    .physical
                    .member_id()
                    .context("placement has no member")?
                    .into(),
                size: entry.size,
            });
            if result.len() == 128 {
                break;
            }
        }
        Ok(result)
    }

    pub fn clear_fault_if_current(&self, location: &str, physical_key: &str) -> Result<()> {
        let mut tx = self.database.begin_write()?;
        tx.set_durability(Durability::Immediate)?;
        let entry: Option<Entry> = tx
            .open_table(OBJECTS)?
            .get(location)?
            .map(|v| bincode::deserialize(v.value()))
            .transpose()?;
        let fault: Option<SegmentFault> = tx
            .open_table(FAULTS)?
            .get(location)?
            .map(|v| bincode::deserialize(v.value()))
            .transpose()?;
        if entry.is_some_and(|e| e.physical.key() == physical_key)
            && fault.is_some_and(|f| f.physical_key == physical_key)
        {
            tx.open_table(FAULTS)?.remove(location)?;
        }
        tx.commit()?;
        Ok(())
    }

    pub fn source_fault(&self, location: &str, physical_key: &str) -> Result<Option<SegmentFault>> {
        let tx = self.database.begin_read()?;
        let fault: Option<SegmentFault> = tx
            .open_table(FAULTS)?
            .get(location)?
            .map(|v| bincode::deserialize(v.value()))
            .transpose()?;
        Ok(fault.filter(|f| f.physical_key == physical_key))
    }

    pub fn placements(&self) -> Result<Vec<PoolPlacement>> {
        self.snapshot(None)?
            .entries
            .into_iter()
            .map(|(key, entry)| {
                Ok(PoolPlacement {
                    physical_key: entry.physical.key().to_owned(),
                    location: Path::parse(key).context("invalid logical path in pool catalog")?,
                    size: entry.size,
                    member_id: entry
                        .physical
                        .member_id()
                        .context("mapped object has no member")?
                        .to_owned(),
                })
            })
            .collect()
    }

    pub fn status(
        &self,
        order: &[PoolMember],
        active: Option<&super::EvacuationProgress>,
    ) -> Result<PoolStatus> {
        let tx = self.database.begin_read()?;
        let mut counts = BTreeMap::<String, u64>::new();
        let mut mapped_objects = 0_u64;
        let mut quarantined_leftovers = Vec::new();
        let health_table = tx.open_table(HEALTH)?;
        for row in tx.open_table(OBJECTS)?.iter()? {
            let (key, value) = row?;
            let entry: Entry = bincode::deserialize(value.value())?;
            let id = entry
                .physical
                .member_id()
                .context("mapped object has no member")?;
            let health: MemberHealth = health_table
                .get(id)?
                .map(|v| bincode::deserialize(v.value()))
                .transpose()?
                .unwrap_or_default();
            if health.quarantine.is_some() {
                quarantined_leftovers.push(PoolPlacement {
                    physical_key: entry.physical.key().to_owned(),
                    location: Path::parse(key.value())?,
                    member_id: id.into(),
                    size: entry.size,
                });
            }
            *counts.entry(id.to_owned()).or_default() += 1;
            mapped_objects += 1;
        }
        let mut unreadable = Vec::new();
        let object_table = tx.open_table(OBJECTS)?;
        for row in tx.open_table(FAULTS)?.iter()? {
            let fault: SegmentFault = bincode::deserialize(row?.1.value())?;
            let entry: Option<Entry> = object_table
                .get(fault.location.as_str())?
                .map(|v| bincode::deserialize(v.value()))
                .transpose()?;
            if entry.is_some_and(|e| e.physical.key() == fault.physical_key) {
                unreadable.push(fault);
            }
        }
        let mut upload_counts = BTreeMap::<String, u64>::new();
        let mut evacuation_reservations = BTreeMap::<String, BTreeMap<String, u64>>::new();
        for row in tx.open_table(UPLOADS)?.iter()? {
            let upload: Upload = bincode::deserialize(row?.1.value())?;
            let target_health: MemberHealth = health_table
                .get(upload.member_id.as_str())?
                .map(|v| bincode::deserialize(v.value()))
                .transpose()?
                .unwrap_or_default();
            if active.is_some_and(|a| {
                a.upload_id.as_deref() == Some(upload.id.as_str()) && a.location == upload.location
            }) && target_health.quarantine.is_none()
                && target_health.monitoring_error.is_none()
                && let Some(value) = object_table.get(upload.location.as_str())?
            {
                let entry: Entry = bincode::deserialize(value.value())?;
                if let Some(source) = entry.physical.member_id()
                    && source != upload.member_id
                    && active.is_some_and(|a| {
                        a.member_id == source && a.source_key == entry.physical.key()
                    })
                {
                    evacuation_reservations
                        .entry(source.into())
                        .or_default()
                        .entry(upload.location.clone())
                        .or_insert(entry.size.min(upload.size));
                }
            }
            *upload_counts.entry(upload.member_id).or_default() += 1;
        }
        let mut garbage_counts = BTreeMap::<String, u64>::new();
        for row in tx.open_table(GARBAGE)?.iter()? {
            let garbage: Garbage = bincode::deserialize(row?.1.value())?;
            if let Some(member) = garbage.entry.physical.member_id() {
                *garbage_counts.entry(member.into()).or_default() += 1;
            }
        }
        let table = tx.open_table(MEMBERS)?;
        let mut members = Vec::new();
        for member in order {
            let record: MemberRecord = bincode::deserialize(
                table
                    .get(member.id.as_str())?
                    .context("missing member")?
                    .value(),
            )?;
            let health: MemberHealth = tx
                .open_table(HEALTH)?
                .get(record.id.as_str())?
                .map(|v| bincode::deserialize(v.value()))
                .transpose()?
                .unwrap_or_default();
            let objects = counts.get(&record.id).copied().unwrap_or(0);
            let unreadable_objects = unreadable
                .iter()
                .filter(|f| f.member_id == record.id)
                .count() as u64;
            let pending_uploads = upload_counts.get(&record.id).copied().unwrap_or(0);
            let cleanup_debts = garbage_counts.get(&record.id).copied().unwrap_or(0);
            let readable_evacuated = health.quarantine.is_some()
                && objects == unreadable_objects
                && record.reserved == 0
                && pending_uploads == 0;
            let loss_bytes = unreadable
                .iter()
                .filter(|f| f.member_id == record.id)
                .try_fold(0_u64, |sum, f| -> Result<u64> {
                    let entry: Entry = bincode::deserialize(
                        object_table
                            .get(f.location.as_str())?
                            .context("fault placement missing")?
                            .value(),
                    )?;
                    sum.checked_add(entry.size)
                        .context("fault byte count overflow")
                })?;
            let safe_to_pull_with_known_loss = readable_evacuated
                && unreadable_objects > 0
                && record.used == loss_bytes
                && cleanup_debts == 0;
            let safe_to_remove = health.quarantine.is_some()
                && record.used == 0
                && record.reserved == 0
                && objects == 0
                && pending_uploads == 0
                && cleanup_debts == 0;
            let evacuation = if health.quarantine.is_none() {
                "not_quarantined"
            } else if safe_to_remove {
                "complete"
            } else if readable_evacuated && unreadable_objects > 0 {
                "readable_evacuated_loss_blocked"
            } else if objects == 0 {
                "cleanup_pending"
            } else if health.evacuation_error.as_deref() == Some("capacity_or_allocation_blocked") {
                "blocked_capacity_or_admission"
            } else if health.evacuation_error.is_some() {
                "blocked_destination"
            } else {
                "incomplete"
            }
            .to_owned();
            let reserved_destination_bytes = evacuation_reservations
                .get(&record.id)
                .map(|entries| {
                    entries
                        .values()
                        .fold(0_u64, |sum, size| sum.saturating_add(*size))
                })
                .unwrap_or(0);
            members.push(PoolMemberStatus {
                id: record.id.clone(),
                capacity_bytes: record.capacity,
                used_bytes: record.used,
                reserved_bytes: record.reserved,
                objects,
                evacuation_capacity: EvacuationCapacity {
                    required_bytes: if health.quarantine.is_some() {
                        record.used.saturating_sub(reserved_destination_bytes)
                    } else {
                        0
                    },
                    reserved_destination_bytes,
                    ..Default::default()
                },
                available_bytes: 0,
                health,
                safe_to_remove,
                safe_to_pull_with_known_loss,
                readable_evacuated,
                unreadable_objects,
                pending_uploads,
                cleanup_debts,
                evacuation,
            });
        }
        super::capacity::assess(order, &mut members);
        let multiple_failures = members
            .iter()
            .filter(|member| {
                member.health.quarantine.is_some()
                    && (member.objects > 0
                        || member.used_bytes > 0
                        || member.reserved_bytes > 0
                        || member.pending_uploads > 0
                        || member.cleanup_debts > 0)
            })
            .count()
            > 1;
        for member in &mut members {
            if member.health.quarantine.is_none() || member.safe_to_remove {
                continue;
            }
            if multiple_failures {
                member.evacuation = "failed_multiple_devices".into();
                member.safe_to_pull_with_known_loss = false;
            } else if member.objects > 0
                && !member.readable_evacuated
                && !member.evacuation_capacity.sufficient
            {
                member.evacuation = "cannot_complete_capacity".into();
                member.safe_to_remove = false;
                member.safe_to_pull_with_known_loss = false;
            }
        }
        let pending_operations = usize::try_from(tx.open_table(UPLOADS)?.len()?)?
            .checked_add(usize::try_from(tx.open_table(GARBAGE)?.len()?)?)
            .context("pending count overflow")?;
        Ok(PoolStatus {
            pool_id: self.identity.pool_id.clone(),
            members,
            pending_operations,
            mapped_objects,
            unreadable,
            quarantined_leftovers,
        })
    }
}

fn health_in(tx: &redb::WriteTransaction, member: &str) -> Result<MemberHealth> {
    Ok(tx
        .open_table(HEALTH)?
        .get(member)?
        .map(|v| bincode::deserialize(v.value()))
        .transpose()?
        .unwrap_or_default())
}

impl Catalog {
    /// Runs before any writes in the same transaction as automatic upload-start
    /// or remap. Zero-byte operations count, while an empty historic quarantine
    /// needs a full ownership scan only once per catalog open.
    fn ensure_single_active_failure(&self, tx: &redb::WriteTransaction) -> Result<()> {
        let mut known_empty = self
            .verified_empty_quarantined
            .lock()
            .map_err(|_| anyhow::anyhow!("empty quarantined-member cache is poisoned"))?;
        let mut active = BTreeSet::new();
        let mut unchecked = BTreeSet::new();
        for row in tx.open_table(MEMBERS)?.iter()? {
            let member: MemberRecord = bincode::deserialize(row?.1.value())?;
            if health_in(tx, &member.id)?.quarantine.is_none() {
                known_empty.remove(&member.id);
                continue;
            }
            if member.used > 0 || member.reserved > 0 {
                // Keep a contradictory durable count authoritative even if a
                // future caller violates the sticky-quarantine invariant.
                known_empty.remove(&member.id);
                active.insert(member.id);
            } else if !known_empty.contains(&member.id) {
                unchecked.insert(member.id);
            }
        }
        if !unchecked.is_empty() {
            #[cfg(test)]
            self.empty_ownership_scans
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            for row in tx.open_table(OBJECTS)?.iter()? {
                let entry: Entry = bincode::deserialize(row?.1.value())?;
                if let Some(id) = entry.physical.member_id()
                    && unchecked.remove(id)
                {
                    active.insert(id.to_owned());
                }
                if unchecked.is_empty() {
                    break;
                }
            }
            if !unchecked.is_empty() {
                for row in tx.open_table(UPLOADS)?.iter()? {
                    let upload: Upload = bincode::deserialize(row?.1.value())?;
                    if unchecked.remove(&upload.member_id) {
                        active.insert(upload.member_id);
                    }
                    if unchecked.is_empty() {
                        break;
                    }
                }
            }
            if !unchecked.is_empty() {
                for row in tx.open_table(GARBAGE)?.iter()? {
                    let garbage: Garbage = bincode::deserialize(row?.1.value())?;
                    if let Some(id) = garbage.entry.physical.member_id()
                        && unchecked.remove(id)
                    {
                        active.insert(id.to_owned());
                    }
                    if unchecked.is_empty() {
                        break;
                    }
                }
            }
            // Every remaining member is quarantined and has no object, upload,
            // or garbage row. Existing ownership never disappears before its
            // successor debt is recorded, and no new target is admitted after
            // quarantine. Since this check precedes transaction mutations,
            // aborting the caller cannot invalidate the observed emptiness.
            known_empty.extend(unchecked);
        }
        if active.len() > 1 {
            return Err(AutomaticEvacuationBlocked.into());
        }
        Ok(())
    }
}

fn ensure_eligible(tx: &redb::WriteTransaction, member: &str) -> Result<()> {
    let health = health_in(tx, member)?;
    if let Some(reason) = health.quarantine.or(health.monitoring_error) {
        return Err(AllocationBlocked {
            member: member.into(),
            reason,
        }
        .into());
    }
    Ok(())
}

#[cfg(test)]
mod catalog_lock_tests {
    use super::*;

    #[test]
    fn catalog_release_does_not_wait_for_inherited_descriptors() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("catalog");
        let catalog = Catalog::open(&root, "db", true, None).unwrap();
        let member = PoolMember {
            id: "hdd-a".into(),
            identity: "memory://hdd-a".into(),
            store: std::sync::Arc::new(object_store::memory::InMemory::new()),
            capacity_bytes: 1024,
            local_path: None,
            min_free_bytes: 0,
            health_device: None,
        };
        catalog.configure_members(&[member]).unwrap();
        catalog.finish_initialization().unwrap();
        catalog
            .kernel_observation("hdd-a", "available".into(), Some(("boot-a".into(), 42)))
            .unwrap();
        let marker = catalog.identity.marker().unwrap();
        let pool_id = catalog.identity.pool_id.clone();

        // fork() duplicates the open file description before CLOEXEC can close
        // it. A duplicated descriptor models that interval without forking a
        // multithreaded test process or relying on scheduler timing.
        let inherited_lock = catalog._lock.0.try_clone().unwrap();
        let assert_locked = || {
            assert!(
                Catalog::open(&root, "db", false, Some(&marker))
                    .unwrap_err()
                    .to_string()
                    .contains("pool catalog is already locked by another process")
            );
        };
        assert_locked();
        drop(catalog);

        let reopened = Catalog::open(&root, "db", false, Some(&marker))
            .expect("closed catalog must release its lock while an inherited descriptor remains");
        assert_eq!(reopened.identity.pool_id, pool_id);
        assert!(reopened.identity.initialized);
        assert_eq!(reopened.member("hdd-a").unwrap().identity, "memory://hdd-a");
        let health: MemberHealth = {
            let read = reopened.database.begin_read().unwrap();
            bincode::deserialize(
                read.open_table(HEALTH)
                    .unwrap()
                    .get("hdd-a")
                    .unwrap()
                    .unwrap()
                    .value(),
            )
            .unwrap()
        };
        assert_eq!(health.kernel_cursor, Some(("boot-a".into(), 42)));
        assert_eq!(health.quarantine.as_deref(), Some("kernel block I/O error"));

        assert_locked();
        drop(inherited_lock);
        // Closing the previous generation's descriptor cannot release the new
        // owner's lock, which belongs to a separate open file description.
        assert_locked();
    }
}

#[cfg(test)]
mod health_format_tests {
    use super::*;
    #[test]
    fn disk_health_unknown_version_fails_closed() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("catalog");
        let catalog = Catalog::open(&root, "db", true, None).unwrap();
        let tx = catalog.database.begin_write().unwrap();
        tx.open_table(META)
            .unwrap()
            .insert(
                "health_format",
                bincode::serialize(&2_u32).unwrap().as_slice(),
            )
            .unwrap();
        tx.commit().unwrap();
        drop(catalog);
        assert!(
            Catalog::open(&root, "db", true, None)
                .unwrap_err()
                .to_string()
                .contains("unsupported pool health version")
        );
    }
}

#[cfg(test)]
mod legacy_health_compatibility {
    use super::*;
    use std::os::unix::fs::{DirBuilderExt, OpenOptionsExt};

    #[derive(Serialize)]
    enum OldPhysical {
        Member {
            member_id: String,
            key: String,
            e_tag: Option<String>,
            version: Option<String>,
        },
    }

    #[test]
    fn disk_health_opens_pre_health_catalog_records_without_reencoding() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("catalog");
        std::fs::DirBuilder::new()
            .mode(0o700)
            .create(&root)
            .unwrap();
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(root.join(CATALOG_FILE_NAME))
            .unwrap();
        let db = Builder::new().create_file(file).unwrap();
        let pool_id = "2f8cb3ea-bce5-43e0-bb27-6da74a919427";
        let directory = root.to_str().unwrap();
        let identity = bincode::serialize(&(1_u32, pool_id, "db", directory, true)).unwrap();
        let marker = bincode::serialize(&(1_u32, pool_id, "db", directory)).unwrap();
        let member = bincode::serialize(&(
            "old-disk",
            "memory://old-disk",
            "old-marker",
            true,
            1024_u64,
            7_u64,
            0_u64,
        ))
        .unwrap();
        let physical = OldPhysical::Member {
            member_id: "old-disk".into(),
            key: format!(".zerofs-pool/{pool_id}/objects/12c50a4d-1447-470d-bb7c-8b0a2e8887af"),
            e_tag: None,
            version: None,
        };
        let entry = bincode::serialize(&(
            physical,
            7_u64,
            123_i64,
            456_u32,
            Some("original-etag".to_owned()),
            None::<String>,
            None::<[u8; 32]>,
            Vec::<u8>::new(),
        ))
        .unwrap();
        let location = format!("db/{}", crate::segment::Segid::new(1, 1).object_key());
        let mut tx = db.begin_write().unwrap();
        tx.set_durability(Durability::Immediate).unwrap();
        tx.open_table(META)
            .unwrap()
            .insert("identity", identity.as_slice())
            .unwrap();
        tx.open_table(MEMBERS)
            .unwrap()
            .insert("old-disk", member.as_slice())
            .unwrap();
        tx.open_table(OBJECTS)
            .unwrap()
            .insert(location.as_str(), entry.as_slice())
            .unwrap();
        tx.open_table(UPLOADS).unwrap();
        tx.open_table(GARBAGE).unwrap();
        tx.open_table(HIDDEN_LEGACY).unwrap();
        tx.commit().unwrap();
        drop(db);
        let catalog = Catalog::open(&root, "db", false, Some(&marker)).unwrap();
        let config = PoolMember {
            id: "old-disk".into(),
            identity: "memory://old-disk".into(),
            store: std::sync::Arc::new(object_store::memory::InMemory::new()),
            capacity_bytes: 1024,
            local_path: None,
            min_free_bytes: 0,
            health_device: None,
        };
        let status = catalog.status(&[config], None).unwrap();
        assert_eq!(status.mapped_objects, 1);
        assert_eq!(status.members[0].used_bytes, 7);
        assert!(status.members[0].health.quarantine.is_none());
        let retained = catalog.lookup(&location).unwrap().entry.unwrap();
        assert_eq!(retained.e_tag.as_deref(), Some("original-etag"));
        assert_eq!(retained.last_modified_secs, 123);
        let read = catalog.database.begin_read().unwrap();
        assert_eq!(
            read.open_table(META)
                .unwrap()
                .get("identity")
                .unwrap()
                .unwrap()
                .value(),
            identity
        );
        assert_eq!(
            read.open_table(MEMBERS)
                .unwrap()
                .get("old-disk")
                .unwrap()
                .unwrap()
                .value(),
            member
        );
        assert_eq!(
            read.open_table(OBJECTS)
                .unwrap()
                .get(location.as_str())
                .unwrap()
                .unwrap()
                .value(),
            entry
        );
    }
}

#[cfg(test)]
mod empty_quarantine_tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::Ordering;

    fn fixture() -> (tempfile::TempDir, Catalog, Vec<PoolMember>) {
        let root = tempfile::tempdir().unwrap();
        let catalog = Catalog::open(&root.path().join("catalog"), "db", true, None).unwrap();
        let members = ["old", "current", "target"]
            .into_iter()
            .map(|id| PoolMember {
                id: id.into(),
                identity: format!("memory://{id}"),
                store: Arc::new(object_store::memory::InMemory::new()),
                capacity_bytes: 1024,
                local_path: None,
                min_free_bytes: 0,
                health_device: None,
            })
            .collect::<Vec<_>>();
        catalog.configure_members(&members).unwrap();
        catalog.finish_initialization().unwrap();
        (root, catalog, members)
    }

    fn insert(
        catalog: &Catalog,
        members: &[PoolMember],
        location: &str,
        member: &str,
        size: u64,
    ) -> Entry {
        let upload = catalog
            .reserve(location, size, members, Some(member))
            .unwrap();
        let entry = Entry {
            physical: Physical::Member {
                member_id: member.into(),
                key: upload.physical_key,
                e_tag: None,
                version: None,
            },
            size,
            last_modified_secs: 0,
            last_modified_nanos: 0,
            e_tag: None,
            version: None,
            sha256: None,
            attributes: Vec::new(),
        };
        catalog
            .publish_for_migration(&upload.id, &entry, None, false)
            .unwrap();
        entry
    }

    fn quarantine(catalog: &Catalog) {
        for member in ["old", "current"] {
            catalog
                .observe_health(member, None, Some("media failure".into()), None)
                .unwrap();
        }
    }

    #[test]
    fn empty_historical_quarantine_is_scanned_once_and_revalidated_after_reopen() {
        let (root, catalog, members) = fixture();
        insert(&catalog, &members, "current-data", "current", 8);
        quarantine(&catalog);
        catalog.require_automatic_evacuation().unwrap();
        assert!(
            catalog
                .verified_empty_quarantined
                .lock()
                .unwrap()
                .contains("old")
        );
        assert_eq!(catalog.empty_ownership_scans.load(Ordering::Relaxed), 1);
        for _ in 0..32 {
            catalog.require_automatic_evacuation().unwrap();
        }
        assert_eq!(catalog.empty_ownership_scans.load(Ordering::Relaxed), 1);
        assert!(
            catalog
                .reserve("new-data", 0, &members, Some("old"))
                .is_err()
        );
        let marker = catalog.identity.marker().unwrap();
        drop(catalog);
        let reopened =
            Catalog::open(&root.path().join("catalog"), "db", false, Some(&marker)).unwrap();
        assert!(
            reopened
                .verified_empty_quarantined
                .lock()
                .unwrap()
                .is_empty()
        );
        assert_eq!(reopened.empty_ownership_scans.load(Ordering::Relaxed), 0);
        reopened.require_automatic_evacuation().unwrap();
        assert_eq!(reopened.empty_ownership_scans.load(Ordering::Relaxed), 1);
        reopened.require_automatic_evacuation().unwrap();
        assert_eq!(reopened.empty_ownership_scans.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn zero_byte_object_upload_and_cleanup_debt_each_prevent_caching_empty() {
        let (_root, catalog, members) = fixture();
        let zero = insert(&catalog, &members, "zero-data", "old", 0);
        let upload = catalog
            .reserve("zero-upload", 0, &members, Some("old"))
            .unwrap();
        insert(&catalog, &members, "current-data", "current", 8);
        quarantine(&catalog);
        assert!(catalog.require_automatic_evacuation().is_err());
        assert!(
            !catalog
                .verified_empty_quarantined
                .lock()
                .unwrap()
                .contains("old")
        );
        let garbage = catalog.retire("zero-data", &zero).unwrap();
        // The upload is now the first remaining zero-byte owner encountered.
        assert!(catalog.require_automatic_evacuation().is_err());
        assert!(
            !catalog
                .verified_empty_quarantined
                .lock()
                .unwrap()
                .contains("old")
        );
        catalog.forget_upload(&upload.id).unwrap();
        // Only a cleanup debt remains; byte counters alone still say zero.
        assert!(catalog.require_automatic_evacuation().is_err());
        assert!(
            !catalog
                .verified_empty_quarantined
                .lock()
                .unwrap()
                .contains("old")
        );
        catalog.forget_garbage(&garbage.id).unwrap();
        catalog.require_automatic_evacuation().unwrap();
        assert!(
            catalog
                .verified_empty_quarantined
                .lock()
                .unwrap()
                .contains("old")
        );
        let scans = catalog.empty_ownership_scans.load(Ordering::Relaxed);
        assert_eq!(scans, 4);
        catalog.require_automatic_evacuation().unwrap();
        assert_eq!(catalog.empty_ownership_scans.load(Ordering::Relaxed), scans);
    }
}
