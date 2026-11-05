//! Durable placement, capacity reservations, and deletion debts.
//!
//! Every change that makes data visible is one immediate-durability redb
//! transaction. An unpublished upload remains reserved until its unique object
//! has been deleted. Superseded objects remain charged until cleanup succeeds.

use super::{PoolMember, PoolMemberStatus, PoolPlacement, PoolStatus};
use anyhow::{Context, Result, bail};
use fs4::fs_std::FileExt;
use object_store::path::Path;
use redb::{Builder, Database, Durability, ReadableDatabase, ReadableTable, ReadableTableMetadata, TableDefinition};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::fs::File;
use std::path::Path as FilePath;
use uuid::Uuid;

use crate::writeback::anchored_dir::AnchoredDir;

pub const CATALOG_FILE_NAME: &str = "pool.redb";
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
    pub initialized: bool,
}

impl Identity {
    pub fn marker(&self) -> Result<Vec<u8>> {
        Ok(bincode::serialize(&(self.format, &self.pool_id, &self.authority_prefix))?)
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
        Ok(bincode::serialize(&(1_u32, pool_id, &self.id, &self.marker_token))?)
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
    database: Database,
    pub identity: Identity,
    _root: AnchoredDir,
    _lock: File,
}

impl std::fmt::Debug for Catalog {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PoolCatalog").field("pool_id", &self.identity.pool_id).finish_non_exhaustive()
    }
}

impl Catalog {
    pub fn open(root: &FilePath, prefix: &str, initialize: bool, marker: Option<&[u8]>) -> Result<Self> {
        let existed = root.join(CATALOG_FILE_NAME).try_exists()?;
        if !existed && !initialize {
            bail!("pool catalog is missing; use `zerofs pool init` for a new pool, or restore the existing catalog");
        }
        if !existed && marker.is_some() {
            bail!("authority belongs to an existing pool but its catalog is missing; restore that catalog instead of initializing a new pool");
        }
        let anchored = if initialize {
            AnchoredDir::open_or_create_absolute(root, 0o700)?
        } else {
            AnchoredDir::open_absolute(root, 0o700)?
        };
        let (lock, _) = anchored.open_owner_file("pool.lock".as_ref(), true)?;
        if !FileExt::try_lock_exclusive(&lock)? {
            bail!("pool catalog is already locked by another process");
        }
        if !initialize {
            // Do not create a replacement database if the file disappeared
            // between the preliminary check and descriptor-anchored open.
            anchored.open_existing_owner_file_read_only(CATALOG_FILE_NAME.as_ref())?;
        }
        let (file, database_existed) = anchored.open_owner_file(CATALOG_FILE_NAME.as_ref(), true)?;
        if !database_existed && marker.is_some() {
            bail!("existing pool catalog disappeared while opening it");
        }
        let database = Builder::new().create_file(file).context("open pool catalog")?;
        if !database_existed {
            anchored.sync()?;
        }
        let mut tx = database.begin_write()?;
        tx.set_durability(Durability::Immediate)?;
        let identity = {
            let mut table = tx.open_table(META)?;
            let existing: Option<Identity> = table.get("identity")?
                .map(|value| bincode::deserialize(value.value())).transpose()?;
            let identity = match existing {
                Some(identity) => identity,
                None if initialize && marker.is_none() => Identity {
                    format: 1,
                    pool_id: Uuid::new_v4().to_string(),
                    authority_prefix: prefix.to_owned(),
                    initialized: false,
                },
                None => bail!("pool catalog has no identity; explicit initialization or recovery is required"),
            };
            if identity.format != 1 || identity.authority_prefix != prefix {
                bail!("pool catalog format or authority prefix does not match this configuration");
            }
            if let Some(marker) = marker {
                if marker != identity.marker()? {
                    bail!("pool catalog does not belong to this authority (pool identity mismatch)");
                }
            } else if identity.initialized {
                bail!("pool authority identity marker is missing; refusing to recreate an existing pool");
            }
            if !initialize && !identity.initialized {
                bail!("pool initialization did not finish; rerun `zerofs pool init` with the original configuration");
            }
            table.insert("identity", bincode::serialize(&identity)?.as_slice())?;
            identity
        };
        tx.open_table(MEMBERS)?;
        tx.open_table(OBJECTS)?;
        tx.open_table(UPLOADS)?;
        tx.open_table(GARBAGE)?;
        tx.open_table(HIDDEN_LEGACY)?;
        tx.commit()?;
        Ok(Self { database, identity, _root: anchored, _lock: lock })
    }

    pub fn finish_initialization(&self) -> Result<()> {
        let mut identity = self.identity.clone();
        identity.initialized = true;
        let mut tx = self.database.begin_write()?;
        tx.set_durability(Durability::Immediate)?;
        tx.open_table(META)?.insert("identity", bincode::serialize(&identity)?.as_slice())?;
        tx.commit()?;
        Ok(())
    }

    pub fn configure_members(&self, members: &[PoolMember]) -> Result<()> {
        let mut ids = BTreeSet::new();
        let mut identities = BTreeSet::new();
        for member in members {
            if member.id.is_empty() || member.id.len() > 64
                || !member.id.bytes().all(|c| c.is_ascii_alphanumeric() || c == b'-' || c == b'_')
            {
                bail!("pool member id must contain 1-64 ASCII letters, digits, '-' or '_'");
            }
            if member.id == "authority" {
                bail!("pool member id 'authority' is reserved for legacy local segments");
            }
            if member.capacity_bytes == 0 || member.identity.is_empty() {
                bail!("pool member {} needs a positive capacity and stable identity", member.id);
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
                    bail!("configured pool member {} is missing; members may be added or reordered but not silently removed", key.value());
                }
            }
            for member in members {
                let existing: Option<MemberRecord> = table.get(member.id.as_str())?
                    .map(|value| bincode::deserialize(value.value())).transpose()?;
                let record = match existing {
                    Some(mut record) => {
                        if record.identity != member.identity {
                            bail!("pool member {} changed backend identity; add a new member id and migrate instead", member.id);
                        }
                        let occupied = record.used.checked_add(record.reserved).context("pool usage overflow")?;
                        if member.capacity_bytes < occupied {
                            bail!("pool member {} capacity is below its used and reserved bytes", member.id);
                        }
                        record.capacity = member.capacity_bytes;
                        record
                    }
                    None => MemberRecord {
                        id: member.id.clone(), identity: member.identity.clone(),
                        marker_token: Uuid::new_v4().to_string(), initialized: false,
                        capacity: member.capacity_bytes, used: 0, reserved: 0,
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
        let value = table.get(id)?.with_context(|| format!("pool member {id} is not registered"))?;
        Ok(bincode::deserialize(value.value())?)
    }

    pub fn mark_member_initialized(&self, id: &str) -> Result<()> {
        let mut tx = self.database.begin_write()?;
        tx.set_durability(Durability::Immediate)?;
        {
            let mut table = tx.open_table(MEMBERS)?;
            let mut member: MemberRecord = bincode::deserialize(table.get(id)?.context("missing pool member")?.value())?;
            member.initialized = true;
            table.insert(id, bincode::serialize(&member)?.as_slice())?;
        }
        tx.commit()?;
        Ok(())
    }

    pub fn lookup(&self, location: &str) -> Result<Lookup> {
        let tx = self.database.begin_read()?;
        let objects = tx.open_table(OBJECTS)?;
        let entry = objects.get(location)?.map(|v| bincode::deserialize(v.value())).transpose()?;
        let legacy_hidden = tx.open_table(HIDDEN_LEGACY)?.get(location)?.is_some();
        Ok(Lookup { entry, legacy_hidden })
    }

    pub fn reserve(&self, location: &str, size: u64, members: &[PoolMember], target: Option<&str>) -> Result<Upload> {
        let mut tx = self.database.begin_write()?;
        tx.set_durability(Durability::Immediate)?;
        let upload = {
            let mut table = tx.open_table(MEMBERS)?;
            // Conservatively share the reservation budget across all local
            // paths. In particular, two directories on the same filesystem
            // must not both promise the same currently-free physical bytes.
            let mut local_reserved = 0_u64;
            for member in members.iter().filter(|member| member.local_path.is_some()) {
                let record: MemberRecord = bincode::deserialize(table.get(member.id.as_str())?
                    .context("unregistered pool member")?.value())?;
                local_reserved = local_reserved.checked_add(record.reserved).context("local pool reservation overflow")?;
            }
            let mut chosen = None;
            for member in members.iter().filter(|m| target.is_none_or(|id| id == m.id)) {
                let mut record: MemberRecord = bincode::deserialize(table.get(member.id.as_str())?
                    .context("unregistered pool member")?.value())?;
                let occupied = record.used.checked_add(record.reserved).context("pool usage overflow")?;
                if record.capacity.saturating_sub(occupied) < size {
                    continue;
                }
                if let Some(path) = &member.local_path {
                    let available = fs4::available_space(path).with_context(|| format!("check free space for pool member {}", member.id))?;
                    // Outstanding reservations may not have reached the device
                    // yet. Counting all of them is deliberately conservative.
                    let required = member.min_free_bytes.checked_add(local_reserved)
                        .and_then(|n| n.checked_add(size)).context("pool free-space reservation overflow")?;
                    if available < required {
                        continue;
                    }
                }
                record.reserved = record.reserved.checked_add(size).context("pool reservation overflow")?;
                table.insert(member.id.as_str(), bincode::serialize(&record)?.as_slice())?;
                chosen = Some(member.id.clone());
                break;
            }
            let member_id = chosen.context("no pool member has sufficient capacity and local free-space reserve")?;
            let id = Uuid::new_v4().to_string();
            Upload {
                physical_key: format!(".zerofs-pool/{}/objects/{id}", self.identity.pool_id),
                id, location: location.to_owned(), member_id, size, started: false,
            }
        };
        tx.open_table(UPLOADS)?.insert(upload.id.as_str(), bincode::serialize(&upload)?.as_slice())?;
        tx.commit()?;
        Ok(upload)
    }

    pub fn mark_started(&self, id: &str) -> Result<()> {
        let mut tx = self.database.begin_write()?;
        tx.set_durability(Durability::Immediate)?;
        {
            let mut table = tx.open_table(UPLOADS)?;
            let mut upload: Upload = bincode::deserialize(table.get(id)?.context("pool upload reservation is missing")?.value())?;
            upload.started = true;
            table.insert(id, bincode::serialize(&upload)?.as_slice())?;
        }
        tx.commit()?;
        Ok(())
    }

    pub fn publish(&self, id: &str, entry: &Entry, previous: Option<&Entry>) -> Result<Option<Garbage>> {
        let mut tx = self.database.begin_write()?;
        tx.set_durability(Durability::Immediate)?;
        let upload: Upload = {
            let table = tx.open_table(UPLOADS)?;
            bincode::deserialize(table.get(id)?.context("pool upload reservation is missing")?.value())?
        };
        if upload.size != entry.size || entry.physical.key() != upload.physical_key
            || entry.physical.member_id() != Some(upload.member_id.as_str())
        {
            bail!("pool publication does not match its durable reservation");
        }
        {
            let mut members = tx.open_table(MEMBERS)?;
            let mut member: MemberRecord = bincode::deserialize(members.get(upload.member_id.as_str())?.context("missing member")?.value())?;
            member.reserved = member.reserved.checked_sub(upload.size).context("pool reserved bytes underflow")?;
            member.used = member.used.checked_add(upload.size).context("pool used bytes overflow")?;
            members.insert(upload.member_id.as_str(), bincode::serialize(&member)?.as_slice())?;
        }
        tx.open_table(OBJECTS)?.insert(upload.location.as_str(), bincode::serialize(entry)?.as_slice())?;
        tx.open_table(UPLOADS)?.remove(id)?;
        let garbage = previous.map(|entry| Garbage {
            id: Uuid::new_v4().to_string(), location: upload.location.clone(), entry: entry.clone(),
        });
        if let Some(garbage) = &garbage {
            tx.open_table(GARBAGE)?.insert(garbage.id.as_str(), bincode::serialize(garbage)?.as_slice())?;
            if matches!(garbage.entry.physical, Physical::Legacy { .. }) {
                tx.open_table(HIDDEN_LEGACY)?.insert(garbage.location.as_str(), 1)?;
            }
        }
        tx.commit()?;
        Ok(garbage)
    }

    pub fn retire(&self, location: &str, entry: &Entry) -> Result<Garbage> {
        let garbage = Garbage { id: Uuid::new_v4().to_string(), location: location.to_owned(), entry: entry.clone() };
        let mut tx = self.database.begin_write()?;
        tx.set_durability(Durability::Immediate)?;
        tx.open_table(OBJECTS)?.remove(location)?;
        tx.open_table(GARBAGE)?.insert(garbage.id.as_str(), bincode::serialize(&garbage)?.as_slice())?;
        if matches!(entry.physical, Physical::Legacy { .. }) {
            tx.open_table(HIDDEN_LEGACY)?.insert(location, 1)?;
        }
        tx.commit()?;
        Ok(garbage)
    }

    pub fn forget_upload(&self, id: &str) -> Result<()> {
        let mut tx = self.database.begin_write()?;
        tx.set_durability(Durability::Immediate)?;
        let upload: Option<Upload> = tx.open_table(UPLOADS)?.get(id)?
            .map(|value| bincode::deserialize(value.value())).transpose()?;
        if let Some(upload) = upload {
            let mut table = tx.open_table(MEMBERS)?;
            let mut member: MemberRecord = bincode::deserialize(table.get(upload.member_id.as_str())?.context("missing member")?.value())?;
            member.reserved = member.reserved.checked_sub(upload.size).context("pool reserved bytes underflow")?;
            table.insert(upload.member_id.as_str(), bincode::serialize(&member)?.as_slice())?;
            drop(table);
            tx.open_table(UPLOADS)?.remove(id)?;
        }
        tx.commit()?;
        Ok(())
    }

    pub fn forget_garbage(&self, id: &str) -> Result<()> {
        let mut tx = self.database.begin_write()?;
        tx.set_durability(Durability::Immediate)?;
        let garbage: Option<Garbage> = tx.open_table(GARBAGE)?.get(id)?
            .map(|value| bincode::deserialize(value.value())).transpose()?;
        if let Some(garbage) = garbage {
            if let Some(id) = garbage.entry.physical.member_id() {
                let mut table = tx.open_table(MEMBERS)?;
                let mut member: MemberRecord = bincode::deserialize(table.get(id)?.context("missing member")?.value())?;
                member.used = member.used.checked_sub(garbage.entry.size).context("pool used bytes underflow")?;
                table.insert(id, bincode::serialize(&member)?.as_slice())?;
            } else {
                tx.open_table(HIDDEN_LEGACY)?.remove(garbage.location.as_str())?;
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
            if !key.value().starts_with(start) { break; }
            entries.insert(key.value().to_owned(), bincode::deserialize(value.value())?);
        }
        let mut hidden = BTreeSet::new();
        for row in tx.open_table(HIDDEN_LEGACY)?.range(start..)? {
            let (key, _) = row?;
            if !key.value().starts_with(start) { break; }
            hidden.insert(key.value().to_owned());
        }
        Ok(Snapshot { entries, hidden })
    }

    pub fn placements(&self) -> Result<Vec<PoolPlacement>> {
        self.snapshot(None)?.entries.into_iter().map(|(key, entry)| Ok(PoolPlacement {
            location: Path::parse(key).context("invalid logical path in pool catalog")?, size: entry.size,
            member_id: entry.physical.member_id().context("mapped object has no member")?.to_owned(),
        })).collect()
    }

    pub fn status(&self, order: &[PoolMember]) -> Result<PoolStatus> {
        let tx = self.database.begin_read()?;
        let mut counts = BTreeMap::<String, u64>::new();
        let mut mapped_objects = 0_u64;
        for row in tx.open_table(OBJECTS)?.iter()? {
            let entry: Entry = bincode::deserialize(row?.1.value())?;
            let id = entry.physical.member_id().context("mapped object has no member")?;
            *counts.entry(id.to_owned()).or_default() += 1;
            mapped_objects += 1;
        }
        let table = tx.open_table(MEMBERS)?;
        let mut members = Vec::new();
        for member in order {
            let record: MemberRecord = bincode::deserialize(table.get(member.id.as_str())?.context("missing member")?.value())?;
            members.push(PoolMemberStatus { id: record.id.clone(), capacity_bytes: record.capacity,
                used_bytes: record.used, reserved_bytes: record.reserved,
                objects: counts.get(&record.id).copied().unwrap_or(0) });
        }
        let pending_operations = usize::try_from(tx.open_table(UPLOADS)?.len()?)?
            .checked_add(usize::try_from(tx.open_table(GARBAGE)?.len()?)?).context("pending count overflow")?;
        Ok(PoolStatus { pool_id: self.identity.pool_id.clone(), members, pending_operations, mapped_objects })
    }
}
