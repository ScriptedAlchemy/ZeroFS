//! Fenced recovery from surviving metadata replicas. It runs only when an
//! operator asks for it; ordinary startup never recovers.
//!
//! Recovery takes shared fences on existing replica regions, reads every
//! source without modifying it, authenticates identity, key and commit
//! records with a key derived from the password-unwrapped data key, refuses
//! forks and corrupt latest generations, and verifies the digest of every
//! byte of the selected generation before it writes anything. The key is
//! unwrapped from the copied original envelope and is never minted.
//!
//! Two targets exist. [`recover_regions`] materializes the generation into
//! owned scratch and opens it read-only: the namespace from the generation's
//! named checkpoint, the catalog as an exact import without startup side
//! effects. [`rebuild_raw_p3`] rebuilds a lost authority NVMe onto a freshly
//! provisioned, blank P3 from the regions on the raw HDD members, so ordinary
//! startup works again.

use super::backend::{Fence, ReplicaBackend, ReplicaError};
use super::format::{self, MacKey, digest, get_hash, get_str, get_u64, hex_encode};
use super::{SurvivalFailure, catalog_object_path, key_path, object_path, replica_commits};
use crate::fs::ZeroFS;
use crate::fs::inode::Inode;
use crate::fs::key_codec::KeyCodec;
use crate::fs::types::AuthContext;
use crate::pool_object_store::{CatalogExport, PoolMember, PoolObjectStore};
use crate::segment::FrameLoc;
use anyhow::{Context, Result, anyhow, ensure};
use arc_swap::ArcSwap;
use object_store::ObjectStore;
use object_store::prefix::PrefixStore;
use serde::Serialize;
use serde_json::{Map, Value};
use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;

pub(crate) struct RecoverySources {
    /// Surviving `.zerofs-survival/v1` roots. Missing devices are omitted.
    pub(crate) replicas: Vec<PathBuf>,
    pub(crate) password: String,
    /// A healthy directory where recovery creates its own scratch child.
    pub(crate) scratch_parent: PathBuf,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub(crate) struct LostRange {
    pub(crate) names: Vec<String>,
    pub(crate) inode: u64,
    pub(crate) offset: u64,
    pub(crate) length: u32,
    pub(crate) member: Option<String>,
}

#[derive(Clone, Debug, Serialize)]
pub(crate) struct RecoveryReport {
    pub(crate) pool_id: String,
    pub(crate) sequence: u64,
    pub(crate) commit_digest: String,
    pub(crate) mutation_cutoff: u64,
    pub(crate) key_epoch: u64,
    pub(crate) created_at: u64,
    pub(crate) sources: Vec<String>,
    pub(crate) selected_from: String,
    pub(crate) unavailable_members: Vec<String>,
    pub(crate) lost_ranges: Vec<LostRange>,
    /// Writes acknowledged after this generation are not recovered,
    /// including any local-only writeback journal suffix. Recovery cannot
    /// count them without the lost journal and does not try to.
    pub(crate) writes_after_generation_not_recovered: bool,
    /// Authority and catalog bytes written to the target.
    pub(crate) restored_bytes: u64,
    /// The scratch directory or the rebuilt device.
    pub(crate) target: String,
    /// Files with at least one lost range, by every name. Empty when every
    /// member answered.
    pub(crate) unreadable_files: Vec<String>,
}

pub(crate) struct Recovered {
    pub(crate) fs: Arc<ZeroFS>,
    pub(crate) pool: PoolObjectStore,
    pub(crate) scratch: tempfile::TempDir,
    pub(crate) authority: PathBuf,
    pub(crate) commit_digest: [u8; 32],
    pub(crate) report: RecoveryReport,
    _locks: Vec<Box<dyn Fence>>,
}

fn fail(kind: SurvivalFailure, detail: impl std::fmt::Display) -> anyhow::Error {
    anyhow::Error::new(kind).context(detail.to_string())
}

async fn read_bounded(region: &dyn ReplicaBackend, key: &str) -> Result<Vec<u8>> {
    if let Some(len) = region.len(key).await? {
        ensure!(
            len as usize <= format::MAX_RECORD_BYTES,
            "{key} on {} is oversized",
            region.describe()
        );
    }
    Ok(region
        .get(key)
        .await?
        .with_context(|| format!("{key} is missing on {}", region.describe()))?
        .to_vec())
}

struct Source {
    region: Arc<dyn ReplicaBackend>,
    identity: Map<String, Value>,
    identity_bytes: Vec<u8>,
    commits: BTreeMap<u64, Vec<u8>>,
}

impl Source {
    fn name(&self) -> String {
        self.region.describe()
    }
}

/// The newest generation that authenticates and verifies completely on a
/// surviving region, with everything needed to open it.
struct Selected {
    pool_id: String,
    sequence: u64,
    record: Map<String, Value>,
    record_bytes: Vec<u8>,
    region: Arc<dyn ReplicaBackend>,
    plan: Plan,
    envelope: Vec<u8>,
    dek: [u8; 32],
    name_mode: crate::fs::name_mode::NameMode,
    bootstrap: Map<String, Value>,
    db_path: object_store::path::Path,
    sources: Vec<String>,
    locks: Vec<Box<dyn Fence>>,
}

/// Recover from surviving regions on the existing local backend, named by
/// their `.zerofs-survival/v1` roots.
pub(crate) async fn recover(sources: RecoverySources) -> Result<Recovered> {
    let regions = sources
        .replicas
        .iter()
        .map(|root| {
            Arc::new(super::replica::LocalDirectoryBackend::from_root(root))
                as Arc<dyn ReplicaBackend>
        })
        .collect();
    recover_regions(regions, &sources.password, &sources.scratch_parent).await
}

/// Authenticate every surviving region and select the generation to recover.
/// Reads only; nothing is written anywhere.
async fn select(regions: Vec<Arc<dyn ReplicaBackend>>, password: &str) -> Result<Selected> {
    if regions.is_empty() {
        return Err(fail(SurvivalFailure::Lost, "no metadata replica survived"));
    }
    let mut locks = Vec::new();
    let mut opened = Vec::new();
    for region in regions {
        // A shared fence on existing regions only; recovery never creates
        // or modifies source state.
        let fence = region.fence_shared().await.map_err(|error| {
            let kind = match error {
                ReplicaError::Locked => SurvivalFailure::Locked,
                ReplicaError::Absent => SurvivalFailure::Lost,
                _ => SurvivalFailure::Corrupt,
            };
            fail(kind, format!("{}: {error}", region.describe()))
        })?;
        locks.push(fence);
        if !region.list("terminal").await?.is_empty() {
            return Err(fail(
                SurvivalFailure::Disabled,
                format!("{} holds a terminal disable record", region.describe()),
            ));
        }
        let identity_bytes = read_bounded(region.as_ref(), "identity.json").await?;
        let identity = format::parse(&identity_bytes)
            .map_err(|e| fail(SurvivalFailure::Corrupt, format!("{e:#}")))?;
        opened.push(Source {
            region,
            commits: BTreeMap::new(),
            identity,
            identity_bytes,
        });
    }
    let first = &opened[0].identity;
    let pool_id = get_str(first, "pool_id")?.to_owned();
    let epoch = get_u64(first, "membership_epoch")?;
    let key_epoch = get_u64(first, "key_epoch")?;
    let membership = get_str(first, "membership_digest")?.to_owned();
    for source in &opened {
        let identity = &source.identity;
        if get_str(identity, "pool_id")? != pool_id
            || get_u64(identity, "membership_epoch")? != epoch
            || get_str(identity, "membership_digest")? != membership
            || get_u64(identity, "key_epoch")? != key_epoch
        {
            return Err(fail(
                SurvivalFailure::Configuration,
                "surviving replicas belong to different pools or membership epochs",
            ));
        }
    }
    let bootstrap = first
        .get("bootstrap")
        .and_then(Value::as_object)
        .context("identity has no bootstrap section")?
        .clone();
    let db_path = object_store::path::Path::from(get_str(&bootstrap, "db_path")?);

    // Bootstrap the original envelope from any survivor that has it.
    let mut key_bytes = None;
    for source in &opened {
        if let Ok(bytes) = read_bounded(
            source.region.as_ref(),
            &super::replica::key(&key_path(key_epoch)),
        )
        .await
        {
            key_bytes = Some(bytes);
            break;
        }
    }
    let key_bytes =
        key_bytes.ok_or_else(|| fail(SurvivalFailure::Corrupt, "no surviving key record"))?;
    let key_record = format::parse(&key_bytes)?;
    let envelope = format::hex_decode(get_str(&key_record, "envelope_hex")?)
        .context("key envelope is not hex")?;
    ensure!(
        digest(&envelope) == get_hash(&key_record, "envelope_sha256")?
            && digest(&envelope) == get_hash(first, "key_sha256")?,
        fail(SurvivalFailure::Corrupt, "key envelope digest mismatch")
    );

    // The existing-key path: a missing envelope is an error and no key is
    // ever generated here.
    let unwrap_store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    object_store::ObjectStoreExt::put(
        unwrap_store.as_ref(),
        &db_path
            .clone()
            .join(crate::key_management::WRAPPED_KEY_FILENAME),
        envelope.clone().into(),
    )
    .await?;
    let (dek, name_mode) = crate::key_management::load_existing_encryption_key_recorded_mode(
        &unwrap_store,
        &db_path,
        password,
    )
    .await
    .context("unwrap the recovered key envelope with the supplied password")?;
    let mac = MacKey::derive(&dek, &pool_id);
    format::open(&key_bytes, &mac)
        .map_err(|e| fail(SurvivalFailure::Corrupt, format!("key record: {e:#}")))?;
    for source in &mut opened {
        format::open(&source.identity_bytes, &mac).map_err(|e| {
            fail(
                SurvivalFailure::Corrupt,
                format!("{} identity: {e:#}", source.name()),
            )
        })?;
        source.commits = replica_commits(source.region.as_ref(), epoch).await?;
    }

    // Authenticate every record and refuse conflicting copies or forks.
    let mut union: BTreeMap<u64, Vec<u8>> = BTreeMap::new();
    for source in &opened {
        for (sequence, bytes) in &source.commits {
            let record = format::open(bytes, &mac).map_err(|e| {
                fail(
                    SurvivalFailure::Corrupt,
                    format!("{} commit {sequence}: {e:#}", source.name()),
                )
            })?;
            ensure!(
                get_u64(&record, "sequence")? == *sequence
                    && get_str(&record, "pool_id")? == pool_id
                    && get_u64(&record, "membership_epoch")? == epoch,
                fail(
                    SurvivalFailure::Corrupt,
                    format!("commit {sequence} binding mismatch")
                )
            );
            match union.get(sequence) {
                Some(existing) if existing != bytes => {
                    return Err(fail(
                        SurvivalFailure::Fork,
                        format!("two authenticated records claim generation {sequence}"),
                    ));
                }
                Some(_) => {}
                None => {
                    union.insert(*sequence, bytes.clone());
                }
            }
        }
    }
    for (sequence, bytes) in &union {
        let record = format::open(bytes, &mac)?;
        let predecessor = get_str(&record, "predecessor")?;
        if let Some(previous) = sequence.checked_sub(1).and_then(|p| union.get(&p)) {
            ensure!(
                predecessor == hex_encode(digest(previous)),
                fail(
                    SurvivalFailure::Fork,
                    format!("generation {sequence} does not chain to its predecessor"),
                )
            );
        }
    }
    let (&sequence, record_bytes) = union.iter().next_back().ok_or_else(|| {
        fail(
            SurvivalFailure::Corrupt,
            "no survival generation was committed",
        )
    })?;
    let record = format::open(record_bytes, &mac)?;

    // The latest generation must verify completely on some survivor. A
    // corrupt latest generation is refused, never silently rolled back.
    let mut chosen = None;
    let mut errors = Vec::new();
    for source in &opened {
        if !source.commits.contains_key(&sequence) {
            continue;
        }
        match verify_generation(source.region.as_ref(), &record, &mac).await {
            Ok(plan) => {
                chosen = Some((Arc::clone(&source.region), plan));
                break;
            }
            Err(error) => errors.push(format!("{}: {error:#}", source.name())),
        }
    }
    let (region, plan) = chosen.ok_or_else(|| {
        fail(
            SurvivalFailure::Corrupt,
            format!(
                "generation {sequence} failed verification: {}",
                errors.join("; ")
            ),
        )
    })?;
    Ok(Selected {
        pool_id,
        sequence,
        record,
        record_bytes: record_bytes.clone(),
        region,
        plan,
        envelope,
        dek,
        name_mode,
        bootstrap,
        db_path,
        sources: opened.iter().map(Source::name).collect(),
        locks,
    })
}

impl Selected {
    fn compression(&self) -> Result<crate::config::CompressionConfig> {
        Ok(serde_json::from_value(
            self.bootstrap
                .get("compression")
                .cloned()
                .context("bootstrap has no compression")?,
        )?)
    }

    fn placement(&self) -> Result<crate::config::SegmentPlacement> {
        Ok(serde_json::from_value(
            self.bootstrap
                .get("segment_placement")
                .cloned()
                .unwrap_or(Value::String("first_fit".into())),
        )?)
    }

    fn export(&self) -> Result<CatalogExport> {
        bincode::deserialize(&self.plan.catalog)
            .map_err(|e| fail(SurvivalFailure::Corrupt, format!("catalog export: {e}")))
    }

    /// Read one planned object from the selected region and check its digest
    /// again, immediately before it is written to a target.
    async fn object(&self, relative: &Path, sha: &[u8; 32]) -> Result<Vec<u8>> {
        let bytes = read_object(
            self.region.as_ref(),
            &super::replica::key(&object_path(sha)),
        )
        .await?;
        ensure!(
            digest(&bytes) == *sha,
            fail(
                SurvivalFailure::Corrupt,
                format!("object {} changed", relative.display())
            )
        );
        Ok(bytes)
    }

    /// Open the recovered namespace read-only at the generation's checkpoint
    /// and report what the unavailable members took with them.
    async fn open_read_only(
        &self,
        pool: &PoolObjectStore,
        unavailable: Vec<String>,
        restored_bytes: u64,
        target: String,
    ) -> Result<(Arc<ZeroFS>, RecoveryReport)> {
        let compression = self.compression()?;
        let store: Arc<dyn ObjectStore> = Arc::new(pool.clone());
        let checkpoint = uuid::Uuid::parse_str(get_str(&self.record, "checkpoint_id")?)?;
        let transformer =
            crate::block_transformer::ZeroFsBlockTransformer::new_arc(&self.dek, compression);
        let reader = slatedb::DbReader::builder(self.db_path.clone(), Arc::clone(&store))
            .with_reader_mode(slatedb::DbReaderMode::Checkpoint(checkpoint))
            .with_block_transformer(transformer)
            .with_filter_policies(crate::fs::filter_policy::filter_policies())
            .with_segment_extractor(Arc::new(crate::segment_extractor::ZeroFsSegmentExtractor))
            .build()
            .await
            .context("open the recovered namespace checkpoint read-only")?;
        let codec = crate::frame_codec::FrameCodec::new(
            &self.dek,
            crate::segment::SEGMENT_INFO,
            compression,
        );
        let segment_store: Arc<dyn ObjectStore> =
            Arc::new(PrefixStore::new(store, self.db_path.clone()));
        let fs = ZeroFS::new_with_slatedb_and_lease(
            crate::db::SlateDbHandle::ReadOnly(ArcSwap::new(Arc::new(reader))),
            u64::MAX,
            None,
            false,
            false,
            None,
            None,
            Arc::new(crate::dedup::DedupCache::new()),
            None,
            crate::object_trace::ObjectTracer::new(),
            segment_store,
            codec,
            None,
            crate::config::StoreProfile::default(),
            self.name_mode,
        )
        .await?;
        let fs = Arc::new(fs);
        let lost_ranges = if unavailable.is_empty() {
            Vec::new()
        } else {
            lost_ranges(&fs, pool, &unavailable, self.name_mode).await?
        };
        let unreadable_files: BTreeSet<String> = lost_ranges
            .iter()
            .flat_map(|range| range.names.iter().cloned())
            .collect();
        let report = RecoveryReport {
            pool_id: self.pool_id.clone(),
            sequence: self.sequence,
            commit_digest: hex_encode(digest(&self.record_bytes)),
            mutation_cutoff: get_u64(&self.record, "mutation_cutoff")?,
            key_epoch: get_u64(&self.record, "key_epoch")?,
            created_at: get_u64(&self.record, "created_at")?,
            sources: self.sources.clone(),
            selected_from: self.region.describe(),
            unavailable_members: unavailable,
            lost_ranges,
            writes_after_generation_not_recovered: true,
            restored_bytes,
            target,
            unreadable_files: unreadable_files.into_iter().collect(),
        };
        Ok((fs, report))
    }
}

/// Recover from any surviving regions into owned scratch, through the
/// backend contract only.
pub(crate) async fn recover_regions(
    regions: Vec<Arc<dyn ReplicaBackend>>,
    password: &str,
    scratch_parent: &Path,
) -> Result<Recovered> {
    let selected = select(regions, password).await?;
    let db_path = selected.db_path.clone();

    // Scratch is an operator workspace on a healthy local filesystem, not a
    // replica.
    std::fs::create_dir_all(scratch_parent)
        .with_context(|| format!("create {}", scratch_parent.display()))?;
    let scratch = tempfile::Builder::new()
        .prefix("zerofs-recovery-")
        .tempdir_in(scratch_parent)?;
    // The original database path stays the logical prefix, so catalog keys
    // and segment locations resolve unchanged; only the root moves.
    let authority_root = scratch.path().join("authority-root");
    let authority = authority_root.join(db_path.as_ref());
    let key_file = authority.join(crate::key_management::WRAPPED_KEY_FILENAME);
    std::fs::create_dir_all(&authority)?;
    std::fs::write(&key_file, &selected.envelope)?;
    let authority_store: Arc<dyn ObjectStore> = Arc::new(
        object_store::local::LocalFileSystem::new_with_prefix(&authority_root)?,
    );

    // Preflight scratch space before copying.
    let scratch_bytes: u64 = selected
        .plan
        .objects
        .values()
        .map(|(len, _)| *len)
        .sum::<u64>()
        + selected.plan.catalog.len() as u64 * 2;
    let available = rustix::fs::statvfs(scratch.path())
        .map(|stat| stat.f_bavail.saturating_mul(stat.f_frsize))
        .unwrap_or(u64::MAX);
    ensure!(
        available > scratch_bytes,
        "recovery needs {scratch_bytes} bytes of scratch but only {available} are free"
    );
    for (relative, (_, sha)) in &selected.plan.objects {
        let destination = authority.join(relative);
        if let Some(parent) = destination.parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::write(&destination, selected.object(relative, sha).await?)?;
    }
    ensure!(
        std::fs::read(&key_file)? == selected.envelope,
        fail(
            SurvivalFailure::Corrupt,
            "generation key envelope differs from the key record"
        )
    );
    let export = selected.export()?;

    // Members: those whose storage is present are opened read-only; the rest
    // stay configured and fail every read.
    let identities = PoolObjectStore::member_records_from_export(&export)?;
    let mut members = Vec::new();
    let mut unavailable = Vec::new();
    for member in selected
        .bootstrap
        .get("members")
        .and_then(Value::as_array)
        .context("bootstrap names no members")?
    {
        let id = member
            .get("id")
            .and_then(Value::as_str)
            .context("member id")?
            .to_owned();
        let url = url::Url::parse(
            member
                .get("url")
                .and_then(Value::as_str)
                .context("member url")?,
        )?;
        let (identity, capacity) = identities
            .get(&id)
            .cloned()
            .with_context(|| format!("member {id} is missing from the recovered catalog"))?;
        let local = (url.scheme() == "file")
            .then(|| url.to_file_path().ok())
            .flatten();
        let store: Arc<dyn ObjectStore> = match &local {
            Some(dir) if dir.is_dir() => {
                Arc::new(object_store::local::LocalFileSystem::new_with_prefix(dir)?)
            }
            _ => {
                unavailable.push(id.clone());
                Arc::new(UnavailableMember(id.clone()))
            }
        };
        members.push(PoolMember {
            id,
            identity,
            store,
            capacity_bytes: capacity,
            local_path: local.filter(|dir| dir.is_dir()),
            min_free_bytes: 0,
            metadata_headroom_bytes: 0,
            health_device: None,
            free_space: None,
        });
    }
    let pool = PoolObjectStore::open_recovered_read_only(
        Arc::clone(&authority_store),
        db_path.clone(),
        scratch.path().join("catalog"),
        export,
        members,
        selected.placement()?,
    )
    .await?;
    let (fs, report) = selected
        .open_read_only(
            &pool,
            unavailable,
            scratch_bytes,
            scratch.path().display().to_string(),
        )
        .await?;
    Ok(Recovered {
        fs,
        pool,
        authority,
        scratch,
        commit_digest: digest(&selected.record_bytes),
        report,
        _locks: selected.locks,
    })
}

/// SlateDB manifest objects are `manifest/<id>.manifest`.
fn manifest_id(relative: &Path) -> Option<u64> {
    let mut parts = relative.components();
    let directory = parts.next()?.as_os_str().to_str()?;
    let name = parts.next()?.as_os_str().to_str()?;
    (directory == "manifest" && parts.next().is_none())
        .then(|| name.strip_suffix(".manifest")?.parse().ok())
        .flatten()
}

/// Whether a generation object belongs on a rebuilt, writable authority.
///
/// The generation lists every authority object that existed when it was
/// copied, which can include manifests newer than its checkpoint. A writable
/// database opens at its newest manifest, so only manifests up to the
/// checkpoint's are restored: the rebuilt namespace is then exactly the state
/// the catalog export was cut with. Compactor state describes work against
/// newer manifests and is rebuilt by the database, and the pool identity
/// marker is rewritten for the rebound catalog.
///
/// SlateDB's collector records the highest manifest and compactions version
/// it has retired in `gc/<name>.boundary`, and refuses any version at or below
/// that mark as already existing. The rebuilt database starts its compactor
/// state again at version 1 and its manifests again after the checkpoint's,
/// so a restored mark would refuse them. Neither mark is restored. They fence
/// writers that outlived a collection, and a blank device has none; the
/// collector writes new marks as it retires versions again.
fn belongs_on_rebuilt_authority(relative: &Path, checkpoint_manifest: u64) -> bool {
    if let Some(id) = manifest_id(relative) {
        return id <= checkpoint_manifest;
    }
    !(relative.starts_with("compactions")
        || relative == Path::new("gc/manifest.boundary")
        || relative == Path::new("gc/compactions.boundary")
        || relative == Path::new(".zerofs-pool/identity"))
}

/// Rebuild a lost authority NVMe onto a blank P3 from the raw pool members.
///
/// `settings` names the pool's raw members and the replacement P3, which
/// must be freshly provisioned for the same pool and hold nothing. Every
/// source is read-only. The newest generation that verifies on a member is
/// selected and every digest is checked before the first target write and
/// again per object as it is written. A member named in `allow_missing` may
/// be absent; any other member that cannot be opened fails the recovery
/// before the target is touched.
///
/// On success the P3 holds the namespace database at the generation's
/// checkpoint, the original key envelope, the placement catalog rebound to
/// this device and an empty writeback journal. The report lists every file
/// with a range on a missing member.
pub(crate) async fn rebuild_raw_p3(
    settings: &crate::config::Settings,
    password: &str,
    allow_missing: &[String],
) -> Result<RecoveryReport> {
    ensure!(
        settings.raw_p3_authority(),
        "rebuilding the authority needs a raw P3 [storage].url; use --replica and --scratch for a directory pool"
    );
    let pool_config = settings
        .storage
        .pool
        .as_ref()
        .context("[storage.pool] is not configured")?;
    for id in allow_missing {
        ensure!(
            pool_config.members.iter().any(|member| &member.id == id),
            "{id} is not a configured pool member"
        );
    }
    let offline = super::offline_member_replicas(settings).await?;
    for (id, reason) in &offline.unopened {
        ensure!(
            allow_missing.contains(id),
            fail(
                SurvivalFailure::Lost,
                format!(
                    "pool member {id} cannot be opened: {reason}. Recovery without it loses the file data it holds; pass --allow-missing-member {id} to proceed and list the affected files"
                ),
            )
        );
    }
    let regions = offline
        .replicas
        .iter()
        .filter(|replica| !offline.unopened.contains_key(&replica.id))
        .map(|replica| Arc::clone(&replica.backend))
        .collect();
    let selected = select(regions, password).await?;
    let export = selected.export()?;
    let checkpoint_manifest = get_u64(&selected.record, "manifest_id")?;
    ensure!(
        selected.db_path.as_ref() == "db",
        "generation was cut for database path {:?}, which a raw P3 does not use",
        selected.db_path.as_ref()
    );

    // Nothing has been written so far. The target is claimed only now, and
    // only a blank one is accepted.
    let binding =
        crate::storage_backend::raw::RawBinding::parse(&url::Url::parse(&settings.storage.url)?)?;
    let target_name = format!("raw P3 {}", binding.locator());
    let target = crate::storage_backend::raw::open_recovery_p3(binding).await?;
    let mut restored_bytes = 0_u64;
    for (relative, (len, sha)) in &selected.plan.objects {
        if !belongs_on_rebuilt_authority(relative, checkpoint_manifest) {
            continue;
        }
        let bytes = selected.object(relative, sha).await?;
        let location = super::authority_location(&target.path, &super::replica::key(relative));
        target
            .authority
            .put_opts(
                &location,
                bytes.into(),
                object_store::PutMode::Create.into(),
            )
            .await
            .with_context(|| format!("write {location} to the recovery target"))?;
        restored_bytes += len;
    }
    let envelope_path = target
        .path
        .clone()
        .join(crate::key_management::WRAPPED_KEY_FILENAME);
    let restored_envelope =
        object_store::ObjectStoreExt::get(target.authority.as_ref(), &envelope_path)
            .await
            .context("the generation carries no key envelope object")?
            .bytes()
            .await?;
    ensure!(
        restored_envelope.as_ref() == selected.envelope.as_slice(),
        fail(
            SurvivalFailure::Corrupt,
            "generation key envelope differs from the key record"
        )
    );

    let identities = PoolObjectStore::member_records_from_export(&export)?;
    let mut members = Vec::new();
    let mut unavailable = Vec::new();
    let mut opened: BTreeMap<String, PoolMember> = offline
        .opened
        .into_iter()
        .map(|member| (member.id.clone(), member))
        .collect();
    for member in &pool_config.members {
        let (identity, capacity) = identities.get(&member.id).cloned().with_context(|| {
            format!("member {} is missing from the recovered catalog", member.id)
        })?;
        match opened.remove(&member.id) {
            Some(present) => {
                ensure!(
                    present.identity == identity,
                    "member {} is bound to {}, but the recovered catalog expects {identity}",
                    member.id,
                    present.identity
                );
                members.push(present);
            }
            None => {
                unavailable.push(member.id.clone());
                members.push(PoolMember {
                    id: member.id.clone(),
                    identity,
                    store: Arc::new(UnavailableMember(member.id.clone())),
                    capacity_bytes: capacity,
                    local_path: None,
                    min_free_bytes: 0,
                    metadata_headroom_bytes: 0,
                    health_device: None,
                    free_space: None,
                });
            }
        }
    }
    restored_bytes += selected.plan.catalog.len() as u64;
    let (pool, marker) = PoolObjectStore::open_recovered_raw(
        Arc::clone(&target.authority),
        target.path.clone(),
        target.catalog,
        export,
        members,
        selected.placement()?,
    )
    .await?;
    // Prove the rebuilt authority opens at the generation before reporting.
    let (fs, report) = selected
        .open_read_only(&pool, unavailable, restored_bytes, target_name)
        .await?;
    fs.stop_new_mutation_admission();
    drop(fs);
    pool.settle().await;

    // The lost journal is gone with its device. The rebuilt one is empty and
    // bound to the original bucket and key, which are never minted here.
    if settings
        .writeback
        .as_ref()
        .is_some_and(|writeback| writeback.enabled)
    {
        crate::cli::recreate_raw_pool_journal(
            settings,
            &target.authority,
            &target.path,
            target.journal,
        )
        .await
        .context("create the empty writeback journal on the recovery target")?;
    }
    // The pool identity marker is written last. Ordinary startup refuses an
    // authority without it, so an interrupted rebuild can never be mistaken
    // for a finished one.
    target
        .authority
        .put_opts(
            &target.path.clone().join(".zerofs-pool").join("identity"),
            marker.into(),
            object_store::PutMode::Create.into(),
        )
        .await
        .context("write the pool identity marker to the recovery target")?;
    Ok(report)
}

struct Plan {
    objects: BTreeMap<PathBuf, (u64, [u8; 32])>,
    catalog: Vec<u8>,
}

async fn read_object(region: &dyn ReplicaBackend, key: &str) -> Result<Vec<u8>> {
    Ok(region
        .get(key)
        .await?
        .with_context(|| format!("{key} is missing on {}", region.describe()))?
        .to_vec())
}

/// Verify every dependency of one generation on one region.
async fn verify_generation(
    region: &dyn ReplicaBackend,
    record: &Map<String, Value>,
    mac: &MacKey,
) -> Result<Plan> {
    use super::replica::key;
    let dependencies_relative = format::safe_relative(get_str(record, "dependencies")?)?;
    let bytes = read_bounded(region, &key(&dependencies_relative)).await?;
    ensure!(
        digest(&bytes) == get_hash(record, "dependencies_sha256")?,
        "dependency list digest mismatch"
    );
    let dependencies = format::open(&bytes, mac)?;
    ensure!(
        get_u64(&dependencies, "sequence")? == get_u64(record, "sequence")?,
        "dependency list belongs to another generation"
    );
    let catalog_sha = get_hash(record, "catalog_sha256")?;
    ensure!(
        get_hash(&dependencies, "catalog_sha256")? == catalog_sha,
        "catalog digest mismatch"
    );
    let catalog = read_object(region, &key(&catalog_object_path(&catalog_sha))).await?;
    ensure!(digest(&catalog) == catalog_sha, "catalog object is corrupt");
    let retention = format::safe_relative(get_str(&dependencies, "retention")?)?;
    let retention_bytes = read_bounded(region, &key(&retention)).await?;
    ensure!(
        digest(&retention_bytes) == get_hash(&dependencies, "retention_sha256")?,
        "retention record digest mismatch"
    );
    let mut objects = BTreeMap::new();
    let listed = dependencies
        .get("objects")
        .and_then(Value::as_object)
        .context("dependency list has no objects")?;
    for (relative, entry) in listed {
        let relative = format::safe_relative(relative)?;
        let sha = format::parse_hash(
            entry
                .get("sha256")
                .and_then(Value::as_str)
                .context("object digest")?,
        )?;
        let len = entry
            .get("length")
            .and_then(Value::as_u64)
            .context("object length")?;
        let bytes = read_object(region, &key(&object_path(&sha))).await?;
        ensure!(
            bytes.len() as u64 == len && digest(&bytes) == sha,
            "object {} is corrupt",
            relative.display()
        );
        objects.insert(relative, (len, sha));
    }
    Ok(Plan { objects, catalog })
}

/// Every current sole-copy range on an unavailable member, by inode and all
/// of its names.
async fn lost_ranges(
    fs: &ZeroFS,
    pool: &PoolObjectStore,
    unavailable: &[String],
    name_mode: crate::fs::name_mode::NameMode,
) -> Result<Vec<LostRange>> {
    let auth = AuthContext::default();
    let mut names: BTreeMap<u64, BTreeSet<String>> = BTreeMap::new();
    let mut stack = vec![(0_u64, String::new())];
    let mut visited = BTreeSet::new();
    while let Some((directory, prefix)) = stack.pop() {
        if !visited.insert(directory) {
            continue;
        }
        let mut after = 0;
        loop {
            let listing = fs
                .readdir(&auth, directory, after, 256)
                .await
                .map_err(|e| anyhow!("list recovered directory {directory}: {e:?}"))?;
            for entry in &listing.entries {
                after = entry.cookie;
                if entry.name == b"." || entry.name == b".." {
                    continue;
                }
                let path = format!("{prefix}/{}", String::from_utf8_lossy(&entry.name));
                match fs.inode_store.get(entry.fileid).await {
                    Ok(Inode::Directory(_)) => stack.push((entry.fileid, path)),
                    Ok(Inode::File(_)) => {
                        names.entry(entry.fileid).or_default().insert(path);
                    }
                    _ => {}
                }
            }
            if listing.end || listing.entries.is_empty() {
                break;
            }
        }
    }
    let placements = pool.placements().await?;
    let codec = KeyCodec::with_name_mode(name_mode);
    let mut lost = Vec::new();
    for (inode, paths) in names {
        let Ok(Inode::File(file)) = fs.inode_store.get(inode).await else {
            continue;
        };
        let extent = crate::fs::EXTENT_SIZE as u64;
        let mut offset = 0;
        while offset < file.size {
            let index = offset / extent;
            let length = (file.size - offset).min(extent) as u32;
            if let Some(bytes) = fs.db.get_bytes(&codec.extent_key(inode, index)).await?
                && let Some(location) = FrameLoc::decode(&bytes)
            {
                let key = location.segid.object_key();
                if let Some(placement) = placements
                    .iter()
                    .find(|p| p.location.as_ref().ends_with(key.as_str()))
                    && unavailable.contains(&placement.member_id)
                {
                    lost.push(LostRange {
                        names: paths.iter().cloned().collect(),
                        inode,
                        offset,
                        length,
                        member: Some(placement.member_id.clone()),
                    });
                }
            }
            offset += extent;
        }
    }
    Ok(lost)
}

/// A member whose device is unavailable: every request fails as I/O.
#[derive(Debug)]
struct UnavailableMember(String);

impl std::fmt::Display for UnavailableMember {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "UnavailableMember({})", self.0)
    }
}

impl UnavailableMember {
    fn error(&self) -> object_store::Error {
        object_store::Error::Generic {
            store: "unavailable-pool-member",
            source: format!("pool member {} is unavailable after device loss", self.0).into(),
        }
    }
}

#[async_trait::async_trait]
impl ObjectStore for UnavailableMember {
    async fn put_opts(
        &self,
        _: &object_store::path::Path,
        _: object_store::PutPayload,
        _: object_store::PutOptions,
    ) -> object_store::Result<object_store::PutResult> {
        Err(self.error())
    }

    async fn put_multipart_opts(
        &self,
        _: &object_store::path::Path,
        _: object_store::PutMultipartOptions,
    ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
        Err(self.error())
    }

    async fn get_opts(
        &self,
        _: &object_store::path::Path,
        _: object_store::GetOptions,
    ) -> object_store::Result<object_store::GetResult> {
        Err(self.error())
    }

    fn delete_stream(
        &self,
        locations: futures::stream::BoxStream<
            'static,
            object_store::Result<object_store::path::Path>,
        >,
    ) -> futures::stream::BoxStream<'static, object_store::Result<object_store::path::Path>> {
        use futures::StreamExt;
        let error = self.0.clone();
        locations
            .map(move |_| {
                Err(object_store::Error::Generic {
                    store: "unavailable-pool-member",
                    source: format!("pool member {error} is unavailable").into(),
                })
            })
            .boxed()
    }

    fn list(
        &self,
        _: Option<&object_store::path::Path>,
    ) -> futures::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>> {
        use futures::StreamExt;
        futures::stream::once(std::future::ready(Err(self.error()))).boxed()
    }

    async fn list_with_delimiter(
        &self,
        _: Option<&object_store::path::Path>,
    ) -> object_store::Result<object_store::ListResult> {
        Err(self.error())
    }

    async fn copy_opts(
        &self,
        _: &object_store::path::Path,
        _: &object_store::path::Path,
        _: object_store::CopyOptions,
    ) -> object_store::Result<()> {
        Err(self.error())
    }
}
