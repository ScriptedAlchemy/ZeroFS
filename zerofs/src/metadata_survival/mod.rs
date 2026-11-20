//! Metadata survival: redundant recovery generations of the namespace,
//! placement catalog and key envelope on every metadata replica.
//!
//! The live SlateDB authority, the placement catalog and `zerofs.key` stay
//! where they are. A survival commit closes mutation admission only long
//! enough to drain admitted work, reach a backend cutoff, pin a named finite
//! SlateDB checkpoint and export the catalog in one snapshot. It reopens
//! admission and then copies every authority object by content hash, the
//! logical catalog export, the exact key envelope and an authenticated commit
//! record to every replica in stable order. A receipt is returned only after
//! every replica holds the durable record. Commits run on an interval and
//! once at clean shutdown.
//!
//! On a raw P3 authority the replicas are the pool's raw members, each
//! holding a region in its own object log (`member_region.rs`). On a
//! directory authority they are configured directories (`replica.rs`).
//!
//! Recovery (`recover.rs`) runs only on request. It reads surviving replicas
//! without modifying them, then either rebuilds a blank P3 or opens the
//! selected generation read-only from owned scratch.
//!
//! See `docs/metadata-survival-design.md` for the accepted mechanism and
//! `docs/metadata-survival.md` for what this implementation delivers.

// Status, fault-injection and operator entry points are reached from the
// CLI binary, the server's survival owner and the acceptance contracts; the
// library target alone does not call every one of them.
#![cfg_attr(not(test), allow(dead_code))]

pub(crate) mod admission;
pub(crate) mod backend;
pub(crate) mod format;
pub(crate) mod member_region;
pub(crate) mod recover;
pub(crate) mod replica;

use crate::config::{Settings, StoragePoolConfig};
use crate::fs::ZeroFS;
use crate::fs::errors::FsError;
use crate::pool_object_store::{CatalogExport, PoolObjectStore};
use anyhow::{Context, Result, anyhow, bail, ensure};
use format::{MacKey, digest, get_hash, get_str, get_u64, hex_encode, seal};
use replica::{InjectedFault, LocalDirectoryBackend, Replica};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::path::{Path as FsPath, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// The membership epoch of a freshly activated replica set.
pub(crate) const INITIAL_EPOCH: u64 = 1;
/// The key epoch of the envelope copied at activation. Password changes are
/// refused while survival is enabled, so it never advances in this version.
pub(crate) const INITIAL_KEY_EPOCH: u64 = 1;
/// Selectable generations kept after a successful commit.
const RETAINED_GENERATIONS: usize = 2;
const CHECKPOINT_PREFIX: &str = "zerofs-survival/";

fn default_recovery_mode() -> String {
    "normal".to_owned()
}
const fn default_commit_interval() -> u64 {
    60
}
const fn default_replica_timeout() -> u64 {
    90
}
const fn default_commit_deadline() -> u64 {
    180
}
const fn default_checkpoint_lifetime() -> u64 {
    600
}
const fn default_predrain() -> u64 {
    120
}
const fn default_residual() -> u64 {
    16 * 1024 * 1024
}
const fn default_final_pause() -> u64 {
    1000
}

/// Metadata headroom kept on each raw pool member unless configured: 4 GiB.
pub(crate) const DEFAULT_HEADROOM_BYTES: u64 = 4 << 30;

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct MetadataSurvivalConfig {
    pub(crate) format_version: u64,
    /// Directory authority only. On a raw P3 authority the replica set is
    /// every pool member, and these three fields must be absent.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub(crate) authority_replica_id: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub(crate) authority_device_id: String,
    #[serde(default = "default_recovery_mode")]
    pub(crate) recovery_mode: String,
    #[serde(default)]
    pub(crate) reserve_free_bytes: u64,
    /// Room on each raw pool member that only generations may use. Defaults
    /// to [`DEFAULT_HEADROOM_BYTES`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) headroom_bytes: Option<u64>,
    #[serde(default = "default_commit_interval")]
    pub(crate) commit_interval_seconds: u64,
    #[serde(default = "default_replica_timeout")]
    pub(crate) replica_timeout_seconds: u64,
    #[serde(default = "default_commit_deadline")]
    pub(crate) commit_deadline_seconds: u64,
    #[serde(default = "default_checkpoint_lifetime")]
    pub(crate) checkpoint_lifetime_seconds: u64,
    #[serde(default = "default_predrain")]
    pub(crate) predrain_max_seconds: u64,
    #[serde(default = "default_residual")]
    pub(crate) final_residual_max_bytes: u64,
    #[serde(default = "default_final_pause")]
    pub(crate) final_pause_max_milliseconds: u64,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub(crate) replicas: Vec<MetadataReplicaConfig>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct MetadataReplicaConfig {
    pub(crate) id: String,
    pub(crate) device_id: String,
    #[serde(deserialize_with = "crate::config::deserialize_expandable_path")]
    pub(crate) dir: PathBuf,
}

fn plain_absolute(path: &FsPath) -> bool {
    path.is_absolute()
        && path
            .components()
            .all(|part| !matches!(part, std::path::Component::ParentDir))
}

fn nested(left: &FsPath, right: &FsPath) -> bool {
    left.starts_with(right) || right.starts_with(left)
}

impl MetadataSurvivalConfig {
    pub(crate) fn member_headroom_bytes(&self) -> u64 {
        self.headroom_bytes.unwrap_or(DEFAULT_HEADROOM_BYTES)
    }

    pub(crate) fn validate(&self, settings: &Settings, pool: &StoragePoolConfig) -> Result<()> {
        ensure!(
            self.format_version == format::FORMAT_VERSION,
            "format_version must be {}",
            format::FORMAT_VERSION
        );
        ensure!(
            self.recovery_mode == "normal",
            "recovery_mode must be \"normal\"; promotion is an offline operation"
        );
        ensure!(
            self.replica_timeout_seconds > 0
                && self.replica_timeout_seconds <= self.commit_deadline_seconds,
            "replica_timeout_seconds must be positive and at most commit_deadline_seconds"
        );
        ensure!(
            self.checkpoint_lifetime_seconds > self.commit_deadline_seconds + 5,
            "checkpoint_lifetime_seconds must exceed commit_deadline_seconds plus the five-second settlement margin"
        );
        ensure!(
            self.final_pause_max_milliseconds > 0,
            "final_pause_max_milliseconds must be positive"
        );
        if settings.raw_p3_authority() {
            // Pool validation already requires every member of a raw P3
            // authority to be a raw device.
            ensure!(
                self.replicas.is_empty()
                    && self.authority_replica_id.is_empty()
                    && self.authority_device_id.is_empty(),
                "on a raw P3 authority every pool member holds the recovery generations; remove authority_replica_id, authority_device_id and [[replicas]]"
            );
            let headroom = self.member_headroom_bytes();
            for member in &pool.members {
                let floor = member.reserve_free_bytes()?.checked_add(headroom);
                let capacity = member.capacity_bytes()?;
                ensure!(
                    floor.is_some_and(|floor| floor < capacity),
                    "headroom_bytes ({headroom}) and the reserve of pool member {} must fit within its capacity",
                    member.id
                );
            }
            return Ok(());
        }
        ensure!(
            self.headroom_bytes.is_none(),
            "headroom_bytes applies only on a raw P3 authority, where pool members hold the generations"
        );
        ensure!(
            !self.replicas.is_empty(),
            "at least one metadata replica on an independent device is required besides the authority; survival needs two devices"
        );
        ensure!(
            !self.authority_replica_id.trim().is_empty()
                && !self.authority_device_id.trim().is_empty(),
            "authority_replica_id and authority_device_id are required"
        );
        let mut ids = BTreeSet::from([self.authority_replica_id.as_str()]);
        let mut devices = BTreeSet::from([self.authority_device_id.as_str()]);
        let authority = url::Url::parse(&settings.storage.url)?
            .to_file_path()
            .map_err(|()| anyhow!("[storage].url must be a local file:// directory"))?;
        let mut roots: Vec<(String, PathBuf)> = vec![
            ("the authority".into(), authority),
            ("catalog_dir".into(), pool.catalog_dir.clone()),
        ];
        for member in &pool.members {
            if let Ok(url) = url::Url::parse(&member.url)
                && url.scheme() == "file"
                && let Ok(path) = url.to_file_path()
            {
                roots.push((format!("member {}", member.id), path));
            }
        }
        for replica in &self.replicas {
            ensure!(
                !replica.id.trim().is_empty() && !replica.device_id.trim().is_empty(),
                "every replica needs an id and a device_id"
            );
            ensure!(
                ids.insert(replica.id.as_str()),
                "duplicate metadata replica id {}",
                replica.id
            );
            ensure!(
                devices.insert(replica.device_id.as_str()),
                "metadata replica {} shares device_id {} with another replica; replicas need independent devices",
                replica.id,
                replica.device_id
            );
            ensure!(
                plain_absolute(&replica.dir),
                "metadata replica {} dir must be absolute without parent components",
                replica.id
            );
            for (name, root) in &roots {
                ensure!(
                    !nested(&replica.dir, root),
                    "metadata replica {} dir overlaps {name}; use a disjoint sibling directory",
                    replica.id
                );
            }
            roots.push((format!("replica {}", replica.id), replica.dir.clone()));
        }
        Ok(())
    }
}

/// Why survival did not acknowledge, or why recovery refused.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub(crate) enum SurvivalFailure {
    #[error("replica deadline expired before record publication")]
    Timeout,
    #[error("metadata replica is full")]
    Full,
    #[error("metadata replica is stale")]
    Stale,
    #[error("metadata replica I/O failed")]
    Io,
    #[error("durable survival configuration is missing or mismatched")]
    Configuration,
    #[error("survival was disabled by a terminal record")]
    Disabled,
    #[error("metadata authentication or dependency verification failed")]
    Corrupt,
    #[error("authenticated commit chains conflict")]
    Fork,
    #[error("metadata replica device is lost")]
    Lost,
    #[error("a metadata replica is locked by another writer")]
    Locked,
    #[error("an authority object changed during the copy; the next attempt retries")]
    Changed,
}

impl SurvivalFailure {
    /// The errno a latched mutation receives after this failure.
    fn latch(self) -> Option<FsError> {
        match self {
            Self::Timeout | Self::Changed => None,
            Self::Lost => Some(FsError::ReadOnlyFilesystem),
            _ => Some(FsError::IoError),
        }
    }
}

fn failure(kind: SurvivalFailure, detail: impl std::fmt::Display) -> anyhow::Error {
    anyhow::Error::new(kind).context(detail.to_string())
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub(crate) struct SurvivalReceipt {
    /// Relative to each replica's `.zerofs-survival/v1`.
    pub(crate) relative_record: PathBuf,
    #[serde(serialize_with = "serialize_hash")]
    pub(crate) digest: [u8; 32],
    pub(crate) sequence: u64,
    pub(crate) mutation_cutoff: u64,
    pub(crate) envelope_epoch: u64,
    pub(crate) completed_at: u64,
}

fn serialize_hash<S: serde::Serializer>(hash: &[u8; 32], s: S) -> Result<S::Ok, S::Error> {
    s.serialize_str(&hex_encode(hash))
}

#[derive(Clone, Debug, Serialize)]
pub struct ReplicaStatus {
    pub(crate) id: String,
    pub(crate) device_id: String,
    /// The highest survival generation durably recorded on this replica.
    pub(crate) last_sequence: Option<u64>,
    pub(crate) last_digest: Option<String>,
    /// Unix seconds at which that generation was cut.
    pub(crate) generation_created_at: Option<u64>,
    /// Seconds since that generation was cut, as of the status read.
    pub(crate) generation_age_seconds: Option<u64>,
    /// Bytes the retained generations occupy on this replica.
    pub(crate) region_bytes: Option<u64>,
    pub(crate) error: Option<String>,
}

#[derive(Clone, Debug, Serialize)]
pub struct SurvivalStatus {
    pub(crate) enabled: bool,
    /// Configured seconds between generations; zero for an offline read.
    pub(crate) commit_interval_seconds: u64,
    pub(crate) last_sequence: Option<u64>,
    pub(crate) last_digest: Option<String>,
    pub(crate) completed_at: Option<u64>,
    pub(crate) lag_seconds: u64,
    #[serde(skip)]
    pub(crate) lag: Duration,
    pub(crate) failed_replicas: BTreeMap<String, String>,
    pub(crate) admission: String,
    pub(crate) admission_open: bool,
    pub(crate) owned_checkpoints: usize,
    pub(crate) retained_generations: Vec<u64>,
    pub(crate) replicas: Vec<ReplicaStatus>,
    pub(crate) attempts: u64,
    pub(crate) last_error: Option<String>,
    /// Wall-clock milliseconds the last successful commit took, end to end.
    pub(crate) last_commit_millis: Option<u64>,
    /// Milliseconds the last commit attempt kept mutation admission closed.
    pub(crate) last_pause_millis: Option<u64>,
}

impl SurvivalStatus {
    /// The same status with every age recomputed for a read at `now`.
    pub(crate) fn at(mut self, now: u64) -> Self {
        if let Some(completed) = self.completed_at {
            self.lag_seconds = now.saturating_sub(completed);
            self.lag = Duration::from_secs(self.lag_seconds);
        }
        for replica in &mut self.replicas {
            replica.generation_age_seconds = replica
                .generation_created_at
                .map(|created| now.saturating_sub(created));
        }
        self
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum CommitPoint {
    AdmissionClosed,
    AdmittedWorkDrained,
    BackendCutoff,
    CheckpointIntentSynced,
    CheckpointPublished,
    CheckpointSynced,
    CatalogPublished,
    CatalogSynced,
    DependencyPublished,
    DependencyFileSynced,
    DependencyDirectorySynced,
    RetentionInstalled,
    CommitPublished,
    CommitFileSynced,
    CommitDirectorySynced,
    RetirementLogged,
    CheckpointReleased,
    DependencyDeleted,
    DebtCompletionCommitted,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Side {
    Before,
    After,
}

/// Observes every commit boundary. Production uses the no-op observer; the
/// acceptance contract uses it to cut the real commit at each boundary.
#[async_trait::async_trait]
pub(crate) trait CommitObserver: Send {
    async fn reach(&mut self, point: CommitPoint, side: Side, replica: &str, object: &FsPath);
    /// The complete candidate record exists in memory, before any fanout.
    fn candidate(&mut self, _receipt: &SurvivalReceipt) {}
    /// Every replica holds the record; the receipt is being returned.
    fn returned(&mut self, _receipt: &SurvivalReceipt) {}
}

struct NoObserver;

#[async_trait::async_trait]
impl CommitObserver for NoObserver {
    async fn reach(&mut self, _: CommitPoint, _: Side, _: &str, _: &FsPath) {}
}

/// Live handles the commit owner needs from the writer.
pub(crate) struct WriterHandles {
    pub(crate) fs: Arc<ZeroFS>,
    pub(crate) pool: PoolObjectStore,
    /// The object store the namespace database was opened with.
    pub(crate) db_store: Arc<dyn object_store::ObjectStore>,
    pub(crate) db_path: object_store::path::Path,
    pub(crate) dek: [u8; 32],
    /// The persistent writeback journal in front of the backend, if any. Its
    /// undrained bytes decide whether a cut may close admission.
    pub(crate) writeback: Option<crate::writeback::store::WritebackObjectStore>,
}

#[derive(Default)]
struct CommitState {
    last: Option<(u64, [u8; 32], u64)>,
    per_replica: BTreeMap<String, Option<(u64, [u8; 32], u64)>>,
    failed: BTreeMap<String, String>,
    retained: Vec<u64>,
    hashes: HashMap<String, (u64, String, [u8; 32])>,
    attempts: u64,
    last_error: Option<String>,
    opened_at: u64,
    region_bytes: BTreeMap<String, u64>,
    last_commit_millis: Option<u64>,
    last_pause_millis: Option<u64>,
}

pub(crate) struct SurvivalManager {
    config: MetadataSurvivalConfig,
    pool_id: String,
    membership_digest: String,
    epoch: u64,
    replicas: Vec<Replica>,
    locks: Mutex<Vec<Box<dyn backend::Fence>>>,
    fs: Arc<ZeroFS>,
    pool: PoolObjectStore,
    db_store: Arc<dyn object_store::ObjectStore>,
    db_path: object_store::path::Path,
    writeback: Option<crate::writeback::store::WritebackObjectStore>,
    /// The live authority object store and database prefix. Authority
    /// objects are enumerated and read only through the object-store API.
    authority: Arc<dyn object_store::ObjectStore>,
    authority_prefix: object_store::path::Path,
    mac: MacKey,
    key_sha: [u8; 32],
    state: Mutex<CommitState>,
    commit_lock: tokio::sync::Mutex<()>,
}

impl std::fmt::Debug for SurvivalManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SurvivalManager")
            .field("pool_id", &self.pool_id)
            .finish_non_exhaustive()
    }
}

pub(crate) fn now_seconds() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

fn object(value: Value) -> Map<String, Value> {
    match value {
        Value::Object(map) => map,
        _ => unreachable!("record literals are objects"),
    }
}

pub(crate) fn authority_dir(settings: &Settings) -> Result<PathBuf> {
    url::Url::parse(&settings.storage.url)?
        .to_file_path()
        .map_err(|()| anyhow!("[storage].url must be a local file:// directory"))
}

/// A raw pool member as a replica: its region lives in the member's log.
fn member_replica(member: crate::pool_object_store::SurvivalMember) -> Replica {
    let region = if member.available {
        member_region::MemberRegion::new(&member.id, &member.identity, member.store, member.pool)
    } else {
        member_region::MemberRegion::absent(&member.id, &member.identity, member.store)
    };
    Replica {
        id: member.id,
        device_id: member.identity,
        backend: Arc::new(region),
        fault: Arc::default(),
    }
}

/// Build the replica set in stable replica-ID order.
///
/// On a raw P3 authority the set is every pool member, each holding its
/// region in its own object log ([`member_region::MemberRegion`]). On a
/// directory authority every configured replica uses the existing local
/// backend ([`LocalDirectoryBackend`]), and the authority replica's region
/// lives inside the authority directory.
pub(crate) fn replica_set(
    settings: &Settings,
    config: &MetadataSurvivalConfig,
    pool: Option<&PoolObjectStore>,
) -> Result<Vec<Replica>> {
    if settings.raw_p3_authority() {
        let pool = pool.context("raw member regions need the opened pool")?;
        let mut replicas: Vec<Replica> = pool
            .survival_members()
            .into_iter()
            .map(member_replica)
            .collect();
        ensure!(
            !replicas.is_empty(),
            "metadata survival needs at least one raw pool member"
        );
        replicas.sort_by(|a, b| a.id.cmp(&b.id));
        return Ok(replicas);
    }
    let local = |id: &str, device_id: &str, dir: PathBuf| Replica {
        id: id.to_owned(),
        device_id: device_id.to_owned(),
        backend: Arc::new(LocalDirectoryBackend::new(dir)),
        fault: Arc::default(),
    };
    let mut replicas = vec![local(
        &config.authority_replica_id,
        &config.authority_device_id,
        authority_dir(settings)?,
    )];
    for replica in &config.replicas {
        replicas.push(local(&replica.id, &replica.device_id, replica.dir.clone()));
    }
    replicas.sort_by(|a, b| a.id.cmp(&b.id));
    Ok(replicas)
}

/// Where durable survival enablement is recorded, if it is. Removing the
/// configuration table never disables survival, so a writer and a password
/// change both consult this.
pub(crate) async fn durable_enablement(
    settings: &Settings,
    pool: Option<&PoolObjectStore>,
) -> Result<Option<String>> {
    if settings.raw_p3_authority() {
        let Some(pool) = pool else {
            return Ok(None);
        };
        for member in pool.survival_members() {
            if !member.available {
                continue;
            }
            let replica = member_replica(member);
            if replica
                .len(FsPath::new("identity.json"))
                .await
                .map_err(|kind| failure(kind, format!("read {}", replica.describe())))?
                .is_some()
            {
                return Ok(Some(replica.describe()));
            }
        }
        return Ok(None);
    }
    let marker = authority_dir(settings)?
        .join(format::ROOT)
        .join("identity.json");
    Ok(marker.is_file().then(|| marker.display().to_string()))
}

pub(crate) fn membership_digest(pool_id: &str, replicas: &[Replica]) -> Result<String> {
    let ids: Vec<&str> = replicas.iter().map(|r| r.id.as_str()).collect();
    Ok(hex_encode(digest(&format::canonical(
        &json!({"pool_id": pool_id, "replicas": ids}),
    )?)))
}

pub(crate) fn commit_path(epoch: u64, sequence: u64) -> PathBuf {
    PathBuf::from(format!("commits/{epoch}/{sequence}.json"))
}

fn intent_path(epoch: u64, sequence: u64) -> PathBuf {
    PathBuf::from(format!("intents/{epoch}/{sequence}.json"))
}

pub(crate) fn dependencies_path(sequence: u64) -> PathBuf {
    PathBuf::from(format!("dependencies/{sequence}.json"))
}

pub(crate) fn retention_path(sequence: u64) -> PathBuf {
    PathBuf::from(format!("retention/{sequence}.json"))
}

pub(crate) fn key_path(epoch: u64) -> PathBuf {
    PathBuf::from(format!("keys/{epoch}.json"))
}

pub(crate) fn object_path(sha: &[u8; 32]) -> PathBuf {
    PathBuf::from(format!("objects/{}", hex_encode(sha)))
}

pub(crate) fn catalog_object_path(sha: &[u8; 32]) -> PathBuf {
    PathBuf::from(format!("catalog/objects/{}", hex_encode(sha)))
}

/// The authority location of an object named relative to the database
/// prefix. Each `/`-separated component is one path part.
pub(crate) fn authority_location(
    prefix: &object_store::path::Path,
    relative: &str,
) -> object_store::path::Path {
    relative
        .split('/')
        .fold(prefix.clone(), |path, part| path.join(part))
}

/// Every commit record visible on one replica region, by sequence.
pub(crate) async fn replica_commits(
    region: &dyn backend::ReplicaBackend,
    epoch: u64,
) -> Result<BTreeMap<u64, Vec<u8>>> {
    let prefix = format!("commits/{epoch}");
    let mut commits = BTreeMap::new();
    for name in region.list(&prefix).await? {
        let Some(sequence) = name
            .strip_suffix(".json")
            .and_then(|stem| stem.parse::<u64>().ok())
        else {
            continue;
        };
        if let Some(bytes) = region.get(&format!("{prefix}/{name}")).await? {
            commits.insert(sequence, bytes.to_vec());
        }
    }
    Ok(commits)
}

/// Authority objects to copy: every visible object under the authority
/// database prefix except the authority's own survival region, keyed by its
/// path relative to the prefix. Hashes are cached by object version
/// (size plus e_tag or modification time) and recomputed on any change.
async fn enumerate_authority(
    store: &dyn object_store::ObjectStore,
    prefix: &object_store::path::Path,
    cache: &mut HashMap<String, (u64, String, [u8; 32])>,
) -> Result<BTreeMap<String, (u64, [u8; 32])>> {
    use futures::TryStreamExt;
    use object_store::ObjectStoreExt;
    let listed: Vec<object_store::ObjectMeta> = store.list(Some(prefix)).try_collect().await?;
    let mut objects = BTreeMap::new();
    let mut seen = BTreeSet::new();
    for meta in listed {
        let Some(relative) = meta.location.prefix_match(prefix).map(|parts| {
            parts
                .map(|part| part.as_ref().to_owned())
                .collect::<Vec<_>>()
        }) else {
            continue;
        };
        if relative.is_empty() || relative[0] == ".zerofs-survival" {
            continue;
        }
        let relative = relative.join("/");
        let version = format!(
            "{}:{}",
            meta.e_tag.as_deref().unwrap_or(""),
            meta.last_modified.timestamp_nanos_opt().unwrap_or_default()
        );
        let sha = match cache.get(&relative) {
            Some((len, cached_version, sha)) if *len == meta.size && *cached_version == version => {
                *sha
            }
            _ => match store.get(&meta.location).await {
                Ok(result) => {
                    let bytes = result.bytes().await?;
                    let sha = digest(&bytes);
                    cache.insert(relative.clone(), (bytes.len() as u64, version, sha));
                    sha
                }
                // Removed by GC after listing: unreferenced, because the
                // owned checkpoint pins everything the generation reads.
                Err(object_store::Error::NotFound { .. }) => continue,
                Err(error) => return Err(error.into()),
            },
        };
        seen.insert(relative.clone());
        objects.insert(relative, (meta.size, sha));
    }
    cache.retain(|path, _| seen.contains(path));
    Ok(objects)
}

/// Reopens admission when a commit leaves the closed phase by any path,
/// including cancellation of the commit future.
struct ReopenOnDrop<'a>(Option<&'a admission::SurvivalAdmission>);

impl ReopenOnDrop<'_> {
    fn reopen(&mut self) {
        if let Some(gate) = self.0.take() {
            gate.reopen();
        }
    }
}

impl Drop for ReopenOnDrop<'_> {
    fn drop(&mut self) {
        self.reopen();
    }
}

struct Candidate {
    intent: Vec<u8>,
    sequence: u64,
    cutoff: u64,
    incarnation: String,
    checkpoint_id: uuid::Uuid,
    checkpoint_name: String,
    manifest_id: u64,
    catalog: Vec<u8>,
    catalog_sha: [u8; 32],
    retention: Vec<u8>,
    objects: BTreeMap<String, (u64, [u8; 32])>,
}

impl SurvivalManager {
    /// Lock every replica and validate or activate the durable replica set.
    pub(crate) async fn open(settings: &Settings, writer: WriterHandles) -> Result<Arc<Self>> {
        let pool_config = settings
            .storage
            .pool
            .as_ref()
            .context("metadata survival requires [storage.pool]")?;
        let config = pool_config
            .metadata_survival
            .clone()
            .context("[storage.pool.metadata_survival] is not configured")?;
        config.validate(settings, pool_config)?;
        let replicas = replica_set(settings, &config, Some(&writer.pool))?;
        let (authority, authority_prefix) = writer.pool.survival_authority();
        let pool_id = writer.pool.status().await?.pool_id;
        let digest_text = membership_digest(&pool_id, &replicas)?;
        let mac = MacKey::derive(&writer.dek, &pool_id);
        let envelope_path = authority_prefix
            .clone()
            .join(crate::key_management::WRAPPED_KEY_FILENAME);
        let envelope = object_store::ObjectStoreExt::get(authority.as_ref(), &envelope_path)
            .await
            .with_context(|| format!("read key envelope {envelope_path}"))?
            .bytes()
            .await?
            .to_vec();
        let key_sha = digest(&envelope);

        for replica in &replicas {
            if let Err(kind) = replica.present().await {
                return Err(failure(
                    kind,
                    format!(
                        "metadata replica {} at {} is missing or foreign; offline promotion or verified replacement is required",
                        replica.id,
                        replica.describe()
                    ),
                ));
            }
        }
        // Activation is all or nothing: either no replica has survival state
        // (fresh enablement) or every one must.
        let mut present = Vec::with_capacity(replicas.len());
        for replica in &replicas {
            present.push(replica.exists(FsPath::new("identity.json")).await);
        }
        let fresh = present.iter().all(|present| !present);
        if !fresh && present.iter().any(|present| !present) {
            let missing: Vec<_> = replicas
                .iter()
                .zip(&present)
                .filter(|(_, present)| !**present)
                .map(|(replica, _)| replica.id.clone())
                .collect();
            return Err(failure(
                SurvivalFailure::Stale,
                format!(
                    "metadata replicas {missing:?} have no survival identity while others do; offline verified catch-up is required"
                ),
            ));
        }
        let mut locks = Vec::with_capacity(replicas.len());
        for replica in &replicas {
            // A failed acquisition drops (releases) every lock already taken.
            let lock = replica.lock_exclusive(fresh).await.map_err(|kind| {
                failure(kind, format!("cannot lock metadata replica {}", replica.id))
            })?;
            locks.push(lock);
        }

        let manager = Arc::new(Self {
            config,
            pool_id,
            membership_digest: digest_text,
            epoch: INITIAL_EPOCH,
            replicas,
            locks: Mutex::new(locks),
            fs: writer.fs,
            pool: writer.pool,
            db_store: writer.db_store,
            db_path: writer.db_path,
            writeback: writer.writeback,
            authority,
            authority_prefix,
            mac,
            key_sha,
            state: Mutex::new(CommitState {
                opened_at: now_seconds(),
                ..Default::default()
            }),
            commit_lock: tokio::sync::Mutex::new(()),
        });
        manager
            .fs
            .survival_admission
            .set_final_pause(Duration::from_millis(
                manager.config.final_pause_max_milliseconds,
            ));
        if fresh {
            manager.activate(settings, &envelope).await?;
        } else {
            manager.validate_identities(&envelope).await?;
            manager.reconcile().await?;
        }
        manager.reconcile_checkpoints().await;
        // Every retained generation is pinned again from authenticated records.
        manager.pool.survival_retention_reconciled();
        manager.publish_status().await;
        Ok(manager)
    }

    fn identity_record(&self, settings: &Settings, replica: &Replica) -> Result<Vec<u8>> {
        let pool = settings.storage.pool.as_ref().context("pool settings")?;
        let members: Vec<Value> = pool
            .members
            .iter()
            .map(|member| json!({"id": member.id, "url": member.url}))
            .collect();
        let replicas: Vec<Value> = self
            .replicas
            .iter()
            .map(|r| json!({"id": r.id, "device_id": r.device_id}))
            .collect();
        seal(
            object(json!({
                "kind": "identity",
                "format_version": format::FORMAT_VERSION,
                "pool_id": self.pool_id,
                "replica_id": replica.id,
                "device_id": replica.device_id,
                "membership_digest": self.membership_digest,
                "membership_epoch": self.epoch,
                "authority_replica_id": self.config.authority_replica_id,
                "replicas": replicas,
                "enabled": true,
                "key_epoch": INITIAL_KEY_EPOCH,
                "key_sha256": hex_encode(self.key_sha),
                "bootstrap": {
                    "authority_url": settings.storage.url,
                    "db_path": self.db_path.to_string(),
                    "catalog_dir": pool.catalog_dir.to_string_lossy(),
                    "segment_placement": serde_json::to_value(pool.segment_placement)?,
                    "compression": serde_json::to_value(settings.compression())?,
                    "members": members,
                },
            })),
            &self.mac,
        )
    }

    fn key_record(&self, envelope: &[u8]) -> Result<Vec<u8>> {
        let (version, name_mode) = crate::key_management::describe_envelope(envelope)?;
        seal(
            object(json!({
                "kind": "key",
                "pool_id": self.pool_id,
                "key_epoch": INITIAL_KEY_EPOCH,
                "envelope_hex": hex_encode(envelope),
                "envelope_sha256": hex_encode(digest(envelope)),
                "envelope_version": version,
                "name_mode": name_mode,
                "kdf": "argon2id",
                "kdf_memory_kib": 65536,
                "kdf_time_cost": 3,
                "kdf_parallelism": 4,
            })),
            &self.mac,
        )
    }

    async fn activate(&self, settings: &Settings, envelope: &[u8]) -> Result<()> {
        let key = Arc::new(self.key_record(envelope)?);
        for replica in &self.replicas {
            let identity = Arc::new(self.identity_record(settings, replica)?);
            self.publish(replica, &key_path(INITIAL_KEY_EPOCH), key.clone())
                .await
                .map_err(|kind| failure(kind, format!("activate replica {}", replica.id)))?;
            self.publish(replica, FsPath::new("identity.json"), identity)
                .await
                .map_err(|kind| failure(kind, format!("activate replica {}", replica.id)))?;
        }
        let mut state = self.lock_state();
        for replica in &self.replicas {
            state.per_replica.insert(replica.id.clone(), None);
        }
        Ok(())
    }

    async fn validate_identities(&self, envelope: &[u8]) -> Result<()> {
        for replica in &self.replicas {
            let bytes = replica
                .read(FsPath::new("identity.json"))
                .await
                .map_err(|kind| failure(kind, format!("read {} identity", replica.id)))?
                .ok_or_else(|| {
                    failure(
                        SurvivalFailure::Stale,
                        format!("{} has no identity", replica.id),
                    )
                })?;
            let identity = format::open(&bytes, &self.mac).map_err(|error| {
                failure(
                    SurvivalFailure::Configuration,
                    format!(
                        "replica {} identity does not authenticate: {error:#}",
                        replica.id
                    ),
                )
            })?;
            let matches = get_str(&identity, "pool_id")? == self.pool_id
                && get_str(&identity, "membership_digest")? == self.membership_digest
                && get_str(&identity, "replica_id")? == replica.id
                && get_str(&identity, "device_id")? == replica.device_id
                && get_u64(&identity, "membership_epoch")? == self.epoch;
            if !matches {
                return Err(failure(
                    SurvivalFailure::Configuration,
                    format!(
                        "replica {} belongs to a different pool, membership or device binding",
                        replica.id
                    ),
                ));
            }
            if get_hash(&identity, "key_sha256")? != digest(envelope) {
                return Err(failure(
                    SurvivalFailure::Configuration,
                    "the live key envelope differs from the envelope survival recorded; password changes are refused while survival is enabled",
                ));
            }
        }
        Ok(())
    }

    /// Complete a partial record fanout and establish the last acknowledged
    /// generation. Never builds a different record at an existing sequence.
    async fn reconcile(&self) -> Result<()> {
        let mut per_replica = Vec::new();
        for replica in &self.replicas {
            per_replica.push(replica_commits(replica.backend.as_ref(), self.epoch).await?);
        }
        let mut union: BTreeMap<u64, Vec<u8>> = BTreeMap::new();
        for commits in &per_replica {
            for (sequence, bytes) in commits {
                format::open(bytes, &self.mac).map_err(|error| {
                    failure(
                        SurvivalFailure::Corrupt,
                        format!("commit {sequence}: {error:#}"),
                    )
                })?;
                if let Some(existing) = union.get(sequence) {
                    if existing != bytes {
                        return Err(failure(
                            SurvivalFailure::Fork,
                            format!("conflicting commit records at sequence {sequence}"),
                        ));
                    }
                } else {
                    union.insert(*sequence, bytes.clone());
                }
            }
        }
        if let Some((&highest, bytes)) = union.iter().next_back() {
            let record = format::open(bytes, &self.mac)?;
            let dependencies = PathBuf::from(get_str(&record, "dependencies")?);
            let relative = commit_path(self.epoch, highest);
            for (replica, commits) in self.replicas.iter().zip(&per_replica) {
                if commits.contains_key(&highest) {
                    continue;
                }
                // Complete the identical record only when this replica has
                // every dependency of the candidate.
                if !replica.exists(&dependencies).await {
                    return Err(failure(
                        SurvivalFailure::Stale,
                        format!(
                            "replica {} missed generation {highest} and lacks its dependencies; offline repair is required",
                            replica.id
                        ),
                    ));
                }
                self.publish(replica, &relative, Arc::new(bytes.clone()))
                    .await
                    .map_err(|kind| failure(kind, format!("complete fanout to {}", replica.id)))?;
            }
            let created = get_u64(&record, "created_at")?;
            let mut state = self.lock_state();
            state.last = Some((highest, digest(bytes), created));
            state.retained = union.keys().copied().collect();
            for replica in &self.replicas {
                state
                    .per_replica
                    .insert(replica.id.clone(), Some((highest, digest(bytes), created)));
            }
        } else {
            let mut state = self.lock_state();
            for replica in &self.replicas {
                state.per_replica.insert(replica.id.clone(), None);
            }
        }
        // Restore retention pins for every retained generation.
        let retained = self.lock_state().retained.clone();
        for sequence in retained {
            if let Some(export) = self.retained_export(sequence).await {
                let _ = self.pool.retain_survival_generation(sequence, &export);
            }
        }
        Ok(())
    }

    async fn retained_export(&self, sequence: u64) -> Option<CatalogExport> {
        let replica = &self.replicas[0];
        let bytes = replica.read(&dependencies_path(sequence)).await.ok()??;
        let record = format::open(&bytes, &self.mac).ok()?;
        let sha = get_hash(&record, "catalog_sha256").ok()?;
        let catalog = replica.read(&catalog_object_path(&sha)).await.ok()??;
        (digest(&catalog) == sha)
            .then(|| bincode::deserialize(&catalog).ok())
            .flatten()
    }

    /// Delete owned survival checkpoints left by an interrupted commit.
    async fn reconcile_checkpoints(&self) {
        let admin =
            slatedb::admin::AdminBuilder::new(self.db_path.clone(), self.db_store.clone()).build();
        if let Ok(checkpoints) = admin.list_checkpoints(None).await {
            for checkpoint in checkpoints {
                if checkpoint
                    .name
                    .as_deref()
                    .is_some_and(|name| name.starts_with(CHECKPOINT_PREFIX))
                {
                    let _ = admin.delete_checkpoint(checkpoint.id).await;
                }
            }
        }
    }

    /// Owned live checkpoints, for status and the zero-pins contract.
    pub(crate) async fn owned_checkpoints(&self) -> Result<Vec<slatedb::Checkpoint>> {
        let admin =
            slatedb::admin::AdminBuilder::new(self.db_path.clone(), self.db_store.clone()).build();
        Ok(admin
            .list_checkpoints(None)
            .await?
            .into_iter()
            .filter(|checkpoint| {
                checkpoint
                    .name
                    .as_deref()
                    .is_some_and(|name| name.starts_with(CHECKPOINT_PREFIX))
            })
            .collect())
    }

    /// Bytes accepted by the writeback journal that the backend does not yet
    /// hold. Zero without a journal: writes then reach the backend directly.
    fn undrained_bytes(&self) -> Result<u64> {
        let Some(writeback) = &self.writeback else {
            return Ok(0);
        };
        let status = writeback.status().map_err(|error| {
            failure(
                SurvivalFailure::Io,
                format!("read writeback status: {error:#}"),
            )
        })?;
        Ok(status
            .dirty_ram_bytes
            .saturating_add(status.dirty_ssd_reserved_bytes))
    }

    fn lock_state(&self) -> std::sync::MutexGuard<'_, CommitState> {
        self.state.lock().unwrap_or_else(|p| p.into_inner())
    }

    /// Durable publication through the backend contract: stage (durable,
    /// invisible), atomic publish, durable flush barrier.
    async fn publish(
        &self,
        replica: &Replica,
        relative: &FsPath,
        bytes: Arc<Vec<u8>>,
    ) -> Result<(), SurvivalFailure> {
        let staged = replica.stage(relative, bytes).await?;
        replica.publish(staged).await?;
        replica.flush(relative).await
    }

    /// Publish with commit boundaries. The contract's point names predate
    /// the backend trait: `*Published` brackets `stage`, `*FileSynced`
    /// brackets the atomic `publish`, and `*DirectorySynced` brackets the
    /// durable `flush` barrier.
    #[allow(clippy::too_many_arguments)]
    async fn publish_observed(
        &self,
        observer: &mut dyn CommitObserver,
        replica: &Replica,
        relative: &FsPath,
        bytes: Arc<Vec<u8>>,
        points: (CommitPoint, Option<CommitPoint>, Option<CommitPoint>),
    ) -> Result<(), SurvivalFailure> {
        replica.present().await?;
        let (published, synced, directory) = points;
        observer
            .reach(published, Side::Before, &replica.id, relative)
            .await;
        let staged = replica.stage(relative, bytes).await?;
        observer
            .reach(published, Side::After, &replica.id, relative)
            .await;
        if let Some(point) = synced {
            observer
                .reach(point, Side::Before, &replica.id, relative)
                .await;
        }
        replica.publish(staged).await?;
        if let Some(point) = synced {
            observer
                .reach(point, Side::After, &replica.id, relative)
                .await;
        }
        if let Some(point) = directory {
            observer
                .reach(point, Side::Before, &replica.id, relative)
                .await;
        }
        replica.flush(relative).await?;
        if let Some(point) = directory {
            observer
                .reach(point, Side::After, &replica.id, relative)
                .await;
        }
        Ok(())
    }

    async fn reach_all(
        &self,
        observer: &mut dyn CommitObserver,
        point: CommitPoint,
        side: Side,
        object: &FsPath,
    ) {
        for replica in &self.replicas {
            observer.reach(point, side, &replica.id, object).await;
        }
    }

    fn record_failure(&self, replica: Option<&Replica>, kind: SurvivalFailure, detail: &str) {
        let mut state = self.lock_state();
        state.last_error = Some(format!("{kind}: {detail}"));
        if let Some(replica) = replica {
            state
                .failed
                .insert(replica.id.clone(), format!("{kind}: {detail}"));
        }
        drop(state);
        if let Some(errno) = kind.latch() {
            self.fs
                .survival_admission
                .latch(errno, format!("{kind}: {detail}"));
        }
    }

    /// Validate every replica before any I/O of a new attempt.
    async fn validate_replicas(&self) -> Result<(), (usize, SurvivalFailure, String)> {
        let last = self.lock_state().last;
        for (index, replica) in self.replicas.iter().enumerate() {
            if let Err(kind) = replica.present().await {
                return Err((
                    index,
                    kind,
                    format!("replica at {} is missing or foreign", replica.describe()),
                ));
            }
            match replica.len(FsPath::new("identity.json")).await {
                Ok(Some(_)) => {}
                Ok(None) => {
                    return Err((index, SurvivalFailure::Stale, "identity is missing".into()));
                }
                Err(kind) => return Err((index, kind, "identity unreadable".into())),
            }
            let Some((sequence, digest_expected, _)) = last else {
                continue;
            };
            match replica.read(&commit_path(self.epoch, sequence)).await {
                Ok(Some(bytes)) if digest(&bytes) == digest_expected => {}
                Ok(Some(_)) => {
                    return Err((
                        index,
                        SurvivalFailure::Corrupt,
                        format!("acknowledged generation {sequence} record changed"),
                    ));
                }
                Ok(None) => {
                    return Err((
                        index,
                        SurvivalFailure::Stale,
                        format!("acknowledged generation {sequence} is missing"),
                    ));
                }
                Err(kind) => return Err((index, kind, "commit record unreadable".into())),
            }
            // The anchor names the newest generation the replica has seen
            // retired around; an older anchor means it missed later state.
            if let Ok(Some(bytes)) = replica.read(FsPath::new("anchor.json")).await {
                match format::open(&bytes, &self.mac).and_then(|anchor| get_u64(&anchor, "latest"))
                {
                    Ok(latest) if latest >= sequence => {}
                    Ok(latest) => {
                        return Err((
                            index,
                            SurvivalFailure::Stale,
                            format!(
                                "anchor names generation {latest}, behind acknowledged {sequence}"
                            ),
                        ));
                    }
                    Err(error) => {
                        return Err((index, SurvivalFailure::Corrupt, format!("{error:#}")));
                    }
                }
            }
        }
        Ok(())
    }

    pub(crate) async fn commit(&self) -> Result<SurvivalReceipt> {
        self.commit_observed(&mut NoObserver).await
    }

    /// Run one survival commit. A receipt means every configured replica
    /// durably holds the authenticated record and all of its dependencies.
    pub(crate) async fn commit_observed(
        &self,
        observer: &mut dyn CommitObserver,
    ) -> Result<SurvivalReceipt> {
        let _owner = self.commit_lock.lock().await;
        self.lock_state().attempts += 1;
        if let Some((errno, reason)) = self.fs.survival_admission.latched() {
            let kind = if errno == FsError::ReadOnlyFilesystem {
                SurvivalFailure::Lost
            } else if reason.starts_with("metadata replica is full") {
                SurvivalFailure::Full
            } else if reason.starts_with("metadata replica is stale") {
                SurvivalFailure::Stale
            } else {
                SurvivalFailure::Io
            };
            return Err(failure(kind, format!("admission is latched: {reason}")));
        }
        if let Err((index, kind, detail)) = self.validate_replicas().await {
            let replica = &self.replicas[index];
            self.record_failure(Some(replica), kind, &detail);
            return Err(failure(kind, format!("replica {}: {detail}", replica.id)));
        }
        let candidate = self.capture(observer).await?;
        let started = tokio::time::Instant::now();
        let replica_deadline = started + Duration::from_secs(self.config.replica_timeout_seconds);
        match tokio::time::timeout_at(
            replica_deadline,
            self.publish_dependencies(observer, &candidate),
        )
        .await
        {
            Ok(Ok(())) => {}
            Ok(Err((index, kind, detail))) => {
                let replica = &self.replicas[index];
                self.abort_candidate(&candidate).await;
                self.record_failure(Some(replica), kind, &detail);
                return Err(failure(kind, format!("replica {}: {detail}", replica.id)));
            }
            Err(_) => {
                self.abort_candidate(&candidate).await;
                let slow = self
                    .replicas
                    .iter()
                    .find(|replica| replica.fault_is_slow())
                    .or_else(|| self.replicas.last());
                self.record_failure(slow, SurvivalFailure::Timeout, "replica deadline expired");
                return Err(failure(
                    SurvivalFailure::Timeout,
                    "replica deadline expired before record publication",
                ));
            }
        }
        let receipt = self.publish_record(observer, &candidate).await?;
        observer.returned(&receipt);
        self.cleanup(observer, &candidate).await;
        Ok(receipt)
    }

    /// Closed phase: drain, cutoff, checkpoint and catalog export.
    async fn capture(&self, observer: &mut dyn CommitObserver) -> Result<Candidate> {
        let sequence = self.lock_state().last.map_or(1, |(seq, _, _)| seq + 1);
        let intent = intent_path(self.epoch, sequence);
        // Pre-drain with admission open so a backlog never becomes an outage.
        let predrain_deadline =
            tokio::time::Instant::now() + Duration::from_secs(self.config.predrain_max_seconds);
        let _predrained = tokio::time::timeout_at(
            predrain_deadline,
            self.fs.administrative_remote_durability(),
        )
        .await
        .map_err(|_| {
            failure(
                SurvivalFailure::Timeout,
                "pre-drain exceeded predrain_max_seconds; admission stayed open",
            )
        })?
        .map_err(|error| failure(SurvivalFailure::Io, format!("pre-drain failed: {error}")))?;
        // Close admission only once what is left to drain is small. Writes
        // that keep arriving faster than the backend drains postpone the cut;
        // they are never stalled behind it.
        loop {
            let undrained = self.undrained_bytes()?;
            if undrained <= self.config.final_residual_max_bytes {
                break;
            }
            if tokio::time::Instant::now() >= predrain_deadline {
                return Err(failure(
                    SurvivalFailure::Timeout,
                    format!(
                        "{undrained} bytes were still undrained after predrain_max_seconds, above final_residual_max_bytes; admission stayed open"
                    ),
                ));
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }

        let gate = &self.fs.survival_admission;
        self.reach_all(
            observer,
            CommitPoint::AdmissionClosed,
            Side::Before,
            &intent,
        )
        .await;
        gate.close()
            .map_err(|_| failure(SurvivalFailure::Io, "admission is latched"))?;
        let closed_at = std::time::Instant::now();
        let mut reopen = ReopenOnDrop(Some(gate));
        self.reach_all(observer, CommitPoint::AdmissionClosed, Side::After, &intent)
            .await;
        self.reach_all(
            observer,
            CommitPoint::AdmittedWorkDrained,
            Side::Before,
            &intent,
        )
        .await;
        tokio::time::timeout(
            Duration::from_secs(self.config.commit_deadline_seconds),
            gate.drained(),
        )
        .await
        .map_err(|_| failure(SurvivalFailure::Timeout, "admitted work did not drain"))?;
        self.reach_all(
            observer,
            CommitPoint::AdmittedWorkDrained,
            Side::After,
            &intent,
        )
        .await;
        self.reach_all(observer, CommitPoint::BackendCutoff, Side::Before, &intent)
            .await;
        let durable = self
            .fs
            .administrative_remote_durability()
            .await
            .map_err(|error| {
                failure(
                    SurvivalFailure::Io,
                    format!("backend cutoff failed: {error}"),
                )
            })?;
        self.reach_all(observer, CommitPoint::BackendCutoff, Side::After, &intent)
            .await;

        let checkpoint_name = format!("{CHECKPOINT_PREFIX}{}/{sequence}", self.epoch);
        let lifetime = Duration::from_secs(self.config.checkpoint_lifetime_seconds);
        let intent_bytes = Arc::new(
            seal(
                object(json!({
                    "kind": "checkpoint_intent",
                    "pool_id": self.pool_id,
                    "membership_epoch": self.epoch,
                    "sequence": sequence,
                    "checkpoint_name": checkpoint_name,
                    "expires_at": now_seconds() + lifetime.as_secs(),
                })),
                &self.mac,
            )
            .map_err(|e| failure(SurvivalFailure::Io, e))?,
        );
        // Only the authority replica, which holds the checkpoint the intent
        // names, is written while admission is closed. Other replicas receive
        // the intent with the dependencies, under the replica deadline, so a
        // slow HDD replica never lengthens the final pause.
        for (index, replica) in self
            .replicas
            .iter()
            .enumerate()
            .filter(|(_, replica)| replica.id == self.config.authority_replica_id)
        {
            observer
                .reach(
                    CommitPoint::CheckpointIntentSynced,
                    Side::Before,
                    &replica.id,
                    &intent,
                )
                .await;
            let deadline = Duration::from_secs(self.config.replica_timeout_seconds);
            let published = tokio::time::timeout(
                deadline,
                self.publish(replica, &intent, intent_bytes.clone()),
            )
            .await
            .unwrap_or(Err(SurvivalFailure::Timeout));
            if let Err(kind) = published {
                let replica = &self.replicas[index];
                reopen.reopen();
                self.record_failure(Some(replica), kind, "checkpoint intent publication failed");
                return Err(failure(kind, format!("replica {}", replica.id)));
            }
            observer
                .reach(
                    CommitPoint::CheckpointIntentSynced,
                    Side::After,
                    &replica.id,
                    &intent,
                )
                .await;
        }

        let raw =
            self.fs.db.raw_writer().ok_or_else(|| {
                failure(SurvivalFailure::Io, "survival needs a writable database")
            })?;
        // No namespace batch may commit between the checkpoint and the
        // catalog export, so every namespace segment reference has its mapping.
        let barrier = self.fs.db.flush_barrier();
        let guard = barrier.write().await;
        self.reach_all(
            observer,
            CommitPoint::CheckpointPublished,
            Side::Before,
            &intent,
        )
        .await;
        let checkpoint = raw
            .create_checkpoint(
                slatedb::config::CheckpointScope::Durable,
                &slatedb::config::CheckpointOptions {
                    lifetime: Some(lifetime),
                    source: None,
                    name: Some(checkpoint_name.clone()),
                },
            )
            .await
            .map_err(|error| failure(SurvivalFailure::Io, format!("create checkpoint: {error}")))?;
        self.reach_all(
            observer,
            CommitPoint::CheckpointPublished,
            Side::After,
            &intent,
        )
        .await;
        let export =
            self.pool.export_catalog().await.map_err(|error| {
                failure(SurvivalFailure::Io, format!("export catalog: {error}"))
            })?;
        drop(guard);
        // The checkpoint's manifest update must reach the authority before it
        // is copied.
        self.reach_all(
            observer,
            CommitPoint::CheckpointSynced,
            Side::Before,
            &intent,
        )
        .await;
        let _drained = self
            .fs
            .administrative_remote_durability()
            .await
            .map_err(|error| failure(SurvivalFailure::Io, format!("checkpoint drain: {error}")))?;
        self.reach_all(
            observer,
            CommitPoint::CheckpointSynced,
            Side::After,
            &intent,
        )
        .await;
        self.pool
            .retain_survival_generation(sequence, &export)
            .map_err(|e| failure(SurvivalFailure::Io, e))?;
        let retention = {
            let generations: Vec<String> =
                crate::pool_object_store::exported_physical_generations(&export)
                    .map_err(|e| failure(SurvivalFailure::Io, e))?
                    .into_iter()
                    .collect();
            seal(
                object(json!({
                    "kind": "retention",
                    "pool_id": self.pool_id,
                    "sequence": sequence,
                    "physical_generations": generations,
                })),
                &self.mac,
            )
            .map_err(|e| failure(SurvivalFailure::Io, e))?
        };
        let catalog = bincode::serialize(&export).map_err(|e| failure(SurvivalFailure::Io, e))?;
        // The final pause ends here, before any replica copy.
        reopen.reopen();
        let pause = closed_at.elapsed();
        tracing::debug!(?pause, sequence, "survival commit reopened admission");
        self.lock_state().last_pause_millis = Some(pause.as_millis() as u64);

        let mut hashes = std::mem::take(&mut self.lock_state().hashes);
        let objects =
            enumerate_authority(self.authority.as_ref(), &self.authority_prefix, &mut hashes).await;
        self.lock_state().hashes = hashes;
        let objects = objects.map_err(|e| failure(SurvivalFailure::Io, format!("{e:#}")))?;
        Ok(Candidate {
            intent: intent_bytes.as_ref().clone(),
            sequence,
            cutoff: durable.mutation_cutoff.sequence,
            incarnation: durable
                .mutation_cutoff
                .mutation_incarnation
                .as_uuid()
                .to_string(),
            checkpoint_id: checkpoint.id,
            checkpoint_name,
            manifest_id: checkpoint.manifest_id,
            catalog_sha: digest(&catalog),
            catalog,
            retention,
            objects,
        })
    }

    fn dependencies_record(&self, candidate: &Candidate) -> Result<Vec<u8>> {
        let mut objects = Map::new();
        for (relative, (len, sha)) in &candidate.objects {
            objects.insert(
                relative.clone(),
                json!({"sha256": hex_encode(sha), "length": len}),
            );
        }
        seal(
            object(json!({
                "kind": "dependencies",
                "pool_id": self.pool_id,
                "sequence": candidate.sequence,
                "db_path": self.db_path.to_string(),
                "objects": objects,
                "catalog_sha256": hex_encode(candidate.catalog_sha),
                "catalog_length": candidate.catalog.len(),
                "key_epoch": INITIAL_KEY_EPOCH,
                "key_sha256": hex_encode(self.key_sha),
                "retention": retention_path(candidate.sequence).to_string_lossy(),
                "retention_sha256": hex_encode(digest(&candidate.retention)),
                "checkpoint_id": candidate.checkpoint_id.to_string(),
                "checkpoint_name": candidate.checkpoint_name,
                "manifest_id": candidate.manifest_id,
            })),
            &self.mac,
        )
    }

    /// Publish every dependency to every replica, in replica order.
    async fn publish_dependencies(
        &self,
        observer: &mut dyn CommitObserver,
        candidate: &Candidate,
    ) -> Result<(), (usize, SurvivalFailure, String)> {
        let catalog = Arc::new(candidate.catalog.clone());
        let retention = Arc::new(candidate.retention.clone());
        let dependencies = Arc::new(self.dependencies_record(candidate).map_err(|error| {
            (
                0,
                SurvivalFailure::Io,
                format!("build the dependency list: {error:#}"),
            )
        })?);
        let intent = intent_path(self.epoch, candidate.sequence);
        for (index, replica) in self.replicas.iter().enumerate() {
            let fail = |kind| (index, kind, "dependency publication failed".to_owned());
            if replica.id != self.config.authority_replica_id {
                observer
                    .reach(
                        CommitPoint::CheckpointIntentSynced,
                        Side::Before,
                        &replica.id,
                        &intent,
                    )
                    .await;
                self.publish(replica, &intent, Arc::new(candidate.intent.clone()))
                    .await
                    .map_err(fail)?;
                observer
                    .reach(
                        CommitPoint::CheckpointIntentSynced,
                        Side::After,
                        &replica.id,
                        &intent,
                    )
                    .await;
            }
            let catalog_relative = catalog_object_path(&candidate.catalog_sha);
            if !replica.exists(&catalog_relative).await {
                self.publish_observed(
                    observer,
                    replica,
                    &catalog_relative,
                    catalog.clone(),
                    (
                        CommitPoint::CatalogPublished,
                        None,
                        Some(CommitPoint::CatalogSynced),
                    ),
                )
                .await
                .map_err(fail)?;
            }
            for (relative, (len, sha)) in &candidate.objects {
                let destination = object_path(sha);
                if replica.len(&destination).await.map_err(fail)? == Some(*len) {
                    continue;
                }
                let source = authority_location(&self.authority_prefix, relative);
                let bytes =
                    match object_store::ObjectStoreExt::get(self.authority.as_ref(), &source).await
                    {
                        Ok(result) => result
                            .bytes()
                            .await
                            .map_err(|_| fail(SurvivalFailure::Io))?,
                        // Deleted or rewritten since enumeration: this attempt
                        // ends without latching and the next one re-enumerates.
                        Err(object_store::Error::NotFound { .. }) => {
                            return Err((
                                index,
                                SurvivalFailure::Changed,
                                format!("{source} was deleted after it was listed"),
                            ));
                        }
                        Err(error) => {
                            return Err((
                                index,
                                SurvivalFailure::Io,
                                format!("read {source}: {error}"),
                            ));
                        }
                    };
                if digest(&bytes) != *sha {
                    return Err((
                        index,
                        SurvivalFailure::Changed,
                        format!("{source} was rewritten after it was listed"),
                    ));
                }
                let bytes = bytes.to_vec();
                self.publish_observed(
                    observer,
                    replica,
                    &destination,
                    Arc::new(bytes),
                    (
                        CommitPoint::DependencyPublished,
                        Some(CommitPoint::DependencyFileSynced),
                        Some(CommitPoint::DependencyDirectorySynced),
                    ),
                )
                .await
                .map_err(fail)?;
            }
            self.publish_observed(
                observer,
                replica,
                &dependencies_path(candidate.sequence),
                dependencies.clone(),
                (
                    CommitPoint::DependencyPublished,
                    Some(CommitPoint::DependencyFileSynced),
                    Some(CommitPoint::DependencyDirectorySynced),
                ),
            )
            .await
            .map_err(fail)?;
            // Durable payload retention precedes any record.
            self.publish_observed(
                observer,
                replica,
                &retention_path(candidate.sequence),
                retention.clone(),
                (CommitPoint::RetentionInstalled, None, None),
            )
            .await
            .map_err(fail)?;
        }
        Ok(())
    }

    fn commit_record(&self, candidate: &Candidate, dependencies_sha: [u8; 32]) -> Result<Vec<u8>> {
        let predecessor = self
            .lock_state()
            .last
            .map(|(_, digest, _)| hex_encode(digest))
            .unwrap_or_default();
        seal(
            object(json!({
                "kind": "commit",
                "format_version": format::FORMAT_VERSION,
                "pool_id": self.pool_id,
                "membership_digest": self.membership_digest,
                "membership_epoch": self.epoch,
                "sequence": candidate.sequence,
                "predecessor": predecessor,
                "mutation_cutoff": candidate.cutoff,
                "mutation_incarnation": candidate.incarnation,
                "dependencies": dependencies_path(candidate.sequence).to_string_lossy(),
                "dependencies_sha256": hex_encode(dependencies_sha),
                "catalog_sha256": hex_encode(candidate.catalog_sha),
                "key_epoch": INITIAL_KEY_EPOCH,
                "key_sha256": hex_encode(self.key_sha),
                "checkpoint_id": candidate.checkpoint_id.to_string(),
                "checkpoint_name": candidate.checkpoint_name,
                "manifest_id": candidate.manifest_id,
                "created_at": now_seconds(),
            })),
            &self.mac,
        )
    }

    async fn publish_record(
        &self,
        observer: &mut dyn CommitObserver,
        candidate: &Candidate,
    ) -> Result<SurvivalReceipt> {
        let dependencies_sha = digest(
            &self.replicas[0]
                .read(&dependencies_path(candidate.sequence))
                .await
                .map_err(|kind| failure(kind, "re-read the dependency list"))?
                .ok_or_else(|| failure(SurvivalFailure::Io, "dependency list vanished"))?,
        );
        let record = self
            .commit_record(candidate, dependencies_sha)
            .map_err(|e| failure(SurvivalFailure::Io, e))?;
        let relative = commit_path(self.epoch, candidate.sequence);
        let mut receipt = SurvivalReceipt {
            relative_record: relative.clone(),
            digest: digest(&record),
            sequence: candidate.sequence,
            mutation_cutoff: candidate.cutoff,
            envelope_epoch: INITIAL_KEY_EPOCH,
            completed_at: 0,
        };
        observer.candidate(&receipt);
        let created_at = format::open(&record, &self.mac)
            .and_then(|record| get_u64(&record, "created_at"))
            .map_err(|e| failure(SurvivalFailure::Io, e))?;
        let record = Arc::new(record);
        let deadline =
            tokio::time::Instant::now() + Duration::from_secs(self.config.replica_timeout_seconds);
        for (published, replica) in self.replicas.iter().enumerate() {
            let result = tokio::time::timeout_at(
                deadline,
                self.publish_observed(
                    observer,
                    replica,
                    &relative,
                    record.clone(),
                    (
                        CommitPoint::CommitPublished,
                        Some(CommitPoint::CommitFileSynced),
                        Some(CommitPoint::CommitDirectorySynced),
                    ),
                ),
            )
            .await
            .unwrap_or(Err(SurvivalFailure::Timeout));
            if let Err(kind) = result {
                // A durable record may exist on some replicas. The exact
                // candidate must be reconciled before admission reopens.
                let kind = if published > 0 && kind == SurvivalFailure::Timeout {
                    SurvivalFailure::Io
                } else {
                    kind
                };
                if published > 0 {
                    self.record_failure(
                        Some(replica),
                        kind.latch().map_or(SurvivalFailure::Io, |_| kind),
                        "partial record fanout; restart reconciles this exact candidate",
                    );
                } else {
                    self.abort_candidate(candidate).await;
                    self.record_failure(Some(replica), kind, "record publication failed");
                }
                return Err(failure(kind, format!("replica {}", replica.id)));
            }
            self.lock_state().per_replica.insert(
                replica.id.clone(),
                Some((candidate.sequence, receipt.digest, created_at)),
            );
        }
        receipt.completed_at = now_seconds();
        let mut state = self.lock_state();
        state.last = Some((candidate.sequence, receipt.digest, receipt.completed_at));
        state.failed.clear();
        state.last_error = None;
        state.retained.push(candidate.sequence);
        Ok(receipt)
    }

    async fn delete_checkpoint(&self, id: uuid::Uuid) {
        let admin =
            slatedb::admin::AdminBuilder::new(self.db_path.clone(), self.db_store.clone()).build();
        if let Err(error) = admin.delete_checkpoint(id).await {
            tracing::warn!(%error, "failed to delete owned survival checkpoint; its finite lifetime will expire it");
        }
    }

    /// Before any record: release the candidate's pin and checkpoint.
    async fn abort_candidate(&self, candidate: &Candidate) {
        self.delete_checkpoint(candidate.checkpoint_id).await;
        let keep: BTreeSet<u64> = self.lock_state().retained.iter().copied().collect();
        self.pool.release_survival_generations(&keep);
    }

    /// Release the live checkpoint, retire generations beyond the retained
    /// pair everywhere and sweep bytes no retained generation references.
    async fn cleanup(&self, observer: &mut dyn CommitObserver, candidate: &Candidate) {
        if let Err(error) = self.cleanup_inner(observer, candidate).await {
            tracing::warn!(error = %format!("{error:#}"), "survival cleanup will be retried by the next commit");
            self.lock_state().last_error = Some(format!("cleanup: {error:#}"));
        }
    }

    async fn cleanup_inner(
        &self,
        observer: &mut dyn CommitObserver,
        candidate: &Candidate,
    ) -> Result<()> {
        let (retained, retired) = {
            let state = self.lock_state();
            let mut all = state.retained.clone();
            all.sort_unstable();
            all.dedup();
            let split = all.len().saturating_sub(RETAINED_GENERATIONS);
            (all[split..].to_vec(), all[..split].to_vec())
        };
        let retirement = Arc::new(seal(
            object(json!({
                "kind": "retirement",
                "pool_id": self.pool_id,
                "membership_epoch": self.epoch,
                "sequence": candidate.sequence,
                "retired": retired,
                "retained": retained,
            })),
            &self.mac,
        )?);
        let retirement_path = PathBuf::from(format!(
            "retirements/{}/{}.json",
            self.epoch, candidate.sequence
        ));
        for replica in &self.replicas {
            self.publish_observed(
                observer,
                replica,
                &retirement_path,
                retirement.clone(),
                (CommitPoint::RetirementLogged, None, None),
            )
            .await
            .map_err(|kind| failure(kind, format!("retirement on {}", replica.id)))?;
        }
        let intent = intent_path(self.epoch, candidate.sequence);
        for (index, replica) in self.replicas.iter().enumerate() {
            observer
                .reach(
                    CommitPoint::CheckpointReleased,
                    Side::Before,
                    &replica.id,
                    &intent,
                )
                .await;
            if index == 0 {
                self.delete_checkpoint(candidate.checkpoint_id).await;
            }
            replica
                .remove(&intent)
                .await
                .map_err(|kind| failure(kind, format!("release intent on {}", replica.id)))?;
            replica
                .flush(&intent)
                .await
                .map_err(|kind| failure(kind, format!("flush {}", replica.id)))?;
            observer
                .reach(
                    CommitPoint::CheckpointReleased,
                    Side::After,
                    &replica.id,
                    &intent,
                )
                .await;
        }
        // Mark from every retained dependency list; sweep the rest.
        let mut live_objects = BTreeSet::new();
        let mut live_catalogs = BTreeSet::new();
        for sequence in &retained {
            let bytes = self.replicas[0]
                .read(&dependencies_path(*sequence))
                .await
                .map_err(|kind| failure(kind, "read a retained dependency list"))?
                .ok_or_else(|| {
                    failure(SurvivalFailure::Corrupt, "retained dependency list missing")
                })?;
            let record = format::open(&bytes, &self.mac)?;
            live_catalogs.insert(hex_encode(get_hash(&record, "catalog_sha256")?));
            if let Some(Value::Object(objects)) = record.get("objects") {
                for entry in objects.values() {
                    if let Some(sha) = entry.get("sha256").and_then(Value::as_str) {
                        live_objects.insert(sha.to_owned());
                    }
                }
            }
        }
        for replica in &self.replicas {
            let mut doomed: Vec<PathBuf> = Vec::new();
            for sequence in &retired {
                doomed.push(commit_path(self.epoch, *sequence));
                doomed.push(dependencies_path(*sequence));
                doomed.push(retention_path(*sequence));
                doomed.push(PathBuf::from(format!(
                    "retirements/{}/{sequence}.json",
                    self.epoch
                )));
            }
            let listed = |kind| failure(kind, format!("list {}", replica.id));
            for name in replica.list(FsPath::new("objects")).await.map_err(listed)? {
                if !live_objects.contains(&name) {
                    doomed.push(PathBuf::from(format!("objects/{name}")));
                }
            }
            for name in replica
                .list(FsPath::new("catalog/objects"))
                .await
                .map_err(listed)?
            {
                if !live_catalogs.contains(&name) {
                    doomed.push(PathBuf::from(format!("catalog/objects/{name}")));
                }
            }
            let mut touched = BTreeSet::new();
            for relative in doomed {
                if !replica.exists(&relative).await {
                    continue;
                }
                touched.insert(relative.parent().map(FsPath::to_owned).unwrap_or_default());
                observer
                    .reach(
                        CommitPoint::DependencyDeleted,
                        Side::Before,
                        &replica.id,
                        &relative,
                    )
                    .await;
                replica
                    .remove(&relative)
                    .await
                    .map_err(|kind| failure(kind, format!("delete on {}", replica.id)))?;
                observer
                    .reach(
                        CommitPoint::DependencyDeleted,
                        Side::After,
                        &replica.id,
                        &relative,
                    )
                    .await;
            }
            // Durable barrier covering every deletion above.
            for parent in touched {
                replica
                    .flush(&parent.join("_"))
                    .await
                    .map_err(|kind| failure(kind, format!("flush {}", replica.id)))?;
            }
        }
        let anchor = Arc::new(seal(
            object(json!({
                "kind": "anchor",
                "pool_id": self.pool_id,
                "membership_epoch": self.epoch,
                "oldest_retained": retained.first().copied().unwrap_or(candidate.sequence),
                "retained": retained,
                "latest": candidate.sequence,
            })),
            &self.mac,
        )?);
        for replica in &self.replicas {
            self.publish_observed(
                observer,
                replica,
                FsPath::new("anchor.json"),
                anchor.clone(),
                (CommitPoint::DebtCompletionCommitted, None, None),
            )
            .await
            .map_err(|kind| failure(kind, format!("anchor on {}", replica.id)))?;
        }
        let keep: BTreeSet<u64> = retained.iter().copied().collect();
        self.lock_state().retained = retained;
        // Released bytes stay charged as debt here. A commit never mutates the
        // catalog it just recorded; reclaim runs between commits.
        self.pool.release_survival_generations(&keep);
        Ok(())
    }

    pub(crate) async fn status(&self) -> SurvivalStatus {
        let owned = self.owned_checkpoints().await.map(|c| c.len()).unwrap_or(0);
        let state = self.lock_state();
        let now = now_seconds();
        let since = state.last.map_or(state.opened_at, |(_, _, at)| at);
        let lag = Duration::from_secs(now.saturating_sub(since));
        let admission = match self.fs.survival_admission.latched() {
            Some((errno, reason)) => format!("latched ({errno:?}): {reason}"),
            None if self.fs.survival_admission.is_open() => "open".into(),
            None => "closed for commit".into(),
        };
        SurvivalStatus {
            enabled: true,
            commit_interval_seconds: self.config.commit_interval_seconds,
            last_sequence: state.last.map(|(seq, _, _)| seq),
            last_digest: state.last.map(|(_, digest, _)| hex_encode(digest)),
            completed_at: state.last.map(|(_, _, at)| at),
            lag_seconds: lag.as_secs(),
            lag,
            failed_replicas: state.failed.clone(),
            admission_open: self.fs.survival_admission.is_open(),
            admission,
            owned_checkpoints: owned,
            retained_generations: state.retained.clone(),
            replicas: self
                .replicas
                .iter()
                .map(|replica| {
                    let last = state.per_replica.get(&replica.id).copied().flatten();
                    ReplicaStatus {
                        id: replica.id.clone(),
                        device_id: replica.device_id.clone(),
                        last_sequence: last.map(|(seq, _, _)| seq),
                        last_digest: last.map(|(_, digest, _)| hex_encode(digest)),
                        generation_created_at: last.map(|(_, _, at)| at),
                        generation_age_seconds: last.map(|(_, _, at)| now.saturating_sub(at)),
                        region_bytes: state.region_bytes.get(&replica.id).copied(),
                        error: state.failed.get(&replica.id).cloned(),
                    }
                })
                .collect(),
            attempts: state.attempts,
            last_error: state.last_error.clone(),
            last_commit_millis: state.last_commit_millis,
            last_pause_millis: state.last_pause_millis,
        }
    }

    /// Hand the current status to the pool, which serves it in `pool status`.
    pub(crate) async fn publish_status(&self) {
        self.pool.set_survival_status(self.status().await);
    }

    /// Run one commit, measure it and publish the resulting status.
    pub(crate) async fn commit_and_report(&self) -> Result<SurvivalReceipt> {
        let started = std::time::Instant::now();
        let result = self.commit().await;
        if result.is_ok() {
            let mut sizes = BTreeMap::new();
            for replica in &self.replicas {
                if let Ok(bytes) = replica.usage().await {
                    sizes.insert(replica.id.clone(), bytes);
                }
            }
            let mut state = self.lock_state();
            state.region_bytes = sizes;
            state.last_commit_millis = Some(started.elapsed().as_millis() as u64);
        }
        self.publish_status().await;
        result
    }

    /// The final commit of a clean shutdown, bounded by the commit deadline.
    /// A failure leaves the previous generation as the recovery point and
    /// never blocks the shutdown.
    pub(crate) async fn shutdown_commit(&self) {
        let deadline = Duration::from_secs(
            self.config.predrain_max_seconds + self.config.commit_deadline_seconds,
        );
        match tokio::time::timeout(deadline, self.commit_and_report()).await {
            Ok(Ok(receipt)) => tracing::info!(
                sequence = receipt.sequence,
                "final metadata generation is durable on every replica"
            ),
            Ok(Err(error)) => tracing::error!(
                error = %format!("{error:#}"),
                "final metadata generation failed; the previous generation remains the recovery point"
            ),
            Err(_) => tracing::error!(
                "final metadata generation exceeded its deadline; the previous generation remains the recovery point"
            ),
        }
    }

    /// Online replica replacement is refused: it needs an offline epoch
    /// transition with verified identities and every retained byte.
    pub(crate) fn replace_replica_online(&self, replica_id: &str) -> Result<()> {
        bail!(
            "online replacement of metadata replica {replica_id} is refused; stop the writer and run offline verified catch-up"
        )
    }

    /// True when this owner still holds every replica's exclusive lock.
    pub(crate) fn holds_every_lock(&self) -> bool {
        self.locks
            .lock()
            .map(|locks| locks.len() == self.replicas.len())
            .unwrap_or(false)
    }

    /// Release every replica lock without a final commit.
    pub(crate) fn release_locks(&self) {
        self.locks.lock().unwrap_or_else(|p| p.into_inner()).clear();
    }

    #[cfg(test)]
    pub(crate) fn inject_fault(&self, replica_id: &str, fault: Option<InjectedFault>) {
        for replica in &self.replicas {
            if replica.id == replica_id {
                replica.fault.set(fault);
            }
        }
    }

    /// Clear a transient fault after its cause is gone: revalidate identity,
    /// retained bytes and the acknowledged record on the unchanged set, then
    /// unlatch admission.
    pub(crate) async fn clear_transient_fault(&self) -> Result<()> {
        if let Err((index, kind, detail)) = self.validate_replicas().await {
            bail!(failure(
                kind,
                format!("replica {}: {detail}", self.replicas[index].id)
            ));
        }
        if self
            .fs
            .survival_admission
            .latched()
            .is_some_and(|(errno, _)| errno == FsError::ReadOnlyFilesystem)
        {
            bail!(failure(
                SurvivalFailure::Lost,
                "a lost replica needs offline promotion or replacement"
            ));
        }
        self.fs.survival_admission.unlatch();
        let mut state = self.lock_state();
        state.failed.clear();
        state.last_error = None;
        Ok(())
    }
}

impl Replica {
    fn fault_is_slow(&self) -> bool {
        matches!(self.fault.get(), Some(InjectedFault::Slow(_)))
    }
}

/// Attach the survival commit owner to a writable pool server and start its
/// interval commits. Refuses a pool whose replicas record durable enablement
/// when the configuration table is missing: removing the table never
/// disables survival.
pub(crate) async fn attach_writer(
    settings: &Settings,
    fs: &Arc<ZeroFS>,
    pool: Option<&PoolObjectStore>,
    db_store: &Arc<dyn object_store::ObjectStore>,
    db_path: &str,
    writeback: Option<&crate::writeback::store::WritebackObjectStore>,
) -> Result<Option<Arc<SurvivalManager>>> {
    let Some(pool_config) = settings.storage.pool.as_ref() else {
        return Ok(None);
    };
    let configured = pool_config.metadata_survival.is_some();
    if !configured {
        if let Some(marker) = durable_enablement(settings, pool).await? {
            return Err(failure(
                SurvivalFailure::Configuration,
                format!(
                    "{marker} records durable metadata-survival enablement but [storage.pool.metadata_survival] is missing; restore the table (offline disable is required to turn survival off)"
                ),
            ));
        }
        return Ok(None);
    }
    if fs.db.is_read_only() {
        return Ok(None);
    }
    if settings.wal.is_some() {
        bail!("metadata survival does not support a separate [wal] store");
    }
    let pool = pool.context("metadata survival requires [storage.pool]")?;
    let path = object_store::path::Path::from(db_path);
    let dek = crate::key_management::load_existing_encryption_key_with_mode(
        db_store,
        &path,
        &settings.storage.encryption_password,
        settings.name_mode(),
    )
    .await?;
    let manager = SurvivalManager::open(
        settings,
        WriterHandles {
            fs: Arc::clone(fs),
            pool: pool.clone(),
            db_store: Arc::clone(db_store),
            db_path: path,
            dek,
            writeback: writeback.cloned(),
        },
    )
    .await?;
    let interval = Duration::from_secs(manager.config.commit_interval_seconds.max(1));
    let weak = Arc::downgrade(&manager);
    crate::task::spawn_named("metadata-survival-commits", async move {
        loop {
            tokio::time::sleep(interval).await;
            let Some(manager) = weak.upgrade() else {
                return;
            };
            match manager.pool.cleanup_released_survival_garbage().await {
                Ok(0) => {}
                Ok(count) => {
                    tracing::info!(count, "reclaimed garbage no survival generation retains")
                }
                Err(error) => tracing::warn!(%error, "released survival garbage reclaim failed"),
            }
            match manager.commit_and_report().await {
                Ok(receipt) => tracing::info!(
                    sequence = receipt.sequence,
                    digest = %hex_encode(receipt.digest),
                    "metadata survival generation committed to every replica"
                ),
                Err(error) => tracing::error!(
                    error = %format!("{error:#}"),
                    "metadata survival commit failed; survival is not acknowledged"
                ),
            }
        }
    });
    Ok(Some(manager))
}

/// Refuse a password change while survival is enabled, including when the
/// configuration table was removed: replicas hold the old envelope epoch.
pub(crate) async fn refuse_password_change(
    settings: &Settings,
    pool: Option<&PoolObjectStore>,
) -> Result<()> {
    let configured = settings
        .storage
        .pool
        .as_ref()
        .is_some_and(|pool| pool.metadata_survival.is_some());
    let durable =
        settings.storage.pool.is_some() && durable_enablement(settings, pool).await?.is_some();
    if configured || durable {
        return Err(failure(
            SurvivalFailure::Configuration,
            "password changes are refused while metadata survival is enabled; every replica records the current key envelope",
        ));
    }
    Ok(())
}

/// Raw members opened on their own, with the authority device not involved.
pub(crate) struct OfflineMembers {
    /// One replica per configured member, in replica-ID order. A member that
    /// could not be opened is present here as an absent region.
    pub(crate) replicas: Vec<Replica>,
    /// The members that opened, holding their device claims.
    pub(crate) opened: Vec<crate::pool_object_store::PoolMember>,
    /// Why each remaining member could not be opened, by member id.
    pub(crate) unopened: BTreeMap<String, String>,
}

/// Claim and open every configured raw member without the pool. Nothing is
/// written. Offline status and recovery use this, so both work after the
/// authority device is lost.
pub(crate) async fn offline_member_replicas(settings: &Settings) -> Result<OfflineMembers> {
    let pool = settings
        .storage
        .pool
        .as_ref()
        .context("[storage.pool] is not configured")?;
    let mut offline = OfflineMembers {
        replicas: Vec::new(),
        opened: Vec::new(),
        unopened: BTreeMap::new(),
    };
    for member in &pool.members {
        let url = url::Url::parse(&member.url)?;
        let identity = crate::storage_backend::raw::RawBinding::parse(&url)?.locator();
        let survival = match crate::storage_backend::open_raw_member(settings, member).await {
            Ok(opened) => {
                let survival = crate::pool_object_store::SurvivalMember {
                    id: member.id.clone(),
                    identity,
                    store: Arc::clone(&opened.store),
                    pool: None,
                    available: true,
                };
                offline.opened.push(opened);
                survival
            }
            Err(error) => {
                offline
                    .unopened
                    .insert(member.id.clone(), format!("{error:#}"));
                crate::pool_object_store::SurvivalMember {
                    id: member.id.clone(),
                    identity,
                    store: Arc::new(object_store::memory::InMemory::new()),
                    pool: None,
                    available: false,
                }
            }
        };
        offline.replicas.push(member_replica(survival));
    }
    offline.replicas.sort_by(|a, b| a.id.cmp(&b.id));
    Ok(offline)
}

/// Read a region's identity and unwrap its recorded key envelope with the
/// password, through the backend contract only.
async fn region_key(
    region: &dyn backend::ReplicaBackend,
    password: &str,
) -> Result<(MacKey, Map<String, Value>, Vec<u8>)> {
    let identity_bytes = region
        .get("identity.json")
        .await?
        .context("region has no identity")?
        .to_vec();
    let identity = format::parse(&identity_bytes)?;
    let key_record = region
        .get(&replica::key(&key_path(get_u64(&identity, "key_epoch")?)))
        .await?
        .context("region has no key record")?;
    let key_record = format::parse(&key_record)?;
    let envelope =
        format::hex_decode(get_str(&key_record, "envelope_hex")?).context("envelope hex")?;
    let store: Arc<dyn object_store::ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let path = object_store::path::Path::from("survival-region");
    object_store::ObjectStoreExt::put(
        store.as_ref(),
        &path
            .clone()
            .join(crate::key_management::WRAPPED_KEY_FILENAME),
        envelope.into(),
    )
    .await?;
    let (dek, _) =
        crate::key_management::load_existing_encryption_key_recorded_mode(&store, &path, password)
            .await?;
    let mac = MacKey::derive(&dek, get_str(&identity, "pool_id")?);
    format::open(&identity_bytes, &mac).context("region identity does not authenticate")?;
    Ok((mac, identity, identity_bytes))
}

/// Per-replica generation status, read through the backend contract.
pub(crate) async fn offline_status(settings: &Settings) -> Result<SurvivalStatus> {
    let pool = settings
        .storage
        .pool
        .as_ref()
        .context("[storage.pool] is not configured")?;
    let config = pool
        .metadata_survival
        .as_ref()
        .context("[storage.pool.metadata_survival] is not configured")?;
    let (replicas, unopened) = if settings.raw_p3_authority() {
        let offline = offline_member_replicas(settings).await?;
        (offline.replicas, offline.unopened)
    } else {
        (replica_set(settings, config, None)?, BTreeMap::new())
    };
    let mut statuses = Vec::new();
    let mut failed = BTreeMap::new();
    let mut lasts: Vec<Option<(u64, [u8; 32], u64)>> = Vec::new();
    for replica in &replicas {
        let mut status = ReplicaStatus {
            id: replica.id.clone(),
            device_id: replica.device_id.clone(),
            last_sequence: None,
            last_digest: None,
            generation_created_at: None,
            generation_age_seconds: None,
            region_bytes: None,
            error: None,
        };
        let result: Result<Option<(u64, [u8; 32], u64)>> = async {
            if let Some(reason) = unopened.get(&replica.id) {
                bail!(failure(
                    SurvivalFailure::Lost,
                    format!("{} cannot be opened: {reason}", replica.describe())
                ));
            }
            if let Err(kind) = replica.present().await {
                bail!(failure(kind, format!("replica at {}", replica.describe())));
            }
            let (mac, identity, _) = region_key(
                replica.backend.as_ref(),
                &settings.storage.encryption_password,
            )
            .await?;
            let epoch = get_u64(&identity, "membership_epoch")?;
            let commits = replica_commits(replica.backend.as_ref(), epoch).await?;
            let Some((sequence, bytes)) = commits.iter().next_back() else {
                return Ok(None);
            };
            let record = format::open(bytes, &mac)?;
            Ok(Some((
                *sequence,
                digest(bytes),
                get_u64(&record, "created_at")?,
            )))
        }
        .await;
        match result {
            Ok(last) => {
                status.last_sequence = last.map(|(seq, _, _)| seq);
                status.last_digest = last.map(|(_, digest, _)| hex_encode(digest));
                status.generation_created_at = last.map(|(_, _, at)| at);
                status.generation_age_seconds =
                    last.map(|(_, _, at)| now_seconds().saturating_sub(at));
                lasts.push(last);
            }
            Err(error) => {
                status.error = Some(format!("{error:#}"));
                failed.insert(replica.id.clone(), format!("{error:#}"));
                lasts.push(None);
            }
        }
        statuses.push(status);
    }
    // Acknowledged only when every replica holds a generation: the lowest.
    let acknowledged = lasts
        .iter()
        .copied()
        .collect::<Option<Vec<_>>>()
        .and_then(|all| all.into_iter().min_by_key(|(seq, _, _)| *seq));
    let now = now_seconds();
    let lag = acknowledged.map_or(0, |(_, _, at)| now.saturating_sub(at));
    Ok(SurvivalStatus {
        enabled: true,
        commit_interval_seconds: config.commit_interval_seconds,
        last_sequence: acknowledged.map(|(seq, _, _)| seq),
        last_digest: acknowledged.map(|(_, digest, _)| hex_encode(digest)),
        completed_at: acknowledged.map(|(_, _, at)| at),
        lag_seconds: lag,
        lag: Duration::from_secs(lag),
        failed_replicas: failed,
        admission: "unknown (offline status)".into(),
        admission_open: false,
        owned_checkpoints: 0,
        retained_generations: Vec::new(),
        replicas: statuses,
        attempts: 0,
        last_error: None,
        last_commit_millis: None,
        last_pause_millis: None,
    })
}

/// The survival-owned key namespaces of one region, for whole-region copies.
fn region_namespaces(epoch: u64) -> Vec<String> {
    vec![
        String::new(),
        "keys".into(),
        "objects".into(),
        "catalog/objects".into(),
        "dependencies".into(),
        "retention".into(),
        format!("commits/{epoch}"),
        format!("intents/{epoch}"),
        format!("retirements/{epoch}"),
    ]
}

/// Offline verified catch-up of a replacement replica region.
///
/// The writer must be stopped. Every retained record and dependency on the
/// healthy `source` region is authenticated and hash-checked, then copied
/// byte for byte into the freshly initialized `destination` region through
/// the backend contract, re-read and verified. The replacement gets its own
/// identity record that keeps the pool, membership, epoch and replica id and
/// binds `device_id`. Online replacement is refused.
pub(crate) async fn offline_catch_up(
    source: &dyn backend::ReplicaBackend,
    destination: &dyn backend::ReplicaBackend,
    replica_id: &str,
    device_id: &str,
    password: &str,
) -> Result<()> {
    let _source_fence = source.fence_shared().await?;
    let (mac, identity, identity_bytes) = region_key(source, password).await?;
    let replicas = identity
        .get("replicas")
        .and_then(Value::as_array)
        .context("identity lists no replicas")?;
    ensure!(
        replicas
            .iter()
            .any(|r| r.get("id").and_then(Value::as_str) == Some(replica_id)),
        "{replica_id} is not a member of this replica set"
    );
    let epoch = get_u64(&identity, "membership_epoch")?;
    for (sequence, bytes) in replica_commits(source, epoch).await? {
        let record = format::open(&bytes, &mac)
            .with_context(|| format!("source commit {sequence} does not authenticate"))?;
        recover_plan_check(source, &record, &mac).await?;
    }
    ensure!(
        destination.probe().await? == backend::Presence::Present,
        "replacement device {} is not present",
        destination.describe()
    );
    ensure!(
        !destination.initialized().await?,
        "replacement {} already holds a survival region",
        destination.describe()
    );
    destination.initialize().await?;
    let _destination_fence = destination.fence_exclusive().await?;
    let mut copied = Vec::new();
    for namespace in region_namespaces(epoch) {
        for name in source.list(&namespace).await? {
            let key = if namespace.is_empty() {
                name
            } else {
                format!("{namespace}/{name}")
            };
            if key == "identity.json" {
                continue;
            }
            let bytes = source
                .get(&key)
                .await?
                .with_context(|| format!("{key} vanished during catch-up"))?;
            let staged = destination.stage(&key, bytes.clone()).await?;
            destination.publish(staged).await?;
            destination.flush(&key).await?;
            copied.push((key, digest(&bytes)));
        }
    }
    let mut replacement = format::parse(&identity_bytes)?;
    replacement.remove("mac");
    replacement.insert("replica_id".into(), Value::String(replica_id.into()));
    replacement.insert("device_id".into(), Value::String(device_id.into()));
    let staged = destination
        .stage("identity.json", seal(replacement, &mac)?.into())
        .await?;
    destination.publish(staged).await?;
    destination.flush("identity.json").await?;
    for (key, expected) in copied {
        let actual = destination
            .get(&key)
            .await?
            .with_context(|| format!("replacement lost {key}"))?;
        ensure!(
            digest(&actual) == expected,
            "replacement copy of {key} did not verify"
        );
    }
    Ok(())
}

async fn recover_plan_check(
    region: &dyn backend::ReplicaBackend,
    record: &Map<String, Value>,
    mac: &MacKey,
) -> Result<()> {
    let read = |key: String| async move {
        region
            .get(&key)
            .await?
            .with_context(|| format!("{key} is missing"))
    };
    let dependencies = replica::key(&format::safe_relative(get_str(record, "dependencies")?)?);
    let bytes = read(dependencies).await?;
    ensure!(
        digest(&bytes) == get_hash(record, "dependencies_sha256")?,
        "dependency list digest mismatch"
    );
    let list = format::open(&bytes, mac)?;
    if let Some(Value::Object(objects)) = list.get("objects") {
        for entry in objects.values() {
            let sha = format::parse_hash(
                entry
                    .get("sha256")
                    .and_then(Value::as_str)
                    .context("object digest")?,
            )?;
            ensure!(
                digest(&read(replica::key(&object_path(&sha))).await?) == sha,
                "retained object {} is corrupt",
                hex_encode(sha)
            );
        }
    }
    let catalog = get_hash(record, "catalog_sha256")?;
    ensure!(
        digest(&read(replica::key(&catalog_object_path(&catalog))).await?) == catalog,
        "retained catalog is corrupt"
    );
    Ok(())
}
