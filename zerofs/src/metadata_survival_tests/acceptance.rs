//! Executable acceptance contracts. No survival implementation lives here.

use super::*;
use crate::fs::key_codec::KeyCodec;
use crate::pool_object_store::survival_test_support::{self, CatalogImage};
use crate::segment::FrameLoc;
use async_trait::async_trait;
use futures::TryStreamExt;
use std::collections::BTreeMap;
use std::path::{Component, Path};
use tokio::sync::oneshot;

const REPLICA_TIMEOUT: Duration = Duration::from_secs(30);
const REPLACEMENT_DEVICE_ID: &str = "fixture-replacement-device";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Row {
    AuthorityLoss,
    JournalBeforeBackend,
    JournalAfterBackend,
    DataAndReplicaLoss,
    PayloadRetention,
    AdmittedWriteLoss,
    InterruptedCommit,
    ExactRecovery,
    SlowReplica,
    FullReplica,
    StaleReplica,
    ErroringReplica,
    CompetingWriter,
    ReplacementCatchUp,
}

impl Row {
    const ALL: [Self; 14] = [
        Self::AuthorityLoss,
        Self::JournalBeforeBackend,
        Self::JournalAfterBackend,
        Self::DataAndReplicaLoss,
        Self::PayloadRetention,
        Self::AdmittedWriteLoss,
        Self::InterruptedCommit,
        Self::ExactRecovery,
        Self::SlowReplica,
        Self::FullReplica,
        Self::StaleReplica,
        Self::ErroringReplica,
        Self::CompetingWriter,
        Self::ReplacementCatchUp,
    ];

    fn fault(self) -> Option<ReplicaFault> {
        match self {
            Self::SlowReplica => Some(ReplicaFault::Slow),
            Self::FullReplica => Some(ReplicaFault::Full),
            Self::StaleReplica => Some(ReplicaFault::Stale),
            Self::ErroringReplica => Some(ReplicaFault::Io),
            Self::CompetingWriter => Some(ReplicaFault::SecondWriter),
            Self::ReplacementCatchUp => Some(ReplicaFault::Replacement),
            _ => None,
        }
    }
}

// The future commit owner must call the test hook before and after each named
// operation. File operations repeat for every dependency, with its exact path.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum CommitPoint {
    AdmissionClosed,
    AdmittedWorkDrained,
    BackendCutoff,
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

impl CommitPoint {
    const ALL: [Self; 18] = [
        Self::AdmissionClosed,
        Self::AdmittedWorkDrained,
        Self::BackendCutoff,
        Self::CheckpointPublished,
        Self::CheckpointSynced,
        Self::CatalogPublished,
        Self::CatalogSynced,
        Self::DependencyPublished,
        Self::DependencyFileSynced,
        Self::DependencyDirectorySynced,
        Self::RetentionInstalled,
        Self::CommitPublished,
        Self::CommitFileSynced,
        Self::CommitDirectorySynced,
        Self::RetirementLogged,
        Self::CheckpointReleased,
        Self::DependencyDeleted,
        Self::DebtCompletionCommitted,
    ];
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Side {
    Before,
    After,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Device {
    Authority,
    Journal,
    DiskA,
    DiskB,
}

impl Device {
    fn directory(self) -> &'static str {
        match self {
            Self::Authority => "authority-device",
            Self::Journal => "journal-device",
            Self::DiskA => "disk-a-device",
            Self::DiskB => "disk-b-device",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Cut {
    point: CommitPoint,
    side: Side,
    replica: &'static str,
    lost: Device,
}

fn cuts() -> Vec<Cut> {
    let mut cuts = Vec::new();
    for point in CommitPoint::ALL {
        for side in [Side::Before, Side::After] {
            for replica in ["authority", "disk-a-metadata"] {
                for lost in [
                    Device::Authority,
                    Device::Journal,
                    Device::DiskA,
                    Device::DiskB,
                ] {
                    cuts.push(Cut {
                        point,
                        side,
                        replica,
                        lost,
                    });
                }
            }
        }
    }
    cuts
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct CutEvent {
    cut: Cut,
    object: PathBuf,
}

fn valid_object_path(path: &Path) -> bool {
    !path.as_os_str().is_empty()
        && path
            .components()
            .all(|part| matches!(part, Component::Normal(_)))
}

// A test build of the future commit owner calls this at each actual boundary.
// Merely reporting a requested cut as successful does not signal this channel.
pub(crate) struct CommitCutHook {
    cut: Cut,
    entered: Option<oneshot::Sender<CutEvent>>,
    release: Option<oneshot::Receiver<()>>,
}

impl CommitCutHook {
    fn channel(cut: Cut) -> (Self, oneshot::Receiver<CutEvent>, oneshot::Sender<()>) {
        let (entered, receiver) = oneshot::channel();
        let (resume, release) = oneshot::channel();
        (
            Self {
                cut,
                entered: Some(entered),
                release: Some(release),
            },
            receiver,
            resume,
        )
    }

    pub(crate) async fn reach(
        &mut self,
        point: CommitPoint,
        side: Side,
        replica: &str,
        object: &Path,
    ) {
        if (point, side, replica) != (self.cut.point, self.cut.side, self.cut.replica)
            || !valid_object_path(object)
        {
            return;
        }
        if let Some(entered) = self.entered.take() {
            let _ = entered.send(CutEvent {
                cut: self.cut,
                object: object.to_owned(),
            });
            if let Some(release) = self.release.take() {
                let _ = release.await;
            }
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ReplicaFault {
    Slow,
    Full,
    Stale,
    Io,
    SecondWriter,
    Replacement,
}

#[derive(Clone, Debug)]
struct SurvivalReceipt {
    relative_record: PathBuf,
    digest: [u8; 32],
    mutation_cutoff: u64,
    envelope_epoch: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct ReadRange {
    names: Vec<String>,
    inode: u64,
    offset: u64,
    length: u32,
    member: Option<String>,
    sha256: [u8; 32],
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct LostRange {
    names: Vec<String>,
    inode: u64,
    offset: u64,
    length: u32,
    member: Option<String>,
}

impl ReadRange {
    fn lost_range(&self) -> LostRange {
        LostRange {
            names: self.names.clone(),
            inode: self.inode,
            offset: self.offset,
            length: self.length,
            member: self.member.clone(),
        }
    }
}

fn canonical_lost(mut ranges: Vec<LostRange>) -> Vec<LostRange> {
    for range in &mut ranges {
        range.names.sort();
    }
    ranges.sort_by_key(|r| (r.inode, r.offset));
    let mut output: Vec<LostRange> = Vec::new();
    for range in ranges {
        if let Some(previous) = output.last_mut()
            && previous.inode == range.inode
            && previous.names == range.names
            && previous.member == range.member
            && previous.offset + u64::from(previous.length) == range.offset
        {
            previous.length += range.length;
        } else {
            output.push(range);
        }
    }
    output
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct State {
    namespace: Vec<(Bytes, Bytes)>,
    catalog: CatalogImage,
    envelope_digest: [u8; 32],
    envelope_version: u64,
    name_mode: Option<serde_json::Value>,
}

impl State {
    async fn capture(fs: &ZeroFS, pool: &PoolObjectStore, authority: &Path) -> Result<Self> {
        let envelope = std::fs::read(authority.join("zerofs.key"))?;
        let wrapped: crate::key_management::WrappedDataKey =
            bincode::deserialize(&envelope).context("decode the production key envelope")?;
        let key = serde_json::to_value(&wrapped)?;
        Ok(Self {
            namespace: fs.db.scan(..).await?.try_collect().await?,
            catalog: survival_test_support::capture(pool).await?,
            envelope_digest: Sha256::digest(envelope).into(),
            envelope_version: u64::from(wrapped.version),
            name_mode: key.get("name_mode").cloned(),
        })
    }

    fn verify(&self, recovered: &Self, exact: bool) -> Result<()> {
        ensure!(
            self.namespace == recovered.namespace,
            "namespace rows or extent generations changed"
        );
        ensure!(
            self.catalog == recovered.catalog,
            "catalog identity, members, mappings, checksums, debts or capacity charges changed"
        );
        ensure!(
            self.envelope_digest == recovered.envelope_digest,
            "key envelope digest changed"
        );
        ensure!(
            self.envelope_version == recovered.envelope_version,
            "key envelope version changed"
        );
        ensure!(
            self.name_mode == recovered.name_mode,
            "key envelope name mode changed"
        );
        if exact {
            survival_test_support::require_nonzero_debts(&self.catalog)?;
            survival_test_support::require_nonzero_debts(&recovered.catalog)?;
            ensure!(
                self.name_mode.is_some(),
                "the committed key envelope must encode its name mode"
            );
        }
        Ok(())
    }
}

struct Live {
    fixture: Fixture,
    fs: Option<Arc<ZeroFS>>,
    pool: Option<PoolObjectStore>,
    expected: State,
    reads: Vec<ReadRange>,
}

impl Live {
    fn fs(&self) -> &Arc<ZeroFS> {
        self.fs.as_ref().expect("live writer")
    }
    fn pool(&self) -> &PoolObjectStore {
        self.pool.as_ref().expect("live pool")
    }

    async fn close_writer(&mut self) {
        if let Some(fs) = self.fs.take() {
            close(fs, self.pool.take().expect("writer pool")).await;
        }
    }

    async fn new(exact: bool) -> Result<Self> {
        let fixture = Fixture::new();
        let (fs, pool) = open(&fixture.settings, true).await?;
        let namespace = populate(&fs).await;
        let _ = fs.administrative_remote_durability().await?;
        verify_namespace(&fs, &namespace).await?;

        // Keep one current, small file on the first member. The larger current
        // movie and the other extra file remain on the second member.
        let mut files = vec![
            (
                namespace.file,
                vec!["/Media/movie.bin".to_owned(), "/alias".to_owned()],
                namespace.payload,
            ),
            (
                namespace.empty_file,
                vec!["/Media/empty".to_owned()],
                Bytes::new(),
            ),
        ];
        for (name, length, seed, member) in [
            (b"lost.bin".as_slice(), 4096, 5, "disk-a"),
            (b"safe.bin".as_slice(), 65536, 6, "disk-b"),
        ] {
            let auth = AuthContext::default();
            let creds = Credentials::from_auth_context(&auth);
            let (inode, _) = fs
                .create(&creds, 0, name, &SetAttributes::default())
                .await?;
            let data = payload(seed).slice(..length);
            fs.write(&auth, inode, 0, &data).await?;
            let _ = fs.administrative_remote_durability().await?;
            let loc = fs
                .db
                .get_bytes(&KeyCodec::new().extent_key(inode, 0))
                .await?
                .context("extra file extent")?;
            let loc = FrameLoc::decode(&loc).context("extra file frame")?;
            let placements = pool.placements().await?;
            let location = placements
                .iter()
                .find(|p| {
                    p.location
                        .as_ref()
                        .ends_with(&loc.segid.object_key().as_str())
                })
                .context("extra file placement")?
                .location
                .clone();
            pool.migrate(&location, member).await?;
            files.push((
                inode,
                vec![format!("/{}", std::str::from_utf8(name)?)],
                data,
            ));
        }
        let placements = pool.placements().await?;
        let mut reads = Vec::new();
        for (inode, names, data) in files {
            if data.is_empty() {
                reads.push(ReadRange {
                    names,
                    inode,
                    offset: 0,
                    length: 0,
                    member: None,
                    sha256: Sha256::digest([]).into(),
                });
                continue;
            }
            for offset in (0..data.len()).step_by(crate::fs::EXTENT_SIZE) {
                let length = (data.len() - offset).min(crate::fs::EXTENT_SIZE);
                let key =
                    KeyCodec::new().extent_key(inode, (offset / crate::fs::EXTENT_SIZE) as u64);
                let loc = fs
                    .db
                    .get_bytes(&key)
                    .await?
                    .context("current file extent")?;
                let loc = FrameLoc::decode(&loc).context("current frame location")?;
                let placement = placements
                    .iter()
                    .find(|p| {
                        p.location
                            .as_ref()
                            .ends_with(&loc.segid.object_key().as_str())
                    })
                    .context("current extent mapping")?;
                reads.push(ReadRange {
                    names: names.clone(),
                    inode,
                    offset: offset as u64,
                    length: length as u32,
                    member: Some(placement.member_id.clone()),
                    sha256: Sha256::digest(&data[offset..offset + length]).into(),
                });
            }
        }
        ensure!(
            reads.iter().any(|r| r.member.as_deref() == Some("disk-a")),
            "the lost member must own current file ranges"
        );
        ensure!(
            reads.iter().any(|r| r.member.as_deref() == Some("disk-b")),
            "the surviving member must own current file ranges"
        );
        if exact {
            survival_test_support::create_debts(&pool).await?;
        }
        let expected = State::capture(
            &fs,
            &pool,
            &fixture.root.path().join("authority-device/authority"),
        )
        .await?;
        if exact {
            survival_test_support::require_nonzero_debts(&expected.catalog)?;
        }
        Ok(Self {
            fixture,
            fs: Some(fs),
            pool: Some(pool),
            expected,
            reads,
        })
    }

    fn replica_roots(&self) -> [PathBuf; 2] {
        [
            self.fixture.root.path().join("authority-device/authority"),
            self.fixture
                .root
                .path()
                .join("disk-a-device/metadata-replica"),
        ]
    }

    fn proposed_document(&self) -> Result<toml::Value> {
        let config = self.fixture.root.path().join("fixture.toml");
        let mut document: toml::Value = toml::from_str(&std::fs::read_to_string(config)?)?;
        document["storage"]["pool"].as_table_mut().unwrap().insert("metadata_survival".to_owned(), toml::Value::try_from(serde_json::json!({
            "format_version": 1, "authority_replica_id": "authority", "authority_device_id": "fixture-authority-device",
            "recovery_mode": "normal", "reserve_free_bytes": 65536,
            "commit_interval_seconds": 60, "replica_timeout_seconds": REPLICA_TIMEOUT.as_secs(),
            "replicas": [{"id": "disk-a-metadata", "device_id": "fixture-disk-a-device", "dir": self.replica_roots()[1]}]
        }))?);
        Ok(document)
    }

    fn proposed_settings(&self) -> Result<Settings> {
        let document = self.proposed_document()?;
        let config = self.fixture.root.path().join("proposed.toml");
        std::fs::write(&config, toml::to_string(&document)?)?;
        Settings::from_file(config)
            .context("survival receipts and replica recovery cannot be configured")
    }
}

struct JournalLossReport {
    last_survival_commit: [u8; 32],
    mutation_cutoff: u64,
    local_only_suffix_lost: bool,
}

struct Recovered {
    fs: Arc<ZeroFS>,
    pool: PoolObjectStore,
    scratch: tempfile::TempDir,
    authority: PathBuf,
    commit_digest: [u8; 32],
    envelope_epoch: u64,
    lost_ranges: Vec<LostRange>,
    local_only_suffix: Option<JournalLossReport>,
}

// Hook results carry actual pending work. A successful no-op cannot provide
// the entered barrier or an unfinished production task.
struct PausedCommit {
    start: oneshot::Sender<()>,
    candidate: Arc<std::sync::Mutex<Option<SurvivalReceipt>>>,
    entered: oneshot::Receiver<CutEvent>,
    release: oneshot::Sender<()>,
    task: tokio::task::JoinHandle<Result<SurvivalReceipt>>,
}

struct PausedWrite {
    start: oneshot::Sender<()>,
    admitted: oneshot::Receiver<()>,
    release: oneshot::Sender<()>,
    task: tokio::task::JoinHandle<Result<()>>,
}

#[derive(Default)]
struct FaultObservation {
    prior_receipt: Option<SurvivalReceipt>,
    injected: Option<ReplicaFault>,
    elapsed: Duration,
    survival_refused: bool,
    writable_refused: bool,
    reported_failed: bool,
    second_writer_refused: bool,
    first_writer_still_owns_lock: bool,
    online_catch_up_refused: bool,
    offline_verified: bool,
    replacement_root: Option<PathBuf>,
}

struct PayloadRetentionObservation {
    range: ReadRange,
    old_object: PathBuf,
    old_object_digest: [u8; 32],
    successor_hash: [u8; 32],
    old_live_bytes: u64,
    old_total_bytes: u64,
    reclaim_passes: u64,
    reclaim_completed: bool,
}

#[derive(Default)]
struct Observation {
    receipt: Option<SurvivalReceipt>,
    recovered: Option<State>,
    recovered_digest: Option<[u8; 32]>,
    recovered_epoch: Option<u64>,
    reads: Vec<(ReadRange, Result<[u8; 32], FsError>)>,
    reported_lost: Vec<LostRange>,
    admitted_pending_at_loss: bool,
    cut: Option<Cut>,
    cut_object: Option<PathBuf>,
    journal_absent: bool,
    original_authority_absent: bool,
    original_catalog_absent: bool,
    expected_local_only_suffix: Option<(u64, u64)>,
    reported_local_only_suffix: Option<JournalLossReport>,
    read_only: bool,
    payload_retention: Option<PayloadRetentionObservation>,
    fault: FaultObservation,
}

#[async_trait]
trait Survival: Sync {
    async fn crash_writer(&self, _live: &mut Live) -> Result<()> {
        None.context("the production interrupted-writer shutdown hook is absent")
    }
    async fn commit(&self, live: &Live) -> Result<SurvivalReceipt>;
    async fn recover(&self, live: &Live, receipt: &SurvivalReceipt) -> Result<Option<Recovered>>;
    async fn pause_commit(&self, _live: &Live, _cut: Cut) -> Result<Option<PausedCommit>> {
        Ok(None)
    }
    async fn pause_write(&self, _live: &Live) -> Result<Option<PausedWrite>> {
        Ok(None)
    }
    async fn fault(&self, _live: &Live, _fault: ReplicaFault) -> Result<FaultObservation> {
        Ok(FaultObservation::default())
    }
}

struct CurrentImplementation;

#[async_trait]
impl Survival for CurrentImplementation {
    async fn commit(&self, live: &Live) -> Result<SurvivalReceipt> {
        let _settings = live.proposed_settings()?;
        // The future production binding must return the owner's real receipt.
        // Accepting the configuration alone supplies no such receipt.
        None.context("the production survival receipt hook is absent")
    }

    async fn recover(&self, _live: &Live, _receipt: &SurvivalReceipt) -> Result<Option<Recovered>> {
        None.context("the production offline recovery hook is absent")
    }
}

// This mutation deliberately accepts the proposed configuration and reports
// success without copying a byte or implementing any of the requested work.
struct StubSurvival;

impl StubSurvival {
    async fn observe(&self, row: Row, live: &Live) -> Result<Observation> {
        Ok(Observation {
            receipt: Some(self.commit(live).await?),
            fault: self
                .fault(live, row.fault().unwrap_or(ReplicaFault::Io))
                .await?,
            ..Default::default()
        })
    }
}

#[async_trait]
impl Survival for StubSurvival {
    async fn commit(&self, live: &Live) -> Result<SurvivalReceipt> {
        let _accepted_configuration = live.proposed_document()?;
        Ok(SurvivalReceipt {
            relative_record: "commits/1/1.json".into(),
            digest: Sha256::digest(b"stub success").into(),
            mutation_cutoff: live.fs().capture_mutation_cutoff().sequence,
            envelope_epoch: 1,
        })
    }
    async fn recover(&self, _live: &Live, _receipt: &SurvivalReceipt) -> Result<Option<Recovered>> {
        Ok(None)
    }
}

fn verify_replica_record(live: &Live, receipt: &SurvivalReceipt) -> Result<()> {
    ensure!(
        receipt
            .relative_record
            .components()
            .all(|p| matches!(p, Component::Normal(_))),
        "unsafe receipt path"
    );
    ensure!(
        receipt.relative_record.starts_with("commits"),
        "receipt must name a commit"
    );
    let roots = live.replica_roots();
    ensure!(
        roots.iter().any(|root| root.exists()),
        "no metadata replica survived"
    );
    for replica in roots.iter().filter(|root| root.exists()) {
        let record = replica
            .join(".zerofs-survival/v1")
            .join(&receipt.relative_record);
        let bytes = std::fs::read(&record)
            .with_context(|| format!("receipt commit is absent on {}", replica.display()))?;
        ensure!(
            <[u8; 32]>::from(Sha256::digest(bytes)) == receipt.digest,
            "receipt commit digest mismatch"
        );
    }
    Ok(())
}

fn fault_oracle(row: Row, live: &Live, observed: &Observation) -> Result<()> {
    let fault = row.fault().context("fault row")?;
    let actual = &observed.fault;
    ensure!(
        actual.injected == Some(fault),
        "{fault:?} replica hook is absent or did not inject the requested fault"
    );
    match fault {
        ReplicaFault::Slow => {
            ensure!(
                actual.elapsed >= REPLICA_TIMEOUT,
                "the slow replica did not cross the 30-second timeout"
            );
            ensure!(
                actual.survival_refused && actual.reported_failed,
                "the timed-out replica must stop survival acknowledgements and be reported failed"
            );
        }
        ReplicaFault::Full | ReplicaFault::Stale | ReplicaFault::Io => {
            ensure!(
                actual.survival_refused && actual.reported_failed,
                "the failed replica must stop survival acknowledgements"
            );
        }
        ReplicaFault::SecondWriter => {
            ensure!(
                actual.second_writer_refused && actual.first_writer_still_owns_lock,
                "the second writer must be refused while the first retains every replica lock"
            );
            for root in live.replica_roots() {
                let lock = std::fs::OpenOptions::new()
                    .read(true)
                    .write(true)
                    .open(root.join(".zerofs-survival/v1/writer.lock"))?;
                ensure!(
                    !fs4::fs_std::FileExt::try_lock_exclusive(&lock)?,
                    "the first writer does not hold every replica lock"
                );
            }
        }
        ReplicaFault::Replacement => {
            ensure!(
                actual.online_catch_up_refused && actual.offline_verified,
                "replacement catch-up must be refused online and verified offline"
            );
            let source = live.replica_roots()[0].join(".zerofs-survival/v1");
            let mut expected = tree_hashes(&source)?;
            let destination = actual
                .replacement_root
                .as_ref()
                .context("no offline replacement directory")?;
            ensure!(
                destination.starts_with(live.fixture.root.path())
                    && !destination.starts_with(&live.replica_roots()[0]),
                "replacement must use its owned replica directory"
            );
            let mut replacement = tree_hashes(destination)?;
            // Replica bindings differ. The retained generations must not.
            for path in ["identity.json", "writer.lock"] {
                expected.remove(Path::new(path));
                replacement.remove(Path::new(path));
            }
            ensure!(
                !expected.is_empty() && expected == replacement,
                "replacement must contain the exact verified generation bytes"
            );
            let source_identity: serde_json::Value =
                serde_json::from_slice(&std::fs::read(source.join("identity.json"))?)?;
            let replacement_identity: serde_json::Value =
                serde_json::from_slice(&std::fs::read(destination.join("identity.json"))?)?;
            for field in [
                "format_version",
                "pool_id",
                "membership_digest",
                "membership_epoch",
            ] {
                ensure!(
                    source_identity.get(field).is_some()
                        && source_identity.get(field) == replacement_identity.get(field),
                    "replacement identity changed {field}"
                );
            }
            ensure!(
                replacement_identity["replica_id"] == "disk-a-metadata"
                    && replacement_identity["device_id"] == REPLACEMENT_DEVICE_ID,
                "replacement identity has the wrong replica or device binding"
            );
        }
    }
    if !matches!(
        fault,
        ReplicaFault::SecondWriter | ReplicaFault::Replacement
    ) {
        ensure!(
            actual.writable_refused,
            "the failed metadata replica must leave the share read-only"
        );
    }
    verify_replica_record(
        live,
        observed
            .receipt
            .as_ref()
            .context("no prior survival receipt for the fault case")?,
    )?;
    Ok(())
}

fn oracle(row: Row, live: &Live, observed: &Observation) -> Result<()> {
    if row.fault().is_some() {
        return fault_oracle(row, live, observed);
    }
    match row {
        Row::PayloadRetention => payload_retention_oracle(
            live,
            observed
                .payload_retention
                .as_ref()
                .context("no overwrite and reclaim evidence")?,
        )?,
        Row::AdmittedWriteLoss => ensure!(
            observed.admitted_pending_at_loss,
            "no admitted write was blocked mid-commit at device loss"
        ),
        Row::InterruptedCommit => {
            ensure!(
                observed.cut.is_some(),
                "the production commit cut hook is absent"
            );
            ensure!(
                observed
                    .cut_object
                    .as_deref()
                    .is_some_and(valid_object_path),
                "the production cut did not identify its affected object"
            );
        }
        Row::JournalBeforeBackend | Row::JournalAfterBackend => {
            ensure!(observed.journal_absent, "the journal device was not lost");
            let expected = observed
                .expected_local_only_suffix
                .context("the real journal loss boundary was not captured")?;
            let report = observed
                .reported_local_only_suffix
                .as_ref()
                .context("recovery did not report local-only suffix loss")?;
            let receipt = observed
                .receipt
                .as_ref()
                .context("journal recovery needs a named receipt")?;
            ensure!(
                report.last_survival_commit == receipt.digest
                    && report.mutation_cutoff == receipt.mutation_cutoff
                    && report.local_only_suffix_lost,
                "journal recovery must identify the last survival commit and warn that its later local-only suffix was lost"
            );
            if row == Row::JournalBeforeBackend {
                ensure!(
                    expected.1 > expected.0,
                    "the paused journal must have a nonempty local-only suffix"
                );
            }
        }
        _ => {}
    }
    ensure!(
        observed.original_authority_absent && observed.original_catalog_absent,
        "recovery must use only receipt-covered replicas after removing the original authority and catalog"
    );
    ensure!(
        observed.read_only,
        "offline recovery must refuse mutations with EROFS"
    );
    let receipt = observed
        .receipt
        .as_ref()
        .context("no named survival receipt")?;
    verify_replica_record(live, receipt)?;
    ensure!(
        observed.recovered_digest == Some(receipt.digest),
        "recovery did not select the receipt commit"
    );
    ensure!(
        observed.recovered_epoch == Some(receipt.envelope_epoch) && receipt.envelope_epoch > 0,
        "committed key envelope epoch changed or is absent"
    );
    live.expected.verify(
        observed
            .recovered
            .as_ref()
            .context("cold recovered state is absent")?,
        row == Row::ExactRecovery,
    )?;
    read_oracle(row, live, observed)
}

fn payload_retention_oracle(live: &Live, observed: &PayloadRetentionObservation) -> Result<()> {
    ensure!(
        live.reads.contains(&observed.range) && observed.successor_hash != observed.range.sha256,
        "the receipt's file must have a different live successor"
    );
    ensure!(
        observed.old_live_bytes == 0 && observed.old_total_bytes > 0,
        "the receipt's old segment must be reclaimable by the live namespace"
    );
    ensure!(
        observed.reclaim_completed && observed.reclaim_passes > 0,
        "a completed reclaim pass must actually run after the overwrite"
    );
    let bytes = std::fs::read(&observed.old_object)
        .context("reclaim deleted payload still needed by the survival receipt")?;
    ensure!(
        <[u8; 32]>::from(Sha256::digest(bytes)) == observed.old_object_digest,
        "the retained physical payload generation changed"
    );
    Ok(())
}

async fn overwrite_and_reclaim(live: &Live) -> Result<PayloadRetentionObservation> {
    let range = live
        .reads
        .iter()
        .find(|range| range.names.iter().any(|name| name == "/lost.bin"))
        .context("the isolated payload-retention file")?
        .clone();
    let key = KeyCodec::new().extent_key(range.inode, 0);
    let old = live.fs().db.get_bytes(&key).await?.context("old extent")?;
    let old = FrameLoc::decode(&old).context("old frame location")?;
    let placement = live
        .pool()
        .placements()
        .await?
        .into_iter()
        .find(|p| {
            p.location
                .as_ref()
                .ends_with(old.segid.object_key().as_str())
        })
        .context("old physical generation")?;
    let old_object = live
        .fixture
        .root
        .path()
        .join(format!("{}-device", placement.member_id))
        .join("data")
        .join(placement.physical_key);
    let old_object_digest = Sha256::digest(std::fs::read(&old_object)?).into();
    let successor = payload(101).slice(..range.length as usize);
    live.fs()
        .write(&AuthContext::default(), range.inode, 0, &successor)
        .await?;
    let _ = live.fs().administrative_remote_durability().await?;
    let current = live
        .fs()
        .db
        .get_bytes(&key)
        .await?
        .context("successor extent")?;
    let current = FrameLoc::decode(&current).context("successor frame location")?;
    ensure!(
        current.segid != old.segid,
        "the overwrite reused the old segment"
    );
    let (bytes, _) = live
        .fs()
        .read_file(&AuthContext::default(), range.inode, 0, range.length)
        .await?;
    ensure!(bytes == successor, "the live successor did not publish");
    let counter = live
        .fs()
        .db
        .get_bytes(&KeyCodec::new().segcount_key(old.segid.epoch, old.segid.counter))
        .await?
        .context("the old segment counter")?;
    let (old_live_bytes, old_total_bytes) =
        KeyCodec::decode_segcount(&counter).context("the old segment byte counts")?;
    ensure!(
        old_live_bytes == 0 && old_total_bytes > 0,
        "the fixture must make the receipt's segment fully reclaimable"
    );
    let stats = live.fs().extent_store.segment_gc_stats();
    let before = stats.passes.load(std::sync::atomic::Ordering::Relaxed);
    // The live export pin must already have transferred to the ledger. Run a
    // full pass so a permanent-checkpoint skip cannot satisfy this contract.
    let pass = live
        .fs()
        .extent_store
        .reclaim_segments_gated(
            || std::future::ready(Ok(Some((chrono::Utc::now(), None)))),
            None,
            Duration::ZERO,
            |_| false,
            64 * 1024 * 1024,
        )
        .await?;
    let after = stats.passes.load(std::sync::atomic::Ordering::Relaxed);
    Ok(PayloadRetentionObservation {
        range,
        old_object,
        old_object_digest,
        successor_hash: Sha256::digest(successor).into(),
        old_live_bytes,
        old_total_bytes,
        reclaim_passes: after.saturating_sub(before),
        reclaim_completed: matches!(pass.status, crate::fs::store::PassStatus::Completed { .. }),
    })
}

fn read_oracle(row: Row, live: &Live, observed: &Observation) -> Result<()> {
    let lost_member = if row == Row::DataAndReplicaLoss {
        Some("disk-a")
    } else {
        observed.cut.and_then(|cut| match cut.lost {
            Device::DiskA => Some("disk-a"),
            Device::DiskB => Some("disk-b"),
            _ => None,
        })
    };
    let expected_lost: Vec<_> = live
        .reads
        .iter()
        .filter(|range| lost_member.is_some() && range.member.as_deref() == lost_member)
        .cloned()
        .collect();
    if row == Row::DataAndReplicaLoss {
        ensure!(
            !expected_lost.is_empty(),
            "the lost device must contain sole-copy current payload"
        );
    }
    ensure!(
        canonical_lost(observed.reported_lost.clone())
            == canonical_lost(expected_lost.iter().map(ReadRange::lost_range).collect()),
        "lost file names or byte ranges differ from the current extent map"
    );
    ensure!(
        observed.reads.len() == live.reads.len(),
        "every current file range must be read after recovery"
    );
    for (expected, (range, actual)) in live.reads.iter().zip(&observed.reads) {
        ensure!(expected == range, "the recovery read plan changed");
        if expected_lost.contains(range) {
            ensure!(
                *actual == Err(FsError::IoError),
                "lost range {:?} must return EIO",
                range.names
            );
        } else {
            ensure!(
                *actual == Ok(range.sha256),
                "unaffected range {:?} changed or returned an error",
                range.names
            );
        }
    }
    Ok(())
}

fn tree_hashes(root: &Path) -> Result<BTreeMap<PathBuf, [u8; 32]>> {
    fn visit(root: &Path, dir: &Path, output: &mut BTreeMap<PathBuf, [u8; 32]>) -> Result<()> {
        for entry in std::fs::read_dir(dir)? {
            let entry = entry?;
            if entry.file_type()?.is_dir() {
                visit(root, &entry.path(), output)?;
            } else {
                output.insert(
                    entry.path().strip_prefix(root)?.to_owned(),
                    Sha256::digest(std::fs::read(entry.path())?).into(),
                );
            }
        }
        Ok(())
    }
    let mut output = BTreeMap::new();
    if root.exists() {
        visit(root, root, &mut output)?;
    }
    Ok(output)
}

async fn collect_recovery(
    live: &Live,
    driver: &dyn Survival,
    receipt: &SurvivalReceipt,
    observed: &mut Observation,
) -> Result<()> {
    let sources: Vec<_> = live
        .replica_roots()
        .iter()
        .map(|root| tree_hashes(&root.join(".zerofs-survival/v1")))
        .collect::<Result<_>>()?;
    if let Some(recovered) = driver.recover(live, receipt).await? {
        let result = async {
            ensure!(
                recovered.authority.starts_with(recovered.scratch.path()),
                "recovery authority must be owned scratch state"
            );
            observed.recovered =
                Some(State::capture(&recovered.fs, &recovered.pool, &recovered.authority).await?);
            observed.recovered_digest = Some(recovered.commit_digest);
            observed.recovered_epoch = Some(recovered.envelope_epoch);
            observed.reported_lost = recovered.lost_ranges;
            observed.reported_local_only_suffix = recovered.local_only_suffix;
            let auth = AuthContext::default();
            let creds = Credentials::from_auth_context(&auth);
            observed.read_only = matches!(
                recovered
                    .fs
                    .create(&creds, 0, b"must-not-create", &SetAttributes::default())
                    .await,
                Err(FsError::ReadOnlyFilesystem)
            );
            for range in &live.reads {
                let read = recovered
                    .fs
                    .read_file(
                        &AuthContext::default(),
                        range.inode,
                        range.offset,
                        range.length.max(1),
                    )
                    .await;
                let actual = read.map(|(bytes, _)| Sha256::digest(bytes).into());
                observed.reads.push((range.clone(), actual));
            }
            Ok(())
        }
        .await;
        close(recovered.fs, recovered.pool).await;
        recovered.scratch.close()?;
        result?;
    }
    let after: Vec<_> = live
        .replica_roots()
        .iter()
        .map(|root| tree_hashes(&root.join(".zerofs-survival/v1")))
        .collect::<Result<_>>()?;
    ensure!(
        sources == after,
        "offline recovery modified surviving source evidence"
    );
    Ok(())
}

fn lose_device(live: &Live, lost: Device) -> Result<()> {
    let device = live.fixture.root.path().join(lost.directory());
    if !device.exists() {
        std::fs::create_dir_all(&device)?;
    }
    std::fs::remove_dir_all(device)?;
    ensure!(
        !live.fixture.root.path().join(lost.directory()).exists(),
        "lost device was recreated"
    );
    Ok(())
}

fn remove_active_originals(live: &Live, observed: &mut Observation) -> Result<()> {
    // Erase live authority objects and the catalog. Keep the authority's
    // survival directory if the other metadata replica was on the lost disk.
    let authority_device = live.fixture.root.path().join("authority-device");
    let authority = authority_device.join("authority");
    if authority.exists() {
        for entry in std::fs::read_dir(&authority)? {
            let entry = entry?;
            if entry.file_name() == ".zerofs-survival" {
                continue;
            }
            if entry.file_type()?.is_dir() {
                std::fs::remove_dir_all(entry.path())?;
            } else {
                std::fs::remove_file(entry.path())?;
            }
        }
    }
    if authority_device.join("catalog").exists() {
        std::fs::remove_dir_all(authority_device.join("catalog"))?;
    }
    observed.journal_absent = !live.fixture.root.path().join("journal-device").exists();
    observed.original_authority_absent =
        !authority.join("zerofs.key").exists() && !authority.join("db").exists();
    observed.original_catalog_absent = !authority_device.join("catalog").exists();
    Ok(())
}

async fn recover_after_loss(
    row: Row,
    live: &mut Live,
    driver: &dyn Survival,
    receipt: SurvivalReceipt,
    mut observed: Observation,
) -> Result<()> {
    remove_active_originals(live, &mut observed)?;
    collect_recovery(live, driver, &receipt, &mut observed).await?;
    observed.receipt = Some(receipt);
    oracle(row, live, &observed)
}

async fn lose_and_recover(
    row: Row,
    live: &mut Live,
    driver: &dyn Survival,
    receipt: SurvivalReceipt,
    lost: Device,
    observed: Observation,
) -> Result<()> {
    verify_replica_record(live, &receipt)?;
    lose_device(live, lost)?;
    if matches!(row, Row::JournalBeforeBackend | Row::JournalAfterBackend) {
        lose_device(live, Device::Authority)?;
    }
    recover_after_loss(row, live, driver, receipt, observed).await
}

async fn journal_case(row: Row, live: &mut Live, driver: &dyn Survival) -> Result<()> {
    let mut receipt = driver
        .commit(live)
        .await
        .context("a journal-loss row requires a named survival commit before its suffix")?;
    ensure!(
        receipt.mutation_cutoff == live.fs().capture_mutation_cutoff().sequence,
        "the receipt does not cover the captured prefix"
    );
    verify_replica_record(live, &receipt)?;
    live.expected = State::capture(live.fs(), live.pool(), &live.replica_roots()[0]).await?;
    // Close the original writer before attaching a paused writeback journal.
    live.close_writer().await;
    // These handles are replaced by the writeback-backed writer below. The
    // contract binding must release the original writer before reopening.
    let journal = live
        .fixture
        .root
        .path()
        .canonicalize()?
        .join("journal-device/journal");
    let (fs, pool, writeback) =
        open_with_writeback(&live.fixture.settings, false, Some(journal)).await?;
    live.fs = Some(fs);
    live.pool = Some(pool);
    let writeback = writeback.context("real writeback overlay")?;
    let result = async {
        let file = live.reads[0].inode;
        let suffix = payload(77).slice(..FINAL_SIZE);
        live.fs()
            .write(&AuthContext::default(), file, 0, &suffix)
            .await?;
        let local_receipt = live
            .fs()
            .flush_coordinator
            .durable_through(
                live.fs().capture_mutation_cutoff(),
                DurabilityTarget::LocalSsd,
            )
            .await?;
        ensure!(
            local_receipt.target == DurabilityTarget::LocalSsd
                && matches!(
                    local_receipt.object_coverage,
                    ObjectCoverage::Writeback { .. }
                ),
            "the suffix needs a real writeback SSD receipt"
        );
        let local = writeback.status()?;
        ensure!(
            local.local_seq > local.remote_seq && local.remote_seq == 0,
            "the suffix must be SSD durable with remote replay paused"
        );
        if row == Row::JournalAfterBackend {
            writeback.activate_remote()?;
            let backend = live.fs().administrative_remote_durability().await?;
            ensure!(backend.target == DurabilityTarget::RemoteBackend);
            receipt = driver.commit(live).await?;
            for range in live.reads.iter_mut().filter(|r| r.inode == file) {
                let start = range.offset as usize;
                range.sha256 = Sha256::digest(&suffix[start..start + range.length as usize]).into();
            }
            live.expected =
                State::capture(live.fs(), live.pool(), &live.replica_roots()[0]).await?;
        }
        ensure!(
            receipt.mutation_cutoff <= live.fs().capture_mutation_cutoff().sequence,
            "invalid receipt cutoff"
        );
        Ok(())
    }
    .await;
    live.close_writer().await;
    if row == Row::JournalAfterBackend {
        writeback.wait_remote(writeback.accepted_sequence()).await?;
    }
    let status = writeback.status()?;
    writeback.shutdown().await?;
    result?;
    lose_and_recover(
        row,
        live,
        driver,
        receipt,
        Device::Journal,
        Observation {
            expected_local_only_suffix: Some((status.remote_seq, status.local_seq)),
            ..Default::default()
        },
    )
    .await
}

async fn one_case(row: Row, live: &mut Live, driver: &dyn Survival) -> Result<()> {
    if let Some(fault) = row.fault() {
        let mut fault = driver.fault(live, fault).await?;
        let observed = Observation {
            receipt: fault.prior_receipt.take(),
            fault,
            ..Default::default()
        };
        return oracle(row, live, &observed);
    }
    if matches!(row, Row::JournalBeforeBackend | Row::JournalAfterBackend) {
        return journal_case(row, live, driver).await;
    }
    if row == Row::PayloadRetention {
        let receipt = driver.commit(live).await?;
        verify_replica_record(live, &receipt)?;
        live.expected = State::capture(live.fs(), live.pool(), &live.replica_roots()[0]).await?;
        let retained = overwrite_and_reclaim(live).await?;
        payload_retention_oracle(live, &retained)?;
        live.close_writer().await;
        return lose_and_recover(
            row,
            live,
            driver,
            receipt,
            Device::Authority,
            Observation {
                payload_retention: Some(retained),
                ..Default::default()
            },
        )
        .await;
    }
    if row == Row::AdmittedWriteLoss {
        let pause = driver
            .pause_write(live)
            .await?
            .context("the admitted-write mid-commit hook is absent")?;
        let result = async {
            let receipt = driver.commit(live).await?;
            verify_replica_record(live, &receipt)?;
            live.expected =
                State::capture(live.fs(), live.pool(), &live.replica_roots()[0]).await?;
            pause
                .start
                .send(())
                .map_err(|_| anyhow::anyhow!("the write start barrier closed"))?;
            tokio::time::timeout(Duration::from_secs(5), pause.admitted).await??;
            ensure!(
                !pause.task.is_finished(),
                "the admitted write completed before device loss"
            );
            lose_device(live, Device::Authority)?;
            Ok(receipt)
        }
        .await;
        drop(pause.release);
        pause.task.abort();
        let _ = pause.task.await;
        let receipt = result?;
        driver.crash_writer(live).await?;
        return recover_after_loss(
            row,
            live,
            driver,
            receipt,
            Observation {
                admitted_pending_at_loss: true,
                ..Default::default()
            },
        )
        .await;
    }
    let receipt = driver.commit(live).await?;
    ensure!(
        receipt.mutation_cutoff == live.fs().capture_mutation_cutoff().sequence,
        "the receipt must cover exactly the captured namespace"
    );
    let captured = State::capture(live.fs(), live.pool(), &live.replica_roots()[0]).await?;
    ensure!(
        live.expected.namespace == captured.namespace,
        "the survival commit changed the quiescent namespace"
    );
    live.expected = captured;
    let lost = if row == Row::DataAndReplicaLoss {
        Device::DiskA
    } else {
        Device::Authority
    };
    live.close_writer().await;
    lose_and_recover(row, live, driver, receipt, lost, Observation::default()).await
}

async fn interrupted_case(
    live: &mut Live,
    driver: &dyn Survival,
    cut: Cut,
    pause: PausedCommit,
) -> Result<()> {
    let candidate_slot = Arc::clone(&pause.candidate);
    let result: Result<(CutEvent, SurvivalReceipt, State, Vec<ReadRange>)> = async {
        let prior = driver
            .commit(live)
            .await
            .context("the prior receipt must precede the interrupted successor")?;
        verify_replica_record(live, &prior)?;
        let prior_state = State::capture(live.fs(), live.pool(), &live.replica_roots()[0]).await?;
        let prior_reads = live.reads.clone();
        // Change a current file so a mixture of predecessor and candidate state
        // cannot satisfy either complete-generation oracle.
        let file = live.reads[0].inode;
        let data = payload(88).slice(..FINAL_SIZE);
        live.fs()
            .write(&AuthContext::default(), file, 0, &data)
            .await?;
        let _ = live.fs().administrative_remote_durability().await?;
        live.expected = State::capture(live.fs(), live.pool(), &live.replica_roots()[0]).await?;
        let placements = live.pool().placements().await?;
        let mut current_reads = live.reads.clone();
        for range in current_reads.iter_mut().filter(|r| r.inode == file) {
            let loc = live
                .fs()
                .db
                .get_bytes(
                    &KeyCodec::new().extent_key(file, range.offset / crate::fs::EXTENT_SIZE as u64),
                )
                .await?
                .context("candidate extent")?;
            let loc = FrameLoc::decode(&loc).context("candidate frame")?;
            range.member = Some(
                placements
                    .iter()
                    .find(|p| {
                        p.location
                            .as_ref()
                            .ends_with(loc.segid.object_key().as_str())
                    })
                    .context("candidate mapping")?
                    .member_id
                    .clone(),
            );
        }
        live.reads = current_reads;
        for range in live.reads.iter_mut().filter(|r| r.inode == file) {
            range.sha256 = Sha256::digest(
                &data[range.offset as usize..range.offset as usize + range.length as usize],
            )
            .into();
        }
        pause
            .start
            .send(())
            .map_err(|_| anyhow::anyhow!("the commit start barrier closed"))?;
        let reached = tokio::time::timeout(Duration::from_secs(5), pause.entered).await??;
        ensure!(
            reached.cut == cut && valid_object_path(&reached.object) && !pause.task.is_finished(),
            "the requested cut did not block the real commit task"
        );
        println!(
            "METADATA_SURVIVAL_OBSERVED_CUT {}",
            serde_json::json!({
                "point": format!("{:?}", reached.cut.point),
                "side": format!("{:?}", reached.cut.side),
                "replica": reached.cut.replica,
                "object": reached.object,
                "lost": format!("{:?}", reached.cut.lost),
            })
        );
        lose_device(live, cut.lost)?;
        Ok((reached, prior, prior_state, prior_reads))
    }
    .await;
    drop(pause.release);
    pause.task.abort();
    let _ = pause.task.await;
    let (reached, prior, prior_state, prior_reads) = result?;
    driver.crash_writer(live).await?;
    let mut observed = Observation {
        cut: Some(reached.cut),
        cut_object: Some(reached.object),
        ..Default::default()
    };
    // A complete unacknowledged candidate is legal after record publication.
    // The driver may name it only when its real hook has built that record.
    let candidate = candidate_slot.lock().unwrap().clone();
    let probe = candidate.as_ref().unwrap_or(&prior);
    remove_active_originals(live, &mut observed)?;
    collect_recovery(live, driver, probe, &mut observed).await?;
    let receipt = if observed.recovered_digest == Some(prior.digest) {
        live.expected = prior_state;
        live.reads = prior_reads;
        prior
    } else {
        candidate.context("recovery selected a generation with no complete candidate record")?
    };
    observed.receipt = Some(receipt);
    oracle(Row::InterruptedCommit, live, &observed)
}

async fn finish(mut live: Live, row: Row, result: Result<()>) -> Result<()> {
    let error = result.as_ref().err().map(|error| format!("{error:#}"));
    live.close_writer().await;
    let root = live.fixture.root.path().to_owned();
    live.fixture.root.close()?;
    ensure!(!root.exists(), "acceptance fixture survived cleanup");
    println!(
        "METADATA_SURVIVAL_ACCEPTANCE {}",
        serde_json::json!({"row": format!("{row:?}"), "error": error, "cleanup": "passed", "fixture": root})
    );
    result
}

async fn acceptance(row: Row) -> Result<()> {
    if row == Row::InterruptedCommit {
        let mut failures = Vec::new();
        let mut idle_fixture = Some(Live::new(false).await?);
        for cut in cuts() {
            let mut live = match idle_fixture.take() {
                Some(live) => live,
                None => Live::new(false).await?,
            };
            let result = match CurrentImplementation.pause_commit(&live, cut).await? {
                Some(pause) => {
                    let result =
                        interrupted_case(&mut live, &CurrentImplementation, cut, pause).await;
                    // A fixture that exercised a cut is never reused.
                    finish(live, row, result).await
                }
                None => {
                    idle_fixture = Some(live);
                    Err(anyhow::anyhow!(
                        "the production commit cut hook is absent for {cut:?}"
                    ))
                }
            };
            if let Err(error) = result {
                println!("METADATA_SURVIVAL_COMMIT_CUT {cut:?} {error:#}");
                failures.push(format!("{cut:?}. {error:#}"));
            }
        }
        let result = if let Some(first) = failures.first() {
            Err(anyhow::anyhow!(
                "{} commit cuts failed. {first}",
                failures.len()
            ))
        } else {
            Ok(())
        };
        if let Some(live) = idle_fixture {
            finish(live, row, Ok(())).await?;
        }
        println!(
            "METADATA_SURVIVAL_ACCEPTANCE {}",
            serde_json::json!({"row": "InterruptedCommit", "requested_cuts": cuts().len(), "failed_cuts": failures.len(), "cleanup": "passed"})
        );
        return result;
    }
    let mut live = Live::new(row == Row::ExactRecovery).await?;
    let result = one_case(row, &mut live, &CurrentImplementation).await;
    finish(live, row, result).await
}

macro_rules! acceptance_row {
    ($name:ident, $row:ident, $reason:literal) => {
        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        #[ignore = $reason]
        async fn $name() {
            tokio::time::timeout(
                Duration::from_secs(if Row::$row == Row::InterruptedCommit {
                    5400
                } else {
                    180
                }),
                acceptance(Row::$row),
            )
            .await
            .expect("acceptance exceeded its deadline")
            .expect($reason);
        }
    };
}

acceptance_row!(
    authority_device_loss_preserves_committed_namespace,
    AuthorityLoss,
    "Named survival receipts and offline authority recovery are missing"
);
acceptance_row!(
    journal_device_loss_before_backend_cutoff_preserves_committed_prefix,
    JournalBeforeBackend,
    "A receipt-covered recovery prefix independent of the journal is missing"
);
acceptance_row!(
    journal_device_loss_after_backend_cutoff_preserves_committed_prefix,
    JournalAfterBackend,
    "A survival commit covering the drained suffix is missing"
);
acceptance_row!(
    data_member_with_metadata_replica_loss_preserves_namespace_and_reports_ranges,
    DataAndReplicaLoss,
    "Metadata replica recovery and exact sole-copy range reporting are missing"
);
acceptance_row!(
    reclaim_preserves_payload_referenced_by_the_last_survival_commit,
    PayloadRetention,
    "Survival receipts and payload-retention enforcement are missing"
);
acceptance_row!(
    writes_in_flight_during_loss_preserve_last_survival_commit,
    AdmittedWriteLoss,
    "The admitted-write mid-commit hook is absent"
);
acceptance_row!(
    interrupted_commit_at_every_step_preserves_a_consistent_generation,
    InterruptedCommit,
    "The production commit cut hook is absent"
);
acceptance_row!(
    recovery_preserves_exact_catalog_identity_cleanup_debts_and_key_epoch,
    ExactRecovery,
    "Replicated catalog exports and committed envelope epochs are missing"
);
acceptance_row!(
    slow_replica_past_timeout_stops_survival_acknowledgements,
    SlowReplica,
    "The Slow replica hook is absent"
);
acceptance_row!(
    full_replica_stops_survival_acknowledgements,
    FullReplica,
    "The Full replica hook is absent"
);
acceptance_row!(
    stale_replica_stops_survival_acknowledgements,
    StaleReplica,
    "The Stale replica hook is absent"
);
acceptance_row!(
    erroring_replica_stops_survival_acknowledgements,
    ErroringReplica,
    "The Io replica hook is absent"
);
acceptance_row!(
    competing_writer_is_refused_by_every_replica_lock,
    CompetingWriter,
    "The SecondWriter replica hook is absent"
);
acceptance_row!(
    replaced_replica_requires_verified_offline_catch_up,
    ReplacementCatchUp,
    "The Replacement replica hook is absent"
);

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn every_acceptance_oracle_rejects_success_without_survival() -> Result<()> {
    let live = Live::new(true).await?;
    let receipt = StubSurvival.commit(&live).await?;
    ensure!(StubSurvival.recover(&live, &receipt).await?.is_none());
    let mut rejected = 0;
    for row in Row::ALL {
        // Feed successful stub output directly to the same oracle used by the
        // ignored row. Configuration rejection cannot make this mutation pass.
        let observed = StubSurvival.observe(row, &live).await?;
        let error = oracle(row, &live, &observed)
            .expect_err("a successful no-op passed an acceptance oracle");

        let optimistic = Observation {
            receipt: Some(receipt.clone()),
            recovered: Some(live.expected.clone()),
            recovered_digest: Some(receipt.digest),
            recovered_epoch: Some(receipt.envelope_epoch),
            reads: live
                .reads
                .iter()
                .map(|range| (range.clone(), Ok(range.sha256)))
                .collect(),
            admitted_pending_at_loss: true,
            cut: Some(cuts()[0]),
            cut_object: Some("objects/stub".into()),
            journal_absent: true,
            original_authority_absent: true,
            original_catalog_absent: true,
            expected_local_only_suffix: Some((0, 1)),
            reported_local_only_suffix: Some(JournalLossReport {
                last_survival_commit: receipt.digest,
                mutation_cutoff: receipt.mutation_cutoff,
                local_only_suffix_lost: true,
            }),
            read_only: true,
            fault: FaultObservation {
                injected: row.fault(),
                elapsed: REPLICA_TIMEOUT,
                survival_refused: true,
                writable_refused: true,
                reported_failed: true,
                second_writer_refused: true,
                first_writer_still_owns_lock: true,
                online_catch_up_refused: true,
                offline_verified: true,
                ..Default::default()
            },
            ..Default::default()
        };
        let optimistic_error = oracle(row, &live, &optimistic)
            .expect_err("optimistic stub flags passed without replica bytes");
        println!(
            "METADATA_SURVIVAL_STUB_REJECTED {row:?} success-only={error:#}; optimistic={optimistic_error:#}"
        );
        rejected += 1;
    }
    ensure!(
        rejected == Row::ALL.len(),
        "the stub did not reach every row oracle"
    );
    // Also prove that accepting a named receipt cannot skip physical evidence.
    ensure!(
        verify_replica_record(&live, &receipt).is_err(),
        "the stub manufactured a durable commit"
    );
    finish(live, Row::ExactRecovery, Ok(())).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn exact_state_oracle_rejects_catalog_and_envelope_mutations() -> Result<()> {
    let live = Live::new(true).await?;
    let mut expected = live.expected.clone();
    // This positive control exercises the comparison contract. It creates no
    // replica or recovery evidence and cannot satisfy an acceptance row.
    expected.name_mode = Some(serde_json::json!("sensitive"));
    expected.verify(&expected, true)?;
    let mut checked = 0;
    for name in expected.catalog.keys() {
        let mut damaged = expected.clone();
        damaged.catalog.remove(name);
        ensure!(
            expected.verify(&damaged, true).is_err(),
            "dropped table {name} passed"
        );
        checked += 1;
        for key in expected.catalog[name].keys() {
            let mut damaged = expected.clone();
            damaged
                .catalog
                .get_mut(name)
                .unwrap()
                .get_mut(key)
                .unwrap()
                .push(0);
            ensure!(
                expected.verify(&damaged, true).is_err(),
                "changed {name}/{key} passed"
            );
            checked += 1;
        }
    }
    for field in ["namespace", "digest", "version", "mode"] {
        let mut damaged = expected.clone();
        match field {
            "namespace" => {
                damaged.namespace.pop();
            }
            "digest" => {
                damaged.envelope_digest[0] ^= 1;
            }
            "version" => {
                damaged.envelope_version += 1;
            }
            "mode" => {
                damaged.name_mode = None;
            }
            _ => unreachable!(),
        }
        ensure!(
            expected.verify(&damaged, true).is_err(),
            "changed {field} passed"
        );
        checked += 1;
    }
    let lost: Vec<_> = live
        .reads
        .iter()
        .filter(|r| r.member.as_deref() == Some("disk-a"))
        .cloned()
        .collect();
    let mut observed = Observation {
        reported_lost: lost.iter().map(ReadRange::lost_range).collect(),
        reads: live
            .reads
            .iter()
            .map(|r| {
                (
                    r.clone(),
                    if lost.contains(r) {
                        Err(FsError::IoError)
                    } else {
                        Ok(r.sha256)
                    },
                )
            })
            .collect(),
        ..Default::default()
    };
    read_oracle(Row::DataAndReplicaLoss, &live, &observed)?;
    observed.reported_lost.clear();
    ensure!(
        read_oracle(Row::DataAndReplicaLoss, &live, &observed).is_err(),
        "dropping the exact lost-range report passed"
    );
    observed.reported_lost = lost.iter().map(ReadRange::lost_range).collect();
    for index in 0..observed.reads.len() {
        let saved = observed.reads[index].1;
        observed.reads[index].1 = match saved {
            Ok(_) => Err(FsError::IoError),
            Err(_) => Ok(observed.reads[index].0.sha256),
        };
        ensure!(
            read_oracle(Row::DataAndReplicaLoss, &live, &observed).is_err(),
            "wrong read outcome at range {index} passed"
        );
        observed.reads[index].1 = saved;
        checked += 1;
    }
    println!("METADATA_SURVIVAL_STATE_MUTATIONS_REJECTED {checked}");
    finish(live, Row::ExactRecovery, Ok(())).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unretained_superseded_payload_is_reclaimed() -> Result<()> {
    let live = Live::new(false).await?;
    let result = async {
        let observed = overwrite_and_reclaim(&live).await?;
        ensure!(observed.reclaim_completed && observed.reclaim_passes > 0);
        ensure!(
            !observed.old_object.exists(),
            "the control must delete the old payload"
        );
        let error = payload_retention_oracle(&live, &observed)
            .expect_err("the retention oracle accepted actual missing payload");
        println!(
            "METADATA_SURVIVAL_RETENTION_CONTROL {}",
            serde_json::json!({
                "survival_gate": "failed", "old_payload_reclaimed": true,
                "reclaim_passes": observed.reclaim_passes, "error": format!("{error:#}"),
            })
        );
        Ok(())
    }
    .await;
    finish(live, Row::PayloadRetention, result).await
}

#[tokio::test]
async fn commit_cut_hook_blocks_each_requested_boundary() -> Result<()> {
    ensure!(cuts().len() == 288);
    for cut in cuts() {
        let (mut hook, entered, release) = CommitCutHook::channel(cut);
        // A different replica must not trigger this boundary.
        tokio::time::timeout(
            Duration::from_secs(1),
            hook.reach(
                cut.point,
                cut.side,
                "unrelated-replica",
                Path::new("objects/expected"),
            ),
        )
        .await?;
        for invalid in ["", "/absolute", "../outside"] {
            tokio::time::timeout(
                Duration::from_secs(1),
                hook.reach(cut.point, cut.side, cut.replica, Path::new(invalid)),
            )
            .await?;
        }
        let object = PathBuf::from("objects/expected");
        let expected = CutEvent {
            cut,
            object: object.clone(),
        };
        let task = tokio::spawn(async move {
            hook.reach(cut.point, cut.side, cut.replica, &object).await;
        });
        ensure!(tokio::time::timeout(Duration::from_secs(1), entered).await?? == expected);
        ensure!(
            !task.is_finished(),
            "the hook reported a cut without blocking"
        );
        release
            .send(())
            .map_err(|_| anyhow::anyhow!("cut release receiver closed"))?;
        tokio::time::timeout(Duration::from_secs(1), task).await??;
    }
    Ok(())
}
