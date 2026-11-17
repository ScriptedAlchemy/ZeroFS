//! Executable acceptance contracts. No survival implementation lives here.

use super::*;
use crate::fs::key_codec::KeyCodec;
use crate::pool_object_store::survival_test_support::{self, CatalogImage};
use crate::segment::FrameLoc;
use async_trait::async_trait;
use futures::{StreamExt, TryStreamExt};
use std::collections::BTreeMap;
use std::path::{Component, Path};
use tokio::sync::oneshot;

const REPLICA_TIMEOUT: Duration = Duration::from_secs(90);
const REPLACEMENT_DEVICE_ID: &str = "fixture-replacement-device";

/// A production prerequisite that the current implementation lacks. Every
/// acceptance row fails today with exactly one of these as its root cause.
/// Nothing else constructs it, so a broken fixture or an unrelated error cannot
/// pass for a missing prerequisite.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[error("{0}")]
struct Missing(String);

fn missing<T>(prerequisite: impl Into<String>) -> Result<T> {
    Err(Missing(prerequisite.into()).into())
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
                    600
                }),
                acceptance(Row::$row),
            )
            .await
            .expect("acceptance exceeded its deadline")
            .expect($reason);
        }
    };
}

// The only list of rows. It defines the enum, `Row::ALL`, each row's stated
// reason and its ignored test, so a row cannot exist without all four.
macro_rules! acceptance_rows {
    (
        core { $($core:ident => $core_test:ident, $core_reason:literal;)+ }
        review { $($review:ident => $review_test:ident, $review_reason:literal;)+ }
    ) => {
        #[derive(Clone, Copy, Debug, PartialEq, Eq)]
        enum Row {
            $($core,)+
            $($review,)+
        }

        impl Row {
            const ALL: &'static [Self] = &[$(Self::$core,)+ $(Self::$review,)+];

            fn review_case(self) -> bool {
                matches!(self, $(Self::$review)|+)
            }

            /// The missing prerequisite this row fails with today, verbatim.
            /// It is also the row's `#[ignore]` reason.
            fn stated_reason(self) -> &'static str {
                match self {
                    $(Self::$core => $core_reason,)+
                    $(Self::$review => $review_reason,)+
                }
            }
        }

        $(acceptance_row!($core_test, $core, $core_reason);)+
        $(acceptance_row!($review_test, $review, $review_reason);)+
    };
}

acceptance_rows! {
    core {
        AuthorityLoss => authority_device_loss_preserves_committed_namespace,
            "the metadata_survival configuration table is unsupported";
        JournalBeforeBackend => journal_device_loss_before_backend_cutoff_preserves_committed_prefix,
            "the metadata_survival configuration table is unsupported";
        JournalAfterBackend => journal_device_loss_after_backend_cutoff_preserves_committed_prefix,
            "the metadata_survival configuration table is unsupported";
        DataAndReplicaLoss => data_member_with_metadata_replica_loss_preserves_namespace_and_reports_ranges,
            "the metadata_survival configuration table is unsupported";
        PayloadRetention => reclaim_preserves_payload_referenced_by_the_last_survival_commit,
            "the metadata_survival configuration table is unsupported";
        AdmittedWriteLoss => writes_in_flight_during_loss_preserve_last_survival_commit,
            "the admitted-write mid-commit hook is absent";
        InterruptedCommit => interrupted_commit_at_every_step_preserves_a_consistent_generation,
            "the production commit cut hook is absent";
        ExactRecovery => recovery_preserves_exact_catalog_identity_cleanup_debts_and_key_epoch,
            "the metadata_survival configuration table is unsupported";
        SlowReplica => slow_replica_past_timeout_stops_survival_acknowledgements,
            "the production Slow replica fault hook is absent";
        FullReplica => full_replica_stops_survival_acknowledgements,
            "the production Full replica fault hook is absent";
        StaleReplica => stale_replica_stops_survival_acknowledgements,
            "the production Stale replica fault hook is absent";
        ErroringReplica => erroring_replica_stops_survival_acknowledgements,
            "the production Io replica fault hook is absent";
        CompetingWriter => competing_writer_is_refused_by_every_replica_lock,
            "the production SecondWriter replica fault hook is absent";
        ReplacementCatchUp => replaced_replica_requires_verified_offline_catch_up,
            "the production Replacement replica fault hook is absent";
    }
    review {
        DurableEnablement => durable_enablement_survives_missing_or_mismatched_configuration,
            "the production DurableEnablement scenario hook is absent";
        TerminalDisable => offline_disable_blocks_stale_recovery_and_requires_a_fresh_epoch,
            "the production TerminalDisable scenario hook is absent";
        StaleRestart => restart_refuses_a_replica_that_missed_a_committed_generation,
            "the production StaleRestart scenario hook is absent";
        CorruptDependency => recovery_refuses_corrupt_dependency_bytes,
            "the production CorruptDependency scenario hook is absent";
        CorruptRecord => recovery_refuses_corrupt_commit_record_bytes,
            "the production CorruptRecord scenario hook is absent";
        HighestGeneration => recovery_selects_highest_valid_chain_without_a_receipt_hint,
            "the production HighestGeneration scenario hook is absent";
        ConflictingForks => recovery_refuses_authenticated_conflicting_forks,
            "the production ConflictingForks scenario hook is absent";
        PartialFanoutRestart => restart_completes_partial_record_fanout_idempotently,
            "the production PartialFanoutRestart scenario hook is absent";
        BacklogRetry => large_writeback_backlog_aborts_and_retries_with_admission_open,
            "the production BacklogRetry scenario hook is absent";
        ReadFaultClosed => read_faults_defer_catalog_writes_while_admission_is_closed,
            "the production ReadFaultClosed scenario hook is absent";
        ReadFaultReadOnly => read_faults_preserve_eio_and_catalog_in_read_only_mode,
            "the production ReadFaultReadOnly scenario hook is absent";
        IdleAndBounds => idle_commits_skip_and_records_and_checkpoints_stabilize,
            "the production IdleAndBounds scenario hook is absent";
        StatusLag => failed_commits_report_actual_status_and_growing_lag,
            "the production StatusLag scenario hook is absent";
        PromoteAuthority => authority_promotion_restores_writes_and_cold_durability,
            "the production PromoteAuthority scenario hook is absent";
        OrphanCheckpoint => restart_reconciles_a_named_orphan_checkpoint,
            "the production OrphanCheckpoint scenario hook is absent";
        AdmissionLockOrder => admission_precedes_contended_inode_locks,
            "the production AdmissionLockOrder scenario hook is absent";
        DataOnlyLoss => payload_only_device_loss_keeps_unaffected_files_writable,
            "the production DataOnlyLoss scenario hook is absent";
        RetentionCrash => retained_dependency_union_survives_restart_and_repeated_sweep,
            "the production RetentionCrash scenario hook is absent";
    }
}

impl Row {
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

impl CommitPoint {
    const ALL: [Self; 19] = [
        Self::AdmissionClosed,
        Self::AdmittedWorkDrained,
        Self::BackendCutoff,
        Self::CheckpointIntentSynced,
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
#[derive(Default)]
struct CommitProgress {
    durable_records: std::collections::BTreeSet<String>,
}

pub(crate) struct CommitCutHook {
    cut: Cut,
    progress: Arc<std::sync::Mutex<CommitProgress>>,
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
                progress: Arc::default(),
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
        if point == CommitPoint::CommitDirectorySynced
            && side == Side::After
            && ["authority", "disk-a-metadata"].contains(&replica)
            && valid_object_path(object)
            && object.starts_with("commits")
        {
            self.progress
                .lock()
                .unwrap()
                .durable_records
                .insert(replica.to_owned());
        }
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
    sequence: u64,
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
    writeback: Option<WritebackObjectStore>,
    replica_override: Option<[PathBuf; 2]>,
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
        if let Some(writeback) = self.writeback.take() {
            writeback
                .shutdown()
                .await
                .expect("shutdown acceptance writeback");
        }
    }

    async fn attach_journal(&mut self) -> Result<()> {
        self.close_writer().await;
        let journal = self
            .fixture
            .root
            .path()
            .canonicalize()?
            .join("journal-device/journal");
        let (fs, pool, writeback) =
            open_with_writeback(&self.fixture.settings, false, Some(journal)).await?;
        let writeback = writeback.context("the interrupted fixture needs real writeback")?;
        writeback.activate_remote()?;
        self.fs = Some(fs);
        self.pool = Some(pool);
        self.writeback = Some(writeback);
        // A local durability receipt must have actual journal coverage. This
        // remains a real journal even when its remote backlog has drained.
        overwrite_first_file(self, 31).await?;
        let local = self.writeback.as_ref().unwrap().status()?;
        ensure!(
            local.local_seq > 0,
            "the journal fixture must contain accepted objects"
        );
        ensure!(
            !tree_hashes(&self.fixture.root.path().join("journal-device"))?.is_empty(),
            "the journal device must contain real files"
        );
        Ok(())
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
            writeback: None,
            replica_override: None,
        })
    }

    fn replica_roots(&self) -> [PathBuf; 2] {
        if let Some(roots) = &self.replica_override {
            return roots.clone();
        }
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

struct RecoverySources {
    replicas: Vec<PathBuf>,
    password: &'static str,
    scratch_parent: PathBuf,
}

impl RecoverySources {
    fn from_live(live: &Live) -> Self {
        Self {
            replicas: live
                .replica_roots()
                .into_iter()
                .map(|root| root.join(".zerofs-survival/v1"))
                .filter(|root| root.is_dir())
                .collect(),
            password: PASSWORD,
            scratch_parent: live.fixture.root.path().join("recovery-scratch"),
        }
    }
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
    // Receipt delivery is independent of the task's later cleanup hooks.
    receipt_returned: oneshot::Receiver<Result<SurvivalReceipt>>,
    progress: Arc<std::sync::Mutex<CommitProgress>>,
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
    commit_error: Option<SurvivalFailure>,
    mutation_result: Option<Result<(), FsError>>,
    before_status: Option<SurvivalStatus>,
    after_status: Option<SurvivalStatus>,
    retry_receipt: Option<SurvivalReceipt>,
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
    unretained_dead_object: Option<PathBuf>,
}

#[derive(Default)]
struct Observation {
    receipt: Option<SurvivalReceipt>,
    recovered: Option<State>,
    recovered_digest: Option<[u8; 32]>,
    recovered_epoch: Option<u64>,
    recovered_unacknowledged_candidate: bool,
    reads: Vec<(ReadRange, Result<[u8; 32], FsError>)>,
    reported_lost: Vec<LostRange>,
    admitted_pending_at_loss: bool,
    commit_waited_for_admitted: bool,
    late_arrival: Option<Result<(), FsError>>,
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
    review: Option<review_cases::ReviewObservation>,
}

#[async_trait]
trait Survival: Sync {
    async fn crash_writer(&self, _live: &mut Live) -> Result<()> {
        missing("the production interrupted-writer shutdown hook is absent")
    }
    async fn commit(&self, live: &Live) -> Result<SurvivalReceipt>;
    async fn recover(&self, sources: &RecoverySources) -> Result<Option<Recovered>>;
    async fn status(&self, _live: &Live) -> Result<SurvivalStatus> {
        missing("the production survival status hook is absent")
    }
    async fn clear_fault(&self, _live: &Live) -> Result<()> {
        missing("the production transient replica retry hook is absent")
    }
    async fn review_action(&self, _live: &mut Live, action: review_cases::Action) -> Result<()> {
        let scenario = match action {
            review_cases::Action::Enable(row) => format!("{row:?}"),
            other => format!("{other:?}"),
        };
        missing(format!("the production {scenario} scenario hook is absent"))
    }
    async fn owned_checkpoints(&self, _live: &Live) -> Result<Vec<slatedb::Checkpoint>> {
        missing("the production owned-checkpoint inspection hook is absent")
    }
    async fn pause_commit(&self, _live: &Live, _cut: Cut) -> Result<Option<PausedCommit>> {
        Ok(None)
    }
    async fn pause_write(&self, _live: &Live) -> Result<Option<PausedWrite>> {
        Ok(None)
    }
    async fn fault(&self, _live: &Live, fault: ReplicaFault) -> Result<FaultObservation> {
        missing(format!(
            "the production {fault:?} replica fault hook is absent"
        ))
    }
}

struct CurrentImplementation;

#[async_trait]
impl Survival for CurrentImplementation {
    async fn commit(&self, live: &Live) -> Result<SurvivalReceipt> {
        // The future production binding must return the owner's real receipt.
        // Accepting the configuration alone supplies no such receipt. Today the
        // parser refuses the table first, because config.rs denies unknown pool
        // fields. Only that exact refusal is the missing prerequisite. Any other
        // parse error is a fixture fault and stays one.
        match live.proposed_settings() {
            Ok(_settings) => missing("the production survival receipt hook is absent"),
            Err(error) if format!("{error:#}").contains("unknown field `metadata_survival`") => {
                missing("the metadata_survival configuration table is unsupported")
            }
            Err(error) => Err(error),
        }
    }

    async fn recover(&self, sources: &RecoverySources) -> Result<Option<Recovered>> {
        let _bootstrap_inputs = (&sources.replicas, sources.password, &sources.scratch_parent);
        missing("the production offline recovery hook is absent")
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
            sequence: 1,
        })
    }
    async fn recover(&self, _sources: &RecoverySources) -> Result<Option<Recovered>> {
        Ok(None)
    }
    // The stub claims no injected fault. The mutation test's optimistic
    // observation supplies one.
    async fn fault(&self, _live: &Live, _fault: ReplicaFault) -> Result<FaultObservation> {
        Ok(FaultObservation::default())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
enum SurvivalFailure {
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
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct SurvivalStatus {
    last_sequence: Option<u64>,
    last_digest: Option<[u8; 32]>,
    completed_at: Option<u64>,
    lag: Duration,
    failed_replicas: BTreeMap<String, String>,
    admission_open: bool,
    owned_checkpoints: usize,
}

async fn probe_create(live: &Live, name: &[u8]) -> Result<Result<(), FsError>> {
    let auth = AuthContext::default();
    let creds = Credentials::from_auth_context(&auth);
    Ok(tokio::time::timeout(
        Duration::from_secs(2),
        live.fs().create(&creds, 0, name, &SetAttributes::default()),
    )
    .await
    .context("a mutation remained queued beyond its admission bound")?
    .map(|_| ()))
}

async fn overwrite_first_file(live: &mut Live, seed: u32) -> Result<()> {
    let file = live.reads[0].inode;
    let data = payload(seed).slice(..FINAL_SIZE);
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
            .context("successor extent")?;
        let loc = FrameLoc::decode(&loc).context("successor frame")?;
        range.member = Some(
            placements
                .iter()
                .find(|p| {
                    p.location
                        .as_ref()
                        .ends_with(loc.segid.object_key().as_str())
                })
                .context("successor mapping")?
                .member_id
                .clone(),
        );
        range.sha256 = Sha256::digest(
            &data[range.offset as usize..range.offset as usize + range.length as usize],
        )
        .into();
    }
    live.reads = current_reads;
    Ok(())
}

async fn create_dead_control(live: &Live, range: &ReadRange) -> Result<(PathBuf, [u8; 32])> {
    // The first successor was never committed for survival. Overwriting it
    // again makes that exact segment an unretained control for the same pass.
    let key = KeyCodec::new().extent_key(range.inode, 0);
    let loc = live
        .fs()
        .db
        .get_bytes(&key)
        .await?
        .context("dead control extent")?;
    let loc = FrameLoc::decode(&loc).context("dead control frame")?;
    let placement = live
        .pool()
        .placements()
        .await?
        .into_iter()
        .find(|p| {
            p.location
                .as_ref()
                .ends_with(loc.segid.object_key().as_str())
        })
        .context("dead control physical generation")?;
    let path = live
        .fixture
        .root
        .path()
        .join(format!("{}-device/data", placement.member_id))
        .join(placement.physical_key);
    ensure!(path.is_file(), "the dead control must exist before reclaim");
    let data = payload(103).slice(..range.length as usize);
    live.fs()
        .write(&AuthContext::default(), range.inode, 0, &data)
        .await?;
    let _ = live.fs().administrative_remote_durability().await?;
    let counter = live
        .fs()
        .db
        .get_bytes(&KeyCodec::new().segcount_key(loc.segid.epoch, loc.segid.counter))
        .await?
        .context("dead control segment counter")?;
    ensure!(
        KeyCodec::decode_segcount(&counter)
            .context("dead control counts")?
            .0
            == 0,
        "the control is still live"
    );
    Ok((path, Sha256::digest(data).into()))
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RecordCoverage {
    AllRequired,
    AllSurviving,
    InterruptedCandidate,
}

fn verify_replica_record(
    live: &Live,
    receipt: &SurvivalReceipt,
    coverage: RecordCoverage,
) -> Result<()> {
    verify_replica_record_at(&live.replica_roots(), receipt, coverage)
}

fn verify_replica_record_at(
    roots: &[PathBuf],
    receipt: &SurvivalReceipt,
    coverage: RecordCoverage,
) -> Result<()> {
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
    ensure!(
        roots.iter().any(|root| root.exists()),
        "no metadata replica survived"
    );
    let mut verified = 0;
    for replica in roots {
        if !replica.exists() && coverage != RecordCoverage::AllRequired {
            continue;
        }
        let record = replica
            .join(".zerofs-survival/v1")
            .join(&receipt.relative_record);
        let bytes = match std::fs::read(&record) {
            Ok(bytes) => bytes,
            Err(error)
                if coverage == RecordCoverage::InterruptedCandidate
                    && error.kind() == std::io::ErrorKind::NotFound =>
            {
                // Dependencies precede record fanout. An interrupted successor
                // may therefore be complete on only one surviving replica.
                continue;
            }
            Err(error) => {
                return Err(error)
                    .with_context(|| format!("receipt commit is absent on {}", replica.display()));
            }
        };
        ensure!(
            <[u8; 32]>::from(Sha256::digest(bytes)) == receipt.digest,
            "receipt commit digest mismatch"
        );
        verified += 1;
    }
    ensure!(verified > 0, "no surviving replica contains the commit");
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
        ReplicaFault::Slow | ReplicaFault::Full | ReplicaFault::Stale | ReplicaFault::Io => {
            let prior = observed
                .receipt
                .as_ref()
                .context("no prior fault receipt")?;
            let before = actual
                .before_status
                .as_ref()
                .context("no status before the fault")?;
            let after = actual
                .after_status
                .as_ref()
                .context("no status after the fault")?;
            ensure!(
                before.last_sequence == Some(prior.sequence)
                    && after.last_sequence == before.last_sequence
                    && after.last_digest == before.last_digest
                    && after.completed_at == before.completed_at,
                "a failed attempt advanced the successful recovery point"
            );
            ensure!(
                after.lag >= before.lag && after.failed_replicas.contains_key("disk-a-metadata"),
                "status omitted growing lag or the failed replica identity"
            );
            let expected = match fault {
                ReplicaFault::Slow => SurvivalFailure::Timeout,
                ReplicaFault::Full => SurvivalFailure::Full,
                ReplicaFault::Stale => SurvivalFailure::Stale,
                ReplicaFault::Io => SurvivalFailure::Io,
                _ => unreachable!(),
            };
            ensure!(
                actual.commit_error == Some(expected),
                "the commit did not return its specific fault"
            );
            if fault == ReplicaFault::Slow {
                ensure!(
                    actual.elapsed >= REPLICA_TIMEOUT
                        && actual.elapsed <= REPLICA_TIMEOUT + Duration::from_secs(5),
                    "the slow replica must return within its 90-second timeout plus settlement margin"
                );
                ensure!(
                    after.admission_open && actual.mutation_result == Some(Ok(())),
                    "a pre-record timeout must reopen mutation admission"
                );
                let retry = actual
                    .retry_receipt
                    .as_ref()
                    .context("online retry returned no receipt")?;
                ensure!(
                    retry.sequence > prior.sequence,
                    "online retry did not advance the generation"
                );
                verify_replica_record(live, retry, RecordCoverage::AllRequired)?;
            } else {
                ensure!(
                    !after.admission_open && actual.mutation_result == Some(Err(FsError::IoError)),
                    "a failed metadata replica must reject an actual mutation with EIO"
                );
            }
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
    verify_replica_record(
        live,
        observed
            .receipt
            .as_ref()
            .context("no prior survival receipt for the fault case")?,
        RecordCoverage::AllSurviving,
    )?;
    Ok(())
}

fn oracle(row: Row, live: &Live, observed: &Observation) -> Result<()> {
    if row.review_case() {
        return review_cases::oracle(row, live, observed);
    }
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
            observed.admitted_pending_at_loss
                && observed.commit_waited_for_admitted
                && observed.late_arrival == Some(Err(FsError::ReadOnlyFilesystem)),
            "the admitted write must overlap a waiting commit and a bounded rejected late arrival"
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
    verify_replica_record(
        live,
        receipt,
        if row == Row::InterruptedCommit && observed.recovered_unacknowledged_candidate {
            RecordCoverage::InterruptedCandidate
        } else {
            RecordCoverage::AllSurviving
        },
    )?;
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
    let dead = observed
        .unretained_dead_object
        .as_ref()
        .context("no dead unretained control object")?;
    ensure!(
        !dead.exists(),
        "reclaim retained an unreferenced control object, so disabling GC could pass"
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
    println!("METADATA_SURVIVAL_RECLAIM_PHASE prepare-unretained-control");
    let (unretained_dead_object, successor_hash) =
        tokio::time::timeout(Duration::from_secs(10), create_dead_control(live, &range))
            .await
            .context("the dead control preparation did not finish")??;
    println!("METADATA_SURVIVAL_RECLAIM_PHASE collect");
    let stats = live.fs().extent_store.segment_gc_stats();
    let before = stats.passes.load(std::sync::atomic::Ordering::Relaxed);
    // The live export pin must already have transferred to the ledger. Run a
    // full pass so a permanent-checkpoint skip cannot satisfy this contract.
    let pass = tokio::time::timeout(
        Duration::from_secs(30),
        live.fs().extent_store.reclaim_segments_gated(
            || std::future::ready(Ok(Some((chrono::Utc::now(), None)))),
            None,
            Duration::ZERO,
            |_| false,
            64 * 1024 * 1024,
        ),
    )
    .await
    .context("the reclaim pass did not finish")??;
    println!("METADATA_SURVIVAL_RECLAIM_PHASE collected");
    let after = stats.passes.load(std::sync::atomic::Ordering::Relaxed);
    Ok(PayloadRetentionObservation {
        range,
        old_object,
        old_object_digest,
        successor_hash,
        old_live_bytes,
        old_total_bytes,
        reclaim_passes: after.saturating_sub(before),
        reclaim_completed: matches!(pass.status, crate::fs::store::PassStatus::Completed { .. }),
        unretained_dead_object: Some(unretained_dead_object),
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

fn recovery_read_plan<'a>(
    recovered_digest: &[u8; 32],
    generations: &[(&SurvivalReceipt, &'a [ReadRange])],
) -> Result<&'a [ReadRange]> {
    generations
        .iter()
        .find(|(receipt, _)| receipt.digest == *recovered_digest)
        .map(|(_, reads)| *reads)
        .context("recovery selected a generation with no captured read plan")
}

async fn collect_recovery(
    live: &Live,
    driver: &dyn Survival,
    read_generations: &[(&SurvivalReceipt, &[ReadRange])],
    observed: &mut Observation,
) -> Result<()> {
    let sources: Vec<_> = live
        .replica_roots()
        .iter()
        .map(|root| tree_hashes(&root.join(".zerofs-survival/v1")))
        .collect::<Result<_>>()?;
    if let Some(recovered) = driver.recover(&RecoverySources::from_live(live)).await? {
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
            for range in recovery_read_plan(&recovered.commit_digest, read_generations)? {
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
    ensure!(
        device.is_dir(),
        "cannot lose a device that never existed: {}",
        device.display()
    );
    ensure!(
        !tree_hashes(&device)?.is_empty(),
        "cannot lose an empty fixture device"
    );
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
    collect_recovery(live, driver, &[(&receipt, &live.reads)], &mut observed).await?;
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
    verify_replica_record(live, &receipt, RecordCoverage::AllRequired)?;
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
    verify_replica_record(live, &receipt, RecordCoverage::AllRequired)?;
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
    if row.review_case() {
        let observed = review_cases::exercise(row, live, driver).await?;
        return oracle(row, live, &observed);
    }
    if let Some(kind) = row.fault() {
        let mut fault = driver.fault(live, kind).await?;
        if matches!(
            kind,
            ReplicaFault::Slow | ReplicaFault::Full | ReplicaFault::Stale | ReplicaFault::Io
        ) && fault.injected == Some(kind)
        {
            fault.before_status = Some(driver.status(live).await?);
            let started = std::time::Instant::now();
            let result = tokio::time::timeout(
                REPLICA_TIMEOUT + Duration::from_secs(5),
                driver.commit(live),
            )
            .await?;
            fault.elapsed = started.elapsed();
            fault.commit_error = result
                .err()
                .and_then(|e| e.downcast_ref::<SurvivalFailure>().copied());
            fault.after_status = Some(driver.status(live).await?);
            fault.mutation_result = Some(probe_create(live, b"fault-mutation").await?);
            if kind == ReplicaFault::Slow {
                driver.clear_fault(live).await?;
                fault.retry_receipt = Some(driver.commit(live).await?);
            }
        }
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
        verify_replica_record(live, &receipt, RecordCoverage::AllRequired)?;
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
        let write = driver
            .pause_write(live)
            .await?
            .ok_or_else(|| Missing("the admitted-write mid-commit hook is absent".to_owned()))?;
        let mut commit = None;
        let result = async {
            let prior = driver.commit(live).await?;
            verify_replica_record(live, &prior, RecordCoverage::AllRequired)?;
            live.expected =
                State::capture(live.fs(), live.pool(), &live.replica_roots()[0]).await?;
            write
                .start
                .send(())
                .map_err(|_| anyhow::anyhow!("write start barrier closed"))?;
            tokio::time::timeout(Duration::from_secs(5), write.admitted).await??;
            let cut = Cut {
                point: CommitPoint::AdmittedWorkDrained,
                side: Side::Before,
                replica: "authority",
                lost: Device::Authority,
            };
            let pause = driver
                .pause_commit(live, cut)
                .await?
                .ok_or_else(|| Missing("the production commit drain hook is absent".to_owned()))?;
            pause
                .start
                .send(())
                .map_err(|_| anyhow::anyhow!("commit start barrier closed"))?;
            let event = tokio::time::timeout(Duration::from_secs(5), pause.entered).await??;
            let waiting =
                event.cut == cut && !pause.task.is_finished() && !write.task.is_finished();
            // Release the artificial hook. Only the admitted write may now
            // prevent the real drain from completing.
            let _ = pause.release.send(());
            commit = Some(pause.task);
            let late = probe_create(live, b"late-arrival").await?;
            tokio::time::sleep(Duration::from_millis(25)).await;
            let pending = !write.task.is_finished();
            let drained_wait = waiting && !commit.as_ref().unwrap().is_finished();
            ensure!(
                pending && drained_wait,
                "commit did not wait for admitted work"
            );
            lose_device(live, Device::Authority)?;
            Ok((prior, pending, drained_wait, late))
        }
        .await;
        drop(write.release);
        write.task.abort();
        let _ = write.task.await;
        if let Some(task) = commit {
            task.abort();
            let _ = task.await;
        }
        let (receipt, pending, waited, late) = result?;
        driver.crash_writer(live).await?;
        return recover_after_loss(
            row,
            live,
            driver,
            receipt,
            Observation {
                admitted_pending_at_loss: pending,
                commit_waited_for_admitted: waited,
                late_arrival: Some(late),
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
    mut pause: PausedCommit,
) -> Result<()> {
    let candidate_slot = Arc::clone(&pause.candidate);
    let result: Result<(CutEvent, SurvivalReceipt, State, Vec<ReadRange>)> = async {
        // Three completed generations precede the candidate: the third may
        // retire the first, and the fourth can record that cleanup's completion.
        // The two-generation retention policy otherwise leaves retirement and
        // later debt-completion cut points unreachable.
        let oldest = driver.commit(live).await?;
        verify_replica_record(live, &oldest, RecordCoverage::AllRequired)?;
        overwrite_first_file(live, 86).await?;
        let middle = driver.commit(live).await?;
        ensure!(
            middle.digest != oldest.digest,
            "the second commit must name a new generation"
        );
        verify_replica_record(live, &middle, RecordCoverage::AllRequired)?;
        overwrite_first_file(live, 87).await?;
        survival_test_support::create_debts(live.pool()).await?;
        let prior = driver
            .commit(live)
            .await
            .context("the prior receipt must precede the interrupted successor")?;
        ensure!(
            prior.digest != middle.digest
                && prior.digest != oldest.digest
                && oldest.sequence < middle.sequence
                && middle.sequence < prior.sequence,
            "the predecessor must be a new generation after the overwrite"
        );
        verify_replica_record(live, &prior, RecordCoverage::AllRequired)?;
        let prior_state = State::capture(live.fs(), live.pool(), &live.replica_roots()[0]).await?;
        survival_test_support::require_nonzero_debts(&prior_state.catalog)?;
        let prior_reads = live.reads.clone();
        // Change a current file so a mixture of predecessor and candidate state
        // cannot satisfy either complete-generation oracle.
        overwrite_first_file(live, 88).await?;
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
    let returned = pause.receipt_returned.try_recv().ok().transpose()?;
    let requires_publication = matches!(
        cut.point,
        CommitPoint::RetirementLogged
            | CommitPoint::CheckpointReleased
            | CommitPoint::DependencyDeleted
            | CommitPoint::DebtCompletionCommitted
    ) || (cut.point == CommitPoint::CommitDirectorySynced
        && cut.side == Side::After
        && cut.replica == "disk-a-metadata");
    let all_records = requires_publication
        || pause.progress.lock().unwrap().durable_records.len() == live.replica_roots().len();
    let may_select_prior = returned.is_none() && !all_records;
    let mut read_generations = vec![(&prior, prior_reads.as_slice())];
    if let Some(candidate) = &candidate {
        ensure!(
            candidate.digest != prior.digest,
            "candidate reused the predecessor digest"
        );
        read_generations.push((candidate, live.reads.as_slice()));
    }
    remove_active_originals(live, &mut observed)?;
    collect_recovery(live, driver, &read_generations, &mut observed).await?;
    let receipt = if observed.recovered_digest == Some(prior.digest) {
        ensure!(
            may_select_prior,
            "recovery rolled back a returned or fully durable commit"
        );
        live.expected = prior_state;
        live.reads = prior_reads;
        prior
    } else {
        observed.recovered_unacknowledged_candidate = may_select_prior;
        let candidate = candidate
            .context("recovery selected a generation with no complete candidate record")?;
        if let Some(returned) = returned {
            ensure!(
                returned.digest == candidate.digest,
                "returned receipt differs from candidate"
            );
        }
        candidate
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
        let mut first = Live::new(false).await?;
        first.attach_journal().await?;
        let mut idle_fixture = Some(first);
        for cut in cuts() {
            let mut live = match idle_fixture.take() {
                Some(live) => live,
                None => {
                    let mut fresh = Live::new(false).await?;
                    fresh.attach_journal().await?;
                    fresh
                }
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
                    Err(anyhow::Error::from(Missing(
                        "the production commit cut hook is absent".to_owned(),
                    ))
                    .context(format!("{cut:?}")))
                }
            };
            if let Err(error) = result {
                println!("METADATA_SURVIVAL_COMMIT_CUT {cut:?} {error:#}");
                failures.push(error);
            }
        }
        // The row keeps a cut's own error only when every requested cut failed
        // with the same root cause. A partial pass or mixed causes produce a
        // summary that names no single missing prerequisite.
        let requested = cuts().len();
        let failed = failures.len();
        let mut causes: Vec<String> = failures
            .iter()
            .map(|error| error.root_cause().to_string())
            .collect();
        causes.sort();
        causes.dedup();
        let result = match failures.into_iter().next() {
            None => Ok(()),
            Some(first) if failed == requested && causes.len() == 1 => {
                Err(first.context(format!("all {requested} commit cuts failed")))
            }
            Some(_) => Err(anyhow::anyhow!(
                "{failed} of {requested} commit cuts failed, with root causes {causes:?}"
            )),
        };
        if let Some(live) = idle_fixture {
            finish(live, row, Ok(())).await?;
        }
        println!(
            "METADATA_SURVIVAL_ACCEPTANCE {}",
            serde_json::json!({"row": "InterruptedCommit", "requested_cuts": cuts().len(), "failed_cuts": failed, "cleanup": "passed"})
        );
        return result;
    }
    let mut live = Live::new(row == Row::ExactRecovery).await?;
    let result = one_case(row, &mut live, &CurrentImplementation).await;
    finish(live, row, result).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn every_acceptance_oracle_rejects_success_without_survival() -> Result<()> {
    let live = Live::new(true).await?;
    let receipt = StubSurvival.commit(&live).await?;
    ensure!(
        StubSurvival
            .recover(&RecoverySources::from_live(&live))
            .await?
            .is_none()
    );
    let mut rejected = 0;
    for &row in Row::ALL {
        // Feed successful stub output directly to the same oracle used by the
        // ignored row. Configuration rejection cannot make this mutation pass.
        let observed = StubSurvival.observe(row, &live).await?;
        let error = oracle(row, &live, &observed)
            .expect_err("a successful no-op passed an acceptance oracle");

        let before_status = SurvivalStatus {
            last_sequence: Some(receipt.sequence),
            last_digest: Some(receipt.digest),
            completed_at: Some(1),
            lag: Duration::from_secs(1),
            failed_replicas: BTreeMap::new(),
            admission_open: true,
            owned_checkpoints: 0,
        };
        let mut after_status = before_status.clone();
        after_status.lag = REPLICA_TIMEOUT;
        after_status.failed_replicas.insert(
            "disk-a-metadata".to_owned(),
            "injected replica fault".to_owned(),
        );
        after_status.admission_open = row == Row::SlowReplica;
        let commit_error = match row {
            Row::FullReplica => SurvivalFailure::Full,
            Row::StaleReplica => SurvivalFailure::Stale,
            Row::ErroringReplica => SurvivalFailure::Io,
            _ => SurvivalFailure::Timeout,
        };
        let mut retry = receipt.clone();
        retry.sequence += 1;
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
            commit_waited_for_admitted: true,
            late_arrival: Some(Err(FsError::ReadOnlyFilesystem)),
            review: row
                .review_case()
                .then(|| review_cases::optimistic(row, &live, &receipt)),
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
                commit_error: Some(commit_error),
                mutation_result: Some(if row == Row::SlowReplica {
                    Ok(())
                } else {
                    Err(FsError::IoError)
                }),
                before_status: Some(before_status),
                after_status: Some(after_status),
                retry_receipt: Some(retry),
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
        verify_replica_record(&live, &receipt, RecordCoverage::AllRequired).is_err(),
        "the stub manufactured a durable commit"
    );
    finish(live, Row::ExactRecovery, Ok(())).await
}

// Each row runs its real driver path against the current implementation, with
// its own fresh fixture, until its first missing prerequisite. The row must
// fail with exactly the `Missing` error it states. A row that passes, panics,
// times out, or fails for any other reason (a broken fixture, an unrelated
// error, a different missing prerequisite) fails this test. Rows come from
// `Row::ALL`, so a new row is covered without editing this test.
//
// The budget bounds each row, not the matrix it would run with a production
// binding. Today every row stops at its first driver call. If a binding lets
// a row run further, this test fails until the row's stated reason is updated.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn every_acceptance_row_fails_for_its_stated_reason() -> Result<()> {
    const ROW_BUDGET: Duration = Duration::from_secs(120);
    let outcomes: Vec<_> = futures::stream::iter(Row::ALL.iter().copied())
        .map(|row| async move {
            let started = std::time::Instant::now();
            let outcome = tokio::spawn(tokio::time::timeout(ROW_BUDGET, acceptance(row))).await;
            (row, started.elapsed(), outcome)
        })
        .buffer_unordered(4)
        .collect()
        .await;
    ensure!(outcomes.len() == Row::ALL.len(), "a row was not run");
    let mut wrong = Vec::new();
    for (row, elapsed, outcome) in outcomes {
        let verdict = match outcome {
            Err(join) => format!("panicked: {join}"),
            Ok(Err(_)) => format!("reached no failure within {ROW_BUDGET:?}"),
            Ok(Ok(Ok(()))) => "passed without a survival implementation".to_owned(),
            Ok(Ok(Err(error))) => match error.root_cause().downcast_ref::<Missing>() {
                Some(Missing(reason)) if reason == row.stated_reason() => {
                    println!(
                        "METADATA_SURVIVAL_STATED_REASON {row:?} seconds={:.1} {reason}",
                        elapsed.as_secs_f64()
                    );
                    continue;
                }
                _ => format!("failed for another reason: {error:#}"),
            },
        };
        wrong.push(format!(
            "{row:?} states \"{}\" but {verdict}",
            row.stated_reason()
        ));
    }
    ensure!(
        wrong.is_empty(),
        "{} of {} rows did not fail for their stated reason:\n{}",
        wrong.len(),
        Row::ALL.len(),
        wrong.join("\n")
    );
    Ok(())
}

#[test]
fn record_oracle_distinguishes_acknowledgement_from_interrupted_fanout() -> Result<()> {
    let fixture = tempfile::tempdir()?;
    let roots = [
        fixture.path().join("authority"),
        fixture.path().join("replica"),
    ];
    let bytes = b"complete candidate record";
    let receipt = SurvivalReceipt {
        relative_record: "commits/1/3.json".into(),
        digest: Sha256::digest(bytes).into(),
        mutation_cutoff: 3,
        sequence: 3,
        envelope_epoch: 1,
    };
    let records = roots
        .iter()
        .map(|root| {
            root.join(".zerofs-survival/v1")
                .join(&receipt.relative_record)
        })
        .collect::<Vec<_>>();
    for record in &records {
        std::fs::create_dir_all(record.parent().unwrap())?;
    }
    ensure!(
        verify_replica_record_at(&roots, &receipt, RecordCoverage::InterruptedCandidate).is_err(),
        "a candidate needs at least one real commit record"
    );
    std::fs::write(&records[0], bytes)?;
    verify_replica_record_at(&roots, &receipt, RecordCoverage::InterruptedCandidate)?;
    ensure!(
        verify_replica_record_at(&roots, &receipt, RecordCoverage::AllRequired).is_err(),
        "partial fanout cannot acknowledge a survival receipt"
    );
    ensure!(
        verify_replica_record_at(&roots, &receipt, RecordCoverage::AllSurviving).is_err(),
        "an acknowledged receipt must be present on every surviving replica"
    );
    std::fs::write(&records[1], b"conflicting candidate record")?;
    ensure!(
        verify_replica_record_at(&roots, &receipt, RecordCoverage::InterruptedCandidate).is_err(),
        "a valid copy cannot hide a conflicting surviving record"
    );
    std::fs::write(&records[1], bytes)?;
    verify_replica_record_at(&roots, &receipt, RecordCoverage::AllRequired)?;
    std::fs::remove_dir_all(&roots[1])?;
    verify_replica_record_at(&roots, &receipt, RecordCoverage::AllSurviving)?;
    verify_replica_record_at(&roots, &receipt, RecordCoverage::InterruptedCandidate)?;
    ensure!(
        verify_replica_record_at(&roots, &receipt, RecordCoverage::AllRequired).is_err(),
        "a missing required replica cannot disappear from the acknowledgement set"
    );
    std::fs::remove_dir_all(&roots[0])?;
    ensure!(
        verify_replica_record_at(&roots, &receipt, RecordCoverage::InterruptedCandidate).is_err(),
        "a lost last record cannot satisfy candidate recovery"
    );
    Ok(())
}

#[test]
fn recovery_reads_follow_the_selected_generation() -> Result<()> {
    let prior = SurvivalReceipt {
        relative_record: "commits/1/2.json".into(),
        digest: [2; 32],
        mutation_cutoff: 2,
        sequence: 2,
        envelope_epoch: 1,
    };
    let candidate = SurvivalReceipt {
        relative_record: "commits/1/3.json".into(),
        digest: [3; 32],
        mutation_cutoff: 3,
        sequence: 3,
        envelope_epoch: 1,
    };
    let prior_reads = [ReadRange {
        names: vec!["/changed.bin".into(), "/alias".into()],
        inode: 1,
        offset: 0,
        length: 4,
        member: Some("disk-a".into()),
        sha256: Sha256::digest(b"past").into(),
    }];
    let mut candidate_reads = prior_reads.clone();
    candidate_reads[0].member = Some("disk-b".into());
    candidate_reads[0].sha256 = Sha256::digest(b"next").into();
    let generations = [
        (&prior, prior_reads.as_slice()),
        (&candidate, candidate_reads.as_slice()),
    ];
    ensure!(
        recovery_read_plan(&prior.digest, &generations)? == prior_reads,
        "predecessor recovery must use its original hashes and lost-member mapping"
    );
    ensure!(
        recovery_read_plan(&candidate.digest, &generations)? == candidate_reads,
        "candidate recovery must use the successor hashes and placement"
    );
    ensure!(
        recovery_read_plan(&[1; 32], &generations).is_err(),
        "a retired or unknown generation cannot borrow another generation's read plan"
    );
    Ok(())
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
        ensure!(
            observed
                .unretained_dead_object
                .as_ref()
                .is_some_and(|path| !path.exists()),
            "the control must delete the unretained intermediate payload"
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
    ensure!(cuts().len() == 304);
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

#[path = "review_cases.rs"]
mod review_cases;
