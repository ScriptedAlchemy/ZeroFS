use serde::Serialize;
use std::fmt::Write as _;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, ensure};
use bytes::Bytes;
use futures::stream::{self, StreamExt, TryStreamExt};

use crate::config::CompressionConfig;
use crate::frame_codec::FrameCodec;
use crate::fs::inode::Inode;
use crate::fs::types::{AuthContext, FallocateMode};
use crate::fs::write_coordinator::WriteCoordinator;
use crate::fs::{ZeroFS, store::ExtentStore};
use crate::segment::SEGMENT_INFO;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
enum DataKind {
    Zero,
    Incompressible,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
enum Workload {
    CodecFrames,
    GrowingEofAppend,
    SparseFirstTouch,
    WarmOverwrite,
}

impl Workload {
    fn as_str(self) -> &'static str {
        match self {
            Self::CodecFrames => "codec_frames",
            Self::GrowingEofAppend => "growing_eof_append",
            Self::SparseFirstTouch => "sparse_first_touch",
            Self::WarmOverwrite => "warm_overwrite",
        }
    }
}

impl DataKind {
    fn as_str(self) -> &'static str {
        match self {
            Self::Zero => "zero",
            Self::Incompressible => "incompressible",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct BenchmarkCase {
    data_kind: DataKind,
    block_bytes: usize,
    concurrency: usize,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum PipelineStage {
    ExtentDirect,
    WriteCoordinator,
    FullFilesystem,
}

impl PipelineStage {
    fn as_str(self) -> &'static str {
        match self {
            Self::ExtentDirect => "extent_direct_db",
            Self::WriteCoordinator => "write_coordinator",
            Self::FullFilesystem => "full_filesystem",
        }
    }
}

#[derive(Clone, Copy, Debug)]
struct HarnessConfig {
    target_measured_bytes: usize,
    warmup_ops_per_worker: usize,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct MeasurementPlan {
    warmup_ops: usize,
    measured_ops: usize,
    measured_bytes: usize,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct OperationTarget {
    member: usize,
    offset: u64,
    old_size: u64,
}

fn member_ops(total_ops: usize, member: usize, members: usize) -> usize {
    if member >= total_ops {
        0
    } else {
        (total_ops - 1 - member) / members + 1
    }
}

fn operation_target(
    workload: Workload,
    operation: usize,
    total_ops: usize,
    members: usize,
    block_bytes: usize,
) -> OperationTarget {
    assert!(members > 0);
    assert!(matches!(
        workload,
        Workload::GrowingEofAppend | Workload::SparseFirstTouch | Workload::WarmOverwrite
    ));
    let member = operation % members;
    let member_operation = operation / members;
    let offset = (member_operation * block_bytes) as u64;
    let old_size = match workload {
        Workload::GrowingEofAppend => offset,
        Workload::SparseFirstTouch | Workload::WarmOverwrite => {
            (member_ops(total_ops, member, members) * block_bytes) as u64
        }
        _ => unreachable!(),
    };
    OperationTarget {
        member,
        offset,
        old_size,
    }
}

impl MeasurementPlan {
    fn new(
        block_bytes: usize,
        concurrency: usize,
        target_measured_bytes: usize,
        warmup_ops_per_worker: usize,
    ) -> Self {
        assert!(block_bytes > 0);
        assert!(concurrency > 0);
        let requested_ops = target_measured_bytes.div_ceil(block_bytes).max(concurrency);
        let measured_ops = requested_ops.div_ceil(concurrency) * concurrency;
        Self {
            warmup_ops: warmup_ops_per_worker * concurrency,
            measured_ops,
            measured_bytes: measured_ops * block_bytes,
        }
    }
}

#[derive(Clone, Debug, Serialize)]
struct BenchmarkRow {
    schema_version: u8,
    stage: String,
    workload: Workload,
    data_kind: DataKind,
    block_bytes: usize,
    concurrency: usize,
    effective_concurrency: usize,
    members: usize,
    warmup_ops: usize,
    warmup_bytes: usize,
    measured_ops: usize,
    measured_bytes: usize,
    measurement_role: &'static str,
    build_mode: &'static str,
    measured_validated_bytes: u64,
    final_state_validated_bytes: u64,
    elapsed_ns: u128,
    mib_per_second: f64,
    ops_per_second: f64,
}

#[derive(Clone, Copy, Debug)]
struct ValidationCounts {
    measured_bytes: u64,
    final_state_bytes: u64,
}

impl BenchmarkRow {
    fn new(
        stage: impl Into<String>,
        workload: Workload,
        case: BenchmarkCase,
        members: usize,
        plan: MeasurementPlan,
        elapsed: Duration,
        validation: ValidationCounts,
    ) -> Self {
        let seconds = elapsed.as_secs_f64().max(f64::EPSILON);
        let stage = stage.into();
        let measurement_role = if workload == Workload::CodecFrames || stage == "extent_direct_db" {
            "independent_baseline"
        } else {
            "cumulative_pipeline"
        };
        let effective_concurrency = if stage == "extent_direct_db" {
            1
        } else {
            case.concurrency
        };
        Self {
            schema_version: 1,
            stage,
            workload,
            data_kind: case.data_kind,
            block_bytes: case.block_bytes,
            concurrency: case.concurrency,
            effective_concurrency,
            members,
            warmup_ops: plan.warmup_ops,
            warmup_bytes: plan.warmup_ops * case.block_bytes,
            measured_ops: plan.measured_ops,
            measured_bytes: plan.measured_bytes,
            measurement_role,
            build_mode: detected_build_mode(),
            measured_validated_bytes: validation.measured_bytes,
            final_state_validated_bytes: validation.final_state_bytes,
            elapsed_ns: elapsed.as_nanos(),
            mib_per_second: plan.measured_bytes as f64 / (1024.0 * 1024.0) / seconds,
            ops_per_second: plan.measured_ops as f64 / seconds,
        }
    }
}

fn detected_build_mode() -> &'static str {
    if cfg!(debug_assertions) {
        "debug_assertions_enabled"
    } else {
        "release_no_debug_assertions"
    }
}

fn require_release_build() -> Result<()> {
    ensure!(
        !cfg!(debug_assertions),
        "performance harness requires --release (debug assertions must be disabled)"
    );
    Ok(())
}

fn render_json_lines(rows: &[BenchmarkRow]) -> String {
    let mut out = String::from("ZEROFS_PERF_JSONL_BEGIN\n");
    for row in rows {
        writeln!(out, "{}", serde_json::to_string(row).unwrap()).unwrap();
    }
    out.push_str("ZEROFS_PERF_JSONL_END\n");
    out
}

fn render_human_table(rows: &[BenchmarkRow]) -> String {
    let mut out = String::new();
    writeln!(out, "build_mode={}", detected_build_mode()).unwrap();
    writeln!(
        out,
        "measurement_roles are independent labels; rows are not subtractable stage costs"
    )
    .unwrap();
    writeln!(
        out,
        "{:<24} {:<21} {:<26} {:<14} {:>9} {:>4} {:>4} {:>7} {:>8} {:>10} {:>9} {:>10} {:>10} {:>10} {:>10} {:>11}",
        "stage",
        "role",
        "workload",
        "data",
        "block_KiB",
        "conc",
        "eff",
        "members",
        "warm_ops",
        "warm_MiB",
        "meas_ops",
        "meas_MiB",
        "elapsed_ms",
        "final_MiB",
        "MiB/s",
        "ops/s"
    )
    .unwrap();
    for row in rows {
        writeln!(
            out,
            "{:<24} {:<21} {:<26} {:<14} {:>9} {:>4} {:>4} {:>7} {:>8} {:>10.2} {:>9} {:>10.2} {:>10.2} {:>10.2} {:>10.2} {:>11.2}",
            row.stage,
            row.measurement_role,
            row.workload.as_str(),
            row.data_kind.as_str(),
            row.block_bytes / 1024,
            row.concurrency,
            row.effective_concurrency,
            row.members,
            row.warmup_ops,
            row.warmup_bytes as f64 / (1024.0 * 1024.0),
            row.measured_ops,
            row.measured_bytes as f64 / (1024.0 * 1024.0),
            row.elapsed_ns as f64 / 1_000_000.0,
            row.final_state_validated_bytes as f64 / (1024.0 * 1024.0),
            row.mib_per_second,
            row.ops_per_second
        )
        .unwrap();
    }
    out
}

struct SealedOperation {
    operation: usize,
    frames: Vec<Vec<u8>>,
}

async fn seal_codec_phase(
    codec: Arc<FrameCodec>,
    payload: Bytes,
    operations: std::ops::Range<usize>,
    concurrency: usize,
) -> Result<Vec<SealedOperation>> {
    stream::iter(operations)
        .map(|operation| {
            let codec = codec.clone();
            let payload = payload.clone();
            tokio::task::spawn_blocking(move || -> Result<SealedOperation> {
                let mut frames = Vec::with_capacity(payload.len().div_ceil(crate::fs::EXTENT_SIZE));
                for (frame_index, frame) in payload.chunks(crate::fs::EXTENT_SIZE).enumerate() {
                    let mut aad = [0u8; 16];
                    aad[..8].copy_from_slice(&(operation as u64).to_le_bytes());
                    aad[8..].copy_from_slice(&(frame_index as u64).to_le_bytes());
                    frames.push(codec.seal(frame, &aad)?);
                }
                Ok(SealedOperation { operation, frames })
            })
        })
        .buffer_unordered(concurrency)
        .map(|joined| match joined {
            Ok(result) => result,
            Err(error) => Err(error.into()),
        })
        .try_collect::<Vec<_>>()
        .await
}

fn validate_codec_phase(
    codec: &FrameCodec,
    payload: &Bytes,
    sealed: &[SealedOperation],
) -> Result<u64> {
    let mut validated = 0u64;
    for operation in sealed {
        ensure!(
            operation.frames.len() == payload.len().div_ceil(crate::fs::EXTENT_SIZE),
            "codec frame count mismatch"
        );
        for (frame_index, (frame, sealed_frame)) in payload
            .chunks(crate::fs::EXTENT_SIZE)
            .zip(&operation.frames)
            .enumerate()
        {
            let mut aad = [0u8; 16];
            aad[..8].copy_from_slice(&(operation.operation as u64).to_le_bytes());
            aad[8..].copy_from_slice(&(frame_index as u64).to_le_bytes());
            let opened = codec.open(sealed_frame, &aad)?;
            ensure!(opened.as_slice() == frame, "codec round-trip mismatch");
            validated += frame.len() as u64;
        }
    }
    Ok(validated)
}

async fn run_codec_case(case: BenchmarkCase, config: HarnessConfig) -> Result<BenchmarkRow> {
    let plan = MeasurementPlan::new(
        case.block_bytes,
        case.concurrency,
        config.target_measured_bytes,
        config.warmup_ops_per_worker,
    );
    let payload = Bytes::from(deterministic_data(case.data_kind, 0x5eed, case.block_bytes));
    let codec = Arc::new(FrameCodec::new(
        &[0x5a; 32],
        SEGMENT_INFO,
        CompressionConfig::default(),
    ));
    let warmup = seal_codec_phase(
        codec.clone(),
        payload.clone(),
        0..plan.warmup_ops,
        case.concurrency,
    )
    .await?;
    ensure!(
        validate_codec_phase(&codec, &payload, &warmup)?
            == plan.warmup_ops as u64 * case.block_bytes as u64
    );
    let started = Instant::now();
    let sealed = seal_codec_phase(
        codec.clone(),
        payload.clone(),
        plan.warmup_ops..plan.warmup_ops + plan.measured_ops,
        case.concurrency,
    )
    .await?;
    let elapsed = started.elapsed();
    let validated = validate_codec_phase(&codec, &payload, &sealed)?;
    ensure!(validated == plan.measured_bytes as u64);
    Ok(BenchmarkRow::new(
        "codec_zstd3",
        Workload::CodecFrames,
        case,
        1,
        plan,
        elapsed,
        ValidationCounts {
            measured_bytes: validated,
            final_state_bytes: 0,
        },
    ))
}

#[derive(Clone)]
struct ExtentPhase {
    store: ExtentStore,
    db: Arc<crate::db::Db>,
    coordinator: Option<WriteCoordinator>,
    workload: Workload,
    payload: Bytes,
    total_ops: usize,
    members: usize,
    concurrency: usize,
}

impl ExtentPhase {
    async fn run(&self, operations: std::ops::Range<usize>) -> Result<u64> {
        let block_bytes = self.payload.len();
        let completed = stream::iter(operations)
            .map(|operation| {
                let store = self.store.clone();
                let db = self.db.clone();
                let coordinator = self.coordinator.clone();
                let payload = self.payload.clone();
                let workload = self.workload;
                let total_ops = self.total_ops;
                let members = self.members;
                async move {
                    let target =
                        operation_target(workload, operation, total_ops, members, block_bytes);
                    let inode = target.member as u64 + 1;
                    match coordinator {
                        Some(coordinator) => {
                            let mut txn = db.new_transaction()?;
                            let tail = store
                                .write(
                                    &mut txn,
                                    inode,
                                    target.offset,
                                    &payload,
                                    target.old_size,
                                )
                                .await?;
                            coordinator.commit(txn).await?;
                            store.apply_tail_update(inode, tail);
                        }
                        None => {
                            super::test_util::write_committed(
                                &store,
                                &db,
                                inode,
                                target.offset,
                                &payload,
                                target.old_size,
                            )
                            .await;
                        }
                    }
                    Ok::<u64, anyhow::Error>(block_bytes as u64)
                }
            })
            .buffer_unordered(effective_extent_concurrency(
                self.coordinator.as_ref(),
                self.concurrency,
            ))
            .try_collect::<Vec<_>>()
            .await?;
        Ok(completed.into_iter().sum())
    }
}

fn effective_extent_concurrency(coordinator: Option<&WriteCoordinator>, requested: usize) -> usize {
    if coordinator.is_some() { requested } else { 1 }
}

async fn validate_extent_operations(
    store: &ExtentStore,
    payload: &Bytes,
    operations: std::ops::Range<usize>,
    total_ops: usize,
    members: usize,
    workload: Workload,
) -> Result<u64> {
    let mut validated = 0;
    for operation in operations {
        let target = operation_target(workload, operation, total_ops, members, payload.len());
        let actual = store
            .read(
                target.member as u64 + 1,
                target.offset,
                payload.len() as u64,
            )
            .await?;
        ensure!(actual == *payload, "measured extent bytes mismatch");
        validated += actual.len() as u64;
    }
    Ok(validated)
}

async fn validate_segment_counters(store: &ExtentStore) -> Result<()> {
    use std::sync::atomic::Ordering::Relaxed;

    let footprint = store.sample_footprint().await?;
    ensure!(footprint.appended_bytes >= footprint.live_bytes);
    ensure!(
        footprint.reclaimable_bytes == footprint.appended_bytes - footprint.live_bytes,
        "segment footprint reclaimable bytes mismatch"
    );
    let stats = store.segment_gc_stats();
    ensure!(stats.segment_count.load(Relaxed) == footprint.segment_count);
    ensure!(stats.appended_bytes.load(Relaxed) == footprint.appended_bytes);
    ensure!(stats.live_bytes.load(Relaxed) == footprint.live_bytes);
    ensure!(stats.reclaimable_bytes.load(Relaxed) == footprint.reclaimable_bytes);
    Ok(())
}

async fn validate_extent_state(
    store: &ExtentStore,
    payload: &Bytes,
    total_ops: usize,
    members: usize,
) -> Result<u64> {
    let mut validated = 0u64;
    for member in 0..members {
        let operations = member_ops(total_ops, member, members);
        let expected = payload.repeat(operations);
        let actual = store
            .read(member as u64 + 1, 0, expected.len() as u64)
            .await?;
        ensure!(
            actual.as_ref() == expected.as_slice(),
            "extent state mismatch"
        );
        validated += actual.len() as u64;
    }
    Ok(validated)
}

async fn run_extent_pipeline_case(
    stage: PipelineStage,
    workload: Workload,
    case: BenchmarkCase,
    members: usize,
    config: HarnessConfig,
) -> Result<BenchmarkRow> {
    let plan = MeasurementPlan::new(
        case.block_bytes,
        case.concurrency,
        config.target_measured_bytes,
        config.warmup_ops_per_worker,
    );
    let total_ops = plan.warmup_ops + plan.measured_ops;
    let payload = Bytes::from(deterministic_data(case.data_kind, 0x6eed, case.block_bytes));
    let (store, db, coordinator) = match stage {
        PipelineStage::ExtentDirect => {
            let (store, db, _) =
                super::test_util::make_with_compression(CompressionConfig::default()).await;
            (store, db, None)
        }
        PipelineStage::WriteCoordinator => {
            let fs = ZeroFS::new_in_memory().await?;
            (
                fs.extent_store.clone(),
                fs.db.clone(),
                Some(fs.write_coordinator.clone()),
            )
        }
        PipelineStage::FullFilesystem => unreachable!(),
    };

    let phase = ExtentPhase {
        store: store.clone(),
        db,
        coordinator,
        workload,
        payload: payload.clone(),
        total_ops,
        members,
        concurrency: case.concurrency,
    };
    if workload == Workload::WarmOverwrite {
        let prefill = ExtentPhase {
            workload: Workload::SparseFirstTouch,
            payload: Bytes::from(warm_prefill_data(case.data_kind, case.block_bytes)),
            ..phase.clone()
        };
        prefill.run(0..total_ops).await?;
    }
    phase.run(0..plan.warmup_ops).await?;
    let started = Instant::now();
    let completed = phase.run(plan.warmup_ops..total_ops).await?;
    let elapsed = started.elapsed();
    ensure!(completed == plan.measured_bytes as u64);
    let measured_validated = validate_extent_operations(
        &store,
        &payload,
        plan.warmup_ops..total_ops,
        total_ops,
        members,
        workload,
    )
    .await?;
    ensure!(measured_validated == plan.measured_bytes as u64);
    let validated = validate_extent_state(&store, &payload, total_ops, members).await?;
    ensure!(validated == total_ops as u64 * case.block_bytes as u64);
    validate_segment_counters(&store).await?;

    Ok(BenchmarkRow::new(
        stage.as_str(),
        workload,
        case,
        members,
        plan,
        elapsed,
        ValidationCounts {
            measured_bytes: measured_validated,
            final_state_bytes: validated,
        },
    ))
}

#[derive(Clone)]
struct FilesystemPhase {
    fs: Arc<ZeroFS>,
    auth: AuthContext,
    inodes: Arc<Vec<u64>>,
    payload: Bytes,
    total_ops: usize,
    workload: Workload,
    concurrency: usize,
}

async fn validate_filesystem_operations(
    fs: &ZeroFS,
    auth: &AuthContext,
    inodes: &[u64],
    payload: &Bytes,
    operations: std::ops::Range<usize>,
    total_ops: usize,
    workload: Workload,
) -> Result<u64> {
    let mut validated = 0;
    for operation in operations {
        let target = operation_target(workload, operation, total_ops, inodes.len(), payload.len());
        let (actual, _) = fs
            .read_file(
                auth,
                inodes[target.member],
                target.offset,
                payload.len() as u32,
            )
            .await?;
        ensure!(actual == *payload, "measured filesystem bytes mismatch");
        validated += actual.len() as u64;
    }
    Ok(validated)
}

impl FilesystemPhase {
    async fn run(&self, operations: std::ops::Range<usize>) -> Result<u64> {
        let members = self.inodes.len();
        let block_bytes = self.payload.len();
        let completed = stream::iter(operations)
            .map(|operation| {
                let fs = self.fs.clone();
                let auth = self.auth.clone();
                let inodes = self.inodes.clone();
                let payload = self.payload.clone();
                let workload = self.workload;
                let total_ops = self.total_ops;
                async move {
                    let target =
                        operation_target(workload, operation, total_ops, members, block_bytes);
                    let attrs = fs
                        .write(&auth, inodes[target.member], target.offset, &payload)
                        .await?;
                    ensure!(attrs.size >= target.offset + block_bytes as u64);
                    Ok::<u64, anyhow::Error>(block_bytes as u64)
                }
            })
            .buffer_unordered(self.concurrency)
            .try_collect::<Vec<_>>()
            .await?;
        Ok(completed.into_iter().sum())
    }
}

async fn validate_filesystem_state(
    fs: &ZeroFS,
    auth: &AuthContext,
    inodes: &[u64],
    payload: &Bytes,
    total_ops: usize,
) -> Result<u64> {
    let mut validated = 0u64;
    for (member, inode_id) in inodes.iter().copied().enumerate() {
        let operations = member_ops(total_ops, member, inodes.len());
        let expected_size = (operations * payload.len()) as u64;
        let inode = fs.inode_store.get(inode_id).await?;
        let Inode::File(file) = inode else {
            anyhow::bail!("benchmark member is not a file");
        };
        ensure!(file.size == expected_size, "filesystem size mismatch");
        for operation in 0..operations {
            let offset = (operation * payload.len()) as u64;
            let (actual, _) = fs
                .read_file(auth, inode_id, offset, payload.len() as u32)
                .await?;
            ensure!(actual == *payload, "filesystem data mismatch");
            validated += actual.len() as u64;
        }
    }
    Ok(validated)
}

async fn run_full_filesystem_case(
    workload: Workload,
    case: BenchmarkCase,
    members: usize,
    config: HarnessConfig,
) -> Result<BenchmarkRow> {
    let plan = MeasurementPlan::new(
        case.block_bytes,
        case.concurrency,
        config.target_measured_bytes,
        config.warmup_ops_per_worker,
    );
    let total_ops = plan.warmup_ops + plan.measured_ops;
    let payload = Bytes::from(deterministic_data(case.data_kind, 0x7eed, case.block_bytes));
    let fs = Arc::new(ZeroFS::new_in_memory().await?);
    let auth = AuthContext::default();
    let mut inode_ids = Vec::with_capacity(members);
    for member in 0..members {
        inode_ids.push(
            fs.create_exclusive(&auth, 0, format!("perf-{member}").as_bytes())
                .await?,
        );
    }
    if workload == Workload::SparseFirstTouch {
        for (member, inode_id) in inode_ids.iter().copied().enumerate() {
            let size = (member_ops(total_ops, member, members) * case.block_bytes) as u64;
            if size > 0 {
                fs.fallocate_opened(&auth, inode_id, 0, size, FallocateMode::Allocate)
                    .await?;
            }
        }
    }
    let inodes = Arc::new(inode_ids);
    let phase = FilesystemPhase {
        fs: fs.clone(),
        auth: auth.clone(),
        inodes: inodes.clone(),
        payload: payload.clone(),
        total_ops,
        workload,
        concurrency: case.concurrency,
    };
    if workload == Workload::WarmOverwrite {
        let prefill = FilesystemPhase {
            workload: Workload::SparseFirstTouch,
            payload: Bytes::from(warm_prefill_data(case.data_kind, case.block_bytes)),
            ..phase.clone()
        };
        prefill.run(0..total_ops).await?;
    }
    phase.run(0..plan.warmup_ops).await?;
    let started = Instant::now();
    let completed = phase.run(plan.warmup_ops..total_ops).await?;
    let elapsed = started.elapsed();
    ensure!(completed == plan.measured_bytes as u64);
    let measured_validated = validate_filesystem_operations(
        &fs,
        &auth,
        &inodes,
        &payload,
        plan.warmup_ops..total_ops,
        total_ops,
        workload,
    )
    .await?;
    ensure!(measured_validated == plan.measured_bytes as u64);
    let validated = validate_filesystem_state(&fs, &auth, &inodes, &payload, total_ops).await?;
    ensure!(validated == total_ops as u64 * case.block_bytes as u64);
    validate_segment_counters(&fs.extent_store).await?;

    Ok(BenchmarkRow::new(
        PipelineStage::FullFilesystem.as_str(),
        workload,
        case,
        members,
        plan,
        elapsed,
        ValidationCounts {
            measured_bytes: measured_validated,
            final_state_bytes: validated,
        },
    ))
}

async fn run_pipeline_case(
    stage: PipelineStage,
    workload: Workload,
    case: BenchmarkCase,
    members: usize,
    config: HarnessConfig,
) -> Result<BenchmarkRow> {
    ensure!(members == 1 || members == 4, "members must be one or four");
    ensure!(matches!(
        workload,
        Workload::GrowingEofAppend | Workload::SparseFirstTouch | Workload::WarmOverwrite
    ));
    match stage {
        PipelineStage::ExtentDirect | PipelineStage::WriteCoordinator => {
            run_extent_pipeline_case(stage, workload, case, members, config).await
        }
        PipelineStage::FullFilesystem => {
            run_full_filesystem_case(workload, case, members, config).await
        }
    }
    .with_context(|| {
        format!(
            "{} {} {} byte blocks at concurrency {} with {} members",
            stage.as_str(),
            workload.as_str(),
            case.block_bytes,
            case.concurrency,
            members
        )
    })
}

fn harness_config_from_env() -> Result<HarnessConfig> {
    let measured_mib = std::env::var("ZEROFS_PERF_MIB")
        .ok()
        .map(|value| value.parse::<usize>())
        .transpose()
        .context("ZEROFS_PERF_MIB must be a positive integer")?
        .unwrap_or(8);
    let warmup_ops_per_worker = std::env::var("ZEROFS_PERF_WARMUP_OPS")
        .ok()
        .map(|value| value.parse::<usize>())
        .transpose()
        .context("ZEROFS_PERF_WARMUP_OPS must be a non-negative integer")?
        .unwrap_or(1);
    ensure!(measured_mib > 0, "ZEROFS_PERF_MIB must be positive");
    Ok(HarnessConfig {
        target_measured_bytes: measured_mib * 1024 * 1024,
        warmup_ops_per_worker,
    })
}

// Cumulative in-process write-pipeline throughput harness. This is deliberately
// ignored: it has no performance pass/fail threshold, but every row validates
// exact bytes and final state before it is printed.
//
// Quick receipt:
//   ZEROFS_PERF_MIB=1 ZEROFS_PERF_WARMUP_OPS=1 \
//     cargo test --release --locked --lib cumulative_write_pipeline_throughput \
//       -- --ignored --nocapture
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "release-only cumulative throughput measurement; run explicitly"]
async fn cumulative_write_pipeline_throughput() {
    require_release_build().expect("release performance harness build");
    let config = harness_config_from_env().expect("valid performance harness configuration");
    let mut rows = Vec::new();

    for case in matrix_cases() {
        rows.push(
            run_codec_case(case, config)
                .await
                .expect("codec stage must validate"),
        );
        for workload in [
            Workload::GrowingEofAppend,
            Workload::SparseFirstTouch,
            Workload::WarmOverwrite,
        ] {
            for stage in [
                PipelineStage::ExtentDirect,
                PipelineStage::WriteCoordinator,
                PipelineStage::FullFilesystem,
            ] {
                for members in [1, 4] {
                    rows.push(
                        run_pipeline_case(stage, workload, case, members, config)
                            .await
                            .expect("pipeline stage must validate"),
                    );
                }
            }
        }
    }

    println!("{}", render_human_table(&rows));
    print!("{}", render_json_lines(&rows));
}

fn matrix_cases() -> Vec<BenchmarkCase> {
    let mut cases = Vec::with_capacity(18);
    for data_kind in [DataKind::Incompressible, DataKind::Zero] {
        for block_bytes in [32 * 1024, 256 * 1024, 1024 * 1024] {
            for concurrency in [1, 4, 8] {
                cases.push(BenchmarkCase {
                    data_kind,
                    block_bytes,
                    concurrency,
                });
            }
        }
    }
    cases
}

fn deterministic_data(kind: DataKind, seed: u64, len: usize) -> Vec<u8> {
    match kind {
        DataKind::Zero => vec![0; len],
        DataKind::Incompressible => {
            let mut state = seed ^ 0x243f_6a88_85a3_08d3;
            (0..len)
                .map(|_| {
                    state ^= state << 13;
                    state ^= state >> 7;
                    state ^= state << 17;
                    (state >> 24) as u8
                })
                .collect()
        }
    }
}

fn warm_prefill_data(_measured_kind: DataKind, len: usize) -> Vec<u8> {
    deterministic_data(DataKind::Incompressible, 0x8eed, len)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deterministic_payloads_cover_zero_and_seeded_incompressible_data() {
        assert_eq!(deterministic_data(DataKind::Zero, 7, 16), vec![0; 16]);

        let first = deterministic_data(DataKind::Incompressible, 7, 64);
        assert_eq!(first, deterministic_data(DataKind::Incompressible, 7, 64));
        assert_ne!(first, deterministic_data(DataKind::Incompressible, 8, 64));
        assert!(first.iter().any(|byte| *byte != 0));
        for measured_kind in [DataKind::Zero, DataKind::Incompressible] {
            let prefill = warm_prefill_data(measured_kind, 64);
            assert!(prefill.iter().any(|byte| *byte != 0));
            assert_ne!(
                prefill,
                deterministic_data(DataKind::Incompressible, 0x6eed, 64)
            );
            assert_ne!(
                prefill,
                deterministic_data(DataKind::Incompressible, 0x7eed, 64)
            );
        }
    }

    #[test]
    fn matrix_covers_each_data_size_and_concurrency_combination() {
        let cases = matrix_cases();
        assert_eq!(cases.len(), 18);
        assert_eq!(
            cases.first(),
            Some(&BenchmarkCase {
                data_kind: DataKind::Incompressible,
                block_bytes: 32 * 1024,
                concurrency: 1,
            })
        );
        assert_eq!(
            cases.last(),
            Some(&BenchmarkCase {
                data_kind: DataKind::Zero,
                block_bytes: 1024 * 1024,
                concurrency: 8,
            })
        );
    }

    #[test]
    fn measurement_plan_rounds_work_to_complete_concurrency_waves() {
        let small = MeasurementPlan::new(32 * 1024, 4, 1024 * 1024, 2);
        assert_eq!(small.warmup_ops, 8);
        assert_eq!(small.measured_ops, 32);
        assert_eq!(small.measured_bytes, 1024 * 1024);

        let one_mib = MeasurementPlan::new(1024 * 1024, 8, 3 * 1024 * 1024, 1);
        assert_eq!(one_mib.warmup_ops, 8);
        assert_eq!(one_mib.measured_ops, 8);
        assert_eq!(one_mib.measured_bytes, 8 * 1024 * 1024);
    }

    #[test]
    fn result_accounting_and_both_output_formats_are_machine_checkable() {
        let case = BenchmarkCase {
            data_kind: DataKind::Incompressible,
            block_bytes: 32 * 1024,
            concurrency: 4,
        };
        let plan = MeasurementPlan::new(case.block_bytes, case.concurrency, 1024 * 1024, 2);
        let row = BenchmarkRow::new(
            "codec",
            Workload::CodecFrames,
            case,
            1,
            plan,
            std::time::Duration::from_secs(2),
            ValidationCounts {
                measured_bytes: plan.measured_bytes as u64,
                final_state_bytes: 0,
            },
        );
        assert_eq!(row.elapsed_ns, 2_000_000_000);
        assert_eq!(row.ops_per_second, 16.0);
        assert_eq!(row.mib_per_second, 0.5);
        assert_eq!(row.measurement_role, "independent_baseline");
        assert_eq!(row.effective_concurrency, 4);
        assert_eq!(row.build_mode, detected_build_mode());
        assert_eq!(row.measured_validated_bytes, 1024 * 1024);
        assert_eq!(row.final_state_validated_bytes, 0);

        let jsonl = render_json_lines(std::slice::from_ref(&row));
        let encoded = jsonl.lines().nth(1).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(encoded).unwrap();
        assert_eq!(parsed["schema_version"], 1);
        assert_eq!(parsed["stage"], "codec");
        assert_eq!(parsed["data_kind"], "incompressible");
        assert_eq!(parsed["measured_bytes"], 1024 * 1024);
        assert_eq!(parsed["measurement_role"], "independent_baseline");
        assert!(jsonl.ends_with("ZEROFS_PERF_JSONL_END\n"));

        let table = render_human_table(&[row]);
        assert!(table.contains("stage"));
        assert!(table.contains("warm_MiB"));
        assert!(table.contains("meas_MiB"));
        assert!(table.contains("codec"));
        assert!(table.contains("0.50"));
    }

    #[test]
    fn operation_targets_distinguish_append_first_touch_and_warm_overwrite() {
        let growing = operation_target(Workload::GrowingEofAppend, 5, 12, 4, 256 * 1024);
        assert_eq!(growing.member, 1);
        assert_eq!(growing.offset, 256 * 1024);
        assert_eq!(growing.old_size, 256 * 1024);

        let sparse = operation_target(Workload::SparseFirstTouch, 5, 12, 4, 256 * 1024);
        assert_eq!(sparse.member, 1);
        assert_eq!(sparse.offset, 256 * 1024);
        assert_eq!(sparse.old_size, 3 * 256 * 1024);

        let warm = operation_target(Workload::WarmOverwrite, 5, 12, 4, 256 * 1024);
        assert_eq!(warm.member, 1);
        assert_eq!(warm.offset, 256 * 1024);
        assert_eq!(warm.old_size, 3 * 256 * 1024);
    }

    #[test]
    fn direct_extent_fallback_is_always_serialized() {
        assert_eq!(effective_extent_concurrency(None, 8), 1);
        let case = BenchmarkCase {
            data_kind: DataKind::Zero,
            block_bytes: 32 * 1024,
            concurrency: 8,
        };
        let plan = MeasurementPlan::new(case.block_bytes, case.concurrency, 1024 * 1024, 1);
        let row = BenchmarkRow::new(
            "extent_direct_db",
            Workload::GrowingEofAppend,
            case,
            1,
            plan,
            Duration::from_secs(1),
            ValidationCounts {
                measured_bytes: plan.measured_bytes as u64,
                final_state_bytes: plan.measured_bytes as u64,
            },
        );
        assert_eq!(row.concurrency, 8);
        assert_eq!(row.effective_concurrency, 1);
    }

    #[test]
    fn build_mode_matches_release_enforcement() {
        assert_eq!(require_release_build().is_ok(), !cfg!(debug_assertions));
        assert_eq!(
            detected_build_mode(),
            if cfg!(debug_assertions) {
                "debug_assertions_enabled"
            } else {
                "release_no_debug_assertions"
            }
        );
    }

    #[tokio::test]
    async fn warm_overwrite_prefill_is_allocated_and_debits_the_old_frame() {
        let (store, db, _) =
            super::super::test_util::make_with_compression(CompressionConfig::default()).await;
        let prefill = Bytes::from(warm_prefill_data(
            DataKind::Incompressible,
            crate::fs::EXTENT_SIZE,
        ));
        let measured = Bytes::from(deterministic_data(
            DataKind::Incompressible,
            0x6eed,
            crate::fs::EXTENT_SIZE,
        ));
        assert_ne!(prefill, measured);

        super::super::test_util::write_committed(
            &store,
            &db,
            1,
            0,
            &prefill,
            crate::fs::EXTENT_SIZE as u64,
        )
        .await;
        let old = super::super::test_util::frameloc_of(&store, &db, 1, 0)
            .await
            .expect("warm prefill must allocate a physical frame");
        assert_eq!(
            super::super::test_util::segcount_pair_of(&store, &db, old.segid).await,
            (old.byte_len as u64, old.byte_len as u64)
        );

        super::super::test_util::write_committed(
            &store,
            &db,
            1,
            0,
            &measured,
            crate::fs::EXTENT_SIZE as u64,
        )
        .await;
        let new = super::super::test_util::frameloc_of(&store, &db, 1, 0)
            .await
            .expect("measured overwrite must remain physically allocated");
        let (old_live, old_total) =
            super::super::test_util::segcount_pair_of(&store, &db, old.segid).await;
        let expected_old_live = if new.segid == old.segid {
            new.byte_len as u64
        } else {
            0
        };
        assert_eq!(old_live, expected_old_live);
        assert_eq!(old_total - old_live, old.byte_len as u64);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn tiny_cumulative_pipeline_validates_all_write_layouts() {
        let case = BenchmarkCase {
            data_kind: DataKind::Incompressible,
            block_bytes: 32 * 1024,
            concurrency: 1,
        };
        let config = HarnessConfig {
            target_measured_bytes: 64 * 1024,
            warmup_ops_per_worker: 1,
        };

        let codec = run_codec_case(case, config).await.unwrap();
        assert_eq!(codec.measured_bytes, 64 * 1024);
        assert_eq!(codec.measured_validated_bytes, 64 * 1024);
        assert_eq!(codec.final_state_validated_bytes, 0);

        for workload in [
            Workload::GrowingEofAppend,
            Workload::SparseFirstTouch,
            Workload::WarmOverwrite,
        ] {
            for stage in [
                PipelineStage::ExtentDirect,
                PipelineStage::WriteCoordinator,
                PipelineStage::FullFilesystem,
            ] {
                let row = run_pipeline_case(stage, workload, case, 1, config)
                    .await
                    .unwrap();
                assert_eq!(row.measured_bytes, 64 * 1024);
                assert_eq!(row.measured_validated_bytes, 64 * 1024);
                assert_eq!(row.final_state_validated_bytes, 3 * 32 * 1024);
            }
        }
    }
}
