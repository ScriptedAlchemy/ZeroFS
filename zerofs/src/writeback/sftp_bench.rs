use crate::config::Settings;
use crate::segment_store::GeneratedSegmentCreate;
use crate::sftp_object_store::SftpObjectStore;
use crate::sftp_transport::{
    OperationKind, RusshSessionFactory, SessionFactory, SftpSessionPool, TransportError,
};
use crate::writeback::config::{AckMode, ShutdownFlush, WritebackAccessMode, WritebackSettings};
use crate::writeback::journal::Journal;
use crate::writeback::model::{JournalIdentity, LocalEtag, MutationRecord};
use crate::writeback::store::WritebackObjectStore;
use crate::writeback::test_util::WriteAdmissionTestControl;
use anyhow::{Context, Result};
use bytes::Bytes;
use object_store::path::Path as ObjectPath;
use object_store::{ObjectStore, ObjectStoreExt, PutMode, PutOptions};
use rand::{RngCore, SeedableRng, rngs::StdRng};
use serde::Serialize;
use sha2::{Digest, Sha256};
use std::collections::BTreeSet;
use std::fs;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};
use url::Url;
use uuid::Uuid;

async fn build_remote_store(
    settings: &Settings,
) -> Result<(Arc<dyn ObjectStore>, ObjectPath, SftpSessionPool)> {
    let endpoint = settings
        .sftp_endpoint()?
        .context("benchmark config must use an sftp:// storage URL")?;
    let url = Url::parse(&settings.storage.url).context("parse benchmark SFTP URL")?;
    let path = ObjectPath::from_url_path(url.path())?;
    SftpObjectStore::validate_prefix(&path)?;
    let config = settings.sftp.clone().unwrap_or_default();
    let factory: Arc<dyn SessionFactory> = Arc::new(RusshSessionFactory::new(
        endpoint.clone(),
        config.identity_file.clone(),
        config.known_hosts.clone(),
    )?);
    let pool = SftpSessionPool::from_config_writable(factory, &config).await?;
    let store = SftpObjectStore::new(pool.clone(), path.clone())?;
    Ok((Arc::new(store), path, pool))
}

const CONFIG_ENV: &str = "ZEROFS_SFTP_WRITEBACK_BENCH_CONFIG";
const TOTAL_MIB_ENV: &str = "ZEROFS_BENCH_SFTP_TOTAL_MIB";
const PAYLOAD_KIB_ENV: &str = "ZEROFS_BENCH_SFTP_PAYLOAD_KIB";
const MANIFEST_KIB_ENV: &str = "ZEROFS_BENCH_SFTP_MANIFEST_KIB";
const WRITERS_ENV: &str = "ZEROFS_BENCH_SFTP_WRITERS";
const MAX_CONNECTIONS_ENV: &str = "ZEROFS_BENCH_SFTP_MAX_CONNECTIONS";
const SSD_MIB_ENV: &str = "ZEROFS_BENCH_SFTP_SSD_MIB";
const FENCE_EVERY_ENV: &str = "ZEROFS_BENCH_SFTP_FENCE_EVERY";
const BENCH_DIR_ENV: &str = "ZEROFS_BENCH_DIR";
const IDENTITY_FILE_ENV: &str = "ZEROFS_BENCH_SFTP_IDENTITY_FILE";
const KNOWN_HOSTS_ENV: &str = "ZEROFS_BENCH_SFTP_KNOWN_HOSTS";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BenchObjectClass {
    Segment,
    Manifest,
}

#[derive(Debug)]
struct BenchObjectSet {
    database_prefix: ObjectPath,
    objects: Vec<ObjectPath>,
    classes: Vec<BenchObjectClass>,
    directories_deepest_first: Vec<PathBuf>,
    fence_objects: usize,
}

impl BenchObjectSet {
    fn new(base: &ObjectPath, run_id: Uuid, object_count: usize) -> Result<Self> {
        Self::new_with_fences(base, run_id, object_count, 0)
    }

    fn new_with_fences(
        base: &ObjectPath,
        run_id: Uuid,
        object_count: usize,
        fence_every: usize,
    ) -> Result<Self> {
        let token = run_id.simple().to_string();
        let database_prefix = ObjectPath::parse(format!("{base}/.zerofs-writeback-bench-{token}"))?;
        let epoch = u64::from_str_radix(&token[..16], 16)?;
        let mut objects = Vec::with_capacity(object_count);
        let mut classes = Vec::with_capacity(object_count);
        let mut directories = BTreeSet::new();
        directories.insert(PathBuf::from(database_prefix.as_ref()));
        directories.insert(PathBuf::from(format!("{database_prefix}/segments")));
        let mut fence_objects = 0;

        for index in 0..object_count {
            let counter = u64::try_from(index)?.saturating_add(1);
            if fence_every != 0 && (index + 1).is_multiple_of(fence_every) {
                directories.insert(PathBuf::from(format!("{database_prefix}/manifest")));
                objects.push(ObjectPath::parse(format!(
                    "{database_prefix}/manifest/{counter:020}.manifest"
                ))?);
                classes.push(BenchObjectClass::Manifest);
                fence_objects += 1;
                continue;
            }
            let shard = counter & 0xff;
            let shard_dir = format!("{database_prefix}/segments/{shard:02x}");
            let generation_dir = format!("{shard_dir}/{epoch:016x}");
            directories.insert(PathBuf::from(&shard_dir));
            directories.insert(PathBuf::from(&generation_dir));
            objects.push(ObjectPath::parse(format!(
                "{generation_dir}/{counter:016x}"
            ))?);
            classes.push(BenchObjectClass::Segment);
        }

        let mut directories_deepest_first: Vec<_> = directories.into_iter().collect();
        directories_deepest_first.sort_by(|left, right| {
            right
                .components()
                .count()
                .cmp(&left.components().count())
                .then_with(|| right.cmp(left))
        });

        Ok(Self {
            database_prefix,
            objects,
            classes,
            directories_deepest_first,
            fence_objects,
        })
    }
}

#[derive(Debug, Serialize)]
struct BenchReport {
    total_bytes: u64,
    object_count: usize,
    payload_bytes: usize,
    manifest_payload_bytes: usize,
    segment_objects: usize,
    manifest_objects: usize,
    writers: usize,
    max_connections: usize,
    upload_concurrency: usize,
    local_concurrency: usize,
    fence_objects: usize,
    ram_ack_seconds: f64,
    ram_ack_mib_per_second: f64,
    local_seconds: f64,
    local_mib_per_second: f64,
    remote_directory_prepare_seconds: f64,
    remote_drain_seconds: f64,
    remote_drain_mib_per_second: f64,
    remote_read_seconds: f64,
    remote_read_mib_per_second: f64,
    remote_read_verified_objects: usize,
    sftp_write_handles_closed: u64,
    sftp_open_total_seconds: f64,
    sftp_publication_window_total_seconds: f64,
    sftp_hardlink_total_seconds: f64,
    sftp_remove_total_seconds: f64,
    sftp_session_write_handles_closed: Vec<u64>,
    sftp_session_write_bytes: Vec<u64>,
    payload_sha256: String,
    manifest_payload_sha256: String,
}

#[derive(Debug, Serialize)]
struct SaturationReport {
    total_bytes: u64,
    object_count: usize,
    payload_bytes: usize,
    manifest_payload_bytes: usize,
    segment_objects: usize,
    manifest_objects: usize,
    writers: usize,
    max_connections: usize,
    upload_concurrency: usize,
    local_concurrency: usize,
    ssd_capacity_bytes: u64,
    ssd_high_watermark_bytes: u64,
    ssd_reservation_bytes_min: u64,
    ssd_reservation_bytes_max: u64,
    prefill_objects: usize,
    prefill_reserved_bytes: u64,
    tail_objects_after_activation: usize,
    prefill_seconds: f64,
    remote_directory_prepare_seconds: f64,
    first_blocked_ack_seconds: f64,
    blocked_ack_seconds: f64,
    blocked_ack_mib_per_second: f64,
    max_blocked_ack_gap_seconds: f64,
    remote_cleanup_bytes_during_blocked_acks: u64,
    remote_cleanup_mib_per_second: f64,
    foreground_to_remote_rate_ratio: f64,
    remote_drain_seconds: f64,
    remote_drain_mib_per_second: f64,
    remote_read_seconds: f64,
    remote_read_mib_per_second: f64,
    remote_read_verified_objects: usize,
    sftp_write_handles_closed: u64,
    sftp_open_total_seconds: f64,
    sftp_publication_window_total_seconds: f64,
    sftp_hardlink_total_seconds: f64,
    sftp_remove_total_seconds: f64,
    sftp_session_write_handles_closed: Vec<u64>,
    sftp_session_write_bytes: Vec<u64>,
    payload_sha256: String,
    manifest_payload_sha256: String,
}

#[derive(Debug, Clone, Copy)]
struct BenchGeometry {
    writers: usize,
    max_connections: usize,
    upload_concurrency: usize,
    local_concurrency: usize,
    remote_directory_prepare: Duration,
}

#[derive(Debug, Clone, Copy)]
struct SaturationGeometry {
    reservation_bytes_min: u64,
    reservation_bytes_max: u64,
    high_watermark_bytes: u64,
    prefill_reserved_bytes: u64,
    prefill_objects: usize,
    paced_objects: usize,
    paced_payload_bytes: u64,
}

impl SaturationGeometry {
    fn reservation_bytes(path: &ObjectPath, payload_bytes: usize) -> Result<u64> {
        MutationRecord::ssd_reservation_estimate(path.as_ref(), None, u64::try_from(payload_bytes)?)
            .context("estimate benchmark SSD reservation")
    }

    fn new(
        objects: &[ObjectPath],
        classes: &[BenchObjectClass],
        segment_payload_bytes: usize,
        manifest_payload_bytes: usize,
        disk_bytes: u64,
        high_watermark_percent: u8,
    ) -> Result<Self> {
        anyhow::ensure!(!objects.is_empty(), "saturation benchmark needs objects");
        anyhow::ensure!(
            objects.len() == classes.len(),
            "saturation object classes must match object paths"
        );
        anyhow::ensure!(
            segment_payload_bytes > 0 && manifest_payload_bytes > 0,
            "saturation payloads must be positive"
        );
        anyhow::ensure!(
            (1..=100).contains(&high_watermark_percent),
            "saturation high watermark must be between 1 and 100"
        );
        let reservation_bytes = objects
            .iter()
            .zip(classes.iter().copied())
            .map(|(path, class)| {
                let payload_bytes = match class {
                    BenchObjectClass::Segment => segment_payload_bytes,
                    BenchObjectClass::Manifest => manifest_payload_bytes,
                };
                Self::reservation_bytes(path, payload_bytes)
            })
            .collect::<Result<Vec<_>>>()?;
        let high_watermark_bytes =
            u64::try_from(u128::from(disk_bytes) * u128::from(high_watermark_percent) / 100)?;
        let mut prefill_reserved_bytes = 0_u64;
        let mut prefill_objects = 0_usize;
        for reservation in &reservation_bytes {
            let Some(next) = prefill_reserved_bytes.checked_add(*reservation) else {
                anyhow::bail!("saturation prefill reservation overflowed");
            };
            if next > high_watermark_bytes {
                break;
            }
            prefill_reserved_bytes = next;
            prefill_objects += 1;
        }
        anyhow::ensure!(
            prefill_objects > 0,
            "saturation SSD budget cannot admit one payload"
        );
        anyhow::ensure!(
            prefill_objects < objects.len(),
            "saturation workload must exceed the SSD high watermark"
        );
        let paced_payload_bytes =
            classes[prefill_objects..]
                .iter()
                .try_fold(0_u64, |total, class| {
                    let payload = match class {
                        BenchObjectClass::Segment => segment_payload_bytes,
                        BenchObjectClass::Manifest => manifest_payload_bytes,
                    };
                    total
                        .checked_add(u64::try_from(payload)?)
                        .context("saturation tail payload overflowed")
                })?;
        let reservation_bytes_min = *reservation_bytes
            .iter()
            .min()
            .context("saturation benchmark needs reservations")?;
        let reservation_bytes_max = *reservation_bytes
            .iter()
            .max()
            .context("saturation benchmark needs reservations")?;

        Ok(Self {
            reservation_bytes_min,
            reservation_bytes_max,
            high_watermark_bytes,
            prefill_reserved_bytes,
            prefill_objects,
            paced_objects: objects.len() - prefill_objects,
            paced_payload_bytes,
        })
    }
}

fn bench_env<T>(name: &str, default: T) -> Result<T>
where
    T: std::str::FromStr,
    T::Err: std::fmt::Display,
{
    match std::env::var(name) {
        Ok(value) => value
            .parse()
            .map_err(|error| anyhow::anyhow!("{name}={value:?} is invalid: {error}")),
        Err(std::env::VarError::NotPresent) => Ok(default),
        Err(error) => Err(error.into()),
    }
}

fn mib_per_second(total_bytes: u64, elapsed: Duration) -> f64 {
    total_bytes as f64 / (1024.0 * 1024.0) / elapsed.as_secs_f64()
}

fn seconds(nanos: u64) -> f64 {
    Duration::from_nanos(nanos).as_secs_f64()
}

fn max_inter_completion_gap(completions: &[Duration]) -> Duration {
    completions
        .windows(2)
        .map(|pair| pair[1].saturating_sub(pair[0]))
        .max()
        .unwrap_or_default()
}

fn validate_saturation_sequence(index: usize, sequence: u64) -> Result<()> {
    let planned = u64::try_from(index)?.saturating_add(1);
    anyhow::ensure!(
        sequence == planned,
        "saturation object {index} received sequence {sequence} instead of planned sequence {planned}"
    );
    Ok(())
}

fn saturation_writer_slots(object_count: usize, writers: usize) -> usize {
    object_count.min(writers)
}

fn saturation_ack_elapsed(activated: Instant, acknowledged: Instant) -> Duration {
    acknowledged.saturating_duration_since(activated)
}

type SaturationCompletion = (usize, std::result::Result<u64, String>, Instant);

async fn next_saturation_completion(
    writers: &mut tokio::task::JoinSet<SaturationCompletion>,
) -> Result<SaturationCompletion> {
    writers
        .join_next()
        .await
        .context("saturation writer task set ended before its completion")?
        .context("saturation writer task failed")
}

#[derive(Clone)]
struct SaturationWriterContext {
    store: WritebackObjectStore,
    segment_payload: Bytes,
    manifest_payload: Bytes,
    paths: Arc<Vec<ObjectPath>>,
    classes: Arc<Vec<BenchObjectClass>>,
}

struct ActiveSaturationWriter {
    control: WriteAdmissionTestControl,
    finished: tokio::sync::oneshot::Receiver<()>,
}

impl SaturationWriterContext {
    fn spawn(
        &self,
        writers: &mut tokio::task::JoinSet<SaturationCompletion>,
        index: usize,
    ) -> ActiveSaturationWriter {
        let context = self.clone();
        let control = WriteAdmissionTestControl::new();
        let writer_control = control.clone();
        let (finished_sender, finished) = tokio::sync::oneshot::channel();
        writers.spawn(async move {
            let class = context.classes[index];
            let mut options = benchmark_put_options(class);
            options.extensions.insert(writer_control);
            let result = match context
                .store
                .put_opts(
                    &context.paths[index],
                    benchmark_payload(class, &context.segment_payload, &context.manifest_payload)
                        .clone()
                        .into(),
                    options,
                )
                .await
            {
                Ok(result) => result
                    .e_tag
                    .as_deref()
                    .and_then(LocalEtag::sequence_from_str)
                    .ok_or_else(|| format!("tail object {index} returned no local sequence")),
                Err(error) => Err(error.to_string()),
            };
            let _ = finished_sender.send(());
            (index, result, Instant::now())
        });
        ActiveSaturationWriter { control, finished }
    }
}

async fn wait_for_saturation_admission(
    active: &mut ActiveSaturationWriter,
    require_queue: bool,
) -> Result<()> {
    tokio::time::timeout(Duration::from_secs(5), async {
        tokio::select! {
            () = active.control.wait_until_registered() => Ok(()),
            result = &mut active.finished => {
                result.context("saturation writer exited before reporting completion")?;
                anyhow::bail!("saturation writer exited before SSD admission")
            }
        }
    })
    .await
    .context("saturation writer did not enter SSD admission")??;
    anyhow::ensure!(
        !require_queue || active.control.was_queued(),
        "saturation writer reached sequence allocation without queuing at the SSD watermark"
    );
    Ok(())
}

async fn release_saturation_allocation(active: &mut ActiveSaturationWriter) -> Result<()> {
    tokio::time::timeout(Duration::from_secs(60), async {
        tokio::select! {
            () = active.control.wait_until_allocation_waits() => Ok(()),
            result = &mut active.finished => {
                result.context("saturation writer exited before reporting completion")?;
                anyhow::bail!("saturation writer exited before sequence allocation")
            }
        }
    })
    .await
    .context("saturation writer did not reach sequence allocation")??;
    active.control.release_allocation();
    tokio::time::timeout(
        Duration::from_secs(5),
        active.control.wait_until_allocated(),
    )
    .await
    .context("saturation writer did not allocate its journal sequence")?;
    Ok(())
}

fn generated_segment_options() -> PutOptions {
    let mut options = PutOptions::from(PutMode::Create);
    options.extensions.insert(GeneratedSegmentCreate);
    options
}

fn benchmark_put_options(class: BenchObjectClass) -> PutOptions {
    match class {
        BenchObjectClass::Segment => generated_segment_options(),
        BenchObjectClass::Manifest => PutOptions::from(PutMode::Create),
    }
}

fn benchmark_payload<'a>(
    class: BenchObjectClass,
    segment: &'a Bytes,
    manifest: &'a Bytes,
) -> &'a Bytes {
    match class {
        BenchObjectClass::Segment => segment,
        BenchObjectClass::Manifest => manifest,
    }
}

fn apply_transport_path_overrides(settings: &mut Settings) -> Result<()> {
    let Some(config) = settings.sftp.as_mut() else {
        anyhow::bail!("benchmark config must include [sftp]");
    };
    if let Some(path) = std::env::var_os(IDENTITY_FILE_ENV) {
        config.identity_file = PathBuf::from(path);
    }
    if let Some(path) = std::env::var_os(KNOWN_HOSTS_ENV) {
        config.known_hosts = PathBuf::from(path);
    }
    Ok(())
}

fn load_benchmark_settings(config_path: &std::path::Path) -> Result<Settings> {
    let content = fs::read_to_string(config_path)
        .with_context(|| format!("read benchmark config {}", config_path.display()))?;
    let mut settings: Settings = toml::from_str(&content)
        .with_context(|| format!("parse benchmark config {}", config_path.display()))?;
    apply_transport_path_overrides(&mut settings)?;
    settings.validate()?;
    Ok(settings)
}

async fn read_and_verify_remote(
    remote: &Arc<dyn ObjectStore>,
    objects: &[ObjectPath],
    classes: &[BenchObjectClass],
    segment_payload: &Bytes,
    manifest_payload: &Bytes,
    readers: usize,
) -> Result<(Duration, usize)> {
    let paths = Arc::new(objects.to_vec());
    let classes = Arc::new(classes.to_vec());
    let started = Instant::now();
    let mut tasks = tokio::task::JoinSet::new();
    for reader in 0..readers {
        let remote = Arc::clone(remote);
        let paths = Arc::clone(&paths);
        let classes = Arc::clone(&classes);
        let segment_payload = segment_payload.clone();
        let manifest_payload = manifest_payload.clone();
        tasks.spawn(async move {
            let mut verified = 0;
            let mut index = reader;
            while index < paths.len() {
                let payload =
                    benchmark_payload(classes[index], &segment_payload, &manifest_payload);
                let readback = remote.get(&paths[index]).await?.bytes().await?;
                anyhow::ensure!(
                    readback == payload,
                    "SFTP readback mismatch for {}: expected {} bytes, got {}",
                    paths[index],
                    payload.len(),
                    readback.len()
                );
                verified += 1;
                index += readers;
            }
            Result::<usize>::Ok(verified)
        });
    }
    let mut verified = 0;
    while let Some(result) = tasks.join_next().await {
        verified += result.context("remote reader task failed")??;
    }
    Ok((started.elapsed(), verified))
}

async fn execute_benchmark(
    store: &WritebackObjectStore,
    remote: &Arc<dyn ObjectStore>,
    objects: &[ObjectPath],
    classes: &[BenchObjectClass],
    segment_payload: Bytes,
    manifest_payload: Bytes,
    geometry: BenchGeometry,
) -> Result<BenchReport> {
    anyhow::ensure!(
        objects.len() == classes.len(),
        "benchmark object plan is inconsistent"
    );
    let total_bytes = classes.iter().try_fold(0_u64, |total, class| {
        total
            .checked_add(u64::try_from(
                benchmark_payload(*class, &segment_payload, &manifest_payload).len(),
            )?)
            .context("benchmark byte count overflowed")
    })?;
    let mut hasher = Sha256::new();
    hasher.update(&segment_payload);
    let payload_sha256 = format!("{:x}", hasher.finalize());
    let mut manifest_hasher = Sha256::new();
    manifest_hasher.update(&manifest_payload);
    let manifest_payload_sha256 = format!("{:x}", manifest_hasher.finalize());
    let paths = Arc::new(objects.to_vec());
    let classes = Arc::new(classes.to_vec());
    crate::sftp_protocol::reset_bench_timing();

    let started = Instant::now();
    if classes.contains(&BenchObjectClass::Manifest) {
        for (path, class) in paths.iter().zip(classes.iter().copied()) {
            store
                .put_opts(
                    path,
                    benchmark_payload(class, &segment_payload, &manifest_payload)
                        .clone()
                        .into(),
                    benchmark_put_options(class),
                )
                .await?;
        }
    } else {
        let mut tasks = tokio::task::JoinSet::new();
        for writer in 0..geometry.writers {
            let store = store.clone();
            let segment_payload = segment_payload.clone();
            let manifest_payload = manifest_payload.clone();
            let paths = Arc::clone(&paths);
            let classes = Arc::clone(&classes);
            tasks.spawn(async move {
                let mut index = writer;
                while index < paths.len() {
                    let class = classes[index];
                    store
                        .put_opts(
                            &paths[index],
                            benchmark_payload(class, &segment_payload, &manifest_payload)
                                .clone()
                                .into(),
                            benchmark_put_options(class),
                        )
                        .await?;
                    index += geometry.writers;
                }
                object_store::Result::<()>::Ok(())
            });
        }
        while let Some(result) = tasks.join_next().await {
            result.context("benchmark writer task failed")??;
        }
    }
    let ram_ack = started.elapsed();

    let target = store.status()?.accepted_seq;
    store.wait_local(target).await?;
    let local = started.elapsed();

    let remote_started = Instant::now();
    store.activate_remote()?;
    store.wait_remote(target).await?;
    let remote_drain = remote_started.elapsed();
    let sftp_timing = crate::sftp_protocol::bench_timing();
    let last_used_session = sftp_timing
        .session_publications
        .iter()
        .rposition(|publications| *publications != 0)
        .map_or(0, |index| index + 1);

    let (remote_read, remote_read_verified_objects) = read_and_verify_remote(
        remote,
        objects,
        &classes,
        &segment_payload,
        &manifest_payload,
        geometry.writers,
    )
    .await?;

    let segment_objects = classes
        .iter()
        .filter(|class| **class == BenchObjectClass::Segment)
        .count();
    let manifest_objects = classes.len() - segment_objects;

    Ok(BenchReport {
        total_bytes,
        object_count: objects.len(),
        payload_bytes: segment_payload.len(),
        manifest_payload_bytes: manifest_payload.len(),
        segment_objects,
        manifest_objects,
        writers: geometry.writers,
        max_connections: geometry.max_connections,
        upload_concurrency: geometry.upload_concurrency,
        local_concurrency: geometry.local_concurrency,
        fence_objects: manifest_objects,
        ram_ack_seconds: ram_ack.as_secs_f64(),
        ram_ack_mib_per_second: mib_per_second(total_bytes, ram_ack),
        local_seconds: local.as_secs_f64(),
        local_mib_per_second: mib_per_second(total_bytes, local),
        remote_directory_prepare_seconds: geometry.remote_directory_prepare.as_secs_f64(),
        remote_drain_seconds: remote_drain.as_secs_f64(),
        remote_drain_mib_per_second: mib_per_second(total_bytes, remote_drain),
        remote_read_seconds: remote_read.as_secs_f64(),
        remote_read_mib_per_second: mib_per_second(total_bytes, remote_read),
        remote_read_verified_objects,
        sftp_write_handles_closed: sftp_timing.publications,
        sftp_open_total_seconds: seconds(sftp_timing.open_nanos),
        sftp_publication_window_total_seconds: seconds(sftp_timing.publication_window_nanos),
        sftp_hardlink_total_seconds: seconds(sftp_timing.hardlink_nanos),
        sftp_remove_total_seconds: seconds(sftp_timing.remove_nanos),
        sftp_session_write_handles_closed: sftp_timing.session_publications[..last_used_session]
            .to_vec(),
        sftp_session_write_bytes: sftp_timing.session_write_bytes[..last_used_session].to_vec(),
        payload_sha256,
        manifest_payload_sha256,
    })
}

async fn execute_saturation_benchmark(
    store: &WritebackObjectStore,
    remote: &Arc<dyn ObjectStore>,
    object_set: &BenchObjectSet,
    segment_payload: Bytes,
    manifest_payload: Bytes,
    geometry: BenchGeometry,
    saturation: SaturationGeometry,
) -> Result<SaturationReport> {
    let objects = &object_set.objects;
    let classes = &object_set.classes;
    anyhow::ensure!(
        objects.len() == classes.len(),
        "saturation object classes must match object paths"
    );
    let total_bytes = classes.iter().try_fold(0_u64, |total, class| {
        let payload = benchmark_payload(*class, &segment_payload, &manifest_payload);
        total
            .checked_add(u64::try_from(payload.len())?)
            .context("saturation benchmark byte count overflowed")
    })?;
    let blocked_bytes = saturation.paced_payload_bytes;
    let mut hasher = Sha256::new();
    hasher.update(&segment_payload);
    let payload_sha256 = format!("{:x}", hasher.finalize());
    let mut manifest_hasher = Sha256::new();
    manifest_hasher.update(&manifest_payload);
    let manifest_payload_sha256 = format!("{:x}", manifest_hasher.finalize());
    let paths = Arc::new(objects.to_vec());
    let classes = Arc::new(classes.to_vec());
    crate::sftp_protocol::reset_bench_timing();

    let prefill_started = Instant::now();
    for index in 0..saturation.prefill_objects {
        let class = classes[index];
        store
            .put_opts(
                &paths[index],
                benchmark_payload(class, &segment_payload, &manifest_payload)
                    .clone()
                    .into(),
                benchmark_put_options(class),
            )
            .await
            .with_context(|| format!("prefill object {index} failed"))?;
    }
    tokio::time::timeout(
        Duration::from_secs(60),
        store.wait_local(u64::try_from(saturation.prefill_objects)?),
    )
    .await
    .context("timed out waiting for saturation prefill durability")??;
    let prefill = prefill_started.elapsed();

    let mut writers = tokio::task::JoinSet::new();
    let writer_context = SaturationWriterContext {
        store: store.clone(),
        segment_payload: segment_payload.clone(),
        manifest_payload: manifest_payload.clone(),
        paths: Arc::clone(&paths),
        classes: Arc::clone(&classes),
    };
    let initially_active = saturation_writer_slots(saturation.paced_objects, geometry.writers);
    let mut next_index = saturation.prefill_objects;
    let mut initial_writers = Vec::with_capacity(initially_active);
    for _ in 0..initially_active {
        let mut active = writer_context.spawn(&mut writers, next_index);
        wait_for_saturation_admission(&mut active, true).await?;
        initial_writers.push(active);
        next_index += 1;
    }
    tokio::time::sleep(Duration::from_millis(100)).await;
    match writers.try_join_next() {
        None => {}
        Some(Err(error)) => return Err(error).context("saturation writer task failed"),
        Some(Ok((index, result, _))) => {
            result.map_err(|error| anyhow::anyhow!("tail object {index} failed: {error}"))?;
            anyhow::bail!("tail object {index} bypassed the full SSD admission boundary")
        }
    }
    let before = store.status()?;
    anyhow::ensure!(
        before.accepted_seq == u64::try_from(saturation.prefill_objects)?,
        "expected {} accepted prefill objects, observed {}",
        saturation.prefill_objects,
        before.accepted_seq
    );
    anyhow::ensure!(
        before.local_seq == before.accepted_seq,
        "saturation prefill was not locally durable"
    );
    anyhow::ensure!(
        before.remote_seq == 0,
        "paused saturation benchmark unexpectedly advanced remote sequence"
    );
    anyhow::ensure!(
        before.dirty_ssd_reserved_bytes == saturation.prefill_reserved_bytes,
        "expected {} prefill SSD bytes, observed {}",
        saturation.prefill_reserved_bytes,
        before.dirty_ssd_reserved_bytes
    );
    anyhow::ensure!(before.terminal_error.is_none(), "prefill became terminal");

    let activated = Instant::now();
    store.activate_remote()?;
    let blocked_ack_times = tokio::time::timeout(Duration::from_secs(300), async {
        let mut completions = Vec::with_capacity(saturation.paced_objects);
        for active in &mut initial_writers {
            release_saturation_allocation(active).await?;
        }
        for _ in 0..saturation.paced_objects {
            let (index, result, acknowledged) = next_saturation_completion(&mut writers).await?;
            let sequence = result
                .map_err(|error| anyhow::anyhow!("blocked object {index} failed: {error}"))?;
            validate_saturation_sequence(index, sequence)?;
            completions.push(saturation_ack_elapsed(activated, acknowledged));
            if next_index < paths.len() {
                let mut active = writer_context.spawn(&mut writers, next_index);
                wait_for_saturation_admission(&mut active, false).await?;
                release_saturation_allocation(&mut active).await?;
                next_index += 1;
            }
        }
        completions.sort_unstable();
        Result::<Vec<Duration>>::Ok(completions)
    })
    .await
    .context("blocked foreground writes did not progress after remote activation")??;
    let after_blocked_acks = store.status()?;
    let target = after_blocked_acks.accepted_seq;
    anyhow::ensure!(
        target == u64::try_from(objects.len())?,
        "expected {} accepted objects after pacing, observed {target}",
        objects.len()
    );
    anyhow::ensure!(
        writers.is_empty(),
        "saturation writer tasks remain after all acknowledgements"
    );
    tokio::time::timeout(Duration::from_secs(60), store.wait_local(target))
        .await
        .context("timed out waiting for the paced tail to become locally durable")??;
    let blocked_ack = blocked_ack_times
        .last()
        .copied()
        .context("saturation benchmark did not record blocked acknowledgements")?;
    let first_blocked_ack = blocked_ack_times[0];
    let max_blocked_ack_gap = max_inter_completion_gap(&blocked_ack_times);
    let remote_cleanup_bytes = after_blocked_acks
        .remote_bytes_completed
        .saturating_sub(before.remote_bytes_completed);
    anyhow::ensure!(
        remote_cleanup_bytes > 0,
        "blocked foreground writes resumed without durable remote cleanup"
    );
    let blocked_rate = mib_per_second(blocked_bytes, blocked_ack);
    let remote_cleanup_rate = mib_per_second(remote_cleanup_bytes, blocked_ack);

    tokio::time::timeout(Duration::from_secs(300), store.wait_remote(target))
        .await
        .context("timed out draining the saturated SSD journal to remote")??;
    let remote_drain = activated.elapsed();
    let final_status = store.status()?;
    anyhow::ensure!(
        final_status.dirty_ssd_reserved_bytes == 0,
        "remote drain left {} SSD bytes reserved",
        final_status.dirty_ssd_reserved_bytes
    );
    anyhow::ensure!(
        final_status.terminal_error.is_none(),
        "saturation benchmark ended terminal: {:?}",
        final_status.terminal_error
    );
    let sftp_timing = crate::sftp_protocol::bench_timing();
    let last_used_session = sftp_timing
        .session_publications
        .iter()
        .rposition(|publications| *publications != 0)
        .map_or(0, |index| index + 1);
    let (remote_read, remote_read_verified_objects) = read_and_verify_remote(
        remote,
        objects,
        &classes,
        &segment_payload,
        &manifest_payload,
        geometry.writers,
    )
    .await?;
    let segment_objects = classes
        .iter()
        .filter(|class| **class == BenchObjectClass::Segment)
        .count();
    let manifest_objects = classes.len() - segment_objects;

    Ok(SaturationReport {
        total_bytes,
        object_count: objects.len(),
        payload_bytes: segment_payload.len(),
        manifest_payload_bytes: manifest_payload.len(),
        segment_objects,
        manifest_objects,
        writers: geometry.writers,
        max_connections: geometry.max_connections,
        upload_concurrency: geometry.upload_concurrency,
        local_concurrency: geometry.local_concurrency,
        ssd_capacity_bytes: final_status.dirty_ssd_capacity_bytes,
        ssd_high_watermark_bytes: saturation.high_watermark_bytes,
        ssd_reservation_bytes_min: saturation.reservation_bytes_min,
        ssd_reservation_bytes_max: saturation.reservation_bytes_max,
        prefill_objects: saturation.prefill_objects,
        prefill_reserved_bytes: saturation.prefill_reserved_bytes,
        tail_objects_after_activation: saturation.paced_objects,
        prefill_seconds: prefill.as_secs_f64(),
        remote_directory_prepare_seconds: geometry.remote_directory_prepare.as_secs_f64(),
        first_blocked_ack_seconds: first_blocked_ack.as_secs_f64(),
        blocked_ack_seconds: blocked_ack.as_secs_f64(),
        blocked_ack_mib_per_second: blocked_rate,
        max_blocked_ack_gap_seconds: max_blocked_ack_gap.as_secs_f64(),
        remote_cleanup_bytes_during_blocked_acks: remote_cleanup_bytes,
        remote_cleanup_mib_per_second: remote_cleanup_rate,
        foreground_to_remote_rate_ratio: blocked_rate / remote_cleanup_rate,
        remote_drain_seconds: remote_drain.as_secs_f64(),
        remote_drain_mib_per_second: mib_per_second(total_bytes, remote_drain),
        remote_read_seconds: remote_read.as_secs_f64(),
        remote_read_mib_per_second: mib_per_second(total_bytes, remote_read),
        remote_read_verified_objects,
        sftp_write_handles_closed: sftp_timing.publications,
        sftp_open_total_seconds: seconds(sftp_timing.open_nanos),
        sftp_publication_window_total_seconds: seconds(sftp_timing.publication_window_nanos),
        sftp_hardlink_total_seconds: seconds(sftp_timing.hardlink_nanos),
        sftp_remove_total_seconds: seconds(sftp_timing.remove_nanos),
        sftp_session_write_handles_closed: sftp_timing.session_publications[..last_used_session]
            .to_vec(),
        sftp_session_write_bytes: sftp_timing.session_write_bytes[..last_used_session].to_vec(),
        payload_sha256,
        manifest_payload_sha256,
    })
}

async fn prepare_remote_run(pool: &SftpSessionPool, directories: &[PathBuf]) -> Result<Duration> {
    let started = Instant::now();
    let mut lease = pool.checkout(OperationKind::Metadata).await?;
    for directory in directories {
        pool.ensure_directory(&mut lease, directory).await?;
    }
    lease.complete().await?;
    Ok(started.elapsed())
}

async fn cleanup_remote_run(
    remote: &Arc<dyn ObjectStore>,
    pool: &SftpSessionPool,
    objects: &[ObjectPath],
    directories: &[PathBuf],
) -> Result<()> {
    let mut errors = Vec::new();
    for path in objects {
        if let Err(error) = remote.delete(path).await
            && !matches!(error, object_store::Error::NotFound { .. })
        {
            errors.push(format!("delete {path}: {error}"));
        }
    }
    for path in objects {
        match remote.head(path).await {
            Err(object_store::Error::NotFound { .. }) => {}
            Ok(_) => errors.push(format!("cleanup verification found {path}")),
            Err(error) => errors.push(format!("verify {path}: {error}")),
        }
    }

    match pool.checkout(OperationKind::Metadata).await {
        Ok(mut lease) => {
            for directory in directories {
                match lease.remove_directory(directory).await {
                    Ok(()) | Err(TransportError::NotFound(_)) => {}
                    Err(error) => {
                        errors.push(format!("remove directory {}: {error}", directory.display()))
                    }
                }
            }
            if let Err(error) = lease.complete().await {
                errors.push(format!("finish cleanup session: {error}"));
            }
        }
        Err(error) => errors.push(format!("open cleanup session: {error}")),
    }

    anyhow::ensure!(
        errors.is_empty(),
        "SFTP benchmark cleanup failed: {}",
        errors.join("; ")
    );
    Ok(())
}

async fn close_pool_after_setup_error(
    remote: Arc<dyn ObjectStore>,
    pool: SftpSessionPool,
    error: anyhow::Error,
) -> anyhow::Error {
    drop(remote);
    match pool.shutdown().await {
        Ok(()) => error,
        Err(cleanup_error) => anyhow::anyhow!(
            "{error:#}; SFTP pool shutdown after setup failure also failed: {cleanup_error}"
        ),
    }
}

pub(super) fn finish_benchmark<T>(
    primary: Result<T>,
    teardowns: impl IntoIterator<Item = Result<()>>,
) -> Result<T> {
    let mut errors = Vec::new();
    if let Err(error) = &primary {
        errors.push(format!("{error:#}"));
    }
    for teardown in teardowns {
        if let Err(error) = teardown {
            errors.push(format!("{error:#}"));
        }
    }
    if errors.is_empty() {
        primary
    } else {
        Err(anyhow::anyhow!(errors.join("; ")))
    }
}

async fn finish_sftp_run<T>(
    store: WritebackObjectStore,
    remote: Arc<dyn ObjectStore>,
    pool: SftpSessionPool,
    scratch: tempfile::TempDir,
    objects: &BenchObjectSet,
    benchmark: Result<T>,
    benchmark_context: &'static str,
) -> Result<T> {
    let shutdown = store.shutdown().await;
    drop(store);
    let cleanup = cleanup_remote_run(
        &remote,
        &pool,
        &objects.objects,
        &objects.directories_deepest_first,
    )
    .await;
    drop(remote);
    let pool_shutdown = pool.shutdown().await;
    let scratch_path = scratch.path().to_path_buf();
    let scratch_cleanup = scratch.close().context("remove local benchmark journal");
    let scratch_absence = if scratch_path.exists() {
        Err(anyhow::anyhow!(
            "local benchmark journal still exists at {}",
            scratch_path.display()
        ))
        .context("verify local benchmark journal removal")
    } else {
        Ok(())
    };
    finish_benchmark(
        benchmark.context(benchmark_context),
        [
            shutdown.context("writeback shutdown failed"),
            cleanup.context("remote benchmark cleanup failed"),
            pool_shutdown.context("SFTP pool shutdown failed"),
            scratch_cleanup,
            scratch_absence,
        ],
    )
}

/// Directly measures the shipping SFTP object store behind the shipping
/// writeback scheduler. This is a normal dev-profile ignored test: it does not
/// build, install, stop, or start ZeroFS. The caller must provide a validated
/// SFTP configuration and an isolated local scratch directory.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "real SFTP writeback benchmark; run explicitly on Linux"]
async fn bench_sftp_writeback_remote_drain() -> Result<()> {
    let config_path = std::env::var_os(CONFIG_ENV)
        .map(PathBuf::from)
        .with_context(|| format!("{CONFIG_ENV} must name a ZeroFS TOML file"))?;
    let mut settings = load_benchmark_settings(&config_path)?;
    anyhow::ensure!(
        settings.sftp_endpoint()?.is_some(),
        "{CONFIG_ENV} must configure an sftp:// storage URL"
    );
    let total_mib: usize = bench_env(TOTAL_MIB_ENV, 256)?;
    let payload_kib: usize = bench_env(PAYLOAD_KIB_ENV, 1024)?;
    let manifest_kib: usize = bench_env(MANIFEST_KIB_ENV, 64)?;
    let writers: usize = bench_env(WRITERS_ENV, 16)?;
    let fence_every: usize = bench_env(FENCE_EVERY_ENV, 0)?;
    let sftp = settings
        .sftp
        .as_mut()
        .context("benchmark config must include [sftp]")?;
    let max_connections: usize = bench_env(MAX_CONNECTIONS_ENV, sftp.max_connections)?;
    sftp.max_connections = max_connections;
    settings.validate()?;
    let production_writeback = settings
        .writeback_settings(WritebackAccessMode::ReadWrite)?
        .context("benchmark config must enable [writeback]")?;
    anyhow::ensure!(total_mib > 0, "{TOTAL_MIB_ENV} must be positive");
    anyhow::ensure!(payload_kib > 0, "{PAYLOAD_KIB_ENV} must be positive");
    anyhow::ensure!(manifest_kib > 0, "{MANIFEST_KIB_ENV} must be positive");
    anyhow::ensure!(writers > 0, "{WRITERS_ENV} must be positive");
    let total_kib = total_mib
        .checked_mul(1024)
        .context("benchmark size overflowed")?;
    anyhow::ensure!(
        total_kib.is_multiple_of(payload_kib),
        "{TOTAL_MIB_ENV} must be divisible by {PAYLOAD_KIB_ENV}"
    );
    let object_count = total_kib / payload_kib;
    let total_bytes = u64::try_from(total_kib)? << 10;
    let payload_bytes = payload_kib
        .checked_mul(1024)
        .context("benchmark payload size overflowed")?;
    let manifest_bytes = manifest_kib
        .checked_mul(1024)
        .context("benchmark manifest size overflowed")?;

    let (remote, base, pool) = build_remote_store(&settings).await?;
    let setup = (|| -> Result<_> {
        let objects =
            BenchObjectSet::new_with_fences(&base, Uuid::new_v4(), object_count, fence_every)?;
        let scratch = match std::env::var_os(BENCH_DIR_ENV) {
            Some(directory) => tempfile::tempdir_in(PathBuf::from(directory))?,
            None => tempfile::tempdir()?,
        };
        let journal_dir = scratch.path().join("writeback");
        let journal = Arc::new(Journal::open(
            &journal_dir,
            JournalIdentity {
                format_version: 1,
                bucket_id: format!("sftp-writeback-bench-{}", Uuid::new_v4().simple()),
                backend_endpoint: "sftp://benchmark-target".to_owned(),
                database_prefix: objects.database_prefix.to_string(),
                backend_kind: "sftp".to_owned(),
                encryption_key_identity_sha256: [0x42; 32],
            },
        )?);
        let bench_settings = WritebackSettings {
            dir: journal_dir,
            ack_mode: AckMode::Memory,
            memory_bytes: total_bytes.saturating_add(64 << 20),
            disk_bytes: total_bytes.saturating_mul(2).saturating_add(256 << 20),
            min_free_bytes: 1,
            high_watermark_percent: 95,
            resume_percent: 85,
            upload_concurrency: production_writeback.upload_concurrency,
            local_concurrency: production_writeback.local_concurrency,
            shutdown_flush: ShutdownFlush::Local,
        };
        Ok((objects, scratch, journal, bench_settings))
    })();
    let (objects, scratch, journal, bench_settings) = match setup {
        Ok(setup) => setup,
        Err(error) => {
            return Err(close_pool_after_setup_error(remote, pool, error).await);
        }
    };
    let store = match WritebackObjectStore::open_paused(
        Arc::clone(&remote),
        journal,
        bench_settings.clone(),
    )
    .await
    {
        Ok(store) => store,
        Err(error) => {
            return Err(close_pool_after_setup_error(remote, pool, error).await);
        }
    };

    let mut payload = vec![0_u8; payload_bytes];
    StdRng::seed_from_u64(0x5f54_4653_4245_4e43).fill_bytes(&mut payload);
    let mut manifest_payload = vec![0_u8; manifest_bytes];
    StdRng::seed_from_u64(0x4d41_4e49_4645_5354).fill_bytes(&mut manifest_payload);
    let remote_directory_prepare =
        prepare_remote_run(&pool, &objects.directories_deepest_first).await;
    let benchmark = match remote_directory_prepare {
        Ok(remote_directory_prepare) => {
            execute_benchmark(
                &store,
                &remote,
                &objects.objects,
                &objects.classes,
                Bytes::from(payload),
                Bytes::from(manifest_payload),
                BenchGeometry {
                    writers,
                    max_connections,
                    upload_concurrency: bench_settings.upload_concurrency,
                    local_concurrency: bench_settings.local_concurrency,
                    remote_directory_prepare,
                },
            )
            .await
        }
        Err(error) => Err(error.context("prepare remote benchmark directories")),
    };
    let report = finish_sftp_run(
        store,
        remote,
        pool,
        scratch,
        &objects,
        benchmark,
        "SFTP writeback benchmark failed",
    )
    .await?;
    println!("SFTP_WRITEBACK_BENCH {}", serde_json::to_string(&report)?);
    Ok(())
}

/// Fills a deliberately small local writeback journal while remote replay is
/// paused, proves additional foreground writes block at the SSD watermark,
/// then measures how smoothly durable remote cleanup admits that blocked tail.
/// It uses the shipping journal, admission, scheduler, native SFTP transport,
/// and generated-segment create contract without installing or restarting the
/// production service.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "real full-SSD SFTP pacing benchmark; run explicitly on Linux"]
async fn bench_sftp_writeback_full_ssd_pacing() -> Result<()> {
    let config_path = std::env::var_os(CONFIG_ENV)
        .map(PathBuf::from)
        .with_context(|| format!("{CONFIG_ENV} must name a ZeroFS TOML file"))?;
    let mut settings = load_benchmark_settings(&config_path)?;
    anyhow::ensure!(
        settings.sftp_endpoint()?.is_some(),
        "{CONFIG_ENV} must configure an sftp:// storage URL"
    );
    let total_mib: usize = bench_env(TOTAL_MIB_ENV, 256)?;
    let payload_kib: usize = bench_env(PAYLOAD_KIB_ENV, 8 * 1024)?;
    let manifest_kib: usize = bench_env(MANIFEST_KIB_ENV, 64)?;
    let writers: usize = bench_env(WRITERS_ENV, 16)?;
    let ssd_mib: usize = bench_env(SSD_MIB_ENV, 64)?;
    let fence_every: usize = bench_env(FENCE_EVERY_ENV, 4)?;
    let sftp = settings
        .sftp
        .as_mut()
        .context("benchmark config must include [sftp]")?;
    let max_connections: usize = bench_env(MAX_CONNECTIONS_ENV, sftp.max_connections)?;
    sftp.max_connections = max_connections;
    settings.validate()?;
    let production_writeback = settings
        .writeback_settings(WritebackAccessMode::ReadWrite)?
        .context("benchmark config must enable [writeback]")?;
    anyhow::ensure!(total_mib > 0, "{TOTAL_MIB_ENV} must be positive");
    anyhow::ensure!(payload_kib > 0, "{PAYLOAD_KIB_ENV} must be positive");
    anyhow::ensure!(manifest_kib > 0, "{MANIFEST_KIB_ENV} must be positive");
    anyhow::ensure!(writers > 0, "{WRITERS_ENV} must be positive");
    anyhow::ensure!(ssd_mib > 0, "{SSD_MIB_ENV} must be positive");
    let total_kib = total_mib
        .checked_mul(1024)
        .context("saturation benchmark size overflowed")?;
    anyhow::ensure!(
        total_kib.is_multiple_of(payload_kib),
        "{TOTAL_MIB_ENV} must be divisible by {PAYLOAD_KIB_ENV}"
    );
    let object_count = total_kib / payload_kib;
    let total_bytes = u64::try_from(total_kib)? << 10;
    let disk_bytes = u64::try_from(ssd_mib)? << 20;
    let payload_bytes = payload_kib
        .checked_mul(1024)
        .context("saturation benchmark payload size overflowed")?;
    let manifest_bytes = manifest_kib
        .checked_mul(1024)
        .context("saturation manifest size overflowed")?;

    let (remote, base, pool) = build_remote_store(&settings).await?;
    let setup = (|| -> Result<_> {
        let objects =
            BenchObjectSet::new_with_fences(&base, Uuid::new_v4(), object_count, fence_every)?;
        let saturation = SaturationGeometry::new(
            &objects.objects,
            &objects.classes,
            payload_bytes,
            manifest_bytes,
            disk_bytes,
            95,
        )?;
        let scratch = match std::env::var_os(BENCH_DIR_ENV) {
            Some(directory) => tempfile::tempdir_in(PathBuf::from(directory))?,
            None => tempfile::tempdir()?,
        };
        let journal_dir = scratch.path().join("writeback");
        let journal = Arc::new(Journal::open(
            &journal_dir,
            JournalIdentity {
                format_version: 1,
                bucket_id: format!("sftp-writeback-saturation-{}", Uuid::new_v4().simple()),
                backend_endpoint: "sftp://benchmark-target".to_owned(),
                database_prefix: objects.database_prefix.to_string(),
                backend_kind: "sftp".to_owned(),
                encryption_key_identity_sha256: [0x52; 32],
            },
        )?);
        let bench_settings = WritebackSettings {
            dir: journal_dir,
            ack_mode: AckMode::Memory,
            memory_bytes: total_bytes.saturating_add(64 << 20),
            disk_bytes,
            min_free_bytes: 1,
            high_watermark_percent: 95,
            resume_percent: 85,
            upload_concurrency: production_writeback.upload_concurrency,
            local_concurrency: production_writeback.local_concurrency,
            shutdown_flush: ShutdownFlush::Local,
        };
        Ok((objects, saturation, scratch, journal, bench_settings))
    })();
    let (objects, saturation, scratch, journal, bench_settings) = match setup {
        Ok(setup) => setup,
        Err(error) => {
            return Err(close_pool_after_setup_error(remote, pool, error).await);
        }
    };
    let store = match WritebackObjectStore::open_paused(
        Arc::clone(&remote),
        journal,
        bench_settings.clone(),
    )
    .await
    {
        Ok(store) => store,
        Err(error) => {
            return Err(close_pool_after_setup_error(remote, pool, error).await);
        }
    };

    let mut payload = vec![0_u8; payload_bytes];
    StdRng::seed_from_u64(0x5353_445f_4655_4c4c).fill_bytes(&mut payload);
    let mut manifest_payload = vec![0_u8; manifest_bytes];
    StdRng::seed_from_u64(0x5353_445f_4d41_4e49).fill_bytes(&mut manifest_payload);
    let remote_directory_prepare =
        prepare_remote_run(&pool, &objects.directories_deepest_first).await;
    let benchmark = match remote_directory_prepare {
        Ok(remote_directory_prepare) => {
            execute_saturation_benchmark(
                &store,
                &remote,
                &objects,
                Bytes::from(payload),
                Bytes::from(manifest_payload),
                BenchGeometry {
                    writers,
                    max_connections,
                    upload_concurrency: bench_settings.upload_concurrency,
                    local_concurrency: bench_settings.local_concurrency,
                    remote_directory_prepare,
                },
                saturation,
            )
            .await
        }
        Err(error) => Err(error.context("prepare remote saturation benchmark directories")),
    };
    let report = finish_sftp_run(
        store,
        remote,
        pool,
        scratch,
        &objects,
        benchmark,
        "full-SSD SFTP pacing benchmark failed",
    )
    .await?;
    println!(
        "SFTP_WRITEBACK_SATURATION_BENCH {}",
        serde_json::to_string(&report)?
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{
        BenchObjectClass, BenchObjectSet, GeneratedSegmentCreate, SaturationGeometry,
        benchmark_put_options, build_remote_store, finish_benchmark, generated_segment_options,
        max_inter_completion_gap, read_and_verify_remote, validate_saturation_sequence,
    };
    use crate::config::Settings;
    use bytes::Bytes;
    use object_store::memory::InMemory;
    use object_store::path::Path;
    use object_store::{ObjectStore, ObjectStoreExt};
    use std::sync::Arc;
    use std::time::Duration;
    use uuid::Uuid;

    #[test]
    fn inter_completion_gap_excludes_initial_ack_latency() {
        assert_eq!(
            max_inter_completion_gap(&[
                Duration::from_millis(900),
                Duration::from_millis(950),
                Duration::from_millis(1_075),
            ]),
            Duration::from_millis(125)
        );
    }

    #[test]
    fn saturation_ack_elapsed_uses_the_writer_completion_timestamp() {
        let activated = std::time::Instant::now();
        let acknowledged = activated + Duration::from_millis(25);

        assert_eq!(
            super::saturation_ack_elapsed(activated, acknowledged),
            Duration::from_millis(25)
        );
    }

    #[test]
    fn benchmark_object_names_are_canonical_and_run_scoped() {
        let run_id = Uuid::parse_str("12345678-1234-1234-1234-123456789abc").unwrap();
        let objects = BenchObjectSet::new(&Path::from("zerofs/prod"), run_id, 2).unwrap();
        let prefix = "zerofs/prod/.zerofs-writeback-bench-12345678123412341234123456789abc";

        assert_eq!(objects.database_prefix.as_ref(), prefix);
        assert_eq!(
            objects
                .objects
                .iter()
                .map(|path| path.as_ref())
                .collect::<Vec<_>>(),
            [
                format!("{prefix}/segments/01/1234567812341234/0000000000000001"),
                format!("{prefix}/segments/02/1234567812341234/0000000000000002"),
            ]
        );
        assert_eq!(
            objects
                .directories_deepest_first
                .last()
                .unwrap()
                .to_string_lossy(),
            prefix
        );
    }

    #[test]
    fn benchmark_uses_the_shipping_generated_segment_contract() {
        assert!(
            generated_segment_options()
                .extensions
                .get::<GeneratedSegmentCreate>()
                .is_some()
        );
    }

    #[test]
    fn mixed_benchmark_paths_insert_real_manifest_fences() {
        let run_id = Uuid::parse_str("12345678-1234-1234-1234-123456789abc").unwrap();
        let objects =
            BenchObjectSet::new_with_fences(&Path::from("zerofs/prod"), run_id, 8, 4).unwrap();

        assert_eq!(objects.fence_objects, 2);
        assert!(objects.objects[3].as_ref().contains("/manifest/"));
        assert!(objects.objects[7].as_ref().contains("/manifest/"));
        assert!(matches!(
            benchmark_put_options(objects.classes[3]).mode,
            object_store::PutMode::Create
        ));
        assert!(
            benchmark_put_options(objects.classes[3])
                .extensions
                .get::<GeneratedSegmentCreate>()
                .is_none(),
            "a SlateDB manifest is create-only but remains an ordered fence"
        );
        assert!(
            benchmark_put_options(objects.classes[0])
                .extensions
                .get::<GeneratedSegmentCreate>()
                .is_some()
        );
    }

    #[test]
    fn saturation_geometry_fills_the_high_watermark_and_leaves_a_blocked_tail() {
        let objects = BenchObjectSet::new_with_fences(
            &Path::from("zerofs/prod"),
            Uuid::parse_str("12345678-1234-1234-1234-123456789abc").unwrap(),
            8,
            4,
        )
        .unwrap();
        let segment_bytes = 8 * 1024 * 1024;
        let manifest_bytes = 64 * 1024;
        let segment_reservation =
            SaturationGeometry::reservation_bytes(&objects.objects[0], segment_bytes).unwrap();
        let geometry = SaturationGeometry::new(
            &objects.objects,
            &objects.classes,
            segment_bytes,
            manifest_bytes,
            segment_reservation * 4,
            95,
        )
        .unwrap();

        assert_eq!(geometry.prefill_objects, 4);
        assert_eq!(geometry.paced_objects, 4);
        assert!(geometry.reservation_bytes_min < geometry.reservation_bytes_max);
        assert!(geometry.prefill_reserved_bytes <= geometry.high_watermark_bytes);
        assert!(
            geometry.prefill_reserved_bytes + geometry.reservation_bytes_max
                > geometry.high_watermark_bytes
        );
        assert_eq!(
            geometry.paced_payload_bytes,
            u64::try_from(segment_bytes * 3 + manifest_bytes).unwrap()
        );
    }

    #[test]
    fn saturation_tail_requires_each_path_to_keep_its_planned_sequence() {
        validate_saturation_sequence(4, 5).unwrap();

        let error = validate_saturation_sequence(4, 6).unwrap_err();
        assert_eq!(
            format!("{error:#}"),
            "saturation object 4 received sequence 6 instead of planned sequence 5"
        );
    }

    #[test]
    fn saturation_writer_pool_is_bounded_independently_of_object_count() {
        assert_eq!(super::saturation_writer_slots(262_144, 8), 8);
        assert_eq!(super::saturation_writer_slots(4, 8), 4);
    }

    #[test]
    fn simultaneous_benchmark_and_teardown_failures_are_all_reported() {
        let error = finish_benchmark(
            Err::<(), _>(
                anyhow::anyhow!("primary failure").context("SFTP writeback benchmark failed"),
            ),
            [
                Err(anyhow::anyhow!("shutdown failure").context("writeback shutdown failed")),
                Err(anyhow::anyhow!("remote cleanup failure")
                    .context("remote benchmark cleanup failed")),
                Err(anyhow::anyhow!("pool shutdown failure").context("SFTP pool shutdown failed")),
                Err(anyhow::anyhow!("scratch close failure")
                    .context("remove local benchmark journal")),
                Err(anyhow::anyhow!("scratch remains")
                    .context("verify local benchmark journal removal")),
            ],
        )
        .unwrap_err();

        assert_eq!(
            format!("{error:#}"),
            "SFTP writeback benchmark failed: primary failure; \
             writeback shutdown failed: shutdown failure; \
             remote benchmark cleanup failed: remote cleanup failure; \
             SFTP pool shutdown failed: pool shutdown failure; \
             remove local benchmark journal: scratch close failure; \
             verify local benchmark journal removal: scratch remains"
        );
    }

    #[tokio::test]
    async fn remote_read_phase_verifies_every_object_and_reports_elapsed_time() {
        let remote: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let payload = Bytes::from_static(b"verified remote payload");
        let paths = (0..4)
            .map(|index| Path::from(format!("bench/{index}")))
            .collect::<Vec<_>>();
        for path in &paths {
            remote.put(path, payload.clone().into()).await.unwrap();
        }

        let classes = vec![BenchObjectClass::Segment; paths.len()];
        let (elapsed, verified) =
            read_and_verify_remote(&remote, &paths, &classes, &payload, &payload, 2)
                .await
                .unwrap();

        assert_eq!(verified, paths.len());
        assert!(!elapsed.is_zero());
    }

    #[tokio::test]
    async fn saturation_writer_panics_are_reported_by_the_completion_owner() {
        let mut writers = tokio::task::JoinSet::new();
        writers.spawn(async {
            panic!("injected saturation writer panic");
            #[allow(unreachable_code)]
            (0, Ok(1), std::time::Instant::now())
        });

        let error = super::next_saturation_completion(&mut writers)
            .await
            .unwrap_err();
        assert!(
            format!("{error:#}").contains("injected saturation writer panic"),
            "panic cause must be preserved: {error:#}"
        );
    }

    #[tokio::test]
    async fn benchmark_rejects_a_root_prefix_before_transport_preflight() {
        let settings: Settings = toml::from_str(
            r#"
[cache]
dir = "/tmp/zerofs-benchmark-prefix-test-cache"
disk_size_gb = 1.0
memory_size_gb = 1.0

[storage]
url = "sftp://benchmark@example.test/"
encryption_password = "test-only"

[sftp]
identity_file = "/definitely/missing/identity"
known_hosts = "/definitely/missing/known-hosts"

[servers]
"#,
        )
        .unwrap();

        let error = build_remote_store(&settings).await.unwrap_err();

        assert!(
            format!("{error:#}").contains("non-root dedicated prefix"),
            "prefix validation must run before transport preflight: {error:#}"
        );
    }
}
