use crate::config::Settings;
use crate::segment_store::GeneratedSegmentCreate;
use crate::sftp_object_store::SftpObjectStore;
use crate::sftp_transport::{
    OperationKind, RusshSessionFactory, SessionFactory, SftpSessionPool, TransportError,
};
use crate::writeback::config::{AckMode, ShutdownFlush, WritebackAccessMode, WritebackSettings};
use crate::writeback::journal::Journal;
use crate::writeback::model::{JournalIdentity, MutationRecord};
use crate::writeback::store::WritebackObjectStore;
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
const WRITERS_ENV: &str = "ZEROFS_BENCH_SFTP_WRITERS";
const MAX_CONNECTIONS_ENV: &str = "ZEROFS_BENCH_SFTP_MAX_CONNECTIONS";
const SSD_MIB_ENV: &str = "ZEROFS_BENCH_SFTP_SSD_MIB";
const FENCE_EVERY_ENV: &str = "ZEROFS_BENCH_SFTP_FENCE_EVERY";
const BENCH_DIR_ENV: &str = "ZEROFS_BENCH_DIR";
const IDENTITY_FILE_ENV: &str = "ZEROFS_BENCH_SFTP_IDENTITY_FILE";
const KNOWN_HOSTS_ENV: &str = "ZEROFS_BENCH_SFTP_KNOWN_HOSTS";

#[derive(Debug)]
struct BenchObjectSet {
    database_prefix: ObjectPath,
    objects: Vec<ObjectPath>,
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
        let mut directories = BTreeSet::new();
        directories.insert(PathBuf::from(database_prefix.as_ref()));
        directories.insert(PathBuf::from(format!("{database_prefix}/segments")));
        let mut fence_objects = 0;

        for index in 0..object_count {
            let counter = u64::try_from(index)?.saturating_add(1);
            if fence_every != 0 && usize::try_from(counter)?.is_multiple_of(fence_every) {
                directories.insert(PathBuf::from(format!("{database_prefix}/manifest")));
                objects.push(ObjectPath::parse(format!(
                    "{database_prefix}/manifest/{counter:020}.manifest"
                ))?);
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
    sftp_publications: u64,
    sftp_open_total_seconds: f64,
    sftp_write_total_seconds: f64,
    sftp_fsync_total_seconds: f64,
    sftp_close_total_seconds: f64,
    sftp_hardlink_total_seconds: f64,
    sftp_remove_total_seconds: f64,
    sftp_session_publications: Vec<u64>,
    sftp_session_write_bytes: Vec<u64>,
    payload_sha256: String,
}

#[derive(Debug, Serialize)]
struct SaturationReport {
    total_bytes: u64,
    object_count: usize,
    payload_bytes: usize,
    writers: usize,
    max_connections: usize,
    upload_concurrency: usize,
    local_concurrency: usize,
    ssd_capacity_bytes: u64,
    ssd_high_watermark_bytes: u64,
    ssd_reservation_bytes_per_object: u64,
    prefill_objects: usize,
    prefill_reserved_bytes: u64,
    blocked_objects_before_activation: usize,
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
    sftp_publications: u64,
    sftp_open_total_seconds: f64,
    sftp_write_total_seconds: f64,
    sftp_fsync_total_seconds: f64,
    sftp_close_total_seconds: f64,
    sftp_hardlink_total_seconds: f64,
    sftp_remove_total_seconds: f64,
    sftp_session_publications: Vec<u64>,
    sftp_session_write_bytes: Vec<u64>,
    payload_sha256: String,
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
    reservation_bytes: u64,
    high_watermark_bytes: u64,
    prefill_reserved_bytes: u64,
    prefill_objects: usize,
    paced_objects: usize,
}

impl SaturationGeometry {
    fn reservation_bytes(path: &ObjectPath, payload_bytes: usize) -> Result<u64> {
        MutationRecord::ssd_reservation_estimate(path.as_ref(), None, u64::try_from(payload_bytes)?)
            .context("estimate benchmark SSD reservation")
    }

    fn new(
        objects: &[ObjectPath],
        payload_bytes: usize,
        disk_bytes: u64,
        high_watermark_percent: u8,
    ) -> Result<Self> {
        anyhow::ensure!(!objects.is_empty(), "saturation benchmark needs objects");
        anyhow::ensure!(payload_bytes > 0, "saturation payload must be positive");
        anyhow::ensure!(
            (1..=100).contains(&high_watermark_percent),
            "saturation high watermark must be between 1 and 100"
        );
        let reservation_bytes = Self::reservation_bytes(&objects[0], payload_bytes)?;
        for path in &objects[1..] {
            anyhow::ensure!(
                Self::reservation_bytes(path, payload_bytes)? == reservation_bytes,
                "saturation benchmark object paths must reserve equally"
            );
        }
        let high_watermark_bytes =
            u64::try_from(u128::from(disk_bytes) * u128::from(high_watermark_percent) / 100)?;
        let prefill_objects = usize::try_from(high_watermark_bytes / reservation_bytes)?;
        anyhow::ensure!(
            prefill_objects > 0,
            "saturation SSD budget cannot admit one payload"
        );
        anyhow::ensure!(
            prefill_objects < objects.len(),
            "saturation workload must exceed the SSD high watermark"
        );
        let prefill_reserved_bytes = reservation_bytes
            .checked_mul(u64::try_from(prefill_objects)?)
            .context("saturation prefill reservation overflowed")?;

        Ok(Self {
            reservation_bytes,
            high_watermark_bytes,
            prefill_reserved_bytes,
            prefill_objects,
            paced_objects: objects.len() - prefill_objects,
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

fn generated_segment_options() -> PutOptions {
    let mut options = PutOptions::from(PutMode::Create);
    options.extensions.insert(GeneratedSegmentCreate);
    options
}

fn benchmark_put_options(path: &ObjectPath) -> PutOptions {
    if path.as_ref().contains("/manifest/") {
        PutOptions::from(PutMode::Overwrite)
    } else {
        generated_segment_options()
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
    payload: &Bytes,
    readers: usize,
) -> Result<(Duration, usize)> {
    let paths = Arc::new(objects.to_vec());
    let started = Instant::now();
    let mut tasks = tokio::task::JoinSet::new();
    for reader in 0..readers {
        let remote = Arc::clone(remote);
        let paths = Arc::clone(&paths);
        let payload = payload.clone();
        tasks.spawn(async move {
            let mut verified = 0;
            let mut index = reader;
            while index < paths.len() {
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
    payload: Bytes,
    geometry: BenchGeometry,
) -> Result<BenchReport> {
    let total_bytes = u64::try_from(objects.len())?
        .checked_mul(u64::try_from(payload.len())?)
        .context("benchmark byte count overflowed")?;
    let mut hasher = Sha256::new();
    hasher.update(&payload);
    let payload_sha256 = format!("{:x}", hasher.finalize());
    let paths = Arc::new(objects.to_vec());
    crate::sftp_protocol::reset_bench_timing();

    let started = Instant::now();
    let mut tasks = tokio::task::JoinSet::new();
    for writer in 0..geometry.writers {
        let store = store.clone();
        let payload = payload.clone();
        let paths = Arc::clone(&paths);
        tasks.spawn(async move {
            let mut index = writer;
            while index < paths.len() {
                store
                    .put_opts(
                        &paths[index],
                        payload.clone().into(),
                        benchmark_put_options(&paths[index]),
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

    let (remote_read, remote_read_verified_objects) =
        read_and_verify_remote(remote, objects, &payload, geometry.writers).await?;

    Ok(BenchReport {
        total_bytes,
        object_count: objects.len(),
        payload_bytes: payload.len(),
        writers: geometry.writers,
        max_connections: geometry.max_connections,
        upload_concurrency: geometry.upload_concurrency,
        local_concurrency: geometry.local_concurrency,
        fence_objects: objects
            .iter()
            .filter(|path| path.as_ref().contains("/manifest/"))
            .count(),
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
        sftp_publications: sftp_timing.publications,
        sftp_open_total_seconds: seconds(sftp_timing.open_nanos),
        sftp_write_total_seconds: seconds(sftp_timing.write_nanos),
        sftp_fsync_total_seconds: seconds(sftp_timing.fsync_nanos),
        sftp_close_total_seconds: seconds(sftp_timing.close_nanos),
        sftp_hardlink_total_seconds: seconds(sftp_timing.hardlink_nanos),
        sftp_remove_total_seconds: seconds(sftp_timing.remove_nanos),
        sftp_session_publications: sftp_timing.session_publications[..last_used_session].to_vec(),
        sftp_session_write_bytes: sftp_timing.session_write_bytes[..last_used_session].to_vec(),
        payload_sha256,
    })
}

async fn execute_saturation_benchmark(
    store: &WritebackObjectStore,
    remote: &Arc<dyn ObjectStore>,
    objects: &[ObjectPath],
    payload: Bytes,
    geometry: BenchGeometry,
    saturation: SaturationGeometry,
) -> Result<SaturationReport> {
    let total_bytes = u64::try_from(objects.len())?
        .checked_mul(u64::try_from(payload.len())?)
        .context("saturation benchmark byte count overflowed")?;
    let blocked_bytes = u64::try_from(saturation.paced_objects)?
        .checked_mul(u64::try_from(payload.len())?)
        .context("saturation blocked byte count overflowed")?;
    let mut hasher = Sha256::new();
    hasher.update(&payload);
    let payload_sha256 = format!("{:x}", hasher.finalize());
    let paths = Arc::new(objects.to_vec());
    crate::sftp_protocol::reset_bench_timing();

    let (completion_sender, mut completion_receiver) =
        tokio::sync::mpsc::unbounded_channel::<(usize, std::result::Result<(), String>)>();
    let mut writers = tokio::task::JoinSet::new();
    let prefill_started = Instant::now();
    for writer in 0..geometry.writers {
        let store = store.clone();
        let payload = payload.clone();
        let paths = Arc::clone(&paths);
        let completion_sender = completion_sender.clone();
        writers.spawn(async move {
            let mut index = writer;
            while index < paths.len() {
                let result = store
                    .put_opts(
                        &paths[index],
                        payload.clone().into(),
                        generated_segment_options(),
                    )
                    .await
                    .map(|_| ())
                    .map_err(|error| error.to_string());
                let failed = result.is_err();
                if completion_sender.send((index, result)).is_err() {
                    break;
                }
                if failed {
                    break;
                }
                index += geometry.writers;
            }
        });
    }
    drop(completion_sender);

    for _ in 0..saturation.prefill_objects {
        let (index, result) =
            tokio::time::timeout(Duration::from_secs(60), completion_receiver.recv())
                .await
                .context("timed out filling the benchmark SSD high watermark")?
                .context("benchmark writers exited before filling the SSD high watermark")?;
        result.map_err(|error| anyhow::anyhow!("prefill object {index} failed: {error}"))?;
    }
    tokio::time::timeout(
        Duration::from_secs(60),
        store.wait_local(u64::try_from(saturation.prefill_objects)?),
    )
    .await
    .context("timed out waiting for saturation prefill durability")??;
    let prefill = prefill_started.elapsed();
    tokio::time::sleep(Duration::from_millis(100)).await;
    match completion_receiver.try_recv() {
        Err(tokio::sync::mpsc::error::TryRecvError::Empty) => {}
        Err(tokio::sync::mpsc::error::TryRecvError::Disconnected) => {
            anyhow::bail!("benchmark writers exited instead of blocking at the SSD watermark")
        }
        Ok((index, result)) => {
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
        for _ in 0..saturation.paced_objects {
            let (index, result) = completion_receiver
                .recv()
                .await
                .context("benchmark writers exited before the blocked tail was admitted")?;
            result.map_err(|error| anyhow::anyhow!("blocked object {index} failed: {error}"))?;
            completions.push(activated.elapsed());
        }
        Result::<Vec<Duration>>::Ok(completions)
    })
    .await
    .context("blocked foreground writes did not progress after remote activation")??;
    while let Some(result) = writers.join_next().await {
        result.context("saturation writer task failed")?;
    }
    let target = store.status()?.accepted_seq;
    anyhow::ensure!(
        target == u64::try_from(objects.len())?,
        "expected {} accepted objects after pacing, observed {target}",
        objects.len()
    );
    tokio::time::timeout(Duration::from_secs(60), store.wait_local(target))
        .await
        .context("timed out waiting for the paced tail to become locally durable")??;
    let after_blocked_acks = store.status()?;
    let blocked_ack = blocked_ack_times
        .last()
        .copied()
        .context("saturation benchmark did not record blocked acknowledgements")?;
    let first_blocked_ack = blocked_ack_times[0];
    let mut previous = Duration::ZERO;
    let mut max_blocked_ack_gap = Duration::ZERO;
    for completion in blocked_ack_times {
        max_blocked_ack_gap = max_blocked_ack_gap.max(completion.saturating_sub(previous));
        previous = completion;
    }
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
    let (remote_read, remote_read_verified_objects) =
        read_and_verify_remote(remote, objects, &payload, geometry.writers).await?;

    Ok(SaturationReport {
        total_bytes,
        object_count: objects.len(),
        payload_bytes: payload.len(),
        writers: geometry.writers,
        max_connections: geometry.max_connections,
        upload_concurrency: geometry.upload_concurrency,
        local_concurrency: geometry.local_concurrency,
        ssd_capacity_bytes: final_status.dirty_ssd_capacity_bytes,
        ssd_high_watermark_bytes: saturation.high_watermark_bytes,
        ssd_reservation_bytes_per_object: saturation.reservation_bytes,
        prefill_objects: saturation.prefill_objects,
        prefill_reserved_bytes: saturation.prefill_reserved_bytes,
        blocked_objects_before_activation: saturation.paced_objects,
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
        sftp_publications: sftp_timing.publications,
        sftp_open_total_seconds: seconds(sftp_timing.open_nanos),
        sftp_write_total_seconds: seconds(sftp_timing.write_nanos),
        sftp_fsync_total_seconds: seconds(sftp_timing.fsync_nanos),
        sftp_close_total_seconds: seconds(sftp_timing.close_nanos),
        sftp_hardlink_total_seconds: seconds(sftp_timing.hardlink_nanos),
        sftp_remove_total_seconds: seconds(sftp_timing.remove_nanos),
        sftp_session_publications: sftp_timing.session_publications[..last_used_session].to_vec(),
        sftp_session_write_bytes: sftp_timing.session_write_bytes[..last_used_session].to_vec(),
        payload_sha256,
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
    let remote_directory_prepare =
        prepare_remote_run(&pool, &objects.directories_deepest_first).await;
    let benchmark = match remote_directory_prepare {
        Ok(remote_directory_prepare) => {
            execute_benchmark(
                &store,
                &remote,
                &objects.objects,
                Bytes::from(payload),
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
    let writers: usize = bench_env(WRITERS_ENV, 16)?;
    let ssd_mib: usize = bench_env(SSD_MIB_ENV, 64)?;
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

    let (remote, base, pool) = build_remote_store(&settings).await?;
    let setup = (|| -> Result<_> {
        let objects = BenchObjectSet::new(&base, Uuid::new_v4(), object_count)?;
        let saturation = SaturationGeometry::new(&objects.objects, payload_bytes, disk_bytes, 95)?;
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
    let remote_directory_prepare =
        prepare_remote_run(&pool, &objects.directories_deepest_first).await;
    let benchmark = match remote_directory_prepare {
        Ok(remote_directory_prepare) => {
            execute_saturation_benchmark(
                &store,
                &remote,
                &objects.objects,
                Bytes::from(payload),
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
        BenchObjectSet, GeneratedSegmentCreate, SaturationGeometry, benchmark_put_options,
        build_remote_store, finish_benchmark, generated_segment_options, read_and_verify_remote,
    };
    use crate::config::Settings;
    use bytes::Bytes;
    use object_store::memory::InMemory;
    use object_store::path::Path;
    use object_store::{ObjectStore, ObjectStoreExt};
    use std::sync::Arc;
    use uuid::Uuid;

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
            benchmark_put_options(&objects.objects[3]).mode,
            object_store::PutMode::Overwrite
        ));
        assert!(
            benchmark_put_options(&objects.objects[0])
                .extensions
                .get::<GeneratedSegmentCreate>()
                .is_some()
        );
    }

    #[test]
    fn saturation_geometry_fills_the_high_watermark_and_leaves_a_blocked_tail() {
        let objects = BenchObjectSet::new(
            &Path::from("zerofs/prod"),
            Uuid::parse_str("12345678-1234-1234-1234-123456789abc").unwrap(),
            8,
        )
        .unwrap();
        let payload_bytes = 8 * 1024 * 1024;
        let reservation =
            SaturationGeometry::reservation_bytes(&objects.objects[0], payload_bytes).unwrap();
        let geometry =
            SaturationGeometry::new(&objects.objects, payload_bytes, reservation * 4, 95).unwrap();

        assert_eq!(geometry.prefill_objects, 3);
        assert_eq!(geometry.paced_objects, 5);
        assert!(geometry.prefill_reserved_bytes <= geometry.high_watermark_bytes);
        assert!(
            geometry.prefill_reserved_bytes + geometry.reservation_bytes
                > geometry.high_watermark_bytes
        );
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

        let (elapsed, verified) = read_and_verify_remote(&remote, &paths, &payload, 2)
            .await
            .unwrap();

        assert_eq!(verified, paths.len());
        assert!(!elapsed.is_zero());
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
