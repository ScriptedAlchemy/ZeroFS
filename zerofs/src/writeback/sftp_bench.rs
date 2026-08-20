use crate::config::{Settings, SftpSshTransport};
use crate::sftp_object_store::SftpObjectStore;
use crate::sftp_transport::{
    OperationKind, RusshSessionFactory, SessionFactory, SftpSessionPool, TransportError,
};
use crate::writeback::config::{AckMode, ShutdownFlush, WritebackAccessMode, WritebackSettings};
use crate::writeback::journal::Journal;
use crate::writeback::model::JournalIdentity;
use crate::writeback::store::WritebackObjectStore;
use anyhow::{Context, Result};
use bytes::Bytes;
use object_store::path::Path as ObjectPath;
use object_store::{ObjectStore, ObjectStoreExt, PutMode, PutOptions};
use rand::{RngCore, SeedableRng, rngs::StdRng};
use serde::Serialize;
use sha2::{Digest, Sha256};
use std::collections::BTreeSet;
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
    let config = settings.sftp.clone().unwrap_or_default();
    let factory: Arc<dyn SessionFactory> = match config.transport {
        SftpSshTransport::Russh => Arc::new(RusshSessionFactory::new(
            endpoint.clone(),
            config.identity_file.clone(),
            config.known_hosts.clone(),
        )?),
        SftpSshTransport::HpnOpenSsh => Arc::new(
            crate::hpn_session::HpnSessionFactory::from_config(endpoint.clone(), &config)?,
        ),
    };
    let pool = SftpSessionPool::from_config_writable(factory, &config).await?;
    let url = Url::parse(&settings.storage.url).context("parse benchmark SFTP URL")?;
    let path = ObjectPath::from_url_path(url.path())?;
    let store = SftpObjectStore::new(pool.clone(), path.clone())?;
    Ok((Arc::new(store), path, pool))
}

const CONFIG_ENV: &str = "ZEROFS_SFTP_WRITEBACK_BENCH_CONFIG";
const TOTAL_MIB_ENV: &str = "ZEROFS_BENCH_SFTP_TOTAL_MIB";
const PAYLOAD_KIB_ENV: &str = "ZEROFS_BENCH_SFTP_PAYLOAD_KIB";
const WRITERS_ENV: &str = "ZEROFS_BENCH_SFTP_WRITERS";
const BENCH_DIR_ENV: &str = "ZEROFS_BENCH_DIR";

#[derive(Debug)]
struct BenchObjectSet {
    database_prefix: ObjectPath,
    objects: Vec<ObjectPath>,
    directories_deepest_first: Vec<PathBuf>,
}

impl BenchObjectSet {
    fn new(base: &ObjectPath, run_id: Uuid, object_count: usize) -> Result<Self> {
        let token = run_id.simple().to_string();
        let database_prefix = ObjectPath::parse(format!("{base}/.zerofs-writeback-bench-{token}"))?;
        let epoch = u64::from_str_radix(&token[..16], 16)?;
        let mut objects = Vec::with_capacity(object_count);
        let mut directories = BTreeSet::new();
        directories.insert(PathBuf::from(database_prefix.as_ref()));
        directories.insert(PathBuf::from(format!("{database_prefix}/segments")));

        for index in 0..object_count {
            let counter = u64::try_from(index)?.saturating_add(1);
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
        })
    }
}

#[derive(Debug, Serialize)]
struct BenchReport {
    total_bytes: u64,
    object_count: usize,
    payload_bytes: usize,
    writers: usize,
    upload_concurrency: usize,
    local_concurrency: usize,
    ram_ack_seconds: f64,
    ram_ack_mib_per_second: f64,
    local_seconds: f64,
    local_mib_per_second: f64,
    remote_drain_seconds: f64,
    remote_drain_mib_per_second: f64,
    payload_sha256: String,
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

async fn execute_benchmark(
    store: &WritebackObjectStore,
    remote: &Arc<dyn ObjectStore>,
    objects: &[ObjectPath],
    payload: Bytes,
    writers: usize,
    upload_concurrency: usize,
    local_concurrency: usize,
) -> Result<BenchReport> {
    let total_bytes = u64::try_from(objects.len())?
        .checked_mul(u64::try_from(payload.len())?)
        .context("benchmark byte count overflowed")?;
    let mut hasher = Sha256::new();
    hasher.update(&payload);
    let payload_sha256 = format!("{:x}", hasher.finalize());
    let paths = Arc::new(objects.to_vec());

    let started = Instant::now();
    let mut tasks = tokio::task::JoinSet::new();
    for writer in 0..writers {
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
                        PutOptions::from(PutMode::Create),
                    )
                    .await?;
                index += writers;
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

    for path in objects {
        let readback = remote.get(path).await?.bytes().await?;
        anyhow::ensure!(
            readback == payload,
            "SFTP readback mismatch for {path}: expected {} bytes, got {}",
            payload.len(),
            readback.len()
        );
    }

    Ok(BenchReport {
        total_bytes,
        object_count: objects.len(),
        payload_bytes: payload.len(),
        writers,
        upload_concurrency,
        local_concurrency,
        ram_ack_seconds: ram_ack.as_secs_f64(),
        ram_ack_mib_per_second: mib_per_second(total_bytes, ram_ack),
        local_seconds: local.as_secs_f64(),
        local_mib_per_second: mib_per_second(total_bytes, local),
        remote_drain_seconds: remote_drain.as_secs_f64(),
        remote_drain_mib_per_second: mib_per_second(total_bytes, remote_drain),
        payload_sha256,
    })
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
    let settings = Settings::from_file(&config_path)?;
    anyhow::ensure!(
        settings.sftp_endpoint()?.is_some(),
        "{CONFIG_ENV} must configure an sftp:// storage URL"
    );
    let production_writeback = settings
        .writeback_settings(WritebackAccessMode::ReadWrite)?
        .context("benchmark config must enable [writeback]")?;

    let total_mib: usize = bench_env(TOTAL_MIB_ENV, 256)?;
    let payload_kib: usize = bench_env(PAYLOAD_KIB_ENV, 1024)?;
    let writers: usize = bench_env(WRITERS_ENV, 16)?;
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
    let objects = BenchObjectSet::new(&base, Uuid::new_v4(), object_count)?;

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
    let store =
        WritebackObjectStore::open_paused(Arc::clone(&remote), journal, bench_settings.clone())
            .await?;

    let mut payload = vec![0_u8; payload_bytes];
    StdRng::seed_from_u64(0x5f54_4653_4245_4e43).fill_bytes(&mut payload);
    let benchmark = execute_benchmark(
        &store,
        &remote,
        &objects.objects,
        Bytes::from(payload),
        writers,
        bench_settings.upload_concurrency,
        bench_settings.local_concurrency,
    )
    .await;
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

    let report = benchmark.context("SFTP writeback benchmark failed")?;
    shutdown.context("writeback shutdown failed")?;
    cleanup.context("remote benchmark cleanup failed")?;
    pool_shutdown.context("SFTP pool shutdown failed")?;
    println!("SFTP_WRITEBACK_BENCH {}", serde_json::to_string(&report)?);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::BenchObjectSet;
    use object_store::path::Path;
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
}
