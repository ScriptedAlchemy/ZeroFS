use super::config::{AckMode, ShutdownFlush, WritebackSettings};
use super::journal::Journal;
use super::model::{
    FenceClass, JournalIdentity, LocalEtag, MutationKind, MutationMode, MutationRecord,
};
use super::overlay::OverlayIndex;
use super::sftp_bench::finish_benchmark;
use super::store::WritebackObjectStore;
use crate::segment_store::GeneratedSegmentCreate;
use anyhow::{Context, Result};
use bytes::Bytes;
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::{ObjectStore, PutMode, PutOptions};
use rand::{RngCore, SeedableRng, rngs::StdRng};
use serde::Serialize;
use sha2::{Digest, Sha256};
use std::path::{Path as FilePath, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};
use uuid::Uuid;

const TOTAL_MIB_ENV: &str = "ZEROFS_BENCH_TIER_TOTAL_MIB";
const PAYLOAD_KIB_ENV: &str = "ZEROFS_BENCH_TIER_PAYLOAD_KIB";
const WRITERS_ENV: &str = "ZEROFS_BENCH_TIER_WRITERS";
const READERS_ENV: &str = "ZEROFS_BENCH_TIER_READERS";
const LOCAL_CONCURRENCY_ENV: &str = "ZEROFS_BENCH_LOCAL_CONCURRENCY";
const BENCH_DIR_ENV: &str = "ZEROFS_BENCH_DIR";

#[derive(Debug, Clone, Copy)]
struct LocalTierGeometry {
    object_count: usize,
    payload_bytes: usize,
    writers: usize,
    readers: usize,
    local_concurrency: usize,
}

#[derive(Debug, Serialize)]
struct LocalTierReport {
    total_bytes: u64,
    object_count: usize,
    payload_bytes: usize,
    writers: usize,
    readers: usize,
    local_concurrency: usize,
    ram_write_seconds: f64,
    ram_write_mib_per_second: f64,
    ram_read_seconds: f64,
    ram_read_mib_per_second: f64,
    ram_read_verified_objects: usize,
    ssd_write_seconds: f64,
    ssd_write_mib_per_second: f64,
    ssd_drain_after_ram_ack_seconds: f64,
    ssd_read_seconds: f64,
    ssd_read_mib_per_second: f64,
    ssd_read_verified_objects: usize,
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

fn generated_segment_options() -> PutOptions {
    let mut options = PutOptions::from(PutMode::Create);
    options.extensions.insert(GeneratedSegmentCreate);
    options
}

fn object_paths(object_count: usize) -> Vec<Path> {
    (0..object_count)
        .map(|index| {
            let counter = index as u64 + 1;
            Path::from(format!(
                "zerofs/tier-bench/segments/{:02x}/0000000000000001/{counter:016x}",
                counter & 0xff
            ))
        })
        .collect()
}

fn mutation_record(sequence: u64, path: &Path, payload: &[u8]) -> MutationRecord {
    let payload_sha256: [u8; 32] = Sha256::digest(payload).into();
    MutationRecord {
        format_version: 1,
        sequence,
        operation_id: Uuid::new_v4(),
        path: path.to_string(),
        kind: MutationKind::Put {
            mode: MutationMode::Create,
            expected_visible_version: None,
            payload_len: payload.len() as u64,
            payload_sha256,
            blob_path: String::new(),
        },
        local_etag: LocalEtag::new(Uuid::nil(), sequence),
        accepted_at_unix_ms: 1_786_435_200_000,
        remote_predecessor_etag: None,
        remote_result_etag: None,
        fence: FenceClass::ImmutableCreate,
        retry_count: 0,
        last_error: None,
    }
}

async fn read_ram_overlay(
    paths: &[Path],
    payload: &Bytes,
    readers: usize,
) -> Result<(Duration, usize)> {
    let remote: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let overlay = OverlayIndex::new(remote);
    for (index, path) in paths.iter().enumerate() {
        overlay
            .install_memory(
                mutation_record(index as u64 + 1, path, payload),
                payload.clone(),
            )
            .await?;
    }
    let paths = Arc::new(paths.to_vec());
    let started = Instant::now();
    let mut tasks = tokio::task::JoinSet::new();
    for reader in 0..readers {
        let overlay = overlay.clone();
        let paths = Arc::clone(&paths);
        let payload = payload.clone();
        tasks.spawn(async move {
            let mut verified = 0;
            let mut index = reader;
            while index < paths.len() {
                let readback = overlay.get(&paths[index]).await?.bytes().await?;
                anyhow::ensure!(
                    readback == payload,
                    "RAM readback mismatch for {}: expected {} bytes, got {}",
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
        verified += result.context("RAM reader task failed")??;
    }
    Ok((started.elapsed(), verified))
}

async fn read_ssd_journal(
    journal: &Arc<Journal>,
    object_count: usize,
    payload: &Bytes,
    readers: usize,
) -> Result<(Duration, usize)> {
    let started = Instant::now();
    let mut tasks = tokio::task::JoinSet::new();
    for reader in 0..readers {
        let journal = Arc::clone(journal);
        let payload = payload.clone();
        tasks.spawn_blocking(move || {
            let mut verified = 0;
            let mut index = reader;
            while index < object_count {
                let sequence = index as u64 + 1;
                let readback = journal.read_blob(sequence)?;
                anyhow::ensure!(
                    readback.as_slice() == payload.as_ref(),
                    "SSD readback mismatch for sequence {sequence}: expected {} bytes, got {}",
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
        verified += result.context("SSD reader task failed")??;
    }
    Ok((started.elapsed(), verified))
}

async fn execute_local_tiers(
    store: &WritebackObjectStore,
    journal: &Arc<Journal>,
    geometry: LocalTierGeometry,
    paths: &[Path],
    payload: Bytes,
) -> Result<LocalTierReport> {
    let total_bytes = u64::try_from(geometry.object_count)?
        .checked_mul(u64::try_from(geometry.payload_bytes)?)
        .context("local tier benchmark byte count overflowed")?;
    let payload_sha256 = format!("{:x}", Sha256::digest(&payload));

    let paths = Arc::new(paths.to_vec());
    let write_started = Instant::now();
    let mut tasks = tokio::task::JoinSet::new();
    for writer in 0..geometry.writers {
        let store = store.clone();
        let paths = Arc::clone(&paths);
        let payload = payload.clone();
        tasks.spawn(async move {
            let mut index = writer;
            while index < paths.len() {
                store
                    .put_opts(
                        &paths[index],
                        payload.clone().into(),
                        generated_segment_options(),
                    )
                    .await?;
                index += geometry.writers;
            }
            object_store::Result::<()>::Ok(())
        });
    }
    while let Some(result) = tasks.join_next().await {
        result.context("local tier writer task failed")??;
    }
    let ram_write = write_started.elapsed();
    store.wait_local(geometry.object_count as u64).await?;
    let ssd_write = write_started.elapsed();

    let (ram_read, ram_read_verified_objects) =
        read_ram_overlay(paths.as_slice(), &payload, geometry.readers).await?;
    let (ssd_read, ssd_read_verified_objects) =
        read_ssd_journal(journal, geometry.object_count, &payload, geometry.readers).await?;

    Ok(LocalTierReport {
        total_bytes,
        object_count: geometry.object_count,
        payload_bytes: geometry.payload_bytes,
        writers: geometry.writers,
        readers: geometry.readers,
        local_concurrency: geometry.local_concurrency,
        ram_write_seconds: ram_write.as_secs_f64(),
        ram_write_mib_per_second: mib_per_second(total_bytes, ram_write),
        ram_read_seconds: ram_read.as_secs_f64(),
        ram_read_mib_per_second: mib_per_second(total_bytes, ram_read),
        ram_read_verified_objects,
        ssd_write_seconds: ssd_write.as_secs_f64(),
        ssd_write_mib_per_second: mib_per_second(total_bytes, ssd_write),
        ssd_drain_after_ram_ack_seconds: ssd_write.saturating_sub(ram_write).as_secs_f64(),
        ssd_read_seconds: ssd_read.as_secs_f64(),
        ssd_read_mib_per_second: mib_per_second(total_bytes, ssd_read),
        ssd_read_verified_objects,
        payload_sha256,
    })
}

async fn run_local_tier_benchmark(
    scratch_parent: &FilePath,
    geometry: LocalTierGeometry,
) -> Result<LocalTierReport> {
    anyhow::ensure!(geometry.object_count > 0, "object count must be positive");
    anyhow::ensure!(geometry.payload_bytes > 0, "payload size must be positive");
    anyhow::ensure!(geometry.writers > 0, "writer count must be positive");
    anyhow::ensure!(geometry.readers > 0, "reader count must be positive");
    anyhow::ensure!(
        geometry.local_concurrency > 0,
        "local concurrency must be positive"
    );
    let total_bytes = u64::try_from(geometry.object_count)?
        .checked_mul(u64::try_from(geometry.payload_bytes)?)
        .context("local tier benchmark byte count overflowed")?;
    let scratch = tempfile::tempdir_in(scratch_parent)?;
    let scratch_path = scratch.path().to_path_buf();
    let journal_dir = scratch.path().join("writeback");
    let journal = Arc::new(Journal::open(
        &journal_dir,
        JournalIdentity {
            format_version: 1,
            bucket_id: format!("local-tier-bench-{}", Uuid::new_v4().simple()),
            backend_endpoint: "memory://unused-paused-remote".to_owned(),
            database_prefix: "zerofs/tier-bench".to_owned(),
            backend_kind: "memory".to_owned(),
            encryption_key_identity_sha256: [0x54; 32],
        },
    )?);
    let remote: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let settings = WritebackSettings {
        dir: journal_dir,
        ack_mode: AckMode::Memory,
        memory_bytes: total_bytes.saturating_add(64 << 20),
        disk_bytes: total_bytes.saturating_mul(2).saturating_add(256 << 20),
        min_free_bytes: 1,
        high_watermark_percent: 95,
        resume_percent: 85,
        upload_concurrency: 1,
        local_concurrency: geometry.local_concurrency,
        shutdown_flush: ShutdownFlush::Local,
    };
    let store =
        match WritebackObjectStore::open_paused(remote, Arc::clone(&journal), settings).await {
            Ok(store) => store,
            Err(error) => {
                drop(journal);
                let scratch_cleanup = scratch
                    .close()
                    .context("remove local tier benchmark scratch");
                let scratch_absence = if scratch_path.exists() {
                    Err(anyhow::anyhow!(
                        "local tier benchmark scratch still exists at {}",
                        scratch_path.display()
                    ))
                    .context("verify local tier benchmark scratch removal")
                } else {
                    Ok(())
                };
                return finish_benchmark(
                    Err(error.context("open local tier benchmark store")),
                    [scratch_cleanup, scratch_absence],
                );
            }
        };

    let mut payload = vec![0_u8; geometry.payload_bytes];
    StdRng::seed_from_u64(0x5a45_524f_4653_5449).fill_bytes(&mut payload);
    let paths = object_paths(geometry.object_count);
    let benchmark =
        execute_local_tiers(&store, &journal, geometry, &paths, Bytes::from(payload)).await;
    let shutdown = store.shutdown().await;
    drop(store);
    drop(journal);
    let scratch_cleanup = scratch
        .close()
        .context("remove local tier benchmark scratch");
    let scratch_absence = if scratch_path.exists() {
        Err(anyhow::anyhow!(
            "local tier benchmark scratch still exists at {}",
            scratch_path.display()
        ))
        .context("verify local tier benchmark scratch removal")
    } else {
        Ok(())
    };
    finish_benchmark(
        benchmark.context("local tier read/write benchmark failed"),
        [
            shutdown.context("local tier writeback shutdown failed"),
            scratch_cleanup,
            scratch_absence,
        ],
    )
}

/// Directly measures the shipping writeback RAM overlay and local SSD journal
/// in the normal dev test profile. The remote scheduler stays paused because
/// this benchmark deliberately measures only the two host-local tiers.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "local RAM/SSD read/write benchmark; run explicitly"]
async fn bench_writeback_local_tier_read_write() -> Result<()> {
    let total_mib: usize = bench_env(TOTAL_MIB_ENV, 256)?;
    let payload_kib: usize = bench_env(PAYLOAD_KIB_ENV, 1024)?;
    let writers: usize = bench_env(WRITERS_ENV, 16)?;
    let readers: usize = bench_env(READERS_ENV, writers)?;
    let local_concurrency: usize = bench_env(LOCAL_CONCURRENCY_ENV, 8)?;
    anyhow::ensure!(total_mib > 0, "{TOTAL_MIB_ENV} must be positive");
    anyhow::ensure!(payload_kib > 0, "{PAYLOAD_KIB_ENV} must be positive");
    let total_kib = total_mib
        .checked_mul(1024)
        .context("local tier benchmark size overflowed")?;
    anyhow::ensure!(
        total_kib.is_multiple_of(payload_kib),
        "{TOTAL_MIB_ENV} must be divisible by {PAYLOAD_KIB_ENV}"
    );
    let scratch_parent = std::env::var_os(BENCH_DIR_ENV)
        .map(PathBuf::from)
        .unwrap_or_else(std::env::temp_dir);
    let report = run_local_tier_benchmark(
        &scratch_parent,
        LocalTierGeometry {
            object_count: total_kib / payload_kib,
            payload_bytes: payload_kib
                .checked_mul(1024)
                .context("local tier payload size overflowed")?,
            writers,
            readers,
            local_concurrency,
        },
    )
    .await?;
    println!("LOCAL_TIER_RW_BENCH {}", serde_json::to_string(&report)?);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{LocalTierGeometry, run_local_tier_benchmark};

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn local_tier_benchmark_reads_production_ram_and_ssd_paths_and_removes_scratch() {
        let parent = tempfile::tempdir().unwrap();
        let report = run_local_tier_benchmark(
            parent.path(),
            LocalTierGeometry {
                object_count: 4,
                payload_bytes: 64 * 1024,
                writers: 2,
                readers: 2,
                local_concurrency: 2,
            },
        )
        .await
        .unwrap();

        assert_eq!(report.total_bytes, 4 * 64 * 1024);
        assert_eq!(report.object_count, 4);
        assert_eq!(report.ram_read_verified_objects, 4);
        assert_eq!(report.ssd_read_verified_objects, 4);
        assert!(report.ram_write_mib_per_second.is_finite());
        assert!(report.ram_read_mib_per_second.is_finite());
        assert!(report.ssd_write_mib_per_second.is_finite());
        assert!(report.ssd_read_mib_per_second.is_finite());
        assert!(std::fs::read_dir(parent.path()).unwrap().next().is_none());
    }
}
