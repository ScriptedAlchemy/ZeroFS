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

/// Base seed for deterministic per-object payload generation. Each object's
/// payload is derived from this seed folded with its index via `seed_for`,
/// so no two objects share content and a sequence-to-path mis-mapping during
/// read-back verification is detectable.
const PAYLOAD_BASE_SEED: u64 = 0x5a45_524f_4653_5449;

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
    /// Reads the on-disk journal blob through `Journal::read_blob`, which is a
    /// buffered read of a file this same process wrote seconds earlier — in
    /// practice served by the OS page cache, not raw SSD media. Treat this as
    /// a hot-cache journal replay figure, not a device-level read rate.
    ssd_journal_cached_read_seconds: f64,
    ssd_journal_cached_read_mib_per_second: f64,
    ssd_journal_cached_read_verified_objects: usize,
    /// Base seed (hex) from which every object's payload is derived via
    /// `seed_for(base_seed, index) = base_seed ^ index`. Payloads are no
    /// longer identical across objects, so there is no single payload hash
    /// to report; this seed plus the index reproduces any object's expected
    /// content or digest.
    payload_seed_base: String,
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
    total_bytes as f64 / (1024.0 * 1024.0) / elapsed.as_secs_f64().max(f64::EPSILON)
}

/// Folds `index` into `base_seed` so every object gets an independent,
/// reproducible seed. Not cryptographically strong and does not need to be:
/// `StdRng` (ChaCha-based) gives adjacent seeds unrelated output streams,
/// which is all that's required to make per-object payloads distinct.
fn seed_for(base_seed: u64, index: u64) -> u64 {
    base_seed ^ index
}

/// Deterministically generates a `len`-byte payload from `seed`. Used both
/// to produce the bytes actually written/read and, independently, to
/// regenerate the same bytes later purely to compute an expected digest —
/// always outside a timed window.
fn generate_payload(seed: u64, len: usize) -> Bytes {
    let mut buffer = vec![0_u8; len];
    StdRng::seed_from_u64(seed).fill_bytes(&mut buffer);
    Bytes::from(buffer)
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

/// Installs a fresh synthetic RAM overlay with one independently-seeded
/// payload per path, then times concurrent reads back against it. Each
/// readback is checked against a SHA-256 digest of that object's own
/// expected payload (not a single shared buffer), so a reader that returns
/// the wrong object's bytes is caught even though every object is the same
/// length. Payload generation and expected-digest hashing happen in the
/// install loop below, entirely before `started` is taken; only the
/// readback digest — unavoidably computed from data the timed read just
/// produced — runs inside the timed window.
async fn read_ram_overlay(
    paths: &[Path],
    base_seed: u64,
    payload_bytes: usize,
    readers: usize,
) -> Result<(Duration, usize)> {
    let remote: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let overlay = OverlayIndex::new(remote);
    let mut digests: Vec<[u8; 32]> = Vec::with_capacity(paths.len());
    for (index, path) in paths.iter().enumerate() {
        let payload = generate_payload(seed_for(base_seed, index as u64), payload_bytes);
        digests.push(Sha256::digest(&payload).into());
        overlay
            .install_memory(mutation_record(index as u64 + 1, path, &payload), payload)
            .await?;
    }
    let digests = Arc::new(digests);
    let paths = Arc::new(paths.to_vec());
    let started = Instant::now();
    let mut tasks = tokio::task::JoinSet::new();
    for reader in 0..readers {
        let overlay = overlay.clone();
        let paths = Arc::clone(&paths);
        let digests = Arc::clone(&digests);
        tasks.spawn(async move {
            let mut verified = 0;
            let mut index = reader;
            while index < paths.len() {
                let readback = overlay.get(&paths[index]).await?.bytes().await?;
                let readback_digest: [u8; 32] = Sha256::digest(&readback).into();
                anyhow::ensure!(
                    readback_digest == digests[index],
                    "RAM readback mismatch for {}: digest does not match the payload expected \
                     for sequence {} (got {} bytes)",
                    paths[index],
                    index + 1,
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

/// Times concurrent reads of the real on-disk journal blobs written earlier
/// in this run and verifies each against its own expected payload digest.
/// Expected digests are regenerated from `base_seed` and each object's
/// index — deterministic and independent of what was actually written —
/// entirely before `started` is taken, so digest computation for the
/// *expected* side never enters the timed window. Only hashing the actual
/// readback, which by definition can't exist before the read happens, runs
/// inside the timed window; this is the digest-based verification called
/// for when per-object buffer comparison would otherwise require either
/// holding all `object_count` payload buffers live through the read or
/// regenerating full payloads (proportional to `payload_bytes`) inside the
/// timed loop.
///
/// `index_to_sequence[i]` is the real journal sequence the store assigned
/// to object `i`'s write, taken from that write's own `PutResult` — not
/// assumed to be `i + 1`. Concurrent writers are admitted in real order,
/// not dispatch-loop order, so that assumption does not generally hold.
async fn read_ssd_journal(
    journal: &Arc<Journal>,
    index_to_sequence: &[u64],
    base_seed: u64,
    payload_bytes: usize,
    readers: usize,
) -> Result<(Duration, usize)> {
    let object_count = index_to_sequence.len();
    let mut digests: Vec<[u8; 32]> = Vec::with_capacity(object_count);
    for index in 0..object_count {
        let payload = generate_payload(seed_for(base_seed, index as u64), payload_bytes);
        digests.push(Sha256::digest(&payload).into());
    }
    let digests = Arc::new(digests);
    let index_to_sequence = Arc::new(index_to_sequence.to_vec());
    let started = Instant::now();
    let mut tasks = tokio::task::JoinSet::new();
    for reader in 0..readers {
        let journal = Arc::clone(journal);
        let digests = Arc::clone(&digests);
        let index_to_sequence = Arc::clone(&index_to_sequence);
        tasks.spawn_blocking(move || {
            let mut verified = 0;
            let mut index = reader;
            while index < object_count {
                let sequence = index_to_sequence[index];
                let readback = journal.read_blob(sequence)?;
                let readback_digest: [u8; 32] = Sha256::digest(&readback).into();
                anyhow::ensure!(
                    readback_digest == digests[index],
                    "SSD readback mismatch for sequence {sequence} (object index {index}): \
                     digest does not match the payload expected for this object (got {} bytes)",
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
    base_seed: u64,
) -> Result<LocalTierReport> {
    let total_bytes = u64::try_from(geometry.object_count)?
        .checked_mul(u64::try_from(geometry.payload_bytes)?)
        .context("local tier benchmark byte count overflowed")?;
    let payload_seed_base = format!("{base_seed:016x}");

    // Every object gets its own payload derived from `base_seed` folded with
    // its index, generated entirely before the write clock starts. This Vec
    // is the only place all `object_count` payload buffers are ever live at
    // once, and it is dropped immediately after the write phase consumes it
    // (see below) — read-side verification works from small per-object
    // digests instead, not from retained buffers.
    let payloads: Vec<Bytes> = (0..geometry.object_count)
        .map(|index| generate_payload(seed_for(base_seed, index as u64), geometry.payload_bytes))
        .collect();
    let payloads = Arc::new(payloads);

    let paths = Arc::new(paths.to_vec());
    let write_started = Instant::now();
    let mut tasks = tokio::task::JoinSet::new();
    for writer in 0..geometry.writers {
        let store = store.clone();
        let paths = Arc::clone(&paths);
        let payloads = Arc::clone(&payloads);
        tasks.spawn(async move {
            // The journal assigns each accepted write its own sequence number
            // based on real admission order across all concurrent writers,
            // which does not follow our dispatch loop's `index` order. Read
            // the actual assigned sequence back out of each PutResult's
            // e_tag rather than assuming `sequence == index + 1` — that
            // assumption was previously silently wrong and only harmless
            // because every object shared one payload.
            let mut assignments = Vec::new();
            let mut index = writer;
            while index < paths.len() {
                let result = store
                    .put_opts(
                        &paths[index],
                        payloads[index].clone().into(),
                        generated_segment_options(),
                    )
                    .await?;
                let sequence = result
                    .e_tag
                    .as_deref()
                    .and_then(LocalEtag::sequence_from_str)
                    .context(
                        "local tier benchmark: put result missing a parseable local sequence e_tag",
                    )?;
                assignments.push((index, sequence));
                index += geometry.writers;
            }
            Result::<Vec<(usize, u64)>>::Ok(assignments)
        });
    }
    let mut index_to_sequence = vec![0_u64; geometry.object_count];
    while let Some(result) = tasks.join_next().await {
        for (index, sequence) in result.context("local tier writer task failed")?? {
            index_to_sequence[index] = sequence;
        }
    }
    let ram_write = write_started.elapsed();
    store.wait_local(geometry.object_count as u64).await?;
    let ssd_write = write_started.elapsed();
    drop(payloads);

    let (ram_read, ram_read_verified_objects) = read_ram_overlay(
        paths.as_slice(),
        base_seed,
        geometry.payload_bytes,
        geometry.readers,
    )
    .await?;
    let (ssd_read, ssd_read_verified_objects) = read_ssd_journal(
        journal,
        &index_to_sequence,
        base_seed,
        geometry.payload_bytes,
        geometry.readers,
    )
    .await?;

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
        ssd_journal_cached_read_seconds: ssd_read.as_secs_f64(),
        ssd_journal_cached_read_mib_per_second: mib_per_second(total_bytes, ssd_read),
        ssd_journal_cached_read_verified_objects: ssd_read_verified_objects,
        payload_seed_base,
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

    let paths = object_paths(geometry.object_count);
    let benchmark =
        execute_local_tiers(&store, &journal, geometry, &paths, PAYLOAD_BASE_SEED).await;
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
    use super::{
        LocalTierGeometry, PAYLOAD_BASE_SEED, generate_payload, mib_per_second,
        run_local_tier_benchmark, seed_for,
    };
    use sha2::{Digest, Sha256};
    use std::time::Duration;

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
        assert_eq!(report.ssd_journal_cached_read_verified_objects, 4);
        assert!(report.ram_write_mib_per_second.is_finite());
        assert!(report.ram_read_mib_per_second.is_finite());
        assert!(report.ssd_write_mib_per_second.is_finite());
        assert!(report.ssd_journal_cached_read_mib_per_second.is_finite());
        assert_eq!(
            report.payload_seed_base,
            format!("{PAYLOAD_BASE_SEED:016x}")
        );
        assert!(std::fs::read_dir(parent.path()).unwrap().next().is_none());
    }

    /// Guards the fix for weak verification from identical payloads: every
    /// object must now get distinct content (and therefore a distinct
    /// digest) derived from its own index, not one payload shared by all
    /// objects. Without this, a reader returning a different object's bytes
    /// of the same length would go undetected.
    #[test]
    fn per_object_payloads_and_digests_are_distinct_across_indices() {
        let payload_bytes = 4096;
        let first = generate_payload(seed_for(PAYLOAD_BASE_SEED, 0), payload_bytes);
        let second = generate_payload(seed_for(PAYLOAD_BASE_SEED, 1), payload_bytes);
        assert_ne!(
            first, second,
            "distinct indices must yield distinct payloads"
        );
        assert_ne!(
            Sha256::digest(&first),
            Sha256::digest(&second),
            "distinct payloads must yield distinct digests"
        );

        // Regenerating from the same (base_seed, index) pair must be
        // reproducible, since read-side verification depends on it.
        let first_again = generate_payload(seed_for(PAYLOAD_BASE_SEED, 0), payload_bytes);
        assert_eq!(first, first_again);
    }

    #[test]
    fn mib_per_second_guards_zero_elapsed() {
        assert!(mib_per_second(1024 * 1024, Duration::ZERO).is_finite());
    }
}
