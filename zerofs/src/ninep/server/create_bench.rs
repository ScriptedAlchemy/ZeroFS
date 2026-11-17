//! Small-file creation through the real 9P server and the real ZeroFS client
//! over one Unix-socket connection, the shape of a NAS share served through a
//! single 9P mount. This is an in-process microbenchmark in the normal test
//! profile against an in-memory object store. It is not NAS performance.
//!
//! ```text
//! ZEROFS_BENCH_DIR=/var/tmp cargo test --locked -p zerofs --lib \
//!   ninep::server::create_bench::bench_p9_small_file_create \
//!   -- --exact --ignored --nocapture
//! ```
//!
//! Each concurrency level gets a fresh filesystem, server and client. Every
//! task creates files in its own directory: `Tlcreateattr`, one `Twrite`, and
//! `Tclunk`. Knobs: `ZEROFS_BENCH_P9_TASKS` (default `1,4,8,16`),
//! `ZEROFS_BENCH_P9_SECONDS` (default 5), `ZEROFS_BENCH_P9_WARMUP_MS`
//! (default 1000), `ZEROFS_BENCH_P9_PAYLOAD` (default 4096),
//! `ZEROFS_BENCH_P9_SHARED_DIR=1` (all tasks create in one directory) and
//! `ZEROFS_BENCH_P9_SERVER_THREADS` (default 16). Each level prints one
//! `P9_CREATE_BENCH` JSON line.

use super::{NinePServer, P9GlobalAdmission};
use crate::fs::ZeroFS;
use ninep_client::NinePClient;
use ninep_proto::{NOFID, P9_MAX_MSIZE};
use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::time::{Duration, Instant};
use tokio_util::sync::CancellationToken;

const S_IFREG: u32 = 0o100_000;
const S_IFDIR: u32 = 0o040_000;

fn env_or<T: std::str::FromStr>(name: &str, default: T) -> T {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

/// CPU seconds per thread name, from `/proc/self/task/*/stat`.
fn thread_cpu_seconds() -> BTreeMap<String, Vec<f64>> {
    // SAFETY: sysconf has no preconditions.
    let ticks = unsafe { libc::sysconf(libc::_SC_CLK_TCK) } as f64;
    let mut by_name: BTreeMap<String, Vec<f64>> = BTreeMap::new();
    let Ok(tasks) = std::fs::read_dir("/proc/self/task") else {
        return by_name;
    };
    for task in tasks.flatten() {
        let Ok(stat) = std::fs::read_to_string(task.path().join("stat")) else {
            continue;
        };
        let (Some(open), Some(close)) = (stat.find('('), stat.rfind(')')) else {
            continue;
        };
        let name = stat[open + 1..close].to_string();
        let fields: Vec<&str> = stat[close + 2..].split_whitespace().collect();
        // utime and stime are fields 14 and 15; `fields` starts at field 3.
        let (Some(utime), Some(stime)) = (fields.get(11), fields.get(12)) else {
            continue;
        };
        let seconds =
            (utime.parse::<f64>().unwrap_or(0.0) + stime.parse::<f64>().unwrap_or(0.0)) / ticks;
        by_name.entry(name).or_default().push(seconds);
    }
    by_name
}

fn cpu_delta(
    before: &BTreeMap<String, Vec<f64>>,
    after: &BTreeMap<String, Vec<f64>>,
) -> serde_json::Value {
    // Threads keep their order under one name between two reads of the same
    // process, and runtime threads live for the whole level.
    let mut out = serde_json::Map::new();
    for (name, now) in after {
        let then = before.get(name).cloned().unwrap_or_default();
        let deltas: Vec<f64> = now
            .iter()
            .enumerate()
            .map(|(index, seconds)| seconds - then.get(index).copied().unwrap_or(0.0))
            .collect();
        let total: f64 = deltas.iter().sum();
        let busiest = deltas.iter().copied().fold(0.0, f64::max);
        if total > 0.0 {
            out.insert(
                name.clone(),
                serde_json::json!({
                    "threads": deltas.len(),
                    "cpu_seconds": total,
                    "busiest_thread_cpu_seconds": busiest,
                }),
            );
        }
    }
    serde_json::Value::Object(out)
}

fn percentile(sorted: &[f64], fraction: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    let index = ((sorted.len() - 1) as f64 * fraction).round() as usize;
    sorted[index]
}

fn summary(mut samples: Vec<f64>) -> serde_json::Value {
    samples.sort_by(f64::total_cmp);
    let mean = samples.iter().sum::<f64>() / samples.len().max(1) as f64;
    serde_json::json!({
        "mean_us": mean * 1e6,
        "p50_us": percentile(&samples, 0.5) * 1e6,
        "p99_us": percentile(&samples, 0.99) * 1e6,
    })
}

#[derive(Default)]
struct TaskResult {
    created: u64,
    create_seconds: Vec<f64>,
    write_seconds: Vec<f64>,
    clunk_seconds: Vec<f64>,
    file_seconds: Vec<f64>,
}

#[allow(clippy::too_many_arguments)]
async fn create_files(
    client: Arc<NinePClient>,
    dir_fid: u32,
    prefix: String,
    payload: Arc<Vec<u8>>,
    stop: Arc<AtomicBool>,
    measuring: Arc<AtomicBool>,
    completed: Arc<AtomicU64>,
    errors: Arc<AtomicUsize>,
) -> TaskResult {
    let mut result = TaskResult::default();
    while !stop.load(Ordering::Relaxed) {
        let name = format!("{prefix}-{:08}", result.created);
        let fid = client.alloc_fid();
        let started = Instant::now();
        if let Err(error) = client
            .lcreateattr(
                dir_fid,
                fid,
                name.as_bytes(),
                (libc::O_RDWR | libc::O_CREAT) as u32,
                S_IFREG | 0o644,
                0,
            )
            .await
        {
            eprintln!("create {name}: {error:?}");
            errors.fetch_add(1, Ordering::Relaxed);
            return result;
        }
        let created = Instant::now();
        match client.write(fid, 0, &payload).await {
            Ok(written) if written == payload.len() as u64 => {}
            other => {
                eprintln!("write {name}: {other:?}");
                errors.fetch_add(1, Ordering::Relaxed);
                return result;
            }
        }
        let written = Instant::now();
        if let Err(error) = client.clunk(fid).await {
            eprintln!("clunk {name}: {error:?}");
            errors.fetch_add(1, Ordering::Relaxed);
            return result;
        }
        client.free_fid(fid);
        let finished = Instant::now();
        result.created += 1;
        if measuring.load(Ordering::Relaxed) {
            completed.fetch_add(1, Ordering::Relaxed);
            result
                .create_seconds
                .push((created - started).as_secs_f64());
            result.write_seconds.push((written - created).as_secs_f64());
            result
                .clunk_seconds
                .push((finished - written).as_secs_f64());
            result.file_seconds.push((finished - started).as_secs_f64());
        }
    }
    result
}

/// Admission counters sampled every 200 microseconds during the window.
#[derive(Default)]
struct AdmissionSamples {
    samples: u64,
    active_sum: u64,
    active_peak: usize,
    reserved_peak: usize,
}

fn run_level(tasks: usize, socket_dir: &std::path::Path) -> serde_json::Value {
    let seconds: u64 = env_or("ZEROFS_BENCH_P9_SECONDS", 5);
    let warmup_ms: u64 = env_or("ZEROFS_BENCH_P9_WARMUP_MS", 1000);
    let payload_len: usize = env_or("ZEROFS_BENCH_P9_PAYLOAD", 4096);
    let shared_dir = env_or("ZEROFS_BENCH_P9_SHARED_DIR", 0u8) == 1;
    let server_threads: usize = env_or("ZEROFS_BENCH_P9_SERVER_THREADS", 16);

    let server_rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(server_threads)
        .thread_name("p9b-server")
        .enable_all()
        .build()
        .unwrap();
    let client_rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .thread_name("p9b-client")
        .enable_all()
        .build()
        .unwrap();

    let socket = socket_dir.join(format!("bench-{tasks}.sock"));
    let filesystem = server_rt.block_on(async { Arc::new(ZeroFS::new_in_memory().await.unwrap()) });
    let shutdown = CancellationToken::new();
    let server = NinePServer::new_unix(Arc::clone(&filesystem), socket.clone());
    let server_task = server_rt.spawn({
        let shutdown = shutdown.clone();
        async move { server.start(shutdown).await }
    });

    let result = client_rt.block_on(async {
        let client = {
            let mut attempts = 0;
            loop {
                match NinePClient::connect_unix(&socket, P9_MAX_MSIZE).await {
                    Ok(client) => break client,
                    Err(error) if attempts < 100 => {
                        attempts += 1;
                        let _ = error;
                        tokio::time::sleep(Duration::from_millis(20)).await;
                    }
                    Err(error) => panic!("connect: {error}"),
                }
            }
        };
        // The client allocates fids from 1, so take the root fid from it too.
        let root_fid = client.alloc_fid();
        client.attach(root_fid, NOFID, "root", "", 0).await.unwrap();
        let dirs = if shared_dir { 1 } else { tasks };
        let mut dir_fids = Vec::new();
        for dir in 0..dirs {
            let name = format!("dir-{dir}");
            client
                .mkdir(root_fid, name.as_bytes(), S_IFDIR | 0o755, 0)
                .await
                .unwrap();
            let fid = client.alloc_fid();
            client
                .walk(root_fid, fid, &[name.as_bytes()])
                .await
                .unwrap();
            dir_fids.push(fid);
        }

        let payload = Arc::new(
            (0..payload_len)
                .map(|i| (i % 251) as u8)
                .collect::<Vec<_>>(),
        );
        let stop = Arc::new(AtomicBool::new(false));
        let measuring = Arc::new(AtomicBool::new(false));
        let completed = Arc::new(AtomicU64::new(0));
        let errors = Arc::new(AtomicUsize::new(0));
        let workers = (0..tasks)
            .map(|task| {
                tokio::spawn(create_files(
                    Arc::clone(&client),
                    dir_fids[if shared_dir { 0 } else { task }],
                    format!("t{task}"),
                    Arc::clone(&payload),
                    Arc::clone(&stop),
                    Arc::clone(&measuring),
                    Arc::clone(&completed),
                    Arc::clone(&errors),
                ))
            })
            .collect::<Vec<_>>();

        tokio::time::sleep(Duration::from_millis(warmup_ms)).await;
        let sampling = Arc::new(AtomicBool::new(true));
        let sampler = std::thread::spawn({
            let sampling = Arc::clone(&sampling);
            move || {
                let mut samples = AdmissionSamples::default();
                while sampling.load(Ordering::Relaxed) {
                    let snapshot = P9GlobalAdmission::shared().snapshot();
                    samples.samples += 1;
                    samples.active_sum += snapshot.active_requests as u64;
                    samples.active_peak = samples.active_peak.max(snapshot.active_requests);
                    samples.reserved_peak = samples.reserved_peak.max(snapshot.reserved_bytes);
                    std::thread::sleep(Duration::from_micros(200));
                }
                samples
            }
        });
        let cpu_before = thread_cpu_seconds();
        let apply_before = filesystem.write_coordinator.apply_nanos();
        let batches_before = filesystem.write_coordinator.batch_sizes().len();
        let window_start = Instant::now();
        measuring.store(true, Ordering::Relaxed);
        tokio::time::sleep(Duration::from_secs(seconds)).await;
        measuring.store(false, Ordering::Relaxed);
        let window = window_start.elapsed().as_secs_f64();
        let measured = completed.load(Ordering::Relaxed);
        let cpu_after = thread_cpu_seconds();
        let apply_seconds =
            (filesystem.write_coordinator.apply_nanos() - apply_before) as f64 / 1e9;
        let window_batches = filesystem.write_coordinator.batch_sizes()[batches_before..].to_vec();
        sampling.store(false, Ordering::Relaxed);
        let samples = sampler.join().unwrap();
        stop.store(true, Ordering::Relaxed);

        let mut results = Vec::new();
        for worker in workers {
            results.push(worker.await.unwrap());
        }
        let errors = errors.load(Ordering::Relaxed);

        // Prove the work happened: the directories list exactly the names the
        // tasks report creating, each once, and one file reads back its payload.
        let mut listed: BTreeMap<String, usize> = BTreeMap::new();
        let mut pages = 0usize;
        for &fid in &dir_fids {
            let open = client.alloc_fid();
            client.walk(fid, open, &[]).await.unwrap();
            client.lopen(open, libc::O_RDONLY as u32).await.unwrap();
            let mut offset = 0;
            loop {
                let entries = client.readdir(open, offset, 64 * 1024).await.unwrap();
                let Some(last) = entries.last() else { break };
                offset = last.offset;
                pages += 1;
                for entry in &entries {
                    let name = String::from_utf8_lossy(entry.name.as_ref()).into_owned();
                    if name != "." && name != ".." {
                        *listed.entry(name).or_default() += 1;
                    }
                }
            }
            client.clunk(open).await.unwrap();
        }
        let created: u64 = results.iter().map(|result| result.created).sum();
        let expected: std::collections::BTreeSet<String> = results
            .iter()
            .enumerate()
            .flat_map(|(task, result)| {
                (0..result.created).map(move |index| format!("t{task}-{index:08}"))
            })
            .collect();
        let duplicated: Vec<&String> = listed
            .iter()
            .filter(|(_, count)| **count > 1)
            .map(|(name, _)| name)
            .collect();
        let unexpected: Vec<&String> = listed
            .keys()
            .filter(|name| !expected.contains(*name))
            .collect();
        let missing: Vec<&String> = expected
            .iter()
            .filter(|name| !listed.contains_key(*name))
            .collect();
        let listing_matches = duplicated.is_empty() && unexpected.is_empty() && missing.is_empty();
        let check = client.alloc_fid();
        let check_name = format!("t0-{:08}", 0);
        client
            .walk(dir_fids[0], check, &[check_name.as_bytes()])
            .await
            .unwrap();
        client.lopen(check, libc::O_RDONLY as u32).await.unwrap();
        let read_back = client.read(check, 0, payload_len as u32 + 1).await.unwrap();
        client.clunk(check).await.unwrap();

        let collect = |pick: fn(&TaskResult) -> &Vec<f64>| {
            results
                .iter()
                .flat_map(|result| pick(result).iter().copied())
                .collect::<Vec<_>>()
        };
        let throughput = measured as f64 / window;
        let file_latency: Vec<f64> = collect(|result| &result.file_seconds);
        let mean_latency = file_latency.iter().sum::<f64>() / file_latency.len().max(1) as f64;
        serde_json::json!({
            "tasks": tasks,
            "shared_dir": shared_dir,
            "server_threads": server_threads,
            "payload_bytes": payload_len,
            "window_seconds": window,
            "files_in_window": measured,
            "files_per_second": throughput,
            "errors": errors,
            "files_created_total": created,
            "files_listed_total": listed.values().sum::<usize>(),
            "readdir_pages": pages,
            "listing_matches": listing_matches,
            "listed_duplicates": duplicated.iter().take(8).collect::<Vec<_>>(),
            "listed_unexpected": unexpected.iter().take(8).collect::<Vec<_>>(),
            "listed_missing": missing.iter().take(8).collect::<Vec<_>>(),
            "read_back_matches": read_back == *payload,
            "file_latency": summary(file_latency),
            "create_latency": summary(collect(|result| &result.create_seconds)),
            "write_latency": summary(collect(|result| &result.write_seconds)),
            "clunk_latency": summary(collect(|result| &result.clunk_seconds)),
            "littles_law_inflight_files": throughput * mean_latency,
            "admission": {
                "active_requests_mean": samples.active_sum as f64 / samples.samples.max(1) as f64,
                "active_requests_peak": samples.active_peak,
                "reserved_bytes_peak": samples.reserved_peak,
                "samples": samples.samples,
            },
            "thread_cpu": cpu_delta(&cpu_before, &cpu_after),
            // The single commit worker: the share of the window it spent
            // applying batches, and how many commits each batch carried.
            "commit_worker": {
                "busy_fraction": apply_seconds / window,
                "batches": window_batches.len(),
                "commits": window_batches.iter().sum::<usize>(),
                "mean_batch": window_batches.iter().sum::<usize>() as f64
                    / window_batches.len().max(1) as f64,
                "max_batch": window_batches.iter().copied().max().unwrap_or(0),
            },
        })
    });

    shutdown.cancel();
    server_rt.block_on(async {
        let _ = tokio::time::timeout(Duration::from_secs(10), server_task).await;
    });
    drop(filesystem);
    server_rt.shutdown_timeout(Duration::from_secs(10));
    client_rt.shutdown_timeout(Duration::from_secs(10));
    let _ = std::fs::remove_file(&socket);
    result
}

#[test]
#[ignore = "benchmark; run explicitly with --ignored"]
fn bench_p9_small_file_create() {
    let parent = std::env::var("ZEROFS_BENCH_DIR").unwrap_or_else(|_| "/var/tmp".to_string());
    let scratch = tempfile::Builder::new()
        .prefix("zerofs-p9-create-bench-")
        .tempdir_in(&parent)
        .unwrap();
    let levels: Vec<usize> = std::env::var("ZEROFS_BENCH_P9_TASKS")
        .unwrap_or_else(|_| "1,4,8,16".to_string())
        .split(',')
        .map(|level| level.trim().parse().unwrap())
        .collect();
    for tasks in levels {
        let result = run_level(tasks, scratch.path());
        println!("P9_CREATE_BENCH {result}");
        assert_eq!(result["errors"], 0, "{result}");
        assert_eq!(result["listing_matches"], true, "{result}");
        assert_eq!(result["read_back_matches"], true, "{result}");
    }
    let path = scratch.path().to_path_buf();
    scratch.close().unwrap();
    assert!(!path.exists(), "scratch directory {path:?} remains");
}
