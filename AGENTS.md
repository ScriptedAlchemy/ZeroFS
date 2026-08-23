# ZeroFS agent notes

## Performance and benchmark map

- `scripts/vm100-pilot.py benchmark` is the maintained end-to-end VM100 tier runner. It measures foreground acknowledgement, local SSD durability, remote durability, and reads against an already-running isolated pilot. It does not deploy or tear down the pilot itself.
- `scripts/vm100-pilot.py protocol-matrix --protocol nfs --idle-read` is the separate mounted NFS long-idle recovery probe. It requires the explicit `ZEROFS_BENCH_NFS_SERVICE_ISOLATED=true` operator authority tied to the metrics `server_instance_id`, writes and remotely drains one owned 64 MiB file, idles for 61 minutes, invalidates the NFS client's page cache, applies a 30-second fio userspace deadline, and verifies exact I/O and SHA-256. `zerofs_sftp_object_read_bytes_total` is service-global interval activity, not request attribution: its delta must cover at least the logical read, and any other protocol/client/export or internal SFTP-reading maintenance in that service instance invalidates the receipt. A returned userspace timeout writes a failed receipt and runs cleanup; an uninterruptible hard-NFS D-state can prevent the timeout from returning, so it cannot guarantee receipt finalization or teardown. The scenario neither deploys nor restarts the server and does not prove a pool session rotated.
- `scripts/vm100-pilot.py setup|teardown|profile` owns the legacy isolated-pilot lifecycle. `profile` temporarily installs a symbolized binary and is intentionally much slower than `benchmark`; do not substitute it for an ordinary throughput run.
- `scripts/vm100_pilot/` contains the benchmark implementation, receipts, metrics sampling, matrices, raw-SFTP comparison, and cleanup logic. Historical receipts normally live under `/var/tmp/zerofs-pilot-results` on `ubuntu-main`.
- `scripts/tiered-writeback-e2e.py` and `scripts/tiered_writeback_e2e/` describe UUID-scoped RAM/SSD/remote scenarios. Real scenarios currently fail closed until the typed durability collector is wired; `--plan-only` is not performance evidence.
- `zerofs/src/writeback/sftp_bench.rs` is the direct real-SFTP/writeback microbenchmark. It constructs the production SFTP transport, pool, object store, journal, and remote scheduler without launching the CLI or protocol servers. It uses the normal dev/CI test profile, not `--release`.
- `zerofs/src/writeback/tier_bench.rs` is the direct host-local read/write microbenchmark. It measures RAM acknowledgement through the production writeback store, RAM reads through the production overlay, SSD durability through the production journaler, and page-cache-served SSD-journal reads verified per object against the payload each object actually got. It also uses the normal dev/CI test profile.
- `zerofs/src/writeback/store.rs`, `journal.rs`, and `journaler.rs` contain older ignored in-process tier microbenchmarks. Their comments currently prescribe `--release`; do not use them when the task asks for the normal dev benchmark.
- `zerofs/src/fs/store/extent/perf_harness.rs` contains in-process cumulative pipeline benchmarking. It is not a deployed remote-backend acceptance test.
- `docs/vm100-benchmark-methodology.md` is the benchmark evidence and safety contract. The detailed historical rollout targets are under `docs/superpowers/specs/` and `docs/superpowers/plans/`.

## Direct SFTP/writeback benchmark

Run the real benchmark on Linux, normally `ubuntu-main`; do not run it on macOS. It is ignored by default and uses the configured SFTP transport and writeback concurrency. It creates one UUID-scoped child under the configured SFTP prefix, verifies every payload, deletes every object, removes the owned remote directories, shuts down its pool, and removes its local temporary journal.

```bash
cd /fast/projects/ZeroFS/zerofs
set -a
source /secure/zerofs-prod.env
set +a
ZEROFS_SFTP_WRITEBACK_BENCH_CONFIG=/secure/zerofs-prod.toml \
ZEROFS_BENCH_DIR=/var/tmp \
ZEROFS_BENCH_SFTP_IDENTITY_FILE=/secure/storage-key \
ZEROFS_BENCH_SFTP_KNOWN_HOSTS=/secure/known_hosts \
cargo test --locked -p zerofs --lib \
  writeback::sftp_bench::bench_sftp_writeback_remote_drain \
  -- --exact --ignored --nocapture
```

Optional normal-profile sizing knobs are `ZEROFS_BENCH_SFTP_TOTAL_MIB` (default 256), `ZEROFS_BENCH_SFTP_PAYLOAD_KIB` (default 1024), `ZEROFS_BENCH_SFTP_MANIFEST_KIB` (default 64), `ZEROFS_BENCH_SFTP_WRITERS` (default 16; also the remote reader count), and `ZEROFS_BENCH_SFTP_MAX_CONNECTIONS` (defaults to the supplied config). `ZEROFS_BENCH_SFTP_FENCE_EVERY=N` turns every Nth object into a distinct create-only manifest fence while the remaining objects use the generated-segment contract; mixed workloads are admitted in exact path order so the journal geometry is deterministic. The output line begins with `SFTP_WRITEBACK_BENCH` and contains JSON for RAM acknowledgement, SSD durability, remote write drain, timed verified remote reads, exact object classes, and SFTP write-handle counts. `writers` is the configured knob (and remote reader fanout); `effective_writers` is the concurrency the acknowledgement phase actually ran at — 1 for ordered mixed workloads (`ZEROFS_BENCH_SFTP_FENCE_EVERY>0`), `writers` otherwise — so compare rates across runs by `effective_writers`, not `writers`. Capture the exact Git SHA, command, output, and cleanup result with any reported rate. When production remains connected, keep its pool plus the benchmark pool within the Storage Box account limit; an eight-connection production pool leaves at most two slots for this benchmark.

To reproduce the full-journal fallback path without changing production, run the separate saturation case with an intentionally small synthetic SSD budget. It proves the tail is blocked before remote activation, reports each remote-cleanup-paced foreground interval, verifies every remote object, and uses the same mandatory teardown gate:

```bash
ZEROFS_SFTP_WRITEBACK_BENCH_CONFIG=/secure/zerofs-prod.toml \
ZEROFS_BENCH_DIR=/var/tmp \
ZEROFS_BENCH_SFTP_IDENTITY_FILE=/secure/storage-key \
ZEROFS_BENCH_SFTP_KNOWN_HOSTS=/secure/known_hosts \
ZEROFS_BENCH_SFTP_TOTAL_MIB=128 \
ZEROFS_BENCH_SFTP_PAYLOAD_KIB=8192 \
ZEROFS_BENCH_SFTP_MANIFEST_KIB=64 \
ZEROFS_BENCH_SFTP_FENCE_EVERY=4 \
ZEROFS_BENCH_SFTP_WRITERS=8 \
ZEROFS_BENCH_SFTP_MAX_CONNECTIONS=2 \
ZEROFS_BENCH_SFTP_SSD_MIB=32 \
cargo test --locked -p zerofs --lib \
  writeback::sftp_bench::bench_sftp_writeback_full_ssd_pacing \
  -- --exact --ignored --nocapture
```

Its output begins with `SFTP_WRITEBACK_SATURATION_BENCH`. Treat `blocked_ack_mib_per_second`, `remote_cleanup_mib_per_second`, their ratio, and `max_blocked_ack_gap_seconds` as the full-SSD pacing evidence. Remote cleanup bytes are sampled at the final memory acknowledgement, before waiting for local durability, and the maximum gap excludes first-ack latency. `tail_objects_after_activation` names the complete paced tail rather than claiming every tail object was simultaneously queued before activation. In this saturation output, `effective_writers` reports the bounded blocked-writer count at the SSD boundary — the tail's effective concurrency, `min(paced_objects, writers)` — rather than the normal profile's ordered-vs-parallel distinction. By default every fourth object is a small create-only manifest fence and the other three are generated immutable segments, matching the shipping flush ordering shape while retaining deterministic admission order. Set `ZEROFS_BENCH_SFTP_FENCE_EVERY=0` only when intentionally measuring an immutable-only control.

ZeroFS uses the in-process native Rust `russh` transport. Production configs and benchmark commands must not depend on an external OpenSSH or HPN executable.

## Direct RAM/SSD read/write benchmark

Run the host-local benchmark in the normal dev profile. It does not need an SFTP configuration, launch ZeroFS, or touch a remote backend. Use a scratch parent on the SSD being measured; the benchmark creates a unique temporary child, shuts down the writeback workers, removes that child, and verifies it is absent before emitting JSON.

```bash
cd /fast/projects/ZeroFS/zerofs
ZEROFS_BENCH_DIR=/var/tmp \
cargo test --locked -p zerofs --lib \
  writeback::tier_bench::bench_writeback_local_tier_read_write \
  -- --exact --ignored --nocapture
```

Optional sizing knobs are `ZEROFS_BENCH_TIER_TOTAL_MIB` (default 256), `ZEROFS_BENCH_TIER_PAYLOAD_KIB` (default 1024), `ZEROFS_BENCH_TIER_WRITERS` (default 16), `ZEROFS_BENCH_TIER_READERS` (defaults to the writer count), and `ZEROFS_BENCH_LOCAL_CONCURRENCY` (default 8). The output line begins with `LOCAL_TIER_RW_BENCH` and contains exact JSON. RAM reads are served from pending in-memory overlay payloads: not a filesystem/page-cache or deployed-protocol claim. The `ssd_journal_cached_read_*` fields replay durable journal blobs through `Journal::read_blob`, a buffered read of a file this same process wrote seconds earlier — that is a hot-cache journal replay rate, not a device-level SSD read rate; the field name says so explicitly, and no other tier's rate should be read as a page-cache or deployed-protocol claim either. Every object gets its own payload, derived deterministically from a run seed and the object's index (`payload_seed_base` in the JSON), and each read-back is verified against a SHA-256 digest of its own object's expected payload rather than one payload shared by every object — this also catches a sequence-to-path mismatch, which an identical-payload comparison cannot.

Do not point legacy pilot lifecycle commands at CT198 or a shared production mount. Do not call a `tiered-writeback-e2e.py --plan-only` receipt a benchmark result. A normal dev microbenchmark is diagnostic evidence; production acceptance still requires the real mounted path, durability cutoffs, integrity, and cleanup.
