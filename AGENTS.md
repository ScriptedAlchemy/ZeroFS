# ZeroFS agent notes

## Performance and benchmark map

- `scripts/vm100-pilot.py benchmark` is the maintained end-to-end VM100 tier runner. It measures foreground acknowledgement, local SSD durability, remote durability, and reads against an already-running isolated pilot. It does not deploy or tear down the pilot itself.
- `scripts/vm100-pilot.py setup|teardown|profile` owns the legacy isolated-pilot lifecycle. `profile` temporarily installs a symbolized binary and is intentionally much slower than `benchmark`; do not substitute it for an ordinary throughput run.
- `scripts/vm100_pilot/` contains the benchmark implementation, receipts, metrics sampling, matrices, raw-SFTP comparison, and cleanup logic. Historical receipts normally live under `/var/tmp/zerofs-pilot-results` on `ubuntu-main`.
- `scripts/tiered-writeback-e2e.py` and `scripts/tiered_writeback_e2e/` describe UUID-scoped RAM/SSD/remote scenarios. Real scenarios currently fail closed until the typed durability collector is wired; `--plan-only` is not performance evidence.
- `zerofs/src/writeback/sftp_bench.rs` is the direct real-SFTP/writeback microbenchmark. It constructs the production SFTP transport, pool, object store, journal, and remote scheduler without launching the CLI or protocol servers. It uses the normal dev/CI test profile, not `--release`.
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
cargo test --locked -p zerofs --lib \
  bench_sftp_writeback_remote_drain -- --ignored --nocapture
```

Optional normal-profile sizing knobs are `ZEROFS_BENCH_SFTP_TOTAL_MIB` (default 256), `ZEROFS_BENCH_SFTP_PAYLOAD_KIB` (default 1024), and `ZEROFS_BENCH_SFTP_WRITERS` (default 16). The output line begins with `SFTP_WRITEBACK_BENCH` and contains JSON. Capture the exact Git SHA, command, output, and cleanup result with any reported rate.

Do not point legacy pilot lifecycle commands at CT198 or a shared production mount. Do not call a `tiered-writeback-e2e.py --plan-only` receipt a benchmark result. A normal dev microbenchmark is diagnostic evidence; production acceptance still requires the real mounted path, durability cutoffs, integrity, and cleanup.
