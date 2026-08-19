# Python VM100 Pilot Harness Design

## Objective

Replace the complete Bash VM100 pilot harness with one dependency-free Python CLI that safely builds, deploys, starts, stops, profiles, benchmarks, and cleans the SFTP-backed ZeroFS NBD pilot on VM100. The Bash script and Bash tests are deleted; there is no wrapper, tombstone, compatibility dispatcher, or second orchestration implementation.

## Scope

The Python CLI must provide these commands through `python3 scripts/vm100-pilot.py <command>`:

- `setup`
- `teardown`
- `restart`
- `status`
- `drain`
- `benchmark`
- `profile`
- `workloads`
- `raw-sftp`
- `iterate`
- `all`

Existing pilot configuration, systemd units, mountpoint, integrity sentinel, NBD export, clean cache, writeback journal, remote prefix, and result directory remain authoritative. The harness must never format or recreate the canonical XFS filesystem or NBD export and must never alter VM100 `/fast` except to build the ZeroFS repository already located there.

## Architecture

`scripts/vm100-pilot.py` is the executable entrypoint. Focused modules under `scripts/vm100_pilot/` separate configuration, subprocess execution, lifecycle, metrics, benchmark workloads, profiling, and receipts. Modules use only the Python 3 standard library. External Linux tools remain explicit runtime dependencies because they are the subjects or instruments of the benchmark: `cargo`, `systemctl`, `fio`, OpenSSH `sftp`, `perf`, `pidstat`, `iostat`, `sar`, `git`, `curl`, `findmnt`, and `sha256sum`.

The process owns one nonblocking `fcntl.flock` for the entire command. No command recursively invokes the CLI. Compound commands call shared Python functions directly, so one process owns lifecycle and cleanup.

## Components

### Configuration

`PilotConfig.from_environment()` resolves repository, binary, receipt, systemd units, mountpoint, metrics URL, benchmark settings, workload pins, and profile target. TOML values are read with Python `tomllib`; secrets are never printed. Numeric and path invariants are validated before mutation.

### Command execution

`Runner` executes argv arrays without a shell, captures receipts, supports timeouts, starts process groups, and terminates complete process trees. Tests use a `FakeRunner`; production code does not branch on test-only environment variables.

### Lifecycle

`PilotLifecycle` validates VM100, service state, cgroup emptiness, binary identity, build receipt, configuration durability mode, exact XFS mount topology, integrity SHA-256, and metadata count. Teardown is strictly ordered and awaited:

1. mount unit;
2. NBD client;
3. ZeroFS daemon.

Startup is transactional. If any layer fails, only layers started by that invocation are unwound in reverse order. Canonical data, caches, journals, and the NBD export are retained.

### Metrics and receipts

Prometheus text is parsed into typed snapshots. Drain fails immediately on terminal error and succeeds only after four stable samples where accepted, local, and remote sequences match and dirty RAM/SSD are zero.

Every run creates a persistent directory under `/var/tmp/zerofs-pilot-results`. `manifest.json` records command, UTC timestamps, checkout/deployed commit, binary/config hashes, effective durability settings, exit state, cleanup state, and artifact paths. Human-readable summaries are also written for terminal use. Partial results survive failure.

### Three-tier benchmark

The benchmark directly measures:

1. foreground user-visible write latency and throughput;
2. local SSD durability using the local completed-payload counter and sync barrier;
3. remote Storage Box durability using the remote completed-payload counter and stable drain barrier;
4. buffered warm-read candidate throughput;
5. direct read throughput that bypasses the guest page cache while remaining eligible for ZeroFS clean cache.

All files are incompressible, scoped to a unique run directory, and removed in `finally`. Cleanup includes sync, remote drain when the daemon remains healthy, and verified absence.

### Profiling

`profile` builds the exact checkout into an isolated target with release optimization plus symbols. It records the validated canonical binary and build receipt, stops the stack, atomically installs the profiling binary, starts the stack, and runs the same Python benchmark engine while collecting:

- `perf record` sampled call graph;
- `perf stat` CPU/cycle/instruction/cache/context-switch/fault counters;
- per-process `pidstat` CPU, memory, and I/O;
- `iostat` device throughput, queueing, utilization, and latency;
- `sar` interface throughput;
- ZeroFS writeback metric samples;
- process `/proc/<pid>/io` and status snapshots;
- socket summary and established Storage Box session counts.

A `finally` block always stops collectors, generates the text call-graph report, tears down the profiling stack, restores the original binary and receipt atomically, restarts the canonical stack, and validates its hashes, integrity sentinel, restart count, and drained watermarks. The isolated profile target is removed only after restoration succeeds.

### Raw SFTP control

The control requires explicit stock and HPN SSH executable paths, rejects a
stock executable that reports HPN provenance, records each
absolute path, version, and SHA-256, and uses the selected executable through
OpenSSH SFTP's `-S` option. It stops the isolated pilot ZeroFS service to avoid
account-session contention, creates four 128 MiB incompressible files, and runs
counterbalanced repeated stock/HPN upload and download trials with identical
jobs, bytes, buffer size, and request depth. Downloads are materialized and
SHA-256 checked against the shared sources. Receipts distinguish each SFTP
process's close acknowledgement from remote durability, which this control does
not measure. UUID-owned remote and local artifacts are cleaned twice, asserted
absent, and the canonical stack is restored in `finally`. Worker failures do
not strand siblings. Metadata commands and parallel transfer phases have fixed
upper deadlines; expiry terminates all owned process groups before cleanup and
stack restoration.

### Real workloads

The harness creates a unique user-owned directory on the XFS pilot mount through privileged directory creation followed by explicit ownership. It clones pinned npm CLI and ripgrep commits, records tool versions, runs npm cold/warm install, serial and parallel `node_modules` deletion, Cargo cold/no-op/incremental builds, local barriers, remote tails, and verified cleanup. Registry dependencies are prefetched outside timed filesystem phases where supported.

## Error and recovery rules

- No destructive path accepts an empty, relative, root, `/fast`, or mount-root target.
- Cleanup and canonical restoration run after success, failure, timeout, signal, or child-process error.
- The original failure remains primary; cleanup/restoration failures are appended to the receipt and make the command fail.
- A profiling binary is never considered canonical and never overwrites the canonical build receipt permanently.
- Service startup, mount topology, runtime binary identity, terminal writeback state, and journal drain are evidence gates rather than informational output.
- No secret URL, password, key contents, or environment-file value is written to receipts.

## Tests

Python `unittest` tests cover configuration validation, metrics parsing, drain stability, stop ordering, transactional startup unwind, process-group cleanup, benchmark arithmetic, unique-directory ownership, raw-worker reaping, partial receipt persistence, profile build flags, collector supervision, forced benchmark failure, forced restoration failure, canonical binary/receipt restoration, and prohibited destructive paths.

The Linux acceptance gate on VM100 is:

1. all Python tests;
2. `ruff` if installed, otherwise `python3 -m compileall` plus standard-library checks;
3. `profile` with a reduced 256 MiB workload;
4. canonical restoration and status validation;
5. full 1 GiB benchmark;
6. pinned npm/Cargo workloads;
7. four-session counterbalanced stock/HPN raw SFTP control;
8. no leftover benchmark directories, collectors, profiling targets, raw files, mounts, or stopped canonical services.

## Removal and documentation

Delete `scripts/vm100-pilot.sh` and `scripts/tests/vm100-pilot-test.sh` in the same commit that introduces the Python replacement and green Python tests. Update every repository reference and example to the Python command. Do not retain shell aliases or compatibility notes suggesting that the deleted Bash implementation remains supported.

## Acceptance criteria

- Every former harness command is implemented natively in Python.
- No Bash harness or Bash harness test remains in the repository.
- Profiling is one command and profiles the same benchmark engine used normally.
- Failure injection proves the canonical binary, receipt, services, mount, and data are restored.
- VM100 `/fast` remains the original ZFS dataset and contains no ZeroFS cache, journal, page file, NBD filesystem, or benchmark data other than the source/build checkout.
- Benchmark receipts distinguish foreground, local SSD, remote SFTP, buffered read, direct read, and raw SFTP control.
- Cleanup leaves only the canonical active NBD v2 pilot and intentional durable result bundles.
