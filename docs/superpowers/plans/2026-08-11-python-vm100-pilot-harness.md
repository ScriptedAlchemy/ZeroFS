# Python VM100 Pilot Harness Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the complete Bash VM100 pilot harness with a dependency-free Python CLI that safely deploys, profiles, benchmarks, restores, and cleans the canonical NBD v2 pilot.

**Architecture:** One Python process owns the global lock and directly composes focused configuration, runner, lifecycle, metrics, receipt, benchmark, profiling, raw-control, and real-workload modules. All compound commands share those modules rather than invoking the CLI recursively. Context-managed cleanup restores the validated canonical binary, receipt, services, mount, and drained journal after any profile or control failure.

**Tech Stack:** Python 3.11+ standard library (`argparse`, `dataclasses`, `fcntl`, `hashlib`, `json`, `pathlib`, `subprocess`, `tempfile`, `tomllib`, `unittest`), Linux systemd/NBD/XFS, Cargo, fio, perf, sysstat, OpenSSH SFTP.

## Global Constraints

- Delete `scripts/vm100-pilot.sh` and `scripts/tests/vm100-pilot-test.sh`; keep no Bash wrapper, tombstone, alias, or compatibility dispatcher.
- Never format or recreate the canonical NBD export/XFS filesystem.
- Never store ZeroFS cache, journal, NBD filesystem, page file, or benchmark data on VM100 `/fast`; only the source/build checkout may use `/fast`.
- Use Python standard library only; external commands are invoked as argv arrays without a shell.
- Preserve the canonical binary and build receipt across profiling and raw SFTP controls.
- Every mutation path has deterministic cleanup and a persistent partial receipt.
- Tests precede production changes and use `unittest` plus fake runners/filesystems.

---

### Task 1: Core package, configuration, runner, and receipts

**Files:**
- Create: `scripts/vm100-pilot.py`
- Create: `scripts/vm100_pilot/__init__.py`
- Create: `scripts/vm100_pilot/config.py`
- Create: `scripts/vm100_pilot/runner.py`
- Create: `scripts/vm100_pilot/receipts.py`
- Create: `scripts/tests/test_vm100_pilot.py`

**Interfaces:**
- Produces: `PilotConfig.from_environment(root: Path) -> PilotConfig`
- Produces: `Runner.run(argv, *, sudo=False, timeout=None, capture=True, check=True) -> CompletedProcess[str]`
- Produces: `Runner.spawn(argv, *, sudo=False, stdout=None, stderr=None) -> ManagedProcess`
- Produces: `RunReceipt.start(config, command) -> RunReceipt`, `record`, `artifact`, `finish`

- [ ] **Step 1: Write failing configuration and safe-path tests**

```python
def test_config_rejects_fast_for_writeback_or_results(self):
    env = {"ZEROFS_PILOT_RESULT_DIR": "/fast/results"}
    with self.assertRaisesRegex(ValueError, "/fast"):
        PilotConfig.from_mapping(ROOT, env)

def test_receipt_survives_failure(self):
    with self.assertRaises(RuntimeError):
        with RunReceipt.start(config, "benchmark") as receipt:
            receipt.record("phase", "write")
            raise RuntimeError("boom")
    self.assertEqual(json.loads(receipt.manifest.read_text())["status"], "failed")
```

- [ ] **Step 2: Run tests and capture RED**

Run: `python3 -m unittest scripts.tests.test_vm100_pilot.CoreTests -v`
Expected: import failure for `scripts.vm100_pilot`.

- [ ] **Step 3: Implement immutable configuration, safe paths, command execution, process groups, and atomic JSON receipts**

```python
@dataclass(frozen=True)
class PilotConfig:
    root: Path
    binary: Path
    build_receipt: Path
    mountpoint: Path
    result_dir: Path

    def require_disposable(self, path: Path) -> Path:
        resolved = path.resolve(strict=False)
        forbidden = {Path("/"), Path("/fast"), self.mountpoint}
        if resolved in forbidden or Path("/fast") in resolved.parents:
            raise ValueError(f"unsafe disposable path: {resolved}")
        return resolved
```

- [ ] **Step 4: Run focused tests GREEN**

Run: `python3 -m unittest scripts.tests.test_vm100_pilot.CoreTests -v`
Expected: all CoreTests pass.

- [ ] **Step 5: Commit**

Run: `git add scripts/vm100-pilot.py scripts/vm100_pilot scripts/tests/test_vm100_pilot.py && git commit -m 'feat(harness): add Python pilot core'`

### Task 2: Metrics, lifecycle, status, setup, teardown, restart, and drain

**Files:**
- Create: `scripts/vm100_pilot/metrics.py`
- Create: `scripts/vm100_pilot/lifecycle.py`
- Modify: `scripts/vm100-pilot.py`
- Modify: `scripts/tests/test_vm100_pilot.py`

**Interfaces:**
- Consumes: `PilotConfig`, `Runner`, `RunReceipt`
- Produces: `WritebackSnapshot.parse(text) -> WritebackSnapshot`
- Produces: `PilotLifecycle.status(validate_data=True) -> dict[str, object]`
- Produces: `PilotLifecycle.stop()`, `start()`, `restart()`, `build_deploy()`
- Produces: `wait_for_drain(metrics, timeout, stable_samples=4) -> DrainReceipt`

- [ ] **Step 1: Write failing tests for metric parsing, terminal fail-fast, ordered stop, startup unwind, and exact mount identity**

```python
def test_stop_is_dependency_ordered(self):
    lifecycle.stop()
    self.assertEqual(runner.stops, [MOUNT_UNIT, CLIENT_UNIT, DAEMON_UNIT])

def test_start_unwinds_only_started_layers(self):
    runner.fail_start(CLIENT_UNIT)
    with self.assertRaises(CommandError):
        lifecycle.start()
    self.assertEqual(runner.stops, [DAEMON_UNIT])
```

- [ ] **Step 2: Run LifecycleTests RED**

Run: `python3 -m unittest scripts.tests.test_vm100_pilot.LifecycleTests -v`
Expected: missing `metrics` and `lifecycle` modules.

- [ ] **Step 3: Implement lifecycle and drain state machines**

```python
for unit in (config.mount_unit, config.client_service, config.service):
    runner.run(["systemctl", "stop", "--no-block", unit], sudo=True, check=False)
    wait_unit_stopped(unit, deadline)
```

Startup appends each successful layer to `started`; exception handling stops `reversed(started)` and re-raises the original exception with cleanup errors attached.

- [ ] **Step 4: Run LifecycleTests GREEN**

Run: `python3 -m unittest scripts.tests.test_vm100_pilot.LifecycleTests -v`
Expected: all LifecycleTests pass.

- [ ] **Step 5: Commit**

Run: `git add scripts/vm100-pilot.py scripts/vm100_pilot/metrics.py scripts/vm100_pilot/lifecycle.py scripts/tests/test_vm100_pilot.py && git commit -m 'feat(harness): manage pilot lifecycle in Python'`

### Task 3: Three-tier benchmark engine

**Files:**
- Create: `scripts/vm100_pilot/benchmark.py`
- Modify: `scripts/vm100-pilot.py`
- Modify: `scripts/tests/test_vm100_pilot.py`

**Interfaces:**
- Consumes: `PilotLifecycle`, `WritebackSnapshot`, `RunReceipt`
- Produces: `BenchmarkRunner.run(total_mib: int, jobs: int) -> BenchmarkResult`
- Produces: `BenchmarkResult.to_dict() -> dict[str, object]`

- [ ] **Step 1: Write failing arithmetic, cleanup, incompressibility, and forced-fio-failure tests**

```python
def test_local_rate_uses_completed_payload_delta_and_full_interval(self):
    result = calculate_tiers(logical=1 << 30, local_delta=1 << 30,
                             foreground_ms=1000, local_barrier_ms=3000,
                             remote_delta=1 << 30, remote_ms=10000)
    self.assertEqual(result.local_mibps, 256.0)

def test_failed_direct_read_removes_files_and_preserves_artifacts(self):
    runner.fail_command("zerofs_direct_read")
    with self.assertRaises(CommandError):
        benchmark.run(4, 1)
    self.assertFalse(any(mount.glob(".zerofs-bench-*")))
    self.assertTrue(receipt.path("direct-read-fio.txt").exists())
```

- [ ] **Step 2: Run BenchmarkTests RED**

Run: `python3 -m unittest scripts.tests.test_vm100_pilot.BenchmarkTests -v`
Expected: missing `BenchmarkRunner`.

- [ ] **Step 3: Implement shared benchmark engine and scoped cleanup**

Use fio argv lists with `--refill_buffers=1`, `--scramble_buffers=1`, and `--buffer_compress_percentage=0`. Create a unique root-owned then user-owned run directory under the mount; never write at the mount root. Sample metrics at 250 ms and preserve all phase outputs.

- [ ] **Step 4: Run BenchmarkTests GREEN**

Run: `python3 -m unittest scripts.tests.test_vm100_pilot.BenchmarkTests -v`
Expected: all BenchmarkTests pass.

- [ ] **Step 5: Commit**

Run: `git add scripts/vm100-pilot.py scripts/vm100_pilot/benchmark.py scripts/tests/test_vm100_pilot.py && git commit -m 'feat(harness): benchmark writeback durability tiers'`

### Task 4: One-command symbolized profiler

**Files:**
- Create: `scripts/vm100_pilot/profile.py`
- Modify: `scripts/vm100-pilot.py`
- Modify: `scripts/tests/test_vm100_pilot.py`

**Interfaces:**
- Consumes: `PilotLifecycle`, `BenchmarkRunner`, `Runner`, `RunReceipt`
- Produces: `ProfileRunner.run(total_mib: int, jobs: int) -> ProfileResult`
- Produces: `CanonicalDeployment.capture(config)`, `install_profile`, `restore_and_validate`

- [ ] **Step 1: Write failing tests for build flags, collector command lines, timeout, failed benchmark restoration, and failed collector restoration**

```python
def test_profile_failure_restores_binary_receipt_and_stack(self):
    canonical = binary.read_bytes(), receipt.read_bytes()
    runner.fail_command("fio")
    with self.assertRaises(CommandError):
        profiler.run(4, 1)
    self.assertEqual((binary.read_bytes(), receipt.read_bytes()), canonical)
    self.assertTrue(lifecycle.status()["healthy"])
```

- [ ] **Step 2: Run ProfileTests RED**

Run: `python3 -m unittest scripts.tests.test_vm100_pilot.ProfileTests -v`
Expected: missing `ProfileRunner`.

- [ ] **Step 3: Implement symbolized build, supervised collectors, shared benchmark call, and unconditional canonical restore**

Build with environment:

```python
env = {
    "CARGO_TARGET_DIR": str(config.profile_target),
    "CARGO_PROFILE_RELEASE_DEBUG": "1",
    "CARGO_PROFILE_RELEASE_STRIP": "false",
}
runner.run([cargo, "build", "--release", "--locked"], cwd=config.crate, env=env)
```

Collectors attach to the validated daemon PID. The `finally` sequence is collectors → report → profiling teardown → canonical binary/receipt restore → canonical start → status/integrity/drain validation → isolated target removal.

- [ ] **Step 4: Run ProfileTests GREEN**

Run: `python3 -m unittest scripts.tests.test_vm100_pilot.ProfileTests -v`
Expected: all ProfileTests pass.

- [ ] **Step 5: Commit**

Run: `git add scripts/vm100-pilot.py scripts/vm100_pilot/profile.py scripts/tests/test_vm100_pilot.py && git commit -m 'feat(harness): profile the durability benchmark'`

### Task 5: Raw SFTP and pinned npm/Cargo workloads

**Files:**
- Create: `scripts/vm100_pilot/raw_sftp.py`
- Create: `scripts/vm100_pilot/workloads.py`
- Modify: `scripts/vm100-pilot.py`
- Modify: `scripts/tests/test_vm100_pilot.py`

**Interfaces:**
- Consumes: lifecycle, metrics, runner, receipt
- Produces: `RawSftpRunner.run(jobs=7, per_job_mib=128) -> RawSftpResult`
- Produces: `WorkloadRunner.run(delete_jobs=4) -> WorkloadResult`

- [ ] **Step 1: Write failing tests for seven-worker reaping, remote cleanup, canonical restoration, privileged user-owned workroot creation, serial/parallel delete comparability, and partial phase receipts**

```python
def test_workroot_is_created_with_explicit_owner(self):
    workloads.prepare_root(run_root)
    self.assertIn(["install", "-d", "-m", "0755", "-o", user, "-g", group,
                   str(run_root)], runner.sudo_argv)
```

- [ ] **Step 2: Run WorkloadTests RED**

Run: `python3 -m unittest scripts.tests.test_vm100_pilot.WorkloadTests -v`
Expected: missing workload modules.

- [ ] **Step 3: Implement raw control and real workloads with `finally` cleanup**

Generate incompressible raw-control files in bounded chunks. Clone exact commits. Record Node/npm/pnpm/Rust/Cargo versions. Prefetch package dependencies before timed installs/builds and use offline modes for timed filesystem phases where supported.

- [ ] **Step 4: Run WorkloadTests GREEN**

Run: `python3 -m unittest scripts.tests.test_vm100_pilot.WorkloadTests -v`
Expected: all WorkloadTests pass.

- [ ] **Step 5: Commit**

Run: `git add scripts/vm100-pilot.py scripts/vm100_pilot/raw_sftp.py scripts/vm100_pilot/workloads.py scripts/tests/test_vm100_pilot.py && git commit -m 'feat(harness): automate raw and real workloads'`

### Task 6: Delete Bash, finish CLI composition, and update documentation

**Files:**
- Delete: `scripts/vm100-pilot.sh`
- Delete: `scripts/tests/vm100-pilot-test.sh`
- Modify: `scripts/vm100-pilot.py`
- Modify: all documentation files found by `rg -l 'vm100-pilot\.sh'`
- Modify: `scripts/tests/test_vm100_pilot.py`

**Interfaces:**
- Produces one command surface: `python3 scripts/vm100-pilot.py <command>`

- [ ] **Step 1: Write failing CLI dispatch tests for all eleven commands and compound-command ordering**

```python
def test_cli_exposes_complete_native_command_set(self):
    self.assertEqual(set(parser_command_names()), {
        "setup", "teardown", "restart", "status", "drain", "benchmark",
        "profile", "workloads", "raw-sftp", "iterate", "all",
    })
```

- [ ] **Step 2: Run CliTests RED**

Run: `python3 -m unittest scripts.tests.test_vm100_pilot.CliTests -v`
Expected: incomplete parser command set.

- [ ] **Step 3: Implement parser/dispatch, delete Bash files, and replace every repository command example**

No Python command invokes the deleted Bash path. `iterate` calls setup → benchmark → workloads → raw control; `all` calls benchmark → workloads → raw control on an already active stack.

- [ ] **Step 4: Run complete local gates**

Run:

```bash
python3 -m unittest discover -s scripts/tests -p 'test_vm100_pilot.py' -v
python3 -m compileall -q scripts/vm100-pilot.py scripts/vm100_pilot
python3 scripts/vm100-pilot.py --help
rg -n 'vm100-pilot\.sh|vm100-pilot-test\.sh' . --glob '!target/**'
git diff --check
gitleaks git --no-banner --redact --log-level warn
```

Expected: tests/compile/help/diff/gitleaks pass; ripgrep returns no matches.

- [ ] **Step 5: Commit and push**

Run: `git add -A scripts documentation docs && git commit -m 'feat(harness): replace Bash pilot with Python' && git push origin develop`

### Task 7: VM100 Linux acceptance, profiling, benchmarks, and cleanup proof

**Files:**
- Runtime artifacts only under `/var/tmp/zerofs-pilot-results`

**Interfaces:**
- Consumes the committed Python CLI at exact pushed `develop` commit
- Produces Linux test, profile, benchmark, workload, raw-control, restoration, and cleanup receipts

- [ ] **Step 1: Pull exact commit and run Python tests on VM100**

Run:

```bash
ssh ubuntu-main 'git -C /fast/projects/ZeroFS pull --ff-only origin develop && cd /fast/projects/ZeroFS && python3 -m unittest discover -s scripts/tests -p "test_vm100_pilot.py" -v'
```

Expected: all Python tests pass; `/fast` remains `fastpool/fast` ZFS.

- [ ] **Step 2: Run reduced profiling acceptance**

Run:

```bash
ssh ubuntu-main 'cd /fast/projects/ZeroFS && python3 scripts/vm100-pilot.py profile --total-mib 256 --jobs 4'
```

Expected: profile result includes `perf.data`, text call graph, perf-stat, pidstat, iostat, sar, benchmark receipt, and `canonical_binary_restored=true`.

- [ ] **Step 3: Classify local and remote bottlenecks from profile evidence**

Inspect top inclusive symbols, CPU utilization, process write rate, device throughput/latency, context switches, SFTP session utilization, local/remote completed byte slopes, and accepted/local/remote watermarks. Record evidence-backed hypotheses; do not infer a bottleneck from elapsed time alone.

- [ ] **Step 4: Run full benchmarks and real workloads**

Run:

```bash
ssh ubuntu-main 'cd /fast/projects/ZeroFS && python3 scripts/vm100-pilot.py benchmark --total-mib 1024 --jobs 4'
ssh ubuntu-main 'cd /fast/projects/ZeroFS && python3 scripts/vm100-pilot.py workloads'
ssh ubuntu-main 'cd /fast/projects/ZeroFS && python3 scripts/vm100-pilot.py raw-sftp --stock-ssh /usr/bin/ssh --hpn-ssh /home/zack/.local/opt/hpnssh/e2dfa0cea55d93747f4c68b4a2b134d6fbe0db06/bin/hpnssh'
```

Expected: persistent receipts for every phase and canonical restoration after raw control.

- [ ] **Step 5: Verify final system cleanliness**

Verify services active, XFS mounted from `/dev/nbd0`, integrity SHA and metadata count valid, accepted/local/remote equal, dirty RAM/SSD zero, terminal zero, `/fast` still ZFS, no profile collectors/targets/temporary workroots/raw-control objects, no Bash harness files, and clean develop checkout.

- [ ] **Step 6: Commit any evidence-driven code fixes through fresh TDD cycles, rerun affected gates, push, redeploy, and repeat profile/benchmark until no correctness-preserving high-impact fix remains**

Every fix must include a deterministic RED, focused GREEN, branch-wide gates proportional to scope, a clean commit, Linux validation, and before/after receipts.
