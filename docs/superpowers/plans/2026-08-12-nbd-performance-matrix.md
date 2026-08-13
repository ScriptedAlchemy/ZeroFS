# NBD Performance Matrix Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add one Python CLI command that runs isolated direct-I/O NBD write cells across the requested block-size and job-count matrix and emits durable JSON/CSV evidence for each durability boundary.

**Architecture:** A focused `performance_matrix.py` module owns immutable cell definitions, fio JSON parsing, one-cell measurement, matrix orchestration, and receipt serialization. It reuses the pilot's lifecycle, writeback metrics, system-I/O sampler, path guards, and receipt type, but keeps raw artifacts in `/dev/shm` until measurement is over so receipt I/O cannot contaminate later cells.

**Tech Stack:** Python 3 standard library, `unittest`, fio JSON, existing `vm100_pilot` lifecycle/metrics/receipt helpers.

## Global Constraints

- Work only in the new linked worktree based on exact commit `fb04552`.
- Do not touch VM100, push, deploy, or change ZeroFS production code.
- Use direct I/O for every fio cell.
- Full matrix block sizes are `32K`, `128K`, `256K`, `1M`, and `4M`; job counts are `1`, `4`, and `8`.
- Persist exact fio bytes/runtime, derived request rate, writeback snapshots, syncfs local tail, remote target crossing, disk/PSI evidence, JSON/CSV receipts, and scoped-cleanup evidence.
- Provide a small `--quick` matrix without Bash.

---

### Task 1: Matrix contracts and fio parsing

**Files:**
- Create: `scripts/vm100_pilot/performance_matrix.py`
- Create: `scripts/tests/test_performance_matrix.py`

**Interfaces:**
- Produces: `MatrixCell(block_size: str, block_size_bytes: int, jobs: int)`, `matrix_cells(quick: bool) -> tuple[MatrixCell, ...]`, and `MatrixFioResult.from_json(path: Path) -> MatrixFioResult`.
- `MatrixFioResult` exposes exact `bytes`, `runtime_ms`, `requests`, and derived `requests_per_second` and `mibps`.

- [ ] **Step 1: Write failing tests for the exact 15-cell full matrix, bounded quick matrix, and hand-derived multi-job fio totals.**
- [ ] **Step 2: Run the focused tests and verify imports or assertions fail for the missing feature.**
- [ ] **Step 3: Implement immutable matrix definitions and strict fio parsing with positive exact counters.**
- [ ] **Step 4: Run the focused tests and verify they pass.**

### Task 2: Isolated one-cell measurement

**Files:**
- Modify: `scripts/vm100_pilot/performance_matrix.py`
- Test: `scripts/tests/test_performance_matrix.py`

**Interfaces:**
- Consumes: existing `PilotConfig`, `PilotLifecycle`, `Runner`, `_MetricSampler`, `WritebackSnapshot`, `SystemIoSnapshot`, and `BlockIoSnapshot`.
- Produces: `PerformanceMatrixRunner._run_cell(...) -> MatrixCellResult` with explicit before/after-fio/accepted/after-syncfs/remote/post-drain snapshots and phase timestamps.

- [ ] **Step 1: Write a failing behavioral test using a real temporary mount tree and deterministic external-boundary fakes.**
- [ ] **Step 2: Verify RED shows the absent runner behavior.**
- [ ] **Step 3: Implement exact-byte fio argv (`--direct=1`, block size, jobs, per-job size), syncfs timing, accepted/local validation, first sampled remote crossing, NBD/local-disk/PSI deltas, and contamination rejection.**
- [ ] **Step 4: Verify GREEN and mutation-check wrong block size, short bytes, missing remote crossing, and a local sequence behind the accepted target.**

### Task 3: Full orchestration, receipts, and cleanup

**Files:**
- Modify: `scripts/vm100_pilot/performance_matrix.py`
- Test: `scripts/tests/test_performance_matrix.py`

**Interfaces:**
- Produces: `PerformanceMatrixRunner.run(total_mib: int, quick: bool) -> PerformanceMatrixResult` and durable `summary.json`, `cells.csv`, per-cell fio JSON, writeback CSV, and system-I/O CSV artifacts.

- [ ] **Step 1: Write failing tests that require per-cell unique roots, `/dev/shm` scratch artifacts, strict between-cell drain, success cleanup, and failure cleanup with a surviving failed manifest.**
- [ ] **Step 2: Verify RED fails because orchestration and artifacts are absent.**
- [ ] **Step 3: Implement sequential isolation, late artifact persistence, atomic receipt manifest updates, scoped root/scratch cleanup, and aggregation that preserves primary plus cleanup failures.**
- [ ] **Step 4: Verify GREEN and inspect artifacts against literal JSON/CSV expectations.**

### Task 4: One-command CLI and final verification

**Files:**
- Modify: `scripts/vm100-pilot.py`
- Test: `scripts/tests/test_performance_matrix.py`

**Interfaces:**
- Produces: `python3 scripts/vm100-pilot.py performance-matrix [--quick] [--total-mib N]`.

- [ ] **Step 1: Write a failing subprocess help test and dispatch test for the new command.**
- [ ] **Step 2: Verify RED fails because the parser rejects `performance-matrix`.**
- [ ] **Step 3: Wire the parser and dispatcher to `PerformanceMatrixRunner`; default to `256 MiB` per full cell and `32 MiB` per quick cell when `--total-mib` is omitted.**
- [ ] **Step 4: Run the focused test, all VM100 pilot Python tests, compile checks, help smoke test, diff review, and clean-status check.**
- [ ] **Step 5: Commit only the plan, Python module, CLI integration, and tests.**

## Self-Review

- Spec coverage: every requested matrix dimension, direct-I/O property, timing/counter/snapshot receipt, telemetry field, output format, cleanup boundary, and quick mode has a corresponding test task.
- Placeholder scan: no deferred implementation placeholders remain.
- Type consistency: `MatrixCell`, `MatrixFioResult`, `MatrixCellResult`, and `PerformanceMatrixResult` flow from parsing through orchestration to CLI JSON emission.
