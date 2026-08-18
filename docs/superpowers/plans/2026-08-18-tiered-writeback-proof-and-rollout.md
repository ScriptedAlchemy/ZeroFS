# Tiered Writeback Proof and Rollout Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Prove unified writeback through real crash, restart, NFS, 9P, NBD, WebUI/RPC, filesystem, integrity, performance, cleanup, quality, merge, and Ubuntu fast-forward evidence.

**Architecture:** Portable build/unit/model/browser gates run on macOS. Every kernel mount, block device, Linux filesystem, real process crash, xfstests, pjdfstest, kernel compile, stress-ng, ZFS/XFS-over-NBD, and performance workload runs only in a UUID-ledgered isolated Ubuntu checkout of the exact pushed SHA.

**Tech Stack:** Cargo, failpoints, DST, dependency-free Python 3 `unittest`, NFSv3, v9fs/native 9P, `nbd-client`, XFS, ZFS, xfstests, pjdfstest, stress-ng, Node 22/WASM, Git/SSH, SHA-256 manifests.

**Spec:** `docs/superpowers/specs/2026-08-18-unified-tiered-writeback-design.md`

## Global Constraints

- Every harness setup/run/receipt carries both independent flags exactly: `--filesystem-ack-mode materialized|volatile_memory` and `--object-ack-mode memory|ssd|remote`.
- The harness uses only Python standard-library `unittest`; no third-party test runner is a dependency or command.
- No mock, in-memory filesystem, fixture-only adapter, or direct internal call counts as protocol acceptance.
- macOS runs portable Rust/build/lint/model/WebUI/WASM tests only. Real Linux protocols, mounts, devices, filesystems, process crashes, and benchmarks run on Ubuntu only.
- The Ubuntu proof checkout is `/fast/projects/ZeroFS-unified-tiered-writeback`; `/fast/projects/ZeroFS` remains the clean `develop` checkout until final fast-forward.
- Never touch CT198, VM100 production mounts, production Storage Box prefixes/exports, or an active NBD device.
- Every process, port, mount, device, filesystem/pool name, object prefix, state directory, scratch directory, and checkout is unique and recorded in one UUID resource ledger.
- Cleanup is idempotent after success, failure, partial setup, cancellation, supervisor cancellation, and crash.
- Keep failure receipts; remove only exact ledger-owned resources.

---

### Task C1: Model Volatile Crash Floors and Canonical NBD Member Prefixes

**Files:**
- Modify: `zerofs/src/failpoints.rs`
- Modify: `zerofs/tests/failpoints/consistency.rs`
- Modify: `zerofs/tests/dst/fp_crash.rs`
- Modify: `zerofs/tests/dst/data.rs`
- Modify: `zerofs/tests/dst/namespace.rs`
- Modify: `zerofs/tests/dst/world.rs`
- Modify: `zerofs/tests/dst/checks.rs`

**Interfaces:**
- Produces: volatile-ack index, completed local durability floor, remote durability floor, per-striped-batch canonical member progress, and shutdown crash windows.
- Consumes: mutation/object receipts, typed incarnations, and isolated restart model.

- [ ] **Step 1: Register RED crash points**

Add named failpoints before/after overlay publish, volatile reply, materializer dispatch, each striped member apply, overlay retirement, seal, metadata flush, local receipt, remote publish, watermark/cleanup, each metadata fence stage, and each shutdown phase.

```rust
pub(crate) struct CrashDurabilityModel {
    pub(crate) volatile_acked_through: u64,
    pub(crate) local_durable_through: u64,
    pub(crate) remote_durable_through: u64,
    pub(crate) striped_canonical_members: std::collections::BTreeMap<u64, usize>,
}
```

- [ ] **Step 2: Encode the exact invariant**

Every mutation at or below `local_durable_through` must recover completely with consistent data, attributes, quota, metadata, namespace, and all striped members. Above that floor, ordinary state may be absent; a striped NBD batch may expose only the canonical member prefix recorded before the crash. That prefix must contain whole canonical members in stripe order and may not create torn metadata, a premature logical completion/namespace claim, a resurrected unlink, or stale prepared attributes.

- [ ] **Step 3: Run portable failpoint/DST gates and commit**

```bash
cd /Volumes/bigssd/projects/ZeroFS/.worktrees/unified-tiered-writeback/zerofs
cargo test -p zerofs --features failpoints --test failpoints --locked -- --nocapture
RUSTFLAGS="--cfg dst --cfg tokio_unstable --cfg io_uring_skip_arch_check" cargo test -p zerofs --features failpoints --test dst --locked -- --nocapture
cargo fmt --all -- --check
git diff --check
git add zerofs/src/failpoints.rs zerofs/tests/failpoints/consistency.rs zerofs/tests/dst/fp_crash.rs zerofs/tests/dst/data.rs zerofs/tests/dst/namespace.rs zerofs/tests/dst/world.rs zerofs/tests/dst/checks.rs
git commit -m "test: model volatile mutation crash durability"
```

---

### Task C2: Build a Real Dual-Acknowledgement Linux Harness

**Files:**
- Create: `scripts/tiered-writeback-e2e.py`
- Create: `scripts/tiered_writeback_e2e/__init__.py`
- Create: `scripts/tiered_writeback_e2e/config.py`
- Create: `scripts/tiered_writeback_e2e/resources.py`
- Create: `scripts/tiered_writeback_e2e/lifecycle.py`
- Create: `scripts/tiered_writeback_e2e/protocols.py`
- Create: `scripts/tiered_writeback_e2e/integrity.py`
- Create: `scripts/tiered_writeback_e2e/crash.py`
- Create: `scripts/tiered_writeback_e2e/linux_suites.py`
- Create: `scripts/tests/test_tiered_writeback_e2e.py`

**Interfaces:**
- Produces: `setup`, `run`, `cleanup --ledger`, `assert-clean --ledger`, and `assert-source-idle` commands plus JSON receipts.
- Consumes: exact ZeroFS binary/config SHA, Ubuntu sudo, unused UUID-owned resources, disposable backend namespace, and real client binaries.

- [ ] **Step 1: Write dependency-free safety RED tests**

```python
import unittest

class ResourceLedgerTests(unittest.TestCase):
    def test_cleanup_rejects_non_uuid_root(self):
        ledger = ResourceLedger(run_id="not-a-uuid", run_root="/var/tmp/test")
        with self.assertRaises(UnsafeCleanupTarget):
            ledger.validate_cleanup_scope()
```

Name tests for rejecting `/`, `/mnt`, `/var/tmp`, workspace roots, CT198/production strings, unowned PIDs/devices/mounts/ports, missing dual ack flags, receipt omission, partial setup, primary-plus-cleanup errors, idempotent cleanup, and supervisor cancellation.

```bash
cd /Volumes/bigssd/projects/ZeroFS/.worktrees/unified-tiered-writeback
python3 -m unittest discover -s scripts/tests -p 'test_*.py' -v
```

Expected RED: modules and commands do not exist.

- [ ] **Step 2: Implement the exact CLI contract**

Every `setup` and `run` command requires both:

```text
--filesystem-ack-mode materialized|volatile_memory
--object-ack-mode memory|ssd|remote
```

The JSON receipt records both fields, source HEAD, binary/config hashes, UUID/root, exact PIDs, ports, devices, mounts, pool/filesystem names, backend prefix, scenario, manifest, durability floors, terminal state, commands, exit status, and cleanup status. `cleanup --ledger PATH` removes only ledger-owned resources and succeeds when repeated. `assert-clean --ledger PATH` fails if any recorded process/listener/mount/device/pool/prefix/path remains. `assert-source-idle --source-root PATH` inspects `/proc/*/cwd` and fails for active `cargo`, `rustc`, test, or harness jobs rooted at PATH.

- [ ] **Step 3: Implement real shipping entry points**

NFS uses a hard NFSv3 kernel mount and actual WRITE/COMMIT; 9P uses both v9fs and native client plus actual Twrite/Tfsync; NBD uses `nbd-client`, a disposable device, XFS/ZFS, WRITE/FUA/FLUSH; RPC uses the actual Unix/TCP gRPC client/server; WebUI uses the real gRPC-Web/WebSocket/9P route and generated WASM client. Internal Rust calls may instrument failpoints but never replace these acceptance legs.

- [ ] **Step 4: Run portable harness gates and commit**

```bash
cd /Volumes/bigssd/projects/ZeroFS/.worktrees/unified-tiered-writeback
python3 -m compileall -q scripts/tiered_writeback_e2e scripts/tiered-writeback-e2e.py
python3 -m unittest discover -s scripts/tests -p 'test_*.py' -v
git diff --check
git add scripts/tiered-writeback-e2e.py scripts/tiered_writeback_e2e/__init__.py scripts/tiered_writeback_e2e/config.py scripts/tiered_writeback_e2e/resources.py scripts/tiered_writeback_e2e/lifecycle.py scripts/tiered_writeback_e2e/protocols.py scripts/tiered_writeback_e2e/integrity.py scripts/tiered_writeback_e2e/crash.py scripts/tiered_writeback_e2e/linux_suites.py scripts/tests/test_tiered_writeback_e2e.py
git commit -m "test: add real tiered writeback Linux harness"
```

---

### Task C3: Prove Real Cross-Protocol Admission, Coherence, Barriers, and WebUI/RPC

**Files:**
- Modify only after a real RED receipt: the smallest harness or production file owning the failure
- Receipt: UUID run root outside Git

**Interfaces:**
- Produces: real simultaneous admission, pending-read, cross-adapter durability, and WebUI/RPC receipts.
- Consumes: exact pushed feature SHA and isolated Ubuntu checkout.

- [ ] **Step 1: Create the isolated checkout and UUID ledger**

```bash
cd /Volumes/bigssd/projects/ZeroFS/.worktrees/unified-tiered-writeback
git push -u origin codex/unified-tiered-writeback
ssh ubuntu-main 'cd /fast/projects/ZeroFS && test -z "$(git status --porcelain=v1)" && git fetch origin codex/unified-tiered-writeback && test ! -e /fast/projects/ZeroFS-unified-tiered-writeback && git worktree add /fast/projects/ZeroFS-unified-tiered-writeback origin/codex/unified-tiered-writeback'
ssh ubuntu-main 'cd /fast/projects/ZeroFS-unified-tiered-writeback && test "$(git rev-parse HEAD)" = "$(git rev-parse origin/codex/unified-tiered-writeback)" && test -z "$(git status --porcelain=v1)"'
```

On Ubuntu:

```bash
cd /fast/projects/ZeroFS-unified-tiered-writeback
RUN_UUID="$(python3 -c 'import uuid; print(uuid.uuid4())')"
RUN_ROOT="/var/tmp/zerofs-tiered-${RUN_UUID}"
LEDGER="${RUN_ROOT}/ledger.json"
sudo python3 scripts/tiered-writeback-e2e.py setup --ledger "$LEDGER" --filesystem-ack-mode volatile_memory --object-ack-mode memory
```

- [ ] **Step 2: Run the exact volatile shipping scenarios**

Each command uses real NBD/NFS/9P/WebUI/RPC clients and appends a receipt carrying both flags:

```bash
sudo python3 scripts/tiered-writeback-e2e.py run --ledger "$LEDGER" --filesystem-ack-mode volatile_memory --object-ack-mode memory --scenario global-admission-nbd-nfs-ninep
sudo python3 scripts/tiered-writeback-e2e.py run --ledger "$LEDGER" --filesystem-ack-mode volatile_memory --object-ack-mode memory --scenario cross-adapter-pending-read-same-backing-inode
sudo python3 scripts/tiered-writeback-e2e.py run --ledger "$LEDGER" --filesystem-ack-mode volatile_memory --object-ack-mode memory --scenario nfs-commit-covers-prior-nbd
sudo python3 scripts/tiered-writeback-e2e.py run --ledger "$LEDGER" --filesystem-ack-mode volatile_memory --object-ack-mode memory --scenario ninep-fsync-covers-prior-nfs
sudo python3 scripts/tiered-writeback-e2e.py run --ledger "$LEDGER" --filesystem-ack-mode volatile_memory --object-ack-mode memory --scenario nbd-flush-covers-prior-ninep
sudo python3 scripts/tiered-writeback-e2e.py run --ledger "$LEDGER" --filesystem-ack-mode volatile_memory --object-ack-mode memory --scenario webui-rpc-production-path
```

The same-backing-inode scenario provisions an NBD member as a normal ZeroFS inode reachable by the direct namespace, pauses canonical materialization, writes through the live NBD server, and reads that exact inode through mounted NFS and 9P. It does not claim guest-XFS namespace unification.

- [ ] **Step 3: Run materialized/object-target controls**

```bash
sudo python3 scripts/tiered-writeback-e2e.py cleanup --ledger "$LEDGER"
sudo python3 scripts/tiered-writeback-e2e.py assert-clean --ledger "$LEDGER"
RUN_UUID="$(python3 -c 'import uuid; print(uuid.uuid4())')"; RUN_ROOT="/var/tmp/zerofs-tiered-${RUN_UUID}"; LEDGER="${RUN_ROOT}/ledger.json"
sudo python3 scripts/tiered-writeback-e2e.py setup --ledger "$LEDGER" --filesystem-ack-mode materialized --object-ack-mode ssd
sudo python3 scripts/tiered-writeback-e2e.py run --ledger "$LEDGER" --filesystem-ack-mode materialized --object-ack-mode ssd --scenario protocol-materialized-control
sudo python3 scripts/tiered-writeback-e2e.py cleanup --ledger "$LEDGER"
sudo python3 scripts/tiered-writeback-e2e.py assert-clean --ledger "$LEDGER"
```

Run the remote object-ack control under a new matching ledger:

```bash
RUN_UUID="$(python3 -c 'import uuid; print(uuid.uuid4())')"; RUN_ROOT="/var/tmp/zerofs-tiered-${RUN_UUID}"; LEDGER="${RUN_ROOT}/ledger.json"
sudo python3 scripts/tiered-writeback-e2e.py setup --ledger "$LEDGER" --filesystem-ack-mode materialized --object-ack-mode remote
sudo python3 scripts/tiered-writeback-e2e.py run --ledger "$LEDGER" --filesystem-ack-mode materialized --object-ack-mode remote --scenario protocol-durability-target-control
sudo python3 scripts/tiered-writeback-e2e.py cleanup --ledger "$LEDGER"
sudo python3 scripts/tiered-writeback-e2e.py assert-clean --ledger "$LEDGER"
```

Ordinary object acknowledgement policy must not alter protocol barrier semantics.

---

### Task C4: Run Exact Ubuntu Filesystem Workflows

**Files:**
- Modify only after real failure: `scripts/tiered_writeback_e2e/linux_suites.py` or the owning production source
- Receipt: UUID run roots outside Git

**Interfaces:**
- Produces: xfstests, pjdfstest, kernel compile, stress-ng, XFS-over-NBD, and ZFS-over-NBD receipts.
- Consumes: harness setup with both ack flags and ledger-generated paths/devices/names.

Before each scenario in this task, create a fresh ledger exactly as in Task C3 and run `setup` with `--filesystem-ack-mode volatile_memory --object-ack-mode ssd`. After the scenario, run `cleanup --ledger` twice and `assert-clean --ledger` before rebinding `RUN_UUID`, `RUN_ROOT`, and `LEDGER` for the next scenario.

- [ ] **Step 1: Run NFS and 9P xfstests using existing workflow commands**

For each new ledger, `linux_suites.py` writes ledger-specific `local.config` and excludes, then executes exactly:

```bash
cd /tmp/xfstests
sudo ./check -g quick -E "$RUN_ROOT/xfstests-nfs.excludes"
sudo env HOST_OPTIONS="$RUN_ROOT/local.9p.strict.config" RESULT_BASE="$RUN_ROOT/results-9p-strict" ./check generic/732
```

Invoke it with both flags:

```bash
sudo python3 scripts/tiered-writeback-e2e.py run --ledger "$LEDGER" --filesystem-ack-mode volatile_memory --object-ack-mode ssd --scenario xfstests-nfs-quick
sudo python3 scripts/tiered-writeback-e2e.py run --ledger "$LEDGER" --filesystem-ack-mode volatile_memory --object-ack-mode ssd --scenario xfstests-ninep-quick-and-strict
```

- [ ] **Step 2: Run pjdfstest and stress-ng exactly**

The harness executes in the ledger-owned 9P/NFS mount:

```bash
find /tmp/pjdfstest/tests -name '*.t' -type f | sort > "$RUN_ROOT/pjdfstest-all.txt"
grep -v -f /fast/projects/ZeroFS-unified-tiered-writeback/.github/.pjdfstest-9p-exclude "$RUN_ROOT/pjdfstest-all.txt" > "$RUN_ROOT/pjdfstest-run.txt"
sudo prove -rv $(cat "$RUN_ROOT/pjdfstest-run.txt")
sudo stress-ng --job /fast/projects/ZeroFS-unified-tiered-writeback/.github/stress-ng-filesystem.job
```

```bash
sudo python3 scripts/tiered-writeback-e2e.py run --ledger "$LEDGER" --filesystem-ack-mode volatile_memory --object-ack-mode ssd --scenario pjdfstest-ninep
sudo python3 scripts/tiered-writeback-e2e.py run --ledger "$LEDGER" --filesystem-ack-mode volatile_memory --object-ack-mode ssd --scenario stress-ng-nfs-ninep
```

- [ ] **Step 3: Compile a pinned kernel over NFS and 9P**

The harness downloads `linux-7.2-rc1.tar.gz`, verifies the ledger-recorded SHA-256, extracts inside each ledger mount, and runs exactly:

```bash
make -C "$KERNEL_SOURCE" O="$KERNEL_BUILD" tinyconfig
make -C "$KERNEL_SOURCE" O="$KERNEL_BUILD" -j"$(nproc)" vmlinux
test -s "$KERNEL_BUILD/vmlinux"
```

```bash
sudo python3 scripts/tiered-writeback-e2e.py run --ledger "$LEDGER" --filesystem-ack-mode volatile_memory --object-ack-mode ssd --scenario kernel-compile-nfs
sudo python3 scripts/tiered-writeback-e2e.py run --ledger "$LEDGER" --filesystem-ack-mode volatile_memory --object-ack-mode ssd --scenario kernel-compile-ninep
```

- [ ] **Step 4: Run XFS and ZFS over the exact ledger NBD device**

The XFS scenario executes `mkfs.xfs -f -L "$LEDGER_XFS_LABEL" "$LEDGER_NBD_DEVICE"`, mounts it at the ledger path, writes deterministic files, `sync`, unmounts, disconnects/reconnects the same export, runs `xfs_repair -n`, remounts, and verifies the SHA-256 manifest. The ZFS scenario executes `zpool create "$LEDGER_ZPOOL" "$LEDGER_NBD_DEVICE"`, `zfs create "$LEDGER_ZPOOL/data"`, writes/checksums, `zpool sync`, exports, reconnects, imports, scrubs, and verifies.

```bash
sudo python3 scripts/tiered-writeback-e2e.py run --ledger "$LEDGER" --filesystem-ack-mode volatile_memory --object-ack-mode ssd --scenario xfs-over-nbd-restart
sudo python3 scripts/tiered-writeback-e2e.py run --ledger "$LEDGER" --filesystem-ack-mode volatile_memory --object-ack-mode ssd --scenario zfs-over-nbd-restart
sudo python3 scripts/tiered-writeback-e2e.py cleanup --ledger "$LEDGER"
sudo python3 scripts/tiered-writeback-e2e.py assert-clean --ledger "$LEDGER"
```

No command in this task runs on macOS.

---

### Task C5: Add One Tiered CI Leg Per Existing Linux Workflow

**Files:**
- Modify: `.github/workflows/xfstests-nfs.yml`
- Modify: `.github/workflows/xfstests-9p.yml`
- Modify: `.github/workflows/pjdfstest-9p.yml`
- Modify: `.github/workflows/kernel-compile-nfs.yml`
- Modify: `.github/workflows/kernel-compile-9p.yml`
- Modify: `.github/workflows/stress-ng.yml`
- Modify: `.github/workflows/zfs-test.yml`
- Create: `.github/workflows/xfs-nbd.yml`
- Modify: `scripts/tests/test_tiered_writeback_e2e.py`

**Interfaces:**
- Produces: retained materialized controls plus one ledgered volatile/tiered leg for every relevant protocol/filesystem workflow.
- Consumes: Task C2 harness and current workflow commands.

- [ ] **Step 1: Add RED static workflow tests**

Use `unittest` to parse workflow text and require both ack flags, a unique ledger, `cleanup --ledger` under `always()`, `assert-clean --ledger`, unchanged materialized control, runner-owned device checks, and no CT198/production target.

- [ ] **Step 2: Add minimal matrix legs and validate**

```bash
cd /Volumes/bigssd/projects/ZeroFS/.worktrees/unified-tiered-writeback
actionlint .github/workflows/*.yml
python3 -m unittest discover -s scripts/tests -p 'test_*.py' -v
git diff --check
git add .github/workflows/xfstests-nfs.yml .github/workflows/xfstests-9p.yml .github/workflows/pjdfstest-9p.yml .github/workflows/kernel-compile-nfs.yml .github/workflows/kernel-compile-9p.yml .github/workflows/stress-ng.yml .github/workflows/zfs-test.yml .github/workflows/xfs-nbd.yml scripts/tests/test_tiered_writeback_e2e.py
git commit -m "ci: exercise tiered writeback Linux protocols"
```

---

### Task C6: Prove Real Crash, Restart, Terminal Fanout, and Bounded Shutdown

**Files:**
- Create: `zerofs/tests/writeback_recovery.rs`
- Create: `zerofs/tests/writeback_faults.rs`
- Modify: `scripts/tiered_writeback_e2e/crash.py`
- Modify: `scripts/tests/test_tiered_writeback_e2e.py`

**Interfaces:**
- Produces: receipts for before-ack, RAM-ack, member-apply, metadata flush, local SSD, remote publish, watermark/cleanup, metadata fence, and every shutdown stage.
- Consumes: real process/PID harness, failpoints, retained SSD journal, clean-cache reopen, and dual ack flags.

- [ ] **Step 1: Add RED crash assertions**

For each boundary, assert complete/consistent recovery at and below the completed local floor. Above it, allow only the explicit canonical striped-member prefix from Task C1 and forbid torn metadata/namespace/logical-completion claims. Remote receipts must survive deleting the exact ledger-owned local state before reopen.

- [ ] **Step 2: Run the real Ubuntu crash modes**

Each command below uses its own newly created ledger. Its preceding `setup` command uses the same filesystem/object ack pair shown on that command; after the run, execute `cleanup --ledger` twice and `assert-clean --ledger` before creating the next ledger.

```bash
cd /fast/projects/ZeroFS-unified-tiered-writeback
sudo python3 scripts/tiered-writeback-e2e.py run --ledger "$LEDGER" --filesystem-ack-mode volatile_memory --object-ack-mode memory --scenario crash-boundary-matrix
sudo python3 scripts/tiered-writeback-e2e.py run --ledger "$LEDGER" --filesystem-ack-mode volatile_memory --object-ack-mode ssd --scenario local-receipt-restart
sudo python3 scripts/tiered-writeback-e2e.py run --ledger "$LEDGER" --filesystem-ack-mode volatile_memory --object-ack-mode remote --scenario remote-receipt-clean-cache-restart
sudo python3 scripts/tiered-writeback-e2e.py run --ledger "$LEDGER" --filesystem-ack-mode volatile_memory --object-ack-mode ssd --scenario terminal-fanout-and-shutdown-timeout
cd zerofs
cargo test -p zerofs --test failover_e2e --locked -- --ignored --nocapture
```

Supervisor cancellation must interrupt the harness during setup, workload, crash/restart, and cleanup; the signal handler invokes idempotent cleanup and the test then runs `assert-clean --ledger`.

- [ ] **Step 3: Run portable regressions and commit exact files**

```bash
cd /Volumes/bigssd/projects/ZeroFS/.worktrees/unified-tiered-writeback/zerofs
cargo test -p zerofs --test writeback_recovery --locked -- --nocapture
cargo test -p zerofs --test writeback_faults --locked -- --nocapture
cd ..
python3 -m unittest discover -s scripts/tests -p 'test_*.py' -v
git diff --check
git add zerofs/tests/writeback_recovery.rs zerofs/tests/writeback_faults.rs scripts/tiered_writeback_e2e/crash.py scripts/tests/test_tiered_writeback_e2e.py
git commit -m "test: prove tiered writeback crash recovery"
```

---

### Task C7: Benchmark Each Durability Boundary From Ledger Scratch

**Files:**
- Modify only after a real RED: existing mutation/writeback benchmark instrumentation
- Receipt: UUID Ubuntu run root outside Git

**Interfaces:**
- Produces: separate foreground RAM-ack, local SSD cutoff, paced remote-drain, and remote flush throughput/latency with integrity.
- Consumes: ledger `local_ssd_scratch`, incompressible payloads, disposable backend, exact metrics, and both ack flags.

- [ ] **Step 1: Derive and validate scratch from the ledger**

```bash
cd /fast/projects/ZeroFS-unified-tiered-writeback
BENCH_SCRATCH="$(python3 scripts/tiered-writeback-e2e.py ledger-value --ledger "$LEDGER" --key local_ssd_scratch)"
python3 scripts/tiered-writeback-e2e.py validate-owned-path --ledger "$LEDGER" --path "$BENCH_SCRATCH"
cd zerofs
```

- [ ] **Step 2: List exact ignored benchmarks and reject zero selection**

```bash
cargo test --release -p zerofs --lib --locked -- --list | tee "$RUN_ROOT/rust-bench-tests.list"
grep -F 'writeback::store::tests::bench_writeback_tier_profile' "$RUN_ROOT/rust-bench-tests.list"
grep -F 'writeback::journaler::tests::drain_throughput_of_the_post_ack_durability_tail' "$RUN_ROOT/rust-bench-tests.list"
grep -F 'writeback::store::tests::bench_remote_replay_throughput_against_throttled_backend' "$RUN_ROOT/rust-bench-tests.list"
grep -F 'writeback::journal::tests::publication_batch_size_amortizes_the_journal_fixed_cost' "$RUN_ROOT/rust-bench-tests.list"
grep -F 'writeback::journal::tests::remote_commit_serialization_cost_bounds_replay_throughput' "$RUN_ROOT/rust-bench-tests.list"
```

- [ ] **Step 3: Run every exact benchmark name**

```bash
ZEROFS_BENCH_DIR="$BENCH_SCRATCH/tier-profile" cargo test --release -p zerofs --lib --locked writeback::store::tests::bench_writeback_tier_profile -- --ignored --exact --nocapture
ZEROFS_BENCH_DIR="$BENCH_SCRATCH/drain" cargo test --release -p zerofs --lib --locked writeback::journaler::tests::drain_throughput_of_the_post_ack_durability_tail -- --ignored --exact --nocapture
ZEROFS_BENCH_DIR="$BENCH_SCRATCH/remote-replay" cargo test --release -p zerofs --lib --locked writeback::store::tests::bench_remote_replay_throughput_against_throttled_backend -- --ignored --exact --nocapture
ZEROFS_BENCH_DIR="$BENCH_SCRATCH/publication-batch" cargo test --release -p zerofs --lib --locked writeback::journal::tests::publication_batch_size_amortizes_the_journal_fixed_cost -- --ignored --exact --nocapture
ZEROFS_BENCH_DIR="$BENCH_SCRATCH/remote-commit" cargo test --release -p zerofs --lib --locked writeback::journal::tests::remote_commit_serialization_cost_bounds_replay_throughput -- --ignored --exact --nocapture
```

Each command must report exactly one executed test; any zero-test or multi-test receipt is rejected.

- [ ] **Step 4: Run real protocol benchmarks and integrity gates**

Each benchmark command uses a separate ledger whose `setup` command has the same two ack flags; cleanup and `assert-clean` complete before the next ledger is created.

```bash
cd /fast/projects/ZeroFS-unified-tiered-writeback
sudo python3 scripts/tiered-writeback-e2e.py run --ledger "$LEDGER" --filesystem-ack-mode volatile_memory --object-ack-mode memory --scenario benchmark-ram-ack
sudo python3 scripts/tiered-writeback-e2e.py run --ledger "$LEDGER" --filesystem-ack-mode volatile_memory --object-ack-mode ssd --scenario benchmark-local-ssd
sudo python3 scripts/tiered-writeback-e2e.py run --ledger "$LEDGER" --filesystem-ack-mode volatile_memory --object-ack-mode remote --scenario benchmark-paced-remote
```

Every receipt includes size/SHA-256/readback, mutation/object floors, dirty tiers, terminal state, CPU/RAM/local allocation, remote rate, and cleanup. Performance without integrity and durability is rejected.

---

### Task C8: Run Complete Repository, Quality, and Evidence Gates

**Files:**
- No planned edits; every failure gets a separate narrow RED/GREEN commit

- [ ] **Step 1: Run macOS root/workspace gates from exact CWDs**

From `/Volumes/bigssd/projects/ZeroFS/.worktrees/unified-tiered-writeback/zerofs`:

```bash
cargo build --workspace --locked
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo test --workspace --all-targets --locked
cargo clippy -p zerofs --features failpoints --tests --locked -- -D warnings
cargo test -p zerofs --features failpoints --test failpoints --locked -- --nocapture
RUSTFLAGS="--cfg dst --cfg tokio_unstable --cfg io_uring_skip_arch_check" cargo clippy -p zerofs --features failpoints --test dst --locked -- -D warnings
RUSTFLAGS="--cfg dst --cfg tokio_unstable --cfg io_uring_skip_arch_check" cargo test -p zerofs --features failpoints --test dst --locked -- --nocapture
cargo test -p zerofs-client --all-features --locked
cargo check -p ninep-client --target wasm32-unknown-unknown --locked
```

From `/Volumes/bigssd/projects/ZeroFS/.worktrees/unified-tiered-writeback`:

```bash
python3 -m compileall -q scripts proxmox
python3 -m unittest discover -s scripts/tests -p 'test_*.py' -v
python3 -m unittest discover -s proxmox/tests -p 'test_*.py' -v
find proxmox -type f -name '*.sh' -exec shellcheck {} +
actionlint .github/workflows/*.yml
make webui
cd zerofs
cargo test -p zerofs --features webui webui::tests::wasm_client_smoke --locked -- --ignored --nocapture
cd ..
git diff --check origin/develop...HEAD
```

From `/Volumes/bigssd/projects/ZeroFS/.worktrees/unified-tiered-writeback/bench`:

```bash
cargo fmt --all -- --check
cargo clippy --all-targets --locked -- -D warnings
cargo build --locked
cargo build --release --locked
cargo run --locked -- run --list
```

- [ ] **Step 2: Run Linux-only gates on the exact pushed SHA**

On Ubuntu `/fast/projects/ZeroFS-unified-tiered-writeback`, prove HEAD equals `origin/codex/unified-tiered-writeback` and porcelain is clean, then run the full workspace/failpoint/DST/ignored failover tests and every Task C3-C7 harness scenario. No Linux mount/device proof runs on Mac.

- [ ] **Step 3: Perform the requirement-to-evidence and cleanup audits**

Map every approved-spec requirement to task, commit SHA, command, host, CWD, flags, receipt path, and result. Separately map every ledger resource to a successful `cleanup` and `assert-clean` receipt. Any missing mapping blocks completion.

- [ ] **Step 4: Run all code-quality reviews**

Run simplify, deslop, branch-scope-audit, low-value-churn-audit, TraceDecay code-health, TraceDecay Hawk, `thermo-nuclear-review`, and `thermo-nuclear-code-quality-review` against `origin/develop...HEAD`. Resolve every P0/P1/P2 and rerun the affected focused, full, Linux, and audit gates.

---

### Task C9: Clean Resources, Merge, Push, Fast-Forward Ubuntu, and Remove Secondary Worktrees

**Files:**
- Git history/worktree state and external ledger-owned resources only

- [ ] **Step 1: Run idempotent cleanup and global ownership audit**

For every ledger:

```bash
sudo python3 scripts/tiered-writeback-e2e.py cleanup --ledger "$LEDGER"
sudo python3 scripts/tiered-writeback-e2e.py cleanup --ledger "$LEDGER"
sudo python3 scripts/tiered-writeback-e2e.py assert-clean --ledger "$LEDGER"
```

Prove no UUID-owned mount/device/pool/filesystem/process/listener/socket/prefix/root remains. Prove no active build/test/harness job exists before touching checkouts. Remove the separate `nfsserve` worktree only after its immutable pushed revision is pinned and its worktree is clean.

- [ ] **Step 2: Final review and merge/push `develop`**

Root records `FEATURE_SHA`, fetches, proves clean porcelain and `origin/develop` ancestry, reruns the complete final gate if history was rewritten, then fast-forwards local `develop` and pushes it. Record `EXPECTED_OLD_SHA` and pushed `EXPECTED_NEW_SHA`.

- [ ] **Step 3: Fail-closed Ubuntu fast-forward**

Before the command, substitute the recorded literal SHAs for `EXPECTED_OLD_SHA` and `EXPECTED_NEW_SHA`. On Ubuntu:

```bash
cd /fast/projects/ZeroFS
test -z "$(git status --porcelain=v1)"
test "$(git branch --show-current)" = develop
test "$(git rev-parse HEAD)" = "$EXPECTED_OLD_SHA"
python3 /fast/projects/ZeroFS-unified-tiered-writeback/scripts/tiered-writeback-e2e.py assert-source-idle --source-root /fast/projects/ZeroFS
git fetch origin develop
test "$(git rev-parse origin/develop)" = "$EXPECTED_NEW_SHA"
git merge-base --is-ancestor "$EXPECTED_OLD_SHA" "$EXPECTED_NEW_SHA"
git merge --ff-only "$EXPECTED_NEW_SHA"
test "$(git rev-parse HEAD)" = "$EXPECTED_NEW_SHA"
test -z "$(git status --porcelain=v1)"
```

Any dirty path, wrong branch/SHA, active job, or failed ancestry check stops without changing the checkout.

- [ ] **Step 4: Remove isolated proof and local feature worktrees last**

After all ledgers are clean and Ubuntu `develop` matches the pushed SHA:

```bash
cd /fast/projects/ZeroFS
git worktree remove /fast/projects/ZeroFS-unified-tiered-writeback
git worktree prune
git worktree list --porcelain
```

Then Root removes only `/Volumes/bigssd/projects/ZeroFS/.worktrees/unified-tiered-writeback`, prunes, and lists local worktrees. The completion receipt explicitly proves no feature deployment/restart occurred on CT198 after the already recorded operational exception; it does not falsely claim CT198 was never restarted.
