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
- Every process, port, mount, device, filesystem/pool name, object prefix, state directory, scratch directory, and tool checkout is unique and recorded in one UUID resource ledger.
- The immutable ledger and cleanup receipts live in `CONTROL_ROOT=/var/tmp/zerofs-tiered-control-$RUN_UUID`; disposable processes, mounts, devices, data, scratch, and tool checkouts live in the separate `RESOURCE_ROOT=/var/tmp/zerofs-tiered-resources-$RUN_UUID`. Cleanup never deletes its own authority.
- Cleanup is idempotent after success, failure, partial setup, cancellation, supervisor cancellation, and crash.
- Keep failure receipts; remove only exact ledger-owned resources.

## Required Promotion Before Every Real Ubuntu Slice

Every real Linux slice, including a rerun after any corrective commit, executes this order with no exceptions: RED evidence; implementation; portable GREEN; exact-file commit and review; push; fail-closed isolated Ubuntu synchronization to that commit's 40-hex SHA; then real Linux proof. From the local feature worktree:

```bash
cd /Volumes/bigssd/projects/ZeroFS/.worktrees/unified-tiered-writeback
test -z "$(git status --porcelain=v1)"
test "$(git branch --show-current)" = codex/unified-tiered-writeback
SLICE_SHA="$(git rev-parse HEAD^{commit})"
test "${#SLICE_SHA}" = 40
git show --stat --oneline "$SLICE_SHA"
git push origin "$SLICE_SHA:refs/heads/codex/unified-tiered-writeback"
test "$(git ls-remote origin refs/heads/codex/unified-tiered-writeback | awk '{print $1}')" = "$SLICE_SHA"
ssh ubuntu-main "cd /fast/projects/ZeroFS && git fetch origin codex/unified-tiered-writeback && test \"\$(git rev-parse origin/codex/unified-tiered-writeback)\" = '$SLICE_SHA'"
ssh ubuntu-main "test -d /fast/projects/ZeroFS-unified-tiered-writeback || (cd /fast/projects/ZeroFS && git worktree add --detach /fast/projects/ZeroFS-unified-tiered-writeback '$SLICE_SHA')"
ssh ubuntu-main "cd /fast/projects/ZeroFS-unified-tiered-writeback && test -z \"\$(git status --porcelain=v1)\" && python3 scripts/tiered-writeback-e2e.py assert-source-idle --source-root /fast/projects/ZeroFS-unified-tiered-writeback && git switch --detach '$SLICE_SHA' && test \"\$(git rev-parse HEAD)\" = '$SLICE_SHA' && test -z \"\$(git status --porcelain=v1)\""
```

The slice receipt records the literal expanded `SLICE_SHA` before any Ubuntu command. A Linux-discovered failure returns to RED/implementation/portable GREEN/commit/review, creates a new literal `SLICE_SHA`, and repeats this full block before the failed Linux command is rerun.

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
- Produces: incarnation-bound volatile acknowledgement cutoff, local/remote durability floors, typed striped-batch canonical member progress, and shutdown crash windows.
- Consumes: mutation/object receipts, typed incarnations, and isolated restart model.

- [ ] **Step 1: Register RED crash points**

Add named failpoints before/after overlay publish, volatile reply, materializer dispatch, each striped member apply, overlay retirement, seal, metadata flush, local receipt, remote publish, watermark/cleanup, each metadata fence stage, and each shutdown phase.

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct BatchIdentity {
    pub(crate) mutation_incarnation: MutationIncarnation,
    pub(crate) sequence: u64,
}

pub(crate) struct CrashDurabilityFloor {
    pub(crate) mutation: MutationCutoff,
    pub(crate) object: ObjectCoverage,
    pub(crate) target: DurabilityTarget,
}

pub(crate) struct CanonicalMemberPrefix {
    pub(crate) batch: BatchIdentity,
    pub(crate) completed_members: u32,
    pub(crate) total_members: u32,
}

pub(crate) struct CrashDurabilityModel {
    pub(crate) volatile_acked_through: Option<MutationCutoff>,
    pub(crate) local_durable_floor: Option<CrashDurabilityFloor>,
    pub(crate) remote_durable_floor: Option<CrashDurabilityFloor>,
    pub(crate) striped_canonical_members:
        std::collections::BTreeMap<BatchIdentity, CanonicalMemberPrefix>,
}
```

- [ ] **Step 2: Encode the exact invariant**

Cutoffs are comparable only when their typed mutation or journal incarnation matches. A restart creates new incarnations; no raw sequence or map key can collide with an old process. Every mutation covered by `local_durable_floor.mutation` and `local_durable_floor.object` must recover completely with consistent data, attributes, quota, metadata, namespace, and all striped members. Above that typed floor, ordinary state may be absent; a striped NBD batch may expose only its `CanonicalMemberPrefix`. That prefix contains whole members in stripe order and may not create torn metadata, a premature logical completion/namespace claim, a resurrected unlink, or stale prepared attributes.

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
- Produces: `setup`, `run`, `cleanup --ledger`, `assert-clean --ledger`, `archive-control`, and `assert-source-idle` commands plus JSON receipts.
- Consumes: exact ZeroFS binary/config SHA, Ubuntu sudo, unused UUID-owned resources, disposable backend namespace, and real client binaries.

- [ ] **Step 1: Write dependency-free safety RED tests**

```python
import unittest

class ResourceLedgerTests(unittest.TestCase):
    def test_cleanup_rejects_non_uuid_root(self):
        ledger = ResourceLedger(
            run_id="not-a-uuid",
            control_root="/var/tmp/control",
            resource_root="/var/tmp/resources",
        )
        with self.assertRaises(UnsafeCleanupTarget):
            ledger.validate_cleanup_scope()
```

Name tests for rejecting `/`, `/mnt`, `/var/tmp`, equal/nested control and resource roots, workspace roots, CT198/production strings, unowned PIDs/devices/mounts/ports, missing dual ack flags, receipt omission, partial setup, primary-plus-cleanup errors, cleanup preserving ledger authority, repeated cleanup after resource-root deletion, final receipt archiving, and supervisor cancellation.

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

For UUID `RUN_UUID`, setup requires `CONTROL_ROOT=/var/tmp/zerofs-tiered-control-$RUN_UUID`, `RESOURCE_ROOT=/var/tmp/zerofs-tiered-resources-$RUN_UUID`, `LEDGER=$CONTROL_ROOT/ledger.json`, and `RECEIPT_ROOT=$CONTROL_ROOT/receipts`; control and resource roots must be disjoint siblings. Identity/config fields in the ledger are immutable and every later event is hash-chained append-only. The JSON receipt records both ack fields, source HEAD, binary/config hashes, both roots, exact PIDs, ports, devices, mounts, pool/filesystem names, backend prefix, tool checkout revisions, scenario, manifest, typed durability floors, terminal state, commands, exit status, and cleanup status. `cleanup --ledger PATH` removes only entries under `RESOURCE_ROOT` and succeeds after that root is already absent. `assert-clean --ledger PATH` continues to read the external ledger and fails if any recorded process/listener/mount/device/pool/prefix/resource path remains; it never requires the control root to be absent. `archive-control --ledger PATH --archive-root /fast/zerofs-tiered-receipts` copies ledger/receipts to `/fast/zerofs-tiered-receipts/$RUN_UUID`, verifies hashes, and only then removes `CONTROL_ROOT`. `assert-source-idle --source-root PATH` inspects `/proc/*/cwd` and fails for active `cargo`, `rustc`, test, or harness jobs rooted at PATH.

- [ ] **Step 3: Implement real shipping entry points**

NFS uses a hard NFSv3 kernel mount and actual WRITE/COMMIT; 9P uses both v9fs and native client plus actual Twrite/Tfsync; NBD uses `nbd-client`, a disposable device, XFS/ZFS, WRITE/FUA/FLUSH; RPC uses the actual Unix/TCP gRPC client/server; WebUI uses the real gRPC-Web/WebSocket/9P route and generated WASM client. Internal Rust calls may instrument failpoints but never replace these acceptance legs.

`linux_suites.py` creates tool checkouts only under `RESOURCE_ROOT/tools` and pins xfstests `1ae822c1c2e2364e966085cee3ce4a97b2500241`, pjdfstest `85a8aea9e685999ef0540392fd80535f873d7ff7`, and pjdfstest_nfs `7d3d7cb0cdc5d39eedd995771bc1d4b3dabf31ab`. Kernel scenarios download only `https://cdn.kernel.org/pub/linux/kernel/v6.x/linux-6.18.tar.xz` and require SHA-256 `9106a4605da9e31ff17659d958782b815f9591ab308d03b0ee21aad6c7dced4b` before extraction. These rules apply equally to focused C4 runs and C8's final scenario reruns.

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
- Receipt: UUID external control root outside Git

**Interfaces:**
- Produces: real simultaneous admission, pending-read, cross-adapter durability, and WebUI/RPC receipts.
- Consumes: exact pushed feature SHA and isolated Ubuntu checkout.

- [ ] **Step 1: Review, push, and synchronize the committed harness SHA**

```bash
cd /Volumes/bigssd/projects/ZeroFS/.worktrees/unified-tiered-writeback
test -z "$(git status --porcelain=v1)"
SLICE_SHA="$(git rev-parse HEAD^{commit})"; test "${#SLICE_SHA}" = 40
git show --stat --oneline "$SLICE_SHA"
git push origin "$SLICE_SHA:refs/heads/codex/unified-tiered-writeback"
test "$(git ls-remote origin refs/heads/codex/unified-tiered-writeback | awk '{print $1}')" = "$SLICE_SHA"
ssh ubuntu-main "cd /fast/projects/ZeroFS && git fetch origin codex/unified-tiered-writeback && test \"\$(git rev-parse origin/codex/unified-tiered-writeback)\" = '$SLICE_SHA'"
ssh ubuntu-main "test -d /fast/projects/ZeroFS-unified-tiered-writeback || (cd /fast/projects/ZeroFS && git worktree add --detach /fast/projects/ZeroFS-unified-tiered-writeback '$SLICE_SHA')"
ssh ubuntu-main "cd /fast/projects/ZeroFS-unified-tiered-writeback && test -z \"\$(git status --porcelain=v1)\" && python3 scripts/tiered-writeback-e2e.py assert-source-idle --source-root /fast/projects/ZeroFS-unified-tiered-writeback && git switch --detach '$SLICE_SHA' && test \"\$(git rev-parse HEAD)\" = '$SLICE_SHA'"
```

- [ ] **Step 2: Create external control authority and disposable resources**

On Ubuntu, record the expanded `SLICE_SHA` in the ledger:

```bash
cd /fast/projects/ZeroFS-unified-tiered-writeback
SLICE_SHA="$(git rev-parse origin/codex/unified-tiered-writeback^{commit})"
test "$(git rev-parse HEAD)" = "$SLICE_SHA"
RUN_UUID="$(python3 -c 'import uuid; print(uuid.uuid4())')"
CONTROL_ROOT="/var/tmp/zerofs-tiered-control-${RUN_UUID}"
RESOURCE_ROOT="/var/tmp/zerofs-tiered-resources-${RUN_UUID}"
LEDGER="${CONTROL_ROOT}/ledger.json"
sudo python3 scripts/tiered-writeback-e2e.py setup --ledger "$LEDGER" --control-root "$CONTROL_ROOT" --resource-root "$RESOURCE_ROOT" --source-sha "$SLICE_SHA" --filesystem-ack-mode volatile_memory --object-ack-mode memory
```

- [ ] **Step 3: Run the exact volatile shipping scenarios**

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

- [ ] **Step 4: Run materialized/object-target controls**

```bash
sudo python3 scripts/tiered-writeback-e2e.py cleanup --ledger "$LEDGER"
sudo python3 scripts/tiered-writeback-e2e.py cleanup --ledger "$LEDGER"
sudo python3 scripts/tiered-writeback-e2e.py assert-clean --ledger "$LEDGER"
RUN_UUID="$(python3 -c 'import uuid; print(uuid.uuid4())')"; CONTROL_ROOT="/var/tmp/zerofs-tiered-control-${RUN_UUID}"; RESOURCE_ROOT="/var/tmp/zerofs-tiered-resources-${RUN_UUID}"; LEDGER="${CONTROL_ROOT}/ledger.json"
sudo python3 scripts/tiered-writeback-e2e.py setup --ledger "$LEDGER" --control-root "$CONTROL_ROOT" --resource-root "$RESOURCE_ROOT" --source-sha "$SLICE_SHA" --filesystem-ack-mode materialized --object-ack-mode ssd
sudo python3 scripts/tiered-writeback-e2e.py run --ledger "$LEDGER" --filesystem-ack-mode materialized --object-ack-mode ssd --scenario protocol-materialized-control
sudo python3 scripts/tiered-writeback-e2e.py cleanup --ledger "$LEDGER"
sudo python3 scripts/tiered-writeback-e2e.py cleanup --ledger "$LEDGER"
sudo python3 scripts/tiered-writeback-e2e.py assert-clean --ledger "$LEDGER"
```

Run the remote object-ack control under a new matching ledger:

```bash
RUN_UUID="$(python3 -c 'import uuid; print(uuid.uuid4())')"; CONTROL_ROOT="/var/tmp/zerofs-tiered-control-${RUN_UUID}"; RESOURCE_ROOT="/var/tmp/zerofs-tiered-resources-${RUN_UUID}"; LEDGER="${CONTROL_ROOT}/ledger.json"
sudo python3 scripts/tiered-writeback-e2e.py setup --ledger "$LEDGER" --control-root "$CONTROL_ROOT" --resource-root "$RESOURCE_ROOT" --source-sha "$SLICE_SHA" --filesystem-ack-mode materialized --object-ack-mode remote
sudo python3 scripts/tiered-writeback-e2e.py run --ledger "$LEDGER" --filesystem-ack-mode materialized --object-ack-mode remote --scenario protocol-durability-target-control
sudo python3 scripts/tiered-writeback-e2e.py cleanup --ledger "$LEDGER"
sudo python3 scripts/tiered-writeback-e2e.py cleanup --ledger "$LEDGER"
sudo python3 scripts/tiered-writeback-e2e.py assert-clean --ledger "$LEDGER"
```

Ordinary object acknowledgement policy must not alter protocol barrier semantics.

---

### Task C4: Run Exact Ubuntu Filesystem Workflows

**Files:**
- Modify only after real failure: `scripts/tiered_writeback_e2e/linux_suites.py` or the owning production source
- Receipt: UUID external control roots outside Git

**Interfaces:**
- Produces: xfstests, pjdfstest, kernel compile, stress-ng, XFS-over-NBD, and ZFS-over-NBD receipts.
- Consumes: harness setup with both ack flags and ledger-generated paths/devices/names.

- [ ] **Step 1: Synchronize the literal reviewed SHA and create ledger-owned tool checkouts**

Run the required promotion block, then on Ubuntu create a new external control root and disposable resource root. Tool sources are never shared through `/tmp`:

```bash
cd /fast/projects/ZeroFS-unified-tiered-writeback
SLICE_SHA="$(git rev-parse origin/codex/unified-tiered-writeback^{commit})"
test "$(git rev-parse HEAD)" = "$SLICE_SHA"
RUN_UUID="$(python3 -c 'import uuid; print(uuid.uuid4())')"
CONTROL_ROOT="/var/tmp/zerofs-tiered-control-${RUN_UUID}"
RESOURCE_ROOT="/var/tmp/zerofs-tiered-resources-${RUN_UUID}"
LEDGER="${CONTROL_ROOT}/ledger.json"
sudo python3 scripts/tiered-writeback-e2e.py setup --ledger "$LEDGER" --control-root "$CONTROL_ROOT" --resource-root "$RESOURCE_ROOT" --source-sha "$SLICE_SHA" --filesystem-ack-mode volatile_memory --object-ack-mode ssd
git clone --no-checkout https://github.com/Barre/xfstests.git "$RESOURCE_ROOT/tools/xfstests"
git -C "$RESOURCE_ROOT/tools/xfstests" checkout --detach 1ae822c1c2e2364e966085cee3ce4a97b2500241
git clone --no-checkout https://github.com/pjd/pjdfstest.git "$RESOURCE_ROOT/tools/pjdfstest"
git -C "$RESOURCE_ROOT/tools/pjdfstest" checkout --detach 85a8aea9e685999ef0540392fd80535f873d7ff7
git clone --no-checkout https://github.com/Barre/pjdfstest_nfs.git "$RESOURCE_ROOT/tools/pjdfstest-nfs"
git -C "$RESOURCE_ROOT/tools/pjdfstest-nfs" checkout --detach 7d3d7cb0cdc5d39eedd995771bc1d4b3dabf31ab
test "$(git -C "$RESOURCE_ROOT/tools/xfstests" rev-parse HEAD)" = 1ae822c1c2e2364e966085cee3ce4a97b2500241
test "$(git -C "$RESOURCE_ROOT/tools/pjdfstest" rev-parse HEAD)" = 85a8aea9e685999ef0540392fd80535f873d7ff7
test "$(git -C "$RESOURCE_ROOT/tools/pjdfstest-nfs" rev-parse HEAD)" = 7d3d7cb0cdc5d39eedd995771bc1d4b3dabf31ab
make -C "$RESOURCE_ROOT/tools/xfstests" -j"$(nproc)"
make -C "$RESOURCE_ROOT/tools/pjdfstest" -j"$(nproc)"
(cd "$RESOURCE_ROOT/tools/pjdfstest-nfs" && autoreconf -fvi && ./configure && make -j"$(nproc)")
```

- [ ] **Step 2: Run NFS quick, 9P quick, and strict generic/732 xfstests**

`linux_suites.py` writes ledger-owned configs/excludes/results. The harness executes these exact commands:

```bash
cd "$RESOURCE_ROOT/tools/xfstests"
sudo env HOST_OPTIONS="$RESOURCE_ROOT/config/xfstests-nfs.config" RESULT_BASE="$RESOURCE_ROOT/results/xfstests-nfs" ./check -g quick -E "$RESOURCE_ROOT/config/xfstests-nfs.excludes"
sudo env HOST_OPTIONS="$RESOURCE_ROOT/config/xfstests-9p.config" RESULT_BASE="$RESOURCE_ROOT/results/xfstests-9p" ./check -g quick -E "$RESOURCE_ROOT/config/xfstests-9p.excludes"
sudo env HOST_OPTIONS="$RESOURCE_ROOT/config/xfstests-9p-strict.config" RESULT_BASE="$RESOURCE_ROOT/results/xfstests-9p-strict" ./check generic/732
sudo python3 /fast/projects/ZeroFS-unified-tiered-writeback/scripts/tiered-writeback-e2e.py run --ledger "$LEDGER" --filesystem-ack-mode volatile_memory --object-ack-mode ssd --scenario xfstests-nfs-quick
sudo python3 /fast/projects/ZeroFS-unified-tiered-writeback/scripts/tiered-writeback-e2e.py run --ledger "$LEDGER" --filesystem-ack-mode volatile_memory --object-ack-mode ssd --scenario xfstests-ninep-quick-and-strict
```

- [ ] **Step 3: Run both NFS and 9P pjdfstest plus stress-ng**

The NFS leg follows `.github/workflows/pjdfstest.yml`; the 9P leg follows `.github/workflows/pjdfstest-9p.yml`:

```bash
find "$RESOURCE_ROOT/tools/pjdfstest-nfs/tests" -name '*.t' -type f | sort > "$RESOURCE_ROOT/results/pjdfstest-nfs-all.txt"
grep -v -f /fast/projects/ZeroFS-unified-tiered-writeback/.github/.pjdfstest-nfs-exclude "$RESOURCE_ROOT/results/pjdfstest-nfs-all.txt" > "$RESOURCE_ROOT/results/pjdfstest-nfs-run.txt"
cd "$RESOURCE_ROOT/mounts/nfs/pjdfstest_test"
sudo prove -rv $(cat "$RESOURCE_ROOT/results/pjdfstest-nfs-run.txt")
find "$RESOURCE_ROOT/tools/pjdfstest/tests" -name '*.t' -type f | sort > "$RESOURCE_ROOT/results/pjdfstest-9p-all.txt"
grep -v -f /fast/projects/ZeroFS-unified-tiered-writeback/.github/.pjdfstest-9p-exclude "$RESOURCE_ROOT/results/pjdfstest-9p-all.txt" > "$RESOURCE_ROOT/results/pjdfstest-9p-run.txt"
cd "$RESOURCE_ROOT/mounts/ninep/pjdfstest_test"
sudo prove -rv $(cat "$RESOURCE_ROOT/results/pjdfstest-9p-run.txt")
sudo stress-ng --job /fast/projects/ZeroFS-unified-tiered-writeback/.github/stress-ng-filesystem.job
sudo python3 /fast/projects/ZeroFS-unified-tiered-writeback/scripts/tiered-writeback-e2e.py run --ledger "$LEDGER" --filesystem-ack-mode volatile_memory --object-ack-mode ssd --scenario pjdfstest-nfs
sudo python3 /fast/projects/ZeroFS-unified-tiered-writeback/scripts/tiered-writeback-e2e.py run --ledger "$LEDGER" --filesystem-ack-mode volatile_memory --object-ack-mode ssd --scenario pjdfstest-ninep
sudo python3 /fast/projects/ZeroFS-unified-tiered-writeback/scripts/tiered-writeback-e2e.py run --ledger "$LEDGER" --filesystem-ack-mode volatile_memory --object-ack-mode ssd --scenario stress-ng-nfs-ninep
```

- [ ] **Step 4: Compile the literal pinned kernel archive over NFS and 9P**

The archive is exactly `https://cdn.kernel.org/pub/linux/kernel/v6.x/linux-6.18.tar.xz` with SHA-256 `9106a4605da9e31ff17659d958782b815f9591ab308d03b0ee21aad6c7dced4b`:

```bash
curl --fail --location --retry 5 --output "$RESOURCE_ROOT/downloads/linux-6.18.tar.xz" https://cdn.kernel.org/pub/linux/kernel/v6.x/linux-6.18.tar.xz
printf '%s  %s\n' 9106a4605da9e31ff17659d958782b815f9591ab308d03b0ee21aad6c7dced4b "$RESOURCE_ROOT/downloads/linux-6.18.tar.xz" | sha256sum --check
tar -C "$RESOURCE_ROOT/mounts/nfs" -xf "$RESOURCE_ROOT/downloads/linux-6.18.tar.xz"
tar -C "$RESOURCE_ROOT/mounts/ninep" -xf "$RESOURCE_ROOT/downloads/linux-6.18.tar.xz"
make -C "$RESOURCE_ROOT/mounts/nfs/linux-6.18" O="$RESOURCE_ROOT/mounts/nfs/linux-6.18-build" tinyconfig
make -C "$RESOURCE_ROOT/mounts/nfs/linux-6.18" O="$RESOURCE_ROOT/mounts/nfs/linux-6.18-build" -j"$(nproc)" vmlinux
test -s "$RESOURCE_ROOT/mounts/nfs/linux-6.18-build/vmlinux"
make -C "$RESOURCE_ROOT/mounts/ninep/linux-6.18" O="$RESOURCE_ROOT/mounts/ninep/linux-6.18-build" tinyconfig
make -C "$RESOURCE_ROOT/mounts/ninep/linux-6.18" O="$RESOURCE_ROOT/mounts/ninep/linux-6.18-build" -j"$(nproc)" vmlinux
test -s "$RESOURCE_ROOT/mounts/ninep/linux-6.18-build/vmlinux"
sudo python3 /fast/projects/ZeroFS-unified-tiered-writeback/scripts/tiered-writeback-e2e.py run --ledger "$LEDGER" --filesystem-ack-mode volatile_memory --object-ack-mode ssd --scenario kernel-compile-nfs
sudo python3 /fast/projects/ZeroFS-unified-tiered-writeback/scripts/tiered-writeback-e2e.py run --ledger "$LEDGER" --filesystem-ack-mode volatile_memory --object-ack-mode ssd --scenario kernel-compile-ninep
sudo python3 /fast/projects/ZeroFS-unified-tiered-writeback/scripts/tiered-writeback-e2e.py cleanup --ledger "$LEDGER"
sudo python3 /fast/projects/ZeroFS-unified-tiered-writeback/scripts/tiered-writeback-e2e.py cleanup --ledger "$LEDGER"
sudo python3 /fast/projects/ZeroFS-unified-tiered-writeback/scripts/tiered-writeback-e2e.py assert-clean --ledger "$LEDGER"
```

- [ ] **Step 5: Run XFS-over-NBD from a fresh setup/cleanup ledger**

```bash
RUN_UUID="$(python3 -c 'import uuid; print(uuid.uuid4())')"; CONTROL_ROOT="/var/tmp/zerofs-tiered-control-${RUN_UUID}"; RESOURCE_ROOT="/var/tmp/zerofs-tiered-resources-${RUN_UUID}"; LEDGER="${CONTROL_ROOT}/ledger.json"
sudo python3 scripts/tiered-writeback-e2e.py setup --ledger "$LEDGER" --control-root "$CONTROL_ROOT" --resource-root "$RESOURCE_ROOT" --source-sha "$SLICE_SHA" --filesystem-ack-mode volatile_memory --object-ack-mode ssd
sudo python3 scripts/tiered-writeback-e2e.py run --ledger "$LEDGER" --filesystem-ack-mode volatile_memory --object-ack-mode ssd --scenario xfs-over-nbd-restart
sudo python3 scripts/tiered-writeback-e2e.py cleanup --ledger "$LEDGER"
sudo python3 scripts/tiered-writeback-e2e.py cleanup --ledger "$LEDGER"
sudo python3 scripts/tiered-writeback-e2e.py assert-clean --ledger "$LEDGER"
```

- [ ] **Step 6: Run ZFS-over-NBD from a different fresh setup/cleanup ledger**

```bash
RUN_UUID="$(python3 -c 'import uuid; print(uuid.uuid4())')"; CONTROL_ROOT="/var/tmp/zerofs-tiered-control-${RUN_UUID}"; RESOURCE_ROOT="/var/tmp/zerofs-tiered-resources-${RUN_UUID}"; LEDGER="${CONTROL_ROOT}/ledger.json"
sudo python3 scripts/tiered-writeback-e2e.py setup --ledger "$LEDGER" --control-root "$CONTROL_ROOT" --resource-root "$RESOURCE_ROOT" --source-sha "$SLICE_SHA" --filesystem-ack-mode volatile_memory --object-ack-mode ssd
sudo python3 scripts/tiered-writeback-e2e.py run --ledger "$LEDGER" --filesystem-ack-mode volatile_memory --object-ack-mode ssd --scenario zfs-over-nbd-restart
sudo python3 scripts/tiered-writeback-e2e.py cleanup --ledger "$LEDGER"
sudo python3 scripts/tiered-writeback-e2e.py cleanup --ledger "$LEDGER"
sudo python3 scripts/tiered-writeback-e2e.py assert-clean --ledger "$LEDGER"
```

No command in this task runs on macOS.

---

### Task C5: Add One Tiered CI Leg Per Existing Linux Workflow

**Files:**
- Modify: `.github/workflows/xfstests-nfs.yml`
- Modify: `.github/workflows/xfstests-9p.yml`
- Modify: `.github/workflows/pjdfstest.yml`
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

Use `unittest` to parse workflow text and require both ack flags, disjoint UUID control/resource roots, the ledger under the control root, `cleanup --ledger` under `always()`, repeated cleanup, `assert-clean --ledger`, unchanged materialized control, runner-owned device checks, and no CT198/production target.

- [ ] **Step 2: Add minimal matrix legs and validate**

```bash
cd /Volumes/bigssd/projects/ZeroFS/.worktrees/unified-tiered-writeback
actionlint .github/workflows/*.yml
python3 -m unittest discover -s scripts/tests -p 'test_*.py' -v
git diff --check
git add .github/workflows/xfstests-nfs.yml .github/workflows/xfstests-9p.yml .github/workflows/pjdfstest.yml .github/workflows/pjdfstest-9p.yml .github/workflows/kernel-compile-nfs.yml .github/workflows/kernel-compile-9p.yml .github/workflows/stress-ng.yml .github/workflows/zfs-test.yml .github/workflows/xfs-nbd.yml scripts/tests/test_tiered_writeback_e2e.py
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

- [ ] **Step 2: Run RED before implementation**

```bash
cd /Volumes/bigssd/projects/ZeroFS/.worktrees/unified-tiered-writeback/zerofs
cargo test -p zerofs --test writeback_recovery --locked -- --nocapture
cargo test -p zerofs --test writeback_faults --locked -- --nocapture
cd ..
python3 -m unittest discover -s scripts/tests -p 'test_*.py' -v
```

Expected RED: the new typed crash assertions, supervisor-cancellation cleanup, or named crash modes are absent.

- [ ] **Step 3: Implement the crash harness and deterministic regressions**

Add typed-incarnation assertions to both Rust test targets, implement exact failpoint/process-stop boundaries in `crash.py`, and add standard-library unit tests that verify each mode records both ack flags, literal source SHA, external ledger authority, typed floors, primary failure, and cleanup outcome. The supervisor-cancellation test interrupts setup, workload, restart, and cleanup independently and then proves repeated cleanup plus `assert-clean` from the surviving control root.

- [ ] **Step 4: Run portable GREEN**

```bash
cd /Volumes/bigssd/projects/ZeroFS/.worktrees/unified-tiered-writeback/zerofs
cargo test -p zerofs --test writeback_recovery --locked -- --nocapture
cargo test -p zerofs --test writeback_faults --locked -- --nocapture
cd ..
python3 -m compileall -q scripts/tiered_writeback_e2e scripts/tiered-writeback-e2e.py
python3 -m unittest discover -s scripts/tests -p 'test_*.py' -v
git diff --check
```

- [ ] **Step 5: Commit exact files and review the commit**

```bash
cd /Volumes/bigssd/projects/ZeroFS/.worktrees/unified-tiered-writeback
git add zerofs/tests/writeback_recovery.rs zerofs/tests/writeback_faults.rs scripts/tiered_writeback_e2e/crash.py scripts/tests/test_tiered_writeback_e2e.py
git commit -m "test: prove tiered writeback crash recovery"
CRASH_SHA="$(git rev-parse HEAD^{commit})"; test "${#CRASH_SHA}" = 40
test "$(git diff-tree --no-commit-id --name-only -r "$CRASH_SHA" | sort | tr '\n' ' ')" = "scripts/tests/test_tiered_writeback_e2e.py scripts/tiered_writeback_e2e/crash.py zerofs/tests/writeback_faults.rs zerofs/tests/writeback_recovery.rs "
git show --check "$CRASH_SHA"
test -z "$(git status --porcelain=v1)"
```

- [ ] **Step 6: Push and fail-closed synchronize the literal crash SHA**

```bash
cd /Volumes/bigssd/projects/ZeroFS/.worktrees/unified-tiered-writeback
git push origin "$CRASH_SHA:refs/heads/codex/unified-tiered-writeback"
test "$(git ls-remote origin refs/heads/codex/unified-tiered-writeback | awk '{print $1}')" = "$CRASH_SHA"
ssh ubuntu-main "cd /fast/projects/ZeroFS && git fetch origin codex/unified-tiered-writeback && test \"\$(git rev-parse origin/codex/unified-tiered-writeback)\" = '$CRASH_SHA'"
ssh ubuntu-main "cd /fast/projects/ZeroFS-unified-tiered-writeback && test -z \"\$(git status --porcelain=v1)\" && python3 scripts/tiered-writeback-e2e.py assert-source-idle --source-root /fast/projects/ZeroFS-unified-tiered-writeback && git switch --detach '$CRASH_SHA' && test \"\$(git rev-parse HEAD)\" = '$CRASH_SHA'"
```

- [ ] **Step 7: Run real Ubuntu proof only from `CRASH_SHA`**

Each invocation below creates its own external control root and disposable resource root:

```bash
cd /fast/projects/ZeroFS-unified-tiered-writeback
CRASH_SHA="$(git rev-parse origin/codex/unified-tiered-writeback^{commit})"
test "$(git rev-parse HEAD)" = "$CRASH_SHA"
run_crash_scenario() {
  object_mode="$1"; scenario="$2"
  RUN_UUID="$(python3 -c 'import uuid; print(uuid.uuid4())')"
  CONTROL_ROOT="/var/tmp/zerofs-tiered-control-${RUN_UUID}"
  RESOURCE_ROOT="/var/tmp/zerofs-tiered-resources-${RUN_UUID}"
  LEDGER="${CONTROL_ROOT}/ledger.json"
  sudo python3 scripts/tiered-writeback-e2e.py setup --ledger "$LEDGER" --control-root "$CONTROL_ROOT" --resource-root "$RESOURCE_ROOT" --source-sha "$CRASH_SHA" --filesystem-ack-mode volatile_memory --object-ack-mode "$object_mode"
  sudo python3 scripts/tiered-writeback-e2e.py run --ledger "$LEDGER" --filesystem-ack-mode volatile_memory --object-ack-mode "$object_mode" --scenario "$scenario"
  sudo python3 scripts/tiered-writeback-e2e.py cleanup --ledger "$LEDGER"
  sudo python3 scripts/tiered-writeback-e2e.py cleanup --ledger "$LEDGER"
  sudo python3 scripts/tiered-writeback-e2e.py assert-clean --ledger "$LEDGER"
}
run_crash_scenario memory crash-boundary-matrix
run_crash_scenario ssd local-receipt-restart
run_crash_scenario remote remote-receipt-clean-cache-restart
run_crash_scenario ssd terminal-fanout-and-shutdown-timeout
cd zerofs
cargo test -p zerofs --test failover_e2e --locked -- --list --ignored | tee "$CONTROL_ROOT/failover-tests.list"
test "$(grep -Ec ': test$' "$CONTROL_ROOT/failover-tests.list")" -gt 0
cargo test -p zerofs --test failover_e2e --locked -- --ignored --nocapture 2>&1 | tee "$CONTROL_ROOT/failover-tests.run"
grep -Eq 'test result: ok\. [1-9][0-9]* passed' "$CONTROL_ROOT/failover-tests.run"
```

Any Linux-discovered defect starts a new RED/GREEN corrective commit. It must be reviewed, pushed, and synchronized to its new literal 40-hex SHA by Step 6 before any Linux proof command is rerun; no dirty Ubuntu checkout is patched in place.

---

### Task C7: Benchmark Each Durability Boundary From Ledger Scratch

**Files:**
- Create: `scripts/benchmarking/__init__.py`
- Create: `scripts/benchmarking/read_matrix.py`
- Create: `scripts/benchmarking/raw_sftp.py`
- Create: `scripts/tiered_writeback_e2e/read_benchmark.py`
- Modify: `scripts/tiered_writeback_e2e/protocols.py`
- Modify: `scripts/tiered_writeback_e2e/integrity.py`
- Modify: `scripts/tiered_writeback_e2e/resources.py`
- Modify: `scripts/tiered-writeback-e2e.py`
- Modify: `scripts/vm100_pilot/real_world_matrix.py`
- Modify: `scripts/vm100_pilot/raw_sftp.py`
- Modify: `scripts/tests/test_real_world_matrix.py`
- Modify: `scripts/tests/test_vm100_pilot.py`
- Modify: `scripts/tests/test_tiered_writeback_e2e.py`
- Create: `scripts/tests/test_read_throughput_benchmark.py`
- Modify only after a real RED: production read instrumentation owning the observed failure
- Receipt: UUID Ubuntu external control root outside Git

**Interfaces:**
- Produces: separate foreground RAM-ack, local SSD cutoff, paced remote-drain, remote flush, remote-cold read, clean-SSD read, and clean-RAM read throughput/latency with integrity.
- Consumes: ledger `local_ssd_scratch`, incompressible payloads, disposable backend, exact metrics, both ack flags, kernel NFSv3, native 9P, `nbd-client`/XFS, and OpenSSH SFTP.

- [ ] **Step 1: Review, push, and synchronize the latest literal SHA**

Run the required promotion block after the most recent committed/reviewed slice. On Ubuntu, prove the isolated checkout is clean and exactly equal to expanded `SLICE_SHA` before allocating benchmark resources.

- [ ] **Step 2: Create external control authority and derive owned scratch**

```bash
cd /fast/projects/ZeroFS-unified-tiered-writeback
SLICE_SHA="$(git rev-parse origin/codex/unified-tiered-writeback^{commit})"
test "$(git rev-parse HEAD)" = "$SLICE_SHA"
RUN_UUID="$(python3 -c 'import uuid; print(uuid.uuid4())')"
CONTROL_ROOT="/var/tmp/zerofs-tiered-control-${RUN_UUID}"
RESOURCE_ROOT="/var/tmp/zerofs-tiered-resources-${RUN_UUID}"
LEDGER="${CONTROL_ROOT}/ledger.json"
sudo python3 scripts/tiered-writeback-e2e.py setup --ledger "$LEDGER" --control-root "$CONTROL_ROOT" --resource-root "$RESOURCE_ROOT" --source-sha "$SLICE_SHA" --filesystem-ack-mode volatile_memory --object-ack-mode ssd
BENCH_SCRATCH="$(python3 scripts/tiered-writeback-e2e.py ledger-value --ledger "$LEDGER" --key local_ssd_scratch)"
python3 scripts/tiered-writeback-e2e.py validate-owned-path --ledger "$LEDGER" --path "$BENCH_SCRATCH"
cd zerofs
```

- [ ] **Step 3: List exact ignored benchmarks and reject zero selection**

```bash
cargo test --release -p zerofs --lib --locked -- --list | tee "$CONTROL_ROOT/rust-bench-tests.list"
grep -F 'writeback::store::tests::bench_writeback_tier_profile' "$CONTROL_ROOT/rust-bench-tests.list"
grep -F 'writeback::journaler::tests::drain_throughput_of_the_post_ack_durability_tail' "$CONTROL_ROOT/rust-bench-tests.list"
grep -F 'writeback::store::tests::bench_remote_replay_throughput_against_throttled_backend' "$CONTROL_ROOT/rust-bench-tests.list"
grep -F 'writeback::journal::tests::publication_batch_size_amortizes_the_journal_fixed_cost' "$CONTROL_ROOT/rust-bench-tests.list"
grep -F 'writeback::journal::tests::remote_commit_serialization_cost_bounds_replay_throughput' "$CONTROL_ROOT/rust-bench-tests.list"
```

- [ ] **Step 4: Run every exact benchmark name**

```bash
ZEROFS_BENCH_DIR="$BENCH_SCRATCH/tier-profile" cargo test --release -p zerofs --lib --locked writeback::store::tests::bench_writeback_tier_profile -- --ignored --exact --nocapture 2>&1 | tee "$CONTROL_ROOT/bench-tier-profile.run"
grep -Eq 'test result: ok\. 1 passed' "$CONTROL_ROOT/bench-tier-profile.run"
ZEROFS_BENCH_DIR="$BENCH_SCRATCH/drain" cargo test --release -p zerofs --lib --locked writeback::journaler::tests::drain_throughput_of_the_post_ack_durability_tail -- --ignored --exact --nocapture 2>&1 | tee "$CONTROL_ROOT/bench-drain.run"
grep -Eq 'test result: ok\. 1 passed' "$CONTROL_ROOT/bench-drain.run"
ZEROFS_BENCH_DIR="$BENCH_SCRATCH/remote-replay" cargo test --release -p zerofs --lib --locked writeback::store::tests::bench_remote_replay_throughput_against_throttled_backend -- --ignored --exact --nocapture 2>&1 | tee "$CONTROL_ROOT/bench-remote-replay.run"
grep -Eq 'test result: ok\. 1 passed' "$CONTROL_ROOT/bench-remote-replay.run"
ZEROFS_BENCH_DIR="$BENCH_SCRATCH/publication-batch" cargo test --release -p zerofs --lib --locked writeback::journal::tests::publication_batch_size_amortizes_the_journal_fixed_cost -- --ignored --exact --nocapture 2>&1 | tee "$CONTROL_ROOT/bench-publication-batch.run"
grep -Eq 'test result: ok\. 1 passed' "$CONTROL_ROOT/bench-publication-batch.run"
ZEROFS_BENCH_DIR="$BENCH_SCRATCH/remote-commit" cargo test --release -p zerofs --lib --locked writeback::journal::tests::remote_commit_serialization_cost_bounds_replay_throughput -- --ignored --exact --nocapture 2>&1 | tee "$CONTROL_ROOT/bench-remote-commit.run"
grep -Eq 'test result: ok\. 1 passed' "$CONTROL_ROOT/bench-remote-commit.run"
cd /fast/projects/ZeroFS-unified-tiered-writeback
sudo python3 scripts/tiered-writeback-e2e.py cleanup --ledger "$LEDGER"
sudo python3 scripts/tiered-writeback-e2e.py cleanup --ledger "$LEDGER"
sudo python3 scripts/tiered-writeback-e2e.py assert-clean --ledger "$LEDGER"
```

Each command must report exactly one executed test; any zero-test or multi-test receipt is rejected.

- [ ] **Step 5: Run real protocol benchmarks and integrity gates**

Each benchmark command uses a separate ledger whose `setup` command has the same two ack flags; cleanup twice and `assert-clean` complete before the next ledger is created.

```bash
cd /fast/projects/ZeroFS-unified-tiered-writeback
SLICE_SHA="$(git rev-parse origin/codex/unified-tiered-writeback^{commit})"
run_benchmark_scenario() {
  object_mode="$1"; scenario="$2"
  RUN_UUID="$(python3 -c 'import uuid; print(uuid.uuid4())')"
  CONTROL_ROOT="/var/tmp/zerofs-tiered-control-${RUN_UUID}"
  RESOURCE_ROOT="/var/tmp/zerofs-tiered-resources-${RUN_UUID}"
  LEDGER="${CONTROL_ROOT}/ledger.json"
  sudo python3 scripts/tiered-writeback-e2e.py setup --ledger "$LEDGER" --control-root "$CONTROL_ROOT" --resource-root "$RESOURCE_ROOT" --source-sha "$SLICE_SHA" --filesystem-ack-mode volatile_memory --object-ack-mode "$object_mode"
  sudo python3 scripts/tiered-writeback-e2e.py run --ledger "$LEDGER" --filesystem-ack-mode volatile_memory --object-ack-mode "$object_mode" --scenario "$scenario"
  sudo python3 scripts/tiered-writeback-e2e.py cleanup --ledger "$LEDGER"
  sudo python3 scripts/tiered-writeback-e2e.py cleanup --ledger "$LEDGER"
  sudo python3 scripts/tiered-writeback-e2e.py assert-clean --ledger "$LEDGER"
}
run_benchmark_scenario memory benchmark-ram-ack
run_benchmark_scenario ssd benchmark-local-ssd
run_benchmark_scenario remote benchmark-paced-remote
```

Every receipt includes size/SHA-256/readback, mutation/object floors, dirty tiers, terminal state, CPU/RAM/local allocation, remote rate, and cleanup. Performance without integrity and durability is rejected.

- [ ] **Step 6: Refactor the maintained read primitives and add RED gates**

Move only protocol-neutral strict pieces from `vm100_pilot/real_world_matrix.py` and `raw_sftp.py` into `scripts/benchmarking`: immutable cell definitions, fio argv/result parsing, exact byte/request validation, SHA-256 manifest parsing, pinned SFTP command construction, and worker reaping. Keep the legacy VM100 commands consuming those same primitives so historical callers do not fork behavior. The UUID-ledgered tiered harness owns lifecycle, protocols, cache state, receipts, and cleanup.

Name dependency-free RED tests that reject:

- missing `benchmark-read-throughput` scenario;
- a zero-cell or incomplete protocol/cache/concurrency matrix;
- a “remote-cold” claim based only on fio `--invalidate=1`;
- a fresh fixture that was hashed or read immediately before a scored cold cell without a new server/cache incarnation;
- SSD or RAM cells that fetch from a lower tier;
- missing exact byte/request/hash/durability/cache/session/lane receipts;
- short or corrupted reads;
- internal/mock adapters substituted for kernel NFS, native 9P, `nbd-client`, or OpenSSH SFTP;
- unexplained one-lane execution, request explosion, dirty state, or incomplete cleanup being counted as a performance pass.
- failure-path NFS BDI cleanup that does not restore the exact recorded original value.

```bash
cd /Volumes/bigssd/projects/ZeroFS/.worktrees/unified-tiered-writeback
set -o pipefail
READ_TEST_LOG="${TMPDIR:-/tmp}/zerofs-read-throughput-red.log"
if python3 -m unittest discover -s scripts/tests -p 'test_read_throughput_benchmark.py' -v 2>&1 | tee "$READ_TEST_LOG"; then
  echo 'expected read-throughput RED suite to fail before implementation' >&2
  exit 1
fi
grep -Eq '^Ran [1-9][0-9]* tests? in ' "$READ_TEST_LOG"
grep -F 'test_missing_benchmark_read_throughput_scenario_is_rejected' "$READ_TEST_LOG"
grep -F 'test_incomplete_protocol_cache_concurrency_matrix_is_rejected' "$READ_TEST_LOG"
grep -F 'test_client_page_cache_cannot_masquerade_as_server_ram' "$READ_TEST_LOG"
grep -F 'test_failed_nfs_readahead_cell_restores_recorded_original' "$READ_TEST_LOG"
```

Expected RED: the scenario and shared benchmark module do not exist.

- [ ] **Step 7: Implement the exact cache-state-matched read matrix**

For configured usable read-session ceiling `P`, sweep concurrency
`{1, ceil(P/2), P, P+1}`. Each concurrency executes ten cells over one frozen
incompressible fixture manifest. The raw SFTP cell is a paired directional backend
control using the same fixture size/order/time window; only ZeroFS process/cache tiers
receive remote-cold/clean-SSD/clean-RAM labels because the harness cannot purge or
truthfully label the Storage Box provider's internal cache.

1. raw OpenSSH SFTP download control;
2. kernel NFSv3 remote-cold, clean-SSD, and clean-RAM;
3. native shipping 9P remote-cold, clean-SSD, and clean-RAM;
4. `nbd-client` plus disposable XFS direct-read remote-cold, clean-SSD, and clean-RAM.

For the historical seven usable read sessions this is `1,4,7,8`. The harness calibrates fixture size outside scored windows, reaches remote durability, verifies restart survival, then counterbalances cell order. It records:

- NFS negotiated `rsize`, READ RPC bytes/count/latency/outstanding depth, and BDI readahead;
- 9P negotiated `msize`, Tread count, and live sessions;
- NBD fio requests and `/sys/block/nbd*/stat`;
- extent count, coalesced/remote run count, unique segments, active/peak backend read lanes, requested/fetched bytes, SFTP session waits, retries/timeouts, CPU/RAM, local-device I/O, and network bytes.

For the UUID-isolated NFS mount only, run a diagnostic readahead A/B at the recorded
kernel default and a larger value derived from the negotiated `rsize`/measured latency.
Ledger the exact BDI path and original value, restore it in unconditional cleanup, and
make both `cleanup` and `assert-clean` reread that exact sysfs path and require byte-for-byte equality with the ledgered original. The dedicated failure-path test kills a scored cell after changing readahead and proves cleanup still restores it. Never mutate VM100's production NFS BDI. This A/B distinguishes client RPC depth from
shared ZeroFS run serialization; it does not silently turn a host sysctl into the
product fix.

Cache state must be proven exactly:

- **remote-cold:** fresh UUID process/cache/state roots, zero RAM/SSD clean-cache coverage before the scored read, and positive remote payload reads;
- **clean-SSD:** populate cold once, prove dirty tiers zero, restart to clear RAM while preserving SSD cache, then require positive local-cache reads and zero remote payload reads;
- **clean-RAM:** warm the exact server ranges, invalidate the isolated Linux client cache after the warmup, then require a positive exact NFS READ-byte, 9P Tread-byte, or NBD/server-logical-byte delta for the scored cell while local-device and remote payload reads remain zero. A zero protocol/server-byte delta is a client-cache hit and fails classification.

`fio --invalidate=1` is recorded only as Linux client-page-cache invalidation and never as proof of a ZeroFS-cold server.

- [ ] **Step 8: Use paired control-derived acceptance and commit the exact benchmark fence**

Run repeated materialized A/A controls to derive a log-throughput/latency noise band from median absolute paired difference plus `3 × MAD`. Volatile-mode median throughput, p95 latency, and requests per logical GiB may not regress beyond their paired materialized band. Before an evidenced CPU/link/SSD/session ceiling, concurrency may not reduce aggregate throughput outside that band. Active read lanes must rise with independent demand until the configured pool or evidenced extent-run fanout is reached; an unexplained single lane fails. An unstable control is inconclusive and must rerun.

Every cell additionally requires exact bytes, protocol-visible SHA-256, unchanged typed durability floors, zero dirty tiers/terminal errors/leaked reservations/temp objects, and idempotent ledger cleanup. Run only on isolated Ubuntu resources at the exact pushed SHA.

```bash
cd /Volumes/bigssd/projects/ZeroFS/.worktrees/unified-tiered-writeback
python3 -m compileall -q scripts/benchmarking scripts/tiered_writeback_e2e scripts/vm100_pilot scripts/tiered-writeback-e2e.py scripts/vm100-pilot.py
set -o pipefail
READ_TEST_LOG="${TMPDIR:-/tmp}/zerofs-read-throughput-green.log"
python3 -m unittest discover -s scripts/tests -p 'test_read_throughput_benchmark.py' -v 2>&1 | tee "$READ_TEST_LOG"
grep -Eq '^Ran [1-9][0-9]* tests? in ' "$READ_TEST_LOG"
grep -F 'test_missing_benchmark_read_throughput_scenario_is_rejected' "$READ_TEST_LOG"
grep -F 'test_incomplete_protocol_cache_concurrency_matrix_is_rejected' "$READ_TEST_LOG"
grep -F 'test_client_page_cache_cannot_masquerade_as_server_ram' "$READ_TEST_LOG"
grep -F 'test_failed_nfs_readahead_cell_restores_recorded_original' "$READ_TEST_LOG"
python3 -m unittest discover -s scripts/tests -p 'test_*.py' -v
git diff --check
git add scripts/benchmarking/__init__.py scripts/benchmarking/read_matrix.py scripts/benchmarking/raw_sftp.py scripts/tiered_writeback_e2e/read_benchmark.py scripts/tiered_writeback_e2e/protocols.py scripts/tiered_writeback_e2e/integrity.py scripts/tiered_writeback_e2e/resources.py scripts/tiered-writeback-e2e.py scripts/vm100_pilot/real_world_matrix.py scripts/vm100_pilot/raw_sftp.py scripts/tests/test_real_world_matrix.py scripts/tests/test_vm100_pilot.py scripts/tests/test_tiered_writeback_e2e.py scripts/tests/test_read_throughput_benchmark.py
git commit -m "bench: compare cache-matched protocol reads"
```

Do not run a post-commit read benchmark from C7: its setup ledgers have already been cleaned and it owns no surviving materialized-control receipt. C8 is the sole execution authority for the paired read benchmark and creates fresh control and candidate ledgers after review, push, and literal-SHA synchronization.

The existing historical NBD/raw-SFTP scripts remain developer controls, not final acceptance authority. No previously reported NFS/9P number is accepted unless reproduced by this committed ledgered runner.

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
cargo test -p zerofs --features webui --locked -- --list | tee /tmp/zerofs-final-wasm-smoke.list
grep -Fx 'webui::tests::wasm_client_smoke: test' /tmp/zerofs-final-wasm-smoke.list
cargo test -p zerofs --features webui webui::tests::wasm_client_smoke --locked -- --ignored --exact --nocapture 2>&1 | tee /tmp/zerofs-final-wasm-smoke.run
grep -Eq 'test result: ok\. 1 passed' /tmp/zerofs-final-wasm-smoke.run
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

After Step 1 and all review fixes are committed, run the required promotion block and record `FINAL_PROOF_SHA=$SLICE_SHA`. Then execute on Ubuntu:

```bash
cd /fast/projects/ZeroFS-unified-tiered-writeback
FINAL_PROOF_SHA="$(git rev-parse origin/codex/unified-tiered-writeback^{commit})"
test "$(git rev-parse HEAD)" = "$FINAL_PROOF_SHA"
test -z "$(git status --porcelain=v1)"
cd zerofs
cargo build --workspace --locked
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo test --workspace --all-targets --locked
cargo clippy -p zerofs --features failpoints --tests --locked -- -D warnings
cargo test -p zerofs --features failpoints --test failpoints --locked -- --nocapture
RUSTFLAGS="--cfg dst --cfg tokio_unstable --cfg io_uring_skip_arch_check" cargo clippy -p zerofs --features failpoints --test dst --locked -- -D warnings
RUSTFLAGS="--cfg dst --cfg tokio_unstable --cfg io_uring_skip_arch_check" cargo test -p zerofs --features failpoints --test dst --locked -- --nocapture
cargo test -p zerofs --test failover_e2e --locked -- --list --ignored | tee /tmp/zerofs-final-failover.list
test "$(grep -Ec ': test$' /tmp/zerofs-final-failover.list)" -gt 0
cargo test -p zerofs --test failover_e2e --locked -- --ignored --nocapture 2>&1 | tee /tmp/zerofs-final-failover.run
grep -Eq 'test result: ok\. [1-9][0-9]* passed' /tmp/zerofs-final-failover.run
cargo test -p zerofs-client --all-features --locked
cargo check -p ninep-client --target wasm32-unknown-unknown --locked
cd ..
python3 -m compileall -q scripts proxmox
python3 -m unittest discover -s scripts/tests -p 'test_*.py' -v
python3 -m unittest discover -s proxmox/tests -p 'test_*.py' -v
find proxmox -type f -name '*.sh' -exec shellcheck {} +
actionlint .github/workflows/*.yml
```

Run every real harness scenario from its own external control/resource roots:

```bash
cd /fast/projects/ZeroFS-unified-tiered-writeback
run_final_scenario() {
  filesystem_mode="$1"; object_mode="$2"; scenario="$3"
  RUN_UUID="$(python3 -c 'import uuid; print(uuid.uuid4())')"
  CONTROL_ROOT="/var/tmp/zerofs-tiered-control-${RUN_UUID}"
  RESOURCE_ROOT="/var/tmp/zerofs-tiered-resources-${RUN_UUID}"
  LEDGER="${CONTROL_ROOT}/ledger.json"
  sudo python3 scripts/tiered-writeback-e2e.py setup --ledger "$LEDGER" --control-root "$CONTROL_ROOT" --resource-root "$RESOURCE_ROOT" --source-sha "$FINAL_PROOF_SHA" --filesystem-ack-mode "$filesystem_mode" --object-ack-mode "$object_mode"
  sudo python3 scripts/tiered-writeback-e2e.py run --ledger "$LEDGER" --filesystem-ack-mode "$filesystem_mode" --object-ack-mode "$object_mode" --scenario "$scenario"
  sudo python3 scripts/tiered-writeback-e2e.py cleanup --ledger "$LEDGER"
  sudo python3 scripts/tiered-writeback-e2e.py cleanup --ledger "$LEDGER"
  sudo python3 scripts/tiered-writeback-e2e.py assert-clean --ledger "$LEDGER"
}
run_final_scenario volatile_memory memory global-admission-nbd-nfs-ninep
run_final_scenario volatile_memory memory cross-adapter-pending-read-same-backing-inode
run_final_scenario volatile_memory memory nfs-commit-covers-prior-nbd
run_final_scenario volatile_memory memory ninep-fsync-covers-prior-nfs
run_final_scenario volatile_memory memory nbd-flush-covers-prior-ninep
run_final_scenario volatile_memory memory webui-rpc-production-path
run_final_scenario materialized ssd protocol-materialized-control
run_final_scenario materialized remote protocol-durability-target-control
run_final_scenario volatile_memory ssd xfstests-nfs-quick
run_final_scenario volatile_memory ssd xfstests-ninep-quick-and-strict
run_final_scenario volatile_memory ssd pjdfstest-nfs
run_final_scenario volatile_memory ssd pjdfstest-ninep
run_final_scenario volatile_memory ssd stress-ng-nfs-ninep
run_final_scenario volatile_memory ssd kernel-compile-nfs
run_final_scenario volatile_memory ssd kernel-compile-ninep
run_final_scenario volatile_memory ssd xfs-over-nbd-restart
run_final_scenario volatile_memory ssd zfs-over-nbd-restart
run_final_scenario volatile_memory memory crash-boundary-matrix
run_final_scenario volatile_memory ssd local-receipt-restart
run_final_scenario volatile_memory remote remote-receipt-clean-cache-restart
run_final_scenario volatile_memory ssd terminal-fanout-and-shutdown-timeout
run_final_scenario volatile_memory memory benchmark-ram-ack
run_final_scenario volatile_memory ssd benchmark-local-ssd
run_final_scenario volatile_memory remote benchmark-paced-remote
```

Run the paired read benchmark separately because the candidate must consume the
immutable materialized-control receipt:

```bash
READ_CONTROL_UUID="$(python3 -c 'import uuid; print(uuid.uuid4())')"
READ_CONTROL_ROOT="/var/tmp/zerofs-tiered-control-${READ_CONTROL_UUID}"
READ_CONTROL_RESOURCES="/var/tmp/zerofs-tiered-resources-${READ_CONTROL_UUID}"
READ_CONTROL_LEDGER="${READ_CONTROL_ROOT}/ledger.json"
sudo python3 scripts/tiered-writeback-e2e.py setup --ledger "$READ_CONTROL_LEDGER" --control-root "$READ_CONTROL_ROOT" --resource-root "$READ_CONTROL_RESOURCES" --source-sha "$FINAL_PROOF_SHA" --filesystem-ack-mode materialized --object-ack-mode ssd
sudo python3 scripts/tiered-writeback-e2e.py run --ledger "$READ_CONTROL_LEDGER" --filesystem-ack-mode materialized --object-ack-mode ssd --scenario benchmark-read-throughput
MATERIALIZED_RECEIPT="$(python3 scripts/tiered-writeback-e2e.py ledger-value --ledger "$READ_CONTROL_LEDGER" --key latest_receipt)"
test -s "$MATERIALIZED_RECEIPT"
case "$MATERIALIZED_RECEIPT" in "$READ_CONTROL_ROOT"/*) ;; *) exit 1 ;; esac
sudo python3 scripts/tiered-writeback-e2e.py cleanup --ledger "$READ_CONTROL_LEDGER"
sudo python3 scripts/tiered-writeback-e2e.py cleanup --ledger "$READ_CONTROL_LEDGER"
sudo python3 scripts/tiered-writeback-e2e.py assert-clean --ledger "$READ_CONTROL_LEDGER"

READ_CANDIDATE_UUID="$(python3 -c 'import uuid; print(uuid.uuid4())')"
READ_CANDIDATE_ROOT="/var/tmp/zerofs-tiered-control-${READ_CANDIDATE_UUID}"
READ_CANDIDATE_RESOURCES="/var/tmp/zerofs-tiered-resources-${READ_CANDIDATE_UUID}"
READ_CANDIDATE_LEDGER="${READ_CANDIDATE_ROOT}/ledger.json"
sudo python3 scripts/tiered-writeback-e2e.py setup --ledger "$READ_CANDIDATE_LEDGER" --control-root "$READ_CANDIDATE_ROOT" --resource-root "$READ_CANDIDATE_RESOURCES" --source-sha "$FINAL_PROOF_SHA" --filesystem-ack-mode volatile_memory --object-ack-mode ssd
sudo python3 scripts/tiered-writeback-e2e.py run --ledger "$READ_CANDIDATE_LEDGER" --filesystem-ack-mode volatile_memory --object-ack-mode ssd --scenario benchmark-read-throughput --control-receipt "$MATERIALIZED_RECEIPT"
sudo python3 scripts/tiered-writeback-e2e.py cleanup --ledger "$READ_CANDIDATE_LEDGER"
sudo python3 scripts/tiered-writeback-e2e.py cleanup --ledger "$READ_CANDIDATE_LEDGER"
sudo python3 scripts/tiered-writeback-e2e.py assert-clean --ledger "$READ_CANDIDATE_LEDGER"
sudo python3 scripts/tiered-writeback-e2e.py archive-control --ledger "$READ_CANDIDATE_LEDGER" --archive-root /fast/zerofs-tiered-receipts
sudo python3 scripts/tiered-writeback-e2e.py archive-control --ledger "$READ_CONTROL_LEDGER" --archive-root /fast/zerofs-tiered-receipts
test ! -e "$READ_CANDIDATE_ROOT"
test ! -e "$READ_CONTROL_ROOT"
```

No command in this step runs on macOS. Any correction repeats portable GREEN, exact commit/review, push, literal-SHA synchronization, and this affected Linux command.

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
RUN_UUID="$(python3 scripts/tiered-writeback-e2e.py ledger-value --ledger "$LEDGER" --key run_uuid)"
sudo python3 scripts/tiered-writeback-e2e.py archive-control --ledger "$LEDGER" --archive-root /fast/zerofs-tiered-receipts
test ! -e "/var/tmp/zerofs-tiered-control-${RUN_UUID}"
test -s "/fast/zerofs-tiered-receipts/${RUN_UUID}/ledger.json"
```

Prove no UUID-owned mount/device/pool/filesystem/process/listener/socket/prefix/resource/control root remains. The preserved receipt archive is evidence, not a live test resource. Prove no active build/test/harness job exists before touching checkouts. Remove the separate `nfsserve` worktree only after its immutable pushed revision is pinned and its worktree is clean.

```bash
test -z "$(git -C /Volumes/bigssd/projects/nfsserve/.worktrees/zerofs-write-context status --porcelain=v1)"
git -C /Volumes/bigssd/projects/nfsserve worktree remove /Volumes/bigssd/projects/nfsserve/.worktrees/zerofs-write-context
git -C /Volumes/bigssd/projects/nfsserve worktree prune
git -C /Volumes/bigssd/projects/nfsserve worktree list --porcelain
test ! -e /Volumes/bigssd/projects/nfsserve/.worktrees/zerofs-write-context
```

- [ ] **Step 2: Final review and merge/push `develop`**

From the clean primary checkout, fast-forward and push with exact checks:

```bash
cd /Volumes/bigssd/projects/ZeroFS
test -z "$(git status --porcelain=v1)"
test "$(git branch --show-current)" = develop
EXPECTED_OLD_SHA="$(git rev-parse HEAD^{commit})"; test "${#EXPECTED_OLD_SHA}" = 40
FEATURE_SHA="$(git -C /Volumes/bigssd/projects/ZeroFS/.worktrees/unified-tiered-writeback rev-parse HEAD^{commit})"; test "${#FEATURE_SHA}" = 40
git fetch origin develop codex/unified-tiered-writeback
test "$(git rev-parse origin/codex/unified-tiered-writeback)" = "$FEATURE_SHA"
test "$(git rev-parse origin/develop)" = "$EXPECTED_OLD_SHA"
git merge-base --is-ancestor "$EXPECTED_OLD_SHA" "$FEATURE_SHA"
git merge --ff-only "$FEATURE_SHA"
EXPECTED_NEW_SHA="$(git rev-parse HEAD^{commit})"
test "$EXPECTED_NEW_SHA" = "$FEATURE_SHA"
git push origin "$EXPECTED_NEW_SHA:refs/heads/develop"
test "$(git ls-remote origin refs/heads/develop | awk '{print $1}')" = "$EXPECTED_NEW_SHA"
test -z "$(git status --porcelain=v1)"
```

- [ ] **Step 3: Fail-closed Ubuntu fast-forward**

Pass the two recorded SHAs from the local primary-checkout shell into one fail-closed Ubuntu command:

```bash
cd /Volumes/bigssd/projects/ZeroFS
test "${#EXPECTED_OLD_SHA}" = 40
test "${#EXPECTED_NEW_SHA}" = 40
ssh ubuntu-main "EXPECTED_OLD_SHA='$EXPECTED_OLD_SHA' EXPECTED_NEW_SHA='$EXPECTED_NEW_SHA' bash -se" <<'REMOTE'
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
REMOTE
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

```bash
cd /Volumes/bigssd/projects/ZeroFS
test -z "$(git -C /Volumes/bigssd/projects/ZeroFS/.worktrees/unified-tiered-writeback status --porcelain=v1)"
git worktree remove /Volumes/bigssd/projects/ZeroFS/.worktrees/unified-tiered-writeback
git worktree prune
git worktree list --porcelain
test ! -e /Volumes/bigssd/projects/ZeroFS/.worktrees/unified-tiered-writeback
test -z "$(git status --porcelain=v1)"
```
