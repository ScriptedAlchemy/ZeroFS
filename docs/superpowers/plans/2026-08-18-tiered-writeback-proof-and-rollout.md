# Tiered Writeback Proof and Rollout Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Prove unified writeback through real crash, restart, bounded resident memory and protocol ingress, NFS, 9P, NBD, WebUI/RPC, filesystem, integrity, SSH/SFTP scaling, performance, cleanup, quality, merge, and Ubuntu fast-forward evidence.

**Architecture:** Portable build/unit/model/browser gates run on macOS. Every kernel mount, block device, Linux filesystem, real process crash, xfstests, pjdfstest, kernel compile, stress-ng, ZFS/XFS-over-NBD, and performance workload runs only in a UUID-ledgered isolated Ubuntu checkout of the exact pushed SHA.

**Tech Stack:** Cargo, failpoints, DST, dependency-free Python 3 `unittest`, Linux cgroup v2, stock OpenSSH, pinned HPN OpenSSH, NFSv3, v9fs/native 9P, `nbd-client`, XFS, ZFS, xfstests, pjdfstest, stress-ng, Node 22/WASM, Git/SSH, SHA-256 manifests.

**Spec:** `docs/superpowers/specs/2026-08-18-unified-tiered-writeback-design.md`

## Global Constraints

- Every harness setup/run/receipt carries both independent flags exactly: `--filesystem-ack-mode materialized|volatile_memory` and `--object-ack-mode memory|ssd|remote`.
- The harness uses only Python standard-library `unittest`; no third-party test runner is a dependency or command.
- No mock, in-memory filesystem, fixture-only adapter, or direct internal call counts as protocol acceptance.
- macOS runs portable Rust/build/lint/model/WebUI/WASM tests only. Real Linux protocols, mounts, devices, filesystems, process crashes, and benchmarks run on Ubuntu only.
- The Ubuntu proof checkout is `/fast/projects/ZeroFS-unified-tiered-writeback`; `/fast/projects/ZeroFS` remains the clean `develop` checkout until final fast-forward.
- Never touch CT198, VM100 production mounts, production Storage Box prefixes/exports, or an active NBD device.
- The 2026-08-19 CT198 OOM/systemd restart is evidence only. No setup, run, cleanup, merge, source-sync, or corrective task deploys or restarts CT198.
- VM100 remains NFS-only for the shared Mac/Linux namespace. Every NBD/XFS/ZFS leg uses a disposable UUID-owned Ubuntu device and is removed by ledger cleanup; no NBD client or mount is installed on VM100 by this rollout.
- `ubuntu-main` is VM100 and may run isolated user-space/NFS/9P/cgroup tests without touching `/mnt/zerofs-files`; it never runs an NBD scenario. NBD/XFS/ZFS requires an explicit `ZEROFS_NBD_PROOF_HOST` that is neither `ubuntu-main`, VM100, nor CT198. An absent or forbidden host skips nothing and fails the NBD gate closed.
- An alias denylist is insufficient proof-host identity. Before any NBD worktree or device command, `verify-proof-host` pins the SSH host-key SHA-256, exact `/etc/machine-id`, and provisioned `/etc/zerofs-proxmox-vmid`; it compares them with live VM100 and CT198 machine IDs and requires the expected VMID to be neither 100 nor 198. Every NBD ledger retains that identity receipt.
- Every process, port, mount, device, cgroup/scope, changed sysfs value, SSH executable/source/build/install root, filesystem/pool name, object prefix/temp object, cache/state directory, scratch directory, and tool checkout is unique and recorded in one UUID resource ledger.
- The immutable ledger and cleanup receipts live in `CONTROL_ROOT=/var/tmp/zerofs-tiered-control-$RUN_UUID`; disposable processes, mounts, devices, data, scratch, and tool checkouts live in the separate `RESOURCE_ROOT=/var/tmp/zerofs-tiered-resources-$RUN_UUID`. Cleanup never deletes its own authority.
- Cleanup is idempotent after success, failure, partial setup, cancellation, supervisor cancellation, and crash.
- Keep failure receipts; remove only exact ledger-owned resources.
- Stock OpenSSH remains installed and unchanged. A pinned HPN executable lives only under a ledger-owned resource root until a separate explicit promotion; global `PATH`, `update-alternatives`, `/usr/bin/ssh`, and host SSH configuration are immutable.
- Any manual multi-command ledger block runs in one `bash -euo pipefail` process with an EXIT trap that saves the primary status, performs cleanup twice plus `assert-clean`, then returns the primary failure before any cleanup failure. Single-scenario calls use harness `supervise`; a later successful command can never mask an earlier failure.

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
- Create: `scripts/tiered_writeback_e2e/scenarios.py`
- Create: `scripts/tests/test_tiered_writeback_e2e.py`

**Interfaces:**
- Produces: `setup`, `list-scenarios`, `verify-proof-host`, `run`, `supervise`, `cleanup --ledger`, `assert-clean --ledger`, `archive-control`, `list-ledgers --campaign`, and `assert-source-idle` commands plus JSON receipts.
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

Name tests for rejecting `/`, `/mnt`, `/var/tmp`, equal/nested control and resource roots, workspace roots, CT198/production strings, unowned PIDs/devices/mounts/ports/cgroups/sysfs paths/SSH binaries, missing dual ack flags, receipt omission, partial setup, primary-plus-cleanup errors, cleanup preserving ledger authority, repeated cleanup after resource-root deletion, final receipt archiving, supervisor cancellation, a successful cleanup masking a failed primary scenario, an alternate alias whose pinned host key/machine ID is VM100 or CT198, an unprovisioned/mismatched Proxmox VMID marker, an unknown scenario, a registered scenario without a real handler, and a documented scenario missing from the registry.

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

For UUID `RUN_UUID`, setup requires `CONTROL_ROOT=/var/tmp/zerofs-tiered-control-$RUN_UUID`, `RESOURCE_ROOT=/var/tmp/zerofs-tiered-resources-$RUN_UUID`, `LEDGER=$CONTROL_ROOT/ledger.json`, `RECEIPT_ROOT=$CONTROL_ROOT/receipts`, and campaign `codex-unified-tiered-writeback`; control and resource roots must be disjoint siblings. Identity/config fields in the ledger are immutable and every later event is hash-chained append-only. Setup atomically appends host, source SHA, UUID, and ledger path to `/fast/zerofs-tiered-receipts/in-progress/codex-unified-tiered-writeback.jsonl`, an append-only authority outside both roots spanning every corrective SHA. The JSON receipt records both ack fields, source HEAD, binary/config hashes, both roots, exact PIDs, ports, devices, mounts, cgroup/scope paths and original sysfs values, pool/filesystem names, backend prefix/temp objects, SSH executable path/version/SHA, HPN source/build/install roots, tool checkout revisions, scenario, manifest, typed durability floors, resident-memory/cgroup samples, terminal state, commands, exit status, and cleanup status. `supervise` executes one registered real scenario, always performs cleanup twice plus `assert-clean`, records primary and cleanup statuses separately, and exits nonzero if either failed; successful cleanup can never mask the primary failure. `cleanup --ledger PATH` removes only entries under `RESOURCE_ROOT`, kills/reaps exact SSH children, removes exact remote temp objects/prefix, restores ledgered sysfs bytes before deleting their owned cgroup/scope, and succeeds after that root is already absent. `assert-clean --ledger PATH` continues to read the external ledger and fails if any recorded process/listener/mount/device/cgroup/sysfs mutation/HPN artifact/pool/prefix/temp object/cache root/resource path remains; it never requires the control root to be absent. `list-ledgers --campaign codex-unified-tiered-writeback` reads the persistent run index and emits every host/ledger pair exactly once. `archive-control --ledger PATH --archive-root /fast/zerofs-tiered-receipts` copies ledger/receipts to `/fast/zerofs-tiered-receipts/$RUN_UUID`, verifies hashes, marks the run-index entry archived, and only then removes `CONTROL_ROOT`. `assert-source-idle --source-root PATH` inspects `/proc/*/cwd` and fails for active `cargo`, `rustc`, test, or harness jobs rooted at PATH.

`verify-proof-host --format token` returns canonical unpadded base64url containing the
signed identity receipt; callers reject anything outside `[A-Za-z0-9_-]+` before
placing it in an SSH environment assignment. `setup` verifies the signature and embeds
the decoded identity receipt in the ledger before any NBD resource allocation.

`scenarios.py` owns one immutable `SCENARIOS: dict[str, ScenarioHandler]`. The CLI
`list-scenarios` prints sorted names and `run` rejects an unknown name or a registry
entry without a concrete handler. The dependency-free test contains the exact set
consumed by C3-C8:

```python
EXPECTED_SCENARIOS = {
    "global-admission-nbd-nfs-ninep",
    "cross-adapter-pending-read-same-backing-inode",
    "nfs-commit-covers-prior-nbd",
    "ninep-fsync-covers-prior-nfs",
    "nbd-flush-covers-prior-ninep",
    "webui-rpc-production-path",
    "protocol-materialized-control",
    "protocol-durability-target-control",
    "xfstests-nfs-quick",
    "xfstests-ninep-quick-and-strict",
    "pjdfstest-nfs",
    "pjdfstest-ninep",
    "stress-ng-nfs-ninep",
    "kernel-compile-nfs",
    "kernel-compile-ninep",
    "xfs-over-nbd-restart",
    "zfs-over-nbd-restart",
    "crash-boundary-matrix",
    "local-receipt-restart",
    "remote-receipt-clean-cache-restart",
    "terminal-fanout-and-shutdown-timeout",
    "benchmark-ram-ack",
    "benchmark-local-ssd",
    "benchmark-paced-remote",
    "benchmark-4gib-foreground-isolation",
    "benchmark-100gib-ram-to-ssd-transition",
    "benchmark-ssd-pressure-to-remote-pacing",
    "benchmark-read-throughput",
    "benchmark-read-throughput-nbd",
    "memory-envelope-nfs-retransmit-gc",
    "sftp-stock-vs-hpn-download",
    "sftp-stock-vs-hpn-upload",
    "zerofs-sftp-session-scaling",
}
```

The test asserts set equality and invokes every handler's argument validation against
a ledger-shaped dry-run context; a lambda/no-op/receipt-only handler is rejected.

- [ ] **Step 3: Implement real shipping entry points**

NFS uses a hard NFSv3 kernel mount and actual WRITE/COMMIT; 9P uses both v9fs and native client plus actual Twrite/Tfsync; NBD uses `nbd-client`, a disposable device, XFS/ZFS, WRITE/FUA/FLUSH; RPC uses the actual Unix/TCP gRPC client/server; WebUI uses the real gRPC-Web/WebSocket/9P route and generated WASM client. Internal Rust calls may instrument failpoints but never replace these acceptance legs.

`linux_suites.py` creates tool checkouts only under `RESOURCE_ROOT/tools` and pins xfstests `1ae822c1c2e2364e966085cee3ce4a97b2500241`, pjdfstest `85a8aea9e685999ef0540392fd80535f873d7ff7`, and pjdfstest_nfs `7d3d7cb0cdc5d39eedd995771bc1d4b3dabf31ab`. Kernel scenarios download only `https://cdn.kernel.org/pub/linux/kernel/v6.x/linux-6.18.tar.xz` and require SHA-256 `9106a4605da9e31ff17659d958782b815f9591ab308d03b0ee21aad6c7dced4b` before extraction. These rules apply equally to focused C4 runs and C8's final scenario reruns.

- [ ] **Step 4: Run portable harness gates and commit**

```bash
cd /Volumes/bigssd/projects/ZeroFS/.worktrees/unified-tiered-writeback
python3 -m compileall -q scripts/tiered_writeback_e2e scripts/tiered-writeback-e2e.py
python3 -m unittest discover -s scripts/tests -p 'test_*.py' -v
python3 scripts/tiered-writeback-e2e.py list-scenarios > "${TMPDIR:-/tmp}/zerofs-scenarios.list"
test "$(wc -l < "${TMPDIR:-/tmp}/zerofs-scenarios.list" | tr -d ' ')" -eq 33
git diff --check
git add scripts/tiered-writeback-e2e.py scripts/tiered_writeback_e2e/__init__.py scripts/tiered_writeback_e2e/config.py scripts/tiered_writeback_e2e/resources.py scripts/tiered_writeback_e2e/lifecycle.py scripts/tiered_writeback_e2e/protocols.py scripts/tiered_writeback_e2e/integrity.py scripts/tiered_writeback_e2e/crash.py scripts/tiered_writeback_e2e/linux_suites.py scripts/tiered_writeback_e2e/scenarios.py scripts/tests/test_tiered_writeback_e2e.py
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
test -n "${ZEROFS_NBD_PROOF_HOST:-}"
case "$ZEROFS_NBD_PROOF_HOST" in ubuntu-main|vm100|100.125.144.4|ct198|10.10.10.55|100.108.226.83) exit 1 ;; esac
VM100_MACHINE_ID="$(ssh ubuntu-main 'cat /etc/machine-id')"
CT198_MACHINE_ID="$(ssh root@100.108.226.83 'pct exec 198 -- cat /etc/machine-id')"
NBD_PROOF_IDENTITY_TOKEN="$(python3 scripts/tiered-writeback-e2e.py verify-proof-host --ssh-host "$ZEROFS_NBD_PROOF_HOST" --expected-host-key-sha256 "$ZEROFS_NBD_PROOF_HOST_KEY_SHA256" --expected-machine-id "$ZEROFS_NBD_PROOF_MACHINE_ID" --expected-proxmox-vmid "$ZEROFS_NBD_PROOF_VMID" --forbid-machine-id "$VM100_MACHINE_ID" --forbid-machine-id "$CT198_MACHINE_ID" --forbid-proxmox-vmid 100 --forbid-proxmox-vmid 198 --format token)"
case "$NBD_PROOF_IDENTITY_TOKEN" in ''|*[!A-Za-z0-9_-]*) exit 1 ;; esac
ssh "$ZEROFS_NBD_PROOF_HOST" "cd /fast/projects/ZeroFS && git fetch origin codex/unified-tiered-writeback && test \"\$(git rev-parse origin/codex/unified-tiered-writeback)\" = '$SLICE_SHA'"
ssh "$ZEROFS_NBD_PROOF_HOST" "test -d /fast/projects/ZeroFS-unified-tiered-writeback || (cd /fast/projects/ZeroFS && git worktree add --detach /fast/projects/ZeroFS-unified-tiered-writeback '$SLICE_SHA')"
ssh "$ZEROFS_NBD_PROOF_HOST" "cd /fast/projects/ZeroFS-unified-tiered-writeback && test -z \"\$(git status --porcelain=v1)\" && python3 scripts/tiered-writeback-e2e.py assert-source-idle --source-root /fast/projects/ZeroFS-unified-tiered-writeback && git switch --detach '$SLICE_SHA' && test \"\$(git rev-parse HEAD)\" = '$SLICE_SHA'"
```

- [ ] **Step 2: Create external control authority and disposable resources**

On the separate NBD proof host, record the expanded `SLICE_SHA` in the ledger. Generate
the UUID in the controlling shell so every later remote step names the same authority:

```bash
C3_UUID="$(python3 -c 'import uuid; print(uuid.uuid4())')"
C3_CONTROL_ROOT="/var/tmp/zerofs-tiered-control-${C3_UUID}"
C3_RESOURCE_ROOT="/var/tmp/zerofs-tiered-resources-${C3_UUID}"
C3_LEDGER="${C3_CONTROL_ROOT}/ledger.json"
ssh "$ZEROFS_NBD_PROOF_HOST" "SLICE_SHA='$SLICE_SHA' CONTROL_ROOT='$C3_CONTROL_ROOT' RESOURCE_ROOT='$C3_RESOURCE_ROOT' LEDGER='$C3_LEDGER' PROOF_HOST_IDENTITY='$NBD_PROOF_IDENTITY_TOKEN' bash -seuo pipefail" <<'REMOTE'
cd /fast/projects/ZeroFS-unified-tiered-writeback
test "$(git rev-parse HEAD)" = "$SLICE_SHA"
primary=0; cleanup=0
sudo python3 scripts/tiered-writeback-e2e.py setup --ledger "$LEDGER" --control-root "$CONTROL_ROOT" --resource-root "$RESOURCE_ROOT" --source-sha "$SLICE_SHA" --proof-host-identity "$PROOF_HOST_IDENTITY" --filesystem-ack-mode volatile_memory --object-ack-mode memory || primary=$?
if test "$primary" -ne 0; then
  sudo python3 scripts/tiered-writeback-e2e.py cleanup --ledger "$LEDGER" || cleanup=$?
  sudo python3 scripts/tiered-writeback-e2e.py cleanup --ledger "$LEDGER" || cleanup=$?
  sudo python3 scripts/tiered-writeback-e2e.py assert-clean --ledger "$LEDGER" || cleanup=$?
  test "$cleanup" -eq 0
  exit "$primary"
fi
REMOTE
```

- [ ] **Step 3: Run the exact volatile shipping scenarios**

Each command uses real NBD/NFS/9P/WebUI/RPC clients on the separate host and appends a
receipt carrying both flags:

```bash
ssh "$ZEROFS_NBD_PROOF_HOST" "LEDGER='$C3_LEDGER' bash -seuo pipefail" <<'REMOTE'
cd /fast/projects/ZeroFS-unified-tiered-writeback
cleanup_preserving_primary() {
  primary=$?
  cleanup=0
  sudo python3 scripts/tiered-writeback-e2e.py cleanup --ledger "$LEDGER" || cleanup=$?
  sudo python3 scripts/tiered-writeback-e2e.py cleanup --ledger "$LEDGER" || cleanup=$?
  sudo python3 scripts/tiered-writeback-e2e.py assert-clean --ledger "$LEDGER" || cleanup=$?
  if test "$primary" -ne 0; then exit "$primary"; fi
  exit "$cleanup"
}
trap cleanup_preserving_primary EXIT
sudo python3 scripts/tiered-writeback-e2e.py run --ledger "$LEDGER" --filesystem-ack-mode volatile_memory --object-ack-mode memory --scenario global-admission-nbd-nfs-ninep
sudo python3 scripts/tiered-writeback-e2e.py run --ledger "$LEDGER" --filesystem-ack-mode volatile_memory --object-ack-mode memory --scenario cross-adapter-pending-read-same-backing-inode
sudo python3 scripts/tiered-writeback-e2e.py run --ledger "$LEDGER" --filesystem-ack-mode volatile_memory --object-ack-mode memory --scenario nfs-commit-covers-prior-nbd
sudo python3 scripts/tiered-writeback-e2e.py run --ledger "$LEDGER" --filesystem-ack-mode volatile_memory --object-ack-mode memory --scenario ninep-fsync-covers-prior-nfs
sudo python3 scripts/tiered-writeback-e2e.py run --ledger "$LEDGER" --filesystem-ack-mode volatile_memory --object-ack-mode memory --scenario nbd-flush-covers-prior-ninep
sudo python3 scripts/tiered-writeback-e2e.py run --ledger "$LEDGER" --filesystem-ack-mode volatile_memory --object-ack-mode memory --scenario webui-rpc-production-path
REMOTE
```

The same-backing-inode scenario provisions an NBD member as a normal ZeroFS inode reachable by the direct namespace, pauses canonical materialization, writes through the live NBD server, and reads that exact inode through mounted NFS and 9P. It does not claim guest-XFS namespace unification.

- [ ] **Step 4: Run materialized/object-target controls**

```bash
ssh "$ZEROFS_NBD_PROOF_HOST" "LEDGER='$C3_LEDGER' bash -seuo pipefail" <<'REMOTE'
cd /fast/projects/ZeroFS-unified-tiered-writeback
cleanup=0
sudo python3 scripts/tiered-writeback-e2e.py cleanup --ledger "$LEDGER" || cleanup=$?
sudo python3 scripts/tiered-writeback-e2e.py cleanup --ledger "$LEDGER" || cleanup=$?
sudo python3 scripts/tiered-writeback-e2e.py assert-clean --ledger "$LEDGER" || cleanup=$?
test "$cleanup" -eq 0
REMOTE

MATERIALIZED_UUID="$(python3 -c 'import uuid; print(uuid.uuid4())')"
MATERIALIZED_CONTROL="/var/tmp/zerofs-tiered-control-${MATERIALIZED_UUID}"
MATERIALIZED_RESOURCES="/var/tmp/zerofs-tiered-resources-${MATERIALIZED_UUID}"
MATERIALIZED_LEDGER="${MATERIALIZED_CONTROL}/ledger.json"
ssh "$ZEROFS_NBD_PROOF_HOST" "SLICE_SHA='$SLICE_SHA' CONTROL_ROOT='$MATERIALIZED_CONTROL' RESOURCE_ROOT='$MATERIALIZED_RESOURCES' LEDGER='$MATERIALIZED_LEDGER' PROOF_HOST_IDENTITY='$NBD_PROOF_IDENTITY_TOKEN' bash -seuo pipefail" <<'REMOTE'
cd /fast/projects/ZeroFS-unified-tiered-writeback
primary=0; cleanup=0
sudo python3 scripts/tiered-writeback-e2e.py setup --ledger "$LEDGER" --control-root "$CONTROL_ROOT" --resource-root "$RESOURCE_ROOT" --source-sha "$SLICE_SHA" --proof-host-identity "$PROOF_HOST_IDENTITY" --filesystem-ack-mode materialized --object-ack-mode ssd || primary=$?
if test "$primary" -eq 0; then
  sudo python3 scripts/tiered-writeback-e2e.py run --ledger "$LEDGER" --filesystem-ack-mode materialized --object-ack-mode ssd --scenario protocol-materialized-control || primary=$?
fi
sudo python3 scripts/tiered-writeback-e2e.py cleanup --ledger "$LEDGER" || cleanup=$?
sudo python3 scripts/tiered-writeback-e2e.py cleanup --ledger "$LEDGER" || cleanup=$?
sudo python3 scripts/tiered-writeback-e2e.py assert-clean --ledger "$LEDGER" || cleanup=$?
test "$primary" -eq 0 && test "$cleanup" -eq 0
REMOTE
```

Run the remote object-ack control under a new matching ledger:

```bash
REMOTE_UUID="$(python3 -c 'import uuid; print(uuid.uuid4())')"
REMOTE_CONTROL="/var/tmp/zerofs-tiered-control-${REMOTE_UUID}"
REMOTE_RESOURCES="/var/tmp/zerofs-tiered-resources-${REMOTE_UUID}"
REMOTE_LEDGER="${REMOTE_CONTROL}/ledger.json"
ssh "$ZEROFS_NBD_PROOF_HOST" "SLICE_SHA='$SLICE_SHA' CONTROL_ROOT='$REMOTE_CONTROL' RESOURCE_ROOT='$REMOTE_RESOURCES' LEDGER='$REMOTE_LEDGER' PROOF_HOST_IDENTITY='$NBD_PROOF_IDENTITY_TOKEN' bash -seuo pipefail" <<'REMOTE'
cd /fast/projects/ZeroFS-unified-tiered-writeback
primary=0; cleanup=0
sudo python3 scripts/tiered-writeback-e2e.py setup --ledger "$LEDGER" --control-root "$CONTROL_ROOT" --resource-root "$RESOURCE_ROOT" --source-sha "$SLICE_SHA" --proof-host-identity "$PROOF_HOST_IDENTITY" --filesystem-ack-mode materialized --object-ack-mode remote || primary=$?
if test "$primary" -eq 0; then
  sudo python3 scripts/tiered-writeback-e2e.py run --ledger "$LEDGER" --filesystem-ack-mode materialized --object-ack-mode remote --scenario protocol-durability-target-control || primary=$?
fi
sudo python3 scripts/tiered-writeback-e2e.py cleanup --ledger "$LEDGER" || cleanup=$?
sudo python3 scripts/tiered-writeback-e2e.py cleanup --ledger "$LEDGER" || cleanup=$?
sudo python3 scripts/tiered-writeback-e2e.py assert-clean --ledger "$LEDGER" || cleanup=$?
test "$primary" -eq 0 && test "$cleanup" -eq 0
REMOTE
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

Run the required promotion block, then on Ubuntu create a new external control root and disposable resource root. Execute Steps 1-4 in one `bash -euo pipefail` process with the status-preserving EXIT cleanup required by the global constraints. Tool sources are never shared through `/tmp`:

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
test -n "${ZEROFS_NBD_PROOF_HOST:-}"
case "$ZEROFS_NBD_PROOF_HOST" in ubuntu-main|vm100|100.125.144.4|ct198|10.10.10.55|100.108.226.83) exit 1 ;; esac
VM100_MACHINE_ID="$(ssh ubuntu-main 'cat /etc/machine-id')"
CT198_MACHINE_ID="$(ssh root@100.108.226.83 'pct exec 198 -- cat /etc/machine-id')"
NBD_PROOF_IDENTITY_TOKEN="$(python3 scripts/tiered-writeback-e2e.py verify-proof-host --ssh-host "$ZEROFS_NBD_PROOF_HOST" --expected-host-key-sha256 "$ZEROFS_NBD_PROOF_HOST_KEY_SHA256" --expected-machine-id "$ZEROFS_NBD_PROOF_MACHINE_ID" --expected-proxmox-vmid "$ZEROFS_NBD_PROOF_VMID" --forbid-machine-id "$VM100_MACHINE_ID" --forbid-machine-id "$CT198_MACHINE_ID" --forbid-proxmox-vmid 100 --forbid-proxmox-vmid 198 --format token)"
case "$NBD_PROOF_IDENTITY_TOKEN" in ''|*[!A-Za-z0-9_-]*) exit 1 ;; esac
ssh "$ZEROFS_NBD_PROOF_HOST" "cd /fast/projects/ZeroFS && git fetch origin codex/unified-tiered-writeback && test \"\$(git rev-parse origin/codex/unified-tiered-writeback)\" = '$SLICE_SHA'"
ssh "$ZEROFS_NBD_PROOF_HOST" "test -d /fast/projects/ZeroFS-unified-tiered-writeback || (cd /fast/projects/ZeroFS && git worktree add --detach /fast/projects/ZeroFS-unified-tiered-writeback '$SLICE_SHA')"
ssh "$ZEROFS_NBD_PROOF_HOST" "cd /fast/projects/ZeroFS-unified-tiered-writeback && test -z \"\$(git status --porcelain=v1)\" && python3 scripts/tiered-writeback-e2e.py assert-source-idle --source-root /fast/projects/ZeroFS-unified-tiered-writeback && git switch --detach '$SLICE_SHA' && test \"\$(git rev-parse HEAD)\" = '$SLICE_SHA'"
ssh "$ZEROFS_NBD_PROOF_HOST" "SLICE_SHA='$SLICE_SHA' PROOF_HOST_IDENTITY='$NBD_PROOF_IDENTITY_TOKEN' bash -seuo pipefail" <<'REMOTE'
cd /fast/projects/ZeroFS-unified-tiered-writeback
RUN_UUID="$(python3 -c 'import uuid; print(uuid.uuid4())')"
CONTROL_ROOT="/var/tmp/zerofs-tiered-control-${RUN_UUID}"
RESOURCE_ROOT="/var/tmp/zerofs-tiered-resources-${RUN_UUID}"
LEDGER="${CONTROL_ROOT}/ledger.json"
sudo python3 scripts/tiered-writeback-e2e.py supervise --ledger "$LEDGER" --control-root "$CONTROL_ROOT" --resource-root "$RESOURCE_ROOT" --source-sha "$SLICE_SHA" --proof-host-identity "$PROOF_HOST_IDENTITY" --filesystem-ack-mode volatile_memory --object-ack-mode ssd --scenario xfs-over-nbd-restart
REMOTE
```

- [ ] **Step 6: Run ZFS-over-NBD from a different fresh setup/cleanup ledger**

```bash
test -n "${ZEROFS_NBD_PROOF_HOST:-}"
case "$ZEROFS_NBD_PROOF_HOST" in ubuntu-main|vm100|100.125.144.4|ct198|10.10.10.55|100.108.226.83) exit 1 ;; esac
case "$NBD_PROOF_IDENTITY_TOKEN" in ''|*[!A-Za-z0-9_-]*) exit 1 ;; esac
ssh "$ZEROFS_NBD_PROOF_HOST" "SLICE_SHA='$SLICE_SHA' PROOF_HOST_IDENTITY='$NBD_PROOF_IDENTITY_TOKEN' bash -seuo pipefail" <<'REMOTE'
cd /fast/projects/ZeroFS-unified-tiered-writeback
test "$(git rev-parse HEAD)" = "$SLICE_SHA"
RUN_UUID="$(python3 -c 'import uuid; print(uuid.uuid4())')"
CONTROL_ROOT="/var/tmp/zerofs-tiered-control-${RUN_UUID}"
RESOURCE_ROOT="/var/tmp/zerofs-tiered-resources-${RUN_UUID}"
LEDGER="${CONTROL_ROOT}/ledger.json"
sudo python3 scripts/tiered-writeback-e2e.py supervise --ledger "$LEDGER" --control-root "$CONTROL_ROOT" --resource-root "$RESOURCE_ROOT" --source-sha "$SLICE_SHA" --proof-host-identity "$PROOF_HOST_IDENTITY" --filesystem-ack-mode volatile_memory --object-ack-mode ssd --scenario zfs-over-nbd-restart
REMOTE
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
  sudo python3 scripts/tiered-writeback-e2e.py supervise --ledger "$LEDGER" --control-root "$CONTROL_ROOT" --resource-root "$RESOURCE_ROOT" --source-sha "$CRASH_SHA" --filesystem-ack-mode volatile_memory --object-ack-mode "$object_mode" --scenario "$scenario"
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
- Create: `scripts/benchmarking/ssh_transport.py`
- Create: `scripts/tiered_writeback_e2e/memory_benchmark.py`
- Create: `scripts/tiered_writeback_e2e/sftp_benchmark.py`
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
- Create: `scripts/tests/test_memory_envelope_benchmark.py`
- Create: `scripts/tests/test_sftp_transport_benchmark.py`
- Modify only after a real RED: production read instrumentation owning the observed failure
- Receipt: UUID Ubuntu external control root outside Git

**Interfaces:**
- Produces: separate foreground RAM-ack, local SSD cutoff, paced remote-drain, remote flush, remote-cold read, clean-SSD read, clean-RAM read, cgroup-bounded mixed-load residency, and direction-specific stock/HPN/ZeroFS SFTP throughput/latency with integrity.
- Consumes: ledger `local_ssd_scratch`, incompressible payloads, disposable backend, exact metrics, both ack flags, kernel NFSv3, native 9P, `nbd-client`/XFS, stock OpenSSH SFTP, and a pinned ledger-owned HPN executable.

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
  sudo python3 scripts/tiered-writeback-e2e.py supervise --ledger "$LEDGER" --control-root "$CONTROL_ROOT" --resource-root "$RESOURCE_ROOT" --source-sha "$SLICE_SHA" --filesystem-ack-mode volatile_memory --object-ack-mode "$object_mode" --scenario "$scenario"
}
run_benchmark_scenario memory benchmark-ram-ack
run_benchmark_scenario ssd benchmark-local-ssd
run_benchmark_scenario remote benchmark-paced-remote
run_benchmark_scenario ssd benchmark-4gib-foreground-isolation
run_benchmark_scenario ssd benchmark-100gib-ram-to-ssd-transition
run_benchmark_scenario ssd benchmark-ssd-pressure-to-remote-pacing
```

The production-shaped scenarios configure 16 GB shared dirty-write RAM, 64 GB clean read cache, a 1 TB local SSD tier split into the configured clean-cache and durable journal/staging budgets, and a 5 TB-class export without counting sparse virtual geometry as remote physical use. The 4 GiB leg must remain on RAM/local SSD and reject any unexplained collapse to remote rate. The 100 GiB leg records the RAM-to-SSD transition and concurrent remote drain. The pressure leg preconditions only its disposable SSD ledger resources near the configured dirty limit, then proves each ordered remote cleanup admits incremental foreground work without a 95-to-85-percent pause.

Each scenario records same-host durable local control and durability-matched same-endpoint raw SFTP control results. The production targets are approximately 800-900 MB/s local SSD and 70-100 MB/s raw SFTP; acceptance is paired to the measured control so a slower external path is diagnosed rather than concealed. Every receipt includes size/SHA-256/readback, mutation/object floors, dirty tiers, tier-transition timestamps, terminal state, CPU/RAM/local allocation, remote rate, and cleanup. Performance without integrity and durability is rejected.

The following resident-memory and SFTP contracts are implemented in Steps 6-8 and
executed only after that exact fence is committed, reviewed, pushed, and synchronized
in C7B/C8; Step 5 does not invoke handlers that do not exist yet.

The memory-envelope scenario has two mandatory full-size legs. First, a 96 GiB/no-swap
cgroup uses the exact incident configuration and must reject the incompatible 64 GiB
clean + 16 GiB volatile profile before opening listeners. Second, a 128 GiB/no-swap
cgroup fills the real 64 GiB clean cache, exercises the real 16 GiB volatile tier, and
then sustains replacement/GC overlap while concurrently running a hard NFSv3 write
with delayed replies/retransmits, native 9P and WebUI requests, segment sealing, and GC. Sample
`memory.current`, `memory.events`, allocator metrics, every resident owner, protocol
in-flight bytes/ops, cache replacement, and GC working bytes at one-second resolution.
Acceptance requires peak current below the finite limit by the configured reserve,
zero `oom`/`oom_kill` deltas, bounded `high` events, every owner reconciling to the
aggregate together with idle baseline and unowned residual within the receipt's
explicit tolerance, all clients completing exact
SHA-256/durability checks, and cleanup of the scope and cache roots. `dirty_ram = 0`
alone is explicitly rejected.

The SFTP scenarios build official HPN tag `hpn-18.9.0` at immutable commit
`e2dfa0cea55d93747f4c68b4a2b134d6fbe0db06` entirely beneath
`$RESOURCE_ROOT/tools/hpn-ssh`, install it beneath `$RESOURCE_ROOT/opt/hpn-ssh`, and
record source SHA, binary SHA-256, and `ssh -V`. They never replace stock SSH. Using
one frozen incompressible fixture and one disposable remote prefix, run both directions
with stock `/usr/bin/ssh`, the pinned HPN executable, ZeroFS selecting stock, and
ZeroFS selecting HPN at one session and configured-many sessions. Hold endpoint,
port, key, host-key policy, cipher, fixture order, durability, and time window constant.
Record RTT, TCP send/receive windows and retransmits, SFTP outstanding request depth,
session waits, per-session bytes/rate, aggregate bytes/rate, CPU, exact bytes, SHA-256,
and remote/local durability. HPN may be selected only for a repeatable winning receive
path. Upload acceptance requires measured pipelining and every configured write lane
carrying bytes; a receive-window change cannot satisfy it.

The handler requires build dependencies to exist and fails without changing APT or
repository configuration. Its exact source/build/install fence is:

```bash
command -v autoreconf make cc >/dev/null
git clone --no-checkout https://github.com/rapier1/hpn-ssh.git "$RESOURCE_ROOT/tools/hpn-ssh"
git -C "$RESOURCE_ROOT/tools/hpn-ssh" checkout --detach e2dfa0cea55d93747f4c68b4a2b134d6fbe0db06
test "$(git -C "$RESOURCE_ROOT/tools/hpn-ssh" rev-parse HEAD)" = e2dfa0cea55d93747f4c68b4a2b134d6fbe0db06
mkdir -p "$RESOURCE_ROOT/build/hpn-ssh" "$RESOURCE_ROOT/opt/hpn-ssh"
(cd "$RESOURCE_ROOT/tools/hpn-ssh" && autoreconf -fvi)
(cd "$RESOURCE_ROOT/build/hpn-ssh" && "$RESOURCE_ROOT/tools/hpn-ssh/configure" --prefix="$RESOURCE_ROOT/opt/hpn-ssh")
make -C "$RESOURCE_ROOT/build/hpn-ssh" -j"$(nproc)"
make -C "$RESOURCE_ROOT/build/hpn-ssh" tests
make -C "$RESOURCE_ROOT/build/hpn-ssh" install-nokeys
test -x "$RESOURCE_ROOT/opt/hpn-ssh/bin/ssh"
"$RESOURCE_ROOT/opt/hpn-ssh/bin/ssh" -V
```

- [ ] **Step 6: Refactor the maintained read primitives and add RED gates**

Move only protocol-neutral strict pieces from `vm100_pilot/real_world_matrix.py` and `raw_sftp.py` into `scripts/benchmarking`: immutable cell definitions, fio argv/result parsing, exact byte/request validation, SHA-256 manifest parsing, pinned SFTP command construction, and worker reaping. Keep the legacy VM100 commands consuming those same primitives so historical callers do not fork behavior. The UUID-ledgered tiered harness owns lifecycle, protocols, cache state, receipts, and cleanup.

`ssh_transport.py` owns executable identity, direction/session cell definitions, safe
argv construction, per-process/socket sampling, and result validation. It accepts an
already ledger-validated executable path and never discovers or mutates host SSH
configuration. `memory_benchmark.py` owns cgroup sampling and owner reconciliation;
`sftp_benchmark.py` owns the real matrix lifecycle and remote-prefix cleanup.

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
- a zero `oom_kill` delta being omitted, owner totals not reconciling, or dirty RAM
  being substituted for resident-memory proof;
- an HPN result without exact executable identity, a direction label, or per-session
  byte evidence;
- an upload improvement attributed only to a larger client receive window;
- a global SSH/PATH/update-alternatives mutation or a surviving HPN build/install root.

The memory suite names
`test_missing_cgroup_events_is_rejected`,
`test_dirty_ram_is_not_resident_proof`,
`test_owner_totals_must_reconcile`, and
`test_cleanup_removes_cgroup_and_restores_sysfs`. The SFTP suite names
`test_missing_executable_identity_is_rejected`,
`test_upload_cannot_claim_receive_window_only`,
`test_each_configured_session_must_carry_bytes`, and
`test_cleanup_removes_hpn_and_remote_prefix`.

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
for test_file in test_memory_envelope_benchmark.py test_sftp_transport_benchmark.py
do
  RED_LOG="${TMPDIR:-/tmp}/${test_file%.py}-red.log"
  if python3 -m unittest discover -s scripts/tests -p "$test_file" -v 2>&1 | tee "$RED_LOG"; then
    echo "expected ${test_file} RED suite to fail before implementation" >&2
    exit 1
  fi
  grep -Eq '^Ran [1-9][0-9]* tests? in ' "$RED_LOG"
done
```

Expected RED: the read, resident-memory, and SFTP scenario modules do not exist.

- [ ] **Step 7: Implement the exact read, resident-memory, and SFTP matrices**

Implement concrete `memory-envelope-nfs-retransmit-gc`,
`sftp-stock-vs-hpn-download`, `sftp-stock-vs-hpn-upload`, and
`zerofs-sftp-session-scaling` handlers and register them in C2's `SCENARIOS` map.
Each handler must launch its real clients/processes and produce its typed receipt; a
handler that only validates arguments or writes a receipt fails the registry contract.

For configured usable read-session ceiling `P`, sweep concurrency
`{1, ceil(P/2), P, P+1}` over one frozen incompressible fixture manifest. On VM100,
`benchmark-read-throughput` executes only the raw SFTP plus NFS/9P cells; it has no NBD
code path and rejects an NBD device argument. On the separate
`ZEROFS_NBD_PROOF_HOST`, `benchmark-read-throughput-nbd` executes only the three NBD/XFS
cache-state cells. The raw SFTP cell is a paired directional backend control using the
same fixture size/order/time window; only ZeroFS process/cache tiers receive
remote-cold/clean-SSD/clean-RAM labels because the harness cannot purge or truthfully
label the Storage Box provider's internal cache.

1. raw OpenSSH SFTP download control;
2. kernel NFSv3 remote-cold, clean-SSD, and clean-RAM;
3. native shipping 9P remote-cold, clean-SSD, and clean-RAM;
4. separate-host `nbd-client` plus disposable XFS direct-read remote-cold, clean-SSD,
   and clean-RAM under `benchmark-read-throughput-nbd`.

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

Every cell additionally requires exact bytes, protocol-visible SHA-256, unchanged typed durability floors, zero dirty tiers/terminal errors/leaked reservations/temp objects, and idempotent ledger cleanup. Memory cells also require a finite cgroup limit, reserve, owner reconciliation, and zero OOM deltas. SFTP cells require exact executable identity, direction, TCP/request-depth evidence, and per-session bytes. Run only on isolated Ubuntu resources at the exact pushed SHA.

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
python3 -m unittest discover -s scripts/tests -p 'test_memory_envelope_benchmark.py' -v
python3 -m unittest discover -s scripts/tests -p 'test_sftp_transport_benchmark.py' -v
python3 -m unittest discover -s scripts/tests -p 'test_*.py' -v
git diff --check
git add scripts/benchmarking/__init__.py scripts/benchmarking/read_matrix.py scripts/benchmarking/raw_sftp.py scripts/benchmarking/ssh_transport.py scripts/tiered_writeback_e2e/read_benchmark.py scripts/tiered_writeback_e2e/memory_benchmark.py scripts/tiered_writeback_e2e/sftp_benchmark.py scripts/tiered_writeback_e2e/protocols.py scripts/tiered_writeback_e2e/integrity.py scripts/tiered_writeback_e2e/resources.py scripts/tiered-writeback-e2e.py scripts/vm100_pilot/real_world_matrix.py scripts/vm100_pilot/raw_sftp.py scripts/tests/test_real_world_matrix.py scripts/tests/test_vm100_pilot.py scripts/tests/test_tiered_writeback_e2e.py scripts/tests/test_read_throughput_benchmark.py scripts/tests/test_memory_envelope_benchmark.py scripts/tests/test_sftp_transport_benchmark.py
git commit -m "bench: prove tiered memory and transport performance"
```

Do not run a post-commit read benchmark from C7: its setup ledgers have already been cleaned and it owns no surviving materialized-control receipt. C8 is the sole execution authority for the paired read benchmark and creates fresh control and candidate ledgers after review, push, and literal-SHA synchronization.

The existing historical NBD/raw-SFTP scripts remain developer controls, not final acceptance authority. No previously reported NFS/9P number is accepted unless reproduced by this committed ledgered runner.

---

### Task C7B: Land the Evidence-Driven SSH/SFTP Shipping Result

**Files:**
- Modify after a measured upload RED: `zerofs/src/config.rs`
- Modify after a measured upload RED: `zerofs/src/sftp_transport.rs`
- Modify after a measured upload RED: `zerofs/src/segment_store.rs`
- Modify after an HPN win: `proxmox/deploy.py`
- Modify after an HPN win: `proxmox/templates/zerofs-prod.toml.example`
- Create after an HPN win: `packaging/hpn-ssh/build.sh`
- Create after an HPN win: `scripts/tests/test_hpn_packaging.py`
- Modify: `scripts/tests/test_sftp_transport_benchmark.py`

**Interfaces:**
- Produces: either an immutable stock-parity receipt or a pinned HPN packaging/configuration slice; when upload trails raw same-session control, it also produces the minimum measured request-depth/session-scheduling correction and rerun receipt.
- Consumes: C7 stock/HPN/ZeroFS upload/download receipts, paired noise bands, A22 `SshProgram`, strict SFTP pool limits, and the no-CT198-deploy boundary.

- [ ] **Step 1: Promote and run the first real A/B**

Commit C7, review/push it, synchronize the literal SHA to Ubuntu, then run
`sftp-stock-vs-hpn-download`, `sftp-stock-vs-hpn-upload`, and
`zerofs-sftp-session-scaling` through `supervise`. A decision receipt records paired
median/MAD bands and exactly one of:

- `stock_parity`: HPN does not win outside noise and ZeroFS matches the same-session
  raw control; keep stock default and do not package HPN;
- `hpn_receive_winner`: pinned HPN download wins outside noise; execute Step 2;
- `zerofs_upload_gap`: ZeroFS upload is slower than the same-session raw control outside
  noise or configured write lanes carry zero bytes; execute Step 3;
- both winner conditions, in which case execute Steps 2 and 3.

A missing/inconclusive decision reruns the A/B; it cannot select stock by default.
The raw upload control requests SFTP fsync (`sftp -f`) and uses the same number of
physical sessions as ZeroFS, so the decision never compares durable staged publication
with an unflushed seven-session control.

- [ ] **Step 2: Package a proven HPN winner without replacing system SSH**

`packaging/hpn-ssh/build.sh` accepts only commit
`e2dfa0cea55d93747f4c68b4a2b134d6fbe0db06`, builds/tests it under a release-owned
ledger resource root, and emits a relocatable package plus manifest and binary SHA-256
under `$RESOURCE_ROOT/packages/hpn-ssh-e2dfa0cea55d9`. The isolated proof executes
that staged binary in place; it writes nothing under `/srv`. During a later separately
approved production deployment only, `proxmox/deploy.py` verifies the package receipt,
installs it at
`/srv/zerofs-persist/tools/hpn-ssh/e2dfa0cea55d93747f4c68b4a2b134d6fbe0db06/bin/ssh`,
and renders the exact absolute `[sftp].ssh_program`; rollback selects the prior
release/config. It
never changes `/usr/bin/ssh`, alternatives, PATH, or sshd. Python tests reject another
commit, missing `make tests`, an unverified digest, a mutable/current symlink, and any
system-SSH mutation. This changes deploy artifacts only; it does not run deploy or
restart CT198.

- [ ] **Step 3: Correct a measured ZeroFS upload scheduling gap**

The observed production trace already shows healthy 255 KiB/64-request per-session
geometry but only about 1.75 of four data sessions active. Ordinary epoch/counter-
unique segment objects enter writeback as `PutMode::Overwrite`, so the scheduler treats
each as an ordering fence; synthetic scheduler benchmarks use only `PutMode::Create`
and miss the real path. Write RED tests
`ordinary_immutable_segment_uses_create_mode`,
`create_collision_fails_closed_without_overwrite`, and
`real_segment_path_fills_configured_upload_lanes`. Change only immutable uniquely named
segments to `PutMode::Create`; an unexpected collision fails closed and never overwrites
remote bytes. Extend the scheduler benchmark through the real `SegmentStore` path and
require every configured lane to carry bytes. Only if the rerun still shows insufficient
per-session request depth may a separate RED add bounded
`[sftp].max_inflight_requests_per_session`. Do not change segment geometry, connection
count, and request depth simultaneously. Rerun focused SFTP tests, workspace tests,
then the complete three-scenario A/B from a new literal pushed SHA.

- [ ] **Step 4: Commit the conditional exact fence and rerun Plan A/C7 gates**

```bash
cd /Volumes/bigssd/projects/ZeroFS/.worktrees/unified-tiered-writeback
python3 -m unittest discover -s scripts/tests -p 'test_sftp_transport_benchmark.py' -v
python3 -m unittest discover -s proxmox/tests -p 'test_*.py' -v
if test -e scripts/tests/test_hpn_packaging.py; then
  python3 -m unittest scripts.tests.test_hpn_packaging -v
  shellcheck packaging/hpn-ssh/build.sh
fi
cd zerofs
cargo_test_nonzero 'sftp_transport::tests' -p zerofs --locked
cargo_test_nonzero 'segment_store::tests' -p zerofs --locked
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo test --workspace --all-targets --locked
cd ..
git diff --check
```

When the decision includes `hpn_receive_winner`, run the actual packaging build on
isolated Ubuntu under a fresh ledger before the A/B rerun:

```bash
RUN_UUID="$(python3 -c 'import uuid; print(uuid.uuid4())')"
CONTROL_ROOT="/var/tmp/zerofs-tiered-control-${RUN_UUID}"
RESOURCE_ROOT="/var/tmp/zerofs-tiered-resources-${RUN_UUID}"
LEDGER="${CONTROL_ROOT}/ledger.json"
primary=0; cleanup=0
sudo python3 scripts/tiered-writeback-e2e.py setup --ledger "$LEDGER" --control-root "$CONTROL_ROOT" --resource-root "$RESOURCE_ROOT" --source-sha "$SLICE_SHA" --filesystem-ack-mode materialized --object-ack-mode remote || primary=$?
if test "$primary" -eq 0; then
  packaging/hpn-ssh/build.sh --source-commit e2dfa0cea55d93747f4c68b4a2b134d6fbe0db06 --work-root "$RESOURCE_ROOT/build/hpn-ssh" --output-root "$RESOURCE_ROOT/packages/hpn-ssh-e2dfa0cea55d9" || primary=$?
fi
if test "$primary" -eq 0; then
  test -x "$RESOURCE_ROOT/packages/hpn-ssh-e2dfa0cea55d9/bin/ssh" || primary=$?
  "$RESOURCE_ROOT/packages/hpn-ssh-e2dfa0cea55d9/bin/ssh" -V || primary=$?
  python3 scripts/tests/test_hpn_packaging.py --manifest "$RESOURCE_ROOT/packages/hpn-ssh-e2dfa0cea55d9/manifest.json" || primary=$?
fi
sudo python3 scripts/tiered-writeback-e2e.py cleanup --ledger "$LEDGER" || cleanup=$?
sudo python3 scripts/tiered-writeback-e2e.py cleanup --ledger "$LEDGER" || cleanup=$?
sudo python3 scripts/tiered-writeback-e2e.py assert-clean --ledger "$LEDGER" || cleanup=$?
test "$primary" -eq 0 && test "$cleanup" -eq 0
```

Stage only files selected by the typed decision receipt and commit `perf(sftp): land
the measured transport result`. Review, push, synchronize the new literal SHA, and
rerun all three A/B scenarios. A packaged winner must show ZeroFS actually spawning
the ledger-staged pinned executable in the isolated Ubuntu receipt; installation under
`/srv` remains deferred to a separately approved deployment. A dormant selector, benchmark-
only HPN binary, or unrepeated upload tuning does not complete C7B.

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
  sudo python3 scripts/tiered-writeback-e2e.py supervise --ledger "$LEDGER" --control-root "$CONTROL_ROOT" --resource-root "$RESOURCE_ROOT" --source-sha "$FINAL_PROOF_SHA" --filesystem-ack-mode "$filesystem_mode" --object-ack-mode "$object_mode" --scenario "$scenario"
}
run_final_scenario volatile_memory memory ninep-fsync-covers-prior-nfs
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
run_final_scenario volatile_memory memory crash-boundary-matrix
run_final_scenario volatile_memory ssd local-receipt-restart
run_final_scenario volatile_memory remote remote-receipt-clean-cache-restart
run_final_scenario volatile_memory ssd terminal-fanout-and-shutdown-timeout
run_final_scenario volatile_memory memory benchmark-ram-ack
run_final_scenario volatile_memory ssd benchmark-local-ssd
run_final_scenario volatile_memory remote benchmark-paced-remote
run_final_scenario volatile_memory ssd benchmark-4gib-foreground-isolation
run_final_scenario volatile_memory ssd benchmark-100gib-ram-to-ssd-transition
run_final_scenario volatile_memory ssd benchmark-ssd-pressure-to-remote-pacing
run_final_scenario volatile_memory ssd memory-envelope-nfs-retransmit-gc
run_final_scenario volatile_memory remote sftp-stock-vs-hpn-download
run_final_scenario volatile_memory remote sftp-stock-vs-hpn-upload
run_final_scenario volatile_memory remote zerofs-sftp-session-scaling
```

Run `global-admission-nbd-nfs-ninep`,
`cross-adapter-pending-read-same-backing-inode`,
`nfs-commit-covers-prior-nbd`, `nbd-flush-covers-prior-ninep`,
`xfs-over-nbd-restart`, and `zfs-over-nbd-restart` through the separate proof host
using the same fail-closed host denylist, exact `FINAL_PROOF_SHA`, fresh per-scenario
ledgers, and status-preserving `supervise` contract from C3/C4. `run_final_scenario`
above is intentionally incapable of dispatching NBD on VM100.

Run the paired read benchmark separately because the candidate must consume the
immutable materialized-control receipt:

```bash
READ_CONTROL_UUID="$(python3 -c 'import uuid; print(uuid.uuid4())')"
READ_CONTROL_ROOT="/var/tmp/zerofs-tiered-control-${READ_CONTROL_UUID}"
READ_CONTROL_RESOURCES="/var/tmp/zerofs-tiered-resources-${READ_CONTROL_UUID}"
READ_CONTROL_LEDGER="${READ_CONTROL_ROOT}/ledger.json"
sudo python3 scripts/tiered-writeback-e2e.py supervise --ledger "$READ_CONTROL_LEDGER" --control-root "$READ_CONTROL_ROOT" --resource-root "$READ_CONTROL_RESOURCES" --source-sha "$FINAL_PROOF_SHA" --filesystem-ack-mode materialized --object-ack-mode ssd --scenario benchmark-read-throughput
MATERIALIZED_RECEIPT="$(python3 scripts/tiered-writeback-e2e.py ledger-value --ledger "$READ_CONTROL_LEDGER" --key latest_receipt)"
test -s "$MATERIALIZED_RECEIPT"
case "$MATERIALIZED_RECEIPT" in "$READ_CONTROL_ROOT"/*) ;; *) exit 1 ;; esac

READ_CANDIDATE_UUID="$(python3 -c 'import uuid; print(uuid.uuid4())')"
READ_CANDIDATE_ROOT="/var/tmp/zerofs-tiered-control-${READ_CANDIDATE_UUID}"
READ_CANDIDATE_RESOURCES="/var/tmp/zerofs-tiered-resources-${READ_CANDIDATE_UUID}"
READ_CANDIDATE_LEDGER="${READ_CANDIDATE_ROOT}/ledger.json"
sudo python3 scripts/tiered-writeback-e2e.py supervise --ledger "$READ_CANDIDATE_LEDGER" --control-root "$READ_CANDIDATE_ROOT" --resource-root "$READ_CANDIDATE_RESOURCES" --source-sha "$FINAL_PROOF_SHA" --filesystem-ack-mode volatile_memory --object-ack-mode ssd --scenario benchmark-read-throughput --control-receipt "$MATERIALIZED_RECEIPT"
sudo python3 scripts/tiered-writeback-e2e.py archive-control --ledger "$READ_CANDIDATE_LEDGER" --archive-root /fast/zerofs-tiered-receipts
sudo python3 scripts/tiered-writeback-e2e.py archive-control --ledger "$READ_CONTROL_LEDGER" --archive-root /fast/zerofs-tiered-receipts
test ! -e "$READ_CANDIDATE_ROOT"
test ! -e "$READ_CONTROL_ROOT"
```

Run the NBD/XFS read pair only on the separate proof host:

```bash
test -n "${ZEROFS_NBD_PROOF_HOST:-}"
case "$ZEROFS_NBD_PROOF_HOST" in ubuntu-main|vm100|100.125.144.4|ct198|10.10.10.55|100.108.226.83) exit 1 ;; esac
VM100_MACHINE_ID="$(ssh ubuntu-main 'cat /etc/machine-id')"
CT198_MACHINE_ID="$(ssh root@100.108.226.83 'pct exec 198 -- cat /etc/machine-id')"
NBD_PROOF_IDENTITY_TOKEN="$(python3 scripts/tiered-writeback-e2e.py verify-proof-host --ssh-host "$ZEROFS_NBD_PROOF_HOST" --expected-host-key-sha256 "$ZEROFS_NBD_PROOF_HOST_KEY_SHA256" --expected-machine-id "$ZEROFS_NBD_PROOF_MACHINE_ID" --expected-proxmox-vmid "$ZEROFS_NBD_PROOF_VMID" --forbid-machine-id "$VM100_MACHINE_ID" --forbid-machine-id "$CT198_MACHINE_ID" --forbid-proxmox-vmid 100 --forbid-proxmox-vmid 198 --format token)"
case "$NBD_PROOF_IDENTITY_TOKEN" in ''|*[!A-Za-z0-9_-]*) exit 1 ;; esac
ssh "$ZEROFS_NBD_PROOF_HOST" "FINAL_PROOF_SHA='$FINAL_PROOF_SHA' PROOF_HOST_IDENTITY='$NBD_PROOF_IDENTITY_TOKEN' bash -seuo pipefail" <<'REMOTE'
cd /fast/projects/ZeroFS-unified-tiered-writeback
test "$(git rev-parse HEAD)" = "$FINAL_PROOF_SHA"
CONTROL_UUID="$(python3 -c 'import uuid; print(uuid.uuid4())')"
CONTROL_ROOT="/var/tmp/zerofs-tiered-control-${CONTROL_UUID}"
CONTROL_RESOURCES="/var/tmp/zerofs-tiered-resources-${CONTROL_UUID}"
CONTROL_LEDGER="${CONTROL_ROOT}/ledger.json"
sudo python3 scripts/tiered-writeback-e2e.py supervise --ledger "$CONTROL_LEDGER" --control-root "$CONTROL_ROOT" --resource-root "$CONTROL_RESOURCES" --source-sha "$FINAL_PROOF_SHA" --proof-host-identity "$PROOF_HOST_IDENTITY" --filesystem-ack-mode materialized --object-ack-mode ssd --scenario benchmark-read-throughput-nbd
CONTROL_RECEIPT="$(python3 scripts/tiered-writeback-e2e.py ledger-value --ledger "$CONTROL_LEDGER" --key latest_receipt)"
test -s "$CONTROL_RECEIPT"
CANDIDATE_UUID="$(python3 -c 'import uuid; print(uuid.uuid4())')"
CANDIDATE_ROOT="/var/tmp/zerofs-tiered-control-${CANDIDATE_UUID}"
CANDIDATE_RESOURCES="/var/tmp/zerofs-tiered-resources-${CANDIDATE_UUID}"
CANDIDATE_LEDGER="${CANDIDATE_ROOT}/ledger.json"
sudo python3 scripts/tiered-writeback-e2e.py supervise --ledger "$CANDIDATE_LEDGER" --control-root "$CANDIDATE_ROOT" --resource-root "$CANDIDATE_RESOURCES" --source-sha "$FINAL_PROOF_SHA" --proof-host-identity "$PROOF_HOST_IDENTITY" --filesystem-ack-mode volatile_memory --object-ack-mode ssd --scenario benchmark-read-throughput-nbd --control-receipt "$CONTROL_RECEIPT"
sudo python3 scripts/tiered-writeback-e2e.py archive-control --ledger "$CANDIDATE_LEDGER" --archive-root /fast/zerofs-tiered-receipts
sudo python3 scripts/tiered-writeback-e2e.py archive-control --ledger "$CONTROL_LEDGER" --archive-root /fast/zerofs-tiered-receipts
test ! -e "$CANDIDATE_ROOT"
test ! -e "$CONTROL_ROOT"
REMOTE
```

No command in this step runs on macOS. Any correction repeats portable GREEN, exact commit/review, push, literal-SHA synchronization, and this affected Linux command.

- [ ] **Step 3: Perform the requirement-to-evidence and cleanup audits**

Map every approved-spec requirement to task, commit SHA, command, host, CWD, flags, receipt path, and result. Separately map every ledger resource—including cgroup/scope, sysfs original/restoration, SSH/HPN source/build/install and child PID, cache root, Prometheus sample, and remote SFTP prefix/temp object—to a successful `cleanup` and `assert-clean` receipt. Any missing mapping blocks completion.

- [ ] **Step 4: Run all code-quality reviews**

Run simplify, deslop, branch-scope-audit, low-value-churn-audit, TraceDecay code-health, TraceDecay Hawk, `thermo-nuclear-review`, and `thermo-nuclear-code-quality-review` against `origin/develop...HEAD`. Resolve every P0/P1/P2 and rerun the affected focused, full, Linux, and audit gates.

---

### Task C9: Clean Resources, Merge, Push, Fast-Forward Ubuntu, and Remove Secondary Worktrees

**Files:**
- Git history/worktree state and external ledger-owned resources only

- [ ] **Step 1: Run idempotent cleanup and global ownership audit**

Enumerate every ledger from the append-only run index rather than shell history. The
command emits `host<TAB>ledger`; for each row, run the following block on that exact
host. The archived index must contain no unarchived entry before merge:

```bash
: > "/tmp/zerofs-ledgers-${FINAL_PROOF_SHA}.tsv"
for proof_host in ubuntu-main "$ZEROFS_NBD_PROOF_HOST"
do
  ssh "$proof_host" "cd /fast/projects/ZeroFS-unified-tiered-writeback && python3 scripts/tiered-writeback-e2e.py list-ledgers --campaign codex-unified-tiered-writeback --format host-tsv" >> "/tmp/zerofs-ledgers-${FINAL_PROOF_SHA}.tsv"
done
sort -u -o "/tmp/zerofs-ledgers-${FINAL_PROOF_SHA}.tsv" "/tmp/zerofs-ledgers-${FINAL_PROOF_SHA}.tsv"
test -s "/tmp/zerofs-ledgers-${FINAL_PROOF_SHA}.tsv"
while IFS="$(printf '\t')" read -r ledger_host ledger_path
do
  test -n "$ledger_host" && test -n "$ledger_path"
  ssh "$ledger_host" "LEDGER='$ledger_path' bash -seuo pipefail" <<'REMOTE'
cd /fast/projects/ZeroFS-unified-tiered-writeback
sudo python3 scripts/tiered-writeback-e2e.py cleanup --ledger "$LEDGER"
sudo python3 scripts/tiered-writeback-e2e.py cleanup --ledger "$LEDGER"
sudo python3 scripts/tiered-writeback-e2e.py assert-clean --ledger "$LEDGER"
sudo python3 scripts/tiered-writeback-e2e.py archive-control --ledger "$LEDGER" --archive-root /fast/zerofs-tiered-receipts
REMOTE
done < "/tmp/zerofs-ledgers-${FINAL_PROOF_SHA}.tsv"
for proof_host in ubuntu-main "$ZEROFS_NBD_PROOF_HOST"
do
  ssh "$proof_host" "cd /fast/projects/ZeroFS-unified-tiered-writeback && python3 scripts/tiered-writeback-e2e.py list-ledgers --campaign codex-unified-tiered-writeback --require-all-archived"
done
```

For each individual ledger receipt, the equivalent asserted sequence is:

```bash
sudo python3 scripts/tiered-writeback-e2e.py cleanup --ledger "$LEDGER"
sudo python3 scripts/tiered-writeback-e2e.py cleanup --ledger "$LEDGER"
sudo python3 scripts/tiered-writeback-e2e.py assert-clean --ledger "$LEDGER"
RUN_UUID="$(python3 scripts/tiered-writeback-e2e.py ledger-value --ledger "$LEDGER" --key run_uuid)"
sudo python3 scripts/tiered-writeback-e2e.py archive-control --ledger "$LEDGER" --archive-root /fast/zerofs-tiered-receipts
test ! -e "/var/tmp/zerofs-tiered-control-${RUN_UUID}"
test -s "/fast/zerofs-tiered-receipts/${RUN_UUID}/ledger.json"
```

Prove no UUID-owned mount/device/cgroup/scope/sysfs mutation/pool/filesystem/process/SSH child/HPN source-build-install root/listener/socket/cache-state root/prefix/temp object/resource/control root remains. Reread every ledgered sysfs path and require byte-for-byte equality with its original value before deleting the control authority. The preserved receipt archive is evidence, not a live test resource. Prove no active build/test/harness job exists before touching checkouts. Remove the separate `nfsserve` worktree only after its immutable pushed revision is pinned and its worktree is clean.

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
ssh ubuntu-main "EXPECTED_OLD_SHA='$EXPECTED_OLD_SHA' EXPECTED_NEW_SHA='$EXPECTED_NEW_SHA' bash -seuo pipefail" <<'REMOTE'
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

Then Root removes only `/Volumes/bigssd/projects/ZeroFS/.worktrees/unified-tiered-writeback`, prunes, and lists local worktrees. The completion receipt records the authorized historical deployment and the later automatic post-OOM restart, then explicitly proves no task in this feature plan deployed or restarted CT198 afterward; it does not falsely claim CT198 was never restarted.

```bash
cd /Volumes/bigssd/projects/ZeroFS
test -z "$(git -C /Volumes/bigssd/projects/ZeroFS/.worktrees/unified-tiered-writeback status --porcelain=v1)"
git worktree remove /Volumes/bigssd/projects/ZeroFS/.worktrees/unified-tiered-writeback
git worktree prune
git worktree list --porcelain
test ! -e /Volumes/bigssd/projects/ZeroFS/.worktrees/unified-tiered-writeback
test -z "$(git status --porcelain=v1)"
```

Finally inventory every remaining local and Ubuntu worktree with path, branch, HEAD,
porcelain, active process, merge-base, and containment in pushed `develop`. Remove an
eligible secondary lane only when it is clean, inactive, and its HEAD is an ancestor of
`develop`. This includes the planning lane after its documentation commit has been
integrated; dirty, active, or unmerged lanes are reported and preserved.

```bash
cd /Volumes/bigssd/projects/ZeroFS
git worktree list --porcelain
test -z "$(git -C /Volumes/bigssd/projects/ZeroFS/.worktrees/read-throughput-plan status --porcelain=v1)"
PLAN_SHA="$(git -C /Volumes/bigssd/projects/ZeroFS/.worktrees/read-throughput-plan rev-parse HEAD^{commit})"
git merge-base --is-ancestor "$PLAN_SHA" develop
git worktree remove /Volumes/bigssd/projects/ZeroFS/.worktrees/read-throughput-plan
git worktree prune
git worktree list --porcelain
test ! -e /Volumes/bigssd/projects/ZeroFS/.worktrees/read-throughput-plan
ssh ubuntu-main 'cd /fast/projects/ZeroFS && git worktree list --porcelain'
```
