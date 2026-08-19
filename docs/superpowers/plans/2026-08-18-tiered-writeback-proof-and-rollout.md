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
- An alias denylist is insufficient proof-host identity. Before any NBD worktree or device command, `verify-proof-host` separately validates a controller-reachable SSH target and pins the machine's immutable SSH host-key SHA-256, exact `/etc/machine-id`, and provisioned `/etc/zerofs-proxmox-vmid`; it compares them with controller-owned, independently verified VM100 and CT198 identity receipts and requires the expected VMID to be neither 100 nor 198. The receipt paths and pinned SHA-256 digests arrive as `ZEROFS_VM100_IDENTITY_RECEIPT`, `ZEROFS_VM100_IDENTITY_RECEIPT_SHA256`, `ZEROFS_CT198_IDENTITY_RECEIPT`, and `ZEROFS_CT198_IDENTITY_RECEIPT_SHA256`. This rollout never connects to CT198 to refresh or validate those receipts. Every NBD ledger retains both the immutable machine-identity receipt and the separately validated controller SSH target; neither may substitute for the other.
- Every process, port, mount, device, cgroup/scope, changed sysfs value, SSH executable/source/build/install root, filesystem/pool name, object prefix/temp object, cache/state directory, scratch directory, and tool checkout is unique and recorded in one UUID resource ledger.
- The immutable ledger and cleanup receipts live in `CONTROL_ROOT=/var/tmp/zerofs-tiered-control-$RUN_UUID`; disposable processes, mounts, devices, data, scratch, and tool checkouts live in the separate `RESOURCE_ROOT=/var/tmp/zerofs-tiered-resources-$RUN_UUID`. Cleanup never deletes its own authority.
- Cleanup is idempotent after success, failure, partial setup, cancellation, supervisor cancellation, and crash.
- Keep failure receipts; remove only exact ledger-owned resources.
- Stock OpenSSH remains installed and unchanged. A pinned HPN executable lives only under a ledger-owned resource root until a separate explicit promotion; global `PATH`, `update-alternatives`, `/usr/bin/ssh`, and host SSH configuration are immutable.
- `supervise` is the only supported setup/run/cleanup entry point for C3, C4, C7, and C7B. It owns setup, the registered scenario handler, TERM/KILL deadlines, double cleanup, and `assert-clean`; it records primary and cleanup statuses separately and returns the primary failure first, otherwise the cleanup failure. Composite recipes are registered scenarios, never ad hoc boolean chains or caller-owned traps. Portable tests inject failures at setup, every primary substep, timeout, first cleanup, second cleanup, and `assert-clean` and prove no later success masks them.

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
CONTROLLER_TARGET_RECEIPT="$(python3 scripts/tiered-writeback-e2e.py verify-controller-target --controller-ssh-target ubuntu-main --expected-host-key-sha256 "${ZEROFS_VM100_HOST_KEY_SHA256:?}" --format target-token)"
case "$CONTROLLER_TARGET_RECEIPT" in ''|*[!A-Za-z0-9_-]*) exit 1 ;; esac
ssh ubuntu-main "SLICE_SHA='$SLICE_SHA' CONTROLLER_TARGET_RECEIPT='$CONTROLLER_TARGET_RECEIPT' bash -seuo pipefail" <<'REMOTE'
cd /fast/projects/ZeroFS
git fetch origin codex/unified-tiered-writeback
test "$(git rev-parse origin/codex/unified-tiered-writeback)" = "$SLICE_SHA"
test -d /fast/projects/ZeroFS-unified-tiered-writeback || git worktree add --detach /fast/projects/ZeroFS-unified-tiered-writeback "$SLICE_SHA"
cd /fast/projects/ZeroFS-unified-tiered-writeback
test -z "$(git status --porcelain=v1)"
python3 scripts/tiered-writeback-e2e.py assert-source-idle --source-root /fast/projects/ZeroFS-unified-tiered-writeback
git switch --detach "$SLICE_SHA"
test "$(git rev-parse HEAD)" = "$SLICE_SHA"
test -z "$(git status --porcelain=v1)"
REMOTE
```

The controller passes `CONTROLLER_TARGET_RECEIPT` into every later `ubuntu-main`
remote shell; it is never inferred from Ubuntu's hostname. The slice receipt records
the literal expanded `SLICE_SHA` before any Ubuntu command. A Linux-discovered failure
returns to RED/implementation/portable GREEN/commit/review, creates a new literal
`SLICE_SHA`, and repeats this full block before the failed Linux command is rerun.

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
- Produces: `setup`, `list-scenarios`, `verify-controller-target`, `validate-recorded-host-identity`, `verify-proof-host`, `run`, `supervise`, `cleanup --ledger`, `assert-clean --ledger`, `ledger-value`, `validate-owned-path`, `archive-control`, `list-ledgers --campaign`, and `assert-source-idle` commands plus JSON receipts.
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

Name tests for rejecting `/`, `/mnt`, `/var/tmp`, equal/nested control and resource roots, workspace roots, CT198/production strings, unowned PIDs/devices/mounts/ports/cgroups/sysfs paths/SSH binaries, missing dual ack flags, receipt omission, partial setup, primary-plus-cleanup errors, cleanup preserving ledger authority, repeated cleanup after resource-root deletion, final receipt archiving, supervisor cancellation, a successful cleanup masking a failed primary scenario, each failed composite substep followed by successful cleanup, an alternate alias whose pinned host key/machine ID is VM100 or CT198, an unprovisioned/mismatched Proxmox VMID marker, a controller SSH target that is unreachable or resolves to a different immutable identity, an unknown scenario, a registered scenario without a real handler, a documented scenario missing from the registry, an absent/unknown `ledger-value` key, and `validate-owned-path` receiving a relative path, symlink escape, traversal, control-root path, or unledgered resource path.

The lifecycle/API suite names
`test_supervise_preserves_each_composite_primary_failure`,
`test_supervise_reports_cleanup_failure_after_primary_success`,
`test_supervise_timeout_terminates_and_reaps_process_group`,
`test_ledger_value_rejects_unknown_or_nonscalar_key`,
`test_ledger_value_receipt_requires_manifest_validation`,
`test_validate_owned_path_rejects_symlink_escape`,
`test_validate_archived_receipt_requires_campaign_manifest`,
`test_list_ledgers_requires_one_archived_decision_authority`, and
`test_list_ledgers_rejects_host_derived_or_unvalidated_controller_target`,
`test_controller_target_must_resolve_to_immutable_identity`,
`test_recorded_host_identity_requires_pinned_digest_role_and_vmid`,
`test_recorded_host_identity_rejects_live_ct198_refresh`, and
`test_controller_target_normalized_output_matches_token`, and
`test_every_ubuntu_remote_block_receives_validated_target_token`, and
`test_every_cleanup_remote_block_mints_and_transports_fresh_target_token`. Each supervisor test
injects the failure, asserts the returned status and separate ledger fields, then
asserts both cleanup attempts and `assert-clean` ran.

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

For UUID `RUN_UUID`, setup requires
`CONTROL_ROOT=/var/tmp/zerofs-tiered-control-$RUN_UUID`,
`RESOURCE_ROOT=/var/tmp/zerofs-tiered-resources-$RUN_UUID`,
`LEDGER=$CONTROL_ROOT/ledger.json`, `RECEIPT_ROOT=$CONTROL_ROOT/receipts`, and campaign
`codex-unified-tiered-writeback`; control and resource roots must be disjoint siblings.
Identity/config fields are immutable and every later event is hash-chained append-only.
Setup atomically appends host, source SHA, UUID, and ledger path to
`/fast/zerofs-tiered-receipts/in-progress/codex-unified-tiered-writeback.jsonl`.
The JSON receipt records both ack fields, source HEAD, binary/config hashes, both roots,
exact resources, HPN/tool identities, scenario/manifest, typed durability floors,
resident-memory/cgroup samples, terminal state, commands, and independent statuses.

`supervise` executes one registered real scenario, always performs cleanup twice plus
`assert-clean`, records primary and cleanup statuses separately, and exits with the
primary failure first or cleanup failure otherwise. `cleanup --ledger PATH` removes
only ledger-owned resources and is idempotent. `assert-clean --ledger PATH` reads the
surviving external authority and rejects any recorded survivor.

`ledger-value --ledger PATH [--receipt VALIDATED_PATH] --key KEY` reads only an
allowlisted immutable/scenario-output scalar from the ledger or a receipt already
covered by that ledger's live/archive manifest, rejects missing/unknown/object values,
and emits one newline-terminated value without shell quoting. `validate-owned-path --ledger PATH
--path PATH` resolves every existing ancestor without following a final symlink,
rejects traversal/symlink escape, and succeeds only for an exact ledgered resource, a
live receipt under `CONTROL_ROOT`, or an archived receipt whose hash and original
control-relative path match the campaign archive manifest. Callers select
`--kind resource|receipt|archived-receipt`.

`list-ledgers --campaign codex-unified-tiered-writeback` reads the persistent run index.
`--format controller-target-ledger-tsv` loads each ledger, verifies its target receipt
against the live/archive manifest, validates the normalized target schema, and emits
exactly `normalized_target<TAB>ledger`. It rejects a hostname copied from the index,
caller, current machine, or SSH discovery host when no manifest-covered target receipt
exists. `--decision-authority c7b --archived
--require-one --format ledger-path` returns exactly one manifest-validated C7B decision
ledger or fails. `archive-control` copies ledger/receipts to
`/fast/zerofs-tiered-receipts/$RUN_UUID`, verifies hashes, marks the run-index entry
archived, and only then removes `CONTROL_ROOT`. `assert-source-idle --source-root PATH`
inspects `/proc/*/cwd` and fails for active build/test/harness jobs rooted at `PATH`.

`verify-proof-host --controller-ssh-target TARGET --format identity-token` first proves
the controller can reach `TARGET` under the pinned host-key policy, then returns
canonical unpadded base64url JSON containing only immutable machine identity
`{host_key_algorithm:"ssh-ed25519",host_key_sha256,machine_id,proxmox_vmid}`. A second
invocation with `--format target-token` returns separate canonical base64url JSON
`{normalized_target,identity_sha256,verified_at}`. These are receipts, not bearer
credentials or signatures; there is no invented signing key. Callers reject either
token outside `[A-Za-z0-9_-]+` before placing it in an SSH environment assignment.
Before allocation, remote `setup` decodes both with strict schema/canonical-form checks,
hashes the identity token and compares it to `identity_sha256`, recomputes the SHA-256
fingerprint of `/etc/ssh/ssh_host_ed25519_key.pub`, and rereads `/etc/machine-id` and
`/etc/zerofs-proxmox-vmid`; every byte must match. It records identity and controller
target separately. A target rename creates a new target receipt but never changes the
machine identity.

For non-NBD hosts, `verify-controller-target --controller-ssh-target TARGET
--expected-host-key-sha256 SHA256 --format target-token` emits the same canonical
target receipt without a VMID identity claim. Every `supervise` invocation requires
`--controller-ssh-target-receipt`; setup validates canonical form and host-key binding,
stores the normalized target separately from local hostname, and adds the receipt hash
to the manifest before allocating resources. The NBD proof-host target receipt remains
additionally bound to its immutable identity token as specified above.
Both `verify-controller-target` and `verify-proof-host` also accept
`--format normalized-target`; they execute the identical live verification and emit
only the validated target string contained in the corresponding fresh target token.
The output must match `[-A-Za-z0-9._:@]+` and is used only to map a manifest-validated
ledger target back to its freshly reverified transport token during C9 cleanup.

`validate-recorded-host-identity --receipt PATH --expected-sha256 HEX
--expected-role vm100|ct198 --expected-proxmox-vmid 100|198 --format machine-id`
accepts only a regular controller-local file whose exact bytes match the lowercase
64-hex digest. It validates canonical JSON schema
`{role,host_key_algorithm,host_key_sha256,machine_id,proxmox_vmid,observed_at,
independent_evidence_sha256}`, rejects symlinks, duplicate/unknown fields, role/VMID
mismatch, invalid machine IDs, and any command/source field that names a live shell or
container refresh. The receipt is prior evidence only: this rollout neither creates nor
refreshes it. The command emits only the validated machine ID. The harness source and
tests contain no live CT198 access path.

Every controller-to-`ubuntu-main` recipe creates a fresh `target-token` with
`verify-controller-target`, rejects characters outside `[A-Za-z0-9_-]`, and passes it
as the literal `CONTROLLER_TARGET_RECEIPT` environment value on the same quoted SSH
command that starts a single-quoted heredoc. Remote recipes never inherit, infer, or
reconstruct the token, and tests parse every documented Ubuntu command block to prove
the explicit transport is present before any `setup`, `run`, or `supervise` call.

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
    "rust-tier-microbenchmarks",
    "benchmark-read-throughput",
    "benchmark-read-throughput-nbd",
    "memory-envelope-nfs-retransmit-gc",
    "sftp-stock-vs-hpn-download",
    "sftp-stock-vs-hpn-upload",
    "zerofs-sftp-session-scaling",
    "sftp-transport-decision",
    "hpn-package-winner",
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
test "$(wc -l < "${TMPDIR:-/tmp}/zerofs-scenarios.list" | tr -d ' ')" -eq 36
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

- [ ] **Step 1: Review, push, identify, and synchronize the committed harness SHA**

Run the required promotion block. From the controller, fail closed unless the separate
NBD proof host is reachable and its transport target resolves to the expected immutable
machine identity:

```bash
set -euo pipefail
cd /Volumes/bigssd/projects/ZeroFS/.worktrees/unified-tiered-writeback
SLICE_SHA="$(git rev-parse HEAD^{commit})"
test "${#SLICE_SHA}" = 40
test -n "${ZEROFS_NBD_PROOF_HOST:-}"
case "$ZEROFS_NBD_PROOF_HOST" in ubuntu-main|vm100|100.125.144.4|ct198|10.10.10.55|100.108.226.83) exit 1 ;; esac
VM100_MACHINE_ID="$(python3 scripts/tiered-writeback-e2e.py validate-recorded-host-identity --receipt "${ZEROFS_VM100_IDENTITY_RECEIPT:?}" --expected-sha256 "${ZEROFS_VM100_IDENTITY_RECEIPT_SHA256:?}" --expected-role vm100 --expected-proxmox-vmid 100 --format machine-id)"
CT198_MACHINE_ID="$(python3 scripts/tiered-writeback-e2e.py validate-recorded-host-identity --receipt "${ZEROFS_CT198_IDENTITY_RECEIPT:?}" --expected-sha256 "${ZEROFS_CT198_IDENTITY_RECEIPT_SHA256:?}" --expected-role ct198 --expected-proxmox-vmid 198 --format machine-id)"
NBD_PROOF_IDENTITY_TOKEN="$(python3 scripts/tiered-writeback-e2e.py verify-proof-host --controller-ssh-target "$ZEROFS_NBD_PROOF_HOST" --expected-host-key-sha256 "$ZEROFS_NBD_PROOF_HOST_KEY_SHA256" --expected-machine-id "$ZEROFS_NBD_PROOF_MACHINE_ID" --expected-proxmox-vmid "$ZEROFS_NBD_PROOF_VMID" --forbid-machine-id "$VM100_MACHINE_ID" --forbid-machine-id "$CT198_MACHINE_ID" --forbid-proxmox-vmid 100 --forbid-proxmox-vmid 198 --format identity-token)"
NBD_PROOF_TARGET_TOKEN="$(python3 scripts/tiered-writeback-e2e.py verify-proof-host --controller-ssh-target "$ZEROFS_NBD_PROOF_HOST" --expected-host-key-sha256 "$ZEROFS_NBD_PROOF_HOST_KEY_SHA256" --expected-machine-id "$ZEROFS_NBD_PROOF_MACHINE_ID" --expected-proxmox-vmid "$ZEROFS_NBD_PROOF_VMID" --forbid-machine-id "$VM100_MACHINE_ID" --forbid-machine-id "$CT198_MACHINE_ID" --forbid-proxmox-vmid 100 --forbid-proxmox-vmid 198 --format target-token)"
case "$NBD_PROOF_IDENTITY_TOKEN" in ''|*[!A-Za-z0-9_-]*) exit 1 ;; esac
case "$NBD_PROOF_TARGET_TOKEN" in ''|*[!A-Za-z0-9_-]*) exit 1 ;; esac
ssh "$ZEROFS_NBD_PROOF_HOST" "SLICE_SHA='$SLICE_SHA' PROOF_HOST_IDENTITY='$NBD_PROOF_IDENTITY_TOKEN' CONTROLLER_TARGET_RECEIPT='$NBD_PROOF_TARGET_TOKEN' bash -seuo pipefail" <<'REMOTE'
cd /fast/projects/ZeroFS
git fetch origin codex/unified-tiered-writeback
test "$(git rev-parse origin/codex/unified-tiered-writeback)" = "$SLICE_SHA"
if ! test -d /fast/projects/ZeroFS-unified-tiered-writeback; then
  git worktree add --detach /fast/projects/ZeroFS-unified-tiered-writeback "$SLICE_SHA"
fi
cd /fast/projects/ZeroFS-unified-tiered-writeback
test -z "$(git status --porcelain=v1)"
python3 scripts/tiered-writeback-e2e.py assert-source-idle --source-root /fast/projects/ZeroFS-unified-tiered-writeback
git switch --detach "$SLICE_SHA"
test "$(git rev-parse HEAD)" = "$SLICE_SHA"
REMOTE
```

- [ ] **Step 2: Run every volatile scenario through the tested supervisor**

Each invocation gets a fresh external control/resource root. `supervise` performs setup,
the complete registered handler, double cleanup, and `assert-clean`, and preserves the
first failing status. No caller runs `setup`, `run`, or `cleanup` directly:

```bash
ssh "$ZEROFS_NBD_PROOF_HOST" "SLICE_SHA='$SLICE_SHA' PROOF_HOST_IDENTITY='$NBD_PROOF_IDENTITY_TOKEN' CONTROLLER_TARGET_RECEIPT='$NBD_PROOF_TARGET_TOKEN' bash -seuo pipefail" <<'REMOTE'
cd /fast/projects/ZeroFS-unified-tiered-writeback
run_c3() {
  filesystem_mode="$1"
  object_mode="$2"
  scenario="$3"
  run_uuid="$(python3 -c 'import uuid; print(uuid.uuid4())')"
  control_root="/var/tmp/zerofs-tiered-control-${run_uuid}"
  resource_root="/var/tmp/zerofs-tiered-resources-${run_uuid}"
  ledger="${control_root}/ledger.json"
  sudo python3 scripts/tiered-writeback-e2e.py supervise --ledger "$ledger" --control-root "$control_root" --resource-root "$resource_root" --source-sha "$SLICE_SHA" --proof-host-identity "$PROOF_HOST_IDENTITY" --controller-ssh-target-receipt "$CONTROLLER_TARGET_RECEIPT" --filesystem-ack-mode "$filesystem_mode" --object-ack-mode "$object_mode" --scenario "$scenario"
}
run_c3 volatile_memory memory global-admission-nbd-nfs-ninep
run_c3 volatile_memory memory cross-adapter-pending-read-same-backing-inode
run_c3 volatile_memory memory nfs-commit-covers-prior-nbd
run_c3 volatile_memory memory ninep-fsync-covers-prior-nfs
run_c3 volatile_memory memory nbd-flush-covers-prior-ninep
run_c3 volatile_memory memory webui-rpc-production-path
REMOTE
```

The same-backing-inode scenario provisions an NBD member as a normal ZeroFS inode
reachable by the direct namespace, pauses canonical materialization, writes through the
live NBD server, and reads that exact inode through mounted NFS and 9P. It does not
claim guest-XFS namespace unification.

- [ ] **Step 3: Run materialized and object-target controls through the same supervisor**

Use the identical `run_c3` helper in a new strict remote shell:

```bash
ssh "$ZEROFS_NBD_PROOF_HOST" "SLICE_SHA='$SLICE_SHA' PROOF_HOST_IDENTITY='$NBD_PROOF_IDENTITY_TOKEN' CONTROLLER_TARGET_RECEIPT='$NBD_PROOF_TARGET_TOKEN' bash -seuo pipefail" <<'REMOTE'
cd /fast/projects/ZeroFS-unified-tiered-writeback
run_c3() {
  filesystem_mode="$1"
  object_mode="$2"
  scenario="$3"
  run_uuid="$(python3 -c 'import uuid; print(uuid.uuid4())')"
  control_root="/var/tmp/zerofs-tiered-control-${run_uuid}"
  resource_root="/var/tmp/zerofs-tiered-resources-${run_uuid}"
  ledger="${control_root}/ledger.json"
  sudo python3 scripts/tiered-writeback-e2e.py supervise --ledger "$ledger" --control-root "$control_root" --resource-root "$resource_root" --source-sha "$SLICE_SHA" --proof-host-identity "$PROOF_HOST_IDENTITY" --controller-ssh-target-receipt "$CONTROLLER_TARGET_RECEIPT" --filesystem-ack-mode "$filesystem_mode" --object-ack-mode "$object_mode" --scenario "$scenario"
}
run_c3 materialized ssd protocol-materialized-control
run_c3 materialized remote protocol-durability-target-control
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

- [ ] **Step 1: Synchronize the literal reviewed SHA**

Run the required promotion block. `linux_suites.py` owns every exact clone, checkout,
build, mount, suite command, result file, and teardown below; callers never allocate
suite resources outside `supervise`. It pins xfstests
`1ae822c1c2e2364e966085cee3ce4a97b2500241`, pjdfstest
`85a8aea9e685999ef0540392fd80535f873d7ff7`, pjdfstest_nfs
`7d3d7cb0cdc5d39eedd995771bc1d4b3dabf31ab`, and Linux 6.18 archive SHA-256
`9106a4605da9e31ff17659d958782b815f9591ab308d03b0ee21aad6c7dced4b`.

- [ ] **Step 2: Run every NFS/9P workflow through the tested supervisor**

From the controller, run the NFS/9P-only body on `ubuntu-main` with the validated
target token transported on the same SSH invocation:

```bash
set -euo pipefail
cd /Volumes/bigssd/projects/ZeroFS/.worktrees/unified-tiered-writeback
SLICE_SHA="$(git rev-parse HEAD^{commit})"
CONTROLLER_TARGET_RECEIPT="$(python3 scripts/tiered-writeback-e2e.py verify-controller-target --controller-ssh-target ubuntu-main --expected-host-key-sha256 "${ZEROFS_VM100_HOST_KEY_SHA256:?}" --format target-token)"
case "$CONTROLLER_TARGET_RECEIPT" in ''|*[!A-Za-z0-9_-]*) exit 1 ;; esac
ssh ubuntu-main "SLICE_SHA='$SLICE_SHA' CONTROLLER_TARGET_RECEIPT='$CONTROLLER_TARGET_RECEIPT' bash -seuo pipefail" <<'REMOTE'
cd /fast/projects/ZeroFS-unified-tiered-writeback
test "$(git rev-parse HEAD)" = "$SLICE_SHA"
run_c4() {
  scenario="$1"
  run_uuid="$(python3 -c 'import uuid; print(uuid.uuid4())')"
  control_root="/var/tmp/zerofs-tiered-control-${run_uuid}"
  resource_root="/var/tmp/zerofs-tiered-resources-${run_uuid}"
  ledger="${control_root}/ledger.json"
  sudo python3 scripts/tiered-writeback-e2e.py supervise --ledger "$ledger" --control-root "$control_root" --resource-root "$resource_root" --source-sha "$SLICE_SHA" --controller-ssh-target-receipt "${CONTROLLER_TARGET_RECEIPT:?}" --filesystem-ack-mode volatile_memory --object-ack-mode ssd --scenario "$scenario"
}
run_c4 xfstests-nfs-quick
run_c4 xfstests-ninep-quick-and-strict
run_c4 pjdfstest-nfs
run_c4 pjdfstest-ninep
run_c4 stress-ng-nfs-ninep
run_c4 kernel-compile-nfs
run_c4 kernel-compile-ninep
REMOTE
```

Each registered handler records its nonzero test inventory before execution. The NFS
pjdfstest leg uses `.github/.pjdfstest-nfs-exclude`, the 9P leg uses
`.github/.pjdfstest-9p-exclude`, strict 9P executes `generic/732`, and the kernel handlers
compile and verify a real `vmlinux` on the mounted protocol. A suite failure, timeout,
or result-parse failure remains the supervisor's primary status even when cleanup passes.

- [ ] **Step 3: Validate the separate NBD host and run XFS/ZFS through the supervisor**

Reuse C3's controller-side immutable identity verification and exact-SHA synchronization,
then run both NBD scenarios from a strict shell on that host:

```bash
ssh "$ZEROFS_NBD_PROOF_HOST" "SLICE_SHA='$SLICE_SHA' PROOF_HOST_IDENTITY='$NBD_PROOF_IDENTITY_TOKEN' CONTROLLER_TARGET_RECEIPT='$NBD_PROOF_TARGET_TOKEN' bash -seuo pipefail" <<'REMOTE'
cd /fast/projects/ZeroFS-unified-tiered-writeback
run_c4_nbd() {
  scenario="$1"
  run_uuid="$(python3 -c 'import uuid; print(uuid.uuid4())')"
  control_root="/var/tmp/zerofs-tiered-control-${run_uuid}"
  resource_root="/var/tmp/zerofs-tiered-resources-${run_uuid}"
  ledger="${control_root}/ledger.json"
  sudo python3 scripts/tiered-writeback-e2e.py supervise --ledger "$ledger" --control-root "$control_root" --resource-root "$resource_root" --source-sha "$SLICE_SHA" --proof-host-identity "$PROOF_HOST_IDENTITY" --controller-ssh-target-receipt "$CONTROLLER_TARGET_RECEIPT" --filesystem-ack-mode volatile_memory --object-ack-mode ssd --scenario "$scenario"
}
run_c4_nbd xfs-over-nbd-restart
run_c4_nbd zfs-over-nbd-restart
REMOTE
```

No command in this task runs on macOS. VM100 never receives an NBD device or mount.

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
CONTROLLER_TARGET_RECEIPT="$(python3 scripts/tiered-writeback-e2e.py verify-controller-target --controller-ssh-target ubuntu-main --expected-host-key-sha256 "${ZEROFS_VM100_HOST_KEY_SHA256:?}" --format target-token)"
case "$CONTROLLER_TARGET_RECEIPT" in ''|*[!A-Za-z0-9_-]*) exit 1 ;; esac
ssh ubuntu-main "CRASH_SHA='$CRASH_SHA' CONTROLLER_TARGET_RECEIPT='$CONTROLLER_TARGET_RECEIPT' bash -seuo pipefail" <<'REMOTE'
cd /fast/projects/ZeroFS
git fetch origin codex/unified-tiered-writeback
test "$(git rev-parse origin/codex/unified-tiered-writeback)" = "$CRASH_SHA"
cd /fast/projects/ZeroFS-unified-tiered-writeback
test -z "$(git status --porcelain=v1)"
python3 scripts/tiered-writeback-e2e.py assert-source-idle --source-root /fast/projects/ZeroFS-unified-tiered-writeback
git switch --detach "$CRASH_SHA"
test "$(git rev-parse HEAD)" = "$CRASH_SHA"
REMOTE
```

- [ ] **Step 7: Run real Ubuntu proof only from `CRASH_SHA`**

Each invocation below creates its own external control root and disposable resource root:

```bash
set -euo pipefail
cd /Volumes/bigssd/projects/ZeroFS/.worktrees/unified-tiered-writeback
CRASH_SHA="$(git rev-parse HEAD^{commit})"
CONTROLLER_TARGET_RECEIPT="$(python3 scripts/tiered-writeback-e2e.py verify-controller-target --controller-ssh-target ubuntu-main --expected-host-key-sha256 "${ZEROFS_VM100_HOST_KEY_SHA256:?}" --format target-token)"
case "$CONTROLLER_TARGET_RECEIPT" in ''|*[!A-Za-z0-9_-]*) exit 1 ;; esac
ssh ubuntu-main "CRASH_SHA='$CRASH_SHA' CONTROLLER_TARGET_RECEIPT='$CONTROLLER_TARGET_RECEIPT' bash -seuo pipefail" <<'REMOTE'
cd /fast/projects/ZeroFS-unified-tiered-writeback
test "$(git rev-parse HEAD)" = "$CRASH_SHA"
run_crash_scenario() {
  object_mode="$1"; scenario="$2"
  RUN_UUID="$(python3 -c 'import uuid; print(uuid.uuid4())')"
  CONTROL_ROOT="/var/tmp/zerofs-tiered-control-${RUN_UUID}"
  RESOURCE_ROOT="/var/tmp/zerofs-tiered-resources-${RUN_UUID}"
  LEDGER="${CONTROL_ROOT}/ledger.json"
  sudo python3 scripts/tiered-writeback-e2e.py supervise --ledger "$LEDGER" --control-root "$CONTROL_ROOT" --resource-root "$RESOURCE_ROOT" --source-sha "$CRASH_SHA" --controller-ssh-target-receipt "${CONTROLLER_TARGET_RECEIPT:?}" --filesystem-ack-mode volatile_memory --object-ack-mode "$object_mode" --scenario "$scenario"
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
REMOTE
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

- [ ] **Step 2: Run the exact Rust microbenchmark recipe through the supervisor**

`rust-tier-microbenchmarks` is a registered composite scenario. Its handler obtains
`local_ssd_scratch` only through `ledger-value`, validates it with
`validate-owned-path --kind resource`, lists the five exact ignored tests below, and
then executes each with `--ignored --exact --nocapture` under a per-test deadline. It
requires exactly one passing test per invocation; zero or multiple tests fail the
primary scenario:

```text
writeback::store::tests::bench_writeback_tier_profile
writeback::journaler::tests::drain_throughput_of_the_post_ack_durability_tail
writeback::store::tests::bench_remote_replay_throughput_against_throttled_backend
writeback::journal::tests::publication_batch_size_amortizes_the_journal_fixed_cost
writeback::journal::tests::remote_commit_serialization_cost_bounds_replay_throughput
```

Run it only through `supervise`:

```bash
set -euo pipefail
cd /Volumes/bigssd/projects/ZeroFS/.worktrees/unified-tiered-writeback
SLICE_SHA="$(git rev-parse HEAD^{commit})"
CONTROLLER_TARGET_RECEIPT="$(python3 scripts/tiered-writeback-e2e.py verify-controller-target --controller-ssh-target ubuntu-main --expected-host-key-sha256 "${ZEROFS_VM100_HOST_KEY_SHA256:?}" --format target-token)"
case "$CONTROLLER_TARGET_RECEIPT" in ''|*[!A-Za-z0-9_-]*) exit 1 ;; esac
ssh ubuntu-main "SLICE_SHA='$SLICE_SHA' CONTROLLER_TARGET_RECEIPT='$CONTROLLER_TARGET_RECEIPT' bash -seuo pipefail" <<'REMOTE'
cd /fast/projects/ZeroFS-unified-tiered-writeback
test "$(git rev-parse HEAD)" = "$SLICE_SHA"
run_uuid="$(python3 -c 'import uuid; print(uuid.uuid4())')"
control_root="/var/tmp/zerofs-tiered-control-${run_uuid}"
resource_root="/var/tmp/zerofs-tiered-resources-${run_uuid}"
ledger="${control_root}/ledger.json"
sudo python3 scripts/tiered-writeback-e2e.py supervise --ledger "$ledger" --control-root "$control_root" --resource-root "$resource_root" --source-sha "$SLICE_SHA" --controller-ssh-target-receipt "${CONTROLLER_TARGET_RECEIPT:?}" --filesystem-ack-mode volatile_memory --object-ack-mode ssd --scenario rust-tier-microbenchmarks
REMOTE
```

The supervisor owns cancellation and cleanup if listing, scratch validation, any Rust
test, or receipt parsing fails.

- [ ] **Step 3: Run real protocol benchmarks and integrity gates**

Each benchmark command uses a separate ledger whose `setup` command has the same two ack flags; cleanup twice and `assert-clean` complete before the next ledger is created.

```bash
set -euo pipefail
cd /Volumes/bigssd/projects/ZeroFS/.worktrees/unified-tiered-writeback
SLICE_SHA="$(git rev-parse HEAD^{commit})"
CONTROLLER_TARGET_RECEIPT="$(python3 scripts/tiered-writeback-e2e.py verify-controller-target --controller-ssh-target ubuntu-main --expected-host-key-sha256 "${ZEROFS_VM100_HOST_KEY_SHA256:?}" --format target-token)"
case "$CONTROLLER_TARGET_RECEIPT" in ''|*[!A-Za-z0-9_-]*) exit 1 ;; esac
ssh ubuntu-main "SLICE_SHA='$SLICE_SHA' CONTROLLER_TARGET_RECEIPT='$CONTROLLER_TARGET_RECEIPT' bash -seuo pipefail" <<'REMOTE'
cd /fast/projects/ZeroFS-unified-tiered-writeback
test "$(git rev-parse HEAD)" = "$SLICE_SHA"
run_benchmark_scenario() {
  object_mode="$1"; scenario="$2"
  RUN_UUID="$(python3 -c 'import uuid; print(uuid.uuid4())')"
  CONTROL_ROOT="/var/tmp/zerofs-tiered-control-${RUN_UUID}"
  RESOURCE_ROOT="/var/tmp/zerofs-tiered-resources-${RUN_UUID}"
  LEDGER="${CONTROL_ROOT}/ledger.json"
  sudo python3 scripts/tiered-writeback-e2e.py supervise --ledger "$LEDGER" --control-root "$CONTROL_ROOT" --resource-root "$RESOURCE_ROOT" --source-sha "$SLICE_SHA" --controller-ssh-target-receipt "${CONTROLLER_TARGET_RECEIPT:?}" --filesystem-ack-mode volatile_memory --object-ack-mode "$object_mode" --scenario "$scenario"
}
run_benchmark_scenario memory benchmark-ram-ack
run_benchmark_scenario ssd benchmark-local-ssd
run_benchmark_scenario remote benchmark-paced-remote
run_benchmark_scenario ssd benchmark-4gib-foreground-isolation
run_benchmark_scenario ssd benchmark-100gib-ram-to-ssd-transition
run_benchmark_scenario ssd benchmark-ssd-pressure-to-remote-pacing
REMOTE
```

The production-shaped scenarios configure 16 GB shared dirty-write RAM, 64 GB clean read cache, a 1 TB local SSD tier split into the configured clean-cache and durable journal/staging budgets, and a 5 TB-class export without counting sparse virtual geometry as remote physical use. The 4 GiB leg must remain on RAM/local SSD and reject any unexplained collapse to remote rate. The 100 GiB leg records the RAM-to-SSD transition and concurrent remote drain. The pressure leg preconditions only its disposable SSD ledger resources near the configured dirty limit, then proves each ordered remote cleanup admits incremental foreground work without a 95-to-85-percent pause.

Each scenario records same-host durable local control and durability-matched same-endpoint raw SFTP control results. The production targets are approximately 800-900 MB/s local SSD and 70-100 MB/s raw SFTP; acceptance is paired to the measured control so a slower external path is diagnosed rather than concealed. Every receipt includes size/SHA-256/readback, mutation/object floors, dirty tiers, tier-transition timestamps, terminal state, CPU/RAM/local allocation, remote rate, and cleanup. Performance without integrity and durability is rejected.

The following resident-memory and SFTP contracts are implemented in Steps 4-6 and
executed only after that exact fence is committed, reviewed, pushed, and synchronized
in C7B/C8; Step 3 does not invoke handlers that do not exist yet.

The memory-envelope scenario has two mandatory full-size legs. First, a 96 GiB/no-swap
cgroup uses the exact incident configuration and must reject the incompatible 64 GiB
clean + 16 GiB volatile profile before opening listeners. Second, a 128 GiB/no-swap
cgroup fills the real 64 GiB clean cache, exercises the real 16 GiB volatile tier, and
then sustains replacement/GC overlap while concurrently running a hard NFSv3 write
with delayed replies/retransmits, native 9P and WebUI requests, segment sealing, and GC. Sample
`memory.current`, `memory.events`, allocator metrics, every resident owner, protocol
in-flight bytes/ops, cache replacement, and GC working bytes at one-second resolution.
OS RSS and cgroup current are the physical authorities. Record jemalloc allocated,
resident, and retained separately; retained is virtual address space and is never added
to resident or used alone to trigger admission. Include a retained-only growth/purge
leg that raises `stats.retained` without a matching RSS/cgroup increase and prove it
does not create false permanent over-cap or poison. The GC overlap includes one real
stored-threshold segment made from 8,192 deterministic incompressible 32 KiB frames and
one real maximally compressible stored-threshold segment produced by the current writer
and codec. The latter is expected to carry roughly 4.07 million rows, plus its real
crossing-batch overshoot, but acceptance uses parsed persisted geometry rather than the
estimate.
Receipts parse the persisted footer and require actual frame-region bytes, directory
bytes, total object bytes, footer `k`, and decoded row count; 256 MiB of plaintext or a
compressible repeated-byte plaintext total is not stored-size evidence.

The same gate enforces only real wire/host representability: footer `k`, sealed body
length, `FrameLoc.byte_len`, frame-index arithmetic, sealed-directory length, offsets,
and total length are checked before mutation or publication. Small-limit boundary tests
prove overflow leaves the open generation byte-for-byte unchanged and emits no object
PUT; no segment-size-derived cardinality cap is permitted.

Verification uses a streaming merge with a fixed per-page cap of 65,536 rows and 4 MiB
of encoded keys plus values,
one sequential live source stream across the memory/durable views, no per-frame point-read fanout,
and precharged directory/page working memory. The existing version-1 sealed directory is
authenticated before row release, decoded into permit-owned 4 MiB/65,536-row external
sort runs, and merged eight-way with a 128 MiB Zstd window or 64 KiB LZ4 history; one
stream stays within 144 MiB resident memory. Every 256-byte run header carries source,
generation, count, key-range, length, and SHA-256 identity, and every multipass input is
manifest-accounted. The canonical SSD owner reserves
`2*dir_len + 2*(k*28) + 2*ceil(k/65536)*256 + 4096` bytes before growth; no
whole-directory allocation or second disk counter is allowed.
The database source itself has one fetch task, no cache admission or forwarding task/
row channel, and one-block read ahead instead of the generic four-way prefetch. Page
counters reset inside that same direct-owned iterator; a post-fetch counter wrapped
around generic `Db::scan` is rejected as memory evidence.
Scratch is UUID-owned, reserved before growth, and cleaned on success/error/cancel and
startup recovery. There is deliberately no total row or page cap. At most 16 logical
ranges per view each use one source scan, so at most 32 source scans cover both views
while sparse ranges may contain arbitrarily many unrelated rows, and every
valid `u32` directory cardinality must make progress to EOF without reopening at page
boundaries. Errors, corruption, source reopen,
changed geometry, or live references return fail-closed `Keep`. The Linux scenario
inserts unrelated rows across many page boundaries, proves one source stream per range, and
reclaims both real threshold objects without a liveness-gating total budget.
All focused GREEN tests use fully qualified `cargo_test_nonzero`; a zero-selection Cargo
success is rejected. The receipt records real stored/object/directory measurements,
total pages/rows/bytes, peak concurrent scans/tasks, and peak permitted working bytes
before scoring reclamation. Typed fields require `page_rows_peak <= 65536`,
`page_encoded_bytes_peak <= 4194304`,
`logical_ranges_total <= 32`, `source_scans_total <= 32`,
`source_scan_tasks_peak <= 1`, `physical_pages_total > 1` for each sparse paging leg,
both-view EOF, zero point reads, `directory_rows_emitted_before_auth = 0`, fixed decoder
window/run/fan-in peaks, exact initial/merge counts and hashes, complete one-shot versus
multipass sorted-key equivalence, zero scratch reservation/residue, and successful deletion only after those
EOF markers.
Before the first process starts, the immutable ledger records these fixed thresholds:
`cgroup_high_event_delta_max=8`, `reconciliation_error_bytes_max=268435456`, and
`unowned_residual_bytes_max=2147483648`. They cannot be supplied by scenario output,
changed after setup, or derived from the observed peak. Acceptance requires peak
current below the finite limit by the configured reserve, zero `oom`/`oom_kill`
deltas, `high` delta at most eight, absolute error between `memory.current` and
`idle_baseline + charged_owners + unowned_residual` at most 256 MiB, unowned residual
at most 2 GiB, all clients completing exact
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

The handler requires build dependencies and GNU `timeout` to exist and fails without
changing APT or repository configuration. The pinned upstream `regress/Makefile`
defines `LTESTS` and its `SKIP_LTESTS` mechanism; the pinned `dynamic-forward.sh`
deliberately forks a background multiplexed client controlled by a socket. The package
proof excludes exactly `dynamic-forward` so no background forwarding child can escape
the test supervisor. This is a documented, digest-recorded exclusion, not a broad test
skip. The handler parses the exact pinned `LTESTS`, records its sorted inventory and
SHA-256, proves `dynamic-forward` occurs once, removes only that name, and rejects an
empty remaining suite. It requires one `run test NAME.sh` receipt for every remaining
entry. The review authority is the immutable upstream
[`regress/Makefile`](https://github.com/rapier1/hpn-ssh/blob/e2dfa0cea55d93747f4c68b4a2b134d6fbe0db06/regress/Makefile)
and
[`dynamic-forward.sh`](https://github.com/rapier1/hpn-ssh/blob/e2dfa0cea55d93747f4c68b4a2b134d6fbe0db06/regress/dynamic-forward.sh),
not the moving default branch. Its exact source/build/test/install fence is:

```bash
command -v autoreconf make cc timeout >/dev/null
git clone --no-checkout https://github.com/rapier1/hpn-ssh.git "$RESOURCE_ROOT/tools/hpn-ssh"
git -C "$RESOURCE_ROOT/tools/hpn-ssh" checkout --detach e2dfa0cea55d93747f4c68b4a2b134d6fbe0db06
test "$(git -C "$RESOURCE_ROOT/tools/hpn-ssh" rev-parse HEAD)" = e2dfa0cea55d93747f4c68b4a2b134d6fbe0db06
mkdir -p "$RESOURCE_ROOT/build/hpn-ssh" "$RESOURCE_ROOT/opt/hpn-ssh"
cd "$RESOURCE_ROOT/tools/hpn-ssh"
autoreconf -fvi
cd "$RESOURCE_ROOT/build/hpn-ssh"
"$RESOURCE_ROOT/tools/hpn-ssh/configure" --prefix="$RESOURCE_ROOT/opt/hpn-ssh"
make -C "$RESOURCE_ROOT/build/hpn-ssh" -j"$(nproc)"
timeout --signal=TERM --kill-after=30s 20m make -C "$RESOURCE_ROOT/build/hpn-ssh" tests SKIP_LTESTS=dynamic-forward
for focused_test in transfer rekey sftp sftp-batch sftp-resume forwarding
do
  timeout --signal=TERM --kill-after=15s 5m make -C "$RESOURCE_ROOT/build/hpn-ssh" t-exec LTESTS="$focused_test"
done
make -C "$RESOURCE_ROOT/build/hpn-ssh" install-nokeys
test -x "$RESOURCE_ROOT/opt/hpn-ssh/bin/hpnssh"
"$RESOURCE_ROOT/opt/hpn-ssh/bin/hpnssh" -V
```

The Python supervisor starts each test command in its own process group, ledgers the
leader and descendants, sends TERM at the fixed deadline, sends KILL after the fixed
grace, reaps every child, and then performs normal double cleanup plus `assert-clean`.
Unit tests reject a missing inventory, any exclusion other than the exact singleton
`dynamic-forward`, zero remaining tests, a missing focused transport name, a deadline
without TERM/KILL/reap evidence, or a full-suite success that lacks a receipt for any
non-excluded `LTESTS` entry.

- [ ] **Step 4: Refactor the maintained read primitives and add RED gates**

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
- missing pre-run cgroup thresholds, a post-run threshold mutation, `high` delta above
  eight, reconciliation error above 256 MiB, or unowned residual above 2 GiB;
- physical residency computed as jemalloc resident plus retained, retained-only growth
  causing backpressure, or a mismatch with OS RSS/cgroup current;
- deriving cardinality from plaintext bytes, treating a repeated-byte fixture as a
  256 MiB stored segment, or failing to assert footer `k`, `dir_offset`, `dir_len`, total
  object bytes, and decoded directory rows;
- a finite total cardinality, row, page, or encoded-byte cap that can make a
  valid version-1 segment permanently unreclaimable;
- unchecked `usize`/`u32` segment geometry, an overflow that mutates the open generation
  or publishes an object, or a boundary test that requires a GiB allocation instead of
  the production checked helper with injected limits;
- a page exceeding 65,536 rows, 4 MiB encoded keys plus values, or its concurrency
  budget, resuming without
  strict key progress, allocating directory/page working memory before its resident
  permit, or deleting before both views reach EOF for one immutable geometry identity;
- a generic cached/four-way-prefetch scan allocating ahead of the page budget, a
  forwarding task/channel, or a source stream reopened at a page boundary;
- a whole-directory allocation, unauthenticated row emission, unbounded codec history or
  external-sort fan-in, scratch growth without reservation, or owned scratch surviving
  success, error, cancellation, or startup recovery;
- a run without bound source/generation/count/range/hash identity, a merge that does not
  consume every manifest input exactly once, or any full-stream difference from the
  authenticated one-shot decoder across duplicates/corruption/cancellation;
- an HPN result without exact executable identity, a direction label, or per-session
  byte evidence;
- an HPN suite without the pinned `LTESTS` inventory, with an exclusion other than
  `dynamic-forward`, with zero remaining tests, without all six focused transport
  receipts, or without TERM/KILL/reap evidence after a deadline;
- an upload improvement attributed only to a larger client receive window;
- a decision authority composed from separate ledgers, mixed source SHAs, stale
  receipts, fewer than all three SFTP measurements, or receipt-only/no-client work;
- a global SSH/PATH/update-alternatives mutation or a surviving HPN build/install root.

The memory suite names
`test_missing_cgroup_events_is_rejected`,
`test_dirty_ram_is_not_resident_proof`,
`test_owner_totals_must_reconcile`, and
`test_cleanup_removes_cgroup_and_restores_sysfs`, plus
`test_memory_thresholds_must_be_fixed_before_run`,
`test_high_event_delta_above_eight_is_rejected`,
`test_reconciliation_error_above_256_mib_is_rejected`, and
`test_unowned_residual_above_2_gib_is_rejected`,
`test_retained_virtual_bytes_are_not_physical_rss`, and
`test_reclaim_page_rows_and_encoded_bytes_are_bounded`,
`test_reclaim_source_scans_are_bounded_and_pages_do_not_reopen`,
`test_reclaim_source_task_concurrency_is_one`,
`test_sparse_unrelated_rows_make_uncapped_multi_page_progress`, and
`test_reclaim_requires_both_view_eof_before_delete`. The SFTP suite names
`test_missing_executable_identity_is_rejected`,
`test_hpn_inventory_and_single_exclusion_are_required`,
`test_hpn_timeout_must_reap_process_group`,
`test_upload_cannot_claim_receive_window_only`,
`test_each_configured_session_must_carry_bytes`, and
`test_decision_handler_runs_all_three_current_sha_measurements`,
`test_decision_rejects_mixed_or_stale_measurements`,
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

- [ ] **Step 5: Implement the exact read, resident-memory, and SFTP matrices**

Implement concrete `memory-envelope-nfs-retransmit-gc`,
`sftp-stock-vs-hpn-download`, `sftp-stock-vs-hpn-upload`, and
`zerofs-sftp-session-scaling` handlers plus the composite
`sftp-transport-decision` handler and register them in C2's `SCENARIOS` map.
Each handler must launch its real clients/processes and produce its typed receipt; a
handler that only validates arguments or writes a receipt fails the registry contract.

`sftp-transport-decision` is the sole decision-authority producer. Inside one
`supervise` ledger and one source SHA, it creates one frozen incompressible fixture and
invokes the same real measurement functions used by the three focused scenarios in
this exact order: stock-versus-HPN download, stock-versus-HPN durability-matched upload,
then ZeroFS session scaling. It does not read another ledger or accept arbitrary
measurement receipt paths. Its typed receipt embeds all three measurement receipts,
requires each `source_sha` to equal the supervising ledger SHA, records exact bytes,
hashes, durability, per-session evidence and paired noise bands, then emits the HPN and
current upload-gap decision fields. A subprocess-free/receipt-only composite handler,
mixed SHA, missing cell, stale timestamp, or non-success measurement fails before an
authority receipt is written.

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

- [ ] **Step 6: Use paired control-derived acceptance and commit the exact benchmark fence**

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
MEMORY_TEST_LOG="${TMPDIR:-/tmp}/zerofs-memory-envelope-green.log"
python3 -m unittest discover -s scripts/tests -p 'test_memory_envelope_benchmark.py' -v 2>&1 | tee "$MEMORY_TEST_LOG"
grep -Eq '^Ran [1-9][0-9]* tests? in ' "$MEMORY_TEST_LOG"
grep -F 'test_reclaim_page_rows_and_encoded_bytes_are_bounded' "$MEMORY_TEST_LOG"
grep -F 'test_reclaim_source_scans_are_bounded_and_pages_do_not_reopen' "$MEMORY_TEST_LOG"
grep -F 'test_reclaim_source_task_concurrency_is_one' "$MEMORY_TEST_LOG"
grep -F 'test_sparse_unrelated_rows_make_uncapped_multi_page_progress' "$MEMORY_TEST_LOG"
grep -F 'test_reclaim_requires_both_view_eof_before_delete' "$MEMORY_TEST_LOG"
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

Commit C7, review/push it, synchronize the literal SHA to Ubuntu, then run only the
registered `sftp-transport-decision` composite through `supervise`. The three focused
scenario names remain diagnostic entry points; their separate ledgers can never be
combined into a decision authority. The composite receipt records paired median/MAD
bands and typed booleans `decision.hpn_receive_winner` and
`decision.zerofs_upload_gap_current`. A current gap executes Step 3. HPN win and an
upload gap may both be true, so both corrections execute. When no gap exists, the same
receipt proves ZeroFS parity with its same-session raw control before stock is retained.

```bash
set -euo pipefail
cd /Volumes/bigssd/projects/ZeroFS/.worktrees/unified-tiered-writeback
SLICE_SHA="$(git rev-parse HEAD^{commit})"
CONTROLLER_TARGET_RECEIPT="$(python3 scripts/tiered-writeback-e2e.py verify-controller-target --controller-ssh-target ubuntu-main --expected-host-key-sha256 "${ZEROFS_VM100_HOST_KEY_SHA256:?}" --format target-token)"
case "$CONTROLLER_TARGET_RECEIPT" in ''|*[!A-Za-z0-9_-]*) exit 1 ;; esac
ssh ubuntu-main "SLICE_SHA='$SLICE_SHA' CONTROLLER_TARGET_RECEIPT='$CONTROLLER_TARGET_RECEIPT' bash -seuo pipefail" <<'REMOTE'
cd /fast/projects/ZeroFS-unified-tiered-writeback
test "$(git rev-parse HEAD)" = "$SLICE_SHA"
run_uuid="$(python3 -c 'import uuid; print(uuid.uuid4())')"
control_root="/var/tmp/zerofs-tiered-control-${run_uuid}"
resource_root="/var/tmp/zerofs-tiered-resources-${run_uuid}"
decision_ledger="${control_root}/ledger.json"
sudo python3 scripts/tiered-writeback-e2e.py supervise --ledger "$decision_ledger" --control-root "$control_root" --resource-root "$resource_root" --source-sha "$SLICE_SHA" --controller-ssh-target-receipt "${CONTROLLER_TARGET_RECEIPT:?}" --filesystem-ack-mode volatile_memory --object-ack-mode remote --scenario sftp-transport-decision
decision_receipt="$(python3 scripts/tiered-writeback-e2e.py ledger-value --ledger "$decision_ledger" --key decision_receipt)"
python3 scripts/tiered-writeback-e2e.py validate-owned-path --ledger "$decision_ledger" --path "$decision_receipt" --kind receipt
test "$(python3 scripts/tiered-writeback-e2e.py ledger-value --ledger "$decision_ledger" --receipt "$decision_receipt" --key source_sha)" = "$SLICE_SHA"
REMOTE
```

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
`/srv/zerofs-persist/tools/hpn-ssh/e2dfa0cea55d93747f4c68b4a2b134d6fbe0db06/bin/hpnssh`,
and renders the exact absolute `[sftp].ssh_program`; rollback selects the prior
release/config. It
never changes `/usr/bin/ssh`, alternatives, PATH, or sshd. Python tests reject another
commit, a missing bounded full-suite inventory/receipt, a missing focused transport
receipt, an unverified digest, a mutable/current symlink, and any system-SSH mutation.
This changes deploy artifacts only; it does not run deploy or restart CT198.

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
The rerun is the same `sftp-transport-decision` composite under one fresh supervisor
ledger at the new SHA, not three independent `supervise` calls. It must report
`decision.zerofs_upload_gap_current=false` before C7B can complete.

- [ ] **Step 4: Commit the conditional exact fence and rerun Plan A/C7 gates**

```bash
set -euo pipefail
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
isolated Ubuntu under a fresh ledger before the A/B rerun. `hpn-package-winner` is a
registered scenario whose handler runs `packaging/hpn-ssh/build.sh`, validates the
staged executable and manifest, runs the bounded inventory/focused transport gates from
C7, and records the package digest. The caller uses only `supervise`:

```bash
set -euo pipefail
cd /Volumes/bigssd/projects/ZeroFS/.worktrees/unified-tiered-writeback
SLICE_SHA="$(git rev-parse HEAD^{commit})"
CONTROLLER_TARGET_RECEIPT="$(python3 scripts/tiered-writeback-e2e.py verify-controller-target --controller-ssh-target ubuntu-main --expected-host-key-sha256 "${ZEROFS_VM100_HOST_KEY_SHA256:?}" --format target-token)"
case "$CONTROLLER_TARGET_RECEIPT" in ''|*[!A-Za-z0-9_-]*) exit 1 ;; esac
ssh ubuntu-main "SLICE_SHA='$SLICE_SHA' CONTROLLER_TARGET_RECEIPT='$CONTROLLER_TARGET_RECEIPT' bash -seuo pipefail" <<'REMOTE'
cd /fast/projects/ZeroFS-unified-tiered-writeback
test "$(git rev-parse HEAD)" = "$SLICE_SHA"
run_uuid="$(python3 -c 'import uuid; print(uuid.uuid4())')"
control_root="/var/tmp/zerofs-tiered-control-${run_uuid}"
resource_root="/var/tmp/zerofs-tiered-resources-${run_uuid}"
ledger="${control_root}/ledger.json"
sudo python3 scripts/tiered-writeback-e2e.py supervise --ledger "$ledger" --control-root "$control_root" --resource-root "$resource_root" --source-sha "$SLICE_SHA" --controller-ssh-target-receipt "${CONTROLLER_TARGET_RECEIPT:?}" --filesystem-ack-mode materialized --object-ack-mode remote --scenario hpn-package-winner
REMOTE
```

Stage only files selected by the typed decision receipt and commit `perf(sftp): land
the measured transport result`. Review, push, synchronize the new literal SHA, and
rerun the composite decision scenario with `--decision-authority c7b`. A packaged winner must show ZeroFS actually spawning
the ledger-staged pinned executable in the isolated Ubuntu receipt; installation under
`/srv` remains deferred to a separately approved deployment. A dormant selector, benchmark-
only HPN binary, or unrepeated upload tuning does not complete C7B.

The final composite rerun writes all three current-SHA measurements, typed flags, and
receipt path into one ledger tagged `decision_authority=c7b`. Validate that live receipt with `validate-owned-path
--kind receipt`, require its `source_sha` to equal the synchronized correction SHA and
`decision.zerofs_upload_gap_current=false`, then run `archive-control`; the campaign index must contain exactly one
successful archived C7B authority after superseded decision ledgers are retained as
non-authoritative history. C8 discovers this authority through `list-ledgers`, never a
free-form decision environment value.

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

After Step 1 and all review fixes are committed, run the required promotion block and
record `FINAL_PROOF_SHA=$SLICE_SHA`. From the controller, transport a newly validated
target token into the Ubuntu gate:

```bash
set -euo pipefail
cd /Volumes/bigssd/projects/ZeroFS/.worktrees/unified-tiered-writeback
FINAL_PROOF_SHA="$(git rev-parse HEAD^{commit})"
CONTROLLER_TARGET_RECEIPT="$(python3 scripts/tiered-writeback-e2e.py verify-controller-target --controller-ssh-target ubuntu-main --expected-host-key-sha256 "${ZEROFS_VM100_HOST_KEY_SHA256:?}" --format target-token)"
case "$CONTROLLER_TARGET_RECEIPT" in ''|*[!A-Za-z0-9_-]*) exit 1 ;; esac
ssh ubuntu-main "FINAL_PROOF_SHA='$FINAL_PROOF_SHA' CONTROLLER_TARGET_RECEIPT='$CONTROLLER_TARGET_RECEIPT' bash -seuo pipefail" <<'REMOTE'
cd /fast/projects/ZeroFS-unified-tiered-writeback
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
REMOTE
```

Run every real harness scenario from its own external control/resource roots:

```bash
set -euo pipefail
cd /Volumes/bigssd/projects/ZeroFS/.worktrees/unified-tiered-writeback
FINAL_PROOF_SHA="$(git rev-parse HEAD^{commit})"
CONTROLLER_TARGET_RECEIPT="$(python3 scripts/tiered-writeback-e2e.py verify-controller-target --controller-ssh-target ubuntu-main --expected-host-key-sha256 "${ZEROFS_VM100_HOST_KEY_SHA256:?}" --format target-token)"
case "$CONTROLLER_TARGET_RECEIPT" in ''|*[!A-Za-z0-9_-]*) exit 1 ;; esac
ssh ubuntu-main "FINAL_PROOF_SHA='$FINAL_PROOF_SHA' CONTROLLER_TARGET_RECEIPT='$CONTROLLER_TARGET_RECEIPT' bash -seuo pipefail" <<'REMOTE'
cd /fast/projects/ZeroFS-unified-tiered-writeback
test "$(git rev-parse HEAD)" = "$FINAL_PROOF_SHA"
run_final_scenario() {
  filesystem_mode="$1"; object_mode="$2"; scenario="$3"
  RUN_UUID="$(python3 -c 'import uuid; print(uuid.uuid4())')"
  CONTROL_ROOT="/var/tmp/zerofs-tiered-control-${RUN_UUID}"
  RESOURCE_ROOT="/var/tmp/zerofs-tiered-resources-${RUN_UUID}"
  LEDGER="${CONTROL_ROOT}/ledger.json"
  sudo python3 scripts/tiered-writeback-e2e.py supervise --ledger "$LEDGER" --control-root "$CONTROL_ROOT" --resource-root "$RESOURCE_ROOT" --source-sha "$FINAL_PROOF_SHA" --controller-ssh-target-receipt "${CONTROLLER_TARGET_RECEIPT:?}" --filesystem-ack-mode "$filesystem_mode" --object-ack-mode "$object_mode" --scenario "$scenario"
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
run_final_scenario volatile_memory ssd rust-tier-microbenchmarks
run_final_scenario volatile_memory ssd memory-envelope-nfs-retransmit-gc
FINAL_SFTP_UUID="$(python3 -c 'import uuid; print(uuid.uuid4())')"
FINAL_SFTP_CONTROL="/var/tmp/zerofs-tiered-control-${FINAL_SFTP_UUID}"
FINAL_SFTP_RESOURCES="/var/tmp/zerofs-tiered-resources-${FINAL_SFTP_UUID}"
FINAL_SFTP_LEDGER="${FINAL_SFTP_CONTROL}/ledger.json"
sudo python3 scripts/tiered-writeback-e2e.py supervise --ledger "$FINAL_SFTP_LEDGER" --control-root "$FINAL_SFTP_CONTROL" --resource-root "$FINAL_SFTP_RESOURCES" --source-sha "$FINAL_PROOF_SHA" --controller-ssh-target-receipt "${CONTROLLER_TARGET_RECEIPT:?}" --filesystem-ack-mode volatile_memory --object-ack-mode remote --scenario sftp-transport-decision --decision-authority final-c8
FINAL_SFTP_RECEIPT="$(python3 scripts/tiered-writeback-e2e.py ledger-value --ledger "$FINAL_SFTP_LEDGER" --key decision_receipt)"
python3 scripts/tiered-writeback-e2e.py validate-owned-path --ledger "$FINAL_SFTP_LEDGER" --path "$FINAL_SFTP_RECEIPT" --kind receipt
test "$(python3 scripts/tiered-writeback-e2e.py ledger-value --ledger "$FINAL_SFTP_LEDGER" --receipt "$FINAL_SFTP_RECEIPT" --key source_sha)" = "$FINAL_PROOF_SHA"
test "$(python3 scripts/tiered-writeback-e2e.py ledger-value --ledger "$FINAL_SFTP_LEDGER" --receipt "$FINAL_SFTP_RECEIPT" --key measurements.sftp_stock_vs_hpn_download)" = true
test "$(python3 scripts/tiered-writeback-e2e.py ledger-value --ledger "$FINAL_SFTP_LEDGER" --receipt "$FINAL_SFTP_RECEIPT" --key measurements.sftp_stock_vs_hpn_upload)" = true
test "$(python3 scripts/tiered-writeback-e2e.py ledger-value --ledger "$FINAL_SFTP_LEDGER" --receipt "$FINAL_SFTP_RECEIPT" --key measurements.zerofs_sftp_session_scaling)" = true
test "$(python3 scripts/tiered-writeback-e2e.py ledger-value --ledger "$FINAL_SFTP_LEDGER" --receipt "$FINAL_SFTP_RECEIPT" --key success)" = true
test "$(python3 scripts/tiered-writeback-e2e.py ledger-value --ledger "$FINAL_SFTP_LEDGER" --receipt "$FINAL_SFTP_RECEIPT" --key decision.zerofs_upload_gap_current)" = false
HPN_RECEIVE_WINNER="$(python3 scripts/tiered-writeback-e2e.py ledger-value --ledger "$FINAL_SFTP_LEDGER" --receipt "$FINAL_SFTP_RECEIPT" --key decision.hpn_receive_winner)"
case "$HPN_RECEIVE_WINNER" in true|false) ;; *) exit 1 ;; esac
if test "$HPN_RECEIVE_WINNER" = true; then
  run_final_scenario materialized remote hpn-package-winner
fi
sudo python3 scripts/tiered-writeback-e2e.py archive-control --ledger "$FINAL_SFTP_LEDGER" --archive-root /fast/zerofs-tiered-receipts
test ! -e "$FINAL_SFTP_CONTROL"
REMOTE
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
set -euo pipefail
cd /Volumes/bigssd/projects/ZeroFS/.worktrees/unified-tiered-writeback
FINAL_PROOF_SHA="$(git rev-parse HEAD^{commit})"
CONTROLLER_TARGET_RECEIPT="$(python3 scripts/tiered-writeback-e2e.py verify-controller-target --controller-ssh-target ubuntu-main --expected-host-key-sha256 "${ZEROFS_VM100_HOST_KEY_SHA256:?}" --format target-token)"
case "$CONTROLLER_TARGET_RECEIPT" in ''|*[!A-Za-z0-9_-]*) exit 1 ;; esac
ssh ubuntu-main "FINAL_PROOF_SHA='$FINAL_PROOF_SHA' CONTROLLER_TARGET_RECEIPT='$CONTROLLER_TARGET_RECEIPT' bash -seuo pipefail" <<'REMOTE'
cd /fast/projects/ZeroFS-unified-tiered-writeback
test "$(git rev-parse HEAD)" = "$FINAL_PROOF_SHA"
READ_CONTROL_UUID="$(python3 -c 'import uuid; print(uuid.uuid4())')"
READ_CONTROL_ROOT="/var/tmp/zerofs-tiered-control-${READ_CONTROL_UUID}"
READ_CONTROL_RESOURCES="/var/tmp/zerofs-tiered-resources-${READ_CONTROL_UUID}"
READ_CONTROL_LEDGER="${READ_CONTROL_ROOT}/ledger.json"
sudo python3 scripts/tiered-writeback-e2e.py supervise --ledger "$READ_CONTROL_LEDGER" --control-root "$READ_CONTROL_ROOT" --resource-root "$READ_CONTROL_RESOURCES" --source-sha "$FINAL_PROOF_SHA" --controller-ssh-target-receipt "${CONTROLLER_TARGET_RECEIPT:?}" --filesystem-ack-mode materialized --object-ack-mode ssd --scenario benchmark-read-throughput
MATERIALIZED_RECEIPT="$(python3 scripts/tiered-writeback-e2e.py ledger-value --ledger "$READ_CONTROL_LEDGER" --key latest_receipt)"
python3 scripts/tiered-writeback-e2e.py validate-owned-path --ledger "$READ_CONTROL_LEDGER" --path "$MATERIALIZED_RECEIPT" --kind receipt
test -s "$MATERIALIZED_RECEIPT"

READ_CANDIDATE_UUID="$(python3 -c 'import uuid; print(uuid.uuid4())')"
READ_CANDIDATE_ROOT="/var/tmp/zerofs-tiered-control-${READ_CANDIDATE_UUID}"
READ_CANDIDATE_RESOURCES="/var/tmp/zerofs-tiered-resources-${READ_CANDIDATE_UUID}"
READ_CANDIDATE_LEDGER="${READ_CANDIDATE_ROOT}/ledger.json"
sudo python3 scripts/tiered-writeback-e2e.py supervise --ledger "$READ_CANDIDATE_LEDGER" --control-root "$READ_CANDIDATE_ROOT" --resource-root "$READ_CANDIDATE_RESOURCES" --source-sha "$FINAL_PROOF_SHA" --controller-ssh-target-receipt "${CONTROLLER_TARGET_RECEIPT:?}" --filesystem-ack-mode volatile_memory --object-ack-mode ssd --scenario benchmark-read-throughput --control-receipt "$MATERIALIZED_RECEIPT"
sudo python3 scripts/tiered-writeback-e2e.py archive-control --ledger "$READ_CANDIDATE_LEDGER" --archive-root /fast/zerofs-tiered-receipts
sudo python3 scripts/tiered-writeback-e2e.py archive-control --ledger "$READ_CONTROL_LEDGER" --archive-root /fast/zerofs-tiered-receipts
test ! -e "$READ_CANDIDATE_ROOT"
test ! -e "$READ_CONTROL_ROOT"
REMOTE
```

Run the NBD/XFS read pair only on the separate proof host:

```bash
test -n "${ZEROFS_NBD_PROOF_HOST:-}"
case "$ZEROFS_NBD_PROOF_HOST" in ubuntu-main|vm100|100.125.144.4|ct198|10.10.10.55|100.108.226.83) exit 1 ;; esac
VM100_MACHINE_ID="$(python3 scripts/tiered-writeback-e2e.py validate-recorded-host-identity --receipt "${ZEROFS_VM100_IDENTITY_RECEIPT:?}" --expected-sha256 "${ZEROFS_VM100_IDENTITY_RECEIPT_SHA256:?}" --expected-role vm100 --expected-proxmox-vmid 100 --format machine-id)"
CT198_MACHINE_ID="$(python3 scripts/tiered-writeback-e2e.py validate-recorded-host-identity --receipt "${ZEROFS_CT198_IDENTITY_RECEIPT:?}" --expected-sha256 "${ZEROFS_CT198_IDENTITY_RECEIPT_SHA256:?}" --expected-role ct198 --expected-proxmox-vmid 198 --format machine-id)"
NBD_PROOF_IDENTITY_TOKEN="$(python3 scripts/tiered-writeback-e2e.py verify-proof-host --controller-ssh-target "$ZEROFS_NBD_PROOF_HOST" --expected-host-key-sha256 "$ZEROFS_NBD_PROOF_HOST_KEY_SHA256" --expected-machine-id "$ZEROFS_NBD_PROOF_MACHINE_ID" --expected-proxmox-vmid "$ZEROFS_NBD_PROOF_VMID" --forbid-machine-id "$VM100_MACHINE_ID" --forbid-machine-id "$CT198_MACHINE_ID" --forbid-proxmox-vmid 100 --forbid-proxmox-vmid 198 --format identity-token)"
NBD_PROOF_TARGET_TOKEN="$(python3 scripts/tiered-writeback-e2e.py verify-proof-host --controller-ssh-target "$ZEROFS_NBD_PROOF_HOST" --expected-host-key-sha256 "$ZEROFS_NBD_PROOF_HOST_KEY_SHA256" --expected-machine-id "$ZEROFS_NBD_PROOF_MACHINE_ID" --expected-proxmox-vmid "$ZEROFS_NBD_PROOF_VMID" --forbid-machine-id "$VM100_MACHINE_ID" --forbid-machine-id "$CT198_MACHINE_ID" --forbid-proxmox-vmid 100 --forbid-proxmox-vmid 198 --format target-token)"
case "$NBD_PROOF_IDENTITY_TOKEN" in ''|*[!A-Za-z0-9_-]*) exit 1 ;; esac
case "$NBD_PROOF_TARGET_TOKEN" in ''|*[!A-Za-z0-9_-]*) exit 1 ;; esac
ssh "$ZEROFS_NBD_PROOF_HOST" "FINAL_PROOF_SHA='$FINAL_PROOF_SHA' PROOF_HOST_IDENTITY='$NBD_PROOF_IDENTITY_TOKEN' CONTROLLER_TARGET_RECEIPT='$NBD_PROOF_TARGET_TOKEN' bash -seuo pipefail" <<'REMOTE'
cd /fast/projects/ZeroFS-unified-tiered-writeback
test "$(git rev-parse HEAD)" = "$FINAL_PROOF_SHA"
CONTROL_UUID="$(python3 -c 'import uuid; print(uuid.uuid4())')"
CONTROL_ROOT="/var/tmp/zerofs-tiered-control-${CONTROL_UUID}"
CONTROL_RESOURCES="/var/tmp/zerofs-tiered-resources-${CONTROL_UUID}"
CONTROL_LEDGER="${CONTROL_ROOT}/ledger.json"
sudo python3 scripts/tiered-writeback-e2e.py supervise --ledger "$CONTROL_LEDGER" --control-root "$CONTROL_ROOT" --resource-root "$CONTROL_RESOURCES" --source-sha "$FINAL_PROOF_SHA" --proof-host-identity "$PROOF_HOST_IDENTITY" --controller-ssh-target-receipt "$CONTROLLER_TARGET_RECEIPT" --filesystem-ack-mode materialized --object-ack-mode ssd --scenario benchmark-read-throughput-nbd
CONTROL_RECEIPT="$(python3 scripts/tiered-writeback-e2e.py ledger-value --ledger "$CONTROL_LEDGER" --key latest_receipt)"
python3 scripts/tiered-writeback-e2e.py validate-owned-path --ledger "$CONTROL_LEDGER" --path "$CONTROL_RECEIPT" --kind receipt
test -s "$CONTROL_RECEIPT"
CANDIDATE_UUID="$(python3 -c 'import uuid; print(uuid.uuid4())')"
CANDIDATE_ROOT="/var/tmp/zerofs-tiered-control-${CANDIDATE_UUID}"
CANDIDATE_RESOURCES="/var/tmp/zerofs-tiered-resources-${CANDIDATE_UUID}"
CANDIDATE_LEDGER="${CANDIDATE_ROOT}/ledger.json"
sudo python3 scripts/tiered-writeback-e2e.py supervise --ledger "$CANDIDATE_LEDGER" --control-root "$CANDIDATE_ROOT" --resource-root "$CANDIDATE_RESOURCES" --source-sha "$FINAL_PROOF_SHA" --proof-host-identity "$PROOF_HOST_IDENTITY" --controller-ssh-target-receipt "$CONTROLLER_TARGET_RECEIPT" --filesystem-ack-mode volatile_memory --object-ack-mode ssd --scenario benchmark-read-throughput-nbd --control-receipt "$CONTROL_RECEIPT"
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
command emits exactly `controller_target<TAB>ledger`; `controller_target` comes only
from the manifest-covered target receipt's `normalized_target`, never the machine
hostname, discovery host, loop variable, or index row. A missing/invalid target receipt
fails enumeration. For each row, run the following block on that validated target. The
archived index must contain no unarchived entry before merge:

```bash
set -euo pipefail
cd /Volumes/bigssd/projects/ZeroFS/.worktrees/unified-tiered-writeback
FINAL_PROOF_SHA="$(git rev-parse HEAD^{commit})"
test "${#FINAL_PROOF_SHA}" = 40
VM100_MACHINE_ID="$(python3 scripts/tiered-writeback-e2e.py validate-recorded-host-identity --receipt "${ZEROFS_VM100_IDENTITY_RECEIPT:?}" --expected-sha256 "${ZEROFS_VM100_IDENTITY_RECEIPT_SHA256:?}" --expected-role vm100 --expected-proxmox-vmid 100 --format machine-id)"
CT198_MACHINE_ID="$(python3 scripts/tiered-writeback-e2e.py validate-recorded-host-identity --receipt "${ZEROFS_CT198_IDENTITY_RECEIPT:?}" --expected-sha256 "${ZEROFS_CT198_IDENTITY_RECEIPT_SHA256:?}" --expected-role ct198 --expected-proxmox-vmid 198 --format machine-id)"
UBUNTU_NORMALIZED_TARGET="$(python3 scripts/tiered-writeback-e2e.py verify-controller-target --controller-ssh-target ubuntu-main --expected-host-key-sha256 "${ZEROFS_VM100_HOST_KEY_SHA256:?}" --format normalized-target)"
NBD_NORMALIZED_TARGET="$(python3 scripts/tiered-writeback-e2e.py verify-proof-host --controller-ssh-target "${ZEROFS_NBD_PROOF_HOST:?}" --expected-host-key-sha256 "${ZEROFS_NBD_PROOF_HOST_KEY_SHA256:?}" --expected-machine-id "${ZEROFS_NBD_PROOF_MACHINE_ID:?}" --expected-proxmox-vmid "${ZEROFS_NBD_PROOF_VMID:?}" --forbid-machine-id "$VM100_MACHINE_ID" --forbid-machine-id "$CT198_MACHINE_ID" --forbid-proxmox-vmid 100 --forbid-proxmox-vmid 198 --format normalized-target)"
case "$UBUNTU_NORMALIZED_TARGET" in ''|*[!A-Za-z0-9._:@-]*) exit 1 ;; esac
case "$NBD_NORMALIZED_TARGET" in ''|*[!A-Za-z0-9._:@-]*) exit 1 ;; esac
test "$UBUNTU_NORMALIZED_TARGET" != "$NBD_NORMALIZED_TARGET"
mint_target_token() {
  target="$1"
  case "$target" in
    "$UBUNTU_NORMALIZED_TARGET")
      python3 scripts/tiered-writeback-e2e.py verify-controller-target --controller-ssh-target "$target" --expected-host-key-sha256 "${ZEROFS_VM100_HOST_KEY_SHA256:?}" --format target-token
      ;;
    "$NBD_NORMALIZED_TARGET")
      python3 scripts/tiered-writeback-e2e.py verify-proof-host --controller-ssh-target "$target" --expected-host-key-sha256 "${ZEROFS_NBD_PROOF_HOST_KEY_SHA256:?}" --expected-machine-id "${ZEROFS_NBD_PROOF_MACHINE_ID:?}" --expected-proxmox-vmid "${ZEROFS_NBD_PROOF_VMID:?}" --forbid-machine-id "$VM100_MACHINE_ID" --forbid-machine-id "$CT198_MACHINE_ID" --forbid-proxmox-vmid 100 --forbid-proxmox-vmid 198 --format target-token
      ;;
    *) return 1 ;;
  esac
}
mint_identity_token() {
  target="$1"
  case "$target" in
    "$UBUNTU_NORMALIZED_TARGET") printf '\n' ;;
    "$NBD_NORMALIZED_TARGET")
      python3 scripts/tiered-writeback-e2e.py verify-proof-host --controller-ssh-target "$target" --expected-host-key-sha256 "${ZEROFS_NBD_PROOF_HOST_KEY_SHA256:?}" --expected-machine-id "${ZEROFS_NBD_PROOF_MACHINE_ID:?}" --expected-proxmox-vmid "${ZEROFS_NBD_PROOF_VMID:?}" --forbid-machine-id "$VM100_MACHINE_ID" --forbid-machine-id "$CT198_MACHINE_ID" --forbid-proxmox-vmid 100 --forbid-proxmox-vmid 198 --format identity-token
      ;;
    *) return 1 ;;
  esac
}
ledger_index="$(mktemp "${TMPDIR:-/tmp}/zerofs-ledgers.${FINAL_PROOF_SHA}.XXXXXX")"
trap 'rm -f "$ledger_index"' EXIT
for controller_target in "$UBUNTU_NORMALIZED_TARGET" "$NBD_NORMALIZED_TARGET"
do
  fresh_target_token="$(mint_target_token "$controller_target")"
  fresh_identity_token="$(mint_identity_token "$controller_target")"
  case "$fresh_target_token" in ''|*[!A-Za-z0-9_-]*) exit 1 ;; esac
  case "$controller_target:$fresh_identity_token" in
    "$UBUNTU_NORMALIZED_TARGET:") ;;
    "$NBD_NORMALIZED_TARGET:"?*) case "$fresh_identity_token" in *[!A-Za-z0-9_-]*) exit 1 ;; esac ;;
    *) exit 1 ;;
  esac
  ssh "$controller_target" "PROOF_HOST_IDENTITY='$fresh_identity_token' CONTROLLER_TARGET_RECEIPT='$fresh_target_token' bash -seuo pipefail" <<'REMOTE' >> "$ledger_index"
cd /fast/projects/ZeroFS-unified-tiered-writeback
python3 scripts/tiered-writeback-e2e.py list-ledgers --campaign codex-unified-tiered-writeback --format controller-target-ledger-tsv
REMOTE
done
sort -u -o "$ledger_index" "$ledger_index"
test -s "$ledger_index"
while IFS="$(printf '\t')" read -r controller_target ledger_path
do
  case "$controller_target" in -*|*[!A-Za-z0-9._:@-]*) exit 1 ;; esac
  case "$ledger_path" in ''|*[!A-Za-z0-9/._-]*) exit 1 ;; esac
  fresh_target_token="$(mint_target_token "$controller_target")"
  fresh_identity_token="$(mint_identity_token "$controller_target")"
  case "$fresh_target_token" in ''|*[!A-Za-z0-9_-]*) exit 1 ;; esac
  case "$controller_target:$fresh_identity_token" in
    "$UBUNTU_NORMALIZED_TARGET:") ;;
    "$NBD_NORMALIZED_TARGET:"?*) case "$fresh_identity_token" in *[!A-Za-z0-9_-]*) exit 1 ;; esac ;;
    *) exit 1 ;;
  esac
  ssh "$controller_target" "LEDGER='$ledger_path' PROOF_HOST_IDENTITY='$fresh_identity_token' CONTROLLER_TARGET_RECEIPT='$fresh_target_token' bash -seuo pipefail" <<'REMOTE'
cd /fast/projects/ZeroFS-unified-tiered-writeback
sudo python3 scripts/tiered-writeback-e2e.py cleanup --ledger "$LEDGER"
sudo python3 scripts/tiered-writeback-e2e.py cleanup --ledger "$LEDGER"
sudo python3 scripts/tiered-writeback-e2e.py assert-clean --ledger "$LEDGER"
sudo python3 scripts/tiered-writeback-e2e.py archive-control --ledger "$LEDGER" --archive-root /fast/zerofs-tiered-receipts
REMOTE
done < "$ledger_index"
for controller_target in "$UBUNTU_NORMALIZED_TARGET" "$NBD_NORMALIZED_TARGET"
do
  fresh_target_token="$(mint_target_token "$controller_target")"
  fresh_identity_token="$(mint_identity_token "$controller_target")"
  case "$fresh_target_token" in ''|*[!A-Za-z0-9_-]*) exit 1 ;; esac
  case "$controller_target:$fresh_identity_token" in
    "$UBUNTU_NORMALIZED_TARGET:") ;;
    "$NBD_NORMALIZED_TARGET:"?*) case "$fresh_identity_token" in *[!A-Za-z0-9_-]*) exit 1 ;; esac ;;
    *) exit 1 ;;
  esac
  ssh "$controller_target" "PROOF_HOST_IDENTITY='$fresh_identity_token' CONTROLLER_TARGET_RECEIPT='$fresh_target_token' bash -seuo pipefail" <<'REMOTE'
cd /fast/projects/ZeroFS-unified-tiered-writeback
python3 scripts/tiered-writeback-e2e.py list-ledgers --campaign codex-unified-tiered-writeback --require-all-archived
REMOTE
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
CONTROLLER_TARGET_RECEIPT="$(python3 scripts/tiered-writeback-e2e.py verify-controller-target --controller-ssh-target ubuntu-main --expected-host-key-sha256 "${ZEROFS_VM100_HOST_KEY_SHA256:?}" --format target-token)"
case "$CONTROLLER_TARGET_RECEIPT" in ''|*[!A-Za-z0-9_-]*) exit 1 ;; esac
ssh ubuntu-main "EXPECTED_OLD_SHA='$EXPECTED_OLD_SHA' EXPECTED_NEW_SHA='$EXPECTED_NEW_SHA' CONTROLLER_TARGET_RECEIPT='$CONTROLLER_TARGET_RECEIPT' bash -seuo pipefail" <<'REMOTE'
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
set -euo pipefail
cd /Volumes/bigssd/projects/ZeroFS
CONTROLLER_TARGET_RECEIPT="$(python3 scripts/tiered-writeback-e2e.py verify-controller-target --controller-ssh-target ubuntu-main --expected-host-key-sha256 "${ZEROFS_VM100_HOST_KEY_SHA256:?}" --format target-token)"
case "$CONTROLLER_TARGET_RECEIPT" in ''|*[!A-Za-z0-9_-]*) exit 1 ;; esac
ssh ubuntu-main "CONTROLLER_TARGET_RECEIPT='$CONTROLLER_TARGET_RECEIPT' bash -seuo pipefail" <<'REMOTE'
cd /fast/projects/ZeroFS
git worktree remove /fast/projects/ZeroFS-unified-tiered-writeback
git worktree prune
git worktree list --porcelain
REMOTE
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
CONTROLLER_TARGET_RECEIPT="$(python3 scripts/tiered-writeback-e2e.py verify-controller-target --controller-ssh-target ubuntu-main --expected-host-key-sha256 "${ZEROFS_VM100_HOST_KEY_SHA256:?}" --format target-token)"
case "$CONTROLLER_TARGET_RECEIPT" in ''|*[!A-Za-z0-9_-]*) exit 1 ;; esac
ssh ubuntu-main "CONTROLLER_TARGET_RECEIPT='$CONTROLLER_TARGET_RECEIPT' bash -seuo pipefail" <<'REMOTE'
cd /fast/projects/ZeroFS
git worktree list --porcelain
REMOTE
```
