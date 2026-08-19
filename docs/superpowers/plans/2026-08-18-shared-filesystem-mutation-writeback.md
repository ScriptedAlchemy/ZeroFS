# Shared Filesystem Mutation Writeback Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the NBD-only volatile overlay with one bounded, coherent filesystem mutation layer shared by NBD, NFS, 9P, WebUI/RPC, and direct callers, with typed local/remote durability and one bounded shutdown owner.

**Architecture:** Canonical writes split into behavior-preserving preparation and application. A process-wide `MutationCoordinator` owns raw accepted batches, request replay, the inode/range overlay, materialization progress, conflict gates, and typed mutation cutoffs; the existing filesystem and object-writeback layers remain the canonical storage and durability authorities.

**Tech Stack:** Rust 2024, Tokio, Bytes, SlateDB, the existing ZeroFS filesystem/object-writeback code, NBD, 9P2000.L, gRPC-Web/WebSocket WebUI, a pinned `ScriptedAlchemy/nfsserve` fork, and Prometheus.

**Spec:** `docs/superpowers/specs/2026-08-18-unified-tiered-writeback-design.md`

## Global Constraints

- Materialized mode remains the default and is behaviorally unchanged before volatile callers are enabled.
- Canonical validation, authorization, quota reservation, timestamps, and returned attributes finish before volatile acknowledgement.
- Duplicate lookup precedes admission; no retry waits behind or pays again for its own pending payload.
- Metadata fences quiesce preparers and materialize without holding a canonical inode, directory, database, or flush-barrier lock.
- Writeback enabled resolves filesystem FLUSH/fsync/COMMIT to `LocalSsd`; materialized/direct-backend mode with writeback disabled resolves it to `RemoteBackend`.
- One logical request produces one `PreparedWriteBatch`; striped NBD members publish atomically to the live overlay and share one completion cutoff.
- A local receipt means mutation materialization plus seal/database flush plus SSD-journal coverage. A remote receipt means the same filesystem cutoff plus remote object coverage.
- The lifecycle task is the only shutdown owner. Observability consumes its phase stream and never invokes a second close.
- Do not edit the approved spec in implementation commits. A spec amendment, if required, is a separately reviewed documentation task.
- Never deploy or restart CT198 during implementation.
- Review limits: facade/config glue 250 production lines; types 300; each admission/progress/durability/request-cache/fence module 400; overlay/materializer 600 each; an async worker loop 150; a function 100; source plus inline tests 1000, after which tests move to a separate test module.
- Each task starts with named RED tests, runs the exact GREEN gate, stages only its exact file fence, and creates one self-contained commit.

## Non-Vacuous Filtered Cargo Gates

Before executing any task in this plan, define this function in the same shell. Every filtered Cargo test command below uses it; raw filtered `cargo test` is not an acceptable substitute.

```bash
cargo_test_nonzero() (
  set -o pipefail
  filter="$1"
  shift
  safe_filter="${filter//[^A-Za-z0-9]/_}"
  list_log="${TMPDIR:-/tmp}/zerofs-${safe_filter}-list.log"
  run_log="${TMPDIR:-/tmp}/zerofs-${safe_filter}-run.log"
  if ! command cargo test "$@" -- --list 2>&1 | tee "$list_log"; then
    exit 1
  fi
  test "$(grep -Fc "$filter" "$list_log")" -gt 0
  if ! command cargo test "$@" "$filter" -- --nocapture 2>&1 | tee "$run_log"; then
    exit 1
  fi
  grep -Eq 'test result: ok\. [1-9][0-9]* passed' "$run_log"
)
```

The subshell scopes `pipefail`; either Cargo or `tee` failing makes the helper fail before selection/result checks. The list count proves the filter exists, and the result assertion proves it executed at least one passing test. Exact ignored tests additionally use `--exact` and assert exactly one pass.

---

### Task A1: Extract Coordination Primitives Without Behavior Change

**Files:**
- Create: `zerofs/src/coordination/mod.rs`
- Create: `zerofs/src/coordination/admission.rs`
- Create: `zerofs/src/coordination/sequence.rs`
- Modify: `zerofs/src/main.rs`
- Modify: `zerofs/src/lib.rs`
- Modify: `zerofs/src/writeback/mod.rs`
- Modify: `zerofs/src/writeback/admission.rs`

**Interfaces:**
- Produces: crate-private generic admission queue and typed contiguous-sequence barrier under `crate::coordination`.
- Consumes: the existing `writeback::admission` policies, waiter nodes, permits, local/remote progress, close state, and poison state.

- [ ] **Step 1: Move tests first and record RED**

Move the current FIFO, cancellation, capacity-refresh, blocked-transition, grant, canceled-grant rollback, release, permit-disarm, close, poison, and contiguous-progress tests to the new modules without changing assertions. Declare `mod coordination;` in both `zerofs/src/main.rs` and `zerofs/src/lib.rs`, then run:

```bash
cd /Volumes/bigssd/projects/ZeroFS/.worktrees/unified-tiered-writeback/zerofs
cargo_test_nonzero 'coordination::' -p zerofs --locked
```

Expected RED: imports fail until the existing implementations move.

- [ ] **Step 2: Perform a behavior-neutral extraction**

Move the existing queue, policy hooks, waiter/grant guards, permits, and sequence waiters intact. Preserve the full lifecycle—refresh, fit, charge/admit, blocked transition, grant, canceled grant/rollback, release, permit disarm, close, and terminal poison—and temporarily re-export old paths. Do not replace it with a new four-method admission trait.

- [ ] **Step 3: Run equivalence gates**

```bash
cd /Volumes/bigssd/projects/ZeroFS/.worktrees/unified-tiered-writeback/zerofs
cargo_test_nonzero 'coordination::' -p zerofs --locked
cargo_test_nonzero 'writeback::admission::tests' -p zerofs --locked
cargo_test_nonzero 'writeback::journaler::tests' -p zerofs --locked
cargo_test_nonzero 'writeback::remote::tests' -p zerofs --locked
cargo fmt --all -- --check
git diff --check
```

- [ ] **Step 4: Commit the exact fence**

```bash
git add zerofs/src/coordination/mod.rs zerofs/src/coordination/admission.rs zerofs/src/coordination/sequence.rs zerofs/src/main.rs zerofs/src/lib.rs zerofs/src/writeback/mod.rs zerofs/src/writeback/admission.rs
git commit -m "refactor: share admission and sequence coordination"
```

---

### Task A2: Declare the Mutation Module and Normalize Acknowledgement Configuration

**Files:**
- Create: `zerofs/src/fs/mutation/mod.rs`
- Create: `zerofs/src/fs/mutation/config.rs`
- Modify: `zerofs/src/fs/mod.rs`
- Modify: `zerofs/src/config.rs`
- Modify: `zerofs/src/cli/server.rs`

**Interfaces:**
- Produces: `FilesystemWriteAckMode`, `FilesystemWriteAckSource`, `FilesystemWriteAckSettings`, and `Settings::filesystem_write_ack_settings`.
- Consumes: filesystem and legacy NBD fields, writeback enablement, access mode, replication, protocol maximum writes, and `ignore_fsync`.

- [ ] **Step 1: Add the normalization matrix as RED tests**

Cover omitted materialized default, explicit materialized, explicit volatile, operation cap/default, legacy-only NBD, matching/conflicting dual forms, invalid numeric budgets, missing writeback, read-only/checkpoint, replication, `ignore_fsync`, insufficient protocol maximum, and generated config.

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum FilesystemWriteAckSource {
    DefaultMaterialized,
    Filesystem,
    LegacyNbd,
    MatchingFilesystemAndLegacyNbd,
}
```

```bash
cd /Volumes/bigssd/projects/ZeroFS/.worktrees/unified-tiered-writeback/zerofs
cargo_test_nonzero 'config::tests::filesystem_write_ack' -p zerofs --locked
```

Expected RED: the mutation module and normalized source/target types do not exist.

- [ ] **Step 2: Implement the exact normalized contract**

```rust
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FilesystemWriteAckMode {
    #[default]
    Materialized,
    VolatileMemory,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ClientDurabilityTarget {
    LocalSsd,
    RemoteBackend,
}

pub(crate) struct FilesystemWriteAckSettings {
    pub(crate) mode: FilesystemWriteAckMode,
    pub(crate) volatile_memory_bytes: u64,
    pub(crate) volatile_max_operations: usize,
    pub(crate) source: FilesystemWriteAckSource,
    pub(crate) client_durability_target: ClientDurabilityTarget,
}
```

Resolve `client_durability_target` once: any enabled object writeback is `LocalSsd`; disabled writeback is valid only with materialized/direct backend and resolves to `RemoteBackend`; volatile plus disabled writeback is invalid. Every protocol adapter consumes this resolved field and never hard-codes SSD.

- [ ] **Step 3: Run configuration gates and commit**

```bash
cd /Volumes/bigssd/projects/ZeroFS/.worktrees/unified-tiered-writeback/zerofs
cargo_test_nonzero 'config::tests' -p zerofs --locked
cargo_test_nonzero 'cli::server::tests::volatile' -p zerofs --locked
cargo fmt --all -- --check
git diff --check
git add zerofs/src/fs/mutation/mod.rs zerofs/src/fs/mutation/config.rs zerofs/src/fs/mod.rs zerofs/src/config.rs zerofs/src/cli/server.rs
git commit -m "config: define shared filesystem acknowledgement contract"
```

---

### Task A3: Extract Canonical Write Preparation and Application

**Files:**
- Create: `zerofs/src/fs/ops/write.rs`
- Create: `zerofs/src/fs/mutation/types.rs`
- Modify: `zerofs/src/fs/ops/io.rs`
- Modify: `zerofs/src/fs/ops/mod.rs`
- Modify: `zerofs/src/fs/mod.rs`
- Modify: `zerofs/src/fs/boot.rs`

**Interfaces:**
- Produces: `PrepareWriteRequest`, `PreparedWriteMember`, `PreparedWriteBatch`, `PreparedBatchResult`, `prepare_write`, and `apply_prepared_batch`.
- Consumes: current locks, dedup, permission/type/overflow checks, inode/extent stores, timestamps, attributes, stats, and write coordinator.

- [ ] **Step 1: Add RED materialized-equivalence tests**

Name tests `prepare_write_has_no_canonical_side_effect`, `apply_uses_preselected_timestamp_and_attrs`, `zero_length_write_preserves_result`, `materialized_write_waits_for_apply`, and `striped_batch_has_one_result_boundary`.

```rust
pub(crate) async fn prepare_write(
    context: &WritePrepareContext,
    request: PrepareWriteRequest,
) -> Result<PreparedWriteBatch, FsError>;

pub(crate) async fn apply_prepared_batch(
    context: &WriteApplyContext,
    batch: &mut PreparedWriteBatch,
) -> Result<PreparedBatchResult, FsError>;
```

```bash
cd /Volumes/bigssd/projects/ZeroFS/.worktrees/unified-tiered-writeback/zerofs
cargo_test_nonzero 'fs::ops::write::tests' -p zerofs --locked
```

Expected RED: extracted types and entry points are absent.

- [ ] **Step 2: Move canonical code without changing policy**

Preparation owns dedup checks, sorted inode-lock acquisition, overlap waits, authorization, inode/type/overflow checks, current quota calls, timestamp and set-id decisions, and exact post-write attributes. Application owns extents, encoding, transactions, inode publication, stats, tracing, dedup publication, and the existing quota updates. Materialized public methods call prepare then apply immediately.

- [ ] **Step 3: Run parity gates and commit**

```bash
cd /Volumes/bigssd/projects/ZeroFS/.worktrees/unified-tiered-writeback/zerofs
cargo_test_nonzero 'fs::ops::write::tests' -p zerofs --locked
cargo_test_nonzero 'fs::ops::io::tests' -p zerofs --locked
cargo_test_nonzero 'fs::store::extent::inflight::tests' -p zerofs --locked
cargo fmt --all -- --check
git diff --check
git add zerofs/src/fs/ops/write.rs zerofs/src/fs/mutation/types.rs zerofs/src/fs/ops/io.rs zerofs/src/fs/ops/mod.rs zerofs/src/fs/mod.rs zerofs/src/fs/boot.rs
git commit -m "refactor(fs): split prepared writes from canonical apply"
```

---

### Task A4: Transfer Quota Ownership Through Preparation

**Files:**
- Create: `zerofs/src/fs/quota.rs`
- Modify: `zerofs/src/fs/mutation/types.rs`
- Modify: `zerofs/src/fs/ops/write.rs`
- Modify: `zerofs/src/fs/ops/setattr.rs`
- Modify: `zerofs/src/fs/ops/remove.rs`
- Modify: `zerofs/src/fs/handle.rs`
- Modify: `zerofs/src/fs/mod.rs`

**Interfaces:**
- Produces: `LogicalQuota`, `ProvisionalQuotaReservation`, and atomic accepted-to-canonical ownership transfer.
- Consumes: canonical and pending visible size, global quota/stat counters, shrink/reclaim, and prepared members.

- [ ] **Step 1: Add RED quota ownership tests**

Name tests `concurrent_preparations_cannot_oversubscribe`, `provisional_drop_releases_once`, `acceptance_transfers_without_arithmetic`, `canonical_apply_transfers_without_gap`, `terminal_retains_pending_charge`, and `shrink_releases_only_after_commit`.

```rust
pub(crate) struct ProvisionalQuotaReservation {
    quota: std::sync::Arc<LogicalQuota>,
    bytes: u64,
    state: QuotaReservationState,
}

pub(crate) enum QuotaReservationState {
    Provisional,
    Accepted,
    Canonical,
    TerminalRetained,
    Released,
}
```

- [ ] **Step 2: Implement checked, single-owner transfer**

CAS-reserve growth against committed plus pending visible size. Drop rolls back only `Provisional`. Acceptance, canonical apply, and terminal retention change state without add/subtract. Shrink/reclaim subtracts only after canonical commit succeeds.

- [ ] **Step 3: Run GREEN and commit**

```bash
cd /Volumes/bigssd/projects/ZeroFS/.worktrees/unified-tiered-writeback/zerofs
cargo_test_nonzero 'fs::quota::tests' -p zerofs --locked
cargo_test_nonzero 'fs::ops::write::tests' -p zerofs --locked
cargo_test_nonzero 'fs::ops::setattr::tests' -p zerofs --locked
cargo_test_nonzero 'fs::ops::remove::tests' -p zerofs --locked
cargo_test_nonzero 'fs::handle::tests' -p zerofs --locked
cargo fmt --all -- --check
git diff --check
git add zerofs/src/fs/quota.rs zerofs/src/fs/mutation/types.rs zerofs/src/fs/ops/write.rs zerofs/src/fs/ops/setattr.rs zerofs/src/fs/ops/remove.rs zerofs/src/fs/handle.rs zerofs/src/fs/mod.rs
git commit -m "refactor(fs): transfer prepared write quota ownership"
```

---

### Task A5: Define Request Identity and Bounded Replay Cache

**Files:**
- Create: `zerofs/src/fs/mutation/request_cache.rs`
- Modify: `zerofs/src/fs/mutation/types.rs`
- Modify: `zerofs/src/fs/mutation/mod.rs`

**Interfaces:**
- Produces: `RequestIdentity`, `RequestFingerprint`, `RequestLookup`, `RequestCache`, `PendingRequest`, `AcceptedRequest`, and `RetainedRequest`.
- Consumes: protocol connection/session incarnations, operation IDs/handles/XIDs, payload/auth/durability fingerprints, operation cap, and canonical dedup.

- [ ] **Step 1: Write RED identity and cache tests**

Name tests for join-before-admission, fingerprint/auth/stability mismatch, pending non-eviction, completed expiry, cache-pressure backpressure, one-shot direct calls, in-flight NBD collision, and NFS reconnect address reuse.

```rust
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) enum RequestIdentity {
    NineP { session_incarnation: u64, operation_id: u64 },
    Nfs { server_incarnation: uuid::Uuid, connection_incarnation: u64, xid: u32 },
    Nbd { connection_incarnation: u64, handle: u64 },
    DirectTagged { caller_incarnation: uuid::Uuid, operation_id: u128 },
    DirectOneShot(uuid::Uuid),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct RequestFingerprint([u8; 32]);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RequestLifetime {
    CanonicalDedup,
    ReplayWindow(std::time::Duration),
    InFlightOnly,
    OneShot,
}

pub(crate) struct RequestOperationSlot {
    cache: std::sync::Weak<RequestCacheInner>,
    active: bool,
}

pub(crate) enum RequestLookup {
    Vacant(RequestVacancy),
    Joined(std::sync::Arc<RetainedRequest>),
    FingerprintMismatch,
    Backpressured,
}

pub(crate) struct RequestCache {
    max_entries: usize,
    inner: std::sync::Arc<RequestCacheInner>,
}

pub(crate) struct RequestVacancy {
    cache: std::sync::Arc<RequestCacheInner>,
    identity: RequestIdentity,
    fingerprint: RequestFingerprint,
    lifetime: RequestLifetime,
    operation_slot: Option<RequestOperationSlot>,
}

pub(crate) struct PendingRequest {
    cache: std::sync::Arc<RequestCacheInner>,
    identity: RequestIdentity,
    fingerprint: RequestFingerprint,
    lifetime: RequestLifetime,
    operation_slot: Option<RequestOperationSlot>,
    state: PendingRequestState,
}

pub(crate) struct AcceptedRequest {
    cache: std::sync::Arc<RequestCacheInner>,
    identity: RequestIdentity,
    operation_slot: Option<RequestOperationSlot>,
}

impl RequestVacancy {
    pub(crate) fn begin_pending(mut self) -> PendingRequest {
        self.cache.mark_pending(&self.identity);
        PendingRequest {
            cache: std::sync::Arc::clone(&self.cache),
            identity: self.identity.clone(),
            fingerprint: self.fingerprint,
            lifetime: self.lifetime,
            operation_slot: self.operation_slot.take(),
            state: PendingRequestState::Preparing,
        }
    }
}

impl PendingRequest {
    pub(crate) fn accept(mut self) -> AcceptedRequest {
        self.state = PendingRequestState::Accepted;
        AcceptedRequest {
            cache: std::sync::Arc::clone(&self.cache),
            identity: self.identity.clone(),
            operation_slot: self.operation_slot.take(),
        }
    }

    pub(crate) fn fail(
        self,
        error: FsError,
    ) -> std::sync::Arc<RetainedRequest> {
        let cache = std::sync::Arc::clone(&self.cache);
        cache.retain_failure(self, error)
    }

    pub(crate) fn cancel(self) {
        let cache = std::sync::Arc::clone(&self.cache);
        cache.remove_pending_and_release_slot(self);
    }
}

impl RequestCache {
    pub(crate) fn lookup_or_reserve(
        &self,
        identity: RequestIdentity,
        fingerprint: RequestFingerprint,
        lifetime: RequestLifetime,
    ) -> Result<RequestLookup, RequestCacheError>;
    pub(crate) fn complete(
        &self,
        accepted: AcceptedRequest,
        result: Result<PreparedBatchResult, FsError>,
    ) -> std::sync::Arc<RetainedRequest>;
}
```

`lookup_or_reserve` performs join/fingerprint/pressure handling before any raw admission call. `Vacant` already owns exactly one cache operation slot. `RequestOperationSlot::drop` releases its operation charge when `active`; the sole transfer path moves it without disarming into the next owner, and retained completion disarms it only when the cache entry assumes the bounded charge. `RequestVacancy::begin_pending(self)` consumes the vacancy and moves that same slot into `PendingRequest`; it does not allocate a second slot. Dropping an unconsumed vacancy or calling `PendingRequest::cancel` removes the provisional entry and releases the slot. `PendingRequest::fail` atomically publishes the retained failure and transfers the slot to its bounded replay lifetime; expiry/removal of that retained failure releases the slot exactly once. `PendingRequest::accept` is the only accepted transition: it consumes the pending owner and moves the same slot into `AcceptedRequest`; `RequestCache::complete` consumes that accepted owner and retains the final result. A joined lookup never reaches raw admission. NFS identity always includes a server-minted transport `connection_incarnation`; client address is part of the fingerprint, not a substitute for the incarnation. Reusing an address after reconnect cannot join old work.

- [ ] **Step 2: Implement vacancy-to-pending ownership**

Insert the vacancy while holding the cache-state mutex, move its single operation slot through `begin_pending`, and make every cancellation/error/drop path remove or retain the entry exactly once. A lookup that returns `Joined` returns the retained shared result immediately and cannot call `RawMutationBudget::acquire`.

- [ ] **Step 3: Run GREEN and commit**

```bash
cd /Volumes/bigssd/projects/ZeroFS/.worktrees/unified-tiered-writeback/zerofs
cargo_test_nonzero 'fs::mutation::request_cache::tests' -p zerofs --locked
cargo clippy -p zerofs --lib --locked -- -D warnings
cargo fmt --all -- --check
git diff --check
git add zerofs/src/fs/mutation/request_cache.rs zerofs/src/fs/mutation/types.rs zerofs/src/fs/mutation/mod.rs
git commit -m "feat(mutation): add bounded protocol request replay"
```

---

### Task A6: Add Raw Admission, Preparation Quiescence, and Progress

**Files:**
- Create: `zerofs/src/fs/mutation/admission.rs`
- Create: `zerofs/src/fs/mutation/progress.rs`
- Modify: `zerofs/src/fs/mutation/types.rs`
- Modify: `zerofs/src/fs/mutation/mod.rs`

**Interfaces:**
- Produces: `RawMutationBudget`, `RawMutationPermit`, `PreparationGuard`, `PreparationAbort`, `ConflictKey`, `ConflictScope`, `MutationIncarnation`, `MutationCutoff`, and `MutationProgress`.
- Consumes: shared coordination primitives, request vacancies, byte/op settings, and terminal state.

- [ ] **Step 1: Write RED admission/progress tests**

Name tests `permit_is_acquired_before_payload_copy`, `cancelled_waiter_rolls_back_bytes_and_ops`, `race_winner_releases_unused_permit`, `guard_must_publish_or_abort`, `guard_failure_retains_result_and_releases_raw_permit`, `guard_cancellation_removes_request_and_releases_slot`, `gate_close_waits_pre_cutoff_guards`, `guard_holds_no_canonical_lock`, `out_of_order_completion_advances_gap_free_prefix`, and `terminal_wakes_all_waiters`.

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) enum ConflictKey {
    Inode(InodeId),
    Directory(InodeId),
}

pub(crate) struct ConflictScope(std::collections::BTreeSet<ConflictKey>);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct MutationIncarnation(uuid::Uuid);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct MutationCutoff {
    pub(crate) mutation_incarnation: MutationIncarnation,
    pub(crate) sequence: u64,
}

pub(crate) struct PreparationGuard {
    gate: std::sync::Arc<PreparationGate>,
    scope: ConflictScope,
    raw_permit: Option<RawMutationPermit>,
    request: Option<PendingRequest>,
    state: PreparationState,
}

pub(crate) struct AcceptedMutation {
    request: AcceptedRequest,
    batch: PreparedWriteBatch,
    raw_permit: RawMutationPermit,
    cutoff: MutationCutoff,
}

pub(crate) enum PreparationAbort {
    RequestFailure(FsError),
    TransportCancellation,
}

impl PreparationGuard {
    pub(crate) fn new(
        gate: std::sync::Arc<PreparationGate>,
        scope: ConflictScope,
        raw_permit: RawMutationPermit,
        request: PendingRequest,
    ) -> Result<Self, MutationError>;
}

impl PreparationGuard {
    pub(crate) fn publish(
        self,
        batch: PreparedWriteBatch,
    ) -> Result<AcceptedMutation, MutationError>;
    pub(crate) fn abort(self, disposition: PreparationAbort) -> Result<(), MutationError>;
}
```

This is the only preparation guard; there is no separate `PreparationLease`. Only `RequestLookup::Vacant(vacancy) -> vacancy.begin_pending() -> RawMutationBudget::acquire -> PreparationGuard::new` reaches admission. `PreparationGuard::new` consumes the exact `PendingRequest` from Task A5 and the exact raw permit; construction failure calls `PendingRequest::cancel` and drops the permit. `publish` calls the sole `PendingRequest::accept` transition and moves the resulting `AcceptedRequest`, batch, permit ownership, and cutoff into `AcceptedMutation`. Materialization/reply completion passes that same `AcceptedRequest` to `RequestCache::complete`. `abort(PreparationAbort::RequestFailure(error))` calls `PendingRequest::fail(error)` and retains the deterministic failed result; `abort(PreparationAbort::TransportCancellation)` calls `PendingRequest::cancel` and removes the provisional entry. Both abort paths release the raw permit exactly once, and cancellation releases the request operation slot exactly once. The guard is acquired before canonical locks, is counted by the conflict gate, and is consumed by publish or abort. Gate quiescence waits guards without holding canonical locks.

- [ ] **Step 2: Implement admission and contiguous progress**

Acquire byte/op ownership cancellation-safely, compose it with the pending cache entry through `PreparationGuard::new`, and publish accepted/materialized gap-free prefixes through the typed sequence barrier. Close and poison wake admission, preparation, and progress waiters without fabricating a successful request result.

- [ ] **Step 3: Run GREEN and commit**

```bash
cd /Volumes/bigssd/projects/ZeroFS/.worktrees/unified-tiered-writeback/zerofs
cargo_test_nonzero 'fs::mutation::admission::tests' -p zerofs --locked
cargo_test_nonzero 'fs::mutation::progress::tests' -p zerofs --locked
cargo fmt --all -- --check
git diff --check
git add zerofs/src/fs/mutation/admission.rs zerofs/src/fs/mutation/progress.rs zerofs/src/fs/mutation/types.rs zerofs/src/fs/mutation/mod.rs
git commit -m "feat(mutation): add raw admission and contiguous progress"
```

---

### Task A7: Add the Atomic Inode/Range Overlay

**Files:**
- Create: `zerofs/src/fs/mutation/data_overlay.rs`
- Modify: `zerofs/src/fs/mutation/mod.rs`
- Modify: `zerofs/src/fs/ops/io.rs`
- Modify: `zerofs/src/fs/ops/lookup.rs`
- Modify: `zerofs/src/fs/mod.rs`

**Interfaces:**
- Produces: `DataOverlay::{install_batch,snapshot,retire_batch,freeze_terminal}` and `VisibleFileSnapshot`.
- Consumes: accepted prepared members, inode identity, sequence ordering, canonical reads, and canonical attributes.

- [ ] **Step 1: Write RED overlay tests**

Name tests for atomic multi-member install, disjoint and overlapping ranges, growth holes, hardlink aliases, one-snapshot reads, retirement with no visibility gap, and frozen terminal view.

```rust
pub(crate) struct VisibleFileSnapshot {
    pub(crate) visible_attrs: FileAttributes,
    pub(crate) canonical_read_len: usize,
    pub(crate) visible_read_len: usize,
    pub(crate) eof: bool,
    intervals: Vec<OverlayInterval>,
}
```

- [ ] **Step 2: Compose the only visible read/attribute seam**

`read_file_inner` authorizes and reads canonical bytes once, zero-fills pending growth holes, and overlays the captured intervals in sequence order. Add one `ZeroFS::getattr` visible-attribute method; leave canonical-only inode-store reads unchanged.

- [ ] **Step 3: Run GREEN and commit**

```bash
cd /Volumes/bigssd/projects/ZeroFS/.worktrees/unified-tiered-writeback/zerofs
cargo_test_nonzero 'fs::mutation::data_overlay::tests' -p zerofs --locked
cargo_test_nonzero 'fs::ops::io::tests' -p zerofs --locked
cargo_test_nonzero 'fs::ops::lookup::tests' -p zerofs --locked
cargo fmt --all -- --check
git diff --check
git add zerofs/src/fs/mutation/data_overlay.rs zerofs/src/fs/mutation/mod.rs zerofs/src/fs/ops/io.rs zerofs/src/fs/ops/lookup.rs zerofs/src/fs/mod.rs
git commit -m "feat(mutation): add coherent pending write overlay"
```

---

### Task A8: Materialize Accepted Batches in Canonical Order

**Files:**
- Create: `zerofs/src/fs/mutation/materializer.rs`
- Modify: `zerofs/src/fs/mutation/mod.rs`
- Modify: `zerofs/src/fs/boot.rs`
- Modify: `zerofs/src/cli/init.rs`

**Interfaces:**
- Produces: `Materializer::{start,dispatch_through,stop}` and materialized-prefix publication.
- Consumes: overlay, progress, `WriteApplyContext`, accepted batches, quota ownership, and terminal poison.

- [ ] **Step 1: Write RED scheduling tests**

Name tests `same_inode_applies_fifo`, `different_inodes_apply_concurrently`, `striped_batch_completes_after_all_members`, `global_prefix_waits_for_gap`, `overlay_retires_after_canonical_visibility`, `panic_poison_retains_frozen_view`, and `stop_joins_worker_loops`.

- [ ] **Step 2: Implement bounded owned workers**

Start only after `Arc<ZeroFS>` exists, via `Weak<ZeroFS>` or cycle-free `WriteApplyContext`. Keep each worker loop below 150 lines. Release raw/quota ownership only after canonical apply owns the corresponding state. First post-ack failure poisons progress and freezes the overlay.

- [ ] **Step 3: Run GREEN and commit**

```bash
cd /Volumes/bigssd/projects/ZeroFS/.worktrees/unified-tiered-writeback/zerofs
cargo_test_nonzero 'fs::mutation::materializer::tests' -p zerofs --locked
cargo_test_nonzero 'fs::mutation::progress::tests' -p zerofs --locked
cargo_test_nonzero 'fs::ops::write::tests' -p zerofs --locked
cargo fmt --all -- --check
git diff --check
git add zerofs/src/fs/mutation/materializer.rs zerofs/src/fs/mutation/mod.rs zerofs/src/fs/boot.rs zerofs/src/cli/init.rs
git commit -m "feat(mutation): materialize accepted batches in order"
```

---

### Task A9: Implement the Conflict-Fence Primitive

**Files:**
- Create: `zerofs/src/fs/mutation/fence.rs`
- Modify: `zerofs/src/fs/mutation/admission.rs`
- Modify: `zerofs/src/fs/mutation/mod.rs`

**Interfaces:**
- Produces: `MaterializationFence` and `MutationCoordinator::materialization_fence`.
- Consumes: `ConflictScope`, active `PreparationGuard` counts, cutoff capture, and materializer progress.

- [ ] **Step 1: Write RED quiescence/lock-order tests**

Name tests `fence_waits_preclosure_guard_to_publish_or_abort`, `postclosure_preparer_waits`, `cutoff_is_captured_after_quiescence`, `drain_holds_no_canonical_lock`, `drop_reopens_scope`, and `cancelled_fence_reopens_scope`.

```rust
pub(crate) async fn materialization_fence(
    &self,
    scope: ConflictScope,
) -> Result<MaterializationFence, MutationError>;
```

- [ ] **Step 2: Implement the four-stage primitive**

Close conflicting preparation admission, wait every pre-closure guard to publish or abort, capture and drain the conflicting accepted cutoff without canonical locks, then return a guard whose drop reopens admission. The fence promises visibility/order only, never SSD durability.

- [ ] **Step 3: Run GREEN and commit**

```bash
cd /Volumes/bigssd/projects/ZeroFS/.worktrees/unified-tiered-writeback/zerofs
cargo_test_nonzero 'fs::mutation::fence::tests' -p zerofs --locked
cargo fmt --all -- --check
git diff --check
git add zerofs/src/fs/mutation/fence.rs zerofs/src/fs/mutation/admission.rs zerofs/src/fs/mutation/mod.rs
git commit -m "feat(mutation): add deadlock-safe conflict fences"
```

---

### Task A10: Compose Conflict Fences Into Metadata Operation Families

**Files:**
- Modify: `zerofs/src/fs/ops/create.rs`
- Modify: `zerofs/src/fs/ops/setattr.rs`
- Modify: `zerofs/src/fs/ops/remove.rs`
- Modify: `zerofs/src/fs/ops/rename.rs`
- Modify: `zerofs/src/fs/ops/link.rs`
- Modify: `zerofs/src/fs/ops/io.rs`
- Modify: `zerofs/src/fs/handle.rs`

**Interfaces:**
- Produces: fenced create/truncate/remove/rename/link/trim/fallocate/last-clunk behavior.
- Consumes: `MaterializationFence` and existing sorted canonical lock/revalidation paths.

- [ ] **Step 1: Add RED operation-family tests**

Add one named test per operation family proving a pending overlapping write drains while the canonical lock remains acquirable, then revalidation and canonical mutation happen under the existing lock order. Add write/truncate, write/rename, write/unlink, write/hardlink, and last-clunk permutations.

- [ ] **Step 2: Compose the primitive without duplicating it**

Each operation calculates resolved inode/directory scope, acquires the fence, then acquires canonical locks, revalidates permission/dedup/namespace, applies, releases canonical locks, and drops the fence. No operation waits for materialization under canonical locks.

- [ ] **Step 3: Run GREEN and commit**

```bash
cd /Volumes/bigssd/projects/ZeroFS/.worktrees/unified-tiered-writeback/zerofs
cargo_test_nonzero 'fs::ops::create::tests' -p zerofs --locked
cargo_test_nonzero 'fs::ops::setattr::tests' -p zerofs --locked
cargo_test_nonzero 'fs::ops::remove::tests' -p zerofs --locked
cargo_test_nonzero 'fs::ops::rename::tests' -p zerofs --locked
cargo_test_nonzero 'fs::ops::link::tests' -p zerofs --locked
cargo_test_nonzero 'fs::handle::tests' -p zerofs --locked
cargo fmt --all -- --check
git diff --check
git add zerofs/src/fs/ops/create.rs zerofs/src/fs/ops/setattr.rs zerofs/src/fs/ops/remove.rs zerofs/src/fs/ops/rename.rs zerofs/src/fs/ops/link.rs zerofs/src/fs/ops/io.rs zerofs/src/fs/handle.rs
git commit -m "feat(fs): fence conflicting metadata operations"
```

---

### Task A11: Return Typed Durability Receipts From Filesystem Flush

**Files:**
- Create: `zerofs/src/fs/mutation/durability.rs`
- Modify: `zerofs/src/fs/flush_coordinator.rs`
- Modify: `zerofs/src/writeback/store.rs`
- Modify: `zerofs/src/fs/mutation/mod.rs`

**Interfaces:**
- Produces: `ObjectCoverage`, `DurabilityTarget`, `DurabilityReceipt`, `DurabilityError`, and `FlushCoordinator::durable_through`.
- Consumes: mutation cutoff/progress, sealer, database flush barrier, journal incarnation, and local/remote object progress.

- [ ] **Step 1: Write RED cutoff/receipt tests**

Name tests for seal+DB ordering, object capture while barrier is held, release before backend wait, direct-backend coverage, stale mutation incarnation, stale journal incarnation, local/remote terminal distinction, and no receipt on any partial failure.

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DurabilityTarget {
    LocalSsd,
    RemoteBackend,
}

impl From<ClientDurabilityTarget> for DurabilityTarget {
    fn from(target: ClientDurabilityTarget) -> Self {
        match target {
            ClientDurabilityTarget::LocalSsd => Self::LocalSsd,
            ClientDurabilityTarget::RemoteBackend => Self::RemoteBackend,
        }
    }
}

pub(crate) enum ObjectCoverage {
    DirectRemote,
    Writeback {
        journal_incarnation: JournalIncarnation,
        sequence: crate::writeback::model::Sequence,
    },
}

pub(crate) struct DurabilityReceipt {
    pub(crate) mutation_cutoff: MutationCutoff,
    pub(crate) object_coverage: ObjectCoverage,
    pub(crate) target: DurabilityTarget,
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum DurabilityError {
    #[error("stale mutation incarnation")]
    StaleMutationIncarnation,
    #[error("stale journal incarnation")]
    StaleJournalIncarnation,
    #[error("mutation materialization failed: {0}")]
    Materialization(#[source] FsError),
    #[error("filesystem flush failed: {0}")]
    FilesystemFlush(#[source] anyhow::Error),
    #[error("object durability failed: {0}")]
    Object(#[source] crate::writeback::WritebackError),
    #[error("durability wait closed before target")]
    Closed,
}

pub(crate) async fn durable_through(
    &self,
    cutoff: MutationCutoff,
    target: DurabilityTarget,
) -> Result<DurabilityReceipt, DurabilityError>;
```

- [ ] **Step 2: Implement ordered capture**

Materialize cutoff before taking the DB barrier; while holding it, seal segments, flush metadata, and capture conservative object coverage; release it before waiting local SSD or remote backend. This `From<ClientDurabilityTarget>` implementation is the only conversion or match on the configured client target. Protocols call `.into()` and never choose SSD or remote themselves.

- [ ] **Step 3: Run GREEN and commit**

```bash
cd /Volumes/bigssd/projects/ZeroFS/.worktrees/unified-tiered-writeback/zerofs
cargo_test_nonzero 'fs::mutation::durability::tests' -p zerofs --locked
cargo_test_nonzero 'fs::flush_coordinator::tests' -p zerofs --locked
cargo_test_nonzero 'writeback::store::tests' -p zerofs --locked
cargo fmt --all -- --check
git diff --check
git add zerofs/src/fs/mutation/durability.rs zerofs/src/fs/flush_coordinator.rs zerofs/src/writeback/store.rs zerofs/src/fs/mutation/mod.rs
git commit -m "feat(mutation): return typed filesystem durability receipts"
```

---

### Task A12: Own Bounded Shutdown in One Lifecycle Operation

**Files:**
- Create: `zerofs/src/cli/server/mutation_lifecycle.rs`
- Modify: `zerofs/src/cli/server.rs`
- Modify: `zerofs/src/fs/boot.rs`
- Modify: `zerofs/src/fs/mutation/mod.rs`
- Modify: `zerofs/src/writeback/store.rs`

**Interfaces:**
- Produces: `MutationLifecycle::close`, `ShutdownPhase`, and `ShutdownReceipt`.
- Consumes: listener/dispatched-call owners, mutation admission/materializer, DB barrier/close, object writeback workers, and SFTP pool.

- [ ] **Step 1: Write RED ownership/order tests**

Name tests `close_stops_listeners_before_admission`, `close_drains_dispatched_calls_before_cutoff`, `close_captures_objects_emitted_by_db_close`, `barrier_blocks_crossing_writes_during_db_close`, `close_stops_mutation_before_writeback_before_sftp`, `cancelled_close_retains_owner`, and `timeout_reports_incomplete_phase`.

```rust
pub(crate) async fn close(
    self: std::sync::Arc<Self>,
    deadline: tokio::time::Instant,
    target: DurabilityTarget,
) -> Result<ShutdownReceipt, ShutdownError>;
```

- [ ] **Step 2: Implement the only shutdown order**

The operation executes exactly:

1. stop listeners and new mutation admission;
2. boundedly drain already-dispatched protocol calls;
3. close mutation admission and capture the final mutation cutoff;
4. materialize through that cutoff;
5. acquire the filesystem flush barrier;
6. seal, flush, and close the database while the barrier prevents crossing writes;
7. capture object coverage after database close so close-emitted objects are included;
8. release the barrier;
9. wait the configured `LocalSsd` or `RemoteBackend` target;
10. stop/join mutation workers;
11. stop/join object-writeback workers;
12. stop/join SFTP last.

Cancellation and deadline expiration keep lifecycle ownership alive and report the exact incomplete phase. No later task calls DB/writeback/SFTP close again.

- [ ] **Step 3: Run GREEN and commit**

```bash
cd /Volumes/bigssd/projects/ZeroFS/.worktrees/unified-tiered-writeback/zerofs
cargo_test_nonzero 'cli::server::mutation_lifecycle::tests' -p zerofs --locked
cargo_test_nonzero 'cli::server::tests' -p zerofs --locked
cargo_test_nonzero 'fs::boot::tests' -p zerofs --locked
cargo_test_nonzero 'writeback::store::tests' -p zerofs --locked
cargo fmt --all -- --check
git diff --check
git add zerofs/src/cli/server/mutation_lifecycle.rs zerofs/src/cli/server.rs zerofs/src/fs/boot.rs zerofs/src/fs/mutation/mod.rs zerofs/src/writeback/store.rs
git commit -m "feat(server): own bounded unified writeback shutdown"
```

---

### Task A13: Compose NBD Striped Writes, FUA, and FLUSH

**Files:**
- Modify: `zerofs/src/nbd/handler.rs`
- Modify: `zerofs/src/nbd/server.rs`
- Modify: `zerofs/src/nbd/mod.rs`
- Modify: `zerofs/src/cli/server.rs`

**Interfaces:**
- Produces: one prepared/accepted batch per logical NBD WRITE and typed FUA/FLUSH receipts.
- Consumes: stripe geometry, `PreparationGuard`, NBD request identity, coordinator write path, and normalized client durability target.

- [ ] **Step 1: Write RED composition tests**

Name tests `striped_write_publishes_one_batch`, `active_handle_collision_is_rejected_after_body_drain`, `reply_drop_releases_one_shot_identity`, `fua_covers_every_stripe_member`, `flush_quiesces_prior_preparers`, and `flush_uses_normalized_target`.

- [ ] **Step 2: Route the production handler**

Register `(connection_incarnation, request.cookie)` before payload copy, acquire one preparation guard, map/group stripe chunks by backing inode, acquire sorted locks, and publish all members under one sequence. FUA calls `durable_through` for the write cutoff; FLUSH quiesces preparations then captures all prior accepted writes. Both use the normalized target.

- [ ] **Step 3: Run GREEN and commit**

```bash
cd /Volumes/bigssd/projects/ZeroFS/.worktrees/unified-tiered-writeback/zerofs
cargo_test_nonzero 'nbd::handler::tests' -p zerofs --locked
cargo_test_nonzero 'nbd::server::tests' -p zerofs --locked
cargo test -p nbd-proto --locked
cargo fmt --all -- --check
git diff --check
git add zerofs/src/nbd/handler.rs zerofs/src/nbd/server.rs zerofs/src/nbd/mod.rs zerofs/src/cli/server.rs
git commit -m "feat(nbd): compose shared prepared write batches"
```

---

### Task A14: Retire the NBD-Local Overlay After Parity

**Files:**
- Delete: `zerofs/src/nbd/volatile_overlay.rs`
- Modify: `zerofs/src/nbd/mod.rs`
- Modify: `zerofs/src/nbd/server.rs`
- Modify: `zerofs/src/cli/server.rs`
- Modify: `zerofs/src/config.rs`

**Interfaces:**
- Produces: no NBD-local volatile queue, drain owner, terminal state, or exclusivity rule.
- Consumes: shared equivalents proven in Tasks A6-A13.

- [ ] **Step 1: Add RED retirement tests and prove the old filter exists**

Add `nbd_uses_no_protocol_local_overlay_owner` and `legacy_nbd_inputs_normalize_without_exclusivity` while the old module still exists; they fail until old construction/drain/exclusivity wiring is removed. Admission tests move to `fs::mutation::admission`, overlay tests to `data_overlay`, ordering tests to `materializer`, and terminal/close tests to `progress`/lifecycle. Before deletion, run the exact list/filter gate:

```bash
cd /Volumes/bigssd/projects/ZeroFS/.worktrees/unified-tiered-writeback/zerofs
set -o pipefail
cargo test -p zerofs --lib --locked -- --list | tee /tmp/zerofs-old-overlay-tests.list
grep -Fx 'nbd::tests::nbd_uses_no_protocol_local_overlay_owner: test' /tmp/zerofs-old-overlay-tests.list
grep -Fx 'config::tests::legacy_nbd_inputs_normalize_without_exclusivity: test' /tmp/zerofs-old-overlay-tests.list
if cargo test -p zerofs nbd::tests::nbd_uses_no_protocol_local_overlay_owner --locked -- --exact --nocapture 2>&1 | tee /tmp/zerofs-old-overlay-red.run; then exit 1; fi
grep -Eq 'test result: FAILED\. 0 passed; 1 failed' /tmp/zerofs-old-overlay-red.run
if cargo test -p zerofs config::tests::legacy_nbd_inputs_normalize_without_exclusivity --locked -- --exact --nocapture 2>&1 | tee /tmp/zerofs-old-exclusivity-red.run; then exit 1; fi
grep -Eq 'test result: FAILED\. 0 passed; 1 failed' /tmp/zerofs-old-exclusivity-red.run
grep -F 'nbd::volatile_overlay::tests::' /tmp/zerofs-old-overlay-tests.list
test "$(grep -Fc 'nbd::volatile_overlay::tests::' /tmp/zerofs-old-overlay-tests.list)" -gt 0
cargo test -p zerofs nbd::volatile_overlay::tests:: --locked -- --nocapture 2>&1 | tee /tmp/zerofs-old-overlay-tests.run
grep -Eq 'test result: ok\. [1-9][0-9]* passed' /tmp/zerofs-old-overlay-tests.run
```

- [ ] **Step 2: Delete only after the shared module gate is green**

```bash
cd /Volumes/bigssd/projects/ZeroFS/.worktrees/unified-tiered-writeback/zerofs
cargo_test_nonzero 'fs::mutation::' -p zerofs --locked
cargo_test_nonzero 'nbd::' -p zerofs --locked
cargo fmt --all -- --check
git diff --check
git add zerofs/src/nbd/volatile_overlay.rs zerofs/src/nbd/mod.rs zerofs/src/nbd/server.rs zerofs/src/cli/server.rs zerofs/src/config.rs
git commit -m "refactor(nbd): retire protocol-local volatile overlay"
```

---

### Task A15: Compose 9P, Direct, RPC, and WebUI Production Paths

**Files:**
- Modify: `zerofs/ninep-proto/src/protocol.rs`
- Modify: `zerofs/ninep-client/src/lib.rs`
- Modify: `zerofs/src/ninep/handler.rs`
- Modify: `zerofs/src/fs/boot.rs`
- Modify: `zerofs/src/cli/transfer/copy.rs`
- Modify: `zerofs/src/cli/transfer/tests.rs`
- Modify: `zerofs/src/rpc/server.rs`
- Modify: `zerofs/src/webui.rs`

**Interfaces:**
- Produces: 9P lineage/barriers and real direct, admin-RPC, gRPC-Web, and WebSocket/9P writes through the shared coordinator.
- Consumes: existing operation IDs, `DurabilityLineage`, request cache, visible reads, typed durability, and lifecycle-dispatch tracking.

- [ ] **Step 1: Write RED production-path tests**

Name tests for 9P request identity/reconnect, `Tfsync` and `Tfsyncdur`, direct upload pending visibility and fenced rename, real Unix admin RPC upload/write, gRPC-Web/WebSocket pending read, terminal fanout, and listener dispatch tracking. Use the existing real `AdminRpcServer` + `RpcClient` Unix-socket harness; do not substitute a mock service.

- [ ] **Step 2: Compose every real caller**

9P standard/verified fsync captures current obligations and requests the normalized target. Direct transfer creates its private file canonically, writes through shared `ZeroFS`, materializes and syncs before atomic rename. Admin RPC calls the same `Arc<ZeroFS>`. WebUI gRPC-Web and WebSocket/9P preserve those real server paths.

- [ ] **Step 3: Run focused production and browser gates**

```bash
cd /Volumes/bigssd/projects/ZeroFS/.worktrees/unified-tiered-writeback/zerofs
cargo test -p ninep-proto --locked
cargo_test_nonzero 'durability_tracking_tests' -p ninep-client --locked
cargo_test_nonzero 'ninep::handler::tests' -p zerofs --locked
cargo_test_nonzero 'rpc::server::tests' -p zerofs --features webui --locked
cargo_test_nonzero 'cli::transfer::tests' -p zerofs --features webui --locked
cargo check -p ninep-client --target wasm32-unknown-unknown --locked
cd /Volumes/bigssd/projects/ZeroFS/.worktrees/unified-tiered-writeback
make webui
cd zerofs
cargo test -p zerofs --features webui --locked -- --list | tee /tmp/zerofs-wasm-smoke.list
grep -Fx 'webui::tests::wasm_client_smoke: test' /tmp/zerofs-wasm-smoke.list
cargo test -p zerofs --features webui webui::tests::wasm_client_smoke --locked -- --ignored --exact --nocapture 2>&1 | tee /tmp/zerofs-wasm-smoke.run
grep -Eq 'test result: ok\. 1 passed' /tmp/zerofs-wasm-smoke.run
cargo fmt --all -- --check
git diff --check
```

- [ ] **Step 4: Commit the exact fence**

```bash
git add zerofs/ninep-proto/src/protocol.rs zerofs/ninep-client/src/lib.rs zerofs/src/ninep/handler.rs zerofs/src/fs/boot.rs zerofs/src/cli/transfer/copy.rs zerofs/src/cli/transfer/tests.rs zerofs/src/rpc/server.rs zerofs/src/webui.rs
git commit -m "feat: compose shared writes through 9p rpc and webui"
```

---

### Task A16: Land the Additive `ScriptedAlchemy/nfsserve` Context API

**Files in separately inventoried dependency worktree:**
- Modify: `src/nfs.rs`
- Modify: `src/vfs.rs`
- Modify: `src/nfs_handlers.rs`
- Modify: `src/rpcwire.rs`

**Worktree exception and ownership:**
- The `ScriptedAlchemy/nfsserve` dependency worktree is explicitly exempt from the single-ZeroFS-worktree rule.
- Root is the one landing owner for both repositories.
- Upstream base is exactly `d61b08456ae66108666978e29524a47d2209f68d`.
- Create branch `codex/zerofs-write-context` from that base in the exact dependency worktree `/Volumes/bigssd/projects/nfsserve/.worktrees/zerofs-write-context`; record that path, branch, exact HEAD, `git status --short`, and `git diff --name-only` before edits.

**Interfaces:**
- Produces: additive `RpcRequestContext`, `WriteRequestContext`, `WriteResult`, `write_with_context`, and `commit_with_context` without breaking legacy implementors.
- Consumes: decoded XID/stable-how/client address and write verifier serialization.

- [ ] **Step 1: Add RED fork tests**

```rust
pub struct RpcRequestContext {
    pub xid: u32,
    pub client_addr: String,
    pub connection_incarnation: u64,
}

pub struct WriteRequestContext {
    pub rpc: RpcRequestContext,
    pub requested_stability: stable_how,
}

pub struct WriteResult {
    pub attributes: fattr3,
    pub committed: stable_how,
    pub verifier: writeverf3,
}

pub struct CommitRequestContext {
    pub rpc: RpcRequestContext,
}

pub struct CommitResult {
    pub verifier: writeverf3,
}

#[async_trait::async_trait]
pub trait NFSFileSystem: Sync {
    async fn write(
        &self,
        auth: &AuthContext,
        id: fileid3,
        offset: u64,
        data: &[u8],
    ) -> Result<fattr3, nfsstat3>;

    async fn write_with_context(
        &self,
        context: &WriteRequestContext,
        auth: &AuthContext,
        id: fileid3,
        offset: u64,
        data: &[u8],
    ) -> Result<WriteResult, nfsstat3> {
        let _ = context;
        let attributes = self.write(auth, id, offset, data).await?;
        Ok(WriteResult {
            attributes,
            committed: stable_how::FILE_SYNC,
            verifier: self.get_write_verf(),
        })
    }

    async fn commit(
        &self,
        _auth: &AuthContext,
        _fileid: fileid3,
        _offset: u64,
        _count: u32,
    ) -> Result<writeverf3, nfsstat3> {
        Ok(self.get_write_verf())
    }

    async fn commit_with_context(
        &self,
        context: &CommitRequestContext,
        auth: &AuthContext,
        fileid: fileid3,
        offset: u64,
        count: u32,
    ) -> Result<CommitResult, nfsstat3> {
        let _ = context;
        Ok(CommitResult {
            verifier: self.commit(auth, fileid, offset, count).await?,
        })
    }

    fn get_write_verf(&self) -> writeverf3 {
        [0u8; NFS3_WRITEVERFSIZE as usize]
    }
}
```

This is the relevant additive excerpt; every unrelated upstream trait method remains unchanged. The trait bound remains exactly `Sync`; the additive API must not impose a new `Send` bound. The existing `write` keeps `auth: &AuthContext` and by-value `id: fileid3`. The existing `commit` keeps `auth: &AuthContext`, by-value `fileid3`, offset/count, and its default delegation to `get_write_verf`; `get_write_verf` keeps its zero-verifier default. The two contextual methods add context without weakening or replacing those defaults, forward the same auth reference and by-value file ID exactly once, and delegate once to the legacy method; no handler calls both paths. The WRITE handler constructs `WriteRequestContext`, and the COMMIT handler constructs `CommitRequestContext`, then passes decoded auth plus the original by-value file ID/offset/count. Test that the server mints a fresh `connection_incarnation` per accepted transport, address reuse after reconnect changes it, XID/stability/auth reach the VFS, returned committed/verifier reach the wire, invalid stable-how returns garbage args, COMMIT forwards auth/XID/range, default delegation calls each legacy method exactly once, a `Sync` but non-`Send` legacy implementor compiles, a legacy implementor that omits `commit` and `get_write_verf` still receives the existing defaults, and all legacy trait implementors still compile.

- [ ] **Step 2: Implement and validate the additive API**

From the dependency worktree root:

```bash
cargo fmt --all -- --check
cargo clippy --all-targets -- -D warnings
cargo test --all-targets
git diff --check
git status --short
git diff --name-only
```

The dirty fence must list only the four named files. Commit only those exact files:

```bash
git add src/nfs.rs src/vfs.rs src/nfs_handlers.rs src/rpcwire.rs
git commit -m "feat: expose NFS write request context"
git push -u origin codex/zerofs-write-context
```

Record the pushed commit SHA, prove `git ls-remote origin refs/heads/codex/zerofs-write-context` returns it, and use that immutable SHA as `NFS_CONTEXT_REV` in Task A17. A branch name alone is never an acceptable ZeroFS dependency pin.

---

### Task A17: Compose Honest ZeroFS NFS WRITE and COMMIT

**Files:**
- Modify: `zerofs/Cargo.toml`
- Modify: `zerofs/Cargo.lock`
- Modify: `zerofs/src/nfs.rs`
- Modify: `zerofs/src/cli/server.rs`

**Interfaces:**
- Produces: NFS connection-scoped request identity, truthful stable replies, restart verifier, and COMMIT through typed durability.
- Consumes: immutable `NFS_CONTEXT_REV`, request cache, shared writes, visible reads, and normalized client durability target.

- [ ] **Step 1: Write RED ZeroFS NFS tests**

Name tests `unstable_write_returns_without_explicit_barrier`, `file_sync_waits_configured_target`, `commit_covers_prior_cross_adapter_cutoff`, `write_and_commit_share_service_verifier`, `service_restart_changes_nonzero_verifier`, `credential_or_stability_fingerprint_mismatch_is_rejected`, and `reconnect_address_reuse_does_not_join_old_xid`.

- [ ] **Step 2: Pin and compose**

Pin `ScriptedAlchemy/nfsserve` by repository URL plus exact `rev = "${NFS_CONTEXT_REV}"`. Generate one random 8-byte verifier per `start_nfs_servers` invocation. Construct NFS identity from server incarnation + server-minted connection incarnation + XID; include address, credentials, ranges, payload hash, requested stability, and durability in the fingerprint. `UNSTABLE` returns after configured write acknowledgement and reports `UNSTABLE`; `DATA_SYNC`/`FILE_SYNC` and COMMIT wait the normalized client durability target and report the achieved stable level.

- [ ] **Step 3: Run GREEN and commit**

```bash
cd /Volumes/bigssd/projects/ZeroFS/.worktrees/unified-tiered-writeback/zerofs
cargo_test_nonzero 'nfs::tests' -p zerofs --locked
cargo_test_nonzero 'cli::server::tests' -p zerofs --locked
cargo test --workspace --all-targets --locked
cargo fmt --all -- --check
git diff --check
git add zerofs/Cargo.toml zerofs/Cargo.lock zerofs/src/nfs.rs zerofs/src/cli/server.rs
git commit -m "feat(nfs): report real write stability and durability"
```

Record upstream base, fork branch, fork test receipt, immutable fork revision, remote push receipt, and Root as landing owner in the ZeroFS commit body.

---

### Task A18: Expose Lifecycle State, Metrics, and Public Documentation

**Files:**
- Create: `zerofs/src/fs/mutation/metrics.rs`
- Modify: `zerofs/src/fs/mutation/mod.rs`
- Modify: `zerofs/src/cli/server/mutation_lifecycle.rs`
- Modify: `zerofs/src/prometheus.rs`
- Modify: `zerofs/src/config.rs`
- Modify: `README.md`

**Interfaces:**
- Produces: mutation/writeback status and bounded-cardinality metrics.
- Consumes: Task A12 `ShutdownPhase`/receipt and coordinator snapshots; it does not own or call shutdown.

- [ ] **Step 1: Write RED metrics/status tests**

Cover accepted/materialized sequence and lag, raw bytes/ops/age, active materializers, local/remote sequence and lag, NFS stability counts, terminal cause class, and shutdown phase/incomplete target. Reject path, inode, operation ID, request fingerprint, and error-string labels.

- [ ] **Step 2: Implement observability and docs**

Subscribe to lifecycle state and snapshots. Document materialized default, explicit RAM-loss boundary, protocol durability semantics, namespace separation, legacy migration, sizing, metrics, and explicit remote flush. Do not edit the approved spec.

- [ ] **Step 3: Run the pre-read composition gate and commit**

```bash
cd /Volumes/bigssd/projects/ZeroFS/.worktrees/unified-tiered-writeback/zerofs
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo test --workspace --all-targets --locked
cargo check -p ninep-client --target wasm32-unknown-unknown --locked
cd /Volumes/bigssd/projects/ZeroFS/.worktrees/unified-tiered-writeback
git diff --check origin/develop...HEAD
git add zerofs/src/fs/mutation/metrics.rs zerofs/src/fs/mutation/mod.rs zerofs/src/cli/server/mutation_lifecycle.rs zerofs/src/prometheus.rs zerofs/src/config.rs README.md
git commit -m "docs(writeback): expose shared mutation lifecycle"
```

Expected: the old NBD overlay is absent; generated config remains materialized; every writable adapter reaches the shared coordinator; lifecycle has exactly one close owner; no approved-spec diff exists.

---

### Task A19: Fetch Independent Fragmented Read Runs With Bounded Concurrency

**Files:**
- Modify: `zerofs/src/fs/store/extent/read.rs`
- Create: `zerofs/src/fs/store/extent/read/run_fetch.rs`
- Create: `zerofs/src/fs/store/extent/read/tests.rs`
- Create: `zerofs/src/fs/store/extent/read/metrics.rs`

**Interfaces:**
- Produces: an ordered read-run plan, bounded concurrent fetch of independent immutable on-store segment runs, and bounded-cardinality logical/read-run utilization metrics.
- Consumes: the existing extent-location range scan, decoded/open-buffer fast paths, `SegmentStore::read_run`, stale-location re-resolution, nomination/crossing accounting, and the existing `PARALLEL_EXTENT_OPS` bound.

- [ ] **Step 1: Split the existing tests and production worker without behavior change**

Move the inline `read.rs` test module to `read/tests.rs` before adding behavior. Extract the existing maximal-run planning and run-fetch control flow from the current `read_range` body into `read/run_fetch.rs`; `read_range` remains a facade that performs validation, delegates planning/fetch, and assembles the result. No async function may exceed 100 lines, `read.rs` and `run_fetch.rs` must each remain below 600 production lines, and source plus tests remain below 1000 lines per file. Run the complete existing module gate and require a nonzero pass count.

```bash
cd /Volumes/bigssd/projects/ZeroFS/.worktrees/unified-tiered-writeback/zerofs
cargo_test_nonzero 'fs::store::extent::read::tests' -p zerofs --locked
cargo fmt --all -- --check
git diff --check
```

- [ ] **Step 2: Add the focused RED tests**

Name tests:

- `one_fragmented_read_fetches_independent_runs_concurrently`
- `fragmented_read_concurrency_is_bounded`
- `fragmented_read_preserves_logical_output_order`
- `contiguous_control_remains_one_ranged_get`
- `stale_location_fallback_remains_correct_under_concurrency`
- `failed_fragmented_read_releases_every_fetch_permit`

Use a latency-gated, peak-concurrency-counting real `ObjectStore` test seam behind the production extent/segment path. Build a logically sequential file whose adjacent extents occupy at least eight independent segment runs. Before releasing any GET, require at least two and at most `PARALLEL_EXTENT_OPS` backend reads to have started. Verify exact bytes and the exact run count. Current code is RED because `read_range` awaits each on-store run before starting the next.

List every fully qualified test first. Run the two concurrency assertions against the pre-fix implementation and require a failing exit. Run the four order/fallback/cleanup controls against the pre-fix implementation and require an exact pass; they protect existing behavior and are not artificial REDs. A missing test or zero selected tests fails either gate.

```bash
cd /Volumes/bigssd/projects/ZeroFS/.worktrees/unified-tiered-writeback/zerofs
READ_TEST_PREFIX='fs::store::extent::read::tests::'
cargo test -p zerofs --locked -- --list 2>&1 | tee "${TMPDIR:-/tmp}/zerofs-read-red-list.log"
for name in \
  one_fragmented_read_fetches_independent_runs_concurrently \
  fragmented_read_concurrency_is_bounded
do
  grep -F "${READ_TEST_PREFIX}${name}: test" "${TMPDIR:-/tmp}/zerofs-read-red-list.log"
  if cargo test -p zerofs --locked "${READ_TEST_PREFIX}${name}" -- --exact --nocapture; then
    echo "expected RED but ${name} passed" >&2
    exit 1
  fi
done
for name in \
  fragmented_read_preserves_logical_output_order \
  contiguous_control_remains_one_ranged_get \
  stale_location_fallback_remains_correct_under_concurrency \
  failed_fragmented_read_releases_every_fetch_permit
do
  grep -F "${READ_TEST_PREFIX}${name}: test" "${TMPDIR:-/tmp}/zerofs-read-red-list.log"
  cargo_test_nonzero "${READ_TEST_PREFIX}${name}" -p zerofs --locked
done
```

- [ ] **Step 3: Implement the minimum shared read fix**

Resolve and coalesce the existing maximal runs first. Serve decoded/open-buffer runs through their current fast paths. Fetch independent immutable on-store runs with bounded ordered concurrency, then assemble results in logical order. Preserve:

- decoded-cache and raw-part-cache identities;
- stale-location re-resolution and retry behavior;
- extent crossing and nomination accounting;
- exact zero-fill/EOF behavior;
- one ranged GET for a contiguous single-segment run;
- cancellation/error cleanup with no leaked permits or background tasks.

The dedicated metrics owner records logical bytes, extent/run counts, unique segment
count, on-store run count/bytes, active and peak run fetches, and total read duration.
It uses no inode, path, object key, request ID, or error-string label. Cache-tier proof
remains a benchmark receipt derived from isolated process/cache roots plus local-device
and network counters; do not fabricate a RAM/SSD/remote label from unavailable cache
internals.

Do not change NFS framing, SFTP packet geometry, cache policy, write acknowledgement, durability, or object layout in this task. Further NFS copy/framing work requires a separate measured RED after this shared fix.

- [ ] **Step 4: Run exact GREEN, the shared-read gate, and exact-fence commit**

```bash
cd /Volumes/bigssd/projects/ZeroFS/.worktrees/unified-tiered-writeback/zerofs
READ_TEST_PREFIX='fs::store::extent::read::tests::'
for name in \
  one_fragmented_read_fetches_independent_runs_concurrently \
  fragmented_read_concurrency_is_bounded \
  fragmented_read_preserves_logical_output_order \
  contiguous_control_remains_one_ranged_get \
  stale_location_fallback_remains_correct_under_concurrency \
  failed_fragmented_read_releases_every_fetch_permit
do
  cargo_test_nonzero "${READ_TEST_PREFIX}${name}" -p zerofs --locked
done
cargo_test_nonzero 'fs::store::extent::read::tests' -p zerofs --locked
cargo_test_nonzero 'fs::ops::io::tests' -p zerofs --locked
cargo_test_nonzero 'segment_store::tests' -p zerofs --locked
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo test --workspace --all-targets --locked
cargo check -p ninep-client --target wasm32-unknown-unknown --locked
git diff --check
git add zerofs/src/fs/store/extent/read.rs zerofs/src/fs/store/extent/read/run_fetch.rs zerofs/src/fs/store/extent/read/tests.rs zerofs/src/fs/store/extent/read/metrics.rs
git commit -m "perf(read): pipeline fragmented segment runs"
```

Expected: the fragmented RED proves peak backend concurrency greater than one; every bounded/error/order control is green; contiguous reads remain one GET; no protocol-specific behavior or durability semantics changed.

---

### Task A20: Bound Aggregate Resident Memory and Cache Admission

**Files:**
- Create: `zerofs/src/resident_memory.rs`
- Modify: `zerofs/src/lib.rs`
- Modify: `zerofs/src/main.rs`
- Modify: `zerofs/src/config.rs`
- Modify: `zerofs/src/cli/server.rs`
- Modify: `zerofs/src/nfs.rs`
- Modify: `zerofs/src/fs/store/extent/mod.rs`
- Modify: `zerofs/src/fs/store/extent/write.rs`
- Modify: `zerofs/src/fs/store/extent/compact.rs`
- Modify: `zerofs/src/fs/store/extent/reclaim.rs`
- Modify: `zerofs/src/fs/store/read_cache.rs`
- Modify: `zerofs/src/fs/mutation/overlay.rs`
- Modify: `zerofs/src/fs/mutation/request_cache.rs`
- Modify: `zerofs/src/segment_store.rs`
- Modify: `zerofs/src/object_store_prefetch.rs`
- Modify: `zerofs/src/writeback/config.rs`
- Modify: `zerofs/src/writeback/store.rs`
- Modify: `zerofs/src/writeback/journal.rs`
- Modify: `zerofs/src/writeback/journaler.rs`
- Modify: `zerofs/src/prometheus.rs`

**Interfaces:**
- Produces: `ResidentMemoryConfig`, `ResidentMemoryKind`, `ResidentMemoryBudget`, `ResidentMemoryPermit`, conservative cache weighers, write-no-allocate, maintenance no-admit reads, and bounded-cardinality resident-memory metrics.
- Consumes: configured clean-cache/writeback/volatile budgets, finite Linux cgroup-v2 `memory.max` when present, cache keys/values, segment buffers, object-prefetch parts, GC/compaction reads, and the generic admission primitive from A1.

- [ ] **Step 1: Write the configuration and ownership RED tests**

Add exact tests:

- `resident_budget_rejects_payload_budgets_without_configured_reserve`
- `finite_cgroup_limit_is_an_additional_hard_ceiling`
- `unlimited_or_unavailable_cgroup_uses_configured_limit`
- `resident_permit_charges_payload_overhead_and_replacement_once`
- `resident_permit_releases_on_cancel_error_and_drop`
- `ordinary_chunked_nfs_write_does_not_admit_decoded_extent_cache`
- `compaction_reads_do_not_admit_raw_parts_or_decoded_cache`
- `compaction_groups_adjacent_source_runs_under_one_bounded_scan`
- `sparse_interleaved_256_mib_segment_uses_at_most_thirty_two_verification_scans`
- `reclaim_scan_row_or_byte_budget_exhaustion_fails_closed`
- `cache_weigher_includes_key_entry_and_allocator_slack`
- `dirty_ram_zero_does_not_satisfy_resident_headroom`
- `physical_residency_does_not_sum_jemalloc_resident_and_retained`
- `retained_only_growth_does_not_consume_physical_headroom`

Use a temporary cgroup-file seam that parses literal `memory.max` and `memory.events`
contents; it is a unit seam for parsing/accounting, not Linux acceptance. Build a real
ordinary chunked NFS write path and real compacted segment fixture for the
write-no-allocate/no-admit
tests. Current code is RED because decoded extent and parts weighers charge payload
length only, every canonical write calls `decoded_insert`, and compaction reads through
the cache-admitting segment path. The reclaim fixture creates a real maximum supported
256 MiB compacted segment at 32 KiB extents: approximately 8,192 candidate frames whose
`(inode, extent)` keys are deliberately sparse and interleaved. Maximal-consecutive-run
grouping could otherwise issue about 16,384 memory/durable point reads.
Before invoking reclaim, the test asserts `logical_payload_bytes == 268435456` and
`candidate_frame_count == 8192`; a smaller fixture cannot satisfy the test by name.
The fixture also counts every scanned row and encoded byte; an adversarial fixture
places unrelated rows between desired keys and has separate table cases that cross the
65,536-row ceiling and the 64 MiB encoded-byte ceiling. Each must stop at the first
exhausted budget and return `Keep` rather than continue scanning.

```bash
cd /Volumes/bigssd/projects/ZeroFS/.worktrees/unified-tiered-writeback/zerofs
MEMORY_TEST_PREFIX='resident_memory::tests::'
cargo test -p zerofs --locked -- --list 2>&1 | tee "${TMPDIR:-/tmp}/zerofs-memory-red-list.log"
for name in \
  resident_budget_rejects_payload_budgets_without_configured_reserve \
  finite_cgroup_limit_is_an_additional_hard_ceiling \
  resident_permit_charges_payload_overhead_and_replacement_once \
  ordinary_chunked_nfs_write_does_not_admit_decoded_extent_cache \
  compaction_reads_do_not_admit_raw_parts_or_decoded_cache \
  compaction_groups_adjacent_source_runs_under_one_bounded_scan \
  physical_residency_does_not_sum_jemalloc_resident_and_retained \
  retained_only_growth_does_not_consume_physical_headroom
do
  grep -F "${MEMORY_TEST_PREFIX}${name}: test" "${TMPDIR:-/tmp}/zerofs-memory-red-list.log"
  if cargo test -p zerofs --locked "${MEMORY_TEST_PREFIX}${name}" -- --exact --nocapture; then
    echo "expected resident-memory RED but ${name} passed" >&2
    exit 1
  fi
done
RECLAIM_RED='fs::store::extent::reclaim::tests::sparse_interleaved_256_mib_segment_uses_at_most_thirty_two_verification_scans'
cargo test -p zerofs --locked -- --list 2>&1 | tee "${TMPDIR:-/tmp}/zerofs-reclaim-red-list.log"
grep -F "${RECLAIM_RED}: test" "${TMPDIR:-/tmp}/zerofs-reclaim-red-list.log"
if cargo test -p zerofs --locked "$RECLAIM_RED" -- --exact --nocapture; then
  echo "expected reclaim scan-bound RED but ${RECLAIM_RED} passed" >&2
  exit 1
fi
RECLAIM_BUDGET_RED='fs::store::extent::reclaim::tests::reclaim_scan_row_or_byte_budget_exhaustion_fails_closed'
grep -F "${RECLAIM_BUDGET_RED}: test" "${TMPDIR:-/tmp}/zerofs-reclaim-red-list.log"
if cargo test -p zerofs --locked "$RECLAIM_BUDGET_RED" -- --exact --nocapture; then
  echo "expected reclaim row/byte-budget RED but ${RECLAIM_BUDGET_RED} passed" >&2
  exit 1
fi
```

- [ ] **Step 2: Define exact aggregate ownership types and validation**

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) enum ResidentMemoryKind {
    CleanDecoded,
    CleanRawPart,
    CleanMetadata,
    ProtocolIngress,
    VolatileMutation,
    ObjectWriteback,
    OpenSegment,
    Seal,
    Compaction,
    RequestReplay,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct ResidentMemoryConfig {
    pub(crate) configured_limit_bytes: u64,
    pub(crate) reserve_bytes: u64,
}

pub(crate) struct ResidentMemoryBudget {
    inner: std::sync::Arc<ResidentMemoryBudgetInner>,
}

struct ResidentMemoryBudgetInner {
    effective_limit_bytes: u64,
    reserve_bytes: u64,
    admission: crate::coordination::Admission,
    charged_by_kind: std::sync::Mutex<
        std::collections::BTreeMap<ResidentMemoryKind, u64>,
    >,
    peak_bytes: std::sync::atomic::AtomicU64,
}

pub(crate) struct ResidentMemoryPermit {
    budget: std::sync::Weak<ResidentMemoryBudgetInner>,
    kind: ResidentMemoryKind,
    charged_bytes: u64,
    active: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ResidentMemorySnapshot {
    pub(crate) effective_limit_bytes: u64,
    pub(crate) reserve_bytes: u64,
    pub(crate) charged_bytes: u64,
    pub(crate) peak_bytes: u64,
    pub(crate) process_resident_bytes: u64,
    pub(crate) cgroup_current_bytes: Option<u64>,
    pub(crate) jemalloc_resident_bytes: Option<u64>,
    pub(crate) jemalloc_retained_virtual_bytes: Option<u64>,
    pub(crate) baseline_bytes: u64,
    pub(crate) unowned_residual_bytes: u64,
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum ResidentMemoryError {
    #[error("resident-memory budget is closed")]
    Closed,
    #[error("resident-memory budget is poisoned")]
    Poisoned,
    #[error("resident-memory request exceeds the effective limit")]
    TooLarge,
}

impl ResidentMemoryBudget {
    pub(crate) async fn acquire(
        &self,
        kind: ResidentMemoryKind,
        payload_bytes: u64,
        ownership_overhead_bytes: u64,
    ) -> Result<ResidentMemoryPermit, ResidentMemoryError>;

    pub(crate) fn snapshot(&self) -> ResidentMemorySnapshot;
}
```

Normalize `[memory].resident_limit_gb` and `[memory].resident_reserve_gb` to exact
bytes. The effective limit is `min(configured_limit, finite cgroup memory.max)`.
Startup sums every configured payload owner, conservative key/entry/allocator slack,
maximum replacement overlap, maintenance working-set maxima, and the reserve. The
example 64+16 GiB profile therefore requires the documented 128 GiB envelope; under a
96 GiB cgroup it fails before listeners start. Do not derive success from dirty-
writeback RAM. The runtime budget uses one cancellation-safe owner transfer per
allocation; no layer temporarily owns uncharged bytes. Mount `ResidentMemoryPermit`
ownership into the decoded/read-metadata caches, raw-part/Foyer buffers, mutation
overlay, retained request cache, open/sealing segments, object-writeback RAM and
journal staging, and compaction work. No enum variant may remain metrics-only.

- [ ] **Step 3: Make write and maintenance cache admission explicit**

Add a typed `CacheAdmission::{Read, NoAdmit}` argument below the extent and segment
facades. User reads keep current cache behavior. Every canonical write, including each
ordinary chunked NFS rsync write, uses `NoAdmit` and cannot call `decoded_insert`;
pending-read coherence remains owned by the shared mutation overlay until canonical
state is visible. GC/compaction uses 16 fixed batches of at most 512 sorted forward-map keys;
each batch is merge-checked by one streaming memory-view scan and one durable-view scan,
so a full supported 256 MiB segment with approximately 8,192 frames issues at most 32
scans even when every key is sparse or interleaved. The two views contain about 16,384
desired rows; the fixed 65,536-row aggregate ceiling permits at most four times that
geometry, including bounded unrelated gap rows, while the independent encoded
key/value ceiling remains 64 MiB. Exceeding either fixed budget stops immediately and
returns `Keep`; it never falls back to one point read per frame. Absent forward keys mean
dead frames as today, while any decode error, scan error, or reference to the segment
fails closed to `Keep`. The scans
use `NoAdmit` for decoded and raw-part caches. Cache weighers include key size, entry/container overhead,
and documented allocator slack rather than `value.len()` alone. Replacement ownership
may overlap only inside its precharged maximum.

- [ ] **Step 4: Expose reconciled metrics and run GREEN**

Expose configured/effective limit, reserve, charged/peak bytes by
`ResidentMemoryKind`, aggregate waiters/backpressure, cache replacement bytes,
maintenance working bytes/no-admit reads, allocator allocated/resident/retained, and
Linux cgroup current/max/events. Labels never contain paths, keys, request IDs, or raw
errors. Physical residency comes from OS RSS and, when available, cgroup
`memory.current`; `jemalloc.stats.resident` is a correlation metric and
`jemalloc.stats.retained` is retained virtual address space. Never add resident and
retained or use retained alone as physical RSS/admission pressure. Reconciliation measures process RSS and cgroup `memory.current`, then records
`owned + calibrated idle baseline + unowned residual = observed current`. The residual
has a conservative configured ceiling and triggers backpressure/fail-closed poison if
it escapes tolerance; merely proving charged owners sum to themselves is rejected.
Plan C freezes `cgroup_high_event_delta_max=8`,
`reconciliation_error_bytes_max=268435456`, and
`unowned_residual_bytes_max=2147483648` in the immutable ledger before starting the
process. Dependency-free rejection tests prove the scenario cannot omit, mutate after
setup, or derive these values from its observed peak.

```bash
cd /Volumes/bigssd/projects/ZeroFS/.worktrees/unified-tiered-writeback/zerofs
cargo_test_nonzero 'resident_memory::tests' -p zerofs --locked
cargo_test_nonzero 'fs::store::extent::tests' -p zerofs --locked
cargo_test_nonzero 'fs::store::read_cache::tests' -p zerofs --locked
cargo_test_nonzero 'fs::mutation::request_cache::tests' -p zerofs --locked
cargo_test_nonzero 'object_store_prefetch::tests' -p zerofs --locked
cargo_test_nonzero 'segment_store::tests' -p zerofs --locked
cargo_test_nonzero 'writeback::store::tests' -p zerofs --locked
cargo_test_nonzero 'writeback::journaler::tests' -p zerofs --locked
cargo_test_nonzero 'config::tests' -p zerofs --locked
cargo_test_nonzero 'fs::store::extent::reclaim::tests::sparse_interleaved_256_mib_segment_uses_at_most_thirty_two_verification_scans' -p zerofs --locked
cargo_test_nonzero 'fs::store::extent::reclaim::tests::reclaim_scan_row_or_byte_budget_exhaustion_fails_closed' -p zerofs --locked
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo test --workspace --all-targets --locked
git diff --check
git add zerofs/src/resident_memory.rs zerofs/src/lib.rs zerofs/src/main.rs zerofs/src/config.rs zerofs/src/cli/server.rs zerofs/src/nfs.rs zerofs/src/fs/store/extent/mod.rs zerofs/src/fs/store/extent/write.rs zerofs/src/fs/store/extent/compact.rs zerofs/src/fs/store/extent/reclaim.rs zerofs/src/fs/store/read_cache.rs zerofs/src/fs/mutation/overlay.rs zerofs/src/fs/mutation/request_cache.rs zerofs/src/segment_store.rs zerofs/src/object_store_prefetch.rs zerofs/src/writeback/config.rs zerofs/src/writeback/store.rs zerofs/src/writeback/journal.rs zerofs/src/writeback/journaler.rs zerofs/src/prometheus.rs
git commit -m "fix(memory): bound server residency and cache admission"
```

Expected: payload-only cache limits are replaced by conservative ownership charges;
write-only streams and maintenance do not pollute clean caches; the aggregate snapshot
has an explicit reserve below the effective limit.

---

### Task A21: Bound Protocol Ingress Before Owned Payload Copies

**Files:**
- Create: `zerofs/src/fs/mutation/ingress.rs`
- Modify: `zerofs/src/fs/mutation/mod.rs`
- Modify: `zerofs/src/fs/mutation/request_cache.rs`
- Modify: `zerofs/src/ninep/server.rs`
- Modify: `zerofs/src/ninep/handler.rs`
- Modify: `zerofs/src/nfs.rs`
- Modify: `zerofs/src/webui.rs`
- Modify: `zerofs/src/rpc/server.rs`
- Modify: `zerofs/src/cli/server.rs`
- Modify: `zerofs/src/prometheus.rs`
- Modify in the separately owned fork: `src/context.rs`
- Modify in the separately owned fork: `src/rpcwire.rs`
- Modify in the separately owned fork: `src/tcp.rs`
- Modify after the fork is pushed: `zerofs/Cargo.toml`
- Modify after the fork is pushed: `zerofs/Cargo.lock`

**Interfaces:**
- Produces: `ProtocolClass`, `ProtocolIngressBudget`, `ProtocolIngressPermit`, pre-copy NFS/9P/WebUI admission, retransmit joining, and per-protocol bounded metrics.
- Consumes: A1 fair admission, A5 request identity/cache, A20 `ResidentMemoryBudget`, protocol maximum frame sizes, the A16 connection context, and the sole lifecycle owner.

- [ ] **Step 1: Write the focused protocol-ingress RED tests**

Name tests:

- `ninep_frame_acquires_bytes_before_owned_copy`
- `webui_message_acquires_bytes_before_owned_copy`
- `nfs_decode_acquires_bytes_before_owned_write_body`
- `nfs_retransmit_joins_before_second_payload_charge`
- `nfs_same_xid_different_payload_is_fingerprint_mismatch`
- `nfs_retransmit_storm_stays_below_global_bytes_and_operations`
- `one_connection_cannot_monopolize_protocol_ingress`
- `ingress_cancel_disconnect_timeout_and_shutdown_release_once`
- `ingress_pressure_backpressures_without_spawning_unbounded_tasks`

The NFS dependency test feeds repeated identical XIDs over a latency-gated real RPC
decoder and asserts peak decoded payload bytes never exceed one joined request plus
the configured protocol bound. ZeroFS integration tests use real 9P frames, the real
NFS service callback, and the production WebSocket/gRPC-Web handler; direct internal
mutation calls do not count as acceptance.

List every exact test before implementation and require the current allocation path to
fail the boundary assertions:

```bash
cd /Volumes/bigssd/projects/ZeroFS/.worktrees/unified-tiered-writeback/zerofs
INGRESS_TEST_PREFIX='fs::mutation::ingress::tests::'
cargo test -p zerofs --locked -- --list 2>&1 | tee "${TMPDIR:-/tmp}/zerofs-ingress-red-list.log"
for name in \
  ninep_frame_acquires_bytes_before_owned_copy \
  webui_message_acquires_bytes_before_owned_copy \
  nfs_decode_acquires_bytes_before_owned_write_body \
  nfs_retransmit_storm_stays_below_global_bytes_and_operations \
  nfs_same_xid_different_payload_is_fingerprint_mismatch
do
  grep -F "${INGRESS_TEST_PREFIX}${name}: test" "${TMPDIR:-/tmp}/zerofs-ingress-red-list.log"
  if cargo test -p zerofs --locked "${INGRESS_TEST_PREFIX}${name}" -- --exact --nocapture; then
    echo "expected protocol-ingress RED but ${name} passed" >&2
    exit 1
  fi
done
```

- [ ] **Step 2: Add one shared pre-copy ingress owner**

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum ProtocolClass {
    Nfs,
    NineP,
    WebUiRpc,
    Direct,
    Nbd,
}

pub(crate) struct ProtocolIngressPermit {
    resident: crate::resident_memory::ResidentMemoryPermit,
    operation: crate::coordination::AdmissionPermit,
    class: ProtocolClass,
}

pub(crate) struct ProtocolIngressBudget {
    resident: std::sync::Arc<crate::resident_memory::ResidentMemoryBudget>,
    operations: crate::coordination::Admission,
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum ProtocolIngressError {
    #[error("protocol ingress is closed")]
    Closed,
    #[error("protocol request exceeds the configured frame bound")]
    TooLarge,
    #[error("resident-memory admission failed: {0}")]
    Resident(#[from] crate::resident_memory::ResidentMemoryError),
}

impl ProtocolIngressBudget {
    pub(crate) async fn acquire_before_copy(
        &self,
        class: ProtocolClass,
        decoded_len: usize,
    ) -> Result<ProtocolIngressPermit, ProtocolIngressError>;
}
```

The 9P codec reads only the fixed header first, validates `msize`, acquires the exact
remaining-frame permit, and only then allocates/reads the body. The WebSocket upgrade
precharges one maximum message per admitted reader session and configures hard frame,
message, and concurrent-session limits before axum/tungstenite may produce an owned
`WsMessage::Binary`; after receive it refunds the unused maximum and transfers the
exact charge. An unbounded callback or response queue is rejected. Every adapter moves
the same permit through request-cache lookup, preparation, and acceptance.

A retry always owns a bounded ingress permit while its complete fingerprint is
streamed/decoded. Only after kind, inode/range, credentials, stability flags, length,
and payload hash match may it join and release the retry's ingress permit before raw
mutation admission. Same connection+XID with different payload remains a fingerprint
mismatch; XID alone never skips body validation or receives an uncharged allocation.

- [ ] **Step 3: Extend and pin the additive nfsserve API**

In the separately inventoried A16 fork worktree, add a server-wide admission hook
whose permit is acquired from RPC record/header length before decoding the opaque WRITE body.
The hook returns an owned permit stored in `RequestContext`; connection incarnation,
XID, peer, credentials, and requested stability remain available. If the wire decoder
cannot know an exact length early, charge the protocol maximum before allocation and
refund the difference after decode. Retries stay bounded while their full fingerprint
is computed, then join before a second raw-mutation/cache charge. Run the complete fork tests, push the immutable revision, then
pin that exact revision in ZeroFS. No branch or local-path dependency is accepted.

```bash
cd /Volumes/bigssd/projects/nfsserve/.worktrees/zerofs-write-context
cargo test --all-targets --locked -- --list 2>&1 | tee "${TMPDIR:-/tmp}/nfsserve-ingress-tests.list"
grep -F 'nfs_same_xid_different_payload_is_fingerprint_mismatch: test' "${TMPDIR:-/tmp}/nfsserve-ingress-tests.list"
cargo test --all-targets --locked
cargo fmt --all -- --check
git diff --check
```

- [ ] **Step 4: Run GREEN and the bounded retransmit gate**

```bash
cd /Volumes/bigssd/projects/ZeroFS/.worktrees/unified-tiered-writeback/zerofs
cargo_test_nonzero 'fs::mutation::ingress::tests' -p zerofs --locked
cargo_test_nonzero 'ninep::server::tests' -p zerofs --locked
cargo_test_nonzero 'nfs::tests' -p zerofs --locked
cargo_test_nonzero 'webui::tests' -p zerofs --locked
cargo_test_nonzero 'rpc::server::tests' -p zerofs --locked
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo test --workspace --all-targets --locked
cargo check -p ninep-client --target wasm32-unknown-unknown --locked
git diff --check
git add zerofs/src/fs/mutation/ingress.rs zerofs/src/fs/mutation/mod.rs zerofs/src/fs/mutation/request_cache.rs zerofs/src/ninep/server.rs zerofs/src/ninep/handler.rs zerofs/src/nfs.rs zerofs/src/webui.rs zerofs/src/rpc/server.rs zerofs/src/cli/server.rs zerofs/src/prometheus.rs zerofs/Cargo.toml zerofs/Cargo.lock
git commit -m "fix(protocol): bound request bodies before dispatch"
```

Record the nfsserve upstream base, exact pushed revision, fork test receipt, and Root
landing ownership in the ZeroFS commit body. Expected: all protocol request bodies are
bounded before owned copies; hard-mount retransmits cannot amplify resident memory.

---

### Task A22: Select SSH/SFTP Transport Only From Direction-Specific Evidence

**Files:**
- Create: `zerofs/src/sftp_transport/ssh_program.rs`
- Modify: `zerofs/src/sftp_transport.rs`
- Modify: `zerofs/src/config.rs`
- Modify: `zerofs/src/parse_object_store.rs`
- Modify: `zerofs/src/prometheus.rs`
- Modify: `README.md`

**Interfaces:**
- Produces: validated optional `[sftp].ssh_program`, `SshProgramIdentity`, exact child-executable selection, status/metrics identity, and unchanged stock default.
- Consumes: the existing owned foreground SSH child, strict host-key/key-only policy, SFTP physical-session pool, and the immutable real A/B contract executed in C7/C8.

- [ ] **Step 1: Write executable-selection RED tests**

Name tests `omitted_ssh_program_preserves_stock_lookup`,
`configured_ssh_program_requires_absolute_executable_regular_file`,
`ssh_program_rejects_arguments_and_non_regular_targets`,
`ssh_program_canonicalizes_an_executable_symlink`,
`selected_program_keeps_strict_authentication_arguments`,
`selected_program_identity_records_path_version_and_sha256`, and
`every_physical_session_uses_the_selected_program`. Use temporary executable scripts
that record argv and implement only the version probe; they prove process selection,
not transport throughput.

```bash
cd /Volumes/bigssd/projects/ZeroFS/.worktrees/unified-tiered-writeback/zerofs
SSH_PROGRAM_TEST_PREFIX='sftp_transport::ssh_program::tests::'
cargo test -p zerofs --locked -- --list 2>&1 | tee "${TMPDIR:-/tmp}/zerofs-ssh-program-red-list.log"
for name in \
  omitted_ssh_program_preserves_stock_lookup \
  configured_ssh_program_requires_absolute_executable_regular_file \
  selected_program_identity_records_path_version_and_sha256 \
  every_physical_session_uses_the_selected_program
do
  grep -F "${SSH_PROGRAM_TEST_PREFIX}${name}: test" "${TMPDIR:-/tmp}/zerofs-ssh-program-red-list.log"
  if cargo test -p zerofs --locked "${SSH_PROGRAM_TEST_PREFIX}${name}" -- --exact --nocapture; then
    echo "expected ssh-program RED but ${name} passed" >&2
    exit 1
  fi
done
```

- [ ] **Step 2: Implement the narrow production selector**

```rust
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SshProgramIdentity {
    pub(crate) canonical_path: std::path::PathBuf,
    pub(crate) version: String,
    pub(crate) sha256: [u8; 32],
}

pub(crate) struct SshProgram {
    executable: std::path::PathBuf,
    identity: SshProgramIdentity,
}

impl SshProgram {
    pub(crate) async fn resolve(
        configured: Option<&std::path::Path>,
    ) -> Result<Self, TransportError>;

    pub(crate) fn command(&self) -> tokio::process::Command;
    pub(crate) fn identity(&self) -> &SshProgramIdentity;
}
```

Omission retains the existing `ssh` lookup. An explicit value must be an absolute,
canonical, executable regular file and contains no argument string. Build each owned
physical session from `SshProgram::command`; preserve `-F`, strict host checking,
known-hosts, identities-only, key, port, user, no ControlMaster, `-T -s -- host sftp`,
and positive child reaping. Record only safe identity fields. Never mutate global
`PATH`, `update-alternatives`, `/usr/bin/ssh`, or host SSH configuration.

- [ ] **Step 3: Run portable GREEN and commit the selector fence**

```bash
cd /Volumes/bigssd/projects/ZeroFS/.worktrees/unified-tiered-writeback/zerofs
cargo_test_nonzero 'sftp_transport::ssh_program::tests' -p zerofs --locked
cargo_test_nonzero 'sftp_transport::tests' -p zerofs --locked
cargo_test_nonzero 'config::tests' -p zerofs --locked
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo test --workspace --all-targets --locked
cargo check -p ninep-client --target wasm32-unknown-unknown --locked
cd /Volumes/bigssd/projects/ZeroFS/.worktrees/unified-tiered-writeback
git diff --check
git add zerofs/src/sftp_transport/ssh_program.rs zerofs/src/sftp_transport.rs zerofs/src/config.rs zerofs/src/parse_object_store.rs zerofs/src/prometheus.rs README.md
git commit -m "feat(sftp): select a pinned ssh executable explicitly"
```

The production selector remains dormant until C7's real Linux A/B proves a pinned HPN
binary wins. HPN receive-window improvement applies to downloads where ZeroFS is the
receiver; uploads require measured request pipelining and all configured physical
write sessions carrying bytes. No claim crosses directions without evidence, and C7B
must land the winning shipping result before the final Plan A gate. Before that
decision, Plan C runs the pinned upstream regression inventory under its ledger
supervisor with fixed TERM/KILL deadlines, excludes only the pinned
`dynamic-forward` test that backgrounds a multiplexed forwarding client, proves the
remaining inventory is nonzero, and separately runs `transfer`, `rekey`, `sftp`,
`sftp-batch`, `sftp-resume`, and `forwarding`. Any missing test/process-reap receipt
rejects the package candidate.
