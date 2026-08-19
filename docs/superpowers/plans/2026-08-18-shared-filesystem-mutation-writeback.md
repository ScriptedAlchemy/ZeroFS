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

- [ ] **Step 3: Run Plan A gate and commit**

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
- Create: `zerofs/src/fs/store/extent/read/tests.rs`
- Create: `zerofs/src/fs/store/extent/read/metrics.rs`

**Interfaces:**
- Produces: an ordered read-run plan, bounded concurrent fetch of independent immutable on-store segment runs, and bounded-cardinality logical/read-run utilization metrics.
- Consumes: the existing extent-location range scan, decoded/open-buffer fast paths, `SegmentStore::read_run`, stale-location re-resolution, nomination/crossing accounting, and the existing `PARALLEL_EXTENT_OPS` bound.

- [ ] **Step 1: Split the existing tests without behavior change**

Move the inline `read.rs` test module to `read/tests.rs` before adding behavior. Keep production below 600 lines and source plus tests below 1000 lines per file. Run the complete existing module gate and require a nonzero pass count.

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

- [ ] **Step 4: Run GREEN, parity, and exact-fence commit**

```bash
cd /Volumes/bigssd/projects/ZeroFS/.worktrees/unified-tiered-writeback/zerofs
cargo_test_nonzero 'fs::store::extent::read::tests' -p zerofs --locked
cargo_test_nonzero 'fs::ops::io::tests' -p zerofs --locked
cargo_test_nonzero 'segment_store::tests' -p zerofs --locked
cargo clippy -p zerofs --lib --locked -- -D warnings
cargo fmt --all -- --check
git diff --check
git add zerofs/src/fs/store/extent/read.rs zerofs/src/fs/store/extent/read/tests.rs zerofs/src/fs/store/extent/read/metrics.rs
git commit -m "perf(read): pipeline fragmented segment runs"
```

Expected: the fragmented RED proves peak backend concurrency greater than one; every bounded/error/order control is green; contiguous reads remain one GET; no protocol-specific behavior or durability semantics changed.
