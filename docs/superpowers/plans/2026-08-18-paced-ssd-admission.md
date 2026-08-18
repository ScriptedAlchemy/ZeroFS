# Paced SSD Admission Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace normal 95-to-85-percent stop/resume cycles with exact SSD-reservation byte/operation credits while preserving hard physical reserve, acknowledgement semantics, recovery, and multipart ownership.

**Architecture:** The existing remote sequencing remains authoritative. Admission is first split into focused modules without behavior change. One sampler owns monotonic physical-space generations; move-only reservation tokens then travel from admission through durable journal ownership, ordered remote cleanup, pacing credits, and atomic multipart promotion.

**Tech Stack:** Rust 2024, existing shared coordination gate, redb journal, Tokio owned workers, `fs4::available_space`, and Prometheus.

**Spec:** `docs/superpowers/specs/2026-08-18-unified-tiered-writeback-design.md`

## Global Constraints

- Begin only after every Plan A gate is green.
- Preserve admission refresh, fit, charge/admit, blocked transition, grant, canceled grant/rollback, release, permit disarm, close, and poison behavior before adding pacing.
- Do not change filesystem/object acknowledgement, mutation ordering, overlay visibility, remote fence ordering, or journal compatibility.
- `SsdReleaseCredit` has one definition in `writeback/pacing.rs`; its byte field is exactly `ssd_reservation_bytes`.
- Credits arise only after contiguous remote watermark commit, overlay retirement, durable local cleanup, and a fresh physical-space sample. Out-of-order upload completion yields zero credit.
- Physical reserve includes admitted-but-not-yet-written claims.
- Multipart parts reserve before buffering/file creation and promote atomically without release/reacquire.
- Do not edit the approved spec in implementation commits.
- Every task uses named RED tests, exact GREEN gates, and exact file staging.

## Non-Vacuous Filtered Cargo Gates

Before executing any task in this plan, define this function in the same shell. Every filtered Cargo test command below uses it; raw filtered `cargo test` is not an acceptable substitute.

```bash
cargo_test_nonzero() {
  filter="$1"
  shift
  safe_filter="${filter//[^A-Za-z0-9]/_}"
  list_log="${TMPDIR:-/tmp}/zerofs-${safe_filter}-list.log"
  run_log="${TMPDIR:-/tmp}/zerofs-${safe_filter}-run.log"
  command cargo test "$@" -- --list | tee "$list_log"
  grep -F "$filter" "$list_log"
  command cargo test "$@" "$filter" -- --nocapture 2>&1 | tee "$run_log"
  grep -Eq 'test result: ok\. [1-9][0-9]* passed' "$run_log"
}
```

The list grep proves the filter exists; the result assertion proves it executed at least one test. Exact ignored tests additionally use `--exact` and assert exactly one pass.

---

### Task B1: Split Existing Admission Policy Without Behavior Change

**Files:**
- Delete: `zerofs/src/writeback/admission.rs`
- Create: `zerofs/src/writeback/admission/mod.rs`
- Create: `zerofs/src/writeback/admission/ram.rs`
- Create: `zerofs/src/writeback/admission/ssd.rs`
- Create: `zerofs/src/writeback/admission/tests.rs`
- Modify: `zerofs/src/writeback/mod.rs`

**Interfaces:**
- Produces: unchanged public/crate-private admission paths with RAM policy, SSD policy, and tests in separate owners.
- Consumes: Task A1 shared coordination queue and current policy/permit lifecycle.

- [ ] **Step 1: Move tests and record RED**

Move the full current admission test set to `admission/tests.rs` while imports point to the new module layout. Run:

```bash
cd /Volumes/bigssd/projects/ZeroFS/.worktrees/unified-tiered-writeback/zerofs
cargo_test_nonzero 'writeback::admission::tests' -p zerofs --locked
```

Expected RED: the new submodules do not yet exist.

- [ ] **Step 2: Move policy code without redesign**

Keep every existing refresh, fit, charge/admit, blocked transition, grant, canceled-grant rollback, release, disarm, close, poison, waiter ordering, and permit-drop behavior unchanged. `mod.rs` re-exports the existing names so downstream call sites do not churn.

- [ ] **Step 3: Prove equivalence and commit**

```bash
cd /Volumes/bigssd/projects/ZeroFS/.worktrees/unified-tiered-writeback/zerofs
cargo_test_nonzero 'writeback::admission::tests' -p zerofs --locked
cargo_test_nonzero 'writeback::journaler::tests' -p zerofs --locked
cargo_test_nonzero 'writeback::store::tests' -p zerofs --locked
cargo fmt --all -- --check
git diff --check
git add zerofs/src/writeback/admission.rs zerofs/src/writeback/admission/mod.rs zerofs/src/writeback/admission/ram.rs zerofs/src/writeback/admission/ssd.rs zerofs/src/writeback/admission/tests.rs zerofs/src/writeback/mod.rs
git commit -m "refactor(writeback): split admission policy owners"
```

---

### Task B2: Establish One Monotonic Physical-Space Sample Authority

**Files:**
- Create: `zerofs/src/writeback/space_sample.rs`
- Modify: `zerofs/src/writeback/mod.rs`
- Modify: `zerofs/src/writeback/bootstrap.rs`

**Interfaces:**
- Produces: `PhysicalSpaceSample`, `PhysicalSpaceSampler`, and monotonic `generation` ownership.
- Consumes: one canonical writeback directory and `fs4::available_space`.

- [ ] **Step 1: Write RED sampler tests**

Name tests `sample_generation_is_strictly_monotonic`, `concurrent_callers_share_one_generation_source`, `sample_uses_canonical_writeback_filesystem`, `failed_probe_does_not_publish_generation`, and `stale_sample_is_rejected`.

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct PhysicalSpaceSample {
    pub(crate) generation: u64,
    pub(crate) available_bytes: u64,
}

pub(crate) struct PhysicalSpaceSampler {
    writeback_dir: std::path::PathBuf,
    next_generation: std::sync::atomic::AtomicU64,
}

impl PhysicalSpaceSampler {
    pub(crate) async fn sample(&self) -> Result<PhysicalSpaceSample, SpaceSampleError>;
}
```

- [ ] **Step 2: Implement the sole generation owner**

Only `PhysicalSpaceSampler::sample` allocates generations. Admission, journal transition, remote cleanup, and refresher tasks consume samples from this owner; none increments or constructs a generation itself.

- [ ] **Step 3: Run GREEN and commit**

```bash
cd /Volumes/bigssd/projects/ZeroFS/.worktrees/unified-tiered-writeback/zerofs
cargo_test_nonzero 'writeback::space_sample::tests' -p zerofs --locked
cargo_test_nonzero 'writeback::bootstrap::tests' -p zerofs --locked
cargo fmt --all -- --check
git diff --check
git add zerofs/src/writeback/space_sample.rs zerofs/src/writeback/mod.rs zerofs/src/writeback/bootstrap.rs
git commit -m "refactor(writeback): centralize physical space sampling"
```

---

### Task B3: Define Exact SSD Reservations and Accounting

**Files:**
- Create: `zerofs/src/writeback/reservation.rs`
- Modify: `zerofs/src/writeback/admission/ssd.rs`
- Modify: `zerofs/src/writeback/model.rs`
- Modify: `zerofs/src/writeback/mod.rs`

**Interfaces:**
- Produces: `SsdReservationRequest`, `SsdReservationToken`, `ReservationState`, and `SsdAdmissionSnapshot`.
- Consumes: shared gate, Task B2 samples, byte/op/high/resume/min-free settings, and stable `MutationRecord::ssd_reservation_bytes`.

- [ ] **Step 1: Write RED exact-accounting tests**

Name tests for byte/op fit, physical reserve across concurrent claims, cancellation rollback, checked underflow/overflow poison, stale sample rejection, close/poison wakeup, and recovery seeding exact pending bytes/ops.

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct SsdReservationRequest {
    pub(crate) ssd_reservation_bytes: u64,
    pub(crate) physical_reservation_bytes: u64,
    pub(crate) operations: u64,
}

pub(crate) struct SsdReservationToken {
    admission: std::sync::Arc<SsdAdmission>,
    request: SsdReservationRequest,
    state: ReservationState,
}
```

`ssd_reservation_bytes` is the stable journal reservation, never logical payload length. `physical_reservation_bytes` is the conservative local allocation claim and remains separately visible.

- [ ] **Step 2: Implement checked, move-only ownership**

Fit proves hard byte/op caps and `available_bytes - outstanding_physical_claims - request.physical_reservation_bytes >= min_free_bytes`. Permit drop rolls back exactly once; disarm transfers ownership; invariant failure poisons admission.

- [ ] **Step 3: Run GREEN and commit**

```bash
cd /Volumes/bigssd/projects/ZeroFS/.worktrees/unified-tiered-writeback/zerofs
cargo_test_nonzero 'writeback::admission::tests' -p zerofs --locked
cargo_test_nonzero 'writeback::model::tests' -p zerofs --locked
cargo fmt --all -- --check
git diff --check
git add zerofs/src/writeback/reservation.rs zerofs/src/writeback/admission/ssd.rs zerofs/src/writeback/model.rs zerofs/src/writeback/mod.rs
git commit -m "refactor(writeback): own exact SSD reservations"
```

---

### Task B4: Transfer Reservation Ownership at Durable Journal Commit

**Files:**
- Modify: `zerofs/src/writeback/journal.rs`
- Modify: `zerofs/src/writeback/journaler.rs`
- Modify: `zerofs/src/writeback/bootstrap.rs`
- Modify: `zerofs/src/writeback/reservation.rs`

**Interfaces:**
- Produces: `SsdReservationToken::commit_local` and recovery-seeded journal ownership.
- Consumes: durable batch commit, exact mutation record reservation bytes/ops, and one fresh Task B2 sample.

- [ ] **Step 1: Write RED transition tests**

Name tests `local_watermark_waits_for_reservation_transition`, `batch_uses_one_fresh_sample`, `sample_failure_retains_ownership_and_poison`, `recovery_seeds_exact_bytes_and_operations`, and `token_cannot_commit_or_release_twice`.

```rust
pub(crate) fn commit_local(
    self,
    current_physical_bytes: u64,
    sample: PhysicalSpaceSample,
) -> Result<CommittedSsdReservation, ReservationError>;
```

- [ ] **Step 2: Implement the transition**

After the redb/container batch is durable, take one fresh sample, transition every token from admitted/unmaterialized to committed/future ownership, then publish the contiguous local watermark. Recovery derives exact bytes/ops and conservative physical claims from pending durable records before accepting writes.

- [ ] **Step 3: Run GREEN and commit**

```bash
cd /Volumes/bigssd/projects/ZeroFS/.worktrees/unified-tiered-writeback/zerofs
cargo_test_nonzero 'writeback::journaler::tests' -p zerofs --locked
cargo_test_nonzero 'writeback::bootstrap::tests' -p zerofs --locked
cargo_test_nonzero 'writeback::journal::tests' -p zerofs --locked
cargo fmt --all -- --check
git diff --check
git add zerofs/src/writeback/journal.rs zerofs/src/writeback/journaler.rs zerofs/src/writeback/bootstrap.rs zerofs/src/writeback/reservation.rs
git commit -m "refactor(writeback): transfer durable SSD ownership"
```

---

### Task B5: Pace Admission From Contiguous Durable Cleanup Credits

**Files:**
- Create: `zerofs/src/writeback/pacing.rs`
- Create: `zerofs/src/writeback/space_refresher.rs`
- Modify: `zerofs/src/writeback/admission/ssd.rs`
- Modify: `zerofs/src/writeback/remote.rs`
- Modify: `zerofs/src/writeback/store.rs`
- Modify: `zerofs/src/writeback/mod.rs`

**Interfaces:**
- Produces: `SsdAdmissionMode`, sole `SsdReleaseCredit` definition, and one owned physical-reserve refresher.
- Consumes: committed reservations, contiguous remote watermark transaction, overlay retirement, durable cleanup, Task B2 sampler, FIFO waiters, and shutdown owner.

- [ ] **Step 1: Write RED pacing/refresher tests**

Name tests `paced_gate_grants_exact_byte_and_op_credit`, `credit_accumulates_for_fifo_head`, `cancelled_grant_returns_credit`, `out_of_order_upload_completion_issues_no_credit`, `watermark_without_overlay_retirement_issues_no_credit`, `cleanup_without_durable_commit_issues_no_credit`, `fresh_sample_required_before_credit`, `resume_is_large_head_escape`, `external_cleanup_wakes_physical_waiter`, and `one_refresher_is_joined_on_shutdown`.

```rust
pub(crate) enum SsdAdmissionMode {
    Burst,
    Paced,
}

pub(crate) struct SsdReleaseCredit {
    pub(crate) ssd_reservation_bytes: u64,
    pub(crate) operations: u64,
}
```

This is the only `SsdReleaseCredit` definition. No logical/payload byte field exists.

- [ ] **Step 2: Implement ordered release**

Enter `Paced` when a request would cross the high watermark. A paced grant consumes both exact reservation-byte and operation credits. Remote completion yields no credit until the contiguous watermark transaction commits, the corresponding overlay intervals retire, local staging/journal cleanup is durably committed, and a new sampler generation confirms the physical reserve. At/below resume, return to `Burst` and clear stale credits so an oversized FIFO head can be reconsidered under hard gates.

- [ ] **Step 3: Implement the owned refresher**

When and only when min-free waiters exist, one `SpaceRefresher` samples every 250 ms and publishes increasing generations. Every success, failure, close, or poison path cancels and joins it.

- [ ] **Step 4: Run GREEN and commit**

```bash
cd /Volumes/bigssd/projects/ZeroFS/.worktrees/unified-tiered-writeback/zerofs
cargo_test_nonzero 'writeback::pacing::tests' -p zerofs --locked
cargo_test_nonzero 'writeback::space_refresher::tests' -p zerofs --locked
cargo_test_nonzero 'writeback::remote::tests' -p zerofs --locked
cargo_test_nonzero 'writeback::store::tests' -p zerofs --locked
cargo fmt --all -- --check
git diff --check
git add zerofs/src/writeback/pacing.rs zerofs/src/writeback/space_refresher.rs zerofs/src/writeback/admission/ssd.rs zerofs/src/writeback/remote.rs zerofs/src/writeback/store.rs zerofs/src/writeback/mod.rs
git commit -m "feat(writeback): pace admission from durable cleanup credit"
```

---

### Task B6: Reserve and Atomically Promote Multipart Ownership

**Files:**
- Create: `zerofs/src/writeback/multipart_reservation.rs`
- Modify: `zerofs/src/writeback/admission/ram.rs`
- Modify: `zerofs/src/writeback/admission/ssd.rs`
- Modify: `zerofs/src/writeback/store.rs`
- Modify: `zerofs/src/writeback/mod.rs`

**Interfaces:**
- Produces: `RamMultipartPartReservation`, `SsdMultipartPartReservation`, `MultipartReservationSet`, `MutationReservation`, `PromotionResult`, and `promote_multipart`.
- Consumes: RAM-part ownership for memory staging and SSD reservation/physical ownership for SSD/remote staging.

- [ ] **Step 1: Add the exact multipart RED tests**

Create these tests before implementation:

- `memory_part_is_reserved_before_buffer_copy`
- `parallel_memory_parts_share_global_cap`
- `ssd_part_is_reserved_before_staging_file_create`
- `ssd_parts_share_physical_free_space_reserve`
- `multipart_promotion_is_atomic_under_capacity_pressure`
- `multipart_abort_releases_all_part_reservations`
- `multipart_completion_holds_staging_and_journal_until_cleanup`
- `ram_promotion_succeeds_with_zero_spare_headroom`
- `ssd_promotion_succeeds_with_zero_spare_headroom`

```rust
pub(crate) struct RamMultipartPartReservation {
    pub(crate) final_ram_share: RamReservationToken,
}

pub(crate) enum MultipartReservationSet {
    Ram(Vec<RamMultipartPartReservation>),
    Ssd(Vec<SsdMultipartPartReservation>),
}

pub(crate) struct SsdMultipartPartReservation {
    pub(crate) staging: SsdStagingToken,
    pub(crate) final_journal_share: SsdJournalShareToken,
}

pub(crate) struct RamMutationReservation {
    pub(crate) final_ram: RamReservationToken,
}

pub(crate) struct SsdMutationReservation {
    pub(crate) final_journal: SsdReservationToken,
    pub(crate) staging_cleanup: Vec<SsdStagingToken>,
}

pub(crate) enum MutationReservation {
    Ram(RamMutationReservation),
    Ssd(SsdMutationReservation),
}

pub(crate) struct PromotionResult {
    pub(crate) mutation: MutationReservation,
}

pub(crate) fn promote_multipart(
    admissions: &AdmissionSet,
    parts: MultipartReservationSet,
) -> Result<PromotionResult, AdmissionError>;
```

`MultipartReservationSet` makes the tier transition explicit and prevents a RAM promotion from accepting an SSD-shaped request. RAM-part admission charges final RAM ownership once before each copy. Promotion holds the RAM admission lock, consumes/disarms every part token, and merges the already-charged ownership into one `final_ram` token without a new fit check, a second charge, or spare headroom. SSD-part admission atomically charges two explicit subclaims before staging-file creation: live staging allocation and that part's conservative final-journal share. SSD promotion holds the SSD admission lock, combines/disarms only the precharged final-journal shares into `final_journal`, and moves every still-live `SsdStagingToken` into `SsdMutationReservation::staging_cleanup` for durable cleanup. Thus final journal ownership and staging cleanup ownership coexist in the returned `MutationReservation::Ssd` without an unreserved byte and without needing capacity beyond what part admission already reserved. Abort/drop releases both subclaims exactly once. Promotion never releases then reacquires capacity.

- [ ] **Step 2: Prove the filter is non-vacuous and run the full module gate**

```bash
cd /Volumes/bigssd/projects/ZeroFS/.worktrees/unified-tiered-writeback/zerofs
cargo test -p zerofs --lib --locked -- --list | tee /tmp/zerofs-multipart-tests.list
grep -Fx 'writeback::multipart_reservation::tests::memory_part_is_reserved_before_buffer_copy: test' /tmp/zerofs-multipart-tests.list
grep -Fx 'writeback::multipart_reservation::tests::parallel_memory_parts_share_global_cap: test' /tmp/zerofs-multipart-tests.list
grep -Fx 'writeback::multipart_reservation::tests::ssd_part_is_reserved_before_staging_file_create: test' /tmp/zerofs-multipart-tests.list
grep -Fx 'writeback::multipart_reservation::tests::ssd_parts_share_physical_free_space_reserve: test' /tmp/zerofs-multipart-tests.list
grep -Fx 'writeback::multipart_reservation::tests::multipart_promotion_is_atomic_under_capacity_pressure: test' /tmp/zerofs-multipart-tests.list
grep -Fx 'writeback::multipart_reservation::tests::multipart_abort_releases_all_part_reservations: test' /tmp/zerofs-multipart-tests.list
grep -Fx 'writeback::multipart_reservation::tests::multipart_completion_holds_staging_and_journal_until_cleanup: test' /tmp/zerofs-multipart-tests.list
grep -Fx 'writeback::multipart_reservation::tests::ram_promotion_succeeds_with_zero_spare_headroom: test' /tmp/zerofs-multipart-tests.list
grep -Fx 'writeback::multipart_reservation::tests::ssd_promotion_succeeds_with_zero_spare_headroom: test' /tmp/zerofs-multipart-tests.list
grep -F 'writeback::store::tests::' /tmp/zerofs-multipart-tests.list
test "$(grep -Fc 'writeback::store::tests::' /tmp/zerofs-multipart-tests.list)" -gt 0
cargo test -p zerofs writeback::multipart_reservation::tests --locked -- --nocapture 2>&1 | tee /tmp/zerofs-multipart-tests.run
grep -Eq 'test result: ok\. [1-9][0-9]* passed' /tmp/zerofs-multipart-tests.run
cargo test -p zerofs writeback::store::tests --locked -- --nocapture 2>&1 | tee /tmp/zerofs-multipart-store.run
grep -Eq 'test result: ok\. [1-9][0-9]* passed' /tmp/zerofs-multipart-store.run
cargo fmt --all -- --check
git diff --check
```

- [ ] **Step 3: Commit the exact fence**

```bash
git add zerofs/src/writeback/multipart_reservation.rs zerofs/src/writeback/admission/ram.rs zerofs/src/writeback/admission/ssd.rs zerofs/src/writeback/store.rs zerofs/src/writeback/mod.rs
git commit -m "fix(writeback): bound and promote multipart reservations"
```

---

### Task B7: Expose Pacing Metrics and Documentation

**Files:**
- Modify: `zerofs/src/writeback/model.rs`
- Modify: `zerofs/src/prometheus.rs`
- Modify: `README.md`

**Interfaces:**
- Produces: bounded-cardinality pacing, waiter, credit, physical-claim, and wait-reason status/metrics.
- Consumes: `SsdAdmissionSnapshot`, `PhysicalSpaceSample`, and existing `WritebackStatus`.

- [ ] **Step 1: Write RED metrics tests**

Assert gauges/counters for mode, waiter count/bytes/ops, exact credit bytes/ops, admitted and future physical claims, available/min-free space, burst/paced grants, sampler generation, and bounded wait reasons. Reject paths, request kinds, object keys, and error strings as labels.

- [ ] **Step 2: Document stable semantics**

Document high-water entry to pacing, resume as large-head/emergency escape, physical reserve as a hard gate, exact `ssd_reservation_bytes` crediting, and that occupancy never changes the configured acknowledgement or durability target. Do not edit the approved spec.

- [ ] **Step 3: Run Plan B final gate and commit**

```bash
cd /Volumes/bigssd/projects/ZeroFS/.worktrees/unified-tiered-writeback/zerofs
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo_test_nonzero 'writeback::' -p zerofs --locked
cargo test --workspace --all-targets --locked
cd /Volumes/bigssd/projects/ZeroFS/.worktrees/unified-tiered-writeback
git diff --check origin/develop...HEAD
git add zerofs/src/writeback/model.rs zerofs/src/prometheus.rs README.md
git commit -m "docs(writeback): expose paced SSD admission state"
```
