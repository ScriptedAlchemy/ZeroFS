# ZeroFS Second-Pass Audit Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development or superpowers:executing-plans. Run behavioral regressions, review each coherent change, and retain exact verification receipts.

**Goal:** Resolve Drafts 07–13 against current develop without weakening the six already repaired boundaries or deploying incomplete fixes.

**Architecture:** Admit a logical write atomically; partial objects must not wait while retaining undrainable reservations. Select publication semantics from the persisted contract before choosing a byte-transfer strategy. Bound execution infrastructure by active work, anchor durable paths outside disposable cache, and require observed runtime evidence for the XFS acceptance job.

**Tech Stack:** Existing Rust/Tokio/object_store/rustix/redb, Python tiered harness, GitHub Actions, Linux NBD/XFS.

**Spec:** The user's seven second-pass reports (Drafts 07–13), supplied in this thread, pinned to dabab48d. Implementation baseline is 43820a0a, which includes the first six audited fixes and the test-only journal-opener cleanup.

## Global Constraints

- Production CT198 remains unchanged. No deployment until the additional P1 repairs and relevant integration gates pass; no journal resets or recovery shortcuts.
- Sol/Terra workers implement; primary agent reads, reviews, plans and integrates.
- Preserve the first six fixes, ordinary POSIX and 9P semantics, NBD raw-byte ownership, configured resource budgets, and all original audiobook data.
- No new framework, broad dependency upgrade, minimum-limit workaround, ambiguous-write replay, or fabricated durability result.
- Worktree isolation is for ownership, not bypassing build locks. Each worktree uses its own default target and plain Cargo/Hauler with `--locked`, `-j2`, and `--features webui` for ZeroFS library gates.
- At most two expensive Linux build lanes: `/tmp/zerofs-audit-test.lock` and `/tmp/zerofs-audit-test-secondary.lock`. Never share root-package binaries/fingerprints between worktrees or count zero-test filters as passing.
- All artifacts remain under BigSSD projects or Linux `/fast`; no iPhone mirroring, app uninstall, keychain changes, unrelated guest changes, or production-mounted NBD tests.
- The prior release build was deliberately cancelled through Hauler after this scope addition. It is not a failed regression or a deployable receipt.

## Ownership and dependencies

| Lane | Drafts | Files | Dependency |
|---|---|---|---|
| Logical NBD admission | 09, 11 | fs/mutation/volatile_overlay.rs budget/permit section; overlay.rs reservation/handoff; nbd/handler.rs | First P1 lane; publish stable reservation API before runtime-lifecycle edits |
| Multipart contracts | 07, 08 | writeback/store.rs, reservation/admission helpers, remote.rs, remote_streaming_tests.rs | One owner for retained ownership and replay publication |
| Path separation | 12 | writeback/config.rs, bootstrap.rs, narrowly journal.rs | Prospective resolution first; anchored opening is a separate reviewed gate |
| Idle lifecycle | 10 | fs/mutation/materializer.rs, volatile_overlay.rs runtime section, overlay.rs maps/dispatch | Rebase on logical NBD admission before production edits to shared files |
| XFS runtime acceptance | 13 | xfs-nbd.yml and tiered_writeback_e2e harness/tests | Code can proceed independently; mounted acceptance waits for reviewed disposable resources and fixed core |

## Drafts 09 and 11: logical NBD admission

Consumes `PreparationGuard`, already-owned raw body bytes, `PrepareWriteRequest.members`, and `VolatileBudget`.
Produces one aggregate byte reservation and one shared logical operation owner, split into per-member byte permits; no second raw admission.

- [ ] Run a real NBD regression: one operation slot, ample bytes/storage, a write at offset 4092 crossing two 4096-byte stripes; require bounded completion, exact readback and FUA.
- [ ] Run cancellation before visibility publication; neither member may become visible, and raw/request/overlay ownership must return to zero.
- [ ] Replace the per-member blocking reserve loop with one checked aggregate reservation. `BudgetPermit` releases only member bytes; a shared `BudgetOperationPermit` releases the operation slot exactly once after the last member retires.
- [ ] Evaluate acceptance predicates outside the budget mutex. Terminal/frozen transitions must wake blocked reservations; preserve single-inode behavior.
- [ ] Map request-cache `WriteAdmissionError::Backpressured` to existing `CommandError::IoError`, not `NoSpace`. Consolidate only the small NBD-local mapping.
- [ ] Hold one request-cache slot, assert the next wire reply is NBD_EIO, release it, and require a subsequent write to succeed without capacity changes or reconnect.
- [ ] Run focused NBD, overlay, cancellation, and striped ordering suites; review and commit.

## Draft 08: incomplete multipart ownership

Consumes retained `RamMultipartPartReservation`, multipart part state and existing owned abort/cleanup.
Produces controlled pre-commit rejection on unavailable incremental growth, with exact refund/abort accounting; ordinary drainable writes retain their existing waiting policy.

- [ ] Execute the A4/B4/A2/B2 schedule with shared capacity 8; both complete objects fit individually. A bounded test must observe the current wait cycle before changing admission.
- [ ] Add nonblocking multipart growth admission rather than blocking behind reservations held by incomplete objects. Reject overload before commit, then use owned abort and a fresh logical upload attempt.
- [ ] Verify failure does not strand active-part counters, retained payloads, FIFO waiters or reservations. Exercise cancellation and retry, including empty multipart completion.
- [ ] Review the analogous SSD staging path for the same partial-ownership cycle. Do not move the deadlock from RAM to disk or silently weaken physical-space accounting.
- [ ] Run multipart and ordinary writeback admission suites; review and commit.

## Draft 07: conditional large-object replay

Consumes persisted `MutationRecord` mode, kind, database prefix and validated canonical object identity; existing `GeneratedSegmentCreate` plus `ConditionalMultipartCreate` handshake.
Produces the same Create/Update guarantee above and below the 36 MiB transfer threshold.

- [ ] Pause large Create replay before publication, insert competing different bytes, resume, and assert those bytes survive and the losing record does not advance the remote frontier.
- [ ] Cover small Create, changed Update ETag, and identical-content lost-reply reconciliation.
- [ ] Choose conditional publication before initiating multipart. For validated generated-segment PUT/Create records, restore the trusted marker and require backend capability acknowledgment.
- [ ] Do not treat `ImmutableCreate` alone as segment provenance: it also covers WAL and compacted SST objects. Reject arbitrary keys, Copy/Rename records, and unsupported conditional completions rather than disguising them as generated segments.
- [ ] Keep bounded streaming; no unbounded collection, extra HEAD-as-lock, or unconditional complete for a conditional mutation.
- [ ] Run real recording/fault backend regressions and SFTP multipart capability tests; review and commit.

## Draft 12: prospective and anchored durable paths

Consumes configured absolute dirty/cache paths and existing Unix descriptor traversal patterns from `cli/transfer/local_destination.rs`.
Produces nonmutating prospective physical separation and anchored creation/opening, without a pathname-only check/create/check claim.

- [ ] Execute real Rust temporary-filesystem tests for `alias -> clean` plus missing `alias/new-dirty`, nested missing suffixes, dangling links, ENOTDIR, relative/parent components, legitimate first start, and `cache2` siblings.
- [ ] Canonicalize the nearest existing ancestor, then append validated missing components. Normalization must create no files or directories.
- [ ] Reproduce the Phase 1 behavioral failure against the tests-only commit, then restore the implementation and run GREEN. Record honestly that implementation preceded this observed RED.
- [ ] Review a narrowly owned directory-handle API before bootstrap/journal edits. Preserve ancestry through base/namespace creation, lock/database opening and initial mutation; reuse `rustix` and redb's file-backed construction where supported.
- [ ] Resolve the read-only redb opening boundary without weakening the first audit's mutation-free identity preflight. No unreviewed vendored patch, platform ban, or silent path-based fallback.
- [ ] Race parent replacement and final-entry substitution; require anchored behavior or fail-closed nonmutation. Run normalization/bootstrap/journal identity tests; review and commit.

## Draft 10: bounded idle execution lifecycle

Consumes overlay visibility/attributes/read retirement and materializer FIFO/multi-inode execution.
Produces generation-safe idle retirement and reaped task bookkeeping, bounded by current work rather than historical inode count.

- [ ] Add a 10,000-distinct-inode churn regression at concurrency one. After drain, assert runtime/lane/worker/map cardinalities directly; RSS alone is not acceptance.
- [ ] Race hot-inode enqueue against retirement. Enqueue and removal must agree on one generation; no two concurrent ordering owners may exist.
- [ ] Retire only when queued, executing, staged, held and read-retirement work is absent. Prune empty bookkeeping and reap completed join handles.
- [ ] Preserve striped ordering, cancellation, frozen-overlay and read-visibility tests after rebasing on the aggregate reservation fix.
- [ ] Only after the bounded lifecycle gate, remove the redundant forwarding execution layer by explicitly retaining visibility/attribute/read ownership in the overlay and FIFO execution in the materializer. Do not delay the P1 commits for this refactor.
- [ ] Run churn/race and existing mutation suites, review and commit each independently verifiable stage.

## Draft 13: real isolated XFS runtime gate

Consumes existing UUID-scoped ledger/resources, `MetricsAuthorityIdentity` and `WritebackSnapshot` parsers, restart and cleanup infrastructure.
Produces an executed receipt tied to the actual process incarnation and persistent filesystem/export, including write/barrier/restart/readback and completed cleanup.

- [ ] Use one checked-in config template for the CI fixture and production parser regression; supply positive `min_free_gb` and valid full memory settings. Removing the reserve must fail production validation.
- [ ] Keep cheap plan/schema validation separate from runtime acceptance. A planned receipt or missing collector cannot satisfy the runtime gate.
- [ ] Add a bounded loopback-only metrics transport reusing existing parsers. Reject missing/malformed identities, invalid or regressing frontiers, terminal state and mismatched filesystem/export.
- [ ] Capture accepted progress after actual write/barrier, wait for that exact local durability frontier, detach NBD before restart, retain the same journal, require a new server instance and unchanged filesystem/export, then compare a stored pre-restart checksum after remount.
- [ ] Preserve ledger-owned resource teardown. Failed commands, stale metrics, checksum mismatch or incomplete cleanup must produce a failed receipt.
- [ ] Run deterministic Python/config tests and targeted actionlint. Record the four existing non-tiered ShellCheck findings separately; leave other plan-only jobs untouched.
- [ ] Before any mounted run, independently verify disposable loopback service/ports, unattached NBD device, UUID paths and cleanup boundaries. Never point the harness at CT198 or production mounts.
- [ ] Execute the real XFS scenario and repeated cleanup/assert-clean, retain receipt, review and commit.

## Combined completion gates

- [ ] Integrate reviewed commits without absorbing unrelated dirty work.
- [ ] Run full WebUI-enabled library, client, formatting/clippy and applicable Python/workflow gates; record ignored and environment-dependent gates literally.
- [ ] Refresh dependent shipped artifacts only where affected, preserving app data and old CLI backup. iOS install remains blocked on keychain authorization and device availability.
- [ ] Rebuild the production release from final reviewed source; then use the existing drain/quiesce/rollback lifecycle, never the cancelled earlier candidate.
- [ ] Verify live upload/readback, remote writeback zero, exact settings/resources and the 1,090-file inventory before reporting deployment success.
