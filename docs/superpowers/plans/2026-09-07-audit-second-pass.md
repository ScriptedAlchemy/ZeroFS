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
- Worktree isolation is for ownership, not bypassing build locks. Each worktree uses its own default target and explicit `hauler exec -- cargo ...` with `--locked`, `-j2`, and `--features webui` for Linux library gates. The Ubuntu login PATH does not reliably select the Cargo shim.
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

- [x] Run a real NBD handler regression: one operation slot, ample bytes/storage, a write at offset 4092 crossing two 4096-byte stripes; require bounded completion, exact readback and the FUA durability callback.
- [x] Run pre-publication rollback and cancellation-lifecycle regressions; neither batch member becomes visible on failed publication, and raw/request/overlay ownership returns to zero.
- [x] Replace the per-member blocking reserve loop with one checked aggregate reservation. `BudgetPermit` releases only member bytes; a shared `BudgetOperationPermit` releases the operation slot exactly once after the last member retires.
- [x] Evaluate acceptance predicates outside the budget mutex. Terminal/frozen transitions wake blocked reservations; single-inode behavior is preserved.
- [x] Map request-cache `WriteAdmissionError::Backpressured` to existing `CommandError::IoError`, not `NoSpace`, with the minimal NBD-local change.
- [x] Hold one request-cache slot, assert NBD_EIO conversion, release it, and execute a subsequent write/readback without capacity changes or reconnect.
- [x] Run focused NBD, overlay, cancellation, and striped ordering suites; reviewed and pushed as `21354714` (worker `06739bf`). Brokered final mutation suite: 94 passed (`cc-9858`); NBD handlers: 25 passed (`cc-9860`). Logs on Ubuntu: `/tmp/zerofs-audit-nbd-fs-mutation-green.log` and `/tmp/zerofs-audit-nbd-handler-green.log`. This is handler/library proof, not the separate mounted-XFS acceptance gate.

## Draft 08: incomplete multipart ownership

Consumes retained `RamMultipartPartReservation`, multipart part state and existing owned abort/cleanup.
Produces controlled pre-commit rejection on unavailable incremental growth, with exact refund/abort accounting; ordinary drainable writes retain their existing waiting policy.

- [x] Execute the A4/B4/A2/B2 schedule with shared capacity 8; both complete objects fit individually. Real RAM and SSD tests each failed at the original wait cycle; declared-length admission failed with zero bytes reserved instead of six. The first implementation's store suite passed 81 tests (two ignored, `cc-9891`), but cancellation/FIFO review followups remain before final acceptance.
- [ ] Add nonblocking multipart growth admission rather than blocking behind reservations held by incomplete objects. Reject overload before commit, then use owned abort and a fresh logical upload attempt.
- [ ] For trusted generated segments with a declared payload length, reserve the complete logical payload before acknowledging multipart support. Validate exact completion length. Unknown-length multipart parts use try-only admission without bypassing queued ordinary writes.
- [ ] Verify failure does not strand active-part counters, retained payloads, FIFO waiters or reservations. Exercise cancellation and retry, including empty multipart completion.
- [ ] Review the analogous SSD staging path for the same partial-ownership cycle. Do not move the deadlock from RAM to disk or silently weaken physical-space accounting.
- [ ] Run multipart and ordinary writeback admission suites; review and commit.

## Draft 07: conditional large-object replay

Consumes persisted `MutationRecord` mode and the existing `ConditionalMultipartCreate` handshake. Generated-segment provenance remains separate from the backend's atomic-create capability.
Produces the same Create/Update guarantee above and below the 36 MiB transfer threshold.

- [ ] Pause large Create replay before publication, insert competing different bytes, resume, and assert those bytes survive and the losing record does not advance the remote frontier.
- [ ] Cover small Create, changed Update ETag, and identical-content lost-reply reconciliation.
- [ ] Choose conditional publication before initiating multipart. Request and require a genuine atomic Create capability for persisted Create records; no redundant generated-segment tag or provenance reconstruction is needed during replay.
- [ ] Preserve large generic Create records such as compacted SSTs using SFTP's genuine atomic create capability. Keep the private capability request separate from generated-segment provenance; do not label ordinary SSTs as generated segments. Backends without atomic completion must fail closed and terminally, rather than retrying an unsupported contract forever.
- [ ] Do not treat `ImmutableCreate` alone as segment provenance: it also covers WAL and compacted SST objects. Never disguise ordinary records as generated segments; reject unsupported conditional completions before sending parts.
- [ ] Keep bounded streaming; no unbounded collection, extra HEAD-as-lock, or unconditional complete for a conditional mutation.
- [ ] Run real recording/fault backend regressions and SFTP multipart capability tests; review and commit.

## Draft 12: prospective and anchored durable paths

Consumes configured absolute dirty/cache paths and existing Unix descriptor traversal patterns from `cli/transfer/local_destination.rs`.
Produces nonmutating prospective physical separation and anchored creation/opening, without a pathname-only check/create/check claim.

- [x] Execute real Rust temporary-filesystem tests for `alias -> clean` plus missing `alias/new-dirty`, nested missing suffixes, dangling links, ENOTDIR, relative/parent components, legitimate first start, and `cache2` siblings.
- [x] Canonicalize the nearest existing ancestor, then append validated missing components. Normalization must create no files or directories.
- [x] Reproduce the Phase 1 behavioral failure against the tests-only commit, then restore the implementation and run GREEN. Implementation preceded observed RED: tests-only `6885110` ran seven tests, five passed and two failed; fixed `f3bdb5f` ran nine, all passed. Logs: `/var/tmp/zerofs-audit-path-{red,green}.log` on Ubuntu. Integrated as `00330430`; concurrent replacement protection below is still pending.
- [x] Review a narrowly owned directory-handle API before bootstrap/journal edits. Preserve ancestry through base/namespace creation, lock/database opening, recovery, publication and cleanup; reuse `rustix` and redb's file-backed construction. Integrated as `47bafd04` (worker `9d7b45c2`), with portability followup `68f3a916`. Final Linux writeback prefix: 319 passed, eight ignored (`cc-9924`); Mac helper: seven passed (`cc-14`); Mac Journal: 74 passed, two ignored (`cc-16`). Multipart staging caller integration remains pending.
- [x] Resolve the read-only redb opening boundary without weakening the first audit's mutation-free identity preflight. Exact-opened-file descriptor probes passed on Linux and macOS; no vendored patch, writable inspection fallback, or platform ban. Parent aliases are normalized before strict final-root traversal; nonregular files cannot block before type validation.
- [ ] Race parent replacement and final-entry substitution; require anchored behavior or fail-closed nonmutation. Run normalization/bootstrap/journal identity tests; review and commit.

## Draft 10: bounded idle execution lifecycle

Consumes overlay visibility/attributes/read retirement and materializer FIFO/multi-inode execution.
Produces generation-safe idle retirement and reaped task bookkeeping, bounded by current work rather than historical inode count.

- [x] Add a 10,000-distinct-inode churn regression at concurrency one. Actual baseline execution retained 10,000 overlay runtimes, workers, pending keys and materializer lanes/handles after drain. The first missing-WebUI compile and a separate hot-runtime fixture timeout are not behavioral failure receipts. The bounded stage passed the actual churn gate; the subsequent single-executor refactor has a separate gate below.
- [x] Race hot-inode enqueue against retirement. Enqueue and removal agree on one generation; both materializer and overlay hot-inode races passed.
- [x] Retire only when queued, executing, staged, held and read-retirement work is absent. Prune empty bookkeeping and reap completed join handles. Integrated bounded-lifecycle stage as `f2300b5f` (worker `a240b72a`). Real 10,000-inode plus race suite: three passed (`cc-9900`); the post-drain census requires exact zero.
- [x] Preserve striped ordering, cancellation, frozen-overlay and read-visibility tests after rebasing on the aggregate reservation fix. Full mutation prefix: 97 passed, zero ignored (`cc-9909`); root independently read the broker receipt.
- [ ] Only after the bounded lifecycle gate, remove the redundant forwarding execution layer by explicitly retaining visibility/attribute/read ownership in the overlay and FIFO execution in the materializer. Do not delay the P1 commits for this refactor.
- [ ] Run churn/race and existing mutation suites, review and commit each independently verifiable stage.

## Draft 13: real isolated XFS runtime gate

Consumes existing UUID-scoped ledger/resources, `MetricsAuthorityIdentity` and `WritebackSnapshot` parsers, restart and cleanup infrastructure.
Produces an executed receipt tied to the actual process incarnation and persistent filesystem/export, including write/barrier/restart/readback and completed cleanup.

- [x] Use one checked-in config template for the CI fixture and production parser regression; supply positive `min_free_gb` and valid full memory settings. Removing the reserve fails production validation. Rust configuration batch: 115 passed (`cc-9890`).
- [x] Keep cheap plan/schema validation separate from runtime acceptance. A planned receipt or missing collector cannot satisfy the runtime gate. Implemented in `fbd9d670` (worker `4c67f5dd`); real mounted acceptance remains pending below.
- [x] Extend existing benchmark authority explicitly for one isolated loopback NBD endpoint and the exact provisioned export. Reuse authority-only TLS with an explicit per-run trusted CA; never emit fabricated plaintext authority. The owned bootstrap/runtime units use the harness UID and an exact materialized configuration, not ambient systemd environment interpolation.
- [x] Add bounded loopback HTTPS sampling reusing existing parsers. Reject missing/malformed identities, invalid or regressing frontiers, terminal state and mismatched filesystem/export. Linux Python gate: 136 passed. Independent Mac execution exposed one unnormalized temporary-path expectation; test-only followup `c84686bb` corrected it. Root rerun on Mac: 136 passed in 4.431 seconds.
- [ ] Capture accepted progress after actual write/barrier, wait for that exact local durability frontier, unmount and detach NBD, then sample and cover the final accepted cutoff before SIGKILL. Retain the same journal, require a new server instance and unchanged filesystem/export, then compare a stored pre-restart checksum after remount.
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
