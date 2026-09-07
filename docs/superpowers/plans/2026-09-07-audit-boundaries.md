# ZeroFS Audit Boundary Fixes Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development or superpowers:executing-plans. Track each test-first implementation and review receipt.

**Goal:** Fix all six user-supplied audit findings without weakening durability, replay, or POSIX contracts.

**Architecture:** Keep filesystem publication and conditional unlink atomic under existing mutation fences and ordered inode locks. Preserve generic replay ambiguity, reuse journal/backend identity normalization, bound HTTP ingress separately from write concurrency, and separate credential refresh from session failure recovery.

**Tech Stack:** Rust, Tokio, SlateDB, Foyer, axum, russh, existing 9P client/server.

**Spec:** The six audit reports pasted by the user on 2026-09-07; their reproductions and required behavior are restated per task below.

## Global Constraints

- Base: 460a48e4, including the pending cache-pressure fixes. Production CT198 is unchanged.
- Sol/Terra workers implement; primary agent reads, reviews, integrates and verifies.
- No production restart, destructive recovery command, journal mutation, cache deletion, credential change, or GitHub-settings change during implementation.
- Preserve ordinary POSIX rename/remove semantics, exact recovery counters/hashes/maintenance checks, and accepted mutation ownership across request cancellation.
- No new framework or dependency unless existing mechanisms demonstrably cannot satisfy the contract.
- All Mac artifacts stay under /Volumes/bigssd/projects. Linux worktrees stay under /fast/projects/ZeroFS-worktrees.
- Run failing regression before implementation, then targeted green tests. No source-string tests. Capture command, exact result, and commit in each report.
- Serialize expensive Linux builds using flock on /tmp/zerofs-audit-test.lock; use plain cargo, -j 2, and the existing warm target /fast/projects/ZeroFS-worktrees/http-upload-storage-pressure/zerofs/target. Do not kill unrelated build jobs or bypass toolchain wrappers.

## Workstream boundaries

| Tasks | Shared surface | Resolution |
|---|---|---|
| 2, 3, 4 | webui.rs, filesystem operations | One worker owns all three; no concurrent edits to this surface. |
| 5 and server startup | cli/init.rs identity construction | Recovery worker may extract only shared identity construction/validation; preserve startup semantics. |
| 1, 6 | independent client and transport code | Independent worktrees; no cross-worker interfaces. |

### Task 1: Preserve ambiguous standalone retries

**Files:** zerofs/ninep-client/src/lib.rs and focused client/server replay tests.
**Consumes:** existing RequestAttempt FIRST/RETRY state, P9_EOPIDSTALE handling, private transfer staging recovery.
**Produces:** no epoch-zero Twrite override; generic ambiguous dispatch remains RETRY.

- [ ] Add the A/applied/lost reply → B/write+fsync → server replay-state loss → A/retry regression; B must survive. Retain cached lost-reply success and proven non-dispatch FIRST retry tests.
- [ ] Run the new regression against old behavior and record the failure.
- [ ] Remove only the standalone wire flag override; use the existing dispatch state:
  ```rust
  P9Message::new_with_op_id_flags_and_origin(tag, op_id, op_flags, origin_epoch, body.clone())
  ```
- [ ] If transfer recovery now sees ambiguity, reconcile only privately owned staging or restart into a fresh temp object in the transfer layer; never silently retry arbitrary filesystem writes.
- [ ] Run replay/client and applicable transfer tests, review and commit.

### Task 2: Bind upload verification to publication

**Files:** zerofs/src/webui.rs, zerofs/src/fs/ops/rename.rs and focused filesystem helpers/tests.
**Consumes:** existing directory-entry inode/cookie, ordered inode locks, metadata fences, volatile-overlay drain and durability barriers.
**Produces:** filesystem-owned verified publication tied to source inode, entry identity, and content.

- [ ] Add deterministic tests for source-path replacement, same-length overwrite, and truncate after verification observation; report conflict rather than publishing unverified bytes. Include accepted volatile overlay writes.
- [ ] Observe the old race; do not settle for asserting mock callbacks.
- [ ] Implement verification and conditional publication under existing fences/locks. Refactor a private locked rename core if necessary, rather than recursively acquiring locks or creating an upload-only lock system. If verification uses a generation token, every materialized and volatile mutation must invalidate it atomically.
- [ ] Keep positive HTTP response bound to the verified inode and configured durability target. Preserve ordinary rename API behavior and destination conflict checks.
- [ ] Run upload, rename, volatile mutation and durability regressions; commit the coherent slice.

### Task 3: Conditional assembly cleanup

**Files:** zerofs/src/fs/ops/remove.rs, webui.rs, focused unlink/assembly tests.
**Consumes:** parent/name, expected inode and directory-entry cookie from assembly resolution.
**Produces:** internal compare-and-unlink using the same filesystem fences/transaction as unlink.

- [ ] Reproduce pathname replacement between observation and cleanup; include remove/recreate and hard-link replacement. Replacement must survive.
- [ ] Add an internal conditional unlink variant; compare expectation under existing locks before any unlink mutation. Ordinary remove remains unchanged.
- [ ] Replace upload lookup-then-remove with that variant; skip/retain changed entries.
- [ ] Run removal and assembly tests, review and commit.

### Task 4: Bound aggregate HTTP ingress memory

**Files:** zerofs/src/webui.rs and its tests; reuse existing admission/cancellation helpers.
**Consumes:** upload_write_permits and filesystem-owned accepted writes.
**Produces:** ingress request/byte ownership separate from active write permits, shared with assembly.

- [ ] Add stalled partial-body and full-buffer/blocked-writer saturation tests asserting resident/request bounds and prompt overload rejection.
- [ ] Acquire bounded ingress ownership before buffer allocation or filesystem entry creation. Account for retained frames plus copied chunks; lazily allocate and reject excess requests rather than building unbounded waiters.
- [ ] Add body-idle and shutdown cancellation without canceling/replaying an already accepted mutation. Assembly participates in the same documented aggregate budget and write concurrency.
- [ ] Retain slow-body fairness regression; test shutdown releases ingress ownership and accepted writes settle safely. Run upload suite and commit.

### Task 5: Bind destructive recovery evidence to journal identity

**Files:** zerofs/src/cli/debug.rs, cli/init.rs, shared existing identity/key helpers, targeted recovery tests.
**Consumes:** JournalIdentity (endpoint/kind/bucket/database prefix/encryption key identity) and bootstrap normalization.
**Produces:** a shared comparison used by accept_remote_writeback_branch and reseed_writeback_predecessor before journal mutation.

- [ ] Add config B/journal A mismatch cases for identity fields, with otherwise valid path/counter/hash evidence. Snapshot counters/rows/payloads and require unchanged state on rejection. Correct relocated offline journal remains valid.
- [ ] Observe the missing guard before implementation.
- [ ] Reuse/extract bootstrap identity normalization and read existing remote bucket/key identity without creating or initializing anything. Compare complete identity before mutation. Do not replace counter/hash/maintenance checks.
- [ ] Run recovery/identity tests, review and commit.

### Task 6: Preserve healthy SFTP sessions during credential refresh

**Files:** zerofs/src/sftp_transport.rs, russh_session classification where necessary, targeted transport tests.
**Consumes:** existing dial gate/backoff, session health/lease activity, session factory reload.
**Produces:** refresh for future dials without unconditionally recycling established sessions; stale recovery decisions cannot override dial success.

- [ ] Reproduce healthy established work + failed expansion dial + expired watchdog; healthy session must survive. Add successful-dial race and rebuild-None/failure cases.
- [ ] Distinguish explicit credential rejection from transport failure during authentication.
- [ ] Separate credential refresh from destructive recycling. Serialize or generation-check against dial success. Recycle only sessions independently evidenced unusable; no pool-wide cancellation merely because authentication timer expired.
- [ ] Preserve genuine auth-stall recovery test and disabled-window behavior. Run transport suite, review and commit.

## Integration and release gates

- [ ] Review each full commit range for spec compliance and code quality; no mock-only race proof.
- [ ] Merge reviewed commits into the main develop tree without absorbing unrelated changes.
- [ ] Run full WebUI-enabled Rust library suite, ninep-client/transfer suites, formatting on touched files, and 179-test Proxmox suite. Record pre-existing failures separately.
- [ ] Build the real production artifact only after all six findings are closed.
- [ ] Production deployment remains subject to existing remote-drain, client quiescence, exact config/resource/namespace, rollback and live data-path verification gates; never bypass drain to finish faster.
- [ ] Recheck all 1090 uploaded source paths/sizes, remote writeback, physical cache capacity and restoration of original host reserved blocks 46841676 after sustainable cache resizing.
