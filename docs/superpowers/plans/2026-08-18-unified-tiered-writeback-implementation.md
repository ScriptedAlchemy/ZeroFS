# Unified Tiered Writeback Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Deliver, prove, integrate, and clean up one bounded volatile mutation/durability layer shared by NBD, NFS, 9P, WebUI/RPC, and direct callers, followed by smooth SSD-to-remote pacing, bounded cache-state-proved read fanout, aggregate resident-memory containment, and measured SSH/SFTP transport scaling.

**Architecture:** Plan A builds the prepared-mutation layer, composes every shipping adapter around one durability/lifecycle authority, removes protocol-visible one-run-at-a-time read serialization, bounds protocol ingress and total process residency, and selects any non-stock SSH transport only from real direction-specific evidence. Plan B separately hardens exact SSD/multipart ownership and adds ordered cleanup credit pacing. Plan C proves the result on portable macOS gates and real UUID-isolated Ubuntu protocol/filesystem/device/crash/performance gates before reviewed merge and fail-closed checkout synchronization.

**Tech Stack:** Rust 2024, Tokio, SlateDB, existing ZeroFS writeback/SFTP, stock OpenSSH plus an optional pinned HPN executable, Linux cgroup v2, NBD, NFSv3, 9P2000.L, gRPC-Web/WebSocket WebUI, Python 3 `unittest`, Linux NFS/v9fs/nbd-client, XFS/ZFS, xfstests, pjdfstest, stress-ng, Cargo, Git worktrees.

**Spec:** `docs/superpowers/specs/2026-08-18-unified-tiered-writeback-design.md`

## Global Constraints

- Work only in `/Volumes/bigssd/projects/ZeroFS/.worktrees/unified-tiered-writeback` until final reviewed merge. The separately inventoried `ScriptedAlchemy/nfsserve` dependency worktree in Task A16 is the sole explicit exception.
- Root is the one landing owner for ZeroFS and the NFS dependency fork. Task agents receive non-overlapping file fences and never merge, push, deploy, or rewrite shared history independently.
- Materialized mode remains the generated/runtime default. Shared `volatile_memory` is explicit, globally byte/op bounded, unsafe before a completed durability barrier, and invalid without persistent local writeback.
- Preserve the production-shaped profile as an acceptance contract: configured 16 GB shared dirty-write RAM, 64 GB separate clean read cache, a 1 TB local SSD tier shared by configured clean-cache and durable journal/staging budgets, and a 5 TB-class user-visible export. FUSE is not a shipping frontend; NBD, NFS, 9P, WebUI, and direct callers use ZeroFS tiering.
- The 16 GB and 64 GB values are payload contracts, not proof that a 96 GiB process/cgroup can hold them. Aggregate cache overhead/replacement, protocol bodies, allocator residency, maintenance work, and an explicit reserve must fit the configured/effective resident limit or startup fails closed.
- A 4 GiB write may not fall to remote speed; a 100 GiB copy proves RAM-to-SSD transition while remote drain proceeds concurrently; an SSD-pressure leg proves smooth remote-rate pacing. Compare the SSD leg with the same-host durable local control (production target about 800-900 MB/s) and remote drain with a durability-matched raw SFTP control (production target about 70-100 MB/s).
- Filesystem validation, permission, quota, timestamps, inode metadata, extents, compression, encryption, and canonical persistence remain owned by the existing filesystem path.
- NBD guest files remain a different namespace from direct NFS/9P files; shared admission/coherence/durability does not claim guest namespace unification.
- VM100's current production file-sharing deployment is NFS-only at `/mnt/zerofs-files`, matching the namespace mounted by the Mac. NBD/XFS remains an optional capability proved only on disposable Ubuntu resources in this project; do not install or mount it on VM100 during this rollout.
- With writeback enabled, filesystem FLUSH/fsync/COMMIT resolves to `LocalSsd`; materialized/direct backend with writeback disabled resolves to `RemoteBackend`. Adapters consume the resolved target.
- Every implementation task starts with a named focused RED test, ends with non-vacuous focused GREEN tests, formatting/diff checks, independent review, and one exact-file commit.
- Commit commands name every file. Never stage broad directories. Never edit the approved spec as part of implementation; any amendment is a separate reviewed documentation task.
- Portable unit/build/lint/model/WebUI/WASM gates may run on macOS. Real Linux protocol, mount, filesystem, block-device, crash-process, and performance proof runs only on Ubuntu from the exact pushed SHA.
- Every real resource is UUID-unique and ledgered. The immutable ledger/cleanup receipts live in a UUID control root separate from the disposable UUID resource root; cleanup removes the resource root while preserving its external authority until receipts are archived and the control root is finalized. Never use CT198, VM100 production mounts, production prefixes/exports, or active devices.
- Every real Ubuntu slice follows RED, implementation, portable GREEN, exact commit/review, push, fail-closed synchronization to that commit's literal 40-hex SHA, then Linux proof. Every corrective commit repeats the same synchronization before proof reruns.
- No fake protocol, mock-only acceptance, in-memory substitute, zero-test filter, or disconnected layer may be presented as integrated proof.
- All canonical writes, including ordinary chunked NFS rsync writes, do not allocate the clean decoded read cache; later reads populate it normally. GC/compaction reads do not admit into clean caches. Every protocol body is byte/op charged before an owned copy; a retransmit is bounded while fingerprinted and joins before another raw-mutation/cache charge.
- Stock OpenSSH remains the default. A pinned HPN client is selected by an explicit absolute `[sftp].ssh_program` only after real upload/download A/B evidence; no task changes global `PATH`, `update-alternatives`, or the system SSH binary.

## Enforceable Review Limits

The task owner splits before exceeding any limit:

- facade/config glue: 250 production lines
- types: 300 production lines
- admission module: 400 production lines
- progress module: 400 production lines
- durability module: 400 production lines
- request-cache module: 400 production lines
- fence module: 400 production lines
- overlay module: 600 production lines
- materializer module: 600 production lines
- any async worker loop: 150 lines
- any function: 100 lines
- source plus inline tests: 1000 lines; move tests to a separate test module before crossing

## Recorded CT198 Operational History and Hard Stop

Before this feature implementation lane, the user authorized one operational exception: production CT198 was deployed at exact commit `00ef7a9f6070b6a7b969e391b6248f506c5b5806` only after the official four-sample drain/no-NFS guard passed. On 2026-08-19 CT198 later hit its 96 GiB/no-swap cgroup limit and systemd automatically restarted the service after the OOM kill. The incident showed zero dirty RAM while logical clean-cache payload was full and process residency escaped the payload budget; it also invalidated process-local 9P retry identity and produced fail-closed `EOPIDSTALE`. Neither event is feature acceptance or permission for another restart. No task in this plan may deploy or restart CT198 during implementation, proof, merge, or Ubuntu source synchronization.

## Synchronized Task Graph

```text
Plan A — shared filesystem mutation and protocol durability
 A1 coordination extraction
  -> A2 mutation module/config + resolved client durability target
  -> A3 canonical write extraction
  -> A4 transferable quota ownership
  -> A5 request identity/cache
  -> A6 raw admission/preparation guard/progress
  -> A7 overlay
  -> A8 materializer
  -> A9 conflict-fence primitive
  -> A10 metadata operation-family fence composition
  -> A11 typed durability/flush receipts
  -> A12 sole lifecycle close owner
  -> A13 NBD composition
  -> A14 old NBD overlay retirement
  -> A15 9P/direct/RPC/WebUI composition
  -> A16 additive nfsserve fork API
  -> A17 ZeroFS NFS composition
  -> A18 metrics/status/docs and pre-read composition gate
             |
             v
Plan B — paced SSD admission
 B1 split admission without behavior change
  -> B2 sole monotonic physical-space sampler
  -> B3 exact SSD reservation types/accounting
  -> B4 journal ownership transition/recovery seed
  -> B5 paced credits + physical refresher
  -> B6 atomic multipart reservation promotion
  -> B7 pacing metrics/docs
             |
             v
Plan A bounded performance extensions — after pacing ownership is stable
 A19 bounded fragmented-read fanout
  -> A20 aggregate resident-memory/cache-admission gate
  -> A21 bounded protocol-ingress/NFS-retransmit gate
  -> A22 explicit SSH selector
             |
             v
Plan C — proof, cleanup, quality, and integration
 C1 failpoint/DST crash model
  -> C2 dual-ack real Linux harness
  -> C3 cross-protocol/WebUI/RPC proof
  -> C4 exact Linux filesystem workflows
  -> C5 CI legs
  -> C6 crash/restart/terminal/shutdown proof
  -> C7 ledger-derived benchmarks
  -> C7B evidence-driven HPN/SFTP shipping correction + final Plan A gate
  -> C8 repository/evidence/quality gates
  -> C9 cleanup/merge/push/Ubuntu fast-forward/worktree removal
```

The detailed plans are:

- `docs/superpowers/plans/2026-08-18-shared-filesystem-mutation-writeback.md`
- `docs/superpowers/plans/2026-08-18-paced-ssd-admission.md`
- `docs/superpowers/plans/2026-08-18-tiered-writeback-proof-and-rollout.md`

## Baseline Gate Before A1

Do not treat a prior receipt as current after rebasing. From `/Volumes/bigssd/projects/ZeroFS/.worktrees/unified-tiered-writeback/zerofs` at the exact recorded starting SHA:

```bash
cargo build --workspace --locked
cargo test --workspace --all-targets --locked
cargo fmt --all -- --check
```

Record SHA, branch, porcelain, commands, and results. Ignored failover/performance tests remain explicit Plan C gates and are not counted by this baseline.

## Execution Ownership

- Before every task, Root records exact HEAD, branch, porcelain, active build/test processes, and file fence.
- After every task, Root reads every complete changed file, checks type/signature consistency with later tasks, runs the named task gate, audits scope/churn, and commits only the exact fence.
- Agents do not run broad Cargo jobs concurrently against one target directory. Root owns full-workspace Cargo gates.
- The NFS dependency worktree is created clean from upstream `d61b08456ae66108666978e29524a47d2209f68d`; its branch, dirty paths, tests, immutable pushed revision, and Root landing ownership are recorded before ZeroFS pins it.
- Ubuntu work occurs only in `/fast/projects/ZeroFS-unified-tiered-writeback`; `/fast/projects/ZeroFS` remains untouched until C9's fail-closed fast-forward.

## Final Evidence and Completion Checklist

- [ ] A requirement-to-evidence audit maps every approved-spec requirement to task, commit SHA, exact command, host/CWD, dual ack flags where applicable, receipt, and result.
- [ ] Plan A is complete and green: all adapters use one coordinator/durability target/lifecycle owner and `nbd/volatile_overlay.rs` is absent.
- [ ] Fragmented logically sequential reads fetch independent immutable segment runs with bounded ordered concurrency while contiguous reads remain one ranged GET and exact bytes/order are preserved.
- [ ] Aggregate resident memory stays below the effective cgroup/process limit by the configured reserve during NFS retransmits, concurrent 9P/WebUI traffic, clean-cache churn, segment work, and GC; every owner reconciles within a documented tolerance and cgroup `oom`/`oom_kill` deltas remain zero.
- [ ] Physical pressure is derived from OS RSS/cgroup current; jemalloc resident and retained remain separate diagnostics, retained virtual mappings are never added to physical RSS, and retained-only growth cannot trigger false permanent backpressure.
- [ ] The immutable pre-run memory receipt fixes cgroup `high` delta at most 8, accounting error at most 256 MiB, and unowned residual at most 2 GiB; missing, post-run-derived, or relaxed values fail the gate.
- [ ] The exact 96 GiB/no-swap incident envelope rejects the incompatible 64+16 GiB profile before serving, and a full-scale 128 GiB/no-swap Linux soak fills 64 GiB clean cache, exercises 16 GiB volatile memory, and survives replacement/GC overlap with the configured reserve.
- [ ] All canonical writes are write-no-allocate, maintenance reads are no-admit, every protocol body is charged before its owned copy, retries are bounded while fingerprinted and join before another raw-mutation/cache charge, and all cancellation/error paths release exactly once.
- [ ] Segment cardinality comes from checked version-1 stored geometry, not plaintext size or a new lower writer cap: a real deterministic-incompressible 8,192-frame object proves the frame region crosses 256 MiB, and a real maximally compressible threshold object records its actual roughly-four-million footer `k`, crossing-batch overshoot, directory bytes, and total object bytes.
- [ ] Wire/host geometry checks cover footer `k`, sealed-body `len`, `FrameLoc.byte_len`, frame-index addition, footer `dir_len`, offsets, buffers, and total length. Injected-limit boundary tests prove reservation/rotation overflow leaves the open generation exact and emits zero object PUTs; no GiB allocation is required to test the boundary.
- [ ] Version-1 directory verification uses authenticated spill-backed 4 MiB chunks, a 128 MiB Zstd window/64 KiB LZ4 history, 65,536-row/4-MiB external-sort runs, eight-way merge, and a 144 MiB resident maximum. The sole `SsdReservationToken` authority reserves `2*dir_len + 2*(k*28) + 2*ceil(k/65536)*256 + 4096` checked bytes before growth. UUID scratch cleans on success/error/cancel/startup; run manifests bind counts/ranges/hashes through every pass; full-stream differential/KAT tests prove exact one-shot equivalence and zero row release before CRC/AEAD success.
- [ ] Reclaim merge-verification coalesces at most 16 logical ranges per view, caps each consumer page at 65,536 rows and 4 MiB of encoded keys plus values, pre-acquires resident permits, and has no finite total row/page cap or point-read fallback. Each logical range owns one uncached source iterator with one fetch task, one-block read ahead, and no forwarding channel; page counters reset inside that stream without reopen. Sparse unrelated rows force multiple pages, source scans stay at most 32 with peak concurrency one, and both views reach EOF for one immutable geometry identity. Malformed/changed geometry, source reopen, scan/decode error, cancellation, or live reference fails closed to `Keep`. Fully qualified focused GREEN gates assert real `dir_offset`, `dir_len`, object bytes, footer `k`, decoded rows, page/source peaks, concurrency, and zero test-selection vacuity.
- [ ] Plan B is complete and green: physical sampling, SSD reservation, release credit, and multipart promotion each have one owner, with no acknowledgement-semantic change.
- [ ] Plan C proves real simultaneous NBD/NFS/9P admission, same-backing-inode pending reads, every cross-adapter barrier, WebUI/RPC production paths, crash/restart, Linux filesystems, integrity, and paced performance.
- [ ] Plan C proves cache-state-matched raw SFTP/NFS/9P/NBD read throughput with true remote-cold, clean-SSD, and clean-RAM evidence; historical client-page-cache-only “cold” numbers are not acceptance.
- [ ] Plan C proves the 4 GiB foreground-isolation, 100 GiB RAM-to-SSD transition, and SSD-pressure-to-remote pacing scenarios with the configured 16 GB/64 GB/1 TB/5 TB-class production profile and paired local/SFTP controls.
- [ ] Plan C proves stock OpenSSH, pinned HPN, and ZeroFS SFTP in both directions at one and configured-many sessions. The selected executable, per-session/aggregate rates, request depth, TCP evidence, exact bytes, SHA-256, durability, and cleanup are recorded; HPN receiver evidence is never claimed as an upload fix.
- [ ] The pinned HPN build passes a supervisor-bounded nonzero upstream inventory plus focused `transfer`, `rekey`, `sftp`, `sftp-batch`, `sftp-resume`, and `forwarding` regressions. Only pinned `dynamic-forward` is excluded, and timeout receipts prove TERM/KILL/reap of the complete process group.
- [ ] One registered `sftp-transport-decision` composite runs download A/B, durability-matched upload A/B, and ZeroFS session scaling inside one supervisor ledger at one SHA and is the only decision-authority producer. C7B reruns it after corrections; C8 runs it fresh at `FINAL_PROOF_SHA` and requires current upload parity. Separate/stale ledgers, a dormant selector, or a free-form decision value are not completion.
- [ ] Materialized mode remains the generated/runtime default; volatile mode is explicitly lossy before the completed local floor and durable afterward.
- [ ] At/below the completed local floor all state is complete/consistent; above it only the explicitly permitted canonical striped-NBD member prefix may survive, with no torn metadata/namespace claim.
- [ ] Every filtered test was listed first or replaced by a complete module gate; no zero-test result is accepted.
- [ ] Every external ledger has two successful idempotent cleanup calls plus `assert-clean`; its control root is removed only after ledger/receipts are hash-verified in `/fast/zerofs-tiered-receipts/$RUN_UUID`; no temporary mount, device, cgroup/scope, changed sysfs value, SSH/HPN process or build/install artifact, listener, socket, pool/filesystem, backend prefix/temp object, cache/state root, resource/control directory, or secondary worktree remains.
- [ ] C3/C4/C7/C7B allocate and run only through the tested `supervise` API; injected setup/substep/timeout/cleanup failures retain their independent receipts and no cleanup success masks a primary failure. `ledger-value` and `validate-owned-path` pass traversal/symlink/unknown-key rejection tests.
- [ ] Every NBD ledger stores immutable host-key/machine-id/VMID identity separately from the controller-reachable SSH target validated to it; before allocation the remote host rereads and byte-compares its own identity, so an alias or route change cannot bypass the VM100/CT198 prohibition and no undefined signing key is required.
- [ ] Every remote ledger, including non-NBD VM100 runs, carries a manifest-covered controller-target receipt. C9 emits and consumes only validated `normalized_target<TAB>ledger` rows and rejects hostname-, loop-, or discovery-host-derived cleanup targets.
- [ ] Simplify, deslop, branch-scope, low-value-churn, TraceDecay code-health/Hawk, thermonuclear correctness/security, and thermonuclear maintainability reviews have no unresolved P0/P1/P2.
- [ ] `develop` is fast-forwarded, pushed, and clean at the reviewed SHA.
- [ ] Ubuntu `/fast/projects/ZeroFS` passed clean-porcelain, exact-branch, expected-old/new-SHA, no-active-job, and ancestry checks before fast-forward; it equals pushed `develop` and is clean afterward.
- [ ] The CT198 history records both the authorized `00ef7a9f6070b6a7b969e391b6248f506c5b5806` deployment and the later automatic post-OOM restart; no task in this feature plan deployed or restarted CT198 afterward.

## Final Command Families

Plan C specifies every CWD and exact command. The non-negotiable families are:

- workspace locked build, full test, strict Clippy, and fmt;
- failpoints and DST strict Clippy/tests;
- `zerofs-client --all-features`, ninep WASM, `make webui`, and the real WASM smoke;
- `python3 -m compileall`, both `unittest discover` trees, Proxmox `shellcheck`, and `actionlint`;
- standalone `bench/` fmt, strict Clippy, debug/release build, and CLI benchmark list;
- real Ubuntu dual-ack protocol, WebUI/RPC, xfstests, pjdfstest, kernel compile, stress-ng, XFS/ZFS-over-NBD, failover, crash, and benchmark receipts;
- the ledgered read matrix over raw SFTP, kernel NFS, native 9P, and NBD/XFS with concurrency, cache, active-lane, exact-byte, and SHA-256 receipts;
- the cgroup-constrained resident-memory/retransmit/cache-churn matrix and the direction-specific stock/HPN/ZeroFS SFTP matrix;
- final diff/evidence/cleanup/quality reviews, merge/push, fail-closed Ubuntu fast-forward, and secondary-worktree removal.
