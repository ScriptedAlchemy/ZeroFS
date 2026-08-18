# Unified Tiered Writeback Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Deliver, prove, integrate, and clean up one bounded volatile mutation/durability layer shared by NBD, NFS, 9P, WebUI/RPC, and direct callers, followed by smooth SSD-to-remote pacing.

**Architecture:** Plan A builds the prepared-mutation layer and composes every shipping adapter around one durability/lifecycle authority. Plan B separately hardens exact SSD/multipart ownership and adds ordered cleanup credit pacing. Plan C proves the result on portable macOS gates and real UUID-isolated Ubuntu protocol/filesystem/device/crash/performance gates before reviewed merge and fail-closed checkout synchronization.

**Tech Stack:** Rust 2024, Tokio, SlateDB, existing ZeroFS writeback/SFTP, NBD, NFSv3, 9P2000.L, gRPC-Web/WebSocket WebUI, Python 3 `unittest`, Linux NFS/v9fs/nbd-client, XFS/ZFS, xfstests, pjdfstest, stress-ng, Cargo, Git worktrees.

**Spec:** `docs/superpowers/specs/2026-08-18-unified-tiered-writeback-design.md`

## Global Constraints

- Work only in `/Volumes/bigssd/projects/ZeroFS/.worktrees/unified-tiered-writeback` until final reviewed merge. The separately inventoried `ScriptedAlchemy/nfsserve` dependency worktree in Task A16 is the sole explicit exception.
- Root is the one landing owner for ZeroFS and the NFS dependency fork. Task agents receive non-overlapping file fences and never merge, push, deploy, or rewrite shared history independently.
- Materialized mode remains the generated/runtime default. Shared `volatile_memory` is explicit, globally byte/op bounded, unsafe before a completed durability barrier, and invalid without persistent local writeback.
- Filesystem validation, permission, quota, timestamps, inode metadata, extents, compression, encryption, and canonical persistence remain owned by the existing filesystem path.
- NBD guest files remain a different namespace from direct NFS/9P files; shared admission/coherence/durability does not claim guest namespace unification.
- With writeback enabled, filesystem FLUSH/fsync/COMMIT resolves to `LocalSsd`; materialized/direct backend with writeback disabled resolves to `RemoteBackend`. Adapters consume the resolved target.
- Every implementation task starts with a named focused RED test, ends with non-vacuous focused GREEN tests, formatting/diff checks, independent review, and one exact-file commit.
- Commit commands name every file. Never stage broad directories. Never edit the approved spec as part of implementation; any amendment is a separate reviewed documentation task.
- Portable unit/build/lint/model/WebUI/WASM gates may run on macOS. Real Linux protocol, mount, filesystem, block-device, crash-process, and performance proof runs only on Ubuntu from the exact pushed SHA.
- Every real resource is UUID-unique and ledgered. Never use CT198, VM100 production mounts, production prefixes/exports, or active devices.
- No fake protocol, mock-only acceptance, in-memory substitute, zero-test filter, or disconnected layer may be presented as integrated proof.

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

## Recorded CT198 Operational Exception

Before this feature implementation lane, the user authorized one operational exception: production CT198 was deployed at exact commit `00ef7a9f6070b6a7b969e391b6248f506c5b5806` only after the official four-sample drain/no-NFS guard passed. This receipt is not feature acceptance and does not prove unified writeback behavior. No further CT198 deployment or restart is permitted during implementation, proof, merge, or Ubuntu source synchronization.

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
  -> A18 metrics/status/docs
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
Plan C — proof, cleanup, quality, and integration
 C1 failpoint/DST crash model
  -> C2 dual-ack real Linux harness
  -> C3 cross-protocol/WebUI/RPC proof
  -> C4 exact Linux filesystem workflows
  -> C5 CI legs
  -> C6 crash/restart/terminal/shutdown proof
  -> C7 ledger-derived benchmarks
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
- [ ] Plan B is complete and green: physical sampling, SSD reservation, release credit, and multipart promotion each have one owner, with no acknowledgement-semantic change.
- [ ] Plan C proves real simultaneous NBD/NFS/9P admission, same-backing-inode pending reads, every cross-adapter barrier, WebUI/RPC production paths, crash/restart, Linux filesystems, integrity, and paced performance.
- [ ] Materialized mode remains the generated/runtime default; volatile mode is explicitly lossy before the completed local floor and durable afterward.
- [ ] At/below the completed local floor all state is complete/consistent; above it only the explicitly permitted canonical striped-NBD member prefix may survive, with no torn metadata/namespace claim.
- [ ] Every filtered test was listed first or replaced by a complete module gate; no zero-test result is accepted.
- [ ] Every ledger has two successful idempotent cleanup calls plus `assert-clean`; no temporary mount, device, process, listener, socket, pool/filesystem, backend prefix, directory, or secondary worktree remains.
- [ ] Simplify, deslop, branch-scope, low-value-churn, TraceDecay code-health/Hawk, thermonuclear correctness/security, and thermonuclear maintainability reviews have no unresolved P0/P1/P2.
- [ ] `develop` is fast-forwarded, pushed, and clean at the reviewed SHA.
- [ ] Ubuntu `/fast/projects/ZeroFS` passed clean-porcelain, exact-branch, expected-old/new-SHA, no-active-job, and ancestry checks before fast-forward; it equals pushed `develop` and is clean afterward.
- [ ] No feature deployment or restart occurred on CT198 after the recorded `00ef7a9f6070b6a7b969e391b6248f506c5b5806` operational exception.

## Final Command Families

Plan C specifies every CWD and exact command. The non-negotiable families are:

- workspace locked build, full test, strict Clippy, and fmt;
- failpoints and DST strict Clippy/tests;
- `zerofs-client --all-features`, ninep WASM, `make webui`, and the real WASM smoke;
- `python3 -m compileall`, both `unittest discover` trees, Proxmox `shellcheck`, and `actionlint`;
- standalone `bench/` fmt, strict Clippy, debug/release build, and CLI benchmark list;
- real Ubuntu dual-ack protocol, WebUI/RPC, xfstests, pjdfstest, kernel compile, stress-ng, XFS/ZFS-over-NBD, failover, crash, and benchmark receipts;
- final diff/evidence/cleanup/quality reviews, merge/push, fail-closed Ubuntu fast-forward, and secondary-worktree removal.
