# Integration audit — 19 November 2026

This pass reviews the open component PRs #86, #87, and #89 together on
`integration/all-open-prs` (#84). The integration PR remains open; this work
does not merge it into `main`.

The final production-source snapshot is published commit
[`5eb785217012678e67738475952490c95d1c2fb3`](https://github.com/ScriptedAlchemy/ZeroFS/commit/5eb785217012678e67738475952490c95d1c2fb3),
tree `5fed08ddaf7f9ed2562918309a50d34d598d6908`. Its complete tree is identical
to local validation commit `6c6846baa2929ba6372f23a1a468a226f6cfa43c`.
Publication preserves the existing remote PR and integration parents and
verifies the complete resulting Git tree before moving the branch.
Documentation-only receipt updates do not alter the tested production source.
The PR descriptions identify the subsequent receipt and merge commits.

## Corrections reviewed

- **PR86:** conditional generation checks for empty objects; startup-missing
  member accounting throughout evacuation fences; ownership of native legacy
  cleanup after waiter timeout; exact-target refusal preservation; admission
  classification for pinned segments; and identity/durability preservation
  when retrying an accepted generated create. See the
  [component receipt](../pr86-audit-current/README.md).
- **PR87:** complete permission requests instead of combining unrelated ACL
  grants; coherent inode/ACL snapshots; NFS ACCESS preservation of pending-write
  attributes; conservative NFSv4 deletion-right translation; correct ACL row
  admission; and reconciliation of the newer FUSE credential and umask changes.
  See the [component receipt](../pr87-audit-current/README.md).
- **PR89:** integration of pool-aware storage construction and restoration of
  production recovery imports that were incorrectly restricted to test builds.
  Ordinary production compilation is checked separately from test fixtures.

## Additional combined-source corrections

### Identical retries under journal congestion

An accepted generated create could be locally durable while remote replay was
paused and the SSD journal budget was full. Its identical retry still waited
for a second SSD reservation. Another writer could also hold the RAM budget
while waiting for that same full journal, blocking the retry even earlier.

The retry now checks the accepted create under its key lock before allocating
RAM or SSD capacity. It hashes the existing immutable payload chunks without
concatenating them and reuses that verified payload for a new write. A matching
retry keeps the original sequence, ETag, exact target, and configured durability
barrier. The implementation checks terminal RAM, SSD, and remote states without
allocating capacity, releases the key lock before waiting for acknowledgment,
and rechecks after admission waits. Ordinary puts retain their original
reservation-before-copy ordering.

The new regressions pause remote replay, fill the exact SSD budget, optionally
queue another writer that owns all available RAM, and require the duplicate to
finish without advancing the accepted sequence. Separate cases preserve the
original remote acknowledgment barrier and reject terminal admission errors.
The original source files were retained for a possible negative control, but
no separate pre-fix executable was run for this additional correction; no
executed-before result is claimed.

A combined storage-construction regression also reopens a pool with one local
member absent and an unused SFTP member configured. It checks surviving local
reads, metadata writes, unchanged placement for the unavailable object, and
zero connections to an owned loopback listener through startup and shutdown.

### Directory mode normalization

The newer FUSE create path includes file-type and encoded umask bits in its
mode field. Regular-file and special-file creation already
normalize this to permission bits, but directory creation persisted the raw
mode. A private mkdir therefore left `S_IFDIR` in the inode's permission mode,
which is also returned in NFS-facing attributes.

Directory creation now uses the same `validate_mode` normalization as the other
creation paths. The combined regression run before this correction passed
130 of 131 cases; the new umask/inheritance test failed because the directory
stored `0o42000` instead of `0o2000`. Its strict raw-mode assertion and separate
inode-type assertions remain unchanged. This is an executed-before failure for
the production defect, distinct from the unexecuted negative control for the
journal-budget correction.

## Validation and environment

All fresh local checks use the isolated cloud Linux x86_64 executor, native
Rust 1.100.0, and no emulation. Native compiler work is serialized. They are
software correctness checks, not Mac Studio Docker, mounted-client,
physical-disk, production NAS, or throughput qualification.

- **All 131 combined native regression entrypoints pass** on the final source
  snapshot. These comprise 80 ACL/FUSE/9P/NFS cases and 51 pool, writeback,
  storage-construction, SFTP lifecycle, and CLI recovery cases. The unchanged
  directory-mode regression now passes. This is a focused exact-source harness
  using the checked-in test bodies and production algorithms, not a complete
  Cargo test invocation. The component receipt records fixture exposure and
  source hashes.
- The real production crate root and full CLI pass a direct `rustc`
  metadata/type check with no whole-crate `cfg(test)` and no default features.
  This ran on local snapshot `faaced5c`; the subsequent `fea4f4d` correction
  changes only two `cfg(test)` files. The final directory-mode correction is
  compiled and exercised by the combined native harness. The earlier metadata
  check is not a final-source Cargo build or Clippy invocation.
- Two current native 9P client protocol regressions pass: legacy fallback masks
  create modes; both new umask dialects preserve complete private-create
  request bodies and leave the caller's request unchanged.
- The Python scripts suite passes **406 tests**. Its initial run suffered six
  timeouts during compiler memory contention; the serialized rerun passed.
- The Web UI suites pass **122 tests**: 31 native Node tests and 91 Vitest tests.
- The kernel permission suite passes **7 tests**.
- A fresh optimized WebAssembly release build, matching wasm-bindgen bindings,
  initialization of the actual WASM module, TypeScript compilation, and Vite
  production build all pass. All **142 tracked build inputs** match the frozen
  source. The generated and bundled 606,727-byte WASM modules are identical,
  SHA-256 `f0e0029ef947415e1d4b1f5de9208cfc15ea6a3bfebf3f5707f33b52d6d7097d`.

The Python, Web UI unit, and kernel suites ran on integration commit
`ca34705e22dd9095d15a3975a48dd310286ea177`. Their relevant source is unchanged
in the final snapshot. The only changed file in those directories is the
manually mounted ACL integration script, which is not imported by the Python
unit suite; its syntax was checked separately. The fresh WASM build and native
protocol checks cover the changed Rust client and wire-protocol sources.

### Proxmox test environment limitation

The Proxmox suite ran 181 tests: **180 passed; one failed because this executor
does not expose `/proc/<pid>/task/<pid>/children`**. The failing test is
`test_holder_death_before_start_ack_never_executes_payload`. Its source and
the production deployment script are unchanged from `main`.

A minimal live parent/child reproduction confirmed the child through its
`/proc` status while the parent's `children` file was absent. A separate local
production-holder probe received both LOCKED and STARTED, verified the child's
stopped state and ownership before acknowledgment, then killed the holder.
The child disappeared, the payload never executed, and the production cleanup
returned success and removed its owned temporary directory. This explains the
environmental failure and checks the intended guard; it does not turn the
original failing test into a pass. The test was not weakened.

## Evidence boundaries

`validation.json` and `webui-source-manifest.json` retain commands, source
identity, input hashes, artifact hashes, and the environment diagnosis. The
component receipts retain their focused native selectors and transformations.
Earlier component and mounted-client receipts remain evidence for their own
revisions. Final-head hosted CI is a separate gate and is not represented as
complete by these local results. No production storage was accessed.
