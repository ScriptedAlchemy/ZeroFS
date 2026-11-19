# Deep audit: protocol authorization, journal reconciliation and fid paths

Reviewed PR84 integration head `8509886dfd61e077c186afec767eaa3f2d643a4f`, alongside the newer degraded-start and ACL drafts. This receipt covers the fixes shared by the integration branch. The feature-specific receipts remain with PR86 and PR87.

## Corrections

- **Journal reconciliation integrity:** reconciliation now consumes one complete, hashed local-payload stream while comparing the remote object. A local journal payload changed after the initial verification cannot be accepted merely because the remote bytes match the changed payload. A final hash mismatch remains a permanent failure, retaining the committed record, payload and dirty-capacity reservation instead of advancing the completion watermark.
- **NFS ACCESS:** the dependency's former grant-all handler now calls a backend authorization hook. ZeroFS evaluates the actual requested and supported rights using the caller's effective identity, including shared-identity mapping, directory write/search requirements, read-only restrictions, and missing-inode errors. Rights are reported independently; unsupported or unrequested bits cannot be granted. The dependency is pinned to `0f7d4f6666979d9ba683ae3288b2c8ee19cc3634` in [ScriptedAlchemy/nfsserve PR2](https://github.com/ScriptedAlchemy/nfsserve/pull/2).
- **Creation GID authorization:** the requested group for a new entry is creation metadata rather than a replacement for the caller's primary group during parent authorization. This closes both an unintended grant and an unintended denial. Setgid-parent inheritance and configured listener identity overrides still take precedence where appropriate. Ordinary setattr rejects the creation-only hint.
- **Locally truncated group lists:** credentials retain the distinction between a complete list, upstream kernel-authorized unknown groups, and groups discarded locally to fit the inline representation. A locally shortened list can no longer claim completeness or inherit the kernel fallback. An omitted restrictive owning group cannot become an OTHER-class grant, and ownership changes require proven membership. The provenance survives credential conversion and participates in replay identity while preserving ordinary request fingerprints. This adds no heap allocation to the credential hot path.
- **Detached parent walks:** when `..` must clamp a detached directory chain to its attach root, the fid path now changes with the selected inode. The bounded reconstruction follows current parent/name pairs under the directory topology lock and verifies that each link still names the expected directory. It handles an attach root renamed after attachment and rejects a removed root. Ordinary walk components do not incur the reconstruction.
- **Repeated 9P Trename:** a successful move updates the still-matching source fid path, allowing another rename through that fid. The destination path is reconstructed from current ancestry instead of copying a possibly stale destination fid path. Same-inode hardlink no-ops and replayed operations leave the fid path alone. Expected-inode checks continue to reject a cached source path reused for another inode; the durable dedup format is unchanged. Failed renames preserve the path, including the ENOTDIR result for a non-directory destination fid.

## Validation

<!-- validation-status:start -->
All **41 selected regression entrypoints passed** against the final production source recorded in `validation.json`. Workspace formatting passed. Strict production-library Clippy passed with the Web UI feature (`--locked --no-default-features --features webui -p zerofs --lib -- -D warnings`). The receipt records the individual results, selected source hashes, harness transformations and check scopes.
<!-- validation-status:end -->

The selected harness compiles the production modules and exposes checked-in test bodies and existing fixture constructors in a temporary copy. It does not replace production permission checks, rename logic, journal verification or storage operations with mocks. Coverage includes the corrections above, journal corruption classification and retention, bounded streaming boundaries, cache retention and sparse extent behavior, directory pins, and volatile-write ordering and fairness. The preparation receipt records the fixture exposure and CLI-entrypoint exclusion explicitly.

For journal reconciliation, restoring only the original `verify_existing` body in a separate copied harness made the new integrity regression fail at the assertion that changed bytes must not retire the committed payload. The repaired implementation passes the same behavioral test. The old-body experiment did not modify the production checkout.

The NFS dependency independently passed all 55 tests and both example builds, with strict release Clippy for all targets and features. Restoring its former ACCESS handler made five new RPC regressions fail while the invalid-handle control passed. The fixed handler was restored byte for byte and all 55 tests passed again. The native client-cache consequence was assessed from kernel source; no mounted cache-exposure reproduction is claimed.

The normal Rust suite retains the regression tests. Representative focused commands in a normal project build environment are:

```sh
cargo test --locked -p zerofs --lib reconciliation_rehashes_compared_bytes_before_retiring_journal
cargo test --locked -p zerofs --lib nfs::access_tests
cargo test --locked -p zerofs --lib requested_creation_gid_never_changes_parent_authorization
cargo test --locked -p zerofs --lib locally_truncated_groups_do_not_gain_mode_or_group_authority
cargo test --locked -p zerofs --lib detached_parent_walk_restores_root_path_before_rename
cargo test --locked -p zerofs --lib ninep::handler::rename_fid_tests
```

## Scope and limits

These are local developer-profile correctness checks on Linux x86_64. They are not a complete workspace test run, a mounted-client qualification, a NAS deployment, or a new throughput measurement. Current-head hosted CI remains required before merge. The historical physical-storage and network-copy receipts do not qualify this changed code.

The Trename change repairs the request's own successful move. It does not establish global path coherence across later external ancestor renames, or eliminate the existing cancellation boundary between a committed namespace change and in-memory fid bookkeeping.

Whole-file placement is an optional future mode, not the default and not an acceptance gate for the existing storage path. This audit does not enable it, require it, or alter that decision.
