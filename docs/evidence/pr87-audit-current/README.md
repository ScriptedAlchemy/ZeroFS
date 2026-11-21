# PR87 and combined integration audit

## Final source snapshot

The final runtime validation uses local source snapshot `6c6846baa2929ba6372f23a1a468a226f6cfa43c`, tree `5fed08ddaf7f9ed2562918309a50d34d598d6908`. It combines the latest PR87 contributor commit `75036e9de5a4ae114fd85decfa9383e041163782`, the reviewed PR86/PR89 integration base `ca34705e22dd9095d15a3975a48dd310286ea177`, and the additional audit corrections.

The matching GitHub source commit is `5eb785217012678e67738475952490c95d1c2fb3`, with exactly the same tree. Its parents preserve the latest PR87 and integration heads. A later evidence-only commit can have a different SHA; the integration audit receipt records the final publication and branch references.

## Corrections and preserved contributor changes

- Namespace mutations and 9P read/write opens request every required permission bit together. Separate matching POSIX group entries cannot each contribute a different bit to one operation.
- An inode's permission mode and ownership stay paired with its ACL in one committed database view. Writer and read-only refresh tests cover the mixed-version failure. NFS ACCESS retains acknowledged pending-write attributes while checking coherent permissions.
- NFSv4 directory write requires explicit DELETE_CHILD; inherited child DELETE alone is refused. Unknown identity flags and undefined named IDs are also refused. ACL transactions account for removed rows before charging new rows.
- The latest contributor's FUSE request credentials and negotiated create-umask handling are preserved. Tests cover both new dialects and the legacy fallback, all three private create request forms, file/directory/regular-node/FIFO creation with and without default ACLs, and setgid handling with incomplete versus known group membership.
- Directory creation now normalizes the stored permission mode, matching regular and special-node creation. The new FUSE path supplies file-type bits separately from permission bits; those type bits must not persist in the inode's permission field.
- The combined storage tests cover degraded startup, exact member placement, journal recovery, deferred SFTP construction, and accepted generated-create retries under full RAM/SSD budgets. Identical retries retain the original acknowledgement target and terminal admission checks without waiting for a second reservation.

## Final validation

| Check | Result and scope |
| --- | --- |
| Combined native regression executable | 131 passed, 0 failed on the final local source snapshot |
| Native client protocol regressions | 2 passed: legacy fallback and both negotiated-UMASK dialects across all private create forms |
| Direct ordinary production typecheck | Passed on earlier local snapshot `faaced5c3fb10ada906ac6618232097319e5448a`; real crate root and full CLI, no whole-crate cfg(test), no default features |
| Formatting and whitespace | Passed |
| Changed mounted-test script | Python syntax checked |

The 131 entrypoints comprise 80 ACL/FUSE/9P/NFS cases and 51 pool, writeback, storage constructor, deferred/warm transport, and recovery cases. Their test bodies and assertions come from the checked-in source. The harness copies all 287 files in the checked-in `zerofs/src` tree, exposes existing fixtures and selected tests, and calls them from a standalone driver. It compiles the actual production `mount.rs` request-credential and umask paths without whole-crate cfg(test). Its CLI subset contains the exact recovery and cleanup functions required by the selected tests.

The real, unmodified crate root and full CLI were separately type-checked with direct rustc. The initial attempt needed an isolated rebuild of the path-based `zerofs-client` wrapper to match the updated 9P dependencies. The successful check passed without warning or error output. The later changes were two test-module typing fixes and one production line normalizing directory modes. The final focused executable compiles and executes that production line; the earlier full-CLI typecheck is not claimed at the final SHA.

The 9P protocol/client and path-based client wrapper were compiled into isolated scratch outputs against the coherent cached dependency graph. These isolated client builds left the shared Cargo target unchanged. The native client test binary executes the original client code and tests, including serialization and operation-envelope assertions.

### Failure found by the combined run

The first executed combined run passed 130 of 131 cases. The new private-create umask regression found a directory mode of `0o42000` where the permission field should have contained `0o2000`: the extra bits were S_IFDIR. Directory creation had accepted the raw mode while its sibling constructors called `validate_mode`. The fix adds the same normalization to mkdir.

The regression's assertions and compiled test file are byte-identical before and after this fix. The strict raw-mode assertion remains, alongside the separate inode-type assertion. The final rerun passes all 131 cases. The previous executable, logs, and receipts were retained as direct failure evidence; no separate synthetic mutation was needed for this defect.

The first expanded harness compilation also exposed missing test-only fixture imports and typing/setup mistakes in the new tests. These were corrected before runtime validation. They did not require changes to production behavior.

## Historical stage and evidence

[The initial audit stage](initial-stage-README.md) and its [validation record](validation.json) retain the earlier 76-case run, overlapping 41-test unit run, and unchanged-test NFSv4 original-defect proof. Those counts are historical stages, not additional unique final tests.

[integration-validation.json](integration-validation.json) records the final 131 results, native client results, local source identifiers, selected source hashes, harness transformations, earlier production-check scope, and unchanged directory-mode regression proof.

Representative normal-suite commands remain:

```sh
cargo test --locked --no-default-features -p zerofs --lib fs::permissions
cargo test --locked --no-default-features -p zerofs --lib fs::ops::xattr
cargo test --locked --no-default-features -p zerofs --lib private_create_umask_is_applied_only_without_a_default_acl
cargo test --locked --no-default-features -p zerofs --lib writeback_segment_retry
cargo test --locked --no-default-features -p zerofs --lib degraded_local_pool_keeps_an_unused_sftp_member_deferred
cargo test --locked -p ninep-client legacy_dialect_fallback_masks_private_create_mode_before_sending
cargo test --locked -p ninep-client negotiated_umask_dialect_preserves_all_private_create_requests
```

This is developer-profile correctness evidence, not a full Cargo test or Clippy run. No new mounted FUSE/kernel 9P qualification, NAS deployment, physical-disk qualification, or throughput measurement was performed. The larger SSD-budget mutation harness was not executed. The integration audit receipt records the separate WebUI/WASM production build and hosted checks.
