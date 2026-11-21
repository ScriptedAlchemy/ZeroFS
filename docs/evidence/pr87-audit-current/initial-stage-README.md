# PR87 current-source audit

This review starts at remote PR87 commit `948aa2563e60d99caa20ad48804225dcfe00a755`. It restores and rechecks the unpublished ACL audit corrections, reconciles them with the current integration code, and validates source commit `3f9706629f849aa71839af01fd29892d1f07728a`. The earlier [pass8 receipt](../deep-audit-pass8-acl/README.md) remains historical evidence.

## Corrections and reconciliation

- Namespace mutations and 9P read/write opens submit their complete permission request. Matching POSIX group entries cannot each contribute a different permission bit to one operation. The regression exercises create, mkdir, mknod, symlink, hardlink, remove, and same-directory and cross-directory rename, as well as 9P opens.
- An inode's mode and ownership are paired with its ACL from one committed database view. The writer and read-only reader tests exercise a commit or refresh between the two reads. NFS ACCESS retains acknowledged, pending-write attributes while using coherent permission fields.
- NFSv4 directory write without explicit DELETE_CHILD is refused. An inherited child DELETE grant cannot safely authorize deleting every child, because a child's group or ACL may differ from its parent's. Unknown identity flags and undefined named IDs are also refused.
- ACL row-limit admission deducts rows removed by the same transaction before charging new rows.
- The merge preserves the current integration's creation-GID validation, truncated-group provenance, removal of the old mode-only authorization API, and NFS ACCESS fast path for zero or unsupported-only rights.
- Additional tests check combined named-user precedence, owner precedence, ACL mask restrictions, and NFS ACCESS with a corrupt ACL when no supported right was requested. Documentation attributes umask narrowing to the client correctly.

## Fresh validation

All **76 focused regression entrypoints passed**. These cover the complete POSIX ACL parser and NFSv4 translator test modules, permissions, ACL storage and inheritance, chmod, row limits, writer/reader snapshot consistency, namespace mutations, 9P opens and creation identity, rename integration regressions, inventory reporting, and NFS ACCESS including pending-write attributes. The current-source executable compiled successfully without warnings or errors.

The harness copied all 287 current production source files and verified their hashes before compilation. It exposed existing fixtures and selected tests, removed test attributes to call those tests from a standalone driver, and omitted the CLI command entrypoint. Production algorithm bodies and selected test assertions were unchanged. This is a focused developer-profile correctness check, rather than a complete Cargo test invocation.

A separate small source harness also passed **41 unit tests**. These overlap with the focused set and are not 41 additional unique regressions. Its two runtime-dependent permission tests were included in, and passed, the 76-case run.

For a fresh negative control, a separate source copy restored the exact original NFSv4 translator function bodies from remote commit `948aa256`. The unchanged `inherited_delete_does_not_authorize_deleting_every_child` test then failed at the expected assertion: the old implementation accepted the unsafe mapping. The fixed test passed in both current-source runs. A larger five-selector negative-control harness was prepared but was not compiled or executed; no result is claimed for it.

The existing shared dependency cache initially contained a SlateDB artifact incompatible with its overwritten Foyer dependency. After the dependency cache was repaired, the fresh source build and all 76 tests passed. The earlier cache failure was not treated as a source failure or as test evidence.

`validation.json` identifies the tested commit and tree, selectors, source hashes, harness transformations, build result, and negative-control result. Formatting, diff whitespace checks, and Python syntax checking of the changed mount script also passed. No new mounted FUSE/kernel 9P run, NAS deployment, or throughput measurement was performed.

The normal Rust suite retains the regressions. Representative commands are:

```sh
cargo test --locked --no-default-features -p zerofs --lib fs::permissions
cargo test --locked --no-default-features -p zerofs --lib fs::ops::xattr
cargo test --locked --no-default-features -p zerofs --lib namespace_mutations_require_a_combined_acl_group_grant
cargo test --locked --no-default-features -p zerofs --lib db::permission_snapshot_tests
cargo test --locked --no-default-features -p zerofs --lib opens_require_one_acl_entry_to_grant_the_complete_access_mode
cargo test --locked --no-default-features -p zerofs --lib nfs::access_tests
```
