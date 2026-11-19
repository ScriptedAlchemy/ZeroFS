# Deep audit: ACLs and protocol authorization

This is the historical pass8 receipt. The [current-source rerun](../pr87-audit-current/README.md) records the review and validation after reconciling these corrections with the current PR87 and integration code.

Reviewed PR87 through incoming commit `84b5dc515185ae918caf7292f7c48144ff3853b0`, preserving its kernel 9P mount findings and historical receipts. The code corrections below are additional to those recorded mounted runs.

## Corrections

- **Complete permission requests:** opening read/write or mutating a directory now asks for all required permission bits together. Two separate POSIX group entries cannot each supply one bit of the same operation. NFS ACCESS still reports its separate rights independently; each directory mutation right requires combined write/search access.
- **Coherent permission state:** inode mode/ownership and ACL rows come from one committed database view when an ACL can affect authorization. Writer snapshots and retained reader iterators prevent mixed-version grants across concurrent ACL changes or reader refresh. Owner, root and no-ACL paths retain their existing fast paths. Permission checks add no canonical inode lock.
- **Group provenance:** an inline list shortened locally cannot claim completeness or inherit the special fallback for an upstream kernel that already checked permissions. Omitted restrictive groups cannot become an OTHER grant; group changes and setgid retention require known membership. Provenance survives credential roundtrips and is included in replay identity without changing ordinary request fingerprints.
- **Creation identity:** a requested new inode group travels as creation metadata instead of replacing the caller's authorization group. Setgid-parent inheritance and listener credential overrides remain effective. The creation-only group hint is rejected by setattr.
- **NFS ACCESS:** the dependency's former grant-all handler now calls an authoritative backend hook. ZeroFS applies shared-identity mapping, ACLs, read-only restrictions, supported/requested-bit filtering and normal missing-inode errors. Pending RAM-write attributes remain visible without materialization; permission-bearing fields stay paired with the ACL snapshot.
- **NFSv4 translation:** directory write without DELETE_CHILD is refused even when an inheritable child DELETE grant exists. A child's group or ACL need not equal the parent's, so that exception could widen deletion rights. The previous contract disclosed the exception; this update removes it and aligns the implementation with the no-widening rule. Unknown identity flags and undefined named IDs are also refused.
- **Attribute limits:** an ACL transaction accounts for rows it removes as well as rows it adds. Replacing an access ACL with a mode-equivalent representation while adding a default ACL no longer reports false ENOSPC at the row limit.

The NFS dependency change is isolated in [ScriptedAlchemy/nfsserve PR2](https://github.com/ScriptedAlchemy/nfsserve/pull/2) and pinned to `0f7d4f6666979d9ba683ae3288b2c8ee19cc3634`.

## Validation

All 65 selected regression entrypoints passed. The set includes the complete ACL parser/translator test modules, storage and inheritance tests, real SlateDB writer/reader snapshot tests, namespace and 9P open checks, creation identity, credential truncation, inventory reporting, NFS ACCESS and pending-write visibility. One initial high-ID positive-control ACL was itself unrepresentable because the owner's access depended on group membership; the fixture was corrected to include an unconditional owner grant. No parser rule was relaxed.

The old-code check compiled a separate harness copy after restoring the original faulty functions from `2ef04b3`. All seven selected regressions then failed at the expected behavioral assertions: 9P combined open rights, namespace combined mutation rights, mixed-generation inode/ACL authorization, unsafe inherited-DELETE translation, false ACL row-limit refusal, omitted restrictive groups, and creation-GID authorization. The test bodies were identical to the green run. The only type-shape adaptation added `groups_truncated: false` to the old credentials constructor; it restored the old completeness claim rather than adding the fix. Production and green-harness sources were left unchanged.

The NFS dependency separately passed all 55 tests and both example builds, plus strict release Clippy for all targets/features. Restoring its original ACCESS handler made five new RPC regressions fail; the fixed source was restored exactly and all 55 tests passed again. Native client cache implications were established by kernel-source inspection, not a mounted cache-exposure reproduction.

`validation.json` records individual results and source hashes. This is a local developer-profile correctness check. Whole-workspace hosted CI and fresh mounted qualification remain required before merge/release; the older mount receipts do not qualify these changes. No NAS deployment or new network-throughput measurement was performed.

The normal Rust suite retains every regression. Representative focused commands are:

```sh
cargo test --locked -p zerofs --lib fs::permissions
cargo test --locked -p zerofs --lib fs::ops::xattr
cargo test --locked -p zerofs --lib db::permission_snapshot_tests
cargo test --locked -p zerofs --lib nfs::access_tests
cargo test --locked -p zerofs --lib requested_creation_gid_never_changes_parent_authorization
```

## Operator-facing behavior

Directory NFSv4 MODIFY presets without explicit DELETE_CHILD now return EOPNOTSUPP with the named refusal rule. The mount script tests refusal without changing stored permissions, then tests an explicitly representable variant. Kernel 9P access=user continues to refuse ACL calls; FUSE remains the path for writing ACLs. Historical receipts are preserved, and the changed mount script was syntax-checked only in this audit.
