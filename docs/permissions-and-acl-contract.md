# Permissions and ACL contract

This document states what ZeroFS enforces, what it translates, and what it
refuses when it stores ownership, mode bits and access control lists. A
migration tool or operator should check a source tree against it before copying
data in. The rule throughout is that ZeroFS never makes access looser than the
source without saying so per file. When it cannot express a source ACL, it
refuses the write.

## Decision for an NFSv4 source

The legacy TrueNAS share uses NFSv4 ACLs. Another lane read the dataset
properties at 2026-11-19T02:52:50Z: `storage/Storage` has `acltype=nfsv4`,
`aclmode=restricted` and `casesensitivity=insensitive`. This lane made no NAS
reads. Three options exist for such a source.

| Option | Status in ZeroFS |
| --- | --- |
| Enforce NFSv4 ACLs natively | Not implemented. ZeroFS has no NFSv4 ACL evaluator. |
| Translate NFSv4 ACLs to enforced POSIX ACLs, refusing every case that would change a read, write or execute decision | Implemented. Described below. |
| Keep NFSv4 ACLs as inert bytes | Not offered. ZeroFS never stores an NFSv4 ACL it does not enforce. Copying without ACLs needs an explicit operator waiver, which the copy tool must record. |

The copy tool must write ACLs, including `system.nfs4_acl_xdr`, through the
ZeroFS FUSE client. The kernel 9P mount refuses ACL writes (see "Client-side
checks").

The operator must decide, per source tree, between translation and a waiver.
`zerofs inventory`, run where the source's NFSv4 ACLs are visible, reports the
translation result for every entry (see "Checking a source tree" below). A tree
with no refused entries can migrate under translation. Each refused entry needs
a decision: change its ACL at the source, or waive ACL migration for it.

## What is enforced

ZeroFS checks permissions on the server for every request from every protocol.
The checks follow Linux `generic_permission`.

- Mode bits, ownership, the sticky bit and setgid inheritance, as before this
  change.
- POSIX access ACLs stored in `system.posix_acl_access`. A caller who is not
  root and not the owner is checked against the ACL: a matching named-user
  entry, else every matching group entry (owning group and named groups), else
  the other entry. Named users and all group-class entries are limited by the
  mask. A matching group entry that lacks the right denies the request; it
  does not fall through to other.
- POSIX default ACLs stored in `system.posix_acl_default` on directories. A new
  file, directory or special file inherits the default as its access ACL,
  limited by the requested mode, as Linux `posix_acl_create` does. A new
  directory also inherits the default as its own default. Symlinks get no ACL.
- `user.*` attribute reads and writes use the same ACL-aware read and write
  checks as file data.

The inode mode is the source of truth for the owner, mask and other
permissions. Setting an access ACL rewrites the mode's permission bits from the
ACL, with the mask in the group bits. A `chmod` rewrites the mask and the other
entry; the named entries keep their stored permissions and the mask limits
them. An ACL that the mode can express exactly (owner, owning group and other
only) is stored as the mode and leaves no ACL row, as on Linux.

Only the owner or root may set or remove an ACL. Any caller may read one, as on
Linux. A default ACL on a non-directory returns `EACCES`. An invalid ACL
(unknown tag, entries out of order, duplicate named ids, a named entry without
a mask, a permission bit above `rwx`) returns `EINVAL` and changes nothing. An
ACL value with a version other than 2 returns `EOPNOTSUPP`. An empty value
removes the ACL. Removing an absent ACL succeeds. ACLs on symlinks return
`EOPNOTSUPP`.

### Callers whose groups are unknown

A plain 9P2000.L attach carries a uid but no group list unless the server has a
credential override. ZeroFS then cannot tell which groups the caller is in.
For such a caller, a named-user entry still applies by uid. Otherwise the
caller gets a right only if the other entry and every group-class entry, each
limited by the mask, grant it. This can deny what Linux would allow. It never
allows what Linux would deny. `Trebind` supports a complete supplementary group
list of up to 16 entries. On Linux, the FUSE client reads the requesting thread's
groups from procfs when its filesystem uid and gid match the request and the
list fits the protocol. When the list is unavailable, the server uses the
request's primary gid and the incomplete-group ACL checks.

### Client-side checks

A kernel client also checks permissions itself before it sends a request. The
effective decision is the stricter of the client's and the server's.

- The ZeroFS FUSE client negotiates `FUSE_POSIX_ACL`. The kernel then fetches
  ACLs from the server and evaluates them, so named-user and named-group grants
  work. With a server that negotiates create umask handling and a kernel that
  supports `FUSE_DONT_MASK`, the server applies the umask only when the parent
  has no default ACL. An older peer or kernel may supply a mode already
  narrowed by the umask.
- The Linux kernel 9P client with the package's `access=user` mount cannot
  read or set ACLs. v9fs marks a superblock as ACL-capable only for
  `access=client,posixacl`, which the app rejected for other reasons, and the
  kernel's ACL calls require that flag. `setfacl` and a read of
  `system.posix_acl_access` fail with `EOPNOTSUPP`, and `getfacl` shows an ACL
  built from the mode. A write of `system.nfs4_acl_xdr` also fails with
  `EOPNOTSUPP`. The caller always gets that error, so an ACL is never
  silently dropped. The kernel checks only mode bits locally, so a named user
  or named group receives no more than the mode's group or other bits on that
  mount. The server still enforces every stored ACL for requests from that
  mount, so restrictions always hold.
- NFSv3 clients cannot read or set ACLs. The server enforces stored ACLs on
  every NFS operation that reaches a permission check.

## Translation of NFSv4 ACLs

A write of `system.nfs4_acl_xdr`, in the OpenZFS layout that TrueNAS emits,
asks ZeroFS to translate an NFSv4 ACL. The caller must own the inode or be
root. ZeroFS translates the ACL into a POSIX access ACL and, for a directory
with inheritable entries, a POSIX default ACL. It stores only the POSIX ACLs.
It does not store or serve the NFSv4 bytes: a read of `system.nfs4_acl_xdr`
returns `ENODATA`, and listings omit it. A refused ACL returns `EOPNOTSUPP` and
changes nothing. The server log records each translation with the resulting
ACLs and any narrowed rights, and each refusal with its rule.

### The acceptance rule

Translation is accepted only when the POSIX ACL makes the same decision as the
NFSv4 ACL for every request of read, write and execute, alone or combined, for
every class of caller. ZeroFS checks this by enumeration, not by pattern
matching. The caller classes are the owner, each named user, an unnamed user,
and every combination of membership in the owning group and the named groups.
For inheritable entries, ZeroFS repeats the check with the future child's owner
set to each named user and to an unnamed user, the child's group set to each
named group and to an unnamed group, and the child being a file and a
directory. NFSv4 evaluation follows RFC 8881 order: for each right, the first
matching entry that mentions it decides it, and an undecided right is denied.

NFSv4 rights map to POSIX bits as follows.

| POSIX | NFSv4 rights it stands for |
| --- | --- |
| `r` | `READ_DATA` (list directory). Requires `READ_NAMED_ATTRS`, because POSIX read also allows reading `user.*` attributes. |
| `w` on a file | `WRITE_DATA` and `APPEND_DATA` together. Requires `WRITE_NAMED_ATTRS` and `WRITE_ATTRIBUTES`, because POSIX write also allows writing `user.*` attributes and setting times to now. |
| `w` on a directory | `ADD_FILE` and `ADD_SUBDIRECTORY` together, with the requirements above, plus `DELETE_CHILD` or an inheritable `DELETE` for the same caller (see narrowings). |
| `x` | `EXECUTE` (traverse directory). |

### Patterns that translate

These TrueNAS patterns translate. The tests in
`zerofs/src/fs/permissions/nfs4/tests.rs` cover each one.

- `owner@`, `group@` and `everyone@` allow entries with the TrueNAS basic sets
  `FULL_CONTROL`, `MODIFY`, `READ` and `TRAVERSE`, including the
  `NFS4_RESTRICTED`, `NFS4_OPEN` and `NFS4_HOME` presets.
- Named group entries, such as the SMB `BUILTIN\Users` (gid 545) and
  `BUILTIN\Administrators` (gid 544) groups, and named user entries. A POSIX
  mask is added equal to the union of the named and group-class permissions.
- File-and-directory inheritance (`FILE_INHERIT` with `DIRECTORY_INHERIT`),
  inherit-only entries, and the informational `INHERITED` flag. Inherit-only
  entries feed only the default ACL.
- Deny entries whose effect POSIX can express, such as a named user denied
  write ahead of an `everyone@` grant, an `owner@` deny ahead of an
  `everyone@` grant, or an `everyone@` deny after the grants.

### Patterns that are refused

Each refusal names one rule. The inventory and the server log print the rule.

| Rule | Cause |
| --- | --- |
| `decision_mismatch` | Some caller class would get a different `r`, `w` or `x` decision. Typical causes: a deny for one group ahead of an allow for another (POSIX unions group entries), owner rights that depend on the owner's group membership, or a deny for a named user who could own a future child. |
| `file_or_directory_only_inheritance` | An entry inherits to files only or to directories only. POSIX defaults apply to both. |
| `no_propagate_inheritance` | `NO_PROPAGATE_INHERIT`. POSIX defaults always propagate. |
| `partial_write_rights` | `WRITE_DATA` without `APPEND_DATA`, or the reverse; on a directory, `ADD_FILE` without `ADD_SUBDIRECTORY`. |
| `data_right_without_named_attribute_right` | Read data without `READ_NAMED_ATTRS`, or write data without `WRITE_NAMED_ATTRS`. |
| `write_without_write_attributes` | Write data without `WRITE_ATTRIBUTES`. |
| `directory_write_without_delete_child` | Directory write without `DELETE_CHILD` and without an inheritable `DELETE` for the same caller. |
| `unsupported_ace_type` | Audit or alarm entries. |
| `unknown_ace_flags`, `unknown_access_bits`, `malformed_xdr` | Bits or layout outside OpenZFS's NFSv4.1 encoding. |
| `too_many_entries`, `too_many_named_principals` | More than 128 entries, 8 named users or 6 named groups. These bound the enumeration. |

### Narrowings that are reported

Some NFSv4 rights have no per-file POSIX equivalent. Translation drops them
and reports each one per file in the `narrowed` list.

- `acl_or_owner_change_limited_to_owner`: `WRITE_ACL` or `WRITE_OWNER` granted
  to someone other than the owner. In POSIX only the owner or root may change
  an ACL or mode, and only root may change the owner.
- `attribute_write_without_data_write_dropped`: `WRITE_NAMED_ATTRS` or
  `WRITE_ATTRIBUTES` without data write. POSIX ties both to `w`.
- `delete_follows_parent_directory_write`: `DELETE` on the object. POSIX
  decides deletion from the parent directory's `w` and `x` and the sticky bit.
- `child_deletion_relies_on_inherited_delete`: directory write was accepted
  without `DELETE_CHILD` because the same caller inherits `DELETE` on new
  children. TrueNAS `MODIFY` produces this. POSIX lets that caller delete every
  child, including an existing child whose own ACL denied the caller `DELETE`.
  This is the one case where translation can be looser, and it is reported for
  every such directory.

### What translation does not keep

- The NFSv4 bytes, the ACL flags (`auto-inherit`, `protected`, `defaulted`)
  and per-entry `INHERITED` flags. These steer Windows-side propagation, not
  access.
- `aclmode=restricted`. On ZFS a `chmod` of a file with a non-trivial ACL fails.
  On ZeroFS the owner may `chmod`, which rewrites the mask and other entry.
- `aclinherit=passthrough`. ZFS ignores the creating mode for inherited entries.
  POSIX limits an inherited ACL by the creating mode, so new files can be
  narrower than on the source. Clients that mask the creating mode also
  narrow the inherited ACL.
- `READ_ATTRIBUTES`, `READ_ACL` and `SYNCHRONIZE`. Any caller who can reach an
  inode can `stat` it and read its ACL, as on Linux. Samba enforces these rights
  for SMB clients on the source, so SMB access through ZeroFS is looser here.

## What is refused outside ACLs

`system.*` names other than `system.posix_acl_access`,
`system.posix_acl_default` and `system.nfs4_acl_xdr` return `EOPNOTSUPP`, as do
unknown namespaces. `docs/extended-attributes.md` covers the other namespaces.

## Checking a source tree

`zerofs inventory` probes `system.posix_acl_access`, `system.posix_acl_default`
and `system.nfs4_acl_xdr` on each entry. For every observed NFSv4 ACL the
entry's ACL record gains a `posix_translation` object, produced by the same
code the server runs:

```json
{"status": "accepted", "access": "u::rwx,g::rwx,o::---", "default": "u::rwx,g::rwx,o::---", "narrowed": ["child_deletion_relies_on_inherited_delete"]}
{"status": "refused", "rule": "file_or_directory_only_inheritance"}
```

The summary record counts entries per ACL format in `acl_formats` and reports
`nfs4_acl_translations_accepted` and `nfs4_acl_translations_refused`. When any
entry is refused, the command also prints a warning to standard error. Linux
exposes `system.nfs4_acl_xdr` only on the ZFS host itself, so a run over an SMB
or NFS client mount reports no NFSv4 ACLs even when the dataset has them. An
inventory of the production share needs the parent's authorization.

## Storage and compatibility

ZeroFS keeps modes, ownership, extended attributes and ACLs only in its own
metadata store, as rows committed in the same transaction as the inode. It
never reads or writes xattrs or ACLs of the filesystem or device that holds
its data, so the contract holds unchanged on a raw block backend that has no
filesystem at all. The only component that reads host xattrs and ACLs is
`zerofs inventory`, and it reads them from the migration source tree.

ACL rows use the extended-attribute keys under the inode, so hardlinks share
them and inode deletion removes them. The first stored ACL also writes a marker
row under inode id `u64::MAX`, which no file can have. A writer without that
marker skips ACL lookups; a read-only process always looks them up.

A ZeroFS binary from before this change ignores ACL rows and checks mode bits
only. Because the mode's group bits hold the mask, such a binary gives a named
user or named group the group or other permissions instead of their entry.
That can loosen access. Do not run an older binary against a volume that holds
ACLs.
