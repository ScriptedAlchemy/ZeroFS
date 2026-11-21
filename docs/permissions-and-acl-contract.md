# Permissions and ACL contract

This document states what ZeroFS enforces and what it refuses when it stores
ownership, mode bits and POSIX access control lists. ZeroFS never makes access
looser than a stored ACL says. When it cannot express an ACL, it refuses the
write.

## What is enforced

ZeroFS checks permissions on the server for every request from every protocol.
The checks follow Linux `generic_permission`.

- Mode bits, ownership, the sticky bit and setgid inheritance, as before this
  change.
- Create requests may explicitly choose the caller's uid and a group the
  caller belongs to. A setgid parent's inherited group is also allowed, even
  when the caller can create there through an ACL rather than group membership.
  Only root may assign a different owner or an otherwise unrelated group.
- Unprivileged writes, truncation (including a same-size truncate), and
  allocation/zeroing discard setuid and executable setgid bits, including when
  the caller owns the file. Root retains them, an empty write changes nothing,
  and setgid without group execute is preserved, matching Linux.
- POSIX access ACLs stored in `system.posix_acl_access`. A caller who is not
  root and not the owner is checked against the ACL: a matching named-user
  entry, else every matching group entry (owning group and named groups), else
  the other entry. Named users and all group-class entries are limited by the
  mask. For a combined request, one matching group entry must grant every
  requested bit; separate group entries cannot combine read with write or
  write with directory search. If groups match but none grants the request,
  access is denied without falling through to other.
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
(unknown tag, entries out of order, duplicate or undefined named ids, a named
entry without a mask, a permission bit above `rwx`) returns `EINVAL` and changes nothing. An
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

If ZeroFS itself truncates a supplied group list to its inline credential
capacity, it preserves that fact across internal credential conversions. The
lost groups never gain the kernel-authorized fallback: ambiguous mode checks
require both group and other permissions, ACL checks remain conservative, and
group changes or setgid retention require known membership.

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
  built from the mode. The caller always gets that error, so an ACL is never
  silently dropped. The kernel checks only mode bits locally, so a named user
  or named group receives no more than the mode's group or other bits on that
  mount. The server still enforces every stored ACL for requests from that
  mount, so restrictions always hold.
- NFSv3 clients cannot read or set ACLs. The server enforces stored ACLs on
  data operations and on the ACCESS RPC used by clients to authorize cached
  opens. ACCESS evaluates each requested NFS right independently, with one
  combined write/search check for directory mutation rights. Shared-identity
  mapping applies before the check. Returned attributes retain acknowledged
  RAM-write size and timestamps without forcing materialization.

## What is refused outside ACLs

`system.*` names other than `system.posix_acl_access` and
`system.posix_acl_default` return `EOPNOTSUPP`, as do
unknown namespaces. `docs/extended-attributes.md` covers the other namespaces.

## Checking a source tree

`zerofs inventory` probes `system.posix_acl_access` and
`system.posix_acl_default` on each entry. The summary record counts entries per
ACL format in `acl_formats`.

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

When an ACL can affect a permission check, its entries and the inode's mode and
ownership are read from one committed database view. A concurrent ACL change,
`chmod`, or read-only reader refresh cannot combine fields from different
permission states. Owner, root, symlink and no-ACL fast paths use the original
inode alone.

The evidence under `docs/evidence/acl-xattr-20261119/` describes its recorded
source revision. Changes to combined permission checks and coherent ACL reads
require fresh mounted qualification; those
historical receipts do not qualify the updated implementation.

A ZeroFS binary from before this change ignores ACL rows and checks mode bits
only. Because the mode's group bits hold the mask, such a binary gives a named
user or named group the group or other permissions instead of their entry.
That can loosen access. Do not run an older binary against a volume that holds
ACLs.
