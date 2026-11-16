# Source metadata inventory

`zerofs inventory <root>` inspects a directory without a server or configuration.
It writes JSON Lines to standard output. Redirect the output outside the source
tree. The command has no output-file option.

```sh
zerofs inventory /path/to/owned/source > /path/outside/source/inventory.jsonl
zerofs inventory /path/to/owned/source --hash sha256 > /path/outside/source/hashed.jsonl
```

The collector opens the root and child directories without following their final
symlink components. It resolves children relative to directory descriptors,
sorts names by raw bytes, and checks opened entries against their preceding
`statat` observations. It does not follow symlinks or descend across device
boundaries. FIFOs, sockets, and devices receive stat metadata only.

The command does not change modes, ownership, timestamps, ACLs, or xattrs, and it
does not create files under the source. Reading directory entries or hashing
contents can update atime under the mount's policy. Only a read-only mount
prevents these updates. Recorded atime comes from the stat before opening each
entry. The inventory is not an atomic filesystem snapshot. Use a stable source
or a filesystem snapshot when concurrent changes matter.

## Records and completeness

The header records the platform, tool version, root identity, filesystem type,
known network filesystem status, collision rules, and hash request. A network
mount exposes the client's metadata mapping. Its inventory does not establish
the server's native ownership or ACLs. Linux reports the filesystem magic even
when the collector has no name for it. The network flag recognizes known network
filesystem types. It cannot identify every network-backed FUSE filesystem.

Entry paths, symlink targets, xattr names and values, and ACL bytes have a `hex`
field and a separate lossy `display` string. Hex is authoritative. Paths are
relative to the root. The root entry has an empty path.

Every metadata field has either `status: "observed"` with a `value`, or
`status: "unavailable"` with a `reason` and nullable `errno`. An observed empty
list means that the API reported no values. A failed listing produces a
`directory_unreadable` record. It does not establish an empty directory.

Entries include the kind, device and inode, link count, uid and gid, full mode,
nanosecond access, modification and change times, birth time when available,
logical and allocated sizes, device number, symlink target, xattrs and ACLs.
macOS provides `st_flags`. Linux reads inode flags with `FS_IOC_GETFLAGS` on
opened files and directories. Allocated bytes come from `st_blocks * 512`.
Data extents come only from `SEEK_DATA` and `SEEK_HOLE`. Allocation below logical
size does not establish sparseness, since compression can also reduce allocation.
An unsupported extent query remains unavailable.

macOS ACL observations contain the raw bytes returned by `acl_to_text`, or null
when the descriptor ACL API reports an absent ACL or no entries. Linux probes
`system.posix_acl_access`,
`system.posix_acl_default`, and `system.nfs4_acl_xdr` explicitly, including when
the name does not appear in the xattr listing. Each raw ACL probe retains its
own status. Linux `ENODATA` means no ACL was reported. `EOPNOTSUPP` means that
the filesystem cannot contain that ACL format. This is a definite answer and
does not make the inventory incomplete. The same rule applies when the xattr
listing API reports that the filesystem does not support xattrs. Unsupported
inode flag queries also count as complete when they return `ENOTTY` or
`EOPNOTSUPP`. These answers use `format_not_supported_by_filesystem` and retain
the errno.

Linux reads symlink xattrs with `llistxattr` and `lgetxattr` through
`/proc/self/fd/<parent fd>/<name>`. The path stays anchored to the opened parent
and the calls do not follow the final symlink. The collector checks the entry
before and after these reads. A missing or inaccessible proc fd anchor produces
`proc_fd_unavailable` and makes the inventory incomplete. POSIX ACLs are
inapplicable to Linux symlinks. The collector still probes their NFSv4 XDR ACLs.
The collector does not decode ACLs. The decoded field remains unavailable with
the reason `not_decoded`.

Hashing is off by default. `--hash sha256` reads regular file contents and
records their SHA-256 values. The collector rechecks regular file metadata
after reading it. A mismatch produces an `entry_changed` record and invalidates
the fields read through the descriptor. The original stat fields remain the
pre-open observations.

Derived records follow the entries. Hardlink groups contain the paths found
in the tree and the observed link count. Links outside the tree equal that
count minus the paths found only when the traversal completed and the link
observations agree. Otherwise, the outside count remains unavailable.

Collision rules operate on UTF-8 names within each directory. `unicode-canonical`
compares NFC forms with `icu_normalizer`. `unicode-case-fold` compares the full
Unicode case fold of the NFC forms with `unicase` 2.10 or later. Version 2.9
reordered three-character folds and failed the ligature regression test.
These rules do not claim equivalence to Samba or ZFS. They do not include the
OpenZFS insensitive rule. That rule will use the shared fold function when the
case-insensitive naming work adds it. Non-UTF-8 names retain their exact bytes,
do not participate in Unicode rules, and increment `non_utf8_names`.

The summary counts unavailable fields and diagnostic reasons. Intentional
omissions for an entry kind, fields inapplicable on the platform, an unrequested
hash, undecoded ACLs, and definite unsupported metadata formats do not make the
inventory incomplete. Collection failures, unsupported applicable APIs, changed
entries, unreadable directories, and skipped device boundaries do.
The command exits with code 0 for a complete inventory, code 3 for an incomplete
inventory, and another nonzero code for usage, root, or output errors.

The last line contains the SHA-256 of every preceding output byte, including
each newline. It excludes its own line. Output order and serialization are
deterministic for the same metadata observations and collector build. A source
whose atime changes between runs can produce different output.

- Not verified: TrueNAS and ZFS collection and ACL exposure require runs on those
  platforms. Local fixtures do not establish which ACLs or xattrs a NAS carries,
  its space usage, or Samba's name matching behavior.
