# Source metadata inventory

`zerofs inventory <root>` inspects a directory without a server or configuration.
It writes JSON Lines to standard output. Redirect the output outside the source
tree. The command has no output-file option.

```sh
zerofs inventory /path/to/owned/source > /path/outside/source/inventory.jsonl
zerofs inventory /path/to/owned/source --hash sha256 > /path/outside/source/hashed.jsonl
zerofs inventory /path/to/owned/source --max-entries-per-second 100 > /path/outside/source/paced.jsonl
```

The collector opens the root and child directories without following their final
symlink components. It resolves children relative to directory descriptors,
sorts names by raw bytes, and checks opened entries against their preceding
`statat` observations. It does not follow symlinks or descend across device
boundaries. Linux also compares statx mount IDs to reject bind mounts within the
same filesystem, including bind mounts of individual files. If the kernel omits
`STATX_MNT_ID` or the query fails, `mount_id_unavailable` makes the entry
incomplete and the collector skips its reads and descendants. macOS detects
different-device mounts through `st_dev`. It cannot identify a mount that keeps
the same device number. The command is available only on Linux and macOS.

Linux pins a candidate regular file with `O_PATH` before opening it for I/O,
checks the pinned type, identity, metadata version and mount ID, then opens the
retained inode through `/proc/self/fd`. A FIFO, symlink or device swapped in
after the initial stat cannot be opened for data; a swap after pinning cannot
redirect the descriptor reopen. Missing proc descriptor access makes collection
incomplete. macOS has no corresponding pin in this collector: it uses a
no-follow nonblocking open and rejects changed metadata afterward. That detects
a replacement, but does not guarantee that a concurrently substituted device
was never opened. Use a stable source or read-only snapshot for macOS collection.

The command does not change modes, ownership, timestamps, ACLs, or xattrs, and it
does not create files under the source. Reading directory entries or hashing
contents can update atime under the mount's policy. Linux opens files and
directories with `O_NOATIME`, then retries without it on `EPERM`. The initial
header's `noatime_fallback` reports the root open. The final `header_update`
record replaces that value with the result for all opens, including directory
scans. `noatime_requested` is false on macOS, which has no such open flag.
Linux `readlinkat` can update symlink atime even when file and directory opens
use `O_NOATIME`. Only a read-only mount prevents all these updates. Recorded
atime comes from the stat before opening each entry. The inventory is not an atomic filesystem snapshot. Use a stable source
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
list means that the API reported no values. Each xattr list also has a
`trusted_namespace` field. Linux can return an observed list while that field
reports `trusted_namespace_not_visible`. The list then contains only the names
the process could see, and the inventory is incomplete. A failed directory
listing produces a `directory_unreadable` record. It does not establish an empty directory.

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

Linux probes trusted xattr visibility once per run. The header records the
result in `trusted_xattr_namespace`. Visibility requires effective
`CAP_SYS_ADMIN`, bit 21 of `/proc/self/status`'s `CapEff`, in the initial user
namespace. The collector recognizes the initial namespace by Linux's reserved
`/proc/self/ns/user` inode number, 4026531837. Child user namespaces, unrecognized
namespace IDs, and failed proc reads count as not visible even if `CapEff`
contains the bit. This conservative rule can report an incomplete inventory on
a kernel that uses another initial namespace ID. macOS has no `trusted.*`
namespace, which the header states explicitly.

Linux reads symlink, FIFO, socket, and device xattrs with `llistxattr` and
`lgetxattr` through `/proc/self/fd/<parent fd>/<name>`. The path stays anchored to the opened parent
and the calls do not follow the final symlink. The collector checks the entry
before and after these reads, including its mount ID. On Linux it never opens
FIFO, socket, or device nodes for I/O to collect their metadata. A missing or inaccessible
proc fd anchor produces `proc_fd_unavailable` and makes the inventory incomplete. POSIX ACLs are
inapplicable to Linux symlinks. The collector still probes their NFSv4 XDR ACLs.
Other special nodes receive all three Linux ACL probes. macOS FIFO, socket,
and device xattrs and ACLs use `not_implemented_on_platform`, which makes the
inventory incomplete. macOS path APIs cannot use `/dev/fd/<parent>/<name>` as
a directory anchor on the tested host. The collector does not decode ACLs.
The decoded field remains unavailable with the reason `not_decoded`.

Hashing is off by default. `--hash sha256` reads regular file contents and
records their SHA-256 values. The collector rechecks regular file metadata
after reading it. A mismatch produces an `entry_changed` record and invalidates
the fields read through the descriptor. The original stat fields remain the
pre-open observations.

Schema 3 includes the shared OpenZFS name rule. Like schema 2, it emits mount
boundaries, changed entries, unreadable directories, and
name collisions during traversal. A directory's collision records precede its
children. Hardlink groups follow traversal and precede the final header update,
summary, and digest. Hardlink groups contain the paths found in the tree and the observed link count. Links outside the tree equal that
count minus the paths found only when the traversal completed and the link
observations agree. Otherwise, the outside count remains unavailable.

The `openzfs-insensitive-unicode-5.0.0` collision rule uses
`fs::name_mode::fold_name`, the same function as ZeroFS insensitive name lookup.
It matches the declared source rule (`casesensitivity=insensitive`,
`normalization=none`, `utf8only=off`): Unicode 5.0.0 simple uppercase with no
normalization, preserving invalid multi-byte spans exactly as the shared fold
does. It keeps `ß` distinct from `SS` and composed accents distinct from their
decomposed forms, but detects `ı`/`I` and collisions containing invalid UTF-8.
This is source-code equivalence, not evidence from a running OpenZFS dataset.

Two additional diagnostic rules operate on UTF-8 names. `unicode-canonical`
compares NFC forms with `icu_normalizer`. `unicode-case-fold` compares the full
Unicode case fold of the NFC forms with `unicase` 2.10 or later. Version 2.9
reordered three-character folds and failed the ligature regression test.
Those two Unicode rules are advisory and do not establish a collision under the
source dataset's rule or Samba's matching behavior. Non-UTF-8 names retain their
exact bytes, participate in the OpenZFS rule only, and increment
`non_utf8_names`. Use the rule field, not the presence of any collision record,
when assessing a transfer to an insensitive ZeroFS filesystem.

The summary counts unavailable fields and diagnostic reasons. Intentional
omissions for an entry kind, fields inapplicable on the platform, an unrequested
hash, undecoded ACLs, and definite unsupported metadata formats do not make the
inventory incomplete. Collection failures, unsupported applicable APIs, changed
entries, unreadable directories, and skipped device boundaries do.
The command exits with code 0 for a complete inventory, code 3 for an incomplete
inventory, and another nonzero code for usage, root, or output errors.

## Load and memory

`--max-entries-per-second <n>` accepts a positive integer and defaults to
unlimited. The collector sleeps between entry starts when the previous entry
took less than the requested interval. It does not limit hashing bytes or pace
the directory listing calls. There is no resume support. An interrupted run
requires a new traversal.

The collector holds a directory's raw names in memory while it sorts them and
visits its children. Active ancestor directories retain their remaining names.
Collision detection also needs per-directory Unicode grouping storage. Ordinary
single-link files do not enter the hardlink map. Each distinct non-directory
inode with `nlink > 1` needs a 16-byte identity key and a 40-byte group value on a
64-bit target, plus B-tree and allocator overhead. Each stored path needs a
24-byte vector descriptor plus its raw path bytes and spare capacity. The path
list itself can have spare capacity. Memory therefore grows with the number of
hardlinked inodes and the total bytes of their paths. Diagnostic records do not
accumulate for the whole tree.

The last line contains the SHA-256 of every preceding output byte, including
each newline. It excludes its own line. Output order and serialization are
deterministic for the same metadata observations and collector build. A source
whose atime changes between runs can produce different output.

The macOS collector's tests were last run on macOS before this serialization
change and are not rerun on the host.

The Linux bind-mount and special-device fixture is explicitly ignored by normal
unit-test runs. The Rust workflow runs it separately with `--ignored --exact`,
the opt-in environment variable, and mount privileges in a private mount
namespace. The step builds as the normal user and elevates only the fixture
binary, retaining `inventory-mounts.log`. Omitting the opt-in when explicitly
running the test is an error, not a successful no-op.

- Not verified: TrueNAS and ZFS collection and ACL exposure require runs on those
  platforms. Local fixtures do not establish which ACLs or xattrs a NAS carries,
  its space usage, or Samba's name matching behavior.
