# Source metadata inventory

`zerofs inventory <root>` inspects a directory without a server or configuration.
It writes JSON Lines to standard output. Redirect the output outside the source
tree. The command has no output-file option.

```sh
zerofs inventory /path/to/owned/source > /path/outside/source/inventory.jsonl
zerofs inventory /path/to/owned/source --hash sha256 > /path/outside/source/hashed.jsonl
zerofs inventory /path/to/owned/source --work-dir /path/to/scratch --max-entries-per-second 100 > /path/outside/source/paced.jsonl
zerofs inventory /path/to/owned/source --work-dir /path/to/scratch --resume-from /path/outside/source/partial.jsonl > /path/outside/source/resumed.jsonl
```

The collector opens the root and child directories without following their final
symlink components. It resolves children relative to directory descriptors,
sorts names by raw bytes, and checks opened entries against their preceding
`statat` observations. It does not follow symlinks or descend across device
boundaries. Linux also compares statx mount IDs to reject bind mounts within the
same filesystem, including bind mounts of individual files. If the kernel omits
`STATX_MNT_ID` or the query fails, `mount_id_unavailable` makes the entry
incomplete and the collector skips its reads and descendants. macOS reports a
mount only for an open descriptor. After `st_dev` matches, the collector opens
each directory, regular file and symlink, then compares its `fstatfs` filesystem
ID (`f_fsid`) and mount point (`f_mntonname`) with the root's before reading
through it. A difference is a `mount_boundary`, and a failed `fstatfs` reports
`mount_id_unavailable`. Either one skips the entry's reads and descendants. The
collector does not open FIFOs, sockets or devices. macOS mounts only on directories, so they
share the mount of the directory that lists them. The command is available only
on Linux and macOS.

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
known network filesystem status, collision rules, hash request, and pacing option. A network
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

Schema 5 emits mount boundaries, changed entries, unreadable directories, and
name collisions during traversal. A directory's collision records precede its
children. Each `hardlink_observation` records one non-directory path with more
than one link, its identity, observed link count, and whether its metadata stayed
stable. These records let resume rebuild the bounded hardlink sorter.
Hardlink groups follow traversal and precede the final header update, summary,
and digest. Links outside the tree equal the observed link count minus the paths
found only when traversal completed and all link observations agree. Otherwise,
the outside count remains unavailable.

The `openzfs-insensitive-unicode-5.0.0` collision rule uses the same `fold_name`
function as filesystem lookup. It applies the OpenZFS Unicode 5.0.0 simple
uppercase table without normalization. It keeps `ß` distinct from `SS` and
composed accents distinct from decomposed accents. It matches `ı` with `I`.
Invalid UTF-8 spans follow the shared OpenZFS byte rule. They participate in
collision detection, and each non-UTF-8 basename increments `non_utf8_names`.
This rule does not establish Samba's matching behavior.

Two additional diagnostic rules operate on UTF-8 names. `unicode-canonical`
compares NFC forms with `icu_normalizer`. `unicode-case-fold` compares the full
Unicode case fold of the NFC forms with `unicase` 2.10 or later. Version 2.9
reordered three-character folds and failed the ligature regression test.
Those two Unicode rules are advisory and do not establish a collision under the
source dataset's rule or Samba's matching behavior. Non-UTF-8 names retain their
exact bytes, participate in the OpenZFS rule only, and increment
`non_utf8_names`. Use the rule field, not the presence of any collision record,
when assessing a transfer to an insensitive ZeroFS filesystem.

Collision groups and hardlink groups contain at most 256 names or paths per
record. The zero-based `chunk` and `last_chunk` fields identify consecutive parts
of one group. Collision chunks share their directory, rule, and folded name key.
Hardlink chunks share their device and inode, and repeat the same link counts.
Consumers must combine those chunks before treating the path list as complete.

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
the directory listing calls.

Each sorter holds at most 4,096 rows and a 1 MiB byte budget. The budget includes
reserved raw byte capacity and the actual capacity of the row-vector allocation.
Each vector descriptor uses 24 bytes on a 64-bit target. One oversized row can
temporarily exceed the budget before it spills. Directory listing creates a raw-name sorter and a folded-key sorter.
The folded sorter holds keys for all three rules. The collector releases it before
visiting children. The hardlink sorter uses the same caps and stores observations instead of an inode-to-path map.
Large groups stream in chunks of 256 rows, so one inode cannot retain all its
paths in memory.

`--work-dir` names an existing scratch parent. It must resolve outside the source
root, including through symlinks. The collector creates a private temporary child
there only when a sorter exceeds a cap. Without this option, exceeding either
cap stops the walk with an error. Sorted runs merge two at a time in binary
levels, with at most one run per level. This limits merge buffers to two input
rows and three 8 KiB I/O buffers, plus file descriptors for logarithmically many
runs. The collector removes scratch files on success and on returned errors. A killed process
can leave its private scratch child for the operator to remove.

An active ancestor can retain up to 1 MiB of sorted raw names. Traversal stops
with an error beyond 128 active directory levels. Retained ancestor names
therefore use at most 128 MiB, plus the current sorters, the hardlink sorter,
path and checkpoint state, and temporary JSON serialization copies. With 255-byte
components, a path can reach about 32 KiB at that depth. A 256-path output chunk
can therefore hold about 8 MiB of raw paths before hex, display, and JSON copies.
These caps bound directory and hardlink storage. They do not cap an individual
entry's ACLs, xattrs, or extent list.

## Resume

`--resume-from <manifest>` reads an earlier partial manifest and writes a new
whole manifest to standard output. Redirect to a different file outside the
source. Never truncate the input manifest with shell redirection. The collector
checks the full header, including root identity, raw root path, collector platform and version,
capability visibility, hash request, and pacing option. The scratch location and
internal sorting cap may change because neither affects output order.

A flushed `directory_complete` record follows each finished directory collection
attempt, including an attempt that reported an unreadable directory. It contains
the SHA-256 of the preceding bytes, active ancestor identities and metadata
versions, each ancestor's last child cursor, and traversal state. Its separate
`state_sha256` protects the checkpoint state itself, including the prefix digest;
a prefix-only digest cannot detect a corrupted final cursor that skips remaining
entries. Resume validates both digests before copying any output, copies the
original bytes through the last complete
checkpoint, rebuilds summary counts and the hardlink sorter, and reopens active
ancestors without following symlinks. A changed ancestor or mount ID stops resume.
The collector continues after the saved child cursors without recollecting
completed subtrees. Resume discards records after that checkpoint, including a
torn final line. A manifest with no complete directory checkpoint cannot resume.
Schema 4 manifests lack the checkpoint-state digest, and the header version check
refuses them. Collect a new schema 5 manifest instead of resuming them.

The source must remain stable across the interruption. Resume retains earlier
observations and does not rescan completed subtrees to detect later changes.
It does not turn the walk into a snapshot. With the same metadata observations
and collector build, resumed output is byte for byte identical to uninterrupted
output. Its final digest includes the copied prefix and all new records.

The last line contains the SHA-256 of every preceding output byte, including
each newline. It excludes its own line. Output order and serialization are
deterministic for the same metadata observations and collector build. A source
whose atime changes between runs can produce different output.

Normal unit-test runs skip the Linux bind-mount and special-device fixture,
which is marked ignored. The Rust workflow runs it separately with `--ignored --exact`,
the opt-in environment variable, and mount privileges in a private mount
namespace. The step builds as the normal user and elevates only the fixture
binary, retaining `inventory-mounts.log`. Omitting the opt-in when explicitly
running the test is an error, not a successful no-op.

The dedicated source-inventory workflow also runs the ordinary inventory suite
and this exact ignored fixture in a private mount namespace.

- Not verified: TrueNAS and ZFS collection and ACL exposure require runs on those
  platforms. Local fixtures do not establish which ACLs or xattrs a NAS carries,
  its space usage, or Samba's name matching behavior.

- Not verified: The macOS collector's tests last ran on macOS before this
  serialization change. This change did not rerun them on the host.
