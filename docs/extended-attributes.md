# Extended attributes

ZeroFS stores each extended attribute under `meta | 0x0A | inode id | name`.
The inode id is an eight-byte big-endian integer. The name and value retain their
original bytes. Attribute names do not follow the volume's case-insensitive
filename rule. Hardlinks share attributes because the key contains the inode id.

Inode serialization and the persisted bloom filter extractor remain unchanged.
Exact attribute reads use the whole-key filter. Listing scans the inode's attribute
prefix. Attribute mutations update ctime, the inode, and its directory-listing row
in the same transaction. Deleting the final inode also deletes its attribute rows
in that transaction. An open-unlinked inode retains its attributes until reclaim.
Once a mutation has passed its checks and built its transaction, the caller submits
it to the commit queue before spawning a task. That task holds the metadata fence
and inode lock through apply. Cancelling the caller at that point does not cancel
the commit. A later unlink takes the same inode lock,
so its attribute scan includes the applied mutation. A successful set can return
before durability unless synchronous writes are enabled.

Older binaries ignore attribute rows. An older binary that deletes an inode leaves
its attribute rows behind. Inode ids are not reused, so those rows consume space
without becoming attributes of a later file. Run a binary with extended-attribute
support when deleting files that carry attributes.

## Names, values, and permissions

Names must contain between one and 255 bytes and must not contain NUL. A namespace
prefix must have a nonempty suffix. Values may be empty and may contain arbitrary
bytes. The core and 9P server accept values up to 2,097,152 bytes. A larger value
returns `E2BIG`. Reading a missing attribute or removing an absent attribute returns
`ENODATA`. A nonzero read buffer that is too small returns `ERANGE`. A zero-size
FUSE read probes the required length. Each inode may carry at most 4,096 attributes.
Adding a new name at that cap returns `ENOSPC`; replacing or deleting an existing
name still works. Listing stops at the cap. Inode deletion streams the attribute
rows directly and carries only their keys in the same transaction as deletion.
It does not apply the listing cap, so excess rows from an earlier development
build cannot block unlink or open-inode reclaim. Newly admitted writes remain
bounded by the count cap.

| Namespace | Read and list visibility | Set and remove |
| --- | --- | --- |
| `user.*` | The caller needs read permission on the inode. | The caller needs write permission on the inode. |
| `trusted.*` | The caller must have uid 0. | The caller must have uid 0. |
| `security.capability` | Reads return `ENODATA` and listings omit the name, including for pre-existing values. | Sets return `EOPNOTSUPP`. The caller must have uid 0 to remove a stored value. |
| Other `security.*` | Any caller may read or list these attributes. | The caller must have uid 0. |
| `system.posix_acl_access`, `system.posix_acl_default` | Any caller may read or list these. Reads of the access ACL take the owner, mask and other entries from the current mode. | The caller must be the owner or uid 0. The value is validated and enforced. |
| `system.nfs4_acl_xdr` | Reads return `ENODATA`; ZeroFS does not store the value. | The caller must be the owner or uid 0. ZeroFS translates the value to POSIX ACLs or returns `EOPNOTSUPP`. |
| Other `system.*` | ZeroFS returns `EOPNOTSUPP`. | ZeroFS returns `EOPNOTSUPP`. |

Only regular files and directories may carry `user.*` attributes. Access to that
namespace on symlinks, FIFOs, sockets, or device nodes returns `EPERM`, including
for uid 0. On a directory with the sticky bit, only the owner or uid 0 may set or
remove `user.*` attributes. Unknown namespace prefixes return `EOPNOTSUPP`.
[permissions-and-acl-contract.md](permissions-and-acl-contract.md) describes
ACL storage, enforcement, inheritance and NFSv4 translation. ACL writes ignore
`XATTR_CREATE` and `XATTR_REPLACE`, as Linux does.

A set with flags zero creates or replaces an attribute. `XATTR_CREATE` requires the
attribute to be absent and returns `EEXIST` if it exists. `XATTR_REPLACE` requires it
to exist and returns `ENODATA` otherwise. The server checks the request's numeric
credentials. 9P clients can assert those identities, so restrict access to trusted
clients and keep uid and gid assignments consistent.

## 9P and FUSE

The server implements standard 9P2000.L `Txattrwalk` and `Txattrcreate`. A walk
returns a snapshot readable with `Tread`. Walking an empty attribute name returns
the visible names in byte order, each followed by NUL. A create converts an
unopened fid into an attribute-write fid. `Twrite` accumulates bytes within the
declared size. `Tclunk` commits only if every declared byte was written. An
incomplete value returns `EINVAL` and changes nothing. Clunk may also return a
namespace, permission, or set-mode error. Disconnect discards values that have
not reached the clunk commit. A commit already submitted by clunk may finish
after disconnect.

Plain `9P2000.L` sessions follow Linux v9fs removal encoding. A `Txattrcreate`
with size zero and `XATTR_REPLACE`, followed by clunk, removes the attribute.
The ZeroFS dialects `9P2000.L.Z` and `9P2000.L.Z.CI` instead use the private
`Txattrcreate.flags` bit `0x80000000` for removal. That flag requires size zero
and no other flags. Plain sessions reject it with `EINVAL`. On ZeroFS sessions,
size zero with `XATTR_REPLACE` stores an empty value in an existing attribute.
The ZeroFS FUSE client uses the private removal flag. Linux v6.18 implements its
plain-session sequence in
[`v9fs_fid_xattr_set`](https://git.kernel.org/pub/scm/linux/kernel/git/torvalds/linux.git/tree/fs/9p/xattr.c?h=v6.18).

Linux kernel 9P cannot distinguish an empty `XATTR_REPLACE` from removal:
both encode size zero and flags `XATTR_REPLACE`, with no value write. Therefore
empty replacement through that client removes the attribute. A server cannot
preserve both operations from the identical request. The
[Linux 7.0 wire receipt](evidence/pr84-acceptance-20261118/README.md)
records the exact bytes and source path. The
[mounted matrix receipt](evidence/acl-xattr-20261119/README.md) repeats the
result on a real kernel 9P mount with the app's options: the empty replacement
returns success and the attribute is then absent. FUSE stores the empty value.
No server change can satisfy an empty-replacement check through the kernel
client. A release gate that requires it on kernel 9P tests an incompatible
requirement; test that case through FUSE instead.

ZeroFS does not deduplicate xattr set and remove across HA failover. `Tclunk` has no
operation id. Retrying a successful create-only set may return `EEXIST`; retrying a
successful removal may return `ENODATA`. The `ninep-client` helpers keep each
exchange on one connection and abort if it is lost. A lost clunk response can leave
the caller uncertain whether the mutation committed. Read back the attribute to
resolve that uncertainty. Under the configured durability contract, a later
filesystem fsync cutoff covers a completed mutation.

`zerofs mount` supports `getxattr`, `listxattr`, `setxattr`, and `removexattr` through
`ninep-client`. Linux caps an attribute value at 65,536 bytes before a request
reaches ZeroFS, even though the core and 9P store accept 2,097,152 bytes.
NFSv3 and the separate Rust kernel client have no xattr API in this change.
The FUSE client negotiates `FUSE_POSIX_ACL`, so the kernel reads and evaluates
`system.posix_acl_*` through these calls.

Client and server must both include extended-attribute support. A new client
connected to a server from `main` receives `EOPNOTSUPP` for get and list. That
server recognizes `Txattrwalk` but rejects it. Set and remove receive `ENOSYS`
because its decoder does not recognize `Txattrcreate`. No attribute changes in
either case. Earlier development encodings from this unmerged change are not
supported peers.
