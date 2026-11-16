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
Once a mutation has passed its checks and built its transaction, a spawned task
holds its metadata fence and inode lock through apply. Cancelling the caller at
that point does not cancel the commit. A later unlink takes the same inode lock,
so its attribute scan includes the applied mutation.

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
FUSE read probes the required length.

| Namespace | Read and list visibility | Set and remove |
| --- | --- | --- |
| `user.*` | The caller needs read permission on the inode. | The caller needs write permission on the inode. |
| `trusted.*` | The caller must have uid 0. | The caller must have uid 0. |
| `security.*` | Any caller may read or list these attributes. | The caller must have uid 0. |
| `system.*` | ZeroFS returns `EOPNOTSUPP`. | ZeroFS returns `EOPNOTSUPP`. |

Only regular files and directories may carry `user.*` attributes. Access to that
namespace on symlinks, FIFOs, sockets, or device nodes returns `EPERM`, including
for uid 0. Unknown namespace prefixes return `EOPNOTSUPP`. POSIX ACL enforcement
is not implemented by storing these attributes.

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

Linux removes an attribute with `Txattrcreate` using size zero and `XATTR_REPLACE`,
then clunks the fid. The server follows that convention. Consequently, size zero
with `XATTR_REPLACE` removes rather than stores an empty value over 9P. Flags zero
or `XATTR_CREATE` can store an empty value. Linux v6.18 implements the sequence in
[`v9fs_fid_xattr_set`](https://git.kernel.org/pub/scm/linux/kernel/git/torvalds/linux.git/tree/fs/9p/xattr.c?h=v6.18).
[`__vfs_removexattr`](https://git.kernel.org/pub/scm/linux/kernel/git/torvalds/linux.git/tree/fs/xattr.c?h=v6.18)
passes a NULL value, size zero, and `XATTR_REPLACE` to the filesystem handler.

Xattr set and remove are not deduplicated across HA failover. `Tclunk` has no
operation id. Retrying a successful create-only set may return `EEXIST`; retrying a
successful removal may return `ENODATA`. The `ninep-client` helpers keep each
exchange on one connection and abort if it is lost. A lost clunk response can leave
the caller uncertain whether the mutation committed. Read back the attribute to
resolve that uncertainty. A completed mutation is covered by a later filesystem
fsync cutoff under the configured durability contract.

`zerofs mount` supports `getxattr`, `listxattr`, `setxattr`, and `removexattr` through
`ninep-client`. Linux's own xattr limits apply before a request reaches ZeroFS.
The 2 MiB core and 9P limit is not a claim that Linux FUSE accepts that size.
NFSv3 and the separate Rust kernel client have no xattr API in this change.
