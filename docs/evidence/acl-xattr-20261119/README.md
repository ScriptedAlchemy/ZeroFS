# ACL and xattr receipts, November 19, 2026 UTC

Source: branch `feat/xattr-replace-and-acl-contract`, rebased onto
`integration/all-open-prs` at `8509886dfd61e077c186afec767eaa3f2d643a4f`. The commit that carries this
receipt contains the tested tree. The suites below were run again after the
rebase; the counts are from that run.

## Where the tests ran

Unless a section names another host, builds and tests ran in Docker on Zack's
Mac Studio. Docker context
`desktop-linux` (Docker Desktop 29.4.1). The host is arm64 and the images were
built and run as native `linux/arm64`, so no emulation was involved. The Docker
VM kernel is `6.12.76-linuxkit`, with `CONFIG_9P_FS=y`, `CONFIG_NET_9P_FD=y`
and `CONFIG_FUSE_FS=y` built in. No module was loaded. The toolchain image was
`rust:1.100.0-bookworm` with cmake, protobuf, fuse3, acl and attr added.
Builds used `CARGO_BUILD_JOBS=4`, with debuginfo and incremental compilation
off: two earlier attempts were killed for memory in the shared 15.35 GiB VM.

## Unit and wire tests

Run in `zerofs/`:

```sh
cargo fmt -- --check                                     # clean
cargo clippy --locked --workspace --all-targets -- -D warnings   # clean
cargo test --locked -p zerofs --lib -- fs::permissions fs::ops::xattr zerofs_client_tests ninep posix_tests
# test result: ok. 356 passed; 0 failed; 1 ignored
```

That run includes the plain-session wire tests
`xattr_wire_plain_dialect_carries_linux_v9fs_acl_frames` and
`xattr_wire_plain_dialect_empty_replace_frames_remove`.

The inventory tests check directory permissions, which root bypasses, so they
ran as uid 1000 from the built test binary:

```sh
setpriv --reuid=1000 --regid=1000 --clear-groups /target/debug/build/zerofs/<hash>/out/zerofs-<hash> cli::inventory
# test result: ok. 40 passed; 0 failed; 1 ignored
```

## Mounted matrix

[mount-matrix.jsonl](mount-matrix.jsonl) is the output of
`scripts/acl_xattr_mount_e2e.py`, at the revision before the kernel 9P
refusal and NFSv4 write checks were added, in a disposable container with
`CAP_SYS_ADMIN`, `/dev/fuse` and no network. The script starts a debug
`zerofs` server on a file store inside the container, then mounts it two ways.
The kernel 9P mount uses the app's package options
`trans=unix,version=9p2000.L,msize=1048576,cache=none,access=user,nodev,nosuid`.
The FUSE mount is `zerofs mount --access all --relaxed-consistency false`. The
script restarts the server midway and removes its fixture at the end. All 65
checks passed.

### Extended attributes

| Operation | Kernel 9P | FUSE |
| --- | --- | --- |
| Create, then create again | ok, then `EEXIST` | ok, then `EEXIST` |
| Replace an existing attribute | ok, value read back | ok, value read back |
| Replace a missing attribute | `ENODATA` | `ENODATA` |
| Empty value with flags 0, or with `XATTR_CREATE` | stored, reads back empty | stored, reads back empty |
| Empty value with `XATTR_REPLACE` | returns ok, but the attribute is removed (`ENODATA`, not listed) | stored, reads back empty, listed |
| Remove, then remove again | ok, then `ENODATA` | ok, then `ENODATA` |

The kernel 9P empty-replace result is the same before and after this change.
Linux v9fs sends one identical `Txattrcreate` (size 0, flags `XATTR_REPLACE`,
no `Twrite`) for an empty replacement and for `removexattr`. The
[PR 84 wire receipt](../pr84-acceptance-20261118/README.md) records the bytes.
The server keeps removal, because the other choice would break `removexattr`
for every kernel 9P user. The matrix now asserts that behaviour, so a change
would show. A server change cannot make the kernel client pass the app's empty
`XATTR_REPLACE` check. The release gate has to treat this case as a documented
kernel-client limitation or test it through FUSE. That decision belongs to the
app owner.

### ACLs

These checks ran through FUSE, which negotiates `FUSE_POSIX_ACL`:

- `setfacl` and `getfacl` round trips, with the mode's group bits holding the
  mask.
- Access decisions for the owner, a named user, a named group, the owning group
  and other.
- A `chmod` that rewrites the mask.
- A refused `setfacl` by a non-owner.
- Default ACL inheritance for files and directories.
- A named user creating a file through the access ACL.
- Both ACLs, and their enforcement, surviving a server restart.

One result differs from a local Linux filesystem, and the contract documents
it. A directory created with umask 022 under a default ACL got mask `r-x`,
because the kernel applied the umask before the request reached the server.

The kernel 9P client in this VM is built without `CONFIG_9P_FS_POSIX_ACL`, so
`setfacl` fails inside the client and the kernel 9P `setfacl` suite was skipped
with that reason recorded. Cross-client checks ran instead. On an ACL set
through FUSE and used through kernel 9P, the kernel checks mode bits locally.
The owner can read, other is denied, and a named user is denied by the local
mode check. A named-user deny that the mode bits would allow is enforced by the
server. The server also denies other after the restart. The wire test
`xattr_wire_plain_dialect_carries_linux_v9fs_acl_frames` sends the frames that
`fs/9p/acl.c` sends for `setfacl`, `getfacl` and `setfacl -b` when a mount is
not `access=client`, and checks storage, enforcement and removal.

## Kernel 9P with POSIX ACL support, x86-64

Host `ubuntu-main`, x86_64, kernel `7.0.0-28-generic`, read at
2026-11-19T06:15:25Z. Its config has `CONFIG_9P_FS_POSIX_ACL=y` and builds
`9p`, `9pnet` and `9pnet_fd` as modules, which loaded only through the mount.
The debug `zerofs` was built natively with `cargo build --locked -p zerofs
--bin zerofs` in a container from `rust:1.100.0-bookworm`. The matrix ran in a
disposable container with `--network none`, `CAP_SYS_ADMIN` through the docker
group (no sudo), `/dev/fuse`, and the host's `/boot/config-7.0.0-28-generic`
bind-mounted read-only. The output is
[mount-matrix-ubuntu-main-x86_64.jsonl](mount-matrix-ubuntu-main-x86_64.jsonl).
All 74 checks passed.

The run answered the open question from the Studio run. With the app's
`access=user` options, a kernel that has `CONFIG_9P_FS_POSIX_ACL=y` still
refuses ACL calls on the mount:

- `setfacl` fails with "Operation not supported".
- `getxattr("system.posix_acl_access")` fails with errno 95.
- `getfacl` prints an ACL built from the mode.
- A write of `system.nfs4_acl_xdr` fails with errno 95.

The cause, from Linux v9fs: it marks a superblock as ACL-capable only for
`access=client,posixacl`, and the kernel's ACL calls require that mark. This
receipt did not capture the wire, so it does not show whether any frame left
the client. The contract now says ACLs, including NFSv4 translation, must be
written through the FUSE client.

The same run repeated the FUSE ACL suite, the restart check, and the
cross-client checks on x86-64. It also wrote a TrueNAS `NFS4_RESTRICTED`
preset (`owner@` full control, `group@` modify, both inherited by files and
directories) to `system.nfs4_acl_xdr` through FUSE. ZeroFS translated it to
`user::rwx,group::rwx,other::---` with the same default ACL. A member of the
owning group could create a file, and other could not list the directory.

## Open items

- Not verified: the inventory against an OpenZFS dataset with
  `acltype=nfsv4`. The NFSv4 translation was tested with byte layouts encoded
  from OpenZFS `zpl_xattr.c` (truenas/zfs `truenas/zfs-2.4-release`), not with
  bytes read from a real dataset. Inventorying the production share needs the
  parent's authorization.

## NAS

This lane made no NAS reads and no NAS changes. The dataset properties quoted
in the contract were read by another lane at 2026-11-19T02:52:50Z.
