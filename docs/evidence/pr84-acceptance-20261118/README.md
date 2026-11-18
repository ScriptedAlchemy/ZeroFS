# Engine acceptance receipt, November 18, 2026 UTC

The engine is **not accepted for the NAS trial**. Kernel 9P empty replacement
and immediate full-pool reuse remain unmet requirements. These receipts support
separate, narrower outcomes; they do not complete the placement design.

For the disposable NAS trial, Zack waived the extended SMART check on the
18 TB WD (`3WH7H3BJ`) on November 18. The deployment owner reported SMART
overall status PASSED, an extended self-test interrupted by host reset, and
no self-test running at the time of the read. Engine and mounted-protocol
qualification can proceed independently of that check.

## Source and executable

Native host: `ubuntu-main`, x86_64, kernel `7.0.0-28-generic`. Containers used
Ubuntu 26.04 for mounted probes and Rust 1.100.0/bookworm for the debug build.
Engine source: `9951d0f60feec3e2b8fe4dfbeef962000d6b594a`. The source tree used
for compilation also contained the appended test in `delete-reuse-red.patch`;
it is behind `cfg(test)` and does not enter the server executable. The production
Rust tree is unchanged by this evidence/fixture correction.

Executable SHA256:
`e34814d3319450e00b5c349df60481ec5ccc4aab31917a584cfec07c7461901c`.
This is a locally built diagnostic executable, not a published release artifact.
Build command, in the archived tree's `zerofs` directory:

```sh
CARGO_BUILD_JOBS=8 CARGO_TARGET_DIR=/target cargo build --locked -p zerofs --bin zerofs
```

The app coordinator receives the final pushed commit and downloaded source
archive hash separately. Neither this debug executable nor these tests deploys
or publishes an image. The app remains separately owned.

## Kernel 9P empty replacement

[kernel-9p-wire.json](kernel-9p-wire.json) captures client frames through a
transparent TCP proxy. The exact operations were:

```python
os.setxattr(target, 'user.probe', b'before')
os.setxattr(target, 'user.probe', b'', os.XATTR_REPLACE)
os.getxattr(target, 'user.probe')  # ENODATA
os.setxattr(target, 'user.probe', b'before')
os.removexattr(target, 'user.probe')
os.getxattr(target, 'user.probe')  # ENODATA
```

Mount command inside the isolated container:

```sh
mount -t 9p -o trans=tcp,port=5641,version=9p2000.L,cache=none,msize=1048576 127.0.0.1 "$fixture/mount"
```

Both operations sent the same complete 35-byte `Txattrcreate` frame:

```text
23000000200000030000000a00757365722e70726f6265000000000000000002000000
```

Decoded: length 35, type 32, tag 0, fid 3, name `user.probe`, size 0, flags 2
(`XATTR_REPLACE`). Neither zero-size phase sent `Twrite`. Both subsequent reads
returned errno 61 (`ENODATA`). A server has no information with which to choose
different results. Preserving removal leaves empty replacement incompatible
with the current kernel-client requirement.

Linux v7.0 source independently explains the bytes:

- [`fs/xattr.c`, `__vfs_removexattr`](https://kernel.googlesource.com/pub/scm/linux/kernel/git/torvalds/linux/+/refs/tags/v7.0/fs/xattr.c#504)
  calls the handler with `NULL, 0, XATTR_REPLACE`.
- [`fs/9p/xattr.c`, `v9fs_fid_xattr_set`](https://kernel.googlesource.com/pub/scm/linux/kernel/git/torvalds/linux/+/refs/tags/v7.0/fs/9p/xattr.c#108)
  sends `p9_client_xattrcreate(fid, name, value_len, flags)`, writes the iterator,
  then clunks. Pointer nullness never enters the protocol.

Downloaded decoded source SHA256s: `fs/xattr.c`
`2f3737b36b44f4773c872d89bbb03cfc47906beebc2b0ab21635e6b65fe71228`;
`fs/9p/xattr.c`
`f5c23351a0dcfabc52170c1529a007403bac8e43231695467988097540f4ee18`.
The running distribution kernel was tested on the wire; the tagged upstream
source is corroboration, not a claim to be its exact distribution build source.

The unchanged `scripts/test-xattrs-fuse.py` control passed both sensitive and
insensitive modes: empty replacement retained a listed zero-length attribute;
explicit removal returned ENODATA. The default client and server semantics were
not changed.

## Full-pool reuse and crash survival

[`delete-reuse-red.json`](delete-reuse-red.json) is a real-process FUSE receipt.
Two members declare 50,000,000 bytes each with 1,000,000-byte free-space reserves;
member B has less free space than its reserve. A closed, flushed 33,554,432-byte
target fills member A enough to block a second fully staged payload. One fsync
on the same pending descriptor immediately after unlink still returns ENOSPC,
in 0.00532 seconds including deletion. The assertion remains red.

The test then SIGKILLs the server before graceful flushing and reopens the
filesystem. The earlier flushed file retains SHA256
`b2b14cf1a69451337919814cc2c4311eed22b35402a4be4a8fcd5d1efb7c56ee`.
The old durable target returns after the unflushed deletion and retains SHA256
`aa27e56650d7a8c19a9a497a409d76299f0740809ff52f3e7570368a818b25be`.
No durability is inferred for replacement bytes whose fsync failed. This is
engine/FUSE evidence; the coordinator owns the independent SMB package test.

The in-process reproducer is preserved as `delete-reuse-red.patch`, not installed
as a skipped or weakened test. Applying it and running the following on Linux
fails the expected-success assertion:

```sh
cargo test --locked -p zerofs --lib pool_full_tests::deleting_a_closed_file_releases_space_for_the_same_pending_flush -- --exact --nocapture
```

Root cause: `fs/ops/remove.rs` tombstones a large final-link deletion;
`fs/gc.rs` later removes extent references. The durable reclaim barrier in
`fs/store/extent/reclaim.rs` calls `seal_open()` before database flush. The retained
replacement segment cannot seal under ENOSPC. Skipping sealing would persist
references to missing data: `cli/server.rs` explicitly disables the WAL and
automatic metadata flushing to prevent this. Independently, the 60-second
in-flight-reader floor and checkpoint pins prevent immediate physical deletion.
Removing the cycle alone would not satisfy the unchanged five-second contract.
No barrier, reserve, checkpoint, or assertion was weakened.

## Reconnect

[`reconnect.json`](reconnect.json) records ten SIGKILL/restart cycles, one FUSE
client process and one surviving read descriptor held across all ten cycles, and
eight concurrent descriptor closes after each outage bound fires. The first
read after restart matched the exact 1 MiB payload in all ten cycles, without
retrying that read. SHA256:
`bed9a73ec5bee8de1df800b5de43c79f44fc3a42b3ead3ee155ba56c624109ce`.
The client uses write-through/direct reads and a one-second outage bound.
These results qualify this reconnect scenario, not every protocol or HA fault.

## Reproduction and cleanup

From an isolated Linux container with Python, fuse3, attr, `/dev/fuse`, and
CAP_SYS_ADMIN, use an owned scratch/evidence directory. TCP binds are contained
in that container's network namespace. No ports are published.

```sh
python3 scripts/test-fuse-reconnect.py --binary "$binary" --scratch "$owned" --evidence "$owned/reconnect" --repetitions 10
python3 scripts/test-pool-delete-reuse.py --binary "$binary" --scratch "$owned" --evidence "$owned/reuse"
python3 scripts/probe-kernel-9p-xattr-wire.py --binary "$binary" --evidence "$owned/wire"
TMPDIR="$owned" python3 scripts/test-xattrs-fuse.py --binary "$binary"
```

The full-pool command intentionally exits 1 after crash/hash checks and cleanup
while the requirement remains unmet. The other three probes pass. Readiness
polling does not replace any data-path assertion. Disconnected FUSE mounts are
found through `/proc/self/mountinfo`, then explicitly unmounted; stat-based
`ismount` alone missed those mounts in the first cleanup attempt. The repeated
probes removed their fixtures, mounts, clients and servers. Two roots from that
earlier cleanup failure and the original wire fixture were removed separately
by exact path after their isolated container namespaces exited.

No NAS, app, host module, authentication, networking, service, production mount,
backup or snapshot changes were made. Retained native resources are source,
diagnostic executables and logs under
`/var/tmp/zerofs-pr84-acceptance-20261118`, plus the pre-existing shared
`zerofs-pr84-target`/`zerofs-pr84-cargo` build volumes and verifier roots.
[retained-resources.txt](retained-resources.txt) lists those retained roots and
volumes; historical verifier trees were preserved.
Persistent local receipts live under
`/Users/zackjackson/.t3/userdata/pstack/orch/ZeroFS/acceptance-20261118`.
No owned running container or live mount remains.

## CI fixture correction

Original failure: [native stress-ng job 134549664809](https://github.com/ScriptedAlchemy/ZeroFS/actions/runs/44223542113/job/134549664809)
on `9951d0f6`, November 18 03:26:08 UTC:

```text
metamix: read failure, errno=4 (Interrupted system call)
metamix: [14206] terminated with an error, exit status=2
```

Pinned stress-ng source `00559dcb1bae3ef12295a1420683094a7d1389e0` deliberately
sends SIGALRM to child workers after the operation count is reached. Its read
path failed on EINTR, whereas this repository's existing fixture patch already
handled interrupted opens. The correction retries only EINTR while running and
leaves the error path when shutdown has interrupted the syscall. EIO, short
reads, and payload/checksum assertions remain failures. The actual patched read
block compiled and passed five injected cases: exact read, EINTR then success,
shutdown EINTR, EIO, and short read. The full patch applies to the exact pinned
upstream sources. CI runs that injection check before building stress-ng.
The complete hosted native stress workload still needs a run on the new commit.

## Unmet design gates

| Gate | Current evidence and remaining requirement |
| --- | --- |
| D1 degraded cold start | Unmet. `storage_backend::local_directory` requires every configured local member directory to exist; readiness preflight also rejects missing roots. Fail-closed startup does not serve unaffected files with a member absent. |
| M1 metadata survival | Unmet. `docs/metadata-survival-design.md` explicitly remains design-only: no recovery generations, recovery command, or survival acknowledgement. A single authority/catalog loss is not covered by the payload crash test above. |
| P1–P7 strict placement | Unmet. Current pool allocation, segment migration and evacuation do not establish versioned per-file bindings, durable policy descriptors, exact-target routing, placement-preserving compaction/recovery, deliberate striping or bound-file moves. No strict capability is enabled here. |
| F1 metadata fidelity | Partial: FUSE xattrs pass, but kernel 9P empty replacement is incompatible as proven above. |
| F3 permissions/ACL | Unmet. `fs/permissions.rs` implements POSIX mode/identity checks; `fs/ops/xattr.rs` rejects `system.*`. No NFSv4 ACL evaluation or inventory-derived ACL preservation/enforcement contract is established. |

Per-policy durable write landing, NVMe retention/reserve policy, dashboard
configuration, verified migration/restore and remote-provider UI belong to
later plan rows. This PR's generic pool, xattrs, inventory and diagnostics do
not establish those product guarantees. Packaging and finite memory measurements
do not close D1, M1, placement or ACL gates.
