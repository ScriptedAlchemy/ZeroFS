# Mounted raw-member fixture

The Studio runs used the parent-supplied v3 working-snapshot binaries with
integration reference `056d4158`. Each log records both executable SHA-256
digests. The fixture uses the production ZeroFS server and FUSE client with
strict consistency and write-through. Both raw members are whole, positively
identified loop devices backed by owned tmpfs inodes.

- [Attempt 1](studio-v3-attempt-1.log) initialized the 32 MiB and 96 MiB loops.
  Startup refused the 2 GiB cgroup because legacy admission required
  43,050,336,256 bytes. This is the bounded Studio result.
- [Attempts 2](studio-v3-attempt-2.log) and [3](studio-v3-attempt-3.log) ran
  under the default Docker VM envelope. Attempt 2 confirmed that the actual
  kernel mount type was `fuse`. Attempt 3 verified the initial mounted hash,
  then showed that the pool identity marker also funds a tombstone.
- [Attempt 4](studio-v3-attempt-4.log) used the same default envelope. The
  first restart verified the initial 8 MiB hash and the catalog assigned one
  segment to each member. The smaller member reached 3,768,320 allocatable
  bytes with its 16 MiB reserve and 16,384-byte retirement reservation.
  Two further mounted fsyncs succeeded and the resulting 28 MiB file matched
  its expected SHA-256. The run was stopped at operator direction before its
  post-spill catalog and final cold-restart checks. Docker removed the owned
  container. The runner uses the original 2 GiB limit again.

## Pinned Spark result

[Spark attempt 4](spark-056d4158-attempt-4.log) passed with exact
Git-archive binaries from `056d41586ee925479e21d169294123a2acb8f565`.
Spark's arm64 Docker container used a 47,244,640,256-byte cgroup limit and
zero swap. The engine admitted its 43,050,336,256-byte startup requirement.
The probe rejected a swapped backing inode and a swapped member UUID.

The 32 MiB and 96 MiB members each held one segment of the initial 8 MiB file,
whose mounted hash matched after restart. At the reserve boundary the smaller
member had 20,537,344 raw bytes free, a 16,777,216-byte reserve, and 16,384
bytes reserved for retirement, leaving 3,743,744 bytes allocatable. Both
following fsyncs succeeded and added two placements to the larger member.
Their 8,651,123 catalog bytes occupied 8,667,136 physical log bytes as predicted
by raw v1 header and alignment costs. The larger member also reserved 8,192
more bytes for retirement. The smaller member's physical space and placements
stayed unchanged.

The final cold mount read 29,360,128 bytes with SHA-256
`d86adf06785b7ddb6654db261aaba2ddb51abff325cc139b501d2ba05c52b54f`.
The final catalog, raw object hashes and physical space matched the
post-overflow snapshot. The receipt verifies all four unmounts, two loop
autoclears, and removal of the backing files, scratch, Docker container and
transfer directory.

Spark [attempt 1](spark-056d4158-attempt-1.log) and
[attempt 2](spark-056d4158-attempt-2.log) exposed transfer-directory permission
errors. [Attempt 3](spark-056d4158-attempt-3.log) passed admission but Docker's
default AppArmor profile refused the FUSE mount syscall. The passing run
disabled that profile only for the disposable Spark container. It kept its
read-only image and fixture binds, private network namespace, narrowly scoped
device rules, cgroup and capability limits. Studio retains its default profile.

## P3 NAS-fit crash, replay and destage

[Spark NAS-fit attempt 2](spark-nas-fit-98e0081f-attempt-2.log) passed with the
package-featured engine, probe and runtime manifest from exact source
`98e0081f33c63f66c5e91706c87bb231a26efc6d`. The 8 GiB cgroup admitted the
7,504 MiB requirement for both reserve settings with zero swap. The container,
including the fixture, FUSE process and tmpfs loop backing, peaked at
542,642,176 bytes (517.5 MiB). Its two-CPU quota showed zero throttled periods.
All sampled memory event counts and swap usage were zero. The index and all
eight referenced WebUI assets returned HTTP 200 with the expected content
types recorded in the receipt.

The 32 MiB and 96 MiB members initialized at the manifest's 16 MiB and 8 MiB
reserves. With both reserves raised to capacity minus 256 KiB, each member had
241,664 allocatable bytes. The 2 MiB ordinary FUSE write returned in
10.48 ms. Fsync returned in 14.57 ms and SIGKILL followed in 0.091 ms.
The offline catalog showed zero payload placements after the kill and after
the restart with raised reserves. The cold mounted read matched all
2,097,152 bytes in 37.49 ms including SHA-256 verification. The P3 journal
contained 3,608,576 bytes at both checkpoints.

After reopening the same loops and identities with their original reserves,
the administrative drain barrier returned in 5.79 ms after mount readiness
(310.24 ms including startup). Offline inspection verified one 2,167,450-byte
raw segment on the smaller member taking 2,174,976 raw log bytes. The final
cold restart matched that catalog entry and its raw object SHA-256. The file
SHA-256 was identical before and after destage
(`4a6ded0a8e23b872621fd07521873a3c5847663a0657a59e1345de073c90865d`).
The P3 journal shrank to 114,688 bytes. The raw catalog and authority remained
populated, and the wrapped key was found only in the key region. No directory
cache, journal, catalog, authority, or key shadow appeared. All four FUSE
mounts were removed, all three loops autocleared, and the container and its
transfer and scratch directories were removed.

[NAS-fit attempt 1](spark-nas-fit-98e0081f-attempt-1.log) preserved the same
admission, exact pin and zero-swap setup. The 4 MiB write exceeded the 4 MiB
seal threshold after framing. The catalog was empty immediately after
SIGKILL, and the held cold read matched, but replay placed a 33,075-byte
trailing segment that fit below the held headroom. The passing retry used
2 MiB to keep the encoded write below the seal threshold and retained the
original held reserves. The receipts include the fixture's synthetic
encryption password as part of the exact rendered manifest.
