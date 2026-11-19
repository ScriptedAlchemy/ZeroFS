# Large raw-device software fixture

Run this fixture on the Spark with the Linux arm64 ZeroFS engine, raw probe and
`memory_raw_nas_fit_fixture.toml` from one pinned build.

```sh
scripts/test-raw-device-large.sh /path/to/bin
```

The launcher gives the FUSE client, engine, harness and a touched 1.5 GiB
anonymous companion allocation one 8 GiB cgroup with swap disabled. A private
ext4 Docker volume supplies 100 GiB of allocated extents for a 64 GiB P3,
12 GiB and 16 GiB payload members, and a reusable 8 GiB synthetic source.
Fallocate reserves the backing first, and the raw claim checks FIEMAP coverage
and independently matches each backing inode to its exclusive loop device.
The P3 has a 60 GiB journal region, a 10 GiB logical writeback budget and a
4 GiB physical free-space floor.

1. Explicitly initialize the pool and verify the original journal, authority,
   catalog, key envelope and memory admission with the server stopped.
2. Mount FUSE with zero admissible space on both payload members. Copy the
   first full 8 GiB file, call `fsync`, and kill the server. Confirm the
   catalog has no payload placements and neither member spent raw log space.
   Evict owned backing caches, start fresh server and client processes, and
   verify the entire file while both members are still held. With the server
   stopped, reconcile the journal file length, allocated pages, dirty bytes
   and the physical resize ceiling.
3. Restore normal reserves, start a distinct second full 8 GiB FUSE copy, and
   delay each actual member PUT by 200 ms through the synthetic binding.
   Observe completed foreground writes overlapping sampled SSD pacing. After
   the administrative remote-durability barrier, require matching frontiers,
   zero dirty reservations or outstanding physical claims, and enough
   remotely completed bytes for both files.
4. Verify every cataloged payload via the raw probe on its independent member.
   Match exact raw log usage to aligned payload records and hash the raw object
   manifest for each member. Flush the owned loop caches, record residual
   backing residency via `fincore`, reopen and verify both complete files.
5. Replace only the two journal length headers with the same device's
   previously saved empty-region headers. Check that both ordinary startup
   and offline readiness refuse the missing original journal and leave the
   headers unchanged under this synthetic omission. Restore the original
   headers and verify offline readiness.
6. Verify autoclear for each owned loop and remove the exact scratch paths,
   container and volume. Receipts retain binary digests, layout, timing,
   measured cgroup counters, verification results and removal evidence.

The simulated device delay is a repeatable source of write pressure.
Cold verification uses new processes and mounts, plus invalidation and
measurement of the Linux caches for the owned devices. The run models a server
process failure and exercises file-backed Linux loop devices.

## Initial 500 ms pacing run

Engine `6a1855b8` completed the held write in 75.334 s and `fsync` in
124.305 ms. The server received SIGKILL 159.872 μs after `fsync` returned.
Both member logs retained their original free space with zero payload
placements. The cold held replay reproduced the full 8 GiB SHA-256 in
78.109 s after `fincore` found zero resident bytes on all three backings.

The held redb file length was 10,737,586,176 bytes. The resize ceiling was
60,129,533,952 bytes after subtracting both 4 KiB headers and the 4 GiB
physical reserve. The 49,391,947,776-byte difference is room for file growth.
The sampled physical-availability gauge included the separately fenced
reserve. Issued dirty reservations and the redb file length are separate
measurements.

The second full copy completed in 584.726 s with a 221.340 ms `fsync`.
The largest sampled SSD reservation was 10,197,103,055 bytes while admission
was paced. The original harness exited 1 when the 360 s admin barrier
deadline elapsed, with 13,295,388,268 remotely completed bytes and
4,577,641,404 dirty SSD bytes still reserved. The completed first replay,
progress through pacing, timeout and verified exact cleanup are in the
[initial receipt](spark-6a1855b8-attempt-1.log).
