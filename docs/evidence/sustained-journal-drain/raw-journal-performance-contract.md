# What the write path needs from a raw journal and raw members

Audience: the owner of the raw block backend. This note states performance requirements only. It proposes no on-device format and no writer; the backend owner designs those. Every measurement cited here was taken with the **directory-backed** journal and members on Studio Docker (native linux/arm64, ext4 inside the Docker VM, on an external USB SSD). Qualification on raw devices is pending the raw backend.

## Contract the path must keep

Ordinary WRITE is acknowledged from bounded RAM. An explicit FLUSH/fsync/COMMIT/FUA succeeds only after every byte it covers is recoverable from the SSD journal. The goal is at least 900 decimal MB/s of payload per copy over 10 GbE, Linux-to-NAS first.

## What the journal issues today (directory backend, measured)

Per 512 MiB of RAM-acknowledged payload (`strace2-head-512m`, 512 KiB WRITEs at depth eight):

| Target | Writes | Bytes per payload byte | Syncs | Notes |
|---|---|---:|---:|---|
| Journal payload containers | one write call per container, about 33 MiB each (16 containers) | 1.009 | one `fdatasync` per container plus its directory `fsync` | up to 64 MiB per container; two containers in flight; one commit at a time |
| Journal redb (records, watermarks) | 4 KiB pages (312) and 512 B (70) | about 0.001 | about 100 commits, each `fdatasync` | one transaction per staged batch, plus one per drained run |
| Pool catalog redb | 4 KiB pages | 0.002 | 52 | about nine catalog transactions per drained segment |
| Filesystem metadata (LSM) | small | 0.001 | 67 | flushed by explicit FLUSH and the LSM flush interval |
| Members (drain) | one write per 32 MiB segment | 1.006 | 3 per segment after `perf(local-store)` | directory backend: file write, `fsync`, rename, directory `fsync` |

Through the first fsync, before drain caught up, the SSD side measured 1.13 to 1.26 bytes per payload byte. With every write-time clean-cache copy removed under v2 (`perf(cache)`), it was 1.01 after drain. Before that change the clean cache added about 1.0 byte per payload byte on the same device.

## Requirements for 900 MB/s through the final FLUSH

1. **Sequential append in large aligned writes.** Payload arrives as sealed 32 MiB segments, which the journaler batches into containers of up to 64 MiB. Keep one large sequential append per batch, aligned to the device's physical sector (and preferably to its optimal I/O size). Never split a segment into sub-sector or misaligned pieces, and never read-modify-write a partial sector.
2. **One flush barrier per group, not per write.** A journal commit today is: payload durable, then the record transaction durable, then the watermark published. On a raw device, the payload write and the commit record of many client writes must share one device cache flush (group commit). The fsync rate the device sustains, times the bytes per group, must exceed 900 MB/s × the journal's bytes per payload byte (about 1.01–1.1).
3. **Do not trust FUA because it is advertised.** Consumer NVMe drives without power-loss protection may advertise FUA or a volatile write cache whose flush behaviour has not been verified. Durability must rest on an explicit, verified flush (NVMe Flush, or `fdatasync` on the block device), checked by a power-loss style test, not on the FUA bit.
4. **Bounded per-device concurrency.** Today two container writes may be in flight with one ordered commit, and up to eight destage uploads (v2) read the journal while new containers are written. The raw backend should bound in-flight writes per device and keep drain reads from starving journal appends. Report queue depth and time blocked in the device flush as metrics, the way `zerofs_writeback_local_publish_phase_seconds` reports `container_write` and `directory_fsync` today.
5. **Retirement without per-object metadata I/O.** Drain should free journal space by advancing a tail pointer or reclaiming whole regions, not by per-payload metadata updates.
6. **Members.** Drain writes whole 32 MiB segments, read once from the journal and verified once (`perf(writeback)`), with the digest passed to the pool (`perf(pool)`). Raw members need large aligned sequential writes, one durable barrier per segment or per group of segments, and enough per-member concurrency to keep both HDDs streaming at once.
7. **Report what sizing needs.** Admission and landing-window validation should use what the journal device reports: usable capacity, the reserve the backend keeps, and a measured sustained synchronous write rate, rather than filesystem free space.

## Directory-backend-specific parts of this branch

These help only while the journal and members are directories on a filesystem, and do not carry over to raw devices:

- `perf(local-store)`: caching durable directory entries and the lock file, to skip repeated ancestor directory fsyncs.
- Container files per batch, directory `fsync` after each container, and unlink-based retirement in the journal.
- The capacity check's use of `statvfs` on the journal directory's filesystem in `small-nas-journal-v2`.

## The measurement the app owner would need on a raw P3

The P3's raw sustained synchronous rate is **unknown**. Every sustained figure so far (57–133 MB/s) went through ZFS. Once the P3 is blank and its conversion is authorized, the useful measurement is a sequential write of at least 16 GiB in large aligned blocks with direct I/O, issuing a device flush every 64 MiB (or at the raw backend's group size), recording throughput over time to show any fast-region boundary, and repeating once after an idle period. A one-second burst figure does not answer the question.

## Assessment of the raw-device store at `b615aace`

Read from `zerofs/raw-device/src/store.rs` at commit `b615aace` (branch `t3/raw-device-durable-object-store`). Not built, run or depended on here. Rates are derived from the directory-backed profiles above and from x86 SHA-NI timings, not measured on a raw device.

| Property in `b615aace` | Effect at 900 MB/s of RAM-acknowledged payload | Ask |
|---|---|---|
| `put` hashes every value with SHA-256 | One pass over 32 MiB took about 22 ms on an EPYC 7742 with SHA-NI (1.5 GB/s): about 0.6 core at 900 MB/s. The writeback layer already hashes each payload at ingest, and drain verifies it again, so this would be a third pass on an 8-CPU quota shared with Samba. | Accept a caller-verified digest (`put_with_sha256`) the way `perf(pool)` passes `KnownPayloadSha256`. Keep verification on read. |
| `put`, `delete`, `flush` take `&mut self` | Appends stop while a flush is syncing. Today's journaler stages the next container while the previous one commits; that overlap is what kept staging below 30% busy on tmpfs. | Split append from the barrier: capture `durable_tail` at barrier start, sync on another handle or thread, and keep appending. Then one writer is enough for 32 MiB sequential appends. |
| `flush` = data sync + slot-0 write/sync + slot-1 write/sync (three syncs) | With 64 MiB groups, 900 MB/s needs about 14 barriers per second, or 42 syncs per second: affordable. If every client FLUSH or COMMIT became its own barrier (an uncached CIFS or NFS client can flush every few MiB), that rises to hundreds of barriers per second, which a drive without power-loss protection cannot sustain. | Group commit: any FLUSH arriving while a barrier is in flight joins the next one. Make the record stream self-validating (records already carry checksums and the per-provision nonce), so recovery scans forward from the last label and the label is rewritten periodically, not on every barrier. That leaves one data sync per barrier. |
| Buffered `pwrite` through the page cache, no O_DIRECT | An 8 GiB burst through a buffered fd is charged to the 8 GiB container's cgroup as page cache. That doubles the data already held in the bounded RAM tier, and dirty-page throttling (`balance_dirty_pages`) sets in before the RAM window is full. The directory-backed runs reached the cgroup limit with page cache alone. | O_DIRECT with 4 KiB-aligned buffers (records are already block-padded), queue depth 2–4. Or at minimum drop written ranges from the page cache after each barrier. |
| Record = one 4 KiB header block + payload padded to 4 KiB; values over 256 KiB are written in two or three pwrites | Negligible overhead for 32 MiB segments (0.01%). Small metadata records cost at least 8 KiB each, but metadata is about 0.003 bytes per payload byte. | Align to the device's reported physical sector and optimal I/O size, not a fixed 4 KiB. Issue the header and payload as one vectored write when possible. |
| No reclamation; the log ends in `NoSpace` | At 900 MB/s a 1 TB device fills in about 19 minutes of continuous copying. The journal retires strictly in sequence order once drain commits. | Circular log with a head pointer advanced by drain retirement (no compaction needed for FIFO journal use), and expose usable capacity and the reserve for admission sizing. |
| I/O error poisons the store, no fsync retry | Correct for fsync failure semantics. | Keep. |

Whether 900 MB/s through the final FLUSH is reachable on the P3 depends on its raw sustained synchronous write rate, which is unknown (see the measurement above). If that rate is S and the journal writes about 1.01 bytes per payload byte, a copy of C bytes arriving at R needs about C × (1 − S/R) of RAM window to keep accepting at R, and the final FLUSH takes about C × (1 − S/R) / S. For C = 8 GiB and R = 900 MB/s, both terms reach zero only when S ≥ about 910 MB/s.
