# Client writes that do not wait for the drain

The production NAS copied 8 GiB from a Spark NFS client at 22 MB/s through
fsync. [The previous pass](../p3-journal-group-commit-20261120/README.md)
reproduced that on a fixture and found that every client WRITE waited for the
drain's catalog commits. This directory measures the changes on this branch
that remove those waits and make each P3 journal flush cover more data.

The target is 900 MB/s. **This branch does not reach it.** On the fixture an
8 GiB copy goes from 55.0 MB/s to 181.7 MB/s, and P3 flushes per MiB fall from
1.19 to 0.14. What limits it now is listed below.

## Fixture

`scripts/bench-raw-journal-flush.sh` with `BENCH_FLUSH_DELAY_US=20000`, which
adds 20 ms after every fdatasync on the P3 loop. The drain is active. Each
512 MiB row is the median and range of 3 copies in one mount. The 8 GiB rows
are one copy each. Every copy was read back cold and matched its SHA-256, and
every run reported `verified_absent` for its container and volume.

Engine binaries are `ci` profile with `webui`, built in `rust:1.100.0-bookworm`
on `ubuntu-main` (AMD EPYC 7742, kernel 7.0.0-28-generic). The container had
3 CPUs and 8 GiB. The backing disk wrote 2 GiB at 1.3 GB/s with `O_DIRECT`.
Builds and benchmarks ran one at a time. Another agent's build shared the host,
at a load average of 3 to 8 on 96 cores.

**The 20 ms flush is a model, not a NAS measurement.**

## Each change, in commit order

Each row adds one commit to the row above. [Receipts](receipts.jsonl)

| Change | MB/s | P3 flushes/MiB | MiB per journal commit |
| --- | --- | --- | --- |
| Base `102b0e59` | 32.5 (25.7–40.1) | 2.13 (1.76–2.75) | 2.3 |
| Admission reads committed catalog rows | 93.7 (91.3–94.8) | 0.65 (0.61–0.71) | 5.2 |
| Region flush does not hold its lock | 115.1 (107.0–124.2) | 0.56 (0.52–0.63) | 6.6 |
| Drained payload hashed once | 115.9 (107.7–116.2) | 0.55 (0.54–0.62) | 6.7 |
| Remote marks ride the record commit | 138.0 (120.8–143.3) | 0.42 (0.42–0.51) | 6.5 |
| NAS fit takes 96 MiB of journal RAM | 175.1 (167.9–200.0) | 0.17 (0.15–0.22) | 34 |
| Segment keeps its leased member | 204.2 (166.9–227.4) | 0.15 (0.14–0.23) | 28 |
| Branch head | 213.6 (165.5–218.8) | 0.14 (0.13–0.22) | 34 |

Two accidental repeats of the region-lock binary gave medians of 109.4 and
119.1 MB/s. A step smaller than about 10 MB/s is therefore noise. By that
measure:

- **Hashing once has no measurable effect on the client copy.** It removes two
  of three SHA-256 passes from the drain, which runs behind the copy here.
- **The lease change is within noise for throughput.** Its effect is on the
  drain: 132 segments of 4.3 MiB per 512 MiB instead of about 220 of 2.4 MiB,
  so about 40% fewer catalog commits per MiB.
- The other four steps are each larger than the noise.

## 8 GiB

| | Base | Branch head |
| --- | --- | --- |
| MB/s through fsync | 55.0 | 181.7 |
| Seconds | 156.2 | 47.3 |
| P3 flushes per MiB | 1.19 | 0.14 |
| Journal commits (record batches) | 2,512 | 192 |
| MiB per journal commit | 3.3 | 43 |
| Journal region syncs / catalog region syncs | 4,232 / 5,415 | 339 / 796 |
| Engine CPU seconds | 92.2 | 72.1 |
| Engine peak resident memory | 400 MiB | 603 MiB |
| Journal bytes written per payload byte | 1.14 | 2.76 |
| Drain after the read-back | 56.9 s | 87.3 s |

Startup admission charges 8,168 MiB of the 8 GiB cgroup with 96 MiB of journal
RAM, up from 8,088 MiB.

## What limits the copy now

A temporary build timed each wait. It was not committed. In a 512 MiB copy at
173 MB/s (3.1 s):

- **The journal's single writer is busy the whole copy.** 21 record batches of
  26 MiB each spent 0.96 s inserting chunks into redb and 2.15 s in 33
  commits. A commit averaged 65 ms: 20 ms of modelled flush and about 45 ms
  writing. That is 5.9 ms per MiB: 1.8 inserting, 2.8 writing and 1.2 in the
  flush. Removing every remaining flush would make this path about 25% faster.
- **The journal writes each payload byte about twice.** Journal bytes per
  payload byte are 1.13 on a fresh journal with small batches and 1.4 to 2.8
  otherwise. Region growth is not the cause: all resizes in two copies zeroed
  less than 1 MiB. Inferred cause, not proven: redb copies the previous chunk's leaf
  when it inserts the next key, and its write buffer (half of a 64 MiB cache)
  has already written that leaf once a batch exceeds it or the read cache is
  full.
- **CPU.** The engine used 8.8 CPU-ms per MiB over the 8 GiB copy, 1.5 of its
  3 cores. At 900 MB/s that is about 7.5 cores of this CPU.
- **The drain.** It finished 87 s after the read-back. With a 20 ms flush each
  4.3 MiB segment still takes three catalog commits, and a sustained copy
  larger than the 10 GiB journal runs at the drain's rate.

## NAS estimate

**Estimate, not a measurement: 100 to 250 MB/s for the same 8 GiB copy, against
22 MB/s measured.** It assumes the NAS P3 flush costs 20 to 40 ms, that the
journal writes 2.8 bytes per payload byte as here, and that the P3 sustains 0.5
to 1.5 GB/s of journal writes. The first follows from 22 MB/s at 1.2 to 2.1
flushes per MiB. The last is unknown and is the largest uncertainty. The app's
2 CPUs cap the top of the range.

900 MB/s leaves 1.1 ms per MiB for the journal writer. redb's insert alone
takes about 1.8 ms per MiB on this host, before any device write. Reaching the
target needs three things this branch does not do:

1. Payload bytes written once and sequentially outside the B-tree, with redb
   holding only the records.
2. More than the app's 2 CPUs. The NAS run charged the FUSE client and `unfsd`
   about 4.9 CPU-ms per MiB between them at 22 MB/s, before the engine.
3. A drain that batches several segments per catalog commit and per member
   flush, for copies larger than the journal.

## Cleanup

Each run removes its container, its labeled Docker volume, its loop mappings
and its backing files, and reports `verified_absent`.
