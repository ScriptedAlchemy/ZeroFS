# P3 journal flushes per acknowledged MiB

The production NAS copied 8 GiB from a Spark NFS client at 22 MB/s through
fsync. The P3 stayed busy for the whole window, and the engine used 0.59 of its
2 CPUs. This directory reproduces that pattern on block-backed loop fixtures,
explains it, and measures the group-commit change on this branch.

## Fixture

`scripts/bench-raw-journal-flush.sh` runs the `raw-device-p3-nas-fit-v1`
fixture manifest (`zerofs/src/cli/memory_raw_nas_fit_fixture.toml`) on three
private loops. They are a 64 GiB P3 with the production layout and 12 GiB and
16 GiB raw members. Each loop is a preallocated file on a Docker volume on
`ubuntu-main`'s ext4 disk, not tmpfs. The client is the FUSE mount. Each run
writes an incompressible file with 1 MiB writes, calls fsync, and records the
`/sys/block/loopN/stat` deltas. Field 15 gives flushes. The run then reads the
file back after dropping the loop and backing page caches, and drains it.

The fixture loop flush costs 0.3–2 ms for 4 KiB and about 2.6 ms for 1 MiB.
The NAS P3 is slower. To model it, `BENCH_FLUSH_DELAY_US=20000` preloads
`scripts/raw-device/flush_shim.c` into the engine, which adds 20 ms after every
fdatasync on the P3 loop only. It also logs every P3 write and sync with its
offset, so syncs can be attributed to layout regions. On the unchanged engine,
this model gives 28–37 MB/s, near the NAS's 22 MB/s. **The 20 ms NAS flush
latency is an inference from that match, not a NAS measurement.**

Engine binaries are `ci` profile with `webui` and are not stripped, built in
`rust:1.100.0-bookworm` on `ubuntu-main` (AMD EPYC 7742, kernel
7.0.0-28-generic). The container had 3 CPUs and 8 GiB. The base is
`88929238`, with zerofs SHA-256 `0c4674b1…6cea`. The final build is this
branch, with SHA-256 `9a6c69b0…2b40`.

## The per-flush pattern (base, 20 ms model, 512 MiB)

A temporary build logged every redb write-transaction wait and commit. It was
not committed. The 512 MiB copy produced 248 segments of about 2.1 MiB each.

| Per segment | Durable commits | Where |
| --- | ---: | --- |
| Ingest journal commit (`commit_record_batch`) | 1 | foreground: releases the RAM journal, gates fsync |
| Remote watermark (`mark_remote_batch`) | 1 | drain |
| Raw prefix retirement (`remove_remote_prefix`) | 1 | drain |
| Catalog reserve, mark started, publish | 3 | drain |

That is about 6 P3 flushes per 2.1 MiB, or 2.9 flushes per MiB. The shim
attributed 730 syncs to the Journal region and 735 to the Catalog region.
Batches averaged 1.0 record: 246 batches for 248 records. Two foreground waits
sat behind those flushes:

- The space sampler opened a redb write transaction before its read-only fast
  path. It ran 722 times and waited 9.1 s in total for in-flight commits.
- Pool `admit` runs on every 512 KiB write. It holds the admission ledger and
  opens a catalog write transaction. It ran 1,024 times and waited 8.9 s in
  total behind the drain's catalog commits.

The copy took 18.6 s. The engine used 7.0 CPU-s.

## Change

- The drain assembles a batch for a sink that stages nothing only when the
  commit half is idle. Every record acknowledged during one commit then shares
  the next durable commit. Directory journals keep their container pipeline.
- The space sampler takes its virgin-region shortcut before acquiring the
  redb writer.
- Raw prefix retirement commits with `Durability::None`. It deletes rows at or
  below the durable remote watermark. `open_raw` retires them again, and the
  next durable commit persists the delete.

The ingest commit stays `Durability::Immediate`. A FLUSH still returns only
after the record and its payload chunks are durable on the P3.

## Before and after

Each cell is the median and range of 3 copies in one mount. The drain runs
between copies except in the paced rows. [Receipts](receipts.jsonl)

| Condition | Base MB/s | Branch MB/s | Base P3 flushes/MiB | Branch P3 flushes/MiB |
| --- | --- | --- | --- | --- |
| 20 ms flush, drain active, 512 MiB | 30.5 (30.4–33.6) | 30.0 (27.5–39.8) | 2.61 (2.48–2.76) | 2.48 (1.74–2.55) |
| 20 ms flush, member PUTs paced 5 s, 512 MiB | 105.9 (64.2–106.4) | 121.2 (110.2–122.0) | 0.28 (0.28–0.60) | 0.23 (0.22–0.28) |
| Fixture flush, drain active, 1 GiB | 137.6 (134.7–139.0) | 134.6 (133.6–135.4) | 1.89 (1.70–2.03) | 1.43 (1.42–1.53) |

Verdict:

- **With the drain active, which is the NAS case, there is no measurable
  throughput difference.** Flushes per MiB fall by up to a third in some runs.
- When the drain is paced so it barely touches the catalog or journal, the
  branch is 14% faster at the median. The first copy into a fresh journal is
  72% faster.
- At the fixture's own flush cost, both sides run at 135 MB/s. That copy is
  bound by the single in-flight seal and CPU, not by flushes.

## What limits the copy instead

- **The drain's catalog commits gate foreground writes.** Each drained segment
  takes three durable catalog commits. Every 512 KiB foreground write takes the
  admission ledger and a catalog write transaction. `reserve` holds that ledger
  across its own flush. In a single-run check, making the drain faster (fewer
  journal flushes) made the foreground slower: admit waits rose from 4.0 s to
  10.7 s. Decoupling `admit` from the drain's catalog commits is the next
  change.
- **Batches stay small.** With the drain paced, batches reach 1.3–1.5 records.
  The raw profile allows one sealed segment in flight (`max_inflight_seals = 1`),
  and the 16 MiB RAM journal bounds what one commit can collect. At 20 ms per
  flush, 900 MB/s needs about 18 MB per flush on the critical path.
- **CPU.** In a `perf` sample of the group-commit build at the fixture flush cost, SHA-256
  is 29% of samples. ChaCha20-Poly1305 is 8.6% and LZ4 is 1.5%. The engine
  spends about 11 CPU-ms per MB written on this host. The drain hashes each
  journal payload twice: once to open it and again while streaming it.

## Read-back

The fixture does not reproduce the NAS's 12 MB/s read-back. Cold reads from
the P3 journal (drain paced) ran at 199–231 MB/s with 1.09–1.15× P3 read
amplification. Cold reads from the members ran at 168–278 MB/s. Every ranged
read of a journal-resident segment re-hashes the whole segment, so it costs
CPU, 10–11 CPU-ms per MB read, but it does not re-read the device. On the NAS
the drain had already moved 7.7 GB to the HDDs, so the read-back most likely
came from the HDDs through a single-threaded `unfsd`. Confirming that needs NAS
read counts and sizes on the HDDs during a read-back.

## Cleanup

Each run removes its container, its labeled Docker volume, its loop mappings
and its backing files, and reports `verified_absent`.
