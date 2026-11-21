# Raw journal payloads written once to an append log

The target is an 8 GiB copy from a Linux NFS client into the share at 900 MB/s
through fsync. [The previous pass](../write-path-decouple-20261120/README.md)
ended with the writeback journal's single redb writer as the limit. This
directory measures the journal redesign on this branch.

**This branch does not reach 900 MB/s.** On the fixture an 8 GiB copy goes from
184.8 MB/s to 294.0 MB/s, and the P3 takes 1.04 bytes per payload byte instead
of 2.56. A single-file copy is now bound by one serial write lane in the
engine, above the journal. The fixture's own P3 loop device stops near
500 MB/s, so it cannot show 900 MB/s for any engine.

## The journal's data shape

The P3 Journal region's data bytes are split at their midpoint.

```text
[ redb file: mutation records only | payload log: a ring of payload bytes ]
```

- A batch's payload bytes are written once, back to back, in block-aligned
  sequential writes of up to 4 MiB. A record names its payload as
  `raw_payload_log_v1#{position}+{len}`.
- redb holds the records, the log head, the local and remote watermarks and
  the SHA-256 of each payload.
- One batch stages its payload while the batch before it commits. The redb
  commit that records a batch issues one device flush. That flush covers the
  batch's payload bytes and its records, because both are writes to one
  device. A record is acknowledged only after it returns.
- Draining a record frees its log bytes from the tail. A reader pins its extent
  so a new batch cannot overwrite it.
- The measured implementation discarded a checksum-failing final commit using
  a watermark from the previous flush. That watermark cannot establish whether
  the final commit was acknowledged. PR review corrected recovery to stop on
  any pending committed-payload mismatch and preserve the records for repair.
  The measurements below predate that recovery correction.

## Fixture

`scripts/bench-raw-journal-flush.sh` with `BENCH_FLUSH_DELAY_US=20000`, which
adds 20 ms after every fdatasync on the P3 loop. The drain is active. Every
copy was read back cold and matched its SHA-256, and every completed run
reported `verified_absent` for its container and volume.

Engine binaries are `ci` profile with `webui`, built in `rust:1.100.0-bookworm`
on `ubuntu-main` (AMD EPYC 7742, kernel 7.0.0-28-generic). The base is
`1465f569`. The container had 3 CPUs and 8 GiB unless a row says otherwise.
Base and branch runs alternate. Other agents shared the host, at a load average
of 2 to 7 on 96 cores.

The base binary ran with the base copy of `raw-device/large_mounted.py`,
because that file asserts the journal layout. The rest of the harness is this
branch's.

Every run predates the merges of `integration/all-open-prs`, last at
`a5287cc2`, into this branch. The required checks ran again on the merged tree. The benchmarks
did not.

**The 20 ms flush is a model, not a NAS measurement.**

## Before and after

Each cell is the median and range. [Receipts](receipts.jsonl)

512 MiB, 6 copies per side in 2 mounts of 3 copies:

| | Base | This branch |
| --- | --- | --- |
| MB/s through fsync | 178.1 (165.0–219.6) | 252.2 (225.9–279.6) |
| P3 bytes written per payload byte | 1.70 (1.54–2.47) | 1.04 (1.04–1.04) |
| P3 flushes per MiB | 0.16 (0.14–0.23) | 0.18 (0.14–0.23) |
| Engine CPU-ms per MiB | 8.38 (8.22–9.53) | 6.52 (6.45–6.74) |
| Journal region writes per copy | 17,282 (16,264–24,315) | 573 (464–644) |

8 GiB, 3 copies per side, one copy per mount:

| | Base | This branch |
| --- | --- | --- |
| MB/s through fsync | 184.8 (184.1–204.3) | 294.0 (292.6–296.1) |
| Seconds | 46.5 (42.1–46.7) | 29.2 (29.0–29.4) |
| P3 bytes written per payload byte | 2.56 (2.09–2.64) | 1.04 (1.04–1.04) |
| P3 flushes per MiB | 0.14 (0.14–0.15) | 0.15 (0.14–0.15) |
| Engine CPU-ms per MiB | 8.76 (8.59–8.88) | 6.77 (6.77–6.85) |
| Journal region writes per copy | 401,713 (334,004–407,640) | 8,962 (8,957–9,153) |
| Journal commits | 193 (184–204) | 537 (534–546) |
| Engine peak resident memory, MiB | 584 (579–598) | 435 (433–460) |
| Cold read back, MB/s | 201 (200–202) | 236 (234–237) |
| Drain after the read-back, seconds | 86.2 (84.8–87.4) | 98.6 (96.5–99.5) |

The branch's drain ends later after the read-back because its copy ends 17
seconds sooner. Counting the copy, the read-back and the drain, the base takes
175 seconds and the branch 164.

2 GiB with other CPU limits, 2 copies per side:

| CPUs | Base MB/s | This branch MB/s |
| --- | --- | --- |
| 2 | 189.7 (188.0–191.5) | 230.6 (219.0–242.3) |
| 8 | 208.5 (206.9–210.1) | 294.3 (275.9–312.6) |

### Which build these rows measured

These rows come from a build that differs from the branch head in one way. It
still compiled a helper that asked the kernel to start writing each log range
out early, and it never called that helper. The head deletes the helper. The
`spot-*` series in the receipts repeat part of the matrix on a build of the
head, while another job held the host at a load average of 10 to 46.

| Spot check on the head build | Base | Head |
| --- | --- | --- |
| 512 MiB, 3 copies, MB/s | 159.1 (116.3–177.0) | 250.0 (223.7–257.5) |
| 8 GiB, 2 copies, MB/s | 182.2, 176.6 | 281.8, 303.3 |
| 8 GiB, P3 bytes written per payload byte | 2.09, 2.07 | 1.04, 1.04 |
| 8 GiB, P3 flushes per MiB | 0.14, 0.14 | 0.15, 0.13 |
| 2 GiB with 8 CPUs, 2 copies, MB/s | not run | 290.3 (284.2–296.3) |

## What limits a single-file copy now

**One file's serial write lane, at about 3.6 ms per MiB on this host.**

- **More CPUs do not help.** 3 CPUs give 294.0 MB/s at 8 GiB and 8 CPUs give
  294.3 MB/s at 2 GiB.
- **More files do.** `BENCH_WRITERS` writes several 1 GiB files at once. With 8
  CPUs the branch reaches 252.2 MB/s for 1 file, 345.3 for 2 and 346.6 for 4,
  the last a median of 5 runs with a range of 341.2 to 357.0. The base reaches
  211.4 MB/s for 4 files, a median of 5 runs with a range of 191.9 to 213.0.
- **A faster journal does not help one file.** The early write-out variant
  below carries 4 files 21% faster and one file no faster.
- **The journal waits for data.** A temporary build of that variant logged
  every batch. It was not committed. Over a 2 GiB copy the journal assembled
  233 batches. At every assembly its queue was empty and no prepared record was
  left over.

A CPU profile of the engine and the FUSE client during a 2 GiB copy puts 18% of
samples in SHA-256, 12.5% in ChaCha20-Poly1305, about 19% in user and kernel
memory copies and 1.8% in LZ4. The profile has no call graphs, so it does not
say which of these run in the lane.

## What limits the journal when it is the limit

From 2 files on, the journal is the limit, at about 346 MB/s here.

- **The fixture's P3 loop device.** The benchmark now times the payload log's
  own pattern on a scratch loop with no engine and no flush model: 32 MiB in
  4 MiB writes, then one fdatasync. Over 68 runs the median was 482 MB/s on a
  first write, with a range of 293 to 601, and 534 MB/s on a rewrite, with a
  range of 331 to 722. **900 MB/s cannot be shown on this fixture.**
- **The journal's commit.** In the 8 GiB copies the journal committed 537
  batches of about 15 MiB. A commit took 52 ms: 20 ms of modelled flush and
  about 32 ms of the device writing the batch out. Staging the payload took
  0.6 ms per MiB, which is two memory copies.

### A variant that was measured and not shipped

Each log write can ask the kernel to start writing its range out, so the
commit's flush has less left to wait for. That helps several files and costs
one file, which is the target, so the branch does not do it. The three
variants ran in different hours, and the base's 8 GiB median was 184.8 MB/s
beside the second row and 194.9 beside the third.

| Journal variant | 8 GiB, 1 file, 3 CPUs | 2 GiB, 1 file, 8 CPUs | 4 files of 1 GiB, 8 CPUs | P3 flushes per MiB at 8 GiB |
| --- | --- | --- | --- | --- |
| Payload written inside the commit | 279.5, 322.0 | not run | 339.7, 339.9 | 0.16, 0.11 |
| Payload staged ahead (this branch) | 294.0 (292.6–296.1) | 294.3 (275.9–312.6) | 346.6 (341.2–357.0) | 0.15 |
| Staged ahead with early write-out | 279.5 (278.3–293.5) | 252.4 (247.5–257.2) | 419.8 (410.4–425.3), 6 runs | 0.23 |

Staging ahead of the commit, without the early write-out, shows no measurable
gain over writing the payload inside the commit, with 1 file or with 4. The
first row's two 8 GiB runs are 43 MB/s apart, and its 279.5 MB/s run has no
receipt because a later run reused its output file.

## A stall that is not from this change

With 4 files written at once, some copies never finish. The base stalled in 1
of 6 runs. This branch stalled in 1 of 6, and the early write-out variant in 3
of 9. The branch's own stall ran into the run's time limit and was not sampled.
The other four were sampled after 150 seconds or more, and three showed this
state:

- `zerofs_nbd_volatile_memory_dirty_bytes` equal to its 16,777,216 byte
  capacity
- `zerofs_writeback_dirty_ram_bytes` at 0
- `zerofs_writeback_local_sequence` equal to `zerofs_writeback_accepted_sequence`
- no terminal error

The fourth differed only in holding 10,485,760 volatile bytes. In each the
journal was idle and every accepted record was durable. In the one stall
inspected further, 32 accepted writes were not materialized, two writer threads
were blocked in `write` and two had written their whole file. The cause is
above the journal and is not established. No single-file copy stalled.

Reproduce with `BENCH_WRITERS=4 BENCH_CPUS=8 BENCH_PAYLOAD_MIB=1024
BENCH_RUNS=1 BENCH_FLUSH_DELAY_US=20000`.

## NAS estimate

**Estimates, not measurements, for the same single 8 GiB file.**

| App CPU limit | Estimate | What bounds it |
| --- | --- | --- |
| 2 CPUs | 150 to 250 MB/s | CPU |
| 8 CPUs | 350 to 600 MB/s | the file's write lane, or `unfsd` |

The NAS CPU is an AMD Ryzen 7 255 with 8 cores and 16 threads, up to 4.97 GHz,
with `sha_ni`, `avx2` and `vaes` (read 2026-11-20T18:46:39Z). The estimates
assume:

- one NAS core does the lane's work 1.6 to 2.2 times as fast as one EPYC 7742
  core, which puts the lane at 1.6 to 2.2 ms per MiB, or 480 to 650 MB/s
- the engine's CPU per MiB scales the same way from 6.5 CPU-ms per MiB here
- the FUSE client and `unfsd` cost 4.9 CPU-ms per MiB between them, as the
  earlier NAS run charged them at 22 MB/s
- the P3 takes a sequential stream at that rate with one 20 to 40 ms flush per
  batch

The last is unknown. `unfsd` runs one thread, and its share of the 4.9 ms is
not known either, so it may bound the copy before the lane does.

## What 900 MB/s still needs

900 MB/s leaves 1.1 ms per MiB for any serial stage.

1. The file's write lane takes about 3.6 ms per MiB here. It needs a third of
   that, or one file's writes spread over several lanes.
2. Then the journal, which stops at 346 MB/s on this fixture, or about 420
   with the early write-out. Its commit spends about 2.1 ms per MiB with the
   device writing the batch out, inferred from the commit time less the 20 ms
   flush. The P3's sequential rate decides this on the NAS.
3. For copies larger than the 10 GiB dirty budget, a drain that batches several
   segments per catalog commit. The payload log has room for a 26 GiB budget,
   which the NAS-fit policy does not allow yet.

## Durability tests

The measured revision ran these tests in `zerofs/src/writeback/journal.rs`,
on a copy of the device bytes taken at the cut:

- `a_torn_payload_tail_is_never_replayed_and_replay_stops_at_the_last_durable_batch`
- `a_power_cut_after_the_flush_keeps_the_whole_last_batch`
- `a_damaged_payload_below_the_last_commit_needs_an_operator_and_drops_nothing`
- `payload_bytes_without_their_commit_are_not_replayed`
- `a_staged_raw_batch_is_replayed_only_after_its_own_commit`
- `a_discarded_raw_batch_frees_its_log_bytes`
- `drained_log_space_is_reused_across_wraps_and_survives_a_power_cut`
- `a_full_payload_log_refuses_the_batch_and_keeps_what_it_holds`
- `a_reader_keeps_its_payload_while_the_log_reclaims_around_it`
- `payloads_left_in_redb_by_an_earlier_engine_replay_and_drain`
- `a_database_an_earlier_engine_grew_into_the_log_is_compacted_on_open`
- `raw_prefix_retirement_needs_no_flush_and_a_power_cut_keeps_every_acknowledged_put`

The first test's automatic rollback policy was later replaced by
`a_torn_raw_tail_requires_repair_without_discarding_committed_records` and
`a_damaged_last_acknowledged_raw_batch_is_not_silently_discarded`. These
regressions require a readable commit's records and byte counters to survive a
payload verification failure. They do not add physical power-loss evidence.

`the_payload_reserve_is_fenced_from_the_file_and_survives_reopen` covers the
region in `zerofs/raw-device`.

`BENCH_CRASH_REPLAY=1` runs the engine binary. It writes 512 MiB and sends the
engine SIGKILL as fsync returns. Member writes are paced so the copy is still
in the P3 journal. On the head build the stopped journal then held 141
records, 1 of them drained, with 557,144,689 undrained payload bytes, 557,191,168
log bytes held and a redb file of 159,744 bytes. The restarted engine read the
file back with a matching SHA-256.

A SIGKILL keeps the page cache, so this is a process crash and not a power cut.
The unit tests above cover the power cut.

## Not run

- `scripts/test-raw-device-mounted.sh` and `scripts/test-raw-device-large.sh`
  need the Studio or the Spark and arm64 binaries. `large_mounted.py` now
  asserts the new layout and has not run there.
- No NAS run. The parent measures on the NAS.

## Cleanup

Each run removes its container, its labeled Docker volume, its loop mappings
and its backing files. Completed runs report `verified_absent`. The stalled
runs were stopped, and their containers and volumes were checked absent
afterwards.
