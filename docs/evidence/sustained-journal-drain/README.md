# Sustained copies through the bounded NAS journal (directory-backed)

**Everything on this page uses the directory-backed journal and members.** It was measured in short-lived containers on the Mac Studio's Docker Desktop: native linux/arm64, no emulation, an Apple M1 Ultra host, a 20-vCPU / 15.35 GiB Linux VM (kernel 6.12.76-linuxkit, cgroup v2) shared with other agents. Each engine ran in an 8-CPU, 8 GiB, zero-swap cgroup unless stated. The journal, members, cache and catalog shared one ext4 volume inside the VM, whose disk image sits on an external USB SSD (`DiskFlush=os`). These rates are relative comparisons between binaries on the same host. They are **not NAS MB/s**, they say nothing about raw devices, and only the app owner's physical trial can close the 900 MB/s objective. Qualification on raw devices is pending the engine owner's raw backend.

Contract held throughout: ordinary WRITE acknowledged from bounded RAM, explicit FLUSH/fsync only after SSD-journal durability, `ignore_fsync = false`, no dirty data dropped.

## Where the time goes

**Pre-saturation write path** ([stage timers](stage-timers.txt), [diagnostic patch](diagnostic-timers-head.patch), not part of the product build):

- On a device-limited volume (`prof-v2-8g-ext4`, 16 GiB v2 journal never paced), the client waited a mean 562 ms per 32 MiB segment for **dirty-object RAM admission**. RAM is freed only as the journaler stages containers: 709 ms per container, 186% busy across its two concurrent writers, on the same device the drain was writing. SSD admission waits were 0.26 ms. So below the journal's watermark, the copy runs at the rate at which the journal device absorbs container writes plus fdatasync.
- With the device removed (`prof-hwsha-2g-tmpfs`, everything on tmpfs), RAM and SSD admission waits were 0.01 and 0.19 ms and journal staging was 28% busy. The ceiling was upstream of writeback, at about 250–265 MB/s per single file. CPU: ChaCha20-Poly1305 segment encryption 35%, async runtime scheduling 26%, 9P handling 8%, catalog/redb 7%, LZ4 under 1%, SHA-256 under 0.1%. Giving the container 16 CPUs instead of 8 did not raise throughput (2.9–4.3 cores used, never throttled).
- **Hashing.** With the sha2 crate's software backend (its default on aarch64), SHA-256 was 68% of CPU and capped the engine near 97 MB/s. With hardware SHA (`--features sha2/asm`, used only for these measurement builds), that rose to 255 MB/s. The NAS's x86 CPU uses SHA-NI by default (Ryzen 7 255, `sha_ni` read 2026-11-19T03:28:12Z), where one pass over a 32 MiB segment took 22 ms in an earlier x86 profile. The digest guards journal payloads before they are published to the HDDs, so it stays.
- **Unaligned writes.** A 9P client that writes msize − 24 bytes per request ends every write partway through a 32 KiB extent. The next write then re-reads and decrypts that extent from a sealed segment. One file with RAM-acknowledged writes: 145.2 MB/s unaligned versus 253.5 MB/s with aligned 512 KiB writes. Kernel 9P mounts split writes this way; FUSE with an aligned `max_write` does not. This is in the filesystem extent path and is not changed here.
- **Per-file serialization.** RAM-acknowledged writes: one file 186.6 MB/s, four files in parallel 414.5 MB/s through last WRITE (372.2 through fsync). Materialized writes: about 250 MB/s either way.

**Drain:**

- Before the changes, each drained 32 MiB segment was read and SHA-256-verified, then read and hashed again while streaming, then hashed a third time by the pool, then written with fsyncs of every ancestor directory plus the lock file, then committed through about nine catalog transactions. In the device-limited profile, member puts took 1,430 ms per segment, 620 ms of it in ancestor/lock fsyncs.
- On a pre-ban x86-64 shared host (ubuntu-main, receipts there outstanding and unverified), the two v1 upload workers were 197.6% busy out of a possible 200% at 540 ms per segment, with the journal pinned at its 95% watermark. That host's own disk wrote 832 MB/s in a single-writer control, so there the drain was engine-limited, not disk-limited.

## Engine changes (each its own commit)

| Commit | Change | Backend |
|---|---|---|
| `perf(writeback)` | Drain reads each journal payload once, hashing exactly the bytes it publishes. | independent |
| `perf(pool)` | The drain passes that verified digest to the pool, which no longer rehashes (`KnownPayloadSha256`). | independent |
| `perf(local-store)` | Directory entries and the lock file are synced once per inode (device, inode, birth time), not on every object write. | **local-directory backend only** |
| `feat(config)` | `small-nas-journal-v2`: journal 512 MiB–16 GiB in 256 MiB steps, free floor max(64 MiB, journal/8), two to eight upload workers, 64 MiB of admission per worker beyond two. v1 unchanged. | independent (its `statvfs` capacity check is directory-specific) |
| `perf(cache)` | v2 skips copying freshly written segments into the clean disk cache; reads still fill it. | independent |

## Before and after (directory-backed, Studio Docker, relative only)

Command: `SCRATCH_VOLUME=<owned volume> IMAGE=<owned image> scripts/run-nas-journal-sustained.sh <root> <binary> <template> <label> <files> <MiB> --write-kib 512 [--depth N]`. Templates come from `scripts/fixtures/nas-journal/make-v2-template.py`. Base is `63ad64ce`; HEAD's engine is identical to `55548730`. Both were built with hardware SHA for these runs. Two runs each where shown. [All rows](summary.jsonl)

| Case | Base, MB/s through fsync | HEAD, MB/s through fsync | SSD bytes per payload byte (journal + metadata + cache) |
|---|---:|---:|---:|
| v1 profile, 8 GiB, 512 KiB × depth 8 | 67.4, 69.9 | 92.3, 107.6 | 2.05 → 1.59–2.05 (v1 keeps the cache warm) |
| HEAD v2 16 GiB journal, RAM ack, 8 GiB, 512 KiB × 8 | — | 155.3, 146.9 (never paced) | 1.03–1.06 by fsync, 1.01 after drain |
| Depth 1 (uncached-client pattern), 2 GiB | 117.8 (v1) | 181.7 (v2) | 1.97 → 1.08 |
| No engine, plain directory, depth 1, same volume | 337.3 | | |
| No engine, buffered depth 8, 8 GiB | 215.3, 215.0 | | |
| No engine, 64 MiB fsynced containers, two in flight | 196.3, 224.9 | | |
| Back-to-back 2 × 8 GiB through a 4 GiB v2 journal | — | 149.4; backlog ≤ 1.1 GiB; drain bursty 0–356 MB/s per 2 s ([series](m4-saturation-series.jsonl)) | 1.02 |

## The Linux strict or uncached CIFS case (hypothesis for the owner's controls)

Owner-measured: 32.37 MB/s through ZeroFS with a 12.1 s final flush, versus 91.13 MB/s plain onto the same P3 dataset. On the deployed v1 profile, each payload byte cost about 2.05 bytes on the SSD (journal plus clean-cache write-through, measured above), and the drain read it back from the same device. Against plain-write rates of 57–133 MB/s on that dataset, that predicts roughly 30–65 MB/s, which brackets the observed figure. The final flush must land the RAM backlog (volatile overlay up to 256 MiB plus dirty object RAM up to 128 MiB): about 384 MiB × 2.05 at about 65 MB/s is about 12 s. A depth-1 client also pays one full round trip per request; in the fixture, depth 1 still reached 117.8 (v1) and 181.7 (v2) MB/s through fsync, so per-request latency alone does not explain a drop to 32 MB/s. The fixture did not record volatile-overlay occupancy, so whether a CIFS request stream exhausts the 2,048-operation cap is unmeasured; the fixture's requests were 512 KiB, while a client issuing small writes would reach the cap at 2,048 × request size. All of this was measured through ZFS. On a raw journal device these mechanisms remain, but their rates do not carry over.

## Landing windows and their arithmetic

There are two operator-set windows: the RAM window (volatile overlay, 16–256 MiB / 1–2,048 operations, charged twice at admission, plus 128 MiB of dirty object RAM) and the SSD landing window (`disk_size_gb` under v2). For a copy of C bytes arriving at R, with the journal device sustaining S bytes per second of synchronous appends at j ≈ 1.01 device bytes per payload byte:

- RAM needed to keep acknowledging at R: about C × (1 − jS/R)⁺.
- Final FLUSH time: about C × (1 − S/R)⁺ / S.
- Through-fsync rate: about min(R, S / j). The windows move time between WRITE and FLUSH; they cannot raise the rate.
- SSD window to avoid pacing during the copy: J ≥ C × j / 0.95, bounded by the device's usable capacity minus its reserve. For 8 GiB that is about 9.1 GiB, inside v2's 16 GiB limit.

| S (journal device, sustained sync) | RAM to accept 8 GiB at 900 MB/s | Final FLUSH | Through-fsync |
|---:|---:|---:|---:|
| 100 MB/s | 7.6 GB | 76 s | 100 MB/s |
| 300 MB/s | 5.7 GB | 19 s | 300 MB/s |
| 600 MB/s | 2.9 GB | 4.8 s | 600 MB/s |
| ≥ 910 MB/s | ~0 | ~0 | 900 MB/s |

The v2 engine envelope at startup is 5.80 GB plus 64 MiB per upload worker beyond two, inside a 6.98 GB administrator limit. A multi-GiB RAM window does not fit an 8 GiB container, and by the table above it would not help the through-fsync rate anyway.

## Is 900 MB/s reachable under this contract?

Only if the journal device sustains synchronous sequential appends of at least about 910 MB/s for the whole copy (j ≈ 1.01), and the engine path keeps up. Neither is shown yet:

1. **The P3's raw sustained synchronous rate is unknown.** Every sustained P3 figure so far (57–133 MB/s) went through ZFS; drive and filesystem were never separated. The needed measurement is in the [raw journal contract](raw-journal-performance-contract.md).
2. **The engine's single-file ceiling** in these containers was about 250–265 MB/s with hardware SHA (415 MB/s with four files). It was not CPU-quota-bound. The losses found are per-file serialization of RAM-acknowledged writes, segment encryption cost, and read-back on unaligned appends. The x86 NAS CPU differs, so this needs the owner's measurement.
3. **Back-to-back copies** are bounded over time by drain to the HDDs: at most about 338 MB/s if both are written in parallel at their measured rates.

If the raw P3 sustains well below 900 MB/s, the item to replace is the journal device: an NVMe whose sustained synchronous sequential write rate exceeds 900 MB/s over at least the largest copy, preferably with power-loss protection so each flush is cheap. Faster HDD-side storage is only needed for back-to-back copies.

## Write amplification and endurance

Directory-backed, per payload byte: SSD 2.05 under v1 (journal 1.01–1.10, clean cache 1.0), 1.01–1.06 under v2; members 1.006. On a 220 TBW drive, that is about 107 TB of copied payload under v1 and about 207–218 TB under v2, before the device's own internal amplification. Under a raw backend this changes with its record and label overhead.

## Durability and correctness checks (HEAD, v2)

- SIGKILL immediately after fsync with 339 MB undrained: restart on the same journal served the correct SHA-256, drained, and a journal-free read-only verifier matched (`m5-head-crash`).
- Every run ended with a SIGKILL at the drained cut, then a fresh read-only engine with the journal parked and the cache removed verified every SHA-256.
- A tail reader re-read 8,066 recently acknowledged 512 KiB blocks during retirement with zero errors or mismatches (`m6-head-tail`). The same probe at `560eaae7` on the pre-ban host (45,366 blocks) and a readback during drain also found no EIO. **The NAS readback EIO was not reproduced.** The NAS path through FUSE and NFS was not exercised.
- Unit tests: v2 accepted and rejected geometry and the capacity check, the v1 policy unchanged, the single-pass journal read with slice corruption, the ancestry cache with a recreated directory, and parts-cache write-through disabled.

## Unmeasured

Physical NAS, raw devices, ZFS, SMB signing, NFS, FUSE, the kernel 9P client, SMR behaviour, x86 CPU cost at 900 MB/s, power loss, and SSD removal.

## Reproduction and cleanup

The [build script](build-one.sh) and [batch](batch.sh) record the exact container invocations; their paths name this run's owned directory. Diagnostic stage timers come from [this patch](diagnostic-timers-head.patch), applied only to profiling builds. Owned Docker objects (volumes, images and containers named `zsd-a24001-*`) and the owned scratch directory were removed by name after the run. Results from the pre-ban x86-64 host are cited above. Its directory `/var/tmp/zerofs-sustained-drain.W397dtuz` stays outstanding and unverified, because that host may not be accessed.
