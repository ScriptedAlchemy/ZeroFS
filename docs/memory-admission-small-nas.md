# Measured package memory admission

The `small-nas-package-v1` policy changes admission for a bounded local-pool package configuration. Other configurations retain the fork's 32 GiB residency and 8 GiB mixed process and companion reserves. The receipt and rejection name the first failed configuration check. No setting disables admission.

The reserves originated in fork commits `ad6835d5` and `4b2b645d`, and are absent upstream at Barre/ZeroFS `330ab236`. This branch changes admission arithmetic and its allocator brake. PR 81 changes the planner, detector and leases without changing admission.

## Admitted configuration

The package configuration comes from the read-only safety template at `truenas-storage-union-safety 63d7a22`. The package owner now pins PR 84 head `734a4a7e`, with `80d1b1cf` as fallback. This widening builds on that fallback. Qualification applies to the exact tested branch binary, not automatically to the package's other pinned revision.

The configuration checks require the following.

- Clean caches resolve to at most 100,663,296 bytes. Requests of 0.001 or 0.1 decimal GB resolve to the same 96 MiB floor. The clean disk-cache request is positive and at most 0.064 decimal GB. Its two existing disk-engine floors are 1 GiB each. Metadata warming is off.
- Writeback and shared volatile acknowledgement are off. Materialized writes retain four open segment lanes and up to four in-flight seals. Admission charges their nominal eight times 256 MiB geometry separately from the observed whole-engine peak.
- Two through four local directory members each declare at most 18,000 decimal GB, or 18,000,000,000,000 bytes. Metadata is local. Member health monitors and SFTP overrides are absent. Replication, NFS and NBD are off.
- 9P and RPC use private Unix sockets. Web UI support is compiled in, with one listener and the measured 120-second session timeout. Names are insensitive, compression is LZ4 and fsync is enabled. LSM and GC use package defaults. Telemetry is off. Prometheus has at most one diagnostic listener.

UID and GID checks were removed because these numeric authorization identities allocate no additional server or queue. Web UI bind address and port checks were removed because the same single listener serves them. The Prometheus bind address check was removed for the same reason. Listener counts and session timeout remain checked because they can change service and retained-session memory. The compiled Web UI check remains because it determines whether HTTP, RPC and 9P services are actually started.

One SMB client uses the FUSE mount with FUSE writeback and relaxed consistency off. CLI jobs are limited operationally to eight. Admission cannot discover either client count or CLI job count. Concurrent clients, CLI transfers and active browser or terminal sessions are outside the measured workload.

## Stored-state coverage

The current receipt and `zerofs memory-status -c config.toml` expose `stored_state_qualification` with 12,890,021,888 logical bytes and 5,001 regular files. They also expose `enforcement = "empirical_not_enforced"`. This is the completed 12 GiB sequential file plus 5,000 distinct 1 KiB files. It is an empirical coverage statement, not a runtime growth limit. Queues, memtables, sessions, replay and allocator growth have no new hard bound.

Widened runtime qualification is in progress. A configuration admitted by the widened checks is not yet an 8 GiB runtime pass. The retained report under `b1-design/widen/report.md` records each completed, failed or incomplete rung. Capacity declarations do not establish coverage for 18 TB of stored data.

The predeclared ladder is 12 GiB with 5,000 small files, 50 GiB with 50,000, and 150 GiB with 200,000. Each completed rung requires full SHA-256 verification, flush and a subsequent GC pass. The top completed rung gets restart and full verification. A separate SIGKILL check verifies every file flushed before the kill. A rung stops after two hours or before starting if less than 80 GiB of backing disk remains.

Three independent clean smallest-rung runs of the same committed optimized binary under 8 GiB determine repeat spread. Noise is the larger of 128 MiB or 5 percent of their median P. The report retains every maximum. Spread above that threshold prevents a stable slope or plateau claim. Growth above 128 MiB per additional 50,000 files beyond noise requires a positive-slope warning. A recomputed requirement above the 6.5 GiB engine budget fails the approved 8 GiB partition. Margins are not reduced to fit it.

## Evidence and allocator accounting

All ZeroFS execution and Rust checks run in Linux. The original completed optimized `ci+webui` cell is `b1-design/measure/package-baseline-ci-limit64g-cache0.001-wb0-jobs8-primary`. Its raw `samples.jsonl` and `companion-samples.jsonl` are retained under `/Users/zackjackson/.t3/userdata/pstack/orch/ZeroFS/`. The source snapshot was `bdc2f331d592a4aea9c1ae55d0b514e25e961fab`, and the binary hash was `7e413bc693a7edba511145fbf8eea096f97453b9ba17493dfb29dcdcedfd74f5`.

| Completed workload observation | Bytes | Repeats, spread and limiter |
| --- | ---: | --- |
| Original P, maximum of sampled jemalloc residency and VmHWM, `ci+webui` | 3,241,541,632 | One cell. Repeat spread unavailable. The old counters did not identify the allocation classes. This remains the conservative policy input. |
| Original K, maximum cgroup kernel plus sock, `ci+webui` | 137,129,984 | One cell. Repeat spread unavailable. Cgroup kernel accounting includes the companions' kernel charges. |
| Native mixed 6 TB and 18 TB bootstrap P, `ci+webui` | 2,681,294,848 | One cell with resumed readback after a driver failure. Repeat spread unavailable. At P, active pages were 2,615,820,288 bytes, large live allocations were 2,568,060,928 bytes, metadata was 51,793,024 bytes and dirty pages were 14,815,232 bytes. |
| Native bootstrap K, `ci+webui` | 79,462,400 | Same single cell. Repeat spread unavailable. The cgroup charged kernel memory, not declared capacity, limits this observation. |

The native raw file is `b1-design/widen/bootstrap-small-1-retry/samples.jsonl`. The adjacent outcome history retains the failed setup and driver stages. Full verification subsequently matched all 5,001 files after restart, and a GC pass completed. Raw-parts cache usage reached its 33,554,432-byte capacity, and decoded-block usage reached 32,848,357 bytes against the same capacity. The work and cache were not skipped. This cell overlapped other agents' containers and host PSI and swap activity. It is not an uncontaminated repeat.

At the original P, whole-cgroup anonymous memory was 947,466,240 bytes, about 3.4 times smaller than P. Jemalloc residency includes active pages that need not be physically touched. It is an allocator accounting upper bound, not physical RSS. The native peak is also dominated by active large allocations, rather than dirty retention. The added aggregate-arena page and size-class counters distinguish live allocation classes, metadata and retained dirty pages. They do not by themselves identify every Rust owner or prove that future stored state cannot double demand. Each qualified rung must retain its peak decomposition and name any attribution that remains unresolved.

`python3 scripts/derive-small-nas-memory.py <completed-cell> ...` checks completion, integrity, GC, swap and OOM receipts before taking the largest P and K across supplied cells. Native manifests must cover at least the smallest qualified workload: one file of at least 12 GiB and at least 5,000 additional files of at least 1 KiB each. Larger ladder rungs remain valid inputs; empty, tiny or bulk-only controls cannot justify the package reserves. Combining the original cell with the completed native bootstrap keeps the higher original inputs. The range between different cells is not a repeat-noise estimate.

## Formula and brake

All terms use integer bytes. R and H round upward to a multiple of 64 MiB.

```text
C = resolved clean cache bytes
D = (4 open lanes + 4 retained seals) * 256 MiB = 2 GiB nominal
P = max(sampled engine jemalloc residency, process VmHWM)
K = max(cgroup memory.stat kernel + sock)
R = ceil64MiB(1.10 * P) = 3,623,878,656 bytes
H = ceil64MiB(0.15 * P + 1.25 * K) = 671,088,640 bytes
required = C + D + R + H = 6,543,114,240 bytes = 6.09375 GiB
brake cap = selected engine limit - H
```

R retains the entire observed engine peak in addition to the charged tiers. No nominal segment capacity is subtracted from P. H leaves kernel and allocator headroom. The percentages and rounding are explicit policy margins over measured workload peaks. They are not measured bounds or global floors. FUSE and smbd userspace RSS and file cache remain outside the engine allowance.

At the exact engine minimum the brake cap is 5.46875 GiB. At the approved 6.5 GiB engine budget it is 5.875 GiB. Both include normal measured residency plus the configured tiers. Outside the profile, the legacy cap remains limit minus both legacy reserves. Upward overrides `unmodeled_residency_reserve_gb` and `engine_headroom_gb` cannot fall below the selected computed values.

## Whole-container and host allocation

The detector treats its current finite cgroup limit as an engine envelope without enumerating other processes. When engine, FUSE and Samba share one cgroup, the app must subtract external allowances before setting `[runtime] memory_limit_gb`. The smaller explicit budget wins.

| Container limit | Engine budget, including R and H | FUSE and Samba userspace | Charged file-cache planning allowance | Planned remaining margin | Evidence label |
| --- | ---: | ---: | ---: | ---: | --- |
| 4 GiB | 2.5 GiB | 0.25 GiB | 1 GiB | 0.25 GiB | Assumed partition. Engine admission refuses it. |
| 8 GiB | 6.5 GiB | 0.25 GiB | 1 GiB | 0.25 GiB | Assumed partition. Widened runtime qualification pending. |

The native bootstrap's separate sampled peaks were 42,332,160 bytes for FUSE with `ci+webui` and 64,716,800 bytes summed across stock Debian smbd processes. Each is a single-cell observation with repeat spread unavailable. The 0.25 GiB userspace allowance exceeds twice their sum. The 1 GiB file-cache allowance requires reclaim and is not an enforced cache cap. The bootstrap retained far more file cache under 64 GiB. A successful limited run can still reach `memory.max` through reclaimable file pages. Its actual remaining cgroup margin must be reported separately from this planning table. The allocator brake does not watch file cache or `memory.high`.

Set `memory_limit_gb = 6.979321856` for the 6.5 GiB engine budget inside an 8 GiB app. The engine arithmetic minimum is 6.09375 GiB. Adding the assumed 1.5 GiB external allocation gives a whole-app planning minimum of 7.59375 GiB. No workload pass is implied by either arithmetic minimum. H already includes the observed cgroup kernel charge and is counted once.

The package owner's snapshot at 2026-11-17T01:50Z reports TrueNAS 27.0.0-RC.1, kernel 6.18.52, MemTotal 30,571,416 kB, MemAvailable 20,206,776 kB, an 8 GiB full persistent ARC cap and five running apps. Total RAM is 29.155 GiB. Assume 12 GiB for the kernel, ZFS outside ARC and Plex, qBittorrent, Ultra Explorer, Tailscale and MakeMKV. This unmeasured assumption allocates 8 GiB to working sets and 4 GiB to bursts and safety. With ARC and an 8 GiB app, the arithmetic remainder is 1.155 GiB. The 7.59375 GiB planning minimum leaves 1.561 GiB. Available memory at one snapshot is not all available for ZeroFS, and neither remainder is a NAS measurement under load.

The Linux members are plain directories with declared quotas. Metadata, catalog and cache use a different directory tree from the data members on the same physical filesystem. They do not emulate separate disks or NVMe latency. Local free-space probes see the real smaller backing filesystem. ZFS-backed members are not measured. NAS ARC is outside the engine and container accounting and must remain in host budgeting.

Web UI assets were built in Linux from this branch with its WASM client and without the terminal VM image. They are static files served on request. Their memory difference from the package's pinned assets is assumed small and unmeasured.

## Qualifying writeback later

Writeback remains on legacy reserves. A future envelope must charge its explicit resolved RAM budget, both volatile acknowledgement stages if enabled, journal-index and replay memory, local and upload worker concurrency, queued object payloads, and allocator overhead. It must measure sustained writes larger than the container limit, a near-full bounded journal and drain, exact readback, GC and crash replay under the selected app limit. The formula can then use those completed workload peaks and keep the external FUSE, Samba and file-cache allowance separate. Merely setting a writeback RAM budget is insufficient evidence for those additional consumers.

- Not verified: widened 8 GiB qualification until the rung receipts pass, full 18 TB stored state, NAS operation, ZFS backing, separate physical data disks and NVMe, bursty co-tenants, long uptime, more than one SMB client, concurrent CLI transfers, active browser or terminal sessions, and hard bounds on remaining unbounded consumers. Final Linux checks, recovery and cleanup are recorded in the widening report.
