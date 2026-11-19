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

This branch removes the UID and GID checks because these numeric authorization identities allocate no additional server or queue. It removes the Web UI bind address and port checks because the same single listener serves them. It removes the Prometheus bind address check for the same reason. Listener counts and session timeout remain checked because they can change service and retained-session memory. The compiled Web UI check remains because it constructs an HTTP router, an RPC-web service, upload-admission state and WebSocket 9P state. Private Unix RPC and 9P start independently. The feature memory delta was not isolated numerically.

One SMB client uses the FUSE mount with FUSE writeback and relaxed consistency off. CLI jobs are limited operationally to eight. Admission cannot discover either client count or CLI job count. Concurrent clients, CLI transfers and active browser or terminal sessions are outside the measured workload.

## Stored-state coverage

The receipt and `zerofs memory-status -c config.toml` expose `stored_state_qualification` with 53,738,291,200 logical bytes and 50,039 regular files. They also expose `enforcement = "empirical_not_enforced"`. The largest completed growth rung retained 50 GiB in 39 large files plus 50,000 distinct 1 KiB files on four declared 18 TB members. Three independent mixed 6 TB and 18 TB runs each verified 12 GiB plus 5,000 small files. The larger coverage is not a separate test of every member layout. This is an empirical coverage statement, not a runtime growth limit. Queues, memtables, sessions, replay and allocator growth have no new hard bound.

The optimized `ci+webui` binary at `b190e399` completed those growth rungs under an 8 GiB cgroup with no app swap or OOM kills. The retained report under `b1-design/widen/report.md` names each exact artifact, final-commit replay, command and incomplete phase. Capacity declarations do not establish coverage for 18 TB of stored data. The 150 GiB and 200,000-file rung was not started. Its predeclared scheduling projection was 9,472.57 seconds, above the 7,200-second rung deadline.

The predeclared ladder is 12 GiB with 5,000 small files, 50 GiB with 50,000, and 150 GiB with 200,000. Each completed rung requires full SHA-256 verification, flush and a subsequent GC pass. The top completed rung gets restart and full verification. A separate SIGKILL check verifies every file flushed before the kill. A rung stops after two hours or before starting if less than 80 GiB of backing disk remains.

Three smallest-rung runs on fresh stores with the same committed optimized binary under 8 GiB determine repeat spread. Noise is the larger of 128 MiB or 5 percent of their median P. The report retains every maximum. Spread above that threshold prevents a stable slope or plateau claim. Growth above 128 MiB per additional 50,000 files beyond noise requires a positive-slope warning. A recomputed requirement above the 6.5 GiB engine budget fails the approved 8 GiB partition. Margins are not reduced to fit it.

## Evidence and allocator accounting

All ZeroFS execution and Rust checks run in Linux. The original completed optimized `ci+webui` cell is `b1-design/measure/package-baseline-ci-limit64g-cache0.001-wb0-jobs8-primary`. Its raw `samples.jsonl` and `companion-samples.jsonl` are retained under that cell label. The source snapshot was `bdc2f331d592a4aea9c1ae55d0b514e25e961fab`, and the binary hash was `7e413bc693a7edba511145fbf8eea096f97453b9ba17493dfb29dcdcedfd74f5`.

| Completed workload observation | Bytes | Repeats, spread and limiter |
| --- | ---: | --- |
| Original P, maximum of sampled jemalloc residency and VmHWM, `ci+webui` | 3,241,541,632 | One cell. Repeat spread unavailable. The old counters did not identify the allocation classes. This remains the conservative policy input. |
| Original K, maximum cgroup kernel plus sock, `ci+webui` | 137,129,984 | One cell. Repeat spread unavailable. Cgroup kernel accounting includes the companions' kernel charges. |
| Native mixed 6 TB and 18 TB bootstrap P, `ci+webui` | 2,681,294,848 | One cell with resumed readback after a driver failure. Repeat spread unavailable. At P, active pages were 2,615,820,288 bytes, large live allocations were 2,568,060,928 bytes, metadata was 51,793,024 bytes and dirty pages were 14,815,232 bytes. |
| Native bootstrap K, `ci+webui` | 79,462,400 | Same single cell. Repeat spread unavailable. The cgroup charged kernel memory, not declared capacity, limits this observation. |
| Native 8 GiB mixed-member P repeats, `ci+webui` | 2,855,235,584; 3,041,099,776; 2,871,488,512 | Three fresh stores. Spread 185,864,192 exceeds noise 143,574,425.6. Eight live 256 MiB allocations at each peak dominate active demand. Dirty-page variation explains most spread. No stable slope or plateau claim. |
| Native 8 GiB medium P, `ci+webui` | 2,781,118,464 | One growth run. Repeat spread unavailable. Eight live 256 MiB allocations, 2,708,574,208 active bytes, 13,012,992 dirty bytes and 60,851,616 metadata bytes at P. |
| Native 8 GiB medium K, `ci+webui` | 111,304,704 | Same single growth run. Repeat spread unavailable. Maximum charged cgroup kernel plus sock across the workload. |

The native raw file is `b1-design/widen/bootstrap-small-1-retry/samples.jsonl`. The adjacent outcome history retains the failed setup and driver stages. Full verification subsequently matched all 5,001 files after restart, and a GC pass completed. Raw-parts cache usage reached its 33,554,432-byte capacity, and decoded-block usage reached 32,848,357 bytes against the same capacity. The work and cache were not skipped. This cell overlapped other agents' containers and host PSI and swap activity. It is not an uncontaminated repeat.

At the original P, whole-cgroup anonymous memory was 947,466,240 bytes, about 3.4 times smaller than P. Jemalloc residency includes active pages that need not be physically touched. It is an allocator accounting upper bound, not physical RSS. Active large allocations, rather than dirty retention, also dominate the native peak. The added aggregate-arena page and size-class counters distinguish live allocation classes, metadata and retained dirty pages. They do not by themselves identify every Rust owner or prove that future stored state cannot double demand. Each qualified rung must retain its peak decomposition and name any attribution that remains unresolved.

`python3 scripts/derive-small-nas-memory.py <completed-cell> ...` checks completion, integrity, GC, swap and OOM receipts before taking the largest P and K across supplied cells. Native manifests must cover at least the smallest qualified workload: one file of at least 12 GiB and at least 5,000 additional files of at least 1 KiB each. Larger ladder rungs remain valid inputs; empty, tiny or bulk-only controls cannot justify the package reserves. Combining the original cell with the completed native bootstrap keeps the higher original inputs. The range between different cells is not a repeat-noise estimate. The completed medium raw samples are `b1-design/widen/candidate-four-medium/samples.jsonl`. `ladder-derived.json` retains the original larger P and K after combining the original workload, the three small repeats and the medium workload. All native runs overlapped other work or host swap and PSI activity. They are disturbed, and cannot establish an uncontaminated stored-state slope.

At the medium peak, eight live 256 MiB allocation classes account for 2 GiB of active demand. Their count matches the direct segment geometry. Sixteen such allocations would require another 2 GiB, which was not observed. The counters do not identify all Rust owners or prohibit that demand in another workload. There is no universal proof of why total P could not double. Mapped and retained virtual bytes are reported separately and are not the resident policy input. Raw-parts usage reached 33,554,432 bytes and decoded-block usage reached 33,490,011 of 33,554,432 bytes. The third extent-cache fill is not directly exposed. Admission charges its full configured share regardless. Every retained file was fetched and SHA-verified, so an unfilled dataset or skipped readback does not explain the low peak.

Medium cgroup `memory.peak` reached 8,589,934,592 bytes with 1,006 `max` events. At its largest sampled `memory.current`, file was 7,526,813,696 bytes and anon was 944,189,440 bytes. These are paired values from one sample. The allocator cap was 6,308,233,216 bytes and the brake recorded zero trips. File cache, outside that brake, drove reclaim at the hard limit. This is not evidence for brake release under pressure or a reserved 0.25 GiB physical margin.

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
| 8 GiB | 6.5 GiB | 0.25 GiB | 1 GiB | 0.25 GiB | Assumed partition. Bounded 8 GiB workloads passed with file reclaim; this split is not enforced. |

The native bootstrap's separate sampled peaks were 42,332,160 bytes for FUSE with `ci+webui` and 64,716,800 bytes summed across stock Debian smbd processes. Each is a single-cell observation with repeat spread unavailable. The 0.25 GiB userspace allowance exceeds twice that bootstrap sum. The larger completed medium cell sampled separate FUSE and summed smbd peaks of 101,191,680 and 64,274,432 bytes with `ci+webui`, one run with repeat spread unavailable. The allowance exceeds their sum by 102,969,344 bytes. This is an assumed margin, not a companion memory limit. The 1 GiB file-cache allowance requires reclaim and is not an enforced cache cap. The bootstrap retained far more file cache under 64 GiB. A successful limited run can still reach `memory.max` through reclaimable file pages. Its actual remaining cgroup margin must be reported separately from this planning table. The allocator brake does not watch file cache or `memory.high`.

Set `memory_limit_gb = 6.979321856` for the 6.5 GiB engine budget inside an 8 GiB app. The engine arithmetic minimum is 6.09375 GiB. Adding the assumed 1.5 GiB external allocation gives a whole-app planning minimum of 7.59375 GiB. No workload pass is implied by either arithmetic minimum. H already includes the observed cgroup kernel charge and is counted once.

The package owner's snapshot at 2026-11-17T01:50Z reports TrueNAS 27.0.0-RC.1, kernel 6.18.52, MemTotal 30,571,416 kB, MemAvailable 20,206,776 kB, an 8 GiB full persistent ARC cap and five running apps. Total RAM is 29.155 GiB. Assume 12 GiB for the kernel, ZFS outside ARC and Plex, qBittorrent, Ultra Explorer, Tailscale and MakeMKV. This unmeasured assumption allocates 8 GiB to working sets and 4 GiB to bursts and safety. With ARC and an 8 GiB app, the arithmetic remainder is 1.155 GiB. The 7.59375 GiB planning minimum leaves 1.561 GiB. Available memory at one snapshot is not all available for ZeroFS, and neither remainder is a NAS measurement under load.

The Linux members are plain directories with declared quotas. Metadata, catalog and cache use a different directory tree from the data members on the same physical filesystem. They do not emulate separate disks or NVMe latency. Local free-space probes see the real smaller backing filesystem. The medium pool status declared four members at 18,000,000,000,000 bytes each but reported only 142,623,799,680 available backing bytes per member. All 293 mapped objects were on member A. This qualifies declaration metadata and stored-state growth, not simultaneous I/O across several data disks. ZFS-backed members are not measured. NAS ARC is outside the engine and container accounting and must remain in host budgeting.

Web UI assets were built in Linux from this branch with its WASM client and without the terminal VM image. They are static files served on request. Their memory difference from the package's pinned assets is assumed small and unmeasured.

## Qualifying writeback later

Writeback remains on legacy reserves. A future envelope must charge its explicit resolved RAM budget, both volatile acknowledgement stages if enabled, journal-index and replay memory, local and upload worker concurrency, queued object payloads, and allocator overhead. It must measure sustained writes larger than the container limit, a near-full bounded journal and drain, exact readback, GC and crash replay under the selected app limit. The formula can then use those completed workload peaks and keep the external FUSE, Samba and file-cache allowance separate. Merely setting a writeback RAM budget is insufficient evidence for those additional consumers.

- Not verified: A 4 GiB workload did not run because admission refuses that limit. The 150 GiB rung did not run. This document also does not establish full 18 TB stored state, NAS operation, ZFS backing, separate physical data disks and NVMe, bursty co-tenants, long uptime, more than one SMB client, concurrent CLI transfers, active browser or terminal sessions, or hard bounds on the remaining unbounded consumers.

The widening report records the final Linux checks, recovery and cleanup.
