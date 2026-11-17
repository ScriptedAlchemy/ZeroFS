# Measured package memory admission

The `small-nas-package-v1` policy changes startup arithmetic for one local-pool configuration. It does not bound queues, memtables, sessions, replay state or allocator growth. Configurations outside its explicit checks retain the fork's legacy 32 GiB residency and 8 GiB mixed process/companion reserves. The startup receipt and rejection identify the first failed configuration check.

Those fixed reserves came from fork commits `ad6835d5` and `4b2b645d`. They are absent from upstream Barre/ZeroFS main at `330ab236`. There is no upstream reserve fix to adopt. This branch changes admission arithmetic and the resulting resident-memory brake cap. PR 81 covers the planner, detector and leases without changing admission.

## Configuration and workload

The primary source is the read-only TrueNAS safety-fix template at `truenas-storage-union-safety`, revision `63d7a22`. Its ZeroFS configuration matches the main package template. The package pins ZeroFS `d498c120`. This branch uses that revision as its base.

The policy requires all of the following.

- The resolved clean caches total at most 100,663,296 bytes, or 96 MiB. Both 0.001 and 0.1 decimal GB requests resolve to that floor. The clean disk-cache request is positive and at most 0.064 decimal GB, which resolves to two existing 1 GiB disk-engine floors. Metadata warming is off.
- Writeback is disabled, so there are no writeback RAM, journal or worker allowances in this profile. Shared volatile acknowledgement is disabled. Materialized writes still use four open segment lanes and four retained seals of 256 MiB each. Admission explicitly charges their nominal 2 GiB geometry.
- There are exactly two local directory pool members, each with declared capacity at most 32 decimal GB. The metadata backend is a local directory. Member health-device monitors are absent. HA replication, NFS and NBD are off.
- 9P and RPC each use only a Unix socket. The Web UI is compiled in and has one listener at `0.0.0.0:8080`, with UID and GID 1000 and the default session timeout. The filesystem uses insensitive names, LZ4 and real fsync. LSM settings match the package defaults, GC settings match the package defaults and telemetry is off. One loopback Prometheus listener is permitted for diagnostics.

Operational qualification uses one SMB client through the ZeroFS FUSE mount, with FUSE writeback and relaxed consistency disabled. CLI jobs are at most eight. Startup cannot discover or enforce the client/job count. Increasing it leaves this workload qualification. The writeback-enabled secondary profile remains on legacy reserves.

The workload writes one 12 GiB file and 5,000 distinct 1 KiB files, reads all files back, checks every size and SHA-256, flushes and observes one additional segment GC pass. The source template declares only 1 GB per member. The fixture increases each member to 32 GB so the required 12 GiB file fits. This is a capacity deviation, not a claim that the shipped 2 GB pool can hold it. The initial baseline's intended idle command returned immediately; its raw error is retained and that idle phase is incomplete.

## Evidence and arithmetic

The baseline was built from the retained `package-baseline-source` snapshot of `bdc2f331d592a4aea9c1ae55d0b514e25e961fab`. Its source manifest is retained beside the binary as `package-baseline-ci/source-provenance.json`. All measurements run in Linux Docker on Docker Desktop, with the optimized `ci` profile and the `webui` feature. The baseline binary's SHA-256 is `7e413bc693a7edba511145fbf8eea096f97453b9ba17493dfb29dcdcedfd74f5`. Engine, FUSE and stock Debian Samba share one cgroup. The transfer client is outside it.

The completed baseline cell is `package-baseline-ci-limit64g-cache0.001-wb0-jobs8-primary`. Its raw engine samples are `b1-design/measure/package-baseline-ci-limit64g-cache0.001-wb0-jobs8-primary/samples.jsonl`. Companion samples are the adjacent `companion-samples.jsonl`. These files are retained under `/Users/zackjackson/.t3/userdata/pstack/orch/ZeroFS/`. The workload completed all integrity checks with no swap or OOM event.

| Input | Bytes | Source and profile |
| --- | ---: | --- |
| P, the maximum of engine jemalloc residency and process VmHWM | 3,241,541,632 | Completed cell above, `ci+webui`, `samples.jsonl` |
| Process VmHWM alone | 1,071,312,896 | Same cell, `ci+webui`, `samples.jsonl` |
| K, maximum `memory.stat` kernel plus sock | 137,129,984 | Same cell, `ci+webui`, `samples.jsonl`; conservatively includes companion kernel charges |
| FUSE peak sampled RSS | 46,616,576 | Same cell, `ci+webui`, `companion-samples.jsonl` |
| Sum of smbd processes at its sampled RSS peak | 68,186,112 | Same cell, stock Debian optimized Samba, `companion-samples.jsonl` |

RSS, jemalloc residency and cgroup usage are different observations. The reserve uses the larger engine observation. At the jemalloc peak, process VmRSS was 884,372 kB and whole-cgroup anonymous memory was 947,466,240 bytes. Jemalloc residency was about 3.4 times that anonymous memory. It is an allocator statistic that can include untouched pages, not a measurement of physical process RSS. The brake watches that same allocator statistic. The samples cannot attribute resident segment-buffer pages separately, so the engine reserve conservatively retains the entire observed engine peak in addition to the nominal configured tiers. This avoids an unsupported subtraction of nominal capacity from measured residency.

Run `python3 scripts/derive-small-nas-memory.py <completed-cell-directory>` to recompute the inputs and reserves from the retained receipt and samples.

The formula uses integer bytes and rounds each derived reserve upward to a multiple of 64 MiB.

```text
C = resolved clean cache bytes
D = (4 open lanes + 4 retained seals) * 256 MiB = 2 GiB
R = ceil64MiB(1.10 * P) = 3,623,878,656 bytes
H = ceil64MiB(0.15 * P + 1.25 * K) = 671,088,640 bytes
required = C + D + R + H = 6,543,114,240 bytes = 6.09375 GiB
brake cap = selected engine limit - H
```

R is the engine-internal residency allowance. H leaves kernel and allocator headroom below the engine limit. Their percentage margins are a policy assumption over completed workload measurements, not an observed bound. The 64 MiB rounding unit adds margin and is not a global floor. Neither reserve includes FUSE or smbd userspace RSS or file cache. Outside this configuration the legacy arithmetic and brake threshold remain unchanged.

At the exact admitted engine minimum the brake cap is 5.46875 GiB. It exceeds P plus C and D. With a 6.5 GiB engine budget the cap is 5.875 GiB. Subtracting both reserves would instead place the threshold near the charged tiers alone, below normal engine residency. Runtime overrides `unmodeled_residency_reserve_gb` and `engine_headroom_gb` must be finite positive decimal GB values at least as large as the selected policy's computed minima. There is no admission-disable flag.

## Whole-container allocation

A finite current-cgroup `memory.max` supplies a dedicated engine envelope in the existing detector. The detector does not enumerate other processes sharing that cgroup. The app must therefore set `[runtime] memory_limit_gb` below the container limit when FUSE and Samba share it. The smaller explicit engine budget wins.

| Container limit | Engine budget | FUSE and Samba allowance | File-cache allowance | Remaining margin | Status of allocation |
| --- | ---: | ---: | ---: | ---: | --- |
| 4 GiB | 2.5 GiB | 0.25 GiB | 1 GiB | 0.25 GiB | Assumed partition; engine admission refuses it |
| 8 GiB | 6.5 GiB | 0.25 GiB | 1 GiB | 0.25 GiB | Assumed partition for runtime qualification |

The 0.25 GiB companion allocation exceeds twice the separate sampled RSS peaks above. The 1 GiB file-cache allowance is an unmeasured allocation assumption and requires reclaim during the streaming workload; it is not a cap. The baseline file cache exceeded it. The engine brake observes jemalloc residency and does not observe cgroup file cache or `memory.high`. Runtime qualification must report both file and anon at the same sampled cgroup peak, plus `memory.events`. The engine budget includes H already. Do not add H or the companion processes again when summing this table.

A 6.5 GiB engine budget is `memory_limit_gb = 6.979321856` in decimal GB. With this 1.5 GiB external allocation, the arithmetic whole-app minimum is 7.59375 GiB. This is an admission minimum, not proof that every workload fits. The final runtime report records the actual tested limits and any rejection or kill.

## Host assumptions and limits

The coordinator reports a persistent 8 GiB ARC cap on the 29.158 GiB NAS, with no swap. We assume a separate 12 GiB host allowance for the kernel, ZFS outside ARC and Plex, qBittorrent, Ultra Explorer, Tailscale and MakeMKV. It consists of an assumed 8 GiB of working sets and 4 GiB for burst activity and safety. Neither amount was measured. An 8 GiB app limit leaves about 1.158 GiB beyond that assumption and the ARC cap. The 7.59375 GiB arithmetic whole-app minimum leaves about 1.565 GiB. These are arithmetic remainders, not verified free memory during Plex transcodes or MakeMKV activity.

The Web UI assets were built in Linux from this branch, including its WASM client, without the terminal VM image. The package pins `d498c120`. The embedded assets are static files served on request. Any memory difference from the package's original assets is assumed small and was not measured.

- Not verified: NAS operation, HDD-backed members, bursty co-tenants, long uptimes, more than one SMB client, increased CLI concurrency, active Web UI browsing or terminal sessions, and a bound on the remaining unbounded engine consumers. The workload receipts, final-commit checks, forced-kill recovery and cleanup inventory are reported separately under `b1-design/measure/report.md`.
