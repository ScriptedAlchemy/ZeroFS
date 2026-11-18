# Bounded NAS journal qualification

Engine source `bf1a9fb5319fbf3f01f94b8ea881d940767b3588` passed native file fsync and authenticated NVMe/TCP crash recovery on ubuntu-main. The engine executable SHA-256 is `a4318f24cae7b6f807f68711efb9ac5003fe85625d5e3265bbb68899cb201b11`. The base is PR #84 at `faddddc08151b9153dca6c74ffa7deaa0ed412cd`. The running physical trial uses the separate released engine at `27e72993`. [Build identity](build-id.json)

## App inputs

Use the bounded profile only for the operator-selected journal mode. Select `[runtime] profile = "small-nas-journal-v1"`, enable the independent writeback directory, and set `[storage.pool] segment_placement = "round_robin_local"` for two-disk distribution. The [writer template](../../../scripts/fixtures/nas-journal/engine.toml.in) has every tested field. Omit the selector and writeback table for the direct path. The [read-only verifier template](../../../scripts/fixtures/nas-journal/verify.toml.in) targets settled destinations. The physical trial uses FUSE for file semantics. Keep FUSE's client write cache disabled.

| Resource or policy | Qualified setting |
| --- | --- |
| Engine envelope | 6,979,321,856 B. Startup accounts for 5,804,916,736 B. The extra 1 GiB worker/replay allowance is an engineering reserve. Stored-state qualification is unset for this profile. |
| Foreground write | Filesystem materialization, with RAM acknowledgement at object-store admission. The client pays the filesystem, encryption and protocol cost. Dirty object RAM is capped at 134,217,728 B. |
| Sealing | Four open lanes, 32 MiB target per segment and two retained seals. The admission charge for segment geometry is 201,326,592 B. |
| SSD journal | 536,870,912 B of admission reservations, 95% high and 85% resume watermarks, at least 67,108,864 B free. One local journal worker. A successful file fsync waits for the accepted local sequence. |
| Destaging | Two upload workers. A catalog transaction advances the local member rotation cursor with each eligible immutable-segment reservation. Each segment lives on one device. Sizes can differ, and file boundaries can cross segments. Existing data stays in place. |
| Clean shutdown | `shutdown_flush = "local"`. Database close can append a new durable manifest after the last observed remote drain. Keep the same journal through restart. |

Direct-to-HDD remains available for bulk writes. The selected journal receives the dirty object traffic for that engine. Per-file bypass, automatic media classification and configurable persistent hot-file pinning have no qualification in this profile. The storage UI exposes diagnostics and scan/refresh. Changing these policies currently requires application configuration. Journal sequence and admission gauges are available in Prometheus.

Before app integration, bind and verify the dedicated persistent P3 journal volume by its expected mount identity. Keep it separate from disposable cache and the catalog, authority and file exports. Require the same journal on every writer restart. Treat an unexpected empty mount, switched backend or uncertain pending journal as a startup refusal. For a mode change, quiesce all writers, retain the journal, and verify the final remote sequence and the destination data through a separate reader. An ordinary local shutdown alone cannot authorize journal removal. Until drain a P3 failure can lose the only durable copy of an acknowledged mutation.

## Durability, pressure and device participation

| Synthetic gate | Native result |
| --- | --- |
| Full members, local fsync, SIGKILL | Six distinct 36 MiB files. Both members were below reserve throughout the first recovery. Fresh-cache hashes matched through the journal. After drain a second SIGKILL and fresh read-only process verified every hash with the original journal and both writer caches absent. [Replay](replay.json) |
| Full journal and one-member recovery | With both destinations below reserve, admission paced at 503,115,610 reserved bytes. The already-issued fsync for file 12 stayed blocked while 11 acknowledged files remained readable. The same fsync completed 350.4 ms after releasing A, with B still below reserve. After releasing B all 17 hashes matched through a journal-free read-only verifier. The prior passing run measured 307.1 ms. [Pressure](pressure.json) · [Both runs](native-summary.json) |
| Concurrent disk-backed drain | Six concurrent clients wrote 216 MiB. Member A gained 10 objects / 121,262,199 B and B gained 9 / 133,554,263 B. A single kernel inotify queue recorded eight overlaps between writable staging file lifetimes on different members. Both directories share one ext4 device. [Events and hashes](disk.json) |
| Authenticated NVMe/TCP | Independent 4 KiB FUA WRITE and WRITE + FLUSH cutoffs. SIGKILL after each success, same-UUID initiator reconnect, exact whole-image and sector checks, and a second verification after remote drain. Single observations were 9.695 ms for FUA, 8.778 ms for normal WRITE, 0.335 ms for its subsequent FLUSH. The pinned target fsyncs normal writes too. [Target, initiator, refusals and receipts](../../../scripts/nas-journal-nvme/README.md) |
| Admission and degraded destinations | 25 focused tests passed, including uneven historical occupancy, concurrent reservations, full or quarantined member reselection, and permanent direct quarantine errors. Base writeback admission required 43,184,553,984 B and refused the 6.5 GiB envelope. Missing-device cold start and metadata survival remain separate product gates. [Tests](unit/) · [Base refusal](base-refusal) · [Selected admission](admission.json) |

## Rates and capacity

Rates below use decimal MB/s unless indicated. On the shared ext4 backing, six concurrent local 9P clients reached **248.1 MB/s** through their last WRITE response, **190.1 MB/s** through their last fsync, and **114.1 MB/s** through observation of a fully drained remote sequence. Each uses 216 MiB of logical file data and elapsed wall time from the first WRITE. Source generation and the warm-up files are outside those intervals. In the sequential, reserve-held case, six 36 MiB fsyncs ranged from 40.8 to 167.1 ms, median 53.0 ms. These tests used the optimized CI engine binary.

In the normal unoptimized Rust test profile, 64 distinct 1 MiB objects averaged 58.39 MiB/s through RAM acceptance and 58.11 MiB/s through local-journal durability. Two writers, two readers and one journal worker were used. The paused in-memory destination and buffered journal reads scope this measurement to the object layer. [Microbenchmark](tier-bench.txt)

The deployment owner measured 9.41 Gb/s Studio-to-NAS and 8.88 Gb/s reverse with iperf. Physical QD1 direct file writes including end fsync achieved 198.9 MB/s on the P3, and concurrent writes to the two HDDs achieved 134.6 and 203.6 MB/s. The live direct trial measured 66.7 / 68.8 MB/s signed SMB / NFS including final flush, and 0.77 / 1.52 MB/s NVMe write-through at QD1 / QD8 including the SPDK RPC client. A 10GbE application rate and a physical P3 or two-HDD speedup remain unverified. At the measured 9.41 Gb/s ingress, the 95% journal watermark covers about 0.43 seconds of payload with zero drain, before record overhead.

| Allocated regular-file bytes | At paced boundary | After both destinations settled |
| --- | ---: | ---: |
| Raw-parts clean cache | 483,090,432 | 740,929,536 |
| Decoded-block clean cache | 0 | 0 |
| Journal payload blobs | 501,510,144 | 0 |
| Journal redb | 114,688 | 94,208 |
| Catalog | 57,344 | 106,496 |
| Metadata authority | 45,056 | 1,380,352 |

The source divides the configured clean disk cache into two stores with **1 GiB floors each**, including when `cache.disk_size_gb = 0.064`. The physical cache dataset reached 786,132,992 B used by dataset, zero by snapshots, up from 143,360 B at initialization. The native table sums `st_blocks × 512` by role. ZFS dataset accounting includes filesystem costs beyond regular-file blocks. [Component samples](pressure.json)

For a finite trial, budget the two 1 GiB cache regions and their filesystem overhead independently of the journal. One candidate journal dataset quota is 1 GiB, comprising 512 MiB admission, 64 MiB free floor and 448 MiB margin for redb growth, allocation and filesystem costs. That quota is a proposed **physical** bound requiring ZFS qualification. The engine's admission ledger limits logical dirty charges. Budget authority, catalog, Samba/config/state, ZFS pool free reserve and any snapshots separately. The app owner must measure per-dataset usage and free space under pressure on ZFS. Sustained ingestion at 100 MB/s corresponds to 8.64 TB/day of logical writes before journal, cache and SSD write amplification. P3 endurance and shared pool space constrain how often to select this mode. Two bounded uploaders emit large independent segments, while long-duration SMR behavior and media write amplification require physical measurements.

The replay, pressure and disk-backed clients ran with their engine inside an 8 GiB, zero-swap, two-CPU cgroup. The largest recorded peak was 2,355,306,496 B, including client, temporary member files and cache. Every measured memory event and OOM count was zero. NVMe used a separate target/initiator fixture. The released aggregate SMB/NFS/NVMe package, host ARC, sustained reserve, ZFS quota exhaustion, power loss and SSD removal have no acceptance receipt for this new profile. [Cgroup receipts and early failures](native-summary.json)

## Reproduction and cleanup

Build on Linux from the engine source above with the matching WebUI dist and the command in the build identity. Run the following fixture entry points inside the two-CPU cgroup. Each requires its own new evidence directory, the candidate binary, both supplied config templates, and a private scratch directory.

| Fixture | Member roots |
| --- | --- |
| `scripts/test-nas-journal.py` | Two empty dedicated 256 MiB tmpfs mounts |
| `scripts/test-nas-journal-pressure.py` | Two empty dedicated 384 MiB tmpfs mounts |
| `scripts/test-nas-journal-file-drain.py` | Both parent arguments equal the dedicated ext4 scratch bind at `/scratch`. The fixture creates separate member subdirectories. |

For the normal-profile microbenchmark use `ZEROFS_BENCH_DIR` set to the canonical absolute private scratch path, `ZEROFS_BENCH_TIER_TOTAL_MIB=64`, `ZEROFS_BENCH_TIER_PAYLOAD_KIB=1024`, `ZEROFS_BENCH_TIER_WRITERS=2`, `ZEROFS_BENCH_TIER_READERS=2`, and `ZEROFS_BENCH_LOCAL_CONCURRENCY=1`. Run `cargo test --locked --no-default-features --features webui --lib writeback::tier_bench::bench_writeback_local_tier_read_write -- --exact --ignored --nocapture`.

Removed the verified owned native root `/var/tmp/zerofs-nas-journal.1XHScyY3`, with its compiler outputs, Cargo download cache, staged protobuf tools, extracted WebUI, fixture scratch, and unredacted logs. All fixture containers and their tmpfs mounts were removed. The SPDK tag, exact build history and six attributable reclaimable cache records were removed. All 734 unrelated builder records remained. The retained artifacts are this local Git worktree, its committed sources, and these sanitized receipts. NAS actions, release and pin changes were zero. [Exact cache IDs](cache-cleanup.json)
