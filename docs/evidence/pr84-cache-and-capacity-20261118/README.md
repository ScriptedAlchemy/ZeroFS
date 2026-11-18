# Package cache and full-pool verification

The engine source under test is `a4adede0f5276529eb34fb9285efd1f6dc3bfc19`. The package recipe and mounted harness come from app commit `21305b0383c448f1927f1d08ba445f62c79bbdaf`. The checks use disposable local member directories on native x86-64 Linux. The deployment owner separately ran the three-protocol Custom App on engine `6b89d23b`. The scanner change has no effect on the package choice of mount client.

## Cache verification

The previous package completed 20 workloads on each client. In both cases its offline scanner exited 4 with one incomplete decoded-block partition and thirteen incomplete raw-parts partitions. Reclaim had cleared only the first page of a directory-backed Foyer block. Rewriting a shorter chain left bytes of the old generation beyond the last valid blob. Recovery-disabled startup could also reuse the previous contents of a block.

The candidate punches the whole file range before block reuse on Linux. A filesystem that rejects punching takes the existing first-page fallback. The offline scanner counts every trailing byte and returns an incomplete result if it finds any nonzero byte in that region. Recovered decoded-block partitions containing entries from older builds can retain stale tails until reclaimed. A clean native ext4 receipt applies to the tested workload and filesystem. ZFS hole punching, startup time and ARC headroom require separate on-target evidence.

The native command lines below ran in the engine's `zerofs` directory with Cargo locked to the archived sources.

```sh
cargo test -j4 --offline --locked --no-default-features -p zerofs --lib cache_scan
cargo test -j4 --offline --locked --no-default-features -p zerofs --lib cache_files_tests -- --test-threads=1 --nocapture
cargo test -j4 --offline --locked --no-default-features -p zerofs --lib pool_full_tests
cargo fmt --all -- --check
```

These groups passed 18, 2 and 8 tests respectively. Formatting and library Clippy with warnings denied also passed on Rust 1.92.0. The two cache-file tests used the production cache writers. The mounted package checks use the same release binary for both clients, with the app service constrained to 8 GiB, no swap and two CPUs. SMB workload clients run outside the service cgroup.

The local test build supplied the package Dockerfile with its standard commit and archive-hash arguments. Its input was a local Git archive with prefix `ZeroFS-a4adede0/` and SHA256 `48003d8b102229c8cf8f450d42073a1d0cc77a32c12e9cdd67d2f7bd711cb5d5`. The release executable SHA256 was `0cd6b449d1c3c0186841945d069aa703a58ab900978ae3e21c81c5b2d3da1cf8`. The local app image ID was `sha256:ad07e4bb9a3c1c7eea002d55df49ebfcda8e6c366a6674332c83b537ac1cb7ed`. The test image ID was `sha256:275816c5cb5607febfc9cb80ca7c0063836ec6b0ade5de5a8aae3df65584b397`.

## What the full-pool test requires

The original SMB test deletes a closed, flushed 33,554,432-byte file and immediately FLUSHes the already-staged replacement on the same open handle. The deadline is five seconds. A separate mounted FUSE control writes different bytes for the old and replacement files with no explicit pin, an open reader across unlink, and a persistent checkpoint in separate fixtures. The test reads the held file after unlink and the checkpoint view after crash/restart. An independently fsynced sentinel is verified after restart in all three cases.

On the candidate the original SMB FLUSH returned `STATUS_DISK_FULL` after 0.012656 s on kernel 9P and 0.019070 s on FUSE. Both probes exited 1. The distinct-payload FUSE case without an explicit pin also exited 1. The held-reader and checkpoint controls exited 0 with the expected ENOSPC, verified the old payload, and preserved the independently fsynced sentinel after SIGKILL and restart. The failed FLUSH supplies no durability acknowledgement for the pending replacement.

The member quota charges physical bodies until deletion. The database-wide flush seals pending data before it makes reference changes durable. A retained replacement seal can therefore prevent the unlink from reaching the durable state required for reclaim. An explicit reader or checkpoint can require the old bytes even after the unlink is durable. The existing reader grace period also protects the gap between resolving and fetching a frame. Removing these protections could lose acknowledged data.

The candidate logged the size of the refused sealed body on each retry. Immediately after unlink, member B had 897,024 filesystem bytes free, below its 1,000,000-byte reserve. Member A had enough filesystem space, but insufficient room under its 50,000,000-byte physical quota.

| SMB mount client | Sealed body (bytes) | Admissible on A (bytes) | Shortfall on A (bytes) |
| --- | ---: | ---: | ---: |
| Kernel 9P | 34,802,471 | 15,197,192 | 19,605,279 |
| FUSE | 34,709,017 | 15,287,892 | 19,421,125 |

The shortfall is the additional headroom needed to keep both generations on A. A distinct destination must fit the entire unsplit body above its own reserve, plus durable bookkeeping. These are measured lower bounds for these fixtures. A landing design must reserve those bytes before use, account for interrupted writes and replay, and preserve reader and checkpoint views. The shipped configuration has no such allocation. For unpinned data, a design that frees the old body first must separately prove the durable unlink and reader exclusion before physical deletion. No quota or durability contract changed in the scanner patch.

## Trial and real-data boundaries

The explicit FUSE trial preserves empty xattr replacement and removal. Stock kernel 9P sends identical frames for the two operations. The product default remains kernel 9P. [Wire receipt and engine acceptance gates](../pr84-acceptance-20261118/README.md)

The earlier three-protocol package trial exercised one public file subtree and a separate private block namespace. Required member mount loss withdrew all endpoints, and cold start with a missing local member or catalog refused. Neither behavior provides degraded availability. Current first-fit placement is per immutable segment. Whole-file policy binding, intentional striping, metadata/catalog survival after device loss and NFSv4 ACL fidelity remain open. Bulk HDD placement and a clean read cache do not establish durable NVMe landing. The deployment owner retains physical commissioning and private publication.
