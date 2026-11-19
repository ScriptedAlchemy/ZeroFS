# PR84 CPU and resource review: pass 7

This note records source observations and host-local diagnostics against review
baseline `20fc0f403ead3166cb6a42b4d45c98676858cd75`. The NAS reference remains
engine `560eaae7d54e21f5b3c3171ea17e4b086e9c51b9`, whose build provenance is
[recorded separately](../bounded-nas-journal/ram-ack/editor/README.md). Its
operator-reported trial settings were eight CPUs, 256 MiB of volatile filesystem
writes, a 512 MiB SSD journal, and SSD-backed SMB FLUSH. **These changes have not
had a new NAS throughput run.**

[Exact measurements](measurements.json) preserve source hashes, raw samples,
profiles, and fixture limitations. The host was Linux x86-64 with Rust 1.100.0.
Journal, worker, and protocol probes used normal development dependencies; the
codec comparison used optimization level 3. Harnesses named in the receipt were
temporary review-workspace programs, not checked-in repository tools. Measurement
source snapshots can precede later error-handling or test additions.

## Journal preload: one verified payload read

Source inspection found that uploads at or below the 36 MiB single-PUT boundary
first read and verified the journal payload, then read and hashed it again while
collecting streaming chunks. The bounded replacement reads once and verifies the
exact owned buffer passed onward. Reading and hashing execute on the blocking
worker; the maximum retained complete object remains 36 MiB per upload lane.

Eight alternating old/new pairs used the same 32 MiB hot-cache journal object.
Every collected payload was compared byte for byte after timing.

| Preload-only median | Before | After |
| --- | ---: | ---: |
| Process CPU time | 0.687293 s | 0.352407 s |
| Elapsed time | 0.688077 s | 0.356970 s |

That is 48.7% less CPU time and 48.1% less elapsed time for this development-profile
stage. The two-to-one reduction in payload verification passes is a source
observation. The timings exclude backend publication, network transfer, and HDD
drain. Physical I/O syscall counts were unavailable because tracing was blocked.

## Native worker reuse

Local object operations previously started a native thread for every dispatch.
The replacement maintains up to 32 reusable workers outside Tokio, with ten-second
idle retirement. The operation retains its admission permit through completion,
including when its async caller stops waiting.

Eight alternating pairs each dispatched 2,000 sequential no-op operations:

| Dispatch-only measurement | Before | After |
| --- | ---: | ---: |
| Distinct thread IDs per sample | 2,000 | 3–13 |
| Median process CPU time | 0.120 s | 0.115 s |
| Median elapsed time | 0.193998 s | 0.170503 s |

Thread churn is the clearest result. CPU time came from process ticks with 10 ms
resolution; the small CPU difference should not be overinterpreted. The test
performed no filesystem I/O and establishes no disk throughput improvement.

## Codec allocations and mixed CPU timing

Compression now prepares nonce space and tag capacity so encryption can consume
the same allocation. Verified relocation also reuses its buffer. Encoded capacity
remains the exact sealed size. For warmed 32 KiB frames, the observed Rust
allocator requests were:

| Operation | Before | After |
| --- | --- | --- |
| LZ4 compression and sealing | 3 allocations + 1 reallocation | 2 allocations + 1 reallocation |
| Zstd, high-entropy input | 3 allocations | 1 allocation + 1 reallocation |
| Zstd, repeated input | 3 allocations | 2 allocations |
| Verified relocation and sealing | 2 allocations | 1 allocation |

These counters exclude Zstd's internal C allocations. Seven alternating pairs of
8,192 frames, pinned to one CPU, measured process CPU time. High-entropy changes
were −1.23% for LZ4, +0.02% for Zstd 1, and −3.03% for Zstd 3. Repeated-input
changes were −4.46%, −6.79%, and +2.94%, respectively. Negative means less CPU
time. Allocation reduction is deterministic; the small, mixed timing results do
not establish a universal speedup. Compatibility checks cover legacy wire bytes,
AAD, corruption rejection, cross-codec reads, and buffer reuse.

## Protocol snapshots and cache ownership

The 9P I/O snapshot now copies only the fields reads and writes need, avoiding a
clone of the fid's path vector. An isolated allocation probe ran 10,000 lookups at
path depths 0, 1, 8, and 48. Nonempty paths previously allocated once per lookup;
the new getter allocated zero times. At depth eight, the removed vector storage
was 256 bytes per lookup. Root-path lookups already allocated zero times. This
probe excludes open-handle lifecycle and throughput.

A separate Bytes/Foyer probe retained one 32 KiB decoded-cache entry from a
1 MiB request. The old shallow slice kept the entire request allocation alive
while charging 32 KiB. Owned extent data released that backing allocation. Small
decoded caches now use one to eight shards so each admitted extent fits a shard;
budgets below one extent skip population. Copying write-populated cache extents
adds one plaintext copy/allocation per cached extent, potentially offsetting
codec savings while correcting retained-memory accounting.

## Contiguous writes inside pre-sized files

The prior projection hint covered EOF appends only. Contiguous overwrites of a
pre-sized file therefore reacquired the canonical inode lock and could wait for
earlier materialization before RAM acknowledgement. The extension reuses the
projection only for nonempty contiguous single-inode writes while its exact weak
guard upgrades. FIFO materialization, partial-extent protection, permission and
quota checks, replay, cancellation fallback, and the 256-write fairness limit
remain. This is a source-backed optimization rationale with focused correctness
regressions, not a measured throughput claim. Final integration and hosted CI
results are reported separately; these diagnostics do not replace deployed
acceptance.

## Validation

[Validation receipt](validation.json) records the tested source hashes and scope.
Formatting and strict production-library Clippy with the Web UI feature passed.
The combined actual-source harness passed all 25 selectors, and its extracted
test code also passed Clippy. Coverage includes held-materializer sequential
overwrites, cancellation and fairness, cache budgets and partial extents,
corrupt-journal handling, open-inode I/O, and paused directory replacement.

Separate source-module suites passed 100 journal tests, 22 local-store/worker
tests, 11 codec tests, and nine frame-reader/path checks. Their fixture limits,
ignored benchmarks, and the duplicated path-helper check are in the receipt.
These targeted runs fit the local compiler memory limit; full current-head
hosted CI and a new physical NAS run remain separate requirements.
