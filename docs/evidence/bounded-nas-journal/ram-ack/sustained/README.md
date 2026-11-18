# Sustained destination drain on local ext4

The materialized and volatile runs used the same private copy of the native
engine built from `7821e763c69437c6785b5c67f677fa166a1c167d`. Its SHA-256 was
`93488a4e5a160de9fd0e170568a8dae05beddfc7ab64b615894e8a482d75bebb`.
The copy came from the build root via `cp --reflink=auto`. The runner
checked the digest before each run, and both receipts independently
recorded it from inside the container. [Runner](run-sustained.sh)

Each run wrote the same six distinct 768 MiB files, for 4,831,838,208 logical
bytes. The generator populated the owned source directory before timing.
Six 9P2000.L clients negotiated 1 MiB messages, read from the source files
in request-sized windows, and sent one WRITE at a time per connection.
The clients waited for all WRITE acknowledgements before issuing concurrent
fsyncs. The service and clients shared one private cgroup on CPUs 0 and 1,
with 8 GiB of memory and zero swap. Both member directories lived on the
same local ext4 filesystem and each was configured with 8 GB of capacity.
[Generator](generate.py) · [Source SHA-256 manifest](source-sha256.json)

## Middle drain window

Sampling used the fixture's 100 ms polling interval. The middle window
begins at the first sample at or above 25% of the
`zerofs_writeback_remote_bytes_completed_total` increase between the
post-warm-up baseline and final drain. It ends at the first sample at or
above 75%. The rate divides the measured counter delta by the measured
elapsed time between those samples. The counter covers completed writeback
bytes, including stored object data beyond the logical file payload.
The SSD admission pacing metric was active at both endpoints in both modes.

| Mode | Full counter increase (bytes) | Middle delta (bytes) | Middle duration (s) | Middle drain (MB/s) |
| --- | ---: | ---: | ---: | ---: |
| Materialized | 4,975,560,923 | 2,515,628,748 | 17.910615 | 140.45 |
| Volatile | 4,975,472,332 | 2,528,707,352 | 23.499796 | 107.61 |

This ordered pair observed about 23% lower middle-window drain throughput
in volatile mode. It measures local ext4 destination drain with foreground
clients active in the same resource envelope.

## Allocation and independent verification

Member allocations sum `st_blocks × 512` for regular files, counting each
inode once within its member. Warm-up allocated 1,093,632 bytes on each
member in both runs. The final offline allocation was:

| Mode | Member A final bytes | Member B final bytes | Member A increase | Member B increase |
| --- | ---: | ---: | ---: | ---: |
| Materialized | 2,493,681,664 | 2,478,497,792 | 2,492,588,032 | 2,477,404,160 |
| Volatile | 2,533,556,224 | 2,438,635,520 | 2,532,462,592 | 2,437,541,888 |

Both placed objects on each member, reached zero journal payload blobs,
and sent SIGKILL at the verified drain cutoff. The verifier opened the
destinations through a new read-only engine with a fresh engine cache and
writeback omitted, while the original journal was parked. All six 768 MiB
files and the warm-up files matched their independent SHA-256 values. The
materialized and volatile cgroups peaked at 6,245,269,504 and 6,235,267,072
bytes, respectively. Both reported zero swap use and zero memory-event
deltas. The cgroup peak includes the workload and kernel memory charged to
the container.

The initial materialized attempt drained successfully, then the verifier
refused to open the catalog because its template still specified 1 GB per
member. The runner was corrected to use 8 GB in both engine and verifier
templates, and the matched pair was restarted. The two reported runs
completed with exit status 0. [Materialized receipt](evidence/materialized/run/receipt.json) · [Volatile receipt](evidence/volatile/run/receipt.json) · [Initial attempt cleanup](evidence/preflight-verifier-capacity/cleanup.txt)

Each run removed and verified the absence of its container and scratch
directory. The separate source directory, binary copy, and private root
were then removed and checked absent. [Materialized cleanup](evidence/materialized/cleanup.txt) · [Volatile cleanup](evidence/volatile/cleanup.txt) · [Source cleanup](source-cleanup.txt)
