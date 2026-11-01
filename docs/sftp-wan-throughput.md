# SFTP read throughput

## What the latency does and does not imply

A 95 ms round trip does not impose a 1 MB/s limit on an SSH connection. The
application has to keep enough independent requests in flight. At 1 Gbit/s,
the bandwidth-delay product is 11,875,000 bytes (about 11.3 MiB). That is the
payload in flight needed to cover one round trip, before protocol overhead or
additional request dependencies. It is not a promised transfer rate.

For the reported CT198-to-Mac measurement of 300–450 Mbit/s, that hop's
arithmetic ceiling is 37.5–56.25 MB/s before overhead. Improving the Hetzner
read path can therefore matter substantially even without relocating ZeroFS.
Those network measurements are operator reports; this change has not repeated
them or benchmarked the deployed Hetzner service.

An earlier operator trace superseded the first throughput estimate:
the destination drive receives **12–14 MB/s**, while approximately **25 MB/s**
arrives at CT198 from Hetzner. The earlier 25 MB/s figure was not completed
copy throughput. Each of eight SFTP sessions delivers approximately 2–4 MB/s;
a separate larger-request/deeper-queue test did not improve that rate. The
reported CT198-to-Mac capacity is 36–57 MB/s and destination write capacity is
167 MB/s. These results make useful-byte efficiency the immediate target;
the short-reply simulation below does not establish the cause of this live
limit. The upstream/downstream gap alone does not prove that every extra byte
is wasted: it can also include useful cache filling, protocol traffic, and
differences between measurement intervals.

A subsequent operator update reported that ten open Hetzner connections and four
concurrent NFS copies settled back to about **13 MB/s at the destination**.
The earlier 37 MB/s burst was attributed to cached data; the forecast of
16–18 MB/s did not materialize. Increasing the pool from eight to ten therefore
did not produce a reported sustained improvement. Open connections alone do
not establish active request concurrency, or distinguish scheduling gaps from
a shared backend limit. That update did not yet identify the running binary revision.
With all ten account slots occupied, an additional SFTP benchmark must wait
for capacity; the eight-production-plus-two-test example below does not apply
to that live configuration.

## Uploaded capture: November 1, 06:09–06:24 UTC

The operator supplied a capture from the running `bbe5f3d6` build with ten
connections, `read_concurrency = 64`, 1 MiB cache parts and four concurrent NFS
copies. The subsequent `e40f15a` missing-range scheduling fix was **not deployed**
for this capture. No live gain is attributed to that follow-up.

The common interval is 06:09:22.611–06:23:57.447 UTC (874.836 seconds). Rates
use cumulative byte differences divided by elapsed time. Socket and Mac
endpoints are interpolated within their approximately one-second samples;
filesystem/cache counters have a separate five-second export cadence.

| Counter layer | Decimal MB/s | MiB/s |
| --- | ---: | ---: |
| TCP received on the ten SFTP connections | 18.610 | 17.748 |
| Accepted SFTP object payload | 18.578 | 17.717 |
| Filesystem read bytes | 18.141 | 17.300 |
| Logical growth of the four destination files | 18.148 | 17.307 |
| Destination filesystem allocated-space growth | 18.146 | 17.305 |

Destination file growth is about 97.5% of TCP bytes received in this window.
That is a cross-layer ratio, **not an exact useful-byte efficiency**: cache
boundaries, compression/encryption representation, metadata and metric export
lag differ. It does not support carrying forward the earlier estimate of a
30% incoming-byte loss. With unchanged ingress, recovering the observed
18.610 minus 18.148 MB/s gap alone would be about 0.46 MB/s, not a large gain.

All ten socket identities persisted across 903 samples, averaging 1.78–1.91
MB/s each. All four destination file identities persisted across 934 samples,
with no logical-size resets. Complete one-minute file-growth intervals ranged
from 16.44 to 21.03 MB/s. The derived `het_1s.csv` exactly matches socket byte
differences and is not a second independent network measurement.

Allocator memory remained steady; cache overflow counters did not increase.
Process thread CPU averaged approximately 0.54 core, **assuming the unrecorded
Linux CLK_TCK value was 100**. The Mac iostat command failed and produced an
empty file; file sizes and `df` therefore do not establish physical disk
latency, device utilization or durable completion. CT198 diskstats lacked a
cache-mount-to-device mapping, so stacked device counters cannot be attributed
to the cache or summed as separate workload traffic.

The metadata engine's unlabeled object-store counters in this capture are not
aggregate request/error counts. The old exporter discarded each series' labels,
merged counters through `absolute` (effectively the highest value), and overwrote
gauges and histogram sums. The follow-up preserves the original label sets.
Do not calculate a failed-read rate from those old flattened values. The
journal contains no WARN/ERROR lines, but that does not classify every engine
error or prove zero retries.

This capture narrows the question to increasing successful backend delivery;
it does not establish a provider throttle, sufficient SFTP request concurrency,
or the exact origin of waiting time. TCP socket statistics do not count SFTP
requests inside the encrypted SSH stream.

### Later operator-reported path matrix

The operator subsequently reported the following results from
`path-matrix-20261101T065508Z`. The raw files for this matrix were not available
to this code session, so these are operator reports, separate from the uploaded
capture analyzed above. The four production copies continued during the tests;
the service configuration and deployed build were unchanged.

| Test | Reported result |
| --- | --- |
| CT198 to Mac iperf3, 1 / 4 / 8 streams | 38 / 62 / 56 MB/s, with the copies competing |
| Four additional readers of cached ZeroFS data | Approximately 15 MB/s above the copy traffic |
| Additional uncached readers, 1 / 4 / 8 | No aggregate increase; shared the existing approximately 18 MB/s backend ingress |
| Destination drive write with fsync | 163 MB/s |
| CT198 cache disk write / read | Approximately 680 / 807 MB/s |

The operator also reports 80–88% idle CPU on CT198, a 1280-byte MTU, about
70 ms round trip and no observed drops on the tested overlay path. These tests
support focusing on uncached backend delivery. The fast CT198-to-Mac TCP test
does not directly measure the separate CT198-to-Hetzner connection, or isolate
SFTP request scheduling from SSH service and backend storage behavior.

Mac Tailscale used approximately 4.3 cores at 60 MB/s. That point identifies a
possible later constraint; it does not establish a hard 60 MB/s throughput
ceiling. The reported object-store error-counter growth (approximately 22 per
ten seconds) is still from the exporter that discarded engine labels. Neither
that delta nor the reported 13k/845k ratio identifies failed SFTP reads. The
diagnostics below separate returned backend outcomes from locally cached
absence responses, while preserving the engine labels for the next capture.

## Follow-up diagnostics: request waits and object phases

The follow-up adds normal-build metrics without changing transfer limits or
read scheduling. They are aggregate process counters, with static `phase`,
`kind`, `operation`, and `outcome` labels; no path, hostname or reconnect ID
labels are added.
The timing covers **client future lifetimes**. It is not a count of packets
on the wire, and a cancelled future may leave a request already sent to the
server that the transport still has to drain.

| Metric | Meaning |
| --- | --- |
| `zerofs_sftp_read_requests_total` | READ futures first polled, including header reads and EOF probes |
| `zerofs_sftp_read_requests_inflight` | Polled READ futures whose result has not returned or been cancelled |
| `zerofs_sftp_read_requested_bytes_total` | Sum of lengths requested by those futures; not delivered bytes |
| `zerofs_sftp_read_requested_bytes_inflight` | Requested lengths owned by currently pending READ futures |
| `zerofs_sftp_read_requests_finished_total{outcome}` | DATA, EOF, error, or cancellation results |
| `zerofs_sftp_read_reply_bytes_total` | All DATA reply bytes, including headers and replies later discarded with a failed object |
| `zerofs_sftp_read_reply_size_bytes` | DATA size distribution; exporter exposes `_count`, `_sum`, and quantiles |
| `zerofs_sftp_read_short_replies_total` | Nonempty DATA replies smaller than requested |
| `zerofs_sftp_read_request_duration_seconds{outcome}` | Client READ duration, including local queueing and transport/server time |
| `zerofs_sftp_phase_inflight{phase,kind}` | Pending phase futures |
| `zerofs_sftp_phase_duration_seconds{phase,kind,outcome}` | Phase duration, including success, error and cancellation |
| `zerofs_sftp_negative_cache_hits_total{operation}` | Get/head absence responses satisfied locally without leasing an SFTP session |
| `zerofs_sftp_backend_read_results_total{operation,outcome}` | Returned get/head backend operation results, classified before conversion to object-store errors |

Pool phases are `admission` and `session`, split by read/write/metadata kind.
Read-operation phases are `open`, `fstat`, `header`, `payload`, and `close`.
FSTAT, header and speculative payload run concurrently: **do not sum their
latencies as sequential wall time**. Both object reads and exact reads carry
OPEN/payload/CLOSE observations. Drop guards release gauges on cancellation;
the counters report cancellation separately from EOF or failure.

Backend results use `operation="get"` or `"head"` and outcomes `success`,
`not_found`, `invalid_range`, `permission_denied`, `corrupt_object`,
`pool_closed`, and `transport_error`. They include admission, session and
handle-completion failures; they are not a count of on-wire READ responses.
They exclude negative-cache hits, rejected local options/paths and cancelled
operations that never return. The phase/request observations above retain the
cancellation evidence. `not_found` can be an expected metadata probe; inspect
the labeled engine operation before treating it as a copy failure.

LSM series keep their `lsm_` names and now retain engine labels such as
component, store, operation and API. Consumers needing totals must aggregate
across the intended labels. The legacy engine histogram export remains a
labeled cumulative-sum gauge, not a request-latency percentile.

### Next capture and decision

At a planned service change point, compare a build containing `e40f15a`,
these diagnostics and the EOF reply-size learning fix below with the captured
`bbe5f3d6` baseline, preserving resumable partial copies. Record the exact
deployed SHA and unchanged effective configuration. A restart/cache burst is
not a steady-state gain.

Capture 10–15 minutes of full raw Prometheus scrapes at one-second intervals
alongside the existing socket and per-file growth data. Preserve labels and
raw cumulative counters. Keep the four-copy workload and account connection
budget unchanged; ten production connections leave no extra SFTP benchmark
slots. Record host clock synchronization, `getconf CLK_TCK`, cgroup CPU limits
and throttling, and the actual cache mount/device mapping. For the Mac disk
monitor, resolve the volume's whole disk with `diskutil info` and monitor that
whole disk; the partition identifier in the previous command was rejected.

Calculate request mean latency from deltas of the duration `_sum` and `_count`,
and mean DATA size from reply bytes divided by the DATA-size `_count` delta.
Use outcome-separated durations and sampled pending gauges. Request-duration
sums per elapsed second approximate average occupied client READ futures only
in steady state, with boundary-censored requests accounted for. Those are
process aggregates, not proof of an even per-connection request distribution.

- Long admission/session waits identify local admission or pool placement as
  the next target; distinguish read demand from metadata/write competition.
- Low pending payload work with substantial OPEN/CLOSE time motivates a
  controlled test of larger useful ranges or safe immutable-handle reuse.
- Many pending READs with slow replies and small local waits point further
  into SSH/network/server service; they do not by themselves prove a provider
  rate cap. A direct native-SFTP comparison needs an isolated connection budget
  and the same object/range workload.
- A persistent large reply-byte/object-payload gap identifies retries, discarded
  work or EOF/metadata overhead for inspection. Compare like windows and known
  object sizes before attributing every difference to waste.
- Compare labeled engine errors with backend-result outcomes and negative-cache
  hits. Locally answered absence probes do not consume SFTP bandwidth. Actual
  permission, corruption, transport or pool failures need their own treatment;
  a combined object-store error count cannot distinguish them.

## Transport and application windows

ZeroFS already configures a 16 MiB SSH receive window. SFTP READ requests are
independent of both that byte window and the physical SSH connection count:

- The default account pool is eight physical connections. Other processes
  using the same Storage Box also consume its ten-connection allowance.
- The default read operation limit is 16, multiplexed over that pool.
- Each payload read pipeline permits at most 128 outstanding SFTP READ requests,
  with a separate 64 × 255 KiB cap on the payload bytes requested at once. Large
  packets therefore keep their previous byte envelope; small packets can fill
  a 4 MiB window in one wave. Negotiated packet and read limits still apply.
- The general object cache begins with a 1 MiB fetch and promotes sequential
  access to 4 MiB windows with eight windows of look-ahead. File segment reads
  instead use the logical file-aware path below; the object-level heuristic
  remains available to metadata/object consumers.
- 9P, CLI/SDK/browser downloads, and mounted read-ahead also need enough
  concurrency to feed the backend. The corresponding changes are collected
  in [PR #45](https://github.com/ScriptedAlchemy/ZeroFS/pull/45).

### Short replies need independent follow-up requests

An SFTP server may return fewer bytes than requested without reaching EOF.
The previous read loop split a fetch into roughly 255 KiB requests and then
repeatedly awaited each request's remaining suffix. For a 1 MiB fetch and
32 KiB replies, this left only about five request chains active despite a
64-request budget. Finishing the largest chains required eight reply waves.

The revised loop keeps a shared queue of missing ranges, learns the smaller
reply size for the current operation, and divides those missing ranges among
free request slots. It refills slots on individual completion, without a
whole-batch barrier. It still checks exact byte coverage, rejects empty or
oversized replies. This first repair retained the 64-request cap and kept the
original packet size for operations receiving only full-sized replies. The
follow-up below improves repeated windows while retaining that byte envelope.

The regression uses virtual time, patterned bytes, and a simulated 95 ms reply
delay. This isolates request scheduling; it does not model TCP, SSH encryption,
server disk speed, OPEN/FSTAT/header/CLOSE overhead, or a production transfer.
It must not be quoted as a measured Hetzner speedup.

| Controlled 1 MiB read with 32 KiB replies | Simulated read time | Reply waves |
| --- | ---: | ---: |
| Previous serial suffix recovery | 760 ms | 8 |
| Shared missing-range queue | 190 ms | 2 |

The old implementation failed the new regression at 760 ms before the scheduler
was changed. The revised implementation passes with identical patterned bytes
at 190 ms. These timings cover the payload scheduler only.

To reproduce the deterministic regression from the `zerofs/` crate directory:

```console
cargo test --locked --no-default-features --lib \
  sftp_protocol::tests::short_sftp_read_replies_fill_available_request_slots \
  -- --exact --nocapture
```

The `--no-default-features` build requires a system `protoc`; the shipping
dependency versions remain locked.

## Reuse a stable reply size across read windows

The first repair still relearned a smaller reply cap on every operation. In a
controlled 4 MiB read with 32 KiB replies, the initial large requests and the
64-request refill limit took three 95 ms waves: **285 ms per window**, including
subsequent windows on the same connection. The new regression failed on that
implementation before the follow-up was applied.

The physical SFTP session now remembers a reply-size hint after a successful
payload read observes at least two equal short replies of 32 KiB or more, with
no larger or inconsistent replies. A single short response does not tune
future objects. Every sixteenth bulk read probes the negotiated maximum so a
transient cap cannot leave the session permanently using small packets. Header
reads do not consume the bulk-probe cadence. The hint is always clipped to the
current negotiated limit; exact-read validation still handles shorter replies.

Up to 128 requests may now be outstanding, while their **combined requested
payload remains capped at 16,711,680 bytes** (the previous 64 × 255 KiB envelope).
That permits 128 × 32 KiB = 4 MiB in a single wave without doubling the large
reply byte allowance. The regression checks the large-packet case still stops
at 64 simultaneous requests, and verifies complete patterned bytes.

| Controlled 4 MiB read with 32 KiB replies and 95 ms RTT | Payload-read time |
| --- | ---: |
| Previous shared queue, each window | 285 ms |
| New pipeline, first window learning the reply size | 190 ms |
| New pipeline, subsequent learned windows | 95 ms |

This is a threefold improvement in the **simulated steady payload-read phase**.
It does not imply a threefold end-to-end copy speedup. OPEN and CLOSE still have
round-trip costs; TCP/SSH, remote storage, NFS, Tailscale and the destination
also contribute. Periodic maximum-size probes can take an additional wave.
No deployed Hetzner measurements are claimed for this follow-up.

```console
cargo test --locked --no-default-features --lib \
  sftp_protocol::tests::repeated_short_reply_windows_keep_the_wan_pipeline_full \
  -- --exact --nocapture
```

### Preserve nonterminal reply-size evidence across EOF

The clipped-read repair originally suppressed reply-size learning whenever a
speculative cache range crossed the object's end. That protected against
mistaking an EOF tail for a server cap, but also discarded every preceding
consistent short reply. Repeated reads of smaller objects could therefore pay
the reply-size discovery cost on every operation on the same connection.

A regression requests a 1 MiB range from a 720,913-byte patterned payload, with
32 KiB capped replies and a deterministic 95 ms response delay. It failed on
the preceding implementation: both the first and second read took 285 ms.
After the fix, the first read still takes 285 ms and the next two take 190 ms.
Each read verifies every payload byte and that DATA transfers the payload
exactly once.

| Controlled EOF-clipped payload read | Before | After |
| --- | ---: | ---: |
| First read on the session | 285 ms | 285 ms |
| Next read on the same session | 285 ms | 190 ms |

The reader now holds back the DATA sample with the greatest end offset while
aggregating earlier samples. After complete coverage validation, it excludes
a terminal short reply when EOF clipped the requested range, but retains the
nonterminal evidence. This takes constant additional state, handles reordered
DATA/EOF, and does not change request, byte, connection or admission limits.
At least two matching nonterminal short replies are still required to learn
a cap. A complete maximum-sized final reply remains valid evidence for a
periodic probe to clear an old smaller hint.

Additional regressions cover empty/small uncapped objects, a 40 KiB EOF tail
after consistent 64 KiB capped replies, out-of-order EOF, and recovery to a
full-sized reply on an object exactly one maximum packet long. Existing
coverage, invalid-reply, negotiated-limit, cancellation, and native SSH
integrity checks remain applicable.

The measured 95 ms reduction is confined to the **simulated payload scheduler**.
It excludes connection/handle setup, metadata, SSH/TCP, remote disk and the
mounted copy path. The actual Hetzner reply-size distribution and the frequency
of these clipped reads have not yet been measured. This is a reproduced
conditional optimization, not a measured live throughput increase.

```console
cargo test --locked --no-default-features --lib \
  sftp_protocol::tests::repeated_eof_clipped_reads_reuse_nonterminal_reply_size \
  -- --exact --nocapture
```

## The reported Mac copy path

The operator reports an NFS v3 mount with 1 MiB reads over a direct Tailscale
path, with four concurrent copies to one external drive. The SFTP changes above
sit beneath NFS and apply to that path. CLI/SDK/9P client-specific changes do not
determine NFS client queue depth.

The pinned NFS server implementation permits 32 in-flight RPCs per connection,
and ZeroFS advertises 1 MiB preferred/maximum reads. These are code limits, not
a measurement of the deployed Mac's outstanding reads:

- [Pinned NFS TCP transport](https://github.com/ScriptedAlchemy/nfsserve/blob/0c19924faa3b09041d198d167785a7de80ea48b5/src/tcp.rs)
- [ZeroFS NFS adapter](https://github.com/ScriptedAlchemy/ZeroFS/blob/604685ed4c0f43d25692ad7e187786aadc623f32/zerofs/src/nfs.rs)

For a live comparison, record the running binary revision, effective SFTP
configuration, the actual copy command, NFS READ size/latency/outstanding count,
server backend activity, and destination write latency over the same interval.
The later uploaded capture above identifies the operator-reported revision
and gives an aligned **18.15 MB/s logical destination growth** baseline. The
older 12–14 MB/s destination and 25 MB/s upstream figures describe an earlier
interval, not this capture. Compare the next build only at an agreed
service change point, recording the same workload and cache conditions while
preserving the in-progress copies.

## Read ahead along the file, not neighboring physical frames

A segment may contain frames from several files. Inferring the next file
bytes from nearby physical offsets can therefore fetch unrelated data. The
old SFTP object heuristic could schedule eight 4 MiB windows into that tail;
the resulting cache pressure could also evict useful parts and fetch them
again. A mixed-segment regression reproduced both behaviors when reading
8 MiB from the start of a roughly 32 MiB segment whose remaining frames belonged
to another inode.

File segment reads now carry a `CacheRangeOnly` hint. They still use the shared
parts cache and single-flight registry, but expand only to cache-part
boundaries. The file's extent map chooses the forward reads, so the logical
8 MiB look-ahead works both within one segment and across segment boundaries.
It is issued as independent 1 MiB logical ranges; a global sixteen-permit
budget covers the individual ranges, rather than sixteen whole 8 MiB windows.
This keeps one file able to use multiple SFTP sessions without letting every
physical object initiate a separate speculative tail.

When the budget admits only part of a window, the planner records only that
prefix. Nearby read completions arriving out of order no longer move the
forward sequence backwards. Cache-part alignment, frame overhead, and
metadata reads still mean backend traffic need not equal logical file bytes.
The object-level heuristic remains available to other consumers such as
metadata storage.

The regression uses real encoded frames, the production segment reader, the
production prefetch cache, and a recording in-memory backend. It checks exact
plaintext and rejects any fetch beyond the final cache part of the requested
file, or duplicate fetching beyond that bound. With 1 MiB cache parts, the
corrected path fetches **9 MiB for the 8 MiB file**, including encoded-frame
overhead and the final part boundary. The previous path reached the unrelated
tail of the approximately 32 MiB segment and repeated some earlier ranges.
This is a reproducible amplification defect; it does not establish how much of
the operator's live upstream/downstream gap it explains.

```console
cargo test --locked --no-default-features --lib \
  object_store_prefetch::tests::segment_reads_do_not_speculate_into_unrelated_file_frames \
  -- --exact --nocapture
```

## Keep the valid bytes when a cache range crosses EOF

Cold cache ranges are aligned before the object's length is known. The SFTP
reader overlaps the payload request with the header and FSTAT requests. If
the aligned range crosses EOF, the old exact-read pipeline failed after
receiving part or all of the valid prefix. Once the header clipped the range,
`read_object` downloaded that prefix again.

A regression over a real local SSH/SFTP connection reproduced this duplicate
transfer. Reading a 720,913-byte suffix of a valid object transferred
1,441,858 SFTP DATA bytes: twice the useful payload plus the 32-byte internal
header. The repaired path transfers 720,945 DATA bytes: the payload once plus
the header. It checks identical patterned contents with both full-sized and
32 KiB server replies. These totals exclude SSH/SFTP framing and are specific
to this clipped-range case, not a claim of halving all backend traffic or
doubling deployed copy speed.

Speculative reads now retain the complete prefix before an explicit SFTP EOF
status. They drain already-issued requests, finish missing earlier ranges,
and validate the byte count against the same open handle's header and FSTAT.
Empty DATA replies, data beyond an earlier EOF, truncated objects, and other
errors still fail. Exact reads still require their full length. A logical
bound that would overflow after adding the internal header waits for metadata
to clip it before issuing payload requests. EOF tails do not update the
session's learned reply-size hint.

The protocol distinction between DATA and EOF is described in the
[SFTP v3 protocol draft, section 6.4](https://datatracker.ietf.org/doc/html/draft-ietf-secsh-filexfer-02#section-6.4).

```console
cargo test --locked --no-default-features --lib \
  russh_session::tests::russh_clipped_object_range_does_not_download_the_payload_twice \
  -- --exact --nocapture
```

## Start independent missing cache parts while earlier parts are pending

The logical read-ahead window already covers 8 MiB. The cache's result stream
had a separate serialization point: it polled one part at a time. If the first
part joined an existing pending fetch, the next missing part did not start
until that earlier fetch completed, even when backend capacity was available.
Adjacent logical ranges can share a cache part at an encoded-frame boundary,
so this dependency can arise within otherwise parallel file-aware reads.

The stream now polls up to eight part reads concurrently and returns their
bytes in order. Each part still uses the common range registry: it joins a
covering fetch, trims around cached/in-flight coverage, or leads only a missing
gap. A contiguous cold range can still coalesce into one backend GET. This
starts useful missing data earlier without increasing the 8 MiB logical
look-ahead distance or the physical SFTP connection limit.

The regression holds an existing prefix fetch open, requests a range spanning
that prefix and a missing tail, and requires the tail GET to start before the
prefix is released. The previous serial stream fails this barrier. The
parallel stream passes, checks the complete patterned output, and verifies
that each backend range was fetched once with no out-of-range speculation.
A cancellation regression holds two missing gaps around a cached part and
checks that dropping the read releases both backend slots and every registered
fetch. These are local scheduling and integrity checks, not a live copy-rate
measurement.

```console
cargo test --locked --no-default-features --lib \
  object_store_prefetch::tests::demand_read_starts_missing_tail_while_joined_prefix_is_pending \
  -- --exact --nocapture
```

## Keep CLI reads within server admission

The transfer client negotiates roughly 9 MiB payloads for uploads. Reusing that
size for four concurrent download reads reserves over 72 MiB for source and
serialized response buffers, exceeding the server's 64 MiB per-connection
budget. The fourth read can block the receive loop, with the client's lineage
liveness probe queued behind it. If the earlier reads remain slow long enough,
the probe timeout can cause a reconnect even though the backend is working.

CLI downloads now cap each read at 1 MiB and keep eight ranges in flight.
That holds roughly 8 MiB of useful read payload and leaves both request slots
and response-byte capacity for control traffic. Upload negotiation is
unchanged. The wire regression negotiates the normal upload size, verifies
small download requests, withholds replies until the full read window is
issued, reverses their order, and checks the resulting file bytes. The server
regression retains all read credits and verifies the lineage probe can still
be admitted.

The operator reported no written bytes in a 150-second CLI trial. The admission
problem above is reproduced independently; the exact cause of that deployed
trial still requires its command and trace. A CLI download writes to a
temporary sibling and publishes the final destination only after completion,
so measurement must include the temporary file or actual device writes.

## Check the endpoint before comparing hosts

Hetzner documents better SFTP performance through its SSH service on port 23
than on port 22:

- [Hetzner external storage configuration](https://docs.hetzner.com/storage/storage-share/configuration/external-storage/)
- [Storage Box access overview](https://docs.hetzner.com/storage/storage-box/access/access-overview/)
- [Storage Box limits](https://docs.hetzner.com/storage/storage-box/general/)

Verify the deployed URL first. If comparing ports, enable SSH support, use the
correct key format and verified host-key entry for the selected port, and
confirm that both endpoint paths resolve to the same existing objects. Port 23
can expose a different root path; a port-only substitution is not sufficient
evidence that the storage namespace is unchanged. Keep the production object
prefix and encryption configuration intact.

## Measurements that distinguish the remaining bottleneck

1. Record the running binary's exact Git revision, selected port, SFTP pool and
   read-concurrency settings, mount/client protocol, and cache state. An open
   PR is not evidence that the service runs its changes.
2. On CT198, download the same existing large remote object with one stock
   OpenSSH SFTP connection. Compare `-B 32768 -R 128` (4 MiB requested) with
   `-B 261120 -R 64` (about 15.9 MiB requested), subject to server support.
   Keep strict host-key verification and use an owned local scratch file.
   Stock OpenSSH is a diagnostic control, not a replacement product transport.
   Its request and buffer options are documented in the
   [OpenSSH SFTP manual](https://man.openbsd.org/sftp.1).
3. Run the production native-russh direct benchmark described in `AGENTS.md`
   on the isolated Linux test environment. Account for production connections:
   an eight-connection production pool leaves at most two benchmark slots.
   Use its integrity and teardown gates, and record both object size and
   effective concurrency. It creates its own UUID-scoped remote objects.
4. Compare the same cold and warm user-visible copy paths. A raw encrypted
   backend object and a logical filesystem file are different workloads;
   report them separately. Do not call a read cold merely because the client
   page cache was invalidated while ZeroFS's server cache remained warm.

If stock SFTP is fast but the native direct benchmark is slow, investigate the
native request/SSH path. If both direct controls are fast but the mounted copy
is slow, investigate cache admission, filesystem/client concurrency, and the
CT198-to-Mac path. If both direct controls remain slow, investigate the route,
loss, and shared Storage Box service before increasing ZeroFS limits.

Do not run the legacy pilot setup/teardown commands against CT198 or a shared
production mount. The existing `AGENTS.md` and
`docs/vm100-benchmark-methodology.md` define the isolated benchmark procedures.
