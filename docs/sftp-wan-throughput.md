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

The operator's later full trace supersedes the early throughput estimate:
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
The exact running revision has not been independently verified here. Use the
corrected **12–14 MB/s destination rate**, not the superseded 25 MB/s estimate,
as the operator-reported baseline. Compare the next build only at an agreed
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
