# WAN upload throughput

The November 15, 2026 investigation found independent limits in the client
tunnel, Linux TCP sending, and the server's writeback admission. Increasing
file concurrency did not solve them. The production example now matches the
measured SSD-backed profile, and deployment validation accepts its ten-session
SFTP pool. The SFTP recovery changes were already merged in
[#71](https://github.com/ScriptedAlchemy/ZeroFS/pull/71).

The server and Mac CLI used for the final tests were built from
`5ac4d4304f04699d9e0895ec60516a6bbad5c57d` by the successful
[PGO release workflow](https://github.com/ScriptedAlchemy/ZeroFS/actions/runs/43680127479).
Both artifacts passed `gh attestation verify --repo ScriptedAlchemy/ZeroFS`.
The [sanitized measurement receipt](benchmarks/2026-11-15-wan-upload.json)
records the tested boundaries and results.

## Separate the stages

Measure each stage before changing concurrency:

1. Check source read speed while the intended concurrent local copies run.
2. Test memory-to-memory TCP and UDP over the actual client route, including
   direct-versus-relayed Tailscale state, RTT, loss and socket counters.
3. Compare a server-local single-file P9 upload with the WAN path.
4. Sample SSD admission, local acknowledgement and remote writeback separately.

The observed home uplink reached 835 Mbit/s against an independent Internet
endpoint. A server-local 256 MiB P9 upload reached 299 MB/s. Neither result
proves that the complete WAN path or Storage Box can sustain that rate.

During a 600 Mbit/s UDP test, 818,238 datagrams entered the Mac tunnel, but only
419,423 matching encrypted packets were captured leaving its Ethernet
interface. Of those, 419,260 matched at the remote physical interface. The
capture reported no kernel capture drops. This localizes most loss before
physical transmission on the Mac; it does not identify the exact kernel
function responsible. The profile spent substantial time in the tunnel's
unconnected UDP `sendto` path. Tailscale's
[upstream investigation](https://github.com/tailscale/tailscale/issues/21602)
describes a closely matching problem, with proposed changes still under review
at measurement time. No unmerged Tailscale build was installed.

Tailscale already transports these packets over UDP. Changing the application
protocol to UDP would not repair loss in that underlying sender path. Consult
the current [Tailscale performance guidance](https://tailscale.com/docs/reference/best-practices/performance)
before changing offload or routing settings; the remote subnet router already
had the recommended UDP GRO settings in this investigation.

## Linux TCP settings for the measured route

The NAS's Linux Tailscale path delivered 594 Mbit/s from a 600 Mbit/s offered
UDP stream. Its single TCP stream initially reached only 32 Mbit/s with CUBIC
and about 1% UDP loss on the path. Selecting BBR for that TCP test reached
315 Mbit/s; two BBR streams reached 537 Mbit/s. This is route-specific evidence,
not a universal congestion-control recommendation.

The NAS then reported `sndbuf_limited` for roughly half the active send time.
Its TCP autotuning ceiling was 4 MiB. The tested persistent TrueNAS SYSCTL
tunables were:

```text
net.ipv4.tcp_congestion_control = bbr
net.ipv4.tcp_wmem = 4096 16384 16777216
```

Verify BBR is available in the running kernel first. Apply settings through
TrueNAS's native tunable configuration, verify the resulting job succeeds,
and read back the live values. Raising `tcp_wmem` changes an autotuning limit;
it does not preallocate 16 MiB to every socket. Account for the host's memory
budget and measure `sndbuf_limited` again. The temporary per-destination BBR
route used during diagnosis was removed after the persistent setting took
effect.

## Keep the source on the Mac while avoiding its WAN tunnel

The tested alternative uses the Mac's existing LAN SSH connection to the NAS.
The NAS opens the WAN TCP connections over its Linux Tailscale path. It does
not stage another copy of the source data on the NAS or replace ZeroFS's
native Rust SFTP backend.

Allow the NAS to reach only the ZeroFS P9 service and, when needed, its
durability metrics. Configure SSH forwarding for the intended account and
source address only, with `AllowTcpForwarding local`, `PermitOpen` restricted
to the ZeroFS host's P9 port, and remote forwarding disabled. Preserve key-only
authentication and host-key verification. Bind the Mac listener to loopback:

```sh
ssh -N -o BatchMode=yes -o ExitOnForwardFailure=yes \
  -L 127.0.0.1:15564:ZEROFS_PRIVATE_IP:5564 NAS_SSH_ALIAS
```

In another terminal, the native client uploads one file:

```sh
zerofs upload 127.0.0.1:15564 /path/to/file /ingest/file --jobs 1
```

`--jobs 1` uses one file worker with the existing two P9 sessions writing
disjoint ranges of that file. Verify the relay command and localhost listener
before using it; a PID alone is insufficient. Remove the relay when its owning
transfer ends. Diagnostic port grants must also be removed after testing.

With the updated server/client and relay, a 1 GiB incompressible file completed
in 26.60 seconds. After increasing the NAS TCP buffer ceiling, a subsequent
1 GiB file completed in 13.56 seconds (633 Mbit/s), and a fresh 4 GiB file in
57.66 seconds (596 Mbit/s). Timing includes setup, native SSD durability and
publication. Server-local readback hashes matched both final test sources.
The NAS Series migration continued during these tests. Cache warmth and
background work were not controlled, so these are operational observations,
not an isolated causal speedup benchmark or proof of 1 Gbit/s throughput.

The live media upload still shares its USB source disk with that local
migration. Short samples varied substantially: one SSD acknowledgement sample
reached 90 MB/s, while a later 40-second TCP sample averaged 279 Mbit/s. Source
sampling showed blocking file reads, and the shared disk delivered roughly
92–101 MB/s during that sample. Do not substitute the NVMe fixture result for
the live media copy's sustained rate.

## SSD acknowledgement is distinct from remote durability

The old 64 GB journal approached its capacity and reported
`zerofs_writeback_ssd_admission_paced = 1`. Expanding the bounded SSD journal
to 256 GB, while retaining a 128 GB free-space floor, cleared that admission
condition on this filesystem. The profile retains 4 GB writeback RAM,
SSD acknowledgement, local shutdown flushing and ten SFTP connections.

The clean disk cache is a separate 200 GB allocation. Check actual filesystem
headroom before applying this profile; a larger writeback tier absorbs a
finite burst but cannot make a slow backend drain faster. The observed remote
drain remained slower than foreground ingestion, with no sampled retry
increase or terminal error after recovery. This investigation does not prove
near-gigabit Storage Box durability.

The old server's controlled shutdown exited nonzero after its listener-drain
grace elapsed and its SFTP pool closed during remote work. Its sole durable
journal was preserved. The updated server recovered local sequence 7,710,043,
covering every acknowledged write observed before shutdown, and resumed replay
without a terminal/status error. Do not erase a journal or clear an error flag
to make a deployment appear healthy. Follow the existing deployment drain and
recovery gates; this incident is not a general authorization to restart a busy
server.

For a move, retain source files until the complete destination inventory and
byte sizes, original source identities, correct destination, and a captured
remote durability cutoff are independently verified. Size the bounded wait
for the pending remote backlog; increasing SSD capacity does not justify
deleting sources at the local acknowledgement boundary. All generated test
payloads, readbacks, packet captures and installation staging were removed.
