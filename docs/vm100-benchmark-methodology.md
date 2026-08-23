# VM100 benchmark methodology

This document defines the maintained benchmark contract implemented by
`scripts/vm100-pilot.py`. It distinguishes the legacy NBD/XFS pilot from the
non-lifecycle-mutating NFS and 9P benchmark paths.

## Safety boundary

The `setup`, `teardown`, `restart`, `migrate-striped`, `reset-fresh`,
`performance-matrix`, and `real-world-matrix` commands belong to the isolated
legacy NBD pilot. They must not be pointed at the production shared NFS mount.

The `protocol-matrix` command:

- accepts only `nfs` or `9p`; VM100 NBD is not a protocol-matrix option;
- requires an already-mounted, explicitly configured filesystem;
- verifies the exact `findmnt` target, source, filesystem type, and required
  mount options before creating a file;
- never mounts a filesystem, starts or stops ZeroFS, deploys code, or changes a
  service;
- creates only a UUID-named direct child of the configured test mount and a
  UUID-named scratch directory;
- removes the exact recorded files, performs cleanup twice, and asserts that
  every owned resource is absent.

Do not use an active production namespace for destructive, crash, or filesystem
acceptance tests. Those belong in the separately isolated tiered-writeback E2E
harness described by the unified rollout plan. A production observer may read
metrics, but it must not pretend that observation is a writable benchmark.

## Real-world matrix cache state

`real-world-matrix` read cells are labeled `cold` or `warm` by client-side
cache handling only. A `cold` cell issues `sync -f` on the mountpoint and then
runs fio with `--invalidate=1`, dropping the client's page cache for the
target file before the timed read starts; a `warm` cell skips invalidation.
"Cold" refers strictly to the client page cache — ZeroFS's own server-internal
caches are never reset by a cell, since that would require a server restart.

## Scenario registry

List the immutable shipping registry without loading VM100 configuration or
creating a result directory:

```console
python3 scripts/vm100-pilot.py list-scenarios
```

The registry contains only nonzero work or real sampling:

- `protocol-matrix-nfs`
- `protocol-idle-read-nfs`
- `protocol-matrix-9p`
- `raw-sftp-stock-hpn`
- `memory-envelope`

Unknown scenario names fail closed. The protocol CLI has no `nbd` choice.
Missing runtime authority reports the scenario as unavailable and exits with an
error; the harness does not silently benchmark a local directory.

## NFS and 9P authority

Each protocol needs the mount values plus a server-emitted metrics identity.
Values are read only for the chosen protocol.

| Protocol | Mountpoint | Endpoint/source | Required options | Metrics URL and identity prefix |
| --- | --- | --- | --- | --- |
| NFS | `ZEROFS_BENCH_NFS_MOUNTPOINT` | `ZEROFS_BENCH_NFS_ENDPOINT` | `ZEROFS_BENCH_NFS_MOUNT_OPTIONS` | `ZEROFS_BENCH_NFS_METRICS_*` |
| 9P | `ZEROFS_BENCH_9P_MOUNTPOINT` | `ZEROFS_BENCH_9P_ENDPOINT` | `ZEROFS_BENCH_9P_MOUNT_OPTIONS` | `ZEROFS_BENCH_9P_METRICS_*` |

The NFS endpoint must contain a literal IP, for example `192.0.2.10:/test`.
A mutable SSH or shell alias is not host authority. The endpoint must match
`findmnt SOURCE` exactly. The configured options are a required subset of the
mounted options. Use the exact 9P source reported by `findmnt`; a missing 9P
mount is an honest unavailable result, not permission to create a local
substitute. NFS metrics must use the same literal server IP as the mount source.
The supported 9P scenario is explicitly local and therefore requires a loopback
metrics URL. Metrics URLs must use HTTPS with normal certificate validation;
plain HTTP is unavailable because an identity label on an unauthenticated
response cannot bind durability evidence. Address equality is not enough to
attribute durability evidence.
URL userinfo, query strings, and fragments are rejected so credentials cannot
enter a persistent manifest. Authentication belongs in transport configuration,
not in serializable benchmark authority.
The harness also requires an exact match between three configured identity
values and exactly one server-emitted series:

```text
zerofs_benchmark_authority_info{server_instance_id="...",filesystem_id="...",export_id="..."} 1
```

For each protocol prefix, set `METRICS_INSTANCE_ID`, `METRICS_FILESYSTEM_ID`,
and `METRICS_EXPORT_ID` to those immutable server values. A same-host listener
with a different identity is rejected before any test path is created. A server
build that does not export this identity is honestly unavailable; host/port
equality alone is never durability authority.

Example against deliberately prepared test mounts:

```console
ZEROFS_BENCH_NFS_MOUNTPOINT=/mnt/zerofs-test-nfs \
ZEROFS_BENCH_NFS_ENDPOINT=192.0.2.10:/test \
ZEROFS_BENCH_NFS_MOUNT_OPTIONS=rw,hard,vers=3,proto=tcp \
ZEROFS_BENCH_NFS_METRICS_URL=https://192.0.2.10:9567/metrics \
ZEROFS_BENCH_NFS_METRICS_INSTANCE_ID=instance-uuid \
ZEROFS_BENCH_NFS_METRICS_FILESYSTEM_ID=filesystem-uuid \
ZEROFS_BENCH_NFS_METRICS_EXPORT_ID=nfs-test-root \
python3 scripts/vm100-pilot.py protocol-matrix --protocol nfs

ZEROFS_BENCH_9P_MOUNTPOINT=/mnt/zerofs-test-9p \
ZEROFS_BENCH_9P_ENDPOINT=zerofs-test \
ZEROFS_BENCH_9P_MOUNT_OPTIONS=rw,trans=unix,access=client \
ZEROFS_BENCH_9P_METRICS_URL=https://127.0.0.1:9567/metrics \
ZEROFS_BENCH_9P_METRICS_INSTANCE_ID=instance-uuid \
ZEROFS_BENCH_9P_METRICS_FILESYSTEM_ID=filesystem-uuid \
ZEROFS_BENCH_9P_METRICS_EXPORT_ID=zerofs-test \
python3 scripts/vm100-pilot.py protocol-matrix --protocol 9p
```

These examples are configuration shapes, not evidence that either mount is
available. The preflight check is authoritative at runtime.
The maintained 9P matrix is local Unix transport only: `trans=unix` is required,
and the server-emitted export ID must exactly equal the observed `findmnt`
source. Remote TCP 9P is unavailable rather than paired with unrelated loopback
metrics.

## Matched protocol workload

NFS and 9P use the same immutable workload geometry:

| Name | Logical bytes | Pattern |
| --- | ---: | --- |
| `sequential-64m` | 67,108,864 | incompressible random bytes |
| `sequential-1g` | 1,073,741,824 | incompressible random bytes |

Source generation occurs outside the timed write. Each source is locally
fsynced, sized, and SHA-256 hashed. Every protocol destination must have the
exact logical size and must reproduce that SHA-256 during protocol-visible
readback. Random bytes differ between runs, so the receipt's source digest—not
a hard-coded digest—is the content authority. The fixed size and generation
method make the shapes comparable; a receipt never claims that separate runs
used identical random contents.

The workload is synthetic. It measures sequential transfer and barrier behavior;
it is not evidence for metadata-heavy trees, application builds, sparse files,
or media-specific access patterns. Real datasets require separate receipts with
their file list, byte total, data-shape declaration, and integrity manifest.

## Timing and durability semantics

Each protocol workload records these independent cutoffs:

1. `foreground_close_ns`: source-to-destination write plus file close. This is a
   user-visible completion time, not a durability claim.
2. `fsync_or_commit_ns`: time spent in the explicit filesystem `fsync`. On an
   NFS mount this includes the client/server stable-write or COMMIT behavior
   selected by the kernel and mount; on 9P it is the client fsync request.
3. `local_cutoff_ns`: elapsed time from write start until the exported ZeroFS
   local sequence covers the accepted sequence.
4. `remote_cutoff_ns`: elapsed time until the exported remote sequence first
   covers that exact accepted sequence.
5. `stable_remote_drain_ns`: elapsed time through the lifecycle's stable drain
   gate. The drain gate requires four stable samples, equal accepted/local/remote
   sequences, zero dirty RAM and SSD, and no terminal error.

`local_cutoff_ns` and `remote_cutoff_ns` are found by polling the metrics
endpoint every 0.05 s, so each cutoff's resolution is bounded by that poll
interval plus one HTTPS round-trip to the metrics endpoint, not by a
sub-poll-interval wall-clock timestamp.

The first remote sequence crossing and stable global drain are intentionally
different fields. A close, an SFTP process exit, or an elapsed-time guess is
never relabeled as remote durability.

Readback begins only after the remote cutoff and stable drain. Its SHA-256 and
rate are separate from the foreground write rate. `readback_mibps` is an
integrity-check rate, not protocol read throughput: it hashes the just-written
file back through the same mount with no cache invalidation, so the read is
frequently served by the client's NFS or 9P cache, and the timed interval
includes SHA-256 hashing CPU cost.

## Long-idle NFS read with backend interval evidence

The ordinary protocol readback above intentionally remains a hot integrity
check. The separate recovery probe is selected explicitly:

```console
ZEROFS_BENCH_NFS_ISOLATED=true \
  python3 scripts/vm100-pilot.py protocol-matrix --protocol nfs --idle-read
```

The registered `protocol-idle-read-nfs` scenario writes one 64 MiB random file,
waits for its exact accepted sequence to reach local durability, remote
durability, and stable drain, then leaves the server's existing SFTP pool idle
for 61 minutes. The interval is intentionally longer than the one-hour
rekey/rotation horizon evaluated by the companion pool-recovery work, but this
benchmark neither requires nor claims that a rekey, reconnect, or rotation
occurred. The runner then performs one fio read with `--invalidate=1`,
`--allow_file_create=0`, a 1 MiB request size, and a 30-second subprocess
deadline. The receipt retains fio JSON and records exact bytes, request count,
runtime, rate, deadline, idle interval, and the before/after service-global
backend-byte counters. SHA-256 is checked separately after the timed read.

`--idle-read` is NFS-only and keeps the protocol-matrix safety boundary: the
harness does not deploy, restart, stop, mount, or unmount ZeroFS. The server
build must export `zerofs_sftp_object_read_bytes_total`, which counts payload
bytes returned by every successful production `SftpObjectStore` read in that
service. `ZEROFS_BENCH_NFS_ISOLATED=true` is a required, recorded operator
assertion that the deliberately prepared export has no concurrent clients or
maintenance traffic. It is a machine-enforced prerequisite, not machine proof
of isolation. The observed interval delta must be at least the benchmark's
logical byte count; a smaller delta fails closed. Even when large enough, the
delta remains service-global interval activity and is not attributed to this
request or file. The metric does not claim the remote provider served physical
media rather than its own cache, nor does it prove which pool session was
reused, retired, rekeyed, or rotated.

The fio subprocess receives a 30-second userspace deadline. If the bounded
process call returns `TimeoutExpired`, the harness writes a failed manifest and
attempts cleanup twice. This is not a wall-clock bound for a `hard` NFS mount:
an uninterruptible kernel D-state can prevent process termination and therefore
prevent `TimeoutExpired` from returning at all. In that case the harness cannot
guarantee manifest finalization or cleanup execution. The attempt receipt
records `timeout_scope=userspace_process_only` and `d_state_bounded=false` so
that limitation is machine-readable. Run this only on a deliberately prepared
isolated test export, never a shared production namespace.

## Fixed memory envelope

Add `--memory-envelope` to a protocol run only when the ZeroFS service and its
cgroup are local to the harness host and explicitly configured:

```console
ZEROFS_BENCH_CGROUP_PATH=/sys/fs/cgroup/system.slice/zerofs.service \
ZEROFS_BENCH_SERVICE=zerofs.service \
python3 scripts/vm100-pilot.py protocol-matrix --protocol nfs --memory-envelope
```

`ZEROFS_BENCH_CGROUP_ROOT` and `ZEROFS_BENCH_PROC_ROOT` exist for isolated test
roots; production defaults are `/sys/fs/cgroup` and `/proc`.

The fixed envelope is not inferred from a mutable container limit:

- cgroup current: at most 96 GiB;
- cgroup peak: at most 112 GiB;
- process RSS and high-water mark: at most 80 GiB;
- cgroup and process swap: zero.

Systemd's reported `ControlGroup` must resolve to the configured cgroup path;
service PID/RSS evidence can never be combined with a different cgroup's
counters. Samples are captured before the run, after every foreground close, fsync/COMMIT,
local cutoff, and stable remote drain, and after cleanup. Every sample records:

- cgroup `memory.current`, `memory.peak`, `memory.swap.current`;
- `memory.events` OOM and OOM-kill counters;
- pinned service PID and `NRestarts`;
- process `VmRSS`, `VmHWM`, and `VmSwap`;
- accepted/local/remote sequences, dirty tiers, and terminal writeback state.

Any PID or restart change, OOM counter increase, terminal state, swap use, or
ceiling breach fails the run. Every captured sample is written to the receipt
before validation, so the violating OOM/restart/ceiling sample survives failure.
The sampler owns no mutable resources; its cleanup
receipt says `observer-owned-no-resources`. The enclosing protocol scenario owns
and double-cleans the UUID paths.

If ZeroFS runs in a remote container and its cgroup is not mounted as explicit
read-only authority, the memory envelope is unavailable. The harness must not
sample VM100's unrelated local cgroup and label it server memory.

## Counterbalanced raw SFTP control

The raw control requires two explicit SSH executables:

```console
python3 scripts/vm100-pilot.py raw-sftp \
  --stock-ssh /usr/bin/ssh \
  --hpn-ssh /opt/zerofs/hpn/bin/hpnssh
```

The HPN executable must identify itself as an HPN build, and the stock
executable must not report HPN provenance. Stock and HPN paths and SHA-256
values must be distinct. Receipts record each absolute path, `ssh -V` output,
and executable SHA-256. Two distinct HPN binaries cannot be mislabeled as a
stock-versus-HPN comparison.

The immutable registered scenario supplies four jobs, 128 MiB per job, four
ABBA repetitions, a 1 MiB SFTP buffer, and request depth 128. The CLI does not
accept geometry overrides, so a named scenario cannot silently change shape.
All trials reuse the exact same source files and therefore the same SHA-256
tuple. Each upload and download records per-session and aggregate rates.

SFTP is invoked with `-S` to select the recorded SSH executable, strict host-key
checking, the configured known-hosts file, no compression, and the same endpoint
and URL prefix for both variants. Endpoint authority records the known-hosts file
SHA-256; the configuration's hostname is never treated as identity by itself.

An SFTP batch process exit is labeled `close_ack`. Raw SFTP does not have access
to ZeroFS object-writeback coverage and therefore records remote durability as
`not_measured`. The control cannot prove durable parity with NFS, 9P, CLI, or
ZeroFS writeback.

Downloads go to local files and must match every source SHA-256. `/dev/null`
downloads are not accepted as integrity evidence. Each remote UUID directory is
cleaned twice and probed for absence. The local UUID scratch path is also cleaned
twice and asserted absent. Cleanup failure makes the command fail, while the
original transfer error remains primary.

Metadata batches have a bounded 60-second-or-configured-stop deadline. Parallel
transfer phases have a bounded 900-second-or-configured-drain deadline. A
deadline terminates every spawned process group, preserves the failed receipt,
runs both cleanup scopes, and restores the stopped pilot stack.

The raw control stops and restores only the separately configured legacy pilot
stack to avoid account-session contention. It must not be configured against a
production service unit.

## Receipts and interpretation

Persistent receipts live under the configured result directory. Each new
scenario writes:

- `manifest.json`, including partial failure state;
- `summary.json` for successful runs;
- `cleanup-ledger.json` with every owned local and remote resource;
- per-session SFTP logs where applicable.

Synthetic throughput is accepted only with exact bytes, SHA-256, the named
cutoffs, terminal-state evidence, and successful cleanup. “Unavailable,”
“not measured,” and “not durable” are valid outcomes; zero work, missing
authority, missing hashes, or fabricated durability are not.

Unit tests exercise registry rejection, authority matching, real local byte/SHA
operations, counterbalancing, SFTP orchestration, memory parsing and ceilings,
failure restoration, and cleanup ledgers. Unit tests do not mount devices, run
live network benchmarks, or start/stop production ZeroFS.

The canonical `MetricsClient` used by legacy drain, benchmark, profile,
performance-matrix, and real-world-matrix paths applies the same HTTPS and
per-response identity rule. With no configured expected identity it pins the
first authenticated server/filesystem/export tuple for the process and rejects
every later drift; an identity-free response always fails.
