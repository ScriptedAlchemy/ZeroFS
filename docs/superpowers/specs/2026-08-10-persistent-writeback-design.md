# Persistent RAM-to-SSD-to-Remote Writeback for ZeroFS

Status: approved design for the `feat/persistent-writeback` implementation lane.

## Context

The SFTP-backed ZeroFS deployment currently has two useful but different cache layers:

- client kernel/FUSE page cache and bounded in-process segment buffers;
- ZeroFS Foyer RAM/SSD read caches for already-published backend objects.

Neither is a persistent dirty-data tier. Once the bounded segment-upload pipeline fills, foreground writes fall to the Hetzner Storage Box drain rate. Live measurements on the target deployment establish the gap:

- local `/fast` ZFS, 4 GiB incompressible buffered write: 1,833 MiB/s foreground; 4.80 s final sync;
- ZeroFS native client, repeated 4 GiB incompressible write: 79.9 MiB/s foreground; 17.60 s final sync;
- host ZeroFS FUSE plus VirtioFS: 82.3 MiB/s foreground; 0.61 s final sync;
- the repeated native run caused 4,296,427,260 SFTP bytes to be acknowledged, confirming that foreground backpressure was coupled to remote upload.

The desired behavior is an explicit three-tier dirty-data path:

`client page cache -> ZeroFS dirty RAM -> durable local SSD journal -> SFTP Storage Box`

The existing Foyer RAM/SSD read caches remain separate and continue serving clean data.

## Goals

1. Ordinary writes can be acknowledged from a configurable volatile RAM tier.
2. `fsync` and `sync` can establish local SSD durability without waiting for Hetzner.
3. A separate remote barrier can prove that every accepted mutation through a sequence point is durable on the Storage Box.
4. Pending writes, overwrites, and deletes remain immediately visible through GET, HEAD, LIST, and ranged reads.
5. Restarting CT105 recovers every SSD-journaled mutation and resumes upload without namespace corruption.
6. RAM exhaustion spills to SSD; SSD exhaustion applies bounded backpressure at remote-drain speed instead of filling the host filesystem.
7. Remote publication never exposes metadata that refers to data objects that have not been published.
8. Existing SFTP connection limits remain authoritative: eight physical sessions, at most seven write and seven read operations, with four or seven deployment workers as configured.
9. Operators can see RAM bytes, SSD bytes, pending age, local and remote durable watermarks, upload rate, retries, failures, and backpressure.
10. The feature is disabled by default and is explicitly opt-in because its local durability boundary differs from normal object-store semantics.

## Non-goals

- This is not replication of the dirty journal. Losing the host SSD can lose locally durable mutations that have not reached the Storage Box.
- `ack_mode = "memory"` does not survive a process, container, host, or power failure until the affected operations cross the local SSD barrier.
- This does not make the current SFTP conditional-update implementation safe for multiple read-write ZeroFS servers. Exactly one read-write daemon may own a prefix.
- This does not replace the clean Foyer read cache, implement block-level NBD caching, or make `rm -rf` a constant-time operation.
- This does not promise that a remote-only restore contains the latest locally acknowledged writes unless a remote barrier completed.

## Chosen approach

Implement a full `ObjectStore` writeback overlay around the remote SFTP store. A segment-only spool is rejected because SlateDB metadata would retain remote-latency and recovery-order gaps. An NBD cache is rejected for this feature because it changes the filesystem topology and single-owner semantics rather than improving the existing ZeroFS namespace.

The overlay owns:

- a sequence-numbered volatile mutation queue;
- a crash-safe SSD journal and external blob files;
- a read-your-writes namespace overlay;
- a remote scheduler with dependency fences;
- local and remote barrier handles;
- capacity accounting, metrics, shutdown, and recovery.

## Deployment profile

CT105 will use 64 GiB RAM:

- 16 GB existing ZeroFS clean read cache;
- 16 GB dirty writeback RAM;
- the remainder for open/sealing segments, SlateDB, Foyer indexes and queues, SSH/SFTP buffers, allocator overhead, and kernel page cache.

The host bind mount backing `/var/cache/zerofs-storagebox-arc` currently has about 2.7 TiB free. The initial deployment budgets are:

- 512 GB existing clean SSD read cache;
- 512 GB dirty SSD journal;
- 256 GB mandatory host-filesystem free-space reserve.

The dirty journal's effective maximum is the lesser of its configured capacity and available filesystem space above the reserve. The daemon must stop accepting new dirty bytes before the reserve is breached.

## Configuration

```toml
[writeback]
enabled = true
dir = "/var/cache/zerofs-storagebox-arc/writeback"

# remote | ssd | memory
ack_mode = "memory"
memory_size_gb = 16.0
disk_size_gb = 512.0
min_free_gb = 256.0

high_watermark_percent = 95
resume_percent = 85
upload_concurrency = 7

# local | remote
shutdown_flush = "local"
```

Validation rules:

- `memory_size_gb > 0` when `ack_mode = "memory"`;
- `disk_size_gb > 0` for `memory` or `ssd` modes;
- `0 < resume_percent < high_watermark_percent <= 100`;
- `upload_concurrency <= sftp.write_concurrency` for SFTP;
- writeback is rejected for read-only/checkpoint servers;
- writeback and HA read-write mode are mutually exclusive in the first implementation;
- the writeback directory must be local, writable, on one filesystem, owned by the ZeroFS service account, and not nested inside Foyer's managed cache directories.

The default remains `enabled = false`. When enabled without an explicit mode, the default is `ack_mode = "memory"`: acknowledgements return at RAM speed while client flush barriers (fsync/FUA/COMMIT) still force SSD durability, matching ordinary volatile write-cache semantics. Deployments that want every acknowledgement SSD-durable select `ssd` explicitly.

## Durability contract

Three sequence watermarks define durability:

- `accepted_seq`: newest mutation accepted into the in-process overlay;
- `local_seq`: greatest contiguous sequence durably committed to SSD;
- `remote_seq`: greatest contiguous sequence durably applied to the Storage Box.

The contract is:

- `ack_mode = "memory"`: a normal object mutation returns after its bytes and namespace effect are owned by the RAM overlay and assigned a sequence number;
- `ack_mode = "ssd"`: it returns only when `local_seq` covers that mutation;
- `ack_mode = "remote"`: it preserves the existing behavior and returns only when `remote_seq` covers it;
- filesystem `fsync`/`sync`: after ZeroFS seals data and flushes metadata, obtains a barrier sequence and waits until `local_seq >= barrier`;
- `zerofs flush --remote`: obtains a barrier sequence and waits until `remote_seq >= barrier`;
- clean shutdown with `shutdown_flush = "local"`: stops admission, seals/flushed filesystem state, and advances `local_seq` through the shutdown barrier before exit;
- clean shutdown with `shutdown_flush = "remote"`: also waits for `remote_seq`.

The existing client-visible `fsync` must never return merely because data is in RAM. The local SSD journal is therefore the minimum `fsync` durability boundary even in unsafe memory mode.

## Mutation model

Every mutation receives a monotonically increasing `u64` sequence and an operation UUID. A record contains:

- format version;
- sequence and operation UUID;
- object path;
- operation type (`put`, `delete`, `copy`, `rename`);
- `PutMode` and expected visible version for conditional writes;
- locally assigned ETag/version;
- payload length and SHA-256 for puts;
- external blob path when a payload exists;
- timestamps and retry/error state;
- remote predecessor and resulting ETag when known;
- fence classification.

Mutation sequences are never reused. The journal persists a contiguous prefix: sequence `N + 1` cannot become locally committed until `N` is committed. This makes RAM-loss recovery a consistent prefix rather than an arbitrary subset.

No mutation coalescing is performed in the first version. Preserving the complete per-key chain makes conditional updates, crash replay, and audit behavior explicit.

## SSD journal layout

```text
writeback/
  LOCK
  journal.redb
  blobs/
    <sequence-shard>/<operation-uuid>.blob
  tmp/
```

`redb` is the selected pure-Rust transactional metadata store. Large object payloads remain external blob files so journal transactions stay small.

To commit one mutation locally:

1. write the payload to a unique `tmp/` file with mode `0600`;
2. verify its length and SHA-256;
3. `sync_all` the file;
4. atomically rename it into `blobs/`;
5. fsync the containing blob directory;
6. commit the mutation record and contiguous `local_seq` advancement in a durable redb write transaction;
7. notify local-barrier waiters.

Journal directories are mode `0700`. Startup rejects symlinks, wrong ownership, an unsupported format version, missing committed blobs, length/hash mismatch, sequence gaps, or an unavailable exclusive lock. It does not silently skip corruption.

The object bytes reaching this layer are already ZeroFS-compressed and encrypted. Paths, lengths, sequencing metadata, and operation types remain visible locally.

## RAM tier

The RAM tier stores accepted mutation records and payload bytes until the journaler commits them to SSD. It is independent of the clean read cache and bounded by `memory_size_gb`.

Admission behavior:

1. accept into RAM while the configured dirty-RAM budget has room;
2. wake the SSD journaler immediately;
3. when RAM is full, writers wait for SSD commits to free RAM;
4. when SSD usage reaches the high-water mark or the free-space reserve, the journaler cannot free RAM faster than the remote scheduler frees SSD, so foreground throughput naturally falls to remote drain speed;
5. admission resumes normally below the resume watermark.

The implementation must account for payload capacities before allocation and must not exceed the configured dirty-RAM budget by accepting several concurrent large puts.

## Namespace overlay

The newest accepted mutation for a key is authoritative over the remote store:

- GET/ranged GET reads pending payload bytes from RAM or the SSD blob;
- HEAD returns pending length, timestamp, and local ETag;
- a pending delete returns `NotFound`;
- LIST merges remote objects with pending puts and removes pending tombstones;
- COPY and RENAME resolve their source through the same overlay;
- multipart completion becomes one atomic put mutation; incomplete multipart uploads remain private and abortable.

The overlay remains authoritative after the remote uploader applies an operation until the contiguous remote watermark and local cleanup transaction both advance past it. Cleanup cannot create a visibility gap.

Local ETags use an unambiguous namespace such as `wb:<journal-incarnation>:<sequence>`. `PutMode::Create` tests the current overlay-visible namespace. `PutMode::Update` must match the newest visible local or remote version under a per-key lock. Stale local versions fail before journal admission.

## Remote ordering and CAS

Large immutable data must upload concurrently, while causal metadata publication must remain ordered.

The remote scheduler uses seven workers and two classes:

- parallel immutable operations: `PutMode::Create` for new segment objects and explicitly recognized SlateDB SST/WAL-like immutable paths;
- fences: every conditional update, overwrite, delete, rename, copy with namespace effects, unrecognized mutation, and explicit barrier.

Classification is fail-safe: only a known immutable create may bypass serial publication. `PutMode::Overwrite` is always a fence in the first implementation, even if a caller currently uses it for an immutable key.

A fence waits for every earlier sequence to finish remote publication, publishes itself, advances the contiguous remote watermark, and only then releases later work. Consequently, a SlateDB manifest/leader update cannot become visible before earlier segment/SST objects. Later data may upload only after the preceding fence is applied.

Per-key remote operations are always FIFO. A chain of local ETags maps to the remote ETag returned by the preceding successful publication. The first pending operation records the visible remote predecessor. Any unexpected remote predecessor or external modification poisons writeback and stops new writes rather than overwriting divergent state.

If a process loses the reply after remote publication, recovery performs idempotency verification:

- if the expected precondition still applies, retry normally;
- if the remote version advanced, compare length and SHA-256 with the staged payload;
- an exact match marks the operation applied and resumes;
- a mismatch is remote divergence and fails closed.

This expensive full verification is a recovery-only path. For immutable objects, an already-existing exact object is success; an existing mismatched object is corruption/conflict.

## Object-store stack

The serving data path becomes:

```text
ZeroFS / SlateDB / SegmentStore
  -> prefix, length-check, prefetch as applicable
  -> WritebackObjectStore (overlay, local barriers, scheduler)
  -> RetryingObjectStore (remote attempts only)
  -> TracingObjectStore (otrace sees actual remote requests)
  -> SftpObjectStore
```

Bucket identity, storage compatibility probing, and encryption-key loading occur before enabling writeback so the journal is bucket-scoped and normal startup compatibility failures are not hidden. The resolved bucket ID is part of the journal identity. A journal created for another bucket, prefix, backend endpoint, or encryption-key identity is rejected.

`StartupContext` and `InitResult` retain a typed `WritebackLifecycle` handle, as they now do for SFTP lifecycle ownership. Filesystem flush paths use it for local barriers; CLI/RPC status and remote flush use it for remote barriers; server shutdown drains it before the SFTP pool.

## Recovery

Startup recovery proceeds before the database opens for read-write service:

1. obtain the exclusive journal lock;
2. remove only uncommitted files from `tmp/`;
3. open and validate journal format and identity;
4. verify every committed blob referenced after `remote_seq`;
5. reconstruct the newest-key overlay and capacity counters;
6. reconcile possibly-applied remote operations at the upload frontier;
7. start SSD and remote workers;
8. expose the object store to SlateDB and ZeroFS;
9. begin serving only after read-your-writes recovery is complete.

RAM-accepted operations above `local_seq` are allowed to disappear after an unclean restart. Because local persistence is strictly prefix ordered, the recovered state is the last complete local prefix. Every client `fsync` completed before the crash must be at or below that prefix.

## Shutdown and failure behavior

Shutdown order is:

1. reject new filesystem requests;
2. drain/abort server background callers under the bounded shutdown policy;
3. seal and flush ZeroFS/SlateDB;
4. obtain a writeback barrier;
5. drain RAM to SSD through that barrier;
6. optionally drain SSD to remote;
7. stop writeback workers and close the journal;
8. shut down the SFTP pool and reap every SSH child;
9. report shutdown complete.

An SSD write/fsync failure, journal corruption, remote divergence, or inability to preserve the configured free-space reserve fails the writeback store closed. Existing readable data remains available where safe, but new mutations are rejected with a typed error. Infinite retries are forbidden for terminal local failures.

A remote outage does not fail ordinary writes until RAM/SSD capacity is exhausted. Remote errors remain visible in metrics and status. Once capacity is exhausted, callers block with cancellation support; they do not receive false success or unbounded memory growth.

## Operational interface

Extend the existing admin RPC and CLI with:

```text
zerofs writeback status -c /etc/zerofs/storagebox-arc.toml
zerofs flush --local -c /etc/zerofs/storagebox-arc.toml
zerofs flush --remote -c /etc/zerofs/storagebox-arc.toml
```

`zerofs flush` without a flag preserves the documented filesystem flush behavior and uses the configured writeback acknowledgement mode. Operators must use `--remote` before migration, journal removal, disaster-recovery validation, or disabling writeback.

Prometheus metrics include:

- accepted, local, and remote sequence watermarks;
- RAM/SSD dirty bytes and operations;
- oldest pending age;
- remote bytes and operations completed;
- worker activity, retries, terminal errors, and last-success timestamp;
- local and remote barrier latency;
- RAM-full, SSD-high-water, and reserve backpressure time;
- recovery reconciliation counts and duration.

The unauthenticated admin RPC must be Unix-socket-only or firewall-restricted before these controls are enabled.

## Test strategy

Implementation follows strict RED/GREEN cycles. Required automated tests include:

### Model and namespace

- generated mutation sequences match an in-memory reference namespace for GET, HEAD, LIST, range, create, update, delete, copy, and rename;
- pending puts and tombstones override remote state;
- local stale ETags fail and same-key operations remain FIFO;
- multipart completion is one visible mutation; abort leaves no visible or billed local payload.

### Local durability

- memory mode returns before SSD completion;
- SSD mode waits for local durability;
- `fsync` in memory mode waits for the local barrier;
- SSD persistence is a contiguous sequence prefix under concurrent puts;
- injected crashes after temp write, file fsync, rename, directory fsync, and redb commit recover to the specified prefix;
- committed missing/corrupt blobs fail startup;
- temporary and uncommitted files are cleaned without touching committed data.

### Remote ordering

- immutable uploads run concurrently up to the configured limit;
- conditional updates and deletes fence every earlier sequence;
- manifest publication never precedes referenced data completion;
- lost successful replies reconcile by exact content;
- mismatched remote content and unexpected ETags poison the store;
- remote watermark advances only contiguously.

### Capacity and cancellation

- exact RAM accounting prevents concurrent oversubscription;
- RAM-full writers resume after SSD spill;
- SSD high-water and free-space reserve block admission until remote cleanup;
- canceled blocked callers leave no mutation, permit, task, or file;
- shutdown wakes blocked callers and leaves no worker or SSH child.

### Integration

- restart with pending SSD data preserves read-your-writes and resumes upload;
- SIGKILL after completed client `fsync` preserves the fsynced tree;
- SIGKILL before local barrier may lose recent writes but mounts a consistent prefix;
- Storage Box outage permits writes through RAM and SSD capacity, then backpressures;
- recovery and remote drain do not exceed eight SFTP sessions or seven writes;
- gitleaks, fmt, clippy, library tests, binary tests, and focused race/fault tests pass from a clean worktree.

## Live acceptance in CT105 and VM100

The feature is not production-ready until these receipts exist:

1. CT105 has 64 GiB RAM and the configured dirty/read budgets without memory pressure or swap growth.
2. A 4 GiB incompressible buffered write in memory mode is at least 500 MiB/s foreground and is materially faster than the 60-85 MiB/s remote path.
3. A workload larger than the RAM tier visibly transitions to SSD-rate admission without exceeding the RAM budget.
4. A reduced-capacity test forces the SSD high-water transition and shows network-rate backpressure without ENOSPC.
5. Background SFTP drain sustains at least 60 MB/s when the Storage Box is healthy, subject to a longer queued test rather than a short burst.
6. `flush --local` and filesystem `fsync` survive a ZeroFS process/container restart.
7. `flush --remote`, followed by a clean journal-disabled read-only mount, reproduces checksums from Storage Box alone.
8. The npm benchmark remains locally responsive while uploads drain, and normal `rm -rf` remains namespace-correct.
9. Host FUSE plus VirtioFS remains the canonical topology unless a separately measured client path wins both performance and recovery tests.
10. Every pilot file, mount, test namespace, and temporary service is removed after acceptance.

## Deployment and rollback

Deployment begins only from a clean, reviewed SFTP-hardening commit. Enablement on the existing prefix requires:

1. remote-flush and cleanly stop the old binary;
2. back up configuration and verify the existing remote sentinel;
3. install the new binary and writeback configuration;
4. start with an empty journal and verify identity;
5. run smoke, crash, capacity, and remote-drain tests in an isolated subtree;
6. expose the canonical mount only after acceptance.

Rollback requires `zerofs flush --remote`, zero pending operations/bytes, a clean stop, and verification that the remote-only namespace contains the sentinel and test checksums. Deleting or bypassing a non-empty journal is never an automatic rollback step.
