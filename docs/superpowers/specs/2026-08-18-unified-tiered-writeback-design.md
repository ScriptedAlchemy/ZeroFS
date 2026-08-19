# Unified Tiered Writeback for NBD, NFS, and 9P

Status: approved design for the `codex/unified-tiered-writeback` implementation lane.

## Context

ZeroFS currently has two different write acknowledgement paths:

- NFS, 9P, WebUI, and materialized NBD writes pass through the shared filesystem
  implementation before acknowledgement. They already share extent management,
  quota checks, metadata, segment creation, SlateDB, object-level writeback, and
  `FlushCoordinator` durability.
- NBD `volatile_memory` mode has a separate pre-codec RAM overlay. It can
  acknowledge at RAM speed, but it is intentionally exclusive because other
  protocols cannot see its pending bytes and cannot include them in their
  durability barriers.

The object-level writeback subsystem already provides ordered RAM-to-SSD-to-remote
publication, crash recovery, local and remote watermarks, a coherent object-store
overlay, capacity accounting, failure propagation, and remote scheduling. It does
not provide a shared pre-codec filesystem mutation overlay, so it cannot by itself
give NFS and 9P the NBD volatile acknowledgement behavior.

The production cache profile also exposes a separate admission problem. The dirty
SSD tier uses 95/85 percent hysteresis. Once the high watermark is crossed, writers
are stopped until the entire hysteresis band drains. On a hard NFS mount that looks
like a multi-minute server hang, causes RPC retransmission amplification, and then
admits another large burst. The desired steady-state behavior is a smooth staircase:

`bounded RAM burst -> bounded SSD-rate admission -> remote-drain-rate admission`

The 2026-08-19 production incident adds a distinct resident-memory constraint. A
96 GiB, no-swap CT198 cgroup killed ZeroFS while dirty writeback RAM remained zero.
The configured 64 GiB logical clean-cache payload had filled and stabilized, then
process/jemalloc residency rose by roughly another 30 GiB in about three minutes
under concurrent NFS writes, 9P/WebSocket traffic, and segment GC. Restart discarded
the process-local 9P replay identities, so a retried mutation correctly failed closed
as `EOPIDSTALE`. This proves that successful RAM-to-SSD drain and payload-only cache
limits are not a server-resident-memory safety proof. The implementation must account
for cache metadata and replacement overlap, allocator overhead, protocol request
bodies, maintenance working sets, and explicit headroom before production reuse.

The namespaces remain deliberately separate. An XFS filesystem inside an NBD
export is not the same namespace as ZeroFS files served directly over NFS or 9P.
This design unifies write admission, coherence, durability, errors, and shutdown;
it does not merge those namespaces.

## Goals

1. Put the volatile acknowledgement boundary below NBD, NFS, 9P, and WebUI so
   every writable protocol observes the same pending file data and ordering.
2. Keep the existing safe materialized mode as the default and make shared volatile
   acknowledgement an explicit opt-in.
3. Preserve the existing filesystem implementation as the sole authority for
   permissions, quota, timestamps, extents, compression, encryption, metadata,
   and storage; volatile acknowledgement follows canonical validation and quota
   reservation, never precedes it.
4. Bound all accepted volatile bytes and operations globally across protocols.
5. Make file growth immediately visible while pending and preserve coherent reads
   through every inode alias.
6. Give NBD FLUSH/FUA, NFS COMMIT, 9P fsync/fsyncdur, filesystem sync, and the
   administrative remote flush precise, typed durability cutoffs.
7. Make terminal materialization, SSD, and remote failures visible to every writer
   and barrier waiter without deadlock or fabricated success.
8. Replace bulk high/resume pauses with credit-paced admission once the SSD tier is
   near capacity, while retaining a hard physical free-space reserve.
9. Recover every SSD-durable mutation after process, container, or host restart and
   never claim RAM-only writes survived a crash.
10. Prove the behavior through cross-protocol, failpoint, restart, filesystem, and
    performance tests before merging or deploying it.
11. Keep logically sequential reads from becoming one-backend-RTT-per-segment-run:
    independent immutable runs are fetched with bounded concurrency, and NFS, 9P,
    and NBD read throughput is proved against paired raw-direction controls and
    cache-state-matched ZeroFS protocol controls.
12. Preserve the original production envelope: a configured 16 GB dirty-write RAM
    budget distinct from the 64 GB clean read cache, a 1 TB local SSD tier shared by
    the configured clean-cache and durable journal/staging budgets, and a 5 TB-class
    user-visible volume/export. Capacity reporting must keep
    sparse virtual-device geometry separate from physically allocated local and
    remote bytes.
13. Bound aggregate server-resident memory before owned protocol payload copies,
    including cache overhead/replacement, raw-parts buffers, segment/compaction work,
    request replay, dirty tiers, and a configured reserve below the effective cgroup
    or process limit. Before a scored process starts, freeze the cgroup-high-event,
    accounting-error, and unowned-residual tolerances in the run ledger; a result may
    not derive or relax them from its observed peak. A zero dirty-RAM gauge is never
    sufficient memory evidence.
14. Prove and remove the SSH/SFTP per-stream ceiling separately for each direction.
    A pinned HPN OpenSSH client may be selected by ZeroFS only after a real stock/HPN
    A/B wins; receiver-window gains must not be misreported as an upload fix.

## Production performance contract

FUSE is not a shipping frontend. ZeroFS itself owns the shared tiering used by NBD,
NFS, 9P, WebUI, and direct callers. Linux may use NBD for a fast guest filesystem and
also mount the direct NFS/9P namespace seen by the Mac; those namespaces remain
distinct even though admission, durability, caching, failures, and remote storage are
shared below the adapters.

The production-shaped acceptance profile is:

- a 4 GiB foreground write must remain entirely on the RAM/local-SSD path and must
  not collapse to remote Storage Box throughput;
- a 100 GiB copy with the configured 16 GB volatile budget must show the bounded RAM burst and
  then sustained local-SSD-rate admission while remote publication proceeds
  concurrently;
- after the configured SSD dirty capacity is genuinely exhausted, foreground
  admission must pace smoothly at measured remote drain without the 95-to-85-percent
  multi-minute stop/resume cycle;
- the local SSD leg is compared with a same-host durable local control, whose known
  production target is approximately 800-900 MB/s on the intended hardware;
- remote publication is compared with a same-endpoint, same-session-count, durable
  raw SFTP control, whose intended production target is approximately 70-100 MB/s;
  an environment whose paired raw control cannot reach that range is reported as an
  external-path limitation rather than hidden by a lower ZeroFS threshold;
- stock OpenSSH, a pinned HPN client, and ZeroFS using the same selected executable
  are compared at one and configured-many physical sessions in both directions.
  Receipts distinguish receive-window, SFTP request-depth, and session-utilization
  limits. HPN is promoted only for a repeatable winning direction and ZeroFS retains
  stock behavior by default.
- any pinned HPN candidate must pass a supervisor-bounded upstream suite with a
  recorded nonzero test inventory and focused transfer/rekey/SFTP tests. The only
  permitted upstream exclusion is the pinned tree's `dynamic-forward` regression,
  whose background multiplexed client is outside the bulk-transfer package proof;
  every test process group is terminated and reaped on deadline.
- ordinary epoch/counter-unique immutable segment objects use create-only publication,
  not overwrite ordering fences. An unexpected name collision fails closed. The real
  SegmentStore-to-writeback path, not a create-only synthetic scheduler fixture, must
  keep configured upload lanes occupied before request-window tuning is considered.

These rates are targets, not permission to weaken integrity or durability. Each result
must include the paired control, tier-occupancy timeline, exact acknowledged and
durable cutoffs, zero terminal errors, and cleanup evidence.

## Non-goals

- The initial implementation does not provide speculative volatile directory
  operations. Create, rename, unlink, link, truncate, and other namespace or size
  mutations use a materialization fence where required.
- It does not make an NBD filesystem's files visible in the direct NFS/9P namespace.
- It does not replicate the local SSD journal. Loss of the host SSD may lose data
  that reached local durability but not remote durability.
- It does not make multi-writer access to one remote prefix safe.
- It does not dynamically weaken or strengthen the acknowledgement contract when a
  tier fills. Backpressure changes latency and throughput, never the promised tier.
- It does not infer remote durability from local completion or vice versa.

## Chosen architecture

Split the canonical write operation into a preparation phase and an application
phase, then put a protocol-neutral `MutationCoordinator` between them:

```text
NBD       NFS       9P       WebUI/direct client
  \        |         |             /
        ZeroFS filesystem API
                 |
        fs::ops::prepare_write
        - deduplication and request identity
        - inode, permission, overflow, and quota validation
        - canonical attributes and quota reservation
                 |
        MutationCoordinator
        - global RAM admission
        - inode/range overlay
        - accepted/materialized cutoffs
        - metadata fences
        - terminal state and shutdown
                 |
        fs::ops::apply_prepared_write
        - prepared metadata and quota transfer
        - compression and encryption
        - extents and segments
        - SlateDB and metadata
                 |
        existing object writeback
        - RAM staging
        - durable SSD journal
        - remote SFTP publication
```

The new coordinator is not a second filesystem implementation. It accepts an opaque
`PreparedWriteBatch` produced under the existing mutation locks. A normal file write
has one member; a striped NBD request has one member per backing inode. The batch
contains canonical post-write attributes, protocol retry identity, logical quota
reservations, and one logical acceptance/result boundary. The coordinator owns only
the minimum state needed to acknowledge that validated batch before encoding and to
make all member bytes and prepared attributes atomically visible until
`fs::ops::apply_prepared_write` materializes them.

The target module boundary is:

```text
zerofs/src/fs/mutation/
  mod.rs            coordinator facade and mode selection
  admission.rs      raw mutation byte/op ownership
  data_overlay.rs   inode/range writes, growth, and coherent reads
  materializer.rs   ordered application through existing fs::ops
  progress.rs       cutoffs, terminal failure, waiters, and shutdown
  durability.rs     typed mutation/object coverage barriers
  request_cache.rs  bounded fingerprints, retained results, and replay lifetime
```

The completed feature replaces `nbd/volatile_overlay.rs`; the NBD adapter becomes
another caller of the shared coordinator. Existing `writeback/` remains the
object-level SSD journal and remote publisher.

## Configuration and compatibility

Materialized mode remains the default. The exact new filesystem settings are:

```toml
[filesystem]
# materialized (default) | volatile_memory
write_ack_mode = "materialized"

# Required only for volatile_memory. This is the global raw-mutation RAM budget
# shared by NBD, NFS, 9P, and WebUI/direct filesystem clients.
volatile_memory_gb = 16.0

# Hard cap for active prepared batches and retained duplicate-request results.
volatile_max_operations = 65536

# Aggregate protocol bodies waiting before or inside canonical preparation. The
# byte permit is acquired before a decoded NFS/9P/WebUI payload becomes owned.
protocol_inflight_memory_gb = 4.0
protocol_inflight_max_operations = 4096

[memory]
# Explicit aggregate process envelope. On Linux the discovered cgroup memory.max,
# when finite, is an additional upper bound; incompatible budgets fail startup.
resident_limit_gb = 128.0
resident_reserve_gb = 16.0

[sftp]
# Optional absolute executable used for the owned SFTP ssh child. Omission keeps
# the existing stock `ssh` lookup. No arbitrary argument string is accepted.
# ssh_program = "/opt/zerofs/hpn-ssh/e2dfa0cea55d93747f4c68b4a2b134d6fbe0db06/bin/hpnssh"
```

`[filesystem].write_ack_mode` controls when a filesystem write may return:

- `materialized`: the existing canonical filesystem apply completes before reply;
- `volatile_memory`: canonical validation and reservation complete, then the shared
  RAM overlay owns the prepared mutation before reply.

This is deliberately separate from `[writeback].ack_mode`, which controls when an
object-store mutation issued by canonical materialization returns from the existing
object writeback layer. `memory`, `ssd`, and `remote` remain valid object-layer
policies. None changes the meaning of a filesystem fsync/COMMIT/FLUSH barrier.

The normalized configuration matrix is:

| Filesystem acknowledgement | Object writeback | Result |
| --- | --- | --- |
| `materialized` | disabled | Existing direct backend path |
| `materialized` | enabled, any object ack mode | Existing canonical path plus configured object writeback |
| `volatile_memory` | enabled, any object ack mode | Shared volatile filesystem path; barriers still reach local SSD |
| `volatile_memory` | disabled | Invalid: no local SSD durability target exists |

Shared volatile mode also requires a read-write single-writer server,
`ignore_fsync = false`, no read-write replication, a finite positive RAM budget
large enough for every enabled protocol's maximum write, a positive bounded operation
cap, and a functional local SSD journal. The duplicate-request cache uses the same
operation cap; zero-length writes and retained results therefore remain bounded even
when they charge no payload bytes. The generated configuration shows `materialized`;
omission selects it.

The aggregate resident envelope applies in both materialized and volatile modes.
Startup computes a conservative upper bound for every configured payload owner plus
entry/key/allocator overhead and the explicit reserve. A finite Linux cgroup limit is
also read as an upper bound. Configuration fails closed when those values cannot fit;
the server never silently reduces a requested durability or cache contract. Runtime
admission uses the same ownership model and poisons or backpressures before exceeding
the hard envelope.

`[sftp].ssh_program`, when present, must be an absolute, executable regular file.
ZeroFS records its canonical path, version, and binary digest without allowing extra
shell arguments. Global `PATH`, `update-alternatives`, and the system `ssh` binary are
never changed by this feature. A pinned HPN executable remains optional and must pass
the same strict host-key and key-only authentication policy as stock OpenSSH.

Existing `[servers.nbd] write_ack_mode = "volatile_memory"` and
`volatile_memory_gb` are deprecated migration inputs. If the new filesystem fields
are omitted, the legacy pair normalizes to shared volatile mode. If both forms are
present, their modes and budgets must agree exactly or validation fails. Legacy
`materialized` has no effect on an explicitly configured shared setting. After one
release window the NBD-only fields and exclusivity rule are removed.

The configuration must keep these budgets distinct:

- clean read-cache RAM and SSD;
- clean-cache entry/key/allocator overhead and replacement overlap;
- raw volatile mutation RAM and operation count;
- decoded protocol request RAM and operation count before and during preparation;
- open/sealing segment memory;
- raw-parts/Foyer buffers and GC/compaction working memory;
- object-writeback RAM and operation count;
- dirty SSD journal/staging bytes and operations;
- immutable process-resident reserve below the effective configured/cgroup limit;
- mandatory physical filesystem free-space reserve.

The production default remains canonical-materialization-before-ack. This is not a
claim of SSD durability: only a completed `LocalSsd` barrier is locally durable.
The unsafe RAM-ack mode remains visibly named and opt-in in generated configuration,
validation errors, status output, metrics, and documentation.

No on-disk filesystem format change is required for the first implementation. The
new RAM overlay is volatile. Canonical materialization continues to create the
existing extent, segment, SlateDB, and object-writeback records.

## Mutation model

The adapter first looks up or joins an existing retained request result. Only an
absent identity obtains a raw byte/op permit without holding an inode or database
lock. It then calls `prepare_write` under the existing inode mutation lock(s),
acquired in ascending inode-ID order. `prepare_write` performs the existing canonical
dedup lookup, inode-type and permission checks, checked end-offset calculation,
pending-visible-size calculation, atomic global quota reservation, set-id clearing,
timestamp selection, and construction of the exact post-write attributes. It
rechecks both the retained request index and canonical
deduplication after every wait; a race winner is joined and the unused permit is
released. Atomic publication of every batch member into the overlay and assignment
of one sequence happen under the same mutation locks before the prepared result
becomes replyable. Request-local failures such as `EACCES`, `EINVAL`, `EDQUOT`, or
`ENOSPC` release provisional permits, return before acknowledgement, and do not
poison the service.

Every accepted volatile file-data batch receives a monotonic mutation sequence
within a process incarnation. A batch contains:

- incarnation and sequence;
- a stable request identity and retained result;
- one or more inode identities, not merely paths;
- each member's byte offset, owned immutable payload, and canonical prepared
  attributes;
- one raw reservation and the members' logical quota reservations;
- materialization state and terminal result.

Request identity composes with the existing dedup authority and includes an operation
fingerprint over operation kind, target inode(s), ranges, lengths, payload hash,
effective credentials or opened-capability mode, FUA/stable-how/durability flags,
and every other input that can change validation or reply semantics. 9P carries its
existing operation ID and lifetime; NFS uses a bounded duplicate-request cache keyed
by transport-connection incarnation plus RPC XID and fingerprint; NBD treats
connection incarnation plus request handle as one-shot while the command is in
flight and rejects an active-handle collision unless protocol retry semantics are
separately proven; direct/WebUI callers use their existing operation identity or an
explicitly untagged one-shot identity.

A retry within the identity's valid protocol lifetime attaches to the original
accepted batch and replays the retained result. It does not allocate payload again,
reserve quota twice, choose a new timestamp, or receive a new mutation sequence. A
reused identity with a different fingerprint is never attached to old work. The
duplicate-request cache retains payload ownership while a batch is pending, then
retains only fingerprint and result for a bounded protocol-specific replay window.
Pending entries are never evicted; cache pressure backpressures new tagged requests
instead. NFSv3 has no globally stable client operation ID across a new connection,
so a client-reissued write after reconnect remains a new NFS operation; its
positioned byte write is idempotent, but ZeroFS does not fabricate cross-connection
exactly-once semantics.

Every protocol shares one fair byte/operation ingress budget. The adapter or framing
layer acquires its permit before copying a decoded payload into an owned request. A
retry is ingress-charged while its complete fingerprint is streamed or decoded under
the hard bound, then joins the retained request before a second raw-mutation/cache
charge. A same-identity/different-payload retry remains a fingerprint mismatch; XID
alone never bypasses body validation. NFS hard-mount retransmits, 9P frames, and
WebUI/RPC messages therefore cannot accumulate unbounded copies behind canonical
preparation; cancellation, disconnect, timeout, fingerprint failure, and shutdown each
release the single permit exactly once. If a protocol library decodes before its
application callback, its connection accepts a precharged maximum frame/session permit
and hard byte/concurrency limits before that library allocation.

All canonical writes are write-no-allocate for the clean decoded read cache.
Pending reads remain coherent through the mutation overlay and canonical store, but a
chunked NFS rsync cannot fill the clean read cache. Subsequent reads populate the
cache normally. Segment GC and compaction group adjacent source ranges into bounded
sequential scans and use explicit no-admit/no-fill reads. Any later write-admission or maintenance-cache
exception requires its own bounded policy and measured RED/GREEN proof.

The coordinator maintains an interval overlay per inode. Reads merge the newest
pending intervals over the canonical materialized file. All members of a batch
become visible under one sequence or none do. Growth is immediately visible through
stat and all hardlink aliases. Overlapping writes follow acceptance order; later
bytes win. Canonical publication is FIFO per inode; different inodes,
including members of one striped NBD batch, may materialize concurrently. Encoding
may be pipelined, but cumulative inode attributes and quota publication never complete
out of acceptance order for one inode. A batch completes only after every member
does, and `materialized_through` advances only across the greatest gap-free global
batch-sequence prefix. A materializer may merge adjacent intervals internally, but
it must not reorder observable writes or release raw-memory or logical-quota
reservations before the canonical path owns the corresponding state. Quota ownership
transfers atomically from each prepared member to canonical global statistics; it is
never absent or double counted.

The initial implementation supports volatile ordinary file-data writes, including
file growth. Namespace-changing operations use scoped materialization fences with
this lock hierarchy:

1. close a coordinator-level admission gate for the affected inode(s) and directory
   relationship(s); writers that entered preparation before closure must either
   publish or abort, and only then may the fence capture the conflicting accepted
   sequence;
2. without holding canonical inode or directory locks, drain earlier conflicting
   data batches;
3. acquire canonical locks in their existing global order, revalidate namespace,
   permissions, and deduplication, and execute the canonical operation;
4. release canonical locks and reopen conflicting admission.

No path may wait for materialization while holding a canonical lock the materializer
can need.

A `MaterializationFence` establishes visibility and ordering only. It does not imply
SSD durability. Only an explicit `LocalDurabilityBarrier` used by fsync, COMMIT,
FLUSH/FUA, a sync workflow, or configured shutdown waits for the SSD journal.

This keeps directory operations atomic and preserves existing permission, link,
rename, unlink, quota, and recovery behavior. A CLI upload therefore still creates
a private temporary file canonically, writes its content through the shared volatile
path, drains before publication, then performs the existing atomic rename and sync.

Striped NBD batch atomicity is a live-process visibility and completion guarantee,
not a new crash-atomic multi-inode transaction. Member inodes may become canonical
at different times. Until every member completes, the overlay keeps the logical batch
fully visible and FUA/FLUSH does not return. A crash after an ordinary RAM-only NBD
acknowledgement may preserve only the canonical member prefix, which is permitted by
the explicitly volatile NBD contract; a completed local durability barrier covers
every member.

## Read coherence

All filesystem reads and attributes must consult the shared coordinator:

- a range read overlays pending bytes over canonical bytes;
- a hole introduced by pending growth reads as zero until written;
- stat reports the pending visible size and coherent timestamps;
- inode aliases observe identical pending data;
- every adapter that addresses the same ZeroFS inode observes the same overlay;
- completion removes an interval only after the canonical filesystem state is
  visible to all readers.

After a post-ack terminal failure, the process retains one deterministic frozen
accepted view: canonical bytes plus every still-owned pending interval and its
prepared attributes. Reads continue to serve that coherent view while the process
is alive. New writes, metadata mutations touching poisoned pending state, and every
durability claim fail with the stored terminal cause. This is not presented as
durable data; an unclean restart may discard its RAM-only suffix.

## Durability cutoffs

Durability is expressed as an incarnation-bound typed cutoff, not a boolean flush:

```text
MutationCutoff { mutation_incarnation: MutationIncarnation, sequence }
ObjectCoverage { journal_incarnation: JournalIncarnation, sequence }
DurabilityTarget = LocalSsd | RemoteBackend
```

The ordered barrier algorithm is:

1. capture the newest accepted mutation cutoff;
2. materialize every mutation through that cutoff via the canonical filesystem;
3. seal affected open segments;
4. flush filesystem metadata through the existing `FlushCoordinator`;
5. while the filesystem flush barrier is still held, capture the object-writeback
   accepted sequence as a conservative coverage cutoff;
6. wait for that cutoff's local or remote durability target;
7. return only if each cutoff belongs to its owning current incarnation and no
   terminal failure occurred. Mutation and journal incarnations are distinct types
   and are never compared to each other.

The coordinator must not hold the database flush barrier while waiting for mutation
materialization, because the materializer itself may need that database path.

`FlushCoordinator` is refactored to return a typed receipt containing this object
coverage cutoff. It may conservatively include unrelated earlier object mutations,
but it must never miss an object created by the covered filesystem flush. Later
filesystem mutations cannot cross the held flush barrier before capture. Receipt
capture ends and releases the database flush barrier; only then does the caller wait
for SSD or remote progress, so the barrier never encloses backend drainage.

Protocol contracts are:

- ordinary volatile write: RAM ownership and mutation sequence only;
- NBD FUA: materialize and reach local SSD durability for the write's cutoff before
  replying;
- NBD FLUSH: capture all prior accepted mutations and reach local SSD durability;
- NFS `UNSTABLE` WRITE: may return after RAM ownership and reports `UNSTABLE`;
- NFS `DATA_SYNC` or `FILE_SYNC`: reaches at least the requested local durability
  boundary before reporting its actual committed level;
- NFS COMMIT: capture prior accepted mutations and reach local SSD durability;
- 9P fsync/fsyncdur: capture the relevant obligations and reach local SSD durability;
- filesystem sync: reach local SSD durability for all captured mutations;
- administrative remote flush: reach remote backend durability for the exact
  mutation cutoff and conservative object coverage cutoff.

The current `zerofs_nfsserve` adapter hides RPC transaction identity and stable-how
and always reports `FILE_SYNC`. This implementation includes a pinned, tested
dependency/API change that passes `{xid, stable_how}` into the filesystem write and
returns `{attrs, committed, verifier}` to the encoder. The write verifier changes
across a server restart. The server never reports `FILE_SYNC` for a RAM-only or
merely buffered write.

The current 9P operation-ID, durability-lineage token, reconnect restoration, and
`ESTALE` fail-closed behavior remain authoritative. A local barrier advances the
lineage only after both mutation and object coverage complete; restart never treats
a lost RAM-only mutation as preserved lineage.

## Tiered admission and smooth backpressure

The `MutationCoordinator` owns only raw pre-codec mutation RAM and operation
reservations. Existing segment and object-writeback components retain ownership of
their own representations. Materialization temporarily charges both raw and encoded
representations because both really coexist; raw ownership is released only after
canonical apply. Compression means these reservations are intentionally not a
byte-for-byte transfer.

The downstream layers expose typed capacity/backpressure snapshots to orchestration
and metrics rather than ceding their accounting to a cross-tier god object.
Multipart staging, segment buffers, object-writeback RAM, SSD journal/staging, and
physical reserve accounting are hardened in the separate pacing phase below.

The intended throughput staircase is:

1. while raw mutation RAM has capacity, volatile writes acknowledge at memory speed;
2. materialization begins immediately rather than waiting for RAM to fill;
3. when RAM is saturated, writers wait for materialization to transfer ownership to
   downstream materialization/writeback capacity and proceed at that local rate;
4. when SSD occupancy reaches its pacing threshold, remote watermark advancement
   plus durable local cleanup releases the exact journal reservation tokens that
   were charged for those records; their exact bytes and ops return admission credit,
   so foreground writes continue at approximately remote drain speed without a
   multi-gigabyte stop/resume cycle;
5. the physical free-space reserve and absolute byte/op limits remain hard gates.

The previous high/resume watermarks become observability and emergency recovery
thresholds, not the normal throughput controller. This admission redesign lands as
a separate phase after shared mutation correctness is proven. If accounting or
free-space safety cannot be proved, admission stops and exposes the reason.

Raw mutation reservations must be cancellation-safe and atomically include concurrent
operations from every adapter. Protocol framing retains its existing bounded maximum;
the overlay permit is acquired before copying that frame into an owned mutation.
Phase 3 extends the same accounting proof to every downstream stage so no stage
temporarily owns unaccounted payload bytes. Waiters wake on released capacity,
external free-space recovery, terminal failure, and shutdown.

## Failure and shutdown behavior

The first post-ack materialization failure, SSD-journal failure, corruption,
accounting invariant violation, or remote divergence poisons the coordinator.
Ordinary pre-ack permission, quota, capacity, and argument failures remain scoped to
their request. Poisoning:

- stops new acknowledgements;
- wakes admission, materialization, barrier, and shutdown waiters;
- retains the primary cause and relevant mutation identity;
- never turns a failed write into successful flush or completion;
- preserves recoverable SSD state for the next startup.

Clean shutdown is bounded and ordered:

1. stop protocol listeners from accepting new sessions and make adapters reject new
   mutation admission;
2. drain already-dispatched protocol calls to a bounded quiescent point;
3. close mutation admission and capture the final mutation cutoff;
4. materialize through it;
5. establish the configured local or remote shutdown target;
6. stop mutation workers and close the canonical database;
7. stop object-writeback workers and then the SFTP pool.

Timeout returns an explicit incomplete-shutdown error and keeps lifecycle ownership
alive; cancelling the close future cannot silently drop accepted work. A service
manager may ultimately kill the process, but that is an explicitly unsafe operational
action and must never be logged as successful shutdown. Deployment stop timeouts must
be longer than the configured local/remote drain policy.

## Recovery and crash guarantees

RAM-only acknowledgements may disappear after an unclean process, container, host,
or power failure. That is the explicit volatile contract.

Anything covered by a completed local durability barrier must be represented in the
existing durable object-writeback journal and recover after restart. Startup recovers
the SSD journal before serving read-write traffic, reconstructs its overlay, resumes
remote publication, and rejects identity mismatch or corruption.

Crash points to prove include:

- before volatile acknowledgement;
- after RAM acknowledgement but before materialization;
- during overlapping materialization;
- after filesystem materialization but before metadata flush;
- after local SSD durability but before remote publication;
- after remote publication but before watermark or cleanup;
- during metadata-fenced truncate, rename, unlink, and hardlink interactions;
- during shutdown at every stage.

## Observability

Metrics and status expose at least:

- volatile accepted/materialized sequence and lag;
- raw overlay bytes/ops and oldest age;
- materializer active count and throughput;
- object accepted/local/remote sequence and lag;
- dirty RAM/SSD bytes and operations;
- paced, hard-cap, and physical-reserve backpressure time and waiter count;
- NFS stability class counts;
- terminal failure state and cause class;
- shutdown phase and incomplete target.
- aggregate resident limit/reserve/current/peak and Linux cgroup current/max/events;
- charged payload and estimated overhead/replacement bytes for each cache owner;
- protocol in-flight bytes/ops/waiters by bounded protocol class;
- GC/compaction working bytes and no-admit read counts;
- allocator allocated/resident/retained bytes;
- selected SFTP executable identity, per-direction physical sessions, request depth,
  session waits, and per-session/aggregate bytes;
- logical read bytes, resolved extent/run fanout, active backend read lanes,
  requested-versus-fetched bytes, and backend wait/latency. Isolated benchmark
  receipts additionally prove cache source from process/cache roots plus local-device
  and network counters rather than inventing an unavailable cache-tier label.

Capacity reporting distinguishes logical namespace size, physically allocated local
bytes, object-store live bytes, and sparse virtual-device geometry. Sparse NBD file
length must not be presented as remote physical consumption.

## Test and proof plan

Implementation follows strict RED/GREEN slices. The required proof matrix includes:

1. interval-overlay unit tests for disjoint, overlapping, growth, holes, aliases,
   cancellation, and reservation release;
2. simultaneous NBD/NFS/9P admission proving one global byte/op limit;
3. cross-adapter immediate reads of a stalled volatile write to the same ZeroFS
   backing inode; this does not claim NFS can see files inside a guest XFS image;
4. cross-adapter barriers: NFS COMMIT waits prior NBD writes to a shared backing
   inode, 9P fsync waits prior NFS writes, and NBD FLUSH waits prior 9P writes;
5. NFS stable-how and restart-verifier tests;
6. metadata-fence permutations for write/truncate/rename/unlink/link;
7. striped-batch all-or-none admission and reply, member completion, FUA coverage,
   and permitted RAM-only crash-prefix behavior;
8. same-inode FIFO metadata publication with concurrent encoding and cross-inode
   materialization;
9. request-cache fingerprint mismatch across credentials and stability flags,
   bounded expiry/pressure, pending non-eviction, and identity reuse;
10. conflict-gate quiescence and lock-order tests that would deadlock if a fence held
    a canonical inode/directory lock while draining;
11. failpoint and subprocess crash tests at every durability transition;
12. terminal-failure fanout and waiter wakeup tests;
13. full workspace tests, strict Clippy, formatting, docs, WASM checks, and failover
   suites applicable to touched crates;
14. Linux end-to-end tests with NFS, 9P, and NBD clients, plus xfstests/pjdfstest and
    the existing ZFS-over-NBD matrix where applicable;
15. controlled performance comparisons separating foreground RAM acknowledgement,
    local SSD barrier, and remote backend flush.
16. a real Ubuntu read matrix over raw SFTP, kernel NFSv3, native 9P, and
    NBD/XFS, separating remote-cold, clean-SSD, and clean-RAM state and sweeping
    concurrency through the configured read-session ceiling. The matrix records
    negotiated request sizes, outstanding requests, extent-run fanout, active
    backend lanes, cache/network/disk evidence, exact bytes, and SHA-256 readback.
17. two cgroup-constrained Ubuntu resident-memory gates combining NFS retransmits,
    concurrent 9P/WebUI requests, clean-cache churn, segment sealing, and GC: the exact
    96 GiB/no-swap incident envelope must reject the incompatible 64+16 GiB profile
    before serving, and a full-scale 128 GiB/no-swap soak must fill the 64 GiB clean
    cache, exercise the 16 GiB volatile tier, then sustain replacement/GC overlap. It
    stays below the effective limit by the configured reserve, records at setup the
    immutable limits `high delta <= 8`, reconciliation error `<= 256 MiB`, and unowned
    residual `<= 2 GiB`, reconciles owned, baseline, and residual residency within
    those limits, and records zero cgroup `oom`/`oom_kill` deltas.
18. stock OpenSSH versus pinned HPN versus ZeroFS SFTP A/Bs for upload and download at
    one and configured-many sessions. Each cell records executable identity, RTT,
    TCP window/retransmits, SFTP depth, lane utilization, exact bytes, SHA-256, and
    durability; all temporary processes and remote prefixes are ledger-cleaned.
19. a harness-supervision matrix proving setup, every composite primary substep,
    timeout, double cleanup, and `assert-clean` preserve their independent statuses;
    a later success never masks the first primary failure. Ledger scalar/path access
    is allowlisted and traversal-safe, and NBD receipts store immutable machine
    identity separately from the controller-reachable SSH target validated to it.

Performance acceptance requires integrity and durability checks, not just throughput:
size, checksum, protocol-visible readback, restart behavior, local barrier, remote
barrier, and zero leaked reservations or temporary objects.

Read performance is judged against paired controls, not a historical absolute rate.
Repeated materialized A/A controls establish a median plus MAD noise band. A candidate
may not regress median throughput, p95 latency, requests per logical GiB, or scaling
outside that band. Active read lanes must rise with independent demand until the
configured pool or an evidenced extent-run limit is reached; an unexplained one-lane
result fails. A control whose variance exceeds its acceptance band is inconclusive and
must be rerun, never counted as a pass.

Cache-state names require server-side proof. `fio --invalidate=1` proves only that the
Linux client page cache was invalidated. Remote-cold requires a fresh process and empty
clean-cache roots plus positive remote payload reads; clean-SSD requires zero remote
payload reads and positive local-cache device reads after a restart that clears RAM;
clean-RAM requires client-cache invalidation after server warmup, positive exact
protocol/server logical read bytes for the scored ranges, and zero local-device and
remote payload reads. A zero protocol/server-byte delta is a client-cache hit, not a
clean-RAM result.

## Implementation phases and rollout

### Phase 1: Shared prepared-mutation core

Land `prepare_write`, the bounded raw-data overlay, retained results, materializer,
contiguous progress, scoped metadata fences, typed barriers, terminal state, and NBD
migration behind the disabled-by-default shared mode. Materialized mode must remain
behaviorally unchanged. The old NBD implementation is removed only after parity.

### Phase 2: Protocol composition

Compose 9P and WebUI/direct callers, then the pinned NFS stable-how/XID API change.
Prove same-inode read coherence, cross-adapter ordering, honest NFS stability, 9P
lineage, and NBD FLUSH/FUA. Each protocol slice has its own focused gate.

### Phase 3: Smooth SSD pacing

Separately replace normal 95/85 bulk pauses with remote-completion byte/op credits,
atomically harden physical reserve and multipart accounting, expose typed downstream
capacity snapshots, and prove the old emergency watermarks still fail safe. This
phase is independently benchmarked and can be reverted without removing shared
mutation correctness.

### Phase 3B: Bounded read fanout and matched protocol benchmarks

Plan logically sequential extent runs before fetching and issue independent immutable
segment reads with bounded ordered concurrency. Preserve output order, cache identity,
stale-location re-resolution, nomination/crossing accounting, and exact bytes. Reuse
the maintained VM100 fio/hash/raw-SFTP benchmark primitives in the UUID-ledgered Ubuntu
harness, but replace its NBD-only lifecycle and client-page-cache-only “cold” claim with
real NFS/9P/NBD entry points and proven server cache states. Further NFS framing/copy
work requires a separate measured RED after this shared read fix; it is not assumed in
advance.

VM100 itself remains NFS-only for the shared Mac/Linux namespace. NBD-containing
cross-adapter, filesystem, and read cells execute only on a separately identified,
disposable Linux proof host; an unavailable proof host fails those gates closed.

### Phase 3C: Resident-memory containment and bounded protocol ingress

Charge protocol frames before owned copies, conservatively account cache and allocator
overhead, make streaming writes write-no-allocate, make GC reads no-admit, and enforce
one aggregate process envelope with explicit reserve. Prove retransmit storms and
cache replacement cannot escape it in a real cgroup-constrained Ubuntu process.

### Phase 3D: Measured SSH/SFTP transport selection

Run direction-specific stock/HPN/ZeroFS A/Bs before changing the shipping transport.
If pinned HPN wins a receiving path, allow the explicitly configured absolute binary
and land its immutable packaging/deployment selection without replacing system SSH.
If upload remains below its raw same-session control, land the measured request-depth
or physical-session scheduling correction and rerun the A/B. A benchmark-only binary
or dormant selector is not completion. The receive-win and upload-gap results are
independent typed flags in one validated archived decision receipt, so their combined
outcome executes both corrections and cannot be replaced by a free-form environment
value. Receiver-window evidence alone cannot justify an upload claim.

### Phase 4: Repository and Linux proof

Run the full repository and thermonuclear quality gates, then isolated Linux protocol,
crash, filesystem, and performance matrices. Merge and push `develop`; fast-forward
the clean Ubuntu source checkout.

- Do not deploy or restart CT198 during this feature implementation, proof, merge, or
  source-synchronization plan. The 2026-08-19 automatic post-OOM service restart is an
  incident receipt, not permission for another restart.
- After a separately approved deployment window, drain and verify production,
  deploy one exact build, validate recovery and metrics, and canary volatile mode
  with bounded data before any large transfer.

Every temporary mount, loop/NBD device, process, port, object-store prefix, and test
directory uses a unique recorded identity. Cleanup unmounts clients, disconnects the
exact device, stops and waits for exact processes, proves listeners and mounts are
gone, and removes only the validated temporary resources.

## Rejected alternatives

### Reuse the NBD overlay unchanged

Rejected because it is keyed to block ranges and cannot make pending bytes coherent
with direct NFS/9P inode operations, metadata, aliases, or durability obligations.

### Treat the object-store writeback overlay as the volatile filesystem tier

Rejected because data has already paid compression, encryption, extent, segment, and
metadata costs before reaching it. It cannot reproduce pre-codec RAM acknowledgement.

### Add separate NFS and 9P volatile queues

Rejected because independent queues recreate the coherence and ordering bugs and
multiply memory accounting, failure, and shutdown logic.

### Speculate every metadata operation in RAM immediately

Rejected for the first implementation because a full directory/inode transaction
overlay substantially expands crash and alias semantics. Metadata fences preserve
correctness while accelerating the dominant media-file data path.

### Switch acknowledgement targets automatically as tiers fill

Rejected because durability promises cannot change implicitly. Tiers apply
backpressure; they do not redefine successful completion.
