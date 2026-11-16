# Consistent metadata recovery after one device is lost

This is the first design stage of the metadata survival implementation. It is
verified against ZeroFS commit `9b67f82eddedb30d268ac31fccaa0e3d49d9f2c3`.
The survival gate still fails. This change adds no replication, recovery command,
survival acknowledgement or placement capability. The proposed mechanism is
promising on source inspection only. Fourteen ignored acceptance contracts fail
at missing production prerequisites. Two ordinary journal-loss controls exercise
today's backend durability. No production recovery or interrupted-commit coverage
is claimed. The coordinator accepted the first mechanism
and its operating limits on November 16, 2026. Those decisions appear below as
requirements. They do not establish that the mechanism works.

The chosen direction in [the comparison](metadata-survival-m0.md) is one live
namespace and catalog with redundant recovery generations. Payload remains
single-copy. Segment directories cannot reconstruct names, tombstones or current
extent ownership. `zerofs/src/segment.rs:105` stores frame offsets, lengths,
inodes and extents. `zerofs/src/fs/store/directory.rs:100` instead resolves names
through the database.

## What the proposed mechanism needs to change

The coordinator's proposal needs four corrections before implementation.

1. Replicating `put_opts` alone misses streamed SST writes and other mutation
   methods. The wrapper must cover multipart completion, copy, rename, deletes,
   conditional updates, pool identity and bucket identity. The production server
   disables the SlateDB WAL. The first implementation must reject a separate WAL
   configuration rather than assume that the authority wrapper covers it.
2. A flush and an independently exported catalog do not identify the same prefix.
   The existing database barrier excludes database writes, but not catalog
   mutations, migration, health changes or cleanup. The accepted first mechanism
   is a quiescent survival commit under one new serialization owner.
   It pauses mutation admission and maintenance, drains admitted work, seals and
   flushes, pins an exact SlateDB checkpoint, then exports one redb read
   transaction while the state stays fixed. Survival is a distinct, explicitly
   requested receipt. Quiescence adds latency and pauses admission for that
   receipt. Ordinary buffered and local SSD acknowledgements retain their current
   meaning. Admission pauses and replicated allocation intents may increase their
   cost and latency while survival is configured.
3. Deleting the primary object before its replicas does not protect the last
   recovery generation. Every dependency and physical payload generation needed
   by a selectable commit needs retention before any deletion. Deferred cleanup
   must keep durable debts and capacity charges. Merely mirroring today's
   immediate cleanup reproduces a dangling recovery reference on every device.
4. Read-only recovery cannot call today's pool open path or open a fresh SlateDB
   writer. Pool opening writes catalog records and recovers pending deletions.
   Recovery needs a separate catalog reader and a pinned SlateDB checkpoint
   reader. It may materialize verified metadata into a new scratch directory,
   but it must never modify the surviving evidence or initialize a key.

With those changes, a single consistent recovery generation is possible in
principle. The current APIs do not provide that cross-store transaction. If the
new owner cannot exclude every mutator without deadlock, stop implementation.
The next alternative is a unified replicated metadata transaction log that owns
both namespace publication and catalog changes. Independent database mirrors
cannot replace that owner.

## Source integration points

Paths and line numbers in this section refer to the pinned ZeroFS commit.
Dependency paths below refer to SlateDB revision
`230ade681f1fdf0f85fc1c2cb6c118575a83ceb1`, pinned at
`zerofs/Cargo.toml:76` and `zerofs/Cargo.lock:5532`. Its repository is
`https://github.com/Barre/slatedb`. The dependency sources were inspected in the
local Cargo checkout, not inferred from upstream documentation.

| Integration point | Verified behavior and required change |
| --- | --- |
| Pool startup | `zerofs/src/storage_backend.rs:111` validates paths and refuses a missing catalog at line 122. It constructs one local authority at line 127 and the pool at line 180. Construct the replica manager before the authority is passed into the pool. Keep ordinary startup refusal. Add an explicit recovery entry point before this path. |
| Directory and device identity | `zerofs/src/storage_backend.rs:263` rejects overlapping authority, catalog, cache, journal and member roots. `zerofs/src/storage_backend.rs:308` derives member identity from its location. Replicas on data devices need disjoint sibling directories, device bindings and mount validation. Different directory names alone do not prove independent devices. |
| Authority writes | `zerofs/src/pool_object_store/mod.rs:1875` forwards non-segment puts to the authority at line 1884. Multipart operations take a separate route at line 1902. Wrap the underlying authority, including privileged pool-marker creation at line 550, rather than only the public pool put method. |
| Authority deletion and movement | `zerofs/src/pool_object_store/mod.rs:2000` implements deletion streams and forwards authority deletion at line 2016. Non-segment copy delegates at line 2115. Rename delegates at line 2165. All paths must pass through replica retention and ordering. Internal `.zerofs-pool` paths are reserved at line 643, so the replica manager needs its own owned operations. |
| Conditional authority objects | `zerofs/src/key_management.rs:279` creates the key using `PutMode::Create`. `zerofs/src/bucket_identity.rs:69` creates `.zerofs_bucket_id` the same way. `zerofs/src/local_object_store.rs:161` handles create and conditional update under local locks. One logical create decision must govern the whole replica set. Repeating independent creates is insufficient. |
| Local durability | `zerofs/src/local_object_store.rs:58` enables fsync. Directory ancestry sync is at line 105. Standalone deletes sync the parent at line 247. A replica implementation must preserve these properties, including owned workers after caller cancellation. These calls do not constitute hardware power-loss evidence. |
| Namespace flush completion | `zerofs/src/fs/flush_coordinator.rs:119` holds the database write barrier through seal and flush. The typed durability path at line 224 materializes a cutoff, seals and flushes at line 242, releases the barrier, then waits for object coverage at line 258. `zerofs/src/db.rs:947` awaits SlateDB flush. Extend this ordering with a separate survival operation and receipt. Existing backend receipts must retain their current meaning. |
| Database exclusion | `zerofs/src/db.rs:885` admits database batches under the barrier's read side. It does not serialize redb or all maintenance. The new owner must sit outside this barrier and drain operations before taking it. A pool operation must not acquire the new gate recursively while the commit owner waits for its drain. |
| Production WAL and background flush | `zerofs/src/cli/server.rs:1162` disables the WAL and automatic size-driven metadata freezing. The settings at line 1170 preserve barrier-controlled publication. `zerofs/src/cli/init.rs:237` can still construct a separate WAL store. Reject that setting in the initial survival mode and keep the WAL disabled in acceptance fixtures. |
| Catalog commits | `zerofs/src/pool_object_store/catalog.rs:658` reserves in an immediate transaction and commits at line 736. Publication starts at line 767 and commits its mapping and old-generation debt at line 835. Retirement commits at line 856. Upload and garbage debt removal commit at lines 887 and 915. Health, membership and fault transactions also need the new owner, including lines 362, 470, 551 and 578. |
| Catalog contents and identity | `zerofs/src/pool_object_store/catalog.rs:40` defines eight tables. Identity at line 51 includes the pool UUID, authority prefix and canonical catalog directory. Opening checks the directory at line 295 and the marker at line 302. The current snapshot at line 932 exports only mappings and hidden legacy objects. Add a complete logical export and a versioned recovery binding, rather than copying `pool.redb` or calling the partial snapshot a backup. |
| Payload publication and cleanup | `zerofs/src/pool_object_store/mod.rs:856` writes a fresh physical generation, publishes its catalog entry at line 943 and immediately cleans the previous generation at line 946. `delete_owned` at line 1803 retires and cleans. `cleanup_garbage` at line 1327 deletes bytes before forgetting debt. All three need recovery-reference retention and replicated cleanup intents. |
| Startup side effects | `zerofs/src/pool_object_store/mod.rs:534` configures members, line 584 finishes initialization and line 588 invokes pending recovery. `zerofs/src/pool_object_store/catalog.rs:271` opens a write transaction even for ordinary reopen. A read-only recovery path must bypass these operations. |
| Key loading and rotation | `zerofs/src/cli/init.rs:225` normally uses load-or-initialize. `zerofs/src/key_management.rs:210` instead refuses a missing existing envelope. Recovery must use the latter behavior. The envelope at line 25 has salt, nonce, wrapped DEK and version, but no pool binding, epoch or encoded KDF parameters. Password change at line 302 overwrites one envelope at line 323. The first survival version must refuse password changes whenever metadata replicas are configured. |
| Writeback placement | `zerofs/src/cli/init.rs:267` attaches writeback above the backend after loading the key. Hooks at lines 1167 through 1207 capture and wait for object coverage. `zerofs/src/writeback/bootstrap.rs:27` recovers a paused overlay. The authority replica wrapper belongs below writeback, so a local journal acknowledgement does not imply replicated durability. Survival records bypass the evictable cache and journal and reach every metadata device directly. |
| Writeback retirement | `zerofs/src/writeback/journal.rs:1371` advances remote completion. `zerofs/src/writeback/journal.rs:1720` removes remote prefixes. A survival commit must stop relying on a journal prefix before it retires it. Losing the journal may still lose a documented local-only suffix. |
| Writer locks | `zerofs/src/pool_object_store/catalog.rs:250` holds an exclusive catalog lock. `zerofs/src/local_object_store.rs:95` orders local locks and keeps their files stable. Neither locks every metadata replica. Add a persistent lock file on each replica and hold every exclusive lock for the writer's lifetime, together with the existing SlateDB writer epoch. |
| Config parsing | `zerofs/src/config.rs:669` denies unknown pool fields. `Settings::from_file` at line 1958 parses and validates. The proposed `metadata_survival` table is unsupported. Configuration acceptance alone provides no replication or recovery evidence. |

The pinned SlateDB implementation supplies several useful pieces, with limits.

| Dependency source | Verified behavior |
| --- | --- |
| `slatedb/src/db.rs:1762` | `flush` waits for memtable flush when the WAL is disabled. It returns no manifest identity. |
| `slatedb/src/checkpoint.rs:30` | `create_checkpoint` returns a checkpoint UUID and manifest ID. `CheckpointScope::All` flushes first. `Durable` pins the already durable state. Use `Durable` after ZeroFS seals and flushes under exclusion. |
| `slatedb/src/manifest/store.rs:130` | Writing a checkpoint updates the manifest. Checkpoint creation itself therefore produces authority writes that must drain before the survival record. |
| `slatedb-txn-obj/src/object_store.rs:369` | Manifest versions use sequential object names and conditional create. Line 503 deletes versions. Lines 316 and 325 update or create mutable GC boundaries. Primary filesystem ETags cannot be reused as replica ETags. |
| `slatedb/src/tablestore.rs:355` | Compacted SSTs use streaming publication. WAL SSTs use a conditional single put. Line 535 deletes SSTs. Multipart completion must be replicated before success. |
| `slatedb/src/db_reader.rs:722` | A pinned checkpoint reader does not poll for a newer manifest. Lines 762 through 779 restrict checkpoint cleanup to managed readers. Use `DbReaderMode::Checkpoint`, not a managed reader that writes checkpoint state. |

## Configuration and ownership

The proposed configuration is a new optional pool table. Its absence preserves
the existing contract. Its presence enables strict survival mode. No field here
exists at the pinned head.

```toml
[storage.pool.metadata_survival]
format_version = 1
authority_replica_id = "authority"
authority_device_id = "operator-device-uuid-a"
recovery_mode = "normal"
reserve_free_bytes = 67108864
commit_interval_seconds = 60
replica_timeout_seconds = 30

[[storage.pool.metadata_survival.replicas]]
id = "disk-a-metadata"
device_id = "operator-device-uuid-b"
dir = "/mnt/disk-a/metadata-replica"
```

The authority counts as one replica. The configured additional directories bring
the total to at least two distinct device bindings. Replicas may share a device
with payload members, but their directories must be siblings of member roots.
Aliases and duplicate physical devices must be rejected. The test fixture uses
explicit logical device IDs on one filesystem. It cannot validate that hardware
check. Metadata space reservations must share the physical device's capacity
accounting with payload reservations.

`recovery_mode = "read_only"` requests a separate recovery path. It accepts an
absent authority, catalog, journal or payload member root without recreating it.
It reads surviving configured replicas and materializes verified state only into
an owned temporary directory. `recovery_mode = "normal"` never elects a surviving
replica automatically. It refuses survival commits after any configured replica
is unavailable, corrupt, full, stale or bound to another pool. The writer stops
survival acknowledgement and reports the failed replica. It never drops a
replica from the acknowledgement set on its own. Memory and SSD acknowledgement
modes do not become survival receipts. Admission must remain bounded while
survival cannot progress, and retained dependencies must remain protected.

### Behavior of the live share after a failure

While survival is configured, a failed replica health check, I/O error, ENOSPC,
identity mismatch, stale replica or commit deadline closes **all mutation
admission**, including memory and local-SSD acknowledgement modes. New writes,
metadata changes and destructive maintenance fail with an explicit storage
error. Already admitted operations retain their ownership until they settle;
they may have their ordinary durability outcome but cannot receive a survival
receipt for an incompletely replicated generation. A deadline stops waiting by
the caller; it must not release the owner's locks or cancel an in-progress
fsync. Queued, unadmitted requests are rejected rather than held indefinitely.

Reads may continue only while their namespace and catalog view remains valid.
The loss of the authority or journal must stop the normal export if that view
cannot be guaranteed; hot cached metadata is not evidence that a cold read can
succeed. A lost payload device returns `EIO` for current ranges on that device,
never zeros or obsolete data, while verified unaffected ranges remain readable.
There is no automatic writable failover. The operator stops the writer and
opens explicit read-only recovery at a validated generation. Normal writable
service resumes only after offline catch-up restores every configured replica
and a fresh startup validates the fixed membership and its exclusive locks.

### Latency, space and write budgets

The first version permits one owned survival commit at a time. Its request
waits for drain, backend flush, checkpoint export, and durable dependency and
commit-record publication on **every** replica. With the sequential publication
order below, the fsync costs add across replicas; an HDD replica participates
in this foreground latency even when the authority is NVMe. Ordinary writes
outside a requested commit do not gain a survival guarantee. They can still
wait behind the commit's closed admission gate.

Deployment must supply a finite commit deadline, an admission byte/request
budget and a generation-retention byte budget covering metadata and pinned
payload. Activation must refuse absent or zero budgets. These are requirements for the implementation, not configuration
fields supported today. Exhausting any budget closes mutation admission and
reports which budget failed. A timeout never authorizes dropping a replica,
forgetting an acknowledged generation or silently switching acknowledgement
semantics. No latency SLO or endurance estimate is established by this PR.

Capacity accounting includes the live metadata, the union of dependencies for
selectable generations, one candidate, durable intents, scratch/export
headroom and the configured free reserve, on each capacity domain. Immutable
dependency bytes may be shared by digest; a shared byte stays charged until
its final retention reference is gone. For `R` replicas and `D` newly copied
metadata bytes, copying alone writes at least `R * D` bytes before catalog,
record, filesystem and sync overhead. The implementation must measure per-replica
bytes written, fsync latency, commit elapsed time, queued admission and retained
bytes. Enabling it on HDD replicas requires measured peak metadata churn and
space/write budgets, not the assumption that metadata is always small.

The first implementation is single-host only. Configuring metadata replicas
together with HA replication is refused. Before opening a writer, acquire an
exclusive lock on every configured replica in stable replica-ID order. Hold
all locks for the writer's lifetime, together with the existing database writer
epoch. Failure to acquire any lock refuses the writable open and releases the
locks already acquired. Lock files remain stable and must never be unlinked
while an owner may hold them. This is a local OS locking contract, not a
multi-host election protocol.

No external freshness service is required. Freshness relies on acknowledgement
on every configured replica and mandatory offline verified catch-up before a
replica that missed any commit may rejoin. Under these rules every surviving
configured replica contains every acknowledged commit. Recovery selects the
highest valid chained commit among the surviving replicas and refuses when they
disagree on pool identity or membership epoch. Recovery cannot rule out a
replica that was swapped for an older copy of itself. Authentication and a local
exclusive lock cannot detect that stale self-copy without independent history.

Membership changes while serving are refused. Replacement remains an offline,
explicit epoch transition with verified catch-up before the next writable open.
Password changes are refused whenever metadata replicas are configured until
the credential matrix passes. The name mode is carried inside the `zerofs.key`
envelope under the coordinator's PR 78 contract. Replicate that envelope byte
for byte. There is no separate name-mode object or dependency.

## Operating contract awaiting operator confirmation

These are coordinator decisions that the operator has not yet confirmed. The
first version must expose these limits before an operator enables replicas.
The metadata replica set must not depend on a single capacity HDD alone.

Ordinary write acknowledgements and fsync acknowledgements do not require a
survival receipt. They can wait for admission during a survival commit, and
replicated allocation intents can increase their cost. They keep today's
configured meaning, with RAM acceptance,
local NVMe journal durability and remote drain to the assigned HDD as separate
cutoffs. A backend receipt does not become a survival receipt.

Status must report the last successful survival sequence, its completion time,
the current lag since that completion, and each failed replica with its error.
Before the first success it reports that no survival recovery point exists.
A failed or stuck attempt does not advance the sequence or completion time.
Lag keeps growing while attempts stall. The configured interval schedules
attempts and must never appear as the achieved recovery point.

The owner runs survival commits on a configurable interval and on explicit
request. The proposed initial interval is 60 seconds. After the authority device
is lost, the recovery point is the last survival commit. Writes acknowledged
after that commit can be lost with the authority device even if their payload
already reached an HDD. An HDD can still hold those bytes while the names and
extent mappings needed to find them are gone. This is the main cost of the
quiescent design. The interval, pause duration and write cost must be measured
before choosing a deployment value. An interval does not promise a maximum
recovery age when commits cannot finish.

New mutations wait while a survival commit holds admission closed. The owner
first drains admitted work, seals and flushes, waits for backend coverage, pins
the checkpoint and exports the catalog. With replicas on HDDs it then waits for
each metadata dependency's file fsync and parent-directory fsync, the catalog
export and dependency list fsyncs, and the commit record fsyncs on every replica.
It verifies the stored bytes before returning the named receipt. Admission
stays closed through that all-replica record publication and durable release
of the live export pin. A slow replica can
therefore pause new writes for the duration of its timeout.

The publication count is variable and must appear in commit diagnostics. With
pre-created, durably synced directories, each newly published file needs one
file fsync and one parent-directory fsync. Let D be the number of new immutable
metadata dependency copies, L the number of live authority file publications
from seal, flush, checkpoint creation and export-pin release, and K be 1 when the versioned key record is
new and 0 when it is already durable. The catalog export, dependency list and
commit record add three files. The generation therefore waits for
`2 * (L + D + K + 3)` fsyncs on each replica. A commit that writes a new key record
has `2 * L + 2 * D + 8` such fsyncs. Reused, verified immutable objects contribute
zero new publication fsyncs. Let A count additional directory fsyncs, including
new ancestry and the native conditional store's repeated syncs of existing
parent ancestry. The total before retirement is `2 * (L + D + K + 3) + A` per
replica. Directory depth therefore affects the count too. Retirement adds
two fsyncs for its batched retirement record on each replica and one parent
sync for each subsequent delete. The ordinary export-pin release is already
counted in L, even when no older generation retires. Allocation and deletion intents have their own file and parent syncs;
the diagnostic must report those separately rather than hide them in D.
HDD fsync latency and the resulting admission pause have not been measured.

The live share has these first-version failure rules:

| Event | Required behavior |
| --- | --- |
| The authority device or a metadata replica device is lost. | Stop mutation admission and leave the share read-only. An operator must finish offline recovery or verified replica replacement before another writable open. No automatic election or degraded survival acknowledgement is allowed. |
| A data member without a metadata replica is lost. | Leave the share read-only too. Return I/O errors only for current file ranges whose sole payload copy was on that member. Preserve metadata, empty files, holes and readable ranges on other members. |
| The NVMe journal is lost. | Stop the service. Offline recovery reports the local-only suffix as lost and selects a receipt-covered generation. A drained suffix survives only if a survival commit covered it. |
| A replica is slow. | Delay the survival commit while it is pending. After the configured 30-second per-replica deadline, report that replica failed, stop survival acknowledgements and leave the share read-only. The timeout covers publication, sync and verification. |
| A replica is full, stale or returning I/O errors. | Stop survival acknowledgements, report the failed replica and leave the share read-only. Preserve retained generations and bounded queued work. |
| Another writer attempts startup. | Refuse it through the persistent lock on every replica. The first writer keeps its locks until shutdown. |
| An operator replaces a replica. | Refuse online catch-up. With every writer stopped, copy and verify the complete retained generations, identities, membership transition, dependencies, debts and key records. Permit a writable open only after verified offline catch-up. |

A journal-loss report names the last survival commit and warns that the later
local-only suffix was lost. Its exact upper sequence or acknowledged-write count
can be unknown because that evidence was on the lost journal. Recovery must not
invent an exact count. The test captures the pre-loss journal boundary only to
prove that its paused workload really created a nonempty local-only suffix.

Replica space per generation, write bytes and write operations per commit on
each replica device, and HDD fsync latency are unknown budgets to measure. The
reserve is an admission check, not a capacity estimate. No benchmark or hardware
claim follows from these local directory tests.

## Proposed on-disk formats

Every replica, including the authority, owns `.zerofs-survival/v1/`. Object paths
inside records are normalized relative paths. Readers reject absolute paths,
parent components, duplicates, unknown required fields and oversized records.
No record stores a raw DEK or password.

| File | Proposed content |
| --- | --- |
| `writer.lock` | A persistent lock file opened and exclusively locked on each configured replica before writable startup. The writer holds every lock until shutdown. Recovery locks existing surviving files without creating or modifying source files. The lock file is not commit content and carries no freshness claim. |
| `identity.json` | The format version, pool UUID, replica ID, device binding, fixed membership digest, membership epoch and original authority and catalog bindings. It is immutable within an epoch and authenticated after key bootstrap. |
| `keys/<epoch>.json` | A versioned recovery record containing the exact `zerofs.key` envelope bytes encoded as hex, their SHA-256, the original envelope version, its required KDF algorithm and parameters, the DEK identity digest, pool UUID and envelope epoch. The name mode remains inside those original envelope bytes. The authenticated commit binds this record and the exact envelope digest to the pool and epoch. Recovery copies the original envelope bytes without rewrapping the DEK or inventing another name-mode field. |
| `catalog/<generation>.json` | A complete logical export of all eight catalog tables under one redb read transaction. Sorted table names and sorted byte keys carry their exact byte values, plus original identity, generation, schema versions and allocation counters. Decode and validate known values before import. Byte preservation avoids silently dropping future health or debt fields. Unknown schema versions refuse recovery. |
| `dependencies/<generation>.json` | The checkpoint UUID, exact manifest ID and path, all transitive manifest and SST dependencies with lengths and SHA-256, bucket identity, the exact `zerofs.key` envelope bytes including name mode, and the catalog export digest. It also names every live physical segment generation and its owner, length and checksum from that catalog. Missing payload is distinguishable from missing metadata. |
| `objects/<sha256>` | Immutable byte copies of metadata dependencies. Mutable logical objects and future overwrites cannot destroy a retained commit's bytes. A dependency map reconstructs the selected logical names in scratch. Live authority names still use the existing SlateDB layout. |
| `intents/<epoch>/<sequence>.json` | Authenticated, chained safety records for new physical allocations, remaps and cleanup attempts. An allocation intent with its exact physical key reaches every replica before bytes can be written. A deletion intent names the retired generation and the commit that permits deletion. Completion may only reduce debt in a later committed catalog export. |
| `retirements/<epoch>/<sequence>.json` | An authenticated retirement batch naming old commit digests, their checkpoint UUIDs, the two retained successor digests and the membership epoch. Every replica must durably record the batch before any retired generation reference is decremented or dependency is deleted. Export pins release separately after durable protection transfers to the ledger. |
| `commits/<epoch>/<sequence>.json` | A canonical authenticated commit with pool UUID, membership digest and epoch, writer epoch, sequence, predecessor digest, mutation incarnation and cutoff, object journal incarnation and cutoff or direct-backend coverage, SlateDB checkpoint and manifest identity, dependency-list digest, catalog generation and digest, envelope epoch and digest. The envelope digest covers the name mode too. |

Use a specified canonical UTF-8 JSON encoding with lexicographically sorted
object keys, decimal integers, no duplicate keys, no floats and no insignificant
whitespace. Encode bytes and digests as lowercase hex. Compute SHA-256 and a
domain-separated HMAC-SHA-256 over the canonical bytes excluding the MAC field.
Derive the MAC key from the recovered DEK using HKDF with the pool UUID and the
literal `zerofs-metadata-survival-v1`. Pin this encoding with golden vectors
before writing persistent state. These authenticated recovery records do not
exist at the pinned head. They preserve the original key envelope bytes.

Recovery starts with bounded envelope candidates and the independently supplied
password and expected pool identity. It unwraps a candidate, verifies commit
authentication and then verifies the envelope's exact digest and binding,
including its name mode. It requires no external freshness floor. The
one-device-loss claim assumes the configured surviving device has not also been
rolled back or corrupted. Recovery must explicitly report that it cannot rule
out a replica replaced by an older copy of itself.

## Exact survival commit ordering

The numbers below are named cut points for the acceptance matrix. Cut immediately
before and after each durable file publication and each sync, on each replica.
Atomic rename without a parent sync is not a completed durable publication.

1. The owner verifies that it still holds every configured replica's exclusive
   lock and the existing database writer epoch. It checks pool identity,
   membership and metadata capacity on every configured replica. It closes admission to new
   namespace and pool mutations. It drains already admitted mutations and stops
   maintenance at an explicit idle acknowledgement. It does not hold the
   database barrier while waiting for workers that need that barrier.
2. With admitted work drained, the owner takes the database write barrier. It
   seals the open segment, flushes SlateDB and captures object coverage. It waits
   for the backend cutoff while maintenance remains paused. The owner must not
   hold a pool lock needed by writeback replay. Pool operations carry an existing
   admission token instead of reacquiring the closed admission gate.
3. The owner creates a temporary non-expiring `Durable` SlateDB checkpoint and receives its
   manifest ID. It drains the checkpoint's metadata writes through writeback to
   the replicated backend. It enumerates the checkpoint's full dependency graph
   and hashes the actual bytes. Compaction publication and GC must remain paused
   or be proven harmless under the same retention protocol.
4. The owner exports all catalog tables from one read transaction after replay
   has settled. It validates every namespace segment reference against this
   catalog. The snapshot covers the same fixed namespace prefix. Extra uploads
   and garbage are explicit debts, not live mappings. It includes member health,
   faults, hidden legacy entries, identities and counters.
5. For each replica in configured ID order, the owner stores and syncs immutable
   key envelope with its name mode, catalog export, dependency bytes and
   dependency list.
   It syncs each file and affected parent directory, then verifies lengths and
   hashes from that replica. It installs retention for the candidate and both
   selectable predecessors, when present. No commit record is written until
   every replica has every metadata dependency. Payload remains on its one
   assigned member.
6. For each replica in the same order, the owner conditionally creates and syncs
   the identical authenticated commit record. The record names the previous
   committed chain, never independently selected latest files. A same-name,
   different-digest record terminates the writer. A same-digest retry completes
   the existing operation without inventing another key or catalog identity.
7. Only after every configured replica has the durable record may the owner
   return a survival receipt. It records the exact commit digest with the
   mutation and object cutoffs. A caller cancellation cannot cancel the owned
   commit task or release its exclusion early. Lost replies permit a complete
   unacknowledged generation to recover.
8. The owner transfers dependency protection to the durable survival-retention
   ledger and explicitly deletes its temporary SlateDB checkpoint through
   `Admin::delete_checkpoint`. It drains that manifest update before releasing
   admission. Cleanup can retire dependencies of older commits only when every
   configured replica has a newer durable commit and no reader or retained
   generation references them. It logs each deletion intent
   on every replica first. It deletes the primary object before replica copies
   only within this permission. Physical debt remains charged until a subsequent
   survival commit records completion. A loss mid-cleanup therefore preserves
   conservative, possibly already satisfied debt and never undercounts it.

### Checkpoint and generation retirement

A survival checkpoint is an export pin, not a permanent live SlateDB reader.
The current segment-GC gate in `zerofs/src/fs/gc.rs` pauses deletion and
compaction whenever *any* non-expiring checkpoint exists. Merely retiring the
oldest checkpoint while keeping the newest would therefore still prevent
reclamation forever. The implementation must release every owned export pin
once its complete immutable dependency bundle is protected on every replica.
Read-only recovery opens that verified bundle in scratch and does not depend
on the continued existence of its checkpoint in the live authority.

A generation pins the full recovery state. This includes its metadata SSTs,
manifest state, catalog export, exact key envelope and every physical payload
object its catalog and captured namespace reference. For example, commit M0 can
name segment S0 while a later overwrite or repack publishes S1. Reclaiming S0
before M0 retires would leave valid M0 metadata pointing at missing payload on a
healthy member. The ledger must preserve S0 even when the live namespace has no
remaining reference to it. The replicas still copy metadata only. These payload
pins retain existing single-copy objects; they add no payload redundancy.

Reclaim, repack, tombstone cleanup and relocation must consult this same ledger
before deleting an object or its old physical generation. A live remap to S1
never grants permission to delete S0 while any selectable generation or reader
references S0. Relocation retains its exact source key and generation until those
references retire. All payload deletion paths, including pool garbage cleanup
and orphan sweep, must honor this permission under the serialization owner.

A member is removable only after a survival generation that no longer requires
it is durable on every replica. Every older selectable generation and reader
that still references the member must retire before physical removal. Keeping
the two newest generations can therefore require another successful commit
before removal is safe. A live relocation count of zero alone cannot authorize
member removal. The first version still refuses online membership changes;
offline removal must validate this ledger and its durable membership transition.

Replacing the pin requires an explicit retention ledger consulted by **all**
metadata and payload deletion/remap paths. It records references by generation
and by recovery reader. The ledger keeps the two newest selectable survival
generations. An older generation may be retired only after a newer generation
is durable on every replica and all readers of the older generation close.
Reader pin acquisition and retirement share the owner, so a new reader cannot
race the no-reader check.
Retirement is itself an authenticated, durable record on every replica. Recovery
must reject retired generations, while retaining their small chain records if
the selected generation still needs them for authentication. Only after this
record is durable everywhere may dependency references be decremented and
unreferenced bytes become cleanup debts. A crash during retirement may leak
space temporarily; it must never invalidate a selectable generation.

On an aborted candidate, the owner records whether any commit record was
published. Before record publication, it may release the export pin after
durably recording abandonment and all candidate allocation debts, preserving
the preceding generation's references. After any record publication, the
candidate remains recoverable until publication is completed or an explicit
offline repair resolves it. Failure to retire a pin keeps admission closed;
startup reconciles owned checkpoint IDs from durable intents before resuming.
Never delete arbitrary user checkpoints. Repeated-commit tests must show that
owned pins return to zero and retained space stabilizes after readers and
cleanup settle. If the live GC paths cannot honor this ledger, survival remains
disabled; keeping a permanent checkpoint is not a deployable substitute.

Retirement is restartable. A partial retirement fanout cannot authorize a
reference decrement or delete. The owner completes the same retirement record
on every replica before proceeding. Interrupted deletion keeps its capacity
debt until a later committed catalog records completion. Recovery applies
retirement records when selecting generations and never runs their deletes.

Without readers or an incomplete commit, this policy bounds retained survival
metadata to the union of two generations' dependency bundles, catalog exports,
dependency lists and key records. It bounds survival-only payload retention to
the physical generations referenced by those two catalogs. A physical object
shared by both generations is charged once per capacity domain. Relocation
copies retain separate charges. One candidate can temporarily add a third generation. Readers can
retain older generations until they close, so admission must account for those
references too and stop before consuming the configured reserve. Failed cleanup
and orphan uploads remain separately charged. Historical authenticated chain
and retirement records also consume space. This policy does not bound those
small records or total pool usage. Discarding chain evidence requires a separate
compaction format contract.

The mandatory retention byte budget covers the union of pinned metadata and
payload physical generations on each capacity domain, including retained
relocation sources, the candidate and active readers. It also reserves space
for durable intents and cleanup debts without spending the configured free
reserve. Overwrites can increase these retained bytes while survival commits
stall. Admission must check the added charge before accepting a mutation or
maintenance allocation. At exhaustion it closes mutation admission with an
explicit storage error, rejects unadmitted queued work and reports the budget
and any failed replica. It preserves every selectable generation and reader pin.
It cannot reclaim their bytes, evict the oldest recovery point, drop a replica
or keep acknowledging new writes to make space. Read-only service follows the
failure rules above until an operator resolves the budget or failed replica
and safe publication and retirement can resume. The byte limit needs workload
measurement before deployment; no safe numeric default is established here.

The wrapper must serialize conditional operations as one logical decision under
the fenced owner. It uses the primary's create or compare-and-swap decision and
records the candidate bytes and operation identity. A partial fanout blocks
further publication until a same-operation repair finishes or the writer stops.
An `AlreadyExists` from the primary must not return success for a partial
replication. The initializer may adopt the winner's key only after its bytes
are durable on every replica. Mutable GC boundaries need per-replica versions
or an internal content-generation comparison, since native ETags differ.

This protocol also needs allocation intents before any new payload write between
survival commits. Otherwise loss of the authority after an uncommitted upload
loses its only cleanup debt. Recovery imports the selected catalog exactly and
reports later allocation intents as separate uncommitted orphan debts. It does
not merge them into live mappings or run their deletions in read-only mode.
Destructive remaps and cleanup cannot invalidate the last selectable commit.

## Interrupted states and recovery decisions

Recovery is a single-host operation. Before inspecting source content, it takes
exclusive locks on the existing lock files of every surviving configured
replica. It opens source content read-only, validates identities and
authentication, and groups records by epoch, sequence and digest. It refuses
survivors that disagree on pool identity or membership epoch. It selects the
highest complete valid chained commit among the surviving replicas in that
epoch. A commit may exist on only one surviving replica because
all dependencies reached every replica before record fanout. Recovery may expose
that complete unacknowledged prefix. It never exposes a partial transaction.

| Interrupted state | Recovery action |
| --- | --- |
| Admission or drain stops before checkpoint creation. | Recover the prior commit. Ignore RAM state and the lost local-only suffix. Preserve replicated allocation intents as orphan debts. |
| A segment or checkpoint finishes, but catalog export has not finished. | Recover the prior commit. Retained payload and metadata generations still satisfy it. New objects remain uncommitted and cannot replace its dependencies. |
| Export, dependency copy or dependency sync stops on any replica. | Ignore staging files without a valid commit. Keep prior dependencies and allocation debts. No survival success is legal. |
| The first commit record is durable, but later replicas lack it. | A survivor with the record may recover the candidate after dependency validation. A survivor without it recovers the predecessor. Both results contain at least the last acknowledged prefix. No cleanup of the predecessor was allowed. |
| All records are durable, but the caller has no reply. | Recover that complete generation. The uncertain reply does not justify rollback or key substitution. |
| Cleanup stops before or after a physical deletion. | Open the selected catalog without replaying cleanup. Keep its exact debts and later intents. A later writable repair rechecks generation identity before satisfying a debt. |
| A referenced metadata dependency is missing, corrupt or has the wrong digest. | Refuse that recovery. Do not silently fall back below the latest valid authenticated commit and hide possible loss of an acknowledged prefix. Missing payload on the declared lost data device is reported separately. |
| Two authenticated records conflict at one epoch and sequence, or chains fork. | Refuse recovery. Do not use timestamps, directory order or a two-replica majority. |
| A replica missed a commit or belongs to another pool or membership. | Refuse unsafe participation and withhold survival success. A replica that missed a commit may rejoin only after offline verified catch-up. Recovery refuses survivors with different pool identities or membership epochs. It cannot rule out an older copy substituted for the same replica. |
| A password change is requested. | Refuse it before any envelope write while metadata replicas are configured. Interrupted credential changes remain outside the first version until their matrix passes. |

Materialization reconstructs only the selected manifest and its named dependency
map in a new scratch authority. It imports the catalog export into a new scratch
redb database under a versioned recovery binding. It preserves the original pool
UUID, member tokens, mapping generations, checksums, reservations and debts.
The original canonical paths remain authenticated history. They must not be
silently replaced by scratch paths in the identity comparison. Recovery needs
an explicit validated binding override, because today's marker encodes those
paths. A pinned checkpoint reader then opens the namespace with the recovered
existing key. No mutating pool open, key initialization, writeback replay,
compaction, migration or cleanup runs on surviving sources.

Loss of a data member keeps metadata readable, including every hardlink and
empty file. Recovery joins catalog segment loss with current namespace extent
locations to report exact inode, file names, offsets and lengths. It must not
report an obsolete segment frame as current loss. Holes remain holes. Reads of
lost ranges return a defined I/O error, and unaffected ranges verify their
original hashes. This per-file reporting path does not exist today.

## Implemented evidence and required acceptance matrix

`zerofs/src/metadata_survival_tests.rs` retains three ordinary controls. The
baseline cold-opens before losing authority objects, the catalog, the authority
device or the wrapped key. The two journal controls discard an SSD-durable
suffix before or after backend drain and cold-read the expected prefix. They
prove today's writeback cutoff behavior. Their fixtures have no survival
replicas or receipts.

`zerofs/src/metadata_survival_tests/acceptance.rs` defines one explicit oracle per
row through the shared `oracle` dispatcher. The current production adapter stops
at missing configuration, receipt, recovery or fault hooks. It supplies no
mechanism. The future mechanism owner must bind those hooks to production
operations. A parser that merely accepts the table cannot satisfy the contract.

Every recovery row requires a receipt naming a commit digest, a mutation cutoff
and an envelope epoch. The test reads that record from each configured replica
before loss and checks its hash. It captures namespace rows, all eight catalog
tables as exact key/value bytes and the original key envelope at the receipt.
Catalog bytes cover pool identity, the complete member set, every mapping's
physical generation and checksum, debts and capacity charges. The exact-recovery
row creates a real retired physical object and a started, unpublished physical
upload through production catalog transactions. Both debt tables and their
capacity charges must be nonzero before and after recovery. The envelope digest,
format version, encoded name mode and committed envelope epoch have separate
checks. The old envelope format does not substitute for an epoch or name mode.

The test removes the active authority objects and catalog before cold recovery.
For journal loss it also removes the original authority device, so the original
catalog and namespace cannot accidentally satisfy the test. Recovery must use
only receipt-covered replica content and an owned scratch directory. The test
hashes surviving source evidence before and after recovery and refuses source
mutation. Logical directories model permanent state loss, not power loss or
independent hardware devices. Every owned directory must be absent after cleanup.

| Acceptance row | Oracle and current missing dependency |
| --- | --- |
| Authority device loss | Recover the receipt's exact namespace, catalog, envelope and readable file hashes from the other replica after removing the authority device. Replica configuration and named survival receipts are absent. |
| Journal loss before backend cutoff | Commit the base prefix, create a real SSD-durable suffix with remote replay paused, lose the journal and the original authority, and recover the state captured at the base receipt. Report the local-only suffix as lost. Named survival receipts are absent. |
| Journal loss after backend cutoff | Drain the real suffix to the pool, commit and capture that suffix, then lose the journal and original authority. Recover the exact receipt-covered suffix and report the local-only loss boundary. Named survival receipts are absent. |
| A data member with a metadata replica is lost. | Capture each current file extent's member, exact file names including hardlinks, byte offset, length and expected hash. Require at least one current sole-copy range on the lost member and one on a survivor. Lose its whole device, recover the surviving authority replica in scratch, compare the exact lost-range report, require EIO for every lost range and hash every other range and empty file. Replica recovery and current-range reporting are absent. |
| A retained payload becomes dead in the live namespace. | Obtain a survival receipt, overwrite a file into a different segment, prove its old segment has zero live references, and run a completed reclaim pass. Require the old physical generation and its hash to remain. Lose the authority device and recover the receipt's version of every file by hash. Survival receipts and payload-retention enforcement are missing. |
| An admitted write is interrupted by device loss. | Install the mid-commit hook, commit the prior prefix, start a real admitted write and wait for its entered barrier while its task is still pending. Remove the device before releasing that task. Recover the prior receipt. The admitted-write mid-commit hook is absent. |
| A survival commit is interrupted. | Use the typed `CommitPoint` enum, before and after each point, for each replica and each logical device loss. Require the real task to enter and block at that exact hook before removing the device. Compare recovery with a complete allowed generation. The production commit cut hook is absent. |
| Exact catalog, debts and key epoch recovery | Compare all captured bytes and require nonzero upload and garbage debts, capacity charges, envelope digest, format version, name mode and committed epoch. Replica configuration and complete generation export are absent. |
| A slow replica exceeds its timeout. | Inject a delay past 30 seconds, require a failed survival operation, a failed-replica report and a read-only share. The slow-replica hook is absent. |
| A replica is full. | Inject ENOSPC, require stopped survival acknowledgements, a failed-replica report and a read-only share. The full-replica hook is absent. |
| A replica is stale. | Supply the preceding generation as the configured replica, require refusal and a read-only share. The stale-replica hook is absent. |
| A replica returns an I/O error. | Inject EIO during publication or sync, require refusal and a read-only share. The I/O-fault hook is absent. |
| A competing writer opens. | Keep the first writer's per-replica locks, attempt a second writer and require refusal while the first still owns every lock. The replica-lock hook is absent. |
| A replaced replica catches up. | Refuse catch-up online. Stop the writer, perform offline catch-up, compare every retained generation file's hash and independently verify the new replica/device identity with the same pool and membership before permitting writable startup. The replacement hook is absent. |

The ordinary `unretained_superseded_payload_is_reclaimed` control uses the same
overwrite and reclaim fixture without a survival receipt. It verifies a zero-live
old segment and a completed collector pass, then requires the old object to be
gone and the payload-retention oracle to reject that loss. This is a real
counterexample to payload survival today, not a passing survival row.

The typed cut matrix currently names 18 points, two sides, two replicas and four
logical devices, for 288 hook requests. Dependency publication and sync points
must repeat for each actual dependency path when the production owner is wired.
The stage does not claim that it has injected any of those cuts. A missing hook
returns a specific prerequisite failure. Each exercised cut needs a fresh
fixture. The matrix returns success only if every requested case passes; it has
no unconditional failure after a newly working implementation.

`CommitCutHook::reach` is the test-only boundary that the future commit owner
must call with the exact affected object path. The event preserves that path.
Its normal test checks that every requested boundary blocks until released and
that an unrelated replica or an invalid object path cannot trigger it. This
tests the hook, not production interruption safety.

`every_acceptance_oracle_rejects_success_without_survival` is the mutation check.
Its test-only `StubSurvival` accepts the proposed configuration and returns a
successful named receipt without replicating content. Every row's own oracle
must reject that stub. A forged success value cannot supply a commit file on a
replica, a cold recovered namespace, exact nonempty debt tables, failed-range
reads, a blocked admitted task, an entered commit hook, a held writer lock or a
verified replacement. The mutation test also supplies optimistic success
reports and still requires physical replica evidence. A separate comparison
test rejects removal or mutation of catalog rows, envelope fields and exact
range outcomes. Both run in the ordinary module suite.

The ordinary module run skips the 14 acceptance rows and passes the four
durability and retention controls, the two mutation checks and the test of the 288 requested
test-channel boundaries. Those channel events are not production commit cuts.
Each ignored row must also be run explicitly,
with its specific missing-dependency line retained. These intentional failures
are design evidence, not survival evidence. The external report records exact
commands, counts, cleanup and the per-row stub result.

The future bindings must also enforce the wider required matrix from the
remote design review. Replica faults must occur separately before and during
publication, ENOSPC must preserve the configured reserve, and corrupt or stale
content must be detected from bytes and identities. Competing writers must run
in distinct processes. Cut requests must expand to every actual object and sync
operation, and only observed production events count as executed cases. These
requirements remain unverified while the hooks are absent.

Repeated-commit retirement tests remain required before implementation can pass
its gate. They must overwrite and remove data, hold and release an older reader,
cut before and after retirement and export-pin release, verify that owned live
pins reach zero, and prove safe reclamation of old dependencies and debts. These
are future mechanism tests, not additional executed rows in this design stage.
The wider comparison matrix still includes key-only and catalog-only loss,
corruption, metadata-only workloads and refusal of password changes. Credential transitions remain
disabled until their own interrupted-change matrix passes.

## Unverified assumptions

- Not verified: The full set of namespace, catalog, health, compaction, GC and
  membership mutators can share the proposed admission and drain protocol
  without deadlock. Audit every caller before choosing lock order.
- Not verified: SlateDB checkpoint dependency enumeration includes every
  transitive object and works with ZeroFS's split metadata and extent tables.
  Exercise the pinned dependency API before adopting the format.
- Not verified: Multipart replication preserves logical preconditions and
  cancellation ownership for every authority mutation. Native local durability
  and the new manager need interrupted-sync tests.
- Not verified: Recovery can preserve catalog identity while rebinding physical
  paths without weakening the existing wrong-pool and wrong-directory checks.
- Not verified: The key recovery record, canonical encoding and HMAC bootstrap
  have a reviewed compatibility and authentication contract. Current key wrapping
  at the pinned head authenticates no pool or epoch associated data. The commit
  must supply that binding without altering the replicated envelope bytes.
  Credential transitions remain disabled until their matrix passes.
- Not verified: This checkout predates PR 78, so the name-mode field and its
  create/load compatibility inside `zerofs.key` have not been source-verified
  here. The accepted contract requires an exact copy of the envelope and no
  separate name-mode dependency. Verify the merged envelope schema before
  implementation and refuse an unknown required mode.
- Not verified: Exclusive locks on every replica and the existing database
  writer epoch fence every single-host writer path through loss and restart.
  Test lock lifetime, competing startup, missing replicas and offline catch-up.
- Not verified: Operator device IDs map to independent hardware and all mount,
  filesystem encryption and password material survives one device loss. Local
  directory fixtures cannot prove those deployment conditions.

The coordinator has selected the initial mechanism. The operating contract
above still awaits operator confirmation. The first version uses a quiescent
commit, a fixed replica set, lifetime exclusive locks
on every replica and all-replica acknowledgement on one host. HA and password
changes are refused. No external freshness service is part of this contract,
and recovery reports the stale self-copy limit. The remaining work is to test
the assumptions above, including the exact drain and checkpoint APIs. If the
serialization owner cannot exclude every mutator without deadlock, stop.
Writable recovery, election, degraded commits, automatic membership changes,
payload redundancy, hardware power-loss claims, NAS validation, benchmarks,
ACLs, extended attributes and strict placement enablement stay outside this stage.
