# Consistent metadata recovery after one device is lost

This is the first design stage of the metadata survival implementation. It is
verified against ZeroFS commit `9b67f82eddedb30d268ac31fccaa0e3d49d9f2c3`.
The survival gate still fails. This change adds no replication, recovery command,
survival acknowledgement or placement capability. The proposed mechanism is
promising on source inspection only. The ignored acceptance tests record missing
dependencies, not a safety proof. The coordinator accepted the first mechanism
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
   meaning and cost.
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
| Config parsing | `zerofs/src/config.rs:669` denies unknown pool fields. `Settings::from_file` at line 1958 parses and validates. The acceptance fixture deliberately submits the proposed `metadata_survival` table here. This is a real missing dependency today. |

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
3. The owner creates a non-expiring `Durable` SlateDB checkpoint and receives its
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
   hashes from that replica. It installs retention for the candidate and the
   preceding selectable commit. No commit record is written until every replica
   has every metadata dependency. Payload remains on its one assigned member.
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
8. The owner may release admission. Cleanup can retire dependencies of older
   commits only when every configured replica has a newer durable commit and no
   reader or retained checkpoint references them. It logs each deletion intent
   on every replica first. It deletes the primary object before replica copies
   only within this permission. Physical debt remains charged until a subsequent
   survival commit records completion. A loss mid-cleanup therefore preserves
   conservative, possibly already satisfied debt and never undercounts it.

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

## Failing acceptance matrix

`zerofs/src/metadata_survival_tests.rs` keeps the existing authority-loss test and
adds separately ignored rows. Every row uses disposable local directories,
closes workers and reloads the existing encrypted namespace before its loss.
Removing a directory models permanent state loss, not power loss. The fixture
does not model independent hardware devices or a process killed during fsync.
It must remove its directories before reporting an expected failure.

| Acceptance row | Required cold result and current missing dependency |
| --- | --- |
| Authority device loss | Keep the existing real pool and namespace test. Current recovery fails because the catalog is missing. |
| Journal loss before backend cutoff | Create a real SSD-durable suffix with remote replay paused, discard the journal and cold-read the prior backend prefix. Then require a survival generation through the proposed replica configuration. The current parser rejects that configuration. The local-only suffix is allowed to disappear. |
| Journal loss after backend cutoff | Drain the real writeback suffix to the pool, discard the journal and cold-read that exact suffix. Require the committed recovery generation without relying on the journal. The current parser rejects the proposed configuration. |
| Payload member with a metadata replica lost | Configure the proposed sibling replica directory, lose its whole logical device and attempt cold recovery. The current parser prevents provisioning a real metadata replica. The row must report that limit instead of copying files to fake replication. |
| Writes in flight during loss | Keep an admitted operation blocked, lose a device, discard its uncommitted work and cold-recover the last survival prefix. The replica configuration and commit owner do not exist. The fixture must label that prerequisite before claiming the concurrency cut was exercised. |
| Every interrupted commit step | Enumerate before and after each durable step above, each replica and each single-device loss. Attempt the proposed configuration for each case and collect every failure. No survival cut hook exists yet, so none of these cuts is exercised by stage 1. No synthetic commit record counts as an implementation test. |
| Exact catalog, cleanup debts and envelope epoch | Cold recovery must compare every catalog table and identity field, nonzero upload and garbage debts, capacity accounting, original envelope digest and committed epoch. The proposed configuration fails first. An empty debt list or the legacy envelope's format version is not evidence of debt or epoch recovery. |

The ordinary module run must skip every acceptance row and pass the baseline
reproduction. Explicitly running each ignored test must fail at the catalog or
configuration dependency named above. The external evidence directory records
the exact tested head, commands, counts, failure lines and cleanup inventory.
The full matrix in the comparison document still applies, including key-only
loss, catalog-only loss, corrupt and full replicas, metadata-only workloads,
stale repair and refusal of password changes. Interrupted password changes must
pass their credential matrix before that operation can be enabled.

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

The coordinator has answered the initial operating-policy questions. The first
version uses a quiescent commit, a fixed replica set, lifetime exclusive locks
on every replica and all-replica acknowledgement on one host. HA and password
changes are refused. No external freshness service is part of this contract,
and recovery reports the stale self-copy limit. The remaining work is to test
the assumptions above, including the exact drain and checkpoint APIs. If the
serialization owner cannot exclude every mutator without deadlock, stop.
Writable recovery, election, degraded commits, automatic membership changes,
payload redundancy, hardware power-loss claims, NAS validation, benchmarks,
ACLs, extended attributes and strict placement enablement stay outside this stage.
