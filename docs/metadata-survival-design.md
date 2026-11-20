# Consistent metadata recovery after one device is lost

> **Implementation status.** Plan row M1 implements this mechanism. See
> [metadata-survival.md](metadata-survival.md) for what is delivered, how to
> configure and recover it, and the design items still deferred. The text
> below is the accepted design as written before implementation.
>
> **Raw-device target (2026-11-18).** ZeroFS controls the authority NVMe and
> the HDD members directly as raw block devices. On that topology every pool
> member holds the recovery generations in its own object log, and the
> ZFS-specific guidance below does not apply: pool-GUID and vdev bindings,
> `sync`, SLOG and `recordsize` assumptions, and the TrueNAS HDD measurement
> recipe.

This is the first design stage of the metadata survival implementation. It is
verified against ZeroFS commit `9b67f82eddedb30d268ac31fccaa0e3d49d9f2c3`.
The metadata survival gate, which requires the namespace, placement catalog and
encryption material to survive the loss of any one device, still fails. This
change adds no replication, recovery command, survival acknowledgement or
placement capability. Source inspection is the only support for the proposed
mechanism. The ignored acceptance contracts fail
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

## Chosen mechanism

The live SlateDB layout, catalog and key envelope stay on the authority alone.
Survival copies their recovery dependencies to every configured metadata replica
at commit time. There is no live mirroring of puts, multipart completion, CAS,
renames or deletes. Ordinary conditional operations keep the authority's native
semantics. The authority wrapper routes every delete, overwrite and rename
through the retention ledger, including internal manifest, SST, GC-boundary,
bucket-identity and key paths. It preserves an old version before replacing it
when a retained generation or candidate needs those bytes.

One serialization owner establishes a common namespace and catalog prefix.
It pre-drains writeback with admission open. It then closes admission for the
final drain, seal, flush, checkpoint and catalog export, a pause bounded by
`final_pause_max_milliseconds`. It reopens
admission before any replica I/O, so replica publication runs with writes
admitted. Ordinary memory, local SSD and backend receipts keep their current
meaning. They can wait at this bounded gate, but do not wait for a survival
receipt of their own. Recovery uses verified immutable copies in owned scratch and never
opens the surviving sources through today's mutating startup path.

This stage changes design text and executable acceptance contracts only.
It adds no production survival mechanism. If the serialization owner cannot
exclude every mutator without deadlock, stop implementation and revisit the
cross-store transaction rather than mirror independent latest files.

## Source integration points

Paths and line numbers in this section refer to the pinned ZeroFS commit.
Dependency paths below refer to SlateDB revision
`230ade681f1fdf0f85fc1c2cb6c118575a83ceb1`, pinned at
`zerofs/Cargo.toml:76` and `zerofs/Cargo.lock:5532`. Its repository is
`https://github.com/Barre/slatedb`. The dependency sources were inspected in the
local Cargo checkout, not inferred from upstream documentation.

| Integration point | Verified behavior and required change |
| --- | --- |
| Pool startup | `zerofs/src/storage_backend.rs:111` validates paths and refuses a missing catalog at line 122. It constructs one local authority at line 127 and the pool at line 180. Construct the retention wrapper before startup passes the authority into the pool. Copy recovery dependencies only during a commit. Keep ordinary startup refusal. Add an explicit recovery entry point before this path. |
| Directory and device identity | `zerofs/src/storage_backend.rs:263` rejects overlapping authority, catalog, cache, journal and member roots. `zerofs/src/storage_backend.rs:308` derives member identity from its location. Replicas on data devices need disjoint sibling directories, device bindings and mount validation. Different directory names alone do not prove independent devices. |
| Authority writes | `zerofs/src/pool_object_store/mod.rs:1875` forwards non-segment puts to the authority at line 1884. Multipart operations take a separate route at line 1902. Wrap the underlying authority, including privileged pool-marker creation at line 550, rather than only the public pool put method. |
| Authority deletion and movement | `zerofs/src/pool_object_store/mod.rs:2000` implements deletion streams and forwards authority deletion at line 2016. Non-segment copy delegates at line 2115. Rename delegates at line 2165. All destructive authority paths must preserve retained versions through the ledger. They do not mirror live mutations. Internal `.zerofs-pool` paths are reserved at line 643, so the replica manager needs its own owned operations. |
| Conditional authority objects | `zerofs/src/key_management.rs:279` creates the key using `PutMode::Create`. `zerofs/src/bucket_identity.rs:69` creates `.zerofs_bucket_id` the same way. `zerofs/src/local_object_store.rs:161` handles create and conditional update under local locks. The live authority makes the create decision. A survival commit copies the winning bytes and binds their digest. |
| Local durability | `zerofs/src/local_object_store.rs:58` enables fsync. Directory ancestry sync is at line 105. Standalone deletes sync the parent at line 247. A replica implementation must preserve these properties, including owned workers after caller cancellation. These calls are not hardware power-loss evidence. |
| Namespace flush completion | `zerofs/src/fs/flush_coordinator.rs:119` holds the database write barrier through seal and flush. The typed durability path at line 224 materializes a cutoff, seals and flushes at line 242, releases the barrier, then waits for object coverage at line 258. `zerofs/src/db.rs:947` awaits SlateDB flush. Extend this ordering with a separate survival operation and receipt. Existing backend receipts must retain their current meaning. |
| Database exclusion | `zerofs/src/db.rs:885` admits database batches under the barrier's read side. It does not serialize redb or all maintenance. The new owner must sit outside this barrier and drain operations before taking it. A pool operation must not acquire the new gate recursively while the commit owner waits for its drain. |
| Production WAL and background flush | `zerofs/src/cli/server.rs:1162` disables the WAL and automatic size-driven metadata freezing. The settings at line 1170 preserve barrier-controlled publication. `zerofs/src/cli/init.rs:237` can still construct a separate WAL store. Reject that setting in the initial survival mode and keep the WAL disabled in acceptance fixtures. |
| Catalog commits | `zerofs/src/pool_object_store/catalog.rs:658` reserves in an immediate transaction and commits at line 736. Publication starts at line 767 and commits its mapping and old-generation debt at line 835. Retirement commits at line 856. Upload and garbage debt removal commit at lines 887 and 915. Health, membership and fault transactions also need the new owner, including lines 362, 470, 551 and 578. |
| Catalog contents and identity | `zerofs/src/pool_object_store/catalog.rs:40` defines eight tables. Identity at line 51 includes the pool UUID, authority prefix and canonical catalog directory. Opening checks the directory at line 295 and the marker at line 302. The current snapshot at line 932 exports only mappings and hidden legacy objects. Add a complete logical export and a versioned recovery binding, rather than copying `pool.redb` or calling the partial snapshot a backup. |
| Payload publication and cleanup | `zerofs/src/pool_object_store/mod.rs:856` writes a fresh physical generation, publishes its catalog entry at line 943 and immediately cleans the previous generation at line 946. `delete_owned` at line 1803 retires and cleans. `cleanup_garbage` at line 1327 deletes bytes before forgetting debt. All three need recovery-reference retention and durable cleanup intents. |
| Startup side effects | `zerofs/src/pool_object_store/mod.rs:534` configures members, line 584 finishes initialization and line 588 invokes pending recovery. `zerofs/src/pool_object_store/catalog.rs:271` opens a write transaction even for ordinary reopen. A read-only recovery path must bypass these operations. |
| Key loading and rotation | `zerofs/src/cli/init.rs:225` normally uses load-or-initialize. `zerofs/src/key_management.rs:210` instead refuses a missing existing envelope. Recovery must use the latter behavior. The envelope at line 25 has salt, nonce, wrapped DEK and version, but no pool binding, epoch or encoded KDF parameters. Password change at line 302 overwrites one envelope at line 323. The first survival version must refuse password changes whenever metadata replicas are configured. |
| Writeback placement | `zerofs/src/cli/init.rs:267` attaches writeback above the backend after loading the key. Hooks at lines 1167 through 1207 capture and wait for object coverage. `zerofs/src/writeback/bootstrap.rs:27` recovers a paused overlay. The authority retention wrapper belongs below writeback, so a local journal acknowledgement does not imply replicated durability. Survival records bypass the evictable cache and journal and reach every metadata device directly. |
| Writeback retirement | `zerofs/src/writeback/journal.rs:1371` advances remote completion. `zerofs/src/writeback/journal.rs:1720` removes remote prefixes. A survival commit must stop relying on a journal prefix before it retires it. Losing the journal may still lose a documented local-only suffix. |
| Writer locks | `zerofs/src/pool_object_store/catalog.rs:250` holds an exclusive catalog lock. `zerofs/src/local_object_store.rs:95` orders local locks and keeps their files stable. Neither locks every metadata replica. Add a persistent lock file on each replica and hold every exclusive lock for the writer's lifetime, together with the existing SlateDB writer epoch. |
| Config parsing | `zerofs/src/config.rs:669` denies unknown pool fields. `Settings::from_file` at line 1958 parses and validates. The proposed `metadata_survival` table is unsupported. Configuration acceptance alone provides no replication or recovery evidence. |

The pinned SlateDB source behaves as follows, including its limits.

| Dependency source | Verified behavior |
| --- | --- |
| `slatedb/src/db.rs:1762` | `flush` waits for memtable flush when the WAL is disabled. It returns no manifest identity. |
| `slatedb/src/checkpoint.rs:30` | `create_checkpoint` returns a checkpoint UUID and manifest ID. `CheckpointScope::All` flushes first. `Durable` pins the already durable state. Use `Durable` after ZeroFS seals and flushes under exclusion. |
| `slatedb/src/manifest/store.rs:130` | Writing a checkpoint updates the manifest. Checkpoint creation itself therefore produces authority writes that must drain before the survival record. |
| `slatedb-txn-obj/src/object_store.rs:369` | Manifest versions use sequential object names and conditional create. Line 503 deletes versions. Lines 316 and 325 update or create mutable GC boundaries. ETags stay local to the live authority. Immutable commit copies use content hashes. |
| `slatedb/src/tablestore.rs:355` | Compacted SSTs use streaming publication. WAL SSTs use a conditional single put. Line 535 deletes SSTs. Multipart completion stays on the authority. A later survival commit copies the immutable result. |
| `slatedb/src/db_reader.rs:722` | A pinned checkpoint reader does not poll for a newer manifest. Lines 762 through 779 restrict checkpoint cleanup to managed readers. Use `DbReaderMode::Checkpoint`, not a managed reader that writes checkpoint state. |

## Configuration and durable enablement

The optional table activates survival only for a pool that has never enabled it.
After activation, authenticated durable pool state records enablement, pool UUID,
membership digest and epoch. Removing the table does not disable survival.
Normal startup refuses a missing table, a different replica set or a stale
replica when durable state says survival is enabled. Startup performs these
checks before cleanup or admitting writes. Reclaim consults retained generations
even if the table is absent. Configuration alone never authorizes deletion.

```toml
[storage.pool.metadata_survival]
format_version = 1
authority_replica_id = "authority"
authority_device_id = "hardware-binding-a"
recovery_mode = "normal"
reserve_free_bytes = 67108864
commit_interval_seconds = 60
replica_timeout_seconds = 90
commit_deadline_seconds = 180
checkpoint_lifetime_seconds = 600
predrain_max_seconds = 120
final_residual_max_bytes = 16777216
final_pause_max_milliseconds = 1000

[[storage.pool.metadata_survival.replicas]]
id = "disk-a-metadata"
device_id = "hardware-binding-b"
dir = "/mnt/disk-a/metadata-replica"
```

The authority counts as one replica. Require at least two independent devices.
Directories on data members must be disjoint siblings of payload roots.
Bind devices to disk serials. Reject aliases and replicas on the authority's
device. Operator labels alone and local
directory fixtures cannot establish independence. Share physical capacity
accounting between payload and metadata roots on the same device.

An explicit offline `disable` command requires every configured replica and
writes an authenticated terminal record to each before changing durable pool
state. A partial terminal fanout blocks writable startup until the same operation
completes. A terminal record invalidates every commit in that membership epoch
for recovery, even when its dependency bytes remain. Read-only recovery reports
that survival was disabled rather than silently restoring old names. Do not
remove the retention ledger until terminal publication completes everywhere.
Re-enabling creates a fresh membership epoch and fresh replica roots, with an
initial verified commit before writes. Old roots cannot rejoin the new epoch.

The first implementation is single-host. Refuse HA and password changes while
survival is enabled, including when its table is missing. The original
`zerofs.key` envelope carries the name mode. Copy it byte for byte. It has no
separate name-mode dependency. There is no automatic election or online
membership change.

Before writable startup, lock every configured replica in stable replica-ID
order and retain those persistent lock files for the writer's lifetime. Keep
the existing database writer epoch too. A failed acquisition releases acquired
locks and refuses startup. Never unlink a possibly held lock file. A replaced
or genuinely missing replica requires offline verified catch-up in an explicit
epoch transition. A timeout before any commit record is published requires no
offline repair and may rejoin on the next online retry.

### Admission, locks and read observations

Every top-level filesystem, protocol and maintenance mutation acquires exactly
one admission token before a metadata conflict fence, inode lock, pool lock,
database barrier or catalog transaction. Within an operation preserve the
existing fence-before-inode order, use stable ordering for multiple inode or
pool locks, and take the database barrier before catalog transactions. No code
waits for admission while holding any of those locks. Detached work carries its
original token through completion. An internal writeback replay uses the
owner's existing capability and never recursively acquires admission.

The commit owner closes admission without holding locks needed by admitted
work. It waits for those tokens and maintenance idle acknowledgements, then
acquires the database barrier. It never holds a catalog transaction or a pool
lock across backend replay. Read paths do not acquire mutation tokens.

Reads currently write catalog state through `record_read_failure` and
`record_source_fault`, `observe_io_error` and `quarantine`. SMART, kernel cursor
and monitoring observations also commit catalog transactions. While admission
is closed, defer all of these advisory writes in a bounded in-memory queue.
Enforce an immediate in-memory quarantine for allocation and return the original
read result, including EIO, without waiting for catalog writes. Coalesce repeated
observations by member and segment. On overflow preserve quarantine and report
an overflow diagnostic. Do not block reads or silently erase the fault.
Flush queued observations under an admission token after reopening, so they
belong to the next catalog generation. In latched read-only mode keep them in
memory and report them, without changing the catalog or source evidence.

## Operating contract awaiting operator confirmation

These are coordinator decisions, pending deployment confirmation. They revise
the earlier first-version rule that made every device loss read-only. A home
NAS must remain writable for unaffected files after losing a payload-only disk.
The metadata replica set must not depend on a single capacity HDD alone.

| Event | Required live behavior |
| --- | --- |
| The authority or a metadata replica device is lost. | Close mutation admission. Mutations fail at once with EROFS. Stop normal export if its namespace view is unsafe. Require offline promotion or verified replacement before writable startup. |
| A data member holding no metadata replica is lost. | Keep unaffected files writable. Exclude the lost member from allocation. Return EIO for exactly its current sole-copy ranges. Keep names, holes and empty files. |
| The NVMe journal is lost. | Stop service and recover a survival generation. Report the later local-only suffix as lost. Do not invent an exact lost-write count from a missing journal. |
| A replica times out before any record publication. | Abort the attempt, settle owned I/O, report increasing lag and retry online. Admission stays open. Do not advance the last successful sequence. |
| A timeout or crash leaves a durable record on only some replicas. | Close admission, stop new publication and reconcile that exact candidate before admission opens again. A replica that missed a durable record needs offline repair if reconciliation cannot complete. |
| A replica is full, stale, corrupt, has the wrong identity or returns an I/O error. | Refuse survival success, report the replica and latch mutation admission closed. Mutations fail at once with EIO. Preserve retained generations and bounded queued work. |
| A competing writer starts. | Refuse it while the first writer retains all replica locks. |
| A replica is replaced. | Refuse online replacement. Require an offline epoch transition and verified identities and all retained bytes. |

A full replica or transient I/O error before record publication can recover
online after the cause clears and the owner revalidates identity, retained
bytes and every durable record on the unchanged set. Admission stays closed
until those checks pass. Only actual device loss or a replica that missed a
durable commit requires offline repair. Identity conflicts and unverifiable
bytes refuse the check rather than permitting unsafe service.

The proposed per-replica timeout is 90 seconds, including publication, sync and
verification, to allow HDD spin-up beyond 30 seconds. The proposed commit
deadline is 180 seconds, counted from closing admission to the last record.
The checkpoint lifetime is 600 seconds and must exceed the commit deadline plus
the maximum owned-I/O settlement margin. A timeout cannot cancel an in-progress
fsync or release its locks early. Stop issuing new I/O, settle owned operations,
record abandonment, then report the failure. A device that cannot settle or is
actually lost follows the device-loss rule. Injected timeout tests must bound
the call by the timeout plus a five-second settlement margin.

### What a write waits for

Every number in this section is a proposed default. None of them comes from a
measurement. The defaults that depend on HDD latency stay unverified until the
HDD measurement below runs.

A survival attempt has three phases, and only the middle one closes admission.

| Phase | Admission | What a new mutation waits for | When the budget runs out |
| --- | --- | --- | --- |
| Pre-drain | Open | Nothing that survival adds. Ordinary writeback backpressure is unchanged. | After `predrain_max_seconds` the owner skips the attempt with admission still open, returns `Timeout` to an explicit request and reports lag. Writers see nothing. |
| Final pause | Closed | The admission gate, for at most `final_pause_max_milliseconds` from the mutation's arrival. | The owner aborts before any record, settles its own I/O and reopens admission. A mutation still waiting when its own bound expires fails without effect with EAGAIN. |
| Replica publication | Open | Nothing. Replica I/O, including HDD fsync and spin-up, runs after admission reopens. | A replica timeout ends the attempt with `Timeout` and admission stays open. A full, stale, corrupt or failing replica latches admission closed. |

Latched states do not make a mutation wait. While a replica fault holds
admission closed, a mutation fails at once with EIO. After the authority or a
metadata replica device is lost, and in offline recovery, a mutation fails at
once with EROFS until offline promotion or verified replacement.

These errors reach programs unchanged through both client paths.
`FsError::RetryLater` maps to errno 11, EAGAIN, at `zerofs/src/fs/errors.rs:141`.
`FsError::ReadOnlyFilesystem` maps to errno 30, EROFS, at line 148, and
`FsError::IoError` to errno 5, EIO. The 9P server returns that errno in its
`Rlerror` reply, and the Linux v9fs client returns it from the system call,
such as write(2). The FUSE client `zerofs mount` speaks 9P to the server and
answers the kernel with the same errno at `zerofs/src/mount.rs:255`, so a
program on a FUSE mount sees the same value. ZeroFS already returns EAGAIN this
way when writeback admission is backpressured, at
`zerofs/src/fs/mutation/overlay_helpers.rs:61`. Returning EAGAIN rather than
EROFS reserves the read-only error for states that need an operator.

| Key | Proposed default | Needs measuring on HDD-backed replicas or members |
| --- | --- | --- |
| `predrain_max_seconds` | 120 | Yes. HDD payload members absorb the pre-drain backlog. |
| `final_residual_max_bytes` | 16,777,216 (16 MiB) | Yes. The final pause waits for this residual to reach HDD payload members. |
| `final_pause_max_milliseconds` | 1,000 | Yes, through the residual drain. Seal, flush, checkpoint and catalog export touch only the authority device. |
| `replica_timeout_seconds` | 90 | Yes. It must cover publication, sync and spin-up on an HDD replica. |
| `commit_deadline_seconds` | 180 | Yes. It must cover publication on every replica. |
| `commit_interval_seconds` | 60 | Yes, for standby. It decides whether HDDs can sleep while writes continue. |
| `checkpoint_lifetime_seconds` | 600 | Only through the commit deadline it must exceed. |

The 16 MiB residual is a guess sized to leave most of the 1,000 ms pause for
seal, flush, checkpoint and export. The acceptance probes bound one mutation by
the final pause plus one second, through `FINAL_PAUSE_MAX` in
`zerofs/src/metadata_survival_tests/acceptance.rs`. The closed-phase rows
expect EAGAIN, and the latched read-only and device-loss rows expect EROFS.

Before closing admission, pre-drain the accepted writeback cutoff with writes
still admitted. Close admission only when the remaining lag is at most
`final_residual_max_bytes`. If incoming traffic keeps the lag above that for
`predrain_max_seconds`, skip the attempt with admission open and report lag.
A 200 GB backlog must not turn an ordinary bulk copy into a read-only outage.
A missed deadline never permits dropping a replica or forgetting a generation.
Finite admission and retention budgets remain mandatory. Budget exhaustion
refuses mutations before consuming the physical free reserve.

Reopening admission before replica publication is a proposal from the review
follow-up and needs the coordinator's confirmation. The earlier ordering kept
admission closed until every replica held the record, which put HDD fsync and
spin-up inside the pause, for up to the 180-second commit deadline. The
candidate is fixed once the checkpoint exists and the catalog export is taken.
The ledger protects its dependencies from the writes that follow, so replica
publication does not need admission closed.

### Measurement that settles the HDD budgets

The HDD-dependent defaults above are unverified. This measurement settles them.
Run it on the NAS that will run survival, against the HDD that would hold a
metadata replica and separately against one HDD payload member. Keep the
production filesystem and its sync and record-size settings unchanged. An SSD, a virtual disk or ubuntu-main does not count.

The probe writes each file, fsyncs it and then fsyncs its directory. That is
the `2 * N` sync pattern of one replica publication. Save it as
`fsync-probe.py`:

```python
#!/usr/bin/env python3
# Times one metadata-replica publication per commit: each file is written and
# fsynced, then its directory is fsynced. Prints one JSON line.
import argparse, json, os, statistics, time

parser = argparse.ArgumentParser()
parser.add_argument("directory", help="an empty directory on the disk under test")
parser.add_argument("--sizes", default="1048576,65536,16384", help="bytes per file")
parser.add_argument("--commits", type=int, default=200)
args = parser.parse_args()
sizes = [int(size) for size in args.sizes.split(",")]
payloads = [os.urandom(size) for size in sizes]
directory = os.open(args.directory, os.O_RDONLY | os.O_DIRECTORY)
seconds = []
for commit in range(args.commits):
    started = time.monotonic()
    for index, payload in enumerate(payloads):
        path = os.path.join(args.directory, f"{commit}-{index}")
        fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        view = memoryview(payload)
        while view:
            view = view[os.write(fd, view):]
        os.fsync(fd)
        os.close(fd)
        os.fsync(directory)
    seconds.append(time.monotonic() - started)
for name in os.listdir(args.directory):
    os.unlink(os.path.join(args.directory, name))
os.fsync(directory)
seconds.sort()
print(json.dumps({
    "commits": len(seconds), "sizes": sizes,
    "p50": statistics.median(seconds),
    "p99": seconds[min(len(seconds) - 1, int(len(seconds) * 0.99))],
    "max": seconds[-1],
}))
```

Make an empty directory beside the replica directory on each disk, for example
`/mnt/disk-a/fsync-probe`, and run these three measurements.

1. Replica publication on a spinning disk. Run
   `python3 fsync-probe.py /mnt/disk-a/fsync-probe` on the replica disk. The
   default sizes are the worked example above, with 1 MiB of SSTs, 64 KiB of
   catalog and 16 KiB of record. Repeat with
   `--sizes 1048576,1048576,1048576,1048576,1048576,1048576,1048576,1048576,1048576,1048576`
   and with `--sizes 16384` to bracket the range.
2. Replica publication from standby. Spin the replica disk down with
   `hdparm -y /dev/<disk>`, confirm standby with `hdparm -C /dev/<disk>`, then
   run the probe with `--commits 1`. Repeat ten times and keep every result.
   On TrueNAS CORE use `camcontrol standby` and `camcontrol cmd` instead.
3. Residual drain on a payload member. On the HDD payload member, run the
   probe with `--sizes 16777216 --commits 50`, then with 4194304 and with
   1048576 in place of 16777216.

Record the disk model and firmware, the pool layout, the `sync` and SLOG
settings, the standby timer and every JSON line. Then apply these rules.

| Result | Design change |
| --- | --- |
| Any standby run in measurement 2 exceeds 60 seconds. | Raise `replica_timeout_seconds` to at least 1.5 times the largest standby result and `commit_deadline_seconds` to twice that, or refuse metadata replicas on disks with standby enabled. |
| The spinning p99 in measurement 1 exceeds 9 seconds, a tenth of the replica timeout. | Ordinary commits would run close to the timeout. Raise the timeout as above or move the replica off HDD. |
| The p99 in measurement 3 at 16 MiB exceeds 500 ms, half the final pause. | Lower `final_residual_max_bytes` to the largest size whose p99 stays under 500 ms. |
| The p99 in measurement 3 at 1 MiB exceeds 500 ms. | No residual budget fits a 1,000 ms pause on this hardware. The coordinator must choose a longer `final_pause_max_milliseconds`, which writers then wait for at each commit. |
| Every result stays inside these limits. | Keep the defaults, record the results here and remove the unverified label. |

Commits run on explicit request and an adaptive configurable interval, initially
60 seconds. Skip publication when the mutation cutoff, catalog generation and
envelope epoch are unchanged. An explicit request then returns the existing
receipt and records a successful freshness check without a new generation.
Reads alone do not force commits unless their deferred observations change the
catalog. Idle skipping lets HDDs sleep. Active minute-by-minute commits can
prevent standby and add spin-up and fsync latency when disks were asleep.

Status reports the last successful sequence and digest, completion time, elapsed
lag, failed replica IDs and errors, retry state, admission state, pre-drain
bytes, retained bytes and owned checkpoint count. Failed or stuck attempts never
advance success fields. Before the first success, report no recovery point.
After an idle freshness check, report no uncovered mutations and retain the
original generation completion time. The interval is a schedule, not an achieved
recovery age. Writes acknowledged after the last survival commit may lose their
names and extent mappings with the authority even if their payload reached HDD.

### Space, writes and record bounds

Copy immutable dependencies by digest. Cache verified hashes for immutable SST
identities and hash only new SSTs on the live authority. Verify actual replica
bytes before first adoption and again on recovery. Reuse catalog table chunks
by content hash. A logical export reads one redb snapshot and replaces only
changed chunks plus its authenticated root. An unchanged catalog costs zero new
catalog bytes. Dependency lists and namespace-to-segment validation use retained
immutable chunks and validated change sets. Full scans remain required at
activation and recovery, rather than in every closed admission phase.

Let D be new metadata dependency bytes, C changed catalog chunks and root bytes,
K new key-record bytes, I durable intent bytes, and B dependency-root, commit and
retirement or anchor bytes for one changed generation. Per replica a commit
writes `D + C + K + I + B` logical bytes. At a 60-second active interval that is
`1440 * (D + C + K + I + B)` bytes per day if each interval has that same churn.
For example, 1 MiB of new SSTs, 64 KiB of changed catalog, zero key and intent bytes
and 16 KiB of record overhead cost 1,130,496 bytes per commit and 1,627,914,240
bytes per day per replica. This is arithmetic, not a workload estimate.
An idle day writes zero new generation bytes. Filesystem amplification and live
authority writes are additional costs to measure.

With pre-existing synced directories and N newly published immutable files,
file and parent-directory durability cost `2 * N` fsyncs per replica. N includes
new chunks, keys, dependency roots, commit and retirement or anchor files.
New ancestry, intent publications and deletes add separately reported syncs.
Live seal, flush and checkpoint writes occur on the authority only. They are
not multiplied across replicas. The authority replica may hardlink immutable
SSTs on the same filesystem or use a verified reflink, then sync the durable
name and required inode state. Never hardlink mutable files or bypass retention.
HDD fsync latency and actual byte and write-operation counts remain unmeasured.

Retain two selectable generations, one candidate and any explicit reader pins.
Charge the union of their metadata and physical payload dependencies once per
capacity domain, plus relocation copies, unresolved upload and cleanup debts,
intents, scratch headroom and free reserve. An immutable object is live if the
union of retained dependency lists, candidate lists or reader pins marks it.
Store no mutable reference counts. Rebuild marks after restart and make sweep
idempotent under the owner. Unknown or partially retired state conservatively
retains bytes. Budget exhaustion refuses new allocations, never evicts a needed
recovery point or disables GC for the entire pool.

Each commit has its own HMAC and predecessor digest. A compact authenticated
anchor names the oldest retained sequence, predecessor digest and retained
successors. Publish that anchor and retirement batch everywhere before deleting
older record files. Recovery validates the retained chain from that anchor.
Compact completed intents only after a committed catalog/debt root and anchor
cover them. Retire obsolete catalog chunks, allocation intents, commit and
retirement files in the same sweep. With no readers and settled cleanup, record
count and bytes stabilize at two generations plus one compact anchor and bounded
current intents. Unresolved debts remain separately charged. Refuse growth past
the configured record-byte budget. The format does not require the full history
back to epoch creation.

## Proposed on-disk formats

Each replica owns `.zerofs-survival/v1/`. Paths inside records are normalized
relative paths. Reject absolute paths, parent components, duplicate keys,
unknown required fields, oversized records and unknown schema versions.
No record stores a raw DEK or password.

| File | Proposed content |
| --- | --- |
| `writer.lock` | A stable local exclusive lock held for the writer lifetime. Recovery locks existing files without creating source files. |
| `identity.json` | The pool UUID, format, replica and hardware bindings, membership digest and epoch, original authority/catalog bindings and durable enablement. It includes enough bounded bootstrap information to recover from a replica plus the password alone. |
| `keys/<epoch>.json` | The exact original envelope bytes, their SHA-256, envelope version, KDF algorithm and parameters, DEK identity digest, pool binding and key epoch. The original envelope includes name mode. |
| `catalog/objects/<sha256>` | Immutable sorted table chunks and an authenticated logical catalog root covering all eight tables, original identity, schema, generation, counters and debts. It contains no raw copy of the live redb file. |
| `dependencies/<generation>.json` | The named checkpoint and manifest, immutable metadata dependency chunks with logical names, sizes and hashes, catalog root, bucket identity and key record. It also names exact physical payload generations, owners, lengths and checksums. |
| `objects/<sha256>` | Immutable metadata bytes copied at commit time. Live authority files remain in their existing layout. |
| `intents/<epoch>/<operation>.json` | Authenticated allocations, checkpoint intents and deletion permissions. A checkpoint intent stores `checkpoint_name` as `zerofs-survival/<epoch>/<sequence>` and its finite expiry before checkpoint creation. An allocation intent reaches every replica before physical writes. |
| `commits/<epoch>/<sequence>.json` | The pool, membership and writer epochs, sequence, predecessor digest, mutation and object cutoffs, checkpoint/manifest, dependency and catalog roots, and key epoch and digest, followed by its MAC field. |
| `retirements/<epoch>/<sequence>.json` | An authenticated retirement batch with retired digests, checkpoint names, retained successors and the epoch. All replicas persist it before sweep. |
| `anchor.json` | An authenticated compact chain floor and retained successors. Replace it durably before pruning covered historical records and completed intents. |
| `terminal/<epoch>.json` | An authenticated offline disable record covering all commits in that epoch. It overrides those records during recovery and prevents stale re-enablement. |

Use canonical UTF-8 JSON with sorted object keys, decimal integers, no floats,
no duplicates or insignificant whitespace, and lowercase hex bytes. Compute the
HMAC over domain-separated canonical bytes excluding the MAC field, then add
that field. The one receipt and predecessor digest definition is
`SHA-256(the complete canonical record file bytes, including the MAC field)`.
The MAC never contains itself. Derive its key from the recovered DEK with HKDF,
the pool UUID and `zerofs-metadata-survival-v1`. Pin golden vectors before any
persistent implementation. The acceptance record check hashes those full bytes.

Recovery bootstraps bounded envelope candidates and pool/membership identity
from surviving `identity.json` plus the password. A separately supplied expected
pool UUID is optional additional validation. Authenticate the identity and all
bindings after unwrapping, before accepting paths. Config and environment files
may be lost with the authority. The password and any underlying encrypted
filesystem unlock material must survive independently. No external freshness
service is required. Recovery reports that it cannot detect substitution of an
older copy of the same otherwise valid device without independent history.

## Exact survival commit ordering

The acceptance hook cuts before and after actual publications and syncs, with
the affected relative object path. Rename without parent sync is incomplete.

1. Hold the fixed membership locks and writer epoch. Validate every replica's
   identity, capacity and durable enablement before any I/O. Pre-drain with
   admission open. If bounded final work
   cannot be reached, report lag and retry later with admission still open.
2. Close admission, drain existing tokens and pause maintenance without holding
   inode, pool, catalog or database locks. Take the database barrier only after
   workers settle. Seal and flush, capture the final object cutoff and wait for
   backend coverage without holding any pool lock or catalog transaction.
3. Publish and sync a checkpoint intent before calling SlateDB. It names the
   deterministic checkpoint and finite lifetime exceeding the commit deadline
   and settlement margin. Create that named `Durable` checkpoint, obtain the
   manifest ID and drain its authority writes. Enumerate its immutable graph,
   reusing verified unchanged dependencies. Checkpoint publication cannot leave
   an unidentifiable permanent pin after a crash.
4. Export the fixed catalog in one redb read transaction. Validate changed
   namespace segment references against it and preserve verified unchanged
   chunks. Advisory read and monitor writes remain deferred. Include all table
   bytes, health, faults, hidden entries, identities, counters and real debts.
   Pin the candidate's dependencies in the in-memory ledger, then reopen
   admission. The final pause ends here, before any replica I/O.
5. In stable replica-ID order, publish and sync all new key, catalog and metadata
   dependency bytes and lists. Verify lengths and hashes on every replica.
   Make the candidate's ledger protection durable before any record. No record may be
   published until all replicas have every metadata dependency. Payload stays
   on its assigned member and is pinned there by exact physical generation.
6. Publish the identical authenticated commit record in replica order, with file
   and parent-directory sync. Retry the same bytes idempotently. Conflicting
   same-sequence bytes refuse publication. Once any durable record exists, do
   not abandon the candidate or reuse its sequence for another record.
7. After every configured replica holds the durable record, return the receipt
   and advance success status. Lost replies may expose this complete generation.
   A caller cancellation cannot cancel the owner's task or release exclusion.
8. Transfer the export pin to immutable ledger protection, delete the owned live
   checkpoint with `Admin::delete_checkpoint` and drain that authority update.
   Retirement can then mark old generations with no readers,
   publish its batch and compact anchor everywhere, and sweep unmarked bytes.
   Keep physical capacity debt until a later committed export covers completion.

### Abort, restart and retirement

Before any record publication, a deadline abort records abandonment and candidate
debts, releases its owned pin and settles I/O. Admission is already open, or
reopens if the abort happened in the final pause. A timeout alone
does not make a replica stale. After partial durable fanout, startup locks all
replicas and reconciles before opening admission. Find the highest unique
candidate present anywhere, validate every dependency everywhere, and complete
the identical record on missing replicas. Never build another record at that
sequence. A second restart repeats the same operation. Complete partial
retirement and terminal fanouts similarly. Identity conflicts, missing devices
or unverifiable dependencies require offline action, not a restart loop.

Reconcile checkpoint intents by deterministic name, including creation followed
by a crash before its returned UUID was recorded. Delete only owned checkpoints.
A finite lifetime lets an orphan expire even if reconciliation cannot run.
The current GC horizon waits out finite checkpoints; a permanent one pauses GC
indefinitely. Every successful or aborted commit must leave zero owned live
export pins after settlement. Never retain the newest generation through a
permanent live SlateDB checkpoint.

Reclaim, repack, relocation, garbage cleanup, orphan sweep and every authority
delete, rename or overwrite consult the ledger even when configuration is
missing. A generation pins the exact old physical segment needed to read it,
including after its live reference count reaches zero. A data member may be
removed only after every selectable generation and reader stops referencing it
and the membership transition is durable. A zero live relocation count alone
is insufficient. Sweep deletes an unmarked dead control object in the same
pass that preserves a retained object. Disabling all reclaim cannot satisfy
this contract. Crashes at each retirement/sweep boundary must preserve the union
of retained lists without counter underflow or double decrements.

## Recovery selection and writable promotion

Offline `recover` takes no receipt hint. It locks existing surviving lock files,
reads sources without modification and validates identity, terminal records,
anchors, HMACs and chain relationships. Select the highest valid chained commit
in the surviving epoch. Reject conflicting forks and corrupt authenticated
latest records or dependencies rather than silently rolling back. A complete
candidate present on one survivor may recover before acknowledgement. If the
commit task returned success, or every replica durably recorded the candidate,
recovery must return that candidate even during later cleanup. Rollback to its
predecessor is a failed acceptance result. If only some records existed and the
only holder was lost, the preceding acknowledged generation remains legal.

Materialize only the selected immutable dependency map into explicitly selected
owned scratch on a healthy device. Preflight space for the selected unique
metadata bytes plus imported redb size and configured filesystem headroom. Do
not count single-copy payload as scratch. Report the exact estimate and refuse
insufficient space before copying. Preserve authenticated original paths as
history and use a versioned binding override for the new scratch paths. Open a
pinned checkpoint reader with the existing recovered key. Never run mutating
pool startup, key initialization, writeback replay or cleanup on source replicas.

Recovery compares the namespace, catalog identity and all table bytes, debts,
charges, key envelope and epoch. Report current sole-copy lost ranges by inode,
all names, offsets and lengths. Reads of those ranges return EIO. Verify hashes
for unaffected ranges, empty files and holes. Later allocation intents are
separate orphan debts and never become live mappings in read-only recovery.

An offline `promote` is required before shipping. With all writers stopped,
select and verify a recovery generation, build a new live authority and catalog
on a replacement device and preserve pool identity through the explicit binding
override. Verify the copied namespace and envelope, import exact debts, exclude
lost payload members from allocation and create a fresh journal incarnation.
Provision and verify an independent new replica set, issue its initial commit
in a new membership/writer epoch, then permit read-write service. Promotion
returns a report naming source and new generations and exact missing ranges.
A create, write, durability barrier and cold reopen must work on an unaffected
file. Keep old evidence unchanged. Read-only recovery remains available before
promotion. Automatic election, degraded survival commits and payload repair
remain outside this design.

## Acceptance contracts and evidence limits

`zerofs/src/metadata_survival_tests.rs` keeps the ordinary cold-open and journal
controls. They prove today's backend behavior without survival receipts.
`metadata_survival_tests/acceptance.rs` and its review cases define ignored
contracts that require actual production bindings. Every row runs its oracle
against both success-only and optimistic stub observations in the ordinary
mutation test. A parser accepting the table or a driver claiming success cannot
supply replica files, a cold namespace, an observed API error or a completed
sweep.

A second ordinary test, `every_acceptance_row_fails_for_its_stated_reason`,
runs every row's real driver path against the current implementation, four
rows at a time with a 120-second budget per row. Each row must fail, and the
root cause must be exactly the `Missing` prerequisite the row states. A pass, a
panic, a timeout or any other error fails the test. One macro defines the row
enum, `Row::ALL`, each stated reason and each ignored test, so a new row is
covered by both ordinary tests without editing them. The check reaches each
row's first missing prerequisite and nothing after it. The interrupted-commit
row requests all 304 cuts, but each stops at the absent cut hook on one reused
fixture, so no cut runs. Its 5,400-second budget applies only once that hook
exists, and this test then fails until the row's stated reason changes.

Every acknowledged receipt requires the record on every configured replica
before loss. A missing directory cannot shrink that set. An interrupted
unacknowledged candidate may exist on one survivor, but every surviving copy
must match. Recovery reads use the hashes and placements captured for the
generation selected by its recovered digest. Two focused ordinary oracle tests
reject missing required replicas, conflicting copies and unknown generations.
These tests do not establish production recovery or file-sync durability.

The interrupted fixture attaches a real nonempty writeback journal, prepares
three distinct committed generations and cleanup debt, and only then starts its
candidate. Its hook records actual paths and all-replica durable record events.
It observes receipt completion separately from post-receipt cleanup. Each cut
uses fresh state. Deleting a nonexistent device fails. Dependency events repeat
for each actual object and sync. Channel tests prove hook behavior only, never
production crash safety.

The admitted-write case starts an admitted blocked write, then holds a commit
at the drain boundary. It proves that drain waits, checks a bounded late arrival,
and loses a device while both are pending. An independent inode-lock schedule
checks admission-before-lock ordering. Fault rows time the actual commit, attempt
a real create and read status before and after it. The slow row verifies online
retry and bounds timeout settlement. Full, stale and I/O failures require
specific errors, failed replica identity and unchanged success sequence.

The metadata replica loss row removes the disk that holds the second metadata
copy and a sole-copy payload range while the writer is open. A commit attempt
and a writable restart must each fail with `Lost`. Status must show admission
closed, name `disk-a-metadata` and keep the last successful sequence, digest and
completion time. The lost range must return EIO, the other range must read
correctly and a create must fail with EROFS. The failed commit must leave the
surviving metadata copy unchanged.

The rows cover authority, journal and combined data/replica loss, exact catalog
and key recovery, retained payload with an unretained dead control, interrupted
commits, admitted work, replica faults, writer locks and offline replacement.
Additional rows cover missing or mismatched configuration, terminal disable and
re-enablement, stale startup, corrupt dependency and record bytes, highest-chain
selection with two valid generations, conflicting forks, partial-fanout restart,
a backlog exceeding the deadline, deferred read faults while closed and
read-only, idle skipping, record bounds and zero owned pins, lag status,
authority promotion, orphan checkpoint recovery, crash-safe shared retention
and live loss of the metadata replica disk.
Fresh activation checks compare the membership epoch in each commit path and
require canonical new replica roots to be disjoint from the old roots and one
another. Advancing the sequence within a disabled epoch cannot satisfy them.
Promotion also verifies that the new replica identities survive a cold restart.
No case from the independent review is deferred without an acceptance row.

Run formatting and the two CI clippy commands on the host through
`with-build-slot`. Run the ordinary module, each ignored row explicitly and the
stub mutation test only in Linux containers using the shared target volume.
Retain exact commands, row failure lines and cleanup inventory in the report.
Ignored failures describe missing mechanisms and are not survival evidence.
Physical power loss, real hardware independence, NAS behavior and performance
require separate deployment tests. No NAS access or benchmarks belong here.

### Named gap: a writeback journal that lost acknowledged writes

The evidence is the storage-union fault lane's
`test_34_observed_drop_writes_journal_data_only` in
`evaluation/faults/vm/test_faults.py` on the `feat/fault-injection` branch of
ScriptedAlchemy/truenas-storage-union. Its transcripts are
`docs/evidence/faults-vm-69fd8259.txt` and `docs/evidence/faults-vm-f6b38100.txt`.
The test puts the writeback journal on its own device and makes that device
acknowledge writes and drop them. Three files are written and fsynced while the
drain to a member is held, and then the server is killed.

With writeback enabled, every object the server writes, metadata store objects
and pool payload alike, is acknowledged once it is durable in the journal and
reaches the backend later. A journal that lost writes reopens at an older redb
commit. The metadata store, the pool catalog and the members hold only what was
drained, which is older still. Every record the server keeps therefore agrees
with the older journal, and startup cannot tell that anything was acknowledged
after it. Nothing outside the journal device records the journal's position.

ZeroFS refuses the one shape that proves the loss by itself. A redb commit that
trims surplus space makes its new header durable and then truncates the file.
When that commit falls inside the lost window, the header write is lost but the
truncation reaches the device as file metadata. The file then ends before the
end its surviving header describes, which a crash on an honest device never
leaves. Both journal opens refuse that file with an error naming it and both
lengths (`a_journal_database_shorter_than_its_last_commit_is_refused_with_its_path`
in `zerofs/src/writeback/journal.rs`). Whether a trim falls inside the window is
a matter of timing. That is why the lane's run at 69fd8259 stopped at startup,
where redb's own assertion panicked, and its run at f6b38100 served the older
state with the three files missing and `status.unreadable` empty. The server's
journal open is the same at both commits. A userspace model of the test keeps
each journal file's bytes from when the window opened at its size after the
kill. Across 16 fault images started by both commits, the 3 with a trim in the
window panicked at both and the other 13 started at both with the three files
missing. At the commit that adds the refusal, all 8 trimmed images of 18 were
refused, and each started after its journal directory was moved aside, with
the drained file intact and the three files missing. The other 10 started
silently. The model script is kept with the fault-fix logs, not in the
repository.

Closing the gap needs a journal high-water mark that lives on a device
independent of the journal.

- The value is the journal incarnation and the highest local sequence whose
  acknowledgement has been returned.
- It must be durable on the independent device before an acknowledgement that
  depends on the journal returns. One write can cover every acknowledgement of
  one journal commit. A mark written on a timer would leave everything since
  the last mark undetectable.
- Startup compares it after the journal opens and before the overlay serves a
  read. A journal with the same incarnation and a lower local sequence is
  behind. Startup then refuses with an error naming the journal directory and
  the recorded and found sequences. An explicit operator override accepts the
  rollback and reports the lost range in status. A different incarnation means
  the journal was replaced and needs the same refusal.
- The pool catalog on the metadata device is a candidate home, because the
  server already writes it durably and it does not live on the journal device.

The blob files that a lost window leaves in the journal directory are not a
substitute. Their names survive and their contents read as zeros, but a crash
between a blob write and its journal commit also leaves blob files the journal
does not reference, so their presence proves nothing.

- Not verified: the latency of one more durable write on a second device per
  journal commit, which is the cost the journal exists to avoid.
- Not verified: the refusal on the lane itself. It was exercised with a
  constructed file and with the userspace model, not with dm-flakey.

## Changes after independent review

| Finding | Decision and section |
| --- | --- |
| F1 | Accepted. [Configuration and durable enablement](#configuration-and-durable-enablement) prevents silent disable and terminal-record rollback. |
| F2 | Accepted. [Operating contract](#operating-contract-awaiting-operator-confirmation) and [Abort, restart and retirement](#abort-restart-and-retirement) use pre-drain, bounded timeout, admission reopening and online retry before publication. |
| F3 | Accepted. [Recovery selection and writable promotion](#recovery-selection-and-writable-promotion) removes receipt hints and forbids acknowledged rollback. [Acceptance contracts](#acceptance-contracts-and-evidence-limits) require two valid generations. |
| F4 | Accepted. [Exact survival commit ordering](#exact-survival-commit-ordering) records a named finite checkpoint intent before creation. [Abort, restart and retirement](#abort-restart-and-retirement) reconciles it. |
| F5 | Accepted. [Chosen mechanism](#chosen-mechanism) uses commit-time copies and only retention in the live authority wrapper. |
| F6 | Accepted. [Space, writes and record bounds](#space-writes-and-record-bounds) specifies reused catalog chunks, adaptive idle skipping, byte arithmetic and authenticated record compaction. |
| F7 | Accepted. [Admission, locks and read observations](#admission-locks-and-read-observations) defers every advisory read-path catalog write. |
| F8 | Accepted. [Admission, locks and read observations](#admission-locks-and-read-observations) fixes token-before-lock order. [Acceptance contracts](#acceptance-contracts-and-evidence-limits) require a contended inode schedule. |
| F9 | Accepted. [Operating contract](#operating-contract-awaiting-operator-confirmation) keeps unaffected files writable after payload-only loss. [Recovery selection and writable promotion](#recovery-selection-and-writable-promotion) specifies offline promotion, scratch sizing and bootstrap without lost config. |
| F10 | Accepted. [Acceptance contracts](#acceptance-contracts-and-evidence-limits) require a real nonempty journal and refuse deletion of nonexistent devices. |
| F11 | Accepted. [Acceptance contracts](#acceptance-contracts-and-evidence-limits) prepares three distinct commits and real cleanup debt before retirement cuts. |
| F12 | Accepted. [Acceptance contracts](#acceptance-contracts-and-evidence-limits) overlaps an admitted write, a draining commit and a late arrival. |
| F13 | Accepted. [Acceptance contracts](#acceptance-contracts-and-evidence-limits) observes actual API results, elapsed time and status for replica faults. |
| F14 | Accepted. [Abort, restart and retirement](#abort-restart-and-retirement) and the payload oracle require a dead unretained control to disappear in the same sweep. |
| F15 | Accepted. [Proposed on-disk formats](#proposed-on-disk-formats) defines one full-record receipt and predecessor digest, including the MAC field. |
| F16 | Accepted. [Abort, restart and retirement](#abort-restart-and-retirement) completes the same partial fanout before admission and sequence reuse. |
| F17 | Accepted as a design ambiguity, not proof of implemented counters. [Space, writes and record bounds](#space-writes-and-record-bounds) derives liveness from retained lists and stores no reference counts. |

## Unverified assumptions

- Not verified: All mutators obey the specified admission and lock order, and
  pre-drain and final quiescence can meet the configured bounds without deadlock.
- Not verified: The pinned SlateDB graph API covers every metadata dependency.
- Not verified: Replica publication, authentication, compaction, terminal records,
  checkpoint reconciliation, recovery and promotion have production bindings.
- Not verified: Catalog path rebinding preserves existing wrong-pool protection.
- Not verified: This branch predates the integrated name-mode envelope schema.
  Verify the merged schema and refuse an unknown required mode before implementation.
- Not verified: Hardware identities, filesystem unlock material, write cost and
  capacity budgets meet deployment requirements.
- Not verified: The HDD-dependent budgets in the write-wait section fit real
  HDDs. They stay unverified until the HDD measurement runs on the NAS.
- Not verified: Reopening admission before replica publication is safe in every
  mutator path. It depends on the ledger protecting the candidate from the
  writes that follow the pause.
- Not verified: The Linux v9fs client and the FUSE kernel module pass EAGAIN to
  the program unchanged. This follows from the ZeroFS sources cited above. No
  mounted test checked it.

The coordinator selected a fixed all-replica single-host commit. HA, password
changes, automatic membership changes, payload redundancy, ACLs, extended
attributes and strict placement enablement remain outside this stage.
