# Metadata survival: a second copy of the metadata on every replica

This document describes what the code delivers. The accepted mechanism is in
[the design](metadata-survival-design.md).

One device holds the live namespace database (SlateDB), the placement catalog
and the key envelope `zerofs.key`. Survival copies a consistent recovery
generation of all three to every replica. Losing the authority device then
loses the writes made after the newest generation, but not the volume.

Code: `zerofs/src/metadata_survival/` holds the commit owner, the record
format, the backend contract, the admission gate and recovery.
`zerofs/src/pool_object_store/survival.rs` and
`zerofs/src/pool_object_store/catalog.rs` hold the catalog export and import.
`zerofs/src/cli/survival.rs` is the command.

## Two topologies

**Raw devices.** The authority is a raw P3 (`raw+p3-*` `[storage].url`): its
journal, catalog region, authority log and key-envelope region. Every pool
member is a raw `RawStore` object log. Each member holds the generations in
its own log, under the reserved key namespace `.zerofs-survival/v1/`. Payload
keys all start with `.zerofs-pool/`, so the two never collide. The replica
set is every pool member and is not configurable.

**Directories.** The authority is a `file://` directory. Replicas are
configured directories on other devices, and the authority directory holds
one more copy.

Survival code reaches a replica only through the `ReplicaBackend` contract in
`metadata_survival/backend.rs`. `MemberRegion` implements it over a raw
member's object store. `LocalDirectoryBackend` implements it over a directory
with temporary files, rename, directory fsync and flock.

On a raw member, one PUT publishes an object: `RawMemberStore` appends the
record and flushes both label slots before it returns. The pool's exclusive
device claim keeps every other process out. The region has no size of its
own. It shares the member's log. Every PUT is admitted through the pool's
space ledger first, so it never takes room promised to segment data or the
member's reserve. A PUT that does not fit is refused as `Full` and writes
nothing. See "When a member fills" in [storage pools](storage-pools.md).

## Configuration

Raw devices:

```toml
[storage.pool.metadata_survival]
format_version = 1
commit_interval_seconds = 60
```

Directories:

```toml
[storage.pool.metadata_survival]
format_version = 1
authority_replica_id = "authority"
authority_device_id = "authority-nvme"

[[storage.pool.metadata_survival.replicas]]
id = "disk-a-metadata"
device_id = "disk-a"
dir = "/mnt/disk-a/metadata-replica"
```

| Key | Default | Meaning |
| --- | --- | --- |
| `commit_interval_seconds` | 60 | Seconds between generations. |
| `final_pause_max_milliseconds` | 1000 | How long a cut may keep mutations waiting for admitted work and the backend cutoff. Past it the cut is deferred to the next attempt and admission reopens. A cut never fails a mutation. |
| `final_residual_max_bytes` | 16777216 | Undrained journal bytes above which a cut is postponed. |
| `predrain_max_seconds` | 120 | How long a cut waits for the journal to drain before it is postponed. |
| `replica_timeout_seconds` | 90 | Deadline for copying a generation to the replicas. |
| `commit_deadline_seconds` | 180 | Deadline for the final shutdown commit after its pre-drain. At least `replica_timeout_seconds`. |
| `checkpoint_lifetime_seconds` | 600 | Lifetime of the database checkpoint a generation pins. |
| `headroom_bytes` | 4294967296 | Raw devices only. Room on each member above its reserve that only generations may use. |

Validation refuses a raw pool that lists replicas or an authority replica, or
whose reserve plus `headroom_bytes` does not fit within a member's capacity. It
refuses a directory pool with no replica, duplicate ids or device ids,
relative paths, and a replica directory that overlaps the authority, the
catalog, a member or another replica. A separate `[wal]` store is refused.

## What one generation holds

Every replica holds the same objects under its region root:

| Key | Content |
| --- | --- |
| `identity.json` | Pool, replica and device binding, membership, and the bootstrap needed to recover without the configuration. |
| `keys/<epoch>.json` | The exact, still-encrypted key envelope bytes and their digest. |
| `objects/<sha256>` | Authority objects, named by content. A later generation reuses every object that did not change. |
| `catalog/objects/<sha256>` | The logical export of all nine catalog tables from one snapshot. |
| `dependencies/<seq>.json` | The object list of generation `<seq>` with lengths and digests. |
| `retention/<seq>.json` | The payload objects generation `<seq>` maps, which the pool must keep. |
| `commits/<epoch>/<seq>.json` | The commit record. Generation `<seq>` is valid on a replica only once this object is durable there. |

Records are canonical JSON sealed with an HMAC. The key is derived from the
data key, so only the password holder can forge or read-verify them.

## What a commit does

1. Validate every replica. A missing device is `Lost`. A missing or changed
   acknowledged record is `Stale` or `Corrupt`.
2. Drain the writeback journal to the members with mutations still admitted.
   If more than `final_residual_max_bytes` are still undrained after
   `predrain_max_seconds`, postpone the cut. Mutations are never stalled
   behind a backlog.
3. Close mutation admission. Wait for admitted mutations, drain the residual,
   create a named SlateDB checkpoint and export the catalog under the
   database flush barrier. Reopen admission.
4. With admission open, copy every authority object, the catalog export, the
   dependency list and the retention record to every replica.
5. Publish the identical commit record to every replica. The commit is
   acknowledged only after every replica holds it.
6. Delete the checkpoint, retire generations beyond the newest two, and
   delete the objects no retained generation references.

The previous generation therefore stays valid until the next one is valid on
every replica.

A commit also runs once at clean shutdown, after serving stops and before the
database closes.

A replica that is full, stale, corrupt or failing latches mutation admission:
mutations fail with EIO until an operator repairs the replica set. A lost
replica latches EROFS. A postponed cut and a replica timeout before any
record do not latch. The next interval retries.

On a raw P3 authority, every member keeps `headroom_bytes` free above its
reserve for generations, 4 GiB (4,294,967,296 bytes) unless configured.
Ordinary WRITEs, segment leases and catalog reservations may not use it, so a
pool full of data answers WRITE with ENOSPC and the next generation still
commits. A replica is full only once the generations outgrow the headroom.
The reserve plus the headroom must fit within every member's capacity.
`headroom_bytes` is refused on a directory authority, whose replicas are not
pool members. `pool status` reports each member's
`space.metadata_headroom_used_bytes` and
`space.metadata_headroom_remaining_bytes`.

Password changes are refused while survival is enabled. Removing the
configuration table does not disable survival: a writer refuses to start when
a replica records enablement and the table is missing.

The pool keeps every payload object that a retained generation maps, charged
as cleanup debt, until a newer generation retires it. The pins are loaded
from the replicas at pool open, before any cleanup runs.

## What is lost with the authority device

Recovery returns the pool to the newest generation valid on a surviving
replica. Every write acknowledged after that generation's cut is lost,
including writes that completed an fsync, because fsync makes a write durable
on the authority device's journal.

- With cuts succeeding, the loss window is at most `commit_interval_seconds`
  plus the duration of one commit.
- While cuts are postponed, the window grows. Cuts are postponed while writes
  arrive faster than the members drain them.
- A clean shutdown leaves no window.

`lag_seconds` in the status is the current window.

## Status

A running server reports the copy in `zerofs pool status`, under
`metadata_survival`:

- `last_sequence`, `completed_at` and `lag_seconds` for the newest generation
  valid on every replica
- per replica: `last_sequence`, `generation_age_seconds`, `region_bytes` and
  `error`
- `last_commit_millis` and `last_pause_millis`, the duration of the last
  commit and of its closed-admission pause
- `admission`, `failed_replicas`, `attempts` and `last_error`

`zerofs survival status -c config.toml` reads the replicas directly and never
writes. On a raw pool it claims the member devices, so stop the server first.
It does not need the authority device.

## Recovery

Recovery runs only when you ask for it. Ordinary startup refuses a blank or
partial authority and never recovers on its own.

Recovery reads the sources and never modifies them. It authenticates every
record, refuses conflicting records at one sequence and a broken predecessor
chain, selects the newest generation, and verifies the digest of every object
on one replica before it writes anything. If the newest generation fails
verification on every replica, recovery fails. It never rolls back to an
older generation on its own. A generation without a commit record is not a
generation: recovery ignores it and selects the previous one.

### Rebuild a lost P3

1. Provision a blank replacement P3 for the same pool UUID.
2. Point `[storage].url` at the replacement. Keep the member URLs and the
   password.
3. Run the rebuild:

    ```sh
    zerofs survival recover --config config.toml
    ```

4. Start the server as usual.

The command claims every member, selects and verifies the generation, and
then claims the P3. The P3 must hold nothing in any of its six roles. The
command writes the authority objects, the original key envelope, the catalog
rebound to the new device and an empty writeback journal. It opens the result
read-only to prove it, and writes the pool identity marker last. An
interrupted rebuild therefore leaves a P3 that both startup and a second
rebuild refuse. Provision it again and rerun.

The rebuilt database opens at exactly the generation's checkpoint. Manifests
newer than the checkpoint and compactor state are not restored.

If a member is also lost, the command fails and names it. To proceed without
it:

```sh
zerofs survival recover --config config.toml --allow-missing-member hdd18
```

The JSON report then lists `unavailable_members`, every lost range by inode,
names, offset, length and member, and `unreadable_files`, the names of every
file with a lost range.

### Read a directory pool's generation

```sh
echo "$PASSWORD" | zerofs survival recover \
  --replica /mnt/disk-a/metadata-replica \
  --scratch /mnt/healthy/recovery-scratch [--keep]
```

This form needs only surviving replica directories and the password. It
materializes the generation into owned scratch and opens it read-only.
Mutations fail with EROFS.

## Tests

`zerofs/src/cli/metadata_survival_raw_tests.rs` drives the raw topology
through the real startup, commit and recovery paths. Its three devices are
private regular files, which test builds accept in place of loop devices.
`zerofs/src/metadata_survival_tests/` holds the acceptance contract for the
mechanism. Its rows run on the directory backend.

## Known limits

- **No hardware evidence.** No test here runs on a physical device, an HDD or
  across a real power cut.
- **Log space.** A raw member's log is append-only. Every commit appends a
  dependency list, a retention record and a commit record, and deleting a
  retired object appends a tombstone. The space returns only when the engine
  reclaims log space.
- **Orphaned payload.** Payload written after the recovered generation stays
  on the members after a rebuild, unreferenced.
- **No online repair.** A commit skips an object a replica already holds at
  the right length. A damaged copy is found at recovery, which then reads the
  other replica. Nothing repairs it online.
- **Degraded start after a rebuild.** A pool rebuilt without a member cannot
  start: `allow_degraded_start` covers directory members only.
- **Dependency enumeration.** SlateDB exposes no way to list a checkpoint's
  files. A commit copies every authority object, deduplicated by digest.
- **Deferred design items.** Idle-commit skipping, terminal offline disable,
  writable promotion of a directory replica, and a command for offline
  catch-up of a replaced directory replica are not implemented. The review
  rows that exercise them still fail at their stated prerequisites.
