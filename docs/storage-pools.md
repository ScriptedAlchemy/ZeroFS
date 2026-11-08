# Local disks and permanent storage pools

ZeroFS can use a mounted local directory as its backing store, or combine local
directories and remote object stores into one filesystem. A storage pool holds
permanent file data. It is independent of the clean read cache and the optional
writeback journal.

## Native local storage

Set `[storage].url` to a local directory:

```toml
[storage]
url = "file:///srv/zerofs/volume"
encryption_password = "${ZEROFS_PASSWORD}"
```

The native backend implements conditional updates with OS file locks shared by
independent processes. All mutations participate in that coordination; atomic
publication, ETags, and the existing raw object layout remain compatible with
the underlying local backend. File publication and directory syncing provide
the existing Unix durability semantics on Linux and macOS. Do not modify the
backing objects or remove the `.zerofs-local-mutations#0` lock files manually.

A local URL is relative to the machine running the server. It does not give a
remote ZeroFS server access to an HDD attached to a client. Existing ordinary
files on an HDD are not imported by pointing ZeroFS at their directory: copy
them through the mounted filesystem or the upload client.

## Create a pool

Use one local directory for authoritative filesystem metadata, a separate
persistent directory for the placement catalog, and one or more data members.
These directories and the clean cache/writeback directories must not overlap,
including through symlinks. Local metadata and member directories must already
exist on their mounted disks. The catalog is created only by explicit pool
initialization.

Example for a local HDD followed by S3 overflow (capacities are illustrative):

```toml
[cache]
dir = "/srv/zerofs/cache"
disk_size_gb = 10.0
memory_size_gb = 1.0

[storage]
url = "file:///srv/zerofs/metadata"
encryption_password = "${ZEROFS_PASSWORD}"

[storage.pool]
catalog_dir = "/srv/zerofs/catalog"

[[storage.pool.members]]
id = "hdd1"
url = "file:///mnt/hdd1/zerofs-data"
capacity_gb = 17000.0
reserve_free_gb = 10.0

[[storage.pool.members]]
id = "cloud"
url = "s3://my-bucket/zerofs-pool"
capacity_gb = 50000.0

[storage.pool.members.aws]
access_key_id = "${AWS_ACCESS_KEY_ID}"
secret_access_key = "${AWS_SECRET_ACCESS_KEY}"
default_region = "us-east-1"
# endpoint = "https://my-s3-compatible-service.example"

[servers.ninep]
addresses = ["127.0.0.1:5564"]

[servers.rpc]
addresses = ["127.0.0.1:7000"]

[telemetry]
enabled = false
```

Create the metadata, cache, and local member directories on the intended disks,
then initialize and start:

```bash
zerofs pool init --config zerofs.toml
zerofs run --config zerofs.toml
```

`pool init` is explicit so a missing catalog or detached metadata disk cannot
silently become an empty replacement pool. An existing authority marker is
bound to its catalog identity. Normal startup refuses a missing catalog, a
changed known member identity, and an existing pool opened without its pool
configuration. The catalog is also bound to its original resolved directory;
copying the database into a different directory cannot create a second owner of
the same member data.

Members are considered in configuration order. Each new immutable segment goes
to the first member whose configured quota and local free-space reserve allow
it. The quota counts committed data, pending uploads, and superseded data still
awaiting deletion. Adding another permanent member adds capacity without
changing filesystem paths or remapping previously written segments.

`capacity_gb` is a physical segment-storage budget in decimal GB. Metadata,
catalog space, provider overhead, and other files on the same disk need their
own headroom. Local `reserve_free_gb` defaults to 1 GB and is checked against
actual filesystem free space as well as the member quota. Keep a practical
reserve for both metadata and data. `[filesystem].max_size_gb` remains the
separate logical filesystem quota; `zerofs pool status` reports physical member
capacity and reservations. Compression means logical and physical usage differ.

The first version owns its local catalog exclusively. It supports one server,
including a read-only or checkpoint server when the writer is stopped, and
rejects HA replication. Adding members and maintenance require a server restart
or a stopped server. It does not perform background cold-data migration or
per-file pinning; explicit migration controls placement.

## SFTP members

The fork's SFTP backend can be a permanent member. Plain FTP and FTPS are not
supported. Use SSH key authentication and strict host-key verification:

```toml
[[storage.pool.members]]
id = "storagebox"
url = "sftp://u123456@u123456.your-storagebox.de:23/zerofs-pool"
capacity_gb = 2000.0

[storage.pool.members.sftp]
identity_file = "/etc/zerofs/storagebox.key"
known_hosts = "/etc/zerofs/known_hosts"
max_connections = 4
read_concurrency = 16
write_concurrency = 16
```

Per-member `[aws]`, `[azure]`, and `[gcp]` settings override matching global cloud
settings. Per-member `[sftp]` settings use their own defaults. Multiple SFTP
members using the same host, port, and account must stay within a combined
10-connection budget. Account aliases or other clients still consume the
provider's account-wide limit. Members can also configure their own supported
`storage_class`.

Idle SFTP members connect on first access, so an unreachable unused remote does
not prevent local metadata and local data from opening. Operations that need
remote-only data still need that member. New writes follow member priority and
quota; a selected member's I/O failure is surfaced, not silently interpreted as
empty data or permission to reassign an existing segment.

## Inspect and expand

Stop the server before catalog maintenance. The same exclusive lock prevents
maintenance from racing a running server.

```bash
zerofs pool status --config zerofs.toml
zerofs pool list --config zerofs.toml
```

To expand, create a new local member directory or choose a new remote prefix,
append a member with a new stable ID and its capacity, and restart with the
updated configuration. Existing members keep their IDs and locations. Reordering
members changes the preference for future allocations. Raising a member quota
allows more data there; it does not resize the underlying HDD or provider plan.
Removing or repointing a known member is rejected. Migrate data before any
physical replacement; automatic member removal is not part of this version.

Move existing segments to another configured member while the server is
stopped:

```bash
zerofs pool migrate --config zerofs.toml --from hdd1 --to cloud --limit 128
```

For one exact object, pass the segment object path returned by `pool list`:

```bash
zerofs pool migrate --config zerofs.toml \
  --segment 'srv/zerofs/metadata/segments/01/0000000000000001/0000000000000001' \
  --to cloud
```

The command copies and hashes the new body, commits the new placement durably,
then deletes the old body. A failed cleanup remains recorded and charged until
it succeeds. Logical object ETags and creation timestamps remain stable during
migration, including the timestamps used by checkpoint garbage collection.

The pool stores one physical object per segment to retain the existing S3/SFTP
I/O geometry. It does not split every segment into many remote objects.
Multipart admission reserves its declared length before accepting parts and
retains bounded zero-copy payload references until atomic publication. Segment
transfer and migration are bounded to 512 MiB; this is an internal object
limit, not a limit on user file size. Normal 256 MiB segments fit within it.

## Existing local volumes and recovery

An existing local ZeroFS volume can become the metadata authority of a pool.
Stop it, configure separate catalog and member directories, initialize the pool,
and restart. Existing segments in the original directory remain readable and
appear with the reserved member name `authority`. They are outside member
quotas until explicitly migrated. For example:

```bash
zerofs pool migrate --config zerofs.toml --from authority --to hdd1 --limit 128
```

Newly allocated pool bodies use unique keys beneath a pool-owned member prefix.
The placement catalog records reservations before writes, publishes mappings
only after verified storage, and retains cleanup debt after interruption.
Restart recovery reconciles recorded work; it does not scan unrelated member
contents or treat a missing mapped object as a new empty file. Member identity
markers prevent an initialized member from silently becoming a fresh empty
directory.

The metadata directory and `catalog_dir` are both authoritative data. A backup
must preserve both consistently, along with the configured encryption password
and the member objects they reference. Restore the catalog at its original
resolved directory; relocating it requires a future explicit relocation
operation. The catalog must never live in an
evictable cache or disposable temporary directory. Aggregate capacity is not
redundancy: a segment stored only on one HDD is lost if that HDD fails.

## Validation

Unit and integration tests cover local compare-and-swap across instances and
processes, cancellation ownership, quota reservation, restart, member identity,
corruption and cleanup failure, migration timestamps, and ordinary local backend
startup checks. `scripts/test-local-storage-pool.py` exercises initialization,
actual 9P uploads/downloads, local-member overflow, capacity expansion,
migration, cache removal, and a server restart against isolated temporary data.
The Rust CI workflow runs that application-level test against its built server.

## Physical disk health

Keep metadata and `catalog_dir` on dependable storage separate from data disks.
Give each physical disk one mounted directory and one pool member. ZeroFS uses
ordinary files on these mounts. It does not format disks, manage mounts, build
RAID, or reconstruct an unreadable single copy.

A Linux local member can opt into read-only ATA SMART and kernel monitoring with
`health_device = "/dev/disk/by-id/ata-EXAMPLE-part1"` in its
`[[storage.pool.members]]` entry. The device must be the block device backing the
mounted filesystem. Whole-disk filesystems use the whole-disk node. Duplicate
physical disk assignments, device-mapper devices, and ambiguous mappings are
rejected. Remote members and directory-only members remain supported without
claiming hardware monitoring coverage.

The server attempts an initial probe before opening listeners. It then samples
at 60-second intervals. `smartctl --json --all` runs without a shell, with a
10-second deadline and a 1 MiB output limit. The service account needs read
access to the configured device and `/dev/kmsg`. ZeroFS does not run `sudo`,
self-tests, repairs, or device-setting commands. Missing tools, unsupported ATA
attributes, access errors, and timeouts block monitored allocation and appear
as `monitoring_error`. Known failed-status or pending-sector evidence still
quarantines and starts evacuation when optional counters are missing. Missing
telemetry never becomes a synthetic zero or evidence that data was destroyed. Kernel
coverage reports restricted access, ring gaps, bounded backlogs, and the limit
to recognized messages with exact device tokens.

Positive pending or uncorrectable sectors and overall SMART failure quarantine
immediately. Reallocated-sector growth quarantines relative to the stored
baseline. The first distinct UDMA CRC increase warns. The second increase
quarantines, even if stable samples occurred between them. Replayed unchanged
samples do not count again. Quarantine survives restart and does not clear after
a good sample or after monitoring is disabled. A missing monitored device or
lost mount binding at startup quarantines that member and records unavailable
coverage while the remaining members continue to serve.

Quarantine commits in the same catalog authority used by reserve, upload start,
and publication. All three reject a quarantined destination, including explicit
migration targets and late multipart completion. Already submitted bytes may
finish as unpublished bodies. Their reservation remains charged until cleanup.
Device binding and member markers are rechecked on writes and before publication.

The running server automatically copies readable quarantined segments through
the existing verified migration path. One migration at a time shares the
512 MiB transfer bound with explicit migration. The destination is verified
before atomic durable remap, and source deletion follows remap. Source read
failures remain tied to their physical generation. Destination verification
errors do not label source data unreadable. Recovery continues to other segments
and retries with bounded polling. The runtime also retries existing deletion
and abandoned-upload debts, excluding uploads that still have a live owner.
Ordinary transient or invalid-range reads do not create durable data faults.
Recorded faults can clear only after the exact physical generation passes its
stored SHA-256 verification, or after verified migration replaces that generation.
Without sufficient healthy capacity, remaining
placements stay visible and completion stays false.

Inspect a running server through its existing admin RPC socket:

```sh
zerofs pool status --endpoint unix:/run/zerofs/admin.sock
zerofs pool affected --endpoint unix:/run/zerofs/admin.sock --member disk1
```

Offline `pool status --config zerofs.toml` still takes the catalog's exclusive
lock and requires a stopped server. Opening storage or running an offline command
does not start hardware probes or automatic migration. Read-only and checkpoint
servers expose status but do not run automatic evacuation.

`quarantined_leftovers` lists remaining source placements. `unreadable` lists
observed source failures with generation and reason. `readable_evacuated` can be
true when only these failures remain. That condition reports
`readable_evacuated_loss_blocked` and keeps the loss-free `safe_to_remove=false`.
`safe_to_pull_with_known_loss=true` explicitly identifies the brief's condition
that only listed unreadable leftovers remain. It additionally requires no
reservations, no old-body cleanup debt, and no active owned operation or read
lease. Pulling in this state abandons the only persistent copies of the listed
segments. It never means data was reconstructed.
`pending_uploads` and `cleanup_debts` count operations even when their byte
counts are zero. `safe_to_remove` requires zero source placements, zero used bytes including old
body cleanup debts, zero reservations, and no active owned operation or read
lease. Status is a snapshot. It does not
remove configuration, delete damaged files, or authorize a hot unplug.

Affected reports join `FrameLoc.segid` metadata to directory entries in retained
SlateDB snapshots, including every retained checkpoint. They never read failed
segment bodies. Paths are lossless absolute byte arrays and include hardlinks.
Unlinked or unreachable inodes retain inode and extent identities with no paths.
Segments with no discovered owner are listed as unresolved. Row and path limits,
metadata errors, cycles, and checkpoint-set changes produce explicit incomplete
reports. No empty result proves data is recoverable or backed up.

Old catalog record encodings and existing backend layouts are unchanged. New
health and fault state lives in separate versioned tables. Old binaries ignore
those tables, so downgrading a health-enabled pool to a pre-health binary is
unsupported and does not preserve quarantine enforcement.

The optional `disk-health-test-hooks` Cargo feature enables a temporary-file
observation source for the local integration harness. It is absent from ordinary
builds and exposes no fault RPC. Run its isolated test with
`scripts/test-local-storage-pool.py --health-recovery` against a binary built with
that feature. Do not use that build for production.
