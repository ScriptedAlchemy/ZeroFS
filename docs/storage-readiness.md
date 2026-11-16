# Declared filesystem profiles and startup readiness

`[storage.readiness]` adds opt-in identity verification before a local pool is
opened. Omitting the section preserves ordinary local and cloud deployments.
Setting `strict = false` skips host identity and filesystem-size observations.
Config loading still checks the declarations. Both strict modes check existing
symlinks on structural paths before storage opens and on requested object paths
before I/O. `strict` must be written explicitly when this section is present.

This completes the declarations and startup preflight for storage readiness.
The schema binds storage paths to filesystems and declares device roles, lifetime
host-write ratings, and capacity-domain reserves. Workload routing still requires
the storage-readiness changes to land before its implementation can proceed.

A successful preflight does not enable strict placement. Policy configuration,
segment routing, relocation,
retention, profile-specific I/O tuning, and metadata survival are not implemented
here. `[placement]` and `placement_enabled` are rejected as unknown fields.
Existing first-fit segment placement retains its existing semantics.

## Hardware declarations do not activate a route

Declaring `nvme` neither enables writeback nor establishes that a device is
suitable for it. The existing `[writeback] enabled` setting selects one global
journal path. Every enabled acknowledgement mode uses that journal, including
`remote`. Selective per-workload bypass is unavailable. Future route selection
must be durable before acknowledgement and remain valid through recovery.

Device profiles, boot flags, lifetime ratings, and reserve quantities are
operator declarations. Strict verification compares device serials and backing
device sets with those declarations. A boot flag remains declared intent, even
after the verifier excludes that declared serial from the storage roots.
Endurance, power-loss protection, sustained throughput, workload expectations,
and write budgets remain unavailable. Filesystem identity alone does not
establish those facts. No declaration in this change activates the Crucial P3
SSD. Its proposed role is optional clean read retention, pending resolution of
the read anomaly in old or untouched regions and measured workload benefit.
Cache fills, promotions, and rebuilds write bytes even though cache hits do not.

Budget pressure must refuse new work before acceptance. It must preserve and
drain already accepted dirty data under its original durability contract.
Direct-to-HDD routing is a deliberate policy choice, with backend publication
semantics. It is not a fallback from an unsatisfied journal policy.

## Declarations and role bindings

Declare each filesystem once with an ID, its canonical mountpoint, its stable
identity, and one operator-selected profile: `nvme`, `hdd-cmr`, or `hdd-smr`.
Profiles are never inferred from devices, model names, mount types, or timings.
The declared-profile and startup-readiness checks do not change scheduling or
promise SMR performance.

Bind authority, catalog, cache, and every pool member to those IDs. Bind `journal`
when writeback is enabled; omit it otherwise. Authority, catalog, and data members
must declare an HDD profile. Cache accepts any profile, and journal must declare
`nvme`. These are
validated declarations, not hardware detection. The strict graph supports local
`file://` pool roots only and rejects a separate WAL store rather than leaving a
storage role unchecked.

All active role directories must already exist, including catalog, cache, and
journal directories. Readiness creates nothing. A role may be a dedicated child
directory on its declared mounted filesystem. Role directories must be disjoint,
including after resolving symlinks. The Linux adapter rejects every mount
strictly below a declared role root. It also checks the containing filesystem
against the declaration. Before opening storage, preflight checks fixed
structural paths, including `.zerofs-pool`, the active pool directory, member
markers, object directory roots, catalog files, and the active bucket's cache
and journal directories. It also checks existing cache partitions before the
cache engine opens them. Their count is bounded by cache configuration.
Startup does not enumerate authority objects, member payloads, or journal blobs.
Object operations check each existing path component when they use it. Listings
reject symlinks while walking the requested subtree. Journal blob opens use
descriptor-anchored no-follow handles. The checks reject symlinks even when
their targets stay inside the declared root.
Missing children use their closest existing ancestor without creating anything.
Nested mounts cannot substitute for the declared mountpoint. Declared
mountpoints must themselves be canonical paths.

Authority, catalog, and one member may share a filesystem through disjoint
directories. Journal and cache may likewise share the NVMe filesystem. Multiple
data members cannot use the same capacity domain. For ext4/XFS, that domain is
the filesystem UUID; for ZFS, it is the pool GUID, even across distinct dataset
GUIDs. This conservatively rejects shared-pool member quotas that could
otherwise count pool free space twice. Aliased declarations and conflicting
profiles in one capacity domain are rejected. Config loading sums configured
member capacities per domain to reject impossible reserves. Storage readiness
adds no new admission accounting. Shared control/data space still needs the
existing reserves.

ZFS GUIDs are canonical nonzero decimal strings, preserving the full unsigned
64-bit range through TOML. UUIDs are nonzero lowercase hyphenated UUID strings.
Dataset names alone and directory existence alone cannot establish readiness.

The complete [synthetic configuration](examples/storage-readiness.toml) is parser
tested. Its paths, IDs, GUIDs, UUID, and serials are fixtures, not discovered operator values
or evidence of an installed disk. Use independently obtained real identities for
an actual deployment; do not substitute those fixture values.

## Device declarations and domain reserves

Each optional `[[storage.readiness.devices]]` entry names a serial, an absolute
whole-device path directly under `/dev/disk/by-id`, a profile, intended roles,
and an explicit `boot` boolean. Ordinal paths and partition by-id paths are
invalid. Serials must be nonempty without surrounding whitespace. Duplicate
serials and by-id paths are invalid. `roles = []` declares a device without a
storage role. A boot device cannot carry any storage role.

Roles use the existing bindings. Their names are `authority`, `catalog`, `cache`,
`journal`, and `member <id>`. A device cannot name an undeclared role. Its profile
must match the filesystem profiles for its roles, and a journal device must be
`nvme`. Each device must name every role sharing the capacity domains it serves.
The configuration derives those domains from the filesystem identities.

`rated_host_writes = { bytes = 220000000000000, source = "Synthetic vendor lifetime host-write rating" }`
declares a lifetime host-write rating in decimal bytes. The quantity parses as
`u64`, must be positive, and requires a nonempty source. An absent rating is
unavailable with a reason. It never becomes zero. An HDD annual workload rating
is not a lifetime host-write rating. Ratings do not establish device wear, a
failure date, a workload budget, or measured activity.

`[[storage.readiness.reserves]]` is required once per capacity domain. Each entry
names a declared `filesystem` ID and a `bytes` quantity parsed as `u64`. There is
no default. Multiple datasets in one ZFS pool require one entry for that pool,
referencing any one of its filesystem IDs. A second entry for the same domain is
invalid. Config loading rejects a quantity below any enforced `reserve_free_gb`
on that domain or below the journal's enforced `min_free_gb`. It also rejects a
reserve larger than the summed configured member capacity on that domain. Domains
without members have no configured capacity bound. Strict Linux verification
compares the domain reserve with the observed `statvfs` size at each role root,
including each bound ZFS dataset. A missing, zero, or overflowing size is
unavailable and refuses the open. The existing pool reserve default remains
1.0 GB. These existing settings remain the enforced
values. The new quantity is a declaration and does not reserve additional space.

Every declaration rule runs at config load in both strict modes. Unknown fields,
including `activate`, `enabled`, and `active`, are invalid in readiness tables.
No declaration activates placement. Configurations may omit device entries, but
then device identity and boot exclusion remain unavailable. Filesystem profiles
and domain reserves still require declarations.

Future reporting must keep declarations separate from observations. Rate math
must use checked `u128` arithmetic and checked conversions. Overflow and an
underflowing remainder must return unavailable. Intervals must retain
milliseconds without a 1 s floor. A short sample must remain a rate over its
interval, without an implicit daily projection. Time to a rated host-write
amount must be a `Duration`. JSON bytes must use decimal strings or integers
within the safe-integer range. This change implements no rate, horizon, SMART
parser, admin report, or UI.

## Host evidence and failures

The native adapter currently targets Linux:

| Filesystem | Required read-only evidence |
| --- | --- |
| ext4 / XFS | Containing mount from `/proc/self/mountinfo`, mounted filesystem root `/`, matching path `st_dev`, matching available block-device `st_rdev`, and UUID from uncached `blkid -p` on that mounted device. |
| ZFS | Containing mount and path device agreement, mounted filesystem root `/`, pool GUID from `zpool get`, and dataset GUID, `mounted=yes`, exact mountpoint, `readonly=off`, and known `keystatus` from `zfs get`. Key status must be `available` or `none`. |

When device entries are present, strict verification also requires every by-id
path to resolve to a whole block device with the declared serial. The Linux
adapter reads serials through `/sys/dev/block` and `device/serial`. It resolves
partition backing to the whole device. It uses the mounted device for ext4/XFS
and the pool's device paths from `zpool status -P` for ZFS. It compares each role's
observed serial set with its declared set and rejects any declared boot serial
behind a role. It does not probe `/` to infer the operating system's boot device.
A declaration of `boot = false` is not an observation of boot identity.

Missing serials, inaccessible by-id paths, unresolved backing, mapped or stacked
block devices, and unsupported ZFS topologies make the device evidence
unavailable and fail strict verification. The adapter includes the ZFS pool's
listed device classes conservatively and requires online device paths.

Both mount and superblock options must say writable. Mount-table or path-device
changes during an observation reject it. Probe errors, missing tools/devices,
permission errors, unavailable keys, unknown properties, ambiguous overmounts,
unsupported filesystems, and mismatched identities fail closed. The adapter only
reads; it never mounts, unlocks, imports a pool, formats, or repairs storage.
Mounted ext4/XFS evidence establishes access to the mapped filesystem, not an
independent assertion about underlying encryption configuration.

macOS has no strict host identity adapter. Strict opening and `pool init` fail
before storage mutation there; ordinary deployments continue to use their
existing behavior. Injected identity and command-output providers exercise the
readiness contract on Mac without mounting anything or accessing a NAS.

## Calls that perform no preflight

Client commands that only contact a running server do no storage preflight on
that client. These include upload, download, rm, mount, checkpoint management,
flush, monitor, fatrace, otrace, NBD provisioning, `pool affected`, and
`pool status --endpoint`. A configuration file used to locate an RPC endpoint
still receives config validation. The running server owns its startup preflight.
Generating a config with `init` also performs no storage preflight.

Lower-level constructors do not run readiness verification. The exported
`ZeroFS::new_with_slatedb_and_lease` constructor accepts an already-open database
and object store. Exported store wrappers such as `RetryingObjectStore::new`
also accept a supplied store without checking its filesystem. Deserializing the
exported `Settings` type with Serde also performs no host probes. These calls
do not acquire the CLI's readiness contract.

The internal `Settings::from_file` parser validates declarations without host
probes. The internal `parse_url_opts` helper builds stores without preflight.
Internal `Journal::open`, writeback store constructors, and `build_slatedb` accept
paths or stores directly without readiness settings. Callers that bypass the
CLI must supply their own readiness checks.

Direct RAM/SSD and SFTP/writeback benchmarks construct their stores, journals,
and workers directly. They do no readiness preflight. In-process tier and extent
benchmarks also bypass it. Config parsing in a benchmark does not establish host
identity or capacity evidence.

With `strict = false`, no host identity or size probes run. The skipped checks
are required role-root existence, canonical mountpoints, resolved root overlap,
containing mounts and mounted filesystem roots, descendant mounts, filesystem
UUIDs or ZFS GUIDs, writable and unlocked state, observed filesystem size versus
the reserve, device serial and whole-block-device identity, backing device sets,
and declared boot-device exclusion. The open-path identity recheck is also
skipped. Config validation and the symlink checks still run. Existing backend
checks can independently reject missing, overlapping, or unsafe paths. Omitting
device declarations also leaves serial, backing-device, and boot exclusion
evidence unavailable in strict mode.

## Threat model

Strict readiness guards against storage that is absent, unmounted, locked,
read-only, misidentified, or declared wrongly when a command starts. Both strict
modes reject symlinks present when they check a path. Startup checks fixed
structural paths and sets bounded by configuration, such as the active cache
namespace's partition leaves. It never enumerates a set that grows with stored
data, such as object files or journal blobs. Per-object operations check
components before I/O, and a listing checks directories as it walks its
requested subtree.

Storage roots must be owned and controlled by the operator and service.
Readiness does not protect against a process that can change entries inside a
storage root while the server runs. That process can already delete pool data.
These runtime races remain:

- `put_opts` checks a path before path-based I/O. Swapping a checked marker or
  parent directory for a symlink between the check and write can redirect a
  member payload write. Foyer opens a cache partition by path and calls
  `set_len`. Replacing that partition with a symlink after its check can
  truncate a file outside the root.
- Multipart `complete` and `abort` take ownership of the native upload before
  checking its path. If that check fails, they drop the upload. The
  `object_store` crate's destructor removes its staging path by name. A parent
  swapped for a symlink during the upload can make cleanup delete a same-named
  file outside the root, even though completion or abort reports an error.

The unanchored sites below retain this risk. The dependency references name the
pinned `object_store` 0.14.1 and `foyer-storage` 0.22.3 sources.

| Operation | Source |
| --- | --- |
| Authority and member writes and reads | `zerofs/src/local_object_store.rs:189`, `:263`, and `:268`. Native file opens are at `object_store/src/local.rs:1069`, `:1363`, and `:506`. |
| Authority and member deletion, copying, and renaming | `zerofs/src/local_object_store.rs:273`, `:327`, and `:345`. Native unlink, link, and rename calls are at `object_store/src/local.rs:780`, `:652`, `:971`, and `:955`. |
| Object listing directory walks and metadata reads | `zerofs/src/local_object_store/listing.rs:126`, `:30`, and `:68`. |
| Native directory creation, mutation locks, and directory sync opens | `zerofs/src/local_object_store.rs:125`, `:150`, and `:108`. The native backend also creates parents and syncs directories at `object_store/src/local.rs:924` and `:1051`. |
| Multipart staging, publication, abort, and destructor cleanup | `zerofs/src/local_object_store.rs:239`, `:433`, and `:455`; `object_store/src/local.rs:1069`, `:1161`, `:1175`, `:1188`, and `:1189`. Part writes retain the opened staging file. |
| Cache namespace creation and ownership lock | `zerofs/src/cli/server.rs:742` and `:756`. The lock leaf uses `O_NOFOLLOW`, but its parents use path resolution. |
| Cache partition inspection and cache directory creation | `zerofs/src/cli/server.rs:815`, `:860`, and `:931`. |
| Cache partition open and resize | `foyer-storage/src/io/device/fs.rs:154` and `:155`, through the builders in `zerofs/src/cli/server.rs:880` and `:948`. |

Catalog and journal storage already use descriptor-anchored no-follow handles.
Moving authority objects, member objects, and cache partitions onto that pattern
is tracked in [the anchored I/O follow-up](storage-readiness-anchored-io.md),
including replacement-race and cleanup tests. Readiness does not retain mount
handles for the lifetime of the server, so a later mount replacement also
remains outside this contract.

## Listing work

Listings use the existing bounded native workers, with at most 32 local
operations in flight across mutations and listings. Stream construction does no
filesystem I/O. The worker checks the prefix's existing components when the
stream is first polled, then rejects symlink entries while walking that subtree.
A delimiter listing walks one directory level. Recursive listings stream batches
of at most 1,024 entries and can return an error after earlier batches succeeded.
They do not scan the rest of the role root before returning results.

Remaining readiness, listing, and cache-pruning walks log progress every five
seconds while processing entries. Cancelling a caller does not stop a batch
already running in a worker. Descriptor-anchored enumeration remains follow-up
work for protection against concurrent parent replacement.

## Scope of evidence

This is a startup preflight, including explicit `pool init` and maintenance opens.
Both `debug reseed-writeback-predecessor` and
`debug accept-remote-writeback-branch` refuse to run whenever readiness is
configured, including `strict = false`. They refuse before
opening the backend or the journal. Their stored backend identity does not bind
a filesystem UUID or GUID, so it cannot validate a relocated journal's filesystem.
Recovery without readiness retains its existing behavior.

Root and catalog-path preflight runs before backend creation and catalog
opening. The catalog then supplies the active pool UUID for fixed member-path
checks before marker creation or recovery writes. Bucket-path and cache
partition checks precede cache initialization and journal replay. Existing
pool/catalog/member identity markers remain in force. An absent catalog is
still rejected on ordinary reopen; readiness never rebuilds it. Expected
filesystem bindings are operator-owned configuration, not new catalog records
or a migration of existing volume formats.

Startup readiness does not pin mounts or directory descriptors through the
server lifetime. It does not guard every publication or resumed write against
later mount loss. Authority and member object I/O and the Foyer cache still use
path-based opens.
The object store checks each requested path and its mutation lock before I/O.
Catalog and journal opens retain their existing descriptor-anchored no-follow
walkers. A symlink created after a check can still redirect a later path-based
open, including an object-store operation or a cache open. The preflight does not
close that window. The placement track must provide its continuing
mutation/publication readiness contract before strict placement can be enabled.
Passing this startup check is not a runtime loss-of-mount guarantee, a
durability receipt, or a migration gate.

Local tests cover injected missing/wrong/locked/read-only/unknown identities for
every role, ordinary fallback directories, aliases, shared ZFS capacity,
independent members, config round-trips, and Mac rejection before mutation.
Device fixtures exercise both strict modes, missing ratings, reserve minimums,
activation refusal, serial and block-device failures, mismatched backing sets,
and declared boot-device exclusion. Parser fixtures exercise sysfs serial text
and ZFS device lists.
Mount-table and command-output tests verify parsing and rejection behavior. They
do not establish OpenZFS runtime behavior. An ignored Linux integration test
checks tmpfs and bind descendants for all five roles in an isolated Docker
container. A separate privileged test checks a loop-mounted ext4 UUID through
real `blkid -p`. It also demonstrates that a mount change after a completed
check reaches the replacement filesystem during a later path-based write.
A fresh check rejects that mount.

- Not verified: OpenZFS lifecycle and same-name dataset recreation, XFS block
  devices, and host bind mounts outside the container were not exercised. The
  completed preflight holds no handle and cannot detect later mount changes.

- Not verified: This change claims no acceptance of TrueNAS app or container
  mount visibility, the encrypted dataset lifecycle, ACL or SMB compatibility,
  hardware durability, or performance.

Colocating control metadata on HDD does not satisfy the separate one-device-loss
survival gate.

## Runtime and migration preconditions remain separate

The current startup memory admission adds fixed reserves of 32 GiB and 8 GiB
before the configured cache and dirty-write budgets. A local run with an explicit
8 GiB application limit was rejected before storage creation. This is a
reproduction budget, not a NAS sizing recommendation. The existing unit test
`small_envelope_does_not_shrink_policy_reserves_into_acceptance` also exercises
that floor. A passing filesystem preflight cannot make this runtime model fit
the N5 Air's reported 29.16 GiB of OS-visible RAM.

A future bounded runtime must leave headroom for TrueNAS, ZFS ARC, Plex, and
other apps, and pass real cgroup-limit, backpressure, OOM-recovery, and dirty-data
tests. An unlimited local run does not establish deployment readiness. Disposable
build scratch is a separate volatile use; it changes no persistent fsync promise.

Independent full-copy capacity is currently insufficient. A proposed external
18 TB drive is not empty or installed and contributes no available capacity.
Recalculate after the operations-owned transfer, whole-drive final audit, and
physical installation, using final inventory, engine overhead, other datasets,
and reserves. Drive labels use decimal TB;
`zpool list` and `zfs list` values with a `T` suffix use TiB. Capacity verification
and all migration safety gates remain required.
