# Declared filesystem profiles and startup readiness

`[storage.readiness]` adds opt-in identity verification before a local pool is
opened. Omitting the section preserves ordinary local/cloud deployments. Setting
`strict = false` retains the ordinary open behavior while still checking the
declaration's structure when loading configuration. No identity probes run in
either case. `strict` must be written explicitly when this section is present.

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
establish those facts. No declaration in this change activates the
installed Crucial P3. Its proposed role is optional clean read retention, pending
resolution of the old/untouched-region read anomaly and measured workload benefit.
Cache fills, promotions, and rebuilds write bytes even though cache hits do not.

Budget pressure must refuse new work before acceptance. It must preserve and
drain already accepted dirty data under its original durability contract.
Direct-to-HDD routing is a deliberate policy choice, with backend publication
semantics. It is not a fallback from an unsatisfied journal policy.

## Declarations and role bindings

Declare each filesystem once with an ID, its canonical mountpoint, its stable
identity, and one operator-selected profile: `nvme`, `hdd-cmr`, or `hdd-smr`.
Profiles are never inferred from devices, model names, mount types, or timings.
No profile changes scheduling or promises SMR performance in S2.

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
against the declaration. Before opening storage, preflight checks the catalog
files and the cache and journal namespace children, including existing symlink
targets. Missing children use their closest existing ancestor without creating
anything. Nested mounts cannot substitute for the declared mountpoint. Declared mountpoints must themselves be canonical paths.

Authority, catalog, and one member may share a filesystem through disjoint
directories. Journal and cache may likewise share the NVMe filesystem. Multiple
data members cannot use the same capacity domain. For ext4/XFS, that domain is
the filesystem UUID; for ZFS, it is the **pool GUID**, even across distinct dataset
GUIDs. This conservatively rejects shared-pool member quotas that could otherwise
count pool free space twice. Aliased declarations and conflicting profiles in one
capacity domain are rejected. S2 performs no capacity summation or new admission
accounting. Shared control/data space still needs the existing reserves.

ZFS GUIDs are canonical nonzero decimal **strings**, preserving the full unsigned
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
on that domain or below the journal's enforced `min_free_gb`. The existing pool
reserve default remains 1.0 GB. These existing settings remain the enforced
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

## Scope of evidence

This is a startup preflight, including explicit `pool init` and maintenance opens.
Both `debug reseed-writeback-predecessor` and
`debug accept-remote-writeback-branch` refuse to run whenever readiness is
configured, including `strict = false`. They refuse before
opening the backend or the journal. Their stored backend identity does not bind
a filesystem UUID or GUID, so it cannot validate a relocated journal's filesystem.
Recovery without readiness retains its existing behavior.

It runs before backend creation, catalog opening, cache initialization, and
journal replay. Existing pool/catalog/member identity markers remain in force.
An absent catalog is still rejected on ordinary reopen; readiness never rebuilds
it. Expected filesystem bindings are operator-owned configuration, not new
catalog records or a migration of existing volume formats.

S2 does **not** pin mounts or directory descriptors through the server lifetime,
and does not guard every publication or resumed write against later mount loss.
There remains a race between preflight and later path-based storage opens. The
placement track must provide its continuing mutation/publication readiness
contract before strict placement can be enabled. Passing this startup check is
not a runtime loss-of-mount guarantee, a durability receipt, or a migration gate.

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

No TrueNAS app/container mount visibility, encrypted dataset lifecycle, ACL, SMB,
hardware durability, or performance acceptance is claimed. Colocating control
metadata on HDD does not satisfy the separate one-device-loss survival gate.

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
