# Declared filesystem profiles and startup readiness

`[storage.readiness]` adds opt-in identity verification before a local pool is
opened. Omitting the section preserves ordinary local/cloud deployments. Setting
`strict = false` retains the ordinary open behavior while still checking the
declaration's structure when loading configuration. No identity probes run in
either case. `strict` must be written explicitly when this section is present.

This implements the filesystem-profile and startup-preflight portion of
placement-plan row S2. S2 remains incomplete. Declarations for device roles,
rated write budgets, and reserves still need the hardware-aware follow-up. The current
schema binds storage paths to filesystems, not workload policies to devices.
P1 remains blocked until the complete S2 row is delivered.

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

The hardware-aware follow-up must distinguish declared ratings, configured
workload expectations, measured activity, and unavailable observations. This
schema has no rated write budget or reserve declarations. It does not verify
device serials, boot-device exclusion, endurance, power-loss protection,
sustained throughput, or write budgets. Filesystem identity alone
does not establish those facts. No declaration in this change activates the
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
must declare an HDD profile; cache and journal must declare `nvme`. These are
validated declarations, not hardware detection. The strict graph supports local
`file://` pool roots only and rejects a separate WAL store rather than leaving a
storage role unchecked.

All active role directories must already exist, including catalog, cache, and
journal directories. Readiness creates nothing. A role may be a dedicated child
directory on its declared mounted filesystem. Role directories must be disjoint,
including after resolving symlinks. Nested mounts cannot substitute for the
declared mountpoint. Declared mountpoints must themselves be canonical paths.

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
tested. Its paths, IDs, GUIDs, and UUID are fixtures, not discovered operator values
or evidence of an installed disk. Use independently obtained real identities for
an actual deployment; do not substitute those fixture values.

## Host evidence and failures

The native adapter currently targets Linux:

| Filesystem | Required read-only evidence |
| --- | --- |
| ext4 / XFS | Containing mount from `/proc/self/mountinfo`, mounted filesystem root `/`, matching path `st_dev`, matching available block-device `st_rdev`, and UUID from uncached `blkid -p` on that mounted device. |
| ZFS | Containing mount and path device agreement, mounted filesystem root `/`, pool GUID from `zpool get`, and dataset GUID, `mounted=yes`, exact mountpoint, `readonly=off`, and known `keystatus` from `zfs get`. Key status must be `available` or `none`. |

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
Mount-table and command-output tests verify parsing and rejection behavior. They
do not establish Linux mount, block-device, or OpenZFS runtime behavior. No
TrueNAS app/container mount visibility, encrypted dataset lifecycle, ACL, SMB,
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
