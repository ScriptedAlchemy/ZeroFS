# Metadata survival after one device is lost

This document is the design and failure reproduction for metadata survival
(plan row M0), written against commit
`5ac4d4304f04699d9e0895ec60516a6bbad5c57d`. The metadata-survival gate **fails
today**. Strict placement and migration remain blocked. This work changes no
production mechanism, journal format, catalog schema, readiness check or disk
profile.

The gate comes from the merged `zerofs-placement.md` in the `truenas-storage-union`
design directory. It requires the namespace, placement catalog and encryption
material to survive the loss of any one device. This document refines that gate
after reading both `zerofs-placement-seat-a.md` and `zerofs-placement-seat-b.md`.
Their proposed single HDD authority keeps drained data from depending on NVMe,
but it still fails when the authority device is lost.

## Verified baseline

The table records what the source does today. It proposes nothing. Paths refer
to the pinned commit, and the prototype below exercises the first four rows.

| Owner | Existing behavior and evidence |
| --- | --- |
| Authority | `storage_backend::open_pool_storage` constructs one local authority. `PoolObjectStore::{put_opts,get_opts}` in `zerofs/src/pool_object_store/mod.rs` sends every non-segment object there. Adding data members therefore does not make the SlateDB manifests, the SSTs or `zerofs.key` redundant. |
| Catalog | `zerofs/src/pool_object_store/catalog.rs` owns `pool.redb`. It holds identity, members, physical generations and checksums, reservations, garbage, health, faults and hidden legacy objects. Immediate redb durability protects one device. `Catalog::open` rejects a missing authority marker, and `storage_backend::open_pool_storage` rejects a missing catalog. Neither rebuilds it from members. Identity is also bound to the canonical catalog directory. |
| Namespace | `zerofs/src/fs/store/directory.rs`, `fs/inode.rs` and `fs/key_codec.rs` keep directory names, inodes, attributes and current extent locations in the database. `segment::DirEntry` holds only the offset, length, inode and extent. It cannot tell a deleted or overwritten extent from the current mapping. It also cannot recover empty directories, hardlink names, lengths, modes or ownership. |
| Encryption | `key_management::{load_or_init_encryption_key,load_existing_encryption_key}` uses a random data encryption key (DEK) and the authority object `zerofs.key`. Its bincode envelope carries the salt, nonce, wrapped DEK and version. The KDF parameters are code constants today. The password alone cannot recreate the DEK. Normal writable initialization can create a new key when none exists, so recovery must use the existing-key path and refuse that substitution. |
| Publication | `fs/flush_coordinator.rs` seals data before it flushes database metadata. Pool `put_reserved` writes a fresh physical generation, optionally verifies it, commits the catalog mapping and then cleans the old generation. `local_object_store.rs` uses fsync. This ordering is not an atomic commit across independent metadata devices. |
| Receipts | `fs/mutation/{ack,durability}.rs` tells the local SSD apart from the remote backend and captures mutation and object cutoffs. Administrative flush targets the backend. Neither target asserts redundant metadata survival today. `pool_export_tests.rs` covers NFS and NBD exports, not recovery without the authority. |

The current inode and protocol contract does not represent ACLs or extended
attributes. Recovery cannot bring back metadata that was never stored, so
extended-attribute support (plan row F1), the permissions and ACL contract
(plan row F3) and the metadata fidelity gate remain separate blockers. The
inspected startup, pool maintenance, segment and transfer paths contain no
command that recovers the whole namespace from the data members alone.

## Alternatives and the choice for the implementation row

| | Live consistent metadata redundancy across members | Reconstruction from segment-embedded records |
| --- | --- | --- |
| Surviving state | One logical namespace and catalog keep a durably consistent recovery generation on at least two independent member devices. Payload stays single-copy. | Self-describing payload carries enough authenticated metadata history and checkpoints to rebuild the namespace and catalog without the authority. |
| Required coverage | The design must cover the authority's manifest and SST dependencies, catalog identity, mappings and debts, keys, membership and writer epochs, the current namespace, tombstones, allocation watermarks, and future bindings and policies. | The design must cover all of the same facts. It must also cover metadata-only transactions, empty files and directories, symlinks, every hardlink name, holes and truncation, rename and unlink history, compaction and remap generations, and incomplete-publication debts. Today's reverse directories cover only frame identity. |
| Loss of one member | Every acknowledged survival commit must exist outside each device that could be lost. A member that carries payload and a metadata replica loses both roles together. | Metadata embedded only beside its sole-copy payload disappears with that member. Recovery records and commit evidence must also span independent devices, including an empty pool with no segments. |
| Cost and risk | The design adds metadata writes, flush latency, a space reserve, cross-store ordering, lag and backpressure, fencing and replica repair. An asynchronous copy of `pool.redb`, or separate mirrors of the two databases, is not enough. | The design needs new authenticated formats, complete transaction replay with tombstones, scan cost, key bootstrap, stale-frame ambiguity and GC retention. It also needs standalone replicated metadata capsules, so it turns into a metadata log. |

The implementation row (plan row M1) should provisionally take **live consistent
metadata redundancy** forward. It keeps the current single logical metadata
owners and requires an explicit shared commit boundary for their recovery state.
It must not add a second mutable namespace or an independently mirrored catalog.
Segment records can later help salvage and verification, but today's records
cannot satisfy this gate. This choice adds metadata copies. It does not add
payload redundancy or a backup product. Stop if metadata copies are forbidden or
if a consistent cross-store recovery generation cannot be made durable. Moving
the authority to an HDD is not an acceptable fallback.

## Proposed commit and loss contract (not implemented)

The existing buffered and local SSD states stay as they are. Local SSD success
covers only the journal on a surviving NVMe device. The suffix that has not
drained yet can be lost permanently with that NVMe device. Today's backend
receipt is a useful cutoff, but it does not establish the new survival contract.

A future survival commit `C` names these values:

- the pool UUID and the membership and writer epoch;
- a monotonic sequence number and the predecessor digest;
- the mutation incarnation and cutoff, and the covered object sequence;
- a namespace checkpoint plus its complete replay prefix;
- the catalog generation and the key-envelope epoch;
- the dependency hashes.

The namespace and the catalog must describe the **same** logical prefix. No
receipt may combine independent "latest" files. Every acknowledged commit must
survive the removal of any one device.

The first implementation design uses a fixed set of at least two metadata
devices. It requires durability on **every configured replica** before it
reports survival success. That choice gives up write availability to keep the
first contract tractable, and it is not a two-replica majority election. After a
loss, recovery runs read-only under one exclusive writer that something outside
the pool fences. Separated survivors must not accept independent mutations.
When too few replicas remain, the system stops acknowledging survival. Replacing
a member requires a fenced epoch transition and a full verified catch-up before
writes resume. Automated election and degraded writes need a separate quorum and
fencing design, which this proposal does not cover.

One serialization owner orders every namespace, catalog, encryption-envelope,
maintenance and membership mutation.

1. The owner fences the writer and assigns the operation and epoch. Before any
   operation can make a sole payload disposable, the owner durably preserves the
   identity, key envelopes, reservation or intent, and recovery dependencies on
   all replicas.
2. The owner publishes and syncs the exact immutable payload generation on its
   target. Data is written before anything references it. If a sole-copy member
   later goes missing, recovery reports the payload as missing, because metadata
   redundancy cannot repair those bytes.
3. The owner produces a recoverable namespace and catalog generation for the
   same cutoff. It includes deletes, allocation counters, pending uploads and
   cleanup debts. The owner flushes every referenced metadata object and every
   checkpoint and replay dependency on each replica. Copying a live redb file is
   not a valid generation protocol.
4. The owner persists an authenticated commit record that names those
   dependencies on every replica, and only then returns survival success. An
   interrupted fanout may expose a complete generation that was never
   acknowledged, but it must never expose half a transaction. Recovery selects
   the highest valid chained commit in the fenced epoch, checks every dependency
   and rejects conflicting digests. Recovery never picks a commit by timestamp.
5. The owner retires journal prefixes and old payload and metadata generations
   only after this commit and every retained recovery or checkpoint reference
   allow it. GC, compaction, relocation and debt cleanup follow the same order.
   A failed read keeps data whose state is uncertain. Replica repair cannot bring
   back an older namespace or debt.

This is the invariant the implementation row must meet. Today's SlateDB flush
plus redb commit does not implement it. Before it adopts the mechanism, that row
must specify and test the checkpoint and replay encoding, crash atomicity,
catalog path rebinding, authentication and fencing integration. Read-only
recovery must keep names and attributes even where the payload is missing, and
it must name the affected files and ranges.

## Encryption survival

Each metadata replica needs the original wrapped DEK envelope with its version,
salt, nonce, KDF algorithm and parameters, key epoch and pool binding. Recovery
must reach the envelope without first decrypting the namespace or consulting a
lost catalog. The binding from the envelope to the pool and commit must be
authenticated. Receipts must never contain a raw DEK or secret. A test must show
that the recovered envelope opens the original metadata and frames. A raw DEK
held in the test process is not recovery evidence.

A password change must publish the new envelope consistently on every replica
and commit its epoch before it reports success. An interrupted change needs
defined credential semantics, and a stale replica must not silently restore a
revoked envelope. The operator must keep the current password or secret
somewhere that does not depend on any one tested device. Native
encrypted-filesystem unlock keys and the required configuration and identity
material need the same independent availability outside the ZeroFS envelope.
Their recovery is an additional deployment acceptance gate.

## Device-loss matrix and current evidence

Every case in the implementation row needs a cold recovery with no RAM state,
caches, hidden original authority or retained raw DEK. Each case compares the
exact namespace and inode identity, the represented attributes, current data
hashes, missing ranges, catalog identity and debts, and the key epoch.

| Fault | Required result | Coverage in this change |
| --- | --- | --- |
| No loss, after the backend cutoff | The exact committed namespace and payloads reopen. | A real local pool, SlateDB namespace and key pass a cold-open control before every loss. |
| NVMe device lost before or after the backend cutoff | Only the documented local-only suffix may disappear. The committed prefix survives without the journal or cache. | Not verified: the prototype has no writeback journal. |
| Authority objects only, with the catalog surviving | Recovery restores committed names and keys from another member. | The baseline fails at the missing authority identity marker. |
| Catalog only, with the authority surviving | Recovery restores the original mappings, identity, reservations and debts. | The baseline refuses to open without the catalog. |
| Authority device, including the catalog and key | Recovery restores the committed prefix from the surviving members and the password. | The ignored acceptance test fails, and the passing reproduction asserts today's refusal. |
| Wrapped key only | Recovery restores the original DEK from a surviving envelope and the password. | The existing-key loader fails even though the pool, catalog and segments survive. |
| Any data member, including one that holds metadata | Recovery restores metadata, reports the exact lost sole-copy ranges and keeps other files readable. | Not verified: whole-file and striped layouts need tests once they exist. |
| Interrupted commit, repair or key change, followed by each device loss | At least the acknowledged prefix survives, with no partial rename or remap, stale replay, key substitution or premature GC. | Not verified: each step above needs a cut before and after its sync and receipt. |
| Offline, full, corrupt, wrong-identity or stale replica, or a concurrent mutation | The owner refuses unsafe commits and keeps the previous recovery state and bounded pending work. | Not verified: the cases include metadata-only and empty-volume workloads. |

The Mac-safe test module `zerofs/src/metadata_survival_tests.rs` uses disposable
directories on one host filesystem as logical devices. It initializes the real
pool and catalog and creates a real encrypted SlateDB namespace. It writes and
overwrites real segments, then truncates, renames, links and removes a file, and
closes all workers. A cold lookup and read verify the control before the test
injects any fault, and it checks physical member hashes after the loss. The
fixture uses `ObjectCoverage::DirectRemote`, so its materialized mutation cutoff
is zero. The completed flush and cold control prove the serialized fixture
prefix. They do not prove NVMe journal coverage or a survival receipt. The test
removes its generated fixture state, and it never opens a real disk, mount, NAS
or source tree. Removing a directory models permanent state loss. It does not
model physical power failure or independent hardware failure domains.

Run these from `zerofs/` on the Mac. They call cargo directly and bypass the
cargo-hauler wrapper on PATH.

```sh
CARGO_TARGET_DIR="$PWD/target/m0-metadata-survival" \
  /Users/zackjackson/.cargo/bin/cargo +1.100.0 test --locked -p zerofs --lib \
  metadata_survival_tests:: -- --nocapture

CARGO_TARGET_DIR="$PWD/target/m0-metadata-survival" \
  /Users/zackjackson/.cargo/bin/cargo +1.100.0 test --locked -p zerofs --lib \
  metadata_survival_tests::authority_device_loss_preserves_committed_namespace \
  -- --exact --ignored --nocapture
```

The passing reproduction documents the missing behavior, and it does not pass
the survival gate. The ignored test attempts a real cold namespace recovery and
must fail at the real missing dependency. The implementation row still owes the
complete matrix, the multi-device commit mechanism and its crash proofs,
encryption survival, integrity and cleanup evidence, and hardware power-loss
validation. This change enables no strict placement capability.

## Local execution evidence

The reproduction command above passed on macOS arm64 with Rust and Cargo
1.100.0. One test passed, none failed and one was ignored. Each of its four cases
first passed cold namespace and catalog verification and kept three unchanged
physical segments across both members. Authority-only loss failed at the
identity marker. Catalog loss and combined device loss failed at the missing
catalog. Key-only loss failed at the existing-key loader. Each case reported a
successful fixture cleanup.

The explicitly ignored acceptance command failed with exit code 101, as
intended, after the control and cleanup passed. This is the expected failure,
not a setup error.

```text
M0 gate failed: committed namespace must survive authority-device loss: open original pool
Caused by:
    pool catalog is missing: initialize a new pool with `zerofs pool init`, or restore an existing pool's catalog; normal startup never recreates it
test result: FAILED. 0 passed; 1 failed; 0 ignored; 0 measured; 2213 filtered out
```

The fixture canonicalizes temporary paths because macOS may expose `/var` as a
symlink, and descriptor-anchored catalog opening rejects such aliases. The first
setup run failed on that alias, and the fixture was corrected before the
evidence above was collected.

The normal library suite passed with **2,194 passed, 0 failed and 20 ignored** in
183.73 seconds. The ignored set includes this acceptance test and the existing
ignored tests. No ignored benchmarks ran. Run it from `zerofs/`.

```sh
m0_tmp_parent=$(mktemp -d /private/tmp/zfsm0.XXXXXX)
TMPDIR="$m0_tmp_parent" CARGO_TARGET_DIR="$PWD/target/m0-metadata-survival" \
  /Users/zackjackson/.cargo/bin/cargo +1.100.0 test --locked -p zerofs --lib \
  -- --test-threads=4
rmdir "$m0_tmp_parent"
```

The short canonical scratch parent was removed. An earlier broad run used a long
`TMPDIR` inside the checkout. It was stopped after existing Unix socket fixtures
failed with `path must be shorter than SUN_LEN`, and the short-path rerun above
cleared those setup failures. The abandoned fixtures from that run were removed.
`cargo +1.100.0 fmt --all -- --check` and `git diff --check` also passed. Every
build and test used the native Mac toolchain directly. None used remote cargo,
changed the default toolchain, ran Linux-only benchmarks, accessed the NAS or
deployed anything.
