# M0: metadata survival after one device is lost

Status: design and failure reproduction at
`5ac4d4304f04699d9e0895ec60516a6bbad5c57d`. The metadata-survival gate
**fails today**. Strict placement and migration remain blocked. M0 changes no
production mechanism, journal format, catalog schema, readiness, or profile.

This refines gate 1 and row M0 of the merged `zerofs-placement.md`, after reading
both `zerofs-placement-seat-a.md` and `zerofs-placement-seat-b.md` in the
`truenas-storage-union` design directory. Their proposed single HDD authority
avoids making drained data depend on NVMe, but still fails authority-device loss.

## Verified baseline

These are source observations, not proposed functionality. Paths refer to the
pinned commit; the prototype below exercises the first four observations.

| Owner | Existing behavior and evidence |
| --- | --- |
| Authority | `storage_backend::open_pool_storage` constructs one local authority. `PoolObjectStore::{put_opts,get_opts}` in `zerofs/src/pool_object_store/mod.rs` delegates non-segment objects there. SlateDB manifests/SSTs and `zerofs.key` therefore do not become redundant by adding data members. |
| Catalog | `zerofs/src/pool_object_store/catalog.rs` owns `pool.redb`: identity, members, physical generations/checksums, reservations, garbage, health, faults, and hidden legacy objects. Immediate redb durability protects one device. `Catalog::open` rejects a missing authority marker; `storage_backend::open_pool_storage` rejects a missing catalog. Neither rebuilds it from members. Identity is also bound to the canonical catalog directory. |
| Namespace | `zerofs/src/fs/store/directory.rs`, `fs/inode.rs`, and `fs/key_codec.rs` hold directory names, inodes, attributes, and current extent locations in the database. `segment::DirEntry` contains only offset, length, inode, and extent. It cannot distinguish a deleted/overwritten extent from the current mapping or recover empty directories, hardlink names, lengths, modes, or ownership. |
| Encryption | `key_management::{load_or_init_encryption_key,load_existing_encryption_key}` uses a random DEK and authority object `zerofs.key`. Its bincode envelope carries salt, nonce, wrapped DEK, and version; KDF parameters are currently code constants. The password alone cannot recreate the DEK. Normal writable initialization can create a new key when absent; recovery must use the existing-key path and refuse that substitution. |
| Publication | `fs/flush_coordinator.rs` seals data before flushing database metadata. Pool `put_reserved` writes a fresh physical generation, optionally verifies it, commits the catalog mapping, then cleans the old generation. `local_object_store.rs` uses fsync. This is useful ordering, not an atomic commit across independent metadata devices. |
| Receipts | `fs/mutation/{ack,durability}.rs` distinguishes local SSD from remote backend and captures mutation/object cutoffs. Administrative flush targets the backend. Neither target currently asserts redundant metadata survival. `pool_export_tests.rs` covers NFS/NBD exports, not recovery without authority. |

The current inode/protocol contract does not represent ACLs/xattrs. M0 cannot
recover unsupported metadata that was never stored; F1/F3 and fidelity gates
remain separate blockers. No complete member-only namespace recovery command
was found in the inspected startup, pool maintenance, segment, or transfer paths.

## Alternatives and choice for M1

| | Live consistent metadata redundancy across members | Reconstruction from segment-embedded records |
| --- | --- | --- |
| Surviving state | One logical namespace/catalog with a durably consistent recovery generation on at least two independent durable member devices. Payload stays single-copy. | Self-describing payload plus enough authenticated metadata history/checkpoints to rebuild namespace and catalog without the authority. |
| Required coverage | Authority manifests/SST dependencies, catalog identity/mappings/debts, keys, membership and writer epochs, current namespace, tombstones, allocation watermarks, and future bindings/policies. | All of the same facts, including metadata-only transactions, empty files/directories, symlinks, all hardlink names, holes/truncation, rename/unlink history, compaction/remap generations, and incomplete-publication debts. Current reverse directories cover only frame identity. |
| One-member loss | Every acknowledged survival commit must exist outside each possible lost device. A member carrying payload and a metadata replica loses both roles together. | Metadata embedded only beside its sole-copy payload disappears with that member. Recovery records and commit evidence must also span independent devices, including an empty pool with no segments. |
| Cost/risk | Extra metadata writes, flush latency, space reserve, cross-store ordering, lag/backpressure, fencing and replica repair. An asynchronous copy of `pool.redb` or separate mirrors of two databases is insufficient. | New authenticated formats, complete transaction replay and tombstones, scan cost, key bootstrap, stale-frame ambiguity, and GC retention. Standalone replicated metadata capsules become necessary; the design converges on a metadata log. |

Take **live consistent metadata redundancy** forward to M1, provisionally. Keep
the current single logical metadata owners; require an explicit shared commit
boundary for their recovery state. Do not implement a second mutable namespace
or independently mirrored catalog. Segment records can later aid salvage and
verification, but current records cannot satisfy this gate. This choice adds
metadata copies, not payload redundancy or a backup product. If metadata copies
are forbidden, or a consistent cross-store recovery generation cannot be made
durable, stop: moving the authority to an HDD is not an acceptable fallback.

## Proposed commit and loss contract (not implemented)

Keep the existing buffered and local-SSD states. Local-SSD success covers the
journal on a surviving NVMe only. An undrained suffix can be lost permanently
with that NVMe. Today's backend receipt is a useful cutoff but does not establish
the new survival contract.

A future survival commit `C` names the pool UUID, membership/writer epoch,
monotonic sequence and predecessor digest, mutation incarnation/cutoff, covered
object sequence, namespace checkpoint plus complete replay prefix, catalog
generation, key-envelope epoch, and dependency hashes. The namespace and catalog
must describe the **same** logical prefix. No receipt may combine independent
"latest" files. All acknowledged commits must survive removal of any one device.

For the first M1 design, use a fixed set of at least two metadata devices and
require durability on **every configured replica** before survival success.
This trades write availability for a tractable initial contract; it is not a
two-replica majority-election scheme. After loss, recover read-only under an
exclusive, externally fenced writer; do not let separated survivors accept
independent mutations. With insufficient redundancy, stop survival acknowledgments.
Membership replacement requires a fenced epoch transition and full verified
catch-up before writes resume. Automated election/degraded writes need a separate
quorum/fencing design and are outside this proposal.

One serialization owner must order all namespace, catalog, encryption-envelope,
maintenance, and membership mutations:

1. Fence the writer and assign the operation/epoch. Durably preserve identity,
   key envelopes, reservation/intent and recovery dependencies across replicas
   before an operation can make a sole payload disposable.
2. Publish and sync the exact immutable payload generation on its target. Preserve
   data-before-reference ordering. A future missing sole-copy member is reported
   as missing payload; metadata redundancy cannot repair those bytes.
3. Produce a recoverable namespace/catalog generation covering the same cutoff,
   including deletes, allocation counters, pending uploads and cleanup debts.
   Flush all referenced metadata objects and checkpoint/replay dependencies on
   each replica. Copying a live redb file is not a valid generation protocol.
4. Persist an authenticated commit record naming those dependencies on every
   replica, then return survival success. An interrupted fanout may expose an
   unacknowledged complete generation, but must never expose a half transaction.
   Recovery selects the highest valid chained commit in the fenced epoch, checks
   every dependency, and rejects conflicting digests; timestamps are not votes.
5. Retire journal prefixes and old payload/metadata generations only after this
   commit and all retained recovery/checkpoint references permit it. GC, compaction,
   relocation, and debt cleanup participate in the same order. Failed reads keep
   uncertain data. Replica repair cannot resurrect an older namespace or debt.

This is the M1 invariant, not a claim that today's SlateDB flush plus redb commit
implements it. M1 must specify and test the actual checkpoint/replay encoding,
crash atomicity, catalog path rebinding, authentication, and fencing integration
before adopting the mechanism. Read-only recovery must preserve names/attributes
even where payload is missing, and identify affected files/ranges explicitly.

## Encryption survival

Each metadata replica needs the original wrapped DEK envelope and its version,
salt, nonce, KDF algorithm/parameters, key epoch and pool binding, accessible
without first decrypting the namespace or consulting a lost catalog. Authenticate
the envelope-to-pool/commit binding. Never emit a raw DEK or secret in receipts.
Test that the recovered envelope opens original metadata and frames. Holding a
raw DEK in the test process is not recovery evidence.

Password changes must publish the new envelope consistently on all replicas and
commit its epoch before reporting success. Interrupted changes must have defined
credential semantics; stale replicas must not silently restore a revoked old
envelope. The operator must retain the current password/secret independently of
any one tested device. Native encrypted-filesystem unlock keys and required
configuration/identity material need the same independent availability, outside
the ZeroFS envelope; their recovery is an additional deployment acceptance gate.

## Device-loss matrix and M0 evidence

All M1 cases need cold recovery with no RAM, caches, hidden original authority,
or retained raw DEK. Compare exact namespace/inode identity, represented attributes,
current data hashes, missing ranges, catalog identity/debts, and key epoch.

| Fault | Required result | M0 coverage |
| --- | --- | --- |
| No loss, after backend cutoff | Exact committed namespace and payloads reopen. | Real local pool/SlateDB/key cold-open control before every loss. |
| NVMe before/after backend cutoff | Only the documented local-only suffix may disappear; committed prefix survives without journal/cache. | Unverified; prototype has no writeback journal. |
| Authority objects only; catalog survives | Recover committed names and keys from another member. | Baseline fails at missing authority identity marker. |
| Catalog only; authority survives | Recover original mappings, identity, reservations and debts. | Baseline refuses missing catalog. |
| Authority device including catalog and key | Recover the committed prefix with surviving members and password. | Ignored acceptance fails; passing reproduction asserts today's refusal. |
| Wrapped key only | Recover original DEK using surviving envelope and password. | Existing-key loader fails, despite usable pool/catalog and surviving segments. |
| Any data member, including one holding metadata | Recover metadata; report exact lost sole-copy ranges, keep other files readable. | Unverified. Test whole-file and striped layouts once implemented. |
| Interrupted commit/repair/key change, then each device loss | At least acknowledged prefix, no partial rename/remap, stale replay, key substitution or premature GC. | Unverified; cut each step above before/after sync and receipt. |
| Offline, full, corrupt, wrong identity, stale replica, concurrent mutation | Refuse unsafe commits; retain previous recovery state and bounded pending work. | Unverified; includes metadata-only and empty-volume workloads. |

The Mac-safe test module `zerofs/src/metadata_survival_tests.rs` uses disposable
directories as logical devices on one host filesystem. It initializes the real
pool/catalog, creates a real encrypted SlateDB namespace, writes and overwrites
real segments, truncates, renames, links, removes a file, and closes all workers.
Cold lookup/read verifies the control before fault injection. It checks physical
member hashes after loss. The fixture uses `ObjectCoverage::DirectRemote`; its
materialized mutation cutoff is zero. The completed flush and cold control prove
the serialized fixture prefix, not NVMe journal coverage or a survival receipt.
Generated fixture state is removed; it never opens a
real disk, mount, NAS, or source tree. Directory removal models permanent state
loss, not physical power failure or independent hardware failure domains.

Run from `zerofs/` on this Mac, bypassing the PATH cargo-hauler wrapper:

```sh
CARGO_TARGET_DIR="$PWD/target/m0-metadata-survival" \
  /Users/zackjackson/.cargo/bin/cargo +1.100.0 test --locked -p zerofs --lib \
  metadata_survival_tests:: -- --nocapture

CARGO_TARGET_DIR="$PWD/target/m0-metadata-survival" \
  /Users/zackjackson/.cargo/bin/cargo +1.100.0 test --locked -p zerofs --lib \
  metadata_survival_tests::authority_device_loss_preserves_committed_namespace \
  -- --exact --ignored --nocapture
```

The passing reproduction documents the missing behavior; it does not pass the
survival gate. The ignored test must attempt actual cold namespace recovery and
fail at the real missing dependency. M1 still owes the complete matrix, the
multi-device commit mechanism and crash proofs, encryption survival, integrity
and cleanup evidence, and hardware power-loss validation. No strict placement
capability is enabled by M0.

## Local execution evidence

On macOS arm64 with Rust/Cargo 1.100.0, the reproduction command above passed
(`1 passed; 0 failed; 1 ignored`). Each of its four cases first passed cold
namespace/catalog verification and retained three unchanged physical segments
across both members. Authority-only loss failed at the identity marker; catalog
and combined device loss failed at the missing catalog; key-only loss failed
at the existing-key loader. Each reported successful fixture cleanup.

The explicitly ignored acceptance command exited **101**, after the control and
cleanup passed, with this intended failure (not a setup error):

```text
M0 gate failed: committed namespace must survive authority-device loss: open original pool
Caused by:
    pool catalog is missing: initialize a new pool with `zerofs pool init`, or restore an existing pool's catalog; normal startup never recreates it
test result: FAILED. 0 passed; 1 failed; 0 ignored; 0 measured; 2213 filtered out
```

The fixture canonicalizes temporary paths because macOS may expose `/var` as a
symlink; descriptor-anchored catalog opening rejects such aliases. An initial
setup failure on that alias was corrected before collecting the evidence above.

The normal library suite passed: **2,194 passed, 0 failed, 20 ignored**, in
183.73 seconds. The ignored set includes the M0 acceptance and existing ignored
tests; no ignored benchmarks were run. Run from `zerofs/`:

```sh
m0_tmp_parent=$(mktemp -d /private/tmp/zfsm0.XXXXXX)
TMPDIR="$m0_tmp_parent" CARGO_TARGET_DIR="$PWD/target/m0-metadata-survival" \
  /Users/zackjackson/.cargo/bin/cargo +1.100.0 test --locked -p zerofs --lib \
  -- --test-threads=4
rmdir "$m0_tmp_parent"
```

The short canonical scratch parent was removed successfully. An earlier broad
run used a long checkout-local `TMPDIR` and was stopped after existing Unix
socket fixtures failed with `path must be shorter than SUN_LEN`; the short-path
rerun above resolved those setup failures. Its abandoned fixtures were removed.
`cargo +1.100.0 fmt --all -- --check` and `git diff --check` also passed. All
builds/tests used the native Mac toolchain directly, with no remote cargo,
toolchain-default changes, Linux-only benchmarks, NAS access, or deployment.
