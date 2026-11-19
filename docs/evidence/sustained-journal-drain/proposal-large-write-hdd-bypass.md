# Proposal, not implemented: large sequential writes direct to HDD members

**Status: proposal only.** No code for this exists on `perf/sustained-journal-drain` or any other branch. The acceptance contract stays RAM-acknowledged WRITE plus SSD-backed explicit FLUSH. This document describes a different contract that would need its own decision and qualification.

## Why consider it

The app owner's plain 8 GiB controls onto the P3 ZFS dataset, with no ZeroFS in the path, landed between about 57 and 133 MB/s across signed SMB, UNFS3, kernel NFS and NAS-local fio. The P3 accepts roughly a gigabyte quickly and is much slower after that; the cause is unresolved. The two HDDs wrote 134.6 and 203.6 MB/s concurrently. Under the current contract every payload byte of a large copy must reach the P3 before FLUSH can succeed. Once the fast region is spent, the copy therefore runs at the P3's sustained rate divided by its write amplification. A path that sends large sequential data straight to the HDD members could run at up to their combined rate.

## Contract it would introduce

- Ordinary WRITE: unchanged, acknowledged from bounded RAM.
- For a file whose dirty extent run passes a size threshold (for example 1 GiB of contiguous appends), sealed segments go to an HDD member without being journalled. Metadata and small or random writes keep using the journal.
- FLUSH/fsync/COMMIT on such a file succeeds only after: every bypassed segment is durable on its member (file data fsync plus directory fsync), the pool catalog transaction naming it has committed, and the filesystem metadata referencing it is durable through the journal. Durability then depends on the HDD and the P3 together, instead of the P3 alone until drain.

## Failure cases to qualify

- Power loss after a member write but before the catalog or metadata commit: orphaned segment, reclaimed by the existing pool garbage collection. No acknowledged FLUSH was issued.
- Power loss after the catalog commit but before the metadata commit: the segment is placed but unreferenced, so collectable. FLUSH not acknowledged.
- An HDD fails or is quarantined mid-copy: today's journal keeps the only copy until drain. Under bypass there is no SSD copy, so FLUSH must fail and the client must retry against the remaining member. This needs the degraded-member work owned by another thread.
- Interleaving with snapshots, rename and truncate of a file in bypass mode: ordering must match the journalled path.
- Mixed small and large writes to the same file: crossing the threshold must not reorder writes the client already saw acknowledged.
- SMR behaviour of the WD and Seagate drives under long sequential runs: unmeasured.

## Expected rate

Bounded by the slowest of: HDD combined rate with segments striped across both members (at most about 338 MB/s from the owner's concurrent dd figures), network (about 1,176 MB/s), and the engine. It cannot reach 900 MB/s on these two HDDs.

## What qualification would need

Fault-injection tests for each cut point above; SIGKILL and power-loss style replay with hash verification through a fresh read-only engine; a degraded-member gate; physical NAS measurement by the app owner; and an explicit operator setting, off by default, because it changes what a successful FLUSH depends on.
