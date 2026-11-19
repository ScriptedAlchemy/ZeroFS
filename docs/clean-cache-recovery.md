# Clean raw-parts cache recovery

The raw-parts cache starts empty after every ZeroFS server restart. It refills
from the configured object store as clients read data. Within a running process,
the cache serves evicted RAM entries from the SSD tier. Failed frame
authentication invalidates cached parts before an authoritative refetch and
authenticated repair.

This policy applies to disposable cached copies of object bytes. It does not
discard authoritative file contents or filesystem metadata. The optional dirty
writeback journal and its recovery retain their existing durability behavior;
the decoded metadata cache retains its existing recovery configuration.

## Why startup recovery is disabled

The pinned Foyer 0.22.3 dependency can lose deletion records when reopening a
multi-page tombstone log and when its bounded log wraps while older data records
remain on disk. Replaying that data can resurrect an invalidated cache entry,
including bytes previously rejected by frame authentication. A cursor-only fix
does not address the log's retention problem.

The production raw-parts builder therefore explicitly selects
`RecoverMode::None` and disables the tombstone log. It starts with an empty disk
index and makes the configured disk capacity available for new cache entries.
Existing partition files can be reused; compatible startup does not require
deleting and recreating every file. Startup does not recover bytes left in
those files as readable cache entries. Deletes and authenticated repairs still invalidate
the active process's cache index.

The regression tests exercise disk reads and repair inside one process, removal
batches spanning the former tombstone page boundaries, a removal count exceeding
the former log's capacity, and repeated restarts. They also check that live
control entries are absent after restart, so the cold start is asserted rather
than incidental.

## Upgrade, rollback, and geometry changes

Current parts-cache files live in `parts_cache_v1` below the bucket's cache
namespace. Older binaries use `parts_cache`. Keeping separate directories
prevents a normal rollback from overwriting files belonging to the current
policy. Startup reclaims canonical legacy Foyer partitions while preserving
unrelated files.

A layout marker records the recovery policy, tombstone setting, block size, and
disk capacity. Startup resets an incompatible layout under the existing
exclusive cache-root ownership lock. It invalidates and syncs the old marker
before removing partitions, and it syncs the removals before publishing the new
marker. Startup therefore retries an interrupted reset even if configuration
rolls back to the earlier geometry. It rejects symlinked or nonregular markers. No manual
cache migration is required.

## Defined padding

foyer pads every cache entry to a 4 KiB page and writes each blob index as a
whole page. foyer-storage 0.22.3 cleared neither the padding nor the index tail
past its last record, so they reached the cache disk holding whatever their
I/O buffers held before: freed heap memory of the server process on first use,
and bytes of earlier entries after that. The patched foyer-storage in
`zerofs/vendor/` writes zeros there.

In a run of the shipped server, about one padding byte in six was non-zero,
and nearly all of those bytes were copies of earlier cache entries. jemalloc
hands freed memory back without clearing it, so heap contents such as
decrypted data could also land there; that run found no plaintext, password,
credential or key-derivation memory, but it does not rule them out.

Caches written before the patch are removed at startup. The parts-cache layout
marker records `padding=zeroed`, so a `parts_cache_v1` directory without it is
reset like any other incompatible layout. The decoded-block cache moved from
`hybrid_cache` to `hybrid_cache_v1`, because older binaries record no layout
there; startup removes the partitions left in `hybrid_cache`. Removing a file
does not overwrite its blocks on the device, so a disk that held an older cache
can still carry those bytes until they are reused or the disk is wiped.

### Checking a cache directory

`zerofs debug scan-cache-files` reads the cache files a server left behind and
checks the bytes that carry no data: in every blob, the index tail past the
last record, and in every entry, the padding from the end of the entry to the
next 4 KiB page. It finds the raw-parts cache (`parts_cache_v1`) and the
decoded-block cache (`hybrid_cache_v1`) under the path it is given, which can
be the `[cache] dir` from the config, one `bucket_...` namespace inside it, or
one cache directory.

Stop the server first, then run:

```
zerofs debug scan-cache-files /var/cache/zerofs
```

Add `--json` for a machine-readable report. The scan needs no password or key,
because nothing is decrypted; the command takes none. It opens files read-only
and changes, repairs or deletes nothing. It refuses a namespace whose
ownership lock a running server holds, because a blob being written can look
torn. `--live` scans anyway and labels the result a snapshot. A server that
starts during an offline scan cannot take the existing namespace lock until
the scan finishes. Copied caches that have no ownership lock remain readable;
pass `--live` to explicitly scan them without that exclusion. An offline scan
refuses a missing ownership lock and never creates one.

A recognized cache path that is a symlink or is not a directory stops the scan.
Discovery also refuses symlinks where a child could be a cache namespace;
scan that target directory explicitly instead. Neither a cache nor a possible
namespace is silently omitted from a clean result. Unrelated symlinks inside
an already discovered namespace are ignored, and no directory symlink is followed.

For each cache the report gives partitions, blobs, entries, and the index-tail,
padding and past-the-last-blob bytes scanned and found non-zero. A clean
result reads `Result: clean: every index-tail, padding and past-the-last-blob
byte is zero, and every cache had entries to scan (exit status 0).`

The scan checks that each recognized blob's index and entry headers agree on
its bounds, lengths and identities. If a recognized index names an incomplete
or inconsistent entry, the report counts that partition as incomplete and
does not call a clean prefix proof that the cache is clean. Files of another
partition size are counted separately without being loaded into memory.

The blob chain ends at the first page that does not start a valid blob. No
index describes the bytes from there to the end of the block, so every one of
them must be zero; a single non-zero byte makes that partition incomplete.
foyer writes a reused block again from offset zero. Upstream foyer zeroes only
the block's first page first, so the earlier generation's entries stayed past
the new chain, with nothing left to say which of their bytes were padding. The
patched foyer-storage punches the whole block instead, each time it reclaims
one and, at startup, for every block it will write again: with recovery
disabled, as for the raw-parts cache, that is every block. A block of the
decoded-block cache that a build without this change reused and that still
holds entries keeps its stale tail, and scans incomplete, until foyer reclaims
it. On a filesystem that cannot punch holes foyer zeroes only the first page,
as upstream does, and its reused blocks scan incomplete. Neither current cache
layout uses auxiliary tombstone partitions, so a partition of another size
makes the scan incomplete.

| Exit status | Meaning |
|---|---|
| 0 | Every scanned byte is zero, and every cache had entries to scan. |
| 3 | Some index-tail or padding bytes are not zero. |
| 4 | A cache had no entries on disk, was not scanned, had a partition of another size, or had a non-zero byte past a block's last blob. An incomplete scan is not evidence that the cache is clean. |
| 1 | The scan could not run, for example because a server holds the cache. |

A cache written by an earlier build (a `hybrid_cache` directory, or a
`parts_cache_v1` whose layout marker lacks `padding=zeroed`) is reported as
written by an earlier build and not covered by the fix, and is not scanned.
`--legacy` counts its bytes too. A server with the fix removes such a cache
when it starts.

The scan checks the files as they are now. It does not show that no secret
was ever written by an earlier build, and a cache deleted before the scan, or
removed at startup, may still sit in the disk's free blocks.

## Operational and benchmark impact

After a restart, initial reads can generate additional backend traffic and take
longer while the raw-parts cache fills. The configured SSD cache budget is still
used during the process's lifetime; a cold index does not disable the disk tier.
Plan restart performance around a cold raw-parts cache.

To measure a clean SSD hit, populate the cache and evict its RAM tier while
keeping the process running. A restart cannot establish a warm raw-parts SSD
measurement under this policy. Historical benchmark plans that use a restart
while preserving SSD files as proof of a clean-SSD hit do not provide that proof
with this configuration.
