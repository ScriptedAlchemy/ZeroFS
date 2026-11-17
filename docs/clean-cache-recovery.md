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
