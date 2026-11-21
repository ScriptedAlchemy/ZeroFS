# FLUSH progress after an unlink on a full pool

Engine source `12cdadcb` (`integration/all-open-prs`) was built as a Cargo release binary on native x86-64 `ubuntu-main` in `rust:1.100.0-bookworm`. The binary SHA-256 is `ffcc52a02a77a9a6d0003eace5c2276becbbc96e4060c661c6c4ab61b47234bd`. The fixture script SHA-256 is `b423a6a1c56418cebb8e28f668acdeb96ae870470af2ebce3a37821674df561e`. Host kernel 7.0.0-28-generic. The branch changes only tests and documentation, so the binary is the base engine.

## Mounted receipts

`scripts/test-pool-delete-reuse.py` ran 12 cases in one privileged disposable container. Each member has a 50,000,000-byte quota and a 1,000,000-byte reserve. Member A holds a flushed 32 MiB file, and ballast keeps member B below its reserve. A distinct 32 MiB replacement is staged on one descriptor. Its first fsync must return ENOSPC, and the engine log gives the refused seal size.

With `--room-from member-b`, the same descriptor must still get ENOSPC after the unlink. The clock then starts, the ballast is removed, and a fresh `statvfs` must show room above reserve for the whole seal. A single fsync on the same descriptor must succeed within 5 s. The server and client are then killed. After restart, the replacement and the earlier sentinel must hash correctly, and the deleted name must be gone. An open reader must read the deleted bytes after the fsync. A checkpoint view must return them after a further restart.

| Client | Room from | Pin | Exit | fsync errno | Unlink or release to fsync (ms) | Refused seal (bytes) | B room above reserve (bytes) |
| --- | --- | --- | ---: | --- | ---: | ---: | ---: |
| FUSE | member-b | none | 0 | none | 65.0 | 34,565,670 | 66,104,768 |
| FUSE | member-b | open reader | 0 | none | 63.2 | 34,576,826 | 66,104,768 |
| FUSE | member-b | checkpoint | 0 | none | 64.9 | 34,569,358 | 66,104,768 |
| Kernel 9P | member-b | none | 0 | none | 65.2 | 34,668,950 | 66,104,768 |
| Kernel 9P | member-b | open reader | 0 | none | 65.6 | 34,668,950 | 66,104,768 |
| Kernel 9P | member-b | checkpoint | 0 | none | 65.2 | 34,668,950 | 66,104,768 |
| FUSE | unlink | none | **1** | ENOSPC | 1.8 | 34,565,670 | — |
| FUSE | unlink | open reader | 0 | ENOSPC | 1.5 | 34,576,826 | — |
| FUSE | unlink | checkpoint | 0 | ENOSPC | 1.9 | 34,576,826 | — |
| Kernel 9P | unlink | none | **1** | ENOSPC | 1.1 | 34,668,950 | — |
| Kernel 9P | unlink | open reader | 0 | ENOSPC | 0.8 | 34,668,950 | — |
| Kernel 9P | unlink | checkpoint | 0 | ENOSPC | 1.1 | 34,668,950 | — |

Times are fixture measurements on tmpfs, not device performance. Every case verified the sentinel after SIGKILL. The unlink-only cases also verified that the old name, if it returned, held the deleted bytes. All 12 fixtures removed their mounts, clients, servers and roots. [Per-case receipts](mounted-receipts.jsonl)

The approved contract passes on both clients. The unlink-only unprotected case stays red on both. It asks the unlink alone to make room, and the engine cannot do that today.

## Why an unlink alone cannot make room

A temporary, uncommitted probe test produced [unlink-only-probe.txt](unlink-only-probe.txt). It used the `pool_full_tests` fixture with the same quotas, a 24 MiB deleted file and a 32 MiB replacement. After the unlink it ran five rounds of tombstone GC, segment reclaim with the horizon one hour in the past, and the same fsync. Every reclaim and every fsync returned `NoSpace`. Member A stayed at 25,214,053 used bytes, and one 33,618,396-byte generation stayed retained.

The source shows a cycle:

1. The replacement's frame locations are already committed to the in-memory database.
2. `FlushCoordinator` runs the seal hook first and skips `db.flush()` when it fails, so the unlink never becomes durable.
3. `reclaim_segments_gated` starts with the same `seal_open()` and returns its error before it scans anything.
4. The deleted body is removed only by a reclaim that follows a durable unlink, so its quota never frees.

Breaking the cycle needs a product decision. Three designs were reviewed on 2026-11-18, and each changes a contract:

- A reclaim lien would let a reservation count bytes made dead by the same flush. It works only where the quota is below physical space, and it redefines `capacity_gb`. It gives nothing on raw devices, where capacity is physical.
- Write-time admission would reserve capacity before a write's references are committed. ENOSPC would then arrive on WRITE instead of FLUSH. The approved contract requires every WRITE to be acknowledged first.
- A durable unlink intent with replay would need a new recovery protocol, coordination with checkpoint creation, and a separate guard for reads in flight.

## Library checks

`cargo fmt --all -- --check`, `cargo clippy --workspace --all-targets -- -D warnings` and `cargo test --lib` passed, with 2,836 passed, 0 failed and 55 ignored. Two tests are new. `fsync_after_unlink_succeeds_once_a_member_has_room` also checks that the deleted body is reclaimed afterwards without a restart. `pinned_bytes_survive_the_fsync_that_follows_an_unlink` holds a 9P reader across the unlink and then applies the persistent-checkpoint reclaim gate. The deleted bytes stay readable and charged after the fsync, and they are freed only when neither pin remains.

No NAS, host module, package or service changes were made. The stock `9p`, `9pnet` and `9pnet_fd` modules were already loaded on the builder.
