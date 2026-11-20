# ENOSPC at WRITE on a full pool

Engine source `cb24706e` was built as a Cargo release binary on native x86-64
`ubuntu-main` in `rust:1.100.0-bookworm`, with no emulation. The binary SHA-256
is `6b623ee98b67c22741abd76dc594e324e063a7e64db227a03e4c9c0705610041`. The
fixture script SHA-256 is
`50bbf3cd14c87d9a2f7ec1bf9da692a5326f82f497e55df2179dfe0c93521c06`. Host kernel
7.0.0-28-generic. The stock `9p`, `9pnet` and `9pnet_fd` modules were already
loaded, and nothing ran `modprobe`.

## Mounted receipts

`scripts/test-pool-delete-reuse.py` ran six cases at once in one privileged,
disposable container, each with its own tmpfs members. Each member has a
50,000,000-byte quota and a 1,000,000-byte reserve, and member B's
filesystem stays below its reserve. A flushed 32 MiB file is deleted. A
distinct 32 MiB replacement is written on one descriptor in 1 MiB requests.

| Client | Pin | Exit | Acknowledged (bytes) | Refused WRITE (ms) | fsync after refusal (ms) | Pinned (s) | Release to freed (s) | Rewrite fsync (ms) | Member A used before → after (bytes) |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| FUSE | none | 0 | 14,680,064 | 3.0 | 33.7 | — | 931 | 39.1 | 49,798,999 → 15,156,425 |
| FUSE | open reader | 0 | 14,680,064 | 3.0 | 34.6 | 1,080 | 71 | 40.3 | 49,836,918 → 15,167,969 |
| FUSE | checkpoint | 0 | 14,680,064 | 2.3 | 33.7 | 1,080 | 66 | 41.4 | 49,715,325 → 15,167,969 |
| Kernel 9P | none | 0 | 14,680,064 | 1.5 | 39.6 | — | 932 | 39.9 | 49,836,918 → 15,167,969 |
| Kernel 9P | open reader | 0 | 14,680,064 | 1.2 | 39.5 | 1,080 | 72 | 40.9 | 49,836,918 → 15,167,969 |
| Kernel 9P | checkpoint | 0 | 14,680,064 | 1.2 | 39.2 | 1,080 | 66 | 42.1 | 49,836,918 → 15,167,969 |

In every case the WRITE that could not be placed answered ENOSPC. The
acknowledged prefix then fsynced and read back exactly. After the unlink, or
after the pin was released, member A's used bytes dropped by at least the
deleted body. The rest of the replacement was then accepted on the same
descriptor, one attempt per WRITE, and fsynced. For the checkpoint, "released"
means the checkpoint was deleted after a restart that read the checkpoint view;
the descriptor was reopened at the acknowledged offset. While pinned, member
A's used bytes did not change for 1,080 s, a WRITE stayed refused, and the
reader or the checkpoint view returned the deleted bytes. SIGKILL and restart
then found the earlier file and the whole replacement hash-identical, with the
deleted name gone. Every case removed its mounts, clients, servers and roots.
[Per-case receipts](mounted-receipts.jsonl)

Times are fixture measurements on tmpfs, not device performance.

## Why freeing took 931 s

A temporary log line, never committed, printed segment GC's deletion deadline.
The deleted segment was first seen dead at 03:36:37 UTC with a deadline of
03:51:59 UTC. No named checkpoint existed. SlateDB writes an unnamed checkpoint
with a 15-minute lifetime on every metadata compaction
(`compactor_state_protocols.rs`, `write_manifest`). Segment GC treats every
ephemeral checkpoint as a reader, so it holds a dead segment until the newest
one it saw expires, plus 30 s of clock skew: 900 s + 30 s. The pinned cases
freed 66 s to 72 s after release, because that checkpoint had already expired
during the pin. This is the existing reclamation policy, not admission. Read
replicas use unnamed ephemeral checkpoints too, so this change does not
exempt them.

## Drain ordering

`a_deletion_stays_behind_an_earlier_create_the_pool_refuses` proves the
suspected writeback deadlock condition. While the pool refuses an earlier
segment create, the journal does not send a later deletion of another segment
to the pool, even when that deletion would make the room. The create retried
at least three times, the remote watermark stayed at 0 and the deleted object
stayed on the backend. Both drained only after room came from outside.
Sending the deletion first would leave the remote copy referencing a deleted
object until the journal catches up. That breaks recovery from the remote or
from a lagging metadata copy, so the ordering stays. Admission avoids the
condition instead. Every journaled segment keeps its lease until the drain
places it, and a restart leases it again. So the frontier create always has the
room it was promised. `the_writeback_journal_places_admitted_segments_and_drains_deletions`
and `the_nas_package_stack_refuses_at_write_and_recovers_after_an_unlink` run
the whole path, and `a_restart_leases_segments_the_journal_has_not_placed`
covers the restart.

## Library checks

`cargo fmt --all -- --check` and `cargo clippy --workspace --all-targets -- -D
warnings` passed. `cargo test --lib` passed 2,842 tests, with 0 failed and 55
ignored, before the codec bound test was added. With admission disabled in the
fixture, 9 of the 13 full-pool tests fail. With the journal re-lease removed,
the restart test fails.

No NAS, host module, package or service changes were made.
