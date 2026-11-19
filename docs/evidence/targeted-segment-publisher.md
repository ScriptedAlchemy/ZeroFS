# Targeted generated-segment publication

The initial parallel run used `de4084138a91e8d0f149a4590c74624afcbfa3d6`.
The serial qualification used `28e7dd0489f709782a54311cafa93064be43adb8`.
Zack's Mac Studio reported `arm64`. The local Docker Desktop
`desktop-linux` context used its local Unix socket, reported Linux
`aarch64`, and ran the pre-existing `rust:1.100.0-bookworm` image as native
`linux/arm64` without emulation.

```sh
docker --context desktop-linux run --rm --pull=never --platform linux/arm64 \
  --name zerofs-affinity-publisher-pr86 --cpus 4 --memory 8g \
  --mount "type=bind,source=$PWD,target=/checkout,readonly" \
  --env CARGO_TARGET_DIR=/scratch/target --env TMPDIR=/scratch/tmp \
  --env CARGO_PROFILE_TEST_DEBUG=0 --env CARGO_PROFILE_DEV_DEBUG=0 \
  rust:1.100.0-bookworm bash -euc '
mkdir -p /scratch/tmp
apt-get update -qq
apt-get install -y -qq protobuf-compiler
cd /checkout/zerofs
cargo test --locked --no-default-features -j4 -p zerofs --lib -- --test-threads=4 segment_store::tests pool_export_tests storage_backend::tests storage_readiness:: pool_object_store::
'
```

The serial qualification used the same command with container
`zerofs-affinity-publisher-pr86-serial` and `--test-threads=1`.

The initial parallel run reported 199 passed, 1 failed, and 2 ignored.
`native_staging_cleanup_failure_keeps_capacity_charged_and_pull_blocked`
failed. The assertion output was not retained, so the cause is unverified.

The generated-segment publisher forwards an explicit member target through
single PUT and multipart initiation, including fallback. The replay fixture
enters through that publisher, flushes to the journal while remote replay is
paused, and then closes and reopens the pool and writeback. The serial run adds
a full target to that fixture and checks that remote progress stays at zero,
the bytes remain readable from the journal, and freeing capacity on the bound
member lets the segment publish there. A collision with a segment on a
different member is a refusal even when its bytes match.

## Whole-file qualification status

| Item | Coverage |
| --- | --- |
| 1. Concurrent multi-segment files, member-pure packing | Pending. There is no filesystem caller that derives the target from a durable inode binding. |
| 2. Concurrent append and overwrite | Pending for bound files. Existing append lanes still hash the inode and can fall back to any idle lane. |
| 3. Mid-file ENOSPC and blocked drain | Lower-level segment admission refuses spill on capacity or quarantine. File creation and growth lack an atomic per-file encoded-space claim that transfers to generated segments without double charging. File-level FLUSH, retained bytes after ENOSPC, and other-member progress in this scenario remain untested. |
| 4. Restart and replay | Explicit segment target and body are tested across cold journal and pool reopen before the first pool put. File-level restart in the admission, growth, and compaction cases remains pending. |
| 5. Compaction and GC | Pending. The compactor currently gathers a cross-inode batch and calls an untargeted seal before conditionally repointing extents. |
| 6. Loss of either member | Existing degraded and affected-report regressions cover the directory reference backend. The two-way loss matrix for concurrent multi-segment bound files remains pending. |

No whole-file mode is enabled. First-eligible and round-robin production
callers still use the existing untargeted path. The raw backend still needs
member identity from the device label, capacity and reserve from device
geometry, and reclamation. The append-only raw log charges overwrites and
tombstones without recovering space, so it cannot currently satisfy
reclaim-based admission or binding-preserving compaction.

## Serial result and cleanup

```text
test pool_object_store::tests::targeted_seal_refuses_identical_bytes_already_on_another_member ... ok
test pool_object_store::tests::writeback_replays_segment_to_persisted_member_before_any_pool_put ... ok
test result: ok. 200 passed; 0 failed; 2 ignored; 0 measured; 2555 filtered out; finished in 63.63s
```

The existing staging-cleanup failure did not reproduce serially. Its cause
remains unverified. No assertions were skipped or changed in that fixture.

Docker removed both named containers and their writable layers, including
`/scratch/target`, `/scratch/tmp`, and the formatter copies at `/scratch/fmt`
and `/scratch/tests.rs` in their respective containers. Both runs reused the
pre-existing image and default Docker networking. They created no named
images, volumes, networks, or host scratch paths. No NAS or remote build host
was used.
