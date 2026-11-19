# Durable pool segment targets

The implementation adds the pool-side part of placement routing. A conditional
put can bind a canonical segment key to an exact configured member. The catalog
commits the binding before admission, so a failed admission retains the target
even if a retry arrives without the request extension. Multipart reservations
participate in the same rule. A conflicting in-flight reservation prevents a
new binding. Migration to a different member is rejected.

This does not activate whole-file placement. Inode creation still needs
transactional binding, placement cohorts need a durable budget for the
admitted remainder, and the packing and compaction paths need to preserve the
member on every generated segment. A writeback entry also needs to recover
the target when it is replayed before the first pool put. Before exposing the
mode, startup needs to fence software versions that ignore segment targets.
There is no whole-file loss or hash-integrity qualification in this cut.

## Runner

Zack's Mac Studio reported `arm64`. Docker Desktop context `desktop-linux`
used its local Unix socket and reported `linux/aarch64`. The pre-existing
`rust:1.100.0-bookworm` image ran as native `linux/arm64`, without emulation.

```sh
docker --context desktop-linux run --rm --pull=never --platform linux/arm64 \
  --name zerofs-affinity-1ca7e2ce --cpus 4 --memory 8g \
  --mount "type=bind,source=$PWD,target=/checkout,readonly" \
  --env CARGO_TARGET_DIR=/scratch/target --env TMPDIR=/scratch/tmp \
  --env CARGO_PROFILE_TEST_DEBUG=0 --env CARGO_PROFILE_DEV_DEBUG=0 \
  rust:1.100.0-bookworm bash -euc '
    mkdir -p /scratch/tmp
    apt-get update -qq
    apt-get install -y -qq protobuf-compiler
    cd /checkout/zerofs
    uname -m
    rustc --version
    protoc --version
    cargo test --locked --no-default-features -j4 -p zerofs --lib -- \
      --test-threads=4 pool_export_tests storage_backend::tests \
      storage_readiness:: pool_object_store::
  '
```

## Result

```text
test pool_object_store::tests::quarantined_segment_target_refuses_creation_without_spilling ... ok
test pool_object_store::tests::segment_target_cannot_override_an_inflight_multipart_reservation ... ok
test pool_object_store::tests::segment_target_survives_capacity_refusal_reopen_and_replay ... ok
test result: FAILED. 169 passed; 1 failed; 2 ignored; 0 measured; 2582 filtered out; finished in 20.58s
```

The existing `disk_health_crash_reopen_preserves_one_verified_placement` test
failed at `before_source_delete`. The committed placement read back correctly,
and status still reported one pending cleanup operation where the test expected
zero. Startup recovery has a bounded cleanup deadline. The cause of the
remaining operation in this run was not verified.

Docker removed the named container and its scratch on exit. The run created no
named volume, network, or image. No remote build host or NAS was used.
