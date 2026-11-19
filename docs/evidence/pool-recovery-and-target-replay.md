# Pool recovery and target replay

The base comparison uses the archived tree at `63ad64ce`. Both runs use local
Docker Desktop on Zack's arm64 Mac Studio. The `desktop-linux` context points
at the local Unix socket. The engine reports `linux/aarch64`. The image runs
as native `linux/arm64` without emulation.

```sh
git archive 63ad64ce | docker --context desktop-linux run --rm -i --pull=never \
  --platform linux/arm64 --name zerofs-pr86-crash-63ad64ce --cpus 4 --memory 8g \
  --mount "type=bind,source=$PWD,target=/checkout,readonly" \
  --env CARGO_TARGET_DIR=/scratch/target --env TMPDIR=/scratch/tmp \
  --env CARGO_PROFILE_TEST_DEBUG=0 --env CARGO_PROFILE_DEV_DEBUG=0 \
  rust:1.100.0-bookworm bash -c '
set -eu
mkdir -p /scratch/base /scratch/tmp
tar -xf - -C /scratch/base
apt-get update -qq
apt-get install -y -qq protobuf-compiler
uname -m
rustc --version
protoc --version
cd /scratch/base/zerofs
set +e
cargo test --locked --no-default-features -j4 -p zerofs --lib -- --exact --nocapture pool_object_store::tests::disk_health_crash_reopen_preserves_one_verified_placement
printf "BASE_EXIT=%s\n" "$?"
set -e
cd /checkout/zerofs
cargo test --locked --no-default-features -j4 -p zerofs --lib -- --test-threads=4 pool_export_tests storage_backend::tests storage_readiness:: pool_object_store::
'
```

The clean base run passed with one test and zero failures. Running the same
unmodified base binary with a held source mutation lock reproduced the prior
failure. A Python helper in the container watched the crash-test subprocesses,
acquired an exclusive flock on `hdd-a`'s `.zerofs-local-mutations#0` for the
`before_source_delete` child, and released it two seconds after the child
exited. This forced cleanup beyond its one-second startup wait.

```sh
/scratch/target/debug/build/zerofs/623728ef76337a58/out/zerofs-623728ef76337a58 \
  --exact --nocapture pool_object_store::tests::disk_health_crash_reopen_preserves_one_verified_placement
```

```text
BASE_NATIVE_SOURCE_LOCK_HELD
assertion `left == right` failed: cutoff before_source_delete
  left: 1
 right: 0
test result: FAILED. 0 passed; 1 failed; 0 ignored; 0 measured; 2747 filtered out
BASE_WITH_LOCK_EXIT=101; held=True
```

The base wraps local garbage cleanup directly in a one-second startup wait.
If the wait expires, it drops the async cleanup while the native worker may
still finish deleting the body. That abandons the catalog settlement step.
The fix tracks the native delete, logical-key guard and catalog settlement as
one task. The startup wait keeps its deadline. The original crash assertion
is unchanged, and a separate regression uses the native mutation lock to
force cleanup beyond the deadline.

For an explicitly targeted generated segment, writeback commits the catalog
target before acknowledging journal admission. A cold-replay test closes both
stores before any pool put and replays with only the durable journal and
catalog. It checks the selected member and the payload.

The filesystem still has no whole-file placement mode. File creation currently
creates a zero-length inode and lacks a final-size admission contract. The
extent writer selects a lane by inode hash and a lane can mix inodes. The
compactor builds new segments from source frames without a member binding.
Enabling affinity requires transactional creation-time placement, a durable
reservation and growth contract, member-aware lanes, and compactor/reclaimer
preservation. The two-member multi-segment loss and affected-path qualification
therefore remains open.

## Candidate result

Rust source at `50a2a7ee`. Rust 1.100.0 with system Protobuf 3.21.12.

```text
test pool_object_store::tests::native_garbage_reconciles_after_startup_wait_times_out ... ok
test pool_object_store::tests::disk_health_crash_reopen_preserves_one_verified_placement ... ok
test pool_object_store::tests::writeback_replays_segment_to_persisted_member_before_any_pool_put ... ok
test result: ok. 172 passed; 0 failed; 2 ignored; 0 measured; 2582 filtered out; finished in 26.61s
```

Docker removed `zerofs-pr86-crash-63ad64ce` and its writable scratch layer,
including the archived base, target artifacts, and fault fixtures. The run
created no named volumes, networks, or images. No remote host or NAS was used.
