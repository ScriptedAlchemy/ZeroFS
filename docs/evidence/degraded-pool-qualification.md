# Degraded local pool qualification

The commands in the historical receipt describe completed runs. The current
runner is the Mac Studio's local Docker Desktop. The former remote host is
permanently forbidden for new activity.

Linux host `ubuntu-main`, source `ff25c08e40aa772d283410c17c6874d4e5952946`.
The build and fixtures lived under `/var/tmp/zerofs-degraded.0LKO2HNj`.
The resumed run confirmed the staged crate matched the pushed source and reused
the completed test binary.

```sh
cd /var/tmp/zerofs-degraded.0LKO2HNj/src
export TMPDIR=/var/tmp/zerofs-degraded.0LKO2HNj/tmp
export CARGO_TARGET_DIR=/var/tmp/zerofs-degraded.0LKO2HNj/target
export CARGO_PROFILE_TEST_DEBUG=0 CARGO_PROFILE_DEV_DEBUG=0
~/.cargo/bin/cargo +1.100.0-x86_64-unknown-linux-gnu test --locked --offline -j4 -p zerofs --lib pool_export_tests -- --nocapture > ../exports.log 2>&1
timeout 180s ../target/debug/build/zerofs/d241d4d67b398f2f/out/zerofs-d241d4d67b398f2f --test-threads=4 storage_backend::tests storage_readiness:: pool_object_store:: > ../qualification.log 2>&1
```

```text
test pool_export_tests::sealed_body_corruption_fails_authentication_and_verification ... ok
test pool_export_tests::degraded_cold_start_serves_survivor_and_reports_missing_files ... ok
test result: ok. 4 passed; 0 failed; 0 ignored; 0 measured; 2747 filtered out; finished in 1.85s

test storage_readiness::tests::degraded_readiness_keeps_authority_and_catalog_identity_checks ... ok
test result: ok. 163 passed; 0 failed; 2 ignored; 0 measured; 2586 filtered out; finished in 3.50s
```

The initial `cargo test --locked -j4 -p zerofs --lib storage_backend::tests`
attempt stopped at compile time because of two missing imports and two fixture
files omitted from the scratch checkout. The completed runs exercised the
fixed source and included that filter. The two ignored tests kept their
repository defaults.

The faults were injected into disposable local directories. The tests exercised
the shared affected-path resolver and member verifier at the engine level. The
strict readiness identity test used injected mount facts. No physical disk
was unplugged. The existing export regression used loopback NFS and a Unix
socket for NBD, with no kernel mount.

The private root was removed after both suites exited. That removed the staged
source, build outputs, fixture directory and logs. The fixture directory was
empty before removal and there were no mounts under the root. No NAS access,
deployment, installed binary, persistent service, or container was involved.

## Whole-file placement still open

1. Persist a versioned inode-to-member binding and define how an incoming
   writer declares its remaining file size. Account for append, sparse writes,
   streaming writes without a final size, and existing files that already span
   members.
2. Reserve the entire admitted remainder on the bound member, accounting for
   its quota, free-space reserve, health, concurrent reservations and seal
   overhead. Reject or backpressure unsatisfiable writes without spilling.
3. Partition the frame-packing lanes by destination and persist the destination
   through sealing, conditional upload and writeback replay. Test interrupted
   publication and replay with the target unavailable.
4. Keep compaction, reclaim, and migration within the binding. Any operation
   that changes the binding needs a whole-file relocation protocol. Test loss
   of either disk with files spanning multiple segments and test that all
   files bound to the survivor remain readable after a cold restart.
