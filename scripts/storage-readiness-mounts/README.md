# Linux storage readiness mount tests

This isolated test crate compiles the production Linux adapter and its identity
model. It avoids building protocol servers for a mount check. The tests compile
only on Linux, are ignored by default, and require
`ZEROFS_READINESS_MOUNT_TEST=1`. The ext4 case also requires
`ZEROFS_READINESS_LOOP_TEST=1` in a separate privileged container.

Run the tmpfs and bind-mount case with `SYS_ADMIN` and `/dev/fuse` in an isolated
Linux container. Mounts and scratch files belong to the fixture. The crate does
not require a host storage directory or a Docker volume.

```sh
ZEROFS_READINESS_MOUNT_TEST=1 cargo test --lib \
  storage_readiness::integration::real_tmpfs_and_bind_descendants_are_rejected \
  -- --exact --ignored --nocapture
```

The ext4 case creates a 64 MiB sparse image, formats only that owned image,
attaches one loop device, and mounts it in the container. It checks the production
adapter against real `blkid -p`. It then changes a descendant mount after the
check and asserts that a later path-based write reaches the replacement
filesystem. A new preflight detects that mount, but the completed preflight
holds no handle and does not detect the intervening change automatically.

```sh
ZEROFS_READINESS_MOUNT_TEST=1 ZEROFS_READINESS_LOOP_TEST=1 cargo test --lib \
  storage_readiness::integration::real_ext4_identity_and_post_check_mount_change \
  -- --exact --ignored --nocapture
```

The fixture unmounts each owned mount and detaches its own loop device. Remove
the test container and any image pulled for the run afterward. Do not detach
unrelated loop devices or prune shared Docker state.

- Not verified: This harness does not establish the full server's CLI dispatch. The tmpfs
  case exercises the mount-table refusal, because tmpfs has no supported storage
  identity. Bind-mount coverage is limited to the container's mount namespace.
