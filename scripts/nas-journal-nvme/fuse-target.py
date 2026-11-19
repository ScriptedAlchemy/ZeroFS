#!/usr/bin/env python3
"""Use the pinned synthetic SPDK launcher with a private ZeroFS FUSE mount."""
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent / "app"))
import target


def assert_fuse_mount(fd, args):
    mount_id = target.read_info(fd)["mnt_id"]
    mounts = [
        (left.split(), right.split())
        for left, right in (
            line.split(" - ", 1)
            for line in Path("/proc/self/mountinfo").read_text().splitlines()
        )
    ]
    [(left, right)] = [(left, right) for left, right in mounts if left[0] == mount_id]
    if (
        mount_id != str(args.mount_id)
        or left[3:5] != ["/", str(args.mount)]
        or "rw" not in left[5].split(",")
        or right[0] not in {"fuse", "fuse.zerofs"}
        or right[1] != f"unix:{args.socket}"
        or any(
            other[0] != mount_id and (
                other[2] == left[2] or other[4].startswith(str(args.mount) + "/")
            )
            for other, _ in mounts
        )
    ):
        raise ValueError("backing mount is not the private writable ZeroFS FUSE mount")


if __name__ == "__main__":
    os.umask(0o077)
    args = target.parse_arguments()
    if not args.isolated_synthetic or args.backing_aname is not None or not args.address.is_loopback:
        raise SystemExit("the FUSE adapter requires an isolated synthetic loopback target")
    target.assert_private_mount = assert_fuse_mount
    target.execute(args)
