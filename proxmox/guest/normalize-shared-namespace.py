#!/usr/bin/env python3

from __future__ import annotations

import os
import stat
import subprocess
from pathlib import Path


NAMESPACE_ROOT = Path("/mnt/zerofs-files-raw")
NBD_CONTROL_DIRECTORY = NAMESPACE_ROOT / ".nbd"
SHARED_UID = 501
SHARED_GID = 20


def _add_shared_write(path: Path, *, uid: int, gid: int) -> None:
    metadata = path.lstat()
    os.lchown(path, uid, gid)
    if stat.S_ISLNK(metadata.st_mode):
        return
    mode = stat.S_IMODE(metadata.st_mode)
    if stat.S_ISDIR(metadata.st_mode):
        path.chmod(mode | 0o770)
    elif stat.S_ISREG(metadata.st_mode):
        shared_execute = 0o110 if mode & 0o111 else 0
        path.chmod(mode | 0o660 | shared_execute)


def normalize_namespace(
    root: Path, *, uid: int = SHARED_UID, gid: int = SHARED_GID
) -> None:
    _add_shared_write(root, uid=uid, gid=gid)
    for directory, names, files in os.walk(root, topdown=True, followlinks=False):
        current = Path(directory)
        if current == root and ".nbd" in names:
            names.remove(".nbd")
        for name in (*names, *files):
            path = current / name
            try:
                _add_shared_write(path, uid=uid, gid=gid)
            except FileNotFoundError:
                continue


def _mount_record(path: Path) -> tuple[str, str, set[str]]:
    result = subprocess.run(
        ["/usr/bin/findmnt", "-M", str(path), "-n", "-o", "SOURCE,FSTYPE,OPTIONS"],
        check=True,
        capture_output=True,
        text=True,
    )
    source, filesystem, options = result.stdout.strip().split(maxsplit=2)
    return source, filesystem, set(options.split(","))


def validate_production_mounts() -> None:
    source, filesystem, options = _mount_record(NAMESPACE_ROOT)
    if source != "10.10.10.55:/" or filesystem != "nfs" or "rw" not in options:
        raise RuntimeError("raw ZeroFS namespace is not the expected read-write NFS mount")

    source, filesystem, options = _mount_record(NBD_CONTROL_DIRECTORY)
    if source != "10.10.10.55:/.nbd" or filesystem != "nfs" or "ro" not in options:
        raise RuntimeError("ZeroFS .nbd control directory is not protected read-only")


def main() -> None:
    if os.geteuid() != 0:
        raise RuntimeError("shared namespace normalization must run as root")
    validate_production_mounts()
    normalize_namespace(NAMESPACE_ROOT)


if __name__ == "__main__":
    main()
