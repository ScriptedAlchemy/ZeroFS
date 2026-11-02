#!/usr/bin/env python3
"""Check POSIX cwd traversal after both a directory and its parent are removed."""

import argparse
import os
from pathlib import Path
import shutil
import tempfile


def check_removed_cwd(mount: Path, *, keep_directory_fds: bool = False) -> None:
    # Own one unique child only, and restore cwd before cleaning that child.
    original = os.open(".", os.O_RDONLY | os.O_DIRECTORY)
    root = Path(tempfile.mkdtemp(prefix="zerofs-removed-cwd-", dir=mount.resolve()))
    directory_fds = []
    try:
        parent = root / "parent"
        child = parent / "child"
        child.mkdir(parents=True)
        parent_ino = parent.stat().st_ino
        child_ino = child.stat().st_ino
        root_ino = root.stat().st_ino
        if keep_directory_fds:
            # Linux v9fs_remove() clunks the removed dentry's walked fids,
            # even when a process holds cwd there. Only opened fids survive
            # and remain available to v9fs_fid_find_inode(). Keep both
            # directory handles for that client; the native regression
            # deliberately exercises cwd references alone by default.
            directory_fds.append(os.open(parent, os.O_RDONLY | os.O_DIRECTORY))
            directory_fds.append(os.open(child, os.O_RDONLY | os.O_DIRECTORY))
        os.chdir(child)
        child.rmdir()
        parent.rmdir()

        # The original names stay absent. Held VFS references still permit
        # searching "." and walking ".." through the removed parent to root.
        assert not parent.exists()
        os.chdir(".")
        assert os.stat(".").st_ino == child_ino
        os.chdir("..")
        assert os.stat(".").st_ino == parent_ino
        os.chdir("..")
        assert os.stat(".").st_ino == root_ino
        assert not parent.exists()
        print("removed cwd and parent traversal passed")
    finally:
        os.fchdir(original)
        for fd in reversed(directory_fds):
            os.close(fd)
        os.close(original)
        shutil.rmtree(root)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mount", type=Path)
    parser.add_argument(
        "--keep-directory-fds",
        action="store_true",
        help="retain opened parent/child handles for the Linux v9fs client",
    )
    args = parser.parse_args()
    check_removed_cwd(args.mount, keep_directory_fds=args.keep_directory_fds)
