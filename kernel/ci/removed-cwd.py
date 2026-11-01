#!/usr/bin/env python3
"""Check POSIX cwd traversal after both a directory and its parent are removed."""

import argparse
import os
from pathlib import Path
import shutil
import tempfile


def check_removed_cwd(mount: Path) -> None:
    # Own one unique child only, and restore cwd before cleaning that child.
    original = os.open(".", os.O_RDONLY | os.O_DIRECTORY)
    root = Path(tempfile.mkdtemp(prefix="zerofs-removed-cwd-", dir=mount.resolve()))
    try:
        parent = root / "parent"
        child = parent / "child"
        child.mkdir(parents=True)
        parent_ino = parent.stat().st_ino
        child_ino = child.stat().st_ino
        root_ino = root.stat().st_ino
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
        os.close(original)
        shutil.rmtree(root)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mount", type=Path)
    check_removed_cwd(parser.parse_args().mount)
