from __future__ import annotations

import shutil
import stat
from pathlib import Path


def present(path: Path) -> bool:
    try:
        path.lstat()
    except FileNotFoundError:
        return False
    return True


def unlink_file(path: Path) -> None:
    try:
        path.unlink()
    except FileNotFoundError:
        return


def remove_empty_directory(path: Path) -> None:
    try:
        path.rmdir()
    except FileNotFoundError:
        return


def remove_tree(path: Path) -> None:
    try:
        metadata = path.lstat()
    except FileNotFoundError:
        return
    if not stat.S_ISDIR(metadata.st_mode) or stat.S_ISLNK(metadata.st_mode):
        raise RuntimeError(f"owned resource is not a real directory: {path}")
    shutil.rmtree(path)


def assert_absent(paths: list[Path]) -> None:
    remaining = [str(path) for path in paths if present(path)]
    if remaining:
        raise RuntimeError(f"owned resources remain after cleanup: {remaining}")
