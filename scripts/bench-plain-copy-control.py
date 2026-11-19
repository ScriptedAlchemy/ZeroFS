#!/usr/bin/env python3
"""No-engine control for the sustained-copy fixture.

Writes the same deterministic incompressible stream as
bench-nas-journal-sustained.py into a plain directory on the same scratch
mount, without ZeroFS:

  buffered      depth-N concurrent pwrite of fixed-size blocks, one final fsync
  journal-like  64 MiB containers, each written then fsynced with its directory,
                two containers in flight (the bounded journal's sync pattern)

It reports MB/s through the last write and through the final fsync, verifies
the file's SHA-256 by reading it back, and removes everything it created.
The read-back may be served from the page cache.
"""

import argparse
from concurrent.futures import ThreadPoolExecutor
import hashlib
import importlib
import json
import os
from pathlib import Path
import shutil
import sys
import tempfile
import time

sys.dont_write_bytecode = True
bench = importlib.import_module("bench-nas-journal-sustained")
MIB = 1024 * 1024
CONTAINER = 64 * MIB


def buffered(path, pattern, depth):
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        offsets = range(0, pattern.size, pattern.chunk)
        started = time.monotonic()
        with ThreadPoolExecutor(max_workers=depth) as pool:
            for written in pool.map(lambda o: os.pwrite(fd, pattern.block(o), o), offsets):
                assert written > 0
        written_at = time.monotonic()
        os.fsync(fd)
        synced_at = time.monotonic()
    finally:
        os.close(fd)
    return started, written_at, synced_at


def journal_like(directory, pattern, depth):
    def container(index):
        name = directory / f"container-{index:06d}"
        fd = os.open(name, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        try:
            start = index * CONTAINER
            for offset in range(start, min(start + CONTAINER, pattern.size), pattern.chunk):
                os.pwrite(fd, pattern.block(offset), offset - start)
            os.fsync(fd)
        finally:
            os.close(fd)
        dirfd = os.open(directory, os.O_RDONLY)
        try:
            os.fsync(dirfd)
        finally:
            os.close(dirfd)

    count = -(-pattern.size // CONTAINER)
    started = time.monotonic()
    with ThreadPoolExecutor(max_workers=2) as pool:
        list(pool.map(container, range(count)))
    done = time.monotonic()
    return started, done, done


def read_back(paths):
    digest = hashlib.sha256()
    for path in paths:
        with open(path, "rb") as source:
            while block := source.read(8 * MIB):
                digest.update(block)
    return digest.hexdigest()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--scratch", type=Path, required=True)
    parser.add_argument("--evidence", type=Path, required=True)
    parser.add_argument("--file-mib", type=int, required=True)
    parser.add_argument("--write-kib", type=int, default=1024)
    parser.add_argument("--depth", type=int, default=8)
    parser.add_argument("--mode", choices=("buffered", "journal-like"), required=True)
    args = parser.parse_args()
    args.evidence.mkdir(mode=0o700)
    root = Path(tempfile.mkdtemp(prefix="plain-", dir=args.scratch))
    receipt = {"status": "failed", "mode": args.mode, "depth": args.depth,
               "write_kib": args.write_kib, "file_mib": args.file_mib,
               "kernel": os.uname().release}
    try:
        pool = os.urandom(bench.Pattern.POOL)
        pattern = bench.Pattern(pool, 0, args.file_mib * MIB, args.write_kib * 1024)
        expected, _ = pattern.digests(False)
        cpu = time.process_time()
        if args.mode == "buffered":
            target = root / "copy"
            started, written, synced = buffered(target, pattern, args.depth)
            paths = [target]
        else:
            started, written, synced = journal_like(root, pattern, args.depth)
            paths = sorted(root.iterdir())
        cpu = time.process_time() - cpu
        observed = read_back(paths)
        assert observed == expected, "read-back SHA-256 mismatch"
        size = pattern.size
        receipt.update(
            status="passed",
            sha256=observed,
            bytes=size,
            write_seconds=written - started,
            through_fsync_seconds=synced - started,
            write_mb_per_second=size / (written - started) / 1e6,
            through_fsync_mb_per_second=size / (synced - started) / 1e6,
            client_cpu_seconds=cpu,
        )
    except Exception as error:
        receipt["error"] = f"{type(error).__name__}: {error}"
    finally:
        shutil.rmtree(root)
        receipt["cleanup_verified"] = not root.exists()
    (args.evidence / "receipt.json").write_text(json.dumps(receipt, indent=2) + "\n")
    print(json.dumps(receipt))
    return 0 if receipt["status"] == "passed" and receipt["cleanup_verified"] else 1


if __name__ == "__main__":
    sys.exit(main())
