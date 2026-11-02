#!/usr/bin/env python3
"""Bounded, read-only snapshots for the disposable Linux NFS CI runner.

Never walk the mounted test filesystem: a diagnostic path lookup there can
block behind the same I/O we need to diagnose. All input comes from procfs.
"""
from __future__ import annotations

import argparse
import datetime
import fcntl
import json
import os
from pathlib import Path

MAX_PROCESSES = 8
MAX_THREADS = 8
PROCESS_READ_LIMIT = 4096
COUNTER_READ_LIMIT = 128 * 1024
LOG_LIMIT = 8 * 1024 * 1024
PROCESS_NAMES = {"tar", "gzip", "zerofs"}
PROCESS_FILES = ("stat", "status", "wchan", "syscall", "stack", "io")
COUNTER_FILES = (
    "net/rpc/nfs", "self/mountstats", "net/tcp", "net/tcp6",
    "pressure/io", "pressure/memory", "meminfo", "vmstat",
)


def read_bounded(path: Path, limit: int) -> dict:
    try:
        with path.open("rb") as stream:
            data = stream.read(limit + 1)
        return {"text": data[:limit].decode("utf-8", errors="replace"),
                "truncated": len(data) > limit}
    except OSError as error:
        # Exited tasks, restricted stacks and absent kernel features are data,
        # not a reason to discard every other observation in the snapshot.
        return {"error": str(error)}


def numeric_directories(path: Path):
    try:
        with os.scandir(path) as entries:
            for entry in entries:
                if entry.name.isascii() and entry.name.isdigit():
                    yield Path(entry.path)
    except OSError:
        return


def snapshot(proc: Path = Path("/proc")) -> dict:
    result = {
        "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "counters": {name: read_bounded(proc / name, COUNTER_READ_LIMIT)
                     for name in COUNTER_FILES},
        "processes": [],
    }
    for task in numeric_directories(proc):
        comm = read_bounded(task / "comm", 128).get("text", "").strip()
        if comm not in PROCESS_NAMES:
            continue
        process = {"pid": int(task.name), "comm": comm,
                   "files": {name: read_bounded(task / name, PROCESS_READ_LIMIT)
                             for name in PROCESS_FILES}, "threads": []}
        if comm == "zerofs":
            for thread in numeric_directories(task / "task"):
                process["threads"].append({
                    "tid": int(thread.name),
                    "wchan": read_bounded(thread / "wchan", PROCESS_READ_LIMIT),
                    "stack": read_bounded(thread / "stack", PROCESS_READ_LIMIT),
                })
                if len(process["threads"]) >= MAX_THREADS:
                    break
        result["processes"].append(process)
        if len(result["processes"]) >= MAX_PROCESSES:
            break
    return result


def append_snapshot(output: Path, value: dict, limit: int = LOG_LIMIT) -> None:
    data = (json.dumps(value, ensure_ascii=True, separators=(",", ":")) + "\n").encode()
    if len(data) > limit:
        raise ValueError("one diagnostic snapshot exceeds the configured log bound")
    output.parent.mkdir(parents=True, exist_ok=True)
    # Serialize final collection with a last in-flight periodic snapshot.
    with output.with_name(output.name + ".lock").open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        if output.exists() and output.stat().st_size + len(data) > limit:
            output.replace(output.with_name(output.name + ".1"))
        with output.open("ab") as stream:
            stream.write(data)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    value = snapshot()
    append_snapshot(args.output, value)
    print(value["timestamp"], "NFS snapshot:",
          ", ".join(f"{p['comm']}[{p['pid']}]={p['files']['wchan'].get('text', 'unavailable').strip()}"
                    for p in value["processes"]))


if __name__ == "__main__":
    main()
