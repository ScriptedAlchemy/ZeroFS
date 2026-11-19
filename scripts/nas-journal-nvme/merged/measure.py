#!/usr/bin/env python3
"""Record the fresh container's aggregate cgroup limits, peak and events."""
import json
import os
import subprocess
import sys
from pathlib import Path

CGROUP = Path("/sys/fs/cgroup")
COUNTERS = ("memory.events", "memory.events.local", "memory.swap.events", "cpu.stat")
VALUES = (
    "memory.max", "memory.current", "memory.peak", "memory.swap.max",
    "memory.swap.current", "memory.swap.peak", "cpuset.cpus.effective", "cpu.max",
)


def snapshot():
    values = {name: (CGROUP / name).read_text().strip() for name in VALUES}
    values.update({
        name: {key: int(value) for key, value in (
            line.split() for line in (CGROUP / name).read_text().splitlines()
        )}
        for name in COUNTERS
    })
    values["affinity"] = sorted(os.sched_getaffinity(0))
    return values


def main():
    output = Path("/fixture/cgroup.json")
    before = snapshot()
    if (
        Path("/proc/self/cgroup").read_text().strip() != "0::/"
        or before["memory.max"] != str(8 * 1024**3)
        or before["memory.swap.max"] != "0"
        or before["memory.swap.current"] != "0"
        or before["cpuset.cpus.effective"] != "0-1"
        or before["affinity"] != [0, 1]
    ):
        raise SystemExit(f"unexpected cgroup limits: {before}")
    child = subprocess.run([sys.executable, "-B", "/fixture/run.py"], check=False)
    after = snapshot()
    events = before["memory.events"]
    deltas = {key: value - events[key] for key, value in after["memory.events"].items()}
    output.write_text(json.dumps({
        "before": before, "after": after, "memory_event_delta": deltas,
        "workload_exit": child.returncode,
    }, indent=2, sort_keys=True) + "\n")
    if (
        any(deltas.get(key, 0) for key in ("max", "oom", "oom_kill", "oom_group_kill"))
        or after["memory.swap.current"] != "0"
        or after["memory.swap.peak"] != "0"
        or any(after[key] != before[key] for key in ("memory.max", "memory.swap.max",
                                                    "cpuset.cpus.effective", "affinity"))
    ):
        raise SystemExit("aggregate cgroup limit or swap check failed")
    return child.returncode


if __name__ == "__main__":
    raise SystemExit(main())
