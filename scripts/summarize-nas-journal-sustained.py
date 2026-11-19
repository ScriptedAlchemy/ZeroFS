#!/usr/bin/env python3
"""Summarize sustained-copy receipts into one JSON line per run.

usage: summarize-nas-journal-sustained.py <evidence-dir>... [--series LABEL]

Each run directory holds run/receipt.json from bench-nas-journal-sustained.py
or receipt.json from bench-plain-copy-control.py. --series prints the drain,
backlog and admission time series of one run at two-second steps.
"""

import json
from pathlib import Path
import sys


def engine_row(path):
    receipt = json.loads((path / "run/receipt.json").read_text())
    transfer = receipt.get("transfer") or {}
    fsync = transfer.get("through_fsync") or {}
    end = transfer.get("through_end") or {}
    row = {
        "run": path.name,
        "status": receipt["status"],
        "error": receipt.get("error"),
        "files": receipt.get("files") if isinstance(receipt.get("files"), int) else None,
        "logical_bytes": transfer.get("logical_bytes"),
        "depth": receipt.get("depth"),
        "write_kib": receipt.get("write_kib"),
        "write_mb_s": transfer.get("write_mb_per_second"),
        "through_fsync_mb_s": transfer.get("through_fsync_mb_per_second"),
        "through_drain_mb_s": transfer.get("through_drain_mb_per_second"),
        "middle_drain_mb_s": (receipt.get("middle_remote_drain") or {}).get("bytes_per_second", 0) / 1e6 or None,
        "paced_before_fsync": transfer.get("paced_before_fsync"),
        "ssd_wa_through_fsync": fsync.get("non_member_write_amplification"),
        "clean_cache_wa_through_fsync": fsync.get("clean_cache_write_amplification"),
        "ssd_wa_end": end.get("non_member_write_amplification"),
        "member_wa_end": end.get("member_write_amplification"),
        "engine_cores_through_fsync": (fsync.get("cgroup_usage_seconds") or 0)
        / max(transfer.get("through_fsync_seconds") or 1, 1e-9),
        "throttled_s": fsync.get("cgroup_throttled_seconds"),
        "anon_peak_bytes": transfer.get("anon_peak_bytes"),
        "memory_peak_bytes": transfer.get("memory_peak_bytes"),
        "crash_after_fsync": receipt.get("crash_after_fsync"),
        "tail_reader": receipt.get("tail_reader"),
    }
    binary = path / "binary.sha256"
    if binary.exists():
        row["binary_sha256"] = binary.read_text().strip()
    return row


def control_row(path):
    receipt = json.loads((path / "receipt.json").read_text())
    return {
        "run": path.name,
        "status": receipt["status"],
        "mode": receipt.get("mode"),
        "depth": receipt.get("depth"),
        "write_kib": receipt.get("write_kib"),
        "write_mb_s": receipt.get("write_mb_per_second"),
        "through_fsync_mb_s": receipt.get("through_fsync_mb_per_second"),
    }


def series(path, step=2.0):
    receipt = json.loads((path / "run/receipt.json").read_text())
    samples = [s for s in receipt["samples"] if "remote_bytes_completed_total" in s]
    start = receipt["baseline_sample"]
    previous = start
    for sample in samples:
        if sample["elapsed_seconds"] - previous["elapsed_seconds"] < step:
            continue
        elapsed = sample["elapsed_seconds"] - previous["elapsed_seconds"]
        print(json.dumps({
            "t": round(sample["elapsed_seconds"] - start["elapsed_seconds"], 1),
            "phase": sample["phase"],
            "drain_mb_s": round((sample["remote_bytes_completed_total"] - previous["remote_bytes_completed_total"]) / elapsed / 1e6, 1),
            "journal_backlog_mib": round(sample["dirty_ssd_reserved_bytes"] / 2**20),
            "dirty_ram_mib": round(sample["dirty_ram_bytes"] / 2**20),
            "paced": sample["ssd_admission_paced"],
        }))
        previous = sample


def main(argv):
    if "--series" in argv:
        series(Path(argv[argv.index("--series") + 1]))
        return 0
    for name in argv:
        path = Path(name)
        row = engine_row(path) if (path / "run/receipt.json").exists() else control_row(path)
        print(json.dumps({k: (round(v, 3) if isinstance(v, float) else v) for k, v in row.items()}))
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
