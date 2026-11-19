#!/usr/bin/env python3
"""Derive a small-nas-journal-v2 fixture template from the v1 template.

usage: make-v2-template.py <journal-MiB> <upload-workers> <output> [--volatile]

Only the profile, journal budget, free floor (max(64 MiB, journal/8)) and
upload worker count change. --volatile selects RAM-acknowledged ordinary
writes (256 MiB, 2048 operations), the NAS trial's write policy; fsync still
waits for the SSD journal. Every other tested field stays as in v1.
"""

from pathlib import Path
import sys

MIB = 1024 * 1024
journal_mib, uploads, output = int(sys.argv[1]), int(sys.argv[2]), Path(sys.argv[3])
if journal_mib % 256 or not 512 <= journal_mib <= 16384 or not 2 <= uploads <= 8:
    sys.exit("journal must be 512-16384 MiB in 256 MiB steps; uploads 2-8")
disk = journal_mib * MIB
floor = max(64 * MIB, disk // 8)
text = (Path(__file__).parent / "engine.toml.in").read_text()
for old, new in (
    ('profile = "small-nas-journal-v1"', 'profile = "small-nas-journal-v2"'),
    ("disk_size_gb = 0.536870912", f"disk_size_gb = {disk / 1e9:.9f}"),
    ("min_free_gb = 0.067108864", f"min_free_gb = {floor / 1e9:.9f}"),
    ("upload_concurrency = 2", f"upload_concurrency = {uploads}"),
):
    if text.count(old) != 1:
        sys.exit(f"template field not found exactly once: {old}")
    text = text.replace(old, new)
if "--volatile" in sys.argv[4:]:
    old = 'write_ack_mode = "materialized"'
    if text.count(old) != 1:
        sys.exit("write_ack_mode not found exactly once")
    text = text.replace(old, 'write_ack_mode = "volatile_memory"\nvolatile_memory_gb = 0.268435456\nvolatile_max_operations = 2048')
output.write_text(text)
