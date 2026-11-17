#!/usr/bin/env python3
"""Recompute the package admission inputs from a completed Linux workload cell."""
import argparse
import json
from pathlib import Path

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('cell', type=Path)
args = parser.parse_args()
receipt = json.loads((args.cell / 'outcome.json').read_text())
workload = receipt['companion']
assert receipt['completed'] and receipt['build_profile'] == 'ci+webui'
assert workload['sha256'] == 'all verified'
assert workload['sequential_bytes'] == 12 * 1024**3
assert workload['small_files'] == 5000
assert workload['gc_passes_after_flush'] >= 1

rows = [json.loads(line) for line in (args.cell / 'samples.jsonl').read_text().splitlines()]
assert all(int(row['swap_current_bytes']) == 0 for row in rows)
assert all(row['events']['oom_kill'] == 0 for row in rows)
resident_samples = [int(row['metrics']['zerofs_jemalloc_resident_bytes'])
                    for row in rows if 'zerofs_jemalloc_resident_bytes' in row['metrics']]
hwm_samples = [int(row['process_status']['VmHWM'].split()[0]) * 1024
               for row in rows if 'VmHWM' in row['process_status']]
assert resident_samples and hwm_samples, 'engine observations are unavailable'
assert all('kernel' in row['stat'] and 'sock' in row['stat'] for row in rows)
resident = max(resident_samples)
hwm = max(hwm_samples)
engine_peak = max(resident, hwm)
kernel_peak = max(row['stat'].get('kernel', 0) + row['stat'].get('sock', 0) for row in rows)
unit = 64 * 1024**2

def rounded(value):
    return (value + unit - 1) // unit * unit

residency = rounded((engine_peak * 110 + 99) // 100)
headroom = rounded((engine_peak * 15 + 99) // 100 + (kernel_peak * 125 + 99) // 100)
print(json.dumps({
    'cell': str(args.cell),
    'profile': receipt['build_profile'],
    'binary_sha256': receipt['binary_sha256'],
    'engine_peak_bytes': engine_peak,
    'kernel_and_sock_peak_bytes': kernel_peak,
    'unmodeled_residency_reserve_bytes': residency,
    'engine_headroom_bytes': headroom,
    'required_bytes': 96 * 1024**2 + 2 * 1024**3 + residency + headroom,
}, indent=2))
