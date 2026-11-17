#!/usr/bin/env python3
"""Recompute the package admission inputs from a completed Linux workload cell."""
import argparse
import json
from pathlib import Path

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('cells', type=Path, nargs='+')
args = parser.parse_args()
observations = []
for cell in args.cells:
    receipt = json.loads((cell / 'outcome.json').read_text())
    assert receipt['completed'], f'{cell}: workload incomplete'
    if 'companion' in receipt:
        workload = receipt['companion']
        profile = receipt['build_profile']
        assert workload['sha256'] == 'all verified'
        assert workload['sequential_bytes'] == 12 * 1024**3
        assert workload['small_files'] == 5000
        assert workload['gc_passes_after_flush'] >= 1
    else:
        profile = receipt['profile']
        verified = json.loads((cell / 'verified-full.json').read_text())
        assert verified['sha256'] == 'all matched'
        assert verified['files'] == receipt['files']
        assert verified['bytes'] == receipt['bytes']
        assert receipt['gc_passes_after_flush'] >= 1
        manifest = json.loads((cell / 'manifest.json').read_text())
        assert len(manifest) == verified['files']
        assert sum(item['size'] for item in manifest.values()) == verified['bytes']
    assert profile == 'ci+webui'
    rows = [json.loads(line) for line in (cell / 'samples.jsonl').read_text().splitlines()]
    if 'sample_window_started_at' in receipt:
        rows = [row for row in rows if receipt['sample_window_started_at'] <= row['unix_seconds'] <= receipt['finished_at']]
    assert rows, f'{cell}: no samples in completed workload window'
    assert all(int(row['swap_current_bytes']) == 0 for row in rows)
    assert all(row['events']['oom_kill'] == 0 for row in rows)
    resident_samples = [int(row['metrics']['zerofs_jemalloc_resident_bytes'])
                        for row in rows if 'zerofs_jemalloc_resident_bytes' in row['metrics']]
    hwm_samples = [int(row['process_status']['VmHWM'].split()[0]) * 1024
                   for row in rows if 'VmHWM' in row['process_status']]
    assert resident_samples and hwm_samples, 'engine observations are unavailable'
    assert all('kernel' in row['stat'] and 'sock' in row['stat'] for row in rows)
    observations.append({
        'cell': str(cell),
        'samples': str(cell / 'samples.jsonl'),
        'profile': profile,
        'run_count': 1,
        'jemalloc_resident_peak_bytes': max(resident_samples),
        'vmhwm_peak_bytes': max(hwm_samples),
        'engine_peak_bytes': max(max(resident_samples), max(hwm_samples)),
        'kernel_and_sock_peak_bytes': max(row['stat']['kernel'] + row['stat']['sock'] for row in rows),
    })
engine_peak = max(row['engine_peak_bytes'] for row in observations)
kernel_peak = max(row['kernel_and_sock_peak_bytes'] for row in observations)
unit = 64 * 1024**2

def rounded(value):
    return (value + unit - 1) // unit * unit

residency = rounded((engine_peak * 110 + 99) // 100)
headroom = rounded((engine_peak * 15 + 99) // 100 + (kernel_peak * 125 + 99) // 100)
print(json.dumps({
    'observations': observations,
    'profile': 'ci+webui',
    'observation_count': len(observations),
    'resident_peak_range_bytes': (max(row['jemalloc_resident_peak_bytes'] for row in observations) - min(row['jemalloc_resident_peak_bytes'] for row in observations)) if len(observations) > 1 else None,
    'engine_peak_bytes': engine_peak,
    'kernel_and_sock_peak_bytes': kernel_peak,
    'unmodeled_residency_reserve_bytes': residency,
    'engine_headroom_bytes': headroom,
    'required_bytes': 96 * 1024**2 + 2 * 1024**3 + residency + headroom,
}, indent=2))
