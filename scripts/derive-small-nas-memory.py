#!/usr/bin/env python3
"""Recompute the package admission inputs from a completed Linux workload cell."""
import argparse
import json
from pathlib import Path


MIN_SEQUENTIAL_BYTES = 12 * 1024**3
MIN_SMALL_FILES = 5000
MIN_SMALL_FILE_BYTES = 1024


def require(condition, message):
    # These receipts justify admission reserves. They must still be checked
    # when Python is run with -O or PYTHONOPTIMIZE is set.
    if not condition:
        raise ValueError(message)


def unsigned_integer(value, source):
    if isinstance(value, str):
        require(value.isascii() and value.isdigit(), f'{source}: invalid unsigned integer')
        return int(value)
    require(type(value) in (int, float), f'{source}: invalid unsigned integer')
    parsed = int(value)
    require(value == parsed and parsed >= 0, f'{source}: invalid unsigned integer')
    return parsed


def vmhwm_bytes(value, source):
    fields = value.split()
    require(len(fields) == 2 and fields[1] == 'kB', f'{source}: VmHWM must use kB')
    return unsigned_integer(fields[0], source) * 1024


parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('cells', type=Path, nargs='+')
args = parser.parse_args()
observations = []
for cell in args.cells:
    receipt = json.loads((cell / 'outcome.json').read_text())
    require(receipt['completed'] is True, f'{cell}: workload incomplete')
    if 'companion' in receipt:
        workload = receipt['companion']
        profile = receipt['build_profile']
        require(workload['sha256'] == 'all verified', f'{cell}: integrity verification failed')
        require(unsigned_integer(workload['sequential_bytes'], str(cell)) == MIN_SEQUENTIAL_BYTES, f'{cell}: sequential workload is incomplete')
        require(unsigned_integer(workload['small_files'], str(cell)) == MIN_SMALL_FILES, f'{cell}: small-file workload is incomplete')
        require(unsigned_integer(workload['gc_passes_after_flush'], str(cell)) >= 1, f'{cell}: no GC pass after flush')
    else:
        profile = receipt['profile']
        verified = json.loads((cell / 'verified-full.json').read_text())
        require(verified['sha256'] == 'all matched', f'{cell}: integrity verification failed')
        verified_files = unsigned_integer(verified['files'], str(cell))
        verified_bytes = unsigned_integer(verified['bytes'], str(cell))
        require(verified_files == unsigned_integer(receipt['files'], str(cell)), f'{cell}: verified file count does not match')
        require(verified_bytes == unsigned_integer(receipt['bytes'], str(cell)), f'{cell}: verified byte count does not match')
        require(unsigned_integer(receipt['gc_passes_after_flush'], str(cell)) >= 1, f'{cell}: no GC pass after flush')
        manifest = json.loads((cell / 'manifest.json').read_text())
        require(len(manifest) == verified_files, f'{cell}: manifest file count does not match')
        sizes = [unsigned_integer(item['size'], str(cell)) for item in manifest.values()]
        require(sum(sizes) == verified_bytes, f'{cell}: manifest byte count does not match')
        # Native receipts may cover larger ladder rungs, but smaller controls
        # cannot justify reserves for the documented minimum package workload.
        # See "Stored-state coverage" in docs/memory-admission-small-nas.md.
        require(any(size >= MIN_SEQUENTIAL_BYTES for size in sizes), f'{cell}: native workload needs a file of at least 12 GiB')
        require(sum(size >= MIN_SMALL_FILE_BYTES for size in sizes) >= MIN_SMALL_FILES + 1, f'{cell}: native workload needs at least 5000 additional files of at least 1 KiB')
    require(profile == 'ci+webui', f'{cell}: unqualified build profile')
    rows = [json.loads(line) for line in (cell / 'samples.jsonl').read_text().splitlines()]
    if 'sample_window_started_at' in receipt:
        rows = [row for row in rows if receipt['sample_window_started_at'] <= row['unix_seconds'] <= receipt['finished_at']]
    require(rows, f'{cell}: no samples in completed workload window')
    require(all(unsigned_integer(row['swap_current_bytes'], f'{cell}: swap') == 0 for row in rows), f'{cell}: workload used swap')
    require(all(unsigned_integer(row['events']['oom_kill'], f'{cell}: OOM kills') == 0 for row in rows), f'{cell}: workload had an OOM kill')
    resident_samples = [unsigned_integer(row['metrics']['zerofs_jemalloc_resident_bytes'], f'{cell}: jemalloc residency')
                        for row in rows if 'zerofs_jemalloc_resident_bytes' in row['metrics']]
    hwm_samples = [vmhwm_bytes(row['process_status']['VmHWM'], str(cell))
                   for row in rows if 'VmHWM' in row['process_status']]
    require(resident_samples and hwm_samples, f'{cell}: engine observations are unavailable')
    require(all('kernel' in row['stat'] and 'sock' in row['stat'] for row in rows), f'{cell}: kernel observations are unavailable')
    observations.append({
        'cell': str(cell),
        'samples': str(cell / 'samples.jsonl'),
        'profile': profile,
        'run_count': 1,
        'jemalloc_resident_peak_bytes': max(resident_samples),
        'vmhwm_peak_bytes': max(hwm_samples),
        'engine_peak_bytes': max(max(resident_samples), max(hwm_samples)),
        'kernel_and_sock_peak_bytes': max(
            unsigned_integer(row['stat']['kernel'], f'{cell}: kernel')
            + unsigned_integer(row['stat']['sock'], f'{cell}: sock') for row in rows),
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
