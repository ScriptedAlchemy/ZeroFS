#!/usr/bin/env bash

# Read-only host evidence for package-builder failures, especially SIGKILL.
# Do not dump environment variables, Docker configuration, or full kernel logs.
set -euo pipefail
export LC_ALL=C

[[ $# -eq 1 ]] || { echo "usage: $0 OUTPUT_DIRECTORY" >&2; exit 2; }
mkdir -p -- "$1"
output=$1/build-failure-diagnostics.txt
{
    printf 'Collected at: '
    date -u +%Y-%m-%dT%H:%M:%SZ
    printf '\nHost memory at collection time (not peak usage):\n'
    grep -E '^(MemTotal|MemAvailable|SwapTotal|SwapFree):' /proc/meminfo || true
    printf '\nKernel OOM evidence (boot-relative timestamps):\n'
    if kernel_log=$(sudo -n dmesg --color=never 2>/dev/null); then
        # Only established OOM record formats, never unrelated kernel output.
        if ! printf '%s\n' "$kernel_log" | grep -E \
            'oom-kill:|Out of memory: Killed process|Memory cgroup out of memory: Killed process'; then
            printf 'No matching OOM records found. This does not rule out OOM.\n'
        fi
    else
        printf 'Kernel log unavailable; the SIGKILL cause remains unknown.\n'
    fi
} >"$output"
printf 'Build failure diagnostics saved to %s\n' "$output"
