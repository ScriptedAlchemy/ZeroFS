#!/usr/bin/env bash
set -euo pipefail

device=${1:?usage: tune-nbd.sh /dev/nbdN}
[[ $device =~ ^/dev/nbd[0-9]+$ ]] || {
  echo "refusing non-NBD device: $device" >&2
  exit 2
}
block=${device#/dev/}
queue="/sys/class/block/$block/queue"
test -d "$queue"

# Match the measured fast path: 4 MiB requests, a deep block queue, and enough
# guest dirty-page headroom for the 16 GB server-side volatile tier.
printf '%s\n' 4096 >"$queue/max_sectors_kb"
printf '%s\n' 256 >"$queue/nr_requests"
printf '%s\n' 16384 >"$queue/read_ahead_kb"
sysctl -q -w vm.dirty_bytes=21474836480
sysctl -q -w vm.dirty_background_bytes=8589934592

major_minor=$(<"/sys/class/block/$block/dev")
bdi="/sys/class/bdi/$major_minor"
if [[ -d $bdi ]]; then
  [[ ! -w $bdi/max_bytes ]] || printf '%s\n' 17179869184 >"$bdi/max_bytes"
  [[ ! -w $bdi/strict_limit ]] || printf '%s\n' 1 >"$bdi/strict_limit"
fi
