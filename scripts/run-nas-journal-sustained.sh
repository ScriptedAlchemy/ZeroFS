#!/usr/bin/env bash
# Run one disposable sustained-copy fixture in a short-lived local Docker container.
#
# usage: run-nas-journal-sustained.sh <owned-root> <binary> <template> <label> <files> <file-mib> [fixture args...]
#
# <owned-root> is a caller-owned directory for receipts. SCRATCH_VOLUME names an
# empty caller-owned Docker volume mounted at /scratch (ext4 inside the Docker VM);
# the fixture creates and removes its own children there. IMAGE is a local image
# with Python 3.11+. The container has no network, no published ports and no
# restart policy, and is removed when the run ends.
set -euo pipefail
umask 077
root=${1:?root}; binary=${2:?binary}; template=${3:?template}; label=${4:?label}
files=${5:?files}; file_mib=${6:?file-mib}; shift 6
scratch=${SCRATCH_VOLUME:?set SCRATCH_VOLUME to an owned empty Docker volume}
memory=${MEMORY:-8g}
scratch_mount=(-v "$scratch:/scratch")
scratch_args=()
if [[ -n ${SCRATCH_TMPFS-} ]]; then
    # Engine-ceiling mode: journal, cache and members on tmpfs, charged to the cgroup.
    scratch_mount=(--tmpfs "/scratch:rw,size=$SCRATCH_TMPFS,mode=1777")
    scratch_args=(--scratch-fs tmpfs)
fi
image=${IMAGE:-rust:1.100.0-bookworm}
cpus=${CPUS:-8}
here=$(cd "$(dirname "$0")" && pwd)
result="$root/evidence/$label"
mkdir -p "$root/evidence"
mkdir "$result"
cp "$template" "$result/engine.toml.in"
cp "$here/fixtures/nas-journal/verify.toml.in" "$result/verify.toml.in"
# Members need room for the whole copy plus reserve.
capacity=$(( (files * file_mib * 1048576 / 1000000000) + 4 ))
sed -i.orig "s/capacity_gb = 1.0/capacity_gb = ${capacity}.0/" "$result/engine.toml.in" "$result/verify.toml.in"
rm -f "$result"/*.orig
shasum -a 256 "$binary" | cut -d' ' -f1 > "$result/binary.sha256"
docker image inspect "$image" --format '{{.Id}} {{.Os}}/{{.Architecture}}' > "$result/image.txt"
name="${scratch}-run-$label"
status=0
diag=()
if [[ -n ${DIAG_PPROF_SECS-} ]]; then
    mkdir "$result/diag"
    diag=(-e ZEROFS_DIAG_DIR=/result/diag -e "ZEROFS_DIAG_PPROF_START=${DIAG_PPROF_START:-20}" -e "ZEROFS_DIAG_PPROF_SECS=$DIAG_PPROF_SECS")
fi
binary_mounts=(-v "$binary:/bin/zerofs:ro")
if [[ -n ${STRACE-} ]]; then
    # Trace only sync calls, with fd paths and time blocked. -D keeps strace a
    # grandchild so the engine keeps the PID the fixture signals.
    mkdir "$result/strace"
    cat > "$result/strace/zerofs" <<'WRAP'
#!/bin/sh
exec strace -D -f -y -T -tt -e trace=fsync,fdatasync,sync_file_range,syncfs,write,pwrite64,writev,pwritev,pwritev2 \
    -o /result/strace/trace.$$ /bin/zerofs-real "$@"
WRAP
    chmod 755 "$result/strace/zerofs"
    binary_mounts=(-v "$binary:/bin/zerofs-real:ro" -v "$result/strace/zerofs:/bin/zerofs:ro")
fi
docker run --rm --pull=never --name "$name" --network none \
    --cap-drop ALL --security-opt no-new-privileges --pids-limit 512 \
    --cpus "$cpus" --memory "$memory" --memory-swap "$memory" \
    ${diag[@]+"${diag[@]}"} \
    -v "$here:/fixtures:ro" "${binary_mounts[@]}" \
    "${scratch_mount[@]}" -v "$result:/result" \
    "$image" sh -c '
for key in cpu.max memory.max memory.swap.max memory.events; do cat /sys/fs/cgroup/$key > /result/start-$key; done
test "$(cat /sys/fs/cgroup/memory.swap.max)" = 0 || exit 2
uname -srm > /result/kernel.txt
python3 -B /fixtures/bench-nas-journal-sustained.py --binary /bin/zerofs \
    --scratch /scratch --evidence /result/run \
    --config-template /result/engine.toml.in --verify-config-template /result/verify.toml.in \
    --member-a /scratch --member-b /scratch "$@" > /result/stdout 2>&1
status=$?
for key in memory.events memory.peak memory.swap.peak cpu.stat; do cat /sys/fs/cgroup/$key > /result/end-$key 2>/dev/null; done
for f in start end; do grep -E "^(oom|oom_kill|oom_group_kill) " /result/$f-memory.events > /result/$f-memory.events.core; done
cmp -s /result/start-memory.events.core /result/end-memory.events.core || { echo "OOM events changed" >> /result/stdout; status=3; }
leftover=$(find /scratch -mindepth 1 -maxdepth 1 | wc -l)
echo "scratch_leftovers=$leftover" > /result/scratch.txt
exit $status
' sh --files "$files" --file-mib "$file_mib" --timeout 1800 ${scratch_args[@]+"${scratch_args[@]}"} "$@" || status=$?
if docker container inspect "$name" >/dev/null 2>&1; then docker rm -f "$name" >/dev/null; fi
echo "$status" > "$result/exit.txt"
tail -c 2000 "$result/stdout"
exit "$status"
