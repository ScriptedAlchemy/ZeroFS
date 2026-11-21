#!/usr/bin/env bash
# Count P3 journal writes and flushes per acknowledged MiB on block-backed loops.
# Usage: bench-raw-journal-flush.sh LINUX_EXECUTABLE_AND_MANIFEST_DIRECTORY
# Env: BENCH_PAYLOAD_MIB (default 2048), BENCH_RUNS (default 3), BENCH_CPUS (default 3),
# BENCH_WRITERS (default 1; files of BENCH_PAYLOAD_MIB written at once),
# BENCH_CRASH_REPLAY=1 (no timing: SIGKILL the engine after fsync, then replay the copy),
# BENCH_FLUSH_DELAY_US (preload flush_shim.c on the P3 loop, adding this delay per sync),
# BENCH_PERF=1 (sample the engine and FUSE client with call graphs during the first copy; needs an
# image with perf and binaries built with frame pointers and symbols),
# BENCH_P3_NO_SYNC=1 (with BENCH_FLUSH_DELAY_US: delay the P3 loop's syncs without issuing them, so
# the fixture disk does not limit the copy; not a durable run),
# BENCH_ALIGNED=1 (write from a page-aligned buffer, so FUSE direct I/O does not split a write),
# BENCH_STAGES=1 (report each stage's queue depth and each thread's CPU for every copy),
# BENCH_MEMBER_PUT_DELAY_MS (pace member PUTs so read-back finds the copy in the P3 journal).
set -euo pipefail

if [[ $# != 1 || ! -x $1/zerofs || ! -x $1/zerofs-raw-device ||
      ! -r $1/memory_raw_nas_fit_fixture.toml ]]; then
    echo "Usage: $0 LINUX_EXECUTABLE_AND_MANIFEST_DIRECTORY" >&2
    exit 2
fi
image=${RAW_DEVICE_MOUNTED_IMAGE:-rust:1.100.0-bookworm}
binaries=$(cd "$1" && pwd -P)
harness=$(cd "$(dirname "$0")/raw-device" && pwd -P)
name=zerofs-journal-bench-$(od -An -N6 -tx1 /dev/urandom | tr -d ' \n')
label=org.zerofs.journal-bench.owner
owner=$(cat /proc/sys/kernel/random/uuid)

cleanup() {
    local rc=$? actual clean=true
    trap - EXIT
    if actual=$(docker container inspect --format "{{ index .Config.Labels \"$label\" }}" "$name" 2>/dev/null) &&
       [[ $actual == "$owner" ]]; then
        docker rm -f "$name" >/dev/null || clean=false
    fi
    if actual=$(docker volume inspect --format "{{ index .Labels \"$label\" }}" "$name" 2>/dev/null) &&
       [[ $actual == "$owner" ]]; then
        docker volume rm "$name" >/dev/null || clean=false
    fi
    if ! docker info --format '{{.Name}}' >/dev/null ||
       docker container inspect "$name" >/dev/null 2>&1 || docker volume inspect "$name" >/dev/null 2>&1; then
        clean=false
    fi
    printf '{"case":"docker_cleanup","name":"%s","verified_absent":%s}\n' "$name" "$clean"
    $clean || rc=1
    exit "$rc"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

docker volume create --label "$label=$owner" "$name" >/dev/null
docker run --rm --name "$name" --label "$label=$owner" --pull never \
    --network none --read-only --security-opt apparmor=unconfined \
    --cap-drop ALL --cap-add SYS_ADMIN --cap-add MKNOD \
    --device=/dev/loop-control --device-cgroup-rule='b 7:* rwm' \
    --device-cgroup-rule='c 10:229 rwm' --shm-size 64m \
    --memory 8g --memory-swap 8g --cpus "${BENCH_CPUS:-3}" --pids-limit 256 \
    --mount "type=volume,src=$name,dst=/fixture-volume,volume-nocopy" \
    --mount "type=bind,src=$binaries,dst=/input,readonly" \
    --mount "type=bind,src=$harness,dst=/h,readonly" \
    -e PYTHONDONTWRITEBYTECODE=1 -e TMPDIR=/dev/shm \
    -e "BENCH_PAYLOAD_MIB=${BENCH_PAYLOAD_MIB:-2048}" -e "BENCH_RUNS=${BENCH_RUNS:-3}" \
    ${BENCH_FLUSH_DELAY_US:+-e "BENCH_FLUSH_DELAY_US=$BENCH_FLUSH_DELAY_US"} \
    ${BENCH_WRITERS:+-e "BENCH_WRITERS=$BENCH_WRITERS"} \
    ${BENCH_CRASH_REPLAY:+-e "BENCH_CRASH_REPLAY=$BENCH_CRASH_REPLAY"} \
    ${BENCH_PERF:+-e "BENCH_PERF=$BENCH_PERF" --cap-add PERFMON} \
    ${BENCH_STAGES:+-e "BENCH_STAGES=$BENCH_STAGES"} \
    ${BENCH_ALIGNED:+-e "BENCH_ALIGNED=$BENCH_ALIGNED"} \
    ${BENCH_P3_NO_SYNC:+-e "BENCH_P3_NO_SYNC=$BENCH_P3_NO_SYNC"} \
    ${BENCH_MEMBER_PUT_DELAY_MS:+-e "BENCH_MEMBER_PUT_DELAY_MS=$BENCH_MEMBER_PUT_DELAY_MS"} \
    "$image" timeout --kill-after=30s 90m python3 -u /h/journal_flush_bench.py
