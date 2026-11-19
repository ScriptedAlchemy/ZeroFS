#!/usr/bin/env bash
# Run the mounted synthetic-loop test using parent-built Linux executables.
set -euo pipefail
if [[ $# != 1 || ! -f $1/zerofs || ! -x $1/zerofs ||
      ! -f $1/zerofs-raw-device || ! -x $1/zerofs-raw-device ]]; then
    echo "Usage: $0 LINUX_EXECUTABLE_DIRECTORY" >&2
    exit 2
fi
engine_root=$(cd "$(dirname "$0")/.." && pwd)
project_root=$(cd "$engine_root/../.." && pwd)
quiet="$project_root/.zerofs-measurement-quiet"
if [[ -e $quiet && -n $(find "$quiet" -mmin -45 -print) ]]; then
    echo "Studio Docker fixtures are paused by $quiet" >&2
    exit 1
fi
context=$(docker context show)
studio=(docker --context "$context")
endpoint=$(docker context inspect "$context" --format '{{.Endpoints.docker.Host}}')
if [[ $endpoint != unix://* || $("${studio[@]}" info --format '{{.Name}}') != docker-desktop ]]; then
    echo 'Studio Docker Desktop local Unix socket required' >&2
    exit 1
fi

binaries=$(cd "$1" && pwd -P)
stage=$(mktemp -d /var/tmp/zerofs-raw-mounted.XXXXXXXX)
cleanup() {
    local rc=$? cid
    trap - EXIT
    if [[ -s $stage/fixture.cid ]]; then
        cid=$(cat "$stage/fixture.cid")
        if [[ ! $cid =~ ^[0-9a-f]{64}$ ]]; then
            rc=1
        elif "${studio[@]}" container inspect "$cid" >/dev/null 2>&1; then
            "${studio[@]}" rm -f "$cid" >/dev/null || rc=1
        fi
    fi
    rm -rf -- "$stage" || rc=1
    if [[ $rc == 0 ]]; then
        echo 'Removed the owned Docker fixture and its CID directory.'
    fi
    exit "$rc"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

"${studio[@]}" run --rm --pull never --cidfile "$stage/fixture.cid" \
    --network none --read-only --cap-drop ALL --cap-add SYS_ADMIN --cap-add MKNOD \
    --device=/dev/loop-control --device-cgroup-rule='b 7:* rwm' \
    --device-cgroup-rule='c 10:229 rwm' --shm-size 512m \
    --memory 2g --memory-swap 2g --cpus 2 --pids-limit 128 \
    --mount "type=bind,src=$binaries,dst=/input,readonly" \
    --mount "type=bind,src=$engine_root/scripts/raw-device,dst=/h,readonly" \
    -e PYTHONDONTWRITEBYTECODE=1 -e TMPDIR=/dev/shm \
    "${RAW_DEVICE_MOUNTED_IMAGE:-rust:1.100.0-bookworm}" \
    timeout --kill-after=15s 12m python3 -u /h/mounted.py
