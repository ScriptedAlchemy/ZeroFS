#!/usr/bin/env bash
# Run the mounted synthetic-loop test using parent-built Linux executables.
set -euo pipefail
suite=studio
if [[ ${1:-} == --spark ]]; then
    suite=spark
    shift
fi
mode=placement
case ${1:-} in
    --p3) mode=p3; shift ;;
    --nas-fit) mode=nas-fit; shift ;;
esac
if [[ $# != 1 || ! -f $1/zerofs || ! -x $1/zerofs ||
      ! -f $1/zerofs-raw-device || ! -x $1/zerofs-raw-device ]]; then
    echo "Usage: $0 [--spark] [--p3|--nas-fit] LINUX_EXECUTABLE_DIRECTORY" >&2
    exit 2
fi
if [[ $mode == nas-fit && ($suite != spark || ! -r $1/memory_raw_fixture.toml) ]]; then
    echo 'NAS-fit requires --spark and the pinned memory_raw_fixture.toml alongside the executables' >&2
    exit 2
fi
engine_root=$(cd "$(dirname "$0")/.." && pwd)
project_root=$(cd "$engine_root/../.." && pwd)
quiet="$project_root/.zerofs-measurement-quiet"
if [[ -e $quiet && -n $(find "$quiet" -mmin -45 -print) ]]; then
    echo "Studio Docker fixtures are paused by $quiet" >&2
    exit 1
fi
image=${RAW_DEVICE_MOUNTED_IMAGE:-rust:1.100.0-bookworm}
if [[ $suite == spark ]]; then
    docker_cmd=(docker --context default)
    endpoint=$(docker context inspect default --format '{{.Endpoints.docker.Host}}')
    identity=$("${docker_cmd[@]}" info --format '{{.Name}} {{.Architecture}} {{.MemTotal}}')
    read -r daemon architecture host_memory <<< "$identity"
    if [[ $(hostname) != gx10-6a8d || $(uname -m) != aarch64 ||
          $endpoint != unix:///var/run/docker.sock || $daemon != gx10-6a8d ||
          $architecture != aarch64 || $host_memory -lt $((48 * 1024 ** 3)) ]]; then
        echo 'Spark gx10-6a8d arm64 with its default local Docker socket and at least 48 GiB required' >&2
        exit 1
    fi
    memory=44g
    memory_bytes=$((44 * 1024 ** 3))
    if [[ $mode == nas-fit ]]; then
        memory=7504m
        memory_bytes=$((7504 * 1024 ** 2))
    fi
    platform=linux/arm64
    # Spark's docker-default AppArmor profile denies the FUSE mount syscall.
    security=(--security-opt apparmor=unconfined)
else
    security=()
    context=$(docker context show)
    docker_cmd=(docker --context "$context")
    endpoint=$(docker context inspect "$context" --format '{{.Endpoints.docker.Host}}')
    if [[ $endpoint != unix://* || $("${docker_cmd[@]}" info --format '{{.Name}}') != docker-desktop ]]; then
        echo 'Studio Docker Desktop local Unix socket required' >&2
        exit 1
    fi
    identity=$("${docker_cmd[@]}" info --format '{{.Name}} {{.Architecture}} {{.MemTotal}}')
    memory=2g
    memory_bytes=$((2 * 1024 ** 3))
    case $(uname -m) in
        arm64|aarch64) platform=linux/arm64 ;;
        x86_64) platform=linux/amd64 ;;
        *) echo 'Unsupported Studio architecture' >&2; exit 1 ;;
    esac
fi
image_info=$("${docker_cmd[@]}" image inspect "$image" --format '{{.Os}}/{{.Architecture}} {{.Id}} {{join .RepoDigests ","}}')
read -r image_platform image_id _ <<< "$image_info"
if [[ $image_platform != "$platform" || ! $image_id =~ ^sha256:[0-9a-f]{64}$ ]]; then
    echo "Expected $platform image $image, got $image_info" >&2
    exit 1
fi
printf 'Docker host: %s (%s)\nDocker image: %s\n' "$identity" "$endpoint" "$image_info"

binaries=$(cd "$1" && pwd -P)
stage=$(mktemp -d /var/tmp/zerofs-raw-mounted.XXXXXXXX)
cleanup() {
    local rc=$? cid cleanup_failed=0
    trap - EXIT
    if [[ -s $stage/fixture.cid ]]; then
        cid=$(cat "$stage/fixture.cid")
        if [[ ! $cid =~ ^[0-9a-f]{64}$ ]]; then
            cleanup_failed=1
        elif "${docker_cmd[@]}" container inspect "$cid" >/dev/null 2>&1; then
            "${docker_cmd[@]}" rm -f "$cid" >/dev/null || cleanup_failed=1
        fi
    fi
    rm -rf -- "$stage" || cleanup_failed=1
    if [[ $cleanup_failed == 0 ]]; then
        echo 'Removed the owned Docker fixture and its CID directory.'
    else
        echo 'Failed to verify owned Docker fixture cleanup.' >&2
        rc=1
    fi
    exit "$rc"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

"${docker_cmd[@]}" run --rm --pull never --platform "$platform" --cidfile "$stage/fixture.cid" \
    --network none --read-only --cap-drop ALL --cap-add SYS_ADMIN --cap-add MKNOD \
    "${security[@]+"${security[@]}"}" \
    --device=/dev/loop-control --device-cgroup-rule='b 7:* rwm' \
    --device-cgroup-rule='c 10:229 rwm' --shm-size 512m \
    --memory "$memory" --memory-swap "$memory" --cpus 2 --pids-limit 128 \
    --mount "type=bind,src=$binaries,dst=/input,readonly" \
    --mount "type=bind,src=$engine_root/scripts/raw-device,dst=/h,readonly" \
    -e PYTHONDONTWRITEBYTECODE=1 -e TMPDIR=/dev/shm \
    -e "RAW_MOUNTED_MEMORY_BYTES=$memory_bytes" -e "RAW_MOUNTED_MODE=$mode" "$image_id" \
    timeout --kill-after=15s 12m python3 -u /h/mounted.py
