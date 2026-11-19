#!/usr/bin/env bash
# Run the 8 GiB mounted raw-device qualification on Spark.
set -euo pipefail

mode=all
if [[ ${1:-} == --journal-refusal ]]; then
    mode=journal-refusal
    shift
fi
if [[ $# != 1 || ! -x $1/zerofs || ! -x $1/zerofs-raw-device ||
      ! -r $1/memory_raw_nas_fit_fixture.toml ]]; then
    echo "Usage: $0 [--journal-refusal] LINUX_EXECUTABLE_AND_MANIFEST_DIRECTORY" >&2
    exit 2
fi

docker_cmd=(docker --context default)
endpoint=$(docker context inspect default --format '{{.Endpoints.docker.Host}}')
identity=$("${docker_cmd[@]}" info --format '{{.Name}} {{.Architecture}} {{.MemTotal}}')
read -r daemon architecture host_memory <<< "$identity"
if [[ $(hostname) != gx10-6a8d || $(uname -m) != aarch64 ||
      $endpoint != unix:///var/run/docker.sock || $daemon != gx10-6a8d ||
      $architecture != aarch64 || $host_memory -lt $((48 * 1024 ** 3)) ]]; then
    echo 'The Spark gx10-6a8d arm64 local Docker daemon is required.' >&2
    exit 1
fi

image=${RAW_DEVICE_MOUNTED_IMAGE:-rust:1.100.0-bookworm}
image_info=$("${docker_cmd[@]}" image inspect "$image" --format '{{.Os}}/{{.Architecture}} {{.Id}} {{join .RepoDigests ","}}')
read -r image_platform image_id _ <<< "$image_info"
if [[ $image_platform != linux/arm64 || ! $image_id =~ ^sha256:[0-9a-f]{64}$ ]]; then
    echo "Expected a local linux/arm64 image, got $image_info" >&2
    exit 1
fi

binaries=$(cd "$1" && pwd -P)
harness=$(cd "$(dirname "$0")/raw-device" && pwd -P)
stage=$(mktemp -d /var/tmp/zerofs-raw-large.XXXXXXXX)
name=${stage##*/}
owner=$(cat /proc/sys/kernel/random/uuid)
label=org.zerofs.raw-large.owner
volume_created=false
cleanup() {
    local rc=$? clean=0 actual
    trap - EXIT
    if actual=$("${docker_cmd[@]}" container inspect --format "{{ index .Config.Labels \"$label\" }}" "$name" 2>/dev/null); then
        if [[ $actual == "$owner" ]]; then
            "${docker_cmd[@]}" rm -f "$name" >/dev/null || clean=1
        else
            clean=1
        fi
    fi
    if $volume_created; then
        if actual=$("${docker_cmd[@]}" volume inspect --format "{{ index .Labels \"$label\" }}" "$name" 2>/dev/null) &&
           [[ $actual == "$owner" ]]; then
            "${docker_cmd[@]}" volume rm "$name" >/dev/null || clean=1
        else
            clean=1
        fi
    fi
    rmdir -- "$stage" || clean=1
    if ! "${docker_cmd[@]}" info --format '{{.Name}}' >/dev/null ||
       [[ -e $stage ]] ||
       "${docker_cmd[@]}" container inspect "$name" >/dev/null 2>&1 ||
       "${docker_cmd[@]}" volume inspect "$name" >/dev/null 2>&1; then
        clean=1
    fi
    printf '{"case":"docker_cleanup","container":"%s","volume":"%s","host_stage":"%s","verified_absent":%s}\n' \
        "$name" "$name" "$stage" "$([[ $clean == 0 ]] && echo true || echo false)"
    if (( clean )); then rc=1; fi
    exit "$rc"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

if "${docker_cmd[@]}" volume inspect "$name" >/dev/null 2>&1 ||
   "${docker_cmd[@]}" container inspect "$name" >/dev/null 2>&1; then
    echo "Fixture name collision: $name" >&2
    exit 1
fi
"${docker_cmd[@]}" volume create --label "$label=$owner" "$name" >/dev/null
volume_created=true
actual=$("${docker_cmd[@]}" volume inspect --format "{{ index .Labels \"$label\" }}" "$name")
[[ $actual == "$owner" ]] || exit 1
printf 'Docker host %s (%s)\nDocker image %s\n' "$identity" "$endpoint" "$image_info"

"${docker_cmd[@]}" run --rm --name "$name" --label "$label=$owner" \
    --pull never --platform linux/arm64 --network none --read-only \
    --security-opt apparmor=unconfined --cap-drop ALL --cap-add SYS_ADMIN --cap-add MKNOD \
    --device=/dev/loop-control --device-cgroup-rule='b 7:* rwm' \
    --device-cgroup-rule='c 10:229 rwm' --shm-size 64m \
    --memory 8g --memory-swap 8g --cpus 4 --pids-limit 128 \
    --mount "type=volume,src=$name,dst=/fixture-volume,volume-nocopy" \
    --mount "type=bind,src=$binaries,dst=/input,readonly" \
    --mount "type=bind,src=$harness,dst=/h,readonly" \
    -e PYTHONDONTWRITEBYTECODE=1 -e TMPDIR=/dev/shm -e "RAW_LARGE_MODE=$mode" "$image_id" \
    timeout --kill-after=30s 75m python3 -u /h/large_mounted.py
