#!/usr/bin/env bash
# Run the raw-device increment in Studio Docker with private tmpfs-backed loop
# devices. The Rust build gets no devices. The fixture container has no network.
set -euo pipefail
engine_root=$(cd "$(dirname "$0")/.." && pwd)
image=${RAW_DEVICE_RUST_IMAGE:-rust:1.100.0-bookworm}
context=$(docker context show)
endpoint=$(docker context inspect "$context" --format '{{.Endpoints.docker.Host}}')
[[ $endpoint == unix://* && $(docker info --format '{{.Name}}') == docker-desktop ]] || {
    echo 'Studio Docker Desktop local Unix socket required' >&2
    exit 1
}
stage=$(mktemp -d /var/tmp/zerofs-raw-device.XXXXXXXX)
name="zerofs-raw-${stage##*.}"
cleanup() {
    local file cid
    for file in "$stage/build.cid" "$stage/probe.cid"; do
        [[ -s $file ]] || continue
        cid=$(cat "$file")
        [[ $cid =~ ^[0-9a-f]{64}$ ]] || return 1
        if docker container inspect "$cid" >/dev/null 2>&1; then
            docker rm -f "$cid" >/dev/null || return 1
        fi
    done
    rm -rf -- "$stage"
    test ! -e "$stage"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
docker run --rm --name "$name-build" --cidfile "$stage/build.cid" --cpus 2 --memory 1g --memory-swap 1g \
    -v "$engine_root/zerofs/raw-device:/src:ro" -v "$stage:/out" -w /src \
    -e CARGO_TARGET_DIR=/out/target "$image" sh -ec '
    cargo test --locked
    cargo build --locked
    sha256sum /out/target/debug/zerofs-raw-device'
docker run --rm --name "$name-probe" --cidfile "$stage/probe.cid" --network none --read-only \
    --cap-drop ALL --cap-add SYS_ADMIN --cap-add MKNOD --device=/dev/loop-control \
    --device-cgroup-rule='b 7:* rwm' --shm-size 128m --memory 512m \
    --memory-swap 512m --cpus 2 --pids-limit 80 \
    -v "$engine_root/scripts/raw-device:/h:ro" \
    -v "$stage/target/debug/zerofs-raw-device:/probe:ro" \
    -e PYTHONDONTWRITEBYTECODE=1 "$image" python3 -u /h/qualify.py /probe
