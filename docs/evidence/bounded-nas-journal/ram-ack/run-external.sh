#!/usr/bin/env bash
# Run from an owned /var/tmp build root containing src/, target/ci/zerofs and evidence/.
set -euo pipefail
umask 077
case "$PWD" in /var/tmp/zerofs-ram-ack.*) ;; *) exit 2 ;; esac
mode=${1:?materialized or volatile}
trial=${2:?trial number}
source_commit=${3:?engine source commit}
binary_sha256=${4:?engine SHA-256}
[[ "$mode" == materialized || "$mode" == volatile ]]
[[ "$trial" =~ ^[0-9]+$ && "$source_commit" =~ ^[0-9a-f]{40}$ && "$binary_sha256" =~ ^[0-9a-f]{64}$ ]]
[[ "$(sha256sum "$PWD/target/ci/zerofs" | cut -d' ' -f1)" == "$binary_sha256" ]]
result="$PWD/evidence/external-$mode-$trial"
mkdir "$result"
work=$(mktemp -d /var/tmp/zerofs-external.XXXXXXXX)
client="zerofs-external-${work##*.}-client"
service="zerofs-external-${work##*.}-service"
cleanup() {
    status=$?
    trap - EXIT
    for name in "$client" "$service"; do
        if docker container inspect "$name" >/dev/null 2>&1; then
            docker rm -f "$name" || status=1
        fi
        if docker container inspect "$name" >/dev/null 2>&1; then
            status=1
        else
            echo "container absent $name"
        fi
    done
    if rm -rf -- "$work" && [[ ! -e "$work" ]]; then
        echo "scratch absent $work"
    else
        status=1
    fi
    exit "$status"
}
trap 'cleanup >"$result/cleanup.txt" 2>&1' EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
mkdir "$work/ipc" "$work/scratch"
config="$result/engine.toml.in"
python3 - "$mode" "$config" <<'PY'
from pathlib import Path
import sys
source = Path("src/scripts/fixtures/nas-journal/engine.toml.in").read_text()
original = 'write_ack_mode = "materialized"'
assert source.count(original) == 1
if sys.argv[1] == "volatile":
    source = source.replace(original, 'write_ack_mode = "volatile_memory"\n'
        'volatile_memory_gb = 0.268435456\nvolatile_max_operations = 2048')
Path(sys.argv[2]).write_text(source)
PY
sha256sum "$PWD/target/ci/zerofs" "$config" > "$result/inputs.sha256"
printf '%s\n' "$source_commit" > "$result/source.txt"
for cpu in {0..7}; do
    read -r package < "/sys/devices/system/cpu/cpu$cpu/topology/physical_package_id"
    read -r core < "/sys/devices/system/cpu/cpu$cpu/topology/core_id"
    echo "$cpu $package $core"
done > "$result/cpu-topology.txt"
[[ "$(cut -d' ' -f2- "$result/cpu-topology.txt" | sort -u | wc -l)" -eq 8 ]]
common=(--pull=never --network none --cgroupns private --cap-drop ALL
    --security-opt no-new-privileges --pids-limit 256 --user "$(id -u):$(id -g)"
    -v "$PWD/src/scripts:/fixtures:ro" -v "$work/ipc:/ipc")
image=sha256:5e2bdb1b0d542d5039cffb8181c65930093949194a65c365dfb1b0fe13a4fa5b
script=/fixtures/test-nas-journal-external-burst.py
docker create --name "$client" "${common[@]}" --cpuset-cpus 2-7 \
    --memory 2g --memory-swap 2g "$image" python -B "$script" --client > "$result/client.id"
docker create --name "$service" "${common[@]}" --cpuset-cpus 0,1 \
    --memory 8g --memory-swap 8g -v "$work/scratch:/scratch" \
    -v "$PWD/target/ci/zerofs:/bin/zerofs:ro" -v "$result:/result" \
    "$image" python -B "$script" --binary /bin/zerofs --scratch /scratch \
    --evidence /result/run --config-template /result/engine.toml.in \
    --verify-config-template /fixtures/fixtures/nas-journal/verify.toml.in \
    --member-a /scratch --member-b /scratch --timeout 600 > "$result/service.id"
docker inspect "$service" "$client" | python3 -c '
import json, sys
rows = []
for container in json.load(sys.stdin):
    host = container["HostConfig"]
    keys = ("NetworkMode", "Memory", "MemorySwap", "CpusetCpus", "CgroupnsMode",
            "CapDrop", "SecurityOpt", "Privileged", "Binds")
    rows.append({"name": container["Name"], "image": container["Image"],
                 **{key: host[key] for key in keys}})
print(json.dumps(rows, indent=2, sort_keys=True))
' > "$result/containers.json"
docker start "$client" >/dev/null
docker start "$service" >/dev/null
service_status=$(docker wait "$service")
docker logs "$service" >"$result/service.log" 2>&1
if [[ "$service_status" != 0 ]]; then
    docker logs "$client" >"$result/client.log" 2>&1
    exit 1
fi
client_status=$(docker wait "$client")
docker logs "$client" >"$result/client.log" 2>&1
printf 'service %s\nclient %s\n' "$service_status" "$client_status" > "$result/exit.txt"
[[ "$client_status" == 0 ]]
