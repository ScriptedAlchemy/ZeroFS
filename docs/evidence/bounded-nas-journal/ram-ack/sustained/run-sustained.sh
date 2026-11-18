#!/usr/bin/env bash
# Run from a private /var/tmp root with zerofs, source/, scripts/, and evidence/.
set -euo pipefail
umask 077
case "$PWD" in /var/tmp/zerofs-sustained.*) ;; *) exit 2 ;; esac
mode=${1:?materialized or volatile}
[[ "$mode" == materialized || "$mode" == volatile ]]
digest=93488a4e5a160de9fd0e170568a8dae05beddfc7ab64b615894e8a482d75bebb
[[ "$(sha256sum zerofs | cut -d' ' -f1)" == "$digest" ]]
result="$PWD/evidence/$mode"
mkdir "$result"
scratch=$(mktemp -d /var/tmp/zerofs-sustained-run.XXXXXXXX)
name=zerofs-sustained-${scratch##*.}
cleanup() {
    status=$?
    trap - EXIT
    if docker container inspect "$name" >/dev/null 2>&1; then
        docker rm -f "$name" >/dev/null || status=1
    fi
    if docker container inspect "$name" >/dev/null 2>&1; then
        status=1
    else
        echo "container absent $name"
    fi
    if rm -rf -- "$scratch" && [[ ! -e "$scratch" ]]; then
        echo "scratch absent $scratch"
    else
        status=1
    fi
    exit "$status"
}
trap 'cleanup >"$result/cleanup.txt" 2>&1' EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
python3 - "$mode" "$result" <<'PY'
from pathlib import Path
import sys
for kind in ("engine", "verify"):
    name = f"{kind}.toml.in"
    source = (Path("scripts/fixtures/nas-journal") / name).read_text()
    assert source.count("capacity_gb = 1.0") == 2
    source = source.replace("capacity_gb = 1.0", "capacity_gb = 8.0")
    original = 'write_ack_mode = "materialized"'
    assert source.count(original) == 1
    if kind == "engine" and sys.argv[1] == "volatile":
        source = source.replace(original, 'write_ack_mode = "volatile_memory"\n'
            'volatile_memory_gb = 0.268435456\nvolatile_max_operations = 2048')
    (Path(sys.argv[2]) / name).write_text(source)
PY
for cpu in 0 1; do
    read -r package < "/sys/devices/system/cpu/cpu$cpu/topology/physical_package_id"
    read -r core < "/sys/devices/system/cpu/cpu$cpu/topology/core_id"
    echo "$cpu $package $core"
done > "$result/cpu-topology.txt"
[[ "$(cut -d' ' -f2- "$result/cpu-topology.txt" | sort -u | wc -l)" -eq 2 ]]
image=sha256:5e2bdb1b0d542d5039cffb8181c65930093949194a65c365dfb1b0fe13a4fa5b
docker create --pull=never --name "$name" --network none --cgroupns private \
    --cap-drop ALL --security-opt no-new-privileges --pids-limit 256 \
    --user "$(id -u):$(id -g)" --cpuset-cpus 0,1 --memory 8g --memory-swap 8g \
    -v "$PWD/scripts:/fixtures:ro" -v "$PWD/source:/source:ro" \
    -v "$PWD/zerofs:/bin/zerofs:ro" -v "$scratch:/scratch" -v "$result:/result" \
    "$image" sh -c '
record() {
    for key in cpu.max cpu.stat cpuset.cpus.effective memory.max memory.peak memory.events memory.swap.max memory.swap.current memory.swap.peak; do
        cat "/sys/fs/cgroup/$key" >"/result/$1-$key" || return 1
    done
}
test "$(cat /proc/self/cgroup)" = "0::/" || exit 2
test "$(cat /sys/fs/cgroup/cpuset.cpus.effective)" = "0-1" || exit 2
test "$(cat /sys/fs/cgroup/memory.max)" = 8589934592 || exit 2
test "$(cat /sys/fs/cgroup/memory.swap.max)" = 0 || exit 2
record start || exit 2
python -B /fixtures/test-nas-journal-file-drain.py --binary /bin/zerofs \
    --scratch /scratch --evidence /result/run --input-dir /source \
    --config-template /result/engine.toml.in \
    --verify-config-template /result/verify.toml.in \
    --member-a /scratch --member-b /scratch --files 6 --file-mib 768 --msize-kib 1024 \
    --timeout 600
status=$?
record end || exit 2
test "$(cat /result/end-memory.swap.current)" = 0 || exit 2
test "$(cat /result/end-memory.swap.peak)" = 0 || exit 2
cmp /result/start-memory.events /result/end-memory.events || exit 2
exit "$status"
' > "$result/container.id"
docker inspect "$name" --format '{{json .HostConfig}}' | python3 -c '
import json, sys
host = json.load(sys.stdin)
keys = ("NetworkMode", "Memory", "MemorySwap", "CpusetCpus", "CgroupnsMode",
        "CapDrop", "SecurityOpt", "Privileged", "Binds")
print(json.dumps({key: host[key] for key in keys}, indent=2, sort_keys=True))
' > "$result/container.json"
docker start "$name" >/dev/null
status=$(docker wait "$name")
docker logs "$name" > "$result/stdout" 2>&1
printf '%s\n' "$status" > "$result/exit.txt"
[[ "$status" == 0 ]]
