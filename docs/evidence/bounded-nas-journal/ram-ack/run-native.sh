#!/usr/bin/env bash
# From a private /var/tmp build root with src/, target/ci/zerofs and evidence/.
set -euo pipefail
umask 077
case "$PWD" in /var/tmp/zerofs-ram-ack.*) ;; *) exit 2 ;; esac
kind=${1:?disk, replay, or pressure}
mode=${2:?materialized or volatile}
files=${3:-6}
file_mib=${4:-36}
trial=${5:-1}
[[ $trial =~ ^[0-9]+$ && $files =~ ^[0-9]+$ && $file_mib =~ ^[0-9]+$ ]]
source_config="$PWD/src/scripts/fixtures/nas-journal/engine.toml.in"
result="$PWD/evidence/$kind-$mode-$files-$file_mib-$trial"
mkdir "$result"
config="$result/engine.toml.in"
case "$mode" in
    materialized) cp "$source_config" "$config" ;;
    volatile) python3 - "$source_config" "$config" <<'PY'
from pathlib import Path
import sys
source = Path(sys.argv[1]).read_text()
line = 'write_ack_mode = "materialized"'
assert source.count(line) == 1
Path(sys.argv[2]).write_text(source.replace(line,
    'write_ack_mode = "volatile_memory"\nvolatile_memory_gb = 0.268435456\nvolatile_max_operations = 2048'))
PY
    ;;
    *) exit 2 ;;
esac
members=(/a /b)
case "$kind" in
    disk) script=test-nas-journal-file-drain.py; members=(/scratch /scratch); mounts=() ;;
    replay) script=test-nas-journal.py; size=256m ;;
    pressure) script=test-nas-journal-pressure.py; size=384m ;;
    *) exit 2 ;;
esac
if [[ $kind != disk ]]; then
    mounts=(--tmpfs "/a:rw,nosuid,nodev,size=$size,mode=1777" --tmpfs "/b:rw,nosuid,nodev,size=$size,mode=1777")
fi
docker run --rm --pull=never --name "zerofs-ram-${PWD##*.}-$kind" \
    --network none --cap-drop ALL --security-opt no-new-privileges \
    --memory 8g --memory-swap 8g --cpus 2 --pids-limit 256 --user "$(id -u):$(id -g)" \
    "${mounts[@]}" -v "$PWD/src/scripts:/fixtures:ro" -v "$PWD/target/ci/zerofs:/bin/zerofs:ro" \
    -v "$PWD/scratch:/scratch" -v "$result:/result" \
    sha256:5e2bdb1b0d542d5039cffb8181c65930093949194a65c365dfb1b0fe13a4fa5b \
    sh -c '
record() {
    for key in cpu.max cpu.stat memory.max memory.peak memory.events memory.swap.max memory.swap.peak; do
        cat "/sys/fs/cgroup/$key" >"/result/$1-$key" || return 1
    done
}
record start || exit 2
python -B "/fixtures/$1" --binary /bin/zerofs --scratch /scratch --evidence /result/run \
    --config-template /result/engine.toml.in --verify-config-template /fixtures/fixtures/nas-journal/verify.toml.in \
    --member-a "$2" --member-b "$3" --files "$4" --file-mib "$5" --timeout 600 >/result/stdout 2>&1
status=$?
record end || exit 2
exit "$status"
' sh "$script" "${members[@]}" "$files" "$file_mib"
