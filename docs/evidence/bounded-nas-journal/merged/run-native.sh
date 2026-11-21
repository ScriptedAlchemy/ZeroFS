#!/usr/bin/env bash
# Run inside the owned ubuntu-main scratch root containing target/ci and src/.
set -euo pipefail
umask 077
case "$PWD" in
    /var/tmp/zerofs-nas-journal-merged.*) ;;
    *) exit 2 ;;
esac
case "${1-}" in
    replay) fixture=test-nas-journal.py; member_size=256m ;;
    pressure) fixture=test-nas-journal-pressure.py; member_size=384m ;;
    disk) fixture=test-nas-journal-file-drain.py; member_size=1m ;;
    *) exit 2 ;;
esac
case_id="$1"
directory="$PWD/evidence/$case_id"
mkdir "$directory"
mkdir -p "$PWD/scratch"
member_a=/a
member_b=/b
mounts=(--tmpfs "/a:rw,nosuid,nodev,size=$member_size,mode=1777" --tmpfs "/b:rw,nosuid,nodev,size=$member_size,mode=1777")
if [[ $case_id == disk ]]; then
    member_a=/scratch
    member_b=/scratch
    mounts=()
fi
docker run --pull=never --rm --name "zerofs-journal-${PWD##*.}-$case_id" \
    --network none --memory 8g --memory-swap 8g --cpus 2 --pids-limit 256 \
    --cap-drop ALL --security-opt no-new-privileges --user "$(id -u):$(id -g)" \
    "${mounts[@]}" \
    -v "$PWD/target/ci/zerofs:/bin/zerofs:ro" -v "$PWD/src/scripts:/fixtures:ro" \
    -v "$PWD/scratch:/scratch" -v "$directory:/result" \
    sha256:5e2bdb1b0d542d5039cffb8181c65930093949194a65c365dfb1b0fe13a4fa5b \
    sh -c '
set -u
for key in memory.max memory.swap.max cpu.max memory.events memory.peak; do
    cat "/sys/fs/cgroup/$key" >"/result/start-$key"
done
python /fixtures/"$1" --binary /bin/zerofs --scratch /scratch --evidence /result/run \
    --config-template /fixtures/fixtures/nas-journal/engine.toml.in \
    --verify-config-template /fixtures/fixtures/nas-journal/verify.toml.in \
    --member-a "$2" --member-b "$3" --timeout 300 >/result/stdout 2>&1 &
driver=$!
while kill -0 "$driver" 2>/dev/null; do
    date +%s.%N | tr "\n" " "
    cat /sys/fs/cgroup/memory.current | tr "\n" " "
    awk "/^(anon|file|kernel) / {printf \"%s=%s \",\$1,\$2}" /sys/fs/cgroup/memory.stat
    echo
    sleep 0.5
done >/result/memory.samples
wait "$driver"; result=$?
for key in memory.events memory.peak; do
    cat "/sys/fs/cgroup/$key" >"/result/end-$key"
done
exit "$result"
' sh "$fixture" "$member_a" "$member_b"
