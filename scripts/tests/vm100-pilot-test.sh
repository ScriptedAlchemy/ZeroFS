#!/usr/bin/env bash
# The single-quoted bodies below are scripts written into fake executables; their
# variables must expand when the fake runs, not while the test creates it.
# shellcheck disable=SC2016
set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
SCRIPT="$ROOT/scripts/vm100-pilot.sh"
TEST_ROOT=$(mktemp -d "${TMPDIR:-/tmp}/vm100-pilot-test.XXXXXX")
trap '[[ ${KEEP_TEST_ROOT:-0} == 1 ]] || rm -rf "$TEST_ROOT"' EXIT

pass=0
fail=0

record_pass() {
  printf 'ok - %s\n' "$1"
  pass=$((pass + 1))
}

record_fail() {
  printf 'not ok - %s\n' "$1" >&2
  fail=$((fail + 1))
}

new_fixture() {
  FIXTURE=$(mktemp -d "$TEST_ROOT/case.XXXXXX")
  FAKEBIN="$FIXTURE/bin"
  mkdir -p "$FAKEBIN" "$FIXTURE/results" "$FIXTURE/tmp" "$FIXTURE/mount/metadata" "$FIXTURE/cgroup/pilot"
  printf '456\n' >"$FIXTURE/cgroup/pilot/cgroup.procs"
  : >"$FIXTURE/env"
  printf '0\n' >"$FIXTURE/date-counter"
  printf 'sentinel\n' >"$FIXTURE/mount/integrity.bin"
  printf 'metadata\n' >"$FIXTURE/mount/metadata/one"
  printf 'binary\n' >"$FIXTURE/zerofs"
  chmod +x "$FIXTURE/zerofs"
  printf 'commit=test-deployed-commit\nbinary_sha256=%s\n' "$(sha256sum "$FIXTURE/zerofs" | awk '{print $1}')" >"$FIXTURE/build-receipt"
  cat >"$FIXTURE/config.toml" <<EOF
[storage]
url = "sftp://user@example.invalid:23/zerofs"

[cache]
dir = "$FIXTURE/read-cache"
disk_size_gb = 10.0

[writeback]
enabled = true
dir = "$FIXTURE/writeback"
ack_mode = "memory"
memory_size_gb = 1.0
disk_size_gb = 10.0
min_free_gb = 1.0
EOF
  mkdir -p "$FIXTURE/proc/123"
  printf '%s\0run\0--config\0%s\0' "$FIXTURE/zerofs" "$FIXTURE/config.toml" >"$FIXTURE/proc/123/cmdline"
  ln -s "$FIXTURE/zerofs" "$FIXTURE/proc/123/exe"
  cat >"$FIXTURE/metrics" <<'EOF'
zerofs_writeback_accepted_sequence 9
zerofs_writeback_local_sequence 9
zerofs_writeback_remote_sequence 9
zerofs_writeback_dirty_ram_bytes 0
zerofs_writeback_dirty_ssd_bytes 0
zerofs_writeback_local_bytes_completed_total 1048576
zerofs_writeback_remote_bytes_completed_total 1048576
zerofs_writeback_terminal_error 0
EOF

  make_fake hostname 'printf "ubuntu-main\n"'
  make_fake flock '[[ ${FAKE_FLOCK_FAIL:-0} != 1 ]]'
  make_fake sudo 'exec "$@"'
  make_fake findmnt '
if [[ ${FAKE_EXACT_MOUNT_REMAINS:-0} == 1 ]]; then
  if [[ $* == "-rn -M $ZEROFS_PILOT_MOUNTPOINT" ]]; then exit 0; fi
  if [[ $* == "-rn $ZEROFS_PILOT_MOUNTPOINT" ]]; then exit 1; fi
fi
if [[ ${1:-} == -rn ]]; then exit 1; fi
printf "/dev/nbd0 xfs %s\n" "$ZEROFS_PILOT_MOUNTPOINT"'
  make_fake find 'if [[ $* == *"-printf ."* ]]; then /usr/bin/find "$1" -type f | while IFS= read -r _; do printf .; done; else exec /usr/bin/find "$@"; fi'
  make_fake sha256sum 'exec /usr/bin/shasum -a 256 "$@"'
  make_fake curl 'cat "$FAKE_METRICS_FILE"'
  make_fake sleep 'printf "sleep\n" >>"$FAKE_CALL_LOG"; /bin/sleep 0.001'
  make_fake timeout 'shift; exec "$@"'
  make_fake date '
if [[ $* == *"%s%3N"* ]]; then
  while ! mkdir "$FAKE_DATE_COUNTER.lock" 2>/dev/null; do /bin/sleep 0.001; done
  value=$(cat "$FAKE_DATE_COUNTER")
  value=$((value + 10))
  printf "%s\n" "$value" >"$FAKE_DATE_COUNTER"
  rmdir "$FAKE_DATE_COUNTER.lock"
  printf "1700000000%03d\n" "$value"
else
  exec /bin/date "$@"
fi'
  make_fake systemctl '
case ${1:-} in
  stop)
    printf "systemctl %s\n" "$*" >>"$FAKE_CALL_LOG"
    if [[ ${FAKE_BLOCKING_STOP:-0} == 1 && " $* " != *" --no-block "* ]]; then exit 88; fi
    unit=${@: -1}; printf "stop %s\n" "$unit" >>"$FAKE_CALL_LOG"; exit 0 ;;
  start) printf "start %s\n" "$2" >>"$FAKE_CALL_LOG"; exit 0 ;;
  reset-failed) exit 0 ;;
  is-active)
    if [[ ${FAKE_STICKY_ACTIVE:-0} == 1 ]]; then printf "active\n"; exit 0; fi
    last=$(grep -F -e "stop $2" -e "start $2" "$FAKE_CALL_LOG" 2>/dev/null | tail -1 || true)
    if [[ $last == "stop $2" ]]; then printf "inactive\n"; exit 3; fi
    printf "active\n"; exit 0 ;;
  show)
    case "$*" in
      *ActiveState*)
        if [[ ${FAKE_STICKY_ACTIVE:-0} == 1 ]]; then printf "active\n"
        elif [[ -n ${FAKE_STUCK_TRANSITION:-} ]]; then printf "%s\n" "$FAKE_STUCK_TRANSITION"
        elif grep -Fq "stop $2" "$FAKE_CALL_LOG" 2>/dev/null; then printf "inactive\n"
        else printf "active\n"
        fi ;;
      *MainPID*)
        if [[ ${FAKE_STICKY_ACTIVE:-0} == 1 || ${FAKE_STALE_MAINPID:-0} == 1 ]]; then printf "123\n"
        elif grep -Fq "stop $2" "$FAKE_CALL_LOG" 2>/dev/null; then printf "0\n"
        else printf "123\n"
        fi ;;
      *ControlPID*) printf "0\n" ;;
      *ControlGroup*)
        if [[ ${FAKE_CGROUP_PROCESS:-0} == 1 ]]; then printf "/pilot\n"; else printf "\n"; fi ;;
      *NRestarts*) printf "0\n" ;;
      *) printf "0\n" ;;
    esac
    exit 0 ;;
  status) exit 0 ;;
esac
exit 1'
  make_fake fio '
name= rw= direct= output= directory= filename_format=
for arg in "$@"; do
  case $arg in
    --name=*) name=${arg#--name=} ;;
    --rw=*) rw=${arg#--rw=} ;;
    --direct=*) direct=${arg#--direct=} ;;
    --output=*) output=${arg#--output=} ;;
    --directory=*) directory=${arg#--directory=} ;;
    --filename_format=*) filename_format=${arg#--filename_format=} ;;
  esac
done
printf "fio name=%s rw=%s direct=%s\n" "$name" "$rw" "${direct:-unset}" >>"$FAKE_CALL_LOG"
if [[ $rw == write && -n $directory && -n $filename_format ]]; then
  file=${filename_format//\$jobnum/0}; : >"$directory/$file"
fi
if [[ -n $output ]]; then
  if [[ $rw == read ]]; then printf "READ: bw=2MiB/s\n" >"$output"; else printf "WRITE: bw=1MiB/s\n" >"$output"; fi
fi
if [[ ${FAKE_FIO_FAIL_NAME:-} == "$name" ]]; then exit 19; fi
exit "${FAKE_FIO_RC:-0}"'
  make_fake sync '
if [[ ${FAKE_ADVANCE_LOCAL_DURABLE:-0} == 1 ]]; then
  sed -i.bak "s/zerofs_writeback_local_bytes_completed_total 1048576/zerofs_writeback_local_bytes_completed_total 3145728/" "$FAKE_METRICS_FILE"
  sed -i.bak2 "s/zerofs_writeback_remote_bytes_completed_total 1048576/zerofs_writeback_remote_bytes_completed_total 3145728/" "$FAKE_METRICS_FILE"
fi
printf "sync %s\n" "$*" >>"$FAKE_CALL_LOG"
exit 0'
  make_fake fallocate 'target=${@: -1}; : >"$target"'
  make_fake sftp '
input=$(cat)
if [[ $input == put\ * ]]; then
  printf "upload_start %s\n" "$$" >>"$FAKE_CALL_LOG"
  trap '\''printf "upload_end %s\n" "$$" >>"$FAKE_CALL_LOG"'\'' EXIT
  if [[ ${FAKE_SFTP_FAIL_UPLOAD:-0} == 1 && $input == *file-0.bin* ]]; then exit 23; fi
  /bin/sleep 0.05
fi
exit 0'
}

make_fake() {
  local name=$1 body=$2
  {
    printf '#!/usr/bin/env bash\nset -euo pipefail\n'
    printf '%s\n' "$body"
  } >"$FAKEBIN/$name"
  chmod +x "$FAKEBIN/$name"
}

run_pilot() {
  local command=$1
  shift
  env \
    PATH="$FAKEBIN:/opt/homebrew/bin:/usr/bin:/bin" \
    FAKE_CALL_LOG="$FIXTURE/calls.log" \
    FAKE_METRICS_FILE="$FIXTURE/metrics" \
    FAKE_DATE_COUNTER="$FIXTURE/date-counter" \
    ZEROFS_PILOT_CONFIG="$FIXTURE/config.toml" \
    ZEROFS_PILOT_ENV="$FIXTURE/env" \
    ZEROFS_PILOT_BINARY="$FIXTURE/zerofs" \
    ZEROFS_PILOT_BUILD_RECEIPT="$FIXTURE/build-receipt" \
    ZEROFS_PILOT_PROC_ROOT="$FIXTURE/proc" \
    ZEROFS_PILOT_CGROUP_ROOT="$FIXTURE/cgroup" \
    ZEROFS_PILOT_LOCK_FILE="$FIXTURE/pilot.lock" \
    ZEROFS_PILOT_TMP_DIR="$FIXTURE/tmp" \
    ZEROFS_PILOT_RESULT_DIR="$FIXTURE/results" \
    ZEROFS_PILOT_MOUNTPOINT="$FIXTURE/mount" \
    ZEROFS_PILOT_INTEGRITY_FILE="$FIXTURE/mount/integrity.bin" \
    ZEROFS_PILOT_INTEGRITY_SHA256="$(sha256sum "$FIXTURE/mount/integrity.bin" | awk '{print $1}')" \
    ZEROFS_PILOT_METADATA_DIR="$FIXTURE/mount/metadata" \
    ZEROFS_PILOT_METADATA_FILE_COUNT=1 \
    ZEROFS_PILOT_STOP_TIMEOUT=2 \
    "$@" bash "$SCRIPT" "$command"
}

test_global_lock_rejects_overlap() {
  new_fixture
  if FAKE_FLOCK_FAIL=1 run_pilot status >"$FIXTURE/out" 2>"$FIXTURE/err"; then
    return 1
  fi
  grep -q 'another vm100-pilot operation is already running' "$FIXTURE/err" || return 1
}

test_teardown_requires_every_unit_inactive() {
  new_fixture
  if FAKE_STICKY_ACTIVE=1 run_pilot teardown >"$FIXTURE/out" 2>"$FIXTURE/err"; then
    return 1
  fi
  grep -q 'did not reach a terminal stopped state' "$FIXTURE/err" || return 1
  [[ $(grep -c '^stop ' "$FIXTURE/calls.log") == 3 ]]
}

test_teardown_uses_nonblocking_stop_before_bounded_polling() {
  new_fixture
  FAKE_BLOCKING_STOP=1 run_pilot teardown >"$FIXTURE/out" 2>"$FIXTURE/err"
  [[ $(grep -c '^systemctl stop --no-block ' "$FIXTURE/calls.log") == 3 ]]
}

test_teardown_rejects_units_stuck_in_transitional_states() {
  local state
  for state in activating deactivating; do
    new_fixture
    if FAKE_STUCK_TRANSITION=$state run_pilot teardown >"$FIXTURE/out" 2>"$FIXTURE/err"; then
      return 1
    fi
    grep -q 'did not reach a terminal stopped state' "$FIXTURE/err" || return 1
    [[ $(grep -c '^sleep$' "$FIXTURE/calls.log") == 2 ]] || return 1
  done
}

test_teardown_rejects_inactive_unit_with_a_stale_main_pid() {
  new_fixture
  if FAKE_STALE_MAINPID=1 run_pilot teardown >"$FIXTURE/out" 2>"$FIXTURE/err"; then
    return 1
  fi
  grep -q 'MainPID=123' "$FIXTURE/err"
}

test_teardown_rejects_inactive_unit_with_a_cgroup_process() {
  new_fixture
  if FAKE_CGROUP_PROCESS=1 run_pilot teardown >"$FIXTURE/out" 2>"$FIXTURE/err"; then
    return 1
  fi
  grep -q 'cgroup_pids=456' "$FIXTURE/err"
}

test_teardown_checks_the_exact_mount_target() {
  new_fixture
  if FAKE_EXACT_MOUNT_REMAINS=1 run_pilot teardown >"$FIXTURE/out" 2>"$FIXTURE/err"; then
    return 1
  fi
  grep -q "$FIXTURE/mount remains mounted" "$FIXTURE/err"
}

test_wait_drain_fails_before_sleep_on_terminal_error() {
  new_fixture
  sed -i.bak 's/zerofs_writeback_terminal_error 0/zerofs_writeback_terminal_error 1/' "$FIXTURE/metrics"
  if run_pilot drain >"$FIXTURE/out" 2>"$FIXTURE/err"; then
    return 1
  fi
  grep -q 'terminal error' "$FIXTURE/err" || return 1
  [[ ! -e $FIXTURE/calls.log ]] || ! grep -q '^sleep$' "$FIXTURE/calls.log"
}

test_status_rejects_unexpected_ack_mode() {
  new_fixture
  sed -i.bak 's/ack_mode = "memory"/ack_mode = "remote"/' "$FIXTURE/config.toml"
  if run_pilot status >"$FIXTURE/out" 2>"$FIXTURE/err"; then
    return 1
  fi
  grep -q 'ack_mode is remote, expected memory' "$FIXTURE/err" || return 1
}

test_status_records_runtime_and_durability_receipt() {
  new_fixture
  run_pilot status >"$FIXTURE/out" 2>"$FIXTURE/err"
  grep -q '^running_binary_sha256=' "$FIXTURE/out" || return 1
  grep -q '^deployed_commit=test-deployed-commit$' "$FIXTURE/out" || return 1
  grep -q '^config_sha256=' "$FIXTURE/out" || return 1
  grep -q '^zerofs_writeback_local_bytes_completed_total=1048576$' "$FIXTURE/out" || return 1
  grep -q 'writeback_enabled=true ack_mode=memory' "$FIXTURE/out"
}

test_status_rejects_stale_deployed_build_receipt() {
  new_fixture
  sed -i.bak 's/^binary_sha256=.*/binary_sha256=stale/' "$FIXTURE/build-receipt"
  if run_pilot status >"$FIXTURE/out" 2>"$FIXTURE/err"; then return 1; fi
  grep -q 'build receipt binary hash' "$FIXTURE/err"
}

test_status_rejects_terminal_writeback_error() {
  new_fixture
  sed -i.bak 's/zerofs_writeback_terminal_error 0/zerofs_writeback_terminal_error 1/' "$FIXTURE/metrics"
  if run_pilot status >"$FIXTURE/out" 2>"$FIXTURE/err"; then return 1; fi
  grep -q 'terminal error' "$FIXTURE/err"
}

test_failed_benchmark_cleans_sampler_and_keeps_evidence() {
  new_fixture
  if FAKE_FIO_RC=17 ZEROFS_BENCH_TOTAL_MIB=4 ZEROFS_BENCH_JOBS=1 run_pilot benchmark >"$FIXTURE/out" 2>"$FIXTURE/err"; then
    return 1
  fi
  if find "$FIXTURE/mount" -maxdepth 1 -name '.zerofs-bench-*' | grep -q .; then return 1; fi
  if find "$FIXTURE/tmp" -type f | grep -q .; then return 1; fi
  find "$FIXTURE/results" -name 'storage-*-status.txt' | grep -q . || return 1
  find "$FIXTURE/results" -name 'storage-*-metrics.csv' | grep -q . || return 1
  result=$(find "$FIXTURE/results" -name 'storage-*.txt' ! -name '*-status.txt' ! -name '*-drain.txt' ! -name '*-fio.txt' | head -1)
  grep -q '^cleanup_failed=0$' "$result" || return 1
  grep -q '^harness_exit_status=17$' "$result" || return 1
  grep -q '^sync -f ' "$FIXTURE/calls.log"
}

test_successful_benchmark_reports_measured_local_durable_throughput() {
  new_fixture
  FAKE_ADVANCE_LOCAL_DURABLE=1 ZEROFS_BENCH_TOTAL_MIB=4 ZEROFS_BENCH_JOBS=1 run_pilot benchmark >"$FIXTURE/out" 2>"$FIXTURE/err"
  result=$(find "$FIXTURE/results" -name 'storage-*.txt' ! -name '*-status.txt' ! -name '*-drain.txt' ! -name '*-fio.txt' | head -1)
  grep -q 'local_durable_bytes=2097152' "$result" || return 1
  grep -q 'local_active_ms=[1-9].*local_active_MiBps=' "$result" || return 1
  grep -q 'local_sync_wait_ms=.*local_durable_end_to_end_ms=.*foreground_to_local_durable_MiBps=' "$result" || return 1
  if grep -q 'throughput=not_computable_without_local_durable_byte_counter' "$result"; then return 1; fi
  local line sync_ms end_ms actual_mibps expected_mibps
  line=$(grep '^local_durable_bytes=' "$result")
  sync_ms=$(sed -E 's/.*local_sync_wait_ms=([0-9]+).*/\1/' <<<"$line")
  end_ms=$(sed -E 's/.*local_durable_end_to_end_ms=([0-9]+).*/\1/' <<<"$line")
  actual_mibps=$(sed -E 's/.*foreground_to_local_durable_MiBps=([0-9.]+).*/\1/' <<<"$line")
  expected_mibps=$(awk -v ms="$end_ms" 'BEGIN { printf "%.2f", 2/(ms/1000) }')
  [[ $end_ms -gt $sync_ms && $actual_mibps == "$expected_mibps" ]] || return 1
  grep -q 'remote_first_drained_end_to_end_ms=' "$result" || return 1
  grep -q 'remote_active_ms=[1-9].*remote_active_MiBps=' "$result" || return 1
  grep -q 'first_drained_epoch_ms=' "$FIXTURE/results"/storage-*-drain.txt
}

test_benchmark_reports_buffered_warm_and_direct_reads_separately() {
  new_fixture
  FAKE_ADVANCE_LOCAL_DURABLE=1 ZEROFS_BENCH_TOTAL_MIB=4 ZEROFS_BENCH_JOBS=1 run_pilot benchmark >"$FIXTURE/out" 2>"$FIXTURE/err"
  result=$(find "$FIXTURE/results" -name 'storage-*.txt' ! -name '*-status.txt' ! -name '*-drain.txt' ! -name '*-fio.txt' | head -1)
  grep -q '^buffered_cached_candidate_read_ms=.*buffered_cached_candidate_read_MiBps=.*cache=guest_page_cache_candidate_not_guaranteed_prewarmed' "$result" || return 1
  grep -q '^direct_read_ms=.*direct_read_MiBps=.*cache=guest_page_cache_bypass_zerofs_cache_eligible' "$result" || return 1
  [[ $(grep -c '^fio name=' "$FIXTURE/calls.log") == 3 ]] || return 1
  grep -q '^fio name=zerofs_buffered_warm_read rw=read direct=0$' "$FIXTURE/calls.log" || return 1
  grep -q '^fio name=zerofs_direct_read rw=read direct=1$' "$FIXTURE/calls.log" || return 1
  find "$FIXTURE/results" -name 'storage-*-write-fio.txt' | grep -q . || return 1
  find "$FIXTURE/results" -name 'storage-*-buffered-warm-read-fio.txt' | grep -q . || return 1
  find "$FIXTURE/results" -name 'storage-*-direct-read-fio.txt' | grep -q .
}

test_failed_direct_read_preserves_all_fio_evidence_and_cleans_files() {
  new_fixture
  if FAKE_ADVANCE_LOCAL_DURABLE=1 FAKE_FIO_FAIL_NAME=zerofs_direct_read ZEROFS_BENCH_TOTAL_MIB=4 ZEROFS_BENCH_JOBS=1 run_pilot benchmark >"$FIXTURE/out" 2>"$FIXTURE/err"; then
    return 1
  fi
  if find "$FIXTURE/mount" -maxdepth 1 -name '.zerofs-bench-*' | grep -q .; then return 1; fi
  find "$FIXTURE/results" -name 'storage-*-write-fio.txt' | grep -q . || return 1
  find "$FIXTURE/results" -name 'storage-*-buffered-warm-read-fio.txt' | grep -q . || return 1
  find "$FIXTURE/results" -name 'storage-*-direct-read-fio.txt' | grep -q .
}

test_raw_sftp_reaps_all_eight_override_workers_after_one_fails() {
  new_fixture
  if FAKE_SFTP_FAIL_UPLOAD=1 ZEROFS_RAW_SFTP_JOBS=8 run_pilot raw-sftp >"$FIXTURE/out" 2>"$FIXTURE/err"; then
    return 1
  fi
  [[ $(grep -c '^upload_start ' "$FIXTURE/calls.log") == 8 ]] || return 1
  [[ $(grep -c '^upload_end ' "$FIXTURE/calls.log") == 8 ]]
}

test_raw_sftp_defaults_to_seven_matched_streams_and_records_bytes() {
  new_fixture
  run_pilot raw-sftp >"$FIXTURE/out" 2>"$FIXTURE/err"
  grep -q 'raw_sftp_jobs=7 raw_sftp_bytes=939524096' "$FIXTURE/out" || return 1
  [[ $(grep -c '^upload_start ' "$FIXTURE/calls.log") == 7 ]]
}

run_test() {
  local name=$1
  if "$name"; then record_pass "$name"; else record_fail "$name"; fi
}

run_test test_global_lock_rejects_overlap
run_test test_teardown_requires_every_unit_inactive
run_test test_teardown_uses_nonblocking_stop_before_bounded_polling
run_test test_teardown_rejects_units_stuck_in_transitional_states
run_test test_teardown_rejects_inactive_unit_with_a_stale_main_pid
run_test test_teardown_rejects_inactive_unit_with_a_cgroup_process
run_test test_teardown_checks_the_exact_mount_target
run_test test_wait_drain_fails_before_sleep_on_terminal_error
run_test test_status_rejects_unexpected_ack_mode
run_test test_status_records_runtime_and_durability_receipt
run_test test_status_rejects_stale_deployed_build_receipt
run_test test_status_rejects_terminal_writeback_error
run_test test_failed_benchmark_cleans_sampler_and_keeps_evidence
run_test test_successful_benchmark_reports_measured_local_durable_throughput
run_test test_benchmark_reports_buffered_warm_and_direct_reads_separately
run_test test_failed_direct_read_preserves_all_fio_evidence_and_cleans_files
run_test test_raw_sftp_reaps_all_eight_override_workers_after_one_fails
run_test test_raw_sftp_defaults_to_seven_matched_streams_and_records_bytes

printf '%s passed; %s failed\n' "$pass" "$fail"
(( fail == 0 ))
