#!/usr/bin/env bash
# VM100 development/deployment harness for the SFTP-backed ZeroFS NBD pilot.
#
# This script never formats, deletes, or recreates the canonical NBD export,
# remote prefix, cache, or writeback journal. `teardown` only stops the mount,
# NBD client, and daemon in dependency order.
set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
CRATE="$ROOT/zerofs"
CONFIG=${ZEROFS_PILOT_CONFIG:-/etc/zerofs/nbd-pilot.toml}
ENV_FILE=${ZEROFS_PILOT_ENV:-/etc/zerofs/nbd-pilot.env}
BINARY=${ZEROFS_PILOT_BINARY:-/usr/local/bin/zerofs-nbd-pilot}
BUILD_RECEIPT=${ZEROFS_PILOT_BUILD_RECEIPT:-$BINARY.build-receipt}
SERVICE=${ZEROFS_PILOT_SERVICE:-zerofs-nbd-pilot.service}
CLIENT_SERVICE=${ZEROFS_PILOT_CLIENT_SERVICE:-zerofs-nbd-client.service}
MOUNT_UNIT=${ZEROFS_PILOT_MOUNT_UNIT:-}
[[ -n $MOUNT_UNIT ]] || MOUNT_UNIT='mnt-storagebox\x2dnbd\x2dpilot.mount'
MOUNTPOINT=${ZEROFS_PILOT_MOUNTPOINT:-/mnt/storagebox-nbd-pilot}
METRICS_URL=${ZEROFS_PILOT_METRICS_URL:-http://127.0.0.1:19567/metrics}
INTEGRITY_FILE=${ZEROFS_PILOT_INTEGRITY_FILE:-$MOUNTPOINT/integrity-v2.bin}
INTEGRITY_SHA256=${ZEROFS_PILOT_INTEGRITY_SHA256:-db1fb0431bce321750e25a93bd46ce41dd20d6eac1a512a9b56af99d43d43c83}
METADATA_DIR=${ZEROFS_PILOT_METADATA_DIR:-$MOUNTPOINT/metadata-v2}
METADATA_FILE_COUNT=${ZEROFS_PILOT_METADATA_FILE_COUNT:-1024}
RESULT_DIR=${ZEROFS_PILOT_RESULT_DIR:-/var/tmp/zerofs-pilot-results}
TMP_DIR=${ZEROFS_PILOT_TMP_DIR:-/tmp}
LOCK_FILE=${ZEROFS_PILOT_LOCK_FILE:-/var/tmp/zerofs-vm100-pilot.lock}
PROC_ROOT=${ZEROFS_PILOT_PROC_ROOT:-/proc}
EXPECTED_ACK_MODE=${ZEROFS_PILOT_EXPECT_ACK_MODE:-memory}
DRAIN_TIMEOUT=${ZEROFS_PILOT_DRAIN_TIMEOUT:-600}
STOP_TIMEOUT=${ZEROFS_PILOT_STOP_TIMEOUT:-60}
CGROUP_ROOT=${ZEROFS_PILOT_CGROUP_ROOT:-/sys/fs/cgroup}
CARGO_CMD=${ZEROFS_PILOT_CARGO:-}
NPM_WORKLOAD_REPO=${ZEROFS_NPM_WORKLOAD_REPO:-https://github.com/npm/cli.git}
NPM_WORKLOAD_COMMIT=${ZEROFS_NPM_WORKLOAD_COMMIT:-64763a341e7aa5b456e696f956759bf9b3440dc1}
RUST_WORKLOAD_REPO=${ZEROFS_RUST_WORKLOAD_REPO:-https://github.com/BurntSushi/ripgrep.git}
RUST_WORKLOAD_COMMIT=${ZEROFS_RUST_WORKLOAD_COMMIT:-af60c2de9d85e7f3d81c78601669468cf02dabab}
DELETE_JOBS=${ZEROFS_DELETE_JOBS:-4}
RAW_SFTP_JOBS=${ZEROFS_RAW_SFTP_JOBS:-7}

if [[ -z $CARGO_CMD ]]; then
  CARGO_CMD=$(command -v cargo 2>/dev/null || true)
fi
if [[ -z $CARGO_CMD && -x ${HOME:-}/.cargo/bin/cargo ]]; then
  CARGO_CMD=${HOME}/.cargo/bin/cargo
fi

log() { printf '[vm100-pilot] %s\n' "$*" >&2; }
die() { log "ERROR: $*"; exit 1; }

command -v flock >/dev/null 2>&1 || die "flock is required"
exec 9>>"$LOCK_FILE" || die "cannot open harness lock $LOCK_FILE"
flock -n 9 || die "another vm100-pilot operation is already running (lock: $LOCK_FILE)"

metric_from() {
  local name=$1 snapshot=$2
  awk -v n="$name" '$1 == n { print $2; exit }' <<<"$snapshot"
}

metrics_snapshot() { curl -fsS --connect-timeout 2 --max-time 5 "$METRICS_URL"; }

config_value() {
  local section=$1 key=$2
  sudo awk -v wanted_section="$section" -v wanted_key="$key" '
    /^[[:space:]]*\[/ {
      current = $0
      sub(/^[[:space:]]*\[/, "", current)
      sub(/\][[:space:]]*(#.*)?$/, "", current)
      next
    }
    current == wanted_section && $0 ~ "^[[:space:]]*" wanted_key "[[:space:]]*=" {
      value = $0
      sub(/^[^=]*=[[:space:]]*/, "", value)
      sub(/[[:space:]]*#.*$/, "", value)
      gsub(/^[[:space:]\"]+|[[:space:]\"]+$/, "", value)
      print value
      exit
    }
  ' "$CONFIG"
}

validate_runtime() {
  local main_pid running_exe cmdline installed_sha running_sha config_sha enabled ack_mode writeback_dir deployed_commit receipt_sha
  main_pid=$(systemctl show "$SERVICE" -p MainPID --value)
  [[ $main_pid =~ ^[1-9][0-9]*$ ]] || die "$SERVICE has no running MainPID"
  running_exe=$(sudo readlink "$PROC_ROOT/$main_pid/exe")
  [[ $running_exe == "$BINARY" ]] || die "$SERVICE is running $running_exe, expected $BINARY"
  cmdline=$(tr '\0' ' ' <"$PROC_ROOT/$main_pid/cmdline")
  case " $cmdline " in
    *" $BINARY "*) ;;
    *) die "$SERVICE command line does not use $BINARY" ;;
  esac
  case " $cmdline " in
    *" --config $CONFIG "*) ;;
    *) die "$SERVICE command line does not use --config $CONFIG" ;;
  esac

  installed_sha=$(sha256sum "$BINARY" | awk '{print $1}')
  running_sha=$(sudo sha256sum "$PROC_ROOT/$main_pid/exe" | awk '{print $1}')
  [[ $running_sha == "$installed_sha" ]] || die "running binary does not match installed binary"
  [[ -f $BUILD_RECEIPT ]] || die "deployed build receipt is missing: $BUILD_RECEIPT"
  deployed_commit=$(sudo awk -F= '$1 == "commit" { print $2; exit }' "$BUILD_RECEIPT")
  receipt_sha=$(sudo awk -F= '$1 == "binary_sha256" { print $2; exit }' "$BUILD_RECEIPT")
  [[ -n $deployed_commit ]] || die "deployed build receipt has no commit"
  [[ $receipt_sha == "$running_sha" ]] || die "build receipt binary hash does not match running binary"
  config_sha=$(sudo sha256sum "$CONFIG" | awk '{print $1}')

  enabled=$(config_value writeback enabled)
  ack_mode=$(config_value writeback ack_mode)
  writeback_dir=$(config_value writeback dir)
  [[ $enabled == true ]] || die "[writeback] enabled is ${enabled:-unset}, expected true"
  [[ $ack_mode == "$EXPECTED_ACK_MODE" ]] || die "[writeback] ack_mode is ${ack_mode:-unset}, expected $EXPECTED_ACK_MODE"
  [[ $writeback_dir == /* ]] || die "[writeback] dir must be an absolute path"

  printf 'running_binary_sha256=%s\n' "$running_sha"
  printf 'deployed_commit=%s\n' "$deployed_commit"
  printf 'config_sha256=%s\n' "$config_sha"
  printf 'writeback_enabled=%s ack_mode=%s writeback_dir=%s\n' "$enabled" "$ack_mode" "$writeback_dir"
}

require_vm100() {
  [[ $(hostname) == ubuntu-main ]] || die "run this only on VM100 (ubuntu-main)"
  [[ -f $CONFIG && -f $ENV_FILE ]] || die "pilot config/environment are missing"
}

wait_active() {
  local unit=$1 timeout=${2:-180}
  for _ in $(seq 1 "$timeout"); do
    [[ $(systemctl is-active "$unit" 2>/dev/null || true) == active ]] && return 0
    sleep 1
  done
  systemctl status "$unit" --no-pager -l >&2 || true
  return 1
}

wait_drain() {
  local timeout=${1:-$DRAIN_TIMEOUT} stable=0 snapshot accepted local_seq remote dirty_ram dirty_ssd terminal
  local started_ms first_drained_epoch_ms=0 now_ms
  started_ms=$(date +%s%3N)
  for _ in $(seq 1 "$timeout"); do
    if ! snapshot=$(metrics_snapshot 2>/dev/null); then
      stable=0
      sleep 1
      continue
    fi
    accepted=$(metric_from zerofs_writeback_accepted_sequence "$snapshot")
    local_seq=$(metric_from zerofs_writeback_local_sequence "$snapshot")
    remote=$(metric_from zerofs_writeback_remote_sequence "$snapshot")
    dirty_ram=$(metric_from zerofs_writeback_dirty_ram_bytes "$snapshot")
    dirty_ssd=$(metric_from zerofs_writeback_dirty_ssd_bytes "$snapshot")
    terminal=$(metric_from zerofs_writeback_terminal_error "$snapshot")
    [[ -n $accepted && -n $local_seq && -n $remote && -n $dirty_ram && -n $dirty_ssd && -n $terminal ]] \
      || die "writeback metrics snapshot is missing required fields"
    [[ $terminal == 0 ]] || die "writeback reported a terminal error while waiting for drain"
    if [[ $accepted == "$local_seq" && $accepted == "$remote" && $dirty_ram == 0 && $dirty_ssd == 0 && $terminal == 0 ]]; then
      now_ms=$(date +%s%3N)
      (( first_drained_epoch_ms > 0 )) || first_drained_epoch_ms=$now_ms
      stable=$((stable + 1))
    else
      stable=0
    fi
    if (( stable >= 4 )); then
      printf 'drained accepted=%s local=%s remote=%s dirty_ram=%s dirty_ssd=%s terminal=%s first_drained_epoch_ms=%s first_drained_wait_ms=%s\n' \
        "$accepted" "$local_seq" "$remote" "$dirty_ram" "$dirty_ssd" "$terminal" \
        "$first_drained_epoch_ms" "$((first_drained_epoch_ms - started_ms))"
      return 0
    fi
    sleep 1
  done
  die "writeback did not drain within ${timeout}s"
}

drain() {
  require_vm100
  wait_drain "$DRAIN_TIMEOUT"
}

status() {
  require_vm100
  local unit
  for unit in "$SERVICE" "$CLIENT_SERVICE" "$MOUNT_UNIT"; do
    [[ $(systemctl is-active "$unit" 2>/dev/null || true) == active ]] || die "$unit is not active"
    printf '%s=active\n' "$unit"
  done
  findmnt -no SOURCE,FSTYPE,TARGET -M "$MOUNTPOINT"
  local binary_sha integrity_sha metadata_count snapshot runtime_receipt terminal
  binary_sha=$(sha256sum "$BINARY" | awk '{print $1}')
  runtime_receipt=$(validate_runtime)
  snapshot=$(metrics_snapshot)
  terminal=$(metric_from zerofs_writeback_terminal_error "$snapshot")
  [[ -n $terminal ]] || die "writeback metrics snapshot is missing terminal-error state"
  [[ $terminal == 0 ]] || die "writeback reported a terminal error"
  integrity_sha=$(sudo timeout 180 sha256sum "$INTEGRITY_FILE" | awk '{print $1}')
  [[ $integrity_sha == "$INTEGRITY_SHA256" ]] || die "integrity sentinel hash mismatch"
  metadata_count=$(sudo find "$METADATA_DIR" -type f -printf . | wc -c | tr -d '[:space:]')
  [[ $metadata_count == "$METADATA_FILE_COUNT" ]] || die "metadata file count is $metadata_count, expected $METADATA_FILE_COUNT"
  printf 'binary_sha256=%s integrity_sha256=%s metadata_files=%s restarts=%s\n' \
    "$binary_sha" "$integrity_sha" "$metadata_count" \
    "$(systemctl show "$SERVICE" -p NRestarts --value)"
  printf '%s\n' "$runtime_receipt"
  for name in \
    zerofs_writeback_accepted_sequence zerofs_writeback_local_sequence \
    zerofs_writeback_remote_sequence zerofs_writeback_dirty_ram_bytes \
    zerofs_writeback_dirty_ssd_bytes zerofs_writeback_local_bytes_completed_total \
    zerofs_writeback_terminal_error; do
    printf '%s=%s\n' "$name" "$(metric_from "$name" "$snapshot")"
  done
}

teardown() {
  require_vm100
  [[ $STOP_TIMEOUT =~ ^[1-9][0-9]*$ ]] || die "ZEROFS_PILOT_STOP_TIMEOUT must be positive"
  log "stopping mount, NBD client, and ZeroFS daemon"
  sudo systemctl stop --no-block "$MOUNT_UNIT" || true
  sudo systemctl stop --no-block "$CLIENT_SERVICE" || true
  sudo systemctl stop --no-block "$SERVICE" || true
  local unit
  for unit in "$MOUNT_UNIT" "$CLIENT_SERVICE" "$SERVICE"; do
    wait_stopped "$unit" "$STOP_TIMEOUT" || die "$unit did not reach a terminal stopped state within ${STOP_TIMEOUT}s"
  done
  findmnt -rn -M "$MOUNTPOINT" >/dev/null && die "$MOUNTPOINT remains mounted"
  log "teardown complete; canonical data/cache/journal were retained"
}

wait_stopped() {
  local unit=$1 timeout=$2 state main_pid control_pid control_group process_file process_ids
  for _ in $(seq 1 "$timeout"); do
    state=$(systemctl show "$unit" -p ActiveState --value 2>/dev/null || true)
    main_pid=$(systemctl show "$unit" -p MainPID --value 2>/dev/null || true)
    control_pid=$(systemctl show "$unit" -p ControlPID --value 2>/dev/null || true)
    control_group=$(systemctl show "$unit" -p ControlGroup --value 2>/dev/null || true)
    main_pid=${main_pid:-0}
    control_pid=${control_pid:-0}
    process_ids=
    if [[ -n $control_group ]]; then
      process_file="$CGROUP_ROOT$control_group/cgroup.procs"
      if [[ -r $process_file ]]; then
        process_ids=$(tr '\n' ',' <"$process_file")
        process_ids=${process_ids%,}
      fi
    fi
    if [[ $state == inactive || $state == failed ]] \
      && [[ $main_pid == 0 && $control_pid == 0 && -z $process_ids ]]; then
      return 0
    fi
    sleep 1
  done
  log "$unit stop state: ActiveState=${state:-unknown} MainPID=$main_pid ControlPID=$control_pid cgroup_pids=${process_ids:-none}"
  systemctl status "$unit" --no-pager -l >&2 || true
  return 1
}

build_deploy() {
  require_vm100
  [[ -z $(git -C "$ROOT" status --porcelain) ]] || die "Git checkout is dirty"
  git -C "$ROOT" pull --ff-only
  [[ -z $(git -C "$ROOT" status --porcelain) ]] || die "Git checkout became dirty after pull"
  [[ -n $CARGO_CMD && -x $CARGO_CMD ]] || die "cargo was not found; set ZEROFS_PILOT_CARGO"
  log "building locked release at $(git -C "$ROOT" rev-parse --short HEAD)"
  (cd "$CRATE" && "$CARGO_CMD" build --release --locked)
  local built_sha deployed_commit receipt_tmp
  built_sha=$(sha256sum "$CRATE/target/release/zerofs" | awk '{print $1}')
  deployed_commit=$(git -C "$ROOT" rev-parse HEAD)
  teardown
  sudo install -m 0755 "$CRATE/target/release/zerofs" "$BINARY"
  [[ $(sha256sum "$BINARY" | awk '{print $1}') == "$built_sha" ]] || die "installed binary hash mismatch"
  receipt_tmp=$(mktemp "$TMP_DIR/zerofs-build-receipt.XXXXXX")
  printf 'commit=%s\nbinary_sha256=%s\n' "$deployed_commit" "$built_sha" >"$receipt_tmp"
  sudo install -o root -g root -m 0644 "$receipt_tmp" "$BUILD_RECEIPT"
  rm -f "$receipt_tmp"
  printf 'installed_binary_sha256=%s\n' "$built_sha"
}

start_stack() {
  require_vm100
  local after start_rc=0
  sudo systemctl reset-failed "$SERVICE" "$CLIENT_SERVICE" "$MOUNT_UNIT" || true
  sudo systemctl start "$SERVICE" || start_rc=$?
  wait_active "$SERVICE" 180 || die "ZeroFS daemon did not become active"
  sudo systemctl start "$CLIENT_SERVICE"
  wait_active "$CLIENT_SERVICE" 60 || die "NBD client did not become active"
  sudo systemctl start "$MOUNT_UNIT"
  wait_active "$MOUNT_UNIT" 60 || die "pilot mount did not become active"
  after=$(systemctl show "$SERVICE" -p NRestarts --value)
  printf 'startup_command_rc=%s startup_restarts=%s\n' "$start_rc" "$after"
  status
  (( start_rc == 0 && after == 0 )) || return 1
}

setup() {
  if [[ ${ZEROFS_SKIP_BUILD:-0} == 1 ]]; then
    start_stack
  else
    build_deploy
    start_stack
  fi
}

sample_metrics() {
  local output=$1 stop_file=$2
  while [[ ! -e $stop_file ]]; do
    local snapshot now
    snapshot=$(metrics_snapshot) || { sleep .25; continue; }
    now=$(date +%s%3N)
    append_metric_sample "$output" "$now" "$snapshot"
    sleep .25
  done
}

append_metric_sample() {
  local output=$1 now=$2 snapshot=$3
  printf '%s,%s,%s,%s,%s,%s,%s,%s\n' \
    "$now" \
    "$(metric_from zerofs_writeback_accepted_sequence "$snapshot")" \
    "$(metric_from zerofs_writeback_local_sequence "$snapshot")" \
    "$(metric_from zerofs_writeback_remote_sequence "$snapshot")" \
    "$(metric_from zerofs_writeback_dirty_ram_bytes "$snapshot")" \
    "$(metric_from zerofs_writeback_dirty_ssd_bytes "$snapshot")" \
    "$(metric_from zerofs_writeback_local_bytes_completed_total "$snapshot")" \
    "$(metric_from zerofs_writeback_remote_bytes_completed_total "$snapshot")" >>"$output"
}

active_interval_ms() {
  local sample=$1 column=$2 base=$3 final=$4
  awk -F, -v column="$column" -v base="$base" -v final="$final" '
    NR == 1 { next }
    {
      timestamp = $1; value = $column
      if (!started && value > base) {
        start = have_previous ? previous_timestamp : timestamp
        started = 1
      }
      if (started && value >= final) { print timestamp - start; exit }
      previous_timestamp = timestamp
      have_previous = 1
    }
    END { if (!started || value < final) print 0 }
  ' "$sample"
}

BENCH_SAMPLER=
BENCH_STOP_FILE=
BENCH_PREFIX=
BENCH_RESULT=

cleanup_benchmark() {
  local exit_status=$? cleanup_failed=0
  trap - EXIT INT TERM
  if [[ -n ${BENCH_STOP_FILE:-} ]]; then
    touch "$BENCH_STOP_FILE" 2>/dev/null || true
  fi
  if [[ -n ${BENCH_SAMPLER:-} ]]; then
    wait "$BENCH_SAMPLER" 2>/dev/null || true
  fi
  cleanup_benchmark_files || cleanup_failed=1
  [[ -z ${BENCH_STOP_FILE:-} ]] || rm -f "$BENCH_STOP_FILE"
  if [[ -n ${BENCH_RESULT:-} ]]; then
    printf 'harness_exit_status=%s\n' "$exit_status" >>"$BENCH_RESULT"
    printf 'cleanup_failed=%s\n' "$cleanup_failed" >>"$BENCH_RESULT"
  fi
  (( exit_status != 0 )) && exit "$exit_status"
  (( cleanup_failed == 0 )) || exit 1
  exit "$exit_status"
}

cleanup_benchmark_files() {
  [[ -n ${BENCH_PREFIX:-} ]] || return 0
  local failed=0
  sudo rm -f "$MOUNTPOINT/$BENCH_PREFIX".* 2>/dev/null || failed=1
  sudo sync -f "$MOUNTPOINT" || failed=1
  if [[ $(systemctl is-active "$SERVICE" 2>/dev/null || true) == active ]]; then
    wait_drain "$DRAIN_TIMEOUT" >/dev/null || failed=1
  fi
  if find "$MOUNTPOINT" -maxdepth 1 -name "$BENCH_PREFIX.*" -print -quit | grep -q .; then
    failed=1
  fi
  (( failed == 0 ))
}

benchmark() {
  require_vm100
  status >/dev/null
  wait_drain "$DRAIN_TIMEOUT" >/dev/null
  local total_mib=${ZEROFS_BENCH_TOTAL_MIB:-1024}
  local jobs=${ZEROFS_BENCH_JOBS:-4}
  (( total_mib > 0 && jobs > 0 && total_mib % jobs == 0 )) || die "total MiB must be positive and divisible by jobs"
  local per_job_mib=$((total_mib / jobs))
  local run timestamp result sample stop_file write_fio_output buffered_read_fio_output direct_read_fio_output status_output drain_output prefix
  timestamp=$(date -u +%Y%m%dT%H%M%SZ)
  run="${timestamp}-$$"
  sudo install -d -m 0755 -o "$(id -un)" -g "$(id -gn)" "$RESULT_DIR"
  result="$RESULT_DIR/storage-$run.txt"
  sample="$RESULT_DIR/storage-$run-metrics.csv"
  stop_file="$TMP_DIR/zerofs-metrics-$run.stop"
  write_fio_output="$RESULT_DIR/storage-$run-write-fio.txt"
  buffered_read_fio_output="$RESULT_DIR/storage-$run-buffered-warm-read-fio.txt"
  direct_read_fio_output="$RESULT_DIR/storage-$run-direct-read-fio.txt"
  status_output="$RESULT_DIR/storage-$run-status.txt"
  drain_output="$RESULT_DIR/storage-$run-drain.txt"
  prefix=".zerofs-bench-$run"
  sudo rm -f "$MOUNTPOINT/$prefix".* "$stop_file"
  status >"$status_output"
  BENCH_STOP_FILE=$stop_file
  BENCH_PREFIX=$prefix
  BENCH_RESULT=$result
  trap cleanup_benchmark EXIT
  trap 'exit 130' INT
  trap 'exit 143' TERM
  local snapshot local_bytes0 local_bytes1 remote_bytes0 remote_bytes1 t0 t1 t2 t3 t4 t5 t6 t7
  snapshot=$(metrics_snapshot)
  local_bytes0=$(metric_from zerofs_writeback_local_bytes_completed_total "$snapshot")
  remote_bytes0=$(metric_from zerofs_writeback_remote_bytes_completed_total "$snapshot")
  printf 'timestamp_ms,accepted,local,remote,dirty_ram,dirty_ssd,local_bytes,remote_bytes\n' >"$sample"
  append_metric_sample "$sample" "$(date +%s%3N)" "$snapshot"
  sample_metrics "$sample" "$stop_file" &
  local sampler=$!
  BENCH_SAMPLER=$sampler
  t0=$(date +%s%3N)
  sudo fio --name=zerofs_user_write --directory="$MOUNTPOINT" \
    "--filename_format=$prefix.\$jobnum" --rw=write --bs=1M \
    "--size=${per_job_mib}M" "--numjobs=$jobs" --group_reporting \
    --fallocate=none --refill_buffers=1 --scramble_buffers=1 \
    --buffer_compress_percentage=0 --output="$write_fio_output"
  t1=$(date +%s%3N)
  sudo sync -f "$MOUNTPOINT"
  t2=$(date +%s%3N)
  snapshot=$(metrics_snapshot)
  local_bytes1=$(metric_from zerofs_writeback_local_bytes_completed_total "$snapshot")
  wait_drain "$DRAIN_TIMEOUT" >"$drain_output"
  t3=$(date +%s%3N)
  snapshot=$(metrics_snapshot)
  remote_bytes1=$(metric_from zerofs_writeback_remote_bytes_completed_total "$snapshot")
  append_metric_sample "$sample" "$(date +%s%3N)" "$snapshot"
  touch "$stop_file"
  BENCH_SAMPLER=
  wait "$sampler"
  t4=$(date +%s%3N)
  sudo fio --name=zerofs_buffered_warm_read --directory="$MOUNTPOINT" \
    "--filename_format=$prefix.\$jobnum" --rw=read --bs=1M \
    "--size=${per_job_mib}M" "--numjobs=$jobs" --group_reporting \
    --direct=0 --invalidate=0 --output="$buffered_read_fio_output"
  t5=$(date +%s%3N)
  t6=$(date +%s%3N)
  sudo fio --name=zerofs_direct_read --directory="$MOUNTPOINT" \
    "--filename_format=$prefix.\$jobnum" --rw=read --bs=1M \
    "--size=${per_job_mib}M" "--numjobs=$jobs" --group_reporting \
    --direct=1 --output="$direct_read_fio_output"
  t7=$(date +%s%3N)
  local user_ms=$((t1 - t0)) local_ms=$((t2 - t1)) end_ms=$((t3 - t0))
  local buffered_read_ms=$((t5 - t4)) direct_read_ms=$((t7 - t6))
  local logical_bytes=$((total_mib * 1024 * 1024))
  local local_durable_bytes=$((local_bytes1 - local_bytes0)) remote_bytes=$((remote_bytes1 - remote_bytes0))
  local user_mibps buffered_read_mibps direct_read_mibps local_durable_mibps local_active local_active_mibps remote_wall_mibps remote_active first_drained_epoch_ms first_drained_ms
  user_mibps=$(awk -v b="$logical_bytes" -v ms="$user_ms" 'BEGIN { printf "%.2f", b/1048576/(ms/1000) }')
  buffered_read_mibps=$(awk -v b="$logical_bytes" -v ms="$buffered_read_ms" 'BEGIN { printf "%.2f", b/1048576/(ms/1000) }')
  direct_read_mibps=$(awk -v b="$logical_bytes" -v ms="$direct_read_ms" 'BEGIN { printf "%.2f", b/1048576/(ms/1000) }')
  local_durable_mibps=$(awk -v b="$local_durable_bytes" -v ms="$((t2 - t0))" 'BEGIN { if (ms>0) printf "%.2f", b/1048576/(ms/1000); else print "0.00" }')
  local_active=$(active_interval_ms "$sample" 7 "$local_bytes0" "$local_bytes1")
  local_active_mibps=$(awk -v b="$local_durable_bytes" -v ms="$local_active" 'BEGIN { if (ms>0) printf "%.2f", b/1048576/(ms/1000); else print "0.00" }')
  remote_wall_mibps=$(awk -v b="$remote_bytes" -v ms="$end_ms" 'BEGIN { printf "%.2f", b/1048576/(ms/1000) }')
  first_drained_epoch_ms=$(awk '{ for (i=1; i<=NF; i++) if ($i ~ /^first_drained_epoch_ms=/) { sub(/^[^=]*=/, "", $i); print $i; exit } }' "$drain_output")
  first_drained_ms=$((first_drained_epoch_ms - t0))
  remote_active=$(active_interval_ms "$sample" 8 "$remote_bytes0" "$remote_bytes1")
  local remote_active_mibps
  remote_active_mibps=$(awk -v b="$remote_bytes" -v ms="$remote_active" 'BEGIN { if (ms>0) printf "%.2f", b/1048576/(ms/1000); else print "0.00" }')
  {
    printf 'checkout_commit=%s\n' "$(git -C "$ROOT" rev-parse HEAD)"
    printf 'deployed_commit=%s\n' "$(awk -F= '$1 == "deployed_commit" { print $2; exit }' "$status_output")"
    printf 'running_binary_sha256=%s\n' "$(awk -F= '$1 == "running_binary_sha256" { print $2; exit }' "$status_output")"
    printf 'logical_bytes=%s remote_bytes=%s\n' "$logical_bytes" "$remote_bytes"
    printf 'user_experienced_ms=%s user_experienced_MiBps=%s durability=volatile_page_cache_and_memory_ack\n' "$user_ms" "$user_mibps"
    printf 'buffered_cached_candidate_read_ms=%s buffered_cached_candidate_read_MiBps=%s cache=guest_page_cache_candidate_not_guaranteed_prewarmed dataset=same_files_after_remote_drain_no_global_cache_drop\n' \
      "$buffered_read_ms" "$buffered_read_mibps"
    printf 'direct_read_ms=%s direct_read_MiBps=%s cache=guest_page_cache_bypass_zerofs_cache_eligible dataset=same_files_after_remote_drain\n' \
      "$direct_read_ms" "$direct_read_mibps"
    printf 'local_durable_bytes=%s local_sync_wait_ms=%s local_durable_end_to_end_ms=%s foreground_to_local_durable_MiBps=%s durability=zerofs_ssd_journal\n' \
      "$local_durable_bytes" "$local_ms" "$((t2 - t0))" "$local_durable_mibps"
    printf 'local_active_ms=%s local_active_MiBps=%s interval=sample_before_first_increment_through_final_count\n' \
      "$local_active" "$local_active_mibps"
    printf 'remote_first_drained_end_to_end_ms=%s remote_stable_end_to_end_ms=%s remote_wall_MiBps=%s remote_active_ms=%s remote_active_MiBps=%s durability=storage_box_sftp_ack\n' \
      "$first_drained_ms" "$end_ms" "$remote_wall_mibps" "$remote_active" "$remote_active_mibps"
    grep -E 'WRITE:|write: IOPS' "$write_fio_output" | tail -n 3
    grep -E 'READ:|read: IOPS' "$buffered_read_fio_output" | tail -n 3
    grep -E 'READ:|read: IOPS' "$direct_read_fio_output" | tail -n 3
    printf 'status_receipt=%s metric_samples=%s write_fio_output=%s buffered_warm_read_fio_output=%s direct_read_fio_output=%s drain_receipt=%s\n' \
      "$status_output" "$sample" "$write_fio_output" "$buffered_read_fio_output" "$direct_read_fio_output" "$drain_output"
  } | tee "$result"
  cleanup_benchmark_files
  rm -f "$stop_file"
  BENCH_STOP_FILE=
  BENCH_PREFIX=
  BENCH_RESULT=
  trap - EXIT INT TERM
  printf 'result=%s\n' "$result"
}

raw_sftp() {
  require_vm100
  [[ $RAW_SFTP_JOBS =~ ^[1-9][0-9]*$ ]] || die "ZEROFS_RAW_SFTP_JOBS must be positive"
  wait_drain "$DRAIN_TIMEOUT" >/dev/null
  local url authority user hostport host port key known timestamp remote localdir result
  local last_job=$((RAW_SFTP_JOBS - 1)) total_mib=$((RAW_SFTP_JOBS * 128)) total_bytes=$((RAW_SFTP_JOBS * 128 * 1024 * 1024))
  url=$(sudo awk -F'"' '/^url = / { print $2; exit }' "$CONFIG")
  authority=${url#sftp://}; authority=${authority%%/*}
  user=${authority%@*}; hostport=${authority#*@}; host=${hostport%:*}; port=${hostport##*:}
  key=$(sudo awk -F'"' '/^identity_file = / { print $2; exit }' "$CONFIG")
  known=$(sudo awk -F'"' '/^known_hosts = / { print $2; exit }' "$CONFIG")
  timestamp=$(date -u +%Y%m%dT%H%M%SZ)
  remote="zerofs-raw-control-$timestamp-$$"
  localdir="/var/tmp/$remote"
  sudo install -d -m 0755 -o "$(id -un)" -g "$(id -gn)" "$RESULT_DIR"
  result="$RESULT_DIR/raw-sftp-$timestamp-$$.txt"
  sudo install -d -m 0755 -o "$(id -un)" -g "$(id -gn)" "$localdir"
  for i in $(seq 0 "$last_job"); do sudo fallocate -l 128M "$localdir/file-$i.bin"; done
  local -a sftp_cmd=(sudo sftp -q -B 261120 -R 64 -P "$port" -i "$key" -o UserKnownHostsFile="$known" -o StrictHostKeyChecking=yes -o BatchMode=yes -o Compression=no "$user@$host")
  teardown
  local restored=0 restore_failed=0 remote_created=0 cleaned=0
  RAW_SFTP_PIDS=()
  cleanup_raw() {
    local exit_status=$?
    trap - EXIT INT TERM
    stop_and_reap_raw_jobs
    if (( remote_created && ! cleaned )); then
      { for i in $(seq 0 "$last_job"); do printf 'rm %s/file-%s.bin\n' "$remote" "$i"; done; printf 'rmdir %s\n' "$remote"; } | "${sftp_cmd[@]}" >/dev/null 2>&1 || true
    fi
    sudo rm -rf "$localdir"
    restore
    (( restore_failed == 0 )) || exit_status=1
    exit "$exit_status"
  }
  restore() {
    if (( ! restored )); then
      ZEROFS_SKIP_BUILD=1 start_stack || restore_failed=1
      restored=1
    fi
  }
  trap cleanup_raw EXIT
  trap 'exit 130' INT
  trap 'exit 143' TERM
  printf 'mkdir %s\n' "$remote" | "${sftp_cmd[@]}"
  remote_created=1
  local t0 t1 t2
  t0=$(date +%s%3N)
  for i in $(seq 0 "$last_job"); do
    (trap - EXIT INT TERM; printf 'put %s %s/file-%s.bin\n' "$localdir/file-$i.bin" "$remote" "$i" | "${sftp_cmd[@]}" >"$RESULT_DIR/raw-sftp-$timestamp-$$-upload-$i.log" 2>&1) &
    RAW_SFTP_PIDS+=("$!")
  done
  wait_all_raw_jobs || die "one or more raw SFTP upload workers failed"
  t1=$(date +%s%3N)
  for i in $(seq 0 "$last_job"); do
    (trap - EXIT INT TERM; printf 'get %s/file-%s.bin /dev/null\n' "$remote" "$i" | "${sftp_cmd[@]}" >"$RESULT_DIR/raw-sftp-$timestamp-$$-download-$i.log" 2>&1) &
    RAW_SFTP_PIDS+=("$!")
  done
  wait_all_raw_jobs || die "one or more raw SFTP download workers failed"
  t2=$(date +%s%3N)
  { for i in $(seq 0 "$last_job"); do printf 'rm %s/file-%s.bin\n' "$remote" "$i"; done; printf 'rmdir %s\n' "$remote"; } | "${sftp_cmd[@]}"
  cleaned=1
  sudo rm -rf "$localdir"
  restore
  trap - EXIT INT TERM
  printf 'raw_sftp_jobs=%s raw_sftp_bytes=%s upload_ms=%s upload_MiBps=%s download_ms=%s download_MiBps=%s\n' \
    "$RAW_SFTP_JOBS" "$total_bytes" \
    "$((t1-t0))" "$(awk -v mib="$total_mib" -v ms="$((t1-t0))" 'BEGIN { printf "%.2f", mib/(ms/1000) }')" \
    "$((t2-t1))" "$(awk -v mib="$total_mib" -v ms="$((t2-t1))" 'BEGIN { printf "%.2f", mib/(ms/1000) }')" | tee "$result"
  printf 'result=%s\n' "$result"
  (( restore_failed == 0 )) || die "raw SFTP control completed, but ZeroFS required a recovery restart"
}

RAW_SFTP_PIDS=()

wait_all_raw_jobs() {
  local pid failed=0
  [[ ${RAW_SFTP_PIDS+x} ]] || return 0
  for pid in "${RAW_SFTP_PIDS[@]}"; do
    wait "$pid" || failed=1
  done
  RAW_SFTP_PIDS=()
  (( failed == 0 ))
}

stop_and_reap_raw_jobs() {
  local pid
  [[ ${RAW_SFTP_PIDS+x} ]] || return 0
  for pid in "${RAW_SFTP_PIDS[@]}"; do
    kill "$pid" 2>/dev/null || true
  done
  for pid in "${RAW_SFTP_PIDS[@]}"; do
    wait "$pid" 2>/dev/null || true
  done
  RAW_SFTP_PIDS=()
}

clone_pinned() {
  local repo=$1 commit=$2 destination=$3
  git init -q "$destination"
  git -C "$destination" remote add origin "$repo"
  git -C "$destination" fetch -q --depth 1 origin "$commit"
  git -C "$destination" checkout -q --detach FETCH_HEAD
  [[ $(git -C "$destination" rev-parse HEAD) == "$commit" ]] || die "pinned checkout mismatch for $repo"
}

workloads() {
  require_vm100
  command -v git >/dev/null || die "git is required for workload benchmarks"
  command -v npm >/dev/null || die "npm is required for workload benchmarks"
  command -v xargs >/dev/null || die "xargs is required for the parallel deletion benchmark"
  [[ -n $CARGO_CMD && -x $CARGO_CMD ]] || die "cargo was not found; set ZEROFS_PILOT_CARGO"
  (( DELETE_JOBS > 0 )) || die "ZEROFS_DELETE_JOBS must be positive"
  status >/dev/null
  wait_drain "$DRAIN_TIMEOUT" >/dev/null

  local timestamp run workroot result npm_log cargo_log
  timestamp=$(date -u +%Y%m%dT%H%M%SZ)
  run="${timestamp}-$$"
  workroot="$MOUNTPOINT/.zerofs-workloads-$run"
  sudo install -d -m 0755 -o "$(id -un)" -g "$(id -gn)" "$RESULT_DIR"
  result="$RESULT_DIR/workloads-$run.txt"
  npm_log="$RESULT_DIR/workloads-$run-npm.log"
  cargo_log="$RESULT_DIR/workloads-$run-cargo.log"
  mkdir -p "$workroot"

  WORKLOAD_ROOT=$workroot
  WORKLOAD_CLEANED=0
  cleanup_workloads() {
    local exit_status=$? cleanup_start
    trap - EXIT
    if (( ! ${WORKLOAD_CLEANED:-1} )); then
      cleanup_start=$(date +%s%3N)
      sudo rm -rf -- "$WORKLOAD_ROOT"
      log "cleaned failed workload tree in $(($(date +%s%3N) - cleanup_start)) ms"
      WORKLOAD_CLEANED=1
      sudo sync -f "$MOUNTPOINT" || exit_status=1
      wait_drain "$DRAIN_TIMEOUT" >/dev/null || exit_status=1
      [[ ! -e $WORKLOAD_ROOT ]] || exit_status=1
    fi
    exit "$exit_status"
  }
  trap cleanup_workloads EXIT

  local clone_start npm_clone_ms npm_cold_start npm_cold_ms npm_cold_sync_start npm_cold_sync_ms npm_cold_remote_start npm_cold_remote_ms
  local npm_remove_start npm_remove_ms npm_remove_sync_start npm_remove_sync_ms npm_remove_remote_start npm_remove_remote_ms
  local npm_warm_start npm_warm_ms npm_warm_sync_start npm_warm_sync_ms npm_warm_remote_start npm_warm_remote_ms
  local npm_parallel_remove_start npm_parallel_remove_ms npm_parallel_remove_sync_start npm_parallel_remove_sync_ms npm_parallel_remove_remote_start npm_parallel_remove_remote_ms
  clone_start=$(date +%s%3N)
  clone_pinned "$NPM_WORKLOAD_REPO" "$NPM_WORKLOAD_COMMIT" "$workroot/npm-cli"
  npm_clone_ms=$(($(date +%s%3N) - clone_start))
  sudo sync -f "$MOUNTPOINT"
  wait_drain "$DRAIN_TIMEOUT" >/dev/null

  npm_cold_start=$(date +%s%3N)
  if ! (cd "$workroot/npm-cli" && npm ci --ignore-scripts --no-audit --no-fund) >"$npm_log" 2>&1; then
    tail -100 "$npm_log" >&2
    die "npm cold install failed"
  fi
  npm_cold_ms=$(($(date +%s%3N) - npm_cold_start))
  npm_cold_sync_start=$(date +%s%3N)
  sudo sync -f "$MOUNTPOINT"
  npm_cold_sync_ms=$(($(date +%s%3N) - npm_cold_sync_start))
  npm_cold_remote_start=$(date +%s%3N)
  wait_drain "$DRAIN_TIMEOUT" >/dev/null
  npm_cold_remote_ms=$(($(date +%s%3N) - npm_cold_remote_start))

  npm_remove_start=$(date +%s%3N)
  rm -rf -- "$workroot/npm-cli/node_modules"
  npm_remove_ms=$(($(date +%s%3N) - npm_remove_start))
  npm_remove_sync_start=$(date +%s%3N)
  sudo sync -f "$MOUNTPOINT"
  npm_remove_sync_ms=$(($(date +%s%3N) - npm_remove_sync_start))
  npm_remove_remote_start=$(date +%s%3N)
  wait_drain "$DRAIN_TIMEOUT" >/dev/null
  npm_remove_remote_ms=$(($(date +%s%3N) - npm_remove_remote_start))

  npm_warm_start=$(date +%s%3N)
  if ! (cd "$workroot/npm-cli" && npm ci --ignore-scripts --no-audit --no-fund) >>"$npm_log" 2>&1; then
    tail -100 "$npm_log" >&2
    die "npm warm install failed"
  fi
  npm_warm_ms=$(($(date +%s%3N) - npm_warm_start))
  npm_warm_sync_start=$(date +%s%3N)
  sudo sync -f "$MOUNTPOINT"
  npm_warm_sync_ms=$(($(date +%s%3N) - npm_warm_sync_start))
  npm_warm_remote_start=$(date +%s%3N)
  wait_drain "$DRAIN_TIMEOUT" >/dev/null
  npm_warm_remote_ms=$(($(date +%s%3N) - npm_warm_remote_start))

  npm_parallel_remove_start=$(date +%s%3N)
  find "$workroot/npm-cli/node_modules" -mindepth 1 -maxdepth 1 -print0 \
    | xargs -0 -r -P "$DELETE_JOBS" rm -rf --
  rmdir "$workroot/npm-cli/node_modules"
  npm_parallel_remove_ms=$(($(date +%s%3N) - npm_parallel_remove_start))
  npm_parallel_remove_sync_start=$(date +%s%3N)
  sudo sync -f "$MOUNTPOINT"
  npm_parallel_remove_sync_ms=$(($(date +%s%3N) - npm_parallel_remove_sync_start))
  npm_parallel_remove_remote_start=$(date +%s%3N)
  wait_drain "$DRAIN_TIMEOUT" >/dev/null
  npm_parallel_remove_remote_ms=$(($(date +%s%3N) - npm_parallel_remove_remote_start))

  local cargo_clone_start cargo_clone_ms cargo_cold_start cargo_cold_ms cargo_cold_sync_start cargo_cold_sync_ms cargo_cold_remote_start cargo_cold_remote_ms
  local cargo_noop_start cargo_noop_ms cargo_incremental_start cargo_incremental_ms cargo_incremental_sync_start cargo_incremental_sync_ms cargo_incremental_remote_start cargo_incremental_remote_ms
  cargo_clone_start=$(date +%s%3N)
  clone_pinned "$RUST_WORKLOAD_REPO" "$RUST_WORKLOAD_COMMIT" "$workroot/ripgrep"
  cargo_clone_ms=$(($(date +%s%3N) - cargo_clone_start))
  sudo sync -f "$MOUNTPOINT"
  wait_drain "$DRAIN_TIMEOUT" >/dev/null

  cargo_cold_start=$(date +%s%3N)
  if ! (cd "$workroot/ripgrep" && "$CARGO_CMD" build --locked) >"$cargo_log" 2>&1; then
    tail -100 "$cargo_log" >&2
    die "Cargo cold build failed"
  fi
  cargo_cold_ms=$(($(date +%s%3N) - cargo_cold_start))
  cargo_cold_sync_start=$(date +%s%3N)
  sudo sync -f "$MOUNTPOINT"
  cargo_cold_sync_ms=$(($(date +%s%3N) - cargo_cold_sync_start))
  cargo_cold_remote_start=$(date +%s%3N)
  wait_drain "$DRAIN_TIMEOUT" >/dev/null
  cargo_cold_remote_ms=$(($(date +%s%3N) - cargo_cold_remote_start))

  cargo_noop_start=$(date +%s%3N)
  (cd "$workroot/ripgrep" && "$CARGO_CMD" build --locked) >>"$cargo_log" 2>&1
  cargo_noop_ms=$(($(date +%s%3N) - cargo_noop_start))

  touch "$workroot/ripgrep/crates/core/main.rs"
  cargo_incremental_start=$(date +%s%3N)
  if ! (cd "$workroot/ripgrep" && "$CARGO_CMD" build --locked) >>"$cargo_log" 2>&1; then
    tail -100 "$cargo_log" >&2
    die "Cargo incremental rebuild failed"
  fi
  cargo_incremental_ms=$(($(date +%s%3N) - cargo_incremental_start))
  cargo_incremental_sync_start=$(date +%s%3N)
  sudo sync -f "$MOUNTPOINT"
  cargo_incremental_sync_ms=$(($(date +%s%3N) - cargo_incremental_sync_start))
  cargo_incremental_remote_start=$(date +%s%3N)
  wait_drain "$DRAIN_TIMEOUT" >/dev/null
  cargo_incremental_remote_ms=$(($(date +%s%3N) - cargo_incremental_remote_start))

  local cleanup_start cleanup_ms
  cleanup_start=$(date +%s%3N)
  sudo rm -rf -- "$workroot"
  cleanup_ms=$(($(date +%s%3N) - cleanup_start))
  WORKLOAD_CLEANED=1
  sudo sync -f "$MOUNTPOINT"
  wait_drain "$DRAIN_TIMEOUT" >/dev/null
  [[ ! -e $workroot ]] || die "disposable workload tree remains after cleanup"
  trap - EXIT

  {
    printf 'commit=%s\n' "$(git -C "$ROOT" rev-parse HEAD)"
    printf 'mountpoint=%s\n' "$MOUNTPOINT"
    printf 'npm_repo=%s npm_commit=%s clone_ms=%s\n' "$NPM_WORKLOAD_REPO" "$NPM_WORKLOAD_COMMIT" "$npm_clone_ms"
    printf 'npm_cold_install_ms=%s npm_cold_local_sync_ms=%s npm_cold_remote_tail_ms=%s npm_cold_end_to_end_ms=%s\n' \
      "$npm_cold_ms" "$npm_cold_sync_ms" "$npm_cold_remote_ms" "$((npm_cold_ms + npm_cold_sync_ms + npm_cold_remote_ms))"
    printf 'npm_remove_node_modules_ms=%s npm_remove_local_sync_ms=%s npm_remove_remote_tail_ms=%s npm_remove_end_to_end_ms=%s\n' \
      "$npm_remove_ms" "$npm_remove_sync_ms" "$npm_remove_remote_ms" "$((npm_remove_ms + npm_remove_sync_ms + npm_remove_remote_ms))"
    printf 'npm_warm_install_ms=%s npm_warm_local_sync_ms=%s npm_warm_remote_tail_ms=%s npm_warm_end_to_end_ms=%s\n' \
      "$npm_warm_ms" "$npm_warm_sync_ms" "$npm_warm_remote_ms" "$((npm_warm_ms + npm_warm_sync_ms + npm_warm_remote_ms))"
    printf 'npm_parallel_remove_jobs=%s npm_parallel_remove_ms=%s npm_parallel_remove_local_sync_ms=%s npm_parallel_remove_remote_tail_ms=%s npm_parallel_remove_end_to_end_ms=%s\n' \
      "$DELETE_JOBS" "$npm_parallel_remove_ms" "$npm_parallel_remove_sync_ms" "$npm_parallel_remove_remote_ms" \
      "$((npm_parallel_remove_ms + npm_parallel_remove_sync_ms + npm_parallel_remove_remote_ms))"
    printf 'cargo_repo=%s cargo_commit=%s clone_ms=%s\n' "$RUST_WORKLOAD_REPO" "$RUST_WORKLOAD_COMMIT" "$cargo_clone_ms"
    printf 'cargo_cold_build_ms=%s cargo_cold_local_sync_ms=%s cargo_cold_remote_tail_ms=%s cargo_cold_end_to_end_ms=%s\n' \
      "$cargo_cold_ms" "$cargo_cold_sync_ms" "$cargo_cold_remote_ms" "$((cargo_cold_ms + cargo_cold_sync_ms + cargo_cold_remote_ms))"
    printf 'cargo_noop_rebuild_ms=%s\n' "$cargo_noop_ms"
    printf 'cargo_incremental_rebuild_ms=%s cargo_incremental_local_sync_ms=%s cargo_incremental_remote_tail_ms=%s cargo_incremental_end_to_end_ms=%s\n' \
      "$cargo_incremental_ms" "$cargo_incremental_sync_ms" "$cargo_incremental_remote_ms" "$((cargo_incremental_ms + cargo_incremental_sync_ms + cargo_incremental_remote_ms))"
    printf 'workload_cleanup_ms=%s cleanup_verified=1\n' "$cleanup_ms"
    printf 'npm_log=%s cargo_log=%s\n' "$npm_log" "$cargo_log"
  } | tee "$result"
  printf 'result=%s\n' "$result"
}

iterate() {
  setup
  benchmark
  workloads
  raw_sftp
}

usage() {
  cat <<'EOF'
Usage: scripts/vm100-pilot.sh <setup|teardown|restart|status|drain|benchmark|workloads|raw-sftp|iterate|all>

Environment:
  ZEROFS_SKIP_BUILD=1       Start without pulling/building/installing.
  ZEROFS_BENCH_TOTAL_MIB=N  Logical benchmark size (default 1024).
  ZEROFS_BENCH_JOBS=N       Concurrent fio jobs (default 4).
  ZEROFS_PILOT_DRAIN_TIMEOUT=N
                             Maximum writeback drain wait in seconds (default 600).
  ZEROFS_PILOT_STOP_TIMEOUT=N
                             Maximum per-unit stop wait in seconds (default 60).
  ZEROFS_PILOT_EXPECT_ACK_MODE=MODE
                             Required configured durability mode (default memory).
  ZEROFS_NPM_WORKLOAD_*     Override the pinned npm repository and commit.
  ZEROFS_RUST_WORKLOAD_*    Override the pinned Cargo repository and commit.
  ZEROFS_DELETE_JOBS=N      Parallel node_modules deletion workers (default 4).
  ZEROFS_RAW_SFTP_JOBS=N    Raw SFTP streams/files (default 7; set 8 for the account maximum).
EOF
}

case ${1:-} in
  setup) setup ;;
  teardown) teardown ;;
  restart) teardown; ZEROFS_SKIP_BUILD=1 start_stack ;;
  status) status ;;
  drain) drain ;;
  benchmark) benchmark ;;
  workloads) workloads ;;
  raw-sftp) raw_sftp ;;
  iterate) iterate ;;
  all) benchmark; workloads; raw_sftp ;;
  *) usage; exit 2 ;;
esac
