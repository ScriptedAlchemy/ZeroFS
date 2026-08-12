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
CARGO_CMD=${ZEROFS_PILOT_CARGO:-}
NPM_WORKLOAD_REPO=${ZEROFS_NPM_WORKLOAD_REPO:-https://github.com/expressjs/express.git}
NPM_WORKLOAD_COMMIT=${ZEROFS_NPM_WORKLOAD_COMMIT:-cd7d4397c398a3f3ecadeaf9ef6ac1377bd414c4}
RUST_WORKLOAD_REPO=${ZEROFS_RUST_WORKLOAD_REPO:-https://github.com/BurntSushi/ripgrep.git}
RUST_WORKLOAD_COMMIT=${ZEROFS_RUST_WORKLOAD_COMMIT:-af60c2de9d85e7f3d81c78601669468cf02dabab}

if [[ -z $CARGO_CMD ]]; then
  CARGO_CMD=$(command -v cargo 2>/dev/null || true)
fi
if [[ -z $CARGO_CMD && -x ${HOME:-}/.cargo/bin/cargo ]]; then
  CARGO_CMD=${HOME}/.cargo/bin/cargo
fi

log() { printf '[vm100-pilot] %s\n' "$*" >&2; }
die() { log "ERROR: $*"; exit 1; }

metric_from() {
  local name=$1 snapshot=$2
  awk -v n="$name" '$1 == n { print $2; exit }' <<<"$snapshot"
}

metrics_snapshot() { curl -fsS "$METRICS_URL"; }

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
  local timeout=${1:-600} stable=0 snapshot accepted local_seq remote dirty_ram dirty_ssd terminal
  for _ in $(seq 1 "$timeout"); do
    snapshot=$(metrics_snapshot)
    accepted=$(metric_from zerofs_writeback_accepted_sequence "$snapshot")
    local_seq=$(metric_from zerofs_writeback_local_sequence "$snapshot")
    remote=$(metric_from zerofs_writeback_remote_sequence "$snapshot")
    dirty_ram=$(metric_from zerofs_writeback_dirty_ram_bytes "$snapshot")
    dirty_ssd=$(metric_from zerofs_writeback_dirty_ssd_bytes "$snapshot")
    terminal=$(metric_from zerofs_writeback_terminal_error "$snapshot")
    if [[ $accepted == "$local_seq" && $accepted == "$remote" && $dirty_ram == 0 && $dirty_ssd == 0 && $terminal == 0 ]]; then
      stable=$((stable + 1))
    else
      stable=0
    fi
    if (( stable >= 4 )); then
      printf 'drained accepted=%s local=%s remote=%s dirty_ram=%s dirty_ssd=%s terminal=%s\n' \
        "$accepted" "$local_seq" "$remote" "$dirty_ram" "$dirty_ssd" "$terminal"
      return 0
    fi
    sleep 1
  done
  die "writeback did not drain within ${timeout}s"
}

status() {
  require_vm100
  local unit
  for unit in "$SERVICE" "$CLIENT_SERVICE" "$MOUNT_UNIT"; do
    [[ $(systemctl is-active "$unit" 2>/dev/null || true) == active ]] || die "$unit is not active"
    printf '%s=active\n' "$unit"
  done
  findmnt -no SOURCE,FSTYPE,TARGET "$MOUNTPOINT"
  local binary_sha integrity_sha metadata_count snapshot
  binary_sha=$(sha256sum "$BINARY" | awk '{print $1}')
  integrity_sha=$(sudo timeout 180 sha256sum "$INTEGRITY_FILE" | awk '{print $1}')
  [[ $integrity_sha == "$INTEGRITY_SHA256" ]] || die "integrity sentinel hash mismatch"
  metadata_count=$(sudo find "$METADATA_DIR" -type f -printf . | wc -c)
  [[ $metadata_count == "$METADATA_FILE_COUNT" ]] || die "metadata file count is $metadata_count, expected $METADATA_FILE_COUNT"
  snapshot=$(metrics_snapshot)
  printf 'binary_sha256=%s integrity_sha256=%s metadata_files=%s restarts=%s\n' \
    "$binary_sha" "$integrity_sha" "$metadata_count" \
    "$(systemctl show "$SERVICE" -p NRestarts --value)"
  for name in \
    zerofs_writeback_accepted_sequence zerofs_writeback_local_sequence \
    zerofs_writeback_remote_sequence zerofs_writeback_dirty_ram_bytes \
    zerofs_writeback_dirty_ssd_bytes zerofs_writeback_terminal_error; do
    printf '%s=%s\n' "$name" "$(metric_from "$name" "$snapshot")"
  done
}

teardown() {
  require_vm100
  log "stopping mount, NBD client, and ZeroFS daemon"
  sudo systemctl stop "$MOUNT_UNIT" || true
  sudo systemctl stop "$CLIENT_SERVICE" || true
  sudo systemctl stop "$SERVICE" || true
  findmnt -rn "$MOUNTPOINT" >/dev/null && die "$MOUNTPOINT remains mounted"
  log "teardown complete; canonical data/cache/journal were retained"
}

build_deploy() {
  require_vm100
  [[ -z $(git -C "$ROOT" status --porcelain) ]] || die "Git checkout is dirty"
  git -C "$ROOT" pull --ff-only
  [[ -z $(git -C "$ROOT" status --porcelain) ]] || die "Git checkout became dirty after pull"
  [[ -n $CARGO_CMD && -x $CARGO_CMD ]] || die "cargo was not found; set ZEROFS_PILOT_CARGO"
  log "building locked release at $(git -C "$ROOT" rev-parse --short HEAD)"
  (cd "$CRATE" && "$CARGO_CMD" build --release --locked)
  local built_sha
  built_sha=$(sha256sum "$CRATE/target/release/zerofs" | awk '{print $1}')
  teardown
  sudo install -m 0755 "$CRATE/target/release/zerofs" "$BINARY"
  [[ $(sha256sum "$BINARY" | awk '{print $1}') == "$built_sha" ]] || die "installed binary hash mismatch"
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
  printf 'timestamp_ms,accepted,local,remote,dirty_ram,dirty_ssd,remote_bytes\n' >"$output"
  while [[ ! -e $stop_file ]]; do
    local snapshot now
    snapshot=$(metrics_snapshot) || { sleep .25; continue; }
    now=$(date +%s%3N)
    printf '%s,%s,%s,%s,%s,%s,%s\n' \
      "$now" \
      "$(metric_from zerofs_writeback_accepted_sequence "$snapshot")" \
      "$(metric_from zerofs_writeback_local_sequence "$snapshot")" \
      "$(metric_from zerofs_writeback_remote_sequence "$snapshot")" \
      "$(metric_from zerofs_writeback_dirty_ram_bytes "$snapshot")" \
      "$(metric_from zerofs_writeback_dirty_ssd_bytes "$snapshot")" \
      "$(metric_from zerofs_writeback_remote_bytes_completed_total "$snapshot")" >>"$output"
    sleep .25
  done
}

benchmark() {
  require_vm100
  status >/dev/null
  wait_drain 600 >/dev/null
  local total_mib=${ZEROFS_BENCH_TOTAL_MIB:-1024}
  local jobs=${ZEROFS_BENCH_JOBS:-4}
  (( total_mib > 0 && jobs > 0 && total_mib % jobs == 0 )) || die "total MiB must be positive and divisible by jobs"
  local per_job_mib=$((total_mib / jobs))
  local run timestamp result sample stop_file fio_output prefix
  timestamp=$(date -u +%Y%m%dT%H%M%SZ)
  run="${timestamp}-$$"
  sudo install -d -m 0755 -o "$(id -un)" -g "$(id -gn)" "$RESULT_DIR"
  result="$RESULT_DIR/storage-$run.txt"
  sample="/tmp/zerofs-metrics-$run.csv"
  stop_file="/tmp/zerofs-metrics-$run.stop"
  fio_output="/tmp/zerofs-fio-$run.txt"
  prefix=".zerofs-bench-$run"
  sudo rm -f "$MOUNTPOINT/$prefix".* "$stop_file"
  sample_metrics "$sample" "$stop_file" &
  local sampler=$!
  local snapshot bytes0 bytes1 t0 t1 t2 t3
  snapshot=$(metrics_snapshot)
  bytes0=$(metric_from zerofs_writeback_remote_bytes_completed_total "$snapshot")
  t0=$(date +%s%3N)
  sudo fio --name=zerofs_user_write --directory="$MOUNTPOINT" \
    "--filename_format=$prefix.\$jobnum" --rw=write --bs=1M \
    "--size=${per_job_mib}M" "--numjobs=$jobs" --group_reporting \
    --fallocate=none --refill_buffers=1 --scramble_buffers=1 \
    --buffer_compress_percentage=0 --output="$fio_output"
  t1=$(date +%s%3N)
  sudo sync -f "$MOUNTPOINT"
  t2=$(date +%s%3N)
  wait_drain 600 >/dev/null
  t3=$(date +%s%3N)
  snapshot=$(metrics_snapshot)
  bytes1=$(metric_from zerofs_writeback_remote_bytes_completed_total "$snapshot")
  touch "$stop_file"
  wait "$sampler"
  local user_ms=$((t1 - t0)) local_ms=$((t2 - t1)) end_ms=$((t3 - t0))
  local logical_bytes=$((total_mib * 1024 * 1024)) remote_bytes=$((bytes1 - bytes0))
  local user_mibps local_mibps remote_wall_mibps remote_active
  user_mibps=$(awk -v b="$logical_bytes" -v ms="$user_ms" 'BEGIN { printf "%.2f", b/1048576/(ms/1000) }')
  local_mibps=$(awk -v b="$logical_bytes" -v ms="$local_ms" 'BEGIN { printf "%.2f", b/1048576/(ms/1000) }')
  remote_wall_mibps=$(awk -v b="$remote_bytes" -v ms="$end_ms" 'BEGIN { printf "%.2f", b/1048576/(ms/1000) }')
  remote_active=$(awk -F, 'NR==2 { base=$7; prev=$7 } NR>2 && $7>prev { if (!first) first=$1; last=$1; prev=$7 } END { if (first && last>first) print last-first; else print 0 }' "$sample")
  local remote_active_mibps
  remote_active_mibps=$(awk -v b="$remote_bytes" -v ms="$remote_active" 'BEGIN { if (ms>0) printf "%.2f", b/1048576/(ms/1000); else print "0.00" }')
  {
    printf 'commit=%s\n' "$(git -C "$ROOT" rev-parse HEAD)"
    printf 'logical_bytes=%s remote_bytes=%s\n' "$logical_bytes" "$remote_bytes"
    printf 'user_experienced_ms=%s user_experienced_MiBps=%s durability=volatile_page_cache_and_memory_ack\n' "$user_ms" "$user_mibps"
    printf 'local_flush_ms=%s local_flush_MiBps=%s durability=zerofs_ssd_journal\n' "$local_ms" "$local_mibps"
    printf 'remote_end_to_end_ms=%s remote_wall_MiBps=%s remote_active_ms=%s remote_active_MiBps=%s durability=storage_box_sftp_ack\n' \
      "$end_ms" "$remote_wall_mibps" "$remote_active" "$remote_active_mibps"
    grep -E 'WRITE:|write: IOPS' "$fio_output" | tail -n 3
    printf 'metric_samples=%s\n' "$sample"
  } | tee "$result"
  sudo rm -f "$MOUNTPOINT/$prefix".*
  sudo sync -f "$MOUNTPOINT"
  wait_drain 600 >/dev/null
  sudo rm -f "$stop_file" "$fio_output"
  printf 'result=%s\n' "$result"
}

raw_sftp() {
  require_vm100
  wait_drain 600 >/dev/null
  local url authority user hostport host port key known timestamp remote localdir
  url=$(sudo awk -F'"' '/^url = / { print $2; exit }' "$CONFIG")
  authority=${url#sftp://}; authority=${authority%%/*}
  user=${authority%@*}; hostport=${authority#*@}; host=${hostport%:*}; port=${hostport##*:}
  key=$(sudo awk -F'"' '/^identity_file = / { print $2; exit }' "$CONFIG")
  known=$(sudo awk -F'"' '/^known_hosts = / { print $2; exit }' "$CONFIG")
  timestamp=$(date -u +%Y%m%dT%H%M%SZ)
  remote="zerofs-raw-control-$timestamp-$$"
  localdir="/var/tmp/$remote"
  sudo install -d -m 0755 -o "$(id -un)" -g "$(id -gn)" "$localdir"
  for i in $(seq 0 7); do sudo fallocate -l 128M "$localdir/file-$i.bin"; done
  local -a sftp_cmd=(sudo sftp -q -B 261120 -R 64 -P "$port" -i "$key" -o UserKnownHostsFile="$known" -o StrictHostKeyChecking=yes -o BatchMode=yes -o Compression=no "$user@$host")
  teardown
  local restored=0 restore_failed=0 remote_created=0 cleaned=0
  cleanup_raw() {
    local exit_status=$?
    if (( remote_created && ! cleaned )); then
      { for i in $(seq 0 7); do printf 'rm %s/file-%s.bin\n' "$remote" "$i"; done; printf 'rmdir %s\n' "$remote"; } | "${sftp_cmd[@]}" >/dev/null 2>&1 || true
    fi
    sudo rm -rf "$localdir"
    restore
    (( restore_failed == 0 )) || exit_status=1
    return "$exit_status"
  }
  restore() {
    if (( ! restored )); then
      ZEROFS_SKIP_BUILD=1 start_stack || restore_failed=1
      restored=1
    fi
  }
  trap cleanup_raw EXIT
  printf 'mkdir %s\n' "$remote" | "${sftp_cmd[@]}"
  remote_created=1
  local t0 t1 t2 pids=()
  t0=$(date +%s%3N)
  for i in $(seq 0 7); do
    (trap - EXIT; printf 'put %s %s/file-%s.bin\n' "$localdir/file-$i.bin" "$remote" "$i" | "${sftp_cmd[@]}" >"$localdir/upload-$i.log" 2>&1) &
    pids+=("$!")
  done
  for pid in "${pids[@]}"; do wait "$pid"; done
  t1=$(date +%s%3N)
  pids=()
  for i in $(seq 0 7); do
    (trap - EXIT; printf 'get %s/file-%s.bin /dev/null\n' "$remote" "$i" | "${sftp_cmd[@]}" >"$localdir/download-$i.log" 2>&1) &
    pids+=("$!")
  done
  for pid in "${pids[@]}"; do wait "$pid"; done
  t2=$(date +%s%3N)
  { for i in $(seq 0 7); do printf 'rm %s/file-%s.bin\n' "$remote" "$i"; done; printf 'rmdir %s\n' "$remote"; } | "${sftp_cmd[@]}"
  cleaned=1
  sudo rm -rf "$localdir"
  restore
  trap - EXIT
  printf 'raw_sftp_bytes=1073741824 upload_ms=%s upload_MiBps=%s download_ms=%s download_MiBps=%s\n' \
    "$((t1-t0))" "$(awk -v ms="$((t1-t0))" 'BEGIN { printf "%.2f", 1024/(ms/1000) }')" \
    "$((t2-t1))" "$(awk -v ms="$((t2-t1))" 'BEGIN { printf "%.2f", 1024/(ms/1000) }')"
  (( restore_failed == 0 )) || die "raw SFTP control completed, but ZeroFS required a recovery restart"
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
  [[ -n $CARGO_CMD && -x $CARGO_CMD ]] || die "cargo was not found; set ZEROFS_PILOT_CARGO"
  status >/dev/null
  wait_drain 600 >/dev/null

  local timestamp run workroot result npm_log cargo_log
  timestamp=$(date -u +%Y%m%dT%H%M%SZ)
  run="${timestamp}-$$"
  workroot="$MOUNTPOINT/.zerofs-workloads-$run"
  sudo install -d -m 0755 -o "$(id -un)" -g "$(id -gn)" "$RESULT_DIR"
  result="$RESULT_DIR/workloads-$run.txt"
  npm_log="$RESULT_DIR/workloads-$run-npm.log"
  cargo_log="$RESULT_DIR/workloads-$run-cargo.log"
  mkdir -p "$workroot"

  local cleaned=0 cleanup_ms=0
  cleanup_workloads() {
    local exit_status=$? cleanup_start
    if (( ! cleaned )); then
      cleanup_start=$(date +%s%3N)
      sudo rm -rf -- "$workroot"
      cleanup_ms=$(($(date +%s%3N) - cleanup_start))
      cleaned=1
      sudo sync -f "$MOUNTPOINT" || exit_status=1
      wait_drain 600 >/dev/null || exit_status=1
      [[ ! -e $workroot ]] || exit_status=1
    fi
    return "$exit_status"
  }
  trap cleanup_workloads EXIT

  local clone_start npm_clone_ms npm_cold_start npm_cold_ms npm_cold_sync_start npm_cold_sync_ms npm_cold_remote_start npm_cold_remote_ms
  local npm_remove_start npm_remove_ms npm_remove_sync_start npm_remove_sync_ms npm_remove_remote_start npm_remove_remote_ms
  local npm_warm_start npm_warm_ms npm_warm_sync_start npm_warm_sync_ms npm_warm_remote_start npm_warm_remote_ms
  clone_start=$(date +%s%3N)
  clone_pinned "$NPM_WORKLOAD_REPO" "$NPM_WORKLOAD_COMMIT" "$workroot/npm-express"
  npm_clone_ms=$(($(date +%s%3N) - clone_start))
  sudo sync -f "$MOUNTPOINT"
  wait_drain 600 >/dev/null

  npm_cold_start=$(date +%s%3N)
  if ! (cd "$workroot/npm-express" && npm ci --ignore-scripts --no-audit --no-fund) >"$npm_log" 2>&1; then
    tail -100 "$npm_log" >&2
    die "npm cold install failed"
  fi
  npm_cold_ms=$(($(date +%s%3N) - npm_cold_start))
  npm_cold_sync_start=$(date +%s%3N)
  sudo sync -f "$MOUNTPOINT"
  npm_cold_sync_ms=$(($(date +%s%3N) - npm_cold_sync_start))
  npm_cold_remote_start=$(date +%s%3N)
  wait_drain 600 >/dev/null
  npm_cold_remote_ms=$(($(date +%s%3N) - npm_cold_remote_start))

  npm_remove_start=$(date +%s%3N)
  rm -rf -- "$workroot/npm-express/node_modules"
  npm_remove_ms=$(($(date +%s%3N) - npm_remove_start))
  npm_remove_sync_start=$(date +%s%3N)
  sudo sync -f "$MOUNTPOINT"
  npm_remove_sync_ms=$(($(date +%s%3N) - npm_remove_sync_start))
  npm_remove_remote_start=$(date +%s%3N)
  wait_drain 600 >/dev/null
  npm_remove_remote_ms=$(($(date +%s%3N) - npm_remove_remote_start))

  npm_warm_start=$(date +%s%3N)
  if ! (cd "$workroot/npm-express" && npm ci --ignore-scripts --no-audit --no-fund) >>"$npm_log" 2>&1; then
    tail -100 "$npm_log" >&2
    die "npm warm install failed"
  fi
  npm_warm_ms=$(($(date +%s%3N) - npm_warm_start))
  npm_warm_sync_start=$(date +%s%3N)
  sudo sync -f "$MOUNTPOINT"
  npm_warm_sync_ms=$(($(date +%s%3N) - npm_warm_sync_start))
  npm_warm_remote_start=$(date +%s%3N)
  wait_drain 600 >/dev/null
  npm_warm_remote_ms=$(($(date +%s%3N) - npm_warm_remote_start))

  local cargo_clone_start cargo_clone_ms cargo_cold_start cargo_cold_ms cargo_cold_sync_start cargo_cold_sync_ms cargo_cold_remote_start cargo_cold_remote_ms
  local cargo_noop_start cargo_noop_ms cargo_incremental_start cargo_incremental_ms cargo_incremental_sync_start cargo_incremental_sync_ms cargo_incremental_remote_start cargo_incremental_remote_ms
  cargo_clone_start=$(date +%s%3N)
  clone_pinned "$RUST_WORKLOAD_REPO" "$RUST_WORKLOAD_COMMIT" "$workroot/ripgrep"
  cargo_clone_ms=$(($(date +%s%3N) - cargo_clone_start))
  sudo sync -f "$MOUNTPOINT"
  wait_drain 600 >/dev/null

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
  wait_drain 600 >/dev/null
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
  wait_drain 600 >/dev/null
  cargo_incremental_remote_ms=$(($(date +%s%3N) - cargo_incremental_remote_start))

  local cleanup_start
  cleanup_start=$(date +%s%3N)
  sudo rm -rf -- "$workroot"
  cleanup_ms=$(($(date +%s%3N) - cleanup_start))
  cleaned=1
  sudo sync -f "$MOUNTPOINT"
  wait_drain 600 >/dev/null
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
Usage: scripts/vm100-pilot.sh <setup|teardown|restart|status|benchmark|workloads|raw-sftp|iterate|all>

Environment:
  ZEROFS_SKIP_BUILD=1       Start without pulling/building/installing.
  ZEROFS_BENCH_TOTAL_MIB=N  Logical benchmark size (default 1024).
  ZEROFS_BENCH_JOBS=N       Concurrent fio jobs (default 4).
  ZEROFS_NPM_WORKLOAD_*     Override the pinned npm repository and commit.
  ZEROFS_RUST_WORKLOAD_*    Override the pinned Cargo repository and commit.
EOF
}

case ${1:-} in
  setup) setup ;;
  teardown) teardown ;;
  restart) teardown; ZEROFS_SKIP_BUILD=1 start_stack ;;
  status) status ;;
  benchmark) benchmark ;;
  workloads) workloads ;;
  raw-sftp) raw_sftp ;;
  iterate) iterate ;;
  all) benchmark; workloads; raw_sftp ;;
  *) usage; exit 2 ;;
esac
