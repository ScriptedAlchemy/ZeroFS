#!/usr/bin/env bash
set -euo pipefail

readonly mode=${1:-}
readonly expected_source=${2:-}
readonly confirmation=${3:-}
readonly mountpoint=/mnt/zerofs-files
readonly target_uid=501
readonly target_gid=20
readonly receipt_prefix=ZEROFS_SHARED_NAMESPACE_V1
readonly receipt_dir=/var/lib/zerofs-deploy/ownership-receipts
readonly receipt_path="$receipt_dir/shared-501-20.receipt"

die() {
  echo "error: $*" >&2
  exit 1
}

case "$mode" in
  inventory | repair) ;;
  *) die 'mode must be inventory or repair' ;;
esac
case "$expected_source" in
  10.*:/ | 192.168.*:/ | 172.1[6-9].*:/ | 172.2[0-9].*:/ | 172.3[01].*:/) ;;
  *) die 'source must be an RFC1918 IPv4 NFS root export' ;;
esac
if [[ $mode == repair && $confirmation != 501:20 ]]; then
  die 'repair requires exact confirmation 501:20'
fi

record=$(findmnt -rn -M "$mountpoint" -o SOURCE,FSTYPE,OPTIONS 2>/dev/null || true)
[[ -n $record ]] || die 'managed NFS mount is unavailable'
read -r actual_source fstype options <<<"$record"
[[ $actual_source == "$expected_source" && $fstype == nfs && ",${options}," == *,rw,* ]] || \
  die 'managed mount must match the requested source and be read-write NFS'

scan() {
  local objects wrong_owner first_uid first_gid reason=$1
  objects=$(find "$mountpoint" -xdev -path "$mountpoint/.nbd" -prune -o -printf . | wc -c)
  wrong_owner=$(find "$mountpoint" -xdev -path "$mountpoint/.nbd" -prune -o \
    \( ! -uid "$target_uid" -o ! -gid "$target_gid" \) -printf . | wc -c)
  first_uid=$(find "$mountpoint" -xdev -path "$mountpoint/.nbd" -prune -o \
    \( ! -uid "$target_uid" -o ! -gid "$target_gid" \) -printf '%U' -quit)
  first_gid=$(find "$mountpoint" -xdev -path "$mountpoint/.nbd" -prune -o \
    \( ! -uid "$target_uid" -o ! -gid "$target_gid" \) -printf '%G' -quit)
  printf '%s verified=1 objects=%s wrong_owner=%s first_uid=%s first_gid=%s reason=%s\n' \
    "$receipt_prefix" "$objects" "$wrong_owner" "${first_uid:--1}" \
    "${first_gid:--1}" "$reason"
}

if [[ $mode == inventory ]]; then
  scan inventory
  exit 0
fi

# GNU find does not follow symlinks by default. --no-dereference changes a
# symlink's own ownership and never its target. -xdev forbids filesystem escape.
find "$mountpoint" -xdev -path "$mountpoint/.nbd" -prune -o \
  \( ! -uid "$target_uid" -o ! -gid "$target_gid" \) \
  -exec chown --no-dereference "$target_uid:$target_gid" -- {} +

receipt=$(scan repaired)
case "$receipt" in
  *' verified=1 '*' wrong_owner=0 '*) ;;
  *) die "ownership repair did not converge: $receipt" ;;
esac
install -d -m 0700 "$receipt_dir"
temporary=$(mktemp "$receipt_dir/.shared-501-20.XXXXXX")
trap 'rm -f -- "$temporary"' EXIT
printf '%s\n' "$receipt" >"$temporary"
chmod 0600 "$temporary"
sync -f "$temporary"
mv -f -- "$temporary" "$receipt_path"
python3 - "$receipt_dir" <<'PY'
import os
import sys

descriptor = os.open(sys.argv[1], os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
try:
    os.fsync(descriptor)
finally:
    os.close(descriptor)
PY
trap - EXIT
printf '%s\n' "$receipt"
printf 'durable_receipt=%s\n' "$receipt_path"
