#!/usr/bin/env bash
# xfstests stops each test's systemd scope. Mount clients must outlive that scope.
set -euo pipefail

dev=${1:?missing device}
mnt=${2:?missing mountpoint}
log=${ZEROFS_MOUNT_LOG:-/tmp/zerofs-mount.log}
unit="zerofs-xfstests-$(systemd-escape --path "$mnt").service"

# Finish the previous client's teardown before mounting on the same path again.
if ! systemctl stop "$unit" 2>/dev/null; then
    [[ $(systemctl show --property=LoadState --value "$unit") == not-found ]] || exit 1
fi
if mountpoint -q "$mnt"; then
    echo "mount.fuse.zerofs: $mnt is already mounted" >&2
    exit 1
fi

systemd-run --quiet --collect --unit="$unit" --service-type=exec \
    --property="StandardOutput=append:$log" \
    --property="StandardError=append:$log" \
    "${ZEROFS_BIN:-/usr/local/bin/zerofs}" mount --access all --writeback false "$dev" "$mnt"
for ((attempt = 0; attempt < 100; attempt++)); do
    systemctl is-active --quiet "$unit" || break
    mountpoint -q "$mnt" && exit 0
    sleep 0.1
done
systemctl stop "$unit"
echo "mount.fuse.zerofs: $mnt did not become a mountpoint" >&2
exit 1
