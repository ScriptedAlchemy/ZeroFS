#!/bin/sh
set -eu
export PATH=/bin:/usr/bin:/sbin:/usr/sbin
exec </dev/console >/dev/console 2>&1
failed() {
    echo BROKER_VM_FAIL
    cat /app/state/driver.log /app/state/error /app/state/engine.log /run/broker.log 2>/dev/null | tail -80 || true
    poweroff -f
}
trap failed EXIT
mount -t proc proc /proc
mount -t sysfs sysfs /sys
mount -t devtmpfs devtmpfs /dev
mount -t tmpfs tmpfs /run
while IFS= read -r filename; do insmod "/modules/$filename"; done </modules/load-order
/usr/lib/systemd/systemd-udevd --daemon
udevadm trigger --action=add --subsystem-match=block
udevadm settle --timeout=20
mount -t cgroup2 none /sys/fs/cgroup
echo +memory >/sys/fs/cgroup/cgroup.subtree_control
mkdir /sys/fs/cgroup/app /run/claim
echo 8589934592 >/sys/fs/cgroup/app/memory.max
echo 0 >/sys/fs/cgroup/app/memory.swap.max
chmod 0755 /run/claim
chown 20001:20001 /app/state
chmod 0700 /app/state
chown 0:20001 /app/etc/*.toml
chmod 0640 /app/etc/*.toml
touch /host-only-canary
start_broker() {
    /bin/zerofs-raw-device physical serve-claims /run/claim/claim.sock 20001 20001 >>/run/broker.log 2>&1 &
    broker=$!
}
start_broker
while ! test -S /run/claim/claim.sock; do kill -0 "$broker"; sleep 0.1; done
for mode in file sequence disconnect missing; do /bin/fault "/run/claim/$mode" "$mode" & done
echo "BROKER_VM_HOST pidns=$(readlink /proc/self/ns/pid) mntns=$(readlink /proc/self/ns/mnt) socket=$(stat -c '%u:%g:%a' /run/claim/claim.sock)"
(echo 0 >/sys/fs/cgroup/app/cgroup.procs; exec unshare --pid --mount --net --cgroup --fork --kill-child --propagation private /isolate) >/app/state/driver.log 2>&1 &
client=$!
wait_marker() {
    while ! test -f "/app/state/$1"; do kill -0 "$client" || return 1; sleep 0.1; done
}
wait_marker held
kill -9 "$broker"
wait "$broker" 2>/dev/null || true
touch /app/state/disconnected
wait_marker reconnect
start_broker
# Wait for a new socket before releasing the isolated client.
while ! test -S /run/claim/claim.sock || test "$(stat -c %i /run/claim/claim.sock 2>/dev/null || true)" = "$(cat /app/state/old-inode)"; do kill -0 "$broker"; sleep 0.1; done
touch /app/state/restarted
wait "$client"
grep '^BROKER_VM_' /app/state/driver.log
echo "BROKER_VM_CGROUP peak=$(cat /sys/fs/cgroup/app/memory.peak) events=$(cat /sys/fs/cgroup/app/memory.events | sed ':a;N;$!ba;s/\n/,/g')"
echo BROKER_VM_PASS
trap - EXIT
poweroff -f
