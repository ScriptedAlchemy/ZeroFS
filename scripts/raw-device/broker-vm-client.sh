#!/bin/sh
set -eu
export PATH=/bin
export ZEROFS_RAW_CLAIM_SOCKET=/run/claims/claim.sock
cfg=/etc/zerofs.toml
target=unix:/state/9p.sock
test "$(id -u):$(id -g):$(id -G)" = "20001:20001:20001"
test ! -e /host-only-canary && test ! -e /proc/1/root/host-only-canary
test ! -e /dev/disk && test ! -e /sys/block && test ! -e /run/udev
test "$(cat /sys/fs/cgroup/memory.max)" = 8589934592
grep -Eq '^CapEff:[[:space:]]+0+$' /proc/self/status
grep -Eq '^NoNewPrivs:[[:space:]]+1$' /proc/self/status
echo "BROKER_VM_CLIENT uid=$(id -u) gid=$(id -g) groups=$(id -G) pidns=$(readlink /proc/self/ns/pid) mntns=$(readlink /proc/self/ns/mnt) cgroup=$(cat /proc/self/cgroup) host_root=absent devices=absent host_sysfs=absent capabilities=0"
refuse() {
    name=$1
    match=$2
    shift 2
    if "$@" >/state/error 2>&1; then cat /state/error; exit 1; fi
    grep -Ei "$match" /state/error >/dev/null || { cat /state/error; return 1; }
    echo "BROKER_VM_REFUSED $name"
}
env -u ZEROFS_RAW_CLAIM_SOCKET zerofs memory-status -c "$cfg" >/state/memory
refuse missing-transport 'ZEROFS_RAW_CLAIM_SOCKET' env -u ZEROFS_RAW_CLAIM_SOCKET zerofs pool verify-raw -c "$cfg"
refuse premature-disconnect 'incomplete claim packet|reset by peer' env ZEROFS_RAW_CLAIM_SOCKET=/run/claims/disconnect zerofs pool verify-raw -c "$cfg"
refuse missing-fd 'missing exclusive claim descriptor' env ZEROFS_RAW_CLAIM_SOCKET=/run/claims/missing zerofs pool verify-raw -c "$cfg"
refuse substituted-file 'must be a block device' env ZEROFS_RAW_CLAIM_SOCKET=/run/claims/file zerofs pool verify-raw -c "$cfg"
refuse substituted-sequence 'descriptor identity mismatch' env ZEROFS_RAW_CLAIM_SOCKET=/run/claims/sequence zerofs pool verify-raw -c "$cfg"
/bin/fault /state/untrusted.sock disconnect &
rogue=$!
while ! test -S /state/untrusted.sock; do kill -0 "$rogue"; sleep 0.1; done
refuse substituted-broker 'requires a root broker outside its PID namespace' env ZEROFS_RAW_CLAIM_SOCKET=/state/untrusted.sock zerofs pool verify-raw -c "$cfg"
wait "$rogue"
refuse run-before-pool-init 'original raw P3 journal is missing' zerofs run -c "$cfg"
zerofs pool init -c "$cfg" >/state/init
zerofs pool verify-raw -c "$cfg" >/state/verify
grep -q '"journal_verified": true' /state/verify
for field in authority_verified catalog_verified key_envelope_verified journal_verified; do grep -q "\"$field\": true" /state/verify; done
grep -q '"included_companion_bytes": 1610612736' /state/verify
grep -q '"journal_allocation": {' /state/verify
echo BROKER_VM_READY authority=true catalog=true key_envelope=true journal=true included_companion_bytes=1610612736
refuse mismatched-member 'mismatch' zerofs pool verify-raw -c /etc/wrong.toml
start_engine() {
    zerofs run -c "$cfg" >> /state/engine.log 2>&1 &
    engine=$!
    while ! test -S /state/9p.sock; do kill -0 "$engine"; sleep 0.1; done
}
start_engine
refuse exclusive 'busy|os error 16' zerofs pool verify-raw -c "$cfg"
stat -c %i "$ZEROFS_RAW_CLAIM_SOCKET" >/state/old-inode
touch /state/held
while ! test -f /state/disconnected; do sleep 0.1; done
dd if=/dev/urandom of=/state/original bs=1M count=12 2>/dev/null
zerofs upload "$target" /state/original /proof --jobs 1
zerofs flush -c "$cfg"
echo "BROKER_VM_AFTER_DISCONNECT uploaded_and_flushed_sha256=$(sha256sum /state/original)"
kill -TERM "$engine"
wait "$engine"
refuse no-broker-reopen 'refused|os error 111' zerofs pool verify-raw -c "$cfg"
touch /state/reconnect
while ! test -f /state/restarted; do sleep 0.1; done
zerofs pool verify-raw -c "$cfg" >/state/reopened
start_engine
zerofs download "$target" /proof /state/copy --jobs 1
cmp /state/original /state/copy
kill -TERM "$engine"
wait "$engine"
echo "BROKER_VM_RESTART recovered_sha256=$(sha256sum /state/copy)"
