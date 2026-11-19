#!/bin/sh
set -eu
mount -t proc proc /app/proc
mount -t cgroup2 none /app/sys/fs/cgroup
mount --bind /run/claim /app/run/claims
mount -o remount,bind,ro /app/run/claims
mount -t tmpfs tmpfs /app/dev
mkdir /app/dev/shm
mknod -m 666 /app/dev/null c 1 3
mknod -m 444 /app/dev/urandom c 1 9
chmod 1777 /app/tmp /app/dev/shm
ip link set lo up
cd /app
# The wrong UID can traverse the socket as a member of its file group.
chroot . /bin/setpriv --reuid 20002 --regid 20001 --clear-groups --bounding-set=-all --no-new-privs \
    /bin/fault /run/claims/claim.sock denied-uid
echo BROKER_VM_REFUSED wrong-uid-even-with-socket-group
exec chroot . /bin/setpriv --reuid 20001 --regid 20001 --clear-groups --inh-caps=-all --ambient-caps=-all --bounding-set=-all --no-new-privs /client
