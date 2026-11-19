#!/bin/busybox sh
set -eu
export PATH=/bin:/usr/bin:/sbin:/usr/sbin
exec </dev/console >/dev/console 2>&1

failed() {
    echo "RAW_PHYSICAL_VM_FAIL"
    poweroff -f
}
trap failed EXIT
mount -t proc proc /proc
mount -t sysfs sysfs /sys
mount -t devtmpfs devtmpfs /dev
mount -t tmpfs tmpfs /run

while IFS= read -r filename; do
    insmod "/modules/$filename"
done </modules/load-order

/usr/lib/systemd/systemd-udevd --daemon
udevadm trigger --action=add --subsystem-match=block
udevadm settle --timeout=20

claim=/bin/zerofs-raw-device
probe() {
    role=$1
    bytes=$2
    result=$("$claim" physical check-claim "$role")
    echo "$result" | grep -F '"exclusive_claim_verified":true' >/dev/null
    echo "$result" | grep -F "\"held_device_bytes\":$bytes" >/dev/null
    echo "RAW_PHYSICAL_VM_PASS role=$role $result"
}

refuse() {
    control=$1
    reason=$2
    shift 2
    if result=$("$@" 2>&1); then
        echo "Unexpectedly accepted $control: $result"
        exit 1
    fi
    echo "$result" | grep -F "$reason" >/dev/null || {
        echo "Wrong reason for $control: $result"
        exit 1
    }
    echo "RAW_PHYSICAL_VM_REFUSAL control=$control $result"
}

for link in /dev/disk/by-id/*; do
    echo "RAW_PHYSICAL_VM_ALIAS $(basename "$link")=$(readlink "$link")"
done

read -r kernel_command_line </proc/cmdline
case " $kernel_command_line " in
    *" zerofs_fixture_model=p3 "*)
        probe nvme 1000204886016
        echo "RAW_PHYSICAL_VM_COMPLETE"
        trap - EXIT
        poweroff -f
        ;;
esac

probe hdd6 6001175126016
probe hdd18 18000207937536

hdd6=/dev/disk/by-id/ata-ST6000DM003-2CY186_WF203KQ8
hdd18=/dev/disk/by-id/ata-WDC_WD180EMFZ-11AFXA0_3WH7H3BJ
refuse competing-exclusive-open 'os error 16' /bin/hold-exclusive "$hdd6" "$claim" physical check-claim hdd6
probe hdd6 6001175126016

ln -sf "$(readlink "$hdd18")" "$hdd6"
refuse crossed-designations 'kernel disk serial mismatch' "$claim" physical check-claim hdd6

nvme=/dev/disk/by-id/nvme-CT1000P3SSD8_2318E6D18CE7
nvme_node=$(readlink /dev/disk/by-id/nvme-QEMU_NVMe_Ctrl_2318E6D18CE7)
ln -s "$nvme_node" "$nvme"
refuse stock-qemu-nvme-model 'expected the designated Crucial P3 namespace' "$claim" physical check-claim nvme

echo "RAW_PHYSICAL_VM_COMPLETE"
trap - EXIT
poweroff -f
