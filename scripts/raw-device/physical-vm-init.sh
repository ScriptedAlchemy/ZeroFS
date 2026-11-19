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

topology_controls() {
    designated=/dev/disk/by-id/ata-ST6000DM003-2CY186_WF203KQ8
    target=$(readlink "$designated")
    disk=${target##*/}
    disk_dev=$(cat "/sys/block/$disk/dev")

    duplicate=/dev/disk/by-id/ata-ZeroFS_duplicate_WF203KQ8
    duplicate_target=$(readlink "$duplicate")
    duplicate_disk=${duplicate_target##*/}
    refuse duplicate-serial-alias 'by-id directory has ambiguous physical identity' "$claim" physical check-claim hdd6
    rm "$duplicate"
    refuse duplicate-kernel-serial 'physical serial or WWN is claimed by multiple whole disks' "$claim" physical check-claim hdd6
    echo 1 >"/sys/block/$duplicate_disk/device/delete"
    udevadm settle --timeout=20
    probe hdd6 6001175126016

    protected=/dev/disk/by-id/ata-ST16000NM001G-2KK103_ZL213G03
    ln -s "$target" "$protected"
    refuse protected-storage-alias 'physical claim intersects a protected disk alias' "$claim" physical check-claim hdd6
    rm "$protected"

    /bin/guest-partition "$designated" add
    udevadm settle --timeout=20
    test "$(cat "/sys/block/$disk/${disk}1/partition")" = 1
    refuse real-partition-alias 'by-id directory has ambiguous physical identity' "$claim" physical check-claim hdd6
    rm "${designated}-part1" /dev/disk/by-id/wwn-0x5000c500ac8aed0a-part1
    ln -s "${target}1" "${protected}-part1"
    refuse protected-partition-alias 'physical claim intersects a protected disk alias' "$claim" physical check-claim hdd6
    rm "${protected}-part1"
    refuse real-kernel-partition 'physical disk has partitions' "$claim" physical check-claim hdd6
    /bin/guest-partition "$designated" remove
    udevadm settle --timeout=20

    dmsetup create --noudevsync zerofs-fixture-claim --table "0 2048 linear $disk_dev 0"
    set -- "/sys/block/$disk/holders/"*
    test -e "$1"
    echo "RAW_PHYSICAL_VM_WITNESS holder=${1##*/} backing=$disk_dev"
    refuse device-mapper-holder 'physical disk has holders or slaves' "$claim" physical check-claim hdd6
    dmsetup remove --noudevsync zerofs-fixture-claim
    udevadm settle --timeout=20
    probe hdd6 6001175126016

    # Keep the original identity inventory while making a new signature that
    # only the mounted-filesystem inventory and direct held-fd probe can see.
    udevadm control --exit
    if grep -E '^E:ID_(FS_TYPE|PART_TABLE_TYPE|PART_ENTRY_TYPE)=' "/run/udev/data/b$disk_dev"; then
        echo "Unexpected storage type in the guest udev cache"
        exit 1
    fi
    mkfs.ext4 -q -F -b 4096 -E nodiscard,lazy_itable_init=0,lazy_journal_init=0 "$designated" 16384
    mount -t ext4 -o ro,noload "$designated" /claim-mount
    mounted=$(grep -F '/claim-mount ' /proc/self/mountinfo)
    echo "RAW_PHYSICAL_VM_WITNESS mountinfo=$mounted"
    refuse mounted-ext4 'physical disk is mounted' "$claim" physical check-claim hdd6
    umount /claim-mount
    test -s "/run/udev/data/b$disk_dev"
    if grep -E '^E:ID_(FS_TYPE|PART_TABLE_TYPE|PART_ENTRY_TYPE)=' "/run/udev/data/b$disk_dev"; then
        echo "New signature unexpectedly appeared in the guest udev cache"
        exit 1
    fi
    echo "RAW_PHYSICAL_VM_WITNESS ext4=64MiB mounted_then_unmounted udev_record_has_no_signature"
    refuse held-fd-foreign-ext4 'physical disk has a recognized foreign storage signature' "$claim" physical check-claim hdd6
}

for link in /dev/disk/by-id/*; do
    echo "RAW_PHYSICAL_VM_ALIAS $(basename "$link")=$(readlink "$link")"
done

read -r kernel_command_line </proc/cmdline
case " $kernel_command_line " in
    *" zerofs_fixture_model=controls "*)
        topology_controls
        echo "RAW_PHYSICAL_VM_COMPLETE"
        trap - EXIT
        poweroff -f
        ;;
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
