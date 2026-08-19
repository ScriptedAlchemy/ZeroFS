#!/usr/bin/env bash
set -euo pipefail

readonly mode=${1:-}
readonly expected_source=${2:-}
readonly unit_source=${3:-}
readonly mountpoint=/mnt/zerofs-files
readonly mount_unit='mnt-zerofs\x2dfiles.mount'
readonly unit_destination="/etc/systemd/system/${mount_unit}"
readonly receipt_prefix=ZEROFS_SHARED_NAMESPACE_V1
readonly legacy_mountpoint=/mnt/zerofs-lxc
readonly legacy_device=/dev/nbd0
readonly legacy_client_unit=zerofs-lxc-nbd-client.service
readonly legacy_client_hash=dc0e8c15fd8bd0ebdc36031ef09d3b7e6075a6152d5ba08fab2938041bfd65e9
readonly legacy_mount_hash=7d0335eaeb350b0d3893e351ae9fb3ff4d43c0dabec7a32279715b07b0beb37f
readonly legacy_mount_rw_hash=99015e0989c4fda8c9377fd1c2e062890c2d83c4c3673ad3415d552d8e73ecdf
readonly legacy_tuner_hash=2a1e4262ea74877b31bb74c9724f1b280c1b852ff5c9b834e4efc560f3e5db07
readonly legacy_normalizer_hash=1afa36606eb79301b17b2cb2a9c3e53a68ff90cf6669d8644763ea4b99a33778
readonly legacy_bindfs_hash=013e9481f2bf7e0ba66f4dbc60bba64937da88293f7f3732c0e62c7cb2c5b33d
readonly legacy_bindfs_permissions_hash=9a0e6e3501a971c13b5d5ad7e609cc92989f83c197821f0c09596a02c3cbeac2
readonly legacy_raw_normalized_hash=fc3dbfeac323bc1dd1c1595e6fa230e1346032c73a81923f9a7be2c63951d30c
readonly legacy_raw_guard_hash=9529ae5e8ab7f7250904146a86f807bd5b1f989ab39ddd3bd2a36f0b5b810370
readonly legacy_exposed_guard_hash=0ecd780b457bce4647ba55b7ab9560aba2ffc116bc7b01cd4e3cbe8f08f595cd
readonly legacy_permissions_hash=62d2ad7a6918ddd64ba03f7a87d7572b90d0f5595f4beeaa1f9d9eb0121416e6

readonly -a legacy_mount_units=(
  'mnt-zerofs\x2dlxc.mount'
  'mnt-zerofs-lxc.mount'
)
readonly -a legacy_nbd_artifacts=(
  '/etc/systemd/system/mnt-zerofs\x2dlxc.mount'
  '/etc/systemd/system/mnt-zerofs-lxc.mount'
  '/etc/systemd/system/zerofs-lxc-nbd-client.service'
  '/usr/local/libexec/zerofs-tune-nbd'
  '/etc/zerofs-lxc/client.env'
)
readonly -a legacy_namespace_units=(
  'mnt-zerofs\x2dfiles\x2draw.mount'
  'mnt-zerofs\x2dfiles\x2draw-.nbd.mount'
  'mnt-zerofs\x2dfiles-.nbd.mount'
  'zerofs-shared-namespace-permissions.service'
)
readonly -a legacy_namespace_mounts=(
  '/mnt/zerofs-files-raw/.nbd'
  '/mnt/zerofs-files-raw'
)
readonly -a legacy_namespace_artifacts=(
  '/etc/systemd/system/mnt-zerofs\x2dfiles\x2draw.mount'
  '/etc/systemd/system/mnt-zerofs\x2dfiles\x2draw-.nbd.mount'
  '/etc/systemd/system/mnt-zerofs\x2dfiles-.nbd.mount'
  '/etc/systemd/system/zerofs-shared-namespace-permissions.service'
  '/usr/local/libexec/zerofs-normalize-shared-namespace'
)

die() {
  echo "error: $*" >&2
  exit 1
}

unit_loaded() {
  [[ $(systemctl show -p LoadState --value "$1" 2>/dev/null || true) != not-found ]]
}

assert_file_hash() {
  local path=$1 expected=$2 label=$3 alternative=${4:-} actual
  [[ -f $path ]] || die "$label is loaded but its canonical file is missing: $path"
  actual=$(sha256sum "$path" | awk '{print $1}')
  [[ $actual == "$expected" || ( -n $alternative && $actual == "$alternative" ) ]] || \
    die "$label content is not a recognized ZeroFS artifact: $path"
}

assert_unit_fragment() {
  local unit=$1 expected=$2 fragment
  fragment=$(systemctl show -p FragmentPath --value "$unit" 2>/dev/null || true)
  [[ $fragment == "$expected" ]] || die "unit $unit has unexpected fragment $fragment"
}

assert_unit_lines() {
  local path=$1 label=$2
  shift 2
  [[ -f $path ]] || die "$label is missing: $path"
  local line
  for line in "$@"; do
    grep -Fqx "$line" "$path" || die "$label is not canonical: missing $line"
  done
}

validate_args() {
  case "$mode" in
    preflight | reconcile) ;;
    *) die 'mode must be preflight or reconcile' ;;
  esac
  case "$expected_source" in
    10.*:/ | 192.168.*:/ | 172.1[6-9].*:/ | 172.2[0-9].*:/ | 172.3[01].*:/) ;;
    *) die 'source must be an RFC1918 IPv4 NFS root export' ;;
  esac
  if [[ $mode == reconcile && ! -f $unit_source ]]; then
    die 'reconcile requires a rendered mount unit file'
  fi
}

mount_record() {
  findmnt -rn -M "$mountpoint" -o SOURCE,FSTYPE,OPTIONS 2>/dev/null || true
}

mount_matches() {
  local record actual_source fstype options
  record=$(mount_record)
  [[ -n $record ]] || return 1
  read -r actual_source fstype options <<<"$record"
  [[ $actual_source == "$expected_source" && $fstype == nfs && ",${options}," == *,rw,* ]]
}

emit_unverified_receipt() {
  printf '%s verified=0 objects=0 wrong_owner=0 first_uid=-1 first_gid=-1 reason=%s\n' \
    "$receipt_prefix" "$1"
}

preflight() {
  local record actual_source fstype options objects wrong_owner first_uid first_gid
  record=$(mount_record)
  if [[ -z $record ]]; then
    emit_unverified_receipt mount_unavailable
    return
  fi
  read -r actual_source fstype options <<<"$record"
  if [[ $actual_source == /mnt/zerofs-files-raw && $fstype == fuse.bindfs && ",${options}," == *,rw,* ]]; then
    assert_unit_fragment "$mount_unit" "$unit_destination"
    assert_file_hash "$unit_destination" "$legacy_bindfs_hash" \
      'legacy bindfs namespace unit' "$legacy_bindfs_permissions_hash"
    emit_unverified_receipt legacy_topology
    return
  fi
  if [[ $actual_source != "$expected_source" || $fstype != nfs || ",${options}," != *,rw,* ]]; then
    emit_unverified_receipt mount_mismatch
    return
  fi

  objects=$(find "$mountpoint" -xdev -path "$mountpoint/.nbd" -prune -o -printf . | wc -c)
  wrong_owner=$(find "$mountpoint" -xdev -path "$mountpoint/.nbd" -prune -o \
    \( ! -uid 501 -o ! -gid 20 \) -printf . | wc -c)
  first_uid=$(find "$mountpoint" -xdev -path "$mountpoint/.nbd" -prune -o \
    \( ! -uid 501 -o ! -gid 20 \) -printf '%U' -quit)
  first_gid=$(find "$mountpoint" -xdev -path "$mountpoint/.nbd" -prune -o \
    \( ! -uid 501 -o ! -gid 20 \) -printf '%G' -quit)
  printf '%s verified=1 objects=%s wrong_owner=%s first_uid=%s first_gid=%s reason=ok\n' \
    "$receipt_prefix" "$objects" "$wrong_owner" "${first_uid:--1}" "${first_gid:--1}"
}

retire_legacy_nbd() {
  local owned=0 source pid unit path client_known=0 actual_env expected_env
  if [[ -e /etc/zerofs-lxc/client.env ]]; then
    [[ -f /etc/zerofs-lxc/client.env ]] || die 'legacy NBD client environment is not a regular file'
    expected_env=$(printf '%s\n' \
      "ZEROFS_NBD_HOST=${expected_source%:/}" \
      'ZEROFS_NBD_PORT=10809' \
      'ZEROFS_NBD_EXPORT=vm100-pilot-64g' \
      'ZEROFS_NBD_CONNECTIONS=8' \
      'ZEROFS_NBD_DEVICE=/dev/nbd0' | LC_ALL=C sort)
    actual_env=$(LC_ALL=C sort /etc/zerofs-lxc/client.env)
    [[ $actual_env == "$expected_env" ]] || \
      die 'legacy NBD client environment is not canonical'
  fi
  path=/etc/systemd/system/zerofs-lxc-nbd-client.service
  if unit_loaded "$legacy_client_unit" || [[ -e $path ]]; then
    unit_loaded "$legacy_client_unit" && assert_unit_fragment "$legacy_client_unit" "$path"
    assert_file_hash "$path" "$legacy_client_hash" 'legacy NBD client unit'
    [[ -f /etc/zerofs-lxc/client.env ]] || die 'legacy NBD client environment is missing'
    client_known=1
  fi
  if systemctl is-active --quiet "$legacy_client_unit"; then
    [[ $client_known == 1 ]] || die 'active legacy NBD client is not canonical'
    owned=1
  fi
  for unit in "${legacy_mount_units[@]}"; do
    path="/etc/systemd/system/${unit}"
    if unit_loaded "$unit" || [[ -e $path ]]; then
      unit_loaded "$unit" && assert_unit_fragment "$unit" "$path"
      assert_file_hash "$path" "$legacy_mount_hash" 'legacy NBD mount unit' \
        "$legacy_mount_rw_hash"
    fi
  done
  if [[ -e /usr/local/libexec/zerofs-tune-nbd ]]; then
    assert_file_hash /usr/local/libexec/zerofs-tune-nbd "$legacy_tuner_hash" 'legacy NBD tuner'
  fi
  if findmnt -rn -M "$legacy_mountpoint" >/dev/null 2>&1; then
    source=$(findmnt -nro SOURCE -M "$legacy_mountpoint")
    [[ $source == "$legacy_device" ]] || die "unexpected legacy mount source: $source"
    owned=1
    sync -f "$legacy_mountpoint"
  fi
  for unit in "${legacy_mount_units[@]}"; do
    if unit_loaded "$unit"; then
      systemctl disable --now "$unit"
      systemctl is-enabled --quiet "$unit" && die "legacy mount unit remains enabled: $unit"
      [[ $(systemctl is-active "$unit" 2>/dev/null || true) != active ]]
    fi
  done
  if findmnt -rn -M "$legacy_mountpoint" >/dev/null 2>&1; then
    umount "$legacy_mountpoint"
  fi
  ! findmnt -rn -M "$legacy_mountpoint" >/dev/null 2>&1 || die 'legacy NBD mount remains active'
  if unit_loaded "$legacy_client_unit"; then
    systemctl disable --now "$legacy_client_unit"
    systemctl is-enabled --quiet "$legacy_client_unit" && die 'legacy NBD client remains enabled'
    [[ $(systemctl is-active "$legacy_client_unit" 2>/dev/null || true) != active ]]
  fi
  if [[ -r /sys/class/block/nbd0/pid ]]; then
    pid=$(</sys/class/block/nbd0/pid)
  else
    pid=
  fi
  if [[ -n $pid ]]; then
    [[ $owned == 1 ]] || die 'nbd0 is connected without recognized legacy ZeroFS state'
    command -v nbd-client >/dev/null || die 'nbd-client is required for legacy disconnect'
    nbd-client -d "$legacy_device"
  fi
  [[ ! -s /sys/class/block/nbd0/pid ]] || die 'legacy nbd0 remains connected'
  rm -f -- "${legacy_nbd_artifacts[@]}"
}

retire_legacy_namespace() {
  local unit legacy_mount path
  path='/etc/systemd/system/mnt-zerofs\x2dfiles\x2draw.mount'
  if unit_loaded 'mnt-zerofs\x2dfiles\x2draw.mount' || [[ -e $path ]]; then
    unit_loaded 'mnt-zerofs\x2dfiles\x2draw.mount' && \
      assert_unit_fragment 'mnt-zerofs\x2dfiles\x2draw.mount' "$path"
    assert_unit_lines "$path" 'legacy raw namespace unit' \
      "What=${expected_source}" 'Where=/mnt/zerofs-files-raw' 'Type=nfs'
    actual=$(sed "s|^What=${expected_source}$|What=@SOURCE@|" "$path" | sha256sum | awk '{print $1}')
    [[ $actual == "$legacy_raw_normalized_hash" ]] || \
      die 'legacy raw namespace unit contains unexpected directives'
  fi
  path='/etc/systemd/system/mnt-zerofs\x2dfiles\x2draw-.nbd.mount'
  if unit_loaded 'mnt-zerofs\x2dfiles\x2draw-.nbd.mount' || [[ -e $path ]]; then
    unit_loaded 'mnt-zerofs\x2dfiles\x2draw-.nbd.mount' && \
      assert_unit_fragment 'mnt-zerofs\x2dfiles\x2draw-.nbd.mount' "$path"
    assert_unit_lines "$path" 'legacy raw NBD guard unit' \
      'What=/mnt/zerofs-files-raw/.nbd' \
      'Where=/mnt/zerofs-files-raw/.nbd' 'Type=none' \
      'Options=bind,ro,nosuid,nodev,noexec,_netdev'
    assert_file_hash "$path" "$legacy_raw_guard_hash" 'legacy raw NBD guard unit'
  fi
  path='/etc/systemd/system/mnt-zerofs\x2dfiles-.nbd.mount'
  if unit_loaded 'mnt-zerofs\x2dfiles-.nbd.mount' || [[ -e $path ]]; then
    unit_loaded 'mnt-zerofs\x2dfiles-.nbd.mount' && \
      assert_unit_fragment 'mnt-zerofs\x2dfiles-.nbd.mount' "$path"
    assert_unit_lines "$path" 'legacy exposed NBD guard unit' \
      'What=/mnt/zerofs-files/.nbd' 'Where=/mnt/zerofs-files/.nbd' \
      'Type=none' 'Options=bind,ro,nosuid,nodev,noexec,_netdev'
    assert_file_hash "$path" "$legacy_exposed_guard_hash" 'legacy exposed NBD guard unit'
  fi
  path=/etc/systemd/system/zerofs-shared-namespace-permissions.service
  if unit_loaded zerofs-shared-namespace-permissions.service || [[ -e $path ]]; then
    unit_loaded zerofs-shared-namespace-permissions.service && \
      assert_unit_fragment zerofs-shared-namespace-permissions.service "$path"
    assert_unit_lines "$path" 'legacy namespace permissions service' \
      'Type=oneshot' \
      'ExecStart=/usr/local/libexec/zerofs-normalize-shared-namespace'
    assert_file_hash "$path" "$legacy_permissions_hash" 'legacy namespace permissions service'
  fi
  if [[ -e /usr/local/libexec/zerofs-normalize-shared-namespace ]]; then
    assert_file_hash /usr/local/libexec/zerofs-normalize-shared-namespace \
      "$legacy_normalizer_hash" 'legacy namespace normalizer'
  fi
  for unit in "${legacy_namespace_units[@]}"; do
    if unit_loaded "$unit"; then
      systemctl disable --now "$unit"
      systemctl is-enabled --quiet "$unit" && die "legacy namespace unit remains enabled: $unit"
      [[ $(systemctl is-active "$unit" 2>/dev/null || true) != active ]]
    fi
  done
  for legacy_mount in "${legacy_namespace_mounts[@]}"; do
    ! findmnt -rn -M "$legacy_mount" >/dev/null 2>&1 || \
      die "legacy namespace mount remains active: $legacy_mount"
    if [[ -d $legacy_mount ]]; then
      rmdir -- "$legacy_mount" || die "legacy namespace mountpoint is not empty: $legacy_mount"
    fi
  done
  rm -f -- "${legacy_namespace_artifacts[@]}"
}

legacy_state_present() {
  local item
  [[ -s /sys/class/block/nbd0/pid ]] && return 0
  findmnt -rn -M "$legacy_mountpoint" >/dev/null 2>&1 && return 0
  for item in "${legacy_nbd_artifacts[@]}" "${legacy_namespace_artifacts[@]}"; do
    [[ -e $item ]] && return 0
  done
  for item in "${legacy_mount_units[@]}" "$legacy_client_unit" \
    "${legacy_namespace_units[@]}"; do
    unit_loaded "$item" && return 0
  done
  return 1
}

reconcile() {
  grep -Fqx "What=${expected_source}" "$unit_source" || die 'rendered mount source differs'
  grep -Fqx "Where=${mountpoint}" "$unit_source" || die 'rendered mountpoint differs'
  grep -Fqx 'Type=nfs' "$unit_source" || die 'rendered mount type differs'

  if cmp -s "$unit_source" "$unit_destination" && mount_matches && \
    systemctl is-enabled --quiet "$mount_unit" && \
    systemctl is-active --quiet "$mount_unit" && ! legacy_state_present; then
    echo 'ZeroFS NFS mount already reconciled'
    return
  fi

  retire_legacy_nbd
  retire_legacy_namespace
  if [[ -n $(mount_record) ]] && ! mount_matches; then
    die 'refusing to replace an unexpected mount at /mnt/zerofs-files'
  fi
  if unit_loaded "$mount_unit"; then
    systemctl stop "$mount_unit"
  fi
  [[ -z $(mount_record) ]] || die 'managed mount remained active before install'
  install -d -m 0755 "$mountpoint" /etc/systemd/system
  install -m 0644 "$unit_source" "$unit_destination"
  systemctl daemon-reload
  systemctl enable --now "$mount_unit"
  systemctl is-enabled --quiet "$mount_unit"
  systemctl is-active --quiet "$mount_unit"
  mount_matches || die 'reconciled mount failed source/type/rw verification'
}

validate_args
case "$mode" in
  preflight) preflight ;;
  reconcile) reconcile ;;
esac
