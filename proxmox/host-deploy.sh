#!/usr/bin/env bash
set -Eeuo pipefail

usage() {
  cat <<'EOF'
Usage: host-deploy.sh deploy|replace|cleanup|promote|finalize|commit|rollback|recover [options]

This script runs on a Proxmox VE host. It never removes the persistent state
bind mount. `replace` additionally requires --confirm-replace CTID.
EOF
}

action=${1:-}
case "$action" in
  deploy|replace|cleanup|promote|finalize|commit|rollback|recover) shift ;;
  *) usage >&2; exit 2 ;;
esac

ctid=
role=
container_ip=
bridge=vmbr1
gateway=10.10.10.1
template=
rootfs=local-lvm:8
memory_mb=98304
cores=8
state_root=
stage=
commit=
binary_sha=
namespace_id=
release_id=
samba_user=zerofs-share
prod_access=nfs
confirm_replace=
dry_run=false
assume_existing=false
assume_stopped=false
dry_run_ct_destroyed=false
defer_commit=false
maintenance_nfs_only=false
drain_timeout=1800
local_durable_upgrade=false
hotpath_profile=0

while (($#)); do
  case "$1" in
    --ctid) ctid=$2; shift 2 ;;
    --role) role=$2; shift 2 ;;
    --container-ip) container_ip=$2; shift 2 ;;
    --bridge) bridge=$2; shift 2 ;;
    --gateway) gateway=$2; shift 2 ;;
    --template) template=$2; shift 2 ;;
    --rootfs) rootfs=$2; shift 2 ;;
    --memory-mb) memory_mb=$2; shift 2 ;;
    --cores) cores=$2; shift 2 ;;
    --state-root) state_root=$2; shift 2 ;;
    --stage) stage=$2; shift 2 ;;
    --commit) commit=$2; shift 2 ;;
    --sha256) binary_sha=$2; shift 2 ;;
    --namespace-id) namespace_id=$2; shift 2 ;;
    --release-id) release_id=$2; shift 2 ;;
    --samba-user) samba_user=$2; shift 2 ;;
    --prod-access) prod_access=$2; shift 2 ;;
    --confirm-replace) confirm_replace=$2; shift 2 ;;
    --assume-existing) assume_existing=true; shift ;;
    --assume-stopped) assume_stopped=true; shift ;;
    --defer-commit) defer_commit=true; shift ;;
    --maintenance-nfs-only) maintenance_nfs_only=true; shift ;;
    --drain-timeout) drain_timeout=$2; shift 2 ;;
    --local-durable-upgrade) local_durable_upgrade=true; shift ;;
    --hotpath-profile) hotpath_profile=$2; shift 2 ;;
    --dry-run) dry_run=true; shift ;;
    *) echo "unknown argument: $1" >&2; exit 2 ;;
  esac
done

[[ $hotpath_profile == 0 || $hotpath_profile == 1 ]] || {
  echo "--hotpath-profile must be 0 or 1" >&2
  exit 2
}

if [[ $assume_stopped == true && $dry_run != true ]]; then
  echo "--assume-stopped is valid only with --dry-run" >&2
  exit 2
fi

[[ $ctid =~ ^[1-9][0-9]{2,8}$ ]] || { echo "invalid --ctid" >&2; exit 2; }
[[ $role == prod || $role == dev ]] || { echo "--role must be prod or dev" >&2; exit 2; }
[[ $prod_access == nfs || $prod_access == smb || $prod_access == both ]] || {
  echo "--prod-access must be nfs, smb, or both" >&2
  exit 2
}
stage_smb=false
if [[ $role == prod && ( $prod_access == smb || $prod_access == both ) ]]; then
  stage_smb=true
fi
has_smb=$stage_smb
[[ $maintenance_nfs_only == true ]] && has_smb=false
[[ $samba_user =~ ^[a-z_][a-z0-9_-]{0,31}$ ]] || {
  echo "unsafe Samba user name" >&2
  exit 2
}
[[ $memory_mb =~ ^[1-9][0-9]*$ ]] || { echo "invalid --memory-mb" >&2; exit 2; }
if ! [[ $cores =~ ^[1-9][0-9]*$ ]] || ((cores > 256)); then
  echo "invalid --cores" >&2
  exit 2
fi
[[ $drain_timeout =~ ^[1-9][0-9]*$ ]] || {
  echo "invalid --drain-timeout" >&2
  exit 2
}
[[ $bridge =~ ^[A-Za-z0-9_.-]+$ ]] || { echo "invalid --bridge" >&2; exit 2; }
[[ $rootfs =~ ^[A-Za-z0-9_.-]+:[1-9][0-9]*$ ]] || {
  echo "invalid --rootfs" >&2
  exit 2
}
if [[ $role == prod && ( $action == replace || $action == cleanup ) ]]; then
  echo "prod permits in-place deploy only; replace and cleanup are dev-only" >&2
  exit 2
fi
if [[ $defer_commit == true && ! ( $role == prod && $action == deploy ) ]]; then
  echo "--defer-commit is valid only for production deploy" >&2
  exit 2
fi
if [[ $maintenance_nfs_only == true && ! ( $role == prod && $action == deploy && $defer_commit == true ) ]]; then
  echo "--maintenance-nfs-only requires a deferred production deploy" >&2
  exit 2
fi
if [[ $local_durable_upgrade == true && ! ( $role == prod && $action == deploy && $defer_commit == true ) ]]; then
  echo "--local-durable-upgrade requires a deferred production deploy" >&2
  exit 2
fi
if [[ $action == promote || $action == finalize || $action == commit || $action == rollback || $action == recover ]] && [[ $role != prod ]]; then
  echo "$action controls only a production deployment transaction" >&2
  exit 2
fi
[[ $container_ip =~ ^10\.[0-9]+\.[0-9]+\.[0-9]+$|^172\.(1[6-9]|2[0-9]|3[01])\.[0-9]+\.[0-9]+$|^192\.168\.[0-9]+\.[0-9]+$ ]] || {
  echo "--container-ip must be an RFC1918 IPv4 address" >&2
  exit 2
}
[[ $gateway =~ ^10\.[0-9]+\.[0-9]+\.[0-9]+$|^172\.(1[6-9]|2[0-9]|3[01])\.[0-9]+\.[0-9]+$|^192\.168\.[0-9]+\.[0-9]+$ ]] || {
  echo "--gateway must be an RFC1918 IPv4 address" >&2
  exit 2
}
expected_state_root="/var/lib/zerofs-lxc/$role-$ctid"
[[ $state_root == "$expected_state_root" ]] || {
  echo "--state-root must be exactly $expected_state_root" >&2
  exit 2
}
if [[ $action == replace && $confirm_replace != "$ctid" ]]; then
  echo "replace requires --confirm-replace $ctid" >&2
  exit 2
fi
if [[ $action != cleanup ]]; then
  [[ -n $template && -n $stage && -n $commit && -n $binary_sha && $namespace_id =~ ^[0-9a-f]{64}$ && $release_id =~ ^[0-9a-f]{12}-[A-Za-z0-9]{6,32}$ ]] || {
    echo "deploy metadata and staging options are required" >&2
    exit 2
  }
fi
deployment_transaction="$state_root/deployment-transaction"

run() {
  printf '+ '
  printf '%s ' "$@"
  printf '\n'
  if [[ $dry_run == false ]]; then
    "$@"
  fi
}

assert_managed_state_child() {
  local path=$1 expected_uid=$2 expected_gid=$3 expected_mode=${4#0}
  local compatible_mode=${5:-} actual
  compatible_mode=${compatible_mode#0}
  if [[ -L $path || ( -e $path && ! -d $path ) ]]; then
    echo "unsafe managed state child (expected directory, never a symlink): $path" >&2
    return 1
  fi
  [[ -e $path ]] || return 0
  actual=$(stat -c '%u:%g:%a' -- "$path")
  if [[ $actual != "$expected_uid:$expected_gid:$expected_mode" \
    && ( -z $compatible_mode || $actual != "$expected_uid:$expected_gid:$compatible_mode" ) ]]; then
    echo "unsafe managed state child ownership/mode: $path ($actual)" >&2
    return 1
  fi
}

writeback_dir_from_config() {
  awk '
    /^\[writeback\][[:space:]]*$/ { in_writeback=1; next }
    /^\[/ { in_writeback=0 }
    in_writeback && /^[[:space:]]*dir[[:space:]]*=/ {
      value=$0
      sub(/^[[:space:]]*dir[[:space:]]*=[[:space:]]*"/, "", value)
      sub(/"[[:space:]]*(#.*)?$/, "", value)
      print value
      found++
    }
    END { if (found != 1) exit 1 }
  ' "$1"
}

prepare_managed_state_tree() {
  if [[ $dry_run == true ]]; then
    echo "+ validate the persistent state root and every managed immediate child"
    echo "+ install -d -o 0 -g 100000 -m 0750 $state_root"
    return 0
  fi
  if [[ -L $state_root || ( -e $state_root && ! -d $state_root ) ]]; then
    echo "unsafe persistent state root (expected directory, never a symlink): $state_root" >&2
    return 1
  fi
  if [[ -d $state_root ]]; then
    local root_state
    root_state=$(stat -c '%u:%g:%a' -- "$state_root")
    if [[ $root_state != "0:100000:750" && $root_state != "100000:100000:750" ]]; then
      echo "unsafe persistent state root ownership/mode: $state_root ($root_state)" >&2
      return 1
    fi
    # Remove mapped CT root's write access to the parent before inspecting any
    # child. Once frozen, CT processes cannot swap a validated child for a link.
    install -d -o 0 -g 100000 -m 0750 "$state_root"
    assert_managed_state_child "$state_root/releases" 0 0 0755
    assert_managed_state_child "$state_root/receipts" 0 0 0755
    assert_managed_state_child "$state_root/rollback" 0 0 0700 0755
    assert_managed_state_child "$state_root/deployment-transaction" 0 0 0700
    assert_managed_state_child "$state_root/state" 100000 100000 0750
    assert_managed_state_child "$state_root/cache" 100000 100000 0750
    assert_managed_state_child "$state_root/backend-dev" 100000 100000 0750
    local child name
    while IFS= read -r child; do
      name=${child##*/}
      case $name in
        releases|receipts|rollback|deployment-transaction|state|cache|backend-dev) ;;
        current)
          [[ -L $child ]] || {
            echo "unsafe managed state child (current must be a symlink): $child" >&2
            return 1
          }
          ;;
        .zerofs-lxc-state)
          [[ -f $child && ! -L $child && $(stat -c '%u:%g' -- "$child") == 0:0 ]] || {
            echo "unsafe managed state marker: $child" >&2
            return 1
          }
          ;;
        *)
          echo "unknown immediate child in persistent state root: $child" >&2
          return 1
          ;;
      esac
    done < <(find -P "$state_root" -mindepth 1 -maxdepth 1 -print)
  else
    install -d -o 0 -g 100000 -m 0750 "$state_root"
  fi
}

if [[ $dry_run == false ]]; then
  [[ $EUID -eq 0 ]] || { echo "host deployment requires root" >&2; exit 1; }
  command -v pct >/dev/null
  command -v pveversion >/dev/null
  command -v flock >/dev/null
  pveversion >/dev/null
  exec 8>"/run/lock/zerofs-lxc-deploy-global.lock"
  flock -n 8 || { echo "another ZeroFS host deployment is active" >&2; exit 75; }
else
  echo "+ acquire exclusive host lock /run/lock/zerofs-lxc-deploy-global.lock"
fi

prepare_managed_state_tree

ct_exists() {
  if [[ $dry_run == true ]]; then
    [[ $dry_run_ct_destroyed == false ]] || return 1
    [[ $assume_existing == true || $action != deploy ]]
    return
  fi
  pct config "$ctid" >/dev/null 2>&1
}

ct_running() {
  if [[ $dry_run == true ]]; then
    [[ $assume_stopped != true ]]
    return
  fi
  [[ $(pct status "$ctid" 2>/dev/null) == "status: running" ]]
}

ct_resource_snapshot=
ct_resources_mutated=false

config_value() {
  local config=$1 key=$2
  awk -F ': ' -v wanted="$key" '$1 == wanted { sub(/^[^:]*: /, ""); print; found=1; exit } END { if (!found) exit 1 }' "$config"
}

csv_field() {
  local input=$1 wanted=$2 field
  local old_ifs=$IFS
  IFS=,
  for field in $input; do
    if [[ ${field%%=*} == "$wanted" ]]; then
      printf '%s\n' "${field#*=}"
      IFS=$old_ifs
      return 0
    fi
  done
  IFS=$old_ifs
  return 1
}

csv_set_field() {
  local input=$1 wanted=$2 value=$3 field output='' found=false
  local old_ifs=$IFS
  IFS=,
  for field in $input; do
    [[ -n $field ]] || continue
    if [[ ${field%%=*} == "$wanted" ]]; then
      field="$wanted=$value"
      found=true
    fi
    if [[ -n $output ]]; then
      output="$output,$field"
    else
      output=$field
    fi
  done
  IFS=$old_ifs
  if [[ $found == false ]]; then
    if [[ -n $output ]]; then
      output="$output,$wanted=$value"
    else
      output="$wanted=$value"
    fi
  fi
  printf '%s\n' "$output"
}

desired_net0() {
  local value=$1
  value=$(csv_set_field "$value" name eth0)
  value=$(csv_set_field "$value" bridge "$bridge")
  value=$(csv_set_field "$value" ip "$container_ip/24")
  value=$(csv_set_field "$value" gw "$gateway")
  value=$(csv_set_field "$value" type veth)
  printf '%s\n' "$value"
}

desired_mp0() {
  local value=$1 first rest
  if [[ -n $value ]]; then
    first=${value%%,*}
    if [[ $first == *=* ]]; then
      value="$state_root,$value"
    else
      rest=${value#*,}
      if [[ $rest == "$value" ]]; then
        value=$state_root
      else
        value="$state_root,$rest"
      fi
    fi
  else
    value=$state_root
  fi
  value=$(csv_set_field "$value" mp /srv/zerofs-persist)
  if [[ $(csv_field "$value" ro 2>/dev/null || true) == 1 ]]; then
    value=$(csv_set_field "$value" ro 0)
  fi
  printf '%s\n' "$value"
}

desired_features() {
  local value=$1
  if [[ $stage_smb == true ]]; then
    value=$(csv_set_field "$value" fuse 1)
  fi
  printf '%s\n' "$value"
}

capture_ct_resources() {
  [[ $had_ct == true && $action == deploy ]] || return 0
  if [[ $dry_run == true ]]; then
    echo "+ capture existing CT resource snapshot"
    return 0
  fi
  local snapshot
  snapshot=$(mktemp "/var/tmp/zerofs-ct-$ctid-resources.XXXXXX")
  chmod 0600 "$snapshot"
  if ! pct config "$ctid" >"$snapshot"; then
    rm -f -- "$snapshot"
    return 1
  fi
  ct_resource_snapshot=$snapshot
}

restore_ct_resources() {
  [[ $ct_resources_mutated == true && -n $ct_resource_snapshot ]] || return 0
  local key value failed=false
  for key in cores memory swap onboot startup net0 mp0 hookscript features; do
    if value=$(config_value "$ct_resource_snapshot" "$key"); then
      if ! pct set "$ctid" "--$key" "$value" >/dev/null 2>&1; then
        echo "rollback failed to restore CT resource: $key" >&2
        failed=true
      fi
    else
      if ! pct set "$ctid" --delete "$key" >/dev/null 2>&1; then
        echo "rollback failed to remove newly introduced CT resource: $key" >&2
        failed=true
      fi
    fi
  done
  [[ $failed == false ]] || return 1
  assert_ct_resource_snapshot
}

assert_ct_resource_snapshot() {
  local current key before after valid=true
  current=$(mktemp "/var/tmp/zerofs-ct-$ctid-rollback-verify.XXXXXX")
  chmod 0600 "$current"
  if ! pct config "$ctid" >"$current"; then
    rm -f -- "$current"
    return 1
  fi
  for key in cores memory swap onboot startup net0 mp0 hookscript features; do
    before=$(config_value "$ct_resource_snapshot" "$key" 2>/dev/null || true)
    after=$(config_value "$current" "$key" 2>/dev/null || true)
    [[ $before == "$after" ]] || valid=false
  done
  rm -f -- "$current"
  [[ $valid == true ]]
}

assert_ct_resources() {
  [[ $dry_run == false ]] || return 0
  local config current current_net current_mp valid=true
  config=$(mktemp "/var/tmp/zerofs-ct-$ctid-verify.XXXXXX")
  chmod 0600 "$config"
  if ! pct config "$ctid" >"$config"; then
    rm -f -- "$config"
    return 1
  fi
  [[ $(config_value "$config" cores 2>/dev/null || true) == "$cores" ]] || valid=false
  [[ $(config_value "$config" memory 2>/dev/null || true) == "$memory_mb" ]] || valid=false
  [[ $(config_value "$config" swap 2>/dev/null || true) == 0 ]] || valid=false
  [[ $(config_value "$config" onboot 2>/dev/null || true) == 1 ]] || valid=false
  current=$(config_value "$config" startup 2>/dev/null || true)
  [[ $(csv_field "$current" order 2>/dev/null || true) == 20 ]] || valid=false
  current_net=$(config_value "$config" net0 2>/dev/null || true)
  [[ $(csv_field "$current_net" name 2>/dev/null || true) == eth0 ]] || valid=false
  [[ $(csv_field "$current_net" bridge 2>/dev/null || true) == "$bridge" ]] || valid=false
  [[ $(csv_field "$current_net" ip 2>/dev/null || true) == "$container_ip/24" ]] || valid=false
  [[ $(csv_field "$current_net" gw 2>/dev/null || true) == "$gateway" ]] || valid=false
  [[ $(csv_field "$current_net" type 2>/dev/null || true) == veth ]] || valid=false
  current_mp=$(config_value "$config" mp0 2>/dev/null || true)
  [[ ${current_mp%%,*} == "$state_root" ]] || valid=false
  [[ $(csv_field "$current_mp" mp 2>/dev/null || true) == /srv/zerofs-persist ]] || valid=false
  [[ $(csv_field "$current_mp" ro 2>/dev/null || true) != 1 ]] || valid=false
  [[ $(config_value "$config" hookscript 2>/dev/null || true) == local:snippets/zerofs-lxc-hook.sh ]] || valid=false
  if [[ $stage_smb == true ]]; then
    current=$(config_value "$config" features 2>/dev/null || true)
    [[ $(csv_field "$current" fuse 2>/dev/null || true) == 1 ]] || valid=false
  fi
  rm -f -- "$config"
  if [[ $valid != true ]]; then
    echo "CT resources do not match the requested deployment profile" >&2
    return 1
  fi
}

reconcile_ct_resources() {
  [[ $had_ct == true && $action == deploy ]] || return 0
  if [[ $dry_run == true ]]; then
    echo "+ validate/reconcile existing CT resources memory=$memory_mb cores=$cores swap=0 onboot=1 startup=order=20 net0=name=eth0,bridge=$bridge,ip=$container_ip/24,gw=$gateway,type=veth mp0=$state_root,mp=/srv/zerofs-persist"
    echo "+ rollback restores captured CT resources"
    return 0
  fi
  local config current desired key value changed=false
  config=$ct_resource_snapshot
  for key in cores memory swap onboot; do
    case $key in
      cores) desired=$cores ;;
      memory) desired=$memory_mb ;;
      swap) desired=0 ;;
      onboot) desired=1 ;;
    esac
    current=$(config_value "$config" "$key" 2>/dev/null || true)
    [[ $current == "$desired" ]] || changed=true
  done
  current=$(config_value "$config" startup 2>/dev/null || true)
  [[ $current == "$(csv_set_field "$current" order 20)" ]] || changed=true
  current=$(config_value "$config" net0 2>/dev/null || true)
  [[ $current == "$(desired_net0 "$current")" ]] || changed=true
  current=$(config_value "$config" mp0 2>/dev/null || true)
  [[ $current == "$(desired_mp0 "$current")" ]] || changed=true
  current=$(config_value "$config" hookscript 2>/dev/null || true)
  [[ $current == local:snippets/zerofs-lxc-hook.sh ]] || changed=true
  if [[ $stage_smb == true ]]; then
    current=$(config_value "$config" features 2>/dev/null || true)
    [[ $current == "$(desired_features "$current")" ]] || changed=true
  fi
  [[ $changed == true ]] || { assert_ct_resources; return 0; }

  if ct_running; then
    graceful_stop
  fi
  ct_resources_mutated=true
  for key in cores memory swap onboot; do
    case $key in
      cores) desired=$cores ;;
      memory) desired=$memory_mb ;;
      swap) desired=0 ;;
      onboot) desired=1 ;;
    esac
    current=$(config_value "$config" "$key" 2>/dev/null || true)
    if [[ $current != "$desired" ]]; then
      run pct set "$ctid" "--$key" "$desired"
    fi
  done
  current=$(config_value "$config" startup 2>/dev/null || true)
  value=$(csv_set_field "$current" order 20)
  if [[ $current != "$value" ]]; then
    run pct set "$ctid" --startup "$value"
  fi
  current=$(config_value "$config" net0 2>/dev/null || true)
  value=$(desired_net0 "$current")
  if [[ $current != "$value" ]]; then
    run pct set "$ctid" --net0 "$value"
  fi
  current=$(config_value "$config" mp0 2>/dev/null || true)
  value=$(desired_mp0 "$current")
  if [[ $current != "$value" ]]; then
    run pct set "$ctid" --mp0 "$value"
  fi
  current=$(config_value "$config" hookscript 2>/dev/null || true)
  if [[ $current != local:snippets/zerofs-lxc-hook.sh ]]; then
    run pct set "$ctid" --hookscript local:snippets/zerofs-lxc-hook.sh
  fi
  if [[ $stage_smb == true ]]; then
    current=$(config_value "$config" features 2>/dev/null || true)
    value=$(desired_features "$current")
    if [[ $current != "$value" ]]; then
      run pct set "$ctid" --features "$value"
    fi
  fi
  assert_ct_resources
}

persist_host_transaction() {
  [[ $defer_commit == true ]] || return 0
  if [[ $dry_run == true ]]; then
    echo "+ persist host rollback transaction $deployment_transaction"
    return 0
  fi
  [[ ! -e $deployment_transaction ]] || {
    echo "deployment transaction already exists: $deployment_transaction" >&2
    return 1
  }
  local temporary_transaction="$state_root/.deployment-transaction.$$"
  [[ ! -e $temporary_transaction ]] || return 1
  install -d -m 0700 "$temporary_transaction"
  if [[ -n $ct_resource_snapshot ]]; then
    install -m 0600 "$ct_resource_snapshot" "$temporary_transaction/ct-resources"
  fi
  if [[ -n ${previous_config:-} ]]; then
    install -m 0600 "$previous_config" "$temporary_transaction/previous-config"
  fi
  if [[ -n ${previous_receipt:-} ]]; then
    install -m 0600 "$previous_receipt" "$temporary_transaction/previous-receipt"
  fi
  printf '%s\n' "$previous_release" >"$temporary_transaction/previous-release"
  {
    printf 'saved_had_ct=%q\n' "$had_ct"
    printf 'saved_had_running_ct=%q\n' "$had_running_ct"
    printf 'saved_prod_mount_was_active=%q\n' "$prod_mount_was_active"
    printf 'saved_prod_smb_was_active=%q\n' "$prod_smb_was_active"
    printf 'saved_prod_mount_was_enabled=%q\n' "$prod_mount_was_enabled"
    printf 'saved_prod_smb_was_enabled=%q\n' "$prod_smb_was_enabled"
    printf 'saved_release_id=%q\n' "$release_id"
  } >"$temporary_transaction/state.env"
  printf '%s\n' prepared >"$temporary_transaction/phase"
  chmod 0600 "$temporary_transaction/previous-release" \
    "$temporary_transaction/state.env" "$temporary_transaction/phase"
  sync -f "$temporary_transaction/previous-release"
  sync -f "$temporary_transaction/state.env"
  sync -f "$temporary_transaction/phase"
  [[ ! -e $temporary_transaction/ct-resources ]] || sync -f "$temporary_transaction/ct-resources"
  [[ ! -e $temporary_transaction/previous-config ]] || sync -f "$temporary_transaction/previous-config"
  [[ ! -e $temporary_transaction/previous-receipt ]] || sync -f "$temporary_transaction/previous-receipt"
  sync -f "$temporary_transaction"
  mv -- "$temporary_transaction" "$deployment_transaction"
  sync -f "$state_root"
}

set_host_transaction_phase() {
  [[ $defer_commit == true || -d $deployment_transaction ]] || return 0
  local phase=$1 temporary="$deployment_transaction/.phase.$$"
  if [[ $dry_run == true ]]; then
    echo "+ persist host deployment phase $phase"
    return 0
  fi
  printf '%s\n' "$phase" >"$temporary"
  chmod 0600 "$temporary"
  sync -f "$temporary"
  mv -f -- "$temporary" "$deployment_transaction/phase"
  sync -f "$deployment_transaction"
}

restore_saved_prod_services() {
  if [[ $saved_prod_mount_was_enabled == true ]]; then
    pct exec "$ctid" -- systemctl enable zerofs-lxc-mount.service
  else
    # shellcheck disable=SC2016
    pct exec "$ctid" -- sh -c \
      'test "$(systemctl show -p LoadState --value zerofs-lxc-mount.service)" = not-found || systemctl disable zerofs-lxc-mount.service'
  fi
  if [[ $saved_prod_smb_was_enabled == true ]]; then
    pct exec "$ctid" -- systemctl enable smbd.service
  else
    # shellcheck disable=SC2016
    pct exec "$ctid" -- sh -c \
      'test "$(systemctl show -p LoadState --value smbd.service)" = not-found || systemctl disable smbd.service'
  fi
  if [[ $saved_prod_mount_was_active == true ]]; then
    pct exec "$ctid" -- systemctl start zerofs-lxc-mount.service
  else
    # shellcheck disable=SC2016
    pct exec "$ctid" -- sh -c \
      'test "$(systemctl show -p LoadState --value zerofs-lxc-mount.service)" = not-found || systemctl stop zerofs-lxc-mount.service'
  fi
  if [[ $saved_prod_smb_was_active == true ]]; then
    pct exec "$ctid" -- systemctl start smbd.service
  else
    # shellcheck disable=SC2016
    pct exec "$ctid" -- sh -c \
      'test "$(systemctl show -p LoadState --value smbd.service)" = not-found || systemctl stop smbd.service'
  fi
}

saved_had_ct=false
saved_had_running_ct=false
saved_prod_mount_was_active=false
saved_prod_smb_was_active=false
saved_prod_mount_was_enabled=false
saved_prod_smb_was_enabled=false
saved_release_id=

wait_for_zerofs() {
  if [[ $dry_run == true ]]; then
    echo "+ wait up to 180s for private $role listeners and services"
    return 0
  fi
  local deadline=$((SECONDS + 180))
  until pct exec "$ctid" -- systemctl is-active --quiet zerofs-lxc.service \
    && curl --fail --silent --show-error --max-time 5 "http://$container_ip:9567/metrics" >/dev/null; do
    ((SECONDS < deadline)) || {
      pct exec "$ctid" -- journalctl -u zerofs-lxc.service --no-pager -n 100 >&2
      return 1
    }
    sleep 1
  done
}

assert_runtime_listeners() {
  local profile=$1 listeners
  if [[ $dry_run == true ]]; then
    if [[ $profile == maintenance ]]; then
      echo "+ prove maintenance NFS listener is private $container_ip:2049"
      echo "+ reject 9P, NBD, WebUI, SMB, wildcard, and public listeners"
    else
      echo "+ prove full private production listeners"
      echo "+ prove NFS listener is private $container_ip:2049"
      echo "+ prove 9P listener is private $container_ip:5564"
      echo "+ prove NBD listener is private $container_ip:10809"
      echo "+ prove WebUI listener is private $container_ip:8080 and rejects wildcard/public binds"
    fi
    return 0
  fi
  listeners=$(pct exec "$ctid" -- ss -H -lnt)
  grep -Fq "$container_ip:9567" <<<"$listeners"
  grep -Fq "$container_ip:2049" <<<"$listeners"
  if [[ $profile == maintenance ]]; then
    if grep -Eq '(^|[[:space:]])[^[:space:]]*:(5564|10809|8080|445)([[:space:]]|$)' <<<"$listeners"; then
      echo "non-NFS production listener escaped maintenance mode" >&2
      return 1
    fi
  else
    grep -Fq "$container_ip:5564" <<<"$listeners"
    grep -Fq "$container_ip:10809" <<<"$listeners"
    grep -Fq "$container_ip:8080" <<<"$listeners"
    if [[ $has_smb == true ]]; then
      grep -Fq "$container_ip:445" <<<"$listeners"
    elif grep -Eq "(^|[[:space:]])$container_ip:445([[:space:]]|$)" <<<"$listeners"; then
      echo "SMB remained active in NFS-only mode" >&2
      return 1
    fi
  fi
  if grep -Eq '(^|[[:space:]])(0\.0\.0\.0|\[::\]):(10809|9567|2049|5564|445|8080)([[:space:]]|$)' <<<"$listeners"; then
    echo "ZeroFS listener escaped the private container address" >&2
    return 1
  fi
}

control_host_transaction() {
  if [[ $dry_run == true ]]; then
    echo "+ $action host deployment transaction $deployment_transaction"
    if [[ $action == rollback ]]; then
      echo "+ restore previous release and exact CT resources before restarting services"
    elif [[ $action == promote ]]; then
      echo "+ prove full private production listeners"
    fi
    return 0
  fi
  if [[ ( $action == recover || $action == finalize ) && ! -d $deployment_transaction ]]; then
    return 0
  fi
  [[ -d $deployment_transaction ]] || {
    echo "missing deployment transaction: $deployment_transaction" >&2
    return 1
  }
  [[ -f $deployment_transaction/state.env && -f $deployment_transaction/previous-release && -f $deployment_transaction/phase ]] || {
    echo "incomplete deployment transaction: $deployment_transaction" >&2
    return 1
  }
  # The transaction is root-created mode 0700; shell quoting was applied when written.
  # shellcheck disable=SC1091
  source "$deployment_transaction/state.env"
  if [[ $action != recover && $action != finalize && $saved_release_id != "$release_id" ]]; then
    echo "deployment transaction belongs to release $saved_release_id" >&2
    return 1
  fi
  phase=$(<"$deployment_transaction/phase")
  if [[ $action == commit || $action == finalize ]]; then
    [[ $phase == activated ]] || {
      echo "deployment transaction is not activated: $phase" >&2
      return 1
    }
    rm -rf -- "$deployment_transaction"
    sync -f "$state_root"
    return 0
  fi
  if [[ $action == promote ]]; then
    [[ $phase == maintenance ]] || {
      echo "deployment transaction is not in maintenance mode: $phase" >&2
      return 1
    }
    [[ -f $stage/zerofs.toml ]] || {
      echo "promotion requires staged full zerofs.toml" >&2
      return 1
    }
    assert_prod_nfs_quiesced
    set_host_transaction_phase promoting
    [[ $saved_release_id =~ ^[0-9a-f]{12}-[A-Za-z0-9]{6,32}$ ]] || {
      echo "transaction release identifier is invalid" >&2
      return 1
    }
    promoted_config="$state_root/releases/$saved_release_id/zerofs.toml"
    [[ -d $state_root/releases/$saved_release_id && ! -L $state_root/releases/$saved_release_id ]] || return 1
    install -o 100000 -g 100000 -m 0600 "$stage/zerofs.toml" "$promoted_config"
    sync -f "$promoted_config"
    pct exec "$ctid" -- systemctl restart zerofs-lxc.service
    wait_for_zerofs
    if [[ $stage_smb == true ]]; then
      pct exec "$ctid" -- systemctl enable zerofs-lxc-mount.service smbd.service
      pct exec "$ctid" -- systemctl start zerofs-lxc-mount.service
      pct exec "$ctid" -- systemctl start smbd.service
      pct exec "$ctid" -- systemctl is-active --quiet zerofs-lxc-mount.service
      pct exec "$ctid" -- systemctl is-active --quiet smbd.service
    fi
    assert_runtime_listeners full
    config_sha=$(sha256sum "$stage/zerofs.toml" | awk '{print $1}')
    receipt="$state_root/receipts/$release_id"
    temporary_receipt="$receipt.promote.$$"
    awk -v value="$config_sha" '
      BEGIN { replaced=0 }
      /^config_sha256=/ { print "config_sha256=" value; replaced=1; next }
      { print }
      END { if (!replaced) print "config_sha256=" value }
    ' "$receipt" >"$temporary_receipt"
    chmod 0600 "$temporary_receipt"
    sync -f "$temporary_receipt"
    mv -f -- "$temporary_receipt" "$receipt"
    sync -f "$state_root/receipts"
    set_host_transaction_phase activated
    return 0
  fi
  action=rollback
  previous_release=$(<"$deployment_transaction/previous-release")
  if ct_exists && ct_running; then
    pct stop "$ctid" --skiplock 1
  fi
  if [[ -n $previous_release ]]; then
    if [[ -f $deployment_transaction/previous-config ]]; then
      is_canonical_release_target "$previous_release" || return 1
      install -o 100000 -g 100000 -m 0600 \
        "$deployment_transaction/previous-config" "$state_root/$previous_release/zerofs.toml"
      sync -f "$state_root/$previous_release/zerofs.toml"
    fi
    if [[ -f $deployment_transaction/previous-receipt ]]; then
      install -o 0 -g 0 -m 0600 "$deployment_transaction/previous-receipt" \
        "$state_root/receipts/${previous_release#releases/}"
      sync -f "$state_root/receipts/${previous_release#releases/}"
    fi
    ln -sfn "$previous_release" "$state_root/current"
  else
    rm -f -- "$state_root/current"
  fi
  if [[ $saved_had_ct == true ]]; then
    ct_resource_snapshot="$deployment_transaction/ct-resources"
    ct_resources_mutated=true
    restore_ct_resources
    if [[ $saved_had_running_ct == true ]]; then
      if ! {
        pct start "$ctid" &&
          pct exec "$ctid" -- systemctl restart zerofs-lxc.service &&
          restore_saved_prod_services
      }; then
        pct stop "$ctid" --skiplock 1 >/dev/null 2>&1 || true
        echo "host recovery compensation failed; CT stopped and transaction preserved" >&2
        return 125
      fi
    fi
  elif ct_exists; then
    pct stop "$ctid" --skiplock 1 >/dev/null 2>&1 || true
  fi
  rm -rf -- "$deployment_transaction"
  sync -f "$state_root"
}

is_canonical_release_target() {
  [[ $1 =~ ^releases/([0-9a-f]{8}|[0-9a-f]{12})-[A-Za-z0-9]{6,32}$ ]]
}

assert_server_drained() {
  ct_running || return 0
  if [[ $dry_run == true ]]; then
    echo "+ verify four stable metrics samples at http://$container_ip:9567/metrics: accepted=local=remote dirty_ram=dirty_ssd=0 terminal=0"
    return
  fi
  local names=(
    zerofs_writeback_accepted_sequence
    zerofs_writeback_local_sequence
    zerofs_writeback_remote_sequence
    zerofs_writeback_dirty_ram_bytes
    zerofs_writeback_dirty_ssd_reserved_bytes
    zerofs_writeback_terminal_error
  )
  if [[ $role == dev ]]; then
    names+=(
      zerofs_nbd_volatile_memory_dirty_bytes
      zerofs_nbd_volatile_memory_dirty_operations
      zerofs_nbd_volatile_memory_terminal
    )
  fi
  local body name stable=0
  local deadline=$((SECONDS + drain_timeout))
  while ((SECONDS < deadline)); do
    body=$(curl --fail --silent --show-error --max-time 10 "http://$container_ip:9567/metrics")
    declare -A value=()
    for name in "${names[@]}"; do
      value[$name]=$(awk -v wanted="$name" '$1 == wanted { print int($2); found=1 } END { if (!found) exit 1 }' <<<"$body") || {
        echo "missing required drain metric: $name" >&2
        return 1
      }
    done
    terminal=false
    [[ ${value[zerofs_writeback_terminal_error]} == 0 ]] || terminal=true
    if [[ $role == dev ]] && [[ ${value[zerofs_nbd_volatile_memory_terminal]} != 0 ]]; then
      terminal=true
    fi
    if [[ $terminal == true ]]; then
      echo "ZeroFS reported a terminal error; refusing container lifecycle mutation" >&2
      return 1
    fi
    drained=true
    [[ ${value[zerofs_writeback_accepted_sequence]} == "${value[zerofs_writeback_local_sequence]}" ]] || drained=false
    [[ ${value[zerofs_writeback_dirty_ram_bytes]} == 0 ]] || drained=false
    if [[ $local_durable_upgrade == false ]]; then
      [[ ${value[zerofs_writeback_local_sequence]} == "${value[zerofs_writeback_remote_sequence]}" ]] || drained=false
      [[ ${value[zerofs_writeback_dirty_ssd_reserved_bytes]} == 0 ]] || drained=false
    fi
    if [[ $role == dev ]]; then
      [[ ${value[zerofs_nbd_volatile_memory_dirty_bytes]} == 0 ]] || drained=false
      [[ ${value[zerofs_nbd_volatile_memory_dirty_operations]} == 0 ]] || drained=false
    fi
    if [[ $drained == true ]]; then
      stable=$((stable + 1))
      ((stable >= 4)) && return 0
    else
      stable=0
    fi
    sleep 1
  done
  echo "ZeroFS did not fully drain within 1800 seconds" >&2
  return 1
}

prod_mount_was_active=false
prod_smb_was_active=false
prod_mount_was_enabled=false
prod_smb_was_enabled=false
capture_prod_share_state() {
  [[ $role == prod ]] || return 0
  ct_exists && ct_running || return 0
  if [[ $dry_run == true ]]; then
    echo "+ capture prior optional SMB/FUSE service state"
    return 0
  fi
  if pct exec "$ctid" -- systemctl is-enabled --quiet smbd.service; then
    prod_smb_was_enabled=true
  fi
  if pct exec "$ctid" -- systemctl is-enabled --quiet zerofs-lxc-mount.service; then
    prod_mount_was_enabled=true
  fi
  if pct exec "$ctid" -- systemctl is-active --quiet smbd.service; then
    prod_smb_was_active=true
  fi
  if pct exec "$ctid" -- systemctl is-active --quiet zerofs-lxc-mount.service; then
    prod_mount_was_active=true
  fi
}

quiesce_prod_share() {
  [[ $role == prod ]] || return 0
  ct_exists || return 0
  ct_running || return 0
  if [[ $dry_run == false ]] && ! pct exec "$ctid" -- systemctl is-active --quiet zerofs-lxc.service; then
    return 0
  fi
  if [[ $dry_run == true ]]; then
    if [[ $has_smb == true ]]; then
      run pct exec "$ctid" -- systemctl stop smbd.service
      run pct exec "$ctid" -- sync -f /srv/zerofs-share
      run pct exec "$ctid" -- systemctl stop zerofs-lxc-mount.service
    else
      echo "+ inspect and quiesce any previously active optional SMB/FUSE share"
    fi
    return
  fi
  if [[ $prod_smb_was_active == true ]]; then
    run pct exec "$ctid" -- systemctl stop smbd.service
  fi
  if [[ $prod_mount_was_active == true ]]; then
    run pct exec "$ctid" -- sync -f /srv/zerofs-share
    run pct exec "$ctid" -- systemctl stop zerofs-lxc-mount.service
  fi
}

assert_prod_nfs_quiesced() {
  [[ $role == prod ]] || return 0
  ct_running || return 0
  if [[ $dry_run == true ]]; then
    echo "+ prove no established NFS clients remain on $container_ip:2049"
    return
  fi
  local deadline=$((SECONDS + 60)) stable=0
  while ((SECONDS < deadline)); do
    if pct exec "$ctid" -- ss -Hnt state established sport = :2049 | grep -q .; then
      stable=0
    else
      stable=$((stable + 1))
      ((stable >= 2)) && return 0
    fi
    sleep 1
  done
  echo "active NFS client remains after 60 seconds; unmount every client before production deploy" >&2
  return 1
}

graceful_stop() {
  ct_exists || return 0
  if ct_running; then
    run pct shutdown "$ctid" --timeout 120
  fi
}

if [[ $action == promote || $action == finalize || $action == commit || $action == rollback || $action == recover ]]; then
  control_host_transaction
  exit 0
fi

if [[ $action == cleanup ]]; then
  if ct_exists; then
    assert_server_drained
    graceful_stop
    run pct destroy "$ctid" --purge 1
  fi
  run rm -f -- "/etc/zerofs-lxc/$ctid.conf"
  echo "preserved_state=$state_root"
  exit 0
fi

if [[ $dry_run == false ]]; then
  for required in "$stage/zerofs" "$stage/zerofs.toml" "$stage/zerofs-lxc.service" "$stage/zerofs-lxc-hook.sh"; do
    [[ -f $required ]] || { echo "missing staged asset: $required" >&2; exit 1; }
  done
  if [[ $stage_smb == true ]]; then
    for required in "$stage/zerofs-lxc-mount.service" "$stage/smb.conf" "$stage/samba-password"; do
      [[ -f $required ]] || { echo "missing staged prod asset: $required" >&2; exit 1; }
    done
  fi
  [[ $(sha256sum "$stage/zerofs" | awk '{print $1}') == "$binary_sha" ]] || {
    echo "staged binary SHA-256 mismatch" >&2
    exit 1
  }
fi

had_ct=false
had_running_ct=false
if ct_exists; then
  had_ct=true
  if ct_running; then
    had_running_ct=true
  fi
fi
previous_release=
previous_config=
previous_receipt=
if [[ $dry_run == false && -L $state_root/current ]]; then
  previous_release=$(readlink "$state_root/current")
  is_canonical_release_target "$previous_release" || {
    echo "current release symlink is not canonical: $previous_release" >&2
    exit 1
  }
  previous_config="$state_root/$previous_release/zerofs.toml"
  [[ -f $previous_config && ! -L $state_root/$previous_release ]] || {
    echo "current release config is not a canonical regular file" >&2
    exit 1
  }
  previous_receipt="$state_root/receipts/${previous_release#releases/}"
  [[ -f $previous_receipt && ! -L $previous_receipt ]] || {
    echo "current release receipt is not a canonical regular file" >&2
    exit 1
  }
fi
if [[ $local_durable_upgrade == true ]]; then
  if [[ $dry_run == true ]]; then
    echo "+ verify the existing and staged releases use the unchanged persistent writeback directory"
  else
    [[ -n $previous_config ]] || {
      echo "local-durable upgrade requires an existing canonical release" >&2
      exit 1
    }
    previous_writeback_dir=$(writeback_dir_from_config "$previous_config") || {
      echo "existing release has no unambiguous [writeback] dir" >&2
      exit 1
    }
    staged_writeback_dir=$(writeback_dir_from_config "$stage/zerofs.toml") || {
      echo "staged release has no unambiguous [writeback] dir" >&2
      exit 1
    }
    [[ $previous_writeback_dir == /srv/zerofs-persist/state/writeback \
      && $staged_writeback_dir == "$previous_writeback_dir" ]] || {
      echo "local-durable upgrade requires the unchanged persistent writeback directory" >&2
      exit 1
    }
  fi
fi
capture_ct_resources
rollback_backup=

rollback() {
  local code=$? rollback_failed=false
  trap - ERR
  set +e
  rollback_try() {
    if ! "$@"; then
      echo "rollback step failed: $*" >&2
      rollback_failed=true
    fi
  }
  echo "rollback: deployment failed; restoring the last runnable container/release" >&2
  if [[ -n $previous_release ]]; then
    if [[ -f $deployment_transaction/previous-config ]]; then
      rollback_try install -o 100000 -g 100000 -m 0600 \
        "$deployment_transaction/previous-config" "$state_root/$previous_release/zerofs.toml"
      rollback_try sync -f "$state_root/$previous_release/zerofs.toml"
    fi
    if [[ -f $deployment_transaction/previous-receipt ]]; then
      rollback_try install -o 0 -g 0 -m 0600 \
        "$deployment_transaction/previous-receipt" \
        "$state_root/receipts/${previous_release#releases/}"
      rollback_try sync -f "$state_root/receipts/${previous_release#releases/}"
    fi
    rollback_try ln -sfn "$previous_release" "$state_root/current"
  fi
  if [[ $ct_resources_mutated == true ]] && pct config "$ctid" >/dev/null 2>&1 && ct_running; then
    rollback_try pct stop "$ctid" --skiplock 1 >/dev/null 2>&1
  fi
  if ! restore_ct_resources; then
    echo "rollback failed to verify original CT resources; leaving CT stopped and preserving $ct_resource_snapshot" >&2
    rollback_failed=true
  fi
  if [[ $rollback_failed == false && -n $rollback_backup ]]; then
    rollback_try pct stop "$ctid" --skiplock 1 >/dev/null 2>&1
    rollback_try pct destroy "$ctid" --purge 1 >/dev/null 2>&1
    rollback_try pct restore "$ctid" "$rollback_backup" --force 1
    [[ $rollback_failed == true ]] || rollback_try pct start "$ctid"
  elif [[ $had_ct == true && $had_running_ct == true ]] && pct config "$ctid" >/dev/null 2>&1; then
    if [[ $rollback_failed == false ]]; then
      rollback_try pct start "$ctid" >/dev/null 2>&1
      [[ $rollback_failed == true ]] || \
        rollback_try pct exec "$ctid" -- systemctl restart zerofs-lxc.service >/dev/null 2>&1
    fi
    if [[ $role == prod && $rollback_failed == false ]]; then
      if [[ $prod_mount_was_enabled == true ]]; then
        rollback_try pct exec "$ctid" -- systemctl enable zerofs-lxc-mount.service >/dev/null 2>&1
      else
        rollback_try pct exec "$ctid" -- systemctl disable zerofs-lxc-mount.service >/dev/null 2>&1
      fi
      if [[ $prod_smb_was_enabled == true ]]; then
        rollback_try pct exec "$ctid" -- systemctl enable smbd.service >/dev/null 2>&1
      else
        rollback_try pct exec "$ctid" -- systemctl disable smbd.service >/dev/null 2>&1
      fi
      if [[ $prod_mount_was_active == true ]]; then
        rollback_try pct exec "$ctid" -- systemctl start zerofs-lxc-mount.service >/dev/null 2>&1
      else
        rollback_try pct exec "$ctid" -- systemctl stop zerofs-lxc-mount.service >/dev/null 2>&1
      fi
      if [[ $prod_smb_was_active == true ]]; then
        rollback_try pct exec "$ctid" -- systemctl start smbd.service >/dev/null 2>&1
      else
        rollback_try pct exec "$ctid" -- systemctl stop smbd.service >/dev/null 2>&1
      fi
    fi
  elif pct config "$ctid" >/dev/null 2>&1; then
    rollback_try pct stop "$ctid" --skiplock 1 >/dev/null 2>&1
  fi
  if [[ $rollback_failed == true ]]; then
    pct stop "$ctid" --skiplock 1 >/dev/null 2>&1 || true
    echo "rollback compensation incomplete; CT left stopped and recovery transaction preserved" >&2
    exit 125
  fi
  if [[ $defer_commit == true && -d $deployment_transaction ]]; then
    rollback_try rm -rf -- "$deployment_transaction"
    rollback_try sync -f "$state_root"
  fi
  [[ -z $ct_resource_snapshot ]] || rollback_try rm -f -- "$ct_resource_snapshot"
  if [[ $rollback_failed == true ]]; then
    echo "rollback cleanup incomplete; recovery artifacts preserved" >&2
    exit 125
  fi
  exit "$code"
}
trap rollback ERR

capture_prod_share_state
persist_host_transaction

if [[ $had_ct == true ]]; then
  quiesce_prod_share
  assert_prod_nfs_quiesced
  assert_server_drained
  assert_prod_nfs_quiesced
  if [[ $role == prod ]] && ct_running; then
    run pct exec "$ctid" -- systemctl stop zerofs-lxc.service
  fi
fi
set_host_transaction_phase quiesced

reconcile_ct_resources

if [[ $dry_run == false ]]; then
  marker="$state_root/.zerofs-lxc-state"
  expected_marker="$role:$ctid:$namespace_id"
  if [[ -e $marker && $(<"$marker") != "$expected_marker" ]]; then
    echo "persistent state marker belongs to another CTID" >&2
    exit 1
  fi
  printf '%s\n' "$expected_marker" >"$marker"
fi
run install -d -o 0 -g 0 -m 0755 "$state_root/releases" "$state_root/receipts"
run install -d -o 0 -g 0 -m 0700 "$state_root/rollback"
run install -d -o 100000 -g 100000 -m 0750 "$state_root/state" "$state_root/cache"
if [[ $role == dev ]]; then
  run install -d -o 100000 -g 100000 -m 0750 "$state_root/backend-dev"
fi
release="$state_root/releases/$release_id"
run install -d -o 0 -g 0 -m 0755 "$release"
run install -m 0755 "$stage/zerofs" "$release/zerofs"
run install -o 100000 -g 100000 -m 0600 "$stage/zerofs.toml" "$release/zerofs.toml"
if [[ $dry_run == false && -f $stage/zerofs.env ]]; then
  run install -o 100000 -g 100000 -m 0600 "$stage/zerofs.env" "$release/zerofs.env"
fi
if [[ $dry_run == false && -f $stage/storage-key ]]; then
  run install -o 100000 -g 100000 -m 0600 "$stage/storage-key" "$release/storage-key"
fi
if [[ $dry_run == false && -f $stage/known_hosts ]]; then
  run install -o 100000 -g 100000 -m 0600 "$stage/known_hosts" "$release/known_hosts"
fi
if [[ $dry_run == false ]]; then
  config_sha=$(sha256sum "$stage/zerofs.toml" | awk '{print $1}')
  printf 'commit=%s\nrelease_id=%s\nbinary_sha256=%s\nconfig_sha256=%s\nhotpath_profile=%s\n' "$commit" "$release_id" "$binary_sha" "$config_sha" "$hotpath_profile" >"$state_root/receipts/$release_id"
fi
run ln -sfn "releases/$release_id" "$state_root/current"

run install -d -m 0755 /etc/zerofs-lxc /var/lib/vz/snippets
if [[ $dry_run == true ]]; then
  echo "+ namespace collision guard /etc/zerofs-lxc/namespaces/$namespace_id -> $role:$ctid:$state_root"
fi
if [[ $dry_run == false ]]; then
  install -d -m 0755 /etc/zerofs-lxc/namespaces
  registry="/etc/zerofs-lxc/namespaces/$namespace_id"
  registry_value="$role:$ctid:$state_root"
  if [[ -e $registry && $(<"$registry") != "$registry_value" ]]; then
    echo "storage namespace collision with $(<"$registry")" >&2
    exit 1
  fi
  printf '%s\n' "$registry_value" >"$registry"
  printf 'ZEROFS_LXC_STATE_ROOT=%q\nZEROFS_LXC_STATE_MARKER=%q\n' \
    "$state_root" "$role:$ctid:$namespace_id" >"/etc/zerofs-lxc/$ctid.conf"
fi
run install -m 0755 "$stage/zerofs-lxc-hook.sh" /var/lib/vz/snippets/zerofs-lxc-hook.sh

if [[ $action == replace && $had_ct == true ]]; then
  run install -d -m 0700 "$state_root/rollback"
  run vzdump "$ctid" --dumpdir "$state_root/rollback" --mode stop --compress zstd
  if [[ $dry_run == false ]]; then
    rollback_backup=$(find "$state_root/rollback" -maxdepth 1 -type f -name "vzdump-lxc-$ctid-*.tar.zst" -print0 | xargs -0 ls -1t | head -n 1)
    [[ -n $rollback_backup ]]
  else
    echo "+ rollback backup retained under $state_root/rollback"
  fi
  graceful_stop
  run pct destroy "$ctid" --purge 1
  if [[ $dry_run == true ]]; then
    dry_run_ct_destroyed=true
  fi
  had_ct=false
fi

if ! ct_exists; then
  run pct create "$ctid" "$template" \
    --hostname "zerofs-$ctid" \
    --unprivileged 1 \
    --cores "$cores" \
    --memory "$memory_mb" \
    --swap 0 \
    --rootfs "$rootfs" \
    --net0 "name=eth0,bridge=$bridge,ip=$container_ip/24,gw=$gateway,type=veth" \
    --mp0 "$state_root,mp=/srv/zerofs-persist" \
    --onboot 1 \
    --startup order=20
fi
if [[ $dry_run == true ]]; then
  current=
else
  current=$(pct config "$ctid" | awk -F ': ' '$1 == "hookscript" { sub(/^[^:]*: /, ""); print; exit }')
fi
if [[ $current != local:snippets/zerofs-lxc-hook.sh ]]; then
  run pct set "$ctid" --hookscript local:snippets/zerofs-lxc-hook.sh
fi
if [[ $stage_smb == true ]]; then
  if [[ $dry_run == true ]]; then
    current=
  else
    current=$(pct config "$ctid" | awk -F ': ' '$1 == "features" { sub(/^[^:]*: /, ""); print; exit }')
  fi
  feature_value=$(desired_features "$current")
  if [[ $current != "$feature_value" ]]; then
    run pct set "$ctid" --features "$feature_value"
  fi
fi
assert_ct_resources
if ! ct_running; then
  run pct start "$ctid"
fi

run pct exec "$ctid" -- apt-get update
packages=(ca-certificates curl iproute2)
if [[ $stage_smb == true ]]; then
  packages+=(fuse3 samba)
fi
run pct exec "$ctid" -- env DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends "${packages[@]}"
run pct push "$ctid" "$stage/zerofs-lxc.service" /etc/systemd/system/zerofs-lxc.service --perms 0644
if [[ $stage_smb == true ]]; then
  run pct exec "$ctid" -- sh -c "grep -qxF user_allow_other /etc/fuse.conf || printf '%s\\n' user_allow_other >>/etc/fuse.conf"
  run pct push "$ctid" "$stage/zerofs-lxc-mount.service" /etc/systemd/system/zerofs-lxc-mount.service --perms 0644
  if [[ $dry_run == false ]]; then
    sed -e "s/@@CONTAINER_IP@@/$container_ip/g" -e "s/@@SAMBA_USER@@/$samba_user/g" "$stage/smb.conf" >"$stage/smb.conf.rendered"
  else
    echo "+ render private Samba config for $container_ip user $samba_user"
  fi
  run pct push "$ctid" "$stage/smb.conf.rendered" /etc/samba/smb.conf --perms 0644
  run pct exec "$ctid" -- sh -c "id -u '$samba_user' >/dev/null 2>&1 || useradd --system --home /nonexistent --shell /usr/sbin/nologin '$samba_user'"
  if [[ $dry_run == false ]]; then
    [[ -s $stage/samba-password ]] || { echo "prod requires staged samba-password" >&2; false; }
    samba_password=$(<"$stage/samba-password")
    printf '%s\n%s\n' "$samba_password" "$samba_password" | pct exec "$ctid" -- smbpasswd -s -a "$samba_user"
  else
    echo "+ install Samba credential from staged password file without logging it"
  fi
fi
run pct exec "$ctid" -- systemctl daemon-reload
run pct exec "$ctid" -- systemctl enable zerofs-lxc.service
run pct exec "$ctid" -- systemctl restart zerofs-lxc.service

wait_for_zerofs
run pct exec "$ctid" -- systemctl is-active --quiet zerofs-lxc.service
if [[ $has_smb == true ]]; then
  run pct exec "$ctid" -- systemctl enable zerofs-lxc-mount.service smbd.service
  run pct exec "$ctid" -- systemctl start zerofs-lxc-mount.service
  run pct exec "$ctid" -- systemctl start smbd.service
  run pct exec "$ctid" -- systemctl is-active --quiet zerofs-lxc-mount.service
  run pct exec "$ctid" -- systemctl is-active --quiet smbd.service
elif [[ $role == prod && $dry_run == false ]]; then
  # A prior `both` rollout may have left these units installed. Native-NFS mode
  # must not reactivate the optional SMB/FUSE layer on the next CT boot.
  pct exec "$ctid" -- sh -c \
    'systemctl disable --now smbd.service zerofs-lxc-mount.service >/dev/null 2>&1 || true'
fi
if [[ $role == prod ]]; then
  if [[ $maintenance_nfs_only == true ]]; then
    assert_runtime_listeners maintenance
  else
    assert_runtime_listeners full
  fi
elif [[ $dry_run == false ]]; then
  listeners=$(pct exec "$ctid" -- ss -H -lnt)
  grep -Fq "$container_ip:9567" <<<"$listeners"
  grep -Fq "$container_ip:10809" <<<"$listeners"
  if grep -Eq '(^|[[:space:]])(0\.0\.0\.0|\[::\]):(10809|9567)([[:space:]]|$)' <<<"$listeners"; then
    echo "ZeroFS listener escaped the private container address" >&2
    false
  fi
fi
if [[ $dry_run == false ]]; then
  running_sha=$(pct exec "$ctid" -- sha256sum /srv/zerofs-persist/current/zerofs | awk '{print $1}')
  [[ $running_sha == "$binary_sha" ]]
fi
echo "deployed_ctid=$ctid"
echo "deployed_commit=$commit"
echo "binary_sha256=$binary_sha"
echo "persistent_state=$state_root"
if [[ $defer_commit == true ]]; then
  if [[ $maintenance_nfs_only == true ]]; then
    set_host_transaction_phase maintenance
  else
    set_host_transaction_phase activated
  fi
  echo "deferred_transaction=$deployment_transaction"
fi
[[ -z $ct_resource_snapshot ]] || rm -f -- "$ct_resource_snapshot"
trap - ERR
