#!/usr/bin/env bash
set -Eeuo pipefail

usage() {
  cat <<'EOF'
Usage: host-deploy.sh deploy|replace|cleanup [options]

This script runs on a Proxmox VE host. It never removes the persistent state
bind mount. `replace` additionally requires --confirm-replace CTID.
EOF
}

action=${1:-}
case "$action" in
  deploy|replace|cleanup) shift ;;
  *) usage >&2; exit 2 ;;
esac

ctid=
role=
container_ip=
bridge=vmbr1
gateway=10.10.10.1
template=
rootfs=local-lvm:8
memory_mb=65536
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
    --dry-run) dry_run=true; shift ;;
    *) echo "unknown argument: $1" >&2; exit 2 ;;
  esac
done

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
has_smb=false
if [[ $role == prod && ( $prod_access == smb || $prod_access == both ) ]]; then
  has_smb=true
fi
[[ $samba_user =~ ^[a-z_][a-z0-9_-]{0,31}$ ]] || {
  echo "unsafe Samba user name" >&2
  exit 2
}
if [[ $role == prod && ( $action == replace || $action == cleanup ) ]]; then
  echo "prod permits in-place deploy only; replace and cleanup are dev-only" >&2
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

run() {
  printf '+ '
  printf '%s ' "$@"
  printf '\n'
  if [[ $dry_run == false ]]; then
    "$@"
  fi
}

if [[ $dry_run == false ]]; then
  [[ $EUID -eq 0 ]] || { echo "host deployment requires root" >&2; exit 1; }
  command -v pct >/dev/null
  command -v pveversion >/dev/null
  pveversion >/dev/null
fi

ct_exists() {
  if [[ $dry_run == true ]]; then
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
  local deadline=$((SECONDS + 1800))
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
    [[ ${value[zerofs_writeback_local_sequence]} == "${value[zerofs_writeback_remote_sequence]}" ]] || drained=false
    [[ ${value[zerofs_writeback_dirty_ram_bytes]} == 0 ]] || drained=false
    [[ ${value[zerofs_writeback_dirty_ssd_reserved_bytes]} == 0 ]] || drained=false
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
  if pct exec "$ctid" -- systemctl is-enabled --quiet smbd.service; then
    prod_smb_was_enabled=true
  fi
  if pct exec "$ctid" -- systemctl is-enabled --quiet zerofs-lxc-mount.service; then
    prod_mount_was_enabled=true
  fi
  if pct exec "$ctid" -- systemctl is-active --quiet smbd.service; then
    prod_smb_was_active=true
    run pct exec "$ctid" -- systemctl stop smbd.service
  fi
  if pct exec "$ctid" -- systemctl is-active --quiet zerofs-lxc-mount.service; then
    prod_mount_was_active=true
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
  if pct exec "$ctid" -- ss -Hnt state established sport = :2049 | grep -q .; then
    echo "active NFS client remains; unmount every client before production deploy" >&2
    return 1
  fi
}

graceful_stop() {
  ct_exists || return 0
  if ct_running; then
    run pct shutdown "$ctid" --timeout 120
  fi
}

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
  if [[ $has_smb == true ]]; then
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
if [[ $dry_run == false && -L $state_root/current ]]; then
  previous_release=$(readlink "$state_root/current")
fi
rollback_backup=

rollback() {
  local code=$?
  trap - ERR
  set +e
  echo "rollback: deployment failed; restoring the last runnable container/release" >&2
  if [[ -n $previous_release ]]; then
    ln -sfn "$previous_release" "$state_root/current"
  fi
  if [[ -n $rollback_backup ]]; then
    pct stop "$ctid" --skiplock 1 >/dev/null 2>&1
    pct destroy "$ctid" --purge 1 >/dev/null 2>&1
    pct restore "$ctid" "$rollback_backup" --force 1
    pct start "$ctid"
  elif [[ $had_ct == true && $had_running_ct == true ]] && pct config "$ctid" >/dev/null 2>&1; then
    pct start "$ctid" >/dev/null 2>&1
    pct exec "$ctid" -- systemctl restart zerofs-lxc.service >/dev/null 2>&1
    if [[ $role == prod ]]; then
      if [[ $prod_mount_was_enabled == true ]]; then
        pct exec "$ctid" -- systemctl enable zerofs-lxc-mount.service >/dev/null 2>&1
      else
        pct exec "$ctid" -- systemctl disable zerofs-lxc-mount.service >/dev/null 2>&1
      fi
      if [[ $prod_smb_was_enabled == true ]]; then
        pct exec "$ctid" -- systemctl enable smbd.service >/dev/null 2>&1
      else
        pct exec "$ctid" -- systemctl disable smbd.service >/dev/null 2>&1
      fi
      if [[ $prod_mount_was_active == true ]]; then
        pct exec "$ctid" -- systemctl start zerofs-lxc-mount.service >/dev/null 2>&1
      else
        pct exec "$ctid" -- systemctl stop zerofs-lxc-mount.service >/dev/null 2>&1
      fi
      if [[ $prod_smb_was_active == true ]]; then
        pct exec "$ctid" -- systemctl start smbd.service >/dev/null 2>&1
      else
        pct exec "$ctid" -- systemctl stop smbd.service >/dev/null 2>&1
      fi
    fi
  fi
  exit "$code"
}
trap rollback ERR

if [[ $had_ct == true ]]; then
  quiesce_prod_share
  assert_prod_nfs_quiesced
  assert_server_drained
  assert_prod_nfs_quiesced
  if [[ $role == prod ]] && ct_running; then
    run pct exec "$ctid" -- systemctl stop zerofs-lxc.service
  fi
fi

run install -d -o 100000 -g 100000 -m 0750 "$state_root"
if [[ $dry_run == false ]]; then
  marker="$state_root/.zerofs-lxc-state"
  expected_marker="$role:$ctid:$namespace_id"
  if [[ -e $marker && $(<"$marker") != "$expected_marker" ]]; then
    echo "persistent state marker belongs to another CTID" >&2
    exit 1
  fi
  printf '%s\n' "$expected_marker" >"$marker"
fi
run install -d -m 0755 "$state_root/releases" "$state_root/receipts" "$state_root/rollback"
run install -d -o 100000 -g 100000 -m 0750 "$state_root/state" "$state_root/cache"
if [[ $role == dev ]]; then
  run install -d -o 100000 -g 100000 -m 0750 "$state_root/backend-dev"
fi
release="$state_root/releases/$release_id"
run install -d -m 0755 "$release"
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
  printf 'commit=%s\nrelease_id=%s\nbinary_sha256=%s\nconfig_sha256=%s\n' "$commit" "$release_id" "$binary_sha" "$config_sha" >"$state_root/receipts/$release_id"
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
run pct set "$ctid" --hookscript local:snippets/zerofs-lxc-hook.sh
if [[ $has_smb == true ]]; then
  run pct set "$ctid" --features fuse=1
fi
if ! ct_running; then
  run pct start "$ctid"
fi

run pct exec "$ctid" -- apt-get update
packages=(ca-certificates curl iproute2)
if [[ $has_smb == true ]]; then
  packages+=(fuse3 samba)
fi
run pct exec "$ctid" -- env DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends "${packages[@]}"
run pct push "$ctid" "$stage/zerofs-lxc.service" /etc/systemd/system/zerofs-lxc.service --perms 0644
if [[ $has_smb == true ]]; then
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

if [[ $dry_run == true ]]; then
  echo "+ wait up to 180s for private $role listeners and services"
  if [[ $role == prod ]]; then
    echo "+ prove NFS listener is private $container_ip:2049"
    echo "+ prove WebUI listener is private $container_ip:8080 and rejects wildcard/public binds"
  fi
else
  deadline=$((SECONDS + 180))
  until pct exec "$ctid" -- systemctl is-active --quiet zerofs-lxc.service \
    && curl --fail --silent --show-error --max-time 5 "http://$container_ip:9567/metrics" >/dev/null; do
    ((SECONDS < deadline)) || {
      pct exec "$ctid" -- journalctl -u zerofs-lxc.service --no-pager -n 100 >&2
      false
    }
    sleep 1
  done
fi
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
if [[ $dry_run == false ]]; then
  listeners=$(pct exec "$ctid" -- ss -H -lnt)
  grep -Fq "$container_ip:9567" <<<"$listeners"
  if [[ $role == dev ]]; then
    grep -Fq "$container_ip:10809" <<<"$listeners"
  else
    grep -Fq "$container_ip:2049" <<<"$listeners"
    grep -Fq "$container_ip:8080" <<<"$listeners"
    if [[ $has_smb == true ]]; then
      grep -Fq "$container_ip:445" <<<"$listeners"
    elif grep -Eq "(^|[[:space:]])$container_ip:445([[:space:]]|$)" <<<"$listeners"; then
      echo "SMB remained active in NFS-only mode" >&2
      false
    fi
  fi
  if grep -Eq '(^|[[:space:]])(0\.0\.0\.0|\[::\]):(10809|9567|2049|445|8080)([[:space:]]|$)' <<<"$listeners"; then
    echo "ZeroFS listener escaped the private container address" >&2
    false
  fi
  running_sha=$(pct exec "$ctid" -- sha256sum /srv/zerofs-persist/current/zerofs | awk '{print $1}')
  [[ $running_sha == "$binary_sha" ]]
fi
echo "deployed_ctid=$ctid"
echo "deployed_commit=$commit"
echo "binary_sha256=$binary_sha"
echo "persistent_state=$state_root"
trap - ERR
