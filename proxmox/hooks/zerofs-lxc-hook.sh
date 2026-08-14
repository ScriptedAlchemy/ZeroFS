#!/usr/bin/env bash
set -euo pipefail

ctid=${1:?Proxmox hook requires a CTID}
phase=${2:?Proxmox hook requires a lifecycle phase}
config="/etc/zerofs-lxc/${ctid}.conf"

case "$phase" in
  pre-start)
    test -r "$config" || {
      echo "missing ZeroFS LXC lifecycle config: $config" >&2
      exit 1
    }
    # This file is written only by host-deploy.sh and contains shell-quoted data.
    # shellcheck disable=SC1090
    source "$config"
    expected="/var/lib/zerofs-lxc/${ctid}"
    test "${ZEROFS_LXC_STATE_ROOT:-}" = "$expected" || {
      echo "unexpected ZeroFS persistent state root" >&2
      exit 1
    }
    test -f "$expected/.zerofs-lxc-state" || {
      echo "persistent state marker is missing: $expected/.zerofs-lxc-state" >&2
      exit 1
    }
    test "$(<"$expected/.zerofs-lxc-state")" = "${ZEROFS_LXC_STATE_MARKER:-}" || {
      echo "persistent state marker does not match CTID $ctid" >&2
      exit 1
    }
    test -d "$expected/state" -a -d "$expected/cache" -a -d "$expected/releases"
    ;;
  post-start|pre-stop|post-stop)
    ;;
  *)
    echo "unknown Proxmox hook phase: $phase" >&2
    exit 1
    ;;
esac
