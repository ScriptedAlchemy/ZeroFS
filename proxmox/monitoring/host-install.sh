#!/usr/bin/env bash
set -Eeuo pipefail

usage() {
  cat <<'EOF'
Usage: host-install.sh --ctid ID --monitoring-ip IP --zerofs-ip IP --stage PATH [--dry-run]

Runs on a Proxmox host. It changes only named Prometheus/Grafana files in an
existing monitoring CT, provisions Prometheus when absent, backs up changed
files, validates both services, and rolls back the exact files on any error.
EOF
}

ctid=
monitoring_ip=
zerofs_ip=
stage=
dry_run=false

while (($#)); do
  case "$1" in
    --ctid) ctid=$2; shift 2 ;;
    --monitoring-ip) monitoring_ip=$2; shift 2 ;;
    --zerofs-ip) zerofs_ip=$2; shift 2 ;;
    --stage) stage=$2; shift 2 ;;
    --dry-run) dry_run=true; shift ;;
    -h|--help) usage; exit 0 ;;
    *) echo "unknown argument: $1" >&2; exit 2 ;;
  esac
done

private_ipv4='^10\.[0-9]+\.[0-9]+\.[0-9]+$|^172\.(1[6-9]|2[0-9]|3[01])\.[0-9]+\.[0-9]+$|^192\.168\.[0-9]+\.[0-9]+$'
[[ $ctid =~ ^[1-9][0-9]{2,8}$ ]] || { echo "invalid --ctid" >&2; exit 2; }
[[ $monitoring_ip =~ $private_ipv4 ]] || { echo "monitoring IP must be RFC1918" >&2; exit 2; }
[[ $zerofs_ip =~ $private_ipv4 ]] || { echo "ZeroFS IP must be RFC1918" >&2; exit 2; }
[[ $monitoring_ip != "$zerofs_ip" ]] || { echo "monitoring and ZeroFS IPs must differ" >&2; exit 2; }
[[ $stage == /var/tmp/zerofs-monitoring-"$ctid" ]] || {
  echo "stage must be exactly /var/tmp/zerofs-monitoring-$ctid" >&2
  exit 2
}
grafana_health_url="http://$monitoring_ip:3000/api/health"

assets=(
  zerofs-scrape.yml
  zerofs-new-prometheus.yml
  zerofs-prometheus.yml
  zerofs-dashboard.yml
  zerofs-overview.json
  zerofs-prometheus-default
  merge-prometheus-config.py
)
if [[ $dry_run == false ]]; then
  [[ $EUID -eq 0 ]] || { echo "host installation requires root" >&2; exit 1; }
  command -v pct >/dev/null
  pct config "$ctid" >/dev/null
  [[ $(pct status "$ctid") == "status: running" ]] || {
    echo "monitoring CT $ctid is not running" >&2
    exit 1
  }
  for asset in "${assets[@]}"; do
    [[ -f $stage/$asset ]] || { echo "missing staged asset: $asset" >&2; exit 1; }
  done
fi

run() {
  printf '+ '
  printf '%q ' "$@"
  printf '\n'
  if [[ $dry_run == false ]]; then
    "$@"
  fi
}

if [[ $dry_run == true ]]; then
  echo "+ verify CT $ctid owns private address $monitoring_ip"
  echo "+ verify CT can scrape private http://$zerofs_ip:9567/metrics"
  echo "+ verify Grafana health at $grafana_health_url"
  echo "+ backup exact destination files under /var/lib/zerofs-monitoring-backups/TIMESTAMP"
  echo "+ provision Prometheus if absent; install assets and bind it to loopback only"
  echo "+ promtool check config, restart services, health-check"
  echo "+ rollback exact files and services on any error"
  exit 0
fi

pct exec "$ctid" -- ip -4 -o addr show scope global | grep -Eq "[[:space:]]$monitoring_ip/[0-9]+[[:space:]]"
pct exec "$ctid" -- systemctl cat grafana-server.service >/dev/null
pct exec "$ctid" -- curl --fail --silent --show-error --max-time 10 "http://$zerofs_ip:9567/metrics" >/dev/null

prometheus_preinstalled=false
prometheus_was_active=false
prometheus_was_enabled=false
grafana_was_active=false
grafana_was_enabled=false
if pct exec "$ctid" -- dpkg-query -W -f="\${Status}" prometheus 2>/dev/null | grep -Fq 'install ok installed'; then
  prometheus_preinstalled=true
fi
if pct exec "$ctid" -- systemctl is-active --quiet prometheus.service; then
  prometheus_was_active=true
fi
if pct exec "$ctid" -- systemctl is-enabled --quiet prometheus.service; then
  prometheus_was_enabled=true
fi
if pct exec "$ctid" -- systemctl is-active --quiet grafana-server.service; then
  grafana_was_active=true
fi
if pct exec "$ctid" -- systemctl is-enabled --quiet grafana-server.service; then
  grafana_was_enabled=true
fi
prometheus_masked_for_install=false
cleanup_install_guard() {
  if [[ $prometheus_masked_for_install == true ]]; then
    pct exec "$ctid" -- systemctl disable --now prometheus.service >/dev/null 2>&1 || true
    pct exec "$ctid" -- systemctl unmask prometheus.service >/dev/null 2>&1 || true
  fi
}
trap cleanup_install_guard EXIT
if [[ $prometheus_preinstalled == false ]]; then
  # Debian-family packages may auto-start services. Mask first so an
  # unconfigured 0.0.0.0:9090 listener can never appear during installation.
  run pct exec "$ctid" -- systemctl mask prometheus.service
  prometheus_masked_for_install=true
  run pct exec "$ctid" -- apt-get update
  run pct exec "$ctid" -- env DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends prometheus curl iproute2 python3
  run pct exec "$ctid" -- systemctl disable --now prometheus.service
  run pct exec "$ctid" -- systemctl unmask prometheus.service
  prometheus_masked_for_install=false
fi
pct exec "$ctid" -- systemctl cat prometheus.service >/dev/null
pct exec "$ctid" -- test -f /etc/prometheus/prometheus.yml

timestamp=$(date -u +%Y%m%dT%H%M%SZ)
backup_root="/var/lib/zerofs-monitoring-backups/$timestamp"
destinations=(
  /etc/prometheus/prometheus.yml
  /etc/prometheus/scrape.d/zerofs-prod.yml
  /etc/default/prometheus
  /etc/grafana/provisioning/datasources/zerofs-prometheus.yml
  /etc/grafana/provisioning/dashboards/zerofs.yml
  /var/lib/grafana/dashboards/zerofs/zerofs-overview.json
)
backup_names=(
  prometheus.yml
  zerofs-prod.yml
  prometheus-default
  zerofs-prometheus.yml
  zerofs-dashboard.yml
  zerofs-overview.json
)
existed=()

run pct exec "$ctid" -- install -d -m 0700 "$backup_root"
for index in "${!destinations[@]}"; do
  destination=${destinations[$index]}
  if pct exec "$ctid" -- test -e "$destination"; then
    existed+=(true)
    run pct exec "$ctid" -- cp -a -- "$destination" "$backup_root/${backup_names[$index]}"
  else
    existed+=(false)
  fi
done

rollback() {
  local code=$?
  trap - ERR
  set +e
  echo "rollback: restoring prior monitoring configuration from $backup_root" >&2
  for index in "${!destinations[@]}"; do
    if [[ ${existed[$index]} == true ]]; then
      pct exec "$ctid" -- cp -a -- "$backup_root/${backup_names[$index]}" "${destinations[$index]}"
    else
      pct exec "$ctid" -- rm -f -- "${destinations[$index]}"
    fi
  done
  pct exec "$ctid" -- systemctl daemon-reload >/dev/null 2>&1
  if [[ $prometheus_preinstalled == false ]]; then
    pct exec "$ctid" -- systemctl disable --now prometheus.service >/dev/null 2>&1
  elif [[ $prometheus_was_active == true ]]; then
    pct exec "$ctid" -- systemctl restart prometheus.service >/dev/null 2>&1
  else
    pct exec "$ctid" -- systemctl stop prometheus.service >/dev/null 2>&1
  fi
  if [[ $prometheus_was_enabled == true ]]; then
    pct exec "$ctid" -- systemctl enable prometheus.service >/dev/null 2>&1
  elif [[ $prometheus_preinstalled == true ]]; then
    pct exec "$ctid" -- systemctl disable prometheus.service >/dev/null 2>&1
  fi
  if [[ $grafana_was_active == true ]]; then
    pct exec "$ctid" -- systemctl restart grafana-server.service >/dev/null 2>&1
  else
    pct exec "$ctid" -- systemctl stop grafana-server.service >/dev/null 2>&1
  fi
  if [[ $grafana_was_enabled == true ]]; then
    pct exec "$ctid" -- systemctl enable grafana-server.service >/dev/null 2>&1
  else
    pct exec "$ctid" -- systemctl disable grafana-server.service >/dev/null 2>&1
  fi
  exit "$code"
}
trap rollback ERR

run pct exec "$ctid" -- install -d -m 0755 \
  /etc/prometheus/scrape.d \
  /etc/grafana/provisioning/datasources \
  /etc/grafana/provisioning/dashboards \
  /var/lib/grafana/dashboards/zerofs

if [[ $prometheus_preinstalled == false ]] \
  || pct exec "$ctid" -- grep -Fqx '# Managed by the ZeroFS Proxmox monitoring bundle.' /etc/prometheus/prometheus.yml; then
  run pct push "$ctid" "$stage/zerofs-new-prometheus.yml" /etc/prometheus/prometheus.yml --perms 0644
elif pct exec "$ctid" -- grep -Eq '^[[:space:]]*scrape_config_files:' /etc/prometheus/prometheus.yml; then
  pct exec "$ctid" -- grep -Fq '/etc/prometheus/scrape.d/*.yml' /etc/prometheus/prometheus.yml || {
    echo "existing scrape_config_files does not include /etc/prometheus/scrape.d/*.yml" >&2
    false
  }
else
  existing_config="$stage/existing-prometheus.yml"
  merged_config="$stage/merged-prometheus.yml"
  run pct pull "$ctid" /etc/prometheus/prometheus.yml "$existing_config"
  run python3 "$stage/merge-prometheus-config.py" \
    --existing "$existing_config" \
    --job "$stage/zerofs-scrape.yml" \
    --output "$merged_config"
  run pct push "$ctid" "$merged_config" /etc/prometheus/prometheus.yml --perms 0644
fi

run pct push "$ctid" "$stage/zerofs-scrape.yml" /etc/prometheus/scrape.d/zerofs-prod.yml --perms 0644
supported_args=false
for expected in 'ARGS=""' "ARGS=''" 'ARGS="--web.listen-address=127.0.0.1:9090"'; do
  if pct exec "$ctid" -- grep -Fqx "$expected" /etc/default/prometheus; then
    supported_args=true
  fi
done
if [[ $supported_args == false ]]; then
  echo "existing /etc/default/prometheus has custom ARGS; refusing to overwrite them" >&2
  false
fi
run pct push "$ctid" "$stage/zerofs-prometheus-default" /etc/default/prometheus --perms 0644
run pct push "$ctid" "$stage/zerofs-prometheus.yml" /etc/grafana/provisioning/datasources/zerofs-prometheus.yml --perms 0644
run pct push "$ctid" "$stage/zerofs-dashboard.yml" /etc/grafana/provisioning/dashboards/zerofs.yml --perms 0644
run pct push "$ctid" "$stage/zerofs-overview.json" /var/lib/grafana/dashboards/zerofs/zerofs-overview.json --perms 0644
run pct exec "$ctid" -- chown grafana:grafana /var/lib/grafana/dashboards/zerofs/zerofs-overview.json

run pct exec "$ctid" -- promtool check config /etc/prometheus/prometheus.yml
run pct exec "$ctid" -- python3 -m json.tool /var/lib/grafana/dashboards/zerofs/zerofs-overview.json
run pct exec "$ctid" -- systemctl daemon-reload
run pct exec "$ctid" -- systemctl enable --now prometheus.service
run pct exec "$ctid" -- systemctl restart prometheus.service
run pct exec "$ctid" -- systemctl is-active --quiet prometheus.service
run pct exec "$ctid" -- curl --fail --silent --show-error --max-time 10 http://127.0.0.1:9090/-/ready
listeners=$(pct exec "$ctid" -- ss -H -lnt)
grep -Fq '127.0.0.1:9090' <<<"$listeners"
if grep -Eq '(^|[[:space:]])(0\.0\.0\.0|\[::\]):9090([[:space:]]|$)' <<<"$listeners"; then
  echo "Prometheus escaped its loopback-only listener" >&2
  false
fi
run pct exec "$ctid" -- systemctl restart grafana-server.service
run pct exec "$ctid" -- systemctl is-active --quiet grafana-server.service
run pct exec "$ctid" -- curl --fail --silent --show-error --max-time 10 "$grafana_health_url"

echo "monitoring_ctid=$ctid"
echo "backup=$backup_root"
echo "dashboard_uid=zerofs-prod-overview"
trap - ERR
