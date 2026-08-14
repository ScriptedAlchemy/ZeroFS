#!/usr/bin/env python3
"""Safely provision ZeroFS Prometheus and Grafana assets in a monitoring LXC."""

from __future__ import annotations

import argparse
import ipaddress
import json
import re
import shutil
import subprocess
import tempfile
import urllib.parse
from pathlib import Path
from typing import Sequence


ROOT = Path(__file__).resolve().parent
RFC1918 = tuple(
    ipaddress.ip_network(value)
    for value in ("10.0.0.0/8", "172.16.0.0/12", "192.168.0.0/16")
)


def private_ipv4(value: str, label: str) -> str:
    try:
        address = ipaddress.ip_address(value)
    except ValueError as error:
        raise ValueError(f"{label} must be an IPv4 address") from error
    if address.version != 4 or not any(address in network for network in RFC1918):
        raise ValueError(f"{label} must be in RFC1918 private space")
    return str(address)


def safe_prometheus_url(value: str) -> str:
    parsed = urllib.parse.urlsplit(value)
    if parsed.scheme != "http" or parsed.username or parsed.password:
        raise ValueError("Prometheus URL must be credential-free HTTP")
    if parsed.query or parsed.fragment or parsed.path not in ("", "/"):
        raise ValueError("Prometheus URL must contain only scheme, host, and port")
    if parsed.hostname is None or parsed.port is None:
        raise ValueError("Prometheus URL requires an explicit host and port")
    try:
        host = ipaddress.ip_address(parsed.hostname)
    except ValueError as error:
        raise ValueError("Prometheus URL host must be an IP address") from error
    if not (host.is_loopback or any(host in network for network in RFC1918)):
        raise ValueError("Prometheus URL must use loopback or RFC1918 private space")
    return value.rstrip("/")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--pve-host", default="gthost-tor-pve-root")
    parser.add_argument("--monitoring-ctid", type=int, default=123)
    parser.add_argument("--monitoring-ip", default="10.10.10.53")
    parser.add_argument("--zerofs-ip", required=True)
    parser.add_argument("--prometheus-url", default="http://127.0.0.1:9090")
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--dry-run", action="store_true")
    mode.add_argument(
        "--apply",
        action="store_true",
        help="perform the reviewed installation; mutations are otherwise impossible",
    )
    return parser


def validate_args(args: argparse.Namespace) -> argparse.Namespace:
    if not 100 <= args.monitoring_ctid <= 999_999_999:
        raise ValueError("monitoring CTID must be between 100 and 999999999")
    if re.fullmatch(r"[A-Za-z0-9_.@-]+", args.pve_host) is None:
        raise ValueError("PVE SSH alias contains unsafe characters")
    args.monitoring_ip = private_ipv4(args.monitoring_ip, "monitoring IP")
    args.zerofs_ip = private_ipv4(args.zerofs_ip, "ZeroFS IP")
    if args.monitoring_ip == args.zerofs_ip:
        raise ValueError("monitoring and ZeroFS must use distinct private addresses")
    args.prometheus_url = safe_prometheus_url(args.prometheus_url)
    return args


def render_assets(args: argparse.Namespace, destination: Path) -> list[Path]:
    destination.mkdir(mode=0o700, parents=True, exist_ok=True)
    templates = {
        ROOT / "prometheus" / "zerofs-scrape.yml.template": "zerofs-scrape.yml",
        ROOT
        / "prometheus"
        / "zerofs-new-prometheus.yml.template": "zerofs-new-prometheus.yml",
        ROOT / "grafana" / "zerofs-prometheus.yml.template": "zerofs-prometheus.yml",
    }
    rendered: list[Path] = []
    replacements = {
        "@@ZEROFS_IP@@": args.zerofs_ip,
        "@@PROMETHEUS_URL@@": args.prometheus_url,
    }
    for source, name in templates.items():
        text = source.read_text()
        for before, after in replacements.items():
            text = text.replace(before, after)
        if "@@" in text:
            raise ValueError(f"unresolved placeholder in {source}")
        target = destination / name
        target.write_text(text)
        rendered.append(target)
    for source in (
        ROOT / "grafana" / "zerofs-dashboard.yml",
        ROOT / "grafana" / "zerofs-overview.json",
        ROOT / "prometheus" / "zerofs-prometheus-default",
        ROOT / "host-install.sh",
    ):
        target = destination / source.name
        shutil.copyfile(source, target)
        rendered.append(target)
    json.loads((destination / "zerofs-overview.json").read_text())
    return rendered


def run(command: Sequence[str], *, input_text: str | None = None) -> None:
    subprocess.run(command, text=True, input=input_text, check=True)


def print_plan(args: argparse.Namespace) -> None:
    print(f"monitoring_ctid={args.monitoring_ctid}")
    print(f"monitoring_ip={args.monitoring_ip}")
    print(f"zerofs_metrics=http://{args.zerofs_ip}:9567/metrics")
    print("+ validate exact CTID/IP and existing grafana-server.service")
    print("+ provision loopback-only prometheus.service if it is absent")
    print("+ create timestamped backup of every changed file inside the monitoring CT")
    print("+ install credential-free Prometheus scrape and Grafana provisioning assets")
    print("+ promtool check config /etc/prometheus/prometheus.yml")
    print("+ restart and health-check prometheus.service and grafana-server.service")
    print("+ rollback exact files and restart prior services on any failed validation")


def install(args: argparse.Namespace) -> None:
    with tempfile.TemporaryDirectory(prefix="zerofs-monitoring-") as directory:
        rendered_root = Path(directory)
        assets = render_assets(args, rendered_root)
        if args.dry_run:
            print_plan(args)
            return

        stage = f"/var/tmp/zerofs-monitoring-{args.monitoring_ctid}"
        run(
            [
                "ssh",
                "-o",
                "BatchMode=yes",
                args.pve_host,
                "install",
                "-d",
                "-m",
                "0700",
                stage,
            ]
        )
        try:
            for asset in assets:
                run(["scp", "-q", str(asset), f"{args.pve_host}:{stage}/{asset.name}"])
            run(
                [
                    "ssh",
                    "-o",
                    "BatchMode=yes",
                    args.pve_host,
                    "bash",
                    f"{stage}/host-install.sh",
                    "--ctid",
                    str(args.monitoring_ctid),
                    "--monitoring-ip",
                    args.monitoring_ip,
                    "--zerofs-ip",
                    args.zerofs_ip,
                    "--stage",
                    stage,
                ]
            )
        finally:
            names = [f"{stage}/{asset.name}" for asset in assets]
            cleanup = "set -e; rm -f -- " + " ".join(names) + f"; rmdir {stage}"
            run(
                ["ssh", "-o", "BatchMode=yes", args.pve_host, "bash", "-se"],
                input_text=cleanup,
            )


def main(argv: Sequence[str] | None = None) -> int:
    try:
        args = validate_args(build_parser().parse_args(argv))
        install(args)
    except (OSError, ValueError, subprocess.CalledProcessError) as error:
        print(f"error: {error}", file=__import__("sys").stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
