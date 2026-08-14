#!/usr/bin/env python3
"""Build and safely deploy ZeroFS into a private Proxmox LXC.

The coordinator runs on the ZeroFS build machine (VM100 in the documented
deployment).  Proxmox mutations are delegated to ``host-deploy.sh`` only after
the NBD consumer is quiesced and every volatile/writeback tier is drained.
"""

from __future__ import annotations

import argparse
import hashlib
import ipaddress
import os
import re
import shlex
import shutil
import subprocess
import sys
import time
import tomllib
import urllib.parse
from dataclasses import dataclass
from pathlib import Path
from typing import Sequence


TARGET_CLIENT_UNIT = "zerofs-lxc-nbd-client.service"
TARGET_MOUNT_UNIT = "mnt-zerofs-lxc.mount"
TARGET_MOUNTPOINT = "/mnt/zerofs-lxc"
TARGET_NBD_DEVICE = "/dev/nbd0"
RFC1918_NETWORKS = tuple(
    ipaddress.ip_network(value)
    for value in ("10.0.0.0/8", "172.16.0.0/12", "192.168.0.0/16")
)


def is_rfc1918(address: ipaddress.IPv4Address | ipaddress.IPv6Address) -> bool:
    return address.version == 4 and any(
        address in network for network in RFC1918_NETWORKS
    )


REQUIRED_METRICS = (
    "zerofs_nbd_volatile_memory_dirty_bytes",
    "zerofs_nbd_volatile_memory_dirty_operations",
    "zerofs_nbd_volatile_memory_terminal",
    "zerofs_writeback_accepted_sequence",
    "zerofs_writeback_local_sequence",
    "zerofs_writeback_remote_sequence",
    "zerofs_writeback_dirty_ram_bytes",
    "zerofs_writeback_dirty_ssd_reserved_bytes",
    "zerofs_writeback_terminal_error",
)


@dataclass(frozen=True)
class DrainState:
    volatile_dirty_bytes: int
    volatile_dirty_operations: int
    volatile_terminal: bool
    accepted: int
    local: int
    remote: int
    dirty_ram: int
    dirty_ssd: int
    writeback_terminal: bool

    @property
    def drained(self) -> bool:
        return (
            not self.volatile_terminal
            and not self.writeback_terminal
            and self.volatile_dirty_bytes == 0
            and self.volatile_dirty_operations == 0
            and self.accepted == self.local == self.remote
            and self.dirty_ram == 0
            and self.dirty_ssd == 0
        )


def parse_drain_state(text: str) -> DrainState:
    values: dict[str, int] = {}
    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        fields = line.split()
        if len(fields) < 2 or fields[0] not in REQUIRED_METRICS:
            continue
        try:
            values[fields[0]] = int(float(fields[1]))
        except ValueError as error:
            raise ValueError(f"invalid ZeroFS metric: {line}") from error
    missing = sorted(set(REQUIRED_METRICS) - values.keys())
    if missing:
        raise ValueError(f"missing ZeroFS metrics: {', '.join(missing)}")
    return DrainState(
        volatile_dirty_bytes=values[REQUIRED_METRICS[0]],
        volatile_dirty_operations=values[REQUIRED_METRICS[1]],
        volatile_terminal=bool(values[REQUIRED_METRICS[2]]),
        accepted=values[REQUIRED_METRICS[3]],
        local=values[REQUIRED_METRICS[4]],
        remote=values[REQUIRED_METRICS[5]],
        dirty_ram=values[REQUIRED_METRICS[6]],
        dirty_ssd=values[REQUIRED_METRICS[7]],
        writeback_terminal=bool(values[REQUIRED_METRICS[8]]),
    )


def _addresses(section: object) -> list[str]:
    if not isinstance(section, dict):
        return []
    value = section.get("addresses", [])
    if not isinstance(value, list) or not all(isinstance(item, str) for item in value):
        raise ValueError("listener addresses must be a list of strings")
    return value


def _split_listener(
    value: str,
) -> tuple[ipaddress.IPv4Address | ipaddress.IPv6Address, int]:
    try:
        host, raw_port = value.rsplit(":", 1)
        host = host.strip("[]")
        return ipaddress.ip_address(host), int(raw_port)
    except (ValueError, TypeError) as error:
        raise ValueError(
            f"listener must be an IP address and port: {value!r}"
        ) from error


def validate_server_config(
    path: Path,
    container_ip: str,
    *,
    memory_mb: int | None = None,
    role: str = "dev",
) -> str:
    if role not in {"prod", "dev"}:
        raise ValueError("role must be prod or dev")
    expected_ip = ipaddress.ip_address(container_ip)
    if not is_rfc1918(expected_ip):
        raise ValueError("container IP must be RFC1918 private space")
    with path.open("rb") as handle:
        settings = tomllib.load(handle)

    filesystem = settings.get("filesystem", {})
    if (
        isinstance(filesystem, dict)
        and filesystem.get("ignore_fsync", False) is not False
    ):
        raise ValueError("[filesystem] ignore_fsync must be false")

    servers = settings.get("servers")
    if not isinstance(servers, dict):
        raise ValueError("[servers] is required")
    budget: int | float = 0
    if role == "dev":
        nbd = servers.get("nbd")
        if not isinstance(nbd, dict):
            raise ValueError("[servers.nbd] is required for the dev role")
        nbd_addresses = _addresses(nbd)
        if not nbd_addresses:
            raise ValueError("NBD must have a private TCP listener")
        for address in nbd_addresses:
            host, port = _split_listener(address)
            if host != expected_ip or port != 10809:
                raise ValueError(
                    "NBD must listen only on the private container address at port 10809"
                )
        if nbd.get("write_ack_mode") != "volatile_memory":
            raise ValueError('NBD write_ack_mode must be "volatile_memory"')
        budget = nbd.get("volatile_memory_gb", 0)
        if not isinstance(budget, (int, float)) or budget <= 0:
            raise ValueError("NBD volatile_memory_gb must be positive")
        for frontend in ("nfs", "ninep", "webui"):
            if frontend in servers and servers[frontend] not in (None, {}):
                raise ValueError(
                    "volatile acknowledgement requires exclusive NBD access"
                )
    else:
        if servers.get("nbd") not in (None, {}):
            raise ValueError("prod must use container-owned 9P, not an NBD listener")
        ninep = servers.get("ninep")
        if (
            not isinstance(ninep, dict)
            or ninep.get("unix_socket") != "/run/zerofs/9p.sock"
        ):
            raise ValueError("prod requires a container-owned 9P Unix socket")
        if _addresses(ninep):
            raise ValueError("prod 9P must be Unix-socket only")
        nfs = servers.get("nfs")
        if not isinstance(nfs, dict):
            raise ValueError("prod requires the private NFS listener")
        nfs_addresses = _addresses(nfs)
        if not nfs_addresses:
            raise ValueError("prod NFS must have a private TCP listener")
        for address in nfs_addresses:
            host, port = _split_listener(address)
            if host != expected_ip or port != 2049:
                raise ValueError(
                    "NFS must listen only on the private container address at port 2049"
                )
        webui = servers.get("webui")
        if not isinstance(webui, dict):
            raise ValueError("prod requires the private WebUI listener")
        webui_addresses = _addresses(webui)
        if not webui_addresses:
            raise ValueError("prod WebUI must have a private TCP listener")
        for address in webui_addresses:
            host, port = _split_listener(address)
            if host != expected_ip or port != 8080:
                raise ValueError(
                    "WebUI must listen only on the private container address at port 8080"
                )
    rpc = servers.get("rpc", {})
    if _addresses(rpc):
        raise ValueError("RPC must be Unix-socket only")
    if not isinstance(rpc, dict) or not rpc.get("unix_socket"):
        raise ValueError("RPC must have a Unix socket for local lifecycle operations")

    prometheus = settings.get("prometheus")
    metrics_addresses = _addresses(prometheus)
    if not metrics_addresses:
        raise ValueError(
            "[prometheus] must expose metrics on the private container address"
        )
    for address in metrics_addresses:
        host, port = _split_listener(address)
        if host != expected_ip or port != 9567:
            raise ValueError(
                "Prometheus must listen only on the private container address at port 9567"
            )

    writeback = settings.get("writeback")
    if not isinstance(writeback, dict) or writeback.get("enabled") is not True:
        raise ValueError("[writeback] must be enabled")
    expected_ack = "ssd" if role == "prod" else "memory"
    if writeback.get("ack_mode") != expected_ack:
        raise ValueError(f"[writeback] ack_mode must be {expected_ack} for {role}")
    writeback_dir = writeback.get("dir")
    if not isinstance(writeback_dir, str) or not writeback_dir.startswith(
        "/srv/zerofs-persist/state/"
    ):
        raise ValueError("[writeback] dir must be below /srv/zerofs-persist/state")
    min_free = writeback.get("min_free_gb", 0)
    if not isinstance(min_free, (int, float)) or min_free <= 0:
        raise ValueError("[writeback] min_free_gb must be positive")

    cache = settings.get("cache")
    if not isinstance(cache, dict) or not str(cache.get("dir", "")).startswith(
        "/srv/zerofs-persist/cache"
    ):
        raise ValueError("[cache] dir must be below /srv/zerofs-persist/cache")
    if memory_mb is not None:
        ram_values = (
            cache.get("memory_size_gb", 0),
            writeback.get("memory_size_gb", 0),
            budget,
        )
        if not all(
            isinstance(value, (int, float)) and value >= 0 for value in ram_values
        ):
            raise ValueError("RAM tier sizes must be non-negative numbers")
        required_mb = int(sum(ram_values) * 1000) + 8192
        if memory_mb < required_mb:
            raise ValueError(
                f"container memory {memory_mb} MiB is below the RAM tiers plus "
                f"8 GiB process overhead ({required_mb} MiB required)"
            )
    storage = settings.get("storage")
    if not isinstance(storage, dict) or not isinstance(storage.get("url"), str):
        raise ValueError("[storage] url is required")
    storage_url = storage["url"]
    sftp = settings.get("sftp")
    if storage_url.startswith("sftp://"):
        if not isinstance(sftp, dict):
            raise ValueError("[sftp] is required for SFTP storage")
        for field in ("max_connections", "read_concurrency", "write_concurrency"):
            value = sftp.get(field)
            if not isinstance(value, int) or not 1 <= value <= 4:
                raise ValueError(f"SFTP {field} must be between one and four")
    return storage_url


def require_replace_confirmation(ctid: int, confirmation: str | None) -> None:
    if confirmation != str(ctid):
        raise ValueError(
            f"replace is destructive; set ZEROFS_CONFIRM_REPLACE={ctid} exactly"
        )


def validate_state_root(value: str, ctid: int, role: str) -> Path:
    path = Path(value)
    expected = Path("/var/lib/zerofs-lxc") / f"{role}-{ctid}"
    if not path.is_absolute() or path != expected:
        raise ValueError(f"state root must be exactly {expected}")
    return path


def namespace_id(storage_url: str, role: str, state_root: Path) -> str:
    if storage_url.startswith("file://"):
        identity = f"{role}\0{state_root}\0{storage_url}"
    else:
        parsed = urllib.parse.urlsplit(storage_url)
        userinfo = parsed.netloc.rsplit("@", 1)[0] + "@" if "@" in parsed.netloc else ""
        host = (parsed.hostname or "").lower()
        if ":" in host:
            host = f"[{host}]"
        port = parsed.port
        port_suffix = (
            ""
            if port is None or (parsed.scheme.lower() == "sftp" and port == 22)
            else f":{port}"
        )
        path = parsed.path.rstrip("/") or "/"
        identity = urllib.parse.urlunsplit(
            (
                parsed.scheme.lower(),
                f"{userinfo}{host}{port_suffix}",
                path,
                parsed.query,
                "",
            )
        )
    return hashlib.sha256(identity.encode("utf-8")).hexdigest()


def release_id(commit: str, paths: Sequence[Path], values: Sequence[str] = ()) -> str:
    digest = hashlib.sha256()
    for value in values:
        encoded = value.encode("utf-8")
        digest.update(len(encoded).to_bytes(8, "big"))
        digest.update(encoded)
    for path in paths:
        digest.update(path.stat().st_size.to_bytes(8, "big"))
        with path.open("rb") as handle:
            for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                digest.update(chunk)
    return f"{commit[:12]}-{digest.hexdigest()[:16]}"


def shell_join(command: Sequence[str]) -> str:
    return shlex.join([str(part) for part in command])


def build_host_plan(
    *,
    action: str,
    ctid: int,
    container_ip: str,
    bridge: str,
    gateway: str = "10.10.10.1",
    template: str,
    state_root: Path,
    memory_mb: int,
    rootfs: str,
) -> list[list[str]]:
    if action not in {"deploy", "replace", "cleanup"}:
        raise ValueError(f"unsupported host action: {action}")
    plan: list[list[str]] = []
    if action == "replace":
        plan.extend(
            [
                ["pct", "shutdown", str(ctid), "--timeout", "120"],
                ["pct", "destroy", str(ctid), "--purge", "1"],
            ]
        )
    if action in {"deploy", "replace"}:
        plan.extend(
            [
                ["install", "-d", "-m", "0750", str(state_root)],
                [
                    "pct",
                    "create",
                    str(ctid),
                    template,
                    "--unprivileged",
                    "1",
                    "--memory",
                    str(memory_mb),
                    "--rootfs",
                    rootfs,
                    "--net0",
                    f"name=eth0,bridge={bridge},ip={container_ip}/24,gw={gateway},type=veth",
                    "--mp0",
                    f"{state_root},mp=/srv/zerofs-persist",
                    "--onboot",
                    "1",
                ],
            ]
        )
    else:
        plan.extend(
            [
                ["pct", "shutdown", str(ctid), "--timeout", "120"],
                ["pct", "destroy", str(ctid), "--purge", "1"],
            ]
        )
    return plan


class Runner:
    def __init__(self, dry_run: bool) -> None:
        self.dry_run = dry_run

    def run(
        self,
        command: Sequence[str],
        *,
        cwd: Path | None = None,
        capture: bool = False,
        input_text: str | None = None,
    ) -> subprocess.CompletedProcess[str]:
        rendered = shell_join(command)
        print(f"+ {rendered}")
        if self.dry_run:
            if input_text:
                for line in input_text.rstrip().splitlines():
                    print(f"  | {line}")
            return subprocess.CompletedProcess(command, 0, "", "")
        return subprocess.run(
            list(command),
            cwd=cwd,
            check=True,
            text=True,
            input=input_text,
            capture_output=capture,
        )


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _repo_root() -> Path:
    return Path(__file__).resolve().parent.parent


def _git_receipt(runner: Runner, root: Path) -> tuple[str, bool]:
    dirty = runner.run(["git", "status", "--porcelain"], cwd=root, capture=True).stdout
    if not runner.dry_run and dirty:
        raise RuntimeError("refusing to deploy from a dirty checkout")
    commit = runner.run(
        ["git", "rev-parse", "HEAD"], cwd=root, capture=True
    ).stdout.strip()
    return commit or "DRY_RUN_COMMIT", bool(dirty)


def node_version_supported(value: str) -> bool:
    match = re.fullmatch(r"v?(\d+)\.(\d+)\.(\d+)", value.strip())
    if match is None:
        return False
    version = tuple(int(component) for component in match.groups())
    major, minor, _patch = version
    return (major == 20 and minor >= 19) or (major == 22 and minor >= 12) or major > 22


def release_rustflags(existing: str) -> str:
    flags = existing.strip()
    for required in ("--cfg tokio_unstable", "--cfg io_uring_skip_arch_check"):
        if required not in flags:
            flags = f"{flags} {required}".strip()
    return flags


def _build(runner: Runner, root: Path, role: str) -> Path:
    target = root / "target" / "proxmox-lxc"
    if role == "prod":
        user_paths = (Path.home() / ".local" / "bin", Path.home() / ".cargo" / "bin")
        os.environ["PATH"] = os.pathsep.join(
            [*(str(path) for path in user_paths), os.environ.get("PATH", "")]
        )
        required = (
            "make",
            "npm",
            "node",
            "wasm-pack",
            "curl",
            "bsdtar",
            "unsquashfs",
            "cpio",
            "xz",
            "sha256sum",
        )
        if not runner.dry_run:
            missing = [command for command in required if shutil.which(command) is None]
            if missing:
                raise RuntimeError(
                    "production WebUI build prerequisites are missing: "
                    + ", ".join(missing)
                    + "; see proxmox/README.md"
                )
            node_version = runner.run(["node", "--version"], capture=True).stdout
            if not node_version_supported(node_version):
                raise RuntimeError(
                    f"production WebUI requires Node 20.19+, 22.12+, or newer; got {node_version.strip()!r}"
                )
        runner.run(["make", "webui"], cwd=root)
        if (
            not runner.dry_run
            and not (root / "webui" / "dist" / "index.html").is_file()
        ):
            raise RuntimeError("make webui completed without webui/dist/index.html")
    command = [
        "cargo",
        "build",
        "--release",
        "--locked",
        "--manifest-path",
        str(root / "zerofs" / "Cargo.toml"),
        "--target-dir",
        str(target),
    ]
    if role == "prod":
        command.extend(["--features", "webui"])
    os.environ["RUSTFLAGS"] = release_rustflags(os.environ.get("RUSTFLAGS", ""))
    runner.run(command, cwd=root)
    return target / "release" / "zerofs"


def _ssh(runner: Runner, host: str, script: str) -> None:
    runner.run(["ssh", "-o", "BatchMode=yes", host, "bash", "-se"], input_text=script)


def _quiesce_guest(runner: Runner, args: argparse.Namespace) -> None:
    script = f"""set -euo pipefail
unit_loaded() {{
  test "$(systemctl show -p LoadState --value "$1" 2>/dev/null || true)" != not-found
}}
if findmnt -rn -M {shlex.quote(args.source_mountpoint)} >/dev/null 2>&1; then
  sudo sync -f {shlex.quote(args.source_mountpoint)}
fi
if unit_loaded {shlex.quote(args.source_mount_unit)}; then
  sudo systemctl stop {shlex.quote(args.source_mount_unit)}
fi
if findmnt -rn -M {shlex.quote(args.source_mountpoint)} >/dev/null 2>&1; then
  echo 'mount remained active; refusing to disconnect NBD' >&2
  exit 1
fi
if unit_loaded {shlex.quote(args.source_client_unit)}; then
  sudo systemctl stop {shlex.quote(args.source_client_unit)}
fi
nbd_pid=$(cat /sys/class/block/nbd0/pid 2>/dev/null || true)
if test -n "$nbd_pid"; then
  echo 'nbd0 remains connected; pass the actual source client unit' >&2
  exit 1
fi
"""
    _ssh(runner, args.vm_host, script)


def _wait_remote_drain(runner: Runner, args: argparse.Namespace) -> None:
    if args.skip_existing_drain:
        return
    if runner.dry_run:
        print(
            f"+ wait-for-drain {args.existing_metrics_url} "
            f"timeout={args.drain_timeout}s stable=4"
        )
        return
    command = [
        "ssh",
        "-o",
        "BatchMode=yes",
        args.vm_host,
        "curl",
        "--fail",
        "--silent",
        "--show-error",
        args.existing_metrics_url,
    ]
    deadline = time.monotonic() + args.drain_timeout
    stable = 0
    last: DrainState | None = None
    while time.monotonic() < deadline:
        result = runner.run(command, capture=True)
        last = parse_drain_state(result.stdout)
        if last.volatile_terminal or last.writeback_terminal:
            raise RuntimeError(f"ZeroFS reported a terminal error: {last}")
        stable = stable + 1 if last.drained else 0
        if stable >= 4:
            print(f"drain verified: {last}")
            return
        time.sleep(1)
    raise TimeoutError(
        f"ZeroFS did not drain within {args.drain_timeout}s; last={last}"
    )


def _stop_source_server(runner: Runner, args: argparse.Namespace) -> None:
    if args.source_server_unit is None:
        return
    script = f"""set -euo pipefail
sudo systemctl stop {shlex.quote(args.source_server_unit)}
test "$(systemctl is-active {shlex.quote(args.source_server_unit)} 2>/dev/null || true)" != active
"""
    _ssh(runner, args.vm_host, script)


def _stage_and_run_host(
    runner: Runner,
    args: argparse.Namespace,
    binary: Path,
    commit: str,
    binary_hash: str,
    namespace: str,
    release: str,
) -> None:
    bundle = Path(__file__).resolve().parent
    stage = f"/var/tmp/zerofs-lxc-deploy-{args.ctid}-{commit[:12]}"
    _ssh(
        runner,
        args.pve_host,
        f"set -euo pipefail\ninstall -d -m 0700 {shlex.quote(stage)}\n",
    )
    files: tuple[tuple[Path, str], ...]
    if args.action == "cleanup":
        files = ((bundle / "host-deploy.sh", "host-deploy.sh"),)
    else:
        files = (
            (binary, "zerofs"),
            (args.config, "zerofs.toml"),
            (bundle / "host-deploy.sh", "host-deploy.sh"),
            (bundle / "hooks" / "zerofs-lxc-hook.sh", "zerofs-lxc-hook.sh"),
            (bundle / "systemd" / "zerofs-lxc.service", "zerofs-lxc.service"),
        )
        if args.role == "prod" and args.prod_access in {"smb", "both"}:
            files += (
                (
                    bundle / "systemd" / "zerofs-lxc-mount.service",
                    "zerofs-lxc-mount.service",
                ),
                (bundle / "templates" / "smb.conf", "smb.conf"),
            )
        if args.env_file is not None:
            files += ((args.env_file, "zerofs.env"),)
        if args.identity_file is not None:
            files += ((args.identity_file, "storage-key"),)
        if args.known_hosts is not None:
            files += ((args.known_hosts, "known_hosts"),)
        if (
            args.role == "prod"
            and args.prod_access in {"smb", "both"}
            and args.samba_password_file is not None
        ):
            files += ((args.samba_password_file, "samba-password"),)
    for path, destination in files:
        runner.run(["scp", "-q", str(path), f"{args.pve_host}:{stage}/{destination}"])
    host_args = [
        "bash",
        f"{stage}/host-deploy.sh",
        args.action,
        "--role",
        args.role,
        "--ctid",
        str(args.ctid),
        "--container-ip",
        args.container_ip,
        "--bridge",
        args.bridge,
        "--gateway",
        args.gateway,
        "--template",
        args.template,
        "--rootfs",
        args.rootfs,
        "--memory-mb",
        str(args.memory_mb),
        "--cores",
        str(args.cores),
        "--state-root",
        args.state_root,
        "--stage",
        stage,
        "--commit",
        commit,
        "--sha256",
        binary_hash,
        "--namespace-id",
        namespace,
        "--release-id",
        release,
        "--samba-user",
        args.samba_user,
        "--prod-access",
        args.prod_access,
    ]
    if args.dry_run:
        host_args.append("--dry-run")
    if args.action == "replace":
        host_args.extend(["--confirm-replace", str(args.ctid)])
    try:
        runner.run(["ssh", "-o", "BatchMode=yes", args.pve_host, *host_args])
    finally:
        _ssh(
            runner,
            args.pve_host,
            f"set -euo pipefail\nrm -rf -- {shlex.quote(stage)}\n",
        )


def _reconnect_guest(runner: Runner, args: argparse.Namespace) -> None:
    script = f"""set -euo pipefail
sudo install -d -m 0755 /etc/zerofs-lxc /etc/systemd/system
sudo install -d -m 0755 /usr/local/libexec
sudo tee /etc/zerofs-lxc/client.env >/dev/null <<'EOF'
ZEROFS_NBD_HOST={args.container_ip}
ZEROFS_NBD_PORT={args.nbd_port}
ZEROFS_NBD_EXPORT={args.nbd_export}
ZEROFS_NBD_CONNECTIONS={args.connections}
ZEROFS_NBD_DEVICE={TARGET_NBD_DEVICE}
EOF
sudo install -m 0644 /tmp/zerofs-lxc-nbd-client.service /etc/systemd/system/{TARGET_CLIENT_UNIT}
sudo install -m 0644 /tmp/mnt-zerofs-lxc.mount /etc/systemd/system/{TARGET_MOUNT_UNIT}
sudo install -m 0755 /tmp/tune-nbd.sh /usr/local/libexec/zerofs-tune-nbd
sudo systemctl daemon-reload
sudo systemctl start {TARGET_CLIENT_UNIT}
sudo systemctl start {TARGET_MOUNT_UNIT}
systemctl is-active --quiet {TARGET_CLIENT_UNIT}
systemctl is-active --quiet {TARGET_MOUNT_UNIT}
findmnt -rn -M {TARGET_MOUNTPOINT}
rm -f /tmp/zerofs-lxc-nbd-client.service /tmp/mnt-zerofs-lxc.mount /tmp/tune-nbd.sh
"""
    bundle = Path(__file__).resolve().parent
    runner.run(
        [
            "scp",
            "-q",
            str(bundle / "systemd" / "zerofs-lxc-nbd-client.service"),
            str(bundle / "systemd" / "mnt-zerofs-lxc.mount"),
            str(bundle / "guest" / "tune-nbd.sh"),
            f"{args.vm_host}:/tmp/",
        ]
    )
    _ssh(runner, args.vm_host, script)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("deploy", "replace", "cleanup", "status"))
    parser.add_argument("--role", choices=("prod", "dev"), required=True)
    parser.add_argument("--pve-host", default="gthost-tor-pve-root")
    parser.add_argument("--vm-host", default="ubuntu-main")
    parser.add_argument("--ctid", type=int, required=True)
    parser.add_argument("--container-ip", required=True)
    parser.add_argument("--bridge", default="vmbr1")
    parser.add_argument("--gateway", default="10.10.10.1")
    parser.add_argument(
        "--template", default="local:vztmpl/debian-13-standard_13.1-2_amd64.tar.zst"
    )
    parser.add_argument("--rootfs", default="local-lvm:8")
    parser.add_argument("--memory-mb", type=int, default=98304)
    parser.add_argument("--cores", type=int, default=8)
    parser.add_argument("--state-root")
    parser.add_argument("--config", type=Path)
    parser.add_argument("--env-file", type=Path)
    parser.add_argument("--identity-file", type=Path)
    parser.add_argument("--known-hosts", type=Path)
    parser.add_argument("--samba-user", default="zerofs-share")
    parser.add_argument("--samba-password-file", type=Path)
    parser.add_argument("--prod-access", choices=("nfs", "smb", "both"), default="nfs")
    parser.add_argument("--metrics-url")
    parser.add_argument("--existing-metrics-url")
    parser.add_argument("--drain-timeout", type=int, default=1800)
    parser.add_argument("--skip-existing-drain", action="store_true")
    parser.add_argument("--source-client-unit", default=TARGET_CLIENT_UNIT)
    parser.add_argument("--source-mount-unit", default=TARGET_MOUNT_UNIT)
    parser.add_argument("--source-mountpoint", default=TARGET_MOUNTPOINT)
    parser.add_argument("--source-server-unit")
    parser.add_argument("--nbd-port", type=int, default=10809)
    parser.add_argument("--nbd-export", default="vm100-pilot-64g")
    parser.add_argument("--connections", type=int, default=8)
    parser.add_argument("--dry-run", action="store_true")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    args.state_root = args.state_root or f"/var/lib/zerofs-lxc/{args.role}-{args.ctid}"
    state_root = validate_state_root(args.state_root, args.ctid, args.role)
    args.metrics_url = args.metrics_url or f"http://{args.container_ip}:9567/metrics"
    args.existing_metrics_url = args.existing_metrics_url or args.metrics_url
    ip = ipaddress.ip_address(args.container_ip)
    if not is_rfc1918(ip):
        raise ValueError("container IP must be RFC1918 private space")
    gateway = ipaddress.ip_address(args.gateway)
    if not is_rfc1918(gateway) or gateway not in ipaddress.ip_network(
        f"{ip}/24", strict=False
    ):
        raise ValueError("gateway must be private and in the container /24")
    if args.role == "prod" and args.action in {"replace", "cleanup"}:
        raise ValueError(
            "prod supports drain-safe in-place deploy only; replace and cleanup are dev-only"
        )
    if args.action == "replace":
        require_replace_confirmation(
            args.ctid, os.environ.get("ZEROFS_CONFIRM_REPLACE")
        )
    if (
        args.role == "prod"
        and args.prod_access in {"smb", "both"}
        and args.action == "deploy"
        and not args.dry_run
    ):
        if args.samba_password_file is None:
            raise ValueError("SMB production access requires --samba-password-file")
    if args.skip_existing_drain and args.source_server_unit is not None:
        raise ValueError("cannot skip drain while migrating a source ZeroFS server")
    runner = Runner(args.dry_run)

    if args.action == "status":
        units = ["zerofs-lxc.service"]
        if args.role == "prod" and args.prod_access in {"smb", "both"}:
            units.extend(["zerofs-lxc-mount.service", "smbd.service"])
        runner.run(
            [
                "ssh",
                "-o",
                "BatchMode=yes",
                args.pve_host,
                "pct",
                "exec",
                str(args.ctid),
                "--",
                "systemctl",
                "status",
                "--no-pager",
                *units,
            ]
        )
        return 0

    if args.role == "dev":
        _quiesce_guest(runner, args)
        _wait_remote_drain(runner, args)
        _stop_source_server(runner, args)

    root = _repo_root()
    if args.action == "cleanup":
        placeholder = root / "proxmox" / "README.md"
        _stage_and_run_host(
            runner,
            args,
            placeholder,
            "cleanup",
            "none",
            "cleanup",
            "cleanup",
        )
        return 0

    if args.config is None:
        raise ValueError("--config is required for deploy and replace")
    storage_url = validate_server_config(
        args.config,
        args.container_ip,
        memory_mb=args.memory_mb,
        role=args.role,
    )
    namespace = namespace_id(storage_url, args.role, state_root)
    commit, _dirty = _git_receipt(runner, root)
    binary = _build(runner, root, args.role)
    binary_hash = "DRY_RUN_SHA256" if args.dry_run else sha256(binary)
    release_paths = [args.config]
    release_paths.extend(
        path
        for path in (
            args.env_file,
            args.identity_file,
            args.known_hosts,
            (args.samba_password_file if args.prod_access in {"smb", "both"} else None),
        )
        if path is not None
    )
    release_extras = [binary_hash, args.prod_access]
    if args.prod_access in {"smb", "both"}:
        release_extras.append(args.samba_user)
    release = (
        f"{commit[:12]}-DRYRUN"
        if args.dry_run
        else release_id(commit, release_paths, release_extras)
    )
    _stage_and_run_host(
        runner,
        args,
        binary,
        commit,
        binary_hash,
        namespace,
        release,
    )
    if args.role == "dev":
        _reconnect_guest(runner, args)
    print(f"deployed_commit={commit}")
    print(f"binary_sha256={binary_hash}")
    print(f"metrics_url={args.metrics_url}")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (OSError, RuntimeError, ValueError, TimeoutError) as error:
        print(f"error: {error}", file=sys.stderr)
        raise SystemExit(1)
