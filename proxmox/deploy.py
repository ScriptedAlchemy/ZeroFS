#!/usr/bin/env python3
"""Build and safely deploy ZeroFS into a private Proxmox LXC.

The coordinator runs on the ZeroFS build machine (VM100 in the documented
deployment).  Proxmox mutations are delegated to ``host-deploy.sh`` only after
an explicitly named legacy NBD consumer is quiesced and every
volatile/writeback tier is drained.
"""

from __future__ import annotations

import argparse
import base64
import contextlib
import hashlib
import ipaddress
import json
import math
import os
import re
import selectors
import shlex
import shutil
import signal
import subprocess
import sys
import tempfile
import time
import tomllib
import urllib.parse
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Iterator, Sequence

try:
    from proxmox import nfs_mount
except ModuleNotFoundError:
    import nfs_mount

LegacyNbdState = nfs_mount.LegacyNbdState
SharedNamespaceOwnershipReceipt = nfs_mount.SharedNamespaceOwnershipReceipt
parse_shared_namespace_ownership_receipt = (
    nfs_mount.parse_shared_namespace_ownership_receipt
)
plan_legacy_nbd_retirement = nfs_mount.plan_legacy_nbd_retirement
validate_shared_namespace_ownership = nfs_mount.validate_shared_namespace_ownership


VM_NFS_MOUNT_UNIT = r"mnt-zerofs\x2dfiles.mount"
VM_NFS_TEMPLATE_SOURCE = "10.10.10.30:/"
DEV_LEGACY_NBD_CLIENT_UNIT = "zerofs-nbd-client.service"
DEV_LEGACY_NBD_MOUNT_UNIT = "mnt-storagebox-nbd-pilot.mount"
DEV_LEGACY_NBD_MOUNTPOINT = "/mnt/storagebox-nbd-pilot"
DEV_LEGACY_NBD_SERVER_UNIT = "zerofs-nbd-pilot.service"
DEV_LEGACY_NBD_DEVICE = "/dev/nbd0"
OWNERSHIP_REPAIR_CONFIRMATION = "501:20"
VM100_VMID = 100
RFC1918_NETWORKS = tuple(
    ipaddress.ip_network(value)
    for value in ("10.0.0.0/8", "172.16.0.0/12", "192.168.0.0/16")
)
DECIMAL_GB = 1_000_000_000
MIB = 1024 * 1024
GIB = 1024 * MIB
PROD_UNIFIED_VOLATILE_MEMORY_GB = 16.0
PROD_FIXED_MEMORY_RESERVE_BYTES = 40 * GIB
SFTP_MAX_ACCOUNT_CONNECTIONS = 8
SFTP_SESSION_MAX_CONCURRENT_OPS = 16
SFTP_MAX_DIRECTION_CONCURRENCY = 64


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


def _require_private_listener(
    addresses: list[str],
    *,
    expected_ip: ipaddress.IPv4Address | ipaddress.IPv6Address,
    port: int,
    message: str,
) -> None:
    for address in addresses:
        host, found_port = _split_listener(address)
        if host != expected_ip or found_port != port:
            raise ValueError(message)


def _posix_identity(
    section: dict[str, object], label: str, *, nested: bool
) -> tuple[int, int]:
    value: object = section.get("shared_identity") if nested else section
    if not isinstance(value, dict):
        raise ValueError(f"{label} must configure numeric uid and gid")
    uid, gid = value.get("uid"), value.get("gid")
    if (
        not isinstance(uid, int)
        or isinstance(uid, bool)
        or not isinstance(gid, int)
        or isinstance(gid, bool)
        or uid < 0
        or gid < 0
    ):
        raise ValueError(f"{label} must configure non-negative numeric uid and gid")
    return uid, gid


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
        _require_private_listener(
            nbd_addresses,
            expected_ip=expected_ip,
            port=10809,
            message="NBD must listen only on the private container address at port 10809",
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
        nbd = servers.get("nbd")
        if not isinstance(nbd, dict):
            raise ValueError("prod requires the private NBD listener")
        nbd_addresses = _addresses(nbd)
        if not nbd_addresses:
            raise ValueError("prod NBD must have a private TCP listener")
        _require_private_listener(
            nbd_addresses,
            expected_ip=expected_ip,
            port=10809,
            message=(
                "prod NBD must listen only on the private container address at port 10809"
            ),
        )
        if nbd.get("unix_socket") != "/run/zerofs/nbd.sock":
            raise ValueError("prod requires the container-owned NBD Unix socket")
        if nbd.get("write_ack_mode") != "materialized":
            raise ValueError('prod NBD write_ack_mode must be "materialized"')
        ninep = servers.get("ninep")
        if (
            not isinstance(ninep, dict)
            or ninep.get("unix_socket") != "/run/zerofs/9p.sock"
        ):
            raise ValueError("prod requires a container-owned 9P Unix socket")
        ninep_addresses = _addresses(ninep)
        if not ninep_addresses:
            raise ValueError("prod 9P must have a private TCP listener")
        _require_private_listener(
            ninep_addresses,
            expected_ip=expected_ip,
            port=5564,
            message="9P must listen only on the private container address at port 5564",
        )
        nfs = servers.get("nfs")
        if not isinstance(nfs, dict):
            raise ValueError("prod requires the private NFS listener")
        nfs_addresses = _addresses(nfs)
        if not nfs_addresses:
            raise ValueError("prod NFS must have a private TCP listener")
        _require_private_listener(
            nfs_addresses,
            expected_ip=expected_ip,
            port=2049,
            message="NFS must listen only on the private container address at port 2049",
        )
        webui = servers.get("webui")
        if not isinstance(webui, dict):
            raise ValueError("prod requires the private WebUI listener")
        webui_addresses = _addresses(webui)
        if not webui_addresses:
            raise ValueError("prod WebUI must have a private TCP listener")
        _require_private_listener(
            webui_addresses,
            expected_ip=expected_ip,
            port=8080,
            message="WebUI must listen only on the private container address at port 8080",
        )
        identities = {
            _posix_identity(nfs, "NFS shared_identity", nested=True),
            _posix_identity(ninep, "9P shared_identity", nested=True),
            _posix_identity(webui, "WebUI", nested=False),
        }
        if len(identities) != 1:
            raise ValueError(
                "writable production frontends must use one shared identity"
            )
        if identities != {(501, 20)}:
            raise ValueError(
                "writable production frontends must use uid 501 and gid 20"
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
    _require_private_listener(
        metrics_addresses,
        expected_ip=expected_ip,
        port=9567,
        message=(
            "Prometheus must listen only on the private container address at port 9567"
        ),
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
    if role == "prod":
        if cache.get("memory_size_gb") != 32.0:
            raise ValueError("production clean cache memory_size_gb must be 32.0 GB")
        if writeback.get("memory_size_gb") != 4.0:
            raise ValueError("production writeback memory_size_gb must be 4.0 GB")
        runtime = settings.get("runtime")
        if not isinstance(runtime, dict) or "memory_limit_gb" not in runtime:
            raise ValueError("production [runtime] memory_limit_gb is required")
        runtime_gb = runtime["memory_limit_gb"]
        if (
            not isinstance(runtime_gb, (int, float))
            or isinstance(runtime_gb, bool)
            or not math.isfinite(runtime_gb)
            or runtime_gb <= 0
        ):
            raise ValueError(
                "production [runtime] memory_limit_gb must be finite and positive"
            )
        runtime_bytes = int(runtime_gb * DECIMAL_GB)
        minimum_runtime_bytes = (
            int(
                (
                    cache["memory_size_gb"]
                    + writeback["memory_size_gb"]
                    + PROD_UNIFIED_VOLATILE_MEMORY_GB
                )
                * DECIMAL_GB
            )
            + PROD_FIXED_MEMORY_RESERVE_BYTES
        )
        if runtime_bytes < minimum_runtime_bytes:
            raise ValueError(
                "production [runtime] memory_limit_gb does not cover the 16.0 GB "
                "unified volatile budget and 40 GiB safety reserves"
            )
        if memory_mb is not None and runtime_bytes > memory_mb * MIB:
            raise ValueError(
                f"production [runtime] memory_limit_gb exceeds container memory "
                f"{memory_mb} MiB"
            )
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
        if sftp.get("transport", "russh") != "russh" or any(
            field in sftp for field in ("hpn_program", "hpn_sha256")
        ):
            raise ValueError("[sftp] supports native russh only")
        max_connections = sftp.get("max_connections")
        if not isinstance(max_connections, int) or not (
            1 <= max_connections <= SFTP_MAX_ACCOUNT_CONNECTIONS
        ):
            raise ValueError(
                "SFTP max_connections must be between one and "
                f"{SFTP_MAX_ACCOUNT_CONNECTIONS}"
            )
        concurrency_ceiling = min(
            max_connections * SFTP_SESSION_MAX_CONCURRENT_OPS,
            SFTP_MAX_DIRECTION_CONCURRENCY,
        )
        for field in ("read_concurrency", "write_concurrency"):
            value = sftp.get(field)
            if not isinstance(value, int) or not 1 <= value <= concurrency_ceiling:
                raise ValueError(
                    f"SFTP {field} must be between one and {concurrency_ceiling} "
                    f"for max_connections = {max_connections}"
                )
    return storage_url


def render_nfs_bootstrap_config(source: str) -> str:
    excluded = ("servers.ninep", "servers.nbd", "servers.webui")
    rendered: list[str] = []
    keep = True
    for line in source.splitlines(keepends=True):
        match = re.match(r"^\s*\[([^]]+)]\s*(?:#.*)?$", line)
        if match:
            table = match.group(1)
            keep = not any(
                table == prefix or table.startswith(f"{prefix}.") for prefix in excluded
            )
        if keep:
            rendered.append(line)
    result = "".join(rendered)
    parsed = tomllib.loads(result)
    servers = parsed.get("servers")
    if not isinstance(servers, dict) or set(servers) != {"nfs", "rpc"}:
        raise ValueError("NFS bootstrap config must contain only NFS and RPC servers")
    if "prometheus" not in parsed:
        raise ValueError("NFS bootstrap config requires Prometheus health checks")
    return result


def require_replace_confirmation(ctid: int, confirmation: str | None) -> None:
    if confirmation != str(ctid):
        raise ValueError(
            f"replace is destructive; set ZEROFS_CONFIRM_REPLACE={ctid} exactly"
        )


def require_local_durable_upgrade_confirmation(
    ctid: int, flag: str | None, environment: str | None
) -> None:
    if flag != str(ctid):
        raise ValueError(
            f"local-durable upgrade requires --confirm-local-durable-upgrade {ctid}"
        )
    if environment != str(ctid):
        raise ValueError(
            f"local-durable upgrade requires ZEROFS_CONFIRM_LOCAL_DURABLE_UPGRADE={ctid}"
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


@dataclass(frozen=True)
class _ActiveLockedPayload:
    token: str
    pgid: int | None
    runtime_directory: str


class _FlockLease:
    def __init__(
        self,
        command: Sequence[str],
        *,
        dry_run: bool,
        display_command: Sequence[str] | None = None,
        force_cleanup_command: Callable[[str], Sequence[str]] | None = None,
    ) -> None:
        self.command = list(command)
        self.display_command = list(display_command or command)
        self.dry_run = dry_run
        self.force_cleanup_command = force_cleanup_command or (
            lambda script: ["bash", "-c", script]
        )
        self.process: subprocess.Popen[str] | None = None
        self.holder_pid: int | None = None
        self.runtime_root: str | None = None
        self._active_payloads: dict[str, _ActiveLockedPayload] = {}

    def __enter__(self) -> _FlockLease:
        print(f"+ acquire-lock {shell_join(self.display_command)}")
        if self.dry_run:
            return self
        process = subprocess.Popen(
            self.command,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            start_new_session=True,
        )
        self.process = process
        assert process.stdout is not None
        selector = selectors.DefaultSelector()
        selector.register(process.stdout, selectors.EVENT_READ)
        try:
            ready = selector.select(timeout=15)
        finally:
            selector.close()
        if ready:
            fields = process.stdout.readline().strip().split(" ")
            if fields[0] == "LOCKED" and len(fields) in (1, 2, 3):
                if len(fields) >= 2:
                    self.holder_pid = int(fields[1])
                if len(fields) == 3:
                    self.runtime_root = base64.b64decode(fields[2]).decode()
                return self
        try:
            _stdout, stderr = process.communicate(timeout=2)
        except subprocess.TimeoutExpired:
            process.terminate()
            _stdout, stderr = process.communicate(timeout=2)
        raise RuntimeError(
            "another ZeroFS deployment owns the deployment lock"
            + (f": {stderr.strip()}" if stderr.strip() else "")
        )

    def __exit__(
        self,
        _exc_type: type[BaseException] | None,
        exc: BaseException | None,
        _traceback: object,
    ) -> None:
        if self.process is None:
            return
        process = self.process
        try:
            returncode = self._shutdown_holder(graceful_timeout=10)
        except BaseException as cleanup_error:
            if exc is not None:
                exc.add_note(f"deployment lock cleanup failed: {cleanup_error}")
                return
            raise
        finally:
            if process.stdout is not None:
                process.stdout.close()
            if process.stderr is not None:
                process.stderr.close()
        if returncode != 0:
            detail = f"deployment lock process exited {returncode}"
            if exc is not None:
                exc.add_note(detail)
                return
            raise RuntimeError(detail)

    def execute(self, script: str) -> subprocess.CompletedProcess[str]:
        if self.dry_run:
            print("+ locked-shell")
            for line in script.rstrip().splitlines():
                print(f"  | {line}")
            return subprocess.CompletedProcess(
                ["locked-remote-shell"], 0, "", ""
            )
        process = self.process
        if process is None or process.poll() is not None:
            raise RuntimeError("deployment lock lease was lost")
        assert process.stdin is not None and process.stdout is not None
        token = hashlib.sha256(os.urandom(32)).hexdigest()
        payload = base64.b64encode(script.encode()).decode()
        if self.runtime_root is not None:
            runtime_directory = os.path.join(
                self.runtime_root,
                f"zerofs-lock-command-{token}",
            )
            self._active_payloads[token] = _ActiveLockedPayload(
                token=token,
                pgid=None,
                runtime_directory=runtime_directory,
            )
        process.stdin.write(f"RUN {token} {payload}\n")
        process.stdin.flush()
        result_marker = f"__ZEROFS_LOCK_RESULT__ {token} "
        started_marker = f"__ZEROFS_LOCK_STARTED__ {token} "
        try:
            while True:
                line = process.stdout.readline()
                if not line:
                    raise RuntimeError("deployment lock lease was lost during command")
                if line.startswith(started_marker):
                    self._record_started(token, line, started_marker)
                    process.stdin.write(f"ACK {token}\n")
                    process.stdin.flush()
                    continue
                if line.startswith(result_marker):
                    self._active_payloads.pop(token, None)
                    return self._parse_result(line, result_marker)
        except KeyboardInterrupt as interrupt:
            self._cancel_active(
                token,
                result_marker,
                started_marker,
                interrupt,
            )
            raise

    def _record_started(
        self,
        token: str,
        line: str,
        marker: str,
    ) -> None:
        pgid_text, runtime_encoded = (
            line.removeprefix(marker).rstrip("\n").split(" ", 1)
        )
        pgid = int(pgid_text)
        if pgid <= 1:
            raise RuntimeError(
                "deployment lock holder reported an invalid payload PGID"
            )
        runtime_directory = base64.b64decode(runtime_encoded).decode()
        pending = self._active_payloads.get(token)
        if (
            pending is not None
            and pending.runtime_directory != runtime_directory
        ):
            raise RuntimeError("deployment lock runtime ownership changed")
        self._active_payloads[token] = _ActiveLockedPayload(
            token=token,
            pgid=pgid,
            runtime_directory=runtime_directory,
        )

    @staticmethod
    def _parse_result(
        line: str, marker: str
    ) -> subprocess.CompletedProcess[str]:
        status_text, stdout_encoded, stderr_encoded = (
            line.removeprefix(marker).rstrip("\n").split(" ", 2)
        )
        status = int(status_text)
        stdout = base64.b64decode(stdout_encoded).decode()
        stderr = base64.b64decode(stderr_encoded).decode()
        if status != 0:
            raise LockedRemoteCommandError(status, stdout, stderr)
        return subprocess.CompletedProcess(
            ["locked-remote-shell"], status, stdout, stderr
        )

    def _readline(self, timeout: float) -> str:
        process = self.process
        if process is None or process.stdout is None:
            return ""
        selector = selectors.DefaultSelector()
        selector.register(process.stdout, selectors.EVENT_READ)
        try:
            if not selector.select(timeout=timeout):
                raise TimeoutError("deployment lock holder response timed out")
            return process.stdout.readline()
        finally:
            selector.close()

    def _cancel_active(
        self,
        token: str,
        result_marker: str,
        started_marker: str,
        interrupt: KeyboardInterrupt,
    ) -> None:
        process = self.process
        if process is None or process.stdin is None or process.stdin.closed:
            interrupt.add_note("deployment lock holder was unavailable for cancellation")
            return
        try:
            process.stdin.write(f"CANCEL {token}\n")
            process.stdin.flush()
        except (BrokenPipeError, OSError) as error:
            interrupt.add_note(f"deployment command cancellation failed: {error}")
            self._shutdown_without_masking(interrupt)
            return
        deadline = time.monotonic() + 5
        try:
            while True:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise TimeoutError("deployment command cancellation timed out")
                line = self._readline(remaining)
                if not line:
                    interrupt.add_note(
                        "deployment lock holder exited during command cancellation"
                    )
                    return
                if line.startswith(started_marker):
                    self._record_started(token, line, started_marker)
                    continue
                if line.startswith(result_marker):
                    self._active_payloads.pop(token, None)
                    return
        except KeyboardInterrupt:
            interrupt.add_note(
                "second interrupt received during deployment command cancellation"
            )
            self._shutdown_without_masking(interrupt)
            raise interrupt
        except TimeoutError as error:
            interrupt.add_note(str(error))
            self._shutdown_without_masking(interrupt)

    def _shutdown_without_masking(self, original: BaseException) -> None:
        try:
            self._shutdown_holder(graceful_timeout=5)
        except BaseException as cleanup_error:
            original.add_note(f"deployment lock cleanup failed: {cleanup_error}")

    def _force_cleanup(self, *, include_holder: bool) -> None:
        if not self._active_payloads and not (
            include_holder and self.holder_pid is not None
        ):
            return
        script = _remote_lock_cleanup(
            self.holder_pid if include_holder else None,
            tuple(self._active_payloads.values()),
        )
        command = list(self.force_cleanup_command(script))
        try:
            result = subprocess.run(
                command,
                check=False,
                text=True,
                capture_output=True,
                timeout=15,
                start_new_session=True,
            )
        except subprocess.TimeoutExpired as error:
            raise RuntimeError("deployment lock forced cleanup timed out") from error
        if result.returncode != 0:
            details = []
            if result.stdout:
                details.append(f"stdout: {result.stdout.strip()}")
            if result.stderr:
                details.append(f"stderr: {result.stderr.strip()}")
            raise RuntimeError(
                f"deployment lock forced cleanup exited {result.returncode}"
                + (f": {'; '.join(details)}" if details else "")
            )
        self._active_payloads.clear()

    def _shutdown_holder(self, *, graceful_timeout: float) -> int:
        process = self.process
        if process is None:
            return 0
        if process.stdin is not None and not process.stdin.closed:
            process.stdin.close()
        try:
            returncode = process.wait(timeout=graceful_timeout)
        except subprocess.TimeoutExpired:
            self._force_cleanup(include_holder=True)
            try:
                returncode = process.wait(timeout=3)
            except subprocess.TimeoutExpired:
                try:
                    os.killpg(process.pid, signal.SIGTERM)
                except ProcessLookupError:
                    pass
                try:
                    returncode = process.wait(timeout=3)
                except subprocess.TimeoutExpired:
                    try:
                        os.killpg(process.pid, signal.SIGKILL)
                    except ProcessLookupError:
                        pass
                    returncode = process.wait(timeout=2)
        if self._active_payloads:
            self._force_cleanup(include_holder=False)
        return returncode


class LockedRemoteCommandError(RuntimeError):
    def __init__(self, returncode: int, stdout: str, stderr: str) -> None:
        self.returncode = returncode
        self.stdout = stdout
        self.stderr = stderr
        details = []
        if stdout:
            details.append(f"stdout: {stdout.strip()}")
        if stderr:
            details.append(f"stderr: {stderr.strip()}")
        super().__init__(
            f"locked remote command exited {returncode}"
            + (f": {'; '.join(details)}" if details else "")
        )


def _remote_lock_cleanup(
    holder_pid: int | None,
    payloads: Sequence[_ActiveLockedPayload],
) -> str:
    encoded_payloads = base64.b64encode(
        json.dumps(
            [
                {
                    "token": payload.token,
                    "pgid": payload.pgid,
                    "runtime_directory": payload.runtime_directory,
                }
                for payload in payloads
            ],
            separators=(",", ":"),
        ).encode()
    ).decode()
    program = r'''
import base64
import json
import os
import shutil
import signal
import sys
import tempfile
import time
from pathlib import Path


def process_state(pid):
    try:
        stat = Path(f"/proc/{pid}/stat").read_text()
    except (FileNotFoundError, ProcessLookupError):
        return None
    return stat[stat.rfind(")") + 2:].split()[0]


def pid_is_live(pid):
    state = process_state(pid)
    return state is not None and state != "Z"


def process_group_exists(pgid):
    try:
        os.killpg(pgid, 0)
    except ProcessLookupError:
        return False
    return True


def group_has_live_members(pgid):
    for entry in Path("/proc").iterdir():
        if not entry.name.isdigit():
            continue
        try:
            fields = (entry / "stat").read_text()
            fields = fields[fields.rfind(")") + 2:].split()
            state = fields[0]
            process_group = int(fields[2])
        except (FileNotFoundError, ProcessLookupError, ValueError, IndexError):
            continue
        if process_group == pgid and state != "Z":
            return True
    return False


def wait_while(predicate, timeout):
    deadline = time.monotonic() + timeout
    while predicate() and time.monotonic() < deadline:
        time.sleep(0.05)


def stop_holder(pid):
    if pid and pid_is_live(pid):
        try:
            os.kill(pid, signal.SIGSTOP)
        except ProcessLookupError:
            pass


def validate_payload(item):
    token = item["token"]
    requested_pgid = item["pgid"]
    runtime = Path(item["runtime_directory"])
    if (
        len(token) != 64
        or any(character not in "0123456789abcdef" for character in token)
        or (
            requested_pgid is not None
            and (
                not isinstance(requested_pgid, int)
                or requested_pgid <= 1
            )
        )
    ):
        raise RuntimeError("invalid forced-cleanup payload identity")
    temp_root = Path(tempfile.gettempdir()).resolve()
    if (
        not runtime.is_absolute()
        or runtime.parent.resolve() != temp_root
        or runtime.name != f"zerofs-lock-command-{token}"
        or runtime.is_symlink()
    ):
        raise RuntimeError("unsafe forced-cleanup runtime directory")
    marker_pgid = None
    if runtime.exists():
        owner = json.loads((runtime / "owner.json").read_text())
        if owner.get("token") != token:
            raise RuntimeError("forced-cleanup payload ownership changed")
        marker_pgid = owner.get("pgid")
        if marker_pgid is not None and (
            not isinstance(marker_pgid, int) or marker_pgid <= 1
        ):
            raise RuntimeError("invalid forced-cleanup marker PGID")
        if (
            requested_pgid is not None
            and marker_pgid is not None
            and requested_pgid != marker_pgid
        ):
            raise RuntimeError("forced-cleanup payload PGID changed")
    elif requested_pgid is not None and process_group_exists(requested_pgid):
        raise RuntimeError("payload runtime disappeared while its group survived")
    return requested_pgid or marker_pgid, runtime


def terminate_payload_group(pgid):
    if process_group_exists(pgid):
        try:
            os.killpg(pgid, signal.SIGTERM)
            os.killpg(pgid, signal.SIGCONT)
        except ProcessLookupError:
            pass
    wait_while(lambda: group_has_live_members(pgid), 2)
    if group_has_live_members(pgid):
        try:
            os.killpg(pgid, signal.SIGKILL)
        except ProcessLookupError:
            pass
    wait_while(lambda: group_has_live_members(pgid), 2)
    if group_has_live_members(pgid):
        raise RuntimeError(f"payload process group {pgid} survived SIGKILL")


def terminate_holder(pid):
    if not pid:
        return
    if pid_is_live(pid):
        try:
            os.kill(pid, signal.SIGTERM)
            os.kill(pid, signal.SIGCONT)
        except ProcessLookupError:
            return
    wait_while(lambda: pid_is_live(pid), 2)
    if pid_is_live(pid):
        try:
            os.kill(pid, signal.SIGKILL)
        except ProcessLookupError:
            return
    wait_while(lambda: pid_is_live(pid), 2)
    if pid_is_live(pid):
        raise RuntimeError(f"lock holder {pid} survived SIGKILL")


def main():
    holder_pid = int(sys.argv[1]) if sys.argv[1] != "-" else None
    payloads = json.loads(base64.b64decode(sys.argv[2]))
    stop_holder(holder_pid)
    validated = [validate_payload(item) for item in payloads]
    for pgid, _runtime in validated:
        if pgid is not None:
            terminate_payload_group(pgid)
    for _pgid, runtime in validated:
        if runtime.exists():
            shutil.rmtree(runtime)
    terminate_holder(holder_pid)
    for pgid, _runtime in validated:
        if pgid is None:
            continue
        wait_while(lambda pgid=pgid: process_group_exists(pgid), 2)
        if process_group_exists(pgid):
            raise RuntimeError(f"payload process group {pgid} was not reaped")


main()
'''
    holder = str(holder_pid) if holder_pid is not None else "-"
    return (
        f"exec python3 -u -c {shlex.quote(program)} "
        f"{shlex.quote(holder)} {shlex.quote(encoded_payloads)}"
    )


def _remote_lock_holder(path: str) -> str:
    program = r'''
import base64
import fcntl
import json
import os
import selectors
import shutil
import signal
import subprocess
import sys
import tempfile
import time
from pathlib import Path


PAYLOAD_LAUNCHER = """
import ctypes
import os
import signal
import sys

expected_parent = int(sys.argv[1])
libc = ctypes.CDLL(None, use_errno=True)
if libc.prctl(1, signal.SIGKILL, 0, 0, 0) != 0:
    raise OSError(ctypes.get_errno(), "prctl(PR_SET_PDEATHSIG) failed")
if os.getppid() != expected_parent:
    raise SystemExit(125)
os.kill(os.getpid(), signal.SIGSTOP)
if os.getppid() != expected_parent:
    raise SystemExit(125)
os.execvp("bash", ["bash", "-se"])
"""


def process_group_exists(pgid):
    try:
        os.killpg(pgid, 0)
    except ProcessLookupError:
        return False
    return True


def process_state(pid):
    try:
        stat = Path(f"/proc/{pid}/stat").read_text()
    except (FileNotFoundError, ProcessLookupError):
        return None
    return stat[stat.rfind(")") + 2:].split()[0]


def terminate_payload(process):
    pgid = process.pid
    if process_group_exists(pgid):
        try:
            os.killpg(pgid, signal.SIGTERM)
            os.killpg(pgid, signal.SIGCONT)
        except ProcessLookupError:
            pass
    deadline = time.monotonic() + 2
    while process_group_exists(pgid) and time.monotonic() < deadline:
        time.sleep(0.05)
    if process_group_exists(pgid):
        try:
            os.killpg(pgid, signal.SIGKILL)
        except ProcessLookupError:
            pass
    try:
        process.wait(timeout=2)
    except subprocess.TimeoutExpired:
        try:
            os.killpg(pgid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        process.wait(timeout=2)


def write_owner(directory, token, pgid):
    owner_path = Path(directory, "owner.json")
    temporary_path = Path(directory, "owner.json.tmp")
    with temporary_path.open("w") as handle:
        json.dump({"token": token, "pgid": pgid}, handle, separators=(",", ":"))
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temporary_path, owner_path)
    descriptor = os.open(directory, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def wait_for_payload_stop(process):
    deadline = time.monotonic() + 2
    while time.monotonic() < deadline:
        state = process_state(process.pid)
        if state in ("T", "t"):
            return
        if state is None or process.poll() is not None:
            raise RuntimeError("payload launcher exited before ownership handshake")
        time.sleep(0.01)
    raise RuntimeError("payload launcher did not stop for ownership handshake")


def stop_holder(signum, _frame):
    raise SystemExit(128 + signum)


def emit_result(token, status, stdout_path, stderr_path):
    stdout = base64.b64encode(stdout_path.read_bytes()).decode()
    stderr = base64.b64encode(stderr_path.read_bytes()).decode()
    print(
        f"__ZEROFS_LOCK_RESULT__ {token} {status} {stdout} {stderr}",
        flush=True,
    )


def main():
    signal.signal(signal.SIGTERM, stop_holder)
    signal.signal(signal.SIGHUP, stop_holder)
    lock_handle = open(sys.argv[1], "w")
    try:
        fcntl.flock(lock_handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        raise SystemExit(75)
    runtime_root = tempfile.gettempdir()
    encoded_root = base64.b64encode(runtime_root.encode()).decode()
    print(f"LOCKED {os.getpid()} {encoded_root}", flush=True)
    active = None
    directory = None
    try:
        while True:
            request = sys.stdin.readline()
            if not request:
                return
            action, token, payload = (request.rstrip("\n").split(" ", 2) + [""])[:3]
            if action == "CANCEL":
                continue
            if action != "RUN" or not token or not payload:
                raise RuntimeError("invalid deployment lock request")
            script = base64.b64decode(payload)
            directory = os.path.join(
                runtime_root,
                f"zerofs-lock-command-{token}",
            )
            os.mkdir(directory, mode=0o700)
            try:
                write_owner(directory, token, None)
                stdout_path = os.path.join(directory, "stdout")
                stderr_path = os.path.join(directory, "stderr")
                script_path = os.path.join(directory, "script")
                with open(script_path, "wb") as handle:
                    handle.write(script)
                    handle.flush()
                    os.fsync(handle.fileno())
                with (
                    open(script_path, "rb") as stdin_handle,
                    open(stdout_path, "wb") as stdout_handle,
                    open(stderr_path, "wb") as stderr_handle,
                ):
                    active = subprocess.Popen(
                        [
                            sys.executable,
                            "-c",
                            PAYLOAD_LAUNCHER,
                            str(os.getpid()),
                        ],
                        stdin=stdin_handle,
                        stdout=stdout_handle,
                        stderr=stderr_handle,
                        start_new_session=True,
                        close_fds=True,
                    )
                wait_for_payload_stop(active)
                write_owner(directory, token, active.pid)
                runtime = base64.b64encode(directory.encode()).decode()
                print(
                    f"__ZEROFS_LOCK_STARTED__ {token} {active.pid} {runtime}",
                    flush=True,
                )
                control = sys.stdin.readline()
                if not control:
                    terminate_payload(active)
                    active = None
                    return
                fields = control.rstrip("\n").split(" ", 2)
                cancelled = fields[:2] == ["CANCEL", token]
                if fields[:2] == ["ACK", token]:
                    os.killpg(active.pid, signal.SIGCONT)
                elif cancelled:
                    terminate_payload(active)
                else:
                    raise RuntimeError("invalid deployment lock start response")
                selector = selectors.DefaultSelector()
                selector.register(sys.stdin, selectors.EVENT_READ)
                try:
                    if cancelled:
                        status = 130
                    else:
                        while active.poll() is None:
                            if not selector.select(timeout=0.1):
                                continue
                            control = sys.stdin.readline()
                            if not control:
                                terminate_payload(active)
                                active = None
                                return
                            fields = control.rstrip("\n").split(" ", 2)
                            if fields[:2] == ["CANCEL", token]:
                                terminate_payload(active)
                                cancelled = True
                                break
                        status = 130 if cancelled else active.wait()
                finally:
                    selector.close()
                active = None
                emit_result(
                    token,
                    status,
                    Path(stdout_path),
                    Path(stderr_path),
                )
            finally:
                if active is not None:
                    terminate_payload(active)
                    active = None
                shutil.rmtree(directory, ignore_errors=True)
                directory = None
    finally:
        if active is not None:
            terminate_payload(active)
        if directory is not None:
            shutil.rmtree(directory, ignore_errors=True)
        lock_handle.close()


main()
'''
    return f"exec python3 -u -c {shlex.quote(program)} {shlex.quote(path)}"


@dataclass(frozen=True)
class LocalVmIdentity:
    vmid: int
    name: str
    smbios_uuid: str


def validate_vm_transport_args(args: argparse.Namespace) -> None:
    if args.vm_transport == "local":
        if args.vm_vmid != VM100_VMID:
            raise ValueError("local VM transport requires --vm-vmid 100")
        return
    if args.vm_vmid is not None:
        raise ValueError("--vm-vmid is valid only with --vm-transport local")


def _parse_pve_vm_identity(config: str, vmid: int) -> LocalVmIdentity:
    name: str | None = None
    smbios_uuid: str | None = None
    for line in config.splitlines():
        key, separator, value = line.partition(":")
        if not separator:
            continue
        if key == "name":
            name = value.strip()
        elif key == "smbios1":
            for field in value.split(","):
                field_key, equals, field_value = field.strip().partition("=")
                if equals and field_key == "uuid":
                    smbios_uuid = field_value.strip().lower()
    if not name or not smbios_uuid:
        raise RuntimeError(
            f"Proxmox VM {vmid} must expose both name and smbios1 UUID"
        )
    return LocalVmIdentity(vmid=vmid, name=name, smbios_uuid=smbios_uuid)


def verify_local_vm_identity(
    runner: Runner, args: argparse.Namespace
) -> LocalVmIdentity:
    validate_vm_transport_args(args)
    config = runner.probe(
        [
            "ssh",
            "-o",
            "BatchMode=yes",
            args.pve_host,
            "qm",
            "config",
            str(args.vm_vmid),
            "--current",
        ],
        capture=True,
    ).stdout
    identity = _parse_pve_vm_identity(config, args.vm_vmid)
    local_name = runner.probe(["hostname"], capture=True).stdout.strip()
    local_uuid = runner.probe(
        ["sudo", "cat", "/sys/class/dmi/id/product_uuid"], capture=True
    ).stdout.strip().lower()
    if local_name != identity.name:
        raise RuntimeError(
            f"local hostname {local_name!r} does not match Proxmox VM "
            f"{identity.vmid} name {identity.name!r}"
        )
    if local_uuid != identity.smbios_uuid:
        raise RuntimeError(
            f"local SMBIOS UUID {local_uuid!r} does not match Proxmox VM "
            f"{identity.vmid} SMBIOS UUID {identity.smbios_uuid!r}"
        )
    return identity


class Runner:
    def __init__(self, dry_run: bool) -> None:
        self.dry_run = dry_run
        self.vm_transport = "ssh"
        self.local_vm_host: str | None = None
        self._active_leases: list[_FlockLease] = []
        self._remote_leases: dict[str, _FlockLease] = {}

    def configure_vm_transport(self, args: argparse.Namespace) -> None:
        validate_vm_transport_args(args)
        if args.vm_transport == "ssh":
            return
        identity = verify_local_vm_identity(self, args)
        print(
            f"local VM identity verified: vmid={identity.vmid} "
            f"name={identity.name} smbios_uuid={identity.smbios_uuid}"
        )
        self.vm_transport = "local"
        self.local_vm_host = args.vm_host

    def is_local_vm_host(self, host: str) -> bool:
        return self.vm_transport == "local" and host == self.local_vm_host

    def _assert_leases_held(self) -> None:
        if self.dry_run:
            return
        for lease in self._active_leases:
            if lease.process is None or lease.process.poll() is not None:
                raise RuntimeError("deployment lock lease was lost")

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
        self._assert_leases_held()
        result = subprocess.run(
            list(command),
            cwd=cwd,
            check=True,
            text=True,
            input=input_text,
            capture_output=capture,
        )
        self._assert_leases_held()
        return result

    def probe(
        self,
        command: Sequence[str],
        *,
        capture: bool = False,
    ) -> subprocess.CompletedProcess[str]:
        print(f"+ probe {shell_join(command)}")
        return subprocess.run(
            list(command),
            check=True,
            text=True,
            capture_output=capture,
        )

    @contextlib.contextmanager
    def remote_deployment_locks(self, args: argparse.Namespace) -> Iterator[None]:
        locks = (
            (
                args.vm_host,
                "/run/lock/zerofs-vm-nfs-global.coordinator.lock",
                True,
            ),
        )
        with contextlib.ExitStack() as stack:
            for host, path, sudo in locks:
                lock_script = _remote_lock_holder(path)
                remote = (
                    f"sudo bash -c {shlex.quote(lock_script)}"
                    if sudo
                    else f"bash -c {shlex.quote(lock_script)}"
                )
                if self.is_local_vm_host(host):
                    if self.dry_run:
                        self.probe(
                            [
                                "sudo",
                                "bash",
                                "-c",
                                f"test ! -e {shlex.quote(path)} || "
                                f"exec flock -n {shlex.quote(path)} true",
                            ],
                            capture=True,
                        )
                    command = ["sudo", "bash", "-c", lock_script]
                else:
                    command = [
                        "ssh",
                        "-o",
                        "BatchMode=yes",
                        "-o",
                        "ServerAliveInterval=15",
                        "-o",
                        "ServerAliveCountMax=3",
                        host,
                        remote,
                    ]
                if self.is_local_vm_host(host):
                    force_cleanup_command = lambda script: [
                        "sudo",
                        "bash",
                        "-c",
                        script,
                    ]
                else:
                    force_cleanup_command = lambda script, host=host: [
                        "ssh",
                        "-o",
                        "BatchMode=yes",
                        "-o",
                        "ServerAliveInterval=15",
                        "-o",
                        "ServerAliveCountMax=3",
                        host,
                        f"sudo bash -c {shlex.quote(script)}",
                    ]
                display_command = (
                    ["local-vm-lock-holder", path]
                    if self.is_local_vm_host(host)
                    else ["ssh", host, "vm-lock-holder", path]
                )
                lease = stack.enter_context(
                    _FlockLease(
                        command,
                        dry_run=self.dry_run,
                        display_command=display_command,
                        force_cleanup_command=force_cleanup_command,
                    )
                )
                self._active_leases.append(lease)
                stack.callback(self._active_leases.remove, lease)
                self._remote_leases[host] = lease
                stack.callback(self._remote_leases.pop, host)
            yield

    def run_remote_shell(
        self, host: str, script: str
    ) -> subprocess.CompletedProcess[str] | str | None:
        lease = self._remote_leases.get(host)
        if lease is not None:
            return lease.execute(script)
        if self.dry_run:
            return None
        if self.is_local_vm_host(host):
            return self.run(
                ["bash", "-se"],
                input_text=script,
                capture=True,
            )
        return None


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
    result = runner.run_remote_shell(host, script)
    if result is not None:
        stdout = result if isinstance(result, str) else result.stdout
        stderr = "" if isinstance(result, str) else result.stderr
        if stdout:
            sys.stdout.write(stdout)
            sys.stdout.flush()
        if stderr:
            sys.stderr.write(stderr)
            sys.stderr.flush()
        return
    if runner.is_local_vm_host(host):
        runner.run(["bash", "-se"], input_text=script)
        return
    runner.run(["ssh", "-o", "BatchMode=yes", host, "bash", "-se"], input_text=script)


def _ssh_capture(runner: Runner, host: str, script: str) -> str:
    result = runner.run_remote_shell(host, script)
    if isinstance(result, str):
        return result
    if result is None and runner.is_local_vm_host(host):
        result = runner.run(
            ["bash", "-se"], input_text=script, capture=True
        )
    elif result is None:
        result = runner.run(
            ["ssh", "-o", "BatchMode=yes", host, "bash", "-se"],
            input_text=script,
            capture=True,
        )
    if result.stderr:
        sys.stderr.write(result.stderr)
        sys.stderr.flush()
    return result.stdout


def _stage_remote_file(
    runner: Runner, host: str, local_path: Path, remote_path: str
) -> None:
    if runner.dry_run:
        ownership = "locked-plan" if host in runner._remote_leases else "plan"
        print(f"+ {ownership} stage-file {local_path} {host}:{remote_path}")
        return
    encoded = base64.b64encode(local_path.read_bytes()).decode("ascii")
    remote = shlex.quote(remote_path)
    temporary = shlex.quote(f"{remote_path}.tmp")
    _ssh(
        runner,
        host,
        f"""set -euo pipefail
trap 'rm -f -- {temporary}' EXIT
printf '%s' {shlex.quote(encoded)} | base64 -d > {temporary}
chmod 0600 {temporary}
mv -f -- {temporary} {remote}
trap - EXIT
""",
    )


def _quiesce_guest(runner: Runner, args: argparse.Namespace) -> None:
    script = f"""set -euo pipefail
unit_loaded() {{
  test "$(systemctl show -p LoadState --value "$1" 2>/dev/null || true)" != not-found
}}
if findmnt -rn -M {shlex.quote(args.source_mountpoint)} >/dev/null 2>&1; then
  source_device=$(findmnt -nro SOURCE -M {shlex.quote(args.source_mountpoint)})
  if test "$source_device" != {shlex.quote(DEV_LEGACY_NBD_DEVICE)}; then
    echo "unexpected legacy NBD mount source: $source_device" >&2
    exit 1
  fi
  sudo sync -f {shlex.quote(args.source_mountpoint)}
fi
if unit_loaded {shlex.quote(args.source_mount_unit)}; then
  sudo systemctl disable --now {shlex.quote(args.source_mount_unit)}
  if systemctl is-enabled --quiet {shlex.quote(args.source_mount_unit)}; then
    echo 'legacy NBD mount remains enabled; refusing reboot persistence' >&2
    exit 1
  fi
  test "$(systemctl is-active {shlex.quote(args.source_mount_unit)} 2>/dev/null || true)" != active
fi
if findmnt -rn -M {shlex.quote(args.source_mountpoint)} >/dev/null 2>&1; then
  echo 'mount remained active; refusing to disconnect NBD' >&2
  exit 1
fi
if unit_loaded {shlex.quote(args.source_client_unit)}; then
  sudo systemctl disable --now {shlex.quote(args.source_client_unit)}
  if systemctl is-enabled --quiet {shlex.quote(args.source_client_unit)}; then
    echo 'legacy NBD client remains enabled; refusing reboot persistence' >&2
    exit 1
  fi
  test "$(systemctl is-active {shlex.quote(args.source_client_unit)} 2>/dev/null || true)" != active
fi
nbd_pid=$(cat /sys/class/block/nbd0/pid 2>/dev/null || true)
if test -n "$nbd_pid"; then
  echo 'nbd0 remains connected; pass the actual source client unit' >&2
  exit 1
fi
"""
    _ssh(runner, args.vm_host, script)


def _has_legacy_nbd_source(args: argparse.Namespace) -> bool:
    source = (
        args.source_client_unit,
        args.source_mount_unit,
        args.source_mountpoint,
        args.source_server_unit,
    )
    if not any(source):
        return False
    if not all(source):
        raise ValueError(
            "legacy NBD quiescing requires all four --source-* options together"
        )
    expected = (
        DEV_LEGACY_NBD_CLIENT_UNIT,
        DEV_LEGACY_NBD_MOUNT_UNIT,
        DEV_LEGACY_NBD_MOUNTPOINT,
        DEV_LEGACY_NBD_SERVER_UNIT,
    )
    if args.role != "dev" or source != expected:
        raise ValueError(
            "--source-* may target only the documented legacy NBD pilot; "
            "production NFS units and mountpoints are never migration sources"
        )
    return True


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
        output = _ssh_capture(
            runner,
            args.vm_host,
            f"set -euo pipefail\n{shell_join(command)}\n",
        )
        last = parse_drain_state(output)
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


def _wait_recovered_server_ready(runner: Runner, args: argparse.Namespace) -> None:
    timeout = min(args.drain_timeout, 600)
    if runner.dry_run:
        print(
            f"+ wait-for-recovered-server {args.existing_metrics_url} "
            f"timeout={timeout}s"
        )
        return
    metrics_url = shlex.quote(args.existing_metrics_url)
    script = f"""set -euo pipefail
deadline=$((SECONDS + {timeout}))
while ((SECONDS < deadline)); do
  if curl --fail --silent --show-error --max-time 10 {metrics_url} >/dev/null 2>&1; then
    exit 0
  fi
  sleep 2
done
echo "recovered ZeroFS server did not become ready within {timeout} seconds" >&2
exit 1
"""
    _ssh(runner, args.vm_host, script)


def _stop_source_server(runner: Runner, args: argparse.Namespace) -> None:
    if args.source_server_unit is None:
        return
    script = f"""set -euo pipefail
sudo systemctl stop {shlex.quote(args.source_server_unit)}
test "$(systemctl is-active {shlex.quote(args.source_server_unit)} 2>/dev/null || true)" != active
"""
    _ssh(runner, args.vm_host, script)


@contextlib.contextmanager
def _staged_host_directory(runner: Runner, args: argparse.Namespace, stage: str):
    """Create a remote scratch stage directory and guarantee its removal.

    The stage is created up front and always removed on the way out, whether
    the body inside the ``with`` block succeeds, raises, or is interrupted.
    """
    _ssh(
        runner,
        args.pve_host,
        f"set -euo pipefail\ninstall -d -m 0700 {shlex.quote(stage)}\n",
    )
    try:
        yield stage
    finally:
        _ssh(
            runner,
            args.pve_host,
            f"set -euo pipefail\nrm -rf -- {shlex.quote(stage)}\n",
        )


def _build_host_deploy_argv(
    *,
    remote_script: str,
    action: str,
    role: str,
    stage: str,
    args: argparse.Namespace,
    commit: str,
    binary_hash: str,
    namespace: str,
    release: str,
    include_drain_timeout: bool,
    extra_flags: Sequence[str] = (),
) -> list[str]:
    host_args = [
        "bash",
        remote_script,
        action,
        "--role",
        role,
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
    if include_drain_timeout:
        host_args.extend(["--drain-timeout", str(args.drain_timeout)])
    host_args.extend(extra_flags)
    return host_args


def _stage_and_run_host(
    runner: Runner,
    args: argparse.Namespace,
    binary: Path,
    commit: str,
    binary_hash: str,
    namespace: str,
    release: str,
    *,
    defer_commit: bool = False,
    config_path: Path | None = None,
    maintenance_nfs_only: bool = False,
) -> None:
    bundle = Path(__file__).resolve().parent
    stage = f"/var/tmp/zerofs-lxc-deploy-{args.ctid}-{commit[:12]}"
    files: tuple[tuple[Path, str], ...]
    if args.action == "cleanup":
        files = ((bundle / "host-deploy.sh", "host-deploy.sh"),)
    else:
        files = (
            (binary, "zerofs"),
            (config_path or args.config, "zerofs.toml"),
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
    extra_flags: list[str] = []
    if args.local_durable_upgrade:
        extra_flags.append("--local-durable-upgrade")
    if args.dry_run:
        extra_flags.append("--dry-run")
    if defer_commit:
        extra_flags.append("--defer-commit")
    if maintenance_nfs_only:
        extra_flags.append("--maintenance-nfs-only")
    if args.action == "replace":
        extra_flags.extend(["--confirm-replace", str(args.ctid)])
    with _staged_host_directory(runner, args, stage):
        for path, destination in files:
            runner.run(
                ["scp", "-q", str(path), f"{args.pve_host}:{stage}/{destination}"]
            )
        host_args = _build_host_deploy_argv(
            remote_script=f"{stage}/host-deploy.sh",
            action=args.action,
            role=args.role,
            stage=stage,
            args=args,
            commit=commit,
            binary_hash=binary_hash,
            namespace=namespace,
            release=release,
            include_drain_timeout=True,
            extra_flags=extra_flags,
        )
        runner.run(["ssh", "-o", "BatchMode=yes", args.pve_host, *host_args])


def _run_host_deployment_control(
    runner: Runner,
    args: argparse.Namespace,
    action: str,
    commit: str,
    binary_hash: str,
    namespace: str,
    release: str,
) -> None:
    if action not in {"promote", "finalize", "commit", "rollback", "recover"}:
        raise ValueError(f"invalid host deployment control: {action}")
    bundle = Path(__file__).resolve().parent
    stage = f"/var/tmp/zerofs-lxc-control-{args.ctid}-{release}"
    remote_script = f"{stage}/host-deploy.sh"
    with _staged_host_directory(runner, args, stage):
        runner.run(
            [
                "scp",
                "-q",
                str(bundle / "host-deploy.sh"),
                f"{args.pve_host}:{remote_script}",
            ]
        )
        if action == "promote":
            runner.run(
                [
                    "scp",
                    "-q",
                    str(args.config),
                    f"{args.pve_host}:{stage}/zerofs.toml",
                ]
            )
        extra_flags = ["--dry-run"] if args.dry_run else []
        host_args = _build_host_deploy_argv(
            remote_script=remote_script,
            action=action,
            role="prod",
            stage=stage,
            args=args,
            commit=commit,
            binary_hash=binary_hash,
            namespace=namespace,
            release=release,
            include_drain_timeout=False,
            extra_flags=extra_flags,
        )
        runner.run(["ssh", "-o", "BatchMode=yes", args.pve_host, *host_args])


def _run_ownership_migration(runner: Runner, args: argparse.Namespace) -> None:
    bundle = Path(__file__).resolve().parent
    helper = bundle / "guest" / "repair-zerofs-ownership.sh"
    remote_stage = f"/tmp/zerofs-ownership-{args.ctid}"
    remote_helper = f"{remote_stage}/repair-zerofs-ownership.sh"
    mode = (
        "inventory"
        if args.dry_run or args.action == "ownership-inventory"
        else "repair"
    )
    with runner.remote_deployment_locks(args):
        try:
            _ssh(
                runner,
                args.vm_host,
                f"set -euo pipefail\ninstall -d -m 0700 {shlex.quote(remote_stage)}\n",
            )
            _stage_remote_file(runner, args.vm_host, helper, remote_helper)
            command = [
                "sudo",
                "bash",
                remote_helper,
                mode,
                f"{args.container_ip}:/",
                *([OWNERSHIP_REPAIR_CONFIRMATION] if mode == "repair" else []),
            ]
            _ssh(
                runner,
                args.vm_host,
                f"set -euo pipefail\n{shell_join(command)}\n",
            )
        finally:
            _ssh(
                runner,
                args.vm_host,
                f"set -euo pipefail\nrm -rf -- {shlex.quote(remote_stage)}\n",
            )


def render_vm_nfs_mount(template: str, container_ip: str) -> str:
    address = ipaddress.ip_address(container_ip)
    if not is_rfc1918(address):
        raise ValueError("VM NFS mount source must be RFC1918 private space")
    if template.count(f"What={VM_NFS_TEMPLATE_SOURCE}") != 1:
        raise ValueError("VM NFS mount template must contain one canonical source")
    return template.replace(f"What={VM_NFS_TEMPLATE_SOURCE}", f"What={address}:/", 1)


def _run_prod_vm_nfs_transaction(
    runner: Runner,
    args: argparse.Namespace,
    release: str,
    activate_host: Callable[[], None],
    commit_host: Callable[[], None],
    rollback_host: Callable[[], None],
    recover_host: Callable[[], None],
    *,
    activate_maintenance: Callable[[], None] | None = None,
    promote_host: Callable[[], None] | None = None,
) -> None:
    bundle = Path(__file__).resolve().parent
    helper = bundle / "vm_nfs_transition.py"
    guest_reconciler = bundle / "guest" / "reconcile-zerofs-nfs.sh"
    template = (bundle / "systemd" / VM_NFS_MOUNT_UNIT).read_text()
    rendered = render_vm_nfs_mount(template, args.container_ip)
    remote_stage = f"/tmp/zerofs-vm-nfs-{args.ctid}-{release}"
    transaction = "/var/lib/zerofs-deploy/transactions/active"
    remote_helper = f"{remote_stage}/vm_nfs_transition.py"
    remote_guest_reconciler = f"{remote_stage}/reconcile-zerofs-nfs.sh"
    remote_unit = f"{remote_stage}/{VM_NFS_MOUNT_UNIT}"
    prepared = False
    host_activated = False
    commit_decision_attempted = False
    ownership_requires_post_mount_proof = False
    legacy_bindfs = False
    failure: BaseException | None = None
    requested_deployment = {
        "ctid": args.ctid,
        "release": release,
        "source": f"{args.container_ip}:/",
        "pve_host": args.pve_host,
    }

    def action(name: str, *extra: str) -> None:
        command = [
            "sudo",
            "python3",
            remote_helper,
            name,
            "--transaction",
            transaction,
            *extra,
        ]
        _ssh(runner, args.vm_host, f"set -euo pipefail\n{shell_join(command)}\n")

    def action_output(name: str) -> str:
        command = [
            "sudo",
            "python3",
            remote_helper,
            name,
            "--transaction",
            transaction,
        ]
        return _ssh_capture(
            runner,
            args.vm_host,
            f"set -euo pipefail\n{shell_join(command)}\n",
        )

    def guest_action(name: str) -> str:
        command = [
            "sudo",
            "bash",
            remote_guest_reconciler,
            name,
            f"{args.container_ip}:/",
        ]
        if name == "reconcile":
            command.append(remote_unit)
        return _ssh_capture(
            runner,
            args.vm_host,
            f"set -euo pipefail\n{shell_join(command)}\n",
        )

    with runner.remote_deployment_locks(args):
        try:
            _ssh(
                runner,
                args.vm_host,
                f"set -euo pipefail\ninstall -d -m 0700 {shlex.quote(remote_stage)}\n",
            )
            _stage_remote_file(runner, args.vm_host, helper, remote_helper)
            _stage_remote_file(
                runner,
                args.vm_host,
                guest_reconciler,
                remote_guest_reconciler,
            )
            with tempfile.TemporaryDirectory(prefix="zerofs-vm-nfs-") as directory:
                rendered_path = Path(directory) / VM_NFS_MOUNT_UNIT
                rendered_path.write_text(rendered)
                _stage_remote_file(runner, args.vm_host, rendered_path, remote_unit)
            status_output = action_output("status")
            status = (
                json.loads(status_output)
                if status_output.strip()
                else {"phase": "absent"}
            )
            if status.get("phase") == "commit_decided":
                if status.get("deployment") != requested_deployment:
                    raise RuntimeError(
                        "durable VM commit decision belongs to another deployment; "
                        f"recorded={status.get('deployment')!r}, "
                        f"requested={requested_deployment!r}"
                    )
                commit_host()
                action("commit")
                return
            recover_host()
            if status.get("phase") != "absent":
                _wait_recovered_server_ready(runner, args)
            action("recover")
            receipt_output = guest_action("preflight")
            if not runner.dry_run:
                receipt = parse_shared_namespace_ownership_receipt(receipt_output)
                if not receipt.verified and receipt.reason in {
                    "mount_unavailable",
                    "legacy_topology",
                }:
                    ownership_requires_post_mount_proof = True
                    legacy_bindfs = receipt.reason == "legacy_topology"
                else:
                    validate_shared_namespace_ownership(receipt)
            else:
                print(
                    "+ if the managed mount is initially unavailable, repeat the "
                    "ownership proof after the new server is mounted"
                )
            prepare_args = [
                "--staged-unit",
                remote_unit,
                "--expected-source",
                f"{args.container_ip}:/",
                "--deployment-ctid",
                str(args.ctid),
                "--deployment-release",
                release,
                "--deployment-pve-host",
                args.pve_host,
            ]
            if legacy_bindfs:
                prepare_args.append("--allow-legacy-bindfs")
            action("prepare", *prepare_args)
            prepared = True
            action("quiesce")
            if ownership_requires_post_mount_proof:
                if activate_maintenance is None or promote_host is None:
                    raise RuntimeError(
                        "bootstrap ownership proof requires NFS-only activation and promotion"
                    )
                activate_maintenance()
            else:
                activate_host()
            host_activated = True
            guest_action("reconcile")
            action("reconcile")
            if ownership_requires_post_mount_proof:
                validate_shared_namespace_ownership(
                    parse_shared_namespace_ownership_receipt(guest_action("preflight"))
                )
                action("quiesce")
                promote_host()
                guest_action("reconcile")
                action("reconcile")
            commit_decision_attempted = True
            action("decide")
            commit_host()
            host_activated = False
            action("commit")
            prepared = False
        except BaseException as error:
            failure = error
            if commit_decision_attempted:
                error.add_note(
                    "commit decision may be durable; retry deployment to recover or finish"
                )
            elif prepared:
                if host_activated:
                    try:
                        rollback_host()
                        host_activated = False
                    except BaseException as host_rollback_error:
                        error.add_note(
                            f"host rollback also failed; VM remains quiesced: "
                            f"{host_rollback_error}"
                        )
                if not host_activated:
                    try:
                        action("rollback")
                        action("commit")
                        prepared = False
                    except BaseException as rollback_error:
                        error.add_note(f"VM NFS rollback also failed: {rollback_error}")
            raise
        finally:
            try:
                _ssh(
                    runner,
                    args.vm_host,
                    f"set -euo pipefail\nrm -rf -- {shlex.quote(remote_stage)}\n",
                )
            except BaseException as cleanup_error:
                if failure is None:
                    raise
                failure.add_note(f"VM NFS staging cleanup also failed: {cleanup_error}")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "action",
        choices=(
            "deploy",
            "replace",
            "cleanup",
            "status",
            "ownership-inventory",
            "ownership-repair",
        ),
    )
    parser.add_argument("--role", choices=("prod", "dev"), required=True)
    parser.add_argument("--pve-host", default="gthost-tor-pve-root")
    parser.add_argument("--vm-host", default="ubuntu-main")
    parser.add_argument(
        "--vm-transport",
        choices=("ssh", "local"),
        default="ssh",
        help="VM command transport; local requires execution inside VM100",
    )
    parser.add_argument(
        "--vm-vmid",
        type=int,
        help="required as 100 with --vm-transport local",
    )
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
    parser.add_argument("--source-client-unit")
    parser.add_argument("--source-mount-unit")
    parser.add_argument("--source-mountpoint")
    parser.add_argument("--source-server-unit")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--confirm-ownership-repair")
    parser.add_argument("--local-durable-upgrade", action="store_true")
    parser.add_argument("--confirm-local-durable-upgrade")
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
    if args.action.startswith("ownership-") and args.role != "prod":
        raise ValueError("ownership migration is valid only for production")
    if args.action == "ownership-repair" and not args.dry_run:
        if args.confirm_ownership_repair != OWNERSHIP_REPAIR_CONFIRMATION:
            raise ValueError(
                "ownership repair requires --confirm-ownership-repair 501:20"
            )
        if (
            os.environ.get("ZEROFS_CONFIRM_OWNERSHIP_REPAIR")
            != OWNERSHIP_REPAIR_CONFIRMATION
        ):
            raise ValueError(
                "ownership repair requires ZEROFS_CONFIRM_OWNERSHIP_REPAIR=501:20"
            )
    if args.action == "replace":
        require_replace_confirmation(
            args.ctid, os.environ.get("ZEROFS_CONFIRM_REPLACE")
        )
    if args.local_durable_upgrade:
        if args.role != "prod" or args.action != "deploy":
            raise ValueError(
                "local-durable upgrade is valid only for production deploy"
            )
        require_local_durable_upgrade_confirmation(
            args.ctid,
            args.confirm_local_durable_upgrade,
            os.environ.get("ZEROFS_CONFIRM_LOCAL_DURABLE_UPGRADE"),
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
    legacy_nbd_source = _has_legacy_nbd_source(args)
    runner = Runner(args.dry_run)
    runner.configure_vm_transport(args)

    if args.action.startswith("ownership-"):
        _run_ownership_migration(runner, args)
        return 0

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
        if legacy_nbd_source:
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
    if args.local_durable_upgrade:
        release_extras.append("local-durable-upgrade")
    if args.prod_access in {"smb", "both"}:
        release_extras.append(args.samba_user)
    release = (
        f"{commit[:12]}-DRYRUN"
        if args.dry_run
        else release_id(commit, release_paths, release_extras)
    )

    def activate_host() -> None:
        _stage_and_run_host(
            runner,
            args,
            binary,
            commit,
            binary_hash,
            namespace,
            release,
            defer_commit=args.role == "prod",
        )

    def activate_maintenance() -> None:
        with tempfile.TemporaryDirectory(prefix="zerofs-nfs-bootstrap-") as directory:
            bootstrap = Path(directory) / "zerofs.toml"
            bootstrap.write_text(render_nfs_bootstrap_config(args.config.read_text()))
            _stage_and_run_host(
                runner,
                args,
                binary,
                commit,
                binary_hash,
                namespace,
                release,
                defer_commit=True,
                config_path=bootstrap,
                maintenance_nfs_only=True,
            )

    def control_host(action: str) -> None:
        _run_host_deployment_control(
            runner,
            args,
            action,
            commit,
            binary_hash,
            namespace,
            release,
        )

    if args.role == "prod":
        _run_prod_vm_nfs_transaction(
            runner,
            args,
            release,
            activate_host,
            lambda: control_host("finalize"),
            lambda: control_host("rollback"),
            lambda: control_host("recover"),
            activate_maintenance=activate_maintenance,
            promote_host=lambda: control_host("promote"),
        )
    else:
        activate_host()
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
