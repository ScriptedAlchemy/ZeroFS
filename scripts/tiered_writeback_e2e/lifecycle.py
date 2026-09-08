from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import tempfile
import time
import re
import string
from dataclasses import asdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from scripts.vm100_pilot.runner import Runner

from .config import (
    DEFAULT_PROC_ROOT,
    HarnessConfig,
    ConfigError,
    HarnessError,
    UnsafeCleanupTarget,
    path_is_within,
    validate_owned_path,
)
from .crash import CRASH_SCENARIOS
from .integrity import sha256_file, verify_copied_tree
from .linux_suites import PINNED_REVISIONS, SUITE_SCENARIOS
from .protocols import NBD_CLIENT, PROTOCOL_SCENARIOS, ScenarioBuilder, ScenarioContext
from .resources import ResourceLedger
from .observed_durability import (
    ObservedDurabilityCollector,
    ObservedSnapshot,
    collector_factory,
    load_observed_endpoint,
)


class LifecycleError(HarnessError):
    pass


class SetupError(LifecycleError):
    pass


class CleanupError(LifecycleError):
    pass


class ResidualResourceError(LifecycleError):
    pass


class SourceBusyError(LifecycleError):
    pass


class PrimaryAndCleanupError(LifecycleError):
    """A scenario failed and the follow-up cleanup failed as well."""

    def __init__(self, primary: BaseException, cleanup_error: BaseException) -> None:
        super().__init__(
            f"scenario failed ({primary}) and cleanup also failed ({cleanup_error})"
        )
        self.primary = primary
        self.cleanup_error = cleanup_error
        self.receipt: str | None = None


SCENARIOS: dict[str, ScenarioBuilder] = {
    **PROTOCOL_SCENARIOS,
    **CRASH_SCENARIOS,
    **SUITE_SCENARIOS,
}
if len(SCENARIOS) != (
    len(PROTOCOL_SCENARIOS) + len(CRASH_SCENARIOS) + len(SUITE_SCENARIOS)
):  # pragma: no cover - guards against future name collisions.
    raise AssertionError("scenario registries overlap")
SCENARIO_NAMES = tuple(sorted(SCENARIOS))

REQUIRED_RECEIPT_FIELDS = (
    "schema",
    "run_uuid",
    "command",
    "scenario",
    "filesystem_ack_mode",
    "object_ack_mode",
    "source_sha",
    "binary_sha256",
    "config_sha256",
    "control_root",
    "resource_root",
    "backend_prefix",
    "units",
    "pids",
    "ports",
    "devices",
    "mounts",
    "pools",
    "tool_revisions",
    "manifest",
    "durability_floors",
    "terminal_state",
    "commands",
    "exit_status",
    "cleanup_status",
    "status",
    "started_at",
)

# Process names that mean the source checkout is still being built or tested.
BUSY_PROCESS_TOKENS = (
    "cargo",
    "rustc",
    "rustdoc",
    "test",
    "tiered-writeback-e2e",
    "zerofs",
)

_RUNTIME_SERVERS_BEGIN = "# TIERED_RUNTIME_SERVERS_BEGIN"
_RUNTIME_SERVERS_END = "# TIERED_RUNTIME_SERVERS_END"
_XFS_RUNTIME_TEMPLATE = "xfs_nbd_tiered.toml.template"


def derive_bootstrap_config(
    runtime_config: Path,
    bootstrap_config: Path,
    *,
    ninep_socket: Path,
) -> None:
    """Replace the fixture's one fixed runtime server block with owned 9P."""
    text = Path(runtime_config).read_text(encoding="utf-8")
    if (
        text.count(_RUNTIME_SERVERS_BEGIN) != 1
        or text.count(_RUNTIME_SERVERS_END) != 1
        or text.index(_RUNTIME_SERVERS_BEGIN) > text.index(_RUNTIME_SERVERS_END)
    ):
        raise ValueError("fixture must contain exactly one fixed runtime server block")
    before, marked = text.split(_RUNTIME_SERVERS_BEGIN, 1)
    _, after = marked.split(_RUNTIME_SERVERS_END, 1)
    replacement = (
        f"{_RUNTIME_SERVERS_BEGIN}\n"
        "[servers.ninep]\n"
        f"unix_socket = {json.dumps(str(ninep_socket))}\n"
        f"{_RUNTIME_SERVERS_END}"
    )
    Path(bootstrap_config).write_text(
        before + replacement + after,
        encoding="utf-8",
    )


def materialize_xfs_runtime_config(
    config: HarnessConfig,
    source_config: Path,
) -> Path:
    source = Path(source_config)
    if source.name != _XFS_RUNTIME_TEMPLATE:
        return source
    values = {
        "RUN_UUID": config.run_uuid,
        "CONTROL_ROOT": str(config.control_root),
        "RESOURCE_ROOT": str(config.resource_root),
        "MINIO_BUCKET": f"zerofs-xfs-{config.run_uuid}",
    }
    try:
        rendered = string.Template(source.read_text(encoding="utf-8")).substitute(
            values
        )
    except (KeyError, OSError) as error:
        raise SetupError("failed to materialize the fixed XFS runtime config") from error
    if "${" in rendered:
        raise SetupError("materialized XFS runtime config retains a placeholder")
    directory = config.control_root / "run"
    directory.mkdir(mode=0o700)
    target = directory / "xfs-nbd-tiered.toml"
    target.write_text(rendered, encoding="utf-8")
    target.chmod(0o600)
    return target


def _utc_stamp() -> str:
    return datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")


class HarnessReceipt:
    """Atomic JSON receipt carrying every field the contract requires."""

    def __init__(
        self,
        config: HarnessConfig,
        command: str,
        identity: dict[str, Any],
        *,
        scenario: str | None = None,
    ) -> None:
        config.receipt_root.mkdir(parents=True, exist_ok=True)
        self.directory = config.receipt_root / f"{command}-{_utc_stamp()}-{os.getpid()}"
        self.directory.mkdir(mode=0o755)
        self.manifest = self.directory / "manifest.json"
        self.payload: dict[str, Any] = {
            "schema": 1,
            "run_uuid": config.run_uuid,
            "command": command,
            "scenario": scenario,
            "filesystem_ack_mode": config.ack.filesystem,
            "object_ack_mode": config.ack.object,
            "source_sha": identity.get("source_sha"),
            "binary_sha256": identity.get("binary_sha256"),
            "config_sha256": identity.get("config_sha256"),
            "control_root": str(config.control_root),
            "resource_root": str(config.resource_root),
            "backend_prefix": config.backend_prefix,
            "units": [],
            "pids": [],
            "ports": [],
            "devices": [],
            "mounts": [],
            "pools": [],
            "tool_revisions": dict(PINNED_REVISIONS),
            "manifest": {},
            "durability_floors": [],
            "terminal_state": "started",
            "commands": [],
            "exit_status": None,
            "cleanup_status": "not-run",
            "status": "running",
            "started_at": datetime.now(UTC).isoformat(),
        }
        self._write()

    def record(self, name: str, value: Any) -> None:
        self.payload[name] = value
        self._write()

    def sync_resources(self, ledger: ResourceLedger) -> None:
        buckets: dict[str, str] = {
            "unit": "units",
            "process": "pids",
            "listener": "ports",
            "device": "devices",
            "mount": "mounts",
            "pool": "pools",
        }
        collected: dict[str, list[Any]] = {name: [] for name in buckets.values()}
        for kind, value, _ in ledger.resources():
            if kind in buckets:
                collected[buckets[kind]].append(value)
        for name, values in collected.items():
            self.payload[name] = values
        self._write()

    def finish(self, status: str) -> None:
        self.payload["status"] = status
        self.payload["finished_at"] = datetime.now(UTC).isoformat()
        self._write()

    def _write(self) -> None:
        fd, temporary = tempfile.mkstemp(prefix=".manifest.", dir=self.directory)
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as handle:
                json.dump(self.payload, handle, indent=2, sort_keys=True, default=str)
                handle.write("\n")
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temporary, self.manifest)
        finally:
            if os.path.exists(temporary):
                os.unlink(temporary)


class Probes:
    """Live-state checks used by cleanup and assert-clean on Linux."""

    def __init__(self, runner: Runner) -> None:
        self.runner = runner

    def _succeeds(self, argv: list[str]) -> bool:
        return self.runner.run(argv, check=False, timeout=5.0).returncode == 0

    def process_alive(self, pid: int) -> bool:
        return self._succeeds(["kill", "-0", str(pid)])

    def unit_active(self, unit: str) -> bool:
        return self._succeeds(["systemctl", "is-active", "--quiet", unit])

    def process_belongs_to_unit(self, pid: int, unit: str) -> bool:
        result = self.runner.run(
            ["systemctl", "show", "--property=MainPID", "--value", unit],
            check=False,
            timeout=5.0,
        )
        raw_pid = result.stdout.strip()
        return result.returncode == 0 and raw_pid.isdecimal() and int(raw_pid) == pid

    def port_listening(self, port: int) -> bool:
        return self._succeeds(["lsof", "-nP", f"-iTCP:{port}", "-sTCP:LISTEN"])

    def mount_active(self, mountpoint: str) -> bool:
        return self._succeeds(["mountpoint", "-q", mountpoint])

    def device_attached(self, device: str) -> bool:
        return self._succeeds([NBD_CLIENT, "-c", device])

    def pool_exists(self, name: str) -> bool:
        return self._succeeds(["zpool", "list", "-H", "-o", "name", name])


def assert_source_idle(
    source_root: Path, *, proc_root: Path = DEFAULT_PROC_ROOT
) -> dict[str, Any]:
    """Fail when cargo/rustc/test/harness jobs are still rooted at the source."""
    source = Path(source_root).resolve(strict=False)
    proc_root = Path(proc_root)
    busy: list[dict[str, Any]] = []
    if proc_root.is_dir():
        for entry in sorted(proc_root.iterdir()):
            if not entry.name.isdigit():
                continue
            try:
                cwd = Path(os.readlink(entry / "cwd")).resolve(strict=False)
            except OSError:
                continue
            if cwd != source and not path_is_within(cwd, source):
                continue
            try:
                comm = (entry / "comm").read_text(encoding="utf-8").strip()
            except OSError:
                comm = ""
            try:
                cmdline = (
                    (entry / "cmdline")
                    .read_bytes()
                    .replace(b"\x00", b" ")
                    .decode("utf-8", errors="replace")
                    .strip()
                )
            except OSError:
                cmdline = ""
            haystack = f"{comm} {cmdline}".lower()
            if any(token in haystack for token in BUSY_PROCESS_TOKENS):
                busy.append({"pid": int(entry.name), "comm": comm, "cwd": str(cwd)})
    if busy:
        rendered = ", ".join(f"pid {job['pid']} ({job['comm']})" for job in busy)
        raise SourceBusyError(f"active jobs rooted at {source}: {rendered}")
    return {"source_root": str(source), "busy": busy}


class HarnessLifecycle:
    def __init__(
        self,
        config: HarnessConfig,
        runner: Runner,
        *,
        probes: Any | None = None,
        platform: str | None = None,
        observed_factory: Any | None = None,
    ) -> None:
        self.config = config
        self.runner = runner
        self.probes = probes if probes is not None else Probes(runner)
        self.platform = platform if platform is not None else sys.platform
        self.observed_factory = observed_factory

    def setup(self, *, zerofs_binary: Path, zerofs_config: Path) -> dict[str, Any]:
        config = self.config
        config.control_root.mkdir(parents=True, exist_ok=True)
        if config.ledger_path.exists():
            raise SetupError(
                f"run {config.run_uuid} is already set up: {config.ledger_path}"
            )
        runtime_config = materialize_xfs_runtime_config(config, zerofs_config)
        source_command = [
            "git",
            "-C",
            str(config.source_root),
            "rev-parse",
            "HEAD",
        ]
        source_sha = self.runner.run(source_command).stdout.strip()
        ledger = ResourceLedger.create(
            config,
            source_sha=source_sha,
            binary_sha256=sha256_file(Path(zerofs_binary)),
            config_sha256=sha256_file(runtime_config),
        )
        try:
            config.receipt_root.mkdir()
            config.resource_root.mkdir()
            ledger.record_resource("path", str(config.resource_root))
            ledger.record_resource("prefix", config.backend_prefix)
            for child in (config.mount_root, config.tools_root, config.run_root):
                child.mkdir()
            receipt = HarnessReceipt(config, "setup", ledger.identity)
            receipt.record("commands", [source_command])
            receipt.record("terminal_state", "completed")
            receipt.record("exit_status", 0)
            receipt.sync_resources(ledger)
            receipt.finish("ok")
        except BaseException as error:
            ledger.record_event("setup-failed", {"error": str(error)})
            raise SetupError(f"setup failed after partial creation: {error}") from error
        return {
            "run_uuid": config.run_uuid,
            "control_root": str(config.control_root),
            "resource_root": str(config.resource_root),
            "ledger": str(config.ledger_path),
            "zerofs_config": str(runtime_config),
            "receipt": str(receipt.manifest),
        }

    def _teardown_resource(
        self, kind: str, value: Any, details: dict[str, Any], scope: Path
    ) -> None:
        if kind == "process":
            # Numeric PIDs are receipt evidence only. The UUID-scoped unit is
            # the sole destructive authority, preventing PID-reuse kills.
            if not isinstance(details.get("unit"), str):
                raise CleanupError(f"process {value} has no unit authority")
        elif kind == "unit":
            if self.probes.unit_active(str(value)):
                self.runner.run(
                    ["systemctl", "stop", str(value)], sudo=True, timeout=10.0
                )
        elif kind == "mount":
            mountpoint = str(validate_owned_path(Path(str(value)), scope))
            if self.probes.mount_active(mountpoint):
                self.runner.run(["umount", mountpoint], sudo=True)
        elif kind == "device":
            if self.probes.device_attached(str(value)):
                self.runner.run([NBD_CLIENT, "-d", str(value)], sudo=True)
        elif kind == "pool":
            if self.probes.pool_exists(str(value)):
                self.runner.run(["zpool", "destroy", str(value)], sudo=True)
        elif kind == "path":
            target = validate_owned_path(Path(str(value)), scope)
            if target.is_dir() and not target.is_symlink():
                shutil.rmtree(target)
            else:
                target.unlink(missing_ok=True)
        # listener and prefix resources die with their process / namespace;
        # releasing them below keeps the ledger authoritative.
        _ = details

    def cleanup(self, ledger: ResourceLedger) -> dict[str, Any]:
        scope = ledger.validate_cleanup_scope()
        ledger.record_event("cleanup-started", {"scope": str(scope)})
        errors: list[str] = []
        released = 0
        for kind, value, details in reversed(ledger.outstanding()):
            try:
                self._teardown_resource(kind, value, details, scope)
                ledger.record_release(kind, value)
                released += 1
            except Exception as error:  # noqa: BLE001 - collected and re-raised.
                errors.append(f"{kind} {value}: {error}")
        if scope.is_symlink() or scope.exists():
            try:
                if scope.is_dir() and not scope.is_symlink():
                    shutil.rmtree(scope)
                else:
                    scope.unlink(missing_ok=True)
            except OSError as error:
                errors.append(f"resource root {scope}: {error}")
        ledger.record_event(
            "cleanup-finished", {"errors": errors, "released": released}
        )
        if errors:
            raise CleanupError("cleanup left residue: " + "; ".join(errors))
        return {
            "resource_root": str(scope),
            "released": released,
            "errors": errors,
        }

    def _probe(self, kind: str, value: Any, details: dict[str, Any]) -> bool:
        if kind == "process":
            unit = details.get("unit")
            return isinstance(unit, str) and self.probes.process_belongs_to_unit(
                value, unit
            )
        if kind == "unit":
            return self.probes.unit_active(str(value))
        if kind == "listener":
            return self.probes.port_listening(value)
        if kind == "mount":
            return self.probes.mount_active(str(value))
        if kind == "device":
            return self.probes.device_attached(str(value))
        if kind == "pool":
            return self.probes.pool_exists(str(value))
        if kind == "path":
            return Path(str(value)).exists()
        return False  # backend prefixes are covered by the outstanding check.

    def _wait_for_device_detached(self, device: str, *, timeout: float = 10.0) -> None:
        deadline = time.monotonic() + timeout
        while self.probes.device_attached(device):
            if time.monotonic() >= deadline:
                raise LifecycleError(f"NBD device {device} remained attached")
            time.sleep(0.1)

    def _wait_for_unit_gone(
        self,
        unit: str,
        pids: list[int],
        *,
        timeout: float = 10.0,
    ) -> None:
        deadline = time.monotonic() + timeout
        while self.probes.unit_active(unit) or any(
            self.probes.process_alive(pid) for pid in pids
        ):
            if time.monotonic() >= deadline:
                raise LifecycleError(
                    f"transient unit {unit} or its owned MainPID remained active"
                )
            time.sleep(0.1)

    def assert_clean(self, ledger: ResourceLedger) -> dict[str, Any]:
        ledger.validate_cleanup_scope()
        failures = [
            f"outstanding {kind} {value} was never released"
            for kind, value, _ in ledger.outstanding()
        ]
        checked = 0
        for kind, value, details in ledger.resources():
            checked += 1
            if self._probe(kind, value, details):
                failures.append(f"recorded {kind} {value} still present")
        if failures:
            raise ResidualResourceError("; ".join(failures))
        return {"clean": True, "checked": checked}

    def archive_control(
        self, ledger: ResourceLedger, archive_root: Path
    ) -> dict[str, Any]:
        ledger.validate_cleanup_scope()
        control_root = ledger.control_root.resolve(strict=False)
        archive = Path(archive_root)
        if not archive.is_absolute():
            raise UnsafeCleanupTarget(f"archive root {archive} is not absolute")
        archive = archive.resolve(strict=False)
        for forbidden in (control_root, ledger.resource_root.resolve(strict=False)):
            if archive == forbidden or path_is_within(archive, forbidden):
                raise UnsafeCleanupTarget(
                    f"archive root {archive} is inside run root {forbidden}"
                )
        destination = archive / ledger.run_uuid
        ledger.record_event("archive-control", {"destination": str(destination)})
        destination.mkdir(parents=True, exist_ok=True)
        shutil.copytree(control_root, destination, dirs_exist_ok=True)
        hashes = verify_copied_tree(control_root, destination)
        shutil.rmtree(control_root)
        return {"destination": str(destination), "files": len(hashes)}

    def run_scenario(
        self,
        ledger: ResourceLedger,
        scenario: str,
        *,
        zerofs_binary: Path,
        zerofs_config: Path,
        plan_only: bool = False,
    ) -> dict[str, Any]:
        if scenario not in SCENARIOS:
            raise ConfigError(f"unknown scenario {scenario!r}")
        binary_sha = sha256_file(Path(zerofs_binary))
        config_sha = sha256_file(Path(zerofs_config))
        for role, actual, expected in (
            ("binary", binary_sha, ledger.identity.get("binary_sha256")),
            ("config", config_sha, ledger.identity.get("config_sha256")),
        ):
            if actual != expected:
                raise ConfigError(
                    f"{role} hash {actual} does not match the ledger identity "
                    f"({expected}); runs must use the exact artifacts from setup"
                )
        context = ScenarioContext(
            config=self.config,
            zerofs_binary=Path(zerofs_binary),
            zerofs_config=Path(zerofs_config),
        )
        plan = SCENARIOS[scenario](context)
        receipt = HarnessReceipt(
            self.config, f"run-{scenario}", ledger.identity, scenario=scenario
        )
        receipt.record("manifest", plan.to_dict())
        if plan_only:
            receipt.record("terminal_state", "planned")
            receipt.finish("planned")
            return {
                "scenario": scenario,
                "terminal_state": "planned",
                "steps": len(plan.steps),
                "receipt": str(receipt.manifest),
            }
        if self.platform != "linux":
            receipt.record("terminal_state", "refused")
            receipt.finish("failed")
            raise LifecycleError(
                "scenario execution requires Linux; use --plan-only elsewhere"
            )
        commands: list[list[str]] = []
        checksums: dict[str, str] = {}
        observed: dict[str, Any] = {}
        collector: ObservedDurabilityCollector | Any | None = None
        observed_factory = self.observed_factory
        initial_observation: ObservedSnapshot | None = None
        pre_restart_observation: ObservedSnapshot | None = None
        restarted_observation: ObservedSnapshot | None = None
        durability_cutoff: int | None = None
        compared_checksums: set[str] = set()
        try:
            if plan.acceptance_gaps:
                raise LifecycleError(
                    f"scenario {scenario!r} is unavailable: "
                    + "; ".join(plan.acceptance_gaps)
                )
            if plan.requires_observed_durability and observed_factory is None:
                if plan.authority_export_id is None:
                    raise LifecycleError(
                        f"scenario {scenario!r} has no exact authority export"
                    )
                endpoint = load_observed_endpoint(
                    zerofs_config,
                    control_root=self.config.control_root,
                    expected_export=plan.authority_export_id,
                )
                observed_factory = collector_factory(endpoint)
            if plan.requires_observed_durability and plan.bootstrap_config is not None:
                bootstrap_config = validate_owned_path(
                    plan.bootstrap_config, self.config.resource_root
                )
                derive_bootstrap_config(
                    zerofs_config,
                    bootstrap_config,
                    ninep_socket=self.config.run_root / "xfs-nbd-bootstrap.9p.sock",
                )
                ledger.record_resource(
                    "path",
                    str(bootstrap_config),
                    scenario=scenario,
                    role="bootstrap-config",
                )
                receipt.record(
                    "bootstrap_config_sha256", sha256_file(bootstrap_config)
                )
            for step in plan.steps:
                for resource in step.requires:
                    ledger.require_active(resource.kind, resource.value)
                commands.append(list(step.argv))
                receipt.record("commands", commands)
                for resource in step.acquires:
                    ledger.record_resource(
                        resource.kind,
                        resource.value,
                        scenario=scenario,
                        step=step.description,
                    )
                receipt.sync_resources(ledger)
                result = self.runner.run(
                    step.argv,
                    sudo=step.sudo,
                    cwd=Path(step.cwd) if step.cwd else None,
                )
                if step.require_stdout is not None:
                    actual_stdout = result.stdout.strip()
                    if actual_stdout != step.require_stdout:
                        raise LifecycleError(
                            f"command output mismatch for {step.description}: "
                            f"expected {step.require_stdout!r}, got {actual_stdout!r}"
                        )
                if step.capture_sha256_as is not None:
                    digest = result.stdout.strip().split(maxsplit=1)[0]
                    if re.fullmatch(r"[0-9a-f]{64}", digest) is None:
                        raise LifecycleError(
                            f"invalid SHA-256 output for {step.description}"
                        )
                    checksums[step.capture_sha256_as] = digest
                    receipt.record("checksums", checksums)
                if step.compare_sha256_with is not None:
                    expected = checksums.get(step.compare_sha256_with)
                    digest = result.stdout.strip().split(maxsplit=1)[0]
                    if expected is None:
                        raise LifecycleError(
                            f"missing pre-restart checksum {step.compare_sha256_with!r}"
                        )
                    if re.fullmatch(r"[0-9a-f]{64}", digest) is None:
                        raise LifecycleError(
                            f"invalid SHA-256 output for {step.description}"
                        )
                    if digest != expected:
                        raise LifecycleError(
                            f"post-restart checksum mismatch: {digest} != {expected}"
                        )
                    compared_checksums.add(step.compare_sha256_with)
                if step.capture_main_pid_unit is not None:
                    pid_command = [
                        "systemctl",
                        "show",
                        "--property=MainPID",
                        "--value",
                        step.capture_main_pid_unit,
                    ]
                    raw_pid = ""
                    for attempt in range(20):
                        commands.append(pid_command)
                        receipt.record("commands", commands)
                        try:
                            result = self.runner.run(
                                pid_command,
                                sudo=True,
                                check=False,
                                timeout=1.0,
                            )
                        except subprocess.TimeoutExpired:
                            result = None
                        if result is None:
                            raw_pid = ""
                        else:
                            raw_pid = result.stdout.strip()
                        if (
                            result is not None
                            and result.returncode == 0
                            and raw_pid.isdecimal()
                            and int(raw_pid) > 0
                        ):
                            break
                        if attempt < 19:
                            time.sleep(0.1)
                    else:
                        raise LifecycleError(
                            f"unit {step.capture_main_pid_unit!r} has no positive MainPID"
                        )
                    ledger.record_resource(
                        "process",
                        int(raw_pid),
                        unit=step.capture_main_pid_unit,
                        scenario=scenario,
                        step=step.description,
                    )
                if step.release_main_pid_unit is not None:
                    owned_pids = [
                        (kind, value)
                        for kind, value, details in ledger.outstanding()
                        if kind == "process"
                        and details.get("unit") == step.release_main_pid_unit
                    ]
                    if not owned_pids:
                        raise LifecycleError(
                            f"unit {step.release_main_pid_unit!r} has no owned MainPID"
                        )
                    if step.verify_stopped_unit is not None:
                        if step.verify_stopped_unit != step.release_main_pid_unit:
                            raise LifecycleError(
                                "stopped-unit probe does not match the released unit"
                            )
                        self._wait_for_unit_gone(
                            step.verify_stopped_unit,
                            [int(value) for _, value in owned_pids],
                        )
                    for kind, value in owned_pids:
                        ledger.record_release(
                            kind,
                            value,
                            scenario=scenario,
                            step=step.description,
                        )
                elif step.verify_stopped_unit is not None:
                    raise LifecycleError(
                        "stopped-unit probe has no owned MainPID release"
                    )
                if step.verify_detached_device is not None:
                    self._wait_for_device_detached(step.verify_detached_device)
                for resource in step.releases:
                    ledger.record_release(
                        resource.kind,
                        resource.value,
                        scenario=scenario,
                        step=step.description,
                    )
                if step.after_checkpoint is not None and not plan.requires_observed_durability:
                    continue
                if step.after_checkpoint == "pin-initial-authority":
                    collector = observed_factory()
                    initial_observation = collector.wait_for_initial_snapshot(
                        timeout=30.0
                    )
                    if (
                        plan.authority_export_id is None
                        or initial_observation.identity.export_id
                        != plan.authority_export_id
                    ):
                        raise LifecycleError(
                            "metrics authority export does not match the scenario export"
                        )
                    observed["initial"] = asdict(initial_observation)
                    receipt.record("observed_durability", observed)
                elif step.after_checkpoint == "final-local-cutoff":
                    if collector is None or initial_observation is None:
                        raise LifecycleError(
                            "final durability cutoff has no pinned initial authority"
                        )
                    accepted = collector.wait_for_accepted_after(
                        initial_observation.writeback.accepted,
                        timeout=30.0,
                    )
                    durability_cutoff = accepted.writeback.accepted
                    pre_restart_observation = collector.wait_for_local_frontier(
                        durability_cutoff,
                        timeout=30.0,
                    )
                    remote_coverage = ObservedDurabilityCollector.classify_recovery_source(
                        target=durability_cutoff,
                        observed=pre_restart_observation,
                    )
                    observed["pre_restart"] = asdict(pre_restart_observation)
                    receipt.record("observed_durability", observed)
                    receipt.record("durability_cutoff", durability_cutoff)
                    receipt.record("pre_kill_remote_coverage", remote_coverage)
                    receipt.record(
                        "durability_floors",
                        [
                            {
                                **floor.to_dict(),
                                "target_sequence": durability_cutoff,
                                "observed_local_sequence": (
                                    pre_restart_observation.writeback.local
                                ),
                                "observed_remote_sequence": (
                                    pre_restart_observation.writeback.remote
                                ),
                            }
                            for floor in plan.durability_floors
                        ],
                    )
                elif step.after_checkpoint == "require-restart":
                    if pre_restart_observation is None or durability_cutoff is None:
                        raise LifecycleError(
                            "restart identity check has no pre-restart durability cutoff"
                        )
                    collector = observed_factory()
                    restarted_observation = collector.wait_for_restarted(
                        pre_restart_observation,
                        timeout=30.0,
                    )
                    if restarted_observation.writeback.local < durability_cutoff:
                        raise LifecycleError(
                            "restarted server lost the locally durable cutoff"
                        )
                    observed["after_restart"] = asdict(restarted_observation)
                    receipt.record("observed_durability", observed)
                elif step.after_checkpoint is not None:
                    raise LifecycleError(
                        f"unknown observed checkpoint {step.after_checkpoint!r}"
                    )
            if plan.requires_observed_durability:
                missing: list[str] = []
                if initial_observation is None:
                    missing.append("initial authority")
                if pre_restart_observation is None or durability_cutoff is None:
                    missing.append("final local cutoff")
                if restarted_observation is None:
                    missing.append("restart authority")
                expected_checksum = "xfs-proof-before-restart"
                if expected_checksum not in checksums:
                    missing.append("pre-restart checksum")
                if expected_checksum not in compared_checksums:
                    missing.append("post-restart checksum comparison")
                if missing:
                    raise LifecycleError(
                        "missing required runtime evidence: " + ", ".join(missing)
                    )
            if plan.requires_completed_cleanup:
                cleanup_runs = [self.cleanup(ledger), self.cleanup(ledger)]
                cleanup_verification = self.assert_clean(ledger)
                receipt.record("cleanup_runs", cleanup_runs)
                receipt.record("cleanup_verification", cleanup_verification)
                receipt.record("cleanup_status", "ok")
        except BaseException as error:
            cancelled = isinstance(error, (KeyboardInterrupt, SystemExit))
            receipt.record("terminal_state", "cancelled" if cancelled else "failed")
            receipt.record("exit_status", 1)
            receipt.record("error", str(error) or type(error).__name__)
            cleanup_error: BaseException | None = None
            try:
                self.cleanup(ledger)
                receipt.record("cleanup_status", "ok")
            except BaseException as second:
                cleanup_error = second
                receipt.record("cleanup_status", "failed")
                receipt.record("cleanup_error", str(second))
            receipt.sync_resources(ledger)
            receipt.finish("failed")
            if cancelled:
                raise
            if cleanup_error is not None:
                combined = PrimaryAndCleanupError(error, cleanup_error)
                combined.receipt = str(receipt.manifest)
                raise combined from error
            raise
        receipt.record("terminal_state", "completed")
        receipt.record("exit_status", 0)
        receipt.sync_resources(ledger)
        receipt.finish("ok")
        return {
            "scenario": scenario,
            "terminal_state": "completed",
            "steps": len(plan.steps),
            "receipt": str(receipt.manifest),
        }
