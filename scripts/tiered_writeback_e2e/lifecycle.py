from __future__ import annotations

import json
import os
import shutil
import sys
import tempfile
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
from .protocols import PROTOCOL_SCENARIOS, ScenarioBuilder, ScenarioContext
from .resources import ResourceLedger


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
        return self.runner.run(argv, check=False).returncode == 0

    def process_alive(self, pid: int) -> bool:
        return self._succeeds(["kill", "-0", str(pid)])

    def port_listening(self, port: int) -> bool:
        return self._succeeds(["lsof", "-nP", f"-iTCP:{port}", "-sTCP:LISTEN"])

    def mount_active(self, mountpoint: str) -> bool:
        return self._succeeds(["mountpoint", "-q", mountpoint])

    def device_attached(self, device: str) -> bool:
        return self._succeeds(["nbd-client", "-c", device])

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
                busy.append(
                    {"pid": int(entry.name), "comm": comm, "cwd": str(cwd)}
                )
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
    ) -> None:
        self.config = config
        self.runner = runner
        self.probes = probes if probes is not None else Probes(runner)
        self.platform = platform if platform is not None else sys.platform

    def setup(self, *, zerofs_binary: Path, zerofs_config: Path) -> dict[str, Any]:
        config = self.config
        config.control_root.mkdir(parents=True, exist_ok=True)
        if config.ledger_path.exists():
            raise SetupError(
                f"run {config.run_uuid} is already set up: {config.ledger_path}"
            )
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
            config_sha256=sha256_file(Path(zerofs_config)),
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
            "receipt": str(receipt.manifest),
        }

    def _teardown_resource(
        self, kind: str, value: Any, details: dict[str, Any], scope: Path
    ) -> None:
        if kind == "process":
            if isinstance(value, str):
                self.runner.run(["systemctl", "stop", value], sudo=True)
            elif self.probes.process_alive(value):
                self.runner.run(["kill", "-9", str(value)], sudo=True)
        elif kind == "mount":
            mountpoint = str(validate_owned_path(Path(str(value)), scope))
            if self.probes.mount_active(mountpoint):
                self.runner.run(["umount", mountpoint], sudo=True)
        elif kind == "device":
            if self.probes.device_attached(str(value)):
                self.runner.run(["nbd-client", "-d", str(value)], sudo=True)
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

    def _probe(self, kind: str, value: Any) -> bool:
        if kind == "process":
            if isinstance(value, str):
                # Transient units are stopped by name; their liveness shows up
                # through the pid/listener resources they also recorded.
                return False
            return self.probes.process_alive(value)
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

    def assert_clean(self, ledger: ResourceLedger) -> dict[str, Any]:
        ledger.validate_cleanup_scope()
        failures = [
            f"outstanding {kind} {value} was never released"
            for kind, value, _ in ledger.outstanding()
        ]
        checked = 0
        for kind, value, _ in ledger.resources():
            checked += 1
            if self._probe(kind, value):
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
        receipt.record(
            "durability_floors", [floor.to_dict() for floor in plan.durability_floors]
        )
        if plan_only:
            receipt.record("terminal_state", "planned")
            receipt.record("exit_status", 0)
            receipt.finish("ok")
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
        try:
            for step in plan.steps:
                commands.append(list(step.argv))
                receipt.record("commands", commands)
                self.runner.run(
                    step.argv,
                    sudo=step.sudo,
                    cwd=Path(step.cwd) if step.cwd else None,
                )
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
