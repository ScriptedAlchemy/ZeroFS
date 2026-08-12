from __future__ import annotations

import hashlib
import time
import tomllib
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .config import PilotConfig
from .metrics import DrainReceipt, MetricsClient, WritebackSnapshot, wait_for_drain
from .runner import CommandError, Runner


@dataclass(frozen=True, slots=True)
class UnitState:
    active_state: str
    main_pid: int
    control_pid: int
    control_group: str
    cgroup_pids: tuple[int, ...]

    @property
    def stopped(self) -> bool:
        return (
            self.active_state in {"inactive", "failed"}
            and self.main_pid == 0
            and self.control_pid == 0
            and not self.cgroup_pids
        )


class PilotLifecycle:
    def __init__(
        self,
        config: PilotConfig,
        runner: Runner,
        metrics: MetricsClient | None = None,
    ) -> None:
        self.config = config
        self.runner = runner
        self.metrics = metrics or MetricsClient(config.metrics_url)

    def require_vm100(self) -> None:
        hostname = self.runner.run(["hostname"], timeout=5).stdout.strip()
        if hostname != "ubuntu-main":
            raise RuntimeError(f"run this only on VM100 (ubuntu-main), got {hostname!r}")
        for path in (self.config.config_file, self.config.env_file):
            result = self.runner.run(["test", "-f", path], sudo=True, check=False)
            if result.returncode:
                raise FileNotFoundError(f"pilot file is missing: {path}")

    def _show(self, unit: str, property_name: str) -> str:
        output = self.runner.run(
            ["systemctl", "show", unit, "-p", property_name, "--value"],
            check=False,
        ).stdout.strip()
        return output.splitlines()[0] if output else ""

    def unit_state(self, unit: str) -> UnitState:
        active_state = self._show(unit, "ActiveState") or "unknown"
        main_pid = int(self._show(unit, "MainPID") or "0")
        control_pid = int(self._show(unit, "ControlPID") or "0")
        control_group = self._show(unit, "ControlGroup")
        cgroup_pids: tuple[int, ...] = ()
        if control_group:
            process_file = self.config.cgroup_root / control_group.lstrip("/") / "cgroup.procs"
            try:
                cgroup_pids = tuple(int(line) for line in process_file.read_text().splitlines())
            except FileNotFoundError:
                pass
        return UnitState(active_state, main_pid, control_pid, control_group, cgroup_pids)

    def _wait_stopped(self, unit: str) -> None:
        deadline = time.monotonic() + self.config.stop_timeout
        while True:
            state = self.unit_state(unit)
            if state.stopped:
                return
            if time.monotonic() >= deadline:
                raise TimeoutError(f"{unit} did not stop: {state}")
            time.sleep(1)

    def _wait_active(self, unit: str, timeout: int) -> None:
        deadline = time.monotonic() + timeout
        while True:
            result = self.runner.run(
                ["systemctl", "is-active", unit], capture=True, check=False
            )
            if result.stdout.strip() == "active":
                return
            if time.monotonic() >= deadline:
                raise TimeoutError(f"{unit} did not become active")
            time.sleep(1)

    def _stop_one(self, unit: str) -> None:
        self.runner.run(
            ["systemctl", "stop", "--no-block", unit], sudo=True, check=False
        )
        self._wait_stopped(unit)

    def stop(self) -> None:
        self.require_vm100()
        for unit in (
            self.config.mount_unit,
            self.config.client_service,
            self.config.service,
        ):
            self._stop_one(unit)
        mounted = self.runner.run(
            ["findmnt", "-rn", "-M", self.config.mountpoint], check=False
        )
        if mounted.returncode == 0:
            raise RuntimeError(f"{self.config.mountpoint} remains mounted")

    def start(self) -> dict[str, int]:
        self.require_vm100()
        units = (
            (self.config.service, 180),
            (self.config.client_service, 60),
            (self.config.mount_unit, 60),
        )
        self.runner.run(
            ["systemctl", "reset-failed", *(unit for unit, _ in units)],
            sudo=True,
            check=False,
        )
        started: list[str] = []
        try:
            for unit, timeout in units:
                self.runner.run(["systemctl", "start", unit], sudo=True)
                started.append(unit)
                self._wait_active(unit, timeout)
        except BaseException as original:
            cleanup: list[str] = []
            for unit in reversed(started):
                try:
                    self._stop_one(unit)
                except BaseException as error:
                    cleanup.append(f"{unit}: {error}")
            if cleanup:
                original.add_note("startup cleanup failures: " + "; ".join(cleanup))
            raise
        return {"restarts": int(self._show(self.config.service, "NRestarts") or "0")}

    def restart(self) -> dict[str, int]:
        self.stop()
        return self.start()

    def drain(self, timeout: int | None = None) -> DrainReceipt:
        return wait_for_drain(
            self.metrics.snapshot,
            timeout=timeout or self.config.drain_timeout,
        )

    def _toml(self) -> dict[str, Any]:
        text = self.runner.run(["cat", self.config.config_file], sudo=True).stdout
        return tomllib.loads(text)

    def _sha256(self, path: Path, *, sudo: bool = False) -> str:
        output = self.runner.run(["sha256sum", path], sudo=sudo, timeout=180).stdout
        return output.split()[0]

    def status(self, *, validate_data: bool = True) -> dict[str, Any]:
        self.require_vm100()
        states = {
            unit: self.unit_state(unit)
            for unit in (self.config.service, self.config.client_service, self.config.mount_unit)
        }
        inactive = [unit for unit, state in states.items() if state.active_state != "active"]
        if inactive:
            raise RuntimeError(f"pilot units are not active: {', '.join(inactive)}")
        mount = self.runner.run(
            ["findmnt", "-no", "SOURCE,FSTYPE,TARGET", "-M", self.config.mountpoint]
        ).stdout.split()
        if mount != ["/dev/nbd0", "xfs", str(self.config.mountpoint)]:
            raise RuntimeError(f"unexpected pilot mount topology: {' '.join(mount)}")
        settings = self._toml()
        writeback = settings.get("writeback", {})
        if writeback.get("enabled") is not True:
            raise RuntimeError("[writeback] must be enabled")
        if writeback.get("ack_mode") != self.config.expected_ack_mode:
            raise RuntimeError(
                f"[writeback] ack_mode is {writeback.get('ack_mode')!r}, "
                f"expected {self.config.expected_ack_mode!r}"
            )
        service_pid = states[self.config.service].main_pid
        installed_sha = self._sha256(self.config.binary, sudo=True)
        running_sha = self._sha256(self.config.proc_root / str(service_pid) / "exe", sudo=True)
        if installed_sha != running_sha:
            raise RuntimeError("running binary does not match installed binary")
        receipt_text = self.runner.run(["cat", self.config.build_receipt], sudo=True).stdout
        receipt = dict(
            line.split("=", 1) for line in receipt_text.splitlines() if "=" in line
        )
        if receipt.get("binary_sha256") != running_sha:
            raise RuntimeError("build receipt binary hash does not match running binary")
        snapshot = self.metrics.snapshot()
        if snapshot.terminal:
            raise RuntimeError("writeback reported a terminal error")
        result: dict[str, Any] = {
            "healthy": True,
            "mount": {"source": mount[0], "fstype": mount[1], "target": mount[2]},
            "running_binary_sha256": running_sha,
            "deployed_commit": receipt.get("commit", ""),
            "config_sha256": self._sha256(self.config.config_file, sudo=True),
            "writeback": snapshot.to_dict(),
            "restarts": int(self._show(self.config.service, "NRestarts") or "0"),
        }
        if validate_data:
            integrity = self._sha256(self.config.integrity_file, sudo=True)
            if integrity != self.config.integrity_sha256:
                raise RuntimeError("integrity sentinel hash mismatch")
            count = int(
                self.runner.run(
                    ["find", self.config.metadata_dir, "-type", "f", "-printf", "."],
                    sudo=True,
                    timeout=180,
                ).stdout.count(".")
            )
            if count != self.config.metadata_file_count:
                raise RuntimeError(
                    f"metadata file count is {count}, expected {self.config.metadata_file_count}"
                )
            result["integrity_sha256"] = integrity
            result["metadata_files"] = count
        return result
