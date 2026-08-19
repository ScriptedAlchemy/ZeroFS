from __future__ import annotations

import ipaddress
import json
import os
import time
import uuid
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Callable, Mapping

from .config import PilotConfig
from .lifecycle import PilotLifecycle
from .metrics import (
    WritebackSnapshot,
    wait_for_accepted_after,
    wait_for_local,
    wait_for_remote,
)
from .receipts import RunReceipt
from .runner import Runner
from .scenarios import ScenarioDefinition
from .system_io import file_sha256


class ScenarioUnavailableError(RuntimeError):
    """The requested scenario has no proven path to its named protocol."""


def _options(value: str) -> tuple[str, ...]:
    options = tuple(sorted({item.strip() for item in value.split(",") if item.strip()}))
    if not options:
        raise ValueError("mount options must not be empty")
    return options


def _nfs_host(endpoint: str) -> str:
    if endpoint.startswith("["):
        closing = endpoint.find("]")
        if closing < 0 or endpoint[closing + 1 : closing + 3] != ":/":
            raise ValueError(f"invalid NFS endpoint: {endpoint!r}")
        return endpoint[1:closing]
    host, separator, path = endpoint.partition(":")
    if separator != ":" or not path.startswith("/"):
        raise ValueError(f"invalid NFS endpoint: {endpoint!r}")
    return host


@dataclass(frozen=True, slots=True)
class ProtocolAuthority:
    protocol: str
    mountpoint: Path
    endpoint: str
    mount_options: tuple[str, ...]

    @classmethod
    def from_mapping(
        cls,
        protocol: str,
        values: Mapping[str, str],
    ) -> "ProtocolAuthority":
        if protocol not in {"nfs", "9p"}:
            raise ValueError(f"unsupported shared-file protocol: {protocol!r}")
        prefix = "ZEROFS_BENCH_NFS" if protocol == "nfs" else "ZEROFS_BENCH_9P"
        mountpoint = values.get(f"{prefix}_MOUNTPOINT", "").strip()
        endpoint = values.get(f"{prefix}_ENDPOINT", "").strip()
        options = values.get(f"{prefix}_MOUNT_OPTIONS", "").strip()
        if not mountpoint or not endpoint or not options:
            label = "NFS" if protocol == "nfs" else "9P"
            raise ScenarioUnavailableError(
                f"{label} benchmark unavailable: set {prefix}_MOUNTPOINT, "
                f"{prefix}_ENDPOINT, and {prefix}_MOUNT_OPTIONS explicitly"
            )
        root = Path(mountpoint)
        if not root.is_absolute():
            raise ValueError(f"{protocol} mountpoint must be absolute: {root}")
        if protocol == "nfs":
            host = _nfs_host(endpoint)
            try:
                ipaddress.ip_address(host)
            except ValueError as error:
                raise ValueError(
                    "NFS benchmark endpoint must use a literal IP; mutable host "
                    f"aliases are not authority: {endpoint!r}"
                ) from error
        return cls(
            protocol=protocol,
            mountpoint=root.resolve(strict=False),
            endpoint=endpoint,
            mount_options=_options(options),
        )

    def verify(self, runner: Runner) -> dict[str, object]:
        if not self.mountpoint.is_dir():
            raise ScenarioUnavailableError(
                f"{self.protocol} benchmark mountpoint is unavailable: {self.mountpoint}"
            )
        completed = runner.run(
            [
                "findmnt",
                "--json",
                "--target",
                self.mountpoint,
                "--output",
                "TARGET,SOURCE,FSTYPE,OPTIONS",
            ]
        )
        try:
            filesystems = json.loads(completed.stdout)["filesystems"]
            if len(filesystems) != 1:
                raise ValueError("expected exactly one filesystem")
            mounted = filesystems[0]
            target = Path(str(mounted["target"])).resolve(strict=False)
            source = str(mounted["source"])
            filesystem = str(mounted["fstype"]).lower()
            mounted_options = _options(str(mounted["options"]))
        except (KeyError, TypeError, ValueError, json.JSONDecodeError) as error:
            raise ScenarioUnavailableError(
                f"cannot establish {self.protocol} mount authority: {error}"
            ) from error
        if target != self.mountpoint:
            raise ScenarioUnavailableError(
                f"{self.protocol} mount target mismatch: expected={self.mountpoint}, "
                f"actual={target}"
            )
        if source != self.endpoint:
            raise ScenarioUnavailableError(
                f"{self.protocol} mount source mismatch: expected={self.endpoint!r}, "
                f"actual={source!r}"
            )
        allowed_types = {"nfs", "nfs4"} if self.protocol == "nfs" else {"9p"}
        if filesystem not in allowed_types:
            raise ScenarioUnavailableError(
                f"{self.protocol} filesystem mismatch: actual={filesystem!r}"
            )
        missing = sorted(set(self.mount_options) - set(mounted_options))
        if missing:
            raise ScenarioUnavailableError(
                f"{self.protocol} mount is missing required options: {missing}"
            )
        return {
            "protocol": self.protocol,
            "target": str(target),
            "source": source,
            "fstype": filesystem,
            "options": list(mounted_options),
            "required_options": list(self.mount_options),
        }

    def require_run_root(self, path: Path) -> Path:
        resolved = path.resolve(strict=False)
        if resolved.parent != self.mountpoint:
            raise ValueError(
                f"unsafe protocol benchmark root: {resolved} is not a direct child "
                f"of {self.mountpoint}"
            )
        if not resolved.name.startswith(".zerofs-protocol-bench-"):
            raise ValueError(f"unsafe protocol benchmark root name: {resolved.name}")
        return resolved


@dataclass(frozen=True, slots=True)
class ProtocolWorkloadResult:
    name: str
    bytes: int
    pattern: str
    source_sha256: str
    readback_sha256: str
    target_sequence: int
    foreground_close_ns: int
    fsync_or_commit_ns: int
    local_cutoff_ns: int
    remote_cutoff_ns: int
    readback_ns: int
    foreground_mibps: float
    readback_mibps: float
    before: WritebackSnapshot
    accepted: WritebackSnapshot
    local: WritebackSnapshot
    remote: WritebackSnapshot


@dataclass(frozen=True, slots=True)
class CleanupEvidence:
    resources: tuple[str, ...]
    attempts: int
    asserted_clean: bool


@dataclass(frozen=True, slots=True)
class ProtocolMatrixResult:
    scenario: str
    protocol: str
    authority: dict[str, object]
    total_bytes: int
    workloads: tuple[ProtocolWorkloadResult, ...]
    cleanup: CleanupEvidence
    receipt_dir: str

    def to_dict(self) -> dict[str, object]:
        return {"schema": 1, **asdict(self)}


class ProtocolMatrixRunner:
    def __init__(
        self,
        config: PilotConfig,
        runner: Runner,
        lifecycle: PilotLifecycle,
        *,
        random_bytes: Callable[[int], bytes] = os.urandom,
    ) -> None:
        self.config = config
        self.runner = runner
        self.lifecycle = lifecycle
        self.random_bytes = random_bytes

    def _create_source(self, path: Path, byte_count: int) -> None:
        remaining = byte_count
        with path.open("xb") as handle:
            while remaining:
                size = min(1024 * 1024, remaining)
                chunk = self.random_bytes(size)
                if len(chunk) != size:
                    raise RuntimeError(
                        f"random source returned {len(chunk)} bytes, expected {size}"
                    )
                handle.write(chunk)
                remaining -= size
            handle.flush()
            os.fsync(handle.fileno())
        if path.stat().st_size != byte_count:
            raise RuntimeError(
                f"source byte count mismatch: {path.stat().st_size} != {byte_count}"
            )

    @staticmethod
    def _copy_without_barrier(source: Path, destination: Path) -> int:
        started = time.monotonic_ns()
        with source.open("rb") as reader, destination.open("xb") as writer:
            while chunk := reader.read(1024 * 1024):
                writer.write(chunk)
        return max(1, time.monotonic_ns() - started)

    @staticmethod
    def _fsync(path: Path) -> int:
        started = time.monotonic_ns()
        with path.open("r+b") as handle:
            os.fsync(handle.fileno())
        return max(1, time.monotonic_ns() - started)

    @staticmethod
    def _rate(byte_count: int, elapsed_ns: int) -> float:
        return round(byte_count / 1_048_576 / (elapsed_ns / 1_000_000_000), 3)

    @staticmethod
    def _write_ledger(
        path: Path,
        resources: list[Path],
        attempts: int,
        asserted_clean: bool,
    ) -> None:
        path.write_text(
            json.dumps(
                {
                    "schema": 1,
                    "resources": [str(resource) for resource in resources],
                    "cleanup_attempts": attempts,
                    "asserted_clean": asserted_clean,
                },
                indent=2,
                sort_keys=True,
            )
            + "\n",
            encoding="utf-8",
        )

    @staticmethod
    def _cleanup_once(files: list[Path], directories: list[Path]) -> None:
        for path in files:
            path.unlink(missing_ok=True)
        for path in directories:
            if path.exists():
                path.rmdir()

    @staticmethod
    def _assert_clean(resources: list[Path]) -> None:
        remaining = [str(path) for path in resources if path.exists()]
        if remaining:
            raise RuntimeError(f"protocol benchmark cleanup incomplete: {remaining}")

    def run(
        self,
        scenario: ScenarioDefinition,
        authority: ProtocolAuthority,
    ) -> ProtocolMatrixResult:
        if scenario.kind != "protocol-matrix" or scenario.protocol is None:
            raise ValueError(f"not a protocol-matrix scenario: {scenario.name}")
        if scenario.protocol != authority.protocol:
            raise ValueError(
                f"scenario protocol {scenario.protocol} does not match authority "
                f"{authority.protocol}"
            )
        total_bytes = sum(workload.bytes for workload in scenario.workloads)
        if total_bytes <= 0:
            raise ValueError(f"scenario {scenario.name!r} has no byte-moving work")

        self.lifecycle.status()
        self.lifecycle.drain()
        authority_receipt = authority.verify(self.runner)
        receipt = RunReceipt.start(self.config, scenario.name)
        run_id = uuid.uuid4().hex
        run_root = authority.require_run_root(
            authority.mountpoint / f".zerofs-protocol-bench-{run_id}"
        )
        scratch = self.config.temp_dir / f"zerofs-protocol-bench-{run_id}"
        self.config.require_temp_child(scratch, "zerofs-protocol-bench-")
        run_root.mkdir(mode=0o700)
        scratch.mkdir(mode=0o700)
        files: list[Path] = []
        directories = [run_root, scratch]
        resources: list[Path] = [run_root, scratch]
        attempts = 0
        asserted_clean = False
        ledger = receipt.path("cleanup-ledger.json")
        results: list[ProtocolWorkloadResult] = []
        primary: BaseException | None = None

        with receipt:
            receipt.record("scenario", scenario.to_dict())
            receipt.record("authority", authority_receipt)
            try:
                for index, workload in enumerate(scenario.workloads):
                    self.lifecycle.drain()
                    source = scratch / f"source-{index}.bin"
                    destination = run_root / f"payload-{index}.bin"
                    files.extend((destination, source))
                    resources.extend((destination, source))
                    self._write_ledger(ledger, resources, attempts, asserted_clean)
                    self._create_source(source, workload.bytes)
                    source_sha256 = file_sha256(source)
                    before = self.lifecycle.metrics.snapshot()
                    write_started = time.monotonic_ns()
                    foreground_ns = self._copy_without_barrier(source, destination)
                    accepted = wait_for_accepted_after(
                        self.lifecycle.metrics.snapshot,
                        previous_sequence=before.accepted,
                        timeout=self.config.drain_timeout,
                    )
                    fsync_ns = self._fsync(destination)
                    local = wait_for_local(
                        self.lifecycle.metrics.snapshot,
                        target_sequence=accepted.accepted,
                        timeout=self.config.drain_timeout,
                    )
                    local_cutoff_ns = max(1, time.monotonic_ns() - write_started)
                    remote = wait_for_remote(
                        self.lifecycle.metrics.snapshot,
                        target_sequence=accepted.accepted,
                        timeout=self.config.drain_timeout,
                    )
                    remote_cutoff_ns = max(1, time.monotonic_ns() - write_started)
                    if destination.stat().st_size != workload.bytes:
                        raise RuntimeError(
                            f"protocol write byte count mismatch for {workload.name}: "
                            f"{destination.stat().st_size} != {workload.bytes}"
                        )
                    read_started = time.monotonic_ns()
                    readback_sha256 = file_sha256(destination)
                    readback_ns = max(1, time.monotonic_ns() - read_started)
                    if readback_sha256 != source_sha256:
                        raise RuntimeError(
                            f"protocol readback SHA-256 mismatch for {workload.name}: "
                            f"source={source_sha256}, readback={readback_sha256}"
                        )
                    results.append(
                        ProtocolWorkloadResult(
                            name=workload.name,
                            bytes=workload.bytes,
                            pattern=workload.pattern,
                            source_sha256=source_sha256,
                            readback_sha256=readback_sha256,
                            target_sequence=accepted.accepted,
                            foreground_close_ns=foreground_ns,
                            fsync_or_commit_ns=fsync_ns,
                            local_cutoff_ns=local_cutoff_ns,
                            remote_cutoff_ns=remote_cutoff_ns,
                            readback_ns=readback_ns,
                            foreground_mibps=self._rate(workload.bytes, foreground_ns),
                            readback_mibps=self._rate(workload.bytes, readback_ns),
                            before=before,
                            accepted=accepted,
                            local=local,
                            remote=remote,
                        )
                    )
                    destination.unlink()
                    source.unlink()
                self.lifecycle.drain()
            except BaseException as error:
                primary = error
                raise
            finally:
                cleanup_errors: list[BaseException] = []
                for _ in range(2):
                    attempts += 1
                    try:
                        self._cleanup_once(files, directories)
                    except BaseException as error:
                        cleanup_errors.append(error)
                    self._write_ledger(ledger, resources, attempts, asserted_clean)
                try:
                    self._assert_clean(resources)
                    asserted_clean = True
                except BaseException as error:
                    cleanup_errors.append(error)
                self._write_ledger(ledger, resources, attempts, asserted_clean)
                if cleanup_errors:
                    detail = "; ".join(str(error) for error in cleanup_errors)
                    if primary is not None:
                        primary.add_note(f"cleanup failures: {detail}")
                    else:
                        raise RuntimeError(f"protocol benchmark cleanup failed: {detail}")

        cleanup = CleanupEvidence(
            resources=tuple(str(path) for path in resources),
            attempts=attempts,
            asserted_clean=asserted_clean,
        )
        result = ProtocolMatrixResult(
            scenario=scenario.name,
            protocol=scenario.protocol,
            authority=authority_receipt,
            total_bytes=total_bytes,
            workloads=tuple(results),
            cleanup=cleanup,
            receipt_dir=str(receipt.directory),
        )
        receipt.path("summary.json").write_text(
            json.dumps(result.to_dict(), indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        return result
