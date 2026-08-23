from __future__ import annotations

import ipaddress
import json
import os
import time
import uuid
from dataclasses import asdict, dataclass
from contextlib import nullcontext
from pathlib import Path
from typing import Callable, Mapping

from .config import PilotConfig
from .memory_envelope import MemoryEnvelopeSession
from .metrics import (
    MetricsAuthorityIdentity,
    WritebackSnapshot,
    validate_metrics_url,
    wait_for_accepted_after,
    wait_for_local,
    wait_for_remote,
)
from .owned_resources import (
    assert_absent,
    atomic_write_json,
    remove_empty_directory,
    unlink_file,
)
from .receipts import RunReceipt
from .runner import Runner
from .scenarios import ProtocolScenario, WorkloadDefinition
from .system_io import (
    aggregate_fio_jobs,
    counter_delta,
    file_sha256,
    load_fio_jobs,
    mib_per_second,
)
from .writeback_observer import WritebackObserver


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
    metrics_url: str
    metrics_identity: MetricsAuthorityIdentity
    isolated_test_export: bool

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
        metrics_url = values.get(f"{prefix}_METRICS_URL", "").strip()
        isolated_value = values.get(f"{prefix}_ISOLATED", "").strip().lower()
        if isolated_value not in {"", "false", "true"}:
            raise ValueError(f"{prefix}_ISOLATED must be true or false")
        identity_values = {
            "server_instance_id": values.get(
                f"{prefix}_METRICS_INSTANCE_ID", ""
            ).strip(),
            "filesystem_id": values.get(
                f"{prefix}_METRICS_FILESYSTEM_ID", ""
            ).strip(),
            "export_id": values.get(f"{prefix}_METRICS_EXPORT_ID", "").strip(),
        }
        if (
            not mountpoint
            or not endpoint
            or not options
            or not metrics_url
            or not all(identity_values.values())
        ):
            label = "NFS" if protocol == "nfs" else "9P"
            raise ScenarioUnavailableError(
                f"{label} benchmark unavailable: set {prefix}_MOUNTPOINT, "
                f"{prefix}_ENDPOINT, {prefix}_MOUNT_OPTIONS, and "
                f"{prefix}_METRICS_URL, {prefix}_METRICS_INSTANCE_ID, "
                f"{prefix}_METRICS_FILESYSTEM_ID, and "
                f"{prefix}_METRICS_EXPORT_ID explicitly"
            )
        root = Path(mountpoint)
        if not root.is_absolute():
            raise ValueError(f"{protocol} mountpoint must be absolute: {root}")
        try:
            metrics = validate_metrics_url(metrics_url)
        except ValueError:
            raise ValueError(
                "ZeroFS metrics endpoint must be credential-free HTTPS without "
                "query or fragment"
            ) from None
        if protocol == "nfs":
            host = _nfs_host(endpoint)
            try:
                ipaddress.ip_address(host)
            except ValueError as error:
                raise ValueError(
                    "NFS benchmark endpoint must use a literal IP; mutable host "
                    f"aliases are not authority: {endpoint!r}"
                ) from error
            if metrics.hostname != host:
                raise ValueError(
                    "NFS mount and metrics authority must use the same literal "
                    f"server IP: mount={host!r}, metrics={metrics.hostname!r}"
                )
        elif metrics.hostname not in {"127.0.0.1", "::1"}:
            raise ValueError(
                "9P benchmark metrics must use explicit loopback authority for "
                f"the local server: {metrics.hostname!r}"
            )
        parsed_options = _options(options)
        if protocol == "9p" and (
            "trans=unix" not in parsed_options
            or identity_values["export_id"] != endpoint
        ):
            raise ScenarioUnavailableError(
                "9P benchmark unavailable: require trans=unix and an export ID "
                "equal to the exact local findmnt source"
            )
        return cls(
            protocol=protocol,
            mountpoint=root.resolve(strict=False),
            endpoint=endpoint,
            mount_options=parsed_options,
            metrics_url=metrics_url,
            metrics_identity=MetricsAuthorityIdentity(**identity_values),
            isolated_test_export=isolated_value == "true",
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
            "metrics_url": self.metrics_url,
            "metrics_identity": asdict(self.metrics_identity),
            "isolated_test_export": self.isolated_test_export,
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
class ClientColdReadResult:
    bytes: int
    runtime_ms: int
    requests: int
    mibps: float
    backend_interval_counter_before: int
    backend_interval_counter_after: int
    backend_interval_activity_bytes: int
    idle_seconds: int
    timeout_seconds: int
    cache_scope: str = "nfs_client_page_cache_only"
    backend_activity_scope: str = "service_global_interval"
    isolated_test_export_required: bool = True


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
    stable_remote_drain_ns: int
    stable_remote_drain: dict[str, object]
    readback_ns: int
    foreground_mibps: float
    # Integrity-check rate, not protocol read throughput: hashes the
    # just-written file back through the same mount with no cache
    # invalidation (often client-cached) and includes SHA-256 CPU cost.
    readback_mibps: float
    before: WritebackSnapshot
    accepted: WritebackSnapshot
    local: WritebackSnapshot
    remote: WritebackSnapshot
    client_cold_read: ClientColdReadResult | None = None


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
    memory_envelope: dict[str, object] | None
    cleanup: CleanupEvidence
    receipt_dir: str

    def to_dict(self) -> dict[str, object]:
        return {"schema": 1, **asdict(self)}


@dataclass(slots=True)
class ProtocolWorkloadExecutor:
    owner: "ProtocolMatrixRunner"
    run_root: Path
    scratch: Path
    ledger: Path
    files: list[Path]
    resources: list[Path]
    receipt: RunReceipt
    scenario: ProtocolScenario

    def run(
        self,
        workload: WorkloadDefinition,
        index: int,
    ) -> ProtocolWorkloadResult:
        observer = self.owner.observer
        observer.drain()
        source = self.scratch / f"source-{index}.bin"
        destination = self.run_root / f"payload-{index}.bin"
        self.files.extend((destination, source))
        self.resources.extend((destination, source))
        self.owner._write_ledger(self.ledger, self.resources, 0, False)
        self.owner._create_source(source, workload.bytes)
        source_sha256 = file_sha256(source)
        before = observer.snapshot()
        write_started = time.monotonic_ns()
        foreground_ns = self.owner._copy_without_barrier(source, destination)
        memory = self.owner.memory_session
        if memory is not None:
            memory.sample(f"foreground_close:{workload.name}")
        accepted = wait_for_accepted_after(
            observer.snapshot,
            previous_sequence=before.accepted,
            timeout=self.owner.config.drain_timeout,
        )
        fsync_ns = self.owner._fsync(destination)
        if memory is not None:
            memory.sample(f"fsync_or_commit:{workload.name}")
        local = wait_for_local(
            observer.snapshot,
            target_sequence=accepted.accepted,
            timeout=self.owner.config.drain_timeout,
        )
        local_cutoff_ns = max(1, time.monotonic_ns() - write_started)
        if memory is not None:
            memory.sample(f"local:{workload.name}")
        remote = wait_for_remote(
            observer.snapshot,
            target_sequence=accepted.accepted,
            timeout=self.owner.config.drain_timeout,
        )
        remote_cutoff_ns = max(1, time.monotonic_ns() - write_started)
        stable_remote_drain = observer.drain()
        try:
            stable_snapshot = stable_remote_drain["snapshot"]
            stable_local_bytes = int(stable_snapshot["local_bytes"])
            stable_remote_bytes = int(stable_snapshot["remote_bytes"])
        except (KeyError, TypeError, ValueError) as error:
            raise RuntimeError("stable drain receipt lacks byte counters") from error
        # Guarded, not raw: a ZeroFS restart inside the workload resets these
        # counters, and a bare subtraction would surface that as a confusing
        # byte-attribution mismatch instead of naming the regressed counter.
        local_bytes = counter_delta(
            stable_local_bytes,
            before.local_bytes,
            f"{workload.name} local encoded bytes",
        )
        remote_bytes = counter_delta(
            stable_remote_bytes,
            before.remote_bytes,
            f"{workload.name} remote encoded bytes",
        )
        if local_bytes != workload.bytes or remote_bytes != workload.bytes:
            raise RuntimeError(
                f"protocol durability byte attribution mismatch for {workload.name}: "
                f"expected={workload.bytes}, local={local_bytes}, remote={remote_bytes}"
            )
        stable_remote_drain_ns = max(
            remote_cutoff_ns,
            time.monotonic_ns() - write_started,
        )
        if memory is not None:
            memory.sample(f"remote:{workload.name}")
        if destination.stat().st_size != workload.bytes:
            raise RuntimeError(
                f"protocol write byte count mismatch for {workload.name}: "
                f"{destination.stat().st_size} != {workload.bytes}"
            )
        client_cold_read = None
        if self.scenario.require_backend_interval_activity:
            self.receipt.record(
                "idle_read_attempt",
                {
                    "cache_scope": "nfs_client_page_cache_only",
                    "deadline_seconds": self.scenario.read_timeout_seconds,
                    "d_state_bounded": False,
                    "timeout_scope": "userspace_process_only",
                    "workload": workload.name,
                },
            )
            client_cold_read = self.owner._run_client_cold_read(
                destination,
                workload,
                output=self.receipt.path(f"client-cold-read-{index}-fio.json"),
                scenario=self.scenario,
            )
        read_started = time.monotonic_ns()
        readback_sha256 = file_sha256(destination)
        readback_ns = max(1, time.monotonic_ns() - read_started)
        if readback_sha256 != source_sha256:
            raise RuntimeError(
                f"protocol readback SHA-256 mismatch for {workload.name}: "
                f"source={source_sha256}, readback={readback_sha256}"
            )
        result = ProtocolWorkloadResult(
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
            stable_remote_drain_ns=stable_remote_drain_ns,
            stable_remote_drain=stable_remote_drain,
            readback_ns=readback_ns,
            foreground_mibps=self.owner._rate(workload.bytes, foreground_ns),
            readback_mibps=self.owner._rate(workload.bytes, readback_ns),
            before=before,
            accepted=accepted,
            local=local,
            remote=remote,
            client_cold_read=client_cold_read,
        )
        destination.unlink()
        source.unlink()
        return result


class ProtocolMatrixRunner:
    def __init__(
        self,
        config: PilotConfig,
        runner: Runner,
        observer: WritebackObserver,
        *,
        random_bytes: Callable[[int], bytes] = os.urandom,
        memory_session: MemoryEnvelopeSession | None = None,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self.config = config
        self.runner = runner
        self.observer = observer
        self.random_bytes = random_bytes
        self.memory_session = memory_session
        self.sleep = sleep

    def _run_client_cold_read(
        self,
        path: Path,
        workload: WorkloadDefinition,
        *,
        output: Path,
        scenario: ProtocolScenario,
    ) -> ClientColdReadResult:
        self.sleep(scenario.read_idle_seconds)
        before = self.observer.counter("zerofs_sftp_object_read_bytes_total")
        self.runner.run(
            [
                "fio",
                "--name=zerofs_nfs_idle_client_cold_read",
                f"--filename={path}",
                "--rw=read",
                "--bs=1M",
                f"--size={workload.bytes}",
                "--numjobs=1",
                "--direct=0",
                "--invalidate=1",
                "--fadvise_hint=0",
                "--allow_file_create=0",
                "--readonly",
                "--group_reporting",
                "--output-format=json",
                f"--output={output}",
            ],
            timeout=scenario.read_timeout_seconds,
            capture=False,
        )
        aggregate = aggregate_fio_jobs(
            load_fio_jobs(output),
            operation="read",
            path=output,
            require_request_counters=True,
        )
        expected_requests = workload.bytes // (1024 * 1024)
        if aggregate.errors:
            raise RuntimeError(f"fio reported I/O errors={aggregate.errors}: {output}")
        if aggregate.byte_count != workload.bytes:
            raise RuntimeError(
                "client-cold fio returned short I/O: "
                f"expected={workload.bytes}, actual={aggregate.byte_count}"
            )
        if aggregate.requests != expected_requests:
            raise RuntimeError(
                "client-cold fio request count mismatch: "
                f"expected={expected_requests}, actual={aggregate.requests}"
            )
        if aggregate.runtime_ms <= 0:
            raise ValueError("client-cold fio has no measurable runtime")
        after = self.observer.counter("zerofs_sftp_object_read_bytes_total")
        backend_activity_bytes = counter_delta(
            after,
            before,
            "long-idle client-cold service-global SFTP read activity bytes",
        )
        if backend_activity_bytes < aggregate.byte_count:
            raise RuntimeError(
                "long-idle client-cold NFS interval observed fewer backend SFTP "
                "activity bytes "
                f"than logical bytes: logical={aggregate.byte_count}, "
                f"backend_interval={backend_activity_bytes}; the interval-global "
                "evidence is insufficient"
            )
        return ClientColdReadResult(
            bytes=aggregate.byte_count,
            runtime_ms=aggregate.runtime_ms,
            requests=aggregate.requests,
            mibps=mib_per_second(aggregate.byte_count, aggregate.runtime_ms / 1000),
            backend_interval_counter_before=before,
            backend_interval_counter_after=after,
            backend_interval_activity_bytes=backend_activity_bytes,
            idle_seconds=scenario.read_idle_seconds,
            timeout_seconds=scenario.read_timeout_seconds,
        )

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
        # Nanoseconds, unlike the millisecond call sites, because the protocol
        # cutoffs this feeds are recorded as `*_ns` and an fsync can be
        # sub-millisecond. Every caller passes a `max(1, ...)` duration, so no
        # zero guard is needed here.
        return mib_per_second(byte_count, elapsed_ns / 1_000_000_000)

    @staticmethod
    def _write_ledger(
        path: Path,
        resources: list[Path],
        attempts: int,
        asserted_clean: bool,
    ) -> None:
        atomic_write_json(
            path,
            {
                    "schema": 1,
                    "resources": [str(resource) for resource in resources],
                    "cleanup_attempts": attempts,
                    "asserted_clean": asserted_clean,
                },
        )

    @staticmethod
    def _cleanup_once(files: list[Path], directories: list[Path]) -> None:
        for path in files:
            unlink_file(path)
        for path in directories:
            remove_empty_directory(path)

    @staticmethod
    def _assert_clean(resources: list[Path]) -> None:
        assert_absent(resources)

    def run(
        self,
        scenario: ProtocolScenario,
        authority: ProtocolAuthority,
        *,
        receipt: RunReceipt | None = None,
    ) -> ProtocolMatrixResult:
        if scenario.protocol != authority.protocol:
            raise ValueError(
                f"scenario protocol {scenario.protocol} does not match authority "
                f"{authority.protocol}"
            )
        if self.observer.metrics_endpoint != authority.metrics_url:
            raise ValueError(
                "protocol mount and writeback observer metrics authority differ: "
                f"mount={authority.metrics_url!r}, "
                f"observer={self.observer.metrics_endpoint!r}"
            )
        total_bytes = sum(workload.bytes for workload in scenario.workloads)
        if total_bytes <= 0:
            raise ValueError(f"scenario {scenario.name!r} has no byte-moving work")

        owns_receipt = receipt is None
        receipt = receipt or RunReceipt.start(self.config, scenario.name)
        run_id = uuid.uuid4().hex
        run_root = authority.mountpoint / f".zerofs-protocol-bench-{run_id}"
        scratch = self.config.temp_dir / f"zerofs-protocol-bench-{run_id}"
        files: list[Path] = []
        directories = [run_root, scratch]
        resources: list[Path] = [run_root, scratch]
        attempts = 0
        asserted_clean = False
        ledger = receipt.directory / "cleanup-ledger.json"
        results: list[ProtocolWorkloadResult] = []
        primary: BaseException | None = None
        memory_envelope: dict[str, object] | None = None
        authority_receipt: dict[str, object] = {}

        with receipt if owns_receipt else nullcontext(receipt):
            receipt.record("scenario", scenario.to_dict())
            receipt.record(
                "requested_authority",
                {
                    "protocol": authority.protocol,
                    "mountpoint": str(authority.mountpoint),
                    "endpoint": authority.endpoint,
                    "mount_options": list(authority.mount_options),
                    "metrics_url": authority.metrics_url,
                    "metrics_identity": asdict(authority.metrics_identity),
                    "isolated_test_export": authority.isolated_test_export,
                },
            )
            receipt.record(
                "requested_observer",
                {"metrics_endpoint": self.observer.metrics_endpoint},
            )
            try:
                receipt.artifact("cleanup-ledger.json", ledger)
                authority.require_run_root(run_root)
                self.config.require_temp_child(scratch, "zerofs-protocol-bench-")
                if (
                    scenario.require_backend_interval_activity
                    and not authority.isolated_test_export
                ):
                    raise ScenarioUnavailableError(
                        "long-idle backend interval evidence requires explicit "
                        "ZEROFS_BENCH_NFS_ISOLATED=true authority"
                    )
                if self.memory_session is not None:
                    self.memory_session.expect_workloads(
                        tuple(workload.name for workload in scenario.workloads)
                    )
                    self.memory_session.attach_artifact(
                        receipt.path("memory-envelope.json")
                    )
                    if not self.memory_session.samples:
                        self.memory_session.begin()
                try:
                    observed_identity = self.observer.identity()
                except (ValueError, OSError) as error:
                    raise ScenarioUnavailableError(
                        "ZeroFS metrics endpoint does not expose one immutable "
                        "benchmark authority identity"
                    ) from error
                if observed_identity != authority.metrics_identity:
                    raise ScenarioUnavailableError(
                        "ZeroFS metrics identity mismatch: "
                        f"expected={asdict(authority.metrics_identity)}, "
                        f"actual={asdict(observed_identity)}"
                    )
                receipt.record("metrics_identity", asdict(observed_identity))
                observer_status = self.observer.status()
                receipt.record("writeback_observer", observer_status)
                self.observer.drain()
                authority_receipt = authority.verify(self.runner)
                receipt.record("authority", authority_receipt)
                self._write_ledger(ledger, resources, attempts, asserted_clean)
                run_root.mkdir(mode=0o700)
                scratch.mkdir(mode=0o700)
                workload_executor = ProtocolWorkloadExecutor(
                    self,
                    run_root,
                    scratch,
                    ledger,
                    files,
                    resources,
                    receipt,
                    scenario,
                )
                for index, workload in enumerate(scenario.workloads):
                    results.append(workload_executor.run(workload, index))
                self.observer.drain()
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
                    try:
                        self._write_ledger(
                            ledger, resources, attempts, asserted_clean
                        )
                    except BaseException as error:
                        cleanup_errors.append(error)
                try:
                    self._assert_clean(resources)
                    asserted_clean = True
                except BaseException as error:
                    cleanup_errors.append(error)
                try:
                    self._write_ledger(
                        ledger, resources, attempts, asserted_clean
                    )
                except BaseException as error:
                    cleanup_errors.append(error)
                if self.memory_session is not None:
                    try:
                        memory_envelope = self.memory_session.finish_after_cleanup(
                            require_complete=primary is None and not cleanup_errors
                        ).to_dict()
                    except BaseException as error:
                        cleanup_errors.append(error)
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
                memory_envelope=memory_envelope,
                cleanup=cleanup,
                receipt_dir=str(receipt.directory),
            )
            atomic_write_json(receipt.path("summary.json"), result.to_dict())
        return result
