from __future__ import annotations

from dataclasses import asdict, dataclass
from types import MappingProxyType
from typing import Mapping, TypeVar


class UnknownScenarioError(ValueError):
    pass


@dataclass(frozen=True, slots=True)
class WorkloadDefinition:
    name: str
    bytes: int
    pattern: str

    def __post_init__(self) -> None:
        if not self.name or self.bytes <= 0 or not self.pattern:
            raise ValueError("a workload must name real, nonzero work")


@dataclass(frozen=True, slots=True)
class MemoryLimits:
    cgroup_current_bytes: int
    cgroup_peak_bytes: int
    pid_rss_bytes: int
    swap_bytes: int

    def __post_init__(self) -> None:
        if min(
            self.cgroup_current_bytes,
            self.cgroup_peak_bytes,
            self.pid_rss_bytes,
        ) <= 0:
            raise ValueError("memory ceilings must be positive")
        if self.swap_bytes < 0:
            raise ValueError("swap ceiling must not be negative")


_PROTOCOL_AUTHORITY = (
    "mountpoint",
    "endpoint",
    "mount_options",
    "metrics_endpoint",
    "metrics_server_instance_id",
    "metrics_filesystem_id",
    "metrics_export_id",
)
_PROTOCOL_CUTOFFS = (
    "foreground_close",
    "fsync_or_commit",
    "local",
    "remote_sequence_crossing",
    "stable_remote_drain",
)


@dataclass(frozen=True, slots=True)
class ProtocolScenario:
    name: str
    protocol: str
    description: str
    workloads: tuple[WorkloadDefinition, ...]
    read_idle_seconds: int = 0
    read_timeout_seconds: int = 0
    require_backend_interval_activity: bool = False

    def __post_init__(self) -> None:
        if self.protocol not in {"nfs", "9p"}:
            raise ValueError(f"unsupported protocol scenario: {self.protocol!r}")
        if not self.name or not self.description or not self.workloads:
            raise ValueError("protocol scenario must define real workloads")
        if self.require_backend_interval_activity:
            if self.protocol != "nfs":
                raise ValueError("backend-observed long-idle reads currently require NFS")
            if self.read_idle_seconds <= 0 or self.read_timeout_seconds <= 0:
                raise ValueError(
                    "backend-observed long-idle reads require positive idle and timeout seconds"
                )
            if any(workload.bytes % (1024 * 1024) for workload in self.workloads):
                raise ValueError("backend-observed read workloads must be whole MiB")
        elif self.read_idle_seconds or self.read_timeout_seconds:
            raise ValueError("read timing is only valid for a backend-observed read")

    @property
    def kind(self) -> str:
        return "protocol-matrix"

    @property
    def required_authority(self) -> tuple[str, ...]:
        if self.require_backend_interval_activity:
            return (*_PROTOCOL_AUTHORITY, "isolated_test_export_assertion")
        return _PROTOCOL_AUTHORITY

    @property
    def cutoffs(self) -> tuple[str, ...]:
        if self.require_backend_interval_activity:
            return (*_PROTOCOL_CUTOFFS, "idle_client_cold_backend_interval")
        return _PROTOCOL_CUTOFFS

    @property
    def sha256_required(self) -> bool:
        return True

    @property
    def cleanup_required(self) -> bool:
        return True

    def to_dict(self) -> dict[str, object]:
        result: dict[str, object] = {
            "schema": 1,
            "name": self.name,
            "kind": self.kind,
            "protocol": self.protocol,
            "description": self.description,
            "workloads": [asdict(workload) for workload in self.workloads],
            "required_authority": list(self.required_authority),
            "cutoffs": list(self.cutoffs),
            "sha256_required": True,
            "cleanup_required": True,
        }
        if self.require_backend_interval_activity:
            result["read_probe"] = {
                "idle_seconds": self.read_idle_seconds,
                "timeout_seconds": self.read_timeout_seconds,
                "cache_scope": "nfs_client_page_cache_only",
                "backend_counter": "zerofs_sftp_object_read_bytes_total",
                "backend_activity_scope": "service_global_interval",
                "isolated_test_export_required": True,
            }
        return result


@dataclass(frozen=True, slots=True)
class RawSftpScenario:
    name: str
    description: str
    jobs: int
    per_job_bytes: int
    buffer_bytes: int
    request_depth: int
    repetitions: int
    pattern: str = "incompressible-random-v1"

    def __post_init__(self) -> None:
        geometry = (
            self.jobs,
            self.per_job_bytes,
            self.buffer_bytes,
            self.request_depth,
            self.repetitions,
        )
        if not self.name or not self.description or min(geometry) <= 0:
            raise ValueError("raw SFTP scenario must define positive real work")
        if self.repetitions % 2:
            raise ValueError("raw SFTP repetitions must be even")
        if self.per_job_bytes % 1_048_576:
            raise ValueError("raw SFTP bytes per job must be whole MiB")

    @property
    def kind(self) -> str:
        return "raw-sftp-ab"

    @property
    def protocol(self) -> str:
        return "sftp"

    @property
    def total_bytes_per_trial(self) -> int:
        return self.jobs * self.per_job_bytes

    def to_dict(self) -> dict[str, object]:
        return {
            "schema": 1,
            "name": self.name,
            "kind": self.kind,
            "protocol": self.protocol,
            "description": self.description,
            "geometry": {
                "jobs": self.jobs,
                "per_job_bytes": self.per_job_bytes,
                "total_bytes_per_trial": self.total_bytes_per_trial,
                "buffer_bytes": self.buffer_bytes,
                "request_depth": self.request_depth,
                "repetitions": self.repetitions,
                "pattern": self.pattern,
            },
            "required_authority": [
                "stock_ssh_binary",
                "hpn_ssh_binary",
                "endpoint",
                "host_key",
            ],
            "cutoffs": ["close_ack", "remote_durability_unproven"],
            "sha256_required": True,
            "cleanup_required": True,
        }


@dataclass(frozen=True, slots=True)
class MemoryEnvelopeScenario:
    name: str
    description: str
    phases: tuple[str, ...]
    limits: MemoryLimits

    def __post_init__(self) -> None:
        if not self.name or not self.description:
            raise ValueError("memory-envelope scenario must be named")
        if len(self.phases) < 4 or self.phases[0] != "before":
            raise ValueError("memory-envelope phases must start with before")
        if self.phases[-1] != "after_cleanup" or len(set(self.phases)) != len(
            self.phases
        ):
            raise ValueError(
                "memory-envelope phases must be unique and end with after_cleanup"
            )

    @property
    def kind(self) -> str:
        return "memory-envelope"

    def to_dict(self) -> dict[str, object]:
        return {
            "schema": 1,
            "name": self.name,
            "kind": self.kind,
            "description": self.description,
            "required_authority": ["cgroup", "pid", "service"],
            "sample_phases": list(self.phases),
            "memory_limits": asdict(self.limits),
            "cleanup_required": False,
            "cleanup_semantics": "observer-owned-no-resources",
        }


Scenario = ProtocolScenario | RawSftpScenario | MemoryEnvelopeScenario

_PROTOCOL_WORKLOADS = (
    WorkloadDefinition(
        name="sequential-64m",
        bytes=64 * 1024 * 1024,
        pattern="incompressible-random-v1",
    ),
    WorkloadDefinition(
        name="sequential-1g",
        bytes=1024 * 1024 * 1024,
        pattern="incompressible-random-v1",
    ),
)

_MEMORY_LIMITS = MemoryLimits(
    cgroup_current_bytes=96 << 30,
    cgroup_peak_bytes=112 << 30,
    pid_rss_bytes=80 << 30,
    swap_bytes=0,
)

_DEFINITIONS: tuple[Scenario, ...] = (
    ProtocolScenario(
        name="protocol-matrix-nfs",
        protocol="nfs",
        description=(
            "NFS shared-namespace writes and reads with separate close, COMMIT, "
            "local, and remote durability evidence"
        ),
        workloads=_PROTOCOL_WORKLOADS,
    ),
    ProtocolScenario(
        name="protocol-matrix-9p",
        protocol="9p",
        description=(
            "9P shared-namespace writes and reads with separate close, fsync, "
            "local, and remote durability evidence"
        ),
        workloads=_PROTOCOL_WORKLOADS,
    ),
    ProtocolScenario(
        name="protocol-idle-read-nfs",
        protocol="nfs",
        description=(
            "NFS read after the configured SFTP pool was idle, with client-cache "
            "invalidation and service-global backend interval evidence"
        ),
        workloads=(_PROTOCOL_WORKLOADS[0],),
        read_idle_seconds=61 * 60,
        read_timeout_seconds=30,
        require_backend_interval_activity=True,
    ),
    RawSftpScenario(
        name="raw-sftp-stock-hpn",
        description=(
            "Counterbalanced stock-versus-HPN SFTP control with identical "
            "payload, concurrency, buffer, request depth, and SHA verification"
        ),
        jobs=4,
        per_job_bytes=128 * 1024 * 1024,
        buffer_bytes=1_048_576,
        request_depth=128,
        repetitions=4,
    ),
    MemoryEnvelopeScenario(
        name="memory-envelope",
        description=(
            "Sample a pinned ZeroFS process and cgroup at protocol phase "
            "boundaries and reject OOM, restart, terminal, or ceiling breaches"
        ),
        phases=(
            "before",
            "foreground_close",
            "fsync_or_commit",
            "local",
            "remote",
            "after_cleanup",
        ),
        limits=_MEMORY_LIMITS,
    ),
)

_SCENARIOS: Mapping[str, Scenario] = MappingProxyType(
    {definition.name: definition for definition in _DEFINITIONS}
)


def list_scenarios() -> tuple[Scenario, ...]:
    return tuple(_SCENARIOS.values())


def require_scenario(name: str) -> Scenario:
    try:
        return _SCENARIOS[name]
    except KeyError as error:
        registered = ", ".join(_SCENARIOS)
        raise UnknownScenarioError(
            f"scenario {name!r} is not registered; choose one of: {registered}"
        ) from error


T = TypeVar("T", ProtocolScenario, RawSftpScenario, MemoryEnvelopeScenario)


def _require_type(name: str, expected: type[T]) -> T:
    scenario = require_scenario(name)
    if not isinstance(scenario, expected):
        raise TypeError(
            f"scenario {name!r} is {type(scenario).__name__}, "
            f"not {expected.__name__}"
        )
    return scenario


def require_protocol_scenario(name: str) -> ProtocolScenario:
    return _require_type(name, ProtocolScenario)


def require_raw_sftp_scenario(name: str) -> RawSftpScenario:
    return _require_type(name, RawSftpScenario)


def require_memory_scenario(name: str) -> MemoryEnvelopeScenario:
    return _require_type(name, MemoryEnvelopeScenario)
