from __future__ import annotations

from dataclasses import asdict, dataclass
from types import MappingProxyType
from typing import Mapping


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


@dataclass(frozen=True, slots=True)
class ScenarioDefinition:
    name: str
    kind: str
    protocol: str | None
    description: str
    workloads: tuple[WorkloadDefinition, ...] = ()
    required_authority: tuple[str, ...] = ()
    cutoffs: tuple[str, ...] = ()
    sha256_required: bool = False
    cleanup_required: bool = True
    repetitions: int = 1
    sample_phases: tuple[str, ...] = ()
    memory_limits: MemoryLimits | None = None

    def __post_init__(self) -> None:
        if not self.name or not self.kind or not self.description:
            raise ValueError("scenario identity must be complete")
        if self.repetitions <= 0:
            raise ValueError("scenario repetitions must be positive")
        if not self.workloads and not self.sample_phases:
            raise ValueError(f"scenario {self.name!r} would execute no work")

    def to_dict(self) -> dict[str, object]:
        return {"schema": 1, **asdict(self)}


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

_PROTOCOL_CUTOFFS = (
    "foreground_close",
    "fsync_or_commit",
    "local",
    "remote",
)

_PROTOCOL_AUTHORITY = ("mountpoint", "endpoint", "mount_options")

_MEMORY_LIMITS = MemoryLimits(
    cgroup_current_bytes=96 << 30,
    cgroup_peak_bytes=112 << 30,
    pid_rss_bytes=80 << 30,
    swap_bytes=0,
)

_DEFINITIONS = (
    ScenarioDefinition(
        name="protocol-matrix-nfs",
        kind="protocol-matrix",
        protocol="nfs",
        description=(
            "NFS shared-namespace writes and reads with separate close, COMMIT, "
            "local, and remote durability evidence"
        ),
        workloads=_PROTOCOL_WORKLOADS,
        required_authority=_PROTOCOL_AUTHORITY,
        cutoffs=_PROTOCOL_CUTOFFS,
        sha256_required=True,
    ),
    ScenarioDefinition(
        name="protocol-matrix-9p",
        kind="protocol-matrix",
        protocol="9p",
        description=(
            "9P shared-namespace writes and reads with separate close, fsync, "
            "local, and remote durability evidence"
        ),
        workloads=_PROTOCOL_WORKLOADS,
        required_authority=_PROTOCOL_AUTHORITY,
        cutoffs=_PROTOCOL_CUTOFFS,
        sha256_required=True,
    ),
    ScenarioDefinition(
        name="raw-sftp-stock-hpn",
        kind="raw-sftp-ab",
        protocol="sftp",
        description=(
            "Counterbalanced stock-versus-HPN SFTP control with identical "
            "payload, concurrency, buffer, request depth, and SHA verification"
        ),
        workloads=(
            WorkloadDefinition(
                name="parallel-128m",
                bytes=128 * 1024 * 1024,
                pattern="incompressible-random-v1",
            ),
        ),
        required_authority=(
            "stock_sftp_binary",
            "hpn_sftp_binary",
            "endpoint",
            "host_key",
        ),
        cutoffs=("close_ack", "remote_durability_unproven"),
        sha256_required=True,
        repetitions=4,
    ),
    ScenarioDefinition(
        name="memory-envelope",
        kind="memory-envelope",
        protocol=None,
        description=(
            "Sample a pinned ZeroFS process and cgroup at protocol phase "
            "boundaries and reject OOM, restart, terminal, or ceiling breaches"
        ),
        required_authority=("cgroup", "pid", "service"),
        sample_phases=(
            "before",
            "foreground_close",
            "fsync_or_commit",
            "local",
            "remote",
            "after_cleanup",
        ),
        memory_limits=_MEMORY_LIMITS,
    ),
)

_SCENARIOS: Mapping[str, ScenarioDefinition] = MappingProxyType(
    {definition.name: definition for definition in _DEFINITIONS}
)


def list_scenarios() -> tuple[ScenarioDefinition, ...]:
    return tuple(_SCENARIOS.values())


def require_scenario(name: str) -> ScenarioDefinition:
    try:
        return _SCENARIOS[name]
    except KeyError as error:
        registered = ", ".join(_SCENARIOS)
        raise UnknownScenarioError(
            f"scenario {name!r} is not registered; choose one of: {registered}"
        ) from error
