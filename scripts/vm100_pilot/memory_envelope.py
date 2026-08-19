from __future__ import annotations

import json
import re
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Mapping, Protocol

from .metrics import WritebackSnapshot
from .runner import Runner
from .scenarios import MemoryEnvelopeScenario, MemoryLimits


_SERVICE = re.compile(r"[A-Za-z0-9@_.-]+\.service\Z")
def _phase_kind(phase: str) -> str:
    return phase.split(":", 1)[0]


class SnapshotSource(Protocol):
    def snapshot(self) -> WritebackSnapshot: ...


def _absolute(path: Path, role: str) -> Path:
    if not path.is_absolute():
        raise ValueError(f"{role} must be absolute: {path}")
    return path.resolve(strict=False)


def _within(path: Path, root: Path, role: str) -> Path:
    resolved = _absolute(path, role)
    allowed = _absolute(root, f"{role} root")
    try:
        relative = resolved.relative_to(allowed)
    except ValueError as error:
        raise ValueError(f"{role} is outside {allowed}: {resolved}") from error
    if not relative.parts:
        raise ValueError(f"{role} must not be the cgroup root: {resolved}")
    return resolved


@dataclass(frozen=True, slots=True)
class MemoryEnvelopeAuthority:
    cgroup_root: Path
    cgroup_path: Path
    proc_root: Path
    service: str

    @classmethod
    def from_mapping(cls, values: Mapping[str, str]) -> "MemoryEnvelopeAuthority":
        root = Path(values.get("ZEROFS_BENCH_CGROUP_ROOT", "/sys/fs/cgroup"))
        configured = values.get("ZEROFS_BENCH_CGROUP_PATH", "").strip()
        service = values.get("ZEROFS_BENCH_SERVICE", "").strip()
        if not configured or not service:
            raise ValueError(
                "memory envelope requires ZEROFS_BENCH_CGROUP_PATH and "
                "ZEROFS_BENCH_SERVICE"
            )
        if not _SERVICE.fullmatch(service):
            raise ValueError(f"invalid systemd service authority: {service!r}")
        return cls(
            cgroup_root=_absolute(root, "cgroup root"),
            cgroup_path=_within(Path(configured), root, "cgroup path"),
            proc_root=_absolute(
                Path(values.get("ZEROFS_BENCH_PROC_ROOT", "/proc")),
                "proc root",
            ),
            service=service,
        )

    def to_dict(self) -> dict[str, str]:
        return {
            "cgroup_root": str(self.cgroup_root),
            "cgroup_path": str(self.cgroup_path),
            "proc_root": str(self.proc_root),
            "service": self.service,
        }


@dataclass(frozen=True, slots=True)
class MemorySample:
    phase: str
    pid: int
    restart_count: int
    control_group: str
    cgroup_current_bytes: int
    cgroup_peak_bytes: int
    cgroup_swap_bytes: int
    oom: int
    oom_kill: int
    pid_rss_bytes: int
    pid_hwm_bytes: int
    pid_swap_bytes: int
    writeback: WritebackSnapshot


@dataclass(frozen=True, slots=True)
class MemoryEnvelopeResult:
    scenario: str
    authority: MemoryEnvelopeAuthority
    limits: MemoryLimits
    samples: tuple[MemorySample, ...]
    cleanup_resources: tuple[str, ...]
    cleanup_attempts: int
    cleanup_asserted: bool
    cleanup_semantics: str
    complete: bool
    missing_phases: tuple[str, ...]

    def to_dict(self) -> dict[str, object]:
        return {
            "schema": 1,
            "scenario": self.scenario,
            "authority": self.authority.to_dict(),
            "limits": asdict(self.limits),
            "samples": [asdict(sample) for sample in self.samples],
            "cleanup_resources": list(self.cleanup_resources),
            "cleanup_attempts": self.cleanup_attempts,
            "cleanup_asserted": self.cleanup_asserted,
            "cleanup_semantics": self.cleanup_semantics,
            "complete": self.complete,
            "missing_phases": list(self.missing_phases),
        }


def _read_integer(path: Path) -> int:
    try:
        value = int(path.read_text(encoding="utf-8").strip())
    except (FileNotFoundError, PermissionError, ValueError) as error:
        raise RuntimeError(f"cannot read integer memory evidence from {path}") from error
    if value < 0:
        raise RuntimeError(f"negative memory evidence in {path}: {value}")
    return value


def _events(path: Path) -> tuple[int, int]:
    try:
        values = {
            key: int(value)
            for key, value in (
                line.split() for line in path.read_text(encoding="utf-8").splitlines()
            )
        }
        return values["oom"], values["oom_kill"]
    except (FileNotFoundError, PermissionError, KeyError, ValueError) as error:
        raise RuntimeError(f"cannot read OOM evidence from {path}") from error


def _status(path: Path) -> tuple[int, int, int]:
    fields: dict[str, int] = {}
    try:
        for line in path.read_text(encoding="utf-8").splitlines():
            key, separator, value = line.partition(":")
            if separator != ":" or key not in {"VmRSS", "VmHWM", "VmSwap"}:
                continue
            parts = value.split()
            if len(parts) != 2 or parts[1] != "kB":
                raise ValueError(f"invalid {key} field")
            fields[key] = int(parts[0]) * 1024
        return fields["VmRSS"], fields["VmHWM"], fields["VmSwap"]
    except (FileNotFoundError, PermissionError, KeyError, ValueError) as error:
        raise RuntimeError(f"cannot read process memory evidence from {path}") from error


@dataclass(frozen=True, slots=True)
class ServiceIdentity:
    pid: int
    restart_count: int
    control_group: str


def _service_identity(runner: Runner, service: str) -> ServiceIdentity:
    completed = runner.run(
        [
            "systemctl",
            "show",
            service,
            "--property=MainPID,NRestarts,ControlGroup",
        ]
    )
    try:
        values = dict(
            line.split("=", 1)
            for line in completed.stdout.splitlines()
            if "=" in line
        )
        pid = int(values["MainPID"])
        restarts = int(values["NRestarts"])
        control_group = values["ControlGroup"]
    except (KeyError, ValueError) as error:
        raise RuntimeError(
            f"cannot establish service identity for {service}: {completed.stdout!r}"
        ) from error
    if pid <= 0 or restarts < 0 or not control_group.startswith("/"):
        raise RuntimeError(
            f"invalid service identity for {service}: pid={pid}, restarts={restarts}"
        )
    return ServiceIdentity(pid, restarts, control_group)


class MemoryEnvelopeSession:
    def __init__(
        self,
        authority: MemoryEnvelopeAuthority,
        runner: Runner,
        metrics: SnapshotSource,
        scenario: MemoryEnvelopeScenario,
    ) -> None:
        self.authority = authority
        self.runner = runner
        self.metrics = metrics
        self.scenario = scenario
        self.limits = scenario.limits
        self._samples: list[MemorySample] = []
        self._pid = 0
        self._restart_count = 0
        self._oom = 0
        self._oom_kill = 0
        self._artifact: Path | None = None
        self._validation_errors: list[str] = []

    @classmethod
    def prepare(
        cls,
        authority: MemoryEnvelopeAuthority,
        runner: Runner,
        metrics: SnapshotSource,
        scenario: MemoryEnvelopeScenario,
    ) -> "MemoryEnvelopeSession":
        return cls(authority, runner, metrics, scenario)

    @classmethod
    def start(
        cls,
        authority: MemoryEnvelopeAuthority,
        runner: Runner,
        metrics: SnapshotSource,
        scenario: MemoryEnvelopeScenario,
    ) -> "MemoryEnvelopeSession":
        session = cls.prepare(authority, runner, metrics, scenario)
        session.begin()
        return session

    def begin(self) -> MemorySample:
        if self._samples:
            raise RuntimeError("memory-envelope baseline was already captured")
        baseline = self._capture("before")
        self._pid = baseline.pid
        self._restart_count = baseline.restart_count
        self._oom = baseline.oom
        self._oom_kill = baseline.oom_kill
        self._record(baseline)
        return baseline

    @property
    def samples(self) -> tuple[MemorySample, ...]:
        return tuple(self._samples)

    def attach_artifact(self, path: Path) -> None:
        if path.name != "memory-envelope.json":
            raise ValueError(f"memory-envelope artifact must use its canonical name: {path}")
        self._artifact = path
        self._persist()

    def _persist(self, result: MemoryEnvelopeResult | None = None) -> None:
        if self._artifact is None:
            return
        payload: dict[str, object] = {
            "schema": 1,
            "scenario": self.scenario.to_dict(),
            "authority": self.authority.to_dict(),
            "samples": [asdict(sample) for sample in self._samples],
            "status": "failed" if self._validation_errors else "running",
            "validation_errors": self._validation_errors,
        }
        if result is not None:
            payload["status"] = "complete" if result.complete else "incomplete"
            payload["result"] = result.to_dict()
        self._artifact.write_text(
            json.dumps(payload, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )

    def _record(self, sample: MemorySample) -> None:
        self._samples.append(sample)
        self._persist()
        try:
            self._validate(sample)
        except BaseException as error:
            self._validation_errors.append(str(error))
            self._persist()
            raise

    def _capture(self, phase: str) -> MemorySample:
        if _phase_kind(phase) not in self.scenario.phases:
            raise ValueError(f"unknown memory-envelope phase: {phase!r}")
        identity = _service_identity(self.runner, self.authority.service)
        service_cgroup = (
            self.authority.cgroup_root / identity.control_group.lstrip("/")
        ).resolve(strict=False)
        if service_cgroup != self.authority.cgroup_path:
            raise RuntimeError(
                "systemd ControlGroup and configured cgroup mismatch: "
                f"service={service_cgroup}, configured={self.authority.cgroup_path}"
            )
        cgroup = self.authority.cgroup_path
        oom, oom_kill = _events(cgroup / "memory.events")
        rss, hwm, process_swap = _status(
            self.authority.proc_root / str(identity.pid) / "status"
        )
        return MemorySample(
            phase=phase,
            pid=identity.pid,
            restart_count=identity.restart_count,
            control_group=identity.control_group,
            cgroup_current_bytes=_read_integer(cgroup / "memory.current"),
            cgroup_peak_bytes=_read_integer(cgroup / "memory.peak"),
            cgroup_swap_bytes=_read_integer(cgroup / "memory.swap.current"),
            oom=oom,
            oom_kill=oom_kill,
            pid_rss_bytes=rss,
            pid_hwm_bytes=hwm,
            pid_swap_bytes=process_swap,
            writeback=self.metrics.snapshot(),
        )

    def _validate(self, sample: MemorySample) -> None:
        if sample.writeback.terminal:
            raise RuntimeError(
                f"terminal writeback state at memory phase {sample.phase}"
            )
        if self._pid and sample.pid != self._pid:
            raise RuntimeError(
                f"ZeroFS PID changed: before={self._pid}, now={sample.pid}"
            )
        if self._samples and sample.restart_count != self._restart_count:
            raise RuntimeError(
                "ZeroFS restart count changed: "
                f"before={self._restart_count}, now={sample.restart_count}"
            )
        if self._samples and (
            sample.oom != self._oom or sample.oom_kill != self._oom_kill
        ):
            raise RuntimeError(
                "cgroup OOM counters changed: "
                f"oom={self._oom}->{sample.oom}, "
                f"oom_kill={self._oom_kill}->{sample.oom_kill}"
            )
        checks = (
            (
                "cgroup current ceiling",
                sample.cgroup_current_bytes,
                self.limits.cgroup_current_bytes,
            ),
            (
                "cgroup peak ceiling",
                sample.cgroup_peak_bytes,
                self.limits.cgroup_peak_bytes,
            ),
            ("PID RSS ceiling", sample.pid_rss_bytes, self.limits.pid_rss_bytes),
            ("PID HWM ceiling", sample.pid_hwm_bytes, self.limits.pid_rss_bytes),
            ("cgroup swap ceiling", sample.cgroup_swap_bytes, self.limits.swap_bytes),
            ("PID swap ceiling", sample.pid_swap_bytes, self.limits.swap_bytes),
        )
        for label, actual, limit in checks:
            if actual > limit:
                raise RuntimeError(
                    f"{label} exceeded at {sample.phase}: actual={actual}, limit={limit}"
                )

    def sample(self, phase: str) -> MemorySample:
        previous = _phase_kind(self._samples[-1].phase)
        current = _phase_kind(phase)
        before = self.scenario.phases[0]
        after = self.scenario.phases[-1]
        cycle = self.scenario.phases[1:-1]
        if previous == before:
            allowed = {cycle[0]}
        elif previous == cycle[-1]:
            allowed = {cycle[0], after}
        elif previous in cycle:
            allowed = {cycle[cycle.index(previous) + 1]}
        else:
            allowed = set()
        if current not in allowed:
            raise RuntimeError(
                "memory-envelope phase order mismatch: "
                f"after={previous}, allowed={sorted(allowed)}, got={phase}"
            )
        sample = self._capture(phase)
        self._record(sample)
        return sample

    def _result(self) -> MemoryEnvelopeResult:
        phases = tuple(_phase_kind(sample.phase) for sample in self._samples)
        missing = tuple(phase for phase in self.scenario.phases if phase not in phases)
        complete = not missing and phases[-1] == self.scenario.phases[-1]
        return MemoryEnvelopeResult(
            scenario=self.scenario.name,
            authority=self.authority,
            limits=self.limits,
            samples=tuple(self._samples),
            cleanup_resources=(),
            cleanup_attempts=0,
            cleanup_asserted=True,
            cleanup_semantics="observer-owned-no-resources",
            complete=complete,
            missing_phases=missing,
        )

    def finish(self) -> MemoryEnvelopeResult:
        result = self._result()
        self._persist(result)
        if not result.complete:
            raise RuntimeError(
                f"memory envelope missing phases: {list(result.missing_phases)}"
            )
        return result

    def finish_after_cleanup(self, *, require_complete: bool) -> MemoryEnvelopeResult:
        if not self._samples or _phase_kind(
            self._samples[-1].phase
        ) != self.scenario.phases[-1]:
            sample = self._capture(self.scenario.phases[-1])
            self._record(sample)
        result = self._result()
        self._persist(result)
        if require_complete and not result.complete:
            raise RuntimeError(
                f"memory envelope missing phases: {list(result.missing_phases)}"
            )
        return result
