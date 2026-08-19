from __future__ import annotations

import time
import urllib.request
import re
from dataclasses import asdict, dataclass
from typing import Callable


class TerminalWritebackError(RuntimeError):
    pass


_AUTHORITY_METRIC = "zerofs_benchmark_authority_info"
_AUTHORITY_LABEL = re.compile(r'([a-z_]+)="([A-Za-z0-9._:/-]+)"\Z')


@dataclass(frozen=True, slots=True)
class MetricsAuthorityIdentity:
    server_instance_id: str
    filesystem_id: str
    export_id: str

    def __post_init__(self) -> None:
        for name, value in asdict(self).items():
            if not value or not re.fullmatch(r"[A-Za-z0-9._:/-]+", value):
                raise ValueError(f"invalid metrics authority {name}: {value!r}")

    @classmethod
    def parse(cls, text: str) -> "MetricsAuthorityIdentity":
        matches: list[MetricsAuthorityIdentity] = []
        for raw_line in text.splitlines():
            line = raw_line.strip()
            if not line.startswith(f"{_AUTHORITY_METRIC}{{"):
                continue
            series, separator, value = line.rpartition(" ")
            if not separator or value != "1":
                raise ValueError(
                    f"invalid {_AUTHORITY_METRIC} sample: {line}"
                )
            prefix = f"{_AUTHORITY_METRIC}{{"
            if not series.endswith("}"):
                raise ValueError(f"invalid {_AUTHORITY_METRIC} series: {series}")
            labels: dict[str, str] = {}
            for token in series[len(prefix) : -1].split(","):
                match = _AUTHORITY_LABEL.fullmatch(token)
                if match is None or match.group(1) in labels:
                    raise ValueError(
                        f"invalid {_AUTHORITY_METRIC} label: {token!r}"
                    )
                labels[match.group(1)] = match.group(2)
            expected = {"server_instance_id", "filesystem_id", "export_id"}
            if set(labels) != expected:
                raise ValueError(
                    f"invalid {_AUTHORITY_METRIC} labels: {sorted(labels)}"
                )
            matches.append(
                cls(
                    labels["server_instance_id"],
                    labels["filesystem_id"],
                    labels["export_id"],
                )
            )
        if len(matches) != 1:
            raise ValueError(
                f"expected exactly one {_AUTHORITY_METRIC} sample, found {len(matches)}"
            )
        return matches[0]


_METRICS = {
    "zerofs_writeback_accepted_sequence": "accepted",
    "zerofs_writeback_local_sequence": "local",
    "zerofs_writeback_remote_sequence": "remote",
    "zerofs_writeback_dirty_ram_bytes": "dirty_ram",
    "zerofs_writeback_dirty_ssd_reserved_bytes": "dirty_ssd_reserved",
    "zerofs_writeback_local_bytes_completed_total": "local_bytes",
    "zerofs_writeback_remote_bytes_completed_total": "remote_bytes",
    "zerofs_writeback_terminal_error": "terminal",
    "zerofs_segment_gc_active": "gc_active",
    "zerofs_segment_gc_passes_total": "gc_passes",
    "zerofs_segment_gc_batches_total": "gc_batches",
    "zerofs_segment_gc_deleted_bytes_total": "gc_deleted_bytes",
}


@dataclass(frozen=True, slots=True)
class WritebackSnapshot:
    accepted: int
    local: int
    remote: int
    dirty_ram: int
    dirty_ssd_reserved: int
    local_bytes: int
    remote_bytes: int
    terminal: bool
    gc_active: bool | None = None
    gc_passes: int = 0
    gc_batches: int = 0
    gc_deleted_bytes: int = 0

    @classmethod
    def parse(cls, text: str) -> "WritebackSnapshot":
        found: dict[str, int] = {}
        for raw_line in text.splitlines():
            line = raw_line.strip()
            if not line or line.startswith("#"):
                continue
            parts = line.split()
            if len(parts) < 2 or parts[0] not in _METRICS:
                continue
            try:
                found[_METRICS[parts[0]]] = int(float(parts[1]))
            except ValueError as error:
                raise ValueError(f"invalid writeback metric: {line}") from error
        required = set(_METRICS.values()) - {"gc_active"}
        missing = sorted(required - found.keys())
        if missing:
            raise ValueError(f"writeback metrics missing fields: {', '.join(missing)}")
        return cls(
            accepted=found["accepted"],
            local=found["local"],
            remote=found["remote"],
            dirty_ram=found["dirty_ram"],
            dirty_ssd_reserved=found["dirty_ssd_reserved"],
            local_bytes=found["local_bytes"],
            remote_bytes=found["remote_bytes"],
            terminal=bool(found["terminal"]),
            gc_active=(bool(found["gc_active"]) if "gc_active" in found else None),
            gc_passes=found["gc_passes"],
            gc_batches=found["gc_batches"],
            gc_deleted_bytes=found["gc_deleted_bytes"],
        )

    @property
    def drained(self) -> bool:
        return (
            not self.terminal
            and self.accepted == self.local == self.remote
            and self.dirty_ram == 0
            and self.dirty_ssd_reserved == 0
        )

    def to_dict(self) -> dict[str, int | bool | None]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class DrainReceipt:
    snapshot: WritebackSnapshot
    elapsed_ms: int
    first_drained_ms: int
    stable_samples: int


class MetricsClient:
    def __init__(self, url: str, timeout: float = 5.0) -> None:
        self.url = url
        self.timeout = timeout

    def _fetch(self) -> str:
        with urllib.request.urlopen(self.url, timeout=self.timeout) as response:
            return response.read().decode("utf-8")

    def snapshot(self) -> WritebackSnapshot:
        return WritebackSnapshot.parse(self._fetch())

    def identity(self) -> MetricsAuthorityIdentity:
        return MetricsAuthorityIdentity.parse(self._fetch())


def wait_for_gc_quiescence(
    snapshot: Callable[[], WritebackSnapshot],
    *,
    timeout: float,
    after_pass: int | None = None,
    stable_samples: int = 4,
    interval: float = 0.25,
    monotonic: Callable[[], float] = time.monotonic,
    sleep: Callable[[float], None] = time.sleep,
) -> WritebackSnapshot:
    """Wait for a reclaim pass to finish and remain idle.

    When ``after_pass`` is supplied, an already-idle old epoch is not enough:
    the caller gets a full clean cadence window immediately after a fresh pass.
    """
    started = monotonic()
    stable = 0
    last: WritebackSnapshot | None = None
    while True:
        last = snapshot()
        if last.terminal:
            raise TerminalWritebackError("writeback reported a terminal error")
        if last.gc_active is None:
            raise ValueError(
                "ZeroFS does not expose zerofs_segment_gc_active; deploy the "
                "profile-capable build before benchmarking"
            )
        minimum_pass = 1 if after_pass is None else after_pass + 1
        if not last.gc_active and last.gc_passes >= minimum_pass:
            stable += 1
            if stable >= stable_samples:
                return last
        else:
            stable = 0
        if monotonic() - started >= timeout:
            raise TimeoutError(
                f"segment GC did not become quiescent within {timeout}s; "
                f"last={last.to_dict()}"
            )
        sleep(interval)


def wait_for_drain(
    snapshot: Callable[[], WritebackSnapshot],
    *,
    timeout: float,
    stable_samples: int = 4,
    interval: float = 1.0,
    monotonic: Callable[[], float] = time.monotonic,
    sleep: Callable[[float], None] = time.sleep,
) -> DrainReceipt:
    started = monotonic()
    first_drained: float | None = None
    stable = 0
    last: WritebackSnapshot | None = None
    while True:
        last = snapshot()
        if last.terminal:
            raise TerminalWritebackError("writeback reported a terminal error")
        now = monotonic()
        if last.drained:
            first_drained = now if first_drained is None else first_drained
            stable += 1
            if stable >= stable_samples:
                return DrainReceipt(
                    snapshot=last,
                    elapsed_ms=round((now - started) * 1000),
                    first_drained_ms=round((first_drained - started) * 1000),
                    stable_samples=stable,
                )
        else:
            first_drained = None
            stable = 0
        if now - started >= timeout:
            raise TimeoutError(
                f"writeback did not drain within {timeout}s; last={last.to_dict()}"
            )
        sleep(interval)


def wait_for_local(
    snapshot: Callable[[], WritebackSnapshot],
    *,
    target_sequence: int,
    timeout: float,
    interval: float = 0.05,
    monotonic: Callable[[], float] = time.monotonic,
    sleep: Callable[[float], None] = time.sleep,
) -> WritebackSnapshot:
    started = monotonic()
    while True:
        current = snapshot()
        if current.terminal:
            raise TerminalWritebackError("writeback reported a terminal error")
        if current.local >= target_sequence:
            return current
        if monotonic() - started >= timeout:
            raise TimeoutError(
                f"writeback did not reach local sequence {target_sequence} within "
                f"{timeout}s; last={current.to_dict()}"
            )
        sleep(interval)


def wait_for_remote(
    snapshot: Callable[[], WritebackSnapshot],
    *,
    target_sequence: int,
    timeout: float,
    interval: float = 0.05,
    monotonic: Callable[[], float] = time.monotonic,
    sleep: Callable[[float], None] = time.sleep,
) -> WritebackSnapshot:
    started = monotonic()
    while True:
        current = snapshot()
        if current.terminal:
            raise TerminalWritebackError("writeback reported a terminal error")
        if current.remote >= target_sequence:
            return current
        if monotonic() - started >= timeout:
            raise TimeoutError(
                f"writeback did not reach remote sequence {target_sequence} within "
                f"{timeout}s; last={current.to_dict()}"
            )
        sleep(interval)


def wait_for_accepted_after(
    snapshot: Callable[[], WritebackSnapshot],
    *,
    previous_sequence: int,
    timeout: float,
    stable_samples: int = 4,
    interval: float = 0.05,
    monotonic: Callable[[], float] = time.monotonic,
    sleep: Callable[[float], None] = time.sleep,
) -> WritebackSnapshot:
    """Wait until the exported status observes work submitted after a baseline."""
    started = monotonic()
    candidate: int | None = None
    stable = 0
    while True:
        current = snapshot()
        if current.terminal:
            raise TerminalWritebackError("writeback reported a terminal error")
        if current.accepted > previous_sequence:
            if current.accepted == candidate:
                stable += 1
            else:
                candidate = current.accepted
                stable = 1
            if stable >= stable_samples:
                return current
        else:
            candidate = None
            stable = 0
        if monotonic() - started >= timeout:
            raise TimeoutError(
                "writeback did not observe a new accepted sequence after "
                f"{previous_sequence} within {timeout}s; last={current.to_dict()}"
            )
        sleep(interval)
