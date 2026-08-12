from __future__ import annotations

import time
import urllib.request
from dataclasses import dataclass
from typing import Callable


class TerminalWritebackError(RuntimeError):
    pass


_METRICS = {
    "zerofs_writeback_accepted_sequence": "accepted",
    "zerofs_writeback_local_sequence": "local",
    "zerofs_writeback_remote_sequence": "remote",
    "zerofs_writeback_dirty_ram_bytes": "dirty_ram",
    "zerofs_writeback_dirty_ssd_bytes": "dirty_ssd",
    "zerofs_writeback_local_bytes_completed_total": "local_bytes",
    "zerofs_writeback_remote_bytes_completed_total": "remote_bytes",
    "zerofs_writeback_terminal_error": "terminal",
}


@dataclass(frozen=True, slots=True)
class WritebackSnapshot:
    accepted: int
    local: int
    remote: int
    dirty_ram: int
    dirty_ssd: int
    local_bytes: int
    remote_bytes: int
    terminal: bool

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
        missing = sorted(set(_METRICS.values()) - found.keys())
        if missing:
            raise ValueError(f"writeback metrics missing fields: {', '.join(missing)}")
        return cls(
            accepted=found["accepted"],
            local=found["local"],
            remote=found["remote"],
            dirty_ram=found["dirty_ram"],
            dirty_ssd=found["dirty_ssd"],
            local_bytes=found["local_bytes"],
            remote_bytes=found["remote_bytes"],
            terminal=bool(found["terminal"]),
        )

    @property
    def drained(self) -> bool:
        return (
            not self.terminal
            and self.accepted == self.local == self.remote
            and self.dirty_ram == 0
            and self.dirty_ssd == 0
        )

    def to_dict(self) -> dict[str, int | bool]:
        return {
            "accepted": self.accepted,
            "local": self.local,
            "remote": self.remote,
            "dirty_ram": self.dirty_ram,
            "dirty_ssd": self.dirty_ssd,
            "local_bytes": self.local_bytes,
            "remote_bytes": self.remote_bytes,
            "terminal": self.terminal,
        }


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

    def snapshot(self) -> WritebackSnapshot:
        with urllib.request.urlopen(self.url, timeout=self.timeout) as response:
            text = response.read().decode("utf-8")
        return WritebackSnapshot.parse(text)


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
