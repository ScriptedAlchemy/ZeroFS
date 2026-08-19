from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Protocol

from .metrics import (
    DrainReceipt,
    MetricsAuthorityIdentity,
    WritebackSnapshot,
    wait_for_drain,
)


class SnapshotSource(Protocol):
    def snapshot(self) -> WritebackSnapshot: ...

    def identity(self) -> MetricsAuthorityIdentity: ...


@dataclass(slots=True)
class WritebackObserver:
    """Protocol-neutral ZeroFS metrics and stable-drain authority."""

    metrics: SnapshotSource
    drain_timeout: int
    metrics_endpoint: str

    def status(self) -> dict[str, object]:
        snapshot = self.metrics.snapshot()
        if snapshot.terminal:
            raise RuntimeError("ZeroFS reports a terminal writeback error")
        return {
            "metrics_endpoint": self.metrics_endpoint,
            "metrics": snapshot.to_dict(),
            "terminal": False,
        }

    def identity(self) -> MetricsAuthorityIdentity:
        return self.metrics.identity()

    def snapshot(self) -> WritebackSnapshot:
        return self.metrics.snapshot()

    def drain(self, timeout: float | None = None) -> dict[str, object]:
        receipt: DrainReceipt = wait_for_drain(
            self.snapshot,
            timeout=self.drain_timeout if timeout is None else timeout,
        )
        return asdict(receipt)
