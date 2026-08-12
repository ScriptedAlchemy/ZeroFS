from __future__ import annotations

import os
import stat
from dataclasses import asdict, dataclass
from pathlib import Path


def filesystem_device(path: Path) -> tuple[int, int]:
    device = os.stat(path).st_dev
    return (os.major(device), os.minor(device))


def root_device() -> tuple[int, int]:
    return filesystem_device(Path("/"))


def block_device(path: Path) -> tuple[int, int]:
    metadata = path.stat()
    if not stat.S_ISBLK(metadata.st_mode):
        raise ValueError(f"not a block device: {path}")
    return (os.major(metadata.st_rdev), os.minor(metadata.st_rdev))


def _pressure(path: Path) -> tuple[float, float, int, int]:
    values: dict[str, dict[str, str]] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        fields = line.split()
        values[fields[0]] = dict(field.split("=", 1) for field in fields[1:])
    return (
        float(values["some"]["avg10"]),
        float(values["full"]["avg10"]),
        int(values["some"]["total"]),
        int(values["full"]["total"]),
    )


def _diskstats(path: Path, device: tuple[int, int]) -> tuple[str, int, int, int]:
    for line in path.read_text(encoding="utf-8").splitlines():
        fields = line.split()
        if (int(fields[0]), int(fields[1])) != device:
            continue
        return (
            fields[2],
            int(fields[5]) * 512,
            int(fields[9]) * 512,
            int(fields[12]),
        )
    raise ValueError(f"block device {device[0]}:{device[1]} missing from diskstats")


@dataclass(frozen=True, slots=True)
class BlockIoSnapshot:
    device: str
    read_bytes: int
    write_bytes: int
    busy_ms: int

    @classmethod
    def capture(cls, proc_root: Path, *, device: tuple[int, int]) -> "BlockIoSnapshot":
        name, read_bytes, write_bytes, busy_ms = _diskstats(
            proc_root / "diskstats", device
        )
        return cls(name, read_bytes, write_bytes, busy_ms)


@dataclass(frozen=True, slots=True)
class PageCacheEvidence:
    device: str
    read_bytes: int
    write_bytes: int
    busy_ms: int
    proven: bool

    def to_dict(self) -> dict[str, str | int | bool]:
        return asdict(self)


def verify_page_cache_hit(
    before: BlockIoSnapshot, after: BlockIoSnapshot
) -> PageCacheEvidence:
    if before.device != after.device:
        raise ValueError("block device changed during page-cache measurement")
    read_bytes = max(0, after.read_bytes - before.read_bytes)
    evidence = PageCacheEvidence(
        device=before.device,
        read_bytes=read_bytes,
        write_bytes=max(0, after.write_bytes - before.write_bytes),
        busy_ms=max(0, after.busy_ms - before.busy_ms),
        proven=read_bytes == 0,
    )
    if not evidence.proven:
        raise RuntimeError(
            f"measured page-cache pass reached {before.device}: read_bytes={read_bytes}"
        )
    return evidence


@dataclass(frozen=True, slots=True)
class SystemIoSnapshot:
    root_device: str
    root_read_bytes: int
    root_write_bytes: int
    root_busy_ms: int
    some_avg10: float
    full_avg10: float
    some_total_us: int
    full_total_us: int

    @classmethod
    def capture(
        cls, proc_root: Path, *, root_device: tuple[int, int]
    ) -> "SystemIoSnapshot":
        some_avg10, full_avg10, some_total_us, full_total_us = _pressure(
            proc_root / "pressure" / "io"
        )
        name, read_bytes, write_bytes, busy_ms = _diskstats(
            proc_root / "diskstats", root_device
        )
        return cls(
            root_device=name,
            root_read_bytes=read_bytes,
            root_write_bytes=write_bytes,
            root_busy_ms=busy_ms,
            some_avg10=some_avg10,
            full_avg10=full_avg10,
            some_total_us=some_total_us,
            full_total_us=full_total_us,
        )

    def to_dict(self) -> dict[str, str | int | float]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class SystemIoSummary:
    root_device: str
    some_stall_ms: float
    full_stall_ms: float
    root_read_mib: float
    root_write_mib: float
    root_busy_ms: int
    root_utilization_percent: float
    peak_some_avg10: float
    peak_full_avg10: float

    def to_dict(self) -> dict[str, str | int | float]:
        return asdict(self)


def summarize_system_io(
    snapshots: list[SystemIoSnapshot], *, elapsed_ms: int
) -> SystemIoSummary:
    if len(snapshots) < 2:
        raise ValueError("system I/O summary requires at least two snapshots")
    before, after = snapshots[0], snapshots[-1]
    if before.root_device != after.root_device:
        raise ValueError("root block device changed during benchmark")
    elapsed_ms = max(1, elapsed_ms)
    return SystemIoSummary(
        root_device=before.root_device,
        some_stall_ms=round(
            max(0, after.some_total_us - before.some_total_us) / 1000, 3
        ),
        full_stall_ms=round(
            max(0, after.full_total_us - before.full_total_us) / 1000, 3
        ),
        root_read_mib=round(
            max(0, after.root_read_bytes - before.root_read_bytes) / 1_048_576, 3
        ),
        root_write_mib=round(
            max(0, after.root_write_bytes - before.root_write_bytes) / 1_048_576, 3
        ),
        root_busy_ms=max(0, after.root_busy_ms - before.root_busy_ms),
        root_utilization_percent=round(
            100 * max(0, after.root_busy_ms - before.root_busy_ms) / elapsed_ms, 2
        ),
        peak_some_avg10=max(snapshot.some_avg10 for snapshot in snapshots),
        peak_full_avg10=max(snapshot.full_avg10 for snapshot in snapshots),
    )
