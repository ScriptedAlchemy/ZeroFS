from __future__ import annotations

import hashlib
import json
import os
import stat
import tempfile
from dataclasses import asdict, dataclass
from pathlib import Path

from .config import PilotConfig
from .runner import Runner


MIB = 1_048_576

# Canonical MiB/s precision for every reported benchmark rate. Entry points must
# not each pick their own, or the same transfer reads as two different numbers
# depending on which command produced the receipt.
RATE_DIGITS = 3


def counter_delta(after: int, before: int, label: str) -> int:
    """Compute a monotonic counter delta, raising if it regressed.

    A regression means the counter's source restarted or was re-enumerated
    mid-measurement. Clamping it (``max(0, after - before)``) would silently
    turn that lost interval into a plausible-looking small or zero delta, so
    every counter-backed benchmark quantity fails closed instead.
    """
    if after < before:
        raise RuntimeError(f"{label} counter regressed: before={before}, after={after}")
    return after - before


def mib_per_second(
    byte_count: int, elapsed_seconds: float, *, digits: int = RATE_DIGITS
) -> float:
    """Shared MiB/s kernel: callers own their own zero-guard and time units.

    Precision is deliberately *not* a caller choice -- see ``RATE_DIGITS``.
    """
    return round(byte_count / MIB / elapsed_seconds, digits)


def file_sha256(path: Path) -> str:
    """Hash a local file in 1 MiB chunks, without reading it all into memory."""
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


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
    # A regressed diskstats counter (device re-enumerated, driver reloaded)
    # must not be clamped to zero: `proven` is derived from a zero read delta,
    # so clamping would turn lost evidence into a fabricated page-cache proof.
    read_bytes = counter_delta(
        after.read_bytes, before.read_bytes, f"{before.device} read bytes"
    )
    evidence = PageCacheEvidence(
        device=before.device,
        read_bytes=read_bytes,
        write_bytes=counter_delta(
            after.write_bytes, before.write_bytes, f"{before.device} write bytes"
        ),
        busy_ms=counter_delta(
            after.busy_ms, before.busy_ms, f"{before.device} busy ms"
        ),
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


def install_config_text(
    runner: Runner, config: PilotConfig, text: str, *, prefix: str
) -> None:
    with tempfile.NamedTemporaryFile(
        mode="w",
        prefix=prefix,
        dir=config.temp_dir,
        delete=False,
    ) as handle:
        handle.write(text)
        temporary = Path(handle.name)
    try:
        runner.run(
            [
                "install",
                "-o",
                "root",
                "-g",
                "root",
                "-m",
                "0600",
                temporary,
                config.config_file,
            ],
            sudo=True,
        )
    finally:
        temporary.unlink(missing_ok=True)


@dataclass(frozen=True, slots=True)
class FioJobAggregate:
    byte_count: int
    runtime_ms: int
    requests: int
    errors: int


def load_fio_jobs(path: Path) -> list[dict]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    jobs = payload.get("jobs")
    if not isinstance(jobs, list) or not jobs:
        raise ValueError(f"fio output has no jobs: {path}")
    return jobs


def aggregate_fio_jobs(
    jobs: list[dict],
    *,
    operation: str,
    path: Path,
    require_request_counters: bool,
) -> FioJobAggregate:
    """Sum io_bytes/total_ios/error and max runtime across fio jobs.

    ``runtime`` is aggregated with ``max`` rather than ``sum`` because fio runs
    every job of an invocation concurrently unless ``stonewall`` is set, and no
    caller here sets it. ``total_bytes / max(runtime)`` is therefore the rate of
    the concurrent phase. (With ``--group_reporting`` fio already collapses the
    array to one grouped entry, so this is usually a one-element reduction.)

    The per-job ``error`` counter is always required and aggregated: a job that
    reported I/O errors cannot back a valid measurement regardless of which
    entry point is reading the file. ``require_request_counters`` gates only the
    optional ``total_ios`` request counter, which just the request-rate callers
    need.

    Callers keep their own validation of the aggregate (short-I/O checks, error
    rejection, measurable-runtime checks); this only does the shared per-job
    aggregation.
    """
    byte_count = 0
    runtime_ms = 0
    requests = 0
    errors = 0
    for job in jobs:
        stats = job.get(operation)
        if not isinstance(stats, dict):
            raise ValueError(f"fio output has no {operation} stats: {path}")
        if "error" not in job:
            raise ValueError(f"fio job has no error counter: {path}")
        errors += int(job["error"])
        if require_request_counters:
            if "total_ios" not in stats:
                raise ValueError(
                    f"fio {operation} stats have no total_ios counter: {path}"
                )
            requests += int(stats["total_ios"])
        byte_count += int(stats.get("io_bytes", 0))
        runtime_ms = max(runtime_ms, int(stats.get("runtime", 0)))
    return FioJobAggregate(
        byte_count=byte_count, runtime_ms=runtime_ms, requests=requests, errors=errors
    )


def prepare_run_root(
    runner: Runner, config: PilotConfig, run_root: Path, *, prefix: str, role: str
) -> None:
    config.require_mount_child(run_root, prefix, role)
    runner.run(
        [
            "install",
            "-d",
            "-m",
            "0755",
            "-o",
            config.user,
            "-g",
            config.group,
            run_root,
        ],
        sudo=True,
    )
