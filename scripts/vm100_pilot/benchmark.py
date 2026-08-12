from __future__ import annotations

import csv
import json
import shutil
import sys
import tempfile
import threading
import time
import uuid
from collections.abc import Callable
from dataclasses import asdict, dataclass, replace
from pathlib import Path

from .config import PilotConfig
from .lifecycle import PilotLifecycle
from .metrics import (
    WritebackSnapshot,
    wait_for_accepted_after,
    wait_for_gc_quiescence,
    wait_for_local,
)
from .receipts import RunReceipt
from .runner import Runner
from .system_io import (
    BlockIoSnapshot,
    SystemIoSnapshot,
    block_device,
    filesystem_device,
    summarize_system_io,
    verify_page_cache_hit,
)


def _rate(byte_count: int, elapsed_ms: int) -> float:
    if elapsed_ms <= 0:
        return 0.0
    return round(byte_count / 1_048_576 / (elapsed_ms / 1000), 2)


def _counter_delta(after: int, before: int, label: str) -> int:
    if after < before:
        raise RuntimeError(f"{label} counter regressed: before={before}, after={after}")
    return after - before


def _monotonic_ms() -> int:
    return time.monotonic_ns() // 1_000_000


class BenchmarkContaminatedError(RuntimeError):
    pass


class BenchmarkCleanupError(RuntimeError):
    def __init__(
        self,
        primary: BaseException | None,
        cleanup_errors: list[BaseException],
    ) -> None:
        self.primary = primary
        self.cleanup_errors = tuple(cleanup_errors)
        details = "; ".join(str(error) for error in cleanup_errors)
        super().__init__(f"benchmark cleanup failed: {details}")


@dataclass(frozen=True, slots=True)
class FioResult:
    bytes: int
    runtime_ms: int
    mibps: float

    @classmethod
    def from_json(cls, path: Path, *, operation: str) -> "FioResult":
        payload = json.loads(path.read_text(encoding="utf-8"))
        jobs = payload.get("jobs")
        if not isinstance(jobs, list) or not jobs:
            raise ValueError(f"fio output has no jobs: {path}")
        byte_count = 0
        runtime_ms = 0
        for job in jobs:
            stats = job.get(operation)
            if not isinstance(stats, dict):
                raise ValueError(f"fio output has no {operation} stats: {path}")
            byte_count += int(stats.get("io_bytes", 0))
            runtime_ms = max(runtime_ms, int(stats.get("runtime", 0)))
        if byte_count <= 0 or runtime_ms <= 0:
            raise ValueError(
                f"fio {operation} did no measurable I/O: "
                f"bytes={byte_count}, runtime_ms={runtime_ms}"
            )
        return cls(
            bytes=byte_count,
            runtime_ms=runtime_ms,
            mibps=_rate(byte_count, runtime_ms),
        )


@dataclass(frozen=True, slots=True)
class DirectReadPair:
    warmup: FioResult
    hot: FioResult
    warmup_start_ns: int
    warmup_end_ns: int
    hot_start_ns: int
    hot_end_ns: int


@dataclass(frozen=True, slots=True)
class DirectWriteTiers:
    write: FioResult
    before: WritebackSnapshot
    accepted: WritebackSnapshot
    local: WritebackSnapshot
    remote: WritebackSnapshot
    write_start_ns: int
    write_end_ns: int
    local_sync_start_ns: int
    local_end_ns: int
    remote_end_ns: int
    io_before: SystemIoSnapshot
    write_io_after: SystemIoSnapshot
    local_io_after: SystemIoSnapshot
    remote_io_after: SystemIoSnapshot

    def phases(
        self,
    ) -> dict[str, tuple[int, int, SystemIoSnapshot, SystemIoSnapshot]]:
        phases = {
            "zerofs_nbd_odirect_write_service_ack": (
                self.write_start_ns,
                self.write_end_ns,
                self.io_before,
                self.write_io_after,
            ),
            "zerofs_nbd_odirect_local_durability_tail": (
                self.local_sync_start_ns,
                self.local_end_ns,
                self.write_io_after,
                self.local_io_after,
            ),
            "zerofs_nbd_odirect_local_durability_end_to_end": (
                self.write_start_ns,
                self.local_end_ns,
                self.io_before,
                self.local_io_after,
            ),
            "zerofs_nbd_odirect_remote_durability_end_to_end": (
                self.write_start_ns,
                self.remote_end_ns,
                self.io_before,
                self.remote_io_after,
            ),
        }
        if self.remote_end_ns > self.local_end_ns:
            phases["zerofs_nbd_odirect_remote_durability_tail"] = (
                self.local_end_ns,
                self.remote_end_ns,
                self.local_io_after,
                self.remote_io_after,
            )
        return phases

    def remote_tail_ms(self) -> int:
        return max(0, round((self.remote_end_ns - self.local_end_ns) / 1_000_000))

    def phase_windows(self) -> dict[str, dict[str, int]]:
        return {
            name: {"start_ns": start_ns, "end_ns": end_ns}
            for name, (start_ns, end_ns, _, _) in self.phases().items()
        }

    def barrier_receipt(self) -> dict[str, int]:
        return {
            "before_sequence": self.before.accepted,
            "accepted_sequence": self.accepted.accepted,
            "local_sequence": self.local.local,
            "remote_sequence": self.remote.remote,
            "fio_bytes": self.write.bytes,
            "fio_runtime_ms": self.write.runtime_ms,
            "local_completed_bytes": _counter_delta(
                self.local.local_bytes,
                self.before.local_bytes,
                "ZeroFS NBD O_DIRECT local encoded bytes",
            ),
            "remote_completed_bytes": _counter_delta(
                self.remote.remote_bytes,
                self.before.remote_bytes,
                "ZeroFS NBD O_DIRECT remote encoded bytes",
            ),
        }


def _validate_fio_bytes(result: FioResult, *, expected_bytes: int, phase: str) -> None:
    if result.bytes != expected_bytes:
        raise RuntimeError(
            f"fio {phase} returned short I/O: "
            f"expected={expected_bytes}, actual={result.bytes}"
        )


def _assert_no_maintenance(before: WritebackSnapshot, after: WritebackSnapshot) -> None:
    before_epoch = (before.gc_passes, before.gc_batches, before.gc_deleted_bytes)
    after_epoch = (after.gc_passes, after.gc_batches, after.gc_deleted_bytes)
    if after.gc_active or after_epoch != before_epoch:
        raise BenchmarkContaminatedError(
            "segment GC overlapped the measured benchmark epoch: "
            f"before={before_epoch}, after={after_epoch}, active={after.gc_active}"
        )


@dataclass(frozen=True, slots=True)
class BenchmarkResult:
    logical_bytes: int
    local_bytes: int
    remote_bytes: int
    user_buffered_page_cache_write_ms: int
    local_end_to_end_ms: int
    remote_end_to_end_ms: int
    local_active_ms: int
    remote_active_ms: int
    page_cache_hot_read_ms: int
    zerofs_direct_read_ms: int
    user_buffered_page_cache_write_mibps: float
    local_mibps: float
    remote_mibps: float
    local_active_mibps: float
    remote_active_mibps: float
    page_cache_hot_read_mibps: float
    zerofs_direct_read_mibps: float
    receipt_dir: str = ""
    zerofs_nbd_odirect_write_bytes: int = 0
    zerofs_nbd_odirect_write_service_ack_ms: int = 0
    zerofs_nbd_odirect_write_service_ack_mibps: float = 0.0
    zerofs_nbd_odirect_local_durability_tail_ms: int = 0
    zerofs_nbd_odirect_local_durability_end_to_end_ms: int = 0
    zerofs_nbd_odirect_remote_durability_tail_ms: int = 0
    zerofs_nbd_odirect_remote_durability_end_to_end_ms: int = 0
    zerofs_nbd_odirect_local_encoded_bytes: int = 0
    zerofs_nbd_odirect_remote_encoded_bytes: int = 0
    zerofs_nbd_odirect_local_durability_tail_mibps: float = 0.0
    zerofs_nbd_odirect_local_durability_end_to_end_mibps: float = 0.0
    zerofs_nbd_odirect_remote_durability_tail_mibps: float = 0.0
    zerofs_nbd_odirect_remote_durability_end_to_end_mibps: float = 0.0

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


def calculate_tiers(
    *,
    logical_bytes: int,
    local_bytes: int,
    remote_bytes: int,
    user_buffered_page_cache_write_ms: int,
    local_end_to_end_ms: int,
    remote_end_to_end_ms: int,
    local_active_ms: int,
    remote_active_ms: int,
    page_cache_hot_read_ms: int,
    zerofs_direct_read_ms: int,
    zerofs_nbd_odirect_write_bytes: int = 0,
    zerofs_nbd_odirect_write_service_ack_ms: int = 0,
    zerofs_nbd_odirect_local_durability_tail_ms: int = 0,
    zerofs_nbd_odirect_local_durability_end_to_end_ms: int = 0,
    zerofs_nbd_odirect_remote_durability_tail_ms: int = 0,
    zerofs_nbd_odirect_remote_durability_end_to_end_ms: int = 0,
    zerofs_nbd_odirect_local_encoded_bytes: int = 0,
    zerofs_nbd_odirect_remote_encoded_bytes: int = 0,
) -> BenchmarkResult:
    return BenchmarkResult(
        logical_bytes=logical_bytes,
        local_bytes=local_bytes,
        remote_bytes=remote_bytes,
        user_buffered_page_cache_write_ms=user_buffered_page_cache_write_ms,
        local_end_to_end_ms=local_end_to_end_ms,
        remote_end_to_end_ms=remote_end_to_end_ms,
        local_active_ms=local_active_ms,
        remote_active_ms=remote_active_ms,
        page_cache_hot_read_ms=page_cache_hot_read_ms,
        zerofs_direct_read_ms=zerofs_direct_read_ms,
        user_buffered_page_cache_write_mibps=_rate(
            logical_bytes, user_buffered_page_cache_write_ms
        ),
        local_mibps=_rate(local_bytes, local_end_to_end_ms),
        remote_mibps=_rate(remote_bytes, remote_end_to_end_ms),
        local_active_mibps=_rate(local_bytes, local_active_ms),
        remote_active_mibps=_rate(remote_bytes, remote_active_ms),
        page_cache_hot_read_mibps=_rate(logical_bytes, page_cache_hot_read_ms),
        zerofs_direct_read_mibps=_rate(logical_bytes, zerofs_direct_read_ms),
        zerofs_nbd_odirect_write_bytes=zerofs_nbd_odirect_write_bytes,
        zerofs_nbd_odirect_write_service_ack_ms=(
            zerofs_nbd_odirect_write_service_ack_ms
        ),
        zerofs_nbd_odirect_write_service_ack_mibps=_rate(
            zerofs_nbd_odirect_write_bytes,
            zerofs_nbd_odirect_write_service_ack_ms,
        ),
        zerofs_nbd_odirect_local_durability_tail_ms=(
            zerofs_nbd_odirect_local_durability_tail_ms
        ),
        zerofs_nbd_odirect_local_durability_end_to_end_ms=(
            zerofs_nbd_odirect_local_durability_end_to_end_ms
        ),
        zerofs_nbd_odirect_remote_durability_tail_ms=(
            zerofs_nbd_odirect_remote_durability_tail_ms
        ),
        zerofs_nbd_odirect_remote_durability_end_to_end_ms=(
            zerofs_nbd_odirect_remote_durability_end_to_end_ms
        ),
        zerofs_nbd_odirect_local_encoded_bytes=(
            zerofs_nbd_odirect_local_encoded_bytes
        ),
        zerofs_nbd_odirect_remote_encoded_bytes=(
            zerofs_nbd_odirect_remote_encoded_bytes
        ),
        zerofs_nbd_odirect_local_durability_tail_mibps=_rate(
            zerofs_nbd_odirect_local_encoded_bytes,
            zerofs_nbd_odirect_local_durability_tail_ms,
        ),
        zerofs_nbd_odirect_local_durability_end_to_end_mibps=_rate(
            zerofs_nbd_odirect_local_encoded_bytes,
            zerofs_nbd_odirect_local_durability_end_to_end_ms,
        ),
        zerofs_nbd_odirect_remote_durability_tail_mibps=_rate(
            zerofs_nbd_odirect_remote_encoded_bytes,
            zerofs_nbd_odirect_remote_durability_tail_ms,
        ),
        zerofs_nbd_odirect_remote_durability_end_to_end_mibps=_rate(
            zerofs_nbd_odirect_remote_encoded_bytes,
            zerofs_nbd_odirect_remote_durability_end_to_end_ms,
        ),
    )


def _active_windows(
    path: Path,
    *,
    before_accepted: int,
    before_local_bytes: int,
    target_local_bytes: int,
    before_remote_bytes: int,
    target_remote_bytes: int,
) -> tuple[int, int]:
    with path.open(newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    if not rows:
        return (0, 0)

    def value(row: dict[str, str], key: str) -> int:
        return int(row[key])

    def transition_start(predicate: Callable[[dict[str, str]], bool]) -> int:
        previous = value(rows[0], "timestamp_ms")
        for row in rows:
            if predicate(row):
                return previous
            previous = value(row, "timestamp_ms")
        return value(rows[0], "timestamp_ms")

    local_start = transition_start(
        lambda row: (
            value(row, "accepted") > before_accepted or value(row, "dirty_ram") > 0
        )
    )
    local_end = next(
        (
            value(row, "timestamp_ms")
            for row in rows
            if value(row, "local_bytes") >= target_local_bytes
        ),
        value(rows[-1], "timestamp_ms"),
    )
    remote_start = transition_start(
        lambda row: value(row, "remote_bytes") > before_remote_bytes
    )
    remote_end = next(
        (
            value(row, "timestamp_ms")
            for row in rows
            if value(row, "remote_bytes") >= target_remote_bytes
        ),
        value(rows[-1], "timestamp_ms"),
    )
    if target_local_bytes <= before_local_bytes:
        local_end = local_start
    return (max(1, local_end - local_start), max(1, remote_end - remote_start))


class _MetricSampler:
    _METRIC_HEADER = (
        "timestamp_ms",
        "accepted",
        "local",
        "remote",
        "dirty_ram",
        "dirty_ssd_reserved",
        "local_bytes",
        "remote_bytes",
        "terminal",
        "gc_active",
        "gc_passes",
        "gc_batches",
        "gc_deleted_bytes",
    )
    _SYSTEM_IO_HEADER = (
        "timestamp_ms",
        "root_device",
        "root_read_bytes",
        "root_write_bytes",
        "root_busy_ms",
        "some_avg10",
        "full_avg10",
        "some_total_us",
        "full_total_us",
    )

    def __init__(
        self,
        lifecycle: PilotLifecycle,
        output: Path,
        system_io_output: Path,
        local_device: tuple[int, int],
    ) -> None:
        self.lifecycle = lifecycle
        self.output = output
        self.system_io_output = system_io_output
        self.stop_event = threading.Event()
        self.thread = threading.Thread(
            target=self._run, name="writeback-metrics", daemon=True
        )
        self.error: BaseException | None = None
        self.root_device = local_device
        self.system_io: list[SystemIoSnapshot] = []
        self.metric_rows: list[tuple[object, ...]] = []
        self.system_io_rows: list[tuple[object, ...]] = []
        self.metric_samples: list[tuple[int, WritebackSnapshot]] = []
        self.sampled = threading.Condition()

    def start(self) -> None:
        self.thread.start()

    def stop(self) -> None:
        self.stop_event.set()
        self.thread.join(timeout=10)
        if self.thread.is_alive():
            raise TimeoutError("writeback metric sampler did not stop")
        if self.error is not None:
            raise RuntimeError(f"writeback metric sampler failed: {self.error}")
        self._persist()

    def _run(self) -> None:
        try:
            while not self.stop_event.is_set():
                snapshot = self.lifecycle.metrics.snapshot()
                io_snapshot = SystemIoSnapshot.capture(
                    self.lifecycle.config.proc_root,
                    root_device=self.root_device,
                )
                self.system_io.append(io_snapshot)
                timestamp_ns = time.monotonic_ns()
                timestamp_ms = timestamp_ns // 1_000_000
                with self.sampled:
                    self.metric_samples.append((timestamp_ns, snapshot))
                    self.metric_rows.append(
                        (timestamp_ms, *snapshot.to_dict().values())
                    )
                    self.system_io_rows.append(
                        (timestamp_ms, *io_snapshot.to_dict().values())
                    )
                    self.sampled.notify_all()
                self.stop_event.wait(0.05)
        except BaseException as error:
            self.error = error
            with self.sampled:
                self.sampled.notify_all()

    def wait_for_remote(
        self, target_sequence: int, timeout: float
    ) -> tuple[WritebackSnapshot, int]:
        deadline = time.monotonic() + timeout
        cursor = 0
        with self.sampled:
            while True:
                for timestamp_ns, snapshot in self.metric_samples[cursor:]:
                    if snapshot.terminal:
                        raise RuntimeError("writeback reported a terminal error")
                    if snapshot.remote >= target_sequence:
                        return snapshot, timestamp_ns
                cursor = len(self.metric_samples)
                if self.error is not None:
                    raise RuntimeError(f"writeback metric sampler failed: {self.error}")
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise TimeoutError(
                        "buffered writeback samples did not reach remote sequence "
                        f"{target_sequence} within {timeout}s"
                    )
                self.sampled.wait(remaining)

    def _persist(self) -> None:
        with (
            self.output.open("w", newline="", encoding="utf-8") as handle,
            self.system_io_output.open("w", newline="", encoding="utf-8") as io_handle,
        ):
            writer = csv.writer(handle)
            writer.writerow(self._METRIC_HEADER)
            writer.writerows(self.metric_rows)
            io_writer = csv.writer(io_handle)
            io_writer.writerow(self._SYSTEM_IO_HEADER)
            io_writer.writerows(self.system_io_rows)


class BenchmarkRunner:
    def __init__(
        self,
        config: PilotConfig,
        runner: Runner,
        lifecycle: PilotLifecycle,
    ) -> None:
        self.config = config
        self.runner = runner
        self.lifecycle = lifecycle

    def _local_device(self) -> tuple[int, int]:
        return filesystem_device(self.config.pilot_state_root)

    def _benchmark_tmpfs_root(self) -> Path:
        return Path("/dev/shm")

    def _system_io(self, device: tuple[int, int]) -> SystemIoSnapshot:
        return SystemIoSnapshot.capture(self.config.proc_root, root_device=device)

    def _nbd_io(self) -> BlockIoSnapshot:
        return BlockIoSnapshot.capture(
            self.config.proc_root, device=block_device(self.config.nbd_device)
        )

    def _wait_clean_gc(self, *, maintenance_isolated: bool) -> WritebackSnapshot:
        if maintenance_isolated:
            return wait_for_gc_quiescence(
                self.lifecycle.metrics.snapshot,
                timeout=self.config.drain_timeout,
            )
        baseline = self.lifecycle.metrics.snapshot().gc_passes
        return wait_for_gc_quiescence(
            self.lifecycle.metrics.snapshot,
            timeout=self.config.drain_timeout,
            after_pass=baseline,
        )

    def prepare_root(self, run_root: Path) -> None:
        self.config.require_mount_child(run_root, ".zerofs-bench-", "benchmark root")
        self.runner.run(
            [
                "install",
                "-d",
                "-m",
                "0755",
                "-o",
                self.config.user,
                "-g",
                self.config.group,
                run_root,
            ],
            sudo=True,
        )

    def _run_fio(
        self,
        *,
        name: str,
        run_root: Path,
        filename_format: str = "file.$jobnum",
        per_job_mib: int,
        jobs: int,
        output: Path,
        read: bool,
        direct: bool | None = None,
    ) -> FioResult:
        argv: list[str | Path] = [
            "fio",
            f"--name={name}",
            f"--directory={run_root}",
            f"--filename_format={filename_format}",
            f"--rw={'read' if read else 'write'}",
            "--bs=1M",
            f"--size={per_job_mib}M",
            f"--numjobs={jobs}",
            "--group_reporting",
            "--output-format=json",
            f"--output={output}",
        ]
        if not read:
            argv.extend(
                (
                    "--fallocate=none",
                    "--refill_buffers=1",
                    "--scramble_buffers=1",
                    "--buffer_compress_percentage=0",
                )
            )
        if direct is not None:
            argv.append(f"--direct={int(direct)}")
        if read and direct is False:
            argv.append("--invalidate=0")
        self.runner.run(argv, sudo=True, capture=False)
        return FioResult.from_json(output, operation="read" if read else "write")

    def _cleanup_root(self, run_root: Path) -> None:
        self.config.require_mount_child(run_root, ".zerofs-bench-", "benchmark root")
        self.runner.run(["rm", "-rf", "--", run_root], sudo=True)
        self.runner.run(["sync", "-f", self.config.mountpoint], sudo=True, check=False)
        try:
            self.lifecycle.drain()
        except BaseException:
            # The primary benchmark error remains authoritative. A live caller
            # records this cleanup failure via the enclosing receipt.
            pass
        remains = self.runner.run(["test", "-e", run_root], sudo=True, check=False)
        if remains.returncode == 0:
            raise RuntimeError(f"benchmark root remains after cleanup: {run_root}")

    def _run_direct_read_pair(
        self,
        *,
        run_root: Path,
        per_job_mib: int,
        jobs: int,
        warmup_output: Path,
        hot_output: Path,
        after_warmup: Callable[[], None] | None = None,
    ) -> DirectReadPair:
        warmup_start_ns = time.monotonic_ns()
        warmup = self._run_fio(
            name="zerofs_direct_read_warmup",
            run_root=run_root,
            per_job_mib=per_job_mib,
            jobs=jobs,
            output=warmup_output,
            read=True,
            direct=True,
        )
        warmup_end_ns = time.monotonic_ns()
        if after_warmup is not None:
            after_warmup()
        hot_start_ns = warmup_end_ns
        hot = self._run_fio(
            name="zerofs_direct_read_hot",
            run_root=run_root,
            per_job_mib=per_job_mib,
            jobs=jobs,
            output=hot_output,
            read=True,
            direct=True,
        )
        hot_end_ns = time.monotonic_ns()
        return DirectReadPair(
            warmup=warmup,
            hot=hot,
            warmup_start_ns=warmup_start_ns,
            warmup_end_ns=warmup_end_ns,
            hot_start_ns=hot_start_ns,
            hot_end_ns=hot_end_ns,
        )

    def _run_direct_write_tiers(
        self,
        *,
        run_root: Path,
        per_job_mib: int,
        jobs: int,
        expected_bytes: int,
        output: Path,
        phase_device: tuple[int, int],
        sampler: _MetricSampler,
    ) -> DirectWriteTiers:
        self.lifecycle.drain()
        self.runner.run(["sync", "-f", self.config.mountpoint], sudo=True)
        before = self.lifecycle.metrics.snapshot()
        io_before = self._system_io(phase_device)
        write_start_ns = time.monotonic_ns()
        write = self._run_fio(
            name="zerofs_nbd_odirect_write",
            run_root=run_root,
            filename_format="odirect-write.$jobnum",
            per_job_mib=per_job_mib,
            jobs=jobs,
            output=output,
            read=False,
            direct=True,
        )
        _validate_fio_bytes(
            write,
            expected_bytes=expected_bytes,
            phase="ZeroFS NBD O_DIRECT write",
        )
        write_end_ns = time.monotonic_ns()
        write_io_after = self._system_io(phase_device)
        accepted = self.lifecycle.metrics.snapshot()
        if accepted.terminal:
            raise RuntimeError("writeback reported a terminal error")
        if accepted.accepted <= before.accepted:
            raise RuntimeError(
                "ZeroFS NBD O_DIRECT write returned before advancing the "
                "accepted sequence"
            )
        local_sync_start_ns = time.monotonic_ns()
        self.runner.run(["sync", "-f", self.config.mountpoint], sudo=True)
        local_end_ns = time.monotonic_ns()
        local = self.lifecycle.metrics.snapshot()
        if local.terminal:
            raise RuntimeError("writeback reported a terminal error")
        if local.local < accepted.accepted:
            raise RuntimeError(
                "syncfs returned before ZeroFS reached local durability: "
                f"target={accepted.accepted}, local={local.local}"
            )
        local_io_after = self._system_io(phase_device)
        remote, remote_end_ns = sampler.wait_for_remote(
            accepted.accepted, self.config.drain_timeout
        )
        remote_io_after = self._system_io(phase_device)
        self.lifecycle.drain()
        return DirectWriteTiers(
            write=write,
            before=before,
            accepted=accepted,
            local=local,
            remote=remote,
            write_start_ns=write_start_ns,
            write_end_ns=write_end_ns,
            local_sync_start_ns=local_sync_start_ns,
            local_end_ns=local_end_ns,
            remote_end_ns=remote_end_ns,
            io_before=io_before,
            write_io_after=write_io_after,
            local_io_after=local_io_after,
            remote_io_after=remote_io_after,
        )

    def run(
        self,
        *,
        total_mib: int = 1024,
        jobs: int = 4,
        maintenance_isolated: bool = False,
    ) -> BenchmarkResult:
        if total_mib <= 0 or jobs <= 0 or total_mib % jobs:
            raise ValueError("total MiB must be positive and divisible by jobs")
        self.lifecycle.status()
        self.lifecycle.drain()
        quiescent = self._wait_clean_gc(maintenance_isolated=maintenance_isolated)
        run_root = self.config.mountpoint / f".zerofs-bench-{uuid.uuid4().hex}"
        primary_error: BaseException | None = None
        try:
            return self._run_in_root(
                run_root=run_root,
                total_mib=total_mib,
                jobs=jobs,
                quiescent=quiescent,
                maintenance_isolated=maintenance_isolated,
            )
        except BaseException as error:
            primary_error = error
            raise
        finally:
            try:
                self._cleanup_root(run_root)
            except BaseException as cleanup_error:
                if primary_error is not None:
                    raise BenchmarkCleanupError(
                        primary_error, [cleanup_error]
                    ) from primary_error
                raise

    def _run_in_root(
        self,
        *,
        run_root: Path,
        total_mib: int,
        jobs: int,
        quiescent: WritebackSnapshot,
        maintenance_isolated: bool,
    ) -> BenchmarkResult:
        per_job_mib = total_mib // jobs
        logical_bytes = total_mib * 1_048_576
        receipt = RunReceipt.start(self.config, "benchmark")
        sampler: _MetricSampler | None = None
        result: BenchmarkResult | None = None
        with receipt:
            receipt.record("total_mib", total_mib)
            receipt.record("jobs", jobs)
            receipt.record("run_root", str(run_root))
            receipt.record("maintenance_isolated", maintenance_isolated)
            receipt.record("maintenance_before", quiescent.to_dict())
            self.prepare_root(run_root)
            scratch: Path | None = None
            try:
                tmpfs_root = self._benchmark_tmpfs_root()
                if not tmpfs_root.is_dir():
                    raise RuntimeError(
                        f"benchmark tmpfs root is unavailable: {tmpfs_root}"
                    )
                scratch = Path(
                    tempfile.mkdtemp(prefix="zerofs-benchmark-", dir=tmpfs_root)
                )
                fio_artifacts = {
                    name: receipt.path(name)
                    for name in (
                        "write-fio.json",
                        "buffered-read-warmup-fio.json",
                        "buffered-read-hot-fio.json",
                        "direct-read-warmup-fio.json",
                        "direct-read-hot-fio.json",
                        "direct-write-fio.json",
                    )
                }
                write_output = scratch / "write-fio.json"
                buffered_warmup_output = scratch / "buffered-read-warmup-fio.json"
                buffered_output = scratch / "buffered-read-hot-fio.json"
                direct_warmup_output = scratch / "direct-read-warmup-fio.json"
                direct_output = scratch / "direct-read-hot-fio.json"
                direct_write_output = scratch / "direct-write-fio.json"
                sample_output = receipt.path("metrics.csv")
                system_io_output = receipt.path("system-io.csv")
                before = self.lifecycle.metrics.snapshot()
                phase_device = self._local_device()
                sampler = _MetricSampler(
                    self.lifecycle, sample_output, system_io_output, phase_device
                )
                sampler.start()
                write_io_before = self._system_io(phase_device)
                phase_windows: dict[str, dict[str, int]] = {}

                def record_phase(name: str, start_ns: int, end_ns: int) -> None:
                    phase_windows[name] = {"start_ns": start_ns, "end_ns": end_ns}

                started = time.monotonic_ns()
                write_result = self._run_fio(
                    name="zerofs_user_write",
                    run_root=run_root,
                    per_job_mib=per_job_mib,
                    jobs=jobs,
                    output=write_output,
                    read=False,
                )
                _validate_fio_bytes(
                    write_result,
                    expected_bytes=logical_bytes,
                    phase="foreground write",
                )
                foreground_end = time.monotonic_ns()
                record_phase(
                    "user_buffered_page_cache_write", started, foreground_end
                )
                write_io_after = self._system_io(phase_device)
                self.runner.run(["sync", "-f", self.config.mountpoint], sudo=True)
                accepted_after_write = wait_for_accepted_after(
                    self.lifecycle.metrics.snapshot,
                    previous_sequence=before.accepted,
                    timeout=self.config.drain_timeout,
                ).accepted
                local_snapshot = wait_for_local(
                    self.lifecycle.metrics.snapshot,
                    target_sequence=accepted_after_write,
                    timeout=self.config.drain_timeout,
                )
                local_end = time.monotonic_ns()
                record_phase("local_durability_tail", foreground_end, local_end)
                record_phase("local_end_to_end", started, local_end)
                local_io_after = self._system_io(phase_device)
                self.lifecycle.drain()
                remote_end = time.monotonic_ns()
                record_phase("remote_durability_tail", local_end, remote_end)
                record_phase("remote_end_to_end", started, remote_end)
                remote_io_after = self._system_io(phase_device)
                remote_snapshot = self.lifecycle.metrics.snapshot()
                warmup_start = time.monotonic_ns()
                buffered_warmup = self._run_fio(
                    name="zerofs_buffered_read_warmup",
                    run_root=run_root,
                    per_job_mib=per_job_mib,
                    jobs=jobs,
                    output=buffered_warmup_output,
                    read=True,
                    direct=False,
                )
                _validate_fio_bytes(
                    buffered_warmup,
                    expected_bytes=logical_bytes,
                    phase="buffered warmup",
                )
                warmup_end = time.monotonic_ns()
                record_phase("buffered_warmup", warmup_start, warmup_end)
                warmup_io_after = self._system_io(phase_device)
                nbd_before = self._nbd_io()
                hot_start = time.monotonic_ns()
                buffered_read = self._run_fio(
                    name="zerofs_buffered_page_cache_hot_read",
                    run_root=run_root,
                    per_job_mib=per_job_mib,
                    jobs=jobs,
                    output=buffered_output,
                    read=True,
                    direct=False,
                )
                _validate_fio_bytes(
                    buffered_read,
                    expected_bytes=logical_bytes,
                    phase="page-cache hot read",
                )
                hot_end = time.monotonic_ns()
                record_phase("page_cache_hot_read", hot_start, hot_end)
                hot_io_after = self._system_io(phase_device)
                nbd_after = self._nbd_io()
                page_cache = verify_page_cache_hit(nbd_before, nbd_after)
                direct_io_boundaries: list[SystemIoSnapshot] = []
                direct_pair = self._run_direct_read_pair(
                    run_root=run_root,
                    per_job_mib=per_job_mib,
                    jobs=jobs,
                    warmup_output=direct_warmup_output,
                    hot_output=direct_output,
                    after_warmup=lambda: direct_io_boundaries.append(
                        self._system_io(phase_device)
                    ),
                )
                if len(direct_io_boundaries) != 1:
                    raise RuntimeError("direct warmup I/O boundary was not captured")
                direct_warmup_io_after = direct_io_boundaries[0]
                _validate_fio_bytes(
                    direct_pair.warmup,
                    expected_bytes=logical_bytes,
                    phase="direct warmup",
                )
                _validate_fio_bytes(
                    direct_pair.hot,
                    expected_bytes=logical_bytes,
                    phase="direct hot read",
                )
                record_phase(
                    "direct_warmup",
                    direct_pair.warmup_start_ns,
                    direct_pair.warmup_end_ns,
                )
                record_phase(
                    "direct_read", direct_pair.hot_start_ns, direct_pair.hot_end_ns
                )
                direct_io_after = self._system_io(phase_device)
                direct_write = self._run_direct_write_tiers(
                    run_root=run_root,
                    per_job_mib=per_job_mib,
                    jobs=jobs,
                    expected_bytes=logical_bytes,
                    output=direct_write_output,
                    phase_device=phase_device,
                    sampler=sampler,
                )
                phase_windows.update(direct_write.phase_windows())
                sampler.stop()
                sampler_system_io = sampler.system_io
                sampler = None
                local_active_ms, remote_active_ms = _active_windows(
                    sample_output,
                    before_accepted=before.accepted,
                    before_local_bytes=before.local_bytes,
                    target_local_bytes=local_snapshot.local_bytes,
                    before_remote_bytes=before.remote_bytes,
                    target_remote_bytes=remote_snapshot.remote_bytes,
                )
                system_io = summarize_system_io(
                    sampler_system_io,
                    elapsed_ms=max(
                        1, round((direct_write.remote_end_ns - started) / 1_000_000)
                    ),
                )

                def phase_io(
                    before_io: SystemIoSnapshot,
                    after_io: SystemIoSnapshot,
                    begin_ns: int,
                    end_ns: int,
                ) -> dict[str, str | int | float]:
                    return summarize_system_io(
                        [before_io, after_io],
                        elapsed_ms=max(1, round((end_ns - begin_ns) / 1_000_000)),
                    ).to_dict()

                direct_phase_io = {
                    name: phase_io(before_io, after_io, start_ns, end_ns)
                    for name, (
                        start_ns,
                        end_ns,
                        before_io,
                        after_io,
                    ) in direct_write.phases().items()
                }

                phase_system_io = {
                        "user_buffered_page_cache_write": phase_io(
                            write_io_before, write_io_after, started, foreground_end
                        ),
                        "local_durability_tail": phase_io(
                            write_io_after, local_io_after, foreground_end, local_end
                        ),
                        "remote_durability_tail": phase_io(
                            local_io_after, remote_io_after, local_end, remote_end
                        ),
                        "buffered_warmup": phase_io(
                            remote_io_after, warmup_io_after, warmup_start, warmup_end
                        ),
                        "page_cache_hot_read": phase_io(
                            warmup_io_after, hot_io_after, hot_start, hot_end
                        ),
                        "direct_warmup": phase_io(
                            hot_io_after,
                            direct_warmup_io_after,
                            direct_pair.warmup_start_ns,
                            direct_pair.warmup_end_ns,
                        ),
                        "direct_read": phase_io(
                            direct_warmup_io_after,
                            direct_io_after,
                            direct_pair.hot_start_ns,
                            direct_pair.hot_end_ns,
                        ),
                        **direct_phase_io,
                    }
                maintenance_after = self.lifecycle.metrics.snapshot()
                _assert_no_maintenance(quiescent, maintenance_after)
                for name, destination in fio_artifacts.items():
                    shutil.copyfile(scratch / name, destination)
                receipt.record("buffered_read_warmup", asdict(buffered_warmup))
                receipt.record("page_cache_evidence", page_cache.to_dict())
                receipt.record("direct_read_warmup", asdict(direct_pair.warmup))
                receipt.record(
                    "zerofs_nbd_odirect_write_barriers",
                    direct_write.barrier_receipt(),
                )
                receipt.record("phase_monotonic_ns", phase_windows)
                receipt.record("system_io", system_io.to_dict())
                receipt.record("phase_system_io", phase_system_io)
                receipt.record("maintenance_after", maintenance_after.to_dict())

                def millis(end: int, begin: int) -> int:
                    return max(1, round((end - begin) / 1_000_000))

                result = calculate_tiers(
                    logical_bytes=logical_bytes,
                    local_bytes=_counter_delta(
                        local_snapshot.local_bytes,
                        before.local_bytes,
                        "buffered-write local encoded bytes",
                    ),
                    remote_bytes=_counter_delta(
                        remote_snapshot.remote_bytes,
                        before.remote_bytes,
                        "buffered-write remote encoded bytes",
                    ),
                    user_buffered_page_cache_write_ms=millis(
                        foreground_end, started
                    ),
                    local_end_to_end_ms=millis(local_end, started),
                    remote_end_to_end_ms=millis(remote_end, started),
                    local_active_ms=local_active_ms,
                    remote_active_ms=remote_active_ms,
                    page_cache_hot_read_ms=buffered_read.runtime_ms,
                    zerofs_direct_read_ms=direct_pair.hot.runtime_ms,
                    zerofs_nbd_odirect_write_bytes=direct_write.write.bytes,
                    zerofs_nbd_odirect_write_service_ack_ms=(
                        direct_write.write.runtime_ms
                    ),
                    zerofs_nbd_odirect_local_durability_tail_ms=millis(
                        direct_write.local_end_ns,
                        direct_write.local_sync_start_ns,
                    ),
                    zerofs_nbd_odirect_local_durability_end_to_end_ms=millis(
                        direct_write.local_end_ns, direct_write.write_start_ns
                    ),
                    zerofs_nbd_odirect_remote_durability_end_to_end_ms=millis(
                        direct_write.remote_end_ns, direct_write.write_start_ns
                    ),
                    zerofs_nbd_odirect_remote_durability_tail_ms=(
                        direct_write.remote_tail_ms()
                    ),
                    zerofs_nbd_odirect_local_encoded_bytes=_counter_delta(
                        direct_write.local.local_bytes,
                        direct_write.before.local_bytes,
                        "ZeroFS NBD O_DIRECT local encoded bytes",
                    ),
                    zerofs_nbd_odirect_remote_encoded_bytes=_counter_delta(
                        direct_write.remote.remote_bytes,
                        direct_write.before.remote_bytes,
                        "ZeroFS NBD O_DIRECT remote encoded bytes",
                    ),
                )
                result = replace(result, receipt_dir=str(receipt.directory))
                receipt.record("result", result.to_dict())
                receipt.path("summary.json").write_text(
                    json.dumps(result.to_dict(), indent=2, sort_keys=True) + "\n",
                    encoding="utf-8",
                )
            finally:
                primary_error = sys.exception()
                cleanup_errors: list[BaseException] = []
                scratch_error: BaseException | None = None
                if sampler is not None:
                    try:
                        sampler.stop()
                    except BaseException as error:
                        cleanup_errors.append(error)
                if scratch is not None:
                    try:
                        shutil.rmtree(scratch)
                    except BaseException as error:
                        scratch_error = error
                        cleanup_errors.append(error)
                if primary_error is not None and scratch_error is not None:
                    raise BenchmarkCleanupError(
                        primary_error, cleanup_errors
                    ) from primary_error
                if primary_error is None and cleanup_errors:
                    raise BenchmarkCleanupError(None, cleanup_errors) from None
        if result is None:
            raise RuntimeError("benchmark completed without a result")
        return result
