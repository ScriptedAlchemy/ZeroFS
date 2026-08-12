from __future__ import annotations

import csv
import json
import threading
import time
import uuid
from dataclasses import asdict, dataclass
from pathlib import Path

from .config import PilotConfig
from .lifecycle import PilotLifecycle
from .receipts import RunReceipt
from .runner import Runner


def _rate(byte_count: int, elapsed_ms: int) -> float:
    if elapsed_ms <= 0:
        return 0.0
    return round(byte_count / 1_048_576 / (elapsed_ms / 1000), 2)


@dataclass(frozen=True, slots=True)
class BenchmarkResult:
    logical_bytes: int
    local_bytes: int
    remote_bytes: int
    foreground_ms: int
    local_end_to_end_ms: int
    remote_end_to_end_ms: int
    buffered_read_ms: int
    direct_read_ms: int
    foreground_mibps: float
    local_mibps: float
    remote_mibps: float
    buffered_read_mibps: float
    direct_read_mibps: float
    receipt_dir: str = ""

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


def calculate_tiers(
    *,
    logical_bytes: int,
    local_bytes: int,
    remote_bytes: int,
    foreground_ms: int,
    local_end_to_end_ms: int,
    remote_end_to_end_ms: int,
    buffered_read_ms: int,
    direct_read_ms: int,
) -> BenchmarkResult:
    return BenchmarkResult(
        logical_bytes=logical_bytes,
        local_bytes=local_bytes,
        remote_bytes=remote_bytes,
        foreground_ms=foreground_ms,
        local_end_to_end_ms=local_end_to_end_ms,
        remote_end_to_end_ms=remote_end_to_end_ms,
        buffered_read_ms=buffered_read_ms,
        direct_read_ms=direct_read_ms,
        foreground_mibps=_rate(logical_bytes, foreground_ms),
        local_mibps=_rate(local_bytes, local_end_to_end_ms),
        remote_mibps=_rate(remote_bytes, remote_end_to_end_ms),
        buffered_read_mibps=_rate(logical_bytes, buffered_read_ms),
        direct_read_mibps=_rate(logical_bytes, direct_read_ms),
    )


class _MetricSampler:
    def __init__(self, lifecycle: PilotLifecycle, output: Path) -> None:
        self.lifecycle = lifecycle
        self.output = output
        self.stop_event = threading.Event()
        self.thread = threading.Thread(target=self._run, name="writeback-metrics", daemon=True)
        self.error: BaseException | None = None

    def start(self) -> None:
        self.thread.start()

    def stop(self) -> None:
        self.stop_event.set()
        self.thread.join(timeout=10)
        if self.thread.is_alive():
            raise TimeoutError("writeback metric sampler did not stop")
        if self.error is not None:
            raise RuntimeError(f"writeback metric sampler failed: {self.error}")

    def _run(self) -> None:
        try:
            with self.output.open("w", newline="", encoding="utf-8") as handle:
                writer = csv.writer(handle)
                writer.writerow(
                    (
                        "timestamp_ms",
                        "accepted",
                        "local",
                        "remote",
                        "dirty_ram",
                        "dirty_ssd",
                        "local_bytes",
                        "remote_bytes",
                        "terminal",
                    )
                )
                while not self.stop_event.is_set():
                    snapshot = self.lifecycle.metrics.snapshot()
                    writer.writerow((round(time.time() * 1000), *snapshot.to_dict().values()))
                    handle.flush()
                    self.stop_event.wait(0.25)
        except BaseException as error:
            self.error = error


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

    def prepare_root(self, run_root: Path) -> None:
        self.config.require_disposable(run_root)
        if run_root.parent.resolve(strict=False) != self.config.mountpoint.resolve(strict=False):
            raise ValueError(f"benchmark root must be a direct mount child: {run_root}")
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
        per_job_mib: int,
        jobs: int,
        output: Path,
        read: bool,
        direct: bool | None = None,
    ) -> None:
        argv: list[str | Path] = [
            "fio",
            f"--name={name}",
            f"--directory={run_root}",
            "--filename_format=file.$jobnum",
            f"--rw={'read' if read else 'write'}",
            "--bs=1M",
            f"--size={per_job_mib}M",
            f"--numjobs={jobs}",
            "--group_reporting",
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

    def _cleanup_root(self, run_root: Path) -> None:
        self.config.require_disposable(run_root)
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

    def run(self, *, total_mib: int = 1024, jobs: int = 4) -> BenchmarkResult:
        if total_mib <= 0 or jobs <= 0 or total_mib % jobs:
            raise ValueError("total MiB must be positive and divisible by jobs")
        self.lifecycle.status()
        self.lifecycle.drain()
        run_root = self.config.mountpoint / f".zerofs-bench-{uuid.uuid4().hex}"
        per_job_mib = total_mib // jobs
        logical_bytes = total_mib * 1_048_576
        receipt = RunReceipt.start(self.config, "benchmark")
        sampler: _MetricSampler | None = None
        result: BenchmarkResult | None = None
        with receipt:
            receipt.record("total_mib", total_mib)
            receipt.record("jobs", jobs)
            receipt.record("run_root", str(run_root))
            self.prepare_root(run_root)
            write_output = receipt.path("write-fio.txt")
            buffered_output = receipt.path("buffered-read-fio.txt")
            direct_output = receipt.path("direct-read-fio.txt")
            sample_output = receipt.path("metrics.csv")
            try:
                before = self.lifecycle.metrics.snapshot()
                sampler = _MetricSampler(self.lifecycle, sample_output)
                sampler.start()
                started = time.monotonic_ns()
                self._run_fio(
                    name="zerofs_user_write",
                    run_root=run_root,
                    per_job_mib=per_job_mib,
                    jobs=jobs,
                    output=write_output,
                    read=False,
                )
                foreground_end = time.monotonic_ns()
                self.runner.run(["sync", "-f", self.config.mountpoint], sudo=True)
                local_end = time.monotonic_ns()
                local_snapshot = self.lifecycle.metrics.snapshot()
                self.lifecycle.drain()
                remote_end = time.monotonic_ns()
                remote_snapshot = self.lifecycle.metrics.snapshot()
                sampler.stop()
                sampler = None

                buffered_start = time.monotonic_ns()
                self._run_fio(
                    name="zerofs_buffered_warm_read",
                    run_root=run_root,
                    per_job_mib=per_job_mib,
                    jobs=jobs,
                    output=buffered_output,
                    read=True,
                    direct=False,
                )
                buffered_end = time.monotonic_ns()
                direct_start = time.monotonic_ns()
                self._run_fio(
                    name="zerofs_direct_read",
                    run_root=run_root,
                    per_job_mib=per_job_mib,
                    jobs=jobs,
                    output=direct_output,
                    read=True,
                    direct=True,
                )
                direct_end = time.monotonic_ns()
                millis = lambda end, begin: max(1, round((end - begin) / 1_000_000))
                result = calculate_tiers(
                    logical_bytes=logical_bytes,
                    local_bytes=max(0, local_snapshot.local_bytes - before.local_bytes),
                    remote_bytes=max(0, remote_snapshot.remote_bytes - before.remote_bytes),
                    foreground_ms=millis(foreground_end, started),
                    local_end_to_end_ms=millis(local_end, started),
                    remote_end_to_end_ms=millis(remote_end, started),
                    buffered_read_ms=millis(buffered_end, buffered_start),
                    direct_read_ms=millis(direct_end, direct_start),
                )
                result = BenchmarkResult(
                    **{**result.to_dict(), "receipt_dir": str(receipt.directory)}
                )
                receipt.record("result", result.to_dict())
                receipt.path("summary.json").write_text(
                    json.dumps(result.to_dict(), indent=2, sort_keys=True) + "\n",
                    encoding="utf-8",
                )
            finally:
                if sampler is not None:
                    sampler.stop()
                self._cleanup_root(run_root)
        if result is None:
            raise RuntimeError("benchmark completed without a result")
        return result
