from __future__ import annotations

import csv
import json
import shutil
import tempfile
import time
import uuid
from dataclasses import asdict, dataclass
from pathlib import Path

from .benchmark import (
    BenchmarkContaminatedError,
    _MetricSampler,
    _assert_no_maintenance,
)
from .config import PilotConfig
from .lifecycle import PilotLifecycle
from .metrics import (
    WritebackSnapshot,
    wait_for_accepted_after,
    wait_for_gc_quiescence,
)
from .receipts import RunReceipt
from .runner import Runner
from .system_io import (
    BlockIoSnapshot,
    SystemIoSnapshot,
    SystemIoSummary,
    block_device,
    filesystem_device,
    prepare_run_root,
    summarize_system_io,
)


@dataclass(frozen=True, slots=True)
class MatrixCell:
    block_size: str
    block_size_bytes: int
    jobs: int

    @property
    def name(self) -> str:
        return f"bs-{self.block_size.lower()}-jobs-{self.jobs}"


_BLOCK_SIZES = (
    ("32K", 32 * 1024),
    ("128K", 128 * 1024),
    ("256K", 256 * 1024),
    ("1M", 1024 * 1024),
    ("4M", 4 * 1024 * 1024),
)
_JOBS = (1, 4, 8)


def matrix_cells(*, quick: bool) -> tuple[MatrixCell, ...]:
    if quick:
        return (
            MatrixCell("32K", 32 * 1024, 1),
            MatrixCell("1M", 1024 * 1024, 4),
            MatrixCell("4M", 4 * 1024 * 1024, 8),
        )
    return tuple(
        MatrixCell(block_size, block_size_bytes, jobs)
        for block_size, block_size_bytes in _BLOCK_SIZES
        for jobs in _JOBS
    )


@dataclass(frozen=True, slots=True)
class MatrixFioResult:
    bytes: int
    runtime_ms: int
    requests: int
    errors: int
    requests_per_second: float
    mibps: float

    @classmethod
    def from_json(cls, path: Path) -> "MatrixFioResult":
        payload = json.loads(path.read_text(encoding="utf-8"))
        jobs = payload.get("jobs")
        if not isinstance(jobs, list) or not jobs:
            raise ValueError(f"fio output has no jobs: {path}")
        byte_count = 0
        runtime_ms = 0
        requests = 0
        errors = 0
        for job in jobs:
            stats = job.get("write")
            if not isinstance(stats, dict):
                raise ValueError(f"fio output has no write stats: {path}")
            if "total_ios" not in stats:
                raise ValueError(f"fio write stats have no total_ios counter: {path}")
            if "error" not in job:
                raise ValueError(f"fio job has no error counter: {path}")
            byte_count += int(stats.get("io_bytes", 0))
            runtime_ms = max(runtime_ms, int(stats.get("runtime", 0)))
            requests += int(stats["total_ios"])
            errors += int(job["error"])
        if byte_count <= 0 or runtime_ms <= 0 or requests <= 0:
            raise ValueError(
                "fio write did no measurable I/O: "
                f"bytes={byte_count}, runtime_ms={runtime_ms}, requests={requests}"
            )
        if errors:
            raise RuntimeError(f"fio reported I/O errors={errors}: {path}")
        seconds = runtime_ms / 1000
        return cls(
            bytes=byte_count,
            runtime_ms=runtime_ms,
            requests=requests,
            errors=errors,
            requests_per_second=round(requests / seconds, 3),
            mibps=round(byte_count / 1_048_576 / seconds, 3),
        )


def require_drained(snapshot: WritebackSnapshot, *, phase: str) -> None:
    if not snapshot.drained:
        raise RuntimeError(
            f"{phase} writeback boundary is not drained: {snapshot.to_dict()}"
        )


@dataclass(frozen=True, slots=True)
class BlockIoDelta:
    device: str
    read_bytes: int
    write_bytes: int
    busy_ms: int

    @classmethod
    def between(cls, before: BlockIoSnapshot, after: BlockIoSnapshot) -> "BlockIoDelta":
        if before.device != after.device:
            raise RuntimeError("NBD device changed during matrix cell")
        return cls(
            device=before.device,
            read_bytes=max(0, after.read_bytes - before.read_bytes),
            write_bytes=max(0, after.write_bytes - before.write_bytes),
            busy_ms=max(0, after.busy_ms - before.busy_ms),
        )


@dataclass(frozen=True, slots=True)
class RemoteCrossing:
    target_sequence: int
    timestamp_ns: int
    snapshot: WritebackSnapshot


@dataclass(frozen=True, slots=True)
class MatrixCellResult:
    cell: MatrixCell
    total_bytes: int
    fio: MatrixFioResult
    before: WritebackSnapshot
    after_fio: WritebackSnapshot
    accepted: WritebackSnapshot
    after_syncfs: WritebackSnapshot
    remote_crossing: RemoteCrossing
    post_drain: WritebackSnapshot
    write_start_ns: int
    write_end_ns: int
    syncfs_start_ns: int
    syncfs_end_ns: int
    syncfs_local_tail_ms: int
    remote_end_to_end_ms: int
    remote_tail_after_syncfs_ms: int
    nbd_io: BlockIoDelta
    system_io: SystemIoSummary

    def to_dict(self) -> dict[str, object]:
        return asdict(self)

    @property
    def maintenance_before(self) -> WritebackSnapshot:
        return self.before


@dataclass(frozen=True, slots=True)
class RunAuthority:
    source_commit: str
    source_dirty: bool
    config_file: str
    config_sha256: str
    nbd_device_path: str
    nbd_device_major: int
    nbd_device_minor: int
    nbd_device_name: str
    local_device_major: int
    local_device_minor: int
    local_device_name: str
    deployed_commit: str
    running_binary_sha256: str
    lifecycle_config_sha256: str


@dataclass(frozen=True, slots=True)
class PerformanceMatrixResult:
    total_mib: int
    quick: bool
    authority: RunAuthority
    cells: tuple[MatrixCellResult, ...]
    receipt_dir: str

    def to_dict(self) -> dict[str, object]:
        return {
            "schema": 1,
            "total_mib": self.total_mib,
            "quick": self.quick,
            "authority": asdict(self.authority),
            "cell_count": len(self.cells),
            "cells": [cell.to_dict() for cell in self.cells],
            "receipt_dir": self.receipt_dir,
        }


class MatrixCleanupError(RuntimeError):
    def __init__(
        self,
        primary: BaseException | None,
        cleanup_errors: list[BaseException],
    ) -> None:
        self.primary = primary
        self.cleanup_errors = tuple(cleanup_errors)
        details = "; ".join(str(error) for error in cleanup_errors)
        super().__init__(f"performance matrix cleanup failed: {details}")


class PerformanceMatrixRunner:
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

    def _local_device_identity(self) -> tuple[int, int, str]:
        device = self._local_device()
        snapshot = BlockIoSnapshot.capture(self.config.proc_root, device=device)
        return (*device, snapshot.device)

    def _nbd_device(self) -> tuple[int, int, str]:
        device = block_device(self.config.nbd_device)
        snapshot = BlockIoSnapshot.capture(self.config.proc_root, device=device)
        return (*device, snapshot.device)

    def _authority(self) -> RunAuthority:
        status = self.lifecycle.status()
        commit = self.runner.run(
            ["git", "-C", self.config.root, "rev-parse", "HEAD"]
        ).stdout.strip()
        if len(commit) != 40:
            raise RuntimeError(f"invalid source commit receipt: {commit!r}")
        dirty = self.runner.run(
            ["git", "-C", self.config.root, "status", "--porcelain"]
        ).stdout
        config_digest = self.runner.run(
            ["sha256sum", self.config.config_file], sudo=True
        ).stdout.split()[0]
        if len(config_digest) != 64:
            raise RuntimeError(f"invalid config SHA-256 receipt: {config_digest!r}")
        deployed_commit = str(status.get("deployed_commit", ""))
        running_binary_sha256 = str(status.get("running_binary_sha256", ""))
        lifecycle_config_sha256 = str(status.get("config_sha256", ""))
        if len(deployed_commit) != 40:
            raise RuntimeError(
                f"invalid lifecycle deployed commit receipt: {deployed_commit!r}"
            )
        if len(running_binary_sha256) != 64:
            raise RuntimeError(
                "invalid lifecycle running binary SHA-256 receipt: "
                f"{running_binary_sha256!r}"
            )
        if len(lifecycle_config_sha256) != 64:
            raise RuntimeError(
                "invalid lifecycle config SHA-256 receipt: "
                f"{lifecycle_config_sha256!r}"
            )
        if commit != deployed_commit:
            raise RuntimeError(
                "checkout commit does not match deployed commit: "
                f"checkout={commit}, deployed={deployed_commit}"
            )
        if config_digest != lifecycle_config_sha256:
            raise RuntimeError(
                "independent config SHA-256 does not match lifecycle config: "
                f"independent={config_digest}, lifecycle={lifecycle_config_sha256}"
            )
        nbd_major, nbd_minor, nbd_name = self._nbd_device()
        local_major, local_minor, local_name = self._local_device_identity()
        return RunAuthority(
            source_commit=commit,
            source_dirty=bool(dirty),
            config_file=str(self.config.config_file),
            config_sha256=config_digest,
            nbd_device_path=str(self.config.nbd_device),
            nbd_device_major=nbd_major,
            nbd_device_minor=nbd_minor,
            nbd_device_name=nbd_name,
            local_device_major=local_major,
            local_device_minor=local_minor,
            local_device_name=local_name,
            deployed_commit=deployed_commit,
            running_binary_sha256=running_binary_sha256,
            lifecycle_config_sha256=lifecycle_config_sha256,
        )

    @staticmethod
    def _scratch_root() -> Path:
        return Path("/dev/shm")

    def _system_io(self, device: tuple[int, int]) -> SystemIoSnapshot:
        return SystemIoSnapshot.capture(self.config.proc_root, root_device=device)

    def _wait_clean_gc(self, timeout: float | None = None) -> WritebackSnapshot:
        baseline = self.lifecycle.metrics.snapshot().gc_passes
        return wait_for_gc_quiescence(
            self.lifecycle.metrics.snapshot,
            timeout=self.config.drain_timeout if timeout is None else timeout,
            after_pass=baseline,
        )

    def _wait_stable_gc_boundary(self, *, phase: str) -> WritebackSnapshot:
        deadline = time.monotonic() + self.config.drain_timeout
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError(f"{phase} GC/drain stabilization timed out")
            maintenance = self._wait_clean_gc(timeout=remaining)

            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError(f"{phase} GC/drain stabilization timed out")
            self.lifecycle.drain(timeout=remaining)
            drained = self.lifecycle.metrics.snapshot()

            drain_error: RuntimeError | None = None
            try:
                require_drained(drained, phase=phase)
            except RuntimeError as error:
                drain_error = error
            try:
                _assert_no_maintenance(maintenance, drained)
            except BenchmarkContaminatedError as error:
                if time.monotonic() >= deadline:
                    raise TimeoutError(
                        f"{phase} GC/drain stabilization timed out"
                    ) from error
                continue
            if drain_error is not None:
                raise drain_error
            return drained

    def _nbd_io(self) -> BlockIoSnapshot:
        return BlockIoSnapshot.capture(
            self.config.proc_root, device=block_device(self.config.nbd_device)
        )

    @staticmethod
    def _system_io_through_remote_crossing(
        sampler: _MetricSampler, remote_timestamp_ns: int
    ) -> list[SystemIoSnapshot]:
        crossing_ms = remote_timestamp_ns // 1_000_000
        snapshots = [
            SystemIoSnapshot(
                root_device=str(row[1]),
                root_read_bytes=int(row[2]),
                root_write_bytes=int(row[3]),
                root_busy_ms=int(row[4]),
                some_avg10=float(row[5]),
                full_avg10=float(row[6]),
                some_total_us=int(row[7]),
                full_total_us=int(row[8]),
            )
            for row in sampler.system_io_rows
            if int(row[0]) <= crossing_ms
        ]
        if not snapshots:
            raise RuntimeError(
                "system I/O sampler has no sample at the remote target crossing"
            )
        return snapshots

    @staticmethod
    def _monotonic_ns() -> int:
        import time

        return time.monotonic_ns()

    @staticmethod
    def _cell_bytes(cell: MatrixCell, total_mib: int) -> tuple[int, int]:
        if total_mib <= 0:
            raise ValueError("total MiB must be positive")
        total_bytes = total_mib * 1_048_576
        if total_bytes % cell.jobs:
            raise ValueError(
                f"total bytes must be divisible by jobs for {cell.name}: {total_bytes}"
            )
        per_job_bytes = total_bytes // cell.jobs
        if per_job_bytes % cell.block_size_bytes:
            raise ValueError(
                "per-job bytes must be divisible by block size for "
                f"{cell.name}: {per_job_bytes}"
            )
        return total_bytes, per_job_bytes

    def _run_fio(
        self,
        *,
        cell: MatrixCell,
        run_root: Path,
        per_job_bytes: int,
        output: Path,
    ) -> MatrixFioResult:
        argv: list[str | Path] = [
            "fio",
            f"--name=zerofs_nbd_matrix_{cell.name}",
            f"--directory={run_root}",
            "--filename_format=matrix.$jobnum",
            "--rw=write",
            f"--bs={cell.block_size}",
            f"--size={per_job_bytes}",
            f"--numjobs={cell.jobs}",
            "--ioengine=psync",
            "--iodepth=1",
            "--direct=1",
            "--fallocate=none",
            "--refill_buffers=1",
            "--scramble_buffers=1",
            "--buffer_compress_percentage=0",
            "--group_reporting",
            "--output-format=json",
            f"--output={output}",
        ]
        self.runner.run(argv, sudo=True)
        return MatrixFioResult.from_json(output)

    def _run_cell(
        self,
        *,
        cell: MatrixCell,
        total_mib: int,
        run_root: Path,
        fio_output: Path,
        sampler: _MetricSampler,
        maintenance_before: WritebackSnapshot,
    ) -> MatrixCellResult:
        total_bytes, per_job_bytes = self._cell_bytes(cell, total_mib)
        before = maintenance_before
        require_drained(before, phase=f"{cell.name} pre-cell")
        local_device = self._local_device()
        io_before = self._system_io(local_device)
        nbd_before = self._nbd_io()

        write_start_ns = self._monotonic_ns()
        fio = self._run_fio(
            cell=cell,
            run_root=run_root,
            per_job_bytes=per_job_bytes,
            output=fio_output,
        )
        write_end_ns = self._monotonic_ns()
        if fio.bytes != total_bytes:
            raise RuntimeError(
                f"fio returned short I/O for {cell.name}: "
                f"expected={total_bytes}, actual={fio.bytes}"
            )
        expected_requests = total_bytes // cell.block_size_bytes
        if fio.requests != expected_requests:
            raise RuntimeError(
                f"fio request count mismatch for {cell.name}: "
                f"expected={expected_requests}, actual={fio.requests}"
            )
        after_fio = self.lifecycle.metrics.snapshot()

        syncfs_start_ns = self._monotonic_ns()
        self.runner.run(["sync", "-f", self.config.mountpoint], sudo=True)
        syncfs_end_ns = self._monotonic_ns()
        accepted = wait_for_accepted_after(
            self.lifecycle.metrics.snapshot,
            previous_sequence=before.accepted,
            timeout=self.config.drain_timeout,
            stable_samples=1,
        )
        after_syncfs = self.lifecycle.metrics.snapshot()
        if after_syncfs.terminal:
            raise RuntimeError("writeback reported a terminal error")
        if after_syncfs.local < accepted.accepted:
            raise RuntimeError(
                "syncfs returned before ZeroFS reached local durability: "
                f"target={accepted.accepted}, local={after_syncfs.local}"
            )

        remote, remote_timestamp_ns = sampler.wait_for_remote(
            accepted.accepted, self.config.drain_timeout
        )
        if remote.remote < accepted.accepted:
            raise RuntimeError(
                "remote sampler returned before the target crossing: "
                f"target={accepted.accepted}, remote={remote.remote}"
            )
        nbd_after = self._nbd_io()
        crossing_io = self._system_io_through_remote_crossing(
            sampler, remote_timestamp_ns
        )
        self.lifecycle.drain()
        post_drain = self.lifecycle.metrics.snapshot()
        require_drained(post_drain, phase=f"{cell.name} post-cell")
        _assert_no_maintenance(before, post_drain)

        elapsed_ms = max(1, round((remote_timestamp_ns - write_start_ns) / 1_000_000))
        system_samples = [io_before, *crossing_io]
        return MatrixCellResult(
            cell=cell,
            total_bytes=total_bytes,
            fio=fio,
            before=before,
            after_fio=after_fio,
            accepted=accepted,
            after_syncfs=after_syncfs,
            remote_crossing=RemoteCrossing(
                target_sequence=accepted.accepted,
                timestamp_ns=remote_timestamp_ns,
                snapshot=remote,
            ),
            post_drain=post_drain,
            write_start_ns=write_start_ns,
            write_end_ns=write_end_ns,
            syncfs_start_ns=syncfs_start_ns,
            syncfs_end_ns=syncfs_end_ns,
            syncfs_local_tail_ms=max(
                0, round((syncfs_end_ns - syncfs_start_ns) / 1_000_000)
            ),
            remote_end_to_end_ms=elapsed_ms,
            remote_tail_after_syncfs_ms=max(
                0, round((remote_timestamp_ns - syncfs_end_ns) / 1_000_000)
            ),
            nbd_io=BlockIoDelta.between(nbd_before, nbd_after),
            system_io=summarize_system_io(system_samples, elapsed_ms=elapsed_ms),
        )

    def _prepare_root(self, run_root: Path) -> None:
        prepare_run_root(
            self.runner,
            self.config,
            run_root,
            prefix=".zerofs-matrix-",
            role="performance matrix root",
        )

    def _cleanup_root(self, run_root: Path) -> None:
        self.config.require_mount_child(
            run_root, ".zerofs-matrix-", "performance matrix root"
        )
        self.runner.run(["rm", "-rf", "--", run_root], sudo=True)
        self.runner.run(["sync", "-f", self.config.mountpoint], sudo=True)
        self.lifecycle.drain()
        require_drained(
            self.lifecycle.metrics.snapshot(), phase="post-cleanup performance matrix"
        )
        remains = self.runner.run(["test", "-e", run_root], sudo=True, check=False)
        if remains.returncode == 0:
            raise RuntimeError(
                f"performance matrix root remains after cleanup: {run_root}"
            )

    def _lay_out_cell(
        self,
        *,
        cell: MatrixCell,
        total_mib: int,
        run_root: Path,
        output: Path,
    ) -> None:
        """Write each cell file at full size before the measured pass.

        An extending O_DIRECT write forces an XFS size-update journal commit per
        request, and each journal commit issues a flush -- ZeroFS's full
        durability barrier. An extending pass therefore measures barrier latency
        rather than the service ACK this matrix compares across block sizes and
        job counts, and it does so unevenly: the barrier cost per request scales
        with request count, so small-block cells are penalized hardest and the
        block-size axis reports the barrier, not the device. Every cell gets a
        fresh run root, so without this pass every cell extends.

        The caller runs this before its syncfs/drain/GC-quiescence boundary, so
        the layout writes and any GC they provoke settle outside the measured
        epoch.
        """
        _, per_job_bytes = self._cell_bytes(cell, total_mib)
        self._run_fio(
            cell=cell,
            run_root=run_root,
            per_job_bytes=per_job_bytes,
            output=output,
        )

    def _measure_cell(
        self,
        *,
        cell: MatrixCell,
        total_mib: int,
        run_root: Path,
        fio_output: Path,
        layout_output: Path,
        metrics_output: Path,
        system_io_output: Path,
    ) -> MatrixCellResult:
        primary: BaseException | None = None
        result: MatrixCellResult | None = None
        sampler: _MetricSampler | None = None
        started = False
        try:
            self._prepare_root(run_root)
            self._lay_out_cell(
                cell=cell,
                total_mib=total_mib,
                run_root=run_root,
                output=layout_output,
            )
            self.runner.run(["sync", "-f", self.config.mountpoint], sudo=True)
            # syncfs can expose previously buffered mount work after an old
            # drained exporter sample. Drain only after that boundary so its
            # stable-sample window absorbs the metrics export cadence.
            self.lifecycle.drain()
            before = self._wait_stable_gc_boundary(phase=f"{cell.name} pre-cell")
            sampler = _MetricSampler(
                self.lifecycle,
                metrics_output,
                system_io_output,
                self._local_device(),
            )
            sampler.start()
            started = True
            result = self._run_cell(
                cell=cell,
                total_mib=total_mib,
                run_root=run_root,
                fio_output=fio_output,
                sampler=sampler,
                maintenance_before=before,
            )
        except BaseException as error:
            primary = error
        cleanup_errors: list[BaseException] = []
        if started and sampler is not None:
            try:
                sampler.stop()
            except BaseException as error:
                cleanup_errors.append(error)
        try:
            self._cleanup_root(run_root)
        except BaseException as error:
            cleanup_errors.append(error)
        if cleanup_errors:
            raise MatrixCleanupError(primary, cleanup_errors) from primary
        if primary is not None:
            raise primary
        if result is None:
            raise RuntimeError("performance matrix cell completed without a result")
        return result

    @staticmethod
    def _csv_row(result: MatrixCellResult) -> dict[str, object]:
        return {
            "block_size": result.cell.block_size,
            "block_size_bytes": result.cell.block_size_bytes,
            "jobs": result.cell.jobs,
            "total_bytes": result.total_bytes,
            "fio_bytes": result.fio.bytes,
            "fio_runtime_ms": result.fio.runtime_ms,
            "fio_requests": result.fio.requests,
            "fio_errors": result.fio.errors,
            "fio_requests_per_second": result.fio.requests_per_second,
            "fio_mibps": result.fio.mibps,
            "syncfs_local_tail_ms": result.syncfs_local_tail_ms,
            "remote_end_to_end_ms": result.remote_end_to_end_ms,
            "remote_tail_after_syncfs_ms": result.remote_tail_after_syncfs_ms,
            "remote_target_sequence": result.remote_crossing.target_sequence,
            "remote_crossing_timestamp_ns": result.remote_crossing.timestamp_ns,
            "before_accepted": result.before.accepted,
            "before_local": result.before.local,
            "before_remote": result.before.remote,
            "after_fio_accepted": result.after_fio.accepted,
            "after_syncfs_local": result.after_syncfs.local,
            "post_drain_remote": result.post_drain.remote,
            "local_completed_bytes": (
                result.after_syncfs.local_bytes - result.before.local_bytes
            ),
            "remote_completed_bytes": (
                result.post_drain.remote_bytes - result.before.remote_bytes
            ),
            "nbd_device": result.nbd_io.device,
            "nbd_read_bytes": result.nbd_io.read_bytes,
            "nbd_write_bytes": result.nbd_io.write_bytes,
            "nbd_busy_ms": result.nbd_io.busy_ms,
            "local_device": result.system_io.root_device,
            "disk_read_mib": result.system_io.root_read_mib,
            "disk_write_mib": result.system_io.root_write_mib,
            "disk_busy_ms": result.system_io.root_busy_ms,
            "disk_utilization_percent": result.system_io.root_utilization_percent,
            "psi_some_stall_ms": result.system_io.some_stall_ms,
            "psi_full_stall_ms": result.system_io.full_stall_ms,
            "psi_peak_some_avg10": result.system_io.peak_some_avg10,
            "psi_peak_full_avg10": result.system_io.peak_full_avg10,
        }

    def _persist_results(
        self,
        *,
        receipt: RunReceipt,
        scratch: Path,
        result: PerformanceMatrixResult,
    ) -> None:
        for source in sorted(scratch.iterdir()):
            if not source.is_file():
                continue
            destination = receipt.path(source.name)
            shutil.copy2(source, destination)
        summary = receipt.path("summary.json")
        summary.write_text(
            json.dumps(result.to_dict(), indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        rows = [self._csv_row(cell) for cell in result.cells]
        cells_csv = receipt.path("cells.csv")
        with cells_csv.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
            writer.writeheader()
            writer.writerows(rows)

    def run(
        self, *, total_mib: int = 256, quick: bool = False
    ) -> PerformanceMatrixResult:
        cells = matrix_cells(quick=quick)
        for cell in cells:
            self._cell_bytes(cell, total_mib)
        authority = self._authority()
        scratch_root = self._scratch_root()
        if not scratch_root.is_dir():
            raise RuntimeError(
                f"performance matrix tmpfs root is unavailable: {scratch_root}"
            )
        scratch = Path(
            tempfile.mkdtemp(prefix="zerofs-performance-matrix-", dir=scratch_root)
        )
        receipt = RunReceipt.start(self.config, "performance-matrix")
        primary: BaseException | None = None
        try:
            with receipt:
                receipt.record("schema", 1)
                receipt.record("total_mib", total_mib)
                receipt.record("quick", quick)
                receipt.record("cell_count", len(cells))
                receipt.record("authority", asdict(authority))
                measured: list[MatrixCellResult] = []
                for index, cell in enumerate(cells, start=1):
                    stem = f"cell-{index:02d}-{cell.name}"
                    run_root = (
                        self.config.mountpoint
                        / f".zerofs-matrix-{index:02d}-{uuid.uuid4().hex}"
                    )
                    measured.append(
                        self._measure_cell(
                            cell=cell,
                            total_mib=total_mib,
                            run_root=run_root,
                            fio_output=scratch / f"{stem}-fio.json",
                            layout_output=scratch / f"{stem}-layout.json",
                            metrics_output=scratch / f"{stem}-writeback.csv",
                            system_io_output=scratch / f"{stem}-system-io.csv",
                        )
                    )
                result = PerformanceMatrixResult(
                    total_mib=total_mib,
                    quick=quick,
                    authority=authority,
                    cells=tuple(measured),
                    receipt_dir=str(receipt.directory),
                )
                self._persist_results(
                    receipt=receipt,
                    scratch=scratch,
                    result=result,
                )
                receipt.record("cells", [cell.to_dict() for cell in result.cells])
                shutil.rmtree(scratch)
                return result
        except BaseException as error:
            primary = error
            raise
        finally:
            if scratch.exists():
                try:
                    shutil.rmtree(scratch)
                except BaseException as cleanup_error:
                    if primary is None:
                        raise MatrixCleanupError(None, [cleanup_error]) from None
                    primary.add_note(f"scratch cleanup failed: {cleanup_error}")
