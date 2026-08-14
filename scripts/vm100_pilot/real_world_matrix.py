from __future__ import annotations

import json
import shutil
import tempfile
import uuid
from dataclasses import asdict
from dataclasses import dataclass, replace
from pathlib import Path

from .metrics import WritebackSnapshot
from .metrics import wait_for_accepted_after
from .config import PilotConfig
from .lifecycle import PilotLifecycle
from .runner import Runner
from .receipts import RunReceipt
from .performance_matrix import PerformanceMatrixRunner, require_drained
from .benchmark import _assert_no_maintenance
from .system_io import prepare_run_root


KIB = 1024
MIB = 1024 * KIB
GIB = 1024 * MIB


@dataclass(frozen=True, slots=True)
class RealWorldCell:
    name: str
    operation: str
    file_size_bytes: int
    files_per_job: int
    jobs: int
    block_size: str
    block_size_bytes: int
    queue_depth: int
    io_mode: str
    access: str
    pattern: str
    layout: str
    cache_state: str = "none"

    @property
    def total_bytes(self) -> int:
        return self.file_size_bytes * self.files_per_job * self.jobs


_QUICK_CELLS = (
    RealWorldCell(
        "tiny-repeat-fresh-buffered",
        "write",
        4 * KIB,
        64,
        1,
        "4K",
        4 * KIB,
        1,
        "buffered",
        "sequential",
        "repeat",
        "fresh",
    ),
    RealWorldCell(
        "1m-incompressible-preallocated-direct",
        "write",
        MIB,
        1,
        1,
        "128K",
        128 * KIB,
        4,
        "direct",
        "sequential",
        "incompressible",
        "preallocated",
    ),
    RealWorldCell(
        "32m-zero-sparse-direct",
        "write",
        32 * MIB,
        1,
        1,
        "1M",
        MIB,
        8,
        "direct",
        "sequential",
        "zero",
        "sparse",
    ),
    RealWorldCell(
        "32m-incompressible-preallocated-direct-4j",
        "write",
        32 * MIB,
        1,
        4,
        "1M",
        MIB,
        8,
        "direct",
        "sequential",
        "incompressible",
        "preallocated",
    ),
    RealWorldCell(
        "32m-repeat-warm-buffered-random-4j",
        "write",
        32 * MIB,
        1,
        4,
        "128K",
        128 * KIB,
        8,
        "buffered",
        "random",
        "repeat",
        "warm",
    ),
    RealWorldCell(
        "32m-incompressible-cold-buffered-read",
        "read",
        32 * MIB,
        1,
        1,
        "1M",
        MIB,
        4,
        "buffered",
        "sequential",
        "incompressible",
        "preallocated",
        "cold",
    ),
    RealWorldCell(
        "32m-repeat-warm-buffered-read",
        "read",
        32 * MIB,
        1,
        1,
        "128K",
        128 * KIB,
        4,
        "buffered",
        "sequential",
        "repeat",
        "preallocated",
        "warm",
    ),
    RealWorldCell(
        "1m-zero-direct-random-read-4j",
        "read",
        MIB,
        1,
        4,
        "4K",
        4 * KIB,
        8,
        "direct",
        "random",
        "zero",
        "preallocated",
        "direct",
    ),
)

_FULL_ONLY_CELLS = (
    RealWorldCell(
        "tiny-incompressible-sparse-direct-random-4j",
        "write",
        4 * KIB,
        64,
        4,
        "4K",
        4 * KIB,
        8,
        "direct",
        "random",
        "incompressible",
        "sparse",
    ),
    RealWorldCell(
        "1m-zero-fresh-buffered",
        "write",
        MIB,
        1,
        1,
        "32K",
        32 * KIB,
        1,
        "buffered",
        "sequential",
        "zero",
        "fresh",
    ),
    RealWorldCell(
        "1m-repeat-sparse-direct-random",
        "write",
        MIB,
        1,
        1,
        "4K",
        4 * KIB,
        8,
        "direct",
        "random",
        "repeat",
        "sparse",
    ),
    RealWorldCell(
        "32m-repeat-warm-direct",
        "write",
        32 * MIB,
        1,
        1,
        "1M",
        MIB,
        8,
        "direct",
        "sequential",
        "repeat",
        "warm",
    ),
    RealWorldCell(
        "1g-incompressible-preallocated-direct",
        "write",
        GIB,
        1,
        1,
        "1M",
        MIB,
        16,
        "direct",
        "sequential",
        "incompressible",
        "preallocated",
    ),
    RealWorldCell(
        "1g-zero-sparse-direct",
        "write",
        GIB,
        1,
        1,
        "1M",
        MIB,
        16,
        "direct",
        "sequential",
        "zero",
        "sparse",
    ),
    RealWorldCell(
        "1g-incompressible-cold-buffered-read",
        "read",
        GIB,
        1,
        1,
        "1M",
        MIB,
        8,
        "buffered",
        "sequential",
        "incompressible",
        "preallocated",
        "cold",
    ),
    RealWorldCell(
        "1g-repeat-warm-buffered-read",
        "read",
        GIB,
        1,
        1,
        "1M",
        MIB,
        8,
        "buffered",
        "sequential",
        "repeat",
        "preallocated",
        "warm",
    ),
    RealWorldCell(
        "1g-incompressible-direct-random-read",
        "read",
        GIB,
        1,
        1,
        "128K",
        128 * KIB,
        16,
        "direct",
        "random",
        "incompressible",
        "preallocated",
        "direct",
    ),
)


def real_world_cells(*, quick: bool) -> tuple[RealWorldCell, ...]:
    if quick:
        return _QUICK_CELLS
    return _QUICK_CELLS + _FULL_ONLY_CELLS


def counter_delta(*, after: int, before: int, label: str) -> int:
    if after < before:
        raise RuntimeError(f"{label} counter regressed: before={before}, after={after}")
    return after - before


@dataclass(frozen=True, slots=True)
class RealWorldFioResult:
    bytes: int
    runtime_ms: int
    requests: int
    errors: int
    mibps: float
    requests_per_second: float

    @classmethod
    def from_json(
        cls,
        path: Path,
        *,
        operation: str,
        expected_bytes: int,
        expected_requests: int,
    ) -> "RealWorldFioResult":
        payload = json.loads(path.read_text(encoding="utf-8"))
        jobs = payload.get("jobs")
        if not isinstance(jobs, list) or not jobs:
            raise ValueError(f"fio output has no jobs: {path}")
        byte_count = 0
        runtime_ms = 0
        requests = 0
        errors = 0
        for job in jobs:
            stats = job.get(operation)
            if not isinstance(stats, dict):
                raise ValueError(f"fio output has no {operation} stats: {path}")
            if "total_ios" not in stats:
                raise ValueError(
                    f"fio {operation} stats have no total_ios counter: {path}"
                )
            if "error" not in job:
                raise ValueError(f"fio job has no error counter: {path}")
            byte_count += int(stats.get("io_bytes", 0))
            runtime_ms = max(runtime_ms, int(stats.get("runtime", 0)))
            requests += int(stats["total_ios"])
            errors += int(job["error"])
        if errors:
            raise RuntimeError(f"fio reported I/O errors={errors}: {path}")
        if byte_count != expected_bytes:
            raise RuntimeError(
                "fio returned short I/O: "
                f"expected={expected_bytes}, actual={byte_count}"
            )
        if requests != expected_requests:
            raise RuntimeError(
                "fio request count mismatch: "
                f"expected={expected_requests}, actual={requests}"
            )
        if runtime_ms <= 0:
            raise ValueError(f"fio {operation} has no measurable runtime: {path}")
        seconds = runtime_ms / 1000
        return cls(
            bytes=byte_count,
            runtime_ms=runtime_ms,
            requests=requests,
            errors=errors,
            mibps=round(byte_count / MIB / seconds, 3),
            requests_per_second=round(requests / seconds, 3),
        )


def build_fio_argv(
    cell: RealWorldCell,
    *,
    directory: Path,
    output: Path,
) -> list[str | Path]:
    rw = {
        ("write", "sequential"): "write",
        ("write", "random"): "randwrite",
        ("read", "sequential"): "read",
        ("read", "random"): "randread",
    }[(cell.operation, cell.access)]
    argv: list[str | Path] = [
        "fio",
        f"--name=zerofs_real_world_{cell.name}",
        f"--directory={directory}",
        "--filename_format=data.$jobnum.$filenum",
        f"--rw={rw}",
        f"--bs={cell.block_size}",
        f"--size={cell.file_size_bytes * cell.files_per_job}",
        f"--filesize={cell.file_size_bytes}",
        f"--nrfiles={cell.files_per_job}",
        f"--numjobs={cell.jobs}",
        "--ioengine=io_uring",
        f"--iodepth={cell.queue_depth}",
        f"--direct={1 if cell.io_mode == 'direct' else 0}",
        "--fallocate=none",
        "--allow_file_create=0",
        "--overwrite=1",
        "--randrepeat=1",
        "--randseed=305419896",
        "--group_reporting",
        "--output-format=json",
        f"--output={output}",
    ]
    if cell.operation == "read":
        argv.append(
            "--invalidate=0" if cell.cache_state == "warm" else "--invalidate=1"
        )
    if cell.pattern == "zero":
        argv.append("--zero_buffers=1")
    elif cell.pattern == "repeat":
        argv.append("--buffer_pattern=0x5a")
    elif cell.pattern == "incompressible":
        argv.extend(
            (
                "--refill_buffers=1",
                "--scramble_buffers=1",
                "--buffer_compress_percentage=0",
            )
        )
    else:  # pragma: no cover - all cells are defined above.
        raise ValueError(f"unsupported data pattern: {cell.pattern}")
    return argv


def parse_sha256_manifest(
    text: str,
    *,
    expected_paths: tuple[Path, ...],
) -> tuple[tuple[str, str], ...]:
    found: dict[str, str] = {}
    for raw_line in text.splitlines():
        if not raw_line:
            continue
        digest = raw_line[:64]
        if len(digest) != 64 or any(
            character not in "0123456789abcdef" for character in digest
        ):
            raise RuntimeError(f"invalid SHA-256 receipt line: {raw_line!r}")
        if len(raw_line) < 67 or raw_line[64:66] not in {"  ", " *"}:
            raise RuntimeError(f"invalid SHA-256 receipt line: {raw_line!r}")
        path = raw_line[66:]
        if path in found:
            raise RuntimeError(f"duplicate SHA-256 receipt path: {path}")
        found[path] = digest
    expected = {str(path) for path in expected_paths}
    if set(found) != expected:
        raise RuntimeError(
            "digest file set mismatch: "
            f"expected={sorted(expected)}, actual={sorted(found)}"
        )
    return tuple((path, found[path]) for path in sorted(expected))


@dataclass(frozen=True, slots=True)
class FileAllocation:
    path: str
    logical_bytes: int
    allocated_bytes: int

    @property
    def sparse(self) -> bool:
        return self.allocated_bytes < self.logical_bytes


def parse_stat_receipt(
    text: str,
    *,
    expected_paths: tuple[Path, ...],
    expected_size: int,
) -> tuple[FileAllocation, ...]:
    found: dict[str, FileAllocation] = {}
    for raw_line in text.splitlines():
        if not raw_line:
            continue
        parts = raw_line.split("\t")
        if len(parts) != 4:
            raise RuntimeError(f"invalid stat receipt line: {raw_line!r}")
        path, size_text, blocks_text, block_size_text = parts
        try:
            logical_bytes = int(size_text)
            allocated_bytes = int(blocks_text) * int(block_size_text)
        except ValueError as error:
            raise RuntimeError(f"invalid stat receipt line: {raw_line!r}") from error
        if logical_bytes != expected_size:
            raise RuntimeError(
                "logical size mismatch: "
                f"path={path}, expected={expected_size}, actual={logical_bytes}"
            )
        if path in found:
            raise RuntimeError(f"duplicate stat receipt path: {path}")
        found[path] = FileAllocation(path, logical_bytes, allocated_bytes)
    expected = {str(path) for path in expected_paths}
    if set(found) != expected:
        raise RuntimeError(
            "stat file set mismatch: "
            f"expected={sorted(expected)}, actual={sorted(found)}"
        )
    return tuple(found[path] for path in sorted(expected))


def cell_paths(cell: RealWorldCell, run_root: Path) -> tuple[Path, ...]:
    return tuple(
        run_root / f"data.{job}.{file_number}"
        for job in range(cell.jobs)
        for file_number in range(cell.files_per_job)
    )


def validate_layout(
    *,
    layout: str,
    phase: str,
    allocations: tuple[FileAllocation, ...],
    expected_size: int,
) -> None:
    if phase == "after":
        for allocation in allocations:
            if allocation.logical_bytes != expected_size:
                raise RuntimeError(
                    "post-I/O logical size mismatch: "
                    f"path={allocation.path}, expected={expected_size}, "
                    f"actual={allocation.logical_bytes}"
                )
            if allocation.allocated_bytes < expected_size:
                raise RuntimeError(
                    "post-I/O allocation mismatch: "
                    f"path={allocation.path}, expected_at_least={expected_size}, "
                    f"actual={allocation.allocated_bytes}"
                )
        return
    if phase != "before":
        raise ValueError(f"unsupported allocation phase: {phase}")
    for allocation in allocations:
        if layout == "fresh":
            if allocation.logical_bytes != 0:
                raise RuntimeError(
                    f"fresh file is not empty before I/O: {allocation.path}"
                )
        elif layout == "sparse":
            if allocation.logical_bytes != expected_size or allocation.allocated_bytes:
                raise RuntimeError(
                    f"sparse file has allocated data before I/O: {allocation.path}"
                )
        elif layout in {"preallocated", "warm"}:
            if (
                allocation.logical_bytes != expected_size
                or allocation.allocated_bytes < expected_size
            ):
                raise RuntimeError(
                    "file is not physically preallocated before I/O: "
                    f"path={allocation.path}, logical={allocation.logical_bytes}, "
                    f"allocated={allocation.allocated_bytes}"
                )
        else:
            raise ValueError(f"unsupported file layout: {layout}")


@dataclass(frozen=True, slots=True)
class WritebackDelta:
    accepted_sequences: int
    local_sequences: int
    remote_sequences: int
    local_encoded_bytes: int
    remote_encoded_bytes: int
    gc_passes: int
    gc_batches: int
    gc_deleted_bytes: int


def writeback_delta(
    before: WritebackSnapshot,
    after: WritebackSnapshot,
) -> WritebackDelta:
    return WritebackDelta(
        accepted_sequences=counter_delta(
            after=after.accepted, before=before.accepted, label="accepted"
        ),
        local_sequences=counter_delta(
            after=after.local, before=before.local, label="local"
        ),
        remote_sequences=counter_delta(
            after=after.remote, before=before.remote, label="remote"
        ),
        local_encoded_bytes=counter_delta(
            after=after.local_bytes,
            before=before.local_bytes,
            label="local encoded bytes",
        ),
        remote_encoded_bytes=counter_delta(
            after=after.remote_bytes,
            before=before.remote_bytes,
            label="remote encoded bytes",
        ),
        gc_passes=counter_delta(
            after=after.gc_passes, before=before.gc_passes, label="GC passes"
        ),
        gc_batches=counter_delta(
            after=after.gc_batches, before=before.gc_batches, label="GC batches"
        ),
        gc_deleted_bytes=counter_delta(
            after=after.gc_deleted_bytes,
            before=before.gc_deleted_bytes,
            label="GC deleted bytes",
        ),
    )


def require_read_quiet(
    before: WritebackSnapshot,
    after: WritebackSnapshot,
    *,
    phase: str,
) -> None:
    delta = writeback_delta(before, after)
    if not after.drained:
        raise RuntimeError(
            f"{phase} writeback boundary is not drained: {after.to_dict()}"
        )
    if any(
        (
            delta.accepted_sequences,
            delta.local_sequences,
            delta.remote_sequences,
            delta.local_encoded_bytes,
            delta.remote_encoded_bytes,
        )
    ):
        raise RuntimeError(
            f"{phase} unexpectedly changed writeback counters: {asdict(delta)}"
        )


def require_local_cutoff(snapshot: WritebackSnapshot, *, phase: str) -> None:
    if snapshot.terminal:
        raise RuntimeError(f"{phase} writeback reported a terminal error")
    if snapshot.local < snapshot.accepted:
        raise RuntimeError(
            f"{phase} returned before local durability: "
            f"accepted={snapshot.accepted}, local={snapshot.local}"
        )


def _jsonable(value: object) -> object:
    if isinstance(value, dict):
        return value
    if hasattr(value, "to_dict"):
        return value.to_dict()  # type: ignore[union-attr]
    return asdict(value)  # type: ignore[arg-type]


@dataclass(frozen=True, slots=True)
class RealWorldMatrixResult:
    quick: bool
    authority: object
    cells: tuple[object, ...]
    receipt_dir: str

    def to_dict(self) -> dict[str, object]:
        return {
            "schema": 1,
            "quick": self.quick,
            "authority": _jsonable(self.authority),
            "cell_count": len(self.cells),
            "total_measured_bytes": sum(
                int(cell.cell.total_bytes)  # type: ignore[union-attr]
                for cell in self.cells
            ),
            "cells": [_jsonable(cell) for cell in self.cells],
            "receipt_dir": self.receipt_dir,
        }


@dataclass(frozen=True, slots=True)
class RealWorldCellResult:
    cell: RealWorldCell
    fio: RealWorldFioResult
    before: WritebackSnapshot
    after_fio: WritebackSnapshot
    after_syncfs: WritebackSnapshot
    post_drain: WritebackSnapshot
    writeback: WritebackDelta
    allocation_before: tuple[FileAllocation, ...]
    allocation_after: tuple[FileAllocation, ...]
    hashes_before: tuple[tuple[str, str], ...]
    hashes_after: tuple[tuple[str, str], ...]
    content_hash_unchanged: bool | None
    foreground_ms: int
    local_flush_tail_ms: int
    remote_tail_ms: int
    local_end_to_end_ms: int
    remote_end_to_end_ms: int
    local_compression_ratio: float | None
    remote_compression_ratio: float | None

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


class RealWorldMatrixCleanupError(RuntimeError):
    def __init__(
        self,
        primary: BaseException | None,
        cleanup_errors: list[BaseException],
    ) -> None:
        self.primary = primary
        self.cleanup_errors = tuple(cleanup_errors)
        details = "; ".join(str(error) for error in cleanup_errors)
        super().__init__(f"real-world matrix cleanup failed: {details}")


class RealWorldMatrixRunner:
    def __init__(
        self,
        config: PilotConfig,
        runner: Runner,
        lifecycle: PilotLifecycle,
    ) -> None:
        self.config = config
        self.runner = runner
        self.lifecycle = lifecycle

    def _run_fio(
        self,
        cell: RealWorldCell,
        *,
        run_root: Path,
        output: Path,
    ) -> RealWorldFioResult:
        self.runner.run(
            build_fio_argv(cell, directory=run_root, output=output), sudo=True
        )
        return RealWorldFioResult.from_json(
            output,
            operation=cell.operation,
            expected_bytes=cell.total_bytes,
            expected_requests=cell.total_bytes // cell.block_size_bytes,
        )

    def _prepare_layout(
        self,
        cell: RealWorldCell,
        *,
        run_root: Path,
        fixture_output: Path,
    ) -> None:
        paths = cell_paths(cell, run_root)
        for path in paths:
            self.runner.run(["truncate", "-s", "0", path], sudo=True)
        if cell.layout in {"sparse"}:
            for path in paths:
                self.runner.run(
                    ["truncate", "-s", str(cell.file_size_bytes), path], sudo=True
                )
        elif cell.layout in {"preallocated", "warm"}:
            for path in paths:
                self.runner.run(
                    ["fallocate", "-l", str(cell.file_size_bytes), path], sudo=True
                )
        elif cell.layout != "fresh":
            raise ValueError(f"unsupported file layout: {cell.layout}")
        if cell.operation == "read" or cell.layout == "warm":
            fixture = replace(
                cell,
                name=f"{cell.name}-fixture",
                operation="write",
                access="sequential",
                cache_state="none",
            )
            self._run_fio(fixture, run_root=run_root, output=fixture_output)

    def _prepare_root(self, run_root: Path) -> None:
        prepare_run_root(
            self.runner,
            self.config,
            run_root,
            prefix=".zerofs-real-world-",
            role="real-world matrix root",
        )

    def _stable_boundary(self, *, phase: str) -> WritebackSnapshot:
        self.runner.run(["sync", "-f", self.config.mountpoint], sudo=True)
        self.lifecycle.drain()
        return PerformanceMatrixRunner(
            self.config, self.runner, self.lifecycle
        )._wait_stable_gc_boundary(phase=phase)

    def _capture_allocation(
        self,
        cell: RealWorldCell,
        *,
        run_root: Path,
        output: Path,
        phase: str,
    ) -> tuple[FileAllocation, ...]:
        paths = cell_paths(cell, run_root)
        completed = self.runner.run(
            ["stat", "-c", "%n\t%s\t%b\t%B", "--", *paths], sudo=True
        )
        output.write_text(completed.stdout, encoding="utf-8")
        expected_size = (
            0 if phase == "before" and cell.layout == "fresh" else cell.file_size_bytes
        )
        allocations = parse_stat_receipt(
            completed.stdout,
            expected_paths=paths,
            expected_size=expected_size,
        )
        validate_layout(
            layout=cell.layout,
            phase=phase,
            allocations=allocations,
            expected_size=cell.file_size_bytes,
        )
        return allocations

    def _capture_hashes(
        self,
        cell: RealWorldCell,
        *,
        run_root: Path,
        output: Path,
    ) -> tuple[tuple[str, str], ...]:
        paths = cell_paths(cell, run_root)
        completed = self.runner.run(["sha256sum", "--", *paths], sudo=True)
        output.write_text(completed.stdout, encoding="utf-8")
        return parse_sha256_manifest(completed.stdout, expected_paths=paths)

    def _capture_fiemap(
        self,
        cell: RealWorldCell,
        *,
        run_root: Path,
        output: Path,
    ) -> None:
        completed = self.runner.run(
            ["filefrag", "-e", "-v", "--", *cell_paths(cell, run_root)],
            sudo=True,
            check=False,
        )
        output.write_text(
            (completed.stdout or "") + (completed.stderr or ""), encoding="utf-8"
        )

    def _wait_accepted(self, snapshot: WritebackSnapshot) -> WritebackSnapshot:
        return wait_for_accepted_after(
            self.lifecycle.metrics.snapshot,
            previous_sequence=snapshot.accepted,
            timeout=self.config.drain_timeout,
            stable_samples=1,
        )

    @staticmethod
    def _now_ns() -> int:
        import time

        return time.monotonic_ns()

    def _cleanup_root(self, run_root: Path) -> None:
        self.config.require_mount_child(
            run_root, ".zerofs-real-world-", "real-world matrix root"
        )
        self.runner.run(["rm", "-rf", "--", run_root], sudo=True)
        self.runner.run(["sync", "-f", self.config.mountpoint], sudo=True)
        self.lifecycle.drain()
        require_drained(
            self.lifecycle.metrics.snapshot(), phase="post-cleanup real-world matrix"
        )
        remains = self.runner.run(["test", "-e", run_root], sudo=True, check=False)
        if remains.returncode == 0:
            raise RuntimeError(
                f"real-world matrix root remains after cleanup: {run_root}"
            )

    @staticmethod
    def _scratch_root() -> Path:
        return Path("/dev/shm")

    def _authority(self) -> object:
        return PerformanceMatrixRunner(
            self.config, self.runner, self.lifecycle
        )._authority()

    def _measure_cell(
        self,
        *,
        cell: RealWorldCell,
        run_root: Path,
        fio_output: Path,
        fixture_output: Path,
        pre_stat_output: Path,
        post_stat_output: Path,
        pre_hash_output: Path,
        post_hash_output: Path,
        fiemap_output: Path,
    ) -> RealWorldCellResult:
        primary: BaseException | None = None
        result: RealWorldCellResult | None = None
        prepared = False
        try:
            prepared = True
            self._prepare_root(run_root)
            self._prepare_layout(cell, run_root=run_root, fixture_output=fixture_output)
            allocation_before = self._capture_allocation(
                cell,
                run_root=run_root,
                output=pre_stat_output,
                phase="before",
            )
            hashes_before = (
                self._capture_hashes(cell, run_root=run_root, output=pre_hash_output)
                if cell.operation == "read"
                else ()
            )
            self._capture_fiemap(cell, run_root=run_root, output=fiemap_output)
            before = self._stable_boundary(phase=f"{cell.name} pre-cell")
            require_drained(before, phase=f"{cell.name} pre-cell")

            foreground_start = self._now_ns()
            fio = self._run_fio(cell, run_root=run_root, output=fio_output)
            foreground_end = self._now_ns()
            after_fio = self.lifecycle.metrics.snapshot()
            writeback_delta(before, after_fio)

            sync_start = foreground_end
            sync_end = foreground_end
            remote_end = foreground_end
            if cell.operation == "write":
                accepted = self._wait_accepted(before)
                writeback_delta(before, accepted)
                sync_start = self._now_ns()
                self.runner.run(["sync", "-f", self.config.mountpoint], sudo=True)
                sync_end = self._now_ns()
                after_syncfs = self.lifecycle.metrics.snapshot()
                writeback_delta(before, after_syncfs)
                require_local_cutoff(after_syncfs, phase=f"{cell.name} syncfs")
                self.lifecycle.drain()
                remote_end = self._now_ns()
                post_drain = self.lifecycle.metrics.snapshot()
                require_drained(post_drain, phase=f"{cell.name} post-cell")
                if post_drain.remote < accepted.accepted:
                    raise RuntimeError(
                        "drain returned before remote durability: "
                        f"target={accepted.accepted}, remote={post_drain.remote}"
                    )
            else:
                after_syncfs = after_fio
                post_drain = after_fio
                require_read_quiet(before, post_drain, phase=f"{cell.name} read")

            allocation_after = self._capture_allocation(
                cell,
                run_root=run_root,
                output=post_stat_output,
                phase="after",
            )
            hashes_after = self._capture_hashes(
                cell, run_root=run_root, output=post_hash_output
            )
            content_hash_unchanged: bool | None = None
            if cell.operation == "read":
                content_hash_unchanged = hashes_before == hashes_after
                if not content_hash_unchanged:
                    raise RuntimeError(f"read cell changed file content: {cell.name}")
            _assert_no_maintenance(before, post_drain)
            delta = writeback_delta(before, post_drain)
            logical = cell.total_bytes
            result = RealWorldCellResult(
                cell=cell,
                fio=fio,
                before=before,
                after_fio=after_fio,
                after_syncfs=after_syncfs,
                post_drain=post_drain,
                writeback=delta,
                allocation_before=allocation_before,
                allocation_after=allocation_after,
                hashes_before=hashes_before,
                hashes_after=hashes_after,
                content_hash_unchanged=content_hash_unchanged,
                foreground_ms=max(
                    1, round((foreground_end - foreground_start) / 1_000_000)
                ),
                local_flush_tail_ms=max(0, round((sync_end - sync_start) / 1_000_000)),
                remote_tail_ms=max(0, round((remote_end - sync_end) / 1_000_000)),
                local_end_to_end_ms=max(
                    1, round((sync_end - foreground_start) / 1_000_000)
                ),
                remote_end_to_end_ms=max(
                    1, round((remote_end - foreground_start) / 1_000_000)
                ),
                local_compression_ratio=(
                    round(delta.local_encoded_bytes / logical, 6)
                    if cell.operation == "write"
                    else None
                ),
                remote_compression_ratio=(
                    round(delta.remote_encoded_bytes / logical, 6)
                    if cell.operation == "write"
                    else None
                ),
            )
        except BaseException as error:
            primary = error
        cleanup_errors: list[BaseException] = []
        if prepared:
            try:
                self._cleanup_root(run_root)
            except BaseException as error:
                cleanup_errors.append(error)
        if cleanup_errors:
            raise RealWorldMatrixCleanupError(primary, cleanup_errors) from primary
        if primary is not None:
            raise primary
        if result is None:
            raise RuntimeError("real-world matrix cell completed without a result")
        return result

    def run(self, *, quick: bool = False) -> RealWorldMatrixResult:
        cells = real_world_cells(quick=quick)
        authority = self._authority()
        scratch_root = self._scratch_root()
        if not scratch_root.is_dir():
            raise RuntimeError(
                f"real-world matrix tmpfs is unavailable: {scratch_root}"
            )
        scratch = Path(
            tempfile.mkdtemp(prefix="zerofs-real-world-matrix-", dir=scratch_root)
        )
        receipt = RunReceipt.start(self.config, "real-world-matrix")
        try:
            with receipt:
                receipt.record("schema", 1)
                receipt.record("quick", quick)
                receipt.record("cell_count", len(cells))
                receipt.record("authority", _jsonable(authority))
                measured: list[object] = []
                for index, cell in enumerate(cells, start=1):
                    stem = f"cell-{index:02d}-{cell.name}"
                    run_root = (
                        self.config.mountpoint
                        / f".zerofs-real-world-{index:02d}-{uuid.uuid4().hex}"
                    )
                    measured.append(
                        self._measure_cell(
                            cell=cell,
                            run_root=run_root,
                            fio_output=scratch / f"{stem}-fio.json",
                            fixture_output=scratch / f"{stem}-fixture.json",
                            pre_stat_output=scratch / f"{stem}-allocation-before.tsv",
                            post_stat_output=scratch / f"{stem}-allocation-after.tsv",
                            pre_hash_output=scratch / f"{stem}-sha256-before.txt",
                            post_hash_output=scratch / f"{stem}-sha256-after.txt",
                            fiemap_output=scratch / f"{stem}-fiemap.txt",
                        )
                    )
                result = RealWorldMatrixResult(
                    quick=quick,
                    authority=authority,
                    cells=tuple(measured),
                    receipt_dir=str(receipt.directory),
                )
                for source in sorted(scratch.iterdir()):
                    if source.is_file():
                        shutil.copy2(source, receipt.path(source.name))
                receipt.path("summary.json").write_text(
                    json.dumps(result.to_dict(), indent=2, sort_keys=True) + "\n",
                    encoding="utf-8",
                )
                receipt.record(
                    "total_measured_bytes", result.to_dict()["total_measured_bytes"]
                )
                receipt.record("cells", result.to_dict()["cells"])
                return result
        finally:
            if scratch.exists():
                shutil.rmtree(scratch)
