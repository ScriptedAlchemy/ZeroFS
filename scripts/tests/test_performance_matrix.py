from __future__ import annotations

import json
import csv
import shutil
import tempfile
import unittest
import importlib.util
import io
import subprocess
import sys
from contextlib import redirect_stdout
from dataclasses import replace
from pathlib import Path
from subprocess import CompletedProcess
from typing import Any, Mapping, Sequence
from unittest import mock

from scripts.vm100_pilot.config import PilotConfig
from scripts.vm100_pilot.metrics import WritebackSnapshot
from scripts.vm100_pilot.runner import CommandError, Runner
from scripts.vm100_pilot.system_io import (
    BlockIoSnapshot,
    SystemIoSnapshot,
    SystemIoSummary,
)
import scripts.vm100_pilot.performance_matrix as matrix_module

MatrixFioResult = matrix_module.MatrixFioResult
matrix_cells = matrix_module.matrix_cells
PerformanceMatrixRunner = getattr(matrix_module, "PerformanceMatrixRunner", None)
BlockIoDelta = getattr(matrix_module, "BlockIoDelta", None)
MatrixCellResult = getattr(matrix_module, "MatrixCellResult", None)
RemoteCrossing = getattr(matrix_module, "RemoteCrossing", None)
require_drained = getattr(matrix_module, "require_drained", None)


class PerformanceMatrixContractTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory(dir="/var/tmp")
        self.addCleanup(self.temp.cleanup)

    def test_full_and_quick_matrix_have_literal_controlled_cells(self) -> None:
        self.assertIsNotNone(matrix_cells, "performance matrix module is unavailable")

        full = tuple(
            (cell.block_size, cell.block_size_bytes, cell.jobs)
            for cell in matrix_cells(quick=False)  # type: ignore[misc]
        )
        self.assertEqual(
            full,
            tuple(
                (block_size, block_bytes, jobs)
                for block_size, block_bytes in (
                    ("32K", 32 * 1024),
                    ("128K", 128 * 1024),
                    ("256K", 256 * 1024),
                    ("1M", 1024 * 1024),
                    ("4M", 4 * 1024 * 1024),
                )
                for jobs in (1, 4, 8)
            ),
        )
        quick = tuple(
            (cell.block_size, cell.jobs)
            for cell in matrix_cells(quick=True)  # type: ignore[misc]
        )
        self.assertEqual(quick, (("32K", 1), ("1M", 4), ("4M", 8)))

    def test_fio_receipt_sums_exact_job_counters_and_derives_request_rate(self) -> None:
        self.assertIsNotNone(
            MatrixFioResult, "performance matrix module is unavailable"
        )
        output = Path(self.temp.name) / "fio.json"
        output.write_text(
            json.dumps(
                {
                    "jobs": [
                        {
                            "error": 0,
                            "write": {
                                "io_bytes": 3 * 1024 * 1024,
                                "runtime": 1500,
                                "total_ios": 96,
                            },
                        },
                        {
                            "error": 0,
                            "write": {
                                "io_bytes": 5 * 1024 * 1024,
                                "runtime": 2000,
                                "total_ios": 160,
                            },
                        },
                    ]
                }
            ),
            encoding="utf-8",
        )

        result = MatrixFioResult.from_json(output)  # type: ignore[union-attr]

        self.assertEqual(result.bytes, 8 * 1024 * 1024)
        self.assertEqual(result.runtime_ms, 2000)
        self.assertEqual(result.requests, 256)
        self.assertEqual(result.errors, 0)
        self.assertEqual(result.requests_per_second, 128.0)
        self.assertEqual(result.mibps, 4.0)

    def test_fio_receipt_rejects_missing_exact_request_counter(self) -> None:
        self.assertIsNotNone(
            MatrixFioResult, "performance matrix module is unavailable"
        )
        output = Path(self.temp.name) / "fio.json"
        output.write_text(
            json.dumps(
                {
                    "jobs": [
                        {"write": {"io_bytes": 1024, "runtime": 1}},
                    ]
                }
            ),
            encoding="utf-8",
        )

        with self.assertRaisesRegex(ValueError, "total_ios"):
            MatrixFioResult.from_json(output)  # type: ignore[union-attr]

    def test_fio_receipt_rejects_nonzero_errors(self) -> None:
        output = Path(self.temp.name) / "fio.json"
        output.write_text(
            json.dumps(
                {
                    "jobs": [
                        {
                            "error": 1,
                            "write": {
                                "io_bytes": 1024,
                                "runtime": 1,
                                "total_ios": 1,
                            },
                        }
                    ]
                }
            ),
            encoding="utf-8",
        )
        with self.assertRaisesRegex(RuntimeError, "fio.*errors=1"):
            MatrixFioResult.from_json(output)

    def test_cell_boundary_requires_equal_sequences_and_zero_dirty_bytes(self) -> None:
        self.assertIsNotNone(require_drained, "drained boundary check is unavailable")
        drained = WritebackSnapshot(9, 9, 9, 0, 0, 1, 1, False, False, 4, 8, 16)
        require_drained(drained, phase="pre-cell")  # type: ignore[misc]

        contaminated = (
            replace(drained, local=8),
            replace(drained, remote=8),
            replace(drained, dirty_ram=1),
            replace(drained, dirty_ssd_reserved=1),
        )
        for snapshot in contaminated:
            with self.subTest(snapshot=snapshot):
                with self.assertRaisesRegex(RuntimeError, "pre-cell.*not drained"):
                    require_drained(snapshot, phase="pre-cell")  # type: ignore[misc]


class _SequenceMetrics:
    def __init__(self, snapshots: Sequence[WritebackSnapshot]) -> None:
        self.snapshots = iter(snapshots)

    def snapshot(self) -> WritebackSnapshot:
        return next(self.snapshots)


class _MatrixLifecycle:
    def __init__(
        self,
        config: PilotConfig,
        snapshots: Sequence[WritebackSnapshot],
        status: Mapping[str, object] | None = None,
    ) -> None:
        self.config = config
        self.metrics = _SequenceMetrics(snapshots)
        self.drain_calls = 0
        self.status_result = dict(status or {"healthy": True})

    def status(self) -> dict[str, object]:
        return self.status_result

    def drain(self, timeout: int | None = None) -> object:
        self.drain_calls += 1
        return {"timeout": timeout}


class _FioRunner(Runner):
    def __init__(self) -> None:
        super().__init__(base_env={})
        self.calls: list[tuple[tuple[str, ...], bool]] = []

    def run(
        self,
        argv: Sequence[str | Path],
        *,
        sudo: bool = False,
        timeout: float | None = None,
        capture: bool = True,
        check: bool = True,
        cwd: Path | None = None,
        env: Mapping[str, str] | None = None,
        input_text: str | None = None,
    ) -> CompletedProcess[str]:
        args = tuple(str(value) for value in argv)
        self.calls.append((args, sudo))
        if args[0] == "fio":
            output = Path(
                next(
                    value.split("=", 1)[1]
                    for value in args
                    if value.startswith("--output=")
                )
            )
            output.write_text(
                json.dumps(
                    {
                        "jobs": [
                            {
                                "error": 0,
                                "write": {
                                    "io_bytes": 32 * 1024 * 1024,
                                    "runtime": 200,
                                    "total_ios": 32,
                                },
                            },
                        ]
                    }
                ),
                encoding="utf-8",
            )
        return CompletedProcess(args, 0, "", "")


class _RemoteSampler:
    def __init__(self, remote: WritebackSnapshot, io_sample: SystemIoSnapshot) -> None:
        self.remote = remote
        self.system_io = [io_sample]
        self.system_io_rows = [(700, *io_sample.to_dict().values())]
        self.targets: list[tuple[int, float]] = []

    def wait_for_remote(
        self, target_sequence: int, timeout: float
    ) -> tuple[WritebackSnapshot, int]:
        self.targets.append((target_sequence, timeout))
        return self.remote, 700_000_000


class PerformanceMatrixCellTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory(dir="/var/tmp")
        self.addCleanup(self.temp.cleanup)
        root = Path(self.temp.name) / "repo"
        root.mkdir()
        mount = Path(self.temp.name) / "mount"
        mount.mkdir()
        self.config = PilotConfig.from_mapping(
            root,
            {
                "ZEROFS_PILOT_RESULT_DIR": str(Path(self.temp.name) / "results"),
                "ZEROFS_PROFILE_TARGET_DIR": str(Path(self.temp.name) / "profile"),
                "ZEROFS_PILOT_MOUNTPOINT": str(mount),
                "ZEROFS_PILOT_INTEGRITY_FILE": str(mount / "integrity"),
                "ZEROFS_PILOT_METADATA_DIR": str(mount / "metadata"),
                "ZEROFS_PILOT_DRAIN_TIMEOUT": "5",
            },
        )

    def test_cell_uses_exact_direct_fio_and_records_durability_boundaries(self) -> None:
        self.assertIsNotNone(
            PerformanceMatrixRunner, "performance matrix runner is unavailable"
        )
        before = WritebackSnapshot(
            9, 9, 9, 0, 0, 1 << 20, 1 << 20, False, False, 4, 8, 16
        )
        after_fio = replace(before, accepted=10, dirty_ram=32 << 20)
        accepted = replace(
            after_fio,
            local=10,
            dirty_ram=0,
            dirty_ssd_reserved=32 << 20,
            local_bytes=33 << 20,
        )
        after_syncfs = accepted
        post_drain = replace(
            accepted,
            remote=10,
            dirty_ssd_reserved=0,
            remote_bytes=33 << 20,
        )
        lifecycle = _MatrixLifecycle(
            self.config,
            (before, after_fio, accepted, after_syncfs, post_drain),
        )
        runner = _FioRunner()
        zero_io = SystemIoSnapshot("sda1", 100, 200, 10, 0.1, 0.0, 1_000, 100)
        final_io = SystemIoSnapshot(
            "sda1", 612, 2_097_352, 210, 2.5, 1.5, 31_000, 10_100
        )
        crossing_io = replace(
            final_io,
            some_avg10=3.0,
            full_avg10=1.75,
        )
        contaminated_io = replace(
            crossing_io,
            root_write_bytes=64 << 20,
            root_busy_ms=20_000,
            some_avg10=99.0,
            full_avg10=88.0,
            some_total_us=900_000,
            full_total_us=800_000,
        )
        sampler = _RemoteSampler(post_drain, crossing_io)
        sampler.system_io.append(contaminated_io)
        sampler.system_io_rows.append((900, *contaminated_io.to_dict().values()))
        nbd_before = BlockIoSnapshot("nbd0", 100, 200, 5)
        nbd_after = BlockIoSnapshot("nbd0", 100, (32 << 20) + 200, 405)

        class DeterministicMatrixRunner(PerformanceMatrixRunner):  # type: ignore[misc,valid-type]
            def _local_device(self) -> tuple[int, int]:
                return (8, 1)

            def _system_io(self, device: tuple[int, int]) -> SystemIoSnapshot:
                self.assert_device = device
                return (zero_io, final_io)[self.system_io_calls()]

            def system_io_calls(self) -> int:
                count = getattr(self, "_system_io_count", 0)
                self._system_io_count = count + 1
                return count

            def _nbd_io(self) -> BlockIoSnapshot:
                count = getattr(self, "_nbd_io_count", 0)
                self._nbd_io_count = count + 1
                return (nbd_before, nbd_after)[count]

            def _monotonic_ns(self) -> int:
                values = (100_000_000, 300_000_000, 350_000_000, 550_000_000)
                count = getattr(self, "_clock_count", 0)
                self._clock_count = count + 1
                return values[count]

        matrix = DeterministicMatrixRunner(
            self.config,
            runner,
            lifecycle,  # type: ignore[arg-type]
        )
        run_root = self.config.mountpoint / ".zerofs-matrix-test"
        run_root.mkdir()
        output = Path(self.temp.name) / "fio.json"

        try:
            result = matrix._run_cell(
                cell=matrix_cells(quick=True)[1],
                total_mib=32,
                run_root=run_root,
                fio_output=output,
                sampler=sampler,
            )
        except RuntimeError as error:
            self.fail(f"group-reported fio aggregate was rejected: {error}")

        fio = next(call[0] for call in runner.calls if call[0][0] == "fio")
        self.assertIn("--bs=1M", fio)
        self.assertIn("--size=8388608", fio)
        self.assertIn("--numjobs=4", fio)
        self.assertIn("--direct=1", fio)
        self.assertIn("--iodepth=1", fio)
        self.assertIn("--ioengine=psync", fio)
        fio_call = next(call for call in runner.calls if call[0][0] == "fio")
        self.assertTrue(fio_call[1])
        self.assertEqual(result.fio.bytes, 32 << 20)
        self.assertEqual(result.fio.runtime_ms, 200)
        self.assertEqual(result.fio.requests, 32)
        self.assertEqual(result.syncfs_local_tail_ms, 200)
        self.assertEqual(result.remote_crossing.target_sequence, 10)
        self.assertEqual(result.remote_crossing.timestamp_ns, 700_000_000)
        self.assertEqual(result.nbd_io.write_bytes, 32 << 20)
        self.assertEqual(result.system_io.some_stall_ms, 30.0)
        self.assertEqual(result.system_io.full_stall_ms, 10.0)
        self.assertEqual(result.system_io.peak_some_avg10, 3.0)
        self.assertEqual(result.system_io.peak_full_avg10, 1.75)
        self.assertEqual(result.before.accepted, 9)
        self.assertEqual(result.after_fio.accepted, 10)
        self.assertEqual(result.after_syncfs.local, 10)
        self.assertEqual(result.post_drain.remote, 10)
        self.assertEqual(sampler.targets, [(10, 5)])
        self.assertEqual(lifecycle.drain_calls, 1)


class _FilesystemRunner(_FioRunner):
    def run(
        self,
        argv: Sequence[str | Path],
        **kwargs: Any,
    ) -> CompletedProcess[str]:
        args = tuple(str(value) for value in argv)
        if args[:2] == ("install", "-d"):
            self.calls.append((args, bool(kwargs.get("sudo", False))))
            Path(args[-1]).mkdir(parents=True, exist_ok=True)
            return CompletedProcess(args, 0, "", "")
        if args[:3] == ("rm", "-rf", "--"):
            self.calls.append((args, bool(kwargs.get("sudo", False))))
            shutil.rmtree(args[3], ignore_errors=True)
            return CompletedProcess(args, 0, "", "")
        if args[:2] == ("test", "-e"):
            self.calls.append((args, bool(kwargs.get("sudo", False))))
            exists = Path(args[2]).exists()
            return CompletedProcess(args, 0 if exists else 1, "", "")
        return super().run(argv, **kwargs)


class _StaticSampler:
    def __init__(
        self,
        lifecycle: object,
        output: Path,
        system_io_output: Path,
        local_device: tuple[int, int],
    ) -> None:
        self.output = output
        self.system_io_output = system_io_output
        self.system_io: list[SystemIoSnapshot] = []

    def start(self) -> None:
        return None

    def stop(self) -> None:
        self.output.write_text("timestamp_ms,accepted\n100,9\n", encoding="utf-8")
        self.system_io_output.write_text(
            "timestamp_ms,root_device\n100,sda1\n", encoding="utf-8"
        )


class PerformanceMatrixOrchestrationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory(dir="/var/tmp")
        self.addCleanup(self.temp.cleanup)
        root = Path(self.temp.name) / "repo"
        root.mkdir()
        self.mount = Path(self.temp.name) / "mount"
        self.mount.mkdir()
        self.scratch = Path(self.temp.name) / "shm"
        self.scratch.mkdir()
        self.config = PilotConfig.from_mapping(
            root,
            {
                "ZEROFS_PILOT_RESULT_DIR": str(Path(self.temp.name) / "results"),
                "ZEROFS_PROFILE_TARGET_DIR": str(Path(self.temp.name) / "profile"),
                "ZEROFS_PILOT_MOUNTPOINT": str(self.mount),
                "ZEROFS_PILOT_INTEGRITY_FILE": str(self.mount / "integrity"),
                "ZEROFS_PILOT_METADATA_DIR": str(self.mount / "metadata"),
            },
        )
        self.snapshot = WritebackSnapshot(
            9, 9, 9, 0, 0, 1 << 20, 1 << 20, False, False, 4, 8, 16
        )

    def _result(self, cell: Any) -> Any:
        fio = MatrixFioResult(
            bytes=32 << 20,
            runtime_ms=200,
            requests=(32 << 20) // cell.block_size_bytes,
            errors=0,
            requests_per_second=160.0,
            mibps=160.0,
        )
        zero_io = SystemIoSummary("sda1", 0.0, 0.0, 0.0, 1.0, 20, 10.0, 1.0, 0.5)
        return MatrixCellResult(
            cell=cell,
            total_bytes=32 << 20,
            fio=fio,
            before=self.snapshot,
            after_fio=replace(self.snapshot, accepted=10),
            accepted=replace(self.snapshot, accepted=10, local=10),
            after_syncfs=replace(self.snapshot, accepted=10, local=10),
            remote_crossing=RemoteCrossing(
                10,
                700_000_000,
                replace(self.snapshot, accepted=10, local=10, remote=10),
            ),
            post_drain=replace(self.snapshot, accepted=10, local=10, remote=10),
            write_start_ns=100_000_000,
            write_end_ns=300_000_000,
            syncfs_start_ns=350_000_000,
            syncfs_end_ns=550_000_000,
            syncfs_local_tail_ms=200,
            remote_end_to_end_ms=600,
            remote_tail_after_syncfs_ms=150,
            nbd_io=BlockIoDelta("nbd0", 0, 32 << 20, 400),
            system_io=zero_io,
        )

    def test_authority_records_exact_commit_config_hash_and_devices(self) -> None:
        self.assertTrue(
            hasattr(PerformanceMatrixRunner, "_authority"),
            "matrix authority receipt is unavailable",
        )
        config_file = Path(self.temp.name) / "pilot.toml"
        config_file.write_text("[filesystem]\nmax_size_gb = 64\n", encoding="utf-8")
        config = replace(self.config, config_file=config_file)

        class AuthorityRunner(_FilesystemRunner):
            def run(
                self, argv: Sequence[str | Path], **kwargs: Any
            ) -> CompletedProcess[str]:
                args = tuple(str(value) for value in argv)
                self.calls.append((args, bool(kwargs.get("sudo", False))))
                if args[:4] == ("git", "-C", str(config.root), "rev-parse"):
                    return CompletedProcess(args, 0, "a" * 40 + "\n", "")
                if args[:4] == ("git", "-C", str(config.root), "status"):
                    return CompletedProcess(args, 0, "", "")
                if args[:1] == ("sha256sum",):
                    return CompletedProcess(
                        args, 0, "b" * 64 + "  " + args[1] + "\n", ""
                    )
                return super().run(argv, **kwargs)

        class AuthorityMatrix(PerformanceMatrixRunner):  # type: ignore[misc,valid-type]
            def _nbd_device(self) -> tuple[int, int, str]:
                return (43, 0, "nbd0")

            def _local_device_identity(self) -> tuple[int, int, str]:
                return (8, 1, "sda1")

        authority = AuthorityMatrix(
            config,
            AuthorityRunner(),
            _MatrixLifecycle(
                config,
                (self.snapshot,),
                status={
                    "healthy": True,
                    "deployed_commit": "a" * 40,
                    "running_binary_sha256": "c" * 64,
                    "config_sha256": "b" * 64,
                },
            ),  # type: ignore[arg-type]
        )._authority()

        self.assertEqual(authority.source_commit, "a" * 40)
        self.assertFalse(authority.source_dirty)
        self.assertEqual(authority.config_file, str(config_file))
        self.assertEqual(authority.config_sha256, "b" * 64)
        self.assertEqual(authority.nbd_device_path, "/dev/nbd0")
        self.assertEqual(authority.nbd_device_major, 43)
        self.assertEqual(authority.nbd_device_minor, 0)
        self.assertEqual(authority.nbd_device_name, "nbd0")
        self.assertEqual(authority.local_device_major, 8)
        self.assertEqual(authority.local_device_minor, 1)
        self.assertEqual(authority.local_device_name, "sda1")
        self.assertEqual(authority.deployed_commit, "a" * 40)
        self.assertEqual(authority.running_binary_sha256, "c" * 64)
        self.assertEqual(authority.lifecycle_config_sha256, "b" * 64)

    def test_authority_rejects_checkout_that_does_not_match_deployed_commit(
        self,
    ) -> None:
        config_file = Path(self.temp.name) / "pilot.toml"
        config_file.write_text("[filesystem]\nmax_size_gb = 64\n", encoding="utf-8")
        config = replace(self.config, config_file=config_file)

        class AuthorityRunner(_FilesystemRunner):
            def run(
                self, argv: Sequence[str | Path], **kwargs: Any
            ) -> CompletedProcess[str]:
                args = tuple(str(value) for value in argv)
                if args[:4] == ("git", "-C", str(config.root), "rev-parse"):
                    return CompletedProcess(args, 0, "a" * 40 + "\n", "")
                if args[:4] == ("git", "-C", str(config.root), "status"):
                    return CompletedProcess(args, 0, "", "")
                if args[:1] == ("sha256sum",):
                    return CompletedProcess(args, 0, "b" * 64 + "  config\n", "")
                return super().run(argv, **kwargs)

        lifecycle = _MatrixLifecycle(
            config,
            (self.snapshot,),
            status={
                "healthy": True,
                "deployed_commit": "d" * 40,
                "running_binary_sha256": "c" * 64,
                "config_sha256": "b" * 64,
            },
        )

        class MismatchAuthorityMatrix(
            PerformanceMatrixRunner  # type: ignore[misc,valid-type]
        ):
            def _nbd_device(self) -> tuple[int, int, str]:
                return (43, 0, "nbd0")

            def _local_device_identity(self) -> tuple[int, int, str]:
                return (8, 1, "sda1")

        with self.assertRaisesRegex(
            RuntimeError, "checkout commit.*does not match deployed commit"
        ):
            MismatchAuthorityMatrix(
                config, AuthorityRunner(), lifecycle  # type: ignore[arg-type]
            )._authority()

    def test_quick_run_isolates_cells_and_persists_json_csv_after_measurement(
        self,
    ) -> None:
        self.assertTrue(
            hasattr(PerformanceMatrixRunner, "run"),
            "matrix orchestration is unavailable",
        )
        roots: list[Path] = []
        lifecycle = _MatrixLifecycle(
            self.config,
            (self.snapshot,) * 20,
        )
        runner = _FilesystemRunner()
        outer = self

        class ReceiptMatrixRunner(PerformanceMatrixRunner):  # type: ignore[misc,valid-type]
            def _scratch_root(self) -> Path:
                return outer.scratch

            def _local_device(self) -> tuple[int, int]:
                return (8, 1)

            def _authority(self) -> Any:
                return matrix_module.RunAuthority(
                    source_commit="a" * 40,
                    source_dirty=False,
                    config_file="/etc/zerofs/nbd-pilot.toml",
                    config_sha256="b" * 64,
                    nbd_device_path="/dev/nbd0",
                    nbd_device_major=43,
                    nbd_device_minor=0,
                    nbd_device_name="nbd0",
                    local_device_major=8,
                    local_device_minor=1,
                    local_device_name="sda1",
                    deployed_commit="a" * 40,
                    running_binary_sha256="c" * 64,
                    lifecycle_config_sha256="b" * 64,
                )

            def _run_cell(self, **kwargs: Any) -> Any:
                run_root = Path(kwargs["run_root"])
                roots.append(run_root)
                self.assert_no_receipt_artifacts_yet()
                Path(kwargs["fio_output"]).write_text("{}\n", encoding="utf-8")
                return outer._result(kwargs["cell"])

            def assert_no_receipt_artifacts_yet(self) -> None:
                receipts = list(self.config.result_dir.glob("performance-matrix-*"))
                if len(receipts) != 1:
                    raise AssertionError(receipts)
                names = {path.name for path in receipts[0].iterdir()}
                if names != {"manifest.json"}:
                    raise AssertionError(names)

        with mock.patch(
            "scripts.vm100_pilot.performance_matrix._MetricSampler", _StaticSampler
        ):
            result = ReceiptMatrixRunner(
                self.config,
                runner,
                lifecycle,  # type: ignore[arg-type]
            ).run(total_mib=32, quick=True)

        receipt = Path(result.receipt_dir)
        manifest = json.loads((receipt / "manifest.json").read_text())
        summary = json.loads((receipt / "summary.json").read_text())
        with (receipt / "cells.csv").open(newline="", encoding="utf-8") as handle:
            rows = list(csv.DictReader(handle))
        self.assertEqual(len(roots), 3)
        self.assertEqual(len(set(roots)), 3)
        self.assertTrue(all(root.parent == self.mount for root in roots))
        self.assertTrue(all(root.name.startswith(".zerofs-matrix-") for root in roots))
        self.assertTrue(all(not root.exists() for root in roots))
        self.assertEqual(list(self.scratch.iterdir()), [])
        self.assertEqual(manifest["status"], "ok")
        self.assertEqual(summary["schema"], 1)
        self.assertEqual(summary["authority"]["source_commit"], "a" * 40)
        self.assertEqual(summary["authority"]["deployed_commit"], "a" * 40)
        self.assertEqual(summary["authority"]["running_binary_sha256"], "c" * 64)
        self.assertEqual(manifest["authority"]["config_sha256"], "b" * 64)
        self.assertEqual(summary["cell_count"], 3)
        self.assertEqual(len(summary["cells"]), 3)
        self.assertEqual(len(rows), 3)
        self.assertEqual(rows[1]["block_size"], "1M")
        self.assertEqual(rows[1]["jobs"], "4")
        self.assertEqual(rows[1]["fio_bytes"], str(32 << 20))
        self.assertEqual(rows[1]["fio_errors"], "0")
        self.assertEqual(rows[1]["syncfs_local_tail_ms"], "200")
        self.assertEqual(rows[1]["remote_target_sequence"], "10")
        self.assertEqual(len(list(receipt.glob("*-fio.json"))), 3)
        self.assertEqual(len(list(receipt.glob("*-writeback.csv"))), 3)
        self.assertEqual(len(list(receipt.glob("*-system-io.csv"))), 3)
        self.assertGreaterEqual(lifecycle.drain_calls, 3)

    def test_cell_sampler_starts_only_after_pre_cell_drain_and_syncfs(self) -> None:
        events: list[str] = []
        outer = self

        class EventLifecycle(_MatrixLifecycle):
            def drain(self, timeout: int | None = None) -> object:
                events.append("drain")
                return super().drain(timeout)

        class EventRunner(_FilesystemRunner):
            def run(
                self, argv: Sequence[str | Path], **kwargs: Any
            ) -> CompletedProcess[str]:
                args = tuple(str(value) for value in argv)
                if args[:2] == ("sync", "-f"):
                    events.append("syncfs")
                return super().run(argv, **kwargs)

        class EventSampler(_StaticSampler):
            def start(self) -> None:
                events.append("sampler-start")

            def stop(self) -> None:
                events.append("sampler-stop")
                super().stop()

        class EventMatrixRunner(PerformanceMatrixRunner):  # type: ignore[misc,valid-type]
            def _local_device(self) -> tuple[int, int]:
                return (8, 1)

            def _run_cell(self, **kwargs: Any) -> Any:
                Path(kwargs["fio_output"]).write_text("{}\n", encoding="utf-8")
                return outer._result(kwargs["cell"])

        lifecycle = EventLifecycle(self.config, (self.snapshot,) * 5)
        run_root = self.mount / ".zerofs-matrix-boundary"
        with mock.patch(
            "scripts.vm100_pilot.performance_matrix._MetricSampler", EventSampler
        ):
            EventMatrixRunner(
                self.config,
                EventRunner(),
                lifecycle,  # type: ignore[arg-type]
            )._measure_cell(
                cell=matrix_cells(quick=True)[0],
                total_mib=32,
                run_root=run_root,
                fio_output=self.scratch / "fio.json",
                metrics_output=self.scratch / "metrics.csv",
                system_io_output=self.scratch / "system.csv",
            )

        self.assertEqual(events[:3], ["drain", "syncfs", "sampler-start"])
        self.assertLess(events.index("sampler-stop"), len(events) - 2)

    def test_failed_cell_removes_only_scoped_root_and_preserves_failed_manifest(
        self,
    ) -> None:
        self.assertTrue(
            hasattr(PerformanceMatrixRunner, "run"),
            "matrix orchestration is unavailable",
        )
        roots: list[Path] = []
        lifecycle = _MatrixLifecycle(self.config, (self.snapshot,) * 10)
        runner = _FilesystemRunner()
        outer = self

        class FailingMatrixRunner(PerformanceMatrixRunner):  # type: ignore[misc,valid-type]
            def _scratch_root(self) -> Path:
                return outer.scratch

            def _local_device(self) -> tuple[int, int]:
                return (8, 1)

            def _authority(self) -> Any:
                return matrix_module.RunAuthority(
                    "a" * 40,
                    False,
                    "/etc/zerofs/nbd-pilot.toml",
                    "b" * 64,
                    "/dev/nbd0",
                    43,
                    0,
                    "nbd0",
                    8,
                    1,
                    "sda1",
                    "a" * 40,
                    "c" * 64,
                    "b" * 64,
                )

            def _run_cell(self, **kwargs: Any) -> Any:
                roots.append(Path(kwargs["run_root"]))
                raise RuntimeError("injected matrix failure")

        with (
            mock.patch(
                "scripts.vm100_pilot.performance_matrix._MetricSampler", _StaticSampler
            ),
            self.assertRaisesRegex(RuntimeError, "injected matrix failure"),
        ):
            FailingMatrixRunner(
                self.config,
                runner,
                lifecycle,  # type: ignore[arg-type]
            ).run(total_mib=32, quick=True)

        self.assertEqual(len(roots), 1)
        self.assertFalse(roots[0].exists())
        self.assertTrue(self.mount.is_dir())
        self.assertEqual(list(self.scratch.iterdir()), [])
        manifests = list(
            self.config.result_dir.glob("performance-matrix-*/manifest.json")
        )
        self.assertEqual(len(manifests), 1)
        payload = json.loads(manifests[0].read_text())
        self.assertEqual(payload["status"], "failed")
        self.assertIn("injected matrix failure", payload["error"])

    def test_pre_measurement_failure_still_removes_the_scoped_root(self) -> None:
        roots: list[Path] = []
        lifecycle = _MatrixLifecycle(self.config, (self.snapshot,) * 10)
        outer = self

        class FailFirstSyncRunner(_FilesystemRunner):
            def __init__(self) -> None:
                super().__init__()
                self.failed = False

            def run(
                self, argv: Sequence[str | Path], **kwargs: Any
            ) -> CompletedProcess[str]:
                args = tuple(str(value) for value in argv)
                if args[:2] == ("sync", "-f") and not self.failed:
                    self.failed = True
                    raise CommandError(args, 19, "injected pre-cell sync failure")
                return super().run(argv, **kwargs)

        class PreflightFailMatrix(PerformanceMatrixRunner):  # type: ignore[misc,valid-type]
            def _scratch_root(self) -> Path:
                return outer.scratch

            def _local_device(self) -> tuple[int, int]:
                return (8, 1)

            def _authority(self) -> Any:
                return matrix_module.RunAuthority(
                    "a" * 40,
                    False,
                    "/etc/zerofs/nbd-pilot.toml",
                    "b" * 64,
                    "/dev/nbd0",
                    43,
                    0,
                    "nbd0",
                    8,
                    1,
                    "sda1",
                    "a" * 40,
                    "c" * 64,
                    "b" * 64,
                )

            def _prepare_root(self, run_root: Path) -> None:
                roots.append(run_root)
                super()._prepare_root(run_root)

        with self.assertRaisesRegex(CommandError, "injected pre-cell sync failure"):
            PreflightFailMatrix(
                self.config,
                FailFirstSyncRunner(),
                lifecycle,  # type: ignore[arg-type]
            ).run(total_mib=32, quick=True)

        self.assertEqual(len(roots), 1)
        self.assertFalse(roots[0].exists())
        self.assertTrue(self.mount.is_dir())
        self.assertEqual(list(self.scratch.iterdir()), [])

    def test_scratch_cleanup_failure_marks_the_manifest_failed(self) -> None:
        lifecycle = _MatrixLifecycle(self.config, (self.snapshot,) * 20)
        outer = self

        class ReceiptMatrixRunner(PerformanceMatrixRunner):  # type: ignore[misc,valid-type]
            def _scratch_root(self) -> Path:
                return outer.scratch

            def _local_device(self) -> tuple[int, int]:
                return (8, 1)

            def _authority(self) -> Any:
                return matrix_module.RunAuthority(
                    "a" * 40,
                    False,
                    "/etc/pilot.toml",
                    "b" * 64,
                    "/dev/nbd0",
                    43,
                    0,
                    "nbd0",
                    8,
                    1,
                    "sda1",
                    "a" * 40,
                    "c" * 64,
                    "b" * 64,
                )

            def _run_cell(self, **kwargs: Any) -> Any:
                Path(kwargs["fio_output"]).write_text("{}\n", encoding="utf-8")
                return outer._result(kwargs["cell"])

        real_rmtree = shutil.rmtree

        def fail_matrix_scratch(path: str | Path, *args: Any, **kwargs: Any) -> None:
            if Path(path).parent == self.scratch:
                raise OSError("injected scratch cleanup failure")
            real_rmtree(path, *args, **kwargs)

        with (
            mock.patch(
                "scripts.vm100_pilot.performance_matrix._MetricSampler", _StaticSampler
            ),
            mock.patch(
                "scripts.vm100_pilot.performance_matrix.shutil.rmtree",
                side_effect=fail_matrix_scratch,
            ),
            self.assertRaisesRegex(OSError, "scratch cleanup failure"),
        ):
            ReceiptMatrixRunner(
                self.config, _FilesystemRunner(), lifecycle  # type: ignore[arg-type]
            ).run(total_mib=32, quick=True)

        manifests = list(
            self.config.result_dir.glob("performance-matrix-*/manifest.json")
        )
        self.assertEqual(len(manifests), 1)
        self.assertEqual(json.loads(manifests[0].read_text())["status"], "failed")


class PerformanceMatrixCliTests(unittest.TestCase):
    @staticmethod
    def _script_module() -> Any:
        script = Path(__file__).parents[1] / "vm100-pilot.py"
        spec = importlib.util.spec_from_file_location("vm100_pilot_matrix_cli", script)
        if spec is None or spec.loader is None:
            raise AssertionError("unable to load vm100-pilot.py")
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module

    def test_cli_exposes_one_performance_matrix_command(self) -> None:
        script = Path(__file__).parents[1] / "vm100-pilot.py"
        completed = subprocess.run(
            [sys.executable, script, "performance-matrix", "--help"],
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=False,
        )

        self.assertEqual(completed.returncode, 0, completed.stderr)
        self.assertIn("--quick", completed.stdout)
        self.assertIn("--total-mib", completed.stdout)

    def test_cli_uses_small_quick_default_and_honors_explicit_total(self) -> None:
        module = self._script_module()
        self.assertTrue(
            hasattr(module, "PerformanceMatrixRunner"),
            "CLI matrix runner is unavailable",
        )
        calls: list[tuple[int, bool]] = []

        class FakeMatrix:
            def __init__(self, *args: object) -> None:
                return None

            def run(self, *, total_mib: int, quick: bool) -> dict[str, object]:
                calls.append((total_mib, quick))
                return {"total_mib": total_mib, "quick": quick}

        config = mock.Mock()
        config.user = "tester"
        config.group = "tester"
        config.result_dir = "/var/tmp/matrix-test-results"
        runner = mock.Mock()
        runner.run.return_value = CompletedProcess(("install",), 0, "", "")
        with (
            mock.patch.object(module, "PerformanceMatrixRunner", FakeMatrix),
            redirect_stdout(io.StringIO()),
        ):
            module.dispatch(
                module.build_parser().parse_args(["performance-matrix", "--quick"]),
                config,
                runner,
            )
            module.dispatch(
                module.build_parser().parse_args(
                    ["performance-matrix", "--total-mib", "64"]
                ),
                config,
                runner,
            )

        self.assertEqual(calls, [(32, True), (64, False)])


if __name__ == "__main__":
    unittest.main()
