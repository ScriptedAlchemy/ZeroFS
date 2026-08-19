from __future__ import annotations

import importlib
import importlib.util
import json
import io
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from subprocess import CompletedProcess
from typing import Mapping, Sequence
from unittest import mock

from scripts.vm100_pilot.metrics import WritebackSnapshot
from scripts.vm100_pilot.config import PilotConfig
from scripts.vm100_pilot.runner import Runner


MODULE = "scripts.vm100_pilot.real_world_matrix"
SPEC = importlib.util.find_spec(MODULE)
matrix_module = importlib.import_module(MODULE) if SPEC is not None else None


class RealWorldMatrixDefinitionTests(unittest.TestCase):
    def test_quick_suite_is_a_bounded_cross_section_and_full_adds_gib_cells(
        self,
    ) -> None:
        self.assertIsNotNone(matrix_module, "real-world matrix module is unavailable")
        quick = matrix_module.real_world_cells(quick=True)
        full = matrix_module.real_world_cells(quick=False)

        self.assertEqual(len(quick), 8)
        self.assertGreater(len(full), len(quick))
        self.assertTrue(
            {cell.name for cell in quick}.issubset({cell.name for cell in full})
        )
        self.assertEqual(
            {cell.file_size_bytes for cell in quick},
            {4 * 1024, 1024 * 1024, 32 * 1024 * 1024},
        )
        self.assertIn(1024 * 1024 * 1024, {cell.file_size_bytes for cell in full})
        self.assertEqual(
            {cell.pattern for cell in full}, {"zero", "repeat", "incompressible"}
        )
        self.assertTrue(
            {"fresh", "sparse", "preallocated", "warm"}.issubset(
                {cell.layout for cell in full}
            )
        )
        self.assertEqual({cell.io_mode for cell in full}, {"buffered", "direct"})
        self.assertEqual({cell.access for cell in full}, {"sequential", "random"})
        self.assertEqual({cell.operation for cell in full}, {"write", "read"})
        self.assertTrue(
            {"cold", "warm", "direct"}.issubset(
                {cell.cache_state for cell in full if cell.operation == "read"}
            )
        )
        self.assertTrue(any(cell.jobs > 1 and cell.queue_depth > 1 for cell in full))

    def test_fio_receipt_requires_exact_bytes_requests_and_zero_errors(self) -> None:
        cell = matrix_module.real_world_cells(quick=True)[1]
        with tempfile.TemporaryDirectory(dir="/var/tmp") as directory:
            path = Path(directory) / "fio.json"
            path.write_text(
                json.dumps(
                    {
                        "jobs": [
                            {
                                "error": 0,
                                "write": {
                                    "io_bytes": cell.total_bytes,
                                    "runtime": 250,
                                    "total_ios": cell.total_bytes
                                    // cell.block_size_bytes,
                                },
                            }
                        ]
                    }
                ),
                encoding="utf-8",
            )
            result = matrix_module.RealWorldFioResult.from_json(
                path,
                operation="write",
                expected_bytes=cell.total_bytes,
                expected_requests=cell.total_bytes // cell.block_size_bytes,
            )

        self.assertEqual(result.bytes, 1024 * 1024)
        self.assertEqual(result.requests, 8)
        self.assertEqual(result.runtime_ms, 250)
        self.assertEqual(result.mibps, 4.0)

    def test_fio_receipt_rejects_short_io_and_counter_regression(self) -> None:
        with tempfile.TemporaryDirectory(dir="/var/tmp") as directory:
            path = Path(directory) / "fio.json"
            path.write_text(
                json.dumps(
                    {
                        "jobs": [
                            {
                                "error": 0,
                                "write": {
                                    "io_bytes": 4096,
                                    "runtime": 1,
                                    "total_ios": 1,
                                },
                            }
                        ]
                    }
                ),
                encoding="utf-8",
            )
            with self.assertRaisesRegex(RuntimeError, "short I/O"):
                matrix_module.RealWorldFioResult.from_json(
                    path,
                    operation="write",
                    expected_bytes=8192,
                    expected_requests=2,
                )
        with self.assertRaisesRegex(RuntimeError, "counter regressed"):
            matrix_module.counter_delta(after=4, before=5, label="accepted")

    def test_fio_command_encodes_geometry_pattern_layout_and_cache_contract(
        self,
    ) -> None:
        cells = {
            cell.name: cell for cell in matrix_module.real_world_cells(quick=False)
        }
        write = matrix_module.build_fio_argv(
            cells["32m-incompressible-preallocated-direct-4j"],
            directory=Path("/mnt/test"),
            output=Path("/tmp/write.json"),
        )
        self.assertIn("--rw=write", write)
        self.assertIn("--direct=1", write)
        self.assertIn("--ioengine=io_uring", write)
        self.assertIn("--iodepth=8", write)
        self.assertIn("--numjobs=4", write)
        self.assertIn(f"--size={32 * 1024 * 1024}", write)
        self.assertIn(f"--filesize={32 * 1024 * 1024}", write)
        self.assertIn("--nrfiles=1", write)
        self.assertIn("--allow_file_create=0", write)
        self.assertIn("--refill_buffers=1", write)
        self.assertIn("--scramble_buffers=1", write)
        self.assertIn("--randrepeat=1", write)

        cold_read = matrix_module.build_fio_argv(
            cells["32m-incompressible-cold-buffered-read"],
            directory=Path("/mnt/test"),
            output=Path("/tmp/read.json"),
        )
        self.assertIn("--rw=read", cold_read)
        self.assertIn("--direct=0", cold_read)
        self.assertIn("--invalidate=1", cold_read)

        random_read = matrix_module.build_fio_argv(
            cells["1g-incompressible-direct-random-read"],
            directory=Path("/mnt/test"),
            output=Path("/tmp/randread.json"),
        )
        self.assertIn("--rw=randread", random_read)
        self.assertIn("--direct=1", random_read)
        self.assertIn("--invalidate=1", random_read)

    def test_digest_manifest_requires_one_valid_digest_per_expected_file(self) -> None:
        first = Path("/mnt/test/data.0.0")
        second = Path("/mnt/test/data.0.1")
        digest_a = "a" * 64
        digest_b = "b" * 64
        parsed = matrix_module.parse_sha256_manifest(
            f"{digest_b}  {second}\n{digest_a}  {first}\n",
            expected_paths=(first, second),
        )
        self.assertEqual(parsed, ((str(first), digest_a), (str(second), digest_b)))

        with self.assertRaisesRegex(RuntimeError, "digest file set mismatch"):
            matrix_module.parse_sha256_manifest(
                f"{digest_a}  {first}\n",
                expected_paths=(first, second),
            )
        with self.assertRaisesRegex(RuntimeError, "invalid SHA-256"):
            matrix_module.parse_sha256_manifest(
                f"not-a-digest  {first}\n{digest_b}  {second}\n",
                expected_paths=(first, second),
            )

    def test_allocation_receipt_proves_logical_size_and_distinguishes_sparse(
        self,
    ) -> None:
        first = Path("/mnt/test/data.0.0")
        second = Path("/mnt/test/data.0.1")
        rows = matrix_module.parse_stat_receipt(
            f"{first}\t1048576\t2048\t512\n{second}\t1048576\t0\t512\n",
            expected_paths=(first, second),
            expected_size=1024 * 1024,
        )
        self.assertEqual(rows[0].allocated_bytes, 1024 * 1024)
        self.assertEqual(rows[1].allocated_bytes, 0)
        self.assertFalse(rows[0].sparse)
        self.assertTrue(rows[1].sparse)

        with self.assertRaisesRegex(RuntimeError, "logical size mismatch"):
            matrix_module.parse_stat_receipt(
                f"{first}\t0\t0\t512\n",
                expected_paths=(first,),
                expected_size=1024 * 1024,
            )

    def test_cell_paths_match_fio_job_and_file_numbering_exactly(self) -> None:
        cell = matrix_module.RealWorldCell(
            "paths",
            "write",
            4096,
            3,
            2,
            "4K",
            4096,
            1,
            "buffered",
            "sequential",
            "zero",
            "fresh",
        )
        self.assertEqual(
            matrix_module.cell_paths(cell, Path("/mnt/run")),
            (
                Path("/mnt/run/data.0.0"),
                Path("/mnt/run/data.0.1"),
                Path("/mnt/run/data.0.2"),
                Path("/mnt/run/data.1.0"),
                Path("/mnt/run/data.1.1"),
                Path("/mnt/run/data.1.2"),
            ),
        )

    def test_layout_validation_rejects_hidden_extension_and_fake_preallocation(
        self,
    ) -> None:
        path = "/mnt/run/data.0.0"
        matrix_module.validate_layout(
            layout="sparse",
            phase="before",
            allocations=(matrix_module.FileAllocation(path, 1024 * 1024, 0),),
            expected_size=1024 * 1024,
        )
        matrix_module.validate_layout(
            layout="preallocated",
            phase="before",
            allocations=(matrix_module.FileAllocation(path, 1024 * 1024, 1024 * 1024),),
            expected_size=1024 * 1024,
        )
        with self.assertRaisesRegex(RuntimeError, "not physically preallocated"):
            matrix_module.validate_layout(
                layout="preallocated",
                phase="before",
                allocations=(matrix_module.FileAllocation(path, 1024 * 1024, 0),),
                expected_size=1024 * 1024,
            )
        with self.assertRaisesRegex(RuntimeError, "post-I/O logical size mismatch"):
            matrix_module.validate_layout(
                layout="fresh",
                phase="after",
                allocations=(matrix_module.FileAllocation(path, 1024 * 1024 + 1, 0),),
                expected_size=1024 * 1024,
            )
        with self.assertRaisesRegex(RuntimeError, "post-I/O allocation mismatch"):
            matrix_module.validate_layout(
                layout="sparse",
                phase="after",
                allocations=(
                    matrix_module.FileAllocation(path, 1024 * 1024, 512 * 1024),
                ),
                expected_size=1024 * 1024,
            )

    def test_writeback_delta_fails_closed_on_any_counter_reset(self) -> None:
        before = WritebackSnapshot(
            accepted=10,
            local=10,
            remote=10,
            dirty_ram=0,
            dirty_ssd_reserved=0,
            local_bytes=100,
            remote_bytes=80,
            terminal=False,
            gc_active=False,
            gc_passes=4,
            gc_batches=1,
            gc_deleted_bytes=2,
        )
        after = WritebackSnapshot(
            accepted=11,
            local=11,
            remote=11,
            dirty_ram=0,
            dirty_ssd_reserved=0,
            local_bytes=120,
            remote_bytes=90,
            terminal=False,
            gc_active=False,
            gc_passes=4,
            gc_batches=1,
            gc_deleted_bytes=2,
        )
        delta = matrix_module.writeback_delta(before, after)
        self.assertEqual(delta.accepted_sequences, 1)
        self.assertEqual(delta.local_encoded_bytes, 20)
        self.assertEqual(delta.remote_encoded_bytes, 10)

        for field in ("accepted", "local", "remote", "local_bytes", "remote_bytes"):
            values = after.to_dict()
            values[field] = before.to_dict()[field] - 1
            regressed = WritebackSnapshot(**values)
            with self.subTest(field=field):
                with self.assertRaisesRegex(RuntimeError, "counter regressed"):
                    matrix_module.writeback_delta(before, regressed)

    def test_read_boundary_rejects_dirty_or_terminal_writeback_state(self) -> None:
        before = WritebackSnapshot(10, 10, 10, 0, 0, 100, 80, False, False, 4, 1, 2)
        matrix_module.require_read_quiet(before, before, phase="read")
        for after in (
            WritebackSnapshot(10, 10, 10, 1, 0, 100, 80, False, False, 4, 1, 2),
            WritebackSnapshot(10, 10, 10, 0, 0, 100, 80, True, False, 4, 1, 2),
        ):
            with self.subTest(after=after):
                with self.assertRaisesRegex(RuntimeError, "read.*not drained"):
                    matrix_module.require_read_quiet(before, after, phase="read")

    def test_syncfs_boundary_requires_local_to_cover_its_full_accepted_cutoff(
        self,
    ) -> None:
        covered = WritebackSnapshot(12, 12, 10, 0, 0, 120, 80, False, False, 4, 1, 2)
        matrix_module.require_local_cutoff(covered, phase="syncfs")
        behind = WritebackSnapshot(12, 11, 10, 0, 0, 120, 80, False, False, 4, 1, 2)
        with self.assertRaisesRegex(RuntimeError, "syncfs.*accepted=12.*local=11"):
            matrix_module.require_local_cutoff(behind, phase="syncfs")

    def test_layout_preparation_uses_real_preallocation_and_preserves_fresh_axis(
        self,
    ) -> None:
        class RecordingRunner(Runner):
            def __init__(self) -> None:
                super().__init__(base_env={})
                self.calls: list[tuple[str, ...]] = []

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
                self.calls.append(args)
                return CompletedProcess(args, 0, "", "")

        cells = {
            cell.name: cell for cell in matrix_module.real_world_cells(quick=False)
        }
        recording = RecordingRunner()
        harness = matrix_module.RealWorldMatrixRunner(
            mock.Mock(), recording, mock.Mock()
        )
        root = Path("/mnt/run")
        harness._prepare_layout(
            cells["32m-incompressible-preallocated-direct-4j"],
            run_root=root,
            fixture_output=Path("/tmp/fixture.json"),
        )
        truncate = [call for call in recording.calls if call[:2] == ("truncate", "-s")]
        fallocate = [
            call for call in recording.calls if call[:2] == ("fallocate", "-l")
        ]
        self.assertEqual(len(truncate), 4)
        self.assertTrue(all(call[2] == "0" for call in truncate))
        self.assertEqual(len(fallocate), 4)
        self.assertTrue(all(call[2] == str(32 * 1024 * 1024) for call in fallocate))

        recording.calls.clear()
        harness._prepare_layout(
            cells["1m-zero-fresh-buffered"],
            run_root=root,
            fixture_output=Path("/tmp/fixture.json"),
        )
        self.assertEqual(
            [call[:3] for call in recording.calls],
            [("truncate", "-s", "0")],
        )

    def test_quick_run_isolates_all_cells_and_persists_machine_readable_summary(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory(dir="/var/tmp") as directory:
            base = Path(directory)
            mount = base / "mount"
            mount.mkdir()
            scratch = base / "scratch"
            scratch.mkdir()
            config = PilotConfig.from_mapping(
                base,
                {
                    "ZEROFS_PILOT_RESULT_DIR": str(base / "results"),
                    "ZEROFS_PROFILE_TARGET_DIR": str(base / "profile"),
                    "ZEROFS_PILOT_MOUNTPOINT": str(mount),
                    "ZEROFS_PILOT_INTEGRITY_FILE": str(mount / "integrity"),
                    "ZEROFS_PILOT_METADATA_DIR": str(mount / "metadata"),
                },
            )
            measured: list[str] = []

            class StubCellResult:
                def __init__(self, cell) -> None:
                    self.cell = cell

                def to_dict(self):
                    return {
                        "cell": {"name": self.cell.name},
                        "fio": {"bytes": self.cell.total_bytes},
                    }

            class ReceiptRunner(matrix_module.RealWorldMatrixRunner):
                def _scratch_root(self) -> Path:
                    return scratch

                def _authority(self):
                    return {
                        "source_commit": "a" * 40,
                        "running_binary_sha256": "b" * 64,
                    }

                def _measure_cell(self, *, cell, **kwargs):
                    measured.append(cell.name)
                    Path(kwargs["fio_output"]).write_text("{}\n", encoding="utf-8")
                    return StubCellResult(cell)

            result = ReceiptRunner(config, mock.Mock(), mock.Mock()).run(quick=True)
            summary = json.loads(
                (Path(result.receipt_dir) / "summary.json").read_text(encoding="utf-8")
            )

        self.assertEqual(
            measured, [cell.name for cell in matrix_module.real_world_cells(quick=True)]
        )
        self.assertEqual(summary["schema"], 1)
        self.assertTrue(summary["quick"])
        self.assertEqual(summary["cell_count"], 8)
        self.assertEqual(
            summary["total_measured_bytes"],
            sum(
                cell.total_bytes for cell in matrix_module.real_world_cells(quick=True)
            ),
        )
        self.assertEqual(summary["authority"]["source_commit"], "a" * 40)

    def test_write_cell_reports_ack_local_and_remote_boundaries_separately(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory(dir="/var/tmp") as directory:
            base = Path(directory)
            mount = base / "mount"
            mount.mkdir()
            config = PilotConfig.from_mapping(
                base,
                {
                    "ZEROFS_PILOT_MOUNTPOINT": str(mount),
                    "ZEROFS_PILOT_RESULT_DIR": str(base / "results"),
                    "ZEROFS_PROFILE_TARGET_DIR": str(base / "profile"),
                    "ZEROFS_PILOT_INTEGRITY_FILE": str(mount / "integrity"),
                    "ZEROFS_PILOT_METADATA_DIR": str(mount / "metadata"),
                },
            )
            cell = matrix_module.real_world_cells(quick=True)[1]
            before = WritebackSnapshot(10, 10, 10, 0, 0, 100, 80, False, False, 4, 1, 2)
            after_fio = WritebackSnapshot(
                11, 10, 10, cell.total_bytes, 0, 100, 80, False, False, 4, 1, 2
            )
            accepted = after_fio
            after_sync = WritebackSnapshot(
                11, 11, 10, 0, 0, 100 + cell.total_bytes // 2, 80, False, False, 4, 1, 2
            )
            post = WritebackSnapshot(
                11,
                11,
                11,
                0,
                0,
                100 + cell.total_bytes // 2,
                80 + cell.total_bytes // 4,
                False,
                False,
                4,
                1,
                2,
            )

            class Metrics:
                def __init__(self) -> None:
                    self.values = iter((after_fio, after_sync, post))

                def snapshot(self):
                    return next(self.values)

            lifecycle = mock.Mock()
            lifecycle.metrics = Metrics()
            runner = mock.Mock()
            allocations = (
                matrix_module.FileAllocation(
                    str(mount / "data.0.0"), cell.file_size_bytes, cell.file_size_bytes
                ),
            )

            class BoundaryHarness(matrix_module.RealWorldMatrixRunner):
                def _prepare_root(self, run_root: Path) -> None:
                    run_root.mkdir()

                def _prepare_layout(self, *args, **kwargs) -> None:
                    pass

                def _stable_boundary(self, *, phase: str):
                    return before

                def _capture_allocation(self, *args, **kwargs):
                    return allocations

                def _capture_hashes(self, *args, **kwargs):
                    return ((str(mount / "data.0.0"), "a" * 64),)

                def _capture_fiemap(self, *args, **kwargs) -> None:
                    pass

                def _run_fio(self, *args, **kwargs):
                    return matrix_module.RealWorldFioResult(
                        cell.total_bytes,
                        250,
                        cell.total_bytes // cell.block_size_bytes,
                        0,
                        4.0,
                        32.0,
                    )

                def _wait_accepted(self, snapshot):
                    return accepted

                def _cleanup_root(self, run_root: Path) -> None:
                    run_root.rmdir()

                def _now_ns(self) -> int:
                    return next(self.times)

            harness = BoundaryHarness(config, runner, lifecycle)
            harness.times = iter(
                (0, 250_000_000, 260_000_000, 360_000_000, 700_000_000)
            )
            root = mount / ".zerofs-real-world-test"
            result = harness._measure_cell(
                cell=cell,
                run_root=root,
                fio_output=base / "fio.json",
                fixture_output=base / "fixture.json",
                pre_stat_output=base / "before.tsv",
                post_stat_output=base / "after.tsv",
                pre_hash_output=base / "before.sha",
                post_hash_output=base / "after.sha",
                fiemap_output=base / "fiemap.txt",
            )

        self.assertEqual(result.foreground_ms, 250)
        self.assertEqual(result.local_flush_tail_ms, 100)
        self.assertEqual(result.remote_tail_ms, 340)
        self.assertEqual(result.remote_end_to_end_ms, 700)
        self.assertEqual(result.writeback.local_encoded_bytes, cell.total_bytes // 2)
        self.assertEqual(result.writeback.remote_encoded_bytes, cell.total_bytes // 4)
        self.assertEqual(result.local_compression_ratio, 0.5)
        self.assertEqual(result.remote_compression_ratio, 0.25)
        runner.run.assert_called_once_with(["sync", "-f", config.mountpoint], sudo=True)
        lifecycle.drain.assert_called_once()

    def test_partial_root_preparation_is_still_cleaned_after_failure(self) -> None:
        with tempfile.TemporaryDirectory(dir="/var/tmp") as directory:
            base = Path(directory)
            mount = base / "mount"
            mount.mkdir()
            config = PilotConfig.from_mapping(
                base,
                {
                    "ZEROFS_PILOT_MOUNTPOINT": str(mount),
                    "ZEROFS_PILOT_RESULT_DIR": str(base / "results"),
                    "ZEROFS_PROFILE_TARGET_DIR": str(base / "profile"),
                    "ZEROFS_PILOT_INTEGRITY_FILE": str(mount / "integrity"),
                    "ZEROFS_PILOT_METADATA_DIR": str(mount / "metadata"),
                },
            )
            root = mount / ".zerofs-real-world-partial"
            cleaned: list[Path] = []

            class PartialFailureHarness(matrix_module.RealWorldMatrixRunner):
                def _prepare_root(self, run_root: Path) -> None:
                    run_root.mkdir()
                    raise RuntimeError("injected preparation failure")

                def _cleanup_root(self, run_root: Path) -> None:
                    cleaned.append(run_root)
                    run_root.rmdir()

            harness = PartialFailureHarness(config, mock.Mock(), mock.Mock())
            with self.assertRaisesRegex(RuntimeError, "injected preparation failure"):
                harness._measure_cell(
                    cell=matrix_module.real_world_cells(quick=True)[0],
                    run_root=root,
                    fio_output=base / "fio.json",
                    fixture_output=base / "fixture.json",
                    pre_stat_output=base / "before.tsv",
                    post_stat_output=base / "after.tsv",
                    pre_hash_output=base / "before.sha",
                    post_hash_output=base / "after.sha",
                    fiemap_output=base / "fiemap.txt",
                )

            self.assertEqual(cleaned, [root])
            self.assertFalse(root.exists())


class RealWorldMatrixCliTests(unittest.TestCase):
    @staticmethod
    def _script_module():
        script = Path(__file__).parents[1] / "vm100-pilot.py"
        spec = importlib.util.spec_from_file_location(
            "vm100_pilot_real_world_cli", script
        )
        if spec is None or spec.loader is None:
            raise AssertionError("unable to load vm100-pilot.py")
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module

    def test_cli_dispatches_quick_and_full_real_world_suites(self) -> None:
        module = self._script_module()
        calls: list[bool] = []

        class FakeRealWorldMatrix:
            def __init__(self, *args: object) -> None:
                pass

            def run(self, *, quick: bool):
                calls.append(quick)
                return {"quick": quick}

        config = mock.Mock()
        config.user = "tester"
        config.group = "tester"
        config.result_dir = "/var/tmp/real-world-matrix-test-results"
        config.metrics_url = "https://127.0.0.1:19567/metrics"
        runner = mock.Mock()
        runner.run.return_value = CompletedProcess(("install",), 0, "", "")
        with (
            mock.patch.object(module, "RealWorldMatrixRunner", FakeRealWorldMatrix),
            redirect_stdout(io.StringIO()),
        ):
            module.dispatch(
                module.build_parser().parse_args(["real-world-matrix", "--quick"]),
                config,
                runner,
            )
            module.dispatch(
                module.build_parser().parse_args(["real-world-matrix"]),
                config,
                runner,
            )

        self.assertEqual(calls, [True, False])


if __name__ == "__main__":
    unittest.main()
