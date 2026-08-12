from __future__ import annotations

import json
import shutil
import tempfile
import unittest
from dataclasses import replace
from pathlib import Path
from subprocess import CompletedProcess
from typing import Any, Sequence

from scripts.vm100_pilot.config import PilotConfig
from scripts.vm100_pilot.benchmark import BenchmarkRunner, calculate_tiers
from scripts.vm100_pilot.lifecycle import PilotLifecycle
from scripts.vm100_pilot.metrics import (
    TerminalWritebackError,
    WritebackSnapshot,
    wait_for_drain,
    wait_for_local,
)
from scripts.vm100_pilot.profile import CanonicalDeployment, ProfileRunner
from scripts.vm100_pilot.raw_sftp import RawSftpRunner, SftpEndpoint
from scripts.vm100_pilot.receipts import RunReceipt
from scripts.vm100_pilot.runner import CommandError
from scripts.vm100_pilot.workloads import WorkloadRunner


class FakeRunner:
    def __init__(self) -> None:
        self.calls: list[tuple[tuple[str, ...], bool]] = []
        self.active: set[str] = set()
        self.fail_start: str | None = None

    def run(
        self,
        argv: list[str | Path] | tuple[str | Path, ...],
        *,
        sudo: bool = False,
        check: bool = True,
        env: dict[str, str] | None = None,
        **_: object,
    ) -> CompletedProcess[str]:
        args = tuple(str(value) for value in argv)
        self.calls.append((args, sudo))
        if args == ("hostname",):
            return CompletedProcess(args, 0, "ubuntu-main\n", "")
        if args[:3] == ("findmnt", "-rn", "-M"):
            return CompletedProcess(args, 1, "", "")
        if args[:2] == ("test", "-e"):
            return CompletedProcess(args, 0 if Path(args[2]).exists() else 1, "", "")
        if args and args[0] == "install":
            if "-d" in args:
                Path(args[-1]).mkdir(parents=True, exist_ok=True)
            else:
                source, destination = Path(args[-2]), Path(args[-1])
                destination.parent.mkdir(parents=True, exist_ok=True)
                shutil.copyfile(source, destination)
            return CompletedProcess(args, 0, "", "")
        if args[:3] == ("rm", "-rf", "--"):
            shutil.rmtree(args[3], ignore_errors=True)
            return CompletedProcess(args, 0, "", "")
        if args[:2] == ("systemctl", "start"):
            unit = args[2]
            if unit == self.fail_start:
                raise CommandError(args, 1, "injected start failure")
            self.active.add(unit)
        elif args[:3] == ("systemctl", "stop", "--no-block"):
            self.active.discard(args[3])
        elif args[:2] == ("systemctl", "is-active"):
            unit = args[2]
            state = "active" if unit in self.active else "inactive"
            return CompletedProcess(
                args, 0 if state == "active" else 3, state + "\n", ""
            )
        elif args[:2] == ("systemctl", "show"):
            unit = args[2]
            active = unit in self.active
            property_name = args[4]
            values = {
                "ActiveState": "active" if active else "inactive",
                "MainPID": "123" if active else "0",
                "ControlPID": "0",
                "ControlGroup": "/fake",
                "NRestarts": "0",
            }
            return CompletedProcess(args, 0, values[property_name] + "\n", "")
        return CompletedProcess(args, 0, "", "")


class CoreTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name) / "repo"
        self.root.mkdir()

    def test_config_rejects_fast_for_results(self) -> None:
        with self.assertRaisesRegex(ValueError, "/fast"):
            PilotConfig.from_mapping(
                self.root,
                {"ZEROFS_PILOT_RESULT_DIR": "/fast/zerofs-results"},
            )

    def test_config_has_complete_command_defaults(self) -> None:
        config = PilotConfig.from_mapping(self.root, {})
        self.assertEqual(config.service, "zerofs-nbd-pilot.service")
        self.assertEqual(config.client_service, "zerofs-nbd-client.service")
        self.assertEqual(config.mountpoint, Path("/mnt/storagebox-nbd-pilot"))
        self.assertEqual(config.expected_ack_mode, "memory")
        self.assertEqual(config.raw_sftp_jobs, 7)

    def test_disposable_path_refuses_root_fast_and_mount_root(self) -> None:
        config = PilotConfig.from_mapping(self.root, {})
        for path in (Path("/"), Path("/fast"), config.mountpoint):
            with self.subTest(path=path):
                with self.assertRaisesRegex(ValueError, "unsafe disposable path"):
                    config.require_disposable(path)

    def test_receipt_survives_failure(self) -> None:
        result_dir = Path(self.temp.name) / "results"
        config = PilotConfig.from_mapping(
            self.root,
            {"ZEROFS_PILOT_RESULT_DIR": str(result_dir)},
        )
        receipt = RunReceipt.start(config, "benchmark")
        with self.assertRaisesRegex(RuntimeError, "boom"):
            with receipt:
                receipt.record("phase", "write")
                raise RuntimeError("boom")
        payload = json.loads(receipt.manifest.read_text())
        self.assertEqual(payload["status"], "failed")
        self.assertEqual(payload["phase"], "write")
        self.assertIn("boom", payload["error"])


class LifecycleTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        root = Path(self.temp.name) / "repo"
        root.mkdir()
        base = PilotConfig.from_mapping(
            root,
            {
                "ZEROFS_PILOT_RESULT_DIR": str(Path(self.temp.name) / "results"),
                "ZEROFS_PROFILE_TARGET_DIR": str(Path(self.temp.name) / "profile"),
                "ZEROFS_PILOT_CGROUP_ROOT": str(Path(self.temp.name) / "cgroup"),
            },
        )
        self.config = replace(base, stop_timeout=1)
        self.runner = FakeRunner()
        self.lifecycle = PilotLifecycle(self.config, self.runner)  # type: ignore[arg-type]

    def test_snapshot_parser_requires_every_durability_field(self) -> None:
        snapshot = WritebackSnapshot.parse(
            "\n".join(
                (
                    "zerofs_writeback_accepted_sequence 9",
                    "zerofs_writeback_local_sequence 8",
                    "zerofs_writeback_remote_sequence 7",
                    "zerofs_writeback_dirty_ram_bytes 6",
                    "zerofs_writeback_dirty_ssd_bytes 5",
                    "zerofs_writeback_local_bytes_completed_total 4",
                    "zerofs_writeback_remote_bytes_completed_total 3",
                    "zerofs_writeback_terminal_error 0",
                )
            )
        )
        self.assertEqual(snapshot.accepted, 9)
        self.assertEqual(snapshot.remote, 7)
        self.assertFalse(snapshot.drained)
        with self.assertRaisesRegex(ValueError, "missing"):
            WritebackSnapshot.parse("zerofs_writeback_accepted_sequence 1\n")

    def test_drain_fails_before_sleep_on_terminal_error(self) -> None:
        terminal = WritebackSnapshot(9, 9, 8, 0, 1, 1, 1, True)
        sleeps: list[float] = []
        with self.assertRaisesRegex(TerminalWritebackError, "terminal"):
            wait_for_drain(
                lambda: terminal,
                timeout=10,
                sleep=lambda delay: sleeps.append(delay),
            )
        self.assertEqual(sleeps, [])

    def test_local_barrier_waits_for_the_captured_accepted_sequence(self) -> None:
        snapshots = iter(
            (
                WritebackSnapshot(12, 10, 9, 1, 1, 10, 9, False),
                WritebackSnapshot(12, 11, 9, 1, 1, 11, 9, False),
                WritebackSnapshot(13, 12, 9, 1, 1, 12, 9, False),
            )
        )
        result = wait_for_local(
            lambda: next(snapshots),
            target_sequence=12,
            timeout=1,
            sleep=lambda _: None,
        )
        self.assertEqual(result.local, 12)
        self.assertEqual(result.accepted, 13)

    def test_stop_is_dependency_ordered(self) -> None:
        self.runner.active.update(
            (self.config.service, self.config.client_service, self.config.mount_unit)
        )
        self.lifecycle.stop()
        stops = [
            call[0][3]
            for call in self.runner.calls
            if call[0][:3] == ("systemctl", "stop", "--no-block")
        ]
        self.assertEqual(
            stops,
            [self.config.mount_unit, self.config.client_service, self.config.service],
        )

    def test_start_unwinds_only_started_layers(self) -> None:
        self.runner.fail_start = self.config.client_service
        with self.assertRaisesRegex(CommandError, "injected start failure"):
            self.lifecycle.start()
        stops = [
            call[0][3]
            for call in self.runner.calls
            if call[0][:3] == ("systemctl", "stop", "--no-block")
        ]
        self.assertEqual(stops, [self.config.service])
        self.assertNotIn(self.config.service, self.runner.active)


class _StaticMetrics:
    def __init__(self, snapshot: WritebackSnapshot) -> None:
        self.value = snapshot

    def snapshot(self) -> WritebackSnapshot:
        return self.value


class _HealthyLifecycle:
    def __init__(self, snapshot: WritebackSnapshot) -> None:
        self.metrics = _StaticMetrics(snapshot)
        self.drain_calls = 0
        self.start_calls = 0
        self.stop_calls = 0

    def status(self, *, validate_data: bool = True) -> dict[str, object]:
        return {"healthy": True, "deployed_commit": "test"}

    def drain(self, timeout: int | None = None) -> object:
        self.drain_calls += 1
        return object()

    def stop(self) -> None:
        self.stop_calls += 1

    def start(self) -> dict[str, int]:
        self.start_calls += 1
        return {"restarts": 0}


class BenchmarkTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
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
            },
        )
        self.snapshot = WritebackSnapshot(9, 9, 9, 0, 0, 1 << 20, 1 << 20, False)
        self.runner = FakeRunner()
        self.lifecycle = _HealthyLifecycle(self.snapshot)

    def test_local_rate_uses_completed_payload_and_full_interval(self) -> None:
        result = calculate_tiers(
            logical_bytes=1 << 30,
            local_bytes=1 << 30,
            remote_bytes=1 << 30,
            foreground_ms=1000,
            local_end_to_end_ms=4000,
            remote_end_to_end_ms=10000,
            buffered_read_ms=2000,
            direct_read_ms=500,
        )
        self.assertEqual(result.foreground_mibps, 1024.0)
        self.assertEqual(result.local_mibps, 256.0)
        self.assertEqual(result.remote_mibps, 102.4)
        self.assertEqual(result.direct_read_mibps, 2048.0)

    def test_prepare_root_uses_explicit_owner(self) -> None:
        benchmark = BenchmarkRunner(self.config, self.runner, self.lifecycle)  # type: ignore[arg-type]
        run_root = self.config.mountpoint / ".zerofs-bench-test"
        benchmark.prepare_root(run_root)
        self.assertIn(
            (
                (
                    "install",
                    "-d",
                    "-m",
                    "0755",
                    "-o",
                    self.config.user,
                    "-g",
                    self.config.group,
                    str(run_root),
                ),
                True,
            ),
            self.runner.calls,
        )

    def test_failed_fio_cleans_scoped_root_and_preserves_receipt(self) -> None:
        class FailingBenchmark(BenchmarkRunner):
            def _run_fio(self, *args: object, **kwargs: object) -> None:
                raise CommandError(("fio",), 19, "injected fio failure")

        benchmark = FailingBenchmark(self.config, self.runner, self.lifecycle)  # type: ignore[arg-type]
        with self.assertRaisesRegex(CommandError, "injected fio failure"):
            benchmark.run(total_mib=4, jobs=1)
        rm_calls = [
            call for call in self.runner.calls if call[0][:3] == ("rm", "-rf", "--")
        ]
        self.assertEqual(len(rm_calls), 1)
        manifests = list(self.config.result_dir.glob("benchmark-*/manifest.json"))
        self.assertEqual(len(manifests), 1)
        self.assertEqual(json.loads(manifests[0].read_text())["status"], "failed")


class ProfileTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        root = Path(self.temp.name) / "repo"
        (root / "zerofs").mkdir(parents=True)
        mount = Path(self.temp.name) / "mount"
        mount.mkdir()
        base = PilotConfig.from_mapping(
            root,
            {
                "ZEROFS_PILOT_RESULT_DIR": str(Path(self.temp.name) / "results"),
                "ZEROFS_PROFILE_TARGET_DIR": str(
                    Path(self.temp.name) / "profile-target"
                ),
                "ZEROFS_PILOT_MOUNTPOINT": str(mount),
                "ZEROFS_PILOT_INTEGRITY_FILE": str(mount / "integrity"),
                "ZEROFS_PILOT_METADATA_DIR": str(mount / "metadata"),
            },
        )
        self.binary = Path(self.temp.name) / "bin" / "zerofs"
        self.receipt_file = Path(self.temp.name) / "bin" / "zerofs.receipt"
        self.binary.parent.mkdir()
        self.binary.write_bytes(b"canonical-binary")
        self.receipt_file.write_text("commit=canonical\nbinary_sha256=old\n")
        self.config = replace(base, binary=self.binary, build_receipt=self.receipt_file)
        self.runner = FakeRunner()
        self.snapshot = WritebackSnapshot(9, 9, 9, 0, 0, 1, 1, False)
        self.lifecycle = _HealthyLifecycle(self.snapshot)

    def test_canonical_restore_reinstalls_binary_and_receipt(self) -> None:
        snapshot = CanonicalDeployment.capture(self.config, self.runner)  # type: ignore[arg-type]
        self.binary.write_bytes(b"profile-binary")
        self.receipt_file.write_text("commit=profile\n")
        snapshot.restore()
        self.assertEqual(self.binary.read_bytes(), b"canonical-binary")
        self.assertEqual(
            self.receipt_file.read_text(), "commit=canonical\nbinary_sha256=old\n"
        )

    def test_profile_failure_restores_canonical_deployment(self) -> None:
        class FailingBenchmark:
            def run(self, *, total_mib: int, jobs: int) -> object:
                raise CommandError(("fio",), 19, "injected profile benchmark failure")

        class TestProfile(ProfileRunner):
            def _build_profile(self) -> Path:
                binary = self.config.profile_target / "release" / "zerofs"
                binary.parent.mkdir(parents=True)
                binary.write_bytes(b"profile-binary")
                return binary

            def _start_collectors(self, pid: int, receipt: RunReceipt) -> Any:
                return type("Collectors", (), {"stop": lambda _self: None})()

            def _service_pid(self) -> int:
                return 123

        profiler = TestProfile(
            self.config,
            self.runner,  # type: ignore[arg-type]
            self.lifecycle,  # type: ignore[arg-type]
            FailingBenchmark(),  # type: ignore[arg-type]
        )
        with self.assertRaisesRegex(CommandError, "injected profile benchmark failure"):
            profiler.run(total_mib=4, jobs=1)
        self.assertEqual(self.binary.read_bytes(), b"canonical-binary")
        self.assertEqual(
            self.receipt_file.read_text(), "commit=canonical\nbinary_sha256=old\n"
        )
        self.assertGreaterEqual(self.lifecycle.stop_calls, 2)
        self.assertGreaterEqual(self.lifecycle.start_calls, 2)
        self.assertFalse(self.config.profile_target.exists())

    def test_profile_install_failure_restores_canonical_deployment(self) -> None:
        class TestProfile(ProfileRunner):
            def _build_profile(self) -> Path:
                binary = self.config.profile_target / "release" / "zerofs"
                binary.parent.mkdir(parents=True)
                binary.write_bytes(b"profile-binary")
                return binary

            def _install_profile(self, binary: Path) -> None:
                self.config.binary.write_bytes(b"partial-profile-install")
                raise RuntimeError("injected install failure")

        profiler = TestProfile(
            self.config,
            self.runner,  # type: ignore[arg-type]
            self.lifecycle,  # type: ignore[arg-type]
        )
        with self.assertRaisesRegex(RuntimeError, "injected install failure"):
            profiler.run(total_mib=4, jobs=1)
        self.assertEqual(self.binary.read_bytes(), b"canonical-binary")
        self.assertGreaterEqual(self.lifecycle.start_calls, 1)
        self.assertFalse(self.config.profile_target.exists())


class WorkloadEngineTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
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
                "ZEROFS_PILOT_TMP_DIR": str(Path(self.temp.name) / "tmp"),
                "ZEROFS_PILOT_MOUNTPOINT": str(mount),
                "ZEROFS_PILOT_INTEGRITY_FILE": str(mount / "integrity"),
                "ZEROFS_PILOT_METADATA_DIR": str(mount / "metadata"),
            },
        )
        self.config.temp_dir.mkdir()
        snapshot = WritebackSnapshot(9, 9, 9, 0, 0, 1, 1, False)
        self.lifecycle = _HealthyLifecycle(snapshot)
        self.runner = FakeRunner()

    def test_workload_root_is_created_with_explicit_owner(self) -> None:
        workload = WorkloadRunner(
            self.config, self.runner, self.lifecycle  # type: ignore[arg-type]
        )
        root = self.config.mountpoint / ".zerofs-workloads-test"
        workload._prepare_root(root)
        self.assertTrue(root.is_dir())
        argv = self.runner.calls[-1][0]
        self.assertEqual(argv[argv.index("-o") + 1], self.config.user)

    def test_parallel_delete_removes_every_child(self) -> None:
        workload = WorkloadRunner(
            self.config, self.runner, self.lifecycle  # type: ignore[arg-type]
        )
        directory = self.config.mountpoint / "node_modules"
        for index in range(8):
            child = directory / f"package-{index}"
            child.mkdir(parents=True)
            (child / "index.js").write_text("module.exports = 1\n")
        workload._parallel_delete(directory, 4)
        self.assertFalse(directory.exists())

    def test_raw_failure_restores_stack_and_removes_scratch(self) -> None:
        class FailingRaw(RawSftpRunner):
            def _endpoint(self) -> SftpEndpoint:
                return SftpEndpoint(
                    "user", "example.invalid", 23, Path("/key"), Path("/known")
                )

            def _run_batch(
                self, *args: object, **kwargs: object
            ) -> CompletedProcess[str]:
                return CompletedProcess(("sftp",), 0, "", "")

            def _parallel_batches(self, *args: object, **kwargs: object) -> None:
                raise RuntimeError("injected raw transfer failure")

        raw = FailingRaw(self.config, self.runner, self.lifecycle)  # type: ignore[arg-type]
        with self.assertRaisesRegex(RuntimeError, "injected raw transfer failure"):
            raw.run(jobs=2, per_job_mib=1)
        self.assertEqual(self.lifecycle.stop_calls, 1)
        self.assertEqual(self.lifecycle.start_calls, 1)
        self.assertEqual(list(self.config.temp_dir.iterdir()), [])

    def test_raw_endpoint_reads_the_storage_section(self) -> None:
        class ConfigRunner(FakeRunner):
            def run(
                self, argv: Sequence[str | Path], **kwargs: Any
            ) -> CompletedProcess[str]:
                args = tuple(str(value) for value in argv)
                if args[:1] == ("cat",):
                    return CompletedProcess(
                        args,
                        0,
                        """
[storage]
url = "sftp://alice@example.invalid:23/prefix"
[sftp]
identity_file = "/root/.ssh/id"
known_hosts = "/root/.ssh/known"
""",
                        "",
                    )
                return super().run(args, **kwargs)

        raw = RawSftpRunner(
            self.config, ConfigRunner(), self.lifecycle  # type: ignore[arg-type]
        )
        endpoint = raw._endpoint()
        self.assertEqual(endpoint.user, "alice")
        self.assertEqual(endpoint.host, "example.invalid")
        self.assertEqual(endpoint.port, 23)


if __name__ == "__main__":
    unittest.main()
