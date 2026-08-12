from __future__ import annotations

import argparse
import hashlib
import importlib.util
import io
import json
import os
import signal
import subprocess
import shutil
import sys
import tempfile
import tomllib
import unittest
from unittest import mock
from contextlib import contextmanager, redirect_stdout
from dataclasses import replace
from pathlib import Path
from subprocess import CompletedProcess
from typing import Any, Mapping, Sequence

from scripts.vm100_pilot.config import PilotConfig
from scripts.vm100_pilot.benchmark import (
    BenchmarkResult,
    BenchmarkRunner,
    FioResult,
    _active_windows,
    _monotonic_ms,
    _validate_fio_bytes,
    calculate_tiers,
)
from scripts.vm100_pilot.lifecycle import PilotLifecycle
from scripts.vm100_pilot.migration import (
    LocalExportNamespace,
    StripedMigrator,
    rewrite_toml_number,
    swap_exports,
)
from scripts.vm100_pilot.reset import FreshResetter, rewrite_storage_prefix
from scripts.vm100_pilot.metrics import (
    TerminalWritebackError,
    WritebackSnapshot,
    wait_for_accepted_after,
    wait_for_drain,
    wait_for_gc_quiescence,
    wait_for_local,
)
from scripts.vm100_pilot.profile import (
    CanonicalDeployment,
    ProfileRunner,
    _load_phase_windows,
    _phase_report_text,
    _phase_perf_report_argv,
    _perf_record_argv,
    _require_perf_data,
)
from scripts.vm100_pilot.system_io import (
    BlockIoSnapshot,
    SystemIoSnapshot,
    filesystem_device,
    summarize_system_io,
    verify_page_cache_hit,
)
from scripts.vm100_pilot.raw_sftp import RawSftpRunner, SftpEndpoint
from scripts.vm100_pilot.receipts import RunReceipt
from scripts.vm100_pilot.runner import CommandError, ManagedProcess, Runner
from scripts.vm100_pilot.workloads import WorkloadRunner
import scripts.vm100_pilot.profile as profile_module


class FakeRunner(Runner):
    def __init__(self) -> None:
        super().__init__(base_env={})
        self.calls: list[tuple[tuple[str, ...], bool]] = []
        self.active: set[str] = set()
        self.fail_start: str | None = None
        self.max_write_zeroes_sectors = "0"

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
        if args == ("hostname",):
            return CompletedProcess(args, 0, "ubuntu-main\n", "")
        if args[:3] == ("findmnt", "-rn", "-M"):
            return CompletedProcess(args, 1, "", "")
        if args[:2] == ("test", "-e"):
            return CompletedProcess(args, 0 if Path(args[2]).exists() else 1, "", "")
        if (
            args[:1] == ("cat",)
            and args[1].startswith("/sys/block/nbd")
            and args[1].endswith("/queue/max_write_zeroes_sectors")
        ):
            return CompletedProcess(args, 0, self.max_write_zeroes_sectors + "\n", "")
        if args[:1] == ("cat",) and Path(args[1]).is_file():
            return CompletedProcess(args, 0, Path(args[1]).read_text(), "")
        if args[:1] == ("sha256sum",) and Path(args[1]).is_file():
            digest = hashlib.sha256(Path(args[1]).read_bytes()).hexdigest()
            return CompletedProcess(args, 0, f"{digest}  {args[1]}\n", "")
        if args[:2] in {("cp", "-a"), ("cp", "-aL")} and Path(args[-2]).exists():
            shutil.copy2(args[-2], args[-1])
            return CompletedProcess(args, 0, "", "")
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
        if args[:3] == ("rm", "-f", "--"):
            Path(args[3]).unlink(missing_ok=True)
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
        self.temp = tempfile.TemporaryDirectory(dir="/var/tmp")
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name) / "repo"
        self.root.mkdir()

    def test_config_rejects_fast_for_results(self) -> None:
        with self.assertRaisesRegex(ValueError, "/fast"):
            PilotConfig.from_mapping(
                self.root,
                {"ZEROFS_PILOT_RESULT_DIR": "/fast/zerofs-results"},
            )
        with self.assertRaisesRegex(ValueError, "/fast"):
            PilotConfig.from_mapping(
                self.root,
                {"ZEROFS_PILOT_ADMIN_MOUNTPOINT": "/fast/zerofs-admin"},
            )

    def test_config_rejects_unsafe_ephemeral_targets(self) -> None:
        mountpoint = "/mnt/storagebox-nbd-pilot"
        unsafe = (
            "/etc/zerofs-work",
            "/usr/local/zerofs-work",
            "relative/zerofs-work",
            "/fast/zerofs-work",
            mountpoint,
            "/var/lib/unrelated-zerofs-work",
        )
        variables = (
            "ZEROFS_PILOT_RESULT_DIR",
            "ZEROFS_PILOT_TMP_DIR",
            "ZEROFS_BUILD_TARGET_DIR",
            "ZEROFS_PROFILE_TARGET_DIR",
        )
        for variable in variables:
            for path in unsafe:
                with self.subTest(variable=variable, path=path):
                    with self.assertRaisesRegex(ValueError, "unsafe .* path"):
                        PilotConfig.from_mapping(self.root, {variable: path})

    def test_config_rejects_non_nbd_and_aliased_device_targets(self) -> None:
        for variable in (
            "ZEROFS_PILOT_NBD_DEVICE",
            "ZEROFS_PILOT_MIGRATION_DEVICE",
        ):
            for path in ("/dev/sda", "/dev/mapper/root", "dev/nbd7"):
                with self.subTest(variable=variable, path=path):
                    with self.assertRaisesRegex(ValueError, "NBD device"):
                        PilotConfig.from_mapping(self.root, {variable: path})

        with self.assertRaisesRegex(ValueError, "must be distinct"):
            PilotConfig.from_mapping(
                self.root,
                {
                    "ZEROFS_PILOT_NBD_DEVICE": "/dev/nbd7",
                    "ZEROFS_PILOT_MIGRATION_DEVICE": "/dev/nbd7",
                },
            )

    def test_config_accepts_distinct_configured_nbd_devices(self) -> None:
        config = PilotConfig.from_mapping(
            self.root,
            {
                "ZEROFS_PILOT_NBD_DEVICE": "/dev/nbd7",
                "ZEROFS_PILOT_MIGRATION_DEVICE": "/dev/nbd8",
            },
        )
        self.assertEqual(config.nbd_device, Path("/dev/nbd7"))
        self.assertEqual(config.migration_device, Path("/dev/nbd8"))

    def test_config_rejects_state_roots_outside_exact_pilot_parent(self) -> None:
        for path in (
            "/var/lib/zerofs",
            "/var/lib/other/nbd-pilot",
            "/var/lib/zerofs/nested/nbd-pilot",
            "/etc/zerofs/nbd-pilot",
            "var/lib/zerofs/nbd-pilot",
        ):
            with self.subTest(path=path):
                with self.assertRaisesRegex(ValueError, "pilot state root"):
                    PilotConfig.from_mapping(
                        self.root,
                        {"ZEROFS_PILOT_STATE_ROOT": path},
                    )

    def test_config_has_complete_command_defaults(self) -> None:
        config = PilotConfig.from_mapping(self.root, {})
        self.assertEqual(config.service, "zerofs-nbd-pilot.service")
        self.assertEqual(config.client_service, "zerofs-nbd-client.service")
        self.assertEqual(config.mountpoint, Path("/mnt/storagebox-nbd-pilot"))
        self.assertEqual(config.expected_ack_mode, "memory")
        self.assertEqual(config.raw_sftp_jobs, 7)
        self.assertEqual(config.build_target, Path("/var/tmp/zerofs-build-target"))
        self.assertNotIn(Path("/fast"), config.build_target.parents)
        self.assertEqual(config.nbd_export, "vm100-pilot-64g")
        self.assertEqual(config.replacement_export, "vm100-pilot-64g-v3")
        self.assertEqual(config.nbd_size_gib, 64)
        self.assertEqual(config.nbd_stripe_lanes, 4)
        self.assertEqual(config.nbd_stripe_kib, 256)
        self.assertEqual(config.migration_device, Path("/dev/nbd1"))
        self.assertEqual(config.nbd_socket, Path("/run/zerofs-nbd-pilot/nbd.sock"))
        self.assertEqual(config.nbd_device, Path("/dev/nbd0"))
        self.assertEqual(config.pilot_state_root, Path("/var/lib/zerofs/nbd-pilot"))
        self.assertEqual(config.ninep_target, "unix:/run/zerofs-nbd-pilot/9p.sock")
        self.assertEqual(config.migration_mountpoint, Path("/mnt/zerofs-nbd-migration"))
        self.assertEqual(config.admin_mountpoint, Path("/mnt/zerofs-admin"))
        self.assertEqual(config.temporary_max_size_gib, 256)
        self.assertEqual(config.maintenance_isolation_secs, 3600)

    def test_profile_maintenance_isolation_has_a_finite_supported_bound(self) -> None:
        for value in (299, 86401):
            with self.subTest(value=value):
                with self.assertRaisesRegex(ValueError, "between 300 and 86400"):
                    PilotConfig.from_mapping(
                        self.root,
                        {"ZEROFS_PROFILE_MAINTENANCE_ISOLATION_SECS": str(value)},
                    )

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

    def test_cli_exposes_the_guarded_striped_migration(self) -> None:
        script = Path(__file__).parents[1] / "vm100-pilot.py"
        result = subprocess.run(
            [sys.executable, script, "migrate-striped", "--help"],
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=False,
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("--replacement-export", result.stdout)
        self.assertIn("--temporary-max-size-gib", result.stdout)

    def test_cli_exposes_an_explicitly_confirmed_fresh_reset(self) -> None:
        script = Path(__file__).parents[1] / "vm100-pilot.py"
        result = subprocess.run(
            [sys.executable, script, "reset-fresh", "--help"],
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=False,
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("--remote-prefix", result.stdout)
        self.assertIn("--confirm-destroy-pilot", result.stdout)

    def test_fresh_reset_rewrites_only_the_storage_prefix(self) -> None:
        source = """[storage]
url = "sftp://pilot@example.test:23/old-prefix"
encryption_password = "${ZEROFS_PASSWORD}"

[filesystem]
max_size_gb = 128
"""
        rewritten = rewrite_storage_prefix(source, "new-prefix")
        self.assertIn('url = "sftp://pilot@example.test:23/new-prefix"', rewritten)
        self.assertIn('encryption_password = "${ZEROFS_PASSWORD}"', rewritten)
        self.assertIn("max_size_gb = 128", rewritten)

    def test_fresh_reset_rejects_a_nested_or_unchanged_remote_prefix(self) -> None:
        source = '[storage]\nurl = "sftp://pilot@example.test:23/current"\n'
        for prefix in ("current", "nested/path", "../escape"):
            with self.subTest(prefix=prefix):
                with self.assertRaises(ValueError):
                    rewrite_storage_prefix(source, prefix)

    def test_interrupt_child_waits_for_the_supervisor_to_reap_it(self) -> None:
        marker = Path(self.temp.name) / "child-interrupted"
        child = Path(self.temp.name) / "profile-child.py"
        parent = Path(self.temp.name) / "profile-supervisor.py"
        child.write_text(
            "import pathlib, signal, sys\n"
            "marker = pathlib.Path(sys.argv[1])\n"
            "def interrupted(_signum, _frame):\n"
            "    marker.write_text('1')\n"
            "    raise SystemExit(0)\n"
            "signal.signal(signal.SIGINT, interrupted)\n"
            "print('ready', flush=True)\n"
            "signal.pause()\n",
            encoding="utf-8",
        )
        parent.write_text(
            "import subprocess, sys\n"
            "child = subprocess.Popen(\n"
            "    [sys.executable, sys.argv[1], sys.argv[2]],\n"
            "    stdout=subprocess.PIPE, text=True,\n"
            ")\n"
            "assert child.stdout is not None\n"
            "assert child.stdout.readline().strip() == 'ready'\n"
            "print('ready', flush=True)\n"
            "raise SystemExit(child.wait())\n",
            encoding="utf-8",
        )
        process = subprocess.Popen(
            [sys.executable, parent, child, marker],
            stdout=subprocess.PIPE,
            text=True,
            start_new_session=True,
        )
        stdout = process.stdout
        self.assertIsNotNone(stdout)
        assert stdout is not None
        self.assertEqual(stdout.readline().strip(), "ready")

        ManagedProcess(process, ("sudo", "perf")).interrupt_child(
            lambda pid: os.kill(pid, signal.SIGINT), timeout=2
        )
        stdout.close()

        self.assertEqual(marker.read_text(), "1")

    def test_export_swap_restores_the_predecessor_when_validation_fails(self) -> None:
        exports = Path(self.temp.name) / "exports"
        exports.mkdir()
        (exports / "canonical").write_bytes(b"predecessor")
        (exports / "replacement").mkdir()
        (exports / "replacement" / "lane-0").write_bytes(b"replacement")
        namespace = LocalExportNamespace(exports)

        with self.assertRaisesRegex(RuntimeError, "injected validation failure"):
            swap_exports(
                namespace,
                canonical="canonical",
                replacement="replacement",
                backup="zerofs-migration-predecessor",
                validate=lambda: (_ for _ in ()).throw(
                    RuntimeError("injected validation failure")
                ),
            )

        self.assertEqual((exports / "canonical").read_bytes(), b"predecessor")
        self.assertEqual(
            (exports / "replacement" / "lane-0").read_bytes(), b"replacement"
        )
        self.assertFalse((exports / "zerofs-migration-predecessor").exists())

    def test_quota_rewrite_changes_only_filesystem_max_size(self) -> None:
        source = """\
[filesystem]
max_size_gb = 128.0

[cache]
max_size_gb = 64.0
"""
        self.assertEqual(
            rewrite_toml_number(source, "filesystem", "max_size_gb", 256),
            """\
[filesystem]
max_size_gb = 256

[cache]
max_size_gb = 64.0
""",
        )


class LifecycleTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory(dir="/var/tmp")
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
        self.lifecycle = PilotLifecycle(self.config, self.runner)

    def _deployment_scenario(
        self,
        *,
        fail_new_start: bool = False,
        fail_new_status: bool = False,
        rollback_failures: frozenset[str] = frozenset(),
    ) -> tuple[
        PilotConfig,
        FakeRunner,
        PilotLifecycle,
        list[str],
        tuple[Any, argparse.Namespace],
    ]:
        build_target = Path(self.temp.name) / "build-target"
        built = build_target / "release" / "zerofs"
        built.parent.mkdir(parents=True)
        built.write_bytes(b"replacement-binary")
        binary = Path(self.temp.name) / "bin" / "zerofs"
        receipt = binary.with_suffix(".build-receipt")
        binary.parent.mkdir()
        binary.write_bytes(b"predecessor-binary")
        receipt.write_bytes(b"commit=predecessor\nbinary_sha256=predecessor\n")
        temp_dir = Path(self.temp.name) / "deploy-temp"
        temp_dir.mkdir()
        config = replace(
            self.config,
            build_target=build_target,
            binary=binary,
            build_receipt=receipt,
            temp_dir=temp_dir,
        )
        events: list[str] = []

        class DeploymentRunner(FakeRunner):
            def run(
                self, argv: Sequence[str | Path], **kwargs: Any
            ) -> CompletedProcess[str]:
                args = tuple(str(value) for value in argv)
                if args == ("git", "rev-parse", "HEAD"):
                    self.calls.append((args, bool(kwargs.get("sudo", False))))
                    return CompletedProcess(args, 0, "replacement-commit\n", "")
                if args and args[0] == "install" and "-d" not in args:
                    source = Path(args[-2])
                    destination = Path(args[-1])
                    if source.parent.name.startswith("zerofs-deploy-backup-"):
                        operation = (
                            "restore-binary"
                            if destination == config.binary
                            else "restore-receipt"
                        )
                        events.append(operation)
                        if operation in rollback_failures:
                            self.calls.append((args, bool(kwargs.get("sudo", False))))
                            raise RuntimeError(f"injected {operation} failure")
                return super().run(args, **kwargs)

        class TransactionLifecycle(PilotLifecycle):
            def __init__(self, runner: DeploymentRunner) -> None:
                super().__init__(config, runner)  # type: ignore[arg-type]
                self.stop_calls = 0
                self.rollback_started = False
                self.start_requirements: list[bool] = []

            def stop(self) -> None:
                self.stop_calls += 1
                if self.stop_calls == 2:
                    self.rollback_started = True
                    events.append("rollback-stop")
                    if "stop" in rollback_failures:
                        raise RuntimeError("injected rollback-stop failure")
                else:
                    events.append("deployment-stop")

            def start(
                self, *, require_write_zeroes_disabled: bool = True
            ) -> dict[str, int]:
                self.start_requirements.append(require_write_zeroes_disabled)
                phase = "rollback" if self.rollback_started else "replacement"
                events.append(f"{phase}-start")
                if phase == "replacement" and fail_new_start:
                    raise RuntimeError("injected replacement-start failure")
                if phase == "rollback" and "start" in rollback_failures:
                    raise RuntimeError("injected rollback-start failure")
                return {"restarts": 0}

            def status(self, *, validate_data: bool = True) -> dict[str, object]:
                phase = "rollback" if self.rollback_started else "replacement"
                events.append(f"{phase}-status")
                if phase == "replacement" and fail_new_status:
                    raise RuntimeError("injected replacement-status failure")
                if phase == "rollback" and "status" in rollback_failures:
                    raise RuntimeError("injected rollback-status failure")
                return {"healthy": True, "deployment": phase}

            def _sha256(self, path: Path, *, sudo: bool = False) -> str:
                return self._local_sha256(path)

        runner = DeploymentRunner()
        lifecycle = TransactionLifecycle(runner)
        script = Path(__file__).parents[1] / "vm100-pilot.py"
        spec = importlib.util.spec_from_file_location("vm100_pilot_cli", script)
        if spec is None or spec.loader is None:
            raise RuntimeError(f"could not load {script}")
        module: Any = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        module.PilotLifecycle = lambda _config, _runner: lifecycle
        args = argparse.Namespace(command="setup", skip_build=False)
        return config, runner, lifecycle, events, (module, args)

    def test_setup_start_failure_restores_the_predecessor_deployment(self) -> None:
        config, _, lifecycle, events, dispatch = self._deployment_scenario(
            fail_new_start=True
        )
        module, args = dispatch

        with self.assertRaisesRegex(RuntimeError, "injected replacement-start failure"):
            module.dispatch(args, config, self.runner)

        self.assertEqual(config.binary.read_bytes(), b"predecessor-binary")
        self.assertEqual(
            config.build_receipt.read_bytes(),
            b"commit=predecessor\nbinary_sha256=predecessor\n",
        )
        self.assertEqual(
            events[-5:],
            [
                "rollback-stop",
                "restore-binary",
                "restore-receipt",
                "rollback-start",
                "rollback-status",
            ],
        )
        self.assertEqual(getattr(lifecycle, "start_requirements"), [True, False])

    def test_setup_status_failure_restores_the_predecessor_deployment(self) -> None:
        config, _, _, events, dispatch = self._deployment_scenario(fail_new_status=True)
        module, args = dispatch

        with self.assertRaisesRegex(
            RuntimeError, "injected replacement-status failure"
        ):
            module.dispatch(args, config, self.runner)

        self.assertEqual(config.binary.read_bytes(), b"predecessor-binary")
        self.assertEqual(
            config.build_receipt.read_bytes(),
            b"commit=predecessor\nbinary_sha256=predecessor\n",
        )
        self.assertEqual(events[-2:], ["rollback-start", "rollback-status"])

    def test_setup_aggregates_every_rollback_failure(self) -> None:
        failures = frozenset(
            {"stop", "restore-binary", "restore-receipt", "start", "status"}
        )
        config, _, _, events, dispatch = self._deployment_scenario(
            fail_new_status=True,
            rollback_failures=failures,
        )
        module, args = dispatch

        with self.assertRaisesRegex(
            RuntimeError, "injected replacement-status failure"
        ) as raised:
            module.dispatch(args, config, self.runner)

        notes = "\n".join(getattr(raised.exception, "__notes__", ()))
        for failure in (
            "rollback-stop",
            "restore-binary",
            "restore-receipt",
            "rollback-start",
            "rollback-status",
        ):
            with self.subTest(failure=failure):
                self.assertIn(failure, notes)
                self.assertIn(failure, " ".join(events))

    def test_setup_retains_backup_through_validation_then_deletes_it(self) -> None:
        config, _, _, events, dispatch = self._deployment_scenario()
        module, args = dispatch
        observed_backups: list[int] = []
        lifecycle = module.PilotLifecycle(config, self.runner)
        original_start = lifecycle.start
        original_status = lifecycle.status

        def start() -> dict[str, int]:
            observed_backups.append(
                len(list(config.temp_dir.glob("zerofs-deploy-backup-*")))
            )
            return original_start()

        def status(*, validate_data: bool = True) -> dict[str, object]:
            observed_backups.append(
                len(list(config.temp_dir.glob("zerofs-deploy-backup-*")))
            )
            return original_status(validate_data=validate_data)

        lifecycle.start = start  # type: ignore[method-assign]
        lifecycle.status = status  # type: ignore[method-assign]

        with redirect_stdout(io.StringIO()):
            module.dispatch(args, config, self.runner)

        self.assertEqual(observed_backups, [1, 1])
        self.assertFalse(list(config.temp_dir.glob("zerofs-deploy-backup-*")))
        self.assertEqual(events[-2:], ["replacement-start", "replacement-status"])

    def test_iterate_status_failure_uses_the_same_deployment_transaction(self) -> None:
        config, runner, _, events, dispatch = self._deployment_scenario(
            fail_new_status=True
        )
        module, _ = dispatch
        args = argparse.Namespace(command="iterate", skip_build=False)

        with self.assertRaisesRegex(
            RuntimeError, "injected replacement-status failure"
        ):
            module.dispatch(args, config, runner)

        self.assertEqual(config.binary.read_bytes(), b"predecessor-binary")
        self.assertEqual(events[-2:], ["rollback-start", "rollback-status"])

    def test_setup_skip_build_starts_the_installed_deployment_unchanged(self) -> None:
        config, runner, _, events, dispatch = self._deployment_scenario()
        module, _ = dispatch
        args = argparse.Namespace(command="setup", skip_build=True)

        with redirect_stdout(io.StringIO()):
            module.dispatch(args, config, runner)

        self.assertEqual(config.binary.read_bytes(), b"predecessor-binary")
        self.assertEqual(
            config.build_receipt.read_bytes(),
            b"commit=predecessor\nbinary_sha256=predecessor\n",
        )
        self.assertEqual(events, ["replacement-start", "replacement-status"])
        self.assertFalse(list(config.temp_dir.glob("zerofs-deploy-backup-*")))

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
                    "zerofs_segment_gc_active 1",
                    "zerofs_segment_gc_passes_total 3",
                    "zerofs_segment_gc_batches_total 5",
                    "zerofs_segment_gc_deleted_bytes_total 100",
                )
            )
        )
        self.assertEqual(snapshot.accepted, 9)
        self.assertEqual(snapshot.remote, 7)
        self.assertTrue(snapshot.gc_active)
        self.assertEqual(snapshot.gc_passes, 3)
        self.assertEqual(snapshot.gc_batches, 5)
        self.assertEqual(snapshot.gc_deleted_bytes, 100)
        self.assertFalse(snapshot.drained)
        legacy_snapshot = WritebackSnapshot.parse(
            "\n".join(
                (
                    "zerofs_writeback_accepted_sequence 9",
                    "zerofs_writeback_local_sequence 9",
                    "zerofs_writeback_remote_sequence 9",
                    "zerofs_writeback_dirty_ram_bytes 0",
                    "zerofs_writeback_dirty_ssd_bytes 0",
                    "zerofs_writeback_local_bytes_completed_total 4",
                    "zerofs_writeback_remote_bytes_completed_total 4",
                    "zerofs_writeback_terminal_error 0",
                    "zerofs_segment_gc_passes_total 3",
                    "zerofs_segment_gc_batches_total 5",
                    "zerofs_segment_gc_deleted_bytes_total 100",
                )
            )
        )
        self.assertIsNone(legacy_snapshot.gc_active)
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

    def test_gc_quiescence_waits_for_the_first_completed_pass(self) -> None:
        from scripts.vm100_pilot.metrics import wait_for_gc_quiescence

        snapshots = iter(
            (
                WritebackSnapshot(9, 9, 9, 0, 0, 1, 1, False, True, 0, 0, 0),
                WritebackSnapshot(9, 9, 9, 0, 0, 1, 1, False, False, 1, 2, 64),
                WritebackSnapshot(9, 9, 9, 0, 0, 1, 1, False, False, 1, 2, 64),
            )
        )
        result = wait_for_gc_quiescence(
            lambda: next(snapshots),
            timeout=1,
            stable_samples=2,
            interval=0,
            monotonic=lambda: 0,
            sleep=lambda _: None,
        )
        self.assertEqual(result.gc_passes, 1)
        self.assertFalse(result.gc_active)

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

    def test_accepted_barrier_waits_for_metrics_to_observe_submitted_work(self) -> None:
        snapshots = iter(
            (
                WritebackSnapshot(12, 12, 12, 0, 0, 10, 10, False),
                WritebackSnapshot(12, 12, 12, 0, 0, 10, 10, False),
                WritebackSnapshot(14, 14, 12, 0, 4, 13, 10, False),
                WritebackSnapshot(15, 15, 12, 0, 4, 14, 10, False),
                WritebackSnapshot(15, 15, 12, 0, 4, 14, 10, False),
                WritebackSnapshot(15, 15, 12, 0, 4, 14, 10, False),
                WritebackSnapshot(15, 15, 12, 0, 4, 14, 10, False),
            )
        )
        result = wait_for_accepted_after(
            lambda: next(snapshots),
            previous_sequence=12,
            timeout=1,
            sleep=lambda _: None,
        )
        self.assertEqual(result.accepted, 15)
        self.assertEqual(result.local_bytes, 14)

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

    def test_stale_write_zeroes_limit_refuses_configured_device_before_mount(
        self,
    ) -> None:
        config = replace(self.config, nbd_device=Path("/dev/nbd7"))
        self.runner.max_write_zeroes_sectors = "1024"
        lifecycle = PilotLifecycle(config, self.runner)

        with self.assertRaisesRegex(RuntimeError, r"/dev/nbd7.*1024"):
            lifecycle.start()

        starts = [
            call[0][2]
            for call in self.runner.calls
            if call[0][:2] == ("systemctl", "start")
        ]
        self.assertEqual(starts, [config.service, config.client_service])
        self.assertFalse(self.runner.active)
        queue_call = (
            ("cat", "/sys/block/nbd7/queue/max_write_zeroes_sectors"),
            False,
        )
        self.assertIn(queue_call, self.runner.calls)
        client_active_call = (
            ("systemctl", "is-active", config.client_service),
            False,
        )
        self.assertLess(
            self.runner.calls.index(client_active_call),
            self.runner.calls.index(queue_call),
        )
        self.assertNotIn(
            (("cat", "/sys/block/nbd0/queue/max_write_zeroes_sectors"), False),
            self.runner.calls,
        )

    def test_storage_client_restart_keeps_the_daemon_running(self) -> None:
        self.runner.active.update(
            (self.config.service, self.config.client_service, self.config.mount_unit)
        )

        self.lifecycle.stop_storage_clients()
        self.assertIn(self.config.service, self.runner.active)
        self.assertNotIn(self.config.client_service, self.runner.active)
        self.assertNotIn(self.config.mount_unit, self.runner.active)

        self.lifecycle.start_storage_clients()
        starts = [
            call[0][2]
            for call in self.runner.calls
            if call[0][:2] == ("systemctl", "start")
        ]
        self.assertEqual(
            starts[-2:], [self.config.client_service, self.config.mount_unit]
        )

    def test_fresh_disk_start_can_pause_between_client_and_mount(self) -> None:
        self.lifecycle.start_daemon()
        self.lifecycle.start_client()
        self.assertIn(self.config.service, self.runner.active)
        self.assertIn(self.config.client_service, self.runner.active)
        self.assertNotIn(self.config.mount_unit, self.runner.active)

        self.lifecycle.start_mount()
        starts = [
            call[0][2]
            for call in self.runner.calls
            if call[0][:2] == ("systemctl", "start")
        ]
        self.assertEqual(
            starts[-3:],
            [self.config.service, self.config.client_service, self.config.mount_unit],
        )

    def test_striped_migration_refuses_a_busy_scratch_device(self) -> None:
        class BusyDeviceRunner(FakeRunner):
            def run(
                self, argv: Sequence[str | Path], **kwargs: Any
            ) -> CompletedProcess[str]:
                args = tuple(str(value) for value in argv)
                if args == ("cat", "/sys/block/nbd1/size"):
                    return CompletedProcess(args, 0, "2048\n", "")
                return super().run(args, **kwargs)

        runner = BusyDeviceRunner()
        lifecycle = PilotLifecycle(self.config, runner)
        migrator = StripedMigrator(self.config, runner, lifecycle)

        with self.assertRaisesRegex(RuntimeError, "/dev/nbd1 is already attached"):
            migrator.run()

        self.assertFalse(any("provision-striped" in call[0] for call in runner.calls))

    def test_cutover_preflight_failure_restarts_the_predecessor_clients(self) -> None:
        checked_devices: list[Path | None] = []

        class CutoverMigrator(StripedMigrator):
            @contextmanager
            def _admin_namespace(self) -> Any:
                yield object()

            def _verify_layout(self, namespace: Any, export: str) -> None:
                return None

            def _device_size(self, path: Path | None = None) -> int:
                checked_devices.append(path)
                return 1

        config = replace(self.config, nbd_device=Path("/dev/nbd7"))
        self.runner.active.update(
            (config.service, config.client_service, config.mount_unit)
        )
        migrator = CutoverMigrator(
            config,
            self.runner,
            self.lifecycle,
        )

        with self.assertRaisesRegex(RuntimeError, "remained attached"):
            migrator._cutover("replacement", "predecessor")

        self.assertIn(self.config.client_service, self.runner.active)
        self.assertIn(self.config.mount_unit, self.runner.active)
        self.assertEqual(checked_devices, [Path("/dev/nbd7")])
        self.assertNotIn(Path("/dev/sda"), checked_devices)

    def test_status_uses_the_configured_nbd_device_authority(self) -> None:
        config = replace(self.config, nbd_device=Path("/dev/nbd7"))

        class StatusRunner(FakeRunner):
            def run(
                self, argv: Sequence[str | Path], **kwargs: Any
            ) -> CompletedProcess[str]:
                args = tuple(str(value) for value in argv)
                if args[:4] == ("findmnt", "-no", "SOURCE,FSTYPE,TARGET", "-M"):
                    self.calls.append((args, bool(kwargs.get("sudo", False))))
                    return CompletedProcess(
                        args,
                        0,
                        f"/dev/nbd7 xfs {config.mountpoint}\n",
                        "",
                    )
                if args[:2] == ("cat", str(config.config_file)):
                    self.calls.append((args, bool(kwargs.get("sudo", False))))
                    return CompletedProcess(
                        args,
                        0,
                        '[writeback]\nenabled = true\nack_mode = "memory"\n',
                        "",
                    )
                if args[:2] == ("cat", str(config.build_receipt)):
                    self.calls.append((args, bool(kwargs.get("sudo", False))))
                    return CompletedProcess(
                        args, 0, "commit=test\nbinary_sha256=abc\n", ""
                    )
                if args and args[0] == "sha256sum":
                    self.calls.append((args, bool(kwargs.get("sudo", False))))
                    return CompletedProcess(args, 0, "abc  target\n", "")
                return super().run(list(argv), **kwargs)

        runner = StatusRunner()
        runner.active.update((config.service, config.client_service, config.mount_unit))
        snapshot = WritebackSnapshot(9, 9, 9, 0, 0, 1, 1, False, False, 1, 0, 0)
        lifecycle = PilotLifecycle(
            config,
            runner,  # type: ignore[arg-type]
            _StaticMetrics(snapshot),  # type: ignore[arg-type]
        )

        status = lifecycle.status(validate_data=False)

        self.assertEqual(status["mount"]["source"], "/dev/nbd7")
        findmnt = next(call[0] for call in runner.calls if call[0][0] == "findmnt")
        self.assertEqual(findmnt[-1], str(config.mountpoint))


class _StaticMetrics:
    def __init__(self, snapshot: WritebackSnapshot) -> None:
        self.value = snapshot

    def snapshot(self) -> WritebackSnapshot:
        return self.value


class FreshResetTests(unittest.TestCase):
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
                "ZEROFS_PILOT_MOUNTPOINT": str(mount),
                "ZEROFS_PILOT_INTEGRITY_FILE": str(mount / "integrity"),
                "ZEROFS_PILOT_METADATA_DIR": str(mount / "metadata"),
                "ZEROFS_PILOT_RESULT_DIR": str(Path(self.temp.name) / "results"),
                "ZEROFS_PROFILE_TARGET_DIR": str(Path(self.temp.name) / "profile"),
            },
        )
        self.events: list[str] = []

    def _resetter(self, *, fail_provision: bool = False) -> FreshResetter:
        events = self.events

        class Lifecycle:
            def require_vm100(self) -> None:
                events.append("require-vm100")

            def status(self) -> dict[str, object]:
                events.append("status")
                return {"healthy": True}

            def drain(self) -> dict[str, object]:
                events.append("drain")
                return {"drained": True}

            def stop(self) -> None:
                events.append("stop")

            def start_daemon(self) -> None:
                events.append("start-daemon")

            def start_client(self) -> None:
                events.append("start-client")

            def start_mount(self) -> None:
                events.append("start-mount")

            def start(self) -> None:
                events.append("start-old-stack")

        class ControlledResetter(FreshResetter):
            def _read_config(self) -> str:
                return '[storage]\nurl = "sftp://pilot@example.test:23/old"\n'

            def _fast_topology(self) -> str:
                return "fastpool/fast zfs /fast"

            def _stage_fixtures(self) -> Path:
                events.append("stage")
                return Path("/var/tmp/reset-seed")

            def _install_config_text(self, text: str) -> None:
                events.append("install-old" if '/old"' in text else "install-new")

            def _activate_fresh_state(self) -> Path:
                events.append("activate-fresh-state")
                return Path("/var/lib/zerofs/nbd-pilot-reset-rollback-test")

            def _restore_old_state(self, backup: Path) -> None:
                events.append("restore-old-state")

            def _remove_old_state(self, backup: Path) -> None:
                events.append("remove-old-state")

            def _provision(self) -> None:
                events.append("provision")
                if fail_provision:
                    raise RuntimeError("injected provision failure")

            def _verify_stripe_layout(self) -> None:
                events.append("verify-layout")

            def _format_device(self) -> None:
                events.append("format")

            def _restore_fixtures(self, seed: Path) -> None:
                events.append("restore-fixtures")

            def _remove_seed(self, seed: Path) -> None:
                events.append("remove-seed")

        return ControlledResetter(
            self.config,
            FakeRunner(),  # type: ignore[arg-type]
            Lifecycle(),  # type: ignore[arg-type]
        )

    def test_fresh_reset_formats_before_mount_and_validates_after_restore(self) -> None:
        result = self._resetter().run(remote_prefix="new", confirm_destroy_pilot=True)
        self.assertTrue(result["reset"])
        self.assertEqual(
            self.events,
            [
                "require-vm100",
                "status",
                "stage",
                "stop",
                "install-new",
                "activate-fresh-state",
                "start-daemon",
                "provision",
                "start-client",
                "verify-layout",
                "format",
                "start-mount",
                "restore-fixtures",
                "status",
                "drain",
                "remove-old-state",
                "remove-seed",
            ],
        )

    def test_fresh_reset_restores_the_old_stack_after_provision_failure(self) -> None:
        with self.assertRaisesRegex(RuntimeError, "injected provision failure"):
            self._resetter(fail_provision=True).run(
                remote_prefix="new", confirm_destroy_pilot=True
            )
        self.assertEqual(
            self.events[-6:],
            [
                "stop",
                "install-old",
                "restore-old-state",
                "start-old-stack",
                "status",
                "remove-seed",
            ],
        )

    def test_fresh_format_refuses_any_existing_block_signature(self) -> None:
        class SignatureRunner(FakeRunner):
            def run(
                self, argv: Sequence[str | Path], **kwargs: Any
            ) -> CompletedProcess[str]:
                args = tuple(str(value) for value in argv)
                self.calls.append((args, bool(kwargs.get("sudo", False))))
                if args == ("cat", "/sys/block/nbd0/size"):
                    return CompletedProcess(args, 0, "134217728\n", "")
                if args[:4] == ("findmnt", "-rn", "-S", "/dev/nbd0"):
                    return CompletedProcess(args, 1, "", "")
                if args[:2] == ("wipefs", "-n"):
                    return CompletedProcess(args, 0, "offset 0x0 xfs\n", "")
                return CompletedProcess(args, 0, "", "")

        runner = SignatureRunner()
        resetter = FreshResetter(
            self.config,
            runner,  # type: ignore[arg-type]
            object(),  # type: ignore[arg-type]
        )
        with self.assertRaisesRegex(RuntimeError, "existing block signature"):
            resetter._format_device()
        self.assertFalse(any(call[0][0] == "mkfs.xfs" for call in runner.calls))

    def test_fresh_format_targets_only_the_configured_device_without_force(
        self,
    ) -> None:
        config = replace(self.config, nbd_device=Path("/dev/nbd7"))

        class EmptyDeviceRunner(FakeRunner):
            def run(
                self, argv: Sequence[str | Path], **kwargs: Any
            ) -> CompletedProcess[str]:
                args = tuple(str(value) for value in argv)
                self.calls.append((args, bool(kwargs.get("sudo", False))))
                if args == ("cat", "/sys/block/nbd7/size"):
                    return CompletedProcess(args, 0, "134217728\n", "")
                if args[:4] == ("findmnt", "-rn", "-S", "/dev/nbd7"):
                    return CompletedProcess(args, 1, "", "")
                if args[:2] == ("wipefs", "-n"):
                    return CompletedProcess(args, 0, "", "")
                return CompletedProcess(args, 0, "", "")

        runner = EmptyDeviceRunner()
        resetter = FreshResetter(
            config,
            runner,  # type: ignore[arg-type]
            object(),  # type: ignore[arg-type]
        )
        resetter._format_device()
        mkfs = next(call[0] for call in runner.calls if call[0][0] == "mkfs.xfs")
        self.assertEqual(mkfs[-1], "/dev/nbd7")
        self.assertNotIn("-f", mkfs)
        device_commands = (
            call[0]
            for call in runner.calls
            if call[0][0] in {"cat", "findmnt", "wipefs", "blkid", "mkfs.xfs"}
        )
        for command in device_commands:
            with self.subTest(command=command):
                self.assertNotIn("/dev/nbd0", command)
                self.assertNotIn("/dev/sda", command)

    def test_reset_state_requires_the_configured_root_and_children(self) -> None:
        class ConfigRunner(FakeRunner):
            def __init__(self, text: str) -> None:
                super().__init__()
                self.text = text

            def run(
                self, argv: Sequence[str | Path], **kwargs: Any
            ) -> CompletedProcess[str]:
                args = tuple(str(value) for value in argv)
                if args[:2] == ("cat", str(self_config.config_file)):
                    self.calls.append((args, bool(kwargs.get("sudo", False))))
                    return CompletedProcess(args, 0, self.text, "")
                return super().run(list(argv), **kwargs)

        self_config = self.config
        valid = (
            '[cache]\ndir = "/var/lib/zerofs/nbd-pilot/read-cache"\n'
            '[writeback]\ndir = "/var/lib/zerofs/nbd-pilot/writeback"\n'
        )
        resetter = FreshResetter(
            self.config,
            ConfigRunner(valid),  # type: ignore[arg-type]
            object(),  # type: ignore[arg-type]
        )
        self.assertEqual(resetter._state_root(), self.config.pilot_state_root)

        invalid_pairs = (
            (
                "/var/lib/zerofs/another-pilot/read-cache",
                "/var/lib/zerofs/another-pilot/writeback",
            ),
            (
                "/var/lib/unrelated/read-cache",
                "/var/lib/unrelated/writeback",
            ),
            (
                "/var/lib/zerofs/nbd-pilot/read-cache",
                "/var/lib/zerofs/nbd-pilot/not-writeback",
            ),
        )
        for cache_dir, writeback_dir in invalid_pairs:
            with self.subTest(cache_dir=cache_dir, writeback_dir=writeback_dir):
                text = (
                    f'[cache]\ndir = "{cache_dir}"\n'
                    f'[writeback]\ndir = "{writeback_dir}"\n'
                )
                resetter = FreshResetter(
                    self.config,
                    ConfigRunner(text),  # type: ignore[arg-type]
                    object(),  # type: ignore[arg-type]
                )
                with self.assertRaisesRegex(ValueError, "configured pilot state root"):
                    resetter._state_root()

    def test_reset_state_cleanup_refuses_an_arbitrary_var_lib_sibling(self) -> None:
        config_text = (
            '[cache]\ndir = "/var/lib/zerofs/nbd-pilot/read-cache"\n'
            '[writeback]\ndir = "/var/lib/zerofs/nbd-pilot/writeback"\n'
        )

        class RecordingRunner(FakeRunner):
            def run(
                self, argv: Sequence[str | Path], **kwargs: Any
            ) -> CompletedProcess[str]:
                args = tuple(str(value) for value in argv)
                self.calls.append((args, bool(kwargs.get("sudo", False))))
                if args[:2] == ("cat", str(self_config.config_file)):
                    return CompletedProcess(args, 0, config_text, "")
                return CompletedProcess(args, 0, "", "")

        self_config = self.config
        runner = RecordingRunner()
        resetter = FreshResetter(
            self.config,
            runner,  # type: ignore[arg-type]
            object(),  # type: ignore[arg-type]
        )

        with self.assertRaisesRegex(ValueError, "reset state backup"):
            resetter._restore_old_state(
                Path("/var/lib/unrelated/nbd-pilot-reset-rollback-deadbeef")
            )

        self.assertFalse(any(call[0][0] == "rm" for call in runner.calls))


class _HealthyLifecycle:
    def __init__(
        self, snapshot: WritebackSnapshot, config: PilotConfig | None = None
    ) -> None:
        self.config = config
        self.metrics = _StaticMetrics(snapshot)
        self.drain_calls = 0
        self.start_calls = 0
        self.stop_calls = 0
        self.status_validations: list[bool] = []

    def status(self, *, validate_data: bool = True) -> dict[str, object]:
        self.status_validations.append(validate_data)
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
            },
        )
        self.snapshot = WritebackSnapshot(
            9, 9, 9, 0, 0, 1 << 20, 1 << 20, False, False, 1, 0, 0
        )
        self.runner = FakeRunner()
        self.lifecycle = _HealthyLifecycle(self.snapshot, self.config)

    def test_benchmark_rejects_short_fio_results(self) -> None:
        with self.assertRaisesRegex(RuntimeError, "short I/O"):
            _validate_fio_bytes(
                FioResult(bytes=3 << 20, runtime_ms=1, mibps=3072.0),
                expected_bytes=4 << 20,
                phase="foreground write",
            )

    def test_direct_read_pair_warms_zerofs_then_measures_the_hot_path(self) -> None:
        calls: list[tuple[str, bool | None]] = []

        class RecordingBenchmark(BenchmarkRunner):
            def _run_fio(self, *args: object, **kwargs: Any) -> FioResult:
                calls.append((kwargs["name"], kwargs.get("direct")))
                return FioResult(bytes=4 << 20, runtime_ms=1, mibps=4096.0)

        benchmark = RecordingBenchmark(
            self.config,
            self.runner,
            self.lifecycle,  # type: ignore[arg-type]
        )
        benchmark._run_direct_read_pair(
            run_root=self.config.mountpoint / ".zerofs-bench-test",
            per_job_mib=4,
            jobs=1,
            warmup_output=Path(self.temp.name) / "direct-warmup.json",
            hot_output=Path(self.temp.name) / "direct-hot.json",
            after_warmup=lambda: calls.append(("snapshot", None)),
        )

        self.assertEqual(
            calls,
            [
                ("zerofs_direct_read_warmup", True),
                ("snapshot", None),
                ("zerofs_direct_read_hot", True),
            ],
        )

    def test_local_rate_uses_completed_payload_and_full_interval(self) -> None:
        result = calculate_tiers(
            logical_bytes=1 << 30,
            local_bytes=1 << 30,
            remote_bytes=1 << 30,
            foreground_ms=1000,
            local_end_to_end_ms=4000,
            remote_end_to_end_ms=10000,
            local_active_ms=1000,
            remote_active_ms=5000,
            page_cache_hot_read_ms=2000,
            zerofs_direct_read_ms=500,
        )
        self.assertEqual(result.foreground_mibps, 1024.0)
        self.assertEqual(result.local_mibps, 256.0)
        self.assertEqual(result.remote_mibps, 102.4)
        self.assertEqual(result.local_active_mibps, 1024.0)
        self.assertEqual(result.remote_active_mibps, 204.8)
        self.assertEqual(result.zerofs_direct_read_mibps, 2048.0)

    def test_fio_result_uses_fio_internal_runtime_and_bytes(self) -> None:
        path = Path(self.temp.name) / "fio.json"
        path.write_text(
            json.dumps(
                {
                    "jobs": [
                        {
                            "read": {
                                "io_bytes": 536_870_912,
                                "runtime": 40,
                                "bw_bytes": 13_421_772_800,
                            },
                            "write": {"io_bytes": 0, "runtime": 0, "bw_bytes": 0},
                        }
                    ]
                }
            ),
            encoding="utf-8",
        )

        result = FioResult.from_json(path, operation="read")

        self.assertEqual(result.bytes, 536_870_912)
        self.assertEqual(result.runtime_ms, 40)
        self.assertEqual(result.mibps, 12_800.0)

    def test_page_cache_hit_requires_zero_nbd_reads(self) -> None:
        before = BlockIoSnapshot("nbd0", 1000, 2000, 30)
        after = BlockIoSnapshot("nbd0", 1000, 2000, 35)
        evidence = verify_page_cache_hit(before, after)
        self.assertEqual(evidence.read_bytes, 0)
        self.assertTrue(evidence.proven)

        with self.assertRaisesRegex(RuntimeError, "reached nbd0"):
            verify_page_cache_hit(before, BlockIoSnapshot("nbd0", 1512, 2000, 40))

    def test_system_io_snapshot_and_summary_attribute_root_disk_pressure(self) -> None:
        proc = Path(self.temp.name) / "proc"
        (proc / "pressure").mkdir(parents=True)
        (proc / "pressure" / "io").write_text(
            "some avg10=1.25 avg60=0.50 avg300=0.10 total=1000000\n"
            "full avg10=0.75 avg60=0.25 avg300=0.05 total=250000\n",
            encoding="utf-8",
        )
        (proc / "diskstats").write_text(
            "8 0 sda 10 0 2048 5 20 0 4096 7 0 12 14 0 0 0 0\n"
            "8 1 sda1 8 0 1024 3 15 0 3072 4 0 8 9 0 0 0 0\n",
            encoding="utf-8",
        )
        before = SystemIoSnapshot.capture(proc, root_device=(8, 1))
        (proc / "pressure" / "io").write_text(
            "some avg10=3.50 avg60=0.50 avg300=0.10 total=1600000\n"
            "full avg10=2.25 avg60=0.25 avg300=0.05 total=400000\n",
            encoding="utf-8",
        )
        (proc / "diskstats").write_text(
            "8 1 sda1 9 0 3072 5 18 0 7168 8 0 508 509 0 0 0 0\n",
            encoding="utf-8",
        )
        after = SystemIoSnapshot.capture(proc, root_device=(8, 1))

        self.assertEqual(before.root_read_bytes, 1024 * 512)
        self.assertEqual(before.root_write_bytes, 3072 * 512)
        summary = summarize_system_io([before, after], elapsed_ms=1000)
        self.assertEqual(summary.some_stall_ms, 600.0)
        self.assertEqual(summary.full_stall_ms, 150.0)
        self.assertEqual(summary.root_read_mib, 1.0)
        self.assertEqual(summary.root_write_mib, 2.0)
        self.assertEqual(summary.root_busy_ms, 500)
        self.assertEqual(summary.root_utilization_percent, 50.0)
        self.assertEqual(summary.peak_some_avg10, 3.5)
        self.assertEqual(summary.peak_full_avg10, 2.25)

    def test_filesystem_device_uses_the_journal_paths_device(self) -> None:
        metadata = os.stat_result((0, 0, 0x1234, 0, 0, 0, 0, 0, 0, 0))
        with mock.patch("scripts.vm100_pilot.system_io.os.stat", return_value=metadata):
            self.assertEqual(
                filesystem_device(Path("/var/lib/zerofs/nbd-pilot")),
                (os.major(metadata.st_dev), os.minor(metadata.st_dev)),
            )

    def test_active_windows_separate_local_journal_and_remote_drain(self) -> None:
        path = Path(self.temp.name) / "metrics.csv"
        path.write_text(
            "timestamp_ms,accepted,local,remote,dirty_ram,dirty_ssd,local_bytes,remote_bytes,terminal\n"
            "1000,10,10,10,0,0,100,100,False\n"
            "1100,11,10,10,64,0,100,100,False\n"
            "1200,11,11,10,0,64,164,100,False\n"
            "1500,11,11,11,0,0,164,164,False\n",
            encoding="utf-8",
        )
        self.assertEqual(
            _active_windows(
                path,
                before_accepted=10,
                before_local_bytes=100,
                target_local_bytes=164,
                before_remote_bytes=100,
                target_remote_bytes=164,
            ),
            (200, 300),
        )

    def test_metric_sampling_uses_monotonic_time(self) -> None:
        with mock.patch(
            "scripts.vm100_pilot.benchmark.time.monotonic_ns",
            return_value=1_234_567_890,
        ):
            self.assertEqual(_monotonic_ms(), 1234)

    def test_benchmark_rejects_a_gc_pass_inside_the_measured_epoch(self) -> None:
        from scripts.vm100_pilot.benchmark import (
            BenchmarkContaminatedError,
            _assert_no_maintenance,
        )

        before = WritebackSnapshot(9, 9, 9, 0, 0, 1, 1, False, False, 4, 8, 10)
        after = replace(before, gc_passes=5, gc_batches=9, gc_deleted_bytes=74)

        with self.assertRaisesRegex(BenchmarkContaminatedError, "segment GC"):
            _assert_no_maintenance(before, after)

    def test_gc_quiescence_waits_for_a_fresh_pass_when_requested(self) -> None:
        snapshots = iter(
            (
                WritebackSnapshot(9, 9, 9, 0, 0, 1, 1, False, False, 4, 0, 0),
                WritebackSnapshot(9, 9, 9, 0, 0, 1, 1, False, True, 5, 0, 0),
                WritebackSnapshot(9, 9, 9, 0, 0, 1, 1, False, False, 5, 0, 0),
            )
        )

        result = wait_for_gc_quiescence(
            lambda: next(snapshots),
            timeout=1,
            after_pass=4,
            stable_samples=1,
            interval=0,
        )

        self.assertEqual(result.gc_passes, 5)

    def test_isolated_benchmark_accepts_the_completed_startup_gc_pass(self) -> None:
        config = replace(self.config, drain_timeout=1)
        benchmark = BenchmarkRunner(config, self.runner, self.lifecycle)  # type: ignore[arg-type]

        result = benchmark._wait_clean_gc(maintenance_isolated=True)

        self.assertEqual(result.gc_passes, 1)

    def test_ordinary_benchmark_still_requires_a_fresh_gc_pass(self) -> None:
        snapshots = iter(
            (
                replace(self.snapshot, gc_passes=4),
                replace(self.snapshot, gc_active=True, gc_passes=5),
                replace(self.snapshot, gc_passes=5),
                replace(self.snapshot, gc_passes=5),
                replace(self.snapshot, gc_passes=5),
                replace(self.snapshot, gc_passes=5),
            )
        )

        class SequenceMetrics:
            def snapshot(self) -> WritebackSnapshot:
                return next(snapshots)

        lifecycle = _HealthyLifecycle(self.snapshot)
        lifecycle.metrics = SequenceMetrics()  # type: ignore[assignment]
        benchmark = BenchmarkRunner(self.config, self.runner, lifecycle)  # type: ignore[arg-type]

        result = benchmark._wait_clean_gc(maintenance_isolated=False)

        self.assertEqual(result.gc_passes, 5)

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
            def _run_fio(self, *args: object, **kwargs: object) -> FioResult:
                raise CommandError(("fio",), 19, "injected fio failure")

            def _local_device(self) -> tuple[int, int]:
                return (8, 1)

            def _system_io(self, device: tuple[int, int]) -> SystemIoSnapshot:
                assert device == (8, 1)
                return SystemIoSnapshot("sda1", 0, 0, 0, 0, 0, 0, 0)

            def _wait_clean_gc(
                self, *, maintenance_isolated: bool
            ) -> WritebackSnapshot:
                return self.lifecycle.metrics.snapshot()

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


class _ProfileBenchmark:
    def __init__(
        self,
        config: PilotConfig,
        config_file: Path,
        error: BaseException | None = None,
    ) -> None:
        self.config = config
        self.config_file = config_file
        self.error = error

    def run(
        self,
        *,
        total_mib: int,
        jobs: int,
        maintenance_isolated: bool = False,
    ) -> BenchmarkResult:
        if not maintenance_isolated:
            raise AssertionError("profile benchmark was not isolated")
        gc = tomllib.loads(self.config_file.read_text())["gc"]
        expected = self.config.maintenance_isolation_secs
        observed = tuple(gc[key] for key in profile_module._GC_CADENCE_KEYS)
        if observed != (expected, expected, expected):
            raise AssertionError(f"unexpected isolated GC config: {gc}")
        if self.error is not None:
            raise self.error
        return BenchmarkResult(
            logical_bytes=4 << 20,
            local_bytes=4 << 20,
            remote_bytes=4 << 20,
            foreground_ms=1,
            local_end_to_end_ms=1,
            remote_end_to_end_ms=1,
            local_active_ms=1,
            remote_active_ms=1,
            page_cache_hot_read_ms=1,
            zerofs_direct_read_ms=1,
            foreground_mibps=4096.0,
            local_mibps=4096.0,
            remote_mibps=4096.0,
            local_active_mibps=4096.0,
            remote_active_mibps=4096.0,
            page_cache_hot_read_mibps=4096.0,
            zerofs_direct_read_mibps=4096.0,
            receipt_dir="",
        )


class _TestProfileRunner(ProfileRunner):
    def _build_profile(self) -> Path:
        binary = self.config.profile_target / "release" / "zerofs"
        binary.parent.mkdir(parents=True)
        binary.write_bytes(b"profile-binary")
        return binary

    def _start_collectors(self, pid: int, receipt: RunReceipt) -> Any:
        return type(
            "Collectors",
            (),
            {"stop": lambda _self, *, phase_windows=None: None},
        )()

    def _service_pid(self) -> int:
        return 123


class ProfileTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory(dir="/var/tmp")
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
        self.config_file = Path(self.temp.name) / "etc" / "zerofs.toml"
        self.binary.parent.mkdir()
        self.config_file.parent.mkdir()
        self.binary.write_bytes(b"canonical-binary")
        self.receipt_file.write_text("commit=canonical\nbinary_sha256=old\n")
        self.original_config = (
            b"# preserved exactly, including comments\n"
            b'[storage]\nurl = "sftp://pilot@example.test:23/data"\n\n'
            b"[gc]\ninterval_secs = 60 # canonical base\n"
            b"idle_interval_secs = 5\nread_directed = true\n"
            b"busy_backlog_interval_secs = 15\n\n"
            b'[writeback]\nenabled = true\nack_mode = "memory"\n'
        )
        self.config_file.write_bytes(self.original_config)
        self.config = replace(
            base,
            binary=self.binary,
            build_receipt=self.receipt_file,
            config_file=self.config_file,
        )
        self.runner = FakeRunner()
        self.snapshot = WritebackSnapshot(9, 9, 9, 0, 0, 1, 1, False)
        self.lifecycle = _HealthyLifecycle(self.snapshot, self.config)

    def test_profile_build_forces_frame_pointers_for_actionable_callchains(
        self,
    ) -> None:
        class ProfileBuildRunner(FakeRunner):
            def __init__(self, binary: Path) -> None:
                super().__init__()
                self.binary = binary
                self.build_env: Mapping[str, str] | None = None

            def run(
                self,
                argv: Sequence[str | Path],
                **kwargs: Any,
            ) -> CompletedProcess[str]:
                args = tuple(str(value) for value in argv)
                if args[:2] == (str(self_config.cargo), "build"):
                    self.build_env = kwargs.get("env")
                    self.binary.parent.mkdir(parents=True, exist_ok=True)
                    self.binary.write_bytes(b"profile-binary")
                    return CompletedProcess(args, 0, "", "")
                if args[:2] == ("readelf", "-S"):
                    return CompletedProcess(args, 0, "[1] .debug_info\n", "")
                return super().run(argv, **kwargs)

        self_config = self.config
        binary = self.config.profile_target / "release" / "zerofs"
        runner = ProfileBuildRunner(binary)
        profiler = ProfileRunner(
            self.config,
            runner,
            self.lifecycle,  # type: ignore[arg-type]
            _ProfileBenchmark(self.config, self.config_file),
        )

        self.assertEqual(profiler._build_profile(), binary)
        self.assertIsNotNone(runner.build_env)
        assert runner.build_env is not None
        self.assertIn("-C force-frame-pointers=yes", runner.build_env["RUSTFLAGS"])

    def test_phase_perf_report_uses_exact_monotonic_window(self) -> None:
        argv = _phase_perf_report_argv(
            Path("/tmp/perf.data"), 1_000_000_001, 2_500_000_009
        )

        self.assertEqual(argv[argv.index("--time") + 1], "1.000000001,2.500000009")
        self.assertEqual(argv[-2:], ["-i", Path("/tmp/perf.data")])

    def test_short_perf_phase_records_insufficient_samples(self) -> None:
        self.assertEqual(
            _phase_report_text("", "zero-sized data", 1),
            "status=insufficient_samples\nreturncode=1\nzero-sized data\n",
        )

    def test_profile_rejects_missing_or_empty_perf_data(self) -> None:
        path = Path(self.temp.name) / "perf.data"
        with self.assertRaisesRegex(RuntimeError, "missing or empty"):
            _require_perf_data(path)
        path.touch()
        with self.assertRaisesRegex(RuntimeError, "missing or empty"):
            _require_perf_data(path)

    def test_perf_record_uses_the_same_monotonic_clock_as_phase_receipts(self) -> None:
        argv = _perf_record_argv(123, Path("/tmp/perf.data"))

        self.assertEqual(argv[argv.index("--clockid") + 1], "monotonic")
        self.assertEqual(argv[argv.index("--call-graph") + 1], "fp")
        self.assertIn("--timestamp", argv)

    def test_profile_loads_benchmark_phase_windows_for_perf_slicing(self) -> None:
        receipt = Path(self.temp.name) / "benchmark-receipt"
        receipt.mkdir()
        (receipt / "manifest.json").write_text(
            json.dumps(
                {
                    "phase_monotonic_ns": {
                        "foreground_write": {"start_ns": 11, "end_ns": 22},
                        "local_end_to_end": {"start_ns": 11, "end_ns": 33},
                        "direct_read": {"start_ns": 33, "end_ns": 44},
                    }
                }
            )
        )
        result = replace(
            calculate_tiers(
                logical_bytes=1,
                local_bytes=1,
                remote_bytes=1,
                foreground_ms=1,
                local_end_to_end_ms=1,
                remote_end_to_end_ms=1,
                local_active_ms=1,
                remote_active_ms=1,
                page_cache_hot_read_ms=1,
                zerofs_direct_read_ms=1,
            ),
            receipt_dir=str(receipt),
        )

        self.assertEqual(
            _load_phase_windows(result),
            {
                "foreground_write": (11, 22),
                "local_end_to_end": (11, 33),
                "direct_read": (33, 44),
            },
        )

    def test_maintenance_rewrite_updates_only_gc_cadence(self) -> None:
        rewritten = profile_module.rewrite_gc_cadence(
            self.original_config.decode(), 3600
        )

        self.assertEqual(
            rewritten,
            self.original_config.decode()
            .replace("interval_secs = 60", "interval_secs = 3600")
            .replace("idle_interval_secs = 5", "idle_interval_secs = 3600")
            .replace(
                "busy_backlog_interval_secs = 15",
                "busy_backlog_interval_secs = 3600",
            ),
        )
        self.assertEqual(
            tomllib.loads(rewritten)["gc"],
            {
                "interval_secs": 3600,
                "idle_interval_secs": 3600,
                "read_directed": True,
                "busy_backlog_interval_secs": 3600,
            },
        )

    def test_maintenance_rewrite_inserts_missing_gc_cadence_keys(self) -> None:
        source = """\
[storage]
url = "sftp://pilot@example.test:23/data"

[gc]
read_directed = false # keep me

[writeback]
enabled = true
"""

        rewritten = profile_module.rewrite_gc_cadence(source, 3600)

        self.assertEqual(
            rewritten,
            """\
[storage]
url = "sftp://pilot@example.test:23/data"

[gc]
interval_secs = 3600
idle_interval_secs = 3600
busy_backlog_interval_secs = 3600
read_directed = false # keep me

[writeback]
enabled = true
""",
        )

    def test_maintenance_rewrite_appends_a_missing_gc_section(self) -> None:
        source = '[storage]\nurl = "sftp://pilot@example.test:23/data"'

        rewritten = profile_module.rewrite_gc_cadence(source, 3600)

        self.assertEqual(
            rewritten,
            source
            + "\n\n[gc]\n"
            + "interval_secs = 3600\n"
            + "idle_interval_secs = 3600\n"
            + "busy_backlog_interval_secs = 3600\n",
        )

    def test_canonical_restore_reinstalls_binary_and_receipt(self) -> None:
        snapshot = CanonicalDeployment.capture(self.config, self.runner)
        self.binary.write_bytes(b"profile-binary")
        self.receipt_file.write_text("commit=profile\n")
        self.config_file.write_text("[gc]\ninterval_secs = 3600\n")
        snapshot.restore()
        self.assertEqual(self.binary.read_bytes(), b"canonical-binary")
        self.assertEqual(
            self.receipt_file.read_text(), "commit=canonical\nbinary_sha256=old\n"
        )
        self.assertEqual(self.config_file.read_bytes(), self.original_config)
        snapshot.cleanup()
        self.assertFalse(snapshot.config_backup.exists())

    def test_profile_success_restores_canonical_config_and_validates_data(self) -> None:
        result = _TestProfileRunner(
            self.config,
            self.runner,
            self.lifecycle,  # type: ignore[arg-type]
            _ProfileBenchmark(self.config, self.config_file),
        ).run(total_mib=4, jobs=1)

        self.assertTrue(result.canonical_binary_restored)
        self.assertEqual(self.config_file.read_bytes(), self.original_config)
        self.assertEqual(self.binary.read_bytes(), b"canonical-binary")
        self.assertEqual(self.lifecycle.status_validations, [True, True, True])

    def test_profile_failure_restores_canonical_deployment(self) -> None:
        profiler = _TestProfileRunner(
            self.config,
            self.runner,  # type: ignore[arg-type]
            self.lifecycle,  # type: ignore[arg-type]
            _ProfileBenchmark(
                self.config,
                self.config_file,
                CommandError(("fio",), 19, "injected profile benchmark failure"),
            ),
        )
        with self.assertRaisesRegex(CommandError, "injected profile benchmark failure"):
            profiler.run(total_mib=4, jobs=1)
        self.assertEqual(self.binary.read_bytes(), b"canonical-binary")
        self.assertEqual(
            self.receipt_file.read_text(), "commit=canonical\nbinary_sha256=old\n"
        )
        self.assertEqual(self.config_file.read_bytes(), self.original_config)
        self.assertGreaterEqual(self.lifecycle.stop_calls, 2)
        self.assertGreaterEqual(self.lifecycle.start_calls, 2)
        self.assertEqual(self.lifecycle.status_validations, [True, True, True])
        self.assertTrue(self.config.profile_target.exists())

    def test_profile_cancellation_restores_config_byte_for_byte(self) -> None:
        profiler = _TestProfileRunner(
            self.config,
            self.runner,  # type: ignore[arg-type]
            self.lifecycle,  # type: ignore[arg-type]
            _ProfileBenchmark(
                self.config,
                self.config_file,
                KeyboardInterrupt("injected cancellation"),
            ),
        )

        with self.assertRaisesRegex(KeyboardInterrupt, "injected cancellation"):
            profiler.run(total_mib=4, jobs=1)

        self.assertEqual(self.config_file.read_bytes(), self.original_config)
        self.assertEqual(self.binary.read_bytes(), b"canonical-binary")
        self.assertEqual(self.lifecycle.status_validations, [True, True, True])

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
            self.runner,
            self.lifecycle,  # type: ignore[arg-type]
        )
        with self.assertRaisesRegex(RuntimeError, "injected install failure"):
            profiler.run(total_mib=4, jobs=1)
        self.assertEqual(self.binary.read_bytes(), b"canonical-binary")
        self.assertGreaterEqual(self.lifecycle.start_calls, 1)
        self.assertTrue(self.config.profile_target.exists())


class WorkloadEngineTests(unittest.TestCase):
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
                "ZEROFS_PILOT_TMP_DIR": str(Path(self.temp.name) / "tmp"),
                "ZEROFS_PILOT_MOUNTPOINT": str(mount),
                "ZEROFS_PILOT_INTEGRITY_FILE": str(mount / "integrity"),
                "ZEROFS_PILOT_METADATA_DIR": str(mount / "metadata"),
            },
        )
        self.config.temp_dir.mkdir()
        snapshot = WritebackSnapshot(9, 9, 9, 0, 0, 1, 1, False)
        self.lifecycle = _HealthyLifecycle(snapshot, self.config)
        self.runner = FakeRunner()

    def test_workload_root_is_created_with_explicit_owner(self) -> None:
        workload = WorkloadRunner(
            self.config,
            self.runner,  # type: ignore[arg-type]
            self.lifecycle,  # type: ignore[arg-type]
        )
        root = self.config.mountpoint / ".zerofs-workloads-test"
        workload._prepare_root(root)
        self.assertTrue(root.is_dir())
        argv = self.runner.calls[-1][0]
        self.assertEqual(argv[argv.index("-o") + 1], self.config.user)

    def test_parallel_delete_removes_every_child(self) -> None:
        workload = WorkloadRunner(
            self.config,
            self.runner,  # type: ignore[arg-type]
            self.lifecycle,  # type: ignore[arg-type]
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
            self.config,
            ConfigRunner(),  # type: ignore[arg-type]
            self.lifecycle,  # type: ignore[arg-type]
        )
        endpoint = raw._endpoint()
        self.assertEqual(endpoint.user, "alice")
        self.assertEqual(endpoint.host, "example.invalid")
        self.assertEqual(endpoint.port, 23)


if __name__ == "__main__":
    unittest.main()
