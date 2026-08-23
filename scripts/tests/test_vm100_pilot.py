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
import threading
import tomllib
import unittest
from unittest import mock
from contextlib import contextmanager, redirect_stdout
from dataclasses import asdict, replace
from pathlib import Path
from subprocess import CompletedProcess
from typing import Any, Mapping, Sequence

from scripts.vm100_pilot.config import PilotConfig
from scripts.vm100_pilot.benchmark import (
    BenchmarkResult,
    BenchmarkCleanupError,
    BenchmarkRunner,
    DirectWriteTiers,
    FioResult,
    _MetricSampler,
    _active_windows,
    _counter_delta,
    _monotonic_ms,
    _rate,
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
    _TemporaryHotpathEnvironment,
    _load_phase_windows,
    _perf_report_argv,
    _phase_report_text,
    _phase_perf_report_argv,
    _perf_record_argv,
    _require_perf_data,
)
from scripts.vm100_pilot.system_io import (
    RATE_DIGITS,
    BlockIoSnapshot,
    SystemIoSnapshot,
    aggregate_fio_jobs,
    filesystem_device,
    mib_per_second,
    summarize_system_io,
    verify_page_cache_hit,
)
from scripts.vm100_pilot.raw_sftp import (
    RawSftpRunner,
    SftpEndpoint,
    SftpEndpointAuthority,
    SshBinaryIdentity,
)
from scripts.vm100_pilot.receipts import RunReceipt
from scripts.vm100_pilot.runner import CommandError, ManagedProcess, Runner
from scripts.vm100_pilot.scenarios import RawSftpScenario
from scripts.vm100_pilot.workloads import WorkloadRunner
import scripts.vm100_pilot.profile as profile_module


_VALID_HOTPATH_REPORT = json.dumps(
    {
        "type": "hotpath_report",
        "functions_timing": {},
        "futures": {},
        "threads": {},
    }
) + "\n"


class FakeRunner(Runner):
    def __init__(self) -> None:
        super().__init__(base_env={})
        self.calls: list[tuple[tuple[str, ...], bool]] = []
        self.active: set[str] = set()
        self.fail_start: str | None = None
        self.write_zeroes_max_bytes: str | None = None
        self.max_write_zeroes_sectors: str | None = "0"

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
            and args[1].endswith("/queue/write_zeroes_max_bytes")
        ):
            if self.write_zeroes_max_bytes is None:
                return CompletedProcess(args, 1, "", "No such file")
            return CompletedProcess(args, 0, self.write_zeroes_max_bytes + "\n", "")
        if (
            args[:1] == ("cat",)
            and args[1].startswith("/sys/block/nbd")
            and args[1].endswith("/queue/max_write_zeroes_sectors")
        ):
            if self.max_write_zeroes_sectors is None:
                return CompletedProcess(args, 1, "", "No such file")
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


class FakeCollectorProcess:
    def __init__(self, argv: Sequence[str | Path], events: list[str]) -> None:
        self.argv = tuple(str(value) for value in argv)
        self.events = events

    def interrupt_child(self, _signal_child: Any) -> None:
        self.events.append("interrupt")

    def terminate(self) -> None:
        self.events.append("terminate")


class CollectorRunner(FakeRunner):
    def __init__(
        self,
        events: list[str],
        sampler_failure: BaseException | None = None,
    ) -> None:
        super().__init__()
        self.events = events
        self.sampler_failure = sampler_failure

    def spawn(
        self,
        argv: Sequence[str | Path],
        **_kwargs: Any,
    ) -> Any:
        args = tuple(str(value) for value in argv)
        if args[:2] == ("perf", "record"):
            self.events.append("record-spawned")
            Path(args[args.index("-o") + 1]).write_bytes(b"perf-data")
        else:
            if "enabled" not in self.events:
                raise AssertionError("sampler started before perf acknowledgement")
            if self.sampler_failure is not None:
                raise self.sampler_failure
            self.events.append(f"sampler-spawned:{args[0]}")
        return FakeCollectorProcess(argv, self.events)

    def run(
        self,
        argv: Sequence[str | Path],
        **kwargs: Any,
    ) -> CompletedProcess[str]:
        args = tuple(str(value) for value in argv)
        if args[:2] == ("perf", "report"):
            return CompletedProcess(args, 0, "profile\n", "")
        return super().run(argv, **kwargs)


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
                    "zerofs_writeback_dirty_ssd_reserved_bytes 5",
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
                    "zerofs_writeback_dirty_ssd_reserved_bytes 0",
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

    def test_modern_write_zeroes_limit_refuses_configured_device_before_mount(
        self,
    ) -> None:
        config = replace(self.config, nbd_device=Path("/dev/nbd7"))
        self.runner.write_zeroes_max_bytes = "4294966784"
        lifecycle = PilotLifecycle(config, self.runner)

        with self.assertRaisesRegex(RuntimeError, r"/dev/nbd7.*4294966784"):
            lifecycle.start()

        self.assertIn(
            (("cat", "/sys/block/nbd7/queue/write_zeroes_max_bytes"), False),
            self.runner.calls,
        )
        self.assertNotIn(
            (("cat", "/sys/block/nbd7/queue/max_write_zeroes_sectors"), False),
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
        # The reset flow backs up the pilot config with a real `cp -a`, which the
        # fake runner executes for real. Keep that inside the temp dir: the default
        # /etc/zerofs/nbd-pilot.toml is root-owned 0600 on a provisioned VM100, so
        # leaving it at the default made these tests pass only on machines where
        # the pilot config happened not to exist.
        config_file = Path(self.temp.name) / "nbd-pilot.toml"
        config_file.write_text(
            '[storage]\nurl = "sftp://pilot@example.test:23/old"\n', encoding="utf-8"
        )
        self.config = PilotConfig.from_mapping(
            root,
            {
                "ZEROFS_PILOT_CONFIG": str(config_file),
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

            def _reset_nbd_module(self) -> None:
                events.append("reset-nbd-module")

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
                "reset-nbd-module",
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

    def test_fresh_reset_reloads_nbd_only_after_every_device_is_detached(self) -> None:
        class ModuleRunner(FakeRunner):
            def run(
                self, argv: Sequence[str | Path], **kwargs: Any
            ) -> CompletedProcess[str]:
                args = tuple(str(value) for value in argv)
                self.calls.append((args, bool(kwargs.get("sudo", False))))
                values = {
                    (
                        "find",
                        "/sys/block",
                        "-maxdepth",
                        "1",
                        "-type",
                        "l",
                        "-name",
                        "nbd*",
                        "-printf",
                        "%f\n",
                    ): "nbd1\nnbd0\n",
                    ("cat", "/sys/module/nbd/parameters/nbds_max"): "16\n",
                    ("cat", "/sys/module/nbd/parameters/max_part"): "31\n",
                    ("cat", "/sys/block/nbd0/size"): "0\n",
                    ("cat", "/sys/block/nbd1/size"): "0\n",
                }
                if args in values:
                    return CompletedProcess(args, 0, values[args], "")
                if args in {
                    ("cat", "/sys/block/nbd0/pid"),
                    ("cat", "/sys/block/nbd1/pid"),
                }:
                    return CompletedProcess(args, 1, "", "No such file")
                return CompletedProcess(args, 0, "", "")

        runner = ModuleRunner()
        resetter = FreshResetter(self.config, runner, object())  # type: ignore[arg-type]

        resetter._reset_nbd_module()

        self.assertEqual(
            runner.calls[-2:],
            [
                (("modprobe", "-r", "nbd"), True),
                (("modprobe", "nbd", "nbds_max=16", "max_part=31"), True),
            ],
        )

    def test_fresh_reset_refuses_to_reload_nbd_with_an_attached_device(self) -> None:
        class AttachedRunner(FakeRunner):
            def run(
                self, argv: Sequence[str | Path], **kwargs: Any
            ) -> CompletedProcess[str]:
                args = tuple(str(value) for value in argv)
                self.calls.append((args, bool(kwargs.get("sudo", False))))
                values = {
                    (
                        "find",
                        "/sys/block",
                        "-maxdepth",
                        "1",
                        "-type",
                        "l",
                        "-name",
                        "nbd*",
                        "-printf",
                        "%f\n",
                    ): "nbd0\n",
                    ("cat", "/sys/module/nbd/parameters/nbds_max"): "16\n",
                    ("cat", "/sys/module/nbd/parameters/max_part"): "31\n",
                    ("cat", "/sys/block/nbd0/size"): "8\n",
                    ("cat", "/sys/block/nbd0/pid"): "1234\n",
                }
                if args in values:
                    return CompletedProcess(args, 0, values[args], "")
                return CompletedProcess(args, 0, "", "")

        runner = AttachedRunner()
        resetter = FreshResetter(self.config, runner, object())  # type: ignore[arg-type]

        with self.assertRaisesRegex(RuntimeError, r"nbd0.*size=8.*pid=1234"):
            resetter._reset_nbd_module()

        self.assertFalse(any(call[0][:1] == ("modprobe",) for call in runner.calls))

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

    def test_benchmark_rejects_completed_byte_counter_regressions(self) -> None:
        with self.assertRaisesRegex(RuntimeError, "counter regressed"):
            _counter_delta(9, 10, "remote encoded bytes")

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

    def test_buffered_read_disables_streaming_fadvise(self) -> None:
        output = Path(self.temp.name) / "buffered-read.json"

        class FioRunner(FakeRunner):
            def run(
                self, argv: Sequence[str | Path], **kwargs: Any
            ) -> CompletedProcess[str]:
                args = tuple(str(value) for value in argv)
                if args[0] == "fio":
                    self.calls.append((args, bool(kwargs.get("sudo", False))))
                    output.write_text(
                        json.dumps(
                            {
                                "jobs": [
                                    {
                                        "error": 0,
                                        "read": {"io_bytes": 4 << 20, "runtime": 1},
                                    }
                                ]
                            }
                        ),
                        encoding="utf-8",
                    )
                    return CompletedProcess(args, 0, "", "")
                return super().run(argv, **kwargs)

        runner = FioRunner()
        benchmark = BenchmarkRunner(
            self.config,
            runner,
            self.lifecycle,  # type: ignore[arg-type]
        )

        benchmark._run_fio(
            name="buffered",
            run_root=self.config.mountpoint / ".zerofs-bench-test",
            per_job_mib=4,
            jobs=1,
            output=output,
            read=True,
            direct=False,
        )

        fio_argv = next(call[0] for call in runner.calls if call[0][0] == "fio")
        self.assertIn("--invalidate=0", fio_argv)
        self.assertIn("--fadvise_hint=0", fio_argv)

    def test_odirect_write_uses_distinct_destructive_filenames(self) -> None:
        output = Path(self.temp.name) / "direct-write.json"

        class FioRunner(FakeRunner):
            def run(
                self, argv: Sequence[str | Path], **kwargs: Any
            ) -> CompletedProcess[str]:
                args = tuple(str(value) for value in argv)
                if args[0] == "fio":
                    self.calls.append((args, bool(kwargs.get("sudo", False))))
                    output.write_text(
                        json.dumps(
                            {
                                "jobs": [
                                    {
                                        "error": 0,
                                        "write": {
                                            "io_bytes": 4 << 20,
                                            "runtime": 2,
                                        },
                                    }
                                ]
                            }
                        ),
                        encoding="utf-8",
                    )
                    return CompletedProcess(args, 0, "", "")
                return super().run(argv, **kwargs)

        runner = FioRunner()
        benchmark = BenchmarkRunner(
            self.config,
            runner,
            self.lifecycle,  # type: ignore[arg-type]
        )
        result = benchmark._run_fio(
            name="zerofs_nbd_odirect_write",
            run_root=self.config.mountpoint / ".zerofs-bench-test",
            filename_format="odirect-write.$jobnum",
            per_job_mib=4,
            jobs=1,
            output=output,
            read=False,
            direct=True,
        )

        fio = next(call[0] for call in runner.calls if call[0][0] == "fio")
        self.assertIn("--filename_format=odirect-write.$jobnum", fio)
        self.assertIn("--direct=1", fio)
        self.assertEqual(result.bytes, 4 << 20)

    def test_odirect_write_captures_accepted_local_and_remote_barriers(self) -> None:
        events: list[str] = []
        before = replace(self.snapshot, accepted=9, local=9, remote=9)
        still_open = before
        accepted = replace(
            before,
            accepted=10,
            dirty_ram=4 << 20,
            local_bytes=1 << 20,
            remote_bytes=1 << 20,
        )
        local = replace(
            accepted,
            local=10,
            dirty_ram=0,
            dirty_ssd_reserved=4 << 20,
            local_bytes=5 << 20,
        )
        remote = replace(
            local,
            remote=10,
            dirty_ssd_reserved=0,
            remote_bytes=5 << 20,
        )

        class SequenceMetrics:
            def __init__(self) -> None:
                self.snapshots = iter((before, still_open, local))

            def snapshot(self) -> WritebackSnapshot:
                snapshot = next(self.snapshots)
                events.append(
                    f"snapshot:{snapshot.accepted}:{snapshot.local}:{snapshot.remote}"
                )
                return snapshot

        class RecordingLifecycle(_HealthyLifecycle):
            def drain(self, timeout: int | None = None) -> object:
                events.append("stable_drain")
                return super().drain(timeout)

        class RecordingRunner(FakeRunner):
            def run(
                self, argv: Sequence[str | Path], **kwargs: Any
            ) -> CompletedProcess[str]:
                if tuple(str(value) for value in argv)[:2] == ("sync", "-f"):
                    events.append("syncfs")
                return super().run(argv, **kwargs)

        class RemoteSampler:
            def wait_for_remote(
                self, target_sequence: int, timeout: float
            ) -> tuple[WritebackSnapshot, int]:
                self_target = target_sequence
                self_timeout = timeout
                events.append(f"sampled_remote:{self_target}:{self_timeout}")
                return remote, 500

        lifecycle = RecordingLifecycle(before, self.config)
        lifecycle.metrics = SequenceMetrics()  # type: ignore[assignment]
        runner = RecordingRunner()
        calls: list[dict[str, object]] = []

        class RecordingBenchmark(BenchmarkRunner):
            def _run_fio(self, *args: object, **kwargs: Any) -> FioResult:
                events.append("fio")
                calls.append(kwargs)
                return FioResult(bytes=4 << 20, runtime_ms=2, mibps=2048.0)

            def _system_io(self, device: tuple[int, int]) -> SystemIoSnapshot:
                return SystemIoSnapshot("sda1", 0, 0, 0, 0, 0, 0, 0)

        benchmark = RecordingBenchmark(
            replace(self.config, drain_timeout=1),
            runner,
            lifecycle,  # type: ignore[arg-type]
        )
        with mock.patch(
            "scripts.vm100_pilot.benchmark.time.monotonic_ns",
            side_effect=(100, 200, 300, 400),
        ):
            phase = benchmark._run_direct_write_tiers(
                run_root=self.config.mountpoint / ".zerofs-bench-test",
                per_job_mib=4,
                jobs=1,
                expected_bytes=4 << 20,
                output=Path(self.temp.name) / "direct-write.json",
                phase_device=(8, 1),
                sampler=RemoteSampler(),  # type: ignore[arg-type]
            )

        self.assertEqual(calls[0]["direct"], True)
        self.assertEqual(calls[0]["filename_format"], "odirect-write.$jobnum")
        self.assertEqual(phase.before.accepted, 9)
        self.assertEqual(phase.accepted.accepted, 10)
        self.assertEqual(phase.local.local, 10)
        self.assertEqual(phase.remote.remote, 10)
        self.assertEqual(
            (
                phase.write_start_ns,
                phase.write_end_ns,
                phase.local_sync_start_ns,
                phase.local_end_ns,
                phase.remote_end_ns,
            ),
            (100, 200, 300, 400, 500),
        )
        self.assertEqual(
            phase.phase_windows(),
            {
                "zerofs_nbd_odirect_write_service_ack": {
                    "start_ns": 100,
                    "end_ns": 200,
                },
                "zerofs_nbd_odirect_local_durability_tail": {
                    "start_ns": 300,
                    "end_ns": 400,
                },
                "zerofs_nbd_odirect_local_durability_end_to_end": {
                    "start_ns": 100,
                    "end_ns": 400,
                },
                "zerofs_nbd_odirect_remote_durability_tail": {
                    "start_ns": 400,
                    "end_ns": 500,
                },
                "zerofs_nbd_odirect_remote_durability_end_to_end": {
                    "start_ns": 100,
                    "end_ns": 500,
                },
            },
        )
        self.assertEqual(
            phase.barrier_receipt(),
            {
                "before_sequence": 9,
                "accepted_sequence": 10,
                "local_sequence": 10,
                "remote_sequence": 10,
                "fio_bytes": 4 << 20,
                "fio_runtime_ms": 2,
                "local_completed_bytes": 4 << 20,
                "remote_completed_bytes": 4 << 20,
            },
        )
        self.assertEqual(
            events,
            [
                # File layout pass: the measured fio below must overwrite
                # pre-sized files in place, or every extending O_DIRECT write
                # pays an XFS journal flush (the full durability barrier).
                "fio",
                "syncfs",
                "stable_drain",
                "syncfs",
                "snapshot:9:9:9",
                "fio",
                "syncfs",
                "snapshot:9:9:9",
                "snapshot:10:10:9",
                "sampled_remote:10:1",
                "stable_drain",
            ],
        )
        self.assertEqual(lifecycle.drain_calls, 2)

    def test_odirect_remote_first_crossing_before_local_has_no_fake_tail(self) -> None:
        snapshot = self.snapshot
        phase = DirectWriteTiers(
            write=FioResult(bytes=4 << 20, runtime_ms=2, mibps=2048.0),
            before=snapshot,
            accepted=replace(snapshot, accepted=10),
            local=replace(snapshot, accepted=10, local=10),
            remote=replace(snapshot, accepted=10, local=10, remote=10),
            write_start_ns=100,
            write_end_ns=200,
            local_sync_start_ns=250,
            local_end_ns=500,
            remote_end_ns=400,
            io_before=SystemIoSnapshot("sda1", 0, 0, 0, 0, 0, 0, 0),
            write_io_after=SystemIoSnapshot("sda1", 0, 0, 0, 0, 0, 0, 0),
            local_io_after=SystemIoSnapshot("sda1", 0, 0, 0, 0, 0, 0, 0),
            remote_io_after=SystemIoSnapshot("sda1", 0, 0, 0, 0, 0, 0, 0),
        )

        windows = phase.phase_windows()

        self.assertEqual(phase.remote_end_ns, 400)
        self.assertNotIn("zerofs_nbd_odirect_remote_durability_tail", windows)
        self.assertEqual(
            windows["zerofs_nbd_odirect_remote_durability_end_to_end"],
            {"start_ns": 100, "end_ns": 400},
        )
        self.assertEqual(phase.remote_tail_ms(), 0)

    def test_benchmark_receipt_keeps_odirect_write_tiers_separate(self) -> None:
        zero_io = SystemIoSnapshot("sda1", 0, 0, 0, 0, 0, 0, 0)
        primary_accepted = replace(self.snapshot, accepted=10)
        primary_local = replace(primary_accepted, local=10, local_bytes=5 << 20)
        direct_before = replace(primary_local, remote=10, remote_bytes=5 << 20)
        direct_accepted = replace(direct_before, accepted=11, dirty_ram=4 << 20)
        direct_local = replace(
            direct_accepted,
            local=11,
            dirty_ram=0,
            dirty_ssd_reserved=4 << 20,
            local_bytes=9 << 20,
        )
        direct_remote = replace(
            direct_local,
            remote=11,
            dirty_ssd_reserved=0,
            remote_bytes=9 << 20,
        )
        direct = DirectWriteTiers(
            write=FioResult(bytes=4 << 20, runtime_ms=4, mibps=1000.0),
            before=direct_before,
            accepted=direct_accepted,
            local=direct_local,
            remote=direct_remote,
            write_start_ns=100,
            write_end_ns=200,
            local_sync_start_ns=300,
            local_end_ns=500,
            remote_end_ns=900,
            io_before=zero_io,
            write_io_after=zero_io,
            local_io_after=zero_io,
            remote_io_after=zero_io,
        )
        direct_calls = 0
        scratch_root = Path(self.temp.name) / "benchmark-tmpfs"
        scratch_root.mkdir()
        fio_outputs: list[Path] = []

        class ReceiptBenchmark(BenchmarkRunner):
            def _wait_clean_gc(
                self, *, maintenance_isolated: bool
            ) -> WritebackSnapshot:
                return self.lifecycle.metrics.snapshot()

            def _local_device(self) -> tuple[int, int]:
                return (8, 1)

            def _system_io(self, device: tuple[int, int]) -> SystemIoSnapshot:
                return zero_io

            def _nbd_io(self) -> BlockIoSnapshot:
                return BlockIoSnapshot("nbd0", 0, 0, 0)

            def _run_fio(self, *args: object, **kwargs: Any) -> FioResult:
                output = Path(kwargs["output"])
                fio_outputs.append(output)
                output.write_text("{}", encoding="utf-8")
                # A runtime the mocked wall clock cannot coincidentally match:
                # every phase here elapses in well under a millisecond, so
                # `millis()` of the wall window would round to 1.
                return FioResult(bytes=4 << 20, runtime_ms=7, mibps=571.429)

            def _run_direct_write_tiers(self, **kwargs: Any) -> DirectWriteTiers:
                nonlocal direct_calls
                direct_calls += 1
                output = Path(kwargs["output"])
                fio_outputs.append(output)
                output.write_text("{}", encoding="utf-8")
                return direct

            def _benchmark_tmpfs_root(self) -> Path:
                return scratch_root

        class FakeSampler:
            def __init__(
                self,
                lifecycle: object,
                output: Path,
                system_io_output: Path,
                local_device: tuple[int, int],
            ) -> None:
                self.output = output
                self.system_io_output = system_io_output
                self.system_io = [zero_io, zero_io]

            def start(self) -> None:
                self.output.write_text(
                    "timestamp_ms,accepted,local,remote,dirty_ram,dirty_ssd_reserved,"
                    "local_bytes,remote_bytes,terminal,gc_active,gc_passes,"
                    "gc_batches,gc_deleted_bytes\n"
                    "100,9,9,9,0,0,1048576,1048576,False,False,1,0,0\n"
                    "200,10,10,10,0,0,5242880,5242880,False,False,1,0,0\n",
                    encoding="utf-8",
                )
                self.system_io_output.write_text("unused\n", encoding="utf-8")

            def stop(self) -> None:
                return None

        benchmark = ReceiptBenchmark(
            self.config,
            self.runner,
            self.lifecycle,  # type: ignore[arg-type]
        )
        with (
            mock.patch("scripts.vm100_pilot.benchmark._MetricSampler", FakeSampler),
            mock.patch(
                "scripts.vm100_pilot.benchmark.wait_for_accepted_after",
                return_value=primary_accepted,
            ),
            mock.patch(
                "scripts.vm100_pilot.benchmark.wait_for_local",
                return_value=primary_local,
            ),
        ):
            result = benchmark.run(total_mib=4, jobs=1)

        manifest = json.loads(
            (Path(result.receipt_dir) / "manifest.json").read_text(encoding="utf-8")
        )
        self.assertEqual(direct_calls, 1)
        self.assertEqual(len(fio_outputs), 6)
        self.assertTrue(all(path.parent.parent == scratch_root for path in fio_outputs))
        self.assertTrue(
            all(not path.exists() for path in fio_outputs),
            "tmpfs benchmark artifacts must be removed after persistence",
        )
        self.assertTrue((Path(result.receipt_dir) / "direct-write-fio.json").is_file())
        # The foreground buffered write is rated on fio's own reported runtime,
        # like its three sibling throughput phases -- not on the wall clock
        # around `sudo fio`, which would charge process startup to the write
        # and leave write and read rates non-comparable in one receipt.
        self.assertEqual(result.user_buffered_page_cache_write_ms, 7)
        self.assertEqual(result.user_buffered_page_cache_write_mibps, 571.429)
        self.assertEqual(result.page_cache_hot_read_ms, 7)
        self.assertEqual(result.zerofs_direct_read_ms, 7)
        # The perf-sliceable phase window stays wall-clock, and the durability
        # end-to-end fields still run from the pre-fio anchor.
        self.assertIn(
            "user_buffered_page_cache_write", manifest["phase_monotonic_ns"]
        )
        self.assertEqual(result.zerofs_nbd_odirect_write_bytes, 4 << 20)
        self.assertEqual(result.zerofs_nbd_odirect_write_service_ack_mibps, 1000.0)
        self.assertEqual(result.zerofs_nbd_odirect_local_durability_end_to_end_ms, 1)
        self.assertEqual(
            manifest["zerofs_nbd_odirect_write_barriers"]["accepted_sequence"],
            11,
        )
        self.assertIn(
            "zerofs_nbd_odirect_write_service_ack",
            manifest["phase_monotonic_ns"],
        )
        self.assertIn(
            "zerofs_nbd_odirect_remote_durability_tail",
            manifest["phase_system_io"],
        )

    def test_local_rate_uses_completed_payload_and_full_interval(self) -> None:
        result = calculate_tiers(
            logical_bytes=1 << 30,
            local_bytes=1 << 30,
            remote_bytes=1 << 30,
            user_buffered_page_cache_write_ms=1000,
            local_end_to_end_ms=4000,
            remote_end_to_end_ms=10000,
            local_active_ms=1000,
            remote_active_ms=5000,
            page_cache_hot_read_ms=2000,
            zerofs_direct_read_ms=500,
        )
        self.assertEqual(result.user_buffered_page_cache_write_mibps, 1024.0)
        self.assertEqual(result.local_mibps, 256.0)
        self.assertEqual(result.remote_mibps, 102.4)
        self.assertEqual(result.local_active_mibps, 1024.0)
        self.assertEqual(result.remote_active_mibps, 204.8)
        self.assertEqual(result.zerofs_direct_read_mibps, 2048.0)
        self.assertEqual(result.zerofs_nbd_odirect_write_bytes, 0)

    def test_result_labels_separate_buffered_and_nbd_odirect_writes(self) -> None:
        result = calculate_tiers(
            logical_bytes=1 << 30,
            local_bytes=1 << 30,
            remote_bytes=1 << 30,
            user_buffered_page_cache_write_ms=100,
            local_end_to_end_ms=1000,
            remote_end_to_end_ms=2000,
            local_active_ms=800,
            remote_active_ms=1000,
            page_cache_hot_read_ms=50,
            zerofs_direct_read_ms=200,
            zerofs_nbd_odirect_write_service_ack_ms=400,
            zerofs_nbd_odirect_write_bytes=1 << 30,
            zerofs_nbd_odirect_local_durability_tail_ms=450,
            zerofs_nbd_odirect_local_durability_end_to_end_ms=900,
            zerofs_nbd_odirect_remote_durability_tail_ms=900,
            zerofs_nbd_odirect_remote_durability_end_to_end_ms=1800,
            zerofs_nbd_odirect_local_encoded_bytes=1 << 30,
            zerofs_nbd_odirect_remote_encoded_bytes=1 << 30,
        )

        self.assertEqual(result.user_buffered_page_cache_write_ms, 100)
        self.assertEqual(result.user_buffered_page_cache_write_mibps, 10_240.0)
        self.assertEqual(result.zerofs_nbd_odirect_write_service_ack_ms, 400)
        self.assertEqual(result.zerofs_nbd_odirect_write_service_ack_mibps, 2560.0)
        # Three decimals, the shared system_io.RATE_DIGITS. This entry point
        # used to round MiB/s to two while every other one used three, so the
        # same transfer read as two different numbers depending on which
        # command wrote the receipt.
        self.assertEqual(
            result.zerofs_nbd_odirect_local_durability_tail_mibps, 2275.556
        )
        self.assertEqual(
            result.zerofs_nbd_odirect_local_durability_end_to_end_mibps, 1137.778
        )
        self.assertEqual(
            result.zerofs_nbd_odirect_remote_durability_tail_mibps, 1137.778
        )
        self.assertEqual(
            result.zerofs_nbd_odirect_remote_durability_end_to_end_mibps, 568.889
        )
        self.assertEqual(
            asdict(result)["user_buffered_page_cache_write_mibps"], 10_240.0
        )
        self.assertNotIn("foreground_ms", asdict(result))
        self.assertNotIn("foreground_mibps", asdict(result))

    def test_fio_result_uses_fio_internal_runtime_and_bytes(self) -> None:
        path = Path(self.temp.name) / "fio.json"
        path.write_text(
            json.dumps(
                {
                    "jobs": [
                        {
                            "error": 0,
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

    def test_page_cache_proof_rejects_regressed_counter(self) -> None:
        """A reset diskstats counter must not be clamped into a fake proof.

        `proven` is `read_bytes == 0`, so clamping a regression with
        `max(0, after - before)` would report a page-cache hit precisely when
        the evidence for one was lost.
        """
        before = BlockIoSnapshot("nbd0", 4096, 2000, 30)
        regressed = BlockIoSnapshot("nbd0", 0, 2000, 30)

        with self.assertRaisesRegex(RuntimeError, "read bytes counter regressed"):
            verify_page_cache_hit(before, regressed)

    def test_fio_result_rejects_reported_io_errors(self) -> None:
        """The benchmark entry point must reject errored fio jobs.

        It is the only fio consumer that does not need `total_ios`, and used to
        skip the `error` counter along with it -- so a run whose jobs reported
        I/O errors still produced a throughput number.
        """
        path = Path(self.temp.name) / "errored-fio.json"
        path.write_text(
            json.dumps(
                {
                    "jobs": [
                        {
                            "error": 5,
                            "write": {"io_bytes": 4 << 20, "runtime": 10},
                        }
                    ]
                }
            ),
            encoding="utf-8",
        )

        with self.assertRaisesRegex(RuntimeError, "I/O errors=5"):
            FioResult.from_json(path, operation="write")

    def test_fio_aggregate_requires_error_counter_without_request_counters(
        self,
    ) -> None:
        path = Path(self.temp.name) / "aggregate-fio.json"
        jobs = [{"write": {"io_bytes": 4 << 20, "runtime": 10, "total_ios": 4}}]

        with self.assertRaisesRegex(ValueError, "no error counter"):
            aggregate_fio_jobs(
                jobs, operation="write", path=path, require_request_counters=False
            )

    def test_fio_aggregate_maxes_runtime_for_concurrent_jobs(self) -> None:
        """fio runs an invocation's jobs concurrently (nothing here stonewalls).

        total_bytes over the longest job's runtime is the concurrent phase rate;
        summing runtimes would understate every multi-job cell.
        """
        path = Path(self.temp.name) / "concurrent-fio.json"
        jobs = [
            {"error": 0, "write": {"io_bytes": 3 << 20, "runtime": 1000}},
            {"error": 0, "write": {"io_bytes": 5 << 20, "runtime": 2000}},
        ]

        aggregate = aggregate_fio_jobs(
            jobs, operation="write", path=path, require_request_counters=False
        )

        self.assertEqual(aggregate.byte_count, 8 << 20)
        self.assertEqual(aggregate.runtime_ms, 2000)
        self.assertEqual(mib_per_second(aggregate.byte_count, 2.0), 4.0)

    def test_every_entry_point_reports_rates_at_one_precision(self) -> None:
        """One MiB/s precision across commands, so receipts are comparable."""
        from scripts.vm100_pilot.protocol_matrix import ProtocolMatrixRunner
        from scripts.vm100_pilot.raw_sftp import _rate as sftp_rate

        byte_count = 1 << 30
        self.assertEqual(RATE_DIGITS, 3)
        # 1024 MiB / 0.45 s = 2275.5555... -- a value that actually exposes the
        # digit count, unlike the round numbers most fixtures use.
        expected = 2275.556
        self.assertEqual(_rate(byte_count, 450), expected)
        self.assertEqual(sftp_rate(byte_count, 450), expected)
        self.assertEqual(
            ProtocolMatrixRunner._rate(byte_count, 450_000_000), expected
        )
        self.assertEqual(mib_per_second(byte_count, 0.45), expected)

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
            "timestamp_ms,accepted,local,remote,dirty_ram,dirty_ssd_reserved,local_bytes,remote_bytes,terminal\n"
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

    def test_metric_sampler_buffers_receipts_until_measurement_stops(self) -> None:
        metrics = Path(self.temp.name) / "metrics.csv"
        system_io = Path(self.temp.name) / "system-io.csv"
        sampler = _MetricSampler(
            self.lifecycle,  # type: ignore[arg-type]
            metrics,
            system_io,
            (8, 1),
        )
        sampler.stop_event.set()

        sampler._run()

        self.assertFalse(metrics.exists())
        self.assertFalse(system_io.exists())
        sampler._persist()
        self.assertTrue(metrics.is_file())
        self.assertTrue(system_io.is_file())
        self.assertIn(
            "dirty_ssd_reserved", metrics.read_text(encoding="utf-8").splitlines()[0]
        )

    def test_benchmark_rejects_a_gc_pass_inside_the_measured_epoch(self) -> None:
        from scripts.vm100_pilot.benchmark import (
            BenchmarkContaminatedError,
            _assert_no_maintenance,
        )

        before = WritebackSnapshot(9, 9, 9, 0, 0, 1, 1, False, False, 4, 8, 10)
        after = replace(before, gc_passes=5, gc_batches=9, gc_deleted_bytes=74)

        with self.assertRaisesRegex(BenchmarkContaminatedError, "segment GC"):
            _assert_no_maintenance(before, after)

    def test_fresh_reset_proceeds_when_the_old_stack_is_unhealthy(self) -> None:
        from scripts.vm100_pilot.reset import FreshResetter

        # A dead stack is precisely when reset-fresh is needed; the
        # pre-capture status must record the failure, not abort the reset.
        captured: dict[str, object] = {}

        class DeadLifecycle:
            def require_vm100(self) -> None:
                pass

            def status(self, **_: object) -> dict[str, object]:
                raise RuntimeError("pilot units are not active: zerofs-nbd-pilot")

        reset = FreshResetter.__new__(FreshResetter)
        reset.lifecycle = DeadLifecycle()

        def stop_after_capture(*_a: object, **_k: object) -> str:
            captured["reached"] = True
            raise SystemExit(0)

        reset._fast_topology = stop_after_capture
        with self.assertRaises(SystemExit):
            reset.run(remote_prefix="fresh-x", confirm_destroy_pilot=True)
        self.assertTrue(captured.get("reached"))

    def test_benchmark_tolerates_an_idle_gc_scan_inside_the_measured_epoch(
        self,
    ) -> None:
        from scripts.vm100_pilot.benchmark import _assert_no_maintenance

        before = WritebackSnapshot(9, 9, 9, 0, 0, 1, 1, False, False, 4, 8, 10)
        # A pass ticked and the scanner is momentarily active, but no batches
        # ran and nothing was deleted: no reclamation work touched the epoch.
        after = replace(before, gc_passes=5, gc_active=True)

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

            def _benchmark_tmpfs_root(self) -> Path:
                return Path(self_temp.name)

        self_temp = self.temp
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

    def test_tmpfs_setup_failure_still_cleans_mount_root(self) -> None:
        missing_tmpfs = Path(self.temp.name) / "missing-tmpfs"

        class MissingTmpfsBenchmark(BenchmarkRunner):
            def _wait_clean_gc(
                self, *, maintenance_isolated: bool
            ) -> WritebackSnapshot:
                return self.lifecycle.metrics.snapshot()

            def _benchmark_tmpfs_root(self) -> Path:
                return missing_tmpfs

        benchmark = MissingTmpfsBenchmark(
            self.config,
            self.runner,
            self.lifecycle,  # type: ignore[arg-type]
        )

        with self.assertRaisesRegex(RuntimeError, "tmpfs root is unavailable"):
            benchmark.run(total_mib=4, jobs=1)

        rm_calls = [
            call for call in self.runner.calls if call[0][:3] == ("rm", "-rf", "--")
        ]
        self.assertEqual(len(rm_calls), 1)

    def test_scratch_cleanup_failure_is_not_silenced(self) -> None:
        scratch_root = Path(self.temp.name) / "benchmark-tmpfs"
        scratch_root.mkdir()

        class FailingBenchmark(BenchmarkRunner):
            def _wait_clean_gc(
                self, *, maintenance_isolated: bool
            ) -> WritebackSnapshot:
                return self.lifecycle.metrics.snapshot()

            def _benchmark_tmpfs_root(self) -> Path:
                return scratch_root

            def _local_device(self) -> tuple[int, int]:
                return (8, 1)

            def _system_io(self, device: tuple[int, int]) -> SystemIoSnapshot:
                return SystemIoSnapshot("sda1", 0, 0, 0, 0, 0, 0, 0)

            def _run_fio(self, *args: object, **kwargs: object) -> FioResult:
                raise CommandError(("fio",), 19, "injected fio failure")

        benchmark = FailingBenchmark(
            self.config,
            self.runner,
            self.lifecycle,  # type: ignore[arg-type]
        )
        cleanup_error = OSError("injected scratch cleanup failure")
        real_rmtree = shutil.rmtree

        def fail_scratch_only(
            path: str | Path, *args: object, **kwargs: object
        ) -> None:
            if Path(path).parent == scratch_root:
                raise cleanup_error
            real_rmtree(path, *args, **kwargs)  # type: ignore[arg-type]

        with (
            mock.patch(
                "scripts.vm100_pilot.benchmark.shutil.rmtree",
                side_effect=fail_scratch_only,
            ),
            self.assertRaisesRegex(
                BenchmarkCleanupError, "benchmark cleanup failed"
            ) as caught,
        ):
            benchmark.run(total_mib=4, jobs=1)

        self.assertTrue(
            "injected fio failure" in str(caught.exception.primary),
        )
        self.assertTrue(
            any(
                "injected scratch cleanup failure" in str(error)
                for error in caught.exception.cleanup_errors
            )
        )


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
            user_buffered_page_cache_write_ms=1,
            local_end_to_end_ms=1,
            remote_end_to_end_ms=1,
            local_active_ms=1,
            remote_active_ms=1,
            page_cache_hot_read_ms=1,
            zerofs_direct_read_ms=1,
            user_buffered_page_cache_write_mibps=4096.0,
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
        binary.parent.mkdir(parents=True, exist_ok=True)
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

    def _fetch_hotpath_runtime(self, _port: int) -> dict[str, object]:
        return profile_module._require_hotpath_runtime(
            getattr(self.lifecycle, "runtime_payload")
        )


class _HotpathEnvironmentRunner(FakeRunner):
    def __init__(self) -> None:
        super().__init__()
        self.hotpath_environment: dict[str, str] = {}
        self.installed_environments: list[dict[str, str]] = []
        self.installed_dropins: list[Path] = []
        self.removed_dropins: list[Path] = []
        self.dropin_directories: set[Path] = set()

    def run(
        self,
        argv: Sequence[str | Path],
        **kwargs: Any,
    ) -> CompletedProcess[str]:
        args = tuple(str(value) for value in argv)
        if args[:2] == ("test", "-d") and args[2].startswith("/run/systemd/"):
            path = Path(args[2])
            return CompletedProcess(
                args, int(path not in self.dropin_directories), "", ""
            )
        if args[:3] == ("install", "-d", "-m") and args[-1].startswith("/run/systemd/"):
            self.calls.append((args, bool(kwargs.get("sudo", False))))
            self.dropin_directories.add(Path(args[-1]))
            return CompletedProcess(args, 0, "", "")
        if args[:1] == ("install",) and args[-1].startswith("/run/systemd/"):
            self.calls.append((args, bool(kwargs.get("sudo", False))))
            self.installed_dropins.append(Path(args[-1]))
            for line in Path(args[-2]).read_text().splitlines():
                if line.startswith("Environment="):
                    assignment = line.removeprefix("Environment=").strip('"')
                    key, value = assignment.split("=", 1)
                    self.hotpath_environment[key] = value
            self.installed_environments.append(dict(self.hotpath_environment))
            return CompletedProcess(args, 0, "", "")
        if args[:3] == ("rm", "-f", "--") and args[3].startswith("/run/systemd/"):
            self.calls.append((args, bool(kwargs.get("sudo", False))))
            path = Path(args[3])
            self.removed_dropins.append(path)
            self.hotpath_environment.clear()
            return CompletedProcess(args, 0, "", "")
        if args[:2] == ("rmdir", "--ignore-fail-on-non-empty"):
            self.calls.append((args, bool(kwargs.get("sudo", False))))
            self.dropin_directories.discard(Path(args[2]))
            return CompletedProcess(args, 0, "", "")
        return super().run(argv, **kwargs)


class _FailingHotpathEnvironmentRunner(_HotpathEnvironmentRunner):
    def __init__(self, fail_stage: str, *, cancellation: bool = False) -> None:
        super().__init__()
        self.fail_stage = fail_stage
        self.cancellation = cancellation
        self.failed = False
        self.fail_cleanup = False

    def run(
        self,
        argv: Sequence[str | Path],
        **kwargs: Any,
    ) -> CompletedProcess[str]:
        args = tuple(str(value) for value in argv)
        result = super().run(argv, **kwargs)
        stage_matches = {
            "directory": args[:3] == ("install", "-d", "-m"),
            "dropin": args[:1] == ("install",) and args[-1].startswith("/run/systemd/"),
            "reload": args[:2] == ("systemctl", "daemon-reload"),
        }
        if not self.failed and stage_matches.get(self.fail_stage, False):
            self.failed = True
            if self.cancellation:
                raise KeyboardInterrupt(f"injected {self.fail_stage} cancellation")
            raise RuntimeError(f"injected {self.fail_stage} failure")
        cleanup_matches = (
            args[:3] == ("rm", "-f", "--")
            or args[:2] == ("rmdir", "--ignore-fail-on-non-empty")
            or args[:2] == ("systemctl", "daemon-reload")
        )
        if self.fail_cleanup and cleanup_matches:
            raise RuntimeError(f"injected cleanup failure: {' '.join(args)}")
        return result


class _BrokenHotpathStagingFile:
    def __init__(self, path: Path, failure: str) -> None:
        self.name = str(path)
        self._handle = path.open("w")
        self.failure = failure

    def write(self, value: str) -> int:
        if self.failure == "write":
            raise OSError("injected staging write failure")
        return self._handle.write(value)

    def flush(self) -> None:
        self._handle.flush()

    def fileno(self) -> int:
        return self._handle.fileno()

    def close(self) -> None:
        self._handle.close()
        if self.failure == "close":
            raise OSError("injected staging close failure")


class _HotpathLifecycle(_HealthyLifecycle):
    def __init__(
        self,
        snapshot: WritebackSnapshot,
        config: PilotConfig,
        runner: _HotpathEnvironmentRunner,
        report: str | None,
        runtime_payload: object | None = None,
        *,
        fail_profile_start: bool = False,
    ) -> None:
        super().__init__(snapshot, config)
        self.runner = runner
        self.report = report
        self.runtime_payload = (
            {
                "num_workers": 1,
                "num_alive_tasks": 0,
                "global_queue_depth": 0,
                "workers": [{"index": 0, "park_count": 1, "busy_duration_ms": 2}],
            }
            if runtime_payload is None
            else runtime_payload
        )
        self.fail_profile_start = fail_profile_start
        self.profile_started = False

    def start(self) -> dict[str, int]:
        if self.fail_profile_start:
            self.fail_profile_start = False
            raise RuntimeError("injected profile start failure")
        self.profile_started = bool(self.runner.hotpath_environment)
        return super().start()

    def stop(self) -> None:
        if self.profile_started:
            output = self.runner.hotpath_environment.get("HOTPATH_OUTPUT_PATH")
            if output is not None and self.report is not None:
                Path(output).write_text(self.report)
            self.profile_started = False
        super().stop()


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
        self.snapshot = WritebackSnapshot(9, 9, 9, 0, 0, 1, 1, False)
        self.runner = _HotpathEnvironmentRunner()
        self.lifecycle = _HotpathLifecycle(
            self.snapshot,
            self.config,
            self.runner,
            _VALID_HOTPATH_REPORT,
        )

    def _hotpath_profiler(
        self,
        report: str | None,
        *,
        fail_profile_start: bool = False,
        benchmark: Any = None,
        runtime_payload: object | None = None,
    ) -> tuple[_TestProfileRunner, _HotpathEnvironmentRunner, _HotpathLifecycle]:
        runner = _HotpathEnvironmentRunner()
        lifecycle = _HotpathLifecycle(
            self.snapshot,
            self.config,
            runner,
            report,
            runtime_payload,
            fail_profile_start=fail_profile_start,
        )
        profiler = _TestProfileRunner(
            self.config,
            runner,
            lifecycle,  # type: ignore[arg-type]
            benchmark or _ProfileBenchmark(self.config, self.config_file),
        )
        return profiler, runner, lifecycle

    def _hotpath_environment(
        self,
        runner: _HotpathEnvironmentRunner,
        *,
        report: Path | None = None,
    ) -> _TemporaryHotpathEnvironment:
        return _TemporaryHotpathEnvironment(
            self.config,
            runner,
            report or Path(self.temp.name) / "hotpath.json",
            38473,
        )

    def test_profile_build_forces_frame_pointers_for_actionable_callchains(
        self,
    ) -> None:
        class ProfileBuildRunner(FakeRunner):
            def __init__(self, binary: Path) -> None:
                super().__init__()
                self.binary = binary
                self.build_env: Mapping[str, str] | None = None
                self.build_args: tuple[str, ...] | None = None

            def run(
                self,
                argv: Sequence[str | Path],
                **kwargs: Any,
            ) -> CompletedProcess[str]:
                args = tuple(str(value) for value in argv)
                if args[:2] == (str(self_config.cargo), "build"):
                    self.build_args = args
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
        self.assertIn("--cfg tokio_unstable", runner.build_env["RUSTFLAGS"])
        self.assertIn("--cfg io_uring_skip_arch_check", runner.build_env["RUSTFLAGS"])
        self.assertEqual(
            runner.build_args,
            (
                str(self.config.cargo),
                "build",
                "--release",
                "--locked",
                "--features",
                "hotpath-profile",
            ),
        )

    def test_profile_retains_unique_static_json_hotpath_report(self) -> None:
        profiler, runner, _lifecycle = self._hotpath_profiler(_VALID_HOTPATH_REPORT)

        result = profiler.run(total_mib=4, jobs=1)

        report = Path(result.receipt_dir) / "hotpath.json"
        manifest = json.loads((Path(result.receipt_dir) / "manifest.json").read_text())
        self.assertEqual(manifest["artifacts"]["hotpath.json"], str(report))
        self.assertEqual(json.loads(report.read_text())["type"], "hotpath_report")
        self.assertEqual(
            runner.hotpath_environment,
            {},
        )
        self.assertEqual(len(runner.installed_environments), 1)
        environment = runner.installed_environments[0]
        self.assertEqual(
            {key: value for key, value in environment.items() if key != "HOTPATH_METRICS_PORT"},
            {
                "HOTPATH_OUTPUT_PATH": str(report),
                "HOTPATH_OUTPUT_FORMAT": "json",
                "HOTPATH_METRICS_SERVER_OFF": "false",
                "HOTPATH_REPORT": "functions-timing,futures,threads",
                "HOTPATH_CPU_BASELINE_OFF": "true",
            },
        )
        self.assertTrue(environment["HOTPATH_METRICS_PORT"].isdigit())
        runtime = Path(result.receipt_dir) / "hotpath-tokio-runtime.json"
        self.assertEqual(manifest["artifacts"]["hotpath-tokio-runtime.json"], str(runtime))
        self.assertEqual(json.loads(runtime.read_text())["num_workers"], 1)
        self.assertEqual(len(runner.installed_dropins), 1)
        self.assertEqual(runner.removed_dropins, runner.installed_dropins)
        installed = runner.installed_dropins[0]
        self.assertTrue(installed.name.startswith("zerofs-hotpath-profile-"))
        self.assertGreaterEqual(
            sum(call[0] == ("systemctl", "daemon-reload") for call in runner.calls),
            2,
        )

    def test_profile_fails_closed_for_missing_empty_or_invalid_hotpath_report(
        self,
    ) -> None:
        for index, (report, message) in enumerate(
            (
                (None, "missing or empty"),
                ("", "missing or empty"),
                ("not-json", "invalid JSON"),
                (
                    json.dumps(
                        {
                            "type": "hotpath_report",
                            "functions_timing": {},
                            "futures": {},
                            "threads": {},
                            "streams": {},
                        }
                    ),
                    "forbidden sections",
                ),
            )
        ):
            with self.subTest(report=report):
                profiler, runner, lifecycle = self._hotpath_profiler(report)
                profiler.config = replace(
                    profiler.config,
                    result_dir=Path(self.temp.name) / f"results-hotpath-{index}",
                )

                with self.assertRaisesRegex(RuntimeError, message):
                    profiler.run(total_mib=4, jobs=1)

                self.assertEqual(runner.hotpath_environment, {})
                self.assertEqual(runner.removed_dropins, runner.installed_dropins)
                manifest = json.loads(
                    next(profiler.config.result_dir.glob("*/manifest.json")).read_text()
                )
                self.assertNotIn("hotpath.json", manifest["artifacts"])
                self.assertEqual(self.config_file.read_bytes(), self.original_config)
                self.assertEqual(self.binary.read_bytes(), b"canonical-binary")
                self.assertGreaterEqual(lifecycle.start_calls, 2)

    def test_profile_fails_closed_for_missing_or_invalid_hotpath_runtime(self) -> None:
        for index, payload in enumerate(({}, {"num_workers": "bad", "workers": []})):
            with self.subTest(payload=payload):
                profiler, _runner, _lifecycle = self._hotpath_profiler(
                    _VALID_HOTPATH_REPORT,
                    runtime_payload=payload,
                )
                profiler.config = replace(
                    profiler.config,
                    result_dir=Path(self.temp.name) / f"results-runtime-{index}",
                )
                with self.assertRaisesRegex(RuntimeError, "Tokio runtime"):
                    profiler.run(total_mib=4, jobs=1)
                manifest = json.loads(
                    next(profiler.config.result_dir.glob("*/manifest.json")).read_text()
                )
                self.assertNotIn("hotpath-tokio-runtime.json", manifest["artifacts"])
                self.assertEqual(self.config_file.read_bytes(), self.original_config)
                self.assertEqual(self.binary.read_bytes(), b"canonical-binary")

    def test_profile_hotpath_environment_is_removed_on_cancellation_and_start_failure(
        self,
    ) -> None:
        cancelled, cancelled_runner, _ = self._hotpath_profiler(
            _VALID_HOTPATH_REPORT,
            benchmark=_ProfileBenchmark(
                self.config,
                self.config_file,
                KeyboardInterrupt("injected cancellation"),
            ),
        )
        with self.assertRaisesRegex(KeyboardInterrupt, "injected cancellation"):
            cancelled.run(total_mib=4, jobs=1)
        self.assertEqual(len(cancelled_runner.installed_dropins), 1)
        self.assertEqual(
            cancelled_runner.removed_dropins, cancelled_runner.installed_dropins
        )
        self.assertEqual(self.config_file.read_bytes(), self.original_config)
        self.assertEqual(self.binary.read_bytes(), b"canonical-binary")

        starter, start_runner, _ = self._hotpath_profiler(
            None,
            fail_profile_start=True,
        )
        starter.config = replace(
            starter.config,
            result_dir=Path(self.temp.name) / "results-hotpath-start-failure",
        )
        with self.assertRaisesRegex(RuntimeError, "injected profile start failure"):
            starter.run(total_mib=4, jobs=1)
        self.assertEqual(len(start_runner.installed_dropins), 1)
        self.assertEqual(start_runner.removed_dropins, start_runner.installed_dropins)
        self.assertEqual(start_runner.hotpath_environment, {})
        self.assertEqual(self.config_file.read_bytes(), self.original_config)
        self.assertEqual(self.binary.read_bytes(), b"canonical-binary")
        manifest = json.loads(
            next(starter.config.result_dir.glob("*/manifest.json")).read_text()
        )
        self.assertNotIn("hotpath.json", manifest["artifacts"])

    def test_hotpath_environment_partial_install_cleanup_is_cancellation_safe(
        self,
    ) -> None:
        for stage in ("directory", "dropin", "reload"):
            with self.subTest(stage=stage):
                cancellation = stage == "directory"
                runner = _FailingHotpathEnvironmentRunner(
                    stage, cancellation=cancellation
                )
                environment = self._hotpath_environment(runner)

                expected = KeyboardInterrupt if cancellation else RuntimeError
                outcome = "cancellation" if cancellation else "failure"
                with self.assertRaisesRegex(expected, f"injected {stage} {outcome}"):
                    environment.install()
                environment.cleanup()

                self.assertEqual(runner.removed_dropins, [environment.dropin])
                self.assertNotIn(environment.directory, runner.dropin_directories)
                self.assertEqual(runner.hotpath_environment, {})
                self.assertGreaterEqual(
                    sum(
                        call[0] == ("systemctl", "daemon-reload")
                        for call in runner.calls
                    ),
                    1,
                )

        runner = _HotpathEnvironmentRunner()
        environment = self._hotpath_environment(runner)
        with (
            mock.patch.object(
                profile_module.tempfile,
                "NamedTemporaryFile",
                side_effect=OSError("injected tempfile failure"),
            ),
            self.assertRaisesRegex(OSError, "injected tempfile failure"),
        ):
            environment.install()
        environment.cleanup()
        self.assertEqual(runner.removed_dropins, [environment.dropin])
        self.assertNotIn(environment.directory, runner.dropin_directories)

    def test_hotpath_environment_cleanup_aggregates_all_failures(self) -> None:
        runner = _FailingHotpathEnvironmentRunner("never")
        environment = self._hotpath_environment(runner)
        environment.install()
        runner.fail_cleanup = True

        with self.assertRaisesRegex(
            RuntimeError,
            "Hotpath environment cleanup failures:.*rm.*rmdir.*daemon-reload",
        ):
            environment.cleanup()

        self.assertEqual(runner.removed_dropins, [environment.dropin])
        self.assertNotIn(environment.directory, runner.dropin_directories)
        self.assertEqual(runner.hotpath_environment, {})

    def test_hotpath_environment_staging_write_and_close_failures_leave_no_file(
        self,
    ) -> None:
        config = replace(self.config, temp_dir=Path(self.temp.name))
        for failure in ("write", "close"):
            with self.subTest(failure=failure):
                runner = _HotpathEnvironmentRunner()
                environment = _TemporaryHotpathEnvironment(
                    config,
                    runner,
                    Path(self.temp.name) / "hotpath.json",
                    38473,
                )
                staging = Path(self.temp.name) / f"zerofs-hotpath-profile-{failure}"
                broken = _BrokenHotpathStagingFile(staging, failure)
                with (
                    mock.patch.object(
                        profile_module.tempfile,
                        "NamedTemporaryFile",
                        return_value=broken,
                    ),
                    self.assertRaisesRegex(OSError, f"staging {failure} failure"),
                ):
                    environment.install()
                environment.cleanup()
                self.assertFalse(staging.exists())
                self.assertEqual(
                    list(Path(self.temp.name).glob("zerofs-hotpath-profile-*")), []
                )

    def test_profile_restores_canonical_deployment_after_hotpath_install_failure(
        self,
    ) -> None:
        runner = _FailingHotpathEnvironmentRunner("reload")
        lifecycle = _HotpathLifecycle(self.snapshot, self.config, runner, None)
        profiler = _TestProfileRunner(
            self.config,
            runner,
            lifecycle,  # type: ignore[arg-type]
            _ProfileBenchmark(self.config, self.config_file),
        )

        with self.assertRaisesRegex(RuntimeError, "injected reload failure"):
            profiler.run(total_mib=4, jobs=1)

        self.assertEqual(runner.removed_dropins, runner.installed_dropins)
        self.assertEqual(self.config_file.read_bytes(), self.original_config)
        self.assertEqual(self.binary.read_bytes(), b"canonical-binary")

    def test_hotpath_environment_rejects_unsafe_report_paths_before_mutation(
        self,
    ) -> None:
        for suffix in ('quote"', "backslash\\", "newline\n", "percent%"):
            with self.subTest(suffix=suffix):
                runner = _HotpathEnvironmentRunner()
                with self.assertRaisesRegex(ValueError, "unsafe Hotpath report path"):
                    self._hotpath_environment(
                        runner,
                        report=Path(self.temp.name) / suffix / "hotpath.json",
                    )
                self.assertEqual(runner.calls, [])

        runner = _HotpathEnvironmentRunner()
        report = Path(self.temp.name) / "receipt with space" / "hotpath.json"
        environment = self._hotpath_environment(runner, report=report)
        environment.install()
        environment.cleanup()
        self.assertEqual(
            runner.installed_environments[0]["HOTPATH_OUTPUT_PATH"], str(report)
        )

    def test_phase_perf_report_uses_exact_monotonic_window(self) -> None:
        argv = _phase_perf_report_argv(
            Path("/tmp/perf.data"), 1_000_000_001, 2_500_000_009
        )

        self.assertEqual(argv[argv.index("--time") + 1], "1.000000001,2.500000009")
        self.assertEqual(argv[-2:], ["-i", Path("/tmp/perf.data")])

    def test_perf_reports_use_flat_symbols_without_rebuilding_callgraphs(self) -> None:
        aggregate = _perf_report_argv(Path("/tmp/perf.data"))
        phase = _phase_perf_report_argv(
            Path("/tmp/perf.data"), 1_000_000_001, 2_500_000_009
        )

        for argv in (aggregate, phase):
            self.assertEqual(argv[argv.index("-g") + 1], "none")
            self.assertEqual(argv[argv.index("--percent-limit") + 1], "0.1")

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

    def test_perf_record_starts_disabled_with_acknowledged_fifo_control(self) -> None:
        argv = _perf_record_argv(
            123,
            Path("/tmp/perf.data"),
            Path("/tmp/perf-control.fifo"),
            Path("/tmp/perf-ack.fifo"),
        )

        self.assertEqual(argv[argv.index("--clockid") + 1], "monotonic")
        self.assertEqual(argv[argv.index("--call-graph") + 1], "fp")
        self.assertIn("--timestamp", argv)
        self.assertIn("--delay=-1", argv)
        self.assertEqual(
            argv[argv.index("--control") + 1],
            "fifo:/tmp/perf-control.fifo,/tmp/perf-ack.fifo",
        )

    def test_perf_control_waits_for_enable_ack_and_cleans_fifos(self) -> None:
        control = profile_module._PerfControlFifos.create(Path(self.temp.name))
        observed: list[str] = []

        def acknowledge() -> None:
            with control.control_fifo.open(encoding="utf-8") as source:
                observed.append(source.readline())
            with control.ack_fifo.open("w", encoding="utf-8") as sink:
                sink.write("ack\n")

        worker = threading.Thread(target=acknowledge)
        worker.start()
        try:
            control.enable(timeout=2)
        finally:
            control.close()
            worker.join(timeout=2)

        self.assertEqual(observed, ["enable\n"])
        self.assertFalse(worker.is_alive())
        self.assertFalse(control.control_fifo.exists())
        self.assertFalse(control.ack_fifo.exists())

    def test_perf_control_timeout_cleans_fifos(self) -> None:
        control = profile_module._PerfControlFifos.create(Path(self.temp.name))
        self.addCleanup(control.close)

        with (
            mock.patch.object(
                profile_module.select,
                "select",
                return_value=([], [], []),
            ),
            self.assertRaisesRegex(TimeoutError, "did not acknowledge"),
        ):
            control.enable(timeout=2)

        self.assertFalse(control.control_fifo.exists())
        self.assertFalse(control.ack_fifo.exists())

    def test_perf_control_cancellation_cleans_fifos(self) -> None:
        control = profile_module._PerfControlFifos.create(Path(self.temp.name))
        self.addCleanup(control.close)

        with (
            mock.patch.object(
                profile_module.select,
                "select",
                side_effect=KeyboardInterrupt("cancelled"),
            ),
            self.assertRaisesRegex(KeyboardInterrupt, "cancelled"),
        ):
            control.enable(timeout=2)

        self.assertFalse(control.control_fifo.exists())
        self.assertFalse(control.ack_fifo.exists())

    def test_collectors_wait_for_perf_ack_before_starting_other_samplers(
        self,
    ) -> None:
        events: list[str] = []
        control = mock.Mock()
        control.control_fifo = Path(self.temp.name) / "perf-control.fifo"
        control.ack_fifo = Path(self.temp.name) / "perf-control-ack.fifo"
        control.enable.side_effect = lambda **_kwargs: events.append("enabled")
        receipt = RunReceipt.start(self.config, "profile-readiness")

        with mock.patch.object(
            profile_module._PerfControlFifos,
            "create",
            return_value=control,
        ):
            collectors = profile_module.CollectorGroup(
                CollectorRunner(events), receipt, pid=123
            )
            collectors.stop()

        self.assertLess(events.index("record-spawned"), events.index("enabled"))
        self.assertLess(events.index("enabled"), events.index("sampler-spawned:perf"))
        control.close.assert_called_once_with()

    def test_collector_start_failure_or_cancellation_cleans_up(
        self,
    ) -> None:
        cases = (
            (RuntimeError("sampler startup failure"), None),
            (KeyboardInterrupt("cancelled"), KeyboardInterrupt("cancelled")),
        )
        for expected, enable_failure in cases:
            with self.subTest(expected=type(expected).__name__):
                events: list[str] = []
                sampler_failure = expected if enable_failure is None else None
                runner = CollectorRunner(events, sampler_failure)
                control = mock.Mock()
                control.control_fifo = Path(self.temp.name) / "perf-control.fifo"
                control.ack_fifo = Path(self.temp.name) / "perf-control-ack.fifo"
                if enable_failure is None:
                    control.enable.side_effect = lambda **_kwargs: events.append(
                        "enabled"
                    )
                else:
                    control.enable.side_effect = enable_failure
                receipt = RunReceipt.start(
                    self.config, f"profile-start-{type(expected).__name__}"
                )

                with (
                    mock.patch.object(
                        profile_module._PerfControlFifos,
                        "create",
                        return_value=control,
                    ),
                    self.assertRaisesRegex(type(expected), str(expected)),
                ):
                    profile_module.CollectorGroup(runner, receipt, pid=123)

                self.assertIn("interrupt", events)
                control.close.assert_called_once_with()

    def test_collector_stop_error_still_cleans_control_fifos(self) -> None:
        receipt = RunReceipt.start(self.config, "profile-stop-failure")
        receipt.path("perf.data").write_bytes(b"perf-data")
        broken_handle = mock.Mock()
        broken_handle.close.side_effect = OSError("injected close failure")
        control = mock.Mock()
        collectors = profile_module.CollectorGroup.__new__(
            profile_module.CollectorGroup
        )
        collectors.runner = CollectorRunner([])
        collectors.receipt = receipt
        collectors.pid = 123
        collectors.processes = []
        collectors.handles = [broken_handle]
        collectors.perf_control = control
        collectors._stopped = False

        with self.assertRaisesRegex(RuntimeError, "collector cleanup failures"):
            collectors.stop()

        control.close.assert_called_once_with()

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
                user_buffered_page_cache_write_ms=1,
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
            def _identify_binaries(
                self, stock_ssh: Path, hpn_ssh: Path
            ) -> tuple[SshBinaryIdentity, SshBinaryIdentity]:
                del stock_ssh, hpn_ssh
                return (
                    SshBinaryIdentity("/stock/ssh", "OpenSSH_stock", "a" * 64),
                    SshBinaryIdentity("/hpn/ssh", "OpenSSH_hpn", "b" * 64),
                )

            def _create_sources(
                self,
                scratch: Path,
                *,
                jobs: int,
                per_job_bytes: int,
            ) -> list[Path]:
                paths = [scratch / f"source-{index}.bin" for index in range(jobs)]
                for path in paths:
                    path.write_bytes(b"x" * per_job_bytes)
                return paths

            def _endpoint(self) -> SftpEndpoint:
                return SftpEndpoint(
                    "user", "example.invalid", 23, Path("/key"), Path("/known")
                )

            def _endpoint_authority(
                self, endpoint: SftpEndpoint
            ) -> SftpEndpointAuthority:
                return SftpEndpointAuthority(
                    endpoint.user,
                    endpoint.host,
                    endpoint.port,
                    endpoint.prefix,
                    "/key",
                    "/known",
                    "c" * 64,
                    "strict-pinned-known-hosts",
                )

            def _run_batch(
                self, *args: object, **kwargs: object
            ) -> CompletedProcess[str]:
                return CompletedProcess(("sftp",), 0, "", "")

            def _parallel_batches(self, *args: object, **kwargs: object) -> None:
                raise RuntimeError("injected raw transfer failure")

        raw = FailingRaw(self.config, self.runner, self.lifecycle)  # type: ignore[arg-type]
        scenario = RawSftpScenario(
            "raw-sftp-test",
            "failure-path SFTP control",
            jobs=2,
            per_job_bytes=1_048_576,
            buffer_bytes=1_048_576,
            request_depth=128,
            repetitions=4,
        )
        with self.assertRaisesRegex(RuntimeError, "injected raw transfer failure"):
            raw.run(
                scenario,
                stock_ssh=Path("/stock/ssh"),
                hpn_ssh=Path("/hpn/ssh"),
            )
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
