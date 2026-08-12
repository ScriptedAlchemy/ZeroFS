from __future__ import annotations

import json
import os
import tempfile
import time
import unittest
from dataclasses import replace
from pathlib import Path
from subprocess import CompletedProcess

from scripts.vm100_pilot.config import PilotConfig
from scripts.vm100_pilot.lifecycle import PilotLifecycle
from scripts.vm100_pilot.metrics import (
    TerminalWritebackError,
    WritebackSnapshot,
    wait_for_drain,
)
from scripts.vm100_pilot.receipts import RunReceipt
from scripts.vm100_pilot.runner import CommandError


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
        **_: object,
    ) -> CompletedProcess[str]:
        args = tuple(str(value) for value in argv)
        self.calls.append((args, sudo))
        if args == ("hostname",):
            return CompletedProcess(args, 0, "ubuntu-main\n", "")
        if args[:3] == ("findmnt", "-rn", "-M"):
            return CompletedProcess(args, 1, "", "")
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
            return CompletedProcess(args, 0 if state == "active" else 3, state + "\n", "")
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
        with self.assertRaisesRegex(RuntimeError, "boom"):
            with RunReceipt.start(config, "benchmark") as receipt:
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

    def test_stop_is_dependency_ordered(self) -> None:
        self.runner.active.update(
            (self.config.service, self.config.client_service, self.config.mount_unit)
        )
        self.lifecycle.stop()
        stops = [call[0][3] for call in self.runner.calls if call[0][:3] == ("systemctl", "stop", "--no-block")]
        self.assertEqual(
            stops,
            [self.config.mount_unit, self.config.client_service, self.config.service],
        )

    def test_start_unwinds_only_started_layers(self) -> None:
        self.runner.fail_start = self.config.client_service
        with self.assertRaisesRegex(CommandError, "injected start failure"):
            self.lifecycle.start()
        stops = [call[0][3] for call in self.runner.calls if call[0][:3] == ("systemctl", "stop", "--no-block")]
        self.assertEqual(stops, [self.config.service])
        self.assertNotIn(self.config.service, self.runner.active)


if __name__ == "__main__":
    unittest.main()
