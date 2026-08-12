from __future__ import annotations

import json
import os
import tempfile
import unittest
from pathlib import Path

from scripts.vm100_pilot.config import PilotConfig
from scripts.vm100_pilot.receipts import RunReceipt


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


if __name__ == "__main__":
    unittest.main()
