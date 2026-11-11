"""Exercise smoke-runner diagnostics without starting ZeroFS or mounting storage."""

import contextlib
from datetime import datetime
import hashlib
import importlib.util
import io
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest import mock


SCRIPT = Path(__file__).resolve().parents[1] / "test-local-storage-pool.py"
SPEC = importlib.util.spec_from_file_location("local_storage_pool_smoke", SCRIPT)
SMOKE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(SMOKE)


class LocalStoragePoolDiagnosticsTests(unittest.TestCase):
    def run_main(self, *, health=False, receipt=None):
        arguments = [str(SCRIPT), "--binary", sys.executable]
        if health:
            arguments.append("--health-recovery")
        if receipt is not None:
            arguments.extend(("--receipt", str(receipt)))
        stdout, stderr = io.StringIO(), io.StringIO()
        with mock.patch.object(sys, "argv", arguments), \
                contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
            result = SMOKE.main()
        return result, stdout.getvalue(), stderr.getvalue()

    def test_nested_loss_failures_print_logs_before_cleanup(self):
        for failed_scenario in ("loss", "full"):
            with self.subTest(scenario=failed_scenario):
                roots = []

                def health(_binary, root):
                    roots.append(root)
                    (root / "server-health.log").write_text("health diagnostic", encoding="utf-8")
                    return {"result": "passed"}

                def loss(_binary, root, no_space=False):
                    root.mkdir()
                    (root / "server-loss-recovery.log").write_text(
                        f"{root.name} diagnostic", encoding="utf-8"
                    )
                    if root.name == failed_scenario:
                        raise SMOKE.SmokeFailure(f"{failed_scenario} failed")
                    return {}

                with mock.patch.object(SMOKE, "health_smoke", side_effect=health), \
                        mock.patch.object(SMOKE, "loss_smoke", side_effect=loss):
                    result, stdout, stderr = self.run_main(health=True)
                self.assertEqual(result, 1)
                self.assertEqual(stdout, "")
                self.assertIn("server-health.log:\nhealth diagnostic", stderr)
                for scenario in (("loss",) if failed_scenario == "loss" else ("loss", "full")):
                    label = Path(scenario) / "server-loss-recovery.log"
                    self.assertIn(f"{label}:\n{scenario} diagnostic", stderr)
                self.assertIn(f"local storage pool smoke test failed: {failed_scenario} failed", stderr)
                self.assertFalse(roots[0].exists(), "owned temporary storage must still be cleaned up")

    def test_top_level_failure_keeps_bounded_decodable_tail(self):
        roots = []

        def fail(_binary, root):
            roots.append(root)
            (root / "server-before-migration.log").write_bytes(
                b"discarded-prefix" + b"x" * 6000 + b"\xffdiagnostic-tail"
            )
            raise SMOKE.SmokeFailure("original failure")

        with mock.patch.object(SMOKE, "smoke", side_effect=fail):
            result, stdout, stderr = self.run_main()
        self.assertEqual(result, 1)
        self.assertEqual(stdout, "")
        self.assertIn("server-before-migration.log:\n", stderr)
        self.assertIn("\ufffddiagnostic-tail", stderr)
        self.assertNotIn("discarded-prefix", stderr)
        self.assertIn("local storage pool smoke test failed: original failure", stderr)
        self.assertLess(len(stderr), 6200)
        self.assertFalse(roots[0].exists())

    def test_success_keeps_receipt_without_failure_diagnostics(self):
        with mock.patch.object(SMOKE, "smoke", return_value={"result": "passed"}):
            result, stdout, stderr = self.run_main()
        self.assertEqual(result, 0)
        self.assertTrue(stdout.startswith("LOCAL_STORAGE_POOL_SMOKE "))
        receipt = json.loads(stdout.removeprefix("LOCAL_STORAGE_POOL_SMOKE "))
        self.assertEqual(receipt["result"], "passed")
        self.assertEqual(receipt["cut_date"], "2026-11-06")
        self.assertEqual(receipt["binary_sha256"], hashlib.sha256(Path(sys.executable).read_bytes()).hexdigest())
        self.assertLessEqual(datetime.fromisoformat(receipt["started_at"]),
                             datetime.fromisoformat(receipt["completed_at"]))
        self.assertEqual(stderr, "")

    def test_failed_rerun_replaces_stale_success_receipt(self):
        with tempfile.TemporaryDirectory() as directory:
            receipt = Path(directory) / "result.json"
            receipt.write_text('{"result":"passed"}')
            with mock.patch.object(SMOKE, "smoke", side_effect=SMOKE.SmokeFailure("copy failed")):
                result, stdout, stderr = self.run_main(receipt=receipt)
            evidence = json.loads(receipt.read_text())
            self.assertEqual((result, stdout, evidence["result"]), (1, "", "failed"))
            self.assertEqual(evidence["error"], "copy failed")
            self.assertEqual(evidence["cut_date"], "2026-11-06")
            self.assertIn("binary_sha256", evidence)
            self.assertIn("copy failed", stderr)
            self.assertEqual(list(Path(directory).iterdir()), [receipt])

    def test_changed_binary_cannot_receive_a_passing_receipt(self):
        with tempfile.TemporaryDirectory() as directory:
            receipt = Path(directory) / "nested" / "result.json"
            with mock.patch.object(SMOKE, "smoke", return_value={"result": "passed"}), \
                    mock.patch.object(SMOKE, "file_sha256", side_effect=["before", "after"]):
                result, stdout, stderr = self.run_main(receipt=receipt)
            evidence = json.loads(receipt.read_text())
            self.assertEqual((result, stdout, evidence["result"]), (1, "", "failed"))
            self.assertEqual(evidence["binary_sha256"], "before")
            self.assertIn("binary changed", evidence["error"])
            self.assertIn("binary changed", stderr)


if __name__ == "__main__":
    unittest.main()
