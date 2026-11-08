"""Exercise smoke-runner diagnostics without starting ZeroFS or mounting storage."""

import contextlib
import importlib.util
import io
from pathlib import Path
import sys
import unittest
from unittest import mock


SCRIPT = Path(__file__).resolve().parents[1] / "test-local-storage-pool.py"
SPEC = importlib.util.spec_from_file_location("local_storage_pool_smoke", SCRIPT)
SMOKE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(SMOKE)


class LocalStoragePoolDiagnosticsTests(unittest.TestCase):
    def run_main(self, *, health=False):
        arguments = [str(SCRIPT), "--binary", sys.executable]
        if health:
            arguments.append("--health-recovery")
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
        self.assertEqual(stdout, 'LOCAL_STORAGE_POOL_SMOKE {"result": "passed"}\n')
        self.assertEqual(stderr, "")


if __name__ == "__main__":
    unittest.main()
