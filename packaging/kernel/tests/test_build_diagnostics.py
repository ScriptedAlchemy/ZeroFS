import os
import subprocess
import tempfile
import unittest
from pathlib import Path


COLLECTOR = Path(__file__).resolve().parents[1] / "collect-build-diagnostics.sh"


class BuildDiagnosticsTest(unittest.TestCase):
    def collect(self, kernel_log, status=0):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            sudo = root / "sudo"
            sudo.write_text(
                '#!/bin/sh\n'
                '[ "$*" = "-n dmesg --color=never" ] || exit 99\n'
                'printf "%s\\n" "$TEST_KERNEL_LOG"\n'
                'exit "$TEST_KERNEL_STATUS"\n',
                encoding="utf-8",
            )
            sudo.chmod(0o755)
            output = root / "artifacts"
            result = subprocess.run(
                ["bash", str(COLLECTOR), str(output)],
                env={
                    **os.environ,
                    "PATH": str(root) + os.pathsep + os.environ["PATH"],
                    "TEST_KERNEL_LOG": kernel_log,
                    "TEST_KERNEL_STATUS": str(status),
                    "DO_NOT_CAPTURE_SECRET": "private-environment-value",
                },
                text=True,
                capture_output=True,
                check=False,
            )
            self.assertEqual(result.returncode, 0, result.stderr)
            report = (output / "build-failure-diagnostics.txt").read_text()
            self.assertNotIn("private-environment-value", report)
            return report

    def test_retains_oom_records_without_unrelated_kernel_logs(self):
        report = self.collect(
            "[10.0] unrelated private-kernel-value\n"
            "[11.0] oom-kill:constraint=CONSTRAINT_MEMCG,task=apt-get\n"
            "[11.1] Memory cgroup out of memory: Killed process 2730 (apt-get)\n"
            "[11.2] Out of memory: Killed process 2731 (cc1)\n"
        )
        self.assertIn("task=apt-get", report)
        self.assertIn("Killed process 2730", report)
        self.assertIn("Killed process 2731", report)
        self.assertNotIn("private-kernel-value", report)
        self.assertIn("MemTotal:", report)

    def test_missing_records_do_not_claim_no_oom(self):
        report = self.collect("[10.0] ordinary kernel log")
        self.assertIn("does not rule out OOM", report)
        self.assertNotIn("ordinary kernel log", report)

    def test_unavailable_kernel_log_is_explicit(self):
        report = self.collect("restricted-kernel-log", status=1)
        self.assertIn("Kernel log unavailable", report)
        self.assertNotIn("restricted-kernel-log", report)


if __name__ == "__main__":
    unittest.main()
