import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest


SCRIPT = Path(__file__).resolve().parents[1] / "bench-raw-journal-flush.sh"


class RawJournalBenchCleanupTests(unittest.TestCase):
    def run_fixture(self, daemon_available):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for name in ("zerofs", "zerofs-raw-device"):
                executable = root / name
                executable.write_text("#!/bin/sh\nexit 0\n")
                executable.chmod(0o755)
            (root / "memory_raw_nas_fit_fixture.toml").touch()
            docker = root / "docker"
            docker.write_text("""#!/usr/bin/env python3
import os
import sys

command = sys.argv[1:]
if command[:2] == ["volume", "create"] or command[:1] == ["run"]:
    sys.exit(0)
if command[:1] == ["info"]:
    sys.exit(0 if os.environ["TEST_DAEMON_AVAILABLE"] == "1" else 1)
if command[:2] in (["container", "inspect"], ["volume", "inspect"]):
    # The same failure status denotes absence and an unreachable daemon.
    sys.exit(1)
raise SystemExit(f"unexpected Docker call: {command}")
""")
            docker.chmod(0o755)
            environment = {
                **os.environ,
                "PATH": f"{root}{os.pathsep}{os.environ['PATH']}",
                "TEST_DAEMON_AVAILABLE": "1" if daemon_available else "0",
            }
            result = subprocess.run(
                ["bash", str(SCRIPT), str(root)],
                env=environment,
                capture_output=True,
                text=True,
                timeout=15,
            )
            receipts = [json.loads(line) for line in result.stdout.splitlines()]
            self.assertEqual(len(receipts), 1, result)
            self.assertEqual(receipts[0]["case"], "docker_cleanup")
            return result, receipts[0]

    def test_cleanup_does_not_claim_absence_when_docker_is_unreachable(self):
        result, receipt = self.run_fixture(daemon_available=False)
        self.assertNotEqual(result.returncode, 0, result)
        self.assertFalse(receipt["verified_absent"])

    def test_cleanup_confirms_absence_with_a_reachable_daemon(self):
        result, receipt = self.run_fixture(daemon_available=True)
        self.assertEqual(result.returncode, 0, result)
        self.assertTrue(receipt["verified_absent"])


if __name__ == "__main__":
    unittest.main()
