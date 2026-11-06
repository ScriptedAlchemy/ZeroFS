import json
import os
import re
import subprocess
import tempfile
import unittest
from pathlib import Path


BUILDER = Path(__file__).resolve().parents[1] / "build-module-container.sh"


class AptBootstrapTest(unittest.TestCase):
    def run_bootstrap(self, failed_update):
        source = BUILDER.read_text(encoding="utf-8")
        match = re.search(
            r"^install_apt_snapshot_ca\(\) \{\n.*?^\}",
            source,
            re.MULTILINE | re.DOTALL,
        )
        self.assertIsNotNone(match)
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            log = root / "calls.jsonl"
            apt = root / "apt-get"
            apt.write_text(
                "#!/usr/bin/env python3\n"
                "import json, os, sys\n"
                "from pathlib import Path\n"
                "log = Path(os.environ['APT_TEST_LOG'])\n"
                "calls = [json.loads(line) for line in "
                "log.read_text().splitlines()] if log.exists() else []\n"
                "args = sys.argv[1:]\n"
                "with log.open('a') as stream:\n"
                "    stream.write(json.dumps(args) + '\\n')\n"
                "update = 1 + sum('update' in call for call in calls)\n"
                "failed = ('update' in args and "
                "update == int(os.environ['APT_TEST_FAILED_UPDATE']))\n"
                "strict = 'APT::Update::Error-Mode=any' in args\n"
                "sys.exit(100 if failed and strict else 0)\n",
                encoding="utf-8",
            )
            apt.chmod(0o755)
            # Exercise the real helper in a subprocess. Redirect only its
            # configuration writes so the host's APT state is untouched.
            helper = match.group().replace("/etc/apt", str(root / "etc/apt"))
            result = subprocess.run(
                ["bash", "-c", "set -euo pipefail\n" + helper
                 + "\ninstall_apt_snapshot_ca\nprintf completed"],
                env={
                    **os.environ,
                    "PATH": str(root) + os.pathsep + os.environ["PATH"],
                    "APT_TEST_LOG": str(log),
                    "APT_TEST_FAILED_UPDATE": str(failed_update),
                },
                capture_output=True,
                text=True,
                check=False,
            )
            calls = [json.loads(line) for line in log.read_text().splitlines()]
            return result, calls

    def test_partial_bootstrap_update_stops_before_install(self):
        result, calls = self.run_bootstrap(1)
        self.assertEqual(result.returncode, 100)
        self.assertEqual(len(calls), 1)
        self.assertNotIn("completed", result.stdout)

    def test_partial_verified_update_stops_before_build(self):
        result, calls = self.run_bootstrap(2)
        self.assertEqual(result.returncode, 100)
        self.assertEqual(len(calls), 3)
        self.assertNotIn("completed", result.stdout)

    def test_complete_updates_preserve_bootstrap_sequence(self):
        result, calls = self.run_bootstrap(0)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout, "completed")
        self.assertEqual(len(calls), 3)
        self.assertEqual(calls[0][-1], "update")
        self.assertIn("ca-certificates", calls[1])
        self.assertEqual(calls[2][-1], "update")
        for call in (calls[0], calls[2]):
            self.assertIn("APT::Update::Error-Mode=any", call)
            self.assertIn("Acquire::Check-Valid-Until=false", call)
        self.assertIn("Acquire::https::Verify-Peer=false", calls[0])
        self.assertNotIn("Acquire::https::Verify-Peer=false", calls[2])


if __name__ == "__main__":
    unittest.main()
