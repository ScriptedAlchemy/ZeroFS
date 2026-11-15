"""Execute the native Jepsen setup steps without package or network side effects."""

import json
import os
from pathlib import Path
import re
import subprocess
import tempfile
import textwrap
import unittest


ROOT = Path(__file__).resolve().parents[2]
WORKFLOW = ROOT / ".github/workflows/jepsen-ha.yml"


class NativeJepsenDepsTests(unittest.TestCase):
    def setup_steps(self):
        blocks = re.findall(
            r"      - name: Install guest JDK, Leiningen, and gnuplot\n"
            r"(?:        [^\n]*\n)*?        run: \|\n((?:          [^\n]*\n|\n)+)",
            WORKFLOW.read_text(),
        )
        self.assertEqual(len(blocks), 2, "exercise both the HA and durability jobs")
        return [textwrap.dedent(block) for block in blocks]

    def run_setup(self, step, fail_phase=""):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            log = root / "commands.jsonl"
            sudo = root / "sudo"
            sudo.write_text('#!/usr/bin/env bash\nexec "$@"\n')
            sudo.chmod(0o755)
            for name in ("apt-get", "curl", "chmod", "lein"):
                tool = root / name
                tool.write_text('''#!/usr/bin/env python3
import json
import os
from pathlib import Path
import sys

name = Path(sys.argv[0]).name
args = sys.argv[1:]
with open(os.environ["COMMAND_LOG"], "a") as log:
    log.write(json.dumps([name, args, os.environ.get("DEBIAN_FRONTEND")]) + "\\n")
if name == "apt-get":
    phase = "update" if "update" in args else "install"
    if os.environ.get("FAIL_PHASE") == phase:
        # apt normally accepts a transient index failure with a warning.
        # Strict refresh must turn that warning into a failure before install.
        if phase == "install" or "APT::Update::Error-Mode=any" in args:
            sys.exit(100)
''')
                tool.chmod(0o755)
            environment = dict(
                os.environ,
                PATH=f"{root}:{os.environ['PATH']}",
                GITHUB_WORKSPACE=str(ROOT),
                COMMAND_LOG=str(log),
                FAIL_PHASE=fail_phase,
            )
            result = subprocess.run(
                ["bash", "--noprofile", "--norc", "-e", "-o", "pipefail", "-c", step],
                env=environment,
                capture_output=True,
                text=True,
                timeout=10,
                check=False,
            )
            commands = [json.loads(line) for line in log.read_text().splitlines()]
            return result, commands

    def test_success_installs_headless_graph_tools_with_bounded_transport(self):
        for step in self.setup_steps():
            with self.subTest(step=step):
                result, commands = self.run_setup(step)
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertEqual([name for name, _, _ in commands],
                                 ["apt-get", "apt-get", "curl", "chmod", "lein"])
                for _, arguments, frontend in commands[:2]:
                    self.assertEqual(frontend, "noninteractive")
                    for option in ("Acquire::Retries=3", "Acquire::http::Timeout=30",
                                   "Acquire::https::Timeout=30"):
                        self.assertIn(option, arguments)
                packages = commands[1][1]
                for package in ("gnuplot-nox", "fonts-liberation", "openjdk-17-jdk-headless"):
                    self.assertIn(package, packages)
                self.assertNotIn("gnuplot", packages)
                self.assertIn("--no-install-recommends", packages)

    def test_failed_index_refresh_stops_before_installing_or_downloading_lein(self):
        for step in self.setup_steps():
            with self.subTest(step=step):
                result, commands = self.run_setup(step, fail_phase="update")
                self.assertEqual(result.returncode, 100, result.stderr)
                self.assertEqual([name for name, _, _ in commands], ["apt-get"])

    def test_failed_package_install_stops_setup_without_retrying_the_step(self):
        for step in self.setup_steps():
            with self.subTest(step=step):
                result, commands = self.run_setup(step, fail_phase="install")
                self.assertEqual(result.returncode, 100, result.stderr)
                self.assertEqual([name for name, _, _ in commands], ["apt-get", "apt-get"])


if __name__ == "__main__":
    unittest.main()
