"""Fail-closed regression tests for external Jepsen dependency acquisition."""
from pathlib import Path
import os
import subprocess
import tempfile
import unittest

SCRIPT = Path(__file__).resolve().parents[1] / "ci" / "retry-lein-deps.sh"


class RetryLeinDepsTests(unittest.TestCase):
    def run_case(self, fail_first: int, *, bootstrap=False, stall=False):
        with tempfile.TemporaryDirectory() as temp:
            temp = Path(temp)
            lein = temp / "lein"
            lein.write_text("""#!/usr/bin/env bash
set -euo pipefail
[[ "$*" == "$EXPECTED_OPERATION" ]] || exit 91
count=$(cat "$ATTEMPT_COUNT" 2>/dev/null || echo 0)
count=$((count + 1))
printf '%s' "$count" > "$ATTEMPT_COUNT"
if [[ "$STALL" == 1 ]]; then
  python3 -c 'import time; time.sleep(60)'
fi
(( count > FAIL_FIRST ))
""")
            lein.chmod(0o755)
            sleep = temp / "sleep"
            sleep.write_text("#!/usr/bin/env bash\nprintf '%s\\n' \"$1\" >> \"$SLEEP_LOG\"\n")
            sleep.chmod(0o755)
            env = dict(os.environ, ATTEMPT_COUNT=str(temp / "count"),
                       FAIL_FIRST=str(fail_first), SLEEP_LOG=str(temp / "sleeps"),
                       EXPECTED_OPERATION="version" if bootstrap else "deps",
                       STALL="1" if stall else "0", LEIN_SETUP_TIMEOUT_SECONDS="1",
                       PATH=f"{temp}:{os.environ['PATH']}")
            options = ["--bootstrap"] if bootstrap else []
            result = subprocess.run(["bash", str(SCRIPT), *options, str(lein)], env=env,
                                    capture_output=True, text=True, check=False, timeout=10)
            attempts = int((temp / "count").read_text())
            delays = (temp / "sleeps").read_text().splitlines() if (temp / "sleeps").exists() else []
            return result, attempts, delays

    def test_first_attempt_success(self):
        result, attempts, delays = self.run_case(0)
        self.assertEqual((result.returncode, attempts, delays), (0, 1, []))

    def test_transient_failure_recovers(self):
        result, attempts, delays = self.run_case(2)
        self.assertEqual((result.returncode, attempts, delays), (0, 3, ["5", "10"]))

    def test_persistent_failure_remains_red(self):
        result, attempts, delays = self.run_case(3)
        self.assertEqual((result.returncode, attempts, delays), (1, 3, ["5", "10"]))
        self.assertIn("failed after 3 attempts", result.stderr)

    def test_stalled_dependency_download_is_bounded_and_remains_red(self):
        result, attempts, delays = self.run_case(0, stall=True)
        self.assertEqual((result.returncode, attempts, delays), (1, 3, ["5", "10"]))
        self.assertEqual(result.stderr.count("exit 124"), 3)

    def test_bootstrap_retries_version_without_running_tests(self):
        result, attempts, delays = self.run_case(2, bootstrap=True)
        self.assertEqual((result.returncode, attempts, delays), (0, 3, ["5", "10"]))

    def test_persistent_bootstrap_failure_remains_red(self):
        result, attempts, delays = self.run_case(3, bootstrap=True)
        self.assertEqual((result.returncode, attempts, delays), (1, 3, ["5", "10"]))
        self.assertIn("Leiningen version failed after 3 attempts", result.stderr)

    def test_invalid_timeout_fails_before_running_command(self):
        result = subprocess.run(
            ["bash", str(SCRIPT), "--bootstrap", "false"],
            env=dict(os.environ, LEIN_SETUP_TIMEOUT_SECONDS="0"),
            capture_output=True, text=True, check=False,
        )
        self.assertEqual(result.returncode, 2)
        self.assertIn("must be a positive integer", result.stderr)

    def test_missing_command_fails_closed(self):
        for options in ([], ["--bootstrap"]):
            with self.subTest(options=options):
                result = subprocess.run(["bash", str(SCRIPT), *options], capture_output=True, text=True, check=False)
                self.assertEqual(result.returncode, 2)


if __name__ == "__main__":
    unittest.main()
