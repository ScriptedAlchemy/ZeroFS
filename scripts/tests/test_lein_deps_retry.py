"""Fail-closed regression tests for external Jepsen dependency acquisition."""
from pathlib import Path
import os
import subprocess
import tempfile
import unittest

SCRIPT = Path(__file__).resolve().parents[1] / "ci" / "retry-lein-deps.sh"


class RetryLeinDepsTests(unittest.TestCase):
    def run_case(self, fail_first: int):
        with tempfile.TemporaryDirectory() as temp:
            temp = Path(temp)
            lein = temp / "lein"
            lein.write_text("""#!/usr/bin/env bash
set -euo pipefail
[[ "$*" == deps ]] || exit 91
count=$(cat "$ATTEMPT_COUNT" 2>/dev/null || echo 0)
count=$((count + 1))
printf '%s' "$count" > "$ATTEMPT_COUNT"
(( count > FAIL_FIRST ))
""")
            lein.chmod(0o755)
            sleep = temp / "sleep"
            sleep.write_text("#!/usr/bin/env bash\nprintf '%s\\n' \"$1\" >> \"$SLEEP_LOG\"\n")
            sleep.chmod(0o755)
            env = dict(os.environ, ATTEMPT_COUNT=str(temp / "count"),
                       FAIL_FIRST=str(fail_first), SLEEP_LOG=str(temp / "sleeps"),
                       PATH=f"{temp}:{os.environ['PATH']}")
            result = subprocess.run(["bash", str(SCRIPT), str(lein)], env=env,
                                    capture_output=True, text=True, check=False)
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

    def test_missing_command_fails_closed(self):
        result = subprocess.run(["bash", str(SCRIPT)], capture_output=True, text=True, check=False)
        self.assertEqual(result.returncode, 2)


if __name__ == "__main__":
    unittest.main()
