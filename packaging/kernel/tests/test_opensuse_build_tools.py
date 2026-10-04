import os
import subprocess
import tempfile
import unittest
from pathlib import Path


HELPER = Path(__file__).resolve().parents[1] / "opensuse-build-tools.sh"


class OpenSuseBuildToolsTest(unittest.TestCase):
    def run_helper(self, failure="", compiler="160200", assembler="24500"):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            commands = root / "bin"
            commands.mkdir()
            log = root / "calls"
            config = root / "auto.conf"
            config.write_text(
                "CONFIG_CC_VERSION_TEXT=gcc (SUSE Linux) 16.2.0\n"
                f"CONFIG_GCC_VERSION={compiler}\n"
                f"CONFIG_AS_VERSION={assembler}\n"
                "CONFIG_LD_VERSION=24500\n"
            )
            scripts = {
                "curl": '''echo "curl $*" >> "$TEST_CALLS"
[ "$FAIL_STAGE" != download ] || exit 1
for argument; do destination=$argument; done
printf fixture > "$destination"
''',
                "sha256sum": '''echo "checksum $*" >> "$TEST_CALLS"
cat >/dev/null
[ "$FAIL_STAGE" != checksum ]
''',
                "rpmkeys": '''echo "signature $*" >> "$TEST_CALLS"
[ "$FAIL_STAGE" != signature ] || exit 1
if [ "$FAIL_STAGE" = unsigned ]; then
    echo 'Payload SHA256 digest: OK'
else
    echo 'Header V4 RSA/SHA512 Signature, key ID 1234abcd: OK'
fi
''',
                "zypper": '''echo "install $*" >> "$TEST_CALLS"
[ "$FAIL_STAGE" != install ]
''',
            }
            for name, body in scripts.items():
                executable = commands / name
                executable.write_text("#!/bin/bash\n" + body)
                executable.chmod(0o755)
            environment = os.environ.copy()
            environment.update(
                PATH=str(commands) + os.pathsep + environment["PATH"],
                TEST_CALLS=str(log),
                FAIL_STAGE=failure,
            )
            result = subprocess.run(
                [
                    "bash", "-c", '''set -eu
die() { echo "$*" >&2; exit 1; }
require_command() { command -v "$1" >/dev/null || die "missing command"; }
config_value() { sed -n "s/^$2=//p" "$1"; }
work_root=$3
source "$1"
install_opensuse_build_tools "$2" "$work_root"
''',
                    "test", str(HELPER), str(config), str(root),
                ],
                env=environment,
                text=True,
                capture_output=True,
                timeout=10,
            )
            return result, log.read_text().splitlines() if log.exists() else []

    def test_verifies_every_pin_and_signature_before_installing(self):
        result, calls = self.run_helper()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual([line.split()[0] for line in calls],
                         ["curl", "checksum", "signature"] * 3 + ["install"])
        self.assertIn("--non-interactive install --oldpackage", calls[-1])
        self.assertTrue(all("/history/20260924/" in line
                            for line in calls if line.startswith("curl ")))
        self.assertFalse(any("import" in line or "gpg-auto" in line for line in calls))

    def test_acquisition_integrity_and_signature_failures_never_install(self):
        for failure in ("download", "checksum", "signature", "unsigned"):
            with self.subTest(failure=failure):
                result, calls = self.run_helper(failure=failure)
                self.assertNotEqual(result.returncode, 0)
                self.assertFalse(any(line.startswith("install ") for line in calls))

    def test_install_failure_propagates(self):
        result, _ = self.run_helper(failure="install")
        self.assertNotEqual(result.returncode, 0)

    def test_other_kernel_toolchains_do_not_use_the_archive(self):
        for configuration in ({"compiler": "160201"}, {"assembler": "24700"}):
            with self.subTest(configuration=configuration):
                result, calls = self.run_helper(**configuration)
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertEqual(calls, [])


if __name__ == "__main__":
    unittest.main()
