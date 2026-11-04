"""Contract tests for isolated PGO profile job recovery gates."""
import unittest
from pathlib import Path

WORKFLOW = Path(__file__).resolve().parents[2] / ".github" / "workflows" / "release-pgo.yml"


class ReleasePGOWorkflowTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.source = WORKFLOW.read_text()
        cls.profile = cls.source.split("  collect-pgo-profile:", 1)[1].split(
            "\n  merge-pgo-data:", 1
        )[0]

    def test_failed_profile_does_not_cancel_other_required_profiles(self):
        strategy = self.profile.split("\n    strategy:", 1)[1].split(
            "\n    steps:", 1
        )[0]
        self.assertIn("fail-fast: false", strategy)
        self.assertIn("test: [9p, fuse, zfs-on-zerofs, nfs-kernel-compile]", strategy)
        self.assertIn("needs: collect-pgo-profile", self.source)

    def test_zfs_profiles_load_nbd_module_before_attaching_device(self):
        install = self.profile.split("      - name: Install dependencies", 1)[1].split(
            "      - uses: actions/setup-go", 1
        )[0]
        self.assertIn("sudo apt-get install -y nbd-client", install)
        self.assertIn("sudo modprobe nbd", install)
        self.assertLess(
            install.index("sudo apt-get install -y nbd-client"),
            install.index("sudo modprobe nbd"),
        )
        self.assertLess(
            self.profile.index("sudo modprobe nbd"),
            self.profile.index("sudo nbd-client 127.0.0.1 10809 /dev/nbd0"),
        )

    def test_nbd_attach_failure_reports_kernel_evidence_and_fails(self):
        command = "if ! sudo nbd-client 127.0.0.1 10809 /dev/nbd0 -N test-device; then"
        self.assertIn(command, self.profile)
        block = self.profile.split(command, 1)[1].split("\n            fi", 1)[0]
        self.assertIn("sudo dmesg | tail -80 || true", block)
        self.assertIn("exit 1", block)


if __name__ == "__main__":
    unittest.main()
