"""Contract tests for isolated PGO profile job recovery gates."""
import unittest
from pathlib import Path

WORKFLOW = Path(__file__).resolve().parents[2] / ".github" / "workflows" / "release-pgo.yml"


class ReleasePGOWorkflowTests(unittest.TestCase):
    def test_zfs_device_is_checked_before_attachment(self):
        self.assertIn("ZEROFS_CI_NBD_DEVICE: /dev/nbd7", self.profile)
        self.assertIn('test -b "$ZEROFS_CI_NBD_DEVICE"', self.profile)
        guard = 'if sudo nbd-client -c "$ZEROFS_CI_NBD_DEVICE" >/dev/null 2>&1; then'
        self.assertIn(guard, self.profile)
        block = self.profile.split(guard, 1)[1].split("\n          fi", 1)[0]
        self.assertIn("exit 1", block)
        self.assertLess(self.profile.index(guard), self.profile.index("sudo nbd-client 127.0.0.1"))
        self.assertIn('sudo zpool create testpool "$ZEROFS_CI_NBD_DEVICE"', self.profile)
        self.assertNotIn("/dev/nbd0", self.profile)

    def test_kernel_workloads_use_persistent_pinned_tarball(self):
        url = "https://git.kernel.org/pub/scm/linux/kernel/git/torvalds/linux.git/snapshot/linux-7.2-rc1.tar.gz"
        self.assertEqual(self.source.count(url), 5)
        self.assertNotIn("https://git.kernel.org/torvalds/t/", self.source)

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
            self.profile.index('sudo nbd-client 127.0.0.1 10809 "$ZEROFS_CI_NBD_DEVICE"'),
        )

    def test_nbd_attach_failure_reports_kernel_evidence_and_fails(self):
        command = 'if ! sudo nbd-client 127.0.0.1 10809 "$ZEROFS_CI_NBD_DEVICE" -N test-device; then'
        self.assertIn(command, self.profile)
        block = self.profile.split(command, 1)[1].split("\n            fi", 1)[0]
        self.assertIn("sudo dmesg | tail -80 || true", block)
        self.assertIn("exit 1", block)
        self.assertIn("cat /sys/block/nbd7/pid", block)
        self.assertIn("cat /sys/block/nbd7/size", block)


if __name__ == "__main__":
    unittest.main()

