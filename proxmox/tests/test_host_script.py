from __future__ import annotations

import subprocess
import unittest
from pathlib import Path


ROOT = Path(__file__).parents[1]
HOST_SCRIPT = ROOT / "host-deploy.sh"
HOOK = ROOT / "hooks" / "zerofs-lxc-hook.sh"


class HostScriptTests(unittest.TestCase):
    def run_host(
        self, action: str, *extra: str, role: str = "dev"
    ) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            [
                "bash",
                str(HOST_SCRIPT),
                action,
                "--role",
                role,
                "--ctid",
                "120",
                "--container-ip",
                "10.10.10.20",
                "--bridge",
                "vmbr1",
                "--gateway",
                "10.10.10.1",
                "--template",
                "local:vztmpl/debian-13-standard.tar.zst",
                "--state-root",
                f"/var/lib/zerofs-lxc/{role}-120",
                "--stage",
                "/var/tmp/zerofs-lxc-stage",
                "--commit",
                "0123456789abcdef",
                "--sha256",
                "a" * 64,
                "--namespace-id",
                "b" * 64,
                "--release-id",
                "0123456789ab-cccccccccccccccc",
                "--dry-run",
                *extra,
            ],
            text=True,
            capture_output=True,
            check=False,
        )

    def test_shell_assets_parse(self) -> None:
        for path in (
            HOST_SCRIPT,
            HOOK,
            ROOT / "deploy.sh",
            ROOT / "guest" / "tune-nbd.sh",
        ):
            with self.subTest(path=path):
                result = subprocess.run(
                    ["bash", "-n", str(path)],
                    text=True,
                    capture_output=True,
                    check=False,
                )
                self.assertEqual(result.returncode, 0, result.stderr)

    def test_deploy_plan_uses_unprivileged_private_lxc_and_persistent_bind(
        self,
    ) -> None:
        result = self.run_host("deploy")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("pct create 120", result.stdout)
        self.assertIn("--unprivileged 1", result.stdout)
        self.assertIn("bridge=vmbr1,ip=10.10.10.20/24", result.stdout)
        self.assertIn("gw=10.10.10.1", result.stdout)
        self.assertIn("mp=/srv/zerofs-persist", result.stdout)
        self.assertIn("namespace collision guard", result.stdout)
        self.assertIn("systemctl is-active --quiet zerofs-lxc.service", result.stdout)
        self.assertNotIn("mkfs", result.stdout)
        self.assertNotIn("0.0.0.0", result.stdout)

    def test_replace_requires_host_side_confirmation_and_creates_rollback_backup(
        self,
    ) -> None:
        rejected = self.run_host("replace")
        self.assertNotEqual(rejected.returncode, 0)
        accepted = self.run_host("replace", "--confirm-replace", "120")
        self.assertEqual(accepted.returncode, 0, accepted.stderr)
        self.assertIn("vzdump 120", accepted.stdout)
        self.assertIn("pct destroy 120", accepted.stdout)
        self.assertIn("rollback", accepted.stdout.lower())

    def test_cleanup_destroys_rootfs_but_explicitly_preserves_state(self) -> None:
        result = self.run_host("cleanup")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("pct destroy 120", result.stdout)
        self.assertIn("preserved_state=/var/lib/zerofs-lxc/dev-120", result.stdout)
        self.assertNotIn("rm -rf -- /var/lib/zerofs-lxc/dev-120", result.stdout)

    def test_prod_update_quiesces_share_and_never_destroys_container(self) -> None:
        result = self.run_host("deploy", "--assume-existing", role="prod")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("systemctl stop smbd.service", result.stdout)
        self.assertIn("systemctl stop zerofs-lxc-mount.service", result.stdout)
        self.assertIn("systemctl start zerofs-lxc-mount.service", result.stdout)
        self.assertIn("systemctl start smbd.service", result.stdout)
        self.assertNotIn("pct destroy 120", result.stdout)

    def test_prod_rejects_replace_and_cleanup_on_host_too(self) -> None:
        for action in ("replace", "cleanup"):
            result = self.run_host(action, role="prod")
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("prod", result.stderr)

    def test_prod_rejects_unsafe_samba_user(self) -> None:
        result = self.run_host("deploy", "--samba-user", "bad;id", role="prod")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("Samba", result.stderr)


if __name__ == "__main__":
    unittest.main()
