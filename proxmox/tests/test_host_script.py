from __future__ import annotations

import subprocess
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).parents[1]
HOST_SCRIPT = ROOT / "host-deploy.sh"
HOOK = ROOT / "hooks" / "zerofs-lxc-hook.sh"


class HostScriptTests(unittest.TestCase):
    def run_resource_function(
        self, function: str, value: str
    ) -> subprocess.CompletedProcess[str]:
        source = HOST_SCRIPT.read_text()
        functions = source[
            source.index("config_value() {") : source.index("assert_server_drained() {")
        ]
        script = (
            "set -euo pipefail\n"
            "state_root=/var/lib/zerofs-lxc/prod-198\n"
            "bridge=vmbr1\ncontainer_ip=10.10.10.55\ngateway=10.10.10.1\n"
            + functions
            + f'\n{function} "$1"\n'
        )
        return subprocess.run(
            ["bash", "-c", script, "resource-test", value],
            text=True,
            capture_output=True,
            check=False,
        )

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

    def test_recover_accepts_an_interrupted_older_release_transaction(self) -> None:
        source = HOST_SCRIPT.read_text()
        functions = source[
            source.index("control_host_transaction() {") : source.index(
                "assert_server_drained() {"
            )
        ]
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            transaction = root / "deployment-transaction"
            transaction.mkdir()
            (transaction / "state.env").write_text(
                "saved_had_ct=false\n"
                "saved_had_running_ct=false\n"
                "saved_prod_mount_was_active=false\n"
                "saved_prod_smb_was_active=false\n"
                "saved_prod_mount_was_enabled=false\n"
                "saved_prod_smb_was_enabled=false\n"
                "saved_release_id=older-release\n"
            )
            (transaction / "previous-release").write_text("\n")
            (transaction / "phase").write_text("quiesced\n")
            script = f"""set -euo pipefail
dry_run=false
action=recover
deployment_transaction={transaction}
state_root={root}
release_id=new-release
ctid=198
ct_resource_snapshot=
ct_resources_mutated=false
previous_release=
ct_exists() {{ return 1; }}
ct_running() {{ return 1; }}
restore_ct_resources() {{ return 0; }}
pct() {{ :; }}
{functions}
control_host_transaction
"""
            result = subprocess.run(
                ["bash", "-c", script],
                text=True,
                capture_output=True,
                check=False,
            )

            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertFalse(transaction.exists())

    def test_commit_rejects_a_transaction_that_is_not_fully_activated(self) -> None:
        source = HOST_SCRIPT.read_text()
        functions = source[
            source.index("set_host_transaction_phase() {") : source.index(
                "assert_server_drained() {"
            )
        ]
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            transaction = root / "deployment-transaction"
            transaction.mkdir()
            (transaction / "state.env").write_text(
                "saved_had_ct=false\n"
                "saved_had_running_ct=false\n"
                "saved_prod_mount_was_active=false\n"
                "saved_prod_smb_was_active=false\n"
                "saved_prod_mount_was_enabled=false\n"
                "saved_prod_smb_was_enabled=false\n"
                "saved_release_id=current-release\n"
            )
            (transaction / "previous-release").write_text("\n")
            (transaction / "phase").write_text("maintenance\n")
            script = f"""set -euo pipefail
dry_run=false
action=commit
defer_commit=false
deployment_transaction={transaction}
state_root={root}
release_id=current-release
ctid=198
ct_resource_snapshot=
ct_resources_mutated=false
previous_release=
ct_exists() {{ return 1; }}
ct_running() {{ return 1; }}
restore_ct_resources() {{ return 0; }}
pct() {{ :; }}
{functions}
control_host_transaction
"""
            rejected = subprocess.run(
                ["bash", "-c", script],
                text=True,
                capture_output=True,
                check=False,
            )

            self.assertNotEqual(rejected.returncode, 0)
            self.assertIn("not activated", rejected.stderr)
            self.assertTrue(transaction.exists())

    def test_shell_assets_parse(self) -> None:
        for path in (
            HOST_SCRIPT,
            HOOK,
            ROOT / "deploy.sh",
        ):
            with self.subTest(path=path):
                result = subprocess.run(
                    ["bash", "-n", str(path)],
                    text=True,
                    capture_output=True,
                    check=False,
                )
                self.assertEqual(result.returncode, 0, result.stderr)

    def test_hook_accepts_only_role_scoped_persistent_state_roots(self) -> None:
        source = HOOK.read_text()
        self.assertIn('"/var/lib/zerofs-lxc/prod-${ctid}"', source)
        self.assertIn('"/var/lib/zerofs-lxc/dev-${ctid}"', source)
        self.assertNotIn('expected="/var/lib/zerofs-lxc/${ctid}"', source)

    def test_host_lock_is_global_for_shared_pve_resources(self) -> None:
        source = HOST_SCRIPT.read_text()

        self.assertIn("/run/lock/zerofs-lxc-deploy-global.lock", source)
        self.assertNotIn("/run/lock/zerofs-lxc-$ctid.coordinator.lock", source)

    def test_state_root_and_prior_release_are_host_owned_and_rollback_safe(
        self,
    ) -> None:
        source = HOST_SCRIPT.read_text()

        self.assertIn('install -d -o 0 -g 100000 -m 0750 "$state_root"', source)
        self.assertNotIn('install -d -o 100000 -g 100000 -m 0750 "$state_root"', source)
        self.assertIn('"$temporary_transaction/previous-config"', source)
        self.assertIn(
            '"$deployment_transaction/previous-config" "$state_root/$previous_release/zerofs.toml"',
            source,
        )
        self.assertIn(
            'promoted_config="$state_root/releases/$saved_release_id/zerofs.toml"',
            source,
        )
        self.assertNotIn('"$state_root/current/zerofs.toml"', source)

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

    def test_existing_ct_resources_are_reconciled_to_requested_values(self) -> None:
        result = self.run_host(
            "deploy",
            "--assume-existing",
            "--memory-mb",
            "49152",
            "--cores",
            "6",
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("validate/reconcile existing CT resources", result.stdout)
        self.assertIn("memory=49152", result.stdout)
        self.assertIn("cores=6", result.stdout)
        self.assertIn("onboot=1", result.stdout)
        self.assertIn("startup=order=20", result.stdout)
        self.assertIn(
            "net0=name=eth0,bridge=vmbr1,ip=10.10.10.20/24,gw=10.10.10.1,type=veth",
            result.stdout,
        )
        self.assertIn(
            "mp0=/var/lib/zerofs-lxc/dev-120,mp=/srv/zerofs-persist",
            result.stdout,
        )

    def test_persistent_bind_is_normalized_read_write_without_losing_options(
        self,
    ) -> None:
        result = self.run_resource_function(
            "desired_mp0",
            "/old/path,mp=/old/mount,backup=1,ro=1,replicate=0",
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        fields = result.stdout.strip().split(",")
        self.assertEqual(fields[0], "/var/lib/zerofs-lxc/prod-198")
        self.assertIn("mp=/srv/zerofs-persist", fields)
        self.assertIn("backup=1", fields)
        self.assertIn("replicate=0", fields)
        self.assertNotIn("ro=1", fields)

    def test_existing_ct_resource_changes_have_a_failure_rollback_plan(self) -> None:
        result = self.run_host("deploy", "--assume-existing")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("capture existing CT resource snapshot", result.stdout)
        self.assertIn("rollback restores captured CT resources", result.stdout)

    def test_resource_rollback_failure_preserves_snapshot_and_refuses_restart(
        self,
    ) -> None:
        source = HOST_SCRIPT.read_text()
        rollback = source[
            source.index("rollback() {") : source.index("trap rollback ERR")
        ]
        self.assertIn("assert_ct_resource_snapshot", source)
        self.assertIn("rollback_failed=true", rollback)
        self.assertIn("recovery transaction preserved", rollback)
        self.assertIn("if [[ $rollback_failed == true ]]; then", rollback)
        self.assertLess(
            rollback.index("recovery transaction preserved"),
            rollback.index('rm -rf -- "$deployment_transaction"'),
        )

    def test_replace_backs_up_original_ct_before_applying_new_resources(self) -> None:
        result = self.run_host(
            "replace",
            "--confirm-replace",
            "120",
            "--memory-mb",
            "49152",
            "--cores",
            "6",
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        before_backup = result.stdout.split("vzdump 120", 1)[0]
        self.assertNotIn("validate/reconcile existing CT resources", before_backup)
        self.assertIn("pct create 120", result.stdout)
        self.assertIn("--memory 49152", result.stdout)
        self.assertIn("--cores 6", result.stdout)

    def test_invalid_resource_requests_fail_before_any_host_plan(self) -> None:
        for extra in (
            ("--memory-mb", "0"),
            ("--cores", "0"),
            ("--bridge", "vmbr1;id"),
            ("--rootfs", "local-lvm:8;id"),
        ):
            with self.subTest(extra=extra):
                result = self.run_host("deploy", *extra)
                self.assertNotEqual(result.returncode, 0)
                self.assertEqual(result.stdout, "")

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
        result = self.run_host(
            "deploy", "--assume-existing", "--prod-access", "both", role="prod"
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("systemctl stop smbd.service", result.stdout)
        self.assertIn("prove no established NFS clients", result.stdout)
        self.assertIn("systemctl stop zerofs-lxc.service", result.stdout)
        self.assertIn("systemctl stop zerofs-lxc-mount.service", result.stdout)
        self.assertIn("systemctl start zerofs-lxc-mount.service", result.stdout)
        self.assertIn("systemctl start smbd.service", result.stdout)
        self.assertIn("private 10.10.10.20:8080", result.stdout)
        self.assertNotIn("pct destroy 120", result.stdout)

    def test_prod_deferred_deploy_has_explicit_commit_and_rollback_controls(
        self,
    ) -> None:
        deploy_result = self.run_host(
            "deploy",
            "--assume-existing",
            "--defer-commit",
            role="prod",
        )
        self.assertEqual(deploy_result.returncode, 0, deploy_result.stderr)
        transaction = "/var/lib/zerofs-lxc/prod-120/deployment-transaction"
        self.assertIn(f"deferred_transaction={transaction}", deploy_result.stdout)
        self.assertIn("persist host rollback transaction", deploy_result.stdout)

        commit = self.run_host("commit", role="prod")
        self.assertEqual(commit.returncode, 0, commit.stderr)
        self.assertIn(
            f"commit host deployment transaction {transaction}", commit.stdout
        )

        rollback = self.run_host("rollback", role="prod")
        self.assertEqual(rollback.returncode, 0, rollback.stderr)
        self.assertIn(
            f"rollback host deployment transaction {transaction}", rollback.stdout
        )

        recover = self.run_host("recover", role="prod")
        self.assertEqual(recover.returncode, 0, recover.stderr)
        self.assertIn(
            f"recover host deployment transaction {transaction}", recover.stdout
        )
        self.assertIn(
            "restore previous release and exact CT resources", rollback.stdout
        )

    def test_prod_persists_recovery_before_any_service_or_share_mutation(self) -> None:
        source = HOST_SCRIPT.read_text()
        persisted = source.index("\npersist_host_transaction\n")
        quiesced = source.index("\n  quiesce_prod_share\n", persisted)
        stopped = source.index(
            'run pct exec "$ctid" -- systemctl stop zerofs-lxc.service', persisted
        )
        self.assertLess(persisted, quiesced)
        self.assertLess(persisted, stopped)

    def test_resource_snapshot_covers_every_ct_setting_mutated_by_deploy(self) -> None:
        source = HOST_SCRIPT.read_text()
        self.assertIn(
            "for key in cores memory swap onboot startup net0 mp0 hookscript features",
            source,
        )

    def test_dev_cannot_defer_or_control_a_production_transaction(self) -> None:
        deferred = self.run_host("deploy", "--defer-commit", role="dev")
        self.assertNotEqual(deferred.returncode, 0)
        for action in ("commit", "rollback"):
            with self.subTest(action=action):
                result = self.run_host(action, role="dev")
                self.assertNotEqual(result.returncode, 0)

    def test_maintenance_activation_exposes_only_nfs_and_metrics(self) -> None:
        result = self.run_host(
            "deploy",
            "--defer-commit",
            "--maintenance-nfs-only",
            role="prod",
        )

        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("prove maintenance NFS listener", result.stdout)
        self.assertIn("10.10.10.20:2049", result.stdout)
        self.assertNotIn("prove 9P listener", result.stdout)
        self.assertNotIn("prove NBD listener", result.stdout)
        self.assertNotIn("prove WebUI listener", result.stdout)
        self.assertIn("persist host deployment phase maintenance", result.stdout)

    def test_maintenance_stages_both_access_without_starting_smb(self) -> None:
        result = self.run_host(
            "deploy",
            "--defer-commit",
            "--maintenance-nfs-only",
            "--prod-access",
            "both",
            role="prod",
        )

        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("apt-get install -y --no-install-recommends", result.stdout)
        self.assertIn("fuse3 samba", result.stdout)
        self.assertIn("zerofs-lxc-mount.service", result.stdout)
        self.assertNotIn("systemctl start smbd.service", result.stdout)
        self.assertIn("persist host deployment phase maintenance", result.stdout)

    def test_promote_requires_the_deferred_production_transaction(self) -> None:
        result = self.run_host("promote", role="prod")

        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("promote host deployment transaction", result.stdout)
        self.assertIn("prove full private production listeners", result.stdout)

    def test_finalize_is_the_idempotent_commit_recovery_action(self) -> None:
        result = self.run_host("finalize", role="prod")

        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("finalize host deployment transaction", result.stdout)

    def test_default_prod_access_is_native_nfs_without_samba(self) -> None:
        result = self.run_host("deploy", role="prod")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("private 10.10.10.20:2049", result.stdout)
        self.assertNotIn(
            "apt-get install -y --no-install-recommends fuse3 samba", result.stdout
        )
        self.assertNotIn("smbd.service", result.stdout)

    def test_stopped_prod_recovery_skips_live_quiesce_and_starts_container(
        self,
    ) -> None:
        result = self.run_host(
            "deploy", "--assume-existing", "--assume-stopped", role="prod"
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertNotIn(
            "pct exec 120 -- systemctl stop zerofs-lxc.service", result.stdout
        )
        self.assertIn("pct start 120", result.stdout)

    def test_failed_deploy_restores_the_original_stopped_state(self) -> None:
        source = HOST_SCRIPT.read_text()
        self.assertRegex(
            source,
            r"elif pct config \"\$ctid\" .*; then\n\s+rollback_try pct stop \"\$ctid\"",
        )

    def test_listener_proof_covers_every_private_production_api(self) -> None:
        source = HOST_SCRIPT.read_text()
        self.assertIn("$container_ip:8080", source)
        self.assertIn("$container_ip:2049", source)
        self.assertIn("$container_ip:5564", source)
        self.assertIn("$container_ip:10809", source)
        self.assertRegex(source, r"10809\|9567\|2049\|5564\|445\|8080")

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
