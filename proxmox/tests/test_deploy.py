from __future__ import annotations

import importlib.util
import os
import subprocess
import sys
import tempfile
import tomllib
import unittest
from pathlib import Path


MODULE_PATH = Path(__file__).parents[1] / "deploy.py"
SPEC = importlib.util.spec_from_file_location("zerofs_proxmox_deploy", MODULE_PATH)
assert SPEC is not None and SPEC.loader is not None
deploy = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = deploy
SPEC.loader.exec_module(deploy)


class MetricsTests(unittest.TestCase):
    def test_drain_requires_volatile_and_writeback_tiers_to_be_clean(self) -> None:
        clean = """
zerofs_nbd_volatile_memory_dirty_bytes 0
zerofs_nbd_volatile_memory_dirty_operations 0
zerofs_nbd_volatile_memory_terminal 0
zerofs_writeback_accepted_sequence 18
zerofs_writeback_local_sequence 18
zerofs_writeback_remote_sequence 18
zerofs_writeback_dirty_ram_bytes 0
zerofs_writeback_dirty_ssd_reserved_bytes 0
zerofs_writeback_terminal_error 0
"""
        self.assertTrue(deploy.parse_drain_state(clean).drained)

        for mutation in (
            clean.replace("dirty_bytes 0", "dirty_bytes 4096"),
            clean.replace("remote_sequence 18", "remote_sequence 17"),
            clean.replace("terminal_error 0", "terminal_error 1"),
        ):
            with self.subTest(mutation=mutation):
                self.assertFalse(deploy.parse_drain_state(mutation).drained)

    def test_missing_required_metric_fails_closed(self) -> None:
        with self.assertRaisesRegex(ValueError, "missing ZeroFS metrics"):
            deploy.parse_drain_state("zerofs_writeback_terminal_error 0\n")


class SystemdTemplateTests(unittest.TestCase):
    def test_nbd_client_reconnects_after_a_transient_server_restart(self) -> None:
        unit = (
            Path(__file__).parents[1]
            / "systemd"
            / "zerofs-lxc-nbd-client.service"
        ).read_text()

        self.assertIn("-persist -timeout 600", unit)

    def test_file_namespace_mount_is_persistent_and_independent_from_nbd(self) -> None:
        raw_unit = (
            Path(__file__).parents[1]
            / "systemd"
            / r"mnt-zerofs\x2dfiles\x2draw.mount"
        ).read_text()

        self.assertIn("What=10.10.10.55:/", raw_unit)
        self.assertIn("Where=/mnt/zerofs-files-raw", raw_unit)
        self.assertIn("Type=nfs", raw_unit)
        self.assertIn("vers=3", raw_unit)
        self.assertIn("proto=tcp", raw_unit)
        self.assertIn("hard", raw_unit)
        self.assertIn("rw", raw_unit)
        self.assertIn("actimeo=1", raw_unit)
        self.assertIn("_netdev", raw_unit)
        self.assertIn("WantedBy=remote-fs.target", raw_unit)
        self.assertNotIn("zerofs-lxc-nbd-client.service", raw_unit)

        nbd_guard = (
            Path(__file__).parents[1]
            / "systemd"
            / r"mnt-zerofs\x2dfiles\x2draw-.nbd.mount"
        ).read_text()
        self.assertIn(r"Requires=mnt-zerofs\x2dfiles\x2draw.mount", nbd_guard)
        self.assertIn("What=/mnt/zerofs-files-raw/.nbd", nbd_guard)
        self.assertIn("Where=/mnt/zerofs-files-raw/.nbd", nbd_guard)
        self.assertIn("bind", nbd_guard)
        self.assertIn("ro", nbd_guard)
        self.assertIn("_netdev", nbd_guard)
        self.assertIn("WantedBy=remote-fs.target", nbd_guard)

        view_unit = (
            Path(__file__).parents[1]
            / "systemd"
            / r"mnt-zerofs\x2dfiles.mount"
        ).read_text()
        self.assertIn(r"Requires=mnt-zerofs\x2dfiles\x2draw-.nbd.mount", view_unit)
        self.assertIn("What=/mnt/zerofs-files-raw", view_unit)
        self.assertIn("Where=/mnt/zerofs-files", view_unit)
        self.assertIn("Type=fuse.bindfs", view_unit)
        self.assertIn("mirror=zack", view_unit)
        self.assertIn("create-for-user=501", view_unit)
        self.assertIn("create-for-group=20", view_unit)
        self.assertIn("chown-ignore", view_unit)
        self.assertIn("chgrp-ignore", view_unit)
        self.assertIn("chmod-ignore", view_unit)
        self.assertIn("WantedBy=remote-fs.target", view_unit)


class ConfigValidationTests(unittest.TestCase):
    def write_config(self, text: str) -> Path:
        handle = tempfile.NamedTemporaryFile(mode="w", suffix=".toml", delete=False)
        with handle:
            handle.write(text)
        self.addCleanup(Path(handle.name).unlink, missing_ok=True)
        return Path(handle.name)

    def valid_config(self) -> str:
        return """
[cache]
dir = "/srv/zerofs-persist/cache"
disk_size_gb = 1000.0
memory_size_gb = 64.0

[storage]
url = "sftp://example.invalid/data"

[sftp]
identity_file = "/srv/zerofs-persist/current/storage-key"
known_hosts = "/srv/zerofs-persist/current/known_hosts"
max_connections = 4
read_concurrency = 4
write_concurrency = 4

[filesystem]
ignore_fsync = false

[writeback]
enabled = true
ack_mode = "memory"
memory_size_gb = 4.0
disk_size_gb = 64.0
min_free_gb = 32.0
dir = "/srv/zerofs-persist/state/writeback"

[servers.nbd]
addresses = ["10.10.10.20:10809"]
write_ack_mode = "volatile_memory"
volatile_memory_gb = 16.0

[servers.rpc]
unix_socket = "/run/zerofs/rpc.sock"

[prometheus]
addresses = ["10.10.10.20:9567"]
"""

    def prod_config(self) -> str:
        return """
[cache]
dir = "/srv/zerofs-persist/cache"
disk_size_gb = 1000.0
memory_size_gb = 64.0

[storage]
url = "sftp://example.invalid/data/zerofs-prod"

[sftp]
identity_file = "/srv/zerofs-persist/current/storage-key"
known_hosts = "/srv/zerofs-persist/current/known_hosts"
max_connections = 4
read_concurrency = 4
write_concurrency = 4

[filesystem]
ignore_fsync = false

[writeback]
enabled = true
ack_mode = "ssd"
memory_size_gb = 4.0
disk_size_gb = 64.0
min_free_gb = 32.0
dir = "/srv/zerofs-persist/state/writeback"

[servers.ninep]
addresses = ["10.10.10.30:5564"]
unix_socket = "/run/zerofs/9p.sock"

[servers.nbd]
addresses = ["10.10.10.30:10809"]
unix_socket = "/run/zerofs/nbd.sock"
write_ack_mode = "materialized"

[servers.nfs]
addresses = ["10.10.10.30:2049"]

[servers.rpc]
unix_socket = "/run/zerofs/rpc.sock"

[servers.webui]
addresses = ["10.10.10.30:8080"]
uid = 0
gid = 0

[prometheus]
addresses = ["10.10.10.30:9567"]
"""

    def test_private_container_config_is_accepted(self) -> None:
        deploy.validate_server_config(
            self.write_config(self.valid_config()), "10.10.10.20"
        )

    def test_public_listener_is_rejected(self) -> None:
        config = self.valid_config().replace('"10.10.10.20:10809"', '"0.0.0.0:10809"')
        with self.assertRaisesRegex(ValueError, "private container address"):
            deploy.validate_server_config(self.write_config(config), "10.10.10.20")

    def test_tcp_rpc_and_other_writable_frontends_are_rejected(self) -> None:
        config = self.valid_config().replace(
            'unix_socket = "/run/zerofs/rpc.sock"',
            'unix_socket = "/run/zerofs/rpc.sock"\naddresses = ["10.10.10.20:7000"]',
        )
        with self.assertRaisesRegex(ValueError, "RPC must be Unix-socket only"):
            deploy.validate_server_config(self.write_config(config), "10.10.10.20")

        config = (
            self.valid_config()
            + '\n[servers.ninep]\naddresses = ["10.10.10.20:5564"]\n'
        )
        with self.assertRaisesRegex(ValueError, "exclusive NBD"):
            deploy.validate_server_config(self.write_config(config), "10.10.10.20")

    def test_nonvolatile_nbd_or_ignored_fsync_is_rejected(self) -> None:
        config = self.valid_config().replace(
            'write_ack_mode = "volatile_memory"', 'write_ack_mode = "materialized"'
        )
        with self.assertRaisesRegex(ValueError, "volatile_memory"):
            deploy.validate_server_config(self.write_config(config), "10.10.10.20")

        config = self.valid_config().replace(
            "ignore_fsync = false", "ignore_fsync = true"
        )
        with self.assertRaisesRegex(ValueError, "ignore_fsync"):
            deploy.validate_server_config(self.write_config(config), "10.10.10.20")

    def test_writeback_requires_persistent_directory_and_free_space_reserve(
        self,
    ) -> None:
        config = self.valid_config().replace(
            'dir = "/srv/zerofs-persist/state/writeback"',
            'journal_dir = "/srv/zerofs-persist/state/writeback"',
        )
        with self.assertRaisesRegex(ValueError, "writeback.*dir"):
            deploy.validate_server_config(self.write_config(config), "10.10.10.20")

        config = self.valid_config().replace("min_free_gb = 32.0", "min_free_gb = 0.0")
        with self.assertRaisesRegex(ValueError, "min_free_gb"):
            deploy.validate_server_config(self.write_config(config), "10.10.10.20")

    def test_container_memory_must_cover_all_ram_tiers_plus_overhead(self) -> None:
        config = self.write_config(self.valid_config())
        with self.assertRaisesRegex(ValueError, "container memory"):
            deploy.validate_server_config(config, "10.10.10.20", memory_mb=65536)
        deploy.validate_server_config(config, "10.10.10.20", memory_mb=98304)

    def test_prod_role_allows_only_private_materialized_nbd(self) -> None:
        config = self.write_config(self.prod_config())
        deploy.validate_server_config(
            config, "10.10.10.30", memory_mb=98304, role="prod"
        )
        for unsafe, message in (
            (
                self.prod_config().replace(
                    'write_ack_mode = "materialized"',
                    'write_ack_mode = "volatile_memory"\nvolatile_memory_gb = 16.0',
                ),
                "materialized",
            ),
            (
                self.prod_config().replace(
                    'addresses = ["10.10.10.30:10809"]',
                    'addresses = ["0.0.0.0:10809"]',
                ),
                "private container address",
            ),
        ):
            with self.subTest(message=message), self.assertRaisesRegex(
                ValueError, message
            ):
                deploy.validate_server_config(
                    self.write_config(unsafe), "10.10.10.30", role="prod"
                )

    def test_prod_template_can_host_a_five_tib_nbd_export(self) -> None:
        template = Path(__file__).parents[1] / "templates/zerofs-prod.toml.example"
        deploy.validate_server_config(
            template, "10.10.10.30", memory_mb=98304, role="prod"
        )
        with template.open("rb") as handle:
            settings = tomllib.load(handle)
        self.assertGreater(
            settings["filesystem"]["max_size_gb"] * 1_000_000_000,
            5 * 1024**4,
        )

    def test_prod_ninep_requires_exact_private_address_and_port(self) -> None:
        for address in ("0.0.0.0:5564", "10.10.10.30:5565", "10.10.10.31:5564"):
            unsafe = self.prod_config().replace("10.10.10.30:5564", address)
            with self.subTest(address=address), self.assertRaisesRegex(
                ValueError, "9P must listen only"
            ):
                deploy.validate_server_config(
                    self.write_config(unsafe), "10.10.10.30", role="prod"
                )

    def test_prod_webui_requires_exact_private_address_and_port(self) -> None:
        deploy.validate_server_config(
            self.write_config(self.prod_config()), "10.10.10.30", role="prod"
        )
        for address in ("0.0.0.0:8080", "10.10.10.30:8081", "10.10.10.31:8080"):
            unsafe = self.prod_config().replace("10.10.10.30:8080", address)
            with self.subTest(address=address), self.assertRaisesRegex(
                ValueError, "WebUI.*private container address.*8080"
            ):
                deploy.validate_server_config(
                    self.write_config(unsafe), "10.10.10.30", role="prod"
                )

    def test_prod_webui_requires_numeric_uid_and_gid(self) -> None:
        for field in ("uid", "gid"):
            incomplete = self.prod_config().replace(f"{field} = 0\n", "")
            with self.subTest(field=field), self.assertRaisesRegex(
                ValueError, f"WebUI.*{field}"
            ):
                deploy.validate_server_config(
                    self.write_config(incomplete), "10.10.10.30", role="prod"
                )

    def test_prod_nfs_requires_exact_private_address_and_port(self) -> None:
        deploy.validate_server_config(
            self.write_config(self.prod_config()), "10.10.10.30", role="prod"
        )
        for address in ("0.0.0.0:2049", "10.10.10.30:2050", "10.10.10.31:2049"):
            unsafe = self.prod_config().replace("10.10.10.30:2049", address)
            with self.subTest(address=address), self.assertRaisesRegex(
                ValueError, "NFS.*private container address.*2049"
            ):
                deploy.validate_server_config(
                    self.write_config(unsafe), "10.10.10.30", role="prod"
                )

    def test_dev_rejects_webui_even_on_private_address(self) -> None:
        dev_with_webui = (
            self.valid_config()
            + '\n[servers.webui]\naddresses = ["10.10.10.20:8080"]\nuid = 0\ngid = 0\n'
        )
        with self.assertRaisesRegex(ValueError, "exclusive NBD"):
            deploy.validate_server_config(
                self.write_config(dev_with_webui), "10.10.10.20", role="dev"
            )

    def test_both_roles_cap_sftp_session_fields_at_four(self) -> None:
        too_many = self.prod_config().replace(
            "max_connections = 4", "max_connections = 8"
        )
        with self.assertRaisesRegex(ValueError, "SFTP.*four"):
            deploy.validate_server_config(
                self.write_config(too_many), "10.10.10.30", role="prod"
            )


class PlanTests(unittest.TestCase):
    def test_webui_node_version_gate_requires_vite_minimum(self) -> None:
        self.assertTrue(deploy.node_version_supported("v24.19.0"))
        self.assertTrue(deploy.node_version_supported("v20.19.0"))
        self.assertFalse(deploy.node_version_supported("v20.18.9"))
        self.assertFalse(deploy.node_version_supported("not-node"))

    def test_release_rustflags_enable_named_tasks_and_io_uring(self) -> None:
        self.assertEqual(
            deploy.release_rustflags("-C target-cpu=native"),
            "-C target-cpu=native --cfg tokio_unstable --cfg io_uring_skip_arch_check",
        )
        existing = "--cfg tokio_unstable --cfg io_uring_skip_arch_check"
        self.assertEqual(deploy.release_rustflags(existing), existing)

    def test_replace_requires_exact_ctid_confirmation(self) -> None:
        with self.assertRaisesRegex(ValueError, "ZEROFS_CONFIRM_REPLACE=120"):
            deploy.require_replace_confirmation(120, None)
        deploy.require_replace_confirmation(120, "120")

    def test_state_root_must_be_dedicated_to_zerofs(self) -> None:
        self.assertEqual(
            deploy.validate_state_root("/var/lib/zerofs-lxc/dev-120", 120, "dev"),
            Path("/var/lib/zerofs-lxc/dev-120"),
        )
        for unsafe in ("/", "/fast", "/var/lib/zerofs-lxc/prod-120"):
            with self.subTest(path=unsafe), self.assertRaises(ValueError):
                deploy.validate_state_root(unsafe, 120, "dev")

    def test_dry_run_plan_uses_private_bridge_and_never_formats_storage(self) -> None:
        plan = deploy.build_host_plan(
            action="replace",
            ctid=120,
            container_ip="10.10.10.20",
            bridge="vmbr1",
            template="local:vztmpl/debian-13-standard.tar.zst",
            state_root=Path("/var/lib/zerofs-lxc/dev-120"),
            memory_mb=65536,
            rootfs="local-lvm:8",
        )
        rendered = "\n".join(deploy.shell_join(command) for command in plan)
        self.assertIn("bridge=vmbr1", rendered)
        self.assertIn("ip=10.10.10.20/24", rendered)
        self.assertIn("gw=10.10.10.1", rendered)
        self.assertIn("mp=/srv/zerofs-persist", rendered)
        self.assertNotIn("mkfs", rendered)
        self.assertNotIn("0.0.0.0", rendered)


class CliDryRunTests(ConfigValidationTests):
    def run_cli(
        self,
        *extra: str,
        env: dict[str, str] | None = None,
        skip_existing: bool = True,
    ) -> subprocess.CompletedProcess[str]:
        command = [
            "python3",
            str(MODULE_PATH),
            *extra,
            "--ctid",
            "120",
            "--container-ip",
            "10.10.10.20",
            "--role",
            "dev",
            "--dry-run",
        ]
        if skip_existing:
            command.append("--skip-existing-drain")
        return subprocess.run(
            command,
            text=True,
            capture_output=True,
            env={**os.environ, **(env or {})},
            check=False,
        )

    def test_deploy_dry_run_prints_build_stage_and_reconnect_without_running_them(
        self,
    ) -> None:
        config = self.write_config(self.valid_config())
        result = self.run_cli("deploy", "--config", str(config))
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("cargo build --release --locked", result.stdout)
        self.assertIn("host-deploy.sh deploy", result.stdout)
        self.assertIn("systemctl start zerofs-lxc-nbd-client.service", result.stdout)
        self.assertNotIn("mkfs", result.stdout)

    def test_cleanup_dry_run_keeps_persistent_state(self) -> None:
        result = self.run_cli("cleanup")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("host-deploy.sh cleanup", result.stdout)
        self.assertNotIn(
            "/var/lib/zerofs-lxc/dev-120", result.stdout.split("rm -rf --")[-1]
        )

    def test_replace_dry_run_still_requires_exact_confirmation(self) -> None:
        config = self.write_config(self.valid_config())
        rejected = self.run_cli("replace", "--config", str(config))
        self.assertNotEqual(rejected.returncode, 0)
        accepted = self.run_cli(
            "replace",
            "--config",
            str(config),
            env={"ZEROFS_CONFIRM_REPLACE": "120"},
            skip_existing=False,
        )
        self.assertEqual(accepted.returncode, 0, accepted.stderr)

    def test_prod_rejects_destructive_replace_and_cleanup(self) -> None:
        for action in ("replace", "cleanup"):
            command = [
                "python3",
                str(MODULE_PATH),
                action,
                "--role",
                "prod",
                "--ctid",
                "130",
                "--container-ip",
                "10.10.10.30",
                "--dry-run",
            ]
            result = subprocess.run(
                command, text=True, capture_output=True, check=False
            )
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("prod", result.stderr)

    def test_prod_deploy_is_container_owned_and_does_not_touch_vm_nbd(self) -> None:
        config = self.write_config(self.prod_config())
        result = subprocess.run(
            [
                "python3",
                str(MODULE_PATH),
                "deploy",
                "--role",
                "prod",
                "--ctid",
                "130",
                "--container-ip",
                "10.10.10.30",
                "--config",
                str(config),
                "--dry-run",
            ],
            text=True,
            capture_output=True,
            check=False,
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("make webui", result.stdout)
        self.assertIn("host-deploy.sh deploy --role prod", result.stdout)
        self.assertIn("--features webui", result.stdout)
        self.assertIn("--prod-access nfs", result.stdout)
        self.assertNotIn("zerofs-lxc-nbd-client.service", result.stdout)
        self.assertNotIn("ubuntu-main bash -se", result.stdout)
        self.assertNotIn("smb.conf", result.stdout)
        self.assertNotIn("smbd.service", result.stdout)

    def test_prod_both_access_stages_samba_and_requires_password_on_apply(self) -> None:
        config = self.write_config(self.prod_config())
        result = subprocess.run(
            [
                "python3",
                str(MODULE_PATH),
                "deploy",
                "--role",
                "prod",
                "--prod-access",
                "both",
                "--ctid",
                "130",
                "--container-ip",
                "10.10.10.30",
                "--config",
                str(config),
                "--dry-run",
            ],
            text=True,
            capture_output=True,
            check=False,
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("smb.conf", result.stdout)
        self.assertIn("--prod-access both", result.stdout)

    def test_remote_namespace_identity_collides_across_roles_but_local_does_not(
        self,
    ) -> None:
        prod_root = Path("/var/lib/zerofs-lxc/prod-130")
        dev_root = Path("/var/lib/zerofs-lxc/dev-120")
        remote = "sftp://storage.example/data/shared"
        self.assertEqual(
            deploy.namespace_id(remote, "prod", prod_root),
            deploy.namespace_id(remote, "dev", dev_root),
        )
        self.assertEqual(
            deploy.namespace_id(remote + "/", "prod", prod_root),
            deploy.namespace_id("sftp://STORAGE.EXAMPLE/data/shared", "dev", dev_root),
        )
        local = "file:///srv/zerofs-persist/backend-dev"
        self.assertNotEqual(
            deploy.namespace_id(local, "prod", prod_root),
            deploy.namespace_id(local, "dev", dev_root),
        )

    def test_release_id_changes_when_config_changes(self) -> None:
        first = self.write_config(self.valid_config())
        second = self.write_config(
            self.valid_config().replace("disk_size_gb = 1000.0", "disk_size_gb = 900.0")
        )
        commit = "a" * 40
        self.assertNotEqual(
            deploy.release_id(commit, [first]),
            deploy.release_id(commit, [second]),
        )

    def test_migration_can_quiesce_old_units_without_overwriting_them(self) -> None:
        config = self.write_config(self.valid_config())
        result = self.run_cli(
            "deploy",
            "--config",
            str(config),
            "--source-client-unit",
            "zerofs-nbd-client.service",
            "--source-mount-unit",
            "mnt-storagebox-nbd-pilot.mount",
            "--source-mountpoint",
            "/mnt/storagebox-nbd-pilot",
            "--source-server-unit",
            "zerofs-nbd-pilot.service",
            "--existing-metrics-url",
            "http://127.0.0.1:19567/metrics",
            skip_existing=False,
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("systemctl stop mnt-storagebox-nbd-pilot.mount", result.stdout)
        self.assertIn("systemctl stop zerofs-nbd-client.service", result.stdout)
        self.assertIn("systemctl stop zerofs-nbd-pilot.service", result.stdout)
        self.assertIn(
            "install -m 0644 /tmp/zerofs-lxc-nbd-client.service "
            "/etc/systemd/system/zerofs-lxc-nbd-client.service",
            result.stdout,
        )


if __name__ == "__main__":
    unittest.main()
