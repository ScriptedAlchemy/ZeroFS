from __future__ import annotations

import importlib.util
import contextlib
import hashlib
import os
import subprocess
import sys
import tempfile
import tomllib
import unittest
from pathlib import Path
from types import SimpleNamespace


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
    def test_vm_nfs_mount_render_uses_the_requested_container_address(self) -> None:
        template = (
            Path(__file__).parents[1] / "systemd" / r"mnt-zerofs\x2dfiles.mount"
        ).read_text()
        renderer = getattr(deploy, "render_vm_nfs_mount", None)
        if renderer is None:
            self.fail("VM NFS mount renderer is missing")

        for container_ip in ("10.10.10.30", "10.10.10.55"):
            with self.subTest(container_ip=container_ip):
                rendered = renderer(template, container_ip)
                self.assertIn(f"What={container_ip}:/", rendered)
                if container_ip != "10.10.10.30":
                    self.assertNotIn("What=10.10.10.30:/", rendered)
                self.assertIn("Where=/mnt/zerofs-files", rendered)
                self.assertIn("Type=nfs", rendered)
                self.assertIn(
                    "Options=rw,noatime,hard,vers=3,proto=tcp,nolock,port=2049,"
                    "mountport=2049,rsize=1048576,wsize=1048576,actimeo=1,_netdev",
                    rendered,
                )

    def test_vm100_has_only_the_direct_persistent_nfs_mount(self) -> None:
        unit = (
            Path(__file__).parents[1] / "systemd" / r"mnt-zerofs\x2dfiles.mount"
        ).read_text()

        self.assertIn("What=10.10.10.30:/", unit)
        self.assertIn("Where=/mnt/zerofs-files", unit)
        self.assertIn("Type=nfs", unit)
        self.assertIn(
            "Options=rw,noatime,hard,vers=3,proto=tcp,nolock,port=2049,"
            "mountport=2049,rsize=1048576,wsize=1048576,actimeo=1,_netdev",
            unit,
        )
        self.assertIn("WantedBy=remote-fs.target", unit)
        self.assertNotIn("zerofs-lxc-nbd-client.service", unit)

        root = Path(__file__).parents[1]
        for obsolete in (
            Path("systemd") / r"mnt-zerofs\x2dfiles\x2draw.mount",
            Path("systemd") / r"mnt-zerofs\x2dfiles\x2draw-.nbd.mount",
            Path("systemd") / "zerofs-shared-namespace-permissions.service",
            Path("systemd") / "zerofs-lxc-nbd-client.service",
            Path("systemd") / "mnt-zerofs-lxc.mount",
            Path("guest") / "normalize-shared-namespace.py",
            Path("guest") / "tune-nbd.sh",
        ):
            with self.subTest(obsolete=obsolete):
                self.assertFalse((root / obsolete).exists())


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
memory_size_gb = 32.0

[runtime]
memory_limit_gb = 96.0

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

[servers.ninep.shared_identity]
uid = 501
gid = 20

[servers.nbd]
addresses = ["10.10.10.30:10809"]
unix_socket = "/run/zerofs/nbd.sock"
write_ack_mode = "materialized"

[servers.nfs]
addresses = ["10.10.10.30:2049"]

[servers.nfs.shared_identity]
uid = 501
gid = 20

[servers.rpc]
unix_socket = "/run/zerofs/rpc.sock"

[servers.webui]
addresses = ["10.10.10.30:8080"]
uid = 501
gid = 20

[prometheus]
addresses = ["10.10.10.30:9567"]
"""

    def test_private_container_config_is_accepted(self) -> None:
        deploy.validate_server_config(
            self.write_config(self.valid_config()), "10.10.10.20"
        )

    def test_bootstrap_config_exposes_only_nfs_rpc_and_metrics(self) -> None:
        rendered = deploy.render_nfs_bootstrap_config(self.prod_config())
        config = tomllib.loads(rendered)

        self.assertEqual(set(config["servers"]), {"nfs", "rpc"})
        self.assertEqual(
            config["servers"]["nfs"]["shared_identity"], {"uid": 501, "gid": 20}
        )
        self.assertEqual(config["prometheus"]["addresses"], ["10.10.10.30:9567"])

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

    def test_prod_enforces_bounded_clean_cache_and_distinct_writeback_ram(
        self,
    ) -> None:
        valid = self.prod_config()
        deploy.validate_server_config(
            self.write_config(valid), "10.10.10.30", memory_mb=98304, role="prod"
        )

        for label, unsafe, message in (
            (
                "oversized clean cache",
                valid.replace("memory_size_gb = 32.0", "memory_size_gb = 64.0", 1),
                "clean cache memory_size_gb must be 32.0",
            ),
            (
                "writeback RAM merged into clean cache budget",
                valid.replace("memory_size_gb = 4.0", "memory_size_gb = 32.0", 1),
                "writeback memory_size_gb must be 4.0",
            ),
        ):
            with self.subTest(label=label), self.assertRaisesRegex(ValueError, message):
                deploy.validate_server_config(
                    self.write_config(unsafe),
                    "10.10.10.30",
                    memory_mb=98304,
                    role="prod",
                )

    def test_prod_runtime_envelope_covers_unified_volatile_budget_and_reserves(
        self,
    ) -> None:
        valid = self.prod_config()
        deploy.validate_server_config(
            self.write_config(valid), "10.10.10.30", memory_mb=98304, role="prod"
        )

        for label, unsafe, message in (
            (
                "missing dedicated envelope",
                valid.replace("[runtime]\nmemory_limit_gb = 96.0\n\n", ""),
                "runtime.*memory_limit_gb.*required",
            ),
            (
                "envelope omits unified volatile and reserves",
                valid.replace("memory_limit_gb = 96.0", "memory_limit_gb = 80.0"),
                "unified volatile.*reserves",
            ),
            (
                "decimal envelope exceeds MiB CT limit",
                valid.replace("memory_limit_gb = 96.0", "memory_limit_gb = 103.1"),
                "exceeds.*container.*98304 MiB",
            ),
        ):
            with self.subTest(label=label), self.assertRaisesRegex(ValueError, message):
                deploy.validate_server_config(
                    self.write_config(unsafe),
                    "10.10.10.30",
                    memory_mb=98304,
                    role="prod",
                )

        at_ct_limit = valid.replace(
            "memory_limit_gb = 96.0", "memory_limit_gb = 103.079215104"
        )
        deploy.validate_server_config(
            self.write_config(at_ct_limit),
            "10.10.10.30",
            memory_mb=98304,
            role="prod",
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
        for field, value in (("uid", 501), ("gid", 20)):
            webui = (
                '[servers.webui]\naddresses = ["10.10.10.30:8080"]\n'
                "uid = 501\ngid = 20\n"
            )
            incomplete = self.prod_config().replace(
                webui, webui.replace(f"{field} = {value}\n", "")
            )
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

    def test_prod_requires_one_shared_namespace_identity(self) -> None:
        valid = self.prod_config()
        for label, unsafe in (
            (
                "nfs uid",
                valid.replace(
                    "[servers.nfs.shared_identity]\nuid = 501\ngid = 20",
                    "[servers.nfs.shared_identity]\nuid = 502\ngid = 20",
                ),
            ),
            (
                "nfs gid",
                valid.replace(
                    "[servers.nfs.shared_identity]\nuid = 501\ngid = 20",
                    "[servers.nfs.shared_identity]\nuid = 501\ngid = 21",
                ),
            ),
            (
                "ninep uid",
                valid.replace(
                    "[servers.ninep.shared_identity]\nuid = 501\ngid = 20",
                    "[servers.ninep.shared_identity]\nuid = 1000\ngid = 20",
                ),
            ),
            (
                "webui uid",
                valid.replace(
                    '[servers.webui]\naddresses = ["10.10.10.30:8080"]\nuid = 501\ngid = 20',
                    '[servers.webui]\naddresses = ["10.10.10.30:8080"]\nuid = 0\ngid = 20',
                ),
            ),
        ):
            with self.subTest(label=label), self.assertRaisesRegex(
                ValueError, "writable production frontends must use one shared identity"
            ):
                deploy.validate_server_config(
                    self.write_config(unsafe), "10.10.10.30", role="prod"
                )

        all_root = valid.replace("uid = 501\ngid = 20", "uid = 0\ngid = 0")
        with self.assertRaisesRegex(ValueError, "uid 501 and gid 20"):
            deploy.validate_server_config(
                self.write_config(all_root), "10.10.10.30", role="prod"
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
    def test_legacy_nbd_retirement_plan_has_safe_order_and_owned_device(self) -> None:
        plan = deploy.plan_legacy_nbd_retirement(
            deploy.LegacyNbdState(
                loaded_mount_units=frozenset(
                    {r"mnt-zerofs\x2dlxc.mount", "mnt-zerofs-lxc.mount"}
                ),
                client_loaded=True,
                client_active=True,
                mount_source="/dev/nbd0",
                device_connected=True,
            )
        )
        self.assertEqual(
            plan.actions,
            (
                "sync:/mnt/zerofs-lxc",
                r"disable:mnt-zerofs\x2dlxc.mount",
                "disable:mnt-zerofs-lxc.mount",
                "unmount:/mnt/zerofs-lxc",
                "disable:zerofs-lxc-nbd-client.service",
                "disconnect:/dev/nbd0",
                "remove-obsolete-artifacts",
            ),
        )

    def test_legacy_nbd_retirement_refuses_unknown_source_or_owner(self) -> None:
        with self.assertRaisesRegex(ValueError, "unexpected legacy mount source"):
            deploy.plan_legacy_nbd_retirement(
                deploy.LegacyNbdState(
                    loaded_mount_units=frozenset(),
                    client_loaded=False,
                    client_active=False,
                    mount_source="/dev/sdz",
                    device_connected=False,
                )
            )
        with self.assertRaisesRegex(ValueError, "without recognized legacy"):
            deploy.plan_legacy_nbd_retirement(
                deploy.LegacyNbdState(
                    loaded_mount_units=frozenset(),
                    client_loaded=False,
                    client_active=False,
                    mount_source=None,
                    device_connected=True,
                )
            )

    def test_shared_namespace_receipt_rejects_wrong_or_unverified_ownership(
        self,
    ) -> None:
        for output, message in (
            (
                "ZEROFS_SHARED_NAMESPACE_V1 verified=1 objects=224 wrong_owner=1 "
                "first_uid=0 first_gid=0 reason=ok",
                "wrong_owner=1.*chown --no-dereference 501:20.*wrong_owner=0",
            ),
            (
                "ZEROFS_SHARED_NAMESPACE_V1 verified=0 objects=0 wrong_owner=0 "
                "first_uid=-1 first_gid=-1 reason=mount_unavailable",
                "verified=0.*mount_unavailable",
            ),
        ):
            with self.subTest(output=output), self.assertRaisesRegex(
                ValueError, message
            ):
                deploy.validate_shared_namespace_ownership(
                    deploy.parse_shared_namespace_ownership_receipt(output)
                )
        with self.assertRaisesRegex(ValueError, "chown --no-dereference 501:20"):
            deploy.validate_shared_namespace_ownership(
                deploy.SharedNamespaceOwnershipReceipt(
                    objects=2, wrong_owner=1, first_uid=0, first_gid=0
                )
            )

    def test_guest_nfs_reconciler_has_recursive_fail_closed_preflight(self) -> None:
        script = Path(__file__).parents[1] / "guest/reconcile-zerofs-nfs.sh"
        source = script.read_text()
        self.assertIn("ZEROFS_SHARED_NAMESPACE_V1", source)
        self.assertIn('"$mountpoint/.nbd" -prune', source)
        self.assertIn("wrong_owner", source)
        self.assertIn("cmp -s", source)
        self.assertIn("! legacy_state_present", source)
        self.assertIn("ZeroFS NFS mount already reconciled", source)
        self.assertIn(r"mnt-zerofs\x2dlxc.mount", source)
        self.assertIn("/usr/local/libexec/zerofs-normalize-shared-namespace", source)
        self.assertIn("assert_file_hash", source)
        self.assertIn("assert_unit_fragment", source)
        self.assertIn("ZEROFS_NBD_DEVICE=/dev/nbd0", source)
        self.assertIn("ZEROFS_NBD_EXPORT=vm100-pilot-64g", source)
        self.assertIn("ZEROFS_NBD_CONNECTIONS=8", source)
        self.assertIn("legacy NBD client environment is not canonical", source)
        self.assertIn('rmdir -- "$legacy_mount"', source)
        self.assertNotIn(
            '"${legacy_namespace_artifacts[@]}" "${legacy_namespace_mounts[@]}"',
            source,
        )
        self.assertIn("What=${expected_source}", source)
        subprocess.run(["bash", "-n", str(script)], check=True)

    def test_guest_reconciler_accepts_only_exact_shipped_legacy_units(self) -> None:
        script = Path(__file__).parents[1] / "guest/reconcile-zerofs-nfs.sh"
        source = script.read_text()
        fixtures = {
            "99015e0989c4fda8c9377fd1c2e062890c2d83c4c3673ad3415d552d8e73ecdf": """[Unit]
Description=ZeroFS XFS volume from private LXC NBD server
Requires=zerofs-lxc-nbd-client.service
After=zerofs-lxc-nbd-client.service

[Mount]
What=/dev/nbd0
Where=/mnt/zerofs-lxc
Type=xfs
Options=rw,noatime,nodiscard
TimeoutSec=60s

[Install]
WantedBy=multi-user.target
""",
            "013e9481f2bf7e0ba66f4dbc60bba64937da88293f7f3732c0e62c7cb2c5b33d": """[Unit]
Description=ZeroFS file namespace mapped for VM100 and macOS ownership
Requires=mnt-zerofs\\x2dfiles\\x2draw-.nbd.mount
After=mnt-zerofs\\x2dfiles\\x2draw-.nbd.mount

[Mount]
What=/mnt/zerofs-files-raw
Where=/mnt/zerofs-files
Type=fuse.bindfs
Options=mirror=zack,create-for-user=501,create-for-group=20,chown-ignore,chgrp-ignore,chmod-ignore,_netdev
TimeoutSec=30s

[Install]
WantedBy=remote-fs.target
""",
            "9a0e6e3501a971c13b5d5ad7e609cc92989f83c197821f0c09596a02c3cbeac2": """[Unit]
Description=ZeroFS file namespace mapped for VM100 and macOS ownership
Requires=zerofs-shared-namespace-permissions.service
After=zerofs-shared-namespace-permissions.service

[Mount]
What=/mnt/zerofs-files-raw
Where=/mnt/zerofs-files
Type=fuse.bindfs
Options=mirror=zack,create-for-user=501,create-for-group=20,chown-ignore,chgrp-ignore,chmod-ignore,_netdev
TimeoutSec=30s

[Install]
WantedBy=remote-fs.target
""",
        }
        for expected, fixture in fixtures.items():
            with self.subTest(expected=expected):
                self.assertEqual(hashlib.sha256(fixture.encode()).hexdigest(), expected)
                self.assertIn(expected, source)
                altered = fixture.replace("[Mount]", "[Mount]\nWhere=/tmp/escape", 1)
                self.assertNotIn(hashlib.sha256(altered.encode()).hexdigest(), source)
        for label in (
            "legacy raw namespace unit contains unexpected directives",
            "legacy raw NBD guard unit",
            "legacy exposed NBD guard unit",
            "legacy namespace permissions service",
        ):
            self.assertIn(label, source)

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
        self.assertIn("--cores 8", rendered)
        self.assertIn("--memory 65536", rendered)
        self.assertIn("--swap 0", rendered)
        self.assertIn("--onboot 1", rendered)
        self.assertIn("--startup order=20", rendered)
        self.assertNotIn("mkfs", rendered)
        self.assertNotIn("0.0.0.0", rendered)


class VmNfsCoordinatorTests(unittest.TestCase):
    class RecordingRunner:
        dry_run = False

        def __init__(self, fail_token: str | None = None) -> None:
            self.fail_token = fail_token
            self.calls: list[str] = []

        @contextlib.contextmanager
        def remote_deployment_locks(self, args):
            self.calls.append("lock vm:global")
            try:
                yield
            finally:
                self.calls.append("unlock vm:global")

        def run(
            self,
            command: list[str],
            *,
            cwd: Path | None = None,
            capture: bool = False,
            input_text: str | None = None,
        ) -> subprocess.CompletedProcess[str]:
            del cwd, capture
            rendered = deploy.shell_join(command)
            if input_text:
                rendered += "\n" + input_text
            self.calls.append(rendered)
            if self.fail_token and self.fail_token in rendered:
                raise RuntimeError(f"injected {self.fail_token}")
            stdout = ""
            if "reconcile-zerofs-nfs.sh preflight " in rendered:
                stdout = (
                    "ZEROFS_SHARED_NAMESPACE_V1 verified=1 objects=9 "
                    "wrong_owner=0 first_uid=-1 first_gid=-1 reason=ok\n"
                )
            return subprocess.CompletedProcess(command, 0, stdout, "")

        def run_remote_shell(self, host: str, script: str) -> str | None:
            rendered = f"locked-shell {host}\n{script}"
            self.calls.append(rendered)
            if self.fail_token and self.fail_token in rendered:
                raise RuntimeError(f"injected {self.fail_token}")
            if "reconcile-zerofs-nfs.sh preflight " in rendered:
                return (
                    "ZEROFS_SHARED_NAMESPACE_V1 verified=1 objects=9 "
                    "wrong_owner=0 first_uid=-1 first_gid=-1 reason=ok\n"
                )
            return ""

    def args(self) -> SimpleNamespace:
        return SimpleNamespace(
            vm_host="ubuntu-main",
            pve_host="pve",
            ctid=198,
            container_ip="10.10.10.55",
        )

    def transaction_runner(self):
        transaction = getattr(deploy, "_run_prod_vm_nfs_transaction", None)
        self.assertIsNotNone(transaction, "production VM NFS transaction is missing")
        return transaction

    @staticmethod
    def actions(calls: list[str]) -> list[str]:
        found: list[str] = []
        for call in calls:
            for action in (
                "recover",
                "prepare",
                "quiesce",
                "reconcile",
                "decide",
                "rollback",
                "commit",
            ):
                if f"vm_nfs_transition.py {action} " in call:
                    found.append(action)
        return found

    def test_prod_nfs_transition_wraps_host_deploy_transactionally(self) -> None:
        runner = self.RecordingRunner()
        host_events: list[str] = []

        self.transaction_runner()(
            runner,
            self.args(),
            "0123456789ab-cccccccccccccccc",
            lambda: host_events.append("activated"),
            lambda: host_events.append("committed"),
            lambda: host_events.append("rolled-back"),
            lambda: host_events.append("recovered"),
        )

        self.assertEqual(host_events, ["recovered", "activated", "committed"])
        self.assertEqual(runner.calls[0], "lock vm:global")
        self.assertEqual(runner.calls[-1], "unlock vm:global")
        self.assertEqual(
            self.actions(runner.calls),
            ["recover", "prepare", "quiesce", "reconcile", "decide", "commit"],
        )

    def test_every_mutating_vm_command_runs_inside_the_lock_session(self) -> None:
        runner = self.RecordingRunner()

        self.transaction_runner()(
            runner,
            self.args(),
            "0123456789ab-cccccccccccccccc",
            lambda: None,
            lambda: None,
            lambda: None,
            lambda: None,
        )

        direct_vm_ssh = [
            call
            for call in runner.calls
            if call.startswith("ssh -o BatchMode=yes ubuntu-main")
        ]
        self.assertEqual(direct_vm_ssh, [])
        self.assertTrue(
            any(
                call.startswith("locked-shell ubuntu-main")
                and "reconcile-zerofs-nfs.sh reconcile " in call
                for call in runner.calls
            )
        )

    def test_absent_initial_mount_is_proven_after_bootstrap_not_fabricated(
        self,
    ) -> None:
        class BootstrapRunner(self.RecordingRunner):
            def __init__(inner_self):
                super().__init__()
                inner_self.preflights = 0

            def run_remote_shell(inner_self, host, script):
                result = super().run_remote_shell(host, script)
                rendered = script
                if "reconcile-zerofs-nfs.sh preflight " in rendered:
                    inner_self.preflights += 1
                    if inner_self.preflights == 1:
                        return (
                            "ZEROFS_SHARED_NAMESPACE_V1 verified=0 objects=0 "
                            "wrong_owner=0 first_uid=-1 first_gid=-1 "
                            "reason=mount_unavailable\n"
                        )
                return result

        runner = BootstrapRunner()
        events: list[str] = []
        self.transaction_runner()(
            runner,
            self.args(),
            "0123456789ab-cccccccccccccccc",
            lambda: self.fail("full activation must wait for ownership proof"),
            lambda: events.append("commit"),
            lambda: events.append("rollback"),
            lambda: events.append("recover"),
            activate_maintenance=lambda: events.append("maintenance"),
            promote_host=lambda: events.append("promote"),
        )

        self.assertEqual(runner.preflights, 2)
        self.assertEqual(events, ["recover", "maintenance", "promote", "commit"])
        self.assertEqual(
            self.actions(runner.calls),
            [
                "recover",
                "prepare",
                "quiesce",
                "reconcile",
                "quiesce",
                "reconcile",
                "decide",
                "commit",
            ],
        )

    def test_recognized_legacy_bindfs_uses_the_transactional_bootstrap_path(
        self,
    ) -> None:
        class LegacyRunner(self.RecordingRunner):
            def run_remote_shell(inner_self, host, script):
                if "reconcile-zerofs-nfs.sh preflight " in script:
                    if not hasattr(inner_self, "preflighted"):
                        inner_self.preflighted = True
                        return (
                            "ZEROFS_SHARED_NAMESPACE_V1 verified=0 objects=0 "
                            "wrong_owner=0 first_uid=-1 first_gid=-1 "
                            "reason=legacy_topology\n"
                        )
                return super().run_remote_shell(host, script)

        runner = LegacyRunner()
        events: list[str] = []
        self.transaction_runner()(
            runner,
            self.args(),
            "0123456789ab-cccccccccccccccc",
            lambda: self.fail("legacy cutover requires maintenance activation"),
            lambda: events.append("commit"),
            lambda: events.append("rollback"),
            lambda: events.append("recover"),
            activate_maintenance=lambda: events.append("maintenance"),
            promote_host=lambda: events.append("promote"),
        )

        prepare = next(
            call for call in runner.calls if "vm_nfs_transition.py prepare " in call
        )
        self.assertIn("--allow-legacy-bindfs", prepare)
        self.assertEqual(events, ["recover", "maintenance", "promote", "commit"])

    def test_bootstrap_promotion_failure_restores_host_before_vm(self) -> None:
        trace: list[str] = []

        class BootstrapRunner(self.RecordingRunner):
            def __init__(inner_self):
                super().__init__()
                inner_self.preflights = 0

            def run_remote_shell(inner_self, host, script):
                result = super().run_remote_shell(host, script)
                if "vm_nfs_transition.py rollback " in script:
                    trace.append("vm-rollback")
                if "reconcile-zerofs-nfs.sh preflight " in script:
                    inner_self.preflights += 1
                    if inner_self.preflights == 1:
                        return (
                            "ZEROFS_SHARED_NAMESPACE_V1 verified=0 objects=0 "
                            "wrong_owner=0 first_uid=-1 first_gid=-1 "
                            "reason=mount_unavailable\n"
                        )
                return result

        runner = BootstrapRunner()
        with self.assertRaisesRegex(RuntimeError, "promotion failed"):
            self.transaction_runner()(
                runner,
                self.args(),
                "0123456789ab-cccccccccccccccc",
                lambda: self.fail("full activation must not run"),
                lambda: self.fail("host commit must not run"),
                lambda: trace.append("host-rollback"),
                lambda: trace.append("host-recover"),
                activate_maintenance=lambda: trace.append("maintenance"),
                promote_host=lambda: (_ for _ in ()).throw(
                    RuntimeError("promotion failed")
                ),
            )

        self.assertEqual(
            trace,
            ["host-recover", "maintenance", "host-rollback", "vm-rollback"],
        )

    def test_prod_nfs_transition_rolls_back_each_mutating_failure_phase(self) -> None:
        runner = self.RecordingRunner("vm_nfs_transition.py prepare ")
        with self.assertRaisesRegex(RuntimeError, "injected"):
            self.transaction_runner()(
                runner,
                self.args(),
                "0123456789ab-cccccccccccccccc",
                lambda: self.fail("host deploy must not run"),
                lambda: self.fail("host commit must not run"),
                lambda: self.fail("host rollback must not run"),
                lambda: None,
            )
        self.assertEqual(self.actions(runner.calls), ["recover", "prepare"])

        for phase in ("quiesce", "reconcile"):
            with self.subTest(phase=phase):
                runner = self.RecordingRunner(f"vm_nfs_transition.py {phase} ")
                with self.assertRaisesRegex(RuntimeError, "injected"):
                    self.transaction_runner()(
                        runner,
                        self.args(),
                        "0123456789ab-cccccccccccccccc",
                        lambda: None,
                        lambda: None,
                        lambda: None,
                        lambda: None,
                    )
                self.assertEqual(
                    self.actions(runner.calls)[-2:], ["rollback", "commit"]
                )

        runner = self.RecordingRunner()
        with self.assertRaisesRegex(RuntimeError, "injected host deploy"):
            self.transaction_runner()(
                runner,
                self.args(),
                "0123456789ab-cccccccccccccccc",
                lambda: (_ for _ in ()).throw(RuntimeError("injected host deploy")),
                lambda: self.fail("host commit must not run"),
                lambda: self.fail("host rollback must not run"),
                lambda: None,
            )
        self.assertEqual(
            self.actions(runner.calls),
            ["recover", "prepare", "quiesce", "rollback", "commit"],
        )

    def test_prod_nfs_staging_failure_does_not_quiesce_or_reconcile(self) -> None:
        runner = self.RecordingRunner("vm_nfs_transition.py")
        with self.assertRaisesRegex(RuntimeError, "injected"):
            self.transaction_runner()(
                runner,
                self.args(),
                "0123456789ab-cccccccccccccccc",
                lambda: self.fail("host deploy must not run"),
                lambda: self.fail("host commit must not run"),
                lambda: self.fail("host rollback must not run"),
                lambda: None,
            )
        self.assertEqual(self.actions(runner.calls), [])

    def test_reconcile_failure_compensates_host_before_vm_rollback(self) -> None:
        trace: list[str] = []

        class TraceRunner(self.RecordingRunner):
            def run_remote_shell(inner_self, host, script):
                if "vm_nfs_transition.py rollback " in script:
                    trace.append("vm-rollback")
                return super().run_remote_shell(host, script)

        runner = TraceRunner("vm_nfs_transition.py reconcile ")
        with self.assertRaisesRegex(RuntimeError, "injected"):
            self.transaction_runner()(
                runner,
                self.args(),
                "0123456789ab-cccccccccccccccc",
                lambda: trace.append("host-activate"),
                lambda: trace.append("host-commit"),
                lambda: trace.append("host-rollback"),
                lambda: trace.append("host-recover"),
            )

        self.assertEqual(
            trace,
            ["host-recover", "host-activate", "host-rollback", "vm-rollback"],
        )

    def test_host_rollback_failure_preserves_quiesced_vm_transaction(self) -> None:
        runner = self.RecordingRunner("vm_nfs_transition.py reconcile ")
        with self.assertRaisesRegex(RuntimeError, "injected") as caught:
            self.transaction_runner()(
                runner,
                self.args(),
                "0123456789ab-cccccccccccccccc",
                lambda: None,
                lambda: self.fail("host commit must not run"),
                lambda: (_ for _ in ()).throw(RuntimeError("host rollback failed")),
                lambda: None,
            )

        self.assertIn(
            "host rollback also failed", "\n".join(caught.exception.__notes__)
        )
        self.assertNotIn("rollback", self.actions(runner.calls))

    def test_host_commit_failure_keeps_reconciled_vm_and_transaction_token(
        self,
    ) -> None:
        runner = self.RecordingRunner()
        with self.assertRaisesRegex(RuntimeError, "host commit failed"):
            self.transaction_runner()(
                runner,
                self.args(),
                "0123456789ab-cccccccccccccccc",
                lambda: None,
                lambda: (_ for _ in ()).throw(RuntimeError("host commit failed")),
                lambda: self.fail("host rollback must not run after VM commit"),
                lambda: None,
            )

        self.assertEqual(
            self.actions(runner.calls),
            ["recover", "prepare", "quiesce", "reconcile", "decide"],
        )

    def test_retry_finishes_a_decided_commit_without_rollback(self) -> None:
        class DecidedRunner(self.RecordingRunner):
            def run_remote_shell(inner_self, host, script):
                if "vm_nfs_transition.py status " in script:
                    return (
                        '{"phase":"commit_decided","deployment":'
                        '{"ctid":198,"pve_host":"pve",'
                        '"release":"0123456789ab-cccccccccccccccc",'
                        '"source":"10.10.10.55:/"}}\n'
                    )
                return super().run_remote_shell(host, script)

        runner = DecidedRunner()
        events: list[str] = []
        self.transaction_runner()(
            runner,
            self.args(),
            "0123456789ab-cccccccccccccccc",
            lambda: self.fail("activation must not repeat"),
            lambda: events.append("finalize-host"),
            lambda: self.fail("decided commit must not roll back"),
            lambda: self.fail("decided commit must not run host rollback recovery"),
        )

        self.assertEqual(events, ["finalize-host"])
        self.assertEqual(self.actions(runner.calls), ["commit"])

    def test_retry_rejects_a_decided_commit_for_another_deployment(self) -> None:
        class OtherDeploymentRunner(self.RecordingRunner):
            def run_remote_shell(inner_self, host, script):
                if "vm_nfs_transition.py status " in script:
                    return (
                        '{"phase":"commit_decided","deployment":'
                        '{"ctid":198,"pve_host":"pve",'
                        '"release":"older-release",'
                        '"source":"10.10.10.55:/"}}\n'
                    )
                return super().run_remote_shell(host, script)

        runner = OtherDeploymentRunner()
        events: list[str] = []
        with self.assertRaisesRegex(RuntimeError, "belongs to another deployment"):
            self.transaction_runner()(
                runner,
                self.args(),
                "0123456789ab-cccccccccccccccc",
                lambda: self.fail("activation must not run"),
                lambda: events.append("finalize-host"),
                lambda: self.fail("rollback must not run"),
                lambda: self.fail("host recovery must not run"),
            )

        self.assertEqual(events, [])
        self.assertEqual(self.actions(runner.calls), [])

    def test_ambiguous_commit_decision_failure_is_never_compensated(self) -> None:
        runner = self.RecordingRunner("vm_nfs_transition.py decide ")
        with self.assertRaisesRegex(RuntimeError, "injected") as caught:
            self.transaction_runner()(
                runner,
                self.args(),
                "0123456789ab-cccccccccccccccc",
                lambda: None,
                lambda: self.fail("host finalization must not run"),
                lambda: self.fail("ambiguous decision must not roll back host"),
                lambda: None,
            )

        self.assertIn("may be durable", "\n".join(caught.exception.__notes__))
        self.assertNotIn("rollback", self.actions(runner.calls))


class DeploymentLockTests(unittest.TestCase):
    def test_lock_owner_executes_remote_commands_and_reports_failures(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            lock = Path(directory) / "coordinator.lock"
            holder = "flock() { return 0; }; " + deploy._remote_lock_holder(str(lock))
            lease = deploy._FlockLease(["bash", "-c", holder], dry_run=False)

            with lease:
                self.assertEqual(
                    lease.execute("printf 'inside-lock\\n'"), "inside-lock\n"
                )
                with self.assertRaisesRegex(
                    RuntimeError, "locked remote command exited 17"
                ):
                    lease.execute("printf 'failed\\n'; exit 17")

    def test_nonblocking_flock_rejects_a_concurrent_coordinator(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            lock = Path(directory) / "coordinator.lock"
            script = """
import fcntl
import sys
handle = open(sys.argv[1], "w")
try:
    fcntl.lockf(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
except BlockingIOError:
    raise SystemExit(75)
print("LOCKED", flush=True)
sys.stdin.read()
"""
            command = [sys.executable, "-u", "-c", script, str(lock)]
            first = deploy._FlockLease(command, dry_run=False)
            second = deploy._FlockLease(command, dry_run=False)

            with first:
                with self.assertRaisesRegex(RuntimeError, "another ZeroFS deployment"):
                    second.__enter__()

    def test_runner_fences_commands_after_a_lock_lease_is_lost(self) -> None:
        runner = deploy.Runner(dry_run=False)
        dead_process = SimpleNamespace(poll=lambda: 75)
        runner._active_leases.append(SimpleNamespace(process=dead_process))

        with self.assertRaisesRegex(RuntimeError, "lock lease was lost"):
            runner.run(["/usr/bin/true"])


class OwnershipMigrationTests(unittest.TestCase):
    def run_cli(self, action: str, *extra: str, env=None):
        return subprocess.run(
            [
                sys.executable,
                str(MODULE_PATH),
                action,
                "--role",
                "prod",
                "--ctid",
                "198",
                "--container-ip",
                "10.10.10.55",
                *extra,
            ],
            text=True,
            capture_output=True,
            check=False,
            env={**os.environ, **(env or {})},
        )

    def test_dry_run_inventory_is_non_mutating_and_uses_the_fixed_target(self) -> None:
        result = self.run_cli("ownership-inventory", "--dry-run")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("repair-zerofs-ownership.sh inventory", result.stdout)
        self.assertIn("10.10.10.55:/", result.stdout)
        self.assertNotIn("repair-zerofs-ownership.sh repair", result.stdout)

        repair_plan = self.run_cli("ownership-repair", "--dry-run")
        self.assertEqual(repair_plan.returncode, 0, repair_plan.stderr)
        self.assertIn("repair-zerofs-ownership.sh inventory", repair_plan.stdout)

    def test_live_repair_requires_matching_flag_and_environment_confirmation(
        self,
    ) -> None:
        missing = self.run_cli("ownership-repair")
        self.assertNotEqual(missing.returncode, 0)
        self.assertIn("501:20", missing.stderr)

        flag_only = self.run_cli(
            "ownership-repair", "--confirm-ownership-repair", "501:20"
        )
        self.assertNotEqual(flag_only.returncode, 0)
        self.assertIn("ZEROFS_CONFIRM_OWNERSHIP_REPAIR", flag_only.stderr)

    def test_repair_script_is_resumable_and_does_not_follow_or_cross(self) -> None:
        script = Path(__file__).parents[1] / "guest/repair-zerofs-ownership.sh"
        source = script.read_text()
        self.assertIn("-xdev", source)
        self.assertIn('"$mountpoint/.nbd" -prune', source)
        self.assertIn("chown --no-dereference", source)
        self.assertIn("repair requires exact confirmation 501:20", source)
        self.assertIn('! -uid "$target_uid" -o ! -gid "$target_gid"', source)
        self.assertIn("durable_receipt=", source)
        self.assertIn("mv -f --", source)
        self.assertIn('sync -f "$temporary"', source)
        self.assertIn("os.fsync", source)
        subprocess.run(["bash", "-n", str(script)], check=True)


class CliDryRunTests(ConfigValidationTests):
    def assert_direct_nfs_mount_is_provisioned(
        self, result: subprocess.CompletedProcess[str], container_ip: str
    ) -> None:
        self.assertIn("ubuntu-main bash -se", result.stdout)
        self.assertIn(r"mnt-zerofs\x2dfiles.mount", result.stdout)
        self.assertIn("vm_nfs_transition.py", result.stdout)
        self.assertIn("reconcile-zerofs-nfs.sh", result.stdout)
        ordered = [
            result.stdout.index(f"vm_nfs_transition.py {action}")
            for action in ("recover", "prepare", "quiesce", "reconcile", "commit")
        ]
        self.assertEqual(ordered, sorted(ordered))
        self.assertIn(f"--expected-source {container_ip}:/", result.stdout)
        self.assertLess(
            result.stdout.index(f"reconcile-zerofs-nfs.sh preflight {container_ip}:/"),
            result.stdout.index("vm_nfs_transition.py quiesce"),
        )
        self.assertLess(
            result.stdout.index("host-deploy.sh deploy --role prod"),
            result.stdout.index(f"reconcile-zerofs-nfs.sh reconcile {container_ip}:/"),
        )
        self.assertNotIn("bindfs", result.stdout)
        self.assertNotIn("nbd-client -d", result.stdout)

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

    def test_dev_nbd_deploy_never_stages_or_reconciles_vm_nfs(self) -> None:
        config = self.write_config(self.valid_config())
        result = self.run_cli("deploy", "--config", str(config))
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("cargo build --release --locked", result.stdout)
        self.assertIn("host-deploy.sh deploy", result.stdout)
        self.assertNotIn("ubuntu-main", result.stdout)
        self.assertNotIn(r"mnt-zerofs\x2dfiles.mount", result.stdout)
        self.assertNotIn("/mnt/zerofs-files", result.stdout)
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

    def test_prod_deploy_transactionally_reconciles_the_single_vm_nfs_mount(
        self,
    ) -> None:
        config = self.write_config(
            self.prod_config().replace("10.10.10.30", "10.10.10.55")
        )
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
                "10.10.10.55",
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
        self.assertIn("--defer-commit", result.stdout)
        self.assertIn("zerofs-vm-nfs-global.coordinator.lock", result.stdout)
        self.assertNotIn("--coordinator-lock-held", result.stdout)
        self.assertIn("host-deploy.sh finalize --role prod", result.stdout)
        self.assertNotIn("host-deploy.sh rollback --role prod", result.stdout)
        self.assertIn("--features webui", result.stdout)
        self.assertIn("--prod-access nfs", result.stdout)
        self.assert_direct_nfs_mount_is_provisioned(result, "10.10.10.55")
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

    def test_migration_quiesces_legacy_nbd_without_restoring_it(self) -> None:
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
        self.assertIn(
            "systemctl disable --now mnt-storagebox-nbd-pilot.mount", result.stdout
        )
        self.assertIn(
            "systemctl disable --now zerofs-nbd-client.service", result.stdout
        )
        self.assertIn(
            "systemctl is-enabled --quiet mnt-storagebox-nbd-pilot.mount",
            result.stdout,
        )
        self.assertIn(
            "systemctl is-enabled --quiet zerofs-nbd-client.service",
            result.stdout,
        )
        self.assertIn("systemctl stop zerofs-nbd-pilot.service", result.stdout)
        self.assertIn("findmnt -nro SOURCE -M /mnt/storagebox-nbd-pilot", result.stdout)
        self.assertIn("unexpected legacy NBD mount source", result.stdout)
        self.assertNotIn(
            "install -m 0644 /tmp/zerofs-lxc-nbd-client.service",
            result.stdout,
        )
        self.assertNotIn("systemctl start zerofs-lxc-nbd-client.service", result.stdout)
        self.assertNotIn("systemctl start mnt-zerofs-lxc.mount", result.stdout)
        self.assertNotIn(
            "systemctl enable --now mnt-storagebox-nbd-pilot.mount", result.stdout
        )
        self.assertNotIn(
            "systemctl enable --now zerofs-nbd-client.service", result.stdout
        )

    def test_dev_migration_rejects_the_production_nfs_unit_and_mount(self) -> None:
        config = self.write_config(self.valid_config())
        result = self.run_cli(
            "deploy",
            "--config",
            str(config),
            "--source-client-unit",
            "mnt-zerofs\\x2dfiles.mount",
            "--source-mount-unit",
            "mnt-zerofs\\x2dfiles.mount",
            "--source-mountpoint",
            "/mnt/zerofs-files",
            "--source-server-unit",
            "zerofs-lxc.service",
            skip_existing=False,
        )
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("documented legacy NBD pilot", result.stderr)
        self.assertNotIn("systemctl disable", result.stdout)


if __name__ == "__main__":
    unittest.main()
