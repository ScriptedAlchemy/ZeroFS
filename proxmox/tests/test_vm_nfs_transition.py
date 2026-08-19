from __future__ import annotations

import dataclasses
import importlib.util
import json
import os
import re
import sys
import tempfile
import unittest
from unittest import mock
from pathlib import Path


MODULE_PATH = Path(__file__).parents[1] / "vm_nfs_transition.py"
transition = None
if MODULE_PATH.is_file():
    SPEC = importlib.util.spec_from_file_location(
        "zerofs_vm_nfs_transition", MODULE_PATH
    )
    assert SPEC is not None and SPEC.loader is not None
    transition = importlib.util.module_from_spec(SPEC)
    sys.modules[SPEC.name] = transition
    SPEC.loader.exec_module(transition)


class FakeSystem:
    def __init__(self, unit_path: Path) -> None:
        self.unit_path = unit_path
        self.enabled = False
        self.active = False
        self.mount = None
        self.loaded_units: set[str] = set()
        self.other_mounts: dict[str, object] = {}
        self.commands: list[str] = []
        self.fail_command: str | None = None

    def record_command(self, command: str) -> None:
        self.commands.append(command)
        if self.fail_command == command.split()[0]:
            raise RuntimeError(f"injected {self.fail_command}")

    def is_enabled(self, unit: str) -> bool:
        return self.enabled

    def is_active(self, unit: str) -> bool:
        return self.active

    def is_loaded(self, unit: str) -> bool:
        return unit in self.loaded_units

    def mount_record(self, mountpoint: Path):
        if str(mountpoint) in self.other_mounts:
            return self.other_mounts[str(mountpoint)]
        return self.mount if mountpoint.name == "zerofs-files" else None

    def daemon_reload(self) -> None:
        self.record_command("daemon-reload")

    def enable(self, unit: str) -> None:
        self.record_command(f"enable {unit}")
        self.enabled = True

    def disable(self, unit: str) -> None:
        self.record_command(f"disable {unit}")
        self.enabled = False

    def start(self, unit: str) -> None:
        self.record_command(f"start {unit}")
        if unit != transition.UNIT_NAME:
            self.loaded_units.add(unit)
            return
        self.active = True
        source = re.search(r"^What=(.+)$", self.unit_path.read_text(), re.MULTILINE)
        assert source is not None
        if source.group(1) == "/mnt/zerofs-files-raw":
            self.mount = transition.MountRecord(
                source=source.group(1), fstype="fuse.bindfs", options=("rw",)
            )
        else:
            self.mount = self.record(source.group(1))

    def stop(self, unit: str) -> None:
        self.record_command(f"stop {unit}")
        self.active = False
        self.mount = None

    @staticmethod
    def record(source: str):
        assert transition is not None
        return transition.MountRecord(
            source=source, fstype="nfs", options=("rw", "hard", "vers=3")
        )


class VmNfsTransitionTests(unittest.TestCase):
    def setUp(self) -> None:
        self.assertIsNotNone(transition, "VM NFS transition helper is missing")
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        root = Path(self.temp.name)
        self.unit_path = root / r"mnt-zerofs\x2dfiles.mount"
        self.mountpoint = root / "mnt" / "zerofs-files"
        self.staged = root / "staged.mount"
        self.transaction = root / "transaction"
        self.system = FakeSystem(self.unit_path)
        self.manager = transition.Transition(
            self.system,
            unit_path=self.unit_path,
            mountpoint=self.mountpoint,
            forbidden_units=("zerofs-lxc-nbd-client.service",),
            forbidden_mounts=(root / "mnt" / "zerofs-files-raw",),
        )
        self.identity = transition.DeploymentIdentity(
            ctid=198,
            release="0123456789ab-cccccccccccccccc",
            source="10.10.10.55:/",
            pve_host="pve",
        )

    @staticmethod
    def unit(source: str) -> str:
        return f"[Mount]\nWhat={source}\nWhere=/mnt/zerofs-files\nType=nfs\nOptions=rw,hard\n"

    def test_reconcile_is_a_true_noop_when_unit_and_mount_are_correct(self) -> None:
        desired = self.unit("10.10.10.55:/")
        self.unit_path.write_text(desired)
        self.staged.write_text(desired)
        self.system.enabled = True
        self.system.active = True
        self.system.mount = self.system.record("10.10.10.55:/")
        self.manager.prepare(
            self.staged, self.transaction, "10.10.10.55:/", self.identity
        )

        self.manager.reconcile(self.transaction)

        self.assertEqual(self.system.commands, [])
        self.assertEqual(self.unit_path.read_text(), desired)
        self.assertTrue(self.system.enabled)
        self.assertTrue(self.system.active)

    def test_rollback_restores_prior_unit_enablement_and_mount(self) -> None:
        old = self.unit("10.10.10.55:/").replace(
            "Options=rw,hard", "Options=rw,hard,noatime"
        )
        desired = self.unit("10.10.10.55:/")
        self.unit_path.write_text(old)
        self.staged.write_text(desired)
        self.system.enabled = True
        self.system.active = True
        self.system.mount = self.system.record("10.10.10.55:/")
        self.mountpoint.mkdir(parents=True)
        self.manager.prepare(
            self.staged, self.transaction, "10.10.10.55:/", self.identity
        )

        self.manager.quiesce(self.transaction)
        self.manager.reconcile(self.transaction)
        self.manager.rollback(self.transaction)

        self.assertEqual(self.unit_path.read_text(), old)
        self.assertTrue(self.system.enabled)
        self.assertTrue(self.system.active)
        self.assertEqual(self.system.mount.source, "10.10.10.55:/")
        self.assertTrue(self.mountpoint.is_dir())

    def test_partial_reconcile_failure_remains_rollback_safe(self) -> None:
        old = self.unit("10.10.10.55:/").replace(
            "Options=rw,hard", "Options=rw,hard,noatime"
        )
        desired = self.unit("10.10.10.55:/")
        self.unit_path.write_text(old)
        self.staged.write_text(desired)
        self.system.enabled = True
        self.system.active = True
        self.system.mount = self.system.record("10.10.10.55:/")
        self.mountpoint.mkdir(parents=True)
        self.manager.prepare(
            self.staged, self.transaction, "10.10.10.55:/", self.identity
        )
        self.manager.quiesce(self.transaction)
        self.system.fail_command = "start"

        with self.assertRaisesRegex(RuntimeError, "injected start"):
            self.manager.reconcile(self.transaction)
        self.system.fail_command = None
        self.manager.rollback(self.transaction)

        self.assertEqual(self.unit_path.read_text(), old)
        self.assertTrue(self.system.enabled)
        self.assertTrue(self.system.active)
        self.assertEqual(self.system.mount.source, "10.10.10.55:/")

    def test_rollback_removes_mountpoint_created_for_a_prior_absent_state(self) -> None:
        self.staged.write_text(self.unit("10.10.10.55:/"))
        self.manager.prepare(
            self.staged, self.transaction, "10.10.10.55:/", self.identity
        )
        self.manager.reconcile(self.transaction)
        self.assertTrue(self.mountpoint.is_dir())

        self.manager.rollback(self.transaction)

        self.assertFalse(self.unit_path.exists())
        self.assertFalse(self.mountpoint.exists())
        self.assertFalse(self.system.enabled)
        self.assertFalse(self.system.active)

    def test_rollback_fsyncs_unit_directory_after_removing_new_unit(self) -> None:
        self.unit_path = (
            self.unit_path.parent / "etc/systemd/system" / self.unit_path.name
        )
        self.manager.unit_path = self.unit_path
        self.system.unit_path = self.unit_path
        self.staged.write_text(self.unit("10.10.10.55:/"))
        self.manager.prepare(
            self.staged, self.transaction, "10.10.10.55:/", self.identity
        )
        self.manager.reconcile(self.transaction)

        with mock.patch.object(
            self.manager, "_fsync_directory", wraps=self.manager._fsync_directory
        ) as fsync_directory:
            self.manager.rollback(self.transaction)

        fsync_directory.assert_any_call(self.unit_path.parent)

    def test_reconcile_fails_closed_when_legacy_mount_or_unit_exists(self) -> None:
        desired = self.unit("10.10.10.55:/")
        self.staged.write_text(desired)
        self.manager.prepare(
            self.staged, self.transaction, "10.10.10.55:/", self.identity
        )

        self.system.loaded_units.add("zerofs-lxc-nbd-client.service")
        with self.assertRaisesRegex(RuntimeError, "legacy.*unit"):
            self.manager.reconcile(self.transaction)
        self.system.loaded_units.clear()
        forbidden = self.manager.forbidden_mounts[0]
        self.system.other_mounts[str(forbidden)] = self.system.record("/dev/nbd0")
        with self.assertRaisesRegex(RuntimeError, "legacy.*mount"):
            self.manager.reconcile(self.transaction)

    def test_reconcile_rejects_a_non_v3_live_nfs_mount(self) -> None:
        desired = self.unit("10.10.10.55:/")
        self.unit_path.write_text(desired)
        self.staged.write_text(desired)
        self.system.enabled = True
        self.system.active = True
        self.system.mount = transition.MountRecord(
            source="10.10.10.55:/",
            fstype="nfs",
            options=("rw", "hard", "vers=4.2"),
        )
        self.manager.prepare(
            self.staged, self.transaction, "10.10.10.55:/", self.identity
        )

        with self.assertRaisesRegex(RuntimeError, "nfs v3 rw"):
            self.manager.reconcile(self.transaction)

    def test_commit_removes_only_the_owned_transaction(self) -> None:
        self.staged.write_text(self.unit("10.10.10.55:/"))
        self.manager.prepare(
            self.staged, self.transaction, "10.10.10.55:/", self.identity
        )
        sibling = self.transaction.parent / "unowned"
        sibling.mkdir()

        self.manager.commit(self.transaction)

        self.assertFalse(self.transaction.exists())
        self.assertTrue(sibling.exists())

    def test_recover_rolls_back_and_removes_an_interrupted_transaction(self) -> None:
        old = self.unit("10.10.10.55:/").replace(
            "Options=rw,hard", "Options=rw,hard,noatime"
        )
        self.unit_path.write_text(old)
        self.staged.write_text(self.unit("10.10.10.55:/"))
        self.mountpoint.mkdir(parents=True)
        self.system.enabled = True
        self.system.active = True
        self.system.mount = self.system.record("10.10.10.55:/")
        self.manager.prepare(
            self.staged, self.transaction, "10.10.10.55:/", self.identity
        )
        self.manager.quiesce(self.transaction)

        recover = getattr(self.manager, "recover", None)
        self.assertIsNotNone(recover, "interrupted transaction recovery is missing")
        recover(self.transaction)

        self.assertEqual(self.unit_path.read_text(), old)
        self.assertTrue(self.system.enabled)
        self.assertTrue(self.system.active)
        self.assertEqual(self.system.mount.source, "10.10.10.55:/")
        self.assertFalse(self.transaction.exists())

    def test_transaction_persists_each_completed_phase_atomically(self) -> None:
        self.staged.write_text(self.unit("10.10.10.55:/"))

        self.manager.prepare(
            self.staged, self.transaction, "10.10.10.55:/", self.identity
        )
        self.assertEqual(
            json.loads((self.transaction / "state.json").read_text())["phase"],
            "prepared",
        )

        self.manager.quiesce(self.transaction)
        self.assertEqual(
            json.loads((self.transaction / "state.json").read_text())["phase"],
            "quiesced",
        )

        self.manager.reconcile(self.transaction)
        self.assertEqual(
            json.loads((self.transaction / "state.json").read_text())["phase"],
            "reconciled",
        )

        self.manager.rollback(self.transaction)
        self.assertEqual(
            json.loads((self.transaction / "state.json").read_text())["phase"],
            "rolled_back",
        )

    def test_prepare_phase_and_commit_fsync_files_and_parent_directories(self) -> None:
        self.staged.write_text(self.unit("10.10.10.55:/"))
        real_fsync = os.fsync
        with mock.patch.object(transition.os, "fsync", wraps=real_fsync) as fsync:
            self.manager.prepare(
                self.staged, self.transaction, "10.10.10.55:/", self.identity
            )
            prepare_calls = fsync.call_count
            self.assertGreaterEqual(prepare_calls, 3)

            self.manager.quiesce(self.transaction)
            self.assertGreater(fsync.call_count, prepare_calls)

            before_commit = fsync.call_count
            self.manager.commit(self.transaction)
            self.assertGreater(fsync.call_count, before_commit)

    def test_recover_fails_closed_on_unknown_transaction_phase(self) -> None:
        self.staged.write_text(self.unit("10.10.10.55:/"))
        self.manager.prepare(
            self.staged, self.transaction, "10.10.10.55:/", self.identity
        )
        state_path = self.transaction / "state.json"
        state = json.loads(state_path.read_text())
        state["phase"] = "alien"
        state_path.write_text(json.dumps(state))

        with self.assertRaisesRegex(RuntimeError, "invalid VM NFS transaction phase"):
            self.manager.recover(self.transaction)

        self.assertTrue(self.transaction.exists())

    def test_decided_commit_recovery_never_rolls_the_mount_back(self) -> None:
        desired = self.unit("10.10.10.55:/")
        self.staged.write_text(desired)
        self.manager.prepare(
            self.staged, self.transaction, "10.10.10.55:/", self.identity
        )
        self.manager.reconcile(self.transaction)
        self.manager.decide_commit(self.transaction)

        status = self.manager.status(self.transaction)
        self.assertEqual(status["phase"], "commit_decided")
        self.manager.recover(self.transaction)

        self.assertFalse(self.transaction.exists())
        self.assertEqual(self.unit_path.read_text(), desired)
        self.assertTrue(self.system.active)

    def test_transaction_persists_the_exact_deployment_identity(self) -> None:
        desired = self.unit("10.10.10.55:/")
        self.staged.write_text(desired)
        identity = transition.DeploymentIdentity(
            ctid=198,
            release="0123456789ab-cccccccccccccccc",
            source="10.10.10.55:/",
            pve_host="pve",
        )

        self.manager.prepare(
            self.staged,
            self.transaction,
            "10.10.10.55:/",
            deployment=identity,
        )

        self.assertEqual(
            self.manager.status(self.transaction)["deployment"],
            dataclasses.asdict(identity),
        )

    def test_source_validation_accepts_only_rfc1918_root_exports(self) -> None:
        self.assertEqual(
            transition._private_nfs_source("10.10.10.55:/"), "10.10.10.55:/"
        )
        for source in (
            "127.0.0.1:/",
            "169.254.1.1:/",
            "100.64.0.1:/",
            "203.0.113.1:/",
            "10.10.10.55:/other",
        ):
            with self.subTest(source=source), self.assertRaises(ValueError):
                transition._private_nfs_source(source)

    def test_prepare_rejects_a_staged_unit_for_another_source(self) -> None:
        self.staged.write_text(self.unit("10.10.10.44:/"))

        with self.assertRaisesRegex(RuntimeError, "expected NFS source"):
            self.manager.prepare(
                self.staged, self.transaction, "10.10.10.55:/", self.identity
            )

        self.assertFalse(self.transaction.exists())

    def test_prepare_rejects_a_live_mount_from_another_container(self) -> None:
        self.staged.write_text(self.unit("10.10.10.55:/"))
        self.system.active = True
        self.system.mount = self.system.record("10.10.10.44:/")

        with self.assertRaisesRegex(RuntimeError, "live VM NFS source"):
            self.manager.prepare(
                self.staged, self.transaction, "10.10.10.55:/", self.identity
            )

        self.assertTrue(self.system.active)
        self.assertEqual(self.system.mount.source, "10.10.10.44:/")
        self.assertFalse(self.transaction.exists())

    def test_recognized_bindfs_cutover_is_snapshotted_and_rollback_safe(self) -> None:
        legacy_unit = """[Unit]
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
"""
        legacy_artifact = self.unit_path.parent / "legacy-raw.mount"
        legacy_artifact.write_text("canonical legacy dependency\n")
        self.manager.legacy_artifacts = (legacy_artifact,)
        self.unit_path.write_text(legacy_unit)
        self.staged.write_text(self.unit("10.10.10.55:/"))
        self.system.enabled = True
        self.system.active = True
        self.system.mount = transition.MountRecord(
            source="/mnt/zerofs-files-raw",
            fstype="fuse.bindfs",
            options=("rw",),
        )
        raw_mountpoint = self.manager.forbidden_mounts[0]
        self.system.other_mounts[str(raw_mountpoint)] = self.system.record(
            "10.10.10.55:/"
        )
        legacy_service = self.manager.forbidden_units[0]
        self.system.loaded_units.add(legacy_service)

        self.manager.prepare(
            self.staged,
            self.transaction,
            "10.10.10.55:/",
            self.identity,
            allow_legacy_bindfs=True,
        )
        self.manager.quiesce(self.transaction)
        legacy_artifact.unlink()
        self.system.loaded_units.clear()
        self.system.other_mounts.clear()
        self.manager.rollback(self.transaction)

        self.assertEqual(legacy_artifact.read_text(), "canonical legacy dependency\n")
        self.assertEqual(self.unit_path.read_text(), legacy_unit)
        self.assertEqual(self.system.mount.source, "/mnt/zerofs-files-raw")
        self.assertEqual(self.system.mount.fstype, "fuse.bindfs")

    def test_legacy_bindfs_override_rejects_unknown_main_unit_bytes(self) -> None:
        self.unit_path.write_text(
            "[Mount]\nWhat=/mnt/zerofs-files-raw\nWhere=/tmp/escape\nType=fuse.bindfs\n"
        )
        self.staged.write_text(self.unit("10.10.10.55:/"))
        self.system.mount = transition.MountRecord(
            source="/mnt/zerofs-files-raw",
            fstype="fuse.bindfs",
            options=("rw",),
        )
        self.system.other_mounts[
            str(self.manager.forbidden_mounts[0])
        ] = self.system.record("10.10.10.55:/")

        with self.assertRaisesRegex(RuntimeError, "recognized legacy bindfs"):
            self.manager.prepare(
                self.staged,
                self.transaction,
                "10.10.10.55:/",
                self.identity,
                allow_legacy_bindfs=True,
            )


if __name__ == "__main__":
    unittest.main()
