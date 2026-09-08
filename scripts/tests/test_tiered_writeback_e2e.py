from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import tomllib
import unittest
import uuid
from dataclasses import replace
from pathlib import Path
from subprocess import CompletedProcess
from typing import Any, Mapping, Sequence
from unittest import mock

from scripts.tiered_writeback_e2e.config import (
    FILESYSTEM_ACK_MODES,
    OBJECT_ACK_MODES,
    PRODUCTION_MARKERS,
    AckModes,
    ConfigError,
    HarnessConfig,
    MissingAckModeError,
    UnsafeCleanupTarget,
    control_root_for,
    require_ack_modes,
    require_run_uuid,
    resource_root_for,
    validate_owned_path,
    validate_run_roots,
)
from scripts.tiered_writeback_e2e.integrity import (
    DurabilityFloor,
    IntegrityError,
    sha256_file,
    verify_copied_tree,
)
from scripts.tiered_writeback_e2e.resources import (
    LedgerIntegrityError,
    ResourceLedger,
    UnownedResourceError,
)
from scripts.tiered_writeback_e2e.observed_durability import ObservedSnapshot
from scripts.tiered_writeback_e2e import lifecycle as lifecycle_module
from scripts.tiered_writeback_e2e.lifecycle import (
    REQUIRED_RECEIPT_FIELDS,
    SCENARIO_NAMES,
    SCENARIOS,
    CleanupError,
    HarnessLifecycle,
    LifecycleError,
    PrimaryAndCleanupError,
    ResidualResourceError,
    SetupError,
    SourceBusyError,
    assert_source_idle,
)
from scripts.tiered_writeback_e2e import linux_suites, protocols
from scripts.tiered_writeback_e2e.protocols import (
    NBD_CLIENT,
    NBD_DEVICE,
    ScenarioContext,
)
from scripts.vm100_pilot.runner import Runner
from scripts.vm100_pilot.metrics import MetricsAuthorityIdentity, WritebackSnapshot

RUN_UUID = "1f4a3c60-8f6f-4c39-9f3e-2b8f6f2d9a01"

CONTRACT_SCENARIOS = frozenset(
    {
        "global-admission-nbd-nfs-ninep",
        "cross-adapter-pending-read-same-backing-inode",
        "nfs-commit-covers-prior-nbd",
        "ninep-fsync-covers-prior-nfs",
        "nbd-flush-covers-prior-ninep",
        "webui-rpc-production-path",
        "protocol-materialized-control",
        "protocol-durability-target-control",
        "xfstests-nfs-quick",
        "xfstests-ninep-quick-and-strict",
        "pjdfstest-nfs",
        "pjdfstest-ninep",
        "stress-ng-nfs-ninep",
        "kernel-compile-nfs",
        "kernel-compile-ninep",
        "xfs-over-nbd-restart",
        "zfs-over-nbd-restart",
        "crash-boundary-matrix",
        "local-receipt-restart",
        "remote-receipt-clean-cache-restart",
        "terminal-fanout-and-shutdown-timeout",
        "benchmark-ram-ack",
        "benchmark-local-ssd",
        "benchmark-paced-remote",
    }
)


class FakeRunner(Runner):
    """Records every command and answers a small set of read-only queries."""

    def __init__(self) -> None:
        super().__init__(base_env={})
        self.calls: list[tuple[tuple[str, ...], bool]] = []
        self.failures: dict[tuple[str, ...], BaseException] = {}
        self.next_pid = 4242

    def run(
        self,
        argv: Sequence[str | Path],
        *,
        sudo: bool = False,
        timeout: float | None = None,
        capture: bool = True,
        check: bool = True,
        cwd: Path | None = None,
        env: Mapping[str, str] | None = None,
        input_text: str | None = None,
    ) -> CompletedProcess[str]:
        args = tuple(str(value) for value in argv)
        self.calls.append((args, sudo))
        for prefix, error in self.failures.items():
            if args[: len(prefix)] == prefix:
                raise error
        if args[0] == "git" and "rev-parse" in args:
            return CompletedProcess(args, 0, "a" * 40 + "\n", "")
        if args[:3] == ("systemctl", "show", "--property=MainPID"):
            pid = self.next_pid
            self.next_pid += 1
            return CompletedProcess(args, 0, f"{pid}\n", "")
        if args[:2] == ("blockdev", "--getsize64"):
            return CompletedProcess(args, 0, f"{4 * 1024 * 1024 * 1024}\n", "")
        if args and args[0] == "sha256sum":
            return CompletedProcess(args, 0, f"{'a' * 64}  {args[-1]}\n", "")
        return CompletedProcess(args, 0, "", "")


def observed_snapshot(
    instance: str,
    *,
    accepted: int,
    local: int,
    remote: int,
) -> ObservedSnapshot:
    return ObservedSnapshot(
        MetricsAuthorityIdentity(instance, "filesystem-a", f"zerofs-tiered-{RUN_UUID}"),
        WritebackSnapshot(
            accepted=accepted,
            local=local,
            remote=remote,
            dirty_ram=0,
            dirty_ssd_reserved=0,
            local_bytes=1,
            remote_bytes=1,
            terminal=False,
        ),
    )


class FakeObservedCollector:
    def __init__(self, initial: ObservedSnapshot, final: ObservedSnapshot) -> None:
        self.initial = initial
        self.final = final
        self.calls: list[tuple[str, object]] = []

    def snapshot(self) -> ObservedSnapshot:
        self.calls.append(("snapshot", None))
        return self.initial

    def wait_for_initial_snapshot(
        self, *, timeout: float, interval: float = 0.05
    ) -> ObservedSnapshot:
        self.calls.append(("wait-initial", timeout))
        return self.initial

    def wait_for_accepted_after(
        self, previous: int, *, timeout: float, interval: float = 0.05
    ) -> ObservedSnapshot:
        self.calls.append(("wait-accepted", previous))
        return self.final

    def wait_for_local_frontier(
        self, target: int, *, timeout: float, interval: float = 0.05
    ) -> ObservedSnapshot:
        self.calls.append(("wait-local", target))
        return self.final

    def require_restarted(self, before: ObservedSnapshot) -> ObservedSnapshot:
        self.calls.append(("require-restarted", before))
        return self.final

    def wait_for_restarted(
        self,
        before: ObservedSnapshot,
        *,
        timeout: float,
        interval: float = 0.05,
    ) -> ObservedSnapshot:
        self.calls.append(("wait-restarted", before))
        return self.final


class FakeProbes:
    """Live-state probes backed by plain sets instead of the running system."""

    def __init__(self) -> None:
        self.active_units: set[str] = set()
        self.unit_pids: dict[str, int] = {}
        self.alive_pids: set[int] = set()
        self.listening_ports: set[int] = set()
        self.active_mounts: set[str] = set()
        self.attached_devices: set[str] = set()
        self.pools: set[str] = set()
        self.socket_probe_results: list[bool] = []

    def process_alive(self, pid: int) -> bool:
        return pid in self.alive_pids

    def unit_active(self, unit: str) -> bool:
        return unit in self.active_units

    def process_belongs_to_unit(self, pid: int, unit: str) -> bool:
        return self.unit_pids.get(unit) == pid

    def port_listening(self, port: int) -> bool:
        return port in self.listening_ports

    def mount_active(self, mountpoint: str) -> bool:
        return mountpoint in self.active_mounts

    def device_attached(self, device: str) -> bool:
        return device in self.attached_devices

    def pool_exists(self, name: str) -> bool:
        return name in self.pools

    def unix_socket_ready(self, path: str) -> bool:
        _ = path
        if self.socket_probe_results:
            return self.socket_probe_results.pop(0)
        return True


class HarnessCase(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.base = Path(self.temp.name)
        self.root_parent = self.base / "roots"
        self.root_parent.mkdir()
        self.workspace = self.base / "workspace"
        self.workspace.mkdir()
        self.binary = self.base / "zerofs"
        self.binary.write_bytes(b"binary-payload")
        self.zerofs_config = self.base / "zerofs.toml"
        self.zerofs_config.write_text("[storage]\n", encoding="utf-8")

    def make_config(
        self,
        *,
        filesystem: str = "materialized",
        object_: str = "remote",
        run_uuid: str = RUN_UUID,
    ) -> HarnessConfig:
        return HarnessConfig.create(
            filesystem_ack_mode=filesystem,
            object_ack_mode=object_,
            run_uuid=run_uuid,
            environ={"ZEROFS_TIERED_ROOT_PARENT": str(self.root_parent)},
            workspace_root=self.workspace,
            source_root=self.workspace,
        )

    def make_lifecycle(
        self, config: HarnessConfig
    ) -> tuple[HarnessLifecycle, FakeRunner, FakeProbes]:
        runner = FakeRunner()
        probes = FakeProbes()
        lifecycle = HarnessLifecycle(config, runner, probes=probes, platform="linux")
        return lifecycle, runner, probes

    def setup_run(
        self, config: HarnessConfig | None = None
    ) -> tuple[HarnessLifecycle, FakeRunner, FakeProbes, ResourceLedger]:
        config = config or self.make_config()
        lifecycle, runner, probes = self.make_lifecycle(config)
        lifecycle.setup(zerofs_binary=self.binary, zerofs_config=self.zerofs_config)
        ledger = ResourceLedger.load(config.ledger_path)
        return lifecycle, runner, probes, ledger


class AckModeTests(unittest.TestCase):
    def test_both_dual_ack_flags_are_required(self) -> None:
        for filesystem, object_ in ((None, "remote"), ("materialized", None)):
            with self.subTest(filesystem=filesystem, object=object_):
                with self.assertRaises(MissingAckModeError):
                    require_ack_modes(filesystem, object_)

    def test_ack_modes_outside_the_contract_are_rejected(self) -> None:
        with self.assertRaises(ConfigError):
            require_ack_modes("eventually", "remote")
        with self.assertRaises(ConfigError):
            require_ack_modes("materialized", "tape")

    def test_valid_ack_modes_produce_a_typed_label(self) -> None:
        ack = require_ack_modes("volatile_memory", "ssd")
        self.assertEqual(ack, AckModes("volatile_memory", "ssd"))
        self.assertEqual(ack.label, "volatile_memory+ssd")
        self.assertIn(ack.filesystem, FILESYSTEM_ACK_MODES)
        self.assertIn(ack.object, OBJECT_ACK_MODES)


class RootValidationTests(unittest.TestCase):
    def _validate(
        self,
        control: str,
        resource: str,
        *,
        run_uuid: str = RUN_UUID,
        workspace: str | None = None,
    ) -> None:
        validate_run_roots(
            run_uuid,
            Path(control),
            Path(resource),
            root_parent=Path("/var/tmp"),
            workspace_root=Path(workspace) if workspace else None,
        )

    def test_cleanup_rejects_non_uuid_root(self) -> None:
        ledger = ResourceLedger(
            run_id="not-a-uuid",
            control_root="/var/tmp/control",
            resource_root="/var/tmp/resources",
        )
        with self.assertRaises(UnsafeCleanupTarget):
            ledger.validate_cleanup_scope()

    def test_root_filesystem_is_rejected(self) -> None:
        with self.assertRaises(UnsafeCleanupTarget):
            self._validate("/", f"/var/tmp/zerofs-tiered-resources-{RUN_UUID}")

    def test_mnt_is_rejected(self) -> None:
        with self.assertRaises(UnsafeCleanupTarget):
            self._validate(f"/var/tmp/zerofs-tiered-control-{RUN_UUID}", "/mnt")

    def test_var_tmp_itself_is_rejected(self) -> None:
        with self.assertRaises(UnsafeCleanupTarget):
            self._validate("/var/tmp", f"/var/tmp/zerofs-tiered-resources-{RUN_UUID}")

    def test_equal_control_and_resource_roots_are_rejected(self) -> None:
        shared = f"/var/tmp/zerofs-tiered-control-{RUN_UUID}"
        with self.assertRaises(UnsafeCleanupTarget):
            self._validate(shared, shared)

    def test_nested_control_and_resource_roots_are_rejected(self) -> None:
        control = f"/var/tmp/zerofs-tiered-control-{RUN_UUID}"
        with self.assertRaises(UnsafeCleanupTarget):
            self._validate(control, f"{control}/zerofs-tiered-resources-{RUN_UUID}")

    def test_workspace_roots_are_rejected(self) -> None:
        control = f"/var/tmp/zerofs-tiered-control-{RUN_UUID}"
        resource = f"/var/tmp/zerofs-tiered-resources-{RUN_UUID}"
        with self.assertRaises(UnsafeCleanupTarget):
            self._validate(control, resource, workspace="/var/tmp")
        with self.assertRaises(UnsafeCleanupTarget):
            self._validate(control, resource, workspace=control)

    def test_ct198_and_production_strings_are_rejected(self) -> None:
        for marker in ("CT198", "production"):
            run_uuid = RUN_UUID
            control = f"/var/tmp/zerofs-tiered-control-{run_uuid}"
            resource = f"/var/tmp/zerofs-tiered-resources-{run_uuid}"
            with self.subTest(marker=marker):
                with self.assertRaises(UnsafeCleanupTarget):
                    validate_run_roots(
                        run_uuid,
                        Path(control),
                        Path(resource),
                        root_parent=Path(f"/var/tmp/{marker}").parent,
                        workspace_root=None,
                        extra_strings=(f"sftp://user@{marker}.example/prefix",),
                    )

    def test_roots_must_be_uuid_named_children_of_the_root_parent(self) -> None:
        resource = f"/var/tmp/zerofs-tiered-resources-{RUN_UUID}"
        for control in (
            f"/var/tmp/zerofs-control-{RUN_UUID}",
            f"/var/tmp/nested/zerofs-tiered-control-{RUN_UUID}",
            "/var/tmp/zerofs-tiered-control-99999999-8f6f-4c39-9f3e-2b8f6f2d9a01",
        ):
            with self.subTest(control=control):
                with self.assertRaises(UnsafeCleanupTarget):
                    self._validate(control, resource)

    def test_run_uuid_must_be_canonical(self) -> None:
        for value in ("not-a-uuid", "", RUN_UUID.upper(), RUN_UUID.replace("-", "")):
            with self.subTest(value=value):
                with self.assertRaises(UnsafeCleanupTarget):
                    require_run_uuid(value)
        self.assertEqual(require_run_uuid(RUN_UUID), RUN_UUID)

    def test_owned_path_validation_rejects_paths_outside_resource_root(self) -> None:
        resource = Path(f"/var/tmp/zerofs-tiered-resources-{RUN_UUID}")
        validate_owned_path(resource / "mnt" / "nfs", resource)
        validate_owned_path(resource, resource)
        for outsider in ("/var/tmp", "/mnt/zerofs", str(resource) + "-evil"):
            with self.subTest(path=outsider):
                with self.assertRaises(UnsafeCleanupTarget):
                    validate_owned_path(Path(outsider), resource)

    def test_derived_roots_are_disjoint_siblings(self) -> None:
        parent = Path("/var/tmp")
        control = control_root_for(parent, RUN_UUID)
        resource = resource_root_for(parent, RUN_UUID)
        self.assertEqual(control.parent, resource.parent)
        self.assertNotEqual(control, resource)
        validate_run_roots(
            RUN_UUID, control, resource, root_parent=parent, workspace_root=None
        )


class ResourceLedgerTests(HarnessCase):
    def make_ledger(self) -> tuple[HarnessConfig, ResourceLedger]:
        config = self.make_config()
        config.control_root.mkdir(parents=True)
        ledger = ResourceLedger.create(
            config,
            source_sha="a" * 40,
            binary_sha256=sha256_file(self.binary),
            config_sha256=sha256_file(self.zerofs_config),
        )
        ledger.record_resource("path", str(config.resource_root))
        ledger.record_resource("prefix", config.backend_prefix)
        return config, ledger

    def test_identity_is_immutable_once_written(self) -> None:
        config, _ = self.make_ledger()
        document = json.loads(config.ledger_path.read_text())
        document["identity"]["object_ack_mode"] = "memory"
        config.ledger_path.write_text(json.dumps(document), encoding="utf-8")
        with self.assertRaises(LedgerIntegrityError):
            ResourceLedger.load(config.ledger_path)

    def test_events_are_hash_chained_and_tamper_evident(self) -> None:
        config, ledger = self.make_ledger()
        ledger.record_resource("unit", config.unit_name)
        ledger.record_resource("process", 4321, unit=config.unit_name)
        ledger.record_release("process", 4321)
        document = json.loads(config.ledger_path.read_text())
        document["events"][0]["payload"]["value"] = 1
        config.ledger_path.write_text(json.dumps(document), encoding="utf-8")
        with self.assertRaises(LedgerIntegrityError):
            ResourceLedger.load(config.ledger_path)

    def test_round_trip_preserves_the_chain(self) -> None:
        config, ledger = self.make_ledger()
        ledger.record_resource("listener", 12049)
        ledger.record_resource("mount", str(config.resource_root / "mnt" / "nfs"))
        loaded = ResourceLedger.load(config.ledger_path)
        self.assertEqual(len(loaded.events), len(ledger.events))
        self.assertEqual(
            {(kind, value) for kind, value, _ in loaded.outstanding()},
            {
                ("path", str(config.resource_root)),
                ("prefix", config.backend_prefix),
                ("listener", 12049),
                ("mount", str(config.resource_root / "mnt" / "nfs")),
            },
        )

    def test_unowned_resources_are_rejected_at_recording_time(self) -> None:
        config, ledger = self.make_ledger()
        with self.assertRaises(UnownedResourceError):
            ledger.record_resource("device", "/dev/sda1")
        with self.assertRaises(UnownedResourceError):
            ledger.record_resource("mount", "/mnt/zerofs-production")
        with self.assertRaises(UnownedResourceError):
            ledger.record_resource("listener", 22)
        with self.assertRaises(UnownedResourceError):
            ledger.record_resource("pool", "tank")
        with self.assertRaises(UnownedResourceError):
            ledger.record_resource("prefix", "someone-elses/prefix")
        with self.assertRaises(UnownedResourceError):
            ledger.record_resource("path", "/var/tmp/other")
        ledger.record_resource("pool", f"zerofs-tiered-{RUN_UUID}")
        with self.assertRaises(UnownedResourceError):
            ledger.record_resource("process", 4321)
        _ = config

    def test_acting_on_an_unrecorded_resource_is_rejected(self) -> None:
        config, ledger = self.make_ledger()
        with self.assertRaises(UnownedResourceError):
            ledger.require_owned("process", 999)
        ledger.record_resource("unit", config.unit_name)
        ledger.record_resource("process", 999, unit=config.unit_name)
        ledger.require_owned("process", 999)

    def test_released_resource_is_not_active(self) -> None:
        _, ledger = self.make_ledger()
        ledger.record_resource("device", "/dev/nbd7")
        ledger.require_active("device", "/dev/nbd7")
        ledger.record_release("device", "/dev/nbd7")
        with self.assertRaises(UnownedResourceError):
            ledger.require_active("device", "/dev/nbd7")

    def test_releases_reduce_the_outstanding_set(self) -> None:
        _, ledger = self.make_ledger()
        ledger.record_resource("listener", 12049)
        ledger.record_release("listener", 12049)
        self.assertNotIn(
            ("listener", 12049),
            {(kind, value) for kind, value, _ in ledger.outstanding()},
        )

    def test_release_then_reacquire_restores_cleanup_ownership(self) -> None:
        _, ledger = self.make_ledger()
        ledger.record_resource("listener", 12049, generation=1)
        ledger.record_release("listener", 12049)
        ledger.record_resource("listener", 12049, generation=2)
        self.assertEqual(
            [entry for entry in ledger.outstanding() if entry[0] == "listener"],
            [("listener", 12049, {"generation": 2})],
        )

    def test_duplicate_active_acquire_and_release_are_rejected(self) -> None:
        _, ledger = self.make_ledger()
        ledger.record_resource("listener", 12049)
        with self.assertRaises(UnownedResourceError):
            ledger.record_resource("listener", 12049)
        ledger.record_release("listener", 12049)
        with self.assertRaises(UnownedResourceError):
            ledger.record_release("listener", 12049)

    def test_ledger_value_reads_dotted_keys(self) -> None:
        config, ledger = self.make_ledger()
        self.assertEqual(ledger.value("identity.run_uuid"), RUN_UUID)
        self.assertEqual(
            ledger.value("identity.control_root"), str(config.control_root)
        )
        self.assertEqual(ledger.value("events.0.kind"), "acquire")
        with self.assertRaises(KeyError):
            ledger.value("identity.missing")


class SetupTests(HarnessCase):
    def test_setup_creates_roots_ledger_and_a_complete_receipt(self) -> None:
        config = self.make_config()
        lifecycle, runner, _ = self.make_lifecycle(config)
        summary = lifecycle.setup(
            zerofs_binary=self.binary, zerofs_config=self.zerofs_config
        )
        self.assertTrue(config.control_root.is_dir())
        self.assertTrue(config.resource_root.is_dir())
        self.assertTrue(config.ledger_path.is_file())
        receipt = json.loads(Path(summary["receipt"]).read_text())
        for field in REQUIRED_RECEIPT_FIELDS:
            self.assertIn(field, receipt, field)
        self.assertEqual(receipt["filesystem_ack_mode"], "materialized")
        self.assertEqual(receipt["object_ack_mode"], "remote")
        self.assertEqual(receipt["source_sha"], "a" * 40)
        self.assertEqual(receipt["binary_sha256"], sha256_file(self.binary))
        self.assertEqual(receipt["config_sha256"], sha256_file(self.zerofs_config))
        self.assertEqual(receipt["control_root"], str(config.control_root))
        self.assertEqual(receipt["resource_root"], str(config.resource_root))
        self.assertEqual(receipt["backend_prefix"], config.backend_prefix)
        self.assertEqual(receipt["tool_revisions"], dict(linux_suites.PINNED_REVISIONS))
        self.assertEqual(receipt["status"], "ok")
        self.assertIn(
            (("git", "-C", str(config.source_root), "rev-parse", "HEAD"), False),
            runner.calls,
        )

    def test_setup_fails_when_the_receipt_cannot_be_written(self) -> None:
        config = self.make_config()
        lifecycle, _, _ = self.make_lifecycle(config)
        config.control_root.mkdir(parents=True)
        config.receipt_root.write_text("blocker", encoding="utf-8")
        with self.assertRaises(SetupError):
            lifecycle.setup(zerofs_binary=self.binary, zerofs_config=self.zerofs_config)
        ledger = ResourceLedger.load(config.ledger_path)
        self.assertIn("setup-failed", [event["kind"] for event in ledger.events])

    def test_partial_setup_is_recorded_and_recoverable_by_cleanup(self) -> None:
        config = self.make_config()
        lifecycle, _, probes = self.make_lifecycle(config)
        config.resource_root.write_text("blocker", encoding="utf-8")
        with self.assertRaises(SetupError):
            lifecycle.setup(zerofs_binary=self.binary, zerofs_config=self.zerofs_config)
        ledger = ResourceLedger.load(config.ledger_path)
        self.assertIn("setup-failed", [event["kind"] for event in ledger.events])
        lifecycle.cleanup(ledger)
        self.assertFalse(config.resource_root.exists())
        lifecycle.assert_clean(ResourceLedger.load(config.ledger_path))
        _ = probes

    def test_setup_refuses_to_reuse_an_existing_control_root(self) -> None:
        config = self.make_config()
        lifecycle, _, _ = self.make_lifecycle(config)
        lifecycle.setup(zerofs_binary=self.binary, zerofs_config=self.zerofs_config)
        with self.assertRaises(SetupError):
            lifecycle.setup(zerofs_binary=self.binary, zerofs_config=self.zerofs_config)

    def test_setup_materializes_and_hashes_the_exact_xfs_runtime_config(self) -> None:
        config = self.make_config(filesystem="volatile_memory", object_="ssd")
        lifecycle, _, _ = self.make_lifecycle(config)
        template = (
            Path(__file__).resolve().parents[1]
            / "tiered_writeback_e2e"
            / "xfs_nbd_tiered.toml.template"
        )

        with mock.patch.dict(os.environ, {}, clear=True):
            summary = lifecycle.setup(
                zerofs_binary=self.binary,
                zerofs_config=template,
            )

        runtime_config = config.control_root / "run" / "xfs-nbd-tiered.toml"
        rendered = runtime_config.read_text(encoding="utf-8")
        self.assertNotIn("${", rendered)
        document = tomllib.loads(rendered)
        self.assertEqual(
            document["prometheus"]["benchmark_authority"]["export_id"],
            config.unit_name,
        )
        self.assertEqual(
            document["storage"]["url"],
            f"s3://zerofs-xfs-{config.run_uuid}/zerofs-tiered/{config.run_uuid}",
        )
        ledger = ResourceLedger.load(config.ledger_path)
        self.assertEqual(ledger.identity["config_sha256"], sha256_file(runtime_config))
        self.assertNotEqual(ledger.identity["config_sha256"], sha256_file(template))
        self.assertEqual(summary["zerofs_config"], str(runtime_config))


class CleanupTests(HarnessCase):
    def test_cleanup_does_not_stop_a_unit_that_was_never_created(self) -> None:
        config = self.make_config()
        lifecycle, runner, _, ledger = self.setup_run(config)
        ledger.record_resource("unit", config.unit_name)
        lifecycle.cleanup(ledger)
        self.assertNotIn(
            (("systemctl", "stop", config.unit_name), True), runner.calls
        )

    def test_cleanup_removes_only_resource_root_entries(self) -> None:
        config = self.make_config()
        lifecycle, _, _, ledger = self.setup_run(config)
        (config.resource_root / "scratch").mkdir()
        (config.resource_root / "scratch" / "data.bin").write_bytes(b"x" * 64)
        decoy = self.root_parent / "unrelated-sibling"
        decoy.mkdir()
        lifecycle.cleanup(ledger)
        self.assertFalse(config.resource_root.exists())
        self.assertTrue(decoy.is_dir())
        self.assertTrue(config.control_root.is_dir())
        self.assertTrue(config.ledger_path.is_file())

    def test_cleanup_preserves_ledger_authority(self) -> None:
        config = self.make_config()
        lifecycle, _, _, ledger = self.setup_run(config)
        lifecycle.cleanup(ledger)
        reloaded = ResourceLedger.load(config.ledger_path)
        kinds = [event["kind"] for event in reloaded.events]
        self.assertIn("cleanup-started", kinds)
        self.assertIn("cleanup-finished", kinds)
        self.assertEqual(reloaded.outstanding(), [])

    def test_repeated_cleanup_succeeds_after_resource_root_deletion(self) -> None:
        config = self.make_config()
        lifecycle, _, _, ledger = self.setup_run(config)
        lifecycle.cleanup(ledger)
        self.assertFalse(config.resource_root.exists())
        lifecycle.cleanup(ResourceLedger.load(config.ledger_path))

    def test_cleanup_tears_down_recorded_processes_mounts_devices_pools(self) -> None:
        config = self.make_config()
        lifecycle, runner, probes, ledger = self.setup_run(config)
        mountpoint = config.resource_root / "mnt" / "nfs"
        pool = f"zerofs-tiered-{RUN_UUID}"
        ledger.record_resource("unit", config.unit_name)
        ledger.record_resource("process", 4242, unit=config.unit_name)
        ledger.record_resource("listener", 12049)
        ledger.record_resource("mount", str(mountpoint))
        ledger.record_resource("device", "/dev/nbd7")
        ledger.record_resource("pool", pool)
        probes.alive_pids.add(4242)  # PID was reused by an unrelated process.
        probes.active_units.add(config.unit_name)
        probes.unit_pids[config.unit_name] = 99999
        probes.active_mounts.add(str(mountpoint))
        probes.attached_devices.add("/dev/nbd7")
        probes.pools.add(pool)
        lifecycle.cleanup(ledger)
        commands = [args for args, _ in runner.calls]
        self.assertIn(("zpool", "destroy", pool), commands)
        self.assertIn((NBD_CLIENT, "-d", "/dev/nbd7"), commands)
        self.assertIn(("umount", str(mountpoint)), commands)
        self.assertNotIn(("kill", "-9", "4242"), commands)
        self.assertIn(("systemctl", "stop", config.unit_name), commands)
        reloaded = ResourceLedger.load(config.ledger_path)
        self.assertEqual(reloaded.outstanding(), [])

    def test_cleanup_refuses_an_unsafe_ledger_scope(self) -> None:
        config = self.make_config()
        lifecycle, runner, _ = self.make_lifecycle(config)
        ledger = ResourceLedger(
            run_id=RUN_UUID,
            control_root=str(config.control_root),
            resource_root="/mnt",
        )
        with self.assertRaises(UnsafeCleanupTarget):
            lifecycle.cleanup(ledger)
        self.assertEqual(runner.calls, [])


class AssertCleanTests(HarnessCase):
    def test_assert_clean_passes_after_cleanup_without_control_root_removal(
        self,
    ) -> None:
        config = self.make_config()
        lifecycle, _, _, ledger = self.setup_run(config)
        lifecycle.cleanup(ledger)
        result = lifecycle.assert_clean(ResourceLedger.load(config.ledger_path))
        self.assertTrue(result["clean"])
        self.assertTrue(config.control_root.is_dir())

    def test_assert_clean_fails_when_a_recorded_resource_remains(self) -> None:
        config = self.make_config()
        lifecycle, _, probes, ledger = self.setup_run(config)
        ledger.record_resource("unit", config.unit_name)
        ledger.record_resource("process", 4242, unit=config.unit_name)
        lifecycle.cleanup(ledger)
        probes.active_units.add(config.unit_name)
        probes.unit_pids[config.unit_name] = 4242
        with self.assertRaisesRegex(ResidualResourceError, "4242"):
            lifecycle.assert_clean(ResourceLedger.load(config.ledger_path))

    def test_assert_clean_fails_for_outstanding_never_released_resources(self) -> None:
        config = self.make_config()
        lifecycle, _, _, ledger = self.setup_run(config)
        ledger.record_resource("listener", 12049)
        with self.assertRaisesRegex(ResidualResourceError, "12049"):
            lifecycle.assert_clean(ResourceLedger.load(config.ledger_path))


class ArchiveControlTests(HarnessCase):
    def test_archive_copies_verifies_then_removes_the_control_root(self) -> None:
        config = self.make_config()
        lifecycle, _, _, ledger = self.setup_run(config)
        lifecycle.cleanup(ledger)
        archive_root = self.base / "archive"
        result = lifecycle.archive_control(
            ResourceLedger.load(config.ledger_path), archive_root
        )
        destination = archive_root / RUN_UUID
        self.assertTrue((destination / "ledger.json").is_file())
        receipts = list((destination / "receipts").rglob("manifest.json"))
        self.assertTrue(receipts)
        self.assertFalse(config.control_root.exists())
        self.assertGreaterEqual(result["files"], 2)

    def test_archive_aborts_and_preserves_control_root_on_hash_mismatch(self) -> None:
        config = self.make_config()
        lifecycle, _, _, ledger = self.setup_run(config)
        lifecycle.cleanup(ledger)
        with mock.patch.object(
            lifecycle_module,
            "verify_copied_tree",
            side_effect=IntegrityError("injected hash mismatch"),
        ):
            with self.assertRaisesRegex(IntegrityError, "injected hash mismatch"):
                lifecycle.archive_control(
                    ResourceLedger.load(config.ledger_path), self.base / "archive"
                )
        self.assertTrue(config.control_root.is_dir())
        self.assertTrue(config.ledger_path.is_file())

    def test_archive_rejects_destinations_inside_the_control_root(self) -> None:
        config = self.make_config()
        lifecycle, _, _, ledger = self.setup_run(config)
        lifecycle.cleanup(ledger)
        with self.assertRaises(UnsafeCleanupTarget):
            lifecycle.archive_control(
                ResourceLedger.load(config.ledger_path),
                config.control_root / "archive",
            )


class SourceIdleTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.base = Path(self.temp.name)
        self.source = self.base / "source"
        (self.source / "zerofs").mkdir(parents=True)
        self.proc = self.base / "proc"
        self.proc.mkdir()

    def add_process(self, pid: int, comm: str, cwd: Path) -> None:
        entry = self.proc / str(pid)
        entry.mkdir()
        (entry / "comm").write_text(comm + "\n", encoding="utf-8")
        (entry / "cmdline").write_bytes(comm.encode() + b"\x00")
        os.symlink(cwd, entry / "cwd")

    def test_active_cargo_jobs_rooted_at_the_source_fail_the_check(self) -> None:
        self.add_process(101, "cargo", self.source / "zerofs")
        with self.assertRaisesRegex(SourceBusyError, "101"):
            assert_source_idle(self.source, proc_root=self.proc)

    def test_unrelated_processes_do_not_fail_the_check(self) -> None:
        elsewhere = self.base / "elsewhere"
        elsewhere.mkdir()
        self.add_process(102, "cargo", elsewhere)
        self.add_process(103, "zsh", self.source)
        result = assert_source_idle(self.source, proc_root=self.proc)
        self.assertEqual(result["busy"], [])

    def test_rustc_and_harness_jobs_are_detected(self) -> None:
        self.add_process(104, "rustc", self.source)
        self.add_process(105, "tiered-writeback-e2e.py", self.source)
        with self.assertRaises(SourceBusyError):
            assert_source_idle(self.source, proc_root=self.proc)


class ScenarioPlanTests(HarnessCase):
    def context(self, **kwargs: Any) -> ScenarioContext:
        return ScenarioContext(
            config=self.make_config(**kwargs),
            zerofs_binary=self.binary,
            zerofs_config=self.zerofs_config,
        )

    def steps_for(self, name: str, **kwargs: Any) -> list[tuple[str, ...]]:
        plan = SCENARIOS[name](self.context(**kwargs))
        return [step.argv for step in plan.steps]

    def test_every_contract_scenario_is_registered(self) -> None:
        self.assertEqual(set(SCENARIO_NAMES), CONTRACT_SCENARIOS)
        self.assertEqual(set(SCENARIOS), CONTRACT_SCENARIOS)

    def test_every_scenario_builds_a_typed_plan(self) -> None:
        for name in sorted(CONTRACT_SCENARIOS):
            with self.subTest(scenario=name):
                object_ = {
                    "benchmark-ram-ack": "memory",
                    "benchmark-local-ssd": "ssd",
                }.get(name, "remote")
                plan = SCENARIOS[name](self.context(object_=object_))
                self.assertEqual(plan.name, name)
                self.assertTrue(plan.steps)
                self.assertTrue(plan.durability_floors)
                self.assertTrue(plan.requires_observed_durability)
                for floor in plan.durability_floors:
                    self.assertIsInstance(floor, DurabilityFloor)

    def test_server_launch_uses_the_run_subcommand(self) -> None:
        steps = self.steps_for("nfs-commit-covers-prior-nbd")
        launch = next(argv for argv in steps if argv[0] == "systemd-run")
        binary_index = launch.index(str(self.binary))
        self.assertEqual(
            launch[binary_index : binary_index + 3],
            (str(self.binary), "run", "--config"),
        )

    def test_nfs_leg_uses_a_hard_v3_mount_with_a_real_commit(self) -> None:
        config = self.make_config()
        steps = self.steps_for("nfs-commit-covers-prior-nbd")
        mount = next(argv for argv in steps if argv[0] == "mount.nfs")
        options = mount[mount.index("-o") + 1]
        self.assertIn("hard", options)
        self.assertIn("vers=3", options)
        self.assertTrue(mount[2].startswith(str(config.resource_root)))
        commit = next(
            argv for argv in steps if argv[0] == "dd" and "conv=fsync" in argv
        )
        self.assertTrue(any(str(config.resource_root) in part for part in commit))

    def test_ninep_leg_mounts_v9fs_and_issues_tfsync(self) -> None:
        steps = self.steps_for("ninep-fsync-covers-prior-nfs")
        mount = next(argv for argv in steps if argv[:3] == ("mount", "-t", "9p"))
        options = mount[mount.index("-o") + 1]
        self.assertIn("trans=tcp", options)
        self.assertIn("version=9p2000.L", options)
        self.assertTrue(any(argv[0] == "dd" and "conv=fsync" in argv for argv in steps))

    def test_nbd_leg_connects_flushes_and_disconnects(self) -> None:
        steps = self.steps_for("nbd-flush-covers-prior-ninep")
        self.assertTrue(any(argv[0] == NBD_CLIENT for argv in steps))
        self.assertTrue(any(argv[:2] == ("blockdev", "--flushbufs") for argv in steps))
        self.assertIn((NBD_CLIENT, "-d", "/dev/nbd7"), steps)

    def test_webui_rpc_path_mutates_rpc_and_moves_bytes_over_websocket(self) -> None:
        steps = self.steps_for("webui-rpc-production-path")
        rpc_methods = {argv[-1] for argv in steps if argv[0] == "grpcurl"}
        self.assertIn("zerofs.admin.AdminService/CreateDirectory", rpc_methods)
        self.assertIn("zerofs.admin.AdminService/Flush", rpc_methods)
        self.assertIn("zerofs.admin.AdminService/RemoveDirectory", rpc_methods)
        self.assertFalse(any(argv[-1] == "list" for argv in steps))
        uploads = [argv for argv in steps if len(argv) > 1 and argv[1] == "upload"]
        downloads = [argv for argv in steps if len(argv) > 1 and argv[1] == "download"]
        self.assertEqual(len(uploads), 1)
        self.assertEqual(len(downloads), 1)
        self.assertIn("/ws/9p", uploads[0][2])
        self.assertIn("/ws/9p", downloads[0][2])
        self.assertTrue(any(argv[0] == "cmp" for argv in steps))
        self.assertFalse(any(argv[0] == "websocat" for argv in steps))

    def test_benchmark_scenarios_require_a_matching_object_ack_mode(self) -> None:
        for name, required in (
            ("benchmark-ram-ack", "memory"),
            ("benchmark-local-ssd", "ssd"),
            ("benchmark-paced-remote", "remote"),
        ):
            with self.subTest(scenario=name):
                plan = SCENARIOS[name](self.context(object_=required))
                self.assertTrue(plan.steps)
                wrong = "remote" if required != "remote" else "memory"
                with self.assertRaises(ConfigError):
                    SCENARIOS[name](self.context(object_=wrong))

    def test_materialized_control_requires_the_materialized_ack_mode(self) -> None:
        SCENARIOS["protocol-materialized-control"](self.context())
        with self.assertRaises(ConfigError):
            SCENARIOS["protocol-materialized-control"](
                self.context(filesystem="volatile_memory")
            )

    def test_crash_scenarios_target_only_run_scoped_units(self) -> None:
        for name in (
            "crash-boundary-matrix",
            "xfs-over-nbd-restart",
            "local-receipt-restart",
        ):
            with self.subTest(scenario=name):
                steps = self.steps_for(name)
                kills = [
                    argv
                    for argv in steps
                    if argv[:2] == ("systemctl", "kill")
                    or argv[:2] == ("systemctl", "stop")
                ]
                self.assertTrue(kills)
                for argv in kills:
                    self.assertTrue(
                        any(RUN_UUID in part for part in argv),
                        f"{name}: {argv} does not target the run-scoped unit",
                    )

    def test_every_systemd_launch_captures_a_real_pid_and_owns_its_unit(self) -> None:
        config = self.make_config()
        for name in sorted(CONTRACT_SCENARIOS):
            with self.subTest(scenario=name):
                object_ = {
                    "benchmark-ram-ack": "memory",
                    "benchmark-local-ssd": "ssd",
                }.get(name, "remote")
                plan = SCENARIOS[name](self.context(object_=object_))
                for step in plan.steps:
                    if step.argv[0] != "systemd-run":
                        continue
                    unit = next(
                        value.removeprefix("--unit=")
                        for value in step.argv
                        if value.startswith("--unit=")
                    )
                    self.assertEqual(step.capture_main_pid_unit, unit)
                    self.assertTrue(unit.startswith(config.unit_name))
                    self.assertIn(
                        ("unit", unit),
                        {(resource.kind, resource.value) for resource in step.acquires},
                    )

    def test_crash_plans_ledger_mount_pool_device_and_listener_lifecycles(self) -> None:
        xfs = SCENARIOS["xfs-over-nbd-restart"](self.context())
        zfs = SCENARIOS["zfs-over-nbd-restart"](self.context())
        matrix = SCENARIOS["crash-boundary-matrix"](self.context())
        self.assertTrue(
            any(
                resource.kind == "mount"
                for step in xfs.steps
                for resource in step.acquires
            )
        )
        self.assertTrue(
            any(
                resource.kind == "pool"
                for step in zfs.steps
                for resource in step.acquires
            )
        )
        self.assertTrue(
            any(
                resource.kind == "device"
                for step in xfs.steps
                for resource in step.requires
            )
        )
        self.assertTrue(
            any(
                resource.kind == "listener"
                for step in matrix.steps
                for resource in step.acquires
            )
        )

    def test_xfs_restart_bootstraps_and_reuses_one_exact_nbd_export(self) -> None:
        config = self.make_config()
        plan = SCENARIOS["xfs-over-nbd-restart"](self.context())
        commands = [step.argv for step in plan.steps]
        provision = [
            argv for argv in commands if "provision-striped" in argv
        ]
        self.assertEqual(len(provision), 1)
        self.assertEqual(
            provision[0][provision[0].index("provision-striped") + 2],
            config.unit_name,
        )
        connects = [
            argv
            for argv in commands
            if argv and argv[0] == NBD_CLIENT and "-d" not in argv
        ]
        self.assertEqual(len(connects), 2)
        for argv in connects:
            self.assertIn("-N", argv)
            self.assertEqual(argv[argv.index("-N") + 1], config.unit_name)
        self.assertEqual(plan.authority_export_id, config.unit_name)
        self.assertEqual(
            plan.bootstrap_config,
            config.run_root / "xfs-nbd-bootstrap.toml",
        )
        launches = [argv for argv in commands if argv[0] == "systemd-run"]
        self.assertEqual(len(launches), 3)
        for launch in launches:
            self.assertFalse(any(value.startswith("--setenv=") for value in launch))
            self.assertIn(f"--uid={os.getuid()}", launch)

    def test_xfs_bootstrap_socket_fits_linux_sun_len_under_runtime_parent(
        self,
    ) -> None:
        config = HarnessConfig.create(
            filesystem_ack_mode="volatile_memory",
            object_ack_mode="ssd",
            run_uuid=RUN_UUID,
            environ={
                "ZEROFS_TIERED_ROOT_PARENT": "/fast/zerofs-audit-runtime"
            },
            workspace_root=self.workspace,
            source_root=self.workspace,
        )

        plan = SCENARIOS["xfs-over-nbd-restart"](
            ScenarioContext(config, self.binary, self.zerofs_config)
        )
        socket = Path(
            next(
                resource.value
                for resource in plan.steps[0].acquires
                if resource.kind == "path"
            )
        )

        self.assertEqual(socket, config.run_root / "9p.sock")
        self.assertEqual(len(os.fsencode(socket)), 99)
        self.assertLess(len(os.fsencode(socket)), 108)

    def test_xfs_bootstrap_socket_rejects_a_parent_that_exceeds_sun_len(
        self,
    ) -> None:
        config = HarnessConfig.create(
            filesystem_ack_mode="volatile_memory",
            object_ack_mode="ssd",
            run_uuid=RUN_UUID,
            environ={
                "ZEROFS_TIERED_ROOT_PARENT": f"/fast/{'x' * 80}"
            },
            workspace_root=self.workspace,
            source_root=self.workspace,
        )

        with self.assertRaisesRegex(ConfigError, "SUN_LEN"):
            SCENARIOS["xfs-over-nbd-restart"](
                ScenarioContext(config, self.binary, self.zerofs_config)
            )

    def test_xfs_bootstrap_socket_uses_one_bounded_readiness_step(self) -> None:
        plan = SCENARIOS["xfs-over-nbd-restart"](self.context())
        waits = [
            step for step in plan.steps if step.wait_for_unix_socket is not None
        ]

        self.assertEqual(len(waits), 1)
        self.assertEqual(waits[0].argv[:2], ("test", "-S"))
        self.assertEqual(waits[0].wait_for_unix_socket, waits[0].argv[2])
        provision_index = next(
            index
            for index, step in enumerate(plan.steps)
            if "provision-striped" in step.argv
        )
        self.assertNotIn(
            ("sleep", "3"),
            [step.argv for step in plan.steps[:provision_index]],
        )

    def test_socket_readiness_wait_retries_and_is_bounded(self) -> None:
        config = self.make_config()
        lifecycle, _, probes = self.make_lifecycle(config)
        socket = config.run_root / "9p.sock"
        probes.socket_probe_results = [False, False, True]

        with mock.patch.object(lifecycle_module.time, "sleep") as sleep:
            lifecycle._wait_for_unix_socket(socket, timeout=10.0)
        self.assertEqual(sleep.call_count, 2)

        probes.socket_probe_results = [False]
        with (
            mock.patch.object(
                lifecycle_module.time,
                "monotonic",
                side_effect=(0.0, 1.0),
            ),
            mock.patch.object(lifecycle_module.time, "sleep"),
            self.assertRaisesRegex(LifecycleError, "Unix socket.*not ready"),
        ):
            lifecycle._wait_for_unix_socket(socket, timeout=0.5)

    def test_xfs_final_local_cutoff_is_after_unmount_and_detach_before_sigkill(
        self,
    ) -> None:
        plan = SCENARIOS["xfs-over-nbd-restart"](self.context())
        steps = list(plan.steps)
        kill_index = next(
            index
            for index, step in enumerate(steps)
            if step.argv[:2] == ("systemctl", "kill")
        )
        detach_index = max(
            index
            for index, step in enumerate(steps[:kill_index])
            if step.argv[:2] == (NBD_CLIENT, "-d")
        )
        unmount_index = max(
            index
            for index, step in enumerate(steps[:detach_index])
            if step.argv and step.argv[0] == "umount"
        )
        cutoff_index = next(
            index
            for index, step in enumerate(steps)
            if step.after_checkpoint == "final-local-cutoff"
        )
        self.assertLess(unmount_index, detach_index)
        self.assertEqual(cutoff_index, detach_index)
        self.assertLess(cutoff_index, kill_index)
        self.assertEqual(steps[detach_index].verify_detached_device, NBD_DEVICE)
        self.assertEqual(
            steps[kill_index].verify_stopped_unit,
            self.make_config().unit_name,
        )

    def test_xfs_restart_captures_and_compares_the_same_checksum_key(self) -> None:
        plan = SCENARIOS["xfs-over-nbd-restart"](self.context())
        captures = [
            step.capture_sha256_as
            for step in plan.steps
            if step.capture_sha256_as is not None
        ]
        comparisons = [
            step.compare_sha256_with
            for step in plan.steps
            if step.compare_sha256_with is not None
        ]
        self.assertEqual(captures, ["xfs-proof-before-restart"])
        self.assertEqual(comparisons, captures)

    def test_xfs_runtime_pins_authority_then_requires_a_restart_identity(self) -> None:
        plan = SCENARIOS["xfs-over-nbd-restart"](self.context())
        checkpoints = [
            step.after_checkpoint
            for step in plan.steps
            if step.after_checkpoint is not None
        ]
        self.assertEqual(
            checkpoints,
            ["pin-initial-authority", "final-local-cutoff", "require-restart"],
        )

    def test_webui_plan_admits_the_missing_browser_wasm_and_grpc_web_leg(self) -> None:
        plan = SCENARIOS["webui-rpc-production-path"](self.context())
        self.assertTrue(plan.acceptance_gaps)
        rendered = " ".join(plan.acceptance_gaps).lower()
        self.assertIn("wasm", rendered)
        self.assertIn("grpc-web", rendered)

    def test_plans_only_touch_owned_mountpoints(self) -> None:
        config = self.make_config()
        for name in sorted(
            CONTRACT_SCENARIOS - {"benchmark-ram-ack", "benchmark-local-ssd"}
        ):
            plan = SCENARIOS[name](self.context())
            for step in plan.steps:
                if step.argv[0] in ("mount.nfs",):
                    self.assertTrue(
                        step.argv[2].startswith(str(config.resource_root)), name
                    )


class LinuxSuiteTests(HarnessCase):
    def test_tool_revisions_match_the_contract_pins(self) -> None:
        self.assertEqual(
            linux_suites.PINNED_REVISIONS,
            {
                "xfstests": "1ae822c1c2e2364e966085cee3ce4a97b2500241",
                "pjdfstest": "85a8aea9e685999ef0540392fd80535f873d7ff7",
                "pjdfstest_nfs": "7d3d7cb0cdc5d39eedd995771bc1d4b3dabf31ab",
            },
        )

    def test_kernel_archive_pin_matches_the_contract(self) -> None:
        self.assertEqual(
            linux_suites.KERNEL_ARCHIVE_URL,
            "https://cdn.kernel.org/pub/linux/kernel/v6.x/linux-6.18.tar.xz",
        )
        self.assertEqual(
            linux_suites.KERNEL_ARCHIVE_SHA256,
            "9106a4605da9e31ff17659d958782b815f9591ab308d03b0ee21aad6c7dced4b",
        )

    def test_kernel_archive_verification_rejects_a_mismatched_hash(self) -> None:
        archive = self.base / "linux-6.18.tar.xz"
        archive.write_bytes(b"definitely not the kernel")
        with self.assertRaises(IntegrityError):
            linux_suites.verify_kernel_archive(archive)
        digest = hashlib.sha256(archive.read_bytes()).hexdigest()
        self.assertEqual(
            linux_suites.verify_kernel_archive(archive, expected=digest), digest
        )

    def test_kernel_plans_verify_the_archive_before_extraction(self) -> None:
        config = self.make_config()
        context = ScenarioContext(
            config=config,
            zerofs_binary=self.binary,
            zerofs_config=self.zerofs_config,
        )
        plan = SCENARIOS["kernel-compile-nfs"](context)
        rendered = [" ".join(step.argv) for step in plan.steps]
        verify_index = next(
            index
            for index, text in enumerate(rendered)
            if linux_suites.KERNEL_ARCHIVE_SHA256 in text
        )
        extract_index = next(
            index for index, text in enumerate(rendered) if text.startswith("tar ")
        )
        self.assertLess(verify_index, extract_index)
        download = next(text for text in rendered if "curl" in text)
        self.assertIn(linux_suites.KERNEL_ARCHIVE_URL, download)

    def test_tool_checkouts_stay_under_the_resource_root_tools_dir(self) -> None:
        config = self.make_config()
        context = ScenarioContext(
            config=config,
            zerofs_binary=self.binary,
            zerofs_config=self.zerofs_config,
        )
        for name in ("xfstests-nfs-quick", "pjdfstest-nfs", "pjdfstest-ninep"):
            plan = SCENARIOS[name](context)
            clones = [
                step.argv
                for step in plan.steps
                if step.argv[0] == "git" and "clone" in step.argv
            ]
            self.assertTrue(clones, name)
            for argv in clones:
                self.assertTrue(
                    argv[-1].startswith(str(config.tools_root)),
                    f"{name}: clone target {argv[-1]} escapes {config.tools_root}",
                )
            checkouts = [
                step.argv
                for step in plan.steps
                if step.argv[0] == "git" and "checkout" in step.argv
            ]
            self.assertTrue(checkouts, name)
            pinned = set(linux_suites.PINNED_REVISIONS.values())
            for argv in checkouts:
                self.assertTrue(pinned & set(argv), f"{name}: unpinned checkout {argv}")


class RunScenarioTests(HarnessCase):
    def run_unit_plan(
        self,
        lifecycle: HarnessLifecycle,
        ledger: ResourceLedger,
        scenario: str,
    ) -> dict[str, Any]:
        builder = SCENARIOS[scenario]

        def without_frontier_gate(context: ScenarioContext):
            return replace(
                builder(context),
                requires_observed_durability=False,
                acceptance_gaps=(),
            )

        with mock.patch.dict(SCENARIOS, {scenario: without_frontier_gate}):
            return lifecycle.run_scenario(
                ledger,
                scenario,
                zerofs_binary=self.binary,
                zerofs_config=self.zerofs_config,
                plan_only=False,
            )

    def test_plan_only_run_writes_a_receipt_with_the_manifest(self) -> None:
        config = self.make_config()
        lifecycle, _, _, ledger = self.setup_run(config)
        summary = lifecycle.run_scenario(
            ledger,
            "global-admission-nbd-nfs-ninep",
            zerofs_binary=self.binary,
            zerofs_config=self.zerofs_config,
            plan_only=True,
        )
        receipt = json.loads(Path(summary["receipt"]).read_text())
        self.assertEqual(receipt["scenario"], "global-admission-nbd-nfs-ninep")
        self.assertEqual(receipt["terminal_state"], "planned")
        self.assertIsNone(receipt["exit_status"])
        self.assertEqual(receipt["status"], "planned")
        self.assertTrue(receipt["manifest"]["steps"])
        self.assertTrue(receipt["manifest"]["expected_durability"])
        self.assertEqual(receipt["durability_floors"], [])
        for field in REQUIRED_RECEIPT_FIELDS:
            self.assertIn(field, receipt, field)

    def test_c3_refuses_to_convert_ack_flags_into_observed_frontiers(self) -> None:
        config = self.make_config(filesystem="volatile_memory", object_="memory")
        lifecycle, runner, _, ledger = self.setup_run(config)
        with self.assertRaisesRegex(LifecycleError, "browser WASM"):
            lifecycle.run_scenario(
                ledger,
                "webui-rpc-production-path",
                zerofs_binary=self.binary,
                zerofs_config=self.zerofs_config,
                plan_only=False,
            )
        self.assertFalse(
            any(args[0] in ("grpcurl", "websocat") for args, _ in runner.calls)
        )
        receipts = sorted(config.receipt_root.rglob("manifest.json"))
        payloads = [json.loads(path.read_text()) for path in receipts]
        payload = next(
            candidate
            for candidate in payloads
            if candidate.get("scenario") == "webui-rpc-production-path"
        )
        self.assertEqual(payload["durability_floors"], [])
        self.assertEqual(payload["terminal_state"], "failed")
        self.assertEqual(payload["cleanup_status"], "ok")

    def test_execution_requires_linux(self) -> None:
        config = self.make_config()
        runner = FakeRunner()
        lifecycle = HarnessLifecycle(
            config, runner, probes=FakeProbes(), platform="darwin"
        )
        lifecycle.setup(zerofs_binary=self.binary, zerofs_config=self.zerofs_config)
        ledger = ResourceLedger.load(config.ledger_path)
        with self.assertRaisesRegex(LifecycleError, "Linux"):
            lifecycle.run_scenario(
                ledger,
                "global-admission-nbd-nfs-ninep",
                zerofs_binary=self.binary,
                zerofs_config=self.zerofs_config,
                plan_only=False,
            )

    def test_run_verifies_the_binary_against_the_ledger_identity(self) -> None:
        config = self.make_config()
        lifecycle, _, _, ledger = self.setup_run(config)
        imposter = self.base / "imposter"
        imposter.write_bytes(b"different payload")
        with self.assertRaises(ConfigError):
            lifecycle.run_scenario(
                ledger,
                "global-admission-nbd-nfs-ninep",
                zerofs_binary=imposter,
                zerofs_config=self.zerofs_config,
                plan_only=True,
            )

    def test_run_failure_triggers_cleanup_and_reports_both_errors(self) -> None:
        config = self.make_config()
        lifecycle, runner, _, ledger = self.setup_run(config)
        runner.failures[("mount.nfs",)] = RuntimeError("injected primary failure")

        real_cleanup = lifecycle.cleanup

        def failing_cleanup(target: ResourceLedger) -> dict[str, Any]:
            real_cleanup(target)
            raise CleanupError("injected cleanup failure")

        with mock.patch.object(lifecycle, "cleanup", side_effect=failing_cleanup):
            with self.assertRaises(PrimaryAndCleanupError) as caught:
                self.run_unit_plan(lifecycle, ledger, "benchmark-paced-remote")
        self.assertIn("injected primary failure", str(caught.exception))
        self.assertIn("injected cleanup failure", str(caught.exception))
        receipt = json.loads(Path(caught.exception.receipt).read_text())
        self.assertEqual(receipt["terminal_state"], "failed")
        self.assertEqual(receipt["cleanup_status"], "failed")

    def test_run_failure_with_successful_cleanup_reports_the_primary_error(
        self,
    ) -> None:
        config = self.make_config()
        lifecycle, runner, _, ledger = self.setup_run(config)
        runner.failures[("mount.nfs",)] = RuntimeError("injected primary failure")
        with self.assertRaisesRegex(RuntimeError, "injected primary failure"):
            self.run_unit_plan(lifecycle, ledger, "benchmark-paced-remote")
        self.assertFalse(config.resource_root.exists())

    def test_supervisor_cancellation_is_recorded_and_cleaned_up(self) -> None:
        config = self.make_config()
        lifecycle, runner, _, ledger = self.setup_run(config)
        runner.failures[("mount.nfs",)] = KeyboardInterrupt()
        with self.assertRaises(KeyboardInterrupt):
            self.run_unit_plan(lifecycle, ledger, "benchmark-paced-remote")
        receipts = sorted(config.receipt_root.rglob("manifest.json"))
        payloads = [json.loads(path.read_text()) for path in receipts]
        cancelled = [
            payload
            for payload in payloads
            if payload.get("terminal_state") == "cancelled"
        ]
        self.assertTrue(cancelled)
        self.assertEqual(cancelled[-1]["cleanup_status"], "ok")
        self.assertFalse(config.resource_root.exists())

    def test_successful_execution_records_commands_and_exit_status(self) -> None:
        config = self.make_config()
        lifecycle, runner, _, ledger = self.setup_run(config)
        summary = self.run_unit_plan(lifecycle, ledger, "benchmark-paced-remote")
        receipt = json.loads(Path(summary["receipt"]).read_text())
        self.assertEqual(receipt["terminal_state"], "completed")
        self.assertEqual(receipt["exit_status"], 0)
        self.assertTrue(receipt["commands"])
        executed = [tuple(command) for command in receipt["commands"]]
        self.assertEqual(
            executed,
            [args for args, _ in runner.calls[len(runner.calls) - len(executed) :]],
        )
        reloaded = ResourceLedger.load(config.ledger_path)
        acquired = {(kind, str(value)) for kind, value, _ in reloaded.resources()}
        self.assertIn(("unit", config.unit_name), acquired)
        self.assertIn(("process", "4242"), acquired)
        self.assertEqual(receipt["pids"], [4242])
        self.assertEqual(receipt["units"], [config.unit_name])
        self.assertIn(("listener", str(protocols.NFS_PORT)), acquired)
        self.assertNotIn(
            "process",
            {kind for kind, _, _ in reloaded.outstanding()},
        )
        self.assertNotIn(
            "listener",
            {kind for kind, _, _ in reloaded.outstanding()},
        )

    def test_crash_restart_reacquires_the_run_scoped_process(self) -> None:
        config = self.make_config()
        lifecycle, _, _, ledger = self.setup_run(config)
        summary = self.run_unit_plan(lifecycle, ledger, "local-receipt-restart")
        self.assertEqual(summary["terminal_state"], "completed")
        reloaded = ResourceLedger.load(config.ledger_path)
        process_acquires = [
            value for kind, value, _ in reloaded.resources() if kind == "process"
        ]
        self.assertEqual(process_acquires, [4242, 4243])
        unit_acquires = [
            value for kind, value, _ in reloaded.resources() if kind == "unit"
        ]
        self.assertEqual(unit_acquires, [config.unit_name, config.unit_name])
        self.assertNotIn(
            "process",
            {kind for kind, _, _ in reloaded.outstanding()},
        )

    def test_every_crash_plan_balances_its_runtime_resources(self) -> None:
        for scenario in (
            "xfs-over-nbd-restart",
            "zfs-over-nbd-restart",
            "crash-boundary-matrix",
            "local-receipt-restart",
            "remote-receipt-clean-cache-restart",
            "terminal-fanout-and-shutdown-timeout",
        ):
            with self.subTest(scenario=scenario):
                config = self.make_config(run_uuid=str(uuid.uuid4()))
                lifecycle, _, _, ledger = self.setup_run(config)
                self.run_unit_plan(lifecycle, ledger, scenario)
                active_kinds = {
                    kind
                    for kind, _, _ in ResourceLedger.load(
                        config.ledger_path
                    ).outstanding()
                }
                expected = (
                    set()
                    if scenario == "xfs-over-nbd-restart"
                    else {"path", "prefix"}
                )
                self.assertEqual(active_kinds, expected)

    def test_xfs_runtime_records_observed_cutoff_restart_and_checksum(self) -> None:
        config = self.make_config(filesystem="volatile_memory", object_="ssd")
        runtime_config = self.base / "xfs-runtime.toml"
        runtime_config.write_text(
            """
[storage]
url = "s3://bucket/prefix"
# TIERED_RUNTIME_SERVERS_BEGIN
[servers.nbd]
addresses = ["127.0.0.1:10809"]
# TIERED_RUNTIME_SERVERS_END
[writeback]
enabled = true
min_free_gb = 0.25
""",
            encoding="utf-8",
        )
        initial = FakeObservedCollector(
            observed_snapshot("instance-a", accepted=2, local=2, remote=2),
            observed_snapshot("instance-a", accepted=9, local=9, remote=4),
        )
        restarted = FakeObservedCollector(
            observed_snapshot("instance-b", accepted=9, local=9, remote=4),
            observed_snapshot("instance-b", accepted=9, local=9, remote=4),
        )
        collectors = iter((initial, restarted))
        runner = FakeRunner()
        lifecycle = HarnessLifecycle(
            config,
            runner,
            probes=FakeProbes(),
            platform="linux",
            observed_factory=lambda: next(collectors),
        )
        lifecycle.setup(zerofs_binary=self.binary, zerofs_config=runtime_config)
        ledger = ResourceLedger.load(config.ledger_path)

        summary = lifecycle.run_scenario(
            ledger,
            "xfs-over-nbd-restart",
            zerofs_binary=self.binary,
            zerofs_config=runtime_config,
        )

        receipt = json.loads(Path(summary["receipt"]).read_text(encoding="utf-8"))
        self.assertEqual(
            initial.calls,
            [
                ("wait-initial", 30.0),
                ("wait-accepted", 2),
                ("wait-local", 9),
            ],
        )
        self.assertEqual(restarted.calls[0][0], "wait-restarted")
        self.assertEqual(receipt["durability_cutoff"], 9)
        self.assertEqual(
            receipt["pre_kill_remote_coverage"],
            "remote-not-covered-at-pre-kill-sample",
        )
        self.assertEqual(receipt["checksums"]["xfs-proof-before-restart"], "a" * 64)
        self.assertEqual(
            receipt["observed_durability"]["after_restart"]["identity"][
                "server_instance_id"
            ],
            "instance-b",
        )
        self.assertEqual(receipt["cleanup_status"], "ok")
        self.assertTrue(receipt["cleanup_verification"]["clean"])
        self.assertFalse(config.resource_root.exists())

    def test_xfs_runtime_rejects_a_plan_missing_restart_evidence(self) -> None:
        config = self.make_config(filesystem="volatile_memory", object_="ssd")
        runtime_config = self.base / "xfs-runtime.toml"
        runtime_config.write_text(
            """
[storage]
url = "s3://bucket/prefix"
# TIERED_RUNTIME_SERVERS_BEGIN
[servers.nbd]
addresses = ["127.0.0.1:10809"]
# TIERED_RUNTIME_SERVERS_END
[writeback]
enabled = true
min_free_gb = 0.25
""",
            encoding="utf-8",
        )
        initial = FakeObservedCollector(
            observed_snapshot("instance-a", accepted=2, local=2, remote=2),
            observed_snapshot("instance-a", accepted=9, local=9, remote=4),
        )
        lifecycle = HarnessLifecycle(
            config,
            FakeRunner(),
            probes=FakeProbes(),
            platform="linux",
            observed_factory=lambda: initial,
        )
        lifecycle.setup(zerofs_binary=self.binary, zerofs_config=runtime_config)
        ledger = ResourceLedger.load(config.ledger_path)
        original = SCENARIOS["xfs-over-nbd-restart"]

        def without_restart_checkpoint(context: ScenarioContext):
            plan = original(context)
            return replace(
                plan,
                steps=tuple(
                    replace(step, after_checkpoint=None)
                    if step.after_checkpoint == "require-restart"
                    else step
                    for step in plan.steps
                ),
            )

        with mock.patch.dict(
            SCENARIOS, {"xfs-over-nbd-restart": without_restart_checkpoint}
        ):
            with self.assertRaisesRegex(LifecycleError, "missing required runtime evidence"):
                lifecycle.run_scenario(
                    ledger,
                    "xfs-over-nbd-restart",
                    zerofs_binary=self.binary,
                    zerofs_config=runtime_config,
                )


class IntegrityHelperTests(HarnessCase):
    def test_verify_copied_tree_detects_divergent_copies(self) -> None:
        source = self.base / "control"
        (source / "receipts").mkdir(parents=True)
        (source / "ledger.json").write_text("{}", encoding="utf-8")
        (source / "receipts" / "manifest.json").write_text("{}", encoding="utf-8")
        destination = self.base / "copy"
        shutil.copytree(source, destination)
        hashes = verify_copied_tree(source, destination)
        self.assertEqual(len(hashes), 2)
        (destination / "ledger.json").write_text('{"tampered":1}', encoding="utf-8")
        with self.assertRaises(IntegrityError):
            verify_copied_tree(source, destination)

    def test_durability_floor_rejects_unknown_modes(self) -> None:
        with self.assertRaises(ConfigError):
            DurabilityFloor("nfs-commit", "hopeful", "remote")
        with self.assertRaises(ConfigError):
            DurabilityFloor("nfs-commit", "materialized", "floppy")
        floor = DurabilityFloor("nfs-commit", "materialized", "remote")
        self.assertEqual(
            floor.to_dict(),
            {
                "operation": "nfs-commit",
                "filesystem_floor": "materialized",
                "object_floor": "remote",
            },
        )


class CliTests(HarnessCase):
    SCRIPT = Path(__file__).parents[1] / "tiered-writeback-e2e.py"

    def cli(self, *args: str, expect: int = 0) -> dict[str, Any]:
        env = dict(os.environ)
        env["ZEROFS_TIERED_ROOT_PARENT"] = str(self.root_parent)
        result = subprocess.run(
            [sys.executable, str(self.SCRIPT), *args],
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env=env,
            check=False,
        )
        self.assertEqual(result.returncode, expect, result.stderr or result.stdout)
        if expect == 0 and result.stdout.strip():
            return json.loads(result.stdout)
        return {}

    def test_cli_exposes_every_contract_subcommand(self) -> None:
        result = subprocess.run(
            [sys.executable, str(self.SCRIPT), "--help"],
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=False,
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        for command in (
            "setup",
            "run",
            "cleanup",
            "assert-clean",
            "archive-control",
            "assert-source-idle",
            "ledger-value",
            "validate-owned-path",
        ):
            self.assertIn(command, result.stdout)

    def test_cli_setup_and_run_require_both_ack_flags(self) -> None:
        for args in (
            ("setup",),
            ("setup", "--filesystem-ack-mode", "materialized"),
            ("setup", "--object-ack-mode", "remote"),
            ("run", "--ledger", "/tmp/x", "--scenario", "pjdfstest-nfs"),
        ):
            with self.subTest(args=args):
                result = subprocess.run(
                    [sys.executable, str(self.SCRIPT), *args],
                    text=True,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    check=False,
                )
                self.assertEqual(result.returncode, 2, result.stderr)
                self.assertIn("ack-mode", result.stderr)

    def test_cli_round_trip_setup_run_cleanup_assert_clean_archive(self) -> None:
        setup = self.cli(
            "setup",
            "--run-uuid",
            RUN_UUID,
            "--filesystem-ack-mode",
            "materialized",
            "--object-ack-mode",
            "remote",
            "--zerofs-binary",
            str(self.binary),
            "--zerofs-config",
            str(self.zerofs_config),
        )
        ledger = setup["ledger"]
        self.assertTrue(Path(ledger).is_file())

        run = self.cli(
            "run",
            "--ledger",
            ledger,
            "--scenario",
            "global-admission-nbd-nfs-ninep",
            "--filesystem-ack-mode",
            "materialized",
            "--object-ack-mode",
            "remote",
            "--zerofs-binary",
            str(self.binary),
            "--zerofs-config",
            str(self.zerofs_config),
            "--plan-only",
        )
        self.assertEqual(run["terminal_state"], "planned")

        value = self.cli("ledger-value", "--ledger", ledger, "identity.run_uuid")
        self.assertEqual(value["value"], RUN_UUID)

        resource_root = self.cli(
            "ledger-value", "--ledger", ledger, "identity.resource_root"
        )["value"]
        owned = self.cli(
            "validate-owned-path",
            "--ledger",
            ledger,
            str(Path(resource_root) / "mnt" / "nfs"),
        )
        self.assertTrue(owned["owned"])
        self.cli("validate-owned-path", "--ledger", ledger, "/mnt", expect=1)

        self.cli("cleanup", "--ledger", ledger)
        self.assertFalse(Path(resource_root).exists())
        self.cli("cleanup", "--ledger", ledger)

        clean = self.cli("assert-clean", "--ledger", ledger)
        self.assertTrue(clean["clean"])

        archive_root = self.base / "archive"
        self.cli(
            "archive-control",
            "--ledger",
            ledger,
            "--archive-root",
            str(archive_root),
        )
        self.assertTrue((archive_root / RUN_UUID / "ledger.json").is_file())
        self.assertFalse(Path(ledger).exists())

    def test_cli_assert_source_idle_uses_the_configured_proc_root(self) -> None:
        proc = self.base / "proc"
        entry = proc / "321"
        entry.mkdir(parents=True)
        (entry / "comm").write_text("cargo\n", encoding="utf-8")
        (entry / "cmdline").write_bytes(b"cargo\x00build\x00")
        os.symlink(self.workspace, entry / "cwd")
        result = subprocess.run(
            [
                sys.executable,
                str(self.SCRIPT),
                "assert-source-idle",
                "--source-root",
                str(self.workspace),
                "--proc-root",
                str(proc),
            ],
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=False,
        )
        self.assertEqual(result.returncode, 1)
        self.assertIn("321", result.stderr)


WORKFLOWS_ROOT = Path(__file__).resolve().parents[2] / ".github" / "workflows"

TIERED_CONTROL_ROOT = "/var/tmp/zerofs-tiered-control-${RUN_UUID}"
TIERED_RESOURCE_ROOT = "/var/tmp/zerofs-tiered-resources-${RUN_UUID}"

TIERED_ACK_FLAGS = (
    "--filesystem-ack-mode volatile_memory",
    "--object-ack-mode ssd",
)

# Workflow file -> (control job id, tiered job id, harness scenario, NBD leg).
TIERED_WORKFLOW_LEGS = {
    "xfstests-nfs.yml": (
        "xfstests",
        "xfstests-tiered",
        "xfstests-nfs-quick",
        False,
    ),
    "xfstests-9p.yml": (
        "xfstests",
        "xfstests-tiered",
        "xfstests-ninep-quick-and-strict",
        False,
    ),
    "pjdfstest.yml": ("pjdfstest", "pjdfstest-tiered", "pjdfstest-nfs", False),
    "pjdfstest-9p.yml": (
        "pjdfstest-9p",
        "pjdfstest-9p-tiered",
        "pjdfstest-ninep",
        False,
    ),
    "kernel-compile-nfs.yml": (
        "kernel-compile-nfs",
        "kernel-compile-nfs-tiered",
        "kernel-compile-nfs",
        False,
    ),
    "kernel-compile-9p.yml": (
        "kernel-compile-9p",
        "kernel-compile-9p-tiered",
        "kernel-compile-ninep",
        False,
    ),
    "stress-ng.yml": (
        "stress-ng-nfs",
        "stress-ng-tiered",
        "stress-ng-nfs-ninep",
        False,
    ),
    "zfs-test.yml": ("zfs-test", "zfs-test-tiered", "zfs-over-nbd-restart", True),
    "xfs-nbd.yml": ("xfs-nbd", "xfs-nbd-tiered", "xfs-over-nbd-restart", True),
}


class WorkflowContractTests(unittest.TestCase):
    """Static contract checks over the tiered CI workflow text."""

    def regions(self, filename: str) -> tuple[str, str]:
        """Split one workflow into (control region, tiered region)."""
        control_job, tiered_job, _, _ = TIERED_WORKFLOW_LEGS[filename]
        path = WORKFLOWS_ROOT / filename
        self.assertTrue(path.is_file(), f"{filename} is missing")
        text = path.read_text(encoding="utf-8")
        self.assertIn(
            f"\n  {control_job}:\n",
            text,
            f"{filename} lost its materialized control job {control_job!r}",
        )
        marker = f"\n  {tiered_job}:\n"
        self.assertIn(marker, text, f"{filename} has no {tiered_job!r} job")
        control_region, tiered_region = text.split(marker, 1)
        return control_region, tiered_region

    def harness_commands(self, region: str) -> list[str]:
        """Join backslash-continued harness invocations into single strings."""
        lines = region.splitlines()
        commands: list[str] = []
        index = 0
        while index < len(lines):
            line = lines[index]
            if "tiered-writeback-e2e.py" in line:
                parts = [line.strip()]
                while parts[-1].endswith("\\"):
                    index += 1
                    parts.append(lines[index].strip())
                commands.append(" ".join(part.rstrip("\\").strip() for part in parts))
            index += 1
        return commands

    def tiered_steps(self, region: str) -> list[str]:
        return re.split(r"\n      - name: ", region)

    def test_every_tiered_leg_declares_both_ack_flags(self) -> None:
        for filename in sorted(TIERED_WORKFLOW_LEGS):
            with self.subTest(workflow=filename):
                _, tiered = self.regions(filename)
                commands = self.harness_commands(tiered)
                setups = [c for c in commands if ".py setup " in f"{c} "]
                runs = [c for c in commands if ".py run " in f"{c} "]
                self.assertTrue(setups, f"{filename} never runs harness setup")
                self.assertTrue(runs, f"{filename} never runs a harness scenario")
                for command in setups + runs:
                    for flag in TIERED_ACK_FLAGS:
                        self.assertIn(
                            flag, command, f"{filename}: {command!r} lacks {flag!r}"
                        )

    def test_run_roots_are_disjoint_uuid_scoped_paths(self) -> None:
        for filename in sorted(TIERED_WORKFLOW_LEGS):
            with self.subTest(workflow=filename):
                _, tiered = self.regions(filename)
                self.assertIn(TIERED_CONTROL_ROOT, tiered, filename)
                self.assertIn(TIERED_RESOURCE_ROOT, tiered, filename)
                self.assertNotEqual(TIERED_CONTROL_ROOT, TIERED_RESOURCE_ROOT)

    def test_ledger_lives_under_the_control_root(self) -> None:
        for filename in sorted(TIERED_WORKFLOW_LEGS):
            with self.subTest(workflow=filename):
                _, tiered = self.regions(filename)
                self.assertIn("${CONTROL_ROOT}/ledger.json", tiered, filename)
                for command in self.harness_commands(tiered):
                    if any(
                        f".py {sub} " in f"{command} "
                        for sub in ("run", "cleanup", "assert-clean")
                    ):
                        self.assertIn(
                            '--ledger "$LEDGER"',
                            command,
                            f"{filename}: {command!r} bypasses the run ledger",
                        )

    def test_cleanup_is_always_run_twice_then_asserted_clean(self) -> None:
        for filename in sorted(TIERED_WORKFLOW_LEGS):
            with self.subTest(workflow=filename):
                _, tiered = self.regions(filename)
                cleanup_steps = [
                    step
                    for step in self.tiered_steps(tiered)
                    if 'cleanup --ledger "$LEDGER"' in step
                ]
                self.assertEqual(
                    len(cleanup_steps),
                    1,
                    f"{filename} needs exactly one tiered cleanup step",
                )
                step = cleanup_steps[0]
                self.assertIn(
                    "if: always()", step, f"{filename} cleanup is conditional"
                )
                self.assertEqual(
                    step.count('cleanup --ledger "$LEDGER"'),
                    2,
                    f"{filename} must invoke cleanup twice (idempotence proof)",
                )
                self.assertIn(
                    'assert-clean --ledger "$LEDGER"',
                    step,
                    f"{filename} never asserts the run left no residue",
                )
                self.assertGreater(
                    step.rindex("assert-clean --ledger"),
                    step.rindex("cleanup --ledger"),
                    f"{filename} asserts cleanliness before cleanup finished",
                )

    def test_tiered_legs_run_registered_scenarios(self) -> None:
        for filename, (_, _, scenario, _) in sorted(TIERED_WORKFLOW_LEGS.items()):
            with self.subTest(workflow=filename):
                self.assertIn(scenario, SCENARIO_NAMES)
                _, tiered = self.regions(filename)
                self.assertIn(f"--scenario {scenario}", tiered, filename)

    def test_materialized_control_legs_stay_unharnessed(self) -> None:
        for filename in sorted(TIERED_WORKFLOW_LEGS):
            with self.subTest(workflow=filename):
                control, _ = self.regions(filename)
                for marker in (
                    "tiered-writeback-e2e",
                    "--filesystem-ack-mode",
                    "--object-ack-mode",
                    "zerofs-tiered-control",
                    "zerofs-tiered-resources",
                    "volatile_memory",
                ):
                    self.assertNotIn(
                        marker,
                        control,
                        f"{filename}: control leg picked up tiered marker {marker!r}",
                    )

    def test_nbd_legs_check_runner_owned_devices(self) -> None:
        for filename, (_, _, _, nbd) in sorted(TIERED_WORKFLOW_LEGS.items()):
            if not nbd:
                continue
            with self.subTest(workflow=filename):
                _, tiered = self.regions(filename)
                self.assertIn(
                    "nbd-client -c /dev/nbd7",
                    tiered,
                    f"{filename} never proves the runner owns /dev/nbd7",
                )
        control, _ = self.regions("xfs-nbd.yml")
        self.assertIn(
            "nbd-client -c /dev/nbd0",
            control,
            "xfs-nbd.yml control leg never proves the runner owns /dev/nbd0",
        )

    def test_no_production_targets_anywhere(self) -> None:
        for filename in sorted(TIERED_WORKFLOW_LEGS):
            with self.subTest(workflow=filename):
                path = WORKFLOWS_ROOT / filename
                self.assertTrue(path.is_file(), f"{filename} is missing")
                text = path.read_text(encoding="utf-8").lower()
                for marker in PRODUCTION_MARKERS:
                    self.assertNotIn(
                        marker,
                        text,
                        f"{filename} mentions production marker {marker!r}",
                    )

    def test_xfs_tiered_separates_planning_from_real_runtime_acceptance(self) -> None:
        _, tiered = self.regions("xfs-nbd.yml")
        runs = [
            command
            for command in self.harness_commands(tiered)
            if ".py run " in f"{command} "
        ]
        self.assertEqual(len(runs), 2)
        self.assertIn("--plan-only", runs[0])
        self.assertNotIn("--plan-only", runs[1])

    def test_xfs_tiered_uses_the_checked_runtime_fixture_and_owned_tls(self) -> None:
        _, tiered = self.regions("xfs-nbd.yml")
        self.assertIn(
            "apt-get install -y nbd-client xfsprogs wget lsof openssl",
            tiered,
        )
        self.assertIn(
            "scripts/tiered_writeback_e2e/xfs_nbd_tiered.toml.template",
            tiered,
        )
        self.assertNotIn("cat > /tmp/zerofs-tiered.toml", tiered)
        self.assertIn('TLS_ROOT="${CONTROL_ROOT}/tls"', tiered)
        self.assertIn("openssl req -x509", tiered)
        self.assertIn("chmod 600", tiered)
        self.assertNotIn(
            'mkdir -p "${RESOURCE_ROOT}/run/cache" "${RESOURCE_ROOT}/run/writeback"',
            tiered,
        )

    def test_xfs_tiered_minio_container_and_bucket_are_uuid_scoped(self) -> None:
        _, tiered = self.regions("xfs-nbd.yml")
        self.assertIn('MINIO_CONTAINER="zerofs-tiered-minio-${RUN_UUID}"', tiered)
        self.assertIn('MINIO_BUCKET="zerofs-xfs-${RUN_UUID}"', tiered)
        self.assertIn('docker rm -f "$MINIO_CONTAINER"', tiered)
        self.assertNotIn("--name minio", tiered)


if __name__ == "__main__":
    unittest.main()
