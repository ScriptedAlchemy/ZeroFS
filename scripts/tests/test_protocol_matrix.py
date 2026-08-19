from __future__ import annotations

import hashlib
import importlib.util
import json
import tempfile
import unittest
import io
from contextlib import redirect_stderr
from dataclasses import replace
from pathlib import Path
from subprocess import CompletedProcess
from typing import Mapping, Sequence
from unittest import mock

from scripts.vm100_pilot.config import PilotConfig
from scripts.vm100_pilot.metrics import (
    MetricsAuthorityIdentity,
    MetricsClient,
    WritebackSnapshot,
)
from scripts.vm100_pilot.protocol_matrix import (
    ProtocolAuthority,
    ProtocolMatrixRunner,
    ScenarioUnavailableError,
)
from scripts.vm100_pilot.runner import Runner
from scripts.vm100_pilot.scenarios import (
    ProtocolScenario,
    WorkloadDefinition,
    require_protocol_scenario,
)


ROOT = Path(__file__).resolve().parents[2]


def load_cli() -> object:
    spec = importlib.util.spec_from_file_location(
        "vm100_pilot_cli_protocol", ROOT / "scripts" / "vm100-pilot.py"
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def metrics_text(identity: MetricsAuthorityIdentity, accepted: int = 3) -> str:
    return "\n".join(
        (
            "zerofs_benchmark_authority_info{"
            f'export_id="{identity.export_id}",'
            f'filesystem_id="{identity.filesystem_id}",'
            f'server_instance_id="{identity.server_instance_id}"'
            "} 1",
            f"zerofs_writeback_accepted_sequence {accepted}",
            f"zerofs_writeback_local_sequence {accepted}",
            f"zerofs_writeback_remote_sequence {accepted}",
            "zerofs_writeback_dirty_ram_bytes 0",
            "zerofs_writeback_dirty_ssd_reserved_bytes 0",
            "zerofs_writeback_local_bytes_completed_total 1",
            "zerofs_writeback_remote_bytes_completed_total 1",
            "zerofs_writeback_terminal_error 0",
            "zerofs_segment_gc_passes_total 1",
            "zerofs_segment_gc_batches_total 1",
            "zerofs_segment_gc_deleted_bytes_total 0",
        )
    )


class AuthorityRunner(Runner):
    def __init__(self, payload: Mapping[str, object]) -> None:
        super().__init__(base_env={})
        self.payload = payload

    def run(
        self,
        argv: Sequence[str | Path],
        **_: object,
    ) -> CompletedProcess[str]:
        args = tuple(str(value) for value in argv)
        if args[:2] == ("findmnt", "--json"):
            return CompletedProcess(args, 0, json.dumps(self.payload), "")
        raise AssertionError(f"unexpected command: {args}")


class SnapshotSource:
    def __init__(
        self,
        snapshots: list[WritebackSnapshot],
        identity: MetricsAuthorityIdentity,
    ) -> None:
        self.snapshots = snapshots
        self.index = 0
        self.authority_identity = identity

    def snapshot(self) -> WritebackSnapshot:
        index = min(self.index, len(self.snapshots) - 1)
        self.index += 1
        return self.snapshots[index]

    def identity(self) -> MetricsAuthorityIdentity:
        return self.authority_identity


class Lifecycle:
    def __init__(
        self,
        snapshots: list[WritebackSnapshot],
        identity: MetricsAuthorityIdentity | None = None,
    ) -> None:
        self.metrics = SnapshotSource(
            snapshots,
            identity
            or MetricsAuthorityIdentity("instance-a", "filesystem-a", "nfs-root"),
        )
        self.drain_calls = 0
        self.metrics_endpoint = "https://10.10.10.55:9567/metrics"

    def status(self) -> dict[str, object]:
        return {"healthy": True}

    def identity(self) -> MetricsAuthorityIdentity:
        return self.metrics.identity()

    def snapshot(self) -> WritebackSnapshot:
        return self.metrics.snapshot()

    def drain(self, timeout: float | None = None) -> dict[str, object]:
        del timeout
        self.drain_calls += 1
        return {"drained": True}


class ProtocolAuthorityTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory(dir="/var/tmp")
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name) / "nfs"
        self.root.mkdir()

    def test_missing_authority_is_honestly_unavailable(self) -> None:
        with self.assertRaisesRegex(ScenarioUnavailableError, "NFS.*unavailable"):
            ProtocolAuthority.from_mapping("nfs", {})

    def test_metrics_identity_is_required_and_must_match_exactly(self) -> None:
        values = {
            "ZEROFS_BENCH_NFS_MOUNTPOINT": str(self.root),
            "ZEROFS_BENCH_NFS_ENDPOINT": "10.10.10.55:/",
            "ZEROFS_BENCH_NFS_MOUNT_OPTIONS": "rw,hard,vers=3",
            "ZEROFS_BENCH_NFS_METRICS_URL": "https://10.10.10.55:9567/metrics",
        }
        with self.assertRaisesRegex(ScenarioUnavailableError, "METRICS_INSTANCE_ID"):
            ProtocolAuthority.from_mapping("nfs", values)

        values |= {
            "ZEROFS_BENCH_NFS_METRICS_INSTANCE_ID": "instance-a",
            "ZEROFS_BENCH_NFS_METRICS_FILESYSTEM_ID": "filesystem-a",
            "ZEROFS_BENCH_NFS_METRICS_EXPORT_ID": "nfs-root",
        }
        authority = ProtocolAuthority.from_mapping("nfs", values)
        self.assertEqual(
            authority.metrics_identity,
            MetricsAuthorityIdentity("instance-a", "filesystem-a", "nfs-root"),
        )
        with self.assertRaisesRegex(ValueError, "credential-free HTTPS"):
            ProtocolAuthority.from_mapping(
                "nfs",
                values
                | {
                    "ZEROFS_BENCH_NFS_METRICS_URL": (
                        "http://10.10.10.55:9567/metrics"
                    )
                },
            )

    def test_metrics_identity_requires_one_exact_server_emitted_series(self) -> None:
        identity = MetricsAuthorityIdentity.parse(
            'zerofs_benchmark_authority_info{export_id="nfs-root",'
            'filesystem_id="filesystem-a",server_instance_id="instance-a"} 1\n'
        )
        self.assertEqual(identity.server_instance_id, "instance-a")
        with self.assertRaisesRegex(ValueError, "exactly one"):
            MetricsAuthorityIdentity.parse("zerofs_writeback_accepted_sequence 3\n")

    def test_every_snapshot_rejects_identity_drift_in_the_same_response(self) -> None:
        expected = MetricsAuthorityIdentity("instance-a", "filesystem-a", "nfs-root")
        wrong = MetricsAuthorityIdentity("instance-b", "filesystem-a", "nfs-root")
        client = MetricsClient("https://10.10.10.55/metrics", expected)
        with mock.patch.object(client, "_fetch", return_value=metrics_text(wrong)) as fetch:
            with self.assertRaisesRegex(ValueError, "identity mismatch"):
                client.snapshot()
        fetch.assert_called_once_with()

        client = MetricsClient("https://10.10.10.55/metrics")
        with mock.patch.object(
            client,
            "_fetch",
            side_effect=(metrics_text(expected), metrics_text(wrong)),
        ):
            self.assertEqual(client.snapshot().accepted, 3)
            with self.assertRaisesRegex(ValueError, "identity mismatch"):
                client.snapshot()

        identity_free = MetricsClient("https://10.10.10.55/metrics")
        with mock.patch.object(
            identity_free,
            "_fetch",
            return_value="zerofs_writeback_accepted_sequence 7\n",
        ):
            with self.assertRaisesRegex(ValueError, "exactly one"):
                identity_free.snapshot()

    def test_9p_authority_requires_unix_transport_and_source_bound_export(self) -> None:
        base = {
            "ZEROFS_BENCH_9P_MOUNTPOINT": str(self.root),
            "ZEROFS_BENCH_9P_ENDPOINT": "zerofs-test",
            "ZEROFS_BENCH_9P_MOUNT_OPTIONS": "rw,trans=unix,access=client",
            "ZEROFS_BENCH_9P_METRICS_URL": "https://127.0.0.1:9567/metrics",
            "ZEROFS_BENCH_9P_METRICS_INSTANCE_ID": "instance-a",
            "ZEROFS_BENCH_9P_METRICS_FILESYSTEM_ID": "filesystem-a",
            "ZEROFS_BENCH_9P_METRICS_EXPORT_ID": "zerofs-test",
        }
        authority = ProtocolAuthority.from_mapping("9p", base)
        self.assertEqual(authority.endpoint, "zerofs-test")
        with self.assertRaisesRegex(ScenarioUnavailableError, "trans=unix"):
            ProtocolAuthority.from_mapping(
                "9p",
                base | {"ZEROFS_BENCH_9P_MOUNT_OPTIONS": "rw,trans=tcp"},
            )
        with self.assertRaisesRegex(ScenarioUnavailableError, "export ID"):
            ProtocolAuthority.from_mapping(
                "9p",
                base | {"ZEROFS_BENCH_9P_METRICS_EXPORT_ID": "other-export"},
            )

    def test_nfs_authority_rejects_mutable_host_aliases(self) -> None:
        values = {
            "ZEROFS_BENCH_NFS_MOUNTPOINT": str(self.root),
            "ZEROFS_BENCH_NFS_ENDPOINT": "zerofs.internal:/",
            "ZEROFS_BENCH_NFS_MOUNT_OPTIONS": "rw,hard,vers=3",
            "ZEROFS_BENCH_NFS_METRICS_URL": "https://10.10.10.55:9567/metrics",
            "ZEROFS_BENCH_NFS_METRICS_INSTANCE_ID": "instance-a",
            "ZEROFS_BENCH_NFS_METRICS_FILESYSTEM_ID": "filesystem-a",
            "ZEROFS_BENCH_NFS_METRICS_EXPORT_ID": "nfs-root",
        }

        with self.assertRaisesRegex(ValueError, "literal IP"):
            ProtocolAuthority.from_mapping("nfs", values)

    def test_findmnt_authority_must_match_endpoint_type_and_options(self) -> None:
        authority = ProtocolAuthority.from_mapping(
            "nfs",
            {
                "ZEROFS_BENCH_NFS_MOUNTPOINT": str(self.root),
                "ZEROFS_BENCH_NFS_ENDPOINT": "10.10.10.55:/",
                "ZEROFS_BENCH_NFS_MOUNT_OPTIONS": "rw,hard,vers=3",
                "ZEROFS_BENCH_NFS_METRICS_URL": "https://10.10.10.55:9567/metrics",
                "ZEROFS_BENCH_NFS_METRICS_INSTANCE_ID": "instance-a",
                "ZEROFS_BENCH_NFS_METRICS_FILESYSTEM_ID": "filesystem-a",
                "ZEROFS_BENCH_NFS_METRICS_EXPORT_ID": "nfs-root",
            },
        )
        runner = AuthorityRunner(
            {
                "filesystems": [
                    {
                        "target": str(self.root),
                        "source": "10.10.10.55:/",
                        "fstype": "nfs",
                        "options": "rw,relatime,vers=3,hard,proto=tcp",
                    }
                ]
            }
        )

        receipt = authority.verify(runner)

        self.assertEqual(receipt["source"], "10.10.10.55:/")
        self.assertEqual(receipt["fstype"], "nfs")
        self.assertEqual(receipt["required_options"], ["hard", "rw", "vers=3"])

        mismatch = AuthorityRunner(
            {
                "filesystems": [
                    {
                        "target": str(self.root),
                        "source": "10.10.10.99:/",
                        "fstype": "nfs",
                        "options": "rw,hard,vers=3",
                    }
                ]
            }
        )
        with self.assertRaisesRegex(ScenarioUnavailableError, "source mismatch"):
            authority.verify(mismatch)

    def test_metrics_authority_must_match_the_nfs_server(self) -> None:
        with self.assertRaisesRegex(ValueError, "same literal server IP"):
            ProtocolAuthority.from_mapping(
                "nfs",
                {
                    "ZEROFS_BENCH_NFS_MOUNTPOINT": str(self.root),
                    "ZEROFS_BENCH_NFS_ENDPOINT": "10.10.10.55:/",
                    "ZEROFS_BENCH_NFS_MOUNT_OPTIONS": "rw,hard,vers=3",
                    "ZEROFS_BENCH_NFS_METRICS_URL": (
                        "https://10.10.10.99:9567/metrics"
                    ),
                    "ZEROFS_BENCH_NFS_METRICS_INSTANCE_ID": "instance-a",
                    "ZEROFS_BENCH_NFS_METRICS_FILESYSTEM_ID": "filesystem-a",
                    "ZEROFS_BENCH_NFS_METRICS_EXPORT_ID": "nfs-root",
                },
            )


class ProtocolMatrixTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory(dir="/var/tmp")
        self.addCleanup(self.temp.cleanup)
        root = Path(self.temp.name) / "repo"
        root.mkdir()
        mountpoint = Path(self.temp.name) / "legacy-mount"
        mountpoint.mkdir()
        self.protocol_root = Path(self.temp.name) / "nfs"
        self.protocol_root.mkdir()
        self.config = PilotConfig.from_mapping(
            root,
            {
                "ZEROFS_PILOT_RESULT_DIR": str(Path(self.temp.name) / "results"),
                "ZEROFS_PROFILE_TARGET_DIR": str(Path(self.temp.name) / "profile"),
                "ZEROFS_PILOT_TMP_DIR": str(Path(self.temp.name) / "tmp"),
                "ZEROFS_PILOT_MOUNTPOINT": str(mountpoint),
                "ZEROFS_PILOT_INTEGRITY_FILE": str(mountpoint / "integrity"),
                "ZEROFS_PILOT_METADATA_DIR": str(mountpoint / "metadata"),
            },
        )
        self.config.temp_dir.mkdir()

    def test_unavailable_protocol_authority_still_writes_failed_manifest(self) -> None:
        module = load_cli()
        args = module.build_parser().parse_args(
            ["protocol-matrix", "--protocol", "nfs"]
        )
        with mock.patch.dict("os.environ", {}, clear=True):
            with self.assertRaisesRegex(ScenarioUnavailableError, "unavailable"):
                module._run_protocol_matrix(args, self.config, Runner(base_env={}))

        manifests = list(self.config.result_dir.glob("*/manifest.json"))
        self.assertEqual(len(manifests), 1)
        payload = json.loads(manifests[0].read_text(encoding="utf-8"))
        self.assertEqual(payload["status"], "failed")
        self.assertEqual(payload["scenario"]["name"], "protocol-matrix-nfs")
        ledger = json.loads(
            (manifests[0].parent / "cleanup-ledger.json").read_text(encoding="utf-8")
        )
        self.assertEqual(ledger["resources"], [])
        self.assertTrue(ledger["asserted_clean"])
        self.assertNotIn("METRICS_INSTANCE_ID=", payload.get("error", ""))

    def test_shipping_protocol_entrypoint_ignores_invalid_legacy_config(self) -> None:
        module = load_cli()
        result_dir = Path(self.temp.name) / "entrypoint-results"
        values = {
            "ZEROFS_PILOT_NBD_SIZE_GIB": "not-an-integer",
            "ZEROFS_PILOT_RESULT_DIR": str(result_dir),
            "ZEROFS_PILOT_TMP_DIR": str(self.config.temp_dir),
            "ZEROFS_PILOT_LOCK_FILE": str(Path(self.temp.name) / "protocol.lock"),
        }
        with mock.patch.dict("os.environ", values, clear=True), redirect_stderr(
            io.StringIO()
        ):
            status = module.main(["protocol-matrix", "--protocol", "nfs"])

        self.assertEqual(status, 1)
        manifests = list(result_dir.glob("*/manifest.json"))
        self.assertEqual(len(manifests), 1)
        self.assertEqual(
            json.loads(manifests[0].read_text(encoding="utf-8"))["status"],
            "failed",
        )

    def test_metrics_url_credentials_never_enter_failed_manifest(self) -> None:
        module = load_cli()
        args = module.build_parser().parse_args(
            ["protocol-matrix", "--protocol", "nfs"]
        )
        values = {
            "ZEROFS_BENCH_NFS_MOUNTPOINT": str(self.protocol_root),
            "ZEROFS_BENCH_NFS_ENDPOINT": "10.10.10.55:/",
            "ZEROFS_BENCH_NFS_MOUNT_OPTIONS": "rw,hard,vers=3",
            "ZEROFS_BENCH_NFS_METRICS_URL": (
                "https://10.10.10.55:9567/metrics?token=REVIEW_SECRET_MARKER"
            ),
            "ZEROFS_BENCH_NFS_METRICS_INSTANCE_ID": "instance-a",
            "ZEROFS_BENCH_NFS_METRICS_FILESYSTEM_ID": "filesystem-a",
            "ZEROFS_BENCH_NFS_METRICS_EXPORT_ID": "nfs-root",
        }
        with mock.patch.dict("os.environ", values, clear=True):
            with self.assertRaisesRegex(ValueError, "credential-free HTTPS"):
                module._run_protocol_matrix(args, self.config, Runner(base_env={}))

        manifests = list(self.config.result_dir.glob("*/manifest.json"))
        self.assertEqual(len(manifests), 1)
        text = manifests[0].read_text(encoding="utf-8")
        self.assertNotIn("REVIEW_SECRET_MARKER", text)

    def test_real_small_transfer_has_exact_sha_cutoffs_and_double_cleanup(self) -> None:
        scenario = ProtocolScenario(
            name="protocol-matrix-nfs-test",
            protocol="nfs",
            description="small real transfer",
            workloads=(WorkloadDefinition("small", 4096, "test-pattern"),),
        )
        authority = ProtocolAuthority.from_mapping(
            "nfs",
            {
                "ZEROFS_BENCH_NFS_MOUNTPOINT": str(self.protocol_root),
                "ZEROFS_BENCH_NFS_ENDPOINT": "10.10.10.55:/",
                "ZEROFS_BENCH_NFS_MOUNT_OPTIONS": "rw,hard,vers=3",
                "ZEROFS_BENCH_NFS_METRICS_URL": "https://10.10.10.55:9567/metrics",
                "ZEROFS_BENCH_NFS_METRICS_INSTANCE_ID": "instance-a",
                "ZEROFS_BENCH_NFS_METRICS_FILESYSTEM_ID": "filesystem-a",
                "ZEROFS_BENCH_NFS_METRICS_EXPORT_ID": "nfs-root",
            },
        )
        findmnt = AuthorityRunner(
            {
                "filesystems": [
                    {
                        "target": str(self.protocol_root),
                        "source": "10.10.10.55:/",
                        "fstype": "nfs",
                        "options": "rw,hard,vers=3",
                    }
                ]
            }
        )
        before = WritebackSnapshot(10, 10, 10, 0, 0, 100, 100, False)
        accepted = replace(before, accepted=11, dirty_ram=4096)
        local = replace(
            accepted,
            local=11,
            dirty_ram=0,
            dirty_ssd_reserved=4096,
            local_bytes=4196,
        )
        remote = replace(
            local,
            remote=11,
            dirty_ssd_reserved=0,
            remote_bytes=4196,
        )
        lifecycle = Lifecycle([before, accepted, accepted, accepted, accepted, local, remote])
        runner = ProtocolMatrixRunner(
            self.config,
            findmnt,
            lifecycle,  # type: ignore[arg-type]
            random_bytes=lambda count: b"x" * count,
        )

        result = runner.run(scenario, authority)

        self.assertEqual(result.total_bytes, 4096)
        workload = result.workloads[0]
        digest = hashlib.sha256(b"x" * 4096).hexdigest()
        self.assertEqual(workload.source_sha256, digest)
        self.assertEqual(workload.readback_sha256, digest)
        self.assertEqual(workload.target_sequence, 11)
        self.assertGreater(workload.foreground_close_ns, 0)
        self.assertGreater(workload.fsync_or_commit_ns, 0)
        self.assertGreater(workload.local_cutoff_ns, 0)
        self.assertGreater(workload.remote_cutoff_ns, 0)
        self.assertGreaterEqual(
            workload.stable_remote_drain_ns,
            workload.remote_cutoff_ns,
        )
        self.assertEqual(workload.stable_remote_drain, {"drained": True})
        self.assertEqual(result.cleanup.attempts, 2)
        self.assertTrue(result.cleanup.asserted_clean)
        self.assertEqual(list(self.protocol_root.iterdir()), [])
        self.assertEqual(list(self.config.temp_dir.iterdir()), [])

    def test_registry_protocol_scenarios_are_not_noops(self) -> None:
        for name in ("protocol-matrix-nfs", "protocol-matrix-9p"):
            scenario = require_protocol_scenario(name)
            self.assertGreater(sum(item.bytes for item in scenario.workloads), 0)
            self.assertTrue(scenario.sha256_required)

    def test_partial_root_creation_failure_is_cleaned(self) -> None:
        scenario = ProtocolScenario(
            name="protocol-matrix-nfs-test",
            protocol="nfs",
            description="small real transfer",
            workloads=(WorkloadDefinition("small", 4096, "test-pattern"),),
        )
        authority = ProtocolAuthority.from_mapping(
            "nfs",
            {
                "ZEROFS_BENCH_NFS_MOUNTPOINT": str(self.protocol_root),
                "ZEROFS_BENCH_NFS_ENDPOINT": "10.10.10.55:/",
                "ZEROFS_BENCH_NFS_MOUNT_OPTIONS": "rw,hard,vers=3",
                "ZEROFS_BENCH_NFS_METRICS_URL": "https://10.10.10.55:9567/metrics",
                "ZEROFS_BENCH_NFS_METRICS_INSTANCE_ID": "instance-a",
                "ZEROFS_BENCH_NFS_METRICS_FILESYSTEM_ID": "filesystem-a",
                "ZEROFS_BENCH_NFS_METRICS_EXPORT_ID": "nfs-root",
            },
        )
        findmnt = AuthorityRunner(
            {
                "filesystems": [
                    {
                        "target": str(self.protocol_root),
                        "source": "10.10.10.55:/",
                        "fstype": "nfs",
                        "options": "rw,hard,vers=3",
                    }
                ]
            }
        )
        before = WritebackSnapshot(10, 10, 10, 0, 0, 100, 100, False)
        lifecycle = Lifecycle([before])
        runner = ProtocolMatrixRunner(
            self.config,
            findmnt,
            lifecycle,  # type: ignore[arg-type]
        )
        original_mkdir = Path.mkdir

        def fail_scratch(path: Path, *args: object, **kwargs: object) -> None:
            if path.parent == self.config.temp_dir and path.name.startswith(
                "zerofs-protocol-bench-"
            ):
                raise OSError("injected scratch mkdir failure")
            original_mkdir(path, *args, **kwargs)

        with mock.patch.object(Path, "mkdir", new=fail_scratch):
            with self.assertRaisesRegex(OSError, "injected scratch mkdir failure"):
                runner.run(scenario, authority)

        self.assertEqual(list(self.protocol_root.iterdir()), [])

    def test_same_host_wrong_metrics_identity_fails_before_writes(self) -> None:
        scenario = ProtocolScenario(
            "protocol-matrix-nfs-test",
            "nfs",
            "small real transfer",
            (WorkloadDefinition("small", 4096, "test-pattern"),),
        )
        authority = ProtocolAuthority.from_mapping(
            "nfs",
            {
                "ZEROFS_BENCH_NFS_MOUNTPOINT": str(self.protocol_root),
                "ZEROFS_BENCH_NFS_ENDPOINT": "10.10.10.55:/",
                "ZEROFS_BENCH_NFS_MOUNT_OPTIONS": "rw,hard,vers=3",
                "ZEROFS_BENCH_NFS_METRICS_URL": "https://10.10.10.55:9567/metrics",
                "ZEROFS_BENCH_NFS_METRICS_INSTANCE_ID": "instance-a",
                "ZEROFS_BENCH_NFS_METRICS_FILESYSTEM_ID": "filesystem-a",
                "ZEROFS_BENCH_NFS_METRICS_EXPORT_ID": "nfs-root",
            },
        )
        findmnt = AuthorityRunner(
            {"filesystems": [{
                "target": str(self.protocol_root),
                "source": "10.10.10.55:/",
                "fstype": "nfs",
                "options": "rw,hard,vers=3",
            }]}
        )
        before = WritebackSnapshot(10, 10, 10, 0, 0, 100, 100, False)
        observer = Lifecycle(
            [before],
            MetricsAuthorityIdentity("wrong-instance", "filesystem-a", "nfs-root"),
        )

        with self.assertRaisesRegex(ScenarioUnavailableError, "identity mismatch"):
            ProtocolMatrixRunner(
                self.config, findmnt, observer  # type: ignore[arg-type]
            ).run(scenario, authority)
        self.assertEqual(list(self.protocol_root.iterdir()), [])


if __name__ == "__main__":
    unittest.main()
