from __future__ import annotations

import hashlib
import json
import tempfile
import unittest
from dataclasses import replace
from pathlib import Path
from subprocess import CompletedProcess
from typing import Mapping, Sequence

from scripts.vm100_pilot.config import PilotConfig
from scripts.vm100_pilot.metrics import WritebackSnapshot
from scripts.vm100_pilot.protocol_matrix import (
    ProtocolAuthority,
    ProtocolMatrixRunner,
    ScenarioUnavailableError,
)
from scripts.vm100_pilot.runner import Runner
from scripts.vm100_pilot.scenarios import (
    ScenarioDefinition,
    WorkloadDefinition,
    require_scenario,
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
    def __init__(self, snapshots: list[WritebackSnapshot]) -> None:
        self.snapshots = snapshots
        self.index = 0

    def snapshot(self) -> WritebackSnapshot:
        index = min(self.index, len(self.snapshots) - 1)
        self.index += 1
        return self.snapshots[index]


class Lifecycle:
    def __init__(self, snapshots: list[WritebackSnapshot]) -> None:
        self.metrics = SnapshotSource(snapshots)
        self.drain_calls = 0

    def status(self) -> dict[str, object]:
        return {"healthy": True}

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

    def test_nfs_authority_rejects_mutable_host_aliases(self) -> None:
        values = {
            "ZEROFS_BENCH_NFS_MOUNTPOINT": str(self.root),
            "ZEROFS_BENCH_NFS_ENDPOINT": "zerofs.internal:/",
            "ZEROFS_BENCH_NFS_MOUNT_OPTIONS": "rw,hard,vers=3",
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

    def test_real_small_transfer_has_exact_sha_cutoffs_and_double_cleanup(self) -> None:
        scenario = ScenarioDefinition(
            name="protocol-matrix-nfs-test",
            kind="protocol-matrix",
            protocol="nfs",
            description="small real transfer",
            workloads=(WorkloadDefinition("small", 4096, "test-pattern"),),
            required_authority=("mountpoint", "endpoint", "mount_options"),
            cutoffs=("foreground_close", "fsync_or_commit", "local", "remote"),
            sha256_required=True,
        )
        authority = ProtocolAuthority.from_mapping(
            "nfs",
            {
                "ZEROFS_BENCH_NFS_MOUNTPOINT": str(self.protocol_root),
                "ZEROFS_BENCH_NFS_ENDPOINT": "10.10.10.55:/",
                "ZEROFS_BENCH_NFS_MOUNT_OPTIONS": "rw,hard,vers=3",
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
        local = replace(accepted, local=11, dirty_ram=0, dirty_ssd_reserved=4096)
        remote = replace(local, remote=11, dirty_ssd_reserved=0)
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
        self.assertEqual(result.cleanup.attempts, 2)
        self.assertTrue(result.cleanup.asserted_clean)
        self.assertEqual(list(self.protocol_root.iterdir()), [])
        self.assertEqual(list(self.config.temp_dir.iterdir()), [])

    def test_registry_protocol_scenarios_are_not_noops(self) -> None:
        for name in ("protocol-matrix-nfs", "protocol-matrix-9p"):
            scenario = require_scenario(name)
            self.assertGreater(sum(item.bytes for item in scenario.workloads), 0)
            self.assertTrue(scenario.sha256_required)


if __name__ == "__main__":
    unittest.main()
