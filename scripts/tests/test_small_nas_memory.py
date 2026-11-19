"""The memory evidence gates must survive optimized Python execution."""

import copy
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest


SCRIPT = Path(__file__).resolve().parents[1] / "derive-small-nas-memory.py"


class SmallNasMemoryEvidenceTests(unittest.TestCase):
    def fixture(self):
        return {
            "outcome.json": {
                "completed": True,
                "build_profile": "ci+webui",
                "companion": {
                    "sha256": "all verified",
                    "sequential_bytes": 12 * 1024**3,
                    "small_files": 5000,
                    "gc_passes_after_flush": 1,
                },
            },
            "samples.jsonl": {
                "swap_current_bytes": 0,
                "events": {"oom_kill": 0},
                "metrics": {"zerofs_jemalloc_resident_bytes": 3_241_541_632},
                "process_status": {"VmHWM": "3000000 kB"},
                "stat": {"kernel": 137_129_984, "sock": 0},
            },
        }

    def run_cell(self, files, optimized):
        with tempfile.TemporaryDirectory(prefix="zerofs-memory-evidence-") as root:
            cell = Path(root)
            for name, content in files.items():
                (cell / name).write_text(json.dumps(content) + "\n")
            flags = ["-O"] if optimized else []
            return subprocess.run(
                [sys.executable, *flags, str(SCRIPT), str(cell)],
                capture_output=True,
                text=True,
                timeout=10,
            )

    def native_fixture(self, manifest=None):
        if manifest is None:
            manifest = {"sequential": {"size": 12 * 1024**3}}
            manifest.update({f"small-{index}": {"size": 1024} for index in range(5000)})
        files = self.fixture()
        count = len(manifest)
        size = sum(item["size"] for item in manifest.values())
        files["outcome.json"] = {
            "completed": True,
            "profile": "ci+webui",
            "files": count,
            "bytes": size,
            "gc_passes_after_flush": 1,
        }
        files["verified-full.json"] = {"sha256": "all matched", "files": count, "bytes": size}
        files["manifest.json"] = manifest
        return files

    def assert_rejected(self, files, reason):
        for optimized in (False, True):
            with self.subTest(reason=reason, optimized=optimized):
                result = self.run_cell(files, optimized)
                self.assertNotEqual(result.returncode, 0, result.stdout)
                self.assertEqual(result.stdout, "")

    def test_completed_evidence_produces_the_same_policy_under_optimization(self):
        for optimized in (False, True):
            with self.subTest(optimized=optimized):
                result = self.run_cell(self.fixture(), optimized)
                self.assertEqual(result.returncode, 0, result.stderr)
                policy = json.loads(result.stdout)
                self.assertEqual(policy["unmodeled_residency_reserve_bytes"], 3_623_878_656)
                self.assertEqual(policy["engine_headroom_bytes"], 671_088_640)
                self.assertEqual(policy["required_bytes"], 6_543_114_240)

    def test_incomplete_or_unqualified_workloads_are_rejected(self):
        original = self.fixture()
        for field, value in (
            ("completed", False),
            ("completed", "false"),
            ("completed", 1),
            ("build_profile", "debug"),
        ):
            files = copy.deepcopy(original)
            files["outcome.json"][field] = value
            self.assert_rejected(files, field)
        for field, value in (
            ("sha256", "mismatch"),
            ("sequential_bytes", 0),
            ("small_files", 0),
            ("gc_passes_after_flush", 0),
            ("gc_passes_after_flush", True),
        ):
            files = copy.deepcopy(original)
            files["outcome.json"]["companion"][field] = value
            self.assert_rejected(files, field)

    def test_pressure_and_missing_observations_are_rejected(self):
        original = self.fixture()
        for field, value in (
            ("swap_current_bytes", 4096),
            ("events", {"oom_kill": 1}),
            ("events", {"oom_kill": False}),
            ("metrics", {}),
            ("metrics", {"zerofs_jemalloc_resident_bytes": -1}),
            ("metrics", {"zerofs_jemalloc_resident_bytes": 1.5}),
            ("process_status", {}),
            ("process_status", {"VmHWM": "3000000 MB"}),
            ("stat", {"sock": 0}),
            ("stat", {"kernel": -1, "sock": 0}),
        ):
            files = copy.deepcopy(original)
            files["samples.jsonl"][field] = value
            self.assert_rejected(files, field)
        files = copy.deepcopy(original)
        del files["outcome.json"]["completed"]
        self.assert_rejected(files, "missing completion flag")

    def test_native_verification_and_manifest_must_match(self):
        files = self.native_fixture()
        for optimized in (False, True):
            result = self.run_cell(files, optimized)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(json.loads(result.stdout)["required_bytes"], 6_543_114_240)
        for name, field, value in (
            ("verified-full.json", "sha256", "mismatch"),
            ("verified-full.json", "files", 2),
            ("verified-full.json", "bytes", 8192),
            ("outcome.json", "gc_passes_after_flush", 0),
        ):
            changed = copy.deepcopy(files)
            changed[name][field] = value
            self.assert_rejected(changed, f"{name}:{field}")
        files["manifest.json"] = {}
        self.assert_rejected(files, "empty manifest")

    def test_native_workloads_must_cover_the_smallest_qualified_shape(self):
        manifests = {
            "empty": {},
            "tiny": {"file": {"size": 4096}},
            "bulk only": {"sequential": {"size": 12 * 1024**3 + 5000 * 1024}},
            "file count only": {f"small-{index}": {"size": 1024} for index in range(5001)},
        }
        for size in (0, 512):
            manifest = {"sequential": {"size": 12 * 1024**3 + 5000 * 1024}}
            manifest.update({f"small-{index}": {"size": size} for index in range(5000)})
            manifests[f"small files of {size} bytes"] = manifest
        below_sequential = {"sequential": {"size": 12 * 1024**3 - 1}}
        below_sequential.update({f"small-{index}": {"size": 2048} for index in range(5000)})
        manifests["sequential file below minimum"] = below_sequential
        for reason, manifest in manifests.items():
            self.assert_rejected(self.native_fixture(manifest), reason)

    def test_larger_native_ladder_rungs_remain_accepted(self):
        for sequential, count, small_size in (
            (50 * 1024**3, 50_000, 1024),
            (12 * 1024**3, 5000, 2048),
        ):
            manifest = {"sequential": {"size": sequential}}
            manifest.update({f"small-{index}": {"size": small_size} for index in range(count)})
            for optimized in (False, True):
                with self.subTest(sequential=sequential, count=count, small_size=small_size, optimized=optimized):
                    result = self.run_cell(self.native_fixture(manifest), optimized)
                    self.assertEqual(result.returncode, 0, result.stderr)
                    self.assertEqual(json.loads(result.stdout)["required_bytes"], 6_543_114_240)


if __name__ == "__main__":
    unittest.main()
