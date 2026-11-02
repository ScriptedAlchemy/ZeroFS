import importlib.util
import json
from pathlib import Path
import tempfile
import unittest
from unittest import mock

MODULE = Path(__file__).resolve().parents[1] / "nfs_ci_diagnostics.py"
spec = importlib.util.spec_from_file_location("nfs_ci_diagnostics", MODULE)
diagnostics = importlib.util.module_from_spec(spec)
spec.loader.exec_module(diagnostics)


class NfsDiagnosticsTests(unittest.TestCase):
    def test_reads_are_bounded_and_absent_files_are_recorded(self):
        with tempfile.TemporaryDirectory() as root:
            path = Path(root) / "data"
            path.write_bytes(b"x" * 100)
            self.assertEqual(diagnostics.read_bounded(path, 8),
                             {"text": "xxxxxxxx", "truncated": True})
            self.assertIn("error", diagnostics.read_bounded(path.parent / "gone", 8))

    def test_permission_failure_does_not_abort_snapshot(self):
        with mock.patch.object(Path, "open", side_effect=PermissionError("restricted")):
            self.assertEqual(diagnostics.read_bounded(Path("stack"), 8),
                             {"error": "restricted"})

    def test_collects_only_relevant_processes_and_preserves_counters(self):
        with tempfile.TemporaryDirectory() as root:
            proc = Path(root)
            (proc / "net/rpc").mkdir(parents=True)
            (proc / "net/rpc/nfs").write_text("rpc 42 1 0\n")
            for pid, comm in ((101, "tar"), (102, "zerofs"), (103, "unrelated")):
                task = proc / str(pid)
                task.mkdir()
                (task / "comm").write_text(comm + "\n")
                (task / "wchan").write_text("wait_on_commit")
            thread = proc / "102/task/104"
            thread.mkdir(parents=True)
            (thread / "stack").write_text("kernel_frame\n")
            value = diagnostics.snapshot(proc)
            self.assertEqual({p["pid"] for p in value["processes"]}, {101, 102})
            self.assertEqual(value["counters"]["net/rpc/nfs"]["text"], "rpc 42 1 0\n")
            server = next(p for p in value["processes"] if p["comm"] == "zerofs")
            self.assertEqual(server["threads"][0]["stack"]["text"], "kernel_frame\n")
            self.assertIn("error", server["files"]["syscall"])

    def test_process_and_thread_counts_are_bounded(self):
        with tempfile.TemporaryDirectory() as root:
            proc = Path(root)
            for pid in range(100, 112):
                task = proc / str(pid)
                (task / "task").mkdir(parents=True)
                (task / "comm").write_text("zerofs\n")
                for tid in range(200, 212):
                    (task / "task" / str(tid)).mkdir()
            value = diagnostics.snapshot(proc)
            self.assertEqual(len(value["processes"]), diagnostics.MAX_PROCESSES)
            self.assertTrue(all(len(p["threads"]) == diagnostics.MAX_THREADS
                                for p in value["processes"]))

    def test_rotates_two_bounded_parseable_logs(self):
        with tempfile.TemporaryDirectory() as root:
            output = Path(root) / "snapshots.jsonl"
            for index in range(20):
                diagnostics.append_snapshot(output, {"sequence": index}, limit=40)
            for path in (output, output.with_name(output.name + ".1")):
                self.assertLessEqual(path.stat().st_size, 40)
                for line in path.read_text().splitlines():
                    self.assertIn("sequence", json.loads(line))
            self.assertEqual(json.loads(output.read_text().splitlines()[-1])["sequence"], 19)
            self.assertEqual(len(list(Path(root).glob("*.jsonl*"))), 3)  # log, previous, lock

    def test_oversize_snapshot_does_not_discard_previous_evidence(self):
        with tempfile.TemporaryDirectory() as root:
            output = Path(root) / "snapshots.jsonl"
            diagnostics.append_snapshot(output, {"ok": True}, limit=40)
            original = output.read_bytes()
            with self.assertRaises(ValueError):
                diagnostics.append_snapshot(output, {"bad": "x" * 100}, limit=40)
            self.assertEqual(output.read_bytes(), original)


if __name__ == "__main__":
    unittest.main()
