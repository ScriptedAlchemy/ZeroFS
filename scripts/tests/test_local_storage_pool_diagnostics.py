"""Exercise smoke-runner diagnostics without starting ZeroFS or mounting storage."""

import contextlib
import copy
from datetime import datetime
import hashlib
import importlib.util
import io
import json
import os
from pathlib import Path
import re
import signal
import subprocess
import sys
import tempfile
import textwrap
import unittest
from unittest import mock


SCRIPT = Path(__file__).resolve().parents[1] / "test-local-storage-pool.py"
SPEC = importlib.util.spec_from_file_location("local_storage_pool_smoke", SCRIPT)
SMOKE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(SMOKE)


class LocalStoragePoolDiagnosticsTests(unittest.TestCase):
    def run_main(self, *, health=False, receipt=None):
        arguments = [str(SCRIPT), "--binary", sys.executable]
        if health:
            arguments.append("--health-recovery")
        if receipt is not None:
            arguments.extend(("--receipt", str(receipt)))
        stdout, stderr = io.StringIO(), io.StringIO()
        with mock.patch.object(sys, "argv", arguments), \
                contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
            result = SMOKE.main()
        return result, stdout.getvalue(), stderr.getvalue()

    def test_nested_loss_failures_print_logs_before_cleanup(self):
        for failed_scenario in ("loss", "full"):
            with self.subTest(scenario=failed_scenario):
                roots = []

                def health(_binary, root):
                    roots.append(root)
                    (root / "server-health.log").write_text("health diagnostic", encoding="utf-8")
                    return {"result": "passed"}

                def loss(_binary, root, no_space=False):
                    root.mkdir()
                    (root / "server-loss-recovery.log").write_text(
                        f"{root.name} diagnostic", encoding="utf-8"
                    )
                    if root.name == failed_scenario:
                        raise SMOKE.SmokeFailure(f"{failed_scenario} failed")
                    return {}

                with mock.patch.object(SMOKE, "health_smoke", side_effect=health), \
                        mock.patch.object(SMOKE, "loss_smoke", side_effect=loss):
                    result, stdout, stderr = self.run_main(health=True)
                self.assertEqual(result, 1)
                self.assertEqual(stdout, "")
                self.assertIn("server-health.log:\nhealth diagnostic", stderr)
                for scenario in (("loss",) if failed_scenario == "loss" else ("loss", "full")):
                    label = Path(scenario) / "server-loss-recovery.log"
                    self.assertIn(f"{label}:\n{scenario} diagnostic", stderr)
                self.assertIn(f"local storage pool smoke test failed: {failed_scenario} failed", stderr)
                self.assertFalse(roots[0].exists(), "owned temporary storage must still be cleaned up")

    def test_top_level_failure_keeps_bounded_decodable_tail(self):
        roots = []

        def fail(_binary, root):
            roots.append(root)
            (root / "server-before-migration.log").write_bytes(
                b"discarded-prefix" + b"x" * 6000 + b"\xffdiagnostic-tail"
            )
            raise SMOKE.SmokeFailure("original failure")

        with mock.patch.object(SMOKE, "smoke", side_effect=fail):
            result, stdout, stderr = self.run_main()
        self.assertEqual(result, 1)
        self.assertEqual(stdout, "")
        self.assertIn("server-before-migration.log:\n", stderr)
        self.assertIn("\ufffddiagnostic-tail", stderr)
        self.assertNotIn("discarded-prefix", stderr)
        self.assertIn("local storage pool smoke test failed: original failure", stderr)
        self.assertLess(len(stderr), 6200)
        self.assertFalse(roots[0].exists())

    def test_success_keeps_receipt_without_failure_diagnostics(self):
        with mock.patch.object(SMOKE, "smoke", return_value={"result": "passed"}):
            result, stdout, stderr = self.run_main()
        self.assertEqual(result, 0)
        receipt = json.loads(stdout.removeprefix("LOCAL_STORAGE_POOL_SMOKE "))
        self.assertEqual(receipt["result"], "passed")
        self.assertEqual(receipt["mode"], "local_pool")
        self.assertEqual(receipt["binary_sha256"], hashlib.sha256(Path(sys.executable).read_bytes()).hexdigest())
        self.assertLessEqual(datetime.fromisoformat(receipt["started_at"]),
                             datetime.fromisoformat(receipt["completed_at"]))
        self.assertEqual(stderr, "")

    def test_failed_rerun_replaces_stale_success_receipt(self):
        with tempfile.TemporaryDirectory() as directory:
            receipt = Path(directory) / "result.json"
            receipt.write_text('{"result":"passed"}')
            with mock.patch.object(SMOKE, "smoke", side_effect=SMOKE.SmokeFailure("copy failed")):
                result, stdout, stderr = self.run_main(receipt=receipt)
            evidence = json.loads(receipt.read_text())
            self.assertEqual((result, stdout, evidence["result"]), (1, "", "failed"))
            self.assertEqual(evidence["error"], "copy failed")
            self.assertIn("binary_sha256", evidence)
            self.assertIn("copy failed", stderr)
            self.assertEqual(list(Path(directory).iterdir()), [receipt])

    def test_changed_binary_cannot_receive_a_passing_receipt(self):
        with tempfile.TemporaryDirectory() as directory:
            receipt = Path(directory) / "nested" / "result.json"
            with mock.patch.object(SMOKE, "smoke", return_value={"result": "passed"}), \
                    mock.patch.object(SMOKE, "file_sha256", side_effect=["before", "after"]):
                result, stdout, stderr = self.run_main(receipt=receipt)
            evidence = json.loads(receipt.read_text())
            self.assertEqual((result, stdout, evidence["result"]), (1, "", "failed"))
            self.assertEqual(evidence["binary_sha256"], "before")
            self.assertIn("binary changed", evidence["error"])
            self.assertIn("binary changed", stderr)

    def test_cancellation_replaces_stale_receipt_and_restores_signal_handler(self):
        with tempfile.TemporaryDirectory() as directory:
            receipt = Path(directory) / "result.json"
            receipt.write_text('{"result":"passed"}')
            previous = signal.getsignal(signal.SIGTERM)
            with mock.patch.object(SMOKE, "smoke", side_effect=lambda *_: os.kill(os.getpid(), signal.SIGTERM)):
                result, stdout, stderr = self.run_main(receipt=receipt)
            evidence = json.loads(receipt.read_text())
            self.assertEqual((result, stdout, evidence["result"]), (143, "", "failed"))
            self.assertEqual(evidence["error"], "cancelled by SIGTERM")
            self.assertIn("cancelled by SIGTERM", stderr)
            self.assertEqual(signal.getsignal(signal.SIGTERM), previous)

    def test_unreadable_log_preserves_original_failure_and_remaining_logs(self):
        read_text = Path.read_text
        for read_error in (PermissionError("log unreadable"), FileNotFoundError("log disappeared")):
            with self.subTest(error=type(read_error).__name__):
                roots = []

                def fail(_binary, root):
                    roots.append(root)
                    (root / "loss").mkdir()
                    (root / "loss" / "server-loss-recovery.log").write_text(
                        "unavailable diagnostic", encoding="utf-8"
                    )
                    (root / "server-health.log").write_text("remaining diagnostic", encoding="utf-8")
                    raise SMOKE.SmokeFailure("original failure")

                def read_log(path, *args, **kwargs):
                    if path.name == "server-loss-recovery.log":
                        raise read_error
                    return read_text(path, *args, **kwargs)

                with mock.patch.object(SMOKE, "smoke", side_effect=fail), \
                        mock.patch.object(Path, "read_text", read_log):
                    result, stdout, stderr = self.run_main()
                self.assertEqual(result, 1)
                self.assertEqual(stdout, "")
                self.assertIn(f"{Path('loss') / 'server-loss-recovery.log'}:\n", stderr)
                self.assertIn(f"unable to read log: {read_error}", stderr)
                self.assertIn("server-health.log:\nremaining diagnostic", stderr)
                self.assertIn("local storage pool smoke test failed: original failure", stderr)
                self.assertFalse(roots[0].exists())


class LocalStoragePoolLossRestartTests(unittest.TestCase):
    def statuses(self, no_space):
        placements = [{"location": f"metadata/segments/{index:02x}/0000000000000001/{index:016x}",
                       "member_id": "disk1", "size": size} for index, size in ((1, 40), (2, 60))]
        members = [{"id": member, "objects": objects, "used_bytes": used, "capacity_bytes": capacity,
                    "reserved_bytes": 0, "pending_uploads": 0, "cleanup_debts": 0,
                    "health": {"quarantine": None}, "safe_to_remove": False,
                    "safe_to_pull_with_known_loss": False, "readable_evacuated": False,
                    "unreadable_objects": 0, "evacuation": "not_quarantined"}
                   for member, objects, used, capacity in (("disk1", 2, 100, 1000),
                                                          ("disk2", 0, 0, 8 if no_space else 1000))]
        original = {"placements": placements,
                    "status": {"members": members, "mapped_objects": 2, "pending_operations": 0,
                               "unreadable": [], "quarantined_leftovers": []}}
        settled = copy.deepcopy(original["status"])
        settled["unreadable"] = [{"location": placements[0]["location"], "member_id": "disk1",
                                  "physical_key": ".zerofs-pool/owned/objects/1"}]
        settled["quarantined_leftovers"] = copy.deepcopy(placements if no_space else placements[:1])
        settled["members"][0].update(
            objects=2 if no_space else 1, used_bytes=100 if no_space else 40,
            health={"quarantine": "pending sectors"}, unreadable_objects=1,
            readable_evacuated=not no_space, safe_to_pull_with_known_loss=not no_space,
            evacuation="blocked_capacity_or_admission" if no_space else "readable_evacuated_loss_blocked")
        if not no_space:
            settled["members"][1].update(objects=1, used_bytes=60)
        return original, settled

    def run_loss(self, no_space, phase=None, change=None):
        original, settled = self.statuses(no_space)
        restarted, final = copy.deepcopy(settled), copy.deepcopy(settled)
        if change:
            target = restarted if phase == "restart" else final
            path, value = change
            for part in path[:-1]:
                target = target[part]
            target[path[-1]] = value
        recovered = copy.deepcopy(original["placements"])
        if not no_space:
            recovered[1]["member_id"] = "disk2"
        active, clock = [None], [0]

        @contextlib.contextmanager
        def server(_binary, _config, _root, name, deadline, _events=None):
            self.assertEqual(deadline, SMOKE.TOTAL_TIMEOUT_SECONDS)
            active[0] = name
            yield mock.Mock(target="owned-9p")

        def monotonic():
            if active[0] == "server-loss-restart":
                clock[0] += SMOKE.TOTAL_TIMEOUT_SECONDS + 1
            return clock[0]

        def query(_binary, _deadline, *arguments):
            action = arguments[:2]
            if action == ("pool", "init"):
                return {}
            if action == ("pool", "list"):
                return copy.deepcopy(original["placements"] if active[0] == "server-loss-upload" else recovered)
            if action == ("pool", "status"):
                if "--endpoint" in arguments:
                    return copy.deepcopy(restarted if active[0] == "server-loss-restart" else settled)
                return copy.deepcopy(original["status"] if active[0] == "server-loss-upload" else final)
            self.assertEqual(action, ("pool", "affected"))
            return {"complete": True, "views": [{"view": "checkpoint/before-loss", "references": [
                {"inode": 1, "paths": [list(b"/first.bin")],
                 "segment": original["placements"][0]["location"].removeprefix("metadata/")}
            ]}]}

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "loss"

            def command(_binary, _deadline, *arguments):
                if arguments[0] == "upload":
                    index = 1 if Path(arguments[2]).name == "first.bin" else 2
                    body = root / "disk1" / ".zerofs-pool/owned/objects" / str(index)
                    body.parent.mkdir(parents=True, exist_ok=True)
                    body.write_bytes(b"x" * original["placements"][index - 1]["size"])

            with mock.patch.object(SMOKE, "Server", side_effect=server), \
                    mock.patch.object(SMOKE, "command", side_effect=command), \
                    mock.patch.object(SMOKE, "command_json", side_effect=query), \
                    mock.patch.object(SMOKE, "download_and_verify"), \
                    mock.patch.object(SMOKE.subprocess, "run", return_value=mock.Mock(returncode=1)), \
                    mock.patch.object(SMOKE.time, "monotonic", side_effect=monotonic), \
                    mock.patch.object(SMOKE.time, "sleep"):
                return SMOKE.loss_smoke(Path("unused"), root, no_space=no_space)

    def test_loss_receipt_records_validated_restart_and_final_status(self):
        for no_space in (False, True):
            with self.subTest(no_space=no_space):
                _, expected = self.statuses(no_space)
                case = "partial_loss_no_space" if no_space else "partial_loss"
                evidence = self.run_loss(no_space)[case + "_evidence"]
                self.assertEqual(evidence["settled_status"], expected)
                self.assertEqual(evidence["restart_status"], expected)
                self.assertEqual(evidence["final_status"], expected)

    def test_restart_and_final_status_cannot_pass_with_regressed_loss_readiness(self):
        for no_space in (False, True):
            changes = [(("members", 0, "safe_to_pull_with_known_loss"), no_space),
                       (("members", 0, "readable_evacuated"), no_space),
                       (("members", 0, "evacuation"), "incomplete"),
                       (("members", 0, "used_bytes"), 101 if no_space else 41)]
            for phase in ("restart", "final"):
                for change in changes:
                    with self.subTest(no_space=no_space, phase=phase, change=change):
                        # Unreadable evidence and safe_to_remove=false survive
                        # these mutations, so the former restart check passed.
                        with self.assertRaisesRegex(SMOKE.SmokeFailure, phase):
                            self.run_loss(no_space, phase, change)

    def test_restart_and_final_status_require_exact_loss_identity_and_leftovers(self):
        for no_space in (False, True):
            changes = [(("unreadable", 0, "physical_key"), "another-body"),
                       (("unreadable", 0, "location"), "another-segment"),
                       (("quarantined_leftovers", 0, "size"), 41),
                       (("quarantined_leftovers", 0, "member_id"), "disk2"),
                       (("quarantined_leftovers",), [])]
            if no_space:
                changes += [(("members", 1, "capacity_bytes"), 9), (("members", 1, "used_bytes"), 1)]
            for phase in ("restart", "final"):
                for change in changes:
                    with self.subTest(no_space=no_space, phase=phase, change=change):
                        with self.assertRaisesRegex(SMOKE.SmokeFailure, phase):
                            self.run_loss(no_space, phase, change)


class LocalStoragePoolWorkflowTests(unittest.TestCase):
    def test_ci_logging_preserves_acceptance_failures_and_receipts(self):
        workflow = (SCRIPT.parents[1] / ".github/workflows/rust.yml").read_text()
        for name in ("Exercise local storage pooling over 9P", "Exercise disk-health recovery over 9P"):
            match = re.search(r"      - name: " + re.escape(name) + r"\n"
                              r"(?:        [^\n]*\n)*?        run: \|\n((?:          [^\n]*\n|\n)+)", workflow)
            self.assertIsNotNone(match, name)
            step = textwrap.dedent(match.group(1))
            for exit_code in (0, 7):
                with self.subTest(step=name, exit_code=exit_code), tempfile.TemporaryDirectory() as directory:
                    root = Path(directory)
                    (root / "target").mkdir()
                    python = root / "python3"
                    python.write_text(
                        f"#!{sys.executable}\nimport json, os, sys\nfrom pathlib import Path\n"
                        "receipt = Path(sys.argv[sys.argv.index('--receipt') + 1])\n"
                        "receipt.write_text(json.dumps({'exit_code': int(os.environ['FIXTURE_EXIT'])}))\n"
                        "print('owned acceptance output')\n"
                        "raise SystemExit(int(os.environ['FIXTURE_EXIT']))\n"
                    )
                    python.chmod(0o700)
                    environment = dict(os.environ, PATH=f"{root}:{os.environ['PATH']}", FIXTURE_EXIT=str(exit_code))
                    result = subprocess.run(["bash", "--noprofile", "--norc", "-e", "-c", step],
                                            cwd=root, env=environment, capture_output=True, text=True, timeout=10)
                    self.assertEqual(result.returncode, exit_code, result.stderr)
                    receipts = list((root / "target").glob("*.json"))
                    logs = list((root / "target").glob("*.log"))
                    self.assertEqual(len(receipts), 1)
                    self.assertEqual(len(logs), 1)
                    self.assertEqual(json.loads(receipts[0].read_text())["exit_code"], exit_code)
                    self.assertIn("owned acceptance output", logs[0].read_text())


if __name__ == "__main__":
    unittest.main()
