"""Exercise smoke-runner diagnostics without starting ZeroFS or mounting storage."""

import contextlib
import copy
from datetime import datetime
import fcntl
import hashlib
import importlib.util
import io
import json
from pathlib import Path
import struct
import sys
import tempfile
import time
import unittest
from unittest import mock


SCRIPT = Path(__file__).resolve().parents[1] / "test-local-storage-pool.py"
SPEC = importlib.util.spec_from_file_location("local_storage_pool_smoke", SCRIPT)
SMOKE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(SMOKE)


class LocalStoragePoolManifestTests(unittest.TestCase):
    POOL_ID = "00000000-0000-4000-8000-000000000001"

    @staticmethod
    def segment_bytes(epoch=8, counter=1, payload=b"frame bytes"):
        # Synthetic frame/directory bytes with the production v1 footer layout.
        # These fixtures test identity and bounds, not encrypted-frame decoding.
        directory = b"sealed directory"
        footer = bytearray(64)
        footer[:4] = b"ZSEG"
        for offset, encoding, value in (
            (4, "<I", 1), (8, "<I", 1),
            (12, "<Q", len(payload)), (20, "<I", len(directory)),
            (24, "<Q", epoch), (32, "<Q", counter), (40, "<Q", 42),
            (48, "<Q", len(payload) + len(directory) + 64),
        ):
            struct.pack_into(encoding, footer, offset, value)
        return payload + directory + footer

    def write_body(self, root, member, slot, *, counter, payload=b"frame bytes"):
        key = f".zerofs-pool/{self.POOL_ID}/objects/{slot:08x}-0000-4000-8000-000000000000"
        body = root / member / key
        body.parent.mkdir(parents=True, exist_ok=True)
        data = self.segment_bytes(counter=counter, payload=payload)
        body.write_bytes(data)
        segment_id = f"segments/{counter & 0xff:02x}/0000000000000008/{counter:016x}"
        return {"segment_id": segment_id, "location": f"metadata/{segment_id}",
                "member_id": member, "physical_key": key,
                "size": len(data), "sha256": hashlib.sha256(data).hexdigest()}

    @staticmethod
    def api_placements(manifest):
        return [{**{key: value for key, value in entry.items() if key != "segment_id"},
                 "physical_state": "present"} for entry in manifest]

    def test_live_manifest_checks_the_fresh_segment_identity_not_only_physical_bytes(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            expected = [self.write_body(root, "disk1", 1, counter=1, payload=b"original"),
                        self.write_body(root, "disk2", 2, counter=257, payload=b"fresh write")]
            status = {"pool_id": self.POOL_ID, "members": [{"id": "disk1"}, {"id": "disk2"}]}
            physical = SMOKE.physical_snapshot(root, status, "metadata")
            self.assertEqual(physical, expected)
            api = self.api_placements(expected)
            self.assertEqual(SMOKE.verify_physical_manifest(physical, api, "restart"), expected)

            swapped = self.api_placements(expected)
            swapped[0]["location"], swapped[1]["location"] = swapped[1]["location"], swapped[0]["location"]
            # The physical body census and SHA values still match exactly; only
            # their logical association changed. That must fail the crash gate.
            with self.assertRaisesRegex(SMOKE.SmokeFailure, "exact segment identity"):
                SMOKE.verify_physical_manifest(physical, swapped, "restart")
            with self.assertRaisesRegex(SMOKE.SmokeFailure, "exact segment identity"):
                SMOKE.verify_physical_manifest(physical, api[:1], "restart")
            for field, value in (("member_id", "disk1"), ("physical_key", "wrong-key"),
                                 ("size", expected[1]["size"] + 1), ("sha256", "0" * 64)):
                with self.subTest(field=field):
                    changed = self.api_placements(expected)
                    changed[1][field] = value
                    with self.assertRaisesRegex(SMOKE.SmokeFailure, "exact segment identity"):
                        SMOKE.verify_physical_manifest(physical, changed, "cold restart")

    def test_live_manifest_rejects_duplicate_segment_identity_on_different_roots(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self.write_body(root, "disk1", 1, counter=1)
            self.write_body(root, "disk2", 2, counter=1, payload=b"another body")
            status = {"pool_id": self.POOL_ID, "members": [{"id": "disk1"}, {"id": "disk2"}]}
            with self.assertRaisesRegex(SMOKE.SmokeFailure, "repeat a logical segment identity"):
                SMOKE.physical_snapshot(root, status, "metadata")

    def test_footer_identity_preserves_all_epoch_and_counter_bits(self):
        data = self.segment_bytes(epoch=0x0123456789abcdef, counter=0xfedcba9876543210)
        with tempfile.TemporaryDirectory() as directory:
            body = Path(directory) / "body"
            body.write_bytes(data)
            self.assertEqual(SMOKE.segment_footer_identity(body, len(data)),
                             "segments/10/0123456789abcdef/fedcba9876543210")

    def test_segment_footer_rejects_malformed_identity_bounds(self):
        original = self.segment_bytes()
        mutations = (
            ("magic", 0, b"BAD!"),
            ("version", 4, struct.pack("<I", 2)),
            ("length", 48, struct.pack("<Q", len(original) + 1)),
            ("reserved", 60, struct.pack("<I", 1)),
            ("directory", 12, struct.pack("<Q", 0)),
            ("directory", 20, struct.pack("<I", 0xffffffff)),
        )
        with tempfile.TemporaryDirectory() as directory:
            body = Path(directory) / "body"
            for label, offset, replacement in mutations:
                with self.subTest(field=label, offset=offset):
                    malformed = bytearray(original)
                    begin = len(original) - 64 + offset
                    malformed[begin:begin + len(replacement)] = replacement
                    body.write_bytes(malformed)
                    with self.assertRaisesRegex(SMOKE.SmokeFailure, label):
                        SMOKE.segment_footer_identity(body, len(malformed))
            body.write_bytes(original[:63])
            with self.assertRaisesRegex(SMOKE.SmokeFailure, "smaller than its footer"):
                SMOKE.segment_footer_identity(body, 63)

    def test_api_manifest_rejects_noncanonical_segment_locations(self):
        with tempfile.TemporaryDirectory() as directory:
            manifest = [self.write_body(Path(directory), "disk1", 1, counter=1)]
            canonical = manifest[0]["location"]
            for location in (
                canonical.replace("segments/01/", "segments/ff/"),
                canonical.replace("0000000000000008", "000000000000000A"),
                canonical.replace("0000000000000001", "1"),
                canonical + "/extra", canonical + "/", "ordinary",
            ):
                with self.subTest(location=location):
                    api = self.api_placements(manifest)
                    api[0]["location"] = location
                    with self.assertRaises(SMOKE.SmokeFailure):
                        SMOKE.verify_physical_manifest(manifest, api, "restart")

    def test_api_manifest_rejects_duplicate_segment_identity(self):
        with tempfile.TemporaryDirectory() as directory:
            manifest = [self.write_body(Path(directory), "disk1", 1, counter=1)]
            api = self.api_placements(manifest)
            duplicate = {**api[0], "member_id": "disk2", "physical_key": "different-body"}
            with self.assertRaisesRegex(SMOKE.SmokeFailure, "API map repeats"):
                SMOKE.verify_physical_manifest(manifest, [api[0], duplicate], "restart")


class LocalStoragePoolRunningStatusTests(unittest.TestCase):
    POOL_ID = LocalStoragePoolManifestTests.POOL_ID

    def lock_path(self, root):
        return root / "disk2" / ".zerofs-pool" / self.POOL_ID / "objects" / SMOKE.NATIVE_MUTATION_LOCK_NAME

    def assert_lock_held(self, path):
        with path.open("a+b") as probe:
            with self.assertRaises(BlockingIOError):
                fcntl.flock(probe.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)

    def assert_lock_released(self, path):
        with path.open("a+b") as probe:
            fcntl.flock(probe.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)

    def statuses(self):
        before = {"pool_id": self.POOL_ID, "mapped_objects": 2, "pending_operations": 0,
                  "members": [{"id": member, "objects": objects, "used_bytes": size,
                               "reserved_bytes": 0, "pending_uploads": 0,
                               "health": {"quarantine": None}, "evacuation": "not_quarantined",
                               "safe_to_remove": False, "safe_to_pull_with_known_loss": False}
                              for member, objects, size in (("disk1", 2, 64), ("disk2", 0, 0))]}
        running = copy.deepcopy(before)
        running["pending_operations"] = 1
        running["members"][0].update(health={"quarantine": "fixture"}, evacuation="running")
        running["members"][1].update(reserved_bytes=32, pending_uploads=1)
        return before, running

    def test_live_status_samples_hold_exact_destination_lock_before_signal_and_release_afterward(self):
        for changes in ({"pending": 1}, {"crc": 2}):
            with self.subTest(changes=changes), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                before, running = self.statuses()
                deadline = time.monotonic() + 60
                queries = []
                inject = SMOKE.inject_smart

                def signal(events, **observations):
                    self.assert_lock_held(self.lock_path(root))
                    self.assertEqual(events, root / "health.json")
                    self.assertEqual(observations, changes)
                    return inject(events, **observations)

                def query(binary, query_deadline, *arguments):
                    self.assert_lock_held(self.lock_path(root))
                    self.assertEqual(arguments, ("pool", "status", "--endpoint", "owned-admin"))
                    self.assertLessEqual(query_deadline, deadline)
                    if len(queries) >= 2:
                        self.assertLessEqual(query_deadline, time.monotonic() + 5)
                    queries.append(query_deadline)
                    return copy.deepcopy(running)

                with mock.patch.object(SMOKE, "inject_smart", side_effect=signal), \
                        mock.patch.object(SMOKE, "command_json", side_effect=query), \
                        mock.patch.object(SMOKE.time, "sleep"):
                    evidence = SMOKE.observe_automatic_evacuation(
                        Path("unused"), deadline, "owned-admin", root, before, changes)
                self.assert_lock_released(self.lock_path(root))
                self.assertEqual(len(queries), 5)
                self.assertEqual(evidence["running"], running)
                pause = evidence["paused_copy"]
                self.assertEqual(pause["member_id"], "disk2")
                self.assertEqual(pause["physical_key"],
                                 self.lock_path(root).relative_to(root / "disk2").as_posix())
                self.assertEqual(pause["status_request_timeout_seconds"], 5)
                self.assertGreaterEqual(pause["held_seconds"], pause["signal_to_running_seconds"])
                self.assertEqual(len(pause["status_samples"]), 3)
                for sample in pause["status_samples"]:
                    self.assertEqual(sample["status"], running)
                    self.assertGreaterEqual(sample["request_seconds"], 0)
                    self.assertGreaterEqual(sample["started_after_signal_seconds"],
                                            pause["signal_to_running_seconds"])
                self.assertEqual(list((root / "disk2").rglob(SMOKE.NATIVE_MUTATION_LOCK_NAME)),
                                 [self.lock_path(root)])
                self.assertFalse((root / "disk1").exists(), "the pause must not lock the source")

    def test_live_status_failure_releases_lock_before_outer_server_cleanup(self):
        for failure in (SMOKE.SmokeFailure("admin status timed out"), KeyboardInterrupt()):
            with self.subTest(failure=type(failure).__name__), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                before, running = self.statuses()
                replies = iter((running, running, failure))

                def query(*_arguments):
                    self.assert_lock_held(self.lock_path(root))
                    reply = next(replies)
                    if isinstance(reply, BaseException):
                        raise reply
                    return copy.deepcopy(reply)

                @contextlib.contextmanager
                def server_cleanup():
                    try:
                        yield
                    finally:
                        # A native writer must be able to resume before the
                        # outer Server context attempts to settle/shut down.
                        self.assert_lock_released(self.lock_path(root))

                with self.assertRaises(type(failure)) as raised, server_cleanup(), \
                        mock.patch.object(SMOKE, "command_json", side_effect=query):
                    SMOKE.observe_automatic_evacuation(
                        Path("unused"), time.monotonic() + 60, "owned-admin", root, before, {"pending": 1})
                self.assertIs(raised.exception, failure)

    def test_paused_copy_cannot_claim_completion_publication_or_pull_ready(self):
        mutations = (
            (0, "evacuation", "complete"), (1, "pending_uploads", 0),
            (1, "reserved_bytes", 0), (1, "objects", 1),
            (0, "safe_to_remove", True), (0, "safe_to_pull_with_known_loss", True),
        )
        for member, field, value in mutations:
            with self.subTest(field=field), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                before, running = self.statuses()
                changed = copy.deepcopy(running)
                changed["members"][member][field] = value
                with mock.patch.object(SMOKE, "command_json", side_effect=[running, running, changed]):
                    with self.assertRaises(SMOKE.SmokeFailure):
                        SMOKE.observe_automatic_evacuation(
                            Path("unused"), time.monotonic() + 60, "owned-admin", root, before, {"pending": 1})
                self.assert_lock_released(self.lock_path(root))


class LocalStoragePoolLossRestartTests(unittest.TestCase):
    def loss_case(self, no_space):
        placements = [
            {"location": f"metadata/segments/{index:02x}/0000000000000001/{index:016x}",
             "member_id": "disk1", "physical_key": f".zerofs-pool/owned/objects/{index}",
             "size": size, "sha256": str(index) * 64, "physical_state": "present"}
            for index, size in ((1, 40), (2, 60))
        ]
        members = [{"id": member, "objects": objects, "used_bytes": used,
                    "capacity_bytes": capacity, "reserved_bytes": 0,
                    "pending_uploads": 0, "cleanup_debts": 0,
                    "available_bytes": capacity - used, "health": {"quarantine": None},
                    "safe_to_remove": False, "safe_to_pull_with_known_loss": False,
                    "readable_evacuated": False, "unreadable_objects": 0,
                    "evacuation": "not_quarantined"}
                   for member, objects, used, capacity in (
                       ("disk1", 2, 100, 1000), ("disk2", 0, 0, 8 if no_space else 1000))]
        original = {"placements": placements,
                    "status": {"pool_id": "owned", "members": members, "mapped_objects": 2,
                               "pending_operations": 0, "unreadable": [], "quarantined_leftovers": []}}
        status = copy.deepcopy(original["status"])
        status["unreadable"] = [{key: placements[0][key]
                                  for key in ("location", "member_id", "physical_key")}]
        status["quarantined_leftovers"] = [
            {key: entry[key] for key in ("location", "member_id", "physical_key", "size")}
            for entry in (placements if no_space else placements[:1])]
        status["members"][0].update(
            objects=2 if no_space else 1, used_bytes=100 if no_space else 40,
            health={"quarantine": "pending sectors"}, unreadable_objects=1,
            readable_evacuated=not no_space, safe_to_pull_with_known_loss=not no_space,
            evacuation="cannot_complete_capacity" if no_space else "readable_evacuated_loss_blocked",
            evacuation_capacity={"required_bytes": 100 if no_space else 40,
                                 "available_bytes": 8 if no_space else 940,
                                 "reserved_destination_bytes": 0, "sufficient": not no_space})
        if not no_space:
            status["members"][1].update(objects=1, used_bytes=60, available_bytes=940)
        return original, status

    def changed(self, status, path, value):
        changed = copy.deepcopy(status)
        parent = changed
        for part in path[:-1]:
            parent = parent[part]
        parent[path[-1]] = value
        return changed

    def regressions(self, no_space):
        fields = [
            (("members", 0, "safe_to_pull_with_known_loss"), no_space),
            (("members", 0, "readable_evacuated"), no_space),
            (("members", 0, "evacuation"), "incomplete"),
            (("quarantined_leftovers", 0, "physical_key"), "different-generation"),
            (("members", 0, "used_bytes"), 101 if no_space else 41),
        ]
        if no_space:
            fields.append((("members", 0, "evacuation_capacity", "sufficient"), True))
        return fields

    def run_loss(self, no_space, *, restart_change=None, final_change=None):
        original, settled = self.loss_case(no_space)
        restarted = self.changed(settled, *restart_change) if restart_change else copy.deepcopy(settled)
        final_status = self.changed(settled, *final_change) if final_change else copy.deepcopy(settled)
        placements = copy.deepcopy(original["placements"])
        placements[0].update(physical_state="missing_quarantined", sha256=None)
        if not no_space:
            placements[1].update(member_id="disk2", physical_key=".zerofs-pool/owned/objects/moved")
        recovered = {"status": settled, "placements": placements}
        final = {"status": final_status, "placements": copy.deepcopy(placements)}
        phase, clock = [None], [0]

        @contextlib.contextmanager
        def server(_binary, _config, _root, name, deadline, _events=None):
            self.assertEqual(deadline, SMOKE.TOTAL_TIMEOUT_SECONDS)
            phase[0] = name
            yield mock.Mock(target="owned-9p")

        def monotonic():
            # An unsettled restart must fail the existing overall deadline;
            # tests never spend minutes polling a deliberately broken status.
            if phase[0] == "server-loss-restart":
                clock[0] += SMOKE.TOTAL_TIMEOUT_SECONDS + 1
            return clock[0]

        def query(_binary, _deadline, *arguments):
            if arguments[:2] == ("pool", "init"):
                return {}
            if arguments[:2] == ("pool", "status"):
                return copy.deepcopy(restarted if phase[0] == "server-loss-restart" else settled)
            self.assertEqual(arguments[:2], ("pool", "affected"))
            return {"complete": True, "views": [{"view": "checkpoint/before-loss", "references": [
                {"inode": 1, "paths": [list(b"/first.bin")],
                 "segment": original["placements"][0]["location"].removeprefix("metadata/")}
            ]}]}

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "loss"

            def command(_binary, _deadline, *arguments):
                if arguments[0] == "upload":
                    entry = original["placements"][0 if Path(arguments[2]).name == "first.bin" else 1]
                    body = root / "disk1" / entry["physical_key"]
                    body.parent.mkdir(parents=True, exist_ok=True)
                    body.write_bytes(b"x" * entry["size"])

            with mock.patch.object(SMOKE, "Server", side_effect=server), \
                    mock.patch.object(SMOKE, "command", side_effect=command), \
                    mock.patch.object(SMOKE, "command_json", side_effect=query), \
                    mock.patch.object(SMOKE, "placement_snapshot", side_effect=[original, recovered, final]), \
                    mock.patch.object(SMOKE, "download_and_verify"), \
                    mock.patch.object(SMOKE.subprocess, "run", return_value=mock.Mock(returncode=1)), \
                    mock.patch.object(SMOKE.time, "monotonic", side_effect=monotonic), \
                    mock.patch.object(SMOKE.time, "sleep"):
                return SMOKE.loss_smoke(Path("unused"), root, no_space=no_space)

    def test_loss_receipt_retains_verified_restart_and_final_states(self):
        for no_space in (False, True):
            with self.subTest(no_space=no_space):
                _, expected = self.loss_case(no_space)
                result = self.run_loss(no_space)
                evidence = result["fixtures"][0]
                self.assertEqual(evidence["settled_status"], expected)
                self.assertEqual(evidence["restart_status"], expected)
                self.assertEqual(evidence["after"]["status"], expected)
                if not no_space:
                    self.assertEqual(result["fixtures"][1]["status"], expected)

    def test_cold_restart_rejects_regressed_loss_state_with_unchanged_placements(self):
        for no_space in (False, True):
            for change in self.regressions(no_space):
                with self.subTest(no_space=no_space, change=change):
                    # Every mutation still passes the former restart check:
                    # at least one unreadable object and safe_to_remove=false.
                    _, status = self.loss_case(no_space)
                    changed = self.changed(status, *change)
                    self.assertTrue(changed["unreadable"])
                    self.assertFalse(changed["members"][0]["safe_to_remove"])
                    with self.assertRaisesRegex(SMOKE.SmokeFailure, "cold restart"):
                        self.run_loss(no_space, restart_change=change)

    def test_final_snapshot_rejects_regressed_state_after_a_valid_restart(self):
        for no_space in (False, True):
            for change in self.regressions(no_space):
                with self.subTest(no_space=no_space, change=change):
                    with self.assertRaisesRegex(SMOKE.SmokeFailure, "final snapshot"):
                        self.run_loss(no_space, final_change=change)

    def test_settled_loss_requires_exact_leftover_identity_and_bytes(self):
        for no_space in (False, True):
            original, status = self.loss_case(no_space)
            changes = [
                (("quarantined_leftovers", 0, field), value)
                for field, value in (("location", "different-segment"), ("member_id", "disk2"),
                                     ("physical_key", "different-key"), ("size", 41))
            ] + [
                (("quarantined_leftovers",), []),
                (("quarantined_leftovers",), status["quarantined_leftovers"] * 2),
                (("unreadable", 0, "location"), "different-segment"),
                (("unreadable",), status["unreadable"] * 2),
                (("members", 0, "objects"), 3),
                (("members", 0, "unreadable_objects"), 2),
                (("members", 0, "safe_to_remove"), True),
            ]
            for change in changes:
                with self.subTest(no_space=no_space, change=change):
                    with self.assertRaises(SMOKE.SmokeFailure):
                        SMOKE.verify_settled_loss_status(self.changed(status, *change), original,
                                                        original["placements"][0], no_space=no_space,
                                                        phase="cold restart")

    def test_no_space_requires_exact_capacity_accounting_after_restart(self):
        original, status = self.loss_case(True)
        changes = [(("members", 0, "evacuation_capacity", field), value)
                   for field, value in (("sufficient", True), ("required_bytes", 99),
                                        ("available_bytes", 7), ("reserved_destination_bytes", 1))]
        changes += [(("members", 1, field), value)
                    for field, value in (("available_bytes", 7), ("objects", 1), ("used_bytes", 1))]
        for change in changes:
            with self.subTest(change=change):
                with self.assertRaises(SMOKE.SmokeFailure):
                    SMOKE.verify_settled_loss_status(self.changed(status, *change), original,
                                                    original["placements"][0], no_space=True,
                                                    phase="final snapshot")

    def test_restart_waits_for_settlement_with_the_existing_deadline(self):
        for no_space in (False, True):
            with self.subTest(no_space=no_space):
                original, status = self.loss_case(no_space)
                transient = self.changed(status, ("pending_operations",), 1)
                deadline = time.monotonic() + 30
                with mock.patch.object(SMOKE, "command_json", side_effect=[transient, status]) as query, \
                        mock.patch.object(SMOKE.time, "sleep") as sleep:
                    actual = SMOKE.wait_settled_loss_status(
                        Path("unused"), deadline, "owned-admin", original, original["placements"][0],
                        no_space=no_space, phase="cold restart")
                self.assertEqual(actual, status)
                self.assertEqual(query.call_count, 2)
                for call in query.call_args_list:
                    self.assertEqual(call.args[1], deadline)
                sleep.assert_called_once_with(0.1)


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
        self.assertTrue(stdout.startswith("LOCAL_STORAGE_POOL_SMOKE "))
        receipt = json.loads(stdout.removeprefix("LOCAL_STORAGE_POOL_SMOKE "))
        self.assertEqual(receipt["result"], "passed")
        self.assertEqual(receipt["cut_date"], "2026-11-06")
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
            self.assertEqual(evidence["cut_date"], "2026-11-06")
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


if __name__ == "__main__":
    unittest.main()
