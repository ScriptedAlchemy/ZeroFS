from __future__ import annotations

import tempfile
import unittest
import json
from pathlib import Path
from subprocess import CompletedProcess
from typing import Sequence

from scripts.vm100_pilot.memory_envelope import (
    MemoryEnvelopeAuthority,
    MemoryEnvelopeSession,
)
from scripts.vm100_pilot.metrics import WritebackSnapshot
from scripts.vm100_pilot.runner import Runner
from scripts.vm100_pilot.scenarios import require_memory_scenario


class SystemctlRunner(Runner):
    def __init__(
        self,
        *,
        pid: int = 123,
        restarts: int = 4,
        control_group: str = "/zerofs.service",
    ) -> None:
        super().__init__(base_env={})
        self.pid = pid
        self.restarts = restarts
        self.control_group = control_group

    def run(
        self,
        argv: Sequence[str | Path],
        **_: object,
    ) -> CompletedProcess[str]:
        args = tuple(str(item) for item in argv)
        if args[:2] == ("systemctl", "show"):
            return CompletedProcess(
                args,
                0,
                f"MainPID={self.pid}\n"
                f"NRestarts={self.restarts}\n"
                f"ControlGroup={self.control_group}\n",
                "",
            )
        raise AssertionError(f"unexpected command: {args}")


class Metrics:
    def __init__(self, snapshot: WritebackSnapshot) -> None:
        self.value = snapshot

    def snapshot(self) -> WritebackSnapshot:
        return self.value


class MemoryEnvelopeTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory(dir="/var/tmp")
        self.addCleanup(self.temp.cleanup)
        base = Path(self.temp.name)
        self.cgroup_root = base / "cgroup"
        self.cgroup = self.cgroup_root / "zerofs.service"
        self.cgroup.mkdir(parents=True)
        (self.cgroup / "memory.current").write_text("1073741824\n")
        (self.cgroup / "memory.peak").write_text("2147483648\n")
        (self.cgroup / "memory.swap.current").write_text("0\n")
        (self.cgroup / "memory.events").write_text(
            "low 0\nhigh 0\nmax 0\noom 2\noom_kill 1\n"
        )
        self.proc_root = base / "proc"
        status = self.proc_root / "123" / "status"
        status.parent.mkdir(parents=True)
        status.write_text(
            "Name:\tzerofs\n"
            "VmRSS:\t1048576 kB\n"
            "VmHWM:\t1572864 kB\n"
            "VmSwap:\t0 kB\n"
        )
        self.authority = MemoryEnvelopeAuthority.from_mapping(
            {
                "ZEROFS_BENCH_CGROUP_ROOT": str(self.cgroup_root),
                "ZEROFS_BENCH_CGROUP_PATH": str(self.cgroup),
                "ZEROFS_BENCH_PROC_ROOT": str(self.proc_root),
                "ZEROFS_BENCH_SERVICE": "zerofs.service",
            }
        )
        self.metrics = Metrics(
            WritebackSnapshot(8, 8, 8, 0, 0, 20, 20, False)
        )
        self.scenario = require_memory_scenario("memory-envelope")

    def test_snapshot_records_cgroup_process_restart_and_writeback_state(self) -> None:
        session = MemoryEnvelopeSession.start(
            self.authority,
            SystemctlRunner(),
            self.metrics,  # type: ignore[arg-type]
            self.scenario,
        )

        before = session.samples[0]

        self.assertEqual(before.phase, "before")
        self.assertEqual(before.cgroup_current_bytes, 1 << 30)
        self.assertEqual(before.cgroup_peak_bytes, 2 << 30)
        self.assertEqual(before.pid_rss_bytes, 1 << 30)
        self.assertEqual(before.pid_hwm_bytes, 1536 << 20)
        self.assertEqual(before.oom, 2)
        self.assertEqual(before.oom_kill, 1)
        self.assertEqual(before.restart_count, 4)
        self.assertFalse(before.writeback.terminal)

    def test_new_oom_or_restart_fails_closed(self) -> None:
        runner = SystemctlRunner()
        session = MemoryEnvelopeSession.start(
            self.authority,
            runner,
            self.metrics,  # type: ignore[arg-type]
            self.scenario,
        )
        (self.cgroup / "memory.events").write_text(
            "low 0\nhigh 0\nmax 0\noom 3\noom_kill 2\n"
        )

        with self.assertRaisesRegex(RuntimeError, "OOM counters changed"):
            session.sample("foreground_close")

        (self.cgroup / "memory.events").write_text(
            "low 0\nhigh 0\nmax 0\noom 2\noom_kill 1\n"
        )
        runner = SystemctlRunner()
        session = MemoryEnvelopeSession.start(
            self.authority,
            runner,
            self.metrics,  # type: ignore[arg-type]
            self.scenario,
        )
        runner.restarts = 5
        with self.assertRaisesRegex(RuntimeError, "restart count changed"):
            session.sample("foreground_close")

    def test_service_control_group_must_match_configured_cgroup(self) -> None:
        with self.assertRaisesRegex(RuntimeError, "ControlGroup.*mismatch"):
            MemoryEnvelopeSession.start(
                self.authority,
                SystemctlRunner(control_group="/other.service"),
                self.metrics,  # type: ignore[arg-type]
                self.scenario,
            )

    def test_ceiling_and_terminal_state_fail_closed(self) -> None:
        session = MemoryEnvelopeSession.start(
            self.authority,
            SystemctlRunner(),
            self.metrics,  # type: ignore[arg-type]
            self.scenario,
        )
        (self.cgroup / "memory.current").write_text(str((96 << 30) + 1))
        with self.assertRaisesRegex(RuntimeError, "cgroup current ceiling"):
            session.sample("foreground_close")

        (self.cgroup / "memory.current").write_text(str(1 << 30))
        self.metrics.value = WritebackSnapshot(8, 8, 8, 0, 0, 20, 20, False)
        session = MemoryEnvelopeSession.start(
            self.authority,
            SystemctlRunner(),
            self.metrics,  # type: ignore[arg-type]
            self.scenario,
        )
        self.metrics.value = WritebackSnapshot(9, 8, 8, 1, 0, 20, 20, True)
        with self.assertRaisesRegex(RuntimeError, "terminal writeback"):
            session.sample("foreground_close")

    def test_finish_requires_every_phase_and_records_idempotent_cleanup(self) -> None:
        session = MemoryEnvelopeSession.start(
            self.authority,
            SystemctlRunner(),
            self.metrics,  # type: ignore[arg-type]
            self.scenario,
        )
        with self.assertRaisesRegex(RuntimeError, "missing phases"):
            session.finish()

        for phase in (
            "foreground_close",
            "fsync_or_commit",
            "local",
            "remote",
            "after_cleanup",
        ):
            session.sample(phase)

        result = session.finish()
        self.assertEqual(len(result.samples), 6)
        self.assertEqual(result.cleanup_attempts, 0)
        self.assertTrue(result.cleanup_asserted)
        self.assertEqual(result.cleanup_semantics, "observer-owned-no-resources")

    def test_failure_finalization_persists_partial_samples_after_cleanup(self) -> None:
        session = MemoryEnvelopeSession.start(
            self.authority,
            SystemctlRunner(),
            self.metrics,  # type: ignore[arg-type]
            self.scenario,
        )
        artifact = Path(self.temp.name) / "memory-envelope.json"
        session.attach_artifact(artifact)
        session.sample("foreground_close:test")

        result = session.finish_after_cleanup(require_complete=False)

        self.assertFalse(result.complete)
        self.assertIn("fsync_or_commit", result.missing_phases)
        payload = json.loads(artifact.read_text(encoding="utf-8"))
        self.assertEqual(payload["status"], "incomplete")
        self.assertEqual(
            [sample["phase"] for sample in payload["samples"]],
            ["before", "foreground_close:test", "after_cleanup"],
        )

    def test_rejected_oom_sample_is_persisted_before_raise(self) -> None:
        artifact = Path(self.temp.name) / "memory-envelope.json"
        session = MemoryEnvelopeSession.prepare(
            self.authority,
            SystemctlRunner(),
            self.metrics,  # type: ignore[arg-type]
            self.scenario,
        )
        session.attach_artifact(artifact)
        session.begin()
        (self.cgroup / "memory.events").write_text(
            "low 0\nhigh 0\nmax 0\noom 3\noom_kill 2\n"
        )

        with self.assertRaisesRegex(RuntimeError, "OOM counters changed"):
            session.sample("foreground_close:test")

        payload = json.loads(artifact.read_text(encoding="utf-8"))
        self.assertEqual(payload["status"], "failed")
        self.assertEqual(payload["samples"][-1]["oom"], 3)
        self.assertIn("OOM counters changed", payload["validation_errors"][-1])


if __name__ == "__main__":
    unittest.main()
