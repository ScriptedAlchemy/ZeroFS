from __future__ import annotations

import importlib.util
import io
import json
import unittest
from contextlib import redirect_stdout
from pathlib import Path

from scripts.vm100_pilot.scenarios import (
    UnknownScenarioError,
    list_scenarios,
    require_scenario,
)


ROOT = Path(__file__).resolve().parents[2]


def load_cli() -> object:
    spec = importlib.util.spec_from_file_location(
        "vm100_pilot_cli", ROOT / "scripts" / "vm100-pilot.py"
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class ScenarioRegistryTests(unittest.TestCase):
    def test_protocol_definitions_use_identical_real_workloads_and_no_nbd(self) -> None:
        nfs = require_scenario("protocol-matrix-nfs")
        ninep = require_scenario("protocol-matrix-9p")

        self.assertEqual(nfs.workloads, ninep.workloads)
        self.assertEqual(
            [(workload.bytes, workload.pattern) for workload in nfs.workloads],
            [
                (64 * 1024 * 1024, "incompressible-random-v1"),
                (1024 * 1024 * 1024, "incompressible-random-v1"),
            ],
        )
        self.assertEqual(nfs.protocol, "nfs")
        self.assertEqual(ninep.protocol, "9p")
        self.assertNotIn("nbd", {item.protocol for item in list_scenarios()})

    def test_protocol_definitions_require_authority_and_distinct_cutoffs(self) -> None:
        for name in ("protocol-matrix-nfs", "protocol-matrix-9p"):
            scenario = require_scenario(name)
            self.assertEqual(
                scenario.required_authority,
                ("mountpoint", "endpoint", "mount_options"),
            )
            self.assertEqual(
                scenario.cutoffs,
                ("foreground_close", "fsync_or_commit", "local", "remote"),
            )
            self.assertTrue(scenario.sha256_required)
            self.assertTrue(scenario.cleanup_required)

    def test_memory_envelope_has_fixed_nonzero_limits(self) -> None:
        scenario = require_scenario("memory-envelope")

        self.assertIsNotNone(scenario.memory_limits)
        assert scenario.memory_limits is not None
        self.assertEqual(scenario.memory_limits.cgroup_current_bytes, 96 << 30)
        self.assertEqual(scenario.memory_limits.cgroup_peak_bytes, 112 << 30)
        self.assertEqual(scenario.memory_limits.pid_rss_bytes, 80 << 30)
        self.assertEqual(scenario.memory_limits.swap_bytes, 0)

    def test_registry_is_immutable_and_unknown_scenarios_fail_closed(self) -> None:
        scenarios = list_scenarios()

        self.assertIsInstance(scenarios, tuple)
        with self.assertRaisesRegex(UnknownScenarioError, "not registered"):
            require_scenario("benchmark-that-does-nothing")

    def test_list_scenarios_cli_emits_registered_nonzero_work(self) -> None:
        module = load_cli()
        output = io.StringIO()

        with redirect_stdout(output):
            module.dispatch(
                module.build_parser().parse_args(["list-scenarios"]),
                config=None,
                runner=None,
            )

        payload = json.loads(output.getvalue())
        self.assertEqual(payload[0]["schema"], 1)
        by_name = {item["name"]: item for item in payload}
        self.assertEqual(
            by_name["protocol-matrix-nfs"]["workloads"][0]["bytes"],
            64 * 1024 * 1024,
        )
        self.assertNotIn("benchmark-that-does-nothing", by_name)


if __name__ == "__main__":
    unittest.main()
