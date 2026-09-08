from __future__ import annotations

import unittest
import tempfile
import tomllib
from pathlib import Path

from scripts.tiered_writeback_e2e.observed_durability import (
    ObservedDurabilityCollector,
    ObservedDurabilityError,
    load_observed_endpoint,
)
from scripts.tiered_writeback_e2e.lifecycle import derive_bootstrap_config
from scripts.vm100_pilot.metrics import MetricsAuthorityIdentity


def metrics_text(
    *,
    instance: str = "instance-a",
    filesystem: str = "filesystem-a",
    export: str = "export-a",
    accepted: int = 3,
    local: int = 2,
    remote: int = 1,
    terminal: int = 0,
) -> str:
    return "\n".join(
        (
            "zerofs_benchmark_authority_info{"
            f'server_instance_id="{instance}",filesystem_id="{filesystem}",'
            f'export_id="{export}"}} 1',
            f"zerofs_writeback_accepted_sequence {accepted}",
            f"zerofs_writeback_local_sequence {local}",
            f"zerofs_writeback_remote_sequence {remote}",
            "zerofs_writeback_dirty_ram_bytes 0",
            "zerofs_writeback_dirty_ssd_reserved_bytes 0",
            "zerofs_writeback_local_bytes_completed_total 1",
            "zerofs_writeback_remote_bytes_completed_total 1",
            f"zerofs_writeback_terminal_error {terminal}",
            "zerofs_segment_gc_passes_total 0",
            "zerofs_segment_gc_batches_total 0",
            "zerofs_segment_gc_deleted_bytes_total 0",
        )
    )


class SequenceFetcher:
    def __init__(self, *samples: str | BaseException) -> None:
        self.samples = list(samples)

    def __call__(self) -> str:
        if len(self.samples) > 1:
            sample = self.samples.pop(0)
        else:
            sample = self.samples[0]
        if isinstance(sample, BaseException):
            raise sample
        return sample


class ObservedDurabilityCollectorTests(unittest.TestCase):
    def test_snapshot_rejects_impossible_frontier_ordering(self) -> None:
        for accepted, local, remote in ((2, 3, 1), (3, 1, 2)):
            with self.subTest(accepted=accepted, local=local, remote=remote):
                collector = ObservedDurabilityCollector(
                    lambda: metrics_text(
                        accepted=accepted,
                        local=local,
                        remote=remote,
                    )
                )
                with self.assertRaisesRegex(
                    ObservedDurabilityError, "remote <= local <= accepted"
                ):
                    collector.snapshot()

    def test_snapshot_wraps_missing_or_malformed_typed_metrics(self) -> None:
        for sample in (
            "zerofs_writeback_accepted_sequence 1\n",
            metrics_text().replace(
                "zerofs_writeback_local_sequence 2",
                "zerofs_writeback_local_sequence not-a-number",
            ),
        ):
            with self.subTest(sample=sample):
                with self.assertRaisesRegex(
                    ObservedDurabilityError, "invalid observed durability metrics"
                ):
                    ObservedDurabilityCollector(lambda: sample).snapshot()

    def test_one_collector_rejects_any_incarnation_change(self) -> None:
        collector = ObservedDurabilityCollector(
            SequenceFetcher(
                metrics_text(instance="instance-a"),
                metrics_text(instance="instance-b", accepted=4, local=3, remote=2),
            )
        )
        collector.snapshot()
        with self.assertRaisesRegex(ObservedDurabilityError, "incarnation changed"):
            collector.snapshot()

    def test_initial_and_restart_waits_retry_transport_readiness_only(self) -> None:
        initial = ObservedDurabilityCollector(
            SequenceFetcher(
                ConnectionRefusedError("not listening"),
                metrics_text(accepted=3, local=3, remote=1),
            )
        ).wait_for_initial_snapshot(timeout=0.1, interval=0)
        self.assertEqual(initial.identity.server_instance_id, "instance-a")

        restarted = ObservedDurabilityCollector(
            SequenceFetcher(
                ConnectionRefusedError("not listening"),
                metrics_text(
                    instance="instance-b",
                    accepted=3,
                    local=3,
                    remote=1,
                ),
            )
        ).wait_for_restarted(initial, timeout=0.1, interval=0)
        self.assertEqual(restarted.identity.server_instance_id, "instance-b")

        malformed = ObservedDurabilityCollector(lambda: "not metrics")
        with self.assertRaisesRegex(
            ObservedDurabilityError, "invalid observed durability metrics"
        ):
            malformed.wait_for_initial_snapshot(timeout=0.1, interval=0)

    def test_wait_for_new_accepted_then_exact_local_cutoff(self) -> None:
        collector = ObservedDurabilityCollector(
            SequenceFetcher(
                metrics_text(accepted=3, local=3, remote=1),
                metrics_text(accepted=4, local=3, remote=1),
                metrics_text(accepted=4, local=4, remote=1),
            )
        )
        first = collector.snapshot()
        accepted = collector.wait_for_accepted_after(
            first.writeback.accepted,
            timeout=0.1,
            interval=0,
        )
        self.assertEqual(accepted.writeback.accepted, 4)
        local = collector.wait_for_local_frontier(4, timeout=0.1, interval=0)
        self.assertEqual(local.writeback.local, 4)
        self.assertEqual(local.identity.server_instance_id, "instance-a")

    def test_local_wait_covers_the_current_accepted_value_when_it_advances(self) -> None:
        collector = ObservedDurabilityCollector(
            SequenceFetcher(
                metrics_text(accepted=4, local=3, remote=1),
                metrics_text(accepted=5, local=4, remote=1),
                metrics_text(accepted=5, local=5, remote=1),
            )
        )
        observed = collector.wait_for_local_frontier(4, timeout=0.1, interval=0)
        self.assertEqual(observed.writeback.accepted, 5)
        self.assertEqual(observed.writeback.local, 5)

    def test_restart_requires_new_instance_same_export_and_nonregressing_frontiers(
        self,
    ) -> None:
        before = ObservedDurabilityCollector(
            lambda: metrics_text(accepted=7, local=7, remote=4)
        ).snapshot()

        restarted = ObservedDurabilityCollector(
            lambda: metrics_text(
                instance="instance-b", accepted=7, local=7, remote=4
            )
        ).require_restarted(before)
        self.assertEqual(restarted.identity.server_instance_id, "instance-b")

        cases = (
            (
                metrics_text(instance="instance-a", accepted=7, local=7, remote=4),
                "did not change",
            ),
            (
                metrics_text(
                    instance="instance-b",
                    filesystem="filesystem-b",
                    accepted=7,
                    local=7,
                    remote=4,
                ),
                "filesystem/export",
            ),
            (
                metrics_text(instance="instance-b", accepted=6, local=6, remote=4),
                "regressed across restart",
            ),
        )
        for sample, message in cases:
            with self.subTest(message=message):
                with self.assertRaisesRegex(ObservedDurabilityError, message):
                    ObservedDurabilityCollector(lambda: sample).require_restarted(before)

    def test_restart_receipt_classifies_remote_coverage_without_overclaiming(self) -> None:
        local_only = ObservedDurabilityCollector.classify_recovery_source(
            target=7,
            observed=ObservedDurabilityCollector(
                lambda: metrics_text(accepted=7, local=7, remote=4)
            ).snapshot(),
        )
        remote_covered = ObservedDurabilityCollector.classify_recovery_source(
            target=7,
            observed=ObservedDurabilityCollector(
                lambda: metrics_text(accepted=7, local=7, remote=7)
            ).snapshot(),
        )
        self.assertEqual(
            local_only, "remote-not-covered-at-pre-kill-sample"
        )
        self.assertEqual(remote_covered, "remote-covered-at-pre-kill-sample")


class MetricsAuthorityIdentityFixtureTests(unittest.TestCase):
    def test_nbd_export_identity_accepts_the_exact_run_scoped_name(self) -> None:
        identity = MetricsAuthorityIdentity.parse(
            metrics_text(
                export="zerofs-tiered-1f4a3c60-8f6f-4c39-9f3e-2b8f6f2d9a01"
            )
        )
        self.assertEqual(
            identity.export_id,
            "zerofs-tiered-1f4a3c60-8f6f-4c39-9f3e-2b8f6f2d9a01",
        )

    def test_runtime_template_derives_one_ninep_only_bootstrap_config(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            runtime = root / "runtime.toml"
            bootstrap = root / "bootstrap.toml"
            runtime.write_text(
                """
[storage]
url = "s3://bucket/prefix"

# TIERED_RUNTIME_SERVERS_BEGIN
[servers.nbd]
addresses = ["127.0.0.1:10809"]

[prometheus]
addresses = ["127.0.0.1:19567"]

[prometheus.benchmark_authority]
adapter = "nbd"
export_id = "exact-export"
tls_certificate = "/tmp/cert"
tls_private_key = "/tmp/key"
# TIERED_RUNTIME_SERVERS_END

[writeback]
enabled = true
min_free_gb = 0.25
""",
                encoding="utf-8",
            )

            derive_bootstrap_config(
                runtime,
                bootstrap,
                ninep_socket=Path("/owned/run/bootstrap.9p.sock"),
            )

            document = tomllib.loads(bootstrap.read_text(encoding="utf-8"))
            self.assertEqual(
                document["servers"]["ninep"]["unix_socket"],
                "/owned/run/bootstrap.9p.sock",
            )
            self.assertNotIn("nbd", document["servers"])
            self.assertNotIn("prometheus", document)
            self.assertEqual(document["writeback"]["min_free_gb"], 0.25)
            self.assertEqual(document["storage"]["url"], "s3://bucket/prefix")

    def test_bootstrap_derivation_rejects_missing_or_repeated_fixed_markers(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            runtime = root / "runtime.toml"
            bootstrap = root / "bootstrap.toml"
            for content in (
                "[storage]\nurl = 's3://bucket/prefix'\n",
                "# TIERED_RUNTIME_SERVERS_BEGIN\n"
                "# TIERED_RUNTIME_SERVERS_END\n"
                "# TIERED_RUNTIME_SERVERS_BEGIN\n"
                "# TIERED_RUNTIME_SERVERS_END\n",
            ):
                with self.subTest(content=content):
                    runtime.write_text(content, encoding="utf-8")
                    with self.assertRaisesRegex(
                        ValueError, "exactly one fixed runtime server block"
                    ):
                        derive_bootstrap_config(
                            runtime,
                            bootstrap,
                            ninep_socket=Path("/owned/run/bootstrap.9p.sock"),
                        )

    def test_checked_runtime_template_resolves_exact_nbd_tls_authority(self) -> None:
        template = (
            Path(__file__).resolve().parents[1]
            / "tiered_writeback_e2e"
            / "xfs_nbd_tiered.toml.template"
        )
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            control = root / "control"
            resources = root / "resources"
            (control / "tls").mkdir(parents=True)
            resources.mkdir()
            certificate = control / "tls" / "metrics.crt"
            private_key = control / "tls" / "metrics.key"
            certificate.write_text("certificate", encoding="utf-8")
            private_key.write_text("private key", encoding="utf-8")
            endpoint = load_observed_endpoint(
                template,
                control_root=control,
                expected_export=(
                    "zerofs-tiered-1f4a3c60-8f6f-4c39-9f3e-2b8f6f2d9a01"
                ),
                environ={
                    "RUN_UUID": "1f4a3c60-8f6f-4c39-9f3e-2b8f6f2d9a01",
                    "CONTROL_ROOT": str(control),
                    "RESOURCE_ROOT": str(resources),
                    "MINIO_BUCKET": "zerofs-tiered-test-bucket",
                },
            )
            self.assertEqual(endpoint.url, "https://127.0.0.1:19567/metrics")
            self.assertEqual(endpoint.ca_file, certificate)
            self.assertEqual(
                endpoint.export_id,
                "zerofs-tiered-1f4a3c60-8f6f-4c39-9f3e-2b8f6f2d9a01",
            )

    def test_observed_endpoint_rejects_unowned_ca_or_export_equivocation(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            control = root / "control"
            control.mkdir()
            outside = root / "outside.crt"
            outside.write_text("certificate", encoding="utf-8")
            config = root / "runtime.toml"
            config.write_text(
                f"""
[servers.nbd]
addresses = ["127.0.0.1:10809"]
[prometheus]
addresses = ["127.0.0.1:19567"]
[prometheus.benchmark_authority]
adapter = "nbd"
export_id = "wrong-export"
tls_certificate = "{outside}"
tls_private_key = "{root / 'outside.key'}"
""",
                encoding="utf-8",
            )
            with self.assertRaisesRegex(
                ObservedDurabilityError, "exact NBD export"
            ):
                load_observed_endpoint(
                    config,
                    control_root=control,
                    expected_export="expected-export",
                    environ={},
                )
            config.write_text(
                config.read_text(encoding="utf-8").replace(
                    'export_id = "wrong-export"',
                    'export_id = "expected-export"',
                ),
                encoding="utf-8",
            )
            with self.assertRaisesRegex(ObservedDurabilityError, "control root"):
                load_observed_endpoint(
                    config,
                    control_root=control,
                    expected_export="expected-export",
                    environ={},
                )


if __name__ == "__main__":
    unittest.main()
