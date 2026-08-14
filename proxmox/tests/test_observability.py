from __future__ import annotations

import importlib.util
import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).parents[1]
INSTALLER = ROOT / "monitoring" / "install.py"
SPEC = importlib.util.spec_from_file_location("zerofs_monitoring_install", INSTALLER)
assert SPEC is not None and SPEC.loader is not None
monitoring = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = monitoring
SPEC.loader.exec_module(monitoring)


class MonitoringInputTests(unittest.TestCase):
    def test_defaults_target_private_existing_monitoring_container(self) -> None:
        args = monitoring.build_parser().parse_args(
            ["--zerofs-ip", "10.10.10.30", "--dry-run"]
        )
        validated = monitoring.validate_args(args)
        self.assertEqual(validated.monitoring_ctid, 123)
        self.assertEqual(validated.monitoring_ip, "10.10.10.53")
        self.assertEqual(validated.prometheus_url, "http://127.0.0.1:9090")

    def test_public_addresses_and_credential_urls_are_rejected(self) -> None:
        rejected = (
            ["--zerofs-ip", "1.1.1.1", "--dry-run"],
            [
                "--zerofs-ip",
                "10.10.10.30",
                "--monitoring-ip",
                "8.8.8.8",
                "--dry-run",
            ],
            [
                "--zerofs-ip",
                "10.10.10.30",
                "--prometheus-url",
                "http://user:secret@127.0.0.1:9090",
                "--dry-run",
            ],
        )
        for argv in rejected:
            with self.subTest(argv=argv), self.assertRaises(ValueError):
                monitoring.validate_args(monitoring.build_parser().parse_args(argv))

    def test_rendered_assets_have_exact_private_target_and_no_placeholders(
        self,
    ) -> None:
        args = monitoring.validate_args(
            monitoring.build_parser().parse_args(
                ["--zerofs-ip", "10.10.10.30", "--dry-run"]
            )
        )
        with tempfile.TemporaryDirectory() as directory:
            rendered = monitoring.render_assets(args, Path(directory))
            combined = "\n".join(path.read_text() for path in rendered)
            self.assertIn('"10.10.10.30:9567"', combined)
            self.assertIn("http://127.0.0.1:9090", combined)
            self.assertNotIn("@@", combined)

    def test_dry_run_prints_backup_validation_and_rollback_without_ssh(
        self,
    ) -> None:
        result = subprocess.run(
            [
                "python3",
                str(INSTALLER),
                "--zerofs-ip",
                "10.10.10.30",
                "--dry-run",
            ],
            text=True,
            capture_output=True,
            check=False,
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("monitoring_ctid=123", result.stdout)
        self.assertIn("monitoring_ip=10.10.10.53", result.stdout)
        self.assertIn("backup", result.stdout.lower())
        self.assertIn("rollback", result.stdout.lower())
        self.assertIn("promtool", result.stdout)
        self.assertIn("grafana-server", result.stdout)
        self.assertNotIn("scp ", result.stdout)
        self.assertNotIn("ssh ", result.stdout)


class MonitoringAssetTests(unittest.TestCase):
    def test_dashboard_covers_writeback_throughput_failures_and_gc(self) -> None:
        dashboard = json.loads(
            (ROOT / "monitoring" / "grafana" / "zerofs-overview.json").read_text()
        )
        expressions = "\n".join(
            target.get("expr", "")
            for panel in dashboard["panels"]
            for target in panel.get("targets", [])
        )
        required = (
            'up{job="zerofs-prod"}',
            "zerofs_writeback_accepted_sequence",
            "zerofs_writeback_local_sequence",
            "zerofs_writeback_remote_sequence",
            "zerofs_writeback_local_lag_operations",
            "zerofs_writeback_remote_lag_operations",
            "zerofs_writeback_dirty_ram_bytes",
            "zerofs_writeback_dirty_ssd_reserved_bytes",
            "rate(zerofs_writeback_local_bytes_completed_total",
            "rate(zerofs_writeback_remote_bytes_completed_total",
            "rate(zerofs_writeback_retries_total",
            "zerofs_writeback_terminal_error",
            "zerofs_segment_gc_active",
            "rate(zerofs_segment_gc_deleted_bytes_total",
        )
        for metric in required:
            with self.subTest(metric=metric):
                self.assertIn(metric, expressions)
        self.assertEqual(dashboard["uid"], "zerofs-prod-overview")

    def test_monitoring_host_installer_has_backup_and_error_rollback(self) -> None:
        host_installer = ROOT / "monitoring" / "host-install.sh"
        result = subprocess.run(
            ["bash", "-n", str(host_installer)],
            text=True,
            capture_output=True,
            check=False,
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        source = host_installer.read_text()
        self.assertIn("trap rollback ERR", source)
        self.assertIn("apt-get install -y --no-install-recommends prometheus", source)
        self.assertIn("promtool check config", source)
        self.assertIn("systemctl restart grafana-server.service", source)
        self.assertIn("127.0.0.1:9090", source)
        self.assertNotIn("rm -rf", source)


if __name__ == "__main__":
    unittest.main()
