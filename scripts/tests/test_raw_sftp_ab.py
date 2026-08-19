from __future__ import annotations

import hashlib
import importlib.util
import io
import json
import signal
import shlex
import tempfile
import time
import unittest
from unittest import mock
from contextlib import redirect_stderr
from dataclasses import replace
from pathlib import Path
from subprocess import CompletedProcess
from typing import Any

from scripts.vm100_pilot.config import PilotConfig
from scripts.vm100_pilot.raw_sftp import (
    RawSftpRunner,
    SftpEndpoint,
    SftpEndpointAuthority,
    SftpPhaseResult,
    SftpSessionResult,
    SshBinaryIdentity,
    counterbalanced_order,
    identify_ssh_binary,
)
from scripts.vm100_pilot.runner import Runner
from scripts.vm100_pilot.scenarios import RawSftpScenario


ROOT = Path(__file__).resolve().parents[2]


def load_cli() -> object:
    spec = importlib.util.spec_from_file_location(
        "vm100_pilot_cli", ROOT / "scripts" / "vm100-pilot.py"
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class RawSftpAbTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory(dir="/var/tmp")
        self.addCleanup(self.temp.cleanup)

    def test_binary_identity_records_resolved_path_version_and_sha256(self) -> None:
        binary = Path("/usr/bin/ssh")

        identity = identify_ssh_binary(binary, Runner(base_env={}))

        self.assertEqual(identity.path, str(binary.resolve()))
        self.assertTrue(identity.version.startswith("OpenSSH_"))
        self.assertEqual(
            identity.sha256,
            hashlib.sha256(binary.read_bytes()).hexdigest(),
        )

    def test_binary_identity_rejects_relative_and_non_executable_paths(self) -> None:
        relative = Path("hpnssh")
        with self.assertRaisesRegex(ValueError, "absolute"):
            identify_ssh_binary(relative, Runner(base_env={}))

        binary = Path(self.temp.name) / "not-executable"
        binary.write_text("content", encoding="utf-8")
        with self.assertRaisesRegex(ValueError, "executable"):
            identify_ssh_binary(binary, Runner(base_env={}))

    def test_stock_and_hpn_controls_must_report_distinct_provenance(self) -> None:
        raw = object.__new__(RawSftpRunner)
        raw.runner = mock_runner = mock.Mock()
        mock_runner.run.side_effect = [
            CompletedProcess(("ssh-a", "-V"), 0, "", "OpenSSH_9.9 HPN-SSH\n"),
            CompletedProcess(("ssh-b", "-V"), 0, "", "OpenSSH_9.9 HPN-SSH\n"),
        ]
        first = Path(self.temp.name) / "ssh-a"
        second = Path(self.temp.name) / "ssh-b"
        for path, content in ((first, "a"), (second, "b")):
            path.write_text(content, encoding="utf-8")
            path.chmod(0o700)

        with self.assertRaisesRegex(ValueError, "stock.*HPN"):
            raw._identify_binaries(first, second)

    def test_parallel_phase_deadline_terminates_every_process_group(self) -> None:
        class NeverProcess:
            def __init__(self) -> None:
                self.process = self
                self.argv = ("sftp",)
                self.terminated = False
                self.returncode: int | None = None

            def poll(self) -> int | None:
                return self.returncode

            def signal_group(self, signal_number: int) -> None:
                self.terminated = True
                if signal_number == signal.SIGKILL:
                    self.returncode = -signal_number

        class SpawnRunner:
            def __init__(self) -> None:
                self.processes: list[NeverProcess] = []

            def spawn(self, *args: object, **kwargs: object) -> NeverProcess:
                del args, kwargs
                process = NeverProcess()
                self.processes.append(process)
                return process

        raw = object.__new__(RawSftpRunner)
        raw.runner = runner = SpawnRunner()
        raw.phase_timeout = 0.01
        scratch = Path(self.temp.name)
        batches = [scratch / "a.batch", scratch / "b.batch"]
        logs = [scratch / "a.log", scratch / "b.log"]
        for batch in batches:
            batch.write_text("quit\n", encoding="utf-8")
        endpoint = SftpEndpoint(
            "alice", "203.0.113.10", 22, Path("/key"), Path("/known"), "/"
        )

        started = time.monotonic()
        with self.assertRaisesRegex(TimeoutError, "deadline"):
            raw._parallel_batches(
                endpoint,
                batches,
                logs,
                Path("/ssh"),
                buffer_bytes=1_048_576,
                request_depth=128,
                bytes_per_session=1,
            )

        self.assertLess(time.monotonic() - started, 1)
        self.assertTrue(runner.processes)
        self.assertTrue(all(process.terminated for process in runner.processes))

    def test_worker_failure_preserves_process_group_termination_failure(self) -> None:
        class Process:
            def __init__(self, returncode: int | None, terminate_error: str = "") -> None:
                self.process = self
                self.argv = ("sftp",)
                self.returncode = returncode
                self.terminate_error = terminate_error

            def poll(self) -> int | None:
                return self.returncode

            def signal_group(self, signal_number: int) -> None:
                del signal_number
                if self.terminate_error:
                    raise RuntimeError(self.terminate_error)
                self.returncode = -15

        class SpawnRunner:
            def __init__(self) -> None:
                self.processes = [Process(7), Process(None, "cannot reap worker")]

            def spawn(self, *args: object, **kwargs: object) -> Process:
                del args, kwargs
                return self.processes.pop(0)

        raw = object.__new__(RawSftpRunner)
        raw.runner = SpawnRunner()
        raw.phase_timeout = 1
        scratch = Path(self.temp.name)
        batches = [scratch / "a.batch", scratch / "b.batch"]
        logs = [scratch / "a.log", scratch / "b.log"]
        for batch in batches:
            batch.write_text("quit\n", encoding="utf-8")
        endpoint = SftpEndpoint(
            "alice", "203.0.113.10", 22, Path("/key"), Path("/known"), "/"
        )

        with self.assertRaisesRegex(RuntimeError, "worker failed") as raised:
            raw._parallel_batches(
                endpoint,
                batches,
                logs,
                Path("/ssh"),
                buffer_bytes=1,
                request_depth=1,
                bytes_per_session=1,
            )

        self.assertIn("cannot reap worker", " ".join(raised.exception.__notes__))

    def test_metadata_deadline_terminates_its_process_group(self) -> None:
        class NeverCommand:
            def __init__(self) -> None:
                self.process = self
                self.argv = ("sudo", "sftp")
                self.returncode: int | None = None
                self.terminated = False
                self.communicate_calls = 0
                self.signal_error = ""

            def communicate(self, timeout: float | None = None) -> tuple[str, str]:
                self.communicate_calls += 1
                if timeout is not None:
                    from subprocess import TimeoutExpired

                    raise TimeoutExpired(self.argv, timeout)
                return "", ""

            def poll(self) -> int | None:
                return self.returncode

            def signal_group(self, signal_number: int) -> None:
                if self.signal_error:
                    raise RuntimeError(self.signal_error)
                self.terminated = True
                self.returncode = -signal_number

        process = NeverCommand()
        runner = mock.Mock()
        runner.spawn.return_value = process
        raw = object.__new__(RawSftpRunner)
        raw.runner = runner
        raw.command_timeout = 0.01
        batch = Path(self.temp.name) / "batch"
        batch.write_text("stat /\n", encoding="utf-8")
        endpoint = SftpEndpoint(
            "alice", "203.0.113.10", 22, Path("/key"), Path("/known"), "/"
        )

        with self.assertRaisesRegex(TimeoutError, "deadline"):
            raw._run_batch(endpoint, batch, Path("/ssh"), buffer_bytes=1, request_depth=1)

        self.assertTrue(process.terminated)
        self.assertEqual(process.communicate_calls, 2)

        failed_shutdown = NeverCommand()
        failed_shutdown.signal_error = "TERMINATION_REAP_MARKER"
        runner.spawn.return_value = failed_shutdown
        with self.assertRaisesRegex(TimeoutError, "deadline") as raised:
            raw._run_batch(endpoint, batch, Path("/ssh"), buffer_bytes=1, request_depth=1)
        self.assertIn(
            "TERMINATION_REAP_MARKER",
            " ".join(raised.exception.__notes__),
        )

    def test_counterbalanced_order_repeats_each_variant_equally(self) -> None:
        self.assertEqual(
            counterbalanced_order(4),
            ("stock", "hpn", "hpn", "stock"),
        )
        self.assertEqual(
            counterbalanced_order(6),
            ("stock", "hpn", "hpn", "stock", "stock", "hpn"),
        )
        with self.assertRaisesRegex(ValueError, "positive even"):
            counterbalanced_order(3)

    def test_sftp_command_pins_selected_ssh_and_identical_geometry(self) -> None:
        endpoint = SftpEndpoint(
            "alice",
            "203.0.113.10",
            23,
            Path("/key"),
            Path("/known"),
            "/data/prefix",
        )
        command = RawSftpRunner._command_for(
            endpoint,
            Path("/tmp/batch"),
            Path("/opt/hpn/bin/hpnssh"),
            buffer_bytes=1_048_576,
            request_depth=128,
        )

        self.assertEqual(command[command.index("-S") + 1], Path("/opt/hpn/bin/hpnssh"))
        self.assertEqual(command[command.index("-B") + 1], "1048576")
        self.assertEqual(command[command.index("-R") + 1], "128")
        self.assertIn("IdentitiesOnly=yes", command)
        self.assertIn("PasswordAuthentication=no", command)
        self.assertIn("KbdInteractiveAuthentication=no", command)
        self.assertIn("ControlMaster=no", command)
        self.assertEqual(command[command.index("-F") + 1], "/dev/null")

    def test_endpoint_authority_records_prefix_and_pinned_known_hosts_sha(self) -> None:
        key = Path(self.temp.name) / "identity"
        known_hosts = Path(self.temp.name) / "known_hosts"
        key.write_text("private-key-placeholder\n", encoding="utf-8")
        known_hosts.write_text(
            "203.0.113.10 ssh-ed25519 AAAATEST\n",
            encoding="utf-8",
        )
        endpoint = SftpEndpoint(
            "alice",
            "203.0.113.10",
            22,
            key,
            known_hosts,
            "/data/prefix",
        )
        raw = object.__new__(RawSftpRunner)

        authority = raw._endpoint_authority(endpoint)

        self.assertEqual(authority.host, "203.0.113.10")
        self.assertEqual(authority.prefix, "/data/prefix")
        self.assertEqual(authority.known_hosts_path, str(known_hosts.resolve()))
        self.assertEqual(
            authority.known_hosts_sha256,
            hashlib.sha256(known_hosts.read_bytes()).hexdigest(),
        )

    def test_remote_absence_does_not_accept_transport_failure(self) -> None:
        scratch = Path(self.temp.name) / "scratch"
        scratch.mkdir()
        endpoint = SftpEndpoint(
            "alice",
            "203.0.113.10",
            22,
            Path("/key"),
            Path("/known"),
            "/prefix",
        )

        class TransportFailureRaw(RawSftpRunner):
            def _run_batch(
                self,
                endpoint: SftpEndpoint,
                batch: Path,
                ssh_binary: Path,
                **kwargs: object,
            ) -> CompletedProcess[str]:
                del endpoint, ssh_binary, kwargs
                command = batch.read_text(encoding="utf-8")
                if "stat /prefix\n" in command:
                    return CompletedProcess(("sftp",), 0, "parent exists\n", "")
                if command.startswith("stat "):
                    return CompletedProcess(
                        ("sftp",),
                        255,
                        "",
                        "Connection closed by remote host\n",
                    )
                return CompletedProcess(("sftp",), 0, "", "")

        raw = object.__new__(TransportFailureRaw)

        with self.assertRaisesRegex(RuntimeError, "cannot prove.*absent"):
            raw._cleanup_remote(
                endpoint,
                scratch,
                "/prefix/zerofs-raw-control-deadbeef",
                Path("/hpn/ssh"),
                jobs=1,
                buffer_bytes=1_048_576,
                request_depth=128,
                label="transport-failure",
            )

    def test_raw_sftp_cli_requires_both_explicit_ssh_paths(self) -> None:
        module = load_cli()
        parser = module.build_parser()

        with redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
            parser.parse_args(["raw-sftp"])
        args = parser.parse_args(
            [
                "raw-sftp",
                "--stock-ssh",
                "/usr/bin/ssh",
                "--hpn-ssh",
                "/opt/hpn/bin/hpnssh",
            ]
        )
        self.assertEqual(args.stock_ssh, Path("/usr/bin/ssh"))
        self.assertEqual(args.hpn_ssh, Path("/opt/hpn/bin/hpnssh"))

    def test_ab_run_verifies_sha_and_persists_final_cleanup_ledger(self) -> None:
        root = Path(self.temp.name) / "repo"
        root.mkdir()
        mountpoint = Path(self.temp.name) / "mount"
        mountpoint.mkdir()
        config = PilotConfig.from_mapping(
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
        config.temp_dir.mkdir()

        class Lifecycle:
            def __init__(self) -> None:
                self.stop_calls = 0
                self.start_calls = 0

            def status(self) -> dict[str, bool]:
                return {"healthy": True}

            def drain(self, timeout: float | None = None) -> dict[str, bool]:
                del timeout
                return {"drained": True}

            def stop(self) -> None:
                self.stop_calls += 1

            def start(self) -> dict[str, bool]:
                self.start_calls += 1
                return {"started": True}

        class InMemoryRaw(RawSftpRunner):
            def __init__(self, *args: Any, **kwargs: Any) -> None:
                super().__init__(*args, **kwargs)
                self.remote_dirs: set[str] = {"/prefix"}
                self.remote_files: dict[str, bytes] = {}
                self.fail_create_ambiguously = False
                self.fail_transfer = False

            def _identify_binaries(
                self, stock_ssh: Path, hpn_ssh: Path
            ) -> tuple[SshBinaryIdentity, SshBinaryIdentity]:
                del stock_ssh, hpn_ssh
                return (
                    SshBinaryIdentity("/stock/ssh", "OpenSSH_stock", "a" * 64),
                    SshBinaryIdentity("/hpn/ssh", "OpenSSH_hpn", "b" * 64),
                )

            def _endpoint(self) -> SftpEndpoint:
                return SftpEndpoint(
                    "alice",
                    "203.0.113.10",
                    22,
                    Path("/key"),
                    Path("/known"),
                    "/prefix",
                )

            def _endpoint_authority(
                self, endpoint: SftpEndpoint
            ) -> SftpEndpointAuthority:
                return SftpEndpointAuthority(
                    endpoint.user,
                    endpoint.host,
                    endpoint.port,
                    endpoint.prefix,
                    "/key",
                    "/known",
                    "c" * 64,
                    "strict-pinned-known-hosts",
                )

            def _create_sources(
                self,
                scratch: Path,
                *,
                jobs: int,
                per_job_bytes: int,
            ) -> list[Path]:
                paths = [scratch / f"source-{index}.bin" for index in range(jobs)]
                for index, path in enumerate(paths):
                    path.write_bytes(bytes([index + 1]) * per_job_bytes)
                return paths

            def _run_batch(
                self,
                endpoint: SftpEndpoint,
                batch: Path,
                ssh_binary: Path,
                **kwargs: object,
            ) -> CompletedProcess[str]:
                del endpoint, ssh_binary, kwargs
                returncode = 0
                for line in batch.read_text(encoding="utf-8").splitlines():
                    command = shlex.split(line.lstrip("-"))
                    if command[0] == "mkdir":
                        self.remote_dirs.add(command[1])
                        if self.fail_create_ambiguously:
                            raise RuntimeError("injected ambiguous mkdir result")
                    elif command[0] == "rm":
                        self.remote_files.pop(command[1], None)
                    elif command[0] == "rmdir":
                        self.remote_dirs.discard(command[1])
                    elif command[0] == "stat":
                        returncode = 0 if command[1] in self.remote_dirs else 1
                stderr = "" if returncode == 0 else "No such file or directory\n"
                return CompletedProcess(("sftp",), returncode, "", stderr)

            def _parallel_batches(
                self,
                endpoint: SftpEndpoint,
                batches: list[Path],
                logs: list[Path],
                ssh_binary: Path,
                *,
                buffer_bytes: int,
                request_depth: int,
                bytes_per_session: int,
            ) -> SftpPhaseResult:
                del endpoint, logs, ssh_binary, buffer_bytes, request_depth
                if self.fail_transfer:
                    raise TimeoutError("injected transfer deadline")
                for batch in batches:
                    command = shlex.split(batch.read_text(encoding="utf-8"))
                    if command[0] == "put":
                        self.remote_files[command[2]] = Path(command[1]).read_bytes()
                    elif command[0] == "get":
                        Path(command[2]).write_bytes(self.remote_files[command[1]])
                sessions = tuple(
                    SftpSessionResult(index, bytes_per_session, 1000, 1.0)
                    for index in range(len(batches))
                )
                return SftpPhaseResult(
                    "close_ack",
                    len(batches) * bytes_per_session,
                    1000,
                    float(len(batches)),
                    sessions,
                )

        lifecycle = Lifecycle()
        scenario = RawSftpScenario(
            "raw-sftp-test",
            "small in-memory SFTP control",
            jobs=2,
            per_job_bytes=1_048_576,
            buffer_bytes=1_048_576,
            request_depth=128,
            repetitions=4,
        )
        result = InMemoryRaw(
            config,
            Runner(base_env={}),
            lifecycle,  # type: ignore[arg-type]
        ).run(
            scenario,
            stock_ssh=Path("/stock/ssh"),
            hpn_ssh=Path("/hpn/ssh"),
        )

        self.assertEqual(result.order, ("stock", "hpn", "hpn", "stock"))
        self.assertEqual(len(result.trials), 4)
        self.assertTrue(all(trial.sha256_verified for trial in result.trials))
        self.assertTrue(
            all(trial.remote_durability == "not_measured" for trial in result.trials)
        )
        self.assertEqual(lifecycle.stop_calls, 1)
        self.assertEqual(lifecycle.start_calls, 1)
        ledger = json.loads(
            (Path(result.receipt_dir) / "cleanup-ledger.json").read_text(
                encoding="utf-8"
            )
        )
        self.assertEqual(ledger["cleanup_attempts"], 2)
        self.assertTrue(ledger["asserted_clean"])
        self.assertEqual(ledger["active_remotes"], [])

        failed_lifecycle = Lifecycle()
        failed_config = replace(
            config,
            result_dir=Path(self.temp.name) / "failed-results",
        )
        failed = InMemoryRaw(
            failed_config,
            Runner(base_env={}),
            failed_lifecycle,  # type: ignore[arg-type]
        )
        failed.fail_create_ambiguously = True
        with self.assertRaisesRegex(RuntimeError, "ambiguous mkdir"):
            failed.run(
                scenario,
                stock_ssh=Path("/stock/ssh"),
                hpn_ssh=Path("/hpn/ssh"),
            )
        self.assertEqual(failed.remote_dirs, {"/prefix"})
        self.assertEqual(failed_lifecycle.start_calls, 1)

        timeout_lifecycle = Lifecycle()
        timeout_config = replace(
            config,
            result_dir=Path(self.temp.name) / "timeout-results",
        )
        timed_out = InMemoryRaw(
            timeout_config,
            Runner(base_env={}),
            timeout_lifecycle,  # type: ignore[arg-type]
        )
        timed_out.fail_transfer = True
        with self.assertRaisesRegex(TimeoutError, "transfer deadline"):
            timed_out.run(
                scenario,
                stock_ssh=Path("/stock/ssh"),
                hpn_ssh=Path("/hpn/ssh"),
            )
        self.assertEqual(timeout_lifecycle.start_calls, 1)
        manifests = list(timeout_config.result_dir.glob("*/manifest.json"))
        self.assertEqual(len(manifests), 1)
        self.assertEqual(
            json.loads(manifests[0].read_text(encoding="utf-8"))["status"],
            "failed",
        )


if __name__ == "__main__":
    unittest.main()
