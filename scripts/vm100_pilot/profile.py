from __future__ import annotations

import json
import os
import re
import select
import shutil
import socket
import tempfile
import time
import tomllib
import urllib.error
import urllib.request
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import IO, Protocol

from .benchmark import BenchmarkResult, BenchmarkRunner
from .config import PilotConfig
from .lifecycle import PilotLifecycle
from .owned_resources import atomic_write_json
from .receipts import RunReceipt
from .runner import ManagedProcess, Runner
from .system_io import file_sha256


_GC_CADENCE_KEYS = (
    "interval_secs",
    "idle_interval_secs",
    "busy_backlog_interval_secs",
)
_SYSTEMD_RUNTIME_DIR = Path("/run/systemd/system")
_SYSTEMD_SERVICE_NAME = re.compile(r"[A-Za-z0-9_.@:-]+\.service\Z")
_UNSAFE_SYSTEMD_ENVIRONMENT_PATH = re.compile(r'["\\\r\n%]')
_HOTPATH_RUNTIME_ATTEMPTS = 5
_HOTPATH_RUNTIME_TIMEOUT = 1.0
_HOTPATH_RUNTIME_MAX_BYTES = 1 << 20


class _NoRedirectHandler(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, request: object, *args: object) -> None:
        del request, args
        return None


@dataclass(slots=True)
class _LoopbackPortReservation:
    socket: socket.socket
    port: int

    @classmethod
    def reserve(cls) -> "_LoopbackPortReservation":
        listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 0)
            listener.bind(("127.0.0.1", 0))
            listener.listen(1)
            return cls(listener, int(listener.getsockname()[1]))
        except BaseException:
            listener.close()
            raise

    def release(self) -> None:
        self.socket.close()


@dataclass(slots=True)
class _PerfControlFifos:
    control_fifo: Path
    ack_fifo: Path
    _control_fd: int | None
    _ack_fd: int | None

    @classmethod
    def create(cls, directory: Path) -> "_PerfControlFifos":
        paths = (
            directory / "perf-control.fifo",
            directory / "perf-control-ack.fifo",
        )
        created: list[Path] = []
        descriptors: list[int] = []
        try:
            for path in paths:
                os.mkfifo(path, mode=0o600)
                created.append(path)
            flags = os.O_RDWR | os.O_NONBLOCK | os.O_CLOEXEC
            for path in paths:
                descriptors.append(os.open(path, flags))
            return cls(paths[0], paths[1], descriptors[0], descriptors[1])
        except BaseException:
            for descriptor in descriptors:
                os.close(descriptor)
            for path in created:
                path.unlink(missing_ok=True)
            raise

    def enable(self, *, timeout: float) -> None:
        if timeout <= 0:
            raise ValueError("perf readiness timeout must be positive")
        if self._control_fd is None or self._ack_fd is None:
            raise RuntimeError("perf control FIFOs are closed")
        try:
            os.write(self._control_fd, b"enable\n")
            deadline = time.monotonic() + timeout
            response = bytearray()
            while b"\n" not in response:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise TimeoutError("perf did not acknowledge enable")
                readable, _, _ = select.select([self._ack_fd], [], [], remaining)
                if not readable:
                    raise TimeoutError("perf did not acknowledge enable")
                chunk = os.read(self._ack_fd, 4096)
                if not chunk:
                    raise RuntimeError("perf acknowledgement FIFO closed")
                response.extend(chunk)
            if response.partition(b"\n")[0] != b"ack":
                raise RuntimeError("perf returned an invalid enable acknowledgement")
        except BaseException:
            self.close()
            raise

    def close(self) -> None:
        for attribute in ("_control_fd", "_ack_fd"):
            descriptor = getattr(self, attribute)
            if descriptor is not None:
                os.close(descriptor)
                setattr(self, attribute, None)
        self.control_fifo.unlink(missing_ok=True)
        self.ack_fifo.unlink(missing_ok=True)


def _perf_report_argv(
    perf_data: Path, *, time_range: str | None = None
) -> list[str | Path]:
    argv: list[str | Path] = [
        "perf",
        "report",
        "--stdio",
        "--no-children",
        "-g",
        "none",
        "--percent-limit",
        "0.1",
        "--sort",
        "comm,dso,symbol",
    ]
    if time_range is not None:
        argv.extend(("--time", time_range))
    argv.extend(("-i", perf_data))
    return argv


def _phase_perf_report_argv(
    perf_data: Path, start_ns: int, end_ns: int
) -> list[str | Path]:
    if end_ns <= start_ns:
        raise ValueError("perf phase must have a positive duration")
    start_seconds, start_fraction = divmod(start_ns, 1_000_000_000)
    end_seconds, end_fraction = divmod(end_ns, 1_000_000_000)
    return _perf_report_argv(
        perf_data,
        time_range=(
            f"{start_seconds}.{start_fraction:09d},{end_seconds}.{end_fraction:09d}"
        ),
    )


def _phase_report_text(stdout: str, stderr: str, returncode: int) -> str:
    if stdout.strip():
        return stdout + stderr
    detail = stderr.rstrip()
    suffix = f"{detail}\n" if detail else ""
    return f"status=insufficient_samples\nreturncode={returncode}\n{suffix}"


def _require_perf_data(path: Path) -> None:
    if not path.is_file() or path.stat().st_size == 0:
        raise RuntimeError(f"perf data is missing or empty: {path}")


def _perf_record_argv(
    pid: int,
    perf_data: Path,
    control_fifo: Path,
    ack_fifo: Path,
) -> list[str | Path]:
    return [
        "perf",
        "record",
        "-F",
        "99",
        "-g",
        "--call-graph",
        "fp",
        "--timestamp",
        "--clockid",
        "monotonic",
        "--delay=-1",
        "--control",
        f"fifo:{control_fifo},{ack_fifo}",
        "-p",
        str(pid),
        "-o",
        perf_data,
    ]


def _load_phase_windows(result: BenchmarkResult) -> dict[str, tuple[int, int]]:
    if not result.receipt_dir:
        return {}
    manifest = Path(result.receipt_dir) / "manifest.json"
    payload = json.loads(manifest.read_text(encoding="utf-8"))
    windows = payload.get("phase_monotonic_ns", {})
    parsed: dict[str, tuple[int, int]] = {}
    for name, window in windows.items():
        if not isinstance(name, str) or not isinstance(window, dict):
            raise RuntimeError("invalid benchmark phase window receipt")
        start_ns = window.get("start_ns")
        end_ns = window.get("end_ns")
        if not isinstance(start_ns, int) or not isinstance(end_ns, int):
            raise RuntimeError(f"invalid benchmark phase window: {name}")
        if end_ns <= start_ns:
            raise RuntimeError(f"non-positive benchmark phase window: {name}")
        parsed[name] = (start_ns, end_ns)
    return parsed


def rewrite_gc_cadence(text: str, interval_secs: int) -> str:
    """Set the three segment-GC cadence tiers without reformatting the config."""

    if not 300 <= interval_secs <= 86400:
        raise ValueError("maintenance GC interval must be between 300 and 86400")
    tomllib.loads(text)
    lines = text.splitlines(keepends=True)
    in_gc = False
    gc_header: int | None = None
    replaced: set[str] = set()
    assignments = {
        key: re.compile(rf"^(\s*{re.escape(key)}\s*=\s*)\d+(\s*(?:#.*)?(?:\r?\n)?)$")
        for key in _GC_CADENCE_KEYS
    }
    for index, line in enumerate(lines):
        stripped = line.strip()
        if stripped.startswith("[") and stripped.endswith("]"):
            in_gc = stripped == "[gc]"
            if in_gc:
                gc_header = index
            continue
        if not in_gc:
            continue
        for key, assignment in assignments.items():
            match = assignment.fullmatch(line)
            if match is None:
                continue
            lines[index] = f"{match.group(1)}{interval_secs}{match.group(2)}"
            replaced.add(key)
            break
    missing = [key for key in _GC_CADENCE_KEYS if key not in replaced]
    if missing:
        newline = "\r\n" if "\r\n" in text else "\n"
        additions = [
            f"{key} = {interval_secs}{newline}"
            for key in _GC_CADENCE_KEYS
            if key in missing
        ]
        if gc_header is not None:
            lines[gc_header + 1 : gc_header + 1] = additions
        else:
            if text.endswith(newline * 2):
                separator = ""
            elif text.endswith(newline):
                separator = newline
            else:
                separator = newline * 2
            lines.append(separator)
            lines.extend([f"[gc]{newline}", *additions])
    rewritten = "".join(lines)
    settings = tomllib.loads(rewritten)
    gc = settings.get("gc", {})
    if any(gc.get(key) != interval_secs for key in _GC_CADENCE_KEYS):
        raise RuntimeError("rewritten [gc] cadence did not validate")
    return rewritten


_sha256 = file_sha256


@dataclass(slots=True)
class CanonicalDeployment:
    config: PilotConfig
    runner: Runner
    directory: Path
    binary: Path
    receipt: Path
    binary_sha256: str
    receipt_sha256: str
    config_backup: Path
    config_sha256: str

    @classmethod
    def capture(cls, config: PilotConfig, runner: Runner) -> "CanonicalDeployment":
        directory = Path(
            tempfile.mkdtemp(prefix="zerofs-canonical-", dir=config.temp_dir)
        )
        config.require_temp_child(directory, "zerofs-canonical-")
        binary = directory / "zerofs"
        receipt = directory / "build-receipt"
        config_backup = config.config_file.with_name(
            f"{config.config_file.name}.profile-rollback-{uuid.uuid4().hex}"
        )
        backup_created = False
        try:
            shutil.copyfile(config.binary, binary)
            shutil.copyfile(config.build_receipt, receipt)
            runner.run(
                ["cp", "-aL", "--", config.config_file, config_backup], sudo=True
            )
            backup_created = True
            config_sha256 = cls._runner_sha256(runner, config.config_file)
            if cls._runner_sha256(runner, config_backup) != config_sha256:
                raise RuntimeError("canonical config backup hash mismatch")
            return cls(
                config=config,
                runner=runner,
                directory=directory,
                binary=binary,
                receipt=receipt,
                binary_sha256=_sha256(binary),
                receipt_sha256=_sha256(receipt),
                config_backup=config_backup,
                config_sha256=config_sha256,
            )
        except BaseException:
            if backup_created:
                runner.run(["rm", "-f", "--", config_backup], sudo=True, check=False)
            shutil.rmtree(directory, ignore_errors=True)
            raise

    @staticmethod
    def _runner_sha256(runner: Runner, path: Path) -> str:
        output = runner.run(["sha256sum", path], sudo=True, timeout=180).stdout
        return output.split()[0]

    def install_maintenance_config(self, interval_secs: int) -> str:
        original = self.runner.run(["cat", self.config_backup], sudo=True).stdout
        rewritten = rewrite_gc_cadence(original, interval_secs)
        with tempfile.NamedTemporaryFile(
            mode="w",
            prefix="zerofs-profile-config-",
            dir=self.config.temp_dir,
            delete=False,
        ) as handle:
            handle.write(rewritten)
            temporary = Path(handle.name)
        try:
            expected_sha256 = _sha256(temporary)
            self.runner.run(
                [
                    "install",
                    "-o",
                    "root",
                    "-g",
                    "root",
                    "-m",
                    "0600",
                    temporary,
                    self.config.config_file,
                ],
                sudo=True,
            )
        finally:
            temporary.unlink(missing_ok=True)
        installed_sha256 = self._runner_sha256(self.runner, self.config.config_file)
        if installed_sha256 != expected_sha256:
            raise RuntimeError("installed maintenance config hash mismatch")
        return installed_sha256

    def restore(self) -> None:
        self.runner.run(
            ["install", "-m", "0755", self.binary, self.config.binary], sudo=True
        )
        self.runner.run(
            [
                "install",
                "-o",
                "root",
                "-g",
                "root",
                "-m",
                "0644",
                self.receipt,
                self.config.build_receipt,
            ],
            sudo=True,
        )
        self.runner.run(
            ["cp", "-a", "--", self.config_backup, self.config.config_file],
            sudo=True,
        )
        if _sha256(self.config.binary) != self.binary_sha256:
            raise RuntimeError("restored canonical binary hash mismatch")
        if _sha256(self.config.build_receipt) != self.receipt_sha256:
            raise RuntimeError("restored canonical build receipt hash mismatch")
        if (
            self._runner_sha256(self.runner, self.config.config_file)
            != self.config_sha256
        ):
            raise RuntimeError("restored canonical config hash mismatch")

    def cleanup(self) -> None:
        self.runner.run(["rm", "-f", "--", self.config_backup], sudo=True)
        self.config.require_temp_child(self.directory, "zerofs-canonical-")
        shutil.rmtree(self.directory)


class _TemporaryHotpathEnvironment:
    """Own one service-scoped Hotpath environment drop-in for a profile run."""

    def __init__(
        self, config: PilotConfig, runner: Runner, report: Path, metrics_port: int
    ) -> None:
        if _SYSTEMD_SERVICE_NAME.fullmatch(config.service) is None:
            raise ValueError(f"unsafe systemd service name: {config.service!r}")
        if (
            not report.is_absolute()
            or report.name != "hotpath.json"
            or _UNSAFE_SYSTEMD_ENVIRONMENT_PATH.search(str(report)) is not None
        ):
            raise ValueError(f"unsafe Hotpath report path: {report}")
        if not 1 <= metrics_port <= 65535:
            raise ValueError(f"invalid Hotpath metrics port: {metrics_port}")
        self.config = config
        self.runner = runner
        self.report = report
        self.metrics_port = metrics_port
        self.directory = _SYSTEMD_RUNTIME_DIR / f"{config.service}.d"
        self.dropin = self.directory / f"zerofs-hotpath-profile-{uuid.uuid4().hex}.conf"
        self._directory_created = False
        self._cleanup_required = False

    def install(self) -> None:
        if (
            self.runner.run(
                ["test", "-e", self.dropin], sudo=True, check=False
            ).returncode
            == 0
        ):
            raise RuntimeError(f"Hotpath service drop-in already exists: {self.dropin}")
        directory_existed = (
            self.runner.run(
                ["test", "-d", self.directory], sudo=True, check=False
            ).returncode
            == 0
        )
        self._directory_created = not directory_existed
        if self._directory_created:
            self._cleanup_required = True
            self.runner.run(
                ["install", "-d", "-m", "0755", self.directory], sudo=True
            )
        content = (
            "[Service]\n"
            f'Environment="HOTPATH_OUTPUT_PATH={self.report}"\n'
            'Environment="HOTPATH_OUTPUT_FORMAT=json"\n'
            'Environment="HOTPATH_METRICS_SERVER_OFF=false"\n'
            f'Environment="HOTPATH_METRICS_PORT={self.metrics_port}"\n'
            'Environment="HOTPATH_CPU_BASELINE_OFF=true"\n'
            'Environment="HOTPATH_REPORT=functions-timing,futures,threads"\n'
        )
        temporary: Path | None = None
        handle = tempfile.NamedTemporaryFile(
            mode="w",
            prefix="zerofs-hotpath-profile-",
            dir=self.config.temp_dir,
            delete=False,
        )
        try:
            temporary = Path(handle.name)
            try:
                handle.write(content)
                handle.flush()
                os.fsync(handle.fileno())
            finally:
                handle.close()
        except BaseException:
            if temporary is not None:
                temporary.unlink(missing_ok=True)
            raise
        try:
            self._cleanup_required = True
            self.runner.run(
                [
                    "install",
                    "-o",
                    "root",
                    "-g",
                    "root",
                    "-m",
                    "0644",
                    temporary,
                    self.dropin,
                ],
                sudo=True,
            )
        finally:
            assert temporary is not None
            temporary.unlink(missing_ok=True)
        self.runner.run(["systemctl", "daemon-reload"], sudo=True)

    def cleanup(self) -> None:
        if not self._cleanup_required:
            return
        errors: list[BaseException] = []
        try:
            self.runner.run(["rm", "-f", "--", self.dropin], sudo=True)
        except BaseException as error:
            errors.append(error)
        if self._directory_created:
            try:
                self.runner.run(
                    ["rmdir", "--ignore-fail-on-non-empty", self.directory],
                    sudo=True,
                )
            except BaseException as error:
                errors.append(error)
        try:
            self.runner.run(["systemctl", "daemon-reload"], sudo=True)
        except BaseException as error:
            errors.append(error)
        if errors:
            raise RuntimeError(
                "Hotpath environment cleanup failures: "
                + "; ".join(str(error) for error in errors)
            )


def _require_hotpath_report(path: Path) -> dict[str, object]:
    if not path.is_file() or path.stat().st_size == 0:
        raise RuntimeError(f"Hotpath report is missing or empty: {path}")
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
        raise RuntimeError(f"Hotpath report contains invalid JSON: {path}") from error
    if not isinstance(payload, dict) or payload.get("type") != "hotpath_report":
        raise RuntimeError(f"Hotpath report has an invalid envelope: {path}")
    required = {"functions_timing", "futures", "threads"}
    if any(not isinstance(payload.get(key), dict) for key in required):
        raise RuntimeError(f"Hotpath report omits required sections: {path}")
    allowed = {
        "type",
        "label",
        "time_sampling",
        "functions_timing",
        "futures",
        "threads",
    }
    if not set(payload).issubset(allowed):
        raise RuntimeError(f"Hotpath report includes forbidden sections: {path}")
    return payload


def _require_hotpath_runtime(payload: object) -> dict[str, object]:
    if not isinstance(payload, dict):
        raise RuntimeError("Hotpath Tokio runtime response is not an object")
    required = ("num_workers", "num_alive_tasks", "global_queue_depth", "workers")
    if any(key not in payload for key in required):
        raise RuntimeError("Hotpath Tokio runtime response omits required fields")
    for key in required[:3]:
        if not isinstance(payload[key], int) or isinstance(payload[key], bool) or payload[key] < 0:
            raise RuntimeError(f"Hotpath Tokio runtime field is invalid: {key}")
    workers = payload["workers"]
    if (
        payload["num_workers"] <= 0
        or not isinstance(workers, list)
        or not workers
        or len(workers) != payload["num_workers"]
    ):
        raise RuntimeError("Hotpath Tokio runtime workers are invalid")
    indices: set[int] = set()
    for worker in workers:
        if not isinstance(worker, dict):
            raise RuntimeError("Hotpath Tokio runtime worker is invalid")
        for key in ("index", "park_count", "busy_duration_ms"):
            value = worker.get(key)
            if not isinstance(value, int) or isinstance(value, bool) or value < 0:
                raise RuntimeError(f"Hotpath Tokio runtime worker field is invalid: {key}")
        indices.add(worker["index"])
    if indices != set(range(payload["num_workers"])):
        raise RuntimeError("Hotpath Tokio runtime worker indices are invalid")
    return payload


def _fetch_hotpath_runtime(port: int) -> dict[str, object]:
    url = f"http://127.0.0.1:{port}/tokio_runtime"
    opener = urllib.request.build_opener(
        urllib.request.ProxyHandler({}), _NoRedirectHandler
    )
    errors: list[str] = []
    for attempt in range(_HOTPATH_RUNTIME_ATTEMPTS):
        try:
            with opener.open(url, timeout=_HOTPATH_RUNTIME_TIMEOUT) as response:
                if response.geturl() != url or response.getcode() != 200:
                    raise RuntimeError("Hotpath Tokio runtime response is not pinned")
                body = response.read(_HOTPATH_RUNTIME_MAX_BYTES + 1)
                if len(body) > _HOTPATH_RUNTIME_MAX_BYTES:
                    raise RuntimeError("Hotpath Tokio runtime response is too large")
                return _require_hotpath_runtime(json.loads(body.decode("utf-8")))
        except (OSError, UnicodeDecodeError, json.JSONDecodeError, RuntimeError, urllib.error.URLError) as error:
            errors.append(str(error))
            if isinstance(error, urllib.error.HTTPError):
                error.close()
            if attempt + 1 < _HOTPATH_RUNTIME_ATTEMPTS:
                time.sleep(0.1)
    raise RuntimeError("Hotpath Tokio runtime retrieval failed: " + "; ".join(errors))


class _Benchmark(Protocol):
    def run(
        self,
        *,
        total_mib: int,
        jobs: int,
        maintenance_isolated: bool = False,
    ) -> BenchmarkResult: ...


class CollectorGroup:
    _PERF_READY_TIMEOUT = 10.0

    def __init__(self, runner: Runner, receipt: RunReceipt, pid: int) -> None:
        self.runner = runner
        self.receipt = receipt
        self.pid = pid
        self.processes: list[tuple[str, ManagedProcess]] = []
        self.handles: list[IO[str]] = []
        self.perf_control: _PerfControlFifos | None = None
        self._stopped = False
        try:
            self._start()
        except BaseException as error:
            self._abort_start(error)
            raise

    def _abort_start(self, primary: BaseException) -> None:
        cleanup_errors: list[str] = []

        def interrupt_privileged_child(pid: int) -> None:
            self.runner.run(["kill", "-INT", str(pid)], sudo=True)

        for mode, process in reversed(self.processes):
            try:
                if mode == "interrupt":
                    process.interrupt_child(interrupt_privileged_child)
                else:
                    process.terminate()
            except BaseException as error:
                cleanup_errors.append(f"{' '.join(process.argv)}: {error}")
        for handle in self.handles:
            try:
                handle.close()
            except BaseException as error:
                cleanup_errors.append(f"collector output: {error}")
        if self.perf_control is not None:
            try:
                self.perf_control.close()
            except BaseException as error:
                cleanup_errors.append(f"perf control: {error}")
        if cleanup_errors:
            primary.add_note(
                "collector startup cleanup failures: " + "; ".join(cleanup_errors)
            )

    def _output(self, name: str) -> tuple[Path, IO[str]]:
        path = self.receipt.path(name)
        handle = path.open("w", encoding="utf-8")
        self.handles.append(handle)
        return path, handle

    def _start(self) -> None:
        proc_io = self.runner.run(
            ["cat", f"/proc/{self.pid}/io"], sudo=True, check=False
        ).stdout
        self.receipt.path("process-io-before.txt").write_text(proc_io, encoding="utf-8")
        proc_status = self.runner.run(
            ["cat", f"/proc/{self.pid}/status"], sudo=True, check=False
        ).stdout
        self.receipt.path("process-status-before.txt").write_text(
            proc_status, encoding="utf-8"
        )
        self.receipt.path("socket-summary-before.txt").write_text(
            self.runner.run(["ss", "-s"], check=False).stdout,
            encoding="utf-8",
        )

        perf_data = self.receipt.path("perf.data")
        self.perf_control = _PerfControlFifos.create(self.receipt.directory)
        perf_record = self.runner.spawn(
            _perf_record_argv(
                self.pid,
                perf_data,
                self.perf_control.control_fifo,
                self.perf_control.ack_fifo,
            ),
            sudo=True,
        )
        self.processes.append(("interrupt", perf_record))
        self.perf_control.enable(timeout=self._PERF_READY_TIMEOUT)

        perf_stat_path = self.receipt.path("perf-stat.txt")
        perf_stat = self.runner.spawn(
            [
                "perf",
                "stat",
                "-p",
                str(self.pid),
                "-o",
                perf_stat_path,
                "-e",
                "task-clock,cycles,instructions,cache-references,cache-misses,context-switches,cpu-migrations,page-faults",
            ],
            sudo=True,
        )
        self.processes.append(("interrupt", perf_stat))

        for name, argv in (
            (
                "pidstat.txt",
                ["pidstat", "-h", "-r", "-u", "-d", "-p", str(self.pid), "1"],
            ),
            ("iostat.txt", ["iostat", "-dxm", "1"]),
            ("network-sar.txt", ["sar", "-n", "DEV", "1"]),
        ):
            _, handle = self._output(name)
            process = self.runner.spawn(argv, stdout=handle, stderr=handle)
            self.processes.append(("terminate", process))

    def stop(self, phase_windows: dict[str, tuple[int, int]] | None = None) -> None:
        if self._stopped:
            return
        self._stopped = True
        errors: list[str] = []

        def interrupt_privileged_child(pid: int) -> None:
            self.runner.run(["kill", "-INT", str(pid)], sudo=True)

        for mode, process in reversed(self.processes):
            try:
                if mode == "interrupt":
                    process.interrupt_child(interrupt_privileged_child)
                else:
                    process.terminate()
            except BaseException as error:
                errors.append(f"{' '.join(process.argv)}: {error}")
        for handle in self.handles:
            try:
                handle.close()
            except BaseException as error:
                errors.append(f"collector output cleanup: {error}")
        if self.perf_control is not None:
            try:
                self.perf_control.close()
            except BaseException as error:
                errors.append(f"perf control cleanup: {error}")
        after = self.runner.run(
            ["cat", f"/proc/{self.pid}/io"], sudo=True, check=False
        ).stdout
        self.receipt.path("process-io-after.txt").write_text(after, encoding="utf-8")
        perf_data = self.receipt.directory / "perf.data"
        _require_perf_data(perf_data)
        report = self.runner.run(_perf_report_argv(perf_data), sudo=True)
        if not report.stdout.strip():
            raise RuntimeError("perf produced an empty aggregate report")
        self.receipt.path("perf-report.txt").write_text(
            report.stdout + report.stderr, encoding="utf-8"
        )
        for phase, (start_ns, end_ns) in (phase_windows or {}).items():
            phase_report = self.runner.run(
                _phase_perf_report_argv(perf_data, start_ns, end_ns),
                sudo=True,
                check=False,
            )
            self.receipt.path(f"perf-report-{phase}.txt").write_text(
                _phase_report_text(
                    phase_report.stdout,
                    phase_report.stderr,
                    phase_report.returncode,
                ),
                encoding="utf-8",
            )
        if errors:
            raise RuntimeError("collector cleanup failures: " + "; ".join(errors))


@dataclass(frozen=True, slots=True)
class ProfileResult:
    receipt_dir: str
    benchmark: dict[str, object]
    canonical_binary_restored: bool


class ProfileRunner:
    def __init__(
        self,
        config: PilotConfig,
        runner: Runner,
        lifecycle: PilotLifecycle,
        benchmark: _Benchmark | None = None,
    ) -> None:
        self.config = config
        self.runner = runner
        self.lifecycle = lifecycle
        self.benchmark = benchmark or BenchmarkRunner(config, runner, lifecycle)

    def _build_profile(self) -> Path:
        self.config.require_profile_target()
        env = {
            "CARGO_TARGET_DIR": str(self.config.profile_target),
            "CARGO_PROFILE_RELEASE_DEBUG": "1",
            "CARGO_PROFILE_RELEASE_STRIP": "false",
            "CARGO_INCREMENTAL": "0",
            "RUSTFLAGS": (
                "--cfg tokio_unstable --cfg io_uring_skip_arch_check "
                "-C force-frame-pointers=yes"
            ),
        }
        self.runner.run(
            [
                self.config.cargo,
                "build",
                "--release",
                "--locked",
                "--features",
                "hotpath-profile",
            ],
            cwd=self.config.crate,
            env=env,
            capture=False,
            timeout=self.config.profile_timeout,
        )
        binary = self.config.profile_target / "release" / "zerofs"
        sections = self.runner.run(["readelf", "-S", binary]).stdout
        if ".debug_info" not in sections:
            raise RuntimeError("profile binary has no .debug_info section")
        return binary

    def _checkout_commit(self) -> str:
        return self.runner.run(
            ["git", "rev-parse", "HEAD"], cwd=self.config.root
        ).stdout.strip()

    def _install_profile(self, binary: Path) -> None:
        self.runner.run(
            ["install", "-m", "0755", binary, self.config.binary], sudo=True
        )
        content = (
            f"commit={self._checkout_commit()}\n"
            f"binary_sha256={_sha256(binary)}\n"
            "profile=1\n"
        )
        with tempfile.NamedTemporaryFile(
            mode="w",
            prefix="zerofs-profile-receipt-",
            dir=self.config.temp_dir,
            delete=False,
        ) as handle:
            handle.write(content)
            receipt = Path(handle.name)
        try:
            self.runner.run(
                [
                    "install",
                    "-o",
                    "root",
                    "-g",
                    "root",
                    "-m",
                    "0644",
                    receipt,
                    self.config.build_receipt,
                ],
                sudo=True,
            )
        finally:
            receipt.unlink(missing_ok=True)

    def _service_pid(self) -> int:
        state = self.lifecycle.unit_state(self.config.service)
        if state.main_pid <= 0:
            raise RuntimeError("profile service has no MainPID")
        return state.main_pid

    def _start_collectors(self, pid: int, receipt: RunReceipt) -> CollectorGroup:
        return CollectorGroup(self.runner, receipt, pid)

    def _fetch_hotpath_runtime(self, port: int) -> dict[str, object]:
        return _fetch_hotpath_runtime(port)

    def run(self, *, total_mib: int = 256, jobs: int = 4) -> ProfileResult:
        self.lifecycle.status()
        self.lifecycle.drain()
        canonical = CanonicalDeployment.capture(self.config, self.runner)
        receipt = RunReceipt.start(self.config, "profile")
        collectors: CollectorGroup | object | None = None
        hotpath_environment: _TemporaryHotpathEnvironment | None = None
        hotpath_report: Path | None = None
        hotpath_runtime: Path | None = None
        hotpath_port: _LoopbackPortReservation | None = None
        deployed = False
        profile_service_started = False
        stop_attempted = False
        installation_started = False
        primary: BaseException | None = None
        benchmark_result: BenchmarkResult | None = None
        restored = False
        with receipt:
            try:
                binary = self._build_profile()
                receipt.record("profile_binary_sha256", _sha256(binary))
                receipt.record("profile_commit", self._checkout_commit())
                stop_attempted = True
                self.lifecycle.stop()
                installation_started = True
                self._install_profile(binary)
                deployed = True
                maintenance_config_sha256 = canonical.install_maintenance_config(
                    self.config.maintenance_isolation_secs
                )
                receipt.record(
                    "maintenance_isolation",
                    {
                        "interval_secs": self.config.maintenance_isolation_secs,
                        "canonical_config_sha256": canonical.config_sha256,
                        "temporary_config_sha256": maintenance_config_sha256,
                    },
                )
                hotpath_report = receipt.directory / "hotpath.json"
                hotpath_port = _LoopbackPortReservation.reserve()
                hotpath_environment = _TemporaryHotpathEnvironment(
                    self.config, self.runner, hotpath_report, hotpath_port.port
                )
                hotpath_environment.install()
                hotpath_port.release()
                self.lifecycle.start()
                profile_service_started = True
                profile_status = self.lifecycle.status(validate_data=True)
                receipt.record("profile_status", profile_status)
                self._fetch_hotpath_runtime(hotpath_port.port)
                pid = self._service_pid()
                receipt.record("profile_service_pid", pid)
                collectors = self._start_collectors(pid, receipt)
                benchmark_result = self.benchmark.run(
                    total_mib=total_mib,
                    jobs=jobs,
                    maintenance_isolated=True,
                )
                receipt.record("benchmark", benchmark_result.to_dict())
                phase_windows = _load_phase_windows(benchmark_result)
                collectors.stop(phase_windows=phase_windows)  # type: ignore[attr-defined]
                collectors = None
                hotpath_runtime = receipt.directory / "hotpath-tokio-runtime.json"
                runtime_payload = self._fetch_hotpath_runtime(hotpath_port.port)
                atomic_write_json(hotpath_runtime, runtime_payload)
                receipt.artifact("hotpath-tokio-runtime.json", hotpath_runtime)
                receipt.record(
                    "hotpath_tokio_runtime",
                    {"path": str(hotpath_runtime), "sha256": _sha256(hotpath_runtime)},
                )
            except BaseException as error:
                primary = error
            finally:
                cleanup_errors: list[BaseException] = []
                if hotpath_port is not None:
                    try:
                        hotpath_port.release()
                    except OSError:
                        pass
                if collectors is not None:
                    try:
                        phase_windows = (
                            _load_phase_windows(benchmark_result)
                            if benchmark_result is not None
                            else {}
                        )
                        collectors.stop(  # type: ignore[attr-defined]
                            phase_windows=phase_windows
                        )
                    except BaseException as error:
                        cleanup_errors.append(error)
                profile_service_stopped = not deployed
                if stop_attempted:
                    try:
                        if deployed:
                            self.lifecycle.stop()
                            profile_service_stopped = True
                        if profile_service_started and profile_service_stopped:
                            assert hotpath_report is not None
                            _require_hotpath_report(hotpath_report)
                            receipt.artifact("hotpath.json", hotpath_report)
                            receipt.record(
                                "hotpath_report",
                                {
                                    "path": str(hotpath_report),
                                    "sha256": _sha256(hotpath_report),
                                },
                            )
                    except BaseException as error:
                        cleanup_errors.append(error)
                    try:
                        if hotpath_environment is not None:
                            hotpath_environment.cleanup()
                    except BaseException as error:
                        cleanup_errors.append(error)
                    try:
                        if installation_started:
                            if deployed and not profile_service_stopped:
                                raise RuntimeError(
                                    "profile service did not stop; canonical deployment retained"
                                )
                            canonical.restore()
                        if installation_started:
                            self.lifecycle.start()
                            status = self.lifecycle.status(validate_data=True)
                            self.lifecycle.drain()
                            receipt.record("canonical_status", status)
                            restored = True
                    except BaseException as error:
                        cleanup_errors.append(error)
                if not installation_started or restored:
                    try:
                        canonical.cleanup()
                    except BaseException as error:
                        cleanup_errors.append(error)
                else:
                    receipt.record(
                        "retained_canonical_backup", str(canonical.directory)
                    )
                    receipt.record(
                        "retained_config_backup", str(canonical.config_backup)
                    )
                receipt.record("canonical_binary_restored", restored)
                if cleanup_errors:
                    if primary is not None:
                        primary.add_note(
                            "profile cleanup failures: "
                            + "; ".join(str(error) for error in cleanup_errors)
                        )
                    else:
                        primary = RuntimeError(
                            "profile cleanup failures: "
                            + "; ".join(str(error) for error in cleanup_errors)
                        )
            if primary is not None:
                raise primary
        if benchmark_result is None:
            raise RuntimeError("profile completed without benchmark result")
        result = ProfileResult(
            receipt_dir=str(receipt.directory),
            benchmark=benchmark_result.to_dict(),
            canonical_binary_restored=restored,
        )
        atomic_write_json(
            receipt.path("summary.json"),
            {
                    "receipt_dir": result.receipt_dir,
                    "benchmark": result.benchmark,
                    "canonical_binary_restored": result.canonical_binary_restored,
                },
        )
        return result
