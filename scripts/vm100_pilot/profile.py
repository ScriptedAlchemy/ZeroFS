from __future__ import annotations

import hashlib
import json
import shutil
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import IO, Protocol

from .benchmark import BenchmarkResult, BenchmarkRunner
from .config import PilotConfig
from .lifecycle import PilotLifecycle
from .receipts import RunReceipt
from .runner import ManagedProcess, Runner


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


@dataclass(slots=True)
class CanonicalDeployment:
    config: PilotConfig
    runner: Runner
    directory: Path
    binary: Path
    receipt: Path
    binary_sha256: str
    receipt_sha256: str

    @classmethod
    def capture(cls, config: PilotConfig, runner: Runner) -> "CanonicalDeployment":
        directory = Path(
            tempfile.mkdtemp(prefix="zerofs-canonical-", dir=config.temp_dir)
        )
        config.require_disposable(directory)
        binary = directory / "zerofs"
        receipt = directory / "build-receipt"
        shutil.copyfile(config.binary, binary)
        shutil.copyfile(config.build_receipt, receipt)
        return cls(
            config=config,
            runner=runner,
            directory=directory,
            binary=binary,
            receipt=receipt,
            binary_sha256=_sha256(binary),
            receipt_sha256=_sha256(receipt),
        )

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
        if _sha256(self.config.binary) != self.binary_sha256:
            raise RuntimeError("restored canonical binary hash mismatch")
        if _sha256(self.config.build_receipt) != self.receipt_sha256:
            raise RuntimeError("restored canonical build receipt hash mismatch")

    def cleanup(self) -> None:
        self.config.require_disposable(self.directory)
        shutil.rmtree(self.directory)


class _Benchmark(Protocol):
    def run(self, *, total_mib: int, jobs: int) -> BenchmarkResult:
        ...


class CollectorGroup:
    def __init__(self, runner: Runner, receipt: RunReceipt, pid: int) -> None:
        self.runner = runner
        self.receipt = receipt
        self.pid = pid
        self.processes: list[tuple[str, ManagedProcess]] = []
        self.handles: list[IO[str]] = []
        self._stopped = False
        self._start()

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
        perf_record = self.runner.spawn(
            [
                "perf",
                "record",
                "-F",
                "99",
                "-g",
                "--call-graph",
                "dwarf",
                "-p",
                str(self.pid),
                "-o",
                perf_data,
            ],
            sudo=True,
        )
        self.processes.append(("interrupt", perf_record))

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

    def stop(self) -> None:
        if self._stopped:
            return
        self._stopped = True
        errors: list[str] = []
        for mode, process in reversed(self.processes):
            try:
                process.interrupt() if mode == "interrupt" else process.terminate()
            except BaseException as error:
                errors.append(f"{' '.join(process.argv)}: {error}")
        for handle in self.handles:
            handle.close()
        after = self.runner.run(
            ["cat", f"/proc/{self.pid}/io"], sudo=True, check=False
        ).stdout
        self.receipt.path("process-io-after.txt").write_text(after, encoding="utf-8")
        perf_data = self.receipt.directory / "perf.data"
        if perf_data.exists() and perf_data.stat().st_size:
            report = self.runner.run(
                [
                    "perf",
                    "report",
                    "--stdio",
                    "--no-children",
                    "--sort",
                    "comm,dso,symbol",
                    "-i",
                    perf_data,
                ],
                sudo=True,
                check=False,
            )
            self.receipt.path("perf-report.txt").write_text(
                report.stdout + report.stderr, encoding="utf-8"
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
        self.config.require_disposable(self.config.profile_target)
        if self.config.profile_target.exists():
            raise RuntimeError(
                f"profile target already exists; refusing replacement: {self.config.profile_target}"
            )
        env = {
            "CARGO_TARGET_DIR": str(self.config.profile_target),
            "CARGO_PROFILE_RELEASE_DEBUG": "1",
            "CARGO_PROFILE_RELEASE_STRIP": "false",
            "CARGO_INCREMENTAL": "0",
        }
        self.runner.run(
            [self.config.cargo, "build", "--release", "--locked"],
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

    def _remove_profile_target(self) -> None:
        self.config.require_disposable(self.config.profile_target)
        if self.config.profile_target.exists():
            shutil.rmtree(self.config.profile_target)

    def run(self, *, total_mib: int = 256, jobs: int = 4) -> ProfileResult:
        self.lifecycle.status()
        self.lifecycle.drain()
        canonical = CanonicalDeployment.capture(self.config, self.runner)
        receipt = RunReceipt.start(self.config, "profile")
        collectors: CollectorGroup | object | None = None
        deployed = False
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
                self.lifecycle.start()
                self.lifecycle.status()
                pid = self._service_pid()
                receipt.record("profile_service_pid", pid)
                collectors = self._start_collectors(pid, receipt)
                benchmark_result = self.benchmark.run(total_mib=total_mib, jobs=jobs)
                receipt.record("benchmark", benchmark_result.to_dict())
            except BaseException as error:
                primary = error
            finally:
                cleanup_errors: list[BaseException] = []
                if collectors is not None:
                    try:
                        collectors.stop()  # type: ignore[attr-defined]
                    except BaseException as error:
                        cleanup_errors.append(error)
                if stop_attempted:
                    try:
                        if deployed:
                            self.lifecycle.stop()
                        if installation_started:
                            canonical.restore()
                        self.lifecycle.start()
                        status = self.lifecycle.status()
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
                if not installation_started or restored:
                    try:
                        self._remove_profile_target()
                    except BaseException as error:
                        cleanup_errors.append(error)
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
        receipt.path("summary.json").write_text(
            json.dumps(
                {
                    "receipt_dir": result.receipt_dir,
                    "benchmark": result.benchmark,
                    "canonical_binary_restored": result.canonical_binary_restored,
                },
                indent=2,
                sort_keys=True,
            )
            + "\n",
            encoding="utf-8",
        )
        return result
