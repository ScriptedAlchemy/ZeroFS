from __future__ import annotations

import json
import os
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Sequence

from .config import PilotConfig
from .lifecycle import PilotLifecycle
from .receipts import RunReceipt
from .runner import Runner


@dataclass(frozen=True, slots=True)
class PhaseTiming:
    foreground_ms: int
    local_sync_ms: int
    remote_tail_ms: int

@dataclass(frozen=True, slots=True)
class WorkloadResult:
    npm_clone_ms: int
    npm_cold: PhaseTiming
    npm_serial_delete: PhaseTiming
    npm_warm: PhaseTiming
    npm_parallel_delete: PhaseTiming
    cargo_clone_ms: int
    cargo_cold: PhaseTiming
    cargo_noop_ms: int
    cargo_incremental: PhaseTiming
    cleanup_ms: int
    receipt_dir: str

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


def _elapsed(start: int) -> int:
    return max(1, round((time.monotonic_ns() - start) / 1_000_000))


class WorkloadRunner:
    def __init__(
        self, config: PilotConfig, runner: Runner, lifecycle: PilotLifecycle
    ) -> None:
        self.config = config
        self.runner = runner
        self.lifecycle = lifecycle

    def _prepare_root(self, root: Path) -> None:
        self.config.require_disposable(root)
        if root.parent.resolve(strict=False) != self.config.mountpoint.resolve(
            strict=False
        ):
            raise ValueError("workload root must be a direct mount child")
        self.runner.run(
            [
                "install",
                "-d",
                "-m",
                "0755",
                "-o",
                self.config.user,
                "-g",
                self.config.group,
                root,
            ],
            sudo=True,
        )

    def _clone_pinned(self, repository: str, commit: str, destination: Path) -> int:
        started = time.monotonic_ns()
        self.runner.run(["git", "init", "-q", destination])
        self.runner.run(
            ["git", "-C", destination, "remote", "add", "origin", repository]
        )
        self.runner.run(
            ["git", "-C", destination, "fetch", "-q", "--depth", "1", "origin", commit]
        )
        self.runner.run(
            ["git", "-C", destination, "checkout", "-q", "--detach", "FETCH_HEAD"]
        )
        actual = self.runner.run(
            ["git", "-C", destination, "rev-parse", "HEAD"]
        ).stdout.strip()
        if actual != commit:
            raise RuntimeError(f"pinned checkout mismatch: {actual} != {commit}")
        return _elapsed(started)

    def _durable_phase(
        self,
        argv: Sequence[str | Path],
        *,
        cwd: Path | None = None,
        log: Path | None = None,
    ) -> PhaseTiming:
        started = time.monotonic_ns()
        completed = self.runner.run(argv, cwd=cwd)
        foreground = _elapsed(started)
        if log is not None:
            with log.open("a", encoding="utf-8") as handle:
                handle.write(completed.stdout or "")
                handle.write(completed.stderr or "")
        sync_started = time.monotonic_ns()
        self.runner.run(["sync", "-f", self.config.mountpoint], sudo=True)
        local_sync = _elapsed(sync_started)
        remote_started = time.monotonic_ns()
        self.lifecycle.drain()
        remote_tail = _elapsed(remote_started)
        return PhaseTiming(foreground, local_sync, remote_tail)

    def _parallel_delete(self, directory: Path, jobs: int) -> None:
        children = [Path(entry.path) for entry in os.scandir(directory)]
        with ThreadPoolExecutor(max_workers=jobs) as executor:
            futures = [
                executor.submit(self.runner.run, ["rm", "-rf", "--", child])
                for child in children
            ]
            for future in futures:
                future.result()
        directory.rmdir()

    def _cleanup(self, root: Path) -> int:
        started = time.monotonic_ns()
        self.runner.run(["rm", "-rf", "--", root], sudo=True)
        self.runner.run(["sync", "-f", self.config.mountpoint], sudo=True)
        self.lifecycle.drain()
        remains = self.runner.run(["test", "-e", root], sudo=True, check=False)
        if remains.returncode == 0:
            raise RuntimeError(f"workload root remains after cleanup: {root}")
        return _elapsed(started)

    def run(self, *, delete_jobs: int | None = None) -> WorkloadResult:
        delete_jobs = delete_jobs or self.config.delete_jobs
        if delete_jobs <= 0:
            raise ValueError("delete jobs must be positive")
        self.lifecycle.status()
        self.lifecycle.drain()
        root = self.config.mountpoint / f".zerofs-workloads-{uuid.uuid4().hex}"
        receipt = RunReceipt.start(self.config, "workloads")
        result: WorkloadResult | None = None
        cleaned = False
        primary: BaseException | None = None
        with receipt:
            receipt.record("root", str(root))
            receipt.record("delete_jobs", delete_jobs)
            try:
                self._prepare_root(root)
                npm_log = receipt.path("npm.log")
                cargo_log = receipt.path("cargo.log")
                npm_root = root / "npm-cli"
                npm_clone = self._clone_pinned(
                    self.config.npm_repo, self.config.npm_commit, npm_root
                )
                self.runner.run(["sync", "-f", self.config.mountpoint], sudo=True)
                self.lifecycle.drain()
                npm_argv = ["npm", "ci", "--ignore-scripts", "--no-audit", "--no-fund"]
                npm_cold = self._durable_phase(npm_argv, cwd=npm_root, log=npm_log)
                serial_delete = self._durable_phase(
                    ["rm", "-rf", "--", npm_root / "node_modules"]
                )
                npm_warm = self._durable_phase(npm_argv, cwd=npm_root, log=npm_log)
                delete_started = time.monotonic_ns()
                self._parallel_delete(npm_root / "node_modules", delete_jobs)
                parallel_foreground = _elapsed(delete_started)
                sync_started = time.monotonic_ns()
                self.runner.run(["sync", "-f", self.config.mountpoint], sudo=True)
                parallel_sync = _elapsed(sync_started)
                remote_started = time.monotonic_ns()
                self.lifecycle.drain()
                parallel_delete = PhaseTiming(
                    parallel_foreground, parallel_sync, _elapsed(remote_started)
                )

                rust_root = root / "ripgrep"
                cargo_clone = self._clone_pinned(
                    self.config.rust_repo, self.config.rust_commit, rust_root
                )
                self.runner.run(["sync", "-f", self.config.mountpoint], sudo=True)
                self.lifecycle.drain()
                cargo_argv: list[str | Path] = [self.config.cargo, "build", "--locked"]
                cargo_cold = self._durable_phase(
                    cargo_argv, cwd=rust_root, log=cargo_log
                )
                noop_started = time.monotonic_ns()
                completed = self.runner.run(cargo_argv, cwd=rust_root)
                cargo_noop = _elapsed(noop_started)
                cargo_log.write_text(
                    cargo_log.read_text(encoding="utf-8")
                    + (completed.stdout or "")
                    + (completed.stderr or ""),
                    encoding="utf-8",
                )
                source = next(rust_root.rglob("*.rs"))
                source.touch()
                cargo_incremental = self._durable_phase(
                    cargo_argv, cwd=rust_root, log=cargo_log
                )
                cleanup_ms = self._cleanup(root)
                cleaned = True
                result = WorkloadResult(
                    npm_clone_ms=npm_clone,
                    npm_cold=npm_cold,
                    npm_serial_delete=serial_delete,
                    npm_warm=npm_warm,
                    npm_parallel_delete=parallel_delete,
                    cargo_clone_ms=cargo_clone,
                    cargo_cold=cargo_cold,
                    cargo_noop_ms=cargo_noop,
                    cargo_incremental=cargo_incremental,
                    cleanup_ms=cleanup_ms,
                    receipt_dir=str(receipt.directory),
                )
                receipt.record("result", result.to_dict())
            except BaseException as error:
                primary = error
                raise
            finally:
                if not cleaned:
                    try:
                        self._cleanup(root)
                    except BaseException as cleanup_error:
                        if primary is not None:
                            primary.add_note(
                                f"workload cleanup failed: {cleanup_error}"
                            )
                        else:
                            raise
        if result is None:
            raise RuntimeError("workloads completed without a result")
        receipt.path("summary.json").write_text(
            json.dumps(result.to_dict(), indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        return result
