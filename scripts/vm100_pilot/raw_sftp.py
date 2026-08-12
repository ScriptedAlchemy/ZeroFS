from __future__ import annotations

import json
import shutil
import tempfile
import time
import tomllib
import uuid
from dataclasses import asdict, dataclass
from pathlib import Path
from subprocess import CompletedProcess
from typing import IO
from urllib.parse import urlsplit

from .config import PilotConfig
from .lifecycle import PilotLifecycle
from .receipts import RunReceipt
from .runner import ManagedProcess, Runner


@dataclass(frozen=True, slots=True)
class SftpEndpoint:
    user: str
    host: str
    port: int
    identity_file: Path
    known_hosts: Path


@dataclass(frozen=True, slots=True)
class RawSftpResult:
    jobs: int
    total_bytes: int
    upload_ms: int
    download_ms: int
    upload_mibps: float
    download_mibps: float
    receipt_dir: str

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


def _rate(total_bytes: int, elapsed_ms: int) -> float:
    return (
        round(total_bytes / 1_048_576 / (elapsed_ms / 1000), 2) if elapsed_ms else 0.0
    )


class RawSftpRunner:
    def __init__(
        self, config: PilotConfig, runner: Runner, lifecycle: PilotLifecycle
    ) -> None:
        self.config = config
        self.runner = runner
        self.lifecycle = lifecycle

    def _endpoint(self) -> SftpEndpoint:
        settings = tomllib.loads(
            self.runner.run(["cat", self.config.config_file], sudo=True).stdout
        )
        storage = settings.get("storage", {})
        parsed = urlsplit(str(storage.get("url", settings.get("url", ""))))
        sftp = settings.get("sftp", {})
        if parsed.scheme != "sftp" or not parsed.username or not parsed.hostname:
            raise ValueError("pilot config does not contain a complete SFTP URL")
        identity = sftp.get("identity_file")
        known_hosts = sftp.get("known_hosts")
        if not identity or not known_hosts:
            raise ValueError(
                "pilot config must specify SFTP identity_file and known_hosts"
            )
        return SftpEndpoint(
            user=parsed.username,
            host=parsed.hostname,
            port=parsed.port or 22,
            identity_file=Path(identity),
            known_hosts=Path(known_hosts),
        )

    def _command(self, endpoint: SftpEndpoint, batch: Path) -> list[str | Path]:
        return [
            "sftp",
            "-q",
            "-B",
            "261120",
            "-R",
            "64",
            "-P",
            str(endpoint.port),
            "-i",
            endpoint.identity_file,
            "-o",
            f"UserKnownHostsFile={endpoint.known_hosts}",
            "-o",
            "StrictHostKeyChecking=yes",
            "-o",
            "BatchMode=yes",
            "-o",
            "Compression=no",
            "-b",
            batch,
            f"{endpoint.user}@{endpoint.host}",
        ]

    def _batch(self, directory: Path, name: str, lines: list[str]) -> Path:
        path = directory / name
        path.write_text("\n".join(lines) + "\n", encoding="utf-8")
        return path

    def _run_batch(
        self, endpoint: SftpEndpoint, batch: Path, *, check: bool = True
    ) -> CompletedProcess[str]:
        return self.runner.run(self._command(endpoint, batch), sudo=True, check=check)

    def _parallel_batches(
        self,
        endpoint: SftpEndpoint,
        batches: list[Path],
        logs: list[Path],
    ) -> None:
        processes: list[tuple[ManagedProcess, IO[str]]] = []
        failure: BaseException | None = None
        try:
            for batch, log in zip(batches, logs, strict=True):
                handle = log.open("w", encoding="utf-8")
                process = self.runner.spawn(
                    self._command(endpoint, batch),
                    sudo=True,
                    stdout=handle,
                    stderr=handle,
                )
                processes.append((process, handle))
            for process, _ in processes:
                returncode = process.process.wait()
                if returncode and failure is None:
                    failure = RuntimeError(
                        f"raw SFTP worker failed ({returncode}): {' '.join(process.argv)}"
                    )
        finally:
            for process, _ in processes:
                if process.process.poll() is None:
                    process.terminate()
            for _, handle in processes:
                handle.close()
        if failure is not None:
            raise failure

    def run(
        self, *, jobs: int | None = None, per_job_mib: int | None = None
    ) -> RawSftpResult:
        jobs = jobs or self.config.raw_sftp_jobs
        per_job_mib = per_job_mib or self.config.raw_sftp_per_job_mib
        if jobs <= 0 or per_job_mib <= 0:
            raise ValueError("raw SFTP jobs and per-job MiB must be positive")
        self.lifecycle.status()
        self.lifecycle.drain()
        endpoint = self._endpoint()
        receipt = RunReceipt.start(self.config, "raw-sftp")
        scratch = Path(
            tempfile.mkdtemp(prefix="zerofs-raw-sftp-", dir=self.config.temp_dir)
        )
        self.config.require_disposable(scratch)
        remote = f"zerofs-raw-control-{uuid.uuid4().hex}"
        remote_created = False
        stop_attempted = False
        primary: BaseException | None = None
        result: RawSftpResult | None = None
        with receipt:
            receipt.record("jobs", jobs)
            receipt.record("per_job_mib", per_job_mib)
            receipt.record("remote_directory", remote)
            try:
                files: list[Path] = []
                for index in range(jobs):
                    path = scratch / f"file-{index}.bin"
                    self.runner.run(
                        [
                            "dd",
                            "if=/dev/urandom",
                            f"of={path}",
                            "bs=1M",
                            f"count={per_job_mib}",
                            "status=none",
                        ]
                    )
                    files.append(path)
                stop_attempted = True
                self.lifecycle.stop()
                self._run_batch(
                    endpoint,
                    self._batch(scratch, "create.batch", [f"mkdir {remote}"]),
                )
                remote_created = True
                upload_batches = [
                    self._batch(
                        scratch,
                        f"upload-{index}.batch",
                        [f"put {path} {remote}/file-{index}.bin"],
                    )
                    for index, path in enumerate(files)
                ]
                upload_logs = [
                    receipt.path(f"upload-{index}.log") for index in range(jobs)
                ]
                started = time.monotonic_ns()
                self._parallel_batches(endpoint, upload_batches, upload_logs)
                upload_end = time.monotonic_ns()
                download_batches = [
                    self._batch(
                        scratch,
                        f"download-{index}.batch",
                        [f"get {remote}/file-{index}.bin /dev/null"],
                    )
                    for index in range(jobs)
                ]
                download_logs = [
                    receipt.path(f"download-{index}.log") for index in range(jobs)
                ]
                self._parallel_batches(endpoint, download_batches, download_logs)
                download_end = time.monotonic_ns()
                total_bytes = jobs * per_job_mib * 1_048_576
                upload_ms = max(1, round((upload_end - started) / 1_000_000))
                download_ms = max(1, round((download_end - upload_end) / 1_000_000))
                result = RawSftpResult(
                    jobs=jobs,
                    total_bytes=total_bytes,
                    upload_ms=upload_ms,
                    download_ms=download_ms,
                    upload_mibps=_rate(total_bytes, upload_ms),
                    download_mibps=_rate(total_bytes, download_ms),
                    receipt_dir=str(receipt.directory),
                )
                receipt.record("result", result.to_dict())
            except BaseException as error:
                primary = error
                raise
            finally:
                cleanup_errors: list[str] = []
                if remote_created:
                    lines = [f"rm {remote}/file-{index}.bin" for index in range(jobs)]
                    lines.append(f"rmdir {remote}")
                    try:
                        cleanup = self._run_batch(
                            endpoint,
                            self._batch(scratch, "cleanup.batch", lines),
                            check=False,
                        )
                        if cleanup.returncode:
                            cleanup_errors.append(
                                f"remote cleanup exited {cleanup.returncode}: {cleanup.stderr}"
                            )
                    except BaseException as error:
                        cleanup_errors.append(f"remote cleanup: {error}")
                shutil.rmtree(scratch, ignore_errors=True)
                if stop_attempted:
                    try:
                        self.lifecycle.start()
                        self.lifecycle.status()
                    except BaseException as error:
                        cleanup_errors.append(f"stack restore: {error}")
                if cleanup_errors:
                    if primary is not None:
                        primary.add_note(
                            "cleanup failures: " + "; ".join(cleanup_errors)
                        )
                    else:
                        raise RuntimeError("; ".join(cleanup_errors))
        if result is None:
            raise RuntimeError("raw SFTP control completed without a result")
        receipt.path("summary.json").write_text(
            json.dumps(result.to_dict(), indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        return result
