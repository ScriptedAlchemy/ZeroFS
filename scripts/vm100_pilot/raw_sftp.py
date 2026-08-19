from __future__ import annotations

import json
import os
import shlex
import time
import tomllib
import uuid
from dataclasses import asdict, dataclass
from pathlib import Path, PurePosixPath
from subprocess import CompletedProcess
from typing import IO
from urllib.parse import unquote, urlsplit

from .config import PilotConfig
from .lifecycle import PilotLifecycle
from .owned_resources import present, remove_tree
from .receipts import RunReceipt
from .runner import ManagedProcess, Runner
from .scenarios import RawSftpScenario
from .system_io import file_sha256


@dataclass(frozen=True, slots=True)
class SftpEndpoint:
    user: str
    host: str
    port: int
    identity_file: Path
    known_hosts: Path
    prefix: str = "."


@dataclass(frozen=True, slots=True)
class SshBinaryIdentity:
    path: str
    version: str
    sha256: str


@dataclass(frozen=True, slots=True)
class SftpEndpointAuthority:
    user: str
    host: str
    port: int
    prefix: str
    identity_file_path: str
    known_hosts_path: str
    known_hosts_sha256: str
    host_key_policy: str


@dataclass(frozen=True, slots=True)
class SftpSessionResult:
    index: int
    bytes: int
    elapsed_ms: int
    mibps: float


@dataclass(frozen=True, slots=True)
class SftpPhaseResult:
    cutoff: str
    total_bytes: int
    aggregate_ms: int
    aggregate_mibps: float
    sessions: tuple[SftpSessionResult, ...]


@dataclass(frozen=True, slots=True)
class SftpTrialResult:
    repetition: int
    variant: str
    ssh: SshBinaryIdentity
    upload_close_ack: SftpPhaseResult
    download_close_ack: SftpPhaseResult
    source_sha256: tuple[str, ...]
    download_sha256: tuple[str, ...]
    sha256_verified: bool
    remote_durability: str


@dataclass(frozen=True, slots=True)
class RawSftpResult:
    scenario: str
    jobs: int
    per_job_bytes: int
    buffer_bytes: int
    request_depth: int
    repetitions: int
    order: tuple[str, ...]
    endpoint: SftpEndpointAuthority
    stock_ssh: SshBinaryIdentity
    hpn_ssh: SshBinaryIdentity
    trials: tuple[SftpTrialResult, ...]
    cleanup_attempts: int
    cleanup_asserted: bool
    receipt_dir: str

    def to_dict(self) -> dict[str, object]:
        return {"schema": 2, **asdict(self)}


def _rate(total_bytes: int, elapsed_ms: int) -> float:
    return (
        round(total_bytes / 1_048_576 / (elapsed_ms / 1000), 3)
        if elapsed_ms
        else 0.0
    )


def counterbalanced_order(repetitions: int) -> tuple[str, ...]:
    if repetitions <= 0 or repetitions % 2:
        raise ValueError("raw SFTP repetitions must be a positive even number")
    order: list[str] = []
    for pair in range(repetitions // 2):
        order.extend(("stock", "hpn") if pair % 2 == 0 else ("hpn", "stock"))
    return tuple(order)


def identify_ssh_binary(path: Path, runner: Runner) -> SshBinaryIdentity:
    if not path.is_absolute():
        raise ValueError(f"SSH binary path must be absolute: {path}")
    resolved = path.resolve(strict=True)
    if not resolved.is_file() or not os.access(resolved, os.X_OK):
        raise ValueError(f"SSH binary is not an executable file: {resolved}")
    version_result = runner.run([resolved, "-V"], check=False, timeout=5)
    version_lines = [
        line.strip()
        for line in (version_result.stderr + "\n" + version_result.stdout).splitlines()
        if line.strip()
    ]
    if not version_lines:
        raise RuntimeError(f"SSH binary did not report a version: {resolved}")
    return SshBinaryIdentity(
        path=str(resolved),
        version=version_lines[0],
        sha256=file_sha256(resolved),
    )


def _remote_prefix(value: str) -> str:
    decoded = unquote(value) or "."
    path = PurePosixPath(decoded)
    if "\n" in decoded or "\r" in decoded or ".." in path.parts:
        raise ValueError(f"unsafe SFTP URL prefix: {decoded!r}")
    return str(path)


def _remote_child(prefix: str, name: str) -> str:
    if not name or "/" in name or name in {".", ".."}:
        raise ValueError(f"unsafe remote child name: {name!r}")
    return str(PurePosixPath(prefix) / name)


class RawSftpRunner:
    def __init__(
        self, config: PilotConfig, runner: Runner, lifecycle: PilotLifecycle
    ) -> None:
        self.config = config
        self.runner = runner
        self.lifecycle = lifecycle

    def _identify_binaries(
        self,
        stock_ssh: Path,
        hpn_ssh: Path,
    ) -> tuple[SshBinaryIdentity, SshBinaryIdentity]:
        stock = identify_ssh_binary(stock_ssh, self.runner)
        hpn = identify_ssh_binary(hpn_ssh, self.runner)
        if stock.path == hpn.path or stock.sha256 == hpn.sha256:
            raise ValueError("stock and HPN controls must use distinct SSH binaries")
        if "hpn" not in hpn.version.lower():
            raise ValueError(
                f"HPN control binary does not report an HPN version: {hpn.version!r}"
            )
        return stock, hpn

    def _create_sources(
        self,
        scratch: Path,
        *,
        jobs: int,
        per_job_bytes: int,
    ) -> list[Path]:
        per_job_mib = per_job_bytes // 1_048_576
        sources: list[Path] = []
        for index in range(jobs):
            path = scratch / f"source-{index}.bin"
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
            if path.stat().st_size != per_job_bytes:
                raise RuntimeError(
                    f"raw SFTP source size mismatch: {path.stat().st_size} "
                    f"!= {per_job_bytes}"
                )
            sources.append(path)
        return sources

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
            prefix=_remote_prefix(parsed.path),
        )

    @staticmethod
    def _endpoint_authority(endpoint: SftpEndpoint) -> SftpEndpointAuthority:
        if not endpoint.identity_file.is_absolute():
            raise ValueError(
                f"SFTP identity file path must be absolute: {endpoint.identity_file}"
            )
        if not endpoint.known_hosts.is_absolute():
            raise ValueError(
                f"SFTP known-hosts path must be absolute: {endpoint.known_hosts}"
            )
        identity = endpoint.identity_file.resolve(strict=True)
        known_hosts = endpoint.known_hosts.resolve(strict=True)
        if not identity.is_file() or not known_hosts.is_file():
            raise ValueError("SFTP identity and known-hosts authority must be files")
        return SftpEndpointAuthority(
            user=endpoint.user,
            host=endpoint.host,
            port=endpoint.port,
            prefix=endpoint.prefix,
            identity_file_path=str(identity),
            known_hosts_path=str(known_hosts),
            known_hosts_sha256=file_sha256(known_hosts),
            host_key_policy="strict-pinned-known-hosts",
        )

    @staticmethod
    def _command_for(
        endpoint: SftpEndpoint,
        batch: Path,
        ssh_binary: Path,
        *,
        buffer_bytes: int,
        request_depth: int,
    ) -> list[str | Path]:
        return [
            "sftp",
            "-q",
            "-B",
            str(buffer_bytes),
            "-R",
            str(request_depth),
            "-S",
            ssh_binary,
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

    def _command(
        self,
        endpoint: SftpEndpoint,
        batch: Path,
        ssh_binary: Path,
        *,
        buffer_bytes: int,
        request_depth: int,
    ) -> list[str | Path]:
        return self._command_for(
            endpoint,
            batch,
            ssh_binary,
            buffer_bytes=buffer_bytes,
            request_depth=request_depth,
        )

    @staticmethod
    def _batch(directory: Path, name: str, lines: list[str]) -> Path:
        path = directory / name
        path.write_text("\n".join(lines) + "\n", encoding="utf-8")
        return path

    def _run_batch(
        self,
        endpoint: SftpEndpoint,
        batch: Path,
        ssh_binary: Path,
        *,
        buffer_bytes: int,
        request_depth: int,
        check: bool = True,
    ) -> CompletedProcess[str]:
        return self.runner.run(
            self._command(
                endpoint,
                batch,
                ssh_binary,
                buffer_bytes=buffer_bytes,
                request_depth=request_depth,
            ),
            sudo=True,
            check=check,
        )

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
        processes: list[tuple[int, ManagedProcess, IO[str], int]] = []
        finished: dict[int, tuple[int, int]] = {}
        failure: BaseException | None = None
        phase_started = time.monotonic_ns()
        try:
            for index, (batch, log) in enumerate(zip(batches, logs, strict=True)):
                handle = log.open("w", encoding="utf-8")
                started = time.monotonic_ns()
                process = self.runner.spawn(
                    self._command(
                        endpoint,
                        batch,
                        ssh_binary,
                        buffer_bytes=buffer_bytes,
                        request_depth=request_depth,
                    ),
                    sudo=True,
                    stdout=handle,
                    stderr=handle,
                )
                processes.append((index, process, handle, started))
            pending = {index for index, _, _, _ in processes}
            while pending:
                for index, process, _, started in processes:
                    if index not in pending:
                        continue
                    returncode = process.process.poll()
                    if returncode is None:
                        continue
                    finished[index] = (
                        max(1, time.monotonic_ns() - started),
                        returncode,
                    )
                    pending.remove(index)
                    if returncode and failure is None:
                        failure = RuntimeError(
                            "raw SFTP worker failed "
                            f"({returncode}): {' '.join(process.argv)}"
                        )
                if failure is not None:
                    break
                if pending:
                    time.sleep(0.01)
        finally:
            for _, process, _, _ in processes:
                if process.process.poll() is None:
                    process.terminate()
            for _, _, handle, _ in processes:
                handle.close()
        if failure is not None:
            raise failure
        if len(finished) != len(batches):
            raise RuntimeError(
                f"raw SFTP phase completed {len(finished)} of {len(batches)} sessions"
            )
        phase_ms = max(1, round((time.monotonic_ns() - phase_started) / 1_000_000))
        sessions = tuple(
            SftpSessionResult(
                index=index,
                bytes=bytes_per_session,
                elapsed_ms=max(1, round(finished[index][0] / 1_000_000)),
                mibps=_rate(
                    bytes_per_session,
                    max(1, round(finished[index][0] / 1_000_000)),
                ),
            )
            for index in range(len(batches))
        )
        total_bytes = len(batches) * bytes_per_session
        return SftpPhaseResult(
            cutoff="close_ack",
            total_bytes=total_bytes,
            aggregate_ms=phase_ms,
            aggregate_mibps=_rate(total_bytes, phase_ms),
            sessions=sessions,
        )

    def _cleanup_remote(
        self,
        endpoint: SftpEndpoint,
        scratch: Path,
        remote: str,
        ssh_binary: Path,
        *,
        jobs: int,
        buffer_bytes: int,
        request_depth: int,
        label: str,
    ) -> None:
        lines = [
            f"-rm {shlex.quote(_remote_child(remote, f'file-{index}.bin'))}"
            for index in range(jobs)
        ]
        lines.append(f"-rmdir {shlex.quote(remote)}")
        for attempt in range(2):
            self._run_batch(
                endpoint,
                self._batch(scratch, f"cleanup-{label}-{attempt}.batch", lines),
                ssh_binary,
                buffer_bytes=buffer_bytes,
                request_depth=request_depth,
                check=False,
            )
        parent = str(PurePosixPath(remote).parent)
        parent_probe = self._run_batch(
            endpoint,
            self._batch(
                scratch,
                f"assert-parent-{label}.batch",
                [f"stat {shlex.quote(parent)}"],
            ),
            ssh_binary,
            buffer_bytes=buffer_bytes,
            request_depth=request_depth,
            check=False,
        )
        if parent_probe.returncode != 0:
            detail = (parent_probe.stderr + "\n" + parent_probe.stdout).strip()
            raise RuntimeError(
                f"cannot prove remote cleanup: parent {parent!r} is unreachable: "
                f"{detail}"
            )
        probe = self._run_batch(
            endpoint,
            self._batch(
                scratch,
                f"assert-absent-{label}.batch",
                [f"stat {shlex.quote(remote)}"],
            ),
            ssh_binary,
            buffer_bytes=buffer_bytes,
            request_depth=request_depth,
            check=False,
        )
        if probe.returncode == 0:
            raise RuntimeError(
                f"raw SFTP remote directory remains after cleanup: {remote}"
            )
        detail = (probe.stderr + "\n" + probe.stdout).strip()
        missing_markers = ("no such file", "not found", "does not exist")
        if not any(marker in detail.lower() for marker in missing_markers):
            raise RuntimeError(
                f"cannot prove remote directory absent: {remote}: {detail}"
            )

    @staticmethod
    def _cleanup_local(scratch: Path) -> None:
        remove_tree(scratch)

    def run(
        self,
        scenario: RawSftpScenario,
        *,
        stock_ssh: Path,
        hpn_ssh: Path,
    ) -> RawSftpResult:
        jobs = scenario.jobs
        per_job_bytes = scenario.per_job_bytes
        repetitions = scenario.repetitions
        buffer_bytes = scenario.buffer_bytes
        request_depth = scenario.request_depth
        order = counterbalanced_order(scenario.repetitions)
        receipt = RunReceipt.start(self.config, "raw-sftp-stock-hpn")
        scratch = self.config.temp_dir / f"zerofs-raw-sftp-{uuid.uuid4().hex}"
        self.config.require_temp_child(scratch, "zerofs-raw-sftp-")
        sources: list[Path] = []
        source_digests: tuple[str, ...] = ()
        active_remotes: list[tuple[str, Path, str]] = []
        remote_resources: list[str] = []
        stop_attempted = False
        cleanup_attempts = 0
        cleanup_asserted = False
        primary: BaseException | None = None
        trials: list[SftpTrialResult] = []
        ledger = receipt.path("cleanup-ledger.json")
        stock_identity: SshBinaryIdentity | None = None
        hpn_identity: SshBinaryIdentity | None = None
        endpoint: SftpEndpoint | None = None
        endpoint_authority: SftpEndpointAuthority | None = None

        def write_ledger() -> None:
            ledger.write_text(
                json.dumps(
                    {
                        "schema": 1,
                        "scratch": str(scratch),
                        "remote_resources": remote_resources,
                        "active_remotes": [item[0] for item in active_remotes],
                        "cleanup_attempts": cleanup_attempts,
                        "asserted_clean": cleanup_asserted,
                    },
                    indent=2,
                    sort_keys=True,
                )
                + "\n",
                encoding="utf-8",
            )

        with receipt:
            receipt.record("order", order)
            receipt.record("scenario", scenario.to_dict())
            receipt.record(
                "requested_ssh",
                {"stock": str(stock_ssh), "hpn": str(hpn_ssh)},
            )
            receipt.record(
                "geometry",
                {
                    "jobs": jobs,
                    "per_job_bytes": per_job_bytes,
                    "buffer_bytes": buffer_bytes,
                    "request_depth": request_depth,
                    "repetitions": repetitions,
                },
            )
            try:
                stock_identity, hpn_identity = self._identify_binaries(
                    stock_ssh, hpn_ssh
                )
                receipt.record("stock_ssh", asdict(stock_identity))
                receipt.record("hpn_ssh", asdict(hpn_identity))
                self.lifecycle.status()
                self.lifecycle.drain()
                endpoint = self._endpoint()
                endpoint_authority = self._endpoint_authority(endpoint)
                receipt.record("endpoint", asdict(endpoint_authority))
                write_ledger()
                scratch.mkdir(mode=0o700)
                sources = self._create_sources(
                    scratch,
                    jobs=jobs,
                    per_job_bytes=per_job_bytes,
                )
                source_digests = tuple(file_sha256(path) for path in sources)
                stop_attempted = True
                self.lifecycle.stop()
                identities = {"stock": stock_identity, "hpn": hpn_identity}
                for repetition, variant in enumerate(order):
                    identity = identities[variant]
                    ssh_binary = Path(identity.path)
                    remote = _remote_child(
                        endpoint.prefix,
                        f"zerofs-raw-control-{uuid.uuid4().hex}",
                    )
                    label = f"{repetition}-{variant}"
                    active_remotes.append((remote, ssh_binary, label))
                    remote_resources.append(remote)
                    write_ledger()
                    self._run_batch(
                        endpoint,
                        self._batch(
                            scratch,
                            f"create-{label}.batch",
                            [f"mkdir {shlex.quote(remote)}"],
                        ),
                        ssh_binary,
                        buffer_bytes=buffer_bytes,
                        request_depth=request_depth,
                    )
                    upload_batches = [
                        self._batch(
                            scratch,
                            f"upload-{label}-{index}.batch",
                            [
                                f"put {shlex.quote(str(path))} "
                                f"{shlex.quote(_remote_child(remote, f'file-{index}.bin'))}"
                            ],
                        )
                        for index, path in enumerate(sources)
                    ]
                    upload_logs = [
                        receipt.path(f"upload-{label}-{index}.log")
                        for index in range(jobs)
                    ]
                    upload = self._parallel_batches(
                        endpoint,
                        upload_batches,
                        upload_logs,
                        ssh_binary,
                        buffer_bytes=buffer_bytes,
                        request_depth=request_depth,
                        bytes_per_session=per_job_bytes,
                    )
                    downloads = [
                        scratch / f"download-{label}-{index}.bin"
                        for index in range(jobs)
                    ]
                    download_batches = [
                        self._batch(
                            scratch,
                            f"download-{label}-{index}.batch",
                            [
                                f"get {shlex.quote(_remote_child(remote, f'file-{index}.bin'))} "
                                f"{shlex.quote(str(downloads[index]))}"
                            ],
                        )
                        for index in range(jobs)
                    ]
                    download_logs = [
                        receipt.path(f"download-{label}-{index}.log")
                        for index in range(jobs)
                    ]
                    download = self._parallel_batches(
                        endpoint,
                        download_batches,
                        download_logs,
                        ssh_binary,
                        buffer_bytes=buffer_bytes,
                        request_depth=request_depth,
                        bytes_per_session=per_job_bytes,
                    )
                    download_digests = tuple(file_sha256(path) for path in downloads)
                    if download_digests != source_digests:
                        raise RuntimeError(
                            f"raw SFTP SHA-256 mismatch for trial {label}: "
                            f"source={source_digests}, download={download_digests}"
                        )
                    for path in downloads:
                        path.unlink()
                    trials.append(
                        SftpTrialResult(
                            repetition=repetition,
                            variant=variant,
                            ssh=identity,
                            upload_close_ack=upload,
                            download_close_ack=download,
                            source_sha256=source_digests,
                            download_sha256=download_digests,
                            sha256_verified=True,
                            remote_durability="not_measured",
                        )
                    )
                    self._cleanup_remote(
                        endpoint,
                        scratch,
                        remote,
                        ssh_binary,
                        jobs=jobs,
                        buffer_bytes=buffer_bytes,
                        request_depth=request_depth,
                        label=label,
                    )
                    active_remotes.remove((remote, ssh_binary, label))
                    write_ledger()
                    receipt.record("trials", [asdict(item) for item in trials])
            except BaseException as error:
                primary = error
                raise
            finally:
                cleanup_errors: list[str] = []
                for remote, ssh_binary, label in list(active_remotes):
                    if endpoint is None:
                        cleanup_errors.append(
                            f"remote cleanup {remote}: endpoint authority unavailable"
                        )
                        continue
                    try:
                        self._cleanup_remote(
                            endpoint,
                            scratch,
                            remote,
                            ssh_binary,
                            jobs=jobs,
                            buffer_bytes=buffer_bytes,
                            request_depth=request_depth,
                            label=f"final-{label}",
                        )
                        active_remotes.remove((remote, ssh_binary, label))
                    except BaseException as error:
                        cleanup_errors.append(f"remote cleanup {remote}: {error}")
                for _ in range(2):
                    cleanup_attempts += 1
                    try:
                        self._cleanup_local(scratch)
                    except BaseException as error:
                        cleanup_errors.append(f"local cleanup: {error}")
                    write_ledger()
                cleanup_asserted = not present(scratch) and not active_remotes
                if not cleanup_asserted:
                    cleanup_errors.append(
                        "raw SFTP cleanup assertion failed: "
                        f"scratch={present(scratch)}, remotes={active_remotes}"
                    )
                write_ledger()
                if stop_attempted:
                    try:
                        self.lifecycle.start()
                        self.lifecycle.status()
                        self.lifecycle.drain()
                    except BaseException as error:
                        cleanup_errors.append(f"stack restore: {error}")
                if cleanup_errors:
                    if primary is not None:
                        primary.add_note(
                            "cleanup failures: " + "; ".join(cleanup_errors)
                        )
                    else:
                        raise RuntimeError("; ".join(cleanup_errors))

        if stock_identity is None or hpn_identity is None or endpoint_authority is None:
            raise RuntimeError("raw SFTP completed without resolved authority")
        result = RawSftpResult(
            scenario=scenario.name,
            jobs=jobs,
            per_job_bytes=per_job_bytes,
            buffer_bytes=buffer_bytes,
            request_depth=request_depth,
            repetitions=repetitions,
            order=order,
            endpoint=endpoint_authority,
            stock_ssh=stock_identity,
            hpn_ssh=hpn_identity,
            trials=tuple(trials),
            cleanup_attempts=cleanup_attempts,
            cleanup_asserted=cleanup_asserted,
            receipt_dir=str(receipt.directory),
        )
        receipt.path("summary.json").write_text(
            json.dumps(result.to_dict(), indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        return result
