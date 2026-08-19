from __future__ import annotations

import json
import os
import signal
import shlex
import time
import tomllib
import uuid
from dataclasses import asdict, dataclass
from pathlib import Path, PurePosixPath
from subprocess import PIPE, CompletedProcess, TimeoutExpired
from typing import IO
from urllib.parse import unquote, urlsplit

from .config import PilotConfig
from .lifecycle import PilotLifecycle
from .owned_resources import atomic_write_json, present, remove_tree
from .receipts import RunReceipt
from .runner import CommandError, ManagedProcess, Runner
from .scenarios import RawSftpScenario
from .system_io import file_sha256


def _stop_process_groups(
    processes: list[ManagedProcess],
    *,
    timeout: float,
    primary: BaseException | None,
) -> BaseException | None:
    def exists(process: ManagedProcess) -> bool:
        checker = getattr(process, "group_exists", None)
        return checker() if checker is not None else process.process.poll() is None

    active = [process for process in processes if exists(process)]
    if not active:
        return primary
    errors: list[str] = []
    started = time.monotonic()
    final_deadline = started + max(0.02, timeout)
    term_deadline = started + max(0.01, timeout / 2)

    for process in active:
        try:
            process.signal_group(signal.SIGTERM)
        except ProcessLookupError:
            pass
        except BaseException as error:
            errors.append(f"TERM {' '.join(process.argv)}: {error}")
    while time.monotonic() < term_deadline:
        active = [process for process in active if exists(process)]
        if not active:
            break
        time.sleep(0.01)

    for process in active:
        try:
            process.signal_group(signal.SIGKILL)
        except ProcessLookupError:
            pass
        except BaseException as error:
            errors.append(f"KILL {' '.join(process.argv)}: {error}")
    while time.monotonic() < final_deadline:
        active = [process for process in active if exists(process)]
        if not active:
            break
        time.sleep(0.01)
    for process in active:
        errors.append(f"process group remained alive: {' '.join(process.argv)}")

    if errors:
        detail = "raw SFTP process-group shutdown failed: " + "; ".join(errors)
        if primary is None:
            return RuntimeError(detail)
        primary.add_note(detail)
    return primary


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


@dataclass(slots=True)
class SftpTrialExecutor:
    owner: "RawSftpRunner"
    endpoint: SftpEndpoint
    receipt: RunReceipt
    scratch: Path
    jobs: int
    per_job_bytes: int
    buffer_bytes: int
    request_depth: int

    def run(
        self,
        *,
        repetition: int,
        variant: str,
        identity: SshBinaryIdentity,
        sources: list[Path],
        source_digests: tuple[str, ...],
        remote: str,
        label: str,
    ) -> SftpTrialResult:
        ssh_binary = Path(identity.path)
        self.owner._run_batch(
            self.endpoint,
            self.owner._batch(
                self.scratch,
                f"create-{label}.batch",
                [f"mkdir {shlex.quote(remote)}"],
            ),
            ssh_binary,
            buffer_bytes=self.buffer_bytes,
            request_depth=self.request_depth,
        )
        upload_batches = [
            self.owner._batch(
                self.scratch,
                f"upload-{label}-{index}.batch",
                [
                    f"put {shlex.quote(str(path))} "
                    f"{shlex.quote(_remote_child(remote, f'file-{index}.bin'))}"
                ],
            )
            for index, path in enumerate(sources)
        ]
        upload = self.owner._parallel_batches(
            self.endpoint,
            upload_batches,
            [
                self.receipt.path(f"upload-{label}-{index}.log")
                for index in range(self.jobs)
            ],
            ssh_binary,
            buffer_bytes=self.buffer_bytes,
            request_depth=self.request_depth,
            bytes_per_session=self.per_job_bytes,
        )
        downloads = [
            self.scratch / f"download-{label}-{index}.bin"
            for index in range(self.jobs)
        ]
        download_batches = [
            self.owner._batch(
                self.scratch,
                f"download-{label}-{index}.batch",
                [
                    f"get {shlex.quote(_remote_child(remote, f'file-{index}.bin'))} "
                    f"{shlex.quote(str(downloads[index]))}"
                ],
            )
            for index in range(self.jobs)
        ]
        download = self.owner._parallel_batches(
            self.endpoint,
            download_batches,
            [
                self.receipt.path(f"download-{label}-{index}.log")
                for index in range(self.jobs)
            ],
            ssh_binary,
            buffer_bytes=self.buffer_bytes,
            request_depth=self.request_depth,
            bytes_per_session=self.per_job_bytes,
        )
        download_digests = tuple(file_sha256(path) for path in downloads)
        if download_digests != source_digests:
            raise RuntimeError(
                f"raw SFTP SHA-256 mismatch for trial {label}: "
                f"source={source_digests}, download={download_digests}"
            )
        for path in downloads:
            path.unlink()
        result = SftpTrialResult(
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
        self.owner._cleanup_remote(
            self.endpoint,
            self.scratch,
            remote,
            ssh_binary,
            jobs=self.jobs,
            buffer_bytes=self.buffer_bytes,
            request_depth=self.request_depth,
            label=label,
        )
        return result


@dataclass(slots=True)
class OwnedSftpResources:
    scratch: Path
    ledger: Path
    active_remotes: list[tuple[str, Path, str]]
    remote_resources: list[str]
    cleanup_attempts: int = 0
    cleanup_asserted: bool = False

    @classmethod
    def create(cls, scratch: Path, ledger: Path) -> "OwnedSftpResources":
        return cls(scratch, ledger, [], [])

    def write(self) -> None:
        atomic_write_json(
            self.ledger,
            {
                    "schema": 1,
                    "scratch": str(self.scratch),
                    "remote_resources": self.remote_resources,
                    "active_remotes": [item[0] for item in self.active_remotes],
                    "cleanup_attempts": self.cleanup_attempts,
                    "asserted_clean": self.cleanup_asserted,
                },
        )

    def persist(self, errors: list[str]) -> None:
        try:
            self.write()
        except BaseException as error:
            errors.append(f"cleanup ledger persistence: {error}")

    def register_remote(self, remote: str, ssh_binary: Path, label: str) -> None:
        self.active_remotes.append((remote, ssh_binary, label))
        self.remote_resources.append(remote)
        self.write()

    def release_remote(self, remote: str, ssh_binary: Path, label: str) -> None:
        self.active_remotes.remove((remote, ssh_binary, label))
        self.write()

    def cleanup_local_twice(self, errors: list[str]) -> None:
        for _ in range(2):
            self.cleanup_attempts += 1
            try:
                remove_tree(self.scratch)
            except BaseException as error:
                errors.append(f"local cleanup: {error}")
            self.persist(errors)

    def assert_clean(self, errors: list[str]) -> None:
        self.cleanup_asserted = not present(self.scratch) and not self.active_remotes
        if not self.cleanup_asserted:
            errors.append(
                "raw SFTP cleanup assertion failed: "
                f"scratch={present(self.scratch)}, remotes={self.active_remotes}"
            )
        self.persist(errors)


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
        drain_timeout = config.drain_timeout
        stop_timeout = config.stop_timeout
        self.phase_timeout = (
            min(float(drain_timeout), 900.0)
            if isinstance(drain_timeout, (int, float))
            else 900.0
        )
        self.command_timeout = (
            min(float(stop_timeout), 60.0)
            if isinstance(stop_timeout, (int, float))
            else 60.0
        )

    def _identify_binaries(
        self,
        stock_ssh: Path,
        hpn_ssh: Path,
    ) -> tuple[SshBinaryIdentity, SshBinaryIdentity]:
        stock = identify_ssh_binary(stock_ssh, self.runner)
        hpn = identify_ssh_binary(hpn_ssh, self.runner)
        if stock.path == hpn.path or stock.sha256 == hpn.sha256:
            raise ValueError("stock and HPN controls must use distinct SSH binaries")
        if "hpn" in stock.version.lower():
            raise ValueError(
                f"stock SSH control reports HPN provenance: {stock.version!r}"
            )
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
        if (
            not identity.is_file()
            or not known_hosts.is_file()
            or not os.access(identity, os.R_OK)
            or not os.access(known_hosts, os.R_OK)
        ):
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
            "-F",
            "/dev/null",
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
            "IdentitiesOnly=yes",
            "-o",
            "PasswordAuthentication=no",
            "-o",
            "KbdInteractiveAuthentication=no",
            "-o",
            "ControlMaster=no",
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
        command = self._command(
                endpoint,
                batch,
                ssh_binary,
                buffer_bytes=buffer_bytes,
                request_depth=request_depth,
            )
        managed = self.runner.spawn(
            command,
            sudo=False,
            stdout=PIPE,
            stderr=PIPE,
        )
        try:
            stdout, stderr = managed.process.communicate(timeout=self.command_timeout)
        except TimeoutExpired as error:
            failure = TimeoutError(
                f"raw SFTP command exceeded {self.command_timeout}s deadline: "
                f"{' '.join(managed.argv)}"
            )
            failure.__cause__ = error
            _stop_process_groups(
                [managed], timeout=self.command_timeout, primary=failure
            )
            if managed.process.poll() is not None:
                managed.process.communicate()
            raise failure
        except BaseException as error:
            _stop_process_groups(
                [managed], timeout=self.command_timeout, primary=error
            )
            if managed.process.poll() is not None:
                managed.process.communicate()
            raise
        completed = CompletedProcess(
            managed.argv,
            managed.process.returncode,
            stdout or "",
            stderr or "",
        )
        if check and completed.returncode:
            raise CommandError(managed.argv, completed.returncode, completed.stderr)
        return completed

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
        deadline = time.monotonic() + self.phase_timeout
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
                    sudo=False,
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
                if time.monotonic() >= deadline:
                    failure = TimeoutError(
                        f"raw SFTP phase exceeded {self.phase_timeout}s deadline"
                    )
                    break
                if pending:
                    time.sleep(0.01)
        except BaseException as error:
            failure = error
        finally:
            failure = _stop_process_groups(
                [process for _, process, _, _ in processes],
                timeout=min(self.phase_timeout, 10.0),
                primary=failure,
            )
            close_errors: list[str] = []
            for _, _, handle, _ in processes:
                try:
                    handle.close()
                except BaseException as error:
                    close_errors.append(str(error))
            if close_errors:
                detail = "raw SFTP log close failed: " + "; ".join(close_errors)
                if failure is None:
                    failure = RuntimeError(detail)
                else:
                    failure.add_note(detail)
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
        cleanup_errors: list[str] = []
        for attempt in range(2):
            try:
                self._run_batch(
                    endpoint,
                    self._batch(scratch, f"cleanup-{label}-{attempt}.batch", lines),
                    ssh_binary,
                    buffer_bytes=buffer_bytes,
                    request_depth=request_depth,
                    check=False,
                )
            except BaseException as error:
                cleanup_errors.append(f"attempt {attempt + 1}: {error}")
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
        if cleanup_errors:
            raise RuntimeError(
                "remote cleanup attempts failed despite final absence proof: "
                + "; ".join(cleanup_errors)
            )

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
        sources: list[Path] = []
        source_digests: tuple[str, ...] = ()
        stop_attempted = False
        primary: BaseException | None = None
        trials: list[SftpTrialResult] = []
        ledger = receipt.directory / "cleanup-ledger.json"
        owned = OwnedSftpResources.create(scratch, ledger)
        stock_identity: SshBinaryIdentity | None = None
        hpn_identity: SshBinaryIdentity | None = None
        endpoint: SftpEndpoint | None = None
        endpoint_authority: SftpEndpointAuthority | None = None

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
                receipt.artifact("cleanup-ledger.json", ledger)
                self.config.require_temp_child(scratch, "zerofs-raw-sftp-")
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
                owned.write()
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
                trial_executor = SftpTrialExecutor(
                    self,
                    endpoint,
                    receipt,
                    scratch,
                    jobs,
                    per_job_bytes,
                    buffer_bytes,
                    request_depth,
                )
                for repetition, variant in enumerate(order):
                    identity = identities[variant]
                    ssh_binary = Path(identity.path)
                    remote = _remote_child(
                        endpoint.prefix,
                        f"zerofs-raw-control-{uuid.uuid4().hex}",
                    )
                    label = f"{repetition}-{variant}"
                    owned.register_remote(remote, ssh_binary, label)
                    trials.append(
                        trial_executor.run(
                            repetition=repetition,
                            variant=variant,
                            identity=identity,
                            sources=sources,
                            source_digests=source_digests,
                            remote=remote,
                            label=label,
                        )
                    )
                    owned.release_remote(remote, ssh_binary, label)
                    receipt.record("trials", [asdict(item) for item in trials])
            except BaseException as error:
                primary = error
                raise
            finally:
                cleanup_errors: list[str] = []
                for remote, ssh_binary, label in list(owned.active_remotes):
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
                        owned.release_remote(remote, ssh_binary, label)
                    except BaseException as error:
                        cleanup_errors.append(f"remote cleanup {remote}: {error}")
                owned.cleanup_local_twice(cleanup_errors)
                owned.assert_clean(cleanup_errors)
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

            if (
                stock_identity is None
                or hpn_identity is None
                or endpoint_authority is None
            ):
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
                cleanup_attempts=owned.cleanup_attempts,
                cleanup_asserted=owned.cleanup_asserted,
                receipt_dir=str(receipt.directory),
            )
            receipt.path("summary.json").write_text(
                json.dumps(result.to_dict(), indent=2, sort_keys=True) + "\n",
                encoding="utf-8",
            )
        return result
