from __future__ import annotations

import os
import signal
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import IO, Mapping, Sequence


class CommandError(RuntimeError):
    def __init__(self, argv: Sequence[str], returncode: int, stderr: str = "") -> None:
        rendered = " ".join(argv)
        detail = f": {stderr.strip()}" if stderr.strip() else ""
        super().__init__(f"command failed ({returncode}): {rendered}{detail}")
        self.argv = tuple(argv)
        self.returncode = returncode
        self.stderr = stderr


@dataclass(slots=True)
class ManagedProcess:
    process: subprocess.Popen[str]
    argv: tuple[str, ...]

    def terminate(self, timeout: float = 10.0) -> None:
        if self.process.poll() is not None:
            return
        try:
            os.killpg(self.process.pid, signal.SIGTERM)
            self.process.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            os.killpg(self.process.pid, signal.SIGKILL)
            self.process.wait(timeout=timeout)

    def interrupt(self, timeout: float = 10.0) -> None:
        if self.process.poll() is not None:
            return
        try:
            os.killpg(self.process.pid, signal.SIGINT)
            self.process.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            self.terminate(timeout)


class Runner:
    def __init__(self, *, base_env: Mapping[str, str] | None = None) -> None:
        self.base_env = dict(base_env or os.environ)

    @staticmethod
    def _argv(argv: Sequence[str | Path], sudo: bool) -> list[str]:
        rendered = [str(value) for value in argv]
        return ["sudo", "--", *rendered] if sudo else rendered

    def run(
        self,
        argv: Sequence[str | Path],
        *,
        sudo: bool = False,
        timeout: float | None = None,
        capture: bool = True,
        check: bool = True,
        cwd: Path | None = None,
        env: Mapping[str, str] | None = None,
        input_text: str | None = None,
    ) -> subprocess.CompletedProcess[str]:
        command = self._argv(argv, sudo)
        process_env = self.base_env | dict(env or {})
        completed = subprocess.run(
            command,
            cwd=cwd,
            env=process_env,
            text=True,
            input=input_text,
            stdout=subprocess.PIPE if capture else None,
            stderr=subprocess.PIPE if capture else None,
            timeout=timeout,
            check=False,
        )
        if check and completed.returncode:
            raise CommandError(command, completed.returncode, completed.stderr or "")
        return completed

    def spawn(
        self,
        argv: Sequence[str | Path],
        *,
        sudo: bool = False,
        cwd: Path | None = None,
        env: Mapping[str, str] | None = None,
        stdout: IO[str] | int | None = None,
        stderr: IO[str] | int | None = None,
        stdin: int | IO[str] | None = None,
    ) -> ManagedProcess:
        command = self._argv(argv, sudo)
        process = subprocess.Popen(
            command,
            cwd=cwd,
            env=self.base_env | dict(env or {}),
            text=True,
            stdin=stdin,
            stdout=stdout,
            stderr=stderr,
            start_new_session=True,
        )
        return ManagedProcess(process=process, argv=tuple(command))
