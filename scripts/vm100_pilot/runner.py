from __future__ import annotations

import os
import signal
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import IO, Callable, Mapping, Sequence


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
            # Signal the supervisor once. In particular, sudo forwards SIGINT
            # to its child; signaling the whole process group would also hit
            # that child directly and can interrupt perf while it finalizes
            # its data header.
            self.process.send_signal(signal.SIGINT)
            self.process.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            self.terminate(timeout)

    def interrupt_child(
        self, signal_child: Callable[[int], None], timeout: float = 10.0
    ) -> None:
        """Interrupt one child and wait for its supervisor to reap it."""
        if self.process.poll() is not None:
            return
        children_path = Path(
            f"/proc/{self.process.pid}/task/{self.process.pid}/children"
        )
        try:
            children = [int(pid) for pid in children_path.read_text().split()]
        except (FileNotFoundError, PermissionError, ValueError):
            discovered = subprocess.run(
                ["ps", "-o", "pid=", "--ppid", str(self.process.pid)],
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.DEVNULL,
                check=False,
            )
            if discovered.returncode:
                discovered = subprocess.run(
                    ["pgrep", "-P", str(self.process.pid)],
                    text=True,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.DEVNULL,
                    check=False,
                )
            children = [int(pid) for pid in discovered.stdout.split()]
        if len(children) != 1:
            raise RuntimeError(
                f"expected one child for {' '.join(self.argv)}, found {children}"
            )
        signal_child(children[0])
        try:
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
