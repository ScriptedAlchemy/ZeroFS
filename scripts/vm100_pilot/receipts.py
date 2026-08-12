from __future__ import annotations

import json
import os
import tempfile
import traceback
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from .config import PilotConfig


def _utc_stamp() -> str:
    return datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")


class RunReceipt:
    def __init__(self, config: PilotConfig, command: str) -> None:
        config.result_dir.mkdir(parents=True, exist_ok=True)
        self.run_id = f"{command}-{_utc_stamp()}-{os.getpid()}"
        self.directory = config.result_dir / self.run_id
        self.directory.mkdir(mode=0o755)
        self.manifest = self.directory / "manifest.json"
        self.payload: dict[str, Any] = {
            "schema": 1,
            "run_id": self.run_id,
            "command": command,
            "status": "running",
            "started_at": datetime.now(UTC).isoformat(),
            "artifacts": {},
        }
        self._write()

    @classmethod
    def start(cls, config: PilotConfig, command: str) -> "RunReceipt":
        return cls(config, command)

    def __enter__(self) -> "RunReceipt":
        return self

    def __exit__(
        self, error_type: object, error: BaseException | None, tb: object
    ) -> bool:
        if error is None:
            self.finish("ok")
        else:
            self.payload["error"] = str(error)
            self.payload["traceback"] = "".join(
                traceback.format_exception(error_type, error, tb)  # type: ignore[arg-type]
            )
            self.finish("failed")
        return False

    def path(self, name: str) -> Path:
        if not name or Path(name).name != name:
            raise ValueError(f"artifact name must be a basename: {name!r}")
        path = self.directory / name
        self.artifact(name, path)
        return path

    def artifact(self, name: str, path: Path) -> None:
        self.payload["artifacts"][name] = str(path)
        self._write()

    def record(self, name: str, value: Any) -> None:
        self.payload[name] = value
        self._write()

    def finish(self, status: str) -> None:
        self.payload["status"] = status
        self.payload["finished_at"] = datetime.now(UTC).isoformat()
        self._write()

    def _write(self) -> None:
        self.directory.mkdir(parents=True, exist_ok=True)
        fd, temporary = tempfile.mkstemp(prefix=".manifest.", dir=self.directory)
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as handle:
                json.dump(self.payload, handle, indent=2, sort_keys=True)
                handle.write("\n")
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temporary, self.manifest)
        finally:
            if os.path.exists(temporary):
                os.unlink(temporary)
