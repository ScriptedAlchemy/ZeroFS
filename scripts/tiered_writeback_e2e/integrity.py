from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

from .config import (
    FILESYSTEM_ACK_MODES,
    OBJECT_ACK_MODES,
    AckModes,
    ConfigError,
    HarnessError,
)


class IntegrityError(HarnessError):
    pass


def canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)


def sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def chain_hash(previous: str, payload: Any) -> str:
    return sha256_text(previous + "\n" + canonical_json(payload))


def verify_copied_tree(source: Path, destination: Path) -> dict[str, str]:
    """Verify every file under ``source`` was copied byte-identically.

    Returns a relative-path -> sha256 map; raises IntegrityError on any
    missing or divergent file so callers refuse to discard the source.
    """
    source = Path(source)
    destination = Path(destination)
    hashes: dict[str, str] = {}
    for path in sorted(source.rglob("*")):
        if not path.is_file():
            continue
        relative = path.relative_to(source)
        copy = destination / relative
        if not copy.is_file():
            raise IntegrityError(f"archive copy missing: {copy}")
        expected = sha256_file(path)
        actual = sha256_file(copy)
        if expected != actual:
            raise IntegrityError(
                f"archive copy diverges for {relative}: "
                f"source {expected} != copy {actual}"
            )
        hashes[str(relative)] = expected
    if not hashes:
        raise IntegrityError(f"nothing to archive under {source}")
    return hashes


@dataclass(frozen=True, slots=True)
class DurabilityFloor:
    """The typed durability floor one acknowledged operation guarantees."""

    operation: str
    filesystem_floor: str
    object_floor: str

    def __post_init__(self) -> None:
        if self.filesystem_floor not in FILESYSTEM_ACK_MODES:
            raise ConfigError(
                f"unknown filesystem durability floor {self.filesystem_floor!r}"
            )
        if self.object_floor not in OBJECT_ACK_MODES:
            raise ConfigError(f"unknown object durability floor {self.object_floor!r}")

    def to_dict(self) -> dict[str, str]:
        return {
            "operation": self.operation,
            "filesystem_floor": self.filesystem_floor,
            "object_floor": self.object_floor,
        }


def floors_for(ack: AckModes, operations: Iterable[str]) -> tuple[DurabilityFloor, ...]:
    return tuple(
        DurabilityFloor(operation, ack.filesystem, ack.object)
        for operation in operations
    )
