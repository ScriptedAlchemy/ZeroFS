from __future__ import annotations

import json
import os
import re
import tempfile
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any, Mapping

from .config import (
    DEFAULT_ROOT_PARENT,
    HarnessError,
    UnsafeCleanupTarget,
    backend_prefix_for,
    validate_owned_path,
    validate_run_roots,
)
from .integrity import canonical_json, chain_hash, sha256_text

if TYPE_CHECKING:
    from .config import HarnessConfig

SCHEMA = 1
RESOURCE_KINDS = (
    "unit",
    "process",
    "listener",
    "mount",
    "device",
    "pool",
    "prefix",
    "path",
)

_NBD_DEVICE = re.compile(r"/dev/nbd(?:0|[1-9][0-9]*)\Z")
_SYSTEM_PORT_CEILING = 1024


class LedgerError(HarnessError):
    pass


class LedgerIntegrityError(LedgerError):
    pass


class UnownedResourceError(LedgerError):
    pass


def _utc_now() -> str:
    return datetime.now(UTC).isoformat()


class ResourceLedger:
    """Append-only, hash-chained record of everything one run owns.

    The identity block (run UUID, roots, ack modes, source/binary/config
    hashes) is written once and anchors the event chain; any later mutation of
    identity or a past event breaks verification on load. Events record
    resource acquisition and release plus lifecycle milestones, and cleanup
    authority derives exclusively from this file.
    """

    def __init__(
        self,
        run_id: object,
        control_root: object,
        resource_root: object,
        *,
        identity: Mapping[str, Any] | None = None,
        events: list[dict[str, Any]] | None = None,
        path: Path | None = None,
    ) -> None:
        if identity is None:
            identity = {
                "schema": SCHEMA,
                "run_uuid": str(run_id),
                "control_root": str(control_root),
                "resource_root": str(resource_root),
                "root_parent": str(DEFAULT_ROOT_PARENT),
                "workspace_root": None,
                "created_at": _utc_now(),
            }
        self.identity: dict[str, Any] = dict(identity)
        self.events: list[dict[str, Any]] = list(events or [])
        self.path = path

    @classmethod
    def create(
        cls,
        config: "HarnessConfig",
        *,
        source_sha: str,
        binary_sha256: str,
        config_sha256: str,
    ) -> "ResourceLedger":
        identity = {
            "schema": SCHEMA,
            "run_uuid": config.run_uuid,
            "control_root": str(config.control_root),
            "resource_root": str(config.resource_root),
            "root_parent": str(config.root_parent),
            "workspace_root": (
                str(config.workspace_root) if config.workspace_root else None
            ),
            "source_root": str(config.source_root),
            "filesystem_ack_mode": config.ack.filesystem,
            "object_ack_mode": config.ack.object,
            "backend_prefix": config.backend_prefix,
            "source_sha": source_sha,
            "binary_sha256": binary_sha256,
            "config_sha256": config_sha256,
            "created_at": _utc_now(),
        }
        ledger = cls(
            config.run_uuid,
            config.control_root,
            config.resource_root,
            identity=identity,
            path=config.ledger_path,
        )
        ledger.save()
        return ledger

    @classmethod
    def load(cls, path: Path) -> "ResourceLedger":
        path = Path(path)
        try:
            document = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as error:
            raise LedgerIntegrityError(f"unreadable ledger {path}: {error}") from error
        identity = document.get("identity")
        if not isinstance(identity, dict):
            raise LedgerIntegrityError(f"ledger {path} has no identity block")
        ledger = cls(
            identity.get("run_uuid"),
            identity.get("control_root"),
            identity.get("resource_root"),
            identity=identity,
            events=list(document.get("events", [])),
            path=path,
        )
        stored_anchor = document.get("identity_hash")
        if stored_anchor != ledger.identity_hash():
            raise LedgerIntegrityError(
                f"ledger {path}: identity block was mutated after creation"
            )
        ledger.verify_chain()
        return ledger

    @property
    def run_uuid(self) -> str:
        return str(self.identity.get("run_uuid"))

    @property
    def control_root(self) -> Path:
        return Path(str(self.identity.get("control_root")))

    @property
    def resource_root(self) -> Path:
        return Path(str(self.identity.get("resource_root")))

    def identity_hash(self) -> str:
        return sha256_text(canonical_json(self.identity))

    def verify_chain(self) -> None:
        previous = self.identity_hash()
        for index, event in enumerate(self.events):
            body = {
                "index": event.get("index"),
                "kind": event.get("kind"),
                "payload": event.get("payload"),
                "recorded_at": event.get("recorded_at"),
            }
            if event.get("index") != index:
                raise LedgerIntegrityError(
                    f"ledger event {index} has index {event.get('index')}"
                )
            if event.get("previous") != previous:
                raise LedgerIntegrityError(f"ledger event {index} breaks the chain")
            if event.get("hash") != chain_hash(previous, body):
                raise LedgerIntegrityError(f"ledger event {index} was rewritten")
            previous = str(event.get("hash"))

    def append(self, kind: str, payload: Mapping[str, Any]) -> dict[str, Any]:
        previous = self.events[-1]["hash"] if self.events else self.identity_hash()
        body = {
            "index": len(self.events),
            "kind": kind,
            "payload": dict(payload),
            "recorded_at": _utc_now(),
        }
        event = dict(body, previous=previous, hash=chain_hash(previous, body))
        self.events.append(event)
        self.save()
        return event

    def record_event(self, kind: str, payload: Mapping[str, Any] | None = None) -> None:
        self.append(kind, payload or {})

    def _validate_resource(self, kind: str, value: Any) -> Any:
        if kind not in RESOURCE_KINDS:
            raise UnownedResourceError(f"unknown resource kind {kind!r}")
        if kind == "unit":
            if not isinstance(value, str) or not value.startswith(
                f"zerofs-tiered-{self.run_uuid}"
            ):
                raise UnownedResourceError(
                    f"unowned unit {value!r}: not scoped to run {self.run_uuid}"
                )
            return value
        if kind == "process":
            if not isinstance(value, int) or value <= 0:
                raise UnownedResourceError(f"unowned process id {value!r}")
            return value
        if kind == "listener":
            if not isinstance(value, int) or not _SYSTEM_PORT_CEILING <= value <= 65535:
                raise UnownedResourceError(
                    f"unowned listener port {value!r}: system and invalid ports are "
                    "never harness-owned"
                )
            return value
        if kind == "device":
            if not _NBD_DEVICE.fullmatch(str(value)):
                raise UnownedResourceError(
                    f"unowned device {value!r}: only /dev/nbdN devices are disposable"
                )
            return str(value)
        if kind == "pool":
            if not str(value).startswith(f"zerofs-tiered-{self.run_uuid}"):
                raise UnownedResourceError(
                    f"unowned pool {value!r}: not scoped to run {self.run_uuid}"
                )
            return str(value)
        if kind == "prefix":
            expected = backend_prefix_for(self.run_uuid)
            if str(value) != expected and not str(value).startswith(expected + "/"):
                raise UnownedResourceError(
                    f"unowned backend prefix {value!r}: expected {expected!r}"
                )
            return str(value)
        # mount and path resources must live under the resource root.
        try:
            validate_owned_path(Path(str(value)), self.resource_root)
        except UnsafeCleanupTarget as error:
            raise UnownedResourceError(str(error)) from error
        return str(value)

    def record_resource(self, kind: str, value: Any, **details: Any) -> None:
        validated = self._validate_resource(kind, value)
        if kind == "process":
            unit = self._validate_resource("unit", details.get("unit"))
            if ("unit", str(unit)) not in {
                (active_kind, str(active_value))
                for active_kind, active_value, _ in self.outstanding()
            }:
                raise UnownedResourceError(
                    f"process {validated!r} has no active unit authority {unit!r}"
                )
        key = (kind, str(validated))
        if any(
            (owned_kind, str(owned_value)) == key
            for owned_kind, owned_value, _ in self.outstanding()
        ):
            raise UnownedResourceError(
                f"{kind} {validated!r} is already active for run {self.run_uuid}"
            )
        self.append(
            "acquire", {"resource": kind, "value": validated, "details": details}
        )

    def record_release(self, kind: str, value: Any, **details: Any) -> None:
        key = (kind, str(value))
        if not any(
            (owned_kind, str(owned_value)) == key
            for owned_kind, owned_value, _ in self.outstanding()
        ):
            raise UnownedResourceError(
                f"{kind} {value!r} is not active for run {self.run_uuid}"
            )
        self.append("release", {"resource": kind, "value": value, "details": details})

    def resources(self) -> list[tuple[str, Any, dict[str, Any]]]:
        return [
            (
                event["payload"]["resource"],
                event["payload"]["value"],
                dict(event["payload"].get("details", {})),
            )
            for event in self.events
            if event["kind"] == "acquire"
        ]

    def outstanding(self) -> list[tuple[str, Any, dict[str, Any]]]:
        active: dict[tuple[str, str], tuple[str, Any, dict[str, Any]]] = {}
        for event in self.events:
            if event["kind"] not in ("acquire", "release"):
                continue
            payload = event["payload"]
            key = (payload["resource"], str(payload["value"]))
            if event["kind"] == "acquire":
                active[key] = (
                    payload["resource"],
                    payload["value"],
                    dict(payload.get("details", {})),
                )
            else:
                active.pop(key, None)
        return list(active.values())

    def require_owned(self, kind: str, value: Any) -> None:
        recorded = {(k, str(v)) for k, v, _ in self.resources()}
        if (kind, str(value)) not in recorded:
            raise UnownedResourceError(
                f"{kind} {value!r} was never recorded by run {self.run_uuid}"
            )

    def require_active(self, kind: str, value: Any) -> None:
        active = {(k, str(v)) for k, v, _ in self.outstanding()}
        if (kind, str(value)) not in active:
            raise UnownedResourceError(
                f"{kind} {value!r} is not active for run {self.run_uuid}"
            )

    def validate_cleanup_scope(self) -> Path:
        """Validate that this ledger authorizes cleanup and return the scope."""
        workspace = self.identity.get("workspace_root")
        _, resource = validate_run_roots(
            self.identity.get("run_uuid"),
            self.control_root,
            self.resource_root,
            root_parent=Path(
                str(self.identity.get("root_parent", DEFAULT_ROOT_PARENT))
            ),
            workspace_root=Path(str(workspace)) if workspace else None,
            extra_strings=(str(self.identity.get("backend_prefix", "")),),
        )
        for kind, value, _ in self.resources():
            if kind in ("mount", "path"):
                validate_owned_path(Path(str(value)), resource)
        return resource

    def value(self, key: str) -> Any:
        document = self.to_document()
        current: Any = document
        for part in key.split("."):
            if isinstance(current, list):
                current = current[int(part)]
            elif isinstance(current, dict):
                if part not in current:
                    raise KeyError(f"ledger has no value at {key!r}")
                current = current[part]
            else:
                raise KeyError(f"ledger has no value at {key!r}")
        return current

    def to_document(self) -> dict[str, Any]:
        return {
            "schema": SCHEMA,
            "identity": self.identity,
            "identity_hash": self.identity_hash(),
            "events": self.events,
        }

    def save(self) -> None:
        if self.path is None:
            return
        directory = self.path.parent
        directory.mkdir(parents=True, exist_ok=True)
        fd, temporary = tempfile.mkstemp(prefix=".ledger.", dir=directory)
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as handle:
                json.dump(self.to_document(), handle, indent=2, sort_keys=True)
                handle.write("\n")
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temporary, self.path)
        finally:
            if os.path.exists(temporary):
                os.unlink(temporary)
