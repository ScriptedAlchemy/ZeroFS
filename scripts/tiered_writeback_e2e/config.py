from __future__ import annotations

import os
import uuid as uuid_module
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Mapping

FILESYSTEM_ACK_MODES = ("materialized", "volatile_memory")
OBJECT_ACK_MODES = ("memory", "ssd", "remote")

CONTROL_PREFIX = "zerofs-tiered-control-"
RESOURCE_PREFIX = "zerofs-tiered-resources-"
BACKEND_PREFIX_ROOT = "zerofs-tiered"

DEFAULT_ROOT_PARENT = Path("/var/tmp")
DEFAULT_PROC_ROOT = Path("/proc")
DEFAULT_ARCHIVE_ROOT = Path("/fast/zerofs-tiered-receipts")

ROOT_PARENT_ENV = "ZEROFS_TIERED_ROOT_PARENT"
PROC_ROOT_ENV = "ZEROFS_TIERED_PROC_ROOT"

# Paths that must never become a control root, resource root, or cleanup scope,
# in both literal and resolved (macOS /var -> /private/var) forms.
_FORBIDDEN_LITERALS = (
    "/",
    "/mnt",
    "/var/tmp",
    "/var",
    "/tmp",
    "/etc",
    "/usr",
    "/home",
    "/dev",
    "/proc",
    "/sys",
    "/var/lib",
    "/var/log",
)
FORBIDDEN_ROOTS = frozenset(
    {literal for literal in _FORBIDDEN_LITERALS}
    | {str(Path(literal).resolve(strict=False)) for literal in _FORBIDDEN_LITERALS}
)

# Substrings that mark production infrastructure; a run root or backend prefix
# containing one of these is never a disposable harness namespace.
PRODUCTION_MARKERS = ("ct198", "production", "storagebox-nbd-pilot")


class HarnessError(RuntimeError):
    """Base class for every intentional harness failure."""


class ConfigError(HarnessError):
    pass


class MissingAckModeError(ConfigError):
    pass


class UnsafeCleanupTarget(HarnessError):
    pass


@dataclass(frozen=True, slots=True)
class AckModes:
    filesystem: str
    object: str

    @property
    def label(self) -> str:
        return f"{self.filesystem}+{self.object}"


def require_ack_modes(filesystem: str | None, object_: str | None) -> AckModes:
    if filesystem is None or object_ is None:
        raise MissingAckModeError(
            "both --filesystem-ack-mode and --object-ack-mode are required"
        )
    if filesystem not in FILESYSTEM_ACK_MODES:
        raise ConfigError(
            f"filesystem ack mode {filesystem!r} is not one of "
            f"{'|'.join(FILESYSTEM_ACK_MODES)}"
        )
    if object_ not in OBJECT_ACK_MODES:
        raise ConfigError(
            f"object ack mode {object_!r} is not one of {'|'.join(OBJECT_ACK_MODES)}"
        )
    return AckModes(filesystem, object_)


def require_run_uuid(value: object) -> str:
    text = str(value)
    try:
        parsed = uuid_module.UUID(text)
    except (ValueError, AttributeError, TypeError) as error:
        raise UnsafeCleanupTarget(f"run id {text!r} is not a UUID") from error
    if str(parsed) != text:
        raise UnsafeCleanupTarget(f"run id {text!r} is not a canonical lowercase UUID")
    return text


def control_root_for(root_parent: Path, run_uuid: str) -> Path:
    return root_parent / f"{CONTROL_PREFIX}{require_run_uuid(run_uuid)}"


def resource_root_for(root_parent: Path, run_uuid: str) -> Path:
    return root_parent / f"{RESOURCE_PREFIX}{require_run_uuid(run_uuid)}"


def backend_prefix_for(run_uuid: str) -> str:
    return f"{BACKEND_PREFIX_ROOT}/{require_run_uuid(run_uuid)}"


def _reject_production_markers(text: str, role: str) -> None:
    lowered = text.lower()
    for marker in PRODUCTION_MARKERS:
        if marker in lowered:
            raise UnsafeCleanupTarget(
                f"unsafe {role}: {text!r} contains production marker {marker!r}"
            )


def _resolved(path: Path, role: str) -> Path:
    if not path.is_absolute():
        raise UnsafeCleanupTarget(f"unsafe {role}: {path} is not absolute")
    return path.resolve(strict=False)


def _reject_forbidden(path: Path, resolved: Path, role: str) -> None:
    for candidate in (str(path), str(resolved)):
        if candidate.rstrip("/") in FORBIDDEN_ROOTS or candidate in FORBIDDEN_ROOTS:
            raise UnsafeCleanupTarget(f"unsafe {role}: {path} is a protected path")


def path_is_within(inner: Path, outer: Path) -> bool:
    try:
        inner.relative_to(outer)
    except ValueError:
        return False
    return True


def validate_run_roots(
    run_uuid: object,
    control_root: Path,
    resource_root: Path,
    *,
    root_parent: Path,
    workspace_root: Path | None,
    extra_strings: Iterable[str] = (),
) -> tuple[Path, Path]:
    """Validate the control/resource pair for one run and return resolved paths.

    Everything unsafe raises UnsafeCleanupTarget: non-UUID run ids, protected
    system paths, roots outside the root parent, wrong prefixes, mismatched
    UUIDs, equal or nested pairs, workspace overlap, and production markers.
    """
    canonical = require_run_uuid(run_uuid)
    parent = _resolved(Path(root_parent), "root parent")
    _reject_production_markers(str(root_parent), "root parent")
    _reject_production_markers(str(parent), "root parent")
    for text in extra_strings:
        _reject_production_markers(text, "run configuration")

    expected_names = {
        "control root": f"{CONTROL_PREFIX}{canonical}",
        "resource root": f"{RESOURCE_PREFIX}{canonical}",
    }
    resolved: dict[str, Path] = {}
    for role, path in (("control root", control_root), ("resource root", resource_root)):
        target = _resolved(Path(path), role)
        _reject_forbidden(Path(path), target, role)
        _reject_production_markers(str(path), role)
        _reject_production_markers(str(target), role)
        if target.parent != parent:
            raise UnsafeCleanupTarget(
                f"unsafe {role}: {target} is not a direct child of {parent}"
            )
        if target.name != expected_names[role]:
            raise UnsafeCleanupTarget(
                f"unsafe {role}: {target.name!r} is not {expected_names[role]!r}"
            )
        if workspace_root is not None:
            workspace = _resolved(Path(workspace_root), "workspace root")
            if (
                target == workspace
                or path_is_within(target, workspace)
                or path_is_within(workspace, target)
            ):
                raise UnsafeCleanupTarget(
                    f"unsafe {role}: {target} overlaps workspace {workspace}"
                )
        resolved[role] = target

    control, resource = resolved["control root"], resolved["resource root"]
    if control == resource:
        raise UnsafeCleanupTarget("control and resource roots must be distinct")
    if path_is_within(control, resource) or path_is_within(resource, control):
        raise UnsafeCleanupTarget("control and resource roots must not nest")
    return control, resource


def validate_owned_path(path: Path, resource_root: Path) -> Path:
    """Require that ``path`` is the resource root or strictly below it."""
    resolved = _resolved(Path(path), "resource path")
    root = _resolved(Path(resource_root), "resource root")
    if not path_is_within(resolved, root):
        raise UnsafeCleanupTarget(
            f"unowned resource path: {resolved} is not below {root}"
        )
    return resolved


@dataclass(frozen=True, slots=True)
class HarnessConfig:
    run_uuid: str
    ack: AckModes
    control_root: Path
    resource_root: Path
    root_parent: Path
    workspace_root: Path | None
    proc_root: Path
    source_root: Path

    @property
    def ledger_path(self) -> Path:
        return self.control_root / "ledger.json"

    @property
    def receipt_root(self) -> Path:
        return self.control_root / "receipts"

    @property
    def tools_root(self) -> Path:
        return self.resource_root / "tools"

    @property
    def mount_root(self) -> Path:
        return self.resource_root / "mnt"

    @property
    def run_root(self) -> Path:
        return self.resource_root / "run"

    @property
    def backend_prefix(self) -> str:
        return backend_prefix_for(self.run_uuid)

    @property
    def unit_name(self) -> str:
        return f"zerofs-tiered-{self.run_uuid}"

    @classmethod
    def create(
        cls,
        *,
        filesystem_ack_mode: str | None,
        object_ack_mode: str | None,
        run_uuid: str | None = None,
        environ: Mapping[str, str] | None = None,
        workspace_root: Path | None = None,
        source_root: Path | None = None,
    ) -> "HarnessConfig":
        env = os.environ if environ is None else environ
        ack = require_ack_modes(filesystem_ack_mode, object_ack_mode)
        canonical = require_run_uuid(run_uuid or str(uuid_module.uuid4()))
        root_parent = Path(env.get(ROOT_PARENT_ENV, str(DEFAULT_ROOT_PARENT)))
        proc_root = Path(env.get(PROC_ROOT_ENV, str(DEFAULT_PROC_ROOT)))
        if workspace_root is None:
            workspace_root = Path(__file__).resolve().parents[2]
        if source_root is None:
            source_root = workspace_root
        control = control_root_for(root_parent, canonical)
        resource = resource_root_for(root_parent, canonical)
        control, resource = validate_run_roots(
            canonical,
            control,
            resource,
            root_parent=root_parent,
            workspace_root=workspace_root,
            extra_strings=(backend_prefix_for(canonical),),
        )
        return cls(
            run_uuid=canonical,
            ack=ack,
            control_root=control,
            resource_root=resource,
            root_parent=root_parent.resolve(strict=False),
            workspace_root=Path(workspace_root).resolve(strict=False),
            proc_root=proc_root,
            source_root=Path(source_root).resolve(strict=False),
        )

    @classmethod
    def from_identity(cls, identity: Mapping[str, object]) -> "HarnessConfig":
        """Rebuild a config from a ledger identity without re-validating.

        Cleanup-side commands must reach validate_cleanup_scope so unsafe
        ledgers fail there with UnsafeCleanupTarget, not during parsing.
        """
        workspace = identity.get("workspace_root")
        return cls(
            run_uuid=str(identity["run_uuid"]),
            ack=AckModes(
                str(identity.get("filesystem_ack_mode", FILESYSTEM_ACK_MODES[0])),
                str(identity.get("object_ack_mode", OBJECT_ACK_MODES[-1])),
            ),
            control_root=Path(str(identity["control_root"])),
            resource_root=Path(str(identity["resource_root"])),
            root_parent=Path(str(identity.get("root_parent", DEFAULT_ROOT_PARENT))),
            workspace_root=Path(str(workspace)) if workspace else None,
            proc_root=Path(
                os.environ.get(PROC_ROOT_ENV, str(DEFAULT_PROC_ROOT))
            ),
            source_root=Path(str(identity.get("source_root", "/"))),
        )
