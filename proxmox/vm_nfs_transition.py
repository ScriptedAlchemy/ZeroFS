#!/usr/bin/env python3
"""Transactionally quiesce and reconcile VM100's single ZeroFS NFS mount."""

from __future__ import annotations

import argparse
import dataclasses
import hashlib
import ipaddress
import json
import os
import shutil
import subprocess
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol, Sequence


UNIT_NAME = r"mnt-zerofs\x2dfiles.mount"
UNIT_PATH = Path("/etc/systemd/system") / UNIT_NAME
MOUNTPOINT = Path("/mnt/zerofs-files")
TRANSACTION_ROOT = Path("/var/lib/zerofs-deploy/transactions")
FORBIDDEN_UNITS = (
    "zerofs-lxc-nbd-client.service",
    "mnt-zerofs-lxc.mount",
    r"mnt-zerofs\x2dlxc.mount",
    r"mnt-zerofs\x2dfiles\x2draw.mount",
    r"mnt-zerofs\x2dfiles\x2draw-.nbd.mount",
    r"mnt-zerofs\x2dfiles-.nbd.mount",
    "zerofs-shared-namespace-permissions.service",
)
FORBIDDEN_MOUNTS = (
    Path("/mnt/zerofs-lxc"),
    Path("/mnt/zerofs-files-raw/.nbd"),
    Path("/mnt/zerofs-files-raw"),
)
RFC1918_NETWORKS = tuple(
    ipaddress.ip_network(value)
    for value in ("10.0.0.0/8", "172.16.0.0/12", "192.168.0.0/16")
)
TRANSACTION_PHASES = frozenset(
    {"prepared", "quiesced", "reconciled", "commit_decided", "rolled_back"}
)
LEGACY_BINDFS_HASHES = frozenset(
    {
        "013e9481f2bf7e0ba66f4dbc60bba64937da88293f7f3732c0e62c7cb2c5b33d",
        "9a0e6e3501a971c13b5d5ad7e609cc92989f83c197821f0c09596a02c3cbeac2",
    }
)
LEGACY_ARTIFACTS = tuple(
    Path(value)
    for value in (
        r"/etc/systemd/system/mnt-zerofs\x2dlxc.mount",
        "/etc/systemd/system/mnt-zerofs-lxc.mount",
        "/etc/systemd/system/zerofs-lxc-nbd-client.service",
        r"/etc/systemd/system/mnt-zerofs\x2dfiles\x2draw.mount",
        r"/etc/systemd/system/mnt-zerofs\x2dfiles\x2draw-.nbd.mount",
        r"/etc/systemd/system/mnt-zerofs\x2dfiles-.nbd.mount",
        "/etc/systemd/system/zerofs-shared-namespace-permissions.service",
        "/usr/local/libexec/zerofs-tune-nbd",
        "/usr/local/libexec/zerofs-normalize-shared-namespace",
        "/etc/zerofs-lxc/client.env",
    )
)


@dataclass(frozen=True)
class MountRecord:
    source: str
    fstype: str
    options: tuple[str, ...]


@dataclass(frozen=True)
class DeploymentIdentity:
    ctid: int
    release: str
    source: str
    pve_host: str


class System(Protocol):
    def is_enabled(self, unit: str) -> bool:
        ...

    def is_active(self, unit: str) -> bool:
        ...

    def is_loaded(self, unit: str) -> bool:
        ...

    def mount_record(self, mountpoint: Path) -> MountRecord | None:
        ...

    def daemon_reload(self) -> None:
        ...

    def enable(self, unit: str) -> None:
        ...

    def disable(self, unit: str) -> None:
        ...

    def start(self, unit: str) -> None:
        ...

    def stop(self, unit: str) -> None:
        ...


class HostSystem:
    @staticmethod
    def _run(*command: str, check: bool = True) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            command,
            check=check,
            text=True,
            capture_output=True,
        )

    def is_enabled(self, unit: str) -> bool:
        return (
            self._run(
                "systemctl", "is-enabled", "--quiet", unit, check=False
            ).returncode
            == 0
        )

    def is_active(self, unit: str) -> bool:
        return (
            self._run("systemctl", "is-active", "--quiet", unit, check=False).returncode
            == 0
        )

    def is_loaded(self, unit: str) -> bool:
        result = self._run(
            "systemctl", "show", "-p", "LoadState", "--value", unit, check=False
        )
        return result.returncode == 0 and result.stdout.strip() != "not-found"

    def mount_record(self, mountpoint: Path) -> MountRecord | None:
        result = self._run(
            "findmnt",
            "-rn",
            "-M",
            str(mountpoint),
            "-o",
            "SOURCE,FSTYPE,OPTIONS",
            check=False,
        )
        if result.returncode != 0 or not result.stdout.strip():
            return None
        fields = result.stdout.strip().split(maxsplit=2)
        if len(fields) != 3:
            raise RuntimeError(
                f"unexpected findmnt record for {mountpoint}: {result.stdout!r}"
            )
        return MountRecord(fields[0], fields[1], tuple(fields[2].split(",")))

    def daemon_reload(self) -> None:
        self._run("systemctl", "daemon-reload")

    def enable(self, unit: str) -> None:
        self._run("systemctl", "enable", unit)

    def disable(self, unit: str) -> None:
        self._run("systemctl", "disable", unit)

    def start(self, unit: str) -> None:
        self._run("systemctl", "start", unit)

    def stop(self, unit: str) -> None:
        self._run("systemctl", "stop", unit)


class Transition:
    def __init__(
        self,
        system: System,
        *,
        unit_path: Path = UNIT_PATH,
        mountpoint: Path = MOUNTPOINT,
        forbidden_units: Sequence[str] = FORBIDDEN_UNITS,
        forbidden_mounts: Sequence[Path] = FORBIDDEN_MOUNTS,
        legacy_artifacts: Sequence[Path] = LEGACY_ARTIFACTS,
    ) -> None:
        self.system = system
        self.unit_path = unit_path
        self.mountpoint = mountpoint
        self.forbidden_units = tuple(forbidden_units)
        self.forbidden_mounts = tuple(forbidden_mounts)
        self.legacy_artifacts = tuple(legacy_artifacts)

    @staticmethod
    def _state_path(transaction: Path) -> Path:
        return transaction / "state.json"

    @staticmethod
    def _fsync_file(path: Path) -> None:
        with path.open("rb") as handle:
            os.fsync(handle.fileno())

    @staticmethod
    def _fsync_directory(path: Path) -> None:
        descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)

    def _load(self, transaction: Path) -> dict[str, object]:
        try:
            state = json.loads(self._state_path(transaction).read_text())
        except (OSError, json.JSONDecodeError) as error:
            raise RuntimeError(f"invalid VM NFS transaction {transaction}") from error
        if not isinstance(state, dict):
            raise RuntimeError(f"invalid VM NFS transaction {transaction}")
        if state.get("phase") not in TRANSACTION_PHASES:
            raise RuntimeError(
                f"invalid VM NFS transaction phase in {transaction}: "
                f"{state.get('phase')!r}"
            )
        deployment = state.get("deployment")
        if (
            not isinstance(deployment, dict)
            or set(deployment) != {"ctid", "release", "source", "pve_host"}
            or not isinstance(deployment.get("ctid"), int)
            or deployment["ctid"] <= 0
            or not all(
                isinstance(deployment.get(field), str) and deployment[field]
                for field in ("release", "source", "pve_host")
            )
        ):
            raise RuntimeError(f"invalid deployment identity in {transaction}")
        return state

    def _set_phase(self, transaction: Path, phase: str) -> None:
        if phase not in TRANSACTION_PHASES:
            raise ValueError(f"invalid transaction phase: {phase}")
        state = self._load(transaction)
        state["phase"] = phase
        with tempfile.NamedTemporaryFile(
            mode="w", dir=transaction, delete=False
        ) as handle:
            temporary = Path(handle.name)
            handle.write(json.dumps(state, sort_keys=True) + "\n")
            handle.flush()
            os.fsync(handle.fileno())
        try:
            os.replace(temporary, self._state_path(transaction))
            self._fsync_directory(transaction)
        finally:
            temporary.unlink(missing_ok=True)

    @staticmethod
    def _mount_from_state(value: object) -> MountRecord | None:
        if value is None:
            return None
        if not isinstance(value, dict):
            raise RuntimeError("invalid saved mount record")
        source = value.get("source")
        fstype = value.get("fstype")
        options = value.get("options")
        if (
            not isinstance(source, str)
            or not isinstance(fstype, str)
            or not isinstance(options, list)
        ):
            raise RuntimeError("invalid saved mount record")
        if not all(isinstance(option, str) for option in options):
            raise RuntimeError("invalid saved mount options")
        return MountRecord(source, fstype, tuple(options))

    @staticmethod
    def _same_bytes(left: Path, right: Path) -> bool:
        return (
            left.is_file()
            and right.is_file()
            and left.read_bytes() == right.read_bytes()
        )

    def _install_unit(self, source: Path) -> None:
        self.unit_path.parent.mkdir(parents=True, exist_ok=True)
        with tempfile.NamedTemporaryFile(
            dir=self.unit_path.parent, delete=False
        ) as handle:
            temporary = Path(handle.name)
        try:
            shutil.copyfile(source, temporary)
            temporary.chmod(0o644)
            self._fsync_file(temporary)
            os.replace(temporary, self.unit_path)
            self._fsync_directory(self.unit_path.parent)
        finally:
            temporary.unlink(missing_ok=True)

    def _install_artifact(self, source: Path, destination: Path, mode: int) -> None:
        destination.parent.mkdir(parents=True, exist_ok=True)
        with tempfile.NamedTemporaryFile(
            dir=destination.parent, delete=False
        ) as handle:
            temporary = Path(handle.name)
        try:
            shutil.copyfile(source, temporary)
            temporary.chmod(mode)
            self._fsync_file(temporary)
            os.replace(temporary, destination)
            self._fsync_directory(destination.parent)
        finally:
            temporary.unlink(missing_ok=True)

    def _recognized_legacy_bindfs(
        self, mount: MountRecord, expected_source: str
    ) -> bool:
        if (
            mount.source != "/mnt/zerofs-files-raw"
            or mount.fstype != "fuse.bindfs"
            or "rw" not in mount.options
            or not self.unit_path.is_file()
        ):
            return False
        if (
            hashlib.sha256(self.unit_path.read_bytes()).hexdigest()
            not in LEGACY_BINDFS_HASHES
        ):
            return False
        raw_mountpoint = next(
            (path for path in self.forbidden_mounts if path.name == "zerofs-files-raw"),
            None,
        )
        if raw_mountpoint is None:
            return False
        raw_mount = self.system.mount_record(raw_mountpoint)
        return bool(
            raw_mount
            and raw_mount.source == expected_source
            and raw_mount.fstype == "nfs"
            and "rw" in raw_mount.options
        )

    def prepare(
        self,
        staged_unit: Path,
        transaction: Path,
        expected_source: str,
        deployment: DeploymentIdentity,
        *,
        allow_legacy_bindfs: bool = False,
    ) -> None:
        if transaction.exists():
            raise RuntimeError(f"VM NFS transaction already exists: {transaction}")
        if not staged_unit.is_file():
            raise RuntimeError(f"staged VM NFS unit is missing: {staged_unit}")
        if staged_unit.read_text().splitlines().count(f"What={expected_source}") != 1:
            raise RuntimeError(
                f"staged VM NFS unit does not contain expected NFS source {expected_source}"
            )
        if deployment.source != expected_source:
            raise RuntimeError(
                "deployment identity source does not match expected source"
            )
        mount = self.system.mount_record(self.mountpoint)
        if (
            mount is not None
            and mount.source != expected_source
            and not (
                allow_legacy_bindfs
                and self._recognized_legacy_bindfs(mount, expected_source)
            )
        ):
            raise RuntimeError(
                f"live VM NFS source is {mount.source}, not {expected_source}; "
                "it is not a recognized legacy bindfs topology"
            )
        transaction.parent.mkdir(parents=True, exist_ok=True)
        temporary = Path(
            tempfile.mkdtemp(prefix=f".{transaction.name}-", dir=transaction.parent)
        )
        try:
            shutil.copyfile(staged_unit, temporary / "desired.mount")
            unit_existed = self.unit_path.is_file()
            if unit_existed:
                shutil.copyfile(self.unit_path, temporary / "previous.mount")
            legacy_directory = temporary / "legacy-artifacts"
            legacy_artifacts: list[dict[str, object]] = []
            for index, artifact in enumerate(self.legacy_artifacts):
                if not artifact.exists():
                    continue
                if not artifact.is_file() or artifact.is_symlink():
                    raise RuntimeError(f"unsafe legacy artifact: {artifact}")
                legacy_directory.mkdir(exist_ok=True)
                snapshot = legacy_directory / str(index)
                shutil.copyfile(artifact, snapshot)
                legacy_artifacts.append(
                    {
                        "index": index,
                        "path": str(artifact),
                        "mode": artifact.stat().st_mode & 0o7777,
                    }
                )
            legacy_units = [
                {
                    "unit": unit,
                    "enabled": self.system.is_enabled(unit),
                    "active": self.system.is_active(unit),
                }
                for unit in self.forbidden_units
                if self.system.is_loaded(unit)
            ]
            state = {
                "phase": "prepared",
                "deployment": dataclasses.asdict(deployment),
                "expected_source": expected_source,
                "unit_existed": unit_existed,
                "enabled": self.system.is_enabled(UNIT_NAME),
                "active": self.system.is_active(UNIT_NAME),
                "mountpoint_existed": self.mountpoint.is_dir(),
                "mount": dataclasses.asdict(mount) if mount else None,
                "legacy_artifacts": legacy_artifacts,
                "legacy_units": legacy_units,
            }
            self._state_path(temporary).write_text(
                json.dumps(state, sort_keys=True) + "\n"
            )
            for path in temporary.rglob("*"):
                if path.is_file():
                    self._fsync_file(path)
                elif path.is_dir():
                    self._fsync_directory(path)
            self._fsync_directory(temporary)
            os.replace(temporary, transaction)
            self._fsync_directory(transaction.parent)
        finally:
            if temporary.exists():
                shutil.rmtree(temporary)

    def quiesce(self, transaction: Path) -> None:
        self._load(transaction)
        if self.system.is_active(UNIT_NAME) or self.system.mount_record(
            self.mountpoint
        ):
            self.system.stop(UNIT_NAME)
        if self.system.mount_record(self.mountpoint) is not None:
            raise RuntimeError(f"VM NFS mount remained active at {self.mountpoint}")
        self._set_phase(transaction, "quiesced")

    def _assert_no_legacy_topology(self) -> None:
        for unit in self.forbidden_units:
            if self.system.is_loaded(unit):
                raise RuntimeError(f"legacy ZeroFS unit remains installed: {unit}")
        for mountpoint in self.forbidden_mounts:
            if self.system.mount_record(mountpoint) is not None:
                raise RuntimeError(f"legacy ZeroFS mount remains active: {mountpoint}")

    def _assert_mount(self, expected_source: str) -> None:
        mount = self.system.mount_record(self.mountpoint)
        if mount is None:
            raise RuntimeError(f"VM NFS mount is absent at {self.mountpoint}")
        if (
            mount.source != expected_source
            or mount.fstype != "nfs"
            or "rw" not in mount.options
            or "vers=3" not in mount.options
        ):
            raise RuntimeError(
                f"unexpected VM NFS mount: {mount}; expected {expected_source} nfs v3 rw"
            )

    def reconcile(self, transaction: Path) -> None:
        state = self._load(transaction)
        expected_source = state.get("expected_source")
        if not isinstance(expected_source, str):
            raise RuntimeError("VM NFS transaction has no expected source")
        self._assert_no_legacy_topology()
        desired = transaction / "desired.mount"
        if not self._same_bytes(self.unit_path, desired):
            self._install_unit(desired)
            self.system.daemon_reload()
        if not self.system.is_enabled(UNIT_NAME):
            self.system.enable(UNIT_NAME)
        if not self.system.is_active(UNIT_NAME):
            self.mountpoint.mkdir(parents=True, exist_ok=True)
            self.mountpoint.chmod(0o755)
            self.system.start(UNIT_NAME)
        self._assert_mount(expected_source)
        self._assert_no_legacy_topology()
        self._set_phase(transaction, "reconciled")

    def rollback(self, transaction: Path) -> None:
        state = self._load(transaction)
        if self.system.is_active(UNIT_NAME) or self.system.mount_record(
            self.mountpoint
        ):
            self.system.stop(UNIT_NAME)
        unit_existed = state.get("unit_existed") is True
        previous = transaction / "previous.mount"
        changed = False
        if unit_existed:
            if not previous.is_file():
                raise RuntimeError("VM NFS transaction lost its prior unit")
            if not self._same_bytes(self.unit_path, previous):
                self._install_unit(previous)
                changed = True
        elif self.unit_path.exists():
            self.unit_path.unlink()
            self._fsync_directory(self.unit_path.parent)
            changed = True
        if changed:
            self.system.daemon_reload()

        legacy_artifacts = state.get("legacy_artifacts", [])
        if not isinstance(legacy_artifacts, list):
            raise RuntimeError("invalid saved legacy artifact state")
        restored_legacy = False
        for item in legacy_artifacts:
            if not isinstance(item, dict):
                raise RuntimeError("invalid saved legacy artifact")
            index = item.get("index")
            path = item.get("path")
            mode = item.get("mode")
            if (
                not isinstance(index, int)
                or not isinstance(path, str)
                or not isinstance(mode, int)
            ):
                raise RuntimeError("invalid saved legacy artifact")
            destination = Path(path)
            if destination not in self.legacy_artifacts:
                raise RuntimeError(f"unowned saved legacy artifact: {destination}")
            snapshot = transaction / "legacy-artifacts" / str(index)
            if not snapshot.is_file():
                raise RuntimeError(f"missing saved legacy artifact: {destination}")
            self._install_artifact(snapshot, destination, mode)
            restored_legacy = True
        if restored_legacy:
            self.system.daemon_reload()

        legacy_units = state.get("legacy_units", [])
        if not isinstance(legacy_units, list):
            raise RuntimeError("invalid saved legacy unit state")
        for item in legacy_units:
            if (
                not isinstance(item, dict)
                or item.get("unit") not in self.forbidden_units
            ):
                raise RuntimeError("invalid saved legacy unit")
            unit = item["unit"]
            assert isinstance(unit, str)
            if item.get("enabled") is True and not self.system.is_enabled(unit):
                self.system.enable(unit)
            if item.get("active") is True and not self.system.is_active(unit):
                self.system.start(unit)

        was_enabled = state.get("enabled") is True
        if was_enabled and not self.system.is_enabled(UNIT_NAME):
            self.system.enable(UNIT_NAME)
        elif not was_enabled and self.system.is_enabled(UNIT_NAME):
            self.system.disable(UNIT_NAME)

        prior_mount = self._mount_from_state(state.get("mount"))
        was_active = state.get("active") is True
        if was_active or prior_mount is not None:
            self.mountpoint.mkdir(parents=True, exist_ok=True)
            self.mountpoint.chmod(0o755)
            self.system.start(UNIT_NAME)
        if prior_mount is None:
            if self.system.mount_record(self.mountpoint) is not None:
                raise RuntimeError(
                    "VM NFS rollback unexpectedly mounted a prior-absent unit"
                )
        else:
            restored = self.system.mount_record(self.mountpoint)
            if (
                restored is None
                or restored.source != prior_mount.source
                or restored.fstype != prior_mount.fstype
                or "rw" not in restored.options
            ):
                raise RuntimeError(
                    f"VM NFS rollback did not restore prior mount: {restored!r}"
                )
        if state.get("mountpoint_existed") is not True and self.mountpoint.is_dir():
            self.mountpoint.rmdir()
        self._set_phase(transaction, "rolled_back")

    def commit(self, transaction: Path) -> None:
        self._load(transaction)
        shutil.rmtree(transaction)
        self._fsync_directory(transaction.parent)

    def decide_commit(self, transaction: Path) -> None:
        state = self._load(transaction)
        if state["phase"] != "reconciled":
            raise RuntimeError("VM NFS transaction is not reconciled for commit")
        self._set_phase(transaction, "commit_decided")

    def status(self, transaction: Path) -> dict[str, object]:
        if not transaction.exists():
            return {"phase": "absent"}
        return self._load(transaction)

    def recover(self, transaction: Path) -> None:
        if not transaction.exists():
            return
        state = self._load(transaction)
        if state["phase"] == "commit_decided":
            self.commit(transaction)
            return
        if state["phase"] != "rolled_back":
            self.rollback(transaction)
        self.commit(transaction)


def _private_nfs_source(value: str) -> str:
    if not value.endswith(":/"):
        raise ValueError("expected source must be an IPv4 NFS root export")
    address = ipaddress.ip_address(value[:-2])
    if address.version != 4 or not any(
        address in network for network in RFC1918_NETWORKS
    ):
        raise ValueError("expected source must use an RFC1918 IPv4 address")
    return value


def _owned_transaction(value: str) -> Path:
    path = Path(value)
    if (
        not path.is_absolute()
        or path.parent != TRANSACTION_ROOT
        or path.name in {"", ".", ".."}
    ):
        raise ValueError(f"transaction must be one child of {TRANSACTION_ROOT}")
    return path


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "action",
        choices=(
            "recover",
            "prepare",
            "quiesce",
            "reconcile",
            "rollback",
            "commit",
            "decide",
            "status",
        ),
    )
    parser.add_argument("--transaction", required=True)
    parser.add_argument("--staged-unit")
    parser.add_argument("--expected-source")
    parser.add_argument("--deployment-ctid", type=int)
    parser.add_argument("--deployment-release")
    parser.add_argument("--deployment-pve-host")
    parser.add_argument("--allow-legacy-bindfs", action="store_true")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    if os.geteuid() != 0:
        raise PermissionError("VM NFS transition requires root")
    args = build_parser().parse_args(argv)
    transaction = _owned_transaction(args.transaction)
    manager = Transition(HostSystem())
    if args.action == "recover":
        manager.recover(transaction)
    elif args.action == "prepare":
        if (
            args.staged_unit is None
            or args.expected_source is None
            or args.deployment_ctid is None
            or args.deployment_release is None
            or args.deployment_pve_host is None
        ):
            raise ValueError(
                "prepare requires staged unit and full deployment identity"
            )
        source = _private_nfs_source(args.expected_source)
        manager.prepare(
            Path(args.staged_unit),
            transaction,
            source,
            DeploymentIdentity(
                ctid=args.deployment_ctid,
                release=args.deployment_release,
                source=source,
                pve_host=args.deployment_pve_host,
            ),
            allow_legacy_bindfs=args.allow_legacy_bindfs,
        )
    elif args.action == "quiesce":
        manager.quiesce(transaction)
    elif args.action == "reconcile":
        manager.reconcile(transaction)
    elif args.action == "rollback":
        manager.rollback(transaction)
    elif args.action == "decide":
        manager.decide_commit(transaction)
    elif args.action == "status":
        print(json.dumps(manager.status(transaction), sort_keys=True))
    else:
        manager.commit(transaction)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (OSError, RuntimeError, ValueError) as error:
        print(f"error: {error}", file=os.sys.stderr)
        raise SystemExit(1)
