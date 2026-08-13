from __future__ import annotations

import getpass
import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Mapping


_NBD_DEVICE = re.compile(r"/dev/nbd(?:0|[1-9][0-9]*)\Z")
_STATE_NAME = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]*\Z")


def _resolved_absolute(path: Path, role: str) -> Path:
    if not path.is_absolute():
        raise ValueError(f"unsafe {role} path: {path} is not absolute")
    return path.resolve(strict=False)


def _require_within(path: Path, parent: Path, role: str) -> Path:
    resolved = _resolved_absolute(path, role)
    allowed = parent.resolve(strict=False)
    try:
        relative = resolved.relative_to(allowed)
    except ValueError as error:
        raise ValueError(
            f"unsafe {role} path: {resolved} is not below {allowed}"
        ) from error
    if not relative.parts:
        raise ValueError(f"unsafe {role} path: {resolved} is the allowed parent")
    return resolved


def _require_direct_child(path: Path, parent: Path, role: str) -> Path:
    resolved = _resolved_absolute(path, role)
    allowed = parent.resolve(strict=False)
    if resolved.parent != allowed:
        raise ValueError(
            f"unsafe {role} path: {resolved} is not a direct child of {allowed}"
        )
    return resolved


def _require_nbd_device(path: Path, role: str) -> Path:
    if not _NBD_DEVICE.fullmatch(str(path)):
        raise ValueError(f"{role} must be an absolute /dev/nbdN NBD device: {path}")
    return path


def _integer(values: Mapping[str, str], name: str, default: int) -> int:
    raw = values.get(name, str(default))
    try:
        value = int(raw)
    except ValueError as error:
        raise ValueError(f"{name} must be an integer") from error
    if value <= 0:
        raise ValueError(f"{name} must be positive")
    return value


def _bounded_integer(
    values: Mapping[str, str],
    name: str,
    default: int,
    *,
    minimum: int,
    maximum: int,
) -> int:
    value = _integer(values, name, default)
    if not minimum <= value <= maximum:
        raise ValueError(f"{name} must be between {minimum} and {maximum}")
    return value


@dataclass(frozen=True, slots=True)
class PilotConfig:
    root: Path
    crate: Path
    config_file: Path
    env_file: Path
    binary: Path
    build_receipt: Path
    service: str
    client_service: str
    mount_unit: str
    mountpoint: Path
    metrics_url: str
    integrity_file: Path
    integrity_sha256: str
    metadata_dir: Path
    metadata_file_count: int
    result_dir: Path
    temp_dir: Path
    lock_file: Path
    proc_root: Path
    cgroup_root: Path
    expected_ack_mode: str
    drain_timeout: int
    stop_timeout: int
    profile_timeout: int
    maintenance_isolation_secs: int
    build_target: Path
    profile_target: Path
    cargo: Path
    npm_repo: str
    npm_commit: str
    rust_repo: str
    rust_commit: str
    delete_jobs: int
    raw_sftp_jobs: int
    raw_sftp_per_job_mib: int
    nbd_export: str
    replacement_export: str
    nbd_size_gib: int
    nbd_stripe_lanes: int
    nbd_stripe_kib: int
    nbd_socket: Path
    nbd_device: Path
    pilot_state_root: Path
    ninep_target: str
    migration_device: Path
    migration_mountpoint: Path
    admin_mountpoint: Path
    temporary_max_size_gib: int
    user: str
    group: str

    @classmethod
    def from_environment(cls, root: Path) -> "PilotConfig":
        return cls.from_mapping(root, os.environ)

    @classmethod
    def from_mapping(cls, root: Path, values: Mapping[str, str]) -> "PilotConfig":
        root = root.resolve()
        mountpoint = Path(
            values.get("ZEROFS_PILOT_MOUNTPOINT", "/mnt/storagebox-nbd-pilot")
        )
        result_dir = Path(
            values.get("ZEROFS_PILOT_RESULT_DIR", "/var/tmp/zerofs-pilot-results")
        )
        profile_target = Path(
            values.get("ZEROFS_PROFILE_TARGET_DIR", "/var/tmp/zerofs-profile-target")
        )
        build_target = Path(
            values.get("ZEROFS_BUILD_TARGET_DIR", "/var/tmp/zerofs-build-target")
        )
        cargo = Path(
            values.get("ZEROFS_PILOT_CARGO", str(Path.home() / ".cargo/bin/cargo"))
        )
        config = cls(
            root=root,
            crate=root / "zerofs",
            config_file=Path(
                values.get("ZEROFS_PILOT_CONFIG", "/etc/zerofs/nbd-pilot.toml")
            ),
            env_file=Path(values.get("ZEROFS_PILOT_ENV", "/etc/zerofs/nbd-pilot.env")),
            binary=Path(
                values.get("ZEROFS_PILOT_BINARY", "/usr/local/bin/zerofs-nbd-pilot")
            ),
            build_receipt=Path(
                values.get(
                    "ZEROFS_PILOT_BUILD_RECEIPT",
                    "/usr/local/bin/zerofs-nbd-pilot.build-receipt",
                )
            ),
            service=values.get("ZEROFS_PILOT_SERVICE", "zerofs-nbd-pilot.service"),
            client_service=values.get(
                "ZEROFS_PILOT_CLIENT_SERVICE", "zerofs-nbd-client.service"
            ),
            mount_unit=values.get(
                "ZEROFS_PILOT_MOUNT_UNIT", "mnt-storagebox\\x2dnbd\\x2dpilot.mount"
            ),
            mountpoint=mountpoint,
            metrics_url=values.get(
                "ZEROFS_PILOT_METRICS_URL", "http://127.0.0.1:19567/metrics"
            ),
            integrity_file=Path(
                values.get(
                    "ZEROFS_PILOT_INTEGRITY_FILE", str(mountpoint / "integrity-v2.bin")
                )
            ),
            integrity_sha256=values.get(
                "ZEROFS_PILOT_INTEGRITY_SHA256",
                "db1fb0431bce321750e25a93bd46ce41dd20d6eac1a512a9b56af99d43d43c83",
            ),
            metadata_dir=Path(
                values.get("ZEROFS_PILOT_METADATA_DIR", str(mountpoint / "metadata-v2"))
            ),
            metadata_file_count=_integer(
                values, "ZEROFS_PILOT_METADATA_FILE_COUNT", 1024
            ),
            result_dir=result_dir,
            temp_dir=Path(values.get("ZEROFS_PILOT_TMP_DIR", "/tmp")),
            lock_file=Path(
                values.get("ZEROFS_PILOT_LOCK_FILE", "/var/tmp/zerofs-vm100-pilot.lock")
            ),
            proc_root=Path(values.get("ZEROFS_PILOT_PROC_ROOT", "/proc")),
            cgroup_root=Path(values.get("ZEROFS_PILOT_CGROUP_ROOT", "/sys/fs/cgroup")),
            expected_ack_mode=values.get("ZEROFS_PILOT_EXPECT_ACK_MODE", "memory"),
            drain_timeout=_integer(values, "ZEROFS_PILOT_DRAIN_TIMEOUT", 600),
            stop_timeout=_integer(values, "ZEROFS_PILOT_STOP_TIMEOUT", 60),
            profile_timeout=_integer(values, "ZEROFS_PROFILE_TIMEOUT", 1800),
            maintenance_isolation_secs=_bounded_integer(
                values,
                "ZEROFS_PROFILE_MAINTENANCE_ISOLATION_SECS",
                3600,
                minimum=300,
                maximum=86400,
            ),
            build_target=build_target,
            profile_target=profile_target,
            cargo=cargo,
            npm_repo=values.get(
                "ZEROFS_NPM_WORKLOAD_REPO", "https://github.com/npm/cli.git"
            ),
            npm_commit=values.get(
                "ZEROFS_NPM_WORKLOAD_COMMIT",
                "64763a341e7aa5b456e696f956759bf9b3440dc1",
            ),
            rust_repo=values.get(
                "ZEROFS_RUST_WORKLOAD_REPO", "https://github.com/BurntSushi/ripgrep.git"
            ),
            rust_commit=values.get(
                "ZEROFS_RUST_WORKLOAD_COMMIT",
                "af60c2de9d85e7f3d81c78601669468cf02dabab",
            ),
            delete_jobs=_integer(values, "ZEROFS_DELETE_JOBS", 4),
            raw_sftp_jobs=_integer(values, "ZEROFS_RAW_SFTP_JOBS", 7),
            raw_sftp_per_job_mib=_integer(values, "ZEROFS_RAW_SFTP_PER_JOB_MIB", 128),
            nbd_export=values.get("ZEROFS_PILOT_NBD_EXPORT", "vm100-pilot-64g"),
            replacement_export=values.get(
                "ZEROFS_PILOT_REPLACEMENT_EXPORT", "vm100-pilot-64g-v3"
            ),
            nbd_size_gib=_integer(values, "ZEROFS_PILOT_NBD_SIZE_GIB", 64),
            nbd_stripe_lanes=_integer(values, "ZEROFS_PILOT_NBD_STRIPE_LANES", 4),
            nbd_stripe_kib=_integer(values, "ZEROFS_PILOT_NBD_STRIPE_KIB", 256),
            nbd_socket=Path(
                values.get("ZEROFS_PILOT_NBD_SOCKET", "/run/zerofs-nbd-pilot/nbd.sock")
            ),
            nbd_device=Path(values.get("ZEROFS_PILOT_NBD_DEVICE", "/dev/nbd0")),
            pilot_state_root=Path(
                values.get("ZEROFS_PILOT_STATE_ROOT", "/var/lib/zerofs/nbd-pilot")
            ),
            ninep_target=values.get(
                "ZEROFS_PILOT_9P_TARGET",
                "unix:/run/zerofs-nbd-pilot/9p.sock",
            ),
            migration_device=Path(
                values.get("ZEROFS_PILOT_MIGRATION_DEVICE", "/dev/nbd1")
            ),
            migration_mountpoint=Path(
                values.get(
                    "ZEROFS_PILOT_MIGRATION_MOUNTPOINT",
                    "/mnt/zerofs-nbd-migration",
                )
            ),
            admin_mountpoint=Path(
                values.get("ZEROFS_PILOT_ADMIN_MOUNTPOINT", "/mnt/zerofs-admin")
            ),
            temporary_max_size_gib=_integer(
                values, "ZEROFS_PILOT_TEMPORARY_MAX_SIZE_GIB", 256
            ),
            user=values.get("ZEROFS_PILOT_USER", getpass.getuser()),
            group=values.get(
                "ZEROFS_PILOT_GROUP", values.get("ZEROFS_PILOT_USER", getpass.getuser())
            ),
        )
        config.require_result_dir()
        config.require_temp_dir()
        config.require_build_target()
        config.require_profile_target()
        config.require_helper_mountpoint(config.migration_mountpoint, "migration")
        config.require_helper_mountpoint(config.admin_mountpoint, "admin")
        if config.migration_mountpoint.resolve(
            strict=False
        ) == config.admin_mountpoint.resolve(strict=False):
            raise ValueError("migration and admin mountpoints must be distinct")
        _require_nbd_device(config.nbd_device, "canonical device")
        _require_nbd_device(config.migration_device, "migration device")
        if config.nbd_device == config.migration_device:
            raise ValueError("canonical and migration NBD devices must be distinct")
        config.require_pilot_state_root(config.pilot_state_root)
        return config

    def require_result_dir(self) -> Path:
        return _require_within(self.result_dir, Path("/var/tmp"), "result directory")

    def require_temp_dir(self) -> Path:
        resolved = _resolved_absolute(self.temp_dir, "temporary directory")
        temporary_root = Path("/tmp").resolve(strict=False)
        if resolved == temporary_root:
            return resolved
        return _require_within(self.temp_dir, Path("/var/tmp"), "temporary directory")

    def require_build_target(self) -> Path:
        return _require_within(self.build_target, Path("/var/tmp"), "build target")

    def require_profile_target(self) -> Path:
        return _require_within(self.profile_target, Path("/var/tmp"), "profile target")

    def require_helper_mountpoint(self, path: Path, role: str) -> Path:
        resolved = _require_direct_child(path, Path("/mnt"), f"{role} mountpoint")
        if resolved == self.mountpoint.resolve(strict=False):
            raise ValueError(
                f"unsafe {role} mountpoint path: canonical mountpoint is not disposable"
            )
        return resolved

    def _require_prefixed_child(
        self, path: Path, base_dir: Path, prefix: str, role: str
    ) -> Path:
        resolved = _require_direct_child(path, base_dir, role)
        if not resolved.name.startswith(prefix):
            raise ValueError(f"unsafe {role} path: {resolved} lacks prefix {prefix!r}")
        return resolved

    def require_temp_child(self, path: Path, prefix: str) -> Path:
        self.require_temp_dir()
        return self._require_prefixed_child(
            path, self.temp_dir, prefix, "temporary child"
        )

    def require_result_child(self, path: Path, prefix: str) -> Path:
        self.require_result_dir()
        return self._require_prefixed_child(
            path, self.result_dir, prefix, "result child"
        )

    def require_mount_child(self, path: Path, prefix: str, role: str) -> Path:
        resolved = _require_direct_child(path, self.mountpoint, role)
        if not resolved.name.startswith(prefix):
            raise ValueError(f"unsafe {role} path: {resolved} lacks prefix {prefix!r}")
        return resolved

    def require_pilot_state_root(self, path: Path) -> Path:
        configured = _require_direct_child(
            self.pilot_state_root,
            Path("/var/lib/zerofs"),
            "pilot state root",
        )
        if not _STATE_NAME.fullmatch(configured.name):
            raise ValueError(f"unsafe pilot state root name: {configured.name!r}")
        resolved = _resolved_absolute(path, "pilot state root")
        if resolved != configured:
            raise ValueError(
                f"unsafe pilot state root path: {resolved} is not configured root "
                f"{configured}"
            )
        return self.pilot_state_root

    def require_reset_state_backup(self, path: Path) -> Path:
        state = self.require_pilot_state_root(self.pilot_state_root)
        resolved = _require_direct_child(path, state.parent, "reset state backup")
        expected = re.compile(
            rf"{re.escape(state.name)}-reset-rollback-[0-9a-f]{{32}}\Z"
        )
        if not expected.fullmatch(resolved.name):
            raise ValueError(f"unsafe reset state backup path: {resolved}")
        return path
