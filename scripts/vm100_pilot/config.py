from __future__ import annotations

import getpass
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Mapping


def _integer(values: Mapping[str, str], name: str, default: int) -> int:
    raw = values.get(name, str(default))
    try:
        value = int(raw)
    except ValueError as error:
        raise ValueError(f"{name} must be an integer") from error
    if value <= 0:
        raise ValueError(f"{name} must be positive")
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
        config.require_disposable(config.result_dir)
        config.require_disposable(config.build_target)
        config.require_disposable(config.profile_target)
        config.require_disposable(config.migration_mountpoint)
        config.require_disposable(config.admin_mountpoint)
        return config

    def require_disposable(self, path: Path) -> Path:
        resolved = path.resolve(strict=False)
        forbidden = {Path("/"), Path("/fast"), self.mountpoint.resolve(strict=False)}
        if resolved in forbidden or Path("/fast") in resolved.parents:
            raise ValueError(f"unsafe disposable path: {resolved}")
        return resolved
