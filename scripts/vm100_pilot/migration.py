from __future__ import annotations

import json
import re
import subprocess
import tempfile
import time
import tomllib
from contextlib import contextmanager
from pathlib import Path
from typing import Callable, Iterator, Protocol

from .config import PilotConfig
from .lifecycle import PilotLifecycle
from .runner import Runner
from .system_io import install_config_text


_EXPORT_NAME = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]*\Z")


class ExportNamespace(Protocol):
    def exists(self, name: str) -> bool: ...

    def rename(self, source: str, destination: str) -> None: ...


class LocalExportNamespace:
    """Filesystem-backed export namespace used by tests and local tools."""

    def __init__(self, root: Path) -> None:
        self.root = root

    def _path(self, name: str) -> Path:
        _require_export_name(name)
        return self.root / name

    def exists(self, name: str) -> bool:
        return self._path(name).exists()

    def rename(self, source: str, destination: str) -> None:
        self._path(source).rename(self._path(destination))


class RunnerExportNamespace:
    """Root-owned ZeroFS export namespace exposed through a temporary mount."""

    def __init__(self, root: Path, runner: Runner) -> None:
        self.root = root
        self.runner = runner

    def _path(self, name: str) -> Path:
        _require_export_name(name)
        return self.root / name

    def exists(self, name: str) -> bool:
        return (
            self.runner.run(
                ["test", "-e", self._path(name)],
                sudo=True,
                check=False,
            ).returncode
            == 0
        )

    def rename(self, source: str, destination: str) -> None:
        self.runner.run(
            ["mv", "--", self._path(source), self._path(destination)], sudo=True
        )

    def remove(self, name: str) -> None:
        self.runner.run(["rm", "-rf", "--", self._path(name)], sudo=True)

    def read_text(self, export: str, relative: str) -> str:
        if relative.startswith("/") or ".." in Path(relative).parts:
            raise ValueError(f"invalid export-relative path: {relative!r}")
        return self.runner.run(["cat", self._path(export) / relative], sudo=True).stdout


def _require_export_name(name: str) -> None:
    if not _EXPORT_NAME.fullmatch(name):
        raise ValueError(f"invalid NBD export name: {name!r}")


def rewrite_toml_number(text: str, section: str, key: str, value: int) -> str:
    """Replace one numeric TOML scalar without rewriting the rest of the file."""

    if value <= 0:
        raise ValueError("replacement TOML number must be positive")
    header = f"[{section}]"
    lines = text.splitlines(keepends=True)
    in_section = False
    replaced = 0
    assignment = re.compile(
        rf"^(\s*{re.escape(key)}\s*=\s*)"
        r"[-+]?(?:\d+(?:\.\d*)?|\.\d+)"
        r"(\s*(?:#.*)?(?:\r?\n)?)$"
    )
    for index, line in enumerate(lines):
        stripped = line.strip()
        if stripped.startswith("[") and stripped.endswith("]"):
            in_section = stripped == header
            continue
        if not in_section:
            continue
        match = assignment.fullmatch(line)
        if match is None:
            continue
        lines[index] = f"{match.group(1)}{value}{match.group(2)}"
        replaced += 1
    if replaced != 1:
        raise ValueError(
            f"expected one [{section}] {key} numeric value, found {replaced}"
        )
    return "".join(lines)


def swap_exports(
    namespace: ExportNamespace,
    *,
    canonical: str,
    replacement: str,
    backup: str,
    validate: Callable[[], None],
) -> str:
    """Replace one export name and restore both names if validation fails."""

    for name in (canonical, replacement, backup):
        _require_export_name(name)
    if len({canonical, replacement, backup}) != 3:
        raise ValueError("canonical, replacement, and backup names must be distinct")
    if not namespace.exists(canonical):
        raise FileNotFoundError(f"canonical export is missing: {canonical}")
    if not namespace.exists(replacement):
        raise FileNotFoundError(f"replacement export is missing: {replacement}")
    if namespace.exists(backup):
        raise FileExistsError(f"migration backup already exists: {backup}")

    namespace.rename(canonical, backup)
    try:
        namespace.rename(replacement, canonical)
        validate()
    except BaseException as original:
        rollback_errors: list[str] = []
        try:
            if namespace.exists(canonical):
                namespace.rename(canonical, replacement)
        except BaseException as error:
            rollback_errors.append(f"replacement restore: {error}")
        try:
            if namespace.exists(backup) and not namespace.exists(canonical):
                namespace.rename(backup, canonical)
        except BaseException as error:
            rollback_errors.append(f"canonical restore: {error}")
        if rollback_errors:
            original.add_note("export rollback failures: " + "; ".join(rollback_errors))
        raise
    return backup


class StripedMigrator:
    def __init__(
        self,
        config: PilotConfig,
        runner: Runner,
        lifecycle: PilotLifecycle,
    ) -> None:
        self.config = config
        self.runner = runner
        self.lifecycle = lifecycle

    def _device_size(self, path: Path | None = None) -> int:
        device = (path or self.config.migration_device).name
        output = self.runner.run(
            ["cat", Path("/sys/block") / device / "size"]
        ).stdout.strip()
        return int(output)

    def _wait_device_size(self, *, expected_zero: bool, timeout: int = 30) -> int:
        deadline = time.monotonic() + timeout
        while True:
            size = self._device_size()
            if (size == 0) == expected_zero:
                return size
            if time.monotonic() >= deadline:
                state = "detach" if expected_zero else "attach"
                raise TimeoutError(
                    f"{self.config.migration_device} did not {state}: sectors={size}"
                )
            time.sleep(0.1)

    def _mount_is_active(self, mountpoint: Path) -> bool:
        return (
            self.runner.run(
                ["findmnt", "-rn", "-M", mountpoint], check=False
            ).returncode
            == 0
        )

    def _install_config_text(self, text: str) -> None:
        install_config_text(
            self.runner, self.config, text, prefix="zerofs-pilot-config-"
        )

    def _provision(self, replacement: str) -> None:
        self.runner.run(
            [
                self.config.binary,
                "nbd",
                "provision-striped",
                self.config.ninep_target,
                replacement,
                "--size",
                f"{self.config.nbd_size_gib}GiB",
                "--lanes",
                str(self.config.nbd_stripe_lanes),
                "--stripe-size",
                f"{self.config.nbd_stripe_kib}KiB",
            ],
            sudo=True,
            timeout=900,
            capture=False,
        )

    def _attach_stage(self, replacement: str) -> None:
        self.runner.run(
            [
                "/usr/sbin/nbd-client",
                "-unix",
                self.config.nbd_socket,
                self.config.migration_device,
                "-N",
                replacement,
                "-persist",
                "-timeout",
                "600",
                "-connections",
                "8",
            ],
            sudo=True,
            timeout=120,
        )
        self._wait_device_size(expected_zero=False)

    def _detach_stage(self) -> None:
        if self._mount_is_active(self.config.migration_mountpoint):
            self.runner.run(
                ["umount", self.config.migration_mountpoint], sudo=True, timeout=120
            )
        if self._device_size() != 0:
            self.runner.run(
                ["/usr/sbin/nbd-client", "-d", self.config.migration_device],
                sudo=True,
                timeout=120,
            )
            self._wait_device_size(expected_zero=True)
        self.runner.run(
            ["rmdir", "--", self.config.migration_mountpoint],
            sudo=True,
            check=False,
        )

    def _format_or_reuse_stage(self) -> str:
        filesystem = self.runner.run(
            [
                "blkid",
                "-p",
                "-s",
                "TYPE",
                "-o",
                "value",
                self.config.migration_device,
            ],
            sudo=True,
            check=False,
        ).stdout.strip()
        if not filesystem:
            self.runner.run(
                [
                    "mkfs.xfs",
                    "-f",
                    "-L",
                    "zerofs-pilot",
                    self.config.migration_device,
                ],
                sudo=True,
                timeout=300,
                capture=False,
            )
            filesystem = "xfs"
        if filesystem != "xfs":
            raise RuntimeError(
                f"replacement export contains unsupported filesystem {filesystem!r}"
            )
        self.runner.run(
            ["install", "-d", "-m", "0755", self.config.migration_mountpoint],
            sudo=True,
        )
        self.runner.run(
            [
                "mount",
                "-t",
                "xfs",
                "-o",
                "noatime,nodiscard",
                self.config.migration_device,
                self.config.migration_mountpoint,
            ],
            sudo=True,
        )
        return filesystem

    def _relative_data_path(self, path: Path) -> Path:
        try:
            return path.relative_to(self.config.mountpoint)
        except ValueError as error:
            raise ValueError(
                f"pilot data path must be below {self.config.mountpoint}: {path}"
            ) from error

    def _copy_and_verify(self) -> dict[str, object]:
        source = f"{self.config.mountpoint}/"
        destination = f"{self.config.migration_mountpoint}/"
        common = [
            "rsync",
            "-aHAX",
            "--numeric-ids",
            "--delete",
            "--one-file-system",
            "--omit-dir-times",
        ]
        self.runner.run(
            [*common, source, destination], sudo=True, timeout=1800, capture=False
        )
        self.runner.run(
            ["sync", "-f", self.config.migration_mountpoint],
            sudo=True,
            timeout=900,
        )
        comparison = self.runner.run(
            [
                *common,
                "--checksum",
                "--dry-run",
                "--itemize-changes",
                source,
                destination,
            ],
            sudo=True,
            timeout=1800,
        ).stdout.strip()
        if comparison:
            raise RuntimeError(f"replacement differs after rsync:\n{comparison}")

        integrity = self.config.migration_mountpoint / self._relative_data_path(
            self.config.integrity_file
        )
        metadata = self.config.migration_mountpoint / self._relative_data_path(
            self.config.metadata_dir
        )
        digest = self.runner.run(
            ["sha256sum", integrity], sudo=True, timeout=300
        ).stdout.split()[0]
        if digest != self.config.integrity_sha256:
            raise RuntimeError("replacement integrity sentinel hash mismatch")
        count = self.runner.run(
            ["find", metadata, "-type", "f", "-printf", "."],
            sudo=True,
            timeout=300,
        ).stdout.count(".")
        if count != self.config.metadata_file_count:
            raise RuntimeError(
                f"replacement metadata count is {count}, "
                f"expected {self.config.metadata_file_count}"
            )
        return {"integrity_sha256": digest, "metadata_files": count}

    @contextmanager
    def _admin_namespace(self) -> Iterator[RunnerExportNamespace]:
        mountpoint = self.config.admin_mountpoint
        if self._mount_is_active(mountpoint):
            raise RuntimeError(f"admin mount is already active: {mountpoint}")
        self.runner.run(["install", "-d", "-m", "0700", mountpoint], sudo=True)
        with tempfile.NamedTemporaryFile(
            mode="w+",
            prefix="zerofs-admin-mount-",
            dir=self.config.temp_dir,
        ) as log:
            process = self.runner.spawn(
                [
                    self.config.binary,
                    "mount",
                    self.config.ninep_target,
                    mountpoint,
                    "--writeback",
                    "false",
                    "--relaxed-consistency",
                    "false",
                ],
                sudo=True,
                stdout=log,
                stderr=log,
                stdin=subprocess.DEVNULL,
            )
            try:
                deadline = time.monotonic() + 30
                while not self._mount_is_active(mountpoint):
                    if process.process.poll() is not None:
                        log.seek(0)
                        raise RuntimeError(
                            "temporary ZeroFS admin mount exited: " + log.read().strip()
                        )
                    if time.monotonic() >= deadline:
                        raise TimeoutError(
                            f"admin mount did not become active: {mountpoint}"
                        )
                    time.sleep(0.1)
                yield RunnerExportNamespace(mountpoint / ".nbd", self.runner)
            finally:
                if self._mount_is_active(mountpoint):
                    self.runner.run(
                        ["umount", mountpoint], sudo=True, check=False, timeout=120
                    )
                try:
                    process.process.wait(timeout=20)
                except subprocess.TimeoutExpired:
                    process.terminate()
                self.runner.run(["rmdir", "--", mountpoint], sudo=True, check=False)

    def _verify_layout(self, namespace: RunnerExportNamespace, export: str) -> None:
        marker = json.loads(namespace.read_text(export, ".zerofs-nbd-stripe-v1"))
        expected_members = [
            f"lane-{index}" for index in range(self.config.nbd_stripe_lanes)
        ]
        if marker.get("version") != 1:
            raise RuntimeError(f"unexpected stripe marker version: {marker!r}")
        if marker.get("stripe_bytes") != self.config.nbd_stripe_kib * 1024:
            raise RuntimeError(f"unexpected stripe size: {marker!r}")
        if marker.get("members") != expected_members:
            raise RuntimeError(f"unexpected stripe members: {marker!r}")

    def _cleanup_replacement(self, replacement: str) -> None:
        with self._admin_namespace() as namespace:
            if namespace.exists(self.config.nbd_export) and namespace.exists(
                replacement
            ):
                namespace.remove(replacement)

    def _cutover(self, replacement: str, backup: str) -> None:
        with self._admin_namespace() as namespace:
            self._verify_layout(namespace, replacement)
            self.lifecycle.stop_storage_clients()
            try:
                if self._device_size(self.config.nbd_device) != 0:
                    raise RuntimeError(
                        f"{self.config.nbd_device} remained attached after client stop"
                    )

                def validate() -> None:
                    try:
                        self.lifecycle.start_storage_clients()
                        self._verify_layout(namespace, self.config.nbd_export)
                        self.lifecycle.status()
                    except BaseException:
                        self.lifecycle.stop_storage_clients()
                        raise

                predecessor = swap_exports(
                    namespace,
                    canonical=self.config.nbd_export,
                    replacement=replacement,
                    backup=backup,
                    validate=validate,
                )
            except BaseException as original:
                try:
                    self.lifecycle.start_storage_clients()
                except BaseException as error:
                    original.add_note(f"predecessor restart failed: {error}")
                raise
            namespace.remove(predecessor)

    def run(
        self,
        *,
        replacement_export: str | None = None,
        temporary_max_size_gib: int | None = None,
    ) -> dict[str, object]:
        self.lifecycle.require_vm100()
        if self._device_size() != 0:
            raise RuntimeError(f"{self.config.migration_device} is already attached")
        if self._mount_is_active(self.config.migration_mountpoint):
            raise RuntimeError(
                f"migration mount is already active: {self.config.migration_mountpoint}"
            )
        replacement = replacement_export or self.config.replacement_export
        _require_export_name(replacement)
        if replacement == self.config.nbd_export:
            raise ValueError("replacement export must differ from the canonical export")
        temporary_max = temporary_max_size_gib or self.config.temporary_max_size_gib
        if temporary_max <= self.config.nbd_size_gib * 2:
            raise ValueError("temporary max size must exceed both 64 GiB exports")

        self.lifecycle.status()
        self.lifecycle.drain()
        original_config = self.runner.run(
            ["cat", self.config.config_file], sudo=True
        ).stdout
        settings = tomllib.loads(original_config)
        current_max = float(settings.get("filesystem", {}).get("max_size_gb", 0))
        if current_max <= 0:
            raise RuntimeError("[filesystem] max_size_gb is missing or invalid")
        quota_changed = current_max < temporary_max
        provisioned = False
        cutover_complete = False
        copy_receipt: dict[str, object] = {}
        primary_error: BaseException | None = None
        try:
            if quota_changed:
                self._install_config_text(
                    rewrite_toml_number(
                        original_config,
                        "filesystem",
                        "max_size_gb",
                        temporary_max,
                    )
                )
                self.lifecycle.restart()
                self.lifecycle.status()

            self._provision(replacement)
            provisioned = True
            self._attach_stage(replacement)
            try:
                self._format_or_reuse_stage()
                copy_receipt = self._copy_and_verify()
            finally:
                self._detach_stage()

            backup = f"{self.config.nbd_export}-predecessor"
            self._cutover(replacement, backup)
            cutover_complete = True
            self.lifecycle.drain()
        except BaseException as error:
            primary_error = error
            if provisioned and not cutover_complete:
                try:
                    self._detach_stage()
                    self._cleanup_replacement(replacement)
                    self.lifecycle.drain()
                except BaseException as cleanup_error:
                    error.add_note(f"replacement cleanup failed: {cleanup_error}")
            raise
        finally:
            if quota_changed:
                try:
                    self._install_config_text(original_config)
                    self.lifecycle.restart()
                except BaseException as restore_error:
                    if primary_error is not None:
                        primary_error.add_note(
                            f"quota/config restoration failed: {restore_error}"
                        )
                    else:
                        raise

        status = self.lifecycle.status()
        final_drain = self.lifecycle.drain()
        return {
            "migrated": True,
            "canonical_export": self.config.nbd_export,
            "replacement_source": replacement,
            "size_gib": self.config.nbd_size_gib,
            "stripe_lanes": self.config.nbd_stripe_lanes,
            "stripe_kib": self.config.nbd_stripe_kib,
            "temporary_max_size_gib": temporary_max,
            "copy": copy_receipt,
            "status": status,
            "drain": final_drain,
        }
