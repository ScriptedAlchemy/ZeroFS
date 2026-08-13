from __future__ import annotations

import re
import tempfile
import tomllib
import uuid
from pathlib import Path
from urllib.parse import urlsplit, urlunsplit

from .config import PilotConfig
from .lifecycle import PilotLifecycle
from .runner import Runner


_REMOTE_PREFIX = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]*\Z")


def rewrite_storage_prefix(text: str, new_prefix: str) -> str:
    """Replace the single SFTP storage path without rewriting other TOML."""

    if not _REMOTE_PREFIX.fullmatch(new_prefix):
        raise ValueError(f"invalid fresh remote prefix: {new_prefix!r}")
    settings = tomllib.loads(text)
    current_url = settings.get("storage", {}).get("url")
    if not isinstance(current_url, str):
        raise ValueError("[storage] url is missing or invalid")
    parsed = urlsplit(current_url)
    if parsed.scheme != "sftp" or not parsed.netloc:
        raise ValueError("fresh reset requires an SFTP storage URL")
    current_prefix = parsed.path.strip("/")
    if not current_prefix or "/" in current_prefix:
        raise ValueError("fresh reset requires a single-component storage prefix")
    if current_prefix == new_prefix:
        raise ValueError("fresh remote prefix must differ from the current prefix")
    replacement_url = urlunsplit(
        (parsed.scheme, parsed.netloc, f"/{new_prefix}", parsed.query, parsed.fragment)
    )

    lines = text.splitlines(keepends=True)
    in_storage = False
    replaced = 0
    assignment = re.compile(r'^(\s*url\s*=\s*)"[^"]*"(\s*(?:#.*)?(?:\r?\n)?)$')
    for index, line in enumerate(lines):
        stripped = line.strip()
        if stripped.startswith("[") and stripped.endswith("]"):
            in_storage = stripped == "[storage]"
            continue
        if not in_storage:
            continue
        match = assignment.fullmatch(line)
        if match is None:
            continue
        lines[index] = f'{match.group(1)}"{replacement_url}"{match.group(2)}'
        replaced += 1
    if replaced != 1:
        raise ValueError(f"expected one [storage] url value, found {replaced}")
    return "".join(lines)


class FreshResetter:
    def __init__(
        self,
        config: PilotConfig,
        runner: Runner,
        lifecycle: PilotLifecycle,
    ) -> None:
        self.config = config
        self.runner = runner
        self.lifecycle = lifecycle

    @property
    def _device(self) -> Path:
        return self.config.nbd_device

    def _read_config(self) -> str:
        return self.runner.run(["cat", self.config.config_file], sudo=True).stdout

    def _install_config_text(self, text: str) -> None:
        with tempfile.NamedTemporaryFile(
            mode="w",
            prefix="zerofs-pilot-reset-config-",
            dir=self.config.temp_dir,
            delete=False,
        ) as handle:
            handle.write(text)
            temporary = Path(handle.name)
        try:
            self.runner.run(
                [
                    "install",
                    "-o",
                    "root",
                    "-g",
                    "root",
                    "-m",
                    "0600",
                    temporary,
                    self.config.config_file,
                ],
                sudo=True,
            )
        finally:
            temporary.unlink(missing_ok=True)

    def _backup_config(self) -> Path:
        backup = self.config.config_file.with_name(
            f"{self.config.config_file.name}.reset-rollback-{uuid.uuid4().hex}"
        )
        self.runner.run(["cp", "-a", self.config.config_file, backup], sudo=True)
        return backup

    def _remove_config_backup(self, backup: Path) -> None:
        self.runner.run(["rm", "-f", "--", backup], sudo=True)

    def _relative_fixture(self, path: Path) -> Path:
        try:
            return path.relative_to(self.config.mountpoint)
        except ValueError as error:
            raise ValueError(
                f"pilot fixture must be below {self.config.mountpoint}: {path}"
            ) from error

    def _verify_fixtures(self, root: Path) -> dict[str, object]:
        integrity = root / self._relative_fixture(self.config.integrity_file)
        metadata = root / self._relative_fixture(self.config.metadata_dir)
        digest = self.runner.run(
            ["sha256sum", integrity], sudo=True, timeout=300
        ).stdout.split()[0]
        if digest != self.config.integrity_sha256:
            raise RuntimeError("fresh-reset integrity sentinel hash mismatch")
        count = self.runner.run(
            ["find", metadata, "-type", "f", "-printf", "."],
            sudo=True,
            timeout=300,
        ).stdout.count(".")
        if count != self.config.metadata_file_count:
            raise RuntimeError(
                f"fresh-reset metadata count is {count}, "
                f"expected {self.config.metadata_file_count}"
            )
        return {"integrity_sha256": digest, "metadata_files": count}

    def _stage_fixtures(self) -> Path:
        seed = self.config.result_dir / f"reset-seed-{uuid.uuid4().hex}"
        self.config.require_result_child(seed, "reset-seed-")
        self.runner.run(
            ["install", "-d", "-o", "root", "-g", "root", "-m", "0700", seed],
            sudo=True,
        )
        sources = []
        for fixture in (self.config.integrity_file, self.config.metadata_dir):
            relative = self._relative_fixture(fixture)
            sources.append(f"{self.config.mountpoint}/./{relative}")
        self.runner.run(
            [
                "rsync",
                "-aHAX",
                "--numeric-ids",
                "--relative",
                "--",
                *sources,
                f"{seed}/",
            ],
            sudo=True,
            timeout=900,
            capture=False,
        )
        self.runner.run(["sync", "-f", seed], sudo=True, timeout=300)
        self._verify_fixtures(seed)
        return seed

    def _remove_seed(self, seed: Path) -> None:
        self.config.require_result_child(seed, "reset-seed-")
        self.runner.run(["rm", "-rf", "--", seed], sudo=True)

    def _state_root(self) -> Path:
        settings = tomllib.loads(self._read_config())
        cache_dir = Path(str(settings.get("cache", {}).get("dir", "")))
        writeback_dir = Path(str(settings.get("writeback", {}).get("dir", "")))
        state = self.config.require_pilot_state_root(self.config.pilot_state_root)
        expected_cache = (state / "read-cache").resolve(strict=False)
        expected_writeback = (state / "writeback").resolve(strict=False)
        if (
            cache_dir.resolve(strict=False) != expected_cache
            or writeback_dir.resolve(strict=False) != expected_writeback
        ):
            raise ValueError(
                "pilot cache/writeback directories must be the expected children "
                "of the configured pilot state root"
            )
        return state

    def _activate_fresh_state(self) -> Path:
        state = self._state_root()
        backup = state.with_name(f"{state.name}-reset-rollback-{uuid.uuid4().hex}")
        self.config.require_reset_state_backup(backup)
        exists = self.runner.run(["test", "-e", backup], sudo=True, check=False)
        if exists.returncode == 0:
            raise FileExistsError(f"reset state backup exists: {backup}")
        self.runner.run(["mv", "--", state, backup], sudo=True)
        try:
            self.runner.run(
                [
                    "install",
                    "-d",
                    "-o",
                    "root",
                    "-g",
                    "root",
                    "-m",
                    "0750",
                    state,
                ],
                sudo=True,
            )
        except BaseException:
            self.runner.run(["mv", "--", backup, state], sudo=True)
            raise
        return backup

    def _restore_old_state(self, backup: Path) -> None:
        state = self._state_root()
        self.config.require_reset_state_backup(backup)
        self.runner.run(["rm", "-rf", "--", state], sudo=True)
        self.runner.run(["mv", "--", backup, state], sudo=True)

    def _remove_old_state(self, backup: Path) -> None:
        self._state_root()
        self.config.require_reset_state_backup(backup)
        self.runner.run(["rm", "-rf", "--", backup], sudo=True)

    def _reset_nbd_module(self) -> None:
        parameters: dict[str, int] = {}
        for name in ("nbds_max", "max_part"):
            path = Path("/sys/module/nbd/parameters") / name
            raw = self.runner.run(["cat", path]).stdout.strip()
            try:
                value = int(raw)
            except ValueError as error:
                raise RuntimeError(f"invalid NBD module parameter {path}: {raw!r}") from error
            if value < 0 or (name == "nbds_max" and value == 0):
                raise RuntimeError(f"invalid NBD module parameter {path}: {value}")
            parameters[name] = value

        output = self.runner.run(
            [
                "find",
                "/sys/block",
                "-maxdepth",
                "1",
                "-type",
                "l",
                "-name",
                "nbd*",
                "-printf",
                "%f\n",
            ]
        ).stdout
        devices = sorted(set(output.splitlines()))
        if not devices or any(re.fullmatch(r"nbd\d+", device) is None for device in devices):
            raise RuntimeError(f"could not enumerate NBD devices safely: {devices!r}")

        attached: list[str] = []
        for device in devices:
            size_path = Path("/sys/block") / device / "size"
            raw_size = self.runner.run(["cat", size_path]).stdout.strip()
            try:
                size = int(raw_size)
            except ValueError as error:
                raise RuntimeError(f"invalid NBD size {size_path}: {raw_size!r}") from error
            pid_result = self.runner.run(
                ["cat", Path("/sys/block") / device / "pid"], check=False
            )
            pid = pid_result.stdout.strip() if pid_result.returncode == 0 else ""
            if size != 0 or pid not in {"", "0"}:
                attached.append(f"{device} size={size} pid={pid or 'none'}")
        if attached:
            raise RuntimeError(
                "refusing to reload nbd while a device is attached: "
                + "; ".join(attached)
            )

        self.runner.run(["modprobe", "-r", "nbd"], sudo=True, timeout=60)
        self.runner.run(
            [
                "modprobe",
                "nbd",
                f"nbds_max={parameters['nbds_max']}",
                f"max_part={parameters['max_part']}",
            ],
            sudo=True,
            timeout=60,
        )

    def _provision(self) -> None:
        self.runner.run(
            [
                self.config.binary,
                "nbd",
                "provision-striped",
                self.config.ninep_target,
                self.config.nbd_export,
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

    def _verify_stripe_layout(self) -> None:
        # Reuse the migration's independently mounted namespace validator.
        from .migration import StripedMigrator

        helper = StripedMigrator(self.config, self.runner, self.lifecycle)
        with helper._admin_namespace() as namespace:
            helper._verify_layout(namespace, self.config.nbd_export)

    def _format_device(self) -> None:
        expected_sectors = self.config.nbd_size_gib * 1024**3 // 512
        sectors = int(
            self.runner.run(
                ["cat", Path("/sys/block") / self._device.name / "size"]
            ).stdout.strip()
        )
        if sectors != expected_sectors:
            raise RuntimeError(
                f"{self._device} has {sectors} sectors, expected {expected_sectors}"
            )
        mounted = self.runner.run(["findmnt", "-rn", "-S", self._device], check=False)
        if mounted.returncode == 0:
            raise RuntimeError(f"fresh device is already mounted: {self._device}")
        signatures = self.runner.run(
            ["wipefs", "-n", self._device], sudo=True, check=False
        ).stdout.strip()
        blkid = self.runner.run(
            ["blkid", "-p", self._device], sudo=True, check=False
        ).stdout.strip()
        if signatures or blkid:
            raise RuntimeError(
                f"fresh device has an existing block signature: {self._device}"
            )
        self.runner.run(
            ["mkfs.xfs", "-L", "zerofs-pilot", self._device],
            sudo=True,
            timeout=300,
            capture=False,
        )

    def _restore_fixtures(self, seed: Path) -> None:
        self.runner.run(
            [
                "rsync",
                "-aHAX",
                "--numeric-ids",
                "--",
                f"{seed}/",
                f"{self.config.mountpoint}/",
            ],
            sudo=True,
            timeout=900,
            capture=False,
        )
        self.runner.run(["sync", "-f", self.config.mountpoint], sudo=True, timeout=900)
        self._verify_fixtures(self.config.mountpoint)

    def _fast_topology(self) -> str:
        result = self.runner.run(
            ["findmnt", "-rn", "-T", "/fast", "-o", "SOURCE,FSTYPE,TARGET"],
            check=False,
        )
        if result.returncode != 0 or not result.stdout.strip():
            # Unit tests use a runner without a real mount table; production VM100
            # must always prove the protected /fast mapping.
            hostname = self.runner.run(["hostname"], timeout=5).stdout.strip()
            if hostname == "ubuntu-main":
                raise RuntimeError("protected /fast topology is unavailable")
        return result.stdout.strip()

    def run(
        self, *, remote_prefix: str, confirm_destroy_pilot: bool = False
    ) -> dict[str, object]:
        if not confirm_destroy_pilot:
            raise ValueError("fresh reset requires --confirm-destroy-pilot")
        self.lifecycle.require_vm100()
        before_status = self.lifecycle.status()
        fast_before = self._fast_topology()
        original_config = self._read_config()
        new_config = rewrite_storage_prefix(original_config, remote_prefix)
        seed = self._stage_fixtures()
        try:
            config_backup = self._backup_config()
        except BaseException as original:
            try:
                self._remove_seed(seed)
            except BaseException as cleanup_error:
                original.add_note(f"reset seed cleanup failed: {cleanup_error}")
            raise
        state_backup: Path | None = None
        try:
            self.lifecycle.stop()
            self._reset_nbd_module()
            self._install_config_text(new_config)
            state_backup = self._activate_fresh_state()
            self.lifecycle.start_daemon()
            self._provision()
            self.lifecycle.start_client()
            self._verify_stripe_layout()
            self._format_device()
            self.lifecycle.start_mount()
            self._restore_fixtures(seed)
            status = self.lifecycle.status()
            drain = self.lifecycle.drain()
            fast_after = self._fast_topology()
            if fast_after != fast_before:
                raise RuntimeError(
                    f"protected /fast topology changed: {fast_before!r} -> {fast_after!r}"
                )
        except BaseException as original:
            rollback_errors: list[str] = []
            for operation in (
                lambda: self.lifecycle.stop(),
                lambda: self._install_config_text(original_config),
                lambda: self._restore_old_state(state_backup)
                if state_backup is not None
                else None,
                lambda: self.lifecycle.start(),
                lambda: self.lifecycle.status(),
            ):
                try:
                    operation()
                except BaseException as error:
                    rollback_errors.append(str(error))
            try:
                if self._fast_topology() != fast_before:
                    rollback_errors.append("protected /fast topology changed")
            except BaseException as error:
                rollback_errors.append(str(error))
            if rollback_errors:
                original.add_note(
                    "fresh-reset rollback failures: " + "; ".join(rollback_errors)
                )
            else:
                try:
                    self._remove_config_backup(config_backup)
                except BaseException as cleanup_error:
                    original.add_note(
                        f"reset config backup cleanup failed: {cleanup_error}"
                    )
            try:
                self._remove_seed(seed)
            except BaseException as cleanup_error:
                original.add_note(f"reset seed cleanup failed: {cleanup_error}")
            raise

        # Validation commits the new stack. Cleanup errors after this point must
        # never roll the working replacement back to the destroyed predecessor.
        self._remove_old_state(state_backup)
        self._remove_config_backup(config_backup)
        self._remove_seed(seed)
        return {
            "reset": True,
            "remote_prefix": remote_prefix,
            "canonical_export": self.config.nbd_export,
            "size_gib": self.config.nbd_size_gib,
            "stripe_lanes": self.config.nbd_stripe_lanes,
            "stripe_kib": self.config.nbd_stripe_kib,
            "protected_fast_topology": fast_after,
            "before": before_status,
            "status": status,
            "drain": drain,
        }
