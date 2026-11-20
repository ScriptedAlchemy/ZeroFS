from __future__ import annotations

import os
from pathlib import Path

from .config import ConfigError, HarnessConfig
from .protocols import (
    NBD_CLIENT,
    NBD_DEVICE,
    NBD_PORT,
    METRICS_PORT,
    NFS_PORT,
    ResourceOwnership,
    ScenarioBuilder,
    ScenarioContext,
    ScenarioPlan,
    Step,
    _mountpoint,
    nbd_connect_steps,
    nbd_disconnect_steps,
    nfs_mount_steps,
    nfs_unmount_steps,
    nfs_write_commit_steps,
    server_crash_steps,
    server_steps,
    server_stop_steps,
)
from .integrity import floors_for

# Crash boundaries the failpoint matrix must cover: before the local journal
# record, after the journal but before remote upload, and after remote ack.
CRASH_BOUNDARIES = ("pre-journal", "post-journal-pre-remote", "post-remote")
LINUX_SUN_LEN = 108


def xfs_bootstrap_socket(config: HarnessConfig) -> Path:
    path = config.run_root / "9p.sock"
    encoded_length = len(os.fsencode(path))
    if encoded_length >= LINUX_SUN_LEN:
        raise ConfigError(
            f"bootstrap Unix socket path is {encoded_length} bytes; "
            f"it must be shorter than SUN_LEN ({LINUX_SUN_LEN})"
        )
    return path


def _restart_cycle(
    context: ScenarioContext, listeners: tuple[ResourceOwnership, ...]
) -> tuple[Step, ...]:
    return server_crash_steps(context, listeners) + server_steps(context, listeners)


def _verify_step(description: str, path: str) -> Step:
    return Step(description, ("sha256sum", path), sudo=True)


def _xfs_over_nbd_restart(context: ScenarioContext) -> ScenarioPlan:
    config = context.config
    mountpoint = _mountpoint(config, "xfs")
    proof = mountpoint / "xfs-restart-proof.bin"
    bootstrap_config = config.run_root / "xfs-nbd-bootstrap.toml"
    bootstrap_socket = xfs_bootstrap_socket(config)
    bootstrap_unit = f"{config.unit_name}-bootstrap"
    listeners = (
        ResourceOwnership("listener", NBD_PORT),
        ResourceOwnership("listener", METRICS_PORT),
    )
    steps = (
        (
            Step(
                "start the run-scoped bootstrap server",
                (
                    "systemd-run",
                    "--collect",
                    f"--unit={bootstrap_unit}",
                    f"--uid={os.getuid()}",
                    str(context.zerofs_binary),
                    "run",
                    "--config",
                    str(bootstrap_config),
                ),
                sudo=True,
                acquires=(
                    ResourceOwnership("unit", bootstrap_unit),
                    ResourceOwnership("path", str(bootstrap_socket)),
                ),
                capture_main_pid_unit=bootstrap_unit,
            ),
            Step(
                "wait for and verify the bootstrap 9P socket",
                ("test", "-S", str(bootstrap_socket)),
                requires=(ResourceOwnership("path", str(bootstrap_socket)),),
                wait_for_unix_socket=str(bootstrap_socket),
            ),
            Step(
                "provision the exact sparse striped NBD export",
                (
                    str(context.zerofs_binary),
                    "nbd",
                    "provision-striped",
                    f"unix:{bootstrap_socket}",
                    config.unit_name,
                    "--size",
                    "4GiB",
                    "--lanes",
                    "4",
                    "--stripe-size",
                    "1MiB",
                ),
            ),
            Step(
                "stop the run-scoped bootstrap server",
                ("systemctl", "stop", bootstrap_unit),
                sudo=True,
                releases=(ResourceOwnership("unit", bootstrap_unit),),
                release_main_pid_unit=bootstrap_unit,
                verify_stopped_unit=bootstrap_unit,
            ),
            Step(
                "remove the retired bootstrap socket",
                ("rm", "-f", "--", str(bootstrap_socket)),
                releases=(ResourceOwnership("path", str(bootstrap_socket)),),
            ),
        )
        + server_steps(
            context,
            listeners,
            after_checkpoint="pin-initial-authority",
            unit_uid=os.getuid(),
        )
        + nbd_connect_steps(config)
        + (
            Step(
                "verify the attached sparse export has the expected size",
                ("blockdev", "--getsize64", NBD_DEVICE),
                sudo=True,
                requires=(ResourceOwnership("device", NBD_DEVICE),),
                require_stdout=str(4 * 1024 * 1024 * 1024),
            ),
            Step(
                "format XFS on the disposable device",
                ("mkfs.xfs", "-f", NBD_DEVICE),
                sudo=True,
            ),
            Step("create the XFS mountpoint", ("mkdir", "-p", str(mountpoint))),
            Step(
                "mount XFS",
                ("mount", NBD_DEVICE, str(mountpoint)),
                sudo=True,
                acquires=(ResourceOwnership("mount", str(mountpoint)),),
            ),
            Step(
                "write and fsync a durability proof",
                (
                    "dd",
                    "if=/dev/urandom",
                    f"of={proof}",
                    "bs=1M",
                    "count=8",
                    "conv=fsync",
                ),
                sudo=True,
            ),
            Step(
                "issue a real NBD FLUSH after the XFS proof fsync",
                ("blockdev", "--flushbufs", NBD_DEVICE),
                sudo=True,
                requires=(ResourceOwnership("device", NBD_DEVICE),),
            ),
            Step(
                "capture the durability proof checksum before restart",
                ("sha256sum", str(proof)),
                sudo=True,
                capture_sha256_as="xfs-proof-before-restart",
            ),
            Step(
                "unmount XFS before the crash",
                ("umount", str(mountpoint)),
                sudo=True,
                releases=(ResourceOwnership("mount", str(mountpoint)),),
            ),
        )
        + (
            Step(
                "detach NBD before the crash and observe the final local cutoff",
                (NBD_CLIENT, "-d", NBD_DEVICE),
                sudo=True,
                releases=(ResourceOwnership("device", NBD_DEVICE),),
                after_checkpoint="final-local-cutoff",
                verify_detached_device=NBD_DEVICE,
            ),
        )
        + server_crash_steps(context, listeners)
        + server_steps(
            context,
            listeners,
            after_checkpoint="require-restart",
            unit_uid=os.getuid(),
        )
        + nbd_connect_steps(config)
        + (
            Step(
                "verify the restarted export has the same sparse size",
                ("blockdev", "--getsize64", NBD_DEVICE),
                sudo=True,
                requires=(ResourceOwnership("device", NBD_DEVICE),),
                require_stdout=str(4 * 1024 * 1024 * 1024),
            ),
            Step("check XFS after crash", ("xfs_repair", "-n", NBD_DEVICE), sudo=True),
            Step(
                "remount XFS",
                ("mount", NBD_DEVICE, str(mountpoint)),
                sudo=True,
                acquires=(ResourceOwnership("mount", str(mountpoint)),),
            ),
            Step(
                "verify the durability proof checksum survived",
                ("sha256sum", str(proof)),
                sudo=True,
                compare_sha256_with="xfs-proof-before-restart",
            ),
            Step(
                "unmount XFS",
                ("umount", str(mountpoint)),
                sudo=True,
                releases=(ResourceOwnership("mount", str(mountpoint)),),
            ),
        )
        + nbd_disconnect_steps(config)
        + server_stop_steps(context, listeners)
    )
    return ScenarioPlan(
        name="xfs-over-nbd-restart",
        legs=("nbd", "xfs"),
        steps=steps,
        durability_floors=floors_for(config.ack, ("nbd-flush",)),
        bootstrap_config=bootstrap_config,
        authority_export_id=config.unit_name,
        requires_completed_cleanup=True,
    )


def _zfs_over_nbd_restart(context: ScenarioContext) -> ScenarioPlan:
    config = context.config
    pool = f"zerofs-tiered-{config.run_uuid}"
    proof = f"/{pool}/zfs-restart-proof.bin"
    listeners = (ResourceOwnership("listener", NBD_PORT),)
    steps = (
        server_steps(context, listeners)
        + nbd_connect_steps(config)
        + (
            Step(
                "create the disposable run-scoped zpool",
                ("zpool", "create", "-f", pool, NBD_DEVICE),
                sudo=True,
                acquires=(ResourceOwnership("pool", pool),),
            ),
            Step(
                "write and sync a durability proof onto ZFS",
                (
                    "dd",
                    "if=/dev/urandom",
                    f"of={proof}",
                    "bs=1M",
                    "count=8",
                    "conv=fsync",
                ),
                sudo=True,
            ),
            Step(
                "export the pool before the crash", ("zpool", "export", pool), sudo=True
            ),
        )
        + _restart_cycle(context, listeners)
        + (
            Step(
                "reattach the device after restart",
                (
                    NBD_CLIENT,
                    "127.0.0.1",
                    str(NBD_PORT),
                    NBD_DEVICE,
                    "-name",
                    config.unit_name,
                ),
                sudo=True,
                requires=(ResourceOwnership("device", NBD_DEVICE),),
            ),
            Step(
                "import the pool after crash",
                ("zpool", "import", pool),
                sudo=True,
                requires=(ResourceOwnership("pool", pool),),
            ),
            Step("scrub the pool", ("zpool", "scrub", "-w", pool), sudo=True),
            _verify_step("verify the durability proof survived", proof),
            Step(
                "destroy the disposable pool",
                ("zpool", "destroy", pool),
                sudo=True,
                releases=(ResourceOwnership("pool", pool),),
            ),
        )
        + nbd_disconnect_steps(config)
        + server_stop_steps(context, listeners)
    )
    return ScenarioPlan(
        name="zfs-over-nbd-restart",
        legs=("nbd", "zfs"),
        steps=steps,
        durability_floors=floors_for(config.ack, ("nbd-flush",)),
    )


def _crash_boundary_matrix(context: ScenarioContext) -> ScenarioPlan:
    config = context.config
    unit = config.unit_name
    listeners = (ResourceOwnership("listener", NFS_PORT),)
    steps: list[Step] = []
    for boundary in CRASH_BOUNDARIES:
        proof = f"boundary-{boundary}.bin"
        steps.extend(
            (
                Step(
                    f"start ZeroFS with the {boundary} failpoint armed",
                    (
                        "systemd-run",
                        "--collect",
                        f"--unit={unit}",
                        f"--setenv=ZEROFS_FAILPOINT={boundary}",
                        str(context.zerofs_binary),
                        "run",
                        "--config",
                        str(context.zerofs_config),
                    ),
                    sudo=True,
                    acquires=(ResourceOwnership("unit", unit),) + listeners,
                    capture_main_pid_unit=unit,
                ),
                Step("wait for protocol listeners", ("sleep", "3")),
            )
        )
        steps.extend(nfs_mount_steps(config))
        steps.extend(nfs_write_commit_steps(config, proof))
        steps.extend(server_crash_steps(context, listeners))
        steps.extend(server_steps(context, listeners))
        steps.append(
            _verify_step(
                f"verify durability across the {boundary} boundary",
                str(_mountpoint(config, "nfs") / proof),
            )
        )
        steps.extend(nfs_unmount_steps(config))
        steps.extend(server_stop_steps(context, listeners))
    return ScenarioPlan(
        name="crash-boundary-matrix",
        legs=("nfs",),
        steps=tuple(steps),
        durability_floors=floors_for(config.ack, ("nfs-commit",)),
    )


def _local_receipt_restart(context: ScenarioContext) -> ScenarioPlan:
    config = context.config
    proof = "local-receipt-proof.bin"
    listeners = (ResourceOwnership("listener", NFS_PORT),)
    steps = (
        server_steps(context, listeners)
        + nfs_mount_steps(config)
        + nfs_write_commit_steps(config, proof)
        + nfs_unmount_steps(config)
        + _restart_cycle(context, listeners)
        + nfs_mount_steps(config)
        + (
            _verify_step(
                "verify the locally-receipted write survived restart",
                str(_mountpoint(config, "nfs") / proof),
            ),
        )
        + nfs_unmount_steps(config)
        + server_stop_steps(context, listeners)
    )
    return ScenarioPlan(
        name="local-receipt-restart",
        legs=("nfs",),
        steps=steps,
        durability_floors=floors_for(config.ack, ("nfs-commit",)),
    )


def _remote_receipt_clean_cache_restart(context: ScenarioContext) -> ScenarioPlan:
    config = context.config
    proof = "remote-receipt-proof.bin"
    cache = config.resource_root / "cache"
    listeners = (ResourceOwnership("listener", NFS_PORT),)
    steps = (
        server_steps(context, listeners)
        + nfs_mount_steps(config)
        + nfs_write_commit_steps(config, proof)
        + nfs_unmount_steps(config)
        + server_crash_steps(context, listeners)
        + (
            Step(
                "wipe the local cache so only the remote receipt remains",
                ("rm", "-rf", "--", str(cache)),
                sudo=True,
            ),
        )
        + server_steps(context, listeners)
        + nfs_mount_steps(config)
        + (
            _verify_step(
                "verify the remotely-receipted write survived a clean-cache restart",
                str(_mountpoint(config, "nfs") / proof),
            ),
        )
        + nfs_unmount_steps(config)
        + server_stop_steps(context, listeners)
    )
    return ScenarioPlan(
        name="remote-receipt-clean-cache-restart",
        legs=("nfs",),
        steps=steps,
        durability_floors=floors_for(config.ack, ("nfs-commit",)),
    )


def _terminal_fanout_and_shutdown_timeout(context: ScenarioContext) -> ScenarioPlan:
    config = context.config
    unit = config.unit_name
    listeners = (ResourceOwnership("listener", NFS_PORT),)
    steps = (
        server_steps(context, listeners)
        + nfs_mount_steps(config)
        + (
            Step(
                "issue writes that stay pending at shutdown",
                (
                    "dd",
                    "if=/dev/urandom",
                    f"of={_mountpoint(config, 'nfs') / 'pending-at-shutdown.bin'}",
                    "bs=1M",
                    "count=64",
                ),
                sudo=True,
            ),
        )
        + nfs_unmount_steps(config)
        + server_stop_steps(context, listeners)
        + (
            Step(
                "collect the terminal fanout from the unit journal",
                ("journalctl", "-u", unit, "--no-pager"),
                sudo=True,
            ),
        )
    )
    return ScenarioPlan(
        name="terminal-fanout-and-shutdown-timeout",
        legs=("nfs",),
        steps=steps,
        durability_floors=floors_for(config.ack, ("shutdown-terminal",)),
    )


CRASH_SCENARIOS: dict[str, ScenarioBuilder] = {
    "xfs-over-nbd-restart": _xfs_over_nbd_restart,
    "zfs-over-nbd-restart": _zfs_over_nbd_restart,
    "crash-boundary-matrix": _crash_boundary_matrix,
    "local-receipt-restart": _local_receipt_restart,
    "remote-receipt-clean-cache-restart": _remote_receipt_clean_cache_restart,
    "terminal-fanout-and-shutdown-timeout": _terminal_fanout_and_shutdown_timeout,
}
