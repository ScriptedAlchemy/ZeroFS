from __future__ import annotations

from .protocols import (
    NBD_DEVICE,
    NBD_PORT,
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


def _restart_cycle(context: ScenarioContext) -> tuple[Step, ...]:
    return server_crash_steps(context) + server_steps(context)


def _verify_step(description: str, path: str) -> Step:
    return Step(description, ("sha256sum", path), sudo=True)


def _xfs_over_nbd_restart(context: ScenarioContext) -> ScenarioPlan:
    config = context.config
    mountpoint = _mountpoint(config, "xfs")
    proof = mountpoint / "xfs-restart-proof.bin"
    steps = (
        server_steps(context)
        + nbd_connect_steps(config)
        + (
            Step("format XFS on the disposable device", ("mkfs.xfs", "-f", NBD_DEVICE), sudo=True),
            Step("create the XFS mountpoint", ("mkdir", "-p", str(mountpoint))),
            Step("mount XFS", ("mount", NBD_DEVICE, str(mountpoint)), sudo=True),
            Step(
                "write and fsync a durability proof",
                ("dd", "if=/dev/urandom", f"of={proof}", "bs=1M", "count=8", "conv=fsync"),
                sudo=True,
            ),
            Step("unmount XFS before the crash", ("umount", str(mountpoint)), sudo=True),
        )
        + _restart_cycle(context)
        + (
            Step(
                "reattach the device after restart",
                ("nbd-client", "127.0.0.1", str(NBD_PORT), NBD_DEVICE, "-name", config.unit_name),
                sudo=True,
            ),
            Step("check XFS after crash", ("xfs_repair", "-n", NBD_DEVICE), sudo=True),
            Step("remount XFS", ("mount", NBD_DEVICE, str(mountpoint)), sudo=True),
            _verify_step("verify the durability proof survived", str(proof)),
            Step("unmount XFS", ("umount", str(mountpoint)), sudo=True),
        )
        + nbd_disconnect_steps(config)
        + server_stop_steps(context)
    )
    return ScenarioPlan(
        name="xfs-over-nbd-restart",
        legs=("nbd", "xfs"),
        steps=steps,
        durability_floors=floors_for(config.ack, ("nbd-flush",)),
    )


def _zfs_over_nbd_restart(context: ScenarioContext) -> ScenarioPlan:
    config = context.config
    pool = f"zerofs-tiered-{config.run_uuid}"
    proof = f"/{pool}/zfs-restart-proof.bin"
    steps = (
        server_steps(context)
        + nbd_connect_steps(config)
        + (
            Step(
                "create the disposable run-scoped zpool",
                ("zpool", "create", "-f", pool, NBD_DEVICE),
                sudo=True,
            ),
            Step(
                "write and sync a durability proof onto ZFS",
                ("dd", "if=/dev/urandom", f"of={proof}", "bs=1M", "count=8", "conv=fsync"),
                sudo=True,
            ),
            Step("export the pool before the crash", ("zpool", "export", pool), sudo=True),
        )
        + _restart_cycle(context)
        + (
            Step(
                "reattach the device after restart",
                ("nbd-client", "127.0.0.1", str(NBD_PORT), NBD_DEVICE, "-name", config.unit_name),
                sudo=True,
            ),
            Step("import the pool after crash", ("zpool", "import", pool), sudo=True),
            Step("scrub the pool", ("zpool", "scrub", "-w", pool), sudo=True),
            _verify_step("verify the durability proof survived", proof),
            Step("destroy the disposable pool", ("zpool", "destroy", pool), sudo=True),
        )
        + nbd_disconnect_steps(config)
        + server_stop_steps(context)
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
                        "--config",
                        str(context.zerofs_config),
                    ),
                    sudo=True,
                ),
                Step("wait for protocol listeners", ("sleep", "3")),
            )
        )
        steps.extend(nfs_mount_steps(config))
        steps.extend(nfs_write_commit_steps(config, proof))
        steps.extend(server_crash_steps(context))
        steps.extend(server_steps(context))
        steps.append(
            _verify_step(
                f"verify durability across the {boundary} boundary",
                str(_mountpoint(config, "nfs") / proof),
            )
        )
        steps.extend(nfs_unmount_steps(config))
        steps.extend(server_stop_steps(context))
    return ScenarioPlan(
        name="crash-boundary-matrix",
        legs=("nfs",),
        steps=tuple(steps),
        durability_floors=floors_for(config.ack, ("nfs-commit",)),
    )


def _local_receipt_restart(context: ScenarioContext) -> ScenarioPlan:
    config = context.config
    proof = "local-receipt-proof.bin"
    steps = (
        server_steps(context)
        + nfs_mount_steps(config)
        + nfs_write_commit_steps(config, proof)
        + nfs_unmount_steps(config)
        + _restart_cycle(context)
        + nfs_mount_steps(config)
        + (
            _verify_step(
                "verify the locally-receipted write survived restart",
                str(_mountpoint(config, "nfs") / proof),
            ),
        )
        + nfs_unmount_steps(config)
        + server_stop_steps(context)
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
    steps = (
        server_steps(context)
        + nfs_mount_steps(config)
        + nfs_write_commit_steps(config, proof)
        + nfs_unmount_steps(config)
        + server_crash_steps(context)
        + (
            Step(
                "wipe the local cache so only the remote receipt remains",
                ("rm", "-rf", "--", str(cache)),
                sudo=True,
            ),
        )
        + server_steps(context)
        + nfs_mount_steps(config)
        + (
            _verify_step(
                "verify the remotely-receipted write survived a clean-cache restart",
                str(_mountpoint(config, "nfs") / proof),
            ),
        )
        + nfs_unmount_steps(config)
        + server_stop_steps(context)
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
    steps = (
        server_steps(context)
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
        + (
            Step(
                "stop with the shutdown timeout armed",
                ("systemctl", "stop", unit),
                sudo=True,
            ),
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
