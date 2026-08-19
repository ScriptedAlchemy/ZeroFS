from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable

from .config import ConfigError, HarnessConfig, validate_owned_path
from .integrity import DurabilityFloor, floors_for

# Loopback endpoints the harness assigns to the run-scoped ZeroFS instance.
NFS_PORT = 12049
NINEP_PORT = 15564
NBD_PORT = 10809
WEBUI_PORT = 18080
RPC_PORT = 18081
NBD_DEVICE = "/dev/nbd7"


@dataclass(frozen=True, slots=True)
class ResourceOwnership:
    kind: str
    value: str | int

    def to_dict(self) -> dict[str, str | int]:
        return {"kind": self.kind, "value": self.value}


@dataclass(frozen=True, slots=True)
class Step:
    """One real command in a scenario plan."""

    description: str
    argv: tuple[str, ...]
    sudo: bool = False
    cwd: str | None = None
    acquires: tuple[ResourceOwnership, ...] = ()
    releases: tuple[ResourceOwnership, ...] = ()
    requires: tuple[ResourceOwnership, ...] = ()
    capture_main_pid_unit: str | None = None
    release_main_pid_unit: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "description": self.description,
            "argv": list(self.argv),
            "sudo": self.sudo,
            "cwd": self.cwd,
            "acquires": [resource.to_dict() for resource in self.acquires],
            "releases": [resource.to_dict() for resource in self.releases],
            "requires": [resource.to_dict() for resource in self.requires],
            "capture_main_pid_unit": self.capture_main_pid_unit,
            "release_main_pid_unit": self.release_main_pid_unit,
        }


@dataclass(frozen=True, slots=True)
class ScenarioPlan:
    name: str
    legs: tuple[str, ...]
    steps: tuple[Step, ...]
    durability_floors: tuple[DurabilityFloor, ...]
    tools: tuple[str, ...] = ()
    requires_observed_durability: bool = True
    acceptance_gaps: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "legs": list(self.legs),
            "steps": [step.to_dict() for step in self.steps],
            "expected_durability": [
                floor.to_dict() for floor in self.durability_floors
            ],
            "tools": list(self.tools),
            "requires_observed_durability": self.requires_observed_durability,
            "acceptance_gaps": list(self.acceptance_gaps),
        }


@dataclass(frozen=True, slots=True)
class ScenarioContext:
    config: HarnessConfig
    zerofs_binary: Path
    zerofs_config: Path


ScenarioBuilder = Callable[[ScenarioContext], ScenarioPlan]


def _mountpoint(config: HarnessConfig, leg: str) -> Path:
    return validate_owned_path(config.mount_root / leg, config.resource_root)


def server_steps(
    context: ScenarioContext,
    listeners: tuple[ResourceOwnership, ...] = (),
) -> tuple[Step, ...]:
    unit = context.config.unit_name
    return (
        Step(
            "start the run-scoped ZeroFS instance as a transient unit",
            (
                "systemd-run",
                "--collect",
                f"--unit={unit}",
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


def server_stop_steps(
    context: ScenarioContext,
    listeners: tuple[ResourceOwnership, ...] = (),
) -> tuple[Step, ...]:
    return (
        Step(
            "stop the run-scoped ZeroFS instance",
            ("systemctl", "stop", context.config.unit_name),
            sudo=True,
            releases=listeners + (ResourceOwnership("unit", context.config.unit_name),),
            release_main_pid_unit=context.config.unit_name,
        ),
    )


def server_crash_steps(
    context: ScenarioContext,
    listeners: tuple[ResourceOwnership, ...] = (),
) -> tuple[Step, ...]:
    return (
        Step(
            "SIGKILL the run-scoped ZeroFS instance at the crash boundary",
            ("systemctl", "kill", "-s", "KILL", context.config.unit_name),
            sudo=True,
            releases=listeners + (ResourceOwnership("unit", context.config.unit_name),),
            release_main_pid_unit=context.config.unit_name,
        ),
    )


def nfs_mount_steps(config: HarnessConfig) -> tuple[Step, ...]:
    mountpoint = _mountpoint(config, "nfs")
    options = (
        f"hard,nolock,tcp,vers=3,port={NFS_PORT},mountport={NFS_PORT},"
        "timeo=600,retrans=2"
    )
    return (
        Step("create the NFS mountpoint", ("mkdir", "-p", str(mountpoint))),
        Step(
            "hard NFSv3 kernel mount against the run-scoped server",
            ("mount.nfs", "127.0.0.1:/", str(mountpoint), "-o", options),
            sudo=True,
            acquires=(ResourceOwnership("mount", str(mountpoint)),),
        ),
    )


def nfs_write_commit_steps(config: HarnessConfig, name: str) -> tuple[Step, ...]:
    target = _mountpoint(config, "nfs") / name
    return (
        Step(
            "issue real NFS WRITEs followed by COMMIT via fsync",
            (
                "dd",
                "if=/dev/urandom",
                f"of={target}",
                "bs=1M",
                "count=8",
                "conv=fsync",
            ),
            sudo=True,
        ),
    )


def nfs_unmount_steps(config: HarnessConfig) -> tuple[Step, ...]:
    return (
        Step(
            "unmount the NFS leg",
            ("umount", str(_mountpoint(config, "nfs"))),
            sudo=True,
            releases=(ResourceOwnership("mount", str(_mountpoint(config, "nfs"))),),
        ),
    )


def ninep_mount_steps(config: HarnessConfig) -> tuple[Step, ...]:
    mountpoint = _mountpoint(config, "ninep")
    options = f"trans=tcp,port={NINEP_PORT},version=9p2000.L,msize=1048576,access=user"
    return (
        Step("create the 9P mountpoint", ("mkdir", "-p", str(mountpoint))),
        Step(
            "v9fs kernel mount against the run-scoped server",
            ("mount", "-t", "9p", "-o", options, "127.0.0.1", str(mountpoint)),
            sudo=True,
            acquires=(ResourceOwnership("mount", str(mountpoint)),),
        ),
    )


def ninep_write_fsync_steps(config: HarnessConfig, name: str) -> tuple[Step, ...]:
    target = _mountpoint(config, "ninep") / name
    return (
        Step(
            "issue real Twrite messages followed by Tfsync via fsync",
            (
                "dd",
                "if=/dev/urandom",
                f"of={target}",
                "bs=1M",
                "count=8",
                "conv=fsync",
            ),
            sudo=True,
        ),
    )


def ninep_native_client_steps(context: ScenarioContext) -> tuple[Step, ...]:
    proof = context.config.resource_root / "ninep-native-proof.bin"
    return (
        Step(
            "drive the shipping native 9P client (Twrite + Tfsync)",
            (
                str(context.zerofs_binary),
                "ninep-client",
                "--target",
                f"tcp://127.0.0.1:{NINEP_PORT}",
                "--write-fsync-proof",
                str(proof),
            ),
        ),
    )


def ninep_unmount_steps(config: HarnessConfig) -> tuple[Step, ...]:
    return (
        Step(
            "unmount the 9P leg",
            ("umount", str(_mountpoint(config, "ninep"))),
            sudo=True,
            releases=(ResourceOwnership("mount", str(_mountpoint(config, "ninep"))),),
        ),
    )


def nbd_connect_steps(config: HarnessConfig) -> tuple[Step, ...]:
    return (
        Step(
            "attach the disposable NBD device to the run-scoped export",
            (
                "nbd-client",
                "127.0.0.1",
                str(NBD_PORT),
                NBD_DEVICE,
                "-name",
                config.unit_name,
                "-persist",
            ),
            sudo=True,
            acquires=(ResourceOwnership("device", NBD_DEVICE),),
        ),
    )


def nbd_write_steps(config: HarnessConfig, *, flush: bool) -> tuple[Step, ...]:
    steps = [
        Step(
            "issue real NBD WRITE (FUA via O_DIRECT+O_SYNC)",
            (
                "dd",
                "if=/dev/urandom",
                f"of={NBD_DEVICE}",
                "bs=1M",
                "count=8",
                "oflag=direct,sync",
            ),
            sudo=True,
        )
    ]
    if flush:
        steps.append(
            Step(
                "issue a real NBD FLUSH",
                ("blockdev", "--flushbufs", NBD_DEVICE),
                sudo=True,
            )
        )
    _ = config
    return tuple(steps)


def nbd_disconnect_steps(config: HarnessConfig) -> tuple[Step, ...]:
    _ = config
    return (
        Step(
            "detach the disposable NBD device",
            ("nbd-client", "-d", NBD_DEVICE),
            sudo=True,
            releases=(ResourceOwnership("device", NBD_DEVICE),),
        ),
    )


def rpc_steps(context: ScenarioContext) -> tuple[Step, ...]:
    socket = context.config.run_root / "rpc.sock"
    proto_root = context.config.source_root / "zerofs" / "proto"
    directory = f"/tiered-rpc-{context.config.run_uuid}"
    common = (
        "-plaintext",
        "-import-path",
        str(proto_root),
        "-proto",
        "admin.proto",
    )
    return (
        Step(
            "create a directory through the production TCP admin RPC",
            (
                "grpcurl",
                *common,
                "-d",
                f'{{"path":"{directory}","mode":493,"uid":0,"gid":0}}',
                f"127.0.0.1:{RPC_PORT}",
                "zerofs.admin.AdminService/CreateDirectory",
            ),
        ),
        Step(
            "flush through the production Unix admin RPC",
            (
                "grpcurl",
                *common,
                "-unix=true",
                "-d",
                "{}",
                str(socket),
                "zerofs.admin.AdminService/Flush",
            ),
        ),
        Step(
            "remove the directory through the production TCP admin RPC",
            (
                "grpcurl",
                *common,
                "-d",
                f'{{"path":"{directory}"}}',
                f"127.0.0.1:{RPC_PORT}",
                "zerofs.admin.AdminService/RemoveDirectory",
            ),
        ),
    )


def webui_steps(context: ScenarioContext) -> tuple[Step, ...]:
    source = context.config.resource_root / "webui-source.bin"
    downloaded = context.config.resource_root / "webui-downloaded.bin"
    target = f"ws://127.0.0.1:{WEBUI_PORT}/ws/9p"
    destination = f"/tiered-webui-{context.config.run_uuid}.bin"
    return (
        Step(
            "create an incompressible WebUI upload source",
            (
                "dd",
                "if=/dev/urandom",
                f"of={source}",
                "bs=1M",
                "count=8",
            ),
            acquires=(ResourceOwnership("path", str(source)),),
        ),
        Step(
            "upload bytes through the native WebSocket 9P transport control",
            (
                str(context.zerofs_binary),
                "upload",
                target,
                str(source),
                destination,
                "--jobs",
                "1",
            ),
        ),
        Step(
            "download bytes through the native WebSocket 9P transport control",
            (
                str(context.zerofs_binary),
                "download",
                target,
                destination,
                str(downloaded),
                "--jobs",
                "1",
            ),
            acquires=(ResourceOwnership("path", str(downloaded)),),
        ),
        Step("compare the WebSocket round trip", ("cmp", str(source), str(downloaded))),
        Step(
            "remove the WebSocket proof from ZeroFS",
            (str(context.zerofs_binary), "rm", target, destination),
        ),
        Step(
            "remove local WebSocket proof files",
            ("rm", "-f", "--", str(source), str(downloaded)),
            releases=(
                ResourceOwnership("path", str(source)),
                ResourceOwnership("path", str(downloaded)),
            ),
        ),
    )


def _plan(
    context: ScenarioContext,
    name: str,
    legs: tuple[str, ...],
    steps: tuple[Step, ...],
    operations: tuple[str, ...],
    tools: tuple[str, ...] = (),
    *,
    requires_observed_durability: bool = True,
    acceptance_gaps: tuple[str, ...] = (),
) -> ScenarioPlan:
    listener_by_leg = {
        "nfs": NFS_PORT,
        "ninep": NINEP_PORT,
        "nbd": NBD_PORT,
        "rpc": RPC_PORT,
        "webui": WEBUI_PORT,
    }
    listeners = tuple(
        ResourceOwnership("listener", listener_by_leg[leg])
        for leg in dict.fromkeys(legs)
        if leg in listener_by_leg
    )
    return ScenarioPlan(
        name=name,
        legs=legs,
        steps=server_steps(context, listeners)
        + steps
        + server_stop_steps(context, listeners),
        durability_floors=floors_for(context.config.ack, operations),
        tools=tools,
        requires_observed_durability=requires_observed_durability,
        acceptance_gaps=acceptance_gaps,
    )


def _global_admission(context: ScenarioContext) -> ScenarioPlan:
    config = context.config
    steps = (
        nbd_connect_steps(config)
        + nbd_write_steps(config, flush=True)
        + nfs_mount_steps(config)
        + nfs_write_commit_steps(config, "global-admission.bin")
        + ninep_mount_steps(config)
        + ninep_write_fsync_steps(config, "global-admission.bin")
        + ninep_unmount_steps(config)
        + nfs_unmount_steps(config)
        + nbd_disconnect_steps(config)
    )
    return _plan(
        context,
        "global-admission-nbd-nfs-ninep",
        ("nbd", "nfs", "ninep"),
        steps,
        ("nbd-flush", "nfs-commit", "ninep-fsync"),
        requires_observed_durability=True,
    )


def _cross_adapter_pending_read(context: ScenarioContext) -> ScenarioPlan:
    config = context.config
    shared = "shared-backing-inode.bin"
    read_target = _mountpoint(config, "ninep") / shared
    steps = (
        nfs_mount_steps(config)
        + ninep_mount_steps(config)
        + (
            Step(
                "write through NFS while the payload is still pending",
                (
                    "dd",
                    "if=/dev/urandom",
                    f"of={_mountpoint(config, 'nfs') / shared}",
                    "bs=1M",
                    "count=8",
                ),
                sudo=True,
            ),
            Step(
                "read the same backing inode through 9P before writeback",
                ("dd", f"if={read_target}", "of=/dev/null", "bs=1M"),
                sudo=True,
            ),
        )
        + ninep_unmount_steps(config)
        + nfs_unmount_steps(config)
    )
    return _plan(
        context,
        "cross-adapter-pending-read-same-backing-inode",
        ("nfs", "ninep"),
        steps,
        ("cross-adapter-read",),
        requires_observed_durability=True,
    )


def _nfs_commit_covers_prior_nbd(context: ScenarioContext) -> ScenarioPlan:
    config = context.config
    steps = (
        nbd_connect_steps(config)
        + nbd_write_steps(config, flush=False)
        + nfs_mount_steps(config)
        + nfs_write_commit_steps(config, "commit-covers-nbd.bin")
        + nfs_unmount_steps(config)
        + nbd_disconnect_steps(config)
    )
    return _plan(
        context,
        "nfs-commit-covers-prior-nbd",
        ("nbd", "nfs"),
        steps,
        ("nfs-commit",),
        requires_observed_durability=True,
    )


def _ninep_fsync_covers_prior_nfs(context: ScenarioContext) -> ScenarioPlan:
    config = context.config
    steps = (
        nfs_mount_steps(config)
        + (
            Step(
                "issue NFS WRITEs without COMMIT",
                (
                    "dd",
                    "if=/dev/urandom",
                    f"of={_mountpoint(config, 'nfs') / 'uncommitted.bin'}",
                    "bs=1M",
                    "count=8",
                ),
                sudo=True,
            ),
        )
        + ninep_mount_steps(config)
        + ninep_write_fsync_steps(config, "fsync-covers-nfs.bin")
        + ninep_native_client_steps(context)
        + ninep_unmount_steps(config)
        + nfs_unmount_steps(config)
    )
    return _plan(
        context,
        "ninep-fsync-covers-prior-nfs",
        ("nfs", "ninep"),
        steps,
        ("ninep-fsync",),
        requires_observed_durability=True,
    )


def _nbd_flush_covers_prior_ninep(context: ScenarioContext) -> ScenarioPlan:
    config = context.config
    steps = (
        ninep_mount_steps(config)
        + (
            Step(
                "issue 9P Twrites without Tfsync",
                (
                    "dd",
                    "if=/dev/urandom",
                    f"of={_mountpoint(config, 'ninep') / 'unsynced.bin'}",
                    "bs=1M",
                    "count=8",
                ),
                sudo=True,
            ),
        )
        + nbd_connect_steps(config)
        + nbd_write_steps(config, flush=True)
        + nbd_disconnect_steps(config)
        + ninep_unmount_steps(config)
    )
    return _plan(
        context,
        "nbd-flush-covers-prior-ninep",
        ("ninep", "nbd"),
        steps,
        ("nbd-flush",),
        requires_observed_durability=True,
    )


def _webui_rpc_production_path(context: ScenarioContext) -> ScenarioPlan:
    steps = rpc_steps(context) + webui_steps(context)
    return _plan(
        context,
        "webui-rpc-production-path",
        ("rpc", "webui"),
        steps,
        ("rpc-ack", "webui-ack"),
        requires_observed_durability=True,
        acceptance_gaps=(
            "the shipping browser WASM client and gRPC-Web route are not wired into this harness",
        ),
    )


def _protocol_materialized_control(context: ScenarioContext) -> ScenarioPlan:
    if context.config.ack.filesystem != "materialized":
        raise ConfigError(
            "protocol-materialized-control requires --filesystem-ack-mode materialized"
        )
    config = context.config
    steps = (
        nfs_mount_steps(config)
        + nfs_write_commit_steps(config, "materialized-control.bin")
        + ninep_mount_steps(config)
        + ninep_write_fsync_steps(config, "materialized-control.bin")
        + ninep_unmount_steps(config)
        + nfs_unmount_steps(config)
    )
    return _plan(
        context,
        "protocol-materialized-control",
        ("nfs", "ninep"),
        steps,
        ("nfs-commit", "ninep-fsync"),
        requires_observed_durability=True,
    )


def _protocol_durability_target_control(context: ScenarioContext) -> ScenarioPlan:
    config = context.config
    steps = (
        nfs_mount_steps(config)
        + nfs_write_commit_steps(config, "durability-target-control.bin")
        + nfs_unmount_steps(config)
        + ninep_mount_steps(config)
        + ninep_write_fsync_steps(config, "durability-target-control.bin")
        + ninep_unmount_steps(config)
    )
    return _plan(
        context,
        "protocol-durability-target-control",
        ("nfs", "ninep"),
        steps,
        ("nfs-commit", "ninep-fsync"),
        requires_observed_durability=True,
    )


def _benchmark(name: str, required_object_ack: str) -> ScenarioBuilder:
    def build(context: ScenarioContext) -> ScenarioPlan:
        config = context.config
        if config.ack.object != required_object_ack:
            raise ConfigError(
                f"{name} requires --object-ack-mode {required_object_ack}, "
                f"got {config.ack.object}"
            )
        mountpoint = _mountpoint(config, "nfs")
        steps = (
            nfs_mount_steps(config)
            + (
                Step(
                    "run the fio tier benchmark on the mounted filesystem",
                    (
                        "fio",
                        "--name",
                        name,
                        f"--directory={mountpoint}",
                        "--rw=write",
                        "--bs=1M",
                        "--size=256M",
                        "--numjobs=4",
                        "--fsync_on_close=1",
                        "--group_reporting",
                    ),
                    sudo=True,
                ),
            )
            + nfs_unmount_steps(config)
        )
        return _plan(context, name, ("nfs",), steps, ("benchmark-write",))

    return build


PROTOCOL_SCENARIOS: dict[str, ScenarioBuilder] = {
    "global-admission-nbd-nfs-ninep": _global_admission,
    "cross-adapter-pending-read-same-backing-inode": _cross_adapter_pending_read,
    "nfs-commit-covers-prior-nbd": _nfs_commit_covers_prior_nbd,
    "ninep-fsync-covers-prior-nfs": _ninep_fsync_covers_prior_nfs,
    "nbd-flush-covers-prior-ninep": _nbd_flush_covers_prior_ninep,
    "webui-rpc-production-path": _webui_rpc_production_path,
    "protocol-materialized-control": _protocol_materialized_control,
    "protocol-durability-target-control": _protocol_durability_target_control,
    "benchmark-ram-ack": _benchmark("benchmark-ram-ack", "memory"),
    "benchmark-local-ssd": _benchmark("benchmark-local-ssd", "ssd"),
    "benchmark-paced-remote": _benchmark("benchmark-paced-remote", "remote"),
}
