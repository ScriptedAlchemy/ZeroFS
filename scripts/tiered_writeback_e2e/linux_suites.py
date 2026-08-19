from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from .config import HarnessConfig, validate_owned_path
from .integrity import IntegrityError, floors_for, sha256_file
from .protocols import (
    ScenarioBuilder,
    ScenarioContext,
    ScenarioPlan,
    Step,
    _mountpoint,
    nfs_mount_steps,
    nfs_unmount_steps,
    ninep_mount_steps,
    ninep_unmount_steps,
    server_steps,
    server_stop_steps,
)

# Pinned tool revisions; focused C4 runs and C8's final reruns use these too.
XFSTESTS_REVISION = "1ae822c1c2e2364e966085cee3ce4a97b2500241"
PJDFSTEST_REVISION = "85a8aea9e685999ef0540392fd80535f873d7ff7"
PJDFSTEST_NFS_REVISION = "7d3d7cb0cdc5d39eedd995771bc1d4b3dabf31ab"

PINNED_REVISIONS = {
    "xfstests": XFSTESTS_REVISION,
    "pjdfstest": PJDFSTEST_REVISION,
    "pjdfstest_nfs": PJDFSTEST_NFS_REVISION,
}

TOOL_REPOSITORIES = {
    "xfstests": "https://git.kernel.org/pub/scm/fs/xfs/xfstests-dev.git",
    "pjdfstest": "https://github.com/pjd/pjdfstest.git",
    "pjdfstest_nfs": "https://github.com/pjd/pjdfstest.git",
}

# Kernel scenarios may download exactly this archive and must verify its
# SHA-256 before extraction.
KERNEL_ARCHIVE_URL = "https://cdn.kernel.org/pub/linux/kernel/v6.x/linux-6.18.tar.xz"
KERNEL_ARCHIVE_SHA256 = (
    "9106a4605da9e31ff17659d958782b815f9591ab308d03b0ee21aad6c7dced4b"
)
KERNEL_SOURCE_DIRNAME = "linux-6.18"


def verify_kernel_archive(path: Path, *, expected: str = KERNEL_ARCHIVE_SHA256) -> str:
    digest = sha256_file(Path(path))
    if digest != expected:
        raise IntegrityError(
            f"kernel archive {path} hash mismatch: expected {expected}, got {digest}"
        )
    return digest


@dataclass(frozen=True, slots=True)
class ToolCheckout:
    name: str
    repository: str
    revision: str
    destination: Path


def tool_checkout(config: HarnessConfig, name: str) -> ToolCheckout:
    destination = validate_owned_path(config.tools_root / name, config.resource_root)
    return ToolCheckout(
        name=name,
        repository=TOOL_REPOSITORIES[name],
        revision=PINNED_REVISIONS[name],
        destination=destination,
    )


def tool_checkout_steps(config: HarnessConfig, name: str) -> tuple[Step, ...]:
    checkout = tool_checkout(config, name)
    return (
        Step(
            f"clone {name} under the resource-root tools directory",
            ("git", "clone", checkout.repository, str(checkout.destination)),
        ),
        Step(
            f"pin {name} to its contract revision",
            (
                "git",
                "-C",
                str(checkout.destination),
                "checkout",
                "--detach",
                checkout.revision,
            ),
        ),
    )


def kernel_archive_steps(config: HarnessConfig, extract_to: Path) -> tuple[Step, ...]:
    archive = validate_owned_path(
        config.tools_root / "linux-6.18.tar.xz", config.resource_root
    )
    return (
        Step(
            "create the tools directory",
            ("mkdir", "-p", str(config.tools_root)),
        ),
        Step(
            "download the pinned kernel archive",
            ("curl", "-fsSL", "--output", str(archive), KERNEL_ARCHIVE_URL),
        ),
        Step(
            "verify the kernel archive SHA-256 before extraction",
            (
                "bash",
                "-c",
                f"echo '{KERNEL_ARCHIVE_SHA256}  {archive}' | "
                "sha256sum --check --strict",
            ),
        ),
        Step(
            "extract the verified kernel archive onto the mounted filesystem",
            ("tar", "-C", str(extract_to), "-xJf", str(archive)),
            sudo=True,
        ),
    )


def _xfstests_config_step(config: HarnessConfig, protocol: str) -> Step:
    checkout = tool_checkout(config, "xfstests")
    mountpoint = _mountpoint(config, protocol)
    local_config = checkout.destination / "local.config"
    content = (
        f"export TEST_DEV=127.0.0.1:/\n"
        f"export TEST_DIR={mountpoint}\n"
        f"export FSTYP={'nfs' if protocol == 'nfs' else '9p'}\n"
    )
    return Step(
        "write the xfstests local.config for this leg",
        ("bash", "-c", f"cat > {local_config} <<'EOF'\n{content}EOF"),
    )


def _xfstests(name: str, protocol: str, groups: tuple[str, ...]) -> ScenarioBuilder:
    def build(context: ScenarioContext) -> ScenarioPlan:
        config = context.config
        mount = (
            nfs_mount_steps(config) if protocol == "nfs" else ninep_mount_steps(config)
        )
        unmount = (
            nfs_unmount_steps(config)
            if protocol == "nfs"
            else ninep_unmount_steps(config)
        )
        checkout = tool_checkout(config, "xfstests")
        run_steps = tuple(
            Step(
                f"run xfstests group {group}",
                ("./check", "-g", group),
                sudo=True,
                cwd=str(checkout.destination),
            )
            for group in groups
        )
        steps = (
            server_steps(context)
            + tool_checkout_steps(config, "xfstests")
            + mount
            + (_xfstests_config_step(config, protocol),)
            + run_steps
            + unmount
            + server_stop_steps(context)
        )
        return ScenarioPlan(
            name=name,
            legs=(protocol,),
            steps=steps,
            durability_floors=floors_for(config.ack, ("suite-write",)),
            tools=("xfstests",),
        )

    return build


def _pjdfstest(name: str, protocol: str, tool: str) -> ScenarioBuilder:
    def build(context: ScenarioContext) -> ScenarioPlan:
        config = context.config
        mount = (
            nfs_mount_steps(config) if protocol == "nfs" else ninep_mount_steps(config)
        )
        unmount = (
            nfs_unmount_steps(config)
            if protocol == "nfs"
            else ninep_unmount_steps(config)
        )
        checkout = tool_checkout(config, tool)
        mountpoint = _mountpoint(config, protocol)
        steps = (
            server_steps(context)
            + tool_checkout_steps(config, tool)
            + (
                Step(
                    "build pjdfstest",
                    (
                        "bash",
                        "-c",
                        "autoreconf -ifs && ./configure && make pjdfstest",
                    ),
                    cwd=str(checkout.destination),
                ),
            )
            + mount
            + (
                Step(
                    "run the pjdfstest POSIX conformance suite on the mount",
                    (
                        "prove",
                        "-rv",
                        str(checkout.destination / "tests"),
                    ),
                    sudo=True,
                    cwd=str(mountpoint),
                ),
            )
            + unmount
            + server_stop_steps(context)
        )
        return ScenarioPlan(
            name=name,
            legs=(protocol,),
            steps=steps,
            durability_floors=floors_for(config.ack, ("suite-write",)),
            tools=(tool,),
        )

    return build


def _stress_ng(context: ScenarioContext) -> ScenarioPlan:
    config = context.config
    steps = (
        server_steps(context)
        + nfs_mount_steps(config)
        + ninep_mount_steps(config)
        + (
            Step(
                "run stress-ng filesystem stressors on the NFS leg",
                (
                    "stress-ng",
                    "--temp-path",
                    str(_mountpoint(config, "nfs")),
                    "--hdd",
                    "4",
                    "--fallocate",
                    "2",
                    "--timeout",
                    "120s",
                ),
                sudo=True,
            ),
            Step(
                "run stress-ng filesystem stressors on the 9P leg",
                (
                    "stress-ng",
                    "--temp-path",
                    str(_mountpoint(config, "ninep")),
                    "--hdd",
                    "4",
                    "--fallocate",
                    "2",
                    "--timeout",
                    "120s",
                ),
                sudo=True,
            ),
        )
        + ninep_unmount_steps(config)
        + nfs_unmount_steps(config)
        + server_stop_steps(context)
    )
    return ScenarioPlan(
        name="stress-ng-nfs-ninep",
        legs=("nfs", "ninep"),
        steps=steps,
        durability_floors=floors_for(config.ack, ("suite-write",)),
    )


def _kernel_compile(name: str, protocol: str) -> ScenarioBuilder:
    def build(context: ScenarioContext) -> ScenarioPlan:
        config = context.config
        mount = (
            nfs_mount_steps(config) if protocol == "nfs" else ninep_mount_steps(config)
        )
        unmount = (
            nfs_unmount_steps(config)
            if protocol == "nfs"
            else ninep_unmount_steps(config)
        )
        mountpoint = _mountpoint(config, protocol)
        source = mountpoint / KERNEL_SOURCE_DIRNAME
        steps = (
            server_steps(context)
            + mount
            + kernel_archive_steps(config, mountpoint)
            + (
                Step(
                    "configure the kernel build",
                    ("make", "-C", str(source), "defconfig"),
                    sudo=True,
                ),
                Step(
                    "compile the kernel on the mounted filesystem",
                    ("make", "-C", str(source), "-j8", "vmlinux"),
                    sudo=True,
                ),
            )
            + unmount
            + server_stop_steps(context)
        )
        return ScenarioPlan(
            name=name,
            legs=(protocol,),
            steps=steps,
            durability_floors=floors_for(config.ack, ("suite-write",)),
        )

    return build


SUITE_SCENARIOS: dict[str, ScenarioBuilder] = {
    "xfstests-nfs-quick": _xfstests("xfstests-nfs-quick", "nfs", ("quick",)),
    "xfstests-ninep-quick-and-strict": _xfstests(
        "xfstests-ninep-quick-and-strict", "ninep", ("quick", "rw")
    ),
    "pjdfstest-nfs": _pjdfstest("pjdfstest-nfs", "nfs", "pjdfstest_nfs"),
    "pjdfstest-ninep": _pjdfstest("pjdfstest-ninep", "ninep", "pjdfstest"),
    "stress-ng-nfs-ninep": _stress_ng,
    "kernel-compile-nfs": _kernel_compile("kernel-compile-nfs", "nfs"),
    "kernel-compile-ninep": _kernel_compile("kernel-compile-ninep", "ninep"),
}
