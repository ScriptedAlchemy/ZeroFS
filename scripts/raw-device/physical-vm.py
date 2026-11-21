#!/usr/bin/env python3
"""Run physical claim witnesses solely inside an isolated synthetic QEMU guest."""

import argparse
import gzip
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import tempfile

HERE = Path(__file__).resolve().parent

STOCK_NVME_MODEL = b"QEMU NVMe Ctrl\0"
P3_NVME_MODEL = b"CT1000P3SSD8\0".ljust(len(STOCK_NVME_MODEL), b"\0")

DUPLICATE_DRIVE = (
    "duplicate", 128 << 20,
    "ide-hd,bus=ide.2,model=ZeroFS duplicate,serial=WF203KQ8,wwn=0x5000c500ac8aed0b,rotation_rate=7200",
)

DRIVES = (
    ("hdd6", 6_001_175_126_016, "ide-hd,bus=ide.0,model=ST6000DM003-2CY186,serial=WF203KQ8,wwn=0x5000c500ac8aed0a,rotation_rate=7200"),
    ("hdd18", 18_000_207_937_536, "ide-hd,bus=ide.1,model=WDC WD180EMFZ-11AFXA0,serial=3WH7H3BJ,wwn=0x5000cca284d17ed1,rotation_rate=7200"),
    ("nvme", 1_000_204_886_016, "nvme,serial=2318E6D18CE7"),
)


def output(*argv, timeout=20, **kwargs):
    return subprocess.check_output(argv, timeout=timeout, text=True, **kwargs).strip()


def digest(path):
    with path.open("rb") as contents:
        return hashlib.file_digest(contents, "sha256").hexdigest()


def stage_library(root, path):
    destination = root / path.lstrip("/")
    destination.parent.mkdir(parents=True, exist_ok=True)
    if not destination.exists():
        shutil.copy2(path, destination)


def stage_binary(root, source, target):
    destination = root / target.lstrip("/")
    destination.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(source, destination)
    for line in output("ldd", source).splitlines():
        library = re.search(r"(?:=> )?(/[^ ]+) \(", line)
        if library:
            stage_library(root, library[1])
        elif "not found" in line:
            raise RuntimeError(f"guest userspace dependency unavailable: {line}")


def stage_modules(root, release):
    inputs = Path("/lib/modules") / release
    dependencies = {}
    for line in (inputs / "modules.dep").read_text().splitlines():
        module, children = line.split(":", 1)
        dependencies[module] = children.split()

    modules = root / "modules"
    modules.mkdir()
    staged = {}

    def stage(module):
        if module in staged:
            return
        for dependency in dependencies[module]:
            stage(dependency)
        source = inputs / module
        suffix = source.suffix
        if suffix == ".zst":
            name = source.stem
            with (modules / name).open("wb") as sink:
                subprocess.run(["zstd", "-dc", source], stdout=sink, check=True, timeout=20)
        elif suffix == ".ko":
            name = source.name
            shutil.copyfile(source, modules / name)
        else:
            raise RuntimeError(f"unsupported guest module compression: {source}")
        staged[module] = name

    for driver in ("ahci", "nvme"):
        candidates = [module for module in dependencies if Path(module).name in {f"{driver}.ko", f"{driver}.ko.zst"}]
        if len(candidates) != 1:
            raise RuntimeError(f"expected one guest module for {driver}: {candidates}")
        stage(candidates[0])
    (modules / "load-order").write_text("\n".join(staged.values()) + "\n")


def seal_initramfs(root, archive):
    # Sort the archive before writing it. All owners inside the guest are root.
    with archive.open("wb") as image:
        with gzip.GzipFile(fileobj=image, mode="wb", mtime=0) as compressed:
            names = ["./" + path.relative_to(root).as_posix() for path in sorted(root.rglob("*"))]
            entries = b".\0" + b"\0".join(os.fsencode(name) for name in names) + b"\0"
            archive_bytes = subprocess.run(
                ["cpio", "--null", "--quiet", "--owner=0:0", "-o", "-H", "newc"],
                input=entries, stdout=subprocess.PIPE, check=True, timeout=30, cwd=root,
            ).stdout
            compressed.write(archive_bytes)


def copy_with_p3_nvme_model(qemu, scratch):
    stock = qemu.read_bytes()
    if stock.count(STOCK_NVME_MODEL) != 1:
        raise RuntimeError("expected exactly one stock QEMU NVMe Identify Controller model string")
    offset = stock.index(STOCK_NVME_MODEL)
    end = offset + len(STOCK_NVME_MODEL)
    modified = stock[:offset] + P3_NVME_MODEL + stock[end:]
    copy = scratch / "qemu-nvme-p3"
    copy.write_bytes(modified)
    copy.chmod(0o700)
    actual = copy.read_bytes()
    if actual[offset:end] != P3_NVME_MODEL or actual[:offset] + actual[end:] != stock[:offset] + stock[end:]:
        raise RuntimeError("fixture QEMU differs outside the approved NVMe model")
    return copy, {
        "model_offset_in_elf": offset,
        "changed_bytes": sum(before != after for before, after in zip(STOCK_NVME_MODEL, P3_NVME_MODEL, strict=True)),
        "original_bytes_hex": STOCK_NVME_MODEL.hex(),
        "substituted_bytes_hex": P3_NVME_MODEL.hex(),
        "stock_sha256": hashlib.sha256(stock).hexdigest(),
        "fixture_sha256": digest(copy),
    }


def boot(qemu, vm, model, environment):
    command = [
        qemu, *vm, "-append", f"console=ttyS0 loglevel=3 panic=1 rdinit=/init zerofs_fixture_model={model}",
    ]
    completed = subprocess.run(
        command, env=environment, stdin=subprocess.DEVNULL, capture_output=True, text=True, timeout=90,
    )
    if completed.returncode or "RAW_PHYSICAL_VM_COMPLETE" not in completed.stdout or "RAW_PHYSICAL_VM_FAIL" in completed.stdout:
        raise RuntimeError(f"{model} guest did not complete: {completed.returncode}\n{completed.stdout}\n{completed.stderr}")
    return [line for line in completed.stdout.splitlines() if line.startswith("RAW_PHYSICAL_VM_")]


def run(scratch, kernel, probe, release):
    root = scratch / "root"
    for directory in ("bin", "dev", "proc", "sys", "run", "etc", "claim-mount", "usr/lib/udev/rules.d"):
        (root / directory).mkdir(parents=True, exist_ok=True)
    shutil.copy2("/usr/bin/busybox", root / "bin/busybox")
    for applet in ("sh", "mount", "umount", "insmod", "poweroff", "grep", "basename", "readlink", "ln", "cat", "rm"):
        (root / "bin" / applet).symlink_to("busybox")
    shutil.copy2(HERE / "physical-vm-init.sh", root / "init")
    (root / "init").chmod(0o755)
    stage_binary(root, probe, "/bin/zerofs-raw-device")
    stage_binary(root, "/usr/bin/udevadm", "/usr/bin/udevadm")
    stage_binary(root, "/usr/lib/systemd/systemd-udevd", "/usr/lib/systemd/systemd-udevd")
    for name in ("ata_id", "scsi_id"):
        stage_binary(root, "/usr/lib/udev/" + name, "/usr/lib/udev/" + name)
    for name in ("50-udev-default.rules", "60-persistent-storage.rules"):
        shutil.copy2("/usr/lib/udev/rules.d/" + name, root / "usr/lib/udev/rules.d" / name)
    subprocess.run(
        ["cc", "-static", "-O2", "-Wall", "-Wextra", "-o", root / "bin/hold-exclusive", HERE / "physical-vm-hold.c"],
        check=True, timeout=30,
    )
    for command in ("dmsetup", "mkfs.ext4"):
        stage_binary(root, "/usr/sbin/" + command, "/usr/sbin/" + command)
    shutil.copy2("/etc/mke2fs.conf", root / "etc/mke2fs.conf")
    subprocess.run(
        ["cc", "-static", "-O2", "-Wall", "-Wextra", "-o", root / "bin/guest-partition", HERE / "physical-vm-partition.c"],
        check=True, timeout=30,
    )
    stage_modules(root, release)
    archive = scratch / "initramfs.cpio.gz"
    seal_initramfs(root, archive)

    system_qemu = shutil.which("qemu-system-x86_64")
    if not system_qemu:
        raise RuntimeError("qemu-system-x86_64 is required")
    qemu = Path(system_qemu).resolve(strict=True)
    model_qemu, model_patch = copy_with_p3_nvme_model(qemu, scratch)
    vm = [
        "-machine", "q35,accel=kvm", "-cpu", "host", "-smp", "1", "-m", "512",
        "-nodefaults", "-no-user-config", "-no-reboot", "-display", "none", "-monitor", "none",
        "-serial", "stdio", "-nic", "none", "-kernel", str(kernel), "-initrd", str(archive),
        "-bios", "/usr/share/seabios/bios-256k.bin", "-snapshot",
    ]
    backing = {}
    control_drive = []
    for role, bytes_, controller in (*DRIVES, DUPLICATE_DRIVE):
        image = scratch / (role + ".qcow2")
        subprocess.run(
            ["qemu-img", "create", "-q", "-f", "qcow2", "-o", "preallocation=off", image, str(bytes_)],
            check=True, timeout=20,
        )
        backing[image] = digest(image)
        options = ["-drive", f"if=none,format=qcow2,id={role},file={image}", "-device", f"{controller},drive={role}"]
        if role == "duplicate":
            control_drive = options
        else:
            vm.extend(options)

    environment = os.environ.copy()
    environment["TMPDIR"] = str(scratch)
    stock_guest = boot(qemu, vm, "stock", environment)
    p3_guest = boot(model_qemu, vm, "p3", environment)
    controls_guest = boot(qemu, vm + control_drive, "controls", environment)
    if digest(qemu) != model_patch["stock_sha256"]:
        raise RuntimeError("system QEMU changed")
    if any(digest(image) != before for image, before in backing.items()):
        raise RuntimeError("a backing image changed")
    return {
        "schema": "zerofs-physical-claim-vm/v1",
        "kernel_release": release,
        "qemu": output(qemu, "--version").splitlines()[0],
        "qemu_nvme_model_patch": model_patch,
        "probe_sha256": digest(probe),
        "kernel_sha256": digest(kernel),
        "backing_images_unchanged": True,
        "stock_guest": stock_guest,
        "p3_guest": p3_guest,
        "controls_guest": controls_guest,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--kernel", type=lambda p: Path(p).resolve(strict=True), required=True)
    parser.add_argument("--probe", type=lambda p: Path(p).resolve(strict=True), required=True)
    parser.add_argument("--release", default=os.uname().release, help="Version of both kernel and matching read-only module source in /lib/modules")
    args = parser.parse_args()
    if not re.fullmatch(r"[0-9][0-9A-Za-z._+-]*", args.release):
        parser.error("invalid Linux release name")
    if not args.kernel.is_file() or not args.probe.is_file():
        parser.error("kernel and probe must be readable regular files")
    if os.uname().machine != "x86_64" or not os.access("/dev/kvm", os.R_OK | os.W_OK):
        parser.error("an x86-64 Linux host with unprivileged KVM access is needed")

    scratch = None
    try:
        with tempfile.TemporaryDirectory(prefix="zerofs-physical-vm-guest-", dir="/var/tmp") as owned:
            scratch = Path(owned)
            receipt = run(scratch, args.kernel, args.probe, args.release)
    finally:
        if scratch is not None:
            if scratch.exists():
                raise RuntimeError(f"scratch retained: {scratch}")
            print(f"Removed owned VM scratch {scratch}", file=sys.stderr)
    receipt["scratch_removed"] = str(scratch)
    print(json.dumps(receipt, indent=2))


if __name__ == "__main__":
    main()
