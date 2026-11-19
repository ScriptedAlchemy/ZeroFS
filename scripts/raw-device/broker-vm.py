"""Boot a disposable, networkless KVM guest and run the isolated engine against its claim broker."""
import argparse
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import shutil
import struct
import subprocess
import tempfile
import tomllib
from uuid import UUID

HERE = Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location("physical_vm", HERE / "physical-vm.py")
vm = importlib.util.module_from_spec(spec)
spec.loader.exec_module(vm)


def blank_labels(config, role, size):
    """Golden empty labels for freshly created, proven-zero software images only."""
    from urllib.parse import parse_qs, urlparse

    members = {m["id"]: m["url"] for m in config["storage"]["pool"]["members"]}
    url = config["storage"]["url"] if role == "nvme" else members[role]
    fields = parse_qs(urlparse(url).query)
    pool = UUID(fields["pool"][0]).bytes
    member = UUID(fields["member"][0]).bytes
    regions = [(size, 1)]
    if role == "nvme":
        regions = list(zip(map(int, fields["layout_bytes"][0].split(",")), (3, 4, 2, 5, 6, 7), strict=True))
    assert sum(n for n, _ in regions) == size
    start = 0
    for length, kind in regions:
        nonce = os.urandom(16)
        for slot in range(2):
            block = bytearray(4096)
            if kind in (3, 4):
                struct.pack_into("<8s16s16sQQQQQ", block, 0, b"ZWBREG02" if kind == 3 else b"ZCTREG02",
                                 pool, member, size, start, length, 1, 0)
            else:
                struct.pack_into("<8sIIQQ16s16sBB", block, 0, b"ZFSRAWL1", 1, 4096,
                                 1, length, pool, member, kind, slot)
                struct.pack_into("<16sQQ", block, 72, nonce, 8192, 0)
            block[-32:] = hashlib.sha256(block[:-32]).digest()
            yield start + slot * 4096, block
        start += length


def run(scratch, kernel, release, engine, probe, manifest):
    root = scratch / "root"
    app = root / "app"
    for base, names in ((root, ("bin", "dev", "sys", "proc", "run", "usr/lib/udev/rules.d")),
                        (app, ("bin", "proc", "sys/fs/cgroup", "dev/shm", "run/claims", "state", "etc", "tmp"))):
        for name in names:
            (base / name).mkdir(parents=True, exist_ok=True)
        shutil.copy2("/usr/bin/busybox", base / "bin/busybox")
        for name in ("sh", "mount", "umount", "mknod", "ip", "mkdir", "cat", "dd", "cmp", "sha256sum",
                     "echo", "sleep", "ls", "grep", "chmod", "chown", "touch", "kill", "id", "head",
                     "tail", "poweroff", "insmod", "chroot", "readlink", "sed", "stat", "env", "cp", "rm"):
            (base / "bin" / name).symlink_to("busybox")
    vm.stage_binary(app, engine, "/bin/zerofs")
    vm.stage_binary(app, "/usr/bin/setpriv", "/bin/setpriv")
    vm.stage_binary(root, probe, "/bin/zerofs-raw-device")
    vm.stage_binary(root, "/usr/bin/unshare", "/bin/unshare")
    vm.stage_binary(root, "/usr/bin/udevadm", "/bin/udevadm")
    vm.stage_binary(root, "/usr/lib/systemd/systemd-udevd", "/usr/lib/systemd/systemd-udevd")
    for name in ("ata_id", "scsi_id"):
        vm.stage_binary(root, "/usr/lib/udev/" + name, "/usr/lib/udev/" + name)
    for name in ("50-udev-default.rules", "60-persistent-storage.rules"):
        shutil.copy2("/usr/lib/udev/rules.d/" + name, root / "usr/lib/udev/rules.d" / name)
    subprocess.run(["cc", "-static", "-O2", "-Wall", "-Wextra", "-Werror", "-o", root / "bin/fault",
                    HERE / "broker-vm-fault.c"], check=True, timeout=30)
    shutil.copy2(root / "bin/fault", app / "bin/fault")
    vm.stage_modules(root, release)
    for script, dest in (("broker-vm-init.sh", root / "init"), ("broker-vm-isolate.sh", root / "isolate"),
                         ("broker-vm-client.sh", app / "client")):
        shutil.copy2(HERE / script, dest)
        dest.chmod(0o755)
    config = manifest.read_text().replace("/fixture/", "/state/")
    (app / "etc/zerofs.toml").write_text(config)
    (app / "etc/wrong.toml").write_text(config.replace("30000000-0000-", "31000000-0000-"))
    archive = scratch / "initramfs.gz"
    vm.seal_initramfs(root, archive)
    qemu, patch = vm.copy_with_p3_nvme_model(Path(shutil.which("qemu-system-x86_64")), scratch)
    command = [qemu, "-enable-kvm", "-machine", "q35", "-cpu", "host", "-m", "10240", "-smp", "2",
               "-nodefaults", "-no-user-config", "-no-reboot", "-display", "none", "-monitor", "none",
               "-serial", "stdio", "-nic", "none", "-kernel", kernel, "-initrd", archive,
               "-bios", "/usr/share/seabios/bios-256k.bin", "-snapshot",
               "-append", "rdinit=/init console=ttyS0 loglevel=3 panic=1"]
    backing = {}
    for role, size, controller in vm.DRIVES:
        path = scratch / (role + ".qcow2")
        assert not path.exists()
        subprocess.run(["qemu-img", "create", "-q", "-f", "qcow2", path, str(size)], check=True)
        ranges = json.loads(vm.output("qemu-img", "map", "--output=json", path))
        assert all(r["zero"] and not r["data"] for r in ranges) and sum(r["length"] for r in ranges) == size
        pair = scratch / "label.bin"
        for offset, block in blank_labels(tomllib.loads(config), role, size):
            pair.write_bytes(block)
            vm.output("qemu-io", "-f", "qcow2", "-c", f"write -s {pair} {offset} 4096", path)
        pair.unlink()
        backing[path] = vm.digest(path)
        command.extend(["-drive", f"if=none,format=qcow2,id={role},file={path}", "-device", f"{controller},drive={role}"])
    env = {**os.environ, "TMPDIR": str(scratch)}
    result = subprocess.run(command, capture_output=True, text=True, env=env, stdin=subprocess.DEVNULL, timeout=240)
    if result.returncode or "BROKER_VM_PASS" not in result.stdout or "BROKER_VM_FAIL" in result.stdout:
        raise RuntimeError(result.stdout[-15000:] + result.stderr[-900:])
    assert all(vm.digest(p) == h for p, h in backing.items())
    return dict(engine_sha256=vm.digest(engine), probe_sha256=vm.digest(probe), kernel_sha256=vm.digest(kernel),
                emulator=patch, base_images_unchanged=True,
                observations=[line for line in result.stdout.splitlines() if line.startswith("BROKER_VM_")])


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    for name in ("engine", "probe", "kernel", "manifest"):
        parser.add_argument("--" + name, required=True, type=Path)
    parser.add_argument("--release", required=True)
    opts = parser.parse_args()
    with tempfile.TemporaryDirectory(prefix="zerofs-claim-ns.", dir="/var/tmp") as tmp:
        receipt = run(Path(tmp), opts.kernel, opts.release, opts.engine, opts.probe, opts.manifest)
    assert not Path(tmp).exists()
    print(json.dumps({**receipt, "verified_removed": tmp}, indent=2))
