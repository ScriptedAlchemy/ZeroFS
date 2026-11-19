"""Private autoclearing loop devices for the Studio Docker qualification only."""

import contextlib
import errno
import fcntl
import os
from pathlib import Path
import stat
import struct
import tempfile


@contextlib.contextmanager
def device(backing, nodes):
    # The loop driver atomically assigns this *open backing descriptor*. No
    # scan, global detach, or pre-existing loop mapping is used.
    with (
        backing.open("r+b", buffering=0) as disk,
        open("/dev/loop-control", "rb") as ctl,
    ):
        for _ in range(32):
            minor = fcntl.ioctl(ctl, 0x4C82)  # LOOP_CTL_GET_FREE
            node = nodes / f"loop{minor}"
            os.mknod(node, stat.S_IFBLK | 0o600, os.makedev(7, minor))
            try:
                handle = node.open("r+b", buffering=0)
            except BaseException:
                node.unlink()
                raise
            config = bytearray(304)  # linux loop_config, including loop_info64.
            struct.pack_into("=II", config, 0, disk.fileno(), 512)
            struct.pack_into("=I", config, 8 + 52, 4)  # LO_FLAGS_AUTOCLEAR
            try:
                fcntl.ioctl(handle, 0x4C0A, config)  # LOOP_CONFIGURE
            except OSError as error:
                handle.close()
                node.unlink()
                if error.errno == errno.EBUSY:
                    continue
                raise
            try:
                status = bytearray(232)
                fcntl.ioctl(handle, 0x4C05, status, True)
                meta = os.fstat(disk.fileno())
                assert struct.unpack_from("=QQ", status) == (meta.st_dev, meta.st_ino)
                yield node
            finally:
                handle.close()  # kernel autoclear on last close
                try:
                    with node.open("rb", buffering=0) as probe:
                        remaining = bytearray(232)
                        try:
                            fcntl.ioctl(probe, 0x4C05, remaining, True)
                        except OSError as error:
                            if error.errno != errno.ENXIO:
                                raise
                        else:
                            if struct.unpack_from("=QQ", remaining) == (
                                meta.st_dev,
                                meta.st_ino,
                            ):
                                raise RuntimeError("owned loop mapping remained active")
                finally:
                    node.unlink()
            return
        raise RuntimeError("could not atomically allocate a private loop")


@contextlib.contextmanager
def pool(count=3, size=16 << 20):
    with tempfile.TemporaryDirectory(dir="/dev/shm", prefix="zerofs-raw-") as scratch:
        with tempfile.TemporaryDirectory(dir="/dev", prefix="zerofs-raw-") as node_dir:
            disks = []
            for i in range(count):
                path = Path(scratch) / f"media-{i}"
                with path.open("xb") as disk:
                    os.fchmod(disk.fileno(), 0o600)
                    disk.truncate(size)
                disks.append(path)
            yield disks, Path(node_dir)
