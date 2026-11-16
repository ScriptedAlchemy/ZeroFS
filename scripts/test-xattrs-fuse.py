#!/usr/bin/env python3
"""Check empty xattr replacement through a local Linux ZeroFS FUSE mount."""

import argparse
import errno
import os
from pathlib import Path
import subprocess
import tempfile
import time


def run(*args):
    return subprocess.run(args, check=True, capture_output=True, timeout=15).stdout


def wait_for(predicate, process):
    deadline = time.monotonic() + 30
    while not predicate():
        if process.poll() is not None:
            raise RuntimeError(f"process stopped with status {process.returncode}")
        if time.monotonic() >= deadline:
            raise TimeoutError("local export did not become ready")
        time.sleep(0.1)


def check(binary, mode):
    with tempfile.TemporaryDirectory(prefix="zerofs-xattrs-fuse-") as scratch:
        root = Path(scratch)
        mount = root / "mount"
        mount.mkdir()
        socket = root / "9p.sock"
        config = root / "zerofs.toml"
        config.write_text(f'''[cache]
dir = "{root / 'cache'}"
disk_size_gb = 1.0
memory_size_gb = 0.5
[storage]
url = "memory:///"
encryption_password = "local-xattr-test"
[filesystem]
name_mode = "{mode}"
[servers.ninep]
unix_socket = "{socket}"
[telemetry]
enabled = false
''')
        processes = []
        with (root / "processes.log").open("wb+") as log:
            try:
                server = subprocess.Popen([binary, "run", "-c", str(config)], stdout=log, stderr=log)
                processes.append(server)
                wait_for(socket.exists, server)
                client = subprocess.Popen(
                    [binary, "mount", "--access", "all", "--writeback", "false",
                     f"unix:{socket}", str(mount)], stdout=log, stderr=log,
                )
                processes.append(client)
                wait_for(lambda: os.path.ismount(mount), client)
                target = mount / "File"
                target.touch()
                run("setfattr", "-n", "user.empty", "-v", "before", str(target))
                assert run("getfattr", "--only-values", "-n", "user.empty", str(target)) == b"before"
                os.setxattr(target, "user.empty", b"", os.XATTR_REPLACE)
                assert run("getfattr", "--only-values", "-n", "user.empty", str(target)) == b""
                assert "user.empty" in os.listxattr(target)
                run("setfattr", "-x", "user.empty", str(target))
                try:
                    os.getxattr(target, "user.empty")
                except OSError as error:
                    assert error.errno == errno.ENODATA, error
                else:
                    raise AssertionError("removed attribute still exists")
                target.unlink()
                print(f"The {mode} FUSE mount passed empty replacement and explicit removal.", flush=True)
            except BaseException:
                log.flush()
                log.seek(0)
                print(log.read().decode(errors="replace"), flush=True)
                raise
            finally:
                try:
                    if os.path.ismount(mount):
                        run("fusermount3", "-u", str(mount))
                finally:
                    for process in reversed(processes):
                        if process.poll() is None:
                            process.terminate()
                        try:
                            process.wait(timeout=15)
                        except subprocess.TimeoutExpired:
                            process.kill()
                            process.wait(timeout=5)
        assert not os.path.ismount(mount)
    assert not root.exists()
    print(f"The {mode} fixture and its processes were removed from {root}.", flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", required=True)
    args = parser.parse_args()
    for mode in ["sensitive", "insensitive"]:
        check(str(Path(args.binary).resolve()), mode)


if __name__ == "__main__":
    main()
