#!/usr/bin/env python3
"""Check case-only rename and exact lookup on a Linux FUSE mount."""

import argparse
import errno
import json
import os
from pathlib import Path
import subprocess
import tempfile
import time


def wait_for(predicate, process, label):
    deadline = time.monotonic() + 60
    while time.monotonic() < deadline:
        if predicate():
            return
        if process.poll() is not None:
            raise RuntimeError(f"{label} exited with {process.returncode}")
        time.sleep(0.1)
    raise TimeoutError(label)


def teardown(mountpoint, processes):
    errors = []
    try:
        if os.path.ismount(mountpoint):
            subprocess.run(["fusermount3", "-u", str(mountpoint)], check=True, timeout=20)
    except (OSError, subprocess.SubprocessError) as error:
        errors.append(f"unmount failed: {error}")
    finally:
        for process in processes:
            if process is None:
                continue
            try:
                process.terminate()
                process.wait(timeout=20)
            except (OSError, subprocess.SubprocessError) as error:
                errors.append(f"process required a kill: {error}")
                try:
                    process.kill()
                    process.wait(timeout=10)
                except (OSError, subprocess.SubprocessError) as error:
                    errors.append(f"process kill failed: {error}")
    if os.path.ismount(mountpoint):
        errors.append("mount required a lazy unmount")
        try:
            subprocess.run(["fusermount3", "-uz", str(mountpoint)], check=True, timeout=20)
        except (OSError, subprocess.SubprocessError) as error:
            errors.append(f"lazy unmount failed: {error}")
    if errors:
        raise RuntimeError("; ".join(errors))


def check(binary, relaxed):
    with tempfile.TemporaryDirectory(prefix="zerofs-case-fuse-") as fixture:
        root = Path(fixture)
        mountpoint = root / "mount"
        mountpoint.mkdir()
        socket = root / "9p.sock"
        config = root / "zerofs.toml"
        config.write_text(f'''
[cache]
dir = "{root / 'cache'}"
disk_size_gb = 0.1
memory_size_gb = 0.05
[storage]
url = "memory:///"
encryption_password = "case-fuse-test-password"
[filesystem]
name_mode = "insensitive"
[servers.ninep]
unix_socket = "{socket}"
[telemetry]
enabled = false
''')
        with (root / "server.log").open("w+") as server_log, (
            root / "mount.log"
        ).open("w+") as mount_log:
            server = subprocess.Popen(
                [binary, "run", "-c", str(config)], stdout=server_log, stderr=server_log
            )
            mount = None
            try:
                wait_for(socket.exists, server, "9P server")
                command = [
                    binary, "mount", "--writeback", "false",
                    "--relaxed-consistency", str(relaxed).lower(),
                ]
                command.extend([f"unix:{socket}", str(mountpoint)])
                mount = subprocess.Popen(command, stdout=mount_log, stderr=mount_log)
                wait_for(lambda: os.path.ismount(mountpoint), mount, "FUSE mount")
                (mountpoint / "Foo").write_bytes(b"payload")
                inode = (mountpoint / "Foo").stat().st_ino
                os.rename(mountpoint / "Foo", mountpoint / "foo")
                names = os.listdir(mountpoint)
                print(json.dumps({"relaxed": relaxed, "names_after_rename": names}), flush=True)
                assert names == ["foo"], names
                assert (mountpoint / "foo").stat().st_ino == inode
                assert (mountpoint / "foo").read_bytes() == b"payload"
                for spelling in ["Foo", "FOO"]:
                    try:
                        (mountpoint / spelling).stat()
                    except FileNotFoundError:
                        pass
                    else:
                        raise AssertionError(f"lookup accepted {spelling}")
                try:
                    fd = os.open(mountpoint / "FOO", os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
                except OSError as error:
                    assert error.errno == errno.EEXIST, error
                else:
                    os.close(fd)
                    raise AssertionError("fold-equal create succeeded")
                # A miss must not hide a newly created entry on a relaxed mount.
                assert not (mountpoint / "new").exists()
                (mountpoint / "new").write_bytes(b"new")
                assert (mountpoint / "new").read_bytes() == b"new"
                print(json.dumps({"relaxed": relaxed, "checks": 8, "result": "passed"}), flush=True)
            except BaseException:
                for label, log in [("server", server_log), ("mount", mount_log)]:
                    log.seek(0)
                    print(f"{label} log:\n{log.read()}", flush=True)
                raise
            finally:
                teardown(mountpoint, [mount, server])
        assert not os.path.ismount(mountpoint)
    assert not root.exists()
    print(f"fixture_removed={root}", flush=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", required=True)
    args = parser.parse_args()
    for relaxed in [False, True]:
        check(str(Path(args.binary).resolve()), relaxed)
