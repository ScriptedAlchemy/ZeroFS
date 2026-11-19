#!/usr/bin/env python3
"""Verify xfstests mount clients survive test scope cleanup and repeated remounts."""

import argparse
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import time


def run(*args, env=None):
    return subprocess.run(args, env=env, check=True, capture_output=True, text=True, timeout=30).stdout


def check(binary, helper, user_systemd):
    with tempfile.TemporaryDirectory(prefix="zerofs-xfstests-lifecycle-") as scratch:
        root = Path(scratch)
        mount = root / "mount"
        mount.mkdir()
        socket = root / "9p.sock"
        config = root / "zerofs.toml"
        config.write_text(f'''[cache]
dir = "{root / 'cache'}"
disk_size_gb = 0.1
memory_size_gb = 0.05
[storage]
url = "memory:///"
encryption_password = "xfstests-lifecycle-test"
[servers.ninep]
unix_socket = "{socket}"
[telemetry]
enabled = false
''')
        env = dict(os.environ, ZEROFS_BIN=binary, ZEROFS_MOUNT_LOG=str(root / "mount.log"))
        # Exercise the same helper against an unprivileged user manager locally.
        # Hosted CI uses the system manager with no wrappers.
        if user_systemd:
            wrappers = root / "bin"
            wrappers.mkdir()
            for command in ["systemctl", "systemd-run"]:
                wrapper = wrappers / command
                wrapper.write_text(f'#!/bin/sh\nexec {shutil.which(command)} --user "$@"\n')
                wrapper.chmod(0o755)
            env["PATH"] = f"{wrappers}:{env['PATH']}"
        unit = "zerofs-xfstests-" + run("systemd-escape", "--path", str(mount)).strip() + ".service"
        scope = "zerofs-xfstests-lifecycle-" + root.name + ".scope"
        with (root / "server.log").open("w+") as log:
            server = subprocess.Popen([binary, "run", "-c", str(config)], stdout=log, stderr=log)
            try:
                deadline = time.monotonic() + 30
                while not socket.exists():
                    if server.poll() is not None or time.monotonic() >= deadline:
                        raise RuntimeError("9P server did not start")
                    time.sleep(0.1)
                for iteration in range(5):
                    run("systemd-run", "--quiet", "--scope", f"--unit={scope}",
                        "bash", "-c", '"$@"; result=$?; sleep 60 </dev/null >/dev/null 2>&1 & exit "$result"',
                        "sh", helper, str(socket), str(mount), env=env)
                    run("systemctl", "stop", scope, env=env)
                    if not os.path.ismount(mount):
                        raise AssertionError("test scope cleanup unmounted the filesystem")
                    target = mount / "data"
                    if iteration:
                        assert target.read_text() == f"iteration {iteration - 1}"
                    target.write_text(f"iteration {iteration}")
                    assert target.read_text() == f"iteration {iteration}"
                    rows = run("df", "-TP").splitlines()
                    assert any(row.split()[0] == str(socket) and row.split()[1] in {"fuse", "fuse.zerofs"}
                               for row in rows[1:]), "df lost the mounted filesystem row"
                    # Alternate an external unmount with replacement of a live mount.
                    if iteration % 2 == 0:
                        run("fusermount3", "-u", str(mount))
                run("systemctl", "stop", unit, env=env)
                assert not os.path.ismount(mount)
                assert subprocess.run(["systemctl", "is-active", "--quiet", unit], env=env).returncode != 0
                print("Five scope cleanup/remount cycles passed, with verified data and df rows.", flush=True)
            except BaseException:
                log.flush()
                print((root / "server.log").read_text(), flush=True)
                if (root / "mount.log").exists():
                    print((root / "mount.log").read_text(), flush=True)
                raise
            finally:
                subprocess.run(["systemctl", "stop", scope, unit], env=env, capture_output=True, timeout=30)
                if os.path.ismount(mount):
                    run("fusermount3", "-u", str(mount))
                server.terminate()
                try:
                    server.wait(timeout=15)
                except subprocess.TimeoutExpired:
                    server.kill()
                    server.wait(timeout=5)
        assert not os.path.ismount(mount)
    assert not root.exists()
    print("The fixture, mount services and server were removed.", flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", required=True)
    parser.add_argument("--helper", default=str(Path(__file__).parent / "ci/xfstests-fuse-mount.sh"))
    parser.add_argument("--user-systemd", action="store_true")
    args = parser.parse_args()
    check(str(Path(args.binary).resolve()), str(Path(args.helper).resolve()), args.user_systemd)


if __name__ == "__main__":
    main()
