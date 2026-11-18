#!/usr/bin/env python3
"""Exercise full-pool reuse and crash integrity on disposable local storage.

This acceptance test currently fails the immediate-reuse assertion. It still
checks surviving data after SIGKILL before reporting that failure. Run on Linux
in an isolated mount namespace with /dev/fuse and CAP_SYS_ADMIN.
"""

import argparse
import errno
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import time


MIB = 1024**2


def command(*args):
    result = subprocess.run(args, capture_output=True, timeout=30)
    if result.returncode:
        raise RuntimeError(
            f"{args}: exit {result.returncode}\n{result.stdout.decode()}\n{result.stderr.decode()}"
        )
    return result.stdout


def wait_for(predicate, process, description):
    deadline = time.monotonic() + 30
    while not predicate():
        if process.poll() is not None:
            raise RuntimeError(f"{description}: process exited {process.returncode}")
        if time.monotonic() >= deadline:
            raise TimeoutError(description)
        time.sleep(0.05)


def stop(process, kill=False):
    if process is None:
        return
    if process.poll() is None:
        process.kill() if kill else process.terminate()
    try:
        process.wait(timeout=10)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait(timeout=5)


def is_mounted(path):
    # stat-based ismount misses disconnected FUSE mounts after SIGKILL.
    escaped = str(path).replace("\\", "\\134").replace(" ", "\\040")
    return any(
        line.split()[4] == escaped
        for line in Path("/proc/self/mountinfo").read_text().splitlines()
    )


def digest(path):
    with path.open("rb") as source:
        return hashlib.file_digest(source, "sha256").hexdigest()


def run(binary, scratch, evidence):
    root = Path(tempfile.mkdtemp(prefix="zerofs-delete-reuse-", dir=scratch))
    mounted = []
    server = client = None
    pending = None
    log = (evidence / "processes.log").open("wb")
    result = {"kernel": os.uname().release, "binary_sha256": digest(binary)}
    try:
        for name in ("member-a", "member-b", "metadata", "catalog", "cache", "mount"):
            (root / name).mkdir(mode=0o700)
        for name, size in (("member-a", "3g"), ("member-b", "8m")):
            path = root / name
            command(
                "mount",
                "-t",
                "tmpfs",
                "-o",
                f"size={size}",
                "zerofs-delete-reuse",
                str(path),
            )
            mounted.append(path)
        space = os.statvfs(root / "member-b")
        (root / "member-b/ballast").write_bytes(
            bytes(space.f_bavail * space.f_frsize - 900_000)
        )
        config = root / "zerofs.toml"
        config.write_text(f'''[cache]
dir = "{root / "cache"}"
disk_size_gb = 0.064
memory_size_gb = 0.001
warm_metadata = "off"
[storage]
url = "file://{root / "metadata"}"
encryption_password = "synthetic-delete-reuse"
[storage.pool]
catalog_dir = "{root / "catalog"}"
[[storage.pool.members]]
id = "member-a"
url = "file://{root / "member-a"}"
capacity_gb = 0.05
reserve_free_gb = 0.001
[[storage.pool.members]]
id = "member-b"
url = "file://{root / "member-b"}"
capacity_gb = 0.05
reserve_free_gb = 0.001
[servers.ninep]
addresses = []
unix_socket = "{root / "9p.sock"}"
[telemetry]
enabled = false
''')
        command(str(binary), "pool", "init", "-c", str(config))

        def start():
            (root / "9p.sock").unlink(missing_ok=True)
            process = subprocess.Popen(
                [str(binary), "run", "-c", str(config)], stdout=log, stderr=log
            )
            wait_for((root / "9p.sock").exists, process, "server readiness")
            return process

        def mount():
            process = subprocess.Popen(
                [
                    str(binary),
                    "mount",
                    "--writeback",
                    "false",
                    "--relaxed-consistency",
                    "false",
                    "--reconnect-timeout",
                    "1",
                    f"unix:{root / '9p.sock'}",
                    str(root / "mount"),
                ],
                stdout=log,
                stderr=log,
            )
            wait_for(lambda: is_mounted(root / "mount"), process, "FUSE readiness")
            return process

        server = start()
        client = mount()
        previous = b"earlier flushed data"
        data = hashlib.shake_256(b"delete-space").digest(32 * MIB)
        for name, payload in (("previous", previous), ("delete-me", data)):
            with (root / "mount" / name).open("wb", buffering=0) as target:
                for offset in range(0, len(payload), MIB):
                    assert target.write(payload[offset : offset + MIB]) == len(
                        payload[offset : offset + MIB]
                    )
                os.fsync(target.fileno())
        pending = os.open(root / "mount/replacement", os.O_WRONLY | os.O_CREAT, 0o600)
        for offset in range(0, len(data), MIB):
            assert os.write(pending, data[offset : offset + MIB]) == MIB
        try:
            os.fsync(pending)
        except OSError as error:
            assert error.errno == errno.ENOSPC, error
        else:
            raise AssertionError("fixture did not fill the pool")
        result["earlier_sha256"] = digest(root / "mount/previous")
        assert result["earlier_sha256"] == hashlib.sha256(previous).hexdigest()
        started = time.monotonic()
        (root / "mount/delete-me").unlink()
        # One retry on the same pending descriptor; no sleep, polling, or restart.
        try:
            os.fsync(pending)
        except OSError as error:
            result["reuse_errno"] = error.errno
        else:
            result["reuse_errno"] = None
        result["delete_and_flush_seconds"] = time.monotonic() - started
        assert digest(root / "mount/previous") == result["earlier_sha256"]

        # Kill before any graceful final seal/flush can change the evidence.
        stop(server, kill=True)
        stop(client, kill=True)
        try:
            os.close(pending)
        except OSError as error:
            assert error.errno in (errno.EIO, errno.ENOTCONN), error
        pending = None
        command("fusermount3", "-u", str(root / "mount"))
        server = start()
        client = mount()
        assert digest(root / "mount/previous") == result["earlier_sha256"]
        result["earlier_hash_after_sigkill"] = result["earlier_sha256"]
        if result["reuse_errno"] is None:
            assert (
                digest(root / "mount/replacement") == hashlib.sha256(data).hexdigest()
            )
            result["replacement_hash_after_sigkill"] = hashlib.sha256(data).hexdigest()
        else:
            # The deletion was not fsynced: its old durable name may return.
            if (root / "mount/delete-me").exists():
                assert (
                    digest(root / "mount/delete-me") == hashlib.sha256(data).hexdigest()
                )
                result["old_target_hash_after_sigkill"] = hashlib.sha256(
                    data
                ).hexdigest()
        print(json.dumps(result, sort_keys=True), flush=True)
        (evidence / "receipt.json").write_text(json.dumps(result, indent=2) + "\n")
        assert result["reuse_errno"] is None, (
            f"immediate reuse remains broken: {result}"
        )
        assert result["delete_and_flush_seconds"] < 5, result
    finally:
        stop(client, kill=True)
        if pending is not None:
            try:
                os.close(pending)
            except OSError:
                pass
        if is_mounted(root / "mount"):
            command("fusermount3", "-u", str(root / "mount"))
        stop(server)
        for path in reversed(mounted):
            command("umount", str(path))
        log.close()
        shutil.rmtree(root)
        assert not root.exists()
        print(f"Removed owned fixture {root}, mounts, clients, and server.", flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--scratch", type=Path, required=True)
    parser.add_argument("--evidence", type=Path, required=True)
    args = parser.parse_args()
    args.evidence.mkdir(parents=True, exist_ok=True)
    run(args.binary.resolve(), args.scratch, args.evidence)


if __name__ == "__main__":
    main()
