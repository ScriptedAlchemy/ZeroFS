#!/usr/bin/env python3
"""Exercise full-pool reuse and crash integrity on disposable local storage.

With `--room-from unlink`, the unprotected case requires the unlink alone to
make room. It fails that assertion today, after checking the surviving data
after SIGKILL. With `--room-from member-b`, the same pending descriptor must
still get ENOSPC after the unlink. Releasing ballast on member-b then makes
room for the whole refused seal, and the next fsync must succeed within five
seconds of that release, with every protection still holding the old bytes.
Run on Linux in an isolated mount namespace with CAP_SYS_ADMIN, and /dev/fuse
for the FUSE client.
"""

import argparse
import errno
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import tempfile
import time


MIB = 1024**2
RESERVE = 1_000_000
QUOTA = 50_000_000
FLUSH_DEADLINE_SECONDS = 5
KERNEL_9P = "trans=unix,version=9p2000.L,msize=1048576,cache=none,access=user,nodev,nosuid"


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
            stop(process, kill=True)
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


def unmount(path, client_kind):
    if client_kind == "kernel-9p":
        # A kernel 9P mount whose server was killed can only be detached.
        command("umount", "-l", str(path))
    else:
        command("fusermount3", "-u", str(path))


def free_bytes(path):
    usage = os.statvfs(path)
    return usage.f_bavail * usage.f_frsize


def refused_seal_bytes(log_path, offset):
    with log_path.open("rb") as source:
        source.seek(offset)
        sizes = set(re.findall(rb"no pool member has room for a (\d+)-byte object", source.read()))
    assert len(sizes) == 1, sizes
    return int(sizes.pop())


def run(binary, scratch, evidence, distinct_payload, protection, client_kind, room_from):
    root = Path(tempfile.mkdtemp(prefix="zerofs-delete-reuse-", dir=scratch))
    mounted = []
    server = client = None
    pending = None
    reader = None
    log_path = evidence / "processes.log"
    log = log_path.open("wb")
    result = {
        "kernel": os.uname().release,
        "binary_sha256": digest(binary),
        "protection": protection,
        "distinct_payload": distinct_payload,
        "client": client_kind,
        "room_from": room_from,
    }
    ballast = None
    try:
        for name in ("member-a", "member-b", "metadata", "catalog", "cache", "mount"):
            (root / name).mkdir(mode=0o700)
        member_b_size = "8m" if room_from == "unlink" else "64m"
        for name, size in (("member-a", "3g"), ("member-b", member_b_size)):
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
        ballast = root / "member-b/ballast"
        ballast.write_bytes(bytes(free_bytes(root / "member-b") - 900_000))
        assert free_bytes(root / "member-b") < RESERVE
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
        if protection == "checkpoint":
            with config.open("a") as stream:
                stream.write(
                    f'\n[servers.rpc]\naddresses = []\nunix_socket = "{root / "rpc.sock"}"\n'
                )
        command(str(binary), "pool", "init", "-c", str(config))

        def start(*options):
            (root / "9p.sock").unlink(missing_ok=True)
            (root / "rpc.sock").unlink(missing_ok=True)
            process = subprocess.Popen(
                [str(binary), "run", "-c", str(config), *options],
                stdout=log,
                stderr=log,
            )
            wait_for((root / "9p.sock").exists, process, "server readiness")
            return process

        def mount():
            if client_kind == "kernel-9p":
                command("mount", "-t", "9p", "-o", KERNEL_9P, str(root / "9p.sock"), str(root / "mount"))
                return None
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
        replacement = (
            hashlib.shake_256(b"replacement").digest(len(data))
            if distinct_payload
            else data
        )
        result["deleted_sha256"] = hashlib.sha256(data).hexdigest()
        result["expected_replacement_sha256"] = hashlib.sha256(replacement).hexdigest()
        for name, payload in (("previous", previous), ("delete-me", data)):
            with (root / "mount" / name).open("wb", buffering=0) as target:
                for offset in range(0, len(payload), MIB):
                    assert target.write(payload[offset : offset + MIB]) == len(
                        payload[offset : offset + MIB]
                    )
                os.fsync(target.fileno())
        if protection == "open-reader":
            reader = (root / "mount/delete-me").open("rb", buffering=0)
        elif protection == "checkpoint":
            command(
                str(binary), "checkpoint", "create", "-c", str(config), "before-unlink"
            )
        pending = os.open(
            root / "mount/replacement", os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600
        )
        for offset in range(0, len(replacement), MIB):
            assert os.write(pending, replacement[offset : offset + MIB]) == MIB
        log.flush()
        offset = log_path.stat().st_size
        try:
            os.fsync(pending)
        except OSError as error:
            assert error.errno == errno.ENOSPC, error
        else:
            raise AssertionError("fixture did not fill the pool")
        seal = refused_seal_bytes(log_path, offset)
        assert seal >= len(replacement), seal
        result["refused_seal_bytes"] = seal
        result["earlier_sha256"] = digest(root / "mount/previous")
        assert result["earlier_sha256"] == hashlib.sha256(previous).hexdigest()
        started = time.monotonic()
        (root / "mount/delete-me").unlink()
        if room_from == "member-b":
            # The unlink alone leaves no member room for the refused seal.
            try:
                os.fsync(pending)
            except OSError as error:
                assert error.errno == errno.ENOSPC, error
            else:
                raise AssertionError("fsync succeeded before any member had room")
            # The deadline starts before the operation that makes room.
            started = time.monotonic()
            ballast.unlink()
            ballast = None
            room = free_bytes(root / "member-b") - RESERVE
            result["member_b_room_bytes"] = room
            assert min(room, QUOTA) >= seal, (room, seal)
        # One retry on the same pending descriptor; no sleep, polling, or restart.
        try:
            os.fsync(pending)
        except OSError as error:
            result["reuse_errno"] = error.errno
        else:
            result["reuse_errno"] = None
        result["delete_and_flush_seconds"] = time.monotonic() - started
        assert digest(root / "mount/previous") == result["earlier_sha256"]
        if reader is not None:
            result["unlinked_reader_sha256"] = hashlib.file_digest(
                reader, "sha256"
            ).hexdigest()
            assert result["unlinked_reader_sha256"] == result["deleted_sha256"]

        # Kill before any graceful final seal/flush can change the evidence.
        stop(server, kill=True)
        stop(client, kill=True)
        if reader is not None:
            # The reader's bytes were verified above; its client is gone.
            try:
                reader.close()
            except OSError as error:
                assert error.errno in (errno.EIO, errno.ENOTCONN), error
            reader = None
        try:
            os.close(pending)
        except OSError as error:
            assert error.errno in (errno.EIO, errno.ENOTCONN), error
        pending = None
        unmount(root / "mount", client_kind)
        server = start()
        client = mount()
        assert digest(root / "mount/previous") == result["earlier_sha256"]
        result["earlier_hash_after_sigkill"] = result["earlier_sha256"]
        if result["reuse_errno"] is None:
            assert (
                digest(root / "mount/replacement")
                == result["expected_replacement_sha256"]
            )
            result["replacement_hash_after_sigkill"] = result[
                "expected_replacement_sha256"
            ]
            # The acknowledged fsync also made the earlier unlink durable.
            assert not (root / "mount/delete-me").exists()
        else:
            # The deletion was not fsynced: its old durable name may return.
            if (root / "mount/delete-me").exists():
                assert (
                    digest(root / "mount/delete-me") == hashlib.sha256(data).hexdigest()
                )
                result["old_target_hash_after_sigkill"] = hashlib.sha256(
                    data
                ).hexdigest()
        if protection == "checkpoint":
            stop(client)
            if is_mounted(root / "mount"):
                unmount(root / "mount", client_kind)
            stop(server)
            server = start("--checkpoint", "before-unlink")
            client = mount()
            result["checkpoint_after_restart_sha256"] = digest(root / "mount/delete-me")
            assert result["checkpoint_after_restart_sha256"] == result["deleted_sha256"]
            assert digest(root / "mount/previous") == result["earlier_sha256"]
        print(json.dumps(result, sort_keys=True), flush=True)
        (evidence / "receipt.json").write_text(json.dumps(result, indent=2) + "\n")
        expected_errno = (
            None if protection == "none" or room_from == "member-b" else errno.ENOSPC
        )
        assert result["reuse_errno"] == expected_errno, result
        assert result["delete_and_flush_seconds"] < FLUSH_DEADLINE_SECONDS, result
    finally:
        stop(client, kill=True)
        if reader is not None:
            reader.close()
        if pending is not None:
            try:
                os.close(pending)
            except OSError:
                pass
        if is_mounted(root / "mount"):
            unmount(root / "mount", client_kind)
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
    parser.add_argument(
        "--distinct-payload",
        action="store_true",
        help="stage different bytes from the file being deleted",
    )
    parser.add_argument(
        "--protection",
        choices=("none", "open-reader", "checkpoint"),
        default="none",
        help="protect old bytes on the same quota-limited fixture; with room only "
        "from the unlink, require ENOSPC",
    )
    parser.add_argument("--client", choices=("fuse", "kernel-9p"), default="fuse")
    parser.add_argument(
        "--room-from",
        choices=("unlink", "member-b"),
        default="unlink",
        help="what makes room for the refused seal: the unlink alone, or "
        "releasing ballast on member-b after the unlink",
    )
    args = parser.parse_args()
    args.evidence.mkdir(parents=True, exist_ok=True)
    if not __debug__:
        parser.error("acceptance assertions require Python without -O")
    run(
        args.binary.resolve(),
        args.scratch,
        args.evidence,
        args.distinct_payload or args.protection != "none" or args.room_from != "unlink",
        args.protection,
        args.client,
        args.room_from,
    )


if __name__ == "__main__":
    main()
