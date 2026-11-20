#!/usr/bin/env python3
"""Exercise full-pool reuse and crash integrity on disposable local storage.

Member A's quota is the only room. A replacement is written on one descriptor
until a WRITE answers ENOSPC; the bytes acknowledged before it must fsync.
Deleting a file must then free its room without a restart: member A's used
bytes drop within RECLAIM_BOUND_SECONDS, and the rest of the replacement is
written and fsynced on the same descriptor. An open reader or a checkpoint
keeps the deleted bytes readable and charged for at least that long, and WRITE
stays refused, until the pin is released. SIGKILL and restart then check
every hash. Run on Linux in an isolated mount namespace with CAP_SYS_ADMIN,
and /dev/fuse for the FUSE client.
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
RESERVE = 1_000_000
# A refused WRITE, and a WRITE or fsync with room, answer within this.
ANSWER_SECONDS = 5
# Unlink to freed room. Segment GC deletes a dead segment once the newest
# ephemeral checkpoint it saw first has expired, plus 30 s of clock skew, and
# never sooner than its 60 s in-flight read floor. Every metadata compaction
# writes such a checkpoint with a 15-minute lifetime. Add the tombstone GC's
# 10 s cadence and passes at the configured 5 s interval.
RECLAIM_BOUND_SECONDS = 900 + 30 + 60 + 10 + 5 * 5 + 55
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


def pool_used(binary, root, member=0):
    report = json.loads(
        command(str(binary), "pool", "status", "--endpoint", f"unix:{root / 'rpc.sock'}")
    )
    return report["members"][member]["used_bytes"]


def write_until_refused(descriptor, data, offset):
    """Write `data` from `offset` in 1 MiB requests. Returns the offset reached
    and the seconds the refused WRITE took, or None when every byte was taken."""
    while offset < len(data):
        started = time.monotonic()
        try:
            written = os.write(descriptor, data[offset : offset + MIB])
        except OSError as error:
            assert error.errno == errno.ENOSPC, error
            return offset, time.monotonic() - started
        assert written > 0
        assert time.monotonic() - started < ANSWER_SECONDS
        offset += written
    return offset, None


def run(binary, scratch, evidence, protection, client_kind):
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
        "client": client_kind,
    }
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
        # Member B's filesystem stays below its reserve for the whole case.
        (root / "member-b/ballast").write_bytes(bytes(free_bytes(root / "member-b") - 900_000))
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
[gc]
interval_secs = 5
idle_interval_secs = 1
[servers.ninep]
addresses = []
unix_socket = "{root / "9p.sock"}"
[servers.rpc]
addresses = []
unix_socket = "{root / "rpc.sock"}"
[telemetry]
enabled = false
''')
        command(str(binary), "pool", "init", "-c", str(config))

        def start(*options):
            (root / "9p.sock").unlink(missing_ok=True)
            (root / "rpc.sock").unlink(missing_ok=True)
            process = subprocess.Popen(
                [str(binary), "run", "-c", str(config), *options],
                stdout=log,
                stderr=log,
            )
            wait_for(
                lambda: (root / "9p.sock").exists() and (root / "rpc.sock").exists(),
                process,
                "server readiness",
            )
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

        def restart(*options):
            nonlocal server, client
            stop(client)
            if is_mounted(root / "mount"):
                unmount(root / "mount", client_kind)
            stop(server)
            server = start(*options)
            client = mount()

        server = start()
        client = mount()
        previous = b"earlier flushed data"
        data = hashlib.shake_256(b"delete-space").digest(32 * MIB)
        replacement = hashlib.shake_256(b"replacement").digest(len(data))
        result["deleted_sha256"] = hashlib.sha256(data).hexdigest()
        result["expected_replacement_sha256"] = hashlib.sha256(replacement).hexdigest()
        for name, payload in (("previous", previous), ("delete-me", data)):
            with (root / "mount" / name).open("wb", buffering=0) as target:
                assert write_until_refused(target.fileno(), payload, 0) == (len(payload), None)
                os.fsync(target.fileno())
        if protection == "open-reader":
            reader = (root / "mount/delete-me").open("rb", buffering=0)
        elif protection == "checkpoint":
            command(str(binary), "checkpoint", "create", "-c", str(config), "before-unlink")

        pending = os.open(root / "mount/replacement", os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        acknowledged, refused = write_until_refused(pending, replacement, 0)
        assert refused is not None, "fixture did not fill the pool"
        assert 0 < acknowledged < len(replacement), acknowledged
        assert refused < ANSWER_SECONDS, refused
        result["write_errno"] = "ENOSPC"
        result["acknowledged_bytes"] = acknowledged
        result["refused_write_seconds"] = refused
        # The acknowledged prefix flushes: nothing refused waits in memory.
        started = time.monotonic()
        os.fsync(pending)
        result["fsync_after_refusal_seconds"] = time.monotonic() - started
        assert result["fsync_after_refusal_seconds"] < ANSWER_SECONDS
        with (root / "mount/replacement").open("rb") as source:
            assert source.read() == replacement[:acknowledged]
        result["earlier_sha256"] = digest(root / "mount/previous")
        assert result["earlier_sha256"] == hashlib.sha256(previous).hexdigest()
        charged = pool_used(binary, root)
        result["member_a_used_before_unlink"] = charged

        (root / "mount/delete-me").unlink()
        unlinked = time.monotonic()
        if protection != "none":
            # As long as an unpinned deletion may take to be freed.
            time.sleep(RECLAIM_BOUND_SECONDS)
            assert pool_used(binary, root) == charged, "a pin did not keep the deleted bytes"
            assert write_until_refused(pending, replacement, acknowledged)[1] is not None
            result["pinned_seconds"] = time.monotonic() - unlinked
            if reader is not None:
                result["unlinked_reader_sha256"] = hashlib.file_digest(reader, "sha256").hexdigest()
                assert result["unlinked_reader_sha256"] == result["deleted_sha256"]
                reader.close()
                reader = None
            else:
                os.close(pending)
                pending = None
                restart("--checkpoint", "before-unlink")
                result["checkpoint_view_sha256"] = digest(root / "mount/delete-me")
                assert result["checkpoint_view_sha256"] == result["deleted_sha256"]
                restart()
                command(str(binary), "checkpoint", "delete", "-c", str(config), "before-unlink")
                pending = os.open(root / "mount/replacement", os.O_WRONLY)
                os.lseek(pending, acknowledged, os.SEEK_SET)
            unlinked = time.monotonic()

        deadline = unlinked + RECLAIM_BOUND_SECONDS
        while pool_used(binary, root) > charged - len(data):
            assert time.monotonic() < deadline, "the deleted bytes were not freed in time"
            time.sleep(1)
        result["release_to_freed_seconds"] = time.monotonic() - unlinked
        result["member_a_used_after_reclaim"] = pool_used(binary, root)
        # One attempt per WRITE: every remaining byte must be taken.
        assert write_until_refused(pending, replacement, acknowledged) == (len(replacement), None)
        started = time.monotonic()
        os.fsync(pending)
        result["rewrite_fsync_seconds"] = time.monotonic() - started
        assert result["rewrite_fsync_seconds"] < ANSWER_SECONDS
        result["release_to_flushed_seconds"] = time.monotonic() - unlinked
        assert digest(root / "mount/previous") == result["earlier_sha256"]

        # Kill before any graceful final seal/flush can change the evidence.
        stop(server, kill=True)
        stop(client, kill=True)
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
        assert digest(root / "mount/replacement") == result["expected_replacement_sha256"]
        result["replacement_hash_after_sigkill"] = result["expected_replacement_sha256"]
        assert not (root / "mount/delete-me").exists()
        print(json.dumps(result, sort_keys=True), flush=True)
        (evidence / "receipt.json").write_text(json.dumps(result, indent=2) + "\n")
    finally:
        stop(client, kill=True)
        if reader is not None:
            try:
                reader.close()
            except OSError:
                pass
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
        "--protection",
        choices=("none", "open-reader", "checkpoint"),
        default="none",
        help="pin the deleted bytes with an open reader or a checkpoint",
    )
    parser.add_argument("--client", choices=("fuse", "kernel-9p"), default="fuse")
    args = parser.parse_args()
    args.evidence.mkdir(parents=True, exist_ok=True)
    if not __debug__:
        parser.error("acceptance assertions require Python without -O")
    run(args.binary.resolve(), args.scratch, args.evidence, args.protection, args.client)


if __name__ == "__main__":
    main()
