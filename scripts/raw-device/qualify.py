"""Run inside the isolated Studio Docker container described in the receipt."""

import contextlib
import fcntl
import hashlib
import json
import os
from pathlib import Path
import secrets
import select
import stat
import subprocess
import sys
import tempfile
import time
import uuid

from fixture import device, pool


def invoke(operation, node, disk, identity, *args, data=b"", expect=None):
    run = subprocess.run(
        [sys.argv[1], operation, node, disk, *identity, *args],
        input=data,
        capture_output=True,
        timeout=30,
        check=False,
    )
    if expect is None:
        if run.returncode:
            raise RuntimeError(f"{operation}: {run.stderr.decode()}")
    elif run.returncode == 0 or expect.lower() not in run.stderr.decode().lower():
        raise RuntimeError(
            f"{operation}: expected {expect!r}, got {run.returncode} {run.stderr!r}"
        )
    return run.stdout


def record(case, **fields):
    print(json.dumps({"case": case, **fields}, sort_keys=True), flush=True)


def digest(data):
    return hashlib.sha256(data).hexdigest()


def cut_session(node, disk, identity, key, replacement, *, flush):
    with subprocess.Popen(
        [sys.argv[1], "session", node, disk, *identity, key],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    ) as proc:
        try:

            def exchange(frame, expected):
                proc.stdin.write(frame)
                proc.stdin.flush()
                if not select.select([proc.stdout], [], [], 20)[0]:
                    raise TimeoutError("raw writer missed its acknowledgement deadline")
                reply = proc.stdout.readline()
                if reply != expected:
                    raise RuntimeError(f"expected {expected!r}, received {reply!r}")

            exchange(f"PUT {len(replacement)}\n".encode() + replacement, b"STAGED\n")
            if flush:
                exchange(b"FLUSH\n", b"FLUSHED\n")
            ack = time.monotonic_ns()
            proc.kill()
            signal_time = time.monotonic_ns()
            assert proc.wait(timeout=10) == -9
        finally:
            if proc.poll() is None:
                proc.kill()
                proc.wait(timeout=10)
    return signal_time - ack


def run():
    if not Path("/.dockerenv").exists():
        raise RuntimeError("disposable Docker fixture required")
    pool_id = str(uuid.uuid4())
    identities = [
        (pool_id, str(uuid.uuid4()), role)
        for role in ("payload", "payload", "authority")
    ]
    size = 16 << 20
    with pool(size=size) as (disks, nodes):
        data = [secrets.token_bytes(512 * 1024 + i) for i in range(len(disks))]
        key = "immutable/object-1"
        with contextlib.ExitStack() as opened:
            active = [opened.enter_context(device(path, nodes)) for path in disks]
            # Never name or open a host boot disk in the test. Admission must
            # reject a non-loop block node by its major before opening it.
            excluded = nodes / "excluded"
            os.mknod(excluded, stat.S_IFBLK | 0o600, os.makedev(240, 99))
            invoke(
                "init-blank",
                excluded,
                disks[0],
                identities[0],
                expect="only Linux loop",
            )
            excluded.unlink()
            invoke(
                "init-blank",
                disks[0],
                disks[0],
                identities[0],
                expect="only Linux loop",
            )
            invoke(
                "init-blank",
                active[0],
                disks[1],
                identities[0],
                expect="identity mismatch",
            )
            assert disks[0].read_bytes() == bytes(size)
            record(
                "excluded device class, regular file and wrong backing inode refused"
            )
            exclusive = os.open(active[0], os.O_RDONLY | os.O_EXCL)
            try:
                invoke("init-blank", active[0], disks[0], identities[0], expect="busy")
            finally:
                os.close(exclusive)
            record("kernel-exclusive holder blocks competing initialization")
            for node, disk, identity, payload in zip(active, disks, identities, data):
                invoke("init-blank", node, disk, identity)
                invoke("put-flush", node, disk, identity, key, data=payload)
                assert invoke("get", node, disk, identity, key) == payload
                before = digest(disk.read_bytes())
                invoke("init-blank", node, disk, identity, expect="already")
                assert digest(disk.read_bytes()) == before
                record(
                    "put-flush and fresh-process get",
                    role=identity[2],
                    uuid=identity[1],
                    payload_sha256=digest(payload),
                    bytes=len(payload),
                )
            # Cut a real live process before and after its explicit durability barrier.
            for committed in (False, True):
                replacement = secrets.token_bytes(600_013)
                delay = cut_session(
                    active[0],
                    disks[0],
                    identities[0],
                    key,
                    replacement,
                    flush=committed,
                )
                if committed:
                    data[0] = replacement
                recovered = invoke("get", active[0], disks[0], identities[0], key)
                assert recovered == data[0]
                record(
                    "SIGKILL and reopen",
                    flush_acknowledged=committed,
                    ack_to_signal_ns=delay,
                    recovered_sha256=digest(recovered),
                )
            # An object larger than available media must preserve the last commit.
            invoke(
                "put-flush",
                active[0],
                disks[0],
                identities[0],
                key,
                data=bytes(size),
                expect="no space",
            )
            assert invoke("get", active[0], disks[0], identities[0], key) == data[0]
            record("raw extent capacity refusal, previous generation readable")
        # All loop bindings have been released. Reverse attachment order,
        # leave the original first member missing and explicitly probe it.
        with contextlib.ExitStack() as reopened:
            p3 = reopened.enter_context(device(disks[2], nodes))
            b = reopened.enter_context(device(disks[1], nodes))
            assert invoke("get", p3, disks[2], identities[2], key) == data[2]
            assert invoke("get", b, disks[1], identities[1], key) == data[1]
            invoke(
                "get",
                nodes / "absent",
                disks[0],
                identities[0],
                key,
                expect="No such file",
            )
            before = digest(disks[1].read_bytes())
            for wrong in (
                identities[0],
                (str(uuid.uuid4()), *identities[1][1:]),
                (*identities[1][:2], "authority"),
            ):
                invoke("get", b, disks[1], wrong, key, expect="binding mismatch")
            assert digest(disks[1].read_bytes()) == before
            record("missing member, wrong pool/member/role, surviving hashes")
            a = reopened.enter_context(device(disks[0], nodes))
            assert invoke("get", a, disks[0], identities[0], key) == data[0]
        with pool(1, size) as (dirty, dirty_nodes):
            with dirty[0].open("r+b", buffering=0) as backing:
                backing.seek(-1, 2)
                backing.write(b"X")
            before = digest(dirty[0].read_bytes())
            with device(dirty[0], dirty_nodes) as node:
                invoke("init-blank", node, dirty[0], identities[0], expect="not blank")
            assert digest(dirty[0].read_bytes()) == before
            record("nonzero last sector refused, bytes preserved")
        # A recognized foreign filesystem is a negative control only.
        with pool(1, size) as (foreign, foreign_nodes):
            with device(foreign[0], foreign_nodes) as node:
                subprocess.run(["mkfs.ext4", "-q", "-F", node], check=True)
                before = digest(foreign[0].read_bytes())
                invoke(
                    "init-blank", node, foreign[0], identities[0], expect="not blank"
                )
                assert digest(foreign[0].read_bytes()) == before
                with tempfile.TemporaryDirectory(dir="/dev/shm") as mounted:
                    subprocess.run(
                        ["mount", "-o", "ro,noload,nodev,nosuid,noexec", node, mounted],
                        check=True,
                    )
                    try:
                        invoke(
                            "init-blank", node, foreign[0], identities[0], expect="busy"
                        )
                    finally:
                        subprocess.run(["umount", mounted], check=True)
                assert digest(foreign[0].read_bytes()) == before
                record(
                    "foreign ext4 and mounted device refused, whole-device hash preserved"
                )
    record("owned loop mappings released, private nodes and backing files removed")


def live_media_loss():
    # The synthetic controller removes the media behind an *already opened*
    # loop. Keep the raw store alive across the cut. Evict clean block cache
    # so GET must observe the impaired device; only the fixture owns this inode.
    identity = (str(uuid.uuid4()), str(uuid.uuid4()), "payload")
    with pool(1) as (disks, nodes), device(disks[0], nodes) as node:
        invoke("init-blank", node, disks[0], identity)
        data = secrets.token_bytes(512_003)
        with (
            node.open("rb", buffering=0) as controller,
            subprocess.Popen(
                [sys.argv[1], "session", node, disks[0], *identity, "live"],
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            ) as proc,
        ):
            try:
                proc.stdin.write(f"PUT {len(data)}\n".encode() + data + b"FLUSH\n")
                proc.stdin.flush()
                assert proc.stdout.readline() == b"STAGED\n"
                assert proc.stdout.readline() == b"FLUSHED\n"
                with disks[0].open("r+b", buffering=0) as media:
                    media.truncate(0)
                fcntl.ioctl(controller, 0x1261)  # BLKFLSBUF, scoped to this loop
                output, error = proc.communicate(b"GET\n", timeout=15)
                assert proc.returncode != 0 and not output, (
                    proc.returncode,
                    output[:40],
                    error,
                )
                record(
                    "already-open store reports live media loss",
                    error=error.decode().strip(),
                    original_sha256=digest(data),
                )
            finally:
                if proc.poll() is None:
                    proc.kill()
                    proc.wait(timeout=10)
        invoke("get", node, disks[0], identity, "live", expect="geometry")
        record("cold open refuses lost backing geometry")


def landing():
    pool_id = str(uuid.uuid4())
    ids = [
        (pool_id, str(uuid.uuid4()), role) for role in ("journal", "payload", "payload")
    ]
    with pool() as (disks, nodes):
        payloads = [secrets.token_bytes(400_001), secrets.token_bytes(600_003)]
        hashes = list(map(digest, payloads))
        with disks[0].open("r+b") as p3:
            p3.truncate(32 << 20)
        plan = "14,4,4,2,4,4"

        def journal_op(op, node, *args, **kwargs):
            return invoke("p3-" + op, node, disks[0], ids[0], plan, *args, **kwargs)

        with contextlib.ExitStack() as stack:
            active = [stack.enter_context(device(d, nodes)) for d in disks]
            invoke("p3-init-blank", active[0], disks[0], (*ids[0][:2], "p3"), plan)
            for role in ("authority", "catalog", "key-envelope", "cache", "scratch"):
                # Independent sentinels exercise region isolation with the same object key.
                invoke(
                    "p3-put-flush",
                    active[0],
                    disks[0],
                    (*ids[0][:2], role),
                    plan,
                    "sentinel",
                    data=role.encode(),
                )
            for i in (1, 2):
                invoke("init-blank", active[i], disks[i], ids[i])

            for i, payload in enumerate(payloads, 1):
                for _ in range(2):
                    assert (
                        journal_op("stage", active[0], ids[i][1], data=payload)
                        .decode()
                        .strip()
                        == hashes[i - 1]
                    )
        # Only the journal is present. Discover the complete pending queue
        # after dropping all processes and device bindings.
        with contextlib.ExitStack() as stack:
            journal = stack.enter_context(device(disks[0], nodes))

            def pending():
                return set(journal_op("pending", journal).decode().splitlines())

            def item(i, h):
                return f"{uuid.UUID(ids[i][1]).hex} {h}"

            assert pending() == {item(1, hashes[0]), item(2, hashes[1])}
            journal_op(
                "drain",
                journal,
                nodes / "missing",
                disks[1],
                ids[1][1],
                hashes[0],
                expect="No such file",
            )
            for role in ("authority", "catalog", "key-envelope", "cache", "scratch"):
                assert (
                    invoke(
                        "p3-get",
                        journal,
                        disks[0],
                        (*ids[0][:2], role),
                        plan,
                        "sentinel",
                    )
                    == role.encode()
                )
            invoke(
                "p3-get",
                journal,
                disks[0],
                (*ids[0][:2], "catalog"),
                "14,4,4,2,5,3",
                "sentinel",
                expect="size",
            )
            record("all six P3 region bindings checked, wrong geometry refused")
            targets = [stack.enter_context(device(d, nodes)) for d in disks[1:]]
            for i, (node, h) in enumerate(zip(targets, hashes), 1):
                for _ in range(2):
                    journal_op("drain", journal, node, disks[i], ids[i][1], h)
                assert item(i, h) not in pending()
                assert (
                    invoke("get", node, disks[i], ids[i], "segment/" + h)
                    == payloads[i - 1]
                )
            assert not pending()
            record(
                "cold journal replay, missing destination retained, idempotent drain to both members",
                sha256=hashes,
            )
            # Fill member B without touching A or its committed data. Refusal
            # to land the next segment must keep its durable journal entry.
            invoke(
                "put-flush",
                targets[1],
                disks[2],
                ids[2],
                "ballast",
                data=bytes(14 << 20),
            )
            large = secrets.token_bytes(2 << 20)
            h = digest(large)
            assert (
                journal_op("stage", journal, ids[2][1], data=large).decode().strip()
                == h
            )
            journal_op(
                "drain", journal, targets[1], disks[2], ids[2][1], h, expect="no space"
            )
            assert pending() == {item(2, h)}
            assert (
                journal_op(
                    "get", journal, "pending/" + uuid.UUID(ids[2][1]).hex + "/" + h
                )
                == large
            )
            record(
                "full destination refused, pending bytes and destination identity survived reopen",
                sha256=h,
            )
        # Journal absent: both already published segments reopen and verify
        # entirely from the two raw payload devices.
        for i in (1, 2):
            with device(disks[i], nodes) as node:
                assert (
                    invoke("get", node, disks[i], ids[i], "segment/" + hashes[i - 1])
                    == payloads[i - 1]
                )
        record("both destination objects independently readable with journal absent")
    record("raw journal fixture mappings and media removed")


def journal_boundary():
    pool_id = str(uuid.uuid4())
    journal = (pool_id, str(uuid.uuid4()), "journal")
    member = (pool_id, str(uuid.uuid4()), "payload")
    plan = "1,3,3,3,3,3"
    with pool(2) as (disks, nodes):
        with device(disks[0], nodes) as p3, device(disks[1], nodes) as hdd:
            invoke("p3-init-blank", p3, disks[0], (*journal[:2], "p3"), plan)
            invoke("init-blank", hdd, disks[1], member)

            def stage(payload, **kwargs):
                return invoke(
                    "p3-stage",
                    p3,
                    disks[0],
                    journal,
                    plan,
                    member[1],
                    data=payload,
                    **kwargs,
                )

            cache = (*journal[:2], "cache")
            invoke("p3-put-flush", p3, disks[0], cache, plan, "unrelated", data=b"keep")
            # Two labels, two 32 KiB index banks, and one map allowance.
            maximum_payload = (1 << 20) - 2 * (32 << 10) - 3 * 4096
            stage(bytes(maximum_payload + 1), expect="no space")
            value = secrets.token_bytes(maximum_payload)
            h = stage(value).decode().strip()
            assert digest(value) == h
            invoke("p3-drain", p3, disks[0], journal, plan, hdd, disks[1], member[1], h)
            assert invoke("p3-pending", p3, disks[0], journal, plan) == b""
            assert stage(b"new").decode().strip() == digest(b"new")
            assert invoke("p3-get", p3, disks[0], cache, plan, "unrelated") == b"keep"
            assert invoke("get", hdd, disks[1], member, "segment/" + h) == value
            record(
                "full journal drains and reuses freed payload space, cache remains isolated"
            )


if __name__ == "__main__":
    run()
    live_media_loss()
    landing()
    journal_boundary()
