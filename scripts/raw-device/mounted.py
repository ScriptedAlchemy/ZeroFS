"""Qualify production ZeroFS placement through a mounted Linux client."""

from collections import Counter
from contextlib import ExitStack, contextmanager
import hashlib
import json
import os
from pathlib import Path
import random
import stat
import subprocess
import tempfile
import time
from urllib.parse import urlencode
from uuid import uuid4

from fixture import device, pool

ZEROFS = "/input/zerofs"
PROBE = "/input/zerofs-raw-device"
MIB = 1 << 20
CHUNK = 4 * MIB
RAW_BLOCK = 4096  # Raw v1 record header and payload alignment.
SIZES = {"small": 32 * MIB, "large": 96 * MIB}
RESERVES = {"small": 16 * MIB, "large": 8 * MIB}


def record(case, **fields):
    print(json.dumps({"case": case, **fields}, sort_keys=True), flush=True)


def run(*args):
    result = subprocess.run(args, capture_output=True, timeout=60)
    if result.returncode:
        raise RuntimeError(f"{args}: {result.stderr.decode(errors='replace')}")
    return result.stdout


def require_fuse():
    fuse = Path("/dev/fuse")
    try:
        if not fuse.exists():
            os.mknod(fuse, stat.S_IFCHR | 0o600, os.makedev(10, 229))
        mode = fuse.stat()
        if not stat.S_ISCHR(mode.st_mode) or mode.st_rdev != os.makedev(10, 229):
            raise RuntimeError("unexpected FUSE device identity")
        with fuse.open("r+b", buffering=0):
            pass
    except (OSError, RuntimeError) as error:
        record("mount_unavailable", transport="userspace-fuse", error=str(error))
        raise RuntimeError("FUSE device is unavailable in Studio Docker") from error


def wait_for(predicate, process):
    deadline = time.monotonic() + 40
    while not predicate():
        if process.poll() is not None:
            raise RuntimeError(f"{process.args[:2]} exited with {process.returncode}")
        if time.monotonic() >= deadline:
            raise TimeoutError(f"{process.args[:2]} did not become ready")
        time.sleep(0.1)


def stop(process):
    if process.poll() is None:
        process.terminate()
    try:
        code = process.wait(timeout=30)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait(timeout=10)
        raise
    if code:
        raise RuntimeError(f"{process.args[:2]} exited with {code}")


@contextmanager
def mounted(config):
    root = config.parent
    mount = root / "mount"
    endpoint = root / "ninep.sock"
    with (root / "process.log").open("w+b") as log:
        try:
            with ExitStack() as cleanup:
                server = subprocess.Popen([ZEROFS, "run", "-c", config], stdout=log, stderr=log)
                cleanup.callback(stop, server)
                wait_for(endpoint.exists, server)
                client = subprocess.Popen(
                    [ZEROFS, "mount", "--writeback", "false", "--relaxed-consistency", "false",
                     "--reconnect-timeout", "1", f"unix:{endpoint}", mount],
                    stdout=log, stderr=log,
                )
                cleanup.callback(stop, client)
                wait_for(lambda: os.path.ismount(mount), client)
                mountinfo = Path("/proc/self/mountinfo").read_text().splitlines()
                types = [line.split(" - ")[1].split()[0] for line in mountinfo
                         if line.split()[4] == str(mount)]
                assert len(types) == 1 and types[0] in {"fuse", "fuse.zerofs"}, types
                record("mounted", transport="userspace-fuse", filesystem_type=types[0],
                       phase=root.name)
                yield mount / "payload"
            assert not os.path.ismount(mount), mount
        except BaseException:
            log.seek(0)
            print(log.read().decode(errors="replace"), flush=True)
            raise


def write_config(root, authority, catalog, members):
    root.mkdir()
    (root / "mount").mkdir()
    config = root / "zerofs.toml"
    content = f"""[cache]
dir = "{root / 'cache'}"
disk_size_gb = 0.064
memory_size_gb = 0.064
warm_metadata = "off"

[storage]
url = "file://{authority}"
encryption_password = "disposable-raw-mounted-fixture"

[storage.pool]
catalog_dir = "{catalog}"
segment_placement = "round_robin_local"

[servers.ninep]
unix_socket = "{root / 'ninep.sock'}"

[servers.rpc]
unix_socket = "{root / 'rpc.sock'}"

[telemetry]
enabled = false
"""
    for member, (node, backing, pool_id, member_id) in members.items():
        query = urlencode({"backing": str(backing), "pool": pool_id, "member": member_id})
        content += f"""
[[storage.pool.members]]
id = "{member}"
url = "raw+loop://{node}?{query}"
capacity_gb = {SIZES[member] / 1e9:.9f}
reserve_free_gb = {RESERVES[member] / 1e9:.9f}
"""
    config.write_text(content)
    return config


def append(path, digest, index):
    # Independently seeded incompressible chunks make quota pressure predictable.
    body = random.Random(index).randbytes(CHUNK)
    with path.open("ab") as file:
        assert file.write(body) == len(body)
        file.flush()
        os.fsync(file.fileno())
    digest.update(body)


def verify(path, digest, chunks, phase):
    with path.open("rb") as file:
        size = os.fstat(file.fileno()).st_size
        actual = hashlib.file_digest(file, "sha256").hexdigest()
    assert size == chunks * CHUNK and actual == digest.hexdigest(), (size, actual)
    record("mounted_hash", phase=phase, bytes=size, sha256=actual)


def member_status(*args):
    status = json.loads(run(ZEROFS, "pool", "status", *args))
    assert status["pending_operations"] == 0 and not status["unreadable"], status
    members = {member["id"]: member for member in status["members"]}
    assert members.keys() == SIZES.keys(), members
    for member_id, member in members.items():
        assert member["capacity_bytes"] == SIZES[member_id], member
        space = member["space"]
        assert space["filesystem_free_bytes"] is not None, member
        # The pool identity marker is live on the raw member and is outside the
        # segment catalog. It funds a tombstone just like every placed segment.
        assert space["retirement_reserved_bytes"] == RAW_BLOCK * (1 + member["objects"]), member
        assert member["health"]["quarantine"] is None, member
        assert member["health"]["monitoring_error"] is None, member
        assert member["reserved_bytes"] == member["pending_uploads"] == member["cleanup_debts"] == 0, member
    return members


def snapshot(config, members, phase):
    status = member_status("--config", config)
    entries = json.loads(run(ZEROFS, "pool", "list", "--config", config))
    placements = {entry["location"]: entry for entry in entries}
    assert len(placements) == len(entries), entries
    sizes, counts = Counter(), Counter()
    for entry in entries:
        member = entry["member_id"]
        node, backing, pool_id, member_id = members[member]
        body = run(PROBE, "get", node, backing, pool_id, member_id, "payload",
                   entry["physical_key"])
        assert len(body) == entry["size"], entry
        entry["raw_object_sha256"] = hashlib.sha256(body).hexdigest()
        sizes[member] += len(body)
        counts[member] += 1
    allocated = {}
    for member, stats in status.items():
        assert sizes[member] == stats["used_bytes"], stats
        assert counts[member] == stats["objects"], stats
        allocated[member] = members[member][1].stat().st_blocks * 512
    record("catalog_and_raw_objects", phase=phase, status=status, placements=entries,
           backing_allocated_bytes=allocated)
    return status, placements


def qualify(root, members):
    authority = root / "authority"
    authority.mkdir()
    catalog = root / "catalog"
    configs = [write_config(root / phase, authority, catalog, members)
               for phase in ("initial", "fill", "overflow", "cold_restart")]
    initialized = json.loads(run(ZEROFS, "pool", "init", "-c", configs[0]))
    assert initialized["mapped_objects"] == 0, initialized
    digest = hashlib.sha256()
    chunks = 0
    with mounted(configs[0]) as path:
        for _ in range(2):
            append(path, digest, chunks)
            chunks += 1
        verify(path, digest, chunks, "initial")
    _, initial = snapshot(configs[0], members, "initial")
    assert {entry["member_id"] for entry in initial.values()} == SIZES.keys(), initial

    with mounted(configs[1]) as path:
        verify(path, digest, chunks, "first_restart")
        while chunks < 16:
            append(path, digest, chunks)
            chunks += 1
            small = member_status("--endpoint", f"unix:{configs[1].parent / 'rpc.sock'}")["small"]
            if small["available_bytes"] < CHUNK:
                break
        else:
            raise AssertionError("the smaller member never reached its reserve boundary")
    before, at_reserve = snapshot(configs[1], members, "reserve_boundary")
    assert all(at_reserve.get(key) == entry for key, entry in initial.items()), at_reserve
    small = before["small"]
    free = small["space"]["filesystem_free_bytes"]
    assert small["capacity_bytes"] - small["used_bytes"] > CHUNK, small
    retirement = small["space"]["retirement_reserved_bytes"]
    floor = RESERVES["small"] + retirement
    assert floor <= free < floor + CHUNK, small
    assert small["available_bytes"] == free - floor, small

    # Two appends with a barrier each force round-robin to reconsider the
    # smaller member even if the first append would naturally go to large.
    with mounted(configs[2]) as path:
        for _ in range(2):
            append(path, digest, chunks)
            chunks += 1
        verify(path, digest, chunks, "overflow")
    after, final = snapshot(configs[2], members, "after_overflow")
    assert all(final.get(key) == entry for key, entry in at_reserve.items()), final
    new = [entry for key, entry in final.items() if key not in at_reserve]
    assert len(new) >= 2 and all(entry["member_id"] == "large" for entry in new), new
    assert all(small["available_bytes"] < entry["size"] for entry in new), new
    assert after["small"]["used_bytes"] == small["used_bytes"], after
    assert after["small"]["space"] == small["space"], after
    assert after["large"]["used_bytes"] - before["large"]["used_bytes"] == sum(
        entry["size"] for entry in new
    ), after
    physical_spent = (
        before["large"]["space"]["filesystem_free_bytes"]
        - after["large"]["space"]["filesystem_free_bytes"]
    )
    expected_cost = sum(
        RAW_BLOCK * (1 + (entry["size"] + RAW_BLOCK - 1) // RAW_BLOCK) for entry in new
    )
    assert physical_spent == expected_cost, (physical_spent, expected_cost, new)
    retirement_added = (
        after["large"]["space"]["retirement_reserved_bytes"]
        - before["large"]["space"]["retirement_reserved_bytes"]
    )
    assert retirement_added == len(new) * RAW_BLOCK, after
    assert before["large"]["available_bytes"] - after["large"]["available_bytes"] == (
        physical_spent + retirement_added
    ), after
    record("reserve_spill", smaller_reserve_bytes=RESERVES["small"],
           smaller_retirement_reserved_bytes=retirement,
           smaller_available_bytes=small["available_bytes"], larger_new_objects=len(new),
           larger_new_bytes=sum(entry["size"] for entry in new),
           larger_raw_log_bytes=physical_spent,
           larger_retirement_reserved_bytes_added=retirement_added)

    with mounted(configs[3]) as path:
        verify(path, digest, chunks, "cold_restart")
    restarted_status, restarted = snapshot(configs[3], members, "cold_restart")
    assert restarted == final, restarted
    for member in SIZES:
        assert restarted_status[member]["space"] == after[member]["space"], restarted_status


def main():
    flags = Path("/sys/class/net").glob("*/flags")
    active = {flag.parent.name for flag in flags if int(flag.read_text(), 16) & 1}
    if not Path("/.dockerenv").exists() or active != {"lo"}:
        raise RuntimeError("disposable Docker fixture with only loopback networking required")
    os.umask(0o077)
    require_fuse()
    for executable in (ZEROFS, PROBE):
        if not os.access(executable, os.X_OK):
            raise RuntimeError(f"missing parent-built Linux executable at {executable}")
    record("binaries", sha256=run("sha256sum", ZEROFS, PROBE).decode().splitlines())
    with tempfile.TemporaryDirectory(dir="/dev/shm", prefix="zerofs-mounted-") as scratch:
        root = Path(scratch)
        pool_id = str(uuid4())
        with pool(count=2) as (disks, nodes), ExitStack() as opened:
            members = {}
            for (member, size), disk in zip(SIZES.items(), disks, strict=True):
                os.truncate(disk, size)
                node = opened.enter_context(device(disk, nodes))
                member_id = str(uuid4())
                run(PROBE, "init-blank", node, disk, pool_id, member_id, "payload")
                members[member] = (node, disk, pool_id, member_id)
                backing = disk.stat()
                record("initialized_loop", member=member, member_uuid=member_id,
                       pool_uuid=pool_id, size_bytes=size, node=str(node), backing=str(disk),
                       backing_device=backing.st_dev, backing_inode=backing.st_ino)
            qualify(root, members)
        assert all(not disk.exists() for disk in disks) and not nodes.exists()
        record("fixture_cleanup", autocleared_loops=len(disks), backing_files_removed=True)
    assert not root.exists()
    record("mounted_raw_members_pass", scratch_removed=True)


if __name__ == "__main__":
    main()
