"""Qualify production ZeroFS placement through a mounted Linux client."""

from collections import Counter
from contextlib import ExitStack, contextmanager
import hashlib
import http.client
import json
import os
from pathlib import Path
import random
import re
import signal
import stat
import subprocess
import tempfile
import time
import tomllib
from urllib.parse import urlencode
from uuid import uuid4

from fixture import device, pool

ZEROFS = "/input/zerofs"
PROBE = "/input/zerofs-raw-device"
MANIFEST = Path("/input/memory_raw_fixture.toml")
CGROUP = Path("/sys/fs/cgroup")
MIB = 1 << 20
CHUNK = 4 * MIB
# Stay below the candidate's 4 MiB seal threshold after framing, so the
# held write does not leave a small trailing segment that either member fits.
NAS_FIT_PAYLOAD = 2 * MIB
RAW_BLOCK = 4096  # Raw v1 record header and payload alignment.
SIZES = {"small": 32 * MIB, "large": 96 * MIB}
RESERVES = {"small": 16 * MIB, "large": 8 * MIB}
P3_LAYOUT = "96,32,88,8,16,16"
P3_SIZE = 256 * MIB


def record(case, **fields):
    print(json.dumps({"case": case, **fields}, sort_keys=True), flush=True)


def run(*args, refuse=None):
    result = subprocess.run(args, capture_output=True, timeout=60)
    if refuse is not None:
        error = result.stderr.decode(errors="replace").strip()
        assert result.returncode and refuse in error, (args, result.returncode, error)
        return error
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
        raise RuntimeError("FUSE device is unavailable in the isolated Docker container") from error


def wait_for(predicate, process):
    deadline = time.monotonic() + 40
    while not predicate():
        if process.poll() is not None:
            raise RuntimeError(f"{process.args[:2]} exited with {process.returncode}")
        if time.monotonic() >= deadline:
            raise TimeoutError(f"{process.args[:2]} did not become ready")
        time.sleep(0.1)


def stop(process, expected=0):
    if process.poll() is None:
        process.terminate()
    try:
        code = process.wait(timeout=30)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait(timeout=10)
        raise
    if code != expected:
        raise RuntimeError(f"{process.args[:2]} exited with {code}, expected {expected}")


@contextmanager
def mounted(config, *, crash=False):
    root = config.parent
    mount = root / "mount"
    endpoint = root / "ninep.sock"
    with (root / "process.log").open("w+b") as log:
        try:
            with ExitStack() as cleanup:
                server = subprocess.Popen([ZEROFS, "run", "-c", config], stdout=log, stderr=log)
                cleanup.callback(stop, server, -signal.SIGKILL if crash else 0)
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
                yield mount / "payload", server
            assert not any(line.split()[4] == str(mount)
                           for line in Path("/proc/self/mountinfo").read_text().splitlines()), mount
            record("unmounted", phase=root.name)
        except BaseException:
            log.seek(0)
            print(log.read().decode(errors="replace"), flush=True)
            raise


def raw_url(scheme, binding, **extra):
    node, backing, pool_id, member_id = binding
    query = urlencode({"backing": str(backing), "pool": pool_id, "member": member_id, **extra})
    return f"{scheme}://{node}?{query}"


def write_config(root, authority_url, catalog, members):
    root.mkdir()
    (root / "mount").mkdir()
    config = root / "zerofs.toml"
    catalog_setting = f'catalog_dir = "{catalog}"\n' if catalog is not None else ""
    disk_size = 0 if catalog is None else 0.064
    content = f"""[cache]
dir = "{root / 'cache'}"
disk_size_gb = {disk_size}
memory_size_gb = 0.064
warm_metadata = "off"

[storage]
url = "{authority_url}"
encryption_password = "disposable-raw-mounted-fixture"

[storage.pool]
{catalog_setting}segment_placement = "round_robin_local"

[servers.ninep]
unix_socket = "{root / 'ninep.sock'}"

[servers.rpc]
unix_socket = "{root / 'rpc.sock'}"

[telemetry]
enabled = false
"""
    for member, binding in members.items():
        content += f"""
[[storage.pool.members]]
id = "{member}"
url = "{raw_url('raw+loop', binding)}"
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


def verify(path, digest, expected_size, phase):
    with path.open("rb") as file:
        size = os.fstat(file.fileno()).st_size
        actual = hashlib.file_digest(file, "sha256").hexdigest()
    assert size == expected_size and actual == digest.hexdigest(), (size, actual)
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


def check_member_bindings(members):
    # The kernel inode and the label UUID are independent requirements.
    node, backing, pool_id, member_id = members["small"]
    _, other_backing, _, other_member = members["large"]
    error = run(PROBE, "get", node, other_backing, pool_id, member_id, "payload", "unused",
                refuse="loop backing identity mismatch")
    record("rejected_loop_binding", member="small", substituted="backing inode", error=error)
    error = run(PROBE, "get", node, backing, pool_id, other_member, "payload", "unused",
                refuse="device label binding mismatch (member)")
    record("rejected_loop_binding", member="small", substituted="member UUID", error=error)


def inspect_p3(binding, phase):
    lines = run(PROBE, "p3-v2-inspect", *binding, "p3", P3_LAYOUT).decode().splitlines()
    stats = {key: json.loads(value) for key, value in (line.split("=", 1) for line in lines)}
    record("p3_regions", phase=phase, **stats)
    return stats


def check_admission(config, memory_limit):
    admission = json.loads(run(ZEROFS, "memory-status", "-c", config))
    assert admission and admission["hard_limit_bytes"] == memory_limit, admission
    assert 0 < admission["required_bytes"] < memory_limit, admission
    record("memory_admission", phase=config.parent.name, **admission)
    return admission


def check_p3_shadows(root, configs):
    shadows = sorted(str(path.relative_to(root)) for path in root.rglob("*")
                     if path.name in {"pool.redb", "pool.lock", "zerofs.key"}
                     or path.name.startswith("bucket_"))
    for config in configs:
        settings = tomllib.loads(config.read_text())
        if "writeback" in settings:
            journal = Path(settings["writeback"]["dir"])
            if journal.exists():
                shadows.append(str(journal))
    assert not shadows and not (root / "authority").exists(), shadows
    record("p3_no_directory_shadows", catalog=True, authority=True, key_envelope=True,
           cache=True, journal=True)


def sample_cgroup(phase):
    resources = {}
    for name in ("memory.current", "memory.peak", "memory.events", "memory.swap.current",
                 "memory.swap.max", "memory.max", "cpu.stat", "cpu.max"):
        value = (CGROUP / name).read_text().strip()
        resources[name] = int(value) if value.isdigit() else value
    record("cgroup_sample", phase=phase, **resources)
    events = dict(line.split() for line in resources["memory.events"].splitlines())
    assert resources["memory.swap.current"] == resources["memory.swap.max"] == 0, resources
    assert not any(int(events[key]) for key in ("oom", "oom_kill")), resources


def render_nas_config(root, template, members, p3, *, held):
    root.mkdir()
    (root / "mount").mkdir()
    manifest = tomllib.loads(template)
    replacements = {
        manifest["cache"]["dir"]: str(root / "unused-clean-cache"),
        manifest["writeback"]["dir"]: str(root / "unused-directory-journal"),
        manifest["storage"]["url"]: raw_url("raw+p3-v2", p3, layout_mib=P3_LAYOUT),
        manifest["servers"]["ninep"]["unix_socket"]: str(root / "ninep.sock"),
        manifest["servers"]["rpc"]["unix_socket"]: str(root / "rpc.sock"),
    }
    for member in manifest["storage"]["pool"]["members"]:
        member_id = member["id"]
        normal = RESERVES[member_id]
        assert round(member["capacity_gb"] * 1e9) == SIZES[member_id], member
        assert round(member["reserve_free_gb"] * 1e9) == normal, member
        replacements[member["url"]] = raw_url("raw+loop", members[member_id])
        if held:
            replacements[f"reserve_free_gb = {normal / 1e9:.9f}"] = (
                f"reserve_free_gb = {(SIZES[member_id] - 256 * 1024) / 1e9:.9f}"
            )
    for original, replacement in replacements.items():
        assert template.count(original) == 1, original
        template = template.replace(original, replacement, 1)
    rendered = root / "zerofs.toml"
    rendered.write_text(template)
    record("runtime_manifest", phase=root.name, held_reserves=held, toml=template)
    return rendered


def report_webui(config):
    [address] = tomllib.loads(config.read_text())["servers"]["webui"]["addresses"]
    host, port = address.split(":")
    assert host == "127.0.0.1", address

    def fetch(path):
        connection = http.client.HTTPConnection(host, int(port), timeout=3)
        try:
            connection.request("GET", path)
            response = connection.getresponse()
            payload = response.read()
            return {"status": response.status, "content_type": response.getheader("Content-Type"),
                    "bytes": len(payload), "sha256": hashlib.sha256(payload).hexdigest()}, payload
        finally:
            connection.close()

    deadline = time.monotonic() + 5
    while True:
        try:
            index, body = fetch("/")
            break
        except ConnectionRefusedError:
            if time.monotonic() >= deadline:
                raise
            time.sleep(0.1)
    references = set(re.findall(rb'(?:src|href)=["\'](/assets/[^"\']+)["\']', body))
    assets = {path.decode(): fetch(path.decode())[0] for path in sorted(references)}
    served = index["status"] == 200 and any(
        asset["status"] == 200 and "javascript" in (asset["content_type"] or "")
        and asset["bytes"] > 0 for asset in assets.values()
    )
    record("webui_assets", index=index, assets=assets, javascript_served=served)


def write_fsync_crash(path, server, body):
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        start = time.monotonic_ns()
        unwritten = memoryview(body)
        while unwritten:
            written = os.write(fd, unwritten)
            assert written > 0, written
            unwritten = unwritten[written:]
        written_at = time.monotonic_ns()
        os.fsync(fd)
        synced_at = time.monotonic_ns()
        server.kill()
        signalled_at = time.monotonic_ns()
        assert server.wait(timeout=10) == -signal.SIGKILL
    finally:
        os.close(fd)
    record("mounted_fsync_sigkill", bytes=len(body), write_ns=written_at - start,
           fsync_ns=synced_at - written_at, fsync_to_signal_ns=signalled_at - synced_at,
           signal=signal.SIGKILL)


def qualify_nas_fit(root, members, memory_limit, p3):
    template = MANIFEST.read_text()
    record("manifest_source", sha256=hashlib.sha256(MANIFEST.read_bytes()).hexdigest())
    phases = ("initialize", "held_write", "held_replay", "destage", "destaged_cold")
    configs = [render_nas_config(root / phase, template, members, p3,
                                 held=phase in {"held_write", "held_replay"})
               for phase in phases]
    initial, held, replay, destage, cold = configs
    for config in (initial, held):
        admission = check_admission(config, memory_limit)
        assert "memory_policy=raw-device-p3-candidate-v1" in admission["accounting"], admission
    blank = inspect_p3(p3, "blank")
    assert blank["journal_bytes"] == blank["catalog_bytes"] == 0, blank
    initialized = json.loads(run(ZEROFS, "pool", "init", "-c", initial))
    assert initialized["pool_id"] == p3[2] and initialized["mapped_objects"] == 0, initialized
    before, placements = snapshot(held, members, "held_before_write")
    assert not placements and all(0 < m["available_bytes"] < NAS_FIT_PAYLOAD for m in before.values()), before

    body = random.Random(0).randbytes(NAS_FIT_PAYLOAD)
    digest = hashlib.sha256(body)
    with mounted(held, crash=True) as (path, server):
        report_webui(held)
        sample_cgroup("before_volatile_write")
        write_fsync_crash(path, server, body)
        sample_cgroup("after_sigkill")
    crashed = inspect_p3(p3, "after_sigkill")
    assert crashed["journal_bytes"] > 0, crashed
    after, placements = snapshot(held, members, "after_sigkill")
    assert not placements and all(after[key]["space"] == m["space"]
                                  for key, m in before.items()), after

    with mounted(replay) as (path, _):
        start = time.monotonic_ns()
        verify(path, digest, len(body), "held_cold_replay")
        record("mounted_replay_read_timing", read_verify_ns=time.monotonic_ns() - start)
        sample_cgroup("held_cold_replay")
    replayed = inspect_p3(p3, "held_cold_replay")
    assert replayed["journal_bytes"] > 0, replayed
    _, placements = snapshot(replay, members, "held_cold_replay")
    assert not placements, placements

    start = time.monotonic_ns()
    with mounted(destage) as (path, _):
        ready = time.monotonic_ns()
        run(ZEROFS, "flush", "-c", destage)
        drained = time.monotonic_ns()
        record("mounted_destage_timing", startup_ns=ready - start, drain_barrier_ns=drained - ready,
               total_ns=drained - start)
        verify(path, digest, len(body), "after_destage")
        sample_cgroup("after_destage")
    status, placed = snapshot(destage, members, "after_destage")
    assert placed and sum(entry["size"] for entry in placed.values()) >= len(body), placed
    for key, member in status.items():
        entries = [entry for entry in placed.values() if entry["member_id"] == key]
        physical_spent = (
            before[key]["space"]["filesystem_free_bytes"]
            - member["space"]["filesystem_free_bytes"]
        )
        expected = sum(RAW_BLOCK * (1 + (entry["size"] + RAW_BLOCK - 1) // RAW_BLOCK)
                       for entry in entries)
        assert physical_spent == expected, (key, physical_spent, expected)
    with mounted(cold) as (path, _):
        verify(path, digest, len(body), "destaged_cold_restart")
        sample_cgroup("destaged_cold_restart")
    cold_status, restarted = snapshot(cold, members, "destaged_cold_restart")
    assert restarted == placed, restarted
    assert all(cold_status[key]["space"] == member["space"] for key, member in status.items())
    roles = inspect_p3(p3, "destaged_cold_restart")
    assert roles["catalog_bytes"] > 0 and roles["authority_objects"] > 0, roles
    assert roles["keys_objects"] == 1 and roles["envelope_in_keys"], roles
    assert not roles["envelope_in_authority"] and roles["cache_objects"] == 0, roles
    check_p3_shadows(root, configs)


def qualify(root, members, memory_limit, p3):
    authority = root / "authority"
    catalog = None
    if p3 is None:
        authority.mkdir()
        authority_url = f"file://{authority}"
        catalog = root / "catalog"
    else:
        authority_url = raw_url("raw+p3-v2", p3, layout_mib=P3_LAYOUT)
        blank = inspect_p3(p3, "blank")
        assert blank["catalog_bytes"] == blank["journal_bytes"] == 0, blank
        assert blank["keys_objects"] == blank["authority_objects"] == 0, blank
    configs = [write_config(root / phase, authority_url, catalog, members)
               for phase in ("initial", "fill", "overflow", "cold_restart")]
    check_admission(configs[0], memory_limit)
    initialized = json.loads(run(ZEROFS, "pool", "init", "-c", configs[0]))
    assert initialized["mapped_objects"] == 0, initialized
    if p3 is not None:
        assert initialized["pool_id"] == p3[2], initialized
        initialized_roles = inspect_p3(p3, "pool_initialized")
        assert initialized_roles["catalog_bytes"] > 0, initialized_roles
    digest = hashlib.sha256()
    chunks = 0
    with mounted(configs[0]) as (path, _):
        for _ in range(2):
            append(path, digest, chunks)
            chunks += 1
        verify(path, digest, chunks * CHUNK, "initial")
    _, initial = snapshot(configs[0], members, "initial")
    assert {entry["member_id"] for entry in initial.values()} == SIZES.keys(), initial

    with mounted(configs[1]) as (path, _):
        verify(path, digest, chunks * CHUNK, "first_restart")
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
    with mounted(configs[2]) as (path, _):
        for _ in range(2):
            append(path, digest, chunks)
            chunks += 1
        verify(path, digest, chunks * CHUNK, "overflow")
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

    with mounted(configs[3]) as (path, _):
        verify(path, digest, chunks * CHUNK, "cold_restart")
    restarted_status, restarted = snapshot(configs[3], members, "cold_restart")
    assert restarted == final, restarted
    for member in SIZES:
        assert restarted_status[member]["space"] == after[member]["space"], restarted_status
    if p3 is not None:
        roles = inspect_p3(p3, "cold_restart")
        assert roles["catalog_bytes"] > 0 and roles["authority_objects"] > 0, roles
        assert roles["keys_objects"] == 1 and roles["envelope_in_keys"], roles
        assert not roles["envelope_in_authority"] and roles["journal_bytes"] == 0, roles
        check_p3_shadows(root, configs)


def main():
    flags = Path("/sys/class/net").glob("*/flags")
    active = {flag.parent.name for flag in flags if int(flag.read_text(), 16) & 1}
    if not Path("/.dockerenv").exists() or active != {"lo"}:
        raise RuntimeError("disposable Docker fixture with only loopback networking required")
    os.umask(0o077)
    require_fuse()
    expected_memory = int(os.environ["RAW_MOUNTED_MEMORY_BYTES"])
    limits = {name: int((CGROUP / name).read_text()) for name in ("memory.max", "memory.swap.max")}
    assert limits == {"memory.max": expected_memory, "memory.swap.max": 0}, limits
    record("container_limits", **limits)
    for executable in (ZEROFS, PROBE):
        if not os.access(executable, os.X_OK):
            raise RuntimeError(f"missing parent-built Linux executable at {executable}")
    record("binaries", sha256=run("sha256sum", ZEROFS, PROBE).decode().splitlines())
    mode = os.environ["RAW_MOUNTED_MODE"]
    assert mode in {"placement", "p3", "nas-fit"}, mode
    nas_fit = mode == "nas-fit"
    if nas_fit:
        sample_cgroup("start")
    with tempfile.TemporaryDirectory(dir="/dev/shm", prefix="zerofs-mounted-") as scratch:
        root = Path(scratch)
        temp = root / "tmp"
        temp.mkdir()
        os.environ["TMPDIR"] = str(temp)
        pool_id = str(uuid4())
        sizes = SIZES | ({"p3": P3_SIZE} if mode != "placement" else {})
        with pool(count=len(sizes)) as (disks, nodes), ExitStack() as opened:
            members = {}
            for (member, size), disk in zip(sizes.items(), disks, strict=True):
                os.truncate(disk, size)
                node = opened.enter_context(device(disk, nodes))
                member_id = str(uuid4())
                binding = (node, disk, pool_id, member_id)
                if member == "p3":
                    run(PROBE, "p3-v2-init-blank", *binding, "p3", P3_LAYOUT)
                else:
                    run(PROBE, "init-blank", *binding, "payload")
                members[member] = binding
                backing = disk.stat()
                block_id = node.stat().st_rdev
                record("initialized_loop", member=member, member_uuid=member_id,
                       pool_uuid=pool_id, size_bytes=size, node=str(node), backing=str(disk),
                       backing_device=backing.st_dev, backing_inode=backing.st_ino,
                       loop_major=os.major(block_id), loop_minor=os.minor(block_id))
            p3 = members.pop("p3", None)
            check_member_bindings(members)
            if nas_fit:
                qualify_nas_fit(root, members, expected_memory, p3)
            else:
                qualify(root, members, expected_memory, p3)
        assert all(not disk.exists() for disk in disks) and not nodes.exists()
        record("fixture_cleanup", autocleared_loops=len(disks), backing_files_removed=True)
    assert not root.exists()
    if nas_fit:
        sample_cgroup("cleaned")
    record("mounted_raw_members_pass", mode=mode, scratch_removed=True)


if __name__ == "__main__":
    main()
