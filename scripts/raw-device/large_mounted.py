"""Copy, crash-replay and drain one incompressible 8 GiB mounted file on Spark."""

from bisect import bisect_left
from contextlib import ExitStack, contextmanager
from functools import partial
import errno
import fcntl
import hashlib
import http.client
import json
import mmap
import os
from pathlib import Path
import random
import shutil
import signal
import stat
import struct
import subprocess
from threading import Event, Thread
import tempfile
import time
import tomllib
from uuid import uuid4

from fixture import device
from mounted import (
    CGROUP, MIB, PROBE, RAW_BLOCK, ZEROFS, check_admission, check_p3_shadows,
    mounted, raw_url, record, require_fuse, sample_cgroup,
)

GIB = 1024 * MIB
PAYLOAD = 8 * GIB
IO = MIB
MEMORY_LIMIT = 8 * GIB
COMPANION = 3 * GIB // 2
PUT_DELAY_MS = 200
P3_LAYOUT = "61440,128,3072,16,32,848"
JOURNAL_BUDGET = 10 * GIB
JOURNAL_HEADERS = 2 * RAW_BLOCK
JOURNAL_PHYSICAL_RESERVE = 4 * GIB
JOURNAL_RESIZE_CEILING = int(P3_LAYOUT.split(',')[0]) * MIB - JOURNAL_HEADERS - JOURNAL_PHYSICAL_RESERVE
SIZES = {"p3": 64 * GIB, "small": 12 * GIB, "large": 16 * GIB}
RESERVE = 256 * MIB
WRITEBACK = "zerofs_writeback_"
METRICS = (
    "enabled", "status_collection_error", "terminal_error", "dirty_ssd_reserved_bytes",
    "dirty_ssd_capacity_bytes", "dirty_ram_bytes", "dirty_ram_capacity_bytes",
    "accepted_sequence", "local_sequence", "remote_sequence",
    "local_bytes_completed_total", "remote_bytes_completed_total",
    "ssd_admission_paced", "ssd_outstanding_physical_claim_bytes", "ssd_available_bytes",
)
TEMPLATE = Path("/input/memory_raw_nas_fit_fixture.toml")
VOLUME = Path("/fixture-volume")
mount_large = partial(mounted, startup_timeout=300)


def command(*args, timeout=120):
    result = subprocess.run(args, capture_output=True, timeout=timeout, check=False)
    if result.returncode:
        raise RuntimeError(f"{args[:2]} returned {result.returncode}: "
                           f"{result.stderr.decode(errors='replace')}")
    return result.stdout


def keyed_lines(path):
    return {k: int(v) for k, v in map(str.split, path.read_text().splitlines())}


def allocated_bytes(path):
    return path.stat().st_blocks * 512


def free_bytes(path):
    fs = os.statvfs(path)
    return fs.f_frsize * fs.f_bavail


def allocate(path, size):
    with path.open("xb", buffering=0) as file:
        os.posix_fallocate(file.fileno(), 0, size)
        os.fdatasync(file.fileno())
    assert path.stat().st_size == size and allocated_bytes(path) >= size, path
    record("preallocated", file=str(path), size_bytes=size, allocated_bytes=allocated_bytes(path),
           remaining_host_free_bytes=free_bytes(path.parent))


def drop_clean_cache(fd, offset=0, length=0):
    os.posix_fadvise(fd, offset, length, os.POSIX_FADV_DONTNEED)


def evict_media(bindings, phase):
    # Flush and invalidate only the exact private loops and their backing inodes.
    for node, backing, *_ in bindings.values():
        with node.open("r+b", buffering=0) as file:
            os.fdatasync(file.fileno())
            fcntl.ioctl(file.fileno(), 0x1261)  # BLKFLSBUF
        with backing.open("r+b", buffering=0) as file:
            os.fdatasync(file.fileno())
            drop_clean_cache(file.fileno())
    if phase == "after_provisioning":
        return
    resident = {}
    for member, (_, backing, *_) in bindings.items():
        cached, size = map(int, command("fincore", "--bytes", "--noheadings",
                                        "--output", "RES,SIZE", backing).split())
        assert size == SIZES[member] and cached <= 16 * MIB, (member, cached, size)
        resident[member] = cached
    record("owned_media_cache_advice", phase=phase, backing_resident_bytes=resident,
           advice="BLKFLSBUF and POSIX_FADV_DONTNEED")


def write_all(fd, data):
    pending = memoryview(data)
    while pending:
        count = os.write(fd, pending)
        assert count > 0
        pending = pending[count:]


@contextmanager
def watchdog(stage, seconds=120):
    """Bound time between successful I/O operations."""
    assert signal.alarm(0) == 0

    def expired(_signum, _frame):
        raise TimeoutError(f"{stage}: no completed I/O within {seconds} seconds")

    previous = signal.signal(signal.SIGALRM, expired)

    def progress():
        signal.alarm(seconds)

    try:
        progress()
        yield progress
    finally:
        signal.alarm(0)
        signal.signal(signal.SIGALRM, previous)


def source_file(path, seed):
    if not path.exists():
        allocate(path, PAYLOAD)
    digest = hashlib.sha256()
    with path.open("r+b", buffering=0) as file:
        generator = random.Random(seed)
        fd = file.fileno()
        for offset in range(0, PAYLOAD, IO):
            chunk = generator.randbytes(IO)
            digest.update(chunk)
            write_all(fd, chunk)
            if (offset + IO) % (64 * MIB) == 0:
                os.fdatasync(fd)
                drop_clean_cache(fd, offset + IO - 64 * MIB, 64 * MIB)
    record("source", bytes=PAYLOAD, seed=seed, sha256=digest.hexdigest())
    return digest.hexdigest()


def copy_file(source, destination, *, crash=None):
    started = time.monotonic_ns()
    longest = 0
    slow_writes = []
    with watchdog(f"copy {destination.name}") as progress, source.open("rb", buffering=0) as src, \
         destination.open("xb", buffering=0) as dst:
        for position in range(0, PAYLOAD, IO):
            data = src.read(IO)
            assert len(data) == IO
            before = time.monotonic_ns()
            write_all(dst.fileno(), data)
            after = time.monotonic_ns()
            progress()
            longest = max(longest, after - before)
            if after - before >= 100_000_000:
                slow_writes.append((before, after, position))
            drop_clean_cache(src.fileno(), position, IO)
            written = position + IO
            if written % GIB == 0:
                record("copy_progress", file=destination.name, bytes=written,
                       elapsed_ns=time.monotonic_ns() - started)
        assert os.fstat(dst.fileno()).st_size == PAYLOAD
        write_done = time.monotonic_ns()
        os.fsync(dst.fileno())
        fsync_done = time.monotonic_ns()
        if crash is not None:
            crash.send_signal(signal.SIGKILL)
            killed_at = time.monotonic_ns()
            assert crash.wait(timeout=10) == -signal.SIGKILL
    record("copy_fsync", file=destination.name, bytes=PAYLOAD, write_ns=write_done - started,
           fsync_ns=fsync_done - write_done, max_write_ns=longest,
           fsync_to_sigkill_ns=None if crash is None else killed_at - fsync_done)
    return slow_writes


def verify_mounted(path, expected, phase):
    digest = hashlib.sha256()
    start = time.monotonic_ns()
    count = 0
    with watchdog(phase) as progress, path.open("rb", buffering=0) as file:
        while chunk := file.read(IO):
            digest.update(chunk)
            count += len(chunk)
            progress()
    assert count == PAYLOAD and digest.hexdigest() == expected, (count, digest.hexdigest())
    record("mounted_hash", phase=phase, bytes=count, sha256=expected,
           read_and_verify_ns=time.monotonic_ns() - start)


def config(root, template, devices, *, held=False):
    root.mkdir()
    (root / "mount").mkdir()
    parsed = tomllib.loads(template)
    assert int(parsed["writeback"]["min_free_gb"] * 1e9) == JOURNAL_PHYSICAL_RESERVE
    replacements = {
        parsed["cache"]["dir"]: str(root / "unused-clean-cache"),
        parsed["writeback"]["dir"]: str(root / "unused-directory-journal"),
        parsed["storage"]["url"]: raw_url("raw+p3-fixture", devices["p3"], layout_mib=P3_LAYOUT),
        parsed["servers"]["ninep"]["unix_socket"]: str(root / "ninep.sock"),
        parsed["servers"]["rpc"]["unix_socket"]: str(root / "rpc.sock"),
    }
    for original, replacement in replacements.items():
        assert template.count(original) == 1, original
        template = template.replace(original, replacement, 1)
    for member in parsed["storage"]["pool"]["members"]:
        member_id = member["id"]
        assert round(member["capacity_gb"] * 1e9) == SIZES[member_id], member
        assert round(member["reserve_free_gb"] * 1e9) == RESERVE, member
        original = f'url = "{member["url"]}"\ncapacity_gb = {SIZES[member_id] / 1e9:.9f}\n'
        original += f'reserve_free_gb = {RESERVE / 1e9:.9f}'
        wanted = SIZES[member_id] if held else RESERVE
        assert template.count(original) == 1, original
        replacement = f'url = "{raw_url("raw+fixture-loop", devices[member_id])}"\n'
        replacement += f'capacity_gb = {SIZES[member_id] / 1e9:.9f}\nreserve_free_gb = {wanted / 1e9:.9f}'
        template = template.replace(original, replacement, 1)
    if "prometheus" not in parsed:
        template += '\n[prometheus]\naddresses = ["127.0.0.1:19091"]\n'
    parsed = tomllib.loads(template)
    [address] = parsed["prometheus"]["addresses"]
    assert address.startswith("127.0.0.1:"), address
    path = root / "zerofs.toml"
    path.write_text(template)
    record("runtime_manifest", phase=root.name, headroom_held=held, toml=template)
    return path


def fetch_metrics(address):
    connection = http.client.HTTPConnection(address, timeout=4)
    try:
        connection.request("GET", "/metrics")
        response = connection.getresponse()
        assert response.status == 200, response.status
        result = {}
        for line in response.read().decode().splitlines():
            if line.startswith(WRITEBACK):
                name, value = line.split()
                key = name.removeprefix(WRITEBACK)
                if key in METRICS:
                    result[key] = int(float(value))
        return result
    finally:
        connection.close()


@contextmanager
def monitor(configuration, server):
    [address] = tomllib.loads(configuration.read_text())["prometheus"]["addresses"]
    phase = configuration.parent.name
    samples = []
    stopped = Event()

    def sample():
        next_report = time.monotonic() + 30
        while not stopped.is_set():
            try:
                metrics = fetch_metrics(address)
                if metrics.keys() == set(METRICS):
                    assert metrics["enabled"] == 1
                    assert metrics["status_collection_error"] == metrics["terminal_error"] == 0
                    assert 0 <= metrics["dirty_ssd_reserved_bytes"] <= metrics["dirty_ssd_capacity_bytes"]
                    assert 0 <= metrics["dirty_ram_bytes"] <= metrics["dirty_ram_capacity_bytes"]
                    memory = keyed_lines(CGROUP / "memory.stat")
                    proc_lines = Path(f"/proc/{server.pid}/status").read_text().splitlines()
                    hwm = next(int(line.split()[1]) * 1024 for line in proc_lines
                               if line.startswith("VmHWM:"))
                    samples.append((time.monotonic_ns(), metrics, hwm, memory["anon"], memory["file"]))
                    if time.monotonic() >= next_report:
                        record("mounted_runtime_sample", phase=phase, metrics=metrics, server_vm_hwm_bytes=hwm,
                               cgroup_anon_bytes=memory["anon"], cgroup_file_bytes=memory["file"])
                        next_report = time.monotonic() + 30
            except OSError as error:
                if server.poll() is None and error.errno not in (
                    None, errno.ENOENT, errno.ECONNREFUSED, errno.ECONNRESET,
                ):
                    samples.append(error)
                    return
            except (AssertionError, ValueError) as error:
                samples.append(error)
                return
            stopped.wait(0.1)

    thread = Thread(target=sample)
    thread.start()
    try:
        yield address, samples
    finally:
        stopped.set()
        thread.join()
        for entry in samples:
            if isinstance(entry, Exception):
                raise entry
        assert samples, phase
        peak = {key: max(s[1][key] for s in samples) for key in METRICS}
        record("mounted_runtime_peak", phase=phase, samples=len(samples), metrics=peak,
               first=samples[0][1], final=samples[-1][1],
               server_vm_hwm_bytes=max(s[2] for s in samples),
               cgroup_anon_sample_peak_bytes=max(s[3] for s in samples),
               cgroup_file_sample_peak_bytes=max(s[4] for s in samples))


def remote_barrier(configuration, address, expected_bytes):
    started = last_progress = time.monotonic_ns()
    remote_bytes = 0
    remote_sequence = 0
    record("remote_drain_start", phase=configuration.parent.name, metrics=fetch_metrics(address))
    with tempfile.TemporaryFile() as log, subprocess.Popen(
        [ZEROFS, "flush", "-c", configuration], stdout=log, stderr=log
    ) as process:
        try:
            while True:
                metrics = fetch_metrics(address)
                now = time.monotonic_ns()
                completed = metrics.get("remote_bytes_completed_total", 0)
                sequence = metrics.get("remote_sequence", 0)
                if completed > remote_bytes or sequence > remote_sequence:
                    remote_bytes, remote_sequence = completed, sequence
                    last_progress = now
                code = process.poll()
                if code:
                    log.seek(0)
                    raise RuntimeError(f"remote barrier returned {code}: {log.read().decode(errors='replace')}")
                if (code == 0 and metrics.keys() == set(METRICS) and completed >= expected_bytes
                        and metrics["accepted_sequence"] == metrics["local_sequence"] == sequence
                        and metrics["dirty_ssd_reserved_bytes"] == metrics["dirty_ram_bytes"]
                        == metrics["ssd_outstanding_physical_claim_bytes"] == 0):
                    record("remote_drain", phase=configuration.parent.name,
                           elapsed_ns=now - started, metrics=metrics)
                    return metrics
                if now - last_progress > 120_000_000_000 or now - started > 1_800_000_000_000:
                    raise TimeoutError(f"remote drain exceeded its progress deadline: {metrics}")
                time.sleep(0.2)
        finally:
            if process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    process.kill()


def inspect_p3(binding, phase):
    lines = command(PROBE, "fixture", "p3-v2-inspect", *binding, "p3", P3_LAYOUT)
    state = {key: json.loads(value) for key, value in
             (line.split("=", 1) for line in lines.decode().splitlines())}
    assert state["journal_bytes"] <= JOURNAL_RESIZE_CEILING, state
    assert state["envelope_in_keys"] and not state["envelope_in_authority"]
    record("p3_regions", phase=phase, **state, journal_resize_ceiling_bytes=JOURNAL_RESIZE_CEILING,
           journal_bytes_before_resize_ceiling=JOURNAL_RESIZE_CEILING - state["journal_bytes"])
    return state


def snapshot(configuration, phase):
    status = json.loads(command(ZEROFS, "pool", "status", "--config", configuration))
    assert status["pending_operations"] == 0 and not status["unreadable"], status
    members = {member["id"]: member for member in status["members"]}
    assert members.keys() == {"small", "large"}, members
    entries = json.loads(command(ZEROFS, "pool", "list", "--config", configuration))
    by_object = {entry["location"]: entry for entry in entries}
    assert len(by_object) == len(entries), phase
    for name, member in members.items():
        selected = [entry for entry in entries if entry["member_id"] == name]
        space = member["space"]
        assert member["capacity_bytes"] == SIZES[name], member
        assert member["objects"] == len(selected), member
        assert member["used_bytes"] == sum(entry["size"] for entry in selected), member
        assert space["retirement_reserved_bytes"] == RAW_BLOCK * (1 + len(selected)), member
        assert member["reserved_bytes"] == member["pending_uploads"] == member["cleanup_debts"] == 0, member
        assert member["health"]["quarantine"] is None and member["health"]["monitoring_error"] is None, member
    record("pool_accounting", phase=phase, members=members, placed_objects=len(entries))
    return members, by_object


def verify_members(devices, baseline, final, entries):
    for name in ("small", "large"):
        placed = {entry["physical_key"]: entry["size"] for entry in entries.values()
                  if entry["member_id"] == name}
        assert placed and len(placed) == final[name]["objects"], name
        lines = command(PROBE, "fixture", "verify", *devices[name], "payload", timeout=600)
        seen = set()
        digest = hashlib.sha256()
        outside_catalog = []
        for line in sorted(lines.decode().splitlines()):
            size, sha256, encoded = line.split()
            key = bytes.fromhex(encoded).decode()
            assert key not in seen and len(bytes.fromhex(sha256)) == 32, key
            seen.add(key)
            digest.update(line.encode() + b"\n")
            if key in placed:
                assert placed[key] == int(size), key
            else:
                outside_catalog.append(key)
        assert placed.keys() <= seen and len(outside_catalog) == 1, (name, outside_catalog)
        spent = (baseline[name]["space"]["filesystem_free_bytes"]
                 - final[name]["space"]["filesystem_free_bytes"])
        expected = sum(RAW_BLOCK * (1 + (size + RAW_BLOCK - 1) // RAW_BLOCK)
                       for size in placed.values())
        assert spent == expected, (name, spent, expected)
        record("member_verified", member=name, bytes=sum(placed.values()), objects=len(placed),
               raw_bytes_spent=spent, record_manifest_sha256=digest.hexdigest(),
               non_catalog_key=outside_catalog[0])


def check_readiness(configuration, pool, phase, *, drained=False, held=False):
    result = json.loads(command(ZEROFS, "pool", "verify-raw", "-c", configuration, timeout=300))
    assert result["pool_id"] == pool and result["profile"] == "raw-device-p3-nas-fit-v1", result
    assert all(result[f"{key}_verified"] for key in ("authority", "catalog", "key_envelope", "journal")), result
    assert result["memory"]["included_companion_bytes"] == COMPANION, result
    assert result["memory"]["hard_limit_bytes"] == MEMORY_LIMIT, result
    allocation = result["journal_allocation"]
    assert allocation["region_data_capacity_bytes"] == JOURNAL_RESIZE_CEILING + JOURNAL_PHYSICAL_RESERVE
    assert allocation["resize_ceiling_bytes"] == JOURNAL_RESIZE_CEILING
    assert allocation["physical_reserve_bytes"] == JOURNAL_PHYSICAL_RESERVE
    allocated_pages_bytes = allocation["allocated_pages"] * allocation["page_size_bytes"]
    assert 0 < allocated_pages_bytes <= allocation["database_length_bytes"] <= JOURNAL_RESIZE_CEILING
    if held:
        assert 0 < result["remote_seq"] < result["local_seq"], result
        assert PAYLOAD <= allocation["dirty_blob_bytes"] <= allocation["stored_key_value_bytes"]
        assert allocation["dirty_blob_bytes"] + allocation["dirty_metadata_reserved_bytes"] <= JOURNAL_BUDGET
    if drained:
        assert 0 < result["local_seq"] == result["remote_seq"], result
        assert allocation["dirty_blob_bytes"] == allocation["dirty_metadata_reserved_bytes"] == 0
    record("offline_readiness", phase=phase, allocated_pages_bytes=allocated_pages_bytes, **result)


@contextmanager
def exclusive_fixture_loop(binding):
    node, backing, *_ = binding
    metadata = backing.stat()
    fd = os.open(node, os.O_RDWR | os.O_EXCL | os.O_NOFOLLOW | os.O_NONBLOCK)
    try:
        device_metadata = os.fstat(fd)
        assert stat.S_ISBLK(device_metadata.st_mode) and os.major(device_metadata.st_rdev) == 7
        status = bytearray(232)
        fcntl.ioctl(fd, 0x4C05, status, True)
        assert struct.unpack_from("=QQ", status) == (metadata.st_dev, metadata.st_ino)
        assert struct.unpack_from("=QQ", status, 24) == (0, 0)
        size = bytearray(8)
        fcntl.ioctl(fd, 0x80081272, size, True)  # BLKGETSIZE64
        assert struct.unpack("=Q", size)[0] == metadata.st_size == SIZES["p3"]
        yield fd
    finally:
        os.close(fd)


def verify_missing_journal(configuration, binding, empty_headers):
    metadata_before = inspect_p3(binding, "before_journal_loss")
    with exclusive_fixture_loop(binding) as fd:
        original = os.pread(fd, JOURNAL_HEADERS, 0)
        assert len(original) == len(empty_headers) == JOURNAL_HEADERS and original != empty_headers
        assert os.pwrite(fd, empty_headers, 0) == JOURNAL_HEADERS
        os.fsync(fd)
    try:
        missing = inspect_p3(binding, "after_injected_journal_loss")
        assert missing["journal_bytes"] == 0
        assert {k: v for k, v in missing.items() if k != "journal_bytes"} == {
            k: v for k, v in metadata_before.items() if k != "journal_bytes"
        }
        [address] = tomllib.loads(configuration.read_text())["prometheus"]["addresses"]
        for mode in (("pool", "verify-raw"), ("run",)):
            result = subprocess.run([ZEROFS, *mode, "-c", configuration], capture_output=True, timeout=30)
            output = (result.stdout + result.stderr).decode(errors="replace")
            assert result.returncode and "original raw P3 journal is missing" in output, (mode, output)
            assert not (configuration.parent / "ninep.sock").exists()
            assert not (configuration.parent / "rpc.sock").exists()
            record("missing_original_journal_refused", command=mode, metrics_endpoint=address, output=output)
        with exclusive_fixture_loop(binding) as fd:
            assert os.pread(fd, JOURNAL_HEADERS, 0) == empty_headers
        assert inspect_p3(binding, "after_refusal") == missing
        record("missing_original_journal_unchanged", original_headers_sha256=hashlib.sha256(original).hexdigest(),
               empty_headers_sha256=hashlib.sha256(empty_headers).hexdigest())
    finally:
        # Put both authoritative headers back on the same device after this negative check.
        with exclusive_fixture_loop(binding) as fd:
            assert os.pwrite(fd, original, 0) == JOURNAL_HEADERS
            os.fsync(fd)
    check_readiness(configuration, binding[2], "after_restoring_journal_headers", drained=True)


def qualify(root, devices, source, empty_headers):
    template = TEMPLATE.read_text()
    record("manifest_source", sha256=hashlib.sha256(template.encode()).hexdigest())
    configs = {phase: config(root / phase, template, devices, held=phase.startswith("held"))
               for phase in ("held_copy", "held_replay", "destage", "cold", "missing_journal")}
    for phase in ("held_copy", "destage"):
        admission = check_admission(configs[phase], MEMORY_LIMIT)
        assert "memory_policy=raw-device-p3-nas-fit-v1" in admission["accounting"], admission
    initialized = json.loads(command(ZEROFS, "pool", "init", "-c", configs["destage"]))
    assert initialized["pool_id"] == devices["p3"][2] and initialized["mapped_objects"] == 0
    check_readiness(configs["destage"], devices["p3"][2], "explicit_pool_init")
    baseline, entries = snapshot(configs["held_copy"], "empty_catalog")
    assert not entries and all(m["available_bytes"] == 0 for m in baseline.values())

    first_hash = source_file(source, seed=0)
    with mount_large(configs["held_copy"], crash=True) as (path, server), monitor(configs["held_copy"], server):
        copy_file(source, path, crash=server)
        sample_cgroup("held_copy_killed")
    held = inspect_p3(devices["p3"], "after_held_crash")
    assert PAYLOAD <= held["journal_bytes"], held
    post_crash, after_entries = snapshot(configs["held_replay"], "after_held_crash")
    assert not after_entries and all(post_crash[m]["space"] == baseline[m]["space"] for m in baseline)

    evict_media(devices, "held_restart")
    with mount_large(configs["held_replay"]) as (path, server), monitor(configs["held_replay"], server):
        verify_mounted(path, first_hash, "held_restart")
        sample_cgroup("held_restart")
    post_replay, replayed_entries = snapshot(configs["held_replay"], "after_held_restart")
    assert not replayed_entries
    assert all(post_replay[m]["space"] == baseline[m]["space"] for m in baseline)
    check_readiness(configs["held_replay"], devices["p3"][2], "after_held_restart", held=True)

    second_hash = source_file(source, seed=1)
    assert second_hash != first_hash
    delay_var = "ZEROFS_FIXTURE_MEMBER_PUT_DELAY_MS"
    assert delay_var not in os.environ
    os.environ[delay_var] = str(PUT_DELAY_MS)
    try:
        with mount_large(configs["destage"]) as (path, server), monitor(configs["destage"], server) as (address, samples):
            record("synthetic_member_put_pacing", delay_ms_per_put=PUT_DELAY_MS)
            slow_writes = copy_file(source, path.with_name("payload-second"))
            drained = remote_barrier(configs["destage"], address, 2 * PAYLOAD)
            assert drained["dirty_ssd_capacity_bytes"] == JOURNAL_BUDGET
    finally:
        del os.environ[delay_var]
    paced = [sample for sample in samples if sample[1]["ssd_admission_paced"] == 1]
    paced_times = [sample[0] for sample in paced]
    stalled = []
    for before, after, position in slow_writes:
        index = bisect_left(paced_times, before)
        if index < len(paced_times) and paced_times[index] <= after:
            stalled.append((after - before, position))
    assert stalled, (len(slow_writes), len(paced))
    stall_ns, offset = max(stalled)
    record("paced_foreground_progress", completed_slow_writes=len(stalled),
           max_write_ns=stall_ns, max_write_offset_bytes=offset, paced_samples=len(paced),
           paced_sample_span_ns=paced[-1][0] - paced[0][0], journal_budget_bytes=JOURNAL_BUDGET,
           max_journal_reserved_bytes=max(sample[1]["dirty_ssd_reserved_bytes"] for sample in paced),
           durable_remote_bytes=drained["remote_bytes_completed_total"])

    final, placements = snapshot(configs["destage"], "fully_drained")
    assert sum(entry["size"] for entry in placements.values()) >= 2 * PAYLOAD
    verify_members(devices, baseline, final, placements)
    inspect_p3(devices["p3"], "fully_drained")
    evict_media(devices, "destaged_restart")
    with mount_large(configs["cold"]) as (path, server), monitor(configs["cold"], server):
        verify_mounted(path, first_hash, "destaged_restart_first")
        verify_mounted(path.with_name("payload-second"), second_hash, "destaged_restart_second")
    check_readiness(configs["cold"], devices["p3"][2], "after_destage_and_cold_restart", drained=True)
    verify_missing_journal(configs["missing_journal"], devices["p3"], empty_headers)
    check_p3_shadows(root, list(configs.values()))


def main():
    active = {path.parent.name for path in Path("/sys/class/net").glob("*/flags")
              if int(path.read_text(), 16) & 1}
    assert Path("/.dockerenv").exists() and active == {"lo"}
    assert int((CGROUP / "memory.max").read_text()) == MEMORY_LIMIT
    assert int((CGROUP / "memory.swap.max").read_text()) == 0
    assert os.path.ismount(VOLUME) and not any(VOLUME.iterdir())
    required = PAYLOAD + sum(SIZES.values())
    free = free_bytes(VOLUME)
    assert free >= required + 4 * GIB, (free, required)
    os.umask(0o077)
    require_fuse()
    record("fixture_limits", cgroup_bytes=MEMORY_LIMIT, app_companion_allowance_bytes=COMPANION,
           swap_bytes=0, media_and_source_preallocation_bytes=required, host_free_bytes=free,
           binaries=command("sha256sum", ZEROFS, PROBE).decode().splitlines())
    start_events = keyed_lines(CGROUP / "memory.events")
    sample_cgroup("start")
    with mmap.mmap(-1, COMPANION, flags=mmap.MAP_PRIVATE | mmap.MAP_ANONYMOUS) as companion:
        ones = b"\x01" * MIB
        for offset in range(0, COMPANION, MIB):
            companion[offset:offset + MIB] = ones
        assert keyed_lines(CGROUP / "memory.stat")["anon"] >= COMPANION
        sample_cgroup("companion_resident")
        run_fixture(start_events)


def run_fixture(start_events):
    with tempfile.TemporaryDirectory(dir=VOLUME, prefix="fixture-") as scratch, \
         tempfile.TemporaryDirectory(dir="/dev", prefix="zerofs-large-") as node_dir:
        root, nodes = Path(scratch), Path(node_dir)
        run_root = Path(tempfile.mkdtemp(dir="/dev/shm", prefix="raw-large-"))
        source = root / "source"
        try:
            pool = str(uuid4())
            with ExitStack() as cleanup:
                bindings = {}
                for name, size in SIZES.items():
                    backing = root / name
                    allocate(backing, size)
                    node = cleanup.enter_context(device(backing, nodes))
                    bindings[name] = (node, backing, pool, str(uuid4()))
                    record("owned_loop", member=name, node=str(node), backing=str(backing),
                           pool_uuid=pool, member_uuid=bindings[name][3], size_bytes=size,
                           backing_inode=backing.stat().st_ino, backing_device=backing.stat().st_dev)
                    if name == "p3":
                        command(PROBE, "fixture", "p3-v2-init-blank", *bindings[name], "p3",
                                P3_LAYOUT, timeout=900)
                    else:
                        command(PROBE, "fixture", "init-blank", *bindings[name], "payload", timeout=900)
                evict_media(bindings, "after_provisioning")
                with exclusive_fixture_loop(bindings["p3"]) as fd:
                    empty_headers = os.pread(fd, JOURNAL_HEADERS, 0)
                qualify(run_root, bindings, source, empty_headers)
                for name, (_, backing, *_) in bindings.items():
                    assert allocated_bytes(backing) >= SIZES[name]
            assert not any(nodes.iterdir()), nodes
        finally:
            # Attach process logs to the receipt on errors through mounted() and remove them.
            shutil.rmtree(run_root)
    assert not root.exists() and not nodes.exists() and not run_root.exists()
    assert not any(VOLUME.iterdir())
    peak = int((CGROUP / "memory.peak").read_text())
    assert peak > 0, peak
    sample_cgroup("cleaned")
    end_events = keyed_lines(CGROUP / "memory.events")
    record("large_raw_pass", scratch=str(root), nodes=str(nodes), runtime=str(run_root),
           exact_paths_removed=True, owned_loop_mappings_autocleared=len(SIZES),
           cgroup_peak_bytes=peak, cgroup_max_bytes=MEMORY_LIMIT,
           cgroup_headroom_at_peak_bytes=max(0, MEMORY_LIMIT - peak),
           cgroup_peak_over_max_bytes=max(0, peak - MEMORY_LIMIT),
           cgroup_memory_events_delta={k: end_events[k] - start_events[k] for k in end_events})


if __name__ == "__main__":
    main()
