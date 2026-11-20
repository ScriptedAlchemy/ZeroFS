"""Count P3 device writes and flushes per acknowledged MiB through a mounted fsync copy.

Runs the production NAS-fit profile on private loops whose backing files live
on a block-backed Docker volume, so every loop flush reaches a real device.
"""

from contextlib import ExitStack
import fcntl
import hashlib
import http.client
import json
import os
from pathlib import Path
import random
import statistics
import subprocess
import tempfile
import threading
import time
from uuid import uuid4

from fixture import device
from large_mounted import (
    P3_LAYOUT, PROBE, SIZES, ZEROFS, allocate, command, config, drop_clean_cache,
    initialize_pool, mount_large, write_all,
)
from mounted import MIB, record, require_fuse, write_fsync_crash

VOLUME = Path("/fixture-volume")
TEMPLATE = Path("/input/memory_raw_nas_fit_fixture.toml")
PAYLOAD = int(os.environ.get("BENCH_PAYLOAD_MIB", "2048")) * MIB
RUNS = int(os.environ.get("BENCH_RUNS", "3"))
# Files written at once. One file's writes pass through one engine lane.
WRITERS = int(os.environ.get("BENCH_WRITERS", "1"))
IO = MIB
SHIM_DELAY_US = os.environ.get("BENCH_FLUSH_DELAY_US")
PERF = os.environ.get("BENCH_PERF") == "1"
# Pace member PUTs so most of the copy is still in the P3 journal when it is read back.
MEMBER_PUT_DELAY_MS = os.environ.get("BENCH_MEMBER_PUT_DELAY_MS")
# Instead of timing copies, SIGKILL the engine after fsync and replay the copy from the journal.
CRASH_REPLAY = os.environ.get("BENCH_CRASH_REPLAY") == "1"
ROLES = ("journal", "catalog", "metadata", "key_envelope", "scratch", "cache")
STAT_FIELDS = (
    "reads", "read_merges", "read_sectors", "read_ms", "writes", "write_merges",
    "write_sectors", "write_ms", "in_flight", "io_ms", "queue_ms", "discards",
    "discard_merges", "discard_sectors", "discard_ms", "flushes", "flush_ms",
)


def disk_stat(node):
    minor = os.minor(node.stat().st_rdev)
    values = map(int, Path(f"/sys/block/loop{minor}/stat").read_text().split())
    return dict(zip(STAT_FIELDS, values))


def disk_stats(bindings):
    return {name: disk_stat(binding[0]) for name, binding in bindings.items()}


def disk_delta(after, before):
    return {name: {key: after[name][key] - before[name][key]
                   for key in STAT_FIELDS if key != "in_flight"} for name in after}


def peak_resident_bytes(pid):
    for line in Path(f"/proc/{pid}/status").read_text().splitlines():
        if line.startswith("VmHWM:"):
            return int(line.split()[1]) * 1024
    raise RuntimeError("engine VmHWM not found")


def cpu_seconds(pid):
    fields = Path(f"/proc/{pid}/stat").read_text().rsplit(")", 1)[1].split()
    return (int(fields[11]) + int(fields[12])) / os.sysconf("SC_CLK_TCK")


def publish_counters(address):
    connection = http.client.HTTPConnection(address, timeout=4)
    try:
        connection.request("GET", "/metrics")
        body = connection.getresponse().read().decode()
    finally:
        connection.close()
    # The phase sums say where the journal's writer spent each batch.
    wanted = ("zerofs_writeback_local_publish_batches_total",
              "zerofs_writeback_local_publish_records_total",
              "zerofs_writeback_local_publish_phase_seconds_sum",
              "zerofs_writeback_local_publish_phase_seconds_count")
    return {line.split()[0]: float(line.split()[1]) for line in body.splitlines()
            if line.split(" ", 1)[0].split("{", 1)[0] in wanted}


class ShimLog:
    """Attribute the shim's P3 writes and syncs to layout regions."""

    def __init__(self, path):
        self.path, self.position = path, 0
        self.bounds, start = [], 0
        for role, mib in zip(ROLES, map(int, P3_LAYOUT.split(","))):
            self.bounds.append((start, start + mib * MIB, role))
            start += mib * MIB

    def region(self, offset):
        return next((start, role) for start, end, role in self.bounds if offset < end)

    def take(self):
        counts = {}
        with self.path.open("rb") as log:
            log.seek(self.position)
            data = log.read()
        complete = data[:data.rfind(b"\n") + 1]
        self.position += len(complete)
        for line in complete.decode().splitlines():
            kind, offset, length = line.split()
            start, role = self.region(int(offset))
            entry = counts.setdefault(role, {"writes": 0, "bytes": 0, "syncs": 0,
                                             "label_writes": 0, "label_syncs": 0})
            label = int(offset) - start < 8192
            if kind == "W":
                entry["writes"] += 1
                entry["bytes"] += int(length)
                entry["label_writes"] += label
            else:
                entry["syncs"] += 1
                entry["label_syncs"] += label
        return counts


def install_shim(root, p3_node):
    if SHIM_DELAY_US is None:
        return None
    library, log = root / "flush_shim.so", root / "flush_shim.log"
    subprocess.run(["gcc", "-shared", "-fPIC", "-O2", "-o", library, "/h/flush_shim.c", "-ldl"],
                   check=True)
    log.touch()
    os.environ.update(LD_PRELOAD=str(library), FLUSH_SHIM_LOG=str(log),
                      FLUSH_SHIM_DELAY_US=SHIM_DELAY_US,
                      FLUSH_SHIM_MINOR=str(os.minor(p3_node.stat().st_rdev)))
    record("flush_shim", added_sync_delay_us=int(SHIM_DELAY_US))
    return ShimLog(log)


def client_pid():
    for entry in Path("/proc").iterdir():
        if entry.name.isdigit():
            try:
                argv = (entry / "cmdline").read_bytes().split(b"\0")
            except OSError:
                continue
            if argv[0] == ZEROFS.encode() and argv[1:2] == [b"mount"]:
                return int(entry.name)
    raise RuntimeError("FUSE client process not found")


def start_perf(root, pids):
    if not PERF:
        return None
    data = root / "perf.data"
    process = subprocess.Popen(["perf", "record", "-q", "-N", "-F", "499", "-o", data,
                                "-p", ",".join(map(str, pids))])
    time.sleep(1)
    return process, data


def stop_perf(handle):
    if handle is None:
        return
    process, data = handle
    process.send_signal(2)
    process.wait(timeout=120)
    report = subprocess.run(["perf", "report", "--stdio", "-q", "--no-children", "-i", data,
                             "--sort", "comm,sym", "--percent-limit", "0.7"],
                            capture_output=True, text=True, timeout=600, check=True).stdout
    data.unlink()
    record("perf_profile", top=[line.strip() for line in report.splitlines() if line.strip()][:60])


def evict(bindings):
    """Drop the loops' and backing files' clean pages; the drain may still be writing."""
    for node, backing, *_ in bindings.values():
        with node.open("r+b", buffering=0) as file:
            os.fdatasync(file.fileno())
            fcntl.ioctl(file.fileno(), 0x1261)  # BLKFLSBUF
        with backing.open("r+b", buffering=0) as file:
            os.fdatasync(file.fileno())
            drop_clean_cache(file.fileno())


def chunks(seed):
    pool = random.Random(seed).randbytes(64 * MIB)
    for index in range(PAYLOAD // IO):
        start = (index * 7919 % 63) * MIB
        yield index.to_bytes(8, "little") + pool[start + 8:start + IO]


def sequential_batches(fd):
    """Time the payload log's pattern: 32 MiB in 4 MiB writes, then one sync."""
    batch, piece = 32 * MIB, os.urandom(4 * MIB)
    samples = []
    for index in range(7):
        start = time.monotonic_ns()
        for offset in range(0, batch, len(piece)):
            os.pwrite(fd, piece, index * batch + offset)
        os.fdatasync(fd)
        samples.append(time.monotonic_ns() - start)
    median = sorted(samples)[len(samples) // 2]
    return {"median_us": median // 1000, "mb_per_s": batch / 1e6 / (median / 1e9),
            "count": len(samples)}


def flush_latency(root, nodes):
    """Time write+fdatasync patterns on scratch loops, before the flush shim is loaded."""
    # The P3 loop's backing file is preallocated and unwritten, as this one is.
    unwritten = root / "flush-probe-unwritten"
    allocate(unwritten, 256 * MIB)
    with device(unwritten, nodes) as node:
        fd = os.open(node, os.O_RDWR)
        try:
            first_write = sequential_batches(fd)
        finally:
            os.close(fd)
    unwritten.unlink()
    backing = root / "flush-probe"
    with backing.open("xb", buffering=0) as file:
        for _ in range(256):
            write_all(file.fileno(), os.urandom(MIB))
        os.fdatasync(file.fileno())
    result = {"32m_sequential_first_write": first_write}
    with device(backing, nodes) as node:
        fd = os.open(node, os.O_RDWR)
        try:
            generator = random.Random(1)
            for label, size, count in (("4k", 4096, 200), ("1m", MIB, 100)):
                block = os.urandom(size)
                samples = []
                for _ in range(count):
                    offset = generator.randrange(0, 256 * MIB - size, 4096)
                    start = time.monotonic_ns()
                    os.pwrite(fd, block, offset)
                    os.fdatasync(fd)
                    samples.append(time.monotonic_ns() - start)
                samples.sort()
                result[label] = {"median_us": samples[len(samples) // 2] // 1000,
                                 "p90_us": samples[len(samples) * 9 // 10] // 1000,
                                 "count": count}
            result["32m_sequential_rewrite"] = sequential_batches(fd)
        finally:
            os.close(fd)
    backing.unlink()
    record("loop_flush_latency", **result)


def write_file(target, seed, done):
    with target.open("xb", buffering=0) as file:
        for chunk in chunks(seed):
            write_all(file.fileno(), chunk)
        written = time.monotonic_ns()
        os.fsync(file.fileno())
    done[target] = (written, time.monotonic_ns())


def run_once(index, bindings, configuration, path, server, address, shim):
    targets = {path.parent / f"bench-{index}-{writer}": index * WRITERS + writer
               for writer in range(WRITERS)}
    digests = {}
    for target, seed in targets.items():
        digest = hashlib.sha256()
        for chunk in chunks(seed):
            digest.update(chunk)
        digests[target] = digest.digest()
    total = PAYLOAD * WRITERS
    client = client_pid()
    before_disk, before_cpu = disk_stats(bindings), cpu_seconds(server.pid)
    before_client = cpu_seconds(client)
    before_publish = publish_counters(address)
    profile = start_perf(path.parent.parent, (server.pid, client)) if index == 0 else None
    if shim:
        shim.take()
    done = {}
    threads = [threading.Thread(target=write_file, args=(target, seed, done))
               for target, seed in targets.items()]
    started = time.monotonic_ns()
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    assert set(done) == set(targets), "a writer failed"
    written = max(times[0] for times in done.values())
    synced = max(times[1] for times in done.values())
    write = disk_delta(disk_stats(bindings), before_disk)
    write_cpu = cpu_seconds(server.pid) - before_cpu
    client_cpu = cpu_seconds(client) - before_client
    stop_perf(profile)
    after_publish = publish_counters(address)
    p3 = write["p3"]
    mib = total / MIB
    seconds = (synced - started) / 1e9
    rate = total / 1e6 / seconds
    record("write_fsync", run=index, bytes=total, writers=WRITERS, seconds=seconds,
           mb_per_s=rate, fsync_seconds=(synced - written) / 1e9,
           engine_cpu_seconds=write_cpu, client_cpu_seconds=client_cpu,
           engine_peak_resident_bytes=peak_resident_bytes(server.pid),
           p3_writes_per_mib=p3["writes"] / mib, p3_flushes_per_mib=p3["flushes"] / mib,
           p3_write_amplification=p3["write_sectors"] * 512 / total,
           p3_mean_write_kib=p3["write_sectors"] / 2 / max(p3["writes"], 1),
           publish={key: after_publish.get(key, 0) - before_publish.get(key, 0)
                    for key in after_publish},
           p3_regions=shim.take() if shim else None, devices=write)

    for target in targets:
        with target.open("rb", buffering=0) as file:
            drop_clean_cache(file.fileno())
    # Read back cold, as an 8 GiB copy is on the NAS: no loop or backing page cache.
    evict(bindings)
    before_disk, before_cpu = disk_stats(bindings), cpu_seconds(server.pid)
    count = 0
    started = time.monotonic_ns()
    for target in targets:
        check = hashlib.sha256()
        with target.open("rb", buffering=0) as file:
            while chunk := file.read(IO):
                check.update(chunk)
                count += len(chunk)
        assert check.digest() == digests[target], target
    seconds = (time.monotonic_ns() - started) / 1e9
    assert count == total, (index, count)
    read = disk_delta(disk_stats(bindings), before_disk)
    record("read_verify", run=index, bytes=count, seconds=seconds,
           mb_per_s=count / 1e6 / seconds,
           p3_read_amplification=read["p3"]["read_sectors"] * 512 / count,
           engine_cpu_seconds=cpu_seconds(server.pid) - before_cpu,
           p3_regions=shim.take() if shim else None, devices=read)

    if MEMBER_PUT_DELAY_MS:
        # A paced drain would take minutes; leave it behind the next copy.
        for target in targets:
            target.unlink()
        return rate
    started = time.monotonic_ns()
    command(ZEROFS, "flush", "-c", configuration, timeout=1800)
    for target in targets:
        target.unlink()
    command(ZEROFS, "flush", "-c", configuration, timeout=1800)
    record("drained", run=index, seconds=(time.monotonic_ns() - started) / 1e9)
    return rate


def crash_replay(bindings, configuration):
    """Kill the engine as fsync returns with the copy still in the P3 journal, then read it back."""
    body = b"".join(chunks(0))
    os.environ["ZEROFS_FIXTURE_MEMBER_PUT_DELAY_MS"] = "5000"
    with mount_large(configuration, crash=True) as (path, server):
        write_fsync_crash(path.parent / "crash-replay", server, body)
    del os.environ["ZEROFS_FIXTURE_MEMBER_PUT_DELAY_MS"]
    # The killed engine left its sockets behind, and the mount waits for them to appear.
    for stale in configuration.parent.glob("*.sock"):
        stale.unlink()
    evict(bindings)
    held = json.loads(command(ZEROFS, "pool", "verify-raw", "-c", configuration, timeout=300))
    journal = held["journal_allocation"]
    assert held["remote_seq"] < held["local_seq"], held
    assert journal["dirty_blob_bytes"] >= PAYLOAD // 2, journal
    with mount_large(configuration) as (path, _):
        check, count = hashlib.sha256(), 0
        with (path.parent / "crash-replay").open("rb", buffering=0) as file:
            while chunk := file.read(IO):
                check.update(chunk)
                count += len(chunk)
        assert count == PAYLOAD and check.digest() == hashlib.sha256(body).digest(), count
        command(ZEROFS, "flush", "-c", configuration, timeout=1800)
    record("crash_replay", bytes=count, sha256_verified=True, local_seq=held["local_seq"],
           remote_seq_at_restart=held["remote_seq"], journal_allocation=journal)


def main():
    assert Path("/.dockerenv").exists() and os.path.ismount(VOLUME)
    os.umask(0o077)
    require_fuse()
    record("bench_setup", payload_bytes=PAYLOAD, runs=RUNS, writers=WRITERS,
           member_put_delay_ms=MEMBER_PUT_DELAY_MS,
           binaries=command("sha256sum", ZEROFS, PROBE).decode().splitlines())
    with tempfile.TemporaryDirectory(dir=VOLUME, prefix="bench-") as scratch, \
         tempfile.TemporaryDirectory(dir="/dev", prefix="zerofs-bench-") as node_dir, \
         tempfile.TemporaryDirectory(dir="/dev/shm", prefix="bench-run-") as run_dir:
        root, nodes, run_root = Path(scratch), Path(node_dir), Path(run_dir)
        flush_latency(root, nodes)
        pool = str(uuid4())
        with ExitStack() as cleanup:
            bindings = {}
            for name, size in SIZES.items():
                backing = root / name
                allocate(backing, size)
                node = cleanup.enter_context(device(backing, nodes))
                bindings[name] = (node, backing, pool, str(uuid4()))
                role = ("p3-v2-init-blank", "p3", P3_LAYOUT) if name == "p3" else (
                    "init-blank", "payload")
                command(PROBE, "fixture", role[0], *bindings[name], *role[1:], timeout=900)
            configuration = config(run_root / "bench", TEMPLATE.read_text(), bindings)
            initialize_pool(configuration, bindings)
            if MEMBER_PUT_DELAY_MS:
                os.environ["ZEROFS_FIXTURE_MEMBER_PUT_DELAY_MS"] = MEMBER_PUT_DELAY_MS
            shim = install_shim(root, bindings["p3"][0])
            if CRASH_REPLAY:
                crash_replay(bindings, configuration)
            else:
                with mount_large(configuration) as (path, server):
                    address = "127.0.0.1:19091"
                    rates = [run_once(index, bindings, configuration, path, server, address, shim)
                             for index in range(RUNS)]
                record("bench_summary", runs=RUNS, payload_bytes=PAYLOAD, writers=WRITERS,
                       write_fsync_mb_per_s_median=statistics.median(rates),
                       write_fsync_mb_per_s_min=min(rates),
                       write_fsync_mb_per_s_max=max(rates))
        assert not any(nodes.iterdir())


if __name__ == "__main__":
    main()
