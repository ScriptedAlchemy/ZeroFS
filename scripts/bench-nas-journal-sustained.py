#!/usr/bin/env python3
"""Sustained large-copy throughput for the bounded NAS journal profiles.

Run inside a disposable container whose cgroup supplies the memory and CPU
envelope. Pass the same dedicated, empty ext4 --scratch mount (for example an
owned Docker volume at /scratch) as both member parents. The fixture writes
incompressible data over pipelined 9P2000.L WRITEs from a stream it hashes
before timing starts, then issues fsync. It reports elapsed time from the first WRITE to the
last WRITE reply, to the last fsync, and to an observed fully drained remote
sequence. It samples writeback gauges, engine and cgroup CPU, memory and the
backing block device. After drain it SIGKILLs the engine, parks the journal,
removes the cache and verifies every SHA-256 through a fresh read-only engine.

With --crash-after-fsync the engine is SIGKILLed immediately after the last
fsync, before drain. A restart on the same journal must serve every hash and
drain, then the journal-free read-only verifier runs as above.
"""

from concurrent.futures import ThreadPoolExecutor
import hashlib
import http.client
import importlib
import os
from pathlib import Path
import shutil
import struct
import sys
import threading
import time


sys.dont_write_bytecode = True
fixture = importlib.import_module("test-nas-journal")
drain = importlib.import_module("test-nas-journal-file-drain")
NineP = fixture.NineP
MIB = fixture.MIB
require = fixture.require


def block_device(path):
    dev = path.stat().st_dev
    name = Path(f"/sys/dev/block/{os.major(dev)}:{os.minor(dev)}").resolve()
    # A partition's parent directory holds the whole-disk statistics.
    disk = name.parent if (name / "partition").exists() else name
    return name.name, disk


def disk_written_bytes(disk):
    try:
        fields = (disk / "stat").read_text().split()
    except OSError:
        return 0
    return int(fields[6]) * 512


def validate_scratch(args):
    """The scratch parent is one dedicated, initially empty ext4 mount.

    Both member parents equal it; the fixture creates separate owned children.
    A Docker named volume or a bind of a private directory both qualify.
    """
    scratch = args.scratch
    require(args.member_mounts == (scratch, scratch), "both member parents must equal --scratch")
    found = None
    for line in Path("/proc/self/mountinfo").read_text().splitlines():
        before, after = line.split(" - ", 1)
        if Path(before.split()[4]) == scratch:
            found = after.split()[0]
    require(found == args.scratch_fs, f"scratch must be a dedicated {args.scratch_fs} mount, found {found}")
    require(not any(scratch.iterdir()), f"scratch mount is not empty: {scratch}")


class Pattern:
    """Deterministic incompressible WRITE payloads drawn from one random pool.

    Each block is a slice of a 256 MiB os.urandom pool at a per-file, per-block
    offset. LZ4 compresses each 32 KiB extent independently, so repeats far
    apart cannot shrink, and the engine has no content deduplication. The whole
    stream is hashed before timing so the timed loop only sends bytes.
    """

    POOL = 256 * MIB

    def __init__(self, pool, file_index, size, chunk):
        self.pool = pool
        self.size = size
        self.chunk = chunk
        self.file_index = file_index

    def block(self, offset):
        length = min(self.chunk, self.size - offset)
        index = offset // self.chunk
        # 4 KiB-aligned start, odd multiplier so successive blocks scatter.
        start = ((index * 2654435761 + self.file_index * 40503) * 4096) % (
            self.POOL - self.chunk
        )
        start -= start % 4096
        return memoryview(self.pool)[start : start + length]

    def digests(self, per_block):
        digest = hashlib.sha256()
        blocks = {}
        for offset in range(0, self.size, self.chunk):
            block = self.block(offset)
            digest.update(block)
            if per_block:
                blocks[offset] = (len(block), hashlib.sha256(block).digest())
        return digest.hexdigest(), blocks


class Progress:
    """Per-block digests and the contiguous acknowledged prefix of one file."""

    def __init__(self):
        self.blocks = {}
        self.prefix = 0
        self.done = False


class PipelinedClient(NineP):
    def write_stream(self, pattern, depth, remaining, progress=None):
        """Keep up to `depth` WRITEs outstanding."""
        size = pattern.size
        outstanding = {}
        offset = 0
        acknowledged = 0
        tag = 0
        while True:
            while len(outstanding) < depth and offset < size:
                block = pattern.block(offset)
                tag = tag % 60000 + 1
                header = struct.pack(
                    "<IBH", 7 + 16 + len(block), 118, tag
                ) + struct.pack("<IQI", 2, offset, len(block))
                sent = self.socket.sendmsg([header, block])
                if sent < len(header):
                    self.socket.sendall(header[sent:])
                if sent < len(header) + len(block):
                    self.socket.sendall(block[max(0, sent - len(header)) :])
                outstanding[tag] = (offset, len(block))
                offset += len(block)
            if not outstanding:
                break
            remaining()
            length, kind, reply_tag = struct.unpack("<IBH", self.receive(7))
            reply = self.receive(length - 7)
            entry = outstanding.pop(reply_tag, None)
            require(entry is not None, f"unexpected 9P reply tag {reply_tag}")
            expected = entry[1]
            if kind == 7:
                (errno,) = struct.unpack("<I", reply)
                raise OSError(errno, f"9P WRITE: {os.strerror(errno)}")
            require(kind == 119, f"unexpected 9P WRITE reply {kind}")
            (count,) = struct.unpack("<I", reply)
            require(count == expected, f"short pipelined WRITE {count}/{expected}")
            acknowledged += count
            if progress is not None:
                progress.prefix = min(
                    (start for start, _ in outstanding.values()), default=offset
                )
        require(acknowledged == size, f"acknowledged {acknowledged} of {size} bytes")


class SustainedFixture(drain.FileDrainFixture):
    profiles = frozenset({"small-nas-journal-v1", "small-nas-journal-v2"})

    def cache_disk_writes(self):
        """Clean-cache bytes written to its disk tier, summed over caches."""
        connection = http.client.HTTPConnection(self.metrics_address, timeout=2)
        try:
            connection.request("GET", "/metrics")
            body = connection.getresponse().read().decode()
        except OSError:
            return None
        finally:
            connection.close()
        total = 0
        for line in body.splitlines():
            if line.startswith("zerofs_cache_disk_write_bytes_total{"):
                total += int(float(line.rsplit(" ", 1)[1]))
        return total

    def sample(self, phase):
        row = {"elapsed_seconds": time.monotonic() - self.start, "phase": phase}
        metrics = self.metric_snapshot()
        if metrics:
            metrics.pop("volatile_overlay", None)
            row.update(metrics)
            row["cache_disk_write_bytes"] = self.cache_disk_writes()
        if self.server is not None and self.server.poll() is None:
            stat = (
                Path(f"/proc/{self.server.pid}/stat")
                .read_text()
                .rsplit(")", 1)[1]
                .split()
            )
            row["engine_cpu_seconds"] = (int(stat[11]) + int(stat[12])) / os.sysconf(
                "SC_CLK_TCK"
            )
            row["engine_rss_bytes"] = int(stat[21]) * os.sysconf("SC_PAGE_SIZE")
            io = dict(
                line.split(": ")
                for line in Path(f"/proc/{self.server.pid}/io").read_text().splitlines()
            )
            row["engine_read_bytes"] = int(io["read_bytes"])
            row["engine_write_bytes"] = int(io["write_bytes"])
            row["engine_cancelled_write_bytes"] = int(io["cancelled_write_bytes"])
        cgroup = Path("/sys/fs/cgroup")
        for key in ("memory.current", "memory.peak"):
            row[key] = int((cgroup / key).read_text())
        for line in (cgroup / "memory.stat").read_text().splitlines():
            key, value = line.split()
            if key in ("anon", "file", "file_dirty", "file_writeback"):
                row[f"memory_{key}"] = int(value)
        row["cpu_stat"] = dict(
            (name, int(value))
            for name, value in (
                line.split() for line in (cgroup / "cpu.stat").read_text().splitlines()
            )
        )
        row["device_written_bytes"] = disk_written_bytes(self.disk)
        row["client_cpu_seconds"] = time.process_time()
        return row

    def sampler(self, samples, stop, phase):
        while not stop.wait(0.25):
            try:
                samples.append(self.sample(phase[0]))
            except Exception as error:  # surfaced after the workload
                samples.append({"sampler_error": repr(error)})
                return

    def write_one(self, name, size):
        with PipelinedClient(
            self.ninep, self.remaining, self.args.msize_kib * 1024
        ) as client:
            client.create(name.encode())
            if self.args.write_kib:
                require(self.args.write_kib * 1024 <= client.io_chunk, "write size exceeds the negotiated I/O unit")
                client.io_chunk = self.args.write_kib * 1024
            progress = self.progress.get(name)
            pattern = Pattern(self.pool, int(name.rsplit("-", 1)[1]), size, client.io_chunk)
            digest, blocks = pattern.digests(progress is not None)
            if progress is not None:
                progress.blocks = blocks
            # Every client starts timing together, after hashing its stream.
            self.start_barrier.wait(timeout=self.remaining())
            started = time.monotonic() - self.start
            client.write_stream(pattern, self.args.depth, self.remaining, progress)
            if progress is not None:
                progress.done = True
            acknowledged = time.monotonic() - self.start
            self.barrier.wait(timeout=self.remaining())
            fsync_started = time.monotonic() - self.start
            client.fsync()
            fsynced = time.monotonic() - self.start
            client.clunk()
        return {
            "file": name,
            "bytes": size,
            "sha256": digest,
            "write_started_seconds": started,
            "write_acknowledged_seconds": acknowledged,
            "fsync_started_seconds": fsync_started,
            "fsync_completed_seconds": fsynced,
        }

    def verify(self, files, label):
        with NineP(self.ninep, self.remaining, 1024 * 1024) as client:
            for row in files:
                started = time.monotonic()
                observed = client.read_hash(row["file"].encode(), row["bytes"])
                require(observed == row["sha256"], f"{label}: digest mismatch for {row['file']}")
                self.event(
                    verify=label,
                    file=row["file"],
                    sha256=observed,
                    read_seconds=time.monotonic() - started,
                )

    def wait_for_prefix(self, progress):
        while progress.prefix == 0 and not progress.done:
            self.remaining()
            time.sleep(0.05)

    def tail_reader(self, name, progress, stop, outcome):
        """Re-read recently acknowledged blocks while they drain and retire."""
        window = self.args.tail_reader_mib * MIB
        try:
            with NineP(self.ninep, self.remaining, 1024 * 1024) as client:
                client.walk(name.encode())
                client.call(12, struct.pack("<II", 2, 0))
                while not stop.is_set():
                    prefix = progress.prefix
                    blocks = dict(list(progress.blocks.items()))
                    starts = sorted(o for o, (n, _) in blocks.items() if prefix - window <= o and o + n <= prefix)
                    if not starts:
                        time.sleep(0.05)
                        continue
                    for start in starts:
                        length, expected = blocks[start]
                        data = bytearray()
                        while len(data) < length:
                            reply = client.call(116, struct.pack("<IQI", 2, start + len(data), length - len(data)))
                            (count,) = struct.unpack_from("<I", reply)
                            require(count > 0, f"{name}: short read at {start + len(data)}")
                            data.extend(reply[4 : 4 + count])
                        outcome["blocks"] += 1
                        if hashlib.sha256(data).digest() != expected:
                            outcome["errors"].append(f"{name}: block {start} content mismatch")
                            return
                        if stop.is_set():
                            break
                    outcome["passes"] += 1
                    if progress.done and self.drained(self.metric_snapshot()):
                        return
        except Exception as error:
            outcome["errors"].append(f"{type(error).__name__}: {error}")

    def start_readback(self, files):
        """Read every file back through a new session while the journal drains."""
        outcome = {"passes": [], "errors": []}

        def run():
            try:
                while True:
                    started = time.monotonic() - self.start
                    with NineP(self.ninep, self.remaining, 1024 * 1024) as client:
                        for row in files:
                            observed = client.read_hash(row["file"].encode(), row["bytes"])
                            if observed != row["sha256"]:
                                outcome["errors"].append(f"digest mismatch for {row['file']}")
                    rows = self.metric_snapshot()
                    outcome["passes"].append({
                        "started_seconds": started,
                        "finished_seconds": time.monotonic() - self.start,
                        "drained_at_finish": self.drained(rows),
                    })
                    if self.drained(rows) or outcome["errors"]:
                        return
            except Exception as error:
                outcome["errors"].append(f"{type(error).__name__}: {error}")

        thread = threading.Thread(target=run, daemon=True)
        thread.start()
        return thread, outcome

    def run(self):
        self.device_partition, self.disk = block_device(self.args.scratch)
        self.receipt.update(
            scope="userspace 9P2000.L, journal and two members on one ext4 device",
            depth=self.args.depth,
            write_kib=self.args.write_kib,
            msize_kib=self.args.msize_kib,
            block_device=self.disk.name,
            crash_after_fsync=self.args.crash_after_fsync,
            readback_during_drain=self.args.readback_during_drain,
            tail_reader_mib=self.args.tail_reader_mib,
        )
        prepared = self.prepare()
        warm = prepared.initial_writeback
        size = self.args.file_mib * MIB
        names = [f"sustained-{index:02d}" for index in range(self.args.files)]
        samples = []
        phase = ["write"]
        stop = threading.Event()
        self.barrier = threading.Barrier(len(names))
        self.start_barrier = threading.Barrier(
            len(names), action=lambda: self.start_samples.append(self.sample("start"))
        )
        self.start_samples = []
        self.pool = os.urandom(Pattern.POOL)
        self.progress = {}
        tail_threads = []
        tail_outcome = {"blocks": 0, "passes": 0, "errors": []}
        tail_stop = threading.Event()
        if self.args.tail_reader_mib:
            for name in names:
                self.progress[name] = Progress()
        self.stage = "sustained-write"
        self.sample("baseline")
        thread = threading.Thread(target=self.sampler, args=(samples, stop, phase), daemon=True)
        thread.start()
        for name, progress in self.progress.items():
            # The file must exist before the reader walks to it.
            reader = threading.Thread(
                target=lambda name=name, progress=progress: (
                    self.wait_for_prefix(progress),
                    self.tail_reader(name, progress, tail_stop, tail_outcome),
                ),
                daemon=True,
            )
            reader.start()
            tail_threads.append(reader)
        try:
            with ThreadPoolExecutor(max_workers=len(names)) as pool:
                futures = [pool.submit(self.write_one, name, size) for name in names]
                try:
                    files = [future.result(timeout=self.remaining()) for future in futures]
                except BaseException:
                    self.barrier.abort()
                    self.start_barrier.abort()
                    raise
            phase[0] = "drain"
            at_fsync = self.sample("fsync")
            first = min(row["write_started_seconds"] for row in files)
            last_write = max(row["write_acknowledged_seconds"] for row in files)
            last_sync = max(row["fsync_completed_seconds"] for row in files)
            if self.args.crash_after_fsync:
                cut = self.sample("crash")
                self.stop_server(crash=True)
                self.event(crash_after_fsync=True, writeback=cut, exit_signal="SIGKILL")
                self.stage = "replay-same-journal"
                stop.set()
                thread.join()
                self.start_server(self.config)
                self.verify(files, "replayed-before-drain")
                drained = self.wait(
                    lambda: rows if self.drained(rows := self.metric_snapshot()) else None
                )
                drained_at = None
            else:
                readback = None
                if self.args.readback_during_drain:
                    readback = self.start_readback(files)
                drained = self.wait(
                    lambda: rows
                    if self.drained(rows := self.metric_snapshot())
                    and rows["remote_sequence"] > warm["remote_sequence"]
                    else None
                )
                drained_at = time.monotonic() - self.start
                if readback is not None:
                    readback_thread, outcome = readback
                    readback_thread.join(timeout=self.remaining())
                    self.receipt["readback_during_drain"] = outcome
                    require(not outcome.get("errors"), f"readback during drain failed: {outcome['errors']}")
                stop.set()
                thread.join()
        finally:
            stop.set()
        tail_stop.set()
        for reader in tail_threads:
            reader.join(timeout=30)
        if tail_threads:
            self.receipt["tail_reader"] = tail_outcome
            require(not tail_outcome["errors"], f"tail reader failed: {tail_outcome['errors']}")
        errors = [row for row in samples if "sampler_error" in row]
        require(not errors, f"sampler failed: {errors[:1]}")
        logical = size * len(names)
        transfer = {
            "logical_bytes": logical,
            "write_seconds": last_write - first,
            "through_fsync_seconds": last_sync - first,
            "write_mb_per_second": logical / (last_write - first) / 1e6,
            "through_fsync_mb_per_second": logical / (last_sync - first) / 1e6,
        }
        if drained_at is not None:
            transfer["through_drain_seconds"] = drained_at - first
            transfer["through_drain_mb_per_second"] = logical / (drained_at - first) / 1e6
            self.record_middle_window(warm, [row for row in samples if "remote_bytes_completed_total" in row], drained)
        end = self.sample("end")
        baseline = self.start_samples[0]

        def phase_deltas(after):
            ssd = (
                after["engine_write_bytes"]
                - baseline["engine_write_bytes"]
                - (after["engine_cancelled_write_bytes"] - baseline["engine_cancelled_write_bytes"])
            )
            members = after["remote_bytes_completed_total"] - baseline["remote_bytes_completed_total"]
            cache = (after.get("cache_disk_write_bytes") or 0) - (baseline.get("cache_disk_write_bytes") or 0)
            return {
                "engine_cpu_seconds": after["engine_cpu_seconds"] - baseline["engine_cpu_seconds"],
                "client_cpu_seconds": after["client_cpu_seconds"] - baseline["client_cpu_seconds"],
                "cgroup_usage_seconds": (after["cpu_stat"]["usage_usec"] - baseline["cpu_stat"]["usage_usec"]) / 1e6,
                "cgroup_throttled_seconds": (after["cpu_stat"]["throttled_usec"] - baseline["cpu_stat"]["throttled_usec"]) / 1e6,
                "device_written_bytes": after["device_written_bytes"] - baseline["device_written_bytes"],
                "engine_net_write_bytes": ssd,
                "member_bytes": members,
                # On the NAS layout every engine write except members lands on the SSD.
                "non_member_write_bytes": ssd - members,
                "non_member_write_amplification": (ssd - members) / logical,
                "clean_cache_disk_write_bytes": cache,
                "clean_cache_write_amplification": cache / logical,
                "journal_and_metadata_write_amplification": (ssd - members - cache) / logical,
                "member_write_amplification": members / logical,
                "dirty_ssd_reserved_bytes": after.get("dirty_ssd_reserved_bytes"),
            }

        transfer["through_fsync"] = phase_deltas(at_fsync)
        # A crash run restarts the engine, so its process counters reset.
        transfer["through_end"] = None if self.args.crash_after_fsync else phase_deltas(end)
        transfer["memory_peak_bytes"] = end["memory.peak"]
        transfer["anon_peak_bytes"] = max(row.get("memory_anon", 0) for row in samples + [end])
        transfer["paced_before_fsync"] = any(
            row.get("ssd_admission_paced") for row in samples if row.get("phase") == "write"
        )
        self.receipt["transfer"] = transfer
        self.receipt["baseline_sample"] = baseline
        self.receipt["fsync_sample"] = at_fsync
        self.receipt["samples"] = samples
        self.event(transfer=transfer, files=files, writeback=drained)

        self.cut_after_remote_drain()
        self.stage = "offline-destination"
        leftovers = fixture.journal_payloads(self.journal)
        require(not leftovers, f"journal payload remains after drain: {len(leftovers)}")
        self.receipt["final_members"] = self.pool_status(offline=True)

        self.stage = "verify-fresh-cache-read-only"
        parked = self.root / "parked-journal"
        self.journal.rename(parked)
        shutil.rmtree(self.cache)
        self.start_server(prepared.verify_config, read_only=True)
        self.verify(files, "journal-free-read-only")
        self.stop_server()
        require(
            parked.is_dir() and not self.journal.exists() and not self.cache.exists(),
            "the original journal or cache path was accessed by the verifier",
        )
        self.receipt["files"] = files
        self.event(parked_journal=True, fresh_read_only_verifier=True)


def configure_parser(parser):
    parser.add_argument("--msize-kib", type=int, choices=(256, 1024), default=1024)
    parser.add_argument("--depth", type=int, default=8)
    parser.add_argument("--write-kib", type=int, default=0, help="fixed WRITE size; default is the I/O unit")
    parser.add_argument("--crash-after-fsync", action="store_true")
    parser.add_argument("--readback-during-drain", action="store_true")
    parser.add_argument(
        "--scratch-fs", choices=("ext4", "tmpfs"), default="ext4",
        help="tmpfs removes the device from the path to expose the engine's own ceiling",
    )
    parser.add_argument("--tail-reader-mib", type=int, default=0, help="re-read this many MiB behind the acknowledged prefix during the copy")


def validate_args(parser, args):
    if not (1 <= args.files <= 8 and 64 <= args.file_mib <= 64 * 1024 and 1 <= args.depth <= 64):
        parser.error("expected 1–8 files of 64 MiB–64 GiB and depth 1–64")


if __name__ == "__main__":
    sys.exit(
        fixture.main(
            SustainedFixture,
            configure_parser=configure_parser,
            validate_args=validate_args,
            validate_mounts=validate_scratch,
            description=__doc__,
        )
    )
