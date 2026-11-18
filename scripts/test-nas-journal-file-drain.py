#!/usr/bin/env python3
"""Observe live pool drain across two directories on a single ext4 device.

Pass the same private --scratch parent to --member-a and --member-b. The
fixture creates, uses, and removes separate owned children. Run it under
/var/tmp or at a /scratch bind mount of a directory under /var/tmp.
"""

from concurrent.futures import Future
import ctypes
import hashlib
import importlib
import os
from pathlib import Path
import re
import shutil
import struct
import sys
import threading
import time
import uuid


sys.dont_write_bytecode = True
fixture = importlib.import_module("test-nas-journal")
MEMBERS = fixture.MEMBERS
MIB = fixture.MIB
require = fixture.require

OPEN = 0x00000020
CLOSE_WRITE = 0x00000008
CREATE = 0x00000100
WATCH_LOST = 0x00000400 | 0x00000800 | 0x00002000 | 0x00008000
OVERFLOW = 0x00004000
IS_DIRECTORY = 0x40000000
EVENT = struct.Struct("=iIII")
STAGE = re.compile(rb"([a-f0-9]{8}(?:-[a-f0-9]{4}){3}-[a-f0-9]{12})#[1-9][0-9]*")


def validate_mounts(args):
    scratch = args.scratch
    require(
        args.member_mounts == (scratch, scratch),
        "both member parents must equal --scratch",
    )
    allowed_root = Path("/var/tmp")
    bind = scratch == Path("/scratch")
    require(
        bind or allowed_root in scratch.parents,
        "scratch must be a private directory under /var/tmp or its /scratch bind mount",
    )
    dev = scratch.stat().st_dev
    device = f"{os.major(dev)}:{os.minor(dev)}"
    candidates = []
    for line in Path("/proc/self/mountinfo").read_text().splitlines():
        before, after = line.split(" - ", 1)
        fields = before.split()
        if fields[2] != device:
            continue
        root, mount = (
            Path(re.sub(r"\\([0-7]{3})", lambda m: chr(int(m[1], 8)), encoded))
            for encoded in fields[3:5]
        )
        if scratch == mount or mount in scratch.parents:
            candidates.append((mount, root, after.split()[0]))
    require(candidates, f"unable to locate scratch mount for device {device}")
    mount, root, fs = max(candidates, key=lambda item: len(item[0].parts))
    require(fs == "ext4", f"scratch filesystem must be ext4, found {fs}")
    if bind:
        require(
            mount == scratch and allowed_root in root.parents,
            "the /scratch bind must originate under /var/tmp",
        )


def uuid_name(name):
    try:
        return str(uuid.UUID(name)) == name
    except ValueError:
        return False


def objects_dir(path):
    directories = [
        p / "objects" for p in (path / ".zerofs-pool").iterdir() if uuid_name(p.name)
    ]
    require(
        len(directories) == 1 and directories[0].is_dir(),
        f"expected one initialized pool objects directory in {path}",
    )
    return directories[0]


def generations(path):
    return {child.name for child in path.iterdir() if uuid_name(child.name)}


class StagingWatch:
    def __init__(self, paths, case):
        self.case = case
        self.order = 0
        self.active = {}
        self.closed = {name: set() for name in MEMBERS}
        self.counts = {
            name: dict.fromkeys(("CREATE", "OPEN", "CLOSE_WRITE"), 0)
            for name in MEMBERS
        }
        self.first_overlap = None
        self.overlap_pairs = 0
        self.watches = {}
        libc = ctypes.CDLL(None, use_errno=True)
        init = libc.inotify_init1
        init.argtypes = [ctypes.c_int]
        init.restype = ctypes.c_int
        add = libc.inotify_add_watch
        add.argtypes = [ctypes.c_int, ctypes.c_char_p, ctypes.c_uint32]
        add.restype = ctypes.c_int
        self.fd = self.checked(init(os.O_NONBLOCK | os.O_CLOEXEC), "inotify_init1")
        try:
            for name, path in zip(MEMBERS, paths):
                watch = self.checked(
                    add(
                        self.fd,
                        os.fsencode(path),
                        OPEN | CLOSE_WRITE | CREATE | WATCH_LOST,
                    ),
                    f"inotify_add_watch {path}",
                )
                require(
                    watch not in self.watches,
                    "pool paths share the same directory inode",
                )
                self.watches[watch] = name
        except BaseException:
            os.close(self.fd)
            raise

    @staticmethod
    def checked(result, call):
        if result < 0:
            errno = ctypes.get_errno()
            raise OSError(errno, f"{call}: {os.strerror(errno)}")
        return result

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        os.close(self.fd)

    def read_pending(self):
        while True:
            self.case.remaining()
            try:
                data = os.read(self.fd, 65536)
            except BlockingIOError:
                return
            observed_at = time.monotonic() - self.case.start
            offset = 0
            while offset < len(data):
                watch, mask, _, length = EVENT.unpack_from(data, offset)
                offset += EVENT.size
                name = data[offset : offset + length].split(b"\0", 1)[0]
                offset += length
                require(
                    not mask & OVERFLOW,
                    "inotify queue overflow invalidates overlap evidence",
                )
                require(not mask & WATCH_LOST, "an inotify directory watch was lost")
                match = STAGE.fullmatch(name)
                if not match or mask & IS_DIRECTORY:
                    continue
                member = self.watches[watch]
                stage = name.decode("ascii")
                event = {CREATE: "CREATE", OPEN: "OPEN", CLOSE_WRITE: "CLOSE_WRITE"}[
                    mask
                ]
                self.order += 1
                self.counts[member][event] += 1
                entry = {
                    "order": self.order,
                    "read_elapsed_seconds": observed_at,
                    "member": member,
                    "staging_file": stage,
                    "event": event,
                }
                self.case.receipt["staging_events"].append(entry)
                key = (member, stage)
                if event == "OPEN":
                    require(key not in self.active, f"duplicate staging OPEN: {key}")
                    for other in self.active.values():
                        if other["member"] != member:
                            self.overlap_pairs += 1
                            if self.first_overlap is None:
                                self.first_overlap = [other, entry]
                    self.active[key] = entry
                elif event == "CLOSE_WRITE":
                    require(
                        key in self.active, f"staging CLOSE_WRITE without OPEN: {key}"
                    )
                    del self.active[key]
                    self.closed[member].add(match[1].decode("ascii"))

    def finish(self, before, after):
        self.read_pending()
        require(
            not self.active, f"staging writers still open after drain: {self.active}"
        )
        for name, old, new in zip(MEMBERS, before, after):
            require(
                new - old and new - old <= self.closed[name],
                f"{name} published generations missing observed staging lifetimes",
            )
        return {
            "events_by_member": self.counts,
            "cross_member_open_lifetime_overlap_observed": self.first_overlap
            is not None,
            "cross_member_overlap_pairs": self.overlap_pairs,
            "first_overlap": self.first_overlap,
            "event_scope": (
                "One Linux inotify queue, ordered OPEN and CLOSE_WRITE on canonical pool "
                "staging names. Timestamps record userspace reads. Both members share one "
                "ext4 device. Overlap describes file lifetimes on that device."
            ),
        }


class FileDrainFixture(fixture.Fixture):
    barrier = None
    writers = ()

    def write_file(self, name, size, barrier=None):
        data = hashlib.shake_256(f"zerofs-file-drain:{name}".encode()).digest(size)
        digest = hashlib.sha256(data).hexdigest()
        with fixture.NineP(self.ninep, self.remaining) as client:
            client.create(name.encode())
            if barrier is not None:
                barrier.wait(timeout=self.remaining())
            started = time.monotonic() - self.start
            client.write(data)
            acknowledged = time.monotonic() - self.start
            client.fsync()
            fsynced = time.monotonic() - self.start
            client.clunk()
        return {
            "file": name,
            "bytes": size,
            "sha256": digest,
            "write_started_seconds": started,
            "write_acknowledged_seconds": acknowledged,
            "fsync_completed_seconds": fsynced,
        }

    def wait_idle_after(self, previous_remote):
        def advanced():
            rows = self.metric_snapshot()
            if self.drained(rows) and rows["remote_sequence"] > previous_remote:
                return rows
            return None

        return self.wait(advanced)

    def start_writers(self):
        self.barrier = threading.Barrier(self.args.files)
        self.writers = []
        for index in range(self.args.files):
            name = f"ext4-drain-{index:02d}"
            result = Future()

            def run(name=name, result=result):
                try:
                    result.set_result(
                        self.write_file(name, self.args.file_mib * MIB, self.barrier)
                    )
                except Exception as error:
                    result.set_exception(error)
                    self.barrier.abort()

            thread = threading.Thread(target=run, daemon=True)
            thread.start()
            self.writers.append((thread, result))

    def run(self):
        self.receipt.update(
            scope="userspace 9P2000.L and two local directories on one ext4 device",
            filesystem_device=self.args.scratch.stat().st_dev,
            staging_events=[],
        )
        prepared = self.prepare()
        self.stage = "warm-pool"
        files = []
        warm_status = prepared.initial_writeback
        for index in range(len(MEMBERS)):
            files.append(self.write_file(f"warm-{index}", MIB))
            warm_status = self.wait_idle_after(warm_status["remote_sequence"])
            self.event(**files[-1], writeback=warm_status)
        baseline = self.pool_status()
        for name in MEMBERS:
            require(
                baseline[name]["objects"]
                == prepared.baseline_status[name]["objects"] + 1
                and baseline[name]["reserved_bytes"]
                == baseline[name]["pending_uploads"]
                == 0,
                f"warm-up did not settle one data object on {name}: {baseline[name]}",
            )
        self.event(members=baseline)
        paths = list(map(objects_dir, self.members))
        before = list(map(generations, paths))

        self.stage = "parallel-9p-write-and-destage"
        with StagingWatch(paths, self) as watch:
            self.start_writers()

            def settled():
                watch.read_pending()
                responses = [
                    result.result() for _, result in self.writers if result.done()
                ]
                rows = self.metric_snapshot()
                if (
                    len(responses) == self.args.files
                    and self.drained(rows)
                    and rows["remote_sequence"] > warm_status["remote_sequence"]
                ):
                    return responses, rows
                return None

            responses, drained = self.wait(settled)
            files.extend(responses)
            for row in responses:
                self.event(**row)
            self.event(writeback=drained)
            self.stop_server()
            self.receipt["staging_observation"] = watch.finish(
                before, list(map(generations, paths))
            )
            self.event(**self.receipt["staging_observation"])

        self.stage = "offline-destination"
        final = self.pool_status(offline=True)
        distribution = {}
        for name in MEMBERS:
            distribution[name] = {
                key: final[name][key] - baseline[name][key]
                for key in ("used_bytes", "objects")
            }
            require(
                all(delta > 0 for delta in distribution[name].values())
                and final[name]["reserved_bytes"]
                == final[name]["pending_uploads"]
                == 0,
                f"{name} did not settle new segments: {final[name]}",
            )
        leftovers = fixture.journal_payloads(self.journal)
        require(not leftovers, f"journal payload remains after drain: {leftovers}")
        self.event(
            members=final, committed_deltas=distribution, journal_payload_files=0
        )

        self.stage = "verify-fresh-cache-read-only"
        parked = self.root / "parked-journal"
        self.journal.rename(parked)
        shutil.rmtree(self.cache)
        self.start_server(prepared.verify_config, read_only=True)
        require(
            len({row["sha256"] for row in files}) == len(files),
            "the synthetic payload digests must be distinct",
        )
        with fixture.NineP(self.ninep, self.remaining) as client:
            for row in files:
                observed = client.read_hash(row["file"].encode(), row["bytes"])
                require(observed == row["sha256"], f"digest mismatch for {row['file']}")
                self.event(file=row["file"], bytes=row["bytes"], sha256=observed)
        self.stop_server()
        require(
            parked.is_dir() and not self.journal.exists() and not self.cache.exists(),
            "the original journal or cache path was accessed by the verifier",
        )
        self.event(
            parked_journal=True,
            fresh_read_only_verifier=True,
            verification_scope="new process and engine cache; ext4 and host page cache shared",
        )

    def cleanup(self):
        if self.barrier is not None:
            self.barrier.abort()
        super().cleanup()
        for thread, _ in self.writers:
            thread.join(timeout=2)
            if thread.is_alive():
                self.receipt.pop("cleanup_verified", None)
                raise RuntimeError("9P writer did not exit during cleanup")


def validate_args(parser, args):
    if not (
        2 <= args.files <= 8
        and 1 <= args.file_mib <= 64
        and args.files * args.file_mib + len(MEMBERS) <= 256
    ):
        parser.error(
            "expected 2–8 files of 1–64 MiB and a total up to 256 MiB with warm-up"
        )


if __name__ == "__main__":
    sys.exit(
        fixture.main(
            FileDrainFixture,
            validate_args=validate_args,
            validate_mounts=validate_mounts,
            description=__doc__,
        )
    )
