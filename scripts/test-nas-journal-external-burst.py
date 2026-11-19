#!/usr/bin/env python3
"""Compare 9P writes from a separate Linux container over a private Unix socket."""

from concurrent.futures import ThreadPoolExecutor
import hashlib
import importlib
import json
import os
from pathlib import Path
import shutil
import sys
import threading
import time

disk = importlib.import_module("test-nas-journal-file-drain")
fixture = disk.fixture
require = disk.require
MIB = disk.MIB
IPC = Path("/ipc")
SOCKET = IPC / "ninep.sock"
FILES = 6
SIZE = 36 * MIB
MSIZE = MIB


def publish(name, payload):
    output = IPC / name
    pending = output.with_suffix(".tmp")
    pending.write_text(json.dumps(payload))
    pending.rename(output)


def client_failure():
    failure = IPC / "client-error.json"
    if failure.exists():
        raise RuntimeError(f"external client: {json.loads(failure.read_text())}")


def await_message(name, remaining):
    path = IPC / name
    while not path.exists():
        client_failure()
        time.sleep(min(0.02, remaining()))
    return json.loads(path.read_text())


def resource_scope():
    base = Path("/sys/fs/cgroup")
    keys = (
        "cpu.max",
        "cpu.stat",
        "cpuset.cpus.effective",
        "memory.max",
        "memory.peak",
        "memory.events",
        "memory.swap.max",
        "memory.swap.current",
        "memory.swap.peak",
    )
    require(
        Path("/proc/self/cgroup").read_text().strip() == "0::/",
        "expected private cgroup",
    )
    return {
        "affinity": sorted(os.sched_getaffinity(0)),
        **{key: (base / key).read_text().strip() for key in keys},
    }


def client():
    deadline = time.monotonic() + 600

    def remaining():
        delay = deadline - time.monotonic()
        if delay <= 0:
            raise TimeoutError("external client deadline")
        return delay

    def read_all(rows):
        verified = []
        with fixture.NineP(SOCKET, remaining, MSIZE) as connection:
            for row in rows:
                observed = connection.read_hash(row["file"].encode(), row["bytes"])
                require(observed == row["sha256"], f"hash mismatch for {row['file']}")
                verified.append({**row, "sha256": observed})
        return verified

    def sample():
        phases.append(
            {
                "monotonic_seconds": time.monotonic(),
                "process_cpu_seconds": time.process_time(),
            }
        )

    def write(row, payload):
        name = row["file"].encode()
        try:
            with fixture.NineP(SOCKET, remaining, MSIZE) as connection:
                connection.create(name)
                barrier.wait(timeout=remaining())
                started = time.monotonic()
                connection.write(payload)
                acknowledged = time.monotonic()
                barrier.wait(timeout=remaining())
                read_all([row])
                visible = time.monotonic()
                barrier.wait(timeout=remaining())
                fsync_started = time.monotonic()
                connection.fsync()
                fsynced = time.monotonic()
                barrier.wait(timeout=remaining())
                connection.clunk()
        except Exception:
            barrier.abort()
            raise
        return {
            **row,
            "io_chunk_bytes": connection.io_chunk,
            "write_started_monotonic": started,
            "write_acknowledged_monotonic": acknowledged,
            "cross_client_hash_verified_monotonic": visible,
            "fsync_started_monotonic": fsync_started,
            "fsync_completed_monotonic": fsynced,
        }

    before = resource_scope()
    phases = []
    barrier = threading.Barrier(FILES, action=sample)
    payloads = []
    for index in range(FILES):
        name = f"ext4-drain-{index:02d}"
        payload = hashlib.shake_256(f"zerofs-file-drain:{name}".encode()).digest(SIZE)
        payloads.append(
            (
                {
                    "file": name,
                    "bytes": SIZE,
                    "sha256": hashlib.sha256(payload).hexdigest(),
                },
                payload,
            )
        )
    publish("ready.json", before)
    await_message("write.json", remaining)
    with ThreadPoolExecutor(max_workers=FILES) as pool:
        writers = [pool.submit(write, row, data) for row, data in payloads]
        responses = [writer.result(timeout=remaining()) for writer in writers]
    publish("written.json", {"files": responses, "phase_cpu": phases})
    expected = await_message("verify.json", remaining)
    verified = read_all(expected)
    publish("verified.json", {"files": verified, "resources": resource_scope()})


class ExternalBurst(disk.FileDrainFixture):
    def render(self, *args, **kwargs):
        self.ninep = SOCKET
        return super().render(*args, **kwargs)

    def sample_cpu(self):
        super().sample_cpu()
        latest = self.receipt["phase_cpu"][-1]
        latest["service_fixture_process_cpu_seconds"] = latest.pop(
            "client_process_cpu_seconds"
        )

    def run(self):
        before = resource_scope()
        external = await_message("ready.json", self.remaining)
        require(
            len(before["affinity"]) == 2
            and before["memory.max"] == str(8 * 1024**3)
            and before["memory.swap.max"] == "0"
            and not set(before["affinity"]).intersection(external["affinity"]),
            "unexpected or overlapping CPU and service memory limits",
        )
        self.receipt.update(
            scope="two isolated containers using userspace 9P2000.L over a private Unix socket and two local ext4 directories",
            msize_bytes=MSIZE,
            staging_events=[],
            service_before=before,
            client_before=external,
        )
        prepared = self.prepare()

        self.stage = "warm-pool"
        warm = []
        status = prepared.initial_writeback
        for index in range(len(disk.MEMBERS)):
            row = self.write_file(f"warm-{index}", MIB)
            warm.append(row)
            status = self.wait_idle_after(status["remote_sequence"])
        baseline = self.pool_status()
        for name in disk.MEMBERS:
            require(
                baseline[name]["objects"]
                == prepared.baseline_status[name]["objects"] + 1,
                f"warm-up did not place a new object on {name}",
            )
        paths = list(map(disk.objects_dir, self.members))
        generations = list(map(disk.generations, paths))
        self.stage = "external-9p-burst"
        self.receipt["phase_cpu"] = []
        samples = []
        with disk.StagingWatch(paths, self) as watch:
            self.sample_cpu()
            publish("write.json", {})

            def settled():
                client_failure()
                watch.read_pending()
                rows = self.metric_snapshot()
                if rows:
                    samples.append({"monotonic_seconds": time.monotonic(), **rows})
                if (
                    (IPC / "written.json").is_file()
                    and self.drained(rows)
                    and rows["remote_sequence"] > status["remote_sequence"]
                ):
                    return rows
                return None

            drained = self.wait(settled)
            self.sample_cpu()
            self.cut_after_remote_drain()
            self.receipt["staging_observation"] = watch.finish(
                generations, list(map(disk.generations, paths))
            )
        responses = await_message("written.json", self.remaining)
        rows = responses["files"]
        first = min(row["write_started_monotonic"] for row in rows)
        last_ack = max(row["write_acknowledged_monotonic"] for row in rows)
        last_visible = max(row["cross_client_hash_verified_monotonic"] for row in rows)
        first_fsync = min(row["fsync_started_monotonic"] for row in rows)
        last_fsync = max(row["fsync_completed_monotonic"] for row in rows)
        require(
            len(rows) == FILES and len({row["sha256"] for row in rows}) == FILES,
            "missing or duplicate workload payloads",
        )
        require(
            last_ack <= last_visible <= first_fsync <= last_fsync,
            "expected cross-client verification before explicit fsync",
        )
        self.receipt.update(
            external_files=rows,
            client_phase_cpu=responses["phase_cpu"],
            writeback_samples=samples,
            drained_writeback=drained,
            transfer={
                "logical_bytes": sum(row["bytes"] for row in rows),
                "write_seconds": last_ack - first,
                "ack_to_hash_verified_seconds": last_visible - last_ack,
                "fsync_phase_seconds": last_fsync - first_fsync,
                "through_fsync_seconds": last_fsync - first,
                "through_observed_drain_seconds": samples[-1]["monotonic_seconds"]
                - first,
                "clients": len(rows),
                "outstanding_writes_per_client": 1,
            },
        )

        self.stage = "offline-destination"
        final = self.pool_status(offline=True)
        for name in disk.MEMBERS:
            require(
                all(
                    final[name][key] > baseline[name][key]
                    for key in ("used_bytes", "objects")
                )
                and final[name]["reserved_bytes"]
                == final[name]["pending_uploads"]
                == 0,
                f"{name} did not settle new segments",
            )
        require(
            not fixture.journal_payloads(self.journal), "journal payload after drain"
        )
        self.event(members=final, journal_payload_files=0)

        self.stage = "verify-fresh-cache-read-only"
        parked = self.root / "parked-journal"
        self.journal.rename(parked)
        shutil.rmtree(self.cache)
        self.start_server(prepared.verify_config, read_only=True)
        expected = [
            {key: row[key] for key in ("file", "bytes", "sha256")}
            for row in warm + rows
        ]
        publish("verify.json", expected)
        verified = await_message("verified.json", self.remaining)
        require(
            verified["files"] == expected, "external verifier did not check all hashes"
        )
        self.stop_server()
        require(
            parked.is_dir() and not self.journal.exists() and not self.cache.exists(),
            "the verifier accessed the original journal or cache",
        )
        self.event(
            parked_journal=True, fresh_read_only_verifier=True, files=verified["files"]
        )
        after = resource_scope()
        self.receipt.update(service_after=after, client_after=verified["resources"])
        for scope, initial, final in (
            ("service", before, after),
            ("client", external, verified["resources"]),
        ):
            require(
                final["memory.swap.current"] == final["memory.swap.peak"] == "0"
                and final["memory.events"] == initial["memory.events"]
                and final["affinity"] == initial["affinity"],
                f"{scope} resource envelope changed or reported memory events",
            )


def parser_options(parser):
    disk.configure_parser(parser)
    parser.set_defaults(msize_kib=1024)


def validate_args(parser, args):
    if (args.files, args.file_mib, args.msize_kib) != (
        FILES,
        SIZE // MIB,
        MSIZE // 1024,
    ):
        parser.error("external comparison requires six 36 MiB files and a 1 MiB msize")


if __name__ == "__main__":
    if sys.argv[1:] == ["--client"]:
        try:
            client()
        except Exception as error:
            publish("client-error.json", f"{type(error).__name__}: {error}")
            raise
    else:
        sys.exit(
            fixture.main(
                ExternalBurst,
                configure_parser=parser_options,
                validate_args=validate_args,
                validate_mounts=disk.validate_mounts,
                description=__doc__,
            )
        )
