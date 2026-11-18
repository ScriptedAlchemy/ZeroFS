#!/usr/bin/env python3
"""Qualify bounded SSD admission and one-member recovery over userspace 9P.

Use two distinct 384 MiB tmpfs roots and the standard NAS journal fixture
templates. The combined capacity must retain the complete synthetic working set.
"""

from dataclasses import asdict, dataclass
import hashlib
import importlib
import shutil
import struct
import sys
import threading
import time
import tomllib


sys.dont_write_bytecode = True
fixture = importlib.import_module("test-nas-journal")
Fixture = fixture.Fixture
NineP = fixture.NineP
MIB = fixture.MIB
MEMBERS = fixture.MEMBERS
require = fixture.require
FILE_MIB = 36
FILES = 17
DISK_BYTES = 512 * MIB
SEAL_BYTES = 32 * MIB
STALL_SECONDS = 3
CAPACITY_MARGIN = 32 * MIB
PLATEAU_METRICS = (
    "accepted_sequence",
    "local_sequence",
    "remote_sequence",
    "dirty_ssd_reserved_bytes",
    "remote_bytes_completed_total",
)


@dataclass(frozen=True)
class Request:
    ordinal: int
    file: str
    operation: str
    offset: int | None
    bytes: int | None
    started_seconds: float


class Writer:
    def __init__(self, case):
        self.case = case
        self.lock = threading.Lock()
        self.active = None
        self.ordinal = 0
        self.acknowledgments = {}
        self.files = {}
        self.error = None
        self.thread = threading.Thread(target=self.run, daemon=True)

    def snapshot(self, ordinal=None):
        with self.lock:
            if self.error is not None:
                raise RuntimeError("pressure writer failed") from self.error
            return self.active, self.acknowledgments.get(ordinal), self.files.copy()

    def run(self):
        writer = self

        class ObservedNineP(NineP):
            def call(self, kind, body, *, tag=None):
                if kind not in (118, 50):
                    return super().call(kind, body, tag=tag)
                offset, size = (
                    struct.unpack_from("<QI", body, 4) if kind == 118 else (None, None)
                )
                with writer.lock:
                    writer.ordinal += 1
                    request = Request(
                        writer.ordinal,
                        self.file,
                        "write" if kind == 118 else "fsync",
                        offset,
                        size,
                        time.monotonic() - writer.case.start,
                    )
                    writer.active = request
                reply = super().call(kind, body, tag=tag)
                with writer.lock:
                    writer.acknowledgments[request.ordinal] = (
                        time.monotonic() - writer.case.start
                    )
                    writer.active = None
                return reply

        try:
            with ObservedNineP(self.case.ninep, self.case.remaining) as client:
                for index in range(FILES):
                    name = f"pressure-{index:02d}"
                    data = hashlib.shake_256(
                        f"zerofs-nas-pressure:{name}".encode()
                    ).digest(FILE_MIB * MIB)
                    digest = hashlib.sha256(data).hexdigest()
                    client.file = name
                    client.create(name.encode())
                    client.write(data)
                    client.fsync()
                    client.clunk()
                    with self.lock:
                        self.files[name] = digest
        except Exception as error:
            with self.lock:
                self.error = error


def member_deltas(members, baseline):
    return {
        name: {
            "committed_bytes_delta": members[name]["used_bytes"]
            - baseline[name]["used_bytes"],
            "committed_objects_delta": members[name]["objects"]
            - baseline[name]["objects"],
        }
        for name in MEMBERS
    }


def require_held(name, members, baseline):
    member = members[name]
    require(
        member["available_bytes"] == 0
        and (member["space"]["full"] or {}).get("reason") == "below_free_space_reserve",
        f"{name} is not below its filesystem reserve: {member}",
    )
    require(
        member["used_bytes"] == baseline[name]["used_bytes"]
        and member["objects"] == baseline[name]["objects"],
        f"{name} committed data while below reserve: {member}",
    )


class PressureFixture(Fixture):
    writer = None

    def sample(self):
        rows = self.metric_snapshot()
        if rows:
            reserved = rows["dirty_ssd_reserved_bytes"]
            capacity = rows["dirty_ssd_capacity_bytes"]
            require(capacity == DISK_BYTES, f"unexpected SSD budget: {capacity}")
            require(
                reserved <= capacity,
                f"SSD reservations exceeded the budget: {reserved}",
            )
            self.receipt["peak_observed_ssd_reservation_bytes"] = max(
                self.receipt.get("peak_observed_ssd_reservation_bytes", 0), reserved
            )
        return rows

    def release(self, name, ballast):
        started = time.monotonic() - self.start
        ballast.unlink()
        release = {
            "unlink_started_seconds": started,
            "unlink_completed_seconds": time.monotonic() - self.start,
        }
        self.receipt.setdefault("ballast_releases", {})[name] = release
        self.event(member=name, **release)
        return release

    def observe_stall(self):
        candidate = None
        unchanged_since = None

        def sustained():
            nonlocal candidate, unchanged_since
            rows = self.sample()
            active, _, acknowledged = self.writer.snapshot()
            require(
                len(acknowledged) < FILES,
                "entire workload bypassed the SSD pressure boundary",
            )
            if (
                not active
                or not rows
                or rows["ssd_admission_paced"] != 1
                or rows["dirty_ssd_reserved_bytes"]
                < DISK_BYTES * 95 // 100 - SEAL_BYTES
                or rows["accepted_sequence"] != rows["local_sequence"]
                or rows["local_sequence"] <= rows["remote_sequence"]
            ):
                candidate = None
                return None
            observed = (active, *(rows[key] for key in PLATEAU_METRICS))
            now = time.monotonic()
            if observed != candidate:
                candidate = observed
                unchanged_since = now
                return None
            if now - unchanged_since >= STALL_SECONDS:
                return active, acknowledged, rows, now - unchanged_since
            return None

        return self.wait(sustained)

    def run(self):
        prepared = self.prepare()
        baseline = prepared.baseline_status
        self.stage = "pressure-capacity-preflight"
        config = tomllib.loads(self.config.read_text())
        reserve = [
            int(member["reserve_free_gb"] * 10**9)
            for member in config["storage"]["pool"]["members"]
        ]
        usable = [
            max(0, min(fixture.space(path) - held, baseline[name]["available_bytes"]))
            for name, path, held in zip(MEMBERS, self.members, reserve)
        ]
        journal_free = fixture.space(self.journal)
        journal_reserve = int(config["writeback"]["min_free_gb"] * 10**9)
        self.receipt.update(
            scope="bounded SSD pressure, 9P2000.L, synthetic local tmpfs destinations",
            recovery_scope="one member resumed the blocked request, both available for final settlement",
            working_set_bytes=FILES * FILE_MIB * MIB,
            admitted_ssd_budget_bytes=DISK_BYTES,
            stall_observation_seconds=STALL_SECONDS,
            initial_member_room_bytes=dict(zip(MEMBERS, usable)),
            initial_journal_filesystem_free_bytes=journal_free,
        )
        require(
            sum(usable) >= FILES * FILE_MIB * MIB + CAPACITY_MARGIN,
            f"combined member capacity after reserves cannot retain the pressure workload: {usable}",
        )
        require(
            usable[0] >= 2 * SEAL_BYTES,
            "member-a cannot provide initial cleanup capacity",
        )
        require(
            journal_free >= DISK_BYTES + journal_reserve + CAPACITY_MARGIN,
            "scratch filesystem lacks journal capacity above its physical reserve",
        )
        self.stage = "hold-both-members"
        ballast = [self.ballast(path) for path in prepared.member_roots]
        self.event(members=self.held(baseline))

        self.stage = "observe-ssd-pressure"
        self.writer = Writer(self)
        self.writer.thread.start()
        blocked, acknowledged, pressure, plateau_seconds = self.observe_stall()
        require(
            len(acknowledged) >= 2
            and len(set(acknowledged.values())) == len(acknowledged),
            "pressure prefill did not yield distinct fsynced files",
        )
        self.event(
            blocked_request=asdict(blocked),
            stable_seconds=plateau_seconds,
            writeback=pressure,
            members=self.held(baseline),
            fsynced_bytes=len(acknowledged) * FILE_MIB * MIB,
        )

        self.stage = "verify-acknowledged-while-both-held"
        self.verify_hashes(acknowledged)
        active, completed, _ = self.writer.snapshot(blocked.ordinal)
        require(
            active == blocked and completed is None,
            "blocked request escaped before release",
        )
        rows = self.sample()
        require(
            rows and all(rows[key] == pressure[key] for key in PLATEAU_METRICS),
            "journal pressure advanced while both destinations were held",
        )
        self.event(writeback=rows, members=self.held(baseline))

        self.stage = "recover-member-a-with-member-b-held"
        released = self.release("member-a", ballast[0])

        def resumed():
            rows = self.sample()
            _, response, _ = self.writer.snapshot(blocked.ordinal)
            if response is None or not rows:
                return None
            members = self.pool_status()
            require_held("member-b", members, baseline)
            deltas = member_deltas(members, baseline)
            if (
                rows["remote_bytes_completed_total"]
                <= pressure["remote_bytes_completed_total"]
                or deltas["member-a"]["committed_bytes_delta"] <= 0
            ):
                return None
            return rows, response, members, deltas

        progress, response, members, deltas = self.wait(resumed)
        require(
            response >= released["unlink_started_seconds"],
            "selected 9P request completed before capacity was released",
        )
        self.event(
            resumed_request=asdict(blocked),
            response_seconds=response,
            response_after_release_started_seconds=response
            - released["unlink_started_seconds"],
            writeback=progress,
            members=members,
            distribution=deltas,
        )

        self.stage = "release-member-b-and-settle"
        self.release("member-b", ballast[1])

        def drained():
            rows = self.sample()
            _, _, completed = self.writer.snapshot()
            if (
                self.writer.thread.is_alive()
                or len(completed) != FILES
                or not self.drained(rows)
            ):
                return None
            return rows, completed

        settled, expected = self.wait(drained)
        require(
            len(set(expected.values())) == FILES, "workload payload digests collided"
        )
        self.event(writeback=settled, fsynced_bytes=len(expected) * FILE_MIB * MIB)
        self.storage_account()
        self.cut_after_remote_drain()

        self.stage = "verify-destination-settlement"
        final = self.pool_status(offline=True)
        distribution = member_deltas(final, baseline)
        for name, path in zip(MEMBERS, self.members):
            physical = fixture.account(path)
            distribution[name]["allocated_bytes_delta"] = (
                physical["allocated_bytes"]
                - prepared.baseline_accounting[name]["allocated_bytes"]
            )
            require(
                all(delta > 0 for delta in distribution[name].values())
                and final[name]["reserved_bytes"]
                == final[name]["pending_uploads"]
                == 0,
                f"{name} has unsettled or missing destination data: {final[name]}",
            )
        leftovers = fixture.journal_payloads(self.journal)
        require(
            not leftovers,
            f"journal retains payload files after settlement: {leftovers}",
        )
        self.event(
            members=final,
            distribution=distribution,
            journal_payload_files=len(leftovers),
        )
        shutil.rmtree(self.journal)
        shutil.rmtree(self.cache)

        self.stage = "independent-read-only-verifier"
        self.start_server(prepared.verify_config, read_only=True)
        self.verify_hashes(expected)
        self.stop_server()
        require(
            not self.journal.exists() and not self.cache.exists(),
            "verifier recreated the original journal or cache",
        )

    def cleanup(self):
        super().cleanup()
        if self.writer is not None and self.writer.thread.ident is not None:
            self.writer.thread.join(timeout=2)
            if self.writer.thread.is_alive():
                self.receipt.pop("cleanup_verified", None)
                raise RuntimeError("pressure writer did not exit during cleanup")


def configure_parser(parser):
    parser.set_defaults(files=FILES, file_mib=FILE_MIB, timeout=600)


def validate_args(parser, args):
    if (args.files, args.file_mib) != (FILES, FILE_MIB):
        parser.error(f"pressure requires {FILES} files of {FILE_MIB} MiB each")


if __name__ == "__main__":
    sys.exit(
        fixture.main(
            PressureFixture,
            configure_parser=configure_parser,
            validate_args=validate_args,
            description=__doc__,
        )
    )
