#!/usr/bin/env python3
"""Exercise local pool initialization, 9P I/O, expansion, and migration.

Run against a freshly built binary, for example:
    python3 scripts/test-local-storage-pool.py --binary zerofs/target/debug/zerofs

Everything, including server sockets and storage, lives in one temporary child.
The script uses the ordinary CLI and never mounts a filesystem or needs root.
"""

from __future__ import annotations

import argparse
import contextlib
import hashlib
import json
import os
from pathlib import Path
import shutil
import signal
import socket
import struct
import subprocess
import sys
import tempfile
import time
from collections import Counter
from contextlib import contextmanager
from datetime import datetime, timezone
import fcntl


PAYLOAD_BYTES = 768 * 1024
RUNNING_STATUS_TIMEOUT_SECONDS = 5
RUNNING_STATUS_SAMPLES = 3
NATIVE_MUTATION_LOCK_NAME = ".zerofs-local-mutations#0"
# Version-one footer from zerofs/src/segment.rs; only identity and bounds are
# parsed here. SHA-256 compares whole bodies; cold reads verify file bytes.
SEGMENT_FOOTER = struct.Struct("<4sIIQIQQQQII")
FIRST_MEMBER_BYTES = 1024 * 1024
OTHER_MEMBER_BYTES = 8 * 1024 * 1024
TOTAL_TIMEOUT_SECONDS = 300
COMMAND_TIMEOUT_SECONDS = 90
START_TIMEOUT_SECONDS = 60
SHUTDOWN_TIMEOUT_SECONDS = 30
CANCEL_SHUTDOWN_TIMEOUT_SECONDS = 2


class SmokeFailure(RuntimeError):
    pass


class SmokeCancelled(SmokeFailure):
    def __init__(self, signum: int):
        super().__init__(f"cancelled by {signal.Signals(signum).name}")


_active_cancellation = None


@contextlib.contextmanager
def defer_cancellation():
    """Finish transferring child ownership before unwinding a signal."""
    state = _active_cancellation
    if state is None:
        yield
        return
    state["deferred"] += 1
    try:
        yield
    finally:
        state["deferred"] -= 1
        if state["received"] and state["deferred"] == 0:
            raise SmokeCancelled(state["received"][0])


@contextlib.contextmanager
def cancellation_signals():
    """Let cancellation unwind owned servers before the runner exits."""
    global _active_cancellation
    previous_state = _active_cancellation
    state = {"received": [], "deferred": 0}
    _active_cancellation = state
    previous = {signum: signal.getsignal(signum) for signum in (signal.SIGINT, signal.SIGTERM)}

    def cancel(signum, _frame):
        # Ignore repeated soft cancellation while bounded reaping completes.
        # Keep a caught handler rather than SIG_IGN, so an in-flight spawn does
        # not inherit ignored signal dispositions into its new executable.
        if state["received"]:
            return
        state["received"].append(signum)
        if state["deferred"] == 0:
            raise SmokeCancelled(signum)

    try:
        for signum in previous:
            signal.signal(signum, cancel)
        yield state["received"]
    finally:
        for signum, handler in previous.items():
            signal.signal(signum, handler)
        _active_cancellation = previous_state


def require(condition: bool, message: str) -> None:
    if not condition:
        raise SmokeFailure(message)


def tail(text: str, limit: int = 6000) -> str:
    return text[-limit:]


def time_left(deadline: float, maximum: float) -> float:
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise SmokeFailure("local storage pool smoke test exceeded its overall deadline")
    return min(remaining, maximum)


def command(binary: Path, deadline: float, *arguments: str) -> subprocess.CompletedProcess[str]:
    argv = [str(binary), *map(str, arguments)]
    environment = os.environ.copy()
    environment.update(RUST_LOG="error", NO_COLOR="1", TERM="dumb")
    try:
        result = subprocess.run(
            argv,
            stdin=subprocess.DEVNULL,
            capture_output=True,
            text=True,
            timeout=time_left(deadline, COMMAND_TIMEOUT_SECONDS),
            env=environment,
            check=False,
        )
    except subprocess.TimeoutExpired as error:
        raise SmokeFailure(f"command timed out: {' '.join(argv)}") from error
    if result.returncode != 0:
        raise SmokeFailure(
            f"command exited {result.returncode}: {' '.join(argv)}\n"
            f"{tail(result.stdout)}\n{tail(result.stderr)}"
        )
    return result


def command_json(binary: Path, deadline: float, *arguments: str):
    result = command(binary, deadline, *arguments)
    try:
        return json.loads(result.stdout)
    except json.JSONDecodeError as error:
        raise SmokeFailure(
            f"command did not return JSON: {' '.join(map(str, arguments))}\n"
            f"{tail(result.stdout)}\n{tail(result.stderr)}"
        ) from error


class Server:
    def __init__(self, binary: Path, config: Path, root: Path, name: str, deadline: float, health_events: Path | None = None):
        self.binary = binary
        self.config = config
        self.socket_path = root / "9p.sock"
        self.log_path = root / f"{name}.log"
        self.deadline = deadline
        self.health_events = health_events
        self.process = None
        self.log = None
        self.expected_kill = False

    @property
    def target(self) -> str:
        return f"unix:{self.socket_path}"

    def __enter__(self):
        self.socket_path.unlink(missing_ok=True)
        self.log = self.log_path.open("wb")
        environment = os.environ.copy()
        # A cold restart must use persisted health state, even when this runner
        # was launched from an environment that already enables test injection.
        environment.pop("ZEROFS_TEST_DISK_HEALTH", None)
        if self.health_events is not None:
            environment["ZEROFS_TEST_DISK_HEALTH"] = str(self.health_events)
        environment.update(RUST_LOG="info", NO_COLOR="1", TERM="dumb")
        try:
            with defer_cancellation():
                self.process = subprocess.Popen(
                    [str(self.binary), "run", "--config", str(self.config)],
                    stdin=subprocess.DEVNULL,
                    stdout=self.log,
                    stderr=subprocess.STDOUT,
                    env=environment,
                    start_new_session=True,
                )
            ready_deadline = time.monotonic() + time_left(self.deadline, START_TIMEOUT_SECONDS)
            while time.monotonic() < ready_deadline:
                if self.process.poll() is not None:
                    raise SmokeFailure(
                        f"server exited before accepting 9P clients: {self.process.returncode}"
                    )
                if self.socket_path.exists():
                    try:
                        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as probe:
                            probe.settimeout(0.25)
                            probe.connect(str(self.socket_path))
                        return self
                    except OSError:
                        pass
                time.sleep(0.05)
            raise SmokeFailure("server did not accept connections before the startup deadline")
        except BaseException as error:
            self.stop(strict=False, cancelled=isinstance(error, SmokeCancelled))
            raise

    def stop(self, *, strict: bool, cancelled: bool = False) -> None:
        failure = None
        try:
            if self.process is not None:
                if self.process.poll() is None:
                    try:
                        os.killpg(self.process.pid, signal.SIGTERM)
                    except ProcessLookupError:
                        pass
                    try:
                        self.process.wait(timeout=CANCEL_SHUTDOWN_TIMEOUT_SECONDS if cancelled
                                          else SHUTDOWN_TIMEOUT_SECONDS)
                    except subprocess.TimeoutExpired:
                        try:
                            os.killpg(self.process.pid, signal.SIGKILL)
                        except ProcessLookupError:
                            pass
                        self.process.wait(timeout=5)
                        failure = "server required SIGKILL instead of graceful shutdown"
                expected_exit = self.expected_kill and self.process.returncode == -signal.SIGKILL
                if self.process.returncode != 0 and not expected_exit and failure is None:
                    failure = f"server exited with status {self.process.returncode}"
        except SmokeCancelled:
            # The first signal can arrive while an otherwise normal __exit__
            # is already waiting for TERM. Finish reaping before unwinding;
            # merely closing the log would leave the detached server alive.
            if self.process is not None:
                if self.process.poll() is None:
                    try:
                        os.killpg(self.process.pid, signal.SIGKILL)
                    except ProcessLookupError:
                        pass
                self.process.wait(timeout=5)
            raise
        finally:
            if self.log is not None:
                self.log.close()
                self.log = None
        if failure is not None:
            if strict:
                raise SmokeFailure(failure)
            print(f"cleanup: {failure}", file=sys.stderr)

    def crash(self) -> None:
        require(self.process is not None and self.process.poll() is None,
                "crash fixture requires a running server")
        os.killpg(self.process.pid, signal.SIGKILL)
        self.process.wait(timeout=5)
        require(self.process.returncode == -signal.SIGKILL,
                f"server did not die from the injected SIGKILL: {self.process.returncode}")
        self.expected_kill = True

    def __exit__(self, exception_type, _exception, _traceback):
        self.stop(strict=exception_type is None,
                  cancelled=exception_type is not None and issubclass(exception_type, SmokeCancelled))
        return False


def write_config(root: Path, members: list[tuple[str, int]]) -> Path:
    # JSON strings are valid TOML basic strings for these generated ASCII paths.
    quote = json.dumps
    config = root / "zerofs.toml"
    sections = [
        "[runtime]",
        "memory_limit_gb = 48.0",
        "",
        "[cache]",
        f"dir = {quote(str(root / 'cache'))}",
        "disk_size_gb = 0.25",
        "memory_size_gb = 0.064",
        "",
        "[storage]",
        f"url = {quote((root / 'metadata').as_uri())}",
        'encryption_password = "local-pool-ci-only"',
        "",
        "[storage.pool]",
        f"catalog_dir = {quote(str(root / 'catalog'))}",
    ]
    for member_id, capacity in members:
        sections.extend(
            [
                "",
                "[[storage.pool.members]]",
                f"id = {quote(member_id)}",
                f"url = {quote((root / member_id).as_uri())}",
                f"capacity_gb = {capacity / 1_000_000_000:.12f}",
                "reserve_free_gb = 0.0",
            ]
        )
    sections.extend(
        [
            "",
            "[filesystem]",
            'compression = "lz4"',
            "",
            "[servers]",
            "",
            "[servers.rpc]",
            f"unix_socket = {quote(str(root / 'admin.sock'))}",
            "",
            "[servers.ninep]",
            f"unix_socket = {quote(str(root / '9p.sock'))}",
            "",
            "[telemetry]",
            "enabled = false",
            "",
        ]
    )
    config.write_text("\n".join(sections), encoding="utf-8")
    return config


def status_members(status: dict) -> dict[str, dict]:
    require(status["pending_operations"] == 0, f"pool still has pending operations: {status}")
    members = {member["id"]: member for member in status["members"]}
    for member in members.values():
        require(member["reserved_bytes"] == 0, f"reservation leaked after shutdown: {member}")
        require(
            0 <= member["used_bytes"] <= member["capacity_bytes"],
            f"member exceeded its capacity: {member}",
        )
    return members


def file_sha256(path: Path) -> str:
    with path.open("rb") as source:
        digest = hashlib.sha256()
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
        return digest.hexdigest()


def write_receipt(path: Path, receipt: dict) -> None:
    """Replace an earlier result atomically so a failed rerun cannot leave a pass."""
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", dir=path.parent,
                                         prefix=f".{path.name}.", delete=False) as output:
            temporary = Path(output.name)
            json.dump(receipt, output, indent=2, sort_keys=True)
            output.write("\n")
        temporary.replace(path)
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)


def fixture(fixture_id: str, **evidence) -> dict:
    return {"fixture_id": fixture_id, "result": "passed", **evidence}


def segment_location_identity(location: str) -> tuple[str, str]:
    """Match Segid::from_object_key's fixed-width lowercase identity and shard."""
    prefix, separator, suffix = location.rpartition("/segments/")
    if not separator:
        require(location.startswith("segments/"), f"noncanonical segment location: {location}")
        prefix, suffix = "", location.removeprefix("segments/")
    parts = suffix.split("/")
    require(len(parts) == 3 and tuple(map(len, parts)) == (2, 16, 16)
            and all(character in "0123456789abcdef" for part in parts for character in part),
            f"noncanonical segment location: {location}")
    counter = int(parts[2], 16)
    require(int(parts[0], 16) == counter & 0xff,
            f"segment path shard disagrees with its counter: {location}")
    return prefix, f"segments/{suffix}"


def segment_footer_identity(body: Path, size: int) -> str:
    """Read identity and bounds using production parse_footer's v1 layout."""
    require(size >= SEGMENT_FOOTER.size, f"segment body is smaller than its footer: {body}")
    with body.open("rb") as source:
        source.seek(-SEGMENT_FOOTER.size, os.SEEK_END)
        footer = source.read(SEGMENT_FOOTER.size)
    require(len(footer) == SEGMENT_FOOTER.size, f"truncated segment footer: {body}")
    (magic, version, _frames, directory_offset, directory_length, epoch, counter,
     _sealed_sequence, total_length, _crc, reserved) = SEGMENT_FOOTER.unpack(footer)
    require(magic == b"ZSEG", f"bad segment footer magic: {body}")
    require(version == 1, f"unsupported segment footer version {version}: {body}")
    require(total_length == size, f"segment footer length disagrees with its body: {body}")
    require(reserved == 0, f"nonzero reserved segment footer bytes: {body}")
    require(directory_offset + directory_length + SEGMENT_FOOTER.size == total_length,
            f"segment directory does not abut its footer: {body}")
    return f"segments/{counter & 0xff:02x}/{epoch:016x}/{counter:016x}"


def physical_snapshot(root: Path, status: dict, logical_prefix: str) -> list[dict]:
    """Derive a live segment map from immutable bodies, without a catalog read."""
    bodies = []
    identities = set()
    for member in status["members"]:
        member_root = root / member["id"]
        directory = member_root / ".zerofs-pool" / status["pool_id"] / "objects"
        if directory.exists():
            for body in directory.rglob("*"):
                if body.is_file() and body.name != NATIVE_MUTATION_LOCK_NAME:
                    size = body.stat().st_size
                    segment_id = segment_footer_identity(body, size)
                    require(segment_id not in identities,
                            f"physical bodies repeat a logical segment identity: {segment_id}")
                    identities.add(segment_id)
                    location = f"{logical_prefix}/{segment_id}" if logical_prefix else segment_id
                    bodies.append({"segment_id": segment_id, "location": location,
                                   "member_id": member["id"],
                                   "physical_key": body.relative_to(member_root).as_posix(),
                                   "size": size, "sha256": file_sha256(body)})
    return sorted(bodies, key=lambda body: body["segment_id"])


def verify_physical_manifest(physical: list[dict], placements: list[dict], phase: str) -> list[dict]:
    """Compare every body-derived identity and placement with pool-list API JSON."""
    manifest = []
    identities = set()
    for entry in placements:
        _, segment_id = segment_location_identity(entry["location"])
        require(segment_id not in identities,
                f"API map repeats a logical segment identity: {segment_id}")
        identities.add(segment_id)
        require(entry["physical_state"] == "present" and entry["sha256"] is not None,
                f"API map has an unverified body during {phase}: {entry}")
        manifest.append({"segment_id": segment_id,
                         **{key: entry[key] for key in
                            ("location", "member_id", "physical_key", "size", "sha256")}})
    manifest.sort(key=lambda entry: entry["segment_id"])
    require(manifest == physical,
            f"{phase} changed the exact segment identity/device/key/size/SHA manifest")
    return manifest


def placement_snapshot(binary: Path, deadline: float, root: Path, config: Path,
                       *, allow_unreadable: bool = False) -> dict:
    """Check the public map against independent backing-file hashes while stopped."""
    status = command_json(binary, deadline, "pool", "status", "--config", config)
    members = status_members(status)
    placements = command_json(binary, deadline, "pool", "list", "--config", config)
    require(len({entry["location"] for entry in placements}) == len(placements),
            "placement JSON assigns a segment more than once")
    require(len(placements) == status["mapped_objects"], "map and status object counts differ")
    faults = {(fault["member_id"], fault["physical_key"]) for fault in status["unreadable"]}
    physical = set()
    counts = Counter()
    verified = []
    for entry in placements:
        member = entry["member_id"]
        require(member in members, f"unexpected placement member: {entry}")
        key = Path(entry["physical_key"])
        require(not key.is_absolute() and ".." not in key.parts,
                f"physical key escapes its member: {entry}")
        identity = (member, entry["physical_key"])
        require(identity not in physical, f"two logical segments share a physical body: {entry}")
        physical.add(identity)
        counts[member] += 1
        body = root / member / key
        if not body.exists():
            require(allow_unreadable and identity in faults,
                    f"mapped segment has no physical body or recorded fault: {entry}")
            verified.append({**entry, "sha256": None, "physical_state": "missing_quarantined"})
        else:
            require(body.is_file() and body.stat().st_size == entry["size"],
                    f"physical body size disagrees with placement: {entry}")
            verified.append({**entry, "sha256": file_sha256(body), "physical_state": "present"})
        for other in members:
            if other != member:
                require(not (root / other / key).exists(),
                        f"segment physical key also exists on another root: {entry}, {other}")
    actual = set()
    for member, report in members.items():
        require(counts[member] == report["objects"], f"per-member map count disagrees: {member}")
        directory = root / member / ".zerofs-pool" / status["pool_id"] / "objects"
        if directory.exists():
            for body in directory.rglob("*"):
                # Native backend coordination locks are not segment bodies.
                if body.is_file() and body.name != NATIVE_MUTATION_LOCK_NAME:
                    actual.add((member, body.relative_to(root / member).as_posix()))
    expected_present = {(entry["member_id"], entry["physical_key"])
                        for entry in verified if entry["physical_state"] == "present"}
    require(actual == expected_present,
            f"backing roots contain untracked bodies or omit mapped bodies: {actual ^ expected_present}")
    return {"status": status, "placements": sorted(verified, key=lambda entry: entry["location"])}


def verify_remapped(before: dict, after: dict, source: str, target: str,
                    *, allow_new: bool = False, allow_unreadable: bool = False) -> None:
    old = {entry["location"]: entry for entry in before["placements"]}
    new = {entry["location"]: entry for entry in after["placements"]}
    require(old.keys() <= new.keys() if allow_new else old.keys() == new.keys(),
            "migration changed the logical segment set")
    for location, previous in old.items():
        current = new[location]
        if current["physical_state"] == "missing_quarantined":
            require(allow_unreadable and current["member_id"] == previous["member_id"]
                    and current["physical_key"] == previous["physical_key"],
                    f"unreadable segment was remapped or falsely marked copied: {current}")
            continue
        require(current["sha256"] == previous["sha256"] and current["size"] == previous["size"],
                f"migration changed segment bytes: {location}")
        expected_member = target if previous["member_id"] == source else previous["member_id"]
        require(current["member_id"] == expected_member, f"unexpected remap: {current}")


def inject_smart(events: Path, **changes) -> dict:
    sample = {"device": "fixture", "pending": 0, "reallocated": 0,
              "uncorrectable": 0, "crc": 0, "failed": False, **changes}
    temporary = events.with_suffix(".next")
    temporary.write_text(json.dumps({"disk1": sample}), encoding="utf-8")
    temporary.replace(events)
    return sample


def wait_status(binary: Path, deadline: float, endpoint: str, predicate,
                description: str) -> dict:
    while True:
        status = command_json(binary, deadline, "pool", "status", "--endpoint", endpoint)
        if predicate(status):
            return status
        require(time.monotonic() < deadline, f"timed out waiting for {description}: {status}")
        time.sleep(0.1)


@contextmanager
def hold_native_mutation_lock(root: Path, member_id: str, pool_id: str):
    """Pause only this fixture's destination PUT, using the backend's OS lock."""
    lock_path = root / member_id / ".zerofs-pool" / pool_id / "objects" / NATIVE_MUTATION_LOCK_NAME
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    # fs4 0.13.1's Unix FileExt::lock_exclusive uses rustix::fs::flock.
    # Separate descriptors therefore contend with fcntl.flock on Linux.
    with lock_path.open("a+b") as lock:
        try:
            fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as error:
            raise SmokeFailure("destination native mutation lock was already held before health injection") from error
        try:
            yield lock_path
        finally:
            # This inner finally runs before health_case's Server cleanup,
            # including when an admin request or assertion fails.
            fcntl.flock(lock.fileno(), fcntl.LOCK_UN)


def observe_automatic_evacuation(binary: Path, deadline: float, endpoint: str,
                                 root: Path, before: dict, changes: dict) -> dict:
    """Require responsive live status while an automatic copy is held in PUT."""
    source_id, target_id = "disk1", "disk2"
    original = {member["id"]: member for member in before["members"]}

    def running(status):
        members = {member["id"]: member for member in status["members"]}
        source, target = members[source_id], members[target_id]
        return (source["health"]["quarantine"] is not None and source["evacuation"] == "running"
                and target["pending_uploads"] > 0 and target["reserved_bytes"] > 0
                and status["pending_operations"] > 0)

    def verify_paused(status):
        require(running(status), "automatic evacuation stopped running while its destination PUT was paused")
        require(status["mapped_objects"] == before["mapped_objects"],
                "paused evacuation changed the mapped segment count")
        for member in status["members"]:
            require(member["objects"] == original[member["id"]]["objects"]
                    and member["used_bytes"] == original[member["id"]]["used_bytes"],
                    "evacuation published a remap while its destination PUT was paused")
            if member["id"] == source_id:
                require(not member["safe_to_remove"] and not member["safe_to_pull_with_known_loss"],
                        "paused evacuation reported the source as pull-ready")

    with hold_native_mutation_lock(root, target_id, before["pool_id"]) as lock_path:
        started = time.monotonic()
        injected = inject_smart(root / "health.json", **changes)
        quarantined = wait_status(binary, deadline, endpoint,
                                  lambda s: s["members"][0]["health"]["quarantine"] is not None,
                                  "durable quarantine after the injected health signal")
        active = wait_status(binary, deadline, endpoint, running,
                             "automatic evacuation with a reserved destination PUT")
        verify_paused(active)
        running_after = time.monotonic() - started
        samples = []
        for index in range(RUNNING_STATUS_SAMPLES):
            requested = time.monotonic()
            status = command_json(binary, min(deadline, requested + RUNNING_STATUS_TIMEOUT_SECONDS),
                                  "pool", "status", "--endpoint", endpoint)
            received = time.monotonic()
            verify_paused(status)
            samples.append({"started_after_signal_seconds": requested - started,
                            "request_seconds": received - requested, "status": status})
            if index + 1 < RUNNING_STATUS_SAMPLES:
                time.sleep(time_left(deadline, 0.1))
        pause = {"reason": "destination native mutation lock", "member_id": target_id,
                 "physical_key": lock_path.relative_to(root / target_id).as_posix(),
                 "signal_to_running_seconds": running_after,
                 "held_seconds": time.monotonic() - started,
                 "status_request_timeout_seconds": RUNNING_STATUS_TIMEOUT_SECONDS,
                 "status_samples": samples}
    return {"observation": injected, "quarantined": quarantined, "running": active, "paused_copy": pause}


def download_and_verify(binary: Path, deadline: float, server: Server, root: Path,
                        phase: str, expected: dict[str, str]) -> None:
    for name, digest in expected.items():
        destination = root / f"{phase}-{name}"
        command(binary, deadline, "download", server.target, f"/{name}", destination, "--jobs", "1")
        require(destination.stat().st_size == PAYLOAD_BYTES, f"wrong size for {phase}/{name}")
        observed = hashlib.sha256(destination.read_bytes()).hexdigest()
        require(observed == digest, f"SHA-256 mismatch for {phase}/{name}: {observed} != {digest}")


def smoke(binary: Path, root: Path) -> dict:
    deadline = time.monotonic() + TOTAL_TIMEOUT_SECONDS
    for directory in ("metadata", "cache", "disk1", "disk2", "disk3", "sources"):
        (root / directory).mkdir(mode=0o700)
    require(len(os.fsencode(root / "9p.sock")) < 104, "temporary Unix socket path is too long")
    config = write_config(root, [("disk1", FIRST_MEMBER_BYTES), ("disk2", OTHER_MEMBER_BYTES)])
    expected = {}
    for name in ("first.bin", "second.bin"):
        data = hashlib.shake_256(f"zerofs-local-pool-smoke/{name}".encode()).digest(PAYLOAD_BYTES)
        (root / "sources" / name).write_bytes(data)
        expected[name] = hashlib.sha256(data).hexdigest()

    print("Initialize a native local storage pool", flush=True)
    initialized = command_json(binary, deadline, "pool", "init", "--config", config)
    require(initialized["mapped_objects"] == 0, "new pool unexpectedly contains segment placements")
    print("Upload and verify two files over the real 9P server", flush=True)
    with Server(binary, config, root, "server-before-migration", deadline) as server:
        for name in expected:
            command(binary, deadline, "upload", server.target, root / "sources" / name,
                    f"/{name}", "--jobs", "1")
        download_and_verify(binary, deadline, server, root, "before", expected)

    original = placement_snapshot(binary, deadline, root, config)
    before = original["status"]
    before_members = status_members(before)
    require(before_members["disk1"]["objects"] > 0, "first local member received no segments")
    require(before_members["disk2"]["objects"] > 0, "quota did not spill segments to the second member")
    require(before["mapped_objects"] >= 2, "two separately synced uploads did not create pool placements")
    logical_prefixes = {segment_location_identity(entry["location"])[0]
                        for entry in original["placements"]}
    require(len(logical_prefixes) == 1, "fixture placements do not share one database prefix")
    logical_prefix = logical_prefixes.pop()
    verify_physical_manifest(physical_snapshot(root, before, logical_prefix),
                             original["placements"], "initial stopped-server snapshot")

    print("Upload a new file, SIGKILL its writer, and verify durable placement and cold reads", flush=True)
    with Server(binary, config, root, "server-before-crash", deadline) as server:
        # This write must not receive a graceful shutdown before the crash. A
        # read-only restart of an already-stopped writer would miss publication
        # durability defects. The CLI upload fsyncs before acknowledging success.
        data = hashlib.shake_256(b"zerofs-local-pool-smoke/crash.bin").digest(PAYLOAD_BYTES)
        (root / "sources" / "crash.bin").write_bytes(data)
        expected["crash.bin"] = hashlib.sha256(data).hexdigest()
        command(binary, deadline, "upload", server.target, root / "sources" / "crash.bin",
                "/crash.bin", "--jobs", "1")
        download_and_verify(binary, deadline, server, root, "pre-crash", expected)
        before_kill = command_json(binary, deadline, "pool", "status", "--endpoint",
                                   f"unix:{root / 'admin.sock'}")
        status_members(before_kill)
        require(before_kill["mapped_objects"] > original["status"]["mapped_objects"],
                "killed writer did not publish a new segment")
        before_kill_bodies = physical_snapshot(root, before_kill, logical_prefix)
        require(len(before_kill_bodies) == before_kill["mapped_objects"],
                "live committed placements do not match the backing-body count")
        server.crash()
    crashed = placement_snapshot(binary, deadline, root, config)
    require(crashed["status"]["mapped_objects"] == before_kill["mapped_objects"],
            "SIGKILL lost a committed placement from the killed writer")
    require(physical_snapshot(root, crashed["status"], logical_prefix) == before_kill_bodies,
            "SIGKILL changed committed backing bodies or their bytes")
    crashed_manifest = verify_physical_manifest(before_kill_bodies, crashed["placements"], "SIGKILL")
    crashed_map = {entry["location"]: entry for entry in crashed["placements"]}
    require(all(crashed_map.get(entry["location"]) == entry for entry in original["placements"]),
            "SIGKILL changed an existing placement or its bytes")
    shutil.rmtree(root / "cache")
    (root / "cache").mkdir(mode=0o700)
    with Server(binary, config, root, "server-after-crash", deadline) as server:
        download_and_verify(binary, deadline, server, root, "post-crash", expected)
    restarted = placement_snapshot(binary, deadline, root, config)
    require(restarted["placements"] == crashed["placements"], "cold restart changed committed placement or bytes")
    restarted_manifest = verify_physical_manifest(before_kill_bodies, restarted["placements"],
                                                 "cold restart")
    before = crashed["status"]
    before_members = status_members(before)

    print("Add a third disk and migrate the first disk's segments while the server is stopped", flush=True)
    config = write_config(root, [
        ("disk1", FIRST_MEMBER_BYTES),
        ("disk2", OTHER_MEMBER_BYTES),
        ("disk3", OTHER_MEMBER_BYTES),
    ])
    migration = command_json(binary, deadline, "pool", "migrate", "--config", config,
                             "--from", "disk1", "--to", "disk3", "--limit", "128")
    require(migration["migrated_segments"] == before_members["disk1"]["objects"],
            "migration did not move every segment from the first member")
    moved = placement_snapshot(binary, deadline, root, config)
    migrated = moved["status"]
    verify_remapped(crashed, moved, "disk1", "disk3")
    migrated_members = status_members(migrated)
    require(migrated["pool_id"] == initialized["pool_id"], "expansion changed the pool identity")
    require(migrated["mapped_objects"] == before["mapped_objects"], "migration changed the segment count")
    require(migrated_members["disk1"]["objects"] == 0, "source member still has live placements")
    require(migrated_members["disk1"]["used_bytes"] == 0, "source cleanup did not release capacity")
    require(migrated_members["disk3"]["objects"] == before_members["disk1"]["objects"],
            "destination member did not receive the migrated placements")
    require(migrated_members["disk2"]["objects"] == before_members["disk2"]["objects"],
            "expansion unexpectedly changed the second member's placements")

    # The cache is disposable and entirely owned by this test. Clearing it makes
    # the next hashes prove reads through the new placement catalog and disks.
    shutil.rmtree(root / "cache")
    (root / "cache").mkdir(mode=0o700)
    print("Restart with an empty read cache and verify all files from their current disks", flush=True)
    with Server(binary, config, root, "server-after-migration", deadline) as server:
        download_and_verify(binary, deadline, server, root, "after", expected)
    final_snapshot = placement_snapshot(binary, deadline, root, config)
    final = final_snapshot["status"]
    status_members(final)
    require(final["pool_id"] == initialized["pool_id"], "restart changed the pool identity")
    require(final_snapshot["placements"] == moved["placements"], "restart changed migrated placement or bytes")
    return {
        "result": "passed",
        "bytes_per_file": PAYLOAD_BYTES,
        "sha256": expected,
        "migrated_segments": migration["migrated_segments"],
        "members": final["members"],
        "fixtures": [
            fixture("fx-place-map-crash", signal="SIGKILL", roots=2,
                    crash_scope="committed writes in the killed process; no in-flight migration claim",
                    before=original, before_kill_status=before_kill, before_kill_bodies=before_kill_bodies,
                    before_kill_manifest_source="physical ZSEG v1 footers and backing-file SHA-256; not live catalog API JSON",
                    after_manifest_source="pool-list API JSON with independently verified backing-file SHA-256",
                    after_kill_manifest=crashed_manifest, after_restart_manifest=restarted_manifest,
                    after_kill=crashed, after_restart=restarted,
                    application_sha256=expected),
            fixture("fx-evacuate-remap-sha", case="explicit_stopped_server_migration",
                    before=crashed, after=final_snapshot, application_sha256=expected),
            fixture("fx-exports-unchanged", protocol="9p", transport="unix_socket",
                    coverage="real client upload/download; no kernel mount or NFS/NBD claim",
                    application_sha256=expected),
        ],
    }


def health_case(binary: Path, root: Path, trigger: str) -> dict:
    root.mkdir()
    deadline = time.monotonic() + TOTAL_TIMEOUT_SECONDS
    for directory in ("metadata", "cache", "disk1", "disk2", "sources"):
        (root / directory).mkdir(mode=0o700)
    config = write_config(root, [("disk1", OTHER_MEMBER_BYTES), ("disk2", OTHER_MEMBER_BYTES)])
    command_json(binary, deadline, "pool", "init", "--config", config)
    events = root / "health.json"
    baseline = inject_smart(events)
    expected = {}
    for name in ("first.bin", "second.bin"):
        data = hashlib.shake_256(f"health/{trigger}/{name}".encode()).digest(PAYLOAD_BYTES)
        (root / "sources" / name).write_bytes(data)
        expected[name] = hashlib.sha256(data).hexdigest()
    endpoint = f"unix:{root / 'admin.sock'}"
    print(f"Exercise automatic evacuation after {trigger}", flush=True)
    with Server(binary, config, root, "server-health-upload", deadline, events) as server:
        for name in expected:
            command(binary, deadline, "upload", server.target, root / "sources" / name, f"/{name}", "--jobs", "1")
    original = placement_snapshot(binary, deadline, root, config)
    before = original["status"]
    require(before["members"][0]["objects"] >= 2, "health fixture needs multiple source segments")
    require(before["members"][1]["objects"] == 0, "health fixture unexpectedly used destination disk")
    warning_status = None
    stable_warning_status = None
    with Server(binary, config, root, "server-health", deadline, events) as server:
        wait_status(binary, deadline, endpoint,
                    lambda s: s["members"][0]["health"]["last_sample"] == baseline,
                    "healthy SMART baseline")
        if trigger == "crc_repeat":
            warning_sample = inject_smart(events, crc=1)
            warning_status = wait_status(binary, deadline, endpoint,
                                        lambda s: s["members"][0]["health"]["last_sample"] == warning_sample,
                                        "first CRC observation")
            warning = warning_status["members"][0]
            require(warning["health"]["warning"] is not None and warning["health"]["crc_increases"] == 1,
                    "first CRC increase was not reported as a warning")
            require(warning["health"]["quarantine"] is None and warning["objects"] == before["mapped_objects"],
                    "first CRC increase quarantined or evacuated the source")
            observed = warning["health"]["observed_ms"]
            stable_warning_status = wait_status(
                binary, deadline, endpoint,
                lambda s: (s["members"][0]["health"]["observed_ms"] or 0) >= observed + 300,
                "repeated samples of the unchanged CRC counter")
            stable = stable_warning_status["members"][0]
            require(stable["health"]["crc_increases"] == 1 and stable["health"]["quarantine"] is None,
                    "replaying an unchanged CRC counter triggered evacuation")
            require(stable["objects"] == before["mapped_objects"], "warning-only samples moved source segments")
            signal_changes = {"crc": 2}
        else:
            signal_changes = {trigger: 1}
        live = observe_automatic_evacuation(binary, deadline, endpoint, root, before, signal_changes)
        injected, quarantined = live["observation"], live["quarantined"]
        completed = wait_status(binary, deadline, endpoint,
                                lambda s: (s["members"][0]["safe_to_remove"]
                                           and s["members"][0]["evacuation"] == "complete"),
                                "automatic evacuation without an operator migrate command")
        source = completed["members"][0]
        require(source["objects"] == 0 and source["used_bytes"] == 0,
                "safe removal with remaining source placements or bytes")
        require(source["evacuation"] == "complete", "completed evacuation has an inconsistent status")
        require(not completed["quarantined_leftovers"] and not completed["unreadable"],
                "successful evacuation retained unexplained leftovers")
        require(completed["members"][1]["objects"] == before["mapped_objects"],
                "automatic migration lost placements")
        data = hashlib.shake_256(f"health/{trigger}/after-quarantine".encode()).digest(PAYLOAD_BYTES)
        (root / "sources" / "after.bin").write_bytes(data)
        expected["after.bin"] = hashlib.sha256(data).hexdigest()
        command(binary, deadline, "upload", server.target, root / "sources" / "after.bin", "/after.bin", "--jobs", "1")
        after = command_json(binary, deadline, "pool", "status", "--endpoint", endpoint)
        require(after["members"][0]["objects"] == 0, "new application write reached quarantined disk")
        require(after["members"][1]["objects"] > before["mapped_objects"],
                "new application write did not seal a fresh segment")
    moved = placement_snapshot(binary, deadline, root, config)
    verify_remapped(original, moved, "disk1", "disk2", allow_new=True)
    shutil.rmtree(root / "cache")
    (root / "cache").mkdir(mode=0o700)
    with Server(binary, config, root, "server-health-restart", deadline) as server:
        download_and_verify(binary, deadline, server, root, "health-cold", expected)
        status = command_json(binary, deadline, "pool", "status", "--endpoint", endpoint)
        require(status["members"][0]["health"]["quarantine"] is not None, "restart lost quarantine")
        report = command_json(binary, deadline, "pool", "affected", "--endpoint", endpoint, "--member", "disk1")
        require(report["complete"], f"affected report incomplete: {report}")
    final = placement_snapshot(binary, deadline, root, config)
    require(final["placements"] == moved["placements"], "restart changed evacuated placement or bytes")
    fixtures = [
        fixture("fx-smart-quarantine", case=trigger, source="injected SMART sample",
                observation=injected, quarantined=quarantined, after_new_write=after),
        fixture("fx-auto-evacuate", case=trigger, operator_migrate_commands=0,
                observed=quarantined, running=live["running"], paused_copy=live["paused_copy"],
                completed=completed),
        fixture("fx-evacuate-remap-sha", case=trigger, before=original, after=final,
                application_sha256=expected),
        fixture("fx-pull-ready", case=trigger, status=completed),
    ]
    if warning_status is not None:
        fixtures.append(fixture("fx-udma-crc-warn", first_increase=warning_status,
                                unchanged_samples=stable_warning_status, repeated_increase=quarantined))
    return {"signal": trigger, "sha256": expected, "fixtures": fixtures}


def health_smoke(binary: Path, root: Path) -> dict:
    cases = [health_case(binary, root / trigger, trigger)
             for trigger in ("pending", "uncorrectable", "reallocated", "crc_repeat")]
    return {
        "result": "passed", "automatic_evacuation": True, "cold_restart": True,
        "signal_cases": [{"signal": case["signal"], "sha256": case["sha256"]} for case in cases],
        "signal_scope": "SMART injection; native kernel attribution is covered by Rust tests",
        "fixtures": [evidence for case in cases for evidence in case["fixtures"]],
    }


def verify_settled_loss_status(status: dict, original: dict, damaged: dict,
                               *, no_space: bool, phase: str) -> None:
    """Apply the same loss/capacity contract before and after cold reopening."""
    members = status_members(status)
    source, destination = members["disk1"], members["disk2"]
    require(source["health"]["quarantine"] is not None, f"{phase}: source lost quarantine")
    require(not source["safe_to_remove"], f"{phase}: loss was marked safe to remove")
    require(source["safe_to_pull_with_known_loss"] == (not no_space),
            f"{phase}: known-loss removal assessment does not match remaining readable data")
    require(source["readable_evacuated"] == (not no_space),
            f"{phase}: readable evacuation assessment does not match remaining source data")
    expected_state = "cannot_complete_capacity" if no_space else "readable_evacuated_loss_blocked"
    require(source["evacuation"] == expected_state,
            f"{phase}: expected {expected_state}, got {source['evacuation']}")

    identity = lambda entry: (entry["location"], entry["member_id"], entry["physical_key"])
    require([identity(entry) for entry in status["unreadable"]] == [identity(damaged)],
            f"{phase}: unreadable report does not identify exactly the planted source body")
    require(source["unreadable_objects"] == 1,
            f"{phase}: source unreadable count does not match the planted loss")
    expected_leftovers = ([entry for entry in original["placements"] if entry["member_id"] == "disk1"]
                          if no_space else [damaged])
    actual_leftovers = sorted((*identity(entry), entry["size"]) for entry in status["quarantined_leftovers"])
    expected_leftovers = sorted((*identity(entry), entry["size"]) for entry in expected_leftovers)
    require(actual_leftovers == expected_leftovers,
            f"{phase}: quarantined leftovers changed exact segment identity, device, key, or bytes")
    require(source["objects"] == len(expected_leftovers)
            and source["used_bytes"] == sum(entry[3] for entry in expected_leftovers),
            f"{phase}: remaining source counts or bytes hide unexplained data")
    require(status["mapped_objects"] == original["status"]["mapped_objects"],
            f"{phase}: loss recovery changed the mapped segment count")
    if no_space:
        original_source = next(member for member in original["status"]["members"] if member["id"] == "disk1")
        require(source["objects"] == original["status"]["mapped_objects"],
                f"{phase}: no-space recovery lost source placements")
        capacity = source["evacuation_capacity"]
        require(not capacity["sufficient"] and capacity["available_bytes"] < capacity["required_bytes"],
                f"{phase}: insufficient space was not reported as cannot-complete: {capacity}")
        require(capacity["required_bytes"] == original_source["used_bytes"]
                and capacity["reserved_destination_bytes"] == 0,
                f"{phase}: capacity assessment does not account for the exact source bytes: {capacity}")
        require(capacity["available_bytes"] == 8 and destination["available_bytes"] == 8,
                f"{phase}: capacity assessment does not match the fixture's 8-byte destination quota: {capacity}")
        require(destination["objects"] == 0 and destination["used_bytes"] == 0,
                f"{phase}: no-space fixture unexpectedly remapped a segment or retained destination bytes")


def wait_settled_loss_status(binary: Path, deadline: float, endpoint: str, original: dict,
                             damaged: dict, *, no_space: bool, phase: str) -> dict:
    def settled(status):
        source = next(member for member in status["members"] if member["id"] == "disk1")
        return (status["pending_operations"] == 0 and bool(status["unreadable"])
                and (source["evacuation"] == "cannot_complete_capacity" if no_space
                     else source["safe_to_pull_with_known_loss"]))

    status = wait_status(binary, deadline, endpoint, settled, f"{phase} settled partial-loss recovery")
    verify_settled_loss_status(status, original, damaged, no_space=no_space, phase=phase)
    return status


def loss_smoke(binary: Path, root: Path, no_space: bool = False) -> dict:
    root.mkdir()
    deadline = time.monotonic() + TOTAL_TIMEOUT_SECONDS
    for directory in ("metadata", "cache", "disk1", "disk2", "disk3", "sources"):
        (root / directory).mkdir(mode=0o700)
    config = write_config(root, [("disk1", OTHER_MEMBER_BYTES), ("disk2", 8 if no_space else OTHER_MEMBER_BYTES)])
    command_json(binary, deadline, "pool", "init", "--config", config)
    events = root / "health.json"
    events.write_text("{}")
    expected = {}
    missing_body = None
    with Server(binary, config, root, "server-loss-upload", deadline, events) as server:
        for name in ("first.bin", "second.bin"):
            data = hashlib.shake_256(f"loss/{name}".encode()).digest(PAYLOAD_BYTES)
            (root / "sources" / name).write_bytes(data)
            expected[name] = hashlib.sha256(data).hexdigest()
            command(binary, deadline, "upload", server.target, root / "sources" / name, f"/{name}", "--jobs", "1")
            if missing_body is None:
                bodies = list((root / "disk1" / ".zerofs-pool").glob("*/objects/*"))
                require(bool(bodies), "first upload created no physical body")
                missing_body = max(bodies, key=lambda p: p.stat().st_size)
        command(binary, deadline, "checkpoint", "create", "--config", config, "before-loss")
    require(missing_body is not None, "missing source fixture")
    original = placement_snapshot(binary, deadline, root, config)
    missing_key = missing_body.relative_to(root / "disk1").as_posix()
    damaged = [entry for entry in original["placements"] if entry["physical_key"] == missing_key]
    require(len(damaged) == 1, "planted unreadable body has no unique catalog placement")
    missing_body.unlink()
    inject_smart(events, pending=1)
    shutil.rmtree(root / "cache")
    (root / "cache").mkdir(mode=0o700)
    endpoint = f"unix:{root / 'admin.sock'}"
    with Server(binary, config, root, "server-loss-recovery", deadline, events) as server:
        if no_space:
            failed_read = subprocess.run([str(binary), "download", server.target, "/first.bin", str(root / "first-failed-read"), "--jobs", "1"], capture_output=True, timeout=time_left(deadline, COMMAND_TIMEOUT_SECONDS))
            require(failed_read.returncode != 0, "missing body unexpectedly readable")
        settled = wait_settled_loss_status(binary, deadline, endpoint, original, damaged[0],
                                          no_space=no_space, phase="initial recovery")
        download_and_verify(binary, deadline, server, root, "loss-healthy", {"second.bin": expected["second.bin"]})
        report = command_json(binary, deadline, "pool", "affected", "--endpoint", endpoint, "--member", "disk1")
        require(report["complete"], f"loss report incomplete: {report}")
        refs = [r for v in report["views"] for r in v["references"]]
        damaged_refs = [r for r in refs if list(b"/first.bin") in r["paths"]]
        require(bool(damaged_refs), "affected report omitted damaged pathname")
        require(any(r["inode"] > 0 and r["segment"].startswith("segments/")
                    and damaged[0]["location"].endswith("/" + r["segment"]) for r in damaged_refs),
                "affected report did not associate the damaged pathname and inode with its unreadable segment")
        require(any("before-loss" in v["view"] and v["references"] for v in report["views"]), "affected report omitted retained checkpoint")
        failed_read = subprocess.run([str(binary), "download", server.target, "/first.bin", str(root / "damaged-download"), "--jobs", "1"], capture_output=True, timeout=time_left(deadline, COMMAND_TIMEOUT_SECONDS))
        require(failed_read.returncode != 0, "damaged file silently read successfully")
    recovered = placement_snapshot(binary, deadline, root, config, allow_unreadable=True)
    verify_settled_loss_status(recovered["status"], original, damaged[0],
                              no_space=no_space, phase="recovered snapshot")
    verify_remapped(original, recovered, "disk1", "disk1" if no_space else "disk2", allow_unreadable=True)
    if no_space:
        before_map = [(e["location"], e["member_id"], e["physical_key"]) for e in original["placements"]]
        after_map = [(e["location"], e["member_id"], e["physical_key"]) for e in recovered["placements"]]
        require(before_map == after_map, "insufficient-space fixture changed the placement map")
    shutil.rmtree(root / "cache")
    (root / "cache").mkdir(mode=0o700)
    with Server(binary, config, root, "server-loss-restart", deadline) as server:
        restarted = wait_settled_loss_status(binary, deadline, endpoint, original, damaged[0],
                                            no_space=no_space, phase="cold restart")
        download_and_verify(binary, deadline, server, root, "loss-restart-healthy", {"second.bin": expected["second.bin"]})
    final = placement_snapshot(binary, deadline, root, config, allow_unreadable=True)
    verify_settled_loss_status(final["status"], original, damaged[0],
                              no_space=no_space, phase="final snapshot")
    require(final["placements"] == recovered["placements"], "restart changed recovered placement or bytes")
    fixture_id = "fx-freespace-gate" if no_space else "fx-evacuate-unreadable"
    evidence = [fixture(fixture_id, before=original, after=final, affected=report,
                        settled_status=settled, restart_status=restarted,
                        damaged_segment=damaged[0]["location"], damaged_inode_references=damaged_refs,
                        healthy_sha256=expected["second.bin"], damaged_download_exit=failed_read.returncode)]
    if not no_space:
        evidence.append(fixture("fx-pull-ready", case="known_loss", status=final["status"]))
    return {"partial_loss_no_space" if no_space else "partial_loss": True,
            "healthy_sha256": expected["second.bin"], "checkpoint_affected": True, "fixtures": evidence}


def run_smoke() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", required=True, type=Path, help="built ZeroFS executable")
    parser.add_argument("--health-recovery", action="store_true", help="requires a disk-health-test-hooks build")
    parser.add_argument("--receipt", type=Path, help="write machine-readable fixture evidence to this JSON file")
    arguments = parser.parse_args()
    provenance = {"cut_date": "2026-11-06", "started_at": datetime.now(timezone.utc).isoformat()}
    try:
        if arguments.receipt:
            write_receipt(arguments.receipt, {"result": "running", **provenance})
        binary = arguments.binary.resolve(strict=True)
        require(binary.is_file() and os.access(binary, os.X_OK), f"binary is not executable: {binary}")
        provenance["binary_sha256"] = file_sha256(binary)
        with tempfile.TemporaryDirectory(prefix="zpool-") as temporary:
            root = Path(temporary).resolve()
            try:
                if arguments.health_recovery:
                    receipt = health_smoke(binary, root)
                    for case, no_space in (("loss", False), ("full", True)):
                        additional = loss_smoke(binary, root / case, no_space=no_space)
                        receipt.setdefault("fixtures", []).extend(additional.pop("fixtures", []))
                        receipt.update(additional)
                else:
                    receipt = smoke(binary, root)
            except BaseException:
                for log in sorted(root.rglob("server-*.log")):
                    try:
                        diagnostic = tail(log.read_text(errors="replace"))
                    except OSError as error:
                        # Keep the original failure and the remaining logs even
                        # if one diagnostic is unreadable or has disappeared.
                        diagnostic = f"<unable to read log: {error}>"
                    print(f"\n{log.relative_to(root)}:\n{diagnostic}", file=sys.stderr)
                raise
        require(file_sha256(binary) == provenance["binary_sha256"], "binary changed while the acceptance suite ran")
        receipt.update(provenance)
        receipt["completed_at"] = datetime.now(timezone.utc).isoformat()
        if arguments.receipt:
            write_receipt(arguments.receipt, receipt)
        print("LOCAL_STORAGE_POOL_SMOKE " + json.dumps(receipt, sort_keys=True), flush=True)
        return 0
    except (SmokeFailure, OSError, subprocess.SubprocessError, KeyError, TypeError) as error:
        if arguments.receipt:
            try:
                write_receipt(arguments.receipt, {"result": "failed", "error": str(error), **provenance,
                                                  "completed_at": datetime.now(timezone.utc).isoformat()})
            except OSError as receipt_error:
                print(f"could not write failure receipt: {receipt_error}", file=sys.stderr)
        print(f"local storage pool smoke test failed: {error}", file=sys.stderr)
        return 1


def main() -> int:
    with cancellation_signals() as received:
        try:
            result = run_smoke()
        except SmokeCancelled as error:
            # Argument parsing precedes run_smoke's ordinary failure handler.
            print(f"local storage pool smoke test failed: {error}", file=sys.stderr)
            result = 1
    return 128 + received[0] if received else result


if __name__ == "__main__":
    raise SystemExit(main())
