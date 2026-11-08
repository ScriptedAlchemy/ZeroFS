#!/usr/bin/env python3
"""Exercise local pool initialization, 9P I/O, expansion, and migration.

Run against a freshly built binary, for example:
    python3 scripts/test-local-storage-pool.py --binary zerofs/target/debug/zerofs

Everything, including server sockets and storage, lives in one temporary child.
The script uses the ordinary CLI and never mounts a filesystem or needs root.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import time


PAYLOAD_BYTES = 768 * 1024
FIRST_MEMBER_BYTES = 1024 * 1024
OTHER_MEMBER_BYTES = 8 * 1024 * 1024
TOTAL_TIMEOUT_SECONDS = 300
COMMAND_TIMEOUT_SECONDS = 90
START_TIMEOUT_SECONDS = 60
SHUTDOWN_TIMEOUT_SECONDS = 30


class SmokeFailure(RuntimeError):
    pass


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

    @property
    def target(self) -> str:
        return f"unix:{self.socket_path}"

    def __enter__(self):
        self.socket_path.unlink(missing_ok=True)
        self.log = self.log_path.open("wb")
        environment = os.environ.copy()
        if self.health_events is not None:
            environment["ZEROFS_TEST_DISK_HEALTH"] = str(self.health_events)
        environment.update(RUST_LOG="info", NO_COLOR="1", TERM="dumb")
        try:
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
        except BaseException:
            self.stop(strict=False)
            raise

    def stop(self, *, strict: bool) -> None:
        failure = None
        try:
            if self.process is not None:
                if self.process.poll() is None:
                    try:
                        os.killpg(self.process.pid, signal.SIGTERM)
                    except ProcessLookupError:
                        pass
                    try:
                        self.process.wait(timeout=SHUTDOWN_TIMEOUT_SECONDS)
                    except subprocess.TimeoutExpired:
                        try:
                            os.killpg(self.process.pid, signal.SIGKILL)
                        except ProcessLookupError:
                            pass
                        self.process.wait(timeout=5)
                        failure = "server required SIGKILL instead of graceful shutdown"
                if self.process.returncode != 0 and failure is None:
                    failure = f"server exited with status {self.process.returncode}"
        finally:
            if self.log is not None:
                self.log.close()
                self.log = None
        if failure is not None:
            if strict:
                raise SmokeFailure(failure)
            print(f"cleanup: {failure}", file=sys.stderr)

    def __exit__(self, exception_type, _exception, _traceback):
        self.stop(strict=exception_type is None)
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

    before = command_json(binary, deadline, "pool", "status", "--config", config)
    before_members = status_members(before)
    require(before_members["disk1"]["objects"] > 0, "first local member received no segments")
    require(before_members["disk2"]["objects"] > 0, "quota did not spill segments to the second member")
    require(before["mapped_objects"] >= 2, "two separately synced uploads did not create pool placements")

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
    migrated = command_json(binary, deadline, "pool", "status", "--config", config)
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
    print("Restart with an empty read cache and verify both files from their current disks", flush=True)
    with Server(binary, config, root, "server-after-migration", deadline) as server:
        download_and_verify(binary, deadline, server, root, "after", expected)
    final = command_json(binary, deadline, "pool", "status", "--config", config)
    status_members(final)
    require(final["pool_id"] == initialized["pool_id"], "restart changed the pool identity")
    return {
        "result": "passed",
        "bytes_per_file": PAYLOAD_BYTES,
        "sha256": expected,
        "migrated_segments": migration["migrated_segments"],
        "members": final["members"],
    }


def health_smoke(binary: Path, root: Path) -> dict:
    deadline = time.monotonic() + TOTAL_TIMEOUT_SECONDS
    for directory in ("metadata", "cache", "disk1", "disk2", "disk3", "sources"):
        (root / directory).mkdir(mode=0o700)
    config = write_config(root, [("disk1", OTHER_MEMBER_BYTES), ("disk2", OTHER_MEMBER_BYTES)])
    command_json(binary, deadline, "pool", "init", "--config", config)
    events = root / "health.json"
    events.write_text("{}")
    expected = {}
    for name in ("first.bin", "second.bin"):
        data = hashlib.shake_256(f"health/{name}".encode()).digest(PAYLOAD_BYTES)
        (root / "sources" / name).write_bytes(data)
        expected[name] = hashlib.sha256(data).hexdigest()
    endpoint = f"unix:{root / 'admin.sock'}"
    with Server(binary, config, root, "server-health", deadline, events) as server:
        for name in expected:
            command(binary, deadline, "upload", server.target, root / "sources" / name, f"/{name}", "--jobs", "1")
        before = command_json(binary, deadline, "pool", "status", "--endpoint", endpoint)
        require(before["members"][0]["objects"] >= 2, "health fixture needs multiple source segments")
        next_events = root / "health.next"
        next_events.write_text(json.dumps({"disk1": {"device": "fixture", "pending": 1, "reallocated": 0, "uncorrectable": 0, "crc": 0, "failed": False}}))
        next_events.replace(events)
        while True:
            status = command_json(binary, deadline, "pool", "status", "--endpoint", endpoint)
            if status["members"][0]["safe_to_remove"]:
                break
            require(time.monotonic() < deadline, "automatic evacuation timed out")
            time.sleep(0.1)
        require(status["members"][0]["objects"] == 0, "safe removal with remaining placements")
        require(status["members"][1]["objects"] == before["mapped_objects"], "automatic migration lost placements")
        data = hashlib.shake_256(b"health/after-quarantine").digest(PAYLOAD_BYTES)
        (root / "sources" / "after.bin").write_bytes(data)
        expected["after.bin"] = hashlib.sha256(data).hexdigest()
        command(binary, deadline, "upload", server.target, root / "sources" / "after.bin", "/after.bin", "--jobs", "1")
        after = command_json(binary, deadline, "pool", "status", "--endpoint", endpoint)
        require(after["members"][0]["objects"] == 0, "new application write reached quarantined disk")
        require(after["members"][1]["objects"] > before["mapped_objects"], "new application write did not seal a fresh segment")
    shutil.rmtree(root / "cache")
    (root / "cache").mkdir(mode=0o700)
    with Server(binary, config, root, "server-health-restart", deadline) as server:
        download_and_verify(binary, deadline, server, root, "health-cold", expected)
        status = command_json(binary, deadline, "pool", "status", "--endpoint", endpoint)
        require(status["members"][0]["health"]["quarantine"] is not None, "restart lost quarantine")
        report = command_json(binary, deadline, "pool", "affected", "--endpoint", endpoint, "--member", "disk1")
        require(report["complete"], f"affected report incomplete: {report}")
    return {"result": "passed", "sha256": expected, "automatic_evacuation": True, "cold_restart": True}


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
    missing_body.unlink()
    events.write_text(json.dumps({"disk1": {"device": "fixture", "pending": 1, "reallocated": 0, "uncorrectable": 0, "crc": 0, "failed": False}}))
    shutil.rmtree(root / "cache")
    (root / "cache").mkdir(mode=0o700)
    endpoint = f"unix:{root / 'admin.sock'}"
    with Server(binary, config, root, "server-loss-recovery", deadline, events) as server:
        if no_space:
            failed_read = subprocess.run([str(binary), "download", server.target, "/first.bin", str(root / "first-failed-read"), "--jobs", "1"], capture_output=True, timeout=time_left(deadline, COMMAND_TIMEOUT_SECONDS))
            require(failed_read.returncode != 0, "missing body unexpectedly readable")
        while True:
            status = command_json(binary, deadline, "pool", "status", "--endpoint", endpoint)
            if no_space and status["unreadable"]:
                require(not status["members"][0]["readable_evacuated"], "no-space recovery claimed readable data evacuated")
                require(status["members"][0]["objects"] >= 2, "no-space recovery lost source placements")
                break
            if not no_space and status["members"][0]["safe_to_pull_with_known_loss"]:
                break
            require(time.monotonic() < deadline, "partial-loss recovery timed out")
            time.sleep(0.1)
        require(not status["members"][0]["safe_to_remove"], "loss was marked safe to remove")
        require(status["members"][0]["safe_to_pull_with_known_loss"] == (not no_space), "known-loss removal assessment does not match remaining readable data")
        require(len(status["unreadable"]) >= 1, "missing body has no persistent evidence")
        download_and_verify(binary, deadline, server, root, "loss-healthy", {"second.bin": expected["second.bin"]})
        report = command_json(binary, deadline, "pool", "affected", "--endpoint", endpoint, "--member", "disk1")
        require(report["complete"], f"loss report incomplete: {report}")
        refs = [r for v in report["views"] for r in v["references"]]
        require(any(list(b"/first.bin") in r["paths"] for r in refs), "affected report omitted damaged pathname")
        require(any("before-loss" in v["view"] and v["references"] for v in report["views"]), "affected report omitted retained checkpoint")
        failed_read = subprocess.run([str(binary), "download", server.target, "/first.bin", str(root / "damaged-download"), "--jobs", "1"], capture_output=True, timeout=time_left(deadline, COMMAND_TIMEOUT_SECONDS))
        require(failed_read.returncode != 0, "damaged file silently read successfully")
    shutil.rmtree(root / "cache")
    (root / "cache").mkdir(mode=0o700)
    with Server(binary, config, root, "server-loss-restart", deadline) as server:
        status = command_json(binary, deadline, "pool", "status", "--endpoint", endpoint)
        require(len(status["unreadable"]) >= 1 and not status["members"][0]["safe_to_remove"], "restart lost loss-blocked status")
        download_and_verify(binary, deadline, server, root, "loss-restart-healthy", {"second.bin": expected["second.bin"]})
    return {"partial_loss_no_space" if no_space else "partial_loss": True, "healthy_sha256": expected["second.bin"], "checkpoint_affected": True}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", required=True, type=Path, help="built ZeroFS executable")
    parser.add_argument("--health-recovery", action="store_true", help="requires a disk-health-test-hooks build")
    arguments = parser.parse_args()
    try:
        binary = arguments.binary.resolve(strict=True)
        require(binary.is_file() and os.access(binary, os.X_OK), f"binary is not executable: {binary}")
        with tempfile.TemporaryDirectory(prefix="zpool-") as temporary:
            root = Path(temporary).resolve()
            try:
                if arguments.health_recovery:
                    receipt = health_smoke(binary, root)
                    receipt.update(loss_smoke(binary, root / "loss"))
                    receipt.update(loss_smoke(binary, root / "full", no_space=True))
                else:
                    receipt = smoke(binary, root)
            except BaseException:
                for log in sorted(root.glob("server-*.log")):
                    print(f"\n{log.name}:\n{tail(log.read_text(errors='replace'))}", file=sys.stderr)
                raise
        print("LOCAL_STORAGE_POOL_SMOKE " + json.dumps(receipt, sort_keys=True), flush=True)
        return 0
    except (SmokeFailure, OSError, subprocess.SubprocessError) as error:
        print(f"local storage pool smoke test failed: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
