#!/usr/bin/env python3
"""Qualify 9P file fsync, SIGKILL recovery and pool destage in an isolated fixture.

Python 3.11+, Linux, two dedicated empty 128–512 MiB tmpfs mount roots and
a local scratch parent with a short path for Unix sockets. The caller owns
the mounts, cgroup and native build. Render the parent-supplied TOML into a
new fixture with @ROOT@, @MEMBER_A@, @MEMBER_B@, @CACHE@, @JOURNAL@,
@NINEP@, @RPC@, @METRICS@ and optionally @WEBUI@. Pass a separate verifier
template with the journal profile and writeback table omitted. Configure
member IDs member-a and member-b in that order. Metadata and catalog must
be @ROOT@/metadata and @ROOT@/catalog. Bind only to fixture endpoints.
"""

import argparse
import hashlib
import http.client
import importlib
import json
import math
import os
from pathlib import Path
import re
import shutil
import signal
import socket
import stat
import subprocess
import sys
import tempfile
import time
import tomllib


sys.dont_write_bytecode = True
NineP = importlib.import_module("test-nas-journal-9p").NineP
MIB = 1024**2
MEMBERS = ("member-a", "member-b")
METRICS = (
    "accepted_sequence",
    "local_sequence",
    "remote_sequence",
    "dirty_ram_bytes",
    "dirty_ssd_reserved_bytes",
    "dirty_ssd_operations",
    "remote_bytes_completed_total",
    "retries_total",
    "terminal_error",
)


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


def validate_mount(path):
    entries = Path("/proc/self/mountinfo").read_text().splitlines()
    for line in entries:
        mount, fs = line.split(" - ", 1)
        target = re.sub(r"\\([0-7]{3})", lambda m: chr(int(m[1], 8)), mount.split()[4])
        if target == str(path) and fs.split()[0] == "tmpfs":
            break
    else:
        raise RuntimeError(f"member must be a dedicated tmpfs mount root: {path}")
    space = os.statvfs(path)
    require(
        128 * MIB <= space.f_blocks * space.f_frsize <= 512 * MIB,
        f"expected a 128–512 MiB tmpfs mount: {path}",
    )
    require(not any(path.iterdir()), f"member mount is not empty: {path}")


def space(path):
    info = os.statvfs(path)
    return info.f_bavail * info.f_frsize


def account(path):
    """Count each regular-file inode once per role, excluding ballast."""
    seen = set()
    result = {"regular_files": 0, "apparent_bytes": 0, "allocated_bytes": 0}
    components = {}
    for child in path.rglob("*"):
        try:
            info = child.stat(follow_symlinks=False)
        except FileNotFoundError:
            # Active processes may unlink or rename a file between listing/stat.
            continue
        inode = (info.st_dev, info.st_ino)
        if stat.S_ISREG(info.st_mode) and inode not in seen:
            seen.add(inode)
            parts = child.relative_to(path).parts
            depth = 2 if parts[0].startswith("bucket_") else 1
            component = "/".join(parts[:depth])
            if len(parts) == 1 and parts[0].startswith("."):
                component = "(root dotfiles)"
            bucket = components.setdefault(component, dict.fromkeys(result, 0))
            for key, count in (
                ("regular_files", 1),
                ("apparent_bytes", info.st_size),
                ("allocated_bytes", info.st_blocks * 512),
            ):
                result[key] += count
                bucket[key] += count
    return {**result, "components": components}


def journal_payloads(path):
    return [
        str(item.relative_to(path))
        for item in path.rglob("*")
        if "blobs" in item.relative_to(path).parts[:-1] and item.is_file()
    ]


def loopback_addresses():
    # Allocate distinct candidate ports at once. The actual server binds after
    # rendering the config and fails explicitly if another process wins a port.
    with socket.socket() as metrics, socket.socket() as webui:
        for listener in (metrics, webui):
            listener.bind(("127.0.0.1", 0))
        return [
            f"127.0.0.1:{listener.getsockname()[1]}" for listener in (metrics, webui)
        ]


class Fixture:
    def __init__(self, args, receipt):
        self.args = args
        self.receipt = receipt
        self.owned = []
        self.server = None
        self.start = time.monotonic()
        self.deadline = self.start + args.timeout
        self.stage = "setup"

    def remaining(self):
        left = self.deadline - time.monotonic()
        if left <= 0:
            raise TimeoutError(f"overall timeout during {self.stage}")
        return left

    def event(self, **values):
        entry = {
            "stage": self.stage,
            "elapsed_seconds": time.monotonic() - self.start,
            **values,
        }
        self.receipt["timeline"].append(entry)
        print(json.dumps(entry, sort_keys=True), flush=True)

    def command(self, *args):
        result = subprocess.run(
            [self.args.binary, *args],
            stdout=subprocess.PIPE,
            stderr=self.log,
            timeout=self.remaining(),
            check=False,
        )
        require(
            result.returncode == 0,
            f"CLI {args} exited {result.returncode}; see processes.log",
        )
        return result.stdout

    def owned_dir(self, parent):
        path = Path(tempfile.mkdtemp(prefix="zjn-", dir=parent))
        self.owned.append(path)
        return path

    def render(self, template, cache, verify=False):
        values = {
            "ROOT": self.root,
            "MEMBER_A": self.members[0],
            "MEMBER_B": self.members[1],
            "CACHE": cache,
            "JOURNAL": self.journal,
            "NINEP": self.ninep,
            "RPC": self.rpc,
            "METRICS": self.metrics_address,
            "WEBUI": self.webui_address,
        }
        source = template.read_text()
        require(
            "${" not in source,
            "environment interpolation is forbidden in the fixture template",
        )

        def substitute(match):
            return json.dumps(str(values[match[1]]), ensure_ascii=False)[1:-1]

        source = re.sub(r"@([A-Z_]+)@", substitute, source)
        document = tomllib.loads(source)
        allowed = {
            "runtime",
            "cache",
            "storage",
            "filesystem",
            "lsm",
            "servers",
            "telemetry",
            "prometheus",
            "writeback",
        }
        require(
            document.keys() <= allowed, "template includes unsupported top-level tables"
        )
        require(document["cache"]["dir"] == str(cache), "cache must use @CACHE@")
        storage = document["storage"]
        require(
            storage["url"] == f"file://{self.root / 'metadata'}"
            and "readiness" not in storage,
            "storage must use owned local metadata",
        )
        pool = storage["pool"]
        require(
            pool["catalog_dir"] == str(self.root / "catalog"), "catalog escapes fixture"
        )
        members = pool["members"]
        require(len(members) == 2, "expected exactly two configured members")
        for member, member_id, member_path in zip(members, MEMBERS, self.members):
            require(
                member["id"] == member_id and member["url"] == f"file://{member_path}",
                f"expected local fixture member {member_id}",
            )
            require(
                member["reserve_free_gb"] > 0.0011, "member reserve must exceed 1 MiB"
            )
        servers = document["servers"]
        require(
            servers.keys() <= {"ninep", "rpc", "webui"}, "unexpected protocol listener"
        )
        for name, path in (("ninep", self.ninep), ("rpc", self.rpc)):
            require(
                servers[name]["unix_socket"] == str(path)
                and servers[name]["addresses"] == [],
                f"{name} must use its fixture Unix socket only",
            )
        if "webui" in servers:
            require(
                servers["webui"]["addresses"] == [self.webui_address],
                "WebUI must use @WEBUI@ only",
            )
        require(
            document["prometheus"] == {"addresses": [self.metrics_address]},
            "Prometheus must use @METRICS@ only",
        )
        require(
            document["telemetry"].get("enabled") is False, "disable external telemetry"
        )
        runtime = document["runtime"]
        if verify:
            require("profile" not in runtime, "verifier must omit the runtime profile")
            require(
                "writeback" not in document, "verifier must omit the writeback table"
            )
            require(str(self.journal) not in source, "verifier refers to the journal")
        else:
            require(
                runtime.get("profile") == "small-nas-journal-v1",
                "engine must explicitly select the bounded journal runtime profile",
            )
            wb = document["writeback"]
            require(
                wb["enabled"] is True and wb["dir"] == str(self.journal),
                "writeback must use the owned journal",
            )
            require(
                wb["ack_mode"] == "memory",
                "fixture requires memory acknowledgement plus fsync",
            )
            require(
                document["filesystem"].get("ignore_fsync") is False,
                "fsync must be enabled",
            )
        config = self.root / ("verify.toml" if verify else "engine.toml")
        config.write_text(source)
        return config

    def start_server(self, config, read_only=False):
        for path in (self.ninep, self.rpc):
            path.unlink(missing_ok=True)
        extra = ["--read-only"] if read_only else []
        self.server = subprocess.Popen(
            [self.args.binary, "run", "-c", config, *extra],
            stdout=self.log,
            stderr=self.log,
        )
        self.wait(lambda: self.ninep.exists() and self.rpc.exists())

    def stop_server(self, crash=False):
        if self.server is None:
            return
        server = self.server
        if server.poll() is None:
            server.kill() if crash else server.terminate()
        code = server.wait(timeout=min(20, self.remaining()))
        self.server = None
        require(
            code == (-signal.SIGKILL if crash else 0),
            f"engine exited unexpectedly: {code}",
        )

    def wait(self, predicate):
        while True:
            self.remaining()
            code = self.server.poll()
            require(
                code is None,
                f"engine exited during {self.stage}: {code}; see processes.log",
            )
            if value := predicate():
                return value
            time.sleep(0.10)

    def metric_snapshot(self):
        connection = http.client.HTTPConnection(
            self.metrics_address, timeout=min(2, self.remaining())
        )
        try:
            connection.request("GET", "/metrics")
            response = connection.getresponse()
            require(
                response.status == 200,
                f"metrics endpoint returned HTTP {response.status}",
            )
            body = response.read(8 * MIB + 1)
            require(len(body) <= 8 * MIB, "metric response too large")
        except (ConnectionRefusedError, ConnectionResetError):
            return {}
        finally:
            connection.close()
        rows = {}
        for line in body.decode().splitlines():
            key, _, value = line.partition(" ")
            name = key.removeprefix("zerofs_writeback_")
            if name in METRICS and key != name:
                require(name not in rows, f"duplicate metric {key}")
                number = float(value)
                require(
                    math.isfinite(number) and number >= 0 and number.is_integer(),
                    f"invalid metric {key}: {value}",
                )
                rows[name] = int(number)
        if set(rows) != set(METRICS):
            return {}
        require(
            rows["terminal_error"] == 0,
            "writeback entered terminal state; see processes.log",
        )
        require(
            rows["accepted_sequence"]
            >= rows["local_sequence"]
            >= rows["remote_sequence"],
            f"invalid writeback frontiers: {rows}",
        )
        return rows

    def pool_status(self, *, offline=False):
        options = ("-c", self.config) if offline else ("--endpoint", f"unix:{self.rpc}")
        output = json.loads(self.command("pool", "status", *options))
        members = {member["id"]: member for member in output["members"]}
        result = {}
        for ident in MEMBERS:
            member = members[ident]
            result[ident] = {
                key: member[key]
                for key in (
                    "used_bytes",
                    "reserved_bytes",
                    "objects",
                    "available_bytes",
                    "pending_uploads",
                )
            }
            result[ident]["space"] = member["space"]
        return result

    def storage_account(self):
        locations = {
            "cache": self.cache,
            "journal": self.journal,
            "metadata": self.root / "metadata",
            "catalog": self.root / "catalog",
            **dict(zip(MEMBERS, self.members)),
        }
        self.event(accounting={role: account(path) for role, path in locations.items()})

    def ballast(self, parent):
        path = parent / "ballast"
        count = space(parent) - MIB
        require(count > 0, f"insufficient tmpfs space before ballast: {parent}")
        with path.open("xb") as target:
            os.posix_fallocate(target.fileno(), 0, count)
            os.fsync(target.fileno())
        require(
            path.stat().st_blocks * 512 >= count and space(parent) <= MIB,
            f"ballast did not occupy real allocation: {path}",
        )
        self.event(ballast=str(path), ballast_bytes=count, free_bytes=space(parent))
        return path

    @staticmethod
    def drained(rows):
        return bool(rows) and (
            rows["accepted_sequence"]
            == rows["local_sequence"]
            == rows["remote_sequence"]
            and rows["dirty_ssd_reserved_bytes"] == rows["dirty_ssd_operations"] == 0
            and rows["dirty_ram_bytes"] == 0
        )

    def held(self, baseline):
        members = self.pool_status()
        for name, member in members.items():
            require(
                member["available_bytes"] == 0
                and (member["space"]["full"] or {}).get("reason")
                == "below_free_space_reserve",
                f"{name} is not held by the actual filesystem reserve: {member}",
            )
            require(
                member["used_bytes"] == baseline[name]["used_bytes"]
                and member["objects"] == baseline[name]["objects"],
                f"{name} published to destination while held: {member}",
            )
        return members

    def verify_hashes(self, expected):
        with NineP(self.ninep, self.remaining) as client:
            for name, digest in expected.items():
                started = time.monotonic()
                observed = client.read_hash(name.encode(), self.args.file_mib * MIB)
                self.event(
                    file=name,
                    sha256=observed,
                    expected_sha256=digest,
                    read_seconds=time.monotonic() - started,
                )
                require(observed == digest, f"SHA-256 mismatch for {name}")

    def run(self):
        self.root = self.owned_dir(self.args.scratch)
        member_roots = [self.owned_dir(path) for path in self.args.member_mounts]
        self.members = [root / "destination" for root in member_roots]
        self.cache = self.root / "cache"
        self.journal = self.root / "journal"
        verify_cache = self.root / "verify-cache"
        for path in (
            self.cache,
            self.journal,
            self.root / "metadata",
            self.root / "catalog",
            *self.members,
        ):
            path.mkdir()
        self.ninep = self.root / "ninep.sock"
        self.rpc = self.root / "rpc.sock"
        require(
            len(os.fsencode(self.ninep)) < 108,
            "scratch pathname exceeds Unix socket limit",
        )
        self.metrics_address, self.webui_address = loopback_addresses()
        self.config = self.render(self.args.config_template, self.cache)
        verify_config = self.render(
            self.args.verify_config_template, verify_cache, verify=True
        )
        self.receipt["engine_config_sha256"] = hashlib.sha256(
            self.config.read_bytes()
        ).hexdigest()
        self.receipt["verifier_config_sha256"] = hashlib.sha256(
            verify_config.read_bytes()
        ).hexdigest()
        self.stage = "init"
        self.command("pool", "init", "-c", self.config)
        self.stage = "startup"
        self.start_server(self.config)
        status = self.wait(
            lambda: rows if self.drained(rows := self.metric_snapshot()) else None
        )
        self.event(writeback=status)
        baseline = self.pool_status()
        member_start = {
            name: account(path) for name, path in zip(MEMBERS, self.members)
        }
        self.event(members=baseline)
        self.storage_account()

        self.stage = "hold"
        ballast = [self.ballast(path) for path in member_roots]
        self.event(members=self.held(baseline))
        self.stage = "write-and-fsync"
        expected = {}
        with NineP(self.ninep, self.remaining) as client:
            for index in range(self.args.files):
                name = f"journal-case-{index:02d}"
                data = hashlib.shake_256(f"zerofs-nas-journal:{name}".encode()).digest(
                    self.args.file_mib * MIB
                )
                expected[name] = hashlib.sha256(data).hexdigest()
                started = time.monotonic()
                client.create(name.encode())
                client.write(data)
                acknowledged = time.monotonic()
                client.fsync()
                flushed = time.monotonic()
                self.event(
                    file=name,
                    bytes=len(data),
                    expected_sha256=expected[name],
                    create_and_write_seconds=acknowledged - started,
                    fsync_seconds=flushed - acknowledged,
                )
                client.clunk()
        require(
            len(set(expected.values())) == self.args.files,
            "fixture digests must be distinct",
        )

        self.stage = "pre-kill-backlog"

        def held_backlog():
            rows = self.metric_snapshot()
            return (
                rows
                if rows
                and (
                    rows["accepted_sequence"]
                    == rows["local_sequence"]
                    > rows["remote_sequence"]
                    and rows["dirty_ssd_reserved_bytes"] > 0
                    and rows["dirty_ssd_operations"] > 0
                )
                else None
            )

        backlog = self.wait(held_backlog)
        self.event(writeback=backlog, members=self.held(baseline))
        self.storage_account()
        self.stop_server(crash=True)
        self.event(exit_signal="SIGKILL")
        require(
            journal_payloads(self.journal), "no journal payload files after SIGKILL"
        )
        shutil.rmtree(self.cache)
        self.cache.mkdir()
        self.event(prior_clean_cache_removed=True)

        self.stage = "recover-still-held"
        self.start_server(self.config)
        self.verify_hashes(expected)
        recovered = self.wait(held_backlog)
        require(
            recovered["local_sequence"] >= backlog["local_sequence"],
            "recovered local sequence regressed",
        )
        self.event(writeback=recovered, members=self.held(baseline))
        self.storage_account()

        self.stage = "release-and-drain"
        overlap_observed = False
        drain_started = time.monotonic()
        for name, path in zip(MEMBERS, ballast):
            before = time.monotonic() - self.start
            path.unlink()
            self.receipt.setdefault("ballast_releases", {})[name] = {
                "unlink_started_seconds": before,
                "unlink_completed_seconds": time.monotonic() - self.start,
            }

        def drain_sample():
            nonlocal overlap_observed
            rows = self.metric_snapshot()
            members = self.pool_status()
            overlap_observed |= all(
                member["reserved_bytes"] > 0 for member in members.values()
            )
            self.event(
                writeback=rows,
                members=members,
                committed_bytes_delta={
                    name: members[name]["used_bytes"] - baseline[name]["used_bytes"]
                    for name in MEMBERS
                },
            )
            return (
                rows
                if self.drained(rows)
                and rows["remote_sequence"] >= recovered["local_sequence"]
                else None
            )

        drained = self.wait(drain_sample)
        self.receipt["drain_seconds"] = time.monotonic() - drain_started
        self.receipt["simultaneous_member_reservations_observed"] = overlap_observed
        self.receipt["drain_remote_bytes_delta"] = (
            drained["remote_bytes_completed_total"]
            - recovered["remote_bytes_completed_total"]
        )
        require(
            self.receipt["drain_remote_bytes_delta"] > 0,
            "no replay bytes completed during drain",
        )
        self.storage_account()

        self.stage = "offline-destination"
        self.stop_server()
        final = self.pool_status(offline=True)
        distribution = {}
        for name, path in zip(MEMBERS, self.members):
            physical = account(path)
            distribution[name] = {
                "committed_bytes_delta": final[name]["used_bytes"]
                - baseline[name]["used_bytes"],
                "committed_objects_delta": final[name]["objects"]
                - baseline[name]["objects"],
                "allocated_bytes_delta": physical["allocated_bytes"]
                - member_start[name]["allocated_bytes"],
                "apparent_bytes_delta": physical["apparent_bytes"]
                - member_start[name]["apparent_bytes"],
            }
        self.event(members=final, distribution=distribution)
        require(
            all(
                all(delta > 0 for delta in distribution[name].values())
                and final[name]["reserved_bytes"] == final[name]["pending_uploads"] == 0
                for name in MEMBERS
            ),
            "both members must retain committed and physically allocated segments",
        )
        leftovers = journal_payloads(self.journal)
        require(
            not leftovers, f"drained journal still contains payload files: {leftovers}"
        )
        shutil.rmtree(self.journal)
        shutil.rmtree(self.cache)
        require(
            not self.journal.exists() and not self.cache.exists(),
            "original journal or cache still exists",
        )

        self.event(
            journal_payload_files_after_drain=len(leftovers),
            original_journal_removed=True,
            original_clean_cache_removed=True,
        )

        self.stage = "independent-read-only-verifier"
        self.start_server(verify_config, read_only=True)
        self.verify_hashes(expected)
        self.stop_server()
        require(
            not self.journal.exists() and not self.cache.exists(),
            "verifier recreated the original journal or cache",
        )
        self.event(original_journal_absent=True, original_cache_absent=True)

    def cleanup(self):
        if self.server is not None:
            if self.server.poll() is None:
                self.server.kill()
            self.server.wait(timeout=5)
        failures = []
        for path in reversed(self.owned):
            try:
                shutil.rmtree(path)
                require(not path.exists(), f"cleanup failed to remove {path}")
            except OSError as error:
                failures.append(str(error))
        require(not failures, f"could not remove all fixture directories: {failures}")
        self.receipt["cleanup_verified"] = True


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for flag in (
        "binary",
        "scratch",
        "evidence",
        "config-template",
        "verify-config-template",
        "member-a",
        "member-b",
    ):
        parser.add_argument(f"--{flag}", type=Path, required=True)
    parser.add_argument(
        "--timeout", type=float, default=300, help="overall run deadline in seconds"
    )
    parser.add_argument("--files", type=int, default=6)
    parser.add_argument("--file-mib", type=int, default=36)
    args = parser.parse_args()
    if not (
        math.isfinite(args.timeout)
        and 0 < args.timeout <= 1800
        and 2 <= args.files <= 16
        and 1 <= args.file_mib <= 64
        and args.files * args.file_mib <= 256
    ):
        parser.error(
            "expected a deadline up to 1800 s and 2–16 files of 1–64 MiB, up to 256 MiB total"
        )
    for key, value in vars(args).items():
        if isinstance(value, Path):
            setattr(args, key, value.resolve())
    args.member_mounts = (args.member_a, args.member_b)
    args.evidence.mkdir(mode=0o700)
    receipt = {
        "status": "failed",
        "timeline": [],
        "scope": "synthetic local tmpfs destinations, native userspace 9P2000.L file I/O",
        "accounting_scope": (
            "Non-atomic scoped regular-file stat samples. Apparent bytes sum st_size. "
            "Allocated bytes sum st_blocks * 512. Component names use the first "
            "child beneath a role or its bucket namespace. No directories, ballast, cgroup, "
            "filesystem metadata, unlinked files or physical-media durability accounting."
        ),
        "timeout_seconds": args.timeout,
        "files": args.files,
        "file_mib": args.file_mib,
        "kernel": os.uname().release,
    }
    fixture = Fixture(args, receipt)

    def interrupted(signum, _frame):
        raise TimeoutError(f"signal {signum} during {fixture.stage}")

    with (args.evidence / "processes.log").open("wb") as fixture.log:
        try:
            for signum in (signal.SIGALRM, signal.SIGTERM, signal.SIGINT):
                signal.signal(signum, interrupted)
            signal.setitimer(signal.ITIMER_REAL, args.timeout)
            for path in args.member_mounts:
                validate_mount(path)
            require(
                args.member_a.stat().st_dev != args.member_b.stat().st_dev,
                "member tmpfs mounts must be distinct filesystems",
            )
            with args.binary.open("rb") as binary:
                receipt["binary_sha256"] = hashlib.file_digest(
                    binary, "sha256"
                ).hexdigest()
            fixture.run()
        except Exception as error:
            receipt["error"] = {
                "stage": fixture.stage,
                "message": f"{type(error).__name__}: {error}",
            }
        finally:
            # Separate cleanup budget, including filesystem operations.
            fixture.stage = "cleanup"
            signal.setitimer(signal.ITIMER_REAL, 15)
            try:
                fixture.cleanup()
            except Exception as error:
                receipt["cleanup_error"] = f"{type(error).__name__}: {error}"
                receipt["remaining_owned_paths"] = [
                    str(path) for path in fixture.owned if path.exists()
                ]
            finally:
                signal.setitimer(signal.ITIMER_REAL, 0)
    if "error" not in receipt and receipt.get("cleanup_verified"):
        receipt["status"] = "passed"
    (args.evidence / "receipt.json").write_text(
        json.dumps(receipt, indent=2, sort_keys=True) + "\n"
    )
    print(
        json.dumps({key: value for key, value in receipt.items() if key != "timeline"}),
        flush=True,
    )
    return 0 if receipt["status"] == "passed" else 1


if __name__ == "__main__":
    sys.exit(main())
