#!/usr/bin/env python3
"""Run two independent, synthetic NVMe/TCP crash scenarios in the owned container."""
import hashlib
import json
import os
import selectors
import socket
import subprocess
import sys
import time
import tomllib
import urllib.request
import uuid
from pathlib import Path

ROOT = Path("/fixture")
ENGINE = ROOT / "zerofs"
TARGET = Path("/out/zerofs-nvmf-tgt")
PROBE = Path("/source/build/bin/zerofs_nvme_probe")
MEMBERS = [ROOT / "member-a", ROOT / "member-b"]
MIB = 1024 * 1024
SIZE = 16 * MIB
HOST = "nqn.2026-11.test.zerofs:host"
SUBSYSTEM = "nqn.2026-11.test.zerofs:namespace"

sys.path.insert(0, str(ROOT / "app"))
from qualification import install_key, wait_for


def emit(event, **values):
    print(json.dumps({"event": event, **values}, sort_keys=True), flush=True)


def call(*args):
    return subprocess.run(list(map(str, args)), check=True, timeout=45, text=True,
                          stdout=subprocess.PIPE).stdout


def mount_row(path):
    rows = [row for row in Path("/proc/self/mountinfo").read_text().splitlines()
            if row.split()[4] == str(path)]
    if len(rows) > 1:
        raise RuntimeError(f"ambiguous mount: {path}")
    return rows[0] if rows else None


def digest(path):
    with path.open("rb") as file:
        return hashlib.file_digest(file, "sha256").hexdigest()


def metrics(port):
    prefix = "zerofs_writeback_"
    names = (
        "accepted_sequence", "local_sequence", "remote_sequence", "dirty_ram_bytes",
        "dirty_ssd_reserved_bytes", "dirty_ssd_capacity_bytes", "dirty_ssd_operations",
        "remote_bytes_completed_total", "terminal_error",
    )
    with urllib.request.urlopen(f"http://127.0.0.1:{port}/metrics", timeout=2) as response:
        body = response.read().decode()
    rows = {}
    for line in body.splitlines():
        name, _, value = line.partition(" ")
        if name.startswith(prefix) and name.removeprefix(prefix) in names:
            rows[name.removeprefix(prefix)] = int(float(value))
    if set(rows) != set(names) or rows["terminal_error"]:
        raise RuntimeError(f"writeback metrics missing or terminal: {rows}")
    return rows


class Scenario:
    def __init__(self, operation, namespace=None):
        self.operation = operation
        self.root = ROOT / operation
        self.root.mkdir(mode=0o700, exist_ok=namespace is not None)
        self.mount = self.root / "mount"
        self.mount.mkdir(exist_ok=namespace is not None)
        self.backing = self.mount / "namespace.img"
        self.config = self.root / "engine.toml"
        self.socket = self.root / "ninep.sock"
        self.key = self.root / "initiator"
        self.wrong_key = self.root / "wrong-initiator"
        self.namespace = namespace or str(uuid.uuid4())
        self.cpu, self.client_cpu, *_ = sorted(os.sched_getaffinity(0))
        self.port = 14420
        self.metric_port = 19851
        self.processes = {}
        self.member_paths = [member / operation for member in MEMBERS]
        self.fills = [member / f"{operation}.fill" for member in MEMBERS]
        for directory in [*self.member_paths, self.root / "metadata", self.root / "journal"]:
            directory.mkdir(exist_ok=namespace is not None)
        install_key(self.key)
        install_key(self.wrong_key)

    def render(self, cache, *, verifier=False):
        template = (ROOT / "nas-journal.toml.in").read_text()
        replacements = {
            "ROOT": self.root, "CACHE": self.root / cache, "JOURNAL": self.root / "journal",
            "MEMBER_A": self.member_paths[0], "MEMBER_B": self.member_paths[1],
            "NINEP": self.socket, "RPC": self.root / "rpc.sock",
            "WEBUI": "127.0.0.1:19850", "METRICS": f"127.0.0.1:{self.metric_port}",
        }
        for name, value in replacements.items():
            template = template.replace(f"@{name}@", str(value))
        if "@" in template or (self.root / cache).exists():
            raise RuntimeError("config substitutions or fresh cache check failed")
        config = tomllib.loads(template)
        writeback = config["writeback"]
        if (
            config["runtime"]["profile"] != "small-nas-journal-v1"
            or config["filesystem"]["write_ack_mode"] != "materialized"
            or config["filesystem"]["ignore_fsync"]
            or config["servers"]["ninep"] != {"addresses": [], "unix_socket": str(self.socket)}
            or not writeback["enabled"]
            or writeback["memory_size_gb"] != 0.134217728
            or writeback["disk_size_gb"] != 0.536870912
            or [item["reserve_free_gb"] for item in config["storage"]["pool"]["members"]]
            != [0.01, 0.01]
        ):
            raise RuntimeError("unexpected NAS journal profile template")
        if verifier:
            before, marker, _ = template.partition("\n[writeback]\n")
            if not marker:
                raise RuntimeError("expected a final writeback table in the template")
            template = before.replace('profile = "small-nas-journal-v1"\n', "")
            parsed = tomllib.loads(template)
            if "profile" in parsed["runtime"] or "writeback" in parsed:
                raise RuntimeError("verifier must omit the journal profile and table")
        self.config.write_text(template, encoding="utf-8")

    def launch(self, role, *args, **kwargs):
        with (self.root / f"{role}.log").open("ab") as log:
            process = subprocess.Popen(list(map(str, args)), stderr=log,
                                       stdout=kwargs.pop("stdout", log), **kwargs)
        self.processes[role] = process
        return process

    def stop(self, role, *, kill=False):
        process = self.processes.get(role)
        if process is None:
            return None
        if process.poll() is None:
            if kill:
                process.kill()
            else:
                process.terminate()
        try:
            result = process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            process.kill()
            result = process.wait(timeout=5)
        del self.processes[role]
        return result

    def start_engine(self, *, read_only=False):
        for socket_path in (self.socket, self.root / "rpc.sock"):
            socket_path.unlink(missing_ok=True)
        engine = self.launch("engine", ENGINE, "run", "--config", self.config,
                             *(("--read-only",) if read_only else ()))
        wait_for(self.socket.is_socket, "engine 9P", engine, timeout=45)
        mount = self.launch("mount", ENGINE, "mount", f"unix:{self.socket}", self.mount,
                            "--writeback", "false", "--relaxed-consistency", "false",
                            *(("--read-only",) if read_only else ()))
        wait_for(lambda: mount_row(self.mount), "private FUSE mount", mount)
        emit("mount", scenario=self.operation, mountinfo=mount_row(self.mount))

    def unmount(self, *, crashed=False):
        if crashed:
            self.stop("mount", kill=True)
        if mount_row(self.mount):
            call("umount", "--no-canonicalize", self.mount)
        self.stop("mount")
        if mount_row(self.mount):
            raise RuntimeError("FUSE mount remained after cleanup")

    def start_target(self):
        target = self.launch(
            "target", sys.executable, "-B", ROOT / "fuse-target.py", "--isolated-synthetic",
            "--binary", TARGET, "--runtime-directory", self.root, "--mount", self.mount,
            "--socket", self.socket, "--mount-id", mount_row(self.mount).split()[0],
            "--filename", self.backing.name, "--inode", self.backing.stat().st_ino, "--size", SIZE,
            "--serial", "NASJOURNAL", "--namespace-uuid", self.namespace, "--address", "127.0.0.1",
            "--port", self.port, "--subsystem-nqn", SUBSYSTEM, "--host-nqn", HOST, "--key", self.key,
            "--cpu", self.cpu, "--memory-mib", 512,
        )
        def port_open():
            with socket.socket() as connection:
                connection.settimeout(0.1)
                return connection.connect_ex(("127.0.0.1", self.port)) == 0

        wait_for(port_open, "NVMe/TCP loopback listener", target)

    def probe_args(self, mode, key, host=HOST):
        return (PROBE, mode, "127.0.0.1", self.port, SUBSYSTEM, host, key, self.namespace,
                SIZE, f"[{self.client_cpu}]")

    def fill_members(self):
        for member, fill in zip(MEMBERS, self.fills):
            row = mount_row(member)
            stat = os.statvfs(member)
            if not row or row.split(" - ")[1].split()[0] != "tmpfs" or stat.f_blocks * stat.f_frsize != 256 * MIB:
                raise RuntimeError(f"refusing to fill a non-dedicated tmpfs: {member}")
            with fill.open("xb") as file:
                os.posix_fallocate(file.fileno(), 0, stat.f_bavail * stat.f_frsize - MIB)
            stat = os.statvfs(member)
            free = stat.f_bavail * stat.f_frsize
            if free > MIB:
                raise RuntimeError(f"member not full: {member}, {free}")
            emit("member_full", scenario=self.operation, member=member.name,
                 total_bytes=stat.f_blocks * stat.f_frsize, free_bytes=free)

    def pool_status(self, baseline=None, *, offline=False):
        options = ("--config", self.config) if offline else (
            "--endpoint", f"unix:{self.root / 'rpc.sock'}"
        )
        output = json.loads(call(ENGINE, "pool", "status", *options))
        rows = {
            row["id"]: {key: row[key] for key in ("used_bytes", "objects", "available_bytes", "space")}
            for row in output["members"]
        }
        if set(rows) != {"member-a", "member-b"}:
            raise RuntimeError(f"unexpected pool members: {rows}")
        if baseline is not None:
            for name, row in rows.items():
                if (row["available_bytes"] != 0
                        or (row["space"]["full"] or {}).get("reason") != "below_free_space_reserve"
                        or any(row[key] != baseline[name][key] for key in ("used_bytes", "objects"))):
                    raise RuntimeError(f"member was not held at baseline: {name}, {row}")
        return rows

    def cutoff(self):
        marker = f"ACK {self.operation.upper()}".encode()
        probe = self.launch("probe", *self.probe_args(f"cutoff-{self.operation}", self.key),
                            stdin=subprocess.PIPE, stdout=subprocess.PIPE)
        transcript = b""
        with selectors.DefaultSelector() as selector:
            selector.register(probe.stdout, selectors.EVENT_READ)
            deadline = time.monotonic() + 30
            while marker not in transcript.splitlines():
                if not selector.select(max(0, deadline - time.monotonic())):
                    raise RuntimeError(f"no NVMe cutoff ACK: {transcript!r}")
                chunk = os.read(probe.stdout.fileno(), 4096)
                if not chunk:
                    raise RuntimeError(f"probe exited before NVMe cutoff ACK: {transcript!r}")
                transcript += chunk
        observed = time.monotonic_ns()
        engine = self.processes["engine"]
        engine.kill()
        signaled = time.monotonic_ns()
        status = engine.wait(timeout=10)
        del self.processes["engine"]
        emit("cutoff", scenario=self.operation, engine_pid=engine.pid, wait_status=status,
             ack_observed_monotonic_ns=observed, sigkill_sent_monotonic_ns=signaled,
             transcript=transcript.decode().splitlines())
        if status != -9:
            raise RuntimeError(f"expected engine SIGKILL, got {status}")
        self.stop("probe", kill=True)
        self.stop("target", kill=True)
        self.unmount(crashed=True)

    def run(self):
        self.render("cache-before")
        call(ENGINE, "pool", "init", "--config", self.config)
        self.start_engine()
        with self.backing.open("xb") as backing:
            os.fchmod(backing.fileno(), 0o600)
            backing.truncate(SIZE)
            os.fsync(backing.fileno())
        baseline = digest(self.backing)
        if baseline != hashlib.sha256(bytes(SIZE)).hexdigest():
            raise RuntimeError("baseline namespace was not zero-filled")
        self.start_target()
        for label, key, host in (
            ("no_key", "-", HOST),
            ("wrong_key", self.wrong_key, HOST),
            ("wrong_host", self.key, f"{HOST}-denied"),
        ):
            denied = call(*self.probe_args("deny", key, host))
            if "authenticated connect refused" not in denied:
                raise RuntimeError(f"missing {label} refusal")
            emit("auth_control", scenario=self.operation, control=label, transcript=denied.splitlines())
        def drained():
            snapshot = metrics(self.metric_port)
            if (snapshot["accepted_sequence"] == snapshot["local_sequence"]
                    == snapshot["remote_sequence"] and snapshot["dirty_ssd_operations"] == 0):
                return snapshot
            return None

        # Establish the empty-journal baseline before denying member placement.
        wait_for(drained, "drained baseline journal")
        baseline_metrics = drained()
        pool_baseline = self.pool_status()
        self.fill_members()
        emit("armed", scenario=self.operation, namespace=self.namespace, baseline_sha256=baseline,
             metrics=baseline_metrics, members=self.pool_status(pool_baseline))
        self.cutoff()
        self.recover(baseline_metrics, pool_baseline)

    def journal_payloads(self):
        return {str(path.relative_to(self.root)): path.stat().st_size
                for path in (self.root / "journal").glob("bucket_*/blobs/**/*.blobs")}

    def recover(self, baseline_metrics, pool_baseline):
        payloads = self.journal_payloads()
        emit("journal_after_kill", scenario=self.operation, payloads=payloads)
        if not payloads or not all(payloads.values()):
            raise RuntimeError("no journal payloads survived engine SIGKILL")
        emit("fresh_cache", scenario=self.operation, fresh_dir="cache-recovery")
        self.render("cache-recovery")
        self.start_engine()
        expected = bytearray(SIZE)
        lba, value = (3, 0x75) if self.operation == "fua" else (8, 0x6b)
        expected[lba * 4096:(lba + 1) * 4096] = bytes([value]) * 4096
        expected_sha256 = hashlib.sha256(expected).hexdigest()
        actual = digest(self.backing)
        recovered = metrics(self.metric_port)
        emit("fresh_cache_read", scenario=self.operation, sha256=actual,
             expected_sha256=expected_sha256, metrics=recovered, members=self.pool_status(pool_baseline))
        if actual != expected_sha256:
            raise RuntimeError("fresh-cache backing payload mismatch")
        if not (recovered["accepted_sequence"] == recovered["local_sequence"]
                > recovered["remote_sequence"]
                and recovered["local_sequence"] > baseline_metrics["local_sequence"]
                and recovered["dirty_ssd_reserved_bytes"] > 0):
            raise RuntimeError(f"expected recovered local-only backlog: {recovered}")
        self.start_target()
        transcript = call(*self.probe_args(f"verify-{self.operation}", self.key))
        emit("recovered", scenario=self.operation, bytes=SIZE, lba=lba, byte=value,
             sha256=actual, transcript=transcript.splitlines())
        self.stop("target")
        self.unmount()
        for fill in self.fills:
            fill.unlink()

        def drained():
            rows = metrics(self.metric_port)
            if (rows["accepted_sequence"] == rows["local_sequence"] == rows["remote_sequence"]
                    and rows["dirty_ssd_operations"] == 0 and not self.journal_payloads()):
                return rows
            return None

        wait_for(drained, "remote frontier with zero journal blobs", timeout=45)
        rows = drained()
        if not rows or rows["remote_sequence"] < recovered["local_sequence"]:
            raise RuntimeError("remote frontier regressed")
        status = self.stop("engine", kill=True)
        blobs = self.journal_payloads()
        emit("drained", scenario=self.operation, metrics=rows, engine_wait_status=status,
             payloads_after_sigkill=blobs)
        if status != -9 or blobs:
            raise RuntimeError("offline drain verification failed")
        offline = self.pool_status(offline=True)
        if not any(member["objects"] > 0 for member in offline.values()):
            raise RuntimeError(f"no placed segment after drain: {offline}")
        self.render("cache-verifier", verifier=True)
        self.start_engine(read_only=True)
        offline_sha256 = digest(self.backing)
        verifier_blobs = self.journal_payloads()
        if offline_sha256 != expected_sha256 or verifier_blobs:
            raise RuntimeError("journal-free verifier mismatch")
        emit("journal_free_read", scenario=self.operation, sha256=offline_sha256,
             expected_sha256=expected_sha256, members=offline, verifier_blob_count=len(verifier_blobs))

    def cleanup(self):
        self.stop("probe", kill=True)
        self.stop("target", kill=True)
        self.unmount()
        self.stop("engine")
        for fill in self.fills:
            fill.unlink(missing_ok=True)
        self.key.unlink(missing_ok=True)
        self.wrong_key.unlink(missing_ok=True)
        emit("case_cleanup", scenario=self.operation, processes=list(self.processes),
             mount_present=bool(mount_row(self.mount)), fills_present=any(p.exists() for p in self.fills))


def main():
    os.umask(0o077)
    if {name for _, name in socket.if_nameindex()} != {"lo"}:
        raise SystemExit("requires a loopback-only container")
    emit("executables", sha256={path.name: digest(path) for path in (ENGINE, TARGET, PROBE)})
    if sys.argv[1:] == ["--preflight"]:
        scenario = Scenario("preflight")
        try:
            scenario.render("cache-before")
            call(ENGINE, "pool", "init", "--config", scenario.config)
            scenario.start_engine()
            if list(scenario.mount.iterdir()):
                raise RuntimeError("preflight expected an empty filesystem")
            emit("preflight_mount_verified")
        finally:
            scenario.cleanup()
        return 0
    if sys.argv[1:]:
        raise SystemExit("usage: run.py [--preflight]")
    failed = False
    for operation in ("fua", "flush"):
        scenario = Scenario(operation)
        try:
            scenario.run()
        except Exception as error:
            failed = True
            emit("failure", scenario=operation, error=str(error))
        finally:
            scenario.cleanup()
    return int(failed)


if __name__ == "__main__":
    raise SystemExit(main())
