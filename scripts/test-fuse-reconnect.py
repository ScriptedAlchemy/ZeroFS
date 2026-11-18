#!/usr/bin/env python3
"""Keep a FUSE client alive across outages and concurrent descriptor closes."""

import argparse
from concurrent.futures import ThreadPoolExecutor
import errno
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile

import importlib.util


spec = importlib.util.spec_from_file_location(
    "delete_reuse", Path(__file__).with_name("test-pool-delete-reuse.py")
)
helpers = importlib.util.module_from_spec(spec)
spec.loader.exec_module(helpers)


def run(binary, scratch, evidence, repetitions):
    root = Path(tempfile.mkdtemp(prefix="zerofs-reconnect-", dir=scratch))
    server = client = None
    opened = set()
    log = (evidence / "processes.log").open("wb")
    try:
        for name in ("mount", "store"):
            (root / name).mkdir()
        config = root / "zerofs.toml"
        config.write_text(f'''[cache]
dir = "{root / "cache"}"
disk_size_gb = 0.064
memory_size_gb = 0.001
warm_metadata = "off"
[storage]
url = "file://{root / "store"}"
encryption_password = "synthetic-reconnect"
[servers.ninep]
addresses = []
unix_socket = "{root / "9p.sock"}"
[telemetry]
enabled = false
''')

        def start():
            (root / "9p.sock").unlink(missing_ok=True)
            process = subprocess.Popen(
                [str(binary), "run", "-c", str(config)], stdout=log, stderr=log
            )
            helpers.wait_for((root / "9p.sock").exists, process, "server readiness")
            return process

        server = start()
        client = subprocess.Popen(
            [
                str(binary),
                "mount",
                "--writeback",
                "false",
                "--relaxed-consistency",
                "false",
                "--reconnect-timeout",
                "1",
                f"unix:{root / '9p.sock'}",
                str(root / "mount"),
            ],
            stdout=log,
            stderr=log,
        )
        helpers.wait_for(
            lambda: helpers.is_mounted(root / "mount"), client, "FUSE readiness"
        )
        data = hashlib.shake_256(b"reconnect-stable").digest(1024**2)
        target = root / "mount/stable"
        with target.open("wb", buffering=0) as out:
            assert out.write(data) == len(data)
            os.fsync(out.fileno())
        reader = os.open(target, os.O_RDONLY)
        opened.add(reader)
        receipt = {
            "kernel": os.uname().release,
            "binary_sha256": helpers.digest(binary),
            "payload_sha256": hashlib.sha256(data).hexdigest(),
            "mount_client_pid": client.pid,
            "surviving_reader_fd": reader,
            "repetitions": [],
        }
        for index in range(repetitions):
            retire = [os.open(target, os.O_RDONLY) for _ in range(8)]
            opened.update(retire)
            assert os.pread(reader, len(data), 0) == data
            helpers.stop(server, kill=True)
            # This response establishes that the outage bound has fired; a
            # timer delay would not establish the client's actual state.
            try:
                os.pread(reader, len(data), 0)
            except OSError as error:
                assert error.errno == errno.EIO, error
            else:
                raise AssertionError("uncached read did not observe the dead server")

            def close(fd):
                try:
                    os.close(fd)
                    return None
                except OSError as error:
                    assert error.errno in (errno.EIO, errno.ENOTCONN), error
                    return error.errno

            with ThreadPoolExecutor(max_workers=8) as executor:
                close_errors = list(executor.map(close, retire))
            opened.difference_update(retire)
            server = start()
            # Exactly one call on the same surviving descriptor. A retry here
            # would conceal the first-call reconnect regression.
            assert os.pread(reader, len(data), 0) == data
            assert client.poll() is None
            row = {
                "iteration": index + 1,
                "first_read": "exact payload",
                "close_errnos": close_errors,
                "server_pid": server.pid,
                "mount_client_pid": client.pid,
                "surviving_reader_fd": reader,
            }
            receipt["repetitions"].append(row)
            print(json.dumps(row), flush=True)
        os.close(reader)
        opened.remove(reader)
        (evidence / "receipt.json").write_text(json.dumps(receipt, indent=2) + "\n")
    finally:
        helpers.stop(client, kill=True)
        for fd in opened:
            try:
                os.close(fd)
            except OSError:
                pass
        if helpers.is_mounted(root / "mount"):
            helpers.command("fusermount3", "-u", str(root / "mount"))
        helpers.stop(server)
        log.close()
        shutil.rmtree(root)
        assert not root.exists()
        print(f"Removed owned fixture {root}, mount, client, and server.", flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--scratch", type=Path, required=True)
    parser.add_argument("--evidence", type=Path, required=True)
    parser.add_argument("--repetitions", type=int, default=10)
    args = parser.parse_args()
    args.evidence.mkdir(parents=True, exist_ok=True)
    run(args.binary.resolve(), args.scratch, args.evidence, args.repetitions)


if __name__ == "__main__":
    main()
