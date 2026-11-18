#!/usr/bin/env python3
"""Capture the Linux 9P wire distinction between empty replacement and removal.

Run in an isolated Linux container with mount authority. No kernel modification
or extra module loading is needed; the 9P mount may autoload stock 9P modules.
"""

import argparse
import errno
import json
import os
import pathlib
import socket
import struct
import subprocess
import threading
import time
import shutil
import tempfile

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("--binary", type=pathlib.Path, required=True)
parser.add_argument("--evidence", type=pathlib.Path, required=True)
args = parser.parse_args()
args.evidence.mkdir(parents=True, exist_ok=True)

root = pathlib.Path(tempfile.mkdtemp(prefix="zerofs-wire-", dir=args.evidence))
mount = root / "mount"
mount.mkdir(exist_ok=True)
config = root / "config.toml"
config.write_text(f'''[cache]
dir="{root / "cache"}"
disk_size_gb=0.064
memory_size_gb=0.001
[storage]
url="memory:///"
encryption_password="synthetic-wire-evidence"
[servers.ninep]
addresses=["127.0.0.1:5640"]
[telemetry]
enabled=false
''')
phase = "startup"
frames = []
connections = []


def copy(src, dst, capture):
    buf = b""
    try:
        while data := src.recv(65536):
            if capture:
                buf += data
                while len(buf) >= 4:
                    size = struct.unpack_from("<I", buf)[0]
                    if len(buf) < size:
                        break
                    frame, buf = buf[:size], buf[size:]
                    frames.append((phase, frame))
            dst.sendall(data)
    except OSError:
        pass
    finally:
        try:
            dst.shutdown(socket.SHUT_WR)
        except OSError:
            pass


listener = socket.socket()
listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
listener.bind(("127.0.0.1", 5641))
listener.listen()


def proxy():
    while True:
        try:
            client, _ = listener.accept()
        except OSError:
            return
        server = socket.create_connection(("127.0.0.1", 5640))
        connections.extend([client, server])
        threading.Thread(target=copy, args=(client, server, True), daemon=True).start()
        threading.Thread(target=copy, args=(server, client, False), daemon=True).start()


threading.Thread(target=proxy, daemon=True).start()
log = open(args.evidence / "server.log", "w")
server = subprocess.Popen(
    [str(args.binary.resolve()), "run", "-c", str(config)], stdout=log, stderr=log
)
try:
    for _ in range(300):
        if server.poll() is not None:
            raise RuntimeError("server startup failed")
        try:
            socket.create_connection(("127.0.0.1", 5640), timeout=0.1).close()
            break
        except OSError:
            time.sleep(0.1)
    subprocess.run(
        [
            "mount",
            "-t",
            "9p",
            "-o",
            "trans=tcp,port=5641,version=9p2000.L,cache=none,msize=1048576",
            "127.0.0.1",
            str(mount),
        ],
        check=True,
        timeout=15,
    )
    target = mount / "file"
    target.write_text("synthetic")
    os.setxattr(target, "user.probe", b"before")
    phase = "set-empty-replace"
    os.setxattr(target, "user.probe", b"", os.XATTR_REPLACE)
    try:
        empty_result = {"value_hex": os.getxattr(target, "user.probe").hex()}
    except OSError as e:
        empty_result = {"errno": e.errno}
    phase = "restore"
    os.setxattr(target, "user.probe", b"before")
    phase = "remove"
    os.removexattr(target, "user.probe")
    try:
        remove_result = {"value_hex": os.getxattr(target, "user.probe").hex()}
    except OSError as e:
        remove_result = {"errno": e.errno}
    rows = []
    for op, frame in frames:
        if frame[4] != 32:
            continue
        fid = struct.unpack_from("<I", frame, 7)[0]
        length = struct.unpack_from("<H", frame, 11)[0]
        name = frame[13 : 13 + length].decode()
        size, flags = struct.unpack_from("<QI", frame, 13 + length)
        rows.append(
            {
                "phase": op,
                "type": 32,
                "fid": fid,
                "name": name,
                "size": size,
                "flags": flags,
                "frame_hex": frame.hex(),
                "semantic_body_hex": frame[11:].hex(),
            }
        )
    chosen = {
        row["phase"]: row
        for row in rows
        if row["phase"] in {"set-empty-replace", "remove"}
    }
    assert (
        chosen["set-empty-replace"]["semantic_body_hex"]
        == chosen["remove"]["semantic_body_hex"]
    ), chosen
    assert empty_result == remove_result == {"errno": errno.ENODATA}, (
        empty_result,
        remove_result,
    )
    result = {
        "kernel": os.uname().release,
        "set_empty_result": empty_result,
        "remove_result": remove_result,
        "xattrcreate_requests": rows,
        "indistinguishable": True,
        "write_requests_in_zero_size_phases": [
            {"phase": op, "frame_hex": f.hex()}
            for op, f in frames
            if op in chosen and f[4] == 118
        ],
    }
    (args.evidence / "requests.json").write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps(result), flush=True)
finally:
    if os.path.ismount(mount):
        subprocess.run(["umount", str(mount)], check=True, timeout=15)
    server.terminate()
    try:
        server.wait(timeout=15)
    except subprocess.TimeoutExpired:
        server.kill()
        server.wait(timeout=5)
    for s in connections:
        s.close()
    listener.close()
    log.close()
    shutil.rmtree(root)
    assert not root.exists()
    print("owned mount/fixture removed; server/proxy stopped", flush=True)
