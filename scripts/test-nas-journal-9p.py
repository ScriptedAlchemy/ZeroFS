"""Minimal synchronous 9P2000.L client for the NAS journal fixture."""

import hashlib
import os
import socket
import struct


MSIZE = 256 * 1024
CHUNK = MSIZE - 24


def string(value):
    return struct.pack("<H", len(value)) + value


class NineP:
    def __init__(self, path, timeout):
        self.socket = socket.socket(socket.AF_UNIX)
        self.timeout = timeout
        self.tag = 0
        try:
            self.socket.settimeout(self.timeout())
            self.socket.connect(str(path))
            dialect = string(b"9P2000.L")
            reply = self.call(100, struct.pack("<I", MSIZE) + dialect, tag=65535)
            if reply != struct.pack("<I", MSIZE) + dialect:
                raise RuntimeError(f"unexpected 9P version response: {reply!r}")
            self.call(
                104,
                struct.pack("<II", 1, 0xFFFFFFFF)
                + string(b"root")
                + string(b"")
                + struct.pack("<I", 0),
            )
        except BaseException:
            self.socket.close()
            raise

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.socket.close()

    def receive(self, length):
        result = bytearray()
        while len(result) < length:
            self.socket.settimeout(self.timeout())
            data = self.socket.recv(length - len(result))
            if not data:
                raise EOFError("9P connection closed before the complete response")
            result.extend(data)
        return result

    def call(self, kind, body, *, tag=None, payload=b""):
        if tag is None:
            self.tag = (self.tag + 1) % 65535
            tag = self.tag
        self.socket.settimeout(self.timeout())
        header = struct.pack("<IBH", 7 + len(body) + len(payload), kind, tag) + body
        if payload:
            # Keep the pre-generated WRITE buffer out of Python frame copies.
            sent = self.socket.sendmsg([header, payload])
            if sent < len(header):
                self.socket.sendall(header[sent:])
            if sent < len(header) + len(payload):
                self.socket.sendall(payload[max(0, sent - len(header)) :])
        else:
            self.socket.sendall(header)
        length, reply_kind, reply_tag = struct.unpack("<IBH", self.receive(7))
        if not 7 <= length <= MSIZE or reply_tag != tag:
            raise RuntimeError(f"invalid 9P response header for request {kind}")
        reply = self.receive(length - 7)
        if reply_kind == 7 and len(reply) == 4:
            (errno,) = struct.unpack("<I", reply)
            raise OSError(errno, f"9P request {kind}: {os.strerror(errno)}")
        if reply_kind != kind + 1:
            raise RuntimeError(f"9P request {kind} got unexpected reply {reply_kind}")
        return reply

    def walk(self, name=None):
        names = b"" if name is None else string(name)
        reply = self.call(110, struct.pack("<IIH", 1, 2, bool(names)) + names)
        if len(reply) != (15 if names else 2) or reply[:2] != struct.pack(
            "<H", bool(names)
        ):
            raise RuntimeError(f"9P walk did not resolve {name!r}")

    def create(self, name):
        self.walk()
        # Use Linux wire flags even when reading this fixture on another host.
        reply = self.call(
            14,
            struct.pack("<I", 2) + string(name) + struct.pack("<III", 0x81, 0o600, 0),
        )
        self.io_chunk = self.open_chunk(reply)

    @staticmethod
    def open_chunk(reply):
        if len(reply) != 17:
            raise RuntimeError("invalid 9P open response")
        iounit = struct.unpack_from("<I", reply, 13)[0]
        return min(iounit or CHUNK, CHUNK)

    def write(self, payload):
        payload = memoryview(payload)
        offset = 0
        while offset < len(payload):
            chunk = payload[offset : offset + self.io_chunk]
            reply = self.call(
                118, struct.pack("<IQI", 2, offset, len(chunk)), payload=chunk
            )
            (count,) = struct.unpack("<I", reply)
            if not 0 < count <= len(chunk):
                raise RuntimeError(f"invalid 9P write count {count} at {offset}")
            offset += count

    def fsync(self):
        if self.call(50, struct.pack("<II", 2, 0)):
            raise RuntimeError("invalid 9P fsync response")

    def clunk(self):
        if self.call(120, struct.pack("<I", 2)):
            raise RuntimeError("invalid 9P clunk response")

    def read_hash(self, name, expected_size):
        self.walk(name)
        io_chunk = self.open_chunk(self.call(12, struct.pack("<II", 2, 0)))
        digest = hashlib.sha256()
        offset = 0
        while offset <= expected_size:
            count = min(io_chunk, expected_size + 1 - offset)
            reply = self.call(116, struct.pack("<IQI", 2, offset, count))
            (read_count,) = struct.unpack_from("<I", reply)
            data = reply[4:]
            if read_count != len(data) or read_count > count:
                raise RuntimeError(f"invalid 9P read count for {name!r} at {offset}")
            if not data:
                break
            digest.update(data)
            offset += read_count
        if offset != expected_size:
            raise RuntimeError(
                f"{name!r}: expected {expected_size} bytes, read {offset}"
            )
        self.clunk()
        return digest.hexdigest()
