"""Exercise fixture transport, validation, and the ZFS setup failure boundary."""

from contextlib import contextmanager, redirect_stderr, redirect_stdout
import gzip
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import importlib.util
import io
import os
from pathlib import Path
import re
import subprocess
import tarfile
import tempfile
import textwrap
import threading
import time
import unittest


ROOT = Path(__file__).resolve().parents[2]
SPEC = importlib.util.spec_from_file_location("fetch_linux_source", ROOT / "scripts/ci/fetch-linux-source.py")
FETCH = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(FETCH)
VERSION = "7.2-rc1"


def archive_bytes(prefix="linux-7.2-rc1", *, links=(), missing=False):
    payload = io.BytesIO()
    with tarfile.open(fileobj=payload, mode="w:gz") as contents:
        for name in ("Makefile", "Kconfig", "MAINTAINERS", "init/main.c"):
            if missing and name == "init/main.c":
                continue
            member = tarfile.TarInfo(f"{prefix}/{name}")
            data = f"fixture {name}\n".encode()
            member.size = len(data)
            contents.addfile(member, io.BytesIO(data))
        for name, target, kind in links:
            member = tarfile.TarInfo(f"{prefix}/{name}")
            member.type, member.linkname = kind, target
            contents.addfile(member)
    return payload.getvalue()


@contextmanager
def serve(responses, *, delay=0):
    requests = []

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            requests.append(self.path)
            status, payload = responses[min(len(requests) - 1, len(responses) - 1)]
            self.send_response(status)
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            if delay:
                time.sleep(delay)
            try:
                self.wfile.write(payload)
            except (BrokenPipeError, ConnectionResetError):
                pass

        def log_message(self, *args):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, kwargs={"poll_interval": 0.01}, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}", requests
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)


class LinuxSourceDownloadTests(unittest.TestCase):
    def fetch(self, output, urls, **kwargs):
        with redirect_stdout(io.StringIO()), redirect_stderr(io.StringIO()):
            FETCH.fetch_archive(VERSION, output, urls=urls, backoff=0, **kwargs)

    def test_transient_http_failure_recovers(self):
        valid = archive_bytes()
        with tempfile.TemporaryDirectory() as directory, serve([(503, b"unavailable"), (200, valid)]) as (url, requests):
            output = Path(directory) / "linux.tar.gz"
            self.fetch(output, [url])
            self.assertEqual(output.read_bytes(), valid)
            self.assertEqual(len(requests), 2)
            self.assertEqual(list(Path(directory).iterdir()), [output])

    def test_truncated_gzip_is_retried_even_after_http_success(self):
        valid = archive_bytes()
        # The tar end markers are present but the gzip CRC/length trailer is not.
        with tempfile.TemporaryDirectory() as directory, serve([(200, valid[:-8]), (200, valid)]) as (url, requests):
            output = Path(directory) / "linux.tar.gz"
            self.fetch(output, [url])
            self.assertEqual(output.read_bytes(), valid)
            self.assertEqual(len(requests), 2)

    def test_invalid_archive_falls_back_to_second_source(self):
        valid = archive_bytes()
        with tempfile.TemporaryDirectory() as directory, serve([(200, b"<html>rate limited</html>"),
                                                              (200, b"not gzip"), (200, valid)]) as (url, requests):
            output = Path(directory) / "linux.tar.gz"
            self.fetch(output, [f"{url}/first", f"{url}/second"])
            self.assertEqual(requests, ["/first", "/first", "/second"])
            self.assertEqual(output.read_bytes(), valid)

    def test_exhausted_sources_leave_existing_archive_untouched(self):
        with tempfile.TemporaryDirectory() as directory, serve([(200, archive_bytes(missing=True))]) as (url, requests):
            output = Path(directory) / "linux.tar.gz"
            output.write_bytes(b"previous archive")
            with self.assertRaisesRegex(RuntimeError, "could not fetch a valid"):
                self.fetch(output, [f"{url}/first", f"{url}/second"])
            self.assertEqual(len(requests), 4)
            self.assertEqual(output.read_bytes(), b"previous archive")
            self.assertEqual(list(Path(directory).iterdir()), [output])

    def test_stalled_download_is_bounded(self):
        with tempfile.TemporaryDirectory() as directory, serve([(200, archive_bytes())], delay=2) as (url, requests):
            output = Path(directory) / "linux.tar.gz"
            started = time.monotonic()
            with self.assertRaises(RuntimeError):
                self.fetch(output, [url], timeout=0.1, attempts=1)
            self.assertLess(time.monotonic() - started, 1.5)
            self.assertEqual(len(requests), 1)
            self.assertEqual(list(Path(directory).iterdir()), [])

    def test_wrong_tree_unsafe_paths_links_and_crc_are_rejected(self):
        valid = archive_bytes()
        invalid = (
            archive_bytes(prefix="linux-wrong"),
            archive_bytes(prefix="../linux-7.2-rc1"),
            archive_bytes(links=[("outside", "../../outside", tarfile.SYMTYPE)]),
            archive_bytes(links=[("outside", "/outside", tarfile.LNKTYPE)]),
            valid[:-8] + bytes([valid[-8] ^ 1]) + valid[-7:],
            gzip.compress(b"not a tar archive"),
        )
        with tempfile.TemporaryDirectory() as directory:
            archive = Path(directory) / "linux.tar.gz"
            for payload in invalid:
                with self.subTest(payload=payload[:25]):
                    archive.write_bytes(payload)
                    with self.assertRaises((ValueError, OSError, tarfile.TarError)):
                        FETCH.validate_archive(archive, VERSION)

    def test_in_tree_links_are_accepted(self):
        with tempfile.TemporaryDirectory() as directory:
            archive = Path(directory) / "linux.tar.gz"
            archive.write_bytes(archive_bytes(links=[("init/Makefile", "../Makefile", tarfile.SYMTYPE)]))
            FETCH.validate_archive(archive, VERSION)

    def test_link_chains_forward_references_and_cycles_are_rejected(self):
        cases = (
            [("a", ".", tarfile.SYMTYPE), ("b", "a/..", tarfile.SYMTYPE)],
            [("b", "a/..", tarfile.SYMTYPE), ("a", ".", tarfile.SYMTYPE)],
            [("a", "b", tarfile.SYMTYPE), ("b", "a", tarfile.SYMTYPE)],
            [("a", ".", tarfile.SYMTYPE), ("b", "linux-7.2-rc1/a/..", tarfile.LNKTYPE)],
            [("a", "Makefile", tarfile.SYMTYPE), ("a", ".", tarfile.SYMTYPE)],
            [("a", ".", tarfile.SYMTYPE), ("a/b", "..", tarfile.SYMTYPE)],
            [("init", ".", tarfile.SYMTYPE)],
        )
        with tempfile.TemporaryDirectory() as directory:
            archive = Path(directory) / "linux.tar.gz"
            for links in cases:
                with self.subTest(links=links):
                    archive.write_bytes(archive_bytes(links=links))
                    with self.assertRaises(ValueError):
                        FETCH.validate_archive(archive, VERSION)

    def test_valid_forward_link_chains_remain_accepted(self):
        with tempfile.TemporaryDirectory() as directory:
            archive = Path(directory) / "linux.tar.gz"
            archive.write_bytes(archive_bytes(links=[
                ("init/a", "../b", tarfile.SYMTYPE),
                ("b", "Makefile", tarfile.SYMTYPE),
            ]))
            FETCH.validate_archive(archive, VERSION)

    def test_hard_links_are_rejected_even_when_target_is_inside_tree(self):
        with tempfile.TemporaryDirectory() as directory:
            archive = Path(directory) / "linux.tar.gz"
            archive.write_bytes(archive_bytes(links=[
                ("a", "linux-7.2-rc1/Makefile", tarfile.LNKTYPE),
            ]))
            with self.assertRaisesRegex(ValueError, "unexpected archive member type"):
                FETCH.validate_archive(archive, VERSION)

    def test_zfs_setup_keeps_archive_and_extraction_and_stops_on_download_failure(self):
        workflow = (ROOT / ".github/workflows/zfs-test.yml").read_text()
        blocks = re.findall(r"      - name: Download and extract Linux kernel\n"
                            r"        run: \|\n((?:          [^\n]*\n|\n)+)", workflow)
        self.assertEqual(len(blocks), 1)
        for failure in (False, True):
            with self.subTest(failure=failure), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                mount = root / "zfsdata"
                mount.mkdir()
                source = root / "fixture.tar.gz"
                source.write_bytes(archive_bytes())
                for name, script in {
                    "sudo": 'exec "$@"',
                    "pigz": 'exec gzip "$@"',
                    "python3": '[[ "$FAIL_DOWNLOAD" == 0 ]] || exit 22\ncp "$SOURCE_ARCHIVE" "${@: -1}"',
                }.items():
                    tool = root / name
                    tool.write_text("#!/usr/bin/env bash\nset -euo pipefail\n" + script + "\n")
                    tool.chmod(0o755)
                script = textwrap.dedent(blocks[0]).replace("/mnt/zfsdata", str(mount))
                result = subprocess.run(
                    ["bash", "-e", "-o", "pipefail", "-c", script],
                    env=dict(os.environ, PATH=f"{root}:{os.environ['PATH']}",
                             RUNNER_TEMP=str(root), GITHUB_WORKSPACE=str(ROOT),
                             SOURCE_ARCHIVE=str(source), FAIL_DOWNLOAD=str(int(failure))),
                    capture_output=True, text=True, timeout=10, check=False,
                )
                self.assertEqual(result.returncode, 22 if failure else 0, result.stderr)
                if failure:
                    self.assertEqual(list(mount.iterdir()), [])
                else:
                    self.assertEqual((mount / "linux-7.2-rc1.tar.gz").read_bytes(), source.read_bytes())
                    self.assertEqual((mount / "linux-7.2-rc1/init/main.c").read_text(), "fixture init/main.c\n")


if __name__ == "__main__":
    unittest.main()
