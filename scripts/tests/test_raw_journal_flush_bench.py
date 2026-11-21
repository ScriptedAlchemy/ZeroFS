"""Flush-rate probes must write every byte they report before syncing."""

import errno
import importlib.util
import itertools
from pathlib import Path
import sys
import unittest
from unittest import mock

from scripts.tests.test_raw_device_large import FIXTURE, LARGE, MOUNTED


SCRIPTS = Path(__file__).resolve().parents[1] / "raw-device"
SPEC = importlib.util.spec_from_file_location(
    "raw_journal_flush_bench", SCRIPTS / "journal_flush_bench.py"
)
BENCH = importlib.util.module_from_spec(SPEC)
with mock.patch.dict(sys.modules, {"fixture": FIXTURE, "mounted": MOUNTED,
                                  "large_mounted": LARGE}):
    SPEC.loader.exec_module(BENCH)


class FlushProbeTests(unittest.TestCase):
    def test_sequential_probe_completes_short_writes(self):
        written = bytearray(7 * 32)

        def pwrite(_fd, body, offset):
            count = min(2, len(body))
            written[offset:offset + count] = body[:count]
            return count

        with (
            mock.patch.object(BENCH, "MIB", 1),
            mock.patch.object(BENCH.os, "urandom", return_value=b"abcd"),
            mock.patch.object(BENCH.os, "pwrite", side_effect=pwrite),
            mock.patch.object(BENCH.os, "fdatasync") as sync,
            mock.patch.object(BENCH.time, "monotonic_ns", side_effect=itertools.count(0, 1000)),
        ):
            result = BENCH.sequential_batches(123)

        self.assertEqual(written, b"abcd" * (7 * 8))
        self.assertEqual(sync.call_count, 7)
        self.assertEqual(result["count"], 7)

    def test_sequential_probe_rejects_zero_progress(self):
        with (
            mock.patch.object(BENCH.os, "pwrite", return_value=0),
            mock.patch.object(BENCH.os, "fdatasync") as sync,
            self.assertRaises(OSError),
        ):
            BENCH.sequential_batches(123)
        sync.assert_not_called()

    def test_sequential_probe_propagates_a_partial_write_failure(self):
        failure = OSError(errno.ENOSPC, "probe device full")
        with (
            mock.patch.object(BENCH.os, "pwrite", side_effect=[2, failure]),
            mock.patch.object(BENCH.os, "fdatasync") as sync,
            self.assertRaises(OSError) as caught,
        ):
            BENCH.sequential_batches(123)
        self.assertIs(caught.exception, failure)
        sync.assert_not_called()


if __name__ == "__main__":
    unittest.main()
