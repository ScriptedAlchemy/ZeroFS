"""The large raw qualification must surface failures from its sampler thread."""

import http.client
import importlib.util
from pathlib import Path
import sys
import threading
import unittest
from unittest import mock


SCRIPTS = Path(__file__).resolve().parents[1] / "raw-device"


def load(name, filename):
    spec = importlib.util.spec_from_file_location(name, SCRIPTS / filename)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


FIXTURE = load("raw_large_fixture", "fixture.py")
with mock.patch.dict(sys.modules, {"fixture": FIXTURE}):
    MOUNTED = load("raw_large_mounted", "mounted.py")
with mock.patch.dict(sys.modules, {"fixture": FIXTURE, "mounted": MOUNTED}):
    LARGE = load("raw_large_qualification", "large_mounted.py")


class RuntimeSamplerTests(unittest.TestCase):
    def probe(self, failure=None):
        metrics = dict.fromkeys(LARGE.METRICS, 0)
        metrics["enabled"] = 1
        sampled_again = threading.Event()
        attempts = 0

        def fetch(_address):
            nonlocal attempts
            attempts += 1
            if attempts > 1:
                sampled_again.set()
                if failure is not None:
                    raise failure
            return metrics.copy()

        configuration = mock.Mock()
        configuration.parent.name = "qualification"
        configuration.read_text.return_value = (
            '[prometheus]\naddresses = ["127.0.0.1:19091"]\n'
        )
        server = mock.Mock(pid=123)
        server.poll.return_value = None
        with (
            mock.patch.object(LARGE, "fetch_metrics", side_effect=fetch),
            mock.patch.object(LARGE, "keyed_lines", return_value={"anon": 100, "file": 200}),
            mock.patch.object(LARGE, "Path") as path,
            mock.patch.object(LARGE, "record") as record,
            mock.patch.object(threading, "excepthook") as thread_errors,
        ):
            path.return_value.read_text.return_value = "VmHWM:\t1 kB\n"
            with LARGE.monitor(configuration, server):
                self.assertTrue(sampled_again.wait(2), "sampler did not complete its second attempt")
            self.assertFalse(thread_errors.called, "sampler failure escaped its thread")
            return record.call_args

    def test_successful_samples_emit_peak(self):
        receipt = self.probe()
        self.assertEqual(receipt.args, ("mounted_runtime_peak",))
        self.assertGreaterEqual(receipt.kwargs["samples"], 2)

    def test_incomplete_http_response_fails_qualification(self):
        failure = http.client.IncompleteRead(b"truncated metrics", 100)
        with self.assertRaises(http.client.IncompleteRead) as caught:
            self.probe(failure)
        self.assertIs(caught.exception, failure)

    def test_unexpected_sample_parser_error_fails_qualification(self):
        failure = KeyError("anon")
        with self.assertRaises(KeyError) as caught:
            self.probe(failure)
        self.assertIs(caught.exception, failure)


if __name__ == "__main__":
    unittest.main()
