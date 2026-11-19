"""The mounted raw fixture must fail when its required WebUI assets fail."""

import importlib.util
from pathlib import Path
import sys
import unittest
from unittest import mock


SCRIPTS = Path(__file__).resolve().parents[1] / "raw-device"
FIXTURE_SPEC = importlib.util.spec_from_file_location("raw_device_fixture", SCRIPTS / "fixture.py")
FIXTURE = importlib.util.module_from_spec(FIXTURE_SPEC)
FIXTURE_SPEC.loader.exec_module(FIXTURE)
SPEC = importlib.util.spec_from_file_location("raw_device_mounted", SCRIPTS / "mounted.py")
MOUNTED = importlib.util.module_from_spec(SPEC)
with mock.patch.dict(sys.modules, {"fixture": FIXTURE}):
    SPEC.loader.exec_module(MOUNTED)


class WebUIQualificationTests(unittest.TestCase):
    def responses(self):
        return {
            "/": (200, "text/html", b'<script src="/assets/main.js"></script>'
                   b'<link rel="modulepreload" href="/assets/vendor.js">'
                   b'<link rel="stylesheet" href="/assets/main.css">'),
            "/assets/main.js": (200, "application/javascript", b"import './vendor.js';"),
            "/assets/vendor.js": (200, "text/javascript; charset=utf-8", b"export const x = 1;"),
            "/assets/main.css": (200, "text/css; charset=utf-8", b"body { color: black; }"),
        }

    def probe(self, responses):
        connections = []

        def connect(*args, **kwargs):
            connection = mock.Mock()

            def request(method, path):
                self.assertEqual(method, "GET")
                status, content_type, body = responses[path]
                response = mock.Mock(status=status)
                response.getheader.return_value = content_type
                response.read.return_value = body
                connection.getresponse.return_value = response

            connection.request.side_effect = request
            connections.append(connection)
            return connection

        config = mock.Mock()
        config.read_text.return_value = '[servers.webui]\naddresses = ["127.0.0.1:8080"]\n'
        with (
            mock.patch.object(MOUNTED.http.client, "HTTPConnection", side_effect=connect),
            mock.patch.object(MOUNTED, "record") as record,
        ):
            try:
                MOUNTED.report_webui(config)
            finally:
                for connection in connections:
                    connection.close.assert_called_once_with()
            return record.call_args

    def test_accepts_and_records_working_webui(self):
        result = self.probe(self.responses())
        self.assertEqual(result.args, ("webui_assets",))
        self.assertTrue(result.kwargs["javascript_served"])
        self.assertEqual(set(result.kwargs["assets"]),
                         {"/assets/main.js", "/assets/vendor.js", "/assets/main.css"})

    def test_rejects_missing_entrypoint(self):
        for replacement in (
            (404, "text/html", b"not found"),
            (200, "text/html", b"no asset references"),
        ):
            with self.subTest(response=replacement):
                responses = self.responses()
                responses["/"] = replacement
                with self.assertRaisesRegex(AssertionError, "WebUI"):
                    self.probe(responses)

    def test_rejects_any_broken_referenced_asset(self):
        for path, replacement in (
            ("/assets/main.js", (404, "text/html", b"not found")),
            ("/assets/main.js", (200, "application/javascript", b"")),
            ("/assets/vendor.js", (200, "text/html", b"fallback page")),
            ("/assets/main.css", (404, "text/html", b"not found")),
            ("/assets/main.css", (200, "text/html", b"fallback page")),
        ):
            with self.subTest(path=path, response=replacement):
                responses = self.responses()
                responses[path] = replacement
                with self.assertRaisesRegex(AssertionError, "WebUI"):
                    self.probe(responses)


if __name__ == "__main__":
    unittest.main()
