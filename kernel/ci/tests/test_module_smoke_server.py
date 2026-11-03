"""Exercise the real smoke startup with owned loopback sockets, without QEMU."""

import os
from pathlib import Path
import shutil
import socket
import subprocess
import sys
import tempfile
import unittest


SMOKE = Path(__file__).resolve().parents[1] / "module-smoke.sh"


class ModuleSmokeServerTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.listener = socket.socket()
        self.addCleanup(self.listener.close)
        self.listener.bind(("127.0.0.1", 0))
        port = self.listener.getsockname()[1]
        self.script = self.root / "kernel/ci/module-smoke.sh"
        self.script.parent.mkdir(parents=True)
        source = SMOKE.read_text(encoding="utf-8")
        # Reserve an ephemeral port without listening; no port-reuse race or
        # collision with another test's fixed smoke listener.
        self.assertEqual(source.count("readonly smoke_port=15565"), 1)
        self.script.write_text(
            source.replace("readonly smoke_port=15565", f"readonly smoke_port={port}"),
            encoding="utf-8",
        )
        self.server = self.root / "server"
        self.server.write_text(
            f"#!{sys.executable}\n"
            "import os, signal, socket, time\n"
            "from pathlib import Path\n"
            "Path(os.environ['STARTED']).touch()\n"
            "if os.environ['MODE'] == 'exit':\n"
            "    raise SystemExit(1)\n"
            "time.sleep(0.1)\n"
            "listener = socket.socket(fileno=int(os.environ['LISTENER_FD']))\n"
            "listener.listen()\n"
            "signal.pause()\n",
            encoding="utf-8",
        )
        self.server.chmod(0o755)
        self.started = self.root / "started"
        self.bash = shutil.which("bash")
        self.assertIsNotNone(self.bash)

    def invoke(self, mode):
        return subprocess.run(
            [self.bash, "-c",
             'source "$1"; work_dir=$2; start_server "$3"; echo STARTUP_RETURNED',
             "smoke-test", str(self.script), str(self.root), str(self.server)],
            env={**os.environ, "MODE": mode, "STARTED": str(self.started),
                 "LISTENER_FD": str(self.listener.fileno())},
            pass_fds=(self.listener.fileno(),),
            capture_output=True,
            text=True,
            timeout=10,
        )

    def test_closed_port_allows_startup_and_waits_for_listener(self):
        result = self.invoke("listen")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("STARTUP_RETURNED", result.stdout)
        self.assertTrue(self.started.exists())
        self.assertEqual(self.listener.getsockopt(socket.SOL_SOCKET, socket.SO_ACCEPTCONN), 1)

    def test_occupied_port_rejects_before_starting_child(self):
        self.listener.listen()
        result = self.invoke("listen")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("already occupied", result.stderr)
        self.assertNotIn("STARTUP_RETURNED", result.stdout)
        self.assertFalse(self.started.exists())

    def test_child_exit_without_listener_is_not_ready(self):
        result = self.invoke("exit")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("ZeroFS did not become ready", result.stderr)
        self.assertNotIn("already occupied", result.stderr)
        self.assertNotIn("STARTUP_RETURNED", result.stdout)
        self.assertTrue(self.started.exists())


if __name__ == "__main__":
    unittest.main()
