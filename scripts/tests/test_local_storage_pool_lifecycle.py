"""Check the pool smoke runner with owned, socket-free child processes."""

import importlib.util
import json
import os
from pathlib import Path
import shutil
import signal
import subprocess
import sys
import tempfile
import time
import unittest
from unittest import mock


SCRIPT = Path(__file__).resolve().parents[1] / "test-local-storage-pool.py"
SPEC = importlib.util.spec_from_file_location("local_storage_pool_lifecycle", SCRIPT)
SMOKE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(SMOKE)


class LocalStoragePoolEnvironmentTests(unittest.TestCase):
    def test_only_explicit_fixture_can_enable_injected_disk_health(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            child = root / "record-environment"
            child.write_text(
                f"#!{sys.executable}\n"
                "import json, os, sys\n"
                "json.dump({'health': os.environ.get('ZEROFS_TEST_DISK_HEALTH')}, sys.stdout)\n"
            )
            child.chmod(0o700)
            inherited = str(root / "unrelated-events.json")
            for events in (None, root / "owned-events.json"):
                with self.subTest(events=events), \
                        mock.patch.dict(os.environ, {"ZEROFS_TEST_DISK_HEALTH": inherited}):
                    server = SMOKE.Server(child, root / "unused.toml", root, "server-env",
                                          time.monotonic() + 10, events)
                    # This child records its environment and exits; no listener
                    # is created and the real Server startup cleanup reaps it.
                    with self.assertRaisesRegex(SMOKE.SmokeFailure, "server exited before"):
                        server.__enter__()
                    observed = json.loads(server.log_path.read_text())
                    self.assertEqual(observed["health"], str(events) if events is not None else None)
                    self.assertEqual(os.environ["ZEROFS_TEST_DISK_HEALTH"], inherited)
                    self.assertEqual(server.process.poll(), 0)
                    self.assertIsNone(server.log)


class LocalStoragePoolCancellationTests(unittest.TestCase):
    def process_alive(self, pid):
        try:
            # A detached child killed during negative-control cleanup may be a
            # zombie until the system reaps it; it cannot keep running or hold IO.
            state = Path(f"/proc/{pid}/stat").read_text().split(") ", 1)[1].split()[0]
            return state not in ("Z", "X")
        except FileNotFoundError:
            return False

    def test_cancellation_reaps_owned_servers_across_lifecycle(self):
        cases = (("spawn", signal.SIGTERM, False, False),
                 ("startup", signal.SIGTERM, False, False), ("running", signal.SIGTERM, False, False),
                 ("startup", signal.SIGINT, True, False), ("running", signal.SIGTERM, True, True),
                 ("shutdown", signal.SIGTERM, True, False), ("shutdown", signal.SIGINT, True, False))
        for phase, signum, ignore_term, repeat in cases:
            with self.subTest(phase=phase, signal=signum, ignore_term=ignore_term, repeat=repeat), \
                    tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                marker = root / "child.json"
                term_observed = root / "term-observed"
                server_root_marker = root / "server-root"
                child = root / "owned-child"
                child.write_text(
                    f"#!{sys.executable}\n"
                    "import json, os, signal, sys, time\nfrom pathlib import Path\n"
                    + ("def observe_term(signum, frame):\n"
                       f"    Path({str(term_observed)!r}).write_text('TERM')\n"
                       "signal.signal(signal.SIGTERM, observe_term)\n" if ignore_term else "")
                    + f"marker = Path({str(marker)!r})\n"
                    + "temporary = marker.with_suffix('.next')\n"
                    + "temporary.write_text(json.dumps(dict(pid=os.getpid(), root=str(Path(sys.argv[-1]).parent))))\n"
                    + "temporary.replace(marker)\n"
                    + "time.sleep(60)\n"
                )
                child.chmod(0o700)
                runner = root / "runner.py"
                runner.write_text(
                    "import importlib.util, os, signal, subprocess, sys, time\nfrom pathlib import Path\n"
                    f"spec = importlib.util.spec_from_file_location('smoke', {str(SCRIPT)!r})\n"
                    "module = importlib.util.module_from_spec(spec)\nspec.loader.exec_module(module)\n"
                    + ("popen = module.subprocess.Popen\n"
                       "def cancel_before_ownership(*args, **kwargs):\n"
                       "    child = popen(*args, **kwargs)\n"
                       "    deadline = time.monotonic() + 5\n"
                       f"    while not Path({str(marker)!r}).exists():\n"
                       "        if time.monotonic() >= deadline:\n"
                       "            child.kill()\n"
                       "            child.wait(timeout=5)\n"
                       "            raise RuntimeError('owned spawn fixture did not initialize')\n"
                       "        time.sleep(0.01)\n"
                       f"    os.kill(os.getpid(), {int(signum)})\n"
                       "    return child\n"
                       "module.subprocess.Popen = cancel_before_ownership\n" if phase == "spawn" else "")
                    + "class ActiveServer(module.Server):\n"
                    "    def __enter__(self):\n"
                    "        self.log = self.log_path.open('wb')\n"
                    "        self.process = subprocess.Popen(\n"
                    "            [str(self.binary), 'run', '--config', str(self.config)],\n"
                    "            stdin=subprocess.DEVNULL, stdout=self.log,\n"
                    "            stderr=subprocess.STDOUT, start_new_session=True)\n"
                    "        return self\n"
                    "def smoke(binary, root):\n"
                    + f"    Path({str(server_root_marker)!r}).write_text(str(root))\n"
                    + ("    server_type = module.Server\n" if phase in ("spawn", "startup")
                       else "    server_type = ActiveServer\n")
                    + f"    with server_type(Path({str(child)!r}), root / 'unused.toml', root,\n"
                    "                     'server-cancel', time.monotonic() + 30):\n"
                    + (f"        while not Path({str(marker)!r}).exists():\n"
                       "            time.sleep(0.01)\n" if phase == "shutdown"
                       else "        time.sleep(60)\n")
                    + "    return {'result': 'passed'}\n"
                    "module.smoke = smoke\n"
                    "sys.argv = [str(spec.origin), '--binary', sys.executable]\n"
                    "raise SystemExit(module.main())\n"
                )
                process = subprocess.Popen([sys.executable, str(runner)], stdout=subprocess.PIPE,
                                           stderr=subprocess.PIPE, text=True)
                pid = None

                def wait_for(path):
                    deadline = time.monotonic() + 5
                    while not path.exists():
                        if process.poll() is not None:
                            self.fail(f"owned fixture exited before {path.name}: {process.communicate()}")
                        self.assertLess(time.monotonic(), deadline, f"owned fixture did not reach {path.name}")
                        time.sleep(0.01)

                try:
                    wait_for(marker)
                    child_info = json.loads(marker.read_text())
                    pid = child_info["pid"]
                    if phase == "shutdown":
                        # Normal stop has sent TERM and is waiting for this
                        # deliberately non-cooperative child before cancellation.
                        wait_for(term_observed)
                    if phase != "spawn":
                        process.send_signal(signum)
                    if repeat:
                        wait_for(term_observed)
                        process.send_signal(signal.SIGINT)
                        process.send_signal(signal.SIGTERM)
                    stdout, stderr = process.communicate(timeout=10)
                    self.assertEqual(process.returncode, 128 + signum, stderr)
                    self.assertEqual(stdout, "")
                    self.assertIn(f"cancelled by {signal.Signals(signum).name}", stderr)
                    self.assertFalse(self.process_alive(pid), "cancelled runner left its detached server alive")
                    self.assertFalse(Path(f"/proc/{pid}").exists(), "owned server was not reaped")
                    self.assertFalse(Path(child_info["root"]).exists(), "owned server storage was not cleaned")
                    if ignore_term and phase != "shutdown":
                        self.assertIn("server required SIGKILL", stderr)
                finally:
                    if process.poll() is None:
                        process.kill()
                        process.wait(timeout=5)
                    if pid is None and marker.exists():
                        pid = json.loads(marker.read_text())["pid"]
                    if pid is not None and self.process_alive(pid):
                        os.killpg(pid, signal.SIGKILL)
                    if server_root_marker.exists():
                        shutil.rmtree(server_root_marker.read_text(), ignore_errors=True)


if __name__ == "__main__":
    unittest.main()
