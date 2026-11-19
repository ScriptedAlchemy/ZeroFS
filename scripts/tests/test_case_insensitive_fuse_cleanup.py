"""The FUSE smoke test must release its services when unmounting fails."""

import importlib.util
from pathlib import Path
import subprocess
import unittest
from unittest.mock import Mock, patch


SCRIPT = Path(__file__).resolve().parents[1] / "test-case-insensitive-fuse.py"
SPEC = importlib.util.spec_from_file_location("case_insensitive_fuse", SCRIPT)
smoke = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(smoke)


class CleanupTests(unittest.TestCase):
    def test_normal_cleanup_succeeds(self):
        server, mount = Mock(), Mock()
        with patch.object(smoke.os.path, "ismount", return_value=False):
            smoke.teardown("/unused/mount", [mount, server])
        for process in [mount, server]:
            process.terminate.assert_called_once_with()
            process.wait.assert_called_once_with(timeout=20)
            process.kill.assert_not_called()

    def test_forced_kill_fails_after_stopping_both_processes(self):
        server, mount = Mock(), Mock()
        mount.wait.side_effect = [subprocess.TimeoutExpired("mount", 20), 0]
        with patch.object(smoke.os.path, "ismount", return_value=False):
            with self.assertRaisesRegex(RuntimeError, "kill"):
                smoke.teardown("/unused/mount", [mount, server])
        mount.kill.assert_called_once_with()
        server.terminate.assert_called_once_with()
        server.wait.assert_called_once_with(timeout=20)

    def test_failed_kill_still_stops_server(self):
        server, mount = Mock(), Mock()
        mount.wait.side_effect = subprocess.TimeoutExpired("mount", 20)
        mount.kill.side_effect = OSError("kill failed")
        with patch.object(smoke.os.path, "ismount", return_value=False):
            with self.assertRaisesRegex(Exception, "kill failed"):
                smoke.teardown("/unused/mount", [mount, server])
        server.terminate.assert_called_once_with()
        server.wait.assert_called_once_with(timeout=20)

    def test_failed_unmount_still_terminates_server_and_mount(self):
        for unmount_error in [
            subprocess.CalledProcessError(1, ["fusermount3"]),
            subprocess.TimeoutExpired(["fusermount3"], 20),
        ]:
            with self.subTest(error=type(unmount_error).__name__):
                server, mount = Mock(), Mock()
                with (
                    patch.object(smoke.os.path, "ismount", return_value=True),
                    patch.object(smoke.subprocess, "run", side_effect=unmount_error),
                ):
                    with self.assertRaisesRegex(RuntimeError, "unmount failed"):
                        smoke.teardown("/unused/mount", [mount, server])
                for process in [mount, server]:
                    process.terminate.assert_called_once_with()
                    process.wait.assert_called_once_with(timeout=20)


if __name__ == "__main__":
    unittest.main()
