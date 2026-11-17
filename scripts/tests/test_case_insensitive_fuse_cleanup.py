"""The FUSE smoke test must release its services when unmounting fails."""

import contextlib
import errno
import importlib.util
import io
from pathlib import Path
import subprocess
import unittest
from unittest.mock import Mock, patch


SCRIPT = Path(__file__).resolve().parents[1] / "test-case-insensitive-fuse.py"
SPEC = importlib.util.spec_from_file_location("case_insensitive_fuse", SCRIPT)
smoke = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(smoke)


class CleanupTests(unittest.TestCase):
    def run_successful_fixture(self, server, mount):
        fixtures = []
        processes = [server, mount]
        open_file = smoke.os.open

        def popen(command, **kwargs):
            if command[1] == "run":
                fixtures.append(Path(command[-1]).parent)
            return processes.pop(0)

        stat_path = Path.stat

        def exclusive_open(path, flags, *args, **kwargs):
            if Path(path).name == "FOO" and flags & smoke.os.O_EXCL:
                raise FileExistsError(errno.EEXIST, "fold-equal create")
            return open_file(path, flags, *args, **kwargs)

        def exact_stat(path, *args, **kwargs):
            # The mount looks names up by exact spelling. The local directory standing in for it
            # may fold case (APFS does), so a lookup must match a listed name byte for byte.
            if (
                fixtures
                and path.parent == fixtures[0] / "mount"
                and path.name not in smoke.os.listdir(path.parent)
            ):
                raise FileNotFoundError(errno.ENOENT, "no exact spelling", str(path))
            return stat_path(path, *args, **kwargs)

        # Use a local directory for the successful body and inject cleanup faults.
        try:
            with (
                patch.object(smoke.subprocess, "Popen", side_effect=popen),
                patch.object(smoke, "wait_for"),
                patch.object(smoke.os.path, "ismount", return_value=False),
                patch.object(smoke.os, "open", side_effect=exclusive_open),
                patch.object(smoke.Path, "stat", exact_stat),
                contextlib.redirect_stdout(io.StringIO()),
            ):
                smoke.check("/unused/zerofs", False)
        finally:
            self.assertFalse(fixtures[0].exists(), "fixture was not removed")

    def test_normal_cleanup_succeeds(self):
        server, mount = Mock(), Mock()
        self.run_successful_fixture(server, mount)
        for process in [mount, server]:
            process.terminate.assert_called_once_with()
            process.wait.assert_called_once_with(timeout=20)
            process.kill.assert_not_called()

    def test_forced_kill_fails_after_stopping_both_processes(self):
        server, mount = Mock(), Mock()
        mount.wait.side_effect = [subprocess.TimeoutExpired("mount", 20), 0]
        with self.assertRaisesRegex(RuntimeError, "kill"):
            self.run_successful_fixture(server, mount)
        mount.kill.assert_called_once_with()
        server.terminate.assert_called_once_with()
        server.wait.assert_called_once_with(timeout=20)

    def test_failed_kill_still_stops_server(self):
        server, mount = Mock(), Mock()
        mount.wait.side_effect = subprocess.TimeoutExpired("mount", 20)
        mount.kill.side_effect = OSError("kill failed")
        with self.assertRaisesRegex(Exception, "kill failed"):
            self.run_successful_fixture(server, mount)
        server.terminate.assert_called_once_with()
        server.wait.assert_called_once_with(timeout=20)

    def test_failed_unmount_still_terminates_server_and_mount(self):
        for unmount_error in [
            subprocess.CalledProcessError(1, ["fusermount3"]),
            subprocess.TimeoutExpired(["fusermount3"], 20),
        ]:
            with self.subTest(error=type(unmount_error).__name__):
                server = Mock()
                mount = Mock()
                with (
                    patch.object(smoke.subprocess, "Popen", side_effect=[server, mount]),
                    patch.object(
                        smoke, "wait_for", side_effect=[None, RuntimeError("mount failed")]
                    ),
                    patch.object(smoke.os.path, "ismount", return_value=True),
                    patch.object(smoke.subprocess, "run", side_effect=unmount_error),
                    contextlib.redirect_stdout(io.StringIO()),
                ):
                    with self.assertRaisesRegex(RuntimeError, "unmount failed"):
                        smoke.check("/unused/zerofs", False)
                for process in [mount, server]:
                    process.terminate.assert_called_once_with()
                    process.wait.assert_called_once_with(timeout=20)


if __name__ == "__main__":
    unittest.main()
