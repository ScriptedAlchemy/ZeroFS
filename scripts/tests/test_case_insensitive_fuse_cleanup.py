"""The FUSE smoke test must release its services when unmounting fails."""

import contextlib
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
                    with self.assertRaises(type(unmount_error)):
                        smoke.check("/unused/zerofs", False)
                for process in [mount, server]:
                    process.terminate.assert_called_once_with()
                    process.wait.assert_called_once_with(timeout=20)


if __name__ == "__main__":
    unittest.main()
