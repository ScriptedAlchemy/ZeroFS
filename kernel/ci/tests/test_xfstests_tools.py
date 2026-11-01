import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[3]
HELPER = ROOT / "scripts/ci/xfstests-gnu-tools.sh"
TOOLS = ("ln", "ls", "mkdir", "mknod", "od", "touch")


class XfstestsGnuToolsTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.suite = self.root / "xfstests"
        (self.suite / "common").mkdir(parents=True)
        (self.suite / "common/preamble").touch()
        self.bin = self.root / "bin"
        self.bin.mkdir()
        self.env = {**os.environ, "LC_ALL": "C"}

    def gnu_tool(self, tool):
        for name in (f"gnu{tool}", tool):
            candidate = shutil.which(name)
            if candidate and "GNU coreutils" in subprocess.check_output(
                [candidate, "--version"], text=True, env=self.env
            ):
                return candidate
        self.fail(f"GNU {tool} is required to run these Linux CI tests")

    def prepare(self):
        return subprocess.run(
            ["bash", str(HELPER), str(self.suite), str(self.bin)],
            text=True, capture_output=True, env=self.env,
        )

    def test_prefixed_gnu_tools_preserve_xfstests_semantics(self):
        for tool in TOOLS:
            (self.bin / f"gnu{tool}").symlink_to(self.gnu_tool(tool))
            replacement = self.bin / tool
            replacement.write_text("#!/bin/sh\necho 'uutils coreutils'\n")
            replacement.chmod(0o755)
        prepared = self.prepare()
        self.assertEqual(prepared.returncode, 0, prepared.stderr)
        for tool in TOOLS:
            self.assertEqual(os.readlink(self.suite / tool), str(self.bin / f"gnu{tool}"))

        # generic/452 copies ls under a new basename. A multicall uutils
        # binary rejects that basename; GNU ls must still list the file.
        renamed = self.root / "ls_on_scratch"
        shutil.copy2(self.suite / "ls", renamed)
        listed = subprocess.run([str(renamed), str(renamed)], text=True,
                                capture_output=True, env=self.env)
        self.assertEqual(listed.returncode, 0, listed.stderr)
        self.assertEqual(listed.stdout, f"{renamed}\n")

        # generic/634 expects GNU's date parser to accept this extreme year.
        touched = subprocess.run(
            [str(self.suite / "touch"), "-d", "Dec 31 23:59:59 UTC 2147483647",
             str(self.root / "timestamp")],
            text=True, capture_output=True, env=self.env,
        )
        self.assertEqual(touched.returncode, 0, touched.stderr)
        self.assertEqual(touched.stderr, "")

        # generic/294 compares GNU's mkdir diagnostic for an existing path.
        made = subprocess.run([str(self.suite / "mkdir"), str(self.root)],
                              text=True, capture_output=True, env=self.env)
        self.assertNotEqual(made.returncode, 0)
        self.assertIn(f"cannot create directory '{self.root}': File exists", made.stderr)

    def test_unprefixed_gnu_host_is_supported_and_setup_is_repeatable(self):
        for tool in TOOLS:
            (self.bin / tool).symlink_to(self.gnu_tool(tool))
        for _ in range(2):
            prepared = self.prepare()
            self.assertEqual(prepared.returncode, 0, prepared.stderr)

    def test_non_gnu_tools_fail_instead_of_changing_test_expectations(self):
        replacement = self.bin / "ln"
        replacement.write_text("#!/bin/sh\necho 'uutils coreutils'\n")
        replacement.chmod(0o755)
        prepared = self.prepare()
        self.assertNotEqual(prepared.returncode, 0)
        self.assertIn("Required GNU coreutils executable is missing: ln", prepared.stderr)

    def test_existing_suite_file_is_not_overwritten(self):
        (self.bin / "ln").symlink_to(self.gnu_tool("ln"))
        existing = self.suite / "ln"
        existing.write_text("owned by the suite\n")
        prepared = self.prepare()
        self.assertNotEqual(prepared.returncode, 0)
        self.assertEqual(existing.read_text(), "owned by the suite\n")


if __name__ == "__main__":
    unittest.main()
