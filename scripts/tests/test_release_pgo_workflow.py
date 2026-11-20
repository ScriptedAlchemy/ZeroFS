"""Contract tests for isolated PGO profile job recovery gates."""
import unittest
from pathlib import Path

WORKFLOW = Path(__file__).resolve().parents[2] / ".github" / "workflows" / "release-pgo.yml"


class ReleasePGOWorkflowTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.source = WORKFLOW.read_text()
        cls.profile = cls.source.split("  collect-pgo-profile:", 1)[1].split(
            "\n  merge-pgo-data:", 1
        )[0]

    def test_failed_profile_does_not_cancel_other_required_profiles(self):
        strategy = self.profile.split("\n    strategy:", 1)[1].split(
            "\n    steps:", 1
        )[0]
        self.assertIn("fail-fast: false", strategy)
        self.assertIn("test: [9p, fuse, nfs-kernel-compile]", strategy)
        self.assertIn("needs: collect-pgo-profile", self.source)


if __name__ == "__main__":
    unittest.main()
