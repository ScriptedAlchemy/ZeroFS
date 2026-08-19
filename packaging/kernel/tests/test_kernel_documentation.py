import unittest
from pathlib import Path


REPOSITORY = Path(__file__).resolve().parents[3]
KERNEL_CLIENT_GUIDE = (
    REPOSITORY / "documentation/src/app/kernel-client/page.mdx"
)


class KernelDocumentationTest(unittest.TestCase):
    def test_guide_does_not_install_from_retired_kernel_channels(self):
        guide = KERNEL_CLIENT_GUIDE.read_text()

        self.assertNotIn("https://pkgs.zerofs.net/kernel/", guide)
        self.assertNotIn("/etc/apt/sources.list.d/zerofs-kernel.list", guide)
        self.assertIn("Prebuilt kernel-client repositories are unavailable", guide)


if __name__ == "__main__":
    unittest.main()
