import unittest
from pathlib import Path


REPOSITORY = Path(__file__).resolve().parents[3]
PUBLIC_ENTRYPOINTS = {
    "README": REPOSITORY / "README.md",
    "quickstart": REPOSITORY / "documentation/src/app/quickstart/page.mdx",
    "9P guide": REPOSITORY / "documentation/src/app/9p-access/page.mdx",
    "kernel-client guide": (
        REPOSITORY / "documentation/src/app/kernel-client/page.mdx"
    ),
}


class KernelDocumentationTest(unittest.TestCase):
    def test_public_guides_do_not_install_from_retired_kernel_channels(self):
        for name, path in PUBLIC_ENTRYPOINTS.items():
            with self.subTest(name=name):
                guide = path.read_text()
                self.assertNotIn("https://pkgs.zerofs.net/kernel/", guide)
                self.assertNotIn(
                    "/etc/apt/sources.list.d/zerofs-kernel.list", guide
                )

    def test_public_entrypoints_require_a_source_built_kernel_client(self):
        expected = {
            "README": "native kernel client after building it from source",
            "quickstart": "Build and load the module from source",
            "9P guide": "Build and install the module from source",
            "kernel-client guide": "Source build; root required",
        }
        retired_claims = (
            "when an exact package is available",
            "when a package is available",
            "matching kernel package",
            "Install the matching package",
            "distributed for specific kernels",
            "Exact-kernel ZeroFS module",
            "Install the package matching",
            "Exact-kernel packages",
            "limited to controlled prebuilt packages",
        )

        for name, path in PUBLIC_ENTRYPOINTS.items():
            with self.subTest(name=name):
                guide = path.read_text()
                self.assertIn(expected[name], guide)
                for claim in retired_claims:
                    self.assertNotIn(claim, guide)

    def test_source_build_does_not_promise_package_signing_or_auto_load(self):
        guide = PUBLIC_ENTRYPOINTS["kernel-client guide"].read_text()

        self.assertIn(
            "Source-built modules must be signed by a key trusted by the running kernel",
            guide,
        )
        self.assertNotIn("selector", guide)
        self.assertNotIn(
            "/usr/share/zerofs/zerofs-module-signing-cert.der", guide
        )
        self.assertNotIn("modules-load.d", guide)


if __name__ == "__main__":
    unittest.main()
