"""Verify out-of-tree bindgen inherits target-kernel API-selection macros."""

import subprocess
import tempfile
import unittest
from pathlib import Path


KERNEL = Path(__file__).resolve().parents[2]
MAKE_PROBE = r"""
.PHONY: show-bindgen-flags
show-bindgen-flags:
	@printf '%s\n' '$(zerofs_bindgen_compiler_flags)'
"""


class BindgenArchitectureFlagsTests(unittest.TestCase):
    def effective_flags(self, *, arch, cflags, cppflags):
        with tempfile.TemporaryDirectory(prefix="zerofs-bindgen-flags-") as temp:
            probe = Path(temp) / "probe.mk"
            probe.write_text(MAKE_PROBE)
            result = subprocess.run(
                [
                    "make",
                    "--no-print-directory",
                    "--silent",
                    "-f", str(KERNEL / "Kbuild"),
                    "-f", str(probe),
                    "show-bindgen-flags",
                    f"SRCARCH={arch}",
                    "obj=.",
                    f"KBUILD_CFLAGS={cflags}",
                    f"KBUILD_CPPFLAGS={cppflags}",
                ],
                check=True,
                text=True,
                capture_output=True,
            )
            return result.stdout.strip().split()

    def test_arm64_new_sysreg_definitions_reach_bindgen(self):
        flags = self.effective_flags(
            arch="arm64",
            cflags="-DCC_USING_ASM_GOTO -fstack-protector-strong",
            cppflags=(
                "-DARM64_S390_COMMON=1 "
                "-DCC_USING_PATCHABLE_FUNCTION_ENTRY -DUNRELATED=1"
            ),
        )
        self.assertEqual(
            set(flags),
            {
                "-DCC_USING_ASM_GOTO",
                "-DARM64_S390_COMMON=1",
                "-DCC_USING_PATCHABLE_FUNCTION_ENTRY",
            },
        )

    def test_older_arm64_kernel_does_not_gain_synthetic_defines(self):
        flags = self.effective_flags(
            arch="arm64",
            cflags="-DCC_USING_ASM_GOTO",
            cppflags="-DLEGACY_ONLY=1",
        )
        self.assertEqual(flags, ["-DCC_USING_ASM_GOTO"])

    def test_other_architectures_preserve_only_compatible_flags(self):
        flags = self.effective_flags(
            arch="x86",
            cflags="-O2 -fno-plt",
            cppflags="-DCC_USING_PATCHABLE_FUNCTION_ENTRY -DUNRELATED=1",
        )
        self.assertEqual(flags, ["-DCC_USING_PATCHABLE_FUNCTION_ENTRY"])


if __name__ == "__main__":
    unittest.main()
