"""Guard the explicitly opaque dependencies of nonrecursive netfs bindgen.

Linux 7.4's netfs_cache_ops gained a query_occupancy callback accepting an
FS-Cache pointer. Bindgen emits the pointer but --no-recursive-allowlist omits
its declaration; the ABI module must supply it before including bindings.rs.
"""

import re
import unittest
from pathlib import Path


NETFS_MODULE = Path(__file__).resolve().parents[2] / "netfs" / "mod.rs"
OPAQUE_FS_CACHE_TYPES = ("fscache_cookie", "fscache_occupancy")


class NetfsOpaqueAbiTests(unittest.TestCase):
    def test_fs_cache_pointer_targets_are_declared_before_generated_bindings(self):
        source = NETFS_MODULE.read_text()
        prefix, separator, _generated = source.partition('include!(concat!(env!("ZEROFS_KBUILD_OUTPUT")')
        self.assertTrue(separator, "the generated netfs bindings include must be present")
        for name in OPAQUE_FS_CACHE_TYPES:
            with self.subTest(type=name):
                declaration = re.compile(
                    rf"#\[repr\(C\)\]\s+pub struct {name}\s*\{{"
                    rf"\s*_private:\s*\[u8;\s*0\],?\s*\}}"
                )
                self.assertEqual(
                    len(declaration.findall(prefix)), 1,
                    f"{name} must be an opaque zero-sized C type in scope before bindgen",
                )


if __name__ == "__main__":
    unittest.main()
