"""Compile and call the production create bridge against both kernel ABIs."""

import os
from pathlib import Path
import shlex
import subprocess
import tempfile
import unittest


KERNEL = Path(__file__).resolve().parents[2]

FS_HEADER = r"""
#include <stdbool.h>
typedef unsigned short umode_t;
struct mnt_idmap { int unused; };
struct inode { int unused; };
struct dentry { int unused; };
struct inode_operations {
    int (*create)(struct mnt_idmap *, struct inode *, struct dentry *, umode_t
#if LEGACY_CREATE
                  , bool
#endif
    );
};
"""

HARNESS = r"""
#include <assert.h>
#include "create_compat.h"

static struct mnt_idmap expected_idmap;
static struct inode expected_parent;
static struct dentry expected_dentry;
static int calls;

int zerofs_vfs_create(struct mnt_idmap *idmap, struct inode *parent,
                     struct dentry *dentry, umode_t mode)
{
    assert(idmap == &expected_idmap);
    assert(parent == &expected_parent);
    assert(dentry == &expected_dentry);
    assert(mode == 0640);
    calls++;
    return -17;
}

int main(void)
{
    struct inode_operations operations = {0};
    zerofs_vfs_assign_create(&operations);
    assert(operations.create != 0);
    for (int attempt = 0; attempt < 2; attempt++) {
        int result = operations.create(&expected_idmap, &expected_parent,
                                       &expected_dentry, 0640
#if LEGACY_CREATE
                                       , attempt != 0
#endif
        );
        assert(result == -17);
    }
    assert(calls == 2);
    return 0;
}
"""


class CreateCompatibilityTests(unittest.TestCase):
    def check_abi(self, legacy):
        with tempfile.TemporaryDirectory(prefix="zerofs-create-abi-") as directory:
            root = Path(directory)
            (root / "linux").mkdir()
            (root / "linux/fs.h").write_text(FS_HEADER)
            source = root / "create.c"
            source.write_text(HARNESS)
            executable = root / "create"
            subprocess.run(
                shlex.split(os.environ.get("CC", "cc"))
                + [
                    "-std=gnu11", "-Wall", "-Wextra", "-Werror",
                    "-Wcast-function-type", "-Wmissing-prototypes",
                    f"-DLEGACY_CREATE={int(legacy)}",
                    "-I", str(root), "-I", str(KERNEL / "vfs"),
                    str(source), "-o", str(executable),
                ],
                check=True,
                capture_output=True,
                text=True,
            )
            subprocess.run([str(executable)], check=True)

    def test_create_with_exclusive_argument(self):
        self.check_abi(legacy=True)

    def test_create_without_exclusive_argument(self):
        self.check_abi(legacy=False)


if __name__ == "__main__":
    unittest.main()
