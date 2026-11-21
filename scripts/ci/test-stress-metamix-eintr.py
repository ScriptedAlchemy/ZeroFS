#!/usr/bin/env python3
"""Compile the patched metamix read block and inject syscall outcomes on Linux."""

import argparse
from pathlib import Path
import subprocess
import tempfile


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("source", type=Path, help="patched stress-metamix.c")
    parser.add_argument("--scratch", type=Path, required=True)
    args = parser.parse_args()
    source = args.source.read_text()
    start = source.index("\t\tdo {\n\t\t\trret = read(fd, buf, data_len);")
    end = source.index("\n\t\tif (verify)", start)
    block = source[start:end]
    harness = (
        r"""
#include <assert.h>
#include <errno.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdlib.h>
#include <sys/types.h>
static int mode, calls, failures, validated;
static bool running;
static bool stress_continue_flag(void) { return running; }
static ssize_t injected_read(int fd, void *buf, size_t len) {
    (void)fd; (void)buf;
    ++calls;
    assert(calls <= 2);
    if ((mode == 1 || mode == 2) && calls == 1) {
        if (mode == 2) running = false;
        errno = EINTR;
        return -1;
    }
    if (mode == 3) { errno = EIO; return -1; }
    return mode == 4 ? (ssize_t)len - 1 : (ssize_t)len;
}
#define read injected_read
#define UNLIKELY(x) (x)
#define pr_fail(...) (++failures)
static int exercise(void) {
    int rc = EXIT_SUCCESS, fd = 0;
    char buf[16];
    size_t data_len = sizeof(buf);
    ssize_t rret;
"""
        + block
        + r"""
    ++validated;
err_close:
    return rc;
}
int main(void) {
    for (mode = 0; mode < 5; ++mode) {
        calls = failures = validated = 0; running = true;
        int result = exercise();
        assert(result == (mode >= 3 ? EXIT_FAILURE : EXIT_SUCCESS));
        assert(calls == (mode == 1 ? 2 : 1));
        assert(validated == (mode <= 1 ? 1 : 0));
        assert(failures == (mode >= 3 ? 1 : 0));
    }
    return 0;
}
"""
    )
    with tempfile.TemporaryDirectory(
        prefix="metamix-eintr-", dir=args.scratch
    ) as scratch:
        root = Path(scratch)
        (root / "test.c").write_text(harness)
        subprocess.run(
            [
                "cc",
                "-Wall",
                "-Wextra",
                "-Werror",
                str(root / "test.c"),
                "-o",
                str(root / "test"),
            ],
            check=True,
        )
        subprocess.run([str(root / "test")], check=True, timeout=5)
    print(
        "Passed: exact read, EINTR retry, shutdown EINTR, EIO failure, short-read failure; fixture removed."
    )


if __name__ == "__main__":
    main()
