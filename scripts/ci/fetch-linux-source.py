#!/usr/bin/env python3
"""Stage a complete Linux fixture before writing it to the filesystem under test."""

import argparse
import gzip
import hashlib
import lzma
from pathlib import Path, PurePosixPath
import re
import subprocess
import sys
import tarfile
import tempfile
import time
import zlib


def source_urls(version):
    if not re.fullmatch(r"[1-9][0-9]*\.[0-9]+(?:\.[0-9]+)?(?:-rc[1-9][0-9]*)?", version):
        raise ValueError(f"invalid Linux version: {version!r}")
    if "-rc" not in version:
        major = version.split(".", 1)[0]
        return (f"https://cdn.kernel.org/pub/linux/kernel/v{major}.x/linux-{version}.tar.xz",)
    # Both endpoints provide the same upstream tag and top-level directory.
    # Codeload avoids depending solely on kernel.org's snapshot service.
    return (
        f"https://codeload.github.com/torvalds/linux/tar.gz/refs/tags/v{version}",
        "https://git.kernel.org/pub/scm/linux/kernel/git/torvalds/linux.git/"
        f"snapshot/linux-{version}.tar.gz",
    )


def validate_archive(archive, version):
    prefix = f"linux-{version}"
    required = {f"{prefix}/{name}" for name in ("Makefile", "Kconfig", "MAINTAINERS", "init/main.c")}
    paths = set()
    links = {}
    # Streaming avoids loading the kernel tree or extracting it on local disk.
    decompress = gzip.open if "-rc" in version else lzma.open
    with decompress(archive, "rb") as stream:
        with tarfile.open(fileobj=stream, mode="r|") as contents:
            for member in contents:
                path = PurePosixPath(member.name)
                if path.is_absolute() or ".." in path.parts or not path.parts or path.parts[0] != prefix:
                    raise ValueError(f"unexpected archive path: {member.name!r}")
                # Git snapshots do not need hard links or special files. In
                # particular, hard-linking a symlink can relocate its relative
                # target, which is different from following a symbolic link.
                if not (member.isfile() or member.isdir() or member.issym()):
                    raise ValueError(f"unexpected archive member type: {member.name!r}")
                name = str(path)
                if name in paths:
                    raise ValueError(f"duplicate archive member: {member.name!r}")
                paths.add(name)
                if member.issym():
                    target = PurePosixPath(member.linkname)
                    if target.is_absolute():
                        raise ValueError(f"absolute archive link: {member.name!r}")
                    # Preserve .. until links are expanded relative to their parent.
                    links[name] = (*path.parent.parts, *target.parts)
                if member.isfile() and member.size > 0:
                    required.discard(name)
        # Drain the compressed stream after tar reaches its end marker so
        # truncation or a checksum error in the trailer fails validation.
        while stream.read(1024 * 1024):
            pass
    if required:
        raise ValueError(f"kernel archive is missing source files: {', '.join(sorted(required))}")

    def resolve(parts, seen):
        resolved = []
        for part in parts:
            if part == ".":
                continue
            if part == "..":
                if len(resolved) <= 1:
                    raise ValueError("archive link escapes kernel tree")
                resolved.pop()
                continue
            resolved.append(part)
            if resolved[0] != prefix:
                raise ValueError("archive link escapes kernel tree")
            name = "/".join(resolved)
            if name in links:
                if name in seen or len(seen) >= 40:
                    raise ValueError(f"cyclic or excessively nested archive link: {name!r}")
                resolved = resolve(links[name], seen | {name})
        if not resolved or resolved[0] != prefix:
            raise ValueError("archive link escapes kernel tree")
        return resolved

    # Resolve against the complete graph, including forward references and links
    # in a member's parent path. Lexical normalization alone would accept a -> .,
    # b -> a/.., although b escapes the kernel tree when the links are followed.
    for name in paths:
        path = PurePosixPath(name)
        # Git trees cannot contain entries beneath a link. Reject this rather
        # than aliasing link definitions to different names during extraction.
        if any(str(parent) in links for parent in path.parents):
            raise ValueError(f"archive member traverses a link parent: {name!r}")
        resolve(path.parts, set())


def fetch_archive(version, output, *, sha256=None, urls=None, attempts=2, timeout=180, backoff=5):
    official_urls = source_urls(version)
    if sha256 is not None and not re.fullmatch(r"[0-9a-f]{64}", sha256):
        raise ValueError("expected SHA-256 must be 64 lowercase hexadecimal digits")
    output = Path(output)
    output.parent.mkdir(parents=True, exist_ok=True)
    # Stage next to the destination so replacing it after validation is atomic.
    with tempfile.TemporaryDirectory(prefix=f".{output.name}.", dir=output.parent) as directory:
        partial = Path(directory) / "archive.part"
        sources = official_urls if urls is None else urls
        for source_number, url in enumerate(sources, 1):
            for attempt in range(1, attempts + 1):
                partial.unlink(missing_ok=True)
                print(f"Fetching Linux {version}: source {source_number}, attempt {attempt}/{attempts}", flush=True)
                try:
                    subprocess.run(
                        ["curl", "--fail", "--location", "--silent", "--show-error",
                         "--connect-timeout", "15", "--max-time", str(timeout),
                         "--output", str(partial), url],
                        check=True, timeout=timeout + 5,
                    )
                    with partial.open("rb") as archive:
                        digest = hashlib.file_digest(archive, "sha256").hexdigest()
                    if sha256 is not None and digest != sha256:
                        raise ValueError(f"kernel archive hash mismatch: expected {sha256}, got {digest}")
                    validate_archive(partial, version)
                except (
                    OSError, EOFError, ValueError, tarfile.TarError, lzma.LZMAError,
                    zlib.error, subprocess.SubprocessError,
                ) as error:
                    print(f"Linux source download failed: {error}", file=sys.stderr, flush=True)
                    if attempt < attempts:
                        time.sleep(backoff * attempt)
                    continue
                size = partial.stat().st_size
                partial.replace(output)
                print(f"Validated {output.name}: {size} bytes, SHA-256 {digest}", flush=True)
                return
    raise RuntimeError(f"could not fetch a valid Linux {version} archive from any source")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--version", required=True)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--sha256", help="required archive digest before publication")
    args = parser.parse_args()
    try:
        fetch_archive(args.version, args.output, sha256=args.sha256)
    except (OSError, ValueError, RuntimeError) as error:
        parser.exit(1, f"{error}\n")


if __name__ == "__main__":
    main()
