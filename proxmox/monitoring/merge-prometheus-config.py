#!/usr/bin/env python3
"""Add the managed ZeroFS scrape job to an existing Prometheus config."""

from __future__ import annotations

import argparse
import re
from pathlib import Path
from typing import Sequence


BEGIN_MARKER = "  # BEGIN managed ZeroFS production scrape"
END_MARKER = "  # END managed ZeroFS production scrape"


def merge(existing: str, job: str) -> str:
    if not re.search(r'^- job_name:\s*["\']?zerofs-prod["\']?\s*$', job, re.MULTILINE):
        raise ValueError("job file must define the zerofs-prod scrape job")
    if "\t" in job:
        raise ValueError("job file must use spaces for indentation")

    lines = existing.splitlines()
    scrape_indexes = [
        index
        for index, line in enumerate(lines)
        if re.fullmatch(r"scrape_configs:\s*(?:#.*)?", line)
    ]
    if len(scrape_indexes) != 1:
        raise ValueError("existing config must have one block-style scrape_configs key")

    begin_indexes = [index for index, line in enumerate(lines) if line == BEGIN_MARKER]
    end_indexes = [index for index, line in enumerate(lines) if line == END_MARKER]
    if begin_indexes or end_indexes:
        if len(begin_indexes) != 1 or len(end_indexes) != 1:
            raise ValueError("existing ZeroFS managed scrape markers are incomplete")
        begin = begin_indexes[0]
        end = end_indexes[0]
        if begin >= end or begin <= scrape_indexes[0]:
            raise ValueError("existing ZeroFS managed scrape markers are misplaced")
        del lines[begin : end + 1]

    scrape_index = scrape_indexes[0]
    insert_at = len(lines)
    for index in range(scrape_index + 1, len(lines)):
        line = lines[index]
        if line and not line[0].isspace() and not line.startswith("#"):
            insert_at = index
            break

    managed_job = [BEGIN_MARKER]
    managed_job.extend(
        f"  {line}" if line else "" for line in job.rstrip().splitlines()
    )
    managed_job.append(END_MARKER)
    if insert_at > 0 and lines[insert_at - 1] != "":
        managed_job.insert(0, "")
    if insert_at < len(lines) and lines[insert_at] != "":
        managed_job.append("")
    lines[insert_at:insert_at] = managed_job
    return "\n".join(lines).rstrip() + "\n"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--existing", type=Path, required=True)
    parser.add_argument("--job", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        merged = merge(args.existing.read_text(), args.job.read_text())
        args.output.write_text(merged)
    except (OSError, ValueError) as error:
        print(f"error: {error}", file=__import__("sys").stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
