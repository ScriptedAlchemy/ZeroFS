#!/usr/bin/env python3

import argparse
import json
import re
from dataclasses import asdict, dataclass


RELEASE_TAG = re.compile(r"^v\d+\.\d+\.\d+(?:-[0-9A-Za-z.-]+)?$")


@dataclass(frozen=True)
class BuildEvent:
    publish: bool
    source_ref: str
    release_tag: str


def resolve_build_event(event_name: str, ref: str, input_tag: str) -> BuildEvent:
    if event_name == "workflow_dispatch":
        if not input_tag:
            return BuildEvent(publish=False, source_ref=ref, release_tag="")
        if not RELEASE_TAG.fullmatch(input_tag):
            raise ValueError(
                f"dispatch tag must be an exact vX.Y.Z release tag: {input_tag!r}"
            )
        return BuildEvent(
            publish=True,
            source_ref=f"refs/tags/{input_tag}",
            release_tag=input_tag,
        )

    if event_name == "push":
        prefix = "refs/tags/"
        if not ref.startswith(prefix):
            raise ValueError(f"publishing push must target a tag ref: {ref!r}")
        release_tag = ref.removeprefix(prefix)
        if not RELEASE_TAG.fullmatch(release_tag):
            raise ValueError(
                f"publishing push must target a vX.Y.Z release tag: {ref!r}"
            )
        return BuildEvent(publish=True, source_ref=ref, release_tag=release_tag)

    if event_name == "pull_request":
        if input_tag:
            raise ValueError("pull requests cannot request a release tag")
        return BuildEvent(publish=False, source_ref=ref, release_tag="")

    raise ValueError(f"unsupported container build event: {event_name!r}")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--event-name", required=True)
    parser.add_argument("--ref", required=True)
    parser.add_argument("--input-tag", default="")
    args = parser.parse_args()

    try:
        event = resolve_build_event(args.event_name, args.ref, args.input_tag)
    except ValueError as error:
        parser.error(str(error))

    print(json.dumps(asdict(event), sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
