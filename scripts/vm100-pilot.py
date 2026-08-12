#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path

from scripts.vm100_pilot.config import PilotConfig


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Manage the VM100 ZeroFS NBD pilot")
    parser.add_subparsers(dest="command", required=True)
    return parser


def main() -> int:
    root = Path(__file__).resolve().parent.parent
    PilotConfig.from_environment(root)
    build_parser().parse_args()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
