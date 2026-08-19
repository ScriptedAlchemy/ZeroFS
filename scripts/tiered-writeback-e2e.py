#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.tiered_writeback_e2e.config import (  # noqa: E402
    DEFAULT_ARCHIVE_ROOT,
    DEFAULT_PROC_ROOT,
    FILESYSTEM_ACK_MODES,
    OBJECT_ACK_MODES,
    ConfigError,
    HarnessConfig,
    require_ack_modes,
    validate_owned_path,
)
from scripts.tiered_writeback_e2e.lifecycle import (  # noqa: E402
    SCENARIO_NAMES,
    HarnessLifecycle,
    assert_source_idle,
)
from scripts.tiered_writeback_e2e.resources import ResourceLedger  # noqa: E402
from scripts.vm100_pilot.runner import Runner  # noqa: E402


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Dual-acknowledgement tiered writeback Linux E2E harness: every "
            "setup/run declares both the filesystem and object ack modes, and "
            "all disposable state lives under UUID-scoped run roots"
        )
    )
    subcommands = parser.add_subparsers(dest="command", required=True)

    def add_dual_ack_flags(command: argparse.ArgumentParser) -> None:
        command.add_argument(
            "--filesystem-ack-mode",
            required=True,
            choices=FILESYSTEM_ACK_MODES,
            help="filesystem-level acknowledgement mode under test",
        )
        command.add_argument(
            "--object-ack-mode",
            required=True,
            choices=OBJECT_ACK_MODES,
            help="object-store acknowledgement mode under test",
        )

    def add_artifact_flags(command: argparse.ArgumentParser) -> None:
        command.add_argument("--zerofs-binary", required=True, type=Path)
        command.add_argument("--zerofs-config", required=True, type=Path)

    setup = subcommands.add_parser(
        "setup", help="create the control/resource roots, ledger, and receipts"
    )
    add_dual_ack_flags(setup)
    add_artifact_flags(setup)
    setup.add_argument("--run-uuid", help="reuse a caller-chosen run UUID")
    setup.add_argument("--source-root", type=Path)

    run = subcommands.add_parser("run", help="run one contract scenario")
    add_dual_ack_flags(run)
    add_artifact_flags(run)
    run.add_argument("--ledger", required=True, type=Path)
    run.add_argument("--scenario", required=True, choices=SCENARIO_NAMES)
    run.add_argument(
        "--plan-only",
        action="store_true",
        help="emit the receipt and command plan without executing (non-Linux)",
    )

    cleanup = subcommands.add_parser(
        "cleanup", help="remove only RESOURCE_ROOT entries; idempotent"
    )
    cleanup.add_argument("--ledger", required=True, type=Path)

    assert_clean = subcommands.add_parser(
        "assert-clean", help="fail if any recorded resource remains"
    )
    assert_clean.add_argument("--ledger", required=True, type=Path)

    archive = subcommands.add_parser(
        "archive-control",
        help="copy ledger/receipts to the archive, verify hashes, remove CONTROL_ROOT",
    )
    archive.add_argument("--ledger", required=True, type=Path)
    archive.add_argument(
        "--archive-root", type=Path, default=DEFAULT_ARCHIVE_ROOT
    )

    idle = subcommands.add_parser(
        "assert-source-idle",
        help="fail when cargo/rustc/test/harness jobs are rooted at the source",
    )
    idle.add_argument("--source-root", required=True, type=Path)
    idle.add_argument("--proc-root", type=Path, default=DEFAULT_PROC_ROOT)

    ledger_value = subcommands.add_parser(
        "ledger-value", help="read one dotted key from the ledger document"
    )
    ledger_value.add_argument("--ledger", required=True, type=Path)
    ledger_value.add_argument("key")

    owned = subcommands.add_parser(
        "validate-owned-path",
        help="fail unless the path is owned by the run's resource root",
    )
    owned.add_argument("--ledger", required=True, type=Path)
    owned.add_argument("path")
    return parser


def _emit(value: object) -> None:
    print(json.dumps(value, indent=2, sort_keys=True, default=str))


def _ledger_lifecycle(
    ledger_path: Path,
) -> tuple[ResourceLedger, HarnessLifecycle]:
    ledger = ResourceLedger.load(ledger_path)
    config = HarnessConfig.from_identity(ledger.identity)
    return ledger, HarnessLifecycle(config, Runner())


def dispatch(args: argparse.Namespace) -> None:
    if args.command == "setup":
        require_ack_modes(args.filesystem_ack_mode, args.object_ack_mode)
        config = HarnessConfig.create(
            filesystem_ack_mode=args.filesystem_ack_mode,
            object_ack_mode=args.object_ack_mode,
            run_uuid=args.run_uuid,
            source_root=args.source_root,
        )
        lifecycle = HarnessLifecycle(config, Runner())
        _emit(
            lifecycle.setup(
                zerofs_binary=args.zerofs_binary, zerofs_config=args.zerofs_config
            )
        )
    elif args.command == "run":
        ack = require_ack_modes(args.filesystem_ack_mode, args.object_ack_mode)
        ledger, lifecycle = _ledger_lifecycle(args.ledger)
        for role, given, recorded in (
            ("filesystem", ack.filesystem, ledger.identity.get("filesystem_ack_mode")),
            ("object", ack.object, ledger.identity.get("object_ack_mode")),
        ):
            if given != recorded:
                raise ConfigError(
                    f"{role} ack mode {given!r} does not match the ledger "
                    f"identity {recorded!r}"
                )
        _emit(
            lifecycle.run_scenario(
                ledger,
                args.scenario,
                zerofs_binary=args.zerofs_binary,
                zerofs_config=args.zerofs_config,
                plan_only=args.plan_only,
            )
        )
    elif args.command == "cleanup":
        ledger, lifecycle = _ledger_lifecycle(args.ledger)
        _emit(lifecycle.cleanup(ledger))
    elif args.command == "assert-clean":
        ledger, lifecycle = _ledger_lifecycle(args.ledger)
        _emit(lifecycle.assert_clean(ledger))
    elif args.command == "archive-control":
        ledger, lifecycle = _ledger_lifecycle(args.ledger)
        _emit(lifecycle.archive_control(ledger, args.archive_root))
    elif args.command == "assert-source-idle":
        _emit(assert_source_idle(args.source_root, proc_root=args.proc_root))
    elif args.command == "ledger-value":
        ledger = ResourceLedger.load(args.ledger)
        _emit({"key": args.key, "value": ledger.value(args.key)})
    elif args.command == "validate-owned-path":
        ledger = ResourceLedger.load(args.ledger)
        resolved = validate_owned_path(Path(args.path), ledger.resource_root)
        _emit({"path": str(resolved), "owned": True})
    else:  # pragma: no cover - argparse enforces this boundary.
        raise AssertionError(f"unhandled command: {args.command}")


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        dispatch(args)
    except BaseException as error:
        print(f"tiered-writeback-e2e: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
