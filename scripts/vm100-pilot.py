#!/usr/bin/env python3
from __future__ import annotations

import argparse
import fcntl
import json
import os
import sys
from contextlib import contextmanager
from dataclasses import asdict, is_dataclass
from pathlib import Path
from typing import Iterator

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.vm100_pilot.benchmark import BenchmarkRunner  # noqa: E402
from scripts.vm100_pilot.config import PilotConfig  # noqa: E402
from scripts.vm100_pilot.lifecycle import PilotLifecycle  # noqa: E402
from scripts.vm100_pilot.memory_envelope import (  # noqa: E402
    MemoryEnvelopeAuthority,
    MemoryEnvelopeSession,
)
from scripts.vm100_pilot.metrics import MetricsClient  # noqa: E402
from scripts.vm100_pilot.migration import StripedMigrator  # noqa: E402
from scripts.vm100_pilot.performance_matrix import PerformanceMatrixRunner  # noqa: E402
from scripts.vm100_pilot.profile import ProfileRunner  # noqa: E402
from scripts.vm100_pilot.protocol_matrix import (  # noqa: E402
    ProtocolAuthority,
    ProtocolMatrixRunner,
)
from scripts.vm100_pilot.raw_sftp import RawSftpRunner  # noqa: E402
from scripts.vm100_pilot.receipts import RunReceipt  # noqa: E402
from scripts.vm100_pilot.real_world_matrix import RealWorldMatrixRunner  # noqa: E402
from scripts.vm100_pilot.reset import FreshResetter  # noqa: E402
from scripts.vm100_pilot.runner import Runner  # noqa: E402
from scripts.vm100_pilot.scenarios import (  # noqa: E402
    list_scenarios,
    require_memory_scenario,
    require_protocol_scenario,
    require_raw_sftp_scenario,
)
from scripts.vm100_pilot.workloads import WorkloadRunner  # noqa: E402
from scripts.vm100_pilot.writeback_observer import WritebackObserver  # noqa: E402


def _positive(value: str) -> int:
    parsed = int(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("must be positive")
    return parsed


def _add_sftp_ab_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--stock-ssh", type=Path, required=True)
    parser.add_argument("--hpn-ssh", type=Path, required=True)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Build, deploy, profile, and benchmark the VM100 ZeroFS NBD pilot"
    )
    subcommands = parser.add_subparsers(dest="command", required=True)
    subcommands.add_parser(
        "list-scenarios",
        help="list the immutable registry of real benchmark scenarios",
    )
    setup = subcommands.add_parser("setup", help="build, deploy, and start the pilot")
    setup.add_argument(
        "--skip-build",
        action="store_true",
        help="start the installed deployment unchanged",
    )
    subcommands.add_parser("teardown", help="stop mount, NBD client, and daemon")
    subcommands.add_parser("restart", help="restart the installed pilot stack")
    subcommands.add_parser("status", help="validate runtime topology and durable state")
    drain = subcommands.add_parser("drain", help="wait for local and remote writeback")
    drain.add_argument("--timeout", type=_positive)
    migration = subcommands.add_parser(
        "migrate-striped",
        help="replace the canonical NBD export with a verified striped export",
    )
    migration.add_argument("--replacement-export")
    migration.add_argument("--temporary-max-size-gib", type=_positive)
    reset = subcommands.add_parser(
        "reset-fresh",
        help="destroy and rebuild only the VM100 ZeroFS pilot on a fresh prefix",
    )
    reset.add_argument("--remote-prefix", required=True)
    reset.add_argument(
        "--confirm-destroy-pilot",
        action="store_true",
        required=True,
        help="confirm that the current pilot filesystem may be replaced",
    )

    for name, help_text, default_mib in (
        ("benchmark", "measure foreground, local SSD, remote, and read tiers", 1024),
        ("profile", "run the tier benchmark with perf and system telemetry", 256),
    ):
        command = subcommands.add_parser(name, help=help_text)
        command.add_argument("--total-mib", type=_positive, default=default_mib)
        command.add_argument("--jobs", type=_positive, default=4)

    workloads = subcommands.add_parser(
        "workloads", help="benchmark npm, Cargo, and serial/parallel deletion"
    )
    workloads.add_argument("--delete-jobs", type=_positive)
    raw = subcommands.add_parser(
        "raw-sftp", help="measure the matched direct SFTP control"
    )
    _add_sftp_ab_arguments(raw)
    matrix = subcommands.add_parser(
        "performance-matrix",
        help="run an isolated direct-I/O NBD block-size and concurrency matrix",
    )
    matrix.add_argument(
        "--quick",
        action="store_true",
        help="run three representative 32 MiB cells instead of the full matrix",
    )
    matrix.add_argument(
        "--total-mib",
        type=_positive,
        help="logical MiB written by each cell (default: full 256, quick 32)",
    )
    real_world = subcommands.add_parser(
        "real-world-matrix",
        help="run file-shape, allocation, data-pattern, and durability scenarios",
    )
    real_world.add_argument(
        "--quick",
        action="store_true",
        help="run the bounded representative suite without 1 GiB cells",
    )
    protocol = subcommands.add_parser(
        "protocol-matrix",
        help="run the registered NFS or 9P shared-namespace matrix",
    )
    protocol.add_argument("--protocol", choices=("nfs", "9p"), required=True)
    protocol.add_argument(
        "--memory-envelope",
        action="store_true",
        help="enforce the registered fixed cgroup/process memory envelope",
    )
    iterate = subcommands.add_parser(
        "iterate", help="deploy, benchmark, run workloads, and run raw SFTP"
    )
    iterate.add_argument("--skip-build", action="store_true")
    _add_sftp_ab_arguments(iterate)
    all_command = subcommands.add_parser(
        "all", help="run benchmark, workloads, and raw SFTP without deployment"
    )
    _add_sftp_ab_arguments(all_command)
    return parser


@contextmanager
def operation_lock(path: Path) -> Iterator[None]:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a+", encoding="utf-8") as handle:
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as error:
            raise RuntimeError(
                f"another vm100-pilot operation holds the lock: {path}"
            ) from error
        handle.seek(0)
        handle.truncate()
        handle.write(f"pid={os.getpid()}\n")
        handle.flush()
        try:
            yield
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


def _emit(value: object) -> None:
    if is_dataclass(value):
        value = asdict(value)  # type: ignore[arg-type]
    elif hasattr(value, "to_dict"):
        value = value.to_dict()  # type: ignore[union-attr]
    elif hasattr(value, "__dict__"):
        value = value.__dict__
    print(json.dumps(value, indent=2, sort_keys=True, default=str))


def _run_protocol_matrix(
    args: argparse.Namespace,
    config: PilotConfig,
    runner: Runner,
) -> object:
    receipt = RunReceipt.start(config, f"protocol-matrix-{args.protocol}")
    with receipt:
        receipt.record("requested_protocol", args.protocol)
        receipt.record("memory_envelope_requested", bool(args.memory_envelope))
        scenario = require_protocol_scenario(f"protocol-matrix-{args.protocol}")
        authority = ProtocolAuthority.from_mapping(args.protocol, os.environ)
        observer = WritebackObserver(
            MetricsClient(authority.metrics_url, authority.metrics_identity),
            config.drain_timeout,
            authority.metrics_url,
        )
        memory_session = None
        if args.memory_envelope:
            memory_scenario = require_memory_scenario("memory-envelope")
            memory_authority = MemoryEnvelopeAuthority.from_mapping(os.environ)
            memory_session = MemoryEnvelopeSession.prepare(
                memory_authority,
                runner,
                observer,
                memory_scenario,
            )
        return ProtocolMatrixRunner(
            config,
            runner,
            observer,
            memory_session=memory_session,
        ).run(scenario, authority, receipt=receipt)


def dispatch(
    args: argparse.Namespace,
    config: PilotConfig | None,
    runner: Runner | None,
) -> None:
    if args.command == "list-scenarios":
        _emit([scenario.to_dict() for scenario in list_scenarios()])
        return
    if config is None or runner is None:
        raise RuntimeError(f"{args.command} requires a configured VM100 runner")
    runner.run(
        [
            "install",
            "-d",
            "-m",
            "0755",
            "-o",
            config.user,
            "-g",
            config.group,
            config.result_dir,
        ],
        sudo=True,
    )
    lifecycle = PilotLifecycle(config, runner)
    benchmark = BenchmarkRunner(config, runner, lifecycle)
    workloads = WorkloadRunner(config, runner, lifecycle)
    raw = RawSftpRunner(config, runner, lifecycle)
    matrix = PerformanceMatrixRunner(config, runner, lifecycle)
    real_world = RealWorldMatrixRunner(config, runner, lifecycle)
    profile = ProfileRunner(config, runner, lifecycle, benchmark)
    migration = StripedMigrator(config, runner, lifecycle)
    resetter = FreshResetter(config, runner, lifecycle)

    if args.command == "setup":
        if args.skip_build:
            _emit(
                {
                    "deployed": None,
                    "started": lifecycle.start(),
                    "status": lifecycle.status(),
                }
            )
        else:
            _emit(lifecycle.deploy_and_start())
    elif args.command == "teardown":
        lifecycle.stop()
        _emit({"stopped": True})
    elif args.command == "restart":
        _emit({"started": lifecycle.restart(), "status": lifecycle.status()})
    elif args.command == "status":
        _emit(lifecycle.status())
    elif args.command == "drain":
        _emit(lifecycle.drain(args.timeout))
    elif args.command == "migrate-striped":
        _emit(
            migration.run(
                replacement_export=args.replacement_export,
                temporary_max_size_gib=args.temporary_max_size_gib,
            )
        )
    elif args.command == "reset-fresh":
        _emit(
            resetter.run(
                remote_prefix=args.remote_prefix,
                confirm_destroy_pilot=args.confirm_destroy_pilot,
            )
        )
    elif args.command == "benchmark":
        _emit(benchmark.run(total_mib=args.total_mib, jobs=args.jobs))
    elif args.command == "profile":
        _emit(profile.run(total_mib=args.total_mib, jobs=args.jobs))
    elif args.command == "workloads":
        _emit(workloads.run(delete_jobs=args.delete_jobs))
    elif args.command == "raw-sftp":
        raw_scenario = require_raw_sftp_scenario("raw-sftp-stock-hpn")
        _emit(
            raw.run(
                raw_scenario,
                stock_ssh=args.stock_ssh,
                hpn_ssh=args.hpn_ssh,
            )
        )
    elif args.command == "performance-matrix":
        total_mib = args.total_mib
        if total_mib is None:
            total_mib = 32 if args.quick else 256
        _emit(matrix.run(total_mib=total_mib, quick=args.quick))
    elif args.command == "real-world-matrix":
        _emit(real_world.run(quick=args.quick))
    elif args.command == "protocol-matrix":
        _emit(_run_protocol_matrix(args, config, runner))
    elif args.command == "iterate":
        if args.skip_build:
            deployed = None
            lifecycle.start()
        else:
            deployment = lifecycle.deploy_and_start()
            deployed = deployment["deployed"]
        _emit(
            {
                "deployed": deployed,
                "benchmark": benchmark.run().to_dict(),
                "workloads": workloads.run().to_dict(),
                "raw_sftp": raw.run(
                    require_raw_sftp_scenario("raw-sftp-stock-hpn"),
                    stock_ssh=args.stock_ssh,
                    hpn_ssh=args.hpn_ssh,
                ).to_dict(),
            }
        )
    elif args.command == "all":
        _emit(
            {
                "benchmark": benchmark.run().to_dict(),
                "workloads": workloads.run().to_dict(),
                "raw_sftp": raw.run(
                    require_raw_sftp_scenario("raw-sftp-stock-hpn"),
                    stock_ssh=args.stock_ssh,
                    hpn_ssh=args.hpn_ssh,
                ).to_dict(),
            }
        )
    else:  # pragma: no cover - argparse enforces this boundary.
        raise AssertionError(f"unhandled command: {args.command}")


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.command == "list-scenarios":
        dispatch(args, None, None)
        return 0
    config = PilotConfig.from_environment(ROOT)
    try:
        with operation_lock(config.lock_file):
            dispatch(args, config, Runner())
    except BaseException as error:
        print(f"vm100-pilot: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
