from __future__ import annotations

import time
import os
import string
import tomllib
from dataclasses import dataclass
from pathlib import Path
from typing import Mapping
from typing import Callable

from scripts.vm100_pilot.metrics import (
    MetricsAuthorityIdentity,
    MetricsClient,
    WritebackSnapshot,
)


class ObservedDurabilityError(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class ObservedSnapshot:
    identity: MetricsAuthorityIdentity
    writeback: WritebackSnapshot


@dataclass(frozen=True, slots=True)
class ObservedEndpoint:
    url: str
    ca_file: Path
    export_id: str


def _require_control_file(path: object, control_root: Path, role: str) -> Path:
    candidate = Path(str(path))
    if not candidate.is_absolute():
        raise ObservedDurabilityError(f"{role} must be an absolute path")
    candidate = candidate.resolve(strict=False)
    control = Path(control_root).resolve(strict=False)
    try:
        relative = candidate.relative_to(control)
    except ValueError as error:
        raise ObservedDurabilityError(f"{role} must be inside the control root") from error
    if not relative.parts:
        raise ObservedDurabilityError(f"{role} must be below the control root")
    if not candidate.is_file():
        raise ObservedDurabilityError(f"{role} is not a regular file")
    return candidate


def load_observed_endpoint(
    config_path: Path,
    *,
    control_root: Path,
    expected_export: str,
    environ: Mapping[str, str] | None = None,
) -> ObservedEndpoint:
    """Load the exact loopback NBD authority endpoint from the runtime config."""
    values = os.environ if environ is None else environ
    try:
        expanded = string.Template(
            Path(config_path).read_text(encoding="utf-8")
        ).substitute(values)
        document = tomllib.loads(expanded)
    except (KeyError, OSError, tomllib.TOMLDecodeError) as error:
        raise ObservedDurabilityError(
            "invalid observed durability runtime config"
        ) from error
    servers = document.get("servers")
    prometheus = document.get("prometheus")
    if not isinstance(servers, dict) or not isinstance(prometheus, dict):
        raise ObservedDurabilityError("runtime config is missing NBD TLS authority")
    if any(name in servers for name in ("nfs", "ninep", "webui")):
        raise ObservedDurabilityError("NBD authority config has an additional export adapter")
    nbd = servers.get("nbd")
    if not isinstance(nbd, dict) or nbd.get("addresses") != ["127.0.0.1:10809"]:
        raise ObservedDurabilityError(
            "NBD authority must use the exact loopback endpoint 127.0.0.1:10809"
        )
    if prometheus.get("addresses") != ["127.0.0.1:19567"]:
        raise ObservedDurabilityError(
            "metrics authority must use the exact loopback endpoint 127.0.0.1:19567"
        )
    authority = prometheus.get("benchmark_authority")
    if not isinstance(authority, dict) or authority.get("adapter") != "nbd":
        raise ObservedDurabilityError("runtime config is missing NBD TLS authority")
    export_id = authority.get("export_id")
    if export_id != expected_export:
        raise ObservedDurabilityError(
            "metrics authority does not name the exact NBD export"
        )
    ca_file = _require_control_file(
        authority.get("tls_certificate"), control_root, "TLS CA file"
    )
    private_key = _require_control_file(
        authority.get("tls_private_key"), control_root, "TLS private key"
    )
    if ca_file == private_key:
        raise ObservedDurabilityError("TLS certificate and private key must differ")
    return ObservedEndpoint(
        url="https://127.0.0.1:19567/metrics",
        ca_file=ca_file,
        export_id=export_id,
    )


def collector_factory(endpoint: ObservedEndpoint) -> Callable[[], "ObservedDurabilityCollector"]:
    def create() -> ObservedDurabilityCollector:
        client = MetricsClient(endpoint.url, tls_ca_file=endpoint.ca_file)
        return ObservedDurabilityCollector(client.fetch_metrics)

    return create


class ObservedDurabilityCollector:
    def __init__(self, fetch_metrics: Callable[[], str]) -> None:
        self._fetch_metrics = fetch_metrics
        self._baseline: MetricsAuthorityIdentity | None = None
        self._last: WritebackSnapshot | None = None

    def snapshot(self) -> ObservedSnapshot:
        text = self._fetch_metrics()
        try:
            identity = MetricsAuthorityIdentity.parse(text)
            writeback = WritebackSnapshot.parse(text)
        except (TypeError, ValueError) as error:
            raise ObservedDurabilityError(
                "invalid observed durability metrics"
            ) from error
        if self._baseline is None:
            self._baseline = identity
        elif identity != self._baseline:
            raise ObservedDurabilityError(
                "metrics server incarnation changed during one observed run"
            )
        if min(writeback.accepted, writeback.local, writeback.remote) < 0:
            raise ObservedDurabilityError("metrics durability frontier is negative")
        if not writeback.remote <= writeback.local <= writeback.accepted:
            raise ObservedDurabilityError(
                "metrics durability frontiers must satisfy remote <= local <= accepted"
            )
        if writeback.terminal:
            raise ObservedDurabilityError("writeback reported terminal error")
        if self._last is not None and any(
            current < previous
            for current, previous in zip(
                (writeback.accepted, writeback.local, writeback.remote),
                (self._last.accepted, self._last.local, self._last.remote),
            )
        ):
            raise ObservedDurabilityError("metrics durability frontier regressed")
        self._last = writeback
        return ObservedSnapshot(identity, writeback)

    def wait_for_initial_snapshot(
        self, *, timeout: float, interval: float = 0.05
    ) -> ObservedSnapshot:
        deadline = time.monotonic() + timeout
        while True:
            try:
                return self.snapshot()
            except ObservedDurabilityError:
                raise
            except OSError as error:
                if time.monotonic() >= deadline:
                    raise ObservedDurabilityError(
                        "metrics authority endpoint did not become ready"
                    ) from error
                time.sleep(interval)

    def wait_for_accepted_after(
        self, previous: int, *, timeout: float, interval: float = 0.05
    ) -> ObservedSnapshot:
        if previous < 0:
            raise ObservedDurabilityError("previous accepted frontier is negative")
        deadline = time.monotonic() + timeout
        while True:
            current = self.snapshot()
            if current.writeback.accepted > previous:
                return current
            if time.monotonic() >= deadline:
                raise ObservedDurabilityError(
                    "accepted durability frontier did not advance"
                )
            time.sleep(interval)

    def wait_for_local_frontier(self, target: int, *, timeout: float, interval: float = 0.05) -> ObservedSnapshot:
        if target < 0:
            raise ObservedDurabilityError("local durability target is negative")
        deadline = time.monotonic() + timeout
        while True:
            current = self.snapshot()
            if (
                current.writeback.accepted >= target
                and current.writeback.local >= current.writeback.accepted
            ):
                return current
            if time.monotonic() >= deadline:
                raise ObservedDurabilityError("local durability frontier did not reach target")
            time.sleep(interval)

    def require_restarted(self, before: ObservedSnapshot) -> ObservedSnapshot:
        current = self.snapshot()
        if current.identity.server_instance_id == before.identity.server_instance_id:
            raise ObservedDurabilityError("server incarnation did not change after restart")
        if (current.identity.filesystem_id, current.identity.export_id) != (
            before.identity.filesystem_id,
            before.identity.export_id,
        ):
            raise ObservedDurabilityError("filesystem/export identity changed after restart")
        if any(
            after < prior
            for after, prior in zip(
                (
                    current.writeback.accepted,
                    current.writeback.local,
                    current.writeback.remote,
                ),
                (
                    before.writeback.accepted,
                    before.writeback.local,
                    before.writeback.remote,
                ),
            )
        ):
            raise ObservedDurabilityError(
                "metrics durability frontier regressed across restart"
            )
        return current

    def wait_for_restarted(
        self,
        before: ObservedSnapshot,
        *,
        timeout: float,
        interval: float = 0.05,
    ) -> ObservedSnapshot:
        deadline = time.monotonic() + timeout
        while True:
            try:
                return self.require_restarted(before)
            except ObservedDurabilityError:
                raise
            except OSError as error:
                if time.monotonic() >= deadline:
                    raise ObservedDurabilityError(
                        "restarted metrics authority endpoint did not become ready"
                    ) from error
                time.sleep(interval)

    @staticmethod
    def classify_recovery_source(*, target: int, observed: ObservedSnapshot) -> str:
        if target < 0:
            raise ObservedDurabilityError("durability target is negative")
        if observed.writeback.accepted < target or observed.writeback.local < target:
            raise ObservedDurabilityError(
                "accepted cutoff was not observed locally durable"
            )
        if observed.writeback.remote < target:
            return "remote-not-covered-at-pre-kill-sample"
        return "remote-covered-at-pre-kill-sample"
