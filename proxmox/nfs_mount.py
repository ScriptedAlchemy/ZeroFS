from __future__ import annotations

from dataclasses import dataclass


RECEIPT_PREFIX = "ZEROFS_SHARED_NAMESPACE_V1"
LEGACY_NBD_MOUNT_UNITS = (
    r"mnt-zerofs\x2dlxc.mount",
    "mnt-zerofs-lxc.mount",
)
LEGACY_NBD_CLIENT_UNIT = "zerofs-lxc-nbd-client.service"
LEGACY_NBD_MOUNTPOINT = "/mnt/zerofs-lxc"
LEGACY_NBD_DEVICE = "/dev/nbd0"


@dataclass(frozen=True)
class LegacyNbdState:
    loaded_mount_units: frozenset[str]
    client_loaded: bool
    client_active: bool
    mount_source: str | None
    device_connected: bool


@dataclass(frozen=True)
class LegacyNbdRetirementPlan:
    actions: tuple[str, ...]


def plan_legacy_nbd_retirement(state: LegacyNbdState) -> LegacyNbdRetirementPlan:
    unknown_units = state.loaded_mount_units.difference(LEGACY_NBD_MOUNT_UNITS)
    if unknown_units:
        raise ValueError(f"unknown legacy NBD mount units: {sorted(unknown_units)!r}")
    if state.mount_source not in (None, LEGACY_NBD_DEVICE):
        raise ValueError(f"unexpected legacy mount source: {state.mount_source}")
    recognized_device = state.client_active or state.mount_source == LEGACY_NBD_DEVICE
    if state.device_connected and not recognized_device:
        raise ValueError("nbd0 is connected without recognized legacy ZeroFS state")

    actions: list[str] = []
    if state.mount_source is not None:
        actions.append(f"sync:{LEGACY_NBD_MOUNTPOINT}")
    actions.extend(
        f"disable:{unit}"
        for unit in LEGACY_NBD_MOUNT_UNITS
        if unit in state.loaded_mount_units
    )
    if state.mount_source is not None:
        actions.append(f"unmount:{LEGACY_NBD_MOUNTPOINT}")
    if state.client_loaded:
        actions.append(f"disable:{LEGACY_NBD_CLIENT_UNIT}")
    if state.device_connected:
        actions.append(f"disconnect:{LEGACY_NBD_DEVICE}")
    actions.append("remove-obsolete-artifacts")
    return LegacyNbdRetirementPlan(tuple(actions))


@dataclass(frozen=True)
class SharedNamespaceOwnershipReceipt:
    objects: int
    wrong_owner: int
    first_uid: int | None
    first_gid: int | None
    verified: bool = True
    reason: str | None = None


def parse_shared_namespace_ownership_receipt(
    output: str,
) -> SharedNamespaceOwnershipReceipt:
    line = next(
        (line for line in output.splitlines() if line.startswith(RECEIPT_PREFIX)),
        None,
    )
    if line is None:
        raise ValueError(
            "existing shared namespace did not return an ownership receipt"
        )
    try:
        fields = dict(field.split("=", 1) for field in line.split()[1:])
        first_uid = int(fields.get("first_uid", "-1"))
        first_gid = int(fields.get("first_gid", "-1"))
        return SharedNamespaceOwnershipReceipt(
            objects=int(fields.get("objects", "0")),
            wrong_owner=int(fields.get("wrong_owner", "0")),
            first_uid=None if first_uid < 0 else first_uid,
            first_gid=None if first_gid < 0 else first_gid,
            verified=fields.get("verified") == "1",
            reason=fields.get("reason"),
        )
    except (TypeError, ValueError) as error:
        raise ValueError("invalid shared namespace ownership receipt") from error


def validate_shared_namespace_ownership(
    receipt: SharedNamespaceOwnershipReceipt,
) -> None:
    if receipt.verified and receipt.objects > 0 and receipt.wrong_owner == 0:
        return
    detail = (
        f"verified={int(receipt.verified)} objects={receipt.objects} "
        f"wrong_owner={receipt.wrong_owner} first={receipt.first_uid}:{receipt.first_gid} "
        f"reason={receipt.reason or 'ownership'}"
    )
    raise ValueError(
        f"existing shared namespace ownership gate failed ({detail}); "
        "with the old server mounted at /mnt/zerofs-files, run "
        "`sudo find /mnt/zerofs-files -xdev -path /mnt/zerofs-files/.nbd -prune "
        "-o -exec chown --no-dereference 501:20 -- {} +`, then verify and retain receipt "
        "`ZEROFS_SHARED_NAMESPACE_V1 verified=1 objects=<count> wrong_owner=0 "
        "first_uid=-1 first_gid=-1 reason=ok` before redeploying"
    )
