# ENOSPC at WRITE on the integrated raw and M1 base

This reruns the six mounted cases from
[write-time-admission-20261120](../write-time-admission-20261120/README.md) on
the merge of #95 into `integration/all-open-prs` (`1810a5df`), which also
carries #91 (raw format v2 and space reuse), #93 (degraded raw start) and #94
(M1 metadata copy on raw members).

Engine source `f3c2da47` was built as a Cargo release binary on native x86-64
`ubuntu-main` in `rust:1.100.0-bookworm`, with no emulation. The binary SHA-256
is `896ca6b4f0f36b5fff8b0deea7bff0020ae2cf7866c6ca143ec57efe5d06ae32`. The
fixture script is unchanged, with SHA-256
`50bbf3cd14c87d9a2f7ec1bf9da692a5326f82f497e55df2179dfe0c93521c06`. Host kernel
7.0.0-28-generic. The stock `9p`, `9pnet` and `9pnet_fd` modules were already
loaded, and nothing ran `modprobe`. The six cases ran at once in one
privileged, disposable container. Each case had its own tmpfs members.

| Client | Pin | Acknowledged (bytes) | Refused WRITE (ms) | fsync after refusal (ms) | Pinned (s) | Release to freed (s) | Rewrite fsync (ms) | Member A used before → after (bytes) |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| FUSE | none | 14,680,064 | 2.4 | 32.8 | — | 936 | 40.1 | 49,726,215 → 15,134,276 |
| FUSE | open reader | 14,680,064 | 2.2 | 31.9 | 1,080 | 71 | 41.5 | 49,776,172 → 15,149,481 |
| FUSE | checkpoint | 14,680,064 | 2.3 | 33.1 | 1,080 | 66 | 41.5 | 49,774,450 → 15,147,759 |
| Kernel 9P | none | 14,680,064 | 1.9 | 36.9 | — | 936 | 40.8 | 49,836,918 → 15,167,969 |
| Kernel 9P | open reader | 14,680,064 | 1.7 | 37.4 | 1,080 | 72 | 41.8 | 49,836,918 → 15,167,969 |
| Kernel 9P | checkpoint | 14,680,064 | 1.4 | 37.1 | 1,080 | 66 | 43.3 | 49,836,918 → 15,167,969 |

Every case exited 0. In every case the WRITE that could not be placed answered
ENOSPC, and the acknowledged prefix fsynced and read back exactly. Member A's
used bytes dropped by at least the deleted body after the unlink or the
release. The rest of the replacement was accepted on the same descriptor and
fsynced. After SIGKILL and restart, the earlier file and the whole replacement
were hash-identical. The 936 s unpinned figure is the same ephemeral
checkpoint expiry the earlier evidence explains. [Per-case receipts](mounted-receipts.jsonl)

These fixtures use directory members, so they do not exercise raw members or
M1. `survival_writes_never_take_room_promised_to_segments` covers that path on
a real `RawMemberStore`. With the survival PUT's ledger admission bypassed,
that test fails, because the member alone still accepts the PUT.

Times are fixture measurements on tmpfs, not device performance. No NAS, host
module, package or service changes were made.
