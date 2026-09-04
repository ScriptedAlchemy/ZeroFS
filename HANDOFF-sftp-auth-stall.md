# Deploy request: SFTP auth-stall watchdog — commit `f4c7ed6f` on `develop`

## What happened (2026-09-03 10:42 → 09-04 08:19 UTC, ~21 h of zero uploads)

Every SSH dial from ZeroFS to the Hetzner box was refused:

    public-key authentication rejected for /srv/zerofs-persist/current/storage-key

~16/min, continuously, preceded once by
`russh connection task failed during close error=No authentication method`.
Writeback poisoned itself the same minute ("permanent remote divergence at
sequence 1565669") as a side effect, which is why it initially looked like
a data-divergence problem.

**It was not the key and not a second writer.** From inside CT198, plain
OpenSSH `sftp -P 23 -i /srv/zerofs-persist/current/storage-key
u618933@u618933.your-storagebox.de` authenticated and listed the bucket
while ZeroFS was still being rejected in the same minute. Both release dirs
hold the identical key (SHA256:OmTbik…, unchanged since 08-30). The
in-process russh client state was bad; `systemctl restart zerofs-lxc`
(08:19:33) cleared it instantly — 0 rejections, 0 divergence since, uploads
back at ~20 Mbps, 650+ files published.

## The fix (this commit)

An auth-stall watchdog next to the flush-stall one, `sftp_transport.rs`
`check_auth_stall`:

- arms on the first `Open("… authentication rejected|failed|No authentication method")`
- after `[sftp] auth_stall_recycle_secs` (default **120**, `0` disables, floor 30)
  with no successful open: reload identity from disk into a **rebuilt**
  `RusshSessionFactory`, `recycle_all_sessions()`, reset `DialBackoff`
- ERROR log `SFTP authentication has been rejected on every dial since the stall began…`
  and metric `zerofs_sftp_pool_auth_recoveries_total`; re-arms per window
- non-auth failures (TCP, timeout, session cap) never arm it

Tests (5, all green with `~/.cargo/bin/cargo test --release --features webui`):
recovery rebuilds exactly once and the next dial is immediate; inert at 0;
non-auth failures never arm it; config floor.

## Deploy ask

Promote current `develop` (`f4c7ed6f`) to CT198 with `proxmox/host-deploy.sh`
so it carries a receipt — the running release `5f146b7a-202608292000` was
hand-rolled by the Claude session and has none. No config change needed
(default 120 s is on). Verify after restart: `journalctl -u zerofs-lxc`
shows 0 `authentication rejected`; `/metrics` (10.10.10.55:8080) lists
`zerofs_sftp_pool_auth_recoveries_total`. Do **not** simulate by breaking
the prod key.

## Still open, not in this commit

- Aug-31 divergence at sequence 1389613 was a *different* event (no auth
  failures before it) — plausibly overlapping restarts on 08-29/30; a
  restart did not clear that one. Worth a look at what did.
- The HTTP upload API returns 200 while writeback is poisoned; commits then
  hang on the durability barrier (client sees -1001). Fails safe, not loud.
