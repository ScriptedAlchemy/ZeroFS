# SFTP reads over a high-latency link

## What the latency does and does not imply

A 95 ms round trip does not impose a 1 MB/s limit on an SSH connection. The
application has to keep enough independent requests in flight. At 1 Gbit/s,
the bandwidth-delay product is 11,875,000 bytes (about 11.3 MiB). That is the
payload in flight needed to cover one round trip, before protocol overhead or
additional request dependencies. It is not a promised transfer rate.

For the reported CT198-to-Mac measurement of 300–450 Mbit/s, that hop's
arithmetic ceiling is 37.5–56.25 MB/s before overhead. Improving the Hetzner
read path can therefore matter substantially even without relocating ZeroFS.
Those network measurements are operator reports; this change has not repeated
them or benchmarked the deployed Hetzner service.

## Transport and application windows

ZeroFS already configures a 16 MiB SSH receive window. SFTP READ requests are
independent of both that byte window and the physical SSH connection count:

- The default account pool is eight physical connections. Other processes
  using the same Storage Box also consume its ten-connection allowance.
- The default read operation limit is 16, multiplexed over that pool.
- Each payload read pipeline permits at most 64 outstanding SFTP READ requests,
  clipped to the server's negotiated packet and read limits.
- The tuned cache begins with a 1 MiB fetch and promotes sequential access to
  4 MiB windows with eight windows of look-ahead. These are queued backend
  ranges, not a measurement of bytes actually on the wire.
- 9P, CLI/SDK/browser downloads, and mounted read-ahead also need enough
  concurrency to feed the backend. The corresponding changes are collected
  in [PR #45](https://github.com/ScriptedAlchemy/ZeroFS/pull/45).

### Short replies need independent follow-up requests

An SFTP server may return fewer bytes than requested without reaching EOF.
The previous read loop split a fetch into roughly 255 KiB requests and then
repeatedly awaited each request's remaining suffix. For a 1 MiB fetch and
32 KiB replies, this left only about five request chains active despite a
64-request budget. Finishing the largest chains required eight reply waves.

The revised loop keeps a shared queue of missing ranges, learns the smaller
reply size for the current operation, and divides those missing ranges among
free request slots. It refills slots on individual completion, without a
whole-batch barrier. It still checks exact byte coverage, rejects empty or
oversized replies, and caps requests at 64 per operation. Operations receiving only full-sized replies
keep their original request size. No change to the physical connection budget
or SSH window is needed.

The regression uses virtual time, patterned bytes, and a simulated 95 ms reply
delay. This isolates request scheduling; it does not model TCP, SSH encryption,
server disk speed, OPEN/FSTAT/header/CLOSE overhead, or a production transfer.
It must not be quoted as a measured Hetzner speedup.

| Controlled 1 MiB read with 32 KiB replies | Simulated read time | Reply waves |
| --- | ---: | ---: |
| Previous serial suffix recovery | 760 ms | 8 |
| Shared missing-range queue | 190 ms | 2 |

The old implementation failed the new regression at 760 ms before the scheduler
was changed. The revised implementation passes with identical patterned bytes
at 190 ms. These timings cover the payload scheduler only.

To reproduce the deterministic regression from the `zerofs/` crate directory:

```console
cargo test --locked --no-default-features --lib \
  sftp_protocol::tests::short_sftp_read_replies_fill_available_request_slots \
  -- --exact --nocapture
```

The `--no-default-features` build requires a system `protoc`; the shipping
dependency versions remain locked.

## Check the endpoint before comparing hosts

Hetzner documents better SFTP performance through its SSH service on port 23
than on port 22:

- [Hetzner external storage configuration](https://docs.hetzner.com/storage/storage-share/configuration/external-storage/)
- [Storage Box access overview](https://docs.hetzner.com/storage/storage-box/access/access-overview/)
- [Storage Box limits](https://docs.hetzner.com/storage/storage-box/general/)

Verify the deployed URL first. If comparing ports, enable SSH support, use the
correct key format and verified host-key entry for the selected port, and
confirm that both endpoint paths resolve to the same existing objects. Port 23
can expose a different root path; a port-only substitution is not sufficient
evidence that the storage namespace is unchanged. Keep the production object
prefix and encryption configuration intact.

## Measurements that distinguish the remaining bottleneck

1. Record the running binary's exact Git revision, selected port, SFTP pool and
   read-concurrency settings, mount/client protocol, and cache state. An open
   PR is not evidence that the service runs its changes.
2. On CT198, download the same existing large remote object with one stock
   OpenSSH SFTP connection. Compare `-B 32768 -R 128` (4 MiB requested) with
   `-B 261120 -R 64` (about 15.9 MiB requested), subject to server support.
   Keep strict host-key verification and use an owned local scratch file.
   Stock OpenSSH is a diagnostic control, not a replacement product transport.
   Its request and buffer options are documented in the
   [OpenSSH SFTP manual](https://man.openbsd.org/sftp.1).
3. Run the production native-russh direct benchmark described in `AGENTS.md`
   on the isolated Linux test environment. Account for production connections:
   an eight-connection production pool leaves at most two benchmark slots.
   Use its integrity and teardown gates, and record both object size and
   effective concurrency. It creates its own UUID-scoped remote objects.
4. Compare the same cold and warm user-visible copy paths. A raw encrypted
   backend object and a logical filesystem file are different workloads;
   report them separately. Do not call a read cold merely because the client
   page cache was invalidated while ZeroFS's server cache remained warm.

If stock SFTP is fast but the native direct benchmark is slow, investigate the
native request/SSH path. If both direct controls are fast but the mounted copy
is slow, investigate cache admission, filesystem/client concurrency, and the
CT198-to-Mac path. If both direct controls remain slow, investigate the route,
loss, and shared Storage Box service before increasing ZeroFS limits.

Do not run the legacy pilot setup/teardown commands against CT198 or a shared
production mount. The existing `AGENTS.md` and
`docs/vm100-benchmark-methodology.md` define the isolated benchmark procedures.
