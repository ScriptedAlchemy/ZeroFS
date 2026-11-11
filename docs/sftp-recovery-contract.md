# SFTP outage recovery

ZeroFS cannot guarantee provider availability. Its recovery contract is that a
temporary SFTP failure must not become permanent writeback corruption, consume
unbounded physical connections, or acknowledge an unverified remote publication.
Durability still depends on the configured journal disk and remote storage.

## Failure classes

| Failure | Recovery | Safety boundary |
| --- | --- | --- |
| Connection handshake exceeds 30 seconds | Cancel it, retain its connection slot until cleanup settles, then retry with dial backoff | An opener that cannot settle within the additional 5 seconds closes the pool |
| Reads, writes, or metadata hold a live pooled session without a completion in that class | Recycle pooled sessions after `flush_stall_recycle_secs` (default 180 seconds); progress in another class does not reset the window | Admission queues, connection placement, dialing, and retired sessions are not evidence of a stalled operation; existing physical-close ownership and connection limits still apply; 0 disables this watchdog |
| An abandoned operation shares a connection with stalled siblings | Remove the connection from placement, allow siblings up to 10 seconds to settle, then run the owned close path | Retain admission through the shared close result and connection capacity until physical cleanup settles; fail closed if cleanup cannot be proven |
| Publication reply and staging removal both fail | Retain the exact staging owner, retry bounded removal with 1–30 second backoff, then replay the durable journal record | Never advance the remote sequence merely because cleanup succeeded |
| Large multipart write fails and abort cannot remove staging | The same typed SFTP cleanup recovery; generic multipart backends retain their existing fail-closed contract | No new publication while the prior attempt's owned cleanup is unresolved |
| Create already committed but its reply was lost | Cleanup, replay, and verify the existing object's exact bytes | Different bytes remain terminal; never overwrite a conflicting immutable object |
| Physical connection cleanup cannot be proven complete | Close the pool and exit through the server's normal durability shutdown; systemd restarts after 30 seconds | Do not reopen an ambiguous pool in place or delete its journal |
| Shutdown during cleanup | Cancel the scheduler-owned wait, retain the journal, hand remaining staging cleanup to its bounded pool owner | No remote acknowledgement; restart replays pending records |
| Content divergence, invalid paths, missing capabilities, or permission failure | Fail closed with the cause exposed | Never clear these errors or discard durable records just to restore a green status |

The service retries startup failures without a permanent systemd start-limit
lockout. This is process supervision, not a timer that restarts slow or busy
writeback. A terminal content conflict alone does not trigger the pool-failure
restart path.

## Why not just replace the SSH client?

The October 3 incident was a publication timeout followed by failed staging
cleanup that the writeback scheduler classified as permanent divergence. The
audit also found a permanently closed pool after a safely cancelled handshake,
and a separate multipart-abort path with the same classification problem.
Those policies sit above the native `russh` client; replacing the client would
not correct them. The existing native client force-closes its socket and awaits
the SSH connection task. Keep that ownership contract rather than introducing
a second production transport.

The November investigation also found that the pinned `russh 0.62.7` included
the upstream channel-window accounting bug
([russh #789](https://github.com/Eugeny/russh/issues/789)). Its application queue
can stop advancing while the SSH connection still appears alive. ZeroFS now
requires `russh 0.65.1`, which includes the upstream fix, alongside the pool
recovery protections above. The incident's healthy fresh SFTP connections and
stalled existing sessions are consistent with this bug; they do not by themselves
prove that it triggered the production stall. Dependency validation uses the
existing native loopback transfer and connection-lifecycle tests.

The September authentication-stall incident was distinct: the existing auth
watchdog refreshes the client factory for future dials without retiring healthy
sessions. Disk-full incidents and genuine immutable-object conflicts are also
separate failures; transport retry must not conceal them.

## FreeBSD journal inspection

FreeBSD uses `/dev/fd` to reopen the already validated, pinned journal descriptor
for read-only identity checks. Mount `fdescfs` at `/dev/fd` before running ZeroFS;
plain `devfs` exposes only descriptors 0–2. A missing descriptor path fails closed
and never falls back to reopening the journal by its mutable filename. The native
FreeBSD cross-compile job mounts `fdescfs` and runs the existing descriptor test,
which replaces the pathname and checks both original identity and writer locking.

## Regression evidence

The regression suite exercises the shipping scheduler, durable journal, SFTP
object store and session pool with deterministic faults. Cases cover small and
multipart lost replies, part/abort failure, matching and divergent create
replays, shutdown during cleanup, caller cancellation and physical slot
ownership. Assertions include unchanged remote watermarks during the outage,
retained journal records, exact payload bytes after recovery, and no leftover
owned staging paths.

Pool regressions also hold read and metadata leases without completions, verify
connection recycling and redial, and distinguish queued work from operations on
live sessions. Abandoning one lease while another retains the same connection
starts the owned physical close after at most 10 seconds of sibling grace. That
close has its own 10-second graceful window and 5-second forced-cleanup window;
an unsettled forced cleanup closes the pool and retains the connection slot
until cleanup finishes. A sibling retiring during an existing close waits for
the same close result, including failures. These deterministic pool tests cover
the recovery policy and connection limit on redial, not a reproduction of every
provider outage or a production deployment receipt.

Connection open, retirement, and close tasks run on the runtime that created
the pool. Dropping a lease outside a runtime, or shutting down a separate
caller's runtime, does not abandon its cleanup or release connection capacity
early. The pool's owning runtime must remain alive through pool shutdown.

`russh_pool_recovers_after_real_stalled_handshake_without_restart` additionally
uses real loopback TCP/SSH/SFTP: write durable bytes, stall the next handshake
past the actual deadline, observe socket closure, reconnect through the same
pool, read the exact bytes, and remove the test's owned file. It does not inject
faults into production or claim to simulate every provider failure.

Run from `zerofs/` (use a canonical temporary root on macOS):

```sh
TMPDIR=/private/tmp cargo test --locked -p zerofs --lib --no-default-features -- --test-threads=4
TMPDIR=/private/tmp cargo test --locked -p zerofs --lib --no-default-features cli::server::tests:: -- --test-threads=4
```

Production acceptance requires the Actions-built, attestation-verified binary's
hash to match the running executable, a clean terminal-error metric, advancing
remote watermarks (or a completely empty backlog), and an independent mounted
write/fsync/read integrity probe. A running process or passing unit tests alone
are not a deployment receipt.
