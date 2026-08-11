# Task 1 report: bounded SFTP pool lifecycle and shutdown

Status: `DONE_WITH_CONCERNS`

Branch: `feat/sftp-hardening`

Baseline: `51ef89691fdd6f78d145fe61f1b3e742a04db2e6`

Scope: the isolated `zerofs-hardening` worktree only. No Hetzner connection,
live service or storage mutation, deployment, push, history rewrite, or
destructive cleanup was performed.

## Outcome

The SFTP pool now owns a bounded lifecycle from physical SSH process creation
through terminal shutdown. Production sessions use one directly owned,
foreground `ssh -s sftp` child per physical pool slot instead of a daemonized
OpenSSH ControlMaster. The pool retains that exact child, cancels it on
fail-closed/shutdown, and performs kill plus bounded wait before treating the
physical capacity as reusable.

Admission is terminally closable and queued/new checkouts return the typed
`TransportError::PoolClosed`. Failed, timed-out, or panicked lifecycle owners
fail the pool closed before releasing their physical permit. Idle sessions are
timestamped, reaped with a warm floor, and all reaper/lifecycle tasks are pool
owned. The shutdown path is idempotent, bounded, drains idle sessions, forces
owned SSH processes behind active leases, and reports an error rather than
logging `Shutdown complete` when logical activity does not return by the
deadline.

## Lifecycle and policy values

- shared physical ceiling: configured value, with the production default and
  tested ceiling remaining 8
- directional ceilings: configured values, with tested ceilings remaining 7
  reads and 7 writes
- SSH connect timeout: 20 seconds
- SSH connection attempts: 1
- server-alive interval/count: 30 seconds / 3
- pool open-owner timeout: 30 seconds
- graceful session close timeout: 10 seconds
- forced child reap timeout: 5 seconds
- pool shutdown timeout: 45 seconds
- idle timeout/reap interval/warm floor: 60 seconds / 10 seconds / 1 session
- terminal final-database close phases for SFTP: 20 seconds graceful, then 20
  seconds after pool shutdown begins; close-worker abort/join is bounded to 5
  seconds

The generated SSH policy also sets `BatchMode yes`, disables password and
keyboard-interactive authentication, requires public-key authentication,
retains strict host-key/known-hosts behavior, and sets `ControlMaster no` and
`ControlPersist no`.

## Server and adapter integration

- `parse_url_opts_with_sftp` returns the lifecycle handle and validates the SFTP
  root prefix before dialing.
- Initialization, server, debug, password-change, and transient checkpoint
  paths retain the handle and attempt bounded shutdown on their owned exits.
- `run_server` performs SFTP shutdown in an outer finalizer; it logs
  `Shutdown complete` only when both server/database teardown and pool shutdown
  succeed.
- Pool closure is recursively classified as terminal by the object-store
  adapter so `RetryingObjectStore` does not retry `PoolClosed` forever,
  including publication cleanup errors.
- The read-only database close owner is abortable/joined. The read-write
  `FlushCoordinator` retains its worker `JoinHandle` and `AbortHandle`; final
  close always joins it, and deadline handling aborts the worker before taking
  its join mutex.
- Periodic flush and other background callers are bounded as one group and are
  aborted and joined before final database close, preventing a stuck periodic
  flush from making the terminal SFTP path unreachable.
- Fatal command/server close paths now propagate errors through the Tokio
  runtime instead of calling `process::exit` while SSH cleanup may still be
  running.

This specifically addresses the supplied live receipt in which the baseline
service remained `deactivating` after final flush and retained SSH
masters/children: the hardened production path owns a non-daemonized child,
prevents terminal pool errors from entering an infinite object-store retry,
owns/aborts the actual final-flush worker, and reaches the bounded pool
finalizer on server errors.

## TDD receipts

Deterministic RED receipts were captured before the corresponding production
changes:

- the initial focused lifecycle matrix was 19 passed / 3 failed: forever open,
  forever close, and close failure with a still-live session
- the idle-reaper test observed 3 live sessions instead of the required warm
  floor of 1
- terminal shutdown initially failed to compile because the pool exposed no
  shutdown API
- the SSH policy test showed the required connect/dead-peer bounds were absent
- canceled forever-open ownership did not close the pool
- `PoolClosed` mapped to retryable `Generic` instead of terminal
  `NotSupported`
- the first production-adapter cleanup overlap diagnostic remained live with
  `dials=1 live=1 closes=0`
- the final background-drain regression initially referenced no bounded
  join/abort helper

The final deterministic suite covers forever-pending open/close, ambiguous
still-live close failure, canceled waiters and canceled open handoff, open and
close owner panics after receiver cancellation, idle expiry/warm floor,
shutdown admission wakeup/drain/idempotence/deadline, direct child kill/reap,
an active lease's process cancellation, concurrent adapter cleanup, and the
8-shared/7-per-direction/32-waiter ceiling.

## Verification receipts

- `cargo test sftp_transport::tests --lib -- --nocapture`: 34 passed, 0 failed
- `cargo test --bin zerofs cli::server::tests::final_drain_aborts_a_stuck_background_caller -- --exact --nocapture`:
  1 passed, 0 failed
- `cargo test --lib`: 634 passed, 0 failed, 1 ignored
- earlier post-integration `cargo test sftp_object_store::tests --lib`: 14
  passed, 0 failed
- `cargo clippy --lib --tests -- -D warnings -A dead-code`: passed after the
  final unwind/background-drain additions; the only warnings under the raw
  `-D warnings` gate were two confirmed baseline dead-code methods
- `cargo fmt --all -- --check`: passed
- `git diff --check`: passed

All final Cargo verification used the required shared target directory. The
final commands set `CARGO_INCREMENTAL=0` because the shared target had briefly
reused a stale cross-worktree test binary; the non-incremental binaries were
confirmed to compile from this worktree.

## Files in the scoped change

- `zerofs/src/sftp_transport.rs`
- `zerofs/src/sftp_object_store.rs`
- `zerofs/src/parse_object_store.rs`
- `zerofs/src/cli/init.rs`
- `zerofs/src/cli/server.rs`
- `zerofs/src/cli/debug.rs`
- `zerofs/src/cli/password.rs`
- `zerofs/src/fs/flush_coordinator.rs`
- `zerofs/src/db.rs`
- `zerofs/src/main.rs`
- this report

## Concerns and remaining external gate

- Live Hetzner/systemd validation was explicitly out of scope and was not run.
  A controlled deployment/restart remains the external acceptance gate.
- Shutdown forcibly reaps the production SSH process behind an active lease,
  but deliberately returns a bounded error if the caller never returns the
  logical lease. This prevents retained SSH processes without falsely claiming
  a clean logical shutdown.
- Third-party/custom `SessionFactory` implementations must honor the force
  token to obtain the production process-exit guarantee. Panic and cancellation
  paths still fail the pool closed before releasing capacity.
