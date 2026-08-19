# ZeroFS Direct Transfer CLI Design

## Goal

Add direct file transfer and removal commands to the existing `zerofs` binary.
The commands use the existing `zerofs-client` 9P implementation, display live
terminal progress, run on macOS and Linux, and do not require a mount or any
server-side change.

The approved commands are:

```text
zerofs upload <target> <local-source> <remote-destination>
zerofs download <target> <remote-source> <local-destination>
zerofs rm <target> <remote-path>
```

For example:

```text
zerofs upload ws://server:8080/ws/9p ./Audiobooks /Audiobooks
zerofs download ws://server:8080/ws/9p /Audiobooks ./Audiobooks
zerofs rm ws://server:8080/ws/9p /old-audiobooks
```

## Scope

The first release copies regular files, empty directories, and recursively
nested directories. It overwrites matching destination files but never deletes
unrelated destination entries during a copy. The source root maps exactly to
the destination argument: a directory source copied to `/Audiobooks` creates
or updates that exact directory, while a file source copied to
`/Audiobooks/book.m4b` creates or replaces that exact file.

`zerofs rm` is the approved destructive counterpart: it recursively removes
only the selected remote file or tree, runs without a prompt, refuses the 9P
attach root, and exits nonzero on cancellation or any failed deletion. It
validates the remote tree before mutation, removes entries deepest-first, and
issues one remove mutation at a time so cancellation is checked between every
irreversible change.

The commands accept every target already supported by the native client:

- `unix:/path/to/socket` or a bare Unix-socket path;
- `tcp://host:port`, `host:port`, or `host`;
- comma-separated high-availability targets; and
- `ws://host/path`, including an existing Web UI `/ws/9p` endpoint.

Native `wss://` is excluded because the current native transport deliberately
accepts private `ws://` only. Adding TLS WebSocket support is a separate,
client-only enhancement. The transfer commands do not change server listeners,
authentication, protocol handling, storage, or configuration.

For upload and download, source symlinks, sockets, device nodes, and other
non-regular entries fail with an explicit unsupported-file-type error during
planning. Upload reopens the final source component with `O_NOFOLLOW` and
revalidates its type and planned size. Download destination ancestry uses a
separate no-follow directory-handle walk described below. The first release
does not preserve ownership, permissions, extended attributes, resource forks,
or timestamps;
uploaded files and directories use the same `0644` and `0755` defaults as the
browser uploader.

## Architecture and Reuse

The commands become new `clap` variants in the existing CLI and dispatch from
the existing Tokio runtime. A focused `cli::transfer` module owns scanning,
path mapping, bounded file scheduling, sequential removal, copy loops, temporary-file
publication, and progress reporting.

The implementation reuses these production paths:

- `zerofs_client::Client` for connection, identity, reconnect, directory
  creation, metadata, rename, and filesystem durability barriers;
- `zerofs_client::File` for positioned `read_at`, `write_at`, and file-handle
  lifecycle;
- negotiated `Client::capabilities()` chunk limits rather than a second
  transfer-specific protocol;
- the current native WebSocket transport already present in `ninep-client`;
- the browser uploader's create-new temporary file, chunked write, and rename
  sequence; and
- the browser uploader's bounded concurrency model across files, extended with
  two reusable 9P sessions per upload worker so high-latency writes can overlap.
  Download workers retain one reusable session.

The terminal renderer is the only UI-specific addition. It uses Indicatif's
`MultiProgress` for one aggregate bar plus the active per-file bars.

## Path Planning

Before moving bytes, upload scans the complete local source tree and download
scans the complete remote source tree. The scan produces an ordered transfer
plan containing directories, regular files, relative paths, individual sizes,
total files, and total bytes.

Scanning before transfer has three purposes:

1. reject unsupported entries and source errors before publication begins;
2. create directories in parent-before-child order; and
3. give progress an accurate total byte and file count.

For upload, local paths are joined beneath the exact remote destination root.
For download, remote paths are joined beneath the exact local destination root.
All joins reject traversal outside the selected root. Empty directories are
retained. A source-directory/destination-file or source-file/destination-
directory type conflict is preflighted for the complete plan and fails before
any file bytes are copied.

## Upload Data Flow

For each planned file, the client:

1. creates the destination parent directories if needed;
2. when `--resume` is set, skips a final regular destination file whose byte
   length matches the planned source; hidden temporary files never qualify;
3. creates a unique `.zerofs-<uuid>.tmp` file in the destination directory with
   create-new semantics;
4. opens the same private temporary file through both of the worker's sessions;
5. reads the local file into reusable buffers capped by the negotiated maximum
   9P write payload;
6. writes each buffer at its absolute offset with `File::write_at`, with at most
   two active writes on either session;
7. updates the active file bar after each acknowledged chunk and advances the
   monotonic aggregate only when that file is durably published;
8. verifies each session's acknowledged-write lineage through its open fid;
9. renames the temporary file over the exact destination path;
10. runs the primary client's filesystem-wide sync to verify the namespace
    publication; and
11. reports the file complete only after every durability barrier succeeds.

Resume is opt-in and size-based so a repeated directory upload can avoid
replacing already materialized files without downloading them. It does not
prove content equality: two regular files with equal byte lengths are treated
as a match. Missing, different-length, and non-regular destinations follow the
normal upload or existing type-conflict path. The default remains overwrite.

Directory uploads run up to eight file copies concurrently by default. Each
upload worker owns two reusable client sessions, capped at twice the requested
job count. Each session keeps at most two extent-aligned writes active, so one
file has at most four chunks in flight and the server can stage disjoint ranges
across high-latency acknowledgements. At the default eight jobs, the uploader
uses at most 16 sessions and about 288 MiB of chunk buffers when every active
file is large enough; higher job counts increase both bounds. The CLI requests
a 9 MiB write payload plus 9P framing and obeys any smaller server-negotiated
maximum, so this needs no new server API or persistent multipart state.

Each session verifies its own acknowledged writes before publication. Rename
makes the fully written file visible; the following filesystem-wide sync
verifies the namespace change before the CLI calls it complete. Empty-directory-
only uploads use the same `Client::sync()` durability endpoint. A connection,
stale-handle, leader, or retry-later (`EAGAIN`) failure restarts that file from
a new private temporary path. Resume eligibility is evaluated only on the first
attempt; retries always rewrite and re-verify the destination. An exhausted or
permanent file failure is
recorded while the remaining queue continues; the command reports every failed
file and exits nonzero after all scheduled work settles. A sync failure states
that the renamed file may be visible but its durability was not verified.

Before rename, cancellation or failure closes the remote handle and always
attempts to remove its temporary file. A cleanup failure is terminal
for that file attempt: it is included in diagnostics and cannot be hidden by a
later successful retry.

## Download Data Flow

For each planned file, the client:

1. preflights all final path types, opens every existing destination ancestor
   without following symlinks, and creates missing parents with `mkdirat` from
   held directory handles;
2. opens the remote file once;
3. creates a unique local temporary file with `openat(O_EXCL|O_NOFOLLOW)` on the
   held destination-parent handle;
4. calls `File::read_at` sequentially for exactly the remaining planned bytes
   and then probes one byte at the planned EOF;
5. writes each returned chunk to the local temporary file and updates progress;
6. calls local `sync_all`, closes the temporary file, and atomically publishes
   it with `renameat` on that same held parent handle. Failure cleanup uses
   `unlinkat` on the handle, never a re-resolved ancestor pathname.

Directory downloads also run up to eight files concurrently. They stream bytes
directly to disk and do not reproduce the browser's memory-bound ZIP step. A
failure before rename always attempts to remove the local temporary file,
reports the path, and exits nonzero; cleanup failure is attached and terminal.

## Progress and Output

On an interactive terminal, upload and download continuously display one
aggregate bar plus one bar for each active file:

- direction and active filename;
- transferred and total bytes;
- overall percentage;
- rolling transfer rate;
- estimated time remaining; and
- completed and total file count.

An active per-file byte counter advances only after the corresponding
destination write succeeds. The aggregate accounts only durably published or
size-skipped files, never moves backward across retries, and excludes skipped
bytes from transfer-rate calculation. A per-file bar remains active through
publication and durability, then disappears before the aggregate completed-file
count advances. `complete` appears only after every relevant sync and rename
succeeds.

When stderr is not an interactive terminal, the renderer emits line-oriented
events in deterministic planned-file order plus one final summary, avoiding
control characters in logs even when files finish concurrently. Diagnostics go
to stderr. A successful transfer exits `0`;
invalid arguments, scan failures, connection failures, copy failures, cleanup
failures, or unverified durability exit nonzero.

Recursive removal displays the removed-entry count and current path, then a
final summary only after the selected file or tree is gone.

## Cancellation and Reconnection

Once transfer execution begins, the first Ctrl-C stops scheduling new files and
allows in-flight 9P operations to settle before cleanup. The command exits
nonzero and never prints completion.
Settling is deliberately unbounded: a 9P reply wait has no aggregate deadline
while its connection keeps proving live, so a legitimately slow bulk write is
never aborted for exceeding a fixed ceiling, and a wedged server operation can
hold the first Ctrl-C indefinitely. Abandoning that wait instead would drop a
dispatched mutation and strand the temporary file it was writing, so the CLI
waits and stays audible: every ten seconds it restates that in-flight
operations are still settling and that a second Ctrl-C exits immediately.
The second Ctrl-C is an explicit emergency exit with status 130 if an in-flight
server operation does not settle; because it bypasses cleanup, it may leave the
current hidden temporary file behind. The 9P client replays an interrupted
mutation
with its original operation ID. If a connection, stale-handle, leader, or
retry-later (`EAGAIN`) error still reaches the transfer layer, the CLI retries
from a new private temporary file rather than blindly duplicating the ambiguous
chunk mutation. It makes at most three file attempts, logs each retry, and
continues other queued files after a permanent or exhausted failure before
returning the complete error list. A failed temporary-file cleanup is never
retried away.

The initial release does not resume partial transfers across process restarts.
Temporary files are deliberately not treated as completed data.

## Testing and Acceptance

The focused test set currently covers:

- CLI parsing for upload, download, and recursive removal;
- local and remote planning for nested files and empty directories, including
  rejection of local source symlinks;
- aggregate and per-file progress accounting plus terminal bar lifecycle and
  narrow-terminal layout;
- real single-file and nested-tree upload/download with exact byte comparison,
  exact destination paths, and preservation of unrelated entries;
- size-based upload resume for single files and nested trees, including
  different-length replacement and local-source revalidation before a skip;
- cancellation before download publication and visibility of files completed
  before another file fails, including mid-transfer temporary-file cleanup;
- exact planned-size reads plus one-byte EOF probes for remote growth and
  truncation;
- no-follow destination ancestry for single-file and directory downloads,
  including an ancestor-exchange race against a held parent directory handle;
- upload sync failure, mandatory cleanup, cleanup failure diagnostics, and
  same-path overwrite preservation;
- monotonic aggregate progress and deterministic non-TTY event ordering;
- exclusion of failed-attempt bytes from aggregate progress, bounded transient
  file retries, and
  continued scheduling after one file exhausts its attempts;
- sequential cancellation-aware recursive removal of files and nested trees
  plus refusal of attach-root aliases; and
- transfer-worker error aggregation and client cleanup-barrier settlement; and
- settling notices that stay silent until cancellation and then repeat on a
  fixed interval.

End-to-end tests use a real in-process ZeroFS 9P server and the production
`zerofs-client`, not a simulated successful transport. They upload and download
single files and nested directories and compare exact byte content through the
real per-file publication and durability path.

Acceptance requires:

1. focused transfer tests pass on macOS and Linux;
2. the complete existing CLI test/build gate remains green;
3. real command-line upload, download, and recursive removal through an
   existing 9P endpoint reproduce source bytes and nested paths, remove only the
   selected tree, and preserve unrelated entries; and
4. observed terminal output reports real byte movement and does not claim
   completion before durability/finalization succeeds.

Prebuilt macOS packaging, signing, notarization, native `wss://`, unbounded or
persisted within-file range fan-out, partial-file byte-offset restart resume,
bidirectional synchronization, checksum-based skip logic, and destination
mirroring are outside this first release.
