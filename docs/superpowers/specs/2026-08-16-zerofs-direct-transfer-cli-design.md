# ZeroFS Direct Transfer CLI Design

## Goal

Add direct file and directory transfer commands to the existing `zerofs`
binary. The commands copy bytes through the existing `zerofs-client` 9P
implementation, display live terminal progress, run on macOS and Linux, and do
not require a mount or any server-side change.

The initial commands are:

```text
zerofs upload <target> <local-source> <remote-destination>
zerofs download <target> <remote-source> <local-destination>
```

For example:

```text
zerofs upload ws://server:8080/ws/9p ./Audiobooks /Audiobooks
zerofs download ws://server:8080/ws/9p /Audiobooks ./Audiobooks
```

## Scope

The first release supports regular files, empty directories, and recursively
nested directories. It overwrites matching destination files but never deletes
unrelated destination entries. The source root maps exactly to the destination
argument: a directory source copied to `/Audiobooks` creates or updates that
exact directory, while a file source copied to `/Audiobooks/book.m4b` creates
or replaces that exact file.

The commands accept every target already supported by the native client:

- `unix:/path/to/socket` or a bare Unix-socket path;
- `tcp://host:port`, `host:port`, or `host`;
- comma-separated high-availability targets; and
- `ws://host/path`, including an existing Web UI `/ws/9p` endpoint.

Native `wss://` is excluded because the current native transport deliberately
accepts private `ws://` only. Adding TLS WebSocket support is a separate,
client-only enhancement. The transfer commands do not change server listeners,
authentication, protocol handling, storage, or configuration.

Symlinks, sockets, device nodes, and other non-regular source entries fail with
an explicit unsupported-file-type error. They are never silently skipped or
followed. The first release does not preserve ownership, permissions, extended
attributes, resource forks, or timestamps; uploaded files and directories use
the same `0644` and `0755` defaults as the browser uploader.

## Architecture and Reuse

The commands become new `clap` variants in the existing CLI and dispatch from
the existing Tokio runtime. A focused `cli::transfer` module owns scanning,
path mapping, bounded file scheduling, copy loops, temporary-file publication,
and progress reporting.

The implementation reuses these production paths:

- `zerofs_client::Client` for connection, identity, reconnect, directory
  creation, metadata, rename, and filesystem durability barriers;
- `zerofs_client::File` for positioned `read_at`, `write_at`, and file-handle
  lifecycle;
- negotiated `Client::capabilities()` chunk limits rather than a second
  transfer-specific protocol;
- the current native WebSocket transport already present in `ninep-client`;
- the browser uploader's create-new temporary file, chunked write, rename, and
  batch durability sequence; and
- the browser uploader's bounded concurrency model across files.

The terminal renderer is the only UI-specific addition. It uses the existing
terminal dependencies rather than introducing a second TUI framework.

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
directory type conflict fails before that item is copied.

## Upload Data Flow

For each planned file, the client:

1. creates the destination parent directories if needed;
2. creates a unique `.zerofs-<uuid>.tmp` file in the destination directory with
   create-new semantics;
3. reads the local file into a reusable buffer capped by the negotiated maximum
   9P write payload;
4. writes each buffer at its absolute offset with `File::write_at`;
5. updates aggregate progress after each acknowledged chunk;
6. renames the temporary file over the exact destination path; and
7. retains honest failure state until the batch durability barrier succeeds.

Directory uploads run up to eight file copies concurrently by default. This
matches the browser's useful acceleration model: files are parallel, while the
chunks within one file remain sequential. There is no separate multipart
server API and no within-file range fan-out in this release.

After all successful file renames, the command calls `Client::sync()`, the same
filesystem-wide durability barrier used by browser upload batches. The command
prints completion and exits zero only after that barrier succeeds. If a copy
fails after other files have already been renamed, it still syncs the published
files, reports the failed paths, and exits nonzero. A durability failure also
exits nonzero and states that publication may be visible but durability was not
verified.

Before rename, cancellation or failure closes the remote handle and makes a
best-effort attempt to remove its temporary file. A cleanup failure is included
in diagnostics and is not presented as successful completion.

## Download Data Flow

For each planned file, the client:

1. creates local destination parents if needed;
2. opens the remote file once;
3. creates a unique local temporary file beside the destination;
4. calls `File::read_at` sequentially using the negotiated maximum read chunk;
5. writes each returned chunk to the local temporary file and updates progress;
6. calls local `sync_all`, closes the temporary file, and atomically renames it
   over the exact destination path.

Directory downloads also run up to eight files concurrently. They stream bytes
directly to disk and do not reproduce the browser's memory-bound ZIP step. A
failure before rename removes the local temporary file on a best-effort basis,
reports the path, and exits nonzero.

## Progress and Output

On an interactive terminal, upload and download continuously display:

- direction and active filename;
- transferred and total bytes;
- overall percentage;
- rolling transfer rate;
- estimated time remaining; and
- completed and total file count.

The byte counter advances only after the corresponding destination write has
succeeded. The final state changes from `transferring` to `verifying durability`
for upload and to `finalizing` for download. `complete` appears only after the
relevant durability and rename steps succeed.

When stderr is not an interactive terminal, the renderer emits stable line-
oriented events at file completion plus one final summary, avoiding control
characters in logs. Diagnostics go to stderr. A successful transfer exits `0`;
invalid arguments, scan failures, connection failures, copy failures, cleanup
failures, or unverified durability exit nonzero.

## Cancellation and Reconnection

The first Ctrl-C stops scheduling new files and allows in-flight 9P operations
to settle before cleanup. The command exits nonzero and never prints completion.
It relies on the existing client's reconnect and mutation replay behavior; it
does not add a second retry layer that could duplicate ambiguous mutations.

The initial release does not resume partial transfers across process restarts.
Temporary files are deliberately not treated as completed data.

## Testing and Acceptance

Implementation follows test-driven development. The focused test set covers:

- CLI parsing for upload and download;
- exact-root path mapping and traversal rejection;
- nested directory planning, empty directories, and unsupported entry types;
- progress accounting and non-TTY output;
- remote and local temporary-file cleanup on failure;
- overwrite behavior without deletion of unrelated entries; and
- nonzero outcomes for transfer and durability failures.

End-to-end tests use a real in-process ZeroFS 9P server and the production
`zerofs-client`, not a simulated successful transport. They upload and download
single files and nested directories, compare byte counts and content hashes,
exercise overwrites, and verify that the upload completion path includes the
real durability barrier.

Acceptance requires:

1. focused transfer tests pass on macOS and Linux;
2. the complete existing CLI test/build gate remains green;
3. a real command-line upload and download through an existing 9P endpoint
   reproduce source bytes and nested paths; and
4. observed terminal output reports real byte movement and does not claim
   completion before durability/finalization succeeds.

Prebuilt macOS packaging, signing, notarization, native `wss://`, within-file
parallel ranges, restart resume, bidirectional synchronization, checksum-based
skip logic, and destination deletion are outside this first release.
