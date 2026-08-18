# ZeroFS Direct Transfer CLI Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add real, progress-reporting recursive upload, download, and removal commands to the existing `zerofs` binary using the production 9P client without mounting or changing the server.

**Architecture:** Add `zerofs upload`, `zerofs download`, and `zerofs rm` to the existing Clap command tree. A focused transfer module scans copy sources into exact-root plans, streams regular files through `zerofs-client` with browser-style temporary files and bounded file concurrency, recursively removes an explicitly selected remote path, and reports terminal progress. Tests exercise pure planning/output units plus real operations through an in-process ZeroFS 9P server.

**Final implementation note:** The completed CLI uses two reusable 9P sessions per bounded upload worker (one per download worker), requests an extent-aligned 9 MiB write payload plus protocol framing, and keeps two positioned writes active on each upload session while obeying any smaller server-negotiated maximum. Each session verifies its acknowledged writes before rename, and a filesystem-wide sync verifies publication before completion. Empty-directory-only uploads retain a final `Client::sync()`. Prompt-free recursive `rm` refuses the attach root. Transport hardening acknowledges actual socket sends before starting reply liveness, retains bounded write-stall detection, and keeps a slow request alive while the server remains responsive. Connection, stale-handle, leader, and retry-later (`EAGAIN`) file errors restart only that file from a new private temp up to three total attempts; cleanup failures are terminal, and exhausted files are reported while the remaining queue continues. These decisions supersede the original single-client, sequential-write, batch-wide `Client::sync()`, fail-fast scheduling, and upload/download-only steps below.

**Tech Stack:** Rust 2024, Clap, Tokio, futures, tokio-util cancellation, Indicatif, uuid, `zerofs-client`, the in-tree ZeroFS 9P test server.

## Global Constraints

- Support macOS and Linux from the existing `zerofs` binary.
- Do not add or change any server listener, handler, protocol, authentication, storage, or configuration behavior.
- Reuse native Unix, TCP, HA, and `ws://` targets; native `wss://` remains out of scope.
- Copy regular files and nested or empty directories; fail explicitly on symlinks and other special source entries.
- Recursively remove only the explicitly selected remote path, without a prompt, and refuse the 9P attach root.
- Map the source root exactly to the destination argument.
- Overwrite matching files during copies but never delete unrelated destination entries.
- Request an extent-aligned 9 MiB write payload plus 9P framing and stream server-negotiated chunks; do not load a whole file or directory archive into memory.
- Default to eight concurrent files, allow `--jobs N` to change that limit, and keep at most two writes active per upload session (four per file across two sessions).
- Upload completion requires every session to verify its writes, followed by rename and a successful filesystem-wide `Client::sync()`; empty-directory-only uploads use the same sync endpoint.
- Download publication requires local `sync_all` followed by rename.
- Do not resume partial transfers across process restarts.
- Replay interrupted 9P mutations with their original operation ID; retry a
  connection, stale-handle, leader, or retry-later (`EAGAIN`) file failure only
  from a new private temporary file, at most three total attempts, and continue
  the remaining queue after exhaustion. Never retry away a failed temporary-file
  cleanup.
- Never print completion after cancellation, transfer failure, cleanup failure, or unverified durability.
- Preserve the existing commits `75b64c9` and `810ba4d` and do not absorb unrelated worktree changes.

---

## File Structure

- Modify `zerofs/src/cli/mod.rs`: declare upload, download, and removal commands and their arguments.
- Modify `zerofs/src/main.rs`: dispatch upload, download, and removal through the existing Tokio runtime.
- Create `zerofs/src/cli/transfer/mod.rs`: public runners, cancellation, bounded scheduling and removal, final durability, and error aggregation.
- Create `zerofs/src/cli/transfer/tests.rs`: real 9P integration and transfer orchestration tests, mounted by `transfer/mod.rs` under `cfg(test)`.
- Create `zerofs/src/cli/transfer/plan.rs`: exact-root local and remote tree scanning and type-conflict validation.
- Create `zerofs/src/cli/transfer/copy.rs`: one-file upload/download loops, temporary-file creation, rename, and cleanup.
- Create `zerofs/src/cli/transfer/progress.rs`: aggregate state, TTY rendering, stable non-TTY events, rate, percentage, and ETA formatting.
- Modify `README.md`: document direct transfers, target forms, examples, overwrite behavior, and limitations.

### Task 1: Exact-Root Transfer Plans

**Files:**
- Create: `zerofs/src/cli/transfer/mod.rs`
- Create: `zerofs/src/cli/transfer/plan.rs`

**Interfaces:**
- Consumes: `zerofs_client::{Client, FileType}`, `std::path::{Path, PathBuf}`.
- Produces: `TransferPlan`, `PlannedFile`, `scan_local`, and `scan_remote` for later command runners.

- [ ] **Step 1: Write failing local-planning tests**

Add unit tests in `transfer/plan.rs` that create a temporary local tree with a regular file and an empty nested directory, assert `total_bytes`, assert relative paths, and assert that a symlink returns an error containing `unsupported source entry`.

- [ ] **Step 2: Run the focused tests and verify RED**

Run:

```bash
cd zerofs
cargo test -p zerofs cli::transfer::plan::tests -- --nocapture
```

Expected: compilation or assertion failure because the planning module does not exist.

- [ ] **Step 3: Implement the plan types**

Create these exact plan records in `transfer/plan.rs`:

```rust
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct PlannedFile {
    pub source: PathBuf,
    pub relative: PathBuf,
    pub size: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct TransferPlan {
    pub source_is_dir: bool,
    pub directories: Vec<PathBuf>,
    pub files: Vec<PlannedFile>,
    pub total_bytes: u64,
}

pub(super) fn scan_local(source: &Path) -> anyhow::Result<TransferPlan>;
pub(super) async fn scan_remote(
    client: &zerofs_client::Client,
    source: &Path,
) -> anyhow::Result<TransferPlan>;
```

Use `symlink_metadata` and `read_dir` for local scanning so symlinks are detected rather than followed. Use `Client::metadata` plus incremental `read_dir` traversal remotely. Validate every child as one normal path component, sort directories parent-first, sort files deterministically, and use checked addition for `total_bytes`.

Declare `mod transfer;` and `mod plan;`, but do not expose a command until its real upload path exists in Task 2.

- [ ] **Step 4: Run focused tests and verify GREEN**

Run:

```bash
cd zerofs
cargo test -p zerofs cli::transfer::plan::tests -- --nocapture
cargo check -p zerofs --bin zerofs
```

Expected: focused tests pass and the binary checks on macOS.

- [ ] **Step 5: Commit the planning slice**

```bash
git add zerofs/src/cli/mod.rs zerofs/src/cli/transfer/mod.rs zerofs/src/cli/transfer/plan.rs
git commit -m "feat(cli): plan direct 9p transfers"
```

### Task 2: Browser-Style Upload with Real Durability

**Files:**
- Modify: `zerofs/src/cli/mod.rs`
- Modify: `zerofs/src/main.rs`
- Create: `zerofs/src/cli/transfer/copy.rs`
- Modify: `zerofs/src/cli/transfer/mod.rs`
- Create: `zerofs/src/cli/transfer/tests.rs`
- Test: focused unit tests in `copy.rs` and real orchestration tests in `tests.rs`

**Interfaces:**
- Consumes: `TransferPlan`, `PlannedFile`, `Client::capabilities`, `Client::open`, `File::write_at`, `Client::rename`, and `Client::sync`.
- Produces: the real `zerofs upload` command, `upload_file`, remote temporary-file cleanup, and a complete `execute_upload` orchestration function.

- [ ] **Step 1: Write failing CLI and real-server upload tests**

Add a Clap parsing test requiring this complete command shape:

```rust
Upload {
    target: String,
    source: PathBuf,
    destination: PathBuf,
    #[arg(long, default_value_t = 8, value_parser = clap::value_parser!(usize).range(1..))]
    jobs: usize,
},
```

In `transfer/mod.rs`, start `NinePServer::new_unix` over `ZeroFS::new_in_memory`, connect with the production `zerofs_client::Client`, and require:

```rust
#[tokio::test]
async fn upload_copies_nested_bytes_and_preserves_unrelated_entries() {
    // Local source: root.bin and nested/child.bin; remote already has /dest/keep.txt.
    // Execute upload to /dest.
    // Assert all source bytes match, /dest/nested exists, and keep.txt remains.
}

#[tokio::test]
async fn cancelled_upload_does_not_publish_or_leave_temporary_files() {
    // Cancel before executing one planned file.
    // Assert the final path is absent and no .zerofs-*.tmp entry remains.
}
```

The first test also records the client's operation counters before execution and asserts that the successful path performs additional 9P operations through the real server.

- [ ] **Step 2: Run upload tests and verify RED**

Run:

```bash
cd zerofs
cargo test -p zerofs cli::transfer::tests::upload_ -- --nocapture
```

Expected: failure because `execute_upload` and `upload_file` are missing.

- [ ] **Step 3: Implement upload copying and publication**

Add the `Upload` command variant and dispatch it from `main.rs` only when these real implementations are added:

```rust
pub async fn run_upload(
    target: &str,
    source: &Path,
    destination: &Path,
    jobs: usize,
) -> anyhow::Result<()>;

pub(super) async fn upload_file(
    client: Arc<Client>,
    local_source: PathBuf,
    remote_destination: PathBuf,
    chunk_size: usize,
    progress: Progress,
    cancel: CancellationToken,
) -> anyhow::Result<()>;

async fn execute_upload(
    client: Arc<Client>,
    plan: TransferPlan,
    destination: PathBuf,
    jobs: usize,
    progress: Progress,
    cancel: CancellationToken,
) -> anyhow::Result<()>;
```

Create remote temporary files beside the final path using:

```rust
OpenOptions::write_only().create_new(true).mode(0o644)
```

Read the local Tokio file into one reusable `Vec<u8>` capped by `max_write_chunk`, call `write_at` sequentially, update progress only after each acknowledged write, rename the temporary path over the exact destination, and close the handle in every outcome. Before rename, errors and cancellation remove the temporary path best-effort and include cleanup failure in the returned error.

Create at most `min(jobs, file_count)` reusable 9P clients. Each worker dequeues
one file at a time and keeps that file's chunks sequential. After writing a
file, rename it into visibility, sync through the still-open file handle, and
only then mark it complete. Settle every active worker before aggregating
errors. An empty-directory-only upload uses `Client::sync()` for its final
durability barrier.

- [ ] **Step 4: Run upload and existing client tests and verify GREEN**

Run:

```bash
cd zerofs
cargo test -p zerofs cli::transfer::tests::upload_ -- --nocapture
cargo test -p zerofs zerofs_client_tests::large_file_chunks_across_msize -- --nocapture
```

Expected: all selected tests pass with real 9P byte movement.

- [ ] **Step 5: Commit the upload slice**

```bash
git add zerofs/src/cli/mod.rs zerofs/src/main.rs zerofs/src/cli/transfer/copy.rs zerofs/src/cli/transfer/mod.rs zerofs/src/cli/transfer/tests.rs
git commit -m "feat(cli): upload files directly over 9p"
```

### Task 3: Streaming Download and Terminal Progress

**Files:**
- Modify: `zerofs/src/cli/mod.rs`
- Modify: `zerofs/src/main.rs`
- Modify: `zerofs/src/cli/transfer/copy.rs`
- Modify: `zerofs/src/cli/transfer/mod.rs`
- Modify: `zerofs/src/cli/transfer/tests.rs`
- Create: `zerofs/src/cli/transfer/progress.rs`
- Test: focused unit tests in `copy.rs` and `progress.rs`, plus real orchestration tests in `tests.rs`

**Interfaces:**
- Consumes: `TransferPlan`, `Client::open`, `File::read_at`, Tokio local file APIs, and `CancellationToken`.
- Produces: the real `zerofs download` command, `download_file`, `execute_download`, and cloneable `Progress` methods used by upload and download.

- [ ] **Step 1: Write failing download and progress tests**

Add a Clap parsing test requiring this complete command shape:

```rust
Download {
    target: String,
    source: PathBuf,
    destination: PathBuf,
    #[arg(long, default_value_t = 8, value_parser = clap::value_parser!(usize).range(1..))]
    jobs: usize,
},
```

Add a real-server test that creates `/source/root.bin`, `/source/nested/child.bin`, and an empty directory through `zerofs-client`, downloads to a temporary local root, and compares all bytes while preserving a pre-existing unrelated local file. Add a cancellation test asserting no final or `.zerofs-*.tmp` file remains.

Add progress tests for Indicatif-backed aggregate and active-file bars. Require
percentage, transferred/total units, rate, ETA, and file counts on interactive
terminals; completed file bars must disappear, and non-TTY completion events
must remain line-oriented.

- [ ] **Step 2: Run download/progress tests and verify RED**

Run:

```bash
cd zerofs
cargo test -p zerofs cli::transfer::tests::download_ -- --nocapture
cargo test -p zerofs cli::transfer::progress::tests -- --nocapture
```

Expected: failure because download copying and progress types are missing.

- [ ] **Step 3: Implement download and progress**

Add the `Download` command variant and dispatch it from `main.rs` only when these real implementations are added:

```rust
pub async fn run_download(
    target: &str,
    source: &Path,
    destination: &Path,
    jobs: usize,
) -> anyhow::Result<()>;

pub(super) async fn download_file(
    client: Arc<Client>,
    remote_source: PathBuf,
    local_destination: PathBuf,
    chunk_size: u32,
    progress: Progress,
    cancel: CancellationToken,
) -> anyhow::Result<()>;
```

Open the remote file once, create a unique local temp beside the destination with `create_new`, loop `read_at(offset, chunk_size)`, write each chunk with Tokio `write_all`, and advance progress only after the local write succeeds. Call local `sync_all`, close, and rename over the exact destination. Remove the temp best-effort on failure or cancellation.

Implement bounded concurrent download scheduling using the same eight-job default. Use Indicatif `MultiProgress` for one aggregate bar plus the active file bars. For non-TTY stderr, print one stable line per completed file and one final summary. Install a Ctrl-C task that cancels a shared token, stops new scheduling, lets in-flight calls settle, performs cleanup, and returns nonzero.

Remove each file bar after publication/finalization and call aggregate `finish()` only after every relevant sync and rename succeeds.

- [ ] **Step 4: Run focused transfer tests and verify GREEN**

Run:

```bash
cd zerofs
cargo test -p zerofs cli::transfer -- --nocapture
cargo check -p zerofs --bin zerofs
```

Expected: transfer tests pass and the existing binary checks on macOS.

- [ ] **Step 5: Commit the download/progress slice**

```bash
git add zerofs/src/cli/mod.rs zerofs/src/main.rs zerofs/src/cli/transfer/copy.rs zerofs/src/cli/transfer/mod.rs zerofs/src/cli/transfer/progress.rs zerofs/src/cli/transfer/tests.rs
git commit -m "feat(cli): download over 9p with progress"
```

### Task 4: Native and Browser 9P Transport Hardening

**Files:**
- Modify: `zerofs/ninep-client/src/lib.rs`
- Modify: `zerofs/ninep-client/src/native_web_transport.rs`
- Modify: `zerofs/ninep-client/src/web_transport.rs`
- Create: `zerofs/ninep-client/src/write_progress.rs`
- Create: `zerofs/ninep-client/src/buffered_send.rs`

Start request reply liveness only after the transport has actually flushed the
frame. Keep FIRST/RETRY ambiguity recorded at enqueue, prefer an already
delivered response over a subsequently dropped send acknowledgement, and bound
an outbound socket with no forward progress. Native TCP and WebSocket writers
acknowledge after flush; browser WebSocket sends use a bounded 4 MiB pipeline
and acknowledge frame boundaries as `bufferedAmount` drains. A slow reply may
continue through repeated bounded liveness probes while the server remains
responsive; loss of liveness triggers reconnect and mutation replay.

Regression tests cover a withheld send acknowledgement, response-versus-ack
failure ordering, a stalled writer, slow ongoing write progress, a reply after
more than eight successful liveness windows, the browser buffering state
machine, and bracketed IPv6 native WebSocket URLs. Verify native tests and
strict Clippy plus the `wasm32-unknown-unknown` compile gate.

### Task 5: Documentation and Cross-Platform Verification

**Files:**
- Modify: `README.md`
- Modify only if required by formatting: files created in Tasks 1-3

**Interfaces:**
- Consumes: the completed command surface.
- Produces: user-facing examples and final verification receipts.

- [ ] **Step 1: Write the README command contract**

Add a `Direct transfers (no mount)` subsection documenting all three commands, Unix/TCP/HA/`ws://` target examples, exact-root copy semantics, explicit recursive removal, overwrite-without-delete behavior during copies, progress fields, per-session write verification before rename and filesystem sync after rename, the requested 9 MiB message size and server-negotiated payload, the eight-job default and `--jobs`, two upload sessions with two active writes each, size-based `upload --resume` for materialized final files, and exclusions for `wss://`, special source entries, partial-file byte-offset resume, and persisted multipart state.

- [ ] **Step 2: Run formatting and focused verification**

Run:

```bash
cd zerofs
cargo fmt --all -- --check
cargo test -p zerofs cli::transfer -- --nocapture
cargo test -p zerofs cli::tests -- --nocapture
cargo test -p ninep-client
cargo check -p ninep-client --target wasm32-unknown-unknown
cargo check -p zerofs --bin zerofs
```

Expected: all commands exit zero with no formatting drift.

- [ ] **Step 3: Run the broader binary test gate**

Run:

```bash
cd zerofs
cargo test -p zerofs --bin zerofs
```

Expected: all binary tests pass. If an unrelated pre-existing failure occurs, retain its exact output and do not call the branch green.

- [ ] **Step 4: Verify Linux compilation**

Run on a Linux runner or configured Linux target:

```bash
cd zerofs
cargo check -p zerofs --bin zerofs
```

Expected: exit zero. The macOS receipt from Tasks 1 and 3 plus this Linux receipt satisfy the two-platform compilation requirement.

- [ ] **Step 5: Commit documentation**

```bash
git add README.md
git commit -m "docs: document direct 9p transfers"
```

- [ ] **Step 6: Record final branch receipts**

Run:

```bash
git status --short --branch
git log --oneline --decorate 75b64c9^..HEAD
```

Expected: no uncommitted transfer changes; the inclusive history contains the preserved base plus the design, planning, command surface, upload, download/removal, progress, transport, and documentation commits.
